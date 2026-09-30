"""Automatic recovery notices must preserve scanner-only input admission."""

import copy
from types import SimpleNamespace

import pytest

import Label_Match as lm
from tests.test_w9scanonly_central import central  # noqa: F401
from tests.test_deferred_lease_clock_retry import clock_case  # noqa: F401
from tests.test_w9scanonly_packaging import packaging, scan, completions, fake_dialogs, products  # noqa: F401
from tests.test_legacy_label_transition import MASTER, _app_for_recovery, _close


def test_held_source_scan_has_only_inline_feedback(central):
    app, dialogs, case, root, releases=central
    master=case.group['scan_payload']
    fields=lm._label_match_parse_compact_phs2(master)
    app._legacy_label_transition_enabled=False
    app.package_outbox.hold_workbench_set(set_id='REVIEW-HELD',source_phs2=master,
        source_input_tag_id=fields['ITG'],snapshot={'current_set_info':{'id':'REVIEW-HELD','raw':[master]}},
        reason='synthetic unknown',held_by='synthetic-authenticated-local-manager')
    scan(app,master)
    assert app.current_set_info['raw']==[]
    assert dialogs==[], 'held source scan opened a modal warning'


@pytest.mark.parametrize("state", ["label_lookup", "label_recovery", "state_review", "central_lookup", "exchange_review", "exchange_pending"])
def test_automatic_recovery_gates_use_inline_notice_and_sound(central, state):
    app, dialogs, _case, _root, _releases = central
    sounds = []
    app._play_sound = sounds.append
    if state == "label_lookup":
        app._phs_label_scan_lookup_in_progress = True
    elif state == "label_recovery":
        app.phs_label_exchange_coordinator = SimpleNamespace(has_pending_reconciliation=lambda: True)
    elif state == "state_review":
        app.current_set_info["recovery_operator_review"] = True
    elif state == "central_lookup":
        app._central_seal_lookup_in_progress = True
    else:
        app._current_sealed_transfer_exchange_attempt = lambda: SimpleNamespace(
            status="OPERATOR_REVIEW" if state == "exchange_review" else "SENDING",
            local_apply_status="PENDING", seal_verification_status="PENDING")
    assert app._sealed_transfer_exchange_blocks_local_action("현재 세트 취소")
    assert dialogs == []
    notice = app.__dict__.get("_phs_label_guidance_notice")
    assert notice and notice.message
    assert sounds == ["fail"]


@pytest.mark.parametrize("different_worker", [False, True])
def test_unfinished_local_set_restores_without_a_question(packaging, tmp_path, monkeypatch, different_worker):
    app, _dialogs = packaging
    for value in [MASTER, products()[0]]:
        scan(app, value)
    saved = app.data_manager.load_current_state()["current_set_info"]
    _close(app)
    restored = _app_for_recovery(lm, tmp_path, monkeypatch)
    if different_worker:
        restored.worker_name = "next-shift-worker"
    dialogs = fake_dialogs(monkeypatch)
    try:
        restored._load_current_set_state()
        assert dialogs == []
        assert restored.current_set_info["raw"] == saved["raw"]
        assert restored.current_set_info["parsed"] == saved["parsed"]
        assert restored._workflow_recovered
    finally:
        _close(restored)


@pytest.mark.parametrize("failure", ["missing_sound", "playback_error"])
def test_automatic_siren_failure_does_not_open_a_window(packaging, monkeypatch, failure):
    import queue
    app, dialogs = packaging
    monkeypatch.setattr(lm, "_label_match_automated_test_mode", lambda: False)
    app.run_tests = False
    app.sound_objects = {}
    if failure == "missing_sound":
        assert app._start_error_siren_thread() is None
    else:
        app._siren_result_queue = queue.Queue()
        app._siren_result_queue.put(("error", "synthetic audio device unavailable"))
        app._siren_threads = []
        app._poll_error_siren()
    assert dialogs == []
    assert app._phs_label_guidance_notice.message


@pytest.mark.parametrize("failure", ["history_read", "history_display"])
def test_automatic_history_failure_is_inline_and_preserves_the_set(packaging, failure):
    import queue
    app, dialogs = packaging
    scan(app, MASTER)
    before = copy.deepcopy(app.current_set_info)
    app.history_load_generation = 1
    app.history_queue = queue.Queue()
    if failure == "history_read":
        app.history_queue.put({"error": "synthetic unreadable history"})
        app._start_history_display_apply = lambda result: None
        app._process_history_queue()
    else:
        def interrupted_steps():
            raise OSError("synthetic display failure")
            yield
        app._apply_history_display_chunk(1, interrupted_steps())
    assert dialogs == []
    assert app._phs_label_guidance_notice.message
    assert app.current_set_info == before


def test_inline_recovery_guidance_keeps_the_real_presenter_open():
    from tests.test_label_operator_action_gates import _render_app
    app = _render_app(raw=("MASTER-A", "PRODUCT-A"), parsed=("ITEM-001", "ITEM-001"))
    sounds = []
    app._play_sound = sounds.append
    lm.Label_Match._show_scanner_notice(app, "이전 작업 복구", "계속 제품을 스캔하세요.")
    view = app._render_operator_workbench()
    assert view.scan_input_enabled
    assert app._phs_label_guidance_notice.message == "계속 제품을 스캔하세요."
    assert not app.__dict__.get("_workflow_blocking_notice")
    assert sounds == ["fail"]


def test_next_accepted_product_clears_transient_recovery_guidance(packaging):
    app, dialogs = packaging
    scan(app, MASTER)
    app._show_scanner_notice("이전 작업 복구", "계속 제품을 스캔하세요.")
    value = products()[0]
    scan(app, value)
    assert app.current_set_info["raw"] == [MASTER, value]
    assert app._phs_label_guidance_notice is None
    assert dialogs == []


def test_accepted_exchange_clears_transient_recovery_guidance(central):
    app, dialogs, case, root, _releases = central
    master = case.group["scan_payload"]
    target = case.snapshot["work_group_source"]["members"][0]["normalized_barcode"]
    replacement = case.group["item_id"] + "-NEW-GOOD"
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get("_phs2_background_pending"), timeout=5)
    lm.Label_Match._show_scanner_notice(app, "이전 작업 복구", "계속 제품을 스캔하세요.")
    for value in ("KMCMD:EXCHANGE", target, replacement):
        scan(app, value)
    assert app.current_set_info["raw"] == [master, replacement]
    assert app._phs_label_guidance_notice is None
    assert dialogs == []


def test_cancel_capture_failure_is_inline_and_preserves_the_active_set(packaging, monkeypatch):
    app, dialogs = packaging
    scan(app, MASTER)
    before = copy.deepcopy(app.current_set_info)
    monkeypatch.setattr(app, "_cancel_deferred_capture_for_set", lambda *a, **k: (_ for _ in ()).throw(OSError("synthetic capture close unavailable")))
    assert not app._reset_current_set(full_reset=True)
    assert dialogs == []
    assert app.current_set_info == before
    assert app._phs_label_guidance_notice.title == "현재 세트 취소 보류"

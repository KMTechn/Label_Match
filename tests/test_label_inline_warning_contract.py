from __future__ import annotations

from copy import deepcopy

import pytest

import Label_Match as label_match_module
from Label_Match import Label_Match
from package_logistics import PackageLogisticsError


class FakeLabel:
    def __init__(self):
        self.options = {}

    def configure(self, **kwargs):
        self.options.update(kwargs)

    config = configure

    def cget(self, name):
        return self.options.get(name, "")

    def focus_set(self):
        self.options["focused"] = True

    focus_force = focus_set


class RecordingDataManager:
    def __init__(self):
        self.events = []

    def log_event(self, event, details):
        self.events.append((event, details))


def _current(*scans):
    return {
        "id": "set-1",
        "raw": list(scans),
        "parsed": list(scans),
        "start_time": None,
        "error_count": 0,
        "has_error_or_reset": False,
        "phase": None,
        "item_name_override": None,
        "production_date": None,
        "sealed_transfer": None,
        "exact_rescan_active": False,
        "exact_rescan_complete": False,
        "exact_rescan_target_count": 0,
        "exact_rescan_source_bundle_id": "",
        "exact_rescan_barcodes": [],
    }


def _error_app(*scans, workbench_ready=True):
    app = Label_Match.__new__(Label_Match)
    app.current_set_info = _current(*scans)
    app.data_manager = RecordingDataManager()
    app.run_tests = False
    app.is_running_simulation = False
    app.is_blinking = False
    app.status_label = FakeLabel()
    app.big_display_label = FakeLabel()
    app.workflow_notice_frame = object() if workbench_ready else None
    app.workflow_notice_label = FakeLabel() if workbench_ready else None
    app.operator_workbench_ready = workbench_ready
    app._render_calls = []
    app._render_operator_workbench = lambda *args, **kwargs: app._render_calls.append((args, kwargs))
    app._modal_calls = []
    app._trigger_modal_error = lambda *args, **kwargs: app._modal_calls.append((args, kwargs))
    app._play_sound = lambda *args, **kwargs: None
    app._save_current_set_state = lambda: True
    app._update_manual_complete_button_state = lambda: None
    app.update_big_display = lambda text, color="": app.big_display_label.configure(
        text=text,
        color=color,
    )
    app._finalize_calls = []
    def finalize(result, details="", **kwargs):
        app._finalize_calls.append((result, details, deepcopy(app.current_set_info)))
        app.current_set_info = _current()
        return True
    app._finalize_set = finalize
    return app


@pytest.mark.parametrize(
    ("method_name", "args", "expected_text"),
    [
        ("_handle_input_error", ("BAD-SCAN",), "입력 오류"),
        ("_handle_mismatch", ("WRONG-PRODUCT", "MASTER-001"), "제품 불일치"),
    ],
)
def test_error_finishes_the_accepted_scans_and_leaves_nonblocking_guidance(
    method_name,
    args,
    expected_text,
):
    app = _error_app("MASTER-001", "PRODUCT-001", workbench_ready=True)
    accepted_before = deepcopy(app.current_set_info["parsed"])

    getattr(Label_Match, method_name)(app, *args)

    assert app._modal_calls == []
    result, details, recorded = app._finalize_calls[0]
    assert result == (app.Results.FAIL_INPUT_ERROR if method_name == "_handle_input_error" else app.Results.FAIL_MISMATCH)
    assert details == args[0]
    assert recorded["parsed"] == accepted_before and recorded["raw"] == accepted_before
    assert recorded["error_count"] == 1 and recorded["has_error_or_reset"] is True
    assert app.current_set_info["raw"] == []
    assert expected_text in app._phs_label_guidance_notice.title
    assert app._phs_label_guidance_notice.kind == "scanner_notice"
    assert not app.__dict__.get("_workflow_blocking_notice")
    assert app._render_calls


def test_error_uses_legacy_modal_only_before_workbench_exists():
    app = _error_app("MASTER-001", workbench_ready=False)

    Label_Match._handle_input_error(
        app,
        "BAD-SCAN",
        title="[현품표 형식 오류]",
        reason="잘못된 형식",
    )

    assert len(app._modal_calls) == 1
    assert app._render_calls == []


def test_mismatch_notice_restarts_from_the_same_label_after_recording_the_failure():
    app = _error_app("MASTER-001", "PRODUCT-001")

    Label_Match._handle_mismatch(app, "WRONG-PRODUCT", "MASTER-001")

    notice = app._phs_label_guidance_notice
    assert notice.title == "제품 불일치"
    assert notice.message == (
        "현품표와 제품이 불일치합니다.\n"
        "현품표 MASTER-001 · 스캔 제품 WRONG-PRODUCT\n"
        "→ 이 세트는 오류 기록되었습니다. 그 제품을 빼고, 같은 현품표부터 다시 스캔하세요."
    )
    assert app.current_set_info["raw"] == []
    assert app._finalize_calls[0][:2] == (app.Results.FAIL_MISMATCH, "WRONG-PRODUCT")
    assert app.data_manager.events == [
        (app.Events.ERROR_MISMATCH, {
            "raw": "WRONG-PRODUCT", "master": "MASTER-001", "set_id": "set-1", "scan_pos": 3,
        }),
    ]
    assert not app.__dict__.get("_workflow_blocking_notice")


def test_legacy_mismatch_display_restarts_from_the_same_label():
    app = _error_app("MASTER-001", "PRODUCT-001", workbench_ready=False)

    Label_Match._handle_mismatch(app, "WRONG-PRODUCT", "MASTER-001")

    assert app.big_display_label.cget("text") == "제품 불일치 - 같은 현품표부터 다시"
    assert app.status_label.cget("text") == (
        "불일치: 같은 현품표부터 다시 시작 | 스캔 제품: WRONG-PRODUCT"
    )
    args, kwargs = app._modal_calls[0]
    title, message, result, details = args
    assert kwargs == {}
    assert title == "[제품 불일치]"
    assert message.endswith(
        "→ 이 세트는 오류 처리됩니다. 그 제품을 빼고, 같은 현품표부터 다시 스캔하세요."
    )
    assert (result, details) == (app.Results.FAIL_MISMATCH, "WRONG-PRODUCT")


def test_compact_mismatch_notice_preserves_the_same_label_restart_instruction():
    app = _error_app("MASTER-001", "PRODUCT-001")
    Label_Match._handle_mismatch(app, "WRONG-PRODUCT", "MASTER-001")

    compact = app._compact_operator_notice_message(app._phs_label_guidance_notice.message)

    assert compact.splitlines() == [
        "현품표와 제품이 불일치합니다.",
        "현품표 MASTER-001 · 스캔 제품 WRONG-PRODUCT",
        "→ 이 세트는 오류 기록되었습니다. 그 제품을 빼고, 같은 현품표부터 다시 스캔하세요.",
    ]


def test_screen_bridge_keeps_the_same_label_instruction_in_result_and_alert():
    from types import SimpleNamespace

    from label_screen_bridge import _alerts, _result
    from ui.workflow_view_state import WorkflowSnapshot, present_workflow

    app = _error_app("MASTER-001", "PRODUCT-001")
    Label_Match._handle_mismatch(app, "WRONG-PRODUCT", "MASTER-001")
    app._deferred_capture_pending_notice = lambda view: None
    app._deferred_capture_failure_notice = lambda: None
    processor = SimpleNamespace(
        app=app, module=label_match_module, _serial=0, _flash=None, _sound=None, _dialogs=[],
    )
    view = present_workflow(WorkflowSnapshot())
    result = _result(processor, view, {"raw": []})
    alerts = _alerts(processor, view, app._phs_label_guidance_notice)
    expected_tail = (
        "→ 이 세트는 오류 기록되었습니다. 그 제품을 빼고, 같은 현품표부터 다시 스캔하세요."
    )

    assert result["kind"] == "bad" and result["text"] == "제품 불일치"
    assert result["sub"].splitlines()[1] == "현품표 MASTER-001 · 스캔 제품 WRONG-PRODUCT"
    assert result["sub"].endswith(expected_tail)
    assert alerts[0]["text"].endswith(expected_tail)
    assert "choices" not in alerts[0]


@pytest.mark.parametrize("long_values", [False, True], ids=["short", "long"])
def test_mismatch_band_matches_tk_and_bridge_and_preserves_number_suffixes(long_values):
    from types import SimpleNamespace

    from label_screen_bridge import _alerts, _result
    from ui.workflow_view_state import WorkflowSnapshot, present_workflow

    master = "MASTER-" + "0123456789" * 10 + "0000000124" if long_values else "MASTER-001"
    product = "PRODUCT-" + "9876543210" * 10 + "0000000580" if long_values else "WRONG-PRODUCT"
    # The handler already receives the accepted item number, as used by the
    # item header. The original physical input must not replace that number.
    app = _error_app("ORIGINAL-PHYSICAL-LABEL", "ACCEPTED-PRODUCT")
    app.current_set_info["parsed"][0] = master
    Label_Match._handle_mismatch(app, product, master)
    notice = app._phs_label_guidance_notice
    expected = "\n".join((
        "현품표와 제품이 불일치합니다.",
        ("현품표 …67890000000124 · 스캔 제품 …32100000000580" if long_values
         else "현품표 MASTER-001 · 스캔 제품 WRONG-PRODUCT"),
        "→ 이 세트는 오류 기록되었습니다. 그 제품을 빼고, 같은 현품표부터 다시 스캔하세요.",
    ))
    app.workflow_notice_frame = FakeLabel()
    app.workflow_notice_title_label = FakeLabel()
    app.colors = {}
    app._set_workflow_notice_ui(notice, "")
    app._deferred_capture_pending_notice = lambda view: None
    app._deferred_capture_failure_notice = lambda: None
    processor = SimpleNamespace(app=app, module=label_match_module, _serial=0,
                                _flash=None, _sound=None, _dialogs=[])
    result = _result(processor, present_workflow(WorkflowSnapshot()), {"raw": []})
    alerts = _alerts(processor, present_workflow(WorkflowSnapshot()), notice)

    assert notice.message == expected
    assert app.workflow_notice_label.cget("text") == result["sub"] == expected
    assert alerts[0]["text"] == f"제품 불일치 — {expected}"
    assert len(expected.splitlines()) == 3
    assert master[-14:] in expected and product[-14:] in expected
    assert "ORIGINAL-PHYSICAL-LABEL" not in expected
    assert app.current_set_info["raw"] == []
    assert app._finalize_calls[0][:2] == (app.Results.FAIL_MISMATCH, product)
    assert app._finalize_calls[0][2]["parsed"][0] == master
    assert app.data_manager.events[0][1]["master"] == master
    if not long_values:
        # The boundary is a value-length rule, independent of the barcode's
        # hyphens or the renderer's wrapping rules.
        for value, display in (("12345678901234", "12345678901234"),
                               ("A12345678901234", "…12345678901234")):
            boundary = _error_app("ORIGINAL-RAW")
            Label_Match._handle_mismatch(boundary, value, value)
            assert boundary._phs_label_guidance_notice.message.splitlines()[1] == (
                f"현품표 {display} · 스캔 제품 {display}")


@pytest.mark.parametrize(("message", "expected"), [
    ("이미 처리된 제품입니다.\n설명\n- 중복 제품: PRODUCT-001\n다른 제품을 스캔하세요.",
     "이미 처리된 제품입니다.\n- 중복 제품: PRODUCT-001\n다른 제품을 스캔하세요."),
    ("중복 스캔입니다.\n설명\n- 중복 스캔: PRODUCT-002\n다른 제품을 스캔하세요.",
     "중복 스캔입니다.\n- 중복 스캔: PRODUCT-002\n다른 제품을 스캔하세요."),
    ("입력 오류입니다.\n설명\n- 입력: BAD-003\n입력을 확인하세요.",
     "입력 오류입니다.\n- 입력: BAD-003\n입력을 확인하세요."),
    ("사유\n값\n\n다음 행동", "사유\n값\n다음 행동"),
    # A mismatch-shaped body alone is not a mismatch marker.
    ("현품표와 제품이 불일치합니다.\n- 현품표: MASTER-001\n- 스캔 제품: PRODUCT-999\n다음 행동",
     "현품표와 제품이 불일치합니다.\n- 스캔 제품: PRODUCT-999\n다음 행동"),
], ids=["duplicate-product", "duplicate-scan", "input-error", "four-physical-lines", "body-without-marker"])
def test_other_notice_compaction_bytes_are_unchanged(message, expected):
    app = _error_app("MASTER-001")
    assert app._compact_operator_notice_message(message).encode("utf-8") == expected.encode("utf-8")


def test_non_mismatch_failure_keeps_its_notice_body_bytes():
    app = _error_app("MASTER-001")
    message = "입력 오류입니다.\n\n- 입력: BAD-003\n\n입력을 확인하세요."
    assert app._present_inline_workflow_error("[입력 오류]", message, app.Results.FAIL_INPUT_ERROR, "BAD-003")
    assert app._phs_label_guidance_notice.message.encode("utf-8") == message.encode("utf-8")


@pytest.mark.parametrize("key", ["Return", "Escape"])
def test_enter_and_escape_acknowledge_inline_error_before_failed_set_is_reset(key):
    app = _error_app("MASTER-001", "PRODUCT-001", workbench_ready=True)
    app._pending_workflow_error = {
        "result": app.Results.FAIL_INPUT_ERROR,
        "error_details": "BAD-SCAN",
    }
    app._workflow_notice = object()
    app._finalize_calls = []
    app._finalize_set = lambda result, error_details="", **kwargs: app._finalize_calls.append(
        (result, error_details, kwargs)
    )
    accepted_before = deepcopy(app.current_set_info["parsed"])
    event = type("FakeEvent", (), {"keysym": key})()

    handled = Label_Match._acknowledge_workflow_notice(app, event)

    assert handled == "break"
    assert app.current_set_info["parsed"] == accepted_before
    assert app._finalize_calls == [(app.Results.FAIL_INPUT_ERROR, "BAD-SCAN", {})]


def test_durable_completion_clears_transient_status_and_returns_to_idle():
    app = _error_app(
        "MASTER-001",
        "PRODUCT-001",
        "PRODUCT-002",
        "PRODUCT-003",
        "FINAL-001",
        workbench_ready=True,
    )
    app._workflow_completion_kind = "full"
    app._workflow_display_scans = tuple(app.current_set_info["raw"])
    app._workflow_display_parsed_scans = tuple(app.current_set_info["parsed"])
    app._workflow_display_central_inherit_all = True
    app._workflow_display_active_label_qr = "PHS2"
    app._workflow_last_normal_override = "FINAL-001"
    app._workflow_item_snapshot = {"set_id": "set-1"}
    app._workflow_blocking_notice = object()
    app._workflow_notice = object()
    app._workflow_notice_action = lambda: None
    app._workflow_notice_action_text = "재시도"
    app._phs_label_guidance_notice = object()
    app._pending_workflow_error = {"result": app.Results.FAIL_INPUT_ERROR}
    app._workflow_pending_error = {"result": app.Results.FAIL_INPUT_ERROR}
    app._workflow_error_message = "old error"
    app._workflow_recovered = True
    reset_calls = []
    refresh_calls = []
    app._reset_current_set = lambda **kwargs: reset_calls.append(kwargs) or True
    app._refresh_session_tree = lambda: refresh_calls.append(True)

    assert Label_Match._return_to_idle_after_finalized_set(app) is True
    assert reset_calls == [{"from_finalize": True}]
    assert refresh_calls == [True]
    assert app._workflow_completion_kind is None
    assert app._workflow_display_scans == ()
    assert app._workflow_blocking_notice is None
    assert app._workflow_notice is None
    assert app._workflow_notice_action is None
    assert app._workflow_notice_action_text == "확인"
    assert app._phs_label_guidance_notice is None
    assert app._pending_workflow_error is None
    assert app._workflow_pending_error is None
    assert app._workflow_error_message == ""
    assert app._workflow_recovered is False
    assert app.big_display_label.cget("text") == ""


def test_submission_block_keeps_all_five_scans_and_offers_retry_without_reset(
    monkeypatch, capsys
):
    scans = ["MASTER-001", "PRODUCT-001", "PRODUCT-002", "PRODUCT-003", "FINAL-001"]
    app = _error_app(*scans, workbench_ready=True)
    app.items_data = {}
    app._queue_authoritative_package = lambda **kwargs: (_ for _ in ()).throw(
        PackageLogisticsError("서버 ACK 확인 필요")
    )
    app._finalize_calls = []
    monkeypatch.setattr(label_match_module.messagebox, "showerror", lambda *args, **kwargs: pytest.fail("workbench error must stay inline"))

    completed = Label_Match._finalize_set(app, app.Results.PASS)

    assert completed is False
    assert app.current_set_info["raw"] == scans
    assert app.current_set_info["parsed"] == scans
    assert "5/5 유지" in app._workflow_blocking_notice.title
    worker_message = app.workflow_notice_label.cget("text")
    assert "로컬 완료 기록을 안전하게 저장하지 못했습니다" in worker_message
    assert "서버 ACK 확인 필요" not in worker_message
    assert "서버 ACK 확인 필요" in capsys.readouterr().out
    assert app._workflow_notice_action_text == "저장 재시도"
    assert callable(app._workflow_notice_action)

    original = deepcopy(app.current_set_info)
    app._finalize_set = lambda result, error_details="", **kwargs: app._finalize_calls.append(
        (result, error_details, kwargs)
    )
    handled = Label_Match._retry_blocked_submission(app)

    assert handled is True
    assert app.current_set_info == original
    assert app._finalize_calls == [(app.Results.PASS, "", {})]

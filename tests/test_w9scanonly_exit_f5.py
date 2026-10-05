"""LM-A1/LM-B4: idle exit and exception-only date exchange (10-02 decision)."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import Label_Match as lm
from tests.test_label_operator_workbench import FakeWidget, operator_workbench  # noqa: F401


def _closing_app(monkeypatch, current, *, answer=True):
    app = lm.Label_Match.__new__(lm.Label_Match)
    app.initialized_successfully = True
    app.run_tests = False
    app.current_set_info = current
    app.is_blinking = False
    app._has_background_work = lambda: False
    app._release_shortage_confirmation = Mock(return_value=True)
    app._save_current_set_state = Mock(return_value=True)
    app._suspend_deferred_validation_for_close = Mock()
    app._cancel_app_close_lane_drain_watchdog = Mock()
    app._show_ui_lane_rejection = Mock()
    app._cancel_pending_ui_jobs = Mock()
    app._begin_app_close_direct_sync = Mock()
    app.direct_sync_bootstrap_context = {"scan_source_dir": "synthetic"}
    app.data_manager = SimpleNamespace(log_event=Mock(), close=Mock())
    def choose_inline(_title, _message, _text, action):
        if answer:
            action()
    question = Mock(side_effect=choose_inline)
    app._request_pointer_confirmation = question
    monkeypatch.setattr(lm.messagebox, "askokcancel", Mock(side_effect=AssertionError("native exit approval forbidden")))
    return app, question


@pytest.mark.parametrize("queued", [False, True], ids=["empty", "durable-outbox-only"])
def test_idle_exit_skips_question_and_runs_normal_cleanup(monkeypatch, queued):
    app, question = _closing_app(monkeypatch, {"raw": []})
    if queued:
        app.package_outbox = SimpleNamespace(pending_count=3)
        app._deferred_validation_after_id = "pending-durable-retry"
    app.on_closing()
    question.assert_not_called()
    app._save_current_set_state.assert_called_once()
    app.data_manager.log_event.assert_called_once()
    assert app.data_manager.log_event.call_args.args[0] == app.Events.APP_CLOSE
    app.data_manager.close.assert_called_once()
    app._cancel_pending_ui_jobs.assert_called_once()
    app._begin_app_close_direct_sync.assert_called_once()


@pytest.mark.parametrize("answer", [True, False], ids=["exit", "continue"])
@pytest.mark.parametrize("current", [
    {"raw": ["MASTER"]},
    {"raw": ["MASTER", "PRODUCT"], "scanner_exchange_phase": "replacement"},
    {"raw": [], "shortage_undo_snapshot": {"raw": ["MASTER", "PRODUCT"]}},
], ids=["partial-set", "exchange", "shortage-undo"])
def test_unfinished_exit_asks_once_and_preserves_declined_work(monkeypatch, current, answer):
    app, question = _closing_app(monkeypatch, current, answer=answer)
    app.on_closing()
    question.assert_called_once()
    assert app.current_set_info == current
    assert app._begin_app_close_direct_sync.call_count == int(answer)
    assert app._release_shortage_confirmation.call_count == int(answer)
    assert app.data_manager.close.call_count == int(answer)


def test_pending_date_exchange_exit_asks_once(monkeypatch):
    app, question = _closing_app(monkeypatch, {"raw": []}, answer=False)
    app.phs_label_exchange_coordinator = SimpleNamespace(
        has_pending_reconciliation=lambda: True, blocks_other_action=lambda current: False,
    )
    app.on_closing()
    question.assert_called_once()
    app.data_manager.close.assert_not_called()


@pytest.mark.parametrize("state", ['_phs_label_scan_lookup_in_progress', '_phs_label_exchange_pending', '_scanner_shortage_undo'])
def test_transient_work_without_raw_still_protects_declined_exit(monkeypatch, state):
    app, question = _closing_app(monkeypatch, {"raw": []}, answer=False)
    setattr(app, state, {"raw": ["MASTER"]} if state == "_scanner_shortage_undo" else True)
    app.on_closing()
    question.assert_called_once()
    app._release_shortage_confirmation.assert_not_called()
    app.data_manager.close.assert_not_called()


def test_normal_screen_has_no_f5_action_or_key(operator_workbench):
    app = operator_workbench
    assert "phs_label_exchange_button" not in app.__dict__
    assert all(sequence != "<F5>" for sequence, *_ in app._root_bindings)
    for widget in app.operator_action_frame.children:
        if widget.winfo_ismapped():
            assert "F5" not in str(widget.cget("text") or "")
    assert "F5" not in repr(app._render_operator_workbench())


@pytest.mark.parametrize("mode", ["none", "single", "single-with-reconciliation-api", "reconciliation"])
def test_exception_recovery_resumes_saved_exchange_without_fresh_lookup(mode):
    app = lm.Label_Match.__new__(lm.Label_Match)
    app.current_set_info = {"raw": []}
    app.run_tests = True
    app._phs_label_exchange_enabled_for_current = lambda: True
    app._sealed_transfer_exchange_blocks_local_action = lambda action: False
    app._show_scanner_notice = Mock()
    app._focus_scan_entry_if_available = Mock()
    app._start_phs_label_recovery = Mock()
    app._start_phs_reconciliation_recovery = Mock()
    app.phs_label_exchange_coordinator = SimpleNamespace(
        reconciliation_available=lambda: mode != "single",
        has_pending_reconciliation=lambda: mode == "reconciliation",
        blocks_other_action=lambda current: mode.startswith("single"),
        journal=SimpleNamespace(load=lambda: {"status": "PRINT_FAILED", "failed_target_label_id": "saved-target"}),
    )
    assert app._recover_phs_label_exchange() == "break"
    if mode.startswith("single"):
        app._start_phs_label_recovery.assert_called_once_with()
    elif mode == "reconciliation":
        app._start_phs_reconciliation_recovery.assert_called_once_with(
            retry_failed_target_ids={"saved-target"}, confirm_ambiguous_reprint_target_ids=set(),
        )
    else:
        app._start_phs_label_recovery.assert_not_called()
        app._start_phs_reconciliation_recovery.assert_not_called()


def test_exception_workbench_exposes_recovery_only_action(operator_workbench, monkeypatch):
    app = operator_workbench
    app.run_tests = False
    app._package_recovery_candidates = lambda: []
    app._recover_phs_label_exchange = Mock()
    window = FakeWidget(kind="exception-window")
    window.title = Mock()
    monkeypatch.setattr(lm.tk, "Toplevel", lambda parent: window)
    monkeypatch.setattr(lm.tk, "StringVar", lambda **kwargs: SimpleNamespace(set=Mock()))
    app._show_package_recovery_workbench()
    pending = [window]
    buttons = []
    while pending:
        widget = pending.pop()
        pending.extend(widget.children)
        if widget.cget("text") == "미완료 현품표 교환 복구":
            buttons.append(widget)
    assert len(buttons) == 1
    buttons[0].cget("command")()
    app._recover_phs_label_exchange.assert_called_once_with()

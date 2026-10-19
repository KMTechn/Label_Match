from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from tests.test_app_stop import isolated_stop_process_observation

import Label_Match as lm
import user_relay
from tests.test_f6_pointer_confirmation import app, click
from tests.test_label_operator_action_gates import FakeWidget
from tests.test_label_ui_lane_recovery import recovery_app
from tests.test_label_ui_lane_integration import _close_test_lanes_even_after_assertion_failure
from tests.test_w9scanonly_packaging import packaging, scan, products, completions
from tests.test_legacy_label_transition import MASTER
from tk_serial_ui_lane import LaneState
from ui.workflow_view_state import WorkflowNotice


def _request(instance, tmp_path):
    root = tmp_path / "relay-root"
    instance.direct_sync_bootstrap_context = {"program_data_root": str(root)}
    user_relay.request_app_stop(root)
    return user_relay.app_stop_request_path(root)


def test_active_request_is_recorded_once_and_does_not_close_after_work_finishes(app, tmp_path):
    path = _request(app, tmp_path)
    before = deepcopy(app.current_set_info)
    for _ in range(3):
        assert app._process_app_stop_request() is False
    assert app.current_set_info == before
    app.data_manager.log_event.assert_called_once_with(
        "멈춤 요청 미룸", {"request_id": app._app_stop_deferred})
    app._save_current_set_state.assert_called_once()
    app.data_manager.close.assert_not_called()
    app._begin_app_close_direct_sync.assert_not_called()
    assert path.is_file()
    app.current_set_info["raw"] = []
    assert app._process_app_stop_request() is False
    app.data_manager.close.assert_not_called()
    assert path.is_file()


@pytest.mark.parametrize("failure", ["log", "checkpoint", "checkpoint-false"])
def test_recording_failure_keeps_work_open_and_does_not_retry_or_block_scans(app, tmp_path, failure):
    path = _request(app, tmp_path)
    if failure == "log":
        app.data_manager.log_event.side_effect = OSError()
    elif failure == "checkpoint":
        app._save_current_set_state.side_effect = OSError()
    else:
        app._save_current_set_state.return_value = False
    assert app._process_app_stop_request() is False
    assert app._process_app_stop_request() is False
    app.process_input = Mock()
    app.entry.insert(0, "NEXT-SCAN")
    app._handle_scan_enter()
    app.process_input.assert_called_once()
    assert app.data_manager.log_event.call_count == 1
    app.data_manager.close.assert_not_called()
    assert path.is_file()


@pytest.mark.parametrize("activity", ["writer", "held-scans", "pin", "exchange", "shortage", "simulation", "test-logs"])
def test_existing_activity_with_no_raw_scans_still_defers(app, tmp_path, activity):
    app.current_set_info["raw"] = []
    if activity == "writer":
        app.ui_lane = SimpleNamespace(state=LaneState.BUSY, is_busy=lambda: True)
    elif activity == "held-scans":
        app._scans_behind_completion = ["HELD-SCAN"]
    elif activity == "exchange":
        app.current_set_info["scanner_exchange_phase"] = "replacement"
    elif activity == "shortage":
        app._scanner_shortage_undo = {"pending": True}
    elif activity in {"simulation", "test-logs"}:
        app._has_background_work = lm.Label_Match._has_background_work.__get__(app)
        setattr(app, "is_running_simulation" if activity == "simulation" else "is_generating_test_logs", True)
    marker = lm.active_pin_operation.set(object()) if activity == "pin" else None
    try:
        path = _request(app, tmp_path)
        assert app._process_app_stop_request() is False
        app.data_manager.close.assert_not_called()
        assert path.is_file()
        if activity in {"simulation", "test-logs"}:
            setattr(app, "is_running_simulation" if activity == "simulation" else "is_generating_test_logs", False)
            assert app._process_app_stop_request() is False
            app.data_manager.close.assert_not_called()
    finally:
        if marker is not None:
            lm.active_pin_operation.reset(marker)


def test_deferred_notice_survives_renders_and_scanner_enter_without_stealing_focus(recovery_app, tmp_path):
    instance, _root, typing = recovery_app
    instance._save_current_set_state = Mock(return_value=True)
    instance.on_closing = Mock()
    instance.workflow_notice_action_button = FakeWidget()
    path = _request(instance, tmp_path)
    assert instance._process_app_stop_request() is False
    phrase = "관리자가 프로그램 종료를 요청했습니다. 지금 트레이를 마친 뒤 [종료] 를 누르세요."
    for notice in (None, WorkflowNotice("안내", "다음 세트를 계속하세요.")):
        instance._phs_label_guidance_notice = notice
        instance._render_operator_workbench()
        assert instance.workflow_notice_label.options["text"].count(phrase) == 1
        assert instance.workflow_notice_action_button.options["text"] == "종료"
        assert instance.workflow_notice_action_button.options["takefocus"] is False
        assert not instance.workflow_notice_action_button.focused
        assert instance.entry.options["state"] == "normal"
    typing[0] = "NEXT-SCAN"
    instance.process_input = Mock()
    instance._handle_scan_enter()
    instance.process_input.assert_called_once()
    instance.on_closing.assert_not_called()
    instance.current_set_info["raw"] = []
    instance.current_set_info["parsed"] = []
    instance._render_operator_workbench()
    assert instance.workflow_notice_label.options["text"].count(phrase) == 1
    assert instance._process_app_stop_request() is False
    instance.on_closing.assert_not_called()
    instance._acknowledge_workflow_notice()
    instance.on_closing.assert_called_once_with()
    assert path.is_file()


@pytest.mark.parametrize("keysym", ["Return", "KP_Enter"])
@pytest.mark.parametrize("work_finished", [False, True])
def test_deferred_stop_keyboard_enter_never_requests_close(recovery_app, tmp_path, keysym, work_finished):
    instance, _root, typing = recovery_app
    instance._save_current_set_state = Mock(return_value=True)
    instance.on_closing = Mock()
    instance.workflow_notice_action_button = FakeWidget()
    path = _request(instance, tmp_path)
    assert instance._process_app_stop_request() is False
    deferred = instance._app_stop_deferred
    if work_finished:
        instance.current_set_info["raw"] = []
        instance.current_set_info["parsed"] = []
    assert not instance.__dict__.get("_pending_workflow_error")
    assert not instance.__dict__.get("_workflow_pending_error")
    assert not instance.__dict__.get("_workflow_notice_action")
    event = SimpleNamespace(keysym=keysym, widget=instance.workflow_notice_action_button)
    assert instance._pointer_confirmation_key(event) == "break"
    assert instance._acknowledge_workflow_notice(event) == "break"
    typing[0] = ""
    instance.process_input = Mock()
    event.widget = instance.entry
    entered_scan = typing[0]
    instance._handle_scan_enter(event)
    instance.process_input.assert_called_once_with(event, raw=entered_scan)
    assert typing[0] == ""
    instance.on_closing.assert_not_called()
    assert instance._app_stop_deferred == deferred and path.is_file()
    # The button command still begins the existing local consent flow.
    instance._acknowledge_workflow_notice()
    instance.on_closing.assert_called_once_with()


def test_existing_notice_action_keeps_priority_over_exit_and_preserves_pointer_identity(recovery_app, tmp_path):
    instance, _root, _typing = recovery_app
    instance._save_current_set_state = Mock(return_value=True)
    instance.on_closing = Mock()
    instance.workflow_notice_action_button = FakeWidget()
    notice = WorkflowNotice("현장 확인", "기존 확인을 진행하세요.")
    action = Mock()
    instance._phs_label_guidance_notice = notice
    instance._workflow_notice_action = action
    instance._workflow_notice_action_text = "기존 확인"
    instance._pointer_confirmation = (notice, action)
    _request(instance, tmp_path)
    instance._process_app_stop_request()
    assert instance._pointer_confirmation == (notice, action)
    assert instance.workflow_notice_action_button.options["text"] == "기존 확인"
    instance._acknowledge_workflow_notice()
    action.assert_called_once()
    instance.on_closing.assert_not_called()


def test_local_exit_keeps_request_until_normal_destroy_and_same_id_cleanup(app, tmp_path, monkeypatch):
    stop_path = Mock(wraps=user_relay.app_stop_request_path)
    monkeypatch.setattr(user_relay, "app_stop_request_path", stop_path)
    path = _request(app, tmp_path)
    stop_path.reset_mock()
    assert app._process_app_stop_request() is False
    app._acknowledge_workflow_notice()
    assert app._pointer_confirmation is not None
    app.data_manager.close.assert_not_called()
    assert path.is_file()
    click(app)
    app.data_manager.close.assert_called_once()
    app._begin_app_close_direct_sync.assert_called_once()
    assert path.is_file()
    app._stop_error_siren = Mock()
    app._join_tk_shutdown_threads = Mock(return_value=[])
    app._close_data_manager_before_tk_destroy = Mock(return_value=True)
    monkeypatch.setattr(lm.tk.Tk, "destroy", lambda _self: None)
    app.destroy()
    assert app._tk_destroy_complete is True
    assert not path.exists()
    assert stop_path.call_count >= 2
    assert all(call.args == (app.direct_sync_bootstrap_context["program_data_root"],)
               for call in stop_path.call_args_list)


@pytest.mark.parametrize("status", ["PRINT_PENDING", "PREPARE_PENDING"])
def test_exchange_journal_without_raw_scans_defers_using_shared_close_predicate(app, tmp_path, status):
    from phs_label_workflow import PHSLabelExchangeCoordinator

    app.current_set_info["raw"] = []
    app.current_set_info["id"] = "exchange-in-progress"
    coordinator = PHSLabelExchangeCoordinator.__new__(PHSLabelExchangeCoordinator)
    coordinator.journal = SimpleNamespace(load=lambda: {
        "status": status, "set_id": app.current_set_info["id"],
        "canonical_input_tag_qr": ""})
    app.phs_label_exchange_coordinator = coordinator
    assert app._app_close_needs_confirmation()
    path = _request(app, tmp_path)
    assert app._process_app_stop_request() is False
    app._save_current_set_state.assert_called_once()
    app.data_manager.close.assert_not_called()
    assert path.is_file()


@pytest.mark.parametrize("destroy_error", [False, True])
def test_cleanup_keeps_successor_or_request_if_normal_destroy_has_not_completed(
    app, tmp_path, monkeypatch, destroy_error
):
    path = _request(app, tmp_path)
    assert app._process_app_stop_request() is False
    if not destroy_error:
        user_relay.request_app_stop(path.parent.parent)
    app._stop_error_siren = Mock()
    app._join_tk_shutdown_threads = Mock(return_value=[])
    app._close_data_manager_before_tk_destroy = Mock(return_value=True)
    native = Mock(side_effect=RuntimeError()) if destroy_error else Mock()
    monkeypatch.setattr(lm.tk.Tk, "destroy", native)
    if destroy_error:
        with pytest.raises(RuntimeError):
            app.destroy()
        assert not app.__dict__.get("_tk_destroy_complete", False)
    else:
        app.destroy()
        assert app._tk_destroy_complete is True
    assert path.is_file()


def test_active_packaging_keeps_real_product_scans_and_completion_after_remote_request(packaging, tmp_path):
    instance, _dialogs = packaging
    scan(instance, MASTER)
    original_id = instance.current_set_info["id"]
    instance.on_closing = Mock()
    path = _request(instance, tmp_path)
    assert instance._process_app_stop_request() is False
    for product in products():
        scan(instance, product)
    assert len(completions(instance)) == 1
    assert completions(instance)[0][1]["set_id"] == original_id
    assert instance.current_set_info["raw"] == []
    assert instance._process_app_stop_request() is False
    instance.on_closing.assert_not_called()
    assert path.is_file()

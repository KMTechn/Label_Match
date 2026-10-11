from copy import deepcopy
from datetime import datetime
import json
import threading
from types import MethodType, SimpleNamespace
from unittest.mock import Mock

import pytest

from package_logistics import PackageOutbox
from tests.test_label_operator_action_gates import FakeWidget, _render_app
from tests.test_label_ui_lane_integration import _close_test_lanes_even_after_assertion_failure
from tests.test_tk_serial_ui_lane import FakeTkRoot
from tests.test_w9scanonly_packaging import packaging, scan, completions
from tk_serial_ui_lane import LaneState, LaneTask, TkSerialUiLane, _ResultEnvelope


@pytest.fixture
def recovery_app(tmp_path):
    app = _render_app()
    root = FakeTkRoot()
    app.after, app.after_cancel, app.run_until = root.after, root.after_cancel, root.run_until
    app._ui_lane_generation = 0
    app._app_close_in_progress = False
    app._tk_shutdown_requested = False
    app._ui_lane_busy_task = app._ui_lane_busy_label = ""
    app._update_status_label = Mock()
    app.update_big_display = Mock()
    app._idle_instruction_text = lambda: "현품표를 스캔하세요."
    app.is_blinking = False
    app.progress_bar = {}
    app.current_set_info.update(id="lane-flow-1", raw=["MASTER", "PRODUCT"], parsed=["ITEM", "ITEM"])
    app.current_set_info["start_time"] = datetime.now()
    for name in ("workflow_notice_title_label", "workflow_notice_label", "workflow_notice_frame"):
        setattr(app, name, FakeWidget())
    app.package_outbox = PackageOutbox(tmp_path / "outbox.sqlite3")
    app.data_manager = SimpleNamespace(save_directory=str(tmp_path),
        _current_state_filename=lambda: "_current_set_state_packaging.json", log_event=Mock())
    typing = [""]
    app.entry.get = lambda: typing[0]
    app.entry.delete = lambda *_args: typing.__setitem__(0, "")
    app._play_sound = Mock()
    app.ui_lane = TkSerialUiLane(root, poll_ms=1,
        generation_provider=lambda: app._ui_lane_generation,
        on_runner_fault=app._handle_ui_lane_fault)
    return app, root, typing


def _checkpoint(app):
    from pathlib import Path
    path = Path(app._package_current_state_path())
    data = json.dumps({"current_set_info": app.current_set_info}, ensure_ascii=False,
                      default=lambda value: value.isoformat()).encode()
    path.write_bytes(data)
    return path, data


def _finish_fault(app, root, *, work=lambda: None, settle=None):
    def finish(_value):
        raise RuntimeError("injected UI failure")
    old = app.ui_lane
    admitted = app._submit_ui_lane_task(name="flow-test", busy_text="처리 중",
        work=work, finish=finish, fail=pytest.fail, settle=settle)
    assert admitted.accepted
    root.run_until(lambda: old.state in {LaneState.BROKEN, LaneState.CLOSED})
    return old


def test_fault_quarantines_original_before_new_lane_and_accepts_next_scan(recovery_app, monkeypatch):
    app, root, typing = recovery_app
    original = json.loads(json.dumps(app.current_set_info, default=lambda value: value.isoformat()))
    path, raw = _checkpoint(app)
    typing[0] = "queued scan"
    app._scans_behind_completion = ["held scan"]
    app._workflow_completion_kind = "full"
    app._workflow_display_scans = tuple(original["raw"])
    app._workflow_last_normal_override = {"result": "PASS"}
    sequence = []
    audit_text = []
    audit, reset = app._audit_package_recovery_action, app._reset_current_set
    old = app.ui_lane
    close, hold = old.close_idle, app.package_outbox.hold_workbench_set
    def record(*args):
        audit(*args)
        audit_text.append(args[-1])
        sequence.append("recorded")
    def clear():
        assert not old.worker_thread.is_alive()
        assert app.package_outbox.get_workbench_hold(original["id"])
        sequence.append("stopped-and-held")
        return reset()
    def close_old():
        assert app.package_outbox.get_workbench_hold(original["id"]) is None
        close()
        assert old.state is LaneState.CLOSED and not old.worker_thread.is_alive()
        sequence.append("close")
    def hold_original(**values):
        assert old.state is LaneState.CLOSED and not old.worker_thread.is_alive()
        sequence.append("hold")
        return hold(**values)
    monkeypatch.setattr(app, "_audit_package_recovery_action", record)
    monkeypatch.setattr(app, "_reset_current_set", clear)
    monkeypatch.setattr(old, "close_idle", close_old)
    monkeypatch.setattr(app.package_outbox, "hold_workbench_set", hold_original)
    old = _finish_fault(app, root)

    assert "격리된 체크포인트가 비교 원본" in app._ui_lane_review_notice.message
    assert "parked 상태는 작업 제출 때 스냅숏" in app._ui_lane_review_notice.message
    assert old.state is LaneState.CLOSED and not old.worker_thread.is_alive()
    assert app.ui_lane is not old and app.ui_lane.state is LaneState.IDLE
    assert app._ui_lane_generation == 1
    assert app._workflow_completion_kind is None
    assert app._workflow_display_scans == ()
    assert app._workflow_last_normal_override is None
    hold = app.package_outbox.get_workbench_hold(original["id"])
    saved = json.loads(hold["snapshot_json"])
    assert saved["current_set_info"] == original
    assert saved["ui_lane_fault"]["result"] == "UNKNOWN"
    assert saved["ui_lane_fault"]["task"] == "flow-test"
    assert saved["ui_lane_fault"]["generation"] == 0
    assert saved["ui_lane_fault"]["barcodes"] == original["raw"]
    assert app.package_outbox.has_workbench_audit(set_id=original["id"], action="UI_LANE_FAULT",
        observed=audit_text[0])
    assert json.loads(audit_text[0]) == saved["ui_lane_fault"]
    assert sequence == ['recorded', 'close', 'hold', 'stopped-and-held']
    assert saved["held_scans"] == ["held scan"] and saved["entry_input"] == "queued scan"
    from pathlib import Path
    assert Path(saved["archive_path"]).read_bytes() == raw and not path.exists()
    assert app.package_outbox.workbench_hold_for_source("MASTER", "")["set_id"] == original["id"]
    assert app._deferred_validation_lane_trigger._lane is app.ui_lane
    assert app._render_operator_workbench().scan_input_enabled
    assert "관리자 확인" in app.workflow_notice_title_label.options["text"]
    typing[0] = "next tray"
    app.process_input = Mock()
    app._handle_scan_enter()
    app.process_input.assert_called_once()
    completed = Mock()
    assert app._submit_ui_lane_task(name="next-set", busy_text="처리 중",
        work=lambda: 2, finish=completed, fail=pytest.fail).accepted
    root.run_until(lambda: not app.ui_lane.is_busy())
    completed.assert_called_once_with(2)


def test_old_broken_result_cannot_settle_or_render_after_recovery(recovery_app):
    app, root, _typing = recovery_app
    _checkpoint(app)
    settled = Mock()
    old = _finish_fault(app, root, settle=settled)
    settled.reset_mock()
    task = old.fault_task
    before = deepcopy(app.current_set_info)
    old._apply_result(_ResultEnvelope(100, "success", 1, 0, task, value="late"))
    assert app.current_set_info == before
    assert app.ui_lane.state is LaneState.IDLE
    settled.assert_not_called()


def test_live_old_worker_prevents_restart_and_hold_but_keeps_original_and_audit(recovery_app, monkeypatch):
    app, root, _typing = recovery_app
    path, raw = _checkpoint(app)
    set_id = app.current_set_info["id"]
    old = app.ui_lane
    old._worker_join_timeout = 0.1
    gate = threading.Event()
    audit_calls = []
    audit = app._audit_package_recovery_action
    def record(*args):
        audit(*args)
        audit_calls.append(args)
    monkeypatch.setattr(app, "_audit_package_recovery_action", record)
    assert app._submit_ui_lane_task(name="held-writer", busy_text="처리 중",
        work=lambda: gate.wait(5), finish=lambda _value: None, fail=pytest.fail).accepted
    try:
        old._break_lane(RuntimeError("injected pump fault"))
        assert app.ui_lane is old and old.state is LaneState.BROKEN
        assert old.worker_thread.is_alive() and app._ui_lane_generation == 0
        assert path.read_bytes() == raw
        assert app.package_outbox.get_workbench_hold(set_id) is None
        assert len(audit_calls) == 1
        assert json.loads(audit_calls[0][-1])["result"] == "UNKNOWN"
        assert app.package_outbox.has_workbench_audit(
            set_id=set_id, action=audit_calls[0][1], observed=audit_calls[0][-1])
        app._render_operator_workbench()
        assert app.entry.options["state"] == "disabled"
    finally:
        gate.set()


@pytest.mark.parametrize("boundary", ["audit", "hold", "archive"])
def test_fault_persistence_failure_keeps_input_stopped_and_original(recovery_app, monkeypatch, boundary):
    app, root, _typing = recovery_app
    path, raw = _checkpoint(app)
    app._scans_behind_completion = ["NEXT-HELD"]
    app.entry.get = lambda: "NEXT-PARTIAL"
    if boundary == "audit":
        monkeypatch.setattr(app.package_outbox, "audit_workbench_action", Mock(side_effect=OSError()))
    elif boundary == "hold":
        monkeypatch.setattr(app.package_outbox, "hold_workbench_set", Mock(side_effect=OSError()))
    else:
        monkeypatch.setattr(app, "_move_package_pin_file", Mock(return_value=False))
    old = _finish_fault(app, root)
    assert app.ui_lane is old and app._ui_lane_generation == 0
    assert path.read_bytes() == raw
    assert app._scans_behind_completion == ["NEXT-HELD"]
    assert app.entry.get() == "NEXT-PARTIAL"
    app._render_operator_workbench()
    assert app.entry.options["state"] == "disabled"


@pytest.mark.parametrize("same_set", [False, True])
def test_repeat_faults_stop_at_third_in_ten_minutes_or_second_same_set(recovery_app, same_set):
    app, root, _typing = recovery_app
    stop_at = 2 if same_set else 3
    for number in range(stop_at):
        app.current_set_info = {"id": "lane-flow-1" if same_set else f"lane-flow-{number}",
                                "raw": [f"MASTER-{number}"], "parsed": ["ITEM"]}
        path, raw = _checkpoint(app)
        held_before = deepcopy(app.package_outbox.get_workbench_hold(app.current_set_info["id"]))
        old = _finish_fault(app, root)
        if number + 1 < stop_at:
            assert app.ui_lane is not old
        else:
            assert app.ui_lane is old and old.state is LaneState.BROKEN
            assert path.read_bytes() == raw
            assert app.package_outbox.get_workbench_hold(app.current_set_info["id"]) == held_before
    assert len(app._ui_lane_faults) == stop_at
    app._render_operator_workbench()
    assert app.entry.options["state"] == "disabled"


def test_closing_fault_creates_no_hold_and_preserves_current_checkpoint(recovery_app):
    app, root, _typing = recovery_app
    path, raw = _checkpoint(app)
    current = deepcopy(app.current_set_info)
    try:
        old = _finish_fault(app, root, work=lambda: setattr(app, "_app_close_in_progress", True))
        assert app.ui_lane is old and old.state is LaneState.BROKEN
        assert app._ui_lane_generation == 0
        assert app.current_set_info == current and path.read_bytes() == raw
        assert app.package_outbox.get_workbench_hold(current["id"]) is None
    finally:
        app._app_close_in_progress = False


def test_expired_faults_do_not_prevent_a_single_recovery(recovery_app):
    import time
    app, root, _typing = recovery_app
    _checkpoint(app)
    app._ui_lane_faults = [(time.monotonic() - 601, "old-set")] * 3
    old = _finish_fault(app, root)
    assert app.ui_lane is not old and len(app._ui_lane_faults) == 1


@pytest.mark.parametrize("mismatch", ["generation", "set-id", "offscreen"])
def test_unmatched_request_cannot_clear_unsaved_current_input(recovery_app, mismatch):
    app, root, typing = recovery_app
    old = app.ui_lane
    original_id = app.current_set_info["id"]
    typing[0] = "PARTIAL-NEXT"
    def finish(_value):
        if mismatch == "generation":
            app._ui_lane_generation += 1
        elif mismatch == "set-id":
            app.current_set_info["id"] = "unsaved-current-set"
        raise RuntimeError("injected unmatched request")
    name = "deferred-validation" if mismatch == "offscreen" else "flow-test"
    assert app._submit_ui_lane_task(name=name, busy_text="처리 중",
        work=lambda: None, finish=finish, fail=pytest.fail).accepted
    root.run_until(lambda: old.state is LaneState.BROKEN)
    assert app.ui_lane is old
    assert app.current_set_info["raw"] == ["MASTER", "PRODUCT"]
    assert typing[0] == "PARTIAL-NEXT"
    assert app.package_outbox.get_workbench_hold(original_id) is None
    app._render_operator_workbench()
    assert app.entry.options["state"] == "disabled"


@pytest.mark.parametrize("stage", ["CAPTURED_LOCAL", "VALIDATED"])
def test_quarantine_excludes_automatic_validation_and_materialization_without_changing_result(
    recovery_app, tmp_path, stage
):
    import sqlite3
    from tests.test_deferred_intent_capture import _capture, _store
    app, root, _typing = recovery_app
    db_path, app.package_outbox, store = _store(tmp_path / "captures")
    held = _capture(store, set_id=app.current_set_info["id"], scan=app.current_set_info["raw"][0])
    following = _capture(store, set_id="following", scan="OTHER-MASTER")
    if stage == "VALIDATED":
        import hashlib
        with sqlite3.connect(db_path) as conn:
            conn.execute("UPDATE deferred_intents SET state='VALIDATING'")
            conn.execute("UPDATE deferred_intents SET state='VALIDATED',validation_snapshot_hash=?",
                         (hashlib.sha256(b"validated fixture").hexdigest(),))
    before = store.get(held.intent_id)
    _checkpoint(app)
    _finish_fault(app, root)
    candidate = store.next_validation_candidate if stage == "CAPTURED_LOCAL" else store.next_materialization_candidate
    assert candidate() == following.intent_id
    assert store.get(held.intent_id) == before
    assert app.package_outbox.get_by_set_id(app._ui_lane_request["current_set_info"]["id"]) is None


def test_next_legacy_set_uses_normal_duplicate_checks_without_replaying_held_original(packaging):
    from tests.test_legacy_label_transition import MASTER
    from Label_Match import Label_Match
    app, _dialogs = packaging
    scan(app, MASTER)
    original = deepcopy(app.current_set_info)
    root = FakeTkRoot()
    app.after, app.after_cancel, app.run_until = root.after, root.after_cancel, root.run_until
    app._ui_lane_generation = 0
    app._reset_current_set = MethodType(Label_Match._reset_current_set, app)
    app.progress_bar = {}
    app._update_status_label = Mock()
    app.entry.focus_set = Mock()
    app.ui_lane = TkSerialUiLane(root, poll_ms=1,
        generation_provider=lambda: app._ui_lane_generation,
        on_runner_fault=app._handle_ui_lane_fault)
    _finish_fault(app, root)
    assert app.ui_lane.state is LaneState.IDLE
    held = app.package_outbox.get_workbench_hold(original["id"])
    assert held
    scan(app, MASTER)
    assert app.current_set_info["raw"] == [MASTER]
    assert app.current_set_info["id"] != original["id"]
    assert completions(app) == []
    assert app.package_outbox.get_workbench_hold(original["id"])["snapshot_json"] == held["snapshot_json"]
    assert json.loads(held["snapshot_json"])["ui_lane_fault"]["result"] == "UNKNOWN"

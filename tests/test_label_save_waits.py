from __future__ import annotations

import json
import csv
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import label_data_manager
from tests.test_label_match_core import (
    load_label_match_module, _CloseFailingDataManager, _FakeWindow)
from tests.test_tk_serial_ui_lane import FakeTkRoot
from tests.test_label_worker_roster import app_with_roster, answer


@pytest.fixture
def worker_change_app(tmp_path):
    module = load_label_match_module()
    response = answer()
    current, requested = response["workers"][1], response["workers"][0]
    app = app_with_roster(tmp_path, {
        "worker_name": current["display_name"], "worker_id": current["worker_id"]}, response)
    app.run_tests = False
    app.is_running_simulation = app.is_generating_test_logs = False
    app.save_directory, app.unique_id = str(tmp_path), current["worker_id"]
    app._has_background_work = lambda: False
    app._block_view_only_action = lambda *args, **kwargs: False
    app._block_duplicate_history_load = lambda **kwargs: False
    app._release_shortage_confirmation = lambda: True
    app._save_app_settings, app._update_save_directory, app.title = Mock(), Mock(), Mock()
    app._sync_worker_record_context = Mock()
    app._destroy_modal_and_refocus = lambda window: window.destroy()
    app.worker_name_var = Mock()
    app.worker_name_var.get.return_value = requested["display_name"]
    manager = module.DataManager(app.save_directory, app.Worker.PACKAGING, app.worker_name, app.unique_id)
    app.data_manager = manager
    try:
        yield module, app, manager, current, requested, _FakeWindow()
    finally:
        for active in {manager, app.data_manager}:
            active.close(timeout=2)


def _record_scan(manager, event_type):
    event = manager.prepare_audit_event(event_type, {"result": "UNKNOWN"})
    manager.log_event(event["event_type"], event["details"])
    return event


def _has_scan(manager, event):
    for path in Path(manager.save_directory).rglob("*.csv"):
        with path.open(encoding="utf-8-sig", newline="") as stream:
            for row in list(csv.reader(stream))[1:]:
                if row[2] == event["event_type"] and json.loads(row[3])["scanner_audit"]["key"] == event["details"]["scanner_audit"]["key"]:
                    return True
    return False


def test_worker_change_flush_timeout_keeps_manager_open_and_next_scan_durable(worker_change_app, monkeypatch):
    module, app, manager, current, requested, window = worker_change_app
    started, release = threading.Event(), threading.Event()
    real_open, close = manager._open_file, manager.close
    errors, fake_clock = [], [0.0]
    def stalled_open(*args, **kwargs):
        started.set()
        assert release.wait(5)
        return real_open(*args, **kwargs)
    def advance(seconds):
        fake_clock[0] += seconds
    monkeypatch.setattr(manager, "_open_file", stalled_open)
    monkeypatch.setattr(manager, "close", Mock(wraps=close))
    monkeypatch.setattr(module.messagebox, "showerror", lambda *args, **kwargs: errors.append(args))
    try:
        _record_scan(manager, "SCAN_BEFORE_FLUSH_TIMEOUT")
        assert started.wait(1)
        with monkeypatch.context() as clock_patch:
            clock_patch.setattr(label_data_manager, "time", SimpleNamespace(
                monotonic=lambda: fake_clock[0], sleep=advance))
            clock_patch.setattr(manager.log_thread, "join", lambda timeout=None: advance(timeout))
            app._save_settings_and_close(window, requested["display_name"])
        assert app.worker_name == current["display_name"] and app.worker_id == current["worker_id"]
        assert app.data_manager is manager and manager.log_thread.is_alive()
        app.worker_name_var.set.assert_called_once_with(module.display_operator_name(app.worker_name))
        assert len(errors) == 1 and errors[0][0] == "저장 보류"
        assert window.destroyed is False
        assert module.LABEL_MATCH_APP_CLOSE_LOG_TIMEOUT_SECONDS <= fake_clock[0] <= module.LABEL_MATCH_APP_CLOSE_LOG_TIMEOUT_SECONDS + 0.02
        following = _record_scan(manager, "SCAN_AFTER_FLUSH_TIMEOUT")
        assert manager._close_requested is False
        manager.close.assert_not_called()
        app._save_app_settings.assert_not_called()
        app._update_save_directory.assert_not_called()
        root, heartbeats = FakeTkRoot(), []
        root.after(0, lambda: heartbeats.append(True))
        root.run_until(lambda: bool(heartbeats))
        assert root.destroyed is False
    finally:
        release.set()
    assert manager.flush(timeout=2)
    assert _has_scan(manager, following)


def test_worker_change_flush_success_preserves_normal_switch_and_next_scan(worker_change_app, monkeypatch):
    module, app, manager, _current, requested, window = worker_change_app
    previous = _record_scan(manager, "SCAN_BEFORE_WORKER_CHANGE")
    monkeypatch.setattr(manager, "flush", Mock(wraps=manager.flush))
    errors = []
    monkeypatch.setattr(module.messagebox, "showerror", lambda *args, **kwargs: errors.append(args))
    app._save_settings_and_close(window, requested["display_name"])
    manager.flush.assert_called_once_with(timeout=module.LABEL_MATCH_APP_CLOSE_LOG_TIMEOUT_SECONDS)
    assert manager._close_requested is True and not manager.log_thread.is_alive()
    assert _has_scan(manager, previous)
    assert app.worker_name == requested["display_name"] and app.worker_id == requested["worker_id"]
    assert app.data_manager is not manager and app.data_manager._close_requested is False
    assert window.destroyed is True and errors == []
    app._save_app_settings.assert_called_once()
    app._update_save_directory.assert_called_once()
    following = _record_scan(app.data_manager, "SCAN_AFTER_WORKER_CHANGE")
    assert app.data_manager.flush(timeout=2)
    assert _has_scan(app.data_manager, following)


def test_worker_change_close_timeout_holds_request_and_keeps_screen_live(monkeypatch):
    module = load_label_match_module()
    observed, errors, fake_clock = [], [], [0.0]
    class TimedClose(_CloseFailingDataManager):
        def close(self, timeout=None):
            observed.append(timeout)
            assert timeout == module.LABEL_MATCH_APP_CLOSE_LOG_TIMEOUT_SECONDS
            fake_clock[0] += timeout
            raise TimeoutError("log writer did not finish")
    app = object.__new__(module.Label_Match)
    manager = TimedClose()
    app.current_set_info = {"id": None}
    app.is_running_simulation = app.is_generating_test_logs = False
    app.run_tests = False
    app.worker_name, app.save_directory, app.data_manager = "TEST_WORKER", "TEST_PATH", manager
    app._save_app_settings = lambda: pytest.fail("settings must remain pending")
    app._update_save_directory = lambda: pytest.fail("operator must remain unchanged")
    app.title = lambda *_args: pytest.fail("title must remain unchanged")
    monkeypatch.setattr(module.messagebox, "showerror", lambda *args, **kwargs: errors.append(args))
    window, root, heartbeats = _FakeWindow(), FakeTkRoot(), []
    module.Label_Match._save_settings_and_close(app, window, "TEST_NEXT_WORKER")
    root.after(0, lambda: heartbeats.append(True))
    root.run_until(lambda: bool(heartbeats))
    assert observed == [10] and fake_clock == [10.0]
    assert len(errors) == 1 and errors[0][0] == "저장 보류"
    assert app.worker_name == "TEST_WORKER" and app.data_manager is manager
    assert window.destroyed is False and root.destroyed is False


@pytest.mark.parametrize("lock_name", ["_state_lock", "_audit_csv_lock"])
def test_checkpoint_lock_timeout_preserves_file_and_allows_later_retry(tmp_path, monkeypatch, lock_name):
    module = load_label_match_module()
    monkeypatch.setattr(label_data_manager, "STATE_SAVE_LOCK_TIMEOUT_SECONDS", 0.02)
    manager = object.__new__(module.DataManager)
    manager.save_directory, manager.worker_name = str(tmp_path), "TEST_WORKER"
    manager._state_lock, manager._audit_csv_lock = threading.RLock(), threading.RLock()
    manager._audit_csv_prefixes = {}
    manager._audit_outbox = manager._unverified_audit_segments = []
    manager._state_write_failure_attempts = {}
    manager._last_state_write_failure = None
    state = {"current_set_info": {"id": None, "raw": []}}
    state_path = tmp_path / module.Label_Match.FILES.CURRENT_STATE
    state_path.write_text(json.dumps(state), encoding="utf-8")
    original = state_path.read_bytes()
    held, release = threading.Event(), threading.Event()
    def hold():
        with getattr(manager, lock_name):
            held.set()
            assert release.wait(2)
    holder = threading.Thread(target=hold)
    holder.start()
    try:
        assert held.wait(1)
        before = time.monotonic()
        assert manager.save_current_state(state) is False
        assert time.monotonic() - before < 0.25
        assert state_path.read_bytes() == original
        assert manager._audit_outbox == []
        assert manager._state_write_failure_attempts == {}
        assert manager._last_state_write_failure is None
        root, heartbeats = FakeTkRoot(), []
        root.after(0, lambda: heartbeats.append(True))
        root.run_until(lambda: bool(heartbeats))
        assert root.destroyed is False
    finally:
        release.set()
        holder.join(2)
    assert not holder.is_alive()
    assert manager.save_current_state(state) is True
    assert json.loads(state_path.read_text(encoding="utf-8"))["current_set_info"] == state["current_set_info"]

from __future__ import annotations

import json
import threading
import time

import pytest

import label_data_manager
from tests.test_label_match_core import (
    load_label_match_module, _CloseFailingDataManager, _FakeWindow)
from tests.test_tk_serial_ui_lane import FakeTkRoot


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

"""Settings may withdraw a close; application shutdown must keep draining it."""
import csv
from contextlib import contextmanager
import json
from pathlib import Path
import threading
import time
from types import SimpleNamespace

import pytest

import Label_Match as module
from tests.test_general_event_retry import manager_for
from tests.test_label_match_core import _FakeWindow


def test_app_close_timeout_keeps_input_blocked_until_writer_stops(tmp_path, monkeypatch):
    locked = threading.Event()
    locked.set()
    failed = threading.Event()

    def open_csv(path, mode, *args, **kwargs):
        if locked.is_set():
            failed.set()
            raise PermissionError('synthetic application shutdown CSV lock')
        return open(path, mode, *args, **kwargs)

    manager = manager_for(tmp_path / 'data', open_csv, durable_events=module.LABEL_MATCH_DURABLE_EVENT_TYPES)
    app = object.__new__(module.Label_Match)
    app.initialized_successfully = True
    app.is_running_simulation = app.is_generating_test_logs = False
    app.is_blinking = app.run_tests = False
    app.current_set_info = {'id': None}
    app.data_manager = manager
    app._save_current_set_state = lambda: True
    app._save_app_settings = lambda: pytest.fail('shutdown is still deferred')
    app.destroy = lambda: pytest.fail('shutdown is still deferred')
    states = {}
    app.entry = SimpleNamespace(configure=lambda **kw: states.update(entry=kw['state']))
    app.settings_button = SimpleNamespace(configure=lambda **kw: states.update(settings=kw['state']))
    scheduled, delays, errors = [], [], []
    app.after = lambda ms, callback: scheduled.append((ms, callback)) or 'close-retry'
    app._show_app_close_cleanup_delay = lambda: delays.append(True)
    app._start_package_outbox_drain = lambda: None
    replacements = []

    def replace(*args, **kwargs):
        replacement = manager_for(tmp_path / 'data', durable_events=module.LABEL_MATCH_DURABLE_EVENT_TYPES)
        replacements.append(replacement)
        return replacement

    monkeypatch.setattr(module, 'DataManager', replace)
    monkeypatch.setattr(module.messagebox, 'showerror', lambda *args, **kw: errors.append(args))
    try:
        manager.log_event('ERROR_INPUT', {'raw_input': 'before-shutdown'})
        assert failed.wait(5)
        started = time.monotonic()
        app.on_closing()
        assert time.monotonic() - started >= module.LABEL_MATCH_APP_CLOSE_LOG_TIMEOUT_SECONDS
        assert app.data_manager is manager and manager.log_thread.is_alive()
        assert manager._close_requested
        assert app._app_close_in_progress
        assert states == {'entry': 'disabled', 'settings': 'disabled'}
        assert delays == [True] and errors == [] and replacements == []
        assert [ms for ms, _ in scheduled] == [1000]
        locked.clear()
        manager.log_thread.join(5)
        assert not manager.log_thread.is_alive()
        scheduled[0][1]()
        assert len(replacements) == 1 and app.data_manager is replacements[0]
        assert not app._app_close_in_progress
        assert states == {'entry': 'normal', 'settings': 'normal'}
        assert errors[0][0] == '종료 보류'
        assert app.data_manager.flush(timeout=5)
        rows = []
        for path in tmp_path.rglob('*.csv'):
            with path.open(encoding='utf-8-sig', newline='') as stream:
                rows.extend(csv.DictReader(stream))
        assert [row['event'] for row in rows].count('APP_CLOSE') == 1
        assert [row['event'] for row in rows].count('APP_CLOSE_CANCELLED') == 1
    finally:
        locked.clear()
        for current in [manager, *replacements]:
            current.close(timeout=5)


def test_settings_close_cancel_race_joins_stopping_writer_before_replacement(tmp_path, monkeypatch):
    calls = []
    alive = [True]
    thread = SimpleNamespace(is_alive=lambda: alive[0])

    def join(timeout):
        calls.append(('join', timeout))
        alive[0] = False

    thread.join = join

    def cancel():
        calls.append(('cancel',))
        return False  # The writer accepted the close immediately after is_alive().

    def close(timeout):
        raise TimeoutError('synthetic shutdown boundary race')

    failed = SimpleNamespace(_close_requested=True, log_thread=thread,
        _cancel_close_request=cancel, flush=lambda timeout: True, close=close,
        save_directory=str(tmp_path), process_name='포장실', worker_name='old-worker', unique_id='RACE-PC')
    replacement = SimpleNamespace(log_event=lambda *args: calls.append(('event',)))
    app = object.__new__(module.Label_Match)
    app.current_set_info = {'id': None}
    app.is_running_simulation = app.is_generating_test_logs = app.run_tests = False
    app.worker_name = 'old-worker'
    app.data_manager = failed
    app._save_app_settings = lambda: pytest.fail('failed settings must not be saved')
    app._update_save_directory = lambda: pytest.fail('failed settings must not change paths')
    monkeypatch.setattr(module, 'DataManager', lambda *args, **kw: replacement)
    monkeypatch.setattr(module.messagebox, 'showerror', lambda *args, **kw: None)
    app._save_settings_and_close(_FakeWindow(), 'next-worker')
    assert app.data_manager is replacement
    assert calls == [('cancel',), ('join', module.LABEL_MATCH_APP_CLOSE_LOG_TIMEOUT_SECONDS)]
    assert app.worker_name == 'old-worker'
    app.data_manager.log_event('ERROR_INPUT', {})
    assert calls[-1] == ('event',)


@pytest.mark.parametrize('blocked_local', [False, True])
@pytest.mark.parametrize('second_retries', [False, True])
def test_locked_csv_keeps_only_its_own_fifo_pending(tmp_path, blocked_local, second_retries):
    locked = threading.Event()
    locked.set()
    failed = threading.Event()
    second_locked = threading.Event()
    if second_retries:
        second_locked.set()
    second_failed = threading.Event()
    written = threading.Event()
    manager = None

    @contextmanager
    def open_csv(path, mode, *args, **kwargs):
        is_local = Path(path).parent == Path(manager.local_event_directory)
        if locked.is_set() and is_local == blocked_local:
            failed.set()
            raise PermissionError('synthetic single CSV lock')
        if second_locked.is_set() and is_local != blocked_local:
            second_failed.set()
            raise PermissionError('synthetic independent CSV retry')
        with open(path, mode, *args, **kwargs) as stream:
            yield stream
        written.set()

    manager = manager_for(tmp_path / 'data', open_csv)
    blocked_event, free_event = (('APP_CLOSE_CANCELLED', 'ERROR_INPUT') if blocked_local
                               else ('ERROR_INPUT', 'APP_CLOSE_CANCELLED'))
    try:
        manager.log_event(blocked_event, {'sequence': 1})
        assert failed.wait(5)
        manager.log_event(blocked_event, {'sequence': 2})
        manager.log_event(free_event, {'sequence': 3})
        if second_retries:
            assert second_failed.wait(5)
            second_locked.clear()
        assert written.wait(5), 'an independent CSV must progress while the first stays locked'
        assert manager.flush(timeout=5)
        free_path = manager._get_log_filepath_for_item([module.datetime.now().isoformat(), '', free_event, ''])
        with Path(free_path).open(encoding='utf-8-sig', newline='') as stream:
            assert [json.loads(row['details'])['sequence'] for row in csv.DictReader(stream)] == [3]
        assert manager.has_pending_log_retries
        locked.clear()
        manager.close(timeout=5)
        blocked_path = manager._get_log_filepath_for_item([module.datetime.now().isoformat(), '', blocked_event, ''])
        with Path(blocked_path).open(encoding='utf-8-sig', newline='') as stream:
            assert [json.loads(row['details'])['sequence'] for row in csv.DictReader(stream)] == [1, 2]
    finally:
        locked.clear()
        second_locked.clear()
        manager.close(timeout=5)

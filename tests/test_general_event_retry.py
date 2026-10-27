"""A failed ordinary event must survive queue completion and a later flush."""
from datetime import datetime
from contextlib import contextmanager
import csv
import json
from pathlib import Path
import threading
import time
from types import SimpleNamespace

import pytest
from label_data_manager import DataManager
from tests.test_w9scanonly_packaging import packaging, scan, products, completions  # noqa: F401
from tests.test_legacy_label_transition import MASTER


def manager_for(path, open_file=open, *, durable_events=()):
    return DataManager(str(path), '포장실', 'fixture-worker', 'RETRY-PC',
        open_file=open_file, clock=lambda: datetime,
        enrich_event=lambda event, details, unique_id: details,
        datetime_encoder=lambda: json.JSONEncoder,
        current_state_filename=lambda: '_current_set_state_packaging.json',
        durable_event_types=lambda: set(durable_events), csv_safe_cell=lambda value: value)


def test_ordinary_event_csv_lock_then_unlock_replays_once(tmp_path):
    blocked = [True]
    failed = threading.Event()
    written = threading.Event()

    def open_csv(path, mode, *args, **kwargs):
        if str(path).endswith('.csv') and blocked[0]:
            failed.set()
            raise PermissionError('synthetic ordinary event CSV lock')
        written.set()
        return open(path, mode, *args, **kwargs)

    manager = manager_for(tmp_path, open_csv)
    try:
        manager.log_event('ERROR_INPUT', {'raw_input': 'fixture-invalid', 'reason': 'fixture-reason'})
        assert failed.wait(5)
        assert manager.flush(timeout=5)
        assert manager.has_pending_log_retries
        blocked[0] = False
        assert written.wait(5)
        deadline = time.monotonic() + 5
        while manager.has_pending_log_retries and time.monotonic() < deadline:
            time.sleep(.01)
        assert not manager.has_pending_log_retries
        assert manager.flush(timeout=5)
        with Path(manager._get_log_filepath()).open(encoding='utf-8-sig', newline='') as stream:
            rows = list(csv.DictReader(stream))
        assert len(rows) == 1
        assert rows[0]['event'] == 'ERROR_INPUT'
        assert json.loads(rows[0]['details'])['raw_input'] == 'fixture-invalid'
        assert manager.flush(timeout=5)
        with Path(manager._get_log_filepath()).open(encoding='utf-8-sig', newline='') as stream:
            assert list(csv.DictReader(stream)) == rows
    finally:
        blocked[0] = False
        try:
            manager.close(timeout=5)
        except RuntimeError:
            pass


def test_close_waits_for_requeued_row_instead_of_passing_its_sentinel(tmp_path):
    blocked = [True]
    failed = threading.Event()

    def open_csv(path, mode, *args, **kwargs):
        if blocked[0]:
            failed.set()
            raise PermissionError('synthetic close pending lock')
        return open(path, mode, *args, **kwargs)

    manager = manager_for(tmp_path, open_csv)
    try:
        manager.log_event('ERROR_INPUT', {'raw_input': 'close-pending'})
        assert failed.wait(5)
        assert manager.flush(timeout=5)
        with pytest.raises(TimeoutError):
            manager.close(timeout=.05)
        assert manager.log_thread.is_alive()
        blocked[0] = False
        assert manager.close(timeout=5)
        with Path(manager._get_log_filepath()).open(encoding='utf-8-sig', newline='') as stream:
            assert len(list(csv.DictReader(stream))) == 1
    finally:
        blocked[0] = False
        try:
            manager.close(timeout=5)
        except RuntimeError:
            pass


def test_settings_timeout_reopens_same_writer_and_next_event_is_saved(tmp_path, monkeypatch):
    import Label_Match as module
    from tests.test_label_match_core import _FakeWindow

    blocked = threading.Event()
    blocked.set()
    failed = threading.Event()

    def open_csv(path, mode, *args, **kwargs):
        if blocked.is_set():
            failed.set()
            raise PermissionError('synthetic settings CSV lock lasting over ten seconds')
        return open(path, mode, *args, **kwargs)

    manager = manager_for(tmp_path, open_csv)
    app = object.__new__(module.Label_Match)
    app.current_set_info = {'id': None}
    app.is_running_simulation = app.is_generating_test_logs = False
    app.run_tests = False
    app.worker_name = 'old-worker'
    app.save_directory = str(tmp_path)
    app.data_manager = manager
    app._save_app_settings = lambda: pytest.fail('failed settings must not be saved')
    app._update_save_directory = lambda: pytest.fail('failed settings must not change paths')
    window = _FakeWindow()
    errors = []
    monkeypatch.setattr(module.messagebox, 'showerror', lambda *args, **kwargs: errors.append(args))
    try:
        manager.log_event('ERROR_INPUT', {'raw_input': 'before-settings'})
        assert failed.wait(5)
        assert manager.flush(timeout=5)
        started = time.monotonic()
        app._save_settings_and_close(window, 'next-worker')
        assert time.monotonic() - started >= module.LABEL_MATCH_APP_CLOSE_LOG_TIMEOUT_SECONDS
        assert errors and not window.destroyed
        assert app.worker_name == 'old-worker'
        assert app.data_manager is manager
        assert manager.log_thread.is_alive()
        assert not manager._close_requested
        manager.log_event('ERROR_INPUT', {'raw_input': 'after-settings'})
        blocked.clear()
        assert manager.close(timeout=5)
        with Path(manager._get_log_filepath()).open(encoding='utf-8-sig', newline='') as stream:
            rows = list(csv.DictReader(stream))
        assert [json.loads(row['details'])['raw_input'] for row in rows] == [
            'before-settings', 'after-settings']
    finally:
        blocked.clear()
        try:
            manager.close(timeout=5)
        except RuntimeError:
            pass


def test_retry_preserves_general_fifo_while_protected_rows_can_pass(tmp_path, monkeypatch):
    import label_data_manager as persistence

    first_attempt = threading.Event()
    start = threading.Event()
    blocked = [True]
    sleeps = []
    original_sleep = time.sleep

    def sleep(seconds):
        if seconds == persistence.GENERAL_LOG_RETRY_BACKOFF_SECONDS:
            sleeps.append(seconds)
            blocked[0] = False
        else:
            original_sleep(seconds)

    monkeypatch.setattr(persistence, 'time', SimpleNamespace(sleep=sleep, monotonic=time.monotonic))
    manager = manager_for(tmp_path, durable_events={'TRAY_COMPLETE'})
    filepath = manager._get_log_filepath_for_item

    def resolve(item):
        if item[2] == 'ERROR_INPUT' and blocked[0]:
            first_attempt.set()
            assert start.wait(5)
            raise PermissionError('synthetic FIFO path lock')
        return filepath(item)

    monkeypatch.setattr(manager, '_get_log_filepath_for_item', resolve)
    try:
        manager.log_event('ERROR_INPUT', {'raw_input': 'A'})
        assert first_attempt.wait(5)
        for marker in ('B', 'C'):
            manager.log_event('ERROR_INPUT', {'raw_input': marker})
        manager.log_event('TRAY_COMPLETE', {'raw_input': 'durable'})
        start.set()
        assert manager.close(timeout=5)
        with Path(manager._get_log_filepath()).open(encoding='utf-8-sig', newline='') as stream:
            rows = list(csv.DictReader(stream))
        assert [json.loads(row['details'])['raw_input'] for row in rows
                if row['event'] == 'ERROR_INPUT'] == ['A', 'B', 'C']
        assert [row['event'] for row in rows].count('TRAY_COMPLETE') == 1
        assert len(rows) == 4
        assert sleeps == [persistence.GENERAL_LOG_RETRY_BACKOFF_SECONDS]
        assert manager.log_queue.unfinished_tasks == 0
    finally:
        blocked[0] = False
        start.set()
        manager.close(timeout=5)


def test_many_locked_general_rows_back_off_once_before_durable_and_audit(tmp_path, monkeypatch):
    import label_data_manager as persistence

    first_attempt = threading.Event()
    start = threading.Event()
    protected = threading.Event()
    blocked = [True]
    sleeps = []
    protected_delays = []
    original_sleep = time.sleep

    def sleep(seconds):
        if seconds == persistence.GENERAL_LOG_RETRY_BACKOFF_SECONDS:
            sleeps.append(seconds)
            original_sleep(.01)
        else:
            original_sleep(seconds)

    monkeypatch.setattr(persistence, 'time', SimpleNamespace(sleep=sleep, monotonic=time.monotonic))
    manager = manager_for(tmp_path, durable_events={'TRAY_COMPLETE'})
    filepath = manager._get_log_filepath_for_item

    def resolve(item):
        if item[2] == 'ERROR_INPUT' and blocked[0]:
            first_attempt.set()
            assert start.wait(5)
            raise PermissionError('synthetic permanently locked ordinary path')
        if item[2] != 'ERROR_INPUT':
            protected_delays.append(len(sleeps))
            if len(protected_delays) == 2:
                protected.set()
        return filepath(item)

    monkeypatch.setattr(manager, '_get_log_filepath_for_item', resolve)
    try:
        manager.log_event('ERROR_INPUT', {'raw_input': 'A'})
        assert first_attempt.wait(5)
        for marker in ('B', 'C', 'D'):
            manager.log_event('ERROR_INPUT', {'raw_input': marker})
        manager.log_event('TRAY_COMPLETE', {})
        manager.log_event('scanner-audit', {'scanner_audit': {'key': 'round-audit',
            'occurred_at': datetime.now().isoformat(), 'operator': 'fixture-worker'}})
        start.set()
        assert protected.wait(5)
        assert protected_delays == [0, 0] or max(protected_delays) <= 1
        assert manager.flush(timeout=5)
        assert manager.has_pending_log_retries
        blocked[0] = False
        assert manager.close(timeout=5)
        with Path(manager._get_log_filepath()).open(encoding='utf-8-sig', newline='') as stream:
            rows = list(csv.DictReader(stream))
        assert len(rows) == 6
        assert manager.log_queue.unfinished_tasks == 0
    finally:
        blocked[0] = False
        start.set()
        manager.close(timeout=5)


@pytest.mark.parametrize('event', ['scanner-audit'])
def test_scanner_failures_keep_the_existing_sticky_flush_contract(tmp_path, event):
    attempts = []

    def open_csv(*args, **kwargs):
        attempts.append(args[0])
        raise PermissionError('synthetic protected event lock')

    manager = manager_for(tmp_path, open_csv, durable_events={'TRAY_COMPLETE'})
    try:
        details = {} if event == 'TRAY_COMPLETE' else {
            'scanner_audit': {'key': 'protected-key', 'occurred_at': datetime.now().isoformat(),
                              'operator': 'fixture-worker'}}
        manager.log_event(event, details)
        with pytest.raises(RuntimeError, match='synthetic protected event lock'):
            manager.flush(timeout=5)
        assert not manager.has_pending_log_retries
        assert len(attempts) == 1
        with pytest.raises(RuntimeError, match='synthetic protected event lock'):
            manager.flush(timeout=5)
        assert len(attempts) == 1
    finally:
        try:
            manager.close(timeout=5)
        except RuntimeError:
            pass


def test_failure_after_append_keeps_original_in_memory_without_appending_again(tmp_path):
    attempts = []

    @contextmanager
    def append_then_fail(path, mode, *args, **kwargs):
        if mode in ('a', 'a+'):
            attempts.append(path)
        with open(path, mode, *args, **kwargs) as stream:
            yield stream
        if mode in ('a', 'a+'):
            raise OSError('synthetic failure after context close')

    manager = manager_for(tmp_path, append_then_fail)
    try:
        manager.log_event('ERROR_INPUT', {'raw_input': 'ambiguous-append'})
        with pytest.raises(RuntimeError, match='synthetic failure after context close'):
            manager.flush(timeout=5)
        before = Path(manager._get_log_filepath()).read_bytes()
        with pytest.raises(RuntimeError):
            manager.flush(timeout=5)
        assert len(attempts) == 1
        assert Path(manager._get_log_filepath()).read_bytes() == before
        retained, = manager._unresolved_log_items.values()
        assert json.loads(retained[3])['raw_input'] == 'ambiguous-append'
    finally:
        try:
            manager.close(timeout=5)
        except RuntimeError:
            pass


@pytest.mark.parametrize('state', ['normal', 'retrying', 'stopped'])
def test_requeued_general_event_notice_uses_existing_relay_label_without_blocking(state):
    import Label_Match as module
    options = {}
    manager = SimpleNamespace(has_pending_log_retries=True)
    app = object.__new__(module.Label_Match)
    app.data_manager = manager
    app.relay_screen_status_label = SimpleNamespace(configure=lambda **values: options.update(values))
    module.Label_Match._render_relay_screen_status(app, state)
    notice = '작업 기록을 다시 저장하는 중입니다 — 작업은 그대로 계속하세요'
    assert options['text'].splitlines()[-1] == notice
    assert options['text'].splitlines()[0] == module.LABEL_MATCH_RELAY_SCREEN_TEXT[state]
    assert options['style'] == ('Warning.TLabel' if state == 'stopped' else 'Status.TLabel')
    assert '_workflow_blocking_notice' not in app.__dict__
    manager.has_pending_log_retries = False
    module.Label_Match._render_relay_screen_status(app, state)
    assert options['text'] == module.LABEL_MATCH_RELAY_SCREEN_TEXT.get(state, '')
    assert options['style'] == ('Warning.TLabel' if state == 'stopped' else 'Status.TLabel')


@pytest.mark.parametrize('state', ['normal', 'stopped'])
def test_web_general_retry_notice_is_quiet_nonblocking_and_disappears(state):
    import Label_Match as module
    from label_screen_bridge import _alerts
    manager = SimpleNamespace(has_pending_log_retries=True)
    app = SimpleNamespace(data_manager=manager, _logistics_relay_state=state)
    processor = SimpleNamespace(app=app, module=module, _dialogs=[])
    before = _alerts(processor, None, None)
    notice = {'kind': 'quiet', 'code': None,
              'text': '작업 기록을 다시 저장하는 중입니다 — 작업은 그대로 계속하세요'}
    assert before[-1] == notice
    assert not hasattr(app, '_workflow_blocking_notice')
    manager.has_pending_log_retries = False
    after = _alerts(processor, None, None)
    assert after == before[:-1]
    if state == 'stopped':
        assert after == [{'kind': 'warn', 'code': None,
                          'text': module.LABEL_MATCH_RELAY_SCREEN_TEXT['stopped']}]


def test_web_retry_notice_does_not_displace_either_pending_dialog():
    import Label_Match as module
    from label_screen_bridge import _alerts

    dialogs = [{'kind': 'warn', 'code': None, 'text': text} for text in ('dialog-one', 'dialog-two')]
    app = SimpleNamespace(data_manager=SimpleNamespace(has_pending_log_retries=True),
        _logistics_relay_state='stopped', _compact_operator_notice_message=lambda text: text)
    notice = SimpleNamespace(kind='scanner_notice', title='existing-band', message='details', tone='danger')
    processor = SimpleNamespace(app=app, module=module, _dialogs=dialogs)
    alerts = _alerts(processor, None, notice)
    assert len(alerts) == 5
    assert [alert for alert in alerts if alert in dialogs] == dialogs


def test_general_retry_does_not_block_five_scan_completion_or_next_package(packaging, monkeypatch):
    app, dialogs = packaging
    manager = app.data_manager
    assert manager.flush(timeout=5)
    target_path = manager._get_log_filepath_for_item
    blocked = [True]
    failed = threading.Event()

    def locked_general_item(log_item):
        if blocked[0] and 'flow-retry-marker' in log_item[3]:
            failed.set()
            raise PermissionError('synthetic generic preappend path lock')
        return target_path(log_item)

    monkeypatch.setattr(manager, '_get_log_filepath_for_item', locked_general_item)
    try:
        manager.log_event('ERROR_INPUT', {'raw_input': 'flow-retry-marker'})
        assert failed.wait(5)
        assert manager.flush(timeout=5)
        assert manager.has_pending_log_retries
        values = [MASTER, *products(prefix='RETRY-FLOW')]
        for value in values:
            scan(app, value)
        completed = completions(app)
        assert len(completed) == 1
        assert completed[0][1]['final_result'] == app.Results.PASS
        assert completed[0][1]['scan_count'] == 5
        assert completed[0][1]['scanned_product_barcodes'] == values
        scan(app, MASTER)
        assert app.current_set_info['raw'] == [MASTER]
        assert manager.has_pending_log_retries
        assert dialogs == []
    finally:
        blocked[0] = False
        deadline = time.monotonic() + 5
        while getattr(manager, 'has_pending_log_retries', False) and time.monotonic() < deadline:
            time.sleep(.01)

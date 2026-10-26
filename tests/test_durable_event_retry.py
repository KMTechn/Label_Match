"""Preappend durable CSV failures recover without a second completion row."""
from contextlib import contextmanager
import csv
from pathlib import Path
import threading
import time
from types import MethodType

import pytest

import Label_Match as label_module
from tests.test_general_event_retry import manager_for
from tests.test_legacy_label_transition import MASTER
from tests.test_w9scanonly_packaging import packaging, scan, products, completions  # noqa: F401


def wait_until(predicate):
    deadline = time.monotonic() + 5
    while not predicate() and time.monotonic() < deadline:
        time.sleep(.01)
    assert predicate()


@pytest.mark.parametrize('event', sorted(label_module.LABEL_MATCH_DURABLE_EVENT_TYPES))
def test_durable_preappend_retry_preserves_original_row_and_fsync(tmp_path, monkeypatch, event):
    blocked = [True]
    failed = threading.Event()
    synced = []
    attempts = []

    def open_csv(path, mode, *args, **kwargs):
        attempts.append(time.monotonic())
        if blocked[0]:
            failed.set()
            raise PermissionError('synthetic durable preappend lock')
        return open(path, mode, *args, **kwargs)

    real_fsync = label_module.os.fsync
    monkeypatch.setattr(label_module.os, 'fsync', lambda fd: (real_fsync(fd), synced.append(fd))[0])
    manager = manager_for(tmp_path, open_csv, durable_events={event})
    try:
        manager.log_event(event, {'set_id': 'durable-retry-set'})
        assert failed.wait(5)
        with pytest.raises(RuntimeError, match='synthetic durable preappend lock'):
            manager.flush(timeout=5)
        assert manager._unresolved_log_items, 'the failed durable row must remain retryable'
        original, = manager._unresolved_log_items.values()
        target = Path(manager._get_log_filepath_for_item(original))
        assert not manager.has_pending_log_retries, 'the ordinary nonblocking notice must stay unchanged'
        assert not target.exists()
        blocked[0] = False
        wait_until(lambda: manager.log_queue.unfinished_tasks == 0)
        assert manager.flush(timeout=5)
        assert synced
        assert len(attempts) == 2
        assert attempts[1] - attempts[0] >= .9
        with target.open(encoding='utf-8-sig', newline='') as stream:
            rows = list(csv.reader(stream))
        assert rows[1:] == [original]
        assert not manager._unresolved_log_items
        assert manager.flush(timeout=5)
    finally:
        blocked[0] = False
        try:
            manager.close(timeout=5)
        except RuntimeError:
            pass


@pytest.mark.parametrize('fault', ['close', 'fsync'])
def test_durable_failure_after_append_is_sticky_without_retry(tmp_path, monkeypatch, fault):
    attempts = []

    @contextmanager
    def append_then_fail(path, mode, *args, **kwargs):
        attempts.append(path)
        with open(path, mode, *args, **kwargs) as stream:
            yield stream
        if fault == 'close':
            raise OSError('synthetic durable ambiguous close')

    if fault == 'fsync':
        def fail_fsync(fd):
            raise OSError('synthetic durable ambiguous fsync')
        monkeypatch.setattr(label_module.os, 'fsync', fail_fsync)
    manager = manager_for(tmp_path, append_then_fail, durable_events={'TRAY_COMPLETE'})
    try:
        manager.log_event('TRAY_COMPLETE', {'set_id': 'ambiguous-durable-set'})
        with pytest.raises(RuntimeError, match='synthetic durable ambiguous'):
            manager.flush(timeout=5)
        before = Path(manager._get_log_filepath()).read_bytes()
        time.sleep(1.1)
        with pytest.raises(RuntimeError):
            manager.flush(timeout=5)
        assert len(attempts) == 1
        assert Path(manager._get_log_filepath()).read_bytes() == before
        assert not manager._log_retry_items
    finally:
        try:
            manager.close(timeout=5)
        except RuntimeError:
            pass


def prepare_blocked_completion(app, monkeypatch, blocked):
    manager = app.data_manager
    assert manager.flush(timeout=5)
    failed = threading.Event()
    attempts = []
    original_path = manager._get_log_filepath_for_item

    def locked_completion(item):
        if item[2] == app.Events.TRAY_COMPLETE and blocked[0]:
            attempts.append(time.monotonic())
            failed.set()
            raise PermissionError('synthetic completion preappend lock')
        return original_path(item)

    monkeypatch.setattr(manager, '_get_log_filepath_for_item', locked_completion)
    app._publish_durable_commit_block = MethodType(label_module.Label_Match._publish_durable_commit_block, app)
    app._process_app_stop_request = lambda: False
    app._read_logistics_transmission_summary = lambda path: (None, None)
    app._render_logistics_connection = lambda *args: None
    app._render_logistics_progress = lambda: None
    app.__dict__.pop('save_status_label', None)
    callbacks = []
    app.after = lambda delay, action: callbacks.append(action)
    for value in [MASTER, *products(prefix='DURABLE-RETRY')]:
        scan(app, value)
    assert failed.wait(5)
    assert app.current_set_info['raw']
    notice = app.__dict__.get('_workflow_blocking_notice')
    assert notice is not None
    assert app._workflow_notice_action_text == '저장 재시도'
    app._refresh_relay_screen_status()
    assert app._workflow_blocking_notice is notice
    return callbacks, notice, attempts


@pytest.mark.parametrize('through_lane_failure', [False, True])
def test_locked_completion_unlocks_and_next_set_starts_without_button(packaging, monkeypatch, through_lane_failure):
    app, dialogs = packaging
    blocked = [True]
    try:
        callbacks, notice, attempts = prepare_blocked_completion(app, monkeypatch, blocked)
        if through_lane_failure:
            with pytest.raises(RuntimeError) as failure:
                app.data_manager.flush(timeout=5)
            app._publish_durable_commit_block(label_module.Failure.from_exception(failure.value))
        assert app.data_manager._unresolved_log_items
        original, = app.data_manager._unresolved_log_items.values()
        blocked[0] = False
        wait_until(lambda: app.data_manager.log_queue.unfinished_tasks == 0)
        callbacks.pop(0)()
        assert app.__dict__.get('_workflow_blocking_notice') is None
        completed = completions(app)
        assert len(completed) == 1
        assert completed[0][1]['final_result'] == app.Results.PASS
        with Path(app.data_manager._get_log_filepath()).open(encoding='utf-8-sig', newline='') as stream:
            assert [r for r in csv.reader(stream) if len(r) > 2 and r[2] == 'TRAY_COMPLETE'] == [original]
        scan(app, MASTER)
        assert app.current_set_info['raw'] == [MASTER]
        assert dialogs == []
    finally:
        blocked[0] = False
        wait_until(lambda: app.data_manager.log_queue.unfinished_tasks == 0)


@pytest.mark.parametrize('manual_first', [False, True])
def test_button_and_auto_retry_overlap_write_one_completion(packaging, monkeypatch, manual_first):
    app, dialogs = packaging
    blocked = [True]
    try:
        callbacks, notice, attempts = prepare_blocked_completion(app, monkeypatch, blocked)
        wait_until(lambda: len(attempts) >= 2)
        assert len(app.data_manager._writer_errors) == 1
        # A button while the file remains locked keeps the same hard block.
        app._acknowledge_workflow_notice()
        assert app.__dict__.get('_workflow_blocking_notice') is not None
        assert app._workflow_notice_action_text == '저장 재시도'
        blocked[0] = False
        if manual_first:
            app._acknowledge_workflow_notice()  # May overlap the writer's retry and fsync.
        wait_until(lambda: app.data_manager.log_queue.unfinished_tasks == 0)
        if manual_first and app.__dict__.get('_workflow_blocking_notice') is not None:
            app._acknowledge_workflow_notice()
        callbacks.pop(0)()
        if not manual_first:
            app._acknowledge_workflow_notice()  # A queued click after automatic completion.
        assert app.__dict__.get('_workflow_blocking_notice') is None
        assert len(completions(app)) == 1
        scan(app, MASTER)
        assert app.current_set_info['raw'] == [MASTER]
        assert dialogs == []
    finally:
        blocked[0] = False
        wait_until(lambda: app.data_manager.log_queue.unfinished_tasks == 0)

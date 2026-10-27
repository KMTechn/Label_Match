"""Real relay log writers share the existing session admission boundary."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import multiprocessing
import os
from pathlib import Path
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch

import pytest


pytestmark = pytest.mark.skipif(os.name != 'nt', reason='Windows named-mutex append regression')


def _install_child_safety():
    if os.environ.get('KMTECH_SUB753_HEADLESS_HELPERS') == '1':
        import sitecustomize
        import pack_safety
        import headless_pytest_plugin
        headless_pytest_plugin.pytest_configure(None)


def _config(root: Path, log: Path, writer: int):
    return SimpleNamespace(
        log_path=log, runtime_status_path=root / f'status-{writer}.json',
        db_path=root / 'queue.sqlite3', worker_id=f'writer-{writer}',
        credential_path=root / 'credential-placeholder.json',
    )


def _emit(root: Path, log: Path, writer: int, sequence: int):
    import direct_sync_runtime as runtime
    from tools import direct_sync_relay_runner as runner
    config = _config(root, log, writer)
    payload = {'sequence': sequence, 'writer': writer, 'detail': '기록' * 4096}
    if writer == 0:
        runtime._append_runtime_event(config, 'concurrent_runtime', payload, source_identity={
            'producer_manifest_ref': 'manifest-placeholder.json',
            'producer_manifest_sha256': 'placeholder',
        })
    else:
        with patch.object(runner, 'source_scan_failure_status', return_value={}), \
             patch.object(runtime, '_carry_clock_error'), \
             patch.object(runtime, '_publish_upload_block_status'):
            runner._persist_scan_runtime_status(config, payload)


def _competing_open(log, opened):
    original_open = Path.open

    def competing_open(path, mode='r', *args, **kwargs):
        handle = original_open(path, mode, *args, **kwargs)
        if path == log and mode.startswith('a'):
            # Both old handles obtain the same EOF before either writes. Once
            # admission serializes opening, the bounded barrier breaks once
            # and subsequent appends proceed without a barrier wait.
            try:
                opened.wait(timeout=0.15)
            except (threading.BrokenBarrierError,):
                pass
        return handle

    return competing_open


def _write_rows(root: str, writer: int, start, opened, install_open=True):
    # Spawned children inherit the isolated paths and reinstall the headless
    # boundary before importing or calling any application helper.
    _install_child_safety()
    root = Path(root)
    log = root / 'relay.jsonl'
    start.wait(timeout=15)
    if not install_open:
        for sequence in range(40):
            _emit(root, log, writer, sequence)
        return
    with patch.object(Path, 'open', _competing_open(log, opened)):
        for sequence in range(40):
            _emit(root, log, writer, sequence)


def _observe(name: str, **fields):
    assigned = os.environ.get('KMTECH_TEST_CA_TASK_ROOT')
    if not assigned:
        return
    root = Path(assigned)
    with (root / 'observations.jsonl').open('a', encoding='utf-8') as handle:
        handle.write(json.dumps({'case': name, **fields}) + '\n')


@pytest.mark.parametrize('iteration', range(3))
@pytest.mark.parametrize('kind', ['threads', 'processes'])
def test_actual_runtime_and_runner_append_all_rows(tmp_path, monkeypatch, kind, iteration):
    import writer_session_fence as fence
    monkeypatch.setenv(fence.TEST_MODE_ENV, '1')
    monkeypatch.setenv(fence.CONTROL_ROOT_OVERRIDE_ENV, str(tmp_path / 'control'))
    if kind == 'threads':
        start, opened = threading.Barrier(2), threading.Barrier(2)
        with patch.object(Path, 'open', _competing_open(tmp_path / 'relay.jsonl', opened)), \
             ThreadPoolExecutor(max_workers=2) as pool:
            jobs = [pool.submit(_write_rows, str(tmp_path), writer, start, opened, False) for writer in range(2)]
            for job in jobs:
                job.result(timeout=30)
    else:
        context = multiprocessing.get_context('spawn')
        start, opened = context.Barrier(2), context.Barrier(2)
        jobs = [context.Process(target=_write_rows, args=(str(tmp_path), writer, start, opened))
                for writer in range(2)]
        for job in jobs:
            job.start()
        for job in jobs:
            job.join(30)
            assert job.exitcode == 0
    lines = (tmp_path / 'relay.jsonl').read_bytes().splitlines()
    entries, malformed = [], 0
    for line in lines:
        try:
            entries.append(json.loads(line))
        except (ValueError, UnicodeDecodeError):
            malformed += 1
    pairs = {(entry['writer'], entry['sequence']) for entry in entries}
    _observe(f'{kind}-{iteration}', expected=80, lines=len(lines), parsed=len(entries),
             unique=len(pairs), malformed=malformed)
    assert len(lines) == len(entries) == len(pairs) == 80
    assert pairs == {(writer, sequence) for writer in range(2) for sequence in range(40)}


@pytest.mark.parametrize('caller', ['runtime', 'operator', 'host', 'runner-error', 'runner-status'])
def test_record_refusal_drops_only_line_and_keeps_caller_result(tmp_path, monkeypatch, caller):
    import direct_sync_runtime as runtime
    import direct_sync_operator as operator
    import label_match_product_host as host
    import writer_session_fence as fence
    from tools import direct_sync_relay_runner as runner

    monkeypatch.setattr(fence, '_acquire_named_mutex', lambda *args, **kwargs: None)
    monkeypatch.setattr(runner, 'source_scan_failure_status', lambda *args: {})
    monkeypatch.setattr(runtime, '_carry_clock_error', lambda *args: None)
    monkeypatch.setattr(runtime, '_publish_upload_block_status', lambda *args: None)
    before = runtime._RELAY_LOG_DROPPED
    log = tmp_path / 'relay.jsonl'
    config = _config(tmp_path, log, 0)
    if caller == 'runtime':
        _emit(tmp_path, log, 0, 0)
    elif caller == 'operator':
        operator._append_jsonl(log, {'event': 'operator'})
    elif caller == 'host':
        host._append_jsonl(log, {'event': 'host'})
    elif caller == 'runner-error':
        result = runner._write_scan_runtime_error(config, OSError('record unavailable'))
        assert result['status'] == 'runtime_error'
    else:
        runner._persist_scan_runtime_status(config, {'status': 'idle'})
    assert not log.exists()
    assert runtime._RELAY_LOG_DROPPED == before + 1
    if caller.startswith('runner'):
        assert json.loads(Path(config.runtime_status_path).read_bytes())['status'] in {'idle', 'runtime_error'}


def test_nested_admission_uses_existing_lease_and_preserves_json_bytes(tmp_path, monkeypatch):
    import direct_sync_runtime as runtime
    import writer_session_fence as fence
    payload = {'event': '기록', 'sequence': 1}
    log = tmp_path / 'relay.jsonl'
    with fence.writer_admission('direct_sync_relay_cycle'):
        monkeypatch.setattr(fence, '_acquire_named_mutex', lambda *args, **kwargs: pytest.fail('nested reacquire'))
        runtime._append_jsonl(log, payload)
        assert fence._WRITER_LOCAL.depth == 1
    assert log.read_bytes() == (json.dumps(payload, ensure_ascii=False, sort_keys=True) + '\n').encode('utf-8')


def _abandon_partial_log(root: str):
    _install_child_safety()
    import writer_session_fence as fence
    with fence.writer_admission('direct_sync_relay_cycle'):
        with (Path(root) / 'relay.jsonl').open('wb') as handle:
            handle.write(b'{"interrupted":')
            handle.flush()
            os.fsync(handle.fileno())
        os._exit(0)


def test_dead_writer_partial_tail_is_preserved_and_next_record_starts_new_line(tmp_path, monkeypatch):
    import direct_sync_runtime as runtime
    import writer_session_fence as fence
    monkeypatch.setenv(fence.TEST_MODE_ENV, '1')
    monkeypatch.setenv(fence.CONTROL_ROOT_OVERRIDE_ENV, str(tmp_path / 'control'))
    context = multiprocessing.get_context('spawn')
    job = context.Process(target=_abandon_partial_log, args=(str(tmp_path),))
    job.start()
    job.join(15)
    assert job.exitcode == 0
    log = tmp_path / 'relay.jsonl'
    partial = log.read_bytes()
    # Keep the native abandoned-mutex refusal; a later writer may acquire it.
    runtime._append_jsonl(log, {'event': 'next'})
    runtime._append_jsonl(log, {'event': 'after'})
    raw = log.read_bytes()
    assert raw.startswith(partial + b'\n')
    rows = raw.splitlines()
    assert len(rows) >= 2
    assert all(json.loads(row)['event'] in {'next', 'after'} for row in rows[1:])


def _hold_writer_for_one_second(ready):
    _install_child_safety()
    import writer_session_fence as fence
    with fence.writer_admission('direct_sync_relay_cycle'):
        ready.set()
        time.sleep(1.2)


def test_busy_writer_drops_one_row_but_real_relay_loop_reaches_next_cycle(tmp_path, monkeypatch):
    import direct_sync_runtime as runtime
    import user_relay
    import writer_session_fence as fence
    monkeypatch.setenv(fence.TEST_MODE_ENV, '1')
    monkeypatch.setenv(fence.CONTROL_ROOT_OVERRIDE_ENV, str(tmp_path / 'control'))
    monkeypatch.setattr(user_relay, '_record_relay_reason', lambda *args, **kwargs: None)
    context = multiprocessing.get_context('spawn')
    ready = context.Event()
    job = context.Process(target=_hold_writer_for_one_second, args=(ready,))
    job.start()
    assert ready.wait(10)
    before = runtime._RELAY_LOG_DROPPED
    attempts = []
    elapsed = []

    def prepare():
        start = time.monotonic()
        _emit(tmp_path, tmp_path / 'relay.jsonl', 0, len(attempts))
        elapsed.append(time.monotonic() - start)
        attempts.append('prepare')
        if len(attempts) == 1:
            assert job.is_alive()
            assert runtime._RELAY_LOG_DROPPED == before + 1
        return {}

    def cycle(prepared):
        _emit(tmp_path, tmp_path / 'relay.jsonl', 1, len(attempts))
        attempts.append('cycle')
        return {'status': 'idle'}

    result = user_relay.run_persistent_relay_loop(
        cycle, prepare_cycle=prepare, status_path=tmp_path / 'loop.json',
        max_cycles=2, wait=lambda seconds: None,
    )
    job.join(10)
    assert job.exitcode == 0
    assert attempts == ['prepare', 'cycle', 'prepare', 'cycle']
    assert result['cycle_count'] == 2 and result['status'] == 'COMPLETED'
    assert json.loads((tmp_path / 'loop.json').read_bytes())['cycle_count'] == 2
    rows = [json.loads(line) for line in (tmp_path / 'relay.jsonl').read_bytes().splitlines()]
    dropped = runtime._RELAY_LOG_DROPPED - before
    assert dropped == 4 - len(rows) == 1
    _observe('busy-real-loop', attempts=4, rows=len(rows), dropped=dropped,
             cycles=result['cycle_count'], log_wait_seconds=elapsed)


def test_admission_access_failure_is_counted_but_log_io_error_still_propagates(tmp_path, monkeypatch):
    import direct_sync_runtime as runtime
    import writer_session_fence as fence
    before = runtime._RELAY_LOG_DROPPED

    def denied(*args, **kwargs):
        raise fence.WriterFenceError('WRITER_MUTEX_CREATE_FAILED', 'access denied')

    with monkeypatch.context() as local:
        local.setattr(fence, '_acquire_named_mutex', denied)
        runtime._append_jsonl(tmp_path / 'absent.jsonl', {'event': 'refused'})
    assert runtime._RELAY_LOG_DROPPED == before + 1
    assert not (tmp_path / 'absent.jsonl').exists()
    destination = tmp_path / 'directory'
    destination.mkdir()
    with pytest.raises(OSError):
        runtime._append_jsonl(destination, {'event': 'io-error'})
    assert runtime._RELAY_LOG_DROPPED == before + 1

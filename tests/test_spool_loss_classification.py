"""Spool loss closes its row; temporary locks retain the retry contract."""

import csv
from dataclasses import replace
import json
from pathlib import Path
import sqlite3
import pytest
import direct_sync_push as push
import direct_sync_runtime as runtime
from tests import test_direct_sync_push as push_helpers
from tests import test_direct_sync_runtime as runtime_helpers
from tests import test_direct_sync_relay_runner as runner_helpers
from tests.test_direct_sync_runtime import _isolate_legacy_runtime_tests_from_runtime_lease
from tools import direct_sync_relay_runner as runner
import os
from types import SimpleNamespace
import Label_Match as label
from tests.test_direct_sync_relay_runner import isolated_data_generation_release

_REAL_PREPARE = push.prepare_runtime_metadata
_REAL_MODE = push.client_runtime_lease_mode

def _read_error(kind):
    if kind == 'missing':
        return FileNotFoundError('synthetic missing spool')
    if kind == 'permission':
        return PermissionError('synthetic spool lock')
    error = OSError('synthetic spool I/O')
    if kind.startswith('win'):
        error.winerror = int(kind[3:])
    return error


def _row(database, relay_id=None):
    with sqlite3.connect(database) as conn:
        conn.row_factory = sqlite3.Row
        if relay_id:
            return dict(conn.execute(
                'SELECT * FROM direct_sync_relay_batches WHERE relay_id=?', (relay_id,)
            ).fetchone())
        return dict(conn.execute('SELECT * FROM direct_sync_relay_batches').fetchone())


def _enqueue(tmp_path):
    _, manifest = push_helpers.make_manifest(tmp_path)
    credentials = push_helpers.make_credentials()
    database = tmp_path / 'relay.sqlite3'
    row = push.enqueue_source_file_for_relay(
        db_path=database, spool_dir=tmp_path / 'spool',
        source_file_path=push_helpers.write_csv(tmp_path),
        producer_manifest_path=manifest, credentials=credentials,
    )
    return database, row, credentials

@pytest.mark.parametrize('failure', ['missing', 'io', 'permission', 'win5', 'win32', 'win33'])
def test_body_reopen_uses_the_same_spool_error_classification(tmp_path, monkeypatch, failure):
    database, row, credentials = _enqueue(tmp_path)
    spool = Path(row.spooled_file_path)
    original_open = Path.open
    reads = 0
    error = _read_error(failure)
    with monkeypatch.context() as patch:
        def reopen(path, mode='r', *args, **kwargs):
            nonlocal reads
            if path == spool and mode == 'rb':
                reads += 1
                if reads == 2:
                    raise error
            return original_open(path, mode, *args, **kwargs)
        patch.setattr(Path, 'open', reopen)
        session = runtime_helpers.EchoAcceptedSession()
        result = push.drain_one_relay_batch(db_path=database, credentials=credentials, session=session)
    locked = failure not in {'missing', 'io'}
    assert result.retryable is locked
    assert _row(database)['status'] == (push.RELAY_STATUS_RETRY_WAIT if locked else push.RELAY_STATUS_FAILED_PERMANENT)
    assert not session.calls


@pytest.mark.parametrize('mode', ['observe', 'enforce'])
@pytest.mark.parametrize('failure_point', ['digest', 'body'])
@pytest.mark.parametrize('failure', ['missing', 'io'])
def test_terminal_spool_loss_after_lost_ack_preserves_the_original_binding(
    tmp_path, monkeypatch, mode, failure_point, failure,
):
    from tests.test_producer_runtime_client import (
        _DataPathServer, _authority_row, _credentials, _data_path_cycle, _make_pending_relay,
    )
    database = tmp_path / 'authority.sqlite3'
    server = _DataPathServer()
    credentials = _credentials(runtime_lease_mode=mode)
    push.init_relay_queue_schema(database)
    with monkeypatch.context() as patch:
        patch.setattr(push, 'prepare_runtime_metadata', _REAL_PREPARE)
        patch.setattr(push, 'client_runtime_lease_mode', _REAL_MODE)
        _data_path_cycle(database, server, 0, tmp_path, mode=mode)
        _make_pending_relay(database, tmp_path, 'original')
        server.lose_source_ack = True
        _, first = _data_path_cycle(database, server, 1, tmp_path, mode=mode)
        server.lose_source_ack = False
        assert first.retryable is True
        authority_before = dict(_authority_row(database))
        metadata_before = json.loads(_row(database)['metadata_json'])
        spool = Path(_row(database)['spooled_file_path'])
        original_open = Path.open
        reads = 0
        error = _read_error(failure)
        with monkeypatch.context() as failed_read:
            if failure_point == 'digest':
                def unreadable(*args, **kwargs):
                    raise error
                failed_read.setattr(push, '_read_file_digest', unreadable)
            else:
                def reopen(path, mode='r', *args, **kwargs):
                    nonlocal reads
                    if path == spool and mode == 'rb':
                        reads += 1
                        if reads == 2:
                            raise error
                    return original_open(path, mode, *args, **kwargs)
                failed_read.setattr(Path, 'open', reopen)
            _, closed = _data_path_cycle(database, server, 2, tmp_path, mode=mode)
        assert closed.retryable is False
        assert _row(database)['status'] == push.RELAY_STATUS_FAILED_PERMANENT
        assert dict(_authority_row(database)) == authority_before
        assert json.loads(_row(database)['metadata_json']) == metadata_before
        _make_pending_relay(database, tmp_path, 'following')
        following = push.drain_one_relay_batch(
            db_path=database, credentials=credentials, session=server, target_relay_id='following',
        )
        assert following.retryable is True
        assert dict(_authority_row(database)) == authority_before
        assert len(server.source_calls) == 1
        assert server.audit == []


def test_oserror_after_post_starts_keeps_the_ambiguous_retry_contract(tmp_path):
    database, row, credentials = _enqueue(tmp_path)
    class AmbiguousSession:
        def post(self, *args, **kwargs):
            raise FileNotFoundError('synthetic failure after POST started')
    result = push.drain_one_relay_batch(
        db_path=database, credentials=credentials, session=AmbiguousSession(),
    )
    assert result.retryable is True
    assert result.error_code == 'transport_error'
    assert _row(database)['status'] == push.RELAY_STATUS_RETRY_WAIT


def test_missing_spool_closes_only_its_row_without_post(tmp_path):
    database, row, credentials = _enqueue(tmp_path)
    Path(row.spooled_file_path).unlink()
    session = runtime_helpers.EchoAcceptedSession()
    result = push.drain_one_relay_batch(db_path=database, credentials=credentials, session=session)
    current = _row(database)
    assert result.error_code == 'spooled_file_missing'
    assert result.retryable is False
    assert current['status'] == push.RELAY_STATUS_FAILED_PERMANENT
    assert current['next_attempt_at'] is None
    assert not session.calls


@pytest.mark.parametrize('winerror', [None, 5, 32, 33])
def test_spool_lock_waits_and_sends_the_same_row_after_unlock(tmp_path, monkeypatch, winerror):
    database, row, credentials = _enqueue(tmp_path)
    spool_before = Path(row.spooled_file_path).read_bytes()
    error = PermissionError('synthetic spool lock') if winerror is None else OSError('synthetic spool lock')
    if winerror is not None:
        error.winerror = winerror
    session = runtime_helpers.EchoAcceptedSession()
    original_digest = push._read_file_digest
    with monkeypatch.context() as patch:
        def locked(path):
            if Path(path) == Path(row.spooled_file_path):
                raise error
            return original_digest(path)
        patch.setattr(push, '_read_file_digest', locked)
        result = push.drain_one_relay_batch(db_path=database, credentials=credentials, session=session)
    current = _row(database)
    assert result.retryable is True
    assert current['status'] == push.RELAY_STATUS_RETRY_WAIT
    assert current['next_attempt_at']
    assert not session.calls
    assert Path(row.spooled_file_path).read_bytes() == spool_before
    with sqlite3.connect(database) as conn:
        conn.execute('UPDATE direct_sync_relay_batches SET next_attempt_at=?', ('2000-01-01T00:00:00Z',))
    recovered = push.drain_one_relay_batch(db_path=database, credentials=credentials, session=session)
    assert recovered.success is True
    assert _row(database)['relay_id'] == row.relay_id
    assert _row(database)['status'] == push.RELAY_STATUS_ACKED
    assert len(session.calls) == 1


def test_missing_delta_counts_in_public_status_and_band_and_blocks_only_its_source(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(runner, 'run_relay_once', lambda config, **kw: {'status': 'idle'})
    source = runner_helpers.write_label_csv(tmp_path / 'sources')
    args = runner_helpers.runner_args(tmp_path, scan_dir=source.parent)
    # The drain stub deliberately reports no ACK, so targeted completion is 1.
    assert runner.main(args) == 1
    capsys.readouterr()
    database = Path(args[args.index('--db-path') + 1])
    row = _row(database)
    assert row['status'] == push.RELAY_STATUS_PENDING
    Path(row['spooled_file_path']).unlink()
    result = push.drain_one_relay_batch(
        db_path=database, credentials=push_helpers.make_credentials(),
        session=runtime_helpers.EchoAcceptedSession(),
    )
    assert result.retryable is False
    records = list(csv.reader(source.read_text(encoding='utf-8-sig').splitlines()))
    with source.open('a', encoding='utf-8', newline='') as stream:
        csv.writer(stream).writerow(records[-1])
    runner_helpers.write_label_csv(source.parent, name='포장실작업이벤트로그_other_20260622.csv')
    assert runner.main(args) != 0
    capsys.readouterr()
    status_path = Path(args[args.index('--runtime-status-path') + 1])
    status = json.loads(status_path.read_text(encoding='utf-8'))
    assert status['scan_terminal_blocked_count'] == 1
    assert status['scan_terminal_blocked_record_count'] == len(records)
    public = json.loads((Path(os.environ['LOCALAPPDATA']) / 'KMTech' / 'Label_Match' / 'upload_block_status.json').read_text(encoding='utf-8'))
    assert public['blocked_files'] == 1
    assert public['blocked_records'] == len(records)
    assert '막힌 기록 파일 1개' in label._label_match_send_warning(SimpleNamespace(), status)
    with sqlite3.connect(database) as conn:
        rows = conn.execute('SELECT relay_id,status FROM direct_sync_relay_batches').fetchall()
    assert len(rows) == 2
    assert dict(rows)[row['relay_id']] == push.RELAY_STATUS_FAILED_PERMANENT


def test_missing_aged_spool_does_not_stop_new_enqueues_after_24_hours(tmp_path):
    config = runtime_helpers.make_config(tmp_path)
    first = runtime_helpers.write_csv(tmp_path, name='aged.csv', barcode='one')
    runtime.enqueue_completed_source_file(config, source_file_path=first)
    original = _row(config.db_path)
    Path(original['spooled_file_path']).unlink()
    with sqlite3.connect(config.db_path) as conn:
        conn.execute('UPDATE direct_sync_relay_batches SET created_at=?', ('2000-01-01T00:00:00Z',))
    config = replace(config, max_active_queue_age_seconds=24 * 60 * 60)
    for name in ('new-one.csv', 'new-two.csv'):
        result = runtime.enqueue_completed_source_file(
            config, source_file_path=runtime_helpers.write_csv(tmp_path, name=name, barcode=name),
            backpressure_recovery_session=runtime_helpers.EchoAcceptedSession(),
        )
        assert result['status'] == 'enqueued'
    assert _row(config.db_path, original['relay_id'])['status'] == push.RELAY_STATUS_FAILED_PERMANENT
    counts = push.relay_queue_status(config.db_path)['counts']
    assert counts[push.RELAY_STATUS_PENDING] == 2
    assert counts[push.RELAY_STATUS_FAILED_PERMANENT] == 1

"""A terminal spool loss must not strand the authority behind a missing ACK."""
import csv
import hashlib
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest
import direct_sync_push as push
import direct_sync_runtime as runtime
from tools import direct_sync_relay_runner as runner
from tests.test_producer_runtime_client import (
    _DataPathServer, _authority_row, _credentials, _data_path_cycle, _make_pending_relay,
)
from tests.test_direct_sync_runtime import _isolate_legacy_runtime_tests_from_runtime_lease
from tests.test_direct_sync_relay_runner import isolated_data_generation_release

_REAL_PREPARE = push.prepare_runtime_metadata
_REAL_MODE = push.client_runtime_lease_mode
_REAL_ENSURE = runtime.ensure_runtime_authority


def _row(database, relay_id='original'):
    with sqlite3.connect(database) as conn:
        conn.row_factory = sqlite3.Row
        return dict(conn.execute('SELECT * FROM direct_sync_relay_batches WHERE relay_id=?', (relay_id,)).fetchone())


def _seed(tmp_path, monkeypatch, *, direct=False):
    monkeypatch.setattr(push, 'prepare_runtime_metadata', _REAL_PREPARE)
    monkeypatch.setattr(push, 'client_runtime_lease_mode', _REAL_MODE)
    database = tmp_path / 'relay.sqlite3'
    config = SimpleNamespace(db_path=database, spool_dir=tmp_path / 'spool')
    source = tmp_path / '포장실작업이벤트로그_held.csv'
    with source.open('w', encoding='utf-8', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(['timestamp', 'worker_name', 'event', 'details'])
        writer.writerow(['2026-10-26T00:00:00Z', 'test', 'APP_START', '{}'])
    push.init_relay_queue_schema(database)
    delta, relative, _ = runner._build_delta_source_file(config, source)
    if direct:
        relative = source.name
    body = delta.read_bytes()
    server = _DataPathServer()
    _data_path_cycle(database, server, 0, tmp_path)
    _make_pending_relay(database, tmp_path, 'original')
    row = _row(database)
    metadata = json.loads(row['metadata_json'])
    digest = hashlib.sha256(body).hexdigest()
    metadata.update(relative_path=relative, content_sha256=digest, byte_length=len(body))
    Path(row['spooled_file_path']).write_bytes(body)
    with sqlite3.connect(database) as conn:
        conn.execute('UPDATE direct_sync_relay_batches SET source_file_path=?,relative_path=?,content_sha256=?,byte_length=?,metadata_json=? WHERE relay_id=?',
                     (str(source if direct else delta), relative, digest, len(body), push.canonical_json(metadata), 'original'))
    server.lose_source_ack = True
    _, first = _data_path_cycle(database, server, 1, tmp_path)
    assert first.retryable is True
    server.lose_source_ack = False
    held = dict(_authority_row(database))
    wire = json.loads(_row(database)['metadata_json'])
    Path(_row(database)['spooled_file_path']).unlink()
    delta.unlink()
    _, missing = _data_path_cycle(database, server, 2, tmp_path)
    assert missing.success is False
    assert _row(database)['status'] == push.RELAY_STATUS_FAILED_PERMANENT
    assert dict(_authority_row(database)) == held
    return database, config, source, server, held, wire, body


def _scan(config, source):
    try:
        return runner._build_delta_source_file(config, source)
    except runner.ExistingTerminalDeltaBlocked:
        return None


def _run_cli(tmp_path, monkeypatch, source, server, wire, *, single=False, source_glob=None):
    from tests.test_direct_sync_relay_runner import runner_args
    credentials = _credentials()
    monkeypatch.setattr(runtime, 'load_credentials_from_json', lambda *a, **kw: credentials)
    args = runner_args(tmp_path, scan_dir=source.parent, producer_install_id=wire['producer_install_id'],
                       source_host_id=wire['source_host_id'], producer_id=credentials.producer_id,
                       key_id=credentials.key_id, secret=credentials.secret.decode('utf-8'))
    if single:
        position = args.index('--scan-source-dir')
        del args[position:position + 2]
        args += ['--enqueue-source-file', str(source)]
    if source_glob is not None:
        args[args.index('--source-glob') + 1] = source_glob
    monkeypatch.setattr(runtime, 'ensure_runtime_authority', _REAL_ENSURE)
    monkeypatch.setattr(runner, 'run_relay_once', lambda current, **kw: runtime.run_relay_once(
        current, credentials=credentials, session=server, **kw))
    return runner.main(args)


@pytest.mark.parametrize('direct', [False, True])
def test_same_range_replay_recovers_lost_ack_and_unblocks_following(tmp_path, monkeypatch, direct):
    database, config, source, server, held, wire, body = _seed(tmp_path, monkeypatch, direct=direct)
    with source.open('a', encoding='utf-8', newline='') as stream:
        csv.writer(stream).writerow(['2026-10-26T00:01:00Z', 'test', 'APP_START', '{}'])
    _scan(config, source)
    _, result = _data_path_cycle(database, server, 3, tmp_path)
    assert result is not None and result.success is True
    assert _row(database)['status'] == push.RELAY_STATUS_ACKED
    assert Path(_row(database)['spooled_file_path']).read_bytes() == body
    assert server.source_calls[-1]['idempotency_key'] == wire['idempotency_key']
    assert _authority_row(database)['assigned_relay_id'] is None
    _make_pending_relay(database, tmp_path, 'following')
    _, following = _data_path_cycle(database, server, 4, tmp_path)
    assert following.success is True
    assert _row(database, 'following')['status'] == push.RELAY_STATUS_ACKED
    assert server.audit == []


@pytest.mark.parametrize('failure', ['changed', 'missing'])
def test_unrecoverable_original_retains_binding_and_shows_all_waiting_records(tmp_path, monkeypatch, failure):
    database, config, source, server, held, wire, body = _seed(tmp_path, monkeypatch)
    if failure == 'changed':
        source.write_bytes(body.replace(b'APP_START', b'APP_OTHER'))
    else:
        source.unlink()
    _run_cli(tmp_path, monkeypatch, source, server, wire)
    _make_pending_relay(database, tmp_path, 'following')
    _, result = _data_path_cycle(database, server, 3, tmp_path)
    assert result is None
    reviewed = _authority_row(database)
    assert reviewed['status'] == 'OPERATOR_REVIEW'
    for field in ('assigned_relay_id', 'next_request_token', 'next_request_sequence'):
        assert reviewed[field] == held[field]
    assert json.loads(_row(database)['metadata_json']) == wire
    assert len(server.source_calls) == 1
    files, records, _ = runner._held_runtime_backlog(config, [source] if source.exists() else [])
    assert files == 2 and records is None


def test_recovery_transient_response_keeps_binding_and_retries_after_backoff(tmp_path, monkeypatch):
    database, config, source, server, held, wire, body = _seed(tmp_path, monkeypatch)
    _scan(config, source)
    server.lose_source_ack = True
    _, first = _data_path_cycle(database, server, 3, tmp_path)
    assert first is not None and first.retryable is True
    assert len(server.source_calls) == 2
    assert dict(_authority_row(database)) == held
    assert json.loads(_row(database)['metadata_json']) == wire
    assert _row(database)['next_attempt_at']
    assert push.drain_one_relay_batch(db_path=database, credentials=_credentials(), session=server,
                                     target_relay_id='original') is None
    server.lose_source_ack = False
    due = _row(database)['next_attempt_at']
    monkeypatch.setattr(push, 'utc_now_text', lambda: due)
    _, recovered = _data_path_cycle(database, server, 4, tmp_path)
    assert recovered.success is True
    assert _row(database)['status'] == push.RELAY_STATUS_ACKED
    assert server.audit == []


@pytest.mark.parametrize('status,valid_json,committed', [
    (408, True, False), (408, True, True), (429, True, False), (429, True, True),
    (500, True, False), (500, True, True), (599, True, False), (599, True, True),
    (409, True, False), (409, True, True), (400, False, False), (200, False, False),
])
def test_recovery_server_refusal_does_not_release_the_prior_assignment(tmp_path, monkeypatch, status, valid_json, committed):
    database, config, source, server, held, wire, body = _seed(tmp_path, monkeypatch)
    _scan(config, source)
    post = server.post
    attempts = []
    def rejected(url, **kwargs):
        if '/source-file' not in url:
            return post(url, **kwargs)
        attempts.append(json.loads(kwargs['data']['metadata']))
        def payload():
            if not valid_json:
                raise ValueError('synthetic invalid response')
            return {'committed': committed, 'retryable': False, 'error': {'code': 'CONTENT_CONFLICT'}}
        return SimpleNamespace(status_code=status, headers={}, json=payload, text='')
    monkeypatch.setattr(server, 'post', rejected)
    _, refused = _data_path_cycle(database, server, 3, tmp_path)
    assert refused.retryable is True
    assert len(attempts) == 1
    assert attempts[0] == wire
    assert dict(_authority_row(database)) == held
    assert json.loads(_row(database)['metadata_json']) == wire
    due = _row(database)['next_attempt_at']
    assert due
    assert push.drain_one_relay_batch(db_path=database, credentials=_credentials(), session=server,
                                     target_relay_id='original') is None
    monkeypatch.setattr(server, 'post', post)
    monkeypatch.setattr(push, 'utc_now_text', lambda: due)
    _, recovered = _data_path_cycle(database, server, 4, tmp_path)
    assert recovered.success is True
    assert server.audit == []


def _queue_delta(database, config, source, relay_id):
    delta, relative, _ = runner._build_delta_source_file(config, source)
    body = delta.read_bytes()
    _make_pending_relay(database, source.parent, relay_id)
    row = _row(database, relay_id)
    metadata = json.loads(row['metadata_json'])
    digest = hashlib.sha256(body).hexdigest()
    metadata.update(relative_path=relative, content_sha256=digest, byte_length=len(body))
    Path(row['spooled_file_path']).write_bytes(body)
    with sqlite3.connect(database) as conn:
        conn.execute('UPDATE direct_sync_relay_batches SET source_file_path=?,relative_path=?,content_sha256=?,byte_length=?,metadata_json=? WHERE relay_id=?',
                     (str(delta), relative, digest, len(body), push.canonical_json(metadata), relay_id))


def test_backlog_counts_original_records_once_across_queued_ranges(tmp_path, monkeypatch):
    database, config, source, server, held, wire, body = _seed(tmp_path, monkeypatch)
    with source.open('a', encoding='utf-8', newline='') as stream:
        csv.writer(stream).writerow(['2026-10-26T00:01:00Z', 'test', 'APP_START', '{}'])
    other = source.with_name('포장실작업이벤트로그_other.csv')
    other.write_bytes(source.read_bytes())
    _queue_delta(database, config, other, 'other')
    with sqlite3.connect(database) as conn:
        columns = [item[1] for item in conn.execute('PRAGMA table_info(direct_sync_relay_batches)')]
        values = dict(_row(database))
        values.update(relay_id='overlap', status='pending')
        overlap_metadata = json.loads(values['metadata_json'])
        overlap_metadata['client_batch_id'] = 'overlap'
        for field in push.RUNTIME_METADATA_FIELDS:
            overlap_metadata.pop(field, None)
        values['metadata_json'] = push.canonical_json(overlap_metadata)
        conn.execute('INSERT INTO direct_sync_relay_batches (' + ','.join(columns) + ') VALUES (' + ','.join('?' for _ in columns) + ')',
                     [values[name] for name in columns])
    files, records, originals = runner._held_runtime_backlog(config, [source, other], [source])
    assert files == 2 and records == 4
    assert set(originals) == {str(source), str(other)}
    import os
    import Label_Match as label
    payload = {'scan_terminal_blocked_count': files, 'scan_terminal_blocked_record_count': records}
    runtime._publish_upload_block_status(config, payload)
    record = json.loads((Path(os.environ['LOCALAPPDATA']) / 'KMTech/Label_Match/upload_block_status.json').read_text(encoding='utf-8'))
    assert set(record) == {'schema', 'app', 'blocked_files', 'blocked_records', 'install_review', 'since', 'updated'}
    assert (record['blocked_records'], record['blocked_files']) == (4, 2)
    band = label._label_match_send_warning(SimpleNamespace(), payload)
    assert '4건' in band and '파일 2개' in band and '관리자' in band


def test_aged_rows_waiting_on_held_assignment_do_not_block_local_enqueue(tmp_path, monkeypatch):
    from dataclasses import replace
    from tests.test_direct_sync_runtime import make_config
    from tests.test_direct_sync_push import write_csv
    database, config, source, server, held, wire, body = _seed(tmp_path, monkeypatch)
    _make_pending_relay(database, tmp_path, 'following')
    with sqlite3.connect(database) as conn:
        conn.execute("UPDATE direct_sync_relay_batches SET created_at='2000-01-01T00:00:00Z' WHERE relay_id='following'")
    (tmp_path / 'new').mkdir()
    normal = replace(make_config(tmp_path / 'new', max_active_queue_age_seconds=86400), db_path=database)
    assert runtime._queue_backpressure_report(normal)['status'] == 'pass'
    outcome = runtime.enqueue_completed_source_file(normal, source_file_path=write_csv(tmp_path / 'new'), credentials=_credentials())
    assert outcome['status'] == 'enqueued'
    assert _authority_row(database)['assigned_relay_id'] == 'original'
    _make_pending_relay(database, tmp_path, 'other-scope')
    with sqlite3.connect(database) as conn:
        conn.execute("UPDATE direct_sync_relay_batches SET producer_id='other-producer',created_at='2000-01-01T00:00:00Z' WHERE relay_id='other-scope'")
    assert runtime._queue_backpressure_report(normal)['reasons'] == ['oldest_active_age_threshold']


def test_existing_abandon_command_reports_missing_spool_without_clearing_assignment(tmp_path, monkeypatch):
    from direct_sync_operator import retry_dead_relay_batch
    database, config, source, server, held, wire, body = _seed(tmp_path, monkeypatch)
    source.unlink()
    runner._review_missing_held_originals(config, [])
    outcome = retry_dead_relay_batch(db_path=database, relay_id='original', operator_id='test-admin',
                                    reason='synthetic recovery check', audit_log_path=tmp_path / 'operator.jsonl')
    assert outcome['status'] == 'BLOCKED'
    assert outcome['error_code'] == 'spooled_file_missing'
    assert _authority_row(database)['assigned_relay_id'] == held['assigned_relay_id']


@pytest.mark.parametrize('restore', ['original', 'other', 'changed'])
def test_admin_original_recovery_requires_the_original_bytes_and_does_not_auto_resume(tmp_path, monkeypatch, restore):
    database, config, source, server, held, wire, body = _seed(tmp_path, monkeypatch)
    source.unlink()
    runner._review_missing_held_originals(config, [])
    assert _authority_row(database)['status'] == 'OPERATOR_REVIEW'
    candidate = source if restore != 'other' else source.with_name('포장실작업이벤트로그_different.csv')
    candidate.write_bytes(body if restore != 'changed' else body.replace(b'APP_START', b'APP_OTHER'))
    _scan(config, candidate)
    assert _authority_row(database)['status'] == 'OPERATOR_REVIEW'
    assert len(server.source_calls) == 1
    repaired = runner._repair_held_assignments_before_scan(config, candidate, administrator=True)
    if restore != 'original':
        assert repaired == []
        assert _authority_row(database)['status'] == 'OPERATOR_REVIEW'
        assert _authority_row(database)['assigned_relay_id'] == 'original'
    else:
        assert repaired == ['original']
        assert _authority_row(database)['status'] == 'ACTIVE'
        assert json.loads(_row(database)['metadata_json']) == wire
        _, recovered = _data_path_cycle(database, server, 3, tmp_path)
        assert recovered.success is True
        assert _row(database)['status'] == push.RELAY_STATUS_ACKED
        assert _authority_row(database)['assigned_relay_id'] is None


def test_admin_recovery_rejects_a_different_runtime_assignment(tmp_path, monkeypatch):
    database, config, source, server, held, wire, body = _seed(tmp_path, monkeypatch)
    source.unlink()
    runner._review_missing_held_originals(config, [])
    source.write_bytes(body)
    with sqlite3.connect(database) as conn:
        conn.execute("UPDATE direct_sync_runtime_authority SET runtime_instance_id='different-runtime'")
    assert runner._repair_held_assignments_before_scan(config, source, administrator=True) == []
    assert _authority_row(database)['status'] == 'OPERATOR_REVIEW'
    assert _authority_row(database)['assigned_relay_id'] == 'original'
    assert len(server.source_calls) == 1


def test_existing_single_source_cli_replays_and_clears_the_public_review(tmp_path, monkeypatch, capsys):
    import os
    database, config, source, server, held, wire, body = _seed(tmp_path, monkeypatch)
    source.unlink()
    runner._review_missing_held_originals(config, [])
    source.write_bytes(body)
    runtime._publish_upload_block_status(config, {'scan_terminal_blocked_count': 1,
        'scan_terminal_blocked_record_count': None, 'runtime_lease': {'status': 'operator_review'}})
    assert _run_cli(tmp_path, monkeypatch, source, server, wire, single=True) == 0
    capsys.readouterr()
    assert _row(database)['status'] == push.RELAY_STATUS_ACKED
    assert _authority_row(database)['assigned_relay_id'] is None
    public = json.loads((Path(os.environ['LOCALAPPDATA']) / 'KMTech/Label_Match/upload_block_status.json').read_text(encoding='utf-8'))
    assert public['blocked_files'] == public['blocked_records'] == 0
    assert public['install_review'] is False
    assert public['since'] is None


def test_narrow_scan_pattern_does_not_treat_an_existing_original_as_missing(tmp_path, monkeypatch, capsys):
    database, config, source, server, held, wire, body = _seed(tmp_path, monkeypatch)
    _run_cli(tmp_path, monkeypatch, source, server, wire, source_glob='포장실작업이벤트로그_other*.csv')
    capsys.readouterr()
    assert _authority_row(database)['status'] == 'ACTIVE'
    assert _authority_row(database)['assigned_relay_id'] == 'original'
    assert len(server.source_calls) == 1


@pytest.mark.parametrize('failure_type', [OSError, RuntimeError, KeyboardInterrupt, SystemExit])
def test_real_repair_copy_failure_isolates_original_and_updates_scan_status(tmp_path, monkeypatch, capsys, failure_type):
    database, config, source, server, held, wire, body = _seed(tmp_path, monkeypatch)
    other = source.with_name('포장실작업이벤트로그_zz-other.csv')
    other.write_bytes(body)
    before = _row(database)
    original_copy = push._copy_file_atomic
    def denied(copy_source, destination):
        if copy_source == Path(before['source_file_path']):
            raise failure_type('synthetic repair copy failure')
        return original_copy(copy_source, destination)
    with monkeypatch.context() as blocked:
        blocked.setattr(push, '_copy_file_atomic', denied)
        if not issubclass(failure_type, Exception):
            with pytest.raises(failure_type):
                _run_cli(tmp_path, monkeypatch, source, server, wire)
            assert _row(database) == before
            assert dict(_authority_row(database)) == held
            return
        _run_cli(tmp_path, monkeypatch, source, server, wire)
    capsys.readouterr()
    assert _row(database) == before
    assert dict(_authority_row(database)) == held
    with sqlite3.connect(database) as conn:
        assert conn.execute('SELECT COUNT(*) FROM direct_sync_relay_batches').fetchone()[0] == 2
    public = json.loads((Path(__import__('os').environ['LOCALAPPDATA']) / 'KMTech/Label_Match/upload_block_status.json').read_text(encoding='utf-8'))
    assert public['blocked_files'] == 2
    assert public['blocked_records'] == 2
    status = json.loads((tmp_path / 'runtime/status.json').read_text(encoding='utf-8'))
    assert str(source) in status['scan_terminal_blocked_source_files']
    assert len(server.source_calls) == 1
    _scan(config, source)
    _, recovered = _data_path_cycle(database, server, 3, tmp_path)
    assert recovered.success is True

"""An uncommitted clock rejection must remain deliverable after clock repair."""

from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import csv
import io
import socket
import requests

import pytest

import direct_sync_push as push
import direct_sync_runtime as runtime
import producer_runtime_client as leases
import Label_Match as label
import label_screen_bridge as bridge
from types import SimpleNamespace
from tests.test_direct_sync_push import (
    FakeResponse, SequenceFakeSession, make_credentials, make_manifest, write_csv,
)
from tests.test_direct_sync_runtime import make_config
from tools import direct_sync_relay_runner as runner
from tests.test_direct_sync_relay_runner import (
    isolated_data_generation_release, write_label_csv, runner_args, disable_scan_drain,
)


SERVER_DATE = 'Mon, 26 Oct 2026 05:00:00 GMT'
PC_TIME = datetime(2026, 10, 26, 5, 10, tzinfo=timezone.utc)
CLOCK = {
    'pc_utc': '2026-10-26T05:10:00Z',
    'server_utc': '2026-10-26T05:00:00Z',
    'seconds': 600.0,
}


def refusal(code='stale_timestamp', status=401, *, date=SERVER_DATE):
    return FakeResponse(status, {
        'committed': False, 'retryable': False,
        'error': {'code': code, 'message': 'request refused'},
    }, headers={'Date': date} if date is not None else {})


@pytest.fixture
def queued(tmp_path, monkeypatch):
    _, manifest = make_manifest(tmp_path)
    source = write_csv(tmp_path)
    credentials = make_credentials()
    database = tmp_path / 'queue.sqlite3'
    row = push.enqueue_source_file_for_relay(
        db_path=database, spool_dir=tmp_path / 'spool',
        source_file_path=source, producer_manifest_path=manifest,
        credentials=credentials,
    )
    monkeypatch.setattr(push, 'prepare_runtime_metadata',
                        lambda **kw: leases.RuntimePreparation(metadata=dict(kw['metadata'])))
    monkeypatch.setattr(push, 'client_runtime_lease_mode', lambda _credentials: 'observe')
    return database, row, credentials


def queue_row(database):
    with sqlite3.connect(database) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute('SELECT * FROM direct_sync_relay_batches').fetchone()


def due(database):
    with sqlite3.connect(database) as conn:
        conn.execute("UPDATE direct_sync_relay_batches SET next_attempt_at='2000-01-01T00:00:00Z'")


def accepted(row):
    metadata = row.metadata
    return FakeResponse(200, {
        'client_batch_id': row.relay_id,
        'server_source_file_id': (
            metadata['source_host_id'] + '/label_match/label_match_events/' + row.relative_path
        ),
        'committed': True, 'status': 'accepted', 'projection_disposition': 'COMPLETE',
        'retryable': False, 'next_retry_after': None,
        'totals': {'inserted': metadata['row_count'], 'replayed': 0, 'quarantined': 0, 'errors': 0},
    })


@pytest.mark.parametrize(('status', 'code', 'retryable'), [
    (401, 'stale_timestamp', True),
    (401, 'invalid_signature', False),
    (401, 'STALE_TIMESTAMP', False),
    (403, 'stale_timestamp', False),
])
def test_only_exact_clock_401_is_retryable(queued, tmp_path, status, code, retryable):
    database, row, credentials = queued
    result = push.drain_one_relay_batch(
        db_path=database, credentials=credentials,
        session=SequenceFakeSession([refusal(code, status)]), status_dir=tmp_path / 'status',
    )
    assert result.retryable is retryable
    current = queue_row(database)
    assert current['status'] == ('retry_wait' if retryable else 'failed_permanent')
    assert bool(current['next_attempt_at']) is retryable


@pytest.mark.parametrize('mode', ['observe', 'enforce'])
def test_clock_repair_retries_same_spool_with_fresh_signature(queued, tmp_path, monkeypatch, mode):
    database, row, credentials = queued
    monkeypatch.setattr(push, 'client_runtime_lease_mode', lambda _credentials: mode)
    original_spool = Path(row.spooled_file_path).read_bytes()
    session = SequenceFakeSession([refusal(), accepted(row)])
    first = push.drain_one_relay_batch(
        db_path=database, credentials=credentials, session=session,
        status_dir=tmp_path / 'status', retry_base_seconds=1,
    )
    assert first.retryable is True
    assert queue_row(database)['status'] == 'retry_wait'
    assert push.drain_one_relay_batch(db_path=database, credentials=credentials, session=session) is None
    due(database)
    second = push.drain_one_relay_batch(
        db_path=database, credentials=credentials, session=session,
        status_dir=tmp_path / 'status', retry_base_seconds=1,
    )
    assert second.success is True
    assert queue_row(database)['status'] == 'acked'
    assert queue_row(database)['attempt_count'] == 2
    assert Path(row.spooled_file_path).read_bytes() == original_spool
    assert session.calls[0]['headers']['X-Producer-Nonce'] != session.calls[1]['headers']['X-Producer-Nonce']
    assert session.calls[0]['metadata'] == session.calls[1]['metadata']


@pytest.mark.parametrize('date', [SERVER_DATE, None, 'invalid', 'Mon, 26 Oct 2026 05:00:00'])
def test_date_diagnostic_is_local_minus_server_and_optional(queued, tmp_path, monkeypatch, date):
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return PC_TIME.astimezone(tz)

    monkeypatch.setattr(leases, 'datetime', FixedDateTime)
    database, _, credentials = queued
    result = push.drain_one_relay_batch(
        db_path=database, credentials=credentials,
        session=SequenceFakeSession([refusal(date=date)]), status_dir=tmp_path / 'status',
    )
    artifact = json.loads(Path(result.status_path).read_text(encoding='utf-8'))
    summary = runtime._result_summary(result)
    if date == SERVER_DATE:
        assert result.receipt['_local_clock_skew'] == CLOCK
        assert artifact['clock_skew'] == CLOCK
        assert summary['clock_skew'] == CLOCK
        assert json.loads(queue_row(database)['receipt_json'])['_local_clock_skew'] == CLOCK
    else:
        assert '_local_clock_skew' not in result.receipt
        assert 'clock_skew' not in artifact
        assert 'clock_skew' not in summary


@pytest.mark.parametrize('code', ['stale_timestamp', 'invalid_signature'])
def test_row_lease_clock_refusal_is_not_terminal(queued, tmp_path, monkeypatch, code):
    database, _, credentials = queued

    class LeaseSession:
        def post(self, *_args, **_kwargs):
            return refusal(code)

    def prepare(**_kwargs):
        _, error = leases._post_lease_request(
            request_value={}, credentials=credentials, session=LeaseSession(), timeout=1,
        )
        return error

    monkeypatch.setattr(push, 'prepare_runtime_metadata', prepare)
    result = push.drain_one_relay_batch(db_path=database, credentials=credentials)
    assert result.retryable is (code == 'stale_timestamp')
    assert queue_row(database)['status'] == ('retry_wait' if code == 'stale_timestamp' else 'failed_permanent')
    assert bool(queue_row(database)['next_attempt_at']) is result.retryable
    if result.retryable:
        assert '_local_clock_skew' in result.receipt


def test_clock_wait_does_not_block_later_csv_delta(tmp_path, monkeypatch):
    config = make_config(tmp_path)
    source = write_csv(tmp_path)
    delta, relative, _ = runner._build_delta_source_file(config, source)
    enqueued = runtime.enqueue_completed_source_file(config, source_file_path=delta, relative_path=relative)
    assert enqueued['status'] == 'enqueued'
    credentials = runtime.load_credentials_from_json(config.credential_path)
    monkeypatch.setattr(push, 'prepare_runtime_metadata',
                        lambda **kw: leases.RuntimePreparation(metadata=dict(kw['metadata'])))
    monkeypatch.setattr(push, 'client_runtime_lease_mode', lambda _credentials: 'observe')
    result = push.drain_one_relay_batch(
        db_path=config.db_path, credentials=credentials,
        session=SequenceFakeSession([refusal()]),
    )
    assert result.error_code == 'stale_timestamp'
    with source.open('ab') as stream:
        stream.write(source.read_bytes().splitlines(keepends=True)[-1])
    # The previous implementation raises ExistingTerminalDeltaBlocked here.
    later = runner._build_delta_source_file(config, source)
    assert later is not None
    assert later[1] != relative
    assert queue_row(config.db_path)['status'] == 'retry_wait'


def test_committed_clock_label_does_not_override_receipt_safety(queued, tmp_path):
    database, _, credentials = queued
    response = refusal()
    response._payload['committed'] = True
    result = push.drain_one_relay_batch(
        db_path=database, credentials=credentials,
        session=SequenceFakeSession([response]), status_dir=tmp_path / 'status',
    )
    assert result.retryable is False
    assert queue_row(database)['status'] == 'operator_review'


def test_clock_warning_survives_save_and_restart_until_successful_upload(tmp_path):
    config = make_config(tmp_path)
    refused = {'status': 'retry_wait', 'status_code': 401,
               'error_code': 'stale_timestamp', 'clock_skew': CLOCK}
    first = runtime._write_runtime_status(
        config, status='retry_wait', queue={}, disk={}, last_result=refused,
    )
    assert first['clock_error']['clock_skew'] == CLOCK
    saved = runtime._write_runtime_status(config, status='enqueued', queue={}, disk={})
    assert saved['clock_error'] == first['clock_error']
    assert label._label_match_clock_warning(saved).startswith('이 PC 시계가 회사 서버와 5분 넘게 달라')
    # A later retry/idle/lease success alone does not prove delivery recovered.
    idle = runtime._write_runtime_status(config, status='idle', queue={}, disk={})
    assert idle['clock_error'] == first['clock_error']
    recovered = runtime._write_runtime_status(
        config, status='acked', queue={}, disk={}, last_result={'status': 'acked', 'success': True},
    )
    assert 'clock_error' not in recovered
    assert label._label_match_clock_warning(recovered) == ''


def test_preflight_clock_warning_survives_scan_status_rewrite(tmp_path):
    config = make_config(tmp_path)
    initial = runtime._write_runtime_status(
        config, status='runtime_error', queue={}, disk={},
        runtime_lease={'status_code': 401, 'error_code': 'stale_timestamp', '_local_clock_skew': CLOCK},
    )
    assert initial['clock_error']['clock_skew'] == CLOCK
    scan = {'status': 'scan_no_new_rows'}
    runner._persist_scan_runtime_status(config, scan)
    persisted = json.loads(Path(config.runtime_status_path).read_text(encoding='utf-8'))
    assert persisted['clock_error'] == initial['clock_error']


def test_screen_poll_connects_clock_warning_and_automatic_clear(tmp_path):
    status_path = tmp_path / 'status.json'
    app = SimpleNamespace(
        direct_sync_bootstrap_context={'runtime_status_path': str(status_path)},
        _process_app_stop_request=lambda: False,
        _read_logistics_transmission_summary=lambda _path: (0, None),
        _render_relay_screen_status=lambda _state: None,
        _render_logistics_connection=lambda *_args: None,
        _render_logistics_progress=lambda: None,
        _refresh_relay_screen_status=lambda: None,
        after=lambda *_args: 'next-poll',
    )
    status_path.write_text(json.dumps({
        'status': 'retry_wait',
        'clock_error': {'status_code': 401, 'error_code': 'stale_timestamp'},
    }), encoding='utf-8')
    label.Label_Match._refresh_relay_screen_status(app)
    assert app._logistics_clock_warning
    status_path.write_text(json.dumps({'status': 'acked'}), encoding='utf-8')
    app._relay_screen_status_mtime_ns = -1
    label.Label_Match._refresh_relay_screen_status(app)
    assert app._logistics_clock_warning == ''


@pytest.mark.parametrize('known_date', [False, True])
def test_clock_band_is_yellow_has_no_action_and_has_send_priority(monkeypatch, known_date):
    diagnostic = {'status_code': 401, 'error_code': 'stale_timestamp'}
    if known_date:
        diagnostic['clock_skew'] = CLOCK
    text = label._label_match_clock_warning({'clock_error': diagnostic})
    assert text.splitlines()[0] == (
        '이 PC 시계가 회사 서버와 5분 넘게 달라 기록을 보내지 못하고 있습니다 — 현장 관리자에게 알리세요'
    )
    assert text.splitlines()[1].startswith('찍은 기록은 이 PC에 그대로 있고, 시계를 맞추면 저절로 보내집니다')
    if known_date:
        for field, words in [('pc_utc', '이 PC'), ('server_utc', '회사 서버')]:
            stamp = datetime.fromisoformat(CLOCK[field].replace('Z', '+00:00')).astimezone().strftime('%H:%M')
            assert words + ' ' + stamp in text
    else:
        assert ' · 이 PC ' not in text
    payload = {'clock_error': diagnostic, 'scan_terminal_blocked_count': 2,
               'scan_terminal_blocked_record_count': 7}
    app = SimpleNamespace(_logistics_relay_state='stopped', _logistics_clock_warning=text,
                          _relay_screen_status_payload=payload)
    processor = SimpleNamespace(app=app, module=label, _dialogs=[])
    monkeypatch.setattr(bridge, '_pointer_pending', lambda _app: None)
    alerts = bridge._alerts(processor, None, None)
    assert len(alerts) == 1
    assert alerts[0]['kind'] == 'warn'
    assert 'choices' not in alerts[0]
    assert text in alerts[0]['text']
    assert '7건' in alerts[0]['text'] and '파일 2개' in alerts[0]['text']

    class Label:
        def configure(self, **options):
            self.options = options

    app.relay_screen_status_label = Label()
    label.Label_Match._render_relay_screen_status(app, 'stopped')
    assert text in app.relay_screen_status_label.options['text']
    assert label.LABEL_MATCH_RELAY_SCREEN_TEXT['stopped'] not in app.relay_screen_status_label.options['text']
    assert app.relay_screen_status_label.options['style'] == 'Warning.TLabel'


@pytest.mark.parametrize('failed_count', [1, 2])
def test_scan_non_source_error_still_drains(tmp_path, monkeypatch, capsys, failed_count):
    calls = disable_scan_drain(monkeypatch, status='acked')
    sync_dir = tmp_path / 'sync'
    broken = write_label_csv(sync_dir, name='포장실작업이벤트로그_a.csv')
    middle = write_label_csv(sync_dir, name='포장실작업이벤트로그_b.csv')
    healthy = write_label_csv(sync_dir, name='포장실작업이벤트로그_c.csv')
    sources = [broken, middle, healthy]
    original = runner.enqueue_completed_source_file
    attempts = []

    def enqueue(config, **kwargs):
        source = kwargs['relative_path']
        attempts.append(source)
        if len(attempts) <= failed_count:
            return {'status': 'enqueue_error', 'error_code': 'direct_sync_source_file_error'}
        return original(config, **kwargs)

    monkeypatch.setattr(runner, 'enqueue_completed_source_file', enqueue)
    assert runner.main(runner_args(tmp_path, scan_dir=sync_dir)) == 2
    capsys.readouterr()
    assert len(attempts) == 3
    assert all(value.startswith(runner._delta_relative_prefix(source))
               for value, source in zip(attempts, sources))
    assert len(calls) == 3 - failed_count
    status = json.loads((tmp_path / 'runtime/status.json').read_text(encoding='utf-8'))
    assert status['scan_status'] == 'enqueue_error'
    assert status['scan_failed_source_file'] == str(broken)
    expected_errors = [{'source_file': str(source), 'error_code': 'direct_sync_source_file_error'}
                       for source in sources[:failed_count]]
    assert status['scan_enqueue_errors'] == expected_errors
    events = [json.loads(line) for line in (tmp_path / 'logs/relay.jsonl').read_text(encoding='utf-8').splitlines()]
    logged = [error for event in events if event.get('event') == 'scan_enqueue_error'
              for error in event.get('scan_enqueue_errors', [])]
    assert logged == expected_errors
    assert events[-1]['scan_enqueue_errors'] == expected_errors


@pytest.mark.parametrize('mode', ['observe', 'enforce'])
@pytest.mark.parametrize('status', [400, 401, 403, 404, 408, 429, 500, 523, 599])
def test_non_json_error_response_is_retryable(queued, tmp_path, monkeypatch, mode, status):
    database, _, credentials = queued
    monkeypatch.setattr(push, 'client_runtime_lease_mode', lambda _credentials: mode)

    class ProxyResponse:
        status_code = status
        headers = {}

        def json(self):
            raise ValueError('proxy response is not JSON')

    result = push.drain_one_relay_batch(
        db_path=database, credentials=credentials,
        session=SequenceFakeSession([ProxyResponse()]),
        status_dir=tmp_path / 'upload-status',
    )
    assert result.retryable is True
    assert queue_row(database)['status'] == 'retry_wait'
    assert result.receipt['_local_response_parse_failed'] is True
    assert runtime._result_summary(result)['response_parse_failed'] is True
    assert json.loads(Path(result.status_path).read_text(encoding='utf-8'))['response_parse_failed'] is True


@pytest.mark.parametrize('status', [400, 401, 403, 404, 523, 599])
def test_non_json_lease_error_is_retryable(queued, monkeypatch, status):
    database, _, credentials = queued

    class ProxyResponse:
        status_code = status
        headers = {}

        def json(self):
            raise ValueError('proxy response is not JSON')

    class Session:
        def post(self, *_args, **_kwargs):
            return ProxyResponse()

    monkeypatch.setattr(push, 'prepare_runtime_metadata', lambda **_kwargs:
        leases._post_lease_request(request_value={}, credentials=credentials,
                                   session=Session(), timeout=1)[1])
    result = push.drain_one_relay_batch(db_path=database, credentials=credentials)
    assert result.retryable is True
    assert queue_row(database)['status'] == 'retry_wait'
    assert result.receipt['_local_response_parse_failed'] is True


@pytest.mark.parametrize('status', [200, 302, 401, 403])
def test_json_nonobject_keeps_existing_protection(queued, status):
    database, _, credentials = queued
    result = push.drain_one_relay_batch(
        db_path=database, credentials=credentials,
        session=SequenceFakeSession([FakeResponse(status, [])]),
    )
    assert '_local_response_parse_failed' not in result.receipt
    assert result.retryable is False


@pytest.mark.parametrize('outcome', ['explicit', 'local', 'unknown', 'nonjson', 'missing_committed', 'expired', 'lease_lost'])
def test_retry_releases_only_confirmed_unconsumed_assignment(tmp_path, monkeypatch, outcome):
    from tests.test_producer_runtime_client import _LeaseSession, _credentials, _make_pending_relay
    database = tmp_path / 'authority.sqlite3'
    credentials = _credentials()
    for relay_id in ('relay-a', 'relay-b'):
        _make_pending_relay(database, tmp_path, relay_id)

    class Session(_LeaseSession):
        def post(self, url, **kwargs):
            if url.endswith(leases.ENDPOINT_PATH):
                return super().post(url, **kwargs)
            if outcome == 'unknown':
                raise TimeoutError('reply lost')
            if outcome == 'nonjson':
                class ProxyResponse:
                    status_code = 403
                    headers = {}
                    def json(self):
                        raise ValueError('proxy reply')
                return ProxyResponse()
            if outcome == 'missing_committed':
                return FakeResponse(403, {'error': {'code': 'proxy_error'}})
            if outcome == 'lease_lost':
                with sqlite3.connect(database) as conn:
                    conn.execute("UPDATE direct_sync_relay_batches SET lease_owner='another-worker' WHERE relay_id='relay-a'")
            return refusal('temporary_unavailable', 503)

    session = Session()
    if outcome == 'local':
        def fail_headers(*_args, **_kwargs):
            raise PermissionError('local busy file')
        monkeypatch.setattr(push, 'signed_headers', fail_headers)
    def drain():
        return push.drain_one_relay_batch(
            db_path=database, credentials=credentials, session=session,
            target_relay_id='relay-a',
        )
    if outcome == 'lease_lost':
        with pytest.raises(push.DirectSyncPushError):
            drain()
    else:
        assert drain().retryable is True
    with sqlite3.connect(database) as conn:
        conn.row_factory = sqlite3.Row
        authority = conn.execute('SELECT * FROM direct_sync_runtime_authority').fetchone()
        row = conn.execute("SELECT * FROM direct_sync_relay_batches WHERE relay_id='relay-a'").fetchone()
    metadata = json.loads(row['metadata_json'])
    released = outcome in {'explicit', 'local', 'expired'}
    assert row['status'] == ('leased' if outcome == 'lease_lost' else 'retry_wait')
    assert authority['status'] == 'ACTIVE'
    assert (authority['assigned_relay_id'] is None) is released
    assert sum(field in metadata for field in leases.METADATA_FIELDS) == (0 if released else 5)
    assert metadata['client_batch_id'] == 'relay-a'
    if outcome == 'lease_lost':
        assert row['lease_owner'] == 'another-worker'
        return
    if outcome == 'expired':
        with sqlite3.connect(database) as conn:
            conn.execute("UPDATE direct_sync_runtime_authority SET expires_at='2000-01-01T00:00:00Z'")
    lease_calls_before = len(session.calls)
    other = push.claim_next_relay_batch(db_path=database, worker_id='next-worker', target_relay_id='relay-b')
    preparation = leases.prepare_runtime_metadata(
        db_path=database, relay_id=other.relay_id, metadata=other.metadata,
        credentials=credentials, expected_lease_owner=other.lease_owner,
        expected_attempt_count=other.attempt_count, session=session,
    )
    if released:
        assert preparation.metadata is not None
        assert len(session.calls) == lease_calls_before + (1 if outcome == 'expired' else 0)
    else:
        assert preparation.retryable is True
        assert preparation.error_code == 'runtime_request_in_flight'


@pytest.mark.parametrize('mode', ['observe', 'enforce'])
@pytest.mark.parametrize('status', [400, 403, 404, 422])
@pytest.mark.parametrize('code', ['proxy_error', 'STALE_RUNTIME_FENCE', 'STALE_RUNTIME_REQUEST_TOKEN'])
def test_json_error_without_committed_is_retryable(tmp_path, mode, status, code):
    from tests.test_producer_runtime_client import (
        _LeaseSession, _authority_row, _credentials, _make_pending_relay,
    )
    database = tmp_path / 'authority.sqlite3'
    credentials = _credentials(runtime_lease_mode=mode)
    for relay_id in ('relay-a', 'relay-b'):
        _make_pending_relay(database, tmp_path, relay_id)

    class Session(_LeaseSession):
        source_calls = 0

        def post(self, url, **kwargs):
            if url.endswith(leases.ENDPOINT_PATH):
                return super().post(url, **kwargs)
            self.source_calls += 1
            return FakeResponse(status, {'retryable': False, 'error': {'code': code}})

    session = Session()
    result = push.drain_one_relay_batch(
        db_path=database, credentials=credentials,
        session=session, target_relay_id='relay-a',
    )
    assert result.retryable is True
    assert queue_row(database)['status'] == 'retry_wait'
    authority = _authority_row(database)
    assert authority['status'] == 'ACTIVE'
    assert authority['assigned_relay_id'] == 'relay-a'
    metadata = json.loads(queue_row(database)['metadata_json'])
    assert all(field in metadata for field in leases.METADATA_FIELDS)

    following = push.drain_one_relay_batch(
        db_path=database, credentials=credentials,
        session=session, target_relay_id='relay-b',
    )
    assert following.retryable is True
    assert following.error_code == 'runtime_request_in_flight'
    assert _authority_row(database)['status'] == 'ACTIVE'
    assert dict(_authority_row(database)) == dict(authority)
    assert json.loads(queue_row(database)['metadata_json']) == metadata
    assert session.source_calls == 1


@pytest.mark.parametrize('mode', ['observe', 'enforce'])
@pytest.mark.parametrize('failure_point', ['spool_read', 'client_setup', 'source_open'])
def test_local_failure_after_lost_ack_preserves_assignment(tmp_path, monkeypatch, mode, failure_point):
    from tests.test_producer_runtime_client import (
        _DataPathServer, _authority_row, _credentials, _data_path_cycle, _make_pending_relay,
    )
    database = tmp_path / 'authority.sqlite3'
    server = _DataPathServer()
    credentials = _credentials(runtime_lease_mode=mode)
    push.init_relay_queue_schema(database)
    _data_path_cycle(database, server, 0, tmp_path, mode=mode)
    _make_pending_relay(database, tmp_path, 'relay-a')
    server.lose_source_ack = True
    _, first = _data_path_cycle(database, server, 10, tmp_path, mode=mode)
    server.lose_source_ack = False
    assert first.retryable is True
    assert 'relay-a' in server.stored
    authority_before = dict(_authority_row(database))
    metadata_before = json.loads(queue_row(database)['metadata_json'])
    spool = Path(queue_row(database)['spooled_file_path'])
    spool_before = spool.read_bytes()

    def locked(*_args, **_kwargs):
        raise PermissionError('synthetic local sharing failure after lost ACK')

    original_open = Path.open
    reads = 0

    def open_spool(path, mode='r', *args, **kwargs):
        nonlocal reads
        if path == spool and mode == 'rb':
            reads += 1
            if reads == 2:  # Digest succeeds; opening the upload body fails.
                locked()
        return original_open(path, mode, *args, **kwargs)

    with monkeypatch.context() as patch:
        if failure_point == 'spool_read':
            patch.setattr(push, '_read_file_digest', locked)
        elif failure_point == 'client_setup':
            patch.setattr(push, 'signed_headers', locked)
        else:
            patch.setattr(Path, 'open', open_spool)
        _, failed = _data_path_cycle(database, server, 20, tmp_path, mode=mode)
    assert failed.retryable is True
    assert queue_row(database)['status'] == 'retry_wait'
    assert dict(_authority_row(database)) == authority_before
    assert json.loads(queue_row(database)['metadata_json']) == metadata_before
    assert all(field in metadata_before for field in leases.METADATA_FIELDS)
    assert spool.read_bytes() == spool_before

    _make_pending_relay(database, tmp_path, 'relay-b')
    following = push.drain_one_relay_batch(
        db_path=database, credentials=credentials,
        session=server, target_relay_id='relay-b',
    )
    assert following.retryable is True
    assert following.error_code == 'runtime_request_in_flight'
    assert dict(_authority_row(database)) == authority_before
    assert len(server.source_calls) == 1

    _, replayed = _data_path_cycle(database, server, 40, tmp_path, mode=mode)
    assert replayed.success is True
    assert server.source_calls[0] == server.source_calls[1]
    _, next_result = _data_path_cycle(database, server, 50, tmp_path, mode=mode)
    assert next_result.success is True
    assert _authority_row(database)['status'] == 'ACTIVE'
    assert _authority_row(database)['assigned_relay_id'] is None
    with sqlite3.connect(database) as conn:
        statuses = dict(conn.execute('SELECT relay_id,status FROM direct_sync_relay_batches'))
    assert statuses == {'relay-a': 'acked', 'relay-b': 'acked'}
    assert server.audit == []


@pytest.mark.parametrize('phase', ['upload', 'lease'])
@pytest.mark.parametrize('mode', ['observe', 'enforce'])
@pytest.mark.parametrize('status', [408, 429, 500, 501, 505, 599])
def test_uncommitted_transient_http_is_not_terminal(queued, monkeypatch, phase, mode, status):
    database, _, credentials = queued
    monkeypatch.setattr(push, 'client_runtime_lease_mode', lambda _credentials: mode)

    class Session:
        def post(self, *_args, **_kwargs):
            return refusal('temporary_unavailable', status)

    if phase == 'lease':
        def prepare(**_kwargs):
            return leases._post_lease_request(
                request_value={}, credentials=credentials, session=Session(), timeout=1,
            )[1]
        monkeypatch.setattr(push, 'prepare_runtime_metadata', prepare)
    result = push.drain_one_relay_batch(
        db_path=database, credentials=credentials,
        session=SequenceFakeSession([refusal('temporary_unavailable', status)]),
    )
    assert result.retryable is True
    assert queue_row(database)['status'] == 'retry_wait'
    assert queue_row(database)['next_attempt_at']


@pytest.mark.parametrize('phase', ['upload', 'lease'])
@pytest.mark.parametrize('error_type', [
    ConnectionRefusedError, socket.gaierror, TimeoutError,
    requests.exceptions.ConnectionError, requests.exceptions.SSLError,
    requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout,
])
def test_network_failure_is_not_terminal(queued, monkeypatch, phase, error_type):
    database, _, credentials = queued

    class Session:
        def post(self, *_args, **_kwargs):
            raise error_type('synthetic transport failure')

    if phase == 'lease':
        def prepare(**_kwargs):
            return leases._post_lease_request(
                request_value={}, credentials=credentials, session=Session(), timeout=1,
            )[1]
        monkeypatch.setattr(push, 'prepare_runtime_metadata', prepare)
    result = push.drain_one_relay_batch(db_path=database, credentials=credentials, session=Session())
    assert result.retryable is True
    assert queue_row(database)['status'] == 'retry_wait'


def test_blocked_count_uses_csv_records_including_terminal_interval(tmp_path):
    content = 'event,value\nfirst,1\nsecond,"two\nlines"\nthird,3\n'.encode('utf-8-sig')
    source = tmp_path / 'events.csv'
    source.write_bytes(content)
    assert runner._count_blocked_source_records(source, 0) == 3
    start = content.index(b'second,')
    assert runner._count_blocked_source_records(source, start) == 2
    source.write_bytes(b'event,value\nsecond,"unfinished')
    assert runner._count_blocked_source_records(source, 0) is None
    assert runner._count_blocked_source_records(tmp_path / 'absent.csv', 0) is None


def test_last_scan_counts_survive_enqueue_and_clear_only_on_next_scan(tmp_path):
    config = make_config(tmp_path)
    runner._persist_scan_runtime_status(config, {
        'status': 'existing_terminal_blocked',
        'scan_terminal_blocked_count': 2, 'scan_terminal_blocked_record_count': 7,
    })
    saved = runtime._write_runtime_status(config, status='enqueued', queue={}, disk={})
    assert saved['scan_terminal_blocked_count'] == 2
    assert saved['scan_terminal_blocked_record_count'] == 7
    runner._persist_scan_runtime_status(config, {
        'status': 'existing_terminal_blocked', 'scan_terminal_blocked_count': 1,
    })
    unknown = json.loads(Path(config.runtime_status_path).read_text(encoding='utf-8'))
    assert 'scan_terminal_blocked_record_count' not in unknown
    runner._persist_scan_runtime_status(config, {'status': 'scan_no_files', 'scan_terminal_blocked_count': 0})
    cleared = runtime._write_runtime_status(config, status='idle', queue={}, disk={})
    assert cleared['scan_terminal_blocked_count'] == 0


def test_send_band_priority_and_unknown_record_count():
    app = SimpleNamespace(_logistics_pending_transmissions=20, _logistics_oldest_pending_age=601)
    blocked = {'scan_terminal_blocked_count': 2, 'scan_terminal_blocked_record_count': 7}
    text = label._label_match_send_warning(app, blocked)
    assert '기록 7건' in text
    assert '막힌 기록 파일 2개' in text
    assert '20' not in text  # queue batch counts are not record counts
    unknown = label._label_match_send_warning(app, {'scan_terminal_blocked_count': 2})
    assert '기록을' in unknown and '7건' not in unknown
    generic = label._label_match_send_warning(app, {'scan_terminal_blocked_count': 0})
    assert generic == (
        '회사 서버로 기록을 보내지 못하고 있습니다 — 현장 관리자에게 알리세요\n'
        '찍은 기록은 이 PC에 그대로 있고, 연결이 돌아오면 저절로 보내집니다'
    )
    app._logistics_oldest_pending_age = 600
    assert label._label_match_send_warning(app, {}) == ''


def test_scan_publishes_record_count_from_terminal_interval(tmp_path, monkeypatch, capsys):
    disable_scan_drain(monkeypatch)
    source = write_label_csv(tmp_path / 'sync')
    args = runner_args(tmp_path, scan_dir=source.parent)
    assert runner.main(args) == 1
    capsys.readouterr()
    with sqlite3.connect(tmp_path / 'relay.sqlite3') as conn:
        conn.execute("UPDATE direct_sync_relay_batches SET status='failed_permanent', last_error_code='invalid_signature'")
    rows = list(csv.reader(io.StringIO(source.read_text(encoding='utf-8-sig'))))
    with source.open('a', encoding='utf-8', newline='') as stream:
        csv.writer(stream).writerow(rows[-1])
    assert runner.main(args) != 0
    capsys.readouterr()
    payload = json.loads((tmp_path / 'runtime' / 'status.json').read_text(encoding='utf-8'))
    assert payload['scan_terminal_blocked_count'] == 1
    assert payload['scan_terminal_blocked_record_count'] == len(rows)


def test_four_existing_warnings_and_one_send_band_fit_the_contract(monkeypatch):
    payload = {'clock_error': {'status_code': 401, 'error_code': 'stale_timestamp'}}
    app = SimpleNamespace(_relay_screen_status_payload=payload)
    existing = [{'kind': 'warn', 'code': None, 'text': '기존 안내 ' + str(i)} for i in range(4)]
    processor = SimpleNamespace(app=app, module=label, _dialogs=existing)
    monkeypatch.setattr(bridge, '_pointer_pending', lambda _app: None)
    alerts = bridge._alerts(processor, None, None)
    assert len(alerts) == 5
    assert all(item in alerts for item in existing)
    contract = Path(__file__).resolve().parents[1] / 'ui' / 'parts' / 'app' / 'lm' / 'contract.json'
    assert contract.stat().st_size < 1024 * 1024
    schema = json.loads(contract.read_text(encoding='utf-8'))
    assert schema['state']['properties']['alerts']['maxItems'] == 5


def test_record_count_is_unknown_when_source_changes_during_read(tmp_path, monkeypatch):
    source = tmp_path / 'events.csv'
    source.write_text('event,value\nfirst,1\nsecond,2\n', encoding='utf-8')
    original_reader = runner.csv.reader

    def changing_reader(*args, **kwargs):
        for index, row in enumerate(original_reader(*args, **kwargs)):
            if index == 1:
                with source.open('a', encoding='utf-8') as stream:
                    stream.write('third,3\n')
            yield row

    monkeypatch.setattr(runner.csv, 'reader', changing_reader)
    assert runner._count_blocked_source_records(source, 0) is None


def public_status(tmp_path):
    return tmp_path / 'local' / 'KMTech' / 'Label_Match' / 'upload_block_status.json'


def test_public_block_status_schema_since_restart_and_clear(tmp_path, monkeypatch):
    monkeypatch.setenv('LOCALAPPDATA', str(tmp_path / 'local'))
    clock = ['2026-10-26T06:00:00Z']
    monkeypatch.setattr(runtime, 'utc_now_text', lambda: clock[0])
    config = make_config(tmp_path)
    path = public_status(tmp_path)
    runtime._publish_upload_block_status(config, {'scan_terminal_blocked_count': 0})
    clear = json.loads(path.read_text(encoding='utf-8'))
    assert set(clear) == {'schema', 'app', 'blocked_files', 'blocked_records', 'install_review', 'since', 'updated'}
    assert clear == {'schema': 1, 'app': 'LM', 'blocked_files': 0, 'blocked_records': 0,
                     'install_review': False, 'since': None, 'updated': clock[0]}
    assert not path.read_bytes().startswith(b'\xef\xbb\xbf')
    clock[0] = '2026-10-26T06:01:00Z'
    blocked = {'scan_terminal_blocked_count': 1, 'scan_terminal_blocked_record_count': 3,
               'scan_terminal_blocked_source_files': ['private']}
    runtime._publish_upload_block_status(config, blocked)
    first = json.loads(path.read_text(encoding='utf-8'))
    assert first['blocked_files'] == 1 and first['blocked_records'] == 3
    assert first['since'] == blocked['upload_block_since'] == clock[0]
    assert 'private' not in path.read_text(encoding='utf-8')
    clock[0] = '2026-10-26T06:02:00Z'
    # A fresh config/runtime record after restart preserves the public file's interval.
    runtime._publish_upload_block_status(make_config(tmp_path), {})
    retained = json.loads(path.read_text(encoding='utf-8'))
    assert retained['since'] == first['since']
    assert retained['updated'] == clock[0]
    runtime._publish_upload_block_status(config, {'scan_terminal_blocked_count': 1})
    assert json.loads(path.read_text(encoding='utf-8'))['blocked_records'] is None
    runtime._publish_upload_block_status(config, {'scan_terminal_blocked_count': 0})
    resolved = json.loads(path.read_text(encoding='utf-8'))
    assert resolved['blocked_files'] == resolved['blocked_records'] == 0
    assert resolved['since'] is None and resolved['install_review'] is False
    assert path.exists()


def test_public_install_review_requires_preflight_evidence(tmp_path, monkeypatch):
    monkeypatch.setenv('LOCALAPPDATA', str(tmp_path / 'local'))
    config = make_config(tmp_path)
    path = public_status(tmp_path)
    runtime._publish_upload_block_status(config, {
        'last_result': {'status': 'operator_review'},
        'runtime_lease': {'status_code': 401, 'error_code': 'stale_timestamp'},
    })
    assert json.loads(path.read_text(encoding='utf-8'))['install_review'] is False
    runtime._publish_upload_block_status(config, {'runtime_lease': {'status': 'operator_review'}})
    reviewed = json.loads(path.read_text(encoding='utf-8'))
    assert reviewed['install_review'] is True and reviewed['since'].endswith('Z')
    runtime._publish_upload_block_status(config, {'runtime_lease': {'status': 'ACTIVE'}})
    assert json.loads(path.read_text(encoding='utf-8'))['install_review'] is True
    runtime._publish_upload_block_status(config, {
        'runtime_lease': {'status': 'ACTIVE', 'server_grant_accepted': True},
    })
    resolved = json.loads(path.read_text(encoding='utf-8'))
    assert resolved['install_review'] is False and resolved['since'] is None


def test_public_write_failure_keeps_runtime_status_and_since(tmp_path, monkeypatch, caplog):
    monkeypatch.setenv('LOCALAPPDATA', str(tmp_path / 'local'))
    config = make_config(tmp_path)
    atomic = runtime._write_json_atomic

    def fail_public_only(path, value):
        if Path(path).name == 'upload_block_status.json':
            raise PermissionError('synthetic diagnostic write refusal')
        return atomic(path, value)

    monkeypatch.setattr(runtime, '_write_json_atomic', fail_public_only)
    atomic(config.runtime_status_path, {
        'scan_terminal_blocked_count': 1, 'scan_terminal_blocked_record_count': 2,
    })
    status = runtime._write_runtime_status(
        config, status='existing_terminal_blocked', disk={}, queue={},
    )
    assert status['status'] == 'existing_terminal_blocked'
    assert Path(config.runtime_status_path).is_file()
    assert status['upload_block_since'].endswith('Z')
    assert caplog.text.count('upload_block_status_write_failed') == 1
    later = runtime._write_runtime_status(config, status='idle', disk={}, queue={})
    assert later['upload_block_since'] == status['upload_block_since']
    assert caplog.text.count('upload_block_status_write_failed') == 2


@pytest.mark.parametrize('failure_point', ['client_setup', 'spool_read'])
def test_local_oserror_before_post_retries_the_same_batch(queued, tmp_path, monkeypatch, failure_point):
    database, row, credentials = queued
    spool_before = Path(row.spooled_file_path).read_bytes()
    session = SequenceFakeSession([accepted(row)])

    def refused(*_args, **_kwargs):
        error = PermissionError('synthetic local sharing failure')
        error.winerror = 32
        raise error

    with monkeypatch.context() as patch:
        if failure_point == 'client_setup':
            patch.setattr(requests, 'Session', refused)
        else:
            patch.setattr(push, '_read_file_digest', refused)
        first = push.drain_one_relay_batch(
            db_path=database, credentials=credentials,
            session=None if failure_point == 'client_setup' else session,
            status_dir=tmp_path / 'status',
        )
    assert first.retryable is True
    assert queue_row(database)['status'] == 'retry_wait'
    assert queue_row(database)['next_attempt_at']
    assert len(session.calls) == 0
    due(database)
    second = push.drain_one_relay_batch(
        db_path=database, credentials=credentials, session=session,
    )
    assert second.success is True
    assert queue_row(database)['relay_id'] == row.relay_id
    assert queue_row(database)['attempt_count'] == 2
    assert Path(row.spooled_file_path).read_bytes() == spool_before


def test_oserror_after_response_retains_operator_review(queued, tmp_path, monkeypatch):
    database, row, credentials = queued
    session = SequenceFakeSession([accepted(row)])

    def failed_artifact(*_args, **_kwargs):
        raise PermissionError('synthetic failure after the response')

    monkeypatch.setattr(push, '_with_upload_status_artifact', failed_artifact)
    result = push.drain_one_relay_batch(
        db_path=database, credentials=credentials, session=session,
        status_dir=tmp_path / 'status',
    )
    assert len(session.calls) == 1
    assert result.retryable is False
    assert queue_row(database)['status'] == 'operator_review'


def test_enforce_other_401_keeps_legacy_review(queued, monkeypatch):
    database, _, credentials = queued
    monkeypatch.setattr(push, 'client_runtime_lease_mode', lambda _credentials: 'enforce')
    result = push.drain_one_relay_batch(
        db_path=database, credentials=credentials,
        session=SequenceFakeSession([refusal('invalid_signature')]),
    )
    assert result.retryable is False
    assert queue_row(database)['status'] == 'operator_review'


@pytest.mark.parametrize('targeted', [False, True])
@pytest.mark.parametrize('known_date', [False, True])
def test_clock_rollback_reclaims_stale_wait_on_next_cycle(queued, monkeypatch, targeted, known_date):
    database, row, credentials = queued
    due(database)
    instant = [PC_TIME]

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return instant[0].astimezone(tz)

    monkeypatch.setattr(push, 'datetime', Clock)
    monkeypatch.setattr(leases, 'datetime', Clock)
    session = SequenceFakeSession([refusal(date=SERVER_DATE if known_date else None), accepted(row)])
    arguments = {'db_path': database, 'credentials': credentials, 'session': session,
                 'target_relay_id': row.relay_id if targeted else ''}
    assert push.drain_one_relay_batch(**arguments).retryable is True
    assert push.drain_one_relay_batch(**arguments) is None  # ordinary backoff stays in place
    instant[0] = datetime(2026, 10, 26, 5, 0, tzinfo=timezone.utc)
    recovered = push.drain_one_relay_batch(**arguments)
    assert recovered is not None and recovered.success is True
    assert queue_row(database)['status'] == 'acked'


def test_clock_rollback_does_not_shorten_other_retry_wait(queued, monkeypatch):
    database, _, credentials = queued
    due(database)
    instant = [PC_TIME]

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return instant[0].astimezone(tz)

    monkeypatch.setattr(push, 'datetime', Clock)
    session = SequenceFakeSession([refusal('temporary_unavailable', 503)])
    assert push.drain_one_relay_batch(db_path=database, credentials=credentials, session=session).retryable
    instant[0] = datetime(2026, 10, 26, 5, 0, tzinfo=timezone.utc)
    assert push.drain_one_relay_batch(db_path=database, credentials=credentials, session=session) is None


def test_retry_dead_resets_age_and_attempts(queued):
    from direct_sync_operator import retry_dead_relay_batch

    database, row, _ = queued
    with sqlite3.connect(database) as conn:
        conn.execute("UPDATE direct_sync_relay_batches SET status='failed_permanent', created_at='2000-01-01T00:00:00Z', attempt_count=8")
    retry_dead_relay_batch(
        db_path=database, relay_id=row.relay_id, operator_id='isolated-operator', reason='isolated retry qualification',
    )
    retried = queue_row(database)
    assert retried['status'] == 'pending'
    assert retried['attempt_count'] == 0
    assert retried['created_at'] == retried['updated_at']
    assert retried['created_at'] != '2000-01-01T00:00:00Z'
    assert retried['content_sha256'] == row.content_sha256
    assert json.loads(retried['metadata_json'])['idempotency_key'] == row.metadata['idempotency_key']

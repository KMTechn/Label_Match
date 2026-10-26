"""An uncommitted clock rejection must remain deliverable after clock repair."""

from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3

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
        'totals': {'inserted': 1, 'replayed': 0, 'quarantined': 0, 'errors': 0},
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
def test_clock_band_is_yellow_has_no_action_and_keeps_other_terminal_warning(monkeypatch, known_date):
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
    app = SimpleNamespace(_logistics_relay_state='stopped', _logistics_clock_warning=text)
    processor = SimpleNamespace(app=app, module=label, _dialogs=[])
    monkeypatch.setattr(bridge, '_pointer_pending', lambda _app: None)
    alerts = bridge._alerts(processor, None, None)
    assert alerts == [
        {'kind': 'warn', 'code': None, 'text': text},
        {'kind': 'warn', 'code': None, 'text': label.LABEL_MATCH_RELAY_SCREEN_TEXT['stopped']},
    ]

    class Label:
        def configure(self, **options):
            self.options = options

    app.relay_screen_status_label = Label()
    label.Label_Match._render_relay_screen_status(app, 'stopped')
    assert text in app.relay_screen_status_label.options['text']
    assert label.LABEL_MATCH_RELAY_SCREEN_TEXT['stopped'] in app.relay_screen_status_label.options['text']
    assert app.relay_screen_status_label.options['style'] == 'Warning.TLabel'

import json
import sqlite3
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import user_relay
from producer_runtime_client import checkpoint_sqlite_database, close_sqlite_connection
from terminal_operation_lease import OperationLeaseStore
from tests.test_f6_pointer_confirmation import app


@pytest.fixture(autouse=True)
def isolated_stop_process_observation(monkeypatch):
    # Existing stop/ACK tests have no native app processes. The process-tree
    # behavior is exercised separately with synthetic identity snapshots.
    monkeypatch.setattr(user_relay, '_app_stop_process_snapshot', lambda **k: [])


def test_idle_request_reuses_relay_stop_and_normal_close(app, tmp_path):
    app.current_set_info["raw"] = []
    root = tmp_path / 'direct-sync'
    app.direct_sync_bootstrap_context = {'program_data_root': str(root), 'runtime_status_path': ''}
    app.after = Mock()
    app._request_pointer_confirmation = Mock(side_effect=AssertionError('remote stop must preserve without a prompt'))
    saved = []
    app._save_current_set_state = lambda: saved.append(list(app.current_set_info['raw'])) or True

    report = user_relay.request_app_stop(root)
    request_path = user_relay.app_stop_request_path(root)
    payload = json.loads(request_path.read_text(encoding='utf-8'))
    assert payload['schema_version'] == user_relay.APP_STOP_SCHEMA
    assert request_path.stat().st_size < 1024
    assert report['relay']['status'] == 'ABSENT'
    assert user_relay.user_relay_stop_path(root).is_file()

    app._refresh_relay_screen_status()

    assert saved == [[]]
    app.data_manager.close.assert_called_once()
    app._begin_app_close_direct_sync.assert_called_once()
    app._request_pointer_confirmation.assert_not_called()
    app.after.assert_not_called()
    assert not request_path.exists()
    assert user_relay.user_relay_stop_path(root).is_file()


def test_cli_stop_runs_before_startup_gate_or_credentials(monkeypatch, tmp_path, capsys):
    import auto_update
    import current_user_onboarding
    import direct_sync_runtime

    forbidden = Mock(side_effect=AssertionError('stop CLI must not start or enroll the relay'))
    monkeypatch.setattr(auto_update, 'startup_gate', forbidden)
    monkeypatch.setattr(current_user_onboarding, 'apply_current_user_runtime_environment', forbidden)
    monkeypatch.setattr(direct_sync_runtime, 'prepare_credentials_from_json', forbidden)
    root = tmp_path / 'direct-sync'

    assert user_relay.main(['--request-app-stop', '--direct-sync-root', str(root)]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report['status'] == 'STOPPED'
    assert report['remaining_process_count'] == 0
    assert report['relay']['status'] == 'ABSENT'
    assert user_relay.app_stop_request_path(root).is_file()
    forbidden.assert_not_called()


def test_cli_default_root_uses_existing_owner_paths(monkeypatch, tmp_path, capsys):
    import current_user_onboarding

    root = tmp_path / 'owner-direct-sync'
    resolve = Mock(return_value=SimpleNamespace(direct_sync_root=root, data_root=tmp_path / 'data'))
    monkeypatch.setattr(current_user_onboarding, 'resolve_current_user_onboarding_paths', resolve)

    assert user_relay.main(['--request-app-stop', '--app-root', str(tmp_path / 'app')]) == 0
    assert json.loads(capsys.readouterr().out)['status'] == 'STOPPED'
    assert resolve.call_count == 1
    assert all(call.args == ((tmp_path / 'app').resolve(),) for call in resolve.call_args_list)
    assert user_relay.app_stop_request_path(root).is_file()


def test_cli_keeps_unknown_relay_absence_as_failure(monkeypatch, tmp_path, capsys):
    root = tmp_path / 'direct-sync'
    monkeypatch.setattr(user_relay, 'request_user_relay_stop', lambda _root, **k: {'status': 'UNKNOWN'})
    monkeypatch.setattr(user_relay, '_acquire_relay_lease', lambda _: None)
    monkeypatch.setattr(user_relay, 'APP_STOP_TIMEOUT_SECONDS', 0)

    assert user_relay.main(['--request-app-stop', '--direct-sync-root', str(root)]) == 1
    assert json.loads(capsys.readouterr().out)['relay']['status'] == 'UNKNOWN'
    assert user_relay.app_stop_request_path(root).is_file()


@pytest.mark.parametrize('payload', [{}, {'schema_version': 'unsupported'}, None])
def test_invalid_app_stop_does_not_close(app, tmp_path, payload):
    root = tmp_path / 'direct-sync'
    request_path = user_relay.app_stop_request_path(root)
    request_path.parent.mkdir(parents=True)
    request_path.write_text(json.dumps(payload), encoding='utf-8')
    app.direct_sync_bootstrap_context = {'program_data_root': str(root)}
    app.on_closing = Mock()

    assert app._process_app_stop_request() is False
    app.on_closing.assert_not_called()
    assert request_path.exists()


def test_rejected_close_keeps_request_for_retry(app, tmp_path):
    app.current_set_info["raw"] = []
    root = tmp_path / 'direct-sync'
    user_relay.request_app_stop(root)
    app.direct_sync_bootstrap_context = {'program_data_root': str(root)}
    app.on_closing = Mock()

    assert app._process_app_stop_request() is False
    app.on_closing.assert_called_once_with(_confirmed=True)
    assert user_relay.app_stop_request_path(root).is_file()


def test_close_does_not_delete_a_successor_app_request(app, tmp_path):
    app.current_set_info["raw"] = []
    root = tmp_path / 'direct-sync'
    user_relay.request_app_stop(root)
    app.direct_sync_bootstrap_context = {'program_data_root': str(root)}
    def close(**kwargs):
        user_relay.request_app_stop(root)
        app._app_close_in_progress = True
    app.on_closing = close

    assert app._process_app_stop_request() is True
    assert user_relay.app_stop_request_path(root).is_file()


@pytest.mark.parametrize('after_start', [False, True], ids=['before-start', 'after-start'])
def test_app_stop_only_closes_the_process_started_before_request(app, tmp_path, monkeypatch, after_start):
    app.current_set_info["raw"] = []
    import Label_Match as lm

    started_at = datetime(2026, 10, 11, 12, 0, 0, 500000, tzinfo=timezone.utc)
    requested_at = started_at + timedelta(microseconds=100 if after_start else -100)
    class RequestClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return requested_at.astimezone(tz)
    monkeypatch.setattr(user_relay, 'datetime', RequestClock)
    monkeypatch.setattr(lm, '_APP_PROCESS_STARTED_AT', started_at, raising=False)
    root = tmp_path / 'direct-sync'
    user_relay.request_app_stop(root)
    request_path = user_relay.app_stop_request_path(root)
    payload = json.loads(request_path.read_text(encoding='utf-8'))
    app.direct_sync_bootstrap_context = {'program_data_root': str(root)}
    app.on_closing = Mock(side_effect=lambda **_kwargs: setattr(app, '_app_close_in_progress', True))

    assert app._process_app_stop_request() is after_start
    assert app.on_closing.call_count == int(after_start)
    assert not request_path.exists()
    assert datetime.fromisoformat(payload['requested_at']) == requested_at
    assert datetime.fromisoformat(payload['requested_at']).utcoffset() == timedelta(0)


@pytest.mark.parametrize('requested_at', [None, 123, 'not-a-date', '2026-10-11T12:00:00.500100'])
def test_app_stop_without_aware_timestamp_is_ignored(app, tmp_path, requested_at):
    root = tmp_path / 'direct-sync'
    user_relay.request_app_stop(root)
    request_path = user_relay.app_stop_request_path(root)
    payload = json.loads(request_path.read_text(encoding='utf-8'))
    payload['requested_at'] = requested_at
    request_path.write_text(json.dumps(payload), encoding='utf-8')
    app.direct_sync_bootstrap_context = {'program_data_root': str(root)}
    app.on_closing = Mock(side_effect=lambda **_kwargs: setattr(app, '_app_close_in_progress', True))

    assert app._process_app_stop_request() is False
    app.on_closing.assert_not_called()
    assert request_path.exists()


def test_checkpoint_and_close_preserves_rows_and_removes_sidecars(tmp_path):
    database = tmp_path / 'wal.sqlite3'
    conn = sqlite3.connect(database)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('CREATE TABLE work (value TEXT)')
    conn.execute("INSERT INTO work VALUES ('unfinished')")
    conn.commit()
    assert database.with_suffix('.sqlite3-wal').is_file()

    close_sqlite_connection(conn)

    assert not database.with_suffix('.sqlite3-wal').exists()
    assert not database.with_suffix('.sqlite3-shm').exists()
    with pytest.raises(sqlite3.ProgrammingError):
        conn.execute('SELECT 1')
    restored = sqlite3.connect(database)
    try:
        assert restored.execute('SELECT value FROM work').fetchall() == [('unfinished',)]
    finally:
        restored.close()


def test_busy_checkpoint_is_logged_and_still_closes(tmp_path, caplog):
    database = tmp_path / 'busy.sqlite3'
    reader = sqlite3.connect(database)
    reader.execute('PRAGMA journal_mode=WAL')
    reader.execute('CREATE TABLE work (value INTEGER)')
    reader.commit()
    reader.execute('BEGIN')
    reader.execute('SELECT * FROM work').fetchall()
    writer = sqlite3.connect(database)
    writer.execute('INSERT INTO work VALUES (1)')
    writer.commit()
    try:
        close_sqlite_connection(writer)
        assert 'WAL checkpoint busy' in caplog.text
        with pytest.raises(sqlite3.ProgrammingError):
            writer.execute('SELECT 1')
    finally:
        reader.rollback()
        close_sqlite_connection(reader)
    assert not database.with_suffix('.sqlite3-wal').exists()
    assert not database.with_suffix('.sqlite3-shm').exists()


def test_checkpoint_error_does_not_prevent_close(caplog):
    conn = SimpleNamespace(execute=Mock(side_effect=sqlite3.OperationalError('injected')), close=Mock())
    close_sqlite_connection(conn)
    conn.close.assert_called_once()
    assert 'WAL checkpoint failed' in caplog.text


def test_operation_lease_store_releases_connections_even_when_referenced(monkeypatch, tmp_path):
    import terminal_operation_lease

    connect = sqlite3.connect
    connections = []
    def tracked(*args, **kwargs):
        conn = connect(*args, **kwargs)
        connections.append(conn)
        return conn
    monkeypatch.setattr(terminal_operation_lease.sqlite3, 'connect', tracked)
    database = tmp_path / 'leases.sqlite3'
    store = OperationLeaseStore(database)
    assert store.get(set_id='missing') is None
    assert connections
    for conn in connections:
        with pytest.raises(sqlite3.ProgrammingError):
            conn.execute('SELECT 1')
    assert not database.with_suffix('.sqlite3-wal').exists()
    assert not database.with_suffix('.sqlite3-shm').exists()


def test_gui_destroy_cleans_sidecars_recreated_by_last_readonly_connection(app, monkeypatch, tmp_path):
    import direct_sync_push
    import Label_Match as lm

    root = tmp_path / 'direct-sync'
    database = root / 'queue' / 'direct_sync_relay.sqlite3'
    direct_sync_push.init_relay_queue_schema(database)
    direct_sync_push.relay_queue_status(database)
    assert database.with_suffix('.sqlite3-wal').exists()
    app.direct_sync_bootstrap_context = {'program_data_root': str(root)}
    app._stop_error_siren = Mock()
    app._join_tk_shutdown_threads = Mock(return_value=[])
    app._close_data_manager_before_tk_destroy = Mock(return_value=True)
    monkeypatch.setattr(lm.tk.Tk, 'destroy', lambda _self: None)

    app.destroy()

    assert app._tk_destroy_complete is True
    assert not database.with_suffix('.sqlite3-wal').exists()
    assert not database.with_suffix('.sqlite3-shm').exists()


def test_final_checkpoint_never_creates_a_missing_database(tmp_path):
    database = tmp_path / 'missing.sqlite3'
    checkpoint_sqlite_database(database)
    assert not database.exists()


def test_final_checkpoint_path_error_is_logged_without_raising(monkeypatch, caplog):
    monkeypatch.setattr(Path, 'resolve', Mock(side_effect=OSError('injected')))
    checkpoint_sqlite_database('unavailable.sqlite3')
    assert 'WAL shutdown cleanup failed (OSError)' in caplog.text


def test_gui_checkpoint_error_does_not_block_destroy(app, monkeypatch, caplog):
    import Label_Match as lm

    app._stop_error_siren = Mock()
    app._join_tk_shutdown_threads = Mock(return_value=[])
    app._close_data_manager_before_tk_destroy = Mock(return_value=True)
    app._checkpoint_app_databases = Mock(side_effect=OSError('injected'))
    monkeypatch.setattr(lm.tk.Tk, 'destroy', lambda _self: None)
    app.destroy()
    assert app._tk_destroy_complete is True
    assert 'WAL shutdown cleanup failed (OSError)' in caplog.text


def test_relay_stop_checkpoints_after_final_readonly_status(monkeypatch, tmp_path):
    import direct_sync_push
    from tests.test_direct_sync_relay_install_pack import _seal_generation_release_input

    app_root = tmp_path / 'app'
    (app_root / 'runtime').mkdir(parents=True)
    (app_root / 'app').mkdir()
    (app_root / 'runtime' / 'python.exe').write_bytes(b'runtime')
    (app_root / 'app' / 'main.py').write_text('pass\n', encoding='utf-8')
    revision = subprocess.check_output(['git', '-c', 'core.autocrlf=false', 'rev-parse', 'HEAD'], text=True).strip()
    _seal_generation_release_input(app_root, revision)
    root = tmp_path / 'direct-sync'
    database = root / 'queue' / 'direct_sync_relay.sqlite3'
    monkeypatch.setenv('LOCALAPPDATA', str(tmp_path / 'local'))
    monkeypatch.setattr('logistics_runtime_profile.load_logistics_runtime_profile',
                        lambda **_kwargs: SimpleNamespace(tls_ca_bundle_path=''))
    released = []
    def release():
        assert not database.with_suffix('.sqlite3-wal').exists()
        assert not database.with_suffix('.sqlite3-shm').exists()
        released.append(True)
    monkeypatch.setattr(user_relay, '_acquire_relay_lease', lambda _key: SimpleNamespace(close=release))
    def cycle(command, _timeout_seconds):
        direct_sync_push.init_relay_queue_schema(database)
        direct_sync_push.relay_queue_status(database)
        assert database.with_suffix('.sqlite3-wal').exists()
        user_relay.request_app_stop(root)
        status_path = Path(command[command.index('--runtime-status-path') + 1])
        status_path.write_text('{"status":"idle"}\n', encoding='utf-8')
        return {'status': 'PASS', 'returncode': 0}
    monkeypatch.setattr(user_relay, '_run_command', cycle)

    assert user_relay.main(['--app-root', str(app_root), '--direct-sync-root', str(root), '--once'],
                           credential_input=object()) == 0
    assert released == [True]
    assert user_relay.user_relay_stop_path(root).is_file()

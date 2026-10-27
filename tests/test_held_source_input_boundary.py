"""Held source read failures retain their assignment and CLI diagnostics."""
import errno
import json
from pathlib import Path
import sqlite3

import pytest

from tests.test_direct_sync_relay_runner import isolated_data_generation_release
from tests.test_held_assignment_recovery import _run_cli, _seed
from tests.test_producer_runtime_client import _authority_row
import direct_sync_runtime as runtime
from tools import direct_sync_relay_runner as runner


def _queue_rows(database):
    with sqlite3.connect(database) as connection:
        connection.row_factory = sqlite3.Row
        return [dict(row) for row in connection.execute(
            'SELECT * FROM direct_sync_relay_batches ORDER BY relay_id')]


def _deny_source(monkeypatch, source, failure):
    if failure == 'missing':
        source.unlink()
        return
    original_open = Path.open

    def locked(path, *args, **kwargs):
        if path == source:
            raise PermissionError(errno.EACCES, 'source read refused')
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, 'open', locked)


def _capture_config(monkeypatch):
    configs = []
    original = runner._build_config

    def build(args):
        config = original(args)
        configs.append(config)
        return config

    monkeypatch.setattr(runner, '_build_config', build)
    return configs


@pytest.mark.parametrize('failure', ['locked', 'missing'])
def test_single_held_source_read_failure_records_status_without_drain(
        tmp_path, monkeypatch, capsys, failure):
    database, config, source, server, held, wire, body = _seed(tmp_path, monkeypatch)
    configs = _capture_config(monkeypatch)
    before = _queue_rows(database)
    calls_before = server.source_calls

    def no_drain(*args, **kwargs):
        pytest.fail('single source read failure must stop before drain')

    monkeypatch.setattr(runtime, 'run_relay_once', no_drain)
    with monkeypatch.context() as blocked:
        _deny_source(blocked, source, failure)
        result = _run_cli(tmp_path, blocked, source, server, wire, single=True)
    captured = capsys.readouterr()
    assert result == 1
    assert 'Traceback' not in captured.err
    status = json.loads(Path(configs[0].runtime_status_path).read_text(encoding='utf-8'))
    assert status['status'] == 'runtime_error'
    assert status['error_code'] == 'source_file_read_failed'
    events = [json.loads(line) for line in Path(configs[0].log_path).read_text(
        encoding='utf-8').splitlines()]
    assert events[-1]['event'] == 'enqueue_runtime_error'
    assert events[-1]['error_code'] == 'source_file_read_failed'
    assert _queue_rows(database) == before
    assert dict(_authority_row(database)) == held
    assert server.source_calls == calls_before
    assert source.read_bytes() == body if failure == 'locked' else not source.exists()


@pytest.mark.parametrize('failure', ['locked', 'missing'])
def test_scan_held_source_read_failure_keeps_other_source_and_drain_running(
        tmp_path, monkeypatch, capsys, failure):
    database, config, source, server, held, wire, body = _seed(tmp_path, monkeypatch)
    configs = _capture_config(monkeypatch)
    healthy = source.with_name(source.name.replace('_', '_healthy_', 1))
    assert healthy != source
    healthy.write_bytes(body)
    before = _queue_rows(database)
    drain_calls = []

    def drain(config, **kwargs):
        drain_calls.append(kwargs)
        return {'status': 'idle'}

    monkeypatch.setattr(runtime, 'run_relay_once', drain)
    with monkeypatch.context() as blocked:
        _deny_source(blocked, source, failure)
        result = _run_cli(tmp_path, blocked, source, server, wire)
    captured = capsys.readouterr()
    assert 'Traceback' not in captured.err
    status = json.loads(Path(configs[0].runtime_status_path).read_text(encoding='utf-8'))
    assert status['scan_enqueued_count'] == 1
    after = _queue_rows(database)
    assert len(after) == len(before) + 1
    assert any(row['status'] == 'pending' and row['relay_id'] != held['assigned_relay_id']
               for row in after)
    assert drain_calls
    assert _authority_row(database)['assigned_relay_id'] == held['assigned_relay_id']
    assert healthy.read_bytes() == body
    assert source.read_bytes() == body if failure == 'locked' else not source.exists()
    assert result == 1  # The drain double does not ACK the targeted healthy row.


@pytest.mark.parametrize('failure', ['locked', 'missing'])
def test_scan_repair_skips_unreadable_held_row_without_changing_assignment(
        tmp_path, monkeypatch, failure):
    database, config, source, server, held, wire, body = _seed(tmp_path, monkeypatch)
    before = _queue_rows(database)
    with monkeypatch.context() as blocked:
        _deny_source(blocked, source, failure)
        assert runner._repair_held_assignments_before_scan(config, source) == []
    assert _queue_rows(database) == before
    assert dict(_authority_row(database)) == held
    assert source.read_bytes() == body if failure == 'locked' else not source.exists()

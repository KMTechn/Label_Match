"""Power-loss suffix recovery through the real queued CSV writer."""
import csv
from datetime import datetime
import hashlib
import io
import json
from pathlib import Path

import pytest

from label_data_manager import DataManager


HEADER = ['timestamp', 'worker_name', 'event', 'details']
OLD = ['2026-10-27T01:00:00', 'worker', 'ERROR_INPUT', '{"old": true}']


def encoded(*rows):
    stream = io.StringIO(newline='')
    csv.writer(stream).writerows(rows)
    return stream.getvalue().encode('utf-8')


def manager(root):
    return DataManager(str(root), 'pack', 'worker', 'TAIL-PC', open_file=open,
        clock=lambda: datetime, enrich_event=lambda event, details, unique_id: details,
        datetime_encoder=lambda: json.JSONEncoder,
        current_state_filename=lambda: '_current_set_state_packaging.json',
        durable_event_types=lambda: {'TRAY_COMPLETE'}, csv_safe_cell=lambda value: value)


@pytest.fixture
def data(tmp_path):
    value = manager(tmp_path)
    yield value
    try:
        value.close(timeout=5)
    except RuntimeError:
        pass  # The rejection tests deliberately retain a writer error.
    assert not value.log_thread.is_alive()


def evidence(path, suffix):
    return path.with_name(path.name + '.held-' + hashlib.sha256(suffix).hexdigest())


@pytest.mark.parametrize('suffix', [b'partial,row', b'partial,\xed\x95',
    b'\0' * 8193, b'partial\0\r\n', b'partial\0\r\nmore\0\r\n'],
    ids=['partial-row', 'split-utf8', 'nul-blocks', 'nul-line', 'nul-lines'])
@pytest.mark.parametrize('audit', [False, True])
def test_append_preserves_complete_prefix_and_quarantines_only_torn_suffix(data, suffix, audit):
    path = Path(data._get_log_filepath())
    prefix = b'\xef\xbb\xbf' + encoded(HEADER, OLD)
    path.write_bytes(prefix + suffix)
    details = {'scanner_audit': {'key': 'new-key'}} if audit else {'new': True}
    data.log_event('TRAY_COMPLETE', details)
    assert data.flush(timeout=5)
    assert data.close(timeout=5)
    raw = path.read_bytes()
    assert raw.startswith(prefix)
    rows = list(csv.reader(io.StringIO(raw.decode('utf-8-sig'), newline=''), strict=True))
    assert rows[:2] == [HEADER, OLD]
    assert len(rows) == 3 and all(len(row) == 4 for row in rows)
    assert json.loads(rows[-1][3]) == details
    assert evidence(path, suffix).read_bytes() == suffix
    assert len(list(path.parent.glob(path.name + '.held-*'))) == 1


@pytest.mark.parametrize('suffix', [b'\xef\xbb', b'\xef\xbb\xbftimestamp,work', b'\0' * 4097],
    ids=['split-bom', 'partial-header', 'nul-header'])
def test_torn_header_is_preserved_before_fresh_header_and_row(data, suffix):
    path = Path(data._get_log_filepath())
    path.write_bytes(suffix)
    data.log_event('TRAY_COMPLETE', {'new': True})
    assert data.flush(timeout=5)
    assert data.close(timeout=5)
    raw = path.read_bytes()
    assert raw.startswith(b'\xef\xbb\xbf' + encoded(HEADER))
    rows = list(csv.reader(io.StringIO(raw.decode('utf-8-sig'), newline=''), strict=True))
    assert len(rows) == 2 and all(len(row) == 4 for row in rows)
    assert evidence(path, suffix).read_bytes() == suffix


@pytest.mark.parametrize('cached', [False, True])
def test_cut_inside_audit_prefix_is_rejected_without_mutation(data, cached, capsys):
    path = Path(data._get_log_filepath())
    data.log_event('TRAY_COMPLETE', {'scanner_audit': {'key': 'protected-key'}})
    assert data.flush(timeout=5)
    original = path.read_bytes()
    if not cached:
        data._audit_csv_indexes.clear()  # The persisted checkpoint proof remains.
    torn = original[:-3]
    path.write_bytes(torn)
    data.log_event('TRAY_COMPLETE', {'scanner_audit': {'key': 'next-key'}})
    with pytest.raises(RuntimeError):
        data.flush(timeout=5)
    assert path.read_bytes() == torn
    assert not list(path.parent.glob(path.name + '.held-*'))
    assert '감사 CSV' in capsys.readouterr().out


def test_evidence_failure_cannot_truncate_or_append(data, monkeypatch):
    import label_data_manager as storage
    path = Path(data._get_log_filepath())
    original = b'\xef\xbb\xbf' + encoded(HEADER, OLD) + b'torn'
    path.write_bytes(original)

    def unavailable(*args, **kwargs):
        raise OSError('synthetic evidence storage failure')

    monkeypatch.setattr(storage, 'write_checked_bytes', unavailable)
    data.log_event('TRAY_COMPLETE', {'scanner_audit': {'key': 'next-key'}})
    with pytest.raises(RuntimeError):
        data.flush(timeout=5)
    assert path.read_bytes() == original


def test_existing_fragment_evidence_is_reused_and_next_append_is_clean(data):
    path = Path(data._get_log_filepath())
    prefix = b'\xef\xbb\xbf' + encoded(HEADER, OLD)
    suffix = b'torn'
    path.write_bytes(prefix + suffix)
    held = evidence(path, suffix)
    held.write_bytes(suffix)
    before = held.stat().st_mtime_ns
    for key in ('one', 'two'):
        data.log_event('TRAY_COMPLETE', {'scanner_audit': {'key': key}})
    assert data.flush(timeout=5)
    assert data.close(timeout=5)
    assert held.stat().st_mtime_ns == before
    assert held.read_bytes() == suffix
    with path.open(encoding='utf-8-sig', newline='') as stream:
        rows = list(csv.reader(stream, strict=True))
    assert len(rows) == 4
    for row in rows[1:]:
        datetime.fromisoformat(row[0])
    assert [json.loads(row[3])['scanner_audit']['key'] for row in rows[2:]] == ['one', 'two']


def test_torn_suffix_after_protected_audit_prefix_can_be_replayed_once(data):
    path = Path(data._get_log_filepath())
    first = {'scanner_audit': {'key': 'first', 'occurred_at': '2026-10-27T01:00:00'}}
    second = {'scanner_audit': {'key': 'second', 'occurred_at': '2026-10-27T01:01:00'}}
    data.log_event('TRAY_COMPLETE', first)
    assert data.flush(timeout=5)
    prefix = path.read_bytes()
    suffix = encoded(['2026-10-27T01:01:00', 'worker', 'TRAY_COMPLETE', json.dumps(second)])[:-7]
    with path.open('ab') as stream:
        stream.write(suffix)
    data.log_event('TRAY_COMPLETE', second)
    assert data.flush(timeout=5)
    raw = path.read_bytes()
    assert raw.startswith(prefix)
    data.log_event('TRAY_COMPLETE', second)
    assert data.flush(timeout=5)
    assert path.read_bytes() == raw
    assert evidence(path, suffix).read_bytes() == suffix


def test_repaired_sent_prefix_resets_relay_delta_to_zero(data, tmp_path):
    from types import SimpleNamespace
    from tools import direct_sync_relay_runner as relay

    path = Path(data._get_log_filepath())
    prefix = b'\xef\xbb\xbf' + encoded(HEADER, OLD)
    suffix = b'incomplete\0\r\n'
    path.write_bytes(prefix + suffix)
    config = SimpleNamespace(db_path=str(tmp_path / 'relay.sqlite3'), spool_dir=str(tmp_path / 'spool'))
    relay._record_source_sent_byte_count(config.db_path, path, len(prefix + suffix))
    data.log_event('TRAY_COMPLETE', {'scanner_audit': {'key': 'new'}})
    assert data.flush(timeout=5)
    result = relay._build_delta_source_file(config, path)
    assert result is not None
    delta, relative, end = result
    assert delta.read_bytes() == path.read_bytes()
    assert relative.rsplit('/', 1)[-1].startswith('bytes-0-')
    assert end == path.stat().st_size

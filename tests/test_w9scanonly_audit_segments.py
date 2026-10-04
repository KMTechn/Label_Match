"""Audit recovery at the real backlog boundary and interrupted segment writes."""
import csv
import hashlib
import json
from pathlib import Path

import pytest

import Label_Match as lm
import label_data_manager as storage
from tests.test_w9scanonly_packaging import packaging, scan  # noqa: F401
from tests.test_legacy_label_transition import MASTER


def records_for(app, count):
    return [app.data_manager.prepare_audit_event(app.Events.SET_RESTORED, {
        'set_id': 'SET-' + str(index), 'reason': 'SCANNER_UNDO',
        'scanned_product_barcodes': [MASTER]}) for index in range(count)]


def restart(app):
    manager = app.data_manager
    manager.close(timeout=5)
    app.data_manager = lm.DataManager(app.save_directory, manager.process_name,
                                     'NEXT-SHIFT', manager.unique_id)
    return app.data_manager


def save(app, records):
    return app.data_manager.save_current_state({
        'current_set_info': app.current_set_info, 'scanner_audit_outbox': records})


def csv_audits(app):
    rows = []
    for path in Path(app.save_directory).rglob('*.csv'):
        with path.open(encoding='utf-8-sig', newline='') as stream:
            for row in csv.reader(stream):
                if len(row) == 4 and row[0] != 'timestamp':
                    details = json.loads(row[3])
                    if details.get('scanner_audit'):
                        rows.append(details)
    return rows


def checkpoint(app):
    return Path(app.save_directory) / app.data_manager._current_state_filename()


def segments(app):
    return sorted((Path(app.save_directory) / '_scanner_audit_outbox').glob('*.json'))


@pytest.fixture
def small_segment_limit(monkeypatch):
    monkeypatch.setattr(storage, 'RECOVERY_MAX_BYTES', 8192)
    monkeypatch.setattr(storage, 'AUDIT_SEGMENT_BYTES', 8192)


def test_product_written_large_backlog_replays_exactly_once_after_two_restarts(packaging, monkeypatch):
    app, _ = packaging
    scan(app, MASTER)
    # The review's 8,939 normal product-created records total 10,090,517
    # bytes in the old inline format; no malformed or padded audit input.
    records = records_for(app, 8939)
    manager = app.data_manager
    original_open = manager._open_file

    def csv_unavailable(path, mode, *args, **kwargs):
        if str(path).endswith('.csv') and 'a' in mode:
            raise OSError('CSV unavailable during the audit backlog')
        return original_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(manager, '_open_file', csv_unavailable)
    manager.log_event(records[0]['event_type'], records[0]['details'])
    with pytest.raises(RuntimeError, match='CSV unavailable'):
        manager.flush(timeout=5)
    legacy = {'current_set_info': app.current_set_info,
              'scanner_audit_outbox': records, 'worker_name': app.worker_name}
    assert len(json.dumps(legacy, ensure_ascii=False, indent=4,
                          cls=lm.DateTimeEncoder).encode('utf-8')) > 10_000_000
    assert save(app, records)
    files = [checkpoint(app), *segments(app)]
    assert sum(path.stat().st_size for path in files) > 8 * 1024 * 1024
    assert all(path.stat().st_size <= 8 * 1024 * 1024 for path in files)
    original = {path: path.read_bytes() for path in segments(app)}
    with pytest.raises(RuntimeError, match='CSV unavailable'):
        manager.close(timeout=5)
    app.data_manager = lm.DataManager(app.save_directory, manager.process_name,
                                     'NEXT-SHIFT', manager.unique_id)
    manager = app.data_manager
    assert manager._audit_outbox == records
    assert manager.replay_audit_events(timeout=600)
    assert not manager._audit_outbox
    assert restart(app).replay_audit_events()
    assert restart(app).replay_audit_events()
    rows = csv_audits(app)
    assert [row['scanner_audit']['key'] for row in rows] == [
        item['details']['scanner_audit']['key'] for item in records]
    assert rows == [item['details'] for item in records]
    assert all(path.read_bytes() == raw for path, raw in original.items())


def test_legacy_large_inline_checkpoint_remains_readable_and_migrates(packaging):
    app, _ = packaging
    scan(app, MASTER)
    records = records_for(app, 8939)
    raw = json.dumps({'current_set_info': app.current_set_info,
                      'scanner_audit_outbox': records, 'worker_name': app.worker_name},
                     ensure_ascii=False, indent=4, cls=lm.DateTimeEncoder).encode('utf-8')
    assert len(raw) > 8 * 1024 * 1024
    checkpoint(app).write_bytes(raw)
    assert storage.read_recovery_file(checkpoint(app), current_state=True)['verified']
    assert checkpoint(app).read_bytes() == raw
    manager = restart(app)
    assert manager._audit_outbox == records
    assert manager.save_current_state(manager.load_current_state())
    assert checkpoint(app).stat().st_size < 8 * 1024 * 1024
    assert restart(app)._audit_outbox == records


@pytest.mark.parametrize('damage', [b'{broken', b'{"scanner_audit_outbox": []}'])
def test_damaged_segment_preserves_pending_and_allows_other_segments_to_replay(
        packaging, monkeypatch, small_segment_limit, damage):
    app, _ = packaging
    scan(app, MASTER)
    records = records_for(app, 40)
    assert save(app, records)
    references = json.loads(checkpoint(app).read_bytes())['scanner_audit_segments']
    damaged = Path(app.save_directory) / '_scanner_audit_outbox' / (references[1] + '.json')
    original = damaged.read_bytes()
    lost_keys = {item['details']['scanner_audit']['key'] for item in
                 json.loads(original)['scanner_audit_outbox']}
    damaged.write_bytes(damage)
    manager = restart(app)
    assert {item['details']['scanner_audit']['key'] for item in manager._audit_outbox} == {
        item['details']['scanner_audit']['key'] for item in records} - lost_keys
    assert not manager.replay_audit_events()
    assert len(csv_audits(app)) == len(records) - len(lost_keys)
    assert json.loads(checkpoint(app).read_bytes())['scanner_audit_segments'] == [references[1]]
    assert damaged.read_bytes() == damage
    manager.delete_current_state()
    assert checkpoint(app).exists()
    # Other work may checkpoint while one damaged audit remains preserved.
    fresh = manager.prepare_audit_event(app.Events.SET_RESTORED, {
        'set_id': 'NEW-WORK', 'reason': 'SCANNER_UNDO', 'scanned_product_barcodes': [MASTER]})
    assert manager.save_current_state({'current_set_info': {'id': None, 'raw': []},
                                       'scanner_audit_outbox': [fresh]})
    assert not restart(app).replay_audit_events()
    damaged.write_bytes(original)
    assert app.data_manager.replay_audit_events()
    assert restart(app).replay_audit_events()
    rows = csv_audits(app)
    assert len(rows) == len(records) + 1
    assert {row['scanner_audit']['key'] for row in rows} == {
        item['details']['scanner_audit']['key'] for item in [*records, fresh]}


def test_interrupted_checkpoint_preserves_segments_without_replaying_uncommitted_audits(
        packaging, monkeypatch, small_segment_limit):
    app, _ = packaging
    scan(app, MASTER)
    previous = checkpoint(app).read_bytes()
    records = records_for(app, 40)
    write = storage.write_checked_bytes

    def fail_checkpoint(path, *args, **kwargs):
        if Path(path) == checkpoint(app):
            raise OSError('checkpoint interrupted after durable segments')
        return write(path, *args, **kwargs)

    monkeypatch.setattr(storage, 'write_checked_bytes', fail_checkpoint)
    assert not save(app, records)
    assert segments(app)
    assert checkpoint(app).read_bytes() == previous
    assert not restart(app)._audit_outbox
    assert app.data_manager.replay_audit_events()
    assert not csv_audits(app)
    monkeypatch.setattr(storage, 'write_checked_bytes', write)
    assert save(app, records)
    assert restart(app).replay_audit_events()
    assert len(csv_audits(app)) == len(records)


def test_segment_ack_checkpoint_failure_replays_without_duplicates(
        packaging, monkeypatch, small_segment_limit):
    app, _ = packaging
    scan(app, MASTER)
    records = records_for(app, 40)
    assert save(app, records)
    raw = checkpoint(app).read_bytes()
    manager = restart(app)
    monkeypatch.setattr(manager, 'save_current_state', lambda *args, **kwargs: False)
    assert not manager.replay_audit_events()
    assert checkpoint(app).read_bytes() == raw
    assert restart(app).replay_audit_events()
    assert restart(app).replay_audit_events()
    assert len(csv_audits(app)) == len(records)


def test_held_checkpoint_replays_segments_without_rewriting_originals(
        packaging, small_segment_limit):
    app, _ = packaging
    scan(app, MASTER)
    records = records_for(app, 40)
    assert save(app, records)
    raw = checkpoint(app).read_bytes()
    archive = checkpoint(app).with_name(checkpoint(app).name + '.held-CASE')
    checkpoint(app).rename(archive)
    evidence = storage.read_recovery_file(archive, current_state=True, expected_bytes=raw)
    assert evidence['verified'] and evidence['sha256'] == hashlib.sha256(raw).hexdigest()
    manager = restart(app)
    assert not manager._audit_outbox
    assert manager.replay_audit_events(archived_events=evidence['value']['scanner_audit_outbox'])
    assert archive.read_bytes() == raw and not checkpoint(app).exists()
    assert restart(app).replay_audit_events(archived_events=evidence['value']['scanner_audit_outbox'])
    assert len(csv_audits(app)) == len(records)


@pytest.mark.parametrize('damage', ['middle', 'same-size-middle', 'same-size-key', 'conflicting-duplicate',
                                  'duplicate', 'truncated', 'shorter', 'reordered'])
@pytest.mark.parametrize('reopen', [False, True], ids=['live-writer', 'restart'])
def test_changed_csv_falls_back_to_the_original_full_row_verdict(packaging, damage, reopen):
    """Proof mismatches retain the old per-key verdict and exact CSV bytes."""
    import io
    import os

    app, _ = packaging
    scan(app, MASTER)
    records = records_for(app, 8)
    assert save(app, records)
    assert app.data_manager.replay_audit_events()
    # A later checkpoint can still carry the original keys after an ACK loss.
    assert save(app, records)
    path = Path(app.data_manager._get_log_filepath_for_item([
        records[0]['details']['scanner_audit']['occurred_at'], '',
        records[0]['event_type'], '']))
    original = path.read_bytes()
    original_stat = path.stat()
    rows = list(csv.reader(io.StringIO(original.decode('utf-8-sig'), newline='')))
    positions = [index for index, row in enumerate(rows)
                 if len(row) == 4 and row[0] != 'timestamp'
                 and json.loads(row[3]).get('scanner_audit')]
    expected_rows = {app.data_manager._audit_key(rows[index]): rows[index].copy() for index in positions}
    if damage == 'truncated':
        changed = original[:-8]
    else:
        if damage in ('middle', 'same-size-middle', 'same-size-key', 'conflicting-duplicate'):
            if damage == 'conflicting-duplicate':
                rows.insert(positions[3], rows[positions[3]].copy())
            details = json.loads(rows[positions[3]][3])
            if damage == 'same-size-key':
                key = details['scanner_audit']['key']
                details['scanner_audit']['key'] = key[:-1] + ('0' if key[-1] != '0' else '1')
            else:
                details['reason'] = 'SCANNER_EDIT' if damage == 'same-size-middle' else 'CHANGED_VALID_JSON'
            rows[positions[3]][3] = json.dumps(details, ensure_ascii=False)
        elif damage == 'duplicate':
            rows.insert(positions[3], rows[positions[3]].copy())
        elif damage == 'shorter':
            rows = rows[:positions[-2]]
        else:
            left, right = positions[2:4]
            rows[left], rows[right] = rows[right], rows[left]
        stream = io.StringIO(newline='')
        csv.writer(stream).writerows(rows)
        changed = stream.getvalue().encode('utf-8-sig')
    path.write_bytes(changed)
    if damage in ('same-size-middle', 'same-size-key'):
        assert len(changed) == len(original)
        os.utime(path, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))
    manager = restart(app) if reopen else app.data_manager
    # Literal reference to the old writer: scan every row per pending key,
    # reject only same-key/different-content, otherwise fsync or append.
    legacy_failed, legacy_bytes = set(), changed
    for record in records:
        key = record['details']['scanner_audit']['key']
        duplicate = False
        for row in csv.reader(io.StringIO(legacy_bytes.decode('utf-8-sig'), newline='')):
            if manager._audit_key(row) == key:
                if row != expected_rows[key]:
                    legacy_failed.add(key)
                    break
                duplicate = True
        if key not in legacy_failed and not duplicate:
            stream = io.StringIO(newline='')
            csv.writer(stream).writerow(expected_rows[key])
            legacy_bytes += stream.getvalue().encode('utf-8')
    assert manager.replay_audit_events() is (not legacy_failed)
    assert manager._audit_outbox == [item for item in records
        if item['details']['scanner_audit']['key'] in legacy_failed]
    assert path.read_bytes() == legacy_bytes
    assert all(str(error) == 'audit key has different CSV content' for error in manager._writer_errors)
    if legacy_failed:
        with pytest.raises(RuntimeError, match='audit key has different CSV content'):
            manager.close(timeout=5)
        app.data_manager = lm.DataManager(app.save_directory, manager.process_name,
                                         'NEXT-SHIFT', manager.unique_id)
    else:
        assert restart(app).replay_audit_events()
        assert path.read_bytes() == legacy_bytes


@pytest.mark.parametrize('damage', ['root', 'null-proof', 'size', 'rows', 'sha256', 'last_row_sha256'])
def test_unreadable_prefix_proof_uses_all_csv_rows_before_ack(packaging, damage, monkeypatch):
    app, _ = packaging
    scan(app, MASTER)
    records = records_for(app, 8)
    assert save(app, records)
    assert app.data_manager.replay_audit_events()
    assert save(app, records)
    state = json.loads(checkpoint(app).read_bytes())
    proofs = state['scanner_audit_csv_prefixes']
    if damage == 'root':
        state['scanner_audit_csv_prefixes'] = 'unreadable\x00hint'
    elif damage == 'null-proof':
        proofs[next(iter(proofs))] = None
    else:
        proofs[next(iter(proofs))][damage] = None
    checkpoint(app).write_text(json.dumps(state, ensure_ascii=False), encoding='utf-8')
    manager = restart(app)
    assert manager._audit_outbox == records
    original = {path: path.read_bytes() for path in Path(app.save_directory).rglob('*.csv')}
    read_keys, audit_key = [], manager._audit_key

    def observed_row(row):
        key = audit_key(row)
        if key:
            read_keys.append(key)
        return key

    monkeypatch.setattr(manager, '_audit_key', observed_row)
    assert manager.replay_audit_events()
    assert all(read_keys.count(item['details']['scanner_audit']['key']) >= 2 for item in records)
    assert not manager._audit_outbox
    assert all(path.read_bytes() == raw for path, raw in original.items())
    assert restart(app).replay_audit_events()
    assert len(csv_audits(app)) == len(records)


def test_durable_rows_after_stale_prefix_checkpoint_restart_exactly_once(packaging, monkeypatch):
    app, _ = packaging
    scan(app, MASTER)
    records = records_for(app, 8)
    assert save(app, records[:4])
    assert app.data_manager.replay_audit_events()
    assert save(app, records[4:])
    raw = checkpoint(app).read_bytes()
    assert json.loads(raw)['scanner_audit_csv_prefixes']
    monkeypatch.setattr(app.data_manager, 'save_current_state', lambda *args, **kwargs: False)
    assert not app.data_manager.replay_audit_events()
    assert checkpoint(app).read_bytes() == raw
    assert csv_audits(app) == [item['details'] for item in records]
    original = {path: path.read_bytes() for path in Path(app.save_directory).rglob('*.csv')}
    assert restart(app).replay_audit_events()
    assert restart(app).replay_audit_events()
    assert not app.data_manager._audit_outbox
    assert all(path.read_bytes() == value for path, value in original.items())
    assert csv_audits(app) == [item['details'] for item in records]

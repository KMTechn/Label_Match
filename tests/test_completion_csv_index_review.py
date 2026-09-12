"""Independent observable CSV truth checks; no GUI or production state."""
import builtins
from datetime import date, timedelta
from pathlib import Path
from contextlib import closing
import csv
import json
import os
import sqlite3
import subprocess
import sys
from types import SimpleNamespace

import pytest

from completion_csv_index import CompletionCsvIndex
from tests.test_label_match_core import load_label_match_module

PREFIX = 'AUDIT작업이벤트로그_PC1_'


def csv_file(root, identities, pc='PC1'):
    path = root / f'AUDIT작업이벤트로그_{pc}_20260912.csv'
    with path.open('w', encoding='utf-8-sig', newline='') as handle:
        writer = csv.writer(handle)
        writer.writerow(['timestamp', 'worker_name', 'event', 'details'])
        for identity in identities:
            writer.writerow(['2026-09-12T10:00:00', 'AUDIT', 'TRAY_COMPLETE', json.dumps({'set_id': identity})])
    return path


def append_event(path, identity, event='TRAY_COMPLETE'):
    with path.open('a', encoding='utf-8-sig', newline='') as handle:
        csv.writer(handle).writerow(['2026-09-12T10:00:00', 'AUDIT', event, json.dumps({'set_id': identity})])
        handle.flush()
        os.fsync(handle.fileno())


def truth(path):
    with path.open(encoding='utf-8-sig', newline='') as handle:
        return [json.loads(row['details'])['set_id'] for row in csv.DictReader(handle)
                if row['event'] == 'TRAY_COMPLETE']


def manager(root):
    return SimpleNamespace(save_directory=str(root), process_name='AUDIT', unique_id='PC1', flush=lambda **kwargs: True)


@pytest.mark.parametrize('restart', [False, True])
def test_same_length_csv_repair_preserving_mtime_cannot_hide_completion(tmp_path, restart):
    module = load_label_match_module()
    path = csv_file(tmp_path, ['old-set'])
    data = manager(tmp_path)
    assert module._label_match_local_completion_event_exists(data, 'new-set') is False
    before = path.stat()
    signature = CompletionCsvIndex.signature(path)
    # In-place copy/repair preserves creation time on Windows; restore mtime,
    # as timestamp-preserving file tools do. Length stays identical.
    csv_file(tmp_path, ['new-set'])
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert CompletionCsvIndex.signature(path) == signature
    assert truth(path) == ['new-set']
    if restart:
        data = manager(tmp_path)
    assert module._label_match_local_completion_event_exists(data, 'new-set') is True


@pytest.mark.parametrize('restart', [False, True])
def test_two_appends_with_same_mtime_are_detected_by_size(tmp_path, restart):
    module = load_label_match_module()
    path = csv_file(tmp_path, ['old-set'])
    data = manager(tmp_path)
    assert module._label_match_local_completion_event_exists(data, 'new-set') is False
    before = path.stat()
    for identity in ['first-new', 'new-set']:
        append_event(path, identity)
        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    if restart:
        data = manager(tmp_path)
    assert module._label_match_local_completion_event_exists(data, 'new-set') is True


def test_lost_completion_rows_with_valid_sqlite_structure_are_not_proof_of_absence(tmp_path):
    module = load_label_match_module()
    path = csv_file(tmp_path, ['real-set'])
    data = manager(tmp_path)
    assert module._label_match_local_completion_event_exists(data, 'new-set') is False
    with closing(sqlite3.connect(data._completion_csv_index.path)) as conn:
        with conn:
            conn.execute('DELETE FROM completions')
        assert conn.execute('PRAGMA quick_check').fetchone()[0] == 'ok'
    assert truth(path) == ['real-set']
    assert module._label_match_local_completion_event_exists(data, 'real-set') is True


@pytest.mark.parametrize('own_event', ['SCAN_ATTEMPT', 'TRAY_COMPLETE'])
def test_external_append_between_writer_stat_and_publication_is_not_covered(tmp_path, own_event):
    module = load_label_match_module()
    path = csv_file(tmp_path, ['old-set'])
    data = manager(tmp_path)
    assert module._label_match_local_completion_event_exists(data, 'external') is False
    index = data._completion_csv_index
    # Deterministic scheduling of DataManager's stat -> write -> note_append:
    # an external writer appends after the saved pre-write signature.
    before = index.signature(path)
    append_event(path, 'external')
    append_event(path, 'own-set', own_event)
    index.note_append(path, before, own_event, json.dumps({'set_id': 'own-set'}))
    assert 'external' in truth(path)
    assert module._label_match_local_completion_event_exists(data, 'external') is True


def test_different_pc_csv_is_not_accepted_as_local_completion(tmp_path):
    module = load_label_match_module()
    csv_file(tmp_path, ['other-pc'], pc='PC2')
    data = manager(tmp_path)
    assert module._label_match_local_completion_event_exists(data, 'other-pc') is False
    csv_file(tmp_path, ['local'])
    assert module._label_match_local_completion_event_exists(data, 'local') is True
    assert module._label_match_local_completion_event_exists(data, 'other-pc') is False


def test_metadata_preserving_repair_does_not_duplicate_real_completion_on_commit(tmp_path):
    module = load_label_match_module()
    path = csv_file(tmp_path, ['old-set'])
    data = module.DataManager(str(tmp_path), 'AUDIT', 'WORKER', 'PC1')
    data._get_log_filepath_for_item = lambda item: str(path)
    try:
        assert module._label_match_local_completion_event_exists(data, 'new-set') is False
        before = path.stat()
        csv_file(tmp_path, ['new-set'])
        os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        app = object.__new__(module.Label_Match)
        app.data_manager = data
        markers = []
        app.package_outbox = SimpleNamespace(mark_local_completion_committed=lambda *a, **k: markers.append(a))
        app._queue_authoritative_package = lambda **kwargs: {'idempotency_key': 'same-key', 'membership_mode': 'INHERIT_ALL'}
        app._commit_finalized_set_durable(
            details={'set_id': 'new-set'}, item_code='ITEM', is_manual_complete=False,
            result=app.Results.PASS, central_inherit_all=True, set_id_for_log='new-set',
        )
        assert markers == [('same-key',)]
        assert truth(path).count('new-set') == 1
    finally:
        data.close(5)


def test_actual_writer_detects_external_append_after_prewrite_stat(tmp_path, monkeypatch):
    module = load_label_match_module()
    path = csv_file(tmp_path, ['old-set'])
    data = module.DataManager(str(tmp_path), 'AUDIT', 'WORKER', 'PC1')
    data._get_log_filepath_for_item = lambda item: str(path)
    real_open = builtins.open
    injected = []

    def open_with_external_append(file, mode='r', *args, **kwargs):
        if Path(file) == path and mode == 'a' and not injected:
            injected.append(True)
            with real_open(path, 'a', encoding='utf-8-sig', newline='') as handle:
                csv.writer(handle).writerow(['2026-09-12T10:00:00', 'AUDIT', 'TRAY_COMPLETE', json.dumps({'set_id': 'external'})])
                handle.flush()
                os.fsync(handle.fileno())
        return real_open(file, mode, *args, **kwargs)

    try:
        assert module._label_match_local_completion_event_exists(data, 'external') is False
        monkeypatch.setattr(module, 'open', open_with_external_append, raising=False)
        data.log_event('TRAY_COMPLETE', {'set_id': 'own-set'})
        assert data.flush(5)
        assert injected == [True]
        assert data._writer_errors == []
        assert truth(path) == ['old-set', 'external', 'own-set']
        assert module._label_match_local_completion_event_exists(data, 'external') is True
    finally:
        data.close(5)


def test_single_byte_lookup_index_damage_falls_back_to_csv(tmp_path):
    module = load_label_match_module()
    path = csv_file(tmp_path, ['real-set'])
    data = manager(tmp_path)
    assert module._label_match_local_completion_event_exists(data, 'new-set') is False
    index_path = data._completion_csv_index.path
    content = bytearray(index_path.read_bytes())
    # The final occurrence is in the completions PK lookup b-tree. Keep
    # schema, file length and source coverage untouched; corrupt one key byte.
    offset = content.rfind(b'real-set')
    assert offset >= 0
    content[offset] = ord('f')
    index_path.write_bytes(content)
    with closing(sqlite3.connect(index_path)) as conn:
        assert conn.execute('PRAGMA quick_check').fetchone()[0] == 'ok'
        full_check = [row[0] for row in conn.execute('PRAGMA integrity_check')]
        assert full_check != ['ok']
    assert truth(path) == ['real-set']
    assert module._label_match_local_completion_event_exists(data, 'real-set') is True


@pytest.mark.parametrize('age', [0, 1, 2, -1, None])
def test_recent_and_unknown_dates_are_scanned_even_if_index_returns_absence(tmp_path, monkeypatch, age):
    module = load_label_match_module()
    path = csv_file(tmp_path, ['real-set'])
    suffix = 'unknown' if age is None else (date.today() - timedelta(days=age)).strftime('%Y%m%d')
    path.rename(tmp_path / f'{PREFIX}{suffix}.csv')
    data = manager(tmp_path)
    monkeypatch.setattr(CompletionCsvIndex, 'candidates', lambda *a: [])
    assert module._label_match_local_completion_event_exists(data, 'real-set') is True


@pytest.mark.parametrize('restart', [False, True])
def test_archive_middle_repair_with_identical_metadata_and_tail_is_detected(tmp_path, restart):
    path = csv_file(tmp_path, ['old-set'] + [f'filler-{i:04d}' for i in range(200)])
    path = path.rename(tmp_path / f'{PREFIX}20000101.csv')
    index = CompletionCsvIndex(tmp_path, PREFIX)
    assert index.candidates('new-set') == []
    before = path.stat()
    content = path.read_bytes()
    changed = content.replace(b'old-set', b'new-set', 1)
    assert changed[-4096:] == content[-4096:]
    path.write_bytes(changed)
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert index.signature(path) == (before.st_size, before.st_mtime_ns, before.st_ctime_ns)
    if restart:
        index = CompletionCsvIndex(tmp_path, PREFIX)
    assert index.candidates('new-set') == [str(path)]


@pytest.mark.parametrize('restart', [False, True])
@pytest.mark.parametrize('damage', ['deleted-row', 'lookup-byte', 'partial-database'])
def test_archive_cache_damage_cannot_duplicate_completion(tmp_path, restart, damage):
    module = load_label_match_module()
    path = csv_file(tmp_path, ['real-set']).rename(tmp_path / f'{PREFIX}20000101.csv')
    data = module.DataManager(str(tmp_path), 'AUDIT', 'WORKER', 'PC1')
    data._get_log_filepath_for_item = lambda item: str(path)
    try:
        assert module._label_match_local_completion_event_exists(data, 'new-set') is False
        index = data._completion_csv_index
        before = index.path.stat()
        if damage == 'deleted-row':
            with closing(sqlite3.connect(index.path)) as conn:
                with conn:
                    conn.execute('DELETE FROM completions')
                assert conn.execute('PRAGMA integrity_check').fetchall() == [('ok',)]
        else:
            content = bytearray(index.path.read_bytes())
            if damage == 'lookup-byte':
                content[content.rfind(b'real-set')] = ord('f')
            else:
                content[len(content) // 2:] = b'\0' * (len(content) - len(content) // 2)
            index.path.write_bytes(content)
        os.utime(index.path, ns=(before.st_atime_ns, before.st_mtime_ns))
        assert index.signature(index.path) == (before.st_size, before.st_mtime_ns, before.st_ctime_ns)
        if restart:
            data._completion_csv_index = CompletionCsvIndex(tmp_path, PREFIX)
        app = object.__new__(module.Label_Match)
        app.data_manager = data
        markers = []
        app.package_outbox = SimpleNamespace(mark_local_completion_committed=lambda *a, **k: markers.append(a))
        app._queue_authoritative_package = lambda **kwargs: {'idempotency_key': 'same-key', 'membership_mode': 'INHERIT_ALL'}
        app._commit_finalized_set_durable(
            details={'set_id': 'real-set'}, item_code='ITEM', is_manual_complete=False,
            result=app.Results.PASS, central_inherit_all=True, set_id_for_log='real-set',
        )
        assert markers == [('same-key',)]
        assert truth(path) == ['real-set']
        with closing(sqlite3.connect(data._completion_csv_index.path)) as conn:
            assert conn.execute('PRAGMA integrity_check').fetchall() == [('ok',)]
            assert conn.execute('SELECT set_id FROM completions').fetchall() == [('real-set',)]
    finally:
        data.close(5)


@pytest.mark.parametrize('stage', ['partial-temp', 'after-replace'])
def test_process_exit_during_index_publication_recovers_from_csv(tmp_path, stage):
    path = csv_file(tmp_path, ['old-set']).rename(tmp_path / f'{PREFIX}20000101.csv')
    code = '''
import csv, json, os, sys
from pathlib import Path
from completion_csv_index import CompletionCsvIndex
root, prefix, stage = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
path = root / (prefix + '20000101.csv')
index = CompletionCsvIndex(root, prefix)
assert index.candidates('new-set') == []
previous = index.signature(path)
with open(path, 'a', encoding='utf-8-sig', newline='') as handle:
    csv.writer(handle).writerow(['2026-09-12T10:00:00', 'AUDIT', 'TRAY_COMPLETE', json.dumps({'set_id': 'new-set'})])
    handle.flush()
    os.fsync(handle.fileno())
publish = index._publish
def stop(temporary):
    if stage == 'partial-temp':
        Path(temporary).write_bytes(b'partial index publication')
    else:
        publish(temporary)
    os._exit(23)
index._publish = stop
index.note_append(path, previous, 'TRAY_COMPLETE', json.dumps({'set_id': 'new-set'}))
'''
    result = subprocess.run([sys.executable, '-c', code, str(tmp_path), PREFIX, stage],
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 23, result.stderr
    assert truth(path) == ['old-set', 'new-set']
    index = CompletionCsvIndex(tmp_path, PREFIX)
    rebuilds = []
    original = index._rebuild
    index._rebuild = lambda sources: rebuilds.append(True) or original(sources)
    assert index.candidates('new-set') == [str(path)]
    assert rebuilds == [True]
    assert truth(path).count('new-set') == 1


@pytest.mark.parametrize('own_event', ['SCAN_ATTEMPT', 'TRAY_COMPLETE'])
def test_archive_append_coverage_includes_external_rows(tmp_path, own_event):
    path = csv_file(tmp_path, ['old-set']).rename(tmp_path / f'{PREFIX}20000101.csv')
    index = CompletionCsvIndex(tmp_path, PREFIX)
    assert index.candidates('external') == []
    before = index.signature(path)
    append_event(path, 'external')
    append_event(path, 'own-set', own_event)
    index.note_append(path, before, own_event, json.dumps({'set_id': 'own-set'}))
    assert index.candidates('external') == [str(path)]
    assert index.candidates('old-set') == [str(path)]
    assert CompletionCsvIndex(tmp_path, PREFIX).candidates('external') == [str(path)]


def test_failed_append_publication_preserves_index_and_durable_csv(tmp_path, monkeypatch):
    module = load_label_match_module()
    path = csv_file(tmp_path, ['old-set']).rename(tmp_path / f'{PREFIX}20000101.csv')
    data = module.DataManager(str(tmp_path), 'AUDIT', 'WORKER', 'PC1')
    data._get_log_filepath_for_item = lambda item: str(path)
    try:
        assert module._label_match_local_completion_event_exists(data, 'new-set') is False
        index = data._completion_csv_index
        previous = index.path.read_bytes()
        def fail_publication(temporary):
            Path(temporary).write_bytes(b'partial cache write')
            raise OSError('cache publication failed')
        monkeypatch.setattr(index, '_publish', fail_publication)
        data.log_event('TRAY_COMPLETE', {'set_id': 'new-set'})
        assert data.flush(5)
        assert not data._writer_errors
        assert index.path.read_bytes() == previous
        assert truth(path) == ['old-set', 'new-set']
        # Same-instance fallback remains safe even while rebuilding fails.
        assert module._label_match_local_completion_event_exists(data, 'new-set') is True
        data._completion_csv_index = CompletionCsvIndex(tmp_path, PREFIX)
        assert module._label_match_local_completion_event_exists(data, 'new-set') is True
        assert truth(path).count('new-set') == 1
    finally:
        data.close(5)

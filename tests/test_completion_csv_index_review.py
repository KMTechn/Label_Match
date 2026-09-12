"""Independent observable CSV truth checks; no GUI or production state."""
import builtins
from datetime import date, timedelta
from pathlib import Path
from contextlib import closing
import csv
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from types import SimpleNamespace

import pytest

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


def signature(path):
    stat = path.stat()
    return stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


def obsolete_index(root, identities=('real-set',)):
    """Leave the retired SQLite layout on disk as an untrusted upgrade artifact."""
    scope = hashlib.sha256(PREFIX.encode('utf-8')).hexdigest()[:24]
    path = root / f'_completion_index_{scope}.sqlite3'
    with closing(sqlite3.connect(path)) as conn:
        with conn:
            conn.execute('CREATE TABLE completions (set_id TEXT, source_id INTEGER, PRIMARY KEY (set_id, source_id))')
            conn.executemany('INSERT INTO completions VALUES (?, 0)', ((value,) for value in identities))
    return path


def commit(module, data, identity):
    app = object.__new__(module.Label_Match)
    app.data_manager = data
    markers = []
    app.package_outbox = SimpleNamespace(mark_local_completion_committed=lambda *a, **k: markers.append(a))
    app._queue_authoritative_package = lambda **kwargs: {'idempotency_key': 'same-key', 'membership_mode': 'INHERIT_ALL'}
    app._commit_finalized_set_durable(
        details={'set_id': identity}, item_code='ITEM', is_manual_complete=False,
        result=app.Results.PASS, central_inherit_all=True, set_id_for_log=identity,
    )
    assert markers == [('same-key',)]


@pytest.mark.parametrize('restart', [False, True])
def test_same_length_csv_repair_preserving_mtime_cannot_hide_completion(tmp_path, restart):
    module = load_label_match_module()
    path = csv_file(tmp_path, ['old-set'])
    data = manager(tmp_path)
    assert module._label_match_local_completion_event_exists(data, 'new-set') is False
    before = path.stat()
    before_signature = signature(path)
    # In-place copy/repair preserves creation time on Windows; restore mtime,
    # as timestamp-preserving file tools do. Length stays identical.
    csv_file(tmp_path, ['new-set'])
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert signature(path) == before_signature
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
    with closing(sqlite3.connect(obsolete_index(tmp_path))) as conn:
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
    append_event(path, 'external')
    append_event(path, 'own-set', own_event)
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
    index_path = obsolete_index(tmp_path)
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


@pytest.mark.parametrize('age', [0, 1, 2, 3, 4, -1, None])
def test_all_dates_are_scanned_despite_empty_obsolete_index(tmp_path, age):
    module = load_label_match_module()
    path = csv_file(tmp_path, ['real-set'])
    suffix = 'unknown' if age is None else (date.today() - timedelta(days=age)).strftime('%Y%m%d')
    path.rename(tmp_path / f'{PREFIX}{suffix}.csv')
    data = manager(tmp_path)
    obsolete_index(tmp_path, identities=())
    assert module._label_match_local_completion_event_exists(data, 'real-set') is True


@pytest.mark.parametrize('restart', [False, True])
def test_archive_middle_repair_with_identical_metadata_and_tail_is_detected(tmp_path, restart):
    module = load_label_match_module()
    path = csv_file(tmp_path, ['old-set'] + [f'filler-{i:04d}' for i in range(200)])
    path = path.rename(tmp_path / f'{PREFIX}20000101.csv')
    data = manager(tmp_path)
    assert module._label_match_local_completion_event_exists(data, 'new-set') is False
    before = path.stat()
    content = path.read_bytes()
    changed = content.replace(b'old-set', b'new-set', 1)
    assert changed[-4096:] == content[-4096:]
    path.write_bytes(changed)
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert signature(path) == (before.st_size, before.st_mtime_ns, before.st_ctime_ns)
    if restart:
        data = manager(tmp_path)
    assert module._label_match_local_completion_event_exists(data, 'new-set') is True


@pytest.mark.parametrize('restart', [False, True])
@pytest.mark.parametrize('damage', ['deleted-row', 'lookup-byte', 'partial-database'])
def test_archive_cache_damage_cannot_duplicate_completion(tmp_path, restart, damage):
    module = load_label_match_module()
    path = csv_file(tmp_path, ['real-set']).rename(tmp_path / f'{PREFIX}20000101.csv')
    data = module.DataManager(str(tmp_path), 'AUDIT', 'WORKER', 'PC1')
    data._get_log_filepath_for_item = lambda item: str(path)
    try:
        assert module._label_match_local_completion_event_exists(data, 'new-set') is False
        index_path = obsolete_index(tmp_path)
        before = index_path.stat()
        if damage == 'deleted-row':
            with closing(sqlite3.connect(index_path)) as conn:
                with conn:
                    conn.execute('DELETE FROM completions')
                assert conn.execute('PRAGMA integrity_check').fetchall() == [('ok',)]
        else:
            content = bytearray(index_path.read_bytes())
            if damage == 'lookup-byte':
                offset = content.rfind(b'real-set')
                assert offset >= 0
                content[offset] = ord('f')
            else:
                content[len(content) // 2:] = b'\0' * (len(content) - len(content) // 2)
            index_path.write_bytes(content)
        os.utime(index_path, ns=(before.st_atime_ns, before.st_mtime_ns))
        assert signature(index_path) == (before.st_size, before.st_mtime_ns, before.st_ctime_ns)
        damaged = index_path.read_bytes()
        if restart:
            assert data.close(5)
            data = module.DataManager(str(tmp_path), 'AUDIT', 'WORKER', 'PC1')
            data._get_log_filepath_for_item = lambda item: str(path)
        commit(module, data, 'real-set')
        assert truth(path) == ['real-set']
        assert not data._writer_errors
        assert index_path.read_bytes() == damaged, 'retired index must be ignored, not rebuilt'
    finally:
        data.close(5)


@pytest.mark.parametrize('stage', ['partial-temp', 'after-replace'])
def test_process_exit_after_durable_csv_with_partial_obsolete_index_recovers(tmp_path, stage):
    path = csv_file(tmp_path, ['old-set']).rename(tmp_path / f'{PREFIX}20000101.csv')
    index_path = obsolete_index(tmp_path, identities=('old-set',))
    code = '''
import os, sys
from pathlib import Path
from tests.test_label_match_core import load_label_match_module
root, path, index_path, stage = map(Path, sys.argv[1:])
module = load_label_match_module()
data = module.DataManager(str(root), 'AUDIT', 'WORKER', 'PC1')
data._get_log_filepath_for_item = lambda item: str(path)
data.log_event('TRAY_COMPLETE', {'set_id': 'new-set'})
assert data.flush(5)
temporary = index_path.with_suffix('.sqlite3.partial')
temporary.write_bytes(b'partial obsolete index publication')
if str(stage) == 'after-replace':
    os.replace(temporary, index_path)
os._exit(23)
'''
    result = subprocess.run([sys.executable, '-c', code, str(tmp_path), str(path), str(index_path), stage],
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 23, result.stderr
    assert truth(path) == ['old-set', 'new-set']
    module = load_label_match_module()
    data = module.DataManager(str(tmp_path), 'AUDIT', 'WORKER', 'PC1')
    data._get_log_filepath_for_item = lambda item: str(path)
    try:
        assert module._label_match_local_completion_event_exists(data, 'new-set') is True
        commit(module, data, 'new-set')
        assert not data._writer_errors
        assert truth(path).count('new-set') == 1
    finally:
        data.close(5)


@pytest.mark.parametrize('own_event', ['SCAN_ATTEMPT', 'TRAY_COMPLETE'])
def test_archive_append_search_includes_external_rows(tmp_path, own_event):
    module = load_label_match_module()
    path = csv_file(tmp_path, ['old-set']).rename(tmp_path / f'{PREFIX}20000101.csv')
    data = manager(tmp_path)
    assert module._label_match_local_completion_event_exists(data, 'external') is False
    append_event(path, 'external')
    append_event(path, 'own-set', own_event)
    assert module._label_match_local_completion_event_exists(data, 'external') is True
    assert module._label_match_local_completion_event_exists(data, 'old-set') is True
    assert module._label_match_local_completion_event_exists(manager(tmp_path), 'external') is True


def test_unavailable_obsolete_index_cannot_fail_durable_csv_write(tmp_path, monkeypatch):
    module = load_label_match_module()
    path = csv_file(tmp_path, ['old-set']).rename(tmp_path / f'{PREFIX}20000101.csv')
    data = module.DataManager(str(tmp_path), 'AUDIT', 'WORKER', 'PC1')
    data._get_log_filepath_for_item = lambda item: str(path)
    index_path = obsolete_index(tmp_path)
    previous = index_path.read_bytes()
    real_open = builtins.open
    def reject_index(file, *args, **kwargs):
        if Path(file) == index_path:
            pytest.fail('retired index must not be opened')
        return real_open(file, *args, **kwargs)
    monkeypatch.setattr(module, 'open', reject_index, raising=False)
    try:
        assert module._label_match_local_completion_event_exists(data, 'new-set') is False
        data.log_event('TRAY_COMPLETE', {'set_id': 'new-set'})
        assert data.flush(5)
        assert not data._writer_errors
        assert index_path.read_bytes() == previous
        assert truth(path) == ['old-set', 'new-set']
        assert module._label_match_local_completion_event_exists(data, 'new-set') is True
        assert module._label_match_local_completion_event_exists(manager(tmp_path), 'new-set') is True
        assert truth(path).count('new-set') == 1
    finally:
        data.close(5)

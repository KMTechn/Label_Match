"""The optional index must never turn stale coverage into a missing completion."""
import builtins
from contextlib import closing
import csv
import json
import os
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from completion_csv_index import CompletionCsvIndex
from tests.test_label_match_core import load_label_match_module


PREFIX = "AUDIT작업이벤트로그_PC1_"


def write_csv(directory, name="20260912", identities=("existing",)):
    path = directory / f"{PREFIX}{name}.csv"
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp", "worker_name", "event", "details"])
        for identity in identities:
            writer.writerow(["2026-09-12T10:00:00", "AUDIT", "TRAY_COMPLETE", json.dumps({"set_id": identity})])
    return path


def test_complete_index_covers_new_set_without_reopening_csv_and_survives_restart(tmp_path, monkeypatch):
    write_csv(tmp_path)
    index = CompletionCsvIndex(tmp_path, PREFIX)
    assert index.candidates("new") == []
    original_open = builtins.open

    def no_csv(file, *args, **kwargs):
        assert Path(file).suffix != ".csv", "covered lookup reread archive CSV"
        return original_open(file, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", no_csv)
    assert index.candidates("another-new") == []
    restarted = CompletionCsvIndex(tmp_path, PREFIX)
    assert restarted.candidates("another-new") == []
    assert restarted.candidates("existing") == [str(tmp_path / f"{PREFIX}20260912.csv")]
    assert CompletionCsvIndex(tmp_path, PREFIX.replace("PC1", "PC2")).path != index.path


@pytest.mark.parametrize("damage", ["missing", "corrupt", "append", "shrink", "backdated", "new-file", "missing-coverage"])
def test_missing_damaged_or_changed_coverage_rebuilds_from_all_csv(tmp_path, damage):
    path = write_csv(tmp_path, identities=("first", "second"))
    index = CompletionCsvIndex(tmp_path, PREFIX)
    assert index.candidates("new") == []
    if damage == "missing":
        index.path.unlink()
    elif damage == "corrupt":
        index.path.write_bytes(b"broken SQLite index")
    elif damage == "append":
        write_csv(tmp_path, identities=("first", "second", "new"))
    elif damage == "shrink":
        write_csv(tmp_path, identities=("new",))
    elif damage == "backdated":
        old = path.stat()
        write_csv(tmp_path, identities=("new", "second"))
        os.utime(path, ns=(old.st_atime_ns, old.st_mtime_ns - 1_000_000_000))
    elif damage == "new-file":
        path = write_csv(tmp_path, "20260913", identities=("new",))
    else:
        with closing(sqlite3.connect(index.path)) as conn:
            with conn:
                conn.execute("DELETE FROM sources")
    expected = "first" if damage in {"missing", "corrupt", "missing-coverage"} else "new"
    rebuilt = []
    original = index._rebuild
    index._rebuild = lambda sources: rebuilt.append(tuple(sources)) or original(sources)
    assert index.candidates(expected) == [str(path)]
    assert rebuilt and path.name in rebuilt[0]
    assert CompletionCsvIndex(tmp_path, PREFIX).candidates(expected) == [str(path)]


def test_failed_index_rebuild_falls_back_to_csv_and_keeps_sync_barrier(tmp_path, monkeypatch):
    module = load_label_match_module()
    path = write_csv(tmp_path)
    barriers = []
    manager = SimpleNamespace(save_directory=str(tmp_path), process_name="AUDIT", unique_id="PC1",
                              flush=lambda **kwargs: barriers.append("flush"))
    monkeypatch.setattr(CompletionCsvIndex, "_rebuild", lambda *args: (_ for _ in ()).throw(OSError("index unavailable")))
    original = os.fsync
    monkeypatch.setattr(os, "fsync", lambda fd: barriers.append("fsync") or original(fd))
    assert module._label_match_local_completion_event_exists(manager, "existing") is True
    assert barriers == ["flush", "fsync"]
    with path.open(encoding="utf-8-sig", newline="") as handle:
        assert len(list(csv.DictReader(handle))) == 1


def test_rebuild_sync_failure_preserves_previous_index_until_complete_publication(tmp_path, monkeypatch):
    write_csv(tmp_path)
    index = CompletionCsvIndex(tmp_path, PREFIX)
    assert index.candidates('new') == []
    previous = index.path.read_bytes()
    path = write_csv(tmp_path, '20260913', identities=('new',))
    with monkeypatch.context() as failure:
        failure.setattr(os, 'fsync', lambda fd: (_ for _ in ()).throw(OSError('index sync failed')))
        assert index.candidates('new') is None
    assert index.path.read_bytes() == previous
    assert index.candidates('new') == [str(path)]
    with closing(sqlite3.connect(index.path)) as conn:
        assert conn.execute('PRAGMA journal_mode').fetchone()[0] == 'delete'
        assert conn.execute('PRAGMA synchronous').fetchone()[0] == 2


def test_index_stat_failure_cannot_fail_a_durable_csv_write(tmp_path, monkeypatch):
    module = load_label_match_module()
    manager = module.DataManager(str(tmp_path), 'AUDIT', 'WORKER', 'PC1')
    try:
        assert module._label_match_local_completion_event_exists(manager, 'durable') is False
        with monkeypatch.context() as failure:
            failure.setattr(manager._completion_csv_index, 'signature',
                            lambda path: (_ for _ in ()).throw(OSError('index metadata unavailable')))
            manager.log_event('TRAY_COMPLETE', {'set_id': 'durable'})
            assert manager.flush(5)
        assert manager._writer_errors == []
        assert module._label_match_local_completion_event_exists(manager, 'durable') is True
    finally:
        manager.close(5)


@pytest.mark.parametrize("index_failure", [False, True])
def test_writer_updates_index_after_csv_fsync_and_failure_rebuilds(tmp_path, monkeypatch, index_failure):
    module = load_label_match_module()
    manager = module.DataManager(str(tmp_path), "AUDIT", "WORKER", "PC1")
    try:
        assert module._label_match_local_completion_event_exists(manager, "durable") is False
        index = manager._completion_csv_index
        original_connect = sqlite3.connect
        original_note = index.note_append
        original_fsync = os.fsync
        synced = []
        monkeypatch.setattr(os, "fsync", lambda fd: synced.append(True) or original_fsync(fd))

        def note(*args):
            if args[2] == "TRAY_COMPLETE":
                assert synced, "index advanced before CSV fsync"
            return original_note(*args)

        monkeypatch.setattr(index, "note_append", note)
        if index_failure:
            monkeypatch.setattr(sqlite3, "connect", lambda *args, **kwargs: (_ for _ in ()).throw(sqlite3.OperationalError("index unavailable")))
        manager.log_event("SCAN_ATTEMPT", {"set_id": "durable"})
        manager.log_event("TRAY_COMPLETE", {"set_id": "durable"})
        assert manager.flush(5)
        monkeypatch.setattr(sqlite3, "connect", original_connect)
        rebuilds = []
        original_rebuild = index._rebuild
        monkeypatch.setattr(index, "_rebuild", lambda sources: rebuilds.append(True) or original_rebuild(sources))
        assert module._label_match_local_completion_event_exists(manager, "durable") is True
        assert bool(rebuilds) is index_failure
        assert module._label_match_local_completion_event_exists(manager, "durable") is True
        with open(manager._get_log_filepath(), encoding="utf-8-sig", newline="") as handle:
            assert sum(row["event"] == "TRAY_COMPLETE" for row in csv.DictReader(handle)) == 1
    finally:
        manager.close(5)

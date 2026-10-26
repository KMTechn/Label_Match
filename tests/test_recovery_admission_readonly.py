"""Refusing broken recovery evidence at start must leave the store files untouched."""
import os
import sqlite3

import pytest

from package_errors import PackageRecoveryEvidenceError, PackageRecoveryStorageError
from package_logistics import PackageOutbox, PackageCancellationOutbox
from tests.test_package_recovery_ledger import immutable
from tests.test_recovery_storage_and_main import fail_recovery_query, logical_snapshot, recovered


def _files(path):
    """DB and WAL bytes and mtime; for -shm only existence and size.

    SQLite's shared-memory index is reader bookkeeping: any read-only reader
    updates one read-mark byte in it. It is derived, not store content.
    """
    out = {}
    for suffix in ("", "-wal", "-shm"):
        item = f"{path}{suffix}"
        if suffix == "-shm":
            out[suffix] = os.path.getsize(item) if os.path.exists(item) else None
        elif os.path.exists(item):
            with open(item, "rb") as handle:
                out[suffix] = (handle.read(), os.stat(item).st_mtime_ns)
        else:
            out[suffix] = None
    return out


@pytest.fixture
def wal_store(tmp_path):
    outbox = recovered(tmp_path)
    path = str(outbox.db_path)
    keeper = sqlite3.connect(path, isolation_level=None)
    keeper.execute("PRAGMA journal_mode=WAL")
    keeper.execute("PRAGMA wal_autocheckpoint=0")
    # A committed change stays in the WAL, as after a crashed or still-open writer.
    keeper.execute("UPDATE package_recovery_evidence SET contract_sha256='unknown'")
    assert os.path.getsize(path + "-wal") > 0
    yield outbox, path, keeper
    keeper.close()


@pytest.mark.parametrize("store", [PackageOutbox, PackageCancellationOutbox])
def test_evidence_refusal_leaves_db_wal_shm_bytes_and_mtime(wal_store, store):
    _outbox, path, keeper = wal_store
    before = _files(path)
    with pytest.raises(PackageRecoveryEvidenceError) as failure:
        store(path)
    assert failure.value.code == PackageRecoveryEvidenceError().code
    assert _files(path) == before


def test_storage_read_failure_refusal_leaves_files_and_logical_content(tmp_path, monkeypatch):
    outbox = recovered(tmp_path)
    path = str(outbox.db_path)
    keeper = sqlite3.connect(path, isolation_level=None)
    keeper.execute("PRAGMA journal_mode=WAL")
    keeper.execute("PRAGMA wal_autocheckpoint=0")
    keeper.execute("CREATE TABLE IF NOT EXISTS marker(x)")
    keeper.execute("INSERT INTO marker VALUES (1)")
    before_files = _files(path)
    before = logical_snapshot(outbox)
    reached = fail_recovery_query(monkeypatch)
    with pytest.raises(PackageRecoveryStorageError):
        PackageOutbox(path)
    assert reached
    assert _files(path) == before_files
    assert logical_snapshot(outbox) == before
    keeper.close()


def test_valid_store_still_opens_and_checkpoints(tmp_path):
    outbox = recovered(tmp_path)
    path = str(outbox.db_path)
    keeper = sqlite3.connect(path, isolation_level=None)
    keeper.execute("PRAGMA journal_mode=WAL")
    keeper.execute("PRAGMA wal_autocheckpoint=0")
    keeper.execute("CREATE TABLE IF NOT EXISTS marker(x)")
    keeper.execute("INSERT INTO marker VALUES (1)")
    assert os.path.getsize(path + "-wal") > 0
    before = immutable(outbox)
    PackageOutbox(path)
    PackageCancellationOutbox(path)
    assert os.path.getsize(path + "-wal") == 0  # accepting path still checkpoints
    assert immutable(outbox) == before
    keeper.close()

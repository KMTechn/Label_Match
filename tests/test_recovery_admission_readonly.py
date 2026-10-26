"""Refusing broken recovery evidence at start must leave the store files untouched."""
import os
import sqlite3

import pytest

from package_errors import PackageLogisticsError, PackageRecoveryEvidenceError, PackageRecoveryStorageError
from package_logistics import PackageOutbox, PackageCancellationOutbox
from package_recovery import CONTRACT_SHA256
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

# --- RO to RW gaps: once the verdict is refusal, no writer step may rewrite the files ---

def _break_evidence(keeper):
    keeper.execute("UPDATE package_recovery_evidence SET contract_sha256='unknown'")


def test_metadata_read_failure_in_ro_view_still_refuses_without_rewriting(wal_store, monkeypatch):
    _outbox, path, keeper = wal_store
    before = _files(path)
    connect = sqlite3.connect
    reached = []

    class MetadataFailure(sqlite3.Connection):
        def execute(self, sql, *args, **kwargs):
            if sql.startswith("SELECT name FROM sqlite_master WHERE type="):
                reached.append(True)
                raise sqlite3.OperationalError("injected metadata read failure")
            return super().execute(sql, *args, **kwargs)

    def failing_connect(*args, **kwargs):
        if kwargs.get("uri"):
            kwargs["factory"] = MetadataFailure
        return connect(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", failing_connect)
    with pytest.raises(PackageRecoveryEvidenceError):
        PackageOutbox(path)
    assert reached
    assert _files(path) == before


@pytest.mark.parametrize("store", [PackageOutbox, PackageCancellationOutbox])
def test_evidence_changed_between_ro_check_and_writer_refuses_without_rewriting(wal_store, monkeypatch, store):
    import package_logistics
    outbox, path, keeper = wal_store
    keeper.execute("UPDATE package_recovery_evidence SET contract_sha256=?", (CONTRACT_SHA256,))  # valid at the RO check
    original = package_logistics._admit_recovery_storage_readonly
    seen = []

    def changed_after_admission(db_path):
        original(db_path)
        _break_evidence(keeper)
        seen.append(_files(path))

    monkeypatch.setattr(package_logistics, "_admit_recovery_storage_readonly", changed_after_admission)
    with pytest.raises(PackageRecoveryEvidenceError):
        store(path)
    assert seen
    assert _files(path) == seen[0]


@pytest.mark.parametrize("store", [PackageOutbox, PackageCancellationOutbox])
def test_missing_evidence_table_with_broken_evidence_refuses_without_rewriting(tmp_path, store):
    outbox = recovered(tmp_path)
    path = str(outbox.db_path)
    keeper = sqlite3.connect(path, isolation_level=None)
    keeper.execute("PRAGMA journal_mode=WAL")
    keeper.execute("PRAGMA wal_autocheckpoint=0")
    keeper.execute("DROP TABLE package_recovery_review_closures")
    _break_evidence(keeper)
    before = _files(path)
    with pytest.raises(PackageRecoveryEvidenceError):
        store(path)
    assert _files(path) == before
    keeper.close()

# --- aux files, schema-too-new order, one read snapshot ---

def _break_closed_store(tmp_path):
    """Evidence broken, every connection closed: no -wal/-shm left behind."""
    outbox = recovered(tmp_path)
    path = str(outbox.db_path)
    conn = sqlite3.connect(path)
    conn.execute("UPDATE package_recovery_evidence SET contract_sha256='unknown'")
    conn.commit()
    conn.close()
    return path


@pytest.mark.parametrize("store", [PackageOutbox, PackageCancellationOutbox])
def test_refusal_on_closed_store_keeps_db_bytes_and_leaves_only_empty_aux_files(tmp_path, store):
    path = _break_closed_store(tmp_path)
    with open(path, "rb") as handle:
        db_before = (handle.read(), os.stat(path).st_mtime_ns)
    with pytest.raises(PackageRecoveryEvidenceError):
        store(path)
    with open(path, "rb") as handle:
        assert (handle.read(), os.stat(path).st_mtime_ns) == db_before
    # Documented: a read-only open of a WAL store may create an empty -wal and a -shm index.
    if os.path.exists(path + "-wal"):
        assert os.path.getsize(path + "-wal") == 0


@pytest.mark.parametrize("store", [PackageOutbox, PackageCancellationOutbox])
def test_refusal_with_wal_but_no_shm_keeps_db_and_wal_bytes(wal_store, tmp_path, store):
    import shutil
    _outbox, path, keeper = wal_store
    target = str(tmp_path / "copy" / "without-shm.db")
    os.makedirs(os.path.dirname(target))
    shutil.copyfile(path, target)
    shutil.copyfile(path + "-wal", target + "-wal")
    assert not os.path.exists(target + "-shm")
    before = _files(target)
    with pytest.raises(PackageRecoveryEvidenceError):
        store(target)
    after = _files(target)
    assert after[""] == before[""] and after["-wal"] == before["-wal"]


def test_newer_schema_store_reports_schema_too_new_before_evidence_refusal(wal_store):
    _outbox, path, keeper = wal_store
    keeper.execute("INSERT OR REPLACE INTO package_outbox_schema_info(key,value) VALUES ('schema_version','label-match-package-outbox-v999')")
    with pytest.raises(PackageLogisticsError) as failure:
        PackageOutbox(path)
    assert failure.value.code == "PACKAGE_OUTBOX_SCHEMA_TOO_NEW"


def test_ro_check_reads_inside_one_transaction(wal_store, monkeypatch):
    _outbox, path, keeper = wal_store
    connect = sqlite3.connect
    log = []

    class Logged(sqlite3.Connection):
        def execute(self, sql, *args, **kwargs):
            log.append(sql.strip())
            return super().execute(sql, *args, **kwargs)

    def logged_connect(*args, **kwargs):
        if kwargs.get("uri"):
            kwargs["factory"] = Logged
        return connect(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", logged_connect)
    with pytest.raises(PackageRecoveryEvidenceError):
        PackageOutbox(path)
    assert log[0].upper().startswith("BEGIN")
    assert any(item.startswith("SELECT * FROM package_recovery_evidence") for item in log)

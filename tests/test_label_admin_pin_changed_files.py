"""Changed recovery files remain individually recoverable across startup."""

import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

import Label_Match as app_module
import label_safe_path
from label_admin_pin import AdminPinIntentStore
from tests.test_label_admin_pin import _active_set_pin_app, _intent, _pin_app


def _restart(app, tmp_path):
    restarted = object.__new__(app_module.Label_Match)
    restarted.__dict__.update(app.__dict__)
    restarted.package_outbox = app_module.PackageOutbox(tmp_path / "outbox.sqlite3")
    restarted._admin_pin_store = AdminPinIntentStore(tmp_path / "admin-pin-intents.sqlite3")
    restarted.current_set_info = {"id": None, "raw": []}
    return restarted


def _restored_f5(tmp_path, monkeypatch, *, rehold_first=False):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    source = journal.path
    selected = "F5:" + hashlib.sha256(source.read_bytes()).hexdigest()
    data = json.loads(source.read_bytes())
    data["state"]["prepare_idempotency_key"] = "PREPARE-CHANGED"
    changed_bytes = json.dumps(data).encode()
    native_open, native_rename = label_safe_path._open, label_safe_path._rename_handle
    changed = False

    def mutate(path, access, *args, **kwargs):
        nonlocal changed
        if not changed and path == source and access == label_safe_path._READ | label_safe_path._DELETE:
            source.write_bytes(changed_bytes)
            changed = True
        return native_open(path, access, *args, **kwargs)

    def interrupt(handle, target, *, replace):
        native_rename(handle, target, replace=replace)
        raise OSError("interrupted after atomic move")

    with monkeypatch.context() as patch:
        patch.setattr(label_safe_path, "_open", mutate)
        patch.setattr(label_safe_path, "_rename_handle", interrupt)
        assert app._run_package_pin_action("LABEL.F5_HOLD", selected) is False
    key = _intent(app)[0]
    app._recover_pending_package_pin_moves()
    held = app._admin_pin_store.changed_hold(key)
    assert held["state"] == "QUARANTINED"
    changed_target = "F5:" + hashlib.sha256(changed_bytes).hexdigest()
    if rehold_first:
        server.issued, server.consumed = None, False
        assert app._run_package_pin_action("LABEL.F5_HOLD", changed_target) is True
    server.issued, server.consumed = None, False
    assert "복원" in str(app._run_package_pin_action(
        "LABEL.RECHECK", changed_target, "__RESTORE_CHANGED_PIN_FILE__"))
    assert source.read_bytes() == changed_bytes
    assert app._admin_pin_store.changed_hold(key)["state"] == "RESTORED"
    return app, server, source, changed_bytes, changed_target, held


@pytest.mark.parametrize("rehold_first", [False, True])
def test_restored_f5_survives_startup(tmp_path, monkeypatch, rehold_first):
    app, _server, source, raw, target, held = _restored_f5(
        tmp_path, monkeypatch, rehold_first=rehold_first)
    restarted = _restart(app, tmp_path)
    restarted._recover_pending_package_pin_moves()
    assert restarted._finalize_label_recovery_holds() is True
    assert source.read_bytes() == raw
    assert not Path(held["archive_path"]).exists()
    assert restarted._admin_pin_store.changed_hold(held["operation_key"])["state"] == "RESTORED"
    assert not restarted._package_recovery_file_issues
    assert restarted.package_outbox.get_workbench_hold(target)


@pytest.mark.parametrize("mismatch", ["archive", "missing", "different", "both"])
def test_restored_f5_mismatch_keeps_manager_recovery(tmp_path, monkeypatch, mismatch):
    app, server, source, raw, target, held = _restored_f5(tmp_path, monkeypatch)
    archive = Path(held["archive_path"])
    if mismatch == "archive":
        source.rename(archive)
    elif mismatch == "missing":
        source.rename(source.with_suffix(".preserved"))
    elif mismatch == "different":
        source.write_bytes(raw + b"\n")
    else:
        archive.write_bytes(raw)
    before = source.read_bytes() if source.exists() else None
    archived = archive.read_bytes() if archive.exists() else None
    restarted = _restart(app, tmp_path)
    restarted._recover_pending_package_pin_moves()
    assert restarted._finalize_label_recovery_holds() is True
    candidate = next(row for row in restarted._package_recovery_candidates()
                     if row["set_id"] == target)
    assert candidate["changed_hold"]["state"] == "RESTORED"
    assert candidate["unverified"] == "RESTORED_FILE_UNVERIFIED"
    assert target in restarted._package_recovery_file_issues
    assert (source.read_bytes() if source.exists() else None) == before
    assert (archive.read_bytes() if archive.exists() else None) == archived
    if mismatch == "archive":
        server.issued, server.consumed = None, False
        assert "복원" in str(restarted._run_package_pin_action(
            "LABEL.RECHECK", target, "__RESTORE_CHANGED_PIN_FILE__"))
        assert source.read_bytes() == raw
        assert not archive.exists()
        assert restarted._finalize_label_recovery_holds() is True
        assert source.read_bytes() == raw


@pytest.mark.parametrize("restore_order", [("current", "journal"), ("journal", "current")])
def test_linked_changed_files_are_held_and_restored_individually(
        tmp_path, monkeypatch, restore_order):
    app, journal, current, server, set_id = _active_set_pin_app(tmp_path, monkeypatch, linked=True)
    hold_id = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    native_open, native_rename = label_safe_path._open, label_safe_path._rename_handle
    current_data = json.loads(current.read_bytes())
    current_data["timestamp"] = "2026-09-24T00:00:04"
    changed_current = json.dumps(current_data).encode()
    changed = set()

    def mutate_current(path, access, *args, **kwargs):
        if path == current and access == label_safe_path._READ | label_safe_path._DELETE and "current" not in changed:
            current.write_bytes(changed_current)
            changed.add("current")
        return native_open(path, access, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(label_safe_path, "_open", mutate_current)
        assert app._run_package_pin_action("LABEL.F5_HOLD", "F5:" + hold_id) is False
    key = _intent(app)[0]
    app._recover_pending_package_pin_moves()
    assert app._admin_pin_store.changed_hold(key)["state"] == "QUARANTINED"
    data = json.loads(journal.path.read_bytes())
    data["state"]["prepare_idempotency_key"] = "PREPARE-SECOND-CHANGED"
    changed_journal = json.dumps(data).encode()

    def mutate_journal(path, access, *args, **kwargs):
        if path == journal.path and access == label_safe_path._READ | label_safe_path._DELETE and "journal" not in changed:
            journal.path.write_bytes(changed_journal)
            changed.add("journal")
        return native_open(path, access, *args, **kwargs)

    def interrupt(handle, target, *, replace):
        native_rename(handle, target, replace=replace)
        if target == journal.path.with_name(journal.path.name + ".held-" + hold_id):
            raise OSError("interrupted immediately after second journal rename")

    with monkeypatch.context() as patch:
        patch.setattr(label_safe_path, "_open", mutate_journal)
        patch.setattr(label_safe_path, "_rename_handle", interrupt)
        assert app._finalize_label_recovery_holds(only_hold_id=hold_id) is True
    restarted = _restart(app, tmp_path)
    restarted._recover_pending_package_pin_moves()
    rows = restarted._admin_pin_store.changed_holds()
    assert len(rows) == 2
    assert {row["operation_key"] for row in rows} == {key}
    expected = {"current": (current, changed_current), "journal": (journal.path, changed_journal)}
    candidates = {row["set_id"]: row for row in restarted._package_recovery_candidates()}
    for row in rows:
        source, raw = expected[row["file_kind"]]
        target = ("CURRENT:" if row["file_kind"] == "current" else "F5:") + row["changed_sha256"]
        assert row["state"] == "QUARANTINED"
        assert not source.exists()
        assert Path(row["archive_path"]).read_bytes() == raw
        assert candidates[target]["changed_hold"] == row
        assert json.loads(restarted.package_outbox.get_workbench_hold(target)["snapshot_json"])["status"] == "UNVERIFIED"
    assert restarted._admin_pin_store.get(key)["state"] == "QUARANTINED_CHANGED"
    for kind in restore_order:
        row = next(row for row in rows if row["file_kind"] == kind)
        target = ("CURRENT:" if kind == "current" else "F5:") + row["changed_sha256"]
        server.issued, server.consumed = None, False
        assert "복원" in str(restarted._run_package_pin_action(
            "LABEL.RECHECK", target, "__RESTORE_CHANGED_PIN_FILE__"))
        restarted = _restart(restarted, tmp_path)
        restarted._recover_pending_package_pin_moves()
        assert restarted._finalize_label_recovery_holds() is True
        source, raw = expected[kind]
        assert source.read_bytes() == raw
        assert not Path(row["archive_path"]).exists()
    assert {row["state"] for row in restarted._admin_pin_store.changed_holds()} == {"RESTORED"}
    assert restarted._admin_pin_store.get(key)["state"] == "QUARANTINED_CHANGED"
    assert restarted.package_outbox.get_workbench_hold(set_id)
    assert restarted.package_outbox.get_label_exchange_hold(hold_id)


def test_legacy_changed_hold_rows_migrate_without_loss(tmp_path):
    path = tmp_path / "pin.sqlite3"
    with sqlite3.connect(path) as conn:
        conn.execute("""CREATE TABLE admin_pin_changed_holds (
            operation_key TEXT PRIMARY KEY, target_id TEXT NOT NULL,
            source_path TEXT NOT NULL, archive_path TEXT NOT NULL,
            original_sha256 TEXT NOT NULL, changed_sha256 TEXT NOT NULL,
            file_kind TEXT NOT NULL, state TEXT NOT NULL)""")
        for state in ("PREPARED", "QUARANTINED", "RESTORING", "RESTORED"):
            conn.execute("INSERT INTO admin_pin_changed_holds VALUES (?,?,?,?,?,?,?,?)",
                         (state, "target", "current", "archive-" + state,
                          "a" * 64, "b" * 64, "current", state))
    store = AdminPinIntentStore(path)
    original = store.changed_holds()
    store.stage_changed_hold(operation_key="QUARANTINED", target_id="target",
                             source_path="journal", archive_path="journal-archive",
                             original_sha256="c" * 64, changed_sha256="d" * 64,
                             file_kind="journal")
    store.finish_changed_hold("QUARANTINED", "RESTORED", file_kind="journal", source_path="journal")
    reopened = AdminPinIntentStore(path)
    assert reopened.changed_holds()[:4] == original
    assert reopened.changed_hold("QUARANTINED", file_kind="journal", source_path="journal")["state"] == "RESTORED"
    with sqlite3.connect(path) as conn:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"

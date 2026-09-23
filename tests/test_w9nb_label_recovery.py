"""Focused journal hold and trusted catalog retry regressions."""

import hashlib
import json
from types import SimpleNamespace

import pytest

import Label_Match as app_module
from phs_label_workflow import PHSLabelExchangeJournal


def _recovery_app(tmp_path, monkeypatch, state):
    monkeypatch.setenv("KMTECH_LABEL_WRITER_TEST_MODE", "1")
    monkeypatch.setenv("KMTECH_LABEL_WRITER_CONTROL_ROOT", str(tmp_path / "fence"))
    monkeypatch.setattr(app_module, "is_protected_admin_code", lambda code: code == "admin")
    journal = PHSLabelExchangeJournal(tmp_path / "label-exchange.json")
    journal.save(state)
    app = object.__new__(app_module.Label_Match)
    app.run_tests = True
    app.current_set_info = {"id": None, "raw": []}
    app.package_outbox = app_module.PackageOutbox(tmp_path / "outbox.sqlite3")
    app.phs_label_exchange_coordinator = SimpleNamespace(journal=journal)
    app._render_operator_workbench = lambda: None
    app._show_package_recovery_block = lambda *args, **kwargs: None
    return app, journal


def test_f5_hold_requires_manager_and_preserves_exact_journal(tmp_path, monkeypatch):
    state = {
        "workflow_mode": "RECONCILIATION", "status": "PREPARE_PENDING",
        "set_id": "", "scan_payload": "PHS2-SOURCE-A",
        "active_scan_label_id": "LABEL-A", "authority_scope_id": "SCOPE-A",
        "prepare_idempotency_key": "PREPARE-A", "exchange_id": "",
    }
    app, journal = _recovery_app(tmp_path, monkeypatch, state)
    original = journal.path.read_bytes()
    digest = hashlib.sha256(original).hexdigest()

    assert app._hold_label_recovery(manager_code="wrong") is False
    assert app.package_outbox.get_label_exchange_hold(digest) is None
    assert journal.path.read_bytes() == original

    assert app._hold_label_recovery(manager_code="admin") is True
    held = app.package_outbox.get_label_exchange_hold(digest)
    assert bytes(held["journal_bytes"]) == original
    assert held["held_by"] == "protected-admin-local"
    assert not journal.path.exists()
    assert (tmp_path / f"label-exchange.json.held-{digest}").read_bytes() == original
    assert app._label_recovery_source_is_held("PHS2-SOURCE-A") is True
    assert app._label_recovery_source_is_held("PHS2-SOURCE-B") is False
    assert app._label_recovery_source_is_held("PHS2-SOURCE-B", "LABEL-A") is True
    assert app._recheck_package_recovery_set("F5:" + digest, manager_code="wrong") == "관리자 확인이 필요합니다."
    assert "보류" in app._recheck_package_recovery_set("F5:" + digest, manager_code="admin")
    assert json.loads(bytes(held["journal_bytes"]))["state"]["prepare_idempotency_key"] == "PREPARE-A"


def test_f5_crash_after_hold_readback_finishes_archive(tmp_path, monkeypatch):
    state = {"workflow_mode": "RECONCILIATION", "status": "PREPARED",
             "scan_payload": "PHS2-SOURCE-A", "active_scan_label_id": "LABEL-A",
             "prepare_idempotency_key": "PREPARE-A", "exchange_id": "EXCHANGE-A"}
    app, journal = _recovery_app(tmp_path, monkeypatch, state)
    original = journal.path.read_bytes()
    digest = hashlib.sha256(original).hexdigest()
    archive = journal.path.with_name(journal.path.name + ".held-" + digest)
    app.package_outbox.hold_label_exchange(
        hold_id=digest, set_id="", label_id="LABEL-A",
        source_label="PHS2-SOURCE-A", source_input_tag_id="",
        journal_bytes=original, archive_path=str(archive),
        held_by="protected-admin-local",
    )

    assert app._finalize_label_recovery_holds() is True
    assert archive.read_bytes() == original
    assert not journal.path.exists()
    assert app._active_label_recovery_state() == {}


def test_f5_archive_failure_preserves_global_lock_and_blocks_same_label(tmp_path, monkeypatch):
    state = {"workflow_mode": "RECONCILIATION", "status": "PREPARED",
             "scan_payload": "PHS2-SOURCE-A", "active_scan_label_id": "LABEL-A",
             "prepare_idempotency_key": "PREPARE-A", "exchange_id": "EXCHANGE-A"}
    app, journal = _recovery_app(tmp_path, monkeypatch, state)
    original = journal.path.read_bytes()
    digest = hashlib.sha256(original).hexdigest()
    original_replace = app_module.os.replace
    monkeypatch.setattr(app_module.os, "replace", lambda *_args: (_ for _ in ()).throw(OSError("locked")))

    assert app._hold_label_recovery(manager_code="admin") is False
    assert journal.path.read_bytes() == original
    assert app._active_label_recovery_state()["exchange_id"] == "EXCHANGE-A"
    assert app.package_outbox.get_label_exchange_hold(digest) is not None
    assert app._begin_phs_reconciliation_lookup("PHS2-SOURCE-A") is False

    monkeypatch.setattr(app_module.os, "replace", original_replace)
    assert app._finalize_label_recovery_holds() is True
    assert app._active_label_recovery_state() == {}


def test_f5_crash_before_linked_set_hold_keeps_active_journal(tmp_path, monkeypatch):
    state = {"workflow_mode": "SINGLE", "status": "PREPARED",
             "set_id": "SET-A", "canonical_input_tag_qr": "PHS2-SOURCE-A",
             "source_label_id": "LABEL-A", "prepare_idempotency_key": "PREPARE-A"}
    app, journal = _recovery_app(tmp_path, monkeypatch, state)
    raw = journal.path.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    archive = journal.path.with_name(journal.path.name + ".held-" + digest)
    app.package_outbox.hold_label_exchange(
        hold_id=digest, set_id="SET-A", label_id="LABEL-A",
        source_label="PHS2-SOURCE-A", source_input_tag_id="",
        journal_bytes=raw, archive_path=str(archive),
        held_by="protected-admin-local",
    )

    assert app._finalize_label_recovery_holds() is True
    assert journal.path.read_bytes() == raw
    assert not archive.exists()
    assert app._hold_label_recovery(manager_code="admin") is False
    assert journal.path.read_bytes() == raw


def test_f5_missing_source_identity_cannot_release_global_lock(tmp_path, monkeypatch):
    state = {"workflow_mode": "RECONCILIATION", "status": "PREPARED",
             "prepare_idempotency_key": "PREPARE-A", "exchange_id": "EXCHANGE-A"}
    app, journal = _recovery_app(tmp_path, monkeypatch, state)
    assert app._hold_label_recovery(manager_code="admin") is False
    assert journal.path.exists()
    assert app.package_outbox.list_label_exchange_holds() == []


def test_catalog_retry_uses_verified_startup_path_without_restart(monkeypatch):
    attempts = []
    error = app_module.ItemCatalogSyncError(
        "unavailable", cause_code=app_module.SNAPSHOT_UNAVAILABLE_AFTER_VERIFY
    )

    def prepare():
        attempts.append(True)
        if len(attempts) == 1:
            raise error
        return None

    constructed = []

    class FakeApp:
        def __init__(self):
            constructed.append(True)

        def title(self):
            return "Label Match"

        def state(self):
            return "normal"

        def mainloop(self):
            return None

    monkeypatch.setattr(app_module, "prepare_startup_item_catalog", prepare)
    monkeypatch.setattr(app_module, "Label_Match", FakeApp)
    monkeypatch.setattr(app_module, "_label_match_startup_trace", lambda *args, **kwargs: None)
    monkeypatch.setattr(app_module, "_offer_item_catalog_startup_retry", lambda exc: exc is error)
    assert app_module._run_label_match_application() == 0
    assert len(attempts) == 2 and constructed == [True]


def test_catalog_retry_cancel_stays_closed(monkeypatch):
    error = app_module.ItemCatalogSyncError(
        "unavailable", cause_code=app_module.SNAPSHOT_UNAVAILABLE_AFTER_VERIFY
    )
    monkeypatch.setattr(app_module, "prepare_startup_item_catalog", lambda: (_ for _ in ()).throw(error))
    monkeypatch.setattr(app_module, "_label_match_startup_trace", lambda *args, **kwargs: None)
    monkeypatch.setattr(app_module, "_offer_item_catalog_startup_retry", lambda exc: False)
    monkeypatch.setattr(app_module, "Label_Match", lambda: pytest.fail("unverified app constructed"))
    with pytest.raises(app_module.ItemCatalogSyncError):
        app_module._run_label_match_application()

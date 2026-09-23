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
    monkeypatch.setattr(app_module, "_current_user_sid", lambda: "S-1-5-21-101")
    journal = PHSLabelExchangeJournal(tmp_path / "label-exchange.json")
    journal.save(state)
    app = object.__new__(app_module.Label_Match)
    app.run_tests = True
    app.worker_name = app_module.PROTECTED_ADMIN_OPERATOR_ID
    app.worker_role = app_module.PROTECTED_ADMIN_ROLE
    app._authenticated_protected_admin = True
    app.current_set_info = {"id": None, "raw": []}
    app.package_outbox = app_module.PackageOutbox(tmp_path / "outbox.sqlite3")
    app.sealed_transfer_exchange_store = SimpleNamespace(blocking_rows=lambda **_kwargs: [])
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

    assert app._hold_label_recovery(digest, manager_code="wrong") is False
    assert app.package_outbox.get_label_exchange_hold(digest) is None
    assert journal.path.read_bytes() == original

    assert app._hold_label_recovery(digest, manager_code="admin") is True
    held = app.package_outbox.get_label_exchange_hold(digest)
    assert bytes(held["journal_bytes"]) == original
    assert held["held_by"] == "S-1-5-21-101"
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

    assert app._hold_label_recovery(digest, manager_code="admin") is False
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
    assert app._hold_label_recovery(digest, manager_code="admin") is False
    assert journal.path.read_bytes() == raw


def test_f5_missing_source_identity_cannot_release_global_lock(tmp_path, monkeypatch):
    state = {"workflow_mode": "RECONCILIATION", "status": "PREPARED",
             "prepare_idempotency_key": "PREPARE-A", "exchange_id": "EXCHANGE-A"}
    app, journal = _recovery_app(tmp_path, monkeypatch, state)
    assert app._hold_label_recovery(app._label_recovery_hold_id(), manager_code="admin") is False
    assert journal.path.exists()
    assert app.package_outbox.list_label_exchange_holds() == []


def test_f5_selected_held_item_cannot_hold_a_different_active_journal(tmp_path, monkeypatch):
    old = {"workflow_mode": "RECONCILIATION", "status": "PREPARED",
           "scan_payload": "PHS2-SOURCE-OLD", "active_scan_label_id": "LABEL-OLD"}
    app, journal = _recovery_app(tmp_path, monkeypatch, old)
    old_bytes = journal.path.read_bytes()
    old_id = hashlib.sha256(old_bytes).hexdigest()
    app.package_outbox.hold_label_exchange(
        hold_id=old_id, set_id="", label_id="LABEL-OLD",
        source_label="PHS2-SOURCE-OLD", source_input_tag_id="",
        journal_bytes=old_bytes, archive_path=str(journal.path) + ".held-" + old_id,
        held_by="S-1-5-21-101",
    )
    journal.save({"workflow_mode": "RECONCILIATION", "status": "PREPARED",
                  "scan_payload": "PHS2-SOURCE-NEW", "active_scan_label_id": "LABEL-NEW"})
    new_bytes = journal.path.read_bytes()
    new_id = hashlib.sha256(new_bytes).hexdigest()
    assert {item["set_id"] for item in app._package_recovery_candidates()} == {
        "F5:" + old_id, "F5:" + new_id,
    }
    assert app._hold_label_recovery(old_id, manager_code="admin") is False
    assert journal.path.read_bytes() == new_bytes
    assert app.package_outbox.get_label_exchange_hold(new_id) is None
    assert app._hold_label_recovery(new_id, manager_code="admin") is True


def test_f5_workbench_button_passes_selected_hold_id(tmp_path, monkeypatch):
    app, _journal = _recovery_app(tmp_path, monkeypatch, {})
    app.run_tests = False
    app._package_recovery_candidates = lambda: [
        {"set_id": "F5:old", "held": {"hold_id": "old"}, "label": {}},
        {"set_id": "F5:new", "held": None, "label": {}},
    ]
    passed = []
    app._hold_label_recovery = lambda hold_id: passed.append(hold_id) or False
    widgets = []

    class Widget:
        def __init__(self, *_args, **options):
            self.options = options
            widgets.append(self)

        def title(self, _value):
            pass

        def pack(self, **_options):
            pass

        def delete(self, *_args):
            pass

        def insert(self, *_args):
            pass

        def curselection(self):
            return (0,)

    class TextValue:
        def __init__(self, value=""):
            self.value = value

        def set(self, value):
            self.value = value

    for toolkit, names in ((app_module.tk, ("Toplevel", "Listbox")),
                           (app_module.ttk, ("Frame", "Label", "Button"))):
        for name in names:
            monkeypatch.setattr(toolkit, name, Widget)
    monkeypatch.setattr(app_module.tk, "StringVar", TextValue)
    app._show_package_recovery_workbench()
    next(item for item in widgets if item.options.get("text") == "보류 후 계속").options["command"]()
    assert passed == ["old"]


@pytest.mark.parametrize("draft_json,expected", [
    ("{bad-json", False),
    (json.dumps({"source_canonical_input_tag_qr": "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-HOLD",
                 "source_input_tag_id": "ITG-HOLD"}), False),
    (json.dumps({"source_canonical_input_tag_qr":
        "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-HOLD|CLC=AAA2270730100|"
        "LBL=LBL-HOLD|HSH=0123456789abcdef", "source_input_tag_id": "ITG-HOLD"}), True),
])
def test_orphan_hold_requires_verified_source_identity(tmp_path, monkeypatch, draft_json, expected):
    app, _journal = _recovery_app(tmp_path, monkeypatch, {})
    source = ("PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-HOLD|CLC=AAA2270730100|"
              "LBL=LBL-HOLD|HSH=0123456789abcdef")
    app._package_recovery_candidates = lambda: [{
        "set_id": "SET-CORRUPT", "current": None,
        "package": {"idempotency_key": "KEY-CORRUPT", "draft_json": draft_json},
        "exchange": None,
    }]
    app._load_current_set_state = lambda: None
    messages = []
    app._show_package_recovery_block = lambda message, **_kwargs: messages.append(message)
    assert app._hold_package_recovery_set("SET-CORRUPT", manager_code="admin") is expected
    hold = app.package_outbox.get_workbench_hold("SET-CORRUPT")
    if expected:
        assert hold["source_input_tag_id"] == "ITG-HOLD"
        assert app.package_outbox.workbench_hold_for_source(source, "ITG-HOLD") is not None
        assert messages == []
    else:
        assert hold is None
        assert app.current_set_info["id"] is None
        assert any("현품표" in message and "작업대를 비울 수 없습니다" in message for message in messages)


def test_recovery_code_alone_does_not_authorize_operator_session(tmp_path, monkeypatch):
    app, journal = _recovery_app(tmp_path, monkeypatch, {
        "workflow_mode": "RECONCILIATION", "status": "PREPARED",
        "scan_payload": "PHS2-SOURCE-A", "active_scan_label_id": "LABEL-A",
    })
    digest = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    app.worker_name = "ordinary-operator"
    app.worker_role = "PACKAGING"
    app._authenticated_protected_admin = False
    assert app._hold_label_recovery(digest, manager_code="admin") is False
    assert app._recheck_package_recovery_set("F5:" + digest, manager_code="admin") == "관리자 확인이 필요합니다."
    assert app.package_outbox.get_label_exchange_hold(digest) is None
    assert journal.path.exists()


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

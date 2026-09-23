"""Focused journal hold and trusted catalog retry regressions."""

import hashlib
import json
import random
import base64
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


def _storage_recovery_app(tmp_path, monkeypatch):
    app, journal = _recovery_app(tmp_path, monkeypatch, {})
    app._show_package_recovery_block = (
        lambda message, **kwargs: app_module.Label_Match._show_package_recovery_block(
            app, message, **kwargs
        )
    )
    current = tmp_path / "current.json"
    app.data_manager = SimpleNamespace(
        save_directory=str(tmp_path), _current_state_filename=lambda: "current.json",
        load_current_state=lambda: json.loads(current.read_text(encoding="utf-8")) if current.exists() else None,
        delete_current_state=lambda: current.unlink(missing_ok=True),
    )
    return app, journal, current


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


def test_f5_archive_failure_blocks_same_label_only(tmp_path, monkeypatch):
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
    assert app.__dict__.get("_workflow_blocking_notice") is None

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


def test_f5_missing_source_identity_is_held_without_losing_journal(tmp_path, monkeypatch):
    state = {"workflow_mode": "RECONCILIATION", "status": "PREPARED",
             "prepare_idempotency_key": "PREPARE-A", "exchange_id": "EXCHANGE-A"}
    app, journal = _recovery_app(tmp_path, monkeypatch, state)
    original = journal.path.read_bytes()
    digest = app._label_recovery_hold_id()
    assert app._hold_label_recovery(digest, manager_code="admin") is True
    assert not journal.path.exists()
    assert app.package_outbox.get_label_exchange_hold(digest)["journal_bytes"] == original
    assert (tmp_path / f"label-exchange.json.held-{digest}").read_bytes() == original
    assert app._label_recovery_source_is_held("PHS2-OTHER") is True
    assert app.__dict__.get("_workflow_blocking_notice") is None


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
def test_orphan_hold_quarantines_unverified_source_identity(tmp_path, monkeypatch, draft_json, expected):
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
    assert app._hold_package_recovery_set("SET-CORRUPT", manager_code="admin") is True
    hold = app.package_outbox.get_workbench_hold("SET-CORRUPT")
    if expected:
        assert hold["source_input_tag_id"] == "ITG-HOLD"
        assert app.package_outbox.workbench_hold_for_source(source, "ITG-HOLD") is not None
        assert messages == []
    else:
        assert hold is not None
        assert hold["source_phs2"] == "" and hold["source_input_tag_id"] == ""
        assert "신원 미확인" in hold["reason"]
        assert app.current_set_info["id"] is None
        assert messages == []


def test_recovery_dialog_authenticates_manager_without_changing_active_operator(tmp_path, monkeypatch):
    app, journal = _recovery_app(tmp_path, monkeypatch, {
        "workflow_mode": "RECONCILIATION", "status": "PREPARED",
        "scan_payload": "PHS2-SOURCE-A", "active_scan_label_id": "LABEL-A",
    })
    digest = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    app.worker_name = "ordinary-operator"
    app.worker_role = "PACKAGING"
    app._authenticated_protected_admin = False
    assert app._hold_label_recovery(digest, manager_code="operator") is False
    assert app._recheck_package_recovery_set("F5:" + digest, manager_code="operator") == "관리자 확인이 필요합니다."
    assert app._hold_label_recovery(digest, manager_code="admin") is True
    assert app.package_outbox.get_label_exchange_hold(digest)["held_by"] == "S-1-5-21-101"
    assert (app.worker_name, app.worker_role, app._authenticated_protected_admin) == (
        "ordinary-operator", "PACKAGING", False,
    )
    assert "보류" in app._recheck_package_recovery_set("F5:" + digest, manager_code="admin")
    with app.package_outbox._connect() as conn:
        actions = conn.execute(
            "SELECT action,manager_id FROM package_workbench_hold_audit ORDER BY audit_id"
        ).fetchall()
    assert [(row["action"], row["manager_id"]) for row in actions] == [
        ("HOLD_LABEL", "S-1-5-21-101"), ("RECHECK", "S-1-5-21-101"),
    ]


def test_live_recovery_dialog_asks_code_while_operator_has_active_set(tmp_path, monkeypatch):
    app, _journal = _recovery_app(tmp_path, monkeypatch, {})
    app.run_tests = False
    app.worker_name = "ordinary-operator"
    app.worker_role = "PACKAGING"
    app._authenticated_protected_admin = False
    app.current_set_info["id"] = "ACTIVE-SET"
    asked = []
    monkeypatch.setattr(app_module.simpledialog, "askstring", lambda *args, **kwargs: (
        asked.append(kwargs) or "admin"
    ))
    assert app._package_recovery_manager() == "S-1-5-21-101"
    assert asked[0]["show"] == "*" and asked[0]["parent"] is app
    assert (app.worker_name, app.worker_role, app._authenticated_protected_admin) == (
        "ordinary-operator", "PACKAGING", False,
    )


@pytest.mark.parametrize("draft_json", [
    "{bad-json", "[]", '{"membership_mode":[]}',
    '{"membership_mode":"INHERIT_ALL","source_input_tag_id":{}}',
    "".join(chr(random.Random(712 + index).randrange(32, 127)) for index in range(40)),
])
def test_corrupt_orphan_draft_is_quarantined_per_row(tmp_path, monkeypatch, draft_json):
    app, _journal, _current = _storage_recovery_app(tmp_path, monkeypatch)
    with app.package_outbox._connect() as conn:
        conn.execute(
            """INSERT INTO package_command_outbox
               (idempotency_key,set_id,command_fingerprint,draft_json,status,created_at,updated_at)
               VALUES (?,?,?,?,?,?,?)""",
            ("KEY-BROKEN", "SET-BROKEN", "FINGERPRINT", draft_json,
             "PENDING", "2026-09-23T00:00:00Z", "2026-09-23T00:00:00Z"),
        )
        conn.commit()
    app._load_current_set_state()
    assert app._workflow_blocking_notice.kind == "submission_blocked"
    assert app._hold_package_recovery_set("SET-BROKEN", manager_code="admin") is True
    hold = app.package_outbox.get_workbench_hold("SET-BROKEN")
    assert hold["source_phs2"] == "" and "신원 미확인" in hold["reason"]
    assert app.package_outbox.get_by_set_id("SET-BROKEN")["draft_json"] == draft_json
    assert app.package_outbox.list_local_completion_pending() == []
    assert app.package_outbox.workbench_hold_for_source("PHS2-OTHER", "ITG-OTHER") is None
    with app.package_outbox._connect() as conn:
        conn.execute(
            """INSERT INTO package_command_outbox
               (idempotency_key,set_id,command_fingerprint,draft_json,status,
                local_completion_committed,created_at,updated_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            ("KEY-GOOD", "SET-GOOD", "FINGERPRINT-GOOD", "{}", "PENDING", 1,
             "2026-09-23T00:00:01Z", "2026-09-23T00:00:01Z"),
        )
        conn.commit()
    assert app.package_outbox.claim_next()["set_id"] == "SET-GOOD"
    assert app.__dict__.get("_workflow_blocking_notice") is None


@pytest.mark.parametrize("damaged", [
    b"\x00\xff", b'{"schema_version":',
    b'{"schema_version":"label-match-phs-label-exchange-v1","state":[]}',
    b'{"schema_version":"label-match-phs-label-exchange-v1","state":{}}',
])
def test_damaged_f5_archive_remains_item_hold_without_startup_block(tmp_path, monkeypatch, damaged):
    app, journal, _current = _storage_recovery_app(tmp_path, monkeypatch)
    journal.save({"status": "PREPARED", "scan_payload": "PHS2-SOURCE-A",
                  "active_scan_label_id": "LABEL-A", "exchange_id": "EXCHANGE-A"})
    digest = app._label_recovery_hold_id()
    assert app._hold_label_recovery(digest, manager_code="admin") is True
    archive = tmp_path / f"label-exchange.json.held-{digest}"
    archive.write_bytes(damaged)
    app._load_current_set_state()
    assert archive.read_bytes() == damaged
    assert "F5:" + digest in app._package_recovery_file_issues
    assert app.__dict__.get("_workflow_blocking_notice") is None
    assert "보류" in app._recheck_package_recovery_set("F5:" + digest, manager_code="admin")
    assert app.package_outbox.get_label_exchange_hold(digest) is not None


@pytest.mark.parametrize("damaged", [
    b"", b"\x00\xff", b'{"schema_version":',
    b'{"schema_version":"label-match-phs-label-exchange-v1","state":null}',
])
def test_corrupt_active_f5_journal_can_be_quarantined_by_digest(tmp_path, monkeypatch, damaged):
    app, journal, _current = _storage_recovery_app(tmp_path, monkeypatch)
    journal.path.write_bytes(damaged)
    digest = hashlib.sha256(damaged).hexdigest()
    assert "F5:" + digest in {item["set_id"] for item in app._package_recovery_candidates()}
    assert app._hold_label_recovery(digest, manager_code="admin") is True
    assert not journal.path.exists()
    assert (tmp_path / f"label-exchange.json.held-{digest}").read_bytes() == damaged
    app._load_current_set_state()
    assert app.__dict__.get("_workflow_blocking_notice") is None
    assert app._label_recovery_source_is_held("PHS2-OTHER") is True


@pytest.mark.parametrize("damaged", [
    b'{"current_set_info":{"id":[]},',
    b'{"current_set_info":{"id":[],"raw":[]}}',
    b'{"current_set_info":{"id":"A","raw":[{}]}}',
    b'{"current_set_info":null}',
])
def test_corrupt_current_file_is_archived_without_changing_bytes(tmp_path, monkeypatch, damaged):
    app, _journal, current = _storage_recovery_app(tmp_path, monkeypatch)
    current.write_bytes(damaged)
    digest = hashlib.sha256(damaged).hexdigest()
    app._load_current_set_state()
    assert app._workflow_blocking_notice.kind == "submission_blocked"
    assert app._hold_package_recovery_set("CURRENT:" + digest, manager_code="admin") is True
    assert not current.exists()
    assert (tmp_path / f"current.json.held-{digest}").read_bytes() == damaged
    assert app.__dict__.get("_workflow_blocking_notice") is None
    assert app.package_outbox.get_workbench_hold("CURRENT:" + digest)["source_phs2"] == ""
    snapshot = json.loads(app.package_outbox.get_workbench_hold("CURRENT:" + digest)["snapshot_json"])
    assert base64.b64decode(snapshot["raw_base64"]) == damaged


def test_validated_current_bytes_are_used_without_a_second_file_read(tmp_path):
    manager = object.__new__(app_module.DataManager)
    manager.save_directory = str(tmp_path)
    manager.worker_name = "ordinary-operator"
    manager._current_state_filename = lambda: "missing-current.json"
    manager._open_file = lambda *_args, **_kwargs: pytest.fail("unexpected file read")
    raw = b'{"worker_name":"ordinary-operator","current_set_info":{"id":"SET-A","raw":[]}}'
    assert manager.load_current_state(verified_bytes=raw)["current_set_info"]["id"] == "SET-A"


@pytest.mark.parametrize("linked", [True, False])
def test_manager_physical_scan_only_binds_matching_saved_command(tmp_path, monkeypatch, linked):
    app, _journal, _current = _storage_recovery_app(tmp_path, monkeypatch)
    source = ("PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-RECOVERY|CLC=AAA2270730100|"
              "LBL=LBL-RECOVERY|HSH=0123456789abcdef")
    command = {
        "command_type": "CREATE_PACKAGE", "idempotency_key": "KEY-RECOVERY",
        "payload": {"source_bundle_id": "SOURCE-A" if linked else "SOURCE-B"},
    }
    with app.package_outbox._connect() as conn:
        conn.execute(
            """INSERT INTO package_command_outbox
               (idempotency_key,set_id,command_fingerprint,draft_json,command_json,
                status,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)""",
            ("KEY-RECOVERY", "SET-RECOVERY", "FINGERPRINT", "{broken",
             json.dumps(command), "PENDING", "2026-09-23T00:00:00Z",
             "2026-09-23T00:00:00Z"),
        )
        conn.commit()
    app.package_outbox.hold_workbench_set(
        set_id="SET-RECOVERY", source_phs2="", source_input_tag_id="",
        snapshot={"package_key": "KEY-RECOVERY"}, reason="신원 미확인 보류",
        held_by="S-1-5-21-101",
    )
    app._resolve_central_phs2_scan_overlay = lambda *_args, **_kwargs: (
        None, {"bundle_id": "SOURCE-A"}, None, None,
    )
    result = app._recheck_package_recovery_physical(
        "SET-RECOVERY", source, manager_code="admin"
    )
    held = app.package_outbox.get_workbench_hold("SET-RECOVERY")
    if linked:
        assert "일치" in result
        assert held["source_input_tag_id"] == "ITG-RECOVERY"
        assert app.package_outbox.workbench_hold_for_source(source, "ITG-RECOVERY") is not None
    else:
        assert "연결되지" in result
        assert held["source_phs2"] == "" and held["source_input_tag_id"] == ""
    assert app._recheck_package_recovery_physical(
        "SET-RECOVERY", source, manager_code="operator"
    ) == "관리자 확인이 필요합니다."


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

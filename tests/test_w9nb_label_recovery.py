"""Focused journal hold and trusted catalog retry regressions."""

import hashlib
import json
import random
import base64
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

import Label_Match as app_module
from phs_label_workflow import PHSLabelExchangeJournal


SOURCE_A = ("PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-A|CLC=ITEM|"
            "LBL=LABEL-A|HSH=aaaaaaaaaaaaaaaa")
SOURCE_B = ("PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-B|CLC=ITEM|"
            "LBL=LABEL-B|HSH=bbbbbbbbbbbbbbbb")
SOURCE_OLD = ("PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-OLD|CLC=ITEM|"
              "LBL=LABEL-OLD|HSH=cccccccccccccccc")
SOURCE_NEW = ("PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-NEW|CLC=ITEM|"
              "LBL=LABEL-NEW|HSH=dddddddddddddddd")
SOURCE_OTHER = ("PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-OTHER|CLC=ITEM|"
                "LBL=LABEL-OTHER|HSH=eeeeeeeeeeeeeeee")


def _operation_key(key, scope="SCOPE-F5"):
    return hashlib.sha256(json.dumps({
        "contract_version": "phs-work-control-v1",
        "authority_scope_id": scope, "idempotency_key": key,
        "command": "PREPARE_LABEL_EXCHANGE",
    }, ensure_ascii=False, sort_keys=True,
        separators=(",", ":")).encode("utf-8")).hexdigest()


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
    # These legacy tests exercise the existing hold/readback writers directly;
    # PIN authorization and its audit binding have separate focused coverage.
    app._package_recovery_manager = lambda code=None: "S-1-5-21-101" if code == "admin" else ""
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
        "set_id": "", "scan_payload": SOURCE_A,
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
    assert app._label_recovery_source_is_held(SOURCE_A) is False
    assert app._label_recovery_source_is_held(SOURCE_B) is False
    assert app._label_recovery_source_is_held(SOURCE_B, "LABEL-A") is False
    assert app._recheck_package_recovery_set("F5:" + digest, manager_code="wrong") == "관리자 확인이 필요합니다."
    assert "보류" in app._recheck_package_recovery_set("F5:" + digest, manager_code="admin")
    assert json.loads(bytes(held["journal_bytes"]))["state"]["prepare_idempotency_key"] == "PREPARE-A"


def test_f5_crash_after_hold_readback_finishes_archive(tmp_path, monkeypatch):
    state = {"workflow_mode": "RECONCILIATION", "status": "PREPARED",
             "scan_payload": SOURCE_A, "active_scan_label_id": "LABEL-A",
             "prepare_idempotency_key": "PREPARE-A", "exchange_id": "EXCHANGE-A"}
    app, journal = _recovery_app(tmp_path, monkeypatch, state)
    original = journal.path.read_bytes()
    digest = hashlib.sha256(original).hexdigest()
    archive = journal.path.with_name(journal.path.name + ".held-" + digest)
    app.package_outbox.hold_label_exchange(
        hold_id=digest, set_id="", label_id="LABEL-A",
        source_label=SOURCE_A, source_input_tag_id="",
        journal_bytes=original, archive_path=str(archive),
        held_by="protected-admin-local",
    )

    assert app._finalize_label_recovery_holds() is True
    assert archive.read_bytes() == original
    assert not journal.path.exists()
    assert app._active_label_recovery_state() == {}


def test_f5_archive_failure_blocks_same_label_only(tmp_path, monkeypatch):
    state = {"workflow_mode": "RECONCILIATION", "status": "PREPARED",
             "scan_payload": SOURCE_A, "active_scan_label_id": "LABEL-A",
             "prepare_idempotency_key": "PREPARE-A", "exchange_id": "EXCHANGE-A"}
    app, journal = _recovery_app(tmp_path, monkeypatch, state)
    original = journal.path.read_bytes()
    digest = hashlib.sha256(original).hexdigest()
    original_replace = app_module.replace_checked
    monkeypatch.setattr(app_module, "replace_checked", lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("locked")))

    assert app._hold_label_recovery(digest, manager_code="admin") is False
    assert journal.path.read_bytes() == original
    assert app._active_label_recovery_state()["exchange_id"] == "EXCHANGE-A"
    assert app.package_outbox.get_label_exchange_hold(digest) is not None
    assert app._label_recovery_source_is_held(SOURCE_A) is False
    assert app.__dict__.get("_workflow_blocking_notice") is None

    monkeypatch.setattr(app_module, "replace_checked", original_replace)
    assert app._finalize_label_recovery_holds() is True
    assert app._active_label_recovery_state() == {}


def test_f5_crash_before_linked_set_hold_keeps_active_journal(tmp_path, monkeypatch):
    state = {"workflow_mode": "SINGLE", "status": "PREPARED",
             "set_id": "SET-A", "canonical_input_tag_qr": SOURCE_A,
             "source_label_id": "LABEL-A", "prepare_idempotency_key": "PREPARE-A"}
    app, journal = _recovery_app(tmp_path, monkeypatch, state)
    raw = journal.path.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    archive = journal.path.with_name(journal.path.name + ".held-" + digest)
    app.package_outbox.hold_label_exchange(
        hold_id=digest, set_id="SET-A", label_id="LABEL-A",
        source_label=SOURCE_A, source_input_tag_id="",
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
    assert app._label_recovery_source_is_held(SOURCE_OTHER) is False
    assert app.__dict__.get("_workflow_blocking_notice") is None


def test_f5_selected_held_item_cannot_hold_a_different_active_journal(tmp_path, monkeypatch):
    old = {"workflow_mode": "RECONCILIATION", "status": "PREPARED",
           "scan_payload": SOURCE_OLD, "active_scan_label_id": "LABEL-OLD"}
    app, journal = _recovery_app(tmp_path, monkeypatch, old)
    old_bytes = journal.path.read_bytes()
    old_id = hashlib.sha256(old_bytes).hexdigest()
    app.package_outbox.hold_label_exchange(
        hold_id=old_id, set_id="", label_id="LABEL-OLD",
        source_label=SOURCE_OLD, source_input_tag_id="",
        journal_bytes=old_bytes, archive_path=str(journal.path) + ".held-" + old_id,
        held_by="S-1-5-21-101",
    )
    journal.save({"workflow_mode": "RECONCILIATION", "status": "PREPARED",
                  "scan_payload": SOURCE_NEW, "active_scan_label_id": "LABEL-NEW"})
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
        {"set_id": "F5:old", "held": {"hold_id": "old"}, "label": {},
         "pin_intent": {"action": "LABEL.F5_HOLD"},
         "pin_intents": [{"action": "LABEL.F5_HOLD"}]},
        {"set_id": "F5:new", "held": None, "label": {}},
    ]
    passed = []
    app._admin_pin_last_message = "관리자 확인 실패"
    app._run_package_pin_action = lambda action, set_id: passed.append((action, set_id)) or "관리자 확인 실패"
    app._park_unverified_pin_item = lambda set_id: passed.append(("PARK", set_id)) or True
    widgets = []
    values = []

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
            values.append(self)

        def set(self, value):
            self.value = value

    for toolkit, names in ((app_module.tk, ("Toplevel", "Listbox")),
                           (app_module.ttk, ("Frame", "Label", "Button"))):
        for name in names:
            monkeypatch.setattr(toolkit, name, Widget)
    monkeypatch.setattr(app_module.tk, "StringVar", TextValue)
    app._show_package_recovery_workbench()
    next(item for item in widgets if item.options.get("text") == "보류 후 계속").options["command"]()
    assert any(value.value == "관리자 확인 실패" for value in values)
    assert not any("다른 세트 작업을 계속" in value.value for value in values)
    next(item for item in widgets if item.options.get("text") == "관리자 확인 다시 조회").options["command"]()
    next(item for item in widgets if item.options.get("text") == "이 건만 보류하고 다른 작업 계속").options["command"]()
    assert passed == [("LABEL.F5_HOLD", "F5:old"),
                      ("LABEL.F5_HOLD", "F5:old"), ("PARK", "F5:old")]


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
        "scan_payload": SOURCE_A, "active_scan_label_id": "LABEL-A",
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


def test_legacy_code_and_sid_cannot_authorize_recovery(tmp_path, monkeypatch):
    app, _journal = _recovery_app(tmp_path, monkeypatch, {})
    del app._package_recovery_manager
    app.worker_name = "ordinary-operator"
    app.worker_role = "PACKAGING"
    app._authenticated_protected_admin = False
    app.current_set_info["id"] = "ACTIVE-SET"
    assert app._package_recovery_manager("admin") == ""
    token = app_module.active_pin_operation.set((
        "operation-key", "verification-id", "LABEL.RECHECK", "ACTIVE-SET",
        "ordinary-operator", "admin-personal-id", "ordinary-operator",
    ))
    try:
        assert app._package_recovery_manager("admin") == "admin-personal-id"
    finally:
        app_module.active_pin_operation.reset(token)
    assert (app.worker_name, app.worker_role, app._authenticated_protected_admin) == (
        "ordinary-operator", "PACKAGING", False,
    )


@pytest.mark.parametrize("draft_json", [
    "{bad-json", "[]", '{"membership_mode":[]}',
    '{"membership_mode":"INHERIT_ALL","source_input_tag_id":{}}',
    pytest.param("[" * 3000 + "]" * 3000, id="deep-json"),
    pytest.param('{"membership_mode":"INHERIT_ALL","padding":"' + "X" * 1_000_000 + '"}', id="large-json"),
    "\x00invalid",
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
    assert app.package_outbox.workbench_hold_for_source(SOURCE_OTHER, "ITG-OTHER") is None
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
    pytest.param(b"[" * 3000 + b"]" * 3000, id="deep-json"),
    b"\x00invalid", b"\xff\xfe",
    b'{"schema_version":"label-match-phs-label-exchange-v1","state":[]}',
    b'{"schema_version":"label-match-phs-label-exchange-v1","state":{}}',
])
def test_damaged_f5_archive_remains_item_hold_without_startup_block(tmp_path, monkeypatch, damaged):
    app, journal, _current = _storage_recovery_app(tmp_path, monkeypatch)
    journal.save({"status": "PREPARED", "scan_payload": SOURCE_A,
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


def test_f5_archive_observations_append_without_replacing_earlier_audit(tmp_path, monkeypatch):
    app, journal, _current = _storage_recovery_app(tmp_path, monkeypatch)
    journal.save({"status": "PREPARED", "scan_payload": SOURCE_A})
    original = journal.path.read_bytes()
    digest = hashlib.sha256(original).hexdigest()
    assert app._hold_label_recovery(digest, manager_code="admin") is True
    archive = tmp_path / f"label-exchange.json.held-{digest}"
    archive.unlink()
    assert app._finalize_label_recovery_holds() is True
    assert archive.read_bytes() == original
    damaged = b"\x00new-corruption"
    archive.write_bytes(damaged)
    assert app._finalize_label_recovery_holds() is True
    assert archive.read_bytes() == damaged
    with app.package_outbox._connect() as conn:
        rows = conn.execute(
            "SELECT action FROM package_workbench_hold_audit WHERE set_id=? ORDER BY audit_id",
            ("F5:" + digest,),
        ).fetchall()
    assert [row["action"] for row in rows] == [
        "HOLD_LABEL", "FILE_MISSING", "FILE_UNVERIFIED",
    ]


@pytest.mark.parametrize("failure", [RecursionError(), ValueError("embedded null character")])
def test_manager_recovery_actions_contain_per_item_read_exceptions(tmp_path, monkeypatch, failure):
    app, journal = _recovery_app(tmp_path, monkeypatch, {})
    app._package_recovery_candidates = lambda: (_ for _ in ()).throw(failure)
    assert app._hold_package_recovery_set("SET-A", manager_code="admin") is False
    digest = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    app.package_outbox.get_label_exchange_hold = lambda _hold_id: (_ for _ in ()).throw(failure)
    assert "보류" in app._recheck_package_recovery_set("F5:" + digest, manager_code="admin")


def test_missing_pin_context_keeps_recovery_closed(tmp_path, monkeypatch):
    app, _journal = _recovery_app(tmp_path, monkeypatch, {})
    del app._package_recovery_manager
    assert app._hold_package_recovery_set("SET-A", manager_code="admin") is False
    assert app._recheck_package_recovery_set("SET-A", manager_code="admin") == "관리자 확인이 필요합니다."


@pytest.mark.parametrize("damaged", [
    b"", b"\x00\xff", b'{"schema_version":',
    pytest.param(b"[" * 3000 + b"]" * 3000, id="deep-json"),
    b"\x00invalid", b"\xff\xfe",
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
    assert app._label_recovery_source_is_held(SOURCE_OTHER) is False
    assert "보류" in app._recheck_package_recovery_set(
        "F5:" + digest, manager_code="admin"
    )


@pytest.mark.parametrize("damaged", [
    b'{"current_set_info":{"id":[]},',
    pytest.param(b"[" * 3000 + b"]" * 3000, id="deep-json"),
    b"\x00invalid", b"\xff\xfe",
    pytest.param(b"\x00" * (8 * 1024 * 1024 + 1), id="oversized-nul"),
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


@pytest.mark.parametrize("central_matches,audit_succeeds,key_matches", [
    (True, True, True), (False, True, True), (True, False, True),
    (True, True, False),
])
def test_unknown_f5_physical_central_binding_is_audited_before_unlock(
    tmp_path, monkeypatch, central_matches, audit_succeeds, key_matches,
):
    source = ("PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-F5|CLC=AAA2270730100|"
              "LBL=LBL-F5|HSH=0123456789abcdef")
    app, journal = _recovery_app(tmp_path, monkeypatch, {
        "workflow_mode": "SINGLE", "status": "PREPARED", "set_id": "",
        "exchange_id": "EX-F5", "authority_scope_id": "SCOPE-F5",
        "prepare_idempotency_key": "KEY-F5",
    })
    digest = app._label_recovery_hold_id()
    assert app._hold_label_recovery(digest, manager_code="admin") is True
    original = app.package_outbox.get_label_exchange_hold(digest)["journal_bytes"]
    app.phs_label_exchange_coordinator.client = SimpleNamespace(
        config=SimpleNamespace(authority_scope_id="SCOPE-F5"),
        resolve_active_phs_label=lambda *_args, **_kwargs: {
            "input_tag": {"qr_payload": source},
        },
        get_phs_label_exchange=lambda *_args, **_kwargs: {
            "exchange": {"exchange_id": "EX-F5", "state": "PREPARED",
                         "operation_key": _operation_key(
                             "KEY-F5" if key_matches else "KEY-OTHER")},
            "source_labels": [{
                "qr_payload": source, "label_id": "LBL-F5",
                "scan_anchor_input_tag_id": "ITG-F5" if central_matches else "ITG-OTHER",
            }],
        },
    )
    if not audit_succeeds:
        monkeypatch.setattr(app.package_outbox, "audit_workbench_action",
                            lambda **_kwargs: (_ for _ in ()).throw(OSError("audit disk")))
    result = app._recheck_package_recovery_physical(
        "F5:" + digest, source, manager_code="admin"
    )
    held = app.package_outbox.get_label_exchange_hold(digest)
    assert held["journal_bytes"] == original
    assert journal.path.with_name(journal.path.name + ".held-" + digest).read_bytes() == original
    if central_matches and audit_succeeds and key_matches:
        assert "다른 현품표" in result
        assert held["source_input_tag_id"] == "ITG-F5"
        assert app._label_recovery_source_is_held(source) is True
        assert app._label_recovery_source_is_held(SOURCE_OTHER) is False
        assert app.package_outbox.has_workbench_audit(
            set_id="F5:" + digest, action="BIND_LABEL_SOURCE",
            observed="CENTRAL_PHYSICAL_MATCH",
        )
    else:
        assert held["source_input_tag_id"] == ""
        assert app._label_recovery_source_is_held(SOURCE_OTHER) is False


def test_unknown_multi_source_f5_binds_all_central_sources(tmp_path, monkeypatch):
    first = ("PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-F5-A|CLC=AAA2270730100|"
             "LBL=LBL-F5-A|HSH=0123456789abcdef")
    second = ("PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-F5-B|CLC=AAA2270730100|"
              "LBL=LBL-F5-B|HSH=fedcba9876543210")
    app, _journal = _recovery_app(tmp_path, monkeypatch, {
        "workflow_mode": "RECONCILIATION", "status": "PREPARED",
        "exchange_id": "EX-MULTI", "authority_scope_id": "SCOPE-F5",
        "prepare_idempotency_key": "KEY-MULTI",
    })
    digest = app._label_recovery_hold_id()
    assert app._hold_label_recovery(digest, manager_code="admin") is True
    app.phs_label_exchange_coordinator.client = SimpleNamespace(
        config=SimpleNamespace(authority_scope_id="SCOPE-F5"),
        resolve_active_phs_label=lambda *_args, **_kwargs: {
            "input_tag": {"qr_payload": first},
        },
        get_phs_label_exchange=lambda *_args, **_kwargs: {
            "exchange": {"exchange_id": "EX-MULTI", "state": "PREPARED",
                         "operation_key": _operation_key("KEY-MULTI")},
            "source_labels": [
                {"qr_payload": first, "label_id": "LBL-F5-A",
                 "scan_anchor_input_tag_id": "ITG-F5-A"},
                {"qr_payload": second, "label_id": "LBL-F5-B",
                 "scan_anchor_input_tag_id": "ITG-F5-B"},
            ],
        },
    )
    result = app._recheck_package_recovery_physical(
        "F5:" + digest, first, manager_code="admin"
    )
    assert "다른 현품표" in result
    assert app._label_recovery_source_is_held(first) is True
    assert app._label_recovery_source_is_held(second) is True
    assert app._label_recovery_source_is_held(SOURCE_OTHER) is False
    with app.package_outbox._connect() as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM phs_label_workbench_hold_sources WHERE hold_id=?",
            (digest,),
        ).fetchone()[0] == 2


def test_valid_but_wrong_journal_scan_does_not_bind_another_central_exchange(
        tmp_path, monkeypatch):
    app, _journal = _recovery_app(tmp_path, monkeypatch, {
        "workflow_mode": "SINGLE", "status": "PREPARED",
        "scan_payload": SOURCE_A, "exchange_id": "EX-WRONG",
        "authority_scope_id": "SCOPE-F5", "prepare_idempotency_key": "KEY-WRONG",
    })
    digest = app._label_recovery_hold_id()
    assert app._hold_label_recovery(digest, manager_code="admin") is True
    app.phs_label_exchange_coordinator.client = SimpleNamespace(
        config=SimpleNamespace(authority_scope_id="SCOPE-F5"),
        resolve_active_phs_label=lambda *_args, **_kwargs: {
            "input_tag": {"qr_payload": SOURCE_B},
        },
        get_phs_label_exchange=lambda *_args, **_kwargs: {
            "exchange": {"exchange_id": "EX-WRONG", "state": "PREPARED",
                         "operation_key": _operation_key("KEY-WRONG")},
            "source_labels": [{"qr_payload": SOURCE_B, "label_id": "LABEL-B",
                               "scan_anchor_input_tag_id": "ITG-B"}],
        },
    )
    result = app._recheck_package_recovery_physical(
        "F5:" + digest, SOURCE_B, manager_code="admin"
    )
    assert "일치하지" in result
    assert app.package_outbox.get_label_exchange_hold(digest)["source_input_tag_id"] == ""
    assert app._label_recovery_source_is_held(SOURCE_B) is False


def test_f5_archive_audit_failure_is_read_back_and_kept_for_retry(tmp_path, monkeypatch):
    app, journal, _current = _storage_recovery_app(tmp_path, monkeypatch)
    journal.save({"status": "PREPARED", "scan_payload": SOURCE_A})
    digest = app._label_recovery_hold_id()
    assert app._hold_label_recovery(digest, manager_code="admin") is True
    archive = journal.path.with_name(journal.path.name + ".held-" + digest)
    archive.write_bytes(b"\x00damaged")
    monkeypatch.setattr(app.package_outbox, "audit_workbench_action",
                        lambda **_kwargs: (_ for _ in ()).throw(OSError("audit disk")))
    assert app._finalize_label_recovery_holds() is True
    assert app._package_recovery_file_issues["F5:" + digest] == "AUDIT_FAILED"
    assert archive.read_bytes() == b"\x00damaged"
    assert app.package_outbox.get_label_exchange_hold(digest) is not None
    assert not app.package_outbox.has_workbench_audit(
        set_id="F5:" + digest, action="FILE_UNVERIFIED", observed="PackageLogisticsError",
    )


def test_recovery_audit_requires_fresh_connection_readback(tmp_path, monkeypatch):
    outbox = app_module.PackageOutbox(tmp_path / "outbox.sqlite3")
    original_connect = outbox._connect
    calls = 0

    @contextmanager
    def missing_after_commit():
        nonlocal calls
        calls += 1
        with original_connect() as conn:
            if calls == 2:
                conn.execute("DELETE FROM package_workbench_hold_audit")
                conn.commit()
            yield conn

    monkeypatch.setattr(outbox, "_connect", missing_after_commit)
    with pytest.raises(app_module.PackageLogisticsError, match="readback"):
        outbox.audit_workbench_action(
            set_id="SET-A", action="RECHECK", manager_id="ADMIN", observed="UNKNOWN",
        )


def test_f5_archive_path_mutation_never_reads_outside_storage(tmp_path, monkeypatch):
    app, journal, _current = _storage_recovery_app(tmp_path, monkeypatch)
    journal.save({"status": "PREPARED", "scan_payload": SOURCE_A})
    digest = app._label_recovery_hold_id()
    assert app._hold_label_recovery(digest, manager_code="admin") is True
    outside = tmp_path.parent / ("outside.held-" + digest)
    outside.write_bytes(b"outside-sentinel")
    with app.package_outbox._connect() as conn:
        conn.execute("UPDATE phs_label_workbench_holds SET archive_path=? WHERE hold_id=?",
                     (str(tmp_path / ".." / outside.name), digest))
        conn.commit()
    assert app._finalize_label_recovery_holds() is True
    assert outside.read_bytes() == b"outside-sentinel"
    assert "F5:" + digest in app._package_recovery_file_issues


def test_f5_hold_writer_rejects_unbound_archive_path(tmp_path, monkeypatch):
    app, journal = _recovery_app(tmp_path, monkeypatch, {"status": "PREPARED"})
    raw = journal.path.read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    with pytest.raises(app_module.PackageLogisticsError):
        app.package_outbox.hold_label_exchange(
            hold_id=digest, set_id="", label_id="", source_label="",
            source_input_tag_id="", journal_bytes=raw,
            archive_path=str(tmp_path / ".." / ("outside.held-" + digest)),
            held_by="S-1-5-21-101",
        )
    assert app.package_outbox.get_label_exchange_hold(digest) is None
    assert journal.path.read_bytes() == raw


@pytest.mark.parametrize("field,value", [
    ("start_time", "not-a-date"),
    ("exact_rescan_target_count", {"bad": 1}),
    ("canonical_input_tag_qr", 7),
])
def test_current_nested_type_probe_is_item_hold_not_startup_exception(
    tmp_path, monkeypatch, field, value,
):
    app, _journal, current = _storage_recovery_app(tmp_path, monkeypatch)
    state = {"current_set_info": {"id": "SET-BAD", "raw": [], field: value}}
    raw = json.dumps(state).encode("utf-8")
    current.write_bytes(raw)
    app._load_current_set_state()
    assert app._workflow_blocking_notice.kind == "submission_blocked"
    digest = hashlib.sha256(raw).hexdigest()
    assert app._hold_package_recovery_set("CURRENT:" + digest, manager_code="admin") is True
    assert current.with_name(current.name + ".held-" + digest).read_bytes() == raw
    assert app.__dict__.get("_workflow_blocking_notice") is None


def test_unverified_current_audit_failure_keeps_retryable_hold(tmp_path, monkeypatch):
    app, _journal, current = _storage_recovery_app(tmp_path, monkeypatch)
    raw = b'{"current_set_info":{"id":[],"raw":[]}}'
    current.write_bytes(raw)
    digest = hashlib.sha256(raw).hexdigest()
    app._load_current_set_state()
    original_audit = app.package_outbox.audit_workbench_action
    monkeypatch.setattr(app.package_outbox, "audit_workbench_action",
                        lambda **_kwargs: (_ for _ in ()).throw(OSError("audit disk")))
    assert app._hold_package_recovery_set("CURRENT:" + digest, manager_code="admin") is False
    archive = current.with_name(current.name + ".held-" + digest)
    assert archive.read_bytes() == raw
    assert app.package_outbox.get_workbench_hold("CURRENT:" + digest) is not None
    assert app._workflow_blocking_notice.kind == "submission_blocked"
    monkeypatch.setattr(app.package_outbox, "audit_workbench_action", original_audit)
    assert app._hold_package_recovery_set("CURRENT:" + digest, manager_code="admin") is True
    assert archive.read_bytes() == raw
    assert app.package_outbox.has_workbench_audit(
        set_id="CURRENT:" + digest, action="FILE_UNVERIFIED", observed="ValueError",
    )
    assert app.__dict__.get("_workflow_blocking_notice") is None


def test_corrupt_draft_recovers_phs2_and_blocks_original_rescan_before_submit(tmp_path, monkeypatch):
    app, _journal = _recovery_app(tmp_path, monkeypatch, {})
    source = ("PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-ORIGINAL|CLC=AAA2270730100|"
              "LBL=LBL-ORIGINAL|HSH=0123456789abcdef")
    draft = {"set_id": "SET-ORIGINAL", "membership_mode": [],
             "source_canonical_input_tag_qr": source,
             "source_input_tag_id": "ITG-ORIGINAL"}
    with app.package_outbox._connect() as conn:
        conn.execute(
            """INSERT INTO package_command_outbox
               (idempotency_key,set_id,command_fingerprint,draft_json,status,created_at,updated_at)
               VALUES (?,?,?,?,?,?,?)""",
            ("KEY-ORIGINAL", "SET-ORIGINAL", "FINGERPRINT", json.dumps(draft),
             "PENDING", "2026-09-23T00:00:00Z", "2026-09-23T00:00:00Z"),
        )
        conn.commit()
    app._load_current_set_state = lambda: None
    assert app._hold_package_recovery_set("SET-ORIGINAL", manager_code="admin") is True
    held = app.package_outbox.get_workbench_hold("SET-ORIGINAL")
    assert held["source_input_tag_id"] == "ITG-ORIGINAL"
    assert app.package_outbox.claim_next() is None  # unresolved original cannot be sent

    class Entry:
        value = source
        state = "normal"

        def get(self):
            return self.value

        def cget(self, _name):
            return self.state

        def configure(self, *, state):
            self.state = state

        def delete(self, *_args):
            self.value = ""

    app.entry = Entry()
    app.data_manager = SimpleNamespace(log_event=lambda *_args, **_kwargs: None)
    app.package_logistics_client = object()
    app.current_set_info = {"id": None, "raw": [], "parsed": []}
    app.is_blinking = False
    app.initialized_successfully = True
    app._ui_lane_is_busy = lambda: False
    app._sealed_transfer_exchange_blocks_local_action = lambda _action: False
    app._block_view_only_action = lambda _action: False
    app._block_active_history_load_action = lambda _action: False
    app._central_inherit_all_active = lambda: False
    app._begin_central_phs2_scan_overlay = lambda *_args, **_kwargs: pytest.fail(
        "held source reached central submission"
    )
    app.process_input()
    assert app.current_set_info["raw"] == []
    assert app.entry.get() == ""


@pytest.mark.parametrize("evidence_location", ["current_file", "command_row", "audit_row"])
def test_identity_recovery_rechecks_each_surviving_durable_source(
    tmp_path, monkeypatch, evidence_location,
):
    app, _journal, current = _storage_recovery_app(tmp_path, monkeypatch)
    source = ("PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-EVIDENCE|CLC=AAA2270730100|"
              "LBL=LBL-EVIDENCE|HSH=0123456789abcdef")
    set_id = "SET-EVIDENCE"
    candidate = {"set_id": set_id}
    if evidence_location == "current_file":
        current.write_bytes(("{\"current_set_info\":{\"id\":[],\"raw\":[\"" + source + "\"]}}")
                            .encode("utf-8"))
        candidate["current_file"] = str(current)
    else:
        with app.package_outbox._connect() as conn:
            conn.execute(
                """INSERT INTO package_command_outbox
                   (idempotency_key,set_id,command_fingerprint,draft_json,command_json,
                    status,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)""",
                ("KEY-EVIDENCE", set_id, "FINGERPRINT", "{broken",
                 json.dumps({"payload": {"source_canonical_input_tag_qr": source}})
                 if evidence_location == "command_row" else None,
                 "PENDING", "2026-09-23T00:00:00Z", "2026-09-23T00:00:00Z"),
            )
            if evidence_location == "audit_row":
                conn.execute(
                    """INSERT INTO package_workbench_hold_audit
                       (set_id,action,manager_id,observed,recorded_at) VALUES (?,?,?,?,?)""",
                    (set_id, "BIND_SOURCE", "SYSTEM",
                     json.dumps({"result": "CENTRAL_MATCH", "source_phs2": source,
                                 "source_input_tag_id": "ITG-EVIDENCE"}),
                     "2026-09-23T00:00:00Z"),
                )
            conn.commit()
    assert app._package_recovery_identity(candidate) == (source, "ITG-EVIDENCE")


def test_unknown_hold_suppresses_original_send_and_late_409_is_manager_review(
    tmp_path, monkeypatch,
):
    app, _journal = _recovery_app(tmp_path, monkeypatch, {})
    with app.package_outbox._connect() as conn:
        conn.execute(
            """INSERT INTO package_command_outbox
               (idempotency_key,set_id,command_fingerprint,draft_json,status,
                local_completion_committed,created_at,updated_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            ("KEY-UNKNOWN", "SET-UNKNOWN", "FINGERPRINT",
             json.dumps({"package_bundle_id": "PACK-UNKNOWN"}), "PENDING", 1,
             "2026-09-23T00:00:00Z", "2026-09-23T00:00:00Z"),
        )
        conn.commit()
    app.package_outbox.hold_workbench_set(
        set_id="SET-UNKNOWN", source_phs2="", source_input_tag_id="",
        snapshot={"package_key": "KEY-UNKNOWN"}, reason="신원 미확인 보류",
        held_by="S-1-5-21-101",
    )
    assert app.package_outbox.claim_next() is None
    # The isolated test models a later explicit release; the product never
    # deletes a hold automatically while its source remains unidentified.
    with app.package_outbox._connect() as conn:
        conn.execute("DELETE FROM package_workbench_holds WHERE set_id=?", ("SET-UNKNOWN",))
        conn.commit()
    assert app.package_outbox.claim_next()["idempotency_key"] == "KEY-UNKNOWN"
    conflict = RuntimeError("source consumed by the other request")
    conflict.code = "PHS_WORK_GROUP_TRANSFER_NOT_READY"
    app.package_outbox.mark_conflict("KEY-UNKNOWN", conflict)
    row = app.package_outbox.get_by_set_id("SET-UNKNOWN")
    assert row["status"] == "CONFLICT"
    assert row["review_status"] == "OPERATOR_REVIEW"
    assert row["last_error_code"] == "PHS_WORK_GROUP_TRANSFER_NOT_READY"


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

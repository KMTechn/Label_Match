"""Headless PIN authorization, interruption, and item quarantine contracts."""

import hashlib
import hmac
import json
import re
import sqlite3
from types import SimpleNamespace
import uuid

import pytest

import Label_Match as app_module
from direct_sync_push import canonical_request_string
from label_admin_pin import AdminPinClient, AdminPinIntentStore, operator_local_id
from tests.test_w9nb_label_recovery import (
    SOURCE_A, SOURCE_B, _operation_key, _recovery_app, _storage_recovery_app,
)


PIN = "246813"
VERIFICATION_ID = "09999999-9999-4999-8999-999999999999"
REDEMPTION_ID = "08888888-8888-4888-8888-888888888888"


class SignedPinServer:
    def __init__(self):
        self.issued = None
        self.consumed = False
        self.verify_calls = 0
        self.redeem_calls = 0
        self.status_calls = 0
        self.lost_issue = False
        self.lost_redeem = False
        self.incomplete = ""
        self.offline = False

    def __call__(self, path, raw, headers):
        if self.offline:
            raise OSError("offline")
        body = json.loads(raw)
        canonical = canonical_request_string(
            method="POST", path=path, query_string="",
            timestamp=headers["X-Producer-Timestamp"], nonce=headers["X-Producer-Nonce"],
            producer_id=headers["X-Producer-Id"], key_id=headers["X-Producer-Key-Id"],
            metadata=body, content_sha256=hashlib.sha256(raw).hexdigest(),
            byte_length=len(raw), content_type="application/json",
        )
        assert hmac.compare_digest(headers["X-Producer-Signature"],
                                   hmac.new(b"test-secret", canonical.encode(), hashlib.sha256).hexdigest())
        if path.endswith("/verifications"):
            self.verify_calls += 1
            if body["pin"] != PIN:
                return 401, b'{"ok":false,"error":{"code":"PIN_INVALID"}}'
            assert body["operator"]["local_id"].startswith("wn:")
            if self.issued is not None:
                assert body == self.issued
            else:
                self.issued = body
            if self.lost_issue:
                self.lost_issue = False
                raise OSError("lost issue response")
            answer = {
                "ok": True, "verification_id": VERIFICATION_ID, "proof": "proof-secret",
                "issued_at": 1000, "expires_at": 1180,
                "admin_id": body["admin_id"], "action": body["action"],
                "target_fingerprint": body["target"]["state_fingerprint"],
                "device_ref": "installation:host", "request_id": body["request_id"],
            }
            if self.incomplete == "verification":
                del answer["expires_at"]
            return 201, json.dumps(answer).encode()
        if path.endswith("/redemptions/status"):
            self.status_calls += 1
            answer = {
                "ok": True, "status": "CONSUMED" if self.consumed else "UNUSED",
                "verification_id": body["verification_id"],
                "operation_key": body["operation_key"],
            }
            if self.consumed:
                answer.update(redemption_id=REDEMPTION_ID, consumed_at=1001)
            if self.incomplete == "status":
                answer.pop("operation_key")
            return 200, json.dumps(answer).encode()
        assert path.endswith("/redemptions")
        self.redeem_calls += 1
        assert self.issued is not None
        assert body["action"] == self.issued["action"]
        assert body["target"] == self.issued["target"]
        assert body["operator"] == self.issued["operator"]
        self.consumed = True
        if self.lost_redeem:
            self.lost_redeem = False
            raise OSError("lost redemption response")
        answer = {
            "ok": True, "redemption_id": REDEMPTION_ID, "consumed_at": 1001,
            "verification_id": body["verification_id"],
            "operation_key": body["operation_key"],
        }
        if self.incomplete == "redemption":
            del answer["redemption_id"]
        return 200, json.dumps(answer).encode()


def _pin_app(tmp_path, monkeypatch, *, pin=PIN):
    app, journal = _recovery_app(tmp_path, monkeypatch, {
        "workflow_mode": "RECONCILIATION", "status": "PREPARE_PENDING",
        "set_id": "", "scan_payload": SOURCE_A,
        "active_scan_label_id": "LABEL-A", "authority_scope_id": "SCOPE-A",
        "prepare_idempotency_key": "PREPARE-A", "exchange_id": "",
    })
    del app._package_recovery_manager
    app.save_directory = str(tmp_path)
    app.worker_name = "홍길동"
    app._admin_pin_login_epoch = uuid.uuid4().hex
    app._admin_pin_store = AdminPinIntentStore(tmp_path / "admin-pin-intents.sqlite3")
    app._prompt_package_admin_pin = lambda: ("admin-personal", pin)
    server = SignedPinServer()
    credentials = SimpleNamespace(
        producer_id="producer-label", key_id="key-label", secret="test-secret",
        endpoint_url="https://example.invalid/api/producer-ingest",
    )
    app._admin_pin_client = AdminPinClient(credentials, transport=server)
    return app, journal, server


def _intent(app):
    with sqlite3.connect(app._admin_pin_store.path) as conn:
        return conn.execute("SELECT operation_key,verification_id,state,error_code FROM admin_pin_intents").fetchone()


def _active_set_pin_app(tmp_path, monkeypatch, *, linked):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    set_id = "SET-VALID"
    if linked:
        journal.save({
            "workflow_mode": "RECONCILIATION", "status": "PREPARE_PENDING",
            "set_id": set_id, "scan_payload": SOURCE_A,
            "active_scan_label_id": "LABEL-A", "authority_scope_id": "SCOPE-A",
            "prepare_idempotency_key": "PREPARE-A", "exchange_id": "",
        })
    else:
        journal.path.unlink()
    app.current_set_info = {"id": set_id, "raw": [SOURCE_A], "parsed": [],
                            "recovery_operator_review": True}
    current = tmp_path / "current.json"
    def save(state):
        current.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
        return True
    app.data_manager = SimpleNamespace(
        save_directory=str(tmp_path), _current_state_filename=lambda: "current.json",
        load_current_state=lambda: json.loads(current.read_text(encoding="utf-8")) if current.exists() else None,
        save_current_state=save, delete_current_state=lambda: current.unlink(missing_ok=True),
    )
    app.initialized_successfully = True
    app.is_blinking = False
    app._reset_current_set = lambda: setattr(app, "current_set_info", {"id": None, "raw": []})
    app._load_current_set_state = lambda: None
    save({"current_set_info": app.current_set_info, "timestamp": "2026-09-24T00:00:00"})
    assert app._read_package_recovery_file(current)["verified"]
    return app, journal, current, server, set_id


@pytest.mark.parametrize("linked", [False, True])
@pytest.mark.parametrize("boundary", ["hold_row", "file_deleted", "slot_released", "applied"])
def test_set_hold_interruptions_resume_same_key(tmp_path, monkeypatch, linked, boundary):
    app, journal, current, server, set_id = _active_set_pin_app(
        tmp_path, monkeypatch, linked=linked
    )
    action = "LABEL.F5_HOLD" if linked else "LABEL.SET_HOLD"
    selected = "F5:" + hashlib.sha256(journal.path.read_bytes()).hexdigest() if linked else set_id
    original_delete = app.data_manager.delete_current_state
    original_reset = app._reset_current_set
    original_transition = app._admin_pin_store.transition
    interrupted = False

    def delete():
        nonlocal interrupted
        if boundary == "hold_row" and not interrupted:
            interrupted = True
            raise OSError("interrupted after hold row")
        original_delete()
        if boundary == "file_deleted" and not interrupted:
            interrupted = True
            raise OSError("interrupted after file delete")

    def reset():
        nonlocal interrupted
        original_reset()
        if boundary == "slot_released" and not interrupted:
            interrupted = True
            raise OSError("interrupted after slot release")

    def transition(key, state, *, error_code=""):
        nonlocal interrupted
        if boundary == "applied" and state == "APPLIED" and not interrupted:
            interrupted = True
            raise OSError("interrupted before APPLIED")
        return original_transition(key, state, error_code=error_code)

    app.data_manager.delete_current_state = delete
    app._reset_current_set = reset
    app._admin_pin_store.transition = transition
    assert app._run_package_pin_action(action, selected) is False
    assert interrupted
    key = _intent(app)[0]
    assert _intent(app)[2] != "APPLIED"
    if boundary == "hold_row":
        assert current.exists()
    if boundary == "file_deleted":
        assert app.current_set_info["id"] == set_id
    if boundary != "applied":
        assert not app._package_pin_effect(app._admin_pin_store.pending(
            "label_f5_recovery" if linked else "label_set_recovery",
            selected[3:] if linked else selected,
        ))
    assert app._run_package_pin_action(action, selected)
    assert _intent(app)[0] == key and _intent(app)[2] == "APPLIED"
    assert not current.exists() and app.current_set_info["id"] is None
    assert server.redeem_calls == server.verify_calls == 1
    with app.package_outbox._connect() as conn:
        rows = conn.execute("""SELECT set_id,action,operation_key FROM package_workbench_hold_audit
            WHERE action='PIN_REVIEW_HOLD'""").fetchall()
    assert any(row["set_id"] == set_id and row["operation_key"] == key for row in rows)
    if linked:
        assert any(row["set_id"] == selected and row["operation_key"] == key for row in rows)
        assert not journal.path.exists()


def test_existing_hold_row_resume_preserves_new_pin_key(tmp_path, monkeypatch):
    app, _journal, current, server, set_id = _active_set_pin_app(
        tmp_path, monkeypatch, linked=False
    )
    app.package_outbox.hold_workbench_set(
        set_id=set_id, source_phs2="", source_input_tag_id="",
        snapshot=json.loads(current.read_text(encoding="utf-8")),
        reason="prior hold", held_by="prior-manager",
    )
    original_delete = app.data_manager.delete_current_state
    interrupted = False

    def delete():
        nonlocal interrupted
        original_delete()
        if not interrupted:
            interrupted = True
            raise OSError("interrupted after file delete")

    app.data_manager.delete_current_state = delete
    assert app._run_package_pin_action("LABEL.SET_HOLD", set_id) is False
    key = _intent(app)[0]
    assert interrupted and not current.exists()
    assert app._run_package_pin_action("LABEL.SET_HOLD", set_id)
    assert _intent(app)[0] == key and _intent(app)[2] == "APPLIED"
    assert app.current_set_info["id"] is None
    assert server.redeem_calls == server.verify_calls == 1


@pytest.mark.parametrize("linked", [False, True])
def test_after_applied_write_keeps_terminal_state_and_single_effect(tmp_path, monkeypatch, linked):
    app, journal, current, server, set_id = _active_set_pin_app(
        tmp_path, monkeypatch, linked=linked
    )
    action = "LABEL.F5_HOLD" if linked else "LABEL.SET_HOLD"
    selected = "F5:" + hashlib.sha256(journal.path.read_bytes()).hexdigest() if linked else set_id
    original_transition = app._admin_pin_store.transition
    interrupted = False

    def after_applied(key, state, *, error_code=""):
        nonlocal interrupted
        original_transition(key, state, error_code=error_code)
        if state == "APPLIED" and not interrupted:
            interrupted = True
            raise OSError("interrupted after APPLIED write")

    app._admin_pin_store.transition = after_applied
    assert app._run_package_pin_action(action, selected)
    key = _intent(app)[0]
    assert interrupted and _intent(app)[2] == "APPLIED"
    assert app._run_package_pin_action(action, selected)
    assert _intent(app)[0] == key and _intent(app)[2] == "APPLIED"
    assert server.verify_calls == server.redeem_calls == 1
    assert not current.exists() and app.current_set_info["id"] is None
    with pytest.raises(Exception):
        app._admin_pin_store.transition(key, "UNVERIFIED_OPERATOR_HOLD")
    assert _intent(app)[2] == "APPLIED"
    with app.package_outbox._connect() as conn:
        count = conn.execute("""SELECT COUNT(*) FROM package_workbench_hold_audit
            WHERE set_id=? AND action='PIN_REVIEW_HOLD' AND operation_key=?""",
            (selected, key)).fetchone()[0]
    assert count == 1


@pytest.mark.parametrize("boundary", ["hold_row", "archive_moved"])
def test_damaged_current_hold_resumes_same_key(tmp_path, monkeypatch, boundary):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    journal.path.unlink()
    current = tmp_path / "current.json"
    current.write_bytes(b"{broken-current-file")
    app.data_manager = SimpleNamespace(
        save_directory=str(tmp_path), _current_state_filename=lambda: "current.json",
        load_current_state=lambda: None, delete_current_state=lambda: current.unlink(missing_ok=True),
    )
    app._load_current_set_state = lambda: None
    digest = hashlib.sha256(current.read_bytes()).hexdigest()
    set_id = "CURRENT:" + digest
    original_replace = app_module.replace_checked
    interrupted = False

    def replace(*args, **kwargs):
        nonlocal interrupted
        if not interrupted:
            interrupted = True
            if boundary == "hold_row":
                raise OSError("interrupted after hold row")
            original_replace(*args, **kwargs)
            raise OSError("interrupted after archive move")
        return original_replace(*args, **kwargs)

    monkeypatch.setattr(app_module, "replace_checked", replace)
    assert app._run_package_pin_action("LABEL.SET_HOLD", set_id) is False
    assert interrupted and app.package_outbox.get_workbench_hold(set_id)
    key = _intent(app)[0]
    assert app._run_package_pin_action("LABEL.SET_HOLD", set_id)
    assert _intent(app)[0] == key and _intent(app)[2] == "APPLIED"
    assert not current.exists()
    assert current.with_name(current.name + ".held-" + digest).read_bytes() == b"{broken-current-file"
    assert server.redeem_calls == server.verify_calls == 1
    with app.package_outbox._connect() as conn:
        rows = conn.execute("""SELECT action,operation_key FROM package_workbench_hold_audit
            WHERE set_id=? AND action='PIN_REVIEW_HOLD'""", (set_id,)).fetchall()
    assert len(rows) == 1 and rows[0]["operation_key"] == key


@pytest.mark.parametrize("admin_id,pin", [("", PIN), ("admin-personal", "12345X")])
def test_pin_format_rejection_is_audited_without_secret(tmp_path, monkeypatch, admin_id, pin):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    app._prompt_package_admin_pin = lambda: (admin_id, pin)
    set_id = "F5:" + hashlib.sha256(journal.path.read_bytes()).hexdigest()
    assert app._run_package_pin_action("LABEL.F5_HOLD", set_id) is False
    assert server.verify_calls == server.redeem_calls == 0
    with app.package_outbox._connect() as conn:
        rows = conn.execute("""SELECT set_id,pin_action,manager_id,operator_id,operator_name,
            observed FROM package_workbench_hold_audit WHERE action='PIN_ATTEMPT'""").fetchall()
    assert len(rows) == 1
    assert tuple(rows[0]) == (set_id, "LABEL.F5_HOLD", admin_id,
                              operator_local_id("홍길동"), "홍길동", "PIN_INVALID")
    assert pin.encode() not in (tmp_path / "outbox.sqlite3").read_bytes()


def test_signed_f5_hold_records_keyed_effect_and_no_secret(tmp_path, monkeypatch, capsys):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    digest = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    assert app._run_package_pin_action("LABEL.F5_HOLD", "F5:" + digest) is True
    assert server.verify_calls == server.redeem_calls == 1
    assert _intent(app)[2] == "APPLIED"
    held = app.package_outbox.get_label_exchange_hold(digest)
    assert held["held_by"] == "admin-personal"
    with app.package_outbox._connect() as conn:
        rows = conn.execute("""SELECT action,operation_key,verification_id,pin_action,
            operator_id,operator_name FROM package_workbench_hold_audit ORDER BY audit_id""").fetchall()
    assert any(row["action"] == "HOLD_LABEL" and row["operation_key"] == _intent(app)[0]
               and row["verification_id"] == VERIFICATION_ID for row in rows)
    assert all(row["operator_name"] == "홍길동" for row in rows)
    assert all(row["operator_id"] == operator_local_id("홍길동") for row in rows)
    durable = app._admin_pin_store.path.read_bytes() + app.package_outbox.db_path.encode()
    durable += (tmp_path / "outbox.sqlite3").read_bytes()
    assert PIN.encode() not in durable and b"proof-secret" not in durable
    output = capsys.readouterr()
    assert PIN not in (output.out + output.err)
    assert "proof-secret" not in (output.out + output.err)
    assert held["journal_bytes"] == (tmp_path / f"label-exchange.json.held-{digest}").read_bytes()


@pytest.mark.parametrize("failure", ["wrong_pin", "incomplete_verification", "incomplete_redemption"])
def test_pin_failure_or_incomplete_200_never_applies(tmp_path, monkeypatch, failure):
    app, journal, server = _pin_app(tmp_path, monkeypatch, pin="000000" if failure == "wrong_pin" else PIN)
    server.incomplete = failure.removeprefix("incomplete_") if failure.startswith("incomplete") else ""
    digest = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    assert app._run_package_pin_action("LABEL.F5_HOLD", "F5:" + digest) is False
    assert journal.path.exists()
    assert app.package_outbox.get_label_exchange_hold(digest) is None
    assert "proof-secret" not in app.__dict__.get("_admin_pin_last_message", "")


def test_prewrite_audit_failure_applies_zero(tmp_path, monkeypatch):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    digest = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    original = app.package_outbox.audit_pin_attempt

    def fail_prepared(**fields):
        if fields["result"] == "PREPARED":
            raise OSError("audit unavailable")
        return original(**fields)

    monkeypatch.setattr(app.package_outbox, "audit_pin_attempt", fail_prepared)
    assert app._run_package_pin_action("LABEL.F5_HOLD", "F5:" + digest) is False
    assert server.redeem_calls == 0
    assert journal.path.exists() and app.package_outbox.get_label_exchange_hold(digest) is None


def test_intent_fsync_failure_prevents_redemption_and_writer(tmp_path, monkeypatch):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    digest = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    monkeypatch.setattr(app._admin_pin_store, "prepare",
                        lambda **_kwargs: (_ for _ in ()).throw(OSError("fsync failed")))
    assert app._run_package_pin_action("LABEL.F5_HOLD", "F5:" + digest) is False
    assert server.verify_calls == 1 and server.redeem_calls == 0
    assert journal.path.exists() and app.package_outbox.get_label_exchange_hold(digest) is None


@pytest.mark.parametrize("route,code,status", [
    ("verifications", "PIN_BUSY", 503),
    ("verifications", "PIN_RATE_LIMITED", 429),
    ("verifications", "CENTRAL_ACTION_UNAVAILABLE", 409),
    ("redemptions", "PROOF_EXPIRED", 409),
    ("redemptions", "PROOF_USED", 409),
    ("redemptions", "PROOF_REVOKED", 409),
])
def test_server_refusals_leave_original_and_apply_zero(tmp_path, monkeypatch, route, code, status):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    digest = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    original = server.__call__

    def reject(path, raw, headers):
        if path.endswith("/" + route):
            return status, json.dumps({"ok": False, "error": {"code": code}}).encode()
        return original(path, raw, headers)

    app._admin_pin_client.transport = reject
    assert app._run_package_pin_action("LABEL.F5_HOLD", "F5:" + digest) is False
    assert journal.path.exists() and app.package_outbox.get_label_exchange_hold(digest) is None
    assert server.redeem_calls == 0
    assert PIN not in app._admin_pin_last_message


def test_lost_issue_and_redemption_response_use_same_request_and_key(tmp_path, monkeypatch):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    server.lost_issue = server.lost_redeem = True
    digest = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    assert app._run_package_pin_action("LABEL.F5_HOLD", "F5:" + digest) is True
    assert server.verify_calls == 2 and server.redeem_calls == 1 and server.status_calls == 1
    assert _intent(app)[2] == "APPLIED"


def test_consumed_intent_resumes_before_writer_without_second_redemption(tmp_path, monkeypatch):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    digest = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    target, source_sha = app._package_pin_target("LABEL.F5_HOLD", "F5:" + digest)
    operator = app._package_pin_operator()
    server.issued = {"action": "LABEL.F5_HOLD", "target": target, "operator": operator}
    server.consumed = True
    app._admin_pin_store.prepare(
        operation_key="resume-key", verification_id=VERIFICATION_ID,
        action="LABEL.F5_HOLD", target=target, operator=operator,
        admin_id="admin-personal", source_evidence_sha256=source_sha,
    )
    assert app._run_package_pin_action("LABEL.F5_HOLD", "F5:" + digest) is True
    assert server.status_calls == 1 and server.redeem_calls == server.verify_calls == 0
    assert _intent(app)[2] == "APPLIED"
    assert app.package_outbox.get_label_exchange_hold(digest) is not None


def test_after_effect_before_applied_recovers_from_keyed_audit(tmp_path, monkeypatch):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    digest = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    transition = app._admin_pin_store.transition
    fail_once = True

    def interrupted(key, state, *, error_code=""):
        nonlocal fail_once
        if state == "APPLIED" and fail_once:
            fail_once = False
            raise OSError("interrupted before APPLIED")
        return transition(key, state, error_code=error_code)

    app._admin_pin_store.transition = interrupted
    assert app._run_package_pin_action("LABEL.F5_HOLD", "F5:" + digest) is False
    assert app.package_outbox.get_label_exchange_hold(digest) is not None
    assert app._run_package_pin_action("LABEL.F5_HOLD", "F5:" + digest)
    assert _intent(app)[2] == "APPLIED"
    assert server.verify_calls == server.redeem_calls == 1
    with app.package_outbox._connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM phs_label_workbench_holds").fetchone()[0] == 1


def test_after_hold_row_before_archive_finishes_same_effect(tmp_path, monkeypatch):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    digest = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    original_replace = app_module.replace_checked
    interrupt_once = True

    def interrupted(*args, **kwargs):
        nonlocal interrupt_once
        if interrupt_once:
            interrupt_once = False
            raise OSError("interrupted after hold row")
        return original_replace(*args, **kwargs)

    monkeypatch.setattr(app_module, "replace_checked", interrupted)
    assert app._run_package_pin_action("LABEL.F5_HOLD", "F5:" + digest) is False
    assert app.package_outbox.get_label_exchange_hold(digest) is not None
    assert journal.path.exists()
    assert app._run_package_pin_action("LABEL.F5_HOLD", "F5:" + digest) is True
    assert not journal.path.exists()
    assert _intent(app)[2] == "APPLIED"
    assert server.redeem_calls == 1


def test_offline_explicit_park_blocks_claimed_f5_and_allows_other(tmp_path, monkeypatch):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    prompts = []
    app._prompt_package_admin_pin = lambda: prompts.append(1) or ("admin-personal", PIN)
    server.offline = True
    digest = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    assert app._run_package_pin_action("LABEL.F5_HOLD", "F5:" + digest) is False
    assert journal.path.exists() and app.package_outbox.get_label_exchange_hold(digest) is None
    assert _intent(app)[2] == "UNVERIFIED_OPERATOR_HOLD"
    assert app._park_unverified_pin_item("F5:" + digest) is True
    assert len(prompts) == 1
    assert app._label_recovery_source_is_held(SOURCE_A) is True
    assert app._label_recovery_source_is_held(SOURCE_B) is False
    assert app._active_label_recovery_state() == {}
    started = []
    app.phs_label_exchange_coordinator.reconciliation_available = lambda: True
    app.phs_label_exchange_coordinator.has_pending_reconciliation = lambda: False
    app._sealed_transfer_exchange_blocks_local_action = lambda _action: False
    app._show_phs_reconciliation_scan_window = lambda: started.append("F5")
    assert app._handle_phs_label_exchange_shortcut() == "break"
    assert started == ["F5"]
    assert not journal.path.exists()
    journal.save({
        "workflow_mode": "RECONCILIATION", "status": "PREPARE_PENDING",
        "set_id": "", "scan_payload": SOURCE_B,
        "active_scan_label_id": "LABEL-B", "authority_scope_id": "SCOPE-B",
        "prepare_idempotency_key": "PREPARE-B", "exchange_id": "",
    })
    assert app._label_recovery_hold_id() != digest
    assert app._label_recovery_source_is_held(SOURCE_B) is False
    journal.path.unlink()
    server.offline = False
    assert app._run_package_pin_action("LABEL.F5_HOLD", "F5:" + digest) is True
    assert _intent(app)[2] == "SUPERSEDED"
    with sqlite3.connect(app._admin_pin_store.path) as conn:
        assert conn.execute("SELECT state FROM admin_pin_intents ORDER BY rowid DESC LIMIT 1").fetchone()[0] == "APPLIED"


def test_cancelled_pin_prompt_does_not_reprompt_or_write(tmp_path, monkeypatch):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    prompts = []
    app._prompt_package_admin_pin = lambda: prompts.append(1) or None
    digest = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    assert app._run_package_pin_action("LABEL.F5_HOLD", "F5:" + digest) is False
    assert prompts == [1]
    assert not app._admin_pin_store.path.exists()
    assert journal.path.exists() and server.verify_calls == 0


def test_offline_park_audit_failure_leaves_original_in_place(tmp_path, monkeypatch):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    server.offline = True
    digest = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    assert app._run_package_pin_action("LABEL.F5_HOLD", "F5:" + digest) is False
    monkeypatch.setattr(app.package_outbox, "audit_pin_attempt",
                        lambda **_kwargs: (_ for _ in ()).throw(OSError("audit unavailable")))
    assert app._park_unverified_pin_item("F5:" + digest) is False
    assert journal.path.exists() and app.package_outbox.get_label_exchange_hold(digest) is None


@pytest.mark.parametrize("linked", [False, True])
def test_offline_park_releases_active_set_and_linked_f5(tmp_path, monkeypatch, linked):
    app, journal, current, server, set_id = _active_set_pin_app(
        tmp_path, monkeypatch, linked=linked
    )
    server.offline = True
    selected = ("F5:" + hashlib.sha256(journal.path.read_bytes()).hexdigest()
                if linked else set_id)
    action = "LABEL.F5_HOLD" if linked else "LABEL.SET_HOLD"
    assert app._run_package_pin_action(action, selected) is False
    assert app._park_unverified_pin_item(selected) is True
    assert not current.exists() and app.current_set_info["id"] is None
    assert not journal.path.exists()
    assert app._workflow_blocking_notice is None
    assert app.package_outbox.get_workbench_hold(set_id) is not None
    with app.package_outbox._connect() as conn:
        rows = conn.execute("SELECT observed FROM package_workbench_hold_audit "
                            "WHERE operation_key=? AND action='PIN_ATTEMPT'",
                            (_intent(app)[0],)).fetchall()
    assert any(row[0] == "UNVERIFIED_HOLD_COMPLETE:PIN_UNAVAILABLE" for row in rows)


def test_offline_park_resumes_f5_after_hold_row(tmp_path, monkeypatch):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    server.offline = True
    digest = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    assert app._run_package_pin_action("LABEL.F5_HOLD", "F5:" + digest) is False
    original = app_module.replace_checked
    interrupted = True

    def fail_once(*args, **kwargs):
        nonlocal interrupted
        if interrupted:
            interrupted = False
            raise OSError("interrupted after durable hold")
        return original(*args, **kwargs)

    monkeypatch.setattr(app_module, "replace_checked", fail_once)
    assert app._park_unverified_pin_item("F5:" + digest) is False
    assert journal.path.exists()
    assert app._park_unverified_pin_item("F5:" + digest) is True
    assert not journal.path.exists()


def test_offline_park_damaged_current_file_releases_slot(tmp_path, monkeypatch):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    journal.path.unlink()
    current = tmp_path / "current.json"
    current.write_bytes(b"{broken-current-file")
    app.data_manager = SimpleNamespace(
        save_directory=str(tmp_path), _current_state_filename=lambda: "current.json",
        load_current_state=lambda: None, delete_current_state=lambda: current.unlink(missing_ok=True),
    )
    app._load_current_set_state = lambda: None
    server.offline = True
    digest = hashlib.sha256(current.read_bytes()).hexdigest()
    selected = "CURRENT:" + digest
    assert app._run_package_pin_action("LABEL.SET_HOLD", selected) is False
    assert app._park_unverified_pin_item(selected) is True
    assert not current.exists()
    assert current.with_name(current.name + ".held-" + digest).read_bytes() == b"{broken-current-file"


def test_offline_park_missing_source_does_not_report_success(tmp_path, monkeypatch):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    server.offline = True
    digest = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    selected = "F5:" + digest
    assert app._run_package_pin_action("LABEL.F5_HOLD", selected) is False
    journal.path.unlink()
    assert app._park_unverified_pin_item(selected) is False
    with app.package_outbox._connect() as conn:
        count = conn.execute("SELECT COUNT(*) FROM package_workbench_hold_audit "
                             "WHERE observed='UNVERIFIED_HOLD_COMPLETE:PIN_UNAVAILABLE'").fetchone()[0]
    assert count == 0


def test_operator_change_after_redemption_applies_zero(tmp_path, monkeypatch):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    original = server.__call__

    def changing(path, raw, headers):
        answer = original(path, raw, headers)
        if path.endswith("/redemptions"):
            app.worker_name = "다른 작업자"
            app._admin_pin_login_epoch = uuid.uuid4().hex
        return answer

    app._admin_pin_client.transport = changing
    digest = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    assert app._run_package_pin_action("LABEL.F5_HOLD", "F5:" + digest) is False
    assert journal.path.exists() and app.package_outbox.get_label_exchange_hold(digest) is None
    assert _intent(app)[2:] == ("UNVERIFIED_OPERATOR_HOLD", "TARGET_CHANGED")


def test_incomplete_status_200_keeps_consumed_intent_unknown(tmp_path, monkeypatch):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    digest = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    target, source_sha = app._package_pin_target("LABEL.F5_HOLD", "F5:" + digest)
    operator = app._package_pin_operator()
    server.issued = {"action": "LABEL.F5_HOLD", "target": target, "operator": operator}
    server.consumed = True
    server.incomplete = "status"
    app._admin_pin_store.prepare(
        operation_key="resume-key", verification_id=VERIFICATION_ID,
        action="LABEL.F5_HOLD", target=target, operator=operator,
        admin_id="admin-personal", source_evidence_sha256=source_sha,
    )
    result = app._run_package_pin_action("LABEL.F5_HOLD", "F5:" + digest)
    assert "불확실" in result and result == app._admin_pin_last_message
    assert journal.path.exists() and app.package_outbox.get_label_exchange_hold(digest) is None


def test_consumed_missing_target_closes_by_pin_handoff_as_unknown(tmp_path, monkeypatch):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    digest = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    set_id = "F5:" + digest
    target, source_sha = app._package_pin_target("LABEL.F5_HOLD", set_id)
    original_journal = journal.path.read_bytes()
    operator = app._package_pin_operator()
    app._admin_pin_store.prepare(
        operation_key="missing-original-key", verification_id=VERIFICATION_ID,
        action="LABEL.F5_HOLD", target=target, operator=operator,
        admin_id="admin-personal", source_evidence_sha256=source_sha,
    )
    server.consumed = True
    journal.path.unlink()
    assert any(row["set_id"] == set_id for row in app._package_recovery_candidates())
    applied = []
    original_apply = app._package_pin_apply

    def only_handoff(action, set_id, physical_qr=""):
        applied.append(action)
        assert action == "LABEL.SUPPORT_HANDOFF"
        return original_apply(action, set_id, physical_qr)

    monkeypatch.setattr(app, "_package_pin_apply", only_handoff)
    result = app._run_package_pin_action("LABEL.SUPPORT_HANDOFF", set_id)
    assert "UNKNOWN" in result and applied == ["LABEL.SUPPORT_HANDOFF"]
    assert server.status_calls == 1
    assert app.package_outbox.get_label_exchange_hold(digest) is None
    with sqlite3.connect(app._admin_pin_store.path) as conn:
        records = conn.execute("SELECT operation_key,state,error_code FROM admin_pin_intents ORDER BY rowid").fetchall()
    assert records[0] == ("missing-original-key", "HANDED_OFF_UNKNOWN", "NO_KEYED_LEDGER_EFFECT")
    assert records[1][1] == "APPLIED"
    assert app.package_outbox.pin_handoff_for("missing-original-key") is True
    app._admin_pin_store = AdminPinIntentStore(tmp_path / "admin-pin-intents.sqlite3")
    assert all(row["set_id"] != set_id for row in app._package_recovery_candidates())
    journal.path.write_bytes(original_journal)
    restored = next(row for row in app._package_recovery_candidates() if row["set_id"] == set_id)
    assert restored["prior_pin_unknown"] is True


def test_consumed_changed_target_stays_unknown_until_pin_handoff(tmp_path, monkeypatch):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    digest = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    set_id = "F5:" + digest
    original, source_sha = app._package_pin_target("LABEL.F5_HOLD", set_id)
    operator = app._package_pin_operator()
    app._admin_pin_store.prepare(
        operation_key="changed-original-key", verification_id=VERIFICATION_ID,
        action="LABEL.F5_HOLD", target=original, operator=operator,
        admin_id="admin-personal", source_evidence_sha256=source_sha,
    )
    server.consumed = True
    actual_target = app._package_pin_target

    def changed(action, selected_id, physical_qr=""):
        target, evidence = actual_target(action, selected_id, physical_qr)
        if action == "LABEL.F5_HOLD":
            target = dict(target, state_fingerprint="0" * 64)
        return target, evidence

    monkeypatch.setattr(app, "_package_pin_target", changed)
    assert "바뀌었습니다" in app._run_package_pin_action("LABEL.F5_HOLD", set_id)
    assert _intent(app)[2] == "UNVERIFIED_OPERATOR_HOLD"
    assert journal.path.exists() and app.package_outbox.get_label_exchange_hold(digest) is None
    assert "UNKNOWN" in app._run_package_pin_action("LABEL.SUPPORT_HANDOFF", set_id)
    assert _intent(app)[2] == "HANDED_OFF_UNKNOWN"
    assert journal.path.exists() and app.package_outbox.get_label_exchange_hold(digest) is None


def test_handoff_effect_before_applied_closes_both_intents_on_retry(tmp_path, monkeypatch):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    digest = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    set_id = "F5:" + digest
    original, source_sha = app._package_pin_target("LABEL.F5_HOLD", set_id)
    app._admin_pin_store.prepare(
        operation_key="lost-original-key", verification_id=VERIFICATION_ID,
        action="LABEL.F5_HOLD", target=original, operator=app._package_pin_operator(),
        admin_id="admin-personal", source_evidence_sha256=source_sha,
    )
    server.consumed = True
    journal.path.unlink()
    transition = app._admin_pin_store.transition
    interrupted = True

    def fail_once(key, state, *, error_code=""):
        nonlocal interrupted
        if state == "APPLIED" and key != "lost-original-key" and interrupted:
            interrupted = False
            raise OSError("interrupted after handoff audit")
        return transition(key, state, error_code=error_code)

    monkeypatch.setattr(app._admin_pin_store, "transition", fail_once)
    assert app._run_package_pin_action("LABEL.SUPPORT_HANDOFF", set_id) is False
    assert app.package_outbox.pin_handoff_for("lost-original-key") is True
    assert "UNKNOWN" in app._run_package_pin_action("LABEL.SUPPORT_HANDOFF", set_id)
    with sqlite3.connect(app._admin_pin_store.path) as conn:
        rows = conn.execute("SELECT state FROM admin_pin_intents ORDER BY rowid").fetchall()
    assert rows == [("HANDED_OFF_UNKNOWN",), ("APPLIED",)]
    with app.package_outbox._connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM package_workbench_hold_audit WHERE action='HANDOFF'").fetchone()[0] == 1


def test_consumed_intent_from_prior_login_epoch_resumes_same_key(tmp_path, monkeypatch):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    digest = hashlib.sha256(journal.path.read_bytes()).hexdigest()
    set_id = "F5:" + digest
    original, source_sha = app._package_pin_target("LABEL.F5_HOLD", set_id)
    app._admin_pin_store.prepare(
        operation_key="prior-session-key", verification_id=VERIFICATION_ID,
        action="LABEL.F5_HOLD", target=original, operator=app._package_pin_operator(),
        admin_id="admin-personal", source_evidence_sha256=source_sha,
    )
    server.consumed = True
    app._admin_pin_login_epoch = uuid.uuid4().hex
    app._admin_pin_store = AdminPinIntentStore(tmp_path / "admin-pin-intents.sqlite3")
    assert app._run_package_pin_action("LABEL.F5_HOLD", set_id) is True
    assert _intent(app)[2] == "APPLIED"
    assert not journal.path.exists()
    with app.package_outbox._connect() as conn:
        row = conn.execute("SELECT operator_name FROM package_workbench_hold_audit "
                           "WHERE operation_key='prior-session-key' "
                           "AND observed='RESUMED:SESSION_CHANGED'").fetchone()
    assert row[0] == app.worker_name


@pytest.mark.parametrize("linked", [False, True])
def test_partial_hold_resumes_after_new_login_and_store_reopen(tmp_path, monkeypatch, linked):
    app, journal, current, server, set_id = _active_set_pin_app(
        tmp_path, monkeypatch, linked=linked
    )
    action = "LABEL.F5_HOLD" if linked else "LABEL.SET_HOLD"
    selected = ("F5:" + hashlib.sha256(journal.path.read_bytes()).hexdigest()
                if linked else set_id)
    original_delete = app.data_manager.delete_current_state
    interrupted = True

    def fail_once():
        nonlocal interrupted
        if interrupted:
            interrupted = False
            raise OSError("interrupted after hold row")
        original_delete()

    app.data_manager.delete_current_state = fail_once
    assert app._run_package_pin_action(action, selected) is False
    key = _intent(app)[0]
    assert app.package_outbox.get_workbench_hold(set_id) is not None
    app._admin_pin_login_epoch = uuid.uuid4().hex
    app._admin_pin_store = AdminPinIntentStore(tmp_path / "admin-pin-intents.sqlite3")
    assert app._run_package_pin_action(action, selected) is True
    assert _intent(app)[0] == key and _intent(app)[2] == "APPLIED"
    assert server.redeem_calls == server.verify_calls == 1
    assert not current.exists() and app.current_set_info["id"] is None


def test_valid_current_file_only_change_is_target_changed(tmp_path, monkeypatch):
    app, _journal, current, server, set_id = _active_set_pin_app(
        tmp_path, monkeypatch, linked=False
    )
    original = server.__call__

    def change_file(path, raw, headers):
        answer = original(path, raw, headers)
        if path.endswith("/redemptions"):
            state = json.loads(current.read_text(encoding="utf-8"))
            current.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
            assert app._read_package_recovery_file(current)["verified"]
        return answer

    app._admin_pin_client.transport = change_file
    assert app._run_package_pin_action("LABEL.SET_HOLD", set_id) is False
    assert _intent(app)[2:] == ("UNVERIFIED_OPERATOR_HOLD", "TARGET_CHANGED")
    assert current.exists() and app.current_set_info["id"] == set_id
    assert app.package_outbox.get_workbench_hold(set_id) is None


def test_valid_current_file_change_after_own_hold_row_remains_target_changed(tmp_path, monkeypatch):
    app, _journal, current, server, set_id = _active_set_pin_app(
        tmp_path, monkeypatch, linked=False
    )
    original_delete = app.data_manager.delete_current_state
    interrupted = True

    def fail_once():
        nonlocal interrupted
        if interrupted:
            interrupted = False
            raise OSError("interrupted after hold row")
        original_delete()

    app.data_manager.delete_current_state = fail_once
    assert app._run_package_pin_action("LABEL.SET_HOLD", set_id) is False
    original = current.read_bytes()
    current.write_text(json.dumps(json.loads(original), ensure_ascii=False, indent=2), encoding="utf-8")
    assert app._read_package_recovery_file(current)["verified"]
    assert "바뀌었습니다" in app._run_package_pin_action("LABEL.SET_HOLD", set_id)
    assert _intent(app)[2:] == ("UNVERIFIED_OPERATOR_HOLD", "TARGET_CHANGED")
    assert current.exists() and app.current_set_info["id"] == set_id
    assert server.redeem_calls == 1


def test_set_hold_uses_pin_and_preserves_damaged_current_file(tmp_path, monkeypatch):
    app, _journal, current = _storage_recovery_app(tmp_path, monkeypatch)
    del app._package_recovery_manager
    app.save_directory = str(tmp_path)
    app.worker_name = "홍길동"
    app._admin_pin_login_epoch = uuid.uuid4().hex
    app._admin_pin_store = AdminPinIntentStore(tmp_path / "admin-pin-intents.sqlite3")
    app._prompt_package_admin_pin = lambda: ("admin-personal", PIN)
    server = SignedPinServer()
    app._admin_pin_client = AdminPinClient(SimpleNamespace(
        producer_id="producer-label", key_id="key-label", secret="test-secret",
        endpoint_url="https://example.invalid/api/producer-ingest",
    ), transport=server)
    current.write_bytes(b"{broken-current-file")
    evidence = app._read_package_recovery_file(current)
    set_id = "CURRENT:" + evidence["sha256"]
    assert app._run_package_pin_action("LABEL.SET_HOLD", set_id) is True
    held = app.package_outbox.get_workbench_hold(set_id)
    assert held is not None and held["held_by"] == "admin-personal"
    assert current.with_name(current.name + ".held-" + evidence["sha256"]).exists()
    assert not current.exists()
    assert app._package_pin_effect({
        "action": "LABEL.SET_HOLD", "target_id": set_id,
        "operation_key": _intent(app)[0], "verification_id": VERIFICATION_ID,
        "operator_id": operator_local_id("홍길동"),
    })


def test_recheck_handoff_and_physical_lookup_each_have_pin_effect(tmp_path, monkeypatch):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    source = ("PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-F5|CLC=AAA2270730100|"
              "LBL=LBL-F5|HSH=0123456789abcdef")
    journal.save({
        "workflow_mode": "SINGLE", "status": "PREPARED", "set_id": "",
        "exchange_id": "EX-F5", "authority_scope_id": "SCOPE-F5",
        "prepare_idempotency_key": "KEY-F5",
    })
    digest = app._label_recovery_hold_id()
    set_id = "F5:" + digest
    assert app._run_package_pin_action("LABEL.F5_HOLD", set_id) is True
    app.phs_label_exchange_coordinator.client = SimpleNamespace(
        config=SimpleNamespace(authority_scope_id="SCOPE-F5"),
        resolve_active_phs_label=lambda *_args, **_kwargs: {
            "input_tag": {"qr_payload": source},
        },
        get_phs_label_exchange=lambda *_args, **_kwargs: {
            "exchange": {"exchange_id": "EX-F5", "state": "PREPARED",
                         "operation_key": _operation_key("KEY-F5")},
            "source_labels": [{
                "qr_payload": source, "label_id": "LBL-F5",
                "scan_anchor_input_tag_id": "ITG-F5",
            }],
        },
    )
    for action, physical in (("LABEL.RECHECK", ""),
                             ("LABEL.PHS_LOOKUP", source),
                             ("LABEL.SUPPORT_HANDOFF", "")):
        server.issued = None
        server.consumed = False
        result = app._run_package_pin_action(action, set_id, physical)
        assert result
        with app.package_outbox._connect() as conn:
            assert conn.execute("""SELECT COUNT(*) FROM package_workbench_hold_audit
                WHERE pin_action=? AND verification_id=? AND operation_key!=''
                  AND action!='PIN_ATTEMPT'""", (action, VERIFICATION_ID)).fetchone()[0] >= 1
    assert app._label_recovery_source_is_held(source) is True


@pytest.mark.parametrize("name", ["worker-12", "홍길동", "가" * 63])
def test_operator_mapping_is_web_safe(name):
    local_id = operator_local_id(name)
    assert re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,127}", local_id)
    assert len(local_id) <= 128


def test_operator_mapping_normalizes_nfc():
    assert operator_local_id("가") == operator_local_id("\u1100\u1161")

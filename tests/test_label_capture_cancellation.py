"""Normal F1 must close only its own unsubmitted capture, before clearing UI."""

import copy
import json
import sqlite3
from dataclasses import replace
from types import SimpleNamespace

import pytest

import Label_Match as label_module
from deferred_intent_capture import DeferredIntentCaptureError
from tests.test_deferred_intent_capture import (
    _b1_local_lease, _capture, _claim_and_plan, _row, _source_evidence, _store,
)


def _validated(store, *, set_id="SET-F1", scan="PHS2-MEASURED"):
    captured = _capture(store, set_id=set_id, scan=scan)
    claim = store.claim_validation(
        captured.intent_id, worker_id="cancel-test-validator",
        now="2026-08-29T01:00:00Z",
    )
    checked = store.verify_local_integrity(claim, now="2026-08-29T01:00:00Z")
    store.plan_label_validation(checked, now="2026-08-29T01:00:00Z")
    store.finish_validation(
        checked, step_id="label-package-source", outcome="VALID",
        reason_code="ORDERED_LABEL_VALIDATION_VALID",
        evidence=_source_evidence(set_id=set_id, scan=scan),
        expires_at=claim.claim_expires_at, now="2026-08-29T01:00:01Z",
    )
    return captured


def _app(store, captured, *, scan="PHS2-MEASURED"):
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app.tk = SimpleNamespace()
    app.current_set_info = {
        "id": _row(store.db_path, captured.intent_id)["local_work_identity"],
        "deferred_intent_id": captured.intent_id, "raw": [scan],
        "physical_scanned_qr_payload": scan, "parsed": ["ITEM-LABEL-1"],
        "central_inherit_all": True,
    }
    app.deferred_intent_capture = store
    app.worker_name = "Cancellation test operator"
    app.run_tests = True
    app.is_blinking = False
    app.initialized_successfully = False
    app.package_outbox = None
    app.history_tree = SimpleNamespace(exists=lambda _key: False)
    app.progress_bar = {}
    app.events = []
    app.data_manager = SimpleNamespace(
        log_event=lambda event, value: app.events.append((event, value)),
        delete_current_state=lambda: app.events.append(("delete-state", None)),
    )
    # Presentation-only dependencies; real reset and capture store remain in use.
    app._clear_workflow_completion = lambda: None
    app._advance_ui_lane_generation = lambda: app.events.append(("generation", None))
    app._block_active_history_load_action = lambda _action: False
    app._block_view_only_action = lambda _action: False
    return app


def _cancel(store, captured, *, row_version=None, set_id=None, scan="PHS2-MEASURED"):
    row = _row(store.db_path, captured.intent_id)
    return store.cancel_unsubmitted(
        intent_id=captured.intent_id,
        local_work_identity=set_id or row["local_work_identity"],
        physical_qr_payload=scan, operator_id="Cancellation test operator",
        expected_row_version=row["row_version"] if row_version is None else row_version,
    )


def test_normal_read_only_f1_cancels_before_ui_clear_and_unblocks_next_source(tmp_path):
    db_path, _outbox, store = _store(tmp_path)
    first = _validated(store)
    before = _row(db_path, first.intent_id)
    app = _app(store, first)
    app.current_set_info["private_state"] = {"seal_token": "fixture-private-cancellation-seal"}

    def log_event(event, value):
        assert _row(db_path, first.intent_id)["state"] == "CANCELLED"
        assert event == app.Events.SET_CANCELLED
        assert value == {"set_id": before["local_work_identity"]}
        app.events.append((event, copy.deepcopy(value)))

    app.data_manager.log_event = log_event
    assert app._reset_current_set(full_reset=True) is True
    after = _row(db_path, first.intent_id)
    assert after["state"] == "CANCELLED"
    for field in ("payload_ciphertext", "authenticated_seal", "payload_hash", "validation_snapshot_hash"):
        assert after[field] == before[field]
    assert not app.current_set_info["id"]
    assert store.next_materialization_candidate() is None
    assert _cancel(store, first)["row_version"] == after["row_version"]
    second = _capture(store, set_id="SET-NEXT", scan="PHS2-NEXT")
    assert store.next_validation_candidate(now="2026-08-29T01:01:00Z") == second.intent_id
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM package_command_outbox").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM deferred_intent_transition_audit WHERE intent_id=? AND transition_code='TC_CANCEL'", (first.intent_id,)).fetchone()[0] == 1


@pytest.mark.parametrize("bad", ["set", "payload", "producer", "scope", "row-version"])
def test_cancellation_refuses_wrong_identity_and_stale_cas(tmp_path, bad):
    db_path, _outbox, store = _store(tmp_path)
    captured = _validated(store)
    before = _row(db_path, captured.intent_id)
    kwargs = {}
    if bad == "set":
        kwargs["set_id"] = "ANOTHER-SET"
    elif bad == "payload":
        kwargs["scan"] = "ANOTHER-PHYSICAL-INPUT"
    elif bad == "producer":
        store.binding = replace(store.binding, producer_install_id="another-install")
    elif bad == "scope":
        store.binding = replace(store.binding, authority_scope_id="ANOTHER-SCOPE")
    else:
        kwargs["row_version"] = before["row_version"] - 1
    with pytest.raises(DeferredIntentCaptureError):
        _cancel(store, captured, **kwargs)
    assert _row(db_path, captured.intent_id) == before


def test_confirmed_prefetch_survives_local_cancellation_and_next_source(tmp_path):
    db_path, _outbox, store = _store(tmp_path)
    captured = _validated(store)
    snapshot = {
        "bundle_id": "TRANSFER-LEGACY", "authority_scope_id": "SCOPE-LABEL-MEASURED",
        "ledger_plane": "SHADOW_CANDIDATE", "plane_epoch": 1,
    }
    lease_store, lease = _b1_local_lease(
        db_path, "PHS2-MEASURED", snapshot,
        lease_id="preserved-legacy-prefetch", item_id="ITEM-LABEL-1", label_id="LBL-LEGACY",
        source_host_id=store.binding.source_host_id,
        expires_at="2026-08-29T01:05:00Z",  # Expiry does not release this reservation.
    )
    lease_store.attach_set(lease_id=lease["lease_id"], set_id="SET-F1")
    app = _app(store, captured)
    app.package_operation_lease_store = lease_store
    app.current_set_info.update(operation_lease_id=lease["lease_id"], operation_lease_fence=4)
    before_state = copy.deepcopy(app.current_set_info)
    before_intent = _row(db_path, captured.intent_id)
    with sqlite3.connect(db_path) as conn:
        before_leases = conn.execute("SELECT * FROM package_operation_leases").fetchall()
        before_attempts = conn.execute("SELECT * FROM package_operation_lease_issue_attempts").fetchall()
    assert app._reset_current_set(full_reset=True) is True
    assert not app.current_set_info["id"]
    after_intent = _row(db_path, captured.intent_id)
    assert after_intent["state"] == "CANCELLED"
    assert after_intent["payload_ciphertext"] == before_intent["payload_ciphertext"]
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT * FROM package_operation_leases").fetchall() == before_leases
        assert conn.execute("SELECT * FROM package_operation_lease_issue_attempts").fetchall() == before_attempts
    second = _capture(store, set_id="SET-OTHER-SOURCE", scan="PHS2-OTHER-SOURCE")
    assert store.next_validation_candidate() == second.intent_id
    assert app._operation_lease_blocks_f4(before_state) is True


def test_pending_package_ownership_is_not_cancelled(tmp_path):
    db_path, _outbox, store = _store(tmp_path)
    captured = _validated(store)
    app = _app(store, captured)
    app.package_outbox = SimpleNamespace(get_by_set_id=lambda _set: {"status": "PENDING"})
    before = _row(db_path, captured.intent_id)
    assert app._reset_current_set(full_reset=True) is False
    assert _row(db_path, captured.intent_id) == before
    assert app.events == []


@pytest.mark.parametrize("pending", ["f3-preflight", "f4-exchange", "f5-exchange", "in-flight"])
def test_pending_business_and_ui_guards_keep_capture_and_visible_set(tmp_path, pending):
    db_path, _outbox, store = _store(tmp_path)
    captured = _validated(store)
    app = _app(store, captured)
    if pending == "f3-preflight":
        app._central_package_preflight_in_progress = True
    elif pending == "f4-exchange":
        app._current_sealed_transfer_exchange_attempt = lambda: SimpleNamespace(
            status="PREPARED", seal_verification_status="PENDING", local_apply_status="PENDING",
        )
    elif pending == "f5-exchange":
        app._phs_label_exchange_pending = True
    else:
        app.is_blinking = True
    before = _row(db_path, captured.intent_id)
    before_state = copy.deepcopy(app.current_set_info)
    assert app._reset_current_set(full_reset=True) is False
    assert _row(db_path, captured.intent_id) == before
    assert app.current_set_info == before_state
    assert app.events == []


def test_committed_exchange_intent_blocks_cancellation_inside_transaction(tmp_path):
    from sealed_transfer_exchange import SealedTransferExchangeStore

    db_path, _outbox, store = _store(tmp_path)
    captured = _validated(store)
    exchange = SealedTransferExchangeStore(db_path)
    prepared = exchange.prepare(
        set_id="SET-F1", target_bundle_id="TRANSFER-LABEL-MEASURED",
        item_id="ITEM-LABEL-1", authority_scope_id=store.binding.authority_scope_id,
        operator="Cancellation test operator", old_seal_qr_payload="TEST-OLD-SEAL",
        old_seal_fields={}, old_barcodes=["OLD-ONE"], new_barcodes=["NEW-ONE"],
    )
    before = _row(db_path, captured.intent_id)
    with pytest.raises(DeferredIntentCaptureError, match="product exchange result"):
        _cancel(store, captured)
    assert _row(db_path, captured.intent_id) == before
    assert dict(exchange.load(prepared["intent_id"])) == dict(prepared)


def test_cancellation_commit_refusal_keeps_intent_and_visible_set(tmp_path):
    db_path, _outbox, store = _store(tmp_path)
    captured = _validated(store)
    app = _app(store, captured)
    before = _row(db_path, captured.intent_id)
    before_state = copy.deepcopy(app.current_set_info)
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TRIGGER refuse_cancel_audit BEFORE INSERT ON deferred_intent_transition_audit WHEN NEW.transition_code='TC_CANCEL' BEGIN SELECT RAISE(ABORT,'simulated durable cancellation failure'); END")
    assert app._reset_current_set(full_reset=True) is False
    assert _row(db_path, captured.intent_id) == before
    assert app.current_set_info == before_state
    assert app.events == []


def test_normal_existing_capture_needs_no_rework_foreground_marker(tmp_path):
    db_path, _outbox, store = _store(tmp_path)
    captured = _validated(store)
    before = _row(db_path, captured.intent_id)
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("SELECT worker_id FROM deferred_intent_transition_audit WHERE intent_id=? AND transition_code='T1_CAPTURE'", (captured.intent_id,)).fetchone()[0] is None
    assert _cancel(store, captured)["state"] == "CANCELLED"
    assert _row(db_path, captured.intent_id)["payload_ciphertext"] == before["payload_ciphertext"]


def test_live_validation_claim_is_not_cancelled(tmp_path):
    db_path, _outbox, store = _store(tmp_path)
    captured = _capture(store)
    store.claim_validation(captured.intent_id, worker_id="live-validator")
    before = _row(db_path, captured.intent_id)
    with pytest.raises(DeferredIntentCaptureError):
        _cancel(store, captured)
    assert _row(db_path, captured.intent_id) == before


def test_crash_after_cancellation_commit_does_not_restore_saved_work(tmp_path):
    db_path, _outbox, store = _store(tmp_path)
    captured = _validated(store)
    app = _app(store, captured)
    state_path = tmp_path / "current-state.json"
    state_path.write_text(json.dumps({"current_set_info": app.current_set_info}), encoding="utf-8")
    _cancel(store, captured)  # Simulate crash before the normal cache deletion.
    app.current_set_info = {"id": None, "raw": [], "parsed": []}
    app.data_manager.load_current_state = lambda: json.loads(state_path.read_text(encoding="utf-8"))
    app.data_manager.delete_current_state = state_path.unlink
    app._migrate_restored_central_package_state = lambda _value: pytest.fail("cancelled work reached restoration")
    app._load_current_set_state()
    assert not state_path.exists()
    assert not app.current_set_info["id"]
    assert _row(db_path, captured.intent_id)["state"] == "CANCELLED"
    assert store.next_materialization_candidate() is None


def test_late_validated_result_cannot_rematerialize_cancelled_capture(tmp_path):
    _db_path, _outbox, store = _store(tmp_path)
    captured = _validated(store)
    app = _app(store, captured)
    _cancel(store, captured)
    app.current_set_info = {"id": None, "raw": [], "parsed": []}
    app._deferred_label_materialization = {"intent_id": captured.intent_id}
    app._accept_resolved_central_phs2_scan = lambda *_a, **_k: pytest.fail("cancelled result applied")
    result = SimpleNamespace(state="VALIDATED", intent_id=captured.intent_id)
    assert app._materialize_validated_deferred_label(result) is False
    assert not app.current_set_info["id"]


def test_normal_initialization_upgrades_existing_state_edge_guard(tmp_path):
    _db_path, _outbox, store = _store(tmp_path)
    captured = _validated(store)
    with sqlite3.connect(store.db_path) as conn:
        original = conn.execute("SELECT sql FROM sqlite_master WHERE type='trigger' AND name='trg_deferred_intent_state_edge_guard'").fetchone()[0]
        old = original.replace("'OPERATOR_REVIEW','SUPERSEDED','CANCELLED'", "'OPERATOR_REVIEW','SUPERSEDED'")
        assert old != original
        conn.execute("DROP TRIGGER trg_deferred_intent_state_edge_guard")
        conn.execute(old)
    with pytest.raises(DeferredIntentCaptureError):
        _cancel(store, captured)
    assert _row(store.db_path, captured.intent_id)["state"] == "VALIDATED"
    store.initialize()
    assert _cancel(store, captured)["state"] == "CANCELLED"


def test_unknown_issue_attempt_keeps_visible_work_and_fifo(tmp_path):
    db_path, _outbox, store = _store(tmp_path)
    captured = _validated(store)
    from terminal_operation_lease import OperationLeaseStore
    lease_store = OperationLeaseStore(db_path)
    lease_store.reserve_issue_attempt("a" * 64)
    before = _row(db_path, captured.intent_id)
    with pytest.raises(DeferredIntentCaptureError):
        store.cancel_unsubmitted(
            intent_id=captured.intent_id, local_work_identity="SET-F1",
            physical_qr_payload="PHS2-MEASURED", operator_id="operator",
            expected_row_version=before["row_version"], issue_request_fingerprint="a" * 64,
        )
    assert _row(db_path, captured.intent_id) == before
    assert lease_store.get_issue_attempt(request_fingerprint="a" * 64)["lease_id"] is None


@pytest.mark.parametrize("issue_outcome", ["verified", "unknown"])
def test_legacy_validation_issue_is_distinguished_from_unknown_commit(tmp_path, issue_outcome):
    db_path, _outbox, store = _store(tmp_path)
    captured = _capture(store, set_id="SET-LEGACY-ISSUE")
    claim = _claim_and_plan(store, captured.intent_id)
    store.record_validation_step_valid(
        claim, step_id="label-package-source",
        evidence=_source_evidence(set_id="SET-LEGACY-ISSUE"), now="2026-08-29T01:00:01Z",
    )
    dispatch = store.record_validation_mutation_attempt(
        claim, step_id="label-operation-lease", now="2026-08-29T01:00:02Z",
    )
    if issue_outcome == "unknown":
        store.finish_validation(
            claim, step_id="label-operation-lease", outcome="UNKNOWN_COMMIT",
            reason_code="OPERATION_LEASE_COMMIT_UNKNOWN",
            evidence={"contract_version": "label-validation-evidence-v1", **dispatch},
            now="2026-08-29T01:00:03Z",
        )
        before = _row(db_path, captured.intent_id)
        with pytest.raises(DeferredIntentCaptureError):
            _cancel(store, captured)
        assert _row(db_path, captured.intent_id) == before
        return
    snapshot = {
        "bundle_id": "TRANSFER-LABEL-MEASURED", "authority_scope_id": "SCOPE-LABEL-MEASURED",
        "ledger_plane": "SHADOW_CANDIDATE", "plane_epoch": 1,
    }
    lease_store, lease = _b1_local_lease(
        db_path, "PHS2-MEASURED", snapshot, lease_id="lease-legacy-verified",
        item_id="ITEM-LABEL-1", label_id="LBL-MEASURED",
        issue_idempotency_key=dispatch["idempotency_key"],
        source_host_id=store.binding.source_host_id,
        expires_at="2026-08-29T01:05:00Z",
    )
    lease_store.attach_set(lease_id=lease["lease_id"], set_id="SET-LEGACY-ISSUE")
    evidence = {
        "contract_version": "label-validation-evidence-v1", "authority_scope_id": "SCOPE-LABEL-MEASURED",
        "authority_epoch": 1, "ledger_plane": "SHADOW_CANDIDATE", "plane_epoch": 1,
        "operation": "CREATE_PACKAGE", **{key: value for key, value in lease.items() if key != "resource_id"},
        "physical_qr_sha256": _source_evidence()["physical_qr_sha256"],
        "observed_at": "2026-08-29T01:00:03Z",
    }
    store.finish_validation(
        claim, step_id="label-operation-lease", outcome="VALID",
        reason_code="ORDERED_LABEL_VALIDATION_VALID", evidence=evidence,
        issued_at=lease["issued_at"], expires_at=lease["expires_at"], now="2026-08-29T01:00:03Z",
    )
    with sqlite3.connect(db_path) as conn:
        before = {table: conn.execute(f"SELECT * FROM {table}").fetchall() for table in
                  ("package_operation_leases", "package_operation_lease_issue_attempts", "deferred_intent_validation_steps")}
    app = _app(store, captured)
    app.current_set_info.update(operation_lease_id=lease["lease_id"], operation_lease_fence=4)
    assert app._reset_current_set(full_reset=True) is True
    assert _row(db_path, captured.intent_id)["state"] == "CANCELLED"
    with sqlite3.connect(db_path) as conn:
        assert {table: conn.execute(f"SELECT * FROM {table}").fetchall() for table in before} == before

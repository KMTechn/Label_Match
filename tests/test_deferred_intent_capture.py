from __future__ import annotations

import hashlib
import hmac
import json
import os
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

import Label_Match as label_module
from deferred_intent_capture import (
    CAPTURE_C14N_VERSION,
    CAPTURE_SCHEMA_VERSION,
    CONTRACT_VERSION,
    DeferredIntentBinding,
    DeferredIntentCaptureError,
    DeferredIntentCaptureStore,
    DeferredValidationClaim,
    DeferredValidationResult,
    append_transition_audit,
    canonical_json_bytes,
)
from package_logistics import (
    PackageApiError,
    PackageCommandDraft,
    PackageOutbox,
    PackageTransportError,
)
from terminal_operation_lease import OperationLeaseError


CONTRACT_PATH = Path(
    r"E:\KMTech\autoloop-20260824\seq292-intent-contract\CONTRACT.md"
)
VECTORS_PATH = Path(
    r"E:\KMTech\autoloop-20260824\seq292-intent-contract\golden-vectors.json"
)
CONTRACT_SHA256 = "cbb9f75ab42fdeaebe10e78e7d27656663c6b72bd42c8c05b39304ce9f904bcf"
VECTORS_SHA256 = "c74de654e457cea8b771b3833b20d9e7513352398a55cdfe5d425c1b2ffc2971"


def _binding() -> DeferredIntentBinding:
    return DeferredIntentBinding(
        producer_id="producer-label-measured",
        producer_install_id="install-label-measured",
        source_host_id="host-label-measured",
        manifest_hash="a" * 64,
        authority_scope_id="SCOPE-LABEL-MEASURED",
    )


def _protect(cleartext: bytes) -> bytes:
    return b"T1" + bytes(value ^ 0xA5 for value in cleartext)


def _unprotect(ciphertext: bytes) -> bytes:
    value = bytes(ciphertext)
    if not value.startswith(b"T1"):
        raise ValueError("invalid test protection envelope")
    return bytes(item ^ 0xA5 for item in value[2:])


def _store(tmp_path: Path, *, max_pending_intents: int = 10_000):
    db_path = tmp_path / "package_logistics_outbox.sqlite3"
    outbox = PackageOutbox(db_path)
    store = DeferredIntentCaptureStore(
        db_path,
        _binding(),
        protect_bytes=_protect,
        unprotect_bytes=_unprotect,
        max_pending_intents=max_pending_intents,
        initialize_schema=False,
    )
    return db_path, outbox, store


def _capture(store, *, set_id="1787940225728641500", scan="PHS2-MEASURED"):
    return store.capture_label_package_source(
        local_work_identity=set_id,
        physical_qr_payload=scan,
        item_code="ITEM-LABEL-1",
    )


def _draft(set_id: str) -> PackageCommandDraft:
    return PackageCommandDraft.build(
        set_id=set_id,
        item_code="ITEM-LABEL-1",
        source_bundle_id="TRANSFER-LABEL-1",
        source_external_label="SOURCE-LABEL-1",
        source_authority_scope_id="SCOPE-LABEL-MEASURED",
        package_bundle_id="PACKAGE-LABEL-1",
        external_label="PACKAGE-EXTERNAL-LABEL-1",
        membership_mode="INHERIT_ALL",
    )


def _row(db_path: Path, intent_id: str):
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        found = conn.execute(
            "SELECT * FROM deferred_intents WHERE intent_id=? LIMIT 1",
            (intent_id,),
        ).fetchone()
        return dict(found) if found else None


def _count(conn, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def test_label_binding_reads_only_secret_free_enrollment_artifacts(tmp_path):
    local_app_data = tmp_path / "LocalAppData"
    direct_root = local_app_data / "KMTech" / "DirectSync" / "label_match"
    status_root = direct_root / "status"
    status_root.mkdir(parents=True)
    identity = {
        "producer_id": "producer-label-capture",
        "producer_install_id": "install-label-capture",
        "source_host_id": "host-label-capture",
    }
    manifest = {
        "contract_version": "producer-manifest-v1",
        "pc_identity": {
            "producer_install_id": identity["producer_install_id"],
            "source_host_id": identity["source_host_id"],
        },
    }
    manifest_hash = label_module.direct_sync_manifest_hash(manifest)
    (direct_root / "producer_identity.json").write_text(
        json.dumps(identity), encoding="utf-8"
    )
    (direct_root / "producer_manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    (status_root / "label_match_worker_pc_registration.json").write_text(
        json.dumps(
            {
                "producer_id": identity["producer_id"],
                "manifest_hash": manifest_hash,
                "server_registration_verified": True,
                "manifest_hash_verified": True,
                "persisted_manifest_hash_verified": True,
            }
        ),
        encoding="utf-8",
    )
    client = SimpleNamespace(
        config=SimpleNamespace(
            source_host_id=identity["source_host_id"],
            authority_scope_id="SCOPE-LABEL-CAPTURE",
        )
    )
    binding = label_module._label_match_deferred_intent_binding(
        client,
        environ={"LOCALAPPDATA": str(local_app_data)},
    )
    assert binding == DeferredIntentBinding(
        producer_id=identity["producer_id"],
        producer_install_id=identity["producer_install_id"],
        source_host_id=identity["source_host_id"],
        manifest_hash=manifest_hash,
        authority_scope_id="SCOPE-LABEL-CAPTURE",
    )
    assert not (direct_root / "credential.json").exists()
    assert not (
        local_app_data
        / "KMTech"
        / "Logistics"
        / "profiles"
        / "Label_Match"
        / "runtime-profile.json"
    ).exists()


def test_frozen_contract_and_35_golden_vectors_are_exact():
    assert hashlib.sha256(CONTRACT_PATH.read_bytes()).hexdigest() == CONTRACT_SHA256
    assert hashlib.sha256(VECTORS_PATH.read_bytes()).hexdigest() == VECTORS_SHA256
    vectors = json.loads(VECTORS_PATH.read_text(encoding="utf-8"))
    assert vectors["contract_version"] == CONTRACT_VERSION
    assert len(vectors["states"]) == 17
    assert len(vectors["transition_codes"]) == 26
    assert len(vectors["cases"]) == 35
    assert {
        "MEASURED_LABEL_CLOSED_PORT_CAPTURE_PRESERVES_SCAN",
        "CAPTURE_ONLY_NO_AUTHORITATIVE_LEAK",
        "CAPTURE_ONLY_ONLINE_LEGACY_HANDOFF_PREVENTS_FUTURE_DUPLICATE",
        "CAPTURE_ONLY_ROW_IS_FORWARD_COMPATIBLE_WITH_FULL_ENGINE",
        "CAPTURE_DISK_FULL_NEVER_REPORTS_SAVED",
    }.issubset({case["id"] for case in vectors["cases"]})


def _execute_frozen_vector(case, vectors):
    states = set(vectors["states"])
    operations = set(vectors["operation_vocabulary"])
    counters = dict(vectors["default_effect_counters"])
    initial = dict(case.get("initial") or {})
    records = {}
    for item in initial.get("intents") or ():
        records[item["target"]] = {
            "state": item["state"],
            "partition_key": item["partition_key"],
            "partition_seq": int(item["partition_seq"]),
            "fence": int(item.get("fence") or 0),
            "command_hash": item.get("command_hash"),
        }
    if not records:
        records["main"] = {
            "state": initial.get("state"),
            "partition_key": "P1",
            "partition_seq": 1,
            "fence": int(initial.get("fence") or 0),
            "command_hash": initial.get("command_hash"),
        }
    if initial.get("intents"):
        counters["captured_intent_rows"] = len(initial["intents"])
    elif initial.get("state") is not None:
        counters["captured_intent_rows"] = 1
    for key in counters:
        if key in initial:
            counters[key] = int(initial[key])
    carry = {
        key: initial[key]
        for key in ("inventory_quantity", "completion_count", "downstream_release_count")
        if key in initial
    }
    command_present = bool(initial.get("command_hash"))

    validation_destinations = vectors["validation_outcome_destinations"]
    reconciliation_destinations = vectors["reconciliation_outcome_destinations"]
    known_outcomes = set(validation_destinations)
    known_outcomes.update(
        outcome
        for phase in reconciliation_destinations.values()
        for outcome in phase
    )
    known_outcomes.update(
        {"APPLIED", "COMMITTED_EXACT", "CRASHED_BEFORE_COMMIT", "SQLITE_FULL"}
    )

    def has_blocking_predecessor(target):
        record = records[target]
        terminal = set(vectors["terminal_states"])
        return any(
            other_target != target
            and other["partition_key"] == record["partition_key"]
            and other["partition_seq"] < record["partition_seq"]
            and other["state"] not in terminal
            for other_target, other in records.items()
        )

    def accepted_for(action, current, target):
        operation = action["op"]
        if operation == "claim_validation":
            return (
                current
                in {"CAPTURED_UNVERIFIED", "RETRY_WAIT_VALIDATION", "WAITING_DEPENDENCY"}
                and not has_blocking_predecessor(target)
            )
        if operation == "materialization_dispatch":
            return current == "VALIDATING"
        if operation == "claim_submit":
            return current in {"READY_TO_SUBMIT", "RETRY_WAIT_SUBMIT"}
        if operation == "attempt_transition":
            return False
        if operation == "cancel":
            return current in {
                "CAPTURED_UNVERIFIED",
                "RETRY_WAIT_VALIDATION",
                "WAITING_DEPENDENCY",
                "BLOCKED_INVALID",
            }
        return True

    def destination_for(action, current, accepted, record):
        operation = action["op"]
        if not accepted:
            return current
        if operation == "capture_commit":
            return "CAPTURED_UNVERIFIED"
        if operation in {"capture_duplicate", "capture_failure", "restart"}:
            return current
        if operation == "capture_conflict":
            return "BLOCKED_INVALID"
        if operation == "claim_validation":
            return "VALIDATING"
        if operation == "validation_result":
            if (
                action.get("recomputed_command_hash")
                and record.get("command_hash")
                and action["recomputed_command_hash"] != record["command_hash"]
            ):
                return "OPERATOR_REVIEW"
            return validation_destinations[action["outcome"]].split(":", 1)[0]
        if operation in {"materialization_dispatch", "materialization_readback"}:
            return current
        if operation == "materialize_command":
            return "READY_TO_SUBMIT"
        if operation == "claim_submit":
            return "SUBMITTING"
        if operation == "submit_result":
            return {
                "UNKNOWN_COMMIT": "RECONCILE_PENDING_SUBMIT",
                "COMMITTED_EXACT": "ACKED",
            }[action["outcome"]]
        if operation == "reconciliation_readback":
            return reconciliation_destinations[action["phase"]][
                action["outcome"]
            ].split(":", 1)[0]
        if operation == "apply_local_effect":
            return (
                "COMPLETED"
                if action["outcome"] == "APPLIED"
                else "LOCAL_EFFECT_PENDING"
            )
        if operation == "legacy_handoff":
            return "SUPERSEDED"
        if operation == "cancel":
            return "CANCELLED"
        if operation == "crash":
            return {
                "after_begin_before_intent_insert": None,
                "after_dispatch_before_step_receipt_persist": "RECONCILE_PENDING_VALIDATION",
                "after_http_200_before_receipt_commit": "RECONCILE_PENDING_SUBMIT",
            }.get(action.get("cut_point"), current)
        if operation == "expire_claim":
            return "RETRY_WAIT_VALIDATION"
        if operation == "attempt_transition":
            return current
        raise AssertionError(f"unhandled frozen operation: {operation}")

    def ui_status_for(action, state):
        if action["op"] in {"capture_failure"} or (
            action["op"] == "crash" and state is None
        ):
            return "저장 실패—다시 스캔 필요"
        return {
            "CAPTURED_UNVERIFIED": "저장됨-검증대기",
            "VALIDATING": "저장됨-검증대기",
            "RETRY_WAIT_VALIDATION": "저장됨-검증대기",
            "WAITING_DEPENDENCY": "저장됨-선행조건대기",
            "READY_TO_SUBMIT": "검증완료-전송대기",
            "RETRY_WAIT_SUBMIT": "검증완료-전송대기",
            "RECONCILE_PENDING_VALIDATION": "결과확인중",
            "RECONCILE_PENDING_SUBMIT": "결과확인중",
            "BLOCKED_INVALID": "관리자확인",
            "OPERATOR_REVIEW": "관리자확인",
            "ACKED": "서버확정-로컬반영대기",
            "LOCAL_EFFECT_PENDING": "서버확정-로컬반영대기",
            "COMPLETED": "완료",
            "CANCELLED": "종결-미완료",
            "SUPERSEDED": "종결-미완료",
        }.get(state)

    assert case["actions"]
    for action in case["actions"]:
        operation = action["op"]
        assert operation in operations
        if "outcome" in action:
            assert action["outcome"] in known_outcomes
        target = action.get("target") or "main"
        record = records[target]
        current = record["state"]
        accepted = accepted_for(action, current, target)
        destination = destination_for(action, current, accepted, record)
        expected = dict(action.get("expect") or {})
        assert destination == expected.get("state")
        assert destination is None or destination in states
        if current is not None and destination != current:
            assert destination in vectors["allowed_edges"][current]
        if "accepted" in expected:
            assert accepted is expected["accepted"]

        if operation == "claim_validation" and accepted:
            record["fence"] += 1
        if "fence" in expected:
            assert record["fence"] == expected["fence"]
        if operation == "materialize_command":
            command_present = True
        record["state"] = destination

        delta = dict(expected.get("effects_delta") or {})
        assert set(delta).issubset(counters)
        for key, value in delta.items():
            counters[key] += int(value)

        observed_flags = {
            "audit_reason": (
                "DUPLICATE_CAPTURE_SUPPRESSED"
                if operation == "capture_duplicate"
                else "LEGACY_PATH_OWNS_SUBMISSION"
                if operation == "legacy_handoff"
                else None
            ),
            "capture_schema_version": CAPTURE_SCHEMA_VERSION,
            "command_hash_unchanged": True,
            "command_present": command_present,
            "contract_version": CONTRACT_VERSION,
            "downstream_outbox_present": counters["downstream_outbox_rows"] > 0,
            "memory_only_fallback": False,
            "not_found_authorization": False,
            "original_ciphertext_unchanged": True,
            "returns_existing_intent_id": True,
            "same_idempotency_key_required": True,
            "same_intent_id": True,
            "same_payload_hash": True,
            "same_step_same_idempotency_key_retry_authorized": True,
            "schema_rewrite_count": 0,
            "stale_worker_update_accepted": False,
            "step_status": (
                "MATERIALIZATION_REQUIRED"
                if action.get("outcome") == "ABSENT_MATERIALIZABLE"
                else "VERIFIED"
                if operation in {"materialization_readback", "reconciliation_readback"}
                and action.get("outcome") == "FOUND_EXACT"
                else None
            ),
            "ui_status": ui_status_for(action, destination),
        }
        error_code = None
        if operation == "capture_conflict":
            error_code = "DUPLICATE_IDENTITY_PAYLOAD_MISMATCH"
        elif operation == "claim_validation" and not accepted:
            error_code = (
                "PREDECESSOR_NOT_TERMINAL"
                if has_blocking_predecessor(target)
                else "TERMINAL_INTENT_NOT_CLAIMABLE"
            )
        elif operation in {"materialization_dispatch", "claim_submit"} and not accepted:
            error_code = "UNKNOWN_COMMIT_REQUIRES_EXACT_READBACK"
        elif operation == "attempt_transition":
            error_code = "FORBIDDEN_EDGE_VALIDATION_NOT_PROVEN"
        elif operation == "cancel" and not accepted:
            error_code = "UNKNOWN_COMMIT_MUST_RECONCILE"
        elif operation == "validation_result" and destination == "OPERATOR_REVIEW" and action.get("recomputed_command_hash"):
            error_code = "IMMUTABLE_COMMAND_BINDING_CHANGED"
        observed_flags["error_code"] = error_code
        for key, value in expected.items():
            if key in {"state", "accepted", "effects_delta", "fence"}:
                continue
            assert observed_flags[key] == value

    main_state = records.get("main", {}).get("state")
    provenance = dict(case.get("provenance") or {})
    observed_final = {
        **counters,
        **carry,
        "state": main_state,
        "captured_input_recoverable": (
            counters["captured_intent_rows"] > 0
            and main_state not in set(vectors["terminal_states"])
        ),
        "schema_rewrite_count": 0,
        "exact_receipt_id": provenance.get("receipt_id"),
        "exact_event_id": provenance.get("event_id"),
        "exact_transfer_id": provenance.get("transfer_id"),
    }
    for key, value in case.get("final_expect", {}).items():
        assert observed_final[key] == value


@pytest.mark.parametrize(
    "case",
    json.loads(VECTORS_PATH.read_text(encoding="utf-8"))["cases"],
    ids=lambda case: case["id"],
)
def test_each_frozen_golden_vector_executes_contract_model(case):
    vectors = json.loads(VECTORS_PATH.read_text(encoding="utf-8"))
    _execute_frozen_vector(case, vectors)


def test_package_outbox_installs_exact_final_v1_schema_and_guards(tmp_path):
    db_path, _outbox, _store_instance = _store(tmp_path)
    expected_columns = {
        "deferred_intents": (
            "intent_id", "contract_version", "app_id", "intent_kind", "state",
            "producer_id", "producer_install_id", "source_host_id", "manifest_hash",
            "authority_scope_id", "authority_epoch", "partition_key", "partition_seq",
            "local_work_identity", "capture_key", "capture_schema_version",
            "capture_c14n_version", "payload_protection", "payload_ciphertext",
            "payload_hash", "binding_hash", "authenticated_seal", "seal_key_ref",
            "validation_generation", "validation_snapshot_hash", "validation_expires_at",
            "command_json", "command_hash", "command_bound_snapshot_hash",
            "server_idempotency_key", "receipt_json", "receipt_hash",
            "downstream_outbox_ref", "local_effect_state", "next_attempt_at",
            "validation_attempt_count", "submit_attempt_count", "claim_owner",
            "claim_expires_at", "fence", "row_version", "last_reason_code",
            "last_error_code", "supersedes_intent_id", "created_at", "updated_at",
        ),
        "deferred_intent_validation_steps": (
            "validation_step_row_id", "intent_id", "validation_generation",
            "step_ordinal", "step_id", "step_kind", "step_effect",
            "validator_contract", "validator_version", "status", "idempotency_key",
            "request_json", "request_hash", "validation_outcome",
            "reconciliation_outcome", "evidence_json", "evidence_hash", "receipt_json",
            "receipt_hash", "issued_at", "expires_at", "attempt_count", "fence",
            "last_error_code", "created_at", "updated_at",
        ),
        "deferred_intent_transition_audit": (
            "audit_row_id", "intent_id", "audit_seq", "from_state", "to_state",
            "transition_code", "reason_code", "validation_outcome",
            "reconciliation_outcome", "attempt_no", "worker_id", "fence",
            "occurred_at", "evidence_hash", "prev_audit_hash", "audit_hash",
        ),
    }
    with sqlite3.connect(db_path) as conn:
        for table, columns in expected_columns.items():
            actual = tuple(row[1] for row in conn.execute(f"PRAGMA table_info({table})"))
            assert actual == columns
            table_sql = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()[0]
            assert table_sql.rstrip().endswith("STRICT")
        trigger_rows = conn.execute(
            "SELECT name,tbl_name,sql FROM sqlite_master WHERE type='trigger'"
        ).fetchall()
        trigger_names = {row[0] for row in trigger_rows}
        assert {
            "trg_deferred_intent_audit_no_update",
            "trg_deferred_intent_audit_no_delete",
            "trg_deferred_intent_capture_immutable",
            "trg_deferred_intent_command_immutable",
            "trg_deferred_intent_receipt_immutable",
            "trg_deferred_intent_state_edge_guard",
        }.issubset(trigger_names)
        deferred_referencing_triggers = [
            row for row in trigger_rows if "deferred_intent" in str(row[2]).lower()
        ]
        assert all(str(row[1]).startswith("deferred_intent") for row in deferred_referencing_triggers)


def test_measured_closed_port_capture_is_durable_encrypted_and_no_effect(tmp_path):
    db_path, _outbox, store = _store(tmp_path)
    domain_tables = (
        "package_command_outbox",
        "package_cancellation_outbox",
        "package_replacement_waiting_outbox",
        "package_post_review_outbox",
    )
    with sqlite3.connect(db_path) as conn:
        before = {table: _count(conn, table) for table in domain_tables}
    result = _capture(store)
    assert result.state == "CAPTURED_UNVERIFIED"
    assert result.pending_count == 1
    assert result.oldest_age_seconds == 0
    row = _row(db_path, result.intent_id)
    assert row["local_work_identity"] == "1787940225728641500"
    assert row["contract_version"] == CONTRACT_VERSION
    assert row["capture_schema_version"] == 1
    assert row["capture_c14n_version"] == CAPTURE_C14N_VERSION
    assert row["payload_protection"] == "WIN_DPAPI_CURRENT_USER_V1"
    assert row["validation_snapshot_hash"] is None
    assert row["command_json"] is None
    assert row["receipt_json"] is None
    assert row["downstream_outbox_ref"] is None
    assert b"PHS2-MEASURED" not in bytes(row["payload_ciphertext"])
    payload = json.loads(_unprotect(bytes(row["payload_ciphertext"])))
    assert payload["physical_qr_payload"] == "PHS2-MEASURED"
    assert hashlib.sha256(canonical_json_bytes(payload)).hexdigest() == row["payload_hash"]
    with sqlite3.connect(db_path) as conn:
        after = {table: _count(conn, table) for table in domain_tables}
        assert after == before
        assert _count(conn, "deferred_intent_validation_steps") == 0
        audit = conn.execute(
            """SELECT audit_seq,from_state,to_state,transition_code,reason_code
                 FROM deferred_intent_transition_audit WHERE intent_id=?""",
            (result.intent_id,),
        ).fetchone()
        assert audit == (
            1,
            None,
            "CAPTURED_UNVERIFIED",
            "T1_CAPTURE",
            "CAPTURE_COMMITTED_BEFORE_REMOTE",
        )
    seal_key = _unprotect(store.seal_key_path.read_bytes())
    seal_binding = {
        "contract_version": CONTRACT_VERSION,
        "app_id": "label",
        "intent_kind": "LABEL_PACKAGE_SOURCE",
        "producer_id": row["producer_id"],
        "producer_install_id": row["producer_install_id"],
        "source_host_id": row["source_host_id"],
        "manifest_hash": row["manifest_hash"],
        "authority_scope_id": row["authority_scope_id"],
        "capture_key": row["capture_key"],
        "intent_id": row["intent_id"],
        "payload_hash": row["payload_hash"],
        "capture_schema_version": row["capture_schema_version"],
        "partition_seq": row["partition_seq"],
    }
    binding_bytes = canonical_json_bytes(seal_binding)
    assert hashlib.sha256(binding_bytes).hexdigest() == row["binding_hash"]
    assert hmac.compare_digest(
        bytes(row["authenticated_seal"]),
        hmac.new(seal_key, binding_bytes, hashlib.sha256).digest(),
    )


@pytest.mark.skipif(os.name != "nt", reason="current-user DPAPI is Windows-only")
def test_real_current_user_dpapi_roundtrip_contains_no_plaintext_row(tmp_path):
    db_path = tmp_path / "package_logistics_outbox.sqlite3"
    PackageOutbox(db_path)
    store = DeferredIntentCaptureStore(
        db_path,
        _binding(),
        initialize_schema=False,
    )
    result = _capture(
        store,
        set_id="SET-REAL-DPAPI",
        scan="PHS2-REAL-DPAPI",
    )
    row = _row(db_path, result.intent_id)
    ciphertext = bytes(row["payload_ciphertext"])
    assert b"PHS2-REAL-DPAPI" not in ciphertext
    decoded = json.loads(store._dpapi_unprotect_bytes(ciphertext))
    assert decoded["physical_qr_payload"] == "PHS2-REAL-DPAPI"
    assert len(store._dpapi_unprotect_bytes(store.seal_key_path.read_bytes())) == 32


def test_duplicate_same_payload_converges_and_audit_chain_is_bound(tmp_path):
    db_path, _outbox, store = _store(tmp_path)
    first = _capture(store)
    second = _capture(store)
    assert second.intent_id == first.intent_id
    assert second.duplicate is True
    with sqlite3.connect(db_path) as conn:
        assert _count(conn, "deferred_intents") == 1
        audits = conn.execute(
            """SELECT audit_seq,reason_code,prev_audit_hash,audit_hash
                 FROM deferred_intent_transition_audit
                WHERE intent_id=? ORDER BY audit_seq""",
            (first.intent_id,),
        ).fetchall()
        assert [row[0] for row in audits] == [1, 2]
        assert audits[1][1] == "DUPLICATE_CAPTURE_SUPPRESSED"
        assert audits[1][2] == audits[0][3]


def test_duplicate_identity_different_payload_blocks_without_ciphertext_rewrite(tmp_path):
    db_path, _outbox, store = _store(tmp_path)
    first = _capture(store)
    before = bytes(_row(db_path, first.intent_id)["payload_ciphertext"])
    with pytest.raises(
        DeferredIntentCaptureError,
        match="same Label work identity",
    ) as blocked:
        _capture(store, scan="PHS2-DIFFERENT")
    assert blocked.value.code == "DUPLICATE_IDENTITY_PAYLOAD_MISMATCH"
    after = _row(db_path, first.intent_id)
    assert after["state"] == "BLOCKED_INVALID"
    assert bytes(after["payload_ciphertext"]) == before
    with sqlite3.connect(db_path) as conn:
        audit = conn.execute(
            """SELECT transition_code,reason_code,evidence_hash
                 FROM deferred_intent_transition_audit
                WHERE intent_id=? ORDER BY audit_seq DESC LIMIT 1""",
            (first.intent_id,),
        ).fetchone()
        assert audit[0:2] == (
            "T4_LOCAL_INVALID",
            "DUPLICATE_IDENTITY_PAYLOAD_MISMATCH",
        )
        assert len(audit[2]) == 64


def test_forbidden_captured_to_submit_family_edges_are_rejected(tmp_path):
    db_path, _outbox, store = _store(tmp_path)
    result = _capture(store)
    submit_family = (
        "READY_TO_SUBMIT",
        "SUBMITTING",
        "RETRY_WAIT_SUBMIT",
        "RECONCILE_PENDING_SUBMIT",
        "ACKED",
        "LOCAL_EFFECT_PENDING",
        "COMPLETED",
    )
    for target in submit_family:
        with sqlite3.connect(db_path) as conn:
            with pytest.raises(sqlite3.IntegrityError, match="forbidden deferred intent state edge"):
                conn.execute(
                    "UPDATE deferred_intents SET state=? WHERE intent_id=?",
                    (target, result.intent_id),
                )
    assert _row(db_path, result.intent_id)["state"] == "CAPTURED_UNVERIFIED"


def test_capture_and_audit_identity_are_immutable_and_audit_append_only(tmp_path):
    db_path, _outbox, store = _store(tmp_path)
    result = _capture(store)
    with sqlite3.connect(db_path) as conn:
        with pytest.raises(sqlite3.IntegrityError, match="capture identity is immutable"):
            conn.execute(
                "UPDATE deferred_intents SET payload_hash=? WHERE intent_id=?",
                ("b" * 64, result.intent_id),
            )
        with pytest.raises(sqlite3.IntegrityError, match="audit is append-only"):
            conn.execute(
                "DELETE FROM deferred_intent_transition_audit WHERE intent_id=?",
                (result.intent_id,),
            )


def test_capture_audit_failure_rolls_back_intent_atomically(tmp_path):
    db_path, _outbox, store = _store(tmp_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """CREATE TRIGGER fail_capture_audit
               BEFORE INSERT ON deferred_intent_transition_audit
               BEGIN SELECT RAISE(ABORT, 'simulated audit failure'); END"""
        )
    with pytest.raises(DeferredIntentCaptureError) as failure:
        _capture(store)
    assert failure.value.code == "SQLITE_CAPTURE_FAILED"
    with sqlite3.connect(db_path) as conn:
        assert _count(conn, "deferred_intents") == 0
        assert _count(conn, "deferred_intent_transition_audit") == 0


def test_disk_full_failure_never_returns_saved_or_memory_fallback(tmp_path, monkeypatch):
    db_path, _outbox, store = _store(tmp_path)

    def full_connect():
        raise sqlite3.OperationalError("database or disk is full")

    monkeypatch.setattr(store, "_connect", full_connect)
    with pytest.raises(DeferredIntentCaptureError) as failure:
        _capture(store)
    assert failure.value.code == "SQLITE_CAPTURE_FAILED"
    with sqlite3.connect(db_path) as conn:
        assert _count(conn, "deferred_intents") == 0


def test_dpapi_or_seal_readback_failure_never_commits(tmp_path):
    db_path = tmp_path / "package_logistics_outbox.sqlite3"
    PackageOutbox(db_path)

    def broken_unprotect(_ciphertext):
        return b"wrong"

    store = DeferredIntentCaptureStore(
        db_path,
        _binding(),
        protect_bytes=_protect,
        unprotect_bytes=broken_unprotect,
        initialize_schema=False,
    )
    with pytest.raises(DeferredIntentCaptureError) as failure:
        _capture(store)
    assert failure.value.code in {
        "CAPTURE_SEAL_KEY_READBACK_FAILED",
        "CAPTURE_SEAL_KEY_UNAVAILABLE",
    }
    with sqlite3.connect(db_path) as conn:
        assert _count(conn, "deferred_intents") == 0


def test_quota_failure_does_not_prune_or_partially_capture(tmp_path):
    db_path, _outbox, store = _store(tmp_path, max_pending_intents=1)
    first = _capture(store, set_id="SET-QUOTA-1")
    with pytest.raises(DeferredIntentCaptureError) as failure:
        _capture(store, set_id="SET-QUOTA-2")
    assert failure.value.code == "CAPTURE_QUOTA_EXCEEDED"
    with sqlite3.connect(db_path) as conn:
        assert _count(conn, "deferred_intents") == 1
        assert conn.execute(
            "SELECT intent_id FROM deferred_intents LIMIT 1"
        ).fetchone()[0] == first.intent_id


def test_online_legacy_outbox_and_supersede_handoff_commit_together(tmp_path):
    db_path, outbox, store = _store(tmp_path)
    captured = _capture(store, set_id="SET-ONLINE-HANDOFF")
    queued = outbox.enqueue(
        _draft("SET-ONLINE-HANDOFF"),
        captured_intent_id=captured.intent_id,
    )
    exact_ref = f"package_command_outbox:{queued['idempotency_key']}"
    row = _row(db_path, captured.intent_id)
    assert row["state"] == "SUPERSEDED"
    assert row["last_reason_code"] == "LEGACY_PATH_OWNS_SUBMISSION"
    assert row["downstream_outbox_ref"] == exact_ref
    with sqlite3.connect(db_path) as conn:
        assert _count(conn, "package_command_outbox") == 1
        audit = conn.execute(
            """SELECT from_state,to_state,transition_code,reason_code,evidence_hash
                 FROM deferred_intent_transition_audit
                WHERE intent_id=? ORDER BY audit_seq DESC LIMIT 1""",
            (captured.intent_id,),
        ).fetchone()
        assert audit[0:4] == (
            "CAPTURED_UNVERIFIED",
            "SUPERSEDED",
            "TS_SUPERSEDE",
            "LEGACY_PATH_OWNS_SUBMISSION",
        )
        assert audit[4] == hashlib.sha256(exact_ref.encode()).hexdigest()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE deferred_intents SET state='VALIDATING' WHERE intent_id=?",
                (captured.intent_id,),
            )


def test_missing_capture_handoff_rolls_back_new_business_outbox_row(tmp_path):
    db_path, outbox, _store_instance = _store(tmp_path)
    with pytest.raises(DeferredIntentCaptureError) as failure:
        outbox.enqueue(
            _draft("SET-ATOMIC-ROLLBACK"),
            captured_intent_id="di_" + "f" * 64,
        )
    assert failure.value.code == "LEGACY_HANDOFF_INTENT_MISSING"
    with sqlite3.connect(db_path) as conn:
        assert _count(conn, "package_command_outbox") == 0


def test_existing_exact_outbox_can_repair_interrupted_handoff(tmp_path):
    db_path, outbox, store = _store(tmp_path)
    captured = _capture(store, set_id="SET-REPAIR-HANDOFF")
    queued = outbox.enqueue(_draft("SET-REPAIR-HANDOFF"))
    outbox.link_captured_intent_to_existing(
        captured_intent_id=captured.intent_id,
        set_id="SET-REPAIR-HANDOFF",
        idempotency_key=queued["idempotency_key"],
    )
    assert _row(db_path, captured.intent_id)["state"] == "SUPERSEDED"


def test_capture_only_row_is_forward_compatible_without_schema_rewrite(tmp_path):
    db_path, _outbox, store = _store(tmp_path)
    captured = _capture(store, set_id="SET-FUTURE-TAKEOVER")
    original_payload_hash = _row(db_path, captured.intent_id)["payload_hash"]
    before_sql = None
    with sqlite3.connect(db_path) as conn:
        before_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='deferred_intents'"
        ).fetchone()[0]
        now = "2026-08-29T00:00:00Z"
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """UPDATE deferred_intents
                  SET state='VALIDATING',claim_owner='future-v1',
                      claim_expires_at='2026-08-29T00:05:00Z',fence=1,
                      row_version=row_version+1,updated_at=? WHERE intent_id=?""",
            (now, captured.intent_id),
        )
        append_transition_audit(
            conn,
            intent_id=captured.intent_id,
            from_state="CAPTURED_UNVERIFIED",
            to_state="VALIDATING",
            transition_code="T2_CLAIM_VALIDATION",
            reason_code="FUTURE_V1_TAKEOVER",
            occurred_at=now,
        )
        conn.commit()
    PackageOutbox(db_path)
    after = _row(db_path, captured.intent_id)
    assert after["state"] == "VALIDATING"
    assert after["payload_hash"] == original_payload_hash
    with sqlite3.connect(db_path) as conn:
        after_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='deferred_intents'"
        ).fetchone()[0]
    assert after_sql == before_sql


def test_label_closed_port_flow_captures_before_remote_and_shows_pending(tmp_path):
    db_path, _outbox, store = _store(tmp_path)
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app.current_set_info = {
        "id": None,
        "raw": [],
        "parsed": [],
        "start_time": None,
        "error_count": 0,
        "has_error_or_reset": False,
    }
    app.deferred_intent_capture = store
    app._deferred_intent_capture_error = ""
    app.run_tests = True
    ordering = []
    original_capture = store.capture_label_package_source

    def capture_first(**kwargs):
        ordering.append("capture_commit")
        return original_capture(**kwargs)

    def closed_port(*_args, **_kwargs):
        ordering.append("remote_call")
        raise PackageTransportError("TCP connection refused 18458")

    store.capture_label_package_source = capture_first
    app._resolve_central_phs2_scan_overlay = closed_port
    assert app._begin_central_phs2_scan_overlay(
        "PHS2-CLOSED-PORT-18458", "ITEM-LABEL-1"
    ) is True
    assert ordering == ["capture_commit", "remote_call"]
    assert app._deferred_capture_ui["status"] == "저장됨-검증대기"
    assert app._deferred_capture_ui["central_check_pending"] is False
    assert app._deferred_capture_ui["operator_complete_signal"] is False
    notice = app._deferred_capture_pending_notice()
    assert notice.title == "저장됨-검증대기"
    assert app._deferred_capture_ui["intent_id"] in notice.message
    assert "대기 1건" in notice.message
    assert "중앙 연결 복구 후 검증됩니다." in notice.message
    assert app.current_set_info["raw"] == []
    assert app.current_set_info["parsed"] == []
    durable = _row(db_path, app.current_set_info["deferred_intent_id"])
    assert durable["state"] == "RETRY_WAIT_VALIDATION"
    assert durable["local_work_identity"] == app.current_set_info["id"]
    assert durable["next_attempt_at"] is not None


def test_label_capture_failure_stops_before_remote_and_reports_rescan(tmp_path):
    _db_path, _outbox, store = _store(tmp_path)
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app.current_set_info = {
        "id": None,
        "raw": [],
        "parsed": [],
        "start_time": None,
        "error_count": 0,
        "has_error_or_reset": False,
    }
    app.deferred_intent_capture = store
    app._deferred_intent_capture_error = ""
    app.run_tests = True
    calls = []

    def capture_failure(**_kwargs):
        raise DeferredIntentCaptureError(
            "SQLITE_CAPTURE_FAILED", "database or disk is full"
        )

    store.capture_label_package_source = capture_failure
    app._resolve_central_phs2_scan_overlay = lambda *_args: calls.append("remote")
    assert app._begin_central_phs2_scan_overlay(
        "PHS2-DISK-FULL", "ITEM-LABEL-1"
    ) is True
    assert calls == []
    assert app._deferred_capture_ui == {
        "status": "저장 실패—다시 스캔 필요",
        "error_code": "SQLITE_CAPTURE_FAILED",
        "operator_complete_signal": False,
    }
    failure_notice = app._deferred_capture_failure_notice()
    assert failure_notice.title == "저장 실패—다시 스캔 필요"
    assert failure_notice.tone == "danger"
    assert "같은 현품표를 다시 스캔하세요." in failure_notice.message


def _claim_and_plan(store, intent_id, *, worker="validator-1", now="2026-08-29T01:00:00Z"):
    claim = store.claim_validation(intent_id, worker_id=worker, now=now)
    assert isinstance(claim, DeferredValidationClaim)
    verified = store.verify_local_integrity(claim, now=now)
    assert isinstance(verified, DeferredValidationClaim)
    plan = store.plan_label_validation(verified, now=now)
    assert [step["step_id"] for step in plan] == [
        "label-package-source",
        "label-operation-lease",
    ]
    return verified


def _source_evidence():
    bundle_id = "TRANSFER-LABEL-MEASURED"
    return {
        "contract_version": "label-validation-evidence-v1",
        "authority_scope_id": "SCOPE-LABEL-MEASURED",
        "authority_epoch": 1,
        "ledger_plane": "SHADOW_CANDIDATE",
        "plane_epoch": 1,
        "source_resolution_basis": "SINGLE_TRANSFER",
        "bundle_id": bundle_id,
        "package_bundle_id": bundle_id,
        "entity_versions": {f"bundle:{bundle_id}": 7},
        "topology_hash": "a" * 64,
        "item_code": "ITEM-LABEL-1",
        "active_label_id": "LBL-MEASURED",
        "membership_hash": "d" * 64,
        "member_count": 4,
        "physical_qr_sha256": hashlib.sha256(
            b"PHS2-MEASURED"
        ).hexdigest(),
        "local_work_identity": "1787940225728641500",
        "observed_at": "2026-08-29T01:00:01Z",
    }


def test_validation_claim_integrity_plan_and_required_absent_are_fenced(tmp_path):
    db_path, _outbox, store = _store(tmp_path)
    captured = _capture(store)
    claim = _claim_and_plan(store, captured.intent_id)
    store.record_validation_step_valid(
        claim,
        step_id="label-package-source",
        evidence=_source_evidence(),
        now="2026-08-29T01:00:01Z",
    )
    dependency = {
        "contract_version": "label-validation-evidence-v1",
        "dependency": {
            "kind": "OPERATION_GRANT",
            "identity": "CREATE_PACKAGE@SCOPE-LABEL-MEASURED",
            "authority_scope_id": "SCOPE-LABEL-MEASURED",
            "operation": "CREATE_PACKAGE",
            "status": "PENDING",
        },
        "http_status": 403,
        "committed": False,
        "error_code": "OPERATION_LEASE_AUTHORIZATION_PENDING",
        "observed_at": "2026-08-29T01:00:02Z",
    }
    mutation = store.record_validation_mutation_attempt(
        claim,
        step_id="label-operation-lease",
        now="2026-08-29T01:00:01Z",
    )
    assert mutation["idempotency_key"].startswith("lease-issue-")
    assert len(mutation["request_hash"]) == 64
    assert mutation["request_hash"] == hashlib.sha256(
        canonical_json_bytes(
            {
                "authority_scope_id": "SCOPE-LABEL-MEASURED",
                "operation": "CREATE_PACKAGE",
                "scan_payload": "PHS2-MEASURED",
            }
        )
    ).hexdigest()
    result = store.finish_validation(
        claim,
        step_id="label-operation-lease",
        outcome="REQUIRED_ABSENT",
        reason_code="OPERATION_LEASE_AUTHORIZATION_PENDING",
        evidence=dependency,
        now="2026-08-29T01:00:02Z",
    )
    assert isinstance(result, DeferredValidationResult)
    assert result.intent_id == captured.intent_id
    assert result.state == "WAITING_DEPENDENCY"
    assert result.outcome == "REQUIRED_ABSENT"
    assert result.reason_code == "OPERATION_LEASE_AUTHORIZATION_PENDING"
    assert result.observed_at == "2026-08-29T01:00:02Z"
    assert result.dependency_kind == "OPERATION_GRANT"
    assert result.dependency_identity == (
        "CREATE_PACKAGE grant · SCOPE-LABEL-MEASURED · 승인 대기"
    )
    assert result.pending_count == 1
    assert result.oldest_age_seconds >= 0
    row = _row(db_path, captured.intent_id)
    assert row["next_attempt_at"] is None
    assert row["claim_owner"] is None
    assert row["claim_expires_at"] is None
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        steps = conn.execute(
            """SELECT step_id,status,validation_outcome
                 FROM deferred_intent_validation_steps
                WHERE intent_id=? ORDER BY step_ordinal""",
            (captured.intent_id,),
        ).fetchall()
        assert [tuple(step) for step in steps] == [
            ("label-package-source", "VERIFIED", "VALID"),
            ("label-operation-lease", "WAITING_DEPENDENCY", "REQUIRED_ABSENT"),
        ]
        audits = conn.execute(
            """SELECT transition_code,validation_outcome,worker_id,fence
                 FROM deferred_intent_transition_audit
                WHERE intent_id=? ORDER BY audit_seq""",
            (captured.intent_id,),
        ).fetchall()
        assert [audit[0] for audit in audits] == [
            "T1_CAPTURE",
            "T2_CLAIM_VALIDATION",
            "T3_LOCAL_INTEGRITY",
            "T5_VALIDATE_PLAN",
            "T5_VALIDATE_PLAN",
            "T5A_RECORD_MUTATION_ATTEMPT",
            "T7_CLASSIFY_ABSENCE",
            "T9_WAIT_DEPENDENCY",
        ]
        assert audits[-1][1:] == ("REQUIRED_ABSENT", "validator-1", 1)
        with pytest.raises(sqlite3.IntegrityError, match="forbidden deferred intent state edge"):
            conn.execute(
                "UPDATE deferred_intents SET state='ACKED' WHERE intent_id=?",
                (captured.intent_id,),
            )
    assert store.next_validation_candidate(now="2026-08-29T02:00:00Z") is None


def test_transport_retry_is_scheduled_and_distinct_from_dependency_wait(tmp_path):
    db_path, _outbox, store = _store(tmp_path)
    captured = _capture(store)
    claim = _claim_and_plan(store, captured.intent_id)
    result = store.finish_validation(
        claim,
        step_id="label-package-source",
        outcome="RETRYABLE_UNAVAILABLE",
        reason_code="PACKAGE_TRANSPORT_UNAVAILABLE",
        evidence={
            "transport": "UNAVAILABLE",
            "observed_at": "2026-08-29T01:00:03Z",
        },
        now="2026-08-29T01:00:03Z",
    )
    assert result.state == "RETRY_WAIT_VALIDATION"
    assert result.next_attempt_at == "2026-08-29T01:00:08Z"
    assert _row(db_path, captured.intent_id)["next_attempt_at"] == result.next_attempt_at


def test_local_integrity_failure_blocks_before_remote_plan(tmp_path):
    db_path, _outbox, store = _store(tmp_path)
    captured = _capture(store)
    claim = store.claim_validation(
        captured.intent_id,
        worker_id="validator-local-invalid",
        now="2026-08-29T01:00:00Z",
    )
    assert isinstance(claim, DeferredValidationClaim)
    store._unprotect_bytes = lambda _value: (_ for _ in ()).throw(ValueError("DPAPI"))
    result = store.verify_local_integrity(claim, now="2026-08-29T01:00:01Z")
    assert isinstance(result, DeferredValidationResult)
    assert result.state == "BLOCKED_INVALID"
    assert result.reason_code == "LOCAL_DPAPI_INVALID"
    with sqlite3.connect(db_path) as conn:
        assert _count(conn, "deferred_intent_validation_steps") == 0
        assert conn.execute(
            """SELECT transition_code FROM deferred_intent_transition_audit
                 WHERE intent_id=? ORDER BY audit_seq DESC LIMIT 1""",
            (captured.intent_id,),
        ).fetchone()[0] == "T4_LOCAL_INVALID"


def test_validation_fifo_and_stale_fence_reject_parallel_claims(tmp_path):
    db_path, _outbox, store = _store(tmp_path)
    first = _capture(store, set_id="SET-FIFO-1")
    second = _capture(store, set_id="SET-FIFO-2")
    assert store.claim_validation(
        second.intent_id,
        worker_id="validator-second",
        now="2026-08-29T01:00:00Z",
    ) is None
    first_claim = store.claim_validation(
        first.intent_id,
        worker_id="validator-first",
        now="2026-08-29T01:00:00Z",
    )
    assert isinstance(first_claim, DeferredValidationClaim)
    assert store.claim_validation(
        first.intent_id,
        worker_id="validator-racer",
        now="2026-08-29T01:00:01Z",
    ) is None
    expired = store.claim_validation(
        first.intent_id,
        worker_id="validator-recovery",
        now="2026-08-29T01:06:00Z",
    )
    assert isinstance(expired, DeferredValidationClaim)
    assert expired.fence == first_claim.fence + 1
    with pytest.raises(DeferredIntentCaptureError) as stale:
        store.verify_local_integrity(
            first_claim,
            now="2026-08-29T01:06:01Z",
        )
    assert stale.value.code == "VALIDATION_CLAIM_LOST"
    assert _row(db_path, second.intent_id)["state"] == "CAPTURED_UNVERIFIED"


def test_valid_freeze_requires_all_ordered_steps_and_has_no_command(tmp_path):
    db_path, _outbox, store = _store(tmp_path)
    captured = _capture(store)
    claim = _claim_and_plan(store, captured.intent_id)
    store.record_validation_step_valid(
        claim,
        step_id="label-package-source",
        evidence=_source_evidence(),
        now="2026-08-29T01:00:01Z",
    )
    result = store.finish_validation(
        claim,
        step_id="label-operation-lease",
        outcome="VALID",
        reason_code="ORDERED_VALIDATION_VALID",
        evidence={
            "contract_version": "label-validation-evidence-v1",
            "authority_epoch": 1,
            "authority_scope_id": "SCOPE-LABEL-MEASURED",
            "ledger_plane": "SHADOW_CANDIDATE",
            "plane_epoch": 1,
            "operation": "CREATE_PACKAGE",
            "lease_id": "lease-measured",
            "fence": 1,
            "snapshot_hash": "c" * 64,
            "status": "PREFETCHED",
            "physical_qr_sha256": hashlib.sha256(
                b"PHS2-MEASURED"
            ).hexdigest(),
            "issued_at": "2026-08-29T01:00:00Z",
            "expires_at": "2026-08-29T01:05:02Z",
            "observed_at": "2026-08-29T01:00:02Z",
        },
        issued_at="2026-08-29T01:00:00Z",
        expires_at="2026-08-29T01:05:02Z",
        now="2026-08-29T01:00:02Z",
    )
    assert result.state == "VALIDATED"
    row = _row(db_path, captured.intent_id)
    assert row["validation_snapshot_hash"] is not None
    assert row["validation_expires_at"] == "2026-08-29T01:05:02Z"
    assert row["command_json"] is None
    assert row["downstream_outbox_ref"] is None
    assert row["receipt_json"] is None


def test_valid_freeze_rejects_incomplete_authority_evidence(tmp_path):
    db_path, _outbox, store = _store(tmp_path)
    captured = _capture(store)
    claim = _claim_and_plan(store, captured.intent_id)
    incomplete = _source_evidence()
    incomplete.pop("authority_epoch")
    store.record_validation_step_valid(
        claim,
        step_id="label-package-source",
        evidence=incomplete,
        now="2026-08-29T01:00:01Z",
    )
    with pytest.raises(DeferredIntentCaptureError) as blocked:
        store.finish_validation(
            claim,
            step_id="label-operation-lease",
            outcome="VALID",
            reason_code="ORDERED_VALIDATION_VALID",
            evidence={
                "contract_version": "label-validation-evidence-v1",
                "authority_epoch": 1,
                "authority_scope_id": "SCOPE-LABEL-MEASURED",
                "ledger_plane": "SHADOW_CANDIDATE",
                "plane_epoch": 1,
                "operation": "CREATE_PACKAGE",
                "lease_id": "lease-measured",
                "fence": 1,
                "snapshot_hash": "c" * 64,
                "status": "PREFETCHED",
                "physical_qr_sha256": hashlib.sha256(
                    b"PHS2-MEASURED"
                ).hexdigest(),
                "issued_at": "2026-08-29T01:00:00Z",
                "expires_at": "2026-08-29T01:05:02Z",
                "observed_at": "2026-08-29T01:00:02Z",
            },
            issued_at="2026-08-29T01:00:00Z",
            expires_at="2026-08-29T01:05:02Z",
            now="2026-08-29T01:00:02Z",
        )
    assert blocked.value.code == "VALIDATION_EVIDENCE_INCOMPLETE"
    assert _row(db_path, captured.intent_id)["state"] == "VALIDATING"


def test_deferred_lease_verification_does_not_persist_signed_capability(
    monkeypatch,
):
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app.package_logistics_client = SimpleNamespace(config=SimpleNamespace())
    persisted = []
    app.package_operation_lease_store = SimpleNamespace(
        save_prefetched=lambda **kwargs: persisted.append(kwargs)
    )
    claims = {
        "lease_id": "lease-secret-free",
        "issued_at": "2026-08-29T01:00:00Z",
        "expires_at": "2026-08-29T01:05:00Z",
        "fence": 1,
        "snapshot_hash": "c" * 64,
    }
    app.package_operation_lease_keyring = SimpleNamespace(
        bootstrap_authenticated=lambda *_args, **_kwargs: None,
        verify=lambda *_args, **_kwargs: dict(claims),
    )
    snapshot = {
        "authority_scope_id": "SCOPE-LABEL-MEASURED",
        "authority_epoch": 1,
        "ledger_plane": "SHADOW_CANDIDATE",
        "plane_epoch": 1,
        "bundle_id": "TRANSFER-LABEL-MEASURED",
        "entity_version": 7,
    }
    app._central_phs2_response_parts = lambda *_args: (
        SimpleNamespace(),
        dict(snapshot),
        None,
    )
    monkeypatch.setattr(
        label_module,
        "_label_match_operation_lease_binding",
        lambda *_args: {"binding": "verified-by-test-keyring"},
    )
    monkeypatch.setattr(
        label_module,
        "normalize_issue_artifact",
        lambda _artifact: {
            "lease_id": claims["lease_id"],
            "kid": "kid-1",
            "expires_at": claims["expires_at"],
            "fence": claims["fence"],
            "snapshot_hash": claims["snapshot_hash"],
            "token": "signed-capability-must-remain-memory-only",
            "operation_snapshot": dict(snapshot),
            "keyring": {"keys": [{"kid": "kid-1"}]},
        },
    )
    _source, _snapshot, _sealed, lease = app._verify_operation_lease_artifact(
        physical_qr="PHS2-MEASURED",
        artifact={"server": "response"},
        issue_idempotency_key="lease-issue-" + "a" * 64,
        authenticated_online=True,
        persist_artifact=False,
        expected_snapshot=snapshot,
    )
    assert persisted == []
    assert lease == {
        "lease_id": "lease-secret-free",
        "fence": 1,
        "snapshot_hash": "c" * 64,
        "issued_at": "2026-08-29T01:00:00Z",
        "expires_at": "2026-08-29T01:05:00Z",
        "status": "PREFETCHED",
    }
    assert "signed-capability" not in json.dumps(lease, sort_keys=True)


@pytest.mark.parametrize(
    "evidence",
    [
        {"token": "must-not-persist"},
        {"nested": {"access_token": "must-not-persist"}},
        {"nested": {"password": "must-not-persist"}},
        {"innocent_name": "Bearer must-not-persist"},
        {
            "innocent_name": (
                "eyJhbGciOiJFUzI1NiJ9."
                + "a" * 40
                + "."
                + "b" * 86
            )
        },
    ],
)
def test_validation_evidence_rejects_secret_fields(tmp_path, evidence):
    _db_path, _outbox, store = _store(tmp_path)
    captured = _capture(store)
    claim = _claim_and_plan(store, captured.intent_id)
    with pytest.raises(DeferredIntentCaptureError) as blocked:
        store.finish_validation(
            claim,
            step_id="label-package-source",
            outcome="RETRYABLE_UNAVAILABLE",
            reason_code="TRANSPORT",
            evidence=evidence,
        )
    assert blocked.value.code == "VALIDATION_EVIDENCE_SECRET_FORBIDDEN"


@pytest.mark.parametrize(
    ("outcome", "expected_state"),
    [
        ("ABSENT_MATERIALIZABLE", "VALIDATING"),
        ("INVALID", "BLOCKED_INVALID"),
        ("CONFLICT", "OPERATOR_REVIEW"),
    ],
)
def test_remaining_typed_validation_outcomes_have_only_frozen_destinations(
    tmp_path, outcome, expected_state
):
    _db_path, _outbox, store = _store(tmp_path)
    captured = _capture(store)
    claim = _claim_and_plan(store, captured.intent_id)
    result = store.finish_validation(
        claim,
        step_id="label-package-source",
        outcome=outcome,
        reason_code=f"MEASURED_{outcome}",
        evidence={
            "step_id": "label-package-source",
            "classification": outcome,
            "observed_at": "2026-08-29T01:00:02Z",
        },
        now="2026-08-29T01:00:02Z",
    )
    assert result.state == expected_state


def test_mutating_validation_unknown_commit_requires_t5a_and_reconcile(tmp_path):
    db_path, _outbox, store = _store(tmp_path)
    captured = _capture(store)
    claim = _claim_and_plan(store, captured.intent_id)
    dispatch = store.record_validation_mutation_attempt(
        claim,
        step_id="label-operation-lease",
        now="2026-08-29T01:00:01Z",
    )
    result = store.finish_validation(
        claim,
        step_id="label-operation-lease",
        outcome="UNKNOWN_COMMIT",
        reason_code="OPERATION_LEASE_COMMIT_UNKNOWN",
        evidence={
            "contract_version": "label-validation-evidence-v1",
            "step_id": "label-operation-lease",
            "step_effect": "IDEMPOTENT_MUTATION",
            "idempotency_key": dispatch["idempotency_key"],
            "request_hash": dispatch["request_hash"],
            "step_attempt_no": dispatch["step_attempt_no"],
            "fence": dispatch["fence"],
            "dispatch_recorded_at": dispatch["recorded_at"],
            "commit_state": "UNKNOWN",
            "http_status": 0,
            "error_code": "OPERATION_LEASE_COMMIT_UNKNOWN",
            "observed_at": "2026-08-29T01:00:02Z",
        },
        now="2026-08-29T01:00:02Z",
    )
    assert result.state == "RECONCILE_PENDING_VALIDATION"
    assert _row(db_path, captured.intent_id)["next_attempt_at"] is None
    assert store.next_validation_candidate(now="2026-08-29T02:00:00Z") is None


def test_expired_claim_after_t5a_never_retries_mutation(tmp_path):
    db_path, _outbox, store = _store(tmp_path)
    captured = _capture(store)
    claim = _claim_and_plan(store, captured.intent_id)
    store.record_validation_mutation_attempt(
        claim,
        step_id="label-operation-lease",
        claim_seconds=1,
        now="2026-08-29T01:00:01Z",
    )
    assert store.next_validation_candidate(
        now="2026-08-29T01:00:03Z"
    ) == captured.intent_id
    assert store.claim_validation(
        captured.intent_id,
        worker_id="validator-after-crash",
        now="2026-08-29T01:00:03Z",
    ) is None
    row = _row(db_path, captured.intent_id)
    assert row["state"] == "RECONCILE_PENDING_VALIDATION"
    assert row["next_attempt_at"] is None
    assert row["validation_generation"] == claim.validation_generation
    with sqlite3.connect(db_path) as conn:
        step = conn.execute(
            """SELECT step_effect,status,idempotency_key,request_hash,
                      attempt_count
                 FROM deferred_intent_validation_steps
                WHERE intent_id=? AND step_id='label-operation-lease'
                LIMIT 1""",
            (captured.intent_id,),
        ).fetchone()
        assert step[0] == "IDEMPOTENT_MUTATION"
        assert step[1] == "REQUEST_RECORDED"
        assert str(step[2]).startswith("lease-issue-")
        assert len(str(step[3])) == 64
        assert step[4] == 1
        assert conn.execute(
            """SELECT transition_code
                 FROM deferred_intent_transition_audit
                WHERE intent_id=? ORDER BY audit_seq DESC LIMIT 1""",
            (captured.intent_id,),
        ).fetchone()[0] == "T6A_VALIDATION_UNKNOWN"


def test_mutating_transport_is_unknown_but_read_only_transport_retries():
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app.package_logistics_client = SimpleNamespace(
        config=SimpleNamespace(authority_scope_id="SCOPE-LABEL-MEASURED")
    )
    error = PackageTransportError("connection reset after dispatch")
    dispatch = {
        "idempotency_key": "lease-issue-" + "a" * 64,
        "request_hash": "b" * 64,
        "step_attempt_no": 1,
        "fence": 2,
        "recorded_at": "2026-08-29T01:00:01Z",
    }
    mutating = app._classify_deferred_validation_error(
        error,
        step_id="label-operation-lease",
        dispatch_record=dispatch,
    )
    response_failure = RuntimeError("local public-key pin write failed")
    response_failure.deferred_mutation_response_received = True
    post_response = app._classify_deferred_validation_error(
        response_failure,
        step_id="label-operation-lease",
        dispatch_record=dispatch,
    )
    read_only = app._classify_deferred_validation_error(
        error,
        step_id="label-package-source",
    )
    assert mutating["outcome"] == "UNKNOWN_COMMIT"
    assert mutating["retry_after_seconds"] is None
    assert mutating["evidence"]["request_hash"] == "b" * 64
    assert post_response["outcome"] == "UNKNOWN_COMMIT"
    assert post_response["reason_code"] == "OPERATION_LEASE_RESULT_NOT_DURABLE"
    assert read_only["outcome"] == "RETRYABLE_UNAVAILABLE"


def test_pending_grant_requires_exact_mutating_step_and_definite_noncommit():
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app.package_logistics_client = SimpleNamespace(
        config=SimpleNamespace(authority_scope_id="SCOPE-LABEL-MEASURED")
    )
    dispatch = {
        "idempotency_key": "lease-issue-" + "a" * 64,
        "request_hash": "b" * 64,
        "step_attempt_no": 1,
        "fence": 2,
        "recorded_at": "2026-08-29T01:00:01Z",
    }
    definite = PackageApiError(
        403,
        "OPERATION_LEASE_AUTHORIZATION_PENDING",
        "pending",
        retryable=False,
        committed=False,
    )
    uncertain = PackageApiError(
        403,
        "OPERATION_LEASE_AUTHORIZATION_PENDING",
        "pending",
        retryable=False,
        committed=None,
    )
    assert app._classify_deferred_validation_error(
        definite,
        step_id="label-operation-lease",
        dispatch_record=dispatch,
    )["outcome"] == "REQUIRED_ABSENT"
    assert app._classify_deferred_validation_error(
        definite,
        step_id="label-package-source",
    )["outcome"] == "INVALID"
    assert app._classify_deferred_validation_error(
        uncertain,
        step_id="label-operation-lease",
        dispatch_record=dispatch,
    )["outcome"] == "UNKNOWN_COMMIT"


def test_real_gui_path_maps_pending_grant_to_waiting_dependency_without_effect(
    tmp_path,
):
    db_path, _outbox, store = _store(tmp_path)
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app.current_set_info = {
        "id": None,
        "raw": [],
        "parsed": [],
        "start_time": None,
        "error_count": 0,
        "has_error_or_reset": False,
    }
    app.deferred_intent_capture = store
    app._deferred_intent_capture_error = ""
    app.package_logistics_client = SimpleNamespace(
        config=SimpleNamespace(authority_scope_id="SCOPE-LABEL-MEASURED")
    )
    app.run_tests = True
    calls = []
    evidence = SimpleNamespace(
        item_id="ITEM-LABEL-1",
        active_label_id="LBL-MEASURED",
        membership_hash="d" * 64,
        member_count=4,
    )
    snapshot = {
        "authority_scope_id": "SCOPE-LABEL-MEASURED",
        "bundle_id": "TRANSFER-LABEL-MEASURED",
        "entity_version": 7,
        "authority_epoch": 1,
        "ledger_plane": "SHADOW_CANDIDATE",
        "plane_epoch": 1,
    }

    def resolve(*_args):
        calls.append("package_source")
        return evidence, snapshot, None, None

    def pending_lease(*_args, **_kwargs):
        calls.append("operation_lease")
        assert _kwargs["reuse_allowed"] is False
        assert _kwargs["persist_artifact"] is False
        assert str(_kwargs["issue_idempotency_key"]).startswith(
            "lease-issue-"
        )
        assert len(str(_kwargs["expected_issue_request_hash"])) == 64
        try:
            raise PackageApiError(
                403,
                "OPERATION_LEASE_AUTHORIZATION_PENDING",
                "grant approval is pending",
                retryable=False,
                committed=False,
            )
        except PackageApiError as api_error:
            raise OperationLeaseError(
                "OPERATION_LEASE_ISSUE_FAILED",
                str(api_error),
            ) from api_error

    app._resolve_central_phs2_scan_overlay = resolve
    app._acquire_operation_lease = pending_lease
    app._accept_resolved_central_phs2_scan = lambda *_args: pytest.fail(
        "validator must not promote or apply the scan"
    )
    assert app._begin_central_phs2_scan_overlay(
        "PHS2-ONLINE-PENDING-GRANT", "ITEM-LABEL-1"
    ) is True
    intent_id = app.current_set_info["deferred_intent_id"]
    row = _row(db_path, intent_id)
    assert calls == ["package_source", "operation_lease"]
    assert row["state"] == "WAITING_DEPENDENCY"
    assert row["next_attempt_at"] is None
    assert row["command_json"] is None
    assert row["receipt_json"] is None
    assert row["downstream_outbox_ref"] is None
    assert app.current_set_info["raw"] == []
    assert app.current_set_info["parsed"] == []
    assert app._deferred_capture_ui["status"] == "저장됨-선행조건대기"
    assert app._deferred_capture_ui["automatic_retry"] is False
    assert (
        app._deferred_capture_ui["dependency_identity"]
        == "CREATE_PACKAGE grant · SCOPE-LABEL-MEASURED · 승인 대기"
    )
    assert app._deferred_capture_ui["last_checked_at"]
    notice = app._deferred_capture_pending_notice()
    assert notice.title == "저장됨-선행조건대기"
    assert "마지막 확인:" in notice.message
    assert "자동 재시도하지 않습니다" in notice.message
    with sqlite3.connect(db_path) as conn:
        assert _count(conn, "package_command_outbox") == 0


def test_gui_local_integrity_invalid_calls_no_remote(tmp_path):
    db_path, _outbox, store = _store(tmp_path)
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app.current_set_info = {
        "id": None,
        "raw": [],
        "parsed": [],
        "start_time": None,
        "error_count": 0,
        "has_error_or_reset": False,
    }
    app.deferred_intent_capture = store
    app._deferred_intent_capture_error = ""
    app.run_tests = True
    remote_calls = []
    original_capture = store.capture_label_package_source

    def capture_then_break_seal(**kwargs):
        result = original_capture(**kwargs)
        store.seal_key_path.write_bytes(_protect(b"z" * 32))
        return result

    store.capture_label_package_source = capture_then_break_seal
    app._resolve_central_phs2_scan_overlay = lambda *_args: remote_calls.append(
        "remote"
    )
    assert app._begin_central_phs2_scan_overlay(
        "PHS2-LOCAL-SEAL-INVALID", "ITEM-LABEL-1"
    ) is True
    row = _row(db_path, app.current_set_info["deferred_intent_id"])
    assert row["state"] == "BLOCKED_INVALID"
    assert row["last_reason_code"] == "LOCAL_SEAL_INVALID"
    assert remote_calls == []
    assert app._deferred_capture_ui["status"] == "관리자확인"

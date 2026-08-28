"""Durable capture and validation slice of ``kmtech.deferred-intent.v1.1``.

The module installs the final v1 storage contract, captures encrypted Label
operator intent before remote work, preserves exact v1 evidence, and runs only
the fenced validation phase.
It deliberately contains no materializer, promoter, submitter, or domain-table
trigger.  The exact legacy-outbox handoff remains for already-owned work.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import base64
import ctypes
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import threading
import time
from typing import Any, Callable, Mapping
from ctypes import wintypes

from logistics_runtime_profile import (
    protect_current_user_secret,
    unprotect_current_user_secret,
)


CONTRACT_VERSION = "kmtech.deferred-intent.v1.1"
LEGACY_CONTRACT_VERSION = "kmtech.deferred-intent.v1"
CAPTURE_SCHEMA_VERSION = 1
CAPTURE_C14N_VERSION = "rfc8785-jcs-v1"
PAYLOAD_PROTECTION = "WIN_DPAPI_CURRENT_USER_V2"
LEGACY_PAYLOAD_PROTECTION = "WIN_DPAPI_CURRENT_USER_V1"
PAYLOAD_PROTECTION_PURPOSE = "kmtech.deferred-intent.payload-protection.v2"
LABEL_APP_ID = "label"
LABEL_INTENT_KIND = "LABEL_PACKAGE_SOURCE"
LABEL_PARTITION_KEY = "label-package-source"
DEFAULT_MAX_PENDING_INTENTS = 10_000
MAX_CAPTURE_PAYLOAD_BYTES = 64 * 1024
SEAL_KEY_FILENAME = "deferred-intent-seal-key.current-user.dpapi"
DEFAULT_VALIDATION_CLAIM_SECONDS = 300
DEFAULT_VALIDATION_RETRY_SECONDS = 5
MAX_VALIDATION_RETRY_SECONDS = 30 * 60

DEFERRED_INTENT_STATES = (
    "CAPTURED_UNVERIFIED",
    "VALIDATING",
    "RETRY_WAIT_VALIDATION",
    "WAITING_DEPENDENCY",
    "BLOCKED_INVALID",
    "RECONCILE_PENDING_VALIDATION",
    "VALIDATED",
    "READY_TO_SUBMIT",
    "SUBMITTING",
    "RETRY_WAIT_SUBMIT",
    "RECONCILE_PENDING_SUBMIT",
    "ACKED",
    "LOCAL_EFFECT_PENDING",
    "COMPLETED",
    "OPERATOR_REVIEW",
    "CANCELLED",
    "SUPERSEDED",
)
DEFERRED_OPERATOR_STATUS_GROUPS = (
    (
        "validation_pending",
        "저장됨-검증대기",
        ("CAPTURED_UNVERIFIED", "VALIDATING", "RETRY_WAIT_VALIDATION"),
    ),
    (
        "dependency_wait",
        "dependency대기",
        ("WAITING_DEPENDENCY",),
    ),
    (
        "transmission_wait",
        "전송대기",
        ("VALIDATED", "READY_TO_SUBMIT", "SUBMITTING", "RETRY_WAIT_SUBMIT"),
    ),
    (
        "result_checking",
        "결과확인중",
        (
            "RECONCILE_PENDING_VALIDATION",
            "RECONCILE_PENDING_SUBMIT",
        ),
    ),
    (
        "admin_review",
        "관리자확인",
        ("BLOCKED_INVALID", "OPERATOR_REVIEW"),
    ),
    (
        "completed",
        "완료",
        ("COMPLETED",),
    ),
)

# Exact per-intent labels remain frozen by CONTRACT.md section 10.  The six
# groups above are only the requested operational summary buckets; states that
# have a distinct completion meaning are never collapsed into those buckets.
DEFERRED_OPERATOR_STATE_LABELS = (
    ("CAPTURED_UNVERIFIED", "저장됨-검증대기"),
    ("VALIDATING", "저장됨-검증대기"),
    ("RETRY_WAIT_VALIDATION", "저장됨-검증대기"),
    ("WAITING_DEPENDENCY", "저장됨-선행조건대기"),
    ("BLOCKED_INVALID", "관리자확인"),
    ("RECONCILE_PENDING_VALIDATION", "결과확인중"),
    ("VALIDATED", "검증완료-전송대기"),
    ("READY_TO_SUBMIT", "검증완료-전송대기"),
    ("SUBMITTING", "검증완료-전송대기"),
    ("RETRY_WAIT_SUBMIT", "검증완료-전송대기"),
    ("RECONCILE_PENDING_SUBMIT", "결과확인중"),
    ("ACKED", "서버확정-로컬반영대기"),
    ("LOCAL_EFFECT_PENDING", "서버확정-로컬반영대기"),
    ("COMPLETED", "완료"),
    ("OPERATOR_REVIEW", "관리자확인"),
    ("CANCELLED", "종결-미완료"),
    ("SUPERSEDED", "종결-미완료"),
)
CLOSED_INCOMPLETE_STATES = ("CANCELLED", "SUPERSEDED")
RETRY_WAIT_STATES = ("RETRY_WAIT_VALIDATION", "RETRY_WAIT_SUBMIT")
QUARANTINE_STATES = ("BLOCKED_INVALID",)
QUARANTINE_PREDICATE_SQL = "state='BLOCKED_INVALID'"
# This is only a quarantine exclusion fragment, not submit eligibility.  A future
# submitter must additionally enforce FIFO, validation freshness/binding, and a
# fenced claim; exporting a partial submit-candidate predicate would be unsafe.
QUARANTINE_EXCLUSION_PREDICATE_SQL = "state<>'BLOCKED_INVALID'"

# These thresholds are local status signals only.  The retry/starvation values
# bind to the validator's 30-minute capped backoff; the dependency SLA reuses
# Label's existing 24-hour maximum server-directed Retry-After horizon.
DEFERRED_ALERT_THRESHOLDS = (
    ("oldest_retry_wait_seconds", MAX_VALIDATION_RETRY_SECONDS),
    ("waiting_dependency_seconds", 24 * 60 * 60),
    ("operator_review_increase", 1),
    ("repeated_seal_failure_count", 2),
    ("partition_starvation_seconds", MAX_VALIDATION_RETRY_SECONDS),
)
DEFAULT_STATUS_READBACK_LIMIT = 20

VALIDATION_OUTCOMES = frozenset(
    {
        "VALID",
        "RETRYABLE_UNAVAILABLE",
        "REQUIRED_ABSENT",
        "ABSENT_MATERIALIZABLE",
        "INVALID",
        "CONFLICT",
        "UNKNOWN_COMMIT",
    }
)
LABEL_VALIDATION_STEPS = (
    (
        "label-package-source",
        "PACKAGE_SOURCE",
        "label-package-source-resolution-v1",
        "READ_ONLY",
    ),
    (
        "label-operation-lease",
        "OPERATION_LEASE",
        "terminal-operation-lease-v1",
        "IDEMPOTENT_MUTATION",
    ),
)

_HEX_64_RE = re.compile(r"^[0-9a-f]{64}$")
_OPERATOR_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9._:@/\\-]{1,200}$")
_OPERATOR_REASON_CODE_ALLOWLIST = frozenset(
    {
        "CAPTURE_COMMITTED_BEFORE_REMOTE",
        "CAPTURE_BINDING_INVALID",
        "CAPTURE_DPAPI_READBACK_FAILED",
        "CAPTURE_DPAPI_ENTROPY_INVALID",
        "CAPTURE_DPAPI_PROTECT_FAILED",
        "CAPTURE_DPAPI_UNAVAILABLE",
        "CAPTURE_DPAPI_V2_CALLBACK_INVALID",
        "CAPTURE_DURABILITY_FAILED",
        "CAPTURE_IMMUTABILITY_BREACH",
        "CAPTURE_PAYLOAD_INVALID",
        "CAPTURE_PAYLOAD_TOO_LARGE",
        "CAPTURE_QUOTA_EXCEEDED",
        "CAPTURE_SCHEMA_VERSION",
        "CAPTURE_SEAL_FAILED",
        "CAPTURE_SEAL_KEY_INVALID",
        "CAPTURE_SEAL_KEY_READBACK_FAILED",
        "CAPTURE_SEAL_KEY_UNAVAILABLE",
        "CAPTURE_SEAL_KEY_WRITE_FAILED",
        "DUPLICATE_CAPTURE_SUPPRESSED",
        "DUPLICATE_CAPTURE_STATE_INVALID",
        "DUPLICATE_IDENTITY_PAYLOAD_MISMATCH",
        "CAPTURE_VERSION_IDENTITY_AMBIGUOUS",
        "FIFO_VALIDATION_CLAIMED",
        "LABEL-PACKAGE-SOURCE_VERIFIED",
        "LABEL-OPERATION-LEASE_VERIFIED",
        "LABEL_VALIDATION_PLAN_PERSISTED",
        "LEGACY_PATH_OWNS_SUBMISSION",
        "LOCAL_BINDING_INVALID",
        "LOCAL_DPAPI_INVALID",
        "LOCAL_IDENTITY_INVALID",
        "LOCAL_INTEGRITY_GATE_FAILED",
        "LOCAL_INTEGRITY_INVALID",
        "LOCAL_INTEGRITY_VALID",
        "LOCAL_PAYLOAD_HASH_INVALID",
        "LOCAL_PAYLOAD_SCHEMA_INVALID",
        "LOCAL_SCHEMA_INVALID",
        "LOCAL_SEAL_INVALID",
        "LOCAL_SEAL_KEY_INVALID",
        "OPERATION_LEASE_AUTHORIZATION_PENDING",
        "OPERATION_LEASE_COMMIT_UNKNOWN",
        "OPERATION_LEASE_RESULT_NOT_DURABLE",
        "ORDERED_LABEL_VALIDATION_VALID",
        "PACKAGE_API_UNAVAILABLE",
        "PACKAGE_TRANSPORT_UNAVAILABLE",
        "VALIDATION_CLAIMED",
        "VALIDATION_CLAIM_EXPIRED",
        "VALIDATION_CONFLICT",
        "VALIDATION_MUTATION_ATTEMPT_RECORDED",
        "VALIDATION_MUTATION_COMMITTED_WITHOUT_EXACT_RESULT",
        "VALIDATION_MUTATION_COMMIT_UNKNOWN",
        "VALIDATION_RESPONSE_INVALID",
        "VALIDATION_RESULT",
    }
)
_TERMINAL_STATES = frozenset({"COMPLETED", "CANCELLED", "SUPERSEDED"})


class _DataBlob(ctypes.Structure):
    _fields_ = (
        ("cbData", wintypes.DWORD),
        ("pbData", ctypes.POINTER(ctypes.c_byte)),
    )


def _dpapi_blob(value: bytes) -> tuple[_DataBlob, Any]:
    buffer = ctypes.create_string_buffer(bytes(value))
    return (
        _DataBlob(
            len(value),
            ctypes.cast(buffer, ctypes.POINTER(ctypes.c_byte)),
        ),
        buffer,
    )


def _operator_safe_identifier(value: Any) -> str:
    selected = str(value or "").strip()
    return selected if _OPERATOR_SAFE_IDENTIFIER_RE.fullmatch(selected) else "[redacted]"


def operator_safe_reason_code(value: Any) -> str:
    """Return only a known semantic reason code, never server-controlled text."""

    selected = str(value or "").strip().upper()
    if not selected:
        return ""
    if selected in _OPERATOR_REASON_CODE_ALLOWLIST:
        return selected
    return "REASON_CODE_REDACTED"


_operator_safe_code = operator_safe_reason_code


class DeferredIntentCaptureError(RuntimeError):
    """A durable capture could not be proven; the UI must not report saved."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = str(code or "DEFERRED_CAPTURE_FAILED")


@dataclass(frozen=True)
class DeferredIntentBinding:
    producer_id: str
    producer_install_id: str
    source_host_id: str
    manifest_hash: str
    authority_scope_id: str

    def validated(self) -> "DeferredIntentBinding":
        values = {
            "producer_id": self.producer_id,
            "producer_install_id": self.producer_install_id,
            "source_host_id": self.source_host_id,
            "authority_scope_id": self.authority_scope_id,
        }
        if any(not str(value or "").strip() for value in values.values()):
            raise DeferredIntentCaptureError(
                "CAPTURE_BINDING_INVALID",
                "deferred intent producer binding is incomplete",
            )
        digest = str(self.manifest_hash or "").strip().lower()
        if not _HEX_64_RE.fullmatch(digest):
            raise DeferredIntentCaptureError(
                "CAPTURE_BINDING_INVALID",
                "deferred intent manifest binding is invalid",
            )
        return DeferredIntentBinding(
            producer_id=str(self.producer_id).strip(),
            producer_install_id=str(self.producer_install_id).strip(),
            source_host_id=str(self.source_host_id).strip(),
            manifest_hash=digest,
            authority_scope_id=str(self.authority_scope_id).strip(),
        )


@dataclass(frozen=True)
class DeferredCaptureResult:
    intent_id: str
    state: str
    duplicate: bool
    pending_count: int
    oldest_age_seconds: int
    created_at: str


@dataclass(frozen=True)
class DeferredValidationClaim:
    intent_id: str
    worker_id: str
    fence: int
    validation_generation: int
    validation_attempt_count: int
    claim_expires_at: str
    payload: Mapping[str, Any]


@dataclass(frozen=True)
class DeferredValidationResult:
    intent_id: str
    state: str
    outcome: str
    reason_code: str
    observed_at: str
    next_attempt_at: str | None = None
    dependency_kind: str = ""
    dependency_identity: str = ""
    pending_count: int = 0
    oldest_age_seconds: int = 0


@dataclass(frozen=True)
class DeferredOperatorStatusGroup:
    key: str
    operator_status: str
    internal_states: tuple[str, ...]
    count: int
    oldest_at: str
    oldest_age_seconds: int


@dataclass(frozen=True)
class DeferredRetrySchedule:
    intent_id: str
    state: str
    partition_key: str
    partition_seq: int
    next_attempt_at: str
    reason_code: str


@dataclass(frozen=True)
class DeferredBlockedPartition:
    partition_key: str
    head_intent_id: str
    head_state: str
    head_partition_seq: int
    blocked_count: int
    head_created_at: str
    head_age_seconds: int
    head_blocked_since: str
    head_blocked_age_seconds: int
    reason_code: str


@dataclass(frozen=True)
class DeferredQuarantineRecord:
    intent_id: str
    state: str
    payload_hash: str
    reason_code: str
    created_at: str
    updated_at: str


@dataclass(frozen=True)
class DeferredStatusAlert:
    code: str
    severity: str
    observed_value: int
    threshold: int
    partition_key: str = ""


@dataclass(frozen=True)
class DeferredIntentStatusReadback:
    observed_at: str
    state_counts: tuple[tuple[str, int], ...]
    operator_groups: tuple[DeferredOperatorStatusGroup, ...]
    total_count: int
    nonterminal_count: int
    oldest_intent_id: str
    oldest_state: str
    oldest_age_seconds: int
    retry_schedule: tuple[DeferredRetrySchedule, ...]
    last_reason_code: str
    last_reason_at: str
    dependency_identity: str
    dependency_checked_at: str
    blocked_partitions: tuple[DeferredBlockedPartition, ...]
    downstream_outbox_refs: tuple[str, ...]
    quarantine: tuple[DeferredQuarantineRecord, ...]
    quarantine_count: int
    closed_incomplete_count: int
    alerts: tuple[DeferredStatusAlert, ...]
    alert_thresholds: tuple[tuple[str, int], ...]
    read_duration_ms: float
    auto_prune_enabled: bool = False


def _validate_jcs_subset(value: Any, *, path: str = "$") -> None:
    """Reject values for which stdlib JSON is not byte-identical to JCS.

    Capture payloads and audit objects use fixed ASCII keys and integer/string
    values.  Floats are intentionally forbidden so there is no implementation-
    dependent ECMAScript number rendering at this durability boundary.
    """

    if value is None or isinstance(value, (bool, int, str)):
        return
    if isinstance(value, float):
        raise DeferredIntentCaptureError(
            "CAPTURE_CANONICALIZATION_INVALID",
            f"floating-point value is forbidden at {path}",
        )
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_jcs_subset(item, path=f"{path}[{index}]")
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise DeferredIntentCaptureError(
                    "CAPTURE_CANONICALIZATION_INVALID",
                    f"non-string object key is forbidden at {path}",
                )
            _validate_jcs_subset(item, path=f"{path}.{key}")
        return
    raise DeferredIntentCaptureError(
        "CAPTURE_CANONICALIZATION_INVALID",
        f"unsupported JSON value at {path}",
    )


def canonical_json_bytes(value: Any) -> bytes:
    _validate_jcs_subset(value)
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise DeferredIntentCaptureError(
            "CAPTURE_CANONICALIZATION_INVALID",
            "capture value cannot be represented as canonical JSON",
        ) from exc


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def payload_protection_entropy(
    *,
    app_id: str,
    authority_scope_id: str,
    capture_key: str,
    contract_version: str,
    intent_kind: str,
    producer_install_id: str,
) -> bytes:
    context = {
        "app_id": str(app_id),
        "authority_scope_id": str(authority_scope_id),
        "capture_key": str(capture_key),
        "contract_version": str(contract_version),
        "intent_kind": str(intent_kind),
        "producer_install_id": str(producer_install_id),
        "purpose": PAYLOAD_PROTECTION_PURPOSE,
    }
    if context["contract_version"] != CONTRACT_VERSION:
        raise DeferredIntentCaptureError(
            "CAPTURE_DPAPI_ENTROPY_INVALID",
            "payload-protection v2 entropy requires the v1.1 contract",
        )
    return hashlib.sha256(canonical_json_bytes(context)).digest()


def common_reader_v2_entropy(row: Mapping[str, Any]) -> bytes:
    """Derive V2 entropy while refusing every ambiguous V1 envelope."""

    contract_version = str(row.get("contract_version") or "").strip()
    protection = str(row.get("payload_protection") or "").strip()
    if (
        contract_version == LEGACY_CONTRACT_VERSION
        or protection == LEGACY_PAYLOAD_PROTECTION
    ):
        raise DeferredIntentCaptureError(
            "AMBIGUOUS_PAYLOAD_PROTECTION_V1",
            "shared readers cannot infer a historical V1 DPAPI envelope",
        )
    if (contract_version, protection) != (CONTRACT_VERSION, PAYLOAD_PROTECTION):
        raise DeferredIntentCaptureError(
            "LOCAL_SCHEMA_INVALID",
            "the captured intent payload envelope is unsupported",
        )
    return payload_protection_entropy(
        app_id=str(row.get("app_id") or ""),
        authority_scope_id=str(row.get("authority_scope_id") or ""),
        capture_key=str(row.get("capture_key") or ""),
        contract_version=contract_version,
        intent_kind=str(row.get("intent_kind") or ""),
        producer_install_id=str(row.get("producer_install_id") or ""),
    )


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _age_seconds(value: str | None, now: str) -> int:
    selected = str(value or "").strip()
    if not selected:
        return 0
    try:
        return max(0, int((_parse_utc(now) - _parse_utc(selected)).total_seconds()))
    except (TypeError, ValueError):
        return 0


def _utc_after(now: str, seconds: float) -> str:
    return (_parse_utc(now) + timedelta(seconds=float(seconds))).isoformat().replace(
        "+00:00", "Z"
    )


DEFERRED_INTENT_SCHEMA_SQL = r"""
CREATE TABLE IF NOT EXISTS deferred_intents (
    intent_id TEXT PRIMARY KEY NOT NULL,
    contract_version TEXT NOT NULL CHECK (contract_version IN (
        'kmtech.deferred-intent.v1','kmtech.deferred-intent.v1.1'
    )),
    app_id TEXT NOT NULL CHECK (app_id IN ('label','defect','rework','container','inspection')),
    intent_kind TEXT NOT NULL CHECK (intent_kind IN (
        'LABEL_PACKAGE_SOURCE','DEFECT_LINKED_DISPATCH','REWORK_GOOD_PRECLAIM',
        'CONTAINER_ADMISSION','INSPECTION_ADMISSION'
    )),
    state TEXT NOT NULL CHECK (state IN (
        'CAPTURED_UNVERIFIED','VALIDATING','RETRY_WAIT_VALIDATION',
        'WAITING_DEPENDENCY','BLOCKED_INVALID','RECONCILE_PENDING_VALIDATION',
        'VALIDATED','READY_TO_SUBMIT','SUBMITTING','RETRY_WAIT_SUBMIT',
        'RECONCILE_PENDING_SUBMIT','ACKED','LOCAL_EFFECT_PENDING','COMPLETED',
        'OPERATOR_REVIEW','CANCELLED','SUPERSEDED'
    )),
    producer_id TEXT NOT NULL,
    producer_install_id TEXT NOT NULL,
    source_host_id TEXT NOT NULL,
    manifest_hash TEXT NOT NULL
        CHECK (length(manifest_hash)=64 AND manifest_hash NOT GLOB '*[^0-9a-f]*'),
    authority_scope_id TEXT NOT NULL,
    authority_epoch INTEGER CHECK (authority_epoch IS NULL OR authority_epoch >= 0),
    partition_key TEXT NOT NULL,
    partition_seq INTEGER NOT NULL CHECK (partition_seq > 0),
    local_work_identity TEXT NOT NULL,
    capture_key TEXT NOT NULL UNIQUE
        CHECK (length(capture_key)=64 AND capture_key NOT GLOB '*[^0-9a-f]*'),
    capture_schema_version INTEGER NOT NULL CHECK (capture_schema_version = 1),
    capture_c14n_version TEXT NOT NULL CHECK (capture_c14n_version = 'rfc8785-jcs-v1'),
    payload_protection TEXT NOT NULL CHECK (payload_protection IN (
        'WIN_DPAPI_CURRENT_USER_V1','WIN_DPAPI_CURRENT_USER_V2'
    )),
    payload_ciphertext BLOB NOT NULL CHECK (length(payload_ciphertext) > 0),
    payload_hash TEXT NOT NULL
        CHECK (length(payload_hash)=64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
    binding_hash TEXT NOT NULL
        CHECK (length(binding_hash)=64 AND binding_hash NOT GLOB '*[^0-9a-f]*'),
    authenticated_seal BLOB NOT NULL CHECK (length(authenticated_seal) > 0),
    seal_key_ref TEXT NOT NULL CHECK (length(seal_key_ref) > 0),
    validation_generation INTEGER NOT NULL DEFAULT 0 CHECK (validation_generation >= 0),
    validation_snapshot_hash TEXT
        CHECK (validation_snapshot_hash IS NULL OR
               (length(validation_snapshot_hash)=64 AND validation_snapshot_hash NOT GLOB '*[^0-9a-f]*')),
    validation_expires_at TEXT,
    command_json TEXT CHECK (command_json IS NULL OR json_valid(command_json)),
    command_hash TEXT
        CHECK (command_hash IS NULL OR
               (length(command_hash)=64 AND command_hash NOT GLOB '*[^0-9a-f]*')),
    command_bound_snapshot_hash TEXT
        CHECK (command_bound_snapshot_hash IS NULL OR
               (length(command_bound_snapshot_hash)=64 AND command_bound_snapshot_hash NOT GLOB '*[^0-9a-f]*')),
    server_idempotency_key TEXT,
    receipt_json TEXT CHECK (receipt_json IS NULL OR json_valid(receipt_json)),
    receipt_hash TEXT
        CHECK (receipt_hash IS NULL OR
               (length(receipt_hash)=64 AND receipt_hash NOT GLOB '*[^0-9a-f]*')),
    downstream_outbox_ref TEXT,
    local_effect_state TEXT NOT NULL DEFAULT 'NONE'
        CHECK (local_effect_state IN ('NONE','PENDING','APPLIED')),
    next_attempt_at TEXT,
    validation_attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (validation_attempt_count >= 0),
    submit_attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (submit_attempt_count >= 0),
    claim_owner TEXT,
    claim_expires_at TEXT,
    fence INTEGER NOT NULL DEFAULT 0 CHECK (fence >= 0),
    row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version > 0),
    last_reason_code TEXT,
    last_error_code TEXT,
    supersedes_intent_id TEXT REFERENCES deferred_intents(intent_id) ON DELETE RESTRICT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK ((claim_owner IS NULL) = (claim_expires_at IS NULL)),
    CHECK ((command_json IS NULL) = (command_hash IS NULL)),
    CHECK ((command_hash IS NULL) = (command_bound_snapshot_hash IS NULL)),
    CHECK ((command_hash IS NULL) = (server_idempotency_key IS NULL)),
    CHECK ((receipt_json IS NULL) = (receipt_hash IS NULL)),
    CHECK (state <> 'CAPTURED_UNVERIFIED' OR
           (validation_snapshot_hash IS NULL AND command_hash IS NULL AND
            receipt_hash IS NULL AND downstream_outbox_ref IS NULL AND
            local_effect_state = 'NONE')),
    CHECK (state <> 'VALIDATED' OR
           (validation_snapshot_hash IS NOT NULL AND command_hash IS NULL AND receipt_hash IS NULL)),
    CHECK (state NOT IN (
        'READY_TO_SUBMIT','SUBMITTING','RETRY_WAIT_SUBMIT',
        'RECONCILE_PENDING_SUBMIT','ACKED','LOCAL_EFFECT_PENDING','COMPLETED'
    ) OR (command_hash IS NOT NULL AND server_idempotency_key IS NOT NULL)),
    CHECK (state NOT IN ('ACKED','LOCAL_EFFECT_PENDING','COMPLETED') OR receipt_hash IS NOT NULL),
    CHECK (state <> 'LOCAL_EFFECT_PENDING' OR local_effect_state = 'PENDING'),
    CHECK (state <> 'COMPLETED' OR local_effect_state = 'APPLIED'),
    CHECK (state NOT IN (
        'CAPTURED_UNVERIFIED','VALIDATING','RETRY_WAIT_VALIDATION',
        'WAITING_DEPENDENCY','BLOCKED_INVALID','RECONCILE_PENDING_VALIDATION',
        'VALIDATED','READY_TO_SUBMIT','SUBMITTING','RETRY_WAIT_SUBMIT',
        'RECONCILE_PENDING_SUBMIT'
    ) OR local_effect_state = 'NONE'),
    CHECK (
        (contract_version='kmtech.deferred-intent.v1' AND
         payload_protection='WIN_DPAPI_CURRENT_USER_V1') OR
        (contract_version='kmtech.deferred-intent.v1.1' AND
         payload_protection='WIN_DPAPI_CURRENT_USER_V2')
    ),
    UNIQUE (app_id, producer_install_id, authority_scope_id, partition_key, partition_seq)
) STRICT;

CREATE TABLE IF NOT EXISTS deferred_intent_validation_steps (
    validation_step_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    intent_id TEXT NOT NULL REFERENCES deferred_intents(intent_id) ON DELETE RESTRICT,
    validation_generation INTEGER NOT NULL CHECK (validation_generation > 0),
    step_ordinal INTEGER NOT NULL CHECK (step_ordinal > 0),
    step_id TEXT NOT NULL,
    step_kind TEXT NOT NULL,
    step_effect TEXT NOT NULL CHECK (step_effect IN ('READ_ONLY','IDEMPOTENT_MUTATION')),
    validator_contract TEXT NOT NULL,
    validator_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN (
        'PLANNED','CLAIMED','REQUEST_RECORDED','RETRY_WAIT','WAITING_DEPENDENCY',
        'MATERIALIZATION_REQUIRED','RECONCILE_PENDING','VERIFIED',
        'BLOCKED_INVALID','OPERATOR_REVIEW'
    )),
    idempotency_key TEXT,
    request_json TEXT CHECK (request_json IS NULL OR json_valid(request_json)),
    request_hash TEXT
        CHECK (request_hash IS NULL OR
               (length(request_hash)=64 AND request_hash NOT GLOB '*[^0-9a-f]*')),
    validation_outcome TEXT CHECK (validation_outcome IS NULL OR validation_outcome IN (
        'VALID','RETRYABLE_UNAVAILABLE','REQUIRED_ABSENT','ABSENT_MATERIALIZABLE',
        'INVALID','CONFLICT','UNKNOWN_COMMIT'
    )),
    reconciliation_outcome TEXT CHECK (
        reconciliation_outcome IS NULL OR reconciliation_outcome IN (
            'FOUND_EXACT','NOT_FOUND','FOUND_MISMATCH','READBACK_UNAVAILABLE'
        )
    ),
    evidence_json TEXT CHECK (evidence_json IS NULL OR json_valid(evidence_json)),
    evidence_hash TEXT
        CHECK (evidence_hash IS NULL OR
               (length(evidence_hash)=64 AND evidence_hash NOT GLOB '*[^0-9a-f]*')),
    receipt_json TEXT CHECK (receipt_json IS NULL OR json_valid(receipt_json)),
    receipt_hash TEXT
        CHECK (receipt_hash IS NULL OR
               (length(receipt_hash)=64 AND receipt_hash NOT GLOB '*[^0-9a-f]*')),
    issued_at TEXT,
    expires_at TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    fence INTEGER NOT NULL DEFAULT 0 CHECK (fence >= 0),
    last_error_code TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK ((request_json IS NULL) = (request_hash IS NULL)),
    CHECK ((evidence_json IS NULL) = (evidence_hash IS NULL)),
    CHECK ((receipt_json IS NULL) = (receipt_hash IS NULL)),
    CHECK (step_effect <> 'IDEMPOTENT_MUTATION' OR idempotency_key IS NOT NULL),
    UNIQUE (intent_id, validation_generation, step_ordinal),
    UNIQUE (intent_id, validation_generation, step_id),
    UNIQUE (intent_id, validation_generation, idempotency_key)
) STRICT;

CREATE TABLE IF NOT EXISTS deferred_intent_transition_audit (
    audit_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    intent_id TEXT NOT NULL REFERENCES deferred_intents(intent_id) ON DELETE RESTRICT,
    audit_seq INTEGER NOT NULL CHECK (audit_seq > 0),
    from_state TEXT CHECK (from_state IS NULL OR from_state IN (
        'CAPTURED_UNVERIFIED','VALIDATING','RETRY_WAIT_VALIDATION',
        'WAITING_DEPENDENCY','BLOCKED_INVALID','RECONCILE_PENDING_VALIDATION',
        'VALIDATED','READY_TO_SUBMIT','SUBMITTING','RETRY_WAIT_SUBMIT',
        'RECONCILE_PENDING_SUBMIT','ACKED','LOCAL_EFFECT_PENDING','COMPLETED',
        'OPERATOR_REVIEW','CANCELLED','SUPERSEDED'
    )),
    to_state TEXT NOT NULL CHECK (to_state IN (
        'CAPTURED_UNVERIFIED','VALIDATING','RETRY_WAIT_VALIDATION',
        'WAITING_DEPENDENCY','BLOCKED_INVALID','RECONCILE_PENDING_VALIDATION',
        'VALIDATED','READY_TO_SUBMIT','SUBMITTING','RETRY_WAIT_SUBMIT',
        'RECONCILE_PENDING_SUBMIT','ACKED','LOCAL_EFFECT_PENDING','COMPLETED',
        'OPERATOR_REVIEW','CANCELLED','SUPERSEDED'
    )),
    transition_code TEXT NOT NULL CHECK (transition_code IN (
        'T1_CAPTURE','T1D_DUPLICATE_SUPPRESSED',
        'T2_CLAIM_VALIDATION','T3_LOCAL_INTEGRITY','T4_LOCAL_INVALID',
        'T5_VALIDATE_PLAN','T5A_RECORD_MUTATION_ATTEMPT','T6_VALIDATION_RETRY',
        'T6A_VALIDATION_UNKNOWN','T6B_VALIDATION_RECONCILED','T6C_VALIDATION_MISMATCH',
        'T7_CLASSIFY_ABSENCE','T8_MATERIALIZE_DEPENDENCY','T9_WAIT_DEPENDENCY',
        'T10_REJECT_OR_REVIEW','T11_FREEZE_VALIDATION','T12_MATERIALIZE_COMMAND',
        'T13_REVALIDATE_BEFORE_COMMAND','T14_CLAIM_SUBMIT','T15_CLASSIFY_SUBMIT',
        'T16_SUBMIT_UNKNOWN','T17_SUBMIT_RECONCILED','T18_APPLY_LOCAL_EFFECT',
        'T19_COMPLETE','TC_CANCEL','TS_SUPERSEDE','TR_RETRY_AUDIT'
    )),
    reason_code TEXT NOT NULL,
    validation_outcome TEXT,
    reconciliation_outcome TEXT,
    attempt_no INTEGER NOT NULL DEFAULT 0 CHECK (attempt_no >= 0),
    worker_id TEXT,
    fence INTEGER NOT NULL DEFAULT 0 CHECK (fence >= 0),
    occurred_at TEXT NOT NULL,
    evidence_hash TEXT
        CHECK (evidence_hash IS NULL OR
               (length(evidence_hash)=64 AND evidence_hash NOT GLOB '*[^0-9a-f]*')),
    prev_audit_hash TEXT
        CHECK (prev_audit_hash IS NULL OR
               (length(prev_audit_hash)=64 AND prev_audit_hash NOT GLOB '*[^0-9a-f]*')),
    audit_hash TEXT NOT NULL
        CHECK (length(audit_hash)=64 AND audit_hash NOT GLOB '*[^0-9a-f]*'),
    CHECK (validation_outcome IS NULL OR validation_outcome IN (
        'VALID','RETRYABLE_UNAVAILABLE','REQUIRED_ABSENT','ABSENT_MATERIALIZABLE',
        'INVALID','CONFLICT','UNKNOWN_COMMIT'
    )),
    CHECK (reconciliation_outcome IS NULL OR reconciliation_outcome IN (
        'FOUND_EXACT','NOT_FOUND','FOUND_MISMATCH','READBACK_UNAVAILABLE'
    )),
    UNIQUE (intent_id, audit_seq),
    UNIQUE (intent_id, audit_hash)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_deferred_intents_validation_ready
    ON deferred_intents(state, next_attempt_at, partition_key, partition_seq)
    WHERE state IN ('CAPTURED_UNVERIFIED','RETRY_WAIT_VALIDATION','WAITING_DEPENDENCY');
CREATE INDEX IF NOT EXISTS idx_deferred_intents_submit_ready
    ON deferred_intents(state, next_attempt_at, updated_at)
    WHERE state IN ('READY_TO_SUBMIT','RETRY_WAIT_SUBMIT');
CREATE INDEX IF NOT EXISTS idx_deferred_intents_reconcile
    ON deferred_intents(state, updated_at)
    WHERE state IN ('RECONCILE_PENDING_VALIDATION','RECONCILE_PENDING_SUBMIT','LOCAL_EFFECT_PENDING');
CREATE INDEX IF NOT EXISTS idx_deferred_intents_partition_fifo
    ON deferred_intents(partition_key, partition_seq, state);
CREATE INDEX IF NOT EXISTS idx_deferred_intents_status_age
    ON deferred_intents(state, created_at);
CREATE INDEX IF NOT EXISTS idx_deferred_intents_observability_updated
    ON deferred_intents(updated_at DESC, intent_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_deferred_intents_server_idempotency_key
    ON deferred_intents(server_idempotency_key)
    WHERE server_idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_deferred_steps_claim
    ON deferred_intent_validation_steps(intent_id, validation_generation, status, step_ordinal);

CREATE TRIGGER IF NOT EXISTS trg_deferred_intent_audit_no_update
BEFORE UPDATE ON deferred_intent_transition_audit
BEGIN
    SELECT RAISE(ABORT, 'deferred intent audit is append-only');
END;
CREATE TRIGGER IF NOT EXISTS trg_deferred_intent_audit_no_delete
BEFORE DELETE ON deferred_intent_transition_audit
BEGIN
    SELECT RAISE(ABORT, 'deferred intent audit is append-only');
END;
CREATE TRIGGER IF NOT EXISTS trg_deferred_intent_step_no_delete
BEFORE DELETE ON deferred_intent_validation_steps
BEGIN
    SELECT RAISE(ABORT, 'deferred intent validation evidence is retained');
END;
CREATE TRIGGER IF NOT EXISTS trg_deferred_intent_no_delete_without_tombstone
BEFORE DELETE ON deferred_intents
BEGIN
    SELECT RAISE(ABORT, 'deferred intent retention proof and prune tombstone required');
END;
CREATE TRIGGER IF NOT EXISTS trg_deferred_intent_capture_immutable
BEFORE UPDATE OF
    intent_id, contract_version, app_id, intent_kind, producer_id,
    producer_install_id, source_host_id, manifest_hash, authority_scope_id,
    partition_key, partition_seq, local_work_identity, capture_key,
    capture_schema_version, capture_c14n_version, payload_protection,
    payload_ciphertext, payload_hash, binding_hash, authenticated_seal,
    seal_key_ref, created_at
ON deferred_intents
BEGIN
    SELECT RAISE(ABORT, 'deferred intent capture identity is immutable');
END;
CREATE TRIGGER IF NOT EXISTS trg_deferred_intent_command_immutable
BEFORE UPDATE OF command_json, command_hash, command_bound_snapshot_hash, server_idempotency_key
ON deferred_intents
WHEN OLD.command_hash IS NOT NULL AND (
    NEW.command_json IS NOT OLD.command_json OR
    NEW.command_hash IS NOT OLD.command_hash OR
    NEW.command_bound_snapshot_hash IS NOT OLD.command_bound_snapshot_hash OR
    NEW.server_idempotency_key IS NOT OLD.server_idempotency_key
)
BEGIN
    SELECT RAISE(ABORT, 'deferred intent command is immutable');
END;
CREATE TRIGGER IF NOT EXISTS trg_deferred_intent_receipt_immutable
BEFORE UPDATE OF receipt_json, receipt_hash ON deferred_intents
WHEN OLD.receipt_hash IS NOT NULL AND (
    NEW.receipt_json IS NOT OLD.receipt_json OR NEW.receipt_hash IS NOT OLD.receipt_hash
)
BEGIN
    SELECT RAISE(ABORT, 'deferred intent receipt is immutable');
END;
CREATE TRIGGER IF NOT EXISTS trg_deferred_intent_state_edge_guard
BEFORE UPDATE OF state ON deferred_intents
WHEN NEW.state <> OLD.state AND NOT (
    (OLD.state='CAPTURED_UNVERIFIED' AND NEW.state IN ('VALIDATING','BLOCKED_INVALID','CANCELLED','SUPERSEDED')) OR
    (OLD.state='VALIDATING' AND NEW.state IN ('VALIDATED','RETRY_WAIT_VALIDATION','WAITING_DEPENDENCY','BLOCKED_INVALID','RECONCILE_PENDING_VALIDATION','OPERATOR_REVIEW')) OR
    (OLD.state='RETRY_WAIT_VALIDATION' AND NEW.state IN ('VALIDATING','CANCELLED','SUPERSEDED','OPERATOR_REVIEW')) OR
    (OLD.state='WAITING_DEPENDENCY' AND NEW.state IN ('VALIDATING','CANCELLED','SUPERSEDED','OPERATOR_REVIEW')) OR
    (OLD.state='BLOCKED_INVALID' AND NEW.state IN ('CANCELLED','SUPERSEDED','OPERATOR_REVIEW')) OR
    (OLD.state='RECONCILE_PENDING_VALIDATION' AND NEW.state IN ('VALIDATING','BLOCKED_INVALID','OPERATOR_REVIEW')) OR
    (OLD.state='VALIDATED' AND NEW.state IN ('READY_TO_SUBMIT','RETRY_WAIT_VALIDATION','OPERATOR_REVIEW')) OR
    (OLD.state='READY_TO_SUBMIT' AND NEW.state IN ('SUBMITTING','RETRY_WAIT_VALIDATION','OPERATOR_REVIEW')) OR
    (OLD.state='SUBMITTING' AND NEW.state IN ('ACKED','RETRY_WAIT_SUBMIT','RECONCILE_PENDING_SUBMIT','OPERATOR_REVIEW')) OR
    (OLD.state='RETRY_WAIT_SUBMIT' AND NEW.state IN ('SUBMITTING','OPERATOR_REVIEW')) OR
    (OLD.state='RECONCILE_PENDING_SUBMIT' AND NEW.state IN ('ACKED','RETRY_WAIT_SUBMIT','OPERATOR_REVIEW')) OR
    (OLD.state='ACKED' AND NEW.state IN ('LOCAL_EFFECT_PENDING','COMPLETED')) OR
    (OLD.state='LOCAL_EFFECT_PENDING' AND NEW.state IN ('COMPLETED','OPERATOR_REVIEW')) OR
    (OLD.state='OPERATOR_REVIEW' AND NEW.state='SUPERSEDED')
)
BEGIN
    SELECT RAISE(ABORT, 'forbidden deferred intent state edge');
END;
"""


_DEFERRED_TABLES = (
    "deferred_intents",
    "deferred_intent_validation_steps",
    "deferred_intent_transition_audit",
)
_DEFERRED_MIGRATION_ORDER = (
    ("deferred_intents", "intent_id"),
    ("deferred_intent_validation_steps", "validation_step_row_id"),
    ("deferred_intent_transition_audit", "audit_row_id"),
)
_DEFERRED_SCHEMA_OBJECT_NAMES = frozenset(
    {
        "idx_deferred_intents_validation_ready",
        "idx_deferred_intents_submit_ready",
        "idx_deferred_intents_reconcile",
        "idx_deferred_intents_partition_fifo",
        "idx_deferred_intents_status_age",
        "idx_deferred_intents_observability_updated",
        "ux_deferred_intents_server_idempotency_key",
        "idx_deferred_steps_claim",
        "trg_deferred_intent_audit_no_update",
        "trg_deferred_intent_audit_no_delete",
        "trg_deferred_intent_step_no_delete",
        "trg_deferred_intent_no_delete_without_tombstone",
        "trg_deferred_intent_capture_immutable",
        "trg_deferred_intent_command_immutable",
        "trg_deferred_intent_receipt_immutable",
        "trg_deferred_intent_state_edge_guard",
    }
)


def _quoted_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _table_byte_fingerprint(
    conn: sqlite3.Connection,
    table: str,
    order_column: str,
) -> tuple[tuple[str, ...], int, str]:
    columns = tuple(
        str(row[1])
        for row in conn.execute(
            f"PRAGMA table_info({_quoted_identifier(table)})"
        ).fetchall()
    )
    digest = hashlib.sha256()
    count = 0
    for row in conn.execute(
        f"SELECT * FROM {_quoted_identifier(table)} "
        f"ORDER BY {_quoted_identifier(order_column)}"
    ):
        count += 1
        for value in row:
            if value is None:
                kind, encoded = b"N", b""
            elif isinstance(value, bytes):
                kind, encoded = b"B", bytes(value)
            elif isinstance(value, int):
                kind, encoded = b"I", str(value).encode("ascii")
            elif isinstance(value, float):
                kind, encoded = b"F", repr(value).encode("ascii")
            else:
                kind, encoded = b"T", str(value).encode("utf-8")
            digest.update(kind)
            digest.update(len(encoded).to_bytes(8, "big"))
            digest.update(encoded)
        digest.update(b"R")
    return columns, count, digest.hexdigest()


def ensure_deferred_intent_schema_compatibility(conn: sqlite3.Connection) -> bool:
    """Upgrade only the v1 table capability, preserving every existing cell."""

    table_sql_row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='deferred_intents'"
    ).fetchone()
    if table_sql_row is None:
        return False
    audit_sql_row = conn.execute(
        "SELECT sql FROM sqlite_master "
        "WHERE type='table' AND name='deferred_intent_transition_audit'"
    ).fetchone()
    missing = [
        table
        for table in _DEFERRED_TABLES
        if conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        is None
    ]
    if missing:
        raise DeferredIntentCaptureError(
            "DEFERRED_SCHEMA_MIGRATION_UNSAFE",
            "installed deferred schema is incomplete",
        )
    existing_table_sql = str(table_sql_row[0] or "")
    existing_audit_sql = str(audit_sql_row[0] or "") if audit_sql_row else ""
    compact_table_sql = re.sub(r"\s+", "", existing_table_sql)
    coupled_envelopes = (
        "(contract_version='kmtech.deferred-intent.v1'AND"
        "payload_protection='WIN_DPAPI_CURRENT_USER_V1')OR"
        "(contract_version='kmtech.deferred-intent.v1.1'AND"
        "payload_protection='WIN_DPAPI_CURRENT_USER_V2')"
    )
    if (
        CONTRACT_VERSION in existing_table_sql
        and PAYLOAD_PROTECTION in existing_table_sql
        and coupled_envelopes in compact_table_sql
        and "T1D_DUPLICATE_SUPPRESSED" in existing_audit_sql
    ):
        return False
    if conn.in_transaction:
        raise DeferredIntentCaptureError(
            "DEFERRED_SCHEMA_MIGRATION_TRANSACTION_ACTIVE",
            "deferred schema migration cannot commit a caller transaction",
        )

    schema_objects = conn.execute(
        """SELECT type,name FROM sqlite_master
             WHERE tbl_name IN (?,?,?) AND sql IS NOT NULL
               AND type IN ('index','trigger')""",
        _DEFERRED_TABLES,
    ).fetchall()
    unexpected_objects = {
        str(row[1]) for row in schema_objects
    } - _DEFERRED_SCHEMA_OBJECT_NAMES
    if unexpected_objects:
        raise DeferredIntentCaptureError(
            "DEFERRED_SCHEMA_EXTENSION_UNSUPPORTED",
            "deferred schema has unknown indexes or triggers: "
            + ",".join(sorted(unexpected_objects)),
        )
    drop_statements = "\n".join(
        f"DROP {str(row[0]).upper()} IF EXISTS {_quoted_identifier(str(row[1]))};"
        for row in schema_objects
    )
    legacy_names = {
        table: f"__kmtech_v1_legacy_{table}" for table in _DEFERRED_TABLES
    }
    rename_statements = "\n".join(
        f"ALTER TABLE {_quoted_identifier(table)} RENAME TO "
        f"{_quoted_identifier(legacy_names[table])};"
        for table in reversed(_DEFERRED_TABLES)
    )
    foreign_keys_enabled = bool(conn.execute("PRAGMA foreign_keys").fetchone()[0])
    try:
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.executescript(
            "BEGIN IMMEDIATE;\n"
            + drop_statements
            + "\n"
            + rename_statements
            + "\n"
            + DEFERRED_INTENT_SCHEMA_SQL
        )
        before = {
            table: _table_byte_fingerprint(
                conn,
                legacy_names[table],
                order_column,
            )
            for table, order_column in _DEFERRED_MIGRATION_ORDER
        }
        for table, _order_column in _DEFERRED_MIGRATION_ORDER:
            columns = before[table][0]
            column_sql = ",".join(_quoted_identifier(column) for column in columns)
            conn.execute(
                f"INSERT INTO {_quoted_identifier(table)} ({column_sql}) "
                f"SELECT {column_sql} FROM {_quoted_identifier(legacy_names[table])}"
            )
        after = {
            table: _table_byte_fingerprint(conn, table, order_column)
            for table, order_column in _DEFERRED_MIGRATION_ORDER
        }
        if before != after:
            raise DeferredIntentCaptureError(
                "DEFERRED_SCHEMA_MIGRATION_READBACK_FAILED",
                "deferred schema capability migration changed stored cells",
            )
        for table in reversed(_DEFERRED_TABLES):
            conn.execute(
                f"DROP TABLE {_quoted_identifier(legacy_names[table])}"
            )
        foreign_key_errors = conn.execute("PRAGMA foreign_key_check").fetchall()
        if foreign_key_errors:
            raise DeferredIntentCaptureError(
                "DEFERRED_SCHEMA_MIGRATION_FOREIGN_KEY_FAILED",
                "deferred schema capability migration failed foreign-key readback",
            )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.execute(
            "PRAGMA foreign_keys=" + ("ON" if foreign_keys_enabled else "OFF")
        )


def _audit_values(
    *,
    intent_id: str,
    audit_seq: int,
    from_state: str | None,
    to_state: str,
    transition_code: str,
    reason_code: str,
    occurred_at: str,
    evidence_hash: str | None = None,
    prev_audit_hash: str | None = None,
    validation_outcome: str | None = None,
    reconciliation_outcome: str | None = None,
    attempt_no: int = 0,
    worker_id: str | None = None,
    fence: int = 0,
) -> dict[str, Any]:
    return {
        "intent_id": intent_id,
        "audit_seq": audit_seq,
        "from_state": from_state,
        "to_state": to_state,
        "transition_code": transition_code,
        "reason_code": reason_code,
        "validation_outcome": validation_outcome,
        "reconciliation_outcome": reconciliation_outcome,
        "attempt_no": int(attempt_no),
        "worker_id": worker_id,
        "fence": int(fence),
        "occurred_at": occurred_at,
        "evidence_hash": evidence_hash,
        "prev_audit_hash": prev_audit_hash,
    }


def append_transition_audit(
    conn: sqlite3.Connection,
    *,
    intent_id: str,
    from_state: str | None,
    to_state: str,
    transition_code: str,
    reason_code: str,
    occurred_at: str,
    evidence_hash: str | None = None,
    validation_outcome: str | None = None,
    reconciliation_outcome: str | None = None,
    attempt_no: int = 0,
    worker_id: str | None = None,
    fence: int = 0,
) -> str:
    prior = conn.execute(
        """SELECT audit_seq,audit_hash
             FROM deferred_intent_transition_audit
            WHERE intent_id=? ORDER BY audit_seq DESC LIMIT 1""",
        (intent_id,),
    ).fetchone()
    audit_seq = int(prior[0]) + 1 if prior else 1
    prev_hash = str(prior[1]) if prior else None
    values = _audit_values(
        intent_id=intent_id,
        audit_seq=audit_seq,
        from_state=from_state,
        to_state=to_state,
        transition_code=transition_code,
        reason_code=reason_code,
        occurred_at=occurred_at,
        evidence_hash=evidence_hash,
        prev_audit_hash=prev_hash,
        validation_outcome=validation_outcome,
        reconciliation_outcome=reconciliation_outcome,
        attempt_no=attempt_no,
        worker_id=worker_id,
        fence=fence,
    )
    audit_hash = canonical_sha256(values)
    conn.execute(
        """INSERT INTO deferred_intent_transition_audit(
               intent_id,audit_seq,from_state,to_state,transition_code,
               reason_code,validation_outcome,reconciliation_outcome,
               attempt_no,worker_id,fence,occurred_at,evidence_hash,
               prev_audit_hash,audit_hash
           ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            values["intent_id"],
            values["audit_seq"],
            values["from_state"],
            values["to_state"],
            values["transition_code"],
            values["reason_code"],
            values["validation_outcome"],
            values["reconciliation_outcome"],
            values["attempt_no"],
            values["worker_id"],
            values["fence"],
            values["occurred_at"],
            values["evidence_hash"],
            values["prev_audit_hash"],
            audit_hash,
        ),
    )
    return audit_hash


def supersede_for_legacy_outbox(
    conn: sqlite3.Connection,
    *,
    intent_id: str,
    local_work_identity: str,
    downstream_outbox_ref: str,
    occurred_at: str,
) -> None:
    row = conn.execute(
        """SELECT intent_id,app_id,intent_kind,state,local_work_identity,
                  downstream_outbox_ref
             FROM deferred_intents WHERE intent_id=?""",
        (str(intent_id or "").strip(),),
    ).fetchone()
    if row is None:
        raise DeferredIntentCaptureError(
            "LEGACY_HANDOFF_INTENT_MISSING",
            "captured intent is unavailable for the legacy outbox handoff",
        )
    current = dict(row)
    if (
        current["app_id"] != LABEL_APP_ID
        or current["intent_kind"] != LABEL_INTENT_KIND
        or current["local_work_identity"] != str(local_work_identity or "").strip()
    ):
        raise DeferredIntentCaptureError(
            "LEGACY_HANDOFF_BINDING_MISMATCH",
            "captured intent does not match the package outbox work identity",
        )
    exact_ref = str(downstream_outbox_ref or "").strip()
    if not exact_ref:
        raise DeferredIntentCaptureError(
            "LEGACY_HANDOFF_REF_INVALID",
            "legacy outbox handoff reference is empty",
        )
    if current["state"] == "SUPERSEDED":
        if str(current["downstream_outbox_ref"] or "") != exact_ref:
            raise DeferredIntentCaptureError(
                "LEGACY_HANDOFF_REF_CONFLICT",
                "captured intent already references a different downstream outbox row",
            )
        return
    if current["state"] != "CAPTURED_UNVERIFIED":
        raise DeferredIntentCaptureError(
            "LEGACY_HANDOFF_STATE_INVALID",
            "captured intent is not eligible for the legacy outbox handoff",
        )
    conn.execute(
        """UPDATE deferred_intents
              SET state='SUPERSEDED',downstream_outbox_ref=?,
                  last_reason_code='LEGACY_PATH_OWNS_SUBMISSION',
                  last_error_code=NULL,row_version=row_version+1,updated_at=?
            WHERE intent_id=? AND state='CAPTURED_UNVERIFIED'""",
        (exact_ref, occurred_at, current["intent_id"]),
    )
    if conn.execute("SELECT changes()").fetchone()[0] != 1:
        raise DeferredIntentCaptureError(
            "LEGACY_HANDOFF_STATE_RACE",
            "captured intent changed before the legacy outbox handoff committed",
        )
    append_transition_audit(
        conn,
        intent_id=current["intent_id"],
        from_state="CAPTURED_UNVERIFIED",
        to_state="SUPERSEDED",
        transition_code="TS_SUPERSEDE",
        reason_code="LEGACY_PATH_OWNS_SUBMISSION",
        occurred_at=occurred_at,
        evidence_hash=hashlib.sha256(exact_ref.encode("utf-8")).hexdigest(),
    )


class DeferredIntentCaptureStore:
    """Durable capture writer; it never performs remote or domain work."""

    def __init__(
        self,
        db_path: str | Path,
        binding: DeferredIntentBinding,
        *,
        seal_key_path: str | Path | None = None,
        protect_bytes: Callable[[bytes], bytes] | None = None,
        unprotect_bytes: Callable[[bytes], bytes] | None = None,
        protect_payload_bytes: Callable[[bytes, bytes], bytes] | None = None,
        unprotect_payload_bytes: Callable[[bytes, bytes], bytes] | None = None,
        max_pending_intents: int = DEFAULT_MAX_PENDING_INTENTS,
        initialize_schema: bool = True,
    ) -> None:
        self.db_path = str(Path(db_path))
        self.binding = binding.validated()
        self.seal_key_path = Path(seal_key_path or Path(self.db_path).with_name(SEAL_KEY_FILENAME))
        self.seal_key_ref = (
            f"file:{self.seal_key_path.name}#WIN_DPAPI_CURRENT_USER_V1"
        )
        self._protect_bytes = protect_bytes or self._dpapi_protect_bytes
        self._unprotect_bytes = unprotect_bytes or self._dpapi_unprotect_bytes
        if (protect_payload_bytes is None) != (unprotect_payload_bytes is None):
            raise DeferredIntentCaptureError(
                "CAPTURE_DPAPI_V2_CALLBACK_INVALID",
                "payload-protection v2 callbacks must be supplied as a pair",
            )
        self._protect_payload_bytes = (
            protect_payload_bytes or self._dpapi_protect_payload_v2
        )
        self._unprotect_payload_bytes = (
            unprotect_payload_bytes or self._dpapi_unprotect_payload_v2
        )
        self.max_pending_intents = int(max_pending_intents)
        if self.max_pending_intents < 1:
            raise DeferredIntentCaptureError(
                "CAPTURE_QUOTA_INVALID", "deferred intent quota must be positive"
            )
        self._lock = threading.RLock()
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        if initialize_schema:
            self.initialize()

    @staticmethod
    def _dpapi_protect_bytes(cleartext: bytes) -> bytes:
        encoded = base64.urlsafe_b64encode(bytes(cleartext)).decode("ascii")
        return protect_current_user_secret(encoded)

    @staticmethod
    def _dpapi_unprotect_bytes(ciphertext: bytes) -> bytes:
        encoded = unprotect_current_user_secret(bytes(ciphertext))
        try:
            return base64.b64decode(encoded.encode("ascii"), altchars=b"-_", validate=True)
        except (UnicodeError, ValueError) as exc:
            raise DeferredIntentCaptureError(
                "CAPTURE_DPAPI_READBACK_FAILED",
                "DPAPI capture readback is invalid",
            ) from exc

    @staticmethod
    def _dpapi_protect_payload_v2(cleartext: bytes, entropy: bytes) -> bytes:
        if os.name != "nt":
            raise DeferredIntentCaptureError(
                "CAPTURE_DPAPI_UNAVAILABLE",
                "DPAPI payload protection is available only on Windows",
            )
        if len(bytes(entropy)) != 32:
            raise DeferredIntentCaptureError(
                "CAPTURE_DPAPI_ENTROPY_INVALID",
                "payload-protection v2 entropy must be 32 bytes",
            )
        clear_blob, clear_buffer = _dpapi_blob(bytes(cleartext))
        entropy_blob, entropy_buffer = _dpapi_blob(bytes(entropy))
        encrypted = _DataBlob()
        if not ctypes.windll.crypt32.CryptProtectData(
            ctypes.byref(clear_blob),
            None,
            ctypes.byref(entropy_blob),
            None,
            None,
            0x1,
            ctypes.byref(encrypted),
        ):
            del clear_buffer, entropy_buffer
            raise DeferredIntentCaptureError(
                "CAPTURE_DPAPI_PROTECT_FAILED",
                "current-user DPAPI payload protection failed",
            )
        try:
            return ctypes.string_at(encrypted.pbData, encrypted.cbData)
        finally:
            ctypes.windll.kernel32.LocalFree(encrypted.pbData)

    @staticmethod
    def _dpapi_unprotect_payload_v2(ciphertext: bytes, entropy: bytes) -> bytes:
        if os.name != "nt":
            raise DeferredIntentCaptureError(
                "CAPTURE_DPAPI_UNAVAILABLE",
                "DPAPI payload readback is available only on Windows",
            )
        if len(bytes(entropy)) != 32:
            raise DeferredIntentCaptureError(
                "CAPTURE_DPAPI_ENTROPY_INVALID",
                "payload-protection v2 entropy must be 32 bytes",
            )
        encrypted_blob, encrypted_buffer = _dpapi_blob(bytes(ciphertext))
        entropy_blob, entropy_buffer = _dpapi_blob(bytes(entropy))
        decrypted = _DataBlob()
        if not ctypes.windll.crypt32.CryptUnprotectData(
            ctypes.byref(encrypted_blob),
            None,
            ctypes.byref(entropy_blob),
            None,
            None,
            0x1,
            ctypes.byref(decrypted),
        ):
            del encrypted_buffer, entropy_buffer
            raise DeferredIntentCaptureError(
                "CAPTURE_DPAPI_READBACK_FAILED",
                "current-user DPAPI payload readback failed",
            )
        try:
            return ctypes.string_at(decrypted.pbData, decrypted.cbData)
        finally:
            ctypes.windll.kernel32.LocalFree(decrypted.pbData)

    @staticmethod
    def _payload_entropy_from_row(row: Mapping[str, Any]) -> bytes:
        return payload_protection_entropy(
            app_id=str(row.get("app_id") or ""),
            authority_scope_id=str(row.get("authority_scope_id") or ""),
            capture_key=str(row.get("capture_key") or ""),
            contract_version=str(row.get("contract_version") or ""),
            intent_kind=str(row.get("intent_kind") or ""),
            producer_install_id=str(row.get("producer_install_id") or ""),
        )

    def unprotect_payload_for_common_reader(
        self,
        row: Mapping[str, Any],
    ) -> bytes:
        """Decode only unambiguous V2; shared readers must never guess V1."""

        return self._unprotect_payload_bytes(
            bytes(row.get("payload_ciphertext") or b""),
            common_reader_v2_entropy(row),
        )

    def _unprotect_label_owned_payload(self, row: Mapping[str, Any]) -> bytes:
        contract_version = str(row.get("contract_version") or "")
        protection = str(row.get("payload_protection") or "")
        if (contract_version, protection) == (CONTRACT_VERSION, PAYLOAD_PROTECTION):
            return self.unprotect_payload_for_common_reader(row)
        if (
            str(row.get("app_id") or "") == LABEL_APP_ID
            and str(row.get("intent_kind") or "") == LABEL_INTENT_KIND
            and (contract_version, protection)
            == (LEGACY_CONTRACT_VERSION, LEGACY_PAYLOAD_PROTECTION)
        ):
            return self._unprotect_bytes(
                bytes(row.get("payload_ciphertext") or b"")
            )
        raise DeferredIntentCaptureError(
            "LOCAL_SCHEMA_INVALID",
            "the captured intent payload envelope has no Label-owned decoder",
        )

    def read_owned_payload_bytes(self, intent_id: str) -> bytes:
        """Read a Label-owned payload through its exact versioned decoder."""

        row = self.get(str(intent_id or "").strip())
        if row is None:
            raise DeferredIntentCaptureError(
                "DEFERRED_INTENT_NOT_FOUND",
                "deferred intent payload is unavailable",
            )
        try:
            cleartext = bytes(self._unprotect_label_owned_payload(row))
        except DeferredIntentCaptureError:
            raise
        except Exception as exc:
            raise DeferredIntentCaptureError(
                "CAPTURE_DPAPI_READBACK_FAILED",
                "protected capture payload failed Label-owned readback",
            ) from exc
        if hashlib.sha256(cleartext).hexdigest() != str(row.get("payload_hash") or ""):
            raise DeferredIntentCaptureError(
                "LOCAL_PAYLOAD_HASH_INVALID",
                "the Label-owned payload hash does not match",
            )
        return cleartext

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    def initialize(self) -> None:
        conn = self._connect()
        try:
            ensure_deferred_intent_schema_compatibility(conn)
            conn.executescript("BEGIN IMMEDIATE;\n" + DEFERRED_INTENT_SCHEMA_SQL)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _load_or_create_seal_key(self) -> bytes:
        path = self.seal_key_path
        if path.is_file():
            try:
                protected = path.read_bytes()
                key = self._unprotect_bytes(protected)
            except Exception as exc:
                raise DeferredIntentCaptureError(
                    "CAPTURE_SEAL_KEY_UNAVAILABLE",
                    "deferred intent seal key cannot be read",
                ) from exc
            if len(key) != 32:
                raise DeferredIntentCaptureError(
                    "CAPTURE_SEAL_KEY_INVALID",
                    "deferred intent seal key has an invalid length",
                )
            return key

        key = secrets.token_bytes(32)
        temp_path = path.with_name(
            f".{path.name}.tmp-{os.getpid()}-{secrets.token_hex(8)}"
        )
        try:
            protected = bytes(self._protect_bytes(key))
            if not protected or self._unprotect_bytes(protected) != key:
                raise DeferredIntentCaptureError(
                    "CAPTURE_SEAL_KEY_READBACK_FAILED",
                    "new deferred intent seal key failed protected readback",
                )
            path.parent.mkdir(parents=True, exist_ok=True)
            with temp_path.open("xb") as handle:
                handle.write(protected)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, path)
            if self._unprotect_bytes(path.read_bytes()) != key:
                raise DeferredIntentCaptureError(
                    "CAPTURE_SEAL_KEY_READBACK_FAILED",
                    "persisted deferred intent seal key failed protected readback",
                )
            return key
        except DeferredIntentCaptureError:
            raise
        except Exception as exc:
            raise DeferredIntentCaptureError(
                "CAPTURE_SEAL_KEY_WRITE_FAILED",
                "deferred intent seal key could not be persisted",
            ) from exc
        finally:
            try:
                if temp_path.exists():
                    temp_path.unlink()
            except OSError:
                pass

    @staticmethod
    def _capture_identity(
        binding: DeferredIntentBinding,
        *,
        local_work_identity: str,
        partition_key: str,
        contract_version: str = CONTRACT_VERSION,
    ) -> dict[str, Any]:
        return {
            "contract_version": str(contract_version),
            "app_id": LABEL_APP_ID,
            "intent_kind": LABEL_INTENT_KIND,
            "producer_install_id": binding.producer_install_id,
            "authority_scope_id": binding.authority_scope_id,
            "partition_key": partition_key,
            "local_work_identity": local_work_identity,
        }

    @staticmethod
    def _seal_binding(
        binding: DeferredIntentBinding,
        *,
        capture_key: str,
        intent_id: str,
        payload_hash: str,
        partition_seq: int,
        contract_version: str = CONTRACT_VERSION,
    ) -> dict[str, Any]:
        return {
            "contract_version": str(contract_version),
            "app_id": LABEL_APP_ID,
            "intent_kind": LABEL_INTENT_KIND,
            "producer_id": binding.producer_id,
            "producer_install_id": binding.producer_install_id,
            "source_host_id": binding.source_host_id,
            "manifest_hash": binding.manifest_hash,
            "authority_scope_id": binding.authority_scope_id,
            "capture_key": capture_key,
            "intent_id": intent_id,
            "payload_hash": payload_hash,
            "capture_schema_version": CAPTURE_SCHEMA_VERSION,
            "partition_seq": partition_seq,
        }

    @staticmethod
    def _pending_summary(conn: sqlite3.Connection, now: str) -> tuple[int, int]:
        placeholders = ",".join("?" for _ in _TERMINAL_STATES)
        row = conn.execute(
            f"""SELECT COUNT(*) AS pending_count,MIN(created_at) AS oldest_at
                  FROM deferred_intents WHERE state NOT IN ({placeholders})""",
            tuple(sorted(_TERMINAL_STATES)),
        ).fetchone()
        pending = int(row["pending_count"] or 0)
        oldest = str(row["oldest_at"] or "")
        return pending, _age_seconds(oldest, now)

    def capture_label_package_source(
        self,
        *,
        local_work_identity: str,
        physical_qr_payload: str,
        item_code: str,
        partition_key: str = LABEL_PARTITION_KEY,
    ) -> DeferredCaptureResult:
        work_id = str(local_work_identity or "").strip()
        physical_qr = str(physical_qr_payload or "").strip()
        normalized_item = str(item_code or "").strip()
        selected_partition = str(partition_key or "").strip()
        if not all((work_id, physical_qr, normalized_item, selected_partition)):
            raise DeferredIntentCaptureError(
                "CAPTURE_PAYLOAD_INVALID",
                "Label deferred intent payload is incomplete",
            )
        payload = {
            "app_id": LABEL_APP_ID,
            "capture_schema_version": CAPTURE_SCHEMA_VERSION,
            "intent_kind": LABEL_INTENT_KIND,
            "item_code": normalized_item,
            "local_work_identity": work_id,
            "physical_qr_payload": physical_qr,
        }
        payload_bytes = canonical_json_bytes(payload)
        if len(payload_bytes) > MAX_CAPTURE_PAYLOAD_BYTES:
            raise DeferredIntentCaptureError(
                "CAPTURE_PAYLOAD_TOO_LARGE",
                "Label deferred intent payload exceeds the capture limit",
            )
        payload_hash = hashlib.sha256(payload_bytes).hexdigest()
        identity = self._capture_identity(
            self.binding,
            local_work_identity=work_id,
            partition_key=selected_partition,
        )
        capture_key = canonical_sha256(identity)
        legacy_identity = self._capture_identity(
            self.binding,
            local_work_identity=work_id,
            partition_key=selected_partition,
            contract_version=LEGACY_CONTRACT_VERSION,
        )
        legacy_capture_key = canonical_sha256(legacy_identity)
        intent_id = "di_" + canonical_sha256({**identity, "payload_hash": payload_hash})
        now = utc_now()

        with self._lock:
            conn: sqlite3.Connection | None = None
            try:
                conn = self._connect()
                conn.execute("BEGIN IMMEDIATE")
                existing_rows = conn.execute(
                    """SELECT * FROM deferred_intents
                         WHERE capture_key IN (?,?)
                         ORDER BY CASE WHEN capture_key=? THEN 0 ELSE 1 END""",
                    (capture_key, legacy_capture_key, capture_key),
                ).fetchall()
                if len(existing_rows) > 1:
                    raise DeferredIntentCaptureError(
                        "CAPTURE_VERSION_IDENTITY_AMBIGUOUS",
                        "both v1 and v1.1 capture identities already exist",
                    )
                existing = existing_rows[0] if existing_rows else None
                if existing is not None:
                    current = dict(existing)
                    if current["payload_hash"] == payload_hash:
                        if current["state"] != "CAPTURED_UNVERIFIED":
                            raise DeferredIntentCaptureError(
                                "DUPLICATE_CAPTURE_STATE_INVALID",
                                "duplicate capture audit requires an unverified intent",
                            )
                        append_transition_audit(
                            conn,
                            intent_id=current["intent_id"],
                            from_state=current["state"],
                            to_state=current["state"],
                            transition_code="T1D_DUPLICATE_SUPPRESSED",
                            reason_code="DUPLICATE_CAPTURE_SUPPRESSED",
                            occurred_at=now,
                            evidence_hash=payload_hash,
                        )
                        pending, age = self._pending_summary(conn, now)
                        conn.commit()
                        return DeferredCaptureResult(
                            intent_id=current["intent_id"],
                            state=current["state"],
                            duplicate=True,
                            pending_count=pending,
                            oldest_age_seconds=age,
                            created_at=current["created_at"],
                        )
                    original_ciphertext = bytes(current["payload_ciphertext"])
                    if current["state"] == "CAPTURED_UNVERIFIED":
                        conn.execute(
                            """UPDATE deferred_intents
                                  SET state='BLOCKED_INVALID',
                                      last_reason_code='DUPLICATE_IDENTITY_PAYLOAD_MISMATCH',
                                      last_error_code='DUPLICATE_IDENTITY_PAYLOAD_MISMATCH',
                                      row_version=row_version+1,updated_at=?
                                WHERE intent_id=?""",
                            (now, current["intent_id"]),
                        )
                        append_transition_audit(
                            conn,
                            intent_id=current["intent_id"],
                            from_state="CAPTURED_UNVERIFIED",
                            to_state="BLOCKED_INVALID",
                            transition_code="T4_LOCAL_INVALID",
                            reason_code="DUPLICATE_IDENTITY_PAYLOAD_MISMATCH",
                            occurred_at=now,
                            evidence_hash=payload_hash,
                        )
                    readback = conn.execute(
                        "SELECT payload_ciphertext FROM deferred_intents WHERE intent_id=?",
                        (current["intent_id"],),
                    ).fetchone()
                    if bytes(readback[0]) != original_ciphertext:
                        raise DeferredIntentCaptureError(
                            "CAPTURE_IMMUTABILITY_BREACH",
                            "conflicting capture changed the original ciphertext",
                        )
                    conn.commit()
                    raise DeferredIntentCaptureError(
                        "DUPLICATE_IDENTITY_PAYLOAD_MISMATCH",
                        "the same Label work identity was captured with different input",
                    )

                pending_before = conn.execute(
                    """SELECT COUNT(*) FROM deferred_intents
                         WHERE state NOT IN ('COMPLETED','CANCELLED','SUPERSEDED')"""
                ).fetchone()[0]
                if int(pending_before) >= self.max_pending_intents:
                    raise DeferredIntentCaptureError(
                        "CAPTURE_QUOTA_EXCEEDED",
                        "deferred intent capture quota is full",
                    )
                sequence = int(
                    conn.execute(
                        """SELECT COALESCE(MAX(partition_seq),0)+1
                             FROM deferred_intents
                            WHERE app_id=? AND producer_install_id=?
                              AND authority_scope_id=? AND partition_key=?""",
                        (
                            LABEL_APP_ID,
                            self.binding.producer_install_id,
                            self.binding.authority_scope_id,
                            selected_partition,
                        ),
                    ).fetchone()[0]
                )
                seal_key = self._load_or_create_seal_key()
                payload_entropy = payload_protection_entropy(
                    app_id=LABEL_APP_ID,
                    authority_scope_id=self.binding.authority_scope_id,
                    capture_key=capture_key,
                    contract_version=CONTRACT_VERSION,
                    intent_kind=LABEL_INTENT_KIND,
                    producer_install_id=self.binding.producer_install_id,
                )
                ciphertext = bytes(
                    self._protect_payload_bytes(payload_bytes, payload_entropy)
                )
                if (
                    not ciphertext
                    or self._unprotect_payload_bytes(ciphertext, payload_entropy)
                    != payload_bytes
                ):
                    raise DeferredIntentCaptureError(
                        "CAPTURE_DPAPI_READBACK_FAILED",
                        "protected capture payload failed current-user readback",
                    )
                seal_binding = self._seal_binding(
                    self.binding,
                    capture_key=capture_key,
                    intent_id=intent_id,
                    payload_hash=payload_hash,
                    partition_seq=sequence,
                )
                binding_bytes = canonical_json_bytes(seal_binding)
                binding_hash = hashlib.sha256(binding_bytes).hexdigest()
                seal = hmac.new(seal_key, binding_bytes, hashlib.sha256).digest()
                if not hmac.compare_digest(
                    seal, hmac.new(seal_key, binding_bytes, hashlib.sha256).digest()
                ):
                    raise DeferredIntentCaptureError(
                        "CAPTURE_SEAL_FAILED",
                        "deferred intent binding seal verification failed",
                    )
                conn.execute(
                    """INSERT INTO deferred_intents(
                           intent_id,contract_version,app_id,intent_kind,state,
                           producer_id,producer_install_id,source_host_id,
                           manifest_hash,authority_scope_id,authority_epoch,
                           partition_key,partition_seq,local_work_identity,
                           capture_key,capture_schema_version,capture_c14n_version,
                           payload_protection,payload_ciphertext,payload_hash,
                           binding_hash,authenticated_seal,seal_key_ref,
                           created_at,updated_at
                       ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        intent_id,
                        CONTRACT_VERSION,
                        LABEL_APP_ID,
                        LABEL_INTENT_KIND,
                        "CAPTURED_UNVERIFIED",
                        self.binding.producer_id,
                        self.binding.producer_install_id,
                        self.binding.source_host_id,
                        self.binding.manifest_hash,
                        self.binding.authority_scope_id,
                        None,
                        selected_partition,
                        sequence,
                        work_id,
                        capture_key,
                        CAPTURE_SCHEMA_VERSION,
                        CAPTURE_C14N_VERSION,
                        PAYLOAD_PROTECTION,
                        ciphertext,
                        payload_hash,
                        binding_hash,
                        seal,
                        self.seal_key_ref,
                        now,
                        now,
                    ),
                )
                append_transition_audit(
                    conn,
                    intent_id=intent_id,
                    from_state=None,
                    to_state="CAPTURED_UNVERIFIED",
                    transition_code="T1_CAPTURE",
                    reason_code="CAPTURE_COMMITTED_BEFORE_REMOTE",
                    occurred_at=now,
                    evidence_hash=payload_hash,
                )
                pending, age = self._pending_summary(conn, now)
                conn.commit()
                return DeferredCaptureResult(
                    intent_id=intent_id,
                    state="CAPTURED_UNVERIFIED",
                    duplicate=False,
                    pending_count=pending,
                    oldest_age_seconds=age,
                    created_at=now,
                )
            except DeferredIntentCaptureError:
                if conn is not None:
                    conn.rollback()
                raise
            except sqlite3.Error as exc:
                if conn is not None:
                    conn.rollback()
                raise DeferredIntentCaptureError(
                    "SQLITE_CAPTURE_FAILED",
                    "deferred intent SQLite commit failed",
                ) from exc
            except Exception as exc:
                if conn is not None:
                    conn.rollback()
                raise DeferredIntentCaptureError(
                    "CAPTURE_DURABILITY_FAILED",
                    "deferred intent could not be durably captured",
                ) from exc
            finally:
                if conn is not None:
                    conn.close()

    def _load_existing_seal_key(self) -> bytes:
        try:
            protected = self.seal_key_path.read_bytes()
            key = self._unprotect_bytes(protected)
        except Exception as exc:
            raise DeferredIntentCaptureError(
                "LOCAL_SEAL_KEY_INVALID",
                "the deferred intent seal key is unavailable",
            ) from exc
        if len(key) != 32:
            raise DeferredIntentCaptureError(
                "LOCAL_SEAL_KEY_INVALID",
                "the deferred intent seal key length is invalid",
            )
        return key

    @staticmethod
    def _claimed_row(
        conn: sqlite3.Connection,
        claim: DeferredValidationClaim,
        *,
        now: str | None = None,
    ) -> sqlite3.Row:
        row = conn.execute(
            """SELECT * FROM deferred_intents
                 WHERE intent_id=? AND state='VALIDATING'
                   AND fence=? AND claim_owner=? LIMIT 1""",
            (claim.intent_id, claim.fence, claim.worker_id),
        ).fetchone()
        if row is None:
            raise DeferredIntentCaptureError(
                "VALIDATION_CLAIM_LOST",
                "the deferred validation fence is no longer owned",
            )
        expires_at = str(row["claim_expires_at"] or "")
        if not expires_at or _parse_utc(expires_at) <= _parse_utc(
            str(now or utc_now())
        ):
            raise DeferredIntentCaptureError(
                "VALIDATION_CLAIM_LOST",
                "the deferred validation claim expired",
            )
        return row

    @staticmethod
    def _validation_retry_delay(attempt_no: int) -> float:
        exponent = max(0, min(int(attempt_no) - 1, 12))
        return float(
            min(
                MAX_VALIDATION_RETRY_SECONDS,
                DEFAULT_VALIDATION_RETRY_SECONDS * (2**exponent),
            )
        )

    @staticmethod
    def _validation_step_idempotency_key(
        *,
        intent_id: str,
        validation_generation: int,
        step_id: str,
    ) -> str:
        material = canonical_json_bytes(
            {
                "contract_version": CONTRACT_VERSION,
                "intent_id": str(intent_id),
                "step_id": str(step_id),
                "validation_generation": int(validation_generation),
            }
        )
        return "lease-issue-" + hashlib.sha256(material).hexdigest()

    def claim_validation(
        self,
        intent_id: str,
        *,
        worker_id: str,
        claim_seconds: int = DEFAULT_VALIDATION_CLAIM_SECONDS,
        now: str | None = None,
        allow_waiting_dependency: bool = False,
    ) -> DeferredValidationClaim | None:
        """Claim one exact eligible FIFO intent and allocate a fresh fence."""

        selected_id = str(intent_id or "").strip()
        selected_worker = str(worker_id or "").strip()
        duration = int(claim_seconds)
        observed_at = str(now or utc_now())
        if not selected_id or not selected_worker or duration < 1:
            raise DeferredIntentCaptureError(
                "VALIDATION_CLAIM_INVALID",
                "deferred validation claim identity is incomplete",
            )

        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT * FROM deferred_intents WHERE intent_id=? LIMIT 1",
                    (selected_id,),
                ).fetchone()
                if row is None:
                    raise DeferredIntentCaptureError(
                        "VALIDATION_INTENT_MISSING",
                        "the captured intent is unavailable for validation",
                    )
                current = dict(row)

                if current["state"] == "VALIDATING":
                    expires_at = str(current.get("claim_expires_at") or "")
                    if not expires_at or _parse_utc(expires_at) > _parse_utc(observed_at):
                        conn.rollback()
                        return None
                    expired_owner = str(current.get("claim_owner") or "")
                    expired_fence = int(current.get("fence") or 0)
                    uncertain = conn.execute(
                        """SELECT request_hash
                             FROM deferred_intent_validation_steps
                            WHERE intent_id=? AND validation_generation=?
                              AND step_effect='IDEMPOTENT_MUTATION'
                              AND status='REQUEST_RECORDED' AND fence=?
                            ORDER BY step_ordinal LIMIT 1""",
                        (
                            selected_id,
                            int(current.get("validation_generation") or 0),
                            expired_fence,
                        ),
                    ).fetchone()
                    if uncertain is not None:
                        cursor = conn.execute(
                            """UPDATE deferred_intents
                                  SET state='RECONCILE_PENDING_VALIDATION',
                                      claim_owner=NULL,claim_expires_at=NULL,
                                      next_attempt_at=NULL,
                                      last_reason_code=
                                          'VALIDATION_MUTATION_COMMIT_UNKNOWN',
                                      last_error_code=
                                          'VALIDATION_MUTATION_COMMIT_UNKNOWN',
                                      row_version=row_version+1,updated_at=?
                                WHERE intent_id=? AND state='VALIDATING'
                                  AND fence=? AND claim_owner=?""",
                            (
                                observed_at,
                                selected_id,
                                expired_fence,
                                expired_owner,
                            ),
                        )
                        if cursor.rowcount != 1:
                            raise DeferredIntentCaptureError(
                                "VALIDATION_CLAIM_LOST",
                                "the uncertain validation claim changed concurrently",
                            )
                        append_transition_audit(
                            conn,
                            intent_id=selected_id,
                            from_state="VALIDATING",
                            to_state="RECONCILE_PENDING_VALIDATION",
                            transition_code="T6A_VALIDATION_UNKNOWN",
                            reason_code="VALIDATION_MUTATION_COMMIT_UNKNOWN",
                            validation_outcome="UNKNOWN_COMMIT",
                            attempt_no=int(
                                current.get("validation_attempt_count") or 0
                            ),
                            worker_id=expired_owner,
                            fence=expired_fence,
                            occurred_at=observed_at,
                            evidence_hash=str(uncertain["request_hash"] or "")
                            or None,
                        )
                        conn.commit()
                        return None
                    cursor = conn.execute(
                        """UPDATE deferred_intents
                              SET state='RETRY_WAIT_VALIDATION',claim_owner=NULL,
                                  claim_expires_at=NULL,next_attempt_at=?,
                                  last_reason_code='VALIDATION_CLAIM_EXPIRED',
                                  last_error_code='VALIDATION_CLAIM_EXPIRED',
                                  row_version=row_version+1,updated_at=?
                            WHERE intent_id=? AND state='VALIDATING'
                              AND fence=? AND claim_owner=?""",
                        (
                            observed_at,
                            observed_at,
                            selected_id,
                            expired_fence,
                            expired_owner,
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise DeferredIntentCaptureError(
                            "VALIDATION_CLAIM_LOST",
                            "the expired validation claim changed concurrently",
                        )
                    append_transition_audit(
                        conn,
                        intent_id=selected_id,
                        from_state="VALIDATING",
                        to_state="RETRY_WAIT_VALIDATION",
                        transition_code="T6_VALIDATION_RETRY",
                        reason_code="VALIDATION_CLAIM_EXPIRED",
                        validation_outcome="RETRYABLE_UNAVAILABLE",
                        attempt_no=int(current.get("validation_attempt_count") or 0),
                        worker_id=expired_owner,
                        fence=expired_fence,
                        occurred_at=observed_at,
                    )
                    current["state"] = "RETRY_WAIT_VALIDATION"
                    current["next_attempt_at"] = observed_at

                eligible = {"CAPTURED_UNVERIFIED", "RETRY_WAIT_VALIDATION"}
                if allow_waiting_dependency:
                    eligible.add("WAITING_DEPENDENCY")
                if current["state"] not in eligible:
                    conn.rollback()
                    return None
                next_attempt = str(current.get("next_attempt_at") or "")
                if next_attempt and _parse_utc(next_attempt) > _parse_utc(observed_at):
                    conn.rollback()
                    return None

                predecessor = conn.execute(
                    """SELECT intent_id,state,partition_seq
                         FROM deferred_intents
                        WHERE app_id=? AND producer_install_id=?
                          AND authority_scope_id=? AND partition_key=?
                          AND partition_seq<?
                          AND state NOT IN ('COMPLETED','CANCELLED','SUPERSEDED')
                        ORDER BY partition_seq LIMIT 1""",
                    (
                        current["app_id"],
                        current["producer_install_id"],
                        current["authority_scope_id"],
                        current["partition_key"],
                        current["partition_seq"],
                    ),
                ).fetchone()
                if predecessor is not None:
                    conn.rollback()
                    return None

                source_state = str(current["state"])
                next_fence = int(current.get("fence") or 0) + 1
                generation = int(current.get("validation_generation") or 0) + 1
                attempt_no = int(current.get("validation_attempt_count") or 0) + 1
                claim_expires_at = _utc_after(observed_at, duration)
                cursor = conn.execute(
                    """UPDATE deferred_intents
                          SET state='VALIDATING',validation_generation=?,
                              validation_attempt_count=?,claim_owner=?,
                              claim_expires_at=?,fence=?,next_attempt_at=NULL,
                              last_reason_code='VALIDATION_CLAIMED',
                              last_error_code=NULL,row_version=row_version+1,
                              updated_at=?
                        WHERE intent_id=? AND state=? AND fence=?""",
                    (
                        generation,
                        attempt_no,
                        selected_worker,
                        claim_expires_at,
                        next_fence,
                        observed_at,
                        selected_id,
                        source_state,
                        int(current.get("fence") or 0),
                    ),
                )
                if cursor.rowcount != 1:
                    raise DeferredIntentCaptureError(
                        "VALIDATION_CLAIM_LOST",
                        "the validation claim changed concurrently",
                    )
                append_transition_audit(
                    conn,
                    intent_id=selected_id,
                    from_state=source_state,
                    to_state="VALIDATING",
                    transition_code="T2_CLAIM_VALIDATION",
                    reason_code="FIFO_VALIDATION_CLAIMED",
                    attempt_no=attempt_no,
                    worker_id=selected_worker,
                    fence=next_fence,
                    occurred_at=observed_at,
                )
                conn.commit()
                return DeferredValidationClaim(
                    intent_id=selected_id,
                    worker_id=selected_worker,
                    fence=next_fence,
                    validation_generation=generation,
                    validation_attempt_count=attempt_no,
                    claim_expires_at=claim_expires_at,
                    payload={},
                )
            except DeferredIntentCaptureError:
                conn.rollback()
                raise
            except (TypeError, ValueError, sqlite3.Error) as exc:
                conn.rollback()
                raise DeferredIntentCaptureError(
                    "VALIDATION_CLAIM_FAILED",
                    "the deferred validation claim could not be committed",
                ) from exc
            finally:
                conn.close()

    def verify_local_integrity(
        self,
        claim: DeferredValidationClaim,
        *,
        now: str | None = None,
    ) -> DeferredValidationClaim | DeferredValidationResult:
        """Run DPAPI, seal, schema, identity, manifest, and scope before HTTP."""

        observed_at = str(now or utc_now())
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = self._claimed_row(conn, claim, now=observed_at)
                current = dict(row)
                if _parse_utc(str(current["claim_expires_at"])) <= _parse_utc(
                    observed_at
                ):
                    raise DeferredIntentCaptureError(
                        "VALIDATION_CLAIM_LOST",
                        "the validation claim expired before local integrity",
                    )
                stored_contract_version = str(current["contract_version"])
                stored_payload_protection = str(current["payload_protection"])
                supported_envelope = (
                    stored_contract_version,
                    stored_payload_protection,
                ) in {
                    (CONTRACT_VERSION, PAYLOAD_PROTECTION),
                    (LEGACY_CONTRACT_VERSION, LEGACY_PAYLOAD_PROTECTION),
                }
                expected_constants = {
                    "app_id": LABEL_APP_ID,
                    "intent_kind": LABEL_INTENT_KIND,
                    "capture_schema_version": CAPTURE_SCHEMA_VERSION,
                    "capture_c14n_version": CAPTURE_C14N_VERSION,
                    "seal_key_ref": self.seal_key_ref,
                }
                if not supported_envelope or any(
                    current.get(key) != value
                    for key, value in expected_constants.items()
                ):
                    raise DeferredIntentCaptureError(
                        "LOCAL_SCHEMA_INVALID",
                        "the captured intent schema binding is invalid",
                    )
                expected_binding = self.binding
                stored_binding = DeferredIntentBinding(
                    producer_id=str(current["producer_id"]),
                    producer_install_id=str(current["producer_install_id"]),
                    source_host_id=str(current["source_host_id"]),
                    manifest_hash=str(current["manifest_hash"]),
                    authority_scope_id=str(current["authority_scope_id"]),
                ).validated()
                if stored_binding != expected_binding:
                    raise DeferredIntentCaptureError(
                        "LOCAL_BINDING_INVALID",
                        "the current producer, install, host, manifest, or scope differs",
                    )
                try:
                    # Dispatch by the exact stored envelope.  The shared reader
                    # intentionally rejects ambiguous V1, while this Label-owned
                    # path retains only Label's historical V1 decoder.
                    payload_bytes = self._unprotect_label_owned_payload(current)
                    payload = json.loads(payload_bytes.decode("utf-8"))
                except Exception as exc:
                    raise DeferredIntentCaptureError(
                        "LOCAL_DPAPI_INVALID",
                        "the captured intent cannot be decrypted and decoded",
                    ) from exc
                if not isinstance(payload, dict) or set(payload) != {
                    "app_id",
                    "capture_schema_version",
                    "intent_kind",
                    "item_code",
                    "local_work_identity",
                    "physical_qr_payload",
                }:
                    raise DeferredIntentCaptureError(
                        "LOCAL_PAYLOAD_SCHEMA_INVALID",
                        "the captured Label payload schema is invalid",
                    )
                if (
                    payload.get("app_id") != LABEL_APP_ID
                    or payload.get("intent_kind") != LABEL_INTENT_KIND
                    or payload.get("capture_schema_version") != CAPTURE_SCHEMA_VERSION
                    or not all(
                        str(payload.get(key) or "").strip()
                        for key in (
                            "item_code",
                            "local_work_identity",
                            "physical_qr_payload",
                        )
                    )
                ):
                    raise DeferredIntentCaptureError(
                        "LOCAL_PAYLOAD_SCHEMA_INVALID",
                        "the captured Label payload values are invalid",
                    )
                canonical_payload = canonical_json_bytes(payload)
                payload_hash = hashlib.sha256(canonical_payload).hexdigest()
                if (
                    payload_bytes != canonical_payload
                    or payload_hash != str(current["payload_hash"])
                ):
                    raise DeferredIntentCaptureError(
                        "LOCAL_PAYLOAD_HASH_INVALID",
                        "the captured Label payload hash is invalid",
                    )
                identity = self._capture_identity(
                    expected_binding,
                    local_work_identity=str(payload["local_work_identity"]),
                    partition_key=str(current["partition_key"]),
                    contract_version=stored_contract_version,
                )
                capture_key = canonical_sha256(identity)
                intent_id = "di_" + canonical_sha256(
                    {**identity, "payload_hash": payload_hash}
                )
                if (
                    str(payload["local_work_identity"])
                    != str(current["local_work_identity"])
                    or capture_key != str(current["capture_key"])
                    or intent_id != str(current["intent_id"])
                ):
                    raise DeferredIntentCaptureError(
                        "LOCAL_IDENTITY_INVALID",
                        "the captured Label identity is invalid",
                    )
                seal_binding = self._seal_binding(
                    expected_binding,
                    capture_key=capture_key,
                    intent_id=intent_id,
                    payload_hash=payload_hash,
                    partition_seq=int(current["partition_seq"]),
                    contract_version=stored_contract_version,
                )
                binding_bytes = canonical_json_bytes(seal_binding)
                binding_hash = hashlib.sha256(binding_bytes).hexdigest()
                seal_key = self._load_existing_seal_key()
                expected_seal = hmac.new(
                    seal_key, binding_bytes, hashlib.sha256
                ).digest()
                if (
                    binding_hash != str(current["binding_hash"])
                    or not hmac.compare_digest(
                        expected_seal, bytes(current["authenticated_seal"])
                    )
                ):
                    raise DeferredIntentCaptureError(
                        "LOCAL_SEAL_INVALID",
                        "the captured Label authenticated seal is invalid",
                    )
                evidence_hash = canonical_sha256(
                    {
                        "contract_version": stored_contract_version,
                        "gate": "LOCAL_INTEGRITY",
                        "binding_hash": binding_hash,
                        "payload_hash": payload_hash,
                        "observed_at": observed_at,
                    }
                )
                cursor = conn.execute(
                    """UPDATE deferred_intents
                          SET last_reason_code='LOCAL_INTEGRITY_VALID',
                              last_error_code=NULL,row_version=row_version+1,
                              updated_at=?
                        WHERE intent_id=? AND state='VALIDATING'
                          AND fence=? AND claim_owner=?""",
                    (observed_at, claim.intent_id, claim.fence, claim.worker_id),
                )
                if cursor.rowcount != 1:
                    raise DeferredIntentCaptureError(
                        "VALIDATION_CLAIM_LOST",
                        "the local integrity fence was lost",
                    )
                append_transition_audit(
                    conn,
                    intent_id=claim.intent_id,
                    from_state="VALIDATING",
                    to_state="VALIDATING",
                    transition_code="T3_LOCAL_INTEGRITY",
                    reason_code="LOCAL_INTEGRITY_VALID",
                    validation_outcome="VALID",
                    attempt_no=claim.validation_attempt_count,
                    worker_id=claim.worker_id,
                    fence=claim.fence,
                    occurred_at=observed_at,
                    evidence_hash=evidence_hash,
                )
                conn.commit()
                return DeferredValidationClaim(
                    intent_id=claim.intent_id,
                    worker_id=claim.worker_id,
                    fence=claim.fence,
                    validation_generation=claim.validation_generation,
                    validation_attempt_count=claim.validation_attempt_count,
                    claim_expires_at=claim.claim_expires_at,
                    payload=dict(payload),
                )
            except DeferredIntentCaptureError as exc:
                if exc.code == "VALIDATION_CLAIM_LOST":
                    conn.rollback()
                    raise
                reason_code = str(exc.code or "LOCAL_INTEGRITY_INVALID")
                evidence_hash = canonical_sha256(
                    {
                        "contract_version": CONTRACT_VERSION,
                        "gate": "LOCAL_INTEGRITY",
                        "reason_code": reason_code,
                        "observed_at": observed_at,
                    }
                )
                cursor = conn.execute(
                    """UPDATE deferred_intents
                          SET state='BLOCKED_INVALID',claim_owner=NULL,
                              claim_expires_at=NULL,next_attempt_at=NULL,
                              last_reason_code=?,last_error_code=?,
                              row_version=row_version+1,updated_at=?
                        WHERE intent_id=? AND state='VALIDATING'
                          AND fence=? AND claim_owner=?""",
                    (
                        reason_code,
                        reason_code,
                        observed_at,
                        claim.intent_id,
                        claim.fence,
                        claim.worker_id,
                    ),
                )
                if cursor.rowcount != 1:
                    conn.rollback()
                    raise DeferredIntentCaptureError(
                        "VALIDATION_CLAIM_LOST",
                        "the invalid local integrity fence was lost",
                    ) from exc
                append_transition_audit(
                    conn,
                    intent_id=claim.intent_id,
                    from_state="VALIDATING",
                    to_state="BLOCKED_INVALID",
                    transition_code="T4_LOCAL_INVALID",
                    reason_code=reason_code,
                    validation_outcome="INVALID",
                    attempt_no=claim.validation_attempt_count,
                    worker_id=claim.worker_id,
                    fence=claim.fence,
                    occurred_at=observed_at,
                    evidence_hash=evidence_hash,
                )
                pending, age = self._pending_summary(conn, observed_at)
                conn.commit()
                return DeferredValidationResult(
                    intent_id=claim.intent_id,
                    state="BLOCKED_INVALID",
                    outcome="INVALID",
                    reason_code=reason_code,
                    observed_at=observed_at,
                    pending_count=pending,
                    oldest_age_seconds=age,
                )
            except Exception as exc:
                conn.rollback()
                raise DeferredIntentCaptureError(
                    "LOCAL_INTEGRITY_GATE_FAILED",
                    "the local integrity gate could not be committed",
                ) from exc
            finally:
                conn.close()

    def plan_label_validation(
        self,
        claim: DeferredValidationClaim,
        *,
        now: str | None = None,
    ) -> tuple[dict[str, Any], ...]:
        """Persist Label's ordered read and idempotent-mutation validation plan."""

        observed_at = str(now or utc_now())
        payload = dict(claim.payload)
        physical_hash = hashlib.sha256(
            str(payload.get("physical_qr_payload") or "").encode("utf-8")
        ).hexdigest()
        requests = (
            {
                "authority_scope_id": self.binding.authority_scope_id,
                "item_code": str(payload.get("item_code") or ""),
                "local_work_identity": str(
                    payload.get("local_work_identity") or ""
                ),
                "physical_qr_sha256": physical_hash,
            },
            {
                "authority_scope_id": self.binding.authority_scope_id,
                "operation": "CREATE_PACKAGE",
                "physical_qr_sha256": physical_hash,
            },
        )
        if not all(requests[0].values()) or not all(requests[1].values()):
            raise DeferredIntentCaptureError(
                "VALIDATION_PLAN_INVALID",
                "the Label validation plan identity is incomplete",
            )
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._claimed_row(conn, claim, now=observed_at)
                planned: list[dict[str, Any]] = []
                for ordinal, (definition, request) in enumerate(
                    zip(LABEL_VALIDATION_STEPS, requests), start=1
                ):
                    (
                        step_id,
                        step_kind,
                        validator_contract,
                        step_effect,
                    ) = definition
                    request_json = canonical_json_bytes(request).decode("utf-8")
                    request_hash_source: Mapping[str, Any] = request
                    if step_effect == "IDEMPOTENT_MUTATION":
                        request_hash_source = {
                            "authority_scope_id": self.binding.authority_scope_id,
                            "operation": "CREATE_PACKAGE",
                            "scan_payload": str(
                                payload.get("physical_qr_payload") or ""
                            ),
                        }
                    request_hash = hashlib.sha256(
                        canonical_json_bytes(request_hash_source)
                    ).hexdigest()
                    idempotency_key = (
                        self._validation_step_idempotency_key(
                            intent_id=claim.intent_id,
                            validation_generation=claim.validation_generation,
                            step_id=step_id,
                        )
                        if step_effect == "IDEMPOTENT_MUTATION"
                        else None
                    )
                    conn.execute(
                        """INSERT INTO deferred_intent_validation_steps(
                               intent_id,validation_generation,step_ordinal,
                               step_id,step_kind,step_effect,validator_contract,
                               validator_version,status,idempotency_key,
                               request_json,request_hash,attempt_count,fence,
                               created_at,updated_at
                           ) VALUES (?,?,?,?,?,?,?,'1','PLANNED',?,
                                     ?,?,0,?,?,?)""",
                        (
                            claim.intent_id,
                            claim.validation_generation,
                            ordinal,
                            step_id,
                            step_kind,
                            step_effect,
                            validator_contract,
                            idempotency_key,
                            request_json,
                            request_hash,
                            claim.fence,
                            observed_at,
                            observed_at,
                        ),
                    )
                    planned.append(
                        {
                            "step_id": step_id,
                            "step_kind": step_kind,
                            "step_effect": step_effect,
                            "idempotency_key": idempotency_key,
                            "request_hash": request_hash,
                        }
                    )
                plan_hash = canonical_sha256(planned)
                cursor = conn.execute(
                    """UPDATE deferred_intents
                          SET last_reason_code='LABEL_VALIDATION_PLAN_PERSISTED',
                              row_version=row_version+1,updated_at=?
                        WHERE intent_id=? AND state='VALIDATING'
                          AND fence=? AND claim_owner=?""",
                    (observed_at, claim.intent_id, claim.fence, claim.worker_id),
                )
                if cursor.rowcount != 1:
                    raise DeferredIntentCaptureError(
                        "VALIDATION_CLAIM_LOST",
                        "the Label validation plan fence was lost",
                    )
                append_transition_audit(
                    conn,
                    intent_id=claim.intent_id,
                    from_state="VALIDATING",
                    to_state="VALIDATING",
                    transition_code="T5_VALIDATE_PLAN",
                    reason_code="LABEL_VALIDATION_PLAN_PERSISTED",
                    attempt_no=claim.validation_attempt_count,
                    worker_id=claim.worker_id,
                    fence=claim.fence,
                    occurred_at=observed_at,
                    evidence_hash=plan_hash,
                )
                conn.commit()
                return tuple(planned)
            except DeferredIntentCaptureError:
                conn.rollback()
                raise
            except sqlite3.Error as exc:
                conn.rollback()
                raise DeferredIntentCaptureError(
                    "VALIDATION_PLAN_PERSIST_FAILED",
                    "the Label validation plan could not be persisted",
                ) from exc
            finally:
                conn.close()

    def record_validation_mutation_attempt(
        self,
        claim: DeferredValidationClaim,
        *,
        step_id: str,
        claim_seconds: int = DEFAULT_VALIDATION_CLAIM_SECONDS,
        now: str | None = None,
    ) -> dict[str, Any]:
        """Commit T5A and renew the fence lease before a mutating dispatch."""

        observed_at = str(now or utc_now())
        selected_step = str(step_id or "").strip()
        duration = int(claim_seconds)
        if not selected_step or duration < 1:
            raise DeferredIntentCaptureError(
                "VALIDATION_MUTATION_ATTEMPT_INVALID",
                "the mutating validation attempt identity is incomplete",
            )
        renewed_expiry = _utc_after(observed_at, duration)
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._claimed_row(conn, claim, now=observed_at)
                step = conn.execute(
                    """SELECT * FROM deferred_intent_validation_steps
                         WHERE intent_id=? AND validation_generation=?
                           AND step_id=? AND fence=? LIMIT 1""",
                    (
                        claim.intent_id,
                        claim.validation_generation,
                        selected_step,
                        claim.fence,
                    ),
                ).fetchone()
                if (
                    step is None
                    or str(step["step_effect"]) != "IDEMPOTENT_MUTATION"
                    or str(step["status"]) not in {"PLANNED", "RETRY_WAIT"}
                    or not str(step["idempotency_key"] or "")
                    or not str(step["request_hash"] or "")
                ):
                    raise DeferredIntentCaptureError(
                        "VALIDATION_MUTATION_ATTEMPT_INVALID",
                        "the mutating validation step is not dispatchable",
                    )
                step_attempt = int(step["attempt_count"] or 0) + 1
                cursor = conn.execute(
                    """UPDATE deferred_intent_validation_steps
                          SET status='REQUEST_RECORDED',attempt_count=?,
                              last_error_code=NULL,updated_at=?
                        WHERE validation_step_row_id=? AND status IN (
                                  'PLANNED','RETRY_WAIT'
                              ) AND fence=?""",
                    (
                        step_attempt,
                        observed_at,
                        int(step["validation_step_row_id"]),
                        claim.fence,
                    ),
                )
                if cursor.rowcount != 1:
                    raise DeferredIntentCaptureError(
                        "VALIDATION_STEP_FENCE_LOST",
                        "the mutating validation step changed before dispatch",
                    )
                cursor = conn.execute(
                    """UPDATE deferred_intents
                          SET claim_expires_at=?,row_version=row_version+1,
                              last_reason_code=
                                  'VALIDATION_MUTATION_ATTEMPT_RECORDED',
                              updated_at=?
                        WHERE intent_id=? AND state='VALIDATING'
                          AND fence=? AND claim_owner=?""",
                    (
                        renewed_expiry,
                        observed_at,
                        claim.intent_id,
                        claim.fence,
                        claim.worker_id,
                    ),
                )
                if cursor.rowcount != 1:
                    raise DeferredIntentCaptureError(
                        "VALIDATION_CLAIM_LOST",
                        "the mutating validation fence was lost before dispatch",
                    )
                append_transition_audit(
                    conn,
                    intent_id=claim.intent_id,
                    from_state="VALIDATING",
                    to_state="VALIDATING",
                    transition_code="T5A_RECORD_MUTATION_ATTEMPT",
                    reason_code="VALIDATION_MUTATION_ATTEMPT_RECORDED",
                    attempt_no=claim.validation_attempt_count,
                    worker_id=claim.worker_id,
                    fence=claim.fence,
                    occurred_at=observed_at,
                    evidence_hash=str(step["request_hash"]),
                )
                conn.commit()
                return {
                    "step_id": selected_step,
                    "idempotency_key": str(step["idempotency_key"]),
                    "request_hash": str(step["request_hash"]),
                    "step_attempt_no": step_attempt,
                    "fence": claim.fence,
                    "recorded_at": observed_at,
                    "claim_expires_at": renewed_expiry,
                }
            except DeferredIntentCaptureError:
                conn.rollback()
                raise
            except (TypeError, ValueError, sqlite3.Error) as exc:
                conn.rollback()
                raise DeferredIntentCaptureError(
                    "VALIDATION_MUTATION_ATTEMPT_PERSIST_FAILED",
                    "the mutating validation attempt could not be committed",
                ) from exc
            finally:
                conn.close()

    @staticmethod
    def _canonical_validation_evidence(evidence: Mapping[str, Any]) -> tuple[str, str]:
        value = dict(evidence or {})
        forbidden_fragments = (
            "access_token",
            "api_key",
            "auth_header",
            "authorization",
            "bearer_token",
            "cookie",
            "credential",
            "password",
            "physical_qr_payload",
            "private_key",
            "refresh_token",
            "scan_payload",
            "secret",
            "token",
        )

        def check(item: Any) -> None:
            if isinstance(item, Mapping):
                for key, child in item.items():
                    normalized_key = re.sub(
                        r"[^a-z0-9]+", "_", str(key).strip().lower()
                    ).strip("_")
                    if any(
                        fragment in normalized_key
                        for fragment in forbidden_fragments
                    ):
                        raise DeferredIntentCaptureError(
                            "VALIDATION_EVIDENCE_SECRET_FORBIDDEN",
                            "validation evidence contains a protected field",
                        )
                    check(child)
            elif isinstance(item, (list, tuple)):
                for child in item:
                    check(child)
            elif isinstance(item, str):
                candidate = item.strip()
                compact_parts = candidate.split(".")
                if candidate.lower().startswith("bearer ") or (
                    len(candidate) >= 80
                    and len(compact_parts) == 3
                    and all(
                        re.fullmatch(r"[A-Za-z0-9_-]+", part or "")
                        for part in compact_parts
                    )
                ):
                    raise DeferredIntentCaptureError(
                        "VALIDATION_EVIDENCE_SECRET_FORBIDDEN",
                        "validation evidence contains protected token material",
                    )

        check(value)
        encoded = canonical_json_bytes(value)
        if len(encoded) > MAX_CAPTURE_PAYLOAD_BYTES:
            raise DeferredIntentCaptureError(
                "VALIDATION_EVIDENCE_OVERSIZED",
                "validation evidence exceeds the bounded row size",
            )
        return encoded.decode("utf-8"), hashlib.sha256(encoded).hexdigest()

    def _operator_dependency(
        self,
        evidence: Mapping[str, Any],
    ) -> tuple[str, str]:
        dependency = evidence.get("dependency")
        if not isinstance(dependency, Mapping):
            return "", ""
        if set(dependency) != {
            "authority_scope_id",
            "identity",
            "kind",
            "operation",
            "status",
        }:
            return "", ""
        kind = str(dependency.get("kind") or "").strip().upper()
        operation = str(dependency.get("operation") or "").strip().upper()
        scope = str(dependency.get("authority_scope_id") or "").strip()
        status = str(dependency.get("status") or "").strip().upper()
        identity = str(dependency.get("identity") or "").strip()
        if (
            kind != "OPERATION_GRANT"
            or operation != "CREATE_PACKAGE"
            or scope != self.binding.authority_scope_id
            or status != "PENDING"
            or identity != f"{operation}@{scope}"
        ):
            return "", ""
        return kind, f"{operation} grant · {scope} · 승인 대기"

    def _validate_verified_step_evidence(
        self,
        *,
        step_id: str,
        request_json: str,
        evidence_json: str,
    ) -> None:
        try:
            request = json.loads(request_json)
            evidence = json.loads(evidence_json)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise DeferredIntentCaptureError(
                "VALIDATION_EVIDENCE_INVALID",
                "verified validation evidence is not canonical JSON",
            ) from exc
        if not isinstance(request, dict) or not isinstance(evidence, dict):
            raise DeferredIntentCaptureError(
                "VALIDATION_EVIDENCE_INVALID",
                "verified validation evidence must be an object",
            )
        selected_step = str(step_id)
        if selected_step == "label-package-source":
            required = {
                "active_label_id",
                "authority_epoch",
                "authority_scope_id",
                "bundle_id",
                "contract_version",
                "entity_versions",
                "item_code",
                "ledger_plane",
                "local_work_identity",
                "member_count",
                "membership_hash",
                "observed_at",
                "package_bundle_id",
                "physical_qr_sha256",
                "plane_epoch",
                "source_resolution_basis",
                "topology_hash",
            }
            versions = evidence.get("entity_versions")
            if (
                set(evidence) != required
                or evidence.get("contract_version")
                != "label-validation-evidence-v1"
                or evidence.get("authority_scope_id")
                != self.binding.authority_scope_id
                or evidence.get("authority_scope_id")
                != request.get("authority_scope_id")
                or evidence.get("physical_qr_sha256")
                != request.get("physical_qr_sha256")
                or evidence.get("item_code") != request.get("item_code")
                or evidence.get("local_work_identity")
                != request.get("local_work_identity")
                or not str(evidence.get("active_label_id") or "")
                or not str(evidence.get("bundle_id") or "")
                or not str(evidence.get("package_bundle_id") or "")
                or str(evidence.get("ledger_plane") or "").upper()
                not in {"AUTHORITATIVE", "SHADOW_CANDIDATE"}
                or int(evidence.get("authority_epoch") or 0) < 1
                or int(evidence.get("plane_epoch") or 0) < 1
                or int(evidence.get("member_count") or 0) < 1
                or not _HEX_64_RE.fullmatch(
                    str(evidence.get("membership_hash") or "")
                )
                or not _HEX_64_RE.fullmatch(
                    str(evidence.get("physical_qr_sha256") or "")
                )
                or not _HEX_64_RE.fullmatch(
                    str(evidence.get("topology_hash") or "")
                )
                or not isinstance(versions, dict)
                or not versions
                or any(
                    not str(identity or "")
                    or isinstance(version, bool)
                    or not isinstance(version, int)
                    or version < 1
                    for identity, version in versions.items()
                )
            ):
                raise DeferredIntentCaptureError(
                    "VALIDATION_EVIDENCE_INCOMPLETE",
                    "the package-source evidence lacks exact authority or version fields",
                )
            _parse_utc(str(evidence.get("observed_at") or ""))
            return
        if selected_step == "label-operation-lease":
            required = {
                "authority_epoch",
                "authority_scope_id",
                "contract_version",
                "expires_at",
                "fence",
                "issued_at",
                "lease_id",
                "ledger_plane",
                "observed_at",
                "operation",
                "physical_qr_sha256",
                "plane_epoch",
                "snapshot_hash",
                "status",
            }
            issued_at = str(evidence.get("issued_at") or "")
            expires_at = str(evidence.get("expires_at") or "")
            observed_at = str(evidence.get("observed_at") or "")
            if (
                set(evidence) != required
                or evidence.get("contract_version")
                != "label-validation-evidence-v1"
                or evidence.get("authority_scope_id")
                != self.binding.authority_scope_id
                or evidence.get("authority_scope_id")
                != request.get("authority_scope_id")
                or evidence.get("operation") != request.get("operation")
                or evidence.get("physical_qr_sha256")
                != request.get("physical_qr_sha256")
                or str(evidence.get("ledger_plane") or "").upper()
                not in {"AUTHORITATIVE", "SHADOW_CANDIDATE"}
                or int(evidence.get("authority_epoch") or 0) < 1
                or int(evidence.get("plane_epoch") or 0) < 1
                or int(evidence.get("fence") or 0) < 1
                or not str(evidence.get("lease_id") or "")
                or str(evidence.get("status") or "") != "PREFETCHED"
                or not _HEX_64_RE.fullmatch(
                    str(evidence.get("physical_qr_sha256") or "")
                )
                or not _HEX_64_RE.fullmatch(
                    str(evidence.get("snapshot_hash") or "")
                )
                or _parse_utc(expires_at) <= _parse_utc(issued_at)
                or _parse_utc(expires_at) <= _parse_utc(observed_at)
            ):
                raise DeferredIntentCaptureError(
                    "VALIDATION_EVIDENCE_INCOMPLETE",
                    "the signed operation-lease evidence is incomplete or expired",
                )
            return
        raise DeferredIntentCaptureError(
            "VALIDATION_EVIDENCE_INVALID",
            "the validation step is not part of the Label plan",
        )

    def record_validation_step_valid(
        self,
        claim: DeferredValidationClaim,
        *,
        step_id: str,
        evidence: Mapping[str, Any],
        issued_at: str | None = None,
        expires_at: str | None = None,
        now: str | None = None,
    ) -> None:
        """Fence and persist one exact read-only step result."""

        observed_at = str(now or utc_now())
        selected_step = str(step_id or "").strip()
        evidence_json, evidence_hash = self._canonical_validation_evidence(evidence)
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._claimed_row(conn, claim, now=observed_at)
                cursor = conn.execute(
                    """UPDATE deferred_intent_validation_steps
                          SET status='VERIFIED',validation_outcome='VALID',
                              evidence_json=?,evidence_hash=?,issued_at=?,
                              expires_at=?,attempt_count=attempt_count+1,
                              last_error_code=NULL,updated_at=?
                        WHERE intent_id=? AND validation_generation=?
                          AND step_id=? AND fence=?
                          AND status IN ('PLANNED','CLAIMED','RETRY_WAIT')""",
                    (
                        evidence_json,
                        evidence_hash,
                        str(issued_at or observed_at),
                        str(expires_at or "") or None,
                        observed_at,
                        claim.intent_id,
                        claim.validation_generation,
                        selected_step,
                        claim.fence,
                    ),
                )
                if cursor.rowcount != 1:
                    raise DeferredIntentCaptureError(
                        "VALIDATION_STEP_FENCE_LOST",
                        "the validation step is no longer claimable",
                    )
                cursor = conn.execute(
                    """UPDATE deferred_intents
                          SET row_version=row_version+1,updated_at=?
                        WHERE intent_id=? AND state='VALIDATING'
                          AND fence=? AND claim_owner=?""",
                    (observed_at, claim.intent_id, claim.fence, claim.worker_id),
                )
                if cursor.rowcount != 1:
                    raise DeferredIntentCaptureError(
                        "VALIDATION_CLAIM_LOST",
                        "the validation step fence was lost",
                    )
                append_transition_audit(
                    conn,
                    intent_id=claim.intent_id,
                    from_state="VALIDATING",
                    to_state="VALIDATING",
                    transition_code="T5_VALIDATE_PLAN",
                    reason_code=f"{selected_step.upper()}_VERIFIED",
                    validation_outcome="VALID",
                    attempt_no=claim.validation_attempt_count,
                    worker_id=claim.worker_id,
                    fence=claim.fence,
                    occurred_at=observed_at,
                    evidence_hash=evidence_hash,
                )
                conn.commit()
            except DeferredIntentCaptureError:
                conn.rollback()
                raise
            except sqlite3.Error as exc:
                conn.rollback()
                raise DeferredIntentCaptureError(
                    "VALIDATION_STEP_PERSIST_FAILED",
                    "the validation step evidence could not be persisted",
                ) from exc
            finally:
                conn.close()

    def finish_validation(
        self,
        claim: DeferredValidationClaim,
        *,
        step_id: str,
        outcome: str,
        reason_code: str,
        evidence: Mapping[str, Any],
        issued_at: str | None = None,
        expires_at: str | None = None,
        retry_after_seconds: float | None = None,
        now: str | None = None,
    ) -> DeferredValidationResult:
        """Classify one of the seven outcomes and persist its sole destination."""

        observed_at = str(now or utc_now())
        selected_step = str(step_id or "").strip()
        selected_outcome = str(outcome or "").strip().upper()
        selected_reason = str(reason_code or "VALIDATION_RESULT").strip().upper()
        if selected_outcome not in VALIDATION_OUTCOMES or not selected_step:
            raise DeferredIntentCaptureError(
                "VALIDATION_OUTCOME_INVALID",
                "the typed validation outcome is invalid",
            )
        evidence_json, evidence_hash = self._canonical_validation_evidence(evidence)
        dependency_kind = ""
        dependency_identity = ""
        if selected_outcome == "REQUIRED_ABSENT":
            dependency_kind, dependency_identity = self._operator_dependency(
                evidence
            )
            if not dependency_kind:
                raise DeferredIntentCaptureError(
                    "VALIDATION_EVIDENCE_INVALID",
                    "required dependency evidence is not an exact safe identity",
                )
        destination = {
            "VALID": "VALIDATED",
            "RETRYABLE_UNAVAILABLE": "RETRY_WAIT_VALIDATION",
            "REQUIRED_ABSENT": "WAITING_DEPENDENCY",
            "ABSENT_MATERIALIZABLE": "VALIDATING",
            "INVALID": "BLOCKED_INVALID",
            "CONFLICT": "OPERATOR_REVIEW",
            "UNKNOWN_COMMIT": "RECONCILE_PENDING_VALIDATION",
        }[selected_outcome]
        step_status = {
            "VALID": "VERIFIED",
            "RETRYABLE_UNAVAILABLE": "RETRY_WAIT",
            "REQUIRED_ABSENT": "WAITING_DEPENDENCY",
            "ABSENT_MATERIALIZABLE": "MATERIALIZATION_REQUIRED",
            "INVALID": "BLOCKED_INVALID",
            "CONFLICT": "OPERATOR_REVIEW",
            "UNKNOWN_COMMIT": "RECONCILE_PENDING",
        }[selected_outcome]
        transition_code = {
            "VALID": "T11_FREEZE_VALIDATION",
            "RETRYABLE_UNAVAILABLE": "T6_VALIDATION_RETRY",
            "REQUIRED_ABSENT": "T9_WAIT_DEPENDENCY",
            "ABSENT_MATERIALIZABLE": "T7_CLASSIFY_ABSENCE",
            "INVALID": "T10_REJECT_OR_REVIEW",
            "CONFLICT": "T10_REJECT_OR_REVIEW",
            "UNKNOWN_COMMIT": "T6A_VALIDATION_UNKNOWN",
        }[selected_outcome]

        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                intent = dict(self._claimed_row(conn, claim, now=observed_at))
                step_before = conn.execute(
                    """SELECT * FROM deferred_intent_validation_steps
                         WHERE intent_id=? AND validation_generation=?
                           AND step_id=? AND fence=? LIMIT 1""",
                    (
                        claim.intent_id,
                        claim.validation_generation,
                        selected_step,
                        claim.fence,
                    ),
                ).fetchone()
                if step_before is None:
                    raise DeferredIntentCaptureError(
                        "VALIDATION_STEP_FENCE_LOST",
                        "the validation result step is unavailable",
                    )
                prior_step_status = str(step_before["status"])
                if selected_outcome in {"REQUIRED_ABSENT", "UNKNOWN_COMMIT"} and (
                    selected_step != "label-operation-lease"
                    or str(step_before["step_effect"])
                    != "IDEMPOTENT_MUTATION"
                    or prior_step_status != "REQUEST_RECORDED"
                    or not str(step_before["idempotency_key"] or "")
                    or not str(step_before["request_hash"] or "")
                ):
                    raise DeferredIntentCaptureError(
                        "VALIDATION_MUTATION_ATTEMPT_MISSING",
                        "the mutating validation result lacks its pre-dispatch T5A record",
                    )
                cursor = conn.execute(
                    """UPDATE deferred_intent_validation_steps
                          SET status=?,validation_outcome=?,evidence_json=?,
                              evidence_hash=?,issued_at=?,expires_at=?,
                              attempt_count=attempt_count + CASE
                                  WHEN status='REQUEST_RECORDED' THEN 0 ELSE 1 END,
                              last_error_code=?,
                              updated_at=?
                        WHERE intent_id=? AND validation_generation=?
                          AND step_id=? AND fence=?
                          AND status IN (
                              'PLANNED','CLAIMED','REQUEST_RECORDED','RETRY_WAIT'
                          )""",
                    (
                        step_status,
                        selected_outcome,
                        evidence_json,
                        evidence_hash,
                        str(issued_at or observed_at),
                        str(expires_at or "") or None,
                        None if selected_outcome == "VALID" else selected_reason,
                        observed_at,
                        claim.intent_id,
                        claim.validation_generation,
                        selected_step,
                        claim.fence,
                    ),
                )
                if cursor.rowcount != 1:
                    raise DeferredIntentCaptureError(
                        "VALIDATION_STEP_FENCE_LOST",
                        "the validation result step is no longer claimable",
                    )

                snapshot_hash = None
                validation_expires_at = None
                if selected_outcome == "VALID":
                    remaining = int(
                        conn.execute(
                            """SELECT COUNT(*)
                                 FROM deferred_intent_validation_steps
                                WHERE intent_id=? AND validation_generation=?
                                  AND status<>'VERIFIED'""",
                            (claim.intent_id, claim.validation_generation),
                        ).fetchone()[0]
                    )
                    if remaining:
                        raise DeferredIntentCaptureError(
                            "VALIDATION_PLAN_INCOMPLETE",
                            "ordered validation steps are not all verified",
                        )
                    step_rows = conn.execute(
                        """SELECT step_ordinal,step_id,step_kind,step_effect,
                                  validator_contract,validator_version,
                                  idempotency_key,request_json,request_hash,
                                  evidence_json,evidence_hash,issued_at,expires_at
                             FROM deferred_intent_validation_steps
                            WHERE intent_id=? AND validation_generation=?
                            ORDER BY step_ordinal""",
                        (claim.intent_id, claim.validation_generation),
                    ).fetchall()
                    if [str(row["step_id"]) for row in step_rows] != [
                        "label-package-source",
                        "label-operation-lease",
                    ]:
                        raise DeferredIntentCaptureError(
                            "VALIDATION_PLAN_INCOMPLETE",
                            "the exact ordered Label validation plan is incomplete",
                        )
                    for row in step_rows:
                        self._validate_verified_step_evidence(
                            step_id=str(row["step_id"]),
                            request_json=str(row["request_json"] or ""),
                            evidence_json=str(row["evidence_json"] or ""),
                        )
                    aggregate = [dict(row) for row in step_rows]
                    snapshot_hash = canonical_sha256(
                        {
                            "contract_version": CONTRACT_VERSION,
                            "intent_id": claim.intent_id,
                            "validation_generation": claim.validation_generation,
                            "steps": aggregate,
                        }
                    )
                    expiries = [
                        str(row["expires_at"])
                        for row in step_rows
                        if row["expires_at"]
                    ]
                    if not expiries:
                        raise DeferredIntentCaptureError(
                            "VALIDATION_EVIDENCE_INCOMPLETE",
                            "validated evidence has no bounded expiry",
                        )
                    validation_expires_at = min(expiries)

                next_attempt_at = None
                if selected_outcome == "RETRYABLE_UNAVAILABLE":
                    if retry_after_seconds is None:
                        retry_delay = self._validation_retry_delay(
                            int(intent.get("validation_attempt_count") or 0)
                        )
                    else:
                        retry_delay = max(
                            0.0,
                            min(
                                MAX_VALIDATION_RETRY_SECONDS,
                                float(retry_after_seconds),
                            ),
                        )
                    next_attempt_at = _utc_after(observed_at, retry_delay)

                keep_claim = selected_outcome == "ABSENT_MATERIALIZABLE"
                cursor = conn.execute(
                    """UPDATE deferred_intents
                          SET state=?,validation_snapshot_hash=?,
                              validation_expires_at=?,next_attempt_at=?,
                              claim_owner=?,claim_expires_at=?,
                              last_reason_code=?,last_error_code=?,
                              row_version=row_version+1,updated_at=?
                        WHERE intent_id=? AND state='VALIDATING'
                          AND fence=? AND claim_owner=?""",
                    (
                        destination,
                        snapshot_hash,
                        validation_expires_at,
                        next_attempt_at,
                        claim.worker_id if keep_claim else None,
                        claim.claim_expires_at if keep_claim else None,
                        selected_reason,
                        None if selected_outcome == "VALID" else selected_reason,
                        observed_at,
                        claim.intent_id,
                        claim.fence,
                        claim.worker_id,
                    ),
                )
                if cursor.rowcount != 1:
                    raise DeferredIntentCaptureError(
                        "VALIDATION_CLAIM_LOST",
                        "the typed validation result fence was lost",
                    )
                if selected_outcome == "REQUIRED_ABSENT":
                    append_transition_audit(
                        conn,
                        intent_id=claim.intent_id,
                        from_state="VALIDATING",
                        to_state="VALIDATING",
                        transition_code="T7_CLASSIFY_ABSENCE",
                        reason_code=selected_reason,
                        validation_outcome=selected_outcome,
                        attempt_no=claim.validation_attempt_count,
                        worker_id=claim.worker_id,
                        fence=claim.fence,
                        occurred_at=observed_at,
                        evidence_hash=evidence_hash,
                    )
                append_transition_audit(
                    conn,
                    intent_id=claim.intent_id,
                    from_state="VALIDATING",
                    to_state=destination,
                    transition_code=transition_code,
                    reason_code=selected_reason,
                    validation_outcome=selected_outcome,
                    attempt_no=claim.validation_attempt_count,
                    worker_id=claim.worker_id,
                    fence=claim.fence,
                    occurred_at=observed_at,
                    evidence_hash=(snapshot_hash or evidence_hash),
                )
                pending, age = self._pending_summary(conn, observed_at)
                conn.commit()
                return DeferredValidationResult(
                    intent_id=claim.intent_id,
                    state=destination,
                    outcome=selected_outcome,
                    reason_code=selected_reason,
                    observed_at=observed_at,
                    next_attempt_at=next_attempt_at,
                    dependency_kind=dependency_kind,
                    dependency_identity=dependency_identity,
                    pending_count=pending,
                    oldest_age_seconds=age,
                )
            except DeferredIntentCaptureError:
                conn.rollback()
                raise
            except (TypeError, ValueError, sqlite3.Error) as exc:
                conn.rollback()
                raise DeferredIntentCaptureError(
                    "VALIDATION_RESULT_PERSIST_FAILED",
                    "the typed validation result could not be persisted",
                ) from exc
            finally:
                conn.close()

    def status_readback(
        self,
        *,
        now: str | None = None,
        previous_operator_review_count: int | None = None,
        limit: int = DEFAULT_STATUS_READBACK_LIMIT,
    ) -> DeferredIntentStatusReadback:
        """Return a bounded, secret-free, local-only operational snapshot."""

        started = time.perf_counter()
        observed_at = str(now or utc_now())
        _parse_utc(observed_at)
        selected_limit = min(100, max(1, int(limit)))
        terminal_placeholders = ",".join("?" for _ in _TERMINAL_STATES)
        conn = self._connect()
        try:
            conn.execute("PRAGMA query_only=ON")
            # Pin every SELECT below to one coherent SQLite snapshot so a
            # concurrent validator cannot make aggregates and detail disagree.
            conn.execute("BEGIN")
            aggregate_rows = conn.execute(
                """SELECT state,COUNT(*) AS row_count,
                          MIN(created_at) AS oldest_at,
                          MIN(updated_at) AS oldest_state_at
                     FROM deferred_intents GROUP BY state"""
            ).fetchall()
            aggregates = {
                str(row["state"]): {
                    "count": int(row["row_count"] or 0),
                    "oldest_at": str(row["oldest_at"] or ""),
                    "oldest_state_at": str(row["oldest_state_at"] or ""),
                }
                for row in aggregate_rows
            }
            state_counts = tuple(
                (
                    state,
                    int((aggregates.get(state) or {}).get("count") or 0),
                )
                for state in DEFERRED_INTENT_STATES
            )
            operator_groups = []
            for key, operator_status, states in DEFERRED_OPERATOR_STATUS_GROUPS:
                oldest_values = tuple(
                    str((aggregates.get(state) or {}).get("oldest_at") or "")
                    for state in states
                    if str((aggregates.get(state) or {}).get("oldest_at") or "")
                )
                oldest_at = min(oldest_values) if oldest_values else ""
                operator_groups.append(
                    DeferredOperatorStatusGroup(
                        key=key,
                        operator_status=operator_status,
                        internal_states=tuple(states),
                        count=sum(
                            int((aggregates.get(state) or {}).get("count") or 0)
                            for state in states
                        ),
                        oldest_at=oldest_at,
                        oldest_age_seconds=_age_seconds(oldest_at, observed_at),
                    )
                )

            total_count = sum(count for _state, count in state_counts)
            nonterminal_count = sum(
                count for state, count in state_counts if state not in _TERMINAL_STATES
            )
            closed_incomplete_count = sum(
                int((aggregates.get(state) or {}).get("count") or 0)
                for state in CLOSED_INCOMPLETE_STATES
            )
            oldest = conn.execute(
                f"""SELECT intent_id,state,created_at
                       FROM deferred_intents
                      WHERE state NOT IN ({terminal_placeholders})
                      ORDER BY created_at,intent_id LIMIT 1""",
                tuple(sorted(_TERMINAL_STATES)),
            ).fetchone()

            retry_rows = conn.execute(
                """SELECT intent_id,state,partition_key,partition_seq,
                          next_attempt_at,last_reason_code
                     FROM deferred_intents
                    WHERE state IN ('RETRY_WAIT_VALIDATION','RETRY_WAIT_SUBMIT')
                      AND next_attempt_at IS NOT NULL
                    ORDER BY next_attempt_at,intent_id LIMIT ?""",
                (selected_limit,),
            ).fetchall()
            retry_schedule = tuple(
                DeferredRetrySchedule(
                    intent_id=_operator_safe_identifier(row["intent_id"]),
                    state=str(row["state"]),
                    partition_key=_operator_safe_identifier(row["partition_key"]),
                    partition_seq=int(row["partition_seq"]),
                    next_attempt_at=str(row["next_attempt_at"]),
                    reason_code=_operator_safe_code(row["last_reason_code"]),
                )
                for row in retry_rows
            )

            last_reason = conn.execute(
                """SELECT last_reason_code,updated_at
                     FROM deferred_intents
                    WHERE last_reason_code IS NOT NULL
                    ORDER BY updated_at DESC,intent_id LIMIT 1"""
            ).fetchone()

            dependency_identity = ""
            dependency_checked_at = ""
            dependency_row = conn.execute(
                """SELECT intent_id FROM deferred_intents
                    WHERE state='WAITING_DEPENDENCY'
                    ORDER BY created_at,intent_id LIMIT 1"""
            ).fetchone()
            if dependency_row is not None:
                evidence_row = conn.execute(
                    """SELECT evidence_json,updated_at
                         FROM deferred_intent_validation_steps
                        WHERE intent_id=? AND validation_outcome='REQUIRED_ABSENT'
                          AND evidence_json IS NOT NULL
                        ORDER BY validation_generation DESC,step_ordinal DESC LIMIT 1""",
                    (dependency_row["intent_id"],),
                ).fetchone()
                if evidence_row is not None:
                    try:
                        decoded = json.loads(str(evidence_row["evidence_json"]))
                        if isinstance(decoded, Mapping):
                            _kind, dependency_identity = self._operator_dependency(decoded)
                            dependency_checked_at = str(evidence_row["updated_at"] or "")
                    except (TypeError, ValueError, json.JSONDecodeError):
                        dependency_identity = ""
                        dependency_checked_at = ""

            blocked_rows = conn.execute(
                f"""WITH open_intents AS (
                         SELECT app_id,producer_install_id,authority_scope_id,
                                partition_key,partition_seq
                           FROM deferred_intents
                          WHERE state NOT IN ({terminal_placeholders})
                     ), blocked_heads AS (
                         SELECT app_id,producer_install_id,authority_scope_id,
                                partition_key,MIN(partition_seq) AS head_seq,
                                COUNT(*) AS open_count
                           FROM open_intents
                          GROUP BY app_id,producer_install_id,authority_scope_id,
                                   partition_key
                         HAVING COUNT(*)>1
                     )
                     SELECT item.partition_key,item.intent_id,item.state,
                            item.partition_seq,item.created_at,item.updated_at,
                            item.last_reason_code,
                            heads.open_count-1 AS blocked_count
                       FROM blocked_heads AS heads
                       JOIN deferred_intents AS item
                         ON item.app_id=heads.app_id
                        AND item.producer_install_id=heads.producer_install_id
                        AND item.authority_scope_id=heads.authority_scope_id
                        AND item.partition_key=heads.partition_key
                        AND item.partition_seq=heads.head_seq
                      ORDER BY item.created_at,item.intent_id LIMIT ?""",
                (*tuple(sorted(_TERMINAL_STATES)), selected_limit),
            ).fetchall()
            blocked_partitions = tuple(
                DeferredBlockedPartition(
                    partition_key=_operator_safe_identifier(row["partition_key"]),
                    head_intent_id=_operator_safe_identifier(row["intent_id"]),
                    head_state=str(row["state"]),
                    head_partition_seq=int(row["partition_seq"]),
                    blocked_count=int(row["blocked_count"]),
                    head_created_at=str(row["created_at"]),
                    head_age_seconds=_age_seconds(row["created_at"], observed_at),
                    head_blocked_since=str(row["updated_at"]),
                    head_blocked_age_seconds=_age_seconds(
                        row["updated_at"], observed_at
                    ),
                    reason_code=_operator_safe_code(row["last_reason_code"]),
                )
                for row in blocked_rows
            )

            downstream_rows = conn.execute(
                """SELECT downstream_outbox_ref FROM deferred_intents
                    WHERE downstream_outbox_ref IS NOT NULL
                    ORDER BY updated_at DESC,intent_id LIMIT ?""",
                (selected_limit,),
            ).fetchall()
            downstream_refs = tuple(
                _operator_safe_identifier(row["downstream_outbox_ref"])
                for row in downstream_rows
            )

            quarantine_rows = conn.execute(
                f"""SELECT intent_id,state,payload_hash,last_reason_code,
                            created_at,updated_at
                       FROM deferred_intents
                      WHERE {QUARANTINE_PREDICATE_SQL}
                      ORDER BY created_at,intent_id LIMIT ?""",
                (selected_limit,),
            ).fetchall()
            quarantine = tuple(
                DeferredQuarantineRecord(
                    intent_id=_operator_safe_identifier(row["intent_id"]),
                    state=str(row["state"]),
                    payload_hash=str(row["payload_hash"]),
                    reason_code=_operator_safe_code(row["last_reason_code"]),
                    created_at=str(row["created_at"]),
                    updated_at=str(row["updated_at"]),
                )
                for row in quarantine_rows
            )

            thresholds = dict(DEFERRED_ALERT_THRESHOLDS)
            alerts = []
            retry_oldest = min(
                (
                    str(
                        (aggregates.get(state) or {}).get("oldest_state_at") or ""
                    )
                    for state in RETRY_WAIT_STATES
                    if str(
                        (aggregates.get(state) or {}).get("oldest_state_at") or ""
                    )
                ),
                default="",
            )
            retry_age = _age_seconds(retry_oldest, observed_at)
            if retry_age >= thresholds["oldest_retry_wait_seconds"]:
                alerts.append(
                    DeferredStatusAlert(
                        code="OLDEST_RETRY_WAIT_SLA_EXCEEDED",
                        severity="warning",
                        observed_value=retry_age,
                        threshold=thresholds["oldest_retry_wait_seconds"],
                    )
                )
            waiting_oldest = str(
                (aggregates.get("WAITING_DEPENDENCY") or {}).get(
                    "oldest_state_at"
                )
                or ""
            )
            waiting_age = _age_seconds(waiting_oldest, observed_at)
            if waiting_age >= thresholds["waiting_dependency_seconds"]:
                alerts.append(
                    DeferredStatusAlert(
                        code="WAITING_DEPENDENCY_SLA_EXCEEDED",
                        severity="warning",
                        observed_value=waiting_age,
                        threshold=thresholds["waiting_dependency_seconds"],
                    )
                )
            review_count = int(
                (aggregates.get("OPERATOR_REVIEW") or {}).get("count") or 0
            )
            if previous_operator_review_count is not None:
                review_delta = max(0, review_count - int(previous_operator_review_count))
                if review_delta >= thresholds["operator_review_increase"]:
                    alerts.append(
                        DeferredStatusAlert(
                            code="OPERATOR_REVIEW_INCREASE",
                            severity="warning",
                            observed_value=review_delta,
                            threshold=thresholds["operator_review_increase"],
                        )
                    )
            seal_failures = int(
                conn.execute(
                    """SELECT COUNT(*) FROM deferred_intents
                        WHERE state='BLOCKED_INVALID'
                          AND last_reason_code='LOCAL_SEAL_INVALID'"""
                ).fetchone()[0]
            )
            if seal_failures >= thresholds["repeated_seal_failure_count"]:
                alerts.append(
                    DeferredStatusAlert(
                        code="REPEATED_SEAL_FAILURE",
                        severity="danger",
                        observed_value=seal_failures,
                        threshold=thresholds["repeated_seal_failure_count"],
                    )
                )
            for partition in blocked_partitions:
                if (
                    partition.head_blocked_age_seconds
                    >= thresholds["partition_starvation_seconds"]
                ):
                    alerts.append(
                        DeferredStatusAlert(
                            code="PARTITION_STARVATION",
                            severity="warning",
                            observed_value=partition.head_blocked_age_seconds,
                            threshold=thresholds["partition_starvation_seconds"],
                            partition_key=partition.partition_key,
                        )
                    )

            duration_ms = round((time.perf_counter() - started) * 1000.0, 3)
            return DeferredIntentStatusReadback(
                observed_at=observed_at,
                state_counts=state_counts,
                operator_groups=tuple(operator_groups),
                total_count=total_count,
                nonterminal_count=nonterminal_count,
                oldest_intent_id=(
                    _operator_safe_identifier(oldest["intent_id"]) if oldest else ""
                ),
                oldest_state=(str(oldest["state"]) if oldest else ""),
                oldest_age_seconds=(
                    _age_seconds(oldest["created_at"], observed_at) if oldest else 0
                ),
                retry_schedule=retry_schedule,
                last_reason_code=(
                    _operator_safe_code(last_reason["last_reason_code"])
                    if last_reason
                    else ""
                ),
                last_reason_at=(str(last_reason["updated_at"]) if last_reason else ""),
                dependency_identity=dependency_identity,
                dependency_checked_at=dependency_checked_at,
                blocked_partitions=blocked_partitions,
                downstream_outbox_refs=downstream_refs,
                quarantine=quarantine,
                quarantine_count=int(
                    (aggregates.get("BLOCKED_INVALID") or {}).get("count") or 0
                ),
                closed_incomplete_count=closed_incomplete_count,
                alerts=tuple(alerts),
                alert_thresholds=DEFERRED_ALERT_THRESHOLDS,
                read_duration_ms=duration_ms,
                auto_prune_enabled=False,
            )
        finally:
            conn.close()

    def validation_status(
        self,
        intent_id: str,
        *,
        now: str | None = None,
    ) -> DeferredValidationResult | None:
        """Return secret-free durable operator state for one intent."""

        selected_id = str(intent_id or "").strip()
        observed_at = str(now or utc_now())
        conn = self._connect()
        try:
            row = conn.execute(
                """SELECT intent_id,state,last_reason_code,next_attempt_at
                     FROM deferred_intents WHERE intent_id=? LIMIT 1""",
                (selected_id,),
            ).fetchone()
            if row is None:
                return None
            evidence_row = conn.execute(
                """SELECT validation_outcome,evidence_json,updated_at
                     FROM deferred_intent_validation_steps
                    WHERE intent_id=? AND evidence_json IS NOT NULL
                    ORDER BY validation_generation DESC,step_ordinal DESC LIMIT 1""",
                (selected_id,),
            ).fetchone()
            evidence: Mapping[str, Any] = {}
            outcome = ""
            checked_at = observed_at
            if evidence_row is not None:
                try:
                    decoded = json.loads(str(evidence_row["evidence_json"]))
                    if isinstance(decoded, Mapping):
                        evidence = decoded
                except (TypeError, ValueError, json.JSONDecodeError):
                    evidence = {}
                outcome = str(evidence_row["validation_outcome"] or "")
                checked_at = str(evidence_row["updated_at"] or observed_at)
            dependency_kind, dependency_identity = self._operator_dependency(
                evidence
            )
            pending, age = self._pending_summary(conn, observed_at)
            return DeferredValidationResult(
                intent_id=selected_id,
                state=str(row["state"]),
                outcome=outcome,
                reason_code=str(row["last_reason_code"] or ""),
                observed_at=checked_at,
                next_attempt_at=(str(row["next_attempt_at"]) if row["next_attempt_at"] else None),
                dependency_kind=dependency_kind,
                dependency_identity=dependency_identity,
                pending_count=pending,
                oldest_age_seconds=age,
            )
        finally:
            conn.close()

    def next_validation_candidate(self, *, now: str | None = None) -> str | None:
        """Return the oldest eligible or expired-claim FIFO validation row."""

        observed_at = str(now or utc_now())
        conn = self._connect()
        try:
            row = conn.execute(
                """SELECT candidate.intent_id
                     FROM deferred_intents AS candidate
                    WHERE (
                          candidate.state IN (
                              'CAPTURED_UNVERIFIED','RETRY_WAIT_VALIDATION'
                          )
                          OR (
                              candidate.state='VALIDATING'
                              AND candidate.claim_expires_at IS NOT NULL
                              AND candidate.claim_expires_at<=?
                          )
                      )
                      AND (
                          candidate.state<>'RETRY_WAIT_VALIDATION'
                          OR candidate.next_attempt_at IS NULL
                          OR candidate.next_attempt_at<=?
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM deferred_intents AS predecessor
                           WHERE predecessor.app_id=candidate.app_id
                             AND predecessor.producer_install_id=
                                 candidate.producer_install_id
                             AND predecessor.authority_scope_id=
                                 candidate.authority_scope_id
                             AND predecessor.partition_key=candidate.partition_key
                             AND predecessor.partition_seq<candidate.partition_seq
                             AND predecessor.state NOT IN (
                                 'COMPLETED','CANCELLED','SUPERSEDED'
                             )
                      )
                    ORDER BY candidate.created_at,candidate.intent_id LIMIT 1""",
                (observed_at, observed_at),
            ).fetchone()
            return str(row[0]) if row else None
        finally:
            conn.close()

    def get(self, intent_id: str) -> dict[str, Any] | None:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM deferred_intents WHERE intent_id=? LIMIT 1",
                (str(intent_id or "").strip(),),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()


__all__ = [
    "CAPTURE_C14N_VERSION",
    "CAPTURE_SCHEMA_VERSION",
    "CLOSED_INCOMPLETE_STATES",
    "CONTRACT_VERSION",
    "DEFAULT_STATUS_READBACK_LIMIT",
    "DEFERRED_ALERT_THRESHOLDS",
    "DEFERRED_INTENT_SCHEMA_SQL",
    "DEFERRED_INTENT_STATES",
    "DEFERRED_OPERATOR_STATUS_GROUPS",
    "DEFERRED_OPERATOR_STATE_LABELS",
    "DeferredBlockedPartition",
    "DeferredCaptureResult",
    "DeferredIntentBinding",
    "DeferredIntentCaptureError",
    "DeferredIntentStatusReadback",
    "DeferredIntentCaptureStore",
    "DeferredOperatorStatusGroup",
    "DeferredQuarantineRecord",
    "DeferredRetrySchedule",
    "DeferredStatusAlert",
    "DeferredValidationClaim",
    "DeferredValidationResult",
    "LABEL_APP_ID",
    "LABEL_INTENT_KIND",
    "LABEL_PARTITION_KEY",
    "LABEL_VALIDATION_STEPS",
    "LEGACY_CONTRACT_VERSION",
    "LEGACY_PAYLOAD_PROTECTION",
    "PAYLOAD_PROTECTION",
    "PAYLOAD_PROTECTION_PURPOSE",
    "QUARANTINE_EXCLUSION_PREDICATE_SQL",
    "QUARANTINE_PREDICATE_SQL",
    "QUARANTINE_STATES",
    "RETRY_WAIT_STATES",
    "VALIDATION_OUTCOMES",
    "append_transition_audit",
    "canonical_json_bytes",
    "canonical_sha256",
    "common_reader_v2_entropy",
    "ensure_deferred_intent_schema_compatibility",
    "operator_safe_reason_code",
    "payload_protection_entropy",
    "supersede_for_legacy_outbox",
]

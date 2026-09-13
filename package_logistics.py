"""Durable exact-membership packaging integration for Label_Match.

The legacy three product scans are QA samples.  They are never promoted to the
package membership.  Authoritative membership is inherited from a sealed
TRANSFER bundle, or supplied as a separate full exact rescan.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import hashlib
import ipaddress
from http.client import HTTPException, IncompleteRead
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import ssl
import threading
from typing import Any, Callable, Iterable, Iterator, Mapping
import unicodedata
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import Request, urlopen

from logistics_runtime_profile import (
    LogisticsRuntimeConfigurationError,
    load_logistics_runtime_profile,
    logistics_runtime_required,
)
from deferred_intent_capture import (
    DEFERRED_INTENT_SCHEMA_SQL,
    ensure_deferred_intent_schema_compatibility,
    supersede_for_legacy_outbox,
)
from writer_session_fence import writer_sink
import carrier_identity_port as carrier_identity
from package_validation import (
    _validate_projection,
    _validate_transfer_seal,
    _validate_work_group_source,
)
from package_outbox_processor import PackageOutboxProcessor as _PackageOutboxProcessor
from package_outbox import PackageOutbox as _PackageOutbox
from package_errors import (
    MAX_RETRY_AFTER_SECONDS,
    PackageLogisticsError,
    PackageTransportError,
    PackageApiError,
    _bounded_retry_after_seconds,
)
from package_command_draft import (
    PACKAGE_CONTRACT_VERSION,
    MEMBERSHIP_MODES,
    canonical_member_ids,
    canonical_barcodes,
    canonical_member_barcodes,
    membership_hash,
    barcode_membership_hash,
    stable_id,
    canonical_json,
    canonical_sha256,
    _attach_operation_lease,
    _json_clone_mapping,
    _strict_int,
    _strict_member_ids,
    _strict_entity_versions,
    _normalize_barcode,
    PackageCommandDraft,
)


OUTBOX_SCHEMA_VERSION = "label-match-package-outbox-v10"
POST_REVIEW_EVENT_VERSION = "label-match-post-review-required-v1"
PACKAGE_CANCELLATION_EVENT_TYPES = {"SET_DELETED", "TRAY_COMPLETION_CANCELLED"}
PACKAGE_HTTP_USER_AGENT = "KMTech-Worker-ClaimClient/1.0 LabelMatch"
PACKAGE_HTTP_CLIENT_HEADER = "Label_Match"
SENDING_LEASE_SECONDS = 300.0


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _utc_after(seconds: float) -> str:
    bounded = _bounded_retry_after_seconds(seconds)
    return (
        datetime.now(timezone.utc) + timedelta(seconds=bounded or 0.0)
    ).isoformat().replace("+00:00", "Z")


def _utc_before(seconds: float) -> str:
    bounded = _bounded_retry_after_seconds(seconds)
    return (
        datetime.now(timezone.utc) - timedelta(seconds=bounded or 0.0)
    ).isoformat().replace("+00:00", "Z")


def _optional_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    normalized = str(value or "").strip().lower()
    if normalized in {"1", "true", "yes"}:
        return True
    if normalized in {"0", "false", "no"}:
        return False
    return None


def _first_optional_bool(*values: Any) -> bool | None:
    for value in values:
        parsed = _optional_bool(value)
        if parsed is not None:
            return parsed
    return None


def _parse_retry_after_seconds(value: Any) -> float | None:
    normalized = str(value if value is not None else "").strip()
    if not normalized:
        return None
    try:
        return _bounded_retry_after_seconds(float(normalized))
    except ValueError:
        try:
            parsed = parsedate_to_datetime(normalized)
        except (TypeError, ValueError, OverflowError):
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return _bounded_retry_after_seconds(
            (parsed - datetime.now(timezone.utc)).total_seconds()
        )


@dataclass(frozen=True)
class PackageCancellationIntent:
    cancellation_event_id: str
    set_id: str
    event_type: str
    reason: str
    evidence: Mapping[str, Any]
    local_event_details: Mapping[str, Any]

    @classmethod
    def build(
        cls,
        *,
        set_id: str,
        event_type: str,
        reason: str,
        evidence: Mapping[str, Any] | None = None,
        local_event_details: Mapping[str, Any] | None = None,
        cancellation_event_id: str = "",
    ) -> "PackageCancellationIntent":
        normalized_set_id = str(set_id or "").strip()
        normalized_event_type = str(event_type or "").strip().upper()
        normalized_reason = str(reason or "").strip()
        if not normalized_set_id:
            raise PackageLogisticsError("package cancellation set_id is required")
        if normalized_event_type not in PACKAGE_CANCELLATION_EVENT_TYPES:
            raise PackageLogisticsError("package cancellation event type is invalid")
        if not normalized_reason:
            raise PackageLogisticsError("package cancellation reason is required")
        try:
            normalized_evidence = json.loads(
                json.dumps(dict(evidence or {}), ensure_ascii=False, sort_keys=True, default=str)
            )
            normalized_local_details = json.loads(
                json.dumps(
                    dict(local_event_details or {}),
                    ensure_ascii=False,
                    sort_keys=True,
                    default=str,
                )
            )
        except (TypeError, ValueError) as exc:
            raise PackageLogisticsError(
                "package cancellation evidence is not JSON serializable"
            ) from exc
        event_id = str(cancellation_event_id or "").strip() or stable_id(
            "package-cancel-event", normalized_set_id, normalized_event_type
        )
        return cls(
            cancellation_event_id=event_id,
            set_id=normalized_set_id,
            event_type=normalized_event_type,
            reason=normalized_reason,
            evidence=normalized_evidence,
            local_event_details=normalized_local_details,
        )

    def fingerprint(self) -> str:
        return hashlib.sha256(
            json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "cancellation_event_id": self.cancellation_event_id,
            "set_id": self.set_id,
            "event_type": self.event_type,
            "reason": self.reason,
            "evidence": dict(self.evidence),
            "local_event_details": dict(self.local_event_details),
        }


def _post_review_event_payload(
    row: Mapping[str, Any],
    *,
    conflict_code: str,
    conflict_origin: str,
    required_at: str,
) -> tuple[str, dict[str, Any], str]:
    key = str(row.get("idempotency_key") or "").strip()
    set_id = str(row.get("set_id") or "").strip()
    if not key or not set_id:
        raise PackageLogisticsError(
            "post-review package conflict identity is incomplete"
        )
    try:
        draft = json.loads(str(row.get("draft_json") or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise PackageLogisticsError(
            "post-review package draft is unreadable"
        ) from exc
    if not isinstance(draft, Mapping):
        raise PackageLogisticsError(
            "post-review package draft is invalid"
        )
    package_bundle_id = str(draft.get("package_bundle_id") or "").strip()
    normalized_code = str(conflict_code or "LOCAL_VALIDATION_CONFLICT").strip()
    normalized_origin = str(conflict_origin or "").strip()
    normalized_required_at = str(required_at or "").strip()
    if not package_bundle_id or not normalized_origin or not normalized_required_at:
        raise PackageLogisticsError(
            "post-review package conflict evidence is incomplete"
        )
    review_event_id = stable_id("label-package-review", key)
    event = {
        "case_id": review_event_id,
        "case_status": "OPEN",
        "case_type": "PACKAGE_CREATE_POST_LOCAL_CONFLICT",
        "conflict_code": normalized_code,
        "conflict_origin": normalized_origin,
        "dedupe_key": review_event_id,
        "event_version": POST_REVIEW_EVENT_VERSION,
        "local_completion_committed": True,
        "package_bundle_id": package_bundle_id,
        "package_idempotency_key": key,
        "required_at": normalized_required_at,
        "review_event_id": review_event_id,
        "review_status": "OPERATOR_REVIEW",
        "set_id": set_id,
    }
    encoded = canonical_json(event)
    fingerprint = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    return review_event_id, event, fingerprint


def _post_review_conflict_origin(error: Exception) -> str:
    if isinstance(error, PackageApiError):
        return "PACKAGE_API_CONFLICT"
    return "LOCAL_VALIDATION_OR_RECEIPT_CONFLICT"


def _initialize_outbox_schema(conn: sqlite3.Connection) -> None:
    """Atomically install the current schema without disturbing live SENDING leases."""

    ensure_deferred_intent_schema_compatibility(conn)
    cancellation_table_existed = (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='package_cancellation_outbox'"
        ).fetchone()
        is not None
    )
    conn.executescript(
        """
        BEGIN IMMEDIATE;
        CREATE TABLE IF NOT EXISTS package_command_outbox (
            idempotency_key TEXT PRIMARY KEY,
            set_id TEXT NOT NULL UNIQUE,
            command_fingerprint TEXT NOT NULL,
            draft_json TEXT NOT NULL,
            resolved_source_bundle_id TEXT,
            command_json TEXT,
            status TEXT NOT NULL CHECK(status IN ('PENDING','SENDING','ACKED','CONFLICT')),
            attempt_count INTEGER NOT NULL DEFAULT 0,
            last_error_code TEXT,
            last_error_message TEXT,
            review_status TEXT
                CHECK(review_status IN ('OPERATOR_REVIEW','MANUAL_REVIEW')
                      OR review_status IS NULL),
            receipt_json TEXT,
            retry_after_at TEXT,
            last_attempt_at TEXT,
            local_completion_committed INTEGER NOT NULL DEFAULT 0
                CHECK(local_completion_committed IN (0,1)),
            local_completion_committed_at TEXT,
            local_recovery_dismissed INTEGER NOT NULL DEFAULT 0
                CHECK(local_recovery_dismissed IN (0,1)),
            local_recovery_dismissed_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_package_command_outbox_status
            ON package_command_outbox(status, created_at);
        CREATE TABLE IF NOT EXISTS package_cancellation_outbox (
            idempotency_key TEXT PRIMARY KEY,
            cancellation_event_id TEXT NOT NULL UNIQUE,
            set_id TEXT NOT NULL,
            package_idempotency_key TEXT NOT NULL,
            package_bundle_id TEXT NOT NULL,
            intent_fingerprint TEXT NOT NULL,
            intent_json TEXT NOT NULL,
            authority_scope_id TEXT,
            authority_epoch INTEGER,
            ledger_plane TEXT,
            plane_epoch INTEGER,
            expected_bundle_version INTEGER,
            command_json TEXT,
            status TEXT NOT NULL CHECK(status IN ('DEFERRED','PENDING','SENDING','ACKED','CONFLICT')),
            attempt_count INTEGER NOT NULL DEFAULT 0,
            last_error_code TEXT,
            last_error_message TEXT,
            receipt_json TEXT,
            local_event_committed INTEGER NOT NULL DEFAULT 0
                CHECK(local_event_committed IN (0,1)),
            local_event_committed_at TEXT,
            retry_after_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(package_idempotency_key)
                REFERENCES package_command_outbox(idempotency_key)
        );
        CREATE INDEX IF NOT EXISTS ix_package_cancellation_outbox_status
            ON package_cancellation_outbox(status, created_at);
        CREATE INDEX IF NOT EXISTS ix_package_cancellation_outbox_set
            ON package_cancellation_outbox(set_id, created_at);
        CREATE TABLE IF NOT EXISTS package_replacement_waiting_outbox (
            dedupe_key TEXT PRIMARY KEY,
            event_fingerprint TEXT NOT NULL,
            event_json TEXT NOT NULL,
            local_csv_committed INTEGER NOT NULL DEFAULT 0
                CHECK(local_csv_committed IN (0,1)),
            local_csv_committed_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS ix_package_replacement_waiting_pending
            ON package_replacement_waiting_outbox(
                local_csv_committed,created_at,dedupe_key
            );
        CREATE TABLE IF NOT EXISTS package_post_review_outbox (
            review_event_id TEXT PRIMARY KEY,
            package_idempotency_key TEXT NOT NULL UNIQUE,
            event_fingerprint TEXT NOT NULL,
            event_json TEXT NOT NULL,
            local_csv_committed INTEGER NOT NULL DEFAULT 0
                CHECK(local_csv_committed IN (0,1)),
            local_csv_committed_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(package_idempotency_key)
                REFERENCES package_command_outbox(idempotency_key)
        );
        CREATE INDEX IF NOT EXISTS ix_package_post_review_pending
            ON package_post_review_outbox(
                local_csv_committed,created_at,review_event_id
            );
        CREATE TABLE IF NOT EXISTS package_outbox_schema_info (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
        + DEFERRED_INTENT_SCHEMA_SQL
    )
    command_columns = {
        str(row["name"] if isinstance(row, sqlite3.Row) else row[1])
        for row in conn.execute("PRAGMA table_info(package_command_outbox)").fetchall()
    }
    if "retry_after_at" not in command_columns:
        conn.execute(
            "ALTER TABLE package_command_outbox ADD COLUMN retry_after_at TEXT"
        )
    if "review_status" not in command_columns:
        conn.execute(
            """ALTER TABLE package_command_outbox
                   ADD COLUMN review_status TEXT
                   CHECK(review_status IN ('OPERATOR_REVIEW','MANUAL_REVIEW')
                         OR review_status IS NULL)"""
        )
    if "last_attempt_at" not in command_columns:
        conn.execute(
            "ALTER TABLE package_command_outbox ADD COLUMN last_attempt_at TEXT"
        )
    if "local_completion_committed" not in command_columns:
        conn.execute(
            """ALTER TABLE package_command_outbox
                   ADD COLUMN local_completion_committed INTEGER NOT NULL DEFAULT 0
                   CHECK(local_completion_committed IN (0,1))"""
        )
    if "local_completion_committed_at" not in command_columns:
        conn.execute(
            "ALTER TABLE package_command_outbox ADD COLUMN local_completion_committed_at TEXT"
        )
    if "local_recovery_dismissed" not in command_columns:
        conn.execute(
            """ALTER TABLE package_command_outbox
                   ADD COLUMN local_recovery_dismissed INTEGER NOT NULL DEFAULT 0
                   CHECK(local_recovery_dismissed IN (0,1))"""
        )
    if "local_recovery_dismissed_at" not in command_columns:
        conn.execute(
            "ALTER TABLE package_command_outbox ADD COLUMN local_recovery_dismissed_at TEXT"
        )
    conn.execute(
        """CREATE INDEX IF NOT EXISTS ix_package_command_outbox_due
               ON package_command_outbox(
                   status,retry_after_at,last_attempt_at,created_at
               )"""
    )
    conn.execute(
        """CREATE INDEX IF NOT EXISTS ix_package_command_outbox_fair_due
               ON package_command_outbox(
                   status,retry_after_at,last_attempt_at,created_at,idempotency_key
               )"""
    )
    conn.execute(
        """CREATE INDEX IF NOT EXISTS ix_package_command_outbox_review
               ON package_command_outbox(
                   status,local_recovery_dismissed,updated_at
               )"""
    )
    conn.execute(
        """UPDATE package_command_outbox
              SET review_status='OPERATOR_REVIEW'
            WHERE status='CONFLICT' AND review_status IS NULL"""
    )
    post_review_rows = conn.execute(
        """SELECT * FROM package_command_outbox
             WHERE status='CONFLICT'
               AND local_completion_committed=1
             ORDER BY updated_at,idempotency_key"""
    ).fetchall()
    for source_row in post_review_rows:
        source = dict(source_row)
        review_event_id, event, fingerprint = _post_review_event_payload(
            source,
            conflict_code=str(
                source.get("last_error_code")
                or "LOCAL_VALIDATION_CONFLICT"
            ),
            conflict_origin="RECOVERED_EXISTING_CONFLICT",
            required_at=str(
                source.get("updated_at")
                or source.get("created_at")
                or utc_now()
            ),
        )
        encoded = canonical_json(event)
        conn.execute(
            """INSERT OR IGNORE INTO package_post_review_outbox(
                   review_event_id,package_idempotency_key,event_fingerprint,
                   event_json,local_csv_committed,created_at,updated_at
               ) VALUES (?,?,?,?,0,?,?)""",
            (
                review_event_id,
                source["idempotency_key"],
                fingerprint,
                encoded,
                event["required_at"],
                event["required_at"],
            ),
        )
    cancellation_columns = {
        str(row["name"] if isinstance(row, sqlite3.Row) else row[1])
        for row in conn.execute("PRAGMA table_info(package_cancellation_outbox)").fetchall()
    }
    added_local_commit_column = False
    if "local_event_committed" not in cancellation_columns:
        conn.execute(
            """ALTER TABLE package_cancellation_outbox
                   ADD COLUMN local_event_committed INTEGER NOT NULL DEFAULT 0
                   CHECK(local_event_committed IN (0,1))"""
        )
        added_local_commit_column = True
    if "local_event_committed_at" not in cancellation_columns:
        conn.execute(
            "ALTER TABLE package_cancellation_outbox ADD COLUMN local_event_committed_at TEXT"
        )
    if "retry_after_at" not in cancellation_columns:
        conn.execute(
            "ALTER TABLE package_cancellation_outbox ADD COLUMN retry_after_at TEXT"
        )
    conn.execute(
        """CREATE INDEX IF NOT EXISTS ix_package_cancellation_outbox_due
               ON package_cancellation_outbox(status,retry_after_at,created_at)"""
    )
    if cancellation_table_existed and added_local_commit_column:
        # The pre-gate implementation enqueued only after the local CSV event
        # was flushed. Preserve that fact during the additive migration.
        conn.execute(
            """UPDATE package_cancellation_outbox
                  SET local_event_committed=1,
                      local_event_committed_at=COALESCE(local_event_committed_at,updated_at)"""
        )
    # Stamp the version only after every table/column/index is present.
    conn.execute(
        "INSERT OR REPLACE INTO package_outbox_schema_info(key,value) VALUES ('schema_version',?)",
        (OUTBOX_SCHEMA_VERSION,),
    )


class PackageOutbox(_PackageOutbox):
    """Keep the existing constructor and resolve facade hooks when called."""

    def __init__(self, db_path: str | Path):
        super().__init__(
            db_path,
            utc_now=lambda: utc_now(),
            utc_after=lambda seconds: _utc_after(seconds),
            utc_before=lambda seconds: _utc_before(seconds),
            sending_lease_seconds=lambda: SENDING_LEASE_SECONDS,
            initialize_schema=lambda conn: _initialize_outbox_schema(conn),
            post_review_event_payload=lambda *args, **kwargs: _post_review_event_payload(
                *args, **kwargs
            ),
            post_review_conflict_origin=lambda error: _post_review_conflict_origin(error),
        )


class PackageCancellationOutbox:
    """Durable cancellation intent, gated on an ACKed CREATE_PACKAGE receipt."""

    def __init__(self, db_path: str | Path):
        self.db_path = str(Path(db_path))
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.initialize()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=10000")
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def initialize(self) -> None:
        with self._connect() as conn:
            _initialize_outbox_schema(conn)
            conn.commit()

    def enqueue(self, intent: PackageCancellationIntent) -> dict[str, Any] | None:
        """Record local intent. Return None only for sets never queued centrally."""

        now = utc_now()
        fingerprint = intent.fingerprint()
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            package_row = conn.execute(
                "SELECT * FROM package_command_outbox WHERE set_id=?", (intent.set_id,)
            ).fetchone()
            if package_row is None:
                conn.commit()
                return None
            draft = self._load_package_draft(package_row)
            package_bundle_id = draft.package_bundle_id
            key = "label-package-cancel-" + stable_id(
                "cmd", intent.cancellation_event_id, package_bundle_id
            )
            existing = conn.execute(
                """SELECT * FROM package_cancellation_outbox
                     WHERE cancellation_event_id=? OR idempotency_key=?""",
                (intent.cancellation_event_id, key),
            ).fetchone()
            if existing:
                if (
                    existing["intent_fingerprint"] != fingerprint
                    or existing["package_bundle_id"] != package_bundle_id
                    or existing["package_idempotency_key"] != package_row["idempotency_key"]
                ):
                    conn.rollback()
                    raise PackageLogisticsError(
                        "package cancellation event was already queued with different data"
                    )
                conn.commit()
                return dict(existing)
            conn.execute(
                """
                INSERT INTO package_cancellation_outbox(
                    idempotency_key,cancellation_event_id,set_id,package_idempotency_key,
                    package_bundle_id,intent_fingerprint,intent_json,status,created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?,'DEFERRED',?,?)
                """,
                (
                    key,
                    intent.cancellation_event_id,
                    intent.set_id,
                    package_row["idempotency_key"],
                    package_bundle_id,
                    fingerprint,
                    json.dumps(intent.to_dict(), ensure_ascii=False, sort_keys=True),
                    now,
                    now,
                ),
            )
            self._promote_row_if_create_acked(conn, key)
            row = conn.execute(
                "SELECT * FROM package_cancellation_outbox WHERE idempotency_key=?", (key,)
            ).fetchone()
            conn.commit()
            return dict(row)

    def promote_deferred(self, *, limit: int = 100) -> int:
        promoted = 0
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """SELECT idempotency_key FROM package_cancellation_outbox
                     WHERE status='DEFERRED' ORDER BY created_at,idempotency_key LIMIT ?""",
                (max(0, int(limit)),),
            ).fetchall()
            for row in rows:
                before = conn.execute(
                    "SELECT status FROM package_cancellation_outbox WHERE idempotency_key=?",
                    (row["idempotency_key"],),
                ).fetchone()
                self._promote_row_if_create_acked(conn, row["idempotency_key"])
                after = conn.execute(
                    "SELECT status FROM package_cancellation_outbox WHERE idempotency_key=?",
                    (row["idempotency_key"],),
                ).fetchone()
                if before and after and before["status"] == "DEFERRED" and after["status"] == "PENDING":
                    promoted += 1
            conn.commit()
        return promoted

    def claim_next(self) -> dict[str, Any] | None:
        self.promote_deferred()
        now = utc_now()
        stale_before = _utc_before(SENDING_LEASE_SECONDS)
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """UPDATE package_cancellation_outbox
                      SET status='PENDING',retry_after_at=NULL,updated_at=?
                    WHERE status='SENDING' AND updated_at<=?""",
                (now, stale_before),
            )
            row = conn.execute(
                """SELECT cancellation.*,
                          package.idempotency_key AS linked_create_idempotency_key,
                          package.status AS create_status,
                          package.command_json AS create_command_json,
                          package.draft_json AS create_draft_json,
                          package.receipt_json AS create_receipt_json
                     FROM package_cancellation_outbox AS cancellation
                     JOIN package_command_outbox AS package
                       ON package.idempotency_key=cancellation.package_idempotency_key
                    WHERE cancellation.status='PENDING'
                      AND cancellation.local_event_committed=1
                      AND (cancellation.retry_after_at IS NULL
                           OR cancellation.retry_after_at<=?)
                    ORDER BY cancellation.created_at,cancellation.idempotency_key LIMIT 1""",
                (now,),
            ).fetchone()
            if row is None:
                conn.commit()
                return None
            cursor = conn.execute(
                """UPDATE package_cancellation_outbox
                       SET status='SENDING',attempt_count=attempt_count+1,
                           retry_after_at=NULL,updated_at=?
                     WHERE idempotency_key=? AND status='PENDING'""",
                (now, row["idempotency_key"]),
            )
            if cursor.rowcount != 1:
                conn.rollback()
                return None
            claimed = conn.execute(
                """SELECT cancellation.*,
                          package.idempotency_key AS linked_create_idempotency_key,
                          package.status AS create_status,
                          package.command_json AS create_command_json,
                          package.draft_json AS create_draft_json,
                          package.receipt_json AS create_receipt_json
                     FROM package_cancellation_outbox AS cancellation
                     JOIN package_command_outbox AS package
                       ON package.idempotency_key=cancellation.package_idempotency_key
                    WHERE cancellation.idempotency_key=?""",
                (row["idempotency_key"],),
            ).fetchone()
            conn.commit()
            return dict(claimed)

    def save_command(self, key: str, command: Mapping[str, Any]) -> None:
        encoded = json.dumps(dict(command), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT command_json,status FROM package_cancellation_outbox WHERE idempotency_key=?",
                (key,),
            ).fetchone()
            if row is None or row["status"] != "SENDING":
                conn.rollback()
                raise PackageLogisticsError("package cancellation command is not exclusively claimed")
            if row["command_json"]:
                existing = json.dumps(
                    json.loads(row["command_json"]),
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                if existing != encoded:
                    conn.rollback()
                    raise PackageLogisticsError("saved package cancellation command is immutable")
                conn.commit()
                return
            cursor = conn.execute(
                """UPDATE package_cancellation_outbox
                       SET command_json=?,updated_at=?
                     WHERE idempotency_key=? AND status='SENDING' AND command_json IS NULL""",
                (encoded, utc_now(), key),
            )
            if cursor.rowcount != 1:
                conn.rollback()
                raise PackageLogisticsError("package cancellation command lost its immutable save CAS")
            conn.commit()

    def mark_acked(self, key: str, receipt: Mapping[str, Any]) -> None:
        with self._connect() as conn:
            cursor = conn.execute(
                """UPDATE package_cancellation_outbox
                       SET status='ACKED',receipt_json=?,last_error_code=NULL,
                           last_error_message=NULL,retry_after_at=NULL,updated_at=?
                     WHERE idempotency_key=? AND status='SENDING'""",
                (json.dumps(dict(receipt), ensure_ascii=False, sort_keys=True), utc_now(), key),
            )
            if cursor.rowcount != 1:
                raise PackageLogisticsError("package cancellation ACK state changed concurrently")
            conn.commit()

    def mark_local_event_committed(self, cancellation_event_id: str) -> None:
        event_id = str(cancellation_event_id or "").strip()
        if not event_id:
            raise PackageLogisticsError("package cancellation local event identity is required")
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT local_event_committed FROM package_cancellation_outbox
                     WHERE cancellation_event_id=?""",
                (event_id,),
            ).fetchone()
            if row is None:
                conn.rollback()
                raise PackageLogisticsError("package cancellation intent is missing")
            if int(row["local_event_committed"] or 0) == 1:
                conn.commit()
                return
            cursor = conn.execute(
                """UPDATE package_cancellation_outbox
                       SET local_event_committed=1,local_event_committed_at=?,updated_at=?
                     WHERE cancellation_event_id=? AND local_event_committed=0""",
                (utc_now(), utc_now(), event_id),
            )
            if cursor.rowcount != 1:
                conn.rollback()
                raise PackageLogisticsError(
                    "package cancellation local event commit changed concurrently"
                )
            conn.commit()

    def mark_retry(self, key: str, error: Exception) -> None:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT attempt_count FROM package_cancellation_outbox
                     WHERE idempotency_key=? AND status='SENDING'""",
                (key,),
            ).fetchone()
            if row is None:
                conn.rollback()
                raise PackageLogisticsError(
                    "package cancellation retry state changed concurrently"
                )
            attempt_count = max(1, int(row["attempt_count"] or 1))
            local_backoff = min(1800.0, 30.0 * (2 ** min(attempt_count - 1, 6)))
            server_backoff = _bounded_retry_after_seconds(
                getattr(error, "retry_after_seconds", None)
            ) or 0.0
            retry_after_at = _utc_after(max(local_backoff, server_backoff))
            code = str(getattr(error, "code", error.__class__.__name__))
            message = str(getattr(error, "message", str(error)))
            cursor = conn.execute(
                """UPDATE package_cancellation_outbox
                       SET status='PENDING',last_error_code=?,last_error_message=?,
                           retry_after_at=?,updated_at=?
                     WHERE idempotency_key=? AND status='SENDING'""",
                (code, message, retry_after_at, utc_now(), key),
            )
            if cursor.rowcount != 1:
                conn.rollback()
                raise PackageLogisticsError(
                    "package cancellation retry state changed concurrently"
                )
            conn.commit()

    def mark_conflict(self, key: str, error: Exception) -> None:
        code = str(getattr(error, "code", "LOCAL_VALIDATION_CONFLICT"))
        message = str(getattr(error, "message", str(error)))
        with self._connect() as conn:
            conn.execute(
                """UPDATE package_cancellation_outbox
                       SET status='CONFLICT',last_error_code=?,last_error_message=?,
                           retry_after_at=NULL,updated_at=?
                     WHERE idempotency_key=? AND status IN ('DEFERRED','SENDING')""",
                (code, message, utc_now(), key),
            )
            conn.commit()

    def get_by_event_id(self, cancellation_event_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """SELECT cancellation.*,
                          package.idempotency_key AS linked_create_idempotency_key,
                          package.status AS create_status,
                          package.command_json AS create_command_json,
                          package.draft_json AS create_draft_json,
                          package.receipt_json AS create_receipt_json
                     FROM package_cancellation_outbox AS cancellation
                     LEFT JOIN package_command_outbox AS package
                       ON package.idempotency_key=cancellation.package_idempotency_key
                    WHERE cancellation.cancellation_event_id=?""",
                (str(cancellation_event_id),),
            ).fetchone()
            return dict(row) if row else None

    def get_by_set_id(self, set_id: str) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT * FROM package_cancellation_outbox
                     WHERE set_id=? ORDER BY created_at,idempotency_key""",
                (str(set_id),),
            ).fetchall()
            return [dict(row) for row in rows]

    def list_conflicts(self, *, limit: int = 20) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT cancellation_event_id,set_id,package_bundle_id,
                          last_error_code,last_error_message AS last_error,
                          updated_at,status
                     FROM package_cancellation_outbox
                    WHERE status='CONFLICT'
                    ORDER BY updated_at DESC,idempotency_key DESC
                    LIMIT ?""",
                (max(0, int(limit)),),
            ).fetchall()
            return [dict(row) for row in rows]

    def uncommitted_local_events(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT * FROM package_cancellation_outbox
                     WHERE local_event_committed=0
                     ORDER BY created_at,idempotency_key"""
            ).fetchall()
            return [dict(row) for row in rows]

    def counts(self) -> dict[str, int]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT status,COUNT(*) AS count FROM package_cancellation_outbox GROUP BY status"
            ).fetchall()
            result = {
                status: 0
                for status in ("DEFERRED", "PENDING", "SENDING", "ACKED", "CONFLICT")
            }
            result.update({row["status"]: int(row["count"]) for row in rows})
            return result

    @staticmethod
    def _load_package_draft(package_row: Mapping[str, Any]) -> PackageCommandDraft:
        try:
            draft_data = json.loads(package_row["draft_json"])
            return PackageCommandDraft.from_dict(draft_data)
        except (
            KeyError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
            PackageLogisticsError,
        ) as exc:
            raise PackageLogisticsError("saved CREATE_PACKAGE draft is invalid") from exc

    @staticmethod
    def _create_ack_metadata(
        package_row: Mapping[str, Any], package_bundle_id: str
    ) -> dict[str, Any] | None:
        if str(package_row["status"] or "") != "ACKED" or not package_row["receipt_json"]:
            return None
        try:
            receipt = json.loads(package_row["receipt_json"])
            command = json.loads(package_row["command_json"] or "{}")
        except (TypeError, json.JSONDecodeError) as exc:
            raise PackageLogisticsError("saved CREATE_PACKAGE ACK evidence is invalid") from exc
        if not isinstance(receipt, Mapping) or not isinstance(command, Mapping):
            raise PackageLogisticsError("saved CREATE_PACKAGE ACK evidence is invalid")
        data = receipt.get("data") if isinstance(receipt.get("data"), Mapping) else receipt
        if not isinstance(data, Mapping):
            raise PackageLogisticsError("saved CREATE_PACKAGE receipt data is invalid")
        if str(data.get("package_bundle_id") or "") != package_bundle_id:
            raise PackageLogisticsError("saved CREATE_PACKAGE receipt package bundle does not match")
        versions = receipt.get("entity_versions")
        if not isinstance(versions, Mapping):
            versions = data.get("entity_versions")
        try:
            version = int((versions or {}).get(f"bundle:{package_bundle_id}") or 0)
            authority_epoch = int(command.get("authority_epoch") or 0)
            plane_epoch = int(command.get("plane_epoch") or 0)
        except (TypeError, ValueError) as exc:
            raise PackageLogisticsError(
                "saved CREATE_PACKAGE command version/epoch context is invalid"
            ) from exc
        if version < 1:
            raise PackageLogisticsError("saved CREATE_PACKAGE receipt package version is invalid")
        scope = str(command.get("authority_scope_id") or "").strip()
        plane = str(command.get("ledger_plane") or "").strip().upper()
        if not scope or not plane or authority_epoch < 0 or plane_epoch < 1:
            raise PackageLogisticsError("saved CREATE_PACKAGE command authority context is invalid")
        return {
            "authority_scope_id": scope,
            "authority_epoch": authority_epoch,
            "ledger_plane": plane,
            "plane_epoch": plane_epoch,
            "expected_bundle_version": version,
        }

    def _promote_row_if_create_acked(self, conn: sqlite3.Connection, key: str) -> None:
        row = conn.execute(
            "SELECT * FROM package_cancellation_outbox WHERE idempotency_key=?", (key,)
        ).fetchone()
        if row is None or row["status"] != "DEFERRED":
            return
        package_row = conn.execute(
            "SELECT * FROM package_command_outbox WHERE idempotency_key=?",
            (row["package_idempotency_key"],),
        ).fetchone()
        if package_row is None:
            conn.execute(
                """UPDATE package_cancellation_outbox
                       SET status='CONFLICT',last_error_code='MISSING_CREATE_PACKAGE',
                           last_error_message='saved CREATE_PACKAGE outbox row is missing',updated_at=?
                     WHERE idempotency_key=? AND status='DEFERRED'""",
                (utc_now(), key),
            )
            return
        if str(package_row["status"] or "") == "CONFLICT":
            create_code = str(package_row["last_error_code"] or "CREATE_PACKAGE_CONFLICT")
            create_message = str(
                package_row["last_error_message"]
                or "CREATE_PACKAGE reached a terminal conflict before cancellation"
            )
            conn.execute(
                """UPDATE package_cancellation_outbox
                       SET status='CONFLICT',last_error_code='CREATE_PACKAGE_CONFLICT',
                           last_error_message=?,updated_at=?
                     WHERE idempotency_key=? AND status='DEFERRED'""",
                (f"{create_code}: {create_message}", utc_now(), key),
            )
            return
        try:
            metadata = self._create_ack_metadata(package_row, row["package_bundle_id"])
        except PackageLogisticsError as exc:
            conn.execute(
                """UPDATE package_cancellation_outbox
                       SET status='CONFLICT',last_error_code='INVALID_CREATE_PACKAGE_ACK',
                           last_error_message=?,updated_at=?
                     WHERE idempotency_key=? AND status='DEFERRED'""",
                (str(exc), utc_now(), key),
            )
            return
        if metadata is None:
            return
        conn.execute(
            """UPDATE package_cancellation_outbox
                   SET status='PENDING',authority_scope_id=?,authority_epoch=?,ledger_plane=?,
                       plane_epoch=?,expected_bundle_version=?,last_error_code=NULL,
                       last_error_message=NULL,updated_at=?
                 WHERE idempotency_key=? AND status='DEFERRED'""",
            (
                metadata["authority_scope_id"],
                metadata["authority_epoch"],
                metadata["ledger_plane"],
                metadata["plane_epoch"],
                metadata["expected_bundle_version"],
                utc_now(),
                key,
            ),
        )


@dataclass(frozen=True)
class PackageClientConfig:
    base_url: str
    token: str = field(repr=False)
    authority_scope_id: str
    source_host_id: str
    device_id: str
    timeout_seconds: float = 8.0
    authority_epoch: int = 0
    authority_plane: str = ""
    ledger_plane: str = ""
    plane_epoch: int = 0
    authoritative_required: bool = False
    tls_ca_bundle_path: str = field(default="", repr=False)

    def validate(self) -> None:
        parsed = urlsplit(self.base_url)
        if (
            parsed.scheme != "https"
            or not parsed.netloc
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise PackageLogisticsError("package logistics base URL must be credential-free HTTPS")
        hostname = str(parsed.hostname or "").rstrip(".").lower()
        is_loopback = hostname == "localhost"
        try:
            is_loopback = is_loopback or ipaddress.ip_address(hostname).is_loopback
        except ValueError:
            pass
        if self.authoritative_required and is_loopback:
            raise PackageLogisticsError("machine package logistics URL must not use loopback")
        if not all((self.token, self.source_host_id, self.device_id)):
            raise PackageLogisticsError("package logistics machine identity/configuration is incomplete")
        selected_ca = str(self.tls_ca_bundle_path or "").strip()
        if selected_ca and not Path(selected_ca).is_file():
            raise PackageLogisticsError("package logistics TLS CA bundle is unavailable")
        selected_ledger_plane = str(
            self.ledger_plane or self.authority_plane or ""
        ).upper()
        if self.authoritative_required and (
            not self.authority_scope_id
            or self.authority_epoch < 1
            or str(self.authority_plane or "").upper() != "AUTHORITATIVE"
            or selected_ledger_plane not in {"AUTHORITATIVE", "SHADOW_CANDIDATE"}
            or self.plane_epoch < 1
        ):
            raise PackageLogisticsError("authoritative package logistics profile is incomplete")


Transport = Callable[[str, str, Mapping[str, str], bytes | None, float], Mapping[str, Any]]


def _read_http_body(response: Any) -> str:
    try:
        raw = response.read()
    except (IncompleteRead, HTTPException, OSError) as exc:
        raise PackageTransportError(
            f"package API response body was incomplete: {exc.__class__.__name__}"
        ) from exc
    if not isinstance(raw, (bytes, bytearray)):
        raise PackageTransportError("package API response body must be bytes")
    try:
        return bytes(raw).decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PackageTransportError(
            "package API response body was not valid UTF-8"
        ) from exc


def _default_transport(
    method: str,
    url: str,
    headers: Mapping[str, str],
    body: bytes | None,
    timeout: float,
    *,
    tls_ca_bundle_path: str = "",
):
    request = Request(url, data=body, headers=dict(headers), method=method)
    selected_ca = str(tls_ca_bundle_path or "").strip()
    try:
        context = (
            ssl.create_default_context(cafile=selected_ca) if selected_ca else None
        )
        open_kwargs: dict[str, Any] = {"timeout": timeout}
        if context is not None:
            open_kwargs["context"] = context
        with urlopen(request, **open_kwargs) as response:
            raw = _read_http_body(response)
    except HTTPError as exc:
        raw = _read_http_body(exc)
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            value = {}
        error = value.get("error") if isinstance(value, Mapping) else {}
        if not isinstance(error, Mapping):
            error = {}
        retry_after_candidates = []
        if "retry_after_seconds" in error:
            retry_after_candidates.append(error.get("retry_after_seconds"))
        if isinstance(value, Mapping) and "retry_after_seconds" in value:
            retry_after_candidates.append(value.get("retry_after_seconds"))
        if exc.headers:
            retry_after_candidates.append(exc.headers.get("Retry-After"))
        retry_after = None
        for candidate in retry_after_candidates:
            retry_after = _parse_retry_after_seconds(candidate)
            if retry_after is not None:
                break
        raise PackageApiError(
            exc.code,
            str(error.get("code") or f"HTTP_{exc.code}"),
            str(error.get("message") or "package API rejected the request"),
            retryable=_first_optional_bool(
                error.get("retryable") if "retryable" in error else None,
                value.get("retryable") if isinstance(value, Mapping) else None,
            ),
            committed=_first_optional_bool(
                error.get("committed") if "committed" in error else None,
                value.get("committed") if isinstance(value, Mapping) else None,
            ),
            retry_after_seconds=retry_after,
        ) from exc
    except (URLError, TimeoutError, OSError, HTTPException) as exc:
        raise PackageTransportError(f"package API transport failed: {exc.__class__.__name__}") from exc
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise PackageTransportError("package API response was not JSON") from exc
    if not isinstance(value, Mapping):
        raise PackageTransportError("package API response must be an object")
    return value


class PackageLogisticsClient:
    def __init__(self, config: PackageClientConfig, *, transport: Transport | None = None):
        config.validate()
        self.config = config
        self._transport = transport or (
            lambda method, url, headers, body, timeout: _default_transport(
                method,
                url,
                headers,
                body,
                timeout,
                tls_ca_bundle_path=config.tls_ca_bundle_path,
            )
        )

    def _assert_authority(
        self,
        scope: str,
        *,
        authority_epoch: Any = None,
        ledger_plane: str = "",
        plane_epoch: Any = None,
    ) -> None:
        scope_id = str(scope or "").strip()
        configured = str(self.config.authority_scope_id or "").strip()
        if configured and scope_id != configured:
            raise PackageLogisticsError(
                "AUTHORITY_PROFILE_MISMATCH: scanned scope is outside the installed profile"
            )
        if self.config.authority_epoch and authority_epoch is not None and int(authority_epoch) != self.config.authority_epoch:
            raise PackageLogisticsError("AUTHORITY_PROFILE_MISMATCH: authority epoch differs")
        configured_ledger_plane = str(
            self.config.ledger_plane or self.config.authority_plane or ""
        ).upper()
        if configured_ledger_plane and ledger_plane and str(ledger_plane).upper() != configured_ledger_plane:
            raise PackageLogisticsError("AUTHORITY_PROFILE_MISMATCH: ledger plane differs")
        if self.config.plane_epoch and plane_epoch is not None and int(plane_epoch) != self.config.plane_epoch:
            raise PackageLogisticsError("AUTHORITY_PROFILE_MISMATCH: plane epoch differs")

    def get_bundle(self, bundle_id: str, *, authority_scope_id: str = "") -> dict[str, Any]:
        source_id = str(bundle_id or "").strip()
        if not source_id:
            raise PackageLogisticsError("source bundle ID is required")
        scope = str(authority_scope_id or self.config.authority_scope_id or "").strip()
        if not scope:
            raise PackageLogisticsError("authority scope is required to get a sealed transfer")
        self._assert_authority(scope)
        path = (
            "/logistics/api/v1/bundles/"
            + quote(scope, safe="")
            + "/"
            + quote(source_id, safe="")
        )
        return self._data(self._request("GET", path))

    def get_capabilities(self) -> dict[str, Any]:
        """Return the server-advertised logistics contract surface.

        Product replacement is deliberately capability-gated because older
        servers cannot invalidate a printed transfer seal safely.
        """

        return self._data(self._request("GET", "/logistics/api/v1/capabilities"))

    @writer_sink("package_operation_lease")
    def issue_operation_lease(
        self,
        *,
        authority_scope_id: str,
        operation: str,
        scan_payload: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        """Prefetch one signed terminal-exclusive operation lease online."""

        scope = str(
            authority_scope_id or self.config.authority_scope_id or ""
        ).strip()
        selected_operation = str(operation or "").strip().upper()
        physical_scan = str(scan_payload or "").strip()
        key = str(idempotency_key or "").strip()
        if (
            not scope
            or selected_operation != "CREATE_PACKAGE"
            or not physical_scan
            or not key
        ):
            raise PackageLogisticsError(
                "operation lease request identity is incomplete"
            )
        self._assert_authority(scope)
        body = json.dumps(
            {
                "authority_scope_id": scope,
                "operation": selected_operation,
                "scan_payload": physical_scan,
            },
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return self._data(
            self._request(
                "POST",
                "/logistics/api/v1/operation-leases/issue",
                body=body,
                key=key,
            )
        )

    def resolve_good_source(
        self, *, authority_scope_id: str, barcode: str
    ) -> dict[str, Any]:
        scope = str(authority_scope_id or self.config.authority_scope_id or "").strip()
        normalized = _normalize_barcode(barcode)
        if not scope or not normalized:
            raise PackageLogisticsError(
                "authority scope and barcode are required to resolve a replacement good"
            )
        self._assert_authority(scope)
        query = urlencode({"authority_scope_id": scope, "barcode": normalized})
        return self._data(
            self._request(
                "GET",
                f"/logistics/api/v1/replacements/good-source/resolve?{query}",
            )
        )

    @writer_sink("package_transfer_reseal")
    def replace_and_reseal_transfer(
        self, command: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Atomically replace members in an AVAILABLE transfer and reseal it."""

        key = str(command.get("idempotency_key") or "").strip()
        scope = str(command.get("authority_scope_id") or "").strip()
        payload = command.get("payload")
        target = (
            str(payload.get("target_bundle_id") or "").strip()
            if isinstance(payload, Mapping)
            else ""
        )
        if not key or not scope or not target:
            raise PackageLogisticsError(
                "sealed transfer replacement command identity is incomplete"
            )
        self._assert_authority(
            scope,
            authority_epoch=command.get("authority_epoch"),
            ledger_plane=str(command.get("ledger_plane") or ""),
            plane_epoch=command.get("plane_epoch"),
        )
        body = json.dumps(
            dict(command), ensure_ascii=False, allow_nan=False, sort_keys=True
        ).encode("utf-8")
        path = (
            "/logistics/api/v1/transfers/"
            + quote(target, safe="")
            + "/members/replace-and-reseal"
        )
        try:
            return self._data(self._request("POST", path, body=body, key=key))
        except PackageApiError as original:
            should_recover_receipt = (
                original.committed is True
                or original.committed is None
                or original.status_code >= 500
            )
            if not should_recover_receipt:
                raise
            try:
                recovered = self.get_receipt_if_exists(
                    key, authority_scope_id=scope
                )
            except PackageLogisticsError:
                raise original
            if recovered is not None:
                return recovered
            raise original
        except PackageTransportError as original:
            try:
                recovered = self.get_receipt_if_exists(
                    key, authority_scope_id=scope
                )
            except PackageLogisticsError:
                raise original
            if recovered is not None:
                return recovered
            raise original

    def resolve_transfer_bundle(
        self,
        *,
        external_label: str,
        input_tag_id: str,
        input_tag_label_id: str = "",
        input_tag_hash_prefix: str = "",
        item_id: str,
        authority_scope_id: str,
        exact_rescan_barcodes: Iterable[str] = (),
        source_bundle_hint: str = "",
    ) -> dict[str, Any]:
        exact = canonical_barcodes(exact_rescan_barcodes)
        input_tag = str(input_tag_id or "").strip()
        bundle_hint = str(source_bundle_hint or "").strip()
        if not input_tag and not bundle_hint:
            raise PackageLogisticsError(
                "PACKAGE_SOURCE resolution requires structured PHS BND or ITG lineage"
            )
        params = {
            "item_id": str(item_id or "").strip(),
            "authority_scope_id": str(authority_scope_id or "").strip(),
            "bundle_role": "PACKAGE_SOURCE",
        }
        self._assert_authority(params["authority_scope_id"])
        if input_tag:
            params["input_tag_id"] = input_tag
            input_tag_label = str(input_tag_label_id or "").strip()
            input_tag_hash = str(input_tag_hash_prefix or "").strip().lower()
            if bool(input_tag_label) != bool(input_tag_hash):
                raise PackageLogisticsError(
                    "PACKAGE_SOURCE PHS2 LBL and HSH identity must be supplied together"
                )
            if input_tag_label:
                params["input_tag_label_id"] = input_tag_label
                params["input_tag_hash_prefix"] = input_tag_hash
        if bundle_hint:
            params["bundle_id"] = bundle_hint
        if exact:
            params["member_count"] = len(exact)
            params["barcode_membership_hash"] = barcode_membership_hash(exact)
        # external_label is intentionally not a resolver key here.  A printed
        # label or compatibility WID can match multiple partial/remainder
        # lineages, while BND/ITG are structured server identities.
        query = urlencode(params)
        return self._data(self._request("GET", f"/logistics/api/v1/bundles/resolve?{query}"))

    def resolve_package_source_projection(
        self, draft: PackageCommandDraft
    ) -> dict[str, Any]:
        """Resolve and validate the live TRANSFER behind a scanned PHS2.

        This read-only preflight is used by the packaging UI before a sealed
        member exchange.  CREATE_PACKAGE still performs its own fresh resolve
        and versioned read so this convenience method cannot weaken the final
        command's compare-and-swap boundary.
        """

        scope = str(
            draft.source_authority_scope_id
            or self.config.authority_scope_id
            or ""
        ).strip()
        if not scope:
            raise PackageLogisticsError(
                "packaging authority scope is required"
            )
        if draft.source_bundle_id:
            projection = self.get_bundle(
                draft.source_bundle_id,
                authority_scope_id=scope,
            )
            self._validate_projection(
                projection,
                draft,
                expected_scope=scope,
            )
            return dict(projection)

        resolved = self.resolve_transfer_bundle(
            external_label=draft.source_external_label,
            input_tag_id=draft.source_input_tag_id,
            input_tag_label_id=draft.source_input_tag_label_id,
            input_tag_hash_prefix=draft.source_input_tag_hash_prefix,
            item_id=draft.item_code,
            authority_scope_id=scope,
            exact_rescan_barcodes=draft.exact_rescan_barcodes,
            source_bundle_hint=draft.source_bundle_hint,
        )
        if (
            resolved.get("source_resolution_basis")
            == "PHS_WORK_GROUP_EXACT_MEMBERSHIP"
        ):
            self._validate_work_group_source(
                resolved,
                draft,
                expected_scope=scope,
            )
            projection = resolved.get("bundle")
            if not isinstance(projection, Mapping):
                raise PackageLogisticsError(
                    "package work-group bundle projection is missing"
                )
            return dict(projection)
        projection = self._resolver_bundle(resolved)
        self._validate_projection(
            projection,
            draft,
            expected_scope=scope,
            require_package_source_role=True,
        )
        return projection

    def resolve_package_source_evidence(
        self, draft: PackageCommandDraft
    ) -> dict[str, Any]:
        """Return the exact package source plus PHS physical-label evidence.

        ``resolve_package_source_projection`` intentionally returns only the
        canonical bundle for older callers.  The packaging scan path also
        needs the immutable input-tag QR and the active physical-label
        overlay, so keep the complete server response after applying the same
        strict bundle validation.
        """

        scope = str(
            draft.source_authority_scope_id
            or self.config.authority_scope_id
            or ""
        ).strip()
        if not scope:
            raise PackageLogisticsError(
                "packaging authority scope is required"
            )
        if draft.source_bundle_id:
            projection = self.get_bundle(
                draft.source_bundle_id,
                authority_scope_id=scope,
            )
            self._validate_projection(
                projection,
                draft,
                expected_scope=scope,
            )
            return {"bundle": dict(projection)}

        resolved = self.resolve_transfer_bundle(
            external_label=draft.source_external_label,
            input_tag_id=draft.source_input_tag_id,
            input_tag_label_id=draft.source_input_tag_label_id,
            input_tag_hash_prefix=draft.source_input_tag_hash_prefix,
            item_id=draft.item_code,
            authority_scope_id=scope,
            exact_rescan_barcodes=draft.exact_rescan_barcodes,
            source_bundle_hint=draft.source_bundle_hint,
        )
        if (
            resolved.get("source_resolution_basis")
            == "PHS_WORK_GROUP_EXACT_MEMBERSHIP"
        ):
            work_group_evidence = self._validate_work_group_source(
                resolved,
                draft,
                expected_scope=scope,
            )
            evidence = dict(resolved)
            evidence["bundle"] = dict(resolved["bundle"])
            evidence["work_group_evidence"] = work_group_evidence
            return evidence
        projection = self._resolver_bundle(resolved)
        self._validate_projection(
            projection,
            draft,
            expected_scope=scope,
            require_package_source_role=True,
        )
        evidence = dict(resolved)
        evidence["bundle"] = projection
        return evidence

    def list_phs_work_instruction_candidates(
        self,
        *,
        authority_scope_id: str,
        business_date: str,
        item_id: str,
        target_qty_pcs: int,
        limit: int = 20,
    ) -> dict[str, Any]:
        scope = str(
            authority_scope_id
            or self.config.authority_scope_id
            or ""
        ).strip()
        if not scope:
            raise PackageLogisticsError(
                "authority scope is required for PHS work instructions"
            )
        self._assert_authority(scope)
        query = urlencode(
            {
                "authority_scope_id": scope,
                "business_date": str(business_date or "").strip(),
                "item_id": str(item_id or "").strip(),
                "target_qty_pcs": int(target_qty_pcs),
                "limit": int(limit),
            }
        )
        return self._data(
            self._request(
                "GET",
                f"/logistics/api/v1/phs-work-instructions/candidates?{query}",
            )
        )

    def resolve_active_phs_label(
        self,
        input_tag_id: str,
        *,
        authority_scope_id: str,
    ) -> dict[str, Any]:
        scope = str(
            authority_scope_id
            or self.config.authority_scope_id
            or ""
        ).strip()
        self._assert_authority(scope)
        query = urlencode({"authority_scope_id": scope})
        return self._data(
            self._request(
                "GET",
                "/logistics/api/v1/input-tags/"
                f"{quote(str(input_tag_id or '').strip(), safe='')}"
                f"/phs-label?{query}",
            )
        )

    def resolve_phs_reconciliation_actions(
        self,
        *,
        authority_scope_id: str,
        scan_payload: str,
        process_context: str = "packaging",
        limit: int = 20,
    ) -> dict[str, Any]:
        scope = str(
            authority_scope_id
            or self.config.authority_scope_id
            or ""
        ).strip()
        self._assert_authority(scope)
        scan = str(scan_payload or "").strip()
        context = str(process_context or "").strip().lower()
        bounded_limit = int(limit)
        if not scan:
            raise PackageLogisticsError(
                "PHS reconciliation scan_payload is required"
            )
        if context != "packaging":
            raise PackageLogisticsError(
                "PHS reconciliation process_context must be packaging"
            )
        if bounded_limit < 1 or bounded_limit > 20:
            raise PackageLogisticsError(
                "PHS reconciliation limit must be between 1 and 20"
            )
        query = urlencode(
            {
                "authority_scope_id": scope,
                "scan_payload": scan,
                "process_context": context,
                "limit": bounded_limit,
            }
        )
        return self._data(
            self._request(
                "GET",
                (
                    "/logistics/api/v1/phs-work-reconciliations/"
                    f"actions/resolve?{query}"
                ),
            )
        )

    @writer_sink("phs_label_adopt")
    def adopt_phs_label(
        self,
        *,
        authority_scope_id: str,
        qr_payload: str,
        business_date: str = "",
        expected_session_version: int | None = None,
    ) -> dict[str, Any]:
        scope = str(
            authority_scope_id
            or self.config.authority_scope_id
            or ""
        ).strip()
        self._assert_authority(scope)
        payload: dict[str, Any] = {
            "authority_scope_id": scope,
            "qr_payload": str(qr_payload or "").strip(),
        }
        if business_date:
            payload["business_date"] = str(business_date).strip()
        if expected_session_version is not None:
            payload["expected_session_version"] = int(
                expected_session_version
            )
        body = json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
        ).encode("utf-8")
        return self._data(
            self._request(
                "POST",
                "/logistics/api/v1/phs-labels/adopt",
                body=body,
            )
        )

    @writer_sink("phs_exchange_prepare")
    def prepare_phs_label_exchange(
        self,
        *,
        authority_scope_id: str,
        exchange_kind: str,
        sources: list[dict[str, Any]],
        targets: list[dict[str, Any]],
        idempotency_key: str,
    ) -> dict[str, Any]:
        scope = str(
            authority_scope_id
            or self.config.authority_scope_id
            or ""
        ).strip()
        self._assert_authority(scope)
        payload = {
            "authority_scope_id": scope,
            "exchange_kind": str(exchange_kind or "").strip().upper(),
            "sources": list(sources),
            "targets": list(targets),
        }
        body = json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
        ).encode("utf-8")
        return self._data(
            self._request(
                "POST",
                "/logistics/api/v1/phs-label-exchanges/prepare",
                body=body,
                key=str(idempotency_key or "").strip(),
            )
        )

    @writer_sink("phs_reconciliation_prepare")
    def prepare_phs_reconciliation_label_exchange(
        self,
        reconciliation_id: str,
        *,
        authority_scope_id: str,
        action_ids: list[str],
        expected_reconciliation_version: int,
        idempotency_key: str,
    ) -> dict[str, Any]:
        scope = str(
            authority_scope_id
            or self.config.authority_scope_id
            or ""
        ).strip()
        self._assert_authority(scope)
        reconciliation = str(reconciliation_id or "").strip()
        selected_action_ids = [
            str(value or "").strip()
            for value in list(action_ids or [])
        ]
        expected_version = int(expected_reconciliation_version)
        key = str(idempotency_key or "").strip()
        if not reconciliation:
            raise PackageLogisticsError(
                "PHS reconciliation_id is required"
            )
        if (
            not selected_action_ids
            or any(not value for value in selected_action_ids)
            or len(selected_action_ids)
            != len(set(selected_action_ids))
            or len(selected_action_ids) > 20
        ):
            raise PackageLogisticsError(
                "PHS reconciliation action_ids must be bounded, nonempty, and unique"
            )
        if expected_version < 1:
            raise PackageLogisticsError(
                "PHS expected reconciliation version must be positive"
            )
        if not key:
            raise PackageLogisticsError(
                "PHS reconciliation idempotency key is required"
            )
        payload = {
            "authority_scope_id": scope,
            "action_ids": selected_action_ids,
            "expected_reconciliation_version": expected_version,
        }
        body = json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
        ).encode("utf-8")
        response = self._data(
            self._request(
                "POST",
                (
                    "/logistics/api/v1/phs-work-reconciliations/"
                    f"{quote(reconciliation, safe='')}"
                    "/label-exchange/prepare"
                ),
                body=body,
                key=key,
            )
        )
        return response

    def get_phs_label_exchange(
        self,
        exchange_id: str,
        *,
        authority_scope_id: str,
    ) -> dict[str, Any]:
        scope = str(
            authority_scope_id
            or self.config.authority_scope_id
            or ""
        ).strip()
        self._assert_authority(scope)
        query = urlencode({"authority_scope_id": scope})
        return self._data(
            self._request(
                "GET",
                "/logistics/api/v1/phs-label-exchanges/"
                f"{quote(str(exchange_id or '').strip(), safe='')}?{query}",
            )
        )

    @writer_sink("phs_print_request")
    def request_phs_label_print(
        self,
        exchange_id: str,
        *,
        authority_scope_id: str,
        label_id: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        scope = str(
            authority_scope_id
            or self.config.authority_scope_id
            or ""
        ).strip()
        self._assert_authority(scope)
        payload = {
            "authority_scope_id": scope,
            "label_id": str(label_id or "").strip(),
        }
        body = json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
        ).encode("utf-8")
        return self._data(
            self._request(
                "POST",
                "/logistics/api/v1/phs-label-exchanges/"
                f"{quote(str(exchange_id or '').strip(), safe='')}/prints",
                body=body,
                key=str(idempotency_key or "").strip(),
            )
        )

    @writer_sink("phs_print_complete")
    def complete_phs_label_print(
        self,
        print_attempt_id: str,
        *,
        authority_scope_id: str,
        succeeded: bool,
        rendered_artifact_hash: str = "",
        proof: Mapping[str, Any] | None = None,
        error_code: str = "",
        error_message: str = "",
    ) -> dict[str, Any]:
        scope = str(
            authority_scope_id
            or self.config.authority_scope_id
            or ""
        ).strip()
        self._assert_authority(scope)
        payload: dict[str, Any] = {
            "authority_scope_id": scope,
            "succeeded": bool(succeeded),
        }
        if succeeded:
            payload["rendered_artifact_hash"] = str(
                rendered_artifact_hash or ""
            ).strip().lower()
            payload["proof"] = dict(proof or {})
        else:
            payload["error_code"] = str(error_code or "").strip()
            payload["error_message"] = str(error_message or "").strip()
            if proof is not None:
                payload["proof"] = dict(proof)
        body = json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
        ).encode("utf-8")
        return self._data(
            self._request(
                "POST",
                "/logistics/api/v1/phs-label-print-attempts/"
                f"{quote(str(print_attempt_id or '').strip(), safe='')}"
                "/complete",
                body=body,
            )
        )

    @writer_sink("phs_exchange_activate")
    def activate_phs_label_exchange(
        self,
        exchange_id: str,
        *,
        authority_scope_id: str,
        expected_exchange_version: int,
    ) -> dict[str, Any]:
        scope = str(
            authority_scope_id
            or self.config.authority_scope_id
            or ""
        ).strip()
        self._assert_authority(scope)
        payload = {
            "authority_scope_id": scope,
            "expected_exchange_version": int(expected_exchange_version),
        }
        body = json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
        ).encode("utf-8")
        return self._data(
            self._request(
                "POST",
                "/logistics/api/v1/phs-label-exchanges/"
                f"{quote(str(exchange_id or '').strip(), safe='')}/activate",
                body=body,
            )
        )

    def build_create_package_command(
        self, draft: PackageCommandDraft, *, idempotency_key: str
    ) -> tuple[str, dict[str, Any]]:
        source_id = draft.source_bundle_id
        scope = str(draft.source_authority_scope_id or self.config.authority_scope_id or "").strip()
        if not scope:
            raise PackageLogisticsError("packaging authority scope is required")
        if (
            draft.source_authority_scope_id
            and self.config.authority_scope_id
            and draft.source_authority_scope_id != self.config.authority_scope_id
        ):
            raise PackageLogisticsError("sealed transfer QR scope is outside the configured allowlist")
        if draft.source_resolution_basis:
            resolved = self.resolve_transfer_bundle(
                external_label=draft.source_external_label,
                input_tag_id=draft.source_input_tag_id,
                input_tag_label_id=draft.source_input_tag_label_id,
                input_tag_hash_prefix=draft.source_input_tag_hash_prefix,
                item_id=draft.item_code,
                authority_scope_id=scope,
                exact_rescan_barcodes=draft.exact_rescan_barcodes,
                source_bundle_hint="",
            )
            evidence = self._validate_work_group_source(
                resolved,
                draft,
                expected_scope=scope,
            )
            server_barcodes = evidence["barcodes"]
            if draft.sample_barcodes and not set(
                draft.sample_barcodes
            ).issubset(set(server_barcodes)):
                raise PackageLogisticsError(
                    "QA sample barcode is outside the package work-group membership"
                )
            payload: dict[str, Any] = {
                "source_resolution_basis": (
                    "PHS_WORK_GROUP_EXACT_MEMBERSHIP"
                ),
                "phs_work_group": evidence["phs_work_group"],
                "source_transfers": evidence["source_transfers"],
                "remainder_cover_groups": evidence[
                    "remainder_cover_groups"
                ],
                "topology_hash": evidence["topology_hash"],
                "package_bundle_id": evidence["package_bundle_id"],
                "external_label": evidence["package_external_label"],
                "item_id": evidence["item_id"],
                "uom": evidence["uom"],
                "membership_mode": draft.membership_mode,
                "member_ids": list(evidence["member_ids"]),
                "membership_hash": evidence["membership_hash"],
                "sample_barcodes": list(draft.sample_barcodes),
            }
            if draft.membership_mode == "EXACT_RESCAN":
                if draft.exact_rescan_barcodes != server_barcodes:
                    raise PackageLogisticsError(
                        "EXACT_RESCAN must equal the package work-group full membership"
                    )
                payload["exact_rescan_barcodes"] = list(
                    draft.exact_rescan_barcodes
                )
                payload["barcode_membership_hash"] = (
                    barcode_membership_hash(
                        draft.exact_rescan_barcodes
                    )
                )
            _attach_operation_lease(payload, draft)
            command = {
                "contract_version": PACKAGE_CONTRACT_VERSION,
                "command_type": "CREATE_PACKAGE",
                "authority_scope_id": evidence["authority_scope_id"],
                "authority_epoch": evidence["authority_epoch"],
                "ledger_plane": evidence["ledger_plane"],
                "plane_epoch": evidence["plane_epoch"],
                "idempotency_key": idempotency_key,
                "expected_versions": dict(evidence["entity_versions"]),
                "payload": payload,
            }
            self._assert_authority(
                command["authority_scope_id"],
                authority_epoch=command["authority_epoch"],
                ledger_plane=command["ledger_plane"],
                plane_epoch=command["plane_epoch"],
            )
            group_id = str(
                evidence["phs_work_group"].get("group_id") or ""
            ).strip()
            if not group_id:
                raise PackageLogisticsError(
                    "package work-group resolver returned no group identity"
                )
            return group_id, command
        resolved_projection: Mapping[str, Any] | None = None
        if not source_id:
            resolved = self.resolve_transfer_bundle(
                external_label=draft.source_external_label,
                input_tag_id=draft.source_input_tag_id,
                input_tag_label_id=draft.source_input_tag_label_id,
                input_tag_hash_prefix=draft.source_input_tag_hash_prefix,
                item_id=draft.item_code,
                authority_scope_id=scope,
                exact_rescan_barcodes=draft.exact_rescan_barcodes,
                source_bundle_hint=draft.source_bundle_hint,
            )
            source = self._resolver_bundle(resolved)
            self._validate_projection(
                source,
                draft,
                expected_scope=scope,
                require_package_source_role=True,
            )
            source_id = str(
                source.get("transfer_bundle_id") or source.get("bundle_id") or ""
            ).strip()
            if not source_id:
                raise PackageLogisticsError("sealed transfer resolver returned no transfer bundle ID")
            resolved_projection = source
        projection = self.get_bundle(source_id, authority_scope_id=scope)
        evidence = self._validate_projection(
            projection,
            draft,
            expected_scope=scope,
        )
        projection_id = str(
            projection.get("transfer_bundle_id") or projection.get("bundle_id") or ""
        ).strip()
        if projection_id != source_id:
            raise PackageLogisticsError("sealed transfer projection identity changed after resolution")
        if resolved_projection is not None:
            resolved_id = str(
                resolved_projection.get("transfer_bundle_id")
                or resolved_projection.get("bundle_id")
                or ""
            ).strip()
            if resolved_id != projection_id:
                raise PackageLogisticsError("PACKAGE_SOURCE resolver lineage changed before packaging")
        version = int(projection.get("entity_version") or 0)
        if version < 1:
            raise PackageLogisticsError("sealed transfer bundle entity_version is invalid")
        barcode_to_unit = evidence["barcode_to_unit"]
        server_barcodes = evidence["barcodes"]
        if draft.sample_barcodes and not set(draft.sample_barcodes).issubset(set(server_barcodes)):
            raise PackageLogisticsError("QA sample barcode is outside the sealed transfer membership")
        payload: dict[str, Any] = {
            "source_bundle_id": source_id,
            "package_bundle_id": draft.package_bundle_id,
            "external_label": draft.external_label,
            "membership_mode": draft.membership_mode,
        }
        if draft.sample_barcodes:
            payload["sample_barcodes"] = list(draft.sample_barcodes)
        if draft.membership_mode == "INHERIT_ALL":
            payload["source_evidence"] = {
                "member_ids": list(evidence["member_ids"]),
                "membership_hash": evidence["membership_hash"],
                "barcode_membership_hash": evidence["barcode_membership_hash"],
            }
        if draft.membership_mode == "EXACT_RESCAN":
            if draft.exact_rescan_barcodes != server_barcodes:
                raise PackageLogisticsError("EXACT_RESCAN must equal the sealed transfer full membership")
            unit_ids = canonical_member_ids(barcode_to_unit[barcode] for barcode in server_barcodes)
            payload["member_ids"] = list(unit_ids)
            payload["membership_hash"] = membership_hash(unit_ids)
            payload["exact_rescan_barcodes"] = list(draft.exact_rescan_barcodes)
            payload["barcode_membership_hash"] = barcode_membership_hash(
                draft.exact_rescan_barcodes
            )
        _attach_operation_lease(payload, draft)
        command = {
            "contract_version": PACKAGE_CONTRACT_VERSION,
            "command_type": "CREATE_PACKAGE",
            "authority_scope_id": str(projection.get("authority_scope_id") or "").strip(),
            "authority_epoch": int(projection.get("authority_epoch") or 0),
            "ledger_plane": str(projection.get("ledger_plane") or "").strip(),
            "plane_epoch": int(projection.get("plane_epoch") or 0),
            "idempotency_key": idempotency_key,
            "expected_versions": {f"bundle:{source_id}": version},
            "payload": payload,
        }
        self._assert_authority(
            command["authority_scope_id"],
            authority_epoch=command["authority_epoch"],
            ledger_plane=command["ledger_plane"],
            plane_epoch=command["plane_epoch"],
        )
        return source_id, command

    @writer_sink("package_create")
    def create_package(self, command: Mapping[str, Any]) -> dict[str, Any]:
        key = str(command.get("idempotency_key") or "").strip()
        if not key:
            raise PackageLogisticsError("idempotency key is required")
        self._assert_authority(
            str(command.get("authority_scope_id") or ""),
            authority_epoch=command.get("authority_epoch"),
            ledger_plane=str(command.get("ledger_plane") or ""),
            plane_epoch=command.get("plane_epoch"),
        )
        body = json.dumps(dict(command), ensure_ascii=False, sort_keys=True).encode("utf-8")
        try:
            return self._data(self._request("POST", "/logistics/api/v1/packages", body=body, key=key))
        except PackageApiError as original:
            if original.committed is not True:
                raise
            try:
                recovered = self.get_receipt_if_exists(
                    key,
                    authority_scope_id=str(command.get("authority_scope_id") or ""),
                )
            except PackageLogisticsError:
                recovered = None
            if recovered is not None:
                return recovered
            raise original
        except PackageTransportError as original:
            try:
                return self.get_receipt(key, authority_scope_id=str(command.get("authority_scope_id") or ""))
            except PackageApiError as receipt_error:
                if receipt_error.status_code == 404 or receipt_error.code == "RECEIPT_NOT_FOUND":
                    raise original
                raise

    def build_cancel_package_command(
        self,
        intent: PackageCancellationIntent,
        outbox_row: Mapping[str, Any],
        *,
        idempotency_key: str,
    ) -> dict[str, Any]:
        package_bundle_id = str(outbox_row.get("package_bundle_id") or "").strip()
        scope = str(outbox_row.get("authority_scope_id") or "").strip()
        plane = str(outbox_row.get("ledger_plane") or "").strip().upper()
        authority_epoch = int(outbox_row.get("authority_epoch") or 0)
        plane_epoch = int(outbox_row.get("plane_epoch") or 0)
        expected_version = int(outbox_row.get("expected_bundle_version") or 0)
        if not package_bundle_id or not scope or not plane or plane_epoch < 1 or expected_version < 1:
            raise PackageLogisticsError("package cancellation CREATE_PACKAGE ACK context is incomplete")
        if authority_epoch < 0:
            raise PackageLogisticsError("package cancellation authority epoch is invalid")
        if self.config.authority_scope_id and scope != self.config.authority_scope_id:
            raise PackageLogisticsError("package cancellation scope is outside the configured allowlist")
        if not idempotency_key:
            raise PackageLogisticsError("package cancellation idempotency key is required")
        evidence = {
            **dict(intent.evidence),
            "cancellation_event_id": intent.cancellation_event_id,
            "event_type": intent.event_type,
            "set_id": intent.set_id,
            "create_package_idempotency_key": str(
                outbox_row.get("package_idempotency_key") or ""
            ),
        }
        command = {
            "contract_version": PACKAGE_CONTRACT_VERSION,
            "command_type": "CANCEL_PACKAGE",
            "authority_scope_id": scope,
            "authority_epoch": authority_epoch,
            "ledger_plane": plane,
            "plane_epoch": plane_epoch,
            "idempotency_key": idempotency_key,
            "expected_versions": {f"bundle:{package_bundle_id}": expected_version},
            "payload": {
                "package_bundle_id": package_bundle_id,
                "reason": intent.reason,
                "evidence": evidence,
            },
        }
        self._assert_authority(
            scope,
            authority_epoch=authority_epoch,
            ledger_plane=plane,
            plane_epoch=plane_epoch,
        )
        return command

    @writer_sink("package_cancel")
    def cancel_package(self, command: Mapping[str, Any]) -> dict[str, Any]:
        key = str(command.get("idempotency_key") or "").strip()
        if not key:
            raise PackageLogisticsError("package cancellation idempotency key is required")
        self._assert_authority(
            str(command.get("authority_scope_id") or ""),
            authority_epoch=command.get("authority_epoch"),
            ledger_plane=str(command.get("ledger_plane") or ""),
            plane_epoch=command.get("plane_epoch"),
        )
        body = json.dumps(dict(command), ensure_ascii=False, sort_keys=True).encode("utf-8")
        try:
            return self._data(
                self._request(
                    "POST", "/logistics/api/v1/packages/cancel", body=body, key=key
                )
            )
        except PackageTransportError as original:
            try:
                return self.get_receipt(
                    key, authority_scope_id=str(command.get("authority_scope_id") or "")
                )
            except PackageApiError as receipt_error:
                if receipt_error.status_code == 404 or receipt_error.code == "RECEIPT_NOT_FOUND":
                    raise original
                raise

    def get_receipt(self, idempotency_key: str, *, authority_scope_id: str = "") -> dict[str, Any]:
        scope = str(authority_scope_id or self.config.authority_scope_id or "").strip()
        if not scope:
            raise PackageLogisticsError("authority scope is required for receipt recovery")
        self._assert_authority(scope)
        path = (
            "/logistics/api/v1/receipts/"
            + quote(scope, safe="")
            + "/"
            + quote(str(idempotency_key), safe="")
        )
        return self._data(self._request("GET", path))

    def get_receipt_if_exists(
        self, idempotency_key: str, *, authority_scope_id: str
    ) -> dict[str, Any] | None:
        try:
            return self.get_receipt(idempotency_key, authority_scope_id=authority_scope_id)
        except PackageApiError as exc:
            if exc.status_code == 404 or exc.code == "RECEIPT_NOT_FOUND":
                return None
            raise

    def _request(self, method: str, path: str, *, body: bytes | None = None, key: str = ""):
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {self.config.token}",
            "X-Logistics-API-Token": self.config.token,
            "User-Agent": PACKAGE_HTTP_USER_AGENT,
            "X-KMTech-Client": PACKAGE_HTTP_CLIENT_HEADER,
            "X-Logistics-Source-Host-Id": self.config.source_host_id,
            "X-Logistics-Device-Id": self.config.device_id,
            "X-Logistics-Program": "Label_Match",
        }
        if key:
            headers["Idempotency-Key"] = key
        return self._transport(
            method,
            self.config.base_url.rstrip("/") + path,
            headers,
            body,
            self.config.timeout_seconds,
        )

    @staticmethod
    def _data(response: Mapping[str, Any]) -> dict[str, Any]:
        ok = response.get("ok")
        if ok is not True and ok is not False:
            raise PackageTransportError("package API ok must be a JSON boolean")
        if ok is False:
            error = response.get("error") if isinstance(response.get("error"), Mapping) else {}
            retry_after = None
            retry_after_candidates = []
            if "retry_after_seconds" in error:
                retry_after_candidates.append(error.get("retry_after_seconds"))
            if "retry_after_seconds" in response:
                retry_after_candidates.append(response.get("retry_after_seconds"))
            for candidate in retry_after_candidates:
                retry_after = _parse_retry_after_seconds(candidate)
                if retry_after is not None:
                    break
            raise PackageApiError(
                int(error.get("status_code") or 400),
                str(error.get("code") or "PACKAGE_API_ERROR"),
                str(error.get("message") or "package command rejected"),
                retryable=_first_optional_bool(
                    error.get("retryable") if "retryable" in error else None,
                    response.get("retryable"),
                ),
                committed=_first_optional_bool(
                    error.get("committed") if "committed" in error else None,
                    response.get("committed"),
                ),
                retry_after_seconds=retry_after,
            )
        data = response.get("data", response)
        if not isinstance(data, Mapping):
            raise PackageTransportError("package API data must be an object")
        return dict(data)

    @staticmethod
    def _resolver_bundle(resolved: Mapping[str, Any]) -> dict[str, Any]:
        bundle = resolved.get("bundle") if isinstance(resolved, Mapping) else None
        if not isinstance(bundle, Mapping):
            raise PackageLogisticsError(
                "PACKAGE_SOURCE resolver response is missing its canonical bundle projection"
            )
        candidate_count = resolved.get("candidate_count")
        if (
            isinstance(candidate_count, bool)
            or not isinstance(candidate_count, int)
            or candidate_count != 1
        ):
            raise PackageLogisticsError("PACKAGE_SOURCE resolver did not select exactly one transfer")
        return dict(bundle)

    @staticmethod
    def _validate_transfer_seal(
        value: Any,
        *,
        bundle_id: str,
        bundle_version: int,
        member_ids: tuple[str, ...],
        item_id: str,
        expected_state: str,
    ) -> dict[str, Any]:
        return _validate_transfer_seal(
            value,
            bundle_id=bundle_id,
            bundle_version=bundle_version,
            member_ids=member_ids,
            item_id=item_id,
            expected_state=expected_state,
        )

    @staticmethod
    def _validate_work_group_source(
        resolved: Mapping[str, Any],
        draft: PackageCommandDraft,
        *,
        expected_scope: str,
    ) -> dict[str, Any]:
        return _validate_work_group_source(
            resolved,
            draft,
            expected_scope=expected_scope,
            validate_transfer_seal=PackageLogisticsClient._validate_transfer_seal,
        )

    @staticmethod
    def _validate_projection(
        projection: Mapping[str, Any],
        draft: PackageCommandDraft,
        *,
        expected_scope: str = "",
        require_package_source_role: bool = False,
    ) -> dict[str, Any]:
        return _validate_projection(
            projection,
            draft,
            expected_scope=expected_scope,
            require_package_source_role=require_package_source_role,
        )


class PackageOutboxProcessor(_PackageOutboxProcessor):
    """Preserve writer admission and resolve receipt checks at call time."""

    @writer_sink("package_outbox_drain")
    def drain(self, *, limit: int = 20) -> dict[str, int]:
        return super().drain(limit=limit)

    @staticmethod
    def _validate_work_group_receipt(
        draft: PackageCommandDraft,
        source_identity: str,
        receipt: Mapping[str, Any],
        *,
        command: Mapping[str, Any] | None,
    ) -> None:
        return _PackageOutboxProcessor._validate_work_group_receipt(
            draft,
            source_identity,
            receipt,
            command=command,
            validate_transfer_seal=PackageLogisticsClient._validate_transfer_seal,
        )

    @staticmethod
    def _validate_receipt(
        draft: PackageCommandDraft,
        source_bundle_id: str,
        receipt: Mapping[str, Any],
        *,
        command: Mapping[str, Any] | None = None,
    ) -> None:
        return _PackageOutboxProcessor._validate_receipt(
            draft,
            source_bundle_id,
            receipt,
            command=command,
            validate_operation_lease_receipt=PackageOutboxProcessor._validate_operation_lease_receipt,
            validate_work_group_receipt=PackageOutboxProcessor._validate_work_group_receipt,
        )


class PackageCancellationOutboxProcessor:
    def __init__(self, outbox: PackageCancellationOutbox, client: PackageLogisticsClient):
        self.outbox = outbox
        self.client = client
        self._drain_lock = threading.Lock()

    @writer_sink("package_cancellation_drain")
    def drain(self, *, limit: int = 20) -> dict[str, int]:
        counts = {"acked": 0, "retry": 0, "conflict": 0, "deferred": 0}
        with self._drain_lock:
            for _ in range(max(0, int(limit))):
                row = self.outbox.claim_next()
                if row is None:
                    break
                key = row["idempotency_key"]
                try:
                    intent_data = json.loads(row["intent_json"])
                    intent = PackageCancellationIntent(
                        cancellation_event_id=str(intent_data["cancellation_event_id"]),
                        set_id=str(intent_data["set_id"]),
                        event_type=str(intent_data["event_type"]),
                        reason=str(intent_data["reason"]),
                        evidence=dict(intent_data.get("evidence") or {}),
                        local_event_details=dict(intent_data.get("local_event_details") or {}),
                    )
                    # Fail closed before any cancellation GET/POST when the
                    # linked authoritative CREATE receipt is missing or has
                    # drifted from its immutable command/draft membership.
                    self._validate_linked_create_receipt(row)
                    if row.get("command_json"):
                        command = json.loads(row["command_json"])
                        scope = str(command.get("authority_scope_id") or "").strip()
                        receipt = self.client.get_receipt_if_exists(
                            key, authority_scope_id=scope
                        )
                        if receipt is None:
                            receipt = self.client.cancel_package(command)
                    else:
                        command = self.client.build_cancel_package_command(
                            intent, row, idempotency_key=key
                        )
                        self.outbox.save_command(key, command)
                        row = {
                            **row,
                            "command_json": json.dumps(
                                command,
                                ensure_ascii=False,
                                sort_keys=True,
                                separators=(",", ":"),
                            ),
                        }
                        receipt = self.client.cancel_package(command)
                    self._validate_receipt(row, receipt)
                    self.outbox.mark_acked(key, receipt)
                    counts["acked"] += 1
                except PackageApiError as exc:
                    if exc.committed is True:
                        self.outbox.mark_conflict(key, exc)
                        counts["conflict"] += 1
                    elif (
                        exc.status_code not in {409, 412}
                        and (
                            exc.status_code in {408, 425, 429}
                            or exc.status_code >= 500
                            or exc.retryable is True
                        )
                    ):
                        self.outbox.mark_retry(key, exc)
                        counts["retry"] += 1
                    else:
                        self.outbox.mark_conflict(key, exc)
                        counts["conflict"] += 1
                except PackageTransportError as exc:
                    self.outbox.mark_retry(key, exc)
                    counts["retry"] += 1
                except (KeyError, TypeError, ValueError, json.JSONDecodeError, PackageLogisticsError) as exc:
                    if not isinstance(exc, PackageLogisticsError):
                        exc = PackageLogisticsError("saved package cancellation intent is invalid")
                    self.outbox.mark_conflict(key, exc)
                    counts["conflict"] += 1
        counts["deferred"] = self.outbox.counts()["DEFERRED"]
        return counts

    @staticmethod
    def _validate_linked_create_receipt(
        outbox_row: Mapping[str, Any],
    ) -> tuple[tuple[str, ...], int, str]:
        expected_key = str(
            outbox_row.get("package_idempotency_key") or ""
        ).strip()
        linked_key = str(
            outbox_row.get("linked_create_idempotency_key") or ""
        ).strip()
        package_bundle_id = str(
            outbox_row.get("package_bundle_id") or ""
        ).strip()
        if (
            not expected_key
            or linked_key != expected_key
            or str(outbox_row.get("create_status") or "").upper() != "ACKED"
            or not package_bundle_id
        ):
            raise PackageLogisticsError(
                "linked CREATE_PACKAGE identity is invalid"
            )
        try:
            create_command = json.loads(
                str(outbox_row.get("create_command_json") or "")
            )
            create_draft = json.loads(
                str(outbox_row.get("create_draft_json") or "")
            )
            create_receipt = json.loads(
                str(outbox_row.get("create_receipt_json") or "")
            )
        except (TypeError, json.JSONDecodeError) as exc:
            raise PackageLogisticsError(
                "linked CREATE_PACKAGE receipt is invalid"
            ) from exc
        if (
            not isinstance(create_command, Mapping)
            or not isinstance(create_draft, Mapping)
            or not isinstance(create_receipt, Mapping)
        ):
            raise PackageLogisticsError(
                "linked CREATE_PACKAGE receipt is invalid"
            )
        create_payload = create_command.get("payload")
        try:
            create_authority_epoch = int(
                create_command.get("authority_epoch")
            )
            create_plane_epoch = int(create_command.get("plane_epoch"))
            expected_authority_epoch = int(
                outbox_row.get("authority_epoch")
            )
            expected_plane_epoch = int(outbox_row.get("plane_epoch"))
        except (TypeError, ValueError) as exc:
            raise PackageLogisticsError(
                "linked CREATE_PACKAGE authority identity is invalid"
            ) from exc
        if (
            str(create_command.get("contract_version") or "")
            != PACKAGE_CONTRACT_VERSION
            or str(create_command.get("command_type") or "") != "CREATE_PACKAGE"
            or str(create_command.get("idempotency_key") or "") != expected_key
            or str(create_command.get("authority_scope_id") or "")
            != str(outbox_row.get("authority_scope_id") or "")
            or create_authority_epoch != expected_authority_epoch
            or str(create_command.get("ledger_plane") or "").upper()
            != str(outbox_row.get("ledger_plane") or "").upper()
            or create_plane_epoch != expected_plane_epoch
            or not isinstance(create_payload, Mapping)
            or str(create_payload.get("package_bundle_id") or "")
            != package_bundle_id
            or str(create_draft.get("set_id") or "")
            != str(outbox_row.get("set_id") or "")
            or str(create_draft.get("package_bundle_id") or "")
            != package_bundle_id
            or (
                str(create_draft.get("source_bundle_id") or "")
                and str(create_payload.get("source_bundle_id") or "")
                != str(create_draft.get("source_bundle_id") or "")
            )
            or not str(create_receipt.get("receipt_id") or "").strip()
        ):
            raise PackageLogisticsError(
                "linked CREATE_PACKAGE identity is invalid"
            )
        receipt_command_type = str(
            create_receipt.get("command_type") or ""
        ).strip()
        receipt_contract_version = str(
            create_receipt.get("contract_version") or ""
        ).strip()
        receipt_status = str(create_receipt.get("status") or "").strip().upper()
        try:
            receipt_authority_epoch = int(create_receipt.get("authority_epoch"))
            receipt_plane_epoch = int(create_receipt.get("resolved_plane_epoch"))
        except (TypeError, ValueError) as exc:
            raise PackageLogisticsError(
                "linked CREATE_PACKAGE receipt authority identity is invalid"
            ) from exc
        if (
            receipt_command_type != "CREATE_PACKAGE"
            or receipt_contract_version != PACKAGE_CONTRACT_VERSION
            or receipt_status != "COMMITTED"
            or str(create_receipt.get("authority_scope_id") or "")
            != str(create_command.get("authority_scope_id") or "")
            or receipt_authority_epoch != create_authority_epoch
            or str(create_receipt.get("resolved_ledger_plane") or "").upper()
            != str(create_command.get("ledger_plane") or "").upper()
            or receipt_plane_epoch != create_plane_epoch
            or not str(create_receipt.get("committed_at") or "").strip()
            or not isinstance(create_receipt.get("event_ids"), (list, tuple))
            or not create_receipt.get("event_ids")
            or any(
                not str(value or "").strip()
                for value in (create_receipt.get("event_ids") or ())
            )
            or not isinstance(create_receipt.get("outbox_ids"), (list, tuple))
            or not create_receipt.get("outbox_ids")
            or any(
                not str(value or "").strip()
                for value in (create_receipt.get("outbox_ids") or ())
            )
        ):
            raise PackageLogisticsError(
                "linked CREATE_PACKAGE receipt identity is invalid"
            )
        create_data = (
            create_receipt.get("data")
            if isinstance(create_receipt.get("data"), Mapping)
            else create_receipt
        )
        if (
            not isinstance(create_data, Mapping)
            or str(create_data.get("package_bundle_id") or "")
            != package_bundle_id
        ):
            raise PackageLogisticsError(
                "linked CREATE_PACKAGE receipt data is invalid"
            )
        create_raw_members = create_data.get("member_ids")
        if not isinstance(create_raw_members, (list, tuple)):
            raise PackageLogisticsError(
                "linked CREATE_PACKAGE exact member IDs are missing"
            )
        create_members = canonical_member_ids(create_raw_members)
        create_normalized_raw = tuple(
            str(value or "").strip() for value in create_raw_members
        )
        try:
            create_count = int(create_data.get("member_count") or 0)
            expected_count = int(create_draft.get("expected_member_count") or 0)
            expected_version = int(
                outbox_row.get("expected_bundle_version") or 0
            )
        except (TypeError, ValueError) as exc:
            raise PackageLogisticsError(
                "linked CREATE_PACKAGE member count/version is invalid"
            ) from exc
        create_digest = str(
            create_data.get("membership_hash") or ""
        ).strip().lower()
        expected_digest = str(
            create_draft.get("expected_membership_hash") or ""
        ).strip().lower()
        versions = create_receipt.get("entity_versions")
        if not isinstance(versions, Mapping):
            versions = create_data.get("entity_versions")
        try:
            receipt_version = int(
                (versions or {}).get(f"bundle:{package_bundle_id}") or 0
            )
        except (TypeError, ValueError, AttributeError) as exc:
            raise PackageLogisticsError(
                "linked CREATE_PACKAGE receipt version is invalid"
            ) from exc
        if (
            not create_members
            or any(not value for value in create_normalized_raw)
            or len(create_normalized_raw) != len(create_members)
            or create_count != len(create_members)
            or create_digest != membership_hash(create_members)
            or (expected_count and create_count != expected_count)
            or (expected_digest and create_digest != expected_digest)
            or expected_version < 1
            or receipt_version != expected_version
        ):
            raise PackageLogisticsError(
                "linked CREATE_PACKAGE membership/version is invalid"
            )
        return create_members, create_count, create_digest

    @staticmethod
    def _validate_receipt(outbox_row: Mapping[str, Any], receipt: Mapping[str, Any]) -> None:
        if not isinstance(receipt, Mapping):
            raise PackageLogisticsError("package cancellation receipt is invalid")
        key = str(outbox_row.get("idempotency_key") or "").strip()
        try:
            command = json.loads(str(outbox_row.get("command_json") or ""))
            intent = json.loads(str(outbox_row.get("intent_json") or ""))
        except (TypeError, json.JSONDecodeError) as exc:
            raise PackageLogisticsError("saved package cancellation command is invalid") from exc
        if not isinstance(command, Mapping) or not isinstance(intent, Mapping):
            raise PackageLogisticsError("saved package cancellation command is invalid")
        package_bundle_id = str(outbox_row.get("package_bundle_id") or "").strip()
        command_payload = command.get("payload")
        command_versions = command.get("expected_versions")
        expected_evidence = {
            **dict(intent.get("evidence") or {}),
            "cancellation_event_id": str(intent.get("cancellation_event_id") or ""),
            "event_type": str(intent.get("event_type") or ""),
            "set_id": str(intent.get("set_id") or ""),
            "create_package_idempotency_key": str(
                outbox_row.get("package_idempotency_key") or ""
            ),
        }
        try:
            expected_version = int(outbox_row.get("expected_bundle_version") or 0)
            command_expected_version = int(
                (command_versions or {}).get(f"bundle:{package_bundle_id}") or 0
            )
            receipt_authority_epoch = int(receipt.get("authority_epoch"))
            command_authority_epoch = int(command.get("authority_epoch"))
            receipt_plane_epoch = int(receipt.get("resolved_plane_epoch"))
            command_plane_epoch = int(command.get("plane_epoch"))
        except (TypeError, ValueError, AttributeError) as exc:
            raise PackageLogisticsError(
                "package cancellation command/receipt version identity is invalid"
            ) from exc
        if (
            not key
            or str(command.get("contract_version") or "") != PACKAGE_CONTRACT_VERSION
            or str(command.get("idempotency_key") or "") != key
            or str(command.get("command_type") or "") != "CANCEL_PACKAGE"
            or str(command.get("authority_scope_id") or "")
            != str(outbox_row.get("authority_scope_id") or "")
            or command_authority_epoch != int(outbox_row.get("authority_epoch"))
            or str(command.get("ledger_plane") or "").upper()
            != str(outbox_row.get("ledger_plane") or "").upper()
            or command_plane_epoch != int(outbox_row.get("plane_epoch"))
            or not isinstance(command_payload, Mapping)
            or str(command_payload.get("package_bundle_id") or "") != package_bundle_id
            or str(command_payload.get("reason") or "")
            != str(intent.get("reason") or "")
            or command_payload.get("evidence") != expected_evidence
            or not isinstance(command_versions, Mapping)
            or command_expected_version != expected_version
        ):
            raise PackageLogisticsError("package cancellation command identity is invalid")
        receipt_id = str(receipt.get("receipt_id") or "").strip()
        receipt_idempotency_key = str(receipt.get("idempotency_key") or "").strip()
        if (
            not receipt_id
            or str(receipt.get("contract_version") or "") != PACKAGE_CONTRACT_VERSION
            or str(receipt.get("command_type") or "") != "CANCEL_PACKAGE"
            or str(receipt.get("status") or "").upper() != "COMMITTED"
            or str(receipt.get("authority_scope_id") or "")
            != str(command.get("authority_scope_id") or "")
            or receipt_authority_epoch != command_authority_epoch
            or str(receipt.get("resolved_ledger_plane") or "").upper()
            != str(command.get("ledger_plane") or "").upper()
            or receipt_plane_epoch != command_plane_epoch
            or not str(receipt.get("committed_at") or "").strip()
            or not isinstance(receipt.get("event_ids"), (list, tuple))
            or not receipt.get("event_ids")
            or any(
                not str(value or "").strip()
                for value in (receipt.get("event_ids") or ())
            )
            or not isinstance(receipt.get("outbox_ids"), (list, tuple))
            or not receipt.get("outbox_ids")
            or any(
                not str(value or "").strip()
                for value in (receipt.get("outbox_ids") or ())
            )
            or (receipt_idempotency_key and receipt_idempotency_key != key)
        ):
            raise PackageLogisticsError("package cancellation receipt identity is invalid")
        data = receipt.get("data") if isinstance(receipt.get("data"), Mapping) else receipt
        if not isinstance(data, Mapping):
            raise PackageLogisticsError("package cancellation receipt data is invalid")
        if str(data.get("package_bundle_id") or "") != package_bundle_id:
            raise PackageLogisticsError("package cancellation receipt bundle does not match")
        if (
            str(data.get("reason") or "") != str(command_payload.get("reason") or "")
            or data.get("evidence") != command_payload.get("evidence")
        ):
            raise PackageLogisticsError(
                "package cancellation receipt command evidence does not match"
            )
        if str(data.get("package_state") or "").upper() != "CANCELLED":
            raise PackageLogisticsError("package cancellation receipt state is invalid")
        if data.get("invalidated") is not True:
            raise PackageLogisticsError("package cancellation receipt is not invalidated")
        if str(data.get("bundle_state") or "").upper() != "AVAILABLE":
            raise PackageLogisticsError("package cancellation must preserve the available bundle state")
        if str(data.get("current_location") or "").upper() != "SHIPPING-WAIT":
            raise PackageLogisticsError("package cancellation must preserve SHIPPING-WAIT inventory")
        package_version = int(data.get("package_entity_version") or 0)
        if expected_version < 1 or package_version != expected_version + 1:
            raise PackageLogisticsError("package cancellation receipt version is invalid")
        versions = receipt.get("entity_versions")
        if not isinstance(versions, Mapping):
            raise PackageLogisticsError("package cancellation entity versions are missing")
        receipt_version = int(versions.get(f"bundle:{package_bundle_id}") or 0)
        if receipt_version != package_version:
            raise PackageLogisticsError("package cancellation entity version receipt is invalid")
        raw_members = data.get("member_ids")
        if not isinstance(raw_members, (list, tuple)):
            raise PackageLogisticsError("package cancellation exact member IDs are missing")
        members = canonical_member_ids(raw_members)
        member_count = int(data.get("member_count") or 0)
        digest = str(data.get("membership_hash") or "").strip().lower()
        normalized_raw_members = tuple(str(value or "").strip() for value in raw_members)
        if (
            not members
            or normalized_raw_members != members
            or member_count != len(members)
        ):
            raise PackageLogisticsError("package cancellation member count is invalid")
        if digest != membership_hash(members):
            raise PackageLogisticsError("package cancellation membership hash is invalid")
        create_members, create_count, create_digest = (
            PackageCancellationOutboxProcessor._validate_linked_create_receipt(
                outbox_row
            )
        )
        if (
            members != create_members
            or member_count != create_count
            or digest != create_digest
        ):
            raise PackageLogisticsError(
                "package cancellation membership does not match linked CREATE_PACKAGE receipt"
            )


def package_client_from_env(
    *,
    transport: Transport | None = None,
    probe_required: bool = True,
    environ: Mapping[str, str] | None = None,
    profile_decryptor: Any = None,
) -> PackageLogisticsClient | None:
    values = os.environ if environ is None else environ
    required = logistics_runtime_required(environ)
    profile = load_logistics_runtime_profile(
        required,
        environ=environ,
        decryptor=profile_decryptor,
    )
    if profile is not None:
        config = PackageClientConfig(
            base_url=profile.base_url,
            token=profile.bearer_token,
            authority_scope_id=profile.authority_scope,
            source_host_id=profile.source_host_id,
            device_id=profile.device_id,
            timeout_seconds=profile.timeout_seconds,
            authority_epoch=profile.authority_epoch,
            authority_plane=profile.authority_plane,
            ledger_plane=profile.ledger_plane,
            plane_epoch=profile.plane_epoch,
            authoritative_required=required,
            tls_ca_bundle_path=profile.tls_ca_bundle_path,
        )
    else:
        base_url = str(
            values.get("LABEL_MATCH_LOGISTICS_API_BASE_URL")
            or values.get("WORKER_ANALYSIS_LOGISTICS_API_BASE_URL")
            or ""
        ).strip()
        token = str(
            values.get("LABEL_MATCH_LOGISTICS_API_TOKEN")
            or values.get("WORKER_ANALYSIS_LOGISTICS_API_TOKEN")
            or ""
        ).strip()
        scope = str(
            values.get("LABEL_MATCH_LOGISTICS_AUTHORITY_SCOPE_ID")
            or values.get("WORKER_ANALYSIS_LOGISTICS_AUTHORITY_SCOPE_ID")
            or ""
        ).strip()
        host = str(
            values.get("LABEL_MATCH_LOGISTICS_SOURCE_HOST_ID")
            or values.get("COMPUTERNAME")
            or ""
        ).strip()
        if not base_url and not token:
            return None
        if not all((base_url, token, host)):
            raise LogisticsRuntimeConfigurationError(
                "legacy Label logistics environment profile is incomplete"
            )
        try:
            timeout = float(values.get("LABEL_MATCH_LOGISTICS_TIMEOUT_SECONDS") or 8)
        except (TypeError, ValueError) as exc:
            raise LogisticsRuntimeConfigurationError(
                "legacy Label logistics timeout is invalid"
            ) from exc
        config = PackageClientConfig(
            base_url=base_url,
            token=token,
            authority_scope_id=scope,
            source_host_id=host,
            device_id=str(values.get("LABEL_MATCH_LOGISTICS_DEVICE_ID") or host).strip(),
            timeout_seconds=timeout,
            tls_ca_bundle_path=str(
                values.get("LABEL_MATCH_LOGISTICS_TLS_CA_BUNDLE_PATH") or ""
            ).strip(),
        )
    try:
        client = PackageLogisticsClient(config=config, transport=transport)
    except PackageLogisticsError as exc:
        raise LogisticsRuntimeConfigurationError(
            "Label logistics runtime profile is invalid"
        ) from exc
    if required and probe_required:
        try:
            capabilities = client.get_capabilities()
            capability = (capabilities.get("capabilities") or {}).get(
                "sealed_transfer_member_replacement_v1"
            )
            if (
                "sealed_transfer_member_replacement_v1"
                not in (capabilities.get("capability_ids") or [])
                or not isinstance(capability, Mapping)
                or capability.get("enabled") is not True
                or capability.get("command_type")
                != "REPLACE_SEALED_TRANSFER_MEMBERS"
                or capability.get("endpoint_template")
                != "/logistics/api/v1/transfers/{target_bundle_id}/members/replace-and-reseal"
                or capability.get("receipt_contract_version")
                != "sealed-transfer-member-replacement-v1"
                or capability.get("replacement_source_bundle_cardinality")
                != "EXACTLY_ONE_ACTIVE_MEMBER"
                or capability.get("multi_member_source_policy")
                != "REJECT_STALE_PHYSICAL_LABEL"
                or capability.get("multi_member_source_error_code")
                != "REPLACEMENT_SOURCE_NOT_SINGLETON"
                or capability.get("seal_qr_contract_version")
                != "transfer-seal-qr-v1"
                or capability.get("max_pairs") != 2
                or capability.get("atomic") is not True
                or capability.get("fail_closed_when_unavailable") is not True
                or capability.get("disabled_server_behavior")
                != "REJECT_COMMAND_DO_NOT_MUTATE_LOCAL_STATE"
                or capability.get("client_rollout_gate")
                != "REQUIRE_ENABLED_CAPABILITY_AND_EXACT_RECEIPT"
            ):
                raise LogisticsRuntimeConfigurationError(
                    "authoritative package capability readiness is incomplete"
                )
        except LogisticsRuntimeConfigurationError:
            raise
        except PackageApiError as exc:
            raise LogisticsRuntimeConfigurationError(
                f"authoritative package readiness failed: {exc.code}"
            ) from exc
        except Exception as exc:
            raise LogisticsRuntimeConfigurationError(
                f"authoritative package readiness failed: {exc.__class__.__name__}"
            ) from exc
    return client


__all__ = [
    "PackageApiError",
    "PackageCancellationIntent",
    "PackageCancellationOutbox",
    "PackageCancellationOutboxProcessor",
    "PackageClientConfig",
    "PackageCommandDraft",
    "PackageLogisticsClient",
    "PackageLogisticsError",
    "PackageOutbox",
    "PackageOutboxProcessor",
    "PackageTransportError",
    "barcode_membership_hash",
    "canonical_barcodes",
    "canonical_member_ids",
    "membership_hash",
    "package_client_from_env",
    "LogisticsRuntimeConfigurationError",
]

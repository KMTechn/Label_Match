"""Durable package outbox storage; the facade supplies live clock/schema hooks."""

from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sqlite3
import threading
from typing import Any, Callable, Iterator, Mapping

from deferred_intent_capture import supersede_for_legacy_outbox
from package_command_draft import PackageCommandDraft, canonical_json, stable_id
from package_errors import PackageLogisticsError, _bounded_retry_after_seconds

class PackageOutbox:
    def __init__(
        self,
        db_path: str | Path,
        *,
        utc_now: Callable[[], str],
        utc_after: Callable[[float], str],
        utc_before: Callable[[float], str],
        sending_lease_seconds: Callable[[], float],
        initialize_schema: Callable[[sqlite3.Connection], None],
        post_review_event_payload: Callable[..., tuple[str, dict[str, Any], str]],
        post_review_conflict_origin: Callable[[Exception], str],
    ):
        self._utc_now = utc_now
        self._utc_after = utc_after
        self._utc_before = utc_before
        self._sending_lease_seconds = sending_lease_seconds
        self._initialize_schema = initialize_schema
        self._post_review_event_payload = post_review_event_payload
        self._post_review_conflict_origin = post_review_conflict_origin
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
            self._initialize_schema(conn)
            conn.commit()

    def enqueue(
        self,
        draft: PackageCommandDraft,
        *,
        captured_intent_id: str = "",
    ) -> dict[str, Any]:
        key = f"label-package-{stable_id('cmd', draft.set_id, draft.package_bundle_id)}"
        fingerprint = draft.fingerprint()
        now = self._utc_now()
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT * FROM package_command_outbox WHERE set_id=? OR idempotency_key=?",
                (draft.set_id, key),
            ).fetchone()
            if existing:
                if existing["command_fingerprint"] != fingerprint:
                    conn.rollback()
                    raise PackageLogisticsError("packaging set was already queued with different data")
                if str(captured_intent_id or "").strip():
                    supersede_for_legacy_outbox(
                        conn,
                        intent_id=captured_intent_id,
                        local_work_identity=draft.set_id,
                        downstream_outbox_ref=(
                            "package_command_outbox:"
                            + str(existing["idempotency_key"])
                        ),
                        occurred_at=now,
                    )
                conn.commit()
                return dict(existing)
            conn.execute(
                """
                INSERT INTO package_command_outbox(
                    idempotency_key,set_id,command_fingerprint,draft_json,status,created_at,updated_at
                ) VALUES (?,?,?,?, 'PENDING',?,?)
                """,
                (
                    key,
                    draft.set_id,
                    fingerprint,
                    json.dumps(draft.to_dict(), ensure_ascii=False, sort_keys=True),
                    now,
                    now,
                ),
            )
            row = conn.execute(
                "SELECT * FROM package_command_outbox WHERE idempotency_key=?", (key,)
            ).fetchone()
            if str(captured_intent_id or "").strip():
                supersede_for_legacy_outbox(
                    conn,
                    intent_id=captured_intent_id,
                    local_work_identity=draft.set_id,
                    downstream_outbox_ref=f"package_command_outbox:{key}",
                    occurred_at=now,
                )
            conn.commit()
            return dict(row)

    def link_captured_intent_to_existing(
        self,
        *,
        captured_intent_id: str,
        set_id: str,
        idempotency_key: str,
    ) -> None:
        """Repair an interrupted exact handoff without creating another row."""

        intent_id = str(captured_intent_id or "").strip()
        normalized_set_id = str(set_id or "").strip()
        key = str(idempotency_key or "").strip()
        if not all((intent_id, normalized_set_id, key)):
            raise PackageLogisticsError(
                "captured intent legacy handoff identity is incomplete"
            )
        now = self._utc_now()
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                """SELECT idempotency_key,set_id
                     FROM package_command_outbox
                    WHERE idempotency_key=? AND set_id=?""",
                (key, normalized_set_id),
            ).fetchone()
            if existing is None:
                raise PackageLogisticsError(
                    "exact package outbox row is unavailable for captured intent handoff"
                )
            supersede_for_legacy_outbox(
                conn,
                intent_id=intent_id,
                local_work_identity=normalized_set_id,
                downstream_outbox_ref=f"package_command_outbox:{key}",
                occurred_at=now,
            )
            conn.commit()

    @staticmethod
    def _replacement_waiting_event_payload(
        payload: Mapping[str, Any],
    ) -> tuple[str, dict[str, Any], str]:
        event = dict(payload or {})
        key = str(event.get("dedupe_key") or "").strip()
        if not key or str(event.get("intent_id") or "").strip() != key:
            raise PackageLogisticsError(
                "replacement-waiting event identity is required"
            )
        required = (
            "intent_version",
            "set_id",
            "session_id",
            "old_label_id",
            "new_label_id",
            "process",
            "location",
            "current_process",
            "current_location",
            "source_system",
            "source_pc_id",
            "marked_at",
        )
        if any(not str(event.get(name) or "").strip() for name in required):
            raise PackageLogisticsError(
                "replacement-waiting event payload is incomplete"
            )
        immutable = dict(event)
        # A replay reconstructs the wall-clock field after restart.  Preserve
        # the first durable timestamp while requiring every business field,
        # including process and location, to remain immutable.
        immutable.pop("marked_at", None)
        fingerprint = hashlib.sha256(
            json.dumps(
                immutable,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        return key, event, fingerprint

    def enqueue_replacement_waiting_event(
        self, payload: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Commit one immutable replacement-waiting event before CSV projection."""

        key, event, fingerprint = self._replacement_waiting_event_payload(
            payload
        )
        now = self._utc_now()
        encoded = json.dumps(
            event,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                """SELECT * FROM package_replacement_waiting_outbox
                     WHERE dedupe_key=?""",
                (key,),
            ).fetchone()
            if existing is not None:
                if str(existing["event_fingerprint"] or "") != fingerprint:
                    conn.rollback()
                    raise PackageLogisticsError(
                        "replacement-waiting event payload is immutable"
                    )
                conn.commit()
                return dict(existing)
            conn.execute(
                """INSERT INTO package_replacement_waiting_outbox(
                       dedupe_key,event_fingerprint,event_json,
                       local_csv_committed,created_at,updated_at
                   ) VALUES (?,?,?,0,?,?)""",
                (key, fingerprint, encoded, now, now),
            )
            row = conn.execute(
                """SELECT * FROM package_replacement_waiting_outbox
                     WHERE dedupe_key=?""",
                (key,),
            ).fetchone()
            conn.commit()
            return dict(row)

    def commit_replacement_waiting_csv_projection(
        self,
        key: str,
        projector: Callable[[Mapping[str, Any]], None],
    ) -> bool:
        """Serialize one fsynced CSV projection with its durable commit marker.

        The SQLite write transaction intentionally spans the projector call.
        This is a rare local event and the cross-process lock closes the
        scan/append race.  If the process dies after CSV fsync but before this
        transaction commits, recovery sees the durable PENDING ledger row,
        finds the CSV dedupe key, and marks it committed without appending.
        """

        identity = str(key or "").strip()
        if not identity or not callable(projector):
            raise PackageLogisticsError(
                "replacement-waiting CSV projection identity is required"
            )
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT * FROM package_replacement_waiting_outbox
                     WHERE dedupe_key=?""",
                (identity,),
            ).fetchone()
            if row is None:
                conn.rollback()
                raise PackageLogisticsError(
                    "replacement-waiting event ledger row does not exist"
                )
            if int(row["local_csv_committed"] or 0) == 1:
                conn.commit()
                return False
            try:
                payload = json.loads(str(row["event_json"] or "{}"))
                if not isinstance(payload, dict):
                    raise ValueError("event payload is not an object")
                projector(payload)
            except Exception:
                conn.rollback()
                raise
            now = self._utc_now()
            cursor = conn.execute(
                """UPDATE package_replacement_waiting_outbox
                       SET local_csv_committed=1,
                           local_csv_committed_at=?,updated_at=?
                     WHERE dedupe_key=? AND local_csv_committed=0""",
                (now, now, identity),
            )
            if cursor.rowcount != 1:
                conn.rollback()
                raise PackageLogisticsError(
                    "replacement-waiting CSV commit marker changed concurrently"
                )
            conn.commit()
            return True

    def get_replacement_waiting_event(self, key: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """SELECT * FROM package_replacement_waiting_outbox
                     WHERE dedupe_key=?""",
                (str(key or "").strip(),),
            ).fetchone()
            return dict(row) if row else None

    def list_replacement_waiting_csv_pending(
        self, *, limit: int = 100
    ) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT * FROM package_replacement_waiting_outbox
                     WHERE local_csv_committed=0
                     ORDER BY created_at,dedupe_key
                     LIMIT ?""",
                (max(0, int(limit)),),
            ).fetchall()
            return [dict(row) for row in rows]

    def commit_post_review_csv_projection(
        self,
        review_event_id: str,
        projector: Callable[[Mapping[str, Any]], None],
    ) -> bool:
        """Project one durable review case to CSV exactly once.

        As with replacement-waiting events, the SQLite transaction spans the
        fsynced projector call. Recovery can therefore close the crash window
        by detecting the deterministic review_event_id in CSV before it marks
        this row committed.
        """

        identity = str(review_event_id or "").strip()
        if not identity or not callable(projector):
            raise PackageLogisticsError(
                "post-review CSV projection identity is required"
            )
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT * FROM package_post_review_outbox
                     WHERE review_event_id=?""",
                (identity,),
            ).fetchone()
            if row is None:
                conn.rollback()
                raise PackageLogisticsError(
                    "post-review event ledger row does not exist"
                )
            if int(row["local_csv_committed"] or 0) == 1:
                conn.commit()
                return False
            try:
                payload = json.loads(str(row["event_json"] or "{}"))
                if not isinstance(payload, dict):
                    raise ValueError("event payload is not an object")
                projector(payload)
            except Exception:
                conn.rollback()
                raise
            now = self._utc_now()
            cursor = conn.execute(
                """UPDATE package_post_review_outbox
                       SET local_csv_committed=1,
                           local_csv_committed_at=?,updated_at=?
                     WHERE review_event_id=? AND local_csv_committed=0""",
                (now, now, identity),
            )
            if cursor.rowcount != 1:
                conn.rollback()
                raise PackageLogisticsError(
                    "post-review CSV commit marker changed concurrently"
                )
            conn.commit()
            return True

    def get_post_review_event(
        self, review_event_id: str
    ) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                """SELECT * FROM package_post_review_outbox
                     WHERE review_event_id=?""",
                (str(review_event_id or "").strip(),),
            ).fetchone()
            return dict(row) if row else None

    def list_post_review_csv_pending(
        self, *, limit: int = 100
    ) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT * FROM package_post_review_outbox
                     WHERE local_csv_committed=0
                     ORDER BY created_at,review_event_id
                     LIMIT ?""",
                (max(0, int(limit)),),
            ).fetchall()
            return [dict(row) for row in rows]

    def claim_next(
        self, *, exclude_keys: Iterable[str] = ()
    ) -> dict[str, Any] | None:
        now = self._utc_now()
        stale_before = self._utc_before(self._sending_lease_seconds())
        excluded = tuple(
            sorted(
                {
                    str(value or "").strip()
                    for value in exclude_keys
                    if str(value or "").strip()
                }
            )
        )
        exclusion_sql = (
            " AND idempotency_key NOT IN ("
            + ",".join("?" for _ in excluded)
            + ")"
            if excluded
            else ""
        )
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """UPDATE package_command_outbox
                      SET status='PENDING',updated_at=?
                    WHERE status='SENDING' AND updated_at<=?""",
                (now, stale_before),
            )
            row = conn.execute(
                """SELECT * FROM package_command_outbox
                     WHERE status='PENDING'
                       AND local_completion_committed=1
                       AND (retry_after_at IS NULL OR retry_after_at<=?)"""
                + exclusion_sql
                + " ORDER BY COALESCE(last_attempt_at,created_at),"
                  "created_at,idempotency_key LIMIT 1",
                (now, *excluded),
            ).fetchone()
            if row is None:
                conn.commit()
                return None
            cursor = conn.execute(
                """UPDATE package_command_outbox
                       SET status='SENDING',attempt_count=attempt_count+1,
                           retry_after_at=NULL,last_attempt_at=?,updated_at=?
                     WHERE idempotency_key=? AND status='PENDING'""",
                (now, now, row["idempotency_key"]),
            )
            if cursor.rowcount != 1:
                conn.rollback()
                return None
            claimed = conn.execute(
                "SELECT * FROM package_command_outbox WHERE idempotency_key=?",
                (row["idempotency_key"],),
            ).fetchone()
            conn.commit()
            return dict(claimed)

    def save_command(self, key: str, source_bundle_id: str, command: Mapping[str, Any]) -> None:
        encoded = json.dumps(dict(command), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT resolved_source_bundle_id,command_json,status FROM package_command_outbox WHERE idempotency_key=?",
                (key,),
            ).fetchone()
            if row is None or row["status"] != "SENDING":
                conn.rollback()
                raise PackageLogisticsError("package outbox command is not exclusively claimed")
            if row["command_json"]:
                existing = json.dumps(
                    json.loads(row["command_json"]),
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                if existing != encoded or str(row["resolved_source_bundle_id"] or "") != source_bundle_id:
                    conn.rollback()
                    raise PackageLogisticsError("saved package command is immutable")
                conn.commit()
                return
            cursor = conn.execute(
                """UPDATE package_command_outbox
                       SET resolved_source_bundle_id=?,command_json=?,updated_at=?
                     WHERE idempotency_key=? AND status='SENDING' AND command_json IS NULL""",
                (source_bundle_id, encoded, self._utc_now(), key),
            )
            if cursor.rowcount != 1:
                conn.rollback()
                raise PackageLogisticsError("package command lost its immutable save CAS")
            conn.commit()

    def mark_acked(
        self,
        key: str,
        receipt: Mapping[str, Any],
        *,
        operation_lease_id: str = "",
        operation_lease_consumption: Mapping[str, Any] | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            now = self._utc_now()
            cursor = conn.execute(
                """UPDATE package_command_outbox
                       SET status='ACKED',receipt_json=?,last_error_code=NULL,
                            last_error_message=NULL,retry_after_at=NULL,updated_at=?
                     WHERE idempotency_key=? AND status='SENDING'""",
                (json.dumps(dict(receipt), ensure_ascii=False, sort_keys=True), now, key),
            )
            if cursor.rowcount != 1:
                conn.rollback()
                raise PackageLogisticsError("package outbox ACK state changed concurrently")
            if operation_lease_id:
                lease_cursor = conn.execute(
                    """UPDATE package_operation_leases
                           SET status='ACKED',consume_receipt_json=?,
                               consume_claimed_at=NULL,last_error_code=NULL,
                               last_error_message=NULL,updated_at=?
                         WHERE lease_id=? AND status='LOCAL_COMPLETED'""",
                    (
                        canonical_json(
                            dict(operation_lease_consumption or {})
                        ),
                        now,
                        operation_lease_id,
                    ),
                )
                if lease_cursor.rowcount != 1:
                    conn.rollback()
                    raise PackageLogisticsError(
                        "operation lease ACK state changed concurrently"
                    )
                attempt_cursor = conn.execute(
                    """UPDATE package_operation_lease_issue_attempts
                           SET status='RETIRED',retire_reason='ACKED',
                               retired_at=?,updated_at=?
                         WHERE lease_id=? AND status='ACTIVE'""",
                    (now, now, operation_lease_id),
                )
                if attempt_cursor.rowcount != 1:
                    conn.rollback()
                    raise PackageLogisticsError(
                        "operation lease issue attempt ACK state changed concurrently"
                    )
            conn.commit()

    def mark_retry(self, key: str, error: Exception) -> None:
        retry_after_seconds = _bounded_retry_after_seconds(
            getattr(error, "retry_after_seconds", None)
        )
        retry_after_at = (
            self._utc_after(retry_after_seconds)
            if retry_after_seconds is not None
            else None
        )
        with self._connect() as conn:
            conn.execute(
                """UPDATE package_command_outbox
                       SET status='PENDING',last_error_code=?,last_error_message=?,
                           retry_after_at=?,updated_at=?
                     WHERE idempotency_key=? AND status='SENDING'""",
                (
                    str(getattr(error, "code", error.__class__.__name__)),
                    str(error),
                    retry_after_at,
                    self._utc_now(),
                    key,
                ),
            )
            conn.commit()

    def mark_conflict(
        self,
        key: str,
        error: Exception,
        *,
        operation_lease_id: str = "",
    ) -> None:
        code = str(getattr(error, "code", "LOCAL_VALIDATION_CONFLICT"))
        message = str(getattr(error, "message", str(error)))
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT * FROM package_command_outbox
                     WHERE idempotency_key=?""",
                (key,),
            ).fetchone()
            if row is None or str(row["status"] or "") != "SENDING":
                conn.rollback()
                raise PackageLogisticsError(
                    "package outbox conflict state changed concurrently"
                )
            now = self._utc_now()
            cursor = conn.execute(
                """UPDATE package_command_outbox
                       SET status='CONFLICT',review_status='OPERATOR_REVIEW',
                           last_error_code=?,last_error_message=?,
                           retry_after_at=NULL,updated_at=?
                     WHERE idempotency_key=? AND status='SENDING'""",
                (code, message, now, key),
            )
            if cursor.rowcount != 1:
                conn.rollback()
                raise PackageLogisticsError(
                    "package outbox conflict state changed concurrently"
                )
            if int(row["local_completion_committed"] or 0) == 1:
                source = dict(row)
                source["updated_at"] = now
                review_event_id, event, fingerprint = (
                    self._post_review_event_payload(
                        source,
                        conflict_code=code,
                        conflict_origin=self._post_review_conflict_origin(error),
                        required_at=now,
                    )
                )
                conn.execute(
                    """INSERT INTO package_post_review_outbox(
                           review_event_id,package_idempotency_key,
                           event_fingerprint,event_json,local_csv_committed,
                           created_at,updated_at
                       ) VALUES (?,?,?,?,0,?,?)""",
                    (
                        review_event_id,
                        key,
                        fingerprint,
                        canonical_json(event),
                        now,
                        now,
                    ),
                )
            if operation_lease_id:
                lease_cursor = conn.execute(
                    """UPDATE package_operation_leases
                           SET status='OPERATOR_REVIEW',consume_claimed_at=NULL,
                               last_error_code=?,last_error_message=?,updated_at=?
                         WHERE lease_id=? AND status='LOCAL_COMPLETED'""",
                    (code, message, now, operation_lease_id),
                )
                if lease_cursor.rowcount != 1:
                    conn.rollback()
                    raise PackageLogisticsError(
                        "operation lease review state changed concurrently"
                    )
                attempt_cursor = conn.execute(
                    """UPDATE package_operation_lease_issue_attempts
                           SET status='RETIRED',
                               retire_reason='OPERATOR_REVIEW',
                               retired_at=?,updated_at=?
                         WHERE lease_id=? AND status='ACTIVE'""",
                    (now, now, operation_lease_id),
                )
                if attempt_cursor.rowcount != 1:
                    conn.rollback()
                    raise PackageLogisticsError(
                        "operation lease issue attempt review state changed concurrently"
                    )
            conn.commit()

    def get_by_set_id(self, set_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM package_command_outbox WHERE set_id=?", (str(set_id),)
            ).fetchone()
            return dict(row) if row else None

    def list_local_completion_pending(self, *, limit: int = 20) -> list[dict[str, Any]]:
        """Return durable package commands whose local completion is unresolved."""

        with self._connect() as conn:
            rows = conn.execute(
                """SELECT * FROM package_command_outbox
                     WHERE local_completion_committed=0
                       AND local_recovery_dismissed=0
                     ORDER BY created_at,idempotency_key
                     LIMIT ?""",
                (max(0, int(limit)),),
            ).fetchall()
            return [dict(row) for row in rows]

    def dismiss_recoverable_prewrite_conflict(
        self, key: str
    ) -> dict[str, Any]:
        """Dismiss local recovery without deleting terminal conflict evidence."""

        identity = str(key or "").strip()
        if not identity:
            raise PackageLogisticsError(
                "package conflict identity is required"
            )
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT * FROM package_command_outbox
                     WHERE idempotency_key=?""",
                (identity,),
            ).fetchone()
            if row is None:
                conn.rollback()
                raise PackageLogisticsError(
                    "package conflict does not exist"
                )
            if int(row["local_recovery_dismissed"] or 0) == 1:
                conn.commit()
                return dict(row)
            if (
                str(row["status"] or "").strip().upper() != "CONFLICT"
                or str(row["last_error_code"] or "").strip().upper()
                != "PHS_WORK_GROUP_COMMAND_CONFLICT"
                or not str(row["set_id"] or "").strip()
                or not str(row["command_json"] or "").strip()
                or str(row["receipt_json"] or "").strip()
                or int(row["local_completion_committed"] or 0) != 0
            ):
                conn.rollback()
                raise PackageLogisticsError(
                    "only an uncommitted PHS work-group command conflict "
                    "can dismiss local recovery"
                )
            now = self._utc_now()
            cursor = conn.execute(
                """UPDATE package_command_outbox
                      SET local_recovery_dismissed=1,
                          local_recovery_dismissed_at=?,updated_at=?
                    WHERE idempotency_key=?
                      AND status='CONFLICT'
                      AND local_recovery_dismissed=0""",
                (now, now, identity),
            )
            if cursor.rowcount != 1:
                conn.rollback()
                raise PackageLogisticsError(
                    "package conflict dismissal changed concurrently"
                )
            updated = conn.execute(
                """SELECT * FROM package_command_outbox
                     WHERE idempotency_key=?""",
                (identity,),
            ).fetchone()
            conn.commit()
            return dict(updated)

    def dismiss_superseded_recoverable_prewrite_conflicts(self) -> int:
        """Hide stale recovery notices after the same source was completed later.

        The terminal conflict row remains immutable audit evidence. Only its
        local operator-review projection is dismissed, and only after a newer
        ACKed command for the exact same resolved source bundle has both a
        receipt and a durable local completion marker.
        """

        now = self._utc_now()
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """
                UPDATE package_command_outbox AS stale
                   SET local_recovery_dismissed=1,
                       local_recovery_dismissed_at=?,
                       updated_at=?
                 WHERE stale.status='CONFLICT'
                   AND UPPER(TRIM(COALESCE(stale.last_error_code,'')))
                       ='PHS_WORK_GROUP_COMMAND_CONFLICT'
                   AND TRIM(COALESCE(stale.resolved_source_bundle_id,''))<>''
                   AND TRIM(COALESCE(stale.receipt_json,''))=''
                   AND stale.local_completion_committed=0
                   AND stale.local_recovery_dismissed=0
                   AND EXISTS (
                       SELECT 1
                         FROM package_command_outbox AS completed
                        WHERE completed.resolved_source_bundle_id
                              =stale.resolved_source_bundle_id
                          AND completed.status='ACKED'
                          AND TRIM(COALESCE(completed.receipt_json,''))<>''
                          AND completed.local_completion_committed=1
                          AND completed.created_at>stale.created_at
                   )
                """,
                (now, now),
            )
            dismissed = max(0, int(cursor.rowcount or 0))
            conn.commit()
            return dismissed

    def mark_local_completion_committed(
        self,
        key: str,
        *,
        operation_lease_id: str = "",
        operation_completed_at: str = "",
    ) -> None:
        """Record the durable local completion independently of central ACK.

        The command row is the append-only central intent.  Once the local
        TRAY_COMPLETE event has been flushed, this marker may be committed in
        any central delivery state; later retries or review must never undo it.
        """

        identity = str(key or "").strip()
        if not identity:
            raise PackageLogisticsError(
                "package local completion identity is required"
            )
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """SELECT status,local_completion_committed
                       FROM package_command_outbox
                      WHERE idempotency_key=?""",
                (identity,),
            ).fetchone()
            if row is None:
                conn.rollback()
                raise PackageLogisticsError(
                    "package local completion intent does not exist"
                )
            if int(row["local_completion_committed"] or 0) == 0:
                conn.execute(
                    """UPDATE package_command_outbox
                          SET local_completion_committed=1,
                              local_completion_committed_at=?,updated_at=?
                        WHERE idempotency_key=?
                          AND local_completion_committed=0""",
                    (self._utc_now(), self._utc_now(), identity),
                )
            if operation_lease_id:
                lease = conn.execute(
                    """SELECT status,operation_result_id,operation_completed_at
                           FROM package_operation_leases
                          WHERE lease_id=?""",
                    (operation_lease_id,),
                ).fetchone()
                if lease is None or lease["status"] not in {
                    "PREFETCHED",
                    "LOCAL_COMPLETED",
                }:
                    conn.rollback()
                    raise PackageLogisticsError(
                        "operation lease cannot record local completion"
                    )
                if lease["status"] == "LOCAL_COMPLETED" and (
                    str(lease["operation_result_id"] or "") != identity
                    or str(lease["operation_completed_at"] or "")
                    != operation_completed_at
                ):
                    conn.rollback()
                    raise PackageLogisticsError(
                        "operation lease local completion is immutable"
                    )
                conn.execute(
                    """UPDATE package_operation_leases
                           SET status='LOCAL_COMPLETED',
                               operation_result_id=?,operation_completed_at=?,
                               consume_idempotency_key=?,updated_at=?
                         WHERE lease_id=?
                           AND status IN ('PREFETCHED','LOCAL_COMPLETED')""",
                    (
                        identity,
                        operation_completed_at,
                        identity,
                        self._utc_now(),
                        operation_lease_id,
                    ),
                )
            conn.commit()

    def counts(self) -> dict[str, int]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT status,COUNT(*) AS count FROM package_command_outbox GROUP BY status"
            ).fetchall()
            result = {status: 0 for status in ("PENDING", "SENDING", "ACKED", "CONFLICT")}
            result.update({row["status"]: int(row["count"]) for row in rows})
            return result

    def list_conflicts(self, *, limit: int = 20) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT * FROM package_command_outbox
                     WHERE status='CONFLICT'
                       AND local_recovery_dismissed=0
                     ORDER BY updated_at DESC,idempotency_key
                     LIMIT ?""",
                (max(0, int(limit)),),
            ).fetchall()
            return [dict(row) for row in rows]

    def list_all_conflicts(self, *, limit: int = 20) -> list[dict[str, Any]]:
        """Return terminal conflicts, including locally dismissed evidence."""

        with self._connect() as conn:
            rows = conn.execute(
                """SELECT * FROM package_command_outbox
                     WHERE status='CONFLICT'
                     ORDER BY updated_at DESC,idempotency_key
                     LIMIT ?""",
                (max(0, int(limit)),),
            ).fetchall()
            return [dict(row) for row in rows]

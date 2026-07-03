"""Local operator controls for the Label_Match direct-sync relay."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import uuid
from pathlib import Path
from typing import Any, Mapping

from direct_sync_push import (
    DirectSyncPushError,
    ProducerCredentials,
    RELAY_STATUS_ACKED,
    RELAY_STATUS_FAILED_PERMANENT,
    RELAY_STATUS_OPERATOR_REVIEW,
    RELAY_STATUS_PENDING,
    build_raw_artifact_restore_url,
    restore_raw_artifact_to_file,
    utc_now_text,
)


PAUSE_SCHEMA_VERSION = "direct-sync-relay-operator-pause-v1"
AUDIT_SCHEMA_VERSION = "direct-sync-relay-operator-audit-v1"
OPERATOR_TOOL_VERSION = "label-match-local-operator-v1"
RETRYABLE_DEAD_STATUSES = frozenset({RELAY_STATUS_FAILED_PERMANENT})
SQLITE_BUSY_TIMEOUT_MS = 30000


def _write_json_atomic(path: str | os.PathLike[str], payload: Mapping[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target.with_suffix(target.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(dict(payload), handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp_path, target)


def _append_jsonl(path: str | os.PathLike[str], payload: Mapping[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(dict(payload), ensure_ascii=False, sort_keys=True))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def _connect_relay_db(db_path: str | os.PathLike[str], *, read_only: bool = False) -> sqlite3.Connection:
    path = Path(db_path)
    if read_only:
        uri = f"file:{path.resolve().as_posix()}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=SQLITE_BUSY_TIMEOUT_MS / 1000)
    else:
        conn = sqlite3.connect(str(path), timeout=SQLITE_BUSY_TIMEOUT_MS / 1000)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    return conn


def _require_text(value: str, *, field_name: str, max_length: int = 512) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field_name} is required")
    if len(text) > max_length:
        raise ValueError(f"{field_name} exceeds {max_length} characters")
    return text


def _reason_evidence(reason: str) -> dict[str, Any]:
    digest = hashlib.sha256(reason.encode("utf-8")).hexdigest()
    return {
        "reason_redacted": f"sha256:{digest[:12]}",
        "reason_sha256": digest,
        "reason_length": len(reason),
    }


def _read_file_digest(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    total = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
            total += len(chunk)
    return digest.hexdigest(), total


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _sqlite_error_message(exc: sqlite3.Error) -> str:
    return f"relay queue database error: {exc.__class__.__name__}"


def _read_pause_marker(path: Path) -> tuple[dict[str, Any], bool]:
    if not path.exists():
        return {}, True
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {}, False
    if not isinstance(payload, dict):
        return {}, False
    return payload, True


def read_operator_pause(pause_path: str | os.PathLike[str]) -> dict[str, Any]:
    path_text = str(pause_path or "").strip()
    if not path_text:
        return {"enabled": False, "paused": False, "path": "", "marker_valid": True}
    marker_path = Path(path_text)
    marker, valid = _read_pause_marker(marker_path)
    return {
        "enabled": True,
        "paused": marker_path.exists(),
        "path": str(marker_path),
        "marker_valid": valid,
        "schema_version": str(marker.get("schema_version") or "") if valid else "",
        "operator_id": str(marker.get("operator_id") or "") if valid else "",
        "reason_redacted": str(marker.get("reason_redacted") or "") if valid else "",
        "reason_sha256": str(marker.get("reason_sha256") or "") if valid else "",
        "reason_length": int(marker.get("reason_length") or 0) if valid else 0,
        "created_at": str(marker.get("created_at") or "") if valid else "",
    }


def _append_operator_audit(audit_log_path: str | os.PathLike[str], *, action: str, report: Mapping[str, Any]) -> None:
    if not str(audit_log_path or "").strip():
        return
    entry = {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "audit_event_id": f"operator-audit-{uuid.uuid4().hex}",
        "action": action,
        "tool_version": OPERATOR_TOOL_VERSION,
        "generated_at": utc_now_text(),
    }
    entry.update(dict(report))
    _append_jsonl(audit_log_path, entry)


def pause_relay(*, pause_path: str | os.PathLike[str], operator_id: str, reason: str, audit_log_path: str | os.PathLike[str] = "") -> dict[str, Any]:
    operator = _require_text(operator_id, field_name="operator_id", max_length=128)
    reason_text = _require_text(reason, field_name="reason")
    target = Path(pause_path)
    previous = read_operator_pause(target)
    reason_fields = _reason_evidence(reason_text)
    marker = {
        "schema_version": PAUSE_SCHEMA_VERSION,
        "status": "paused",
        "operator_id": operator,
        **reason_fields,
        "created_at": utc_now_text(),
    }
    _write_json_atomic(target, marker)
    report = {
        "status": "PASS",
        "operation": "pause",
        "operator_id": operator,
        "tool_version": OPERATOR_TOOL_VERSION,
        **reason_fields,
        "pause": read_operator_pause(target),
        "previous_paused": bool(previous.get("paused")),
    }
    _append_operator_audit(audit_log_path, action="pause", report=report)
    return report


def resume_relay(*, pause_path: str | os.PathLike[str], operator_id: str, reason: str, audit_log_path: str | os.PathLike[str] = "") -> dict[str, Any]:
    operator = _require_text(operator_id, field_name="operator_id", max_length=128)
    reason_text = _require_text(reason, field_name="reason")
    target = Path(pause_path)
    previous = read_operator_pause(target)
    reason_fields = _reason_evidence(reason_text)
    if target.exists():
        target.unlink()
    report = {
        "status": "PASS",
        "operation": "resume",
        "operator_id": operator,
        "tool_version": OPERATOR_TOOL_VERSION,
        **reason_fields,
        "pause": read_operator_pause(target),
        "previous_paused": bool(previous.get("paused")),
    }
    _append_operator_audit(audit_log_path, action="resume", report=report)
    return report


def read_relay_queue_status_read_only(db_path: str | os.PathLike[str]) -> dict[str, Any]:
    path = Path(db_path)
    if not path.is_file():
        return {"status": "not_initialized", "counts": {}, "oldest_active_created_at": ""}
    try:
        conn = _connect_relay_db(path, read_only=True)
    except sqlite3.Error as exc:
        return {"status": "blocked", "counts": {}, "oldest_active_created_at": "", "error_code": "relay_db_open_failed", "error_message": _sqlite_error_message(exc)}
    try:
        counts = {
            row["status"]: int(row["count"])
            for row in conn.execute("SELECT status, COUNT(*) AS count FROM direct_sync_relay_batches GROUP BY status").fetchall()
        }
        oldest = conn.execute(
            """
            SELECT created_at
            FROM direct_sync_relay_batches
            WHERE status IN ('pending', 'retry_wait', 'leased')
            ORDER BY created_at
            LIMIT 1
            """
        ).fetchone()
        return {"status": "PASS", "counts": counts, "oldest_active_created_at": oldest["created_at"] if oldest else ""}
    except sqlite3.Error as exc:
        return {"status": "blocked", "counts": {}, "oldest_active_created_at": "", "error_code": "relay_db_schema_unavailable", "error_message": _sqlite_error_message(exc)}
    finally:
        conn.close()


def operator_status(*, db_path: str | os.PathLike[str], pause_path: str | os.PathLike[str] = "") -> dict[str, Any]:
    return {
        "status": "PASS",
        "operation": "status",
        "tool_version": OPERATOR_TOOL_VERSION,
        "queue": read_relay_queue_status_read_only(db_path),
        "pause": read_operator_pause(pause_path),
    }


def restore_relay_spool_from_server(
    *,
    db_path: str | os.PathLike[str],
    relay_id: str,
    spool_root: str | os.PathLike[str],
    credentials: ProducerCredentials,
    operator_id: str,
    reason: str,
    audit_log_path: str | os.PathLike[str] = "",
    session: Any = None,
) -> dict[str, Any]:
    relay = _require_text(relay_id, field_name="relay_id", max_length=128)
    operator = _require_text(operator_id, field_name="operator_id", max_length=128)
    reason_text = _require_text(reason, field_name="reason")
    reason_fields = _reason_evidence(reason_text)

    def blocked(error_code: str, **extra: Any) -> dict[str, Any]:
        report = {
            "status": "BLOCKED",
            "operation": "restore-spool",
            "relay_id": relay,
            "operator_id": operator,
            "tool_version": OPERATOR_TOOL_VERSION,
            **reason_fields,
            "error_code": error_code,
        }
        report.update(extra)
        _append_operator_audit(audit_log_path, action="restore-spool-blocked", report=report)
        return report

    if not Path(db_path).is_file():
        return blocked("relay_db_not_initialized")
    spool_root_path = Path(spool_root).expanduser().resolve()
    conn = _connect_relay_db(db_path)
    try:
        row = conn.execute(
            """
            SELECT relay_id, status, spooled_file_path, content_sha256, byte_length,
                   metadata_json, producer_id, key_id, endpoint_url
            FROM direct_sync_relay_batches
            WHERE relay_id = ?
            """,
            (relay,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return blocked("relay_not_found")
    previous_status = str(row["status"] or "")
    if previous_status != RELAY_STATUS_ACKED:
        return blocked("relay_status_not_restoreable", previous_status=previous_status)
    for field, expected in {
        "producer_id": credentials.producer_id,
        "key_id": credentials.key_id,
        "endpoint_url": credentials.endpoint_url,
    }.items():
        actual = str(row[field] or "")
        if not actual:
            return blocked("relay_credential_binding_missing", missing_field=field)
        if actual != str(expected):
            return blocked("relay_credential_binding_mismatch", mismatch_field=field)
    spool_path_candidate = Path(str(row["spooled_file_path"] or "")).expanduser()
    if spool_path_candidate.is_symlink():
        return blocked("spooled_file_symlink")
    spool_path = spool_path_candidate.resolve()
    if not _is_within(spool_path, spool_root_path):
        return blocked("spooled_file_outside_spool_root", spool_root=str(spool_root_path))
    expected_hash = str(row["content_sha256"] or "")
    expected_bytes = int(row["byte_length"])
    if spool_path.exists():
        if not spool_path.is_file():
            return blocked("spooled_file_not_regular")
        actual_hash, actual_bytes = _read_file_digest(spool_path)
        if actual_hash != expected_hash or actual_bytes != expected_bytes:
            return blocked(
                "spooled_file_already_exists_mismatch",
                content_sha256=expected_hash,
                byte_length=expected_bytes,
                actual_content_sha256=actual_hash,
                actual_byte_length=actual_bytes,
            )
        report = {
            "status": "PASS",
            "operation": "restore-spool",
            "relay_id": relay,
            "operator_id": operator,
            "tool_version": OPERATOR_TOOL_VERSION,
            **reason_fields,
            "previous_status": previous_status,
            "restored": False,
            "local_file_already_present": True,
            "content_sha256": expected_hash,
            "byte_length": expected_bytes,
            "spooled_file_path": str(spool_path),
            "restore_url": build_raw_artifact_restore_url(
                credentials.endpoint_url,
                content_sha256=expected_hash,
                byte_length=expected_bytes,
            ),
        }
        _append_operator_audit(audit_log_path, action="restore-spool", report=report)
        return report
    try:
        metadata = json.loads(str(row["metadata_json"] or "{}"))
    except json.JSONDecodeError:
        return blocked("relay_upload_metadata_invalid")
    if not isinstance(metadata, dict):
        return blocked("relay_upload_metadata_invalid")
    try:
        result = restore_raw_artifact_to_file(
            credentials=credentials,
            metadata=metadata,
            destination_path=spool_path,
            session=session,
        )
    except DirectSyncPushError:
        return blocked("relay_restore_input_invalid")
    if not result.success:
        return blocked(
            result.error_code or "relay_restore_failed",
            restore_retryable=result.retryable,
            restore_status_code=result.status_code,
            error_message=result.error_message,
        )
    report = {
        "status": "PASS",
        "operation": "restore-spool",
        "relay_id": relay,
        "operator_id": operator,
        "tool_version": OPERATOR_TOOL_VERSION,
        **reason_fields,
        "previous_status": previous_status,
        "restored": True,
        "local_file_already_present": False,
        "content_sha256": expected_hash,
        "byte_length": expected_bytes,
        "spooled_file_path": str(spool_path),
        "restore_url": build_raw_artifact_restore_url(
            credentials.endpoint_url,
            content_sha256=expected_hash,
            byte_length=expected_bytes,
        ),
    }
    _append_operator_audit(audit_log_path, action="restore-spool", report=report)
    return report


def retry_dead_relay_batch(
    *,
    db_path: str | os.PathLike[str],
    relay_id: str,
    operator_id: str,
    reason: str,
    audit_log_path: str | os.PathLike[str] = "",
) -> dict[str, Any]:
    relay = _require_text(relay_id, field_name="relay_id", max_length=128)
    operator = _require_text(operator_id, field_name="operator_id", max_length=128)
    reason_text = _require_text(reason, field_name="reason")
    reason_fields = _reason_evidence(reason_text)
    if not Path(db_path).is_file():
        report = {"status": "BLOCKED", "operation": "retry-dead", "relay_id": relay, "operator_id": operator, "tool_version": OPERATOR_TOOL_VERSION, **reason_fields, "error_code": "relay_db_not_initialized"}
        _append_operator_audit(audit_log_path, action="retry-dead-blocked", report=report)
        return report
    now = utc_now_text()
    conn = _connect_relay_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT relay_id, status, attempt_count, spooled_file_path, content_sha256, byte_length
            FROM direct_sync_relay_batches
            WHERE relay_id = ?
            """,
            (relay,),
        ).fetchone()
        if row is None:
            conn.rollback()
            report = {"status": "BLOCKED", "operation": "retry-dead", "relay_id": relay, "operator_id": operator, "tool_version": OPERATOR_TOOL_VERSION, **reason_fields, "error_code": "relay_not_found"}
            _append_operator_audit(audit_log_path, action="retry-dead-blocked", report=report)
            return report
        previous_status = str(row["status"])
        previous_attempt_count = int(row["attempt_count"])
        if previous_status not in RETRYABLE_DEAD_STATUSES:
            conn.rollback()
            report = {"status": "BLOCKED", "operation": "retry-dead", "relay_id": relay, "operator_id": operator, "tool_version": OPERATOR_TOOL_VERSION, **reason_fields, "previous_status": previous_status, "error_code": "relay_status_not_retryable_by_operator"}
            _append_operator_audit(audit_log_path, action="retry-dead-blocked", report=report)
            return report
        spool_path = Path(str(row["spooled_file_path"] or ""))
        if not spool_path.is_file():
            conn.rollback()
            report = {"status": "BLOCKED", "operation": "retry-dead", "relay_id": relay, "operator_id": operator, "tool_version": OPERATOR_TOOL_VERSION, **reason_fields, "previous_status": previous_status, "error_code": "spooled_file_missing"}
            _append_operator_audit(audit_log_path, action="retry-dead-blocked", report=report)
            return report
        actual_hash, actual_bytes = _read_file_digest(spool_path)
        expected_hash = str(row["content_sha256"] or "")
        expected_bytes = int(row["byte_length"])
        if actual_hash != expected_hash or actual_bytes != expected_bytes:
            conn.rollback()
            report = {
                "status": "BLOCKED",
                "operation": "retry-dead",
                "relay_id": relay,
                "operator_id": operator,
                "tool_version": OPERATOR_TOOL_VERSION,
                **reason_fields,
                "previous_status": previous_status,
                "content_sha256": expected_hash,
                "byte_length": expected_bytes,
                "actual_content_sha256": actual_hash,
                "actual_byte_length": actual_bytes,
                "error_code": "spooled_file_digest_mismatch",
            }
            _append_operator_audit(audit_log_path, action="retry-dead-blocked", report=report)
            return report
        cursor = conn.execute(
            """
            UPDATE direct_sync_relay_batches
            SET status = ?,
                lease_owner = NULL,
                lease_expires_at = NULL,
                next_attempt_at = NULL,
                last_error_code = NULL,
                last_error_message = NULL,
                updated_at = ?
            WHERE relay_id = ?
              AND status = ?
              AND lease_owner IS NULL
              AND lease_expires_at IS NULL
            """,
            (RELAY_STATUS_PENDING, now, relay, RELAY_STATUS_FAILED_PERMANENT),
        )
        if cursor.rowcount != 1:
            conn.rollback()
            report = {"status": "BLOCKED", "operation": "retry-dead", "relay_id": relay, "operator_id": operator, "tool_version": OPERATOR_TOOL_VERSION, **reason_fields, "previous_status": previous_status, "error_code": "relay_status_changed"}
            _append_operator_audit(audit_log_path, action="retry-dead-blocked", report=report)
            return report
        conn.commit()
    finally:
        conn.close()
    report = {
        "status": "PASS",
        "operation": "retry-dead",
        "relay_id": relay,
        "operator_id": operator,
        "tool_version": OPERATOR_TOOL_VERSION,
        **reason_fields,
        "previous_status": previous_status,
        "new_status": RELAY_STATUS_PENDING,
        "previous_attempt_count": previous_attempt_count,
        "content_sha256": expected_hash,
        "byte_length": expected_bytes,
        "spool_file_name": spool_path.name,
        "queue": read_relay_queue_status_read_only(db_path),
    }
    _append_operator_audit(audit_log_path, action="retry-dead", report=report)
    return report


def ack_reviewed_relay_batch(
    *,
    db_path: str | os.PathLike[str],
    relay_id: str,
    operator_id: str,
    reason: str,
    review_evidence_ref: str = "",
    expected_content_sha256: str = "",
    expected_request_id: str = "",
    expected_error_code: str = "",
    audit_log_path: str | os.PathLike[str] = "",
) -> dict[str, Any]:
    relay = _require_text(relay_id, field_name="relay_id", max_length=128)
    operator = _require_text(operator_id, field_name="operator_id", max_length=128)
    reason_text = _require_text(reason, field_name="reason")
    reason_fields = _reason_evidence(reason_text)
    expected_hash = str(expected_content_sha256 or "").strip().lower()
    expected_request = str(expected_request_id or "").strip()
    expected_error = str(expected_error_code or "").strip()
    evidence_ref = str(review_evidence_ref or "").strip()

    def blocked(error_code: str, **extra: Any) -> dict[str, Any]:
        report = {
            "status": "BLOCKED",
            "operation": "ack-reviewed",
            "relay_id": relay,
            "operator_id": operator,
            "tool_version": OPERATOR_TOOL_VERSION,
            **reason_fields,
            "review_evidence_ref": evidence_ref,
            "error_code": error_code,
        }
        report.update(extra)
        _append_operator_audit(audit_log_path, action="ack-reviewed-blocked", report=report)
        return report

    if not Path(db_path).is_file():
        return blocked("relay_db_not_initialized")
    if not evidence_ref:
        return blocked("review_evidence_ref_required")
    if not expected_hash:
        return blocked("expected_content_sha256_required")
    if not expected_request:
        return blocked("expected_request_id_required")
    if not expected_error:
        return blocked("expected_error_code_required")
    now = utc_now_text()
    conn = _connect_relay_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT relay_id, status, content_sha256, byte_length, receipt_json,
                   relative_path, metadata_json,
                   upload_status_path, last_error_code, last_error_message,
                   lease_owner, lease_expires_at
            FROM direct_sync_relay_batches
            WHERE relay_id = ?
            """,
            (relay,),
        ).fetchone()
        if row is None:
            conn.rollback()
            return blocked("relay_not_found")
        previous_status = str(row["status"] or "")
        if previous_status != RELAY_STATUS_OPERATOR_REVIEW:
            conn.rollback()
            return blocked("relay_status_not_ackable_by_operator", previous_status=previous_status)
        if row["lease_owner"] is not None or row["lease_expires_at"] is not None:
            conn.rollback()
            return blocked("relay_is_leased", previous_status=previous_status)
        content_sha256 = str(row["content_sha256"] or "").lower()
        if expected_hash and content_sha256 != expected_hash:
            conn.rollback()
            return blocked(
                "relay_content_sha256_mismatch",
                previous_status=previous_status,
                content_sha256=content_sha256,
            )
        previous_error_code = str(row["last_error_code"] or "")
        if expected_error != "operator_review_required":
            conn.rollback()
            return blocked(
                "relay_expected_error_code_not_ackable",
                previous_status=previous_status,
                expected_error_code=expected_error,
            )
        try:
            receipt = json.loads(str(row["receipt_json"] or "{}"))
        except json.JSONDecodeError:
            conn.rollback()
            return blocked("relay_receipt_invalid_json", previous_status=previous_status)
        if not isinstance(receipt, dict):
            conn.rollback()
            return blocked("relay_receipt_not_object", previous_status=previous_status)
        try:
            metadata = json.loads(str(row["metadata_json"] or "{}"))
        except json.JSONDecodeError:
            conn.rollback()
            return blocked("relay_metadata_invalid_json", previous_status=previous_status)
        if not isinstance(metadata, dict):
            conn.rollback()
            return blocked("relay_metadata_not_object", previous_status=previous_status)
        if receipt.get("committed") is not True:
            conn.rollback()
            return blocked("relay_receipt_not_committed", previous_status=previous_status)
        if receipt.get("retryable") is not False:
            conn.rollback()
            return blocked("relay_receipt_retryable", previous_status=previous_status)
        if receipt.get("next_retry_after") is not None:
            conn.rollback()
            return blocked("relay_receipt_has_retry_after", previous_status=previous_status)
        if receipt.get("error") is not None:
            conn.rollback()
            return blocked("relay_receipt_has_error", previous_status=previous_status)
        if str(receipt.get("client_batch_id") or "") != relay:
            conn.rollback()
            return blocked(
                "relay_receipt_client_batch_id_mismatch",
                previous_status=previous_status,
                receipt_client_batch_id=str(receipt.get("client_batch_id") or ""),
            )
        receipt_status = str(receipt.get("status") or "")
        if receipt_status != "accepted":
            conn.rollback()
            return blocked("relay_receipt_status_not_accepted", previous_status=previous_status, receipt_status=receipt_status)
        if expected_request and str(receipt.get("request_id") or "") != expected_request:
            conn.rollback()
            return blocked(
                "relay_request_id_mismatch",
                previous_status=previous_status,
                receipt_request_id=str(receipt.get("request_id") or ""),
            )
        totals = receipt.get("totals") if isinstance(receipt.get("totals"), dict) else {}
        try:
            errors = int(totals.get("errors") or 0)
        except (TypeError, ValueError):
            conn.rollback()
            return blocked("relay_receipt_errors_invalid", previous_status=previous_status)
        if errors:
            conn.rollback()
            return blocked("relay_receipt_has_errors", previous_status=previous_status, receipt_errors=errors)
        try:
            inserted = int(totals.get("inserted") or 0)
            replayed = int(totals.get("replayed") or 0)
            quarantined = int(totals.get("quarantined") or 0)
        except (TypeError, ValueError):
            conn.rollback()
            return blocked("relay_receipt_totals_invalid", previous_status=previous_status)
        row_count = metadata.get("row_count")
        if type(row_count) is not int:
            conn.rollback()
            return blocked("relay_metadata_row_count_invalid", previous_status=previous_status)
        if inserted + replayed + quarantined + errors != row_count:
            conn.rollback()
            return blocked(
                "relay_receipt_totals_do_not_match_row_count",
                previous_status=previous_status,
                receipt_total_rows=inserted + replayed + quarantined + errors,
                metadata_row_count=row_count,
            )
        source_file = receipt.get("source_file") if isinstance(receipt.get("source_file"), dict) else {}
        if str(source_file.get("content_sha256") or "").lower() != content_sha256:
            conn.rollback()
            return blocked("relay_receipt_source_hash_mismatch", previous_status=previous_status)
        try:
            receipt_byte_length = int(source_file.get("byte_length") or -1)
            receipt_row_count = int(source_file.get("declared_row_count") or -1)
        except (TypeError, ValueError):
            conn.rollback()
            return blocked("relay_receipt_source_shape_invalid", previous_status=previous_status)
        if receipt_byte_length != int(row["byte_length"]):
            conn.rollback()
            return blocked("relay_receipt_source_byte_length_mismatch", previous_status=previous_status)
        if receipt_row_count != row_count:
            conn.rollback()
            return blocked("relay_receipt_source_row_count_mismatch", previous_status=previous_status)
        expected_source_file_id = "/".join(
            str(metadata.get(field) or "").strip("/")
            for field in ("source_host_id", "source_system", "stream_name", "relative_path")
        )
        if str(receipt.get("server_source_file_id") or "") != expected_source_file_id:
            conn.rollback()
            return blocked(
                "relay_receipt_server_source_file_id_mismatch",
                previous_status=previous_status,
                receipt_server_source_file_id=str(receipt.get("server_source_file_id") or ""),
                expected_server_source_file_id=expected_source_file_id,
            )
        cursor = conn.execute(
            """
            UPDATE direct_sync_relay_batches
            SET status = ?,
                lease_owner = NULL,
                lease_expires_at = NULL,
                next_attempt_at = NULL,
                last_error_code = NULL,
                last_error_message = NULL,
                updated_at = ?
            WHERE relay_id = ?
              AND status = ?
              AND lease_owner IS NULL
              AND lease_expires_at IS NULL
            """,
            (RELAY_STATUS_ACKED, now, relay, RELAY_STATUS_OPERATOR_REVIEW),
        )
        if cursor.rowcount != 1:
            conn.rollback()
            return blocked("relay_status_changed", previous_status=previous_status)
        conn.commit()
    finally:
        conn.close()

    report = {
        "status": "PASS",
        "operation": "ack-reviewed",
        "relay_id": relay,
        "operator_id": operator,
        "tool_version": OPERATOR_TOOL_VERSION,
        **reason_fields,
        "review_evidence_ref": evidence_ref,
        "previous_status": previous_status,
        "new_status": RELAY_STATUS_ACKED,
        "previous_error_code": previous_error_code,
        "content_sha256": content_sha256,
        "byte_length": int(row["byte_length"]),
        "receipt_request_id": str(receipt.get("request_id") or ""),
        "receipt_totals": totals,
        "upload_status_path": str(row["upload_status_path"] or ""),
        "queue": read_relay_queue_status_read_only(db_path),
    }
    _append_operator_audit(audit_log_path, action="ack-reviewed", report=report)
    return report

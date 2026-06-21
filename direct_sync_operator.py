"""Local operator controls for the Label_Match direct-sync relay."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import uuid
from pathlib import Path
from typing import Any, Mapping

from direct_sync_push import RELAY_STATUS_FAILED_PERMANENT, RELAY_STATUS_PENDING, utc_now_text


PAUSE_SCHEMA_VERSION = "direct-sync-relay-operator-pause-v1"
AUDIT_SCHEMA_VERSION = "direct-sync-relay-operator-audit-v1"
OPERATOR_TOOL_VERSION = "label-match-local-operator-v1"
RETRYABLE_DEAD_STATUSES = frozenset({RELAY_STATUS_FAILED_PERMANENT})


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
    uri = f"file:{path.resolve().as_posix()}?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True)
        conn.row_factory = sqlite3.Row
    except sqlite3.Error as exc:
        return {"status": "blocked", "counts": {}, "oldest_active_created_at": "", "error_code": "relay_db_open_failed", "error_message": str(exc)}
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
        return {"status": "blocked", "counts": {}, "oldest_active_created_at": "", "error_code": "relay_db_schema_unavailable", "error_message": str(exc)}
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
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
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

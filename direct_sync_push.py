# -*- coding: utf-8 -*-
"""Direct HTTP source-file uploader for Label_Match CSV logs."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import sqlite3
import unicodedata
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Mapping
from urllib.parse import urlparse


CONTRACT_VERSION = "producer-ingest-source-file-v1"
SIGNATURE_VERSION = "PRODUCER-HMAC-SHA256-V1"
DEFAULT_ENDPOINT_PATH = "/api/producer-ingest/v1/source-file"
DEFAULT_STREAM_NAME = "label_match_events"
DEFAULT_SOURCE_SYSTEM = "label_match"
DEFAULT_SOURCE_TRANSPORT = "legacy_packaging_csv"
DEFAULT_TIMEOUT_SECONDS = 30
RELAY_STATUS_PENDING = "pending"
RELAY_STATUS_LEASED = "leased"
RELAY_STATUS_RETRY_WAIT = "retry_wait"
RELAY_STATUS_ACKED = "acked"
RELAY_STATUS_FAILED_PERMANENT = "failed_permanent"
RELAY_STATUS_OPERATOR_REVIEW = "operator_review"
DEFAULT_LEASE_SECONDS = 300
DEFAULT_RETRY_SECONDS = 60


class DirectSyncPushError(Exception):
    pass


@dataclass(frozen=True)
class ProducerCredentials:
    producer_id: str
    key_id: str
    secret: str | bytes
    endpoint_url: str


@dataclass(frozen=True)
class SourceFilePlan:
    source_file_path: str
    metadata: Dict[str, Any]
    content_sha256: str
    byte_length: int


@dataclass(frozen=True)
class UploadResult:
    success: bool
    status_code: int
    committed: bool
    retryable: bool
    receipt: Dict[str, Any]
    status_path: str = ""
    error_code: str = ""
    error_message: str = ""


@dataclass(frozen=True)
class RelayQueueRow:
    relay_id: str
    status: str
    spooled_file_path: str
    producer_manifest_path: str
    relative_path: str
    content_sha256: str
    byte_length: int
    attempt_count: int


def _normalize_for_json(value: Any) -> Any:
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value)
    if isinstance(value, list):
        return [_normalize_for_json(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _normalize_for_json(value[key]) for key in sorted(value)}
    return value


def canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        _normalize_for_json(dict(value)),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def manifest_hash(manifest: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(manifest).encode("utf-8")).hexdigest()


def canonical_content_type(content_type: str) -> str:
    return str(content_type or "").split(";", 1)[0].strip().lower()


def canonical_request_string(
    *,
    method: str,
    path: str,
    query_string: str,
    timestamp: str,
    nonce: str,
    producer_id: str,
    key_id: str,
    metadata: Mapping[str, Any],
    content_sha256: str,
    byte_length: int,
    content_type: str,
) -> str:
    metadata_hash = hashlib.sha256(canonical_json(metadata).encode("utf-8")).hexdigest()
    return "\n".join(
        [
            SIGNATURE_VERSION,
            method.upper(),
            path,
            query_string,
            timestamp,
            nonce,
            producer_id,
            key_id,
            metadata_hash,
            str(content_sha256).lower(),
            str(int(byte_length)),
            canonical_content_type(content_type),
        ]
    )


def sign_canonical_request(secret: str | bytes, canonical_request: str) -> str:
    secret_bytes = secret.encode("utf-8") if isinstance(secret, str) else secret
    return hmac.new(secret_bytes, canonical_request.encode("utf-8"), hashlib.sha256).hexdigest()


def utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _load_manifest(path: str | os.PathLike[str]) -> Dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise DirectSyncPushError("producer manifest must be a JSON object")
    return payload


def _stream_from_manifest(manifest: Mapping[str, Any], stream_name: str) -> Mapping[str, Any]:
    for stream in manifest.get("streams") or []:
        if stream.get("stream_name") == stream_name:
            return stream
    raise DirectSyncPushError(f"producer manifest does not include stream: {stream_name}")


def _safe_relative_path(value: str) -> str:
    text = str(value or "").replace("\\", "/").strip("/")
    parts = text.split("/")
    if not text or any(part in {"", ".", ".."} for part in parts):
        raise DirectSyncPushError("relative_path must be safe and relative")
    if any(part.startswith((".", "~")) or ":" in part for part in parts):
        raise DirectSyncPushError("relative_path contains an unsafe segment")
    return text


def _read_file_digest(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    byte_count = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
            byte_count += len(chunk)
    return digest.hexdigest(), byte_count


def count_csv_data_rows(path: str | os.PathLike[str]) -> int:
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        return max(0, sum(1 for line in handle if line.strip()) - 1)


def build_source_file_plan(
    *,
    source_file_path: str | os.PathLike[str],
    producer_manifest_path: str | os.PathLike[str],
    credentials: ProducerCredentials,
    relative_path: str = "",
    client_batch_id: str = "",
    idempotency_key: str = "",
) -> SourceFilePlan:
    file_path = Path(source_file_path)
    if not file_path.is_file():
        raise DirectSyncPushError(f"source file does not exist: {file_path}")
    manifest = _load_manifest(producer_manifest_path)
    identity = manifest.get("pc_identity") or {}
    producer_install_id = str(identity.get("producer_install_id") or "").strip()
    source_host_id = str(identity.get("source_host_id") or "").strip()
    if not producer_install_id or not source_host_id:
        raise DirectSyncPushError("producer manifest identity is incomplete")
    stream = _stream_from_manifest(manifest, DEFAULT_STREAM_NAME)
    if stream.get("source_system") != DEFAULT_SOURCE_SYSTEM or stream.get("source_transport") != DEFAULT_SOURCE_TRANSPORT:
        raise DirectSyncPushError("producer manifest stream does not match Label_Match legacy CSV")
    safe_relative_path = _safe_relative_path(relative_path or f"legacy_csv/{file_path.name}")
    if safe_relative_path.split("/", 1)[0] == DEFAULT_STREAM_NAME:
        raise DirectSyncPushError("relative_path must not include stream_name")
    content_sha256, byte_length = _read_file_digest(file_path)
    source_file_id = f"{source_host_id}/{DEFAULT_STREAM_NAME}/{safe_relative_path}"
    stable_key = f"source-file:{source_file_id}"
    row_count = count_csv_data_rows(file_path)
    metadata = {
        "contract_version": CONTRACT_VERSION,
        "producer_install_id": producer_install_id,
        "client_batch_id": client_batch_id or stable_key,
        "idempotency_key": idempotency_key or stable_key,
        "source_host_id": source_host_id,
        "manifest_hash": manifest_hash(manifest),
        "stream_name": DEFAULT_STREAM_NAME,
        "source_system": DEFAULT_SOURCE_SYSTEM,
        "source_transport": DEFAULT_SOURCE_TRANSPORT,
        "relative_path": safe_relative_path,
        "batch_kind": "whole_file",
        "row_count": row_count,
        "first_row_number": 2 if row_count else 0,
        "last_row_number": row_count + 1 if row_count else 0,
        "content_sha256": content_sha256,
        "byte_length": byte_length,
    }
    return SourceFilePlan(
        source_file_path=str(file_path),
        metadata=metadata,
        content_sha256=content_sha256,
        byte_length=byte_length,
    )


def signed_headers(
    credentials: ProducerCredentials,
    metadata: Mapping[str, Any],
    *,
    timestamp: str = "",
    nonce: str = "",
) -> Dict[str, str]:
    parsed = urlparse(credentials.endpoint_url)
    timestamp = timestamp or utc_now_text()
    nonce = nonce or uuid.uuid4().hex
    canonical = canonical_request_string(
        method="POST",
        path=parsed.path or DEFAULT_ENDPOINT_PATH,
        query_string=parsed.query or "",
        timestamp=timestamp,
        nonce=nonce,
        producer_id=credentials.producer_id,
        key_id=credentials.key_id,
        metadata=metadata,
        content_sha256=metadata["content_sha256"],
        byte_length=int(metadata["byte_length"]),
        content_type="multipart/form-data",
    )
    return {
        "X-Producer-Id": credentials.producer_id,
        "X-Producer-Key-Id": credentials.key_id,
        "X-Producer-Timestamp": timestamp,
        "X-Producer-Nonce": nonce,
        "X-Producer-Signature": sign_canonical_request(credentials.secret, canonical),
    }


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp_path, path)


def _response_json(response: Any) -> Dict[str, Any]:
    try:
        payload = response.json()
    except Exception:
        payload = {}
    return payload if isinstance(payload, dict) else {}


def upload_source_file(
    plan: SourceFilePlan,
    credentials: ProducerCredentials,
    *,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    status_dir: str | os.PathLike[str] = "",
) -> UploadResult:
    if session is None:
        import requests

        session = requests.Session()
    headers = signed_headers(credentials, plan.metadata)
    with Path(plan.source_file_path).open("rb") as handle:
        response = session.post(
            credentials.endpoint_url,
            data={"metadata": canonical_json(plan.metadata)},
            files={"file": (Path(plan.source_file_path).name, handle, "application/octet-stream")},
            headers=headers,
            timeout=timeout,
        )
    payload = _response_json(response)
    status_code = int(getattr(response, "status_code", 0) or 0)
    totals = payload.get("totals") if isinstance(payload.get("totals"), dict) else {}
    committed = bool(payload.get("committed")) and 200 <= status_code < 300
    success = committed and int(totals.get("errors") or 0) == 0 and int(totals.get("quarantined") or 0) == 0
    error = payload.get("error") if isinstance(payload.get("error"), dict) else {}
    result = UploadResult(
        success=success,
        status_code=status_code,
        committed=committed,
        retryable=bool(payload.get("retryable")) or status_code in {408, 429, 500, 502, 503, 504},
        receipt=payload,
        error_code=str(error.get("code") or ""),
        error_message=str(error.get("message") or ""),
    )
    if status_dir:
        suffix = hashlib.sha256(plan.metadata["idempotency_key"].encode("utf-8")).hexdigest()[:12]
        status_path = Path(status_dir) / f"direct_sync_upload_status_{suffix}.json"
        _write_json_atomic(
            status_path,
            {
                "success": result.success,
                "status_code": result.status_code,
                "committed": result.committed,
                "retryable": result.retryable,
                "receipt": result.receipt,
                "error_code": result.error_code,
                "error_message": result.error_message,
                "metadata": dict(plan.metadata),
                "source_file_path": plan.source_file_path,
                "generated_at": utc_now_text(),
            },
        )
        return UploadResult(
            success=result.success,
            status_code=result.status_code,
            committed=result.committed,
            retryable=result.retryable,
            receipt=result.receipt,
            status_path=str(status_path),
            error_code=result.error_code,
            error_message=result.error_message,
        )
    return result


def _connect_relay_db(db_path: str | os.PathLike[str]) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_relay_queue_schema(db_path: str | os.PathLike[str]) -> None:
    conn = _connect_relay_db(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS direct_sync_relay_batches (
                relay_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                source_file_path TEXT NOT NULL,
                spooled_file_path TEXT NOT NULL,
                producer_manifest_path TEXT NOT NULL,
                relative_path TEXT NOT NULL,
                content_sha256 TEXT NOT NULL,
                byte_length INTEGER NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                lease_owner TEXT,
                lease_expires_at TEXT,
                next_attempt_at TEXT,
                last_error_code TEXT,
                last_error_message TEXT,
                receipt_json TEXT,
                upload_status_path TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_direct_sync_relay_status_due
            ON direct_sync_relay_batches(status, next_attempt_at, created_at)
            """
        )
        conn.commit()
    finally:
        conn.close()


def _relay_row(row: sqlite3.Row) -> RelayQueueRow:
    return RelayQueueRow(
        relay_id=str(row["relay_id"]),
        status=str(row["status"]),
        spooled_file_path=str(row["spooled_file_path"]),
        producer_manifest_path=str(row["producer_manifest_path"]),
        relative_path=str(row["relative_path"]),
        content_sha256=str(row["content_sha256"]),
        byte_length=int(row["byte_length"]),
        attempt_count=int(row["attempt_count"]),
    )


def _copy_file_atomic(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination.with_suffix(destination.suffix + ".tmp")
    with source.open("rb") as src, temp_path.open("wb") as dst:
        for chunk in iter(lambda: src.read(1024 * 1024), b""):
            dst.write(chunk)
        dst.flush()
        os.fsync(dst.fileno())
    os.replace(temp_path, destination)


def enqueue_source_file_for_relay(
    *,
    db_path: str | os.PathLike[str],
    spool_dir: str | os.PathLike[str],
    source_file_path: str | os.PathLike[str],
    producer_manifest_path: str | os.PathLike[str],
    credentials: ProducerCredentials,
    relative_path: str = "",
) -> RelayQueueRow:
    init_relay_queue_schema(db_path)
    source_path = Path(source_file_path)
    if not source_path.is_file():
        raise DirectSyncPushError(f"source file does not exist: {source_path}")
    plan = build_source_file_plan(
        source_file_path=source_path,
        producer_manifest_path=producer_manifest_path,
        credentials=credentials,
        relative_path=relative_path,
    )
    relay_id = f"relay-{uuid.uuid4().hex}"
    spool_path = Path(spool_dir) / f"{relay_id}{source_path.suffix or '.bin'}"
    _copy_file_atomic(source_path, spool_path)
    spooled_hash, spooled_bytes = _read_file_digest(spool_path)
    if spooled_hash != plan.content_sha256 or spooled_bytes != plan.byte_length:
        raise DirectSyncPushError("spooled file hash or byte length mismatch")
    now = utc_now_text()
    conn = _connect_relay_db(db_path)
    try:
        conn.execute(
            """
            INSERT INTO direct_sync_relay_batches (
                relay_id, status, source_file_path, spooled_file_path,
                producer_manifest_path, relative_path, content_sha256,
                byte_length, attempt_count, next_attempt_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
            """,
            (
                relay_id,
                RELAY_STATUS_PENDING,
                str(source_path),
                str(spool_path),
                str(producer_manifest_path),
                plan.metadata["relative_path"],
                plan.content_sha256,
                plan.byte_length,
                now,
                now,
                now,
            ),
        )
        conn.commit()
        row = conn.execute(
            "SELECT * FROM direct_sync_relay_batches WHERE relay_id = ?",
            (relay_id,),
        ).fetchone()
        return _relay_row(row)
    finally:
        conn.close()


def reset_stale_relay_leases(
    *,
    db_path: str | os.PathLike[str],
    now: str = "",
) -> int:
    init_relay_queue_schema(db_path)
    now = now or utc_now_text()
    conn = _connect_relay_db(db_path)
    try:
        cursor = conn.execute(
            """
            UPDATE direct_sync_relay_batches
            SET status = ?,
                lease_owner = NULL,
                lease_expires_at = NULL,
                updated_at = ?
            WHERE status = ?
              AND lease_expires_at IS NOT NULL
              AND lease_expires_at <= ?
            """,
            (RELAY_STATUS_PENDING, now, RELAY_STATUS_LEASED, now),
        )
        conn.commit()
        return int(cursor.rowcount)
    finally:
        conn.close()


def claim_next_relay_batch(
    *,
    db_path: str | os.PathLike[str],
    worker_id: str,
    lease_seconds: int = DEFAULT_LEASE_SECONDS,
    now: str = "",
) -> RelayQueueRow | None:
    init_relay_queue_schema(db_path)
    now = now or utc_now_text()
    reset_stale_relay_leases(db_path=db_path, now=now)
    lease_expires_at = (
        datetime.fromisoformat(now.replace("Z", "+00:00")) + timedelta(seconds=lease_seconds)
    ).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    conn = _connect_relay_db(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT *
            FROM direct_sync_relay_batches
            WHERE status IN (?, ?)
              AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
            ORDER BY created_at, relay_id
            LIMIT 1
            """,
            (RELAY_STATUS_PENDING, RELAY_STATUS_RETRY_WAIT, now),
        ).fetchone()
        if row is None:
            conn.rollback()
            return None
        conn.execute(
            """
            UPDATE direct_sync_relay_batches
            SET status = ?,
                attempt_count = attempt_count + 1,
                lease_owner = ?,
                lease_expires_at = ?,
                updated_at = ?
            WHERE relay_id = ?
              AND status IN (?, ?)
            """,
            (
                RELAY_STATUS_LEASED,
                worker_id,
                lease_expires_at,
                now,
                row["relay_id"],
                RELAY_STATUS_PENDING,
                RELAY_STATUS_RETRY_WAIT,
            ),
        )
        conn.commit()
        claimed = conn.execute(
            "SELECT * FROM direct_sync_relay_batches WHERE relay_id = ?",
            (row["relay_id"],),
        ).fetchone()
        return _relay_row(claimed)
    finally:
        conn.close()


def _set_relay_status(
    *,
    db_path: str | os.PathLike[str],
    relay_id: str,
    status: str,
    receipt: Mapping[str, Any] | None = None,
    upload_status_path: str = "",
    next_attempt_at: str = "",
    error_code: str = "",
    error_message: str = "",
) -> None:
    now = utc_now_text()
    conn = _connect_relay_db(db_path)
    try:
        conn.execute(
            """
            UPDATE direct_sync_relay_batches
            SET status = ?,
                lease_owner = NULL,
                lease_expires_at = NULL,
                next_attempt_at = ?,
                last_error_code = ?,
                last_error_message = ?,
                receipt_json = ?,
                upload_status_path = ?,
                updated_at = ?
            WHERE relay_id = ?
            """,
            (
                status,
                next_attempt_at or None,
                error_code,
                error_message,
                json.dumps(dict(receipt or {}), ensure_ascii=False, sort_keys=True) if receipt is not None else None,
                upload_status_path,
                now,
                relay_id,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _retry_after_seconds(attempt_count: int, base_seconds: int) -> int:
    multiplier = min(max(1, attempt_count), 5)
    return max(1, int(base_seconds)) * multiplier


def drain_one_relay_batch(
    *,
    db_path: str | os.PathLike[str],
    credentials: ProducerCredentials,
    worker_id: str = "direct-sync-relay",
    session: Any = None,
    status_dir: str | os.PathLike[str] = "",
    retry_base_seconds: int = DEFAULT_RETRY_SECONDS,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
) -> UploadResult | None:
    row = claim_next_relay_batch(db_path=db_path, worker_id=worker_id)
    if row is None:
        return None
    plan = build_source_file_plan(
        source_file_path=row.spooled_file_path,
        producer_manifest_path=row.producer_manifest_path,
        credentials=credentials,
        relative_path=row.relative_path,
        client_batch_id=row.relay_id,
    )
    result = upload_source_file(
        plan,
        credentials,
        session=session,
        timeout=timeout,
        status_dir=status_dir,
    )
    if result.success:
        _set_relay_status(
            db_path=db_path,
            relay_id=row.relay_id,
            status=RELAY_STATUS_ACKED,
            receipt=result.receipt,
            upload_status_path=result.status_path,
        )
    elif result.committed:
        _set_relay_status(
            db_path=db_path,
            relay_id=row.relay_id,
            status=RELAY_STATUS_OPERATOR_REVIEW,
            receipt=result.receipt,
            upload_status_path=result.status_path,
            error_code=result.error_code,
            error_message=result.error_message,
        )
    elif result.retryable:
        retry_after = _retry_after_seconds(row.attempt_count, retry_base_seconds)
        next_attempt_at = (
            datetime.now(timezone.utc) + timedelta(seconds=retry_after)
        ).isoformat().replace("+00:00", "Z")
        _set_relay_status(
            db_path=db_path,
            relay_id=row.relay_id,
            status=RELAY_STATUS_RETRY_WAIT,
            receipt=result.receipt,
            upload_status_path=result.status_path,
            next_attempt_at=next_attempt_at,
            error_code=result.error_code,
            error_message=result.error_message,
        )
    else:
        _set_relay_status(
            db_path=db_path,
            relay_id=row.relay_id,
            status=RELAY_STATUS_FAILED_PERMANENT,
            receipt=result.receipt,
            upload_status_path=result.status_path,
            error_code=result.error_code,
            error_message=result.error_message,
        )
    return result


def relay_queue_status(db_path: str | os.PathLike[str]) -> Dict[str, Any]:
    init_relay_queue_schema(db_path)
    conn = _connect_relay_db(db_path)
    try:
        counts = {
            row["status"]: int(row["count"])
            for row in conn.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM direct_sync_relay_batches
                GROUP BY status
                """
            ).fetchall()
        }
        oldest = conn.execute(
            """
            SELECT created_at
            FROM direct_sync_relay_batches
            WHERE status IN (?, ?, ?)
            ORDER BY created_at
            LIMIT 1
            """,
            (RELAY_STATUS_PENDING, RELAY_STATUS_RETRY_WAIT, RELAY_STATUS_LEASED),
        ).fetchone()
        return {
            "counts": counts,
            "oldest_active_created_at": oldest["created_at"] if oldest else "",
        }
    finally:
        conn.close()

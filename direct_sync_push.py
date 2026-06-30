# -*- coding: utf-8 -*-
"""Direct HTTP source-file uploader for Label_Match CSV logs."""

from __future__ import annotations

import hashlib
import hmac
import ipaddress
import json
import math
import os
import re
import sqlite3
import time
import unicodedata
import uuid
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping
from urllib.parse import urlparse


CONTRACT_VERSION = "producer-ingest-source-file-v1"
SIGNATURE_VERSION = "PRODUCER-HMAC-SHA256-V1"
DEFAULT_ENDPOINT_PATH = "/api/producer-ingest/v1/source-file"
DEFAULT_STREAM_NAME = "label_match_events"
DEFAULT_SOURCE_SYSTEM = "label_match"
DEFAULT_SOURCE_TRANSPORT = "legacy_packaging_csv"
DEFAULT_PRODUCER_ROLE = "label_match"
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_PRODUCER_USER_AGENT = "KMTech-LabelMatch-DirectSync/producer-ingest-source-file-v1"
RELAY_STATUS_PENDING = "pending"
RELAY_STATUS_LEASED = "leased"
RELAY_STATUS_RETRY_WAIT = "retry_wait"
RELAY_STATUS_ACKED = "acked"
RELAY_STATUS_FAILED_PERMANENT = "failed_permanent"
RELAY_STATUS_OPERATOR_REVIEW = "operator_review"
DEFAULT_LEASE_SECONDS = 300
DEFAULT_RETRY_SECONDS = 60
MAX_RETRY_AFTER_SECONDS = 24 * 60 * 60
SQLITE_BUSY_TIMEOUT_MS = 30000
RELAY_METADATA_IDENTITY_FIELDS = (
    "producer_install_id",
    "source_host_id",
    "producer_role",
    "stream_name",
    "source_system",
    "source_transport",
    "relative_path",
)
AUTHORIZATION_HEADER_RE = re.compile(r"(?i)authorization\s*:\s*[^\r\n\t ]+(?:[ \t]+[^\r\n\t ]+)?")
CONTROL_TEXT_RE = re.compile(r"[\x00-\x1f\x7f]+")


class DirectSyncPushError(Exception):
    pass


class RelaySpoolFileError(DirectSyncPushError):
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
    retry_after_seconds: int | None = None
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
    metadata: Dict[str, Any]
    producer_id: str = ""
    key_id: str = ""
    endpoint_url: str = ""
    lease_owner: str = ""
    deduped_existing: bool = False
    metadata_error: str = ""


@dataclass(frozen=True)
class AckedRelayRetentionCandidate:
    relay_id: str
    spooled_file_path: str
    upload_status_path: str
    producer_manifest_path: str
    relative_path: str
    content_sha256: str
    byte_length: int
    metadata: Dict[str, Any]
    receipt: Dict[str, Any]


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
    source_file_id = f"{source_host_id}/{DEFAULT_PRODUCER_ROLE}/{DEFAULT_STREAM_NAME}/{safe_relative_path}"
    stable_key = f"source-file:{source_file_id}"
    row_count = count_csv_data_rows(file_path)
    metadata = {
        "contract_version": CONTRACT_VERSION,
        "producer_install_id": producer_install_id,
        "client_batch_id": client_batch_id or stable_key,
        "idempotency_key": idempotency_key or stable_key,
        "source_host_id": source_host_id,
        "producer_role": DEFAULT_PRODUCER_ROLE,
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
    validate_endpoint_url(credentials.endpoint_url)
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
        "User-Agent": DEFAULT_PRODUCER_USER_AGENT,
        "X-Producer-Id": credentials.producer_id,
        "X-Producer-Key-Id": credentials.key_id,
        "X-Producer-Timestamp": timestamp,
        "X-Producer-Nonce": nonce,
        "X-Producer-Signature": sign_canonical_request(credentials.secret, canonical),
    }


def validate_endpoint_url(endpoint_url: str) -> None:
    parsed = urlparse(str(endpoint_url or "").strip())
    if parsed.scheme.lower() != "https":
        raise DirectSyncPushError("endpoint_url must use https")
    if not parsed.netloc or not parsed.hostname:
        raise DirectSyncPushError("endpoint_url must include a hostname")
    if parsed.username or parsed.password:
        raise DirectSyncPushError("endpoint_url must not include username or password")
    if parsed.path != DEFAULT_ENDPOINT_PATH:
        raise DirectSyncPushError(f"endpoint_url path must be {DEFAULT_ENDPOINT_PATH}")
    if parsed.query or parsed.fragment:
        raise DirectSyncPushError("endpoint_url must not include query or fragment")
    host = parsed.hostname.strip().lower()
    if host == "localhost" or host.endswith(".localhost"):
        raise DirectSyncPushError("endpoint_url must not target localhost")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return
    if (
        address.is_loopback
        or address.is_unspecified
        or address.is_private
        or address.is_link_local
        or address.is_multicast
        or address.is_reserved
    ):
        raise DirectSyncPushError("endpoint_url must not target non-public literal IP addresses")


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp.{os.getpid()}.{uuid.uuid4().hex}")
    try:
        with temp_path.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    except Exception:
        try:
            temp_path.unlink()
        except OSError:
            pass
        raise


def _response_json(response: Any) -> Dict[str, Any]:
    try:
        payload = response.json()
    except Exception:
        payload = {}
    return payload if isinstance(payload, dict) else {}


def _redact_remote_error_message(
    value: Any,
    *,
    credentials: ProducerCredentials,
    headers: Mapping[str, str],
    limit: int = 500,
) -> str:
    text = str(value or "")
    for sensitive in (
        credentials.secret,
        headers.get("X-Producer-Signature", ""),
        SIGNATURE_VERSION,
        "X-Producer-Signature",
    ):
        if sensitive:
            text = text.replace(str(sensitive), "[redacted]")
    text = AUTHORIZATION_HEADER_RE.sub("[redacted]", text)
    text = CONTROL_TEXT_RE.sub(" ", text)
    return text.strip()[:limit]


def _redact_remote_error_payload(
    payload: Mapping[str, Any],
    *,
    credentials: ProducerCredentials,
    headers: Mapping[str, str],
) -> Dict[str, Any]:
    def redact_value(value: Any) -> Any:
        if isinstance(value, str):
            return _redact_remote_error_message(value, credentials=credentials, headers=headers)
        if isinstance(value, list):
            return [redact_value(item) for item in value]
        if isinstance(value, dict):
            return {
                _redact_remote_error_message(key, credentials=credentials, headers=headers): redact_value(item)
                for key, item in value.items()
            }
        return value

    return redact_value(dict(payload))


def _transport_error_result(exc: Exception) -> UploadResult:
    return UploadResult(
        success=False,
        status_code=0,
        committed=False,
        retryable=True,
        receipt={},
        error_code="transport_error",
        error_message=f"producer ingest transport error: {type(exc).__name__}",
    )


def _response_header(response: Any, name: str) -> str:
    headers = getattr(response, "headers", {}) or {}
    try:
        return str(headers.get(name) or headers.get(name.lower()) or "").strip()
    except AttributeError:
        return ""


def _retry_after_header_seconds(value: str, *, now: datetime | None = None) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        seconds = int(text)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(text)
        except (TypeError, ValueError, IndexError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        comparison_now = now or datetime.now(timezone.utc)
        if comparison_now.tzinfo is None:
            comparison_now = comparison_now.replace(tzinfo=timezone.utc)
        seconds = max(0, math.ceil((retry_at - comparison_now.astimezone(retry_at.tzinfo)).total_seconds()))
        return min(seconds, MAX_RETRY_AFTER_SECONDS)
    return min(max(0, seconds), MAX_RETRY_AFTER_SECONDS)


def _server_source_file_id_from_metadata(metadata: Mapping[str, Any]) -> str:
    return (
        f"{metadata['source_host_id']}/{metadata['producer_role']}/"
        f"{metadata['stream_name']}/{metadata['relative_path']}"
    )


def _receipt_identity_error(plan: SourceFilePlan, receipt: Mapping[str, Any]) -> tuple[str, str]:
    if str(receipt.get("client_batch_id") or "") != str(plan.metadata["client_batch_id"]):
        return (
            "receipt_identity_mismatch",
            "accepted receipt client_batch_id does not match the uploaded source-file plan",
        )
    if str(receipt.get("server_source_file_id") or "") != _server_source_file_id_from_metadata(plan.metadata):
        return (
            "receipt_identity_mismatch",
            "accepted receipt server_source_file_id does not match the uploaded source-file plan",
        )
    return "", ""


def _receipt_accepted_shape_error(receipt: Mapping[str, Any]) -> tuple[str, str]:
    if str(receipt.get("status") or "") != "accepted":
        return "producer_receipt_invalid", "accepted receipt status must be accepted"
    if receipt.get("retryable") is not False:
        return "producer_receipt_invalid", "accepted receipt retryable must be false"
    if receipt.get("next_retry_after") is not None:
        return "producer_receipt_invalid", "accepted receipt next_retry_after must be null"
    if receipt.get("error") is not None:
        return "producer_receipt_invalid", "accepted receipt must not include error details"
    return "", ""


def _receipt_totals_error(plan: SourceFilePlan, receipt: Mapping[str, Any]) -> tuple[str, str]:
    totals = receipt.get("totals")
    if not isinstance(totals, dict):
        return "producer_receipt_invalid", "accepted receipt totals must be an object"
    total_count = 0
    for key in ("inserted", "replayed", "quarantined", "errors"):
        value = totals.get(key)
        if type(value) is not int or value < 0:
            return "producer_receipt_invalid", f"accepted receipt totals.{key} must be a non-negative integer"
        total_count += value
    if total_count != plan.metadata["row_count"]:
        return "producer_receipt_invalid", "accepted receipt totals must account for every uploaded CSV row"
    return "", ""


def _upload_response_result(
    plan: SourceFilePlan,
    response: Any,
    *,
    credentials: ProducerCredentials,
    headers: Mapping[str, str],
) -> UploadResult:
    payload = _response_json(response)
    safe_payload = _redact_remote_error_payload(payload, credentials=credentials, headers=headers)
    status_code = int(getattr(response, "status_code", 0) or 0)
    retry_after_seconds = _retry_after_header_seconds(_response_header(response, "Retry-After"))
    committed_value = payload.get("committed")
    if 300 <= status_code < 400:
        return UploadResult(
            success=False,
            status_code=status_code,
            committed=False,
            retryable=False,
            receipt=safe_payload,
            error_code="producer_redirect_not_allowed",
            error_message="producer ingest redirected; redirects are not allowed",
        )
    if 200 <= status_code < 300 and committed_value is not True:
        return UploadResult(
            success=False,
            status_code=status_code,
            committed=True,
            retryable=False,
            receipt=safe_payload,
            error_code="producer_receipt_invalid",
            error_message="producer ingest response receipt is missing or invalid",
        )
    if committed_value is True and not (200 <= status_code < 300):
        return UploadResult(
            success=False,
            status_code=status_code,
            committed=True,
            retryable=False,
            receipt=safe_payload,
            error_code="producer_committed_status_mismatch",
            error_message="producer ingest response claimed committed with non-2xx status",
        )
    committed = committed_value is True and 200 <= status_code < 300
    receipt_error_code = ""
    receipt_error_message = ""
    if committed:
        receipt_error_code, receipt_error_message = _receipt_identity_error(plan, payload)
        if not receipt_error_code:
            receipt_error_code, receipt_error_message = _receipt_accepted_shape_error(payload)
        if not receipt_error_code:
            receipt_error_code, receipt_error_message = _receipt_totals_error(plan, payload)
    totals = payload.get("totals") if isinstance(payload.get("totals"), dict) else {}
    success = (
        committed
        and not receipt_error_code
        and type(totals.get("errors")) is int
        and totals["errors"] == 0
        and type(totals.get("quarantined")) is int
        and totals["quarantined"] == 0
    )
    error = safe_payload.get("error") if isinstance(safe_payload.get("error"), dict) else {}
    retryable = False if receipt_error_code else payload.get("retryable") is True or status_code in {408, 429, 500, 502, 503, 504}
    return UploadResult(
        success=success,
        status_code=status_code,
        committed=committed,
        retryable=retryable,
        receipt=safe_payload,
        retry_after_seconds=retry_after_seconds if retryable else None,
        error_code=receipt_error_code or str(error.get("code") or ""),
        error_message=receipt_error_message or str(error.get("message") or ""),
    )


def _with_upload_status_artifact(
    plan: SourceFilePlan,
    result: UploadResult,
    status_dir: str | os.PathLike[str],
) -> UploadResult:
    if not status_dir:
        return result
    suffix = hashlib.sha256(plan.metadata["idempotency_key"].encode("utf-8")).hexdigest()[:12]
    status_path = Path(status_dir) / f"direct_sync_upload_status_{suffix}.json"
    receipt = dict(result.receipt or {})
    status_path_text = str(status_path)
    status_error_code = ""
    status_error_message = ""
    try:
        _write_json_atomic(
            status_path,
            {
                "success": result.success,
                "status_code": result.status_code,
                "committed": result.committed,
                "retryable": result.retryable,
                "retry_after_seconds": result.retry_after_seconds,
                "receipt": receipt,
                "error_code": result.error_code,
                "error_message": result.error_message,
                "metadata": dict(plan.metadata),
                "source_file_path": plan.source_file_path,
                "generated_at": utc_now_text(),
            },
        )
    except OSError as exc:
        status_path_text = ""
        status_error_code = "upload_status_write_failed"
        status_error_message = f"upload status artifact write failed: {exc.__class__.__name__}"
        if result.error_code:
            receipt["_local_upload_status_write_error_code"] = status_error_code
            receipt["_local_upload_status_write_error_message"] = status_error_message
            status_error_code = result.error_code or status_error_code
            status_error_message = result.error_message or status_error_message
    return UploadResult(
        success=result.success,
        status_code=result.status_code,
        committed=result.committed,
        retryable=result.retryable,
        receipt=receipt,
        retry_after_seconds=result.retry_after_seconds,
        status_path=status_path_text,
        error_code=status_error_code or result.error_code,
        error_message=status_error_message or result.error_message,
    )


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
    try:
        with Path(plan.source_file_path).open("rb") as handle:
            response = session.post(
                credentials.endpoint_url,
                data={"metadata": canonical_json(plan.metadata)},
                files={"file": (Path(plan.source_file_path).name, handle, "application/octet-stream")},
                headers=headers,
                timeout=timeout,
                allow_redirects=False,
            )
    except Exception as exc:
        result = _transport_error_result(exc)
    else:
        result = _upload_response_result(plan, response, credentials=credentials, headers=headers)
    return _with_upload_status_artifact(plan, result, status_dir)


def _execute_with_busy_retry(
    conn: sqlite3.Connection,
    statement: str,
    *,
    attempts: int = 6,
) -> sqlite3.Cursor:
    for attempt in range(attempts):
        try:
            return conn.execute(statement)
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt == attempts - 1:
                raise
            time.sleep(0.05 * (attempt + 1))
    raise RuntimeError("unreachable sqlite retry state")


def _connect_relay_db(db_path: str | os.PathLike[str]) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=SQLITE_BUSY_TIMEOUT_MS / 1000)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    _execute_with_busy_retry(conn, "PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _connect_relay_db_readonly(db_path: str | os.PathLike[str]) -> sqlite3.Connection | None:
    path = Path(db_path)
    if not path.exists():
        parent = path.parent
        if parent.exists() and not parent.is_dir():
            raise FileExistsError(str(parent))
        return None
    conn = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True, timeout=SQLITE_BUSY_TIMEOUT_MS / 1000)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    return conn


def _relay_batches_table_exists(conn: sqlite3.Connection) -> bool:
    return conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table'
          AND name = 'direct_sync_relay_batches'
        LIMIT 1
        """
    ).fetchone() is not None


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
                metadata_json TEXT,
                producer_id TEXT,
                key_id TEXT,
                endpoint_url TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        _ensure_relay_queue_columns(conn)
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_direct_sync_relay_status_due
            ON direct_sync_relay_batches(status, next_attempt_at, created_at)
            """
        )
        conn.commit()
    finally:
        conn.close()


def _ensure_relay_queue_columns(conn: sqlite3.Connection) -> None:
    columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(direct_sync_relay_batches)").fetchall()}
    migrations = {
        "metadata_json": "ALTER TABLE direct_sync_relay_batches ADD COLUMN metadata_json TEXT",
        "producer_id": "ALTER TABLE direct_sync_relay_batches ADD COLUMN producer_id TEXT",
        "key_id": "ALTER TABLE direct_sync_relay_batches ADD COLUMN key_id TEXT",
        "endpoint_url": "ALTER TABLE direct_sync_relay_batches ADD COLUMN endpoint_url TEXT",
    }
    for column, statement in migrations.items():
        if column not in columns:
            conn.execute(statement)


def _relay_metadata(row: sqlite3.Row) -> tuple[Dict[str, Any], str]:
    if "metadata_json" not in row.keys() or not row["metadata_json"]:
        return {}, "relay metadata_json is missing"
    try:
        payload = json.loads(str(row["metadata_json"]))
    except json.JSONDecodeError:
        return {}, "relay metadata_json is invalid JSON"
    if not isinstance(payload, dict):
        return {}, "relay metadata_json must be a JSON object"
    return payload, ""


def _relay_metadata_identity_matches(existing_metadata: Mapping[str, Any], planned_metadata: Mapping[str, Any]) -> bool:
    return all(
        str(existing_metadata.get(field) or "").strip() == str(planned_metadata.get(field) or "").strip()
        for field in RELAY_METADATA_IDENTITY_FIELDS
    )


def _relay_row_metadata_identity_matches(row: sqlite3.Row, plan: SourceFilePlan) -> bool:
    metadata, metadata_error = _relay_metadata(row)
    if metadata_error:
        return False
    return _relay_metadata_identity_matches(metadata, plan.metadata)


def _relay_row(row: sqlite3.Row, *, deduped_existing: bool = False) -> RelayQueueRow:
    metadata, metadata_error = _relay_metadata(row)
    return RelayQueueRow(
        relay_id=str(row["relay_id"]),
        status=str(row["status"]),
        spooled_file_path=str(row["spooled_file_path"]),
        producer_manifest_path=str(row["producer_manifest_path"]),
        relative_path=str(row["relative_path"]),
        content_sha256=str(row["content_sha256"]),
        byte_length=int(row["byte_length"]),
        attempt_count=int(row["attempt_count"]),
        metadata=metadata,
        producer_id=str(row["producer_id"] or ""),
        key_id=str(row["key_id"] or ""),
        endpoint_url=str(row["endpoint_url"] or ""),
        lease_owner=str(row["lease_owner"] or ""),
        deduped_existing=deduped_existing,
        metadata_error=metadata_error,
    )


def _json_object_from_text(text: str) -> Dict[str, Any]:
    payload = json.loads(str(text or "{}"))
    if not isinstance(payload, dict):
        raise DirectSyncPushError("JSON payload must be an object")
    return payload


def _upload_status_artifact_matches_relay(
    *,
    status_path: str,
    plan: SourceFilePlan,
    receipt: Mapping[str, Any],
) -> bool:
    try:
        status = _json_object_from_text(Path(status_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, DirectSyncPushError):
        return False
    return (
        status.get("success") is True
        and status.get("committed") is True
        and status.get("retryable") is False
        and status.get("receipt") == dict(receipt)
        and status.get("metadata") == dict(plan.metadata)
        and str(status.get("source_file_path") or "") == str(plan.source_file_path)
    )


def _acked_relay_retention_report(conn: sqlite3.Connection) -> Dict[str, Any]:
    rows = conn.execute(
        """
        SELECT spooled_file_path, upload_status_path, created_at
        FROM direct_sync_relay_batches
        WHERE status = ?
        ORDER BY created_at, relay_id
        """,
        (RELAY_STATUS_ACKED,),
    ).fetchall()
    spooled_bytes = 0
    missing_spooled_files = 0
    missing_upload_status_files = 0
    for row in rows:
        try:
            spooled_bytes += Path(str(row["spooled_file_path"] or "")).stat().st_size
        except OSError:
            missing_spooled_files += 1
        upload_status_path = str(row["upload_status_path"] or "")
        if not upload_status_path or not Path(upload_status_path).is_file():
            missing_upload_status_files += 1
    return {
        "status": "RETAIN_REQUIRED" if rows else "not_applicable",
        "read_only": True,
        "cleanup_safe": False,
        "acked_row_delete_allowed": False,
        "acked_spool_delete_allowed": False,
        "acked_upload_status_delete_allowed": False,
        "acked_count": len(rows),
        "acked_spool_total_bytes": spooled_bytes,
        "missing_acked_spool_count": missing_spooled_files,
        "missing_acked_upload_status_count": missing_upload_status_files,
        "oldest_acked_created_at": str(rows[0]["created_at"] or "") if rows else "",
        "blockers": [
            "acked rows are duplicate/lost-ack replay anchors",
            "server receipt replay retention proof is not attached",
            "no prune tombstone manifest exists for acked spool/status artifacts",
        ],
    }


def _empty_acked_relay_retention_report() -> Dict[str, Any]:
    return {
        "status": "not_applicable",
        "read_only": True,
        "cleanup_safe": False,
        "acked_row_delete_allowed": False,
        "acked_spool_delete_allowed": False,
        "acked_upload_status_delete_allowed": False,
        "acked_count": 0,
        "acked_spool_total_bytes": 0,
        "missing_acked_spool_count": 0,
        "missing_acked_upload_status_count": 0,
        "oldest_acked_created_at": "",
        "blockers": [
            "acked rows are duplicate/lost-ack replay anchors",
            "server receipt replay retention proof is not attached",
            "no prune tombstone manifest exists for acked spool/status artifacts",
        ],
    }


def acked_relay_retention_candidates(
    db_path: str | os.PathLike[str],
    *,
    limit: int = 20,
    excluded_relay_ids: Iterable[str] | None = None,
) -> tuple[AckedRelayRetentionCandidate, ...]:
    fetch_limit = max(0, int(limit or 0))
    if fetch_limit == 0:
        return tuple()
    excluded = tuple(str(relay_id) for relay_id in (excluded_relay_ids or ()) if str(relay_id))
    exclude_clause = ""
    params: list[Any] = [RELAY_STATUS_ACKED]
    if excluded:
        placeholders = ", ".join("?" for _ in excluded)
        exclude_clause = f"AND relay_id NOT IN ({placeholders})"
        params.extend(excluded)
    params.append(max(fetch_limit * 5, fetch_limit))
    conn = _connect_relay_db_readonly(db_path)
    if conn is None:
        return tuple()
    try:
        if not _relay_batches_table_exists(conn):
            return tuple()
        rows = conn.execute(
            f"""
            SELECT *
            FROM direct_sync_relay_batches
            WHERE status = ?
              AND receipt_json IS NOT NULL
              AND receipt_json != ''
              AND upload_status_path IS NOT NULL
              AND upload_status_path != ''
              {exclude_clause}
            ORDER BY updated_at, created_at, relay_id
            LIMIT ?
            """,
            params,
        ).fetchall()
    finally:
        conn.close()

    candidates: list[AckedRelayRetentionCandidate] = []
    for row in rows:
        try:
            relay_row = _relay_row(row)
            plan = _source_file_plan_from_relay_row(relay_row)
            receipt = _json_object_from_text(str(row["receipt_json"] or "{}"))
            receipt_error_code, _receipt_error_message = _receipt_identity_error(plan, receipt)
            if not receipt_error_code:
                receipt_error_code, _receipt_error_message = _receipt_accepted_shape_error(receipt)
            if not receipt_error_code:
                receipt_error_code, _receipt_error_message = _receipt_totals_error(plan, receipt)
            totals = receipt.get("totals") if isinstance(receipt.get("totals"), dict) else {}
            if (
                receipt.get("committed") is not True
                or receipt_error_code
                or totals.get("errors") != 0
                or totals.get("quarantined") != 0
            ):
                continue
            upload_status_path = str(row["upload_status_path"] or "")
            if not _upload_status_artifact_matches_relay(
                status_path=upload_status_path,
                plan=plan,
                receipt=receipt,
            ):
                continue
            spooled_hash, spooled_bytes = _read_file_digest(Path(relay_row.spooled_file_path))
            if spooled_hash != relay_row.content_sha256 or spooled_bytes != relay_row.byte_length:
                continue
            candidates.append(
                AckedRelayRetentionCandidate(
                    relay_id=relay_row.relay_id,
                    spooled_file_path=relay_row.spooled_file_path,
                    upload_status_path=upload_status_path,
                    producer_manifest_path=relay_row.producer_manifest_path,
                    relative_path=relay_row.relative_path,
                    content_sha256=relay_row.content_sha256,
                    byte_length=relay_row.byte_length,
                    metadata=dict(relay_row.metadata),
                    receipt=receipt,
                )
            )
        except (OSError, DirectSyncPushError, json.JSONDecodeError, ValueError, TypeError):
            continue
        if len(candidates) >= fetch_limit:
            break
    return tuple(candidates)


def _copy_file_atomic(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination.with_suffix(destination.suffix + ".tmp")
    with source.open("rb") as src, temp_path.open("wb") as dst:
        for chunk in iter(lambda: src.read(1024 * 1024), b""):
            dst.write(chunk)
        dst.flush()
        os.fsync(dst.fileno())
    os.replace(temp_path, destination)


def _copy_spool_file_atomic(source: Path, destination: Path) -> None:
    try:
        _copy_file_atomic(source, destination)
    except OSError as exc:
        raise RelaySpoolFileError(f"relay spool file cannot be written: {exc.__class__.__name__}") from exc


def _find_existing_relay_batch(
    conn: sqlite3.Connection,
    *,
    plan: SourceFilePlan,
) -> sqlite3.Row | None:
    rows = conn.execute(
        """
        SELECT *
        FROM direct_sync_relay_batches
        WHERE relative_path = ?
          AND content_sha256 = ?
          AND byte_length = ?
        ORDER BY created_at, relay_id
        """,
        (
            plan.metadata["relative_path"],
            plan.content_sha256,
            plan.byte_length,
        ),
    ).fetchall()
    for row in rows:
        if _relay_row_metadata_identity_matches(row, plan):
            return row
    return None


def _find_conflicting_relay_batch(
    conn: sqlite3.Connection,
    *,
    plan: SourceFilePlan,
) -> sqlite3.Row | None:
    rows = conn.execute(
        """
        SELECT *
        FROM direct_sync_relay_batches
        WHERE relative_path = ?
          AND (content_sha256 != ? OR byte_length != ?)
        ORDER BY created_at, relay_id
        """,
        (
            plan.metadata["relative_path"],
            plan.content_sha256,
            plan.byte_length,
        ),
    ).fetchall()
    for row in rows:
        if _relay_row_metadata_identity_matches(row, plan):
            return row
    return None


def _spooled_file_matches_relay_row(row: sqlite3.Row) -> bool:
    try:
        spooled_hash, spooled_bytes = _read_file_digest(Path(str(row["spooled_file_path"])))
    except OSError:
        return False
    return spooled_hash == str(row["content_sha256"]).lower() and spooled_bytes == int(row["byte_length"])


def _repair_relay_spool_for_existing_row(
    conn: sqlite3.Connection,
    *,
    existing: sqlite3.Row,
    source_path: Path,
    spool_dir: str | os.PathLike[str],
) -> sqlite3.Row:
    relay_id = str(existing["relay_id"])
    repaired_spool_path = Path(spool_dir) / f"{relay_id}{source_path.suffix or '.bin'}"
    _copy_spool_file_atomic(source_path, repaired_spool_path)
    spooled_hash, spooled_bytes = _read_file_digest(repaired_spool_path)
    if spooled_hash != str(existing["content_sha256"]).lower() or spooled_bytes != int(existing["byte_length"]):
        raise DirectSyncPushError("repaired spool file hash or byte length mismatch")
    conn.execute(
        """
        UPDATE direct_sync_relay_batches
        SET source_file_path = ?,
            spooled_file_path = ?,
            updated_at = ?
        WHERE relay_id = ?
        """,
        (
            str(source_path),
            str(repaired_spool_path),
            utc_now_text(),
            relay_id,
        ),
    )
    return conn.execute(
        "SELECT * FROM direct_sync_relay_batches WHERE relay_id = ?",
        (relay_id,),
    ).fetchone()


def enqueue_source_file_for_relay(
    *,
    db_path: str | os.PathLike[str],
    spool_dir: str | os.PathLike[str],
    source_file_path: str | os.PathLike[str],
    producer_manifest_path: str | os.PathLike[str],
    credentials: ProducerCredentials,
    relative_path: str = "",
    dedupe_existing: bool = False,
) -> RelayQueueRow:
    init_relay_queue_schema(db_path)
    source_path = Path(source_file_path).resolve()
    manifest_path = Path(producer_manifest_path).resolve()
    if not source_path.is_file():
        raise DirectSyncPushError(f"source file does not exist: {source_path}")
    relay_id = f"relay-{uuid.uuid4().hex}"
    plan = build_source_file_plan(
        source_file_path=source_path,
        producer_manifest_path=manifest_path,
        credentials=credentials,
        relative_path=relative_path,
        client_batch_id=relay_id,
    )
    conn = _connect_relay_db(db_path)
    spool_path: Path | None = None
    try:
        conn.execute("BEGIN IMMEDIATE")
        conflicting = _find_conflicting_relay_batch(
            conn,
            plan=plan,
        )
        if conflicting is not None:
            raise DirectSyncPushError("source file content conflict for existing relay identity")
        if dedupe_existing:
            existing = _find_existing_relay_batch(
                conn,
                plan=plan,
            )
            if existing is not None:
                if str(existing["status"]) in {RELAY_STATUS_PENDING, RELAY_STATUS_RETRY_WAIT} and not _spooled_file_matches_relay_row(existing):
                    existing = _repair_relay_spool_for_existing_row(
                        conn,
                        existing=existing,
                        source_path=source_path,
                        spool_dir=spool_dir,
                    )
                conn.commit()
                return _relay_row(existing, deduped_existing=True)

        spool_path = Path(spool_dir) / f"{relay_id}{source_path.suffix or '.bin'}"
        _copy_spool_file_atomic(source_path, spool_path)
        spooled_hash, spooled_bytes = _read_file_digest(spool_path)
        if spooled_hash != plan.content_sha256 or spooled_bytes != plan.byte_length:
            raise DirectSyncPushError("spooled file hash or byte length mismatch")
        now = utc_now_text()
        conn.execute(
            """
            INSERT INTO direct_sync_relay_batches (
                relay_id, status, source_file_path, spooled_file_path,
                producer_manifest_path, relative_path, content_sha256,
                byte_length, attempt_count, next_attempt_at, metadata_json,
                producer_id, key_id, endpoint_url, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                relay_id,
                RELAY_STATUS_PENDING,
                str(source_path),
                str(spool_path),
                str(manifest_path),
                plan.metadata["relative_path"],
                plan.content_sha256,
                plan.byte_length,
                now,
                canonical_json(plan.metadata),
                credentials.producer_id,
                credentials.key_id,
                credentials.endpoint_url,
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
    except Exception:
        conn.rollback()
        if spool_path is not None and spool_path.exists():
            try:
                spool_path.unlink()
            except OSError:
                pass
        raise
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
            f"""
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
    lease_owner: str,
    status: str,
    receipt: Mapping[str, Any] | None = None,
    upload_status_path: str = "",
    next_attempt_at: str = "",
    error_code: str = "",
    error_message: str = "",
    expected_attempt_count: int | None = None,
) -> None:
    now = utc_now_text()
    conn = _connect_relay_db(db_path)
    try:
        where_clause = """
            WHERE relay_id = ?
              AND status = ?
              AND lease_owner = ?
            """
        params: list[Any] = [
            status,
            next_attempt_at or None,
            error_code,
            error_message,
            json.dumps(dict(receipt or {}), ensure_ascii=False, sort_keys=True) if receipt is not None else None,
            upload_status_path,
            now,
            relay_id,
            RELAY_STATUS_LEASED,
            lease_owner,
        ]
        if expected_attempt_count is not None:
            where_clause += " AND attempt_count = ?"
            params.append(expected_attempt_count)
        cursor = conn.execute(
            f"""
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
            {where_clause}
            """,
            params,
        )
        conn.commit()
        if cursor.rowcount != 1:
            raise DirectSyncPushError("relay lease is no longer owned by this worker")
    finally:
        conn.close()


def _retry_after_seconds(attempt_count: int, base_seconds: int, jitter_key: str = "") -> int:
    multiplier = min(max(1, attempt_count), 5)
    base_delay = max(1, int(base_seconds)) * multiplier
    key = str(jitter_key or "").strip()
    if not key:
        return base_delay
    jitter_window = min(max(1, base_delay // 5), max(1, int(base_seconds)), 300)
    jitter = int(hashlib.sha256(key.encode("utf-8")).hexdigest()[:8], 16) % (jitter_window + 1)
    return base_delay + jitter


def _source_file_plan_from_relay_row(row: RelayQueueRow) -> SourceFilePlan:
    if row.metadata_error:
        raise DirectSyncPushError(row.metadata_error)
    metadata = dict(row.metadata)
    if str(metadata.get("client_batch_id") or "") != row.relay_id:
        raise DirectSyncPushError("relay metadata client_batch_id does not match relay_id")
    if str(metadata.get("relative_path") or "") != row.relative_path:
        raise DirectSyncPushError("relay metadata relative_path does not match queued row")
    if str(metadata.get("content_sha256") or "").lower() != row.content_sha256.lower():
        raise DirectSyncPushError("relay metadata content_sha256 does not match queued row")
    if type(metadata.get("byte_length")) is not int or metadata["byte_length"] != row.byte_length:
        raise DirectSyncPushError("relay metadata byte_length does not match queued row")
    return SourceFilePlan(
        source_file_path=row.spooled_file_path,
        metadata=metadata,
        content_sha256=row.content_sha256,
        byte_length=row.byte_length,
    )


def _relay_credentials_issue(row: RelayQueueRow, credentials: ProducerCredentials) -> tuple[str, str]:
    if not row.producer_id or not row.key_id or not row.endpoint_url:
        return (
            "relay_credentials_unpinned",
            "relay batch was queued before producer credentials were pinned; operator review is required",
        )
    if (
        row.producer_id != credentials.producer_id
        or row.key_id != credentials.key_id
        or row.endpoint_url != credentials.endpoint_url
    ):
        return (
            "relay_credentials_changed",
            "current producer credentials do not match the queued relay batch",
        )
    return "", ""


def _upload_exception_result(row: RelayQueueRow, exc: Exception) -> UploadResult:
    return UploadResult(
        success=False,
        status_code=0,
        committed=False,
        retryable=False,
        receipt={"client_batch_id": row.relay_id},
        error_code="upload_unhandled_exception",
        error_message=f"direct-sync upload failed before returning a result: {exc.__class__.__name__}",
    )


def _set_claimed_relay_status(
    row: RelayQueueRow,
    *,
    db_path: str | os.PathLike[str],
    status: str,
    receipt: Mapping[str, Any] | None = None,
    upload_status_path: str = "",
    next_attempt_at: str = "",
    error_code: str = "",
    error_message: str = "",
) -> None:
    return _set_relay_status(
        db_path=db_path,
        relay_id=row.relay_id,
        lease_owner=row.lease_owner,
        status=status,
        receipt=receipt,
        upload_status_path=upload_status_path,
        next_attempt_at=next_attempt_at,
        error_code=error_code,
        error_message=error_message,
        expected_attempt_count=row.attempt_count,
    )


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
    try:
        spooled_hash, spooled_bytes = _read_file_digest(Path(row.spooled_file_path))
    except OSError as exc:
        error_code = "spooled_file_missing" if isinstance(exc, FileNotFoundError) else "spooled_file_unreadable"
        result = UploadResult(
            success=False,
            status_code=0,
            committed=False,
            retryable=False,
            receipt={"client_batch_id": row.relay_id},
            error_code=error_code,
            error_message=f"spooled file cannot be read: {exc}",
        )
        _set_claimed_relay_status(
            row,
            db_path=db_path,
            status=RELAY_STATUS_FAILED_PERMANENT,
            receipt=result.receipt,
            error_code=result.error_code,
            error_message=result.error_message,
        )
        return result
    if spooled_hash != row.content_sha256 or spooled_bytes != row.byte_length:
        result = UploadResult(
            success=False,
            status_code=0,
            committed=False,
            retryable=False,
            receipt={},
            error_code="spooled_file_digest_mismatch",
            error_message="spooled file content does not match queued content hash/byte length",
        )
        _set_claimed_relay_status(
            row,
            db_path=db_path,
            status=RELAY_STATUS_FAILED_PERMANENT,
            error_code=result.error_code,
            error_message=result.error_message,
        )
        return result
    try:
        plan = _source_file_plan_from_relay_row(row)
    except (DirectSyncPushError, ValueError, TypeError) as exc:
        result = UploadResult(
            success=False,
            status_code=0,
            committed=False,
            retryable=False,
            receipt={"client_batch_id": row.relay_id},
            error_code="relay_metadata_invalid",
            error_message=str(exc),
        )
        _set_claimed_relay_status(
            row,
            db_path=db_path,
            status=RELAY_STATUS_OPERATOR_REVIEW,
            receipt=result.receipt,
            error_code=result.error_code,
            error_message=result.error_message,
        )
        return result
    credential_error_code, credential_error_message = _relay_credentials_issue(row, credentials)
    if credential_error_code:
        result = UploadResult(
            success=False,
            status_code=0,
            committed=False,
            retryable=False,
            receipt={"client_batch_id": row.relay_id},
            error_code=credential_error_code,
            error_message=credential_error_message,
        )
        _set_claimed_relay_status(
            row,
            db_path=db_path,
            status=RELAY_STATUS_OPERATOR_REVIEW,
            receipt=result.receipt,
            error_code=result.error_code,
            error_message=result.error_message,
        )
        return result
    try:
        result = upload_source_file(
            plan,
            credentials,
            session=session,
            timeout=timeout,
            status_dir=status_dir,
        )
    except Exception as exc:
        result = _upload_exception_result(row, exc)
        _set_claimed_relay_status(
            row,
            db_path=db_path,
            status=RELAY_STATUS_OPERATOR_REVIEW,
            receipt=result.receipt,
            error_code=result.error_code,
            error_message=result.error_message,
        )
        return result
    if result.success:
        _set_claimed_relay_status(
            row,
            db_path=db_path,
            status=RELAY_STATUS_ACKED,
            receipt=result.receipt,
            upload_status_path=result.status_path,
            error_code=result.error_code,
            error_message=result.error_message,
        )
    elif result.committed:
        _set_claimed_relay_status(
            row,
            db_path=db_path,
            status=RELAY_STATUS_OPERATOR_REVIEW,
            receipt=result.receipt,
            upload_status_path=result.status_path,
            error_code=result.error_code,
            error_message=result.error_message,
        )
    elif result.retryable:
        retry_after = (
            result.retry_after_seconds
            if result.retry_after_seconds is not None
            else _retry_after_seconds(
                row.attempt_count,
                retry_base_seconds,
                row.relay_id,
            )
        )
        next_attempt_at = (
            datetime.now(timezone.utc) + timedelta(seconds=retry_after)
        ).isoformat().replace("+00:00", "Z")
        _set_claimed_relay_status(
            row,
            db_path=db_path,
            status=RELAY_STATUS_RETRY_WAIT,
            receipt=result.receipt,
            upload_status_path=result.status_path,
            next_attempt_at=next_attempt_at,
            error_code=result.error_code,
            error_message=result.error_message,
        )
    else:
        _set_claimed_relay_status(
            row,
            db_path=db_path,
            status=RELAY_STATUS_FAILED_PERMANENT,
            receipt=result.receipt,
            upload_status_path=result.status_path,
            error_code=result.error_code,
            error_message=result.error_message,
        )
    return result


def relay_queue_status(db_path: str | os.PathLike[str]) -> Dict[str, Any]:
    conn = _connect_relay_db_readonly(db_path)
    if conn is None:
        return {
            "counts": {},
            "oldest_active_created_at": "",
            "acked_retention": _empty_acked_relay_retention_report(),
        }
    try:
        if not _relay_batches_table_exists(conn):
            return {
                "counts": {},
                "oldest_active_created_at": "",
                "acked_retention": _empty_acked_relay_retention_report(),
            }
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
            "acked_retention": _acked_relay_retention_report(conn),
        }
    finally:
        conn.close()

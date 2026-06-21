# -*- coding: utf-8 -*-
"""Direct HTTP source-file uploader for Label_Match CSV logs."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import unicodedata
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
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

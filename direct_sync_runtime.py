# -*- coding: utf-8 -*-
"""Local runtime wrapper for the Label_Match direct-sync relay."""

from __future__ import annotations

import json
import os
import re
import shutil
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from direct_sync_push import (
    DEFAULT_RETRY_SECONDS,
    DEFAULT_TIMEOUT_SECONDS,
    DirectSyncPushError,
    ProducerCredentials,
    RELAY_STATUS_LEASED,
    RELAY_STATUS_PENDING,
    RELAY_STATUS_RETRY_WAIT,
    UploadResult,
    drain_one_relay_batch,
    enqueue_source_file_for_relay,
    relay_queue_status,
    reset_stale_relay_leases,
    utc_now_text,
    validate_endpoint_url,
)
from direct_sync_operator import read_operator_pause


DEFAULT_WORKER_ID = "direct-sync-relay-label-match"
PRODUCTION_PROFILE_ENV_NAMES = ("APP_ENV", "ENV", "LABEL_MATCH_PRODUCTION", "DIRECT_SYNC_PRODUCTION")
SECRET_REF_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")
WINDOWS_RESERVED_DEVICE_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *{f"COM{index}" for index in range(1, 10)},
    *{f"LPT{index}" for index in range(1, 10)},
}


@dataclass(frozen=True)
class DirectSyncRuntimeConfig:
    db_path: str | os.PathLike[str]
    spool_dir: str | os.PathLike[str]
    producer_manifest_path: str | os.PathLike[str]
    credential_path: str | os.PathLike[str]
    upload_status_dir: str | os.PathLike[str]
    runtime_status_path: str | os.PathLike[str]
    log_path: str | os.PathLike[str]
    worker_id: str = DEFAULT_WORKER_ID
    min_free_bytes: int = 0
    retry_base_seconds: int = DEFAULT_RETRY_SECONDS
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    operator_pause_path: str | os.PathLike[str] = ""
    max_active_queue_count: int = 0
    max_active_queue_age_seconds: int = 0


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


def _production_profile_enabled() -> bool:
    return any(
        str(os.getenv(name) or "").strip().lower() in {"1", "true", "prod", "production"}
        for name in PRODUCTION_PROFILE_ENV_NAMES
    )


def _safe_secret_ref_name(value: str) -> str:
    text = str(value or "")
    if not text or text != text.strip():
        raise DirectSyncPushError("secret_ref target name is unsafe")
    if text in {".", ".."} or text.startswith(".") or text.endswith("."):
        raise DirectSyncPushError("secret_ref target name is unsafe")
    if not SECRET_REF_NAME_RE.fullmatch(text):
        raise DirectSyncPushError("secret_ref target name is unsafe")
    reserved_base = text.split(".", 1)[0].upper()
    if reserved_base in WINDOWS_RESERVED_DEVICE_NAMES:
        raise DirectSyncPushError("secret_ref target name is unsafe")
    return text


def _default_secret_data_dir(credential_path: Path) -> Path:
    local_app_data = os.getenv("LOCALAPPDATA")
    if local_app_data:
        return Path(local_app_data) / "CompanyProducerConnector"
    return credential_path.parent / "CompanyProducerConnector"


def _dpapi_unprotect_current_user(protected: bytes) -> bytes:
    if sys.platform != "win32":
        raise DirectSyncPushError("dpapi secret_ref requires Windows")
    import ctypes
    from ctypes import byref, c_void_p, wintypes

    class DataBlob(ctypes.Structure):
        _fields_ = [("cbData", wintypes.DWORD), ("pbData", c_void_p)]

    crypt32 = ctypes.windll.crypt32
    kernel32 = ctypes.windll.kernel32
    input_buffer = ctypes.create_string_buffer(protected, len(protected))
    input_blob = DataBlob(len(protected), ctypes.cast(input_buffer, c_void_p))
    output_blob = DataBlob()
    if not crypt32.CryptUnprotectData(byref(input_blob), None, None, None, None, 0, byref(output_blob)):
        raise DirectSyncPushError("dpapi secret_ref could not be read")
    try:
        return ctypes.string_at(output_blob.pbData, output_blob.cbData)
    finally:
        kernel32.LocalFree(output_blob.pbData)


def _read_wincred_secret(target_name: str) -> str:
    if sys.platform != "win32":
        raise DirectSyncPushError("wincred secret_ref requires Windows")
    import ctypes
    from ctypes import POINTER, byref, c_void_p, wintypes

    class FileTime(ctypes.Structure):
        _fields_ = [("dwLowDateTime", wintypes.DWORD), ("dwHighDateTime", wintypes.DWORD)]

    class Credential(ctypes.Structure):
        _fields_ = [
            ("Flags", wintypes.DWORD),
            ("Type", wintypes.DWORD),
            ("TargetName", wintypes.LPWSTR),
            ("Comment", wintypes.LPWSTR),
            ("LastWritten", FileTime),
            ("CredentialBlobSize", wintypes.DWORD),
            ("CredentialBlob", c_void_p),
            ("Persist", wintypes.DWORD),
            ("AttributeCount", wintypes.DWORD),
            ("Attributes", c_void_p),
            ("TargetAlias", wintypes.LPWSTR),
            ("UserName", wintypes.LPWSTR),
        ]

    advapi32 = ctypes.windll.advapi32
    credential_ptr = c_void_p()
    if not advapi32.CredReadW(target_name, 1, 0, byref(credential_ptr)):
        raise DirectSyncPushError("wincred secret_ref could not be read")
    try:
        credential = ctypes.cast(credential_ptr, POINTER(Credential)).contents
        secret_bytes = ctypes.string_at(credential.CredentialBlob, credential.CredentialBlobSize)
        return secret_bytes.decode("utf-8")
    finally:
        advapi32.CredFree(credential_ptr)


def _resolve_secret_ref(secret_ref: str, *, credential_path: Path, secret_data_dir: str = "") -> str:
    text = str(secret_ref or "").strip()
    if ":" not in text:
        raise DirectSyncPushError("secret_ref must start with env:, dpapi:, or wincred:")
    scheme, target = text.split(":", 1)
    scheme = scheme.lower()
    name = _safe_secret_ref_name(target)
    if scheme == "env":
        if _production_profile_enabled():
            raise DirectSyncPushError("env secret_ref is disabled in production")
        value = os.getenv(name)
        if not value:
            raise DirectSyncPushError("env secret_ref is not available")
        return value
    if scheme == "dpapi":
        base_dir = Path(secret_data_dir).expanduser() if secret_data_dir else _default_secret_data_dir(credential_path)
        protected_path = base_dir / "secrets" / f"{name}.dpapi"
        if not protected_path.is_file():
            raise DirectSyncPushError("dpapi secret_ref artifact is missing")
        return _dpapi_unprotect_current_user(protected_path.read_bytes()).decode("utf-8")
    if scheme == "wincred":
        return _read_wincred_secret(f"KMTech.DirectSync.{name}")
    raise DirectSyncPushError("secret_ref must start with env:, dpapi:, or wincred:")


def load_credentials_from_json(path: str | os.PathLike[str]) -> ProducerCredentials:
    credential_path = Path(path)
    payload = json.loads(credential_path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise DirectSyncPushError("credential file must be a JSON object")
    producer_id = str(payload.get("producer_id") or "").strip()
    key_id = str(payload.get("key_id") or "").strip()
    secret = payload.get("secret")
    secret_ref = str(payload.get("secret_ref") or "").strip()
    endpoint_url = str(payload.get("endpoint_url") or "").strip()
    if secret and secret_ref:
        raise DirectSyncPushError("credential file must not contain both secret and secret_ref")
    if secret and _production_profile_enabled():
        raise DirectSyncPushError("raw credential secret is disabled in production")
    if secret_ref:
        secret = _resolve_secret_ref(
            secret_ref,
            credential_path=credential_path,
            secret_data_dir=str(payload.get("secret_data_dir") or ""),
        )
    if not producer_id or not key_id or not secret or not endpoint_url:
        raise DirectSyncPushError("credential file is missing producer_id, key_id, secret/secret_ref, or endpoint_url")
    validate_endpoint_url(endpoint_url)
    return ProducerCredentials(
        producer_id=producer_id,
        key_id=key_id,
        secret=secret,
        endpoint_url=endpoint_url,
    )


def _disk_pressure_report(config: DirectSyncRuntimeConfig) -> dict[str, Any]:
    spool_dir = Path(config.spool_dir)
    spool_dir.mkdir(parents=True, exist_ok=True)
    usage = shutil.disk_usage(spool_dir)
    min_free = max(0, int(config.min_free_bytes or 0))
    return {
        "status": "blocked" if usage.free < min_free else "pass",
        "path": str(spool_dir),
        "free_bytes": int(usage.free),
        "total_bytes": int(usage.total),
        "used_bytes": int(usage.used),
        "min_free_bytes": min_free,
    }


def _safe_relay_queue_status(db_path: str | os.PathLike[str]) -> dict[str, Any]:
    try:
        return relay_queue_status(db_path)
    except sqlite3.DatabaseError as exc:
        return {
            "status": "unavailable",
            "error_code": "relay_queue_db_error",
            "error_message": f"relay queue database error: {exc.__class__.__name__}",
        }


def _runtime_error_details(exc: Exception) -> tuple[str, str]:
    if isinstance(exc, sqlite3.DatabaseError):
        return "relay_queue_db_error", f"relay queue database error: {exc.__class__.__name__}"
    return "direct_sync_runtime_error", str(exc)


def _parse_utc_text(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _queue_backpressure_report(
    config: DirectSyncRuntimeConfig,
    *,
    now: str = "",
) -> dict[str, Any]:
    max_count = max(0, int(config.max_active_queue_count or 0))
    max_age_seconds = max(0, int(config.max_active_queue_age_seconds or 0))
    if max_count <= 0 and max_age_seconds <= 0:
        return {
            "status": "pass",
            "enabled": False,
            "max_active_queue_count": max_count,
            "max_active_queue_age_seconds": max_age_seconds,
        }
    queue = relay_queue_status(config.db_path)
    counts = dict(queue.get("counts") or {})
    active_count = sum(
        int(counts.get(status, 0) or 0)
        for status in (RELAY_STATUS_PENDING, RELAY_STATUS_RETRY_WAIT, RELAY_STATUS_LEASED)
    )
    oldest_active = str(queue.get("oldest_active_created_at") or "")
    oldest_age_seconds = 0
    reasons: list[str] = []
    if max_count > 0 and active_count >= max_count:
        reasons.append("active_queue_count_threshold")
    if max_age_seconds > 0 and oldest_active:
        oldest_dt = _parse_utc_text(oldest_active)
        now_dt = _parse_utc_text(now) or datetime.now(timezone.utc)
        if oldest_dt is None:
            reasons.append("oldest_active_age_unknown")
        else:
            oldest_age_seconds = max(0, int((now_dt - oldest_dt).total_seconds()))
            if oldest_age_seconds >= max_age_seconds:
                reasons.append("oldest_active_age_threshold")
    return {
        "status": "blocked" if reasons else "pass",
        "enabled": True,
        "reasons": reasons,
        "active_queue_count": active_count,
        "oldest_active_created_at": oldest_active,
        "oldest_active_age_seconds": oldest_age_seconds,
        "max_active_queue_count": max_count,
        "max_active_queue_age_seconds": max_age_seconds,
        "queue": queue,
    }


def _result_summary(result: UploadResult | None) -> dict[str, Any]:
    if result is None:
        return {
            "status": "idle",
            "success": False,
            "committed": False,
            "retryable": False,
            "status_code": 0,
            "error_code": "",
        }
    relay_id = ""
    if isinstance(result.receipt, Mapping):
        relay_id = str(result.receipt.get("client_batch_id") or "").strip()
    if result.success:
        status = "acked"
    elif result.committed:
        status = "operator_review"
    elif result.retryable:
        status = "retry_wait"
    else:
        status = "failed_permanent"
    return {
        "status": status,
        "success": result.success,
        "committed": result.committed,
        "retryable": result.retryable,
        "status_code": result.status_code,
        "error_code": result.error_code,
        "relay_id": relay_id,
        "upload_status_path": result.status_path,
    }


def _write_runtime_status(
    config: DirectSyncRuntimeConfig,
    *,
    status: str,
    queue: Mapping[str, Any],
    disk: Mapping[str, Any],
    stale_leases_reset: int = 0,
    last_result: Mapping[str, Any] | None = None,
    operator_control: Mapping[str, Any] | None = None,
    queue_backpressure: Mapping[str, Any] | None = None,
    error_code: str = "",
    error_message: str = "",
) -> dict[str, Any]:
    payload = {
        "status": status,
        "app": "Label_Match",
        "worker_id": config.worker_id,
        "queue": dict(queue),
        "disk": dict(disk),
        "stale_leases_reset": int(stale_leases_reset),
        "last_result": dict(last_result or {}),
        "operator_control": dict(operator_control or {}),
        "queue_backpressure": dict(queue_backpressure or {}),
        "error_code": error_code,
        "error_message": error_message,
        "updated_at": utc_now_text(),
    }
    _write_json_atomic(config.runtime_status_path, payload)
    return payload


def _append_runtime_event(config: DirectSyncRuntimeConfig, event: str, payload: Mapping[str, Any]) -> None:
    entry = {
        "event": event,
        "app": "Label_Match",
        "worker_id": config.worker_id,
        "credential_ref": str(config.credential_path),
        "generated_at": utc_now_text(),
    }
    entry.update(dict(payload))
    _append_jsonl(config.log_path, entry)


def _paused_by_operator(config: DirectSyncRuntimeConfig) -> dict[str, Any]:
    return read_operator_pause(config.operator_pause_path)


def _write_paused_status(config: DirectSyncRuntimeConfig, *, event: str) -> dict[str, Any]:
    pause = _paused_by_operator(config)
    queue = relay_queue_status(config.db_path)
    status = _write_runtime_status(
        config,
        status="paused_by_operator",
        queue=queue,
        disk={"status": "not_checked", "reason": "operator_pause"},
        operator_control=pause,
        error_code="operator_paused",
        error_message="direct-sync relay is paused by local operator control",
    )
    _append_runtime_event(config, event, status)
    return status


def _write_backpressure_status(
    config: DirectSyncRuntimeConfig,
    *,
    backpressure: Mapping[str, Any],
    event: str,
) -> dict[str, Any]:
    status = _write_runtime_status(
        config,
        status="blocked_queue_backpressure",
        queue=backpressure.get("queue") if isinstance(backpressure.get("queue"), Mapping) else relay_queue_status(config.db_path),
        disk={"status": "not_checked", "reason": "queue_backpressure"},
        queue_backpressure=backpressure,
        error_code="queue_backpressure",
        error_message="direct-sync relay active queue exceeds configured enqueue threshold",
    )
    _append_runtime_event(config, event, status)
    return status


def enqueue_completed_source_file(
    config: DirectSyncRuntimeConfig,
    *,
    source_file_path: str | os.PathLike[str],
    relative_path: str = "",
    credentials: ProducerCredentials | None = None,
) -> dict[str, Any]:
    """Spool one completed Label_Match CSV and persist local operator evidence."""
    if _paused_by_operator(config).get("paused"):
        return _write_paused_status(config, event="enqueue_paused_by_operator")

    backpressure = _queue_backpressure_report(config)
    if backpressure["status"] != "pass":
        return _write_backpressure_status(
            config,
            backpressure=backpressure,
            event="enqueue_blocked_queue_backpressure",
        )

    disk = _disk_pressure_report(config)
    if disk["status"] != "pass":
        queue = _safe_relay_queue_status(config.db_path)
        status = _write_runtime_status(
            config,
            status="blocked_disk_pressure",
            queue=queue,
            disk=disk,
            error_code="disk_pressure",
            error_message="free space is below configured direct-sync relay minimum",
        )
        _append_runtime_event(config, "enqueue_blocked_disk_pressure", status)
        return status
    try:
        creds = credentials or load_credentials_from_json(config.credential_path)
        row = enqueue_source_file_for_relay(
            db_path=config.db_path,
            spool_dir=config.spool_dir,
            source_file_path=source_file_path,
            producer_manifest_path=config.producer_manifest_path,
            credentials=creds,
            relative_path=relative_path,
            dedupe_existing=True,
        )
    except DirectSyncPushError as exc:
        queue = relay_queue_status(config.db_path)
        status = _write_runtime_status(
            config,
            status="enqueue_error",
            queue=queue,
            disk=disk,
            error_code="direct_sync_enqueue_error",
            error_message=str(exc),
        )
        _append_runtime_event(config, "enqueue_error", status)
        return status
    queue = relay_queue_status(config.db_path)
    status = _write_runtime_status(
        config,
        status="enqueued",
        queue=queue,
        disk=disk,
        last_result={
            "relay_id": row.relay_id,
            "relay_status": row.status,
            "spooled_file_path": row.spooled_file_path,
            "relative_path": row.relative_path,
            "content_sha256": row.content_sha256,
            "byte_length": row.byte_length,
            "deduped_existing": row.deduped_existing,
        },
    )
    _append_runtime_event(
        config,
        "enqueue_completed_source_file",
        {
            "relay_id": row.relay_id,
            "relay_status": row.status,
            "spooled_file_path": row.spooled_file_path,
            "relative_path": row.relative_path,
            "content_sha256": row.content_sha256,
            "byte_length": row.byte_length,
            "deduped_existing": row.deduped_existing,
        },
    )
    return status


def run_relay_once(
    config: DirectSyncRuntimeConfig,
    *,
    session: Any = None,
    credentials: ProducerCredentials | None = None,
    now: str = "",
) -> dict[str, Any]:
    """Run one bounded relay drain cycle and persist status/log evidence."""
    if _paused_by_operator(config).get("paused"):
        return _write_paused_status(config, event="relay_paused_by_operator")

    disk = _disk_pressure_report(config)
    if disk["status"] != "pass":
        queue = relay_queue_status(config.db_path)
        status = _write_runtime_status(
            config,
            status="blocked_disk_pressure",
            queue=queue,
            disk=disk,
            error_code="disk_pressure",
            error_message="free space is below configured direct-sync relay minimum",
        )
        _append_runtime_event(config, "relay_blocked_disk_pressure", status)
        return status

    reset_count = 0
    try:
        reset_count = reset_stale_relay_leases(db_path=config.db_path, now=now or utc_now_text())
        creds = credentials or load_credentials_from_json(config.credential_path)
        result = drain_one_relay_batch(
            db_path=config.db_path,
            credentials=creds,
            worker_id=config.worker_id,
            session=session,
            status_dir=config.upload_status_dir,
            retry_base_seconds=config.retry_base_seconds,
            timeout=config.timeout_seconds,
        )
    except (DirectSyncPushError, sqlite3.DatabaseError) as exc:
        queue = _safe_relay_queue_status(config.db_path)
        error_code, error_message = _runtime_error_details(exc)
        status = _write_runtime_status(
            config,
            status="runtime_error",
            queue=queue,
            disk=disk,
            stale_leases_reset=reset_count,
            error_code=error_code,
            error_message=error_message,
        )
        _append_runtime_event(config, "relay_runtime_error", status)
        return status

    result_summary = _result_summary(result)
    queue = _safe_relay_queue_status(config.db_path)
    status = _write_runtime_status(
        config,
        status=result_summary["status"],
        queue=queue,
        disk=disk,
        stale_leases_reset=reset_count,
        last_result=result_summary,
    )
    _append_runtime_event(config, "relay_runner_once", status)
    return status

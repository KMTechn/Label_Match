# -*- coding: utf-8 -*-
"""Local runtime wrapper for the Label_Match direct-sync relay."""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from direct_sync_push import (
    DEFAULT_RETRY_SECONDS,
    DEFAULT_TIMEOUT_SECONDS,
    DirectSyncPushError,
    ProducerCredentials,
    UploadResult,
    drain_one_relay_batch,
    enqueue_source_file_for_relay,
    relay_queue_status,
    reset_stale_relay_leases,
    utc_now_text,
)
from direct_sync_operator import read_operator_pause


DEFAULT_WORKER_ID = "direct-sync-relay-label-match"


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


def load_credentials_from_json(path: str | os.PathLike[str]) -> ProducerCredentials:
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if not isinstance(payload, dict):
        raise DirectSyncPushError("credential file must be a JSON object")
    producer_id = str(payload.get("producer_id") or "").strip()
    key_id = str(payload.get("key_id") or "").strip()
    secret = payload.get("secret")
    endpoint_url = str(payload.get("endpoint_url") or "").strip()
    if not producer_id or not key_id or not secret or not endpoint_url:
        raise DirectSyncPushError("credential file is missing producer_id, key_id, secret, or endpoint_url")
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

    reset_count = reset_stale_relay_leases(db_path=config.db_path, now=now or utc_now_text())
    try:
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
    except DirectSyncPushError as exc:
        queue = relay_queue_status(config.db_path)
        status = _write_runtime_status(
            config,
            status="runtime_error",
            queue=queue,
            disk=disk,
            stale_leases_reset=reset_count,
            error_code="direct_sync_runtime_error",
            error_message=str(exc),
        )
        _append_runtime_event(config, "relay_runtime_error", status)
        return status

    result_summary = _result_summary(result)
    queue = relay_queue_status(config.db_path)
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

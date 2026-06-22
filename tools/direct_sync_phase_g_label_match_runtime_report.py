#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Generate local Phase G Label_Match relay runtime evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import subprocess
import sys
from argparse import Namespace
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TOOLS = Path(__file__).resolve().parent
for path in (ROOT, TOOLS):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from direct_sync_push import (  # noqa: E402
    RELAY_STATUS_ACKED,
    RELAY_STATUS_FAILED_PERMANENT,
    RELAY_STATUS_LEASED,
    RELAY_STATUS_OPERATOR_REVIEW,
    RELAY_STATUS_PENDING,
    RELAY_STATUS_RETRY_WAIT,
    ProducerCredentials,
    build_source_file_plan,
    claim_next_relay_batch,
    relay_queue_status,
    upload_source_file,
)
from direct_sync_operator import operator_status, pause_relay, resume_relay, retry_dead_relay_batch  # noqa: E402
from direct_sync_relay_install_pack import build_install_plan  # noqa: E402
from direct_sync_relay_runner import _scan_source_files  # noqa: E402
from direct_sync_runtime import (  # noqa: E402
    DirectSyncRuntimeConfig,
    enqueue_completed_source_file,
    load_credentials_from_json,
    run_relay_once,
)


class FakeResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload

    def json(self) -> dict:
        return self._payload


class EchoAcceptedSession:
    def __init__(self):
        self.calls: list[dict] = []

    def post(self, url, *, data, files, headers, timeout):
        file_name, file_handle, content_type = files["file"]
        metadata = json.loads(data["metadata"])
        self.calls.append(
            {
                "url": url,
                "metadata": data["metadata"],
                "headers": dict(headers),
                "timeout": timeout,
                "file_name": file_name,
                "file_bytes": file_handle.read(),
                "content_type": content_type,
            }
        )
        return FakeResponse(
            200,
            {
                "request_id": f"request-{metadata['client_batch_id']}",
                "client_batch_id": metadata["client_batch_id"],
                "server_source_file_id": (
                    f"{metadata['source_host_id']}/{metadata['producer_role']}/"
                    f"{metadata['stream_name']}/{metadata['relative_path']}"
                ),
                "committed": True,
                "status": "accepted",
                "totals": {"inserted": 1, "replayed": 0, "quarantined": 0, "errors": 0},
            },
        )


class FixedSession:
    def __init__(self, response: FakeResponse):
        self.response = response
        self.calls: list[dict] = []

    def post(self, url, *, data, files, headers, timeout):
        file_name, file_handle, content_type = files["file"]
        self.calls.append(
            {
                "url": url,
                "metadata": data["metadata"],
                "headers": dict(headers),
                "timeout": timeout,
                "file_name": file_name,
                "file_bytes": file_handle.read(),
                "content_type": content_type,
            }
        )
        return self.response


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _make_manifest(tmp_root: Path) -> Path:
    manifest = {
        "schema_version": "producer-onboarding-manifest-v1",
        "pc_identity": {
            "pc_id": "LABEL-PC01",
            "source_host_id": "label-match-phase-g-host",
            "producer_install_id": "install-label-match-phase-g",
        },
        "apps": ["LabelMatch"],
        "streams": [
            {
                "producer_role": "label_match",
                "stream_name": "label_match_events",
                "source_system": "label_match",
                "source_transport": "legacy_packaging_csv",
            }
        ],
        "sync": {"sync_dir": str(tmp_root / "sync")},
        "server": {"health_target": "https://worker.example.invalid/health/ingest"},
    }
    path = tmp_root / "producer_manifest.json"
    _write_json(path, manifest)
    return path


def _source_scope_identity(manifest_path: Path) -> dict:
    manifest_raw = manifest_path.read_bytes()
    manifest = json.loads(manifest_raw.decode("utf-8"))
    stream = manifest["streams"][0]
    source_scope_key = (
        f"{manifest['pc_identity']['source_host_id']}/"
        f"{stream['producer_role']}/{stream['stream_name']}"
    )
    return {
        "source_host_id": manifest["pc_identity"]["source_host_id"],
        "producer_install_id": manifest["pc_identity"]["producer_install_id"],
        "producer_role": stream["producer_role"],
        "stream_name": stream["stream_name"],
        "source_transport": "http_push",
        "manifest_source_transport": stream["source_transport"],
        "manifest_hash": hashlib.sha256(manifest_raw).hexdigest(),
        "source_scope_key": source_scope_key,
        "source_scope_key_sha256": hashlib.sha256(source_scope_key.encode("utf-8")).hexdigest(),
    }


def _runtime_artifact_bindings(runtime_status_path: Path, log_path: Path) -> dict:
    return {
        "status_json_artifact_ref": str(runtime_status_path),
        "status_json_artifact_path": str(runtime_status_path),
        "status_json_artifact_sha256": hashlib.sha256(runtime_status_path.read_bytes()).hexdigest(),
        "redacted_log_artifact_ref": str(log_path),
        "redacted_log_artifact_path": str(log_path),
        "redacted_log_artifact_sha256": hashlib.sha256(log_path.read_bytes()).hexdigest(),
    }


def _bind_evidence_artifact(entry: dict, *, report_path: Path, evidence_name: str) -> None:
    artifact_path = report_path.parent / f"{evidence_name}.artifact.json"
    artifact = {
        "evidence": evidence_name,
        "status": entry["status"],
        "production_ready": False,
        "source_scope_key_sha256": entry.get("source_scope_key_sha256", ""),
        "blocked_reason": entry.get("blocked_reason", ""),
    }
    _write_json(artifact_path, artifact)
    entry["artifact_ref"] = str(artifact_path)
    entry["artifact_path"] = str(artifact_path)
    entry["artifact_sha256"] = hashlib.sha256(artifact_path.read_bytes()).hexdigest()
    entry["artifact_status"] = artifact["status"]


def _runtime_path_boundary_report(install_pack: dict) -> dict:
    return {
        "status": "PASS" if Path(install_pack["program_data_root"]).is_absolute() else "FAIL",
        "scope": "local install-pack program_data_root path boundary dry-run",
        "program_data_root": install_pack["program_data_root"],
    }


def _queue_status_count(queue: dict, status: str) -> int:
    return int((queue.get("counts") or {}).get(status, 0) or 0)


def _flow_runtime_required_metrics(runner: dict, install_pack: dict) -> dict:
    queue = runner.get("queue") or {}
    operator_pause_path_present = bool(install_pack.get("operator_pause_path_present"))
    runner_has_operator_pause = bool(install_pack.get("runner_has_operator_pause"))
    reboot_resume_proof = False
    logoff_resume_proof = False
    sleep_resume_proof = False
    return {
        "operator_pause_path_present": operator_pause_path_present,
        "runner_has_operator_pause": runner_has_operator_pause,
        "reboot_resume_proof": reboot_resume_proof,
        "logoff_resume_proof": logoff_resume_proof,
        "sleep_resume_proof": sleep_resume_proof,
        "accepted_receipt_count": 0,
        "local_acked_queue_count": _queue_status_count(queue, RELAY_STATUS_ACKED),
        "pending_queue_count": _queue_status_count(queue, RELAY_STATUS_PENDING),
        "leased_queue_count": _queue_status_count(queue, RELAY_STATUS_LEASED),
        "retry_wait_count": _queue_status_count(queue, RELAY_STATUS_RETRY_WAIT),
        "failed_queue_count": _queue_status_count(queue, RELAY_STATUS_FAILED_PERMANENT),
        "operator_review_count": _queue_status_count(queue, RELAY_STATUS_OPERATOR_REVIEW),
        "missing_server_receipt_count": 1,
        "runtime_checks": {
            "operator_pause_path_present": operator_pause_path_present,
            "runner_has_operator_pause": runner_has_operator_pause,
            "reboot_resume_proof": reboot_resume_proof,
            "logoff_resume_proof": logoff_resume_proof,
            "sleep_resume_proof": sleep_resume_proof,
            "accepted_receipt_count": 0,
            "local_acked_queue_count": _queue_status_count(queue, RELAY_STATUS_ACKED),
            "missing_server_receipt_count": 1,
            "production_reboot_logoff_sleep_status": "BLOCKED",
            "blocked_reason": "No real producer-PC reboot, logoff, sleep, or server receipt evidence.",
        },
    }


def _flow_runtime_subreports(
    *,
    runner: dict,
    process_kill: dict,
    reboot_recovery: dict,
    disk: dict,
    retry: dict,
    queue_backpressure: dict,
    lost_ack: dict,
    retry_dead_letter: dict,
    operator_status: dict,
    operator_control: dict,
    install_pack: dict,
    source_scan_admission: dict,
    credential_secret_ref: dict,
    lost_ack_blocked_reason: str,
) -> dict:
    return {
        **_flow_runtime_required_metrics(runner, install_pack),
        "relay_state_machine_report": runner,
        "lost_ack_replay_report": {
            "status": "BLOCKED",
            "local_replay_report": lost_ack,
            "blocked_reason": lost_ack_blocked_reason,
        },
        "process_kill_recovery_report": process_kill,
        "reboot_recovery_report": reboot_recovery,
        "disk_pressure_report": disk,
        "retry_wait_report": retry,
        "queue_backpressure_report": queue_backpressure,
        "retry_dead_letter_report": retry_dead_letter,
        "operator_status_report": operator_status,
        "operator_control_report": operator_control,
        "source_scan_install_pack_report": install_pack,
        "runtime_path_boundary_report": _runtime_path_boundary_report(install_pack),
        "source_scan_admission_report": source_scan_admission,
        "credential_secret_ref_report": credential_secret_ref,
    }


def _make_credential(tmp_root: Path) -> Path:
    path = tmp_root / "credential.json"
    _write_json(
        path,
        {
            "producer_id": "producer-label-phase-g",
            "key_id": "key-label-phase-g",
            "secret": "label-phase-g-local-secret",
            "endpoint_url": "https://worker.example.invalid/api/producer-ingest/v1/source-file",
        },
    )
    return path


def _credential_secret_ref_report(tmp_root: Path) -> dict:
    import os

    env_name = "LABEL_PHASE_G_SECRET_REF"
    secret_value = "label-phase-g-secret-ref-fixture"
    credential_path = tmp_root / "credential_secret_ref.json"
    _write_json(
        credential_path,
        {
            "producer_id": "producer-label-phase-g",
            "key_id": "key-label-phase-g",
            "secret_ref": f"env:{env_name}",
            "endpoint_url": "https://worker.example.invalid/api/producer-ingest/v1/source-file",
        },
    )
    previous = os.environ.get(env_name)
    os.environ[env_name] = secret_value
    try:
        credentials = load_credentials_from_json(credential_path)
    finally:
        if previous is None:
            os.environ.pop(env_name, None)
        else:
            os.environ[env_name] = previous
    payload = json.loads(credential_path.read_text(encoding="utf-8-sig"))
    secret_material_field_present = any(key in payload for key in ("secret", "secret_hex", "raw_secret"))
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    ok = (
        credentials.producer_id == "producer-label-phase-g"
        and credentials.key_id == "key-label-phase-g"
        and credentials.secret == secret_value
        and credentials.endpoint_url == "https://worker.example.invalid/api/producer-ingest/v1/source-file"
        and payload.get("secret_ref") == f"env:{env_name}"
        and secret_material_field_present is False
        and secret_value not in serialized
    )
    return {
        "status": "PASS" if ok else "FAIL",
        "scope": "local env secret_ref loader fixture only",
        "credential_path": str(credential_path),
        "secret_ref_scheme": "env",
        "secret_material_field_present": secret_material_field_present,
        "secret_material_value_in_file": secret_value in serialized,
        "production_readback_status": "BLOCKED",
        "blocked_reason": "No real producer-PC wincred:/dpapi: credential bootstrap and readback evidence.",
    }


def _runtime_config(
    tmp_root: Path,
    *,
    name: str,
    min_free_bytes: int = 0,
    max_active_queue_count: int = 0,
    max_active_queue_age_seconds: int = 0,
) -> DirectSyncRuntimeConfig:
    return DirectSyncRuntimeConfig(
        db_path=tmp_root / name / "direct_sync_relay.sqlite3",
        spool_dir=tmp_root / name / "spool",
        producer_manifest_path=tmp_root / "producer_manifest.json",
        credential_path=tmp_root / "credential.json",
        upload_status_dir=tmp_root / name / "upload_status",
        runtime_status_path=tmp_root / name / "runtime_status" / "status.json",
        log_path=tmp_root / name / "logs" / "relay.jsonl",
        min_free_bytes=min_free_bytes,
        retry_base_seconds=1,
        timeout_seconds=5,
        operator_pause_path=tmp_root / name / "control" / "pause.json",
        max_active_queue_count=max_active_queue_count,
        max_active_queue_age_seconds=max_active_queue_age_seconds,
    )


def _write_source_file(tmp_root: Path, *, name: str = "label_match_phase_g.csv") -> Path:
    tmp_root.mkdir(parents=True, exist_ok=True)
    path = tmp_root / name
    path.write_text(
        "timestamp,worker_name,event,details\n"
        "2026-06-22T00:00:00,worker,LABEL_MATCHED,\"{ \"\"product_barcode\"\": \"\"BC-1\"\" }\"\n",
        encoding="utf-8",
    )
    return path


def _artifacts_redacted(config: DirectSyncRuntimeConfig) -> bool:
    status_bytes = Path(config.runtime_status_path).read_bytes()
    log_bytes = Path(config.log_path).read_bytes()
    forbidden = (b"label-phase-g-local-secret", b"X-Producer-Signature", b"PRODUCER-HMAC-SHA256-V1")
    return not any(token in status_bytes or token in log_bytes for token in forbidden)


def _runner_status_log_report(tmp_root: Path) -> dict:
    config = _runtime_config(tmp_root, name="runner")
    source_file = _write_source_file(tmp_root / "runner")
    enqueued = enqueue_completed_source_file(config, source_file_path=source_file)
    session = EchoAcceptedSession()
    status = run_relay_once(config, session=session)
    queue = relay_queue_status(config.db_path)
    ok = (
        enqueued["status"] == "enqueued"
        and status["status"] == "acked"
        and queue["counts"].get(RELAY_STATUS_ACKED) == 1
        and _artifacts_redacted(config)
        and len(session.calls) == 1
    )
    return {
        "status": "PASS" if ok else "FAIL",
        "scope": "local Label_Match CSV queue/status/log runner proof with fixture session",
        "enqueue_status": enqueued["status"],
        "run_status": status["status"],
        "queue": queue,
        "redaction_pass": _artifacts_redacted(config),
        "queue_db_path": str(config.db_path),
        "runtime_status_path": str(config.runtime_status_path),
        "log_path": str(config.log_path),
        **_runtime_artifact_bindings(config.runtime_status_path, config.log_path),
    }


def _stale_lease_report(tmp_root: Path) -> dict:
    config = _runtime_config(tmp_root, name="stale-lease")
    source_file = _write_source_file(tmp_root / "stale-lease")
    enqueue_completed_source_file(config, source_file_path=source_file)
    claimed = claim_next_relay_batch(
        db_path=config.db_path,
        worker_id="previous-process",
        lease_seconds=1,
        now="2099-01-01T00:00:00Z",
    )
    status = run_relay_once(config, session=EchoAcceptedSession(), now="2099-01-01T00:00:02Z")
    queue = relay_queue_status(config.db_path)
    ok = claimed is not None and status["stale_leases_reset"] == 1 and queue["counts"].get(RELAY_STATUS_ACKED) == 1
    return {
        "status": "PASS" if ok else "FAIL",
        "scope": "local expired lease reset after simulated process death",
        "stale_leases_reset": status["stale_leases_reset"],
        "queue": queue,
    }


def _process_kill_recovery_report(tmp_root: Path) -> dict:
    config = _runtime_config(tmp_root, name="process-kill")
    source_file = _write_source_file(tmp_root / "process-kill")
    enqueue_completed_source_file(config, source_file_path=source_file)
    claim_script = f"""
import os
import sys

sys.path.insert(0, {str(ROOT)!r})
from direct_sync_push import claim_next_relay_batch

row = claim_next_relay_batch(
    db_path={str(config.db_path)!r},
    worker_id="killed-process",
    lease_seconds=1,
    now="2099-01-01T00:00:00Z",
)
os._exit(17 if row is not None else 31)
"""
    killed = subprocess.run(
        [sys.executable, "-c", claim_script],
        cwd=str(ROOT),
        check=False,
        capture_output=True,
        text=True,
    )
    with sqlite3.connect(config.db_path) as conn:
        leased_before_reset = conn.execute(
            "SELECT COUNT(*) FROM direct_sync_relay_batches WHERE status = 'leased'"
        ).fetchone()[0]
    status = run_relay_once(config, session=EchoAcceptedSession(), now="2099-01-01T00:00:02Z")
    queue = relay_queue_status(config.db_path)
    ok = (
        killed.returncode == 17
        and leased_before_reset == 1
        and status["stale_leases_reset"] == 1
        and queue["counts"].get(RELAY_STATUS_ACKED) == 1
    )
    return {
        "status": "PASS" if ok else "FAIL",
        "scope": "local subprocess exit after claim proves stale lease recovery without duplicate post",
        "claim_process_exit_code": killed.returncode,
        "leased_before_reset": int(leased_before_reset),
        "stale_leases_reset": status["stale_leases_reset"],
        "queue": queue,
    }


def _disk_pressure_report(tmp_root: Path) -> dict:
    source_file = _write_source_file(tmp_root / "disk")
    normal_config = _runtime_config(tmp_root, name="disk")
    enqueue_completed_source_file(normal_config, source_file_path=source_file)
    blocked_config = _runtime_config(tmp_root, name="disk", min_free_bytes=10**20)
    session = EchoAcceptedSession()
    status = run_relay_once(blocked_config, session=session)
    queue = relay_queue_status(normal_config.db_path)
    ok = status["status"] == "blocked_disk_pressure" and queue["counts"].get(RELAY_STATUS_PENDING) == 1 and not session.calls
    return {
        "status": "PASS" if ok else "FAIL",
        "scope": "local free-space preflight blocks before claim/post",
        "runtime_status": status["status"],
        "queue": queue,
        "post_count": len(session.calls),
    }


def _retry_wait_report(tmp_root: Path) -> dict:
    config = _runtime_config(tmp_root, name="retry")
    source_file = _write_source_file(tmp_root / "retry")
    enqueue_completed_source_file(config, source_file_path=source_file)
    retry_session = FixedSession(
        FakeResponse(
            503,
            {
                "committed": False,
                "retryable": True,
                "error": {"code": "temporary_unavailable", "message": "try later"},
            },
        )
    )
    first = run_relay_once(config, session=retry_session)
    early_success = EchoAcceptedSession()
    second = run_relay_once(config, session=early_success)
    queue = relay_queue_status(config.db_path)
    ok = (
        first["status"] == "retry_wait"
        and second["status"] == "idle"
        and queue["counts"].get(RELAY_STATUS_RETRY_WAIT) == 1
        and len(retry_session.calls) == 1
        and not early_success.calls
    )
    return {
        "status": "PASS" if ok else "FAIL",
        "scope": "local retryable error records retry_wait and prevents early resend",
        "first_status": first["status"],
        "second_status": second["status"],
        "queue": queue,
    }


def _queue_backpressure_report(tmp_root: Path) -> dict:
    config = _runtime_config(tmp_root, name="backpressure")
    source_file = _write_source_file(tmp_root / "backpressure")
    enqueue_completed_source_file(config, source_file_path=source_file)
    blocked_config = DirectSyncRuntimeConfig(
        **{
            **_runtime_config(tmp_root, name="backpressure", max_active_queue_count=1).__dict__,
            "credential_path": tmp_root / "missing_credential.json",
        }
    )
    blocked = enqueue_completed_source_file(blocked_config, source_file_path=source_file)
    drained = run_relay_once(
        _runtime_config(tmp_root, name="backpressure", max_active_queue_count=1),
        session=EchoAcceptedSession(),
    )
    queue = relay_queue_status(config.db_path)
    ok = (
        blocked["status"] == "blocked_queue_backpressure"
        and "active_queue_count_threshold" in blocked.get("queue_backpressure", {}).get("reasons", [])
        and blocked["disk"]["status"] == "not_checked"
        and drained["status"] == "acked"
        and queue["counts"].get(RELAY_STATUS_ACKED) == 1
    )
    return {
        "status": "PASS" if ok else "FAIL",
        "scope": "local active Label_Match relay queue threshold blocks enqueue before credential load while drain remains allowed",
        "blocked_status": blocked["status"],
        "blocked_reasons": blocked.get("queue_backpressure", {}).get("reasons", []),
        "drain_status": drained["status"],
        "queue": queue,
    }


def _lost_ack_replay_report(tmp_root: Path) -> dict:
    config = _runtime_config(tmp_root, name="lost-ack")
    source_file = _write_source_file(tmp_root / "lost-ack")
    enqueue_completed_source_file(config, source_file_path=source_file)
    claimed = claim_next_relay_batch(
        db_path=config.db_path,
        worker_id="crashed-process",
        lease_seconds=1,
        now="2099-01-01T00:00:00Z",
    )
    credentials = ProducerCredentials(
        producer_id="producer-label-phase-g",
        key_id="key-label-phase-g",
        secret="label-phase-g-local-secret",
        endpoint_url="https://worker.example.invalid/api/producer-ingest/v1/source-file",
    )
    plan = build_source_file_plan(
        source_file_path=claimed.spooled_file_path,
        producer_manifest_path=claimed.producer_manifest_path,
        credentials=credentials,
        relative_path=claimed.relative_path,
        client_batch_id=claimed.relay_id,
    )
    committed_but_unacked = EchoAcceptedSession()
    upload = upload_source_file(
        plan,
        credentials,
        session=committed_but_unacked,
        status_dir=tmp_root / "lost-ack" / "crash_status",
    )
    retry_session = EchoAcceptedSession()
    retry = run_relay_once(config, session=retry_session, now="2099-01-01T00:00:02Z")
    first_metadata = json.loads(committed_but_unacked.calls[0]["metadata"]) if committed_but_unacked.calls else {}
    retry_metadata = json.loads(retry_session.calls[0]["metadata"]) if retry_session.calls else {}
    queue = relay_queue_status(config.db_path)
    same_replay_identity = (
        bool(claimed)
        and first_metadata.get("client_batch_id") == retry_metadata.get("client_batch_id") == claimed.relay_id
        and first_metadata.get("idempotency_key") == retry_metadata.get("idempotency_key")
        and first_metadata.get("content_sha256") == retry_metadata.get("content_sha256")
    )
    ok = upload.success and retry["status"] == "acked" and retry["stale_leases_reset"] == 1 and same_replay_identity
    return {
        "status": "PASS" if ok else "FAIL",
        "scope": "local committed-before-local-ack crash simulation with same relay batch retry",
        "same_replay_identity": same_replay_identity,
        "stale_leases_reset": retry["stale_leases_reset"],
        "queue": queue,
    }


def _retry_dead_letter_report(tmp_root: Path) -> dict:
    review_config = _runtime_config(tmp_root, name="operator-review")
    review_source = _write_source_file(tmp_root / "operator-review")
    enqueue_completed_source_file(review_config, source_file_path=review_source)
    review_status = run_relay_once(
        review_config,
        session=FixedSession(
            FakeResponse(
                200,
                {
                    "request_id": "request-operator-review",
                    "client_batch_id": "relay-operator-review",
                    "committed": True,
                    "status": "accepted",
                    "totals": {"inserted": 0, "replayed": 0, "quarantined": 1, "errors": 0},
                },
            )
        ),
    )
    review_queue = relay_queue_status(review_config.db_path)
    review_relay_id = ""
    with sqlite3.connect(review_config.db_path) as conn:
        row = conn.execute("SELECT relay_id FROM direct_sync_relay_batches LIMIT 1").fetchone()
        review_relay_id = row[0] if row else ""

    permanent_config = _runtime_config(tmp_root, name="failed-permanent")
    permanent_source = _write_source_file(tmp_root / "failed-permanent")
    permanent_enqueued = enqueue_completed_source_file(permanent_config, source_file_path=permanent_source)
    permanent_status = run_relay_once(
        permanent_config,
        session=FixedSession(
            FakeResponse(
                400,
                {
                    "committed": False,
                    "retryable": False,
                    "error": {"code": "metadata_invalid", "message": "bad metadata"},
                },
            )
        ),
    )
    permanent_queue = relay_queue_status(permanent_config.db_path)
    retried_permanent = retry_dead_relay_batch(
        db_path=permanent_config.db_path,
        relay_id=permanent_enqueued.get("last_result", {}).get("relay_id", ""),
        operator_id="phase-g-operator",
        reason="local drill retry failed permanent",
        audit_log_path=tmp_root / "retry-dead" / "operator.jsonl",
    )
    blocked_review_retry = retry_dead_relay_batch(
        db_path=review_config.db_path,
        relay_id=review_relay_id,
        operator_id="phase-g-operator",
        reason="local drill review must not retry",
        audit_log_path=tmp_root / "retry-dead" / "operator.jsonl",
    )
    ok = (
        review_status["status"] == "operator_review"
        and review_queue["counts"].get(RELAY_STATUS_OPERATOR_REVIEW) == 1
        and permanent_status["status"] == "failed_permanent"
        and permanent_queue["counts"].get(RELAY_STATUS_FAILED_PERMANENT) == 1
        and retried_permanent["status"] == "PASS"
        and blocked_review_retry["status"] == "BLOCKED"
    )
    return {
        "status": "PASS" if ok else "FAIL",
        "scope": "local committed-conflict operator review and permanent failure dead-letter proof",
        "operator_review_status": review_status["status"],
        "operator_review_queue": review_queue,
        "failed_permanent_status": permanent_status["status"],
        "failed_permanent_queue": permanent_queue,
        "retry_dead_permanent_status": retried_permanent["status"],
        "operator_review_retry_status": blocked_review_retry["status"],
    }


def _operator_control_report(tmp_root: Path) -> dict:
    config = _runtime_config(tmp_root, name="operator-control")
    source_file = _write_source_file(tmp_root / "operator-control")
    audit_log_path = tmp_root / "operator-control" / "logs" / "operator.jsonl"
    paused = pause_relay(
        pause_path=config.operator_pause_path,
        operator_id="phase-g-operator",
        reason="local drill pause",
        audit_log_path=audit_log_path,
    )
    paused_enqueue = enqueue_completed_source_file(config, source_file_path=source_file)
    paused_run = run_relay_once(config, session=EchoAcceptedSession())
    resumed = resume_relay(
        pause_path=config.operator_pause_path,
        operator_id="phase-g-operator",
        reason="local drill resume",
        audit_log_path=audit_log_path,
    )
    enqueued = enqueue_completed_source_file(config, source_file_path=source_file)
    failed = run_relay_once(
        config,
        session=FixedSession(
            FakeResponse(
                400,
                {
                    "committed": False,
                    "retryable": False,
                    "error": {"code": "metadata_invalid", "message": "bad metadata"},
                },
            )
        ),
    )
    relay_id = str(enqueued.get("last_result", {}).get("relay_id") or "")
    retried = retry_dead_relay_batch(
        db_path=config.db_path,
        relay_id=relay_id,
        operator_id="phase-g-operator",
        reason="local drill retry after permanent failure",
        audit_log_path=audit_log_path,
    )
    acked = run_relay_once(config, session=EchoAcceptedSession())
    status_report = operator_status(db_path=config.db_path, pause_path=config.operator_pause_path)
    audit_bytes = Path(audit_log_path).read_bytes()
    forbidden = (b"label-phase-g-local-secret", b"X-Producer-Signature", b"PRODUCER-HMAC-SHA256-V1")
    audit_redacted = not any(token in audit_bytes for token in forbidden)
    ok = (
        paused["status"] == "PASS"
        and paused_enqueue["status"] == "paused_by_operator"
        and paused_run["status"] == "paused_by_operator"
        and resumed["status"] == "PASS"
        and failed["status"] == "failed_permanent"
        and retried["status"] == "PASS"
        and acked["status"] == "acked"
        and status_report["queue"]["counts"].get(RELAY_STATUS_ACKED) == 1
        and audit_redacted
    )
    return {
        "status": "PASS" if ok else "FAIL",
        "scope": "local operator pause/resume/status/retry-dead proof with fixture relay queue",
        "pause_status": paused["status"],
        "paused_enqueue_status": paused_enqueue["status"],
        "paused_run_status": paused_run["status"],
        "resume_status": resumed["status"],
        "retry_dead_status": retried["status"],
        "final_run_status": acked["status"],
        "operator_status": status_report,
        "audit_log_path": str(audit_log_path),
        "audit_redaction_pass": audit_redacted,
    }


def _install_pack_dry_run_report(tmp_root: Path) -> dict:
    plan = build_install_plan(
        Namespace(
            app_root=str(ROOT),
            python_exe=sys.executable,
            program_data_root=str(tmp_root / "ProgramData" / "KMTech" / "DirectSync" / "label_match"),
            producer_manifest_path=str(tmp_root / "producer_manifest.json"),
            credential_path=str(tmp_root / "credential.json"),
            task_name="direct-sync-relay-label-match",
            minute_interval=1,
            min_free_bytes=512 * 1024 * 1024,
            scan_source_dir=str(tmp_root / "sync"),
            source_glob=["포장실작업이벤트로그_*.csv"],
            max_enqueue_files=100,
            max_active_queue_count=1000,
            max_active_queue_age_seconds=24 * 60 * 60,
            apply=False,
            uninstall=False,
            confirm_production_install=False,
        )
    )
    serialized = json.dumps(plan, ensure_ascii=False, sort_keys=True)
    ok = (
        plan["status"] == "DRY_RUN"
        and plan["scheduled_task_create_command"][0] == "schtasks.exe"
        and "direct_sync_relay_runner.py" in " ".join(plan["runner_command"])
        and "--scan-source-dir" in plan["runner_command"]
        and "--operator-pause-path" in plan["runner_command"]
        and "--max-active-queue-count" in plan["runner_command"]
        and "--max-active-queue-age-seconds" in plan["runner_command"]
        and plan["source_scan"]["enabled"] is True
        and plan["backpressure"]["max_active_queue_count"] == 1000
        and plan["backpressure"]["max_active_queue_age_seconds"] == 24 * 60 * 60
        and "label-phase-g-local-secret" not in serialized
        and plan["secret_redaction"]["raw_secret_in_report"] is False
    )
    return {
        "status": "PASS" if ok else "FAIL",
        "scope": "local scheduled-task install pack dry-run only",
        "task_name": plan["task_name"],
        "program_data_root": plan["program_data_root"],
        "source_scan": plan["source_scan"],
        "backpressure": plan["backpressure"],
        "operator_pause_path": plan["runtime_paths"].get("operator_pause_path", ""),
        "operator_pause_path_present": bool(plan["runtime_paths"].get("operator_pause_path")),
        "runner_has_operator_pause": "--operator-pause-path" in plan["runner_command"],
        "runner_command": plan["runner_command"],
        "runner_script": plan["runner_script"],
        "secret_redaction": plan["secret_redaction"],
    }


def _source_scan_admission_report(tmp_root: Path) -> dict:
    scan_dir = tmp_root / "source_scan_admission"
    scan_dir.mkdir(parents=True, exist_ok=True)
    allowed_file = scan_dir / "포장실작업이벤트로그_admission.csv"
    ignored_file = scan_dir / "unrelated.csv"
    nested_dir = scan_dir / "nested"
    nested_dir.mkdir(exist_ok=True)
    nested_allowed = nested_dir / "포장실작업이벤트로그_nested.csv"
    allowed_file.write_text("event_id,status\nLM-ADMIT-1,ok\n", encoding="utf-8")
    ignored_file.write_text("event_id,status\nLM-IGNORE-1,ok\n", encoding="utf-8")
    nested_allowed.write_text("event_id,status\nLM-NESTED-1,ok\n", encoding="utf-8")

    selected = _scan_source_files(str(scan_dir), ["*.csv"], 100)
    recursive_rejected = False
    path_rejected = False
    try:
        _scan_source_files(str(scan_dir), ["**/*.csv"], 100)
    except SystemExit:
        recursive_rejected = True
    try:
        _scan_source_files(str(scan_dir), ["nested/*.csv"], 100)
    except SystemExit:
        path_rejected = True

    selected_names = [path.name for path in selected]
    ok = selected_names == [allowed_file.name] and recursive_rejected and path_rejected
    return {
        "status": "PASS" if ok else "FAIL",
        "scope": "local source scan admission fixture only",
        "approved_file_family": "포장실작업이벤트로그_*.csv",
        "broad_glob_selected_files": selected_names,
        "ignored_file_selected": ignored_file.name in selected_names,
        "nested_file_selected": nested_allowed.name in selected_names,
        "recursive_glob_rejected": recursive_rejected,
        "path_glob_rejected": path_rejected,
    }


def build_report(tmp_root: Path, report_path: Path) -> dict:
    manifest_path = _make_manifest(tmp_root)
    source_identity = _source_scope_identity(manifest_path)
    credential_path = _make_credential(tmp_root)
    runner = _runner_status_log_report(tmp_root)
    stale_lease = _stale_lease_report(tmp_root)
    process_kill = _process_kill_recovery_report(tmp_root)
    disk = _disk_pressure_report(tmp_root)
    retry = _retry_wait_report(tmp_root)
    queue_backpressure = _queue_backpressure_report(tmp_root)
    lost_ack = _lost_ack_replay_report(tmp_root)
    retry_dead_letter = _retry_dead_letter_report(tmp_root)
    operator_control = _operator_control_report(tmp_root)
    install_pack = _install_pack_dry_run_report(tmp_root)
    source_scan_admission = _source_scan_admission_report(tmp_root)
    credential_secret_ref = _credential_secret_ref_report(tmp_root)
    reboot_recovery = {
        "status": "BLOCKED",
        "blocked_reason": "No real Windows scheduled task/service reboot, logoff, or sleep/resume evidence.",
    }
    operator_status_summary = {
        "status": runner["status"],
        "scope": "local generated runtime status JSON and redacted JSONL relay log",
    }
    local_pass = all(
        item["status"] == "PASS"
        for item in (
            runner,
            stale_lease,
            process_kill,
            disk,
            retry,
            queue_backpressure,
            lost_ack,
            retry_dead_letter,
            operator_control,
            install_pack,
            source_scan_admission,
            credential_secret_ref,
        )
    )
    report = {
        "report_version": "direct-sync-phase-g-label-match-runtime-v1",
        "status": "BLOCKED" if local_pass else "FAIL",
        "production_ready": False,
        "tmp_root": str(tmp_root),
        "producer_manifest_path": str(manifest_path),
        "credential_path": str(credential_path),
        "local_contract_status": "PASS" if local_pass else "FAIL",
        "label_match_runtime_relay_report": {
            "status": "BLOCKED" if local_pass else "FAIL",
            **source_identity,
            "flow": "LabelMatch",
            "producer_repo": "Label_Match",
            "task_or_service_name": install_pack["task_name"],
            "task_or_service_installed": False,
            "runtime_kind": "scheduled_task",
            "queue_db_path": runner["queue_db_path"],
            "service_task_status": "BLOCKED",
            "status_log_status": runner["status"],
            "reboot_logoff_sleep_status": "BLOCKED",
            "status_json_artifact_ref": runner["status_json_artifact_ref"],
            "status_json_artifact_path": runner["status_json_artifact_path"],
            "status_json_artifact_sha256": runner["status_json_artifact_sha256"],
            "redacted_log_artifact_ref": runner["redacted_log_artifact_ref"],
            "redacted_log_artifact_path": runner["redacted_log_artifact_path"],
            "redacted_log_artifact_sha256": runner["redacted_log_artifact_sha256"],
            **_flow_runtime_subreports(
                runner=runner,
                process_kill=process_kill,
                reboot_recovery=reboot_recovery,
                disk=disk,
                retry=retry,
                queue_backpressure=queue_backpressure,
                lost_ack=lost_ack,
                retry_dead_letter=retry_dead_letter,
                operator_status=operator_status_summary,
                operator_control=operator_control,
                install_pack=install_pack,
                source_scan_admission=source_scan_admission,
                credential_secret_ref=credential_secret_ref,
                lost_ack_blocked_reason="No real server committed-but-local-ack-lost replay drill from a Label_Match producer PC.",
            ),
            "local_runner_status_log_report": runner,
            "blocked_reason": "No real Label_Match producer-PC scheduled task/service run or production direct receipts.",
        },
        "operator_status_report": operator_status_summary,
        "stale_lease_recovery_report": stale_lease,
        "process_kill_recovery_report": process_kill,
        "disk_pressure_report": disk,
        "retry_wait_report": retry,
        "queue_backpressure_report": queue_backpressure,
        "retry_dead_letter_report": retry_dead_letter,
        "operator_control_report": operator_control,
        "source_scan_admission_report": source_scan_admission,
        "credential_secret_ref_report": credential_secret_ref,
        "lost_ack_replay_report": {
            "status": "BLOCKED",
            "local_replay_report": lost_ack,
            "blocked_reason": "No real server committed-but-local-ack-lost replay drill from a Label_Match producer PC.",
        },
        "reboot_recovery_report": reboot_recovery,
        "reboot_logoff_sleep_report": {
            "status": "BLOCKED",
            "blocked_reason": "No real Windows scheduled task/service reboot, logoff, or sleep/resume evidence.",
        },
        "production_install_pack_report": {
            "status": "BLOCKED",
            "local_dry_run_report": install_pack,
            "blocked_reason": "No approved Label_Match production install, task/service registration, smoke test, uninstall, or restore evidence.",
        },
    }
    _bind_evidence_artifact(
        report["label_match_runtime_relay_report"],
        report_path=report_path,
        evidence_name="label_match_runtime_relay_report",
    )
    _write_json(report_path, report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase G local Label_Match runtime evidence.")
    parser.add_argument("--tmp-root", required=True)
    parser.add_argument("--report-path", required=True)
    args = parser.parse_args()
    tmp_root = Path(args.tmp_root).resolve()
    tmp_root.mkdir(parents=True, exist_ok=True)
    report_path = Path(args.report_path).resolve()
    try:
        report_path.relative_to(tmp_root)
    except ValueError as exc:
        raise SystemExit(f"report_path_outside_tmp_root={report_path}") from exc
    if report_path.exists():
        raise SystemExit(f"report_path_exists={report_path}")
    report = build_report(tmp_root, report_path)
    print(f"phase_g_label_match_runtime_report={report_path}")
    if report["status"] == "BLOCKED":
        return 2
    if report["status"] == "PASS":
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

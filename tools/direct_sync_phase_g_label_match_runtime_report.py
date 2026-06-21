#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Generate local Phase G Label_Match relay runtime evidence."""

from __future__ import annotations

import argparse
import json
import sqlite3
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
    RELAY_STATUS_OPERATOR_REVIEW,
    RELAY_STATUS_PENDING,
    RELAY_STATUS_RETRY_WAIT,
    ProducerCredentials,
    build_source_file_plan,
    claim_next_relay_batch,
    relay_queue_status,
    upload_source_file,
)
from direct_sync_relay_install_pack import build_install_plan  # noqa: E402
from direct_sync_runtime import DirectSyncRuntimeConfig, enqueue_completed_source_file, run_relay_once  # noqa: E402


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


def _runtime_config(tmp_root: Path, *, name: str, min_free_bytes: int = 0) -> DirectSyncRuntimeConfig:
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
        "runtime_status_path": str(config.runtime_status_path),
        "log_path": str(config.log_path),
    }


def _stale_lease_report(tmp_root: Path) -> dict:
    config = _runtime_config(tmp_root, name="stale-lease")
    source_file = _write_source_file(tmp_root / "stale-lease")
    enqueue_completed_source_file(config, source_file_path=source_file)
    claimed = claim_next_relay_batch(
        db_path=config.db_path,
        worker_id="previous-process",
        lease_seconds=1,
        now="2026-06-22T00:00:00Z",
    )
    status = run_relay_once(config, session=EchoAcceptedSession(), now="2026-06-22T00:00:02Z")
    queue = relay_queue_status(config.db_path)
    ok = claimed is not None and status["stale_leases_reset"] == 1 and queue["counts"].get(RELAY_STATUS_ACKED) == 1
    return {
        "status": "PASS" if ok else "FAIL",
        "scope": "local expired lease reset after simulated process death",
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


def _lost_ack_replay_report(tmp_root: Path) -> dict:
    config = _runtime_config(tmp_root, name="lost-ack")
    source_file = _write_source_file(tmp_root / "lost-ack")
    enqueue_completed_source_file(config, source_file_path=source_file)
    claimed = claim_next_relay_batch(
        db_path=config.db_path,
        worker_id="crashed-process",
        lease_seconds=1,
        now="2026-06-22T00:00:00Z",
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
    retry = run_relay_once(config, session=retry_session, now="2026-06-22T00:00:02Z")
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

    permanent_config = _runtime_config(tmp_root, name="failed-permanent")
    permanent_source = _write_source_file(tmp_root / "failed-permanent")
    enqueue_completed_source_file(permanent_config, source_file_path=permanent_source)
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
    ok = (
        review_status["status"] == "operator_review"
        and review_queue["counts"].get(RELAY_STATUS_OPERATOR_REVIEW) == 1
        and permanent_status["status"] == "failed_permanent"
        and permanent_queue["counts"].get(RELAY_STATUS_FAILED_PERMANENT) == 1
    )
    return {
        "status": "PASS" if ok else "FAIL",
        "scope": "local committed-conflict operator review and permanent failure dead-letter proof",
        "operator_review_status": review_status["status"],
        "operator_review_queue": review_queue,
        "failed_permanent_status": permanent_status["status"],
        "failed_permanent_queue": permanent_queue,
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
        and "label-phase-g-local-secret" not in serialized
        and plan["secret_redaction"]["raw_secret_in_report"] is False
    )
    return {
        "status": "PASS" if ok else "FAIL",
        "scope": "local scheduled-task install pack dry-run only",
        "task_name": plan["task_name"],
        "program_data_root": plan["program_data_root"],
        "runner_script": plan["runner_script"],
        "secret_redaction": plan["secret_redaction"],
    }


def build_report(tmp_root: Path, report_path: Path) -> dict:
    manifest_path = _make_manifest(tmp_root)
    credential_path = _make_credential(tmp_root)
    runner = _runner_status_log_report(tmp_root)
    stale_lease = _stale_lease_report(tmp_root)
    disk = _disk_pressure_report(tmp_root)
    retry = _retry_wait_report(tmp_root)
    lost_ack = _lost_ack_replay_report(tmp_root)
    retry_dead_letter = _retry_dead_letter_report(tmp_root)
    install_pack = _install_pack_dry_run_report(tmp_root)
    local_pass = all(
        item["status"] == "PASS"
        for item in (runner, stale_lease, disk, retry, lost_ack, retry_dead_letter, install_pack)
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
            "local_runner_status_log_report": runner,
            "blocked_reason": "No real Label_Match producer-PC scheduled task/service run or production direct receipts.",
        },
        "operator_status_report": {
            "status": runner["status"],
            "scope": "local generated runtime status JSON and redacted JSONL relay log",
        },
        "stale_lease_recovery_report": stale_lease,
        "disk_pressure_report": disk,
        "retry_wait_report": retry,
        "retry_dead_letter_report": retry_dead_letter,
        "lost_ack_replay_report": {
            "status": "BLOCKED",
            "local_replay_report": lost_ack,
            "blocked_reason": "No real server committed-but-local-ack-lost replay drill from a Label_Match producer PC.",
        },
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

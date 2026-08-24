#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Generate local Phase G Label_Match relay runtime evidence."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import sqlite3
import subprocess
import sys
from argparse import Namespace
from dataclasses import replace
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
TOOLS = Path(__file__).resolve().parent
for path in (ROOT, TOOLS):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from direct_sync_push import (  # noqa: E402
    DEFAULT_ENDPOINT_PATH,
    RELAY_STATUS_ACKED,
    RELAY_STATUS_FAILED_PERMANENT,
    RELAY_STATUS_LEASED,
    RELAY_STATUS_OPERATOR_REVIEW,
    RELAY_STATUS_PENDING,
    RELAY_STATUS_RETRY_WAIT,
    ProducerCredentials,
    build_source_file_plan,
    canonical_json,
    claim_next_relay_batch,
    relay_queue_status,
    upload_source_file,
)
from direct_sync_operator import operator_status, pause_relay, resume_relay, retry_dead_relay_batch  # noqa: E402
from direct_sync_relay_install_pack import build_install_plan  # noqa: E402
from direct_sync_relay_runner import _scan_source_files  # noqa: E402
import producer_runtime_client as runtime_client  # noqa: E402
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


def _json_copy(value: dict) -> dict:
    return json.loads(json.dumps(value, ensure_ascii=False, sort_keys=True))


class RuntimeLeaseFixtureAuthority:
    """Stateful server-side model for lease rotation and ingest replay."""

    def __init__(self, *, force_renew_on_issue: bool = False):
        self.force_renew_on_issue = bool(force_renew_on_issue)
        self.producer_id = "producer-label-phase-g"
        self.key_id = "key-label-phase-g"
        self.secret = b"label-phase-g-local-secret"
        self.producer_install_id = "install-label-match-phase-g"
        self.lease_id = "lease-label-match-phase-g-fixture"
        self.runtime_instance_id = ""
        self.public_jwk_thumbprint = ""
        self.fence = 1
        self.current_request_token = ""
        self.current_request_sequence = 0
        self.expires_at = ""
        self.issue_count = 0
        self.renew_count = 0
        self.consume_count = 0
        self.exact_replay_count = 0
        self.mismatched_replay_count = 0
        self._token_serial = 0
        self._lease_anchors: dict[str, tuple[str, dict]] = {}
        self._source_receipts: dict[str, tuple[str, int, dict]] = {}

    def _next_token(self) -> str:
        self._token_serial += 1
        return f"T{self._token_serial:042d}"

    def _assert_signed_lease_request(self, request: dict, headers: dict) -> None:
        assert headers.get("X-Producer-Id") == self.producer_id
        assert headers.get("X-Producer-Key-Id") == self.key_id
        canonical = runtime_client._canonical_request(
            timestamp=headers["X-Producer-Timestamp"],
            nonce=headers["X-Producer-Nonce"],
            producer_id=headers["X-Producer-Id"],
            key_id=headers["X-Producer-Key-Id"],
            body=request,
        )
        expected = hmac.new(self.secret, canonical.encode("utf-8"), hashlib.sha256).hexdigest()
        assert hmac.compare_digest(expected, str(headers.get("X-Producer-Signature") or ""))

    def acquire(self, request: dict, headers: dict) -> FakeResponse:
        self._assert_signed_lease_request(request, headers)
        assert request.get("contract_version") == runtime_client.CONTRACT_VERSION
        issue_key = str(request["issue_idempotency_key"])
        request_fingerprint = hashlib.sha256(canonical_json(request).encode("utf-8")).hexdigest()
        anchored = self._lease_anchors.get(issue_key)
        if anchored is not None:
            assert anchored[0] == request_fingerprint
            return FakeResponse(200, _json_copy(anchored[1]))

        runtime_id = str(request["runtime_instance_id"])
        thumbprint = runtime_client._jwk_thumbprint(request["public_jwk"])
        renewal_fields = ("runtime_fence", "runtime_request_token", "runtime_request_sequence")
        renewal_field_count = sum(field_name in request for field_name in renewal_fields)
        assert renewal_field_count in {0, len(renewal_fields)}
        renewing = renewal_field_count == len(renewal_fields)
        if renewing:
            renewal_proof_valid = (
                self.runtime_instance_id == runtime_id
                and self.public_jwk_thumbprint == thumbprint
                and request["runtime_fence"] == self.fence
                and request["runtime_request_token"] == self.current_request_token
                and request["runtime_request_sequence"] == self.current_request_sequence
            )
            if not renewal_proof_valid:
                return FakeResponse(
                    409,
                    {
                        "ok": False,
                        "status": "OPERATOR_REVIEW",
                        "error": {
                            "code": "STALE_RUNTIME_REQUEST_TOKEN",
                            "message": "runtime renewal proof does not match current authority",
                        },
                    },
                )
            self.renew_count += 1
            operation = "renewed"
            next_sequence = self.current_request_sequence + 1
            expires_at = "2099-08-06T00:00:00Z"
        else:
            assert not self.runtime_instance_id
            self.runtime_instance_id = runtime_id
            self.public_jwk_thumbprint = thumbprint
            self.issue_count += 1
            operation = "issued"
            next_sequence = 1
            expires_at = (
                "2099-01-01T00:00:30Z"
                if self.force_renew_on_issue
                else "2099-08-06T00:00:00Z"
            )

        self.current_request_token = self._next_token()
        self.current_request_sequence = next_sequence
        self.expires_at = expires_at
        grant = {
            "ok": True,
            "status": "ACTIVE",
            "contract_version": runtime_client.CONTRACT_VERSION,
            "operation": operation,
            "lease_id": self.lease_id,
            "producer_install_id": self.producer_install_id,
            "runtime_instance_id": runtime_id,
            "public_jwk_thumbprint": thumbprint,
            "issue_idempotency_key": issue_key,
            "fence": self.fence,
            "issued_at": "2099-01-01T00:00:00Z",
            "expires_at": expires_at,
            "next_request_token": self.current_request_token,
            "next_request_sequence": self.current_request_sequence,
        }
        self._lease_anchors[issue_key] = (request_fingerprint, _json_copy(grant))
        return FakeResponse(200, grant)

    @staticmethod
    def _source_fingerprint(metadata: dict, file_bytes: bytes) -> str:
        digest = hashlib.sha256()
        digest.update(canonical_json(metadata).encode("utf-8"))
        digest.update(b"\0")
        digest.update(file_bytes)
        return digest.hexdigest()

    def source_response(
        self,
        *,
        metadata: dict,
        file_bytes: bytes,
        response: FakeResponse,
    ) -> FakeResponse:
        payload = _json_copy(response.json())
        replay_key = str(metadata.get("idempotency_key") or metadata.get("client_batch_id") or "")
        assert replay_key
        request_fingerprint = self._source_fingerprint(metadata, file_bytes)
        stored = self._source_receipts.get(replay_key)
        if stored is not None:
            stored_fingerprint, stored_status_code, stored_payload = stored
            if stored_fingerprint != request_fingerprint:
                self.mismatched_replay_count += 1
                return FakeResponse(
                    409,
                    {
                        "committed": False,
                        "retryable": False,
                        "status": "operator_review",
                        "error": {
                            "code": "STALE_RUNTIME_REQUEST_TOKEN",
                            "message": "lost-ACK replay request changed",
                        },
                    },
                )
            self.exact_replay_count += 1
            return FakeResponse(stored_status_code, _json_copy(stored_payload))

        if payload.get("committed") is not True:
            return FakeResponse(response.status_code, payload)

        try:
            metadata_thumbprint = runtime_client._jwk_thumbprint(metadata["runtime_public_jwk"])
        except (KeyError, TypeError, ValueError):
            metadata_thumbprint = ""
        consume_proof_valid = (
            metadata.get("producer_install_id") == self.producer_install_id
            and metadata.get("runtime_instance_id") == self.runtime_instance_id
            and metadata_thumbprint == self.public_jwk_thumbprint
            and metadata.get("runtime_fence") == self.fence
            and metadata.get("runtime_request_token") == self.current_request_token
            and metadata.get("runtime_request_sequence") == self.current_request_sequence
        )
        if not consume_proof_valid:
            return FakeResponse(
                409,
                {
                    "committed": False,
                    "retryable": False,
                    "status": "operator_review",
                    "error": {
                        "code": "STALE_RUNTIME_REQUEST_TOKEN",
                        "message": "runtime consume proof does not match current authority",
                    },
                },
            )

        self.current_request_token = self._next_token()
        self.current_request_sequence += 1
        self.consume_count += 1
        payload["producer_install_id"] = self.producer_install_id
        payload["runtime_lease"] = {
            "contract_version": runtime_client.CONTRACT_VERSION,
            "validation_status": "consumed",
            "lease_id": self.lease_id,
            "fence": self.fence,
            "next_request_token": self.current_request_token,
            "next_request_sequence": self.current_request_sequence,
            "expires_at": self.expires_at,
        }
        self._source_receipts[replay_key] = (
            request_fingerprint,
            response.status_code,
            _json_copy(payload),
        )
        self._lease_anchors.clear()
        return FakeResponse(response.status_code, payload)


class RuntimeLeaseFixtureSession:
    def __init__(self, *, authority: RuntimeLeaseFixtureAuthority | None = None):
        self.authority = authority or RuntimeLeaseFixtureAuthority()
        self.lease_calls: list[dict] = []
        self.calls: list[dict] = []
        self.source_responses: list[dict] = []

    def post(self, url, *, data, files=None, headers, timeout, allow_redirects=False):
        if str(url).endswith(runtime_client.ENDPOINT_PATH):
            request = json.loads(bytes(data).decode("utf-8"))
            self.lease_calls.append({"request": request, "headers": dict(headers)})
            return self.authority.acquire(request, dict(headers))
        assert files is not None
        file_name, file_handle, content_type = files["file"]
        metadata = json.loads(data["metadata"])
        file_bytes = file_handle.read()
        self.calls.append(
            {
                "url": url,
                "metadata": data["metadata"],
                "headers": dict(headers),
                "timeout": timeout,
                "allow_redirects": allow_redirects,
                "file_name": file_name,
                "file_bytes": file_bytes,
                "content_type": content_type,
            }
        )
        response = self.authority.source_response(
            metadata=metadata,
            file_bytes=file_bytes,
            response=self._source_response(metadata),
        )
        self.source_responses.append(_json_copy(response.json()))
        return response

    def _source_response(self, metadata: dict) -> FakeResponse:
        raise NotImplementedError


class EchoAcceptedSession(RuntimeLeaseFixtureSession):
    def _source_response(self, metadata: dict) -> FakeResponse:
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
                "retryable": False,
                "next_retry_after": None,
                "totals": {"inserted": 1, "replayed": 0, "quarantined": 0, "errors": 0},
            },
        )


class FixedSession(RuntimeLeaseFixtureSession):
    def __init__(
        self,
        response: FakeResponse,
        *,
        authority: RuntimeLeaseFixtureAuthority | None = None,
    ):
        super().__init__(authority=authority)
        self.response = response

    def _source_response(self, metadata: dict) -> FakeResponse:
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
        "credential_secret_ref_report": entry.get("credential_secret_ref_report"),
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


def _flow_secret_scan_report(runner: dict, install_pack: dict, credential_secret_ref: dict) -> dict:
    install_secret_redaction = install_pack.get("secret_redaction") or {}
    checks = {
        "runner_artifacts_redacted": runner.get("redaction_pass") is True,
        "install_pack_raw_secret_in_report": install_secret_redaction.get("raw_secret_in_report") is False,
        "credential_secret_material_field_present": credential_secret_ref.get("secret_material_field_present") is False,
        "credential_secret_material_value_in_file": credential_secret_ref.get("secret_material_value_in_file") is False,
    }
    return {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "scope": "local runtime report/artifact secret redaction scan",
        **checks,
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
        "secret_scan_report": _flow_secret_scan_report(runner, install_pack, credential_secret_ref),
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
    endpoint_url = "https://worker.example.invalid/api/producer-ingest/v1/source-file"
    credential_path = tmp_root / "credential_secret_ref.json"
    _write_json(
        credential_path,
        {
            "producer_id": "producer-label-phase-g",
            "key_id": "key-label-phase-g",
            "secret_ref": f"env:{env_name}",
            "endpoint_url": endpoint_url,
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
        and credentials.endpoint_url == endpoint_url
        and payload.get("secret_ref") == f"env:{env_name}"
        and secret_material_field_present is False
        and secret_value not in serialized
    )
    parsed_endpoint = urlparse(credentials.endpoint_url)
    endpoint_transport_ok = (
        parsed_endpoint.scheme == "https"
        and parsed_endpoint.path == DEFAULT_ENDPOINT_PATH
        and not parsed_endpoint.query
        and not parsed_endpoint.fragment
        and not parsed_endpoint.username
        and not parsed_endpoint.password
    )
    return {
        "status": "PASS" if ok else "FAIL",
        "scope": "local env secret_ref loader fixture only",
        "credential_path": str(credential_path),
        "secret_ref_scheme": "env",
        "secret_material_field_present": secret_material_field_present,
        "secret_material_value_in_file": secret_value in serialized,
        "endpoint_transport_report": {
            "status": "PASS" if endpoint_transport_ok else "FAIL",
            "endpoint_scheme": parsed_endpoint.scheme,
            "endpoint_path": parsed_endpoint.path,
            "endpoint_url_sha256": hashlib.sha256(credentials.endpoint_url.encode("utf-8")).hexdigest(),
            "endpoint_host_sha256": hashlib.sha256(str(parsed_endpoint.hostname or "").encode("utf-8")).hexdigest(),
            "query_or_fragment_present": bool(parsed_endpoint.query or parsed_endpoint.fragment),
            "userinfo_present": bool(parsed_endpoint.username or parsed_endpoint.password),
        },
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
        and len(session.lease_calls) == 1
    )
    return {
        "status": "PASS" if ok else "FAIL",
        "scope": "local Label_Match CSV queue/status/log runner proof with fixture session",
        "enqueue_status": enqueued["status"],
        "run_status": status["status"],
        "runtime_lease_post_count": len(session.lease_calls),
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
    authority = RuntimeLeaseFixtureAuthority(force_renew_on_issue=True)
    session = EchoAcceptedSession(authority=authority)
    runtime_preparation = runtime_client.prepare_runtime_metadata(
        db_path=config.db_path,
        relay_id=claimed.relay_id,
        metadata=plan.metadata,
        credentials=credentials,
        expected_lease_owner=claimed.lease_owner,
        expected_attempt_count=claimed.attempt_count,
        runtime_fencing_policy=claimed.runtime_fencing_policy,
        session=session,
        timeout=config.timeout_seconds,
        now="2099-01-01T00:00:00Z",
    )
    prepared_metadata = dict(runtime_preparation.metadata or {})
    with sqlite3.connect(config.db_path) as conn:
        row = conn.execute(
            "SELECT metadata_json FROM direct_sync_relay_batches WHERE relay_id=?",
            (claimed.relay_id,),
        ).fetchone()
    persisted_metadata = json.loads(str(row[0])) if row and row[0] else {}
    upload = None
    if prepared_metadata:
        upload = upload_source_file(
            replace(plan, metadata=prepared_metadata),
            credentials,
            session=session,
            status_dir=tmp_root / "lost-ack" / "crash_status",
        )
    first_runtime_lease_post_count = len(session.lease_calls)
    first_source_post_count = len(session.calls)
    retry = run_relay_once(config, session=session, now="2099-01-01T00:00:02Z")
    retry_runtime_lease_post_count = len(session.lease_calls) - first_runtime_lease_post_count
    retry_source_post_count = len(session.calls) - first_source_post_count
    first_metadata = json.loads(session.calls[0]["metadata"]) if first_source_post_count == 1 else {}
    retry_metadata = (
        json.loads(session.calls[first_source_post_count]["metadata"])
        if retry_source_post_count == 1
        else {}
    )
    queue = relay_queue_status(config.db_path)
    runtime_fields = set(runtime_client.METADATA_FIELDS)
    first_post_fenced = runtime_fields.issubset(first_metadata)
    retry_post_fenced = runtime_fields.issubset(retry_metadata)
    same_replay_identity = (
        bool(claimed)
        and first_metadata.get("client_batch_id") == retry_metadata.get("client_batch_id") == claimed.relay_id
        and first_metadata.get("idempotency_key") == retry_metadata.get("idempotency_key")
        and first_metadata.get("content_sha256") == retry_metadata.get("content_sha256")
    )
    exact_fenced_request_replayed = (
        first_post_fenced
        and retry_post_fenced
        and first_metadata == retry_metadata == persisted_metadata
    )
    runtime_token_replayed_exactly = (
        first_post_fenced
        and first_metadata.get("runtime_request_token") == retry_metadata.get("runtime_request_token")
    )
    runtime_sequence_replayed_exactly = (
        first_post_fenced
        and first_metadata.get("runtime_request_sequence") == retry_metadata.get("runtime_request_sequence")
    )
    runtime_fence_replayed_exactly = (
        first_post_fenced
        and first_metadata.get("runtime_fence") == retry_metadata.get("runtime_fence")
    )
    first_transport_nonce = (
        session.calls[0]["headers"].get("X-Producer-Nonce")
        if first_source_post_count == 1
        else ""
    )
    retry_transport_nonce = (
        session.calls[first_source_post_count]["headers"].get("X-Producer-Nonce")
        if retry_source_post_count == 1
        else ""
    )
    fresh_transport_nonce = bool(
        first_transport_nonce
        and retry_transport_nonce
        and first_transport_nonce != retry_transport_nonce
    )
    first_response = session.source_responses[0] if first_source_post_count == 1 else {}
    retry_response = (
        session.source_responses[first_source_post_count]
        if retry_source_post_count == 1
        else {}
    )
    exact_replay_response = bool(first_response) and canonical_json(first_response) == canonical_json(retry_response)
    exact_runtime_receipt = (
        isinstance(first_response.get("runtime_lease"), dict)
        and canonical_json(first_response["runtime_lease"])
        == canonical_json(retry_response.get("runtime_lease") or {})
    )
    lease_operations = [
        "renew" if "runtime_fence" in call["request"] else "issue"
        for call in session.lease_calls[:first_runtime_lease_post_count]
    ]
    ok = (
        runtime_preparation.metadata is not None
        and upload is not None
        and upload.success
        and upload.committed
        and authority.issue_count == 1
        and authority.renew_count == 1
        and authority.consume_count == 1
        and first_runtime_lease_post_count == 2
        and retry_runtime_lease_post_count == 0
        and first_source_post_count == 1
        and retry_source_post_count == 1
        and exact_fenced_request_replayed
        and runtime_token_replayed_exactly
        and runtime_sequence_replayed_exactly
        and runtime_fence_replayed_exactly
        and fresh_transport_nonce
        and exact_replay_response
        and exact_runtime_receipt
        and lease_operations == ["issue", "renew"]
        and authority.exact_replay_count == 1
        and authority.mismatched_replay_count == 0
        and retry["status"] == "acked"
        and retry["stale_leases_reset"] == 1
        and same_replay_identity
        and queue["counts"].get(RELAY_STATUS_ACKED) == 1
    )
    return {
        "status": "PASS" if ok else "FAIL",
        "scope": "local stateful runtime lease issue/renew/consume and committed-before-local-ack exact replay",
        "same_replay_identity": same_replay_identity,
        "first_post_fenced": first_post_fenced,
        "retry_post_fenced": retry_post_fenced,
        "exact_fenced_request_replayed": exact_fenced_request_replayed,
        "runtime_token_replayed_exactly": runtime_token_replayed_exactly,
        "runtime_sequence_replayed_exactly": runtime_sequence_replayed_exactly,
        "runtime_fence_replayed_exactly": runtime_fence_replayed_exactly,
        "fresh_transport_nonce": fresh_transport_nonce,
        "issue_count": authority.issue_count,
        "renew_count": authority.renew_count,
        "consume_count": authority.consume_count,
        "lease_operations": lease_operations,
        "exact_replay_response": exact_replay_response,
        "exact_runtime_receipt": exact_runtime_receipt,
        "first_runtime_lease_post_count": first_runtime_lease_post_count,
        "retry_runtime_lease_post_count": retry_runtime_lease_post_count,
        "first_source_post_count": first_source_post_count,
        "retry_source_post_count": retry_source_post_count,
        "server_exact_replay_count": authority.exact_replay_count,
        "server_mismatched_replay_count": authority.mismatched_replay_count,
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
                    "retryable": False,
                    "next_retry_after": None,
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
    authority = RuntimeLeaseFixtureAuthority()
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
            ),
            authority=authority,
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
    acked = run_relay_once(config, session=EchoAcceptedSession(authority=authority))
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
        and plan["runner_command_mode"] == "bundled_executable"
        and Path(plan["runner_command"][0]) == Path(plan["runner_exe"])
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

    selected, deferred_count = _scan_source_files(str(scan_dir), ["*.csv"], 100)
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
        "deferred_file_count": deferred_count,
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
            "evidence": "label_match_runtime_relay_report",
            "requested_evidence": "label_match_runtime_relay_report",
            "evidence_scope": "local_fixture",
            "production_ready": False,
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

import hashlib
import hmac
import json
import subprocess
import sys
from pathlib import Path

import pytest

import producer_runtime_client as runtime_client
import tools.direct_sync_phase_g_label_match_runtime_report as runtime_report_tool


def _assert_endpoint_transport_report(report):
    endpoint = report["endpoint_transport_report"]
    assert endpoint["status"] == "PASS"
    assert endpoint["endpoint_scheme"] == "https"
    assert endpoint["endpoint_path"] == "/api/producer-ingest/v1/source-file"
    assert len(endpoint["endpoint_url_sha256"]) == 64
    assert len(endpoint["endpoint_host_sha256"]) == 64
    assert endpoint["query_or_fragment_present"] is False
    assert endpoint["userinfo_present"] is False


def _signed_lease_headers(authority, request):
    timestamp = "2099-01-01T00:00:00Z"
    nonce = "fixture-negative-proof"
    canonical = runtime_client._canonical_request(
        timestamp=timestamp,
        nonce=nonce,
        producer_id=authority.producer_id,
        key_id=authority.key_id,
        body=request,
    )
    signature = hmac.new(authority.secret, canonical.encode("utf-8"), hashlib.sha256).hexdigest()
    return {
        "X-Producer-Id": authority.producer_id,
        "X-Producer-Key-Id": authority.key_id,
        "X-Producer-Timestamp": timestamp,
        "X-Producer-Nonce": nonce,
        "X-Producer-Signature": signature,
    }


def _issued_authority():
    authority = runtime_report_tool.RuntimeLeaseFixtureAuthority()
    runtime_id, public_jwk = runtime_client.new_runtime_identity()
    issue_request = {
        "contract_version": runtime_client.CONTRACT_VERSION,
        "producer_install_id": authority.producer_install_id,
        "runtime_instance_id": runtime_id,
        "public_jwk": public_jwk,
        "issue_idempotency_key": "negative-issue",
        "ttl_seconds": 600,
    }
    authority.acquire(issue_request, _signed_lease_headers(authority, issue_request))
    return authority, runtime_id, public_jwk


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("runtime_fence", 999),
        ("runtime_request_token", "Z" * 43),
        ("runtime_request_sequence", 999),
    ],
)
def test_runtime_authority_rejects_arbitrary_renewal_proof(field_name, invalid_value):
    authority, runtime_id, public_jwk = _issued_authority()
    renewal_request = {
        "contract_version": runtime_client.CONTRACT_VERSION,
        "producer_install_id": authority.producer_install_id,
        "runtime_instance_id": runtime_id,
        "public_jwk": public_jwk,
        "issue_idempotency_key": f"negative-renew-{field_name}",
        "ttl_seconds": 600,
        "runtime_fence": authority.fence,
        "runtime_request_token": authority.current_request_token,
        "runtime_request_sequence": authority.current_request_sequence,
    }
    renewal_request[field_name] = invalid_value

    response = authority.acquire(
        renewal_request,
        _signed_lease_headers(authority, renewal_request),
    )

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "STALE_RUNTIME_REQUEST_TOKEN"
    assert authority.renew_count == 0


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("runtime_fence", 999),
        ("runtime_request_token", "Z" * 43),
        ("runtime_request_sequence", 999),
    ],
)
def test_runtime_authority_rejects_arbitrary_consume_proof(field_name, invalid_value):
    authority, runtime_id, public_jwk = _issued_authority()
    metadata = {
        "producer_install_id": authority.producer_install_id,
        "runtime_instance_id": runtime_id,
        "runtime_public_jwk": public_jwk,
        "runtime_fence": authority.fence,
        "runtime_request_token": authority.current_request_token,
        "runtime_request_sequence": authority.current_request_sequence,
        "idempotency_key": f"negative-consume-{field_name}",
        "client_batch_id": f"negative-consume-{field_name}",
    }
    metadata[field_name] = invalid_value

    response = authority.source_response(
        metadata=metadata,
        file_bytes=b"fixture",
        response=runtime_report_tool.FakeResponse(200, {"committed": True}),
    )

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "STALE_RUNTIME_REQUEST_TOKEN"
    assert authority.consume_count == 0


def test_runtime_authority_rejects_invalid_lease_hmac():
    authority = runtime_report_tool.RuntimeLeaseFixtureAuthority()
    runtime_id, public_jwk = runtime_client.new_runtime_identity()
    request = {
        "contract_version": runtime_client.CONTRACT_VERSION,
        "producer_install_id": authority.producer_install_id,
        "runtime_instance_id": runtime_id,
        "public_jwk": public_jwk,
        "issue_idempotency_key": "negative-hmac",
        "ttl_seconds": 600,
    }
    headers = _signed_lease_headers(authority, request)
    headers["X-Producer-Signature"] = "0" * 64

    with pytest.raises(AssertionError):
        authority.acquire(request, headers)

    assert authority.issue_count == 0


def test_phase_g_label_match_runtime_report_is_local_pass_but_production_blocked(tmp_path):
    report_path = tmp_path / "reports" / "phase-g-label-match-runtime.json"
    completed = subprocess.run(
        [
            sys.executable,
            "tools/direct_sync_phase_g_label_match_runtime_report.py",
            "--tmp-root",
            str(tmp_path),
            "--report-path",
            str(report_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))
    report_text = report_path.read_text(encoding="utf-8-sig")
    assert report["status"] == "BLOCKED"
    assert report["production_ready"] is False
    assert report["local_contract_status"] == "PASS"
    runtime_report = report["label_match_runtime_relay_report"]
    assert runtime_report["status"] == "BLOCKED"
    assert runtime_report["evidence"] == "label_match_runtime_relay_report"
    assert runtime_report["requested_evidence"] == "label_match_runtime_relay_report"
    assert runtime_report["evidence_scope"] == "local_fixture"
    assert runtime_report["production_ready"] is False
    manifest_path = Path(report["producer_manifest_path"])
    assert runtime_report["flow"] == "LabelMatch"
    assert runtime_report["producer_repo"] == "Label_Match"
    assert runtime_report["source_host_id"] == "label-match-phase-g-host"
    assert runtime_report["producer_install_id"] == "install-label-match-phase-g"
    assert runtime_report["producer_role"] == "label_match"
    assert runtime_report["stream_name"] == "label_match_events"
    assert runtime_report["source_transport"] == "http_push"
    assert runtime_report["manifest_source_transport"] == "legacy_packaging_csv"
    assert runtime_report["manifest_hash"] == hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    assert runtime_report["task_or_service_name"] == "direct-sync-relay-label-match"
    assert runtime_report["task_or_service_installed"] is False
    assert runtime_report["runtime_kind"] == "scheduled_task"
    assert runtime_report["service_task_status"] == "BLOCKED"
    assert runtime_report["status_log_status"] == "PASS"
    assert runtime_report["reboot_logoff_sleep_status"] == "BLOCKED"
    assert runtime_report["source_scope_key"] == "label-match-phase-g-host/label_match/label_match_events"
    assert runtime_report["source_scope_key_sha256"] == hashlib.sha256(
        runtime_report["source_scope_key"].encode("utf-8")
    ).hexdigest()
    runner_report = runtime_report["local_runner_status_log_report"]
    runtime_artifact_path = Path(runtime_report["artifact_path"])
    status_artifact_path = Path(runtime_report["status_json_artifact_path"])
    log_artifact_path = Path(runtime_report["redacted_log_artifact_path"])
    assert runtime_report["queue_db_path"] == runner_report["queue_db_path"]
    assert runtime_report["artifact_ref"] == str(runtime_artifact_path)
    assert runtime_report["artifact_sha256"] == hashlib.sha256(runtime_artifact_path.read_bytes()).hexdigest()
    runtime_artifact = json.loads(runtime_artifact_path.read_text(encoding="utf-8-sig"))
    assert runtime_artifact["evidence"] == "label_match_runtime_relay_report"
    assert runtime_artifact["status"] == "BLOCKED"
    _assert_endpoint_transport_report(runtime_artifact["credential_secret_ref_report"])
    assert runtime_report["artifact_status"] == "BLOCKED"
    assert runtime_report["status_json_artifact_ref"] == str(status_artifact_path)
    assert runtime_report["redacted_log_artifact_ref"] == str(log_artifact_path)
    assert runtime_report["status_json_artifact_sha256"] == hashlib.sha256(
        status_artifact_path.read_bytes()
    ).hexdigest()
    assert runtime_report["redacted_log_artifact_sha256"] == hashlib.sha256(
        log_artifact_path.read_bytes()
    ).hexdigest()
    assert runner_report["status_json_artifact_sha256"] == runtime_report["status_json_artifact_sha256"]
    assert runner_report["redacted_log_artifact_sha256"] == runtime_report["redacted_log_artifact_sha256"]
    assert runtime_report["relay_state_machine_report"]["status"] == "PASS"
    assert runtime_report["source_scan_install_pack_report"]["status"] == "PASS"
    assert runtime_report["source_scan_install_pack_report"]["operator_pause_path_present"] is True
    assert runtime_report["source_scan_install_pack_report"]["runner_has_operator_pause"] is True
    assert runtime_report["runtime_path_boundary_report"]["status"] == "PASS"
    assert runtime_report["source_scan_admission_report"]["status"] == "PASS"
    assert runtime_report["credential_secret_ref_report"]["status"] == "PASS"
    _assert_endpoint_transport_report(runtime_report["credential_secret_ref_report"])
    assert runtime_report["secret_scan_report"]["status"] == "PASS"
    assert runtime_report["secret_scan_report"]["runner_artifacts_redacted"] is True
    assert runtime_report["secret_scan_report"]["credential_secret_material_field_present"] is True
    assert runtime_report["process_kill_recovery_report"]["status"] == "PASS"
    assert runtime_report["queue_backpressure_report"]["status"] == "PASS"
    assert runtime_report["operator_status_report"]["status"] == "PASS"
    assert runtime_report["operator_control_report"]["status"] == "PASS"
    assert runtime_report["lost_ack_replay_report"]["status"] == "BLOCKED"
    local_lost_ack = runtime_report["lost_ack_replay_report"]["local_replay_report"]
    assert local_lost_ack["status"] == "PASS"
    assert local_lost_ack["first_post_fenced"] is True
    assert local_lost_ack["retry_post_fenced"] is True
    assert local_lost_ack["exact_fenced_request_replayed"] is True
    assert local_lost_ack["runtime_token_replayed_exactly"] is True
    assert local_lost_ack["runtime_sequence_replayed_exactly"] is True
    assert local_lost_ack["runtime_fence_replayed_exactly"] is True
    assert local_lost_ack["fresh_transport_nonce"] is True
    assert local_lost_ack["issue_count"] == 1
    assert local_lost_ack["renew_count"] == 1
    assert local_lost_ack["consume_count"] == 1
    assert local_lost_ack["lease_operations"] == ["issue", "renew"]
    assert local_lost_ack["exact_replay_response"] is True
    assert local_lost_ack["exact_runtime_receipt"] is True
    assert local_lost_ack["first_runtime_lease_post_count"] == 2
    assert local_lost_ack["retry_runtime_lease_post_count"] == 0
    assert local_lost_ack["first_source_post_count"] == 1
    assert local_lost_ack["retry_source_post_count"] == 1
    assert local_lost_ack["server_exact_replay_count"] == 1
    assert local_lost_ack["server_mismatched_replay_count"] == 0
    assert runtime_report["reboot_recovery_report"]["status"] == "BLOCKED"
    assert runtime_report["operator_pause_path_present"] is True
    assert runtime_report["runner_has_operator_pause"] is True
    assert runtime_report["reboot_resume_proof"] is False
    assert runtime_report["logoff_resume_proof"] is False
    assert runtime_report["sleep_resume_proof"] is False
    assert runtime_report["accepted_receipt_count"] == 0
    assert runtime_report["local_acked_queue_count"] == 1
    assert runtime_report["pending_queue_count"] == 0
    assert runtime_report["leased_queue_count"] == 0
    assert runtime_report["retry_wait_count"] == 0
    assert runtime_report["failed_queue_count"] == 0
    assert runtime_report["operator_review_count"] == 0
    assert runtime_report["missing_server_receipt_count"] == 1
    assert runtime_report["runtime_checks"]["production_reboot_logoff_sleep_status"] == "BLOCKED"
    assert report["operator_status_report"]["status"] == "PASS"
    assert report["operator_control_report"]["status"] == "PASS"
    assert report["operator_control_report"]["audit_redaction_pass"] is True
    assert report["credential_secret_ref_report"]["status"] == "PASS"
    assert report["credential_secret_ref_report"]["secret_ref_scheme"] == "env"
    assert report["credential_secret_ref_report"]["secret_material_field_present"] is False
    assert report["credential_secret_ref_report"]["secret_material_value_in_file"] is False
    assert report["credential_secret_ref_report"]["production_readback_status"] == "BLOCKED"
    _assert_endpoint_transport_report(report["credential_secret_ref_report"])
    assert report["stale_lease_recovery_report"]["status"] == "PASS"
    assert report["process_kill_recovery_report"]["status"] == "PASS"
    assert report["process_kill_recovery_report"]["claim_process_exit_code"] == 17
    assert report["disk_pressure_report"]["status"] == "PASS"
    assert report["retry_wait_report"]["status"] == "PASS"
    assert report["queue_backpressure_report"]["status"] == "PASS"
    assert report["queue_backpressure_report"]["blocked_status"] == "blocked_queue_backpressure"
    assert report["retry_dead_letter_report"]["status"] == "PASS"
    assert report["source_scan_admission_report"]["status"] == "PASS"
    assert report["source_scan_admission_report"]["broad_glob_selected_files"] == [
        "포장실작업이벤트로그_admission.csv"
    ]
    assert report["source_scan_admission_report"]["ignored_file_selected"] is False
    assert report["source_scan_admission_report"]["nested_file_selected"] is False
    assert report["source_scan_admission_report"]["recursive_glob_rejected"] is True
    assert report["source_scan_admission_report"]["path_glob_rejected"] is True
    assert report["lost_ack_replay_report"]["local_replay_report"]["status"] == "PASS"
    assert report["reboot_recovery_report"]["status"] == "BLOCKED"
    assert report["production_install_pack_report"]["local_dry_run_report"]["status"] == "PASS"
    assert report["production_install_pack_report"]["local_dry_run_report"]["operator_pause_path"]
    assert "--operator-pause-path" in report["production_install_pack_report"]["local_dry_run_report"]["runner_command"]
    assert report["production_install_pack_report"]["local_dry_run_report"]["backpressure"] == {
        "max_active_queue_age_seconds": 24 * 60 * 60,
        "max_active_queue_count": 1000,
    }
    assert "--max-active-queue-count" in report["production_install_pack_report"]["local_dry_run_report"]["runner_command"]
    assert "label-phase-g-local-secret" not in report_text
    assert "label-phase-g-secret-ref-fixture" not in report_text
    assert "X-Producer-Signature" not in report_text
    assert '"runtime_request_token"' not in report_text
    assert '"next_request_token"' not in report_text


def test_lost_ack_report_fails_closed_when_first_post_is_unfenced(tmp_path, monkeypatch):
    runtime_report_tool._make_manifest(tmp_path)
    runtime_report_tool._make_credential(tmp_path)

    monkeypatch.setattr(
        runtime_report_tool.runtime_client,
        "prepare_runtime_metadata",
        lambda **kwargs: runtime_client.RuntimePreparation(metadata=dict(kwargs["metadata"])),
    )

    report = runtime_report_tool._lost_ack_replay_report(tmp_path)

    assert report["status"] == "FAIL"
    assert report["first_post_fenced"] is False
    assert report["exact_fenced_request_replayed"] is False
    assert report["first_runtime_lease_post_count"] == 0

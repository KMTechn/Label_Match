import hashlib
import json
import subprocess
import sys
from pathlib import Path


def _assert_endpoint_transport_report(report):
    endpoint = report["endpoint_transport_report"]
    assert endpoint["status"] == "PASS"
    assert endpoint["endpoint_scheme"] == "https"
    assert endpoint["endpoint_path"] == "/api/producer-ingest/v1/source-file"
    assert len(endpoint["endpoint_url_sha256"]) == 64
    assert len(endpoint["endpoint_host_sha256"]) == 64
    assert endpoint["query_or_fragment_present"] is False
    assert endpoint["userinfo_present"] is False


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
    assert runtime_report["lost_ack_replay_report"]["local_replay_report"]["status"] == "PASS"
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

import json
import subprocess
import sys


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
    assert report["label_match_runtime_relay_report"]["status"] == "BLOCKED"
    assert report["operator_status_report"]["status"] == "PASS"
    assert report["operator_control_report"]["status"] == "PASS"
    assert report["operator_control_report"]["audit_redaction_pass"] is True
    assert report["credential_secret_ref_report"]["status"] == "PASS"
    assert report["credential_secret_ref_report"]["secret_ref_scheme"] == "env"
    assert report["credential_secret_ref_report"]["raw_secret_field_present"] is False
    assert report["credential_secret_ref_report"]["raw_secret_value_in_file"] is False
    assert report["credential_secret_ref_report"]["production_readback_status"] == "BLOCKED"
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

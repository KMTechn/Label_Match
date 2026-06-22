import json
import subprocess
import sys


def make_manifest_and_credential(tmp_path):
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "pc_identity": {
                    "pc_id": "LABEL-PC01",
                    "source_host_id": "install-pack-host",
                    "producer_install_id": "install-pack-producer",
                },
                "streams": [],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    credential_path = tmp_path / "credential.json"
    credential_path.write_text(
        json.dumps(
            {
                "producer_id": "producer-1",
                "key_id": "key-1",
                "secret": "install-pack-secret",
                "endpoint_url": "https://worker.example.invalid/api/producer-ingest/v1/source-file",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return manifest_path, credential_path


def test_install_pack_dry_run_writes_redacted_scheduled_task_plan(tmp_path):
    manifest_path, credential_path = make_manifest_and_credential(tmp_path)
    report_path = tmp_path / "install-pack.json"
    completed = subprocess.run(
        [
            sys.executable,
            "tools/direct_sync_relay_install_pack.py",
            "--producer-manifest-path",
            str(manifest_path),
            "--credential-path",
            str(credential_path),
            "--program-data-root",
            str(tmp_path / "ProgramData" / "KMTech" / "DirectSync" / "label_match"),
            "--scan-source-dir",
            str(tmp_path / "sync"),
            "--source-glob",
            "포장실작업이벤트로그_*.csv",
            "--report-path",
            str(report_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))
    report_text = report_path.read_text(encoding="utf-8-sig")
    assert report["status"] == "DRY_RUN"
    assert report["task_name"] == "direct-sync-relay-label-match"
    assert "direct_sync_relay_runner.py" in " ".join(report["runner_command"])
    assert "--scan-source-dir" in report["runner_command"]
    assert str((tmp_path / "sync").resolve()) in report["runner_command"]
    assert "포장실작업이벤트로그_*.csv" in report["runner_command"]
    assert report["source_scan"]["enabled"] is True
    assert report["source_scan"]["max_enqueue_files"] == 100
    assert report["runtime_path_boundary"]["status"] == "PASS"
    assert report["runtime_path_boundary"]["all_runtime_paths_under_program_data_root"] is True
    assert "--operator-pause-path" in report["runner_command"]
    assert report["runtime_paths"]["operator_pause_path"] in report["runner_command"]
    assert report["backpressure"] == {
        "max_active_queue_age_seconds": 24 * 60 * 60,
        "max_active_queue_count": 1000,
    }
    assert "--max-active-queue-count" in report["runner_command"]
    assert "--max-active-queue-age-seconds" in report["runner_command"]
    assert "schtasks.exe" == report["scheduled_task_create_command"][0]
    assert str(credential_path.resolve()) in report["runner_command"]
    assert "install-pack-secret" not in report_text
    assert report["secret_redaction"]["raw_secret_in_report"] is False


def test_install_pack_apply_without_confirm_is_blocked(tmp_path):
    manifest_path, credential_path = make_manifest_and_credential(tmp_path)
    report_path = tmp_path / "install-pack-blocked.json"
    completed = subprocess.run(
        [
            sys.executable,
            "tools/direct_sync_relay_install_pack.py",
            "--producer-manifest-path",
            str(manifest_path),
            "--credential-path",
            str(credential_path),
            "--report-path",
            str(report_path),
            "--apply",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))
    assert report["status"] == "BLOCKED"
    assert report["blocked_reason"] == "apply requires --confirm-production-install"


def test_install_pack_blocks_relative_program_data_root(tmp_path):
    manifest_path, credential_path = make_manifest_and_credential(tmp_path)
    report_path = tmp_path / "install-pack-relative-root.json"
    completed = subprocess.run(
        [
            sys.executable,
            "tools/direct_sync_relay_install_pack.py",
            "--producer-manifest-path",
            str(manifest_path),
            "--credential-path",
            str(credential_path),
            "--program-data-root",
            "relative-runtime-root",
            "--report-path",
            str(report_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))
    assert report["status"] == "BLOCKED"
    assert report["blocked_reason"] == "program_data_root must be an absolute path"
    assert report["runtime_path_boundary"]["status"] == "FAIL"

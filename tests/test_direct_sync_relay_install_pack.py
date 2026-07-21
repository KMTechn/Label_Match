import argparse
import base64
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


def load_install_pack_module():
    module_path = Path(__file__).resolve().parents[1] / "tools" / "direct_sync_relay_install_pack.py"
    spec = importlib.util.spec_from_file_location("direct_sync_relay_install_pack_for_tests", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def decode_encoded_powershell_command(command):
    assert command[:5] == ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-EncodedCommand"]
    return base64.b64decode(command[5]).decode("utf-16le")


def find_command(commands, executable):
    for command in commands:
        if command and command[0] == executable:
            return command
    raise AssertionError(f"command not found: {executable}")


def make_manifest_and_credential(tmp_path):
    os.environ["INSTALL_PACK_SECRET"] = "install-pack-secret"
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "pc_identity": {
                    "pc_id": "LABEL-PC01",
                    "source_host_id": "install-pack-host",
                    "producer_install_id": "install-pack-producer",
                },
                "streams": [
                    {
                        "stream_name": "label_match_events",
                        "source_system": "label_match",
                        "source_transport": "legacy_packaging_csv",
                    }
                ],
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
                "secret_ref": "env:INSTALL_PACK_SECRET",
                "endpoint_url": "https://worker.example.invalid/api/producer-ingest/v1/source-file",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return manifest_path, credential_path


def make_raw_secret_credential(tmp_path, *, secret="install-pack-secret"):
    credential_path = tmp_path / "raw-secret-credential.json"
    credential_path.write_text(
        json.dumps(
            {
                "producer_id": "producer-1",
                "key_id": "key-1",
                "secret": secret,
                "endpoint_url": "https://worker.example.invalid/api/producer-ingest/v1/source-file",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return credential_path


def test_install_pack_dry_run_writes_redacted_scheduled_task_plan(tmp_path):
    manifest_path, credential_path = make_manifest_and_credential(tmp_path)
    report_path = tmp_path / "install-pack.json"
    scan_source_dir = tmp_path / "sync"
    env = os.environ.copy()
    env["LABEL_MATCH_SAVE_DIR"] = str(scan_source_dir)
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
            str(scan_source_dir),
            "--source-glob",
            "포장실작업이벤트로그_*.csv",
            "--report-path",
            str(report_path),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )

    assert completed.returncode == 0
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))
    report_text = report_path.read_text(encoding="utf-8-sig")
    assert report["status"] == "DRY_RUN"
    assert report["install_preflight"]["status"] == "PASS"
    assert report["task_name"] == "direct-sync-relay-label-match"
    assert "direct_sync_relay_runner.py" in " ".join(report["runner_command"])
    assert "--scan-source-dir" in report["runner_command"]
    assert str(scan_source_dir.resolve()) in report["runner_command"]
    assert "포장실작업이벤트로그_*.csv" in report["runner_command"]
    assert report["source_scan"]["enabled"] is True
    assert report["source_scan"]["max_enqueue_files"] == 100
    assert report["source_scan"]["min_source_file_age_seconds"] == 60
    assert "--baseline-existing-source-files" in report["source_scan_baseline_command"]
    assert report["source_scan_baseline_command"][-2:] == ["--min-source-file-age-seconds", "0"]
    assert report["runtime_path_boundary"]["status"] == "PASS"
    assert report["runtime_path_boundary"]["all_runtime_paths_under_program_data_root"] is True
    assert str(scan_source_dir.resolve()) in report["directories_to_create"]
    assert str((tmp_path / "ProgramData" / "KMTech" / "DirectSync" / "label_match" / "queue").resolve()) in report["directories_to_create"]
    assert "--operator-pause-path" in report["runner_command"]
    assert report["runtime_paths"]["operator_pause_path"] in report["runner_command"]
    assert report["backpressure"] == {
        "max_active_queue_age_seconds": 24 * 60 * 60,
        "max_active_queue_count": 1000,
    }
    assert "--max-active-queue-count" in report["runner_command"]
    assert "--max-active-queue-age-seconds" in report["runner_command"]
    assert "--min-source-file-age-seconds" in report["runner_command"]
    assert "60" in report["runner_command"]
    assert "schtasks.exe" == report["scheduled_task_create_command"][0]
    assert str(credential_path.resolve()) in report["runner_command"]
    assert "install-pack-secret" not in report_text
    assert report["secret_redaction"]["raw_secret_in_report"] is False


def test_install_pack_blocks_raw_credential_secret_even_without_production_env(tmp_path):
    manifest_path, _credential_path = make_manifest_and_credential(tmp_path)
    credential_path = make_raw_secret_credential(tmp_path, secret="raw-production-secret")
    report_path = tmp_path / "install-pack-raw-secret.json"
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
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))
    report_text = report_path.read_text(encoding="utf-8-sig")
    assert report["status"] == "BLOCKED"
    assert "raw credential secret is disabled for production install packs" in report["blocked_reason"]
    assert any(
        check["name"] == "production_credential_secret_policy" and check["status"] == "FAIL"
        for check in report["install_preflight"]["checks"]
    )
    assert "raw-production-secret" not in report_text


def test_install_pack_blocks_app_save_path_that_does_not_match_relay_scan_dir(tmp_path):
    module = load_install_pack_module()
    app_root = tmp_path / "app-root"
    config_dir = app_root / "config"
    config_dir.mkdir(parents=True)
    for path in [
        app_root / "direct_sync_push.py",
        app_root / "direct_sync_runtime.py",
        app_root / "direct_sync_operator.py",
        app_root / "tools" / "direct_sync_relay_runner.py",
    ]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("# preflight fixture\n", encoding="utf-8")
    (config_dir / "app_settings.json").write_text(
        json.dumps({"custom_save_path": str(tmp_path / "legacy-sync")}, ensure_ascii=False),
        encoding="utf-8",
    )
    manifest_path, credential_path = make_manifest_and_credential(tmp_path)
    report_path = tmp_path / "install-pack-save-path-mismatch.json"
    args = argparse.Namespace(
        app_root=str(app_root),
        python_exe=str(tmp_path / "missing-python.exe"),
        program_data_root=str(tmp_path / "ProgramData" / "KMTech" / "DirectSync" / "label_match"),
        producer_manifest_path=str(manifest_path),
        credential_path=str(credential_path),
        task_name="direct-sync-relay-label-match",
        minute_interval=1,
        min_free_bytes=123,
        scan_source_dir=str(tmp_path / "ProgramData" / "KMTech" / "Label_Match" / "data"),
        source_glob=None,
        max_enqueue_files=100,
        min_source_file_age_seconds=60,
        max_active_queue_count=1000,
        max_active_queue_age_seconds=24 * 60 * 60,
        apply=False,
        uninstall=False,
        confirm_production_install=False,
        report_path=str(report_path),
    )

    plan = module.build_install_plan(args, run_preflight=True)

    assert plan["install_preflight"]["status"] == "FAIL"
    assert "app save path does not match relay scan source dir" in plan["install_preflight"]["blocked_reason"]
    save_path_check = next(
        check for check in plan["install_preflight"]["checks"]
        if check["name"] == "app_save_path_matches_relay_scan_dir"
    )
    assert save_path_check["status"] == "FAIL"
    assert save_path_check["custom_save_path_configured"] == "true"
    assert save_path_check["app_save_path"] == str((tmp_path / "legacy-sync").resolve())
    assert save_path_check["relay_scan_source_dir"] == str((tmp_path / "ProgramData" / "KMTech" / "Label_Match" / "data").resolve())


def test_install_pack_preflight_can_use_explicit_app_settings_path(tmp_path):
    module = load_install_pack_module()
    manifest_path, credential_path = make_manifest_and_credential(tmp_path)
    scan_source_dir = tmp_path / "ProgramData" / "KMTech" / "Label_Match" / "data"
    active_settings_path = tmp_path / "active-config" / "app_settings.json"
    active_settings_path.parent.mkdir(parents=True)
    active_settings_path.write_text(
        json.dumps({"custom_save_path": str(scan_source_dir)}, ensure_ascii=False),
        encoding="utf-8",
    )
    args = argparse.Namespace(
        app_root=str(Path(__file__).resolve().parents[1]),
        app_settings_path=str(active_settings_path),
        python_exe=sys.executable,
        program_data_root=str(tmp_path / "ProgramData" / "KMTech" / "DirectSync" / "label_match"),
        producer_manifest_path=str(manifest_path),
        credential_path=str(credential_path),
        task_name="direct-sync-relay-label-match",
        minute_interval=1,
        min_free_bytes=123,
        scan_source_dir=str(scan_source_dir),
        source_glob=None,
        max_enqueue_files=100,
        min_source_file_age_seconds=60,
        max_active_queue_count=1000,
        max_active_queue_age_seconds=24 * 60 * 60,
        apply=False,
        uninstall=False,
        confirm_production_install=False,
        report_path=str(tmp_path / "install-pack-explicit-settings.json"),
    )

    plan = module.build_install_plan(args, run_preflight=True)

    assert plan["install_preflight"]["status"] == "PASS"
    save_path_check = next(
        check for check in plan["install_preflight"]["checks"]
        if check["name"] == "app_save_path_matches_relay_scan_dir"
    )
    assert save_path_check["settings_path"] == str(active_settings_path.resolve())
    assert save_path_check["app_save_path"] == str(scan_source_dir.resolve())


def test_install_pack_defaults_to_label_match_durable_source_dir(tmp_path):
    module = load_install_pack_module()
    manifest_path, credential_path = make_manifest_and_credential(tmp_path)
    args = argparse.Namespace(
        app_root=str(Path(__file__).resolve().parents[1]),
        python_exe=sys.executable,
        program_data_root=str(tmp_path / "ProgramData" / "KMTech" / "DirectSync" / "label_match"),
        producer_manifest_path=str(manifest_path),
        credential_path=str(credential_path),
        task_name="direct-sync-relay-label-match",
        minute_interval=1,
        min_free_bytes=123,
        scan_source_dir=module.DEFAULT_LABEL_MATCH_DATA_ROOT,
        source_glob=None,
        max_enqueue_files=100,
        min_source_file_age_seconds=60,
        max_active_queue_count=1000,
        max_active_queue_age_seconds=24 * 60 * 60,
        apply=False,
        uninstall=False,
        confirm_production_install=False,
    )

    plan = module.build_install_plan(args)

    assert plan["source_scan"]["enabled"] is True
    assert plan["source_scan"]["scan_source_dir"] == str(Path(module.DEFAULT_LABEL_MATCH_DATA_ROOT).resolve())
    assert plan["source_scan"]["source_globs"] == [module.DEFAULT_SOURCE_GLOB]
    assert "--scan-source-dir" in plan["runner_command"]
    assert str(Path(module.DEFAULT_LABEL_MATCH_DATA_ROOT).resolve()) in plan["runner_command"]
    assert module.DEFAULT_SOURCE_GLOB in plan["runner_command"]


def test_install_pack_self_enroll_dry_run_does_not_require_existing_manifest_or_credential(tmp_path):
    module = load_install_pack_module()
    report_path = tmp_path / "self-enroll-install-pack.json"
    completed = subprocess.run(
        [
            sys.executable,
            "tools/direct_sync_relay_install_pack.py",
            "--self-enroll",
            "--server-base-url",
            "https://worker.example.invalid",
            "--program-data-root",
            str(tmp_path / "ProgramData" / "KMTech" / "DirectSync" / "label_match"),
            "--scan-source-dir",
            str(tmp_path / "ProgramData" / "KMTech" / "Label_Match" / "data"),
            "--report-path",
            str(report_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))
    assert report["status"] == "DRY_RUN"
    assert report["self_enrollment"]["enabled"] is True
    assert report["self_enrollment"]["manual_pc_approval_required"] is False
    assert report["self_enrollment"]["deferred_until_apply"] is True
    assert report["self_enrollment"]["endpoint_url"] == "https://worker.example.invalid/api/producer-ingest/v1/source-file"
    assert report["producer_manifest_path"].endswith("producer_manifest.json")
    assert report["credential_path"].endswith("credential.json")
    assert report["install_preflight"]["status"] == "NOT_RUN"
    assert "--producer-manifest-path" in report["runner_command"]
    assert "--credential-path" in report["runner_command"]
    assert "PRODUCER_SELF_ENROLL_TOKEN" not in report_path.read_text(encoding="utf-8-sig")


def test_install_pack_apply_blocks_direct_enrollment_token_before_registration(tmp_path, monkeypatch):
    module = load_install_pack_module()
    registration_calls = []
    monkeypatch.setattr(module, "_run_self_enrollment_registration", lambda args: registration_calls.append(args))

    report_path = tmp_path / "direct-token-blocked.json"
    result = module.main(
        [
            "--self-enroll",
            "--apply",
            "--enrollment-token",
            "raw-enrollment-token",
            "--report-path",
            str(report_path),
        ]
    )

    assert result == 2
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))
    assert report["status"] == "BLOCKED"
    assert report["blocked_reason"] == "direct --enrollment-token is disabled for apply; use env/file token delivery"
    assert "raw-enrollment-token" not in report_path.read_text(encoding="utf-8-sig")
    assert registration_calls == []


def test_self_enrollment_registration_omits_raw_stdout_and_stderr(tmp_path, monkeypatch):
    module = load_install_pack_module()
    assert module.DEFAULT_ENROLLMENT_TOKEN_ENV == "PRODUCER_SELF_ENROLL_TOKEN"
    monkeypatch.setattr(
        module,
        "_run_command",
        lambda command: {
            "returncode": 0,
            "stdout": "registration stdout may contain token material",
            "stderr": "registration stderr may contain token material",
        },
    )
    args = argparse.Namespace(
        app_root=str(tmp_path),
        python_exe=sys.executable,
        registration_exe="",
        program_data_root=str(tmp_path / "ProgramData" / "KMTech" / "DirectSync" / "label_match"),
        producer_manifest_path="",
        credential_path="",
        registration_report_path="",
        server_base_url="https://worker.example.invalid",
        endpoint_url="",
        enrollment_url="",
        enrollment_token="direct-token-for-redaction",
        enrollment_token_file="",
        enrollment_token_env=module.DEFAULT_ENROLLMENT_TOKEN_ENV,
        enrollment_timeout_seconds=30,
        scan_source_dir=str(tmp_path / "scan-source"),
        pc_id="",
        source_host_id="",
        producer_install_id="",
        producer_id="",
        key_id="",
        secret_ref_target="",
    )

    result = module._run_self_enrollment_registration(args)

    assert "stdout" not in result
    assert "stderr" not in result
    assert result["stdout_omitted"] is True
    assert result["stderr_omitted"] is True
    assert result["stdout_bytes"] > 0
    assert result["stderr_bytes"] > 0
    assert "direct-token-for-redaction" not in result["command_redacted"]
    assert "[redacted]" in result["command_redacted"]
    token_env_index = result["command_redacted"].index("--enrollment-token-env")
    assert result["command_redacted"][token_env_index + 1] == "PRODUCER_SELF_ENROLL_TOKEN"


def test_install_pack_apply_creates_runtime_and_source_directories_before_schtasks(tmp_path, monkeypatch):
    module = load_install_pack_module()
    manifest_path, credential_path = make_manifest_and_credential(tmp_path)
    report_path = tmp_path / "install-pack-apply.json"
    program_data_root = tmp_path / "ProgramData" / "KMTech" / "DirectSync" / "label_match"
    scan_source_dir = tmp_path / "ProgramData" / "KMTech" / "Label_Match" / "data"
    monkeypatch.setenv("LABEL_MATCH_SAVE_DIR", str(scan_source_dir))
    commands = []
    monkeypatch.setattr(
        module,
        "_run_command",
        lambda command: commands.append(command) or {"returncode": 0, "stdout": "", "stderr": ""},
    )

    result = module.main(
        [
            "--producer-manifest-path",
            str(manifest_path),
            "--credential-path",
            str(credential_path),
            "--program-data-root",
            str(program_data_root),
            "--scan-source-dir",
            str(scan_source_dir),
            "--report-path",
            str(report_path),
            "--apply",
            "--confirm-production-install",
            "--allow-interactive-task-for-local-test",
        ]
    )

    assert result == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "PASS"
    assert report["directory_create_result"]["status"] == "PASS"
    assert scan_source_dir.is_dir()
    assert (program_data_root / "queue").is_dir()
    assert (program_data_root / "spool").is_dir()
    assert (program_data_root / "status").is_dir()
    assert (program_data_root / "logs").is_dir()
    assert (program_data_root / "control").is_dir()
    assert report["task_wrapper_write_result"]["status"] == "PASS"
    assert Path(report["task_wrapper"]["path"]).is_file()
    assert report["task_launcher_write_result"]["status"] == "PASS"
    launcher_path = Path(report["task_launcher"]["path"])
    assert launcher_path.is_file()
    assert report["task_launcher"]["script_encoding"] == "ascii"
    assert report["task_launcher_write_result"]["encoding"] == "ascii"
    assert not launcher_path.read_bytes().startswith(b"\xef\xbb\xbf")
    assert any("--baseline-existing-source-files" in command for command in commands)
    task_command = find_command(commands, "schtasks.exe")
    assert "wscript.exe" == task_command[task_command.index("/TR") + 1].split()[0]


def test_local_test_task_wrapper_persists_only_allowlisted_transport_environment(tmp_path, monkeypatch):
    module = load_install_pack_module()
    proxy_url = "http://127.0.0.1:51947"
    ca_path = str(tmp_path / "server-cert.pem")
    monkeypatch.setenv("HTTPS_PROXY", proxy_url)
    monkeypatch.setenv("HTTP_PROXY", proxy_url)
    monkeypatch.setenv("NO_PROXY", "")
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", ca_path)
    monkeypatch.setenv("PRODUCER_SELF_ENROLL_TOKEN", "must-not-be-captured")
    args = argparse.Namespace(allow_interactive_task_for_local_test=True)

    environment = module._local_test_task_environment(args)
    wrapper = module._task_wrapper_content(
        [sys.executable, "relay.py"],
        environment=environment,
    )

    assert list(environment) == [
        "HTTPS_PROXY",
        "HTTP_PROXY",
        "NO_PROXY",
        "REQUESTS_CA_BUNDLE",
    ]
    assert f"$env:HTTPS_PROXY = '{proxy_url}'" in wrapper
    assert f"$env:REQUESTS_CA_BUNDLE = '{ca_path}'" in wrapper
    assert "$env:NO_PROXY = ''" in wrapper
    assert "PRODUCER_SELF_ENROLL_TOKEN" not in wrapper
    assert "must-not-be-captured" not in wrapper


def test_local_test_task_environment_rejects_proxy_credentials(monkeypatch):
    module = load_install_pack_module()
    monkeypatch.setenv("HTTPS_PROXY", "http://user:password@127.0.0.1:51947")
    args = argparse.Namespace(allow_interactive_task_for_local_test=True)

    with pytest.raises(ValueError, match="must not contain proxy credentials"):
        module._local_test_task_environment(args)


def test_install_pack_apply_supports_stored_password_task_without_leaking_password(tmp_path, monkeypatch):
    module = load_install_pack_module()
    manifest_path, credential_path = make_manifest_and_credential(tmp_path)
    report_path = tmp_path / "install-pack-task-user.json"
    program_data_root = tmp_path / "ProgramData" / "KMTech" / "DirectSync" / "label_match"
    scan_source_dir = tmp_path / "ProgramData" / "KMTech" / "Label_Match" / "data"
    commands = []
    monkeypatch.setenv("LABEL_MATCH_SAVE_DIR", str(scan_source_dir))
    monkeypatch.setenv("TASK_PASSWORD_FOR_TEST", "stored-task-password")
    monkeypatch.setattr(
        module,
        "_run_command",
        lambda command: commands.append(command) or {"returncode": 0, "stdout": "", "stderr": ""},
    )

    result = module.main(
        [
            "--producer-manifest-path",
            str(manifest_path),
            "--credential-path",
            str(credential_path),
            "--program-data-root",
            str(program_data_root),
            "--scan-source-dir",
            str(scan_source_dir),
            "--report-path",
            str(report_path),
            "--task-run-user",
            "TEST1\\kmtech-remote-admin",
            "--task-run-password-env",
            "TASK_PASSWORD_FOR_TEST",
            "--apply",
            "--confirm-production-install",
        ]
    )

    assert result == 0
    task_command = find_command(commands, "powershell.exe")
    assert "stored-task-password" not in " ".join(task_command)
    command_script = decode_encoded_powershell_command(task_command)
    assert "Register-ScheduledTask" in command_script
    assert "TASK_PASSWORD_FOR_TEST" in command_script
    assert "stored-task-password" not in command_script
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))
    report_text = report_path.read_text(encoding="utf-8-sig")
    assert report["task_principal"]["mode"] == "stored_password"
    assert report["task_principal"]["password_source"] == "env:TASK_PASSWORD_FOR_TEST"
    assert report["task_principal"]["password_in_report"] is False
    assert report["task_runtime_acl"]["enabled"] is True
    assert report["task_runtime_acl"]["principal"] == "TEST1\\kmtech-remote-admin"
    assert str(program_data_root.resolve()) in report["task_runtime_acl"]["paths"]
    assert any(
        command[:3] == ["icacls.exe", str(program_data_root.resolve()), "/grant:r"]
        and command[3] == "TEST1\\kmtech-remote-admin:(OI)(CI)M"
        for command in commands
    )
    assert report["scheduled_task_create_command"][report["scheduled_task_create_command"].index("/RP") + 1] == "[redacted]"
    assert "stored-task-password" not in report_text


def test_install_pack_apply_supports_password_file_without_leaking_password(tmp_path, monkeypatch):
    module = load_install_pack_module()
    manifest_path, credential_path = make_manifest_and_credential(tmp_path)
    report_path = tmp_path / "install-pack-task-password-file.json"
    password_file = tmp_path / "task-password.txt"
    password_file.write_text("  file-task-password\t\n", encoding="utf-8")
    scan_source_dir = tmp_path / "ProgramData" / "KMTech" / "Label_Match" / "data"
    scan_source_dir.mkdir(parents=True)
    commands = []
    monkeypatch.setenv("LABEL_MATCH_SAVE_DIR", str(scan_source_dir))
    monkeypatch.setattr(
        module,
        "_run_command",
        lambda command: commands.append(command) or {"returncode": 0, "stdout": "", "stderr": ""},
    )

    result = module.main(
        [
            "--producer-manifest-path",
            str(manifest_path),
            "--credential-path",
            str(credential_path),
            "--program-data-root",
            str(tmp_path / "ProgramData" / "KMTech" / "DirectSync" / "label_match"),
            "--scan-source-dir",
            str(scan_source_dir),
            "--report-path",
            str(report_path),
            "--task-run-user",
            r"TEST1\kmtech-remote-admin",
            "--task-run-password-file",
            str(password_file),
            "--apply",
            "--confirm-production-install",
        ]
    )

    assert result == 0
    task_command = find_command(commands, "powershell.exe")
    assert "file-task-password" not in " ".join(task_command)
    command_script = decode_encoded_powershell_command(task_command)
    assert "Register-ScheduledTask" in command_script
    assert str(password_file.resolve()) in command_script
    assert "file-task-password" not in command_script
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))
    report_text = report_path.read_text(encoding="utf-8-sig")
    assert report["status"] == "PASS"
    assert report["task_principal"]["mode"] == "stored_password"
    assert report["task_principal"]["password_source"] == "file"
    assert report["scheduled_task_create_command"][report["scheduled_task_create_command"].index("/RP") + 1] == "[redacted]"
    assert "file-task-password" not in report_text
    password, source, error = module._read_task_password(argparse.Namespace(task_run_password_env="", task_run_password_file=str(password_file)))
    assert (password, source, error) == ("  file-task-password\t", "file", "")


def test_install_pack_blocks_task_user_without_password_source(tmp_path):
    module = load_install_pack_module()
    manifest_path, credential_path = make_manifest_and_credential(tmp_path)
    report_path = tmp_path / "install-pack-task-user-blocked.json"

    result = module.main(
        [
            "--producer-manifest-path",
            str(manifest_path),
            "--credential-path",
            str(credential_path),
            "--program-data-root",
            str(tmp_path / "ProgramData" / "KMTech" / "DirectSync" / "label_match"),
            "--report-path",
            str(report_path),
            "--task-run-user",
            "TEST1\\kmtech-remote-admin",
        ]
    )

    assert result == 2
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))
    assert report["status"] == "BLOCKED"
    assert report["task_principal"]["status"] == "FAIL"
    assert "requires --task-run-password" in report["blocked_reason"]


def test_install_pack_apply_blocks_interactive_task_without_explicit_local_test_flag(tmp_path):
    module = load_install_pack_module()
    manifest_path, credential_path = make_manifest_and_credential(tmp_path)
    report_path = tmp_path / "install-pack-interactive-task-blocked.json"

    result = module.main(
        [
            "--producer-manifest-path",
            str(manifest_path),
            "--credential-path",
            str(credential_path),
            "--program-data-root",
            str(tmp_path / "ProgramData" / "KMTech" / "DirectSync" / "label_match"),
            "--report-path",
            str(report_path),
            "--apply",
            "--confirm-production-install",
        ]
    )

    assert result == 2
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))
    assert report["status"] == "BLOCKED"
    assert report["task_principal"]["status"] == "FAIL"
    assert "production apply requires --task-run-user" in report["blocked_reason"]
    assert "--allow-interactive-task-for-local-test" in report["blocked_reason"]


def test_install_pack_blocks_invalid_task_password_sources(tmp_path, monkeypatch):
    module = load_install_pack_module()
    manifest_path, credential_path = make_manifest_and_credential(tmp_path)
    scan_source_dir = tmp_path / "ProgramData" / "KMTech" / "Label_Match" / "data"
    scan_source_dir.mkdir(parents=True)
    password_file = tmp_path / "task-password.txt"
    password_file.write_text("file-task-password", encoding="utf-8")
    empty_password_file = tmp_path / "empty-task-password.txt"
    empty_password_file.write_text("", encoding="utf-8")
    monkeypatch.setenv("LABEL_MATCH_SAVE_DIR", str(scan_source_dir))
    monkeypatch.delenv("MISSING_TASK_PASSWORD", raising=False)
    cases = [
        (["--task-run-password-env", "MISSING_TASK_PASSWORD"], "requires --task-run-user"),
        (
            ["--task-run-user", r"TEST1\kmtech-remote-admin", "--task-run-password-env", "MISSING_TASK_PASSWORD"],
            "env var is empty",
        ),
        (
            ["--task-run-user", r"TEST1\kmtech-remote-admin", "--task-run-password-file", str(empty_password_file)],
            "file is empty",
        ),
        (
            [
                "--task-run-user",
                r"TEST1\kmtech-remote-admin",
                "--task-run-password-env",
                "MISSING_TASK_PASSWORD",
                "--task-run-password-file",
                str(password_file),
            ],
            "use only one",
        ),
    ]
    for index, (extra_args, expected_reason) in enumerate(cases):
        report_path = tmp_path / f"invalid-task-password-{index}.json"
        result = module.main(
            [
                "--producer-manifest-path",
                str(manifest_path),
                "--credential-path",
                str(credential_path),
                "--program-data-root",
                str(tmp_path / "ProgramData" / "KMTech" / "DirectSync" / "label_match"),
                "--scan-source-dir",
                str(scan_source_dir),
                "--report-path",
                str(report_path),
                *extra_args,
            ]
        )

        assert result == 2
        report = json.loads(report_path.read_text(encoding="utf-8-sig"))
        assert report["status"] == "BLOCKED"
        assert expected_reason in report["blocked_reason"]


def test_install_pack_uninstall_skips_task_password_validation(tmp_path, monkeypatch):
    module = load_install_pack_module()
    report_path = tmp_path / "install-pack-uninstall.json"
    commands = []
    monkeypatch.setattr(
        module,
        "_run_command",
        lambda command: commands.append(command) or {"returncode": 0, "stdout": "", "stderr": ""},
    )

    result = module.main(
        [
            "--report-path",
            str(report_path),
            "--task-run-user",
            r"TEST1\kmtech-remote-admin",
            "--apply",
            "--uninstall",
        ]
    )

    assert result == 0
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))
    assert report["status"] == "PASS"
    assert report["task_principal"]["status"] == "SKIPPED"
    assert report["scheduled_task_create_command"] == []
    assert commands == [["schtasks.exe", "/Delete", "/TN", "direct-sync-relay-label-match", "/F"]]


def test_install_pack_apply_self_enroll_runs_registration_before_schtasks(tmp_path, monkeypatch):
    module = load_install_pack_module()
    report_path = tmp_path / "install-pack-self-enroll-apply.json"
    program_data_root = tmp_path / "ProgramData" / "KMTech" / "DirectSync" / "label_match"
    scan_source_dir = tmp_path / "ProgramData" / "KMTech" / "Label_Match" / "data"
    manifest_path = program_data_root / "producer_manifest.json"
    credential_path = program_data_root / "credential.json"
    registration_commands = []
    task_commands = []
    monkeypatch.setenv("LABEL_MATCH_SAVE_DIR", str(scan_source_dir))
    monkeypatch.setenv("INSTALL_PACK_SECRET", "install-pack-secret")

    def fake_registration(args):
        registration_commands.append(args)
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        credential_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(
            json.dumps(
                {
                    "pc_identity": {
                        "pc_id": "PACK-PC-01",
                        "source_host_id": "label-match-pack-pc-01",
                        "producer_install_id": "install-label-match-pack-pc-01",
                    },
                    "streams": [
                        {
                            "stream_name": "label_match_events",
                            "source_system": "label_match",
                            "source_transport": "legacy_packaging_csv",
                        }
                    ],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        credential_path.write_text(
            json.dumps(
                {
                    "producer_id": "producer-label-match-pack-pc-01",
                    "key_id": "key-label-match-pack-pc-01",
                    "secret_ref": "env:INSTALL_PACK_SECRET",
                    "endpoint_url": "https://worker.example.invalid/api/producer-ingest/v1/source-file",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        return {
            "returncode": 0,
            "stdout": "registration_report=fixture\n",
            "stderr": "",
            "command_redacted": ["python", "register_label_match_worker_pc.py", "--apply"],
        }

    monkeypatch.setattr(module, "_run_self_enrollment_registration", fake_registration)
    monkeypatch.setattr(
        module,
        "_run_command",
        lambda command: task_commands.append(command) or {"returncode": 0, "stdout": "", "stderr": ""},
    )

    result = module.main(
        [
            "--self-enroll",
            "--server-base-url",
            "https://worker.example.invalid",
            "--program-data-root",
            str(program_data_root),
            "--scan-source-dir",
            str(scan_source_dir),
            "--report-path",
            str(report_path),
            "--apply",
            "--confirm-production-install",
            "--allow-interactive-task-for-local-test",
        ]
    )

    assert result == 0
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))
    assert registration_commands
    assert any("--baseline-existing-source-files" in command for command in task_commands)
    assert find_command(task_commands, "schtasks.exe")
    assert report["task_wrapper_write_result"]["status"] == "PASS"
    assert Path(report["task_wrapper"]["path"]).is_file()
    assert report["task_launcher_write_result"]["status"] == "PASS"
    assert Path(report["task_launcher"]["path"]).is_file()
    assert report["status"] == "PASS"
    assert report["install_preflight"]["status"] == "PASS"
    assert report["self_enrollment"]["enabled"] is True
    assert report["self_enrollment_registration"]["returncode"] == 0
    assert str(manifest_path.resolve()) in report["runner_command"]
    assert str(credential_path.resolve()) in report["runner_command"]


def test_install_pack_uses_windows_quoting_for_scheduled_task_action(tmp_path):
    module = load_install_pack_module()
    app_root = tmp_path / "App Root With Spaces"
    python_exe = tmp_path / "Python Runtime" / "python.exe"
    args = argparse.Namespace(
        app_root=str(app_root),
        python_exe=str(python_exe),
        program_data_root=str(tmp_path / "Program Data" / "KMTech" / "DirectSync" / "label match"),
        producer_manifest_path=str(tmp_path / "Producer Manifest.json"),
        credential_path=str(tmp_path / "Credential File.json"),
        task_name="direct sync relay",
        minute_interval=1,
        min_free_bytes=123,
        scan_source_dir=str(tmp_path / "Source Folder"),
        source_glob=["포장실 *.csv"],
        max_enqueue_files=7,
        min_source_file_age_seconds=9,
        max_active_queue_count=11,
        max_active_queue_age_seconds=13,
        apply=False,
        uninstall=False,
        confirm_production_install=False,
    )

    plan = module.build_install_plan(args)
    create_command = plan["scheduled_task_create_command"]
    task_action = create_command[create_command.index("/TR") + 1]

    assert "'" not in task_action
    assert "wscript.exe" in task_action
    assert "//B //NoLogo" in task_action
    assert str(plan["task_launcher"]["path"]) in task_action
    wrapper_content = module._task_wrapper_content(plan["runner_command"])
    launcher_content = module._task_launcher_content(plan["task_wrapper"]["path"])
    assert str(python_exe.resolve()) in wrapper_content
    assert str((app_root / "tools" / "direct_sync_relay_runner.py").resolve()) in wrapper_content
    assert "포장실 *.csv" in wrapper_content
    assert "powershell.exe" in launcher_content
    assert "-ExecutionPolicy Bypass" in launcher_content
    assert str(plan["task_wrapper"]["path"]) in launcher_content


def test_install_pack_can_schedule_bundled_runner_exe_without_python_script_command(tmp_path):
    module = load_install_pack_module()
    manifest_path, credential_path = make_manifest_and_credential(tmp_path)
    runner_exe = tmp_path / "tools" / "direct_sync_relay_runner.exe"
    runner_exe.parent.mkdir(parents=True)
    runner_exe.write_text("fixture exe", encoding="utf-8")
    args = argparse.Namespace(
        app_root=str(tmp_path / "App Root"),
        python_exe=str(tmp_path / "missing-python.exe"),
        runner_exe=str(runner_exe),
        registration_exe=str(tmp_path / "tools" / "register_label_match_worker_pc.exe"),
        program_data_root=str(tmp_path / "ProgramData" / "KMTech" / "DirectSync" / "label_match"),
        producer_manifest_path=str(manifest_path),
        credential_path=str(credential_path),
        task_name="direct-sync-relay-label-match",
        minute_interval=1,
        min_free_bytes=123,
        scan_source_dir=str(tmp_path / "ProgramData" / "KMTech" / "Label_Match" / "data"),
        source_glob=None,
        max_enqueue_files=100,
        min_source_file_age_seconds=60,
        max_active_queue_count=1000,
        max_active_queue_age_seconds=24 * 60 * 60,
        apply=False,
        uninstall=False,
        confirm_production_install=False,
        self_enroll=True,
        server_base_url="https://worker.example.invalid",
        endpoint_url="",
        enrollment_url="",
        enrollment_token="",
        enrollment_token_file="",
        enrollment_token_env="PRODUCER_SELF_ENROLL_TOKEN",
        registration_report_path="",
    )

    plan = module.build_install_plan(args)
    wrapper_content = module._task_wrapper_content(plan["runner_command"])

    assert plan["runner_command"][0] == str(runner_exe.resolve())
    assert "direct_sync_relay_runner.py" not in plan["runner_command"]
    assert str(runner_exe.resolve()) in wrapper_content
    assert "missing-python.exe" not in wrapper_content
    assert plan["runner_exe"] == str(runner_exe.resolve())


def test_install_pack_blocks_missing_release_runtime_preflight(tmp_path):
    manifest_path, credential_path = make_manifest_and_credential(tmp_path)
    report_path = tmp_path / "install-pack-missing-runtime.json"
    completed = subprocess.run(
        [
            sys.executable,
            "tools/direct_sync_relay_install_pack.py",
            "--app-root",
            str(tmp_path / "Missing App Root"),
            "--producer-manifest-path",
            str(manifest_path),
            "--credential-path",
            str(credential_path),
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
    assert report["blocked_reason"].startswith("install preflight failed:")
    assert report["install_preflight"]["status"] == "FAIL"
    assert any(
        check["name"] == "runner_script" and check["status"] == "FAIL"
        for check in report["install_preflight"]["checks"]
    )


def test_install_pack_blocks_missing_manifest_preflight(tmp_path):
    _manifest_path, credential_path = make_manifest_and_credential(tmp_path)
    report_path = tmp_path / "install-pack-missing-manifest.json"
    completed = subprocess.run(
        [
            sys.executable,
            "tools/direct_sync_relay_install_pack.py",
            "--producer-manifest-path",
            str(tmp_path / "missing-manifest.json"),
            "--credential-path",
            str(credential_path),
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
    assert report["blocked_reason"].startswith("install preflight failed:")
    assert any(
        check["name"] == "producer_manifest_path" and check["status"] == "FAIL"
        for check in report["install_preflight"]["checks"]
    )


def test_install_pack_blocks_missing_credential_preflight(tmp_path):
    manifest_path, _credential_path = make_manifest_and_credential(tmp_path)
    report_path = tmp_path / "install-pack-missing-credential.json"
    completed = subprocess.run(
        [
            sys.executable,
            "tools/direct_sync_relay_install_pack.py",
            "--producer-manifest-path",
            str(manifest_path),
            "--credential-path",
            str(tmp_path / "missing-credential.json"),
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
    assert report["blocked_reason"].startswith("install preflight failed:")
    assert any(
        check["name"] == "credential_path" and check["status"] == "FAIL"
        for check in report["install_preflight"]["checks"]
    )


def test_install_pack_apply_with_missing_manifest_does_not_run_schtasks(tmp_path, monkeypatch):
    module = load_install_pack_module()
    _manifest_path, credential_path = make_manifest_and_credential(tmp_path)
    report_path = tmp_path / "install-pack-apply-missing-manifest.json"
    monkeypatch.setattr(
        module,
        "_run_command",
        lambda command: (_ for _ in ()).throw(AssertionError("schtasks should not run")),
    )

    result = module.main(
        [
            "--producer-manifest-path",
            str(tmp_path / "missing-manifest.json"),
            "--credential-path",
            str(credential_path),
            "--report-path",
            str(report_path),
            "--apply",
            "--confirm-production-install",
            "--allow-interactive-task-for-local-test",
        ]
    )

    assert result == 2
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))
    assert report["status"] == "BLOCKED"


def test_install_pack_blocks_manifest_missing_label_match_stream(tmp_path):
    manifest_path, credential_path = make_manifest_and_credential(tmp_path)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["streams"] = []
    manifest_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    report_path = tmp_path / "install-pack-missing-stream.json"

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
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 2
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))
    assert report["status"] == "BLOCKED"
    assert report["install_preflight"]["status"] == "FAIL"


def test_install_pack_blocks_manifest_wrong_source_system_or_transport(tmp_path):
    for field, value in [
        ("source_system", "other_system"),
        ("source_transport", "other_transport"),
    ]:
        case_dir = tmp_path / field
        case_dir.mkdir()
        manifest_path, credential_path = make_manifest_and_credential(case_dir)
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        payload["streams"][0][field] = value
        manifest_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        report_path = case_dir / "install-pack-wrong-stream.json"

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
            ],
            check=False,
            capture_output=True,
            text=True,
        )

        assert completed.returncode == 2
        report = json.loads(report_path.read_text(encoding="utf-8-sig"))
        assert report["status"] == "BLOCKED"
        assert report["install_preflight"]["status"] == "FAIL"


def test_install_pack_apply_without_confirm_creates_task_plan(tmp_path, monkeypatch):
    module = load_install_pack_module()
    manifest_path, credential_path = make_manifest_and_credential(tmp_path)
    report_path = tmp_path / "install-pack-apply-no-confirm.json"
    program_data_root = tmp_path / "ProgramData" / "KMTech" / "DirectSync" / "label_match"
    scan_source_dir = tmp_path / "ProgramData" / "KMTech" / "Label_Match" / "data"
    commands = []
    monkeypatch.setenv("LABEL_MATCH_SAVE_DIR", str(scan_source_dir))
    monkeypatch.setattr(
        module,
        "_run_command",
        lambda command: commands.append(command) or {"returncode": 0, "stdout": "", "stderr": ""},
    )

    result = module.main(
        [
            "--producer-manifest-path",
            str(manifest_path),
            "--credential-path",
            str(credential_path),
            "--program-data-root",
            str(program_data_root),
            "--scan-source-dir",
            str(scan_source_dir),
            "--report-path",
            str(report_path),
            "--apply",
            "--allow-interactive-task-for-local-test",
        ]
    )

    assert result == 0
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))
    assert report["status"] == "PASS"
    assert any("--baseline-existing-source-files" in command for command in commands)
    assert find_command(commands, "schtasks.exe")
    assert report["production_apply_guard"]["requires_confirm_production_install"] is False


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

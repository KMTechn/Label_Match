#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Build or apply the Label_Match direct-sync scheduled-task install pack."""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Sequence
from urllib.parse import urlparse


DEFAULT_TASK_NAME = "direct-sync-relay-label-match"
DEFAULT_PROGRAM_DATA_ROOT = r"C:\ProgramData\KMTech\DirectSync\label_match"
DEFAULT_LABEL_MATCH_DATA_ROOT = r"C:\ProgramData\KMTech\Label_Match\data"
DEFAULT_SOURCE_GLOB = "포장실작업이벤트로그_*.csv"
DEFAULT_SERVER_BASE_URL = "https://worker.kmtecherp.com"
DEFAULT_ENDPOINT_PATH = "/api/producer-ingest/v1/source-file"
DEFAULT_ENROLLMENT_PATH = "/api/producer-ingest/v1/enroll"
DEFAULT_ENROLLMENT_TOKEN_ENV = ""
LABEL_MATCH_SAVE_DIR_ENV = "LABEL_MATCH_SAVE_DIR"
SAFE_TASK_FILE_RE = re.compile(r"[^A-Za-z0-9._-]+")
LOCAL_TEST_TASK_ENV_NAMES = (
    "HTTPS_PROXY",
    "HTTP_PROXY",
    "NO_PROXY",
    "REQUESTS_CA_BUNDLE",
    "SSL_CERT_FILE",
)


def _quote_cmd(parts: Sequence[str]) -> str:
    return subprocess.list2cmdline([str(part) for part in parts])


def _safe_task_file_name(task_name: str) -> str:
    text = SAFE_TASK_FILE_RE.sub("_", str(task_name or "direct-sync-relay-label-match")).strip("._-")
    return (text or "direct-sync-relay-label-match")[:80]


def _task_wrapper_path(program_data_root: str | os.PathLike[str], task_name: str) -> Path:
    return Path(program_data_root).expanduser().resolve() / "bin" / f"run_{_safe_task_file_name(task_name)}.ps1"


def _task_launcher_path(program_data_root: str | os.PathLike[str], task_name: str) -> Path:
    return Path(program_data_root).expanduser().resolve() / "bin" / f"run_{_safe_task_file_name(task_name)}.vbs"


def _ps_single_quote(value: str | os.PathLike[str]) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _vbs_string(value: str | os.PathLike[str]) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _local_test_task_environment(args: argparse.Namespace) -> dict[str, str]:
    if not bool(getattr(args, "allow_interactive_task_for_local_test", False)):
        return {}
    values: dict[str, str] = {}
    for env_name in LOCAL_TEST_TASK_ENV_NAMES:
        if env_name not in os.environ:
            continue
        value = str(os.environ.get(env_name) or "")
        if any(character in value for character in ("\x00", "\r", "\n")):
            raise ValueError(f"{env_name} contains characters unsafe for a local-test task wrapper")
        if env_name in {"HTTPS_PROXY", "HTTP_PROXY"} and value:
            parsed = urlparse(value)
            if parsed.scheme not in {"http", "https"} or not parsed.hostname:
                raise ValueError(f"{env_name} must be an HTTP(S) proxy URL")
            if parsed.username or parsed.password:
                raise ValueError(f"{env_name} must not contain proxy credentials")
        values[env_name] = value
    return values


def _task_wrapper_content(
    runner_parts: Sequence[str],
    *,
    environment: dict[str, str] | None = None,
) -> str:
    python_exe = str(runner_parts[0])
    runner_args = [str(part) for part in runner_parts[1:]]
    lines = [
        "$ErrorActionPreference = 'Stop'",
    ]
    lines.extend(
        f"$env:{env_name} = {_ps_single_quote(value)}"
        for env_name, value in (environment or {}).items()
    )
    lines.append("$arguments = @(")
    lines.extend(f"    {_ps_single_quote(part)}" for part in runner_args)
    lines.extend(
        [
            ")",
            f"& {_ps_single_quote(python_exe)} @arguments",
            "exit $LASTEXITCODE",
            "",
        ]
    )
    return "\n".join(lines)


def _task_launcher_content(wrapper_path: str | os.PathLike[str]) -> str:
    wrapper = str(Path(wrapper_path).expanduser().resolve())
    lines = [
        'Set shell = CreateObject("WScript.Shell")',
        'powerShell = shell.ExpandEnvironmentStrings("%SystemRoot%") & "\\System32\\WindowsPowerShell\\v1.0\\powershell.exe"',
        f"wrapper = {_vbs_string(wrapper)}",
        'command = """" & powerShell & """ -NoProfile -ExecutionPolicy Bypass -File """ & wrapper & """"',
        "exitCode = shell.Run(command, 0, True)",
        "WScript.Quit exitCode",
        "",
    ]
    return "\r\n".join(lines)


def _task_wrapper_command(launcher_path: str | os.PathLike[str]) -> list[str]:
    return [
        "wscript.exe",
        "//B",
        "//NoLogo",
        str(Path(launcher_path).expanduser().resolve()),
    ]


def _read_task_password(args: argparse.Namespace) -> tuple[str, str, str]:
    env_name = str(getattr(args, "task_run_password_env", "") or "").strip()
    file_path = str(getattr(args, "task_run_password_file", "") or "").strip()
    if env_name and file_path:
        return "", "", "use only one of --task-run-password-env or --task-run-password-file"
    if env_name:
        value = str(os.getenv(env_name) or "")
        if not value:
            return "", f"env:{env_name}", "task run password env var is empty or unavailable"
        return value, f"env:{env_name}", ""
    if file_path:
        try:
            value = Path(file_path).read_text(encoding="utf-8-sig").rstrip("\r\n")
        except Exception as exc:
            return "", "file", f"task run password file could not be read: {exc.__class__.__name__}"
        if not value:
            return "", "file", "task run password file is empty"
        return value, "file", ""
    return "", "", "stored-password task mode requires --task-run-password-env or --task-run-password-file"


def _task_principal_args(args: argparse.Namespace, *, redact_password: bool) -> tuple[list[str], dict]:
    user = str(getattr(args, "task_run_user", "") or "").strip()
    password_env = str(getattr(args, "task_run_password_env", "") or "").strip()
    password_file = str(getattr(args, "task_run_password_file", "") or "").strip()
    apply_requested = bool(getattr(args, "apply", False))
    uninstall = bool(getattr(args, "uninstall", False))
    allow_interactive = bool(getattr(args, "allow_interactive_task_for_local_test", False))
    report = {
        "status": "PASS",
        "mode": "interactive_token_default",
        "run_user": "",
        "password_source": "",
        "password_supplied": False,
        "password_in_report": False,
        "blocked_reason": "",
    }
    if not user:
        if password_env or password_file:
            report.update({
                "status": "FAIL",
                "blocked_reason": "task password source requires --task-run-user",
            })
        elif apply_requested and not uninstall and not allow_interactive:
            report.update({
                "status": "FAIL",
                "blocked_reason": (
                    "production apply requires --task-run-user with password source "
                    "or --allow-interactive-task-for-local-test"
                ),
            })
        return [], report
    password, source, error = _read_task_password(args)
    report.update({
        "mode": "stored_password",
        "run_user": user,
        "password_source": source,
        "password_supplied": bool(password),
        "blocked_reason": error,
        "status": "FAIL" if error else "PASS",
    })
    if error:
        return [], report
    return ["/RU", user, "/RP", "[redacted]" if redact_password else password], report


def _scheduled_task_create_command(
    *,
    task_name: str,
    minute_interval: int,
    task_action: str,
    task_principal_args: Sequence[str],
) -> list[str]:
    return [
        "schtasks.exe",
        "/Create",
        "/TN",
        task_name,
        "/SC",
        "MINUTE",
        "/MO",
        str(max(1, int(minute_interval))),
        "/TR",
        task_action,
        *[str(part) for part in task_principal_args],
        "/F",
    ]


def _encoded_powershell_command(script: str) -> list[str]:
    encoded = base64.b64encode(script.encode("utf-16le")).decode("ascii")
    return [
        "powershell.exe",
        "-NoProfile",
        "-ExecutionPolicy",
        "Bypass",
        "-EncodedCommand",
        encoded,
    ]


def _stored_password_task_register_command(
    *,
    task_name: str,
    minute_interval: int,
    task_action_parts: Sequence[str],
    args: argparse.Namespace,
) -> list[str]:
    user = str(getattr(args, "task_run_user", "") or "").strip()
    env_name = str(getattr(args, "task_run_password_env", "") or "").strip()
    file_path = str(getattr(args, "task_run_password_file", "") or "").strip()
    if env_name:
        password_script = "\n".join(
            [
                f"$password = [Environment]::GetEnvironmentVariable({_ps_single_quote(env_name)}, 'Process')",
                "if ([string]::IsNullOrEmpty($password)) { throw 'task run password env var is empty or unavailable' }",
            ]
        )
    else:
        password_script = "\n".join(
            [
                f"$passwordPath = {_ps_single_quote(Path(file_path).expanduser().resolve())}",
                "$password = [System.IO.File]::ReadAllText($passwordPath, [System.Text.Encoding]::UTF8)",
                "if ($password.Length -gt 0 -and $password[0] -eq [char]0xfeff) { $password = $password.Substring(1) }",
                "$password = $password -replace '(?:\\r\\n|\\r|\\n)+$', ''",
                "if ($password.Length -eq 0) { throw 'task run password file is empty' }",
            ]
        )
    task_args = _quote_cmd([str(part) for part in task_action_parts[1:]])
    script = "\n".join(
        [
            "$ErrorActionPreference = 'Stop'",
            f"$taskName = {_ps_single_quote(task_name)}",
            f"$execute = {_ps_single_quote(str(task_action_parts[0]))}",
            f"$arguments = {_ps_single_quote(task_args)}",
            f"$user = {_ps_single_quote(user)}",
            password_script,
            "$action = New-ScheduledTaskAction -Execute $execute -Argument $arguments",
            "$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).Date -RepetitionInterval (New-TimeSpan -Minutes "
            + str(max(1, int(minute_interval)))
            + ") -RepetitionDuration (New-TimeSpan -Days 3650)",
            "$settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries",
            "Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -User $user -Password $password -Force | Out-Null",
            "",
        ]
    )
    return _encoded_powershell_command(script)


def _write_task_wrapper(
    wrapper_path: str | os.PathLike[str],
    runner_parts: Sequence[str],
    *,
    environment: dict[str, str] | None = None,
) -> dict:
    target = Path(wrapper_path)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            _task_wrapper_content(runner_parts, environment=environment),
            encoding="utf-8-sig",
            newline="\r\n",
        )
        return {"status": "PASS", "path": str(target), "encoding": "utf-8-sig"}
    except Exception as exc:
        return {"status": "FAIL", "path": str(target), "error": str(exc)}


def _write_task_launcher(launcher_path: str | os.PathLike[str], wrapper_path: str | os.PathLike[str]) -> dict:
    target = Path(launcher_path)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(_task_launcher_content(wrapper_path), encoding="ascii", newline="\r\n")
        return {"status": "PASS", "path": str(target), "encoding": "ascii"}
    except Exception as exc:
        return {"status": "FAIL", "path": str(target), "error": str(exc)}


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _runtime_paths(program_data_root: str | os.PathLike[str]) -> dict[str, str]:
    root = Path(program_data_root).expanduser().resolve()
    return {
        "db_path": str(root / "queue" / "direct_sync_relay.sqlite3"),
        "spool_dir": str(root / "spool"),
        "upload_status_dir": str(root / "upload_status"),
        "runtime_status_path": str(root / "status" / "direct_sync_relay_status.json"),
        "log_path": str(root / "logs" / "direct_sync_relay.jsonl"),
        "operator_pause_path": str(root / "control" / "pause.json"),
    }


def _default_manifest_path(program_data_root: str | os.PathLike[str]) -> str:
    return str(Path(program_data_root).expanduser().resolve() / "producer_manifest.json")


def _default_credential_path(program_data_root: str | os.PathLike[str]) -> str:
    return str(Path(program_data_root).expanduser().resolve() / "credential.json")


def _default_registration_report_path(program_data_root: str | os.PathLike[str]) -> str:
    return str(Path(program_data_root).expanduser().resolve() / "status" / "label_match_worker_pc_registration.json")


def _join_url(base_url: str, path: str) -> str:
    base = str(base_url or "").strip().rstrip("/")
    suffix = path if path.startswith("/") else f"/{path}"
    return f"{base}{suffix}"


def _endpoint_url(args: argparse.Namespace) -> str:
    endpoint = str(getattr(args, "endpoint_url", "") or "").strip()
    if endpoint:
        return endpoint
    return _join_url(str(getattr(args, "server_base_url", "") or DEFAULT_SERVER_BASE_URL), DEFAULT_ENDPOINT_PATH)


def _enrollment_url(args: argparse.Namespace) -> str:
    endpoint = str(getattr(args, "enrollment_url", "") or "").strip()
    if endpoint:
        return endpoint
    return _join_url(str(getattr(args, "server_base_url", "") or DEFAULT_SERVER_BASE_URL), DEFAULT_ENROLLMENT_PATH)


def _enrollment_token_source(args: argparse.Namespace) -> str:
    if str(getattr(args, "enrollment_token", "") or "").strip():
        return "argument"
    if str(getattr(args, "enrollment_token_file", "") or "").strip():
        return "file"
    env_name = str(getattr(args, "enrollment_token_env", "") or "").strip()
    if env_name and str(os.getenv(env_name) or "").strip():
        return "env"
    return "tokenless_ip_allowlist"


def _runtime_path_boundary_report(program_data_root: str | os.PathLike[str], paths: dict[str, str]) -> dict:
    raw_root = str(program_data_root).strip()
    if not raw_root:
        return {
            "status": "FAIL",
            "blocked_reason": "program_data_root is required",
            "all_runtime_paths_under_program_data_root": False,
        }
    root_path = Path(raw_root).expanduser()
    if not root_path.is_absolute():
        return {
            "status": "FAIL",
            "blocked_reason": "program_data_root must be an absolute path",
            "program_data_root": raw_root,
            "all_runtime_paths_under_program_data_root": False,
        }
    resolved_root = root_path.resolve()
    escaped_paths: list[str] = []
    resolved_paths: dict[str, str] = {}
    for name, path in paths.items():
        resolved = Path(path).expanduser().resolve()
        resolved_paths[name] = str(resolved)
        if not resolved.is_relative_to(resolved_root):
            escaped_paths.append(name)
    ok = not escaped_paths
    return {
        "status": "PASS" if ok else "FAIL",
        "blocked_reason": "" if ok else "runtime path escaped program_data_root",
        "program_data_root": str(resolved_root),
        "all_runtime_paths_under_program_data_root": ok,
        "escaped_paths": escaped_paths,
        "resolved_runtime_paths": resolved_paths,
    }


def _task_runtime_acl_plan(args: argparse.Namespace) -> dict:
    user = str(getattr(args, "task_run_user", "") or "").strip()
    root = Path(args.program_data_root).expanduser().resolve()
    enabled = bool(user) and not bool(getattr(args, "uninstall", False))
    status = "PASS"
    blocked_reason = ""
    if enabled and root.parent == root:
        status = "FAIL"
        blocked_reason = "program_data_root must not be a filesystem root"
    return {
        "status": status,
        "blocked_reason": blocked_reason,
        "enabled": enabled,
        "principal": user,
        "rights": "M",
        "inheritance": "(OI)(CI)",
        "paths": [str(root)] if enabled else [],
    }


def _apply_task_runtime_acl(plan: dict) -> dict:
    if plan.get("status") != "PASS":
        return {
            "status": "FAIL",
            "blocked_reason": plan.get("blocked_reason") or "task runtime ACL plan is not pass",
            "command_results": [],
        }
    paths = [str(path) for path in plan.get("paths") or []]
    created_paths: list[str] = []
    for path in paths:
        Path(path).mkdir(parents=True, exist_ok=True)
        created_paths.append(path)
    if not plan.get("enabled"):
        return {
            "status": "SKIPPED",
            "blocked_reason": "",
            "reason": "task_run_user_not_configured",
            "created_paths": created_paths,
            "command_results": [],
        }
    if os.name != "nt":
        return {
            "status": "SKIPPED",
            "blocked_reason": "",
            "reason": "non_windows_runtime",
            "created_paths": created_paths,
            "command_results": [],
        }
    principal = str(plan.get("principal") or "").strip()
    rights = str(plan.get("rights") or "M")
    inheritance = str(plan.get("inheritance") or "(OI)(CI)")
    grant = f"{principal}:{inheritance}{rights}"
    command_results = []
    for path in paths:
        command = ["icacls.exe", path, "/grant:r", grant]
        result = _run_command(command)
        command_results.append({
            "command": command,
            "returncode": result.get("returncode"),
            "stdout_omitted": bool(result.get("stdout")),
            "stderr_omitted": bool(result.get("stderr")),
            "stdout_bytes": len(str(result.get("stdout") or "").encode("utf-8", errors="replace")),
            "stderr_bytes": len(str(result.get("stderr") or "").encode("utf-8", errors="replace")),
        })
    ok = all(int(result.get("returncode") or 0) == 0 for result in command_results)
    return {
        "status": "PASS" if ok else "FAIL",
        "blocked_reason": "" if ok else "icacls grant failed for task runtime path",
        "created_paths": created_paths,
        "command_results": command_results,
    }


def _same_resolved_path(left: str | os.PathLike[str], right: str | os.PathLike[str]) -> bool:
    return os.path.normcase(str(Path(left).expanduser().resolve())) == os.path.normcase(
        str(Path(right).expanduser().resolve())
    )


def _app_save_path_scan_dir_check(
    app_root: Path,
    relay_scan_source_dir: str,
    app_settings_path: str | os.PathLike[str] = "",
) -> tuple[dict[str, str], str]:
    settings_path = (
        Path(app_settings_path).expanduser().resolve()
        if str(app_settings_path or "").strip()
        else app_root / "config" / "app_settings.json"
    )
    expected_save_path = str(Path(DEFAULT_LABEL_MATCH_DATA_ROOT).resolve())
    if not settings_path.is_file():
        return {
            "name": "app_save_path_matches_relay_scan_dir",
            "status": "PASS",
            "settings_path": str(settings_path),
            "app_save_path": expected_save_path,
            "relay_scan_source_dir": str(Path(relay_scan_source_dir).resolve()) if relay_scan_source_dir else "",
            "settings_present": "false",
        }, ""
    try:
        payload = json.loads(settings_path.read_text(encoding="utf-8-sig"))
    except Exception as exc:
        return {
            "name": "app_save_path_matches_relay_scan_dir",
            "status": "FAIL",
            "settings_path": str(settings_path),
            "relay_scan_source_dir": str(Path(relay_scan_source_dir).resolve()) if relay_scan_source_dir else "",
            "error": str(exc),
        }, "app settings preflight failed"
    configured = str(payload.get("custom_save_path", "") or "").strip() if isinstance(payload, dict) else ""
    env_override = str(os.getenv(LABEL_MATCH_SAVE_DIR_ENV) or "").strip()
    app_save_path = configured or env_override or DEFAULT_LABEL_MATCH_DATA_ROOT
    relay_scan_dir = str(relay_scan_source_dir or "").strip()
    matches = bool(relay_scan_dir) and _same_resolved_path(app_save_path, relay_scan_dir)
    return {
        "name": "app_save_path_matches_relay_scan_dir",
        "status": "PASS" if matches else "FAIL",
        "settings_path": str(settings_path),
        "settings_present": "true",
        "custom_save_path_configured": "true" if configured else "false",
        "env_save_path_configured": "true" if env_override else "false",
        "app_save_path": str(Path(app_save_path).expanduser().resolve()),
        "relay_scan_source_dir": str(Path(relay_scan_dir).expanduser().resolve()) if relay_scan_dir else "",
    }, "" if matches else "app save path does not match relay scan source dir"


def _install_preflight(
    app_root: Path,
    python_exe: str,
    runner_script: Path,
    runner_exe: Path | None,
    producer_manifest_path: Path,
    credential_path: Path,
    relay_scan_source_dir: str,
    app_settings_path: str | os.PathLike[str] = "",
) -> dict:
    checks: list[dict[str, str]] = []
    failures: list[str] = []

    def add_file_check(name: str, path: Path) -> None:
        exists = path.is_file()
        checks.append({
            "name": name,
            "path": str(path),
            "status": "PASS" if exists else "FAIL",
        })
        if not exists:
            failures.append(f"{name} missing")

    python_path = Path(python_exe)
    if runner_exe is not None:
        add_file_check("runner_exe", runner_exe)
    else:
        add_file_check("python_exe", python_path)
        add_file_check("runner_script", runner_script)
        for module_name in ["direct_sync_push.py", "direct_sync_runtime.py", "direct_sync_operator.py"]:
            add_file_check(module_name, app_root / module_name)
    add_file_check("producer_manifest_path", producer_manifest_path)
    add_file_check("credential_path", credential_path)
    save_path_check, save_path_failure = _app_save_path_scan_dir_check(
        app_root,
        relay_scan_source_dir,
        app_settings_path,
    )
    checks.append(save_path_check)
    if save_path_failure:
        failures.append(save_path_failure)

    if credential_path.is_file():
        try:
            credential_payload = json.loads(credential_path.read_text(encoding="utf-8-sig"))
            raw_secret_present = isinstance(credential_payload, dict) and bool(credential_payload.get("secret"))
            credential_fields_present = (
                isinstance(credential_payload, dict)
                and bool(str(credential_payload.get("producer_id") or "").strip())
                and bool(str(credential_payload.get("key_id") or "").strip())
                and bool(str(credential_payload.get("endpoint_url") or "").strip())
                and bool(
                    str(credential_payload.get("secret_ref") or "").strip()
                    or str(credential_payload.get("secret") or "").strip()
                )
            )
            checks.append({
                "name": "production_credential_secret_policy",
                "status": "FAIL" if raw_secret_present else "PASS",
                "raw_secret_allowed": "false",
            })
            if raw_secret_present:
                failures.append("raw credential secret is disabled for production install packs")
            checks.append({
                "name": "credential_contract",
                "status": "PASS" if credential_fields_present else "FAIL",
            })
            if not credential_fields_present:
                failures.append("credential contract preflight failed")
        except Exception as exc:
            checks.append({
                "name": "production_credential_secret_policy",
                "status": "FAIL",
                "raw_secret_allowed": "false",
                "error": str(exc),
            })
            failures.append("credential secret policy preflight failed")

    if producer_manifest_path.is_file():
        try:
            manifest_payload = json.loads(producer_manifest_path.read_text(encoding="utf-8-sig"))
            identity = manifest_payload.get("pc_identity") if isinstance(manifest_payload, dict) else {}
            streams = manifest_payload.get("streams") if isinstance(manifest_payload, dict) else []
            stream = next(
                (
                    item
                    for item in streams
                    if isinstance(item, dict)
                    and item.get("stream_name") == "label_match_events"
                    and item.get("source_system") == "label_match"
                    and item.get("source_transport") == "legacy_packaging_csv"
                ),
                None,
            )
            manifest_ok = (
                isinstance(identity, dict)
                and bool(str(identity.get("producer_install_id") or "").strip())
                and bool(str(identity.get("source_host_id") or "").strip())
                and isinstance(stream, dict)
            )
            checks.append({
                "name": "manifest_label_match_contract",
                "status": "PASS" if manifest_ok else "FAIL",
            })
            if not manifest_ok:
                failures.append("manifest Label_Match contract preflight failed")
        except Exception as exc:
            checks.append({
                "name": "manifest_label_match_contract",
                "status": "FAIL",
                "error": str(exc),
            })
            failures.append("manifest Label_Match contract preflight failed")

    if runner_exe is not None and runner_exe.is_file():
        try:
            completed = subprocess.run(
                [str(runner_exe), "--help"],
                check=False,
                capture_output=True,
                text=True,
                timeout=15,
            )
            checks.append({
                "name": "runner_exe_help",
                "status": "PASS" if completed.returncode == 0 else "FAIL",
                "returncode": str(completed.returncode),
                "stderr": completed.stderr[-500:],
            })
            if completed.returncode != 0:
                failures.append("runner executable preflight failed")
        except Exception as exc:
            checks.append({
                "name": "runner_exe_help",
                "status": "FAIL",
                "error": str(exc),
            })
            failures.append("runner executable preflight failed")

    if runner_exe is None and python_path.is_file():
        env = os.environ.copy()
        env["PYTHONPATH"] = str(app_root) + os.pathsep + env.get("PYTHONPATH", "")
        probe = (
            "import json, pathlib, sys; "
            "sys.exit(3) if sys.version_info < (3, 10) else None; "
            "import requests; "
            "import direct_sync_push, direct_sync_runtime, direct_sync_operator; "
            f"manifest=json.loads(pathlib.Path({str(producer_manifest_path)!r}).read_text(encoding='utf-8-sig')); "
            "identity=manifest.get('pc_identity') if isinstance(manifest, dict) else {}; "
            "sys.exit(4) if not isinstance(identity, dict) or not str(identity.get('producer_install_id') or '').strip() or not str(identity.get('source_host_id') or '').strip() else None; "
            "streams=manifest.get('streams') if isinstance(manifest, dict) else []; "
            "stream=next((item for item in streams if isinstance(item, dict) and item.get('stream_name') == direct_sync_push.DEFAULT_STREAM_NAME), None); "
            "sys.exit(5) if not isinstance(stream, dict) or stream.get('source_system') != direct_sync_push.DEFAULT_SOURCE_SYSTEM or stream.get('source_transport') != direct_sync_push.DEFAULT_SOURCE_TRANSPORT else None; "
            f"direct_sync_runtime.load_credentials_from_json({str(credential_path)!r})"
        )
        try:
            completed = subprocess.run(
                [str(python_path), "-c", probe],
                check=False,
                capture_output=True,
                text=True,
                timeout=15,
                env=env,
            )
            checks.append({
                "name": "python_imports",
                "status": "PASS" if completed.returncode == 0 else "FAIL",
                "returncode": str(completed.returncode),
                "stderr": completed.stderr[-500:],
            })
            if completed.returncode != 0:
                failures.append("python import/version preflight failed")
        except Exception as exc:
            checks.append({
                "name": "python_imports",
                "status": "FAIL",
                "error": str(exc),
            })
            failures.append("python import/version preflight failed")

    return {
        "status": "PASS" if not failures else "FAIL",
        "blocked_reason": "" if not failures else "; ".join(failures),
        "checks": checks,
    }


def _source_scan_config(args: argparse.Namespace) -> dict:
    scan_source_dir = str(getattr(args, "scan_source_dir", "") or "").strip()
    source_globs = [str(item) for item in (getattr(args, "source_glob", None) or [DEFAULT_SOURCE_GLOB])]
    max_enqueue_files = max(0, int(getattr(args, "max_enqueue_files", 100) or 0))
    min_source_file_age_seconds = max(0, int(getattr(args, "min_source_file_age_seconds", 60) or 0))
    return {
        "enabled": bool(scan_source_dir),
        "scan_source_dir": str(Path(scan_source_dir).resolve()) if scan_source_dir else "",
        "source_globs": source_globs,
        "max_enqueue_files": max_enqueue_files,
        "min_source_file_age_seconds": min_source_file_age_seconds,
    }


def _append_source_scan_args(runner_parts: list[str], source_scan: dict) -> None:
    if not source_scan["enabled"]:
        return
    runner_parts.extend(["--scan-source-dir", source_scan["scan_source_dir"]])
    for pattern in source_scan["source_globs"]:
        runner_parts.extend(["--source-glob", pattern])
    runner_parts.extend(["--max-enqueue-files", str(source_scan["max_enqueue_files"])])
    runner_parts.extend(["--min-source-file-age-seconds", str(source_scan["min_source_file_age_seconds"])])


def _source_scan_baseline_command(runner_parts: Sequence[str], source_scan: dict) -> list[str]:
    if not source_scan["enabled"]:
        return []
    return [
        *[str(part) for part in runner_parts],
        "--baseline-existing-source-files",
        "--min-source-file-age-seconds",
        "0",
    ]


def _directories_to_create(program_data_root: str | os.PathLike[str], paths: dict[str, str], source_scan: dict) -> list[str]:
    candidates = [Path(program_data_root).expanduser().resolve()]
    for name, path in paths.items():
        resolved = Path(path).expanduser().resolve()
        candidates.append(resolved if name.endswith("_dir") else resolved.parent)
    if source_scan["enabled"]:
        candidates.append(Path(source_scan["scan_source_dir"]).expanduser().resolve())
    return sorted({str(path) for path in candidates})


def _create_install_directories(directories: Sequence[str]) -> dict:
    created: list[str] = []
    failed: list[dict[str, str]] = []
    for directory in directories:
        path = Path(directory)
        try:
            if path.exists() and not path.is_dir():
                failed.append({"path": str(path), "error": "path exists and is not a directory"})
                continue
            path.mkdir(parents=True, exist_ok=True)
            created.append(str(path))
        except Exception as exc:
            failed.append({"path": str(path), "error": str(exc)})
    return {
        "status": "PASS" if not failed else "FAIL",
        "created_or_existing": created,
        "failed": failed,
    }


def _backpressure_config(args: argparse.Namespace) -> dict:
    return {
        "max_active_queue_count": max(0, int(getattr(args, "max_active_queue_count", 1000) or 0)),
        "max_active_queue_age_seconds": max(
            0,
            int(getattr(args, "max_active_queue_age_seconds", 24 * 60 * 60) or 0),
        ),
    }


def build_install_plan(args: argparse.Namespace, run_preflight: bool = False) -> dict:
    app_root = Path(args.app_root).resolve()
    python_exe = str(Path(args.python_exe).resolve())
    runner_script = app_root / "tools" / "direct_sync_relay_runner.py"
    runner_exe_text = str(getattr(args, "runner_exe", "") or "").strip()
    runner_exe = Path(runner_exe_text).resolve() if runner_exe_text else None
    producer_manifest_path = Path(
        getattr(args, "producer_manifest_path", "") or _default_manifest_path(args.program_data_root)
    ).resolve()
    credential_path = Path(getattr(args, "credential_path", "") or _default_credential_path(args.program_data_root)).resolve()
    app_settings_path = str(getattr(args, "app_settings_path", "") or "").strip()
    paths = _runtime_paths(args.program_data_root)
    runtime_path_boundary = _runtime_path_boundary_report(args.program_data_root, paths)
    source_scan = _source_scan_config(args)
    backpressure = _backpressure_config(args)
    task_runtime_acl = _task_runtime_acl_plan(args)
    local_test_task_environment = _local_test_task_environment(args)
    self_enroll = bool(getattr(args, "self_enroll", False))
    uninstall = bool(getattr(args, "uninstall", False))
    run_install_preflight = run_preflight and not uninstall and not (self_enroll and not bool(getattr(args, "apply", False)))
    runner_parts = [
        str(runner_exe) if runner_exe is not None else python_exe,
    ]
    if runner_exe is None:
        runner_parts.append(str(runner_script))
    runner_parts.extend([
        "--db-path",
        paths["db_path"],
        "--spool-dir",
        paths["spool_dir"],
        "--producer-manifest-path",
        str(producer_manifest_path),
        "--credential-path",
        str(credential_path),
        "--upload-status-dir",
        paths["upload_status_dir"],
        "--runtime-status-path",
        paths["runtime_status_path"],
        "--log-path",
        paths["log_path"],
        "--operator-pause-path",
        paths["operator_pause_path"],
        "--worker-id",
        args.task_name,
        "--min-free-bytes",
        str(max(0, int(args.min_free_bytes))),
        "--max-active-queue-count",
        str(backpressure["max_active_queue_count"]),
        "--max-active-queue-age-seconds",
        str(backpressure["max_active_queue_age_seconds"]),
    ])
    _append_source_scan_args(runner_parts, source_scan)
    task_wrapper = _task_wrapper_path(args.program_data_root, args.task_name)
    task_launcher = _task_launcher_path(args.program_data_root, args.task_name)
    task_action_parts = _task_wrapper_command(task_launcher)
    task_action = _quote_cmd(task_action_parts)
    if uninstall:
        task_principal = {
            "status": "SKIPPED",
            "mode": "uninstall",
            "run_user": "",
            "password_source": "",
            "password_supplied": False,
            "password_in_report": False,
            "blocked_reason": "",
        }
        create_command: list[str] = []
    else:
        task_principal_args, task_principal = _task_principal_args(args, redact_password=True)
        create_command = _scheduled_task_create_command(
            task_name=args.task_name,
            minute_interval=args.minute_interval,
            task_action=task_action,
            task_principal_args=task_principal_args,
        )
    delete_command = ["schtasks.exe", "/Delete", "/TN", args.task_name, "/F"]
    return {
        "report_version": "label-match-direct-sync-install-pack-v1",
        "status": "DRY_RUN" if not args.apply else "APPLY_REQUESTED",
        "apply": bool(args.apply),
        "uninstall": bool(args.uninstall),
        "task_name": args.task_name,
        "program_data_root": str(Path(args.program_data_root).expanduser().resolve()),
        "app_settings_path": str(
            Path(app_settings_path).expanduser().resolve()
            if app_settings_path
            else app_root / "config" / "app_settings.json"
        ),
        "producer_manifest_path": str(producer_manifest_path),
        "credential_path": str(credential_path),
        "runtime_paths": paths,
        "task_runtime_acl": task_runtime_acl,
        "directories_to_create": _directories_to_create(args.program_data_root, paths, source_scan),
        "runtime_path_boundary": runtime_path_boundary,
        "source_scan": source_scan,
        "source_scan_baseline_command": _source_scan_baseline_command(runner_parts, source_scan),
        "backpressure": backpressure,
        "runner_script": str(runner_script),
        "runner_exe": str(runner_exe) if runner_exe is not None else "",
        "runner_command": runner_parts,
        "task_wrapper": {
            "enabled": True,
            "path": str(task_wrapper),
            "command": task_action_parts,
            "script_encoding": "utf-8-sig",
        },
        "task_launcher": {
            "enabled": True,
            "path": str(task_launcher),
            "target_wrapper_path": str(task_wrapper),
            "command": task_action_parts,
            "script_encoding": "ascii",
        },
        "local_test_task_environment_names": list(local_test_task_environment),
        "local_test_task_environment_persisted": bool(local_test_task_environment),
        "task_principal": task_principal,
        "scheduled_task_create_command": create_command,
        "scheduled_task_delete_command": delete_command,
        "install_preflight": (
            _install_preflight(
                app_root,
                python_exe,
                runner_script,
                runner_exe,
                producer_manifest_path,
                credential_path,
                str(source_scan.get("scan_source_dir") or ""),
                app_settings_path,
            )
            if run_install_preflight
            else {"status": "NOT_RUN"}
        ),
        "self_enrollment": {
            "enabled": self_enroll,
            "manual_pc_approval_required": False if self_enroll else True,
            "server_base_url": str(getattr(args, "server_base_url", "") or DEFAULT_SERVER_BASE_URL),
            "endpoint_url": _endpoint_url(args),
            "enrollment_url": _enrollment_url(args),
            "enrollment_token_source": _enrollment_token_source(args),
            "registration_script": str(app_root / "tools" / "register_label_match_worker_pc.py"),
            "registration_executable": (
                str(Path(str(getattr(args, "registration_exe", "") or "")).resolve())
                if str(getattr(args, "registration_exe", "") or "").strip()
                else ""
            ),
            "registration_command_mode": (
                "bundled_executable"
                if str(getattr(args, "registration_exe", "") or "").strip()
                else "python_script"
            ),
            "registration_report_path": str(
                Path(
                    getattr(args, "registration_report_path", "")
                    or _default_registration_report_path(args.program_data_root)
                ).resolve()
            ),
            "deferred_until_apply": self_enroll and not bool(getattr(args, "apply", False)),
        },
        "secret_redaction": {
            "credential_path_only": True,
            "raw_secret_in_report": False,
        },
        "production_apply_guard": {
            "requires_apply": True,
            "requires_confirm_production_install": False,
            "confirm_production_install": bool(args.confirm_production_install),
            "confirm_production_install_accepted_legacy_flag": True,
        },
    }


def _run_command(command: Sequence[str]) -> dict:
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    return {
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def _self_enrollment_registration_command(args: argparse.Namespace) -> list[str]:
    app_root = Path(args.app_root).resolve()
    program_data_root = Path(args.program_data_root).expanduser().resolve()
    registration_exe_text = str(getattr(args, "registration_exe", "") or "").strip()
    if registration_exe_text:
        command = [str(Path(registration_exe_text).resolve())]
    else:
        command = [
            str(Path(args.python_exe).resolve()),
            str(app_root / "tools" / "register_label_match_worker_pc.py"),
        ]
    enrollment_token_env = str(getattr(args, "enrollment_token_env", "") or "")
    command.extend([
        "--apply",
        "--server-base-url",
        str(getattr(args, "server_base_url", "") or DEFAULT_SERVER_BASE_URL),
        "--endpoint-url",
        _endpoint_url(args),
        "--enrollment-url",
        _enrollment_url(args),
        "--enrollment-token-env",
        enrollment_token_env,
        "--sync-dir",
        str(Path(getattr(args, "scan_source_dir", "") or DEFAULT_LABEL_MATCH_DATA_ROOT).expanduser().resolve()),
        "--data-dir",
        str(program_data_root),
        "--manifest-path",
        str(Path(getattr(args, "producer_manifest_path", "") or _default_manifest_path(program_data_root)).resolve()),
        "--credential-path",
        str(Path(getattr(args, "credential_path", "") or _default_credential_path(program_data_root)).resolve()),
        "--report-path",
        str(
            Path(
                getattr(args, "registration_report_path", "")
                or _default_registration_report_path(program_data_root)
            ).resolve()
        ),
    ])
    optional_pairs = [
        ("--enrollment-token-file", "enrollment_token_file"),
        ("--pc-id", "pc_id"),
        ("--source-host-id", "source_host_id"),
        ("--producer-install-id", "producer_install_id"),
        ("--producer-id", "producer_id"),
        ("--key-id", "key_id"),
        ("--secret-ref-target", "secret_ref_target"),
    ]
    for flag, attribute in optional_pairs:
        value = str(getattr(args, attribute, "") or "").strip()
        if value:
            command.extend([flag, value])
    token = str(getattr(args, "enrollment_token", "") or "").strip()
    if token:
        command.extend(["--enrollment-token", token])
    timeout_seconds = int(getattr(args, "enrollment_timeout_seconds", 30) or 30)
    command.extend(["--enrollment-timeout-seconds", str(max(1, timeout_seconds))])
    return command


def _redact_registration_command(command: Sequence[str]) -> list[str]:
    redacted: list[str] = []
    skip_next = False
    for part in command:
        if skip_next:
            redacted.append("[redacted]")
            skip_next = False
            continue
        redacted.append(str(part))
        if str(part) == "--enrollment-token":
            skip_next = True
    return redacted


def _run_self_enrollment_registration(args: argparse.Namespace) -> dict:
    command = _self_enrollment_registration_command(args)
    result = _run_command(command)
    stdout = str(result.pop("stdout", "") or "")
    stderr = str(result.pop("stderr", "") or "")
    result["stdout_omitted"] = bool(stdout)
    result["stderr_omitted"] = bool(stderr)
    result["stdout_bytes"] = len(stdout.encode("utf-8", errors="replace"))
    result["stderr_bytes"] = len(stderr.encode("utf-8", errors="replace"))
    result["command_redacted"] = _redact_registration_command(command)
    report_path = Path(
        getattr(args, "registration_report_path", "")
        or _default_registration_report_path(args.program_data_root)
    ).resolve()
    result["registration_report_path"] = str(report_path)
    if report_path.is_file():
        try:
            payload = json.loads(report_path.read_text(encoding="utf-8-sig"))
            result["registration_report_summary"] = {
                "status": payload.get("status"),
                "blocked_reason": payload.get("blocked_reason"),
                "source_host_id": payload.get("source_host_id"),
                "producer_install_id": payload.get("producer_install_id"),
                "producer_id": payload.get("producer_id"),
                "key_id": payload.get("key_id"),
                "manual_pc_approval_required": payload.get("manual_pc_approval_required"),
                "endpoint_url": payload.get("endpoint_url"),
                "secret_material_persisted": payload.get("secret_material_persisted"),
            }
        except Exception as exc:
            result["registration_report_summary_error"] = str(exc)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Label_Match direct-sync relay scheduled-task install pack")
    parser.add_argument("--app-root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--app-settings-path", default="")
    parser.add_argument("--python-exe", default=sys.executable)
    parser.add_argument("--runner-exe", default="")
    parser.add_argument("--registration-exe", default="")
    parser.add_argument("--program-data-root", default=DEFAULT_PROGRAM_DATA_ROOT)
    parser.add_argument("--producer-manifest-path", default="")
    parser.add_argument("--credential-path", default="")
    parser.add_argument("--self-enroll", action="store_true")
    parser.add_argument("--server-base-url", default=DEFAULT_SERVER_BASE_URL)
    parser.add_argument("--endpoint-url", default="")
    parser.add_argument("--enrollment-url", default="")
    parser.add_argument("--enrollment-token", default="")
    parser.add_argument("--enrollment-token-file", default="")
    parser.add_argument("--enrollment-token-env", default=DEFAULT_ENROLLMENT_TOKEN_ENV)
    parser.add_argument("--enrollment-timeout-seconds", type=int, default=30)
    parser.add_argument("--pc-id", default="")
    parser.add_argument("--source-host-id", default="")
    parser.add_argument("--producer-install-id", default="")
    parser.add_argument("--producer-id", default="")
    parser.add_argument("--key-id", default="")
    parser.add_argument("--secret-ref-target", default="")
    parser.add_argument("--registration-report-path", default="")
    parser.add_argument("--task-name", default=DEFAULT_TASK_NAME)
    parser.add_argument("--minute-interval", type=int, default=1)
    parser.add_argument("--min-free-bytes", type=int, default=512 * 1024 * 1024)
    parser.add_argument("--scan-source-dir", default=DEFAULT_LABEL_MATCH_DATA_ROOT)
    parser.add_argument("--source-glob", action="append", default=None)
    parser.add_argument("--max-enqueue-files", type=int, default=100)
    parser.add_argument("--min-source-file-age-seconds", type=int, default=60)
    parser.add_argument("--max-active-queue-count", type=int, default=1000)
    parser.add_argument("--max-active-queue-age-seconds", type=int, default=24 * 60 * 60)
    parser.add_argument("--report-path", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--uninstall", action="store_true")
    parser.add_argument("--confirm-production-install", action="store_true")
    parser.add_argument("--task-run-user", default="")
    parser.add_argument("--task-run-password-env", default="")
    parser.add_argument("--task-run-password-file", default="")
    parser.add_argument("--allow-interactive-task-for-local-test", action="store_true")
    args = parser.parse_args(argv)

    if not args.self_enroll and (not args.producer_manifest_path or not args.credential_path) and not args.uninstall:
        plan = {
            "report_version": "label-match-direct-sync-install-pack-v1",
            "status": "BLOCKED",
            "blocked_reason": "--producer-manifest-path and --credential-path are required unless --self-enroll is used",
            "self_enrollment": {"enabled": False},
        }
        _write_json(Path(args.report_path), plan)
        print(f"install_pack_report={Path(args.report_path).resolve()}")
        return 2

    if args.self_enroll and args.apply and str(getattr(args, "enrollment_token", "") or "").strip():
        plan = {
            "report_version": "label-match-direct-sync-install-pack-v1",
            "status": "BLOCKED",
            "blocked_reason": "direct --enrollment-token is disabled for apply; use env/file token delivery",
            "self_enrollment": {"enabled": True},
        }
        _write_json(Path(args.report_path), plan)
        print(f"install_pack_report={Path(args.report_path).resolve()}")
        return 2

    task_principal_plan = build_install_plan(args, run_preflight=False)
    if task_principal_plan["task_principal"]["status"] not in {"PASS", "SKIPPED"}:
        task_principal_plan["status"] = "BLOCKED"
        task_principal_plan["blocked_reason"] = task_principal_plan["task_principal"]["blocked_reason"]
        _write_json(Path(args.report_path), task_principal_plan)
        print(f"install_pack_report={Path(args.report_path).resolve()}")
        return 2

    if args.self_enroll and args.apply and not args.uninstall:
        registration_result = _run_self_enrollment_registration(args)
        if registration_result["returncode"] != 0:
            plan = build_install_plan(args, run_preflight=False)
            plan["status"] = "BLOCKED"
            plan["blocked_reason"] = "self-enrollment registration failed"
            plan["self_enrollment_registration"] = registration_result
            _write_json(Path(args.report_path), plan)
            print(f"install_pack_report={Path(args.report_path).resolve()}")
            return 2

    plan = build_install_plan(args, run_preflight=True)
    if args.self_enroll and args.apply and not args.uninstall:
        plan["self_enrollment_registration"] = registration_result
    if plan["runtime_path_boundary"]["status"] != "PASS":
        plan["status"] = "BLOCKED"
        plan["blocked_reason"] = plan["runtime_path_boundary"]["blocked_reason"]
        _write_json(Path(args.report_path), plan)
        print(f"install_pack_report={Path(args.report_path).resolve()}")
        return 2
    if not args.uninstall and plan["install_preflight"]["status"] not in {"PASS", "NOT_RUN"}:
        plan["status"] = "BLOCKED"
        plan["blocked_reason"] = f"install preflight failed: {plan['install_preflight']['blocked_reason']}"
        _write_json(Path(args.report_path), plan)
        print(f"install_pack_report={Path(args.report_path).resolve()}")
        return 2
    if plan["task_principal"]["status"] not in {"PASS", "SKIPPED"}:
        plan["status"] = "BLOCKED"
        plan["blocked_reason"] = plan["task_principal"]["blocked_reason"]
        _write_json(Path(args.report_path), plan)
        print(f"install_pack_report={Path(args.report_path).resolve()}")
        return 2
    if not args.uninstall and plan["task_runtime_acl"]["status"] != "PASS":
        plan["status"] = "BLOCKED"
        plan["blocked_reason"] = plan["task_runtime_acl"]["blocked_reason"]
        _write_json(Path(args.report_path), plan)
        print(f"install_pack_report={Path(args.report_path).resolve()}")
        return 2

    if args.apply:
        if args.uninstall:
            command = plan["scheduled_task_delete_command"]
        else:
            actual_principal_args, actual_principal = _task_principal_args(args, redact_password=True)
            if actual_principal["status"] != "PASS":
                plan["status"] = "BLOCKED"
                plan["blocked_reason"] = actual_principal["blocked_reason"]
                plan["task_principal"] = actual_principal
                _write_json(Path(args.report_path), plan)
                print(f"install_pack_report={Path(args.report_path).resolve()}")
                return 2
            if actual_principal["mode"] == "stored_password":
                command = _stored_password_task_register_command(
                    task_name=args.task_name,
                    minute_interval=args.minute_interval,
                    task_action_parts=plan["task_launcher"]["command"],
                    args=args,
                )
            else:
                command = _scheduled_task_create_command(
                    task_name=args.task_name,
                    minute_interval=args.minute_interval,
                    task_action=plan["scheduled_task_create_command"][plan["scheduled_task_create_command"].index("/TR") + 1],
                    task_principal_args=actual_principal_args,
                )
        if not args.uninstall:
            plan["directory_create_result"] = _create_install_directories(plan["directories_to_create"])
            if plan["directory_create_result"]["status"] != "PASS":
                plan["status"] = "FAIL"
                _write_json(Path(args.report_path), plan)
                print(f"install_pack_report={Path(args.report_path).resolve()}")
                return 1
            acl_result = _apply_task_runtime_acl(plan["task_runtime_acl"])
            plan["task_runtime_acl"]["apply_result"] = acl_result
            if acl_result["status"] == "FAIL":
                plan["status"] = "FAIL"
                plan["blocked_reason"] = acl_result["blocked_reason"]
                _write_json(Path(args.report_path), plan)
                print(f"install_pack_report={Path(args.report_path).resolve()}")
                return 1
            baseline_command = plan.get("source_scan_baseline_command") or []
            if baseline_command:
                plan["source_scan_baseline_result"] = _run_command(baseline_command)
                if int(plan["source_scan_baseline_result"].get("returncode") or 0) != 0:
                    plan["status"] = "FAIL"
                    plan["blocked_reason"] = "source scan baseline failed"
                    _write_json(Path(args.report_path), plan)
                    print(f"install_pack_report={Path(args.report_path).resolve()}")
                    return 1
            plan["task_wrapper_write_result"] = _write_task_wrapper(
                plan["task_wrapper"]["path"],
                plan["runner_command"],
                environment=_local_test_task_environment(args),
            )
            if plan["task_wrapper_write_result"]["status"] != "PASS":
                plan["status"] = "FAIL"
                _write_json(Path(args.report_path), plan)
                print(f"install_pack_report={Path(args.report_path).resolve()}")
                return 1
            plan["task_launcher_write_result"] = _write_task_launcher(
                plan["task_launcher"]["path"],
                plan["task_wrapper"]["path"],
            )
            if plan["task_launcher_write_result"]["status"] != "PASS":
                plan["status"] = "FAIL"
                _write_json(Path(args.report_path), plan)
                print(f"install_pack_report={Path(args.report_path).resolve()}")
                return 1
        plan["command_result"] = _run_command(command)
        plan["status"] = "PASS" if plan["command_result"]["returncode"] == 0 else "FAIL"

    _write_json(Path(args.report_path), plan)
    print(f"install_pack_report={Path(args.report_path).resolve()}")
    return 0 if plan["status"] in {"DRY_RUN", "PASS"} else 1


if __name__ == "__main__":
    raise SystemExit(main())

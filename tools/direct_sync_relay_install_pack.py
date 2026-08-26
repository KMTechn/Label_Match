#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Build or apply the Label_Match direct-sync scheduled-task install pack."""

from __future__ import annotations

import argparse
import base64
import contextlib
import importlib
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path
from typing import Sequence
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


DEFAULT_TASK_NAME = "direct-sync-relay-label-match"
DEFAULT_PROGRAM_DATA_ROOT = r"C:\ProgramData\KMTech\DirectSync\label_match"
CANONICAL_INSTALL_ROOT = r"C:\KMTech\Apps\Label_Match\current"
NONCANONICAL_LAYOUT_TEST_MODE_ENV = "KMTECH_FACTORY_INSTALL_TEST_MODE"
DEFAULT_LABEL_MATCH_DATA_ROOT = r"C:\ProgramData\KMTech\Label_Match\data"
DEFAULT_SOURCE_GLOB = "포장실작업이벤트로그_*.csv"
DEFAULT_SERVER_BASE_URL = "https://worker.kmtecherp.com"
DEFAULT_ENDPOINT_PATH = "/api/producer-ingest/v1/source-file"
DEFAULT_ENROLLMENT_PATH = "/api/producer-ingest/v1/enroll"
DEFAULT_ENROLLMENT_TOKEN_ENV = "PRODUCER_SELF_ENROLL_TOKEN"
LABEL_MATCH_SAVE_DIR_ENV = "LABEL_MATCH_SAVE_DIR"
SAFE_TASK_FILE_RE = re.compile(r"[^A-Za-z0-9._-]+")
LOCAL_TEST_TASK_ENV_NAMES = (
    "HTTPS_PROXY",
    "HTTP_PROXY",
    "NO_PROXY",
    "REQUESTS_CA_BUNDLE",
    "SSL_CERT_FILE",
)
COMMAND_OUTPUT_LIMIT = 16 * 1024
DIAGNOSTIC_TEXT_LIMIT = 512
CHILD_FAILURE_CODES = frozenset(
    {
        "CHILD_EXCEPTION",
        "CHILD_IMPORT_FAILED",
        "CHILD_NONZERO_EXIT",
        "CHILD_PROCESS_START_FAILED",
        "CHILD_PROCESS_TIMEOUT",
    }
)
SENSITIVE_DIAGNOSTIC_RE = re.compile(
    r"(?i)\b([A-Za-z0-9_-]*(?:token|password|secret|authorization|cookie|api[_-]?key)"
    r"[A-Za-z0-9_-]*)\b"
    r"(\s*[:=]\s*)([^\s,;]+)"
)
BEARER_DIAGNOSTIC_RE = re.compile(r"(?i)\bbearer\s+[^\s,;]+")


def _bounded_diagnostic_text(
    value: object, *, redact_values: Sequence[object] = ()
) -> str:
    text = str(value or "")[: DIAGNOSTIC_TEXT_LIMIT * 8]
    for raw_value in sorted(
        {str(item) for item in redact_values if str(item)}, key=len, reverse=True
    ):
        text = text.replace(raw_value, "[redacted]")
    text = re.sub(r"[\x00-\x1f\x7f]+", " ", text)
    text = SENSITIVE_DIAGNOSTIC_RE.sub(
        lambda match: f"{match.group(1)}{match.group(2)}[redacted]", text
    )
    text = BEARER_DIAGNOSTIC_RE.sub("Bearer [redacted]", text)
    text = " ".join(text.split())
    return text[:DIAGNOSTIC_TEXT_LIMIT]


def _bounded_command_identity(command: Sequence[str] | str, fallback: str = "child_process") -> str:
    if isinstance(command, str):
        candidate = command
    else:
        candidate = str(command[0]) if command else ""
    leaf = re.split(r"[\\/]", candidate.strip().strip('"'))[-1]
    bounded = re.sub(r"[^A-Za-z0-9._:-]+", "_", leaf).strip("._:-")[:96]
    return bounded or fallback


def _child_failure_diagnostic(
    *,
    command_identity: str,
    child_exit_code: int | None,
    failure_code: str,
    exception: BaseException | None = None,
    redact_values: Sequence[object] = (),
) -> dict:
    code = failure_code if failure_code in CHILD_FAILURE_CODES else "CHILD_EXCEPTION"
    diagnostic: dict[str, object] = {
        "diagnostic_version": "label-match-child-failure-v1",
        "command_identity": _bounded_command_identity(command_identity),
        "child_exit_code": child_exit_code,
        "failure_code": code,
    }
    if exception is not None:
        diagnostic["inner_exception_type"] = _bounded_diagnostic_text(
            exception.__class__.__name__
        )
        message = _bounded_diagnostic_text(exception, redact_values=redact_values)
        if message:
            diagnostic["inner_exception_message"] = message
    return diagnostic


def _normalize_failure_diagnostic(candidate: object) -> dict | None:
    if not isinstance(candidate, dict):
        return None
    failure_code = str(candidate.get("failure_code") or "")
    if failure_code not in CHILD_FAILURE_CODES:
        failure_code = "CHILD_EXCEPTION"
    raw_exit_code = candidate.get("child_exit_code")
    child_exit_code = (
        raw_exit_code
        if isinstance(raw_exit_code, int) and not isinstance(raw_exit_code, bool)
        else None
    )
    diagnostic = _child_failure_diagnostic(
        command_identity=str(candidate.get("command_identity") or "child_process"),
        child_exit_code=child_exit_code,
        failure_code=failure_code,
    )
    exception_type = _bounded_diagnostic_text(candidate.get("inner_exception_type"))
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]{0,127}", exception_type):
        diagnostic["inner_exception_type"] = exception_type
    exception_message = _bounded_diagnostic_text(candidate.get("inner_exception_message"))
    if exception_message:
        diagnostic["inner_exception_message"] = exception_message
    return diagnostic


def _first_failure_diagnostic(value: object, *, depth: int = 0) -> dict | None:
    if depth > 8:
        return None
    if isinstance(value, dict):
        candidate = value.get("failure_diagnostic")
        if isinstance(candidate, dict):
            return _normalize_failure_diagnostic(candidate)
        for key, nested in value.items():
            if key == "failure_diagnostic":
                continue
            found = _first_failure_diagnostic(nested, depth=depth + 1)
            if found is not None:
                return found
    elif isinstance(value, (list, tuple)):
        for nested in value[:100]:
            found = _first_failure_diagnostic(nested, depth=depth + 1)
            if found is not None:
                return found
    return None


def _default_app_root() -> str:
    if getattr(sys, "frozen", False):
        return CANONICAL_INSTALL_ROOT
    return str(Path(__file__).resolve().parents[1])


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
    runner_executable = str(runner_parts[0])
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
            f"& {_ps_single_quote(runner_executable)} @arguments",
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
    uninstall = bool(getattr(args, "uninstall", False)) or bool(getattr(args, "rollback", False))
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
        elif allow_interactive:
            report.update({
                "mode": "interactive_token_default",
                "run_user": "",
            })
        elif not uninstall:
            report.update({
                "mode": "system_service_account",
                "run_user": "SYSTEM",
            })
            return ["/RU", "SYSTEM"], report
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


def _scheduled_task_probe_command(task_name: str) -> list[str]:
    script = "\n".join(
        [
            "$ErrorActionPreference = 'Stop'",
            f"$taskName = {_ps_single_quote(task_name)}",
            "$tasks = @(Get-ScheduledTask -ErrorAction Stop | Where-Object { [string]$_.TaskPath -eq '\\' -and [string]$_.TaskName -eq $taskName })",
            "if ($tasks.Count -eq 0) {",
            "  [ordered]@{ operation = 'probe'; state = 'ABSENT'; task_name = $taskName } | ConvertTo-Json -Compress -Depth 8",
            "  exit 0",
            "}",
            "if ($tasks.Count -ne 1) { throw 'scheduled task name is ambiguous' }",
            "$task = $tasks[0]",
            "$actions = @($task.Actions | ForEach-Object { [ordered]@{ execute = [string]$_.Execute; arguments = [string]$_.Arguments } })",
            "[ordered]@{ operation = 'probe'; state = 'PRESENT'; task_name = $task.TaskName; task_path = $task.TaskPath; runtime_state = [string]$task.State; actions = $actions } | ConvertTo-Json -Compress -Depth 8",
            "",
        ]
    )
    return _encoded_powershell_command(script)


def _scheduled_task_stop_command(
    task_name: str, expected_action_parts: Sequence[str]
) -> list[str]:
    expected_execute = str(expected_action_parts[0])
    expected_arguments = _quote_cmd([str(part) for part in expected_action_parts[1:]])
    script = "\n".join(
        [
            "$ErrorActionPreference = 'Stop'",
            f"$taskName = {_ps_single_quote(task_name)}",
            f"$expectedExecute = {_ps_single_quote(expected_execute)}",
            f"$expectedArguments = {_ps_single_quote(expected_arguments)}",
            "$tasks = @(Get-ScheduledTask -ErrorAction Stop | Where-Object { [string]$_.TaskPath -eq '\\' -and [string]$_.TaskName -eq $taskName })",
            "if ($tasks.Count -eq 0) { [ordered]@{ operation = 'stop'; status = 'ABSENT'; task_name = $taskName } | ConvertTo-Json -Compress; exit 0 }",
            "if ($tasks.Count -ne 1) { throw 'scheduled task name is ambiguous' }",
            "$task = $tasks[0]",
            "$actions = @($task.Actions)",
            "if ($actions.Count -ne 1 -or [string]$actions[0].Execute -ine $expectedExecute -or [string]$actions[0].Arguments -cne $expectedArguments) { throw 'scheduled task action is foreign or ambiguous' }",
            "if ([string]$task.State -ne 'Running') { [ordered]@{ operation = 'stop'; status = 'ALREADY_STOPPED'; task_name = $taskName } | ConvertTo-Json -Compress; exit 0 }",
            "Stop-ScheduledTask -InputObject $task -ErrorAction Stop",
            "for ($attempt = 0; $attempt -lt 50; $attempt += 1) {",
            "  $remaining = @(Get-ScheduledTask -ErrorAction Stop | Where-Object { [string]$_.TaskPath -eq '\\' -and [string]$_.TaskName -eq $taskName })",
            "  if ($remaining.Count -eq 0) { [ordered]@{ operation = 'stop'; status = 'ABSENT'; task_name = $taskName } | ConvertTo-Json -Compress; exit 0 }",
            "  if ($remaining.Count -ne 1) { throw 'scheduled task name became ambiguous during stop' }",
            "  $remainingActions = @($remaining[0].Actions)",
            "  if ($remainingActions.Count -ne 1 -or [string]$remainingActions[0].Execute -ine $expectedExecute -or [string]$remainingActions[0].Arguments -cne $expectedArguments) { throw 'scheduled task action changed during stop' }",
            "  $state = [string]$remaining[0].State",
            "  if ($state -ne 'Running') { [ordered]@{ operation = 'stop'; status = 'STOPPED'; task_name = $taskName; final_state = $state } | ConvertTo-Json -Compress; exit 0 }",
            "  Start-Sleep -Milliseconds 100",
            "}",
            "throw 'scheduled task remained running after bounded stop wait'",
            "",
        ]
    )
    return _encoded_powershell_command(script)


def _scheduled_task_delete_command(
    task_name: str, expected_action_parts: Sequence[str]
) -> list[str]:
    expected_execute = str(expected_action_parts[0])
    expected_arguments = _quote_cmd([str(part) for part in expected_action_parts[1:]])
    script = "\n".join(
        [
            "$ErrorActionPreference = 'Stop'",
            f"$taskName = {_ps_single_quote(task_name)}",
            f"$expectedExecute = {_ps_single_quote(expected_execute)}",
            f"$expectedArguments = {_ps_single_quote(expected_arguments)}",
            "$tasks = @(Get-ScheduledTask -ErrorAction Stop | Where-Object { [string]$_.TaskPath -eq '\\' -and [string]$_.TaskName -eq $taskName })",
            "if ($tasks.Count -eq 0) { [ordered]@{ operation = 'delete'; status = 'ABSENT'; task_name = $taskName } | ConvertTo-Json -Compress; exit 0 }",
            "if ($tasks.Count -ne 1) { throw 'scheduled task name is ambiguous' }",
            "$task = $tasks[0]",
            "$actions = @($task.Actions)",
            "if ($actions.Count -ne 1 -or [string]$actions[0].Execute -ine $expectedExecute -or [string]$actions[0].Arguments -cne $expectedArguments) { throw 'scheduled task action is foreign or ambiguous' }",
            "Unregister-ScheduledTask -InputObject $task -Confirm:$false -ErrorAction Stop",
            "[ordered]@{ operation = 'delete'; status = 'DELETED'; task_name = $taskName } | ConvertTo-Json -Compress",
            "",
        ]
    )
    return _encoded_powershell_command(script)


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
    if payload.get("status") not in {"PASS", "DRY_RUN", "APPLY_REQUESTED"}:
        diagnostic = _normalize_failure_diagnostic(payload.get("failure_diagnostic"))
        if diagnostic is None:
            diagnostic = _first_failure_diagnostic(payload)
        if diagnostic is not None:
            payload["failure_diagnostic"] = diagnostic
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


def _owned_direct_sync_change_plan(
    program_data_root: str | os.PathLike[str],
    task_name: str,
    scan_source_dir: str | os.PathLike[str],
) -> dict[str, object]:
    root = Path(program_data_root).expanduser().resolve()
    if root.parent == root:
        raise ValueError("program_data_root must not be a filesystem root")
    runtime = _runtime_paths(root)
    wrapper = _task_wrapper_path(root, task_name)
    launcher = _task_launcher_path(root, task_name)
    identity_files = [root / "producer_manifest.json", root / "credential.json"]
    preserved_runtime = [
        runtime["db_path"],
        f'{runtime["db_path"]}-shm',
        f'{runtime["db_path"]}-wal',
        runtime["spool_dir"],
        runtime["upload_status_dir"],
        runtime["runtime_status_path"],
        runtime["log_path"],
        runtime["operator_pause_path"],
        str(root / "status"),
    ]
    return {
        "safe_uninstall": {
            "requires_task_absence_proven": True,
            "remove_only_if_install_summary_proves_created_and_hash_matches": [
                str(wrapper),
                str(launcher),
                *[str(path) for path in identity_files],
            ],
            "preserve_business_evidence": preserved_runtime,
            "external_preserve": [str(Path(scan_source_dir).expanduser().resolve())],
            "remove_program_data_root": False,
        },
        "exact_fresh_target_rollback": {
            "requires_task_absence_proven": True,
            "requires_install_prestate": "absent",
            "preserve_before_remove": [
                *preserved_runtime,
                str(Path(scan_source_dir).expanduser().resolve()),
            ],
            "remove_program_data_root_only_if_install_summary_proves_created": str(root),
            "final_receipt_must_be_external": True,
        },
    }


def _same_path(left: str | os.PathLike[str], right: str | os.PathLike[str]) -> bool:
    return os.path.normcase(str(Path(left).expanduser().resolve())) == os.path.normcase(
        str(Path(right).expanduser().resolve())
    )


def _field_layout_contract(args: argparse.Namespace) -> dict:
    expected_install_root = Path(CANONICAL_INSTALL_ROOT)
    actual_install_root = Path(args.app_root).expanduser().resolve()
    expected_direct_sync_root = Path(DEFAULT_PROGRAM_DATA_ROOT)
    actual_direct_sync_root = Path(args.program_data_root).expanduser().resolve()
    expected_task_launcher_path = (
        expected_direct_sync_root / "bin" / "run_direct-sync-relay-label-match.vbs"
    )
    actual_task_launcher_path = _task_launcher_path(actual_direct_sync_root, args.task_name)
    expected_state_db_path = expected_direct_sync_root / "queue" / "direct_sync_relay.sqlite3"
    actual_state_db_path = Path(_runtime_paths(actual_direct_sync_root)["db_path"])
    matches = {
        "install_root_matches": _same_path(actual_install_root, expected_install_root),
        "direct_sync_root_matches": _same_path(actual_direct_sync_root, expected_direct_sync_root),
        "task_name_matches": str(args.task_name) == DEFAULT_TASK_NAME,
        "task_launcher_path_matches": _same_path(actual_task_launcher_path, expected_task_launcher_path),
        "state_db_path_matches": _same_path(actual_state_db_path, expected_state_db_path),
    }
    production_layout_matches = all(matches.values())
    local_test_override_requested = bool(
        getattr(args, "allow_noncanonical_layout_for_test", False)
    )
    local_test_override_enabled = (
        local_test_override_requested
        and str(os.getenv(NONCANONICAL_LAYOUT_TEST_MODE_ENV) or "").strip() == "1"
    )
    return {
        "status": "PASS" if production_layout_matches else "MISMATCH",
        "expected_install_root": str(expected_install_root),
        "actual_install_root": str(actual_install_root),
        "expected_direct_sync_root": str(expected_direct_sync_root),
        "actual_direct_sync_root": str(actual_direct_sync_root),
        "expected_task_name": DEFAULT_TASK_NAME,
        "actual_task_name": str(args.task_name),
        "expected_task_launcher_path": str(expected_task_launcher_path),
        "actual_task_launcher_path": str(actual_task_launcher_path),
        "expected_state_db_path": str(expected_state_db_path),
        "actual_state_db_path": str(actual_state_db_path),
        **matches,
        "production_layout_matches": production_layout_matches,
        "local_test_override_requested": local_test_override_requested,
        "local_test_override_enabled": local_test_override_enabled,
        "production_apply_allowed": production_layout_matches,
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
    enabled = bool(user) and not (
        bool(getattr(args, "uninstall", False)) or bool(getattr(args, "rollback", False))
    )
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


def _app_runtime_acl_plan(args: argparse.Namespace) -> dict:
    user = str(getattr(args, "app_run_user", "") or "").strip()
    raw_data_root = str(getattr(args, "scan_source_dir", "") or "").strip()
    enabled = bool(user) and not (
        bool(getattr(args, "uninstall", False)) or bool(getattr(args, "rollback", False))
    )
    status = "PASS"
    blocked_reason = ""
    data_root = Path(raw_data_root).expanduser()
    if enabled and not data_root.is_absolute():
        status = "FAIL"
        blocked_reason = "scan_source_dir must be absolute for app runtime ACL"
    resolved_root = data_root.resolve()
    if enabled and resolved_root.parent == resolved_root:
        status = "FAIL"
        blocked_reason = "scan_source_dir must not be a filesystem root"
    return {
        "status": status,
        "blocked_reason": blocked_reason,
        "enabled": enabled,
        "principal": user,
        "rights": "M",
        "inheritance": "(OI)(CI)",
        "recursive_existing": True,
        "paths": [str(resolved_root)] if enabled else [],
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
    recursive_existing = bool(plan.get("recursive_existing"))
    command_results = []
    for path in paths:
        command = ["icacls.exe", path, "/grant:r", grant]
        if recursive_existing:
            command.append("/T")
        result = _run_command(command)
        command_result = {
            "command": command,
            "returncode": result.get("returncode"),
            "stdout_omitted": bool(result.get("stdout")),
            "stderr_omitted": bool(result.get("stderr")),
            "stdout_bytes": int(result.get("stdout_bytes") or 0),
            "stderr_bytes": int(result.get("stderr_bytes") or 0),
        }
        if isinstance(result.get("failure_diagnostic"), dict):
            command_result["failure_diagnostic"] = result["failure_diagnostic"]
        command_results.append(command_result)
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
    runner_script: Path,
    runner_exe: Path,
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

    add_file_check("embedded_python_host", app_root / "tools" / "invoke_embedded_python.ps1")
    embedded_runtime_root = app_root / "_internal"
    if embedded_runtime_root.is_dir():
        add_file_check("embedded_python_dll", embedded_runtime_root / "python312.dll")
        add_file_check("embedded_python_base_library", embedded_runtime_root / "base_library.zip")
    add_file_check("runner_script", runner_script)
    add_file_check("scheduled_runner_executable", runner_exe)
    for module_name in [
        "producer_runtime_client.py",
        "direct_sync_push.py",
        "direct_sync_runtime.py",
        "direct_sync_operator.py",
    ]:
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

    if runner_script.is_file():
        completed = _run_imported_main("tools.direct_sync_relay_runner", ["--help"])
        runner_check = {
            "name": "runner_in_process_help",
            "status": "PASS" if completed["returncode"] == 0 else "FAIL",
            "returncode": str(completed["returncode"]),
            "stdout_omitted": bool(completed.get("stdout")),
            "stderr_omitted": bool(completed.get("stderr")),
            "stdout_bytes": int(completed.get("stdout_bytes") or 0),
            "stderr_bytes": int(completed.get("stderr_bytes") or 0),
        }
        if isinstance(completed.get("failure_diagnostic"), dict):
            runner_check["failure_diagnostic"] = completed["failure_diagnostic"]
        checks.append(runner_check)
        if completed["returncode"] != 0:
            failures.append("in-process runner preflight failed")

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
    runner_script = app_root / "tools" / "direct_sync_relay_runner.py"
    runner_exe_text = str(getattr(args, "runner_exe", "") or "").strip()
    runner_exe = (
        Path(runner_exe_text).resolve()
        if runner_exe_text
        else app_root / "tools" / "direct_sync_relay_runner.exe"
    )
    producer_manifest_path = Path(
        getattr(args, "producer_manifest_path", "") or _default_manifest_path(args.program_data_root)
    ).resolve()
    credential_path = Path(getattr(args, "credential_path", "") or _default_credential_path(args.program_data_root)).resolve()
    app_settings_path = str(getattr(args, "app_settings_path", "") or "").strip()
    paths = _runtime_paths(args.program_data_root)
    field_layout_contract = _field_layout_contract(args)
    runtime_path_boundary = _runtime_path_boundary_report(args.program_data_root, paths)
    source_scan = _source_scan_config(args)
    backpressure = _backpressure_config(args)
    task_runtime_acl = _task_runtime_acl_plan(args)
    app_runtime_acl = _app_runtime_acl_plan(args)
    local_test_task_environment = _local_test_task_environment(args)
    self_enroll = bool(getattr(args, "self_enroll", False))
    uninstall = bool(getattr(args, "uninstall", False))
    rollback = bool(getattr(args, "rollback", False))
    removal = uninstall or rollback
    run_install_preflight = run_preflight and not removal and not (self_enroll and not bool(getattr(args, "apply", False)))
    runner_arguments = [
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
    ]
    runner_parts = [str(runner_exe), *runner_arguments]
    baseline_runner_parts = [str(runner_script), *runner_arguments]
    _append_source_scan_args(runner_parts, source_scan)
    _append_source_scan_args(baseline_runner_parts, source_scan)
    task_wrapper = _task_wrapper_path(args.program_data_root, args.task_name)
    task_launcher = _task_launcher_path(args.program_data_root, args.task_name)
    task_action_parts = _task_wrapper_command(task_launcher)
    task_action = _quote_cmd(task_action_parts)
    if removal:
        task_principal = {
            "status": "SKIPPED",
            "mode": "rollback" if rollback else "uninstall",
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
    probe_command = _scheduled_task_probe_command(args.task_name)
    stop_command = _scheduled_task_stop_command(args.task_name, task_action_parts)
    delete_command = _scheduled_task_delete_command(args.task_name, task_action_parts)
    return {
        "report_version": "label-match-direct-sync-install-pack-v2",
        "status": "DRY_RUN" if not args.apply else "APPLY_REQUESTED",
        "apply": bool(args.apply),
        "uninstall": bool(args.uninstall),
        "rollback": rollback,
        "operation_mode": (
            "exact_rollback_task_phase"
            if rollback
            else ("safe_uninstall" if uninstall else "install")
        ),
        "task_name": args.task_name,
        "field_layout_contract": field_layout_contract,
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
        "app_runtime_acl": app_runtime_acl,
        "directories_to_create": _directories_to_create(args.program_data_root, paths, source_scan),
        "runtime_path_boundary": runtime_path_boundary,
        "source_scan": source_scan,
        "source_scan_baseline_command": _source_scan_baseline_command(
            baseline_runner_parts, source_scan
        ),
        "backpressure": backpressure,
        "runner_script": str(runner_script),
        "runner_exe": str(runner_exe),
        "runner_command_mode": "bundled_executable",
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
        "scheduled_task_probe_command": probe_command,
        "scheduled_task_stop_command": stop_command,
        "scheduled_task_delete_command": delete_command,
        "scheduled_task_absence_command": probe_command,
        "owned_direct_sync_change_plan": _owned_direct_sync_change_plan(
            args.program_data_root,
            args.task_name,
            str(source_scan.get("scan_source_dir") or ""),
        ),
        "install_preflight": (
            _install_preflight(
                app_root,
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
            "logistics_profile_path": str(
                getattr(args, "logistics_profile_path", "") or ""
            ).strip(),
            "registration_script": str(app_root / "tools" / "register_label_match_worker_pc.py"),
            "registration_executable": "",
            "registration_command_mode": "in_process_source",
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
            "requires_canonical_field_layout": True,
            "canonical_field_layout": field_layout_contract["production_layout_matches"],
            "allow_noncanonical_layout_for_test": field_layout_contract[
                "local_test_override_enabled"
            ],
        },
    }


def _bounded_output(value: str, *, limit: int = COMMAND_OUTPUT_LIMIT) -> tuple[str, bool, int]:
    raw = str(value or "")
    encoded = raw.encode("utf-8", errors="replace")
    if len(encoded) <= limit:
        return raw, False, len(encoded)
    bounded = encoded[:limit].decode("utf-8", errors="ignore")
    return bounded, True, len(encoded)


def _read_bounded_command_stream(stream) -> tuple[str, bool, int]:
    stream.flush()
    stream.seek(0, os.SEEK_END)
    total_bytes = stream.tell()
    stream.seek(0)
    bounded = stream.read(COMMAND_OUTPUT_LIMIT)
    return bounded.decode("utf-8", errors="replace"), total_bytes > COMMAND_OUTPUT_LIMIT, total_bytes


def _bounded_captured_text(value: str) -> tuple[str, bool, int]:
    encoded = value.encode("utf-8", errors="replace")
    return (
        encoded[:COMMAND_OUTPUT_LIMIT].decode("utf-8", errors="replace"),
        len(encoded) > COMMAND_OUTPUT_LIMIT,
        len(encoded),
    )


def _reportable_child_result(result: dict) -> dict:
    reportable = dict(result)
    stdout = str(reportable.pop("stdout", "") or "")
    stderr = str(reportable.pop("stderr", "") or "")
    reportable["stdout_omitted"] = bool(stdout) or int(reportable.get("stdout_bytes") or 0) > 0
    reportable["stderr_omitted"] = bool(stderr) or int(reportable.get("stderr_bytes") or 0) > 0
    reportable.setdefault("stdout_bytes", len(stdout.encode("utf-8", errors="replace")))
    reportable.setdefault("stderr_bytes", len(stderr.encode("utf-8", errors="replace")))
    diagnostic = _normalize_failure_diagnostic(reportable.get("failure_diagnostic"))
    if diagnostic is not None:
        reportable["failure_diagnostic"] = diagnostic
    return reportable


def _run_in_process_main(entrypoint, arguments: Sequence[str]) -> dict:
    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()
    command_identity = _bounded_command_identity(
        f"{getattr(entrypoint, '__module__', '')}.{getattr(entrypoint, '__name__', 'main')}",
        "in_process_main",
    )
    failure_code = ""
    inner_exception: BaseException | None = None
    with contextlib.redirect_stdout(stdout_buffer), contextlib.redirect_stderr(stderr_buffer):
        try:
            raw_returncode = entrypoint([str(part) for part in arguments])
            returncode = int(raw_returncode or 0)
        except SystemExit as exc:
            if exc.code is None:
                returncode = 0
            elif isinstance(exc.code, int):
                returncode = exc.code
            else:
                print(str(exc.code), file=sys.stderr)
                returncode = 1
            if returncode != 0:
                failure_code = "CHILD_NONZERO_EXIT"
        except Exception as exc:
            traceback.print_exc(file=sys.stderr)
            returncode = 1
            failure_code = "CHILD_EXCEPTION"
            inner_exception = exc
    stdout, stdout_truncated, stdout_bytes = _bounded_captured_text(stdout_buffer.getvalue())
    stderr, stderr_truncated, stderr_bytes = _bounded_captured_text(stderr_buffer.getvalue())
    result = {
        "returncode": returncode,
        "stdout": stdout,
        "stderr": stderr,
        "stdout_bytes": stdout_bytes,
        "stderr_bytes": stderr_bytes,
        "stdout_truncated": stdout_truncated,
        "stderr_truncated": stderr_truncated,
        "timed_out": False,
        "in_process": True,
        "command_identity": command_identity,
    }
    if returncode != 0:
        result["failure_diagnostic"] = _child_failure_diagnostic(
            command_identity=command_identity,
            child_exit_code=returncode,
            failure_code=failure_code or "CHILD_NONZERO_EXIT",
            exception=inner_exception,
            redact_values=arguments,
        )
    return result


def _run_imported_main(module_name: str, arguments: Sequence[str]) -> dict:
    command_identity = _bounded_command_identity(f"{module_name}.main", "imported_main")
    try:
        module = importlib.import_module(module_name)
        entrypoint = getattr(module, "main")
    except Exception as exc:
        return {
            "returncode": 1,
            "stdout": "",
            "stderr": "",
            "stdout_bytes": 0,
            "stderr_bytes": 0,
            "stdout_truncated": False,
            "stderr_truncated": False,
            "timed_out": False,
            "in_process": True,
            "command_identity": command_identity,
            "failure_diagnostic": _child_failure_diagnostic(
                command_identity=command_identity,
                child_exit_code=None,
                failure_code="CHILD_IMPORT_FAILED",
                exception=exc,
                redact_values=arguments,
            ),
        }
    return _run_in_process_main(entrypoint, arguments)


def _run_command(command: Sequence[str]) -> dict:
    command_identity = _bounded_command_identity(command)
    with tempfile.TemporaryFile(mode="w+b") as stdout_stream, tempfile.TemporaryFile(
        mode="w+b"
    ) as stderr_stream:
        timed_out = False
        failure_code = ""
        inner_exception: BaseException | None = None
        child_exit_code: int | None
        try:
            completed = subprocess.run(
                command,
                check=False,
                stdout=stdout_stream,
                stderr=stderr_stream,
                text=False,
                timeout=30,
            )
            returncode = completed.returncode
            child_exit_code = returncode
            if returncode != 0:
                failure_code = "CHILD_NONZERO_EXIT"
        except subprocess.TimeoutExpired:
            timed_out = True
            returncode = 124
            child_exit_code = None
            failure_code = "CHILD_PROCESS_TIMEOUT"
        except Exception as exc:
            returncode = 1
            child_exit_code = None
            failure_code = "CHILD_PROCESS_START_FAILED"
            inner_exception = exc
        stdout, stdout_truncated, stdout_bytes = _read_bounded_command_stream(stdout_stream)
        stderr, stderr_truncated, stderr_bytes = _read_bounded_command_stream(stderr_stream)
        result = {
            "returncode": returncode,
            "stdout": stdout,
            "stderr": stderr,
            "stdout_bytes": stdout_bytes,
            "stderr_bytes": stderr_bytes,
            "stdout_truncated": stdout_truncated,
            "stderr_truncated": stderr_truncated,
            "timed_out": timed_out,
            "command_identity": command_identity,
        }
        if returncode != 0:
            result["failure_diagnostic"] = _child_failure_diagnostic(
                command_identity=command_identity,
                child_exit_code=child_exit_code,
                failure_code=failure_code or "CHILD_NONZERO_EXIT",
                exception=inner_exception,
                redact_values=command[1:],
            )
        return result


def _typed_json_result(result: dict, *, operation: str) -> dict:
    if int(result.get("returncode", 1)) != 0:
        return {
            "status": "FAILED",
            "operation": operation,
            "error": "command failed",
            "command_result": _reportable_child_result(result),
        }
    try:
        payload = json.loads(str(result.get("stdout") or "").strip())
    except (TypeError, ValueError):
        return {
            "status": "FAILED",
            "operation": operation,
            "error": "typed command output was not valid JSON",
            "command_result": _reportable_child_result(result),
        }
    if not isinstance(payload, dict) or payload.get("operation") != operation:
        return {
            "status": "FAILED",
            "operation": operation,
            "error": "typed command output did not identify the expected operation",
            "command_result": _reportable_child_result(result),
        }
    return payload


def _normalize_task_executable(value: object) -> str:
    text = str(value or "").strip().strip('"')
    return os.path.normcase(text)


def _classify_task_probe(result: dict, expected_action_parts: Sequence[str]) -> dict:
    payload = _typed_json_result(result, operation="probe")
    if payload.get("status") == "FAILED":
        return payload
    state = str(payload.get("state") or "")
    if state == "ABSENT":
        return {"status": "ABSENT", "ownership": "NOT_APPLICABLE", "evidence": payload}
    actions = payload.get("actions")
    expected_arguments = _quote_cmd([str(part) for part in expected_action_parts[1:]])
    owned = (
        state == "PRESENT"
        and str(payload.get("task_path") or "") == "\\"
        and isinstance(actions, list)
        and len(actions) == 1
        and isinstance(actions[0], dict)
        and _normalize_task_executable(actions[0].get("execute"))
        == _normalize_task_executable(expected_action_parts[0])
        and str(actions[0].get("arguments") or "").strip() == expected_arguments
    )
    return {
        "status": "PRESENT" if owned else "CONFLICT",
        "ownership": "OWNED" if owned else "FOREIGN_OR_AMBIGUOUS",
        "evidence": payload,
    }


def _stop_scheduled_task_typed(task_name: str, expected_action_parts: Sequence[str]) -> dict:
    lifecycle: dict[str, object] = {
        "status": "FAILED",
        "task_name": task_name,
        "phase": "stop",
        "operations": [],
        "absence_proven": False,
    }
    probe = _classify_task_probe(
        _run_command(_scheduled_task_probe_command(task_name)), expected_action_parts
    )
    lifecycle["initial_probe"] = probe
    if probe["status"] == "ABSENT":
        lifecycle.update({"status": "PASS", "absence_proven": True, "idempotent_absence": True})
        return lifecycle
    if probe["status"] != "PRESENT":
        lifecycle["blocked_reason"] = "scheduled task ownership was not proven"
        return lifecycle

    stop = _typed_json_result(
        _run_command(_scheduled_task_stop_command(task_name, expected_action_parts)),
        operation="stop",
    )
    lifecycle["operations"].append(stop)
    if stop.get("status") not in {"STOPPED", "ALREADY_STOPPED", "ABSENT"}:
        lifecycle["blocked_reason"] = "scheduled task stop failed"
        return lifecycle

    lifecycle.update(
        {
            "status": "PASS",
            "absence_proven": stop.get("status") == "ABSENT",
            "idempotent_absence": stop.get("status") == "ABSENT",
            "stopped_or_absent": True,
        }
    )
    return lifecycle


def _delete_scheduled_task_typed(task_name: str, expected_action_parts: Sequence[str]) -> dict:
    lifecycle: dict[str, object] = {
        "status": "FAILED",
        "task_name": task_name,
        "phase": "delete",
        "operations": [],
        "absence_proven": False,
    }
    probe = _classify_task_probe(
        _run_command(_scheduled_task_probe_command(task_name)), expected_action_parts
    )
    lifecycle["initial_probe"] = probe
    if probe["status"] == "ABSENT":
        lifecycle.update({"status": "PASS", "absence_proven": True, "idempotent_absence": True})
        return lifecycle
    if probe["status"] != "PRESENT":
        lifecycle["blocked_reason"] = "scheduled task ownership was not proven"
        return lifecycle
    if str(probe.get("evidence", {}).get("runtime_state") or "").casefold() == "running":
        lifecycle["blocked_reason"] = "scheduled task is still running; stop phase must pass first"
        return lifecycle

    delete = _typed_json_result(
        _run_command(_scheduled_task_delete_command(task_name, expected_action_parts)),
        operation="delete",
    )
    lifecycle["operations"].append(delete)
    if delete.get("status") not in {"DELETED", "ABSENT"}:
        lifecycle["blocked_reason"] = "scheduled task delete failed"
        return lifecycle

    final_probe = _classify_task_probe(
        _run_command(_scheduled_task_probe_command(task_name)), expected_action_parts
    )
    lifecycle["final_probe"] = final_probe
    if final_probe["status"] != "ABSENT":
        lifecycle["blocked_reason"] = "scheduled task absence was not proven"
        return lifecycle
    lifecycle.update({"status": "PASS", "absence_proven": True, "idempotent_absence": False})
    return lifecycle


def _remove_scheduled_task_typed(task_name: str, expected_action_parts: Sequence[str]) -> dict:
    stop = _stop_scheduled_task_typed(task_name, expected_action_parts)
    if stop.get("status") != "PASS" or stop.get("absence_proven") is True:
        return {
            **stop,
            "phase": "full",
            "stop": stop,
            "delete": None,
        }
    delete = _delete_scheduled_task_typed(task_name, expected_action_parts)
    return {
        "status": delete.get("status"),
        "task_name": task_name,
        "phase": "full",
        "stop": stop,
        "delete": delete,
        "operations": [*stop.get("operations", []), *delete.get("operations", [])],
        "absence_proven": bool(delete.get("absence_proven")),
        "idempotent_absence": False,
        **(
            {"blocked_reason": delete.get("blocked_reason", "scheduled task deletion failed")}
            if delete.get("status") != "PASS"
            else {}
        ),
    }


def _self_enrollment_registration_command(args: argparse.Namespace) -> list[str]:
    program_data_root = Path(args.program_data_root).expanduser().resolve()
    command: list[str] = []
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
        ("--logistics-profile-path", "logistics_profile_path"),
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
    if bool(getattr(args, "require_machine_credential_bundle", False)):
        command.append("--require-machine-credential-bundle")
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
    result = _run_imported_main("tools.register_label_match_worker_pc", command)
    stdout = str(result.pop("stdout", "") or "")
    stderr = str(result.pop("stderr", "") or "")
    result["stdout_omitted"] = bool(stdout) or int(result.get("stdout_bytes") or 0) > 0
    result["stderr_omitted"] = bool(stderr) or int(result.get("stderr_bytes") or 0) > 0
    result.setdefault("stdout_bytes", len(stdout.encode("utf-8", errors="replace")))
    result.setdefault("stderr_bytes", len(stderr.encode("utf-8", errors="replace")))
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
    parser.add_argument("--app-root", default=_default_app_root())
    parser.add_argument("--app-settings-path", default="")
    parser.add_argument("--runner-exe", default="")
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
    parser.add_argument("--require-machine-credential-bundle", action="store_true")
    parser.add_argument("--logistics-profile-path", default="")
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
    parser.add_argument("--rollback", action="store_true")
    parser.add_argument(
        "--task-removal-phase",
        choices=("full", "stop", "delete"),
        default="full",
    )
    parser.add_argument("--confirm-production-install", action="store_true")
    parser.add_argument("--task-run-user", default="")
    parser.add_argument("--app-run-user", default="")
    parser.add_argument("--task-run-password-env", default="")
    parser.add_argument("--task-run-password-file", default="")
    parser.add_argument("--allow-interactive-task-for-local-test", action="store_true")
    parser.add_argument("--allow-noncanonical-layout-for-test", action="store_true")
    args = parser.parse_args(argv)
    removal = bool(args.uninstall or args.rollback)

    if args.uninstall and args.rollback:
        plan = {
            "report_version": "label-match-direct-sync-install-pack-v2",
            "status": "BLOCKED",
            "blocked_reason": "--uninstall and --rollback are mutually exclusive",
        }
        _write_json(Path(args.report_path), plan)
        print(f"install_pack_report={Path(args.report_path).resolve()}")
        return 2

    if not args.self_enroll and (not args.producer_manifest_path or not args.credential_path) and not removal:
        plan = {
            "report_version": "label-match-direct-sync-install-pack-v2",
            "status": "BLOCKED",
            "blocked_reason": "--producer-manifest-path and --credential-path are required unless --self-enroll is used",
            "self_enrollment": {"enabled": False},
        }
        _write_json(Path(args.report_path), plan)
        print(f"install_pack_report={Path(args.report_path).resolve()}")
        return 2

    if args.self_enroll and args.apply and str(getattr(args, "enrollment_token", "") or "").strip():
        plan = {
            "report_version": "label-match-direct-sync-install-pack-v2",
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

    if (
        args.apply
        and not removal
        and not task_principal_plan["field_layout_contract"]["production_layout_matches"]
        and not task_principal_plan["field_layout_contract"]["local_test_override_enabled"]
    ):
        task_principal_plan["status"] = "BLOCKED"
        task_principal_plan["blocked_reason"] = (
            f"noncanonical layout override requires {NONCANONICAL_LAYOUT_TEST_MODE_ENV}=1"
            if args.allow_noncanonical_layout_for_test
            else "production apply requires the canonical Label_Match field layout"
        )
        _write_json(Path(args.report_path), task_principal_plan)
        print(f"install_pack_report={Path(args.report_path).resolve()}")
        return 2

    if args.self_enroll and args.apply and not removal:
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
    if args.self_enroll and args.apply and not removal:
        plan["self_enrollment_registration"] = registration_result
    if plan["runtime_path_boundary"]["status"] != "PASS":
        plan["status"] = "BLOCKED"
        plan["blocked_reason"] = plan["runtime_path_boundary"]["blocked_reason"]
        _write_json(Path(args.report_path), plan)
        print(f"install_pack_report={Path(args.report_path).resolve()}")
        return 2
    if not removal and plan["install_preflight"]["status"] not in {"PASS", "NOT_RUN"}:
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
    if not removal and plan["task_runtime_acl"]["status"] != "PASS":
        plan["status"] = "BLOCKED"
        plan["blocked_reason"] = plan["task_runtime_acl"]["blocked_reason"]
        _write_json(Path(args.report_path), plan)
        print(f"install_pack_report={Path(args.report_path).resolve()}")
        return 2
    if not removal and plan["app_runtime_acl"]["status"] != "PASS":
        plan["status"] = "BLOCKED"
        plan["blocked_reason"] = plan["app_runtime_acl"]["blocked_reason"]
        _write_json(Path(args.report_path), plan)
        print(f"install_pack_report={Path(args.report_path).resolve()}")
        return 2

    if args.apply:
        if removal:
            removal_handlers = {
                "full": _remove_scheduled_task_typed,
                "stop": _stop_scheduled_task_typed,
                "delete": _delete_scheduled_task_typed,
            }
            plan["scheduled_task_lifecycle"] = removal_handlers[args.task_removal_phase](
                args.task_name, plan["task_launcher"]["command"]
            )
            plan["task_removal_phase"] = args.task_removal_phase
            plan["status"] = plan["scheduled_task_lifecycle"]["status"]
            if plan["status"] != "PASS":
                plan["blocked_reason"] = plan["scheduled_task_lifecycle"].get(
                    "blocked_reason", "scheduled task removal failed"
                )
            _write_json(Path(args.report_path), plan)
            print(f"install_pack_report={Path(args.report_path).resolve()}")
            return 0 if plan["status"] == "PASS" else 1
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
        if not removal:
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
            app_acl_result = _apply_task_runtime_acl(plan["app_runtime_acl"])
            plan["app_runtime_acl"]["apply_result"] = app_acl_result
            if app_acl_result["status"] == "FAIL":
                plan["status"] = "FAIL"
                plan["blocked_reason"] = app_acl_result["blocked_reason"]
                _write_json(Path(args.report_path), plan)
                print(f"install_pack_report={Path(args.report_path).resolve()}")
                return 1
            baseline_command = plan.get("source_scan_baseline_command") or []
            if baseline_command:
                baseline_result = _run_imported_main(
                    "tools.direct_sync_relay_runner", baseline_command[1:]
                )
                plan["source_scan_baseline_result"] = _reportable_child_result(
                    baseline_result
                )
                if int(baseline_result.get("returncode") or 0) != 0:
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
        command_result = _run_command(command)
        plan["command_result"] = _reportable_child_result(command_result)
        plan["status"] = "PASS" if command_result["returncode"] == 0 else "FAIL"

    _write_json(Path(args.report_path), plan)
    print(f"install_pack_report={Path(args.report_path).resolve()}")
    return 0 if plan["status"] in {"DRY_RUN", "PASS"} else 1


if __name__ == "__main__":
    raise SystemExit(main())

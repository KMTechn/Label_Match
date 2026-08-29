"""Current-user Limited TimeTrigger ownership for the Label relay.

The canonical installer only places protected code.  This module is called by
the unelevated ``--onboard-current-user`` product phase and never starts a task
manually or stops an existing process/task.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
from typing import Any, Callable, Mapping

TASK_CONTRACT_VERSION = "label-match-current-user-task-v1"
TASK_NAME = "direct-sync-relay-label-match"
LEGACY_TASK_NAME = "direct-sync-relay-label-match-current-pc"
SCHEDULED_RELAY_MODE = "--label-match-scheduled-relay"
CANONICAL_ROOT = Path(r"C:\KMTech\Apps\Label_Match\current")
MAX_COMMAND_OUTPUT_BYTES = 1024 * 1024


class CurrentUserScheduledTaskError(RuntimeError):
    pass


def _resolved(path: str | os.PathLike[str]) -> Path:
    return Path(path).expanduser().resolve(strict=False)


def _same_path(left: Path, right: Path) -> bool:
    return os.path.normcase(str(_resolved(left))) == os.path.normcase(
        str(_resolved(right))
    )


def build_current_user_task_spec(
    app_root: str | os.PathLike[str],
) -> dict[str, Any]:
    root = _resolved(app_root)
    if not _same_path(root, CANONICAL_ROOT):
        raise CurrentUserScheduledTaskError(
            "the scheduled task may bind only the canonical Label install root"
        )
    execute = root / "runtime" / "python.exe"
    entrypoint = root / "app" / "main.py"
    if not execute.is_file() or not entrypoint.is_file():
        raise CurrentUserScheduledTaskError(
            "canonical scheduled-task runtime or entrypoint is absent"
        )
    arguments = subprocess.list2cmdline(
        ["-I", "-B", str(entrypoint), SCHEDULED_RELAY_MODE, "--app-root", str(root)]
    )
    action_identity = json.dumps(
        {
            "arguments": arguments,
            "execute": str(execute),
            "working_directory": str(entrypoint.parent),
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return {
        "schema": TASK_CONTRACT_VERSION,
        "task_name": TASK_NAME,
        "legacy_task_name": LEGACY_TASK_NAME,
        "execute": str(execute),
        "arguments": arguments,
        "working_directory": str(entrypoint.parent),
        "action_sha256": hashlib.sha256(action_identity).hexdigest(),
        "principal": "exact_current_user",
        "logon_type": "InteractiveToken",
        "run_level": "Limited",
        "trigger": "TimeTrigger",
        "repetition_interval": "PT1M",
        "multiple_instances": "IgnoreNew",
        "start_when_available": True,
        "execution_time_limit": "PT2M",
    }


_TASK_POWERSHELL = r"""
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$spec = $env:KMTECH_LABEL_CURRENT_USER_TASK_SPEC | ConvertFrom-Json
if (
    [string]$spec.schema -cne 'label-match-current-user-task-v1' -or
    [string]$spec.task_name -cne 'direct-sync-relay-label-match' -or
    [string]$spec.legacy_task_name -cne 'direct-sync-relay-label-match-current-pc' -or
    [string]$spec.logon_type -cne 'InteractiveToken' -or
    [string]$spec.run_level -cne 'Limited' -or
    [string]$spec.repetition_interval -cne 'PT1M' -or
    [string]$spec.multiple_instances -cne 'IgnoreNew' -or
    [string]$spec.execution_time_limit -cne 'PT2M'
) { throw 'Current-user scheduled-task specification is invalid.' }

function Get-TextSha256([string]$Value) {
    $sha = [Security.Cryptography.SHA256]::Create()
    try {
        $bytes = [Text.UTF8Encoding]::new($false).GetBytes($Value)
        return ([BitConverter]::ToString($sha.ComputeHash($bytes))).Replace('-', '').ToLowerInvariant()
    }
    finally { $sha.Dispose() }
}

function Get-TaskSnapshot([string]$Name) {
    $matches = @(Get-ScheduledTask -TaskPath '\' -TaskName $Name -ErrorAction SilentlyContinue)
    if ($matches.Count -gt 1) { throw "More than one root task matched $Name." }
    if ($matches.Count -eq 0) { return [ordered]@{ exists = $false; name = $Name } }
    $task = $matches[0]
    $xmlText = Export-ScheduledTask -TaskPath '\' -TaskName $Name -ErrorAction Stop
    [xml]$xml = $xmlText
    $actions = @($task.Actions)
    $triggers = @($xml.Task.Triggers.ChildNodes | Where-Object NodeType -eq 'Element')
    return [ordered]@{
        exists = $true
        name = $Name
        state = [string]$task.State
        execute = if ($actions.Count -eq 1) { [string]$actions[0].Execute } else { '' }
        arguments = if ($actions.Count -eq 1) { [string]$actions[0].Arguments } else { '' }
        working_directory = if ($actions.Count -eq 1) { [string]$actions[0].WorkingDirectory } else { '' }
        action_count = $actions.Count
        principal_user_id = [string]$task.Principal.UserId
        principal_logon_type = [string]$task.Principal.LogonType
        principal_run_level = [string]$task.Principal.RunLevel
        trigger_count = $triggers.Count
        trigger_type = if ($triggers.Count -eq 1) { [string]$triggers[0].LocalName } else { '' }
        repetition_interval = if ($triggers.Count -eq 1) { [string]$triggers[0].Repetition.Interval } else { '' }
        multiple_instances = [string]$xml.Task.Settings.MultipleInstancesPolicy
        start_when_available = ([string]$xml.Task.Settings.StartWhenAvailable -ieq 'true')
        execution_time_limit = [string]$xml.Task.Settings.ExecutionTimeLimit
        xml = $xmlText
        xml_sha256 = Get-TextSha256 $xmlText
    }
}

function Test-ExactTask($Snapshot) {
    if (-not [bool]$Snapshot.exists) { return $false }
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principalMatches = (
        [string]$Snapshot.principal_user_id -ieq [string]$identity.Name -or
        [string]$Snapshot.principal_user_id -ieq [string]$identity.User.Value
    )
    return (
        [int]$Snapshot.action_count -eq 1 -and
        [string]$Snapshot.execute -ceq [string]$spec.execute -and
        [string]$Snapshot.arguments -ceq [string]$spec.arguments -and
        [string]$Snapshot.working_directory -ceq [string]$spec.working_directory -and
        $principalMatches -and
        [string]$Snapshot.principal_logon_type -in @('Interactive', 'InteractiveToken') -and
        [string]$Snapshot.principal_run_level -ceq 'Limited' -and
        [int]$Snapshot.trigger_count -eq 1 -and
        [string]$Snapshot.trigger_type -ceq 'TimeTrigger' -and
        [string]$Snapshot.repetition_interval -ceq 'PT1M' -and
        [string]$Snapshot.multiple_instances -ceq 'IgnoreNew' -and
        [bool]$Snapshot.start_when_available -and
        [string]$Snapshot.execution_time_limit -ceq 'PT2M'
    )
}

function Restore-TaskSnapshot($Snapshot) {
    $current = Get-TaskSnapshot ([string]$Snapshot.name)
    if ([bool]$current.exists -and [string]$current.state -ceq 'Running') {
        throw "Refusing to stop a running task during restoration: $($Snapshot.name)"
    }
    if ([bool]$Snapshot.exists) {
        Register-ScheduledTask -TaskPath '\' -TaskName ([string]$Snapshot.name) -Xml ([string]$Snapshot.xml) -Force | Out-Null
    }
    elseif ([bool]$current.exists) {
        Unregister-ScheduledTask -TaskPath '\' -TaskName ([string]$Snapshot.name) -Confirm:$false
    }
    $after = Get-TaskSnapshot ([string]$Snapshot.name)
    if ([bool]$after.exists -ne [bool]$Snapshot.exists) { throw 'Task restoration existence readback failed.' }
    if ([bool]$Snapshot.exists -and [string]$after.xml_sha256 -cne [string]$Snapshot.xml_sha256) {
        throw 'Task restoration XML readback failed.'
    }
}

$beforeCanonical = Get-TaskSnapshot ([string]$spec.task_name)
$beforeLegacy = Get-TaskSnapshot ([string]$spec.legacy_task_name)
foreach ($snapshot in @($beforeCanonical, $beforeLegacy)) {
    if ([bool]$snapshot.exists -and [string]$snapshot.state -ceq 'Running') {
        throw "Refusing to stop or replace a running scheduled task: $($snapshot.name)"
    }
}
$operation = [string]$env:KMTECH_LABEL_CURRENT_USER_TASK_OPERATION
if ($operation -notin @('Apply', 'Remove')) { throw 'Scheduled-task operation is invalid.' }

try {
    if ($operation -ceq 'Apply') {
        $action = 'REUSED'
        if (-not (Test-ExactTask $beforeCanonical)) {
            $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
            $taskAction = New-ScheduledTaskAction -Execute ([string]$spec.execute) -Argument ([string]$spec.arguments) -WorkingDirectory ([string]$spec.working_directory)
            $trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 1)
            $settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Minutes 2)
            $principal = New-ScheduledTaskPrincipal -UserId $identity.Name -LogonType Interactive -RunLevel Limited
            Register-ScheduledTask -TaskPath '\' -TaskName ([string]$spec.task_name) -Action $taskAction -Trigger $trigger -Settings $settings -Principal $principal -Description 'KMTech canonical current-user Label DirectSync TimeTrigger' -Force | Out-Null
            $action = if ([bool]$beforeCanonical.exists) { 'REPLACED' } else { 'CREATED' }
        }
        $afterCanonical = Get-TaskSnapshot ([string]$spec.task_name)
        if (-not (Test-ExactTask $afterCanonical)) { throw 'Canonical current-user task exact readback failed.' }
        if ([bool]$beforeLegacy.exists) {
            Unregister-ScheduledTask -TaskPath '\' -TaskName ([string]$spec.legacy_task_name) -Confirm:$false
        }
        $afterLegacy = Get-TaskSnapshot ([string]$spec.legacy_task_name)
        if ([bool]$afterLegacy.exists) { throw 'Legacy Label task survived migration.' }
        [ordered]@{
            schema = [string]$spec.schema
            status = 'PASS'
            action = $action
            manual_start = $false
            process_or_task_stopped = $false
            spec = $spec
            preimage = [ordered]@{ canonical = $beforeCanonical; legacy = $beforeLegacy }
            canonical = $afterCanonical
            legacy = $afterLegacy
        } | ConvertTo-Json -Depth 20 -Compress
        exit 0
    }

    foreach ($snapshot in @($beforeCanonical, $beforeLegacy)) {
        if ([bool]$snapshot.exists) {
            Unregister-ScheduledTask -TaskPath '\' -TaskName ([string]$snapshot.name) -Confirm:$false
        }
    }
    $afterCanonical = Get-TaskSnapshot ([string]$spec.task_name)
    $afterLegacy = Get-TaskSnapshot ([string]$spec.legacy_task_name)
    if ([bool]$afterCanonical.exists -or [bool]$afterLegacy.exists) {
        throw 'Current-user Label task removal readback failed.'
    }
    [ordered]@{
        schema = [string]$spec.schema
        status = 'ABSENT'
        manual_start = $false
        process_or_task_stopped = $false
        spec = $spec
        preimage = [ordered]@{ canonical = $beforeCanonical; legacy = $beforeLegacy }
        canonical = $afterCanonical
        legacy = $afterLegacy
    } | ConvertTo-Json -Depth 20 -Compress
    exit 0
}
catch {
    $failure = $_.Exception.Message
    $rollbackFailure = ''
    try {
        Restore-TaskSnapshot $beforeCanonical
        Restore-TaskSnapshot $beforeLegacy
    }
    catch { $rollbackFailure = $_.Exception.Message }
    [ordered]@{
        schema = [string]$spec.schema
        status = if ($rollbackFailure) { 'ROLLBACK_FAILED' } else { 'FAILED_ROLLED_BACK' }
        failure = $failure
        rollback_failure = $rollbackFailure
        manual_start = $false
        process_or_task_stopped = $false
        spec = $spec
        preimage = [ordered]@{ canonical = $beforeCanonical; legacy = $beforeLegacy }
    } | ConvertTo-Json -Depth 20 -Compress
    exit 1
}
"""


def _powershell_executable() -> Path:
    system_root = Path(os.environ.get("SystemRoot") or r"C:\Windows")
    executable = (
        system_root / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe"
    )
    if not executable.is_file():
        raise CurrentUserScheduledTaskError("Windows PowerShell is unavailable")
    return executable


def _run_task_operation(
    operation: str,
    spec: Mapping[str, Any],
    *,
    runner: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    selected_runner = subprocess.run if runner is None else runner
    if runner is None and os.name != "nt":
        raise CurrentUserScheduledTaskError(
            "current-user scheduled-task registration is available only on Windows"
        )
    environment = os.environ.copy()
    environment["KMTECH_LABEL_CURRENT_USER_TASK_OPERATION"] = operation
    environment["KMTECH_LABEL_CURRENT_USER_TASK_SPEC"] = json.dumps(
        dict(spec), ensure_ascii=True, separators=(",", ":"), sort_keys=True
    )
    command = [
        str(_powershell_executable()) if runner is None else "powershell.exe",
        "-NoLogo",
        "-NoProfile",
        "-NonInteractive",
        "-ExecutionPolicy",
        "RemoteSigned",
        "-Command",
        "-",
    ]
    completed = selected_runner(
        command,
        input=_TASK_POWERSHELL,
        text=True,
        capture_output=True,
        env=environment,
        timeout=60,
        check=False,
    )
    output = str(getattr(completed, "stdout", "") or "")
    error = str(getattr(completed, "stderr", "") or "")
    if (
        len(output.encode("utf-8")) > MAX_COMMAND_OUTPUT_BYTES
        or len(error.encode("utf-8")) > MAX_COMMAND_OUTPUT_BYTES
    ):
        raise CurrentUserScheduledTaskError(
            "scheduled-task command output is oversized"
        )
    lines = [line for line in output.splitlines() if line.strip()]
    try:
        report = json.loads(lines[-1]) if lines else {}
    except json.JSONDecodeError as exc:
        raise CurrentUserScheduledTaskError(
            "scheduled-task command returned malformed evidence"
        ) from exc
    if not isinstance(report, dict):
        raise CurrentUserScheduledTaskError(
            "scheduled-task command returned a non-object report"
        )
    if int(getattr(completed, "returncode", 1)) != 0:
        detail = str(
            report.get("failure") or error or "scheduled-task operation failed"
        )
        raise CurrentUserScheduledTaskError(detail[:500])
    return report


def install_current_user_scheduled_task(
    app_root: str | os.PathLike[str],
    *,
    runner: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    spec = build_current_user_task_spec(app_root)
    report = _run_task_operation("Apply", spec, runner=runner)
    canonical = report.get("canonical") if isinstance(report, dict) else None
    if (
        report.get("schema") != TASK_CONTRACT_VERSION
        or report.get("status") != "PASS"
        or report.get("manual_start") is not False
        or report.get("process_or_task_stopped") is not False
        or not isinstance(canonical, dict)
        or canonical.get("execute") != spec["execute"]
        or canonical.get("arguments") != spec["arguments"]
        or canonical.get("working_directory") != spec["working_directory"]
        or canonical.get("repetition_interval") != "PT1M"
        or canonical.get("principal_run_level") != "Limited"
    ):
        raise CurrentUserScheduledTaskError(
            "scheduled-task apply evidence failed exact product readback"
        )
    return report


def remove_current_user_scheduled_task(
    app_root: str | os.PathLike[str],
    *,
    runner: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    spec = build_current_user_task_spec(app_root)
    report = _run_task_operation("Remove", spec, runner=runner)
    if (
        report.get("schema") != TASK_CONTRACT_VERSION
        or report.get("status") != "ABSENT"
        or report.get("manual_start") is not False
        or report.get("process_or_task_stopped") is not False
    ):
        raise CurrentUserScheduledTaskError(
            "scheduled-task removal evidence failed exact product readback"
        )
    return report

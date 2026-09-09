"""Ownership checks and normal retirement of historical Label scheduled tasks.

HKCU Run and the resident product relay are the current startup authority.
Only migration/removal consumers retain the old canonical task specification;
this module never creates, starts, or force-stops a task or process.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
from typing import Any, Callable, Mapping

from writer_session_fence import writer_sink

TASK_CONTRACT_VERSION = "label-match-current-user-task-v1"
TASK_NAME = "direct-sync-relay-label-match"
LEGACY_TASK_NAME = "direct-sync-relay-label-match-current-pc"
LEGACY_TASK_QUIESCENCE_VERSION = "label-match-legacy-task-quiescence-v1"
LEGACY_TASK_REQUIRED_STATE = "ABSENT_OR_DISABLED"
LEGACY_TASK_REMEDIATION = (
    "Disable or remove root scheduled task "
    r"\direct-sync-relay-label-match-current-pc before Label enrollment; "
    "the supported replacement is HKCU Run KMTech.LabelMatch.Relay and "
    "the current-user resident product relay."
)
SCHEDULED_RELAY_MODE = "--label-match-scheduled-relay"
CANONICAL_ROOT = Path(r"C:\KMTech\Apps\Label_Match\current")
MAX_COMMAND_OUTPUT_BYTES = 1024 * 1024


class CurrentUserScheduledTaskError(RuntimeError):
    pass


def evaluate_legacy_task_quiescence(
    snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    """Classify the legacy task without accepting an enabled compatibility writer."""

    report: dict[str, Any] = {
        "schema": LEGACY_TASK_QUIESCENCE_VERSION,
        "status": "FAIL",
        "reason_code": "LEGACY_TASK_OBSERVATION_INVALID",
        "legacy_task_name": LEGACY_TASK_NAME,
        "required_state": LEGACY_TASK_REQUIRED_STATE,
        "read_only": True,
        "task_or_process_mutated": False,
        "remediation": LEGACY_TASK_REMEDIATION,
        "observation": dict(snapshot) if isinstance(snapshot, Mapping) else {},
    }
    if not isinstance(snapshot, Mapping):
        return report
    observed_name = snapshot.get("name")
    if (
        not isinstance(observed_name, str)
        or observed_name.casefold() != LEGACY_TASK_NAME.casefold()
        or type(snapshot.get("exists")) is not bool
    ):
        return report
    if snapshot["exists"] is False:
        report.update(status="PASS", reason_code="LEGACY_TASK_ABSENT")
        return report

    state = snapshot.get("state")
    if not isinstance(state, str) or not state.strip():
        return report
    if state.strip().casefold() == "disabled":
        report.update(status="PASS", reason_code="LEGACY_TASK_DISABLED")
        return report
    report["reason_code"] = "LEGACY_TASK_PRESENT_ENABLED"
    return report


def _resolved(path: str | os.PathLike[str]) -> Path:
    return Path(path).expanduser().resolve(strict=False)


def _same_path(left: Path, right: Path) -> bool:
    return os.path.normcase(str(_resolved(left))) == os.path.normcase(
        str(_resolved(right))
    )


def build_current_user_task_spec(
    app_root: str | os.PathLike[str],
) -> dict[str, Any]:
    """Describe the historical task solely to prove migration ownership."""
    root = _resolved(app_root)
    if not _same_path(root, CANONICAL_ROOT):
        raise CurrentUserScheduledTaskError(
            "the scheduled task may bind only the canonical Label install root"
        )
    execute = root / "runtime" / "python.exe"
    entrypoint = root / "app" / "main.py"
    from current_user_onboarding import resolve_current_user_onboarding_paths

    log_path = (
        resolve_current_user_onboarding_paths(root).logs_dir
        / "scheduled_direct_sync_relay.jsonl"
    )
    arguments = subprocess.list2cmdline(
        ["-I", "-B", str(entrypoint), SCHEDULED_RELAY_MODE, "--app-root", str(root),
         "--log-path", str(log_path)]
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


_LEGACY_TASK_READBACK_POWERSHELL = r"""
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$name = 'direct-sync-relay-label-match-current-pc'

function Get-TextSha256([string]$Value) {
    $sha = [Security.Cryptography.SHA256]::Create()
    try {
        $bytes = [Text.UTF8Encoding]::new($false).GetBytes($Value)
        return ([BitConverter]::ToString($sha.ComputeHash($bytes))).Replace('-', '').ToLowerInvariant()
    }
    finally { $sha.Dispose() }
}

$matches = @(
    Get-ScheduledTask -TaskPath '\' -ErrorAction Stop |
        Where-Object { [string]$_.TaskName -ieq $name }
)
if ($matches.Count -gt 1) { throw "More than one root task matched $name." }
if ($matches.Count -eq 0) {
    [ordered]@{ exists = $false; name = $name } | ConvertTo-Json -Compress
    exit 0
}
$task = $matches[0]
$actions = @($task.Actions)
$actionIdentity = @(
    $actions | ForEach-Object {
        ([string]$_.Execute) + [char]0 + ([string]$_.Arguments) + [char]0 +
            ([string]$_.WorkingDirectory)
    }
) -join ([char]0)
[ordered]@{
    exists = $true
    name = $name
    task_path = [string]$task.TaskPath
    state = [string]$task.State
    action_count = $actions.Count
    action_identity_sha256 = Get-TextSha256 $actionIdentity
    principal_user_id = [string]$task.Principal.UserId
    principal_logon_type = [string]$task.Principal.LogonType
    principal_run_level = [string]$task.Principal.RunLevel
} | ConvertTo-Json -Compress
exit 0
"""


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
    $matches = @(
        Get-ScheduledTask -TaskPath '\' -ErrorAction Stop |
            Where-Object { [string]$_.TaskName -ieq $Name }
    )
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
        enabled = [bool]$task.Settings.Enabled
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
    $userName = [string]$identity.Name
    $sam = $userName.Split('\')[-1]
    $principalMatches = (
        [string]$Snapshot.principal_user_id -ieq $userName -or
        [string]$Snapshot.principal_user_id -ieq $sam -or
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


$beforeCanonical = Get-TaskSnapshot ([string]$spec.task_name)
$beforeLegacy = Get-TaskSnapshot ([string]$spec.legacy_task_name)
$report = [ordered]@{
    schema = [string]$spec.schema
    status = 'UNKNOWN'
    action = 'ALREADY_ABSENT'
    manual_start = $false
    process_or_task_stopped = $false
    task_disabled = $false
    spec = $spec
    preimage = [ordered]@{ canonical = $beforeCanonical; legacy = $beforeLegacy }
}
try {
    # Historical SYSTEM-task cleanup belongs to the elevated public installer.
    # An unrelated task is never removed just because its name is familiar.
    if ([bool]$beforeLegacy.exists -and [string]$beforeLegacy.state -ine 'Disabled') {
        throw 'Enabled historical Label task requires normal owned-task cleanup by the public installer.'
    }
    if ([bool]$beforeCanonical.exists) {
        if (-not (Test-ExactTask $beforeCanonical)) {
            throw 'Refusing to retire a current-user task with different ownership or command.'
        }
        $service = New-Object -ComObject 'Schedule.Service'
        $service.Connect()
        $folder = $service.GetFolder('\')
        if ([bool]$beforeCanonical.enabled) {
            Disable-ScheduledTask -TaskPath '\' -TaskName ([string]$spec.task_name) -ErrorAction Stop | Out-Null
            $report.task_disabled = $true
        }
        $deadline = [DateTime]::UtcNow.AddSeconds(120)
        do {
            $current = Get-TaskSnapshot ([string]$spec.task_name)
            if (-not (Test-ExactTask $current) -or [bool]$current.enabled) {
                throw 'Historical current-user task changed during retirement.'
            }
            # Disabled is not proof that an already running instance exited.
            # Exact current-user/Limited ownership makes GetInstances complete.
            $instances = $folder.GetTask([string]$spec.task_name).GetInstances(0)
            if ($null -eq $instances -or $null -eq $instances.Count -or [int]$instances.Count -lt 0) {
                throw 'Historical current-user task instance absence could not be observed.'
            }
            if ([int]$instances.Count -eq 0) { break }
            if ([DateTime]::UtcNow -ge $deadline) {
                throw 'Historical current-user task did not finish naturally before the retirement timeout.'
            }
            Start-Sleep -Milliseconds 200
        } while ($true)
        $final = Get-TaskSnapshot ([string]$spec.task_name)
        if (-not (Test-ExactTask $final) -or [bool]$final.enabled) {
            throw 'Historical current-user task changed after its last instance finished.'
        }
        Unregister-ScheduledTask -TaskPath '\' -TaskName ([string]$spec.task_name) -Confirm:$false -ErrorAction Stop
        $report.action = 'RETIRED'
    }
    $afterCanonical = Get-TaskSnapshot ([string]$spec.task_name)
    if ([bool]$afterCanonical.exists) { throw 'Historical current-user task absence was not proven.' }
    $report.status = 'ABSENT'
    $report.canonical = $afterCanonical
    $report.legacy = $beforeLegacy
    $report | ConvertTo-Json -Depth 20 -Compress
    exit 0
}
catch {
    # Keep a failed migration disabled; never recreate/re-enable an obsolete lane.
    $report.status = 'FAILED'
    $report.failure = $_.Exception.Message
    $report | ConvertTo-Json -Depth 20 -Compress
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


def read_legacy_system_task_quiescence(
    *, runner: Callable[..., Any] | None = None
) -> dict[str, Any]:
    """Read the legacy root task and require it to be absent or disabled."""

    selected_runner = subprocess.run if runner is None else runner
    if runner is None and os.name != "nt":
        raise CurrentUserScheduledTaskError(
            "legacy scheduled-task readback is available only on Windows"
        )
    command = [
        str(_powershell_executable()) if runner is None else "powershell.exe",
        "-NoLogo",
        "-NoProfile",
        "-NonInteractive",
        "-ExecutionPolicy",
        "RemoteSigned",
        "-Command",
        _LEGACY_TASK_READBACK_POWERSHELL,
    ]
    completed = selected_runner(
        command,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    output = str(getattr(completed, "stdout", "") or "")
    error = str(getattr(completed, "stderr", "") or "")
    if (
        len(output.encode("utf-8")) > MAX_COMMAND_OUTPUT_BYTES
        or len(error.encode("utf-8")) > MAX_COMMAND_OUTPUT_BYTES
    ):
        raise CurrentUserScheduledTaskError(
            "legacy scheduled-task readback output is oversized"
        )
    if int(getattr(completed, "returncode", 1)) != 0:
        raise CurrentUserScheduledTaskError(
            (error or "legacy scheduled-task readback failed")[:500]
        )
    lines = [line for line in output.splitlines() if line.strip()]
    try:
        snapshot = json.loads(lines[-1]) if lines else None
    except json.JSONDecodeError as exc:
        raise CurrentUserScheduledTaskError(
            "legacy scheduled-task readback returned malformed evidence"
        ) from exc
    if not isinstance(snapshot, dict):
        raise CurrentUserScheduledTaskError(
            "legacy scheduled-task readback returned a non-object snapshot"
        )
    return evaluate_legacy_task_quiescence(snapshot)


def _run_task_removal(
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
    environment["KMTECH_LABEL_CURRENT_USER_TASK_SPEC"] = json.dumps(
        dict(spec), ensure_ascii=True, separators=(",", ":"), sort_keys=True
    )
    encoded = base64.b64encode(_TASK_POWERSHELL.encode("utf-16le")).decode("ascii")
    command = [
        str(_powershell_executable()) if runner is None else "powershell.exe",
        "-NoLogo",
        "-NoProfile",
        "-NonInteractive",
        "-ExecutionPolicy",
        "RemoteSigned",
        "-EncodedCommand",
        encoded,
    ]
    completed = selected_runner(
        command,
        text=True,
        capture_output=True,
        env=environment,
        timeout=150,
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




@writer_sink("scheduled_task_remove")
def remove_current_user_scheduled_task(
    app_root: str | os.PathLike[str],
    *,
    runner: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    spec = build_current_user_task_spec(app_root)
    report = _run_task_removal(spec, runner=runner)
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

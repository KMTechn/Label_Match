import json
import os
from pathlib import Path
import shutil
import subprocess
from types import SimpleNamespace

import pytest

import current_user_scheduled_task as scheduled_task


def _canonical_fixture(monkeypatch, tmp_path: Path) -> Path:
    root = (tmp_path / "canonical").resolve()
    (root / "runtime").mkdir(parents=True)
    (root / "app").mkdir()
    (root / "runtime" / "python.exe").write_bytes(b"signed-runtime-fixture")
    (root / "app" / "main.py").write_text("pass\n", encoding="utf-8")
    monkeypatch.setattr(scheduled_task, "CANONICAL_ROOT", root)
    return root


def _successful_runner(command, **kwargs):
    spec = json.loads(kwargs["env"]["KMTECH_LABEL_CURRENT_USER_TASK_SPEC"])
    status = "ABSENT"
    report = {
        "schema": scheduled_task.TASK_CONTRACT_VERSION,
        "status": status,
        "action": "CREATED",
        "manual_start": False,
        "process_or_task_stopped": False,
        "canonical": {
            "execute": spec["execute"],
            "arguments": spec["arguments"],
            "working_directory": spec["working_directory"],
            "repetition_interval": "PT1M",
            "principal_run_level": "Limited",
        },
        "legacy": {"exists": False, "name": scheduled_task.LEGACY_TASK_NAME},
        "legacy_quiescence_before": {
            "status": "PASS",
            "reason_code": "LEGACY_TASK_ABSENT",
            "required_state": scheduled_task.LEGACY_TASK_REQUIRED_STATE,
        },
    }
    return SimpleNamespace(returncode=0, stdout=json.dumps(report), stderr="")


def test_task_spec_is_exact_current_user_limited_pt1m(monkeypatch, tmp_path):
    root = _canonical_fixture(monkeypatch, tmp_path)

    spec = scheduled_task.build_current_user_task_spec(root)

    assert spec["task_name"] == "direct-sync-relay-label-match"
    assert spec["legacy_task_name"] == "direct-sync-relay-label-match-current-pc"
    assert spec["execute"] == str(root / "runtime" / "python.exe")
    assert spec["working_directory"] == str(root / "app")
    assert "--label-match-scheduled-relay" in spec["arguments"]
    assert spec["logon_type"] == "InteractiveToken"
    assert spec["run_level"] == "Limited"
    assert spec["repetition_interval"] == "PT1M"
    assert spec["multiple_instances"] == "IgnoreNew"
    assert spec["start_when_available"] is True
    assert spec["execution_time_limit"] == "PT2M"
    assert len(spec["action_sha256"]) == 64


def test_task_retirement_returns_exact_readback_without_manual_start(
    monkeypatch, tmp_path
):
    root = _canonical_fixture(monkeypatch, tmp_path)

    report = scheduled_task.remove_current_user_scheduled_task(
        root, runner=_successful_runner
    )

    assert report["status"] == "ABSENT"
    assert report["manual_start"] is False
    assert report["process_or_task_stopped"] is False




def test_task_script_refuses_manual_start_and_task_stop() -> None:
    source = scheduled_task._TASK_POWERSHELL

    assert "New-ScheduledTaskTrigger" not in source
    assert "Register-ScheduledTask" not in source
    assert "Disable-ScheduledTask" in source
    assert "Unregister-ScheduledTask" in source
    assert "Start-ScheduledTask" not in source
    assert "Stop-ScheduledTask" not in source
    assert "Stop-Process" not in source
    assert "schtasks /run" not in source.lower()


def test_task_operation_sends_script_as_encoded_command(monkeypatch, tmp_path):
    root = _canonical_fixture(monkeypatch, tmp_path)
    captured: dict[str, object] = {}

    def runner(command, **kwargs):
        captured["command"] = list(command)
        captured["input"] = kwargs.get("input")
        return _successful_runner(command, **kwargs)

    scheduled_task.remove_current_user_scheduled_task(root, runner=runner)

    command = captured["command"]
    assert "-EncodedCommand" in command
    assert command[command.index("-EncodedCommand") + 1]
    assert captured["input"] in (None, "")
    assert command[-2:] != ["-Command", "-"]


def test_task_powershell_accepts_sam_account_principal_readback() -> None:
    source = scheduled_task._TASK_POWERSHELL
    assert "$sam = $userName.Split('\\')[-1]" in source
    assert "-ieq $sam" in source


@pytest.mark.parametrize(
    ("snapshot", "status", "reason"),
    [
        (
            {"exists": False, "name": scheduled_task.LEGACY_TASK_NAME},
            "PASS",
            "LEGACY_TASK_ABSENT",
        ),
        (
            {
                "exists": True,
                "name": scheduled_task.LEGACY_TASK_NAME,
                "state": "Disabled",
            },
            "PASS",
            "LEGACY_TASK_DISABLED",
        ),
        (
            {
                "exists": True,
                "name": scheduled_task.LEGACY_TASK_NAME,
                "state": "Ready",
                "principal_user_id": "SYSTEM",
            },
            "FAIL",
            "LEGACY_TASK_PRESENT_ENABLED",
        ),
    ],
)
def test_legacy_task_quiescence_is_fail_closed(snapshot, status, reason):
    report = scheduled_task.evaluate_legacy_task_quiescence(snapshot)

    assert report["status"] == status
    assert report["reason_code"] == reason
    assert report["required_state"] == "ABSENT_OR_DISABLED"
    assert report["read_only"] is True
    assert report["task_or_process_mutated"] is False
    if status == "FAIL":
        assert r"\direct-sync-relay-label-match-current-pc" in report["remediation"]
        assert "Disable or remove" in report["remediation"]


def test_legacy_task_readback_uses_only_read_commands_and_rejects_enabled_task():
    def runner(command, **kwargs):
        source = command[-1]
        assert "Get-ScheduledTask" in source
        assert "input" not in kwargs
        for forbidden in (
            "Register-ScheduledTask",
            "Unregister-ScheduledTask",
            "Enable-ScheduledTask",
            "Disable-ScheduledTask",
            "Start-ScheduledTask",
            "Stop-ScheduledTask",
            "Stop-Process",
        ):
            assert forbidden not in source
        snapshot = {
            "exists": True,
            "name": scheduled_task.LEGACY_TASK_NAME,
            "state": "Ready",
            "principal_user_id": "SYSTEM",
            "principal_logon_type": "ServiceAccount",
            "principal_run_level": "Highest",
        }
        return SimpleNamespace(returncode=0, stdout=json.dumps(snapshot), stderr="")

    report = scheduled_task.read_legacy_system_task_quiescence(runner=runner)

    assert report["status"] == "FAIL"
    assert report["reason_code"] == "LEGACY_TASK_PRESENT_ENABLED"




def test_task_spec_rejects_noncanonical_root(tmp_path):
    root = tmp_path / "not-canonical"
    root.mkdir()

    with pytest.raises(
        scheduled_task.CurrentUserScheduledTaskError,
        match="canonical Label install root",
    ):
        scheduled_task.build_current_user_task_spec(root)


def _run_task_script_fixture(monkeypatch, tmp_path, **case):
    """Run the shipped PowerShell decision against task cmdlet doubles only."""
    powershell = shutil.which("powershell.exe")
    if not powershell:
        pytest.skip("Windows PowerShell is required")
    root = _canonical_fixture(monkeypatch, tmp_path)
    spec = scheduled_task.build_current_user_task_spec(root)
    mutations = tmp_path / "task-mutations.txt"
    script = tmp_path / "task-decision.ps1"
    script.write_text(
        r"""
$case = $env:KMTECH_LABEL_TASK_TEST_CASE | ConvertFrom-Json
$expected = $env:KMTECH_LABEL_CURRENT_USER_TASK_SPEC | ConvertFrom-Json
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = switch ([string]$case.principal) {
    'sid' { $identity.User.Value }
    'sam' { $identity.Name.Split('\')[-1] }
    default { $identity.Name }
}
$script:taskFixtureState = [pscustomobject]@{
    state = if ($case.state) { $case.state } else { 'Running' }
    execute = $expected.execute; arguments = $expected.arguments
    working_directory = $expected.working_directory
    action_count = 1; principal_user_id = $principal
    principal_logon_type = 'Interactive'; principal_run_level = 'Limited'
    trigger_count = 1; trigger_type = 'TimeTrigger'; repetition_interval = 'PT1M'
    multiple_instances = 'IgnoreNew'; start_when_available = $true
    execution_time_limit = 'PT2M'
}
if ($case.field) { $script:taskFixtureState.($case.field) = $case.value }
$script:taskFixtureQueries = 0
$script:taskFixturePresent = $case.present -ne $false
$script:taskFixtureEnabled = $case.enabled -ne $false
$script:taskFixtureInstanceQueries = 0
$script:taskFixtureLastCount = 0
function Get-ScheduledTask {
    param($TaskPath, $ErrorAction)
    $script:taskFixtureQueries++
    if ($case.readback_drift -and $script:taskFixtureQueries -ge 3) {
        $script:taskFixtureState.execute = 'C:\unexpected\python.exe'
    }
    $s = $script:taskFixtureState
    if ($script:taskFixturePresent) { [pscustomobject]@{
        TaskName = $expected.task_name; State = $s.state
        Settings = [pscustomobject]@{Enabled=$script:taskFixtureEnabled}
        Actions = @(for ($i = 0; $i -lt $s.action_count; $i++) {
            [pscustomobject]@{Execute=$s.execute; Arguments=$s.arguments; WorkingDirectory=$s.working_directory}
        })
        Principal = [pscustomobject]@{UserId=$s.principal_user_id; LogonType=$s.principal_logon_type; RunLevel=$s.principal_run_level}
    } }
    if ($case.legacy_state) {
        [pscustomobject]@{TaskName=$expected.legacy_task_name; State=$case.legacy_state; Actions=@(); Principal=$null}
    }
}
function Export-ScheduledTask {
    param($TaskPath, $TaskName, $ErrorAction)
    $s = $script:taskFixtureState
    $trigger = '<' + $s.trigger_type + '><Repetition><Interval>' + $s.repetition_interval + '</Interval></Repetition></' + $s.trigger_type + '>'
    $triggers = $trigger * $s.trigger_count
    '<Task><Triggers>' + $triggers + '</Triggers><Settings><MultipleInstancesPolicy>' + $s.multiple_instances + '</MultipleInstancesPolicy><StartWhenAvailable>' + ([string]$s.start_when_available).ToLowerInvariant() + '</StartWhenAvailable><ExecutionTimeLimit>' + $s.execution_time_limit + '</ExecutionTimeLimit></Settings></Task>'
}
function Deny-TaskMutation([string]$Name) {
    [IO.File]::AppendAllText($env:KMTECH_LABEL_TASK_TEST_MUTATIONS, $Name + [Environment]::NewLine)
    throw ('Unexpected task mutation: ' + $Name)
}
function Write-TaskStep([string]$Name) {
    [IO.File]::AppendAllText($env:KMTECH_LABEL_TASK_TEST_MUTATIONS, $Name + [Environment]::NewLine)
}
function Disable-ScheduledTask {
    param($TaskPath, $TaskName, $ErrorAction)
    if ($TaskName -cne $expected.task_name -or $TaskPath -cne '\') { throw 'Wrong disable target' }
    Write-TaskStep 'Disable'
    $script:taskFixtureEnabled = $false
}
function Unregister-ScheduledTask {
    param($TaskPath, $TaskName, $Confirm, $ErrorAction)
    if ($TaskName -cne $expected.task_name -or $TaskPath -cne '\') { throw 'Wrong removal target' }
    if ($script:taskFixtureLastCount -ne 0 -or $script:taskFixtureEnabled) { throw 'Removed a live/enabled task' }
    Write-TaskStep 'Unregister'
    $script:taskFixturePresent = $false
}
function Start-Sleep {
    param($Milliseconds)
    Write-TaskStep 'Wait'
}
$script:taskFixtureRegistered = [pscustomobject]@{}
$script:taskFixtureRegistered | Add-Member ScriptMethod GetInstances {
    param($flags)
    if ($flags -ne 0) { throw 'Invalid GetInstances flags' }
    if ($case.instance_error) { throw 'Simulated instance readback failure' }
    if ($case.instance_null) { return $null }
    if ($case.instance_count_missing) { return [pscustomobject]@{} }
    $script:taskFixtureInstanceQueries++
    $script:taskFixtureLastCount = if ($case.state -eq 'Running' -and $script:taskFixtureInstanceQueries -eq 1) { 1 } else { 0 }
    if ($case.after_instance_drift -and $script:taskFixtureLastCount -eq 0) {
        $script:taskFixtureState.execute = 'C:\unexpected\python.exe'
    }
    return [pscustomobject]@{Count=$script:taskFixtureLastCount}
}
$script:taskFixtureFolder = [pscustomobject]@{}
$script:taskFixtureFolder | Add-Member ScriptMethod GetTask {
    param($name)
    if ($name -cne $expected.task_name) { throw 'Wrong instance task' }
    return $script:taskFixtureRegistered
}
$script:taskFixtureService = [pscustomobject]@{}
$script:taskFixtureService | Add-Member ScriptMethod Connect {}
$script:taskFixtureService | Add-Member ScriptMethod GetFolder {
    param($path)
    if ($path -cne '\') { throw 'Wrong task folder' }
    return $script:taskFixtureFolder
}
function New-Object {
    param($ComObject)
    if ($ComObject -cne 'Schedule.Service') { throw 'Unexpected native object request' }
    return $script:taskFixtureService
}
function Register-ScheduledTask { Deny-TaskMutation 'Register' }
function Enable-ScheduledTask { Deny-TaskMutation 'Enable' }
function Start-ScheduledTask { Deny-TaskMutation 'Start' }
function Stop-ScheduledTask { Deny-TaskMutation 'Stop' }
function Stop-Process { Deny-TaskMutation 'StopProcess' }
function New-ScheduledTaskAction { Deny-TaskMutation 'NewAction' }
function New-ScheduledTaskTrigger { Deny-TaskMutation 'NewTrigger' }
function New-ScheduledTaskSettingsSet { Deny-TaskMutation 'NewSettings' }
function New-ScheduledTaskPrincipal { Deny-TaskMutation 'NewPrincipal' }
""" + scheduled_task._TASK_POWERSHELL,
        encoding="utf-8-sig",
    )
    env = dict(os.environ)
    env.update(
        KMTECH_LABEL_CURRENT_USER_TASK_SPEC=json.dumps(spec),
        KMTECH_LABEL_TASK_TEST_CASE=json.dumps(case),
        KMTECH_LABEL_TASK_TEST_MUTATIONS=str(mutations),
    )
    completed = subprocess.run(
        [powershell, "-NoLogo", "-NoProfile", "-NonInteractive", "-File", str(script)],
        env=env, capture_output=True, text=True, encoding="oem", timeout=30,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    steps = mutations.read_text().splitlines() if mutations.exists() else []
    return completed, steps



@pytest.mark.parametrize("state", ["Running", "Ready"])
@pytest.mark.parametrize("principal", ["name", "sam", "sid"])
def test_owned_old_task_finishes_before_retirement(monkeypatch, tmp_path, state, principal):
    completed, steps = _run_task_script_fixture(monkeypatch, tmp_path, state=state, principal=principal)
    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert steps == (["Disable", "Wait", "Unregister"] if state == "Running" else ["Disable", "Unregister"])
    report = json.loads(completed.stdout)
    assert report["status"] == "ABSENT"
    assert report["action"] == "RETIRED"
    assert report["canonical"]["exists"] is False
    assert report["process_or_task_stopped"] is False
    assert report["manual_start"] is False


def test_absent_task_does_not_touch_healthy_resident_or_scheduler(monkeypatch, tmp_path):
    completed, steps = _run_task_script_fixture(monkeypatch, tmp_path, present=False)
    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert steps == []
    assert json.loads(completed.stdout)["action"] == "ALREADY_ABSENT"


def test_disabled_task_still_waits_for_running_instances(monkeypatch, tmp_path):
    completed, steps = _run_task_script_fixture(monkeypatch, tmp_path, enabled=False, state="Running")
    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert steps == ["Wait", "Unregister"]


@pytest.mark.parametrize(("field", "value"), [
    ("execute", r"C:\unexpected\python.exe"),
    ("arguments", "--unexpected"),
    ("working_directory", r"C:\unexpected"),
    ("action_count", 2),
    ("principal_user_id", "unrelated-user"),
    ("principal_logon_type", "ServiceAccount"),
    ("principal_run_level", "Highest"),
    ("trigger_count", 2),
    ("trigger_type", "LogonTrigger"),
    ("repetition_interval", "PT2M"),
    ("multiple_instances", "Parallel"),
    ("start_when_available", False),
    ("execution_time_limit", "PT0S"),
])
def test_mismatched_task_is_never_disabled_or_removed(monkeypatch, tmp_path, field, value):
    completed, steps = _run_task_script_fixture(monkeypatch, tmp_path, field=field, value=value)
    assert completed.returncode == 1
    assert steps == []
    assert "different ownership or command" in json.loads(completed.stdout)["failure"]


@pytest.mark.parametrize("legacy_state", ["Running", "Ready"])
def test_historical_system_task_requires_owned_installer_cleanup(monkeypatch, tmp_path, legacy_state):
    completed, steps = _run_task_script_fixture(monkeypatch, tmp_path, legacy_state=legacy_state)
    assert completed.returncode == 1
    assert steps == []
    assert "public installer" in json.loads(completed.stdout)["failure"]


@pytest.mark.parametrize("failure", [
    "readback_drift", "instance_error", "after_instance_drift",
    "instance_null", "instance_count_missing",
])
def test_failed_retirement_keeps_task_disabled_without_stop_restore_or_delete(monkeypatch, tmp_path, failure):
    completed, steps = _run_task_script_fixture(monkeypatch, tmp_path, **{failure: True})
    assert completed.returncode == 1
    assert steps == ["Disable"]
    report = json.loads(completed.stdout)
    assert report["status"] == "FAILED"
    assert report["task_disabled"] is True
    assert report["process_or_task_stopped"] is False

import json
from pathlib import Path
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
    operation = kwargs["env"]["KMTECH_LABEL_CURRENT_USER_TASK_OPERATION"]
    status = "PASS" if operation == "Apply" else "ABSENT"
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


def test_task_registration_returns_exact_readback_without_manual_start(
    monkeypatch, tmp_path
):
    root = _canonical_fixture(monkeypatch, tmp_path)

    report = scheduled_task.install_current_user_scheduled_task(
        root, runner=_successful_runner
    )

    assert report["status"] == "PASS"
    assert report["manual_start"] is False
    assert report["process_or_task_stopped"] is False


def test_task_removal_returns_exact_absence(monkeypatch, tmp_path):
    root = _canonical_fixture(monkeypatch, tmp_path)

    report = scheduled_task.remove_current_user_scheduled_task(
        root, runner=_successful_runner
    )

    assert report["status"] == "ABSENT"


def test_task_script_refuses_manual_start_and_task_stop() -> None:
    source = scheduled_task._TASK_POWERSHELL

    assert "New-ScheduledTaskTrigger" in source
    assert "Register-ScheduledTask" in source
    assert "-LogonType Interactive" in source
    assert "-RunLevel Limited" in source
    assert "Start-ScheduledTask" not in source
    assert "Stop-ScheduledTask" not in source
    assert "Stop-Process" not in source
    assert "schtasks /run" not in source.lower()


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


def test_apply_and_remove_check_enabled_legacy_before_any_task_mutation():
    source = scheduled_task._TASK_POWERSHELL

    gate = source.index("LEGACY_TASK_PRESENT_ENABLED")
    registration = source.index("New-ScheduledTaskAction")
    removal_mutation = source.index(
        "Unregister-ScheduledTask -TaskPath '\\' -TaskName ([string]$snapshot.name)",
        gate,
    )
    assert gate < registration
    assert gate < removal_mutation
    assert "$operation -ceq 'Apply' -and" not in source
    assert scheduled_task.LEGACY_TASK_REMEDIATION in source


def test_task_spec_rejects_noncanonical_root(tmp_path):
    root = tmp_path / "not-canonical"
    root.mkdir()

    with pytest.raises(
        scheduled_task.CurrentUserScheduledTaskError,
        match="canonical Label install root",
    ):
        scheduled_task.build_current_user_task_spec(root)

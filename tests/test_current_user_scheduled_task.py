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


def test_task_spec_rejects_noncanonical_root(tmp_path):
    root = tmp_path / "not-canonical"
    root.mkdir()

    with pytest.raises(
        scheduled_task.CurrentUserScheduledTaskError,
        match="canonical Label install root",
    ):
        scheduled_task.build_current_user_task_spec(root)

"""GUI child flags and source autostart commands, without native execution."""
import json
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pytest

import current_user_scheduled_task as scheduled_task
import label_data_generation as generation
import user_relay


@pytest.mark.parametrize("operation", ["read", "remove"])
@pytest.mark.parametrize("use_default", [False, True])
@pytest.mark.parametrize("failed", [False, True])
def test_task_children_hide_windows_and_preserve_readback(monkeypatch, operation, use_default, failed):
    calls = []
    monkeypatch.setattr(subprocess, "CREATE_NO_WINDOW", 0x08000000, raising=False)
    monkeypatch.setattr(scheduled_task, "_powershell_executable", lambda: Path("fixture-powershell.exe"))

    def runner(command, **kwargs):
        calls.append((command, kwargs))
        assert kwargs["creationflags"] & 0x08000000
        assert kwargs["capture_output"] and kwargs["text"]
        assert kwargs["check"] is False
        assert kwargs["timeout"] == (30 if operation == "read" else 150)
        assert "-NoProfile" in command and "-NonInteractive" in command
        if operation == "read":
            assert "-Command" in command and "env" not in kwargs
            assert "Get-ScheduledTask" in command[-1]
            for verb in ("Register-ScheduledTask", "Unregister-ScheduledTask", "Stop-ScheduledTask", "Stop-Process"):
                assert verb not in command[-1]
            output = {"exists": False, "name": scheduled_task.LEGACY_TASK_NAME}
        else:
            assert "-EncodedCommand" in command
            assert command[-1] and "input" not in kwargs
            assert json.loads(kwargs["env"]["KMTECH_LABEL_CURRENT_USER_TASK_SPEC"]) == {}
            output = {"status": "PASS"}
        return SimpleNamespace(returncode=2 if failed else 0, stdout=json.dumps(output), stderr="readback refused" if failed else "")

    if use_default:
        monkeypatch.setattr(subprocess, "run", runner)
    selected = None if use_default else runner
    invoke = (lambda: scheduled_task.read_legacy_system_task_quiescence(runner=selected)) if operation == "read" else (lambda: scheduled_task._run_task_removal({}, runner=selected))
    if failed:
        with pytest.raises(scheduled_task.CurrentUserScheduledTaskError, match="readback refused"):
            invoke()
    else:
        report = invoke()
        assert report["status"] == "PASS"
        if operation == "read":
            assert report["task_or_process_mutated"] is False
    assert len(calls) == 1
    assert calls[0][0][0] == ("fixture-powershell.exe" if use_default else "powershell.exe")


@pytest.mark.parametrize("state", ["clean", "dirty", "untracked", "error"])
def test_source_identity_git_children_hide_windows_and_remain_fail_closed(monkeypatch, tmp_path, state):
    monkeypatch.setattr(generation, "__file__", str(tmp_path / "label_data_generation.py"))
    monkeypatch.setattr(subprocess, "CREATE_NO_WINDOW", 0x08000000, raising=False)
    calls = []

    def check_output(command, **kwargs):
        calls.append(command[1])
        assert kwargs["creationflags"] & 0x08000000
        assert kwargs["cwd"] == tmp_path and kwargs["timeout"] == 15
        if state == "error":
            raise OSError("fixture git refused")
        return "a" * 40 if command[1] == "rev-parse" else (b"unexpected.py" if state == "untracked" else b"")

    def run(command, **kwargs):
        calls.append(command[1])
        assert kwargs["creationflags"] & 0x08000000
        assert kwargs["capture_output"] and kwargs["check"] is False
        assert kwargs["cwd"] == tmp_path and kwargs["timeout"] == 15
        return SimpleNamespace(returncode=1 if state == "dirty" else 0)

    monkeypatch.setattr(subprocess, "check_output", check_output)
    monkeypatch.setattr(subprocess, "run", run)
    if state == "clean":
        assert generation.release_identity(tmp_path)["build_id"] == "a" * 40
    else:
        with pytest.raises(generation.DataGenerationError):
            generation.release_identity(tmp_path)
    assert calls == (["rev-parse"] if state == "error" else ["rev-parse", "diff", "ls-files"])


@pytest.mark.parametrize("frozen", [False, True])
def test_packaged_identity_never_borrows_git(monkeypatch, tmp_path, frozen):
    import current_user_onboarding

    monkeypatch.setattr(generation.sys, "frozen", frozen, raising=False)
    monkeypatch.setattr(current_user_onboarding, "verify_bootstrap_integrity", lambda paths, required: {"status": "PASS"})
    manifest = {"source_commit": "b" * 40}
    if frozen:
        manifest.update(build_identity_schema_version=1, app_id="label_match", app_version=generation.CURRENT_VERSION)
        name = "build-identity.json"
    else:
        manifest.update(schema="label-match-portable-tree-v1", version=generation.CURRENT_VERSION)
        name = "portable-manifest.json"
    (tmp_path / name).write_text(json.dumps(manifest), encoding="utf-8")
    def forbidden(*args, **kwargs):
        pytest.fail("packaged identity invoked Git")
    monkeypatch.setattr(subprocess, "run", forbidden)
    monkeypatch.setattr(subprocess, "check_output", forbidden)
    assert generation.release_identity(tmp_path)["build_id"] == "b" * 40


def test_source_resident_uses_verified_pythonw_and_preserves_console_mode(monkeypatch, tmp_path):
    app = tmp_path / "source"
    app.mkdir()
    (app / "Label_Match.py").write_text("pass\n", encoding="utf-8")
    runtime = tmp_path / "python"
    runtime.mkdir()
    python = runtime / "python.exe"
    pythonw = runtime / "pythonw.exe"
    python.write_bytes(b"fixture")
    pythonw.write_bytes(b"fixture")
    monkeypatch.setattr(user_relay.sys, "executable", str(python))
    monkeypatch.setattr(user_relay.sys, "frozen", False, raising=False)
    assert user_relay._application_command(app) == [str(pythonw), str(app / "Label_Match.py")]
    assert user_relay.build_user_relay_command(app) == [str(pythonw), str(app / "Label_Match.py"), user_relay.USER_RELAY_MODE]
    assert user_relay._application_command(app, console=True) == [str(python), str(app / "Label_Match.py")]
    pythonw.unlink()
    with pytest.raises(user_relay.UserRelayError, match="windowless"):
        user_relay.build_user_relay_command(app)
    assert user_relay._application_command(app, console=True)[0] == str(python)
    monkeypatch.setattr(user_relay.sys, "frozen", True)
    with pytest.raises(user_relay.UserRelayError, match="hardened"):
        user_relay._application_command(app)

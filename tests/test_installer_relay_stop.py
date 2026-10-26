from pathlib import Path
import ast
import hashlib
import re
import shutil
import subprocess
import sys
import types
import pytest

ROOT=Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("function_name", ("Stop-LabelInstallerRelayPreimage", "Stop-LabelRollbackCandidateRelay"))
@pytest.mark.parametrize("exit_after", (20, 149, 150, 151))
def test_installer_stop_waits_for_direct_sync_child(monkeypatch, tmp_path, function_name, exit_after):
    source = (ROOT / "INSTALL_CANONICAL_PORTABLE.ps1").read_text(encoding="utf-8-sig")
    function = source.split("function " + function_name, 1)[1].split("\n}", 1)[0]
    probe = re.search(r"(?s)\$probe\s*=\s*@'\n(.*?)\n'@", function).group(1)
    assert hashlib.sha256(probe.encode()).hexdigest() in function
    relay_source = ast.parse((ROOT / "user_relay.py").read_text(encoding="utf-8-sig"))
    child_limit = next(ast.literal_eval(node.value) for node in relay_source.body
                       if isinstance(node, ast.Assign) and any(
                           isinstance(target, ast.Name) and target.id == "RELAY_DIRECT_SYNC_PROCESS_TIMEOUT_SECONDS"
                           for target in node.targets))
    calls = []
    relay = types.ModuleType("user_relay")
    relay.RELAY_DIRECT_SYNC_PROCESS_TIMEOUT_SECONDS = child_limit

    def request_stop(state, *, timeout_seconds):
        calls.append((state, timeout_seconds))
        return {"status": "ABSENT" if exit_after <= timeout_seconds else "UNKNOWN"}

    relay.request_user_relay_stop = request_stop
    monkeypatch.setitem(sys.modules, "user_relay", relay)
    monkeypatch.setattr(sys, "path", sys.path[:])
    monkeypatch.setattr(sys, "argv", ["-c", str(tmp_path / "packet"), str(tmp_path / "state")])
    if exit_after <= child_limit + 30:
        exec(compile(probe, "installer-relay-stop", "exec"), {})
    else:
        with pytest.raises(RuntimeError, match="candidate relay did not stop normally"):
            exec(compile(probe, "installer-relay-stop", "exec"), {})
    assert calls == [(tmp_path / "state", child_limit + 30)]


@pytest.mark.parametrize("engine",["powershell.exe","pwsh.exe"])
@pytest.mark.parametrize("placement",["REUSED_VERIFIED","INSTALL_REQUIRED"])
@pytest.mark.parametrize("failure",["success","undeclared","absent"])
def test_ordinary_installer_stops_preimage_after_admission_and_requires_ready(tmp_path,engine,placement,failure):
    executable=shutil.which(engine)
    assert executable is not None
    child=subprocess.run([executable,"-NoLogo","-NoProfile","-NonInteractive","-File",
        str(ROOT/"tests/lm_installer_relay_stop.ps1"),"-Repo",str(ROOT),"-Root",str(tmp_path),
        "-Placement",placement,"-Failure",failure],capture_output=True,text=True,
        creationflags=subprocess.CREATE_NO_WINDOW)
    assert child.returncode==0 and child.stdout.splitlines()[-1]=="PASS"


@pytest.mark.parametrize("engine", ("powershell.exe", "pwsh.exe"))
@pytest.mark.parametrize("case", (
    "normal", "frozen", "standalone", "other-parent", "other-session", "other-sid",
    "elevated", "other-integrity", "older-child", "other-root", "other-entry",
    "unknown-security", "missing-parent",
))
def test_installer_waits_only_for_verified_relay_child(tmp_path, engine, case):
    executable = shutil.which(engine)
    assert executable is not None
    result = subprocess.run([
        executable, "-NoLogo", "-NoProfile", "-NonInteractive", "-File",
        str(ROOT / "tests/lm_installer_relay_child.ps1"), "-Repo", str(ROOT),
        "-Root", str(tmp_path), "-Case", case,
    ], capture_output=True, text=True, encoding="utf-8", creationflags=subprocess.CREATE_NO_WINDOW)
    assert result.returncode == 0 and result.stdout.splitlines()[-1] == "PASS"

from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("scenario", ['ready', 'timeout', 'absent'])
def test_public_installer_outer_failure_handler_keeps_relay_proof(tmp_path, engine, scenario):
    executable = shutil.which(engine)
    assert executable is not None
    child = subprocess.run([executable, "-NoLogo", "-NoProfile", "-NonInteractive", "-File",
        str(ROOT / "tests/lm_rollback_audit.ps1"), "-Repo", str(ROOT), "-Root", str(tmp_path),
        "-Scenario", scenario], capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW)
    assert child.returncode == 0 and child.stdout.splitlines()[-1] == "PASS"

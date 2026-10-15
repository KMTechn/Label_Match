from pathlib import Path
import shutil
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("engine", ["powershell", "pwsh"])
@pytest.mark.parametrize("case", ["proof", "channels", "rollback", "rollback_failure", "rollback_fence_failure"])
def test_native_machine_proof_channels_and_automatic_failure_handlers(tmp_path, engine, case):
    executable = shutil.which(engine)
    assert executable is not None
    child = subprocess.run(
        [executable, "-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
         "-File", str(ROOT / "tests/lm_machine_lifecycle_proof.ps1"), "-Repo", str(ROOT),
         "-Root", str(tmp_path), "-Python", sys.executable, "-Case", case],
        capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW,
    )
    assert child.returncode == 0
    assert child.stdout.strip() == "PASS"

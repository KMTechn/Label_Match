from pathlib import Path
import shutil
import subprocess
import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("scenario", ["restore", "drift"])
def test_owned_publication_preserves_unrelated_standard_links(tmp_path, engine, scenario):
    executable = shutil.which(engine)
    assert executable is not None
    child = subprocess.run([executable, "-NoLogo", "-NoProfile", "-NonInteractive", "-File",
        str(ROOT / "tests/lm_shortcut_preservation.ps1"), "-Repo", str(ROOT), "-Root", str(tmp_path),
        "-Scenario", scenario], capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW)
    assert child.returncode == 0 and child.stdout.splitlines()[-1] == "PASS"

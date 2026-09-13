"""Execute isolated original/adopted installer leaf parity on both supported engines."""
import os
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
def test_shared_portable_leaf_parity_and_trust_boundary(tmp_path, engine):
    _run_parity(tmp_path, engine)


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
def test_array_manifest_keeps_original_rejection(tmp_path, engine):
    _run_parity(tmp_path, engine, "-ArrayManifestCase")


def _run_parity(tmp_path, engine, *extra):
    executable = shutil.which(engine)
    assert executable, f"Required PowerShell engine is unavailable: {engine}"
    environment = os.environ.copy()
    environment["PSModulePath"] = (
        str(Path(os.environ["SystemRoot"]) / "System32/WindowsPowerShell/v1.0/Modules")
        + ";" + str(Path(os.environ["ProgramFiles"]) / "WindowsPowerShell/Modules")
    )
    result = subprocess.run(
        [executable, "-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
         "-File", str(ROOT / "tests/test_shared_portable_leaves.ps1"), "-WorkRoot", str(tmp_path / "parity"), *extra],
        capture_output=True, text=True, encoding="utf-8", errors="replace", check=False,
        timeout=120, env=environment, creationflags=subprocess.CREATE_NO_WINDOW,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "PASS PowerShell" in result.stdout
    print(result.stdout.strip())

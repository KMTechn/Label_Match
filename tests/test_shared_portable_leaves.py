"""Execute isolated original/adopted installer leaf parity on both supported engines."""
import os
import hashlib
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_ps7_parent_preserves_actual_lm_failure_evidence_hash_check(tmp_path):
    child = tmp_path / "check.ps1"
    child.write_text("""param([string]$Library,[string]$Trace,[string]$TraceSha)
$ErrorActionPreference='Stop'
. $Library
Assert-InstallerFailureEvidence @{path=$Trace;sha256=$TraceSha}
Write-Output 'PASS_INHERITED_HASH'
""", encoding="utf-8")
    parent = tmp_path / "parent.ps1"
    parent.write_text("""param([string]$Child,[string]$Probe,[string]$Library,[string]$Trace,[string]$TraceSha)
$env:PSModulePath=Join-Path $PSHOME 'Modules'
$start=[Diagnostics.ProcessStartInfo]::new()
$start.FileName=$Child;$start.UseShellExecute=$false;$start.CreateNoWindow=$true
$start.RedirectStandardOutput=$true;$start.RedirectStandardError=$true
foreach($arg in @('-NoProfile','-NonInteractive','-WindowStyle','Hidden','-File',$Probe,
    '-Library',$Library,'-Trace',$Trace,'-TraceSha',$TraceSha)){$start.ArgumentList.Add($arg)}
$p=[Diagnostics.Process]::Start($start)
$output=$p.StandardOutput.ReadToEnd();$failure=$p.StandardError.ReadToEnd()
$p.WaitForExit();[Console]::Out.Write($output);[Console]::Error.Write($failure);exit $p.ExitCode
""", encoding="utf-8")
    trace = tmp_path / ("label-embedded-diagnostic-" + "1" * 32) / "trace.txt"
    trace.parent.mkdir()
    trace.write_bytes(b"synthetic bounded diagnostic\n")
    environment = os.environ.copy()
    environment["TEMP"] = environment["TMP"] = str(tmp_path)
    winps = Path(os.environ["SystemRoot"]) / "System32/WindowsPowerShell/v1.0/powershell.exe"
    pwsh = shutil.which("pwsh.exe")
    assert pwsh, "Required PS7 parent is unavailable"
    result = subprocess.run([pwsh, "-NoProfile", "-NonInteractive", "-WindowStyle", "Hidden",
        "-File", str(parent), "-Child", str(winps), "-Probe", str(child),
        "-Library", str(ROOT / "tools/invoke_embedded_python.ps1"), "-Trace", str(trace),
        "-TraceSha", hashlib.sha256(trace.read_bytes()).hexdigest()], env=environment,
        capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "PASS_INHERITED_HASH" in result.stdout


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

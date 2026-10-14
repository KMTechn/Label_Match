"""Exercise the public helper's in-memory scope after supported removal."""

import json
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("shell", ["powershell", "pwsh"])
@pytest.mark.parametrize("outer_lease", ["absent", "unrelated"])
def test_public_memory_helper_observes_removed_state_in_its_own_scope(tmp_path, shell, outer_lease):
    executable = shutil.which(shell)
    if executable is None:
        pytest.fail("Required PowerShell interpreter unavailable")
    retained = tmp_path / "business-data"
    retained.write_bytes(b"preserved")
    command = [executable, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File",
        str(ROOT / "tests/lm_placement_finally_scope.ps1"), "-Root", str(tmp_path), "-State", "REMOVED"]
    if outer_lease != 'absent':
        command.append("-OuterLease")
    child = subprocess.run(command, capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    assert child.returncode == 0
    assert child.stdout.strip() == b"PASS"
    assert retained.read_bytes() == b"preserved"


@pytest.mark.parametrize("admission", ["missing", "null", "unacquired"])
def test_memory_helper_rejects_missing_admission_before_child(tmp_path, admission):
    executable = shutil.which("powershell")
    if executable is None:
        pytest.fail("Windows PowerShell unavailable")
    script = tmp_path / "missing-admission.ps1"
    script.write_text(
        r"""
param([string]$Installer, [string]$Admission)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$tokens = $null; $errors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile($Installer, [ref]$tokens, [ref]$errors)
$function = $ast.FindAll({param($n)
    $n -is [Management.Automation.Language.FunctionDefinitionAst] -and
    $n.Name -eq 'Invoke-LabelPlacementLifecycleChild'
}, $false)[0]
$body = $function.Extent.Text + @'

$lease = $null
if ($Admission -eq 'unacquired') { $lease = [pscustomobject]@{acquired=$false} }
$arguments = @{Action={throw 'CHILD_MUST_NOT_RUN'}}
if ($Admission -ne 'missing') { $arguments.Lease = [ref]$lease }
try {
    Invoke-LabelPlacementLifecycleChild @arguments
    throw 'MISSING_ADMISSION_ACCEPTED'
} catch {
    if ($_.Exception.Message -cne 'LABEL_PLACEMENT_CHILD_ADMISSION_MISSING') { throw }
    'MISSING_ADMISSION_REJECTED'
}
'@
& ([scriptblock]::Create($body))
""",
        encoding="utf-8-sig",
    )
    result = subprocess.run(
        [executable, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
         "-File", str(script), "-Installer", str(ROOT / "INSTALL_THIS_PC.ps1"),
         "-Admission", admission],
        capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW,
    )
    assert result.returncode == 0, "Missing admission must remain rejected"
    assert result.stdout.strip() == "MISSING_ADMISSION_REJECTED"

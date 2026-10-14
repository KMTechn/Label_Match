"""Exercise the public helper's in-memory scope after supported removal."""

import json
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("shell", ["powershell", "pwsh"])
@pytest.mark.parametrize("outer_lease", ["absent", "unrelated"])
def test_public_memory_helper_observes_removed_state_in_its_own_scope(
    tmp_path, shell, outer_lease
):
    executable = shutil.which(shell)
    if executable is None:
        pytest.skip("PowerShell edition unavailable")
    machine = tmp_path / "machine"
    machine.mkdir()
    (machine / "versions").mkdir()
    (machine / "update-control").mkdir()
    marker = machine / "removed.json"
    marker.write_text(json.dumps({"lifecycle_state": "REMOVED"}), encoding="utf-8")
    preserved = machine / "retained-evidence.txt"
    preserved.write_bytes(b"preserved supported-removal evidence")
    before = {p: p.read_bytes() for p in (marker, preserved)}
    script = tmp_path / "public-memory-helper.ps1"
    script.write_text(
        r"""
param([string]$Installer, [string]$Machine, [string]$OuterLease)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$tokens = $null; $errors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile($Installer, [ref]$tokens, [ref]$errors)
if ($errors.Count) { throw 'INSTALLER_PARSE_FAILED' }
$functions = $ast.FindAll({param($n)
    $n -is [Management.Automation.Language.FunctionDefinitionAst] -and
    $n.Name -in @('Enter-LabelCanonicalPlacement','Invoke-LabelPlacementLifecycleChild')
}, $false)
if ($functions.Count -ne 2) { throw 'PLACEMENT_FUNCTIONS_MISSING' }
$source = [IO.File]::ReadAllText($Installer)
$start = $source.IndexOf('$writerFenceLease = $null')
$end = $source.IndexOf('$versionedPlacement =', $start)
$placement = $source.Substring($start, $end - $start)
# Keep the real admission and removed-state branch; replace only native product execution.
$probe = $ast.FindAll({param($n)
    $n -is [Management.Automation.Language.CommandAst] -and
    $n.GetCommandName() -eq 'Invoke-LabelPlacementLifecycleChild'
}, $false)[0]
$action = @($probe.CommandElements | Where-Object {
    $_ -is [Management.Automation.Language.ScriptBlockExpressionAst]
})[0].Extent.Text
$placement = $placement.Replace($action, @'
{
    if ($script:held) { throw 'CHILD_MUTEX_STILL_HELD' }
    $script:observed++
    $global:LASTEXITCODE = 0
    '{"status":"PASS","lifecycle_state":"REMOVED"}'
}
'@)
$script:held = $false
$script:observed = 0
$script:released = 0
$script:acquired = 0
$script:restored = 0
$script:initialFence = [pscustomobject]@{status='INSTALLING'; generation=0}
$script:activeFence = $script:initialFence
function Enter-LabelWriterDelegatedOperation {
    $script:held = $true
    return [pscustomobject]@{acquired=$true; generation=0}
}
function Exit-LabelWriterAdmission($Lease) {
    if (-not $Lease.acquired -or -not $script:held) { throw 'LEASE_NOT_HELD' }
    $Lease.acquired = $false
    $script:held = $false
    $script:released++
}
function Enter-LabelWriterAdmission {
    if ($script:held) { throw 'PARENT_MUTEX_STILL_HELD' }
    $script:held = $true
    $script:acquired++
    return [pscustomobject]@{acquired=$true; generation=$script:acquired}
}
function Assert-LabelWriterFenceOwner { return $script:activeFence }
function Set-LabelWriterFenceDelegation {
    $script:activeFence = [pscustomobject]@{status='INSTALLING'; generation=1}
    return $script:activeFence
}
function Write-LabelWriterFenceAtomic($Root, $Fence) {
    $script:activeFence = $Fence
    $script:restored++
}
if ($OuterLease -eq 'unrelated') {
    $script:writerFenceLease = [pscustomobject]@{acquired=$false; generation=-1}
}
$testOverride = $false
$WriterFenceFunctionsPreloaded = [switch]$true
$WriterFenceControlRoot = $Machine
$WriterFenceSessionId = 'scope-session'
$WriterFenceAttemptId = 'scope-attempt'
$WriterFenceReplacementTransactionId = 'scope-transaction'
$WriterFenceDelegationToken = [guid]::NewGuid().ToString('N')
$installRootFull = Join-Path $Machine 'current'
$body = ($functions.Extent.Text -join "`n") + "`n" + $placement + @'

try {
    if ($supportedContext.status -cne 'PASS' -or $supportedContext.lifecycle_state -cne 'REMOVED') {
        throw 'REMOVED_STATE_NOT_ACCEPTED'
    }
    if (-not $writerFenceLease.acquired -or $writerFenceLease.generation -ne 1) {
        throw 'LOCAL_REACQUIRED_LEASE_MISSING'
    }
} finally { Exit-LabelWriterAdmission $writerFenceLease }
'@
# Match the encoded public launcher: invoke verified helper bytes as a ScriptBlock.
& ([scriptblock]::Create($body))
if ($OuterLease -eq 'unrelated' -and
    ($script:writerFenceLease.acquired -or $script:writerFenceLease.generation -ne -1)) {
    throw 'OUTER_LEASE_CHANGED'
}
[ordered]@{observed=$script:observed; released=$script:released;
    acquired=$script:acquired; restored=$script:restored; held=$script:held} |
    ConvertTo-Json -Compress
""",
        encoding="utf-8-sig",
    )
    # Product execution is replaced before invocation; no GUI, scheduler or installed state.
    command = [executable, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
               "-File", str(script), "-Installer", str(ROOT / "INSTALL_THIS_PC.ps1"),
               "-Machine", str(machine), "-OuterLease", outer_lease]
    result = subprocess.run(command, capture_output=True, text=True,
                            creationflags=subprocess.CREATE_NO_WINDOW)
    assert result.returncode == 0, "Public helper scope regression"
    assert json.loads(result.stdout) == {
        "observed": 1, "released": 2, "acquired": 1, "restored": 1, "held": False,
    }
    assert all(p.read_bytes() == content for p, content in before.items())
    assert not (machine / "current").exists()


@pytest.mark.parametrize("admission", ["missing", "null", "unacquired"])
def test_memory_helper_rejects_missing_admission_before_child(tmp_path, admission):
    executable = shutil.which("powershell")
    if executable is None:
        pytest.skip("Windows PowerShell unavailable")
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

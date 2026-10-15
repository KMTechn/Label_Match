"""A best-effort relay hint must preserve the failed source admission."""
from pathlib import Path

import pytest

from tests.test_shared_ancestor_protection import run_functions


INSTALLER = Path(__file__).resolve().parents[1] / "INSTALL_CANONICAL_PORTABLE.ps1"


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("secondary", ["inventory", "guidance"])
def test_source_admission_keeps_first_cause_when_relay_hint_fails(tmp_path, engine, secondary):
    result = run_functions(tmp_path, INSTALLER,
        ["Invoke-LabelSourceDataAdmission", "Write-LabelPreparationFailure", "Save", "Full", "Relays"], r'''
function Product { throw [InvalidOperationException]::new('ISOLATED_SOURCE_REFUSED') }
function Get-CimInstance {
    if($env:LM_SECONDARY_FAILURE -ceq 'inventory'){throw 'Isolated relay inventory failure'}
    return @()
}
function Write-LabelInstallerRelayRecoveryGuidance {throw 'Isolated relay guidance failure'}
function Get-LabelLaunchFailure {
    param($Failure,$Phase)
    if($Failure.Exception.Message -cne 'ISOLATED_SOURCE_REFUSED'){throw 'First cause was replaced'}
    return [ordered]@{status='FAILED';stage='SUPPORT';reason_code='INSTALLER_FAILED';report_status='REPORT_UNAVAILABLE'}
}
$auditRoot=Join-Path $env:LM_FIRST_FAILURE_ROOT 'audit'
$external=Join-Path $env:LM_FIRST_FAILURE_ROOT 'external.json'
$refused=$false
try {
    Invoke-LabelSourceDataAdmission $env:LM_FIRST_FAILURE_ROOT $env:LM_FIRST_FAILURE_ROOT `
        @{sha256=('a'*64)} $auditRoot $external
} catch {
    if($_.Exception.Message -cne 'ISOLATED_SOURCE_REFUSED'){throw}
    $refused=$true
}
if(-not $refused){throw 'Failed source admission continued'}
$files=@(Get-ChildItem -LiteralPath $auditRoot -File)
if($files.Count -ne 1){throw 'First failure audit is absent or duplicated'}
$audit=Get-Content -LiteralPath $files[0].FullName -Raw | ConvertFrom-Json
if($audit.status -cne 'FAILED' -or $audit.preparation_status -cne 'FAILED_PREPARATION_ADMISSION' -or
   $audit.code_placement -cne 'NOT_STARTED' -or $audit.rollback.applied){throw 'First failure state differs'}
if((Get-FileHash -LiteralPath $files[0].FullName).Hash -cne
   (Get-FileHash -LiteralPath $external).Hash){throw 'First failure evidence differs'}
'PRESERVED'
''', engine=engine, values={"LM_FIRST_FAILURE_ROOT": str(tmp_path),
                           "LM_SECONDARY_FAILURE": secondary})
    assert result.returncode == 0, result.stderr
    assert "PRESERVED" in result.stdout

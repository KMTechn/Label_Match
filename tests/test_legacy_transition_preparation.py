"""Actual first-install control flow with isolated task and ACL doubles."""
from pathlib import Path

import pytest

from tests.test_shared_ancestor_protection import run_functions

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("existing", [False, True])
def test_disable_prepares_base_before_owned_task_and_ordinary_placement(tmp_path, engine, existing):
    source = (ROOT / "INSTALL_THIS_PC.ps1").read_text(encoding="utf-8-sig")
    start = source.index("if ($LegacyRelayTransition -and\n")
    end = source.index("if ($Uninstall.IsPresent) {", start)
    body = source[start:end]
    script = r'''
Set-StrictMode -Version Latest
$installRootFull=Join-Path $env:LM_TRANSITION_ROOT 'Label_Match/current'
$application=Split-Path -Parent $installRootFull
$DryRun=[Management.Automation.SwitchParameter]::new($false)
$Uninstall=[Management.Automation.SwitchParameter]::new($false)
$StartupUpdateRestorePreimage='';$testOverride=$false
$LegacyRelayPreimageSha256='a'*64;$WriterFenceAttemptId='b'*32
$WriterFenceControlRoot='fixture';$WriterFenceSessionId='c'*32
$WriterFenceReplacementTransactionId='d'*32;$WriterFenceDelegationToken='fixture-only'
$script:ownedTaskEnabled=$true;$script:ownerRecords=0;$script:placementRan=$false
$script:disables=0
function Assert-AlreadyElevated {}
function Assert-LabelProtectedInstallPath { param($Path,[switch]$AllowSupportedPreparation) }
function Protect-KmtechCodeAncestors { param($Path,[switch]$BeforeAppCreation) }
function Set-HardenedCodeAcl { param($Path) }
function Assert-ProtectedCodeAncestors { param($Path)
 if(-not [IO.Directory]::Exists($Path)){throw 'Base absent at protected admission'}
}
function Write-LabelInstallerOwnerRecord { param($Path) $script:ownerRecords++ }
function Write-ElevationLog { param($Phase,$Message) }
function Enter-LabelWriterDelegatedOperation { param($ControlRoot,$SessionId,$AttemptId,$ReplacementTransactionId,$DelegationToken,$Source) return 'lease' }
function Exit-LabelWriterAdmission { param($Lease) }
function Invoke-LabelOfficialLegacyTaskTransition { param($Mode,$Directory,$Hash)
 if($Mode -cne 'Disable' -or $Hash -cne ('a'*64)){throw 'Transition input differs'}
 if(-not [IO.Directory]::Exists($application)){throw 'Task disabled before base preparation'}
 $script:disables++;$script:ownedTaskEnabled=$false
}
if($env:LM_TRANSITION_EXISTING -ceq 'true'){[void][IO.Directory]::CreateDirectory($application)}
function Invoke-ActualPreparationAndTransition {
''' + body + r'''
 $script:placementRan=$true
}
$LegacyRelayTransition='Disable';Invoke-ActualPreparationAndTransition
if($script:ownedTaskEnabled -or $script:ownerRecords -ne 0 -or $script:placementRan){throw 'Transition control flow differs'}
if(-not [IO.Directory]::Exists($application)){throw 'Prepared base missing'}
$LegacyRelayTransition='';Invoke-ActualPreparationAndTransition
if(-not $script:placementRan -or $script:ownerRecords -ne 1 -or $script:disables -ne 1){throw 'Ordinary placement not reached'}
'PASS'
'''
    result = run_functions(tmp_path, ROOT / "INSTALL_THIS_PC.ps1",
                           ["Initialize-LabelProtectedInstallBase"], script, engine=engine,
                           values={"LM_TRANSITION_ROOT": str(tmp_path / "isolated"),
                                   "LM_TRANSITION_EXISTING": str(existing).lower()})
    assert result.returncode == 0, "isolated transition driver failed; child evidence retained"
    assert result.stdout.strip() == "PASS"


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
def test_empty_diagnostic_object_preserves_original_exception_under_strict_mode(tmp_path, engine):
    result = run_functions(tmp_path, ROOT / "INSTALL_CANONICAL_PORTABLE.ps1", ["Protect-LabelFailureText"], r'''
Set-StrictMode -Version Latest
try {throw [IO.DirectoryNotFoundException]::new('original first-install cause')}
catch {
 $original=$_
 $record=[ordered]@{failure_type=$original.Exception.GetType().FullName;empty=[pscustomobject]@{};message=$original.Exception.Message}
 $safe=ConvertTo-KmtechRedactedSinkObject $record @{}
 if($safe.failure_type -cne 'System.IO.DirectoryNotFoundException' -or $safe.message -cne 'original first-install cause'){
  throw 'Original cause changed during diagnostic redaction'
 }
 if(($safe.empty | ConvertTo-Json -Compress) -cne '{}'){throw 'Empty object changed'}
}
'PASS'
''', engine=engine)
    assert result.returncode == 0, "strict diagnostic driver failed; child evidence retained"
    assert result.stdout.strip() == "PASS"

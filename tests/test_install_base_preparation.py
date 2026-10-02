"""Run preparation functions only; no elevation, installer, relay or task starts."""
import pytest
from tests.test_shared_ancestor_protection import ROOT, run_functions


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('scenario', ('fresh', 'already_protected', 'app_create_failure', 'trust_failure'))
def test_preparation_orders_shared_protection_creation_and_trust(tmp_path, engine, scenario):
    result = run_functions(tmp_path, ROOT/'INSTALL_THIS_PC.ps1',
                           ['Initialize-LabelProtectedInstallBase'], r'''
$script:steps=@()
function Assert-LabelProtectedInstallPath {param($Path,[switch]$AllowSupportedPreparation)
 if(-not $AllowSupportedPreparation){throw 'Preparation admission was strict.'}
 $script:steps+=,'admit'
}
function Protect-KmtechCodeAncestors {param($CodeParent,[switch]$BeforeAppCreation)
 if(-not $BeforeAppCreation -or $CodeParent -cne 'C:\KMTech\Apps\Label_Match'){throw 'Wrong preparation target.'}
 $script:steps+=,'protect'
}
function Test-Path {param($LiteralPath) return ($env:LM_PREP_SCENARIO -ceq 'already_protected')}
function New-Item {param($ItemType,$Path,$ErrorAction)
 $script:steps+=,'create'
 if($env:LM_PREP_SCENARIO -ceq 'app_create_failure'){throw 'original create failure'}
}
function Set-HardenedCodeAcl {param($Path) $script:steps+=,'harden_app'}
function Assert-ProtectedCodeAncestors {param($Path)
 $script:steps+=,'trust'
 if($env:LM_PREP_SCENARIO -ceq 'trust_failure'){throw 'original trust failure'}
}
function Get-InstallationPreparationExitCode {param($Failure) return 66}
$failure=$null
try{Initialize-LabelProtectedInstallBase 'C:\KMTech\Apps\Label_Match\current'}catch{$failure=$_}
$expected=switch($env:LM_PREP_SCENARIO){
 'already_protected' {'admit,protect,trust'}
 'app_create_failure' {'admit,protect,create'}
 default {'admit,protect,create,harden_app,trust'}
}
if(($script:steps -join ',') -cne $expected){throw 'Preparation stage order differs.'}
if($env:LM_PREP_SCENARIO -ceq 'app_create_failure'){
 if($null -eq $failure -or $failure.Exception.Message -cne 'original create failure' -or $failure.Exception.Data['installation_preparation_exit_code'] -ne 64){throw 'Original creation failure/code lost.'}
}elseif($env:LM_PREP_SCENARIO -ceq 'trust_failure'){
 if($null -eq $failure -or $failure.Exception.Message -cne 'original trust failure'){throw 'Original trust failure lost.'}
}elseif($null -ne $failure){throw $failure}
'PASS'
''', engine=engine, values={'LM_PREP_SCENARIO': scenario})
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('scenario', ('fresh', 'absent_shared', 'strict_default', 'trusted_installer', 'user_owner'))
def test_ordinary_preflight_accepts_supported_defaults_without_writing(tmp_path, engine, scenario):
    result = run_functions(tmp_path, ROOT/'tools/bootstrap_integrity.ps1',
                           ['Protect-KmtechCodeAncestors', 'Assert-LabelProtectedInstallPath',
                            'Get-InstallationPreparationExitCode'], r'''
function Write-LabelAncestorPreparationEvent {param($Payload)}
function Test-Path {param($LiteralPath)
 if($LiteralPath -ceq 'C:\KMTech\Apps\Label_Match'){return $false}
 if($env:LM_ADMIT_SCENARIO -ceq 'absent_shared' -and $LiteralPath -in @('C:\KMTech','C:\KMTech\Apps')){return $false}
 return $true
}
function Get-Item {param($LiteralPath,$ErrorAction) return [pscustomobject]@{Attributes=[IO.FileAttributes]::Directory}}
function Get-Acl {param($LiteralPath,$ErrorAction)
 $acl=[Security.AccessControl.DirectorySecurity]::new()
 $sddl=if($LiteralPath -ceq 'C:\'){'O:BAG:BAD:P(A;OICI;FA;;;BA)(A;OICI;FA;;;SY)(A;OICI;0x1200a9;;;BU)'}else{'O:BAG:BAD:AI(A;OICIID;FA;;;BA)(A;OICIID;FA;;;SY)(A;OICIID;0x1200a9;;;BU)(A;ID;0x1301bf;;;AU)(A;OICIIOID;0xe0010000;;;AU)'}
 if($LiteralPath -ceq 'C:\' -and $env:LM_ADMIT_SCENARIO -ceq 'trusted_installer'){$sddl=$sddl.Replace('O:BA','O:S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464')}
 if($LiteralPath -ceq 'C:\KMTech\Apps' -and $env:LM_ADMIT_SCENARIO -ceq 'user_owner'){$sddl=$sddl.Replace('O:BA','O:S-1-5-21-1-2-3-1000')}
 $acl.SetSecurityDescriptorSddlForm($sddl);return $acl
}
function Set-Acl {throw 'Ordinary preflight attempted an ACL write.'}
$failure=$null
try{Assert-LabelProtectedInstallPath 'C:\KMTech\Apps\Label_Match' -AllowSupportedPreparation:($env:LM_ADMIT_SCENARIO -cne 'strict_default')}catch{$failure=$_}
$expectedFailure=$env:LM_ADMIT_SCENARIO -in @('strict_default','user_owner')
if(($null -ne $failure) -ne $expectedFailure){throw 'Preflight admission differs.'}
if($env:LM_ADMIT_SCENARIO -ceq 'user_owner' -and (Get-InstallationPreparationExitCode $failure) -ne 63){throw 'Untrusted owner diagnostic differs.'}
'PASS'
''', engine=engine, values={'LM_ADMIT_SCENARIO': scenario})
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
def test_preparation_exit_codes_preserve_inner_failure_and_korean_diagnostic(tmp_path, engine):
    result = run_functions(tmp_path, ROOT/'tools/bootstrap_integrity.ps1',
                           ['Get-InstallationPreparationExitCode', 'Get-InstallationPreparationDiagnostic'], r'''
foreach($code in 61..66){
 $inner=[InvalidOperationException]::new('original failure')
 $inner.Data['installation_preparation_exit_code']=$code
 $outer=[InvalidOperationException]::new('wrapper',$inner)
 if((Get-InstallationPreparationExitCode $outer) -ne $code){throw 'Inner preparation code lost.'}
 $diagnostic=Get-InstallationPreparationDiagnostic $code
 if($diagnostic.exit_code -ne $code -or $diagnostic.code -cnotmatch '^INSTALLATION_PREPARATION_' -or $diagnostic.message -notmatch '[가-힣]'){throw 'Korean diagnostic/code missing.'}
}
if($null -ne (Get-InstallationPreparationDiagnostic 4)){throw 'Unrelated placement failure mapped to preparation.'}
if((Get-InstallationPreparationExitCode ([Exception]::new('unknown'))) -ne 66){throw 'Unexpected preparation failure lacks fallback.'}
'PASS'
''', engine=engine)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
def test_canonical_parent_maps_actual_child_codes_without_running_installer(tmp_path, engine):
    result = run_functions(tmp_path, ROOT/'tools/bootstrap_integrity.ps1',
                           ['Get-InstallationPreparationDiagnostic'], r'''
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile($env:LM_CANONICAL_SOURCE,[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'Canonical parse failed.'}
$checks=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.IfStatementAst] -and $n.Clauses[0].Item1.Extent.Text -ceq '$placementExitCode -ne 0'},$true))
if($checks.Count -ne 1){throw 'Canonical placement check missing/ambiguous.'}
$check=[scriptblock]::Create($checks[0].Extent.Text)
foreach($placementExitCode in 0,4,61,62,63,64,65,66){
 $failure=$null;try{. $check}catch{$failure=$_}
 if($placementExitCode -eq 0){if($null -ne $failure){throw 'Success was rejected.'};continue}
 if($null -eq $failure){throw 'Child failure was admitted.'}
 if($placementExitCode -eq 4){
  if($failure.Exception.Message -cne 'Code placement failed: 4'){throw 'Unrelated code changed.'}
 }elseif($failure.Exception.Data['installation_preparation_exit_code'] -ne $placementExitCode -or $failure.Exception.Message -notmatch '[가-힣]' -or $failure.Exception.Message -cnotmatch 'INSTALLATION_PREPARATION_'){
  throw 'Parent lost preparation code/Korean reason.'
 }
}
'PASS'
''', engine=engine, values={'LM_CANONICAL_SOURCE': str(ROOT/'INSTALL_CANONICAL_PORTABLE.ps1')})
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('scenario', ('success', 'failure', 'log_failure'))
def test_elevated_preparation_keeps_sddl_and_original_failure_when_log_io_fails(tmp_path, engine, scenario):
    result = run_functions(tmp_path, ROOT/'tools/bootstrap_integrity.ps1',
                           ['Get-InstallationPreparationExitCode'], r'''
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile($env:LM_PLACEMENT_SOURCE,[ref]$tokens,[ref]$errors)
$blocks=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.TryStatementAst] -and $n.Body.Extent.Text.Contains('Initialize-LabelProtectedInstallBase $installRootFull')},$true))
if($errors.Count -or $blocks.Count -ne 1){throw 'Actual preparation boundary missing/ambiguous.'}
$script:captured=@()
function Initialize-LabelProtectedInstallBase {param($InstallRoot)
 $Script:LabelAncestorPreparationEvents=[Collections.Generic.List[object]]::new()
 $Script:LabelAncestorPreparationEvents.Add(@{event='ANCESTOR_ACL_BEFORE';before_sddl='original-before'})
 $Script:LabelAncestorPreparationEvents.Add(@{event='ANCESTOR_ACL_AFTER';after_sddl='original-after'})
 if($env:LM_LOG_SCENARIO -cne 'success'){
  $failure=[InvalidOperationException]::new('original ancestor failure')
  $failure.Data['installation_preparation_exit_code']=61;throw $failure
 }
}
function Write-ElevationLog {param($Status,$Message)
 if($env:LM_LOG_SCENARIO -ceq 'log_failure'){throw 'secondary audit IO failure'}
 $script:captured+=,@{status=$Status;message=$Message}
}
$failure=$null
try{. ([scriptblock]::Create($blocks[0].Extent.Text))}catch{$failure=$_}
if($env:LM_LOG_SCENARIO -ceq 'success'){
 if($null -ne $failure -or $script:captured[0].status -cne 'PREPARATION'){throw 'Successful preparation evidence missing.'}
 $events=$script:captured[0].message | ConvertFrom-Json
 if($events.Count -ne 2){throw 'Successful before/after SDDL missing.'}
}else{
 if($null -eq $failure -or $failure.Exception.Message -cne 'original ancestor failure' -or (Get-InstallationPreparationExitCode $failure) -ne 61){throw 'Audit IO masked original preparation failure.'}
 if($env:LM_LOG_SCENARIO -ceq 'failure'){
  $record=$script:captured[0].message | ConvertFrom-Json
  if($record.events.Count -ne 2 -or $record.events[0].before_sddl -cne 'original-before' -or $record.events[1].after_sddl -cne 'original-after'){throw 'Partial persistent-change evidence lost.'}
 }
}
'PASS'
''', engine=engine, values={'LM_PLACEMENT_SOURCE': str(ROOT/'INSTALL_THIS_PC.ps1'), 'LM_LOG_SCENARIO': scenario})
    assert result.returncode == 0, result.stdout + result.stderr

"""Real startup rollback call and native writer delegation; no host installation."""
from pathlib import Path

import pytest

from tests.test_shared_ancestor_protection import run_functions

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("engine", ("powershell.exe", "pwsh.exe"))
@pytest.mark.parametrize("case", ("valid", "expired", "current_retained", "parent_binding", "parent_lease", "parent_token", "child_failure"))
def test_actual_startup_restore_refreshes_only_live_bound_owner(tmp_path, engine, case):
    result = run_functions(tmp_path, ROOT / "INSTALL_CANONICAL_PORTABLE.ps1", ["Arg", "Invoke-LabelRollbackStep", "Get-LabelLaunchFailure"], r'''
Set-StrictMode -Version Latest
. $env:LM_ROUND5_LIBRARY
$root=$env:LM_ROUND5_ROOT;$control=Join-Path $root 'control'
$session='1'*32;$attempt='2'*32;$transaction='3'*32;$orchestrator='4'*64;$contract='5'*64
$synthetic='isolated-round5-'+('6'*64)
$authority=Enter-LabelWriterSessionAuthority $session $attempt $orchestrator $transaction $contract
$childPath=Join-Path $root 'child.ps1';$inputPath=Join-Path $root 'child-input.json';$resultPath=Join-Path $root 'child-result.json'
$child=@'
$ErrorActionPreference='Stop';Set-StrictMode -Version Latest
$record=Get-Content -LiteralPath $env:LM_ROUND5_INPUT -Raw|ConvertFrom-Json
. $record.library
$lease=$null;$failure='';$restored=$false;$ordinaryDenied=$false
try{
 try{$ordinary=Enter-LabelWriterDelegatedOperation -ControlRoot $record.control -SessionId $record.session -AttemptId $record.attempt -ReplacementTransactionId $record.transaction -DelegationToken $record.delegation -Source 'ordinary_writer';Exit-LabelWriterAdmission $ordinary}
 catch{$ordinaryDenied=$true}
 $lease=Enter-LabelWriterDelegatedOperation -ControlRoot $record.control -SessionId $record.session -AttemptId $record.attempt -ReplacementTransactionId $record.transaction -DelegationToken $record.delegation -Source 'auto_update_restore_preimage'
 if($record.case -ceq 'child_failure'){throw 'ISOLATED_STARTUP_RESTORE_FAILED'}
 [IO.File]::WriteAllBytes($record.target,[IO.File]::ReadAllBytes($record.preimage));$restored=$true
}catch{$failure=$_.Exception.Message}
finally{if($null -ne $lease){Exit-LabelWriterAdmission $lease}}
[IO.File]::WriteAllText($record.result,([ordered]@{restored=$restored;ordinary_denied=$ordinaryDenied;failure=$failure}|ConvertTo-Json),[Text.UTF8Encoding]::new($false))
if($restored){exit 0};exit 1
'@
[IO.File]::WriteAllText($childPath,$child,[Text.UTF8Encoding]::new($true))
$original=Join-Path $root 'old-folder';$renamed=Join-Path $root 'old-folder-preserved'
[void](New-Item -ItemType Directory -Path $renamed)
$preimage=Join-Path $renamed 'old-file.dat';[IO.File]::WriteAllBytes($preimage,[byte[]](0,1,255,13,10))
$target=Join-Path $root 'startup-state.dat';[IO.File]::WriteAllBytes($target,[byte[]](9,9))
$script:invocations=0;$script:LabelPlacementChildFailure=$null
function InvokeFrozenPlacementHelper($Frozen,[hashtable]$HelperParameters){
 $script:invocations++
 $record=[ordered]@{library=$env:LM_ROUND5_LIBRARY;control=$control;session=$session;attempt=$attempt;transaction=$transaction;delegation=[string]$HelperParameters.WriterFenceDelegationToken;result=$resultPath;case=$env:LM_ROUND5_CASE;preimage=$preimage;target=$target}
 [IO.File]::WriteAllText($inputPath,($record|ConvertTo-Json),[Text.UTF8Encoding]::new($false))
 $info=[Diagnostics.ProcessStartInfo]::new();$info.FileName=(Get-Process -Id $PID).Path
 $info.Arguments='-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -File "'+$childPath+'"'
 $info.UseShellExecute=$false;$info.CreateNoWindow=$true;$info.RedirectStandardOutput=$true;$info.RedirectStandardError=$true
 $info.EnvironmentVariables['LM_ROUND5_INPUT']=$inputPath
 $process=[Diagnostics.Process]::Start($info)
 if(-not $process.WaitForExit(15000)){throw 'Isolated restoration child did not finish'}
 $exitCode=$process.ExitCode;$process.Dispose()
 if($exitCode){$script:LabelPlacementChildFailure=[ordered]@{schema='label-match-placement-child-failure-v1';phase='HELPER_EXECUTION';placement_code='PLACEMENT_CHILD_FAILED';exit_code=$exitCode;exception_type='System.Management.Automation.RuntimeException';hresult=-2146233087;message='Isolated restore failed.'}}
 return $exitCode
}
try{
 $expiry=if($env:LM_ROUND5_CASE -ceq 'valid'){[DateTime]::UtcNow.AddMinutes(5)}else{[DateTime]::UtcNow.AddMinutes(-1)}
 $payload=New-LabelWriterFencePayload -Status RESTORING -SessionId $session -AttemptId $attempt -ReplacementTransactionId $transaction -SessionStartedAtUtc ([DateTime]::UtcNow.ToString('r')) -OrchestratorSha256 $orchestrator -WriterContractSha256 $contract -DelegationToken $synthetic -DelegatedSources @('canonical_placement','auto_update_restore_preimage') -DelegationExpiresAtUtc ($expiry.ToString('r'))
 $payload.activated_at_utc=[DateTime]::UtcNow.ToString('r');[void](Write-LabelWriterFenceAtomic $control $payload)
 $before=[IO.File]::ReadAllText((Get-LabelWriterFenceActivePath $control))
 $writerAuthority=$authority;$frozenPlacement=$null
 $helperParameters=@{WriterFenceControlRoot=$control;WriterFenceSessionId=$session;WriterFenceAttemptId=$attempt;WriterFenceReplacementTransactionId=$transaction;WriterFenceDelegationToken=$synthetic;ReplaceExistingVerifiedPortable=$false}
 if($env:LM_ROUND5_CASE -ceq 'parent_binding'){$helperParameters.WriterFenceAttemptId='f'*32}
 if($env:LM_ROUND5_CASE -ceq 'parent_token'){$helperParameters.WriterFenceDelegationToken+='wrong'}
 if($env:LM_ROUND5_CASE -ceq 'parent_lease'){$writerAuthority=[pscustomobject]@{acquired=$false;name=$authority.name;mutex=$authority.mutex}}
 if($env:LM_ROUND5_CASE -ceq 'current_retained'){
  [void](New-Item -ItemType Directory -Path (Join-Path $root 'current'))
  [IO.File]::WriteAllText((Join-Path $root 'current/new-code.txt'),'preserved candidate')
 }
 $audit=[ordered]@{startup_update_preimage=[ordered]@{path=$renamed;sha256=('a'*64)}}
 $rollbackStartupProven=$false
 $ast=[Management.Automation.Language.Parser]::ParseFile($env:LM_CONTRACT_SOURCE,[ref]$tokens,[ref]$errors)
 $definitions=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Invoke-LabelDelegatedPlacementHelper'},$true))
 if($definitions.Count -eq 1){. ([scriptblock]::Create($definitions[0].Extent.Text))}
 $commands=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.CommandAst] -and $n.GetCommandName() -ceq 'Invoke-LabelRollbackStep' -and $n.CommandElements[1].Extent.Text -ceq "'protected_startup_restore'"},$true))
 if($commands.Count -ne 1){throw 'Actual protected startup restore call is ambiguous'}
 $action=$commands[0].CommandElements[2].ScriptBlock.Extent.Text
 $Script:LabelRollbackSteps=[Collections.Generic.List[object]]::new()
 $Script:LabelRollbackErrors=[Collections.Generic.List[object]]::new()
 $Script:LabelRollbackFailures=[Collections.Generic.List[object]]::new()
 $caught=$null
 . Invoke-LabelRollbackStep 'protected_startup_restore' ([scriptblock]::Create($action.Substring(1,$action.Length-2)))
 if($Script:LabelRollbackErrors.Count){$caught=$Script:LabelRollbackErrors[0]}
 $parentReject=$env:LM_ROUND5_CASE -cin @('parent_binding','parent_lease','parent_token')
 if($parentReject){
  if($null -eq $caught -or $script:invocations -ne 0){throw 'Foreign authority dispatched restoration'}
  if([IO.File]::ReadAllText((Get-LabelWriterFenceActivePath $control)) -cne $before){throw 'Foreign authority changed fence'}
 }else{
  $childResult=Get-Content -LiteralPath $resultPath -Raw|ConvertFrom-Json
  if(-not $childResult.ordinary_denied){throw 'Ordinary writer passed restoration fence'}
  if($env:LM_ROUND5_CASE -ceq 'child_failure'){
   if($null -eq $caught -or $rollbackStartupProven -or $childResult.failure -cne 'ISOLATED_STARTUP_RESTORE_FAILED'){throw 'Child failure was lost'}
   if(-not $caught.Exception.Data.Contains('placement_child_failure')){throw 'Child diagnostic was lost'}
  }else{
   if($null -ne $caught){throw $caught}
   if(-not $rollbackStartupProven -or -not $childResult.restored){throw ('Startup restoration failed: '+$childResult.failure)}
   if($Script:LabelRollbackSteps[0].status -cne 'PROVEN'){throw 'Actual rollback audit did not prove restoration'}
   if([Convert]::ToBase64String([IO.File]::ReadAllBytes($target)) -cne [Convert]::ToBase64String([IO.File]::ReadAllBytes($preimage))){throw 'Startup preimage bytes differ'}
   if($env:LM_ROUND5_CASE -ceq 'current_retained' -and [IO.File]::ReadAllText((Join-Path $root 'current/new-code.txt')) -cne 'preserved candidate'){throw 'Retained candidate was changed'}
   if($env:LM_ROUND5_WRAPPER_SOURCE){
    [void](Stop-LabelWriterFence -ControlRoot $control -SessionId $session -AttemptId $attempt -ReplacementTransactionId $transaction)
    $wrapper=[Management.Automation.Language.Parser]::ParseFile($env:LM_ROUND5_WRAPPER_SOURCE,[ref]$tokens,[ref]$errors)
    if($errors.Count){throw 'Original wrapper parser failed'}
    foreach($fn in $wrapper.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst]},$false)){. ([scriptblock]::Create($fn.Extent.Text))}
    $wrapperEntry=[Management.Automation.Language.Parser]::ParseFile((Join-Path (Split-Path -Parent $env:LM_ROUND5_WRAPPER_SOURCE) 'Install-KmtechPc.ps1'),[ref]$tokens,[ref]$errors)
    foreach($fn in $wrapperEntry.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Assert-KmtechOrdinaryPath'},$false)){. ([scriptblock]::Create($fn.Extent.Text))}
    function Assert-KmtechLegacyWritablePath([string]$Path,[bool]$AllowSync=$false){$full=[IO.Path]::GetFullPath($Path);if($full -ine $root -and -not $full.StartsWith($root+'\',[StringComparison]::OrdinalIgnoreCase)){throw 'Fixture path escapes owned root'};return $full}
    function Assert-KmtechLegacyEntryWritable($Entry){[void](Assert-KmtechLegacyWritablePath $Entry.path)}
    function Get-KmtechConnectionSinkEnvironment($Environment,$ServerBaseUrl,$EnrollmentTlsCaBundlePath){return $Environment}
    function Save-KmtechLegacyState($State,$Environment){$State|ConvertTo-Json -Depth 8|Set-Content -LiteralPath (Join-Path $root 'wrapper-state.json') -Encoding UTF8}
    function Add-KmtechLegacyStep($State,$Name,$Status,$Started,$Environment){$State.steps+=@{name=$Name;status=$Status};Save-KmtechLegacyState $State $Environment}
    function Get-KmtechLegacyNewInstallationState($App){if($env:LM_ROUND5_CASE -ceq 'current_retained'){return 'PRESENT'};return 'ABSENT'}
    function Invoke-KmtechLegacyUninstall($State,$Environment,$ServerBaseUrl,$EnrollmentTlsCaBundlePath){$State.code_retained=$true}
    $backup=Join-Path $root 'backup';[void][IO.Directory]::CreateDirectory($backup)
    $copy=Join-Path $backup 'old-file.dat';Copy-Item -LiteralPath $preimage -Destination $copy
    $manifest=Join-Path $backup 'manifest.jsonl'
    @{source_path=(Join-Path $original 'old-file.dat');backup_path=$copy;bytes=(Get-Item -LiteralPath $copy).Length;sha256=(Get-FileHash -LiteralPath $copy).Hash}|ConvertTo-Json -Compress|Set-Content -LiteralPath $manifest -Encoding UTF8
    $state=@{app='LM';install_phase='FAILED';backup_root=$backup;log_root=$root;entry=@{path=$original;app_dirs=@($original);never_touch=@();untouched_candidate_dirs=@();data_paths=@();config_files=@();backup_required=@();rename_targets=@($original);processes=@();autostart=@()};renames=@(@{from_path=$original;to_path=$renamed;phase='RENAMED'});autostart=@();manifest_path=$manifest;manifest_sha256=(Get-FileHash -LiteralPath $manifest).Hash;code_retained=($env:LM_ROUND5_CASE -ceq 'current_retained');status='';steps=@()}
    $restored=Invoke-KmtechLegacyRollback $state @{}
    if($restored.Status -cnotin @('ROLLED_BACK','ROLLED_BACK_CODE_RETAINED') -or (Test-Path -LiteralPath $renamed) -or -not (Test-Path -LiteralPath (Join-Path $original 'old-file.dat'))){throw 'Actual wrapper did not restore original legacy name'}
    if((Get-FileHash -LiteralPath (Join-Path $original 'old-file.dat')).Hash -cne (Get-FileHash -LiteralPath $copy).Hash){throw 'Actual wrapper restored different legacy bytes'}
    if($env:LM_ROUND5_CASE -ceq 'current_retained' -and [IO.File]::ReadAllText((Join-Path $root 'current/new-code.txt')) -cne 'preserved candidate'){throw 'Actual wrapper changed retained code'}
   }
  }
 }
 if(-not $authority.mutex.WaitOne(0)){throw 'Parent authority lost'};$authority.mutex.ReleaseMutex()
 'PASS'
}finally{Exit-LabelWriterSessionAuthority $authority}
''', engine=engine, values={"LM_ROUND5_ROOT": str(tmp_path), "LM_ROUND5_CASE": case,
                            "LM_ROUND5_LIBRARY": str(ROOT / "tools/label_writer_fence.ps1")})
    (tmp_path / "driver.stdout").write_text(result.stdout, encoding="utf-8")
    (tmp_path / "driver.stderr").write_text(result.stderr, encoding="utf-8")
    assert result.returncode == 0, f"startup rollback failed; evidence: {tmp_path}"

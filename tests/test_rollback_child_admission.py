"""Actual rollback call, native parent/child mutexes, and unchanged delegation."""
from pathlib import Path

import pytest

from tests.test_shared_ancestor_protection import run_functions

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('case', ('valid', 'child_token', 'child_busy', 'child_failure',
                                  'parent_lease', 'parent_binding', 'parent_expiry'))
def test_actual_absent_code_rollback_delegates_only_to_guarded_child(tmp_path, engine, case):
    result = run_functions(tmp_path, ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1', ['Arg'], r'''
Set-StrictMode -Version Latest
. $env:LM_ADMISSION_LIBRARY
$root=$env:LM_ADMISSION_ROOT;$control=Join-Path $root 'control'
$session='1'*32;$attempt='2'*32;$transaction='3'*32;$orchestrator='4'*64;$contract='5'*64
$synthetic='synthetic-'+('6'*64)
$authority=Enter-LabelWriterSessionAuthority $session $attempt $orchestrator $transaction $contract
$childPath=Join-Path $root 'child.ps1';$inputPath=Join-Path $root 'child-input.json';$resultPath=Join-Path $root 'child-result.json'
$child=@'
$ErrorActionPreference='Stop';Set-StrictMode -Version Latest
$inputRecord=Get-Content -LiteralPath $env:LM_ADMISSION_INPUT -Raw|ConvertFrom-Json
. $inputRecord.library
$lease=$null;$foreignDenied=$false;$success=$false;$failure=''
try {
 $foreign=$null
 try{$foreign=Enter-LabelWriterDelegatedOperation -ControlRoot $inputRecord.control -SessionId $inputRecord.session -AttemptId $inputRecord.attempt -ReplacementTransactionId $inputRecord.transaction -DelegationToken $inputRecord.delegation -Source 'ordinary_writer' -TimeoutMilliseconds 0}
 catch{$foreignDenied=$true}
 finally{if($null -ne $foreign){Exit-LabelWriterAdmission $foreign}}
 $lease=Enter-LabelWriterDelegatedOperation -ControlRoot $inputRecord.control -SessionId $inputRecord.session -AttemptId $inputRecord.attempt -ReplacementTransactionId $inputRecord.transaction -DelegationToken $inputRecord.delegation -Source 'canonical_placement' -TimeoutMilliseconds 300
 if(-not $lease.acquired -or -not $foreignDenied){throw 'Child mutation gate missing.'}
 if($inputRecord.case -ceq 'child_failure'){throw 'INJECTED_CHILD_FAILURE'}
 $success=$true
}catch{$failure=$_.Exception.Message}
finally{if($null -ne $lease){Exit-LabelWriterAdmission $lease}}
[IO.File]::WriteAllText($inputRecord.result,([ordered]@{success=$success;foreign_denied=$foreignDenied;failure=$failure}|ConvertTo-Json),[Text.UTF8Encoding]::new($false))
if($success){exit 0};exit 1
'@
[IO.File]::WriteAllText($childPath,$child,[Text.UTF8Encoding]::new($true))
$script:invocations=0
function InvokeFrozenPlacementHelper($Frozen,[hashtable]$HelperParameters){
 $script:invocations++
 $childToken=[string]$HelperParameters.WriterFenceDelegationToken
 if($env:LM_ADMISSION_CASE -ceq 'child_token'){$childToken+='invalid'}
 $record=[ordered]@{library=$env:LM_ADMISSION_LIBRARY;control=$control;session=$session;attempt=$attempt;transaction=$transaction;
  delegation=$childToken;result=$resultPath;case=$env:LM_ADMISSION_CASE}
 [IO.File]::WriteAllText($inputPath,($record|ConvertTo-Json),[Text.UTF8Encoding]::new($false))
 $busy=$null
 try {
  if($env:LM_ADMISSION_CASE -ceq 'child_busy'){$busy=Enter-LabelWriterAdmission $control}
  $info=[Diagnostics.ProcessStartInfo]::new()
  $info.FileName=(Get-Process -Id $PID).Path
  $info.Arguments='-NoLogo -NoProfile -NonInteractive -File "'+$childPath+'"'
  $info.UseShellExecute=$false;$info.CreateNoWindow=$true
  $info.RedirectStandardOutput=$true;$info.RedirectStandardError=$true
  $info.EnvironmentVariables['LM_ADMISSION_INPUT']=$inputPath
  $process=[Diagnostics.Process]::Start($info)
  if(-not $process.WaitForExit(15000)){throw 'Native admission child did not finish.'}
  $exitCode=$process.ExitCode;$process.Dispose();return $exitCode
 }finally{if($null -ne $busy){Exit-LabelWriterAdmission $busy}}
}
try {
 # Valid RFC timestamps keep the wire strings intact in both JSON adapters
 # (PS7.6 automatically materializes ISO strings as DateTime by default).
 $payload=New-LabelWriterFencePayload -Status RESTORING -SessionId $session -AttemptId $attempt -ReplacementTransactionId $transaction -SessionStartedAtUtc ([DateTime]::UtcNow.ToString('r')) -OrchestratorSha256 $orchestrator -WriterContractSha256 $contract -DelegationToken $synthetic -DelegatedSources @('canonical_placement') -DelegationExpiresAtUtc ([DateTime]::UtcNow.AddMinutes(5).ToString('r'))
 $payload.activated_at_utc=[DateTime]::UtcNow.ToString('r')
 [void](Write-LabelWriterFenceAtomic $control $payload)
 if($env:LM_ADMISSION_CASE -ceq 'parent_expiry'){
  $active=Read-LabelWriterFence $control;$active.delegation_expires_at_utc=[DateTime]::UtcNow.AddMinutes(-1).ToString('r');[void](Write-LabelWriterFenceAtomic $control $active)
 }
 $before=[IO.File]::ReadAllText((Get-LabelWriterFenceActivePath $control))
 $removeParameters=@{WriterFenceControlRoot=$control;WriterFenceSessionId=$session;WriterFenceAttemptId=$attempt;WriterFenceReplacementTransactionId=$transaction;WriterFenceDelegationToken=$synthetic;Uninstall=$true}
 $writerFenceControlRoot=$control;$writerAuthority=$authority;$frozenPlacement=$null
 if($env:LM_ADMISSION_CASE -ceq 'parent_lease'){$writerAuthority=[pscustomobject]@{acquired=$false;name=$authority.name;mutex=$authority.mutex}}
 if($env:LM_ADMISSION_CASE -ceq 'parent_binding'){$removeParameters.WriterFenceAttemptId='f'*32}
 $ast=[Management.Automation.Language.Parser]::ParseFile($env:LM_CONTRACT_SOURCE,[ref]$tokens,[ref]$errors)
 $function=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Invoke-LabelDelegatedPlacementHelper'},$true))
 if($function.Count -eq 1){. ([scriptblock]::Create($function[0].Extent.Text))}
 $calls=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.AssignmentStatementAst] -and $n.Left.Extent.Text -ceq '$removeExit'},$true))
 if($calls.Count -ne 1){throw 'Actual rollback child call ambiguous.'}
 $call=$calls[0].Extent.Text
 # Before the fix the real enclosing try/finally retains admission during
 # this actual call. Shorten only its timeout for the harmless native child.
 $old=$function.Count -eq 0;$oldLease=$null;$caught=$null
 try {
  if($old){$oldLease=Enter-LabelWriterAdmission $control}
  . ([scriptblock]::Create($call))
 }catch{$caught=$_}
 finally{if($null -ne $oldLease){Exit-LabelWriterAdmission $oldLease}}
 $parentReject=$env:LM_ADMISSION_CASE -cin @('parent_lease','parent_binding','parent_expiry')
 if($parentReject){
  if($null -eq $caught -or $script:invocations -ne 0){throw 'Invalid parent delegation reached child.'}
 }else{
  if($null -ne $caught){throw $caught}
  $result=Get-Content -LiteralPath $resultPath -Raw|ConvertFrom-Json
  if(-not $result.foreign_denied){throw 'Ordinary writer passed active fence.'}
  if($env:LM_ADMISSION_CASE -ceq 'valid'){
   if($removeExit -ne 0 -or -not $result.success){throw ('Valid delegated child failed: '+$result.failure)}
  }else{
   $expected=@{child_token='LABEL_WRITER_FENCE_OWNER_DELEGATION_MISMATCH';child_busy='LABEL_WRITER_ADMISSION_MUTEX_TIMEOUT';child_failure='INJECTED_CHILD_FAILURE'}[$env:LM_ADMISSION_CASE]
   if($removeExit -ne 1 -or $result.success -or $result.failure -cne $expected){throw 'Child refusal/failure was lost.'}
  }
 }
 if([IO.File]::ReadAllText((Get-LabelWriterFenceActivePath $control)) -cne $before){throw 'Delegation changed active fence.'}
 $check=Enter-LabelWriterAdmission $control -TimeoutMilliseconds 0;Exit-LabelWriterAdmission $check
 # Balancing this recursion must preserve the original session owner until
 # the final cleanup; the child already checked HeldByOther natively.
 if(-not $authority.mutex.WaitOne(0)){throw 'Parent authority lost.'};$authority.mutex.ReleaseMutex()
 'PASS'
}finally{Exit-LabelWriterSessionAuthority $authority}
''', engine=engine, values={'LM_ADMISSION_ROOT': str(tmp_path), 'LM_ADMISSION_CASE': case,
                            'LM_ADMISSION_LIBRARY': str(ROOT / 'tools/label_writer_fence.ps1')})
    assert result.returncode == 0, result.stdout + result.stderr

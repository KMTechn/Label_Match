"""Protected-owner publication: actual PS5/7 functions, NTFS, process creation.

Host elevation is forbidden. Only privileged path/ACL and writer admissions
are seams; source PID/start validation, immutable bytes and readback are real.
"""
from pathlib import Path
import pytest
from tests.test_shared_ancestor_protection import run_functions

ROOT = Path(__file__).resolve().parents[1]

@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('case', ('publish', 'wrong-start', 'wrong-pid', 'wrong-fence', 'conflict'))
def test_installer_publishes_only_native_bound_immutable_owner(tmp_path, engine, case):
    result = run_functions(tmp_path, ROOT/'INSTALL_THIS_PC.ps1', ['Write-LabelInstallerOwnerRecord'], r'''
$InstallRoot=Join-Path $env:LM_OWNER_ROOT 'current'
$InstallerRunId='20261004T000000000Z-'+('a'*32)
$InstallerOwnerProcessId=$PID
$InstallerOwnerProcessStartedAtFileTime=[Diagnostics.Process]::GetCurrentProcess().StartTime.ToUniversalTime().ToFileTimeUtc().ToString()
$WriterFenceControlRoot=Join-Path $env:LM_OWNER_ROOT 'control'
$WriterFenceSessionId='1'*32;$WriterFenceAttemptId='2'*32;$WriterFenceReplacementTransactionId='3'*32
$script:active=[pscustomobject]@{session_id=$WriterFenceSessionId;attempt_id=$WriterFenceAttemptId;replacement_transaction_id=$WriterFenceReplacementTransactionId;
 writer_inventory_sha256=('4'*64);orchestrator_sha256=('5'*64);writer_contract_sha256=('6'*64);session_started_at_utc='2026-10-04T00:00:00Z'}
$script:protected=0;$script:released=0
function Enter-LabelCanonicalPlacement {param($TimeoutMilliseconds) return 'owned-test-lease'}
function Exit-LabelWriterAdmission {param($Lease) $script:released++}
function Read-LabelWriterFence {param($Root) return $script:active}
function Assert-ProtectedCodeAncestors {param($Path)}
function Assert-NoReparsePoint {param($Path,$Context) if(Test-Path -LiteralPath $Path){if((Get-Item -LiteralPath $Path).Attributes -band [IO.FileAttributes]::ReparsePoint){throw 'reparse'}}}
function Protect-LabelInstallerOwnerFile {param($Path) $script:protected++}
$case=$env:LM_OWNER_CASE
if($case -ceq 'wrong-start'){$InstallerOwnerProcessStartedAtFileTime=([int64]$InstallerOwnerProcessStartedAtFileTime+1).ToString()}
if($case -ceq 'wrong-pid'){$InstallerOwnerProcessId=0}
if($case -ceq 'wrong-fence'){$script:active.attempt_id='f'*32}
$failure=$null
try {Write-LabelInstallerOwnerRecord $InstallRoot} catch {$failure=$_}
$path=Join-Path $env:LM_OWNER_ROOT ('.installer-owner-'+$WriterFenceSessionId+'-'+('a'*32)+'.json')
if($case -in @('wrong-start','wrong-pid','wrong-fence')){
 if($null -eq $failure -or (Test-Path -LiteralPath $path)){throw 'Invalid owner published evidence'}
} else {
 if($null -ne $failure){throw $failure}
 $bytes=[IO.File]::ReadAllBytes($path);$written=(Get-Item -LiteralPath $path).LastWriteTimeUtc.Ticks
 $record=Get-Content -LiteralPath $path -Raw|ConvertFrom-Json
 if($record.owner_process_id -ne $PID -or $record.owner_process_started_at_filetime -cne $InstallerOwnerProcessStartedAtFileTime -or
    $record.run_id -cne $InstallerRunId -or $record.attempt_id -cne $WriterFenceAttemptId){throw 'Native binding differs'}
 if($case -ceq 'conflict'){
  $record.owner_process_started_at_filetime='1'
  $record|ConvertTo-Json -Depth 5 -Compress|Set-Content -LiteralPath $path -Encoding utf8
  $before=[IO.File]::ReadAllBytes($path);$denied=$false
  try{Write-LabelInstallerOwnerRecord $InstallRoot}catch{$denied=$true}
  if(-not $denied -or [Convert]::ToBase64String($before) -cne [Convert]::ToBase64String([IO.File]::ReadAllBytes($path))){throw 'Conflicting record overwritten'}
 } else {
  Write-LabelInstallerOwnerRecord $InstallRoot
  if($script:protected -ne 1 -or (Get-Item -LiteralPath $path).LastWriteTimeUtc.Ticks -ne $written -or
     [Convert]::ToBase64String($bytes) -cne [Convert]::ToBase64String([IO.File]::ReadAllBytes($path))){throw 'Owner proof rewritten'}
 }
 if($script:released -ne 2){throw 'Publication lease leaked'}
}
'PASS'
''',engine=engine,values={'LM_OWNER_ROOT':str(tmp_path),'LM_OWNER_CASE':case})
    assert result.returncode == 0, result.stdout + result.stderr

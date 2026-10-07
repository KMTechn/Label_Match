"""Native guardian/protocol checks without elevation or product installation.

Only the existing launch seam replaces RunAs with a same-context hidden child.
Code/startup restoration under a privileged token remains a lab acceptance gate.
"""
import hashlib
import json
from pathlib import Path
import time

import pytest

from tests.test_shared_ancestor_protection import run_functions


INSTALLER = Path(__file__).resolve().parents[1] / 'INSTALL_CANONICAL_PORTABLE.ps1'
SESSION_FUNCTIONS = ['ByteSha', 'Sha', 'Initialize-LabelPlacementSessionProtocol',
                     'Read-LabelPlacementSessionRecord', 'Start-LabelPlacementSession',
                     'Invoke-LabelPlacementSessionCommand', 'Close-LabelPlacementSession',
                     'Stop-LabelRegistrationChild', 'Wait-LabelPlacementChild']
SESSION_SETUP = r"""
$root=$env:LM_SESSION_ROOT
$winps=Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe'
$principal=[Security.Principal.WindowsPrincipal]::new([Security.Principal.WindowsIdentity]::GetCurrent())
$elevated=$principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
$launch=@{FilePath=$winps;ArgumentList=@();WindowStyle='Hidden';Wait=$true;PassThru=$true}
if(-not $elevated){$launch.Verb='RunAs'}
$script:dispatches=0;$script:consents=0
$transport={param($options)
 $script:dispatches++
 if($options.ContainsKey('Verb')){$script:consents++}
 if($options.ContainsKey('Verb') -eq $elevated){throw 'TOKEN_DISPATCH_DIFFERS'}
 if($options.WindowStyle -cne 'Hidden' -or $options.Wait){throw 'GUARDIAN_LAUNCH_DIFFERS'}
 $sameContext=$options.Clone();$sameContext.Remove('Verb')
 Start-Process @sameContext
}
$session=Start-LabelPlacementSession $launch 'transaction-native-1' (Join-Path $root 'session') $transport
"""


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
def test_one_guardian_dispatch_runs_children_with_actual_exit_codes_and_seals_last(tmp_path, engine):
    result = run_functions(tmp_path, INSTALLER, SESSION_FUNCTIONS, SESSION_SETUP + r"""
try {
 foreach($expected in @(70,0)){
  $command=[Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes(('exit '+$expected)))
  if((Invoke-LabelPlacementSessionCommand $session $command) -ne $expected){throw 'ACTUAL_EXIT_CODE_LOST'}
 }
 Close-LabelPlacementSession $session 'REVERTED'
 if($script:dispatches -ne 1 -or $script:consents -ne [int](-not $elevated)){throw 'APPROVAL_COUNT_DIFFERS'}
 $journal=[IO.File]::ReadAllText($session.result_base+'.journal.jsonl')
 $rows=@($journal.TrimEnd().Split([char]10)|ForEach-Object {$_|ConvertFrom-Json})
 if($rows[-1].phase -cne 'TERMINAL' -or $rows[-1].status -cne 'REVERTED'){throw 'JOURNAL_NOT_FINAL'}
 if([IO.File]::Exists($session.guardian_path)){throw 'GUARDIAN_CODE_NOT_RETIRED'}
 'ONE_DISPATCH_ACTUAL_CHILD_EXIT_SEALED'
} finally {if(-not $session.closed){Close-LabelPlacementSession $session 'ATTENTION'}}
""", values={'LM_SESSION_ROOT': str(tmp_path), 'KMTECH_FACTORY_INSTALL_TEST_MODE': '1'}, engine=engine)
    assert result.returncode == 0, result.stderr
    assert 'ONE_DISPATCH_ACTUAL_CHILD_EXIT_SEALED' in result.stdout
    seal_path = next(tmp_path.glob('*.seal.json'))
    seal = json.loads(seal_path.read_text())
    journal = Path(str(seal_path).removesuffix('.seal.json') + '.journal.jsonl')
    assert seal['sha256'] == hashlib.sha256(journal.read_bytes()).hexdigest()


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('fault', ('hash', 'transaction', 'sequence', 'oversize', 'reparse', 'missing'))
def test_result_record_requires_pinned_bounded_ordinary_same_transaction(tmp_path, engine, fault):
    result = run_functions(tmp_path, INSTALLER, ['ByteSha', 'Read-LabelPlacementSessionRecord'], r"""
$path=Join-Path $env:LM_SESSION_ROOT 'result.json'
$record=@{schema='label-placement-session-result-v1';transaction_id='expected';sequence=1;exit_code=0}
if($env:LM_RESULT_FAULT -ceq 'transaction'){$record.transaction_id='another'}
if($env:LM_RESULT_FAULT -ceq 'sequence'){$record.sequence=2}
[byte[]]$bytes=[Text.Encoding]::UTF8.GetBytes(($record|ConvertTo-Json -Compress))
$expected=ByteSha $bytes
if($env:LM_RESULT_FAULT -ceq 'hash'){$bytes[0]=[byte][char]' '}
if($env:LM_RESULT_FAULT -ceq 'oversize'){$bytes=[byte[]]::new(65537);$expected=ByteSha $bytes}
[IO.File]::WriteAllBytes($path,$bytes)
if($env:LM_RESULT_FAULT -ceq 'missing'){[IO.File]::Delete($path)}
if($env:LM_RESULT_FAULT -ceq 'reparse'){
 $target=Join-Path $env:LM_SESSION_ROOT 'ordinary';[void][IO.Directory]::CreateDirectory($target)
 [IO.File]::Move($path,(Join-Path $target 'result.json'))
 $junction=Join-Path $env:LM_SESSION_ROOT 'junction'
 $start=[Diagnostics.ProcessStartInfo]::new();$start.FileName=$env:ComSpec
 $start.Arguments='/d /c mklink /J "'+$junction+'" "'+$target+'"'
 $start.UseShellExecute=$false;$start.CreateNoWindow=$true;$start.RedirectStandardOutput=$true;$start.RedirectStandardError=$true
 $child=[Diagnostics.Process]::Start($start);$child.WaitForExit();if($child.ExitCode -ne 0){throw 'JUNCTION_SETUP_FAILED'};$child.Dispose()
 $path=Join-Path $junction 'result.json'
}
$rejected=$false
try{[void](Read-LabelPlacementSessionRecord $path $expected 'expected' 1)}catch{$rejected=$true}
if(-not $rejected){throw 'INVALID_RESULT_ADMITTED'}
'INVALID_RESULT_REJECTED'
""", values={'LM_SESSION_ROOT': str(tmp_path), 'LM_RESULT_FAULT': fault}, engine=engine)
    assert result.returncode == 0, result.stderr
    assert 'INVALID_RESULT_REJECTED' in result.stdout


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
def test_parent_exit_is_fail_closed_with_typed_location_and_final_journal(tmp_path, engine):
    result = run_functions(tmp_path, INSTALLER, SESSION_FUNCTIONS, SESSION_SETUP + r"""
[IO.File]::WriteAllText((Join-Path $root 'guardian-base.txt'),$session.result_base)
# The live guardian observes this exact parent's native process handle exiting.
exit 0
""", values={'LM_SESSION_ROOT': str(tmp_path), 'KMTECH_FACTORY_INSTALL_TEST_MODE': '1'}, engine=engine)
    assert result.returncode == 0, result.stderr
    result_base = (tmp_path / 'guardian-base.txt').read_text()
    failure = Path(result_base + '.failure.json')
    seal = Path(result_base + '.seal.json')
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline and not seal.exists():
        time.sleep(.1)
    assert seal.exists(), 'guardian did not settle after its owner exited'
    record = json.loads(failure.read_text())
    assert record['status'] == 'RECOVERY_REQUIRED'
    assert record['exception_type'] and record['location_module'] and record['location_function']
    assert record['location_line'] > 0
    terminal = json.loads(seal.read_text())
    assert terminal['status'] == 'RECOVERY_REQUIRED' and terminal['exit_code'] == 70
    assert not list(tmp_path.glob('*.1.json')), 'orphan guardian executed a mutation'


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
def test_registration_timeout_stops_only_proven_child_before_returning_rollback_permission(tmp_path, engine):
    result = run_functions(tmp_path, INSTALLER,
        ['Stop-LabelRegistrationChild', 'Wait-LabelRegistrationChild'], r"""
$start=[Diagnostics.ProcessStartInfo]::new()
$start.FileName=Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe'
$start.Arguments='-NoLogo -NoProfile -NonInteractive -Command Start-Sleep -Seconds 60'
$start.UseShellExecute=$false;$start.CreateNoWindow=$true
$child=[Diagnostics.Process]::Start($start)
try {
 $failed=$false
 try{Wait-LabelRegistrationChild $child 250}catch{
  if(-not $_.Exception.Data.Contains('registration_child_exit_verified') -or $_.Exception.Data.Contains('registration_child_unproven')){throw}
  $failed=$true
 }
 if(-not $failed -or -not $child.HasExited){throw 'ROLLBACK_PERMITTED_WITH_LIVE_CHILD'}
 'OWNED_CHILD_STOPPED_BEFORE_ROLLBACK'
} finally {$child.Dispose()}
""", engine=engine)
    assert result.returncode == 0, result.stderr
    assert 'OWNED_CHILD_STOPPED_BEFORE_ROLLBACK' in result.stdout


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('cancelled', (False, True))
def test_actual_placement_function_uses_one_transport_for_multiple_steps_or_cancellation(tmp_path, engine, cancelled):
    result = run_functions(tmp_path, INSTALLER,
        SESSION_FUNCTIONS + ['InvokeFrozenPlacementHelper'], r"""
$root=$env:LM_SESSION_ROOT
[void][IO.Directory]::CreateDirectory((Join-Path $root 'tools'))
$files=@{helper=(Join-Path $root 'helper.ps1');integrity=(Join-Path $root 'tools\bootstrap_integrity.ps1');writer=(Join-Path $root 'writer.ps1');generation=(Join-Path $root 'generation.ps1')}
[IO.File]::WriteAllText($files.helper,'param([string]$SourceRoot) [IO.File]::AppendAllText($SourceRoot,"step`n")')
[IO.File]::WriteAllText($files.integrity,'param([string]$SharedCodeRoot)')
[IO.File]::WriteAllText($files.writer,'# isolated transport input')
[IO.File]::WriteAllText($files.generation,'# isolated transport input')
$frozen=[pscustomobject]@{root=$root;helper_path=$files.helper;helper_sha256=(ByteSha ([IO.File]::ReadAllBytes($files.helper)));integrity_sha256=(ByteSha ([IO.File]::ReadAllBytes($files.integrity)));writer_fence_path=$files.writer;writer_fence_sha256=(ByteSha ([IO.File]::ReadAllBytes($files.writer)));data_generation_path=$files.generation;data_generation_sha256=(ByteSha ([IO.File]::ReadAllBytes($files.generation)))}
$parameters=@{SourceRoot=(Join-Path $root 'child-steps.txt');InstallRoot=(Join-Path $root 'isolated-code');ElevationLogPath=(Join-Path $root 'placement');ExpectedBootstrapScriptSha256=$frozen.helper_sha256;VerifiedBootstrapScriptPath=$files.helper;BootstrapIntegrityPreloaded=$true;ExpectedSourceAggregateSha256=('e'*64);ExpectedSourceFileCount=0;ExpectedSourceByteCount=[uint64]0;WriterFenceFunctionsPreloaded=$true;WriterFenceControlRoot=(Join-Path $root 'control');DataGenerationSubjectSid='';DataGenerationMarkerPaths=@();WriterFenceSessionId='session';WriterFenceAttemptId='attempt';WriterFenceReplacementTransactionId='transaction-transport';WriterFenceDelegationToken='';AllowNoncanonicalLayoutForTest=$true;ReplaceExistingVerifiedPortable=$false;DryRun=$false}
$Script:LabelPlacementSessionRequested=$true;$Script:LabelPlacementSession=$null;$Script:LabelPlacementSessionLaunchAttempted=$false
$Script:dispatchCount=0;$Script:consentCount=0
$elevated=([Security.Principal.WindowsPrincipal]::new([Security.Principal.WindowsIdentity]::GetCurrent())).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
$transport={param($options)
 $Script:dispatchCount++
 if($options.ContainsKey('Verb')){$Script:consentCount++}
 if($env:LM_CANCELLED -ceq '1'){throw [ComponentModel.Win32Exception]::new(1223)}
 $same=$options.Clone();$same.Remove('Verb');Start-Process @same
}
try {
 for($step=0;$step -lt 2;$step++){
  if($env:LM_CANCELLED -ceq '1'){
   $rejected=$false
   try{[void](InvokeFrozenPlacementHelper $frozen $parameters $transport)}catch{
    $base=$_.Exception.GetBaseException()
    if($step -eq 0 -and ($base -isnot [ComponentModel.Win32Exception] -or $base.NativeErrorCode -ne 1223)){throw}
    if($step -eq 1 -and $_.Exception.Message -cne 'PLACEMENT_GUARDIAN_UNAVAILABLE'){throw}
    $rejected=$true
   }
   if(-not $rejected){throw 'CANCELLATION_ADMITTED'}
  }else{
   if((InvokeFrozenPlacementHelper $frozen $parameters $transport) -ne 0){throw 'PINNED_CHILD_FAILED'}
  }
 }
 if($Script:dispatchCount -ne 1 -or $Script:consentCount -ne [int](-not $elevated)){throw 'SECOND_APPROVAL_REQUESTED'}
 if($null -ne $Script:LabelPlacementSession){Close-LabelPlacementSession $Script:LabelPlacementSession 'REVERTED'}
 if($env:LM_CANCELLED -ceq '1'){
  if([IO.File]::Exists($parameters.SourceRoot)){throw 'CANCELLED_CHILD_EXECUTED'}
 }elseif([IO.File]::ReadAllLines($parameters.SourceRoot).Count -ne 2){throw 'PINNED_CHILD_STEPS_LOST'}
 'ACTUAL_PLACEMENT_ONE_DISPATCH'
}finally{
 if($null -ne $Script:LabelPlacementSession -and -not $Script:LabelPlacementSession.closed){Close-LabelPlacementSession $Script:LabelPlacementSession 'ATTENTION'}
 $Script:LabelPlacementSessionRequested=$false
}
""", values={'LM_SESSION_ROOT': str(tmp_path), 'LM_CANCELLED': '1' if cancelled else '0',
             'KMTECH_FACTORY_INSTALL_TEST_MODE': '1'}, engine=engine)
    assert result.returncode == 0, result.stderr
    assert 'ACTUAL_PLACEMENT_ONE_DISPATCH' in result.stdout


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
def test_actual_failure_formatter_keeps_hyphenated_function_and_typed_location(tmp_path, engine):
    result = run_functions(tmp_path, INSTALLER, ['Get-LabelLaunchFailure'], r"""
function Invoke-ActualLocationFailure {throw [InvalidOperationException]::new('synthetic-private-value')}
try{Invoke-ActualLocationFailure}catch{
 $record=Get-LabelLaunchFailure $_ 'forward'
 if($record.exception_type -cne 'System.InvalidOperationException' -or
    $record.location_function -cne 'Invoke-ActualLocationFailure' -or
    $record.location_module -cne 'INSTALL_CANONICAL_PORTABLE.ps1' -or $record.location_line -le 0){throw 'FAILURE_LOCATION_LOST'}
 if(($record|ConvertTo-Json -Compress).Contains('synthetic-private-value')){throw 'FAILURE_VALUE_PUBLIC'}
}
$report=Join-Path (Get-Location).Path 'guardian-failure.json'
[IO.File]::WriteAllText($report,'{"status":"RECOVERY_REQUIRED"}',[Text.UTF8Encoding]::new($false))
$sha=(Get-FileHash -LiteralPath $report -Algorithm SHA256).Hash.ToLowerInvariant()
$forwarded=[InvalidOperationException]::new('synthetic-private-forwarder')
$forwarded.Data['child_output']=[ordered]@{exception_type='System.TimeoutException';location_module='INSTALL_CANONICAL_PORTABLE.ps1';location_function='Wait-LabelPlacementChild';location_line=7;report_path=$report;report_sha256=$sha}
try{throw $forwarded}catch{
 $record=Get-LabelLaunchFailure $_ 'forward'
 if($record.exception_type -cne 'System.TimeoutException' -or $record.location_function -cne 'Wait-LabelPlacementChild' -or
    $record.location_line -ne 7 -or $record.report_path -cne $report -or $record.report_sha256 -cne $sha){throw 'PINNED_ORIGINAL_HELPER_LOCATION_LOST'}
 if(($record|ConvertTo-Json -Compress).Contains('synthetic-private-forwarder')){throw 'FORWARDED_FAILURE_VALUE_PUBLIC'}
}
'TYPED_LOCATION_PRESERVED'
""", engine=engine)
    assert result.returncode == 0, result.stderr
    assert 'TYPED_LOCATION_PRESERVED' in result.stdout


def test_python_location_function_accepts_powershell_identifier_without_free_text():
    from failure_redaction import public_record
    assert public_record({'location_function': 'Confirm-LabelCanonicalPlacement'}) == {
        'location_function': 'Confirm-LabelCanonicalPlacement'}
    invalid = public_record({'location_function': 'Invoke-Function arbitrary value'})
    assert 'location_function' not in invalid


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('owned', (True, False))
def test_running_helper_deadline_requires_owned_exit_and_records_actual_native_code(tmp_path, engine, owned):
    result = run_functions(tmp_path, INSTALLER,
        ['Stop-LabelRegistrationChild', 'Wait-LabelPlacementChild'], r"""
$parent=[Diagnostics.Process]::GetCurrentProcess()
$child=$parent
if($env:LM_OWNED_CHILD -ceq '1'){
 $start=[Diagnostics.ProcessStartInfo]::new()
 $start.FileName=Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe'
 $start.Arguments='-NoLogo -NoProfile -NonInteractive -Command Start-Sleep -Seconds 60'
 $start.UseShellExecute=$false;$start.CreateNoWindow=$true
 $child=[Diagnostics.Process]::Start($start)
}
try {
 $caught=$false
 try{[void](Wait-LabelPlacementChild $child $parent 250)}catch{
  $failure=$_.Exception.GetBaseException()
  if($failure -isnot [TimeoutException] -or $failure.Message -cne 'HELPER_EXECUTION_EXPIRED'){throw}
  if($env:LM_OWNED_CHILD -ceq '1'){
   if(-not $child.HasExited -or -not $failure.Data.Contains('helper_child_exit_verified') -or
      $failure.Data.Contains('helper_child_unproven') -or
      [int]$failure.Data['helper_child_exit_code'] -ne [int]$child.ExitCode){throw 'NATIVE_EXIT_NOT_PROVEN'}
  }elseif(-not $failure.Data.Contains('helper_child_unproven') -or
           $failure.Data.Contains('helper_child_exit_verified') -or $parent.HasExited){throw 'UNOWNED_CHILD_STOPPED'}
  $caught=$true
 }
 if(-not $caught){throw 'HELPER_DEADLINE_NOT_ENFORCED'}
 'ACTUAL_HELPER_DEADLINE_EXIT_PROOF'
} finally {if($child -ne $parent){$child.Dispose()};$parent.Dispose()}
""", values={'LM_OWNED_CHILD': '1' if owned else '0'}, engine=engine)
    assert result.returncode == 0, result.stderr
    assert 'ACTUAL_HELPER_DEADLINE_EXIT_PROOF' in result.stdout


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
def test_parent_exit_during_running_helper_seals_before_the_child_finishes(tmp_path, engine):
    result = run_functions(tmp_path, INSTALLER, SESSION_FUNCTIONS, SESSION_SETUP + r"""
[IO.File]::WriteAllText((Join-Path $root 'guardian-base.txt'),$session.result_base)
$marker=Join-Path $root 'running-helper.txt'
$body='[IO.File]::WriteAllText('''+$marker.Replace("'","''")+''',[string]$PID);Start-Sleep -Seconds 12;exit 0'
$command=[Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($body))
$session.sequence=1
[LabelPlacementChannel]::Write($session.pipe,(@{operation='EXECUTE';transaction_id=$session.transaction_id;sequence=1;command=$command;rollback=$false}|ConvertTo-Json -Compress))
$clock=[Diagnostics.Stopwatch]::StartNew()
while(-not [IO.File]::Exists($marker)){
 if($clock.ElapsedMilliseconds -ge 15000){throw 'HELPER_DID_NOT_START'}
 Start-Sleep -Milliseconds 50
}
exit 0
""", values={'LM_SESSION_ROOT': str(tmp_path), 'KMTECH_FACTORY_INSTALL_TEST_MODE': '1'}, engine=engine)
    assert result.returncode == 0, result.stderr
    result_base = (tmp_path / 'guardian-base.txt').read_text()
    seal = Path(result_base + '.seal.json')
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and not seal.exists():
        time.sleep(.05)
    assert seal.exists(), 'guardian waited for a child after losing its owner'
    record = json.loads(Path(result_base + '.failure.json').read_text())
    assert record['status'] == 'RECOVERY_REQUIRED' and record['reason_code'] == 'PARENT_EXITED'
    assert record['exception_type'] and record['location_function'] and record['location_line'] > 0
    assert json.loads(seal.read_text())['status'] == 'RECOVERY_REQUIRED'
    assert not list(tmp_path.glob('*.1.json'))


def test_local_pipe_rejects_remote_connections_without_denying_ssh_network_groups():
    source = INSTALLER.read_text(encoding='utf-8-sig')
    session = source.split('function Start-LabelPlacementSession(', 1)[1].split('function Invoke-LabelPlacementSessionCommand(', 1)[0]
    assert "SecurityIdentifier]::new('S-1-5-2')" not in session
    assert 'CreateNamedPipe(@"\\\\.\\pipe\\"+name,0x40080003,0x00000008,' in source
    assert '[LabelPlacementChannel]::Server($pipeName,$security.GetSecurityDescriptorBinaryForm())' in session
    assert 'GetNamedPipeClientProcessId' in source and 'GetNamedPipeServerProcessId' in source
    assert '$parent.StartTime.ToUniversalTime().ToFileTimeUtc() -ne $ParentBirth' in session


def test_guardian_pins_created_bytes_and_seals_the_still_open_journal_handle():
    source = INSTALLER.read_text(encoding='utf-8-sig')
    guardian = source.split("$guardian=@'", 1)[1].split("\n'@", 1)[0]
    publish = guardian.split('function Publish(', 1)[1].split('function Append(', 1)[0]
    assert 'return (ByteSha $bytes)' in publish
    assert 'Get-FileHash' not in publish
    assert "Open($journalPath,'CreateNew','ReadWrite','Read')" in guardian
    terminal = guardian.index("phase='TERMINAL'")
    sealed = guardian.index('$hash.ComputeHash($journal)', terminal)
    closed = guardian.index('$journal.Dispose()', terminal)
    assert terminal < sealed < closed
    assert 'Append ' not in guardian[sealed:]
    assert 'Get-FileHash' not in guardian[terminal:]

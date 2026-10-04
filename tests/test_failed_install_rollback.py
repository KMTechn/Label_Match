from pathlib import Path
import pytest
from tests.test_shared_ancestor_protection import run_functions

INSTALLER = Path(__file__).resolve().parents[1] / 'INSTALL_CANONICAL_PORTABLE.ps1'


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
def test_packet_runtime_removal_binds_canonical_logical_root(tmp_path, engine):
    result = run_functions(tmp_path, INSTALLER, ['Arg', 'Product'], r'''
$source=Join-Path $env:LM_ROLLBACK_ROOT 'packet'
$install=Join-Path $env:LM_ROLLBACK_ROOT 'canonical\current'
$script:arguments=''
function Start-Process {
 param($FilePath,$ArgumentList,$WindowStyle,[switch]$PassThru,$RedirectStandardOutput,$RedirectStandardError)
 $script:arguments=$ArgumentList
 if($FilePath -cne (Join-Path $source 'runtime\pythonw.exe')){throw 'Execution source changed.'}
 [pscustomobject]@{ExitCode=0;HasExited=$true}
}
function Wait-Process {param($InputObject)}
Product $source '--remove-current-user-setup' -LogicalAppRoot $install
$expected='-I -B {0} --remove-current-user-setup --app-root {1}' -f (Arg (Join-Path $source 'app\main.py')),(Arg $install)
if($arguments -cne $expected){throw 'Packet source was used as logical install root.'}
'PASS'
''', engine=engine, values={'LM_ROLLBACK_ROOT': str(tmp_path)})
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('cut', ('remove', 'run', 'task', 'custody'))
def test_actual_install_rollback_attempts_later_restoration_and_records_failure(tmp_path, engine, cut):
    names = ['Get-LabelLaunchFailure']
    if 'function Invoke-LabelRollbackStep' in INSTALLER.read_text(encoding='utf-8'):
        names.append('Invoke-LabelRollbackStep')
    result = run_functions(tmp_path, INSTALLER, names, r'''
$root=$env:LM_ROLLBACK_ROOT
$source=Join-Path $root 'packet';$install=Join-Path $root 'canonical\current'
$stop=Join-Path $root 'stop.json';$removalPath=Join-Path $root 'removal.json'
$auditPath=Join-Path $root 'audit.json';$EvidencePath='';$localAuditRoot=$root
$mutated=$true;$legacyFirstRegistration=$false;$healthyRemovedInstall=$false
$existingVerified=$false;$writerFenceStarted=$true;$writerAuthority=1
$startupLifecycle=$null;$old=@();$before=[pscustomobject]@{exists=$false;kind='';data=''}
$taskBefore=[pscustomobject]@{exists=$false};$stopBefore=[pscustomobject]@{exists=$false}
$healthyRemovalReportBefore=$null;$removalReportExisted=$false
$writerFenceControlRoot=Join-Path $root 'control'
$writerSessionId='1';$writerAttemptId='2';$writerTransactionId='3';$writerDelegationToken='synthetic'
$rollbackSources=@('current_user_setup_removal')
$audit=[ordered]@{rollback=[ordered]@{};writer_fence=[ordered]@{}}
$script:events=[Collections.Generic.List[string]]::new()
function Same {param($A,$B) return [StringComparer]::OrdinalIgnoreCase.Equals([IO.Path]::GetFullPath($A),[IO.Path]::GetFullPath($B))}
function Set-LabelWriterFenceDelegation {param($ControlRoot,$Status,$SessionId,$AttemptId,$ReplacementTransactionId,$DelegationToken,$DelegatedSources,$LifetimeSeconds)
 if($env:LM_ROLLBACK_CUT -eq 'custody'){throw 'injected custody failure'}
}
function SetWriterDelegationEnvironment {param($a,$b,$c,$d)}
function Set-LabelWriterFenceInstalledTree {param($a,$b,$c,$d,$e)}
function Enter-LabelWriterAdmission {param($ControlRoot,$TimeoutMilliseconds) return 1}
function Exit-LabelWriterAdmission {param($Lease)}
function Product {param($Root,$Mode,$ExtraArguments,$LogicalAppRoot)
 $events.Add('remove');if($env:LM_ROLLBACK_CUT -eq 'remove'){throw 'injected removal failure'}
}
function Assert-RollbackRelayPreimage {param($ExpectedRelays) $events.Add('relay-readback')}
function Restore {param($Snapshot) $events.Add('run');if($env:LM_ROLLBACK_CUT -eq 'run'){throw 'injected Run restore failure'}}
function RestoreScheduledTask {param($Snapshot) $events.Add('task');if($env:LM_ROLLBACK_CUT -eq 'task'){throw 'injected task restore failure'}}
function Stop-LabelWriterFence {param($ControlRoot,$SessionId,$AttemptId,$ReplacementTransactionId,$TimeoutMilliseconds)
 $events.Add('fence-release');[pscustomobject]@{writer_inventory_sha256='fixture'}
}
function Snapshot {$events.Add('run-readback');return $before}
function Save {param($Path,$Value) [IO.File]::WriteAllText($Path,($Value|ConvertTo-Json -Depth 20))}
function Full {param($Path,$Name) return $Path}
$ast=[Management.Automation.Language.Parser]::ParseFile($env:LM_CONTRACT_SOURCE,[ref]$tokens,[ref]$errors)
$catches=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.CatchClauseAst] -and
 $n.Body.Extent.Text.Contains('$original = $_')},$true))
if($catches.Count -ne 1){throw 'Actual transaction rollback catch is ambiguous.'}
$body=$catches[0].Body.Extent.Text
$body=$body.Substring(1,$body.Length-2)
try {throw 'original forward failure'} catch {try{. ([scriptblock]::Create($body))}catch{}}
if(-not $events.Contains('run-readback')){throw 'Final rollback observation skipped.'}
if($env:LM_ROLLBACK_CUT -eq 'custody'){
 if($events.Contains('run') -or $events.Contains('task')){throw 'Mutation passed unproven deployment custody.'}
}elseif(-not $events.Contains('run') -or -not $events.Contains('task')){throw 'Later independent rollback steps were skipped.'}
if($audit.status -cne 'ROLLBACK_FAILED' -or $audit.rollback.runtime_restored -ne $false){throw 'Incomplete rollback reported success.'}
if(@($audit.rollback.step_failures).Count -eq 0){throw 'Original rollback step failures absent.'}
if($events.Contains('fence-release')){throw 'Failed recovery released the protective fence.'}
'PASS'
''', engine=engine, values={'LM_ROLLBACK_ROOT': str(tmp_path), 'LM_ROLLBACK_CUT': cut})
    assert result.returncode == 0, result.stdout + result.stderr

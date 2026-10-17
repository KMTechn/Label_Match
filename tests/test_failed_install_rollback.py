from pathlib import Path
import pytest
from tests.test_shared_ancestor_protection import run_functions

INSTALLER = Path(__file__).resolve().parents[1] / 'INSTALL_CANONICAL_PORTABLE.ps1'


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
def test_packet_runtime_removal_binds_canonical_logical_root(tmp_path, engine):
    result = run_functions(tmp_path, INSTALLER, ['Arg', 'Product', 'Initialize-LabelChildOutputReader',
                           'ConvertTo-LabelFailureOutput', 'Protect-LabelFailureText', 'New-LabelChildFailure'], r"""
$source=Join-Path $env:LM_ROLLBACK_ROOT 'packet'
$install=Join-Path $env:LM_ROLLBACK_ROOT 'canonical\current'
New-Item -ItemType Directory -Path (Join-Path $source 'runtime') -Force | Out-Null
$capture=Join-Path $env:LM_ROLLBACK_ROOT 'arguments.txt'
$env:LM_CAPTURE_ARGS=$capture
$compiler=Join-Path $env:LM_ROLLBACK_ROOT 'compile.ps1'
$compilerText=@'
param($OutputPath)
Add-Type -OutputAssembly $OutputPath -OutputType ConsoleApplication -TypeDefinition @"
using System;
using System.IO;
public static class ArgumentProbe {
 public static void Main(string[] args) {File.WriteAllText(Environment.GetEnvironmentVariable("LM_CAPTURE_ARGS"),String.Join("\n",args));}
}
"@
'@
[IO.File]::WriteAllText($compiler,$compilerText,[Text.UTF8Encoding]::new($false))
& powershell.exe -NoProfile -NonInteractive -File $compiler (Join-Path $source 'runtime\pythonw.exe')
if($LASTEXITCODE -ne 0){throw 'Isolated child compilation failed'}
# 25a7d2a: the packet fixture must include a supported headless entry.
[void][IO.Directory]::CreateDirectory((Join-Path $source 'app'))
[IO.File]::WriteAllText((Join-Path $source 'app\label_match_relay_entry.py'), '')
Product $source '--remove-current-user-setup' -LogicalAppRoot $install
$actual=[IO.File]::ReadAllLines($capture)
if($actual.Count -ne 6 -or $actual[2] -cne (Join-Path $source 'app\label_match_relay_entry.py') -or $actual[3] -cne '--remove-current-user-setup' -or $actual[5] -cne $install){throw 'Packet source was used as logical install root.'}
'PASS'
""", engine=engine, values={'LM_ROLLBACK_ROOT': str(tmp_path)})
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('cut', ('remove', 'run', 'task', 'custody', 'generation_before', 'generation_after', 'uac_cancel'))
def test_actual_install_rollback_attempts_later_restoration_and_records_failure(tmp_path, engine, cut):
    names = ['Get-LabelLaunchFailure', 'Protect-LabelFailureText', 'Save-LabelRollbackContext',
             'Invoke-LabelCanonicalRollback', 'Invoke-LabelGenerationRefusalRecovery']
    if 'function Invoke-LabelRollbackStep' in INSTALLER.read_text(encoding='utf-8'):
        names.append('Invoke-LabelRollbackStep')
    names.extend(['ByteSha', 'InvokeFrozenPlacementHelper'])
    if 'function Assert-LabelRollbackGeneration' in INSTALLER.read_text(encoding='utf-8'):
        names.append('Assert-LabelRollbackGeneration')
    result = run_functions(tmp_path, INSTALLER, names + ['Get-LabelLaunchFailure'], ". '" + str((INSTALLER.parent / "tools/failure_redaction.ps1")).replace("'", "''") + "'\n" + r"""
function Wait-LabelRollbackRelayReady { return $true }

$root=$env:LM_ROLLBACK_ROOT
. (Join-Path (Split-Path -Parent $env:LM_CONTRACT_SOURCE) 'kmtech_shared/powershell/data_generation.ps1')
$generationSubjectSid=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$generationMarkerPaths=@(Join-Path $root 'data-generation.json')
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
$script:generationChecks=0
if ($env:LM_ROLLBACK_CUT -like 'generation_*') {
 function Assert-LabelRollbackGeneration {
  $script:generationChecks++
  if (($env:LM_ROLLBACK_CUT -ceq 'generation_before' -and $script:generationChecks -eq 1) -or
      ($env:LM_ROLLBACK_CUT -ceq 'generation_after' -and $script:generationChecks -eq 2)) {
   throw 'injected generation refusal'
  }
 }
}
function Same {param($A,$B) return [StringComparer]::OrdinalIgnoreCase.Equals([IO.Path]::GetFullPath($A),[IO.Path]::GetFullPath($B))}
function Set-LabelWriterFenceDelegation {param($ControlRoot,$Status,$SessionId,$AttemptId,$ReplacementTransactionId,$DelegationToken,$DelegatedSources,$LifetimeSeconds)
 if($env:LM_ROLLBACK_CUT -eq 'custody'){throw 'injected custody failure'}
}
function SetWriterDelegationEnvironment {param($a,$b,$c,$d)}
function Stop-LabelRollbackCandidateRelay { }
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
function Assert-LabelRollbackTaskPreimage {if($env:LM_ROLLBACK_CUT -eq 'task'){throw 'Injected task preimage mismatch'}}
function InvokeFrozenIntegrityProbe {throw 'Candidate consistency is unproven in this legacy failure fixture'}
function Save {param($Path,$Value) [IO.File]::WriteAllText($Path,($Value|ConvertTo-Json -Depth 20))}
function Full {param($Path,$Name) return $Path}
$ast=[Management.Automation.Language.Parser]::ParseFile($env:LM_CONTRACT_SOURCE,[ref]$tokens,[ref]$errors)
$catches=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.CatchClauseAst] -and
 $n.Body.Extent.Text.Contains('$original = $_')},$true))
if($catches.Count -ne 1){throw 'Actual transaction rollback catch is ambiguous.'}
$body=$catches[0].Body.Extent.Text
$body=$body.Substring(1,$body.Length-2)
try {
 if($env:LM_ROLLBACK_CUT -ceq 'uac_cancel'){
  $frozen=[pscustomobject]@{root=$root;helper_path=(Join-Path $root 'helper.ps1');helper_sha256=('a'*64);
   integrity_sha256=('b'*64);writer_fence_path=(Join-Path $root 'writer.ps1');writer_fence_sha256=('c'*64);
   data_generation_path=(Join-Path $root 'generation.ps1');data_generation_sha256=('d'*64)}
  function Start-Process {param($FilePath,$Verb,$ArgumentList,$WindowStyle,[switch]$Wait,[switch]$PassThru)
   throw [ComponentModel.Win32Exception]::new(1223)
  }
  [void](InvokeFrozenPlacementHelper $frozen @{ElevationLogPath=(Join-Path $root 'placement')})
  throw 'UAC cancellation was not raised.'
 }
 throw 'original forward failure'
} catch {try{. ([scriptblock]::Create($body))}catch{}}
if(-not $events.Contains('run-readback')){throw 'Final rollback observation skipped.'}
if($env:LM_ROLLBACK_CUT -like 'generation_*') {
 if($events.Contains('remove') -or $events.Contains('run') -or $events.Contains('task') -or
    $events.Contains('fence-release')){throw 'Mutation continued after a generation refusal.'}
 $expectedChecks=if($env:LM_ROLLBACK_CUT -ceq 'generation_before'){1}else{2}
 if($script:generationChecks -ne $expectedChecks){throw 'Wrong generation boundary reached.'}
 if($audit.status -cne 'ROLLBACK_FAILED'){throw 'Generation refusal reported rollback success.'}
 'PASS';return
}
if($env:LM_ROLLBACK_CUT -ceq 'uac_cancel'){
 if($audit.status -cne 'FAILED_ROLLED_BACK' -or $audit.rollback.runtime_restored -ne $true){throw 'Cancellation rollback changed.'}
 if(-not $events.Contains('run') -or -not $events.Contains('task') -or -not $events.Contains('fence-release')){throw 'Cancellation skipped restoration.'}
 if(@($audit.rollback.step_failures).Count -ne 0){throw 'Cancellation rollback acquired a new failure.'}
 if(@(Get-ChildItem -LiteralPath $root -Filter 'placement-child-*').Count){throw 'Cancellation retained private transport.'}
 'PASS';return
}
if($env:LM_ROLLBACK_CUT -eq 'custody'){
 if(-not $events.Contains('run') -or -not $events.Contains('task')){throw 'Independent preimage restoration was skipped after custody failure.'}
}elseif(-not $events.Contains('run') -or -not $events.Contains('task')){throw 'Later independent rollback steps were skipped.'}
if($audit.status -cne 'ROLLBACK_FAILED'){throw 'Incomplete rollback reported success.'}
if(@($audit.rollback.step_failures).Count -eq 0){throw 'Original rollback step failures absent.'}
if($env:LM_ROLLBACK_CUT -in @('task','custody') -and $events.Contains('fence-release')){throw 'Unproven recovery released the protective fence.'}
if($env:LM_ROLLBACK_CUT -in @('remove','run') -and -not $events.Contains('fence-release')){throw 'Proven recovery skipped fence release because an earlier phase failed.'}
'PASS'
""", engine=engine, values={'LM_ROLLBACK_ROOT': str(tmp_path), 'LM_ROLLBACK_CUT': cut})
    assert result.returncode == 0, result.stdout + result.stderr

"""Execute installer restoration and real G in isolated PowerShell children."""
from pathlib import Path

import pytest

from tests.test_shared_ancestor_protection import run_functions

ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1'
NAMES = ['Protect-LabelFailureText', 'Get-LabelLaunchFailure', 'Invoke-LabelRollbackStep',
         'Invoke-LabelCanonicalRollback', 'Invoke-LabelGenerationRefusalRecovery',
         'Get-LabelPortableDataVersion', 'Assert-LabelRollbackGeneration', 'Assert-LabelDataDowngrade',
         'RestoreScheduledTask', 'Assert-LabelRollbackTaskPreimage', 'TextSha', 'ByteSha',
         'Test-RelayPersistentRetry', 'Get-RequiredExternalBoolean', 'Save-LabelRollbackContext',
         'Invoke-LabelResumeRollback']


@pytest.mark.parametrize('engine', ['powershell.exe', 'pwsh.exe'])
@pytest.mark.parametrize('case', ['onboarding', 'early', 'unproven', 'code_failed', 'task_failed',
                                 'startup_failed', 'g_ready', 'g_unready', 'resume', 'resume_unproven',
                                 'released_allow', 'released_refuse'])
def test_exact_preimages_and_safe_resume_with_real_generation(tmp_path, engine, case):
    result = run_functions(tmp_path, INSTALLER, NAMES, r'''
$root=$env:LM_RECOVERY_ROOT;$case=$env:LM_RECOVERY_CASE
. (Join-Path $env:LM_RECOVERY_REPO 'kmtech_shared/powershell/data_generation.ps1')
$source=$env:LM_RECOVERY_REPO;$sourceCommit='a'*40;$inventorySha='b'*64
$sourceManifest=[pscustomobject]@{version='v2.0.95';source_commit=$sourceCommit}
$install=Join-Path $root 'current';New-Item -ItemType Directory -Path (Join-Path $install 'runtime') | Out-Null
[IO.File]::WriteAllBytes((Join-Path $install 'runtime/pythonw.exe'),[byte[]]@())
$lad=$root;$localAuditRoot=Join-Path $root 'KMTech/Label_Match/install-audit'
New-Item -ItemType Directory -Path $localAuditRoot | Out-Null
$auditPath=Join-Path $localAuditRoot 'canonical-portable-isolated.json';$runId='isolated';$EvidencePath=''
$stop=Join-Path $root 'stop.json';$removalPath=Join-Path $root 'removal.json';$relayPath=Join-Path $root 'relay.json'
$generationSubjectSid=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$generationMarkerPaths=@((Join-Path $root 'data-generation.json'))
if($case -like 'g_*' -or $case -ceq 'released_refuse') {
 [void](Write-KmtechDataGeneration -MarkerPath $generationMarkerPaths[0] -Version 'v2.0.95' -AppId 'Label_Match' -SubjectSid $generationSubjectSid -BuildId $sourceCommit)
}
$writerFenceControlRoot=Join-Path $root 'control';$writerSessionId='c'*32;$writerAttemptId='d'*32;$writerTransactionId='e'*32
$writerStartedAt=[DateTime]::UtcNow.ToString('o');$writerAuthority=$null;$writerFenceStarted=$true
$writerDelegationToken=$null;$writerContractSha256='f'*64
$writerOrchestratorSha256=(Get-FileHash -LiteralPath (Join-Path $source 'INSTALL_CANONICAL_PORTABLE.ps1') -Algorithm SHA256).Hash.ToLowerInvariant()
$WriterDelegationEnvironmentNames=@();$writerEnvironmentBefore=@();$rollbackSources=@();$restoreSources=@()
$startupLifecycle=$null;$legacyFirstRegistration=$false;$existingVerified=$false;$healthyRemovedInstall=$false
$firstRegistrationStateVerified=$false;$firstRegistrationStarted=$false;$firstRegistrationAttempt=$null
$firstRegistrationAttemptPath='';$firstRegistrationStatePaths=@();$firstRegistrationAbsentAncestors=@()
$replacementRollbackRoot='';$installedPreimageInventory=$null;$receiptSource=[pscustomobject]@{sha256=$inventorySha;critical_file_sha256=@{}}
$writerTransition=[pscustomobject]@{installed_inventory_sha256=$inventorySha}
$before=[pscustomobject]@{exists=$true;kind='ExpandString';data='legacy-relay-command %LOCALAPPDATA%'}
$wanted='candidate-command';$script:runState=[pscustomobject]@{exists=$true;kind='String';data=$wanted}
$legacyXml='<Task xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task"><Triggers><TimeTrigger><Repetition><Interval>PT1M</Interval></Repetition></TimeTrigger></Triggers><Actions><Exec><Command>wscript.exe</Command><Arguments>"legacy/bin/run_direct-sync-relay-label-match.vbs"</Arguments></Exec></Actions></Task>'
$taskBackup=Join-Path $root 'task-preimage.xml';[IO.File]::WriteAllText($taskBackup,$legacyXml,[Text.UTF8Encoding]::new($false))
$taskBefore=[pscustomobject]@{exists=$true;enabled=$true;backup_path=$taskBackup;xml_sha256=(TextSha $legacyXml)}
$script:taskXml='candidate-task';$script:taskEnabled=$false;$CanonicalTaskName='direct-sync-relay-label-match'
$stopBefore=[pscustomobject]@{exists=$false};$healthyRemovalReportBefore=$null;$removalReportExisted=$false
$old=@([pscustomobject]@{ExecutablePath=(Join-Path $root 'old-runtime.exe');CommandLine='legacy-relay-command'})
$helperParameters=@{};$frozenPlacement=[pscustomobject]@{root=$root};$mutated=$true
$audit=[ordered]@{schema='label-match-canonical-portable-install-v1';status='ROLLBACK_FAILED';run_id=$runId;
 install_root=$install;source_commit=$sourceCommit;elevation_log_path=(Join-Path $root 'elevated.jsonl');
 startup_update_preimage=[ordered]@{path=(Join-Path $root '.startup-update-install');sha256=$inventorySha};
 rollback=[ordered]@{};writer_fence=[ordered]@{status='RESTORING';session_id=$writerSessionId;
 attempt_id=$writerAttemptId;replacement_transaction_id=$writerTransactionId;session_started_at_utc=$writerStartedAt}}
$script:events=[Collections.Generic.List[string]]::new();$script:liveRelays=@();$script:held=$true
function Full {param($Path,$Purpose) [IO.Path]::GetFullPath($Path)}
function Same {param($A,$B) [StringComparer]::OrdinalIgnoreCase.Equals([IO.Path]::GetFullPath($A),[IO.Path]::GetFullPath($B))}
function Sha {param($Path) (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()}
function Save {param($Path,$Value) [IO.File]::WriteAllText($Path,($Value|ConvertTo-Json -Depth 30),[Text.UTF8Encoding]::new($false))}
function Snapshot {return $script:runState}
function Restore {param($State) $script:events.Add('run');$script:runState=$State}
function Get-ScheduledTask { [pscustomobject]@{TaskName=$CanonicalTaskName;TaskPath='\';Settings=[pscustomobject]@{Enabled=$script:taskEnabled}} }
function Export-ScheduledTask {param($TaskName,$TaskPath) return $script:taskXml}
function Register-ScheduledTask {param($TaskName,$TaskPath,$Xml,[switch]$Force)
 $script:events.Add('task');if($case -ceq 'task_failed'){throw 'Injected task failure'};$script:taskXml=$Xml
}
function Enable-ScheduledTask {param($TaskName,$TaskPath) $script:taskEnabled=$true}
function Disable-ScheduledTask {param($TaskName,$TaskPath) $script:taskEnabled=$false}
function Product {param($Root,$Mode,$ExtraArguments,$LogicalAppRoot)
 $script:events.Add('remove');if($case -ceq 'early'){throw 'Injected removal failure'}
}
function Manifest {param($Root,$Skip) return $sourceManifest}
function PortableInventory {param($Root) [pscustomobject]@{sha256=$inventorySha}}
function InvokeFrozenIntegrityProbe {param($Frozen,$Root) if(-not (Test-Path -LiteralPath $Root)){throw 'Tree absent'} }
function Invoke-LabelDelegatedPlacementHelper {param($Frozen,$Parameters,$Authority)
 $script:events.Add('code');if($case -ceq 'code_failed'){throw 'Injected code failure'}
 Remove-Item -LiteralPath $install -Recurse;return 0
}
function InvokeFrozenPlacementHelper {param($Frozen,$Parameters)
 $script:events.Add('startup');if(Test-Path -LiteralPath $install){throw 'Startup restore still requires current'}
 if($case -ceq 'startup_failed'){return 1};return 0
}
function Assert-RollbackRelayPreimage {param($ExpectedRelays)
 $script:events.Add('relay-readback')
 if($case -in @('unproven','resume_unproven') -and @($ExpectedRelays).Count -eq 0){throw 'Writer absence is unproven'}
 if(@($ExpectedRelays).Count -ne @($script:liveRelays).Count){throw 'Relay count differs'}
}
function Set-LabelWriterFenceDelegation { }
function Stop-LabelRollbackCandidateRelay {
 $script:events.Add('protect-stop');$script:liveRelays=@()
 [IO.File]::WriteAllText((Join-Path $root 'stop-proof.txt'),'ABSENT')
}
function SetWriterDelegationEnvironment { }
function Set-LabelWriterFenceInstalledTree { }
function Enter-LabelWriterAdmission {return 1}
function Exit-LabelWriterAdmission { }
function Enter-LabelWriterSessionAuthority {return 1}
function Exit-LabelWriterSessionAuthority { }
function RestoreWriterDelegationEnvironment { }
function Stop-LabelWriterFence {$script:events.Add('release');$script:held=$false;[pscustomobject]@{writer_inventory_sha256=$inventorySha}}
function Start-LabelWriterFence {$script:events.Add('acquire');$script:held=$true}
function StartRaw {param($CommandLine) $script:events.Add('relay');$script:liveRelays=$old;return 123}
function Get-CimInstance { [pscustomobject]@{ExecutablePath=$old[0].ExecutablePath} }
function Start-Sleep { }
function HealthyLifecycle {param($Source,$Install) if($case -cne 'g_ready'){throw 'Identity is unproven'};[pscustomobject]@{status='HEALTHY_CURRENT_USER';marker_present=$false}}
Save $relayPath ([ordered]@{status='READY';persistent_retry=$true})
if($case -like 'resume*') {
 Save-LabelRollbackContext;Save $auditPath $audit
 $ResumeRollbackAuditPath=$auditPath;$ExpectedRollbackAuditSha256=Sha $auditPath;$testMode=$true;$PlanOnly=$false
 $empty=Join-Path $root 'empty.ps1';[IO.File]::WriteAllText($empty,'')
 function FreezePlacementHelper {param($Source,$Audit,$RunId,$Critical,$Origin) [pscustomobject]@{root=$root;writer_fence_path=$empty;writer_fence_sha256=$writerContractSha256;helper_sha256=$inventorySha;helper_path=$empty} }
 function ReceiptSource {param($Source,$Manifest) return $receiptSource}
 function Read-LabelWriterFence {return [pscustomobject]@{session_id=$writerSessionId;attempt_id=$writerAttemptId;
  replacement_transaction_id=$writerTransactionId;orchestrator_sha256=$writerOrchestratorSha256;writer_contract_sha256=$writerContractSha256}}
 try{. Invoke-LabelResumeRollback}catch{if($case -ceq 'resume'){throw}}
} else {
 if($case -like 'released_*'){
  $writerFenceStarted=$false;$script:held=$false
  $audit.writer_fence.status='RELEASED_AFTER_RESTORE'
  $badPath=Join-Path $root 'final-audit-failed';New-Item -ItemType Directory -Path $badPath | Out-Null
  $ast=[Management.Automation.Language.Parser]::ParseFile($env:LM_CONTRACT_SOURCE,[ref]$tokens,[ref]$errors)
  $catch=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.CatchClauseAst] -and $n.Body.Extent.Text.Contains('$original = $_')},$true))
  if($catch.Count -ne 1){throw 'Actual transaction catch is ambiguous'}
  $body=$catch[0].Body.Extent.Text
  try{[IO.File]::WriteAllText($badPath,'final audit')}catch{try{. ([scriptblock]::Create($body.Substring(1,$body.Length-2)))}catch{}}
  if(-not $script:events.Contains('acquire') -or -not $script:events.Contains('protect-stop')){throw 'Released fence protection was skipped'}
 }else{try{. Invoke-LabelCanonicalRollback}catch{}}
}
if($case -in @('onboarding','early','resume','released_allow')) {
 if($script:held -or -not $script:events.Contains('release') -or -not $script:events.Contains('relay')){throw 'Proven restoration did not resume old relay'}
 if($script:runState.data -cne $before.data -or $script:runState.kind -cne $before.kind -or $script:taskXml -cne $legacyXml){throw 'Autostart bytes differ'}
 if(-not $script:events.Contains('startup') -or (Test-Path -LiteralPath $install)){throw 'Absent code/startup restoration failed'}
 if($case -ceq 'early' -and @($audit.rollback.step_failures).Count -eq 0){throw 'Earlier failure was discarded'}
} elseif($case -like 'g_*' -or $case -ceq 'released_refuse') {
 if($script:events.Contains('code') -or $script:events.Contains('task') -or $script:events.Contains('relay') -or -not (Test-Path -LiteralPath $install)){throw 'G refusal selected old code/persistence'}
 if($case -ceq 'g_ready') {
  if($script:held -or $script:runState.data -cne $wanted -or $audit.rollback.retained_candidate_ready -ne $true){throw 'Consistent new candidate was not retained'}
 } elseif(-not $script:held -or $script:runState.exists -or -not $audit.rollback.guidance){throw 'Unproven new candidate was not held with guidance'}
} else {
 if(-not $script:held -or $script:events.Contains('release') -or $script:events.Contains('relay') -or -not $audit.rollback.guidance){throw 'Unproven restoration released fence'}
 if(-not $script:events.Contains('run') -or -not $script:events.Contains('task') -or -not $script:events.Contains('startup')){throw 'Independent autostart restoration was skipped'}
}
$phases=@($audit.rollback.steps | ForEach-Object phase)
if($case -notlike 'g_*' -and $case -cne 'released_refuse' -and @($phases | Where-Object {$_ -ceq 'user_preimage_admission'}).Count -ne 1){throw 'Nested parent phase was lost'}
if(@($phases | Where-Object {$_ -ceq 'run_restore'}).Count -gt 1){throw 'Nested child phase was duplicated'}
'PASS'
''', engine=engine, values={'LM_RECOVERY_ROOT': str(tmp_path), 'LM_RECOVERY_CASE': case,
                            'LM_RECOVERY_REPO': str(ROOT)})
    assert result.returncode == 0, result.stdout + result.stderr

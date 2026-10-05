param([string]$Repo='C:/KMTech/w9setupprepsub260/candidate',
      [Parameter(Mandatory)][string]$Root,
      [Parameter(Mandatory)][string]$Output,
      [ValidateSet('success','early_failure','runtime_unproven','g_refusal')][string]$Case='early_failure')
$ErrorActionPreference='Stop'
# Keep every extracted function name together for sub-274.
$ExtractedFunctionNames=@('Invoke-LabelRollbackStep','Get-LabelLaunchFailure',
    'Get-LabelPortableDataVersion','Assert-LabelDataDowngrade','Assert-LabelRollbackGeneration',
    'Invoke-LabelCanonicalRollback','Save-LabelRollbackContext',
    'Stop-LabelRollbackCandidateRelay','Invoke-LabelGenerationRefusalRecovery',
    'Assert-LabelRollbackTaskPreimage','Get-LabelCurrentUserScheduledTasks',
    'Protect-LabelFailureText')
New-Item -ItemType Directory -Path $Root,(Join-Path $Root 'installed'),(Join-Path $Root 'control') | Out-Null
New-Item -ItemType Directory -Path (Join-Path $Root 'installed/runtime') | Out-Null
# Membership placeholder only; Product is a native-free stand-in and never executes it.
[IO.File]::WriteAllBytes((Join-Path $Root 'installed/runtime/pythonw.exe'),[byte[]]@())
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile((Join-Path $Repo 'INSTALL_CANONICAL_PORTABLE.ps1'),[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'Source parse failed'}
$functions=@{}
foreach($node in $ast.FindAll({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst]},$true)){
    $functions[$node.Name]=$node.Extent.Text
}
foreach($name in @($ExtractedFunctionNames | Where-Object { $_ -cne 'Assert-LabelRollbackGeneration' })){
    . ([scriptblock]::Create($functions[$name]))
}
$gDefinition=$functions['Assert-LabelRollbackGeneration'].Replace('function Assert-LabelRollbackGeneration','function Invoke-ActualLabelRollbackGeneration')
. ([scriptblock]::Create($gDefinition))
# Use the unmodified shared marker reader and downgrade policy.
. (Join-Path $Repo 'kmtech_shared/powershell/data_generation.ps1')
$script:GCalls=0
function Assert-LabelRollbackGeneration {
    $script:GCalls++
    Invoke-ActualLabelRollbackGeneration
}
$script:NativeCalls=[ordered]@{product=0;run_restore=0;task_restore=0;fence_release=0;relay_restart=0;authority_release=0;inventory=0}
# Only native installation, process, task, Run, and mutex effects are stand-ins.
# Generation policy, rollback step collection, and the complete catch/finally are real source.
function Manifest { param($Path,$Skip) return [pscustomobject]@{version='v2.0.95';source_commit=$sourceCommit} }
function PortableInventory { param($Path) $script:NativeCalls.inventory++;return [pscustomobject]@{sha256=$inventorySha;file_count=1;byte_count=1} }
function Same { param($a,$b) return [IO.Path]::GetFullPath($a) -ceq [IO.Path]::GetFullPath($b) }
function InvokeFrozenIntegrityProbe { param($Frozen,$Path) if(-not (Test-Path -LiteralPath $Path -PathType Container)){throw 'Isolated tree absent'} }
function Set-LabelWriterFenceDelegation { }
function SetWriterDelegationEnvironment { }
function Set-LabelWriterFenceInstalledTree { }
function Assert-RollbackRelayPreimage { param([object[]]$ExpectedRelays) return $true }
function Product { $script:NativeCalls.product++;if($Case -ceq 'early_failure'){throw 'Injected isolated current-user removal failure'} }
function Enter-LabelWriterAdmission { return [pscustomobject]@{isolated=$true} }
function Exit-LabelWriterAdmission { }
function Restore { param($Before) $script:NativeCalls.run_restore++ }
function RestoreScheduledTask { param($Before) $script:NativeCalls.task_restore++ }
function Snapshot {
    if($Case -ceq 'runtime_unproven'){return [pscustomobject]@{exists=$true;kind='String';data='unproven-run-command'}}
    return $before
}
function Stop-LabelWriterFence { $script:NativeCalls.fence_release++;return [pscustomobject]@{writer_inventory_sha256=$inventorySha} }
function StartRaw { $script:NativeCalls.relay_restart++;return 1 }
function Get-CimInstance { return [pscustomobject]@{ExecutablePath=(Join-Path $Root 'prior-runtime.exe')} }
function Start-Sleep { }
function Start-Process { throw 'R4 native process execution prohibited' }
function Get-ScheduledTask { throw 'R4 real scheduler access prohibited' }
function Export-ScheduledTask { throw 'R4 real scheduler access prohibited' }
function Register-ScheduledTask { throw 'R4 real scheduler access prohibited' }
function Unregister-ScheduledTask { throw 'R4 real scheduler access prohibited' }
function RestoreWriterDelegationEnvironment { }
function Exit-LabelWriterSessionAuthority { $script:NativeCalls.authority_release++ }
function Save { param($Path,$Value) $Value | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath $Path -Encoding UTF8 }

$sourceCommit='a'*40
$inventorySha='a'*64
$startupLifecycle=$null;$mutated=$true;$legacyFirstRegistration=$false
$existingVerified=$true;$healthyRemovedInstall=$false
$install=Join-Path $Root 'installed';$source=$Repo;$replacementRollbackRoot=$install
$writerFenceStarted=$true;$writerAuthority=[pscustomobject]@{isolated=$true}
$writerFenceControlRoot=Join-Path $Root 'control'
$writerSessionId=[guid]::NewGuid().ToString();$writerAttemptId=[guid]::NewGuid().ToString()
$writerTransactionId=[guid]::NewGuid().ToString();$writerDelegationToken=$null
$writerOrchestratorSha256=$inventorySha;$writerContractSha256=$inventorySha
$writerStartedAt=(Get-Date).ToUniversalTime().ToString('o');$rollbackSources=@()
$generationSubjectSid='S-1-5-18'
$generationMarkerPaths=@((Join-Path $Root 'data-generation.json'))
if($Case -ceq 'g_refusal'){
    [void](Write-KmtechDataGeneration -MarkerPath $generationMarkerPaths[0] -Version 'v2.0.96' `
        -AppId 'Label_Match' -SubjectSid $generationSubjectSid -BuildId ('b'*40))
}
$installedPreimageInventory=[pscustomobject]@{sha256=$inventorySha}
$receiptSource=[pscustomobject]@{sha256=$inventorySha}
$before=[pscustomobject]@{exists=$true;kind='String';data='isolated-prior-command'}
$taskBefore=[pscustomobject]@{exists=$false}
$stopBefore=[pscustomobject]@{exists=$false};$stop=Join-Path $Root 'stop.json'
$removalPath=Join-Path $Root 'removal.json';$removalReportExisted=$false
$healthyRemovalReportBefore=$null;$localAuditRoot=$Root
$old=@([pscustomobject]@{CommandLine='isolated-prior-command';ExecutablePath=(Join-Path $Root 'prior-runtime.exe')})
$audit=[ordered]@{status='RUNNING';rollback=[ordered]@{};writer_fence=[ordered]@{status='RESTORING'}}
$auditPath=Join-Path $Root 'audit.json';$EvidencePath=$null;$writerEnvironmentBefore=$null
$transaction=@($ast.FindAll({param($n)
    $n -is [Management.Automation.Language.TryStatementAst] -and
    @($n.CatchClauses | Where-Object {$_.Body.Extent.Text.Contains('$Script:LabelRollbackErrors =')}).Count -gt 0
},$true))
if($transaction.Count -ne 1){throw 'Unique product transaction not found'}
$catchBody=$transaction[0].CatchClauses[0].Body.Extent.Text
$finallyBody=$transaction[0].Finally.Extent.Text
$execute=[scriptblock]::Create("try {throw 'Injected isolated forward failure'} catch $catchBody finally $finallyBody")
$failed=$false
try{. $execute}catch{$failed=$true;$failureMessage=[string]$_.Exception.Message}
$steps=@($audit.rollback.steps)
$failedSteps=@($steps | Where-Object status -ceq 'FAILED' | ForEach-Object phase)
$record=[ordered]@{source_commit=$sourceCommit;case=$Case;g_calls=$script:GCalls;
    failed=$failed;status=$audit.status;writer_fence_still_started=$writerFenceStarted;
    native_effects='isolated stand-ins only';counts=$script:NativeCalls;failed_steps=$failedSteps;
    phases=@($steps | ForEach-Object phase);code_restored=$audit.rollback.code_restored;
    original_failure_preserved=([string]$audit.failure_details.phase -ceq 'forward');
    guidance_present=(([string]$audit.rollback.guidance+' '+[string]$failureMessage) -match '[가-힣]');
    errors=@($Script:LabelRollbackFailures | ForEach-Object phase);
    real_generation_helper=$true;real_product_catch_and_finally=$true}
$record | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $Output -Encoding UTF8

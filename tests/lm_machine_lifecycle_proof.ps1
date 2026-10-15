param([string]$Repo,[string]$Root,[string]$Python,[ValidateSet('proof','channels','rollback','rollback_failure','rollback_fence_failure')][string]$Case)
$ErrorActionPreference='Stop'
Set-StrictMode -Version Latest
$WorkerSid=''
trap {
    [IO.File]::WriteAllText((Join-Path $Root 'failure-site.json'),(@{type=$_.Exception.GetType().Name;line=$_.InvocationInfo.ScriptLineNumber;locations=@($_.ScriptStackTrace -split '\r?\n'|ForEach-Object{if($_ -match ': line (\d+)'){$Matches[1]}})}|ConvertTo-Json -Compress))
    exit 1
}
function Import-Functions([string]$Path,[string[]]$Names){
    $tokens=$null;$errors=$null
    $ast=[Management.Automation.Language.Parser]::ParseFile($Path,[ref]$tokens,[ref]$errors)
    if($errors.Count){throw 'SOURCE_PARSE_FAILED'}
    foreach($name in $Names){
        $node=$ast.Find({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $name},$true)
        if($null -eq $node){throw 'SOURCE_FUNCTION_MISSING'}
        . ([scriptblock]::Create($node.Extent.Text.Replace(('function '+$name),('function global:'+$name))))
    }
}
$thisPc=Join-Path $Repo 'INSTALL_THIS_PC.ps1';$canonical=Join-Path $Repo 'INSTALL_CANONICAL_PORTABLE.ps1'
Import-Functions $thisPc @('Get-EarlyFileSha256','Get-FileSha256','Save-LabelLifecycleChannel','Invoke-LabelPlacementLifecycleProcess','Throw-LabelPlacementLifecycleFailure','Write-LabelProtectedReusedProof','Get-CodeInventory','Get-InventoryAggregate')
Import-Functions $canonical @('Read-LabelProtectedReusedProof','Invoke-LabelMachinePlacementRollback','ByteSha','Invoke-LabelPinnedAtomicReplace','ConvertTo-LabelPlacementPublicFailure')
. (Join-Path $Repo 'tools/bootstrap_integrity.ps1') -SharedCodeRoot $Repo
. (Join-Path $Repo 'tools/failure_redaction.ps1')
function Assert-NoReparsePoint([string]$Path){if(Test-Path -LiteralPath $Path){if([IO.File]::GetAttributes($Path) -band [IO.FileAttributes]::ReparsePoint){throw 'REPARSE'}}}
# Only host privileged ACL observations are facades. File channels, hashes,
# inventories, native child rc/birth and the production proof consumers are real.
$script:protected=0
function Protect-LabelInstallerOwnerFile {$script:protected++}
function Assert-ProtectedCodeAncestors {}
function Assert-LabelReusedProofProtection {Assert-NoReparsePoint $args[0]}
function Assert-BootstrapIntegrityRecord {return @{status='PASS'}}
function Sha([string]$Path){Get-FileSha256 $Path}
function Same([string]$Left,[string]$Right){[StringComparer]::OrdinalIgnoreCase.Equals([IO.Path]::GetFullPath($Left),[IO.Path]::GetFullPath($Right))}
$WriterFenceControlRoot=Join-Path $Root 'machine-fence';[void][IO.Directory]::CreateDirectory($WriterFenceControlRoot)
if($Case -ceq 'channels'){
    $public=ConvertTo-LabelPlacementPublicFailure ([pscustomobject]@{schema='private';phase='placement';hresult=7;full_exception_trace=@{path=(Join-Path $Root 'private-report');sha256=('e'*64)};full_exception_trace_text='private';placement_code='PLACEMENT_FAILED';exit_code=7;exception_type='InvalidOperationException'})
    foreach($key in @('schema','phase','hresult','full_exception_trace','full_exception_trace_text')){if($public.Contains($key)){throw 'PRIVATE_CHILD_FIELD_PUBLISHED'}}
    if($public.exit_code -ne 7 -or $public.stage -cne 'SUPPORT'){throw 'PUBLIC_CHILD_RESULT_LOST'}
    $child=Join-Path $Root 'channel-child.py'
    [IO.File]::WriteAllText($child,"import sys`nprint('{`"result`":`"separate_stdout`"}')`nprint('LABEL_MATCH_LIFECYCLE_FAILURE={`"status`":`"FAILED`",`"stage`":`"SUPPORT`",`"reason_code`":`"INSTALLER_FAILED`"}',file=sys.stderr)`nsys.exit(7)`n")
    $lifecycleAuthorityArguments=@()
    $output=@(Invoke-LabelPlacementLifecycleProcess $Python @('-B',$child))
    if($LASTEXITCODE -ne 7 -or $output.Count -ne 1 -or ($output[0]|ConvertFrom-Json).result -cne 'separate_stdout'){throw 'NATIVE_CHANNEL_RESULT_DIFFERS'}
    if($Script:LabelPlacementLifecycleFailure.exit_code -ne 7 -or $Script:LabelPlacementLifecycleFailure.reason_code -cne 'INSTALLER_FAILED'){throw 'NATIVE_FAILURE_LOST'}
    foreach($channel in @('stdout','stderr')){
        $path=$Script:LabelLifecycleChannels[$channel+'_path']
        if((Sha $path) -cne $Script:LabelLifecycleChannels[$channel+'_sha256']){throw 'CHANNEL_HASH_DIFFERS'}
    }
    if($script:protected -ne 2){throw 'CHANNEL_PROTECTION_SKIPPED'}
    try{Throw-LabelPlacementLifecycleFailure 'SUPPORTED_ACTIVATION_FAILED'}catch{
        if(-not $_.Exception.Data.Contains('child_output') -or -not $_.Exception.Data.Contains('lifecycle_channels')){throw 'CHANNEL_REFERENCE_LOST'}
    }
}elseif($Case -ceq 'proof'){
    $sourceRootFull=Join-Path $Root 'source';$installRootFull=Join-Path $Root 'machine/current';$placement=Join-Path $Root 'machine/versions/candidate'
    $IntegrityFileName='bootstrap-integrity.json'
    foreach($folder in @($sourceRootFull,$installRootFull,$placement)){
        [void][IO.Directory]::CreateDirectory((Join-Path $folder 'app'))
        foreach($name in @('portable-manifest.json','app/update-manifest-key-config.json','app/auto-update-settings.json','INSTALL_CANONICAL_PORTABLE.ps1')){[IO.File]::WriteAllText((Join-Path $folder $name),'{}')}
    }
    $nextCommit='a'*40;$ExpectedActiveCommit=$nextCommit;$supportedContext=[pscustomobject]@{from_commit=$nextCommit}
    $sourceAggregate=Get-InventoryAggregate @(Get-CodeInventory $sourceRootFull)
    $ExpectedSourceFileCount=@(Get-CodeInventory $sourceRootFull).Count;$ExpectedSourceByteCount=8
    $ExpectedBootstrapScriptSha256='b'*64;$InstallerRunId='fixture-run'
    $WriterFenceSessionId='session';$WriterFenceAttemptId='attempt';$WriterFenceReplacementTransactionId='transaction';$WriterFenceDelegationToken='synthetic-placeholder-not-authority'
    $Script:LabelWriterFenceInventorySha256='c'*64
    $fence=[pscustomobject]@{session_id=$WriterFenceSessionId;attempt_id=$WriterFenceAttemptId;replacement_transaction_id=$WriterFenceReplacementTransactionId;
        writer_inventory_sha256=$Script:LabelWriterFenceInventorySha256;orchestrator_sha256=(Sha (Join-Path $sourceRootFull 'INSTALL_CANONICAL_PORTABLE.ps1'));writer_contract_sha256=('d'*64)}
    function Assert-LabelWriterFenceOwner {return $fence}
    Write-LabelProtectedReusedProof $placement
    $process=[Diagnostics.Process]::GetCurrentProcess();try{$birth=$process.StartTime.ToUniversalTime().ToFileTimeUtc().ToString()}finally{$process.Dispose()}
    $session=[pscustomobject]@{last_child_process_id=[int]$PID;last_child_process_started_at_filetime=$birth}
    $parameters=@{InstallRoot=$installRootFull;SourceRoot=$sourceRootFull;InstallerRunId=$InstallerRunId;ExpectedSourceFileCount=$ExpectedSourceFileCount;ExpectedSourceByteCount=$ExpectedSourceByteCount;
        WriterFenceSessionId=$WriterFenceSessionId;WriterFenceAttemptId=$WriterFenceAttemptId;WriterFenceReplacementTransactionId=$WriterFenceReplacementTransactionId}
    $frozen=@{helper_sha256=$ExpectedBootstrapScriptSha256;writer_fence_sha256=$fence.writer_contract_sha256}
    $manifest=@{source_commit=$nextCommit};$receipt=@{bootstrap_aggregate_sha256=$sourceAggregate;sha256=$sourceAggregate}
    function PortableInventory([string]$Path){return @{sha256=(Get-InventoryAggregate @(Get-CodeInventory $Path))}}
    $accepted=Read-LabelProtectedReusedProof $frozen $parameters $manifest $receipt $placement $session
    if($accepted.code_replaced -or $accepted.helper_process_id -ne $PID){throw 'REUSED_PROOF_NOT_ACCEPTED'}
    $path=$accepted.proof_path;$original=[IO.File]::ReadAllText($path)
    foreach($field in @('run_id','helper_process_started_at_filetime','helper_sha256','source_manifest_sha256','session_id','writer_inventory_sha256')){
        $record=$original|ConvertFrom-Json;$record.$field='different';[IO.File]::WriteAllText($path,($record|ConvertTo-Json -Compress))
        $refused=$false;try{[void](Read-LabelProtectedReusedProof $frozen $parameters $manifest $receipt $placement $session)}catch{$refused=$true}
        if(-not $refused){throw 'REUSED_BINDING_DRIFT_ACCEPTED'}
    }
    [IO.File]::WriteAllText($path,$original);[IO.File]::WriteAllText((Join-Path $placement 'app/auto-update-settings.json'),'changed')
    $refused=$false;try{[void](Read-LabelProtectedReusedProof $frozen $parameters $manifest $receipt $placement $session)}catch{$refused=$true}
    if(-not $refused){throw 'REUSED_INSTALLED_DRIFT_ACCEPTED'}
}else{
    $Install=Join-Path $Root 'machine/current';$priorRoot=Join-Path $Root 'machine/versions/old';$Source=Join-Path $Root 'source'
    [void][IO.Directory]::CreateDirectory($priorRoot);[void][IO.Directory]::CreateDirectory($Source)
    $preimage=@{sha256='old-inventory'};$receipt=@{sha256='new-inventory'};$existing=$true
    $Lifecycle=[pscustomobject]@{lifecycle_state='INSTALLED';from_commit=('a'*40)}
    $control=$WriterFenceControlRoot;$session='session';$attempt='attempt';$transaction='transaction';$delegation='synthetic-placeholder';$run='run';$parameters=@{};$frozen=@{};$backupsBefore=@()
    $auditRoot=$Root;$auditFile=Join-Path $Root 'audit.json';$EvidencePath='';$fenced=$true;$authority=@{};$machineFenceReleaseProven=$false
    $workerArtifactsCommitted=$false;$workerArtifacts=@();$workerPendingForPlacedCode=@{candidate='new'};$workerPendingPath=Join-Path $Root 'pending.json'
    $Script:LabelPlacementSession=$null;$script:restores=0;$script:released=0
    $startupDirectory=Join-Path (Split-Path -Parent $Install) ('.startup-update-install.'+$attempt)
    [void][IO.Directory]::CreateDirectory($startupDirectory)
    [IO.File]::WriteAllText((Join-Path $startupDirectory 'preimage.json'),'{}')
    [IO.File]::WriteAllText((Join-Path $Root 'active.json'),'new')
    $audit=[ordered]@{writer_fence=[ordered]@{status='INSTALLING'};rollback=[ordered]@{available=$true;applied=$false;runtime_restored=$false}}
    function Set-LabelWriterFenceDelegation {}
    function InvokeFrozenPlacementHelper { $script:restores++;[IO.File]::WriteAllText((Join-Path $Root 'active.json'),'old');return 0 }
    function InvokeFrozenIntegrityProbe {}
    function PortableInventory {return @{sha256=$(if($Case -ceq 'rollback_failure'){'wrong'}else{'old-inventory'})}}
    function Read-CanonicalStartupLifecycle {if([IO.File]::ReadAllText((Join-Path $Root 'active.json')) -cne 'old'){throw 'NOT_RESTORED'};return @{lifecycle_state='INSTALLED';from_commit=$Lifecycle.from_commit;active_code_root=$priorRoot}}
    function Get-LabelLaunchFailure {param($Record,$Location);return @{exception_type=$Record.Exception.GetType().Name;location_function=$Location}}
    function Write-LabelInstallerRelayRecoveryGuidance {}
    function Save {param($Path,$Record);[IO.File]::WriteAllText($Path,($Record|ConvertTo-Json -Depth 15 -Compress))}
    function Stop-LabelWriterFence {$script:released++;if($Case -ceq 'rollback_fence_failure'){throw [IO.IOException]::new('INJECTED_FENCE_RELEASE_FAILURE')}}
    function Exit-LabelWriterSessionAuthority {}
    $tokens=$null;$errors=$null;$ast=[Management.Automation.Language.Parser]::ParseFile($canonical,[ref]$tokens,[ref]$errors)
    $machine=$ast.Find({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Invoke-LabelMachineWorkerInstall'},$true)
    $transactionTry=$machine.Body.FindAll({param($n)$n -is [Management.Automation.Language.TryStatementAst] -and $null -ne $n.Finally -and $n.CatchClauses.Count},$true)|Select-Object -First 1
    $catchBody=$transactionTry.CatchClauses[0].Body.Extent.Text;$finallyBody=$transactionTry.Finally.Extent.Text
    $first=$null
    try{try{throw 'INJECTED_PLACEMENT_FAILURE'}catch{. ([scriptblock]::Create($catchBody.Substring(1,$catchBody.Length-2)))}}catch{$first=$_}
    . ([scriptblock]::Create($finallyBody.Substring(1,$finallyBody.Length-2)))
    if($null -eq $first -or $first.Exception.Message -cne 'INJECTED_PLACEMENT_FAILURE' -or $script:restores -ne 1 -or -not $audit.rollback.applied -or $audit.rollback.runtime_restored){throw 'ORIGINAL_MACHINE_FAILURE_LOST'}
    if($Case -ceq 'rollback'){
        if(-not $audit.rollback.code_restored -or -not $audit.rollback.autostart_restored -or $script:released -ne 1 -or $null -ne $workerPendingForPlacedCode -or -not $audit.rollback.fence_released){throw 'MACHINE_RESTORE_UNPROVEN'}
        if($audit.rollback.relay_readiness -cne 'NOT_APPLICABLE_MACHINE_WORKER_PENDING'){throw 'MACHINE_RELAY_SCOPE_CHANGED'}
    }elseif($Case -ceq 'rollback_failure'){
        if($audit.status -cne 'ROLLBACK_FAILED' -or $script:released -ne 0 -or $audit.rollback.fence_released){throw 'UNPROVEN_MACHINE_FENCE_RELEASED'}
    }else{
        if($audit.status -cne 'ROLLBACK_FAILED' -or $script:released -ne 1 -or $audit.rollback.fence_released -or $audit.writer_fence.status -cne 'RELEASE_UNPROVEN'){throw 'FENCE_RELEASE_FAILURE_LOST'}
    }
}
'PASS'

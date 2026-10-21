param([string]$Repo,[string]$Seed,[string]$Output)
$ErrorActionPreference='Stop'
Set-StrictMode -Version Latest
$ast=[Management.Automation.Language.Parser]::ParseFile((Join-Path $Repo 'INSTALL_CANONICAL_PORTABLE.ps1'),[ref]$null,[ref]$null)
$names=@('Full','Same','ByteSha','PinnedFileBytes','UInt64BE','HexBytes','PortableInventory','Save','Invoke-LabelResumeRollback')
foreach($node in $ast.FindAll({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst]},$true)) {
    if($node.Name -in $names -or $node.Name -like '*-LabelOrphan*') {
        . ([scriptblock]::Create($node.Extent.Text))
    }
}
. (Join-Path $Repo 'tools/label_writer_fence.ps1')
function Sha([string]$Path){Get-FileHash -LiteralPath $Path -Algorithm SHA256|ForEach-Object {$_.Hash.ToLowerInvariant()}}
function Get-KmtechFileSha([string]$Path){Sha $Path}
function ConvertTo-KmtechPublicDiagnosticTree($Value){return $Value}
function ConvertTo-KmtechRedactedSinkObject($Value){return $Value}
function Assert-LabelOrphanProtectedPath([string]$Path,[string]$MachineRoot=''){}
function Get-LabelOrphanProcessRows { return @() }
function Snapshot {return [pscustomobject]@{exists=$false;kind='';data=''}}
function Get-LabelCurrentUserScheduledTasks {return $null}
function ReceiptSource($Source,$Manifest){return [pscustomobject]@{critical_file_sha256=[pscustomobject]@{writer_fence_helper=(Sha (Join-Path $Source 'tools/label_writer_fence.ps1'))}}}
$script:ReplayCount=0
foreach($name in @('Invoke-LabelCanonicalRollback','FreezePlacementHelper','StartRaw','Start-LabelPlacementSession','RestoreScheduledTask','Restore')) {
    Set-Item -LiteralPath ('function:'+ $name) -Value {$script:ReplayCount++;throw 'ORPHAN_REPLAY_FORBIDDEN'}
}
$jsonArgs=@{}
if((Get-Command ConvertFrom-Json).Parameters.ContainsKey('DateKind')){$jsonArgs.DateKind='String'}
$seeds=Get-Content -LiteralPath $Seed -Raw -Encoding UTF8|ConvertFrom-Json @jsonArgs
$results=@()
foreach($seedCase in $seeds){
    $root=[string]$seedCase.case_root
    $install=[string]$seedCase.audit.install_root
    $source=$Repo
    $sourceManifest=[pscustomobject]@{source_commit=[string]$seedCase.audit.source_commit}
    $lad=Join-Path $root 'local'
    $localAuditRoot=Join-Path $lad 'KMTech/Label_Match/install-audit'
    [void][IO.Directory]::CreateDirectory($localAuditRoot)
    $writerFenceControlRoot=Join-Path $root 'control'
    [void][IO.Directory]::CreateDirectory($writerFenceControlRoot)
    $stop=Join-Path $root 'relay.stop.json'
    $CanonicalTaskName='direct-sync-relay-label-match'
    $testMode=$true;$PlanOnly=$false;$generationSubjectSid='S-1-5-21-1'
    $Script:LabelWriterFenceAdmissionProductionRoot=$writerFenceControlRoot
    $seedCase.audit.code_state='UNKNOWN'
    $seedCase.audit|Add-Member -NotePropertyName writer_transition -NotePropertyValue ([pscustomobject]@{
        installed_portable_inventory_sha256='';candidate_portable_inventory_sha256=(PortableInventory $install).sha256}) -Force
    $ResumeRollbackAuditPath=Join-Path $localAuditRoot ('canonical-portable-'+$seedCase.audit.run_id+'.json')
    Save $ResumeRollbackAuditPath $seedCase.audit
    $ExpectedRollbackAuditSha256=Sha $ResumeRollbackAuditPath
    $activePath=Join-Path $writerFenceControlRoot 'active.json'
    [IO.File]::WriteAllText($activePath,($seedCase.active|ConvertTo-Json -Depth 10),[Text.UTF8Encoding]::new($false))
    $owner=[ordered]@{schema='label-match-installer-owner-v1';run_id=[string]$seedCase.audit.run_id;
        install_root=$install;control_root=$writerFenceControlRoot}
    foreach($field in @('session_id','attempt_id','replacement_transaction_id','writer_inventory_sha256','orchestrator_sha256','writer_contract_sha256','session_started_at_utc','owner_process_id','owner_process_started_at_filetime')){
        $owner[$field]=$seedCase.audit.writer_fence.$field
    }
    $ownerPath=Join-Path (Split-Path -Parent $install) ('.installer-owner-'+$owner.session_id+'-'+($owner.run_id -split '-')[-1]+'.json')
    [IO.File]::WriteAllText($ownerPath,($owner|ConvertTo-Json -Depth 10),[Text.UTF8Encoding]::new($false))
    $item=[ordered]@{site=$seedCase.row.site;status=$seedCase.row.status;fence_status=$seedCase.row.fence_status;
        released=$false;reinstall_open=$false;idempotent=$false;control=$writerFenceControlRoot;error_type='';error_line=0;reason=''}
    try {
        Invoke-LabelResumeRollback
        $releasePath=Join-Path $localAuditRoot ('orphan-fence-release-'+$owner.attempt_id+'.json')
        $release=Get-Content -LiteralPath $releasePath -Raw -Encoding UTF8|ConvertFrom-Json @jsonArgs
        $item.released=(-not(Test-Path -LiteralPath $activePath)) -and $release.state -ceq 'RELEASED'
        $releaseHash=Sha $releasePath
        Invoke-LabelResumeRollback
        $item.idempotent=(Sha $releasePath) -ceq $releaseHash -and -not(Test-Path -LiteralPath $activePath)
        $new=[Guid]::NewGuid().ToString('N')
        [void](Start-LabelWriterFence -ControlRoot $writerFenceControlRoot -Status 'QUIESCING' -SessionId $new -AttemptId $new `
            -ReplacementTransactionId $new -OrchestratorSha256 $owner.orchestrator_sha256 -WriterContractSha256 $owner.writer_contract_sha256 -AuthorityOwnedByCaller)
        $item.reinstall_open=Test-Path -LiteralPath $activePath
        Remove-Item -LiteralPath $activePath -ErrorAction Stop
    } catch {
        $item.error_type=$_.Exception.GetType().Name;$item.error_line=$_.InvocationInfo.ScriptLineNumber
        if($_.Exception.Message -cmatch '^ORPHAN_[A-Z_]+$'){$item.reason=$_.Exception.Message}
    }
    $item['audit_unchanged']=(Sha $ResumeRollbackAuditPath) -ceq $ExpectedRollbackAuditSha256
    $item['replay_count']=$script:ReplayCount
    $results += [pscustomobject]$item
}
[IO.File]::WriteAllText($Output,($results|ConvertTo-Json -Depth 15),[Text.UTF8Encoding]::new($false))

param([string]$Repo,[string]$Root,[string]$Python)
$ErrorActionPreference='Stop'
trap {
    $code=if($_.Exception.Message -match '^[A-Z][A-Z0-9_]{0,80}$'){$_.Exception.Message}else{'UNCLASSIFIED'}
    [IO.File]::WriteAllText((Join-Path $Root 'parent-failure.json'),(@{commands=@([regex]::Matches($_.ScriptStackTrace,'(?m)^at ([A-Za-z_][A-Za-z_0-9-]*),')|ForEach-Object{$_.Groups[1].Value});type=$_.Exception.GetType().Name;code=$code;line=$_.InvocationInfo.ScriptLineNumber;locations=@([regex]::Matches($_.ScriptStackTrace,': line ([0-9]+)')|ForEach-Object{[int]$_.Groups[1].Value})}|ConvertTo-Json -Compress))
    exit 1
}
$case=Get-Content -LiteralPath (Join-Path $Root 'case.json') -Raw|ConvertFrom-Json
. (Join-Path $Repo 'tools/bootstrap_integrity.ps1') -SharedCodeRoot $Repo
. (Join-Path $Repo 'tools/label_writer_fence.ps1')
. (Join-Path $Repo 'tools/failure_redaction.ps1')
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile((Join-Path $Repo 'INSTALL_CANONICAL_PORTABLE.ps1'),[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'SOURCE_PARSE_FAILED'}
foreach($name in @('Protect-LabelFailureText','Arg','Initialize-LabelChildOutputReader','ConvertTo-LabelFailureOutput',
    'Invoke-LabelLifecycleContextProcess','Read-CanonicalStartupLifecycle','Invoke-LabelMachinePlacementRollback','Invoke-LabelMachineWorkerInstall')){
    $node=$ast.Find({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $name},$true)
    if($null -eq $node){throw 'FUNCTION_MISSING'}
    . ([scriptblock]::Create($node.Extent.Text))
}
. (Join-Path $Repo 'tests/lm_machine_acl_fixture.ps1')
$Script:testProgramData=Join-Path $Root 'ProgramData'
$Script:testAnchor=Join-Path $Root 'trusted-anchor'
Set-TestMachineAcl $Script:testProgramData -ProgramData
Set-TestMachineAcl $Script:testAnchor
$Script:actualLauncher=(Get-Command Invoke-LabelLifecycleContextProcess).ScriptBlock
$backend=Join-Path $Repo 'tests/_machine_parent_lifecycle_child.py'
$requestPath=Join-Path $Root 'authority-request.json'
$names=@('KMTECH_LABEL_WRITER_DELEGATION_TOKEN','KMTECH_LABEL_WRITER_DELEGATION_SESSION_ID',
    'KMTECH_LABEL_WRITER_DELEGATION_ATTEMPT_ID','KMTECH_LABEL_WRITER_DELEGATION_TRANSACTION_ID')
$parentBefore=@()
foreach($name in $names){
    [Environment]::SetEnvironmentVariable($name,([Guid]::NewGuid().ToString('N')),'Process')
    $parentBefore+= [Environment]::GetEnvironmentVariable($name,'Process')
}
function Run-Backend($Mode){
    $result=& $Script:actualLauncher $Python @('-B',$backend,$Mode,$requestPath) @{}
    if($result.exit_code -ne 0){throw 'FIXTURE_PRODUCER_FAILED'}
}
function Invoke-LabelLifecycleContextProcess($IgnoredPython,[string[]]$Arguments,[hashtable]$DelegationEnvironment){
    if($Arguments.Count -lt 4 -or $Arguments[0] -cne '-I' -or $Arguments[1] -cne '-B'){throw 'READBACK_ARGUMENTS_DIFFERS'}
    $Script:readCalls++
    $stage=if($Script:readCalls -eq 1){'forward'}else{'restore'}
    if($case.scenario -like ($stage+'-*')){
        $request.fault=($case.scenario -split '-')[1]
        [IO.File]::WriteAllText($requestPath,($request|ConvertTo-Json -Depth 10))
        Run-Backend 'mutate'
    }
    # The installed runtime is an OS fixture boundary: run its unchanged CLI
    # through the guarded host bootstrap instead of unguarded isolated Python.
    $guarded=@('-B',$backend,'read',$requestPath)+@($Arguments|Select-Object -Skip 3)
    return (& $Script:actualLauncher $Python $guarded $DelegationEnvironment)
}
function Same($A,$B){return [IO.Path]::GetFullPath($A) -ieq [IO.Path]::GetFullPath($B)}
function Full($Path,$Label){return [IO.Path]::GetFullPath($Path)}
function Sha($Path){return ('a'*64)}
function Assert-LabelMachineEvidencePath {}
function UnquiescedProductWriters {return @()}
function Assert-WriterTransition($Source,$Installed){return @{compatibility='PASS';candidate_inventory_sha256=$case.inventory;installed_inventory_sha256=$case.inventory}}
$receipt=@{sha256=('b'*64);bootstrap_aggregate_sha256=('b'*64);file_count=1;byte_count=1;critical_file_sha256=@{}}
$preimageReceipt=@{sha256=('a'*64);bootstrap_aggregate_sha256=('a'*64);file_count=1;byte_count=1;critical_file_sha256=@{}}
function PortableInventory($Path){
    if(Same $Path $case.source){return $receipt}
    $active=Get-Content -LiteralPath (Join-Path $case.managed 'update-control/active.json') -Raw|ConvertFrom-Json
    if($active.sha -ceq $case.selected -and $Script:placed){return $receipt}
    return $preimageReceipt
}
function FreezePlacementHelper($Source,$AuditRoot,$Run,$Critical){
    [void][IO.Directory]::CreateDirectory($AuditRoot)
    $frozenRoot=Join-Path $Root 'frozen';[void][IO.Directory]::CreateDirectory($frozenRoot)
    $leaf=Join-Path $frozenRoot 'writer.ps1';[IO.File]::WriteAllText($leaf,'# fixture boundary')
    return @{root=$frozenRoot;writer_fence_path=$leaf;writer_fence_sha256=('a'*64);integrity_sha256=('a'*64);helper_sha256=('a'*64);helper_path=$leaf}
}
function PinnedFileBytes($Path,$Hash){return ,([Text.Encoding]::UTF8.GetBytes('# fixture boundary'))}
function Enter-LabelWriterSessionAuthority {
    param($SessionId,$AttemptId,$OrchestratorSha256,$ReplacementTransactionId,$WriterContractSha256)
    $Script:request=@{managed=$case.managed;control=$control;run=$run;status='INSTALLING';delegation=$delegation;
        program_data=$Script:testProgramData;acl_anchor=$Script:testAnchor;owner_pid=[int]$PID;owner_birth=[Diagnostics.Process]::GetCurrentProcess().StartTime.ToUniversalTime().ToFileTimeUtc().ToString();
        sources=@('auto_update_fixed_helper','canonical_placement');
        tuple=@{session_id=$SessionId;attempt_id=$AttemptId;orchestrator_sha256=$OrchestratorSha256;
            replacement_transaction_id=$ReplacementTransactionId;writer_contract_sha256=$WriterContractSha256;writer_inventory_sha256=$case.inventory}}
    [IO.File]::WriteAllText($requestPath,($request|ConvertTo-Json -Depth 10));Run-Backend 'publish'
    $descriptor=Get-Content -LiteralPath (Join-Path $Root 'authority-descriptor.json') -Raw|ConvertFrom-Json
    $Script:authorityMutex=[Threading.Mutex]::new($false,[string]$descriptor.mutex)
    if(-not $Script:authorityMutex.WaitOne(0)){throw 'AUTHORITY_MUTEX_BUSY'}
    return $Script:authorityMutex
}
function Exit-LabelWriterSessionAuthority($Authority){$Authority.ReleaseMutex();$Authority.Dispose()}
function Start-LabelWriterFence {return @{} }
function Set-LabelWriterFenceDelegation {
    param($ControlRoot,$Status,$SessionId,$AttemptId,$ReplacementTransactionId,$DelegationToken,$LifetimeSeconds,$DelegatedSources)
    $request.status=$Status;$request.delegation=$DelegationToken;$request.sources=@($DelegatedSources)
    [IO.File]::WriteAllText($requestPath,($request|ConvertTo-Json -Depth 10));Run-Backend 'publish'
    return @{}
}
function Stop-LabelWriterFence {return @{} }
function InvokeFrozenPlacementHelper($Frozen,$Parameters){
    $activePath=Join-Path $case.managed 'update-control/active.json';$transitionPath=Join-Path $case.managed 'update-control/transition.json'
    if($Parameters.ContainsKey('StartupUpdateRestorePreimage')){
        [IO.File]::WriteAllBytes($activePath,$Script:oldActive);[IO.File]::WriteAllBytes($transitionPath,$Script:oldTransition)
        $Script:placed=$false;return 0
    }
    $Script:oldActive=[IO.File]::ReadAllBytes($activePath);$Script:oldTransition=[IO.File]::ReadAllBytes($transitionPath)
    $active=$Script:oldActive|ForEach-Object{$_};$active=([Text.Encoding]::UTF8.GetString($Script:oldActive))|ConvertFrom-Json
    $active.sha=$case.selected
    $transition=([Text.Encoding]::UTF8.GetString($Script:oldTransition))|ConvertFrom-Json
    $transition | Add-Member -NotePropertyMembers @{phase='CONFIRMED';candidate_sha=$case.selected;previous_sha=$case.before_sha} -Force
    [IO.File]::WriteAllText($activePath,($active|ConvertTo-Json -Depth 10));[IO.File]::WriteAllText($transitionPath,($transition|ConvertTo-Json -Depth 10))
    $startup=Join-Path $case.managed ('.startup-update-install.'+$Parameters.WriterFenceAttemptId)
    [void][IO.Directory]::CreateDirectory($startup);[IO.File]::WriteAllText((Join-Path $startup 'preimage.json'),'{}')
    $Script:placed=$true;return 0
}
function InvokeFrozenIntegrityProbe {}
function Read-LabelProtectedReusedProof {return @{status='PASS';code_replaced=$false}}
function New-LabelMachineWorkerPendingMarker {return @{status='PENDING'}}
function Install-LabelMachineWorkerShortcut {
    if($case.scenario -eq 'restore' -or $case.scenario -like 'restore-*'){throw 'INJECTED_SHORTCUT_FAILURE'}
    return @{published=@()}
}
function Get-LabelManagedEmergencyPublished {return @()}
function Get-LabelLaunchFailure($Failure,$Stage){return @{stage=$Stage;exception_type=$Failure.Exception.GetType().Name}}
function Write-LabelInstallerRelayRecoveryGuidance {}
function Save($Path,$Value){[void][IO.Directory]::CreateDirectory((Split-Path -Parent $Path));[IO.File]::WriteAllText($Path,($Value|ConvertTo-Json -Depth 20))}
function Save-LabelMachineMetadata($Path,$Value){Save $Path $Value}
$env:PROGRAMDATA=$Script:testProgramData
$EvidencePath=Join-Path $Root 'final-audit.json';$testMode=$true;$serverBaseUrl='';$serverBaseUrlSource='product_default'
$Script:LabelWriterFenceInventorySha256=$case.inventory;$Script:readCalls=0;$Script:placed=$false
$policy=Get-Content -LiteralPath (Join-Path $case.managed 'update-control/policy.json') -Raw|ConvertFrom-Json
$worker=@{sid=$policy.operator_sid;local_app_data=$policy.operator_local_app_data;inventory_sha256=('a'*64)}
$manifest=Get-Content -LiteralPath (Join-Path $case.source 'portable-manifest.json') -Raw|ConvertFrom-Json
$lifecycle=@{lifecycle_state='INSTALLED';active_code_root=$case.prior;from_commit=$case.before_sha}
$failed=$false
try{Invoke-LabelMachineWorkerInstall $worker $case.source (Join-Path $case.managed 'current') $manifest $lifecycle}catch{
    $failed=$true
    $code=if($_.Exception.Message -match '^[A-Z][A-Z0-9_]{0,80}$'){$_.Exception.Message}else{'UNCLASSIFIED'}
    [IO.File]::WriteAllText((Join-Path $Root 'parent-call-failure.json'),(@{commands=@([regex]::Matches($_.ScriptStackTrace,'(?m)^at ([A-Za-z_][A-Za-z_0-9-]*),')|ForEach-Object{$_.Groups[1].Value});type=$_.Exception.GetType().Name;code=$code;line=$_.InvocationInfo.ScriptLineNumber;locations=@([regex]::Matches($_.ScriptStackTrace,': line ([0-9]+)')|ForEach-Object{[int]$_.Groups[1].Value})}|ConvertTo-Json -Compress))
}
for($i=0;$i -lt $names.Count;$i++){
    if([Environment]::GetEnvironmentVariable($names[$i],'Process') -cne $parentBefore[$i]){throw 'PARENT_ENVIRONMENT_CHANGED'}
}
$saved=Get-Content -LiteralPath $EvidencePath -Raw|ConvertFrom-Json
if($case.scenario -in @('same','new')){
    if($failed -or $saved.status -cne 'PASS_MACHINE_WORKER_PENDING'){throw 'PARENT_SUCCESS_UNPROVEN'}
}elseif($case.scenario -like 'restore-*'){
    if(-not $failed -or $saved.status -cne 'ROLLBACK_FAILED' -or $saved.rollback.fence_released){throw 'RESTORE_AUTHORITY_NOT_REJECTED'}
}else{
    if(-not $failed -or -not $saved.rollback.code_restored -or -not $saved.rollback.fence_released){throw 'RESTORE_READBACK_UNPROVEN'}
}
'PASS'

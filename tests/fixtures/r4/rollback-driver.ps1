param([string]$Repo='C:/KMTech/w9setupprepsub260/candidate',
      [Parameter(Mandatory)][string]$Root,
      [Parameter(Mandatory)][string]$Output,
      [ValidateSet('success','early_failure','runtime_unproven','g_refusal','fresh_cancel','installed_cancel')][string]$Case='early_failure',
      [ValidateSet('complete','preimage_present','install_present','placement_committed','setup_started',
          'missing_preimage','missing_placement','missing_setup','string_setup','absence_unproven')][string]$Proof='complete')
$ErrorActionPreference='Stop'
# Keep every extracted function name together for sub-274.
$ExtractedFunctionNames=@('Invoke-LabelRollbackStep','Get-LabelLaunchFailure',
    'Get-LabelPortableDataVersion','Assert-LabelDataDowngrade','Assert-LabelRollbackGeneration',
    'Invoke-LabelCanonicalRollback','Save-LabelRollbackContext',
    'Stop-LabelRollbackCandidateRelay','Invoke-LabelGenerationRefusalRecovery',
    'Assert-LabelRollbackTaskPreimage','Get-LabelCurrentUserScheduledTasks',
    'Protect-LabelFailureText','ByteSha','Sha','InvokeFrozenPlacementHelper')
. (Join-Path $Repo 'tools/failure_redaction.ps1')
New-Item -ItemType Directory -Path $Root,(Join-Path $Root 'control') | Out-Null
if($Case -cne 'fresh_cancel'){
New-Item -ItemType Directory -Path (Join-Path $Root 'installed/runtime') | Out-Null
# Membership placeholder only; Product is a native-free stand-in and never executes it.
[IO.File]::WriteAllBytes((Join-Path $Root 'installed/runtime/pythonw.exe'),[byte[]]@())
}
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
. (Join-Path $Repo 'kmtech_shared/powershell/portable.ps1')
$script:GCalls=0
function Assert-LabelRollbackGeneration {
    $script:GCalls++
    Invoke-ActualLabelRollbackGeneration
}
$script:NativeCalls=[ordered]@{product=0;setup=0;placement=0;run_restore=0;task_restore=0;fence_release=0;relay_restart=0;authority_release=0;inventory=0}
# Only native installation, process, task, Run, and mutex effects are stand-ins.
# Generation policy, rollback step collection, and the complete catch/finally are real source.
function Manifest { param($Path,$Skip) return [pscustomobject]@{version='v2.0.95';source_commit=$sourceCommit} }
function PortableInventory { param($Path) $script:NativeCalls.inventory++;return [pscustomobject]@{sha256=$inventorySha;file_count=1;byte_count=1} }
function Same { param($a,$b) return [IO.Path]::GetFullPath($a) -ceq [IO.Path]::GetFullPath($b) }
function InvokeFrozenIntegrityProbe { param($Frozen,$Path) if(-not (Test-Path -LiteralPath $Path -PathType Container)){throw 'Isolated tree absent'} }
function Set-LabelWriterFenceDelegation { }
function SetWriterDelegationEnvironment { }
function Set-LabelWriterFenceInstalledTree { }
function Relays { return @() }
function Get-LabelCurrentUserScheduledTasks { return @() }
function Assert-RollbackRelayPreimage { param([object[]]$ExpectedRelays) return $true }
function Product {
    param($Root,$Mode,$ExtraArguments,$LogicalAppRoot)
    if($Mode -ceq '--onboard-current-user'){$script:NativeCalls.setup++;throw 'Injected setup child launch failure'}
    $script:NativeCalls.product++
    $script:RemovalArguments=[ordered]@{root=$Root;mode=$Mode;logical_root=$LogicalAppRoot}
    if($Case -cin @('early_failure','fresh_cancel','installed_cancel')){throw 'Injected isolated current-user removal failure'}
}
function Enter-LabelWriterAdmission { return [pscustomobject]@{isolated=$true} }
function Exit-LabelWriterAdmission { }
function Restore { param($Before) $script:NativeCalls.run_restore++;$script:RunState=$Before }
function RestoreScheduledTask { param($Before) $script:NativeCalls.task_restore++;$script:TaskState=$Before }
function Snapshot {
    if($Case -ceq 'runtime_unproven'){return [pscustomobject]@{exists=$true;kind='String';data='unproven-run-command'}}
    return $script:RunState
}
function Stop-LabelWriterFence { $script:NativeCalls.fence_release++;return [pscustomobject]@{writer_inventory_sha256=$inventorySha} }
function StartRaw { $script:NativeCalls.relay_restart++;return 1 }
function Get-CimInstance { return [pscustomobject]@{ExecutablePath=(Join-Path $Root 'prior-runtime.exe')} }
function Get-AuthenticodeSignature { return [pscustomobject]@{Status='NotSigned'} }
function Start-Sleep { }
function Start-Process { throw 'R4 native process execution prohibited' }
function Start-LabelPlacementSession { $script:NativeCalls.placement++;throw [ComponentModel.Win32Exception]::new(1223) }
function Save-LabelPlacementRecoveryPreimages { }
function Invoke-LabelDelegatedPlacementHelper {
    if(-not $install.StartsWith($Root+[IO.Path]::DirectorySeparatorChar,[StringComparison]::OrdinalIgnoreCase)){throw 'Fixture cleanup escaped isolated root'}
    Remove-Item -LiteralPath $install -Recurse
    return 0
}
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
$script:RemovalArguments=$null
$transaction=@($ast.FindAll({param($n)
    $n -is [Management.Automation.Language.TryStatementAst] -and
    @($n.CatchClauses | Where-Object {$_.Body.Extent.Text.Contains('. Invoke-LabelCanonicalRollback')}).Count -gt 0
},$true))
if($transaction.Count -ne 1){throw 'Unique product transaction not found'}
if($Case -cin @('fresh_cancel','installed_cancel')){
    if($Case -ceq 'fresh_cancel'){$existingVerified=$false;$old=@();$replacementRollbackRoot=''}
    # Run the complete source audit initializer before altering any proof input.
    # Its file hash reads only a fixture packet; Authenticode is a native stand-in.
    $source=Join-Path $Root 'packet'
    New-Item -ItemType Directory -Path (Join-Path $source 'runtime') | Out-Null
    [IO.File]::WriteAllBytes((Join-Path $source 'runtime/pythonw.exe'),[byte[]]@())
    $runId=[guid]::NewGuid().ToString();$pristineInstall=-not $existingVerified
    $sourceManifest=[pscustomobject]@{source_commit=$sourceCommit};$candidate=$sourceManifest
    $elevationLogPath=Join-Path $Root 'placement.jsonl';$RunName='isolated-run'
    $serverBaseUrlSource='isolated';$serverBaseUrl='';$wanted='isolated-mutated-command'
    $writerTransition=[pscustomobject]@{compatibility='isolated';installed_inventory_sha256=$inventorySha;candidate_inventory_sha256=$inventorySha}
    $auditInitialization=@($ast.FindAll({param($n)
        $n -is [Management.Automation.Language.AssignmentStatementAst] -and
        $n.Left -is [Management.Automation.Language.VariableExpressionAst] -and
        $n.Left.VariablePath.UserPath -ceq 'audit' -and
        $n.Right.Extent.Text.Contains("schema = 'label-match-canonical-portable-install-v1'") -and
        $n.Right.Extent.Text.Contains('code_preimage =')
    },$true))
    if($auditInitialization.Count -ne 1){throw 'Unique source audit initializer not found'}
    . ([scriptblock]::Create($auditInitialization[0].Extent.Text))
    $initialAudit=[ordered]@{}
    foreach($name in @('schema','status','code_preimage','code_state','code_placement','current_user_setup_started')){
        $initialAudit[$name]=$audit[$name]
    }
    $audit.writer_fence=[ordered]@{status='RESTORING'}
    $helperParameters=@{}
    $frozenPlacement=[pscustomobject]@{root=$Root;helper_path=(Join-Path $Root 'helper.ps1');helper_sha256=('a'*64);
        integrity_sha256=('b'*64);writer_fence_path=(Join-Path $Root 'writer.ps1');writer_fence_sha256=('c'*64);
        data_generation_path=(Join-Path $Root 'generation.ps1');data_generation_sha256=('d'*64)}
    if($Proof -ceq 'preimage_present'){$audit.code_preimage='PRESENT'}
    if($Proof -ceq 'install_present'){New-Item -ItemType Directory -Path $install | Out-Null}
    if($Proof -ceq 'placement_committed'){$audit.code_placement='PASS'}
    if($Proof -ceq 'missing_preimage'){$audit.Remove('code_preimage')}
    if($Proof -ceq 'missing_placement'){$audit.Remove('code_placement')}
    if($Proof -ceq 'missing_setup'){$audit.Remove('current_user_setup_started')}
    if($Proof -ceq 'string_setup'){$audit.current_user_setup_started='false'}
    if($Proof -ceq 'setup_started'){
        # Execute the real forward call and its adjacent attempt record even
        # though this synthetic installed tree has subsequently disappeared.
        $statements=@($transaction[0].Body.Statements)
        $onboarding=@($statements | Where-Object {$_.Extent.Text.Contains("'--onboard-current-user'") -and $_ -is [Management.Automation.Language.TryStatementAst]})
        if($onboarding.Count -ne 1){throw 'Forward onboarding call is ambiguous'}
        $index=[array]::IndexOf($statements,$onboarding[0])
        $productInstall=$install
        if($statements[$index-1].Extent.Text.StartsWith('$audit.current_user_setup_started')){
            . ([scriptblock]::Create($statements[$index-1].Extent.Text))
        }
        . ([scriptblock]::Create($onboarding[0].Extent.Text))
    }
    $script:AbsenceQueryRefused=$false
    if($Proof -ceq 'absence_unproven'){
        function Test-Path {
            param($LiteralPath,$Path,[string]$PathType,[string]$ErrorAction)
            $phaseStack=Get-Variable -Name LabelRollbackPhaseStack -Scope Script -ErrorAction SilentlyContinue
            if($LiteralPath -ceq $install -and $null -ne $phaseStack -and $phaseStack.Value.Count -eq 0 -and -not $script:AbsenceQueryRefused){$script:AbsenceQueryRefused=$true;throw 'Injected absence observation failure'}
            $arguments=@{}
            if($LiteralPath){$arguments.LiteralPath=$LiteralPath}else{$arguments.Path=$Path}
            if($PathType){$arguments.PathType=$PathType}
            Microsoft.PowerShell.Management\Test-Path @arguments
        }
    }
    $Script:LabelPlacementSessionRequested=$true;$Script:LabelPlacementSession=$null;$Script:LabelPlacementSessionLaunchAttempted=$false
    $placementAttempted=$true
}
$catchBody=$transaction[0].CatchClauses[0].Body.Extent.Text
$finallyBody=$transaction[0].Finally.Extent.Text
$script:RunState=$before;$script:TaskState=$taskBefore
if($Case -cin @('fresh_cancel','installed_cancel')){
    $script:RunState=[pscustomobject]@{exists=$true;kind='String';data='isolated-mutated-command'}
    $script:TaskState=[pscustomobject]@{exists=$true}
}
$shortcutPath=Join-Path $Root 'shortcut-stand-in.txt'
[IO.File]::WriteAllText($shortcutPath,'isolated prior shortcut')
$forward=if($Case -cin @('fresh_cancel','installed_cancel')){'[void](InvokeFrozenPlacementHelper $frozenPlacement @{ElevationLogPath=(Join-Path $Root ''placement'')})'}else{'throw ''Injected isolated forward failure'''}
$execute=[scriptblock]::Create("try {$forward} catch $catchBody finally $finallyBody")
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
if($Case -cin @('fresh_cancel','installed_cancel')){
    $record.initial_audit=$initialAudit
    $record.run_preimage_restored=($script:RunState.exists -eq $before.exists -and $script:RunState.data -ceq $before.data)
    $record.task_preimage_restored=($script:TaskState.exists -eq $taskBefore.exists)
    $record.shortcut_preimage_restored=([IO.File]::ReadAllText($shortcutPath) -ceq 'isolated prior shortcut')
    $record.absence_query_refused=$script:AbsenceQueryRefused
    $record.proof=$Proof;$record.steps=$steps;$record.runtime_restored=$audit.rollback.runtime_restored
    $record.code_placement=$audit.rollback.code_placement;$record.setup_started=$audit.current_user_setup_started
    $record.removal_arguments=$script:RemovalArguments
    $record.original_failure_preserved=([string]$audit.failure_details.placement_code -ceq 'PLACEMENT_LAUNCH_FAILED')
    $record.original_native_error=$original.Exception.GetBaseException().NativeErrorCode
}
$record | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $Output -Encoding UTF8

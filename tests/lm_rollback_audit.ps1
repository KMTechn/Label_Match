param([string]$Repo,[string]$Root,[string]$Scenario)
$ErrorActionPreference='Stop'
. (Join-Path $Repo 'tools/label_writer_fence.ps1')
trap {
    [IO.File]::WriteAllText((Join-Path $Root 'failure-site.json'),(@{type=$_.Exception.GetType().Name;line=$_.InvocationInfo.ScriptLineNumber;locations=@($_.ScriptStackTrace -split '\r?\n'|ForEach-Object{if($_ -match ': line (\d+)'){$Matches[1]}})}|ConvertTo-Json -Compress))
    exit 1
}
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile((Join-Path $Repo 'INSTALL_CANONICAL_PORTABLE.ps1'),[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'SOURCE_PARSE_FAILED'}
foreach($name in @('Wait-LabelRollbackRelayReady','Invoke-LabelRollbackStep')){
    $node=$ast.Find({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $name},$true)
    . ([scriptblock]::Create($node.Extent.Text))
}
$rollback=$ast.Find({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Invoke-LabelCanonicalRollback'},$true)
$steps=@($rollback.Body.FindAll({param($n)$n -is [Management.Automation.Language.CommandAst] -and $n.GetCommandName() -ceq 'Invoke-LabelRollbackStep'},$true))
$relayStep=@($steps|Where-Object{$_.CommandElements[1].Value -ceq 'relay_ready'})
if($relayStep.Count -ne 1){throw 'RELAY_STEP_DIFFERS'}
$bodyStart=$rollback.Body.Extent.StartOffset+1
$initial=$rollback.Extent.Text.Substring($bodyStart-$rollback.Extent.StartOffset,$steps[0].Parent.Extent.StartOffset-$bodyStart)
$tailAssignment=@($rollback.Body.FindAll({param($n)$n -is [Management.Automation.Language.AssignmentStatementAst] -and
    $n.Left.Extent.Text -cin @('$audit.rollback.runtime_restored','$audit.rollback.autostart_restored') -and
    $n.Extent.StartOffset -gt $relayStep[0].Extent.StartOffset},$true))|Sort-Object {$_.Extent.StartOffset}|Select-Object -First 1
$tailStart=$tailAssignment.Extent.StartOffset
$tail=$rollback.Extent.Text.Substring($tailStart-$rollback.Extent.StartOffset,$rollback.Body.Extent.EndOffset-1-$tailStart)
# Only shorten the timeout in the extracted production call; the real bounded
# readiness helper throws before the production audit field assignment.
$step=$relayStep[0].Parent.Extent.Text.Replace('Wait-LabelRollbackRelayReady','Wait-LabelRollbackRelayReady -TimeoutSeconds 0.001')
$prepared='$rollbackRuntimeProven=$true;$rollbackStartupProven=$true;$audit.rollback.code_restored=$true;'
. ([scriptblock]::Create('function Invoke-LabelCanonicalRollback {'+$initial+$prepared+$step+"`n"+$tail+'}'))
$outer=@($ast.EndBlock.Statements|Where-Object{$_ -is [Management.Automation.Language.TryStatementAst] -and $_.CatchClauses.Count -gt 0 -and $_.CatchClauses[0].Body.Extent.Text.Contains('Save-LabelRollbackContext')})
if($outer.Count -ne 1){throw 'OUTER_HANDLER_DIFFERS'}
$text=$outer[0].CatchClauses[0].Body.Extent.Text
$failureHandler=[scriptblock]::Create($text.Substring(1,$text.Length-2))
$audit=[ordered]@{rollback=[ordered]@{}}
$auditPath=Join-Path $Root 'audit.json';$install=$Root;$EvidencePath='';$mutated=$true;$placementAttempted=$false
$legacyFirstRegistration=$false;$writerFenceStarted=$false;$startupLifecycle=$null
$Script:LabelPlacementSession=$null;$Script:LabelCanonicalConfirmation=''
$before=@{exists=$false;kind='';data=''}
$relayPath=Join-Path $Root 'relay.json';$candidate=@{source_commit=('a'*40)}
$rollbackRelayStartedAt=[DateTime]::UtcNow
$process=[Diagnostics.Process]::GetCurrentProcess();try{$created=$process.StartTime.ToUniversalTime()}finally{$process.Dispose()}
$old=@([pscustomobject]@{ExecutablePath=(Join-Path $Root 'runtime/pythonw.exe');ProcessId=[int]$PID;CreationDate=$created})
function Assert-RollbackRelayPreimage {param($ExpectedRelays);return $ExpectedRelays}
function Assert-LabelShortcutCopyPath {}
function Test-RelayPersistentRetry {return $true}
function Same([string]$Left,[string]$Right){return [StringComparer]::OrdinalIgnoreCase.Equals([IO.Path]::GetFullPath($Left),[IO.Path]::GetFullPath($Right))}
if($Scenario -ceq 'ready'){
    [IO.File]::WriteAllText($relayPath,(@{status='RUNNING';source_commit=$candidate.source_commit;ordinary_user=$true;process_id=[int]$PID;code_root=$Root;updated_at=[DateTime]::UtcNow.ToString('o')}|ConvertTo-Json -Compress))
    # Give a ready record a practical deadline, leaving the real timeout case short.
    $preparedStep=$step.Replace('-TimeoutSeconds 0.001','-TimeoutSeconds 2')
    . ([scriptblock]::Create('function Invoke-LabelCanonicalRollback {'+$initial+$prepared+$preparedStep+"`n"+$tail+'}'))
}elseif($Scenario -ceq 'absent'){$old=@()}
function Get-LabelLaunchFailure($Failure,$Stage){return @{guidance='fixed';exception_type=$Failure.Exception.GetType().Name;readiness_timeout=($Failure.Exception.Message -ceq 'Rollback relay readiness remains unproven.')}}
function Save-LabelRollbackContext {}
function Snapshot {return @{exists=$false;kind='';data=''}}
function Save($Path,$Value){[IO.File]::WriteAllText($Path,($Value|ConvertTo-Json -Depth 20))}
try{try{throw [InvalidOperationException]::new('ORIGINAL_FORWARD_FAILURE')}catch{. $failureHandler}}catch{}
$saved=Get-Content -LiteralPath $auditPath -Raw -Encoding UTF8|ConvertFrom-Json
if(-not $saved.rollback.code_restored -or -not $saved.rollback.autostart_restored){throw 'RESTORATION_FACTS_LOST'}
if($Scenario -cin @('ready','absent')){
    if($saved.status -cne 'FAILED_ROLLED_BACK' -or -not $saved.rollback.runtime_restored){throw 'READY_RESTORE_DIFFERS'}
    $wanted=if($Scenario -ceq 'ready'){'PROVEN'}else{'NO_RELAY_PREIMAGE'}
    if($saved.rollback.relay_readiness -cne $wanted -or $saved.rollback.step_failures.Count -ne 0){throw 'READY_PROOF_DIFFERS'}
}else{
    if($saved.status -cne 'ROLLBACK_FAILED' -or $saved.rollback.runtime_restored -or $saved.rollback.relay_readiness -cne 'UNPROVEN'){throw 'TIMEOUT_AUDIT_DIFFERS'}
    if($saved.rollback.step_failures.Count -ne 1 -or -not $saved.rollback.step_failures[0].readiness_timeout -or -not $saved.rollback.failure_details.readiness_timeout){throw 'ORIGINAL_READINESS_ERROR_LOST'}
}
'PASS'

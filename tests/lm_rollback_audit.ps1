param([string]$Repo,[string]$Root,[string]$Scenario)
$ErrorActionPreference='Stop'
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile((Join-Path $Repo 'INSTALL_CANONICAL_PORTABLE.ps1'),[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'SOURCE_PARSE_FAILED'}
$outer=@($ast.EndBlock.Statements | Where-Object {
    $_ -is [Management.Automation.Language.TryStatementAst] -and
    $_.CatchClauses.Count -gt 0 -and $_.CatchClauses[0].Body.Extent.Text.Contains('. Invoke-LabelCanonicalRollback')
})
if($outer.Count -ne 1){throw 'OUTER_CATCH_SELECTION_FAILED'}
$text=$outer[0].CatchClauses[0].Body.Extent.Text
$failureHandler=[scriptblock]::Create($text.Substring(1,$text.Length-2))
$audit=[ordered]@{rollback=[ordered]@{}}
$auditPath=Join-Path $Root 'audit.json'
$install=$Root;$EvidencePath='';$mutated=$true;$placementAttempted=$false
$legacyFirstRegistration=$false;$writerFenceStarted=$false
$Script:LabelPlacementSession=$null;$Script:LabelCanonicalConfirmation=''
$before=@{exists=$false;kind='';data=''}
function Get-LabelLaunchFailure($Failure,$Stage){return @{guidance='FIXED_FAILURE';exception_type=$Failure.Exception.GetType().Name}}
function Save-LabelRollbackContext {}
function Snapshot {return @{exists=$false;kind='';data=''}}
function Save($Path,$Value){[IO.File]::WriteAllText($Path,($Value|ConvertTo-Json -Depth 20))}
function Invoke-LabelCanonicalRollback {
    $rollbackRuntimeProven=$true;$rollbackStartupProven=$true
    $audit.rollback.code_restored=$true
    $audit.rollback.autostart_restored=$true
    $audit.rollback.relay_readiness=if($Scenario -ceq 'ready'){'PROVEN'}else{'UNPROVEN'}
    $audit.rollback.runtime_restored=($Scenario -ceq 'ready')
    if($Scenario -cne 'ready'){throw [TimeoutException]::new('INJECTED_RELAY_DEADLINE')}
}
try {try {throw [InvalidOperationException]::new('INJECTED_FORWARD')} catch {. $failureHandler}} catch {}
$saved=Get-Content -LiteralPath $auditPath -Raw -Encoding UTF8 | ConvertFrom-Json
if(-not $saved.rollback.code_restored -or -not $saved.rollback.autostart_restored){throw 'PREIMAGE_FACTS_LOST'}
if($Scenario -ceq 'ready'){
    if($saved.status -cne 'FAILED_ROLLED_BACK' -or -not $saved.rollback.runtime_restored){throw 'READY_AUDIT_FAILED'}
}else{
    if($saved.status -cne 'ROLLBACK_FAILED' -or $saved.rollback.runtime_restored -or $saved.rollback.relay_readiness -cne 'UNPROVEN'){throw 'FALSE_RUNTIME_RESTORED'}
}
'PASS'

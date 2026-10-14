param([string]$Root,[string]$State='INSTALLED',[string]$Fault='none',[switch]$OuterLease,[string]$SourcePath='')
$ErrorActionPreference='Stop'
if(-not $SourcePath){$SourcePath=Join-Path (Split-Path -Parent $PSScriptRoot) 'INSTALL_THIS_PC.ps1'}
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile($SourcePath,[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'SOURCE_PARSE_FAILED'}
$function=$ast.Find({param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Invoke-LabelPlacementLifecycleChild'},$true)
Invoke-Expression $function.Extent.Text
$calls=@($ast.FindAll({param($node) $node -is [Management.Automation.Language.CommandAst] -and $node.GetCommandName() -eq 'Invoke-LabelPlacementLifecycleChild'},$true))
if($calls.Count -ne 2){throw 'CALLER_COUNT_DIFFERS'}
$query=$calls[0];$activation=$calls[1]
$queryTry=$query.Parent
while($queryTry -and $queryTry -isnot [Management.Automation.Language.TryStatementAst]){$queryTry=$queryTry.Parent}
$activateTry=$activation.Parent
while($activateTry -and $activateTry -isnot [Management.Automation.Language.TryStatementAst]){$activateTry=$activateTry.Parent}
if(-not $activateTry){throw 'ACTIVATION_FINALLY_ABSENT'}
$source=[IO.File]::ReadAllText($SourcePath)
$start=$source.IndexOf('$writerFenceLease = $null')
if($start -lt 0){throw 'LEASE_INITIALIZATION_ABSENT'}
$prefix=$source.Substring($start,$activateTry.Extent.StartOffset-$start)
$statements=@($activateTry.Body.Statements)
$selected=@()
foreach($statement in $statements){
    if($statement.Extent.Text.StartsWith('$stagingRoot = Join-Path')){break}
    $selected += $statement.Extent.Text
}
$activateIf=$activateTry.Body.Find({param($node) $node -is [Management.Automation.Language.IfStatementAst] -and $node.Extent.Text.Contains('$activation = @(Invoke-LabelPlacementLifecycleChild')},$true)
if(-not $activateIf){throw 'ACTIVATION_CALLER_ABSENT'}
$body=($selected -join "`n")+"`n"+$activateIf.Extent.Text
if(-not $queryTry){
    # Keep the parent's actual ordering: first query precedes its final finally.
    $body=$activateIf.Extent.Text
}
$queryAction=$query.Find({param($node) $node -is [Management.Automation.Language.ScriptBlockExpressionAst]},$true)
$activateAction=$activation.Find({param($node) $node -is [Management.Automation.Language.ScriptBlockExpressionAst]},$true)
$queryReplacement=@'
{ $script:actions++; if($Fault -ceq 'query_throw'){throw [IO.IOException]::new('Injected query failure')}
  $global:LASTEXITCODE=if($Fault -ceq 'query_exit'){9}else{0}
  if($Fault -ceq 'query_json'){'not-json'}else{
    @{status='PASS';lifecycle_state=$State;from_commit=$ExpectedActiveCommit}|ConvertTo-Json -Compress
  }
}
'@
$activateReplacement=@'
{ $script:actions++;if($Fault -ceq 'activation_throw'){throw [IO.IOException]::new('Injected activation failure')};$global:LASTEXITCODE=0 }
'@
$prefix=$prefix.Replace($queryAction.Extent.Text,$queryReplacement)
$body=$body.Replace($queryAction.Extent.Text,$queryReplacement).Replace($activateAction.Extent.Text,$activateReplacement)
$memory=$prefix+"`ntry {`n"+$body+"`n} finally "+$activateTry.Finally.Extent.Text
$script:released=[Collections.Generic.List[object]]::new();$script:acquired=0;$script:restored=0;$script:actions=0
$script:binding=[pscustomobject]@{status='HELD';epoch=0}
function Enter-LabelCanonicalPlacement {$script:acquired++;return [pscustomobject]@{acquired=$true;id=$script:acquired}}
function Enter-LabelWriterAdmission {$script:acquired++;return [pscustomobject]@{acquired=$true;id=$script:acquired}}
function Exit-LabelWriterAdmission {param($Lease) $script:released.Add($Lease)}
function Assert-LabelWriterFenceOwner {return $script:binding}
function Set-LabelWriterFenceDelegation {$script:binding=[pscustomobject]@{status='HELD';epoch=1};return $script:binding}
function Write-LabelWriterFenceAtomic {param($Control,$Before) $script:binding=$Before;$script:restored++}
function Assert-NoReparsePoint {}
function Initialize-LabelStartupUpdate {}
$machine=Join-Path $Root 'machine';$sourceRootFull=Join-Path $Root 'source'
[void][IO.Directory]::CreateDirectory($machine);[void][IO.Directory]::CreateDirectory($sourceRootFull)
$installRootFull=Join-Path $machine 'current'
[IO.File]::WriteAllText((Join-Path $machine 'removed.json'),'{}')
$ExpectedActiveCommit='a'*40
[IO.File]::WriteAllText((Join-Path $sourceRootFull 'portable-manifest.json'),(@{source_commit=('b'*40)}|ConvertTo-Json -Compress))
$testOverride=$false;$WriterFenceFunctionsPreloaded=[Management.Automation.SwitchParameter]::new($true)
$WriterFenceControlRoot=Join-Path $Root 'control'
$WriterFenceSessionId=[guid]::NewGuid().ToString('N');$WriterFenceAttemptId=[guid]::NewGuid().ToString('N')
$WriterFenceReplacementTransactionId=[guid]::NewGuid().ToString('N');$WriterFenceDelegationToken=''
if($OuterLease){$script:writerFenceLease=[pscustomobject]@{acquired=$false;id=-1}}
else{Remove-Variable writerFenceLease -Scope Script -ErrorAction SilentlyContinue}
$outerBefore=Get-Variable writerFenceLease -Scope Script -ValueOnly -ErrorAction SilentlyContinue
$nameAssignment=$function.Body.Find({param($node) $node -is [Management.Automation.Language.AssignmentStatementAst] -and
    $node.Left -is [Management.Automation.Language.VariableExpressionAst] -and $node.Left.VariablePath.UserPath -ceq 'names'},$true)
$environmentNames=@(& ([scriptblock]::Create($nameAssignment.Right.Extent.Text)))
$environmentLengths=@($environmentNames|ForEach-Object { $value=[Environment]::GetEnvironmentVariable($_,'Process');if($null -eq $value){-1}else{$value.Length} })
$failure=$null
try{& ([scriptblock]::Create($memory))}catch{$failure=$_}
$wanted=if($State -ceq 'INSTALLED' -and $Fault -notlike 'query_*'){3}else{2}
if($script:acquired -ne $wanted -or $script:released.Count -ne $wanted){throw 'LOCAL_LEASE_FINAL_RELEASE_MISSING'}
for($index=0;$index -lt $wanted;$index++){if($script:released[$index].id -ne $index+1){throw 'LEASE_RELEASE_IDENTITY_DIFFERS'}}
if($script:restored -ne $wanted-1 -or $script:binding.epoch -ne 0){throw 'DELEGATION_RESTORE_DIFFERS'}
$outerAfter=Get-Variable writerFenceLease -Scope Script -ValueOnly -ErrorAction SilentlyContinue
if(-not [object]::ReferenceEquals($outerBefore,$outerAfter)){throw 'OUTER_LEASE_CHANGED'}
$afterLengths=@($environmentNames|ForEach-Object { $value=[Environment]::GetEnvironmentVariable($_,'Process');if($null -eq $value){-1}else{$value.Length} })
if(($environmentLengths -join ',') -cne ($afterLengths -join ',')){
    [IO.File]::WriteAllText((Join-Path $Root 'environment-lengths.json'),(@{before=@($environmentLengths);after=@($afterLengths)}|ConvertTo-Json -Compress))
    throw 'ENVIRONMENT_RESTORE_DIFFERS'
}
if(($Fault -ceq 'none') -ne ($null -eq $failure)){throw 'CHILD_FAILURE_RESULT_DIFFERS'}
'PASS'

param([string]$Repo,[string]$Output,[string]$InputPath)
$ErrorActionPreference='Stop'
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile((Join-Path $Repo 'INSTALL_CANONICAL_PORTABLE.ps1'),[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'R4 product parse failure'}
$newNodes=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.AssignmentStatementAst] -and $n.Left.Extent.Text -ceq '$legacyRelayBefore'},$true))
if($newNodes.Count -eq 1){
    & (Join-Path $PSScriptRoot 'relay-driver.ps1') -Repo $Repo -Output $Output -InputPath $InputPath
    exit 0
}
$script:queries=0
$CanonicalTaskName='direct-sync-relay-label-match'
$ComputerName='r4-fixture-pc';$SourceHostId='label-match-r4-fixture-pc-123456789abc'
$row=[pscustomobject]@{TaskName=('direct-sync-relay-'+$SourceHostId);TaskPath='\';State='Ready';Settings=[pscustomobject]@{Enabled=$true}}
function Get-ScheduledTask {param($TaskName,$TaskPath,$ErrorAction) $script:queries++;return $row}
$nodes=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.AssignmentStatementAst] -and $n.Left.Extent.Text -ceq '$scheduledTasksAtPreflight'},$true))
if($nodes.Count -ne 1){throw 'R4 preflight assignment missing or ambiguous; update extraction target with sub-274'}
. ([scriptblock]::Create($nodes[0].Extent.Text))
[ordered]@{scheduler_queries=$script:queries;observed_count=@($scheduledTasksAtPreflight).Count} |
    ConvertTo-Json | Set-Content -LiteralPath $Output -Encoding UTF8

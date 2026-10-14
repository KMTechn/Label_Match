param([string]$HostRoot,[string]$FixtureRoot)
$ErrorActionPreference='Stop';$WarningPreference='SilentlyContinue'
$tokens=$null;$errors=$null
$tree=[Management.Automation.Language.Parser]::ParseFile((Join-Path $HostRoot 'INSTALL_THIS_PC.ps1'),[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'PARSE_ERROR'}
$blocks=@($tree.FindAll({param($n)$n -is [Management.Automation.Language.IfStatementAst] -and $n.Clauses[0].Item1.Extent.Text -ceq '$Uninstall.IsPresent'},$false))
if($blocks.Count -ne 1){throw 'UNINSTALL_ENTRY_COUNT'}
$Uninstall=[switch]$true;$DryRun=[switch]$false
$testOverride=$true;$WriterFenceFunctionsPreloaded=[switch]$false
$installRootFull=Join-Path $FixtureRoot 'application/current'
$WorkerShortcutRecordPath='fixture-record';$VerifiedWorkerShortcutHelperPath='fixture-helper'
$ExpectedWorkerShortcutHelperSha256='fixture-hash'
[void][IO.Directory]::CreateDirectory((Split-Path -Parent $installRootFull))
$script:events=[Collections.Generic.List[string]]::new()
$script:original=[IO.IOException]::new('test refusal')
function Remove-LabelEmergencyShortcut {param([string]$InstallRoot)
 $script:events.Add('emergency');throw $script:original
}
function Invoke-LabelWorkerShortcutOperation {param($root,$operation,$record,$helper,$hash)
 if($operation -cne 'Restore'){throw 'RESTORE_OPERATION'}
 $script:events.Add('worker')
}
function Invoke-LabelConvergentRemoval {param($InstallRoot,$TaskFolder)
 $script:events.Add('code');return [pscustomobject]@{evidence_root='fixture';retained_paths=@()}
}
$failure=$null;$output=@()
try{$output=@(& ([scriptblock]::Create($blocks[0].Extent.Text)))}catch{$failure=$_.Exception.GetBaseException()}
if(-not [object]::ReferenceEquals($failure,$script:original)){throw 'FIRST_FAILURE_LOST'}
if(($script:events -join ',') -cne 'emergency,worker,code'){throw 'OTHER_REMOVAL_BLOCKED'}
if($output.Count){throw 'FALSE_SUCCESS_OUTPUT'}
'PASS'

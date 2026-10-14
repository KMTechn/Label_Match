param([string]$HostRoot,[string]$FixtureRoot,[ValidateSet('none','worker','core')][string]$NextFailure='none')
$ErrorActionPreference='Stop';$WarningPreference='SilentlyContinue'
$tokens=$null;$errors=$null
$tree=[Management.Automation.Language.Parser]::ParseFile((Join-Path $HostRoot 'INSTALL_THIS_PC.ps1'),[ref]$tokens,[ref]$errors)
foreach($definition in $tree.FindAll({param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst]},$true)){
    $parent=$definition.Parent;$nested=$false
    while($parent){if($parent -is [Management.Automation.Language.FunctionDefinitionAst]){$nested=$true;break};$parent=$parent.Parent}
    if(-not $nested){Invoke-Expression $definition.Extent.Text}
}
$Uninstall=[Management.Automation.SwitchParameter]::new($true)
$DryRun=[Management.Automation.SwitchParameter]::new($false)
$testOverride=$true;$WriterFenceFunctionsPreloaded=[Management.Automation.SwitchParameter]::new($false)
$installRootFull=Join-Path $FixtureRoot 'application\current'
$WorkerShortcutRecordPath=Join-Path $FixtureRoot 'worker-shortcuts.json'
$VerifiedWorkerShortcutHelperPath='';$ExpectedWorkerShortcutHelperSha256=''
$script:events=[Collections.Generic.List[string]]::new()
$script:original=[IO.IOException]::new('Injected emergency unlink refusal')
$script:second=[IO.IOException]::new('Injected later removal refusal')
$linkPath=Get-LabelEmergencyShortcutPath $installRootFull
[void][IO.Directory]::CreateDirectory((Split-Path -Parent $linkPath))
$shell=Microsoft.PowerShell.Utility\New-Object -ComObject WScript.Shell
try{
    $link=$shell.CreateShortcut($linkPath)
    $link.TargetPath=Join-Path $installRootFull 'launch-label-match.cmd'
    $link.Arguments='--ui tk';$link.Description='KMTech.LabelMatch.EmergencyTk.v1'
    $link.Save()
}finally{[void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($shell)}
function Remove-Item {
    [CmdletBinding()]param([string]$LiteralPath)
    if($LiteralPath -ieq $linkPath){$script:events.Add('emergency');throw $script:original}
    Microsoft.PowerShell.Management\Remove-Item -LiteralPath $LiteralPath -ErrorAction Stop
}
function Invoke-LabelWorkerShortcutOperation {
    $script:events.Add('worker')
    if($NextFailure -ceq 'worker'){throw $script:second}
}
function Invoke-LabelConvergentRemoval {
    $script:events.Add('core')
    if($NextFailure -ceq 'core'){throw $script:second}
    return @{evidence_root=$FixtureRoot;retained_paths=@()}
}
$entry=$tree.Find({param($node) $node -is [Management.Automation.Language.IfStatementAst] -and
    $node.Extent.Text.StartsWith('if ($Uninstall.IsPresent)') -and
    $node.Extent.Text.Contains('$removed = Invoke-LabelConvergentRemoval')},$true)
if($null -eq $entry){throw 'Production uninstall branch absent'}
$failure=$null;$output=@()
try{$output=@(Invoke-Expression $entry.Extent.Text)}catch{$failure=$_}
if($null -eq $failure){throw 'Emergency cause was not preserved'}
if($NextFailure -ceq 'none'){
    if($failure.Exception -ne $script:original){throw 'Emergency cause was not preserved'}
}else{
    $aggregate=$failure.Exception.GetBaseException()
    if($aggregate -isnot [AggregateException] -or
       -not $aggregate.InnerExceptions.Contains($script:original) -or
       -not $aggregate.InnerExceptions.Contains($script:second)){throw 'Combined causes were not preserved'}
}
$wanted=if($NextFailure -ceq 'worker'){'emergency,worker'}else{'emergency,worker,core'}
if(($script:events -join ',') -cne $wanted){throw 'Later uninstall stages did not run'}
if(@($output|Where-Object {$_ -like '*PASS*'}).Count){throw 'False uninstall success'}
if(-not[IO.File]::Exists($linkPath)){throw 'Injected refusal did not retain actual shortcut'}
'PASS'

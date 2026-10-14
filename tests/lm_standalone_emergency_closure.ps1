param([string]$Root)
$ErrorActionPreference='Stop'
$repo=Split-Path -Parent $PSScriptRoot
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile((Join-Path $repo 'INSTALL_THIS_PC.ps1'),[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'INSTALLER_PARSE_FAILED'}
$functions=@($ast.FindAll({param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst]},$true))
foreach($function in $functions){
    $parent=$function.Parent;$nested=$false
    while($null -ne $parent){
        if($parent -is [Management.Automation.Language.FunctionDefinitionAst]){$nested=$true;break}
        $parent=$parent.Parent
    }
    if(-not $nested){Invoke-Expression $function.Extent.Text}
}
if(-not(Get-Command Remove-LabelEmergencyShortcut -CommandType Function -ErrorAction SilentlyContinue)){
    throw 'STANDALONE_EMERGENCY_FUNCTION_ABSENT'
}
$application=Join-Path $Root 'application'
$installRootFull=Join-Path $application 'current'
[void][IO.Directory]::CreateDirectory($application)
$linkPath=Get-LabelEmergencyShortcutPath $installRootFull
[void][IO.Directory]::CreateDirectory((Split-Path -Parent $linkPath))
$shell=Microsoft.PowerShell.Utility\New-Object -ComObject WScript.Shell
try{
    $link=$shell.CreateShortcut($linkPath)
    $link.TargetPath=Join-Path $installRootFull 'launch-label-match.cmd'
    $link.Arguments='--ui tk';$link.Description='KMTech.LabelMatch.EmergencyTk.v1'
    $link.WorkingDirectory=$installRootFull;$link.Save()
}finally{[void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($shell)}
if(-not[IO.File]::Exists($linkPath)){throw 'REAL_EMERGENCY_SHORTCUT_NOT_CREATED'}
# Native privilege/scheduler discovery is a seam; file/link removal and marker
# publication use the actual installer functions. No bootstrap is loaded.
function Assert-ProtectedCodeAncestors([string]$Path){}
function Get-LabelOfficialLegacyTaskSnapshot {return $null}
function Get-LabelSupportedStartupTasks([string]$Path){return @()}
function Get-OwnedLegacyTaskRetirementSnapshot([string]$Name){return $null}
$taskFolder=[pscustomobject]@{}
$scheduler=[pscustomobject]@{}
$scheduler | Add-Member ScriptMethod Connect {}
$scheduler | Add-Member ScriptMethod GetFolder {param($Path) return $taskFolder}
function New-Object {
    [CmdletBinding(DefaultParameterSetName='Type')]
    param([Parameter(ParameterSetName='Type',Position=0)][string]$TypeName,
          [Parameter(ParameterSetName='Type',Position=1)][object[]]$ArgumentList,
          [Parameter(ParameterSetName='Com')][string]$ComObject)
    if($PSCmdlet.ParameterSetName -eq 'Com'){
        if($ComObject -ceq 'Schedule.Service'){return $scheduler}
        return Microsoft.PowerShell.Utility\New-Object -ComObject $ComObject
    }
    return Microsoft.PowerShell.Utility\New-Object -TypeName $TypeName -ArgumentList $ArgumentList
}
$testOverride=$false;$earlyTestOverride=$false
$Uninstall=[Management.Automation.SwitchParameter]::new($true)
$DryRun=[Management.Automation.SwitchParameter]::new($false)
$BootstrapIntegrityPreloaded=[Management.Automation.SwitchParameter]::new($false)
$WriterFenceFunctionsPreloaded=[Management.Automation.SwitchParameter]::new($false)
$WorkerShortcutRecordPath=''
$bootstrapSelection=$ast.Find({param($node)
    $node -is [Management.Automation.Language.IfStatementAst] -and
    $node.Extent.Text.StartsWith('if (') -and
    $node.Extent.Text.Contains('$BootstrapIntegrityFunctions = Join-Path')
},$true)
if($null -eq $bootstrapSelection){throw 'BOOTSTRAP_SELECTION_ABSENT'}
$BootstrapIntegrityFunctions=''
Invoke-Expression $bootstrapSelection.Extent.Text
if($BootstrapIntegrityFunctions){throw 'PRODUCTION_UNINSTALL_LOADED_BOOTSTRAP'}
$entry=$ast.Find({param($node)
    $node -is [Management.Automation.Language.IfStatementAst] -and
    $node.Extent.Text.StartsWith('if ($Uninstall.IsPresent)') -and
    $node.Extent.Text.Contains('$removed = Invoke-LabelConvergentRemoval')
},$true)
if($null -eq $entry){throw 'PRODUCTION_UNINSTALL_ENTRY_ABSENT'}
Invoke-Expression $entry.Extent.Text

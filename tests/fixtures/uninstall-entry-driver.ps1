param([string]$Repo,[string]$Root,[string]$InputPath,[string]$Scenario)
$ErrorActionPreference='Stop'
$global:RemovalTestRoot=[IO.Path]::GetFullPath($Root)
$global:RemovalTestScenario=$Scenario
$global:RemovalTestEvents=[Collections.Generic.List[string]]::new()
$inputRecord=Get-Content -LiteralPath $InputPath -Raw -Encoding UTF8 | ConvertFrom-Json
$global:RemovalTestTasks=@(foreach($record in @($inputRecord.tasks)) {
    $xml=[xml]$record.xml
    [pscustomobject]@{TaskName=$record.name;TaskPath=$record.path;Xml=[string]$record.xml;
        State='Ready';Settings=[pscustomobject]@{Enabled=$true};
        Actions=@($xml.Task.Actions.ChildNodes | Where-Object {$_.NodeType -eq 'Element'} | ForEach-Object {
            [pscustomobject]@{Execute=[string]$_.Command;Arguments=[string]$_.Arguments;WorkingDirectory=[string]$_.WorkingDirectory}
        })}
})
$env:COMPUTERNAME='R4-FIXTURE-PC'
function Assert-FixtureBoundary([string]$Path) {
    $full=[IO.Path]::GetFullPath($Path)
    if($full -cne $global:RemovalTestRoot -and -not $full.StartsWith($global:RemovalTestRoot+'\',[StringComparison]::OrdinalIgnoreCase)) {
        throw 'Fixture path escaped its isolated root.'
    }
}
function Fixture-Acl {param([string]$Path,[switch]$Recursive,[string]$Purpose)
    Assert-FixtureBoundary $Path
}
function Fixture-Elevated { }
function Fixture-RunKey { return $null }
function Fixture-NoTasks { return @() }
function Fixture-NoRetirement { return $null }
# Alias precedence replaces only native/ACL/registry seams. Product definitions,
# hashes, redaction, task identity, transition and removal execute from the file.
foreach($name in @('Set-HardenedCodeAcl','Assert-HardenedCodeAcl','Assert-ProtectedCodeAncestors')) {
    Set-Alias -Name $name -Value Fixture-Acl -Scope Global
}
Set-Alias -Name Assert-AlreadyElevated -Value Fixture-Elevated -Scope Global
Set-Alias -Name Open-LabelRunKey -Value Fixture-RunKey -Scope Global
Set-Alias -Name Get-LabelSupportedStartupTasks -Value Fixture-NoTasks -Scope Global
Set-Alias -Name Get-OwnedLegacyTaskRetirementSnapshot -Value Fixture-NoRetirement -Scope Global
function Get-ScheduledTask {param($TaskName,$TaskPath,$ErrorAction)
    $global:RemovalTestEvents.Add('query')
    if($global:RemovalTestScenario -in @('query','query-sidecar-failure')) {throw [IO.IOException]::new('SYNTHETIC_NATIVE_CAUSE_409')}
    @($global:RemovalTestTasks | Where-Object {(-not $TaskName -or $_.TaskName -ieq $TaskName) -and (-not $TaskPath -or $_.TaskPath -ieq $TaskPath)})
}
function Export-ScheduledTask {param($TaskName,$TaskPath,$ErrorAction)
    $global:RemovalTestEvents.Add('export')
    if($global:RemovalTestScenario -eq 'export') {throw [IO.IOException]::new('SYNTHETIC_NATIVE_CAUSE_409')}
    [string](@($global:RemovalTestTasks | Where-Object {$_.TaskName -ieq $TaskName})[0].Xml)
}
function Disable-ScheduledTask {param($TaskName,$TaskPath,$ErrorAction)
    $global:RemovalTestEvents.Add('disable')
    foreach($task in $global:RemovalTestTasks) {
        $task.Settings.Enabled=$false
        $task.Xml=[regex]::Replace($task.Xml,'(<(?:\w+:)?Enabled>)true(</(?:\w+:)?Enabled>)','$1false$2')
    }
}
function Register-ScheduledTask {throw 'Fixture refuses task registration.'}
function Unregister-ScheduledTask {throw 'Fixture refuses task deletion.'}
function Stop-ScheduledTask {throw 'Fixture refuses task stopping.'}
function Start-Process {throw 'Fixture refuses process launch.'}
function Get-CimInstance { [pscustomobject]@{ProcessId=$PID;ExecutablePath='fixture';CommandLine='fixture'} }
function Get-ItemProperty { [pscustomobject]@{MachineGuid='R4_SYNTHETIC_MACHINE'} }
function Set-Acl {param($LiteralPath,$AclObject,$ErrorAction) Assert-FixtureBoundary $LiteralPath}
function New-Object {
    [CmdletBinding(DefaultParameterSetName='Type')]
    param([Parameter(Position=0,ParameterSetName='Type')][string]$TypeName,
        [Parameter(Position=1,ParameterSetName='Type')][object[]]$ArgumentList,
        [Parameter(ParameterSetName='Com')][string]$ComObject)
    if($PSCmdlet.ParameterSetName -eq 'Type') {return Microsoft.PowerShell.Utility\New-Object @PSBoundParameters}
    if($ComObject -cne 'Schedule.Service') {throw 'Fixture refuses an unmocked COM object.'}
    $service=[pscustomobject]@{}
    $service | Add-Member ScriptMethod Connect { }
    $service | Add-Member ScriptMethod GetFolder {
        param($Path)
        $folder=[pscustomobject]@{}
        $folder | Add-Member ScriptMethod GetTask {
            param($Name)
            $task=[pscustomobject]@{}
            $task | Add-Member ScriptMethod GetInstances {param($Flags) [pscustomobject]@{Count=0}}
            return $task
        }
        return $folder
    }
    return $service
}
function Fixture-SidecarFailure {param($Text) throw [IO.IOException]::new('Synthetic evidence IO failure.')}
if($Scenario -eq 'query-sidecar-failure') {
    Set-Alias -Name Write-KmtechProtectedFailureSidecar -Value Fixture-SidecarFailure -Scope Global
}
$entry=Join-Path $Repo 'INSTALL_THIS_PC.ps1'
$lines=[IO.File]::ReadAllLines($entry)
$line=0
for($index=0;$index -lt $lines.Length;$index++) {
    if($lines[$index] -ceq '$OperatorSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value') {$line=$index+1;break}
}
if(-not $line) {throw 'Fixture entry seam is unavailable.'}
# Redirect only the canonical literal used for admission. The production
# Uninstall flags remain unchanged, so its helper-free load branch is real.
$breakpoint=Set-PSBreakpoint -Script $entry -Line $line -Action {
    Set-Variable -Name ExpectedInstallRoot -Value (Join-Path $global:RemovalTestRoot 'current') -Scope 1
}
$arguments=@{Uninstall=$true;InstallRoot=(Join-Path $Root 'current');OperatorLocalAppDataRoot=$Root}
if($Scenario -eq 'elevation-log') {$arguments.ElevationLogPath=Join-Path $Root 'elevation.jsonl'}
try { & $entry @arguments; $entryExit=$LASTEXITCODE }
finally {
    $errors=@($global:Error | Select-Object -First 8 | ForEach-Object {
        @{exception_type=$_.Exception.GetType().FullName;command=[string]$_.InvocationInfo.MyCommand.Name;line=[int]$_.InvocationInfo.ScriptLineNumber}
    })
    [IO.File]::WriteAllText((Join-Path $Root 'adapter.json'),
        (@{events=$global:RemovalTestEvents.ToArray();enabled=@($global:RemovalTestTasks | ForEach-Object {$_.Settings.Enabled});errors=$errors} | ConvertTo-Json -Depth 5 -Compress),[Text.UTF8Encoding]::new($false))
}
exit $entryExit

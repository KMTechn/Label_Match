param([string]$Repo,[string]$Root,[string]$Scenario)
$ErrorActionPreference='Stop'
function Load-Functions($Path,$Names){
    $tokens=$null;$errors=$null;$ast=[Management.Automation.Language.Parser]::ParseFile($Path,[ref]$tokens,[ref]$errors)
    if($errors.Count){throw 'parse failed'}
    foreach($node in $ast.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst]},$false)){
        if($null -eq $Names -or $node.Name -cin $Names){. ([scriptblock]::Create($node.Extent.Text))}
    }
}
. Load-Functions (Join-Path $Repo 'tools/label_worker_shortcuts.ps1') $null
. Load-Functions (Join-Path $Repo 'INSTALL_CANONICAL_PORTABLE.ps1') @('Get-LabelManagedEmergencyPublished','Get-LabelPublishedShortcut','Assert-LabelShortcutCopyPath','Complete-LabelCanonicalWorkerShortcuts','Install-LabelMachineWorkerShortcut')
. Load-Functions (Join-Path $Repo 'tools/bootstrap_integrity.ps1') @('Get-LabelEmergencyFullPath','Assert-LabelEmergencyPath','Get-LabelEmergencyShortcutPath','Test-LabelEmergencyShortcutDirectory','Test-LabelEmergencyShortcutOwner','Install-LabelEmergencyShortcut')
function Same($a,$b){[IO.Path]::GetFullPath($a).TrimEnd('\') -ieq [IO.Path]::GetFullPath($b).TrimEnd('\')}
function Sha($Path){(Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()}
function Get-LabelWorkerShortcutFolders([switch]$CommonOnly,[switch]$UserOnly){
    foreach($name in @('CommonDesktopDirectory','CommonPrograms')){
        $path=Join-Path $Root $name;[void][IO.Directory]::CreateDirectory($path)
        [pscustomobject]@{name=$name;path=$path;recursive=$name.EndsWith('Programs')}
    }
}
$install=Join-Path $Root 'current';[void][IO.Directory]::CreateDirectory((Join-Path $install 'app/assets'))
[IO.File]::WriteAllText((Join-Path $install 'launch-label-match.cmd'),'fixture')
[void][IO.Directory]::CreateDirectory((Join-Path $install 'runtime'))
[IO.File]::WriteAllText((Join-Path $install 'runtime/pythonw.exe'),'fixture')
[IO.File]::WriteAllText((Join-Path $install 'app/main.py'),'fixture')
[IO.File]::WriteAllText((Join-Path $install 'app/assets/logo.ico'),'fixture')
$record=Join-Path $Root 'shortcuts.json';$held=$null;$caught=$null
if($Scenario -ceq 'state_failure'){
    $script:saveCount=0
    $script:originalSave=${function:Save-LabelShortcutRecord}
    function Save-LabelShortcutRecord($Path,$Record){
        $script:saveCount++
        if($script:saveCount -eq 2){$script:held=[IO.File]::Open($Path,'Open','Read','Read')}
        & $script:originalSave $Path $Record
    }
}
try{$result=Install-LabelWorkerShortcuts $install @{launcher='launch-label-match.cmd'} $record -CommonOnly}catch{$caught=$_}finally{if($script:held){$script:held.Dispose()}}
if($Scenario -ceq 'state_failure'){
    if($null -eq $caught -or $null -ne $result){throw 'state publication failure returned success'}
    $saved=Get-Content -LiteralPath $record -Raw -Encoding UTF8|ConvertFrom-Json
    if($saved.status -ceq 'INSTALLED'){throw 'failure published installed state'}
    'PASS';exit 0
}
if($caught){throw $caught}
if(@($result.published).Count -ne 2){throw 'desktop/start menu audit missing'}
$saved=Get-Content -LiteralPath $record -Raw -Encoding UTF8|ConvertFrom-Json
foreach($entry in $saved.published){
    $actual=Get-LabelPublishedShortcut $entry.location $entry.shortcut_path $entry.target_path $entry.arguments $entry.working_directory
    if(($actual|ConvertTo-Json -Compress) -cne ($entry|ConvertTo-Json -Compress)){throw 'published schema/readback differs'}
}
$emergency=Install-LabelEmergencyShortcut $install
$emergencyPath=Get-LabelEmergencyShortcutPath $install
if($Scenario -ceq 'foreign'){
    $shell=New-Object -ComObject WScript.Shell;$link=$shell.CreateShortcut($emergencyPath)
    $link.Description='foreign fixture';$link.Save()
    [void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($shell)
}
$audit=[ordered]@{shortcuts=[ordered]@{status='existing';published=@()};worker_shortcuts=[ordered]@{common_record_path=$record;user_record_path=$record;helper_path='unused';helper_sha256='unused'}}
function Invoke-LabelUserShortcutOperation($Root,$Operation,$RecordPath,$HelperPath,$ExpectedSha256){[ordered]@{published=@()}}
Complete-LabelCanonicalWorkerShortcuts $install 'NEW' $audit @{} $null $null
$expectedCount=if($Scenario -ceq 'foreign'){2}else{3}
if($audit.shortcuts.status -cne 'existing' -or @($audit.shortcuts.published).Count -ne $expectedCount){throw 'audit existing fields or locations differ'}
foreach($entry in $audit.shortcuts.published){
    $shell=New-Object -ComObject WScript.Shell;$actual=$shell.CreateShortcut($entry.shortcut_path)
    if($entry.target_path -cne $actual.TargetPath -or $entry.arguments -cne $actual.Arguments -or $entry.working_directory -cne $actual.WorkingDirectory){throw 'actual link differs'}
    [void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($shell)
}
'PASS'

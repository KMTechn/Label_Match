param([string]$Repo,[string]$Root,[string]$Scenario)
$ErrorActionPreference='Stop'
trap { [IO.File]::WriteAllText((Join-Path $Root 'failure-site.json'),(@{type=$_.Exception.GetType().Name;line=$_.InvocationInfo.ScriptLineNumber;locations=@([regex]::Matches($_.ScriptStackTrace,'line (\d+)')|ForEach-Object{[int]$_.Groups[1].Value})}|ConvertTo-Json -Compress));throw }
. (Join-Path $Repo 'tools/label_worker_shortcuts.ps1')
$productRoot=Join-Path $Root 'current';$foreignRoot=Join-Path $Root 'other'
$desktop=Join-Path $Root 'desktop';$menu=Join-Path $Root 'programs'
foreach($folder in @($productRoot,$foreignRoot,$desktop,$menu,(Join-Path $productRoot 'app/assets'))){[void][IO.Directory]::CreateDirectory($folder)}
[IO.File]::WriteAllText((Join-Path $productRoot 'launch-label-match.cmd'),'qualification')
[void][IO.Directory]::CreateDirectory((Join-Path $productRoot 'runtime'))
[IO.File]::WriteAllText((Join-Path $productRoot 'runtime/pythonw.exe'),'fixture')
[IO.File]::WriteAllText((Join-Path $productRoot 'app/main.py'),'fixture')
[IO.File]::WriteAllText((Join-Path $productRoot 'app/assets/logo.ico'),'qualification')
$other=Join-Path $foreignRoot 'other.exe';[IO.File]::WriteAllText($other,'qualification')
function Get-LabelWorkerShortcutFolders {
    param([switch]$CommonOnly,[switch]$UserOnly)
    [pscustomobject]@{name='CommonDesktopDirectory';path=$desktop;recursive=$false}
    [pscustomobject]@{name='CommonPrograms';path=$menu;recursive=$true}
}
$originals=@{};$foreign=@{}
foreach($folder in @($desktop,$menu)){
    $own=Join-Path $folder 'stale-emergency.lnk'
    Set-LabelShortcutProperties $own @{target_path=(Join-Path $productRoot 'app/gone.py');arguments='';working_directory=$productRoot;icon_location=($other+',0');window_style=7;description='';hotkey=''}
    $originals[$own]=Get-LabelShortcutSha256 $own
    $relative=if($folder -ceq $menu){'KMTech/포장.lnk'}else{'포장.lnk'}
    $unrelated=Join-Path $folder $relative
    Set-LabelShortcutProperties $unrelated @{target_path=$other;arguments='';working_directory=$foreignRoot;icon_location=($other+',0');window_style=7;description='';hotkey=''}
    $foreign[$unrelated]=Get-LabelShortcutSha256 $unrelated
}
$record=Join-Path $Root 'record.json'
foreach($attempt in @(1,2)){
    $result=Install-LabelWorkerShortcuts $productRoot @{launcher='launch-label-match.cmd'} $record -CommonOnly
    $saved=Get-Content -LiteralPath $record -Raw -Encoding UTF8|ConvertFrom-Json
    if($result.status -cne 'PASS' -or @($saved.published).Count -ne 2 -or @($saved.preserved).Count -ne 2){throw 'PUBLICATION_SCOPE_FAILED'}
    foreach($entry in @($saved.published)){
        if(-not $originals.ContainsKey($entry.shortcut_path) -or $foreign.ContainsKey($entry.shortcut_path)){throw 'UNRELATED_LINK_ADOPTED'}
        $actual=Get-LabelShortcutProperties $entry.shortcut_path
        foreach($field in @('target_path','arguments','working_directory')){
            if([string]$entry.$field -cne [string]$actual.$field){throw 'READBACK_DIFFERS'}
        }
    }
    foreach($path in $foreign.Keys){if((Get-LabelShortcutSha256 $path) -cne $foreign[$path]){throw 'FOREIGN_BYTES_CHANGED'}}
}
if($Scenario -ceq 'drift'){
    $saved=Get-Content -LiteralPath $record -Raw -Encoding UTF8|ConvertFrom-Json
    $bad=@($saved.entries)[0].path
    $actual=Get-LabelShortcutProperties $bad;$actual.arguments='changed'
    Set-LabelShortcutProperties $bad $actual
    $rejected=$false
    try{[void](Get-LabelWorkerShortcutPublished $saved)}catch{$rejected=$true}
    if(-not $rejected){throw 'OWN_READBACK_NOT_ENFORCED'}
}else{
    Restore-LabelWorkerShortcuts $productRoot $record -CommonOnly
    foreach($path in $originals.Keys){if((Get-LabelShortcutSha256 $path) -cne $originals[$path]){throw 'ORIGINAL_BYTES_NOT_RESTORED'}}
}
foreach($path in $foreign.Keys){if((Get-LabelShortcutSha256 $path) -cne $foreign[$path]){throw 'FOREIGN_BYTES_CHANGED'}}
'PASS'

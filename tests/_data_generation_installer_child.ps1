param([string]$Repo, [string]$Root, [string]$Python, [string]$Field,
      [string]$State, [string]$Spelling, [string]$Output)
$ErrorActionPreference = 'Stop'
$Script:SettingsReads = 0
$Script:LifecycleCalls = 0
$nativeJoin = Get-Command Join-Path -CommandType Cmdlet
function Join-Path {
    param([string]$Path, [string]$ChildPath)
    # The real interpreter executes the unchanged resolver; no product launcher
    # or host installation is run by this definition-only test.
    if ($ChildPath -eq 'runtime\python.exe') { return $Python }
    return & $nativeJoin $Path $ChildPath
}
function Get-Content {
    param([string]$LiteralPath, [switch]$Raw, [string]$Encoding)
    if ($LiteralPath -eq $selectedSettingsPath) { $Script:SettingsReads++ }
    return Microsoft.PowerShell.Management\Get-Content -LiteralPath $LiteralPath -Raw -Encoding UTF8
}
function Requested([string]$Path) {
    if ($Spelling -eq 'tilde') { return '~\' + $Path.Substring($env:USERPROFILE.Length + 1) }
    if ($Spelling -eq 'relative') { return $Path.Substring($Root.Length + 1) }
    return $Path
}
$installer = [IO.File]::ReadAllText((Join-Path $Repo 'INSTALL_CANONICAL_PORTABLE.ps1'))
$parseTokens = $null
$parseErrors = $null
$tree = [Management.Automation.Language.Parser]::ParseInput($installer, [ref]$parseTokens, [ref]$parseErrors)
if ($parseErrors.Count) { throw 'Installer syntax error' }
$definitions = $tree.FindAll({ param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] }, $false)
foreach ($name in @('Full','Arg','Get-LabelPortableDataVersion','Assert-LabelDataDowngrade','Resolve-LabelGenerationInputPath')) {
    $definition = @($definitions | Where-Object Name -eq $name)
    if ($definition.Count) { . ([ScriptBlock]::Create($definition[0].Extent.Text)) }
}
. (Join-Path $Repo 'kmtech_shared\powershell\portable.ps1')
. (Join-Path $Repo 'kmtech_shared\powershell\data_generation.ps1')
$lad = Join-Path $Root 'local'
$env:LOCALAPPDATA = $lad
$env:APPDATA = Join-Path $Root 'roaming'
$env:PROGRAMDATA = Join-Path $Root 'program'
$env:USERPROFILE = Join-Path $Root 'home'
$env:KMTECH_LABEL_WRITER_CONTROL_ROOT = Join-Path $Root 'writer-control'
$env:KMTECH_LABEL_WRITER_TEST_MODE = '1'
foreach ($name in @('LABEL_MATCH_SETTINGS_PATH','LABEL_MATCH_LOGISTICS_PROFILE_PATH','LABEL_MATCH_SAVE_DIR',
                   'LABEL_MATCH_DIRECT_SYNC_ROOT','LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT')) {
    [Environment]::SetEnvironmentVariable($name, '', 'Process')
}
$external = Join-Path $env:USERPROFILE 'external'
[void][IO.Directory]::CreateDirectory($external)
$settings = Join-Path $lad 'KMTech\Label_Match\config\app_settings.json'
$profile = Join-Path $lad 'KMTech\Logistics\profiles\Label_Match\runtime-profile.json'
$data = Join-Path $lad 'KMTech\Label_Match\data'
$direct = Join-Path $lad 'KMTech\DirectSync\label_match'
switch ($Field) {
    'settings' { $settings = Join-Path $external 'app_settings.json'; $env:LABEL_MATCH_SETTINGS_PATH = Requested $settings; $markerRoot = $external }
    'profile' { $profile = Join-Path $external 'runtime-profile.json'; $env:LABEL_MATCH_LOGISTICS_PROFILE_PATH = Requested $profile; $markerRoot = $external }
    'direct' { $direct = $external; $env:LABEL_MATCH_DIRECT_SYNC_ROOT = Requested $direct; $markerRoot = $external }
    'legacy-direct' { $direct = $external; $env:LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT = Requested $direct; $markerRoot = $external }
    'data' { $data = $external; $env:LABEL_MATCH_SAVE_DIR = Requested $data; $markerRoot = $external }
    'custom' { $data = $external; $markerRoot = $external }
    'default' { $markerRoot = Split-Path -Parent $settings }
    'default-profile' { $markerRoot = Split-Path -Parent $profile }
    default { throw 'Unknown isolated field' }
}
[void][IO.Directory]::CreateDirectory((Split-Path -Parent $settings))
$payload = if ($Field -eq 'custom') { @{ custom_save_path = (Requested $data) } } else { @{} }
[IO.File]::WriteAllText($settings, ($payload | ConvertTo-Json -Compress), [Text.UTF8Encoding]::new($false))
$subject = @('S',1,5,21,900,901,902,1001) -join '-'
$marker = Join-Path $markerRoot 'data-generation.json'
if ($State -ne 'absent') {
    [void][IO.Directory]::CreateDirectory($markerRoot)
    $version = if ($State -eq 'high') { 'v3.0.0' } else { 'v2.0.95' }
    $value = @{ schema_version=1; app_id='Label_Match'; subject_sid=$subject; version=$version;
               build_id=('a'*40); data_compatible_min_version=$version; compatibility_verified=$false }
    # Use the common helper's own persisted schema through the real Python API.
    $markerScript = Join-Path $Root 'marker.py'
    [IO.File]::WriteAllText($markerScript, @'
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from kmtech_shared.data_generation import record_generation
record_generation(Path(sys.argv[2]),sys.argv[3],app_id='Label_Match',subject_sid=sys.argv[4],build_id='a'*40)
'@)
    & $Python -I -B $markerScript $Repo $marker $version $subject | Out-Null
    if ($LASTEXITCODE) { throw 'Marker fixture creation failed' }
    if ($State -eq 'corrupt') { [IO.File]::WriteAllText($marker, '{broken') }
}
$source = Join-Path $Root 'packet'
$sourceManifest = [pscustomobject]@{ version='v2.0.95'; source_commit=('a'*40) }
$testMode = $true
$blockStart = $installer.IndexOf('$defaultDirectSyncRoot =')
$blockEnd = $installer.IndexOf('$before = Snapshot', $blockStart)
if ($blockStart -lt 0 -or $blockEnd -lt 0) { throw 'Installer selection boundary is missing' }
$block = $installer.Substring($blockStart, $blockEnd - $blockStart)
$block = $block.Replace('[Security.Principal.WindowsIdentity]::GetCurrent().User.Value', '$subject')
$before = @{}
foreach ($file in @(Get-ChildItem -LiteralPath $Root -File -Recurse)) { $before[$file.FullName] = (Get-FileHash -LiteralPath $file.FullName).Hash }
Set-Location -LiteralPath $Root
$refused = $false
$guidance = $false
try { . ([ScriptBlock]::Create($block)) }
catch { $refused=$true; $guidance = $_.Exception.Message.Contains('자료') -and $_.Exception.Message.Contains('복구') }
$changed = 0
foreach ($path in $before.Keys) {
    if (-not (Test-Path -LiteralPath $path -PathType Leaf) -or (Get-FileHash -LiteralPath $path).Hash -ne $before[$path]) { $changed++ }
}
$added = @(Get-ChildItem -LiteralPath $Root -File -Recurse | Where-Object { -not $before.ContainsKey($_.FullName) }).Count
$samePaths = $false
$cwdMatches = $false
if (-not $refused) {
    $childScript = Join-Path $Root 'reader.py'
    $childOutput = Join-Path $Root 'reader.json'
    [IO.File]::WriteAllText($childScript, @'
import json,os,sys
from pathlib import Path
sys.path.insert(0,str(Path(sys.argv[1])/'app'))
from current_user_onboarding import resolve_current_user_onboarding_paths
p=resolve_current_user_onboarding_paths(Path(sys.argv[1])/'app')
Path(sys.argv[2]).write_text(json.dumps({'cwd':os.getcwd(),'settings':str(p.settings_path),'profile':str(p.logistics_profile_path),'direct':str(p.direct_sync_root),'data':str(p.data_root)}))
'@)
    $line = '-I -B ' + (Arg $childScript) + ' ' + (Arg $source) + ' ' + (Arg $childOutput)
    $child = Start-Process -FilePath $Python -ArgumentList $line -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput (Join-Path $Root 'reader-stdout.log') `
        -RedirectStandardError (Join-Path $Root 'reader-stderr.log')
    Wait-Process -InputObject $child
    $null = $child.HasExited
    if ($child.ExitCode -ne 0) { throw 'Isolated resolver child failed' }
    $selected = [IO.File]::ReadAllText($childOutput) | ConvertFrom-Json
    $cwdMatches = $selected.cwd.Equals($Root, 'OrdinalIgnoreCase')
    $samePaths = $selected.settings.Equals($selectedSettingsPath,'OrdinalIgnoreCase') -and
        $selected.profile.Equals($selectedProfilePath,'OrdinalIgnoreCase') -and
        $selected.direct.Equals($selectedDirectSyncRoot,'OrdinalIgnoreCase') -and
        $selected.data.Equals($generationDataRoot,'OrdinalIgnoreCase')
}
$result = @{ refused=$refused; guidance=$guidance; settings_reads=$Script:SettingsReads;
             lifecycle_calls=$Script:LifecycleCalls; changed=$changed; added=$added;
             parent_child_same_paths=$samePaths; child_cwd_matches=$cwdMatches;
             ps_version=$PSVersionTable.PSVersion.ToString() }
[IO.File]::WriteAllText($Output, ($result | ConvertTo-Json -Compress), [Text.UTF8Encoding]::new($false))

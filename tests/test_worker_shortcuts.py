"""Real isolated .lnk files; product/Explorer/installer execution is excluded."""
from pathlib import Path
import hashlib

import pytest

from tests.test_shared_ancestor_protection import ROOT, run_functions


HELPER = ROOT / "tools/label_worker_shortcuts.ps1"
NAMES = [
    "Copy-LabelAtomicStream", "Copy-LabelAtomicFile", "Get-LabelWorkerShortcutPublished",
    "Get-LabelWorkerShortcutFolders", "Assert-LabelShortcutPath", "Get-LabelShortcutSha256",
    "Get-LabelShortcutProperties", "Set-LabelShortcutProperties",
    "Save-LabelShortcutRecord", "Get-LabelWorkerShortcutFiles",
    "Test-LabelShortcutProperties", "Install-LabelWorkerShortcuts",
    "Restore-LabelWorkerShortcuts",
]


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("case", ["broken", "broken_forward_path", "absent", "collision", "write_failure", "drift", "record_scope", "record_path"])
def test_worker_shortcuts_roundtrip_and_ownership(tmp_path, engine, case):
    result = run_functions(tmp_path, HELPER, NAMES, r'''
$root=$env:LM_SHORTCUT_ROOT;$case=$env:LM_SHORTCUT_CASE
$script:fixtureRoot=$root
function Get-FileHash {param($LiteralPath)
 $digest=[Security.Cryptography.SHA256]::Create()
 try{[pscustomobject]@{Hash=([BitConverter]::ToString($digest.ComputeHash([IO.File]::ReadAllBytes($LiteralPath)))).Replace('-','').ToLowerInvariant()}}
 finally{$digest.Dispose()}
}
$install=Join-Path $root 'Apps/Label_Match/current'
$record=Join-Path $root 'LOCALAPPDATA/install-audit/shortcuts.json'
if($case -eq 'broken_forward_path'){$record=$record.Replace('\','/')}
$script:fixtureFolderNames=@('Desktop','Programs','CommonDesktop','CommonPrograms')
foreach($name in $script:fixtureFolderNames){[void][IO.Directory]::CreateDirectory((Join-Path $root $name))}
function Get-LabelWorkerShortcutFolders {param([switch]$CommonOnly,[switch]$UserOnly)
 foreach($name in $script:fixtureFolderNames){if((-not $CommonOnly -or $name.StartsWith('Common')) -and
                             (-not $UserOnly -or -not $name.StartsWith('Common'))){
  [pscustomobject]@{name=$name;path=(Join-Path $script:fixtureRoot $name);recursive=$name.EndsWith('Programs')}
 }}
}
[void][IO.Directory]::CreateDirectory((Join-Path $install 'app/assets'))
[IO.File]::WriteAllText((Join-Path $install 'launch-label-match.cmd'),'never execute')
[void][IO.Directory]::CreateDirectory((Join-Path $install 'runtime'))
[IO.File]::WriteAllText((Join-Path $install 'runtime/pythonw.exe'),'never execute')
[IO.File]::WriteAllText((Join-Path $install 'app/main.py'),'never execute')
[IO.File]::WriteAllBytes((Join-Path $install 'app/assets/logo.ico'),[byte[]]@(0))
$manifest=[pscustomobject]@{launcher='launch-label-match.cmd'}
$shell=New-Object -ComObject WScript.Shell
$originals=@{}
function Make-Link($path,$target,$argsValue){
 [void][IO.Directory]::CreateDirectory((Split-Path -Parent $path))
 $link=$shell.CreateShortcut($path);$link.TargetPath=$target;$link.Arguments=$argsValue
 $link.WorkingDirectory=$root;$link.IconLocation=(Join-Path $root 'old.ico')+',3'
 $link.WindowStyle=3;$link.Description='original';$link.Hotkey='CTRL+ALT+F12';$link.Save()
 $originals[$path]=(Get-FileHash -LiteralPath $path).Hash
}
$other=Join-Path $root 'Desktop/other.lnk'
Make-Link $other (Join-Path $root 'Apps/Container_Audit/current/Container_Audit.exe') ''
$otherRoot=Join-Path $root 'Desktop/other-root.lnk'
Make-Link $otherRoot (Join-Path $root 'AnotherRoot/Label_Match/current/Label_Match.exe') ''
$healthyTarget=Join-Path $install 'existing/Label_Match.exe'
[void][IO.Directory]::CreateDirectory((Split-Path -Parent $healthyTarget));[IO.File]::WriteAllText($healthyTarget,'old')
$healthy=Join-Path $root 'Programs/healthy.lnk';Make-Link $healthy $healthyTarget ''
if($case -in @('broken','broken_forward_path','drift')){
 foreach($name in $script:fixtureFolderNames){
  $path=Join-Path $root ($name+'/KMTech/submenu/old.lnk')
  if($name.EndsWith('Desktop')){$path=Join-Path $root ($name+'/old.lnk')}
  Make-Link $path (Join-Path $install 'Label_Match.exe') '--old-argument'
 }
}
if($case -eq 'collision'){
 $collision=Join-Path $root 'CommonDesktop/포장.lnk'
 Make-Link $collision (Join-Path $root 'another.exe') ''
}
if($case -eq 'write_failure'){
 $setter=${function:Set-LabelShortcutProperties};$script:saves=0
 function Set-LabelShortcutProperties {param($Path,$Properties)
  $script:saves++;if($script:saves -eq 2){throw 'injected save failure'}
  & $setter $Path $Properties
 }
}
$failed=$false
try{$proof=Install-LabelWorkerShortcuts $install $manifest $record}catch{$failed=$true;$failure=$_}
if($case -eq 'write_failure'){
 if(-not $failed){throw 'Expected safe refusal'}
 foreach($path in $originals.Keys){if((Get-FileHash -LiteralPath $path).Hash -ne $originals[$path]){throw 'Original link changed on refusal'}}
 if($case -eq 'write_failure' -and (Test-Path -LiteralPath (Join-Path $root 'CommonDesktop/포장.lnk'))){throw 'Partial new link survived'}
 'PASS';return
}
if($failed){throw $failure}
$journal=Get-Content -LiteralPath $record -Raw -Encoding UTF8|ConvertFrom-Json
$wantedCount=if($case -in @('broken','broken_forward_path','drift')){4}elseif($case -eq 'collision'){1}else{2}
if(@($journal.entries).Count -ne $wantedCount){throw 'Wrong owned link count'}
foreach($entry in $journal.entries){
 $props=Get-LabelShortcutProperties $entry.path
 if($props.target_path -ne (Join-Path $install 'runtime/pythonw.exe') -or $props.arguments -cne ('-I -B "'+(Join-Path $install 'app/main.py')+'"') -or
    $props.window_style -ne 1 -or $props.icon_location -ne ((Join-Path $install 'app/assets/logo.ico')+',0')){throw 'Installed shortcut mismatch'}
 if($null -ne $entry.original -and ($props.description -cne $entry.original.description -or
    $props.hotkey -cne $entry.original.hotkey)){throw 'Unrequested shortcut properties changed'}
}
foreach($path in @($other,$otherRoot,$healthy)){if((Get-FileHash -LiteralPath $path).Hash -ne $originals[$path]){throw 'Unrelated/healthy link changed'}}
$journalSha=(Get-FileHash -LiteralPath $record).Hash
[void](Install-LabelWorkerShortcuts $install $manifest $record)
if((Get-FileHash -LiteralPath $record).Hash -ne $journalSha){throw 'Repeat install lost preimage'}
if($case -in @('record_scope','record_path')){
 $ownedHashes=@{}
 foreach($entry in $journal.entries){$ownedHashes[$entry.path]=(Get-FileHash -LiteralPath $entry.path).Hash}
 if($case -eq 'record_scope'){$journal.common_only=$true}
 else{$journal.entries[0].path=Join-Path $root 'CommonDesktop/../outside.lnk'}
 Save-LabelShortcutRecord $record $journal
 $failed=$false;try{Restore-LabelWorkerShortcuts $install $record}catch{$failed=$true}
 if(-not $failed){throw 'Changed restoration boundary was admitted'}
 foreach($path in $ownedHashes.Keys){if((Get-FileHash -LiteralPath $path).Hash -cne $ownedHashes[$path]){throw 'Refused restoration changed a link'}}
 'PASS';return
}
if($case -eq 'drift'){
 $link=$shell.CreateShortcut($journal.entries[0].path);$link.Arguments='--changed';$link.Save()
 $failed=$false;try{Restore-LabelWorkerShortcuts $install $record}catch{$failed=$true}
 if(-not $failed){throw 'Drifted link was overwritten'}
 'PASS';return
}
# Both rollback and uninstall use this same restoration boundary after old code restore.
Restore-LabelWorkerShortcuts $install $record
Restore-LabelWorkerShortcuts $install $record
foreach($path in $originals.Keys){if((Get-FileHash -LiteralPath $path).Hash -ne $originals[$path]){throw 'Original link bytes not restored'}}
if($case -eq 'absent'){
 foreach($path in @('CommonDesktop/포장.lnk','CommonPrograms/KMTech/포장.lnk')){
  if(Test-Path -LiteralPath (Join-Path $root $path)){throw 'Created link survived restore'}
 }
}
'PASS'
''', engine=engine, values={"LM_SHORTCUT_ROOT": str(tmp_path), "LM_SHORTCUT_CASE": case})
    assert result.returncode == 0, result.stdout + result.stderr
    assert "PASS" in result.stdout


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("removal", ["Rollback", "Uninstall"])
@pytest.mark.parametrize("placement", ["PASS", "REUSED_VERIFIED"])
def test_machine_and_worker_entrypoints_share_reversible_record(tmp_path, engine, removal, placement):
    helper = tmp_path / "isolated-helper.ps1"
    # Replace only the known-folder boundary before invoking the real script body.
    text = HELPER.read_text(encoding="utf-8-sig")
    override = r'''
function Get-LabelWorkerShortcutFolders {param([switch]$CommonOnly,[switch]$UserOnly)
 foreach($name in @('Desktop','Programs','CommonDesktop','CommonPrograms')){
  if(($CommonOnly -and -not $name.StartsWith('Common')) -or ($UserOnly -and $name.StartsWith('Common'))){continue}
  [pscustomobject]@{name=$name;path=(Join-Path $env:LM_SHORTCUT_ROOT $name);recursive=$name.EndsWith('Programs')}
 }
}
'''
    text = text.replace("if ($Install -or $Rollback -or $Uninstall)", override + "\nif ($Install -or $Rollback -or $Uninstall)", 1)
    helper.write_text(text, encoding="utf-8-sig", newline="\n")
    pin = hashlib.sha256(helper.read_bytes()).hexdigest()
    result = run_functions(tmp_path, ROOT / "INSTALL_THIS_PC.ps1",
                           ["Assert-NoReparsePoint", "Invoke-LabelWorkerShortcutOperation"], r'''
$root=$env:LM_SHORTCUT_ROOT;$install=Join-Path $root 'Apps/Label_Match/current'
$commonRecord=Join-Path $root 'LOCALAPPDATA/install-audit/common.json'
$userRecord=Join-Path $root 'LOCALAPPDATA/install-audit/user.json'
foreach($name in @('Desktop','Programs','CommonDesktop','CommonPrograms','Apps/Label_Match/current/app/assets')){
 [void][IO.Directory]::CreateDirectory((Join-Path $root $name))
}
[IO.File]::WriteAllText((Join-Path $install 'launch-label-match.cmd'),'never execute')
[void][IO.Directory]::CreateDirectory((Join-Path $install 'runtime'))
[IO.File]::WriteAllText((Join-Path $install 'runtime/pythonw.exe'),'never execute')
[IO.File]::WriteAllText((Join-Path $install 'app/main.py'),'never execute')
[IO.File]::WriteAllBytes((Join-Path $install 'app/assets/logo.ico'),[byte[]]@(0))
[IO.File]::WriteAllText((Join-Path $install 'portable-manifest.json'),'{'+'"launcher":"launch-label-match.cmd"'+'}')
$private=Join-Path $root 'Desktop/old.lnk'
$shell=New-Object -ComObject WScript.Shell;$link=$shell.CreateShortcut($private)
$link.TargetPath=Join-Path $install 'Label_Match.exe';$link.Save()
$original=[IO.File]::ReadAllBytes($private)
if($env:LM_SHORTCUT_PLACEMENT -ceq 'PASS'){
 Invoke-LabelWorkerShortcutOperation $install 'Install' $commonRecord $env:LM_SHORTCUT_HELPER $env:LM_SHORTCUT_PIN
}
if($shell.CreateShortcut($private).TargetPath -ne (Join-Path $install 'Label_Match.exe')){throw 'Machine step touched a private link'}
$canonical=[Management.Automation.Language.Parser]::ParseFile($env:LM_SHORTCUT_CANONICAL,[ref]$tokens,[ref]$errors)
foreach($name in @('Get-LabelPublishedShortcut','Assert-LabelShortcutCopyPath','Get-LabelManagedEmergencyPublished','Complete-LabelCanonicalWorkerShortcuts','Invoke-LabelUserShortcutOperation','PinnedFileBytes','ByteSha')){
 $definition=@($canonical.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $name},$true))
 . ([scriptblock]::Create($definition[0].Extent.Text))
}
$script:commonCalls=0
function Invoke-LabelDelegatedPlacementHelper {param($frozen,$parameters,$authority,[switch]$RefreshDelegation)
 if(-not $RefreshDelegation -or $parameters.ReplaceExistingVerifiedPortable){throw 'Reuse placement parameters differ'}
 $script:commonCalls++
 [void](Invoke-LabelWorkerShortcutOperation $install $parameters.WorkerShortcutOperation $parameters.WorkerShortcutRecordPath `
   $parameters.VerifiedWorkerShortcutHelperPath $parameters.ExpectedWorkerShortcutHelperSha256)
 return 0
}
function Sha($path){return ByteSha ([IO.File]::ReadAllBytes($path))}
$shortcuts=[ordered]@{common_record_path=$commonRecord;user_record_path=$userRecord;
 helper_path=$env:LM_SHORTCUT_HELPER;helper_sha256=$env:LM_SHORTCUT_PIN}
$audit=[ordered]@{worker_shortcuts=$shortcuts}
$parameters=@{WorkerShortcutRecordPath=$commonRecord;VerifiedWorkerShortcutHelperPath=$env:LM_SHORTCUT_HELPER;
 ExpectedWorkerShortcutHelperSha256=$env:LM_SHORTCUT_PIN;ReplaceExistingVerifiedPortable=$true}
Complete-LabelCanonicalWorkerShortcuts $install $env:LM_SHORTCUT_PLACEMENT $audit $parameters $null $null
$expectedCalls=if($env:LM_SHORTCUT_PLACEMENT -ceq 'REUSED_VERIFIED'){1}else{0}
if($script:commonCalls -ne $expectedCalls -or -not $parameters.ReplaceExistingVerifiedPortable){throw 'Common reuse boundary differs'}
if($shortcuts.user_readback.status -cne 'PASS' -or $shortcuts.common_record_sha256 -cne (Sha $commonRecord) -or
 $shortcuts.user_record_sha256 -cne (Sha $userRecord)){throw 'Canonical receipt readback differs'}
if($shell.CreateShortcut($private).TargetPath -ne (Join-Path $install 'runtime/pythonw.exe')){throw 'Worker step missed private link'}
$operation=@{InstallRoot=$install;RecordPath=$userRecord;UserOnly=$true}
$operation[$env:LM_SHORTCUT_REMOVAL]=$true
& $env:LM_SHORTCUT_HELPER @operation
Invoke-LabelWorkerShortcutOperation $install 'Restore' $commonRecord $env:LM_SHORTCUT_HELPER $env:LM_SHORTCUT_PIN
if([BitConverter]::ToString([IO.File]::ReadAllBytes($private)) -cne [BitConverter]::ToString($original)){throw 'Private preimage bytes differ'}
foreach($path in @('CommonDesktop/포장.lnk','CommonPrograms/KMTech/포장.lnk')){
 if(Test-Path -LiteralPath (Join-Path $root $path)){throw 'Common link survived restore'}
}
# One-byte tampering must be rejected before the helper can run.
[IO.File]::AppendAllText($env:LM_SHORTCUT_HELPER,"`n# changed")
$refused=$false
try{Invoke-LabelWorkerShortcutOperation $install 'Install' $commonRecord $env:LM_SHORTCUT_HELPER $env:LM_SHORTCUT_PIN}catch{$refused=$true}
if(-not $refused){throw 'Changed shortcut helper was admitted'}
'PASS'
''', engine=engine, values={"LM_SHORTCUT_ROOT": str(tmp_path), "LM_SHORTCUT_HELPER": str(helper),
                           "LM_SHORTCUT_PIN": pin, "LM_SHORTCUT_CANONICAL": str(ROOT / "INSTALL_CANONICAL_PORTABLE.ps1"),
                           "LM_SHORTCUT_REMOVAL": removal, "LM_SHORTCUT_PLACEMENT": placement})
    assert result.returncode == 0, result.stdout + result.stderr
    assert "PASS" in result.stdout

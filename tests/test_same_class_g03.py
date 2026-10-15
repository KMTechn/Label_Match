"""G03: inspect actual isolated shortcut publication without launching its target."""
import json
import os
import re
from pathlib import Path

import pytest

from tests.test_shared_ancestor_protection import run_functions
from tests.test_worker_shortcuts import HELPER, NAMES


def test_worker_shortcut_targets_windowless_runtime_directly(tmp_path):
    result = run_functions(tmp_path, HELPER, NAMES, r'''
$script:fixtureRoot=$env:LM_G03_ROOT
$root=Join-Path $script:fixtureRoot 'Apps/Label_Match/current'
$desktop=Join-Path $script:fixtureRoot 'Desktop'
$record=Join-Path $script:fixtureRoot 'audit/shortcuts.json'
foreach($folder in @($desktop,(Join-Path $root 'app/assets'),(Join-Path $root 'runtime'),(Split-Path -Parent $record))){[void][IO.Directory]::CreateDirectory($folder)}
foreach($file in @('launch-label-match.cmd','app/main.py','runtime/pythonw.exe','app/assets/logo.ico')){[IO.File]::WriteAllText((Join-Path $root $file),'inert fixture; never execute')}
function Get-LabelWorkerShortcutFolders {param([switch]$CommonOnly,[switch]$UserOnly)
 [pscustomobject]@{name='CommonDesktop';path=(Join-Path $script:fixtureRoot 'Desktop');recursive=$false}
}
function Get-FileHash {param($LiteralPath)
 $algorithm=[Security.Cryptography.SHA256]::Create()
 try{[pscustomobject]@{Hash=([BitConverter]::ToString($algorithm.ComputeHash([IO.File]::ReadAllBytes($LiteralPath)))).Replace('-','')}}finally{$algorithm.Dispose()}
}
$published=Install-LabelWorkerShortcuts $root ([pscustomobject]@{launcher='launch-label-match.cmd'}) $record
$paths=@($published.published)
if($paths.Count -ne 1){throw 'isolated shortcut was not published'}
$actual=Get-LabelShortcutProperties $paths[0].shortcut_path
[ordered]@{status=$published.status;target=$actual.target_path;arguments=$actual.arguments}|ConvertTo-Json -Compress
''', values={"LM_G03_ROOT": str(tmp_path)})
    evidence = Path(os.environ.get("KMTECH_TEST_CA_EVIDENCE_ROOT") or tmp_path)
    if result.returncode:
        (evidence / "g03-harness.json").write_text(json.dumps({"rc": result.returncode,
            "error_ids": re.findall(r"FullyQualifiedErrorId\s*:\s*([^\r\n]+)", result.stderr)}), encoding="utf-8")
    assert result.returncode == 0, "isolated shortcut publication harness failed"
    observed = json.loads(result.stdout)
    (evidence / "g03-observation.json").write_text(json.dumps(observed), encoding="utf-8")
    assert observed["status"] == "PASS"
    assert Path(observed["target"]).name.lower() == "pythonw.exe", observed
    assert "app" in observed["arguments"] and "main.py" in observed["arguments"]


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("case", ["unrecorded", "recorded", "restore_old", "drift", "preimage_drift", "write_failure"])
def test_legacy_worker_shortcuts_keep_preimages_and_foreign_links(tmp_path, engine, case):
    result = run_functions(tmp_path, HELPER, NAMES, r'''
$fixture=$env:LM_G03_ROOT;$case=$env:LM_G03_CASE
$script:fixtureRoot=$fixture
$root=Join-Path $fixture 'Apps/Label_Match/current';$recordPath=Join-Path $fixture 'audit/shortcuts.json'
foreach($folder in @('Desktop','CommonDesktop','CommonPrograms','Apps/Label_Match/current/app/assets','Apps/Label_Match/current/runtime','audit')){
 [void][IO.Directory]::CreateDirectory((Join-Path $fixture $folder))
}
foreach($name in @('launch-label-match.cmd','app/main.py','runtime/pythonw.exe','app/assets/logo.ico')){
 [IO.File]::WriteAllText((Join-Path $root $name),'inert; never execute')
}
function Get-LabelWorkerShortcutFolders {param([switch]$CommonOnly,[switch]$UserOnly)
 foreach($name in @('Desktop','CommonDesktop','CommonPrograms')){
  [pscustomobject]@{name=$name;path=(Join-Path $script:fixtureRoot $name);recursive=$name.EndsWith('Programs')}
 }
}
$legacy=[ordered]@{target_path=(Join-Path $root 'launch-label-match.cmd');arguments='';working_directory=$root;
 icon_location=((Join-Path $root 'app/assets/logo.ico')+',0');window_style=7;description='original';hotkey='CTRL+ALT+F12'}
$foreign=Join-Path $fixture 'CommonDesktop/포장.lnk'
$foreignProps=@{};foreach($key in $legacy.Keys){$foreignProps[$key]=$legacy[$key]};$foreignProps.arguments='--foreign'
Set-LabelShortcutProperties $foreign $foreignProps
$foreignHash=Get-LabelShortcutSha256 $foreign
$record=[ordered]@{schema='label-match-worker-shortcuts-v1';install_root=$root;common_only=$false;user_only=$false;
 status='INSTALLED';expected=$legacy;entries=@()}
$originals=@{}
foreach($relative in @('Desktop/old.lnk','CommonPrograms/KMTech/old.lnk')){
 $path=Join-Path $fixture $relative
 if($case -ne 'unrecorded'){
  $before=@{};foreach($key in $legacy.Keys){$before[$key]=$legacy[$key]}
  $before.target_path=Join-Path $root 'missing-old.exe';$before.arguments='--original'
  Set-LabelShortcutProperties $path $before
  $originals[$path]=Get-LabelShortcutSha256 $path
  $backup=Join-Path ($recordPath+'.preimages') ($record.entries.Count.ToString()+'.lnk')
  [void][IO.Directory]::CreateDirectory((Split-Path -Parent $backup));[IO.File]::Copy($path,$backup)
  $entry=[ordered]@{path=$path;original=(Get-LabelShortcutProperties $path);original_sha256=$originals[$path];
   backup_path=$backup;installed_sha256='';restored=$false}
 }
 Set-LabelShortcutProperties $path $legacy
 $legacy.hotkey=(Get-LabelShortcutProperties $path).hotkey
 if($case -eq 'unrecorded'){$originals[$path]=Get-LabelShortcutSha256 $path}
 else{$entry.installed_sha256=Get-LabelShortcutSha256 $path;$record.entries+=$entry}
}
if($case -ne 'unrecorded'){Save-LabelShortcutRecord $recordPath $record}
if($case -eq 'restore_old'){
 Restore-LabelWorkerShortcuts $root $recordPath
}elseif($case -in @('drift','preimage_drift')){
 if($case -eq 'drift'){
  $changed=Get-LabelShortcutProperties $record.entries[0].path;$changed.arguments='--changed-by-user'
  Set-LabelShortcutProperties $record.entries[0].path $changed
 }else{[IO.File]::AppendAllText($record.entries[0].backup_path,'changed preimage')}
 $before=@{};foreach($entry in $record.entries){$before[$entry.path]=Get-LabelShortcutSha256 $entry.path}
 $rejected=$false;try{Install-LabelWorkerShortcuts $root @{launcher='launch-label-match.cmd'} $recordPath}catch{$rejected=$true}
 if(-not $rejected){throw 'Drifted legacy record was admitted'}
 foreach($path in $before.Keys){if((Get-LabelShortcutSha256 $path) -cne $before[$path]){throw 'Drift refusal changed a link'}}
}else{
 if($case -eq 'write_failure'){
  $script:setter=${function:Set-LabelShortcutProperties};$script:saveCount=0
  function Set-LabelShortcutProperties($Path,$Properties){
   $script:saveCount++;if($script:saveCount -eq 2){throw 'injected migration publication failure'}
   & $script:setter $Path $Properties
  }
 }
 $rejected=$false;try{$published=Install-LabelWorkerShortcuts $root @{launcher='launch-label-match.cmd'} $recordPath}catch{$rejected=$true;[Console]::Error.WriteLine('MIGRATION_LINE='+$_.InvocationInfo.ScriptLineNumber)}
 if($case -eq 'write_failure'){
  if(-not $rejected){throw 'Migration failure was not exercised'}
 }else{
  if($rejected){throw 'Legacy migration was refused'}
  if(@($published.published).Count -ne 2){throw 'Legacy links were not both published'}
  foreach($path in $originals.Keys){
   $actual=Get-LabelShortcutProperties $path
   if($actual.target_path -cne (Join-Path $root 'runtime/pythonw.exe') -or
      $actual.arguments -cne ('-I -B "'+(Join-Path $root 'app/main.py')+'"') -or
      $actual.working_directory -cne $root -or $actual.icon_location -cne $legacy.icon_location -or
      $actual.description -cne $legacy.description -or $actual.hotkey -cne $legacy.hotkey){throw 'Windowless shortcut binding differs'}
  }
  $recordHash=Get-LabelShortcutSha256 $recordPath
  [void](Install-LabelWorkerShortcuts $root @{launcher='launch-label-match.cmd'} $recordPath)
  if((Get-LabelShortcutSha256 $recordPath) -cne $recordHash){throw 'Repeat migration changed the journal'}
  Restore-LabelWorkerShortcuts $root $recordPath
 }
}
if($case -notin @('drift','preimage_drift')){
 foreach($path in $originals.Keys){if((Get-LabelShortcutSha256 $path) -cne $originals[$path]){throw 'Original preimage was not restored'}}
}
if((Get-LabelShortcutSha256 $foreign) -cne $foreignHash){throw 'Foreign public link changed'}
'PASS'
''', engine=engine, values={"LM_G03_ROOT": str(tmp_path), "LM_G03_CASE": case})
    failures = [code for code in ("Legacy migration was refused", "Legacy links were not both published",
        "Windowless shortcut binding differs", "Original preimage was not restored", "Repeat migration changed the journal")
        if code in result.stderr]
    if result.returncode:
        evidence = Path(os.environ.get("KMTECH_TEST_CA_EVIDENCE_ROOT") or tmp_path)
        (evidence / ("g03-" + case + "-" + engine + ".json")).write_text(json.dumps({
            "failures": failures, "lines": re.findall(r"(?:MIGRATION_LINE=|:)(\d+)(?:\b| char)", result.stderr)}), encoding="utf-8")
    assert result.returncode == 0, (failures, re.findall(r"(?:MIGRATION_LINE=|:)(\d+)(?:\b| char)", result.stderr))
    assert result.stdout.strip().endswith("PASS")


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
def test_shortcut_publication_failure_keeps_complete_previous_link(tmp_path, engine):
    result = run_functions(tmp_path, HELPER, NAMES, r'''
$root=$env:LM_G03_ROOT;$path=Join-Path $root 'entry.lnk'
$properties=@{target_path=(Join-Path $root 'old.exe');arguments='--old';working_directory=$root;
 icon_location=((Join-Path $root 'logo.ico')+',0');window_style=1;description='original';hotkey=''}
Set-LabelShortcutProperties $path $properties
$before=Get-LabelShortcutSha256 $path
function Copy-LabelAtomicStream($SourceStream,$DestinationStream){
 $DestinationStream.WriteByte(0);$DestinationStream.Flush($true);throw 'injected interrupted copy'
}
$properties.target_path=Join-Path $root 'runtime/pythonw.exe'
$rejected=$false;try{Set-LabelShortcutProperties $path $properties}catch{$rejected=$true}
if(-not $rejected -or (Get-LabelShortcutSha256 $path) -cne $before){throw 'Interrupted publication changed the live link'}
$actual=Get-LabelShortcutProperties $path
if($actual.target_path -cne (Join-Path $root 'old.exe') -or $actual.arguments -cne '--old'){throw 'Prior shortcut is incomplete'}
'PASS'
''', engine=engine, values={"LM_G03_ROOT": str(tmp_path)})
    assert result.returncode == 0, "isolated interrupted shortcut publication failed"
    assert result.stdout.strip().endswith("PASS")

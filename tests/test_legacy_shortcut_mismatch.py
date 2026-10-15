"""P1 review reproduction; real isolated COM links, no product process.

Use pytest --basetemp E:/KMTech/w9firstupd-tmp/sub239/repro-tmp.
The reviewed repository must be importable so tests.test_worker_shortcuts resolves
from its isolated, pinned checkout. No product process or visible window is started.
"""
from pathlib import Path
import json
import os
import re
import subprocess

import pytest

from tests.test_shared_ancestor_protection import run_functions
from tests.test_worker_shortcuts import NAMES

from tests.test_worker_shortcuts import HELPER


_SCRIPT = r"""
$ErrorActionPreference = 'Stop'
. '__HELPER__'
$root = Join-Path $PSScriptRoot 'installed'
$script:ReviewDesktop = Join-Path $PSScriptRoot 'desktop'
$script:ReviewPrograms = Join-Path $PSScriptRoot 'programs'
foreach ($directory in @($root,(Join-Path $root 'runtime'),(Join-Path $root 'app/assets'),$script:ReviewDesktop,$script:ReviewPrograms)) {
    [void][IO.Directory]::CreateDirectory($directory)
}
foreach ($relative in @('runtime/pythonw.exe','app/main.py','app/assets/logo.ico','launch-label-match.cmd')) {
    [IO.File]::WriteAllText((Join-Path $root $relative),'fixture')
}
function Get-LabelWorkerShortcutFolders([switch]$CommonOnly,[switch]$UserOnly) {
    [pscustomobject]@{name='Desktop';path=$script:ReviewDesktop;recursive=$false}
    [pscustomobject]@{name='Programs';path=$script:ReviewPrograms;recursive=$true}
}
$first = Join-Path $script:ReviewDesktop 'primary.lnk'
$second = Join-Path $script:ReviewPrograms 'secondary.lnk'
$recordPath = Join-Path $PSScriptRoot 'shortcuts.json'
$legacy = [ordered]@{
    target_path=(Join-Path $root 'launch-label-match.cmd');arguments='';working_directory=$root
    icon_location=((Join-Path $root 'app/assets/logo.ico')+',0');window_style=7;description='포장';hotkey=''
}
$manifest = [pscustomobject]@{launcher='launch-label-match.cmd'}
if ('__CASE__' -ceq 'recorded_drift') {
    Set-LabelShortcutProperties $first $legacy
    Set-LabelShortcutProperties $second $legacy
    $entries = @()
    foreach ($file in @($first,$second)) {
        $entries += [ordered]@{path=$file;original=$null;original_sha256='';backup_path='';
            installed_sha256=(Get-LabelShortcutSha256 $file);restored=$false}
    }
    $record = [ordered]@{schema='label-match-worker-shortcuts-v1';install_root=$root;
        common_only=$false;user_only=$true;status='INSTALLED';expected=$legacy;entries=$entries}
    Save-LabelShortcutRecord $recordPath $record
    $changed = [ordered]@{}; foreach ($key in $legacy.Keys) { $changed[$key] = $legacy[$key] }
    $changed.arguments = '--ui tk'
    Set-LabelShortcutProperties $first $changed
    $before = Get-LabelShortcutSha256 $first
    # Real installer: a changed link must be preserved while the other link migrates.
    $result = Install-LabelWorkerShortcuts $root $manifest $recordPath -UserOnly
    if ($result.status -cne 'PASS') { throw 'Expected nonblocking installation.' }
    if ((Get-LabelShortcutSha256 $first) -cne $before) { throw 'Changed link was overwritten.' }
    if ((Get-LabelShortcutProperties $second).target_path -cne (Join-Path $root 'runtime/pythonw.exe')) {
        throw 'Unchanged legacy link did not migrate.'
    }
}
Write-Output 'PASS'
"""


def _run_case(tmp_path: Path, case: str) -> None:
    allowed = Path('E:/KMTech/w9firstupd-tmp/sub239').resolve()
    assert tmp_path.resolve().is_relative_to(allowed), 'Use the required isolated basetemp.'
    helper = str(HELPER.resolve()).replace("'", "''")
    script = _SCRIPT.replace('__HELPER__', helper).replace('__CASE__', case)
    script_path = tmp_path / 'review-case.ps1'
    script_path.write_text(script, encoding='utf-8-sig')
    result = subprocess.run(
        ['powershell.exe', '-NoLogo', '-NoProfile', '-NonInteractive', '-WindowStyle', 'Hidden',
         '-ExecutionPolicy', 'Bypass', '-File', str(script_path)],
        cwd=tmp_path, capture_output=True, text=True, encoding='utf-8', errors='replace',
        creationflags=0x08000000,
    )
    if result.returncode:
        messages = ['Installed worker shortcut drifted.', 'Recorded shortcut escaped its known folder.',
            'Expected nonblocking installation.', 'Unchanged legacy link did not migrate.',
            'Changed link was overwritten.', 'Shortcut preimage is unproven.',
            'Published shortcut readback differs.', 'Prepared worker shortcut readback differs.',
            'Worker shortcut changed after capture.', 'Preserved shortcut record differs.']
        evidence = Path(os.environ.get('KMTECH_TEST_CA_EVIDENCE_ROOT') or tmp_path)
        (evidence / 'review-repro-failure.json').write_text(json.dumps({
            'rc': result.returncode, 'fixed_messages': [m for m in messages if m in result.stderr],
            'error_ids': re.findall(r'FullyQualifiedErrorId\s*:\s*([^\r\n]+)', result.stderr),
        }), encoding='utf-8')
    # Do not attach subprocess output: native errors can contain identifying paths.
    assert result.returncode == 0, 'Shortcut mismatch must be preserved without stopping installation.'
    assert result.stdout.strip().endswith('PASS'), 'Expected completed fixture verification.'


def test_recorded_legacy_link_drift_preserves_link_and_continues(tmp_path):
    _run_case(tmp_path, 'recorded_drift')


_REGRESSION = r"""
$fixture=$env:LM_REVIEW_ROOT;$mode=$env:LM_REVIEW_MODE;$scope=$env:LM_REVIEW_SCOPE
$script:fixtureRoot=$fixture
$root=Join-Path $fixture 'installed';$recordPath=Join-Path $fixture 'audit/shortcuts.json'
foreach($relative in @('installed/runtime','installed/app/assets','Desktop','Programs','CommonDesktop','CommonPrograms')){
 [void][IO.Directory]::CreateDirectory((Join-Path $fixture $relative))
}
foreach($relative in @('runtime/pythonw.exe','app/main.py','app/assets/logo.ico','launch-label-match.cmd')){
 [IO.File]::WriteAllText((Join-Path $root $relative),'inert fixture')
}
function Get-LabelWorkerShortcutFolders([switch]$CommonOnly,[switch]$UserOnly){
 foreach($name in @('Desktop','Programs','CommonDesktop','CommonPrograms')){
  if(($CommonOnly -and -not $name.StartsWith('Common')) -or ($UserOnly -and $name.StartsWith('Common'))){continue}
  [pscustomobject]@{name=$name;path=(Join-Path $script:fixtureRoot $name);recursive=$name.EndsWith('Programs')}
 }
}
$options=@{UserOnly=($scope -ceq 'UserOnly');CommonOnly=($scope -ceq 'CommonOnly')}
$prefix=if($options.CommonOnly){'Common'}else{''}
$first=Join-Path $fixture ($prefix+'Desktop/포장.lnk')
$second=Join-Path $fixture ($prefix+'Programs/KMTech/normal.lnk')
$legacy=[ordered]@{target_path=(Join-Path $root 'launch-label-match.cmd');arguments='';working_directory=$root;
 icon_location=((Join-Path $root 'app/assets/logo.ico')+',0');window_style=7;description='original';hotkey=''}
$record=[ordered]@{schema='label-match-worker-shortcuts-v1';install_root=$root;common_only=$options.CommonOnly;
 user_only=$options.UserOnly;status='INSTALLED';expected=$legacy;entries=@()}
foreach($file in @($first,$second)){
 $original=[ordered]@{};foreach($key in $legacy.Keys){$original[$key]=$legacy[$key]}
 $original.target_path=Join-Path $root 'old-missing.exe';$original.arguments='--original'
 Set-LabelShortcutProperties $file $original
 $backup=Join-Path ($recordPath+'.preimages') ($record.entries.Count.ToString()+'.lnk')
 [void][IO.Directory]::CreateDirectory([IO.Path]::GetDirectoryName($backup));[IO.File]::Copy($file,$backup)
 $entry=[ordered]@{path=$file;original=(Get-LabelShortcutProperties $file);original_sha256=(Get-LabelShortcutSha256 $file);
  backup_path=$backup;installed_sha256='';restored=$false}
 Set-LabelShortcutProperties $file $legacy
 $entry.installed_sha256=Get-LabelShortcutSha256 $file;$record.entries+=$entry
}
$normalPreimage=$record.entries[1].original_sha256
if($mode -ceq 'hash_only'){
 [IO.File]::AppendAllText($first,'retained non-property bytes')
 if(-not (Test-LabelShortcutProperties (Get-LabelShortcutProperties $first) $legacy)){throw 'HASH_ONLY_SETUP'}
}else{
 $changed=Get-LabelShortcutProperties $first;$changed.arguments='--ui tk'
 Set-LabelShortcutProperties $first $changed
 if($mode -ceq 'properties_only'){$record.entries[0].installed_sha256=Get-LabelShortcutSha256 $first}
}
$before=Get-LabelShortcutSha256 $first;$healthyBefore=Get-LabelShortcutSha256 $second
switch($mode){
 'record_schema' {$record.schema='other-schema'}
 'record_hash' {$record.entries[0].installed_sha256='not-a-sha256'}
 'record_root' {$record.install_root=Join-Path $fixture 'other-root'}
 'record_scope' {$record.common_only=-not $record.common_only}
 'record_expected' {$record.expected.arguments='--untrusted'}
 'record_path' {$record.entries[0].path=Join-Path $fixture 'outside.lnk';[IO.File]::Copy($first,$record.entries[0].path)}
 'preimage' {[IO.File]::AppendAllText($record.entries[0].backup_path,'tampered')}
 'record_preserved' {$record.preserved=@([ordered]@{location='desktop';shortcut_path=(Join-Path $fixture 'outside.lnk');reason_code='MODIFIED_SHORTCUT_PRESERVED'})}
}
Save-LabelShortcutRecord $recordPath $record
$recordBefore=Get-LabelShortcutSha256 $recordPath
$invalid=$mode -in @('record_schema','record_hash','record_root','record_scope','record_expected','record_path','preimage','record_preserved')
if($mode -ceq 'rollback_failure'){
 $script:realSetter=${function:Set-LabelShortcutProperties}
 function Set-LabelShortcutProperties($Path,$Properties){throw 'INJECTED_NORMAL_LINK_FAILURE'}
}
$failed=$false;try{$proof=Install-LabelWorkerShortcuts $root @{launcher='launch-label-match.cmd'} $recordPath @options}catch{$failed=$true}
if($invalid){
 if(-not $failed){throw 'INTEGRITY_REFUSAL_MISSING'}
 if((Get-LabelShortcutSha256 $recordPath) -cne $recordBefore -or
    (Get-LabelShortcutSha256 $first) -cne $before -or (Get-LabelShortcutSha256 $second) -cne $healthyBefore){throw 'REFUSAL_WROTE_STATE'}
 'PASS';return
}
if($mode -ceq 'rollback_failure'){
 if(-not $failed -or (Get-LabelShortcutSha256 $second) -cne $normalPreimage -or
    (Get-LabelShortcutSha256 $first) -cne $before){throw 'COMPENSATION_LOST_ORIGINAL'}
 'PASS';return
}
if($failed -or $proof.status -cne 'PASS'){throw 'INSTALL_BLOCKED'}
if((Get-LabelShortcutSha256 $first) -cne $before){throw 'DRIFT_BYTES_CHANGED'}
if((Get-LabelShortcutProperties $second).target_path -cne (Join-Path $root 'runtime/pythonw.exe')){throw 'NORMAL_NOT_MIGRATED'}
$journal=Get-Content -LiteralPath $recordPath -Raw -Encoding UTF8|ConvertFrom-Json
if(@($journal.entries).Count -ne 2 -or @($journal.published).Count -ne 1 -or
   @($journal.preserved).Count -ne 1 -or $journal.preserved[0].reason_code -cne 'MODIFIED_SHORTCUT_PRESERVED' -or
   $journal.preserved[0].shortcut_path -cne $first){throw 'PRESERVATION_RECORD_MISSING'}
foreach($entry in $journal.entries){if((Get-LabelShortcutSha256 $entry.backup_path) -cne $entry.original_sha256){throw 'PREIMAGE_CHANGED'}}
$journalHash=Get-LabelShortcutSha256 $recordPath
$secondProof=Install-LabelWorkerShortcuts $root @{launcher='launch-label-match.cmd'} $recordPath @options
if($secondProof.status -cne 'PASS' -or (Get-LabelShortcutSha256 $recordPath) -cne $journalHash -or
   (Get-LabelShortcutSha256 $first) -cne $before){throw 'REPEAT_CHANGED_VERDICT'}
Restore-LabelWorkerShortcuts $root $recordPath @options
if((Get-LabelShortcutSha256 $first) -cne $before -or
   (Get-LabelShortcutSha256 $second) -cne $normalPreimage){throw 'RESTORE_OVERWROTE_DRIFT'}
'PASS'
"""


@pytest.mark.parametrize('engine', ['powershell.exe', 'pwsh.exe'])
@pytest.mark.parametrize('scope', ['CommonOnly', 'UserOnly'])
@pytest.mark.parametrize('mode', ['properties_only', 'hash_only', 'rollback_failure'])
def test_recorded_legacy_drift_is_selective_and_repeatable(tmp_path, engine, scope, mode):
    result = run_functions(tmp_path, HELPER, NAMES, _REGRESSION, engine=engine, values={
        'LM_REVIEW_ROOT': str(tmp_path), 'LM_REVIEW_SCOPE': scope, 'LM_REVIEW_MODE': mode})
    assert result.returncode == 0, 'Selective migration/compensation must preserve changed bytes.'
    assert result.stdout.strip().endswith('PASS')


@pytest.mark.parametrize('engine', ['powershell.exe', 'pwsh.exe'])
@pytest.mark.parametrize('mode', ['record_schema', 'record_hash', 'record_root', 'record_scope', 'record_expected',
                                  'record_path', 'preimage', 'record_preserved'])
def test_drift_never_relaxes_record_or_preimage_integrity(tmp_path, engine, mode):
    result = run_functions(tmp_path, HELPER, NAMES, _REGRESSION, engine=engine, values={
        'LM_REVIEW_ROOT': str(tmp_path), 'LM_REVIEW_SCOPE': 'UserOnly', 'LM_REVIEW_MODE': mode})
    assert result.returncode == 0, 'Invalid record/preimage must still refuse before writing.'
    assert result.stdout.strip().endswith('PASS')

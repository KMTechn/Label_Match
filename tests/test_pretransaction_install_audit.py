"""G49 admission uses exact installer AST fragments and preserves old evidence."""
import json
import re
from pathlib import Path
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]

PS = r'''
param([string]$Installer,[string]$Repo,[string]$Root,[string]$Scenario,[string]$Result)
$ErrorActionPreference='Stop'
Set-StrictMode -Version Latest
. (Join-Path $Repo 'tools\failure_redaction.ps1')
$tokens=$null;$errors=$null
$text=[IO.File]::ReadAllText($Installer)
$ast=[Management.Automation.Language.Parser]::ParseInput($text,[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'Installer syntax differs'}
foreach($name in @('Test-PretransactionInstallAudit','Test-PristineInstallState','Read-FirstRegistrationAttempt','Archive-PretransactionFailure','Same','Save')){
 $node=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $name},$true))
 if($node.Count -ne 1){throw 'Required AST function absent'}
 . ([scriptblock]::Create($node[0].Extent.Text))
}
function Invoke-LabelPinnedAtomicReplace([string]$Source,[string]$Destination,[string]$Mode){
 if($Mode -ceq 'Replace'){[IO.File]::Copy($Source,$Destination,$true);[IO.File]::Delete($Source)}else{[IO.File]::Move($Source,$Destination)}
}
$lad=Join-Path $Root 'local'
$auditRoot=Join-Path $lad 'KMTech\Label_Match\install-audit'
$defaultDirectSyncRoot=Join-Path $lad 'KMTech\DirectSync\label_match';$selectedDirectSyncRoot=$defaultDirectSyncRoot
$writerFenceControlRoot=Join-Path $defaultDirectSyncRoot 'control\writer-session'
$defaultDataRoot=Join-Path $Root 'data';$selectedDataRoot=$defaultDataRoot
$defaultSettingsPath=Join-Path $Root 'settings\settings.json';$selectedSettingsPath=$defaultSettingsPath
$defaultProfilePath=Join-Path $Root 'profiles\profile.json';$selectedProfilePath=$defaultProfilePath
$bootstrapCaPath=Join-Path $Root 'ca\ca.pem'
$stop=Join-Path $defaultDirectSyncRoot 'control\stop.json'
$install=Join-Path $Root 'code\current'
$before=[pscustomobject]@{exists=$false};$old=@();$scheduledTasksAtPreflight=@()
$source=Join-Path $Root 'source'
$runId='new-run'
$oldId='old-run'
[void][IO.Directory]::CreateDirectory($auditRoot)
$oldPath=Join-Path $auditRoot ('canonical-portable-'+$oldId+'.json')
$record=[ordered]@{schema='label-match-canonical-portable-install-v1';run_id=$oldId;status='PREIMAGE_SAVED';code_placement='PENDING';current_user_setup_started=$false;current_user_registration_mode='FIRST_INSTALL'}
if($Scenario -cin @('failed','lifecycle','two-ids')){
 $record=[ordered]@{schema='label-match-canonical-portable-install-v1';run_id=$oldId;status='FAILED';preparation_status='FAILED_PREPARATION_ADMISSION';code_placement='NOT_STARTED';rollback=@{applied=$false}}
}
if($Scenario -cnotin @('empty','copy-only','copy-json-null','absent')){Save $oldPath $record}
if($Scenario -ceq 'absent'){[IO.Directory]::Delete($auditRoot)}
if($Scenario -cin @('preimage','copy-only')){[IO.File]::WriteAllText((Join-Path $auditRoot ('canonical-portable-'+$oldId+'-task-preimage.xml')),'<Task/>')}
if($Scenario -ceq 'two-ids'){Save (Join-Path $auditRoot 'canonical-portable-older-run.json') $record}
if($Scenario -ceq 'active'){[void][IO.Directory]::CreateDirectory($writerFenceControlRoot);Save (Join-Path $writerFenceControlRoot 'active.json') @{status='QUIESCING'}}
if($Scenario -ceq 'code-changed'){$record.code_placement='PLACED';Save $oldPath $record}
if($Scenario -ceq 'setup-started'){$record.current_user_setup_started=$true;Save $oldPath $record}
if($Scenario -ceq 'string-boolean'){$record.current_user_setup_started='false';Save $oldPath $record}
if($Scenario -ceq 'registration'){Save (Join-Path $auditRoot ('canonical-portable-'+$oldId+'-first-registration-attempt.json')) @{status='PREFLIGHT_RECORDED'}}
if($Scenario -ceq 'rollback'){Save (Join-Path $auditRoot 'rollback-audit.json') @{status='ROLLBACK_FAILED'}}
if($Scenario -ceq 'unknown'){[IO.File]::WriteAllText((Join-Path $auditRoot 'unknown.txt'),'unknown')}
if($Scenario -ceq 'other-residue'){[void][IO.Directory]::CreateDirectory($defaultDataRoot)}
if($Scenario -ceq 'broken'){[IO.File]::WriteAllText($oldPath,'{')}
if($Scenario -ceq 'copy-json-null'){[IO.File]::WriteAllText((Join-Path $auditRoot ('canonical-portable-'+$oldId+'-stop-preimage.json')),'null')}
if($Scenario -ceq 'aux-empty'){[IO.File]::WriteAllText((Join-Path $auditRoot ('canonical-portable-'+$oldId+'-stop-preimage.json')),'')}
if($Scenario -ceq 'aux-broken'){[IO.File]::WriteAllText((Join-Path $auditRoot ('canonical-portable-'+$oldId+'-stop-preimage.json')),'{')}
if($Scenario -ceq 'bad-schema'){$record.schema='other-schema';Save $oldPath $record}
if($Scenario -ceq 'orphan-temp'){[IO.File]::Delete($oldPath);Save ($oldPath+'.tmp.123') $record}
if($Scenario -ceq 'helper-reparse'){
 $target=Join-Path $Root 'target';[void][IO.Directory]::CreateDirectory($target)
 [void](New-Item -ItemType Junction -Path (Join-Path $auditRoot ('canonical-portable-'+$oldId+'-helper')) -Target $target)
}
[byte[]]$beforeBytes=@()
if([IO.File]::Exists($oldPath)){$beforeBytes=[IO.File]::ReadAllBytes($oldPath)}
$locked=$null
if($Scenario -ceq 'locked'){$locked=[IO.File]::Open($oldPath,[IO.FileMode]::Open,[IO.FileAccess]::ReadWrite,[IO.FileShare]::None)}
$assignment=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.AssignmentStatementAst] -and $n.Left.Extent.Text -ceq '$pristineResiduePaths'},$true))
if($assignment.Count -ne 1){throw 'Pristine residue AST differs'}
. ([scriptblock]::Create($assignment[0].Extent.Text))
$pristine=Test-PristineInstallState $install $before $old $scheduledTasksAtPreflight $pristineResiduePaths $bootstrapCaPath '' ''
if($null -ne $locked){$locked.Dispose()}
$ignored=@($Script:LabelIgnoredPretransactionRunIds)
$preserved=if([IO.File]::Exists($oldPath)){[Convert]::ToBase64String($beforeBytes) -ceq [Convert]::ToBase64String([IO.File]::ReadAllBytes($oldPath))}else{$beforeBytes.Count -eq 0}
$out=[ordered]@{pristine=[bool]$pristine;ignored=$ignored;preserved=$preserved}
if($Scenario -ceq 'lifecycle'){
 # Evaluate the real main audit initializer and its actual Save call.
 function Sha([string]$Path){return ('a'*64)}
 $existingVerified=$false;$legacyFirstRegistration=$false;$pristineInstall=$pristine
 $sourceManifest=[pscustomobject]@{source_commit=('b'*40)}
 $elevationLogPath=Join-Path $auditRoot ('canonical-portable-'+$runId+'-elevated.jsonl')
 $RunName='test-run';$serverBaseUrlSource='test';$serverBaseUrl='http://127.0.0.1:1';$wanted='test-command'
 $taskBefore=[pscustomobject]@{exists=$false;enabled=$false;xml_sha256='';backup_path=''}
 $stopBefore=[ordered]@{exists=$false;sha256='';backup_path=''}
 $writerTransition=[pscustomobject]@{compatibility='UNCHANGED';installed_inventory_sha256='';candidate_inventory_sha256=('c'*64)}
 $receiptSource=[pscustomobject]@{sha256=('d'*64)}
 function Get-AuthenticodeSignature([string]$Path){return [pscustomobject]@{Status='NotSigned'}}
 $init=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.AssignmentStatementAst] -and $n.Left.Extent.Text -ceq '$audit' -and $n.Extent.Text.Contains('runtime_pythonw_signature =')},$true))
 if($init.Count -ne 1){throw 'Main audit AST differs'}
 . ([scriptblock]::Create($init[0].Extent.Text))
 $auditPath=Join-Path $auditRoot ('canonical-portable-'+$runId+'.json')
 $save=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.CommandAst] -and $n.Extent.Text -ceq 'Save $auditPath $audit'},$true))[0].Extent.Text
 . ([scriptblock]::Create($save))
 $success=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.AssignmentStatementAst] -and $n.Left.Extent.Text -ceq '$audit.status' -and $n.Right.Extent.Text -ceq ("'PASS'") -and $n.Extent.StartOffset -gt $init[0].Extent.EndOffset},$true))[0]
 . ([scriptblock]::Create($success.Extent.Text));. ([scriptblock]::Create($save))
 $saved=Get-Content -LiteralPath $auditPath -Raw -Encoding UTF8 | ConvertFrom-Json
 $out.recorded_id=[string]$saved.run_id;$out.schema=[string]$saved.schema;$out.recorded_ignored=@($saved.ignored_pretransaction_run_ids)
 # A completed first install has no first-registration rollback attempt.
 $resume=Read-FirstRegistrationAttempt $auditRoot $install ('b'*40) ('a'*64) @()
 $out.resume_absent=$null -eq $resume
 $archive=Archive-PretransactionFailure $auditRoot $runId
 $out.archived_names=@(Get-ChildItem -LiteralPath $archive -Force | ForEach-Object {$_.Name})
 $out.old_still_present=[IO.File]::Exists($oldPath)
 $out.old_still_equal=[Convert]::ToBase64String($beforeBytes) -ceq [Convert]::ToBase64String([IO.File]::ReadAllBytes($oldPath))
}
[IO.File]::WriteAllText($Result,($out|ConvertTo-Json -Depth 8),(New-Object Text.UTF8Encoding($false)))
'''


def _invoke(tmp_path, scenario):
    script = tmp_path / 'admission.ps1'
    script.write_text(PS, encoding='utf-8-sig')
    result = tmp_path / 'result.json'
    installer = ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1'
    completed = subprocess.run(
        ['powershell.exe', '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass',
         '-File', str(script), '-Installer', str(installer), '-Repo', str(ROOT), '-Root', str(tmp_path / 'case'),
         '-Scenario', scenario, '-Result', str(result)],
        capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW,
    )
    if completed.returncode:
        error = completed.stderr.decode(errors='replace')
        (tmp_path / 'ps-error.json').write_text(json.dumps({'variables': re.findall(r'\$([A-Za-z_][A-Za-z_0-9]*)',error), 'ids': re.findall(r'FullyQualifiedErrorId\s*:\s*([A-Za-z0-9_,.]+)',error), 'locations': re.findall(r'[A-Za-z0-9_./\\:-]+\.ps1:[0-9]+',error)}))
    assert completed.returncode == 0, f'fragment execution rc={completed.returncode}'
    return json.loads(result.read_text(encoding='utf-8-sig'))


@pytest.mark.parametrize('scenario', ['absent', 'empty', 'failed', 'copy-only', 'copy-json-null', 'preimage', 'two-ids'])
def test_only_pretransaction_evidence_admits_new_pc(tmp_path, scenario):
    result = _invoke(tmp_path, scenario)
    assert result['pristine'] is True
    assert result['preserved'] is True
    assert all(isinstance(value, str) and value.replace('-', '').isalnum() for value in result['ignored'])
    assert set(result['ignored']) == (set() if scenario in ('absent', 'empty') else
                                         {'old-run', 'older-run'} if scenario == 'two-ids' else {'old-run'})


@pytest.mark.parametrize('scenario', [
    'active', 'code-changed', 'setup-started', 'string-boolean', 'registration',
    'rollback', 'unknown', 'other-residue', 'broken', 'aux-broken', 'aux-empty', 'bad-schema',
    'orphan-temp', 'helper-reparse', 'locked',
])
def test_existing_or_unprovable_state_remains_residue(tmp_path, scenario):
    result = _invoke(tmp_path, scenario)
    assert result['pristine'] is False
    assert result['preserved'] is True


def test_success_record_resume_and_cleanup_use_new_run_only(tmp_path):
    result = _invoke(tmp_path, 'lifecycle')
    assert result['pristine'] is True
    assert result['schema'] == 'label-match-canonical-portable-install-v1'
    assert result['recorded_id'] == 'new-run'
    assert result['recorded_ignored'] == ['old-run']
    assert result['resume_absent'] is True
    assert result['archived_names'] == ['canonical-portable-new-run.json']
    assert result['old_still_present'] is True and result['old_still_equal'] is True

"""Task ownership consumes the actual installed task XML, without the scheduler."""
import json
import shutil
import subprocess
from pathlib import Path
import pytest
import auto_update as update
from tests.test_supported_removal_convergence import REPO, tmp_path

TASK_SCRIPT = r'''
param([string]$Source,[string]$Root,[string]$Spec,[string]$Mutation)
$ErrorActionPreference='Stop'
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile($Source,[ref]$tokens,[ref]$errors)
if ($errors.Count) { throw 'parse failed' }
$sidFn=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'ConvertTo-LabelTaskSid'},$true))
if($sidFn.Count -eq 1){. ([scriptblock]::Create($sidFn[0].Extent.Text))}
$fn=$ast.FindAll({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Get-LabelSupportedStartupTasks'},$true)
if ($fn.Count -ne 1) { throw 'missing function' }
. ([scriptblock]::Create($fn[0].Extent.Text))
$specObject=Get-Content -LiteralPath $Spec -Raw | ConvertFrom-Json
[xml]$xml=$specObject.xml
$task=[pscustomobject]@{TaskName=$specObject.name;TaskPath='\';
 # The scheduler CIM principal reports registration mode 5 as ServiceAccount;
 # the unsupported optional XML element is absent from the emitted definition.
 Principal=[pscustomobject]@{UserId=[string]$xml.Task.Principals.Principal.UserId;LogonType='ServiceAccount';RunLevel=$(if ([string]$xml.Task.Principals.Principal.RunLevel -ceq 'HighestAvailable') {'Highest'} else {'Limited'})};
 Actions=@([pscustomobject]@{Execute=[string]$xml.Task.Actions.Exec.Command;Arguments=[string]$xml.Task.Actions.Exec.Arguments;WorkingDirectory=[string]$xml.Task.Actions.Exec.WorkingDirectory});
 Triggers=@([pscustomobject]@{UserId=[string]$xml.Task.Triggers.LogonTrigger.UserId})}
switch ($Mutation) {
 'name' {$task.TaskName='KMTech.LabelMatch.StartupUpdate.'+('0'*24)}
 'sid' {$task.Triggers[0].UserId='S-1-5-21-11-22-33-1002'}
 'arguments' {$task.Actions[0].Arguments+=' -Mode Apply'}
 'executable' {$task.Actions[0].Execute='unowned.exe'}
 'directory' {$task.Actions[0].WorkingDirectory=$Root}
 'principal' {$task.Principal.UserId='S-1-5-21-11-22-33-1001'}
 'runlevel' {$task.Principal.RunLevel='Limited'}
 'path' {$task.TaskPath='\Foreign\'}
 'actions' {$task.Actions+= $task.Actions[0]}
 'triggers' {$task.Triggers+= $task.Triggers[0]}
}
function Get-ScheduledTask { return $task }
$result=@(Get-LabelSupportedStartupTasks $Root)
if($result.Count -ne 1) { throw 'ownership missing' }
'PASS'
'''


@pytest.mark.parametrize('source', ['INSTALL_THIS_PC.ps1', 'tools/apply_prepared_update.ps1'])
@pytest.mark.parametrize('mutation', ['', 'name', 'sid', 'arguments', 'executable', 'directory', 'principal', 'runlevel', 'path', 'actions', 'triggers'])
def test_actual_task_ownership_matches_installed_xml_and_refuses_tampering(tmp_path, source, mutation):
    root=tmp_path/'machine/current'
    spec=update.install_task_spec(root, 'S-1-5-21-11-22-33-1001')
    spec_path=tmp_path/'task.json';spec_path.write_text(json.dumps(spec), encoding='utf-8')
    script=tmp_path/'ownership.ps1';script.write_text(TASK_SCRIPT, encoding='ascii')
    completed=subprocess.run([shutil.which('powershell.exe'), '-NoProfile', '-NonInteractive', '-File', str(script), str(REPO/source), str(root), str(spec_path), mutation], capture_output=True, text=True,
        creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    if mutation:
        assert completed.returncode != 0
        assert 'SUPPORTED_TASK_' in completed.stderr
    else:
        assert completed.returncode == 0, completed.stderr
        assert completed.stdout.strip() == 'PASS'

CALLER_SCRIPT = r'''
param([string]$Source,[string]$Root,[string]$Delegated,[string]$Fail)
$ErrorActionPreference='Stop'
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile($Source,[ref]$tokens,[ref]$errors)
$caller=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.IfStatementAst] -and $n.Extent.Text.StartsWith('if ($Uninstall.IsPresent) {')},$true))
if ($caller.Count -ne 1) {throw 'missing actual caller'}
$Uninstall=[switch]$true;$DryRun=[switch]$false;$testOverride=$false
$WriterFenceFunctionsPreloaded=[switch]($Delegated -eq 'True')
$WriterFenceControlRoot='fixture';$WriterFenceSessionId='fixture';$WriterFenceAttemptId='fixture'
$WriterFenceReplacementTransactionId='fixture';$WriterFenceDelegationToken='synthetic-local-only'
$installRootFull=Join-Path $Root 'current';$script:entered=$false;$script:released=$false;$script:converged=$false
function Assert-ProtectedCodeAncestors { }
function Enter-LabelWriterDelegatedOperation {
 param($ControlRoot,$SessionId,$AttemptId,$ReplacementTransactionId,$DelegationToken,$Source)
 if($Source -cne 'canonical_placement') { throw 'wrong authority' }
 $script:entered=$true;return [pscustomobject]@{fixture=$true}
}
function Exit-LabelWriterAdmission { $script:released=$true }
$script:fixtureFolder=[pscustomobject]@{fixture=$true}
$scheduler=[pscustomobject]@{}
$scheduler | Add-Member ScriptMethod Connect { }
$scheduler | Add-Member ScriptMethod GetFolder {param($path) if($path -cne '\'){throw 'wrong folder'};return $script:fixtureFolder}
function New-Object {param($ComObject) if($ComObject -cne 'Schedule.Service'){throw 'unexpected object'};return $scheduler}
function Invoke-LabelConvergentRemoval {
 param($InstallRoot,$TaskFolder)
 if($InstallRoot -cne $installRootFull -or $TaskFolder.fixture -ne $true){throw 'wrong removal boundary'}
 $script:converged=$true
 if($Fail -eq 'True'){throw 'injected removal interruption'}
 return @{evidence_root='fixture';retained_paths=@('unique-data')}
}
$text=$caller[0].Extent.Text -replace '(?m)^    exit 0\s*$','    return'
$caught=$false
try { & ([scriptblock]::Create($text)) | Out-Null } catch {if($_.Exception.Message -cne 'injected removal interruption'){throw};$caught=$true}
@{entered=$script:entered;released=$script:released;converged=$script:converged;caught=$caught} | ConvertTo-Json -Compress
'''


@pytest.mark.parametrize('delegated', [False, True])
@pytest.mark.parametrize('failure', [False, True])
def test_actual_supported_removal_caller_preserves_optional_exact_delegation(tmp_path, delegated, failure):
    script=tmp_path/'caller.ps1';script.write_text(CALLER_SCRIPT, encoding='ascii')
    completed=subprocess.run([shutil.which('powershell.exe'), '-NoProfile', '-NonInteractive', '-File', str(script), str(REPO/'INSTALL_THIS_PC.ps1'), str(tmp_path), str(delegated), str(failure)], capture_output=True, text=True,
        creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    assert completed.returncode == 0, completed.stderr
    result=json.loads(completed.stdout)
    assert result == dict(entered=delegated, released=delegated, converged=True, caught=failure)

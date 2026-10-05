"""Real identity translation and task-definition comparison; scheduler is never used."""
from pathlib import Path

import pytest

from tests.test_shared_ancestor_protection import run_functions

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('scenario', ('sid', 'account', 'principal_account', 'other_sid',
    'unknown_account', 'malformed_sid', 'duplicate_user', 'nested_user', 'extra_trigger',
    'extra_attribute', 'changed_action'))
def test_startup_definition_binds_sid_and_preserves_raw_xml(tmp_path, engine, scenario):
    result = run_functions(tmp_path, ROOT / 'INSTALL_THIS_PC.ps1',
        ['Assert-LabelStartupUpdateTaskDefinition'], r'''
$identity=[Security.Principal.WindowsIdentity]::GetCurrent()
$sid=$identity.User.Value
[xml]$expected='<Task xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task"><Triggers><LogonTrigger><Enabled>true</Enabled><UserId>'+ $sid +'</UserId></LogonTrigger></Triggers><Principals><Principal id="System"><UserId>S-1-5-18</UserId><RunLevel>HighestAvailable</RunLevel></Principal></Principals><Actions Context="System"><Exec><Command>fixture.exe</Command><Arguments>fixed</Arguments></Exec></Actions><Settings><MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy><Hidden>true</Hidden><ExecutionTimeLimit>PT2M</ExecutionTimeLimit><AllowHardTerminate>true</AllowHardTerminate><StartWhenAvailable>true</StartWhenAvailable><DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries><StopIfGoingOnBatteries>false</StopIfGoingOnBatteries></Settings></Task>'
[xml]$actual=$expected.OuterXml
switch($env:LM_SID_SCENARIO){
 'account' {$actual.Task.Triggers.LogonTrigger.UserId=$identity.Name}
 'principal_account' {$actual.Task.Principals.Principal.UserId=[Security.Principal.SecurityIdentifier]::new('S-1-5-18').Translate([Security.Principal.NTAccount]).Value}
 'other_sid' {$actual.Task.Triggers.LogonTrigger.UserId='S-1-5-21-11-22-33-1002'}
 'unknown_account' {$actual.Task.Triggers.LogonTrigger.UserId='KMTech-no-such-account-158'}
 'malformed_sid' {$actual.Task.Triggers.LogonTrigger.UserId='S-1-malformed'}
 'duplicate_user' {[void]$actual.Task.Triggers.LogonTrigger.AppendChild($actual.Task.Triggers.LogonTrigger.SelectSingleNode('*[local-name()="UserId"]').CloneNode($true))}
 'nested_user' {$actual.Task.Triggers.LogonTrigger.SelectSingleNode('*[local-name()="UserId"]').InnerXml='<nested xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">'+$sid+'</nested>'}
 'extra_trigger' {[void]$actual.Task.Triggers.LogonTrigger.AppendChild($actual.CreateElement('Unknown',$actual.DocumentElement.NamespaceURI))}
 'extra_attribute' {$actual.Task.Triggers.LogonTrigger.SetAttribute('unexpected','true')}
 'changed_action' {$actual.Task.Actions.Exec.Arguments='foreign'}
}
$rawActual=$actual.OuterXml;$rawExpected=$expected.OuterXml
$failed=$false
try{Assert-LabelStartupUpdateTaskDefinition $actual $expected}catch{$failed=$true}
$valid=$env:LM_SID_SCENARIO -in @('sid','account','principal_account')
if($failed -eq $valid){throw 'Task identity/structure verdict differs'}
if($actual.OuterXml -cne $rawActual -or $expected.OuterXml -cne $rawExpected){throw 'Raw preimage changed'}
'PASS'
''', engine=engine, values={'LM_SID_SCENARIO': scenario})
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('source', ('INSTALL_THIS_PC.ps1', 'tools/apply_prepared_update.ps1'))
@pytest.mark.parametrize('scenario', ('sid', 'account', 'other_sid', 'unresolved'))
def test_supported_task_census_resolves_trigger_and_principal(tmp_path, engine, source, scenario):
    result = run_functions(tmp_path, ROOT / source, ['Get-LabelSupportedStartupTasks'], r'''
$identity=[Security.Principal.WindowsIdentity]::GetCurrent();$sid=$identity.User.Value
$root=Join-Path $env:TEMP 'current';$sha=[Security.Cryptography.SHA256]::Create()
try{$digest=([BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes($root.ToLowerInvariant()+[char]10+$sid)))).Replace('-','').ToLowerInvariant()}finally{$sha.Dispose()}
$task=[pscustomobject]@{TaskName='KMTech.LabelMatch.StartupUpdate.'+$digest.Substring(0,24);TaskPath='\';
 Principal=[pscustomobject]@{UserId='S-1-5-18';LogonType='ServiceAccount';RunLevel='Highest'};
 Actions=@([pscustomobject]@{Execute=Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe';Arguments='-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -File "'+(Join-Path $root 'tools\apply_prepared_update.ps1')+'" -InstallRoot "'+$root+'"';WorkingDirectory=''});
 Triggers=@([pscustomobject]@{UserId=$sid})}
switch($env:LM_SID_SCENARIO){
 'account' {$task.Triggers[0].UserId=$identity.Name;$task.Principal.UserId=[Security.Principal.SecurityIdentifier]::new('S-1-5-18').Translate([Security.Principal.NTAccount]).Value}
 'other_sid' {$task.Triggers[0].UserId='S-1-5-21-11-22-33-1002'}
 'unresolved' {$task.Principal.UserId='KMTech-no-such-account-158'}
}
function Get-ScheduledTask {param($ErrorAction) return $task}
$failed=$false
try{$result=@(Get-LabelSupportedStartupTasks $root);if($result.Count -ne 1){throw 'Owned task missing'}}catch{$failed=$true}
if($failed -eq ($env:LM_SID_SCENARIO -in @('sid','account'))){throw 'Supported task verdict differs'}
'PASS'
''', engine=engine, values={'LM_SID_SCENARIO': scenario})
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
def test_task_sid_boundary_copies_have_exact_ast_body(tmp_path, engine):
    result = run_functions(tmp_path, ROOT / 'INSTALL_THIS_PC.ps1', ['Assert-LabelStartupUpdateTaskDefinition'], r'''
$texts=@()
foreach($file in @('INSTALL_THIS_PC.ps1','INSTALL_CANONICAL_PORTABLE.ps1','tools/apply_prepared_update.ps1','current_user_scheduled_task.py')){
 $text=[IO.File]::ReadAllText((Join-Path $env:LM_SID_REPO $file))
 if($file.EndsWith('.py')){
  $start=$text.IndexOf('function ConvertTo-LabelTaskSid');$end=$text.IndexOf('function Test-ExactTask',$start)
  if($start -lt 0 -or $end -lt 0){throw 'Task SID copy missing'}
  $text=$text.Substring($start,$end-$start)
 }
 $tree=[Management.Automation.Language.Parser]::ParseInput($text,[ref]$tokens,[ref]$errors)
 if($errors.Count){throw 'Boundary copy parse failed'}
 $definitions=@($tree.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'ConvertTo-LabelTaskSid'},$true))
 if($definitions.Count -ne 1){throw 'Task SID copy count differs'}
 $texts+=,$definitions[0].Body.Extent.Text
}
foreach($body in $texts){if($body -cne $texts[0]){throw 'Task SID boundary copy drift'}}
'PASS'
''', engine=engine, values={'LM_SID_REPO': str(ROOT)})
    assert result.returncode == 0, result.stdout + result.stderr

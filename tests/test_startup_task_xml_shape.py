"""Compare generated/native Task Scheduler XML with the real PS comparer."""
from pathlib import Path

import pytest

from auto_update import logon_task_xml
from tests.test_shared_ancestor_protection import run_functions

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('case', ('exact', 'native_defaults', 'account_identity', 'previous_policy', 'wrong_policy', 'extra_argument',
                                  'disabled_trigger', 'disabled_demand', 'command',
                                  'extra_action', 'extra_trigger', 'extra_principal',
                                  'duplicate_argument', 'optional_added', 'missing_runlevel'))
def test_actual_startup_task_comparer_keeps_strict_ownership(tmp_path, engine, case):
    expected = tmp_path / 'expected.xml'
    expected.write_text(logon_task_xml(app_root=Path(r'C:\KMTech\Apps\Label_Match\current'),
                                      operator_sid='S-1-5-21-1-2-3-1000'), encoding='utf-8')
    result = run_functions(tmp_path, ROOT / 'INSTALL_THIS_PC.ps1',
                           ['Assert-LabelStartupUpdateTaskDefinition'], r'''
Set-StrictMode -Version Latest
[xml]$expected=[IO.File]::ReadAllText($env:LM_XML_EXPECTED)
[xml]$actual=$expected.CloneNode($true)
$ns=[Xml.XmlNamespaceManager]::new($actual.NameTable)
$ns.AddNamespace('t','http://schemas.microsoft.com/windows/2004/02/mit/task')
function Node([string]$Path){return $actual.SelectSingleNode('/t:Task/'+$Path,$ns)}
switch($env:LM_XML_CASE){
 'previous_policy' {$n=Node 't:Actions/t:Exec/t:Arguments';$n.set_InnerText($n.get_InnerText().Replace(' -ExecutionPolicy Bypass -File ',' -File '))}
 'wrong_policy' {$n=Node 't:Actions/t:Exec/t:Arguments';$n.set_InnerText($n.get_InnerText().Replace('Bypass','Unrestricted'))}
 'extra_argument' {$n=Node 't:Actions/t:Exec/t:Arguments';$n.set_InnerText($n.get_InnerText()+' -Unexpected')}
 'native_defaults' {
  foreach($path in @('Triggers/t:LogonTrigger/t:Enabled','Settings/t:AllowStartOnDemand')){
   $n=Node ('t:'+$path);[void]$n.ParentNode.RemoveChild($n)
  }
 }
 'account_identity' {(Node 't:Principals/t:Principal/t:UserId').set_InnerText('SYSTEM')}
 'disabled_trigger' {(Node 't:Triggers/t:LogonTrigger/t:Enabled').set_InnerText('false')}
 'disabled_demand' {(Node 't:Settings/t:AllowStartOnDemand').set_InnerText('false')}
 'command' {(Node 't:Actions/t:Exec/t:Command').set_InnerText('different.exe')}
 'extra_action' {$n=Node 't:Actions/t:Exec';[void]$n.ParentNode.AppendChild($n.CloneNode($true))}
 'extra_trigger' {$n=Node 't:Triggers/t:LogonTrigger';[void]$n.ParentNode.AppendChild($n.CloneNode($true))}
 'extra_principal' {$n=Node 't:Principals/t:Principal';[void]$n.ParentNode.AppendChild($n.CloneNode($true))}
 'duplicate_argument' {$n=Node 't:Actions/t:Exec/t:Arguments';[void]$n.ParentNode.AppendChild($n.CloneNode($true))}
 'optional_added' {$n=$actual.CreateElement('WorkingDirectory',$ns.LookupNamespace('t'));$n.set_InnerText('C:\other');[void](Node 't:Actions/t:Exec').AppendChild($n)}
 'missing_runlevel' {$n=Node 't:Principals/t:Principal/t:RunLevel';[void]$n.ParentNode.RemoveChild($n)}
}
$beforeActual=$actual.OuterXml;$beforeExpected=$expected.OuterXml
$accepted=$false
try{Assert-LabelStartupUpdateTaskDefinition $actual $expected;$accepted=$true}
catch{if($_.Exception.Message -cne 'STARTUP_UPDATE_OWNED_TASK_DIFFERS'){throw}}
$shouldAccept=$env:LM_XML_CASE -cin @('exact','native_defaults','account_identity','previous_policy')
if($accepted -ne $shouldAccept){throw 'Task ownership comparison result differs.'}
if($actual.OuterXml -cne $beforeActual -or $expected.OuterXml -cne $beforeExpected){throw 'Original XML preimage changed.'}
'PASS'
''', engine=engine, values={'LM_XML_EXPECTED': str(expected), 'LM_XML_CASE': case})
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('relative', ('INSTALL_THIS_PC.ps1', 'tools/apply_prepared_update.ps1'))
@pytest.mark.parametrize('policy', ('previous', 'bypass', 'wrong', 'extra'))
def test_supported_task_observers_preserve_exact_previous_action(tmp_path, engine, relative, policy):
    result = run_functions(tmp_path, ROOT / relative, ['Get-LabelSupportedStartupTasks'], r'''
$root='E:\synthetic\current';$sid='S-1-5-21-1-2-3-1001'
$arguments='-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File "'+$root+'\tools\apply_prepared_update.ps1" -InstallRoot "'+$root+'"'
switch($env:LM_POLICY){
 'previous' {$arguments=$arguments.Replace(' -ExecutionPolicy Bypass -File ',' -File ')}
 'wrong' {$arguments=$arguments.Replace('Bypass','Unrestricted')}
 'extra' {$arguments+=' -Unexpected'}
}
$sha=[Security.Cryptography.SHA256]::Create()
try{$digest=([BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes($root.ToLowerInvariant()+[char]10+$sid)))).Replace('-','').ToLowerInvariant()}finally{$sha.Dispose()}
$task=[pscustomobject]@{TaskName=('KMTech.LabelMatch.StartupUpdate.'+$digest.Substring(0,24));TaskPath='\';
 Actions=@([pscustomobject]@{Execute=(Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe');Arguments=$arguments;WorkingDirectory=''});
 Triggers=@([pscustomobject]@{UserId=$sid});Principal=[pscustomobject]@{UserId='S-1-5-18';LogonType='ServiceAccount';RunLevel='Highest'}}
function Get-ScheduledTask {param($ErrorAction) return $task}
$accepted=$false
try{$accepted=@(Get-LabelSupportedStartupTasks $root).Count -eq 1}catch{if($_.Exception.Message -cne 'SUPPORTED_TASK_ACTION_DIFFERS'){throw}}
if($accepted -ne ($env:LM_POLICY -cin @('previous','bypass'))){throw 'Task ownership differs'}
if($task.Actions[0].Arguments -cne $arguments){throw 'Original task action changed'}
'PASS'
''', engine=engine, values={'LM_POLICY': policy})
    assert result.returncode == 0, result.stdout + result.stderr

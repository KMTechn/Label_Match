"""Compare generated/native Task Scheduler XML with the real PS comparer."""
from pathlib import Path

import pytest

from auto_update import logon_task_xml
from tests.test_shared_ancestor_protection import run_functions

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('case', ('exact', 'native_defaults', 'account_identity',
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
$shouldAccept=$env:LM_XML_CASE -cin @('exact','native_defaults','account_identity')
if($accepted -ne $shouldAccept){throw 'Task ownership comparison result differs.'}
if($actual.OuterXml -cne $beforeActual -or $expected.OuterXml -cne $beforeExpected){throw 'Original XML preimage changed.'}
'PASS'
''', engine=engine, values={'LM_XML_EXPECTED': str(expected), 'LM_XML_CASE': case})
    assert result.returncode == 0, result.stdout + result.stderr

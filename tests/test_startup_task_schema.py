"""Actual read-only functions and generated XML; native host targets are mocked."""
from pathlib import Path

import pytest
import auto_update
from tests.test_shared_ancestor_protection import run_functions

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('mutation', ('none', 'reordered', 'service_account', 'duplicate_user', 'missing_user', 'missing_action', 'trigger_order'))
def test_generated_system_task_schema_and_required_behavior(tmp_path, engine, mutation):
    xml = tmp_path/'generated-task.xml'
    xml.write_text(auto_update.logon_task_xml(app_root=tmp_path/'current', operator_sid='S-1-5-21-1-2-3-1001'), encoding='utf-8')
    result = run_functions(tmp_path, ROOT/'INSTALL_THIS_PC.ps1', ['Assert-LabelStartupUpdateTaskDefinition'], r'''
$task=[xml](Get-Content -LiteralPath $env:LM_TASK_XML -Raw -Encoding UTF8)
$ns=[Xml.XmlNamespaceManager]::new($task.NameTable);$ns.AddNamespace('t','http://schemas.microsoft.com/windows/2004/02/mit/task')
$principal=$task.SelectSingleNode('/t:Task/t:Principals/t:Principal',$ns)
switch($env:LM_TASK_MUTATION){
 'reordered' {[void]$principal.PrependChild($principal.SelectSingleNode('t:RunLevel',$ns).CloneNode($true));[void]$principal.RemoveChild($principal.SelectNodes('t:RunLevel',$ns)[1])}
 'service_account' {$logon=$task.CreateElement('LogonType',$ns.LookupNamespace('t'));$logon.InnerText='ServiceAccount';[void]$principal.AppendChild($logon)}
 'duplicate_user' {[void]$principal.AppendChild($principal.SelectSingleNode('t:UserId',$ns).CloneNode($true))}
 'missing_user' {[void]$principal.RemoveChild($principal.SelectSingleNode('t:UserId',$ns))}
 'missing_action' {[void]$task.Task.Actions.RemoveAll()}
 'trigger_order' {$trigger=$task.SelectSingleNode('/t:Task/t:Triggers/t:LogonTrigger',$ns);[void]$trigger.AppendChild($trigger.SelectSingleNode('t:Enabled',$ns))}
}
$valid=$true
try{
 $settings=[Xml.XmlReaderSettings]::new();$settings.ValidationType=[Xml.ValidationType]::Schema
 [void]$settings.Schemas.Add($ns.LookupNamespace('t'),$env:LM_TASK_SCHEMA)
 $reader=[Xml.XmlReader]::Create([IO.StringReader]::new($principal.OuterXml),$settings)
 try{while($reader.Read()){}}finally{$reader.Dispose()}
 # Required application behavior, beyond the optional native schema fields.
 if([string]$principal.UserId -cne 'S-1-5-18' -or [string]$principal.RunLevel -cne 'HighestAvailable' -or [string]$principal.id -cne 'System'){throw 'SYSTEM principal changed'}
 if($task.SelectNodes('/t:Task/t:Actions/t:Exec',$ns).Count -ne 1 -or [string]$task.Task.Actions.Context -cne 'System'){throw 'Fixed action missing'}
 $trigger=$task.SelectSingleNode('/t:Task/t:Triggers/t:LogonTrigger',$ns)
 if((($trigger.ChildNodes | ForEach-Object {$_.LocalName}) -join ',') -cne 'Enabled,UserId'){throw 'Logon base/extension sequence changed'}
 if([string]$trigger.Enabled -cne 'true' -or [string]$trigger.UserId -cne 'S-1-5-21-1-2-3-1001'){throw 'Operator trigger changed'}
 if([string]$task.Task.Settings.AllowHardTerminate -cne 'false' -or [string]$task.Task.Settings.Hidden -cne 'true' -or [string]$task.Task.Settings.MultipleInstancesPolicy -cne 'IgnoreNew'){throw 'Task behavior changed'}
}catch{$valid=$false}
$expected=$env:LM_TASK_MUTATION -in @('none','reordered')
if($valid -ne $expected){throw ('Generated XML schema/behavior mismatch: '+$env:LM_TASK_MUTATION)}
'PASS'
''', engine=engine, values={'LM_TASK_XML': str(xml), 'LM_TASK_SCHEMA': str(ROOT/'tests/fixtures/task-principal-emitted-schema.xsd'), 'LM_TASK_MUTATION': mutation})
    assert result.returncode == 0, result.stdout + result.stderr

"""Actual read-only functions and generated XML; native host targets are mocked."""
from pathlib import Path

import pytest
from tests.test_shared_ancestor_protection import run_functions

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('scenario', ('absent', 'directory', 'denied', 'file'))
def test_optional_update_settings_only_skip_an_absent_path(tmp_path, engine, scenario):
    result = run_functions(tmp_path, ROOT/'INSTALL_THIS_PC.ps1', ['Initialize-LabelStartupUpdate'], r'''
function Test-Path {
 [CmdletBinding()]param($LiteralPath,$PathType)
 if($env:LM_FRESH_SCENARIO -ceq 'denied'){throw [UnauthorizedAccessException]::new('denied')}
 if($env:LM_FRESH_SCENARIO -ceq 'absent'){return $false}
 if($env:LM_FRESH_SCENARIO -ceq 'directory' -and $PathType -ceq 'Leaf'){return $false}
 return $true
}
$script:trustReached=$false
function Assert-LabelStartupUpdateTrust {param($Root) $script:trustReached=$true;throw 'Trust boundary reached'}
$testOverride=$false;$failed=$false
try{Initialize-LabelStartupUpdate $env:TEMP}catch{$failed=$true}
if($env:LM_FRESH_SCENARIO -ceq 'absent'){
 if($failed -or $script:trustReached){throw 'Missing optional file did not skip read-only'}
}elseif($env:LM_FRESH_SCENARIO -ceq 'file'){
 if(-not $script:trustReached){throw 'Existing file did not reach normal trust'}
}elseif(-not $failed -or $script:trustReached){throw 'Invalid/inaccessible settings treated as absent'}
'PASS'
''', engine=engine, values={'LM_FRESH_SCENARIO': scenario})
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('function', ('ScheduledTaskSnapshot', 'RestoreScheduledTask'))
@pytest.mark.parametrize('scenario', ('absent', 'foreign', 'denied', 'other_error'))
def test_canonical_task_observers_only_accept_proven_absence(tmp_path, engine, function, scenario):
    result = run_functions(tmp_path, ROOT/'INSTALL_CANONICAL_PORTABLE.ps1', [function], r'''
$CanonicalTaskName='owned'
$script:mutations=0
function Get-ScheduledTask {
 [CmdletBinding()]param([string]$TaskName,[string]$TaskPath)
 if($env:LM_FRESH_SCENARIO -in @('denied','other_error')){
  $exception=if($env:LM_FRESH_SCENARIO -ceq 'denied'){[UnauthorizedAccessException]::new('query denied')}else{[IO.IOException]::new('scheduler failed')}
  Write-Error -Exception $exception -ErrorId 'SchedulerQueryFailed';return
 }
 # A named query on a fresh PC can throw FileNotFoundException. Successful
 # enumeration has an empty result instead, and preserves all other failures.
 if($TaskName){Write-Error -Exception ([IO.FileNotFoundException]::new('absent')) -ErrorId 'TaskNotFound';return}
 if($env:LM_FRESH_SCENARIO -ceq 'foreign'){
  return @([pscustomobject]@{TaskName='owned';TaskPath='\foreign\'},[pscustomobject]@{TaskName='different';TaskPath='\'})
 }
 return @()
}
function Unregister-ScheduledTask {$script:mutations++}
function Register-ScheduledTask {$script:mutations++;throw 'Unexpected registration'}
function Export-ScheduledTask {throw 'Unexpected task export'}
$failed=$false
try{
 if($env:LM_FRESH_FUNCTION -ceq 'ScheduledTaskSnapshot'){
  $actual=ScheduledTaskSnapshot $env:TEMP 'fresh';if($actual.exists){throw 'Foreign/absent task adopted'}
 }else{RestoreScheduledTask ([pscustomobject]@{exists=$false})}
}catch{$failed=$true}
if($failed -ne ($env:LM_FRESH_SCENARIO -in @('denied','other_error'))){throw 'Failed query was treated as absence, or proven absence was refused'}
if($script:mutations){throw 'Observation mutated a task'}
'PASS'
''', engine=engine, values={'LM_FRESH_FUNCTION': function, 'LM_FRESH_SCENARIO': scenario})
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('scenario', ('com_not_found', 'file_not_found', 'access_denied', 'com_other', 'file_other', 'io_error'))
def test_removed_task_readback_accepts_only_api_not_found(tmp_path, engine, scenario):
    result = run_functions(tmp_path, ROOT/'INSTALL_THIS_PC.ps1', ['Remove-LabelScheduledTasks'], r'''
Add-Type -TypeDefinition @'
using System; using System.IO; using System.Runtime.InteropServices;
public class OtherFileFailure : FileNotFoundException {
 public OtherFileFailure() { HResult=unchecked((int)0x80070005); }
}
public class FreshTask {
 public string Xml="fixture";
 public string GetSecurityDescriptor(int flags) { return "fixture"; }
 public void Stop(int flags) {}
}
public class FreshFolder {
 public bool Removed=false; public string Scenario;
 public FreshTask GetTask(string name) {
  if(!Removed) return new FreshTask();
  switch(Scenario) {
   case "com_not_found": throw new COMException("missing",unchecked((int)0x80070002));
   case "file_not_found": throw new FileNotFoundException("missing");
   case "access_denied": throw new COMException("denied",unchecked((int)0x80070005));
   case "com_other": throw new COMException("failed",unchecked((int)0x80004005));
   case "file_other": throw new OtherFileFailure();
   default: throw new IOException("failed");
  }
 }
 public void DeleteTask(string name,int flags) { Removed=true; }
}
'@
$script:Folder=[FreshFolder]::new();$script:Folder.Scenario=$env:LM_FRESH_SCENARIO
function Get-LabelSupportedStartupTasks {
 param($Root)
 if(-not $script:Folder.Removed){return [pscustomobject]@{TaskName='owned'}}
 return @()
}
function Write-LabelStartupUpdateRecord {param($Path,$Payload)}
function Get-OwnedLegacyTaskRetirementSnapshot {param($Name) return $null}
$failed=$false
try{Remove-LabelScheduledTasks $script:Folder $env:TEMP 'fixture'}catch{$failed=$true}
if($failed -ne ($env:LM_FRESH_SCENARIO -notin @('com_not_found','file_not_found'))){throw 'Removal readback hid an error or refused exact not-found'}
'PASS'
''', engine=engine, values={'LM_FRESH_SCENARIO': scenario})
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('scenario', ('missing_key', 'missing_value', 'present', 'denied', 'bad_argument', 'enumeration_error', 'empty_value'))
def test_run_presence_query_does_not_hide_other_errors(tmp_path, engine, scenario):
    result = run_functions(tmp_path, ROOT/'INSTALL_THIS_PC.ps1', ['Test-CurrentUserRelayPersistencePresent'], r'''
function Get-Item {
 [CmdletBinding()]param($LiteralPath)
 switch($env:LM_FRESH_SCENARIO){
  'missing_key' {throw [Management.Automation.ItemNotFoundException]::new('missing key')}
  'denied' {throw [UnauthorizedAccessException]::new('denied')}
  'bad_argument' {throw [Management.Automation.PSArgumentException]::new('unrelated argument failure')}
 }
 $key=[pscustomobject]@{}
 $key | Add-Member ScriptMethod GetValueNames {
  if($env:LM_FRESH_SCENARIO -ceq 'enumeration_error'){throw [IO.IOException]::new('read failed')}
  if($env:LM_FRESH_SCENARIO -ceq 'missing_value'){return @()}
  return @('KMTech.LabelMatch.Relay')
 }
 $key | Add-Member ScriptMethod Dispose {}
 return $key
}
function Get-ItemPropertyValue {
 [CmdletBinding()]param($LiteralPath,$Name)
 switch($env:LM_FRESH_SCENARIO){
  'missing_key' {throw [Management.Automation.ItemNotFoundException]::new('missing key')}
  'missing_value' {throw [Management.Automation.PSArgumentException]::new('missing value')}
  'denied' {throw [UnauthorizedAccessException]::new('denied')}
  'bad_argument' {throw [Management.Automation.PSArgumentException]::new('unrelated argument failure')}
  'enumeration_error' {throw [IO.IOException]::new('read failed')}
  'empty_value' {return ''}
 }
 return 'fixture command'
}
$failed=$false;$actual=$null
try{$actual=Test-CurrentUserRelayPersistencePresent}catch{$failed=$true}
$expectedFailure=$env:LM_FRESH_SCENARIO -in @('denied','bad_argument','enumeration_error','empty_value')
if($failed -ne $expectedFailure){throw 'Registry query error classified as absence'}
if(-not $failed -and [bool]$actual -ne ($env:LM_FRESH_SCENARIO -ceq 'present')){throw 'Registry presence differs'}
'PASS'
''', engine=engine, values={'LM_FRESH_SCENARIO': scenario})
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('scenario', ('missing_key', 'missing_value', 'present', 'kind_io_error', 'names_denied', 'value_disappeared'))
def test_run_snapshot_uses_successful_name_observation_for_absence(tmp_path, engine, scenario):
    result = run_functions(tmp_path, ROOT/'INSTALL_CANONICAL_PORTABLE.ps1', ['Snapshot'], r'''
$fixture=[pscustomobject]@{}
$fixture | Add-Member ScriptMethod GetValueNames {
 if($env:LM_FRESH_SCENARIO -ceq 'names_denied'){throw [UnauthorizedAccessException]::new('denied')}
 if($env:LM_FRESH_SCENARIO -ceq 'missing_value'){return @()}
 return @('fixture')
}
$fixture | Add-Member ScriptMethod GetValueKind {
 param($Name)
 if($env:LM_FRESH_SCENARIO -in @('kind_io_error','missing_value')){throw [IO.IOException]::new('kind read failed')}
 return [Microsoft.Win32.RegistryValueKind]::String
}
$fixture | Add-Member ScriptMethod GetValue {
 param($Name,$Default,$Options)
 if($env:LM_FRESH_SCENARIO -ceq 'value_disappeared'){return $null}
 return 'fixture command'
}
$fixture | Add-Member ScriptMethod Dispose {}
$script:FixtureKey=if($env:LM_FRESH_SCENARIO -ceq 'missing_key'){$null}else{$fixture}
$RunName='fixture'
$definition=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Snapshot'},$true))[0].Extent.Text
$native='[Microsoft.Win32.Registry]::CurrentUser.OpenSubKey($RunKey, $false)'
if(-not $definition.Contains($native)){throw 'Registry read seam changed'}
. ([scriptblock]::Create($definition.Replace($native,'$script:FixtureKey')))
$failed=$false;$actual=$null
try{$actual=Snapshot}catch{$failed=$true}
$expectedFailure=$env:LM_FRESH_SCENARIO -in @('kind_io_error','names_denied','value_disappeared')
if($failed -ne $expectedFailure){throw 'Snapshot read failure classified as absence/presence'}
if(-not $failed -and [bool]$actual.exists -ne ($env:LM_FRESH_SCENARIO -ceq 'present')){throw 'Run snapshot presence differs'}
'PASS'
''', engine=engine, values={'LM_FRESH_SCENARIO': scenario})
    assert result.returncode == 0, result.stdout + result.stderr



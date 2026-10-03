"""Native registry status is injected before any host API call."""
from pathlib import Path

import pytest

from tests.test_shared_ancestor_protection import run_functions

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('source', ('INSTALL_THIS_PC.ps1', 'INSTALL_CANONICAL_PORTABLE.ps1'))
@pytest.mark.parametrize('status', (2, 5, 87, 1018, 0))
def test_native_open_accepts_only_exact_key_not_found(tmp_path, engine, source, status):
    result = run_functions(tmp_path, ROOT / source, ['Open-LabelRunKey'], r'''
$definition=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Open-LabelRunKey'},$true))[0].Extent.Text
$native='[KmtechLabelRunRegistryNative]::RegOpenKeyExW([IntPtr]::new(-2147483647), $SubKey, 0, 1, [ref]$handle)'
if(-not $definition.Contains($native)){throw 'Native status seam changed'}
# The emitted DllImport compiles in both engines, but is never invoked.
. ([scriptblock]::Create($definition.Replace($native,'[int]$env:LM_REGISTRY_STATUS')))
$failed=$false;$code=$null
try{$actual=Open-LabelRunKey 'Software\KMTech-sub158-test';if($null -ne $actual){throw 'Not-found returned a key'}}
catch{
 $failed=$true;$errorValue=$_.Exception
 for($i=0;$null -ne $errorValue -and $i -lt 8;$i++){
  if($errorValue -is [ComponentModel.Win32Exception]){$code=$errorValue.NativeErrorCode;break}
  $errorValue=$errorValue.InnerException
 }
}
$status=[int]$env:LM_REGISTRY_STATUS
if($failed -ne ($status -ne 2)){throw 'Native registry error became absence'}
if($status -notin @(0,2) -and $code -ne $status){throw 'Native registry error code lost'}
if($status -eq 0 -and -not $failed){throw 'Zero successful handle adopted'}
'PASS'
''', engine=engine, values={'LM_REGISTRY_STATUS': str(status)})
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('scenario', ('missing', 'denied', 'other_error'))
def test_snapshot_cannot_adopt_wrapper_null_after_native_error(tmp_path, engine, scenario):
    result = run_functions(tmp_path, ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1', ['Snapshot'], r'''
$definition=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Snapshot'},$true))[0].Extent.Text
function Open-LabelRunKey {param($SubKey)
 if($env:LM_REGISTRY_SCENARIO -ceq 'missing'){return $null}
 $code=if($env:LM_REGISTRY_SCENARIO -ceq 'denied'){5}else{1018}
 throw [ComponentModel.Win32Exception]::new($code)
}
# Preserve the old API's observed null result without querying host HKCU.
$old='[Microsoft.Win32.Registry]::CurrentUser.OpenSubKey($RunKey, $false)'
if($definition.Contains($old)){. ([scriptblock]::Create($definition.Replace($old,'$null')))}
$RunKey='fixture';$RunName='fixture';$failed=$false
try{$actual=Snapshot;if($actual.exists){throw 'Absent/error key adopted'}}catch{$failed=$true}
if($failed -ne ($env:LM_REGISTRY_SCENARIO -cne 'missing')){throw 'Unknown key was treated as absent'}
'PASS'
''', engine=engine, values={'LM_REGISTRY_SCENARIO': scenario})
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
def test_native_open_boundary_copies_have_same_body(tmp_path, engine):
    result = run_functions(tmp_path, ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1', ['Snapshot'], r'''
$bodies=@()
foreach($file in @('INSTALL_THIS_PC.ps1','INSTALL_CANONICAL_PORTABLE.ps1')){
 $tree=[Management.Automation.Language.Parser]::ParseFile((Join-Path $env:LM_REGISTRY_REPO $file),[ref]$tokens,[ref]$errors)
 if($errors.Count){throw 'Registry boundary parse failed'}
 $definitions=@($tree.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Open-LabelRunKey'},$true))
 if($definitions.Count -ne 1){throw 'Native registry open copy missing'}
 $bodies+=,$definitions[0].Body.Extent.Text
}
if($bodies[0] -cne $bodies[1]){throw 'Native registry open boundary drift'}
'PASS'
''', engine=engine, values={'LM_REGISTRY_REPO': str(ROOT)})
    assert result.returncode == 0, result.stdout + result.stderr

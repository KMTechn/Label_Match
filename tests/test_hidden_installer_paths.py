"""Real Hidden folder inputs for the product's read-only path admission."""

import json
from pathlib import Path

import pytest

from tests.test_shared_ancestor_protection import run_functions


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("case", ["evidence", "diagnostics", "bootstrap", "failure-sidecar"])
def test_product_walkers_accept_hidden_programdata_and_appdata(tmp_path, engine, case):
    source, functions = {
        "evidence": (ROOT / "INSTALL_CANONICAL_PORTABLE.ps1", ["Assert-LabelMachineEvidencePath"]),
        "diagnostics": (ROOT / "INSTALL_CANONICAL_PORTABLE.ps1", ["Initialize-LabelMachineDiagnostics"]),
        "bootstrap": (ROOT / "tools/bootstrap_integrity.ps1", ["Protect-KmtechCodeAncestors"]),
        "failure-sidecar": (ROOT / "tools/failure_redaction.ps1", ["Write-KmtechProtectedFailureSidecar"]),
    }[case]
    result = run_functions(tmp_path, source, functions, r'''
$root=[IO.Path]::GetFullPath($env:LM_HIDDEN_ROOT)
$programData=Join-Path $root 'ProgramData'
$appData=Join-Path $root 'profile\AppData'
foreach($path in @($programData,$appData)){
 [void][IO.Directory]::CreateDirectory($path)
 [IO.File]::SetAttributes($path,([IO.File]::GetAttributes($path) -bor [IO.FileAttributes]::Hidden))
 if(-not ([IO.File]::GetAttributes($path) -band [IO.FileAttributes]::Hidden)){throw 'Fixture is not hidden'}
 # The native counterfactual must fail on both supported engines.
 $failed=$false
 try{[void](Get-Item -LiteralPath $path -ErrorAction Stop)}catch{$failed=$true}
 if(-not $failed){throw 'Hidden fixture did not reproduce the native rejection'}
 $entry=Get-Item -LiteralPath $path -Force -ErrorAction Stop
 if(-not $entry.PSIsContainer -or ($entry.Attributes -band [IO.FileAttributes]::ReparsePoint)){throw 'Fixture scope differs'}
}
$testMode=$true
$env:PROGRAMDATA=$programData
$env:LOCALAPPDATA=Join-Path $appData 'Local'
[void][IO.Directory]::CreateDirectory($env:LOCALAPPDATA)
function Full($path,$purpose){return [IO.Path]::GetFullPath($path)}
# Only administrative ACL application/redaction is isolated; path reads are native.
function Set-Acl {[CmdletBinding()]param($LiteralPath,$AclObject)}
function ConvertTo-KmtechRedactedSinkText {param($Text)return $Text}
switch($env:LM_HIDDEN_CASE){
 'evidence'{
  $EvidencePath=Join-Path $programData 'KMTech\Label_Match\install-audit\evidence.json'
  Assert-LabelMachineEvidencePath
  if(Test-Path -LiteralPath $EvidencePath){throw 'Admission wrote evidence'}
 }
 'diagnostics'{
  Initialize-LabelMachineDiagnostics
  if(-not ([IO.Path]::GetTempPath()).StartsWith($programData+'\',[StringComparison]::OrdinalIgnoreCase)){throw 'Diagnostics escaped fixture'}
 }
 'bootstrap'{
  # Execute all three real Get-Item command expressions in the ACL ancestor walker.
  $definition=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Protect-KmtechCodeAncestors'},$true))[0]
  $reads=@($definition.FindAll({param($n)$n -is [Management.Automation.Language.CommandAst] -and $n.GetCommandName() -ceq 'Get-Item'},$true))
  if($reads.Count -ne 3){throw 'Bootstrap ancestor reads changed'}
  foreach($hidden in @($programData,$appData)){
   $ancestor=$hidden;$path=$hidden
   foreach($read in $reads){
    $entry=& ([scriptblock]::Create($read.Extent.Text))
    if(-not $entry.PSIsContainer -or -not ($entry.Attributes -band [IO.FileAttributes]::Hidden)){throw 'Bootstrap read differs'}
   }
  }
 }
 'failure-sidecar'{
  $env:TEMP=Join-Path $appData 'Local\Temp';$env:TMP=$env:TEMP
  [void][IO.Directory]::CreateDirectory($env:TEMP)
  $reference=Write-KmtechProtectedFailureSidecar -Text 'isolated diagnostic'
  if(-not ([string]$reference.path).StartsWith($env:TEMP+'\',[StringComparison]::OrdinalIgnoreCase)){throw 'Sidecar escaped fixture'}
 }
}
'PASS'
''', engine=engine, values={"LM_HIDDEN_ROOT": str(tmp_path), "LM_HIDDEN_CASE": case})
    (tmp_path / "hidden-native.json").write_text(json.dumps({"engine": engine, "case": case,
        "exit": result.returncode, "counterfactual_without_force": "required failure"}), encoding="utf-8")
    assert result.returncode == 0, "real hidden ancestor rejected; private native evidence retained"
    assert result.stdout.strip() == "PASS"


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
def test_native_free_package_check_sees_hidden_forbidden_child(tmp_path, engine):
    result = run_functions(tmp_path, ROOT / "tools/build_frozen_release_candidate.ps1",
        ["Assert-LowRiskNativeFreePackage"], r'''
$package=Join-Path $env:LM_HIDDEN_ROOT 'ProgramData\package'
$directory=Join-Path $package 'charset_normalizer'
[void][IO.Directory]::CreateDirectory($directory)
$file=Join-Path $directory 'hidden.pyd'
[IO.File]::WriteAllText($file,'isolated non-executable native-file witness')
[IO.File]::SetAttributes($file,([IO.File]::GetAttributes($file) -bor [IO.FileAttributes]::Hidden))
if(@(Get-ChildItem -LiteralPath $package -Recurse -File).Count -ne 0){throw 'Hidden child counterfactual differs'}
$definition=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Assert-LowRiskNativeFreePackage'},$true))[0]
$counterfactual=$definition.Extent.Text.Replace('Get-ChildItem -Force','Get-ChildItem')
. ([scriptblock]::Create($counterfactual))
$rejected=$false
try{[void](Assert-LowRiskNativeFreePackage $package)}catch{$rejected=$true}
if($rejected){throw 'Counterfactual did not omit the hidden child'}
. ([scriptblock]::Create($definition.Extent.Text))
$rejected=$false
try{[void](Assert-LowRiskNativeFreePackage $package)}catch{$rejected=$true}
if(-not $rejected){throw 'Product omitted the hidden forbidden child'}
'PASS'
''', engine=engine, values={"LM_HIDDEN_ROOT": str(tmp_path)})
    assert result.returncode == 0, "hidden child validation failed; native fixture retained"
    assert result.stdout.strip() == "PASS"

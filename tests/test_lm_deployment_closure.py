"""Actual stock copying/import closure and native PowerShell AST publishing guard."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from tools import build_portable_release_candidate as builder

ROOT = Path(__file__).resolve().parents[1]
NEW_MODULES = ("label_atomic_replace", "label_match_instance_win32", "label_native_policy")


@pytest.mark.parametrize("fault", ["none", "missing", "broken"])
def test_stock_copy_imports_new_modules_from_the_copied_application(fault):
    # iwrun2 owns this root; the allowlist fixes each complete -I argv/cwd.
    run_root = Path(os.environ["IWRUN_CONFIG"]).parent
    output = run_root / ("copy-smoke" + ("" if fault == "none" else "-" + fault))
    tools = builder._copy_application(ROOT, output / "app")
    site = output / "app/site-packages"
    site.mkdir()
    builder._copy_third_party(site)
    for name in NEW_MODULES:
        assert (output / "app" / (name + ".py")).read_bytes() == (ROOT / (name + ".py")).read_bytes()
    changed = output / "app/label_native_policy.py"
    if fault == "missing":
        changed.unlink()
    elif fault == "broken":
        changed.write_text("raise RuntimeError('copied module rejected')\n", encoding="utf-8")
    if fault == "none":
        builder._assert_portable_import_closure(output, ROOT, tools, Path(sys.executable))
    else:
        with pytest.raises(builder.PortableBuildError, match="import closure failed"):
            builder._assert_portable_import_closure(output, ROOT, tools, Path(sys.executable))


def test_installer_file_publication_uses_atomic_helper_and_ast_guards_direct_moves(tmp_path):
    driver = tmp_path / "guard.ps1"
    quote = lambda value: "'" + str(value).replace("'", "''") + "'"
    driver.write_text(r"""
$ErrorActionPreference='Stop'
$bad=@();$direct=0;$atomic=0
foreach($path in @(@PATHS@)){
 $tokens=$null;$errors=$null
 $ast=[Management.Automation.Language.Parser]::ParseFile($path,[ref]$tokens,[ref]$errors)
 if($errors.Count){throw 'PowerShell AST parse failed'}
 foreach($node in $ast.FindAll({param($n) $n -is [Management.Automation.Language.CommandAst]},$true)){
  $name=$node.GetCommandName()
  if($name -in @('Invoke-LabelAtomicReplace','Invoke-LabelPinnedAtomicReplace')){$atomic++}
  if(($name -replace '^.*\\','') -notin @('Move-Item','Rename-Item')){continue};$direct++
  $parent=$node.Parent;$guarded=$false
  while($null -ne $parent){
   if($parent -is [Management.Automation.Language.IfStatementAst] -and
      $parent.Clauses[0].Item1.Extent.Text -match '\A\[IO\.Directory\]::Exists\(' -and
      $node.Extent.StartOffset -ge $parent.Clauses[0].Item2.Extent.StartOffset -and
      $node.Extent.EndOffset -le $parent.Clauses[0].Item2.Extent.EndOffset){$guarded=$true;break}
   $parent=$parent.Parent
  }
  # These source operands are version directories, never file publishers.
  if(-not $guarded -and $node.Extent.Text -notmatch '-LiteralPath \$(stagingRoot|placementRoot|replacementRollbackRoot|installRootFull|parent) -Destination '){$bad+=1}
 }
}
@{bad=$bad.Count;direct=$direct;atomic=$atomic}|ConvertTo-Json -Compress
""".replace("@PATHS@", ",".join(quote(ROOT / name) for name in
            ["INSTALL_THIS_PC.ps1", "INSTALL_CANONICAL_PORTABLE.ps1"])), encoding="utf-8-sig")
    child = subprocess.run([shutil.which("powershell"), "-NoProfile", "-NonInteractive", "-File", str(driver)],
                           cwd=tmp_path, capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    assert child.returncode == 0
    result = json.loads(child.stdout.decode("utf-8-sig"))
    assert result["bad"] == 0
    assert result["direct"] >= 5 and result["atomic"] >= 7


@pytest.mark.parametrize("engine", ["powershell", "pwsh"])
@pytest.mark.parametrize("publisher", ["Save", "Save-LabelMachineMetadata"])
def test_actual_installer_publication_tolerates_a_read_overlap(tmp_path, engine, publisher):
    from tests.test_shared_ancestor_protection import run_functions
    result = run_functions(tmp_path, ROOT / "INSTALL_CANONICAL_PORTABLE.ps1", [publisher], r"""
function ConvertTo-KmtechRedactedSinkObject($Value){return $Value}
function ConvertTo-KmtechPublicDiagnosticTree($Value){return $Value}
function Assert-KmtechMachineMetadataText([string]$Text){}
Add-Type -TypeDefinition @'
using System.IO;using System.Threading;
public static class PublishingReader {
 public static void Hold(string path){
  var reader=File.Open(path,FileMode.Open,FileAccess.Read,FileShare.Read);
  new Thread(()=>{Thread.Sleep(150);reader.Dispose();}){IsBackground=true}.Start();
 }
}
'@
$path=Join-Path $env:LM_PUBLICATION_ROOT 'published.json'
[IO.File]::WriteAllText($path,'{}')
[PublishingReader]::Hold($path)
& $env:LM_PUBLICATION_FUNCTION $path @{status='PASS'}
$record=[IO.File]::ReadAllText($path)|ConvertFrom-Json
if($record.status -cne 'PASS'){throw 'published bytes differ'}
'PASS'
""", engine=engine, values={"LM_PUBLICATION_ROOT": str(tmp_path),
                           "LM_PUBLICATION_FUNCTION": publisher})
    assert result.returncode == 0
    assert result.stdout.strip() == "PASS"

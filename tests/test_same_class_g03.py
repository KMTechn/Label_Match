"""G03: inspect actual isolated shortcut publication without launching its target."""
import json
import os
import re
from pathlib import Path

from tests.test_shared_ancestor_protection import run_functions
from tests.test_worker_shortcuts import HELPER, NAMES


def test_worker_shortcut_targets_windowless_runtime_directly(tmp_path):
    result = run_functions(tmp_path, HELPER, NAMES, r'''
$script:fixtureRoot=$env:LM_G03_ROOT
$root=Join-Path $script:fixtureRoot 'Apps/Label_Match/current'
$desktop=Join-Path $script:fixtureRoot 'Desktop'
$record=Join-Path $script:fixtureRoot 'audit/shortcuts.json'
foreach($folder in @($desktop,(Join-Path $root 'app/assets'),(Join-Path $root 'runtime'),(Split-Path -Parent $record))){[void][IO.Directory]::CreateDirectory($folder)}
foreach($file in @('launch-label-match.cmd','app/main.py','runtime/pythonw.exe','app/assets/logo.ico')){[IO.File]::WriteAllText((Join-Path $root $file),'inert fixture; never execute')}
function Get-LabelWorkerShortcutFolders {param([switch]$CommonOnly,[switch]$UserOnly)
 [pscustomobject]@{name='CommonDesktop';path=(Join-Path $script:fixtureRoot 'Desktop');recursive=$false}
}
function Get-FileHash {param($LiteralPath)
 $algorithm=[Security.Cryptography.SHA256]::Create()
 try{[pscustomobject]@{Hash=([BitConverter]::ToString($algorithm.ComputeHash([IO.File]::ReadAllBytes($LiteralPath)))).Replace('-','')}}finally{$algorithm.Dispose()}
}
$published=Install-LabelWorkerShortcuts $root ([pscustomobject]@{launcher='launch-label-match.cmd'}) $record
$paths=@($published.published)
if($paths.Count -ne 1){throw 'isolated shortcut was not published'}
$actual=Get-LabelShortcutProperties $paths[0].shortcut_path
[ordered]@{status=$published.status;target=$actual.target_path;arguments=$actual.arguments}|ConvertTo-Json -Compress
''', values={"LM_G03_ROOT": str(tmp_path)})
    evidence = Path(os.environ.get("KMTECH_TEST_CA_EVIDENCE_ROOT") or tmp_path)
    if result.returncode:
        (evidence / "g03-harness.json").write_text(json.dumps({"rc": result.returncode,
            "error_ids": re.findall(r"FullyQualifiedErrorId\s*:\s*([^\r\n]+)", result.stderr)}), encoding="utf-8")
    assert result.returncode == 0, "isolated shortcut publication harness failed"
    observed = json.loads(result.stdout)
    (evidence / "g03-observation.json").write_text(json.dumps(observed), encoding="utf-8")
    assert observed["status"] == "PASS"
    assert Path(observed["target"]).name.lower() == "pythonw.exe", observed
    assert "app" in observed["arguments"] and "main.py" in observed["arguments"]

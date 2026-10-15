"""G02: equivalent scheduler enum readbacks must keep ownership recognizable."""
import json
import os
from pathlib import Path

import pytest

from tests.test_shared_ancestor_protection import ROOT, run_functions


@pytest.mark.parametrize("numeric", [False, True], ids=["named", "numeric"])
def test_supported_update_task_accepts_equivalent_scheduler_enums(tmp_path, numeric):
    result = run_functions(
        tmp_path, ROOT / "tools/apply_prepared_update.ps1",
        ["Get-LabelSupportedStartupTasks"], r'''
$root=$env:LM_G02_ROOT
$sid='S-1-5-21-101-102-103-1001'
$sha=[Security.Cryptography.SHA256]::Create()
try{$digest=([BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes($root.ToLowerInvariant()+[char]10+$sid)))).Replace('-','').ToLowerInvariant()}
finally{$sha.Dispose()}
$helper=Join-Path $root 'tools\apply_prepared_update.ps1'
$arguments='-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File "'+$helper+'" -InstallRoot "'+$root+'"'
$logon='ServiceAccount';$level='Highest'
if($env:LM_G02_NUMERIC -eq '1'){$logon=5;$level=1}
$script:task=[pscustomobject]@{
 TaskName=('KMTech.LabelMatch.StartupUpdate.'+$digest.Substring(0,24));TaskPath='\';
 Principal=[pscustomobject]@{UserId='S-1-5-18';LogonType=$logon;RunLevel=$level};
 Triggers=@([pscustomobject]@{UserId=$sid});
 Actions=@([pscustomobject]@{Execute=(Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\v1.0\powershell.exe');Arguments=$arguments;WorkingDirectory=''})
}
function Get-ScheduledTask {param($ErrorAction) $script:task}
$code='';$count=0
try{$count=@(Get-LabelSupportedStartupTasks $root).Count}catch{$code=$_.Exception.Message}
[ordered]@{count=$count;code=$code}|ConvertTo-Json -Compress
''', values={"LM_G02_ROOT": str(tmp_path), "LM_G02_NUMERIC": str(int(numeric))})
    assert result.returncode == 0, "isolated task readback harness failed"
    observed = json.loads(result.stdout)
    (Path(os.environ.get("KMTECH_TEST_CA_EVIDENCE_ROOT") or tmp_path) / ("g02-" + str(numeric) + ".json")).write_text(
        json.dumps(observed), encoding="utf-8")
    assert observed == {"count": 1, "code": ""}, observed

"""Execute the production query dispatch/supervisor, with pure query doubles."""
import json
import os
from pathlib import Path
import subprocess

import pytest

import auto_update


def _source():
    override = os.environ.get("KMTECH_LIVE_STOP_QUERY_SOURCE")
    return Path(override or Path(__file__).parents[1] / "tools/apply_prepared_update.ps1").read_text(encoding="utf-8-sig")


def _supervisor(source):
    start = source.find("function Invoke-LabelStartupReadOnlyQuery {")
    return "" if start < 0 else source[start:source.index("if ($SupportedLifecycle -and", start)]


def _powershell(script):
    completed = subprocess.run([
        auto_update.system_executable("WindowsPowerShell/v1.0/powershell.exe"),
        "-NoLogo", "-NoProfile", "-NonInteractive", "-WindowStyle", "Hidden", "-Command", script,
    ], capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW)
    assert completed.returncode == 0
    return json.loads(completed.stdout)


def test_process_query_wall_timeout_is_unknown(tmp_path):
    source = _source()
    start = source.index("if ($Mode -ceq 'Observe') {")
    block = source[start:source.index("if ($Mode -ceq 'Verify')", start)].replace("    exit 0", "")
    script = _supervisor(source) + r'''
function Assert-LabelStartupNoReparsePoint {}
function Get-StartupUpdateProcesses { Start-Sleep -Seconds 11; return ,@() }
$authority=[pscustomobject]@{root='X:\isolated';policy=@{operator_sid=''}}
$Mode='Observe'
$clock=[Diagnostics.Stopwatch]::StartNew()
$status='RETURNED'
try { $ignored = & { BODY } } catch { $status='UNKNOWN' }
@{status=$status;seconds=$clock.Elapsed.TotalSeconds} | ConvertTo-Json -Compress
'''.replace("BODY", block)
    result = _powershell(script)
    (tmp_path / "wall-evidence.json").write_text(json.dumps(result, sort_keys=True), encoding="utf-8")
    assert result["status"] == "UNKNOWN"
    # Real wall observation, separate from the model's ten-second wait argument.
    assert 10 <= result["seconds"] < 12


@pytest.mark.parametrize("case", ["complete", "timeout", "empty", "failed", "wrong-pid", "other-generation"])
def test_query_supervisor_never_accepts_incomplete_or_unowned_results(case):
    source = _source()
    script = _supervisor(source) + r'''
function Assert-LabelStartupNoReparsePoint {}
function Get-StartupUpdateProcesses { return ,@() }
$case='CASE'
$p=[pscustomobject]@{
 StartInfo=[Diagnostics.ProcessStartInfo]::new();Handle=[IntPtr]42;Id=42;
 StartTime=[datetime]'2026-10-22T00:00:00Z';HasExited=$false;ExitCode=0;
 StandardOutput=[IO.StringReader]::new('[]');StandardError=[IO.StringReader]::new('');
 Killed=$false;Disposed=$false;Waits=@()
}
if($case -eq 'wrong-pid'){$p.Id=0}
if($case -eq 'empty'){$p.StandardOutput=[IO.StringReader]::new('')}
if($case -eq 'failed'){$p.ExitCode=1}
$p | Add-Member ScriptMethod Start { return $true }
$p | Add-Member ScriptMethod WaitForExit {
 param($milliseconds)
 $this.Waits+=@($milliseconds)
 if($null -eq $milliseconds){$this.HasExited=$true;return}
 if($case -eq 'other-generation'){$this.StartTime=$this.StartTime.AddSeconds(1);return $false}
 if($case -eq 'timeout'){return $false}
 $this.HasExited=$true;return $true
}
$p | Add-Member ScriptMethod Kill { $this.Killed=$true;$this.HasExited=$true }
$p | Add-Member ScriptMethod Dispose { $this.Disposed=$true }
$status='RETURNED'
try{$ignored=Invoke-LabelStartupReadOnlyQuery -QueryMode Observe -Root 'X:\isolated' -NewProcess {$p}}
catch{$status='UNKNOWN'}
@{status=$status;killed=$p.Killed;waits=$p.Waits} | ConvertTo-Json -Compress
'''.replace("CASE", case)
    result = _powershell(script)
    assert result["status"] == ("RETURNED" if case == "complete" else "UNKNOWN")
    assert result["killed"] is (case == "timeout")
    if case != "wrong-pid":
        assert result["waits"][0] == 10000


@pytest.mark.parametrize("raw", ["", "[", "null", "{}"])
def test_process_snapshot_partial_output_never_means_absence(monkeypatch, tmp_path, raw):
    monkeypatch.setattr(auto_update, "_run_fixed_helper", lambda *_a, **_k: raw)
    with pytest.raises((ValueError, TypeError)):
        auto_update.process_snapshot(tmp_path)


def test_live_product_parent_keeps_official_removal_guard_closed():
    source = (Path(__file__).parents[1] / "INSTALL_THIS_PC.ps1").read_text(encoding="utf-8-sig")
    functions = []
    for name in ("Test-LabelRemovalProcessReferencesRoot", "Assert-LabelRemovalQuiescent"):
        start = source.index("function " + name + "(")
        functions.append(source[start:source.index("\nfunction ", start + 1)])
    script = "\n".join(functions) + r'''
function Get-StrictFullPath { param($Path,$Purpose); return [IO.Path]::GetFullPath($Path) }
function Get-LabelVerifiedRemovalProcessChain { return @() }
function Get-CimInstance {
 [pscustomobject]@{ProcessId=424242;ExecutablePath='X:\isolated\versions\test\runtime\python.exe';CommandLine=''}
}
$status='ALLOWED'
try{Assert-LabelRemovalQuiescent 'X:\isolated'}catch{$status=$_.Exception.Message}
@{refused=($status -ceq 'REMOVAL_REQUIRES_PRODUCT_PROCESS_STOP')} | ConvertTo-Json -Compress
'''
    assert _powershell(script)["refused"] is True

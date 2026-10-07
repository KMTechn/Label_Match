"""Slow CIM observations cannot extend either child's shared stop deadline."""
import json
from pathlib import Path

import pytest

from tests.test_shared_ancestor_protection import run_functions


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('kind', ('registration', 'helper'))
def test_slow_ownership_observation_returns_unproven_without_stopping_child(tmp_path, engine, kind):
    installer = Path(__file__).resolve().parents[1] / 'INSTALL_CANONICAL_PORTABLE.ps1'
    result = run_functions(tmp_path, installer,
        ['Stop-LabelRegistrationChild', 'Wait-LabelRegistrationChild', 'Wait-LabelPlacementChild'], r"""
$start=[Diagnostics.ProcessStartInfo]::new()
$start.FileName=Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell/v1.0/powershell.exe'
$start.Arguments='-NoLogo -NoProfile -NonInteractive -Command Start-Sleep -Seconds 55'
$start.UseShellExecute=$false;$start.CreateNoWindow=$true
$child=[Diagnostics.Process]::Start($start)
$parent=[Diagnostics.Process]::GetCurrentProcess()
function Get-CimInstance {
 param($ClassName,$Filter,$OperationTimeoutSec,$ErrorAction)
 # This deliberately ignores the CIM timeout, modelling a stalled provider.
 [IO.File]::WriteAllText($env:LM_BUDGET_QUERY,[string]$OperationTimeoutSec)
 Start-Sleep -Seconds 35
 CimCmdlets\Get-CimInstance @PSBoundParameters
}
$clock=[Diagnostics.Stopwatch]::StartNew()
$unproven=$false;$verified=$false
try {
 try {
  if($env:LM_BUDGET_KIND -ceq 'helper'){[void](Wait-LabelPlacementChild $child $parent 250)}
  else {Wait-LabelRegistrationChild $child 250}
 }catch{
  $prefix=if($env:LM_BUDGET_KIND -ceq 'helper'){'helper'}else{'registration'}
  $unproven=$_.Exception.Data.Contains($prefix+'_child_unproven')
  $verified=$_.Exception.Data.Contains($prefix+'_child_exit_verified')
 }
 $elapsed=$clock.Elapsed.TotalSeconds
 $alive=-not $child.HasExited
 [void]$child.WaitForExit(60000)
 [ordered]@{elapsed_seconds=$elapsed;exit_unproven=$unproven;exit_verified=$verified;
  child_alive_at_return=$alive;natural_exit_code=$child.ExitCode;
  total_seconds=$clock.Elapsed.TotalSeconds}|ConvertTo-Json -Compress
}finally{$child.Dispose();$parent.Dispose()}
""", engine=engine, values={'LM_BUDGET_KIND': kind, 'LM_BUDGET_QUERY': str(tmp_path / 'query-timeout.txt')})
    (tmp_path / 'budget.stdout').write_text(result.stdout, encoding='utf-8')
    (tmp_path / 'budget.stderr').write_text(result.stderr, encoding='utf-8')
    assert result.returncode == 0, result.stderr
    record = json.loads(result.stdout)
    assert record['exit_unproven'] and not record['exit_verified']
    assert record['child_alive_at_return'] and record['natural_exit_code'] == 0
    assert 29 <= record['elapsed_seconds'] <= 32, record
    assert record['total_seconds'] >= 54, record
    assert 1 <= int((tmp_path / 'query-timeout.txt').read_text()) <= 30

"""One admitted source shared by real placement/removal; no installer execution."""
from pathlib import Path

import pytest

from tests.test_shared_ancestor_protection import run_functions
from writer_sink_inventory import derive_writer_sink_inventory, WriterSinkInventoryError

ROOT = Path(__file__).resolve().parents[1]


def test_actual_canonical_placement_inventory_has_one_admitted_source():
    rows = derive_writer_sink_inventory(ROOT)
    sources = [row.source for row in rows]
    assert len(sources) == len(set(sources))
    placement = [row for row in rows if row.source == 'canonical_placement']
    assert len(placement) == 1
    assert placement[0].source_path == 'INSTALL_THIS_PC.ps1'
    assert placement[0].guard_kind == 'powershell_delegated_operation'


def test_duplicate_source_is_still_rejected_at_another_caller(tmp_path):
    caller = "Enter-LabelWriterDelegatedOperation -Source 'canonical_placement'\n"
    (tmp_path / 'first.ps1').write_text(caller, encoding='utf-8')
    (tmp_path / 'second.ps1').write_text(caller, encoding='utf-8')
    with pytest.raises(WriterSinkInventoryError, match='duplicated: canonical_placement'):
        derive_writer_sink_inventory(tmp_path)


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('timeout', (5000, 120000))
def test_actual_shared_admission_preserves_exact_delegation(tmp_path, engine, timeout):
    result = run_functions(tmp_path, ROOT / 'INSTALL_THIS_PC.ps1', ['Enter-LabelCanonicalPlacement'], r'''
$WriterFenceControlRoot='control';$WriterFenceSessionId='session';$WriterFenceAttemptId='attempt'
$WriterFenceReplacementTransactionId='replacement';$WriterFenceDelegationToken='synthetic-local-only'
$script:entered=0
function Enter-LabelWriterDelegatedOperation {
 param($ControlRoot,$SessionId,$AttemptId,$ReplacementTransactionId,$DelegationToken,$Source,$TimeoutMilliseconds)
 if($ControlRoot -cne 'control' -or $SessionId -cne 'session' -or $AttemptId -cne 'attempt' -or
    $ReplacementTransactionId -cne 'replacement' -or $DelegationToken -cne 'synthetic-local-only' -or
    $Source -cne 'canonical_placement' -or $TimeoutMilliseconds -ne [int]$env:LM_PLACEMENT_TIMEOUT){throw 'Delegation tuple differs'}
 $script:entered++;return [pscustomobject]@{fixture=$true}
}
if([int]$env:LM_PLACEMENT_TIMEOUT -eq 5000){$lease=Enter-LabelCanonicalPlacement}
else{
 $assignments=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.AssignmentStatementAst] -and
  $n.Left.Extent.Text -ceq '$writerFenceLease' -and $n.Right.Extent.Text -match 'Enter-LabelCanonicalPlacement -TimeoutMilliseconds'},$true))
 if($assignments.Count -ne 1){throw 'Actual placement admission call missing'}
 . ([scriptblock]::Create($assignments[0].Extent.Text));$lease=$writerFenceLease
}
if($script:entered -ne 1 -or -not $lease.fixture){throw 'Admission result lost/duplicated'}
'PASS'
''', engine=engine, values={'LM_PLACEMENT_TIMEOUT': str(timeout)})
    assert result.returncode == 0, result.stdout + result.stderr

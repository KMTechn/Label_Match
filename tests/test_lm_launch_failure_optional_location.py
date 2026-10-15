"""Optional placement metadata cannot replace the first native failure."""
from pathlib import Path

import pytest

from tests.test_shared_ancestor_protection import run_functions


INSTALLER = Path(__file__).resolve().parents[1] / "INSTALL_CANONICAL_PORTABLE.ps1"


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("record_kind", ["dictionary", "object"])
@pytest.mark.parametrize("with_location", [False, True])
def test_launch_failure_preserves_cause_without_optional_location(
    tmp_path, engine, record_kind, with_location
):
    result = run_functions(tmp_path, INSTALLER, ["Get-LabelLaunchFailure"], r'''
Set-StrictMode -Version Latest
function Write-KmtechProtectedFailureSidecar {throw 'Isolated evidence sink unavailable'}
$metadata=@{placement_code='PLACEMENT_CHILD_FAILED';phase='HELPER_EXECUTION';
            exit_code=70;exception_type='System.InvalidOperationException'}
if($env:LM_OPTIONAL_LOCATION -ceq '1'){$metadata.location_function='VerifiedPlacementPhase'}
if($env:LM_OPTIONAL_KIND -ceq 'object'){$metadata=[pscustomobject]$metadata}
try {
    $failure=[InvalidOperationException]::new('Isolated first placement failure')
    $failure.Data['placement_child_failure']=$metadata
    throw $failure
} catch {$record=Get-LabelLaunchFailure $_ 'forward'}
$expectedLocation=if($env:LM_OPTIONAL_LOCATION -ceq '1'){'VerifiedPlacementPhase'}else{'forward'}
if($record.status -cne 'FAILED' -or $record.placement_code -cne 'PLACEMENT_CHILD_FAILED'){
    throw 'First placement failure was replaced'
}
if($record.exception_type -cne 'System.InvalidOperationException' -or
   $record.location_function -cne $expectedLocation -or $record.report_status -cne 'REPORT_UNAVAILABLE'){
    throw 'Optional metadata changed failure evidence'
}
'PRESERVED'
''', engine=engine, values={"LM_OPTIONAL_LOCATION": "1" if with_location else "0",
                           "LM_OPTIONAL_KIND": record_kind})
    assert result.returncode == 0, result.stderr
    assert "PRESERVED" in result.stdout

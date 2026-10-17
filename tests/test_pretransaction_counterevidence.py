"""Contradictory transaction evidence must remain installation residue."""
import pytest
from tests.test_shared_ancestor_protection import ROOT, run_functions


@pytest.mark.parametrize('engine', ['powershell', 'pwsh'])
@pytest.mark.parametrize('evidence', ['setup', 'fence', 'empty-fence', 'absent'])
def test_admission_failure_rejects_transaction_counterevidence(tmp_path, engine, evidence):
    result = run_functions(tmp_path, ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1',
        ['Test-PretransactionInstallAudit'], r'''
$auditRoot=Join-Path $env:TEMP ('audit-'+[guid]::NewGuid().ToString('N'))
[void][IO.Directory]::CreateDirectory($auditRoot)
$record=[ordered]@{schema='label-match-canonical-portable-install-v1';status='FAILED';preparation_status='FAILED_PREPARATION_ADMISSION';code_placement='NOT_STARTED';rollback=@{applied=$false}}
switch($env:COUNTEREVIDENCE){
 'setup' {$record.current_user_setup_started=$true}
 'fence' {$record.writer_fence=@{session_id='test-session'}}
 'empty-fence' {$record.writer_fence=$null}
}
$path=Join-Path $auditRoot 'canonical-portable-test-run.json'
[IO.File]::WriteAllText($path,($record|ConvertTo-Json -Depth 5),[Text.UTF8Encoding]::new($false))
$before=[IO.File]::ReadAllText($path)
$admitted=Test-PretransactionInstallAudit $auditRoot (Join-Path $auditRoot 'active-absent')
if($admitted -ne ($env:COUNTEREVIDENCE -ceq 'absent')){throw 'COUNTEREVIDENCE_ADMISSION'}
if([IO.File]::ReadAllText($path) -cne $before){throw 'AUDIT_CHANGED'}
''', engine=engine, values={'COUNTEREVIDENCE': evidence})
    assert result.returncode == 0, 'counterevidence admission failed; values withheld'

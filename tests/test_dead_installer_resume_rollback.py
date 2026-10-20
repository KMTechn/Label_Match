"""G99b packaging: a dead unfinished installer that saved its recovery context resumes its rollback.

G99 let the supported user removal settle the fence of an installer that died
before anything was recorded.  A run that died while it was changing code
(PREIMAGE_SAVED, fence QUIESCING/INSTALLING/RESTORING) must not lose that
signal: the installer now saves its recovery context before its first change,
the user-removal door refuses such a run and points to the supported rollback
resume, and the resume accepts the dead run after the same death proof.
"""
import json
import os
import re
from pathlib import Path

import pytest

import writer_session_fence as fence
from tests.test_shared_ancestor_protection import run_functions
from tests.test_dead_installer_fence_entries import (  # noqa: F401  (fixtures)
    carried, fence_only_removal, stuck, _remove, _resume_gate, _bind_live_owner, _native_started,
    failed_install,
)

pytestmark = pytest.mark.skipif(os.name != 'nt', reason='Windows named mutex required')

ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1'
CONTEXT = {'operator_sid': 'S-1-5-21-1-2-3-1001', 'source_root': 'x', 'values': {}}


def _with_context(stuck, fence_status=None):
    stuck.record['recovery_context'] = CONTEXT
    stuck.audit.write_text(json.dumps(stuck.record), encoding='utf-8')
    if fence_status:
        stuck.payload['status'] = fence_status
        (stuck.control / fence.ACTIVE_FILENAME).write_text(json.dumps(stuck.payload), encoding='utf-8')
        stuck.original = (stuck.control / fence.ACTIVE_FILENAME).read_bytes()


def test_user_removal_refuses_a_dead_run_that_saved_its_context_and_points_to_resume(stuck):
    _with_context(stuck)
    events = []
    with pytest.raises(fence.WriterFencedError) as refused:
        _remove(events)
    assert refused.value.code == 'RESUME_ROLLBACK_REQUIRED'
    assert events == []
    assert (stuck.control / fence.ACTIVE_FILENAME).read_bytes() == stuck.original
    assert not (stuck.control / 'settled').exists()


@pytest.mark.parametrize('fence_status', ('QUIESCING', 'INSTALLING', 'RESTORING'))
def test_resume_proof_accepts_a_dead_unfinished_run_with_context_and_changes_nothing(stuck, fence_status):
    _with_context(stuck, fence_status)
    assert fence.prove_dead_unfinished_installer(str(stuck.audit)) is True
    assert (stuck.control / fence.ACTIVE_FILENAME).read_bytes() == stuck.original
    assert not (stuck.control / 'settled').exists()
    assert stuck.audit.is_file()


@pytest.mark.parametrize('case', (
    'no-context', 'live-owner', 'live-owner-other-start', 'writer-or-relay-present', 'no-owner-record',
    'other-audit-path', 'COMPLETE', 'FAILED_ROLLED_BACK', 'fence-restore-failed', 'duplicate-record',
))
def test_resume_proof_stays_closed_unless_death_idle_and_context_are_proven(stuck, case):
    _with_context(stuck)
    trusted_path = next(fence._installer_owner_root().glob('*.json'))
    target = str(stuck.audit)
    if case == 'no-context':
        stuck.record.pop('recovery_context')
        stuck.audit.write_text(json.dumps(stuck.record), encoding='utf-8')
    elif case == 'live-owner':
        _bind_live_owner(stuck.audit, stuck.record)
    elif case == 'live-owner-other-start':
        record = stuck.record
        record['writer_fence']['owner_process_id'] = os.getpid()
        record['writer_fence']['owner_process_started_at_filetime'] = str(int(_native_started(os.getpid())) + 1)
        stuck.audit.write_text(json.dumps(record), encoding='utf-8')
        trusted = json.loads(trusted_path.read_text(encoding='utf-8'))
        trusted.update({key: record['writer_fence'][key] for key in ('owner_process_id', 'owner_process_started_at_filetime')})
        trusted_path.write_text(json.dumps(trusted), encoding='utf-8')
    elif case == 'writer-or-relay-present':
        stuck.idle = False
    elif case == 'no-owner-record':
        trusted_path.unlink()
    elif case == 'other-audit-path':
        other = stuck.audit.parent / 'canonical-portable-other.json'
        other.write_text(json.dumps(dict(stuck.record, run_id='other')), encoding='utf-8')
        target = str(other)
    elif case in ('COMPLETE', 'FAILED_ROLLED_BACK'):
        stuck.record['status'] = case
        stuck.audit.write_text(json.dumps(stuck.record), encoding='utf-8')
    elif case == 'fence-restore-failed':
        _with_context(stuck, 'RESTORE_FAILED')
    else:
        (stuck.audit.parent / 'canonical-portable-duplicate.json').write_text(json.dumps(stuck.record), encoding='utf-8')
    assert fence.prove_dead_unfinished_installer(target) is False
    assert (stuck.control / fence.ACTIVE_FILENAME).read_bytes() == stuck.original


def test_resume_gate_admits_an_unfinished_audit_status(stuck, tmp_path):
    _with_context(stuck)
    # The status gate no longer rejects a run that is only unfinished; the
    # context check that follows is the next (and here failing) one.
    assert _resume_gate(tmp_path, stuck.audit, stuck.record) == 'REFUSED:Rollback context belongs to a different user or installer.'


def test_installer_saves_the_recovery_context_before_its_first_change():
    text = INSTALLER.read_text(encoding='utf-8')
    body = text[text.index('$mutated = $false\n$Script:LabelRollbackCandidateRelays'):]
    start = body.index('$audit.writer_fence = [ordered]@{')
    first_change = body.index('Stop-LabelInstallerRelayPreimage $old')
    pre = body[start:first_change]
    # One helper saves the context with the fence record and reads it back.
    assert re.search(r'^\s*\. Save-LabelPreChangeRecoveryContext\s*$', pre, re.M), 'context is not saved before the first change'
    helper = text[text.index('function Save-LabelPreChangeRecoveryContext'):]
    helper = helper[:helper.index('\nfunction ')]
    assert helper.index('Save-LabelRollbackContext') < helper.index('Save $auditPath $audit') < helper.index('readback differs')


@pytest.mark.parametrize('engine', ['powershell.exe', 'pwsh.exe'])
def test_pre_change_context_is_read_back_and_later_drift_is_recorded(tmp_path, engine):
    result = run_functions(tmp_path, INSTALLER, ['Save-LabelRollbackContext', 'Save-LabelPreChangeRecoveryContext'], r"""
$root=$env:LM_CTX_ROOT
$generationSubjectSid='S-1-5-21-1';$source='src';$writerOrchestratorSha256='o';$writerContractSha256='c';$generationMarkerPaths=@('m')
$install=Join-Path $root 'current';$existingVerified=$true
New-Item -ItemType Directory -Path (Join-Path $root '.current.rollback.1') | Out-Null
$before=[pscustomobject]@{exists=$true;kind='String';data='x'};$replacementRollbackRoot=''
$auditPath=Join-Path $root 'a.json'
$audit=[ordered]@{status='PREIMAGE_SAVED';rollback=[ordered]@{}}
function Save {param($Path,$Value) [IO.File]::WriteAllText($Path,($Value|ConvertTo-Json -Depth 30),[Text.UTF8Encoding]::new($false))}
. Save-LabelPreChangeRecoveryContext
$saved=Get-Content -LiteralPath $auditPath -Raw|ConvertFrom-Json
if($saved.recovery_context.operator_sid -cne 'S-1-5-21-1' -or @($saved.recovery_context.values.replacementBackupsBefore).Count -ne 1){throw 'Pre-change context was not saved'}
$replacementRollbackRoot=Join-Path $root '.current.rollback.2'
Save-LabelRollbackContext
if($audit.rollback.Contains('recovery_context_drift')){throw 'A refined preimage folder is not drift'}
$before=[pscustomobject]@{exists=$true;kind='String';data='changed'}
Save-LabelRollbackContext
if(@($audit.rollback.recovery_context_drift) -notcontains 'before'){throw 'Changed preimage was not recorded'}
'PASS'
""", engine=engine, values={'LM_CTX_ROOT': str(tmp_path)})
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip().endswith('PASS')

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
    assert re.search(r'Save-LabelRollbackContext', pre), 'context is not saved before the first change'
    assert pre.index('Save-LabelRollbackContext') < pre.index('Save $auditPath $audit')

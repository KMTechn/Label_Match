"""G99c packaging: a dead installer is never stuck, whichever step it was killed at.

The installer is killed at four steps: (1) before the fence exists, (2) after
the fence but before the protected owner record, (3) while code is copied (owner
record present) and (4) while the rollback runs (fence RESTORING).  Before G99c
the fence entries recognized the dead run only through a ``writer_fence`` tuple
in its audit, which the installer saved after it had created the fence; a run
killed in between left an audit without it.  The entries now also accept the
run when the *protected owner record* equals the fence and the audit of the same
run_id carries no fence tuple at all.  Another run's fence or a contradicting
tuple is still refused.

Each case reports the four entries: user removal, official reinstall (fence
start), supported rollback resume (death proof) and the app start admission.
"""
import json
import os
import re
from pathlib import Path

import pytest

import writer_session_fence as fence
from tests.test_dead_installer_fence_entries import (  # noqa: F401  (fixtures)
    carried, fence_only_removal, _remove, _start_new_fence, _ordinary_writer_refused,
)
from tests.test_failed_deployment_removal import failed_install  # noqa: F401

pytestmark = pytest.mark.skipif(os.name != 'nt', reason='Windows named mutex required')

ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1'
CONTEXT = {'operator_sid': 'S-1-5-21-1-2-3-1001', 'source_root': 'x', 'values': {}}
# The machine (worker kit) path has no user-path rollback inputs to resume from:
# its context only marks the kind, so a dead run is settled like a no-context run.
MACHINE_CONTEXT = {'kind': 'MACHINE_WORKER_DEFERRED', 'operator_sid': 'S-1-5-21-1-2-3-1001', 'source_root': 'x'}
MACHINE_INSTALLER = ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1'
HELPER = ROOT / 'INSTALL_THIS_PC.ps1'

# stage -> (fence status, audit status, audit keeps tuple, context saved, owner record present)
CASES = {
    'S3-code-copy-no-tuple-no-context': ('INSTALLING', 'PREIMAGE_SAVED', False, False, True),
    'S3-code-copy-no-tuple-context': ('INSTALLING', 'PREIMAGE_SAVED', False, True, True),
    'S3-code-copy-tuple-context': ('INSTALLING', 'PREIMAGE_SAVED', True, True, True),
    'S4-rollback-no-tuple-context': ('RESTORING', 'PREIMAGE_SAVED', False, True, True),
    'S4-rollback-tuple-context': ('RESTORING', 'PREIMAGE_SAVED', True, True, True),
    # (118 review) The owner record is written BEFORE the fence, so a fence never
    # exists without it: the run killed right after the fence is made has both.
    'S2-fence-just-made-owner-written-first': ('QUIESCING', 'PREIMAGE_SAVED', True, False, True),
    # Machine path: tuple saved before the fence, kind-only context, owner first.
    'M2-machine-fence-just-made': ('INSTALLING', 'PREIMAGE_SAVED', True, 'machine', True),
    'M3-machine-code-copy': ('INSTALLING', 'PREIMAGE_SAVED', True, 'machine', True),
    'M5-machine-rollback': ('RESTORING', 'PREIMAGE_SAVED', True, 'machine', True),
}
# Every stage leaves one accepted door; nothing is stuck by design any more.
STUCK_BY_DESIGN = frozenset()
MACHINE_CASES = tuple(name for name in CASES if name.startswith('M'))


def _shape(carried, monkeypatch, case):
    control, payload, audit, record = carried
    fence_status, audit_status, tuple_kept, context, owner = CASES[case]
    record['status'] = audit_status
    if not tuple_kept:
        record.pop('writer_fence')
    if context:
        record['recovery_context'] = MACHINE_CONTEXT if context == 'machine' else CONTEXT
    audit.write_text(json.dumps(record), encoding='utf-8')
    payload['status'] = fence_status
    (control / fence.ACTIVE_FILENAME).write_text(json.dumps(payload), encoding='utf-8')
    if not owner:
        next(fence._installer_owner_root().glob('*.json')).unlink()
    monkeypatch.setattr(fence, '_installer_machine_idle', lambda trusted, environ: True, raising=False)
    return control, payload, audit, record


def _entries(tmp_path, control, audit):
    out = {}
    out['resume'] = fence.prove_dead_unfinished_installer(str(audit))
    try:
        _ordinary_writer_refused()
        out['app_start'] = 'refused'
    except BaseException:
        out['app_start'] = 'passed?'
    try:
        report = _remove([])
        out['removal'] = 'settled' if 'abandoned_fence_settled' in report else 'PASS-not-settled'
    except fence.WriterFencedError as error:
        out['removal'] = 'refused:' + str(getattr(error, 'code', ''))
    out['reinstall'] = _start_new_fence(tmp_path, control)
    return out


@pytest.mark.parametrize('case', sorted(CASES))
def test_dead_installer_is_not_stuck_at_any_stage(carried, monkeypatch, tmp_path, case):
    control, payload, audit, record = _shape(carried, monkeypatch, case)
    out = _entries(tmp_path, control, audit)
    with open(tmp_path.parent / 'g99c-table.jsonl', 'a', encoding='utf-8') as handle:
        handle.write(json.dumps({'case': case, **out}, ensure_ascii=False) + chr(10))
    removal_door = out['removal'] == 'settled' and out['reinstall'] == 'STARTED'
    assert case not in STUCK_BY_DESIGN
    if case in MACHINE_CASES:
        # No resume exists for the machine path: removal then reinstall must open.
        assert removal_door and not out['resume'], out
        return
    assert out['resume'] or removal_door, out


def test_other_runs_fence_and_contradicting_tuple_stay_refused(carried, monkeypatch, tmp_path):
    control, payload, audit, record = _shape(carried, monkeypatch, 'S3-code-copy-no-tuple-context')
    # An audit of another run_id is not this fence's audit.
    record['run_id'] = '20261004T000000000Z-' + 'f' * 32
    audit.write_text(json.dumps(record), encoding='utf-8')
    assert fence.prove_dead_unfinished_installer(str(audit)) is False
    # A tuple that names another fence contradicts the owner record.
    record['run_id'] = json.loads(next(fence._installer_owner_root().glob('*.json')).read_text(encoding='utf-8'))['run_id']
    record['writer_fence'] = {'session_id': 'x' * 32, 'attempt_id': 'y' * 32}
    audit.write_text(json.dumps(record), encoding='utf-8')
    assert fence.prove_dead_unfinished_installer(str(audit)) is False
    with pytest.raises(fence.WriterFencedError):
        _remove([])
    assert (control / fence.ACTIVE_FILENAME).is_file()


def test_installer_saves_the_fence_tuple_before_it_creates_the_fence():
    text = INSTALLER.read_text(encoding='utf-8')
    body = text[text.index('$mutated = $false\n$Script:LabelRollbackCandidateRelays'):]
    start_fence = body.index('[void](Start-LabelWriterFence `\n        -ControlRoot $writerFenceControlRoot `\n        -Status \'QUIESCING\'')
    before = body[:start_fence]
    marker = before.rindex('. Save-LabelPreFenceWriterTuple')
    assert marker > before.rindex('Enter-LabelWriterSessionAuthority')
    helper = text[text.index('function Save-LabelPreFenceWriterTuple'):]
    helper = helper[:helper.index('\nfunction ')]
    for name in ('session_id', 'attempt_id', 'replacement_transaction_id', 'session_started_at_utc',
                 'orchestrator_sha256', 'writer_contract_sha256', 'writer_inventory_sha256',
                 'owner_process_id', 'owner_process_started_at_filetime'):
        assert name in helper
    assert helper.index('Save $auditPath $audit') < helper.index('Get-Content') < helper.index('tuple readback differs')


def test_s1_killed_before_the_fence_exists_leaves_nothing_to_settle(carried, monkeypatch, tmp_path):
    control, payload, audit, record = _shape(carried, monkeypatch, 'S3-code-copy-no-tuple-no-context')
    (control / fence.ACTIVE_FILENAME).unlink()
    record.pop('recovery_context', None)
    audit.write_text(json.dumps(record), encoding='utf-8')
    removal = _remove([])
    assert 'abandoned_fence_settled' not in removal
    assert _start_new_fence(tmp_path, control) == 'STARTED'
    with open(tmp_path.parent / 'g99c-table.jsonl', 'a', encoding='utf-8') as handle:
        handle.write(json.dumps({'case': 'S1-before-fence', 'resume': 'n/a', 'app_start': 'passes (no fence)',
                                 'removal': 'PASS-not-settled', 'reinstall': 'STARTED'}) + chr(10))


def test_general_path_writes_the_owner_record_before_it_creates_the_fence():
    text = INSTALLER.read_text(encoding='utf-8')
    body = text[text.index('$mutated = $false\n$Script:LabelRollbackCandidateRelays'):]
    fence = body.index('[void](Start-LabelWriterFence `\n        -ControlRoot $writerFenceControlRoot')
    call = body.index('Invoke-LabelPreFenceOwnerRecord')
    assert body.index('. Save-LabelPreFenceWriterTuple') < call < fence
    assert call > body.index('Enter-LabelWriterSessionAuthority')


def test_machine_path_saves_tuple_context_and_owner_record_before_the_fence():
    text = MACHINE_INSTALLER.read_text(encoding='utf-8')
    start = text.index('$authority = $null; $fenced = $false;$machineFenceReleaseProven=$false')
    body = text[start:start + 9000]
    fence = body.index("[void](Start-LabelWriterFence -ControlRoot $control -Status 'INSTALLING'")
    delegation = body.index('Set-LabelWriterFenceDelegation')
    for name in ('Save-LabelMachinePreFenceRecord', 'Invoke-LabelPreFenceOwnerRecord'):
        assert name in body[:fence], name
    assert body.index('Save-LabelMachinePreFenceRecord') < body.index('Invoke-LabelPreFenceOwnerRecord') < fence < delegation
    helper = text[text.index('function Save-LabelMachinePreFenceRecord'):]
    helper = helper[:helper.index('\nfunction ')]
    assert 'PENDING_FENCE' in helper and 'MACHINE_WORKER_DEFERRED' in helper
    assert helper.index('Save $auditFile $audit') < helper.index('Get-Content') < helper.index('readback differs')


def test_helper_has_an_owner_record_only_mode_that_needs_no_fence():
    helper = HELPER.read_text(encoding='utf-8')
    assert '[switch]$WriteInstallerOwnerRecordOnly' in helper
    function = helper[helper.index('function Write-LabelInstallerOwnerRecord'):]
    function = function[:function.index('\nfunction ')]
    assert '$Tuple' in function
    # With a caller tuple the fence is not read and no admission lease is taken.
    assert function.index('if ($null -ne $Tuple)') < function.index('Read-LabelWriterFence')
    text = MACHINE_INSTALLER.read_text(encoding='utf-8')
    assert text.count('WriteInstallerOwnerRecordOnly') >= 3  # allowed names, mapping, caller
    for name in ('InstallerOwnerWriterInventorySha256', 'InstallerOwnerSessionStartedAtUtc'):
        assert name in text and name in helper

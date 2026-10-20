"""G99 packaging: an installer that died mid-run leaves its fence and an unfinished audit.

A killed canonical installer (power loss, reboot, closed window) keeps only the
fence file; its audit stays unfinished (RUNNING; the installer itself writes
PREIMAGE_SAVED) and no failure record or recovery context exists.  Before the
fix every official entry refused, so the PC was stuck.  The supported user
removal now accepts that exact dead owner and, only for this case, settles the
fence afterwards so the official reinstall can start.
"""
from contextlib import contextmanager
import hashlib
import json
import os
import shutil
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

import current_user_onboarding as onboarding
import writer_session_fence as fence
from tests.test_failed_deployment_removal import (
    CANONICAL, _bind_live_owner, _native_started, _remove, failed_install,
)
from tests.test_shared_ancestor_protection import run_functions
from tests.test_writer_session_fence import _quote, _run_powershell_harness

pytestmark = pytest.mark.skipif(os.name != 'nt', reason='Windows named mutex required')

ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1'
UNFINISHED = ('RUNNING', 'PREIMAGE_SAVED')


@pytest.fixture(autouse=True)
def fence_only_removal(monkeypatch):
    """Isolate fence behaviour from the installed-identity data-generation check.

    That admission needs a real installed release under C:\\KMTech\\Apps and has its own
    tests; here it is replaced so these tests run on any PC, host included.
    """
    import label_data_generation as generation

    @contextmanager
    def no_support_source(app_root):
        yield None

    monkeypatch.setattr(generation, '_authenticated_support_removal_source', no_support_source)
    monkeypatch.setattr(onboarding, 'check_user_data_open', lambda *args, **kwargs: None)


@pytest.fixture
def carried(failed_install):
    """The dead run's fence at the control root the product will actually use.

    Plain runs use the fixture's own root.  A runner that binds the writer root
    (the host test runner does) hides that root, so the fence is carried there
    for the test and removed again afterwards.
    """
    control, payload, audit, record = failed_install
    root = Path(fence.canonical_control_root(os.environ))
    if root == control:
        yield control, payload, audit, record
        return
    root.mkdir(parents=True, exist_ok=True)
    assert not (root / fence.ACTIVE_FILENAME).exists()
    (control / fence.ACTIVE_FILENAME).replace(root / fence.ACTIVE_FILENAME)
    trusted_path = next(fence._installer_owner_root().glob('*.json'))
    trusted = json.loads(trusted_path.read_text(encoding='utf-8'))
    trusted['control_root'] = str(root)
    trusted_path.write_text(json.dumps(trusted), encoding='utf-8')
    try:
        yield root, payload, audit, record
    finally:
        (root / fence.ACTIVE_FILENAME).unlink(missing_ok=True)
        shutil.rmtree(root / 'settled', ignore_errors=True)


@pytest.fixture(params=UNFINISHED)
def stuck(carried, monkeypatch, request):
    """Fence ACTIVE, audit unfinished, no failure record, owner process ended."""
    control, payload, audit, record = carried
    record['status'] = request.param
    audit.write_text(json.dumps(record), encoding='utf-8')
    state = SimpleNamespace(control=control, payload=payload, audit=audit, record=record,
                            idle=True, original=(control / fence.ACTIVE_FILENAME).read_bytes())
    # Native process and relay observation is replaced by this seam; the
    # observation itself has its own tests below.
    monkeypatch.setattr(fence, '_installer_machine_idle', lambda trusted, environ: state.idle, raising=False)
    return state


def _start_new_fence(tmp_path, control):
    """The official reinstall's first fence step, in the PowerShell child shape of the fence tests."""
    completed = _run_powershell_harness(tmp_path, f"""
try {{
    [void](Start-LabelWriterFence -ControlRoot '{_quote(control)}' -Status 'QUIESCING' -SessionId ('a' * 32) `
        -AttemptId ('b' * 32) -ReplacementTransactionId ('c' * 32) `
        -SessionStartedAtUtc ([DateTime]::UtcNow.ToString('o')) `
        -OrchestratorSha256 ('d' * 64) -WriterContractSha256 ('e' * 64) -AuthorityOwnedByCaller)
    'STARTED'
}} catch {{ 'REFUSED:' + $_.Exception.Message }}
""")
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return completed.stdout.strip().splitlines()[-1]


def _resume_gate(tmp_path, audit, record):
    """The supported rollback resume's admission gate (audit pin, identity, status, context)."""
    record = dict(record, source_commit='a' * 40)
    audit.write_text(json.dumps(record), encoding='utf-8')
    result = run_functions(tmp_path, INSTALLER, ['Full', 'Same', 'Sha', 'Invoke-LabelResumeRollback'], r'''
. (Join-Path $env:LM_G99_REPO 'kmtech_shared/powershell/portable.ps1')
$PlanOnly=$false;$testMode=$true;$lad=$env:LM_G99_LOCAL;$install=$env:LM_G99_INSTALL
$sourceManifest=[pscustomobject]@{source_commit=('a'*40)}
$ResumeRollbackAuditPath=$env:LM_G99_AUDIT;$ExpectedRollbackAuditSha256=Sha $env:LM_G99_AUDIT
try{. Invoke-LabelResumeRollback;'ENTERED'}catch{'REFUSED:'+$_.Exception.Message}
''', values={'LM_G99_REPO': str(ROOT), 'LM_G99_LOCAL': str(audit.parents[3]), 'LM_G99_INSTALL': CANONICAL,
            'LM_G99_AUDIT': str(audit)})
    assert result.returncode == 0, result.stderr or result.stdout
    return result.stdout.strip().splitlines()[-1]


def _ordinary_writer_refused():
    with pytest.raises(fence.WriterFencedError):
        with fence.writer_admission('direct_sync_enqueue'):
            pytest.fail('Ordinary writer passed an orphaned deployment fence')


def test_stuck_dead_installer_fence_refuses_every_official_entry_while_a_writer_remains(stuck, tmp_path):
    """Entry 1 (user removal) refuses while a writer/relay survives; entries 2-4 refuse until the fence is gone."""
    stuck.idle = False
    events = []
    with pytest.raises(fence.WriterFencedError):
        _remove(events)
    assert events == []
    assert (stuck.control / fence.ACTIVE_FILENAME).read_bytes() == stuck.original
    # Entry 2: the official reinstall's fence start.
    assert _start_new_fence(tmp_path, stuck.control) == 'REFUSED:LABEL_WRITER_FENCE_ALREADY_ACTIVE'
    # Entry 3: the supported rollback resume needs a recorded failure plus its saved context.
    assert _resume_gate(tmp_path, stuck.audit, stuck.record) == 'REFUSED:Rollback audit identity/status differs.'
    failed = dict(stuck.record, status='ROLLBACK_FAILED')
    assert _resume_gate(tmp_path, stuck.audit, failed) == 'REFUSED:Rollback context belongs to a different user or installer.'
    # Entry 4: the app's start admission (startup_check reports ACTIVE_WRITER_FENCE).
    assert fence.active_fence(stuck.control) is not None
    _ordinary_writer_refused()


def test_dead_unfinished_installer_removal_settles_only_its_fence_and_reinstall_starts(stuck, tmp_path):
    retained = stuck.audit.parent / 'preserved-registration-and-data.bin'
    retained.write_bytes(b'unique retained data and identity')
    events = []
    report = _remove(events)
    assert report['status'] == 'PASS_DATA_PRESERVED'
    assert events == ['run', 'relay', 'task']
    digest = hashlib.sha256(stuck.original).hexdigest()
    settled = report['abandoned_fence_settled']
    assert set(settled) == {'session_id_sha256', 'attempt_id_sha256', 'fence_copy_sha256', 'fence_copy_path'}
    assert settled['fence_copy_sha256'] == digest
    assert settled['fence_copy_path'] == f'{stuck.control.name}/settled/{digest}.json'
    assert (stuck.control.parent / settled['fence_copy_path']).read_bytes() == stuck.original
    for name in ('session_id', 'attempt_id'):
        assert settled[name + '_sha256'] == hashlib.sha256(stuck.payload[name].encode('utf-8')).hexdigest()
    paths = onboarding.resolve_current_user_onboarding_paths(CANONICAL)
    assert json.loads(paths.removal_report_path.read_text(encoding='utf-8'))['abandoned_fence_settled'] == settled
    # Only the fence file is gone; the audit, the protected owner record and the data stay.
    assert not (stuck.control / fence.ACTIVE_FILENAME).exists()
    assert fence.active_fence(stuck.control) is None
    assert stuck.audit.is_file()
    assert next(fence._installer_owner_root().glob('*.json')).is_file()
    assert retained.read_bytes() == b'unique retained data and identity'
    # A second supported removal has no fence left to settle.
    again = _remove([])
    assert again['status'] == 'PASS_DATA_PRESERVED'
    assert 'abandoned_fence_settled' not in again
    # The official reinstall's fence start now passes instead of LABEL_WRITER_FENCE_ALREADY_ACTIVE.
    assert _start_new_fence(tmp_path, stuck.control) == 'STARTED'
    assert (stuck.control / fence.ACTIVE_FILENAME).is_file()


@pytest.mark.parametrize('case', (
    'live-owner', 'live-owner-other-start', 'writer-or-relay-present', 'no-audit', 'no-owner-record',
    'COMPLETE', 'PASS', 'FAILED_PRETRANSACTION', 'fence-restoring', 'duplicate-record',
))
def test_unfinished_audit_door_stays_closed_unless_death_and_idle_are_proven(stuck, case):
    protected = fence._installer_owner_root()
    trusted_path = next(protected.glob('*.json'))
    if case == 'live-owner':
        _bind_live_owner(stuck.audit, stuck.record)
    elif case == 'live-owner-other-start':
        # PID reuse: the same PID with a different birth time proves nothing, so it stays UNKNOWN.
        record = stuck.record
        record['writer_fence']['owner_process_id'] = os.getpid()
        record['writer_fence']['owner_process_started_at_filetime'] = str(int(_native_started(os.getpid())) + 1)
        stuck.audit.write_text(json.dumps(record), encoding='utf-8')
        trusted = json.loads(trusted_path.read_text(encoding='utf-8'))
        trusted.update({key: record['writer_fence'][key] for key in ('owner_process_id', 'owner_process_started_at_filetime')})
        trusted_path.write_text(json.dumps(trusted), encoding='utf-8')
    elif case == 'writer-or-relay-present':
        stuck.idle = False
    elif case == 'no-audit':
        stuck.audit.unlink()
    elif case == 'no-owner-record':
        trusted_path.unlink()
    elif case in ('COMPLETE', 'PASS', 'FAILED_PRETRANSACTION'):
        stuck.record['status'] = case
        stuck.audit.write_text(json.dumps(stuck.record), encoding='utf-8')
    elif case == 'fence-restoring':
        # A rollback was in progress: only the forward-run fence states are settled.
        stuck.payload['status'] = 'RESTORING'
        (stuck.control / fence.ACTIVE_FILENAME).write_text(json.dumps(stuck.payload), encoding='utf-8')
        stuck.original = (stuck.control / fence.ACTIVE_FILENAME).read_bytes()
    else:
        other = json.loads(json.dumps(stuck.record))
        (stuck.audit.parent / 'canonical-portable-duplicate.json').write_text(json.dumps(other), encoding='utf-8')
    events = []
    with pytest.raises(fence.WriterFencedError):
        _remove(events)
    assert events == []
    assert (stuck.control / fence.ACTIVE_FILENAME).read_bytes() == stuck.original
    assert not (stuck.control / 'settled').exists()


def test_live_installer_authority_keeps_an_unfinished_run_fence(stuck):
    ready, stop = threading.Event(), threading.Event()

    def hold():
        lease = fence._acquire_named_mutex(stuck.payload['session_authority_mutex_name'], 0)
        ready.set()
        stop.wait()
        lease.release()

    worker = threading.Thread(target=hold)
    worker.start()
    ready.wait()
    try:
        with pytest.raises(fence.WriterFencedError):
            _remove([])
    finally:
        stop.set()
        worker.join()
    assert (stuck.control / fence.ACTIVE_FILENAME).read_bytes() == stuck.original


def test_settlement_needs_the_admitted_scope_and_the_unchanged_fence(stuck):
    # Outside an admission nothing is settled.
    assert fence.settle_dead_installer_fence() is None
    path = stuck.control / fence.ACTIVE_FILENAME
    with fence.writer_admission('current_user_setup_removal'):
        # A fence rewritten after admission is not the one proven dead.
        path.write_bytes(stuck.original + b'\n')
        with pytest.raises(ValueError):
            fence.settle_dead_installer_fence()
        assert path.read_bytes() == stuck.original + b'\n'
        path.write_bytes(stuck.original)
        settled = fence.settle_dead_installer_fence()
        assert settled['fence_copy_sha256'] == hashlib.sha256(stuck.original).hexdigest()
        assert not path.exists()
        assert fence.settle_dead_installer_fence() is None
    assert fence.settle_dead_installer_fence() is None


def test_failed_status_removal_does_not_settle_the_fence(carried):
    control, payload, audit, record = carried
    original = (control / fence.ACTIVE_FILENAME).read_bytes()
    assert 'abandoned_fence_settled' not in _remove([])
    assert (control / fence.ACTIVE_FILENAME).read_bytes() == original
    assert not (control / 'settled').exists()


@pytest.fixture
def idle_world(tmp_path, monkeypatch):
    import auto_update
    local = tmp_path / 'idle-local'
    local.mkdir()
    monkeypatch.setenv('LOCALAPPDATA', str(local))
    control = tmp_path / 'idle-managed' / 'update-control'
    control.mkdir(parents=True)
    world = SimpleNamespace(rows=[], control=control, calls=[])
    monkeypatch.setattr(auto_update, 'control_root', lambda app_root: world.control)

    def snapshot(selected):
        world.calls.append(selected)
        if isinstance(world.rows, Exception):
            raise world.rows
        return world.rows

    monkeypatch.setattr(auto_update, 'process_snapshot', snapshot)
    world.paths = onboarding.resolve_current_user_onboarding_paths(CANONICAL)
    return world


def _idle(trusted=None):
    return fence._installer_machine_idle(trusted or {'install_root': CANONICAL}, os.environ)


def test_idle_proof_needs_no_writer_process_and_no_relay(idle_world):
    assert _idle() is True
    assert idle_world.calls == [idle_world.control]


def test_idle_proof_refuses_a_surviving_writer_process(idle_world):
    idle_world.rows = [{'pid': os.getpid() + 1, 'observable': True, 'bootstrap': False}]
    assert _idle() is False
    # The bootstrap helper and this process are not writers.
    idle_world.rows = [{'pid': os.getpid(), 'observable': True}, {'pid': 1, 'observable': True, 'bootstrap': True}]
    assert _idle() is True


@pytest.mark.parametrize('failure', (RuntimeError('snapshot failed'), ValueError('unobservable')))
def test_idle_proof_fails_closed_when_processes_cannot_be_observed(idle_world, failure):
    idle_world.rows = failure
    assert _idle() is False
    idle_world.rows = [{'pid': os.getpid() + 1, 'observable': False}]
    assert _idle() is False


def test_idle_proof_refuses_a_live_relay(idle_world):
    import user_relay
    lease = user_relay._acquire_relay_lease(idle_world.paths.direct_sync_root / 'user-relay-instance')
    assert lease is not None
    try:
        assert _idle() is False
    finally:
        lease.close()
    assert _idle() is True


def test_idle_proof_refuses_without_update_control(idle_world, monkeypatch):
    import auto_update
    monkeypatch.setattr(auto_update, 'control_root', lambda app_root: None)
    assert _idle() is False

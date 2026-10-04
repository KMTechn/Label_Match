from pathlib import Path
import json
import os
import subprocess
import sys
import uuid

import pytest
import current_user_onboarding as onboarding
import writer_session_fence as fence
from tests.test_writer_session_fence import _payload

CANONICAL = r'C:\KMTech\Apps\Label_Match\current'


def _native_started(pid):
    import ctypes
    from ctypes import wintypes as w
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.OpenProcess.argtypes = [w.DWORD, w.BOOL, w.DWORD]
    kernel.OpenProcess.restype = w.HANDLE
    kernel.GetProcessTimes.argtypes = [w.HANDLE, *([ctypes.POINTER(w.FILETIME)] * 4)]
    kernel.CloseHandle.argtypes = [w.HANDLE]
    handle = kernel.OpenProcess(0x1000, False, pid)
    assert handle
    try:
        values = [w.FILETIME() for _ in range(4)]
        assert kernel.GetProcessTimes(handle, *[ctypes.byref(value) for value in values])
        return str((values[0].dwHighDateTime << 32) | values[0].dwLowDateTime)
    finally:
        kernel.CloseHandle(handle)


def _bind_live_owner(audit, record):
    record['writer_fence']['owner_process_id'] = os.getpid()
    record['writer_fence']['owner_process_started_at_filetime'] = _native_started(os.getpid())
    audit.write_text(json.dumps(record), encoding='utf-8')
    path = next(fence._installer_owner_root().glob('*.json'))
    trusted = json.loads(path.read_text(encoding='utf-8'))
    trusted.update({key: record['writer_fence'][key] for key in ('owner_process_id', 'owner_process_started_at_filetime')})
    path.write_text(json.dumps(trusted), encoding='utf-8')


@pytest.fixture
def failed_install(tmp_path, monkeypatch):
    local = tmp_path / 'local'
    control = local / fence.CONTROL_RELATIVE_PATH
    control.mkdir(parents=True)
    audit_root = local / 'KMTech/Label_Match/install-audit'
    audit_root.mkdir(parents=True)
    child = subprocess.Popen([sys.executable, '-B', '-c', 'pass'], creationflags=subprocess.CREATE_NO_WINDOW)
    import ctypes
    from ctypes import wintypes as w
    creation, exit_time, kernel_time, user_time = (w.FILETIME() for _ in range(4))
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.GetProcessTimes.argtypes = [w.HANDLE, *([ctypes.POINTER(w.FILETIME)] * 4)]
    assert kernel.GetProcessTimes(int(child._handle), ctypes.byref(creation), ctypes.byref(exit_time), ctypes.byref(kernel_time), ctypes.byref(user_time))
    created = str((creation.dwHighDateTime << 32) | creation.dwLowDateTime)
    child.wait()
    payload = _payload(delegated_sources=[])
    (control / fence.ACTIVE_FILENAME).write_text(json.dumps(payload), encoding='utf-8')
    record = dict(schema='label-match-canonical-portable-install-v1', status='ROLLBACK_FAILED',
        install_root=CANONICAL, writer_fence=dict(
            session_id=payload['session_id'], attempt_id=payload['attempt_id'],
            replacement_transaction_id=payload['replacement_transaction_id'],
            writer_inventory_sha256=payload['writer_inventory_sha256'],
            orchestrator_sha256=payload['orchestrator_sha256'],
            writer_contract_sha256=payload['writer_contract_sha256'],
            session_started_at_utc=payload['session_started_at_utc'],
            owner_process_id=child.pid))
    audit = audit_root / ('canonical-portable-' + uuid.uuid4().hex + '.json')
    audit.write_text(json.dumps(record), encoding='utf-8')
    monkeypatch.setenv('LOCALAPPDATA', str(local))
    monkeypatch.setenv(fence.TEST_MODE_ENV, '1')
    monkeypatch.setenv(fence.CONTROL_ROOT_OVERRIDE_ENV, str(control))
    # The trust-admission seam is separate from native NTFS/PID/mutex behavior.
    # Host elevation is forbidden; production's native ACL rejection has its own test.
    protected = Path(os.environ['LM_NTFS_TEST_ROOT']) / ('owner-' + uuid.uuid4().hex[:8])
    protected.mkdir()
    record['run_id'] = '20261004T000000000Z-' + uuid.uuid4().hex
    record['writer_fence']['owner_process_started_at_filetime'] = created
    audit.rename(audit_root / ('canonical-portable-' + record['run_id'] + '.json'))
    audit = audit_root / ('canonical-portable-' + record['run_id'] + '.json')
    audit.write_text(json.dumps(record), encoding='utf-8')
    trusted = dict(schema='label-match-installer-owner-v1', run_id=record['run_id'],
        install_root=CANONICAL, control_root=str(control), **record['writer_fence'])
    (protected / ('.installer-owner-' + payload['session_id'] + '-' + record['run_id'].rsplit('-', 1)[-1] + '.json')).write_text(json.dumps(trusted), encoding='utf-8')
    monkeypatch.setattr(fence, '_installer_owner_root', lambda: protected, raising=False)
    monkeypatch.setattr(fence, '_assert_protected_owner_path', lambda path: None, raising=False)
    yield control, payload, audit, record


def _remove(events, cut=''):
    def action(name):
        def invoke(*args):
            events.append(name)
            if name == 'task':
                assert str(args[0]).casefold() == str(Path(CANONICAL)).casefold()
            if name == cut:
                raise RuntimeError('injected ' + name + ' removal failure')
            return {'status': 'ABSENT'}
        return invoke
    return onboarding.remove_current_user_setup(CANONICAL, autostart_remover=action('run'),
        relay_stopper=action('relay'), scheduled_task_remover=action('task'))


def test_dead_failed_installer_allows_only_supported_user_removal(failed_install):
    control, payload, audit, record = failed_install
    original = (control / fence.ACTIVE_FILENAME).read_bytes()
    retained = audit.parent / 'preserved-registration-and-data.bin'
    retained.write_bytes(b'unique retained data and identity')
    events = []
    assert _remove(events)['status'] == 'PASS_DATA_PRESERVED'
    assert events == ['run', 'relay', 'task']
    assert _remove([])['status'] == 'PASS_DATA_PRESERVED'
    assert (control / fence.ACTIVE_FILENAME).read_bytes() == original
    assert retained.read_bytes() == b'unique retained data and identity'
    with pytest.raises(fence.WriterFencedError):
        with fence.writer_admission('direct_sync_enqueue'):
            pytest.fail('Ordinary writer passed an orphaned deployment fence')


@pytest.mark.parametrize('case', ('live-owner', 'live-authority', 'different-run', 'no-audit'))
def test_failed_install_cleanup_keeps_live_and_unbound_fences(failed_install, case):
    control, payload, audit, record = failed_install
    authority = None
    if case == 'live-owner':
        _bind_live_owner(audit, record)
    elif case == 'live-authority':
        # Another thread holds the native authority mutex, not this caller.
        import threading
        ready, stop = threading.Event(), threading.Event()
        def hold():
            lease = fence._acquire_named_mutex(payload['session_authority_mutex_name'], 0)
            ready.set()
            stop.wait()
            lease.release()
        worker = threading.Thread(target=hold)
        worker.start()
        ready.wait()
        authority = (stop, worker)
    elif case == 'different-run':
        record['writer_fence']['attempt_id'] = 'f' * 32
        audit.write_text(json.dumps(record), encoding='utf-8')
    else:
        audit.unlink()
    original = (control / fence.ACTIVE_FILENAME).read_bytes()
    events = []
    try:
        with pytest.raises(fence.WriterFencedError):
            _remove(events)
    finally:
        if authority:
            authority[0].set()
            authority[1].join()
    assert events == []
    assert (control / fence.ACTIVE_FILENAME).read_bytes() == original


@pytest.mark.parametrize('cut', ('run', 'relay', 'task'))
def test_supported_removal_attempts_later_steps_after_independent_failure(tmp_path, monkeypatch, cut):
    monkeypatch.setenv('LOCALAPPDATA', str(tmp_path))
    monkeypatch.setenv(fence.TEST_MODE_ENV, '1')
    monkeypatch.setenv(fence.CONTROL_ROOT_OVERRIDE_ENV, str(tmp_path / 'control'))
    events = []
    with pytest.raises(onboarding.CurrentUserOnboardingError):
        _remove(events, cut)
    assert events == (['run', 'relay'] if cut == 'relay' else ['run', 'relay', 'task'])
    paths = onboarding.resolve_current_user_onboarding_paths(CANONICAL)
    report = json.loads(paths.removal_report_path.read_text(encoding='utf-8'))
    assert report['status'] == 'FAILED'
    assert report['data_preserved'] is True
    assert report['removal_failures'][0]['step'] == cut
    if cut == 'relay':
        assert report['scheduled_task']['status'] == 'NOT_TESTED_RELAY_UNPROVEN'


@pytest.mark.parametrize("status,live,allowed", (
    ("FAILED_SUPPORTED_REMOVAL_REQUIRED", False, True),
    ("FAILED_SUPPORTED_REMOVAL_REQUIRED", True, False),
    ("RUNNING", False, False),
    ("COMPLETE", False, False),
    ("FAILED_PRETRANSACTION", False, False),
))
def test_supported_removal_required_accepts_only_exact_dead_failure(failed_install, status, live, allowed):
    control, payload, audit, record = failed_install
    record['status'] = status
    if live:
        _bind_live_owner(audit, record)
    audit.write_text(json.dumps(record), encoding='utf-8')
    original = (control / fence.ACTIVE_FILENAME).read_bytes()
    events = []
    if allowed:
        assert _remove(events)['status'] == 'PASS_DATA_PRESERVED'
        assert events == ['run', 'relay', 'task']
    else:
        with pytest.raises(fence.WriterFencedError):
            _remove(events)
        assert events == []
    assert (control / fence.ACTIVE_FILENAME).read_bytes() == original


@pytest.mark.parametrize('case', ('missing-protected', 'forged-pid', 'forged-run', 'forged-start',
    'duplicate-dead', 'duplicate-live', 'duplicate-unknown', 'duplicate-status',
    'protected-duplicate', 'protected-conflict', 'protected-misnamed'))
def test_failed_removal_rejects_forged_conflicting_and_duplicate_records(failed_install, case):
    control, payload, audit, record = failed_install
    protected = fence._installer_owner_root()
    trusted_path = next(protected.glob('*.json'))
    if case == 'missing-protected':
        trusted_path.unlink()
    elif case == 'forged-pid':
        record['writer_fence']['owner_process_id'] = 0xFFFFFFFE
        audit.write_text(json.dumps(record), encoding='utf-8')
    elif case == 'forged-run':
        record['run_id'] = '20261004T000000000Z-' + 'f' * 32
        audit.write_text(json.dumps(record), encoding='utf-8')
    elif case == 'forged-start':
        record['writer_fence']['owner_process_started_at_filetime'] = '2'
        audit.write_text(json.dumps(record), encoding='utf-8')
    elif case.startswith('duplicate-'):
        other = json.loads(json.dumps(record))
        if case == 'duplicate-live':
            other['writer_fence']['owner_process_id'] = os.getpid()
        elif case == 'duplicate-unknown':
            other['writer_fence']['owner_process_id'] = 4  # protected System PID: unknown/access denied
        elif case == 'duplicate-status':
            other['status'] = 'RUNNING'
        (audit.parent / 'canonical-portable-duplicate.json').write_text(json.dumps(other), encoding='utf-8')
    else:
        other = json.loads(trusted_path.read_text(encoding='utf-8'))
        if case == 'protected-conflict':
            other['owner_process_id'] = os.getpid()
        (protected / (('.installer-owner-misnamed.json' if case == 'protected-misnamed' else trusted_path.stem + '-duplicate.json'))).write_text(json.dumps(other), encoding='utf-8')
    original = (control / fence.ACTIVE_FILENAME).read_bytes()
    events = []
    with pytest.raises(fence.WriterFencedError):
        _remove(events)
    assert events == []
    assert (control / fence.ACTIVE_FILENAME).read_bytes() == original


def test_protected_installer_owner_guard_rejects_user_mutable_native_file(tmp_path):
    path = tmp_path / 'owner.json'
    path.write_text('{}', encoding='utf-8')
    with pytest.raises(OSError):
        fence._assert_protected_owner_path(path)


def test_dead_process_creation_time_mismatch_remains_unknown(failed_install):
    _, _, _, record = failed_install
    owner = record['writer_fence']
    assert fence._installer_process_exited(owner['owner_process_id'], owner['owner_process_started_at_filetime'])
    assert not fence._installer_process_exited(owner['owner_process_id'], str(int(owner['owner_process_started_at_filetime']) + 1))


def test_bound_live_and_unobservable_processes_fail_closed():
    assert not fence._installer_process_exited(os.getpid(), _native_started(os.getpid()))
    assert not fence._installer_process_exited(4, '1')
    assert not fence._installer_process_exited(True, '1')

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


@pytest.fixture
def failed_install(tmp_path, monkeypatch):
    local = tmp_path / 'local'
    control = local / fence.CONTROL_RELATIVE_PATH
    control.mkdir(parents=True)
    audit_root = local / 'KMTech/Label_Match/install-audit'
    audit_root.mkdir(parents=True)
    child = subprocess.Popen([sys.executable, '-B', '-c', 'pass'], creationflags=subprocess.CREATE_NO_WINDOW)
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
    return control, payload, audit, record


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
        record['writer_fence']['owner_process_id'] = os.getpid()
        audit.write_text(json.dumps(record), encoding='utf-8')
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
        record['writer_fence']['owner_process_id'] = os.getpid()
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

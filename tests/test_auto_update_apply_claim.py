"""E1: wait for a started apply; only proved owner absence permits recovery."""
import contextlib
import ctypes
from ctypes import wintypes
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

import auto_update as updates
from tests.test_auto_update_endpoints import endpoint, assert_ordinary, hotfix_packet, bootstrap_variant, OLD, NEW
from kmtech_shared import data_generation as generation


from tests._update_clock_fixture import isolated_update_time as _isolated_update_time

def own_claim():
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.GetCurrentProcess.restype = wintypes.HANDLE
    kernel.GetProcessTimes.argtypes = [wintypes.HANDLE] + [ctypes.POINTER(wintypes.FILETIME)] * 4
    times = [wintypes.FILETIME() for _ in range(4)]
    assert kernel.GetProcessTimes(kernel.GetCurrentProcess(), *[ctypes.byref(value) for value in times])
    identity = (times[0].dwHighDateTime << 32) | times[0].dwLowDateTime
    return dict(schema='code-update-apply-claim-v1', status='RUNNING',
                pid=os.getpid(), creation_identity=str(identity), started_at=updates.now_utc())


def prepared_start(endpoint):
    control, stage, _, _, _, _ = endpoint
    (control/'transition.json').unlink()
    updates.write_json_durable(control/'active.json', {'sha': OLD})
    updates.write_json_durable(stage/'prepared.json', {'source_commit': NEW})
    return control


def publish_completed_task(endpoint):
    # The OS task adapter returns a valid completed protected state using the
    # real transition functions; updater/READY/ES256 mechanics have separate
    # endpoint coverage. This test measures supervision, not task scheduling.
    control, _, _, state, _, _ = endpoint
    applied = updates.apply_transition({**state, 'phase': 'PREPARED'},
        writers_absent=True, packet_verified=True, durable_prepared=True, now=updates.now_utc())
    trial = updates.bind_trial(applied, sha=NEW, pid=os.getpid(), ordinary_user=True, now=updates.now_utc())
    receipt = {**{name: True for name in updates.READY_CHECKS}, 'nonce': trial['nonce'],
               'transition_id': trial['transition_id'], 'sha': NEW, 'pid': os.getpid(),
               'ready_at': updates.now_utc()}
    confirmed = updates.confirm_transition(trial, receipt, process_matches=True,
        relay_matches=True, now=updates.now_utc())
    updates.write_json_durable(control/'transition.json', confirmed)
    updates.write_json_durable(control/'active.json', {'sha': NEW})
    updates.write_json_durable(control/'apply-claim.json', {**own_claim(), 'status': 'SETTLED'})


@pytest.mark.parametrize('busy', [False, True])
def test_e1_live_claim_waits_through_90_second_apply_then_starts_new(endpoint, monkeypatch, busy):
    _clock_time = _isolated_update_time(monkeypatch)
    control = prepared_start(endpoint)
    observation = endpoint[5]
    clock = [0.0]
    monkeypatch.setattr(_clock_time, 'monotonic', lambda: clock[0])
    updates.write_json_durable(control/'apply-claim.json', own_claim())
    monkeypatch.setattr(updates, 'request_apply_task', lambda *_: None)
    def progress(_):
        clock[0] += 30.0
        assert not observation.exists(), 'current code started while the apply owner was live'
        if clock[0] == 90.0:
            publish_completed_task(endpoint)
    monkeypatch.setattr(_clock_time, 'sleep', progress)
    if busy:
        @contextlib.contextmanager
        def owner_lock(*_args, **_kwargs):
            if clock[0] < 90.0:
                raise RuntimeError('update mutex ownership is busy')
            yield
        monkeypatch.setattr(updates, 'update_lock', owner_lock)
    assert_ordinary(endpoint, NEW)
    assert clock[0] == 90.0
    assert updates.read_json(control/'transition.json')['phase'] == 'CONFIRMED'


@pytest.mark.parametrize('owner', ['exited', 'reused_pid'])
def test_e1_absent_or_reused_apply_owner_starts_current(endpoint, monkeypatch, owner):
    control = prepared_start(endpoint)
    claim = own_claim()
    if owner == 'exited':
        native = subprocess.Popen.native
        process = native([sys.executable, '-B', '-c', 'pass'],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, creationflags=subprocess.CREATE_NO_WINDOW)
        process.communicate(timeout=30)
        assert process.returncode == 0
        claim['pid'] = process.pid
    else:
        claim['creation_identity'] = '0'  # Same live PID, an earlier creation identity.
    updates.write_json_durable(control/'apply-claim.json', claim)
    requests, reports = [], []
    written = updates.write_json_durable
    def record_write(path, value):
        if path == endpoint[1]/'startup-support.json':
            reports.append(value)
        written(path, value)
    monkeypatch.setattr(updates, 'write_json_durable', record_write)
    monkeypatch.setattr(updates, 'request_apply_task', lambda *_: requests.append(True))
    assert_ordinary(endpoint, OLD)
    first_pid = json.loads(endpoint[5].read_text())['pid']
    endpoint[5].unlink()
    assert_ordinary(endpoint, OLD)
    assert json.loads(endpoint[5].read_text())['pid'] != first_pid
    assert not requests, 'same-session recovery must not repeatedly restart the dead SYSTEM task'
    assert len(reports) == 1, 'same dead claim must publish the support code once'
    assert reports[0]['reason_code'] == 'UPDATE_SUPERVISION_UNAVAILABLE'
    assert updates.apply_claim_status(control) == 'ABSENT'
    assert updates.read_json(control/'active.json')['sha'] == OLD


def test_e1_unknown_owner_waits_existing_window_and_reports_progress(endpoint, monkeypatch, capsys, bootstrap_variant):
    _clock_time = _isolated_update_time(monkeypatch)
    control = prepared_start(endpoint)
    clock = [0.0]
    updates.write_json_durable(control/'apply-claim.json', own_claim())
    monkeypatch.setattr(updates, 'native_process_identity', lambda *_: {'status': 'UNKNOWN'}, raising=False)
    monkeypatch.setattr(updates, 'request_apply_task', lambda *_: None)
    monkeypatch.setattr(_clock_time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(_clock_time, 'sleep', lambda _: clock.__setitem__(0, clock[0]+30.0))
    assert updates.bootstrap(control.parent/'current/app', [], role='gui') == 20
    assert clock[0] == 240.0 and not endpoint[5].exists()
    report = json.loads(capsys.readouterr().out)
    assert report['reason_code'] == 'UPDATE_IN_PROGRESS'
    assert report['guidance'] == updates.STARTUP_PROGRESS_MESSAGE


def test_e1_normal_apply_exit_durably_retires_only_its_claim(endpoint, monkeypatch):
    control = endpoint[0]
    updates.write_json_durable(control/'apply-claim.json', own_claim())
    monkeypatch.setattr(updates, '_apply_prepared', lambda _: 0)
    assert updates.apply_prepared(control) == 0
    assert updates.read_json(control/'apply-claim.json')['status'] == 'SETTLED'
    assert updates.apply_claim_status(control) == 'NONE'


def test_e1_next_logon_system_replaces_absent_claim_and_applies(endpoint, monkeypatch):
    control = endpoint[0]
    from tests._rollback_ready_fixture import install_previous_relay_observer
    install_previous_relay_observer(monkeypatch, updates, control, endpoint[2])
    updates.write_json_durable(control/'apply-claim.json', {**own_claim(), 'creation_identity': '0'})
    assert updates.apply_claim_status(control) == 'ABSENT'
    written, started = updates.write_json_durable, []
    def observe(path, value):
        written(path, value)
        if path == control/'apply-claim.json' and value['status'] == 'RUNNING':
            started.append(value)
            assert updates.apply_claim_status(control) == 'LIVE'
    monkeypatch.setattr(updates, 'write_json_durable', observe)
    # Existing logon-trigger task invokes the real SYSTEM apply entrypoint.
    assert updates.apply_prepared(control) == 0
    assert len(started) == 1 and started[0]['creation_identity'] == own_claim()['creation_identity']
    assert updates.read_json(control/'transition.json')['phase'] == 'ROLLED_BACK'
    assert updates.read_json(control/'transition.json')['failed_shas'] == []
    assert updates.read_json(control/'apply-claim.json')['status'] == 'SETTLED'
    assert_ordinary(endpoint, OLD)


@pytest.mark.parametrize('phase', ['FAILED', 'UNKNOWN'])
@pytest.mark.parametrize('guard', ['allow', 'writer', 'generation_unknown', 'generation_denied', 'prepared'])
def test_b1_settled_trial_recovers_in_same_bootstrap(endpoint, monkeypatch, phase, guard, bootstrap_variant):
    _clock_time = _isolated_update_time(monkeypatch)
    control, stage, policy, state, marker, observation = endpoint
    clock = [0.0]
    monkeypatch.setattr(_clock_time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(_clock_time, 'sleep', lambda _: clock.__setitem__(0, clock[0] + 30.0))
    protected = {}
    requests = []

    def settle(*_):
        requests.append(True)
        updates.write_json_durable(control / 'transition.json', {**state, 'phase': phase})
        updates.write_json_durable(control / 'apply-claim.json', {**own_claim(), 'status': 'SETTLED'})
        if guard == 'writer':
            monkeypatch.setattr(updates, 'process_snapshot', lambda *_: [{'pid': 456, 'observable': True}])
        elif guard == 'generation_unknown':
            marker.write_bytes(b'{unreadable-generation')
        elif guard == 'generation_denied':
            generation.record_generation(marker, 'v2.0.100', app_id='Label_Match',
                subject_sid=policy['operator_sid'], build_id=NEW)
        elif guard == 'prepared':
            updates.write_json_durable(stage / 'prepared.json', {'source_commit': OLD})
        protected.update({path: path.read_bytes() for path in control.glob('*.json')})

    monkeypatch.setattr(updates, 'request_apply_task', settle)
    if guard == 'allow' and bootstrap_variant == 'a63caec':
        assert_ordinary(endpoint, NEW)
        assert clock[0] == 0.0
        assert updates.read_json(stage / 'startup-support.json')['reason_code'] == 'UPDATE_TERMINAL_RECOVERED'
    else:
        assert updates.bootstrap(control.parent / 'current/app', [], role='gui') == (0 if guard == 'writer' and bootstrap_variant == 'a63caec' else 20)
        assert not observation.exists()
    assert requests == [True]
    assert updates.apply_claim_status(control) == 'NONE'
    assert all(path.read_bytes() == value for path, value in protected.items())
    assert not (stage / 'trial.json').exists()


@pytest.mark.parametrize('guard', [
    'allow', 'fresh', 'boundary', 'future', 'live', 'stat_error', 'writer',
    'generation_unknown', 'generation_denied', 'prepared', 'claim_renewed', 'lock_busy',
])
def test_b3_stale_unknown_claim_recovers_only_after_window_and_recheck(endpoint, monkeypatch, guard, bootstrap_variant):
    _clock_time = _isolated_update_time(monkeypatch)
    control, stage, policy, state, marker, observation = endpoint
    updates.write_json_durable(control / 'transition.json', {**state, 'phase': 'FAILED'})
    claim_path = control / 'apply-claim.json'
    updates.write_json_durable(claim_path, own_claim() if guard == 'live' else {'schema': 'unsupported'})
    clock = [0.0]
    wall = int(_clock_time.time())
    monkeypatch.setattr(_clock_time, 'time', lambda: wall)
    age = {'fresh': 60, 'boundary': 900, 'future': -60}.get(guard, 960)
    os.utime(claim_path, (wall - age, wall - age))
    monkeypatch.setattr(_clock_time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(_clock_time, 'sleep', lambda _: clock.__setitem__(0, clock[0] + 30.0))
    requests = []
    monkeypatch.setattr(updates, 'request_apply_task', lambda *_: requests.append(True))
    if guard == 'writer':
        monkeypatch.setattr(updates, 'process_snapshot', lambda *_: [{'pid': 456, 'observable': True}])
    elif guard == 'generation_unknown':
        marker.write_bytes(b'{unreadable-generation')
    elif guard == 'generation_denied':
        generation.record_generation(marker, 'v2.0.100', app_id='Label_Match',
            subject_sid=policy['operator_sid'], build_id=NEW)
    elif guard == 'prepared':
        updates.write_json_durable(stage / 'prepared.json', {'source_commit': OLD})
    elif guard == 'stat_error':
        native_stat = Path.stat
        def unreadable(path, *args, **kwargs):
            if path == claim_path:
                raise OSError('synthetic claim metadata unavailable')
            return native_stat(path, *args, **kwargs)
        monkeypatch.setattr(Path, 'stat', unreadable)
    locked = []
    @contextlib.contextmanager
    def lock(*_args, **_kwargs):
        if clock[0] >= 240.0:
            if guard == 'claim_renewed':
                updates.write_json_durable(claim_path, own_claim())
            if guard == 'lock_busy':
                raise RuntimeError('update mutex ownership is busy')
            locked.append(clock[0])
        yield
    monkeypatch.setattr(updates, 'update_lock', lock)
    protected = {path: path.read_bytes() for path in control.glob('*.json')}
    if guard == 'allow' and bootstrap_variant == 'a63caec':
        assert_ordinary(endpoint, NEW)
        assert locked and locked[0] == 240.0
        report = updates.read_json(stage / 'startup-support.json')
        assert report['reason_code'] == 'UPDATE_SUPERVISION_UNAVAILABLE'
        assert report['apply_claim_sha256'] == updates.hashlib.sha256(claim_path.read_bytes()).hexdigest()
    else:
        assert updates.bootstrap(control.parent / 'current/app', [], role='gui') == (0 if guard == 'writer' and bootstrap_variant == 'a63caec' else 20)
        assert not observation.exists()
    assert clock[0] == 240.0
    assert not requests
    assert all(path.read_bytes() == value for path, value in protected.items() if not (guard == 'claim_renewed' and path == claim_path))
    assert not (stage / 'trial.json').exists()


@pytest.mark.parametrize('crash_phase', ['PREPARED', 'APPLIED_PENDING_READY'])
def test_system_restart_supervises_only_a_window_it_opens(endpoint, monkeypatch, crash_phase):
    control, _, _, state, _, _ = endpoint
    from tests._rollback_ready_fixture import install_previous_relay_observer
    install_previous_relay_observer(monkeypatch, updates, control, endpoint[2])
    updates.write_json_durable(control/'transition.json', {**state, 'phase': 'FAILED'})
    updates.write_json_durable(control/'active.json', {'sha': OLD})
    hot = hotfix_packet(endpoint, monkeypatch)
    assert updates.apply_prepared(control) == 0
    assert updates.read_json(control/'transition.json')['phase'] == 'ROLLED_BACK'
    assert updates.read_json(control/'active.json')['sha'] == OLD
    written = updates.write_json_durable
    def crash(path, value):
        written(path, value)
        if crash_phase == 'PREPARED':
            if path == control/'transition.json' and value.get('phase') == 'PREPARED':
                raise SystemExit(0)
        elif path == control/'active.json' and value.get('sha') == hot:
            raise SystemExit(0)
    monkeypatch.setattr(updates, 'write_json_durable', crash)
    with pytest.raises(SystemExit):
        updates.apply_prepared(control)
    assert updates.read_json(control/'transition.json')['phase'] == crash_phase
    observations = []
    def observe(path, value):
        written(path, value)
        if path == control/'transition.json':
            observations.append(value)
    monkeypatch.setattr(updates, 'write_json_durable', observe)
    # Same interpreter: ownership must be local to each SYSTEM call.
    resumed_at = updates.now_utc()
    assert updates.apply_prepared(control) == 0
    finished_at = updates.now_utc()
    settled = updates.read_json(control/'transition.json')
    assert settled['phase'] == 'ROLLED_BACK'
    # ANSWER-15: READY timeout consumes a chance without blacklisting the SHA.
    assert settled['failed_shas'] == []
    first_at = settled['retry_shas'][hot]['first_at']
    assert settled['retry_shas'] == {
        hot: {'attempts': 1, 'first_at': first_at, 'last_at': first_at},
    }
    assert updates._utc(resumed_at) <= updates._utc(first_at) <= updates._utc(finished_at)
    if crash_phase == 'PREPARED':
        assert settled['reason'] == 'candidate startup failed or readiness timed out'
    assert any(value['phase'] == 'APPLIED_PENDING_READY' for value in observations) is (crash_phase == 'PREPARED')
    assert any(value['phase'] == 'UNKNOWN' for value in observations) is (crash_phase == 'APPLIED_PENDING_READY')
    assert updates.read_json(control/'active.json')['sha'] == OLD

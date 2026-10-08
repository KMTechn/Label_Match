"""Relay launch retries preserve the existing admission and deadline."""
import contextlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

import auto_update as updates
import writer_session_fence as fence
from tests.test_auto_update_endpoints import endpoint, NEW

NATIVE_SLEEP = time.sleep
NATIVE_MONOTONIC = time.monotonic
RELAY = "--label-match-user-relay"


@contextlib.contextmanager
def held_admission(endpoint, monkeypatch, tmp_path, *, release_after=None):
    ready, release = tmp_path / 'holder-ready', tmp_path / 'holder-release'
    mutex = fence.WRITER_MUTEX_NAME + '.relay-retry.' + updates.hashlib.sha256(str(tmp_path).encode()).hexdigest()[:16]
    monkeypatch.setattr(fence, 'writer_admission_mutex_name', lambda *a, **k: mutex)
    source = '''import sys, time
from pathlib import Path
import writer_session_fence as f
lease = f._acquire_named_mutex(sys.argv[1], 1.0)
assert lease is not None
try:
    Path(sys.argv[2]).write_text('held')
    deadline = time.monotonic() + float(sys.argv[4])
    while not Path(sys.argv[3]).exists() and time.monotonic() < deadline:
        time.sleep(0.01)
finally:
    lease.release()
'''
    holder = subprocess.Popen.native([sys.executable, '-B', '-c', source, mutex,
        str(ready), str(release), str(release_after or 60.0)],
        cwd=Path(updates.__file__).parent, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        creationflags=subprocess.CREATE_NO_WINDOW)
    try:
        deadline = NATIVE_MONOTONIC() + 10
        while not ready.exists() and holder.poll() is None and NATIVE_MONOTONIC() < deadline:
            NATIVE_SLEEP(0.01)
        assert ready.exists(), 'native holder did not acquire admission'
        yield holder
    finally:
        release.write_text('release')
        _, error = holder.communicate(timeout=15)
        assert holder.returncode == 0, error[-1500:]


def ordinary_relay(endpoint):
    control, _, _, state, _, _ = endpoint
    updates.write_json_durable(control / 'transition.json', {**state, 'phase': 'CONFIRMED'})
    return control


def test_b6_bootstrap_relay_retries_native_writer_timeout(endpoint, monkeypatch, tmp_path):
    control = ordinary_relay(endpoint)
    protected = {p: p.read_bytes() for p in control.glob('*.json')}
    release_child = tmp_path / 'relay-release'
    main = updates.version_bundle(control, NEW) / 'app/main.py'
    main.write_text(main.read_text() + '\nimport time\nwhile not Path(' + repr(str(release_child)) + ').exists(): time.sleep(0.01)\n')
    native_popen = subprocess.Popen.native
    children = []
    def resident_launch(command, **kwargs):
        command = [sys.executable, '-B', *command[3:]]
        child = native_popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)
        children.append(child)
        return child
    monkeypatch.setattr(subprocess, 'Popen', resident_launch)
    resident_launch.native = native_popen
    monkeypatch.setattr(time, 'monotonic', NATIVE_MONOTONIC)
    pauses = []
    def pause(seconds):
        assert getattr(fence._WRITER_LOCAL, 'depth', 0) == 0
        pauses.append(seconds)
        NATIVE_SLEEP(seconds)
    monkeypatch.setattr(time, 'sleep', pause)
    try:
        with held_admission(endpoint, monkeypatch, tmp_path, release_after=8.0):
            started = NATIVE_MONOTONIC()
            assert updates.bootstrap(control.parent / 'current/app', [RELAY], role='relay') == 0
            deadline = NATIVE_MONOTONIC() + 15
            while not endpoint[-1].exists() and NATIVE_MONOTONIC() < deadline:
                NATIVE_SLEEP(0.01)
            result = json.loads(endpoint[-1].read_text())
            assert result['sha'] == NEW and result['gate'] is None
            assert result['status'] == 'ALLOW' and result['ordinary'] is True
            assert len(children) == 1 and children[0].poll() is None
            assert result['pid'] == children[0].pid
            assert 7 <= NATIVE_MONOTONIC() - started < 30
    finally:
        release_child.write_text('release')
        for child in children:
            _, error = child.communicate(timeout=30)
            assert child.returncode == 0, error[-1500:]
    assert pauses, 'no retry occurred after the native 5-second timeout'
    assert json.loads(endpoint[-1].read_text())['relay'] is True
    assert all(p.read_bytes() == value for p, value in protected.items())


def test_b6_deadline_preserves_native_timeout_failure(endpoint, monkeypatch, tmp_path):
    control = ordinary_relay(endpoint)
    protected = {p: p.read_bytes() for p in control.glob('*.json')}
    clock = [0.0]
    monkeypatch.setattr(time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(time, 'sleep', lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    native_acquire = fence._acquire_named_mutex
    attempts = []
    def acquire(name, timeout):
        attempts.append(timeout)
        result = native_acquire(name, timeout)
        if result is None:
            clock[0] = 240.0
        return result
    monkeypatch.setattr(fence, '_acquire_named_mutex', acquire)
    with held_admission(endpoint, monkeypatch, tmp_path):
        with pytest.raises(fence.WriterFencedError) as caught:
            updates.bootstrap(control.parent / 'current/app', [RELAY], role='relay')
    assert caught.value.code == 'WRITER_GATE_TIMEOUT'
    assert attempts == [5.0]
    assert not endpoint[-1].exists()
    assert all(p.read_bytes() == value for p, value in protected.items())


@pytest.mark.parametrize('code', ['ACTIVE_WRITER_FENCE', 'WRITER_GATE_ABANDONED', 'WRITER_SOURCE_MISSING'])
def test_b6_persistent_denial_is_immediate(endpoint, monkeypatch, code):
    control = ordinary_relay(endpoint)
    calls = []
    def denied(*args, **kwargs):
        calls.append(True)
        raise fence.WriterFencedError(code, 'synthetic persistent writer denial')
    monkeypatch.setattr(updates, 'launch_version', denied)
    with pytest.raises(fence.WriterFencedError) as caught:
        updates.bootstrap(control.parent / 'current/app', [RELAY], role='relay')
    assert caught.value.code == code
    assert calls == [True]
    assert not endpoint[-1].exists()


def test_b6_gui_launch_timeout_is_not_retried(endpoint, monkeypatch):
    control = ordinary_relay(endpoint)
    calls = []
    def denied(*args, **kwargs):
        calls.append(True)
        raise fence.WriterFencedError('WRITER_GATE_TIMEOUT', 'synthetic timeout')
    monkeypatch.setattr(updates, 'launch_version', denied)
    with pytest.raises(fence.WriterFencedError):
        updates.bootstrap(control.parent / 'current/app', [], role='gui')
    assert calls == [True]


def test_b6_retry_does_not_start_after_deadline(endpoint, monkeypatch):
    control = ordinary_relay(endpoint)
    clock = [0.0]
    calls = []
    monkeypatch.setattr(time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(time, 'sleep', lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    def denied(*args, **kwargs):
        calls.append(True)
        clock[0] = 239.95
        raise fence.WriterFencedError('WRITER_GATE_TIMEOUT', 'synthetic timeout near deadline')
    monkeypatch.setattr(updates, 'launch_version', denied)
    with pytest.raises(fence.WriterFencedError):
        updates.bootstrap(control.parent / 'current/app', [RELAY], role='relay')
    assert clock[0] == 240.0
    assert calls == [True]


def test_b6_trial_relay_timeout_retries_without_duplicate_gui(endpoint, monkeypatch):
    control, stage, _, state, _, observation = endpoint
    clock = [0.0]
    monkeypatch.setattr(time, 'monotonic', lambda: clock[0])
    def pause(seconds):
        assert getattr(fence._WRITER_LOCAL, 'depth', 0) == 0
        clock[0] += seconds
    monkeypatch.setattr(time, 'sleep', pause)
    monkeypatch.setattr(updates, 'request_apply_task', lambda *_: None)
    native_launch = updates.launch_version
    roles = []
    def launch(*args, **kwargs):
        is_relay = args[2][:1] == [RELAY]
        roles.append('relay' if is_relay else 'gui')
        if not is_relay:
            return 123
        if roles.count('relay') == 1:
            raise fence.WriterFencedError('WRITER_GATE_TIMEOUT', 'synthetic transient timeout')
        pid = native_launch(*args, **kwargs)
        for child in getattr(endpoint, 'children', []):
            _, error = child.communicate(timeout=30)
            assert child.returncode == 0, error[-1500:]
        updates.write_json_durable(control / 'transition.json', {**state, 'phase': 'CONFIRMED'})
        return pid
    monkeypatch.setattr(updates, 'launch_version', launch)
    assert updates.bootstrap(control.parent / 'current/app', [], role='gui') == 0
    assert roles == ['gui', 'relay', 'relay']
    assert observation.exists() and json.loads(observation.read_text())['relay'] is True
    assert updates.read_json(stage / 'trial.json')['pid'] == 123
    assert not (stage / 'trial-failure.json').exists()

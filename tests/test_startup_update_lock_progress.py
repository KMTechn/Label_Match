"""Startup admission while the updater owns its mutex; no business IO."""
from contextlib import contextmanager
from copy import deepcopy
import json
from pathlib import Path
import sys
import threading
import time

import pytest

import auto_update as updates

NATIVE_LOCK = updates.update_lock
NATIVE_CLOCK = time.monotonic
NATIVE_SLEEP = time.sleep


@pytest.fixture
def waiting_startup(monkeypatch, tmp_path):
    control = tmp_path / 'managed/update-control'
    app = control.parent / 'current/app'
    policy = {'operator_sid': 'S-1-5-21-727', 'staging_root': str(tmp_path / 'stage')}
    sha = 'a' * 40
    records = {'active.json': {'sha': sha}, 'transition.json': {'schema': updates.SCHEMA, 'phase': 'CONFIRMED'},
               'policy.json': policy}
    clock = [0.0]
    world = {'busy_until': float('inf'), 'claim': 'ABSENT', 'generation': 'ALLOW',
             'reports': [], 'guidance': [], 'waits': [], 'released': 0}
    busy = ('update mutex ownership is busy' if hasattr(updates, '_startup_gate')
            else 'update mutex ownership is busy or abandoned')

    @contextmanager
    def lock(_control, _sid, *, timeout_ms=5000, **_kwargs):
        world['waits'].append(timeout_ms)
        clock[0] += min(timeout_ms / 1000, max(0, world['busy_until'] - clock[0]))
        if clock[0] < world['busy_until']:
            raise RuntimeError(busy)
        try:
            yield
        finally:
            world['released'] += 1

    monkeypatch.setattr(updates.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(updates.time, 'sleep', lambda delay: clock.__setitem__(0, clock[0] + delay))
    monkeypatch.setattr(updates, 'control_root', lambda _app: control)
    monkeypatch.setattr(updates, 'load_policy', lambda _control: policy, raising=False)
    monkeypatch.setattr(updates, 'read_json', lambda path: deepcopy(records[Path(path).name]))
    monkeypatch.setattr(updates, 'optional_record', lambda path: deepcopy(records.get(Path(path).name)))
    monkeypatch.setattr(updates, 'ordinary_user_context', lambda *_: True)
    monkeypatch.setattr(updates, 'version_manifest', lambda _: {'source_commit': sha})
    monkeypatch.setattr(updates, 'current_generation_status', lambda *_: world['generation'])
    monkeypatch.setattr(updates, 'prepared_pending', lambda *_: False)
    monkeypatch.setattr(updates, 'apply_claim_status', lambda _: world['claim'])
    monkeypatch.setattr(updates, 'update_lock', lock)
    monkeypatch.setattr(updates, 'emit_support_record', lambda fields, **_: world['reports'].append(fields))
    monkeypatch.setattr(updates, 'emit_progress_guidance', lambda: world['guidance'].append(True))
    monkeypatch.setattr(updates, 'request_bootstrap', lambda *_a, **_k: pytest.fail('unexpected bootstrap'))
    if hasattr(updates, 'bootstrap'):
        monkeypatch.setattr(updates, 'bootstrap', lambda *_a, **_k: pytest.fail('unexpected bootstrap'))
    for name in ('KMTECH_LM_UPDATE_DEFERRED_AT', 'KMTECH_LM_UPDATE_TRIAL_NONCE',
                 'KMTECH_CA_UPDATE_DEFERRED_AT', 'KMTECH_CA_UPDATE_TRIAL_NONCE'):
        monkeypatch.delenv(name, raising=False)
    return app, control, policy, records, clock, world


def test_startup_progress_budget_is_120_seconds():
    assert updates.APPLY_PROGRESS_TIMEOUT == 120
    assert updates.APPLY_REQUEST_PROGRESS_TIMEOUT == 60


def test_real_six_second_mutex_owner_does_not_abort_startup(waiting_startup, monkeypatch):
    app, control, policy, records, _, world = waiting_startup
    policy['operator_sid'] = updates.current_process_sid()
    before = deepcopy(records)
    monkeypatch.setattr(updates, 'update_lock', NATIVE_LOCK)
    monkeypatch.setattr(time, 'monotonic', NATIVE_CLOCK)
    monkeypatch.setattr(time, 'sleep', NATIVE_SLEEP)
    if hasattr(updates, 'bootstrap'):
        def still_busy(*_args, **_kwargs):
            with NATIVE_LOCK(control, policy['operator_sid']):
                return None
        monkeypatch.setattr(updates, 'bootstrap', still_busy)
    ready = threading.Event()
    errors = []

    def apply_owner():
        try:
            with NATIVE_LOCK(control, policy['operator_sid']):
                ready.set()
                NATIVE_SLEEP(12.2)
        except BaseException as error:
            errors.append(error)

    owner = threading.Thread(target=apply_owner)
    owner.start()
    try:
        assert ready.wait(10), 'native mutex owner did not acquire'
        started = NATIVE_CLOCK()
        assert updates.startup_gate(app, []) is None
        elapsed = NATIVE_CLOCK() - started
        assert 11.7 <= elapsed < 17.2
    finally:
        owner.join(15)
    assert not owner.is_alive() and not errors
    assert records == before and not world['reports'] and not world['guidance']


@pytest.mark.parametrize('release_at', [6.0, 119.0])
def test_gate_waits_then_both_calls_admit_current_without_writes(waiting_startup, release_at):
    app, _, _, records, clock, world = waiting_startup
    before = deepcopy(records)
    world['busy_until'] = release_at
    assert updates.startup_gate(app, []) is None
    assert release_at <= clock[0] < 120
    assert updates.startup_gate(app, []) is None
    assert world['released'] == 2 and len(world['waits']) >= 3
    assert max(world['waits']) <= 5000
    assert records == before and not world['reports'] and not world['guidance']


@pytest.mark.parametrize('claim,expected', [('ABSENT', 120), ('LIVE', 240), ('UNKNOWN', 240)])
def test_expired_wait_admits_only_allowed_current_without_writes(waiting_startup, claim, expected):
    app, _, _, records, clock, world = waiting_startup
    before = deepcopy(records)
    world['claim'] = claim
    assert updates.startup_gate(app, []) is None
    assert expected <= clock[0] < expected + 0.01
    assert world['reports'] == [{'reason_code': 'UPDATE_LOCK_WAIT_EXPIRED', 'exit_code': 0}]
    assert not world['guidance'] and not world['released'] and records == before


@pytest.mark.parametrize('refusal', ['different_active', 'trial', 'generation', 'recovery', 'retained_pointer'])
def test_expired_wait_preserves_admission_refusals(waiting_startup, refusal):
    app, _, _, records, _, world = waiting_startup
    state = records['transition.json']
    if refusal == 'different_active':
        records['active.json']['sha'] = 'b' * 40
    elif refusal == 'generation':
        world['generation'] = 'REFUSE'
    elif refusal == 'trial':
        state['phase'] = 'APPLIED_PENDING_READY'
    elif refusal == 'recovery':
        state.update(phase='DEFERRED', recovery_pending=True)
    else:
        state.update(phase='DEFERRED', retained_candidate=True, previous_sha='a' * 40,
                     candidate_sha='b' * 40, failure_decision={'selected_sha': 'b' * 40})
    before = deepcopy(records)
    assert updates.startup_gate(app, []) == 20
    assert world['guidance'] == [True] and not world['reports']
    assert records == before and not world['released']


@pytest.mark.parametrize('claim', ['LIVE', 'UNKNOWN'])
def test_live_or_unknown_claim_can_release_after_120(waiting_startup, claim):
    app, _, _, _, clock, world = waiting_startup
    world.update(claim=claim, busy_until=150.0)
    assert updates.startup_gate(app, []) is None
    assert clock[0] == 150.0 and world['released'] == 1


def test_wait_does_not_swallow_errors_from_the_admitted_body(waiting_startup):
    _, control, policy, _, _, world = waiting_startup
    world['busy_until'] = 0
    with pytest.raises(RuntimeError, match='body failure'):
        with updates.startup_update_lock(control, policy) as acquired:
            assert acquired
            raise RuntimeError('body failure')
    assert world['released'] == 1 and len(world['waits']) == 1


def test_apply_prepared_mode_keeps_powershell_stderr_quiet(waiting_startup, monkeypatch, capsys):
    _, control, policy, _, _, world = waiting_startup
    world['busy_until'] = 0
    monkeypatch.setattr(sys, 'argv', ['auto_update.py', '--apply-prepared'])
    with updates.startup_update_lock(control, policy) as acquired:
        assert acquired
    assert capsys.readouterr().err == ''


@pytest.mark.parametrize('acquired', [True, False])
def test_wait_emits_value_free_elapsed_diagnostic(waiting_startup, capsys, acquired):
    _, control, policy, _, _, world = waiting_startup
    world['busy_until'] = 6.0 if acquired else float('inf')
    with updates.startup_update_lock(control, policy) as result:
        assert result is acquired
    report = json.loads(capsys.readouterr().err)
    assert report == {'stage': 'update_lock_wait', 'elapsed_ms': 6000 if acquired else 120000,
                      'acquired': acquired}


@pytest.mark.parametrize('stream', ['absent', 'broken'])
def test_elapsed_diagnostic_is_best_effort(waiting_startup, monkeypatch, stream):
    _, control, policy, _, _, world = waiting_startup
    world['busy_until'] = 0

    class BrokenStream:
        def write(self, _value):
            raise OSError('closed diagnostic stream')

    with monkeypatch.context() as context:
        context.setattr(sys, 'stderr', None if stream == 'absent' else BrokenStream())
        with updates.startup_update_lock(control, policy) as acquired:
            assert acquired

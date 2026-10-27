"""Real startup gates and journals under daily-update contention."""
# Actual-bootstrap GREEN3/cap observations cover CA supervision,480,candidate Popen0 and UTC/monotonic boundaries.
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

import auto_update as updates
import current_user_onboarding as onboarding
import writer_session_fence as fence
from tests.test_auto_update import OLD, NEW
from tests.test_auto_update_endpoints import endpoint, hotfix_packet
from tests.test_onboarding_lifecycle_scope import managed_onboarding as base_managed_onboarding
from tests.test_onboarding_writer_admission import _onboard
from tests.test_current_user_onboarding import isolated_data_generation_release
from tests.test_portable_gui_onboarding import portable_gui

NATIVE_LOCK = updates.update_lock
NATIVE_LAUNCH = updates.launch_version
NATIVE_SLEEP = time.sleep
NATIVE_CLOCK = time.monotonic


from tests._update_clock_fixture import isolated_update_time as _isolated_update_time

@pytest.fixture
def managed_onboarding(base_managed_onboarding, monkeypatch):
    paths, environment, control, helpers = base_managed_onboarding
    operator_sid = updates.load_policy(control)["operator_sid"]
    monkeypatch.setattr(updates, "process_security", lambda _pid: {
        "sid": operator_sid, "elevated": False,
        "integrity_rid": 8192, "session_id": 1,
    })
    stage = control.parent / 'startup-stage'
    stage.mkdir()
    policy = updates.read_json(control / 'policy.json')
    updates.write_json_durable(control / 'policy.json', {**policy, 'staging_root': str(stage)})
    return paths, environment, control, helpers


@pytest.fixture(autouse=True)
def ordinary_start_environment(monkeypatch):
    monkeypatch.setenv('KMTECH_LM_UPDATE_DEFERRED_AT', '')
    monkeypatch.setenv('KMTECH_LM_UPDATE_TRIAL_NONCE', '')


@contextmanager
def held_update_lock(control, seconds, tmp_path):
    ready = tmp_path / 'lock-ready'
    source = '''import sys, time
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import auto_update as updates
control = Path(sys.argv[2])
with updates.update_lock(control, updates.current_process_sid()):
    Path(sys.argv[3]).touch()
    time.sleep(float(sys.argv[4]))
'''
    native = getattr(subprocess.Popen, 'native', subprocess.Popen)
    process = native([sys.executable, '-B', '-c', source, str(Path(updates.__file__).parent),
                      str(control), str(ready), str(seconds)],
                     stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                     creationflags=subprocess.CREATE_NO_WINDOW)
    try:
        deadline = NATIVE_CLOCK() + 10
        while not ready.exists() and process.poll() is None and NATIVE_CLOCK() < deadline:
            NATIVE_SLEEP(0.02)
        assert ready.exists(), 'isolated lock owner did not acquire the native mutex'
        yield process
    finally:
        process.communicate()
        assert process.returncode == 0, 'isolated lock owner failed'


def ordinary_endpoint(endpoint, monkeypatch):
    _clock_time = _isolated_update_time(monkeypatch)
    control, _stage, policy, _state, _marker, _observation = endpoint
    updates.write_json_durable(control / 'active.json', {'schema': updates.SCHEMA, 'sha': OLD})
    updates.write_json_durable(control / 'transition.json', {'schema': updates.SCHEMA, 'phase': 'DEFERRED'})
    native_sid = updates.current_process_sid()
    monkeypatch.setattr(updates, 'update_lock', lambda root, _sid, **kwargs: NATIVE_LOCK(root, native_sid, **kwargs))
    monkeypatch.setattr(_clock_time, 'sleep', NATIVE_SLEEP)
    monkeypatch.setattr(_clock_time, 'monotonic', NATIVE_CLOCK)
    monkeypatch.setattr(updates, "bootstrap_supervision_deadline",
        lambda state, *, started, now, utc_now, **_observation: started + updates.STARTUP_WAIT_SECONDS)
    monkeypatch.setattr(updates, 'launch_version', lambda *_args, **_kwargs: 99)
    monkeypatch.setattr(updates, 'request_bootstrap', lambda *_args, **_kwargs: pytest.fail('unexpected launcher'))
    return control, updates.version_bundle(control, OLD) / 'app'


@pytest.mark.parametrize('seconds', [6, 30], ids=['six-seconds', 'thirty-seconds'])
def test_startup_waits_for_another_process_update_lock(endpoint, monkeypatch, tmp_path, seconds):
    control, app = ordinary_endpoint(endpoint, monkeypatch)
    launched = []
    monkeypatch.setattr(updates, 'launch_version', lambda *_args, **_kwargs: launched.append(True) or 99)
    with held_update_lock(control, seconds, tmp_path):
        started = NATIVE_CLOCK()
        result = updates.startup_gate(app, [])
        elapsed = NATIVE_CLOCK() - started
    assert result in {None, 0}
    assert elapsed >= seconds - 0.5
    assert elapsed < seconds + 5
    assert result is None or launched == [True]


def test_startup_lock_limit_starts_current_and_preserves_journal(endpoint, monkeypatch, tmp_path, capsys):
    control, app = ordinary_endpoint(endpoint, monkeypatch)
    monkeypatch.setattr(updates, 'APPLY_PROGRESS_TIMEOUT', 0.1)
    before = {name: (control / name).read_bytes() for name in ('active.json', 'transition.json')}
    with held_update_lock(control, 6, tmp_path) as owner:
        started = NATIVE_CLOCK()
        assert updates.startup_gate(app, []) is None
        elapsed = NATIVE_CLOCK() - started
        assert owner.poll() is None, 'ordinary start must succeed before update ownership is released'
    assert 0.1 <= elapsed < 6
    assert all((control / name).read_bytes() == raw for name, raw in before.items())
    report = json.loads(capsys.readouterr().out)
    assert report['reason_code'] == 'UPDATE_LOCK_WAIT_EXPIRED' and report['exit_code'] == 0
    persisted = updates.read_json(Path(endpoint[2]['staging_root']) / 'startup-support.json')
    assert persisted['reason_code'] == report['reason_code'] and persisted['exit_code'] == 0


@pytest.mark.parametrize('refusal', ['generation', 'cohort'])
def test_busy_startup_keeps_current_refusal(endpoint, monkeypatch, capsys, refusal):
    control = endpoint[0]
    app = updates.version_bundle(control, NEW) / 'app'
    @contextmanager
    def busy(*_args, **_kwargs):
        raise RuntimeError('update mutex ownership is busy')
        yield
    monkeypatch.setattr(updates, 'update_lock', busy)
    monkeypatch.setattr(updates, 'APPLY_PROGRESS_TIMEOUT', 1)
    monkeypatch.setattr(updates, 'current_generation_status', lambda *_args: 'UNKNOWN' if refusal == 'generation' else 'ALLOW')
    monkeypatch.setattr(updates, 'launch_version', lambda *_args, **_kwargs: pytest.fail('denied current state must not start'))
    if refusal == 'generation':
        assert updates.startup_gate(app, []) == 20
        assert json.loads(capsys.readouterr().out)['reason_code'] == 'UPDATE_IN_PROGRESS'
    else:
        monkeypatch.setattr(updates, 'process_snapshot', lambda *_args: [{'pid': os.getpid() + 1, 'observable': True}])
        assert updates.startup_gate(app, []) == 20
        assert json.loads(capsys.readouterr().out)['reason_code'] == 'UPDATE_IN_PROGRESS'
        with pytest.raises(ValueError, match='unrelated writer'):
            updates.assert_ordinary_recovery(control, endpoint[2], NEW, updates.now_utc())


@pytest.mark.parametrize('damage', ['signature', 'marker'], ids=['bad-signature', 'malformed-marker'])
def test_failed_prepared_packet_is_quarantined_once(endpoint, monkeypatch, damage):
    control, stage, policy, _state, _marker, _observation = endpoint
    updates.write_json_durable(control / 'active.json', {'schema': updates.SCHEMA, 'sha': OLD})
    updates.write_json_durable(control / 'transition.json', {'schema': updates.SCHEMA, 'phase': 'DEFERRED'})
    hotfix_packet(endpoint, monkeypatch)
    if damage == 'signature':
        (stage / 'manifest.sig').write_bytes(b'invalid signature')
    else:
        (stage / 'prepared.json').write_bytes(b'{invalid marker')
        (stage / 'manifest.sig').write_bytes(b'invalid signature')
    original = {name: (stage / name).read_bytes() for name in ('prepared.json', 'manifest.json', 'manifest.sig', 'packet.zip')}
    assert updates.apply_prepared(control) == 0
    state = updates.read_json(control / 'transition.json')
    assert state['phase'] == 'DEFERRED' and state['reason_code'] == 'UPDATE_PREPARATION_FAILED'
    before = {path.name: path.read_bytes() for path in control.glob('*.json')}
    preserved = list(control.glob('packet-*'))
    assert len(preserved) == 1
    assert all((preserved[0] / name).read_bytes() == raw for name, raw in original.items())
    assert updates.apply_prepared(control) == 0
    assert {path.name: path.read_bytes() for path in control.glob('*.json')} == before
    assert not updates.prepared_pending(control, policy, updates.read_json(control / 'transition.json'))
    assert all((stage / name).read_bytes() == raw for name, raw in original.items())
    if damage == 'signature':
        assert updates.read_json(control / 'transition.json')['failed_shas']
        updates.write_json_durable(stage / 'prepared.json', {'source_commit': updates.read_json(stage / 'manifest.json')['source_commit'],
                                                          'prepared_at': updates.now_utc()})
        assert not updates.prepared_pending(control, policy, updates.read_json(control / 'transition.json'))
    (stage / 'prepared.json').write_text(json.dumps({'source_commit': NEW}))
    assert updates.prepared_pending(control, policy, updates.read_json(control / 'transition.json'))


def test_quarantine_does_not_skip_retained_pointer_recovery(endpoint):
    control, stage, _policy, state, _marker, _observation = endpoint
    updates.write_json_durable(stage / 'prepared.json', {'source_commit': NEW})
    updates.write_json_durable(control / 'active.json', {'schema': updates.SCHEMA, 'sha': OLD})
    updates.write_json_durable(control / 'transition.json', {
        **state, 'phase': 'DEFERRED', 'reason_code': 'UPDATE_PREPARATION_FAILED',
        'failed_prepared_sha256': hashlib.sha256((stage / 'prepared.json').read_bytes()).hexdigest(),
        'retained_candidate': True, 'failure_decision': {'selected_sha': NEW},
    })
    assert updates.apply_prepared(control) == 20
    assert updates.read_json(control / 'active.json')['sha'] == NEW


def test_transition_same_meaning_rewrite_during_onboarding_passes(managed_onboarding):
    paths, environment, control, helpers = managed_onboarding
    def launcher(_root):
        transition = control / 'transition.json'
        transition.write_text(json.dumps(updates.read_json(transition), indent=4) + '\n')
        return {'status': 'ALIVE', 'pid': 77}
    report = _onboard(paths, environment, relay_launcher=launcher)
    assert report['status'] == 'READY'
    assert [row['mode'] for row in helpers] == ['Verify', 'ObserveLifecycle']
    assert all(row['admission_depth'] == 0 for row in helpers)
    assert all(row['parent_admission_depth'] == 0 for row in helpers)


def test_startup_onboarding_rereads_changed_observation_under_update_lock(managed_onboarding, monkeypatch):
    paths, environment, control, helpers = managed_onboarding
    original = updates.lifecycle_task_observation
    observations, locks = [], []
    @contextmanager
    def observed_lock(*_args, **_kwargs):
        assert getattr(fence._WRITER_LOCAL, 'depth', 0) == 0
        locks.append(True)
        yield
    def rewrite(root):
        observations.append(True)
        if len(observations) == 1:
            state = updates.read_json(control / 'transition.json')
            updates.write_json_durable(control / 'transition.json', {**state, 'deferred_at': updates.now_utc()})
        return original(root)
    monkeypatch.setattr(updates, 'lifecycle_task_observation', rewrite)
    monkeypatch.setattr(updates, 'update_lock', observed_lock)
    report = _onboard(paths, environment, _startup=True,
                      relay_launcher=lambda _root: {'status': 'ALIVE', 'pid': 77})
    assert report['status'] == 'READY' and locks == [True]
    assert len(observations) == 2


def test_startup_onboarding_unresolved_race_keeps_current_with_unknown(managed_onboarding, monkeypatch, capsys):
    paths, environment, control, _helpers = managed_onboarding
    original = updates.lifecycle_task_observation
    def rewrite(root):
        state = updates.read_json(control / 'transition.json')
        updates.write_json_durable(control / 'transition.json', {**state, 'deferred_at': updates.now_utc()})
        return original(root)
    monkeypatch.setattr(updates, 'lifecycle_task_observation', rewrite)
    report = _onboard(paths, environment, _startup=True,
                      relay_launcher=lambda _root: pytest.fail('uncertain onboarding must not write READY'))
    assert report['status'] == 'UNKNOWN'
    emitted = json.loads(capsys.readouterr().out)
    assert emitted['status'] == 'UNKNOWN' and emitted['exit_code'] == 0
    assert not paths.onboarding_report_path.exists()
    persisted = updates.read_json(Path(updates.load_policy(control)['staging_root']) / 'startup-support.json')
    assert persisted['status'] == 'UNKNOWN' and persisted['exit_code'] == 0


def test_gui_keeps_data_guard_after_deferred_startup_onboarding(portable_gui, monkeypatch):
    import Label_Match as app
    calls = []
    def deferred(_root, **kwargs):
        assert kwargs.get('_startup') is True
        return {'status': 'UNKNOWN'}
    monkeypatch.setattr(app, 'onboard_current_user', deferred)
    monkeypatch.setattr(app, 'run_guarded_entrypoint', lambda *_args, **_kwargs: calls.append('guard') or 0)
    assert app.main([]) == 0 and calls == ['guard']


def test_startup_onboarding_pending_phase_uses_existing_ordinary_recovery(managed_onboarding, monkeypatch, capsys):
    # No prior startup_gate admission: preserve the existing UNKNOWN fallback.
    paths, environment, control, _helpers = managed_onboarding
    policy = updates.read_json(control / 'policy.json')
    policy.pop('staging_root')  # Legacy installation can still publish its console support record.
    updates.write_json_durable(control / 'policy.json', policy)
    state = updates.read_json(control / 'transition.json')
    updates.write_json_durable(control / 'transition.json', {**state, 'phase': 'PREPARED'})
    monkeypatch.setattr(updates, 'current_generation_status', lambda *_args: 'ALLOW')
    monkeypatch.setattr(updates, 'process_snapshot', lambda *_args: [])
    report = _onboard(paths, environment, _startup=True,
                      relay_launcher=lambda _root: pytest.fail('pending onboarding must not write READY'))
    assert report['status'] == 'UNKNOWN'
    assert os.environ.get('KMTECH_LM_UPDATE_DEFERRED_AT')
    with fence.writer_admission('gui_settings_save'):
        pass
    assert json.loads(capsys.readouterr().out)['exit_code'] == 0
    monkeypatch.delenv('KMTECH_LM_UPDATE_DEFERRED_AT', raising=False)


def test_startup_onboarding_keeps_generation_denial(managed_onboarding, monkeypatch):
    paths, environment, control, _helpers = managed_onboarding
    state = updates.read_json(control / 'transition.json')
    updates.write_json_durable(control / 'transition.json', {**state, 'phase': 'PREPARED'})
    monkeypatch.setattr(updates, 'current_generation_status', lambda *_args: 'UNKNOWN')
    monkeypatch.setattr(updates, 'process_snapshot', lambda *_args: [])
    with pytest.raises(ValueError, match='generation allowance'):
        _onboard(paths, environment, _startup=True,
                  relay_launcher=lambda _root: pytest.fail('generation denial must preserve registration'))


@pytest.mark.parametrize('creation', ['during', 'before'])
def test_startup_onboarding_preserves_existing_cohort_bound(managed_onboarding, monkeypatch, creation):
    paths, environment, control, _helpers = managed_onboarding
    original = updates.lifecycle_task_observation
    rows = []
    def rewrite(root):
        state = updates.read_json(control / 'transition.json')
        updates.write_json_durable(control / 'transition.json', {**state, 'phase': 'PREPARED'})
        rows.append({'pid': os.getpid() + 1, 'observable': True, 'ordinary_user': True,
                     'sha': updates.read_json(control / 'active.json')['sha'],
                     'sid': updates.load_policy(control)['operator_sid'], 'role': 'relay',
                     'created_at': updates.now_utc() if creation == 'during' else '2000-01-01T00:00:00Z'})
        return original(root)
    monkeypatch.setattr(updates, 'lifecycle_task_observation', rewrite)
    monkeypatch.setattr(updates, 'current_generation_status', lambda *_args: 'ALLOW')
    monkeypatch.setattr(updates, 'process_snapshot', lambda *_args: rows)
    if creation == 'before':
        with pytest.raises(ValueError, match='earlier or unrelated'):
            _onboard(paths, environment, _startup=True)
    else:
        assert _onboard(paths, environment, _startup=True)['status'] == 'UNKNOWN'
        with fence.writer_admission('gui_settings_save'):
            pass


def test_manual_onboarding_keeps_unsettled_phase_refusal(managed_onboarding):
    # Manual classification without an accepted startup proof remains strict.
    paths, environment, control, _helpers = managed_onboarding
    state = updates.read_json(control / 'transition.json')
    updates.write_json_durable(control / 'transition.json', {**state, 'phase': 'PREPARED'})
    with pytest.raises(ValueError, match='removal and reinstall'):
        _onboard(paths, environment, relay_launcher=lambda _root: pytest.fail('manual recovery must remain strict'))


def test_startup_onboarding_keeps_active_identity_denial(managed_onboarding, monkeypatch):
    paths, environment, control, _helpers = managed_onboarding
    state = updates.read_json(control / 'transition.json')
    updates.write_json_durable(control / 'transition.json', {**state, 'phase': 'PREPARED'})
    updates.write_json_durable(control / 'active.json', {'schema': updates.SCHEMA, 'sha': OLD})
    monkeypatch.setattr(updates, 'current_generation_status', lambda *_args: 'ALLOW')
    monkeypatch.setattr(updates, 'process_snapshot', lambda *_args: [])
    with pytest.raises(ValueError, match='not the active version'):
        _onboard(paths, environment, _startup=True,
                 relay_launcher=lambda _root: pytest.fail('wrong active identity must not register'))


def test_bound_trial_keeps_nonce_admission_when_startup_onboarding_is_pending(endpoint, monkeypatch, capsys):
    control, stage, _policy, state, _marker, _observation = endpoint
    app = updates.version_bundle(control, NEW) / 'app'
    monkeypatch.setattr(fence, '__file__', str(app / 'writer_session_fence.py'))
    monkeypatch.setattr(updates, 'runtime_matches', lambda *_args: True)
    monkeypatch.setenv('KMTECH_LM_UPDATE_TRIAL_NONCE', state['nonce'])
    updates.write_json_durable(stage / 'trial.json', {'nonce': state['nonce'],
                               'transition_id': state['transition_id'], 'pid': os.getpid()})
    assert updates.startup_gate(app, []) is None
    report = onboarding.onboard_current_user(app.parent, _startup=True)
    assert report['status'] == 'UNKNOWN'
    assert not os.environ.get('KMTECH_LM_UPDATE_DEFERRED_AT')
    with fence.writer_admission('gui_settings_save'):
        pass
    assert json.loads(capsys.readouterr().out)['exit_code'] == 0

"""Bootstrap readback preserves identity while letting its relay acquire ownership."""
from types import SimpleNamespace
import json
import time

import pytest

import user_relay


BUSY = "update mutex ownership is busy"


@pytest.mark.parametrize('nonrelay_writer', [False, True])
def test_terminal_bootstrap_releases_native_root_while_relay_becomes_ready(tmp_path, monkeypatch, nonrelay_writer):
    """A relay needing root then trial must run during its parent's READY wait."""
    import copy
    import os
    import threading
    import auto_update as updates
    import current_user_onboarding

    control, stage = tmp_path / 'control', tmp_path / 'stage'
    state = {'schema': updates.SCHEMA, 'phase': 'DEFERRED', 'retained_candidate': False}
    active = {'sha': 'a' * 40}
    records = {control / 'transition.json': state, control / 'active.json': active,
               stage / 'trial.json': None, stage / 'ready.json': None}
    policy = {'operator_sid': updates.current_process_sid(), 'staging_root': str(stage)}
    ready, launched = threading.Event(), threading.Event()
    workers, errors, deadlines = [], [], []
    native_lock = updates.update_lock

    def relay():
        try:
            launched.set()
            with native_lock(control, policy['operator_sid']):
                with native_lock(control, policy['operator_sid'], trial=True):
                    ready.set()
        except BaseException as error:
            errors.append(error)

    def launch(*args, deadline, **kwargs):
        deadlines.append(deadline)
        worker = threading.Thread(target=relay)
        workers.append(worker)
        worker.start()
        assert launched.wait(1)
        return os.getpid()

    def wait(*args, deadline, **kwargs):
        assert deadline is deadlines[0]
        return ready.wait(.5)

    monkeypatch.setattr(current_user_onboarding, '_machine_worker_context', lambda *a: None)
    monkeypatch.setattr(updates, 'control_root', lambda *a: control)
    monkeypatch.setattr(updates, 'load_policy', lambda *a: policy)
    monkeypatch.setattr(updates, 'ordinary_user_context', lambda *a: True)
    monkeypatch.setattr(updates, 'optional_record', lambda path: copy.deepcopy(records.get(path)))
    monkeypatch.setattr(updates, 'read_json', lambda path: copy.deepcopy(records[path]))
    monkeypatch.setattr(updates, 'apply_claim_status', lambda *a: 'NONE')
    monkeypatch.setattr(updates, 'relay_stop_requested', lambda *a: False)
    monkeypatch.setattr(updates, 'prepared_pending', lambda *a: False)
    nonrelays = [dict(pid=os.getpid() + 100, role='gui', observable=True,
        ordinary_user=True, sha=active['sha'], sid=policy['operator_sid'],
        created_at=updates.now_utc())] if nonrelay_writer else []
    monkeypatch.setattr(updates, 'process_snapshot', lambda *a: nonrelays)
    monkeypatch.setattr(updates, 'write_json_durable', lambda *a: None)
    monkeypatch.setattr(updates, '_launch_bootstrap_version', launch)
    monkeypatch.setattr(updates, 'wait_current_relay', wait)
    try:
        result = updates._bootstrap(tmp_path, ['--label-match-user-relay'], role='relay')
    finally:
        for worker in workers:
            worker.join(6)
    assert not errors and workers and not any(worker.is_alive() for worker in workers)
    assert result == 0, 'A parent holding root prevents its relay from reaching READY'


@pytest.mark.parametrize('changed_transition', [False, True])
def test_overlapping_terminal_bootstraps_share_one_live_relay_outside_root(tmp_path, monkeypatch, changed_transition):
    """Both requests wait for one launch; changed state remains fail-closed."""
    import copy
    import os
    import subprocess
    import sys
    import threading
    import auto_update as updates
    import current_user_onboarding
    control, stage = tmp_path / 'control', tmp_path / 'stage'
    state = {'schema': updates.SCHEMA, 'phase': 'DEFERRED', 'nonce': 'unit-nonce',
             'transition_id': 'unit-transition', 'retained_candidate': False}
    active = {'sha': 'a' * 40}
    records = {control / 'transition.json': state, control / 'active.json': active,
               stage / 'trial.json': None, stage / 'ready.json': None}
    policy = {'operator_sid': updates.current_process_sid(), 'staging_root': str(stage),
              'operator_local_app_data': str(tmp_path / 'operator')}
    first_wait, second_wait, ready = threading.Event(), threading.Event(), threading.Event()
    rows, launches, waits, outcomes, errors, relay_workers, children = [], [], [], [], [], [], []
    deadlines = {}
    native_lock, native_deadline = updates.update_lock, updates._relay_preparation_deadline

    def make_deadline(*args):
        value = native_deadline(*args)
        deadlines[threading.get_ident()] = value
        return value

    def relay():
        try:
            assert second_wait.wait(5)
            with native_lock(control, policy['operator_sid']):
                with native_lock(control, policy['operator_sid'], trial=True):
                    if changed_transition:
                        records[control / 'transition.json'] = {**state, 'phase': 'UNKNOWN'}
                    ready.set()
        except BaseException as error:
            errors.append(error)

    def launch(*args, deadline, **kwargs):
        assert deadline is deadlines[threading.get_ident()]
        child = subprocess.Popen([sys.executable, '-B', '-c',
            'import sys;sys.stdin.buffer.read(1)'], stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW)
        children.append(child)
        launches.append(child.pid)
        rows.append(dict(pid=child.pid, role='relay', observable=True, ordinary_user=True,
                         sha=active['sha'], sid=policy['operator_sid'], created_at=updates.now_utc()))
        worker = threading.Thread(target=relay)
        relay_workers.append(worker); worker.start()
        return child.pid

    def wait(*args, deadline, pid, **kwargs):
        assert deadline is deadlines[threading.get_ident()]
        waits.append((pid, deadline))
        (first_wait if len(waits) == 1 else second_wait).set()
        return ready.wait(5)

    monkeypatch.delenv('KMTECH_LM_UPDATE_DEFERRED_AT', raising=False)
    monkeypatch.setattr(current_user_onboarding, '_machine_worker_context', lambda *a: None)
    monkeypatch.setattr(updates, '_relay_preparation_deadline', make_deadline)
    monkeypatch.setattr(updates, 'control_root', lambda *a: control)
    monkeypatch.setattr(updates, 'load_policy', lambda *a: policy)
    monkeypatch.setattr(updates, 'ordinary_user_context', lambda *a: True)
    monkeypatch.setattr(updates, 'optional_record', lambda path: copy.deepcopy(records.get(path)))
    monkeypatch.setattr(updates, 'read_json', lambda path: copy.deepcopy(records[path]))
    monkeypatch.setattr(updates, 'apply_claim_status', lambda *a: 'NONE')
    monkeypatch.setattr(updates, 'relay_stop_requested', lambda *a: False)
    monkeypatch.setattr(updates, 'prepared_pending', lambda *a: False)
    monkeypatch.setattr(updates, 'process_snapshot', lambda *a: copy.deepcopy(rows))
    monkeypatch.setattr(updates, 'current_generation_status', lambda *a: 'ALLOW')
    monkeypatch.setattr(updates, 'write_json_durable', lambda *a: None)
    monkeypatch.setattr(updates, '_launch_bootstrap_version', launch)
    monkeypatch.setattr(updates, 'wait_current_relay', wait)
    def bootstrap():
        try:
            outcomes.append(updates._bootstrap(tmp_path, ['--label-match-user-relay'], role='relay'))
        except BaseException as error:
            errors.append(error)
    bootstraps = [threading.Thread(target=bootstrap) for _ in range(2)]
    try:
        bootstraps[0].start()
        assert first_wait.wait(5)
        bootstraps[1].start()
        for worker in bootstraps:
            worker.join(10)
    finally:
        second_wait.set()
        for worker in [*bootstraps, *relay_workers]:
            if worker.ident is not None:
                worker.join(10)
        for child in children:
            child.communicate(b'x', timeout=5)
            assert child.returncode == 0
    assert not errors and not any(worker.is_alive() for worker in [*bootstraps, *relay_workers])
    assert len(launches) == 1 and len(waits) == 2 and len(deadlines) == 2
    assert all(pid == launches[0] for pid, _ in waits)
    assert sorted(outcomes) == ([20, 20] if changed_transition else [0, 0])


@pytest.mark.parametrize('changed_transition', [False, True])
def test_terminal_exit_is_rechecked_under_root_before_the_existing_failure(tmp_path, monkeypatch, changed_transition):
    from contextlib import ExitStack, contextmanager
    import copy
    import auto_update as updates
    state = {'phase': 'DEFERRED', 'nonce': 'n', 'transition_id': 't'}
    identity = {'status': 'LIVE', 'creation_identity': 'unit-birth'}
    depth, events = [], []
    error = updates._RelayPreparationUnavailable('RELAY_EXITED_BEFORE_RUNNING')
    @contextmanager
    def lock(*args, **kwargs):
        assert not depth
        depth.append(True); events.append('root acquired')
        try:
            yield
        finally:
            depth.pop(); events.append('root released')
    def record(path):
        return copy.deepcopy(state) if path.name == 'transition.json' else None
    def readback():
        assert not depth
        events.append('relay exited')
        identity.update(status='ABSENT', creation_identity=None)
        if changed_transition:
            state['nonce'] = 'changed'
        raise error
    monkeypatch.setattr(updates, 'update_lock', lock)
    monkeypatch.setattr(updates, 'optional_record', record)
    monkeypatch.setattr(updates, 'read_json', lambda path: {'sha': 'a' * 40})
    monkeypatch.setattr(updates, 'native_process_identity', lambda pid: copy.deepcopy(identity))
    with ExitStack() as ownership:
        ownership.enter_context(lock())
        if changed_transition:
            assert updates._bootstrap_relay_readback(tmp_path, {'operator_sid': 'unit-sid',
                'staging_root': str(tmp_path)}, 'a' * 40, readback, ownership=ownership, pid=71) is False
        else:
            with pytest.raises(updates._RelayPreparationUnavailable) as caught:
                updates._bootstrap_relay_readback(tmp_path, {'operator_sid': 'unit-sid',
                    'staging_root': str(tmp_path)}, 'a' * 40, readback, ownership=ownership, pid=71)
            assert caught.value is error and caught.value.code == 'RELAY_EXITED_BEFORE_RUNNING'
        assert depth == [True]
    assert events == ['root acquired', 'root released', 'relay exited', 'root acquired', 'root released']


@pytest.mark.parametrize('changed', [None, 'phase', 'nonce', 'transition_id', 'active',
                                    'claim', 'ready', 'pid_birth', 'pid_exit', 'deadline',
                                    'pid_birth_after_reacquire', 'pid_exit_after_reacquire'])
def test_terminal_relay_readback_revalidates_every_protected_binding(tmp_path, monkeypatch, changed):
    from contextlib import ExitStack, contextmanager
    import copy
    import auto_update as updates

    control, stage = tmp_path / 'control', tmp_path / 'stage'
    state = {'phase': 'DEFERRED', 'nonce': 'n', 'transition_id': 't'}
    active = {'sha': 'a' * 40}
    records = {control / 'transition.json': state, control / 'active.json': active,
               stage / 'trial.json': {'pid': 71}, stage / 'ready.json': {'pid': 71}}
    identity = {'status': 'LIVE', 'creation_identity': '1234'}
    depth, events, clock, identity_reads = [], [], [10.0], []

    @contextmanager
    def lock(*a, **kw):
        assert not depth and not kw.get('trial')
        depth.append('root'); events.append('acquire')
        try:
            yield
        finally:
            depth.pop(); events.append('release')

    def readback():
        assert not depth
        events.append('wait')
        if changed in {'phase', 'nonce', 'transition_id'}:
            records[control / 'transition.json'][changed] = 'changed'
        elif changed == 'active':
            records[control / 'active.json']['sha'] = 'b' * 40
        elif changed in {'claim', 'ready'}:
            records[stage / ('trial.json' if changed == 'claim' else 'ready.json')]['pid'] = 72
        elif changed == 'pid_birth':
            identity['creation_identity'] = '5678'
        elif changed == 'pid_exit':
            identity['status'] = 'ABSENT'
        elif changed == 'deadline':
            clock[0] = 31.0
        return True

    monkeypatch.setattr(updates, 'update_lock', lock)
    monkeypatch.setattr(updates, 'optional_record', lambda path: copy.deepcopy(records[path]))
    monkeypatch.setattr(updates, 'read_json', lambda path: copy.deepcopy(records[path]))
    def native_identity(pid):
        identity_reads.append(list(depth))
        current = copy.deepcopy(identity)
        if len(identity_reads) == 3 and changed == 'pid_birth_after_reacquire':
            current['creation_identity'] = '5678'
        if len(identity_reads) == 3 and changed == 'pid_exit_after_reacquire':
            current['status'] = 'ABSENT'
        return current
    monkeypatch.setattr(updates, 'native_process_identity', native_identity)
    monkeypatch.setattr(updates.time, 'monotonic', lambda: clock[0])
    with ExitStack() as ownership:
        ownership.enter_context(lock())
        result = updates._bootstrap_relay_readback(control, {'operator_sid': 'unit-sid',
            'staging_root': str(stage)}, active['sha'], readback, ownership=ownership, pid=71, deadline=30.0)
        assert depth == ['root']
    assert result is (changed is None)
    assert events == ['acquire', 'release', 'wait', 'acquire', 'release']


def test_terminal_readback_reacquires_root_before_propagating_the_original_error(tmp_path, monkeypatch):
    from contextlib import ExitStack, contextmanager
    import auto_update as updates
    depth = []
    error = RuntimeError('unrelated readback failure')
    @contextmanager
    def lock(*a, **kw):
        assert not depth
        depth.append(True)
        try:
            yield
        finally:
            depth.pop()
    def readback():
        assert not depth
        raise error
    monkeypatch.setattr(updates, 'update_lock', lock)
    monkeypatch.setattr(updates, 'optional_record', lambda path: None)
    monkeypatch.setattr(updates, 'read_json', lambda path: {'sha': 'a' * 40})
    monkeypatch.setattr(updates, 'native_process_identity', lambda pid:
                        {'status': 'LIVE', 'creation_identity': '1234'})
    with ExitStack() as ownership:
        ownership.enter_context(lock())
        with pytest.raises(RuntimeError) as caught:
            updates._bootstrap_relay_readback(tmp_path, {'operator_sid': 'unit-sid',
                'staging_root': str(tmp_path)}, 'a' * 40, readback, ownership=ownership, pid=71)
        assert caught.value is error and depth == [True]


@pytest.mark.parametrize('changed', [None, 'status_pid', 'status_time', 'birth', 'missing_pid'])
def test_gui_terminal_readback_binds_the_discovered_relay_generation(tmp_path, monkeypatch, changed):
    from contextlib import ExitStack, contextmanager
    import copy
    import auto_update as updates
    import current_user_onboarding
    control, stage = tmp_path / 'control', tmp_path / 'stage'
    status_path = tmp_path / 'relay-status.json'
    status = {'status': 'RUNNING', 'process_id': 71, 'updated_at': '2026-10-24T00:00:01Z'}
    if changed == 'missing_pid':
        status.pop('process_id')
    records = {control / 'transition.json': {'phase': 'DEFERRED'},
               control / 'active.json': {'sha': 'a' * 40},
               stage / 'trial.json': None, stage / 'ready.json': None, status_path: status}
    depth, births = [], []
    @contextmanager
    def lock(*args, **kwargs):
        assert not depth
        depth.append(True)
        try:
            yield
        finally:
            depth.pop()
    def identity(pid):
        assert pid == 71
        births.append(bool(depth))
        return {'status': 'LIVE', 'creation_identity':
                '5678' if changed == 'birth' and len(births) > 1 else '1234'}
    def prove(*args, **kwargs):
        assert not depth and kwargs == {'started_at': '2026-10-24T00:00:00Z', 'pid': None}
        if changed == 'status_pid':
            status['process_id'] = 72
        if changed == 'status_time':
            status['updated_at'] = '2026-10-24T00:00:02Z'
        return True
    def readback():
        assert not depth
        return True
    monkeypatch.setattr(updates, 'update_lock', lock)
    monkeypatch.setattr(updates, 'optional_record', lambda path: copy.deepcopy(records[path]))
    monkeypatch.setattr(updates, 'read_json', lambda path: copy.deepcopy(records[path]))
    monkeypatch.setattr(updates, 'native_process_identity', identity)
    monkeypatch.setattr(updates, 'current_relay_ready', prove)
    monkeypatch.setattr(updates, 'version_bundle', lambda *args: tmp_path)
    monkeypatch.setattr(current_user_onboarding, 'resolve_current_user_onboarding_paths',
                        lambda *args: SimpleNamespace(direct_sync_root=tmp_path))
    monkeypatch.setattr(user_relay, 'user_relay_status_path', lambda *args: status_path)
    with ExitStack() as ownership:
        ownership.enter_context(lock())
        result = updates._bootstrap_relay_readback(control, {'operator_sid': 'unit-sid',
            'staging_root': str(stage)}, 'a' * 40, readback, ownership=ownership,
            started_at='2026-10-24T00:00:00Z')
        assert depth == [True]
    assert result is (changed is None)


def test_busy_owner_readback_retries_only_while_own_bootstrap_is_alive():
    attempts = []
    process = SimpleNamespace(pid=41, poll=lambda: None)

    def owner():
        attempts.append(True)
        if len(attempts) < 3:
            raise RuntimeError(BUSY)
        return {"process_id": 42, "updated_at": "2026-10-24T00:00:00Z"}

    result = user_relay._read_started_relay(
        process, owner, 0, time.monotonic() + 5, True, lambda _: None,
    )
    assert result["status"] == "ALIVE"
    assert len(attempts) == 3


@pytest.mark.parametrize("returncode,message", [
    (0, BUSY), (None, "update mutex ownership was abandoned"),
    (None, "unrelated runtime failure"),
])
def test_other_owner_readback_errors_keep_the_original_exception(returncode, message):
    original = RuntimeError(message)

    def owner():
        raise original

    with pytest.raises(RuntimeError) as caught:
        user_relay._read_started_relay(
            SimpleNamespace(pid=41, poll=lambda: returncode), owner,
            0, time.monotonic() + 5, True, lambda _: None,
        )
    assert caught.value is original


def test_busy_is_not_ignored_if_bootstrap_exits_during_the_query():
    state = [None]

    def owner():
        state[0] = 0
        raise RuntimeError(BUSY)

    with pytest.raises(RuntimeError, match=BUSY):
        user_relay._read_started_relay(
            SimpleNamespace(pid=41, poll=lambda: state[0]), owner,
            0, time.monotonic() + 5, True, lambda _: None,
        )


def test_busy_until_parent_deadline_remains_unproven(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(user_relay.time, "monotonic", lambda: clock[0])

    def owner():
        clock[0] += 1
        raise RuntimeError(BUSY)

    with pytest.raises(user_relay.UserRelayError, match="did not prove"):
        user_relay._read_started_relay(
            SimpleNamespace(pid=41, poll=lambda: None), owner,
            0, 3, True, lambda _: None,
        )


@pytest.mark.parametrize("exception_type", [OSError, RuntimeError])
def test_parent_owner_readback_exception_has_a_fixed_support_code(tmp_path, monkeypatch, exception_type):
    import auto_update
    import current_user_onboarding as onboarding

    original = exception_type("owner query failed")
    records, calls = [], []
    monkeypatch.setattr(onboarding, "_machine_worker_context", lambda *_: None)
    monkeypatch.setattr(onboarding, "resolve_current_user_onboarding_paths",
                        lambda *_: SimpleNamespace(direct_sync_root=tmp_path / "direct"))
    monkeypatch.setattr(user_relay, "build_user_relay_command",
                        lambda *_: ["test-runtime", "--bootstrap", "--"])
    monkeypatch.setattr(auto_update, "start_ordinary_process",
                        lambda *_a, **_kw: SimpleNamespace(pid=41, poll=lambda: None))

    def owner(*_args, **_kwargs):
        calls.append(True)
        if len(calls) > 1:
            raise original
        return None

    monkeypatch.setattr(user_relay, "_existing_user_relay_owner", owner)
    monkeypatch.setattr(user_relay, "_record_relay_reason", lambda code, **kw: records.append((code, kw)))
    with pytest.raises(exception_type) as caught:
        user_relay.start_user_relay_process(tmp_path, survival_seconds=0)
    assert caught.value is original
    assert [code for code, _ in records] == ["RELAY_OWNER_READBACK_FAILED"]
    assert records[0][1]["error"] is original


def test_owner_readback_support_code_survives_the_actual_public_record(capsys):
    user_relay._record_relay_reason("RELAY_OWNER_READBACK_FAILED", error=RuntimeError("private query detail"))
    record = json.loads(capsys.readouterr().out)
    assert record["reason_code"] == "RELAY_OWNER_READBACK_FAILED"
    assert record["exception_type"] == "RuntimeError"
    assert "nonconforming_fields" not in record
    assert "private query detail" not in json.dumps(record)


def _system_confirmation_case(tmp_path, monkeypatch, failures, *, busy_seconds=0, body_error=None):
    """Enter the actual SYSTEM function with an already switched trial."""
    from contextlib import contextmanager
    import auto_update as updates

    clock, attempts, observations, released = [0.0], [], [], []
    state = {"schema": updates.SCHEMA, "phase": "APPLIED_PENDING_READY", "previous_sha": "a" * 40,
             "candidate_sha": "b" * 40, "nonce": "unit-nonce", "transition_id": "unit-transition"}
    control, stage = tmp_path / "control", tmp_path / "stage"
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "operator"))
    monkeypatch.setattr(updates, "fixed_helper", lambda *_a, **_kw: None)
    monkeypatch.setattr(updates, "load_policy", lambda *_: {
        "operator_sid": "S-1-5-21-100-200-300-1001",
        "operator_local_app_data": str(tmp_path / "operator"), "staging_root": str(stage)})
    monkeypatch.setattr(updates, "optional_record",
                        lambda path: state.copy() if path.name == "transition.json" else None)
    monkeypatch.setattr(updates, "read_json",
                        lambda path: {"sha": "b" * 40} if path.name == "active.json" else state.copy())
    monkeypatch.setattr(updates, "write_json_durable", lambda *_: None)
    monkeypatch.setattr(updates, "new_apply_claim", lambda: {})
    monkeypatch.setattr(updates, "process_snapshot", lambda *_: [])
    monkeypatch.setattr(updates.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(updates.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))

    @contextmanager
    def lock(*_args, **_kwargs):
        attempts.append(clock[0])
        index = len(attempts) - 2  # The first lock protects the existing journal.
        if 0 <= index < len(failures) and failures[index] is not None:
            clock[0] += busy_seconds
            raise failures[index]
        try:
            yield
        finally:
            released.append(len(attempts))

    def observe(_control, _policy, current, *_args, deadline):
        observations.append(deadline)
        if body_error is not None:
            raise body_error
        return current, True, False

    monkeypatch.setattr(updates, "update_lock", lock)
    monkeypatch.setattr(updates, "observe_trial_ready", observe)
    return SimpleNamespace(run=lambda: updates._apply_prepared_once(control), attempts=attempts,
                           observations=observations, released=released)


def test_system_confirmation_acquisition_busy_retries_with_the_original_deadline(tmp_path, monkeypatch):
    case = _system_confirmation_case(tmp_path, monkeypatch, [RuntimeError(BUSY), RuntimeError(BUSY)])
    assert case.run() == 0
    assert len(case.attempts) == 4
    assert case.observations == [120.0]
    assert case.released == [1, 4]


@pytest.mark.parametrize("error", [RuntimeError("update mutex ownership was abandoned"), OSError("lock failed")])
def test_system_confirmation_other_acquisition_errors_are_unchanged(tmp_path, monkeypatch, error):
    case = _system_confirmation_case(tmp_path, monkeypatch, [error])
    with pytest.raises(type(error)) as caught:
        case.run()
    assert caught.value is error
    assert len(case.attempts) == 2 and case.observations == []


@pytest.mark.parametrize("message", [BUSY, "confirmation body failed"])
def test_system_confirmation_body_errors_are_never_pending(tmp_path, monkeypatch, message):
    original = RuntimeError(message)
    case = _system_confirmation_case(tmp_path, monkeypatch, [], body_error=original)
    with pytest.raises(RuntimeError) as caught:
        case.run()
    assert caught.value is original
    assert len(case.attempts) == 2 and case.released == [1, 2]


@pytest.mark.parametrize("elapsed", [120, 121])
def test_system_confirmation_busy_after_deadline_keeps_the_original_error(tmp_path, monkeypatch, elapsed):
    original = RuntimeError(BUSY)
    case = _system_confirmation_case(tmp_path, monkeypatch, [original], busy_seconds=elapsed)
    with pytest.raises(RuntimeError) as caught:
        case.run()
    assert caught.value is original
    assert len(case.attempts) == 2 and case.observations == []


def test_system_confirmation_busy_cannot_renew_the_confirmation_window(tmp_path, monkeypatch):
    original = RuntimeError(BUSY)
    case = _system_confirmation_case(tmp_path, monkeypatch, [original] * 3, busy_seconds=50)
    with pytest.raises(RuntimeError) as caught:
        case.run()
    assert caught.value is original
    assert len(case.attempts) == 4 and case.observations == []


def _rollback_confirmation_case(tmp_path, monkeypatch, failures, *, busy_seconds=0,
                                body_error=None, timeout_seconds=120):
    from contextlib import contextmanager
    import auto_update as updates

    clock, attempts, observations, released = [0.0], [], [], []
    state = {"phase": "ROLLBACK_PENDING_READY", "previous_sha": "a" * 40}
    policy = {"operator_sid": "S-1-5-21-100-200-300-1001", "staging_root": str(tmp_path / "stage")}
    monkeypatch.setattr(updates, "read_json",
                        lambda path: {"sha": "a" * 40} if path.name == "active.json" else state.copy())
    monkeypatch.setattr(updates, "optional_record", lambda *_: None)
    monkeypatch.setattr(updates, "process_snapshot", lambda *_: [])
    monkeypatch.setattr(updates.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(updates.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))

    @contextmanager
    def lock(*_args, **_kwargs):
        attempts.append(clock[0])
        index = len(attempts) - 1
        if index < len(failures) and failures[index] is not None:
            clock[0] += busy_seconds
            raise failures[index]
        try:
            yield
        finally:
            released.append(len(attempts))

    def observe(_control, _policy, current, *_args, deadline):
        observations.append(deadline)
        if body_error is not None:
            raise body_error
        return current, True, False

    monkeypatch.setattr(updates, "update_lock", lock)
    monkeypatch.setattr(updates, "observe_trial_ready", observe)
    return SimpleNamespace(run=lambda: updates.wait_for_rollback_ready(
        tmp_path / "control", policy, timeout_seconds=timeout_seconds), attempts=attempts,
        observations=observations, released=released)


@pytest.mark.parametrize("timeout_seconds", [120, 17])
def test_rollback_confirmation_acquisition_busy_uses_the_original_deadline(tmp_path, monkeypatch, timeout_seconds):
    case = _rollback_confirmation_case(tmp_path, monkeypatch,
        [RuntimeError(BUSY), RuntimeError(BUSY)], timeout_seconds=timeout_seconds)
    assert case.run() == 0
    assert len(case.attempts) == 3 and case.released == [3]
    assert case.observations == [float(timeout_seconds)]


@pytest.mark.parametrize("error", [RuntimeError("update mutex ownership was abandoned"),
                                 RuntimeError("other acquisition failure"), OSError("lock failed")])
def test_rollback_confirmation_other_acquisition_errors_are_unchanged(tmp_path, monkeypatch, error):
    case = _rollback_confirmation_case(tmp_path, monkeypatch, [error])
    with pytest.raises(type(error)) as caught:
        case.run()
    assert caught.value is error
    assert len(case.attempts) == 1 and case.observations == []


@pytest.mark.parametrize("message", [BUSY, "rollback confirmation body failed"])
def test_rollback_confirmation_body_errors_are_never_pending(tmp_path, monkeypatch, message):
    original = RuntimeError(message)
    case = _rollback_confirmation_case(tmp_path, monkeypatch, [], body_error=original)
    with pytest.raises(RuntimeError) as caught:
        case.run()
    assert caught.value is original
    assert len(case.attempts) == 1 and case.released == [1]


@pytest.mark.parametrize("elapsed", [120, 121])
def test_rollback_confirmation_busy_after_deadline_keeps_the_original_error(tmp_path, monkeypatch, elapsed):
    original = RuntimeError(BUSY)
    case = _rollback_confirmation_case(tmp_path, monkeypatch, [original], busy_seconds=elapsed)
    with pytest.raises(RuntimeError) as caught:
        case.run()
    assert caught.value is original
    assert len(case.attempts) == 1 and case.observations == []


def test_rollback_confirmation_busy_cannot_renew_the_window(tmp_path, monkeypatch):
    original = RuntimeError(BUSY)
    case = _rollback_confirmation_case(tmp_path, monkeypatch, [original] * 3, busy_seconds=50)
    with pytest.raises(RuntimeError) as caught:
        case.run()
    assert caught.value is original
    assert len(case.attempts) == 3 and case.observations == []


def test_rollback_final_lock_outside_the_window_keeps_the_original_busy(tmp_path, monkeypatch):
    original = RuntimeError(BUSY)
    case = _rollback_confirmation_case(tmp_path, monkeypatch, [original], timeout_seconds=0)
    with pytest.raises(RuntimeError) as caught:
        case.run()
    assert caught.value is original
    assert len(case.attempts) == 1 and case.observations == []


def test_diagnostic_native_live_identity_uses_the_original_process_handle():
    import subprocess
    import sys
    from tests._first_setup_host import _native_guard_process, _native_process_times
    process = subprocess.Popen([sys.executable, "-B", "-c", "import time; time.sleep(30)"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, creationflags=subprocess.CREATE_NO_WINDOW)
    try:
        born = _native_process_times(process)["creation_identity"]
        proof = _native_guard_process(process, born, process.pid, time.time_ns())
        assert proof["native_alive"] is True and proof["native_pid_creation_matches"] is True
        assert proof["native_exit_code"] is None and proof["native_exit_guard_seconds"] is None
        assert proof["native_wall_seconds"] >= 0
        assert proof["native_cpu_seconds"] >= 0
        assert proof["native_cpu_seconds"] == pytest.approx(
            proof["native_cpu_kernel_seconds"] + proof["native_cpu_user_seconds"])
        assert _native_guard_process(process, born + 1, process.pid, time.time_ns())["native_pid_creation_matches"] is False
    finally:
        process.terminate()
        process.wait(timeout=10)


def test_diagnostic_native_exit_code_and_time_survive_child_exit():
    import subprocess
    import sys
    from tests._first_setup_host import _native_guard_process, _native_process_times
    process = subprocess.Popen([sys.executable, "-B", "-c", "raise SystemExit(23)"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, creationflags=subprocess.CREATE_NO_WINDOW)
    try:
        assert process.wait(timeout=10) == 23
        born = _native_process_times(process)["creation_identity"]
        proof = _native_guard_process(process, born, process.pid, time.time_ns())
        assert proof["native_alive"] is False and proof["native_pid_creation_matches"] is True
        assert proof["native_exit_code"] == 23 and proof["native_exit_guard_seconds"] <= 0
        assert proof["native_wall_seconds"] >= 0 and proof["native_cpu_seconds"] >= 0
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=10)


def test_diagnostic_cim_observer_preserves_valid_powershell_syntax(tmp_path):
    import subprocess
    from pathlib import Path
    from tests._first_setup_host import observe_script
    source = Path(__file__).resolve().parents[1] / "tools/apply_prepared_update.ps1"
    script = tmp_path / "query.ps1"
    script.write_text(observe_script(source, tmp_path / "current", tmp_path / "processes.json",
        diagnostic=tmp_path / "diagnostic.json", target_pid=1), encoding="utf-8-sig")
    escaped = str(script).replace("'", "''")
    command = ("$lex=$null;$issues=$null;[System.Management.Automation.Language.Parser]::ParseFile('"
        + escaped + "',[ref]$lex,[ref]$issues)|Out-Null;"
        "$issues|ForEach-Object{$_.ErrorId}|ConvertTo-Json -Compress;exit [int]($issues.Count -ne 0)")
    result = subprocess.run(["powershell.exe", "-NoLogo", "-NoProfile", "-NonInteractive",
        "-WindowStyle", "Hidden", "-Command", command], capture_output=True, text=True,
        creationflags=subprocess.CREATE_NO_WINDOW, timeout=30)
    assert result.returncode == 0, result.stdout


@pytest.mark.parametrize("raises", [False, True])
def test_guard_trace_is_restored_after_bootstrap_call(raises):
    import sys
    from tests import _first_setup_host as host
    previous = sys.gettrace()
    def trace(frame, event, argument):
        return None
    def callback():
        assert sys.gettrace() is trace
        if raises:
            raise ValueError("observer self-check")
        return 23
    if raises:
        with pytest.raises(ValueError, match="observer self-check"):
            host._run_with_trace(callback, trace)
    else:
        assert host._run_with_trace(callback, trace) == 23
    assert sys.gettrace() is previous


def test_sync_timeout_observer_keeps_original_child_and_records_later_exit(tmp_path):
    import subprocess
    import sys
    from tests import _first_setup_host as host
    config = tmp_path / "host-config.json"
    config.write_text("{}", encoding="utf-8")
    started, release = tmp_path / "child-started", tmp_path / "child-release"
    script = ("from pathlib import Path\nimport time\nroot=Path(" + repr(str(tmp_path)) + ")\n"
              "(root/'child-started').touch()\n"
              "while not (root/'child-release').exists():time.sleep(0.02)\n"
              "raise SystemExit(23)\n")
    child = subprocess.Popen([sys.executable, "-B", "-c", script],
                             creationflags=subprocess.CREATE_NO_WINDOW)
    child.host_test_output = tmp_path / "child.private.log"
    child.host_test_output.write_bytes(b"")
    try:
        # Measure the observer's later-exit path after this owned child's
        # interpreter has started, independently of host import/startup load.
        startup_deadline = time.monotonic() + 15
        while not started.exists() and child.poll() is None and time.monotonic() < startup_deadline:
            time.sleep(0.02)
        assert started.exists() and child.poll() is None
        with pytest.raises(subprocess.TimeoutExpired):
            host._wait_observed_sync_child(child, config, 0.01)
        assert child.poll() is None
        release.touch()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            paths = list((tmp_path / "direct-sync-child-observations").glob("*.json"))
            record = json.loads(paths[0].read_bytes()) if paths else {}
            if "native_after_wait" in record:
                break
            time.sleep(0.02)
        assert record["wait_error_type"] == "TimeoutExpired"
        assert record["native_at_wait_end"]["native_alive"] is True
        assert record["native_after_wait"]["native_alive"] is False
        assert record["native_after_wait"]["native_exit_code"] == 23
        assert record["native_after_wait"]["native_exit_guard_seconds"] > 0
        assert child.wait(timeout=5) == 23
    finally:
        if child.poll() is None:
            child.terminate()
            child.wait(timeout=5)


def _initial_confirmation_case(tmp_path, monkeypatch, failures, *, phase="APPLIED_PENDING_READY",
                               age=60, busy_seconds=5, unreadable=False, malformed=False,
                               body_error=None):
    from contextlib import contextmanager
    from datetime import datetime, timedelta, timezone
    import auto_update as updates
    clock, attempts, observations, reads, depth = [0.0], [], [], [], [0]
    origin = datetime(2026, 1, 1, tzinfo=timezone.utc)
    state = {"schema": updates.SCHEMA, "phase": phase, "previous_sha": "a" * 40,
             "candidate_sha": "b" * 40, "nonce": "protected-nonce", "transition_id": "protected-transition",
             "applied_at": (origin-timedelta(seconds=age)).isoformat(),
             "rollback_at": (origin-timedelta(seconds=age)).isoformat()}
    if malformed:
        state["applied_at"] = state["rollback_at"] = "invalid"
    control, stage = tmp_path / "control", tmp_path / "stage"
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "operator"))
    monkeypatch.setattr(updates, "fixed_helper", lambda *_a, **_kw: None)
    monkeypatch.setattr(updates, "load_policy", lambda *_: {
        "operator_sid": "S-1-5-21-100-200-300-1001", "operator_local_app_data": str(tmp_path / "operator"),
        "staging_root": str(stage)})
    def optional(path):
        if path.name != "transition.json":
            return None
        reads.append(bool(depth[0]))
        if not depth[0] and unreadable:
            raise OSError("unit journal read failure")
        if depth[0] and body_error is not None:
            raise body_error
        return {**state, "nonce": state["nonce"] if depth[0] else "untrusted-budget-only"}
    monkeypatch.setattr(updates, "optional_record", optional)
    monkeypatch.setattr(updates, "read_json", lambda path: {
        "sha": "a" * 40 if phase == "ROLLBACK_PENDING_READY" else "b" * 40
    } if path.name == "active.json" else state.copy())
    monkeypatch.setattr(updates, "write_json_durable", lambda *_: None)
    monkeypatch.setattr(updates, "new_apply_claim", lambda: {})
    monkeypatch.setattr(updates, "process_snapshot", lambda *_: [])
    monkeypatch.setattr(updates.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(updates.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0]+seconds))
    monkeypatch.setattr(updates, "now_utc", lambda: (origin+timedelta(seconds=clock[0])).isoformat())
    @contextmanager
    def lock(*_a, **_kw):
        index = len(attempts)
        attempts.append(clock[0])
        if index < len(failures) and failures[index] is not None:
            clock[0] += busy_seconds
            raise failures[index]
        depth[0] += 1
        try:
            yield
        finally:
            depth[0] -= 1
    def observe(_control, _policy, current, *_args, deadline):
        assert current["nonce"] == "protected-nonce"
        observations.append(deadline)
        return current, True, False
    monkeypatch.setattr(updates, "update_lock", lock)
    monkeypatch.setattr(updates, "observe_trial_ready", observe)
    return SimpleNamespace(run=lambda: updates._apply_prepared(control), attempts=attempts,
                           observations=observations, reads=reads, clock=clock)


@pytest.mark.parametrize("phase", ["APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"])
def test_initial_pending_acquisition_busy_uses_journal_window_and_protected_reread(tmp_path, monkeypatch, phase):
    busy = RuntimeError("update mutex ownership is busy")
    case = _initial_confirmation_case(tmp_path, monkeypatch, [busy, busy, None], phase=phase)
    assert case.run() == 0
    assert case.observations == [60.0]
    assert len(case.attempts) == 4
    assert False in case.reads and True in case.reads


@pytest.mark.parametrize("phase", ["APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"])
def test_initial_pending_expired_journal_cannot_open_another_window(tmp_path, monkeypatch, phase):
    busy = RuntimeError("update mutex ownership is busy")
    case = _initial_confirmation_case(tmp_path, monkeypatch, [busy], phase=phase, age=120)
    with pytest.raises(RuntimeError) as caught:
        case.run()
    assert caught.value is busy and len(case.attempts) == 1 and not case.observations


@pytest.mark.parametrize("options", [{"phase": "DEFERRED"}, {"unreadable": True}, {"malformed": True}])
def test_initial_busy_without_readable_pending_window_is_fail_closed(tmp_path, monkeypatch, options):
    busy = RuntimeError("update mutex ownership is busy")
    case = _initial_confirmation_case(tmp_path, monkeypatch, [busy], **options)
    with pytest.raises(RuntimeError) as caught:
        case.run()
    assert caught.value is busy and len(case.attempts) == 1 and not case.observations


@pytest.mark.parametrize("error", [RuntimeError("update mutex ownership was abandoned"), OSError("unit acquisition error")])
def test_initial_acquisition_other_errors_are_never_pending(tmp_path, monkeypatch, error):
    case = _initial_confirmation_case(tmp_path, monkeypatch, [error])
    with pytest.raises(type(error)) as caught:
        case.run()
    assert caught.value is error and len(case.attempts) == 1 and not case.reads


def test_initial_acquired_body_busy_is_not_retried(tmp_path, monkeypatch):
    busy = RuntimeError("update mutex ownership is busy")
    case = _initial_confirmation_case(tmp_path, monkeypatch, [None], body_error=busy)
    with pytest.raises(RuntimeError) as caught:
        case.run()
    assert caught.value is busy and len(case.attempts) == 1 and case.reads == [True]


@pytest.mark.parametrize("phase", ["APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"])
def test_initial_busy_retries_cannot_renew_journal_deadline(tmp_path, monkeypatch, phase):
    busy = RuntimeError("update mutex ownership is busy")
    case = _initial_confirmation_case(tmp_path, monkeypatch, [busy]*8, phase=phase, age=119, busy_seconds=0.5)
    with pytest.raises(RuntimeError) as caught:
        case.run()
    assert caught.value is busy and len(case.attempts) == 2 and case.clock[0] == 1.25


def test_initial_retry_does_not_start_an_acquisition_at_the_journal_deadline(tmp_path, monkeypatch):
    busy = RuntimeError("update mutex ownership is busy")
    case = _initial_confirmation_case(tmp_path, monkeypatch, [busy]*8, age=119, busy_seconds=0.75)
    with pytest.raises(RuntimeError) as caught:
        case.run()
    assert caught.value is busy and len(case.attempts) == 1 and case.clock[0] == 1.0


def test_native_launch_publishes_cim_metadata_before_child_interpreter_runs(tmp_path, monkeypatch):
    import subprocess
    import sys
    from types import SimpleNamespace
    from tests import _first_setup_host as host
    native = getattr(subprocess.Popen, "native", subprocess.Popen)
    def paused_child(_command, **kwargs):
        return native([sys.executable, "-B", "-c", "import time;time.sleep(0.5)"], **kwargs)
    monkeypatch.setattr(subprocess, "Popen", SimpleNamespace(native=paused_child))
    config = tmp_path / "host-config.json"
    config.write_text(json.dumps({"repo": str(tmp_path), "current": str(tmp_path / "current")}), encoding="utf-8")
    child = host.launch(config, str(tmp_path / "candidate/app/label_match_relay_entry.py"), ["--user-session-relay"])
    try:
        assert child.poll() is None
        path = tmp_path / "processes" / (str(child.pid) + ".json")
        assert path.is_file(), "The CIM adapter must see the native launch before child_main imports"
        row = json.loads(path.read_bytes())
        assert row["CreationIdentity"] == host._native_process_times(child)["creation_identity"]
        assert row["ProcessId"] == child.pid and row["OrdinaryUser"] is True and row["Sid"] == host.SID
        assert row["ExecutablePath"] == str(tmp_path / "candidate/runtime/pythonw.exe")
    finally:
        child.wait(timeout=5)


@pytest.mark.parametrize('failure_at', [None, 'enter', 'body'])
def test_read_only_lock_observer_preserves_original_context_and_error(tmp_path, failure_at):
    from contextlib import contextmanager
    from tests._first_setup_lock_observer import Timeline, observed_update_lock
    import os
    error = RuntimeError('update mutex ownership is busy')
    calls = []
    @contextmanager
    def native(control, sid, **options):
        calls.append(('enter', control, sid, options))
        if failure_at == 'enter':
            raise error
        try:
            yield
        finally:
            calls.append(('release',))
    timeline = Timeline(tmp_path, 'unit')
    lock = observed_update_lock(native, 'native-sid', timeline)
    def execute():
        with lock(tmp_path, 'virtual-sid', trial=True):
            calls.append(('body',))
            if failure_at == 'body':
                raise error
    if failure_at:
        with pytest.raises(RuntimeError) as caught:
            execute()
        assert caught.value is error
    else:
        execute()
    assert calls[0] == ('enter', tmp_path, 'native-sid', {'trial': True})
    events = [json.loads(line) for line in timeline.path.read_text().splitlines()]
    kinds = [item['event'] for item in events]
    assert kinds[0] == 'update_request'
    assert ('update_acquired' in kinds) == (failure_at != 'enter')
    assert ('update_released' in kinds) == (failure_at != 'enter')
    assert all(item['pid'] == os.getpid() for item in events)
    if failure_at:
        assert next(item for item in events if item['event']=='update_error')['busy'] is True


def test_lock_observer_writer_methods_preserve_lease_and_native_arguments(tmp_path):
    from contextlib import contextmanager
    import sys
    from types import SimpleNamespace
    from tests._first_setup_lock_observer import install
    calls = []
    class Lease:
        handle, owned, abandoned = 7, True, False
        def suspend(self):
            calls.append('suspend'); self.owned=False
        def resume(self, timeout):
            calls.append(('resume', timeout)); self.owned=True
        def release(self):
            calls.append('release'); self.owned=False; self.handle=0
    lease = Lease()
    def acquire(name, timeout):
        calls.append(('acquire', name, timeout)); return lease
    @contextmanager
    def lock(*args, **kwargs):
        yield
    updates = SimpleNamespace()
    fence = SimpleNamespace(_acquire_named_mutex=acquire, _NamedMutexLease=Lease)
    previous = sys.getprofile()
    try:
        timeline = install(tmp_path, updates, fence, lock, 'native-sid', tmp_path)
        assert fence._acquire_named_mutex('original-native-name', 5.0) is lease
        lease.suspend(); lease.resume(5.0); lease.release()
    finally:
        sys.setprofile(previous)
    assert calls == [('acquire','original-native-name',5.0),'suspend',('resume',5.0),'release']
    events = [json.loads(line) for line in timeline.path.read_text().splitlines()]
    assert [e['event'] for e in events if e['event'].startswith('writer_')] == [
        'writer_request','writer_acquired','writer_suspend_request','writer_suspend_return',
        'writer_resume_request','writer_resume_return','writer_release_request','writer_release_return']


@pytest.mark.parametrize('phase', ['APPLIED_PENDING_READY', 'ROLLBACK_PENDING_READY'])
def test_ready_data_validation_is_read_only_until_explicit_receipt_commit(tmp_path, monkeypatch, phase):
    import auto_update as updates
    from tests.test_update_rollback_readiness import test_parent_bootstrap_publishes_readonly_relay_ready_without_self_nonce
    checked = []
    def split_publish(app, pid):
        before = {p:p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}
        proof = updates._relay_ready_receipt(app, pid)
        assert not isinstance(proof, bool)
        assert {p:p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()} == before
        checked.append(proof)
        return updates._write_ready_receipt(proof)
    monkeypatch.setattr(updates, 'publish_relay_ready', split_publish)
    test_parent_bootstrap_publishes_readonly_relay_ready_without_self_nonce(tmp_path, monkeypatch, phase, 'valid')
    assert len(checked) == 1


@pytest.mark.parametrize('changed', [None, 'phase', 'nonce', 'transition_id', 'active',
                                    'pid', 'birth', 'dead', 'unknown', 'failure', 'ready'])
def test_trial_observation_is_discarded_when_locked_recheck_changes(tmp_path, monkeypatch, changed):
    import auto_update as updates
    control, stage = tmp_path/'control', tmp_path/'stage'
    state = dict(phase='APPLIED_PENDING_READY',nonce='1'*32,transition_id='transition-one')
    active = {'sha':'a'*40}
    claim = dict(pid=71,nonce=state['nonce'],transition_id=state['transition_id'],role='relay')
    policy = dict(staging_root=str(stage))
    current_state, current_active, current_claim = dict(state),dict(active),dict(claim)
    identity = dict(status='LIVE',creation_identity='1234')
    current_identity = dict(identity)
    failure = None
    if changed in {'phase','nonce','transition_id'}:current_state[changed]='changed'
    if changed == 'active':current_active['sha']='b'*40
    if changed == 'pid':current_claim['pid']=72
    if changed == 'birth':current_identity['creation_identity']='5678'
    if changed == 'dead':current_identity={'status':'ABSENT'}
    if changed == 'unknown':identity=current_identity={'status':'UNKNOWN'}
    if changed == 'failure':failure=dict(nonce=state['nonce'],transition_id=state['transition_id'])
    records = {control/'transition.json':current_state, control/'active.json':current_active,
               stage/'trial.json':current_claim,stage/'trial-failure.json':failure,
               stage/'ready.json':{'changed':True} if changed=='ready' else None}
    monkeypatch.setattr(updates,'optional_record',lambda p:records[p])
    monkeypatch.setattr(updates,'read_json',lambda p:records[p])
    monkeypatch.setattr(updates,'native_process_identity',lambda pid:current_identity)
    writes=[]
    monkeypatch.setattr(updates,'write_json_durable',lambda p,v:writes.append((p,v)))
    payload=dict(nonce=state['nonce'],transition_id=state['transition_id'],pid=71,sha=active['sha'])
    proof=(stage/'ready.json',payload)
    result=updates._commit_bootstrap_trial_observation(control,policy,state,active,claim,(identity,proof,None))
    assert result is (changed is None)
    assert writes == ([(stage/'ready.json',payload)] if changed is None else [])


def test_trial_observation_read_checks_take_no_product_lock_or_writer_admission(tmp_path, monkeypatch):
    import auto_update as updates
    state=dict(nonce='1'*32,transition_id='transition-one')
    active={'sha':'a'*40}
    claim=dict(pid=71,role='relay',transition_id=state['transition_id'],claimed_at='2026-10-24T00:00:02Z')
    row=dict(pid=71,created_at='2026-10-24T00:00:01Z')
    policy=dict(operator_sid='unit-sid')
    monkeypatch.setattr(updates,'update_lock',lambda *a,**k:pytest.fail('read took update lock'))
    monkeypatch.setattr(updates,'native_process_identity',lambda pid:dict(status='LIVE',creation_identity='1234'))
    monkeypatch.setattr(updates,'process_snapshot',lambda root:[row])
    monkeypatch.setattr(updates,'process_matches_trial',lambda *a,**k:True)
    monkeypatch.setattr(updates,'version_bundle',lambda *a:tmp_path/'bundle')
    monkeypatch.setattr(updates,'_relay_ready_receipt',lambda *a:False)
    import writer_session_fence as fence
    monkeypatch.setattr(fence,'_acquire_named_mutex',lambda *a:pytest.fail('read acquired writer admission'))
    identity,proof,error=updates._read_bootstrap_trial_observation(tmp_path,policy,state,active,claim)
    assert identity==dict(status='LIVE',creation_identity='1234') and proof is False and error is None


def _startup_trial_case(tmp_path, monkeypatch, errors, *, phase='APPLIED_PENDING_READY',
                        age=60, mismatch=None, busy_seconds=5):
    from contextlib import contextmanager
    from datetime import datetime,timedelta,timezone
    import auto_update as updates
    clock=[0.0]; attempts=[]; bootstraps=[]
    origin=datetime(2026,10,24,tzinfo=timezone.utc)
    state=dict(phase=phase,nonce='1'*32,transition_id='2'*32,
               applied_at=(origin-timedelta(seconds=age)).isoformat(),
               rollback_at=(origin-timedelta(seconds=age)).isoformat())
    claim=dict(nonce=state['nonce'],transition_id=state['transition_id'])
    control=tmp_path/'control';policy=dict(operator_sid='unit-sid',staging_root=str(tmp_path/'stage'))
    if mismatch=='nonce':claim['nonce']='3'*32
    if mismatch=='transition':claim['transition_id']='4'*32
    if mismatch=='time':state.pop('applied_at');state.pop('rollback_at')
    if mismatch=='format':claim=[]
    read_attempts=[]
    def optional(path):
        read_attempts.append(path)
        if mismatch=='unreadable' and len(read_attempts)==1:raise OSError('unit read refusal')
        return state if path==control/'transition.json' else claim
    monkeypatch.setattr(updates,'optional_record',optional)
    monkeypatch.setattr(updates.time,'monotonic',lambda:clock[0])
    monkeypatch.setattr(updates.time,'sleep',lambda seconds:clock.__setitem__(0,clock[0]+seconds))
    monkeypatch.setattr(updates,'now_utc',lambda:(origin+timedelta(seconds=clock[0])).isoformat())
    monkeypatch.setenv('KMTECH_LM_UPDATE_TRIAL_NONCE','5'*32 if mismatch=='environment_nonce' else '1'*32)
    monkeypatch.delenv('KMTECH_LM_UPDATE_DEFERRED_AT',raising=False)
    @contextmanager
    def lock(*a,**kw):
        index=len(attempts);attempts.append(clock[0])
        error=errors[min(index,len(errors)-1)]
        if error is not None:
            clock[0]+=busy_seconds
            raise error
        yield
    monkeypatch.setattr(updates,'update_lock',lock)
    def gate(*a,**kw):
        with updates._startup_trial_lock(control,policy,role='relay'):
            return None
    monkeypatch.setattr(updates,'_startup_gate',gate)
    monkeypatch.setattr(updates,'control_root',lambda app:control)
    monkeypatch.setattr(updates,'bootstrap',lambda *a,**kw:bootstraps.append(True) or 37)
    return SimpleNamespace(run=lambda:updates.startup_gate(tmp_path,['--label-match-user-relay'],role='relay'),
        attempts=attempts,bootstraps=bootstraps,clock=clock,control=control,policy=policy)


@pytest.mark.parametrize('phase',['APPLIED_PENDING_READY','ROLLBACK_PENDING_READY'])
def test_nonce_startup_busy_retries_only_original_durable_window(tmp_path,monkeypatch,phase):
    busy=RuntimeError(BUSY)
    case=_startup_trial_case(tmp_path,monkeypatch,[busy,busy,None],phase=phase)
    assert case.run() is None and len(case.attempts)==3 and not case.bootstraps


@pytest.mark.parametrize('mismatch',['nonce','transition','time','format','unreadable','environment_nonce','expired'])
def test_nonce_startup_invalid_or_expired_context_keeps_original_busy_fallback(tmp_path,monkeypatch,mismatch):
    case=_startup_trial_case(tmp_path,monkeypatch,[RuntimeError(BUSY)],
        age=120 if mismatch=='expired' else 60,mismatch=mismatch)
    assert case.run()==37 and len(case.attempts)==1 and case.bootstraps==[True]


def test_nonce_startup_cannot_renew_window_after_repeated_busy(tmp_path,monkeypatch):
    case=_startup_trial_case(tmp_path,monkeypatch,[RuntimeError(BUSY)],age=119,busy_seconds=0.5)
    assert case.run()==37 and len(case.attempts)==2 and case.clock[0]==pytest.approx(1.1)


@pytest.mark.parametrize('error',[RuntimeError('update mutex ownership was abandoned'),OSError('unit acquisition failure')])
def test_nonce_startup_other_acquisition_errors_are_never_retried(tmp_path,monkeypatch,error):
    import auto_update as updates
    case=_startup_trial_case(tmp_path,monkeypatch,[error])
    with pytest.raises(type(error)) as caught:
        with updates._startup_trial_lock(case.control,case.policy,role='relay'):pass
    assert caught.value is error and len(case.attempts)==1


def test_nonce_startup_acquired_body_busy_is_never_retried(tmp_path,monkeypatch):
    import auto_update as updates
    case=_startup_trial_case(tmp_path,monkeypatch,[None])
    error=RuntimeError(BUSY)
    with pytest.raises(RuntimeError) as caught:
        with updates._startup_trial_lock(case.control,case.policy,role='relay'):raise error
    assert caught.value is error and len(case.attempts)==1


@pytest.mark.parametrize('observe_seconds,changed',[(15,False),(30,False),(15,True),(15,'birth')])
def test_existing_claim_bootstrap_releases_both_locks_for_observe_and_system(tmp_path,monkeypatch,observe_seconds,changed):
    from contextlib import contextmanager
    import copy
    import auto_update as updates
    import current_user_onboarding
    from tests.test_relay_trial_roles import pending
    control,stage=tmp_path/'control',tmp_path/'stage'
    state=pending();active={'sha':state['candidate_sha']}
    claim=dict(pid=71,role='relay',nonce=state['nonce'],transition_id=state['transition_id'],claimed_at=updates.now_utc())
    records={control/'transition.json':state,control/'active.json':active,
             stage/'trial.json':claim,stage/'ready.json':None,stage/'trial-failure.json':None}
    policy=dict(operator_sid='unit-sid',staging_root=str(stage))
    clock=[0.0];depth=[];held=[];system=[];writes=[];requests=[];birth=['1234'];native_depths=[]
    @contextmanager
    def lock(*a,trial=False,**kw):
        name='trial' if trial else 'root'
        assert depth==(['root'] if trial else [])
        began=clock[0];depth.append(name)
        try:yield
        finally:
            assert depth.pop()==name;held.append((name,clock[0]-began))
    monkeypatch.setattr(updates,'update_lock',lock)
    monkeypatch.setattr(updates.time,'monotonic',lambda:clock[0])
    def sleep(t):
        clock[0]+=t
        if (changed=='birth' and system) or writes:
            records[control/'transition.json']={**state,'phase':'CONFIRMED'}
    monkeypatch.setattr(updates.time,'sleep',sleep)
    monkeypatch.setattr(updates,'_relay_preparation_deadline',lambda *a:updates._RelayPreparationDeadline(1000))
    monkeypatch.setattr(current_user_onboarding,'_machine_worker_context',lambda *a:None)
    monkeypatch.setattr(updates,'control_root',lambda *a:control)
    monkeypatch.setattr(updates,'load_policy',lambda *a:policy)
    monkeypatch.setattr(updates,'ordinary_user_context',lambda *a:True)
    monkeypatch.setattr(updates,'optional_record',lambda p:copy.deepcopy(records[p]))
    monkeypatch.setattr(updates,'read_json',lambda p:copy.deepcopy(records[p]))
    monkeypatch.setattr(updates,'apply_claim_status',lambda *a:
        'LIVE' if records[control/'transition.json']['phase']=='APPLIED_PENDING_READY' else 'ABSENT')
    monkeypatch.setattr(updates,'relay_stop_requested',lambda *a:False)
    monkeypatch.setattr(updates,'prepared_pending',lambda *a:False)
    monkeypatch.setattr(updates,'request_apply_task',lambda *a:requests.append(True))
    monkeypatch.setattr(updates,'_launch_bootstrap_version',lambda *a,**kw:pytest.fail('existing claim launched another child'))
    def identity(*a):
        native_depths.append(list(depth))
        return dict(status='LIVE',creation_identity=birth[0])
    monkeypatch.setattr(updates,'native_process_identity',identity)
    monkeypatch.setattr(updates,'process_matches_trial',lambda *a,**kw:True)
    def observe(*a):
        assert depth==[]
        clock[0]+=observe_seconds
        with lock(control,policy['operator_sid']):system.append(True)
        if changed is True:records[control/'transition.json']={**state,'phase':'CONFIRMED'}
        if changed=='birth':birth[0]='5678'
        return [dict(pid=71,created_at=state['applied_at'])]
    monkeypatch.setattr(updates,'process_snapshot',observe)
    def receipt(*a):
        assert depth==[]
        return stage/'ready.json',dict(nonce=state['nonce'],transition_id=state['transition_id'],pid=71,sha=active['sha'])
    monkeypatch.setattr(updates,'_relay_ready_receipt',receipt)
    def write(path,value):
        assert depth==['root','trial']
        writes.append(path);records[path]=value
    monkeypatch.setattr(updates,'write_json_durable',write)
    assert updates._bootstrap(tmp_path,['--label-match-user-relay'],role='relay')==0
    assert system==[True] and requests==[True]
    assert writes==([] if changed else [stage/'ready.json'])
    assert held and max(seconds for _,seconds in held)<=1 and not depth
    assert native_depths and all(d==['root','trial'] for d in native_depths)


def test_overlapping_empty_observations_do_not_launch_two_trial_relays(tmp_path, monkeypatch):
    """Two unlocked empty reads cannot override one protected live claim."""
    from contextlib import contextmanager
    import copy
    import threading
    import time
    import auto_update as updates
    import current_user_onboarding
    from tests.test_relay_trial_roles import pending

    control, stage = tmp_path / 'control', tmp_path / 'stage'
    state = pending()
    active = {'sha': state['candidate_sha']}
    records = {control / 'transition.json': state, control / 'active.json': active,
               stage / 'trial.json': None, stage / 'ready.json': None,
               stage / 'trial-failure.json': None}
    policy = dict(operator_sid='unit-sid', staging_root=str(stage))
    ownership = {'root': threading.RLock(), 'trial': threading.RLock()}
    local = threading.local()
    begin = threading.Barrier(2)
    empty_reads = threading.Barrier(2)
    launches, claims, observed, committed, outcomes, errors = [], [], [], [], [], []

    @contextmanager
    def lock(*args, trial=False, **kwargs):
        name = 'trial' if trial else 'root'
        depth = getattr(local, 'depth', [])
        assert depth == (['root'] if trial else [])
        assert ownership[name].acquire(timeout=2)
        local.depth = depth + [name]
        try:
            yield
        finally:
            local.depth = depth
            ownership[name].release()

    monkeypatch.setattr(updates, 'update_lock', lock)
    monkeypatch.setattr(updates, '_relay_preparation_deadline',
                        lambda *args: updates._RelayPreparationDeadline(time.monotonic() + 60))
    monkeypatch.setattr(current_user_onboarding, '_machine_worker_context', lambda *args: None)
    monkeypatch.setattr(updates, 'control_root', lambda *args: control)
    monkeypatch.setattr(updates, 'load_policy', lambda *args: policy)
    monkeypatch.setattr(updates, 'ordinary_user_context', lambda *args: True)
    monkeypatch.setattr(updates, 'optional_record', lambda path: copy.deepcopy(records[path]))
    monkeypatch.setattr(updates, 'read_json', lambda path: copy.deepcopy(records[path]))
    monkeypatch.setattr(updates, 'apply_claim_status', lambda *args:
                        'LIVE' if records[control / 'transition.json']['phase'] == 'APPLIED_PENDING_READY' else 'ABSENT')
    monkeypatch.setattr(updates, 'relay_stop_requested', lambda *args: False)
    monkeypatch.setattr(updates, 'prepared_pending', lambda *args: False)
    monkeypatch.setattr(updates, 'request_apply_task', lambda *args: None)

    def launch(*args, **kwargs):
        assert local.depth == ['root', 'trial']
        launches.append(threading.get_ident())
        return 71

    def write(path, value):
        assert local.depth == ['root', 'trial']
        if path == stage / 'trial.json':
            claims.append(copy.deepcopy(value))
        records[path] = copy.deepcopy(value)

    def identity(pid):
        assert local.depth == ['root', 'trial']
        assert pid == 71
        return dict(status='LIVE', creation_identity='1234')

    def observe(*args):
        assert local.depth == []
        observed.append(threading.get_ident())
        empty_reads.wait(timeout=2)
        return []

    original_commit = updates._commit_bootstrap_trial_observation

    def commit(*args, **kwargs):
        try:
            return original_commit(*args, **kwargs)
        finally:
            committed.append(threading.get_ident())
            if len(committed) == 2:
                records[control / 'transition.json'] = {**state, 'phase': 'CONFIRMED'}

    monkeypatch.setattr(updates, '_launch_bootstrap_version', launch)
    monkeypatch.setattr(updates, 'write_json_durable', write)
    monkeypatch.setattr(updates, 'native_process_identity', identity)
    monkeypatch.setattr(updates, 'process_snapshot', observe)
    monkeypatch.setattr(updates, '_commit_bootstrap_trial_observation', commit)

    def bootstrap():
        try:
            begin.wait(timeout=2)
            outcomes.append(updates._bootstrap(tmp_path, ['--label-match-user-relay'], role='relay'))
        except BaseException as error:
            errors.append(error)
            records[control / 'transition.json'] = {**state, 'phase': 'CONFIRMED'}

    workers = [threading.Thread(target=bootstrap) for _ in range(2)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=5)
    assert not any(worker.is_alive() for worker in workers)
    assert not errors and outcomes == [0, 0]
    assert len(set(observed)) == len(set(committed)) == 2
    assert len(launches) == len(claims) == 1
    assert claims[0]['pid'] == 71 and claims[0]['role'] == 'relay'


@pytest.mark.parametrize('name', ['_launch_bootstrap_version', 'start_current',
                                  'recovery_writer_result', 'wait_current_relay'])
def test_product_call_labels_delegate_without_a_global_profile(tmp_path, name):
    from contextlib import contextmanager
    from types import SimpleNamespace
    import sys
    from tests._first_setup_lock_observer import install

    result = object()
    original_error = ValueError('controlled observer failure')
    calls = []
    def original(*args, **kwargs):
        calls.append((args, kwargs))
        if kwargs.get('fail'):
            raise original_error
        return result
    class Lease:
        def suspend(self):
            pass
        def resume(self, timeout):
            pass
        def release(self):
            pass
    @contextmanager
    def lock(*args, **kwargs):
        yield
    updates = SimpleNamespace(**{name: original})
    fence = SimpleNamespace(_acquire_named_mutex=lambda *args: None, _NamedMutexLease=Lease)
    previous = sys.getprofile()
    timeline = install(tmp_path, updates, fence, lock, 'unit-sid', tmp_path)
    assert sys.getprofile() is previous
    wrapped = getattr(updates, name)
    assert wrapped(result, _label=result, _original=result) is result
    with pytest.raises(ValueError) as caught:
        wrapped(result, fail=True)
    assert caught.value is original_error and sys.getprofile() is previous
    assert calls == [((result,), dict(_label=result, _original=result)),
                     ((result,), dict(fail=True))]
    events = [json.loads(line) for line in timeline.path.read_text().splitlines()
              if json.loads(line)['event'] in {'product_call', 'product_return'}]
    assert [row['event'] for row in events] == ['product_call', 'product_return'] * 2
    assert all(row['frames'][0]['function'] == name for row in events)
    assert events[1]['error_type'] is None and events[3]['error_type'] == 'ValueError'
    assert events[1]['elapsed_seconds'] >= 0 and events[3]['elapsed_seconds'] >= 0


@pytest.mark.parametrize('changed', [None, 'gui', 'phase', 'nonce', 'transition_id',
                                    'active', 'pid', 'claim', 'role'])
def test_candidate_observe_delay_is_bound_to_one_existing_applied_claim(tmp_path, changed):
    from tests._first_setup_host import _claim_update_observe_delay
    from tests.test_relay_trial_roles import pending

    state = pending()
    active = {'sha': state['candidate_sha']}
    claim = dict(pid=71, role='relay', nonce=state['nonce'], transition_id=state['transition_id'])
    if changed == 'gui':
        claim['role'] = 'gui'
    elif changed == 'phase':
        state['phase'] = 'CONFIRMED'
    elif changed in {'nonce', 'transition_id'}:
        claim[changed] = 'different'
    elif changed == 'active':
        active['sha'] = 'different'
    elif changed == 'pid':
        claim['pid'] = None
    elif changed == 'claim':
        claim = None
    elif changed == 'role':
        claim['role'] = 'different'
    proof = _claim_update_observe_delay(tmp_path, state, active, claim, 30)
    assert bool(proof) is (changed in (None, 'gui'))
    assert (tmp_path / 'update-slow-observe-started').exists() is bool(proof)
    if proof:
        assert proof['requested_seconds'] == 30 and proof['candidate_pid'] == 71
        assert proof['nonce_match'] and proof['transition_id_match'] and proof['active_candidate_match']
        assert proof['applied_at'] == state['applied_at']
    assert _claim_update_observe_delay(tmp_path, state, active, claim, 30) is None

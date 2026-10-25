"""Value-free host observation delegates once and retains the SYSTEM deadline."""
from contextlib import contextmanager
import json
import time
from types import SimpleNamespace

import pytest

from tests._first_setup_lock_observer import install


def test_ready_window_observer_records_actual_deadline_without_new_state_reads(tmp_path):
    calls, result = [], object()
    error = ValueError('controlled READY observation failure')
    def original(*args, **kwargs):
        calls.append((args, kwargs))
        if kwargs.get('fail'):
            raise error
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
    updates = SimpleNamespace(observe_trial_ready=original, READY_WINDOW_SECONDS=240)
    fence = SimpleNamespace(_acquire_named_mutex=lambda *args: None, _NamedMutexLease=Lease)
    timeline = install(tmp_path, updates, fence, lock, 'unit-sid', tmp_path)
    state = dict(phase='APPLIED_PENDING_READY')
    deadline = time.monotonic() + 240
    args = (tmp_path, {}, state, [], None, None)
    assert updates.observe_trial_ready(*args, deadline=deadline) is result
    with pytest.raises(ValueError) as caught:
        updates.observe_trial_ready(*args, deadline=deadline, fail=True)
    assert caught.value is error
    assert calls == [(args, dict(deadline=deadline)), (args, dict(deadline=deadline, fail=True))]
    events = [json.loads(line) for line in timeline.path.read_text().splitlines()]
    observations = [row for row in events if row['event'] == 'ready_window']
    assert len(observations) == 2
    assert all(row['configured_seconds'] == 240 and 0 < row['remaining_seconds'] <= 240
               and row['phase'] == state['phase'] for row in observations)
    assert not any(row['event'] == 'ready_window_observation_error' for row in events)

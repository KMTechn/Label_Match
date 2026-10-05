"""Shortage UNDO expires on the owner Tk loop, including durable recovery."""
import copy
from datetime import datetime, timezone
from types import MethodType

import pytest

import Label_Match as label_module
from tests.test_label_match_core import _FakeEntry
from tests.test_w9scanonly_central import central  # noqa: F401
from tests.test_w9scanonly_packaging import packaging, scan, completions  # noqa: F401
from tests.test_deferred_lease_clock_retry import clock_case  # noqa: F401
from tests.test_w9scanonly_short_tray import scanner_clock  # noqa: F401


@pytest.fixture
def shortage(central, scanner_clock, monkeypatch):
    app, dialogs, case, root, _releases = central
    jobs = {}
    sequence = [0]
    original_after = root.after

    def after(delay, callback, *args):
        if delay < 1000:
            return original_after(delay, callback, *args)
        sequence[0] += 1
        key = f"grace-{sequence[0]}"
        jobs[key] = (scanner_clock[0] + delay / 1000, callback, args)
        return key

    def cancel(key):
        jobs.pop(key, None)
        root.after_cancel(key)

    app.after, app.after_cancel = after, cancel
    monkeypatch.setattr(_FakeEntry, 'focus_set', lambda self: None, raising=False)
    app._refresh_session_tree = lambda: None
    app._idle_instruction_text = lambda: "다음 현품표"
    app._return_to_idle_after_finalized_set = MethodType(
        type(app)._return_to_idle_after_finalized_set, app)
    master = case.group['scan_payload']
    sample = case.snapshot['work_group_source']['members'][0]['normalized_barcode']
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get('_phs2_background_pending'), timeout=5)
    identity = app.current_set_info['id']
    scan(app, sample)
    scanner_clock[0] += 5
    scan(app, master)
    root.run_until(lambda: not app.ui_lane.is_busy(), timeout=5)
    assert len(completions(app)) == 1 and not app.blocks and not dialogs
    assert app.package_outbox.get_by_set_id(identity) is None
    queued = []
    queue = type(app)._queue_authoritative_package

    def observe_queue(owner, **kwargs):
        released = kwargs.get('current_set_info') or {}
        if released.get('shortage_released'):
            queued.append(released['id'])
        return queue(owner, **kwargs)

    monkeypatch.setattr(type(app), '_queue_authoritative_package', observe_queue)

    def advance(seconds):
        scanner_clock[0] += seconds
        while True:
            due = [(key, value) for key, value in jobs.items() if value[0] <= scanner_clock[0]]
            if not due:
                break
            key, (_at, callback, args) = min(due, key=lambda pair: pair[1][0])
            jobs.pop(key)
            callback(*args)

    return app, identity, advance, jobs, queued, master, scanner_clock


def test_idle_sixty_seconds_releases_one_authoritative_outbox(shortage):
    app, identity, advance, _jobs, queued, _master, _clock = shortage
    saved = app.data_manager.load_current_state()['current_set_info']
    assert saved['shortage_undo_snapshot']
    assert saved['shortage_confirmed_at'] == saved['shortage_undo_snapshot']['shortage_confirmed_at']
    advance(59)
    assert app.package_outbox.get_by_set_id(identity) is None
    advance(1)
    assert app.package_outbox.get_by_set_id(identity) is not None
    assert queued == [identity]
    advance(120)
    assert queued == [identity] and not app.blocks


@pytest.mark.parametrize('boundary', ['next_scan', 'undo', 'shutdown'])
def test_boundary_invalidates_even_an_already_queued_timer(shortage, boundary):
    app, identity, advance, jobs, queued, master, _clock = shortage
    callbacks = [entry[1] for entry in jobs.values()]
    advance(59)
    if boundary == 'next_scan':
        following = master.replace('ITG-WORK-ONE', 'ITG-NEXT').replace('LBL-WORK-PACKAGE', 'LBL-NEXT')
        scan(app, following)
        assert app.current_set_info['raw'] == [following]
    elif boundary == 'undo':
        scan(app, 'KMCMD:UNDO')
    else:
        assert app._release_shortage_confirmation()
    assert queued == ([] if boundary == 'undo' else [identity])
    assert bool(app.package_outbox.get_by_set_id(identity)) == (boundary != 'undo')
    advance(1)
    for callback in callbacks:
        callback()  # A Tk callback already dequeued before after_cancel.
    assert queued == ([] if boundary == 'undo' else [identity])
    assert bool(app.package_outbox.get_by_set_id(identity)) == (boundary != 'undo')
    assert not app.blocks


def _restart(shortage, *, age=None, legacy=False, pending=False):
    app, _identity, _advance, jobs, _queued, _master, clock = shortage
    saved = app.data_manager.load_current_state()
    current = saved['current_set_info']
    stamp = datetime.fromtimestamp(label_module.time.time() - (age or 0), timezone.utc).isoformat()
    if pending:
        snapshot = copy.deepcopy(current['shortage_undo_snapshot'])
        snapshot.update(shortage_confirmed=True, shortage_confirmation_pending=True,
                        shortage_undo_snapshot=current['shortage_undo_snapshot'])
        saved['current_set_info'] = current = snapshot
    for record in (current, current['shortage_undo_snapshot']):
        if legacy:
            record.pop('shortage_confirmed_at', None)
        elif age is not None:
            record['shortage_confirmed_at'] = stamp
    assert app.data_manager.save_current_state(saved)
    jobs.clear()  # Old interpreter callbacks do not survive a restart.
    restarted = object.__new__(type(app))
    restarted.__dict__.update(app.__dict__)
    for key, value in list(restarted.__dict__.items()):
        if isinstance(value, MethodType) and value.__self__ is app:
            restarted.__dict__[key] = MethodType(value.__func__, restarted)
        if key.startswith('_shortage_confirmation_') or key == '_scanner_shortage_undo':
            restarted.__dict__.pop(key, None)
    restarted.current_set_info = {}
    restarted._sync_worker_record_context = lambda: None
    restarted._recover_pending_package_pin_moves = lambda: None
    restarted._finalize_label_recovery_holds = lambda: True
    restarted._load_current_set_state_checked()
    restarted.ui_lane._root.run_until(lambda: not restarted.ui_lane.is_busy(), timeout=5)
    # The original fixture owns the lane and writer teardown; both owners
    # observe the same durable outbox, while callbacks belong to the new owner.
    return restarted


@pytest.mark.parametrize('age,delay', [(61, 0), (10, 50), (60, 0)])
def test_restart_uses_durable_confirmation_time(shortage, age, delay):
    app, identity, advance, _jobs, queued, _master, _clock = shortage
    restarted = _restart(shortage, age=age)
    if delay:
        advance(delay - 1)
        assert app.package_outbox.get_by_set_id(identity) is None
        advance(1)
    assert app.package_outbox.get_by_set_id(identity) is not None
    assert queued == [identity] and not restarted.blocks


@pytest.mark.parametrize('pending', [False, True])
def test_old_state_gets_a_durable_new_sixty_second_grace(shortage, pending):
    app, identity, advance, _jobs, queued, _master, _clock = shortage
    _restart(shortage, legacy=True, pending=pending)
    saved = app.data_manager.load_current_state()['current_set_info']
    assert saved.get('shortage_confirmed_at')
    advance(59)
    assert app.package_outbox.get_by_set_id(identity) is None
    advance(1)
    assert app.package_outbox.get_by_set_id(identity) is not None and queued == [identity]


def test_old_state_second_restart_keeps_the_saved_grace(shortage):
    app, identity, advance, _jobs, queued, _master, _clock = shortage
    _restart(shortage, legacy=True)
    advance(10)
    _restart(shortage)
    advance(49)
    assert app.package_outbox.get_by_set_id(identity) is None
    advance(1)
    assert app.package_outbox.get_by_set_id(identity) is not None and queued == [identity]


def test_restart_pending_completion_finishes_before_expired_release(shortage):
    app, identity, _advance, _jobs, queued, _master, _clock = shortage
    restarted = _restart(shortage, age=61, pending=True)
    if restarted.blocks:
        raise restarted.blocks[0]
    assert app.package_outbox.get_by_set_id(identity) is not None
    assert len(completions(app)) == 1 and queued == [identity] and not restarted.blocks


def test_failed_timer_release_retries_the_original_decision(shortage, monkeypatch):
    app, identity, advance, _jobs, queued, _master, _clock = shortage
    save = app.data_manager.save_current_state
    failures = []

    def fail_once(state):
        if state['current_set_info'].get('shortage_release_pending') and not failures:
            failures.append(True)
            return False
        return save(state)

    monkeypatch.setattr(app.data_manager, 'save_current_state', fail_once)
    advance(60)
    assert app.package_outbox.get_by_set_id(identity) is None and len(app.blocks) == 1
    advance(1)
    assert app.package_outbox.get_by_set_id(identity) is not None and queued == [identity]
    assert len(completions(app)) == 1


def test_failed_shortage_undo_never_releases_a_cancelled_completion(shortage, monkeypatch):
    from tests.test_legacy_label_transition import _events
    app, identity, advance, _jobs, queued, _master, _clock = shortage
    save = app.data_manager.save_current_state

    def fail_undo_checkpoint(state):
        current = state['current_set_info']
        return False if str(current.get('completion_identity') or '').endswith('-undo') else save(state)

    monkeypatch.setattr(app.data_manager, 'save_current_state', fail_undo_checkpoint)
    advance(59)
    scan(app, 'KMCMD:UNDO')
    assert len(app.blocks) == 1
    assert app.data_manager.flush(timeout=5)
    assert len(_events(app.save_directory, app.Events.TRAY_COMPLETION_CANCELLED)) == 1
    advance(61)
    assert queued == [] and app.package_outbox.get_by_set_id(identity) is None


def test_live_timer_uses_monotonic_clock_after_wall_clock_jumps(shortage, monkeypatch):
    app, identity, advance, _jobs, queued, _master, _clock = shortage
    original = label_module.time.time
    monkeypatch.setattr(label_module.time, 'time', lambda: original() + 3600)
    advance(59)
    assert app.package_outbox.get_by_set_id(identity) is None
    monkeypatch.setattr(label_module.time, 'time', lambda: original() - 3600)
    advance(1)
    assert app.package_outbox.get_by_set_id(identity) is not None and queued == [identity]

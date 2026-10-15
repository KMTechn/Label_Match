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


@pytest.fixture(autouse=True)
def _single_member_work_group(monkeypatch):
    # The timeout contract follows a valid complete small group, not one
    # sampled product from a larger group that would overstate shipment.
    from tests import test_deferred_lease_clock_retry as lease_tests
    from tests import test_package_logistics as source_tests

    monkeypatch.setattr(source_tests, "UNITS", source_tests.UNITS[:2])
    monkeypatch.setattr(source_tests, "BARCODES", source_tests.BARCODES[:2])
    original = source_tests._work_group_response
    monkeypatch.setattr(lease_tests, "_work_group_response", lambda **kwargs:
        original(split=True, split_size=1))


@pytest.fixture
def shortage(central, scanner_clock, monkeypatch, request):
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
    app._test_confirmation_bytes = __import__("pathlib").Path(app._package_current_state_path()).read_bytes()
    scan(app, master)
    root.run_until(lambda: not app.ui_lane.is_busy(), timeout=5)
    assert len(completions(app)) == 0 and not app.blocks and not dialogs
    assert app._partial_submit_confirm_state() is not None
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


def test_confirmation_and_continue_write_no_decision(shortage):
    from label_screen_bridge import LabelMatchScreenProcessor
    from types import SimpleNamespace
    app, identity, _advance, _jobs, queued, _master, _clock = shortage
    before = app._test_confirmation_bytes
    current = copy.deepcopy(app.current_set_info)
    assert app._partial_submit_confirm_state() == {
        "kind": "partial_submit", "total": 4, "done": 1, "short": 3,
    }
    assert "_partial_submit_confirmation" not in app.data_manager.load_current_state()
    assert LabelMatchScreenProcessor._command_partial_continue(SimpleNamespace(app=app), {}) == {"ok": True}
    assert app._partial_submit_confirm_state() is None
    assert app.current_set_info == current
    assert __import__("pathlib").Path(app._package_current_state_path()).read_bytes() == before
    assert not completions(app) and not queued and not app.package_outbox.get_by_set_id(identity)


def test_confirmation_stays_open_without_starting_a_completion_timer(shortage):
    app, identity, advance, _jobs, queued, _master, _clock = shortage
    before = copy.deepcopy(app.current_set_info)
    advance(601)
    assert app._partial_submit_confirm_state() is not None
    assert app.current_set_info == before
    assert not app.ui_lane.is_busy()
    assert not completions(app) and not queued and not app.package_outbox.get_by_set_id(identity)
    assert app._submit_partial_scanner()
    assert queued == [identity]


@pytest.mark.parametrize("answer", ["label", "enter", "command"])
def test_acceptance_releases_immediately_once(shortage, answer):
    from label_screen_bridge import LabelMatchScreenProcessor
    from types import SimpleNamespace
    app, identity, advance, jobs, queued, master, _clock = shortage
    if answer == "command":
        assert LabelMatchScreenProcessor._command_partial_submit(SimpleNamespace(app=app), {})["ok"]
    else:
        scan(app, master if answer == "label" else "")
    assert len(completions(app)) == 1
    row = app.package_outbox.get_by_set_id(identity)
    assert row and row["local_completion_committed"]
    assert queued == [identity]
    key, draft = row["idempotency_key"], row["draft_json"]
    assert not app._partial_submit_confirm_state()
    assert not app.__dict__.get("_scanner_shortage_undo")
    assert "_shortage_confirmation_deadline" not in app.__dict__
    assert app._arm_shortage_confirmation_timeout()
    advance(601)
    assert queued == [identity]
    same = app.package_outbox.get_by_set_id(identity)
    assert (same["idempotency_key"], same["draft_json"]) == (key, draft)


def test_escape_and_product_scan_continue_normal_work(shortage, monkeypatch):
    app, _identity, _advance, _jobs, queued, master, _clock = shortage
    app._handle_workflow_escape()
    assert app._partial_submit_confirm_state() is None
    assert app._request_scanner_shortage()
    seen = []
    process = app.process_input
    def observe(*args, **kwargs):
        assert app._partial_submit_confirm_state() is None
        seen.append(True)
        return process(*args, **kwargs)
    monkeypatch.setattr(app, "process_input", observe)
    scan(app, app.current_set_info["raw"][-1])
    assert seen == [True]
    assert not completions(app) and not queued


def test_stale_confirmation_cannot_complete_a_changed_set(shortage):
    app, _identity, _advance, _jobs, queued, _master, _clock = shortage
    app.current_set_info["raw"].append("changed-product")
    assert app._submit_partial_scanner() is False
    assert app._partial_submit_confirm_state() is None
    assert not completions(app) and not queued


@pytest.mark.parametrize("legacy,pending", [(False, False), (True, False), (False, True)])
def test_restart_releases_the_same_durable_decision_without_grace(shortage, monkeypatch, legacy, pending):
    app, identity, _advance, _jobs, queued, _master, _clock = shortage
    release = type(app)._release_shortage_confirmation
    monkeypatch.setattr(app, "_release_shortage_confirmation", lambda: False)
    assert app._submit_partial_scanner() is False
    assert len(completions(app)) == 1
    saved = copy.deepcopy(app.data_manager.load_current_state()["current_set_info"]["shortage_undo_snapshot"])
    monkeypatch.delattr(app, "_release_shortage_confirmation")
    restarted = _restart(shortage, age=10, legacy=legacy, pending=pending)
    row = restarted.package_outbox.get_by_set_id(identity)
    assert row and row["local_completion_committed"]
    assert queued == [identity]
    import json
    draft = json.loads(row["draft_json"])
    assert draft["sample_barcodes"] == saved["raw"][1:]
    assert restarted._arm_shortage_confirmation_timeout()
    assert queued == [identity]
    assert not restarted.__dict__.get("_scanner_shortage_undo")


def test_release_checkpoint_failure_retries_one_outbox_and_identical_bytes(shortage, monkeypatch):
    app, identity, _advance, _jobs, _queued, _master, _clock = shortage
    save = app.data_manager.save_current_state
    failed = []
    def interrupt(value):
        current = value.get("current_set_info") or {}
        if not failed and app.package_outbox.get_by_set_id(identity) and not current.get("shortage_undo_snapshot"):
            failed.append(True)
            return False
        return save(value)
    monkeypatch.setattr(app.data_manager, "save_current_state", interrupt)
    assert app._submit_partial_scanner() is False
    first = app.package_outbox.get_by_set_id(identity)
    assert first and failed == [True]
    assert app._arm_shortage_confirmation_timeout()
    second = app.package_outbox.get_by_set_id(identity)
    assert (second["idempotency_key"], second["draft_json"]) == (first["idempotency_key"], first["draft_json"])
    assert len(completions(app)) == 1
    assert not app.__dict__.get("_scanner_shortage_undo")


def test_completed_partial_submit_cannot_be_undone(shortage):
    app, identity, _advance, _jobs, queued, _master, _clock = shortage
    assert app._submit_partial_scanner()
    before = app.package_outbox.get_by_set_id(identity)["draft_json"]
    scan(app, "KMCMD:UNDO")
    assert len(completions(app)) == 1 and queued == [identity]
    assert app.package_outbox.get_by_set_id(identity)["draft_json"] == before

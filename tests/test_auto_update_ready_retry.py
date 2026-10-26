"""READY window of 600 seconds, a bounded second chance after a READY timeout,
and a bounded pile of update-preparation failure files."""
import json
import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest

import auto_update as updates
import user_relay
from tests.test_auto_update_ready_window import _policy, _transition
from tests.test_first_setup_owner_readback_mutex import _system_confirmation_case

OLD, NEW = "a" * 40, "b" * 40
BASE = datetime(2026, 10, 1, 4, tzinfo=timezone.utc)


def at(hours):
    return (BASE + timedelta(hours=hours)).isoformat()


def trial_state(**extra):
    state = updates.prepare_transition(
        previous_sha=OLD, candidate_sha=NEW, packet_sha256="c" * 64, forward_pin="d" * 64,
        rollback_pin="e" * 64, transition_id="2" * 32, nonce="1" * 32, now=at(0), **extra)
    return updates.apply_transition(state, writers_absent=True, packet_verified=True,
                                    durable_prepared=True, now=at(0))


def test_ready_window_is_ten_minutes_and_the_bootstrap_cap_outlasts_it():
    assert updates.READY_WINDOW_SECONDS == 600
    assert updates.BOOTSTRAP_OBSERVATION_CAP_SECONDS >= updates.READY_WINDOW_SECONDS + 240
    state = {"phase": "APPLIED_PENDING_READY", "applied_at": at(0)}
    deadline = updates.bootstrap_supervision_deadline(state, started=0, now=0, utc_now=at(0))
    assert deadline == updates.READY_WINDOW_SECONDS + 15


def test_a_confirmed_rejection_stays_permanent_at_once():
    state = updates.request_rollback(trial_state(), reason="candidate startup failed or readiness timed out")
    assert state["failed_shas"] == [NEW] and "retry_shas" not in state


def test_a_transient_failure_is_not_blacklisted_until_the_third_chance_inside_a_day():
    state = trial_state()
    first = updates.request_rollback(state, reason="timeout", transient=True, now=at(0))
    assert first["failed_shas"] == []
    assert first["retry_shas"][NEW] == {"attempts": 1, "first_at": at(0), "last_at": at(0)}
    second = updates.request_rollback({**state, "retry_shas": first["retry_shas"]}, reason="timeout",
                                      transient=True, now=at(1))
    assert second["failed_shas"] == [] and second["retry_shas"][NEW]["attempts"] == 2
    third = updates.request_rollback({**state, "retry_shas": second["retry_shas"]}, reason="timeout",
                                     transient=True, now=at(2))
    assert third["failed_shas"] == [NEW] and third["retry_shas"][NEW]["attempts"] == 3


def test_chances_start_over_after_a_day_and_bad_records_are_ignored():
    state = trial_state()
    old = {NEW: {"attempts": 2, "first_at": at(0), "last_at": at(1)},
           "not-a-sha": {"attempts": 5, "first_at": at(0), "last_at": at(0)}}
    later = updates.request_rollback({**state, "retry_shas": old}, reason="timeout", transient=True, now=at(25))
    assert later["failed_shas"] == []
    assert later["retry_shas"] == {NEW: {"attempts": 1, "first_at": at(25), "last_at": at(25)}}


def test_the_next_transition_keeps_the_retry_record():
    retry = {NEW: {"attempts": 1, "first_at": at(0), "last_at": at(0)}}
    assert trial_state(retry_shas=retry)["retry_shas"] == retry
    assert "retry_shas" not in trial_state()


def _prepared_case(tmp_path, monkeypatch, *, retry, ready_at):
    """Drive the real SYSTEM READY wait; the candidate becomes READY at ``ready_at`` or never."""
    case = _system_confirmation_case(tmp_path, monkeypatch, [])
    state = _transition("PREPARED")
    if retry is not None:
        state["retry_shas"] = retry
    monkeypatch.setattr(updates, "load_policy", lambda *_: _policy(tmp_path))
    monkeypatch.setattr(updates, "optional_record", lambda p: dict(state) if p.name == "transition.json" else None)
    monkeypatch.setattr(updates, "read_json", lambda p: dict(schema=updates.SCHEMA, sha="b" * 40,
                        transition_id=state["transition_id"]) if p.name == "active.json" else dict(state))
    monkeypatch.setattr(updates, "write_json_durable", lambda p, value: state.update(value) if p.name == "transition.json" else None)
    monkeypatch.setattr(updates, "defer_stopped_relay", lambda *_: False)
    monkeypatch.setattr(updates, "authorize_prepared_replay", lambda *_: None)
    monkeypatch.setattr(updates, "version_bundle", lambda *_: tmp_path / "candidate")
    monkeypatch.setattr(updates, "apply_transition", lambda current, **fields:
                        {**current, "phase": "APPLIED_PENDING_READY", "applied_at": fields["now"]})
    monkeypatch.setattr(updates, "persist_switch", lambda _c, value, _s: state.update(value))
    monkeypatch.setattr(updates, "now_utc", lambda: at(updates.time.monotonic() / 3600))
    monkeypatch.setattr(updates, "settle_trial_failure", lambda *_: {"retained_candidate": True})

    def ready(_c, _p, current, *_a, deadline):
        confirmed = ready_at is not None and ready_at <= updates.time.monotonic() < deadline
        if confirmed:
            state["phase"] = "CONFIRMED"
        return dict(state), confirmed, False

    monkeypatch.setattr(updates, "observe_trial_ready", ready)
    return case, state


def test_candidate_ready_at_300_seconds_confirms_instead_of_rolling_back(tmp_path, monkeypatch):
    case, state = _prepared_case(tmp_path, monkeypatch, retry=None, ready_at=300)
    assert case.run() == 0
    assert state["phase"] == "CONFIRMED" and state["failed_shas"] == []


@pytest.mark.parametrize("attempts,permanent", [(0, False), (1, False), (2, True)])
def test_a_ready_timeout_blacklists_only_after_the_chances_are_used(tmp_path, monkeypatch, attempts, permanent):
    retry = {NEW: {"attempts": attempts, "first_at": at(0), "last_at": at(0)}} if attempts else None
    case, state = _prepared_case(tmp_path, monkeypatch, retry=retry, ready_at=None)
    assert case.run() == 20
    assert state["phase"] == "ROLLBACK_PENDING"
    assert state["retry_shas"][NEW]["attempts"] == attempts + 1
    assert (state["failed_shas"] == [NEW]) is permanent


def _failure_files(logs, count):
    logs.mkdir(parents=True, exist_ok=True)
    names = []
    for index in range(count):
        path = logs / ("update-preparation-failure-" + uuid.uuid4().hex + ".json")
        path.write_text("{}", encoding="utf-8")
        os.utime(path, (1_000_000 + index, 1_000_000 + index))
        names.append(path.name)
    return names


def test_update_preparation_failure_files_keep_only_the_newest_twenty(tmp_path):
    logs = tmp_path / "logs"
    names = _failure_files(logs, 25)
    other = [logs / ("cycle-failure-" + "0" * 32 + ".json"), logs / "direct_sync_relay.jsonl",
             logs / ("update-preparation-failure-" + "z" * 32 + ".json")]
    for path in other:
        path.write_text("{}", encoding="utf-8")
    user_relay._prune_update_preparation_failures(logs)
    kept = {path.name for path in logs.iterdir()}
    assert set(names[5:]) <= kept and not (set(names[:5]) & kept)
    assert all(path.name in kept for path in other)


def test_each_failed_update_preparation_prunes_after_it_writes(tmp_path, monkeypatch):
    root = tmp_path / "direct-sync"
    _failure_files(root / "logs", 25)
    monkeypatch.setattr(user_relay, "run_session_direct_sync_once", lambda **_: {"status": "PASS"})
    user_relay._runtime_cycle(app_root=tmp_path, direct_sync_root=root, scan_source_dir=tmp_path,
                              update_preparation=ValueError("synthetic"))
    count = len([p for p in (root / "logs").iterdir() if p.name.startswith("update-preparation-failure-")])
    assert count == user_relay.UPDATE_PREPARATION_FAILURE_KEEP


def test_the_retry_record_survives_the_real_journal_writer(tmp_path):
    control = tmp_path / "control"
    control.mkdir()
    state = updates.request_rollback(trial_state(), reason="candidate startup failed or readiness timed out",
                                     transient=True, now=at(0))
    updates.write_json_durable(control / "transition.json", state)
    assert updates.read_json(control / "transition.json")["retry_shas"] == state["retry_shas"]

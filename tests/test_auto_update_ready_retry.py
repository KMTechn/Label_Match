"""READY window of 900 seconds, a bounded second chance after a READY timeout,
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


def test_ready_window_is_fifteen_minutes_and_the_bootstrap_cap_outlasts_it():
    assert updates.READY_WINDOW_SECONDS == 900
    assert updates.BOOTSTRAP_OBSERVATION_CAP_SECONDS == updates.READY_WINDOW_SECONDS + 240 == 1140
    state = {"phase": "APPLIED_PENDING_READY", "applied_at": at(0)}
    deadline = updates.bootstrap_supervision_deadline(state, started=0, now=0, utc_now=at(0))
    assert deadline == updates.READY_WINDOW_SECONDS + 15


def test_a_confirmed_rejection_stays_permanent_at_once():
    state = updates.request_rollback(trial_state(), reason="candidate startup failed or readiness timed out")
    assert state["failed_shas"] == [NEW] and "retry_shas" not in state


def test_transient_failures_never_blacklist_even_after_three_chances():
    state = trial_state()
    first = updates.request_rollback(state, reason="timeout", transient=True, now=at(0))
    assert first["failed_shas"] == []
    assert first["retry_shas"][NEW] == {"attempts": 1, "first_at": at(0), "last_at": at(0)}
    second = updates.request_rollback({**state, "retry_shas": first["retry_shas"]}, reason="timeout",
                                      transient=True, now=at(1))
    assert second["failed_shas"] == [] and second["retry_shas"][NEW]["attempts"] == 2
    third = updates.request_rollback({**state, "retry_shas": second["retry_shas"]}, reason="timeout",
                                     transient=True, now=at(2))
    assert third["failed_shas"] == [] and third["retry_shas"][NEW]["attempts"] == 3


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
def test_ready_timeout_exhaustion_never_blacklists_the_candidate(tmp_path, monkeypatch, attempts, permanent):
    retry = {NEW: {"attempts": attempts, "first_at": at(0), "last_at": at(0)}} if attempts else None
    case, state = _prepared_case(tmp_path, monkeypatch, retry=retry, ready_at=None)
    assert case.run() == 20
    assert state["phase"] == "ROLLBACK_PENDING"
    assert state["retry_shas"][NEW]["attempts"] == attempts + 1
    assert state["failed_shas"] == []
    assert updates._retry_review_required(state) is permanent


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


def _preparation_case(tmp_path, monkeypatch, error):
    from contextlib import nullcontext
    from types import SimpleNamespace
    control, stage = tmp_path / "control", tmp_path / "stage"
    policy = {"operator_sid": "test-sid", "staging_root": str(stage), "feed_url": "https://invalid.test/feed"}
    files = {control / "active.json": {"sha": OLD}, control / "policy.json": policy,
             stage / "prepared.json": {"source_commit": NEW}}
    clock = [0]
    monkeypatch.setattr(updates, "now_utc", lambda: at(clock[0] / 3600))
    monkeypatch.setattr(updates, "read_json", lambda path: files[path].copy())
    monkeypatch.setattr(updates, "optional_record", lambda path: files.get(path, {}).copy() or None)
    monkeypatch.setattr(updates, "write_json_durable", lambda path, value: files.__setitem__(path, dict(value)))
    monkeypatch.setattr(updates, "fixed_helper", lambda *a, **k: None)
    monkeypatch.setattr(updates, "update_lock", lambda *a, **k: nullcontext())
    monkeypatch.setattr(updates, "complete_apply_claim", lambda *a: None)
    if hasattr(updates, "load_policy"):
        monkeypatch.setattr(updates, "load_policy", lambda *a: policy)
        monkeypatch.setattr(updates, "prepared_marker_sha256", lambda *a: "c" * 64)
    else:
        monkeypatch.setattr(updates, "prepared_failure_identity", lambda *a: "c" * 64)

    def fail(*args):
        raise error

    monkeypatch.setattr(updates, "_apply_prepared", fail)
    return SimpleNamespace(control=control, policy=policy, files=files, clock=clock,
                           state=lambda: files[control / "transition.json"],
                           run=lambda: updates.apply_prepared(control))


def _native_error(code):
    error = OSError("simulated native I/O failure")
    error.winerror = code
    return error


@pytest.mark.parametrize("error", [TimeoutError(), PermissionError(), ConnectionResetError(),
    _native_error(5), _native_error(32), _native_error(33), _native_error(1117),
    RuntimeError("unknown failure"), ValueError("unknown failure"), OSError("unknown failure")])
def test_transient_preparation_gets_a_durable_retry_after_900_seconds(tmp_path, monkeypatch, error):
    case = _preparation_case(tmp_path, monkeypatch, error)
    assert case.run() == 0
    state = case.state()
    assert NEW not in state.get("failed_shas", [])
    assert state["retry_shas"][NEW]["attempts"] == 1
    assert not updates.prepared_pending(case.control, case.policy, state)
    case.clock[0] = 899
    assert not updates.prepared_pending(case.control, case.policy, state)
    case.clock[0] = 900
    assert updates.prepared_pending(case.control, case.policy, state)
    case.clock[0] = 1800
    assert case.run() == 0
    assert case.state()["retry_shas"][NEW]["attempts"] == 2
    case.clock[0] = 2700
    assert case.run() == 0
    assert case.state().get("failed_shas", []) == []
    case.clock[0] = 30 * 3600
    assert updates.prepared_pending(case.control, case.policy, case.state())


def _confirmed_error():
    error = ValueError("manifest verification failed")
    error._preparation_rejection = True
    return error


@pytest.mark.parametrize("error", [_confirmed_error(), _native_error(112)])
def test_confirmed_preparation_failure_still_blocks_immediately(tmp_path, monkeypatch, error):
    case = _preparation_case(tmp_path, monkeypatch, error)
    assert case.run() == 0
    assert not case.state().get("retry_shas")
    case.clock[0] = 3600
    assert not updates.prepared_pending(case.control, case.policy, case.state())


def test_preparation_retry_window_resets_after_24_hours(tmp_path, monkeypatch):
    case = _preparation_case(tmp_path, monkeypatch, TimeoutError())
    case.run()
    case.clock[0] = 24 * 3600
    assert updates.prepared_pending(case.control, case.policy, case.state())
    case.run()
    assert case.state()["retry_shas"][NEW] == {"attempts": 1, "first_at": at(24), "last_at": at(24)}


def test_a_preparation_error_after_an_uncertain_pointer_never_opens_a_retry(tmp_path, monkeypatch):
    case = _preparation_case(tmp_path, monkeypatch, TimeoutError())
    case.files[case.control / "transition.json"] = {"phase": "APPLIED_PENDING_READY"}
    assert case.run() == 20
    assert case.state()["phase"] == "UNKNOWN"
    assert not case.state().get("retry_shas")


def test_observe_timeout_is_classified_before_child_output_is_redacted(monkeypatch, tmp_path):
    import subprocess
    from types import SimpleNamespace
    completed = SimpleNamespace(returncode=1, stdout="", stderr="STARTUP_QUERY_TIMEOUT_UNKNOWN")
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: completed)
    monkeypatch.setattr(updates, "system_executable", lambda *a: tmp_path / "powershell.exe")
    try:
        updates._run_fixed_helper(tmp_path / "control", "Observe")
    except RuntimeError as error:
        assert updates._transient_preparation_error(error)
    else:
        pytest.fail("Observe must reject the timed-out query")


def test_system_preparation_entry_obeys_the_saved_retry_interval(tmp_path, monkeypatch):
    from contextlib import nullcontext
    case = _preparation_case(tmp_path, monkeypatch, TimeoutError())
    case.run()
    case.policy["operator_local_app_data"] = str(tmp_path / "profile")
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "profile"))
    lock = "_initial_ready_lock" if hasattr(updates, "_initial_ready_lock") else "_ready_window_update_lock"
    monkeypatch.setattr(updates, lock, lambda *a, **k: nullcontext())
    def unexpected_claim():
        pytest.fail("cooldown must return before starting a new apply")
    monkeypatch.setattr(updates, "new_apply_claim", unexpected_claim)
    case.clock[0] = 899
    assert updates._apply_prepared_once(case.control) == 0
    assert case.state()["retry_shas"][NEW]["attempts"] == 1


@pytest.mark.parametrize("kind", ["network", "query_timeout", "disk_io", "disk_full"])
def test_only_the_approved_io_errors_are_temporary(kind):
    import errno
    import subprocess
    from requests.exceptions import ConnectionError as RequestConnectionError
    errors = {"network": RequestConnectionError(), "query_timeout": subprocess.TimeoutExpired("Observe", 20),
              "disk_io": OSError(errno.EIO, "simulated"), "disk_full": OSError(errno.ENOSPC, "simulated")}
    assert updates._transient_preparation_error(errors[kind]) is (kind != "disk_full")


def test_an_unreadable_prepared_identity_keeps_its_saved_wait_after_restart(tmp_path, monkeypatch):
    real_write, real_read = updates.write_json_durable, updates.read_json
    case = _preparation_case(tmp_path, monkeypatch, PermissionError())
    readable = [False]
    original_read, original_optional = updates.read_json, updates.optional_record
    def read(path):
        if path.name == "prepared.json" and not readable[0]:
            raise PermissionError()
        return original_read(path)
    def optional(path):
        if path.name == "prepared.json" and not readable[0]:
            raise PermissionError()
        return original_optional(path)
    monkeypatch.setattr(updates, "read_json", read)
    monkeypatch.setattr(updates, "optional_record", optional)
    if hasattr(updates, "prepared_marker_sha256"):
        monkeypatch.setattr(updates, "prepared_marker_sha256", lambda *a: "d" * 64 if readable[0] else None)
    else:
        monkeypatch.setattr(updates, "prepared_failure_identity", lambda *a: ("d" if readable[0] else "c") * 64)
    assert case.run() == 0
    assert case.state()["preparation_retry_pending"] is True
    assert not case.state().get("retry_shas") and not case.state().get("failed_shas")
    assert not updates.prepared_pending(case.control, case.policy, case.state())
    # Read through the real protected JSON writer, then resume with that record.
    journal = tmp_path / "persisted" / "transition.json"
    journal.parent.mkdir()
    real_write(journal, case.state())
    case.files[case.control / "transition.json"] = real_read(journal)
    readable[0] = True
    case.clock[0] = 899
    assert not updates.prepared_pending(case.control, case.policy, case.state())
    case.clock[0] = 900
    assert updates.prepared_pending(case.control, case.policy, case.state())
    case.run()
    assert not case.state().get("preparation_retry_pending")
    assert case.state()["retry_shas"][NEW]["attempts"] == 1


def test_exhaustion_reports_the_approved_notice_and_keeps_the_current_app(tmp_path, monkeypatch, capsys):
    case = _preparation_case(tmp_path, monkeypatch, RuntimeError("unknown failure"))
    # A prior confirmed candidate must not hide the current preparation's budget.
    case.files[case.control / "transition.json"] = {"schema": updates.SCHEMA, "phase": "CONFIRMED", "candidate_sha": OLD}
    for second in (0, 900, 1800):
        case.clock[0] = second
        assert case.run() == 0
    state = case.state()
    assert NEW not in state.get("failed_shas", [])
    assert state["retry_shas"][NEW]["attempts"] == 3
    assert not updates.prepared_pending(case.control, case.policy, state)
    launched = []
    monkeypatch.setattr(updates, "launch_version", lambda _c, sha, *a, **k: (launched.append(sha), 123)[1])
    if hasattr(updates, "_bootstrap_relay_readback"):
        monkeypatch.setattr(updates, "_bootstrap_relay_readback", lambda *a, **k: True)
    assert updates.start_current(case.control, case.policy, state, {"sha": OLD}, [], code="UPDATE_PREPARATION_FAILED") == 0
    report = case.files[__import__("pathlib").Path(case.policy["staging_root"]) / "startup-support.json"]
    assert report["reason_code"] == "UPDATE_RETRY_REVIEW_REQUIRED"
    assert report["guidance"] == updates.RETRY_REVIEW_MESSAGE
    import json
    emitted = [json.loads(line.split("=", 1)[-1]) for line in capsys.readouterr().out.splitlines()]
    assert any(record.get("guidance") == updates.RETRY_REVIEW_MESSAGE for record in emitted)
    assert launched == [OLD]
    case.clock[0] = 24 * 3600
    assert updates.prepared_pending(case.control, case.policy, state)
    assert not updates._retry_review_required(state)


def test_a_rolled_back_ready_timeout_has_the_same_review_budget(monkeypatch):
    monkeypatch.setattr(updates, "now_utc", lambda: at(2))
    state = trial_state()
    for hour in (0, 1, 2):
        pending = trial_state(retry_shas=state.get("retry_shas"))
        state = updates.request_rollback(pending, reason="timeout", transient=True, now=at(hour))
    state["phase"] = "ROLLED_BACK"
    assert state["failed_shas"] == []
    assert updates._retry_review_required(state)
    monkeypatch.setattr(updates, "now_utc", lambda: at(24))
    assert not updates._retry_review_required(state)


@pytest.mark.parametrize("marker,temporary", [("STARTUP_UPDATE_FILE_PIN_DIFFERS", False),
                                             ("STARTUP_UPDATE_FAILURE", True)])
def test_fixed_helper_distinguishes_a_proved_rejection_from_an_unknown_error(tmp_path, monkeypatch, marker, temporary):
    import subprocess
    from types import SimpleNamespace
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: SimpleNamespace(returncode=1, stdout="", stderr=marker))
    monkeypatch.setattr(updates, "system_executable", lambda *a: tmp_path / "powershell.exe")
    try:
        updates._run_fixed_helper(tmp_path / "control", "Verify")
    except RuntimeError as error:
        assert updates._transient_preparation_error(error) is temporary
    else:
        pytest.fail("the failed Verify helper must still reject this invocation")



@pytest.mark.parametrize("replay", [False, True])
def test_an_authenticated_manifest_pair_rejection_is_permanent_in_both_preparation_paths(tmp_path, monkeypatch, replay):
    from contextlib import nullcontext
    from pathlib import Path
    import Label_Match
    case = _preparation_case(tmp_path, monkeypatch, RuntimeError())
    case.policy.update(channel="stable", operator_local_app_data=str(tmp_path / "profile"))
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "profile"))
    current = {"source_commit": OLD, "version": "v2.0.124"}
    candidate = {"source_commit": NEW, "version": "v2.0.125"}
    manifest = {**candidate, "auto_update": {}}
    case.files[case.control.parent / "current/app/update-manifest-key-config.json"] = {"manifest_public_key": None}
    if replay:
        case.files[case.control / "transition.json"] = {
            "schema": updates.SCHEMA, "phase": "PREPARED", "previous_sha": OLD, "candidate_sha": NEW,
            "prepared_packet_root": str(case.control / ("packet-" + "1" * 32)), "packet_sha256": "c" * 64}
    monkeypatch.setattr(updates, "_apply_prepared", updates._apply_prepared_once)
    monkeypatch.setattr(updates, "_initial_ready_lock", lambda *a, **k: nullcontext())
    monkeypatch.setattr(updates, "new_apply_claim", lambda: {})
    monkeypatch.setattr(updates, "defer_stopped_relay", lambda *a: False)
    monkeypatch.setattr(updates, "process_snapshot", lambda *a: [])
    monkeypatch.setattr(updates, "version_bundle", lambda control, sha: control / "versions" / sha)
    monkeypatch.setattr(updates, "version_manifest", lambda path: dict(current if path.name == OLD else candidate))
    monkeypatch.setattr(updates, "create_protected_stage", lambda *a: None)
    monkeypatch.setattr(updates, "copy_prepared_file", lambda *a, **k: None)
    monkeypatch.setattr(updates, "verify_prepared_packet", lambda *a, **k: (manifest, {"sha256": "c" * 64, "archive": {}}))
    monkeypatch.setattr(updates, "extract_prepared_code", lambda *a: None)
    monkeypatch.setattr(Label_Match, "_sha256_file", lambda *a: "f" * 64)
    assert case.run() == 0
    assert case.state()["phase"] == "DEFERRED"
    assert case.files[case.control / "active.json"] == {"sha": OLD}
    assert case.state().get("failed_shas") == [NEW]
    assert not case.state().get("retry_shas")
    assert case.state()["failed_prepared_sha256"] == "c" * 64



def test_a_new_day_budget_takes_precedence_over_a_recent_attempt_interval(tmp_path, monkeypatch):
    case = _preparation_case(tmp_path, monkeypatch, TimeoutError())
    last = at(24 - 1 / 60)
    state = {"schema": updates.SCHEMA, "phase": "DEFERRED", "reason_code": "UPDATE_PREPARATION_FAILED",
             "deferred_at": last, "retry_shas": {NEW: {"attempts": 3, "first_at": at(0), "last_at": last}}}
    case.files[case.control / "transition.json"] = state
    case.clock[0] = 24 * 3600 - 1
    assert not updates.prepared_pending(case.control, case.policy, state)
    case.clock[0] = 24 * 3600
    assert updates.prepared_pending(case.control, case.policy, state)
    assert case.run() == 0
    assert case.state().get("failed_shas", []) == []
    assert case.state()["retry_shas"][NEW] == {"attempts": 1, "first_at": at(24), "last_at": at(24)}



@pytest.mark.parametrize("ready_at", [750, None], ids=["relay-running-in-window", "ready-timeout"])
def test_system_decides_the_same_pending_window_after_bootstrap_preparation_expires(ready_at):
    from contextlib import contextmanager
    from pathlib import Path
    from tests.recovery_memory_world import MemoryPath, World
    source = Path(updates.__file__).read_text(encoding="utf-8-sig")
    w = World("LM-next-decider-" + str(ready_at), source,
              phase="APPLIED_PENDING_READY", writer=None, claim="LIVE")
    w.clock = 0.0
    w.state.update(applied_at=w.at(0), failed_shas=[])
    w.write(w.control / "active.json", {"sha": w.candidate})
    w.policy.update(schema="label-match-startup-update-policy-v1", settings_sha256="f" * 64,
                    feed_url="https://setup.test.invalid/feed", channel="stable", profile_auth=True,
                    apply_during_work=False, hiberboot_enabled=None, hiberboot_read_status="UNPROVEN")
    w.guard = "UNKNOWN"

    @contextmanager
    def busy(*args, **kwargs):
        raise RuntimeError("update mutex ownership is busy")
        yield

    def advance(seconds):
        w.clock = round(w.clock + seconds, 9)
        assert w.clock < 2000, "bounded memory-world clock exceeded"
    w.ns["update_lock"] = busy
    w.time.sleep = advance
    assert w.ns["_bootstrap"](MemoryPath("/memory/current/app"), [], role="gui") == 20
    assert 600 <= w.clock < 601 and not w.launches
    assert w.state["phase"] == "APPLIED_PENDING_READY"

    # Resume the actual SYSTEM implementation with the same durable journal.
    w.ns["update_lock"] = w.lock
    w.rows = [w.row("gui", sha=w.candidate, pid=101), w.row("relay", sha=w.candidate, pid=102)]
    w.files[str(w.stage / "trial.json")] = {"pid": 101, "role": "gui", "nonce": w.state["nonce"],
        "transition_id": w.state["transition_id"]}
    w.auto_ready = True
    def system_advance(seconds):
        advance(seconds)
        if ready_at is not None and w.clock >= ready_at and w.state["phase"] == "APPLIED_PENDING_READY":
            w.files[str(w.stage / "ready.json")] = {"pid": 101, "role": "gui", "sha": w.candidate,
                "nonce": w.state["nonce"], "transition_id": w.state["transition_id"],
                "ready_at": w.at(w.clock), **{flag: True for flag in w.ns["READY_CHECKS"]}}
        if w.state["phase"] in {"UNKNOWN", "ROLLBACK_PENDING"}:
            w.rows = []  # The ordinary cohort acknowledges the existing normal stop.
    w.time.sleep = system_advance
    assert w.ns["_apply_prepared"](w.control) == 0
    w.ns["complete_apply_claim"](w.control)
    if ready_at is not None:
        assert w.state["phase"] == "CONFIRMED" and w.active == w.candidate
        assert 750 <= w.clock < 900
        assert w.optional(MemoryPath("/memory/relay-status.json"))["status"] == "RUNNING"
        assert "write:transition.json:CONFIRMED" in w.events
    else:
        assert w.state["phase"] == "ROLLED_BACK" and w.active == w.previous
        assert w.state["failure_decision"]
        assert w.state["retry_shas"][w.candidate]["attempts"] == 1
        assert w.candidate not in w.state["failed_shas"]
        assert "write:transition.json:ROLLED_BACK" in w.events

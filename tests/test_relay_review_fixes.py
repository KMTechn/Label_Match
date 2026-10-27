"""Behavioral regressions for the relay watchdog independent review."""
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

import auto_update as updates
import label_match_product_host as host
import label_match_relay_entry as entry
import user_relay as relay
from startup_timing import StartupStopRequested


def test_bootstrap_rechecks_owner_after_successful_exit(monkeypatch):
    owner = {"process_id": 77, "updated_at": "now"}
    reads = iter([None, owner])
    result = relay._read_started_relay(
        SimpleNamespace(pid=42, poll=lambda: 0), lambda: next(reads),
        0.0, relay.time.monotonic() + 10, True, lambda _: None,
    )
    assert result["process_id"] == 77


def test_completed_bootstrap_missing_owner_waits_only_two_seconds(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(relay.time, "monotonic", lambda: clock[0])
    with pytest.raises(relay.UserRelayError) as caught:
        relay._read_started_relay(
            SimpleNamespace(pid=42, poll=lambda: 0), lambda: None, 2.0, 700.0, True,
            lambda seconds: clock.__setitem__(0, clock[0] + seconds),
        )
    assert caught.value.cause_code == "RELAY_EXITED_BEFORE_RUNNING"
    assert caught.value.exit_code == 0
    assert clock[0] == pytest.approx(104.0)


def test_bootstrap_grace_starts_on_first_zero_poll_and_is_not_renewed(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(relay.time, "monotonic", lambda: clock[0])
    child = SimpleNamespace(pid=42, poll=lambda: 0 if clock[0] >= 0.3 else None)
    with pytest.raises(relay.UserRelayError) as caught:
        relay._read_started_relay(
            child, lambda: None, 0.0, 10.0, True,
            lambda seconds: clock.__setitem__(0, clock[0] + seconds),
        )
    assert caught.value.cause_code == "RELAY_EXITED_BEFORE_RUNNING"
    assert clock[0] == pytest.approx(2.3)


@pytest.mark.parametrize("deadline", [0.25, 2.0, 2.5])
def test_bootstrap_grace_stays_within_parent_deadline(monkeypatch, deadline):
    clock = [0.0]
    monkeypatch.setattr(relay.time, "monotonic", lambda: clock[0])
    with pytest.raises(relay.UserRelayError) as caught:
        relay._read_started_relay(
            SimpleNamespace(pid=42, poll=lambda: 0), lambda: None, 0.0, deadline, True,
            lambda seconds: clock.__setitem__(0, clock[0] + seconds),
        )
    assert caught.value.cause_code == "RELAY_EXITED_BEFORE_RUNNING"
    assert clock[0] == pytest.approx(min(2.0, deadline))


def test_bootstrap_accepts_query_started_inside_grace(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(relay.time, "monotonic", lambda: clock[0])
    starts = []
    def late_owner():
        starts.append(clock[0])
        if clock[0] < 1.9:
            clock[0] = 1.9
            return None
        clock[0] = 2.6
        return {"process_id": 77, "updated_at": "now"}
    result = relay._read_started_relay(
        SimpleNamespace(pid=42, poll=lambda: 0), late_owner, 0.0, 10.0, True,
        lambda seconds: clock.__setitem__(0, clock[0] + seconds),
    )
    assert result["process_id"] == 77
    assert starts == [0.0, 1.9]
    assert clock[0] == 2.6


def test_bootstrap_rejects_owner_readback_after_parent_deadline(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(relay.time, "monotonic", lambda: clock[0])
    def late_owner():
        clock[0] = 10.0
        return {"process_id": 77, "updated_at": "now"}
    with pytest.raises(relay.UserRelayError) as caught:
        relay._read_started_relay(
            SimpleNamespace(pid=42, poll=lambda: 0), late_owner, 0.0, 10.0, True,
            lambda seconds: clock.__setitem__(0, clock[0] + seconds),
        )
    assert caught.value.cause_code == "RELAY_EXITED_BEFORE_RUNNING"


def test_bootstrap_does_not_start_another_query_after_slow_read_exhausts_grace(monkeypatch):
    clock = [0.0]
    reads = []
    monkeypatch.setattr(relay.time, "monotonic", lambda: clock[0])
    def slow_missing_owner():
        reads.append(clock[0])
        clock[0] += 3.0
        return None
    with pytest.raises(relay.UserRelayError) as caught:
        relay._read_started_relay(
            SimpleNamespace(pid=42, poll=lambda: 0), slow_missing_owner, 0.0, 10.0, True,
            lambda seconds: clock.__setitem__(0, clock[0] + seconds),
        )
    assert caught.value.cause_code == "RELAY_EXITED_BEFORE_RUNNING"
    assert reads == [0.0]


@pytest.mark.parametrize("failure", ["support", "print", "import", "traceback"])
def test_diagnostic_failure_preserves_successful_main(monkeypatch, tmp_path, failure):
    import builtins

    def fail(*args, **kwargs):
        raise OSError("diagnostic unavailable")

    if failure == "support":
        monkeypatch.setattr(updates, "support_record", fail)
    elif failure == "print":
        monkeypatch.setattr(builtins, "print", fail)
    elif failure == "traceback":
        monkeypatch.setattr(relay.traceback, "walk_tb", fail)
    else:
        original = builtins.__import__

        def guarded(name, *args, **kwargs):
            if name == "auto_update":
                fail()
            return original(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", guarded)

    def run(*args, **kwargs):
        relay._record_relay_reason("RELAY_STARTUP_HANDLED", error=ValueError())
        return 0

    monkeypatch.setattr(relay, "_run_user_relay", run)
    assert relay.main(["--app-root", str(tmp_path)]) == 0


@pytest.mark.parametrize("stop", [False, True])
def test_running_normal_completion_keeps_status_without_failure_copy(monkeypatch, tmp_path, stop):
    monkeypatch.setattr(updates, "control_root", lambda _: tmp_path / "control")
    monkeypatch.setattr(updates, "load_policy", lambda _: {"staging_root": str(tmp_path / "stage")})
    monkeypatch.setattr(relay, "writer_admission", lambda *a, **k: nullcontext())
    cycles = []

    def cycle():
        cycles.append(1)
        return {"status": "PASS"}

    token = relay._relay_main_running.set(False)
    try:
        report = relay.run_persistent_relay_loop(
            cycle, status_path=tmp_path / "status.json", max_cycles=1,
            stop_requested=lambda: stop and bool(cycles), metadata={"app_root": str(tmp_path)},
        )
    finally:
        relay._relay_main_running.reset(token)
    assert report["status"] == ("STOPPED" if stop else "COMPLETED")
    assert (tmp_path / "status.json").exists()
    assert not list((tmp_path / "stage").glob("failure-*.json"))


@pytest.mark.parametrize("running", [False, True])
def test_startup_handled_copy_only_before_running(monkeypatch, tmp_path, running):
    monkeypatch.setattr(updates, "control_root", lambda _: tmp_path / "control")
    monkeypatch.setattr(updates, "load_policy", lambda _: {"staging_root": str(tmp_path / "stage")})
    token = relay._relay_main_running.set(running)
    try:
        relay._record_relay_reason("RELAY_STARTUP_HANDLED", app_root=tmp_path)
    finally:
        relay._relay_main_running.reset(token)
    assert len(list((tmp_path / "stage").glob("failure-*.json"))) == int(not running)


@pytest.mark.parametrize("mode", ["user", "headless"])
def test_requested_startup_stop_returns_zero_without_failure_copy(monkeypatch, tmp_path, mode):
    records = []
    monkeypatch.setattr(relay, "_record_relay_reason", lambda *a, **k: records.append(a))

    def stop(*args, **kwargs):
        raise StartupStopRequested()

    if mode == "user":
        monkeypatch.setattr(relay, "_run_user_relay", stop)
        result = relay.main(["--app-root", str(tmp_path)])
    else:
        monkeypatch.setattr(entry, "APP_ROOT", tmp_path)
        monkeypatch.setattr(host, "dispatch_product_mode", stop)
        import current_user_onboarding as onboarding
        monkeypatch.setattr(onboarding, "_machine_worker_context", lambda _: None)
        result = entry.main([relay.USER_RELAY_MODE])
    assert result == 0
    assert records == []


@pytest.mark.parametrize("running", [False, True])
def test_headless_late_exception_uses_running_history(monkeypatch, tmp_path, running):
    import current_user_onboarding as onboarding
    monkeypatch.setattr(entry, "APP_ROOT", tmp_path)
    monkeypatch.setattr(onboarding, "_machine_worker_context", lambda _: None)
    monkeypatch.setattr(relay, "writer_admission", lambda *a, **k: nullcontext())
    records = []
    monkeypatch.setattr(relay, "_record_relay_reason", lambda code, **k: records.append(code))

    def run(*args, **kwargs):
        if running:
            relay.run_persistent_relay_loop(
                lambda: {"status": "PASS"}, status_path=tmp_path / "status.json", max_cycles=1,
            )
        return 0

    def dispatch(arguments):
        relay.main(["--app-root", str(tmp_path)])
        return None  # A dispatch error after the inner main has reset its context.

    monkeypatch.setattr(relay, "_run_user_relay", run)
    monkeypatch.setattr(host, "dispatch_product_mode", dispatch)
    with pytest.raises(ValueError):
        entry.main([relay.USER_RELAY_MODE])
    assert records[-1] == ("RELAY_UNHANDLED_EXCEPTION" if running else "RELAY_EXITED_BEFORE_RUNNING")


def test_rollback_exit_retains_old_reason_and_rc(monkeypatch, tmp_path):
    import current_user_onboarding as onboarding
    monkeypatch.setattr(onboarding, "_machine_worker_context", lambda _: None)
    def make_preparation_deadline(*args, **kwargs):
        assert set(kwargs) == {"defer_preparation"}
        assert kwargs["defer_preparation"] is True
        return 100.0

    monkeypatch.setattr(updates, "_relay_preparation_deadline", make_preparation_deadline)
    monkeypatch.setattr(updates.time, "monotonic", lambda: 0.0)
    monkeypatch.setattr(updates.time, "sleep", lambda _: None)
    monkeypatch.setattr(updates, "control_root", lambda _: tmp_path)
    monkeypatch.setattr(updates, "load_policy", lambda _: {
        "operator_sid": "worker", "staging_root": str(tmp_path / "stage")})
    monkeypatch.setattr(updates, "ordinary_user_context", lambda _: True)
    monkeypatch.setattr(updates, "update_lock", lambda *a, **k: nullcontext())
    pending_state = {"phase": "APPLIED_PENDING_READY"}
    settled_state = {
        "phase": "ROLLED_BACK", "trial_role": "relay", "trial_sha": "a" * 40,
        "trial_started_at": "2026-10-23T00:00:00Z", "trial_pid": 42,
    }
    reads = []
    def record(path):
        if path.name != "transition.json":
            return None
        reads.append(path)
        return dict(pending_state if len(reads) == 1 else settled_state)
    monkeypatch.setattr(updates, "optional_record", record)
    monkeypatch.setattr(updates, "native_process_identity", lambda pid: {
        "status": "LIVE", "creation_identity": "rollback-fixture-42"})
    claims = iter(["LIVE", "ABSENT"])
    monkeypatch.setattr(updates, "apply_claim_status", lambda _: next(claims))
    monkeypatch.setattr(updates, "relay_stop_requested", lambda _: False)
    monkeypatch.setattr(updates, "prepared_pending", lambda *a: False)
    monkeypatch.setattr(updates, "startup_action", lambda *a, **k: "WAIT")
    monkeypatch.setattr(updates, "read_json", lambda _: {"sha": "a" * 40})

    def exited(*args, **kwargs):
        raise updates._RelayPreparationUnavailable("RELAY_EXITED_BEFORE_RUNNING")

    monkeypatch.setattr(updates, "wait_current_relay", exited)
    records = []
    monkeypatch.setattr(updates, "emit_support_record", lambda fields, **k: records.append(fields))
    assert updates.bootstrap(tmp_path, [relay.USER_RELAY_MODE], role="relay") == 20
    assert records == [{"reason_code": "UPDATE_APPLY_NO_PROGRESS"}]


@pytest.mark.parametrize("failure", ["RELAY_EXITED_BEFORE_RUNNING", "RELAY_PARENT_EXITED"])
def test_start_current_preserves_recovery_reason_and_rc(monkeypatch, tmp_path, failure):
    monkeypatch.setattr(updates.time, "monotonic", lambda: 0.0)
    monkeypatch.setattr(updates, "now_utc", lambda: "2026-10-23T00:00:00Z")
    monkeypatch.setattr(updates, "_launch_bootstrap_version", lambda *a, **kw: 42)
    monkeypatch.setattr(updates, "write_json_durable", lambda *a: None)
    records = []
    monkeypatch.setattr(updates, "emit_support_record", lambda fields, **kw: records.append(fields))
    policy = {"staging_root": str(tmp_path)}
    active = {"sha": "a" * 40}
    monkeypatch.setattr(updates, "wait_current_relay", lambda *a, **kw: False)
    assert updates.start_current(tmp_path, policy, None, active, [relay.USER_RELAY_MODE],
                                 code="UPDATE_TERMINAL_RECOVERED", deadline=100.0) == 20
    old_reason = records[-1]

    def exited(*args, **kwargs):
        raise updates._RelayPreparationUnavailable(failure)

    monkeypatch.setattr(updates, "wait_current_relay", exited)
    assert updates.start_current(tmp_path, policy, None, active, [relay.USER_RELAY_MODE],
                                 code="UPDATE_TERMINAL_RECOVERED", deadline=100.0) == 20
    assert records[-1] == (old_reason if failure == "RELAY_EXITED_BEFORE_RUNNING"
                           else {"reason_code": failure})
    assert old_reason == {"reason_code": "UPDATE_APPLY_NO_PROGRESS"}

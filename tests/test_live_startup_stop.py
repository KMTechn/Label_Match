"""Headless cooperative boundaries; OS, registration and task mutation are doubles."""
from types import SimpleNamespace
from contextlib import nullcontext
import hashlib
import json
import threading
import time

import pytest

import current_user_onboarding as onboarding
import user_relay
from startup_timing import StartupStopRequested, startup_stop_scope
from test_current_user_onboarding import isolated_data_generation_release
from tests.test_portable_gui_onboarding import portable_gui


def test_reuse_survival_observes_stop_before_claiming_alive():
    pending = False
    owner = {"process_id": 42, "updated_at": "2026-10-22T00:00:00+00:00"}

    def checkpoint():
        if pending:
            raise StartupStopRequested()

    def wait(_seconds):
        nonlocal pending
        pending = True

    with startup_stop_scope(checkpoint), pytest.raises(StartupStopRequested):
        user_relay._reuse_existing_user_relay(lambda: owner, 2, wait)


def test_bootstrap_owner_wait_observes_stop_and_keeps_parent_until_child_exit(monkeypatch, tmp_path):
    pending = False
    clock = [0.0]
    polls = []
    child = SimpleNamespace(pid=42)

    def poll():
        polls.append(clock[0])
        return 0 if pending and clock[0] >= 0.3 else None

    child.poll = poll

    def wait(seconds):
        nonlocal pending
        clock[0] += seconds
        pending = clock[0] >= 0.1

    def checkpoint():
        if pending:
            raise StartupStopRequested()

    import auto_update
    monkeypatch.setattr(onboarding, "_machine_worker_context", lambda *_: None)
    monkeypatch.setattr(onboarding, "resolve_current_user_onboarding_paths",
                        lambda *_: SimpleNamespace(direct_sync_root=tmp_path))
    monkeypatch.setattr(user_relay, "build_user_relay_command", lambda *_: ["double", "--bootstrap"])
    monkeypatch.setattr(user_relay, "_existing_user_relay_owner", lambda *a, **k: None)
    monkeypatch.setattr(auto_update, "start_ordinary_process", lambda *a, **k: child)
    monkeypatch.setattr(user_relay.time, "monotonic", lambda: clock[0])
    with startup_stop_scope(checkpoint), pytest.raises(StartupStopRequested):
        user_relay.start_user_relay_process(tmp_path, survival_seconds=0, wait=wait)
    assert 0.3 <= clock[0] < 1
    assert polls[-1] >= 0.3


def test_bootstrap_cleanup_failure_is_not_converted_to_normal_cancel(monkeypatch, tmp_path):
    child = SimpleNamespace(poll=lambda: next(exits))
    exits = iter([None, None, 20, 20])
    waits = []
    def cancelled(*_args):
        raise StartupStopRequested()
    monkeypatch.setattr(user_relay, "_read_started_relay", cancelled)
    import auto_update
    monkeypatch.setattr(onboarding, "_machine_worker_context", lambda *_: None)
    monkeypatch.setattr(onboarding, "resolve_current_user_onboarding_paths",
                        lambda *_: SimpleNamespace(direct_sync_root=tmp_path))
    monkeypatch.setattr(user_relay, "build_user_relay_command", lambda *_: ["double", "--bootstrap"])
    monkeypatch.setattr(user_relay, "_existing_user_relay_owner", lambda *a, **k: None)
    monkeypatch.setattr(auto_update, "start_ordinary_process", lambda *a, **k: SimpleNamespace(pid=42, poll=child.poll))
    with pytest.raises(user_relay.UserRelayError):
        user_relay.start_user_relay_process(tmp_path, wait=waits.append)
    assert waits == [0.1]


@pytest.fixture
def startup_case(tmp_path, monkeypatch):
    import test_current_user_onboarding as support
    root = tmp_path / "app"
    root.mkdir()
    (root / "config").mkdir()
    (root / "config/app_settings.json").write_text("{}", encoding="utf-8")
    environment = support._environment(tmp_path)
    paths = onboarding.resolve_current_user_onboarding_paths(root, environ=environment)
    pending = []
    events = []
    monkeypatch.setattr(onboarding, "_machine_worker_context", lambda *_: None)
    monkeypatch.setattr(onboarding, "validate_pending_worker_install", lambda *_: None)
    monkeypatch.setattr(onboarding, "_portable_stop_marker_release_preflight",
                        lambda *a, **k: {"marker_present": False})

    def register(selected):
        events.append("registration-start")
        pending.append(True)
        support._ready_state(selected)
        for path in (selected.credential_path, selected.logistics_profile_path):
            protected[path] = hashlib.sha256(path.read_bytes()).hexdigest()
        events.append("registration-committed")
        return 0

    def checkpoint():
        if pending:
            events.append("stop")
            raise StartupStopRequested()

    protected = {}
    kwargs = dict(environ=environment, require_bootstrap_integrity=False,
                  registration_runner=register, profile_loader=support._profile_loader,
                  credential_loader=support._credential_loader, ledger_factory=support._ledger_factory,
                  autostart_installer=support._autostart, scheduled_task_remover=support._scheduled_task_absent,
                  legacy_task_quiescence_reader=support._legacy_task_quiescent,
                  relay_launcher=support._relay_start)
    return SimpleNamespace(root=root, paths=paths, kwargs=kwargs, checkpoint=checkpoint,
                           events=events, pending=pending, protected=protected)


def test_registration_commits_before_cancel_and_preserves_report(startup_case):
    case = startup_case
    case.paths.onboarding_report_path.parent.mkdir(parents=True, exist_ok=True)
    original = b'{"status":"READY","generation":7}'
    case.paths.onboarding_report_path.write_bytes(original)
    with startup_stop_scope(case.checkpoint), pytest.raises(StartupStopRequested):
        onboarding.onboard_current_user(case.root, **case.kwargs)
    assert case.events == ["registration-start", "registration-committed", "stop"]
    assert case.paths.onboarding_report_path.read_bytes() == original
    assert not case.paths.ledger_path.exists()
    assert all(hashlib.sha256(path.read_bytes()).hexdigest() == digest for path, digest in case.protected.items())


def test_registration_failure_wins_over_pending_stop(startup_case):
    case = startup_case

    def failed(_paths):
        case.pending.append(True)
        return 1

    case.kwargs["registration_runner"] = failed
    with startup_stop_scope(case.checkpoint), pytest.raises(onboarding.CurrentUserOnboardingError) as caught:
        onboarding.onboard_current_user(case.root, **case.kwargs)
    assert caught.value.status == "FAILED"
    assert "stop" not in case.events


@pytest.mark.parametrize("failed", [False, True])
def test_task_mutation_finishes_before_stop_or_original_failure(startup_case, failed):
    import test_current_user_onboarding as support
    case = startup_case
    case.kwargs["registration_runner"] = lambda paths: (support._ready_state(paths), 0)[1]

    def retire(_root):
        case.events.append("mutation-start")
        case.pending.append(True)
        # No callback is run inside Disable/readback/Unregister's durable unit.
        case.events.append("parent-alive-removal-guard-closed")
        case.events.append("mutation-finished")
        return {"status": "UNKNOWN" if failed else "ABSENT"}

    case.kwargs["scheduled_task_remover"] = retire
    expected = onboarding.CurrentUserOnboardingError if failed else StartupStopRequested
    with startup_stop_scope(case.checkpoint), pytest.raises(expected):
        onboarding.onboard_current_user(case.root, **case.kwargs)
    assert case.events[:3] == ["mutation-start", "parent-alive-removal-guard-closed", "mutation-finished"]
    assert ("stop" in case.events) is not failed


def test_bootstrap_stop_waits_for_live_or_unknown_mutator(monkeypatch, tmp_path):
    import auto_update
    states = iter(["LIVE", "UNKNOWN", "ABSENT"])
    observed = []

    def claim(_control):
        value = next(states)
        observed.append(value)
        return value

    monkeypatch.setattr(auto_update, "_relay_preparation_deadline", lambda *_: time.monotonic() + 5)
    monkeypatch.setattr(onboarding, "_machine_worker_context", lambda *_: None)
    monkeypatch.setattr(auto_update, "control_root", lambda *_: tmp_path)
    monkeypatch.setattr(auto_update, "load_policy", lambda *_: {"operator_sid": "isolated"})
    monkeypatch.setattr(auto_update, "ordinary_user_context", lambda *_: True)
    monkeypatch.setattr(auto_update, "update_lock", lambda *a, **k: nullcontext())
    monkeypatch.setattr(auto_update, "optional_record", lambda *_: None)
    monkeypatch.setattr(auto_update, "apply_claim_status", claim)
    monkeypatch.setattr(auto_update, "relay_stop_requested", lambda *_: True)
    assert auto_update.bootstrap(tmp_path, ["--label-match-user-relay"], role="relay") == 0
    assert observed == ["LIVE", "UNKNOWN", "ABSENT"]


def test_official_150_stop_reaches_main_and_fresh_absence(startup_case, portable_gui, monkeypatch):
    import Label_Match as lm
    import auto_update
    import test_current_user_onboarding as support
    case = startup_case
    case.kwargs["registration_runner"] = lambda paths: (support._ready_state(paths), 0)[1]
    ready = threading.Event()
    alive = [True]
    launched = [False]
    snapshots = []
    results = []
    traces = []
    child = SimpleNamespace(pid=42, poll=lambda: 0 if user_relay.user_relay_stop_path(case.paths.direct_sync_root).exists() else None)

    def owner(*_args, **_kwargs):
        if launched[0]:
            ready.set()
        return None

    def launch(*_args, **_kwargs):
        launched[0] = True
        return child

    def snapshot(**_kwargs):
        snapshots.append(time.monotonic())
        return []

    lease = lambda *_: SimpleNamespace(close=lambda: None)
    request_relay_stop = user_relay.request_user_relay_stop
    monkeypatch.setattr(user_relay, "request_user_relay_stop", lambda *a, **k:
                        request_relay_stop(*a, **k, lease_factory=lease, marker_lease_factory=lease))
    monkeypatch.setattr(user_relay, "_acquire_relay_lease", lease)
    monkeypatch.setattr(user_relay, "_app_stop_process_snapshot", snapshot)
    monkeypatch.setattr(user_relay, "_owned_app_stop_processes", lambda *a, **k: [{"pid": 42}] if alive[0] else [])
    monkeypatch.setattr(user_relay, "build_user_relay_command", lambda *_: ["double", "--bootstrap"])
    monkeypatch.setattr(user_relay, "_existing_user_relay_owner", owner)
    monkeypatch.setattr(auto_update, "start_ordinary_process", launch)
    case.kwargs["relay_launcher"] = lambda root: user_relay.reuse_or_start_user_relay_process(root, survival_seconds=0)
    monkeypatch.setattr(onboarding, "try_window_first_start", lambda *a, **k: None)
    monkeypatch.setattr(lm, "_label_match_runtime_app_root", lambda: case.root)
    monkeypatch.setattr(lm, "validate_pending_worker_install", lambda *_: None)
    monkeypatch.setattr(lm, "check_user_data_open", lambda *_: None)
    monkeypatch.setattr(lm, "_startup_app_stop_root", lambda: case.paths.direct_sync_root)
    monkeypatch.setattr(lm, "onboard_current_user", lambda *_a, **_k: onboarding.onboard_current_user(case.root, **case.kwargs))
    monkeypatch.setattr(lm, "_label_match_startup_trace", lambda stage, **_k: traces.append(stage))
    monkeypatch.setattr(lm, "_run_label_match_application", lambda: pytest.fail("GUI must not start"))

    def official_stop():
        if ready.wait(3):
            results.append(user_relay.request_app_stop(case.paths.direct_sync_root,
                app_root=case.root, webview_root=case.root / "webview", timeout_seconds=150))

    worker = threading.Thread(target=official_stop)
    started = time.monotonic()
    worker.start()
    try:
        assert lm.main(["--old-screen"]) == 0
    finally:
        alive[0] = False
        worker.join(3)
    assert not worker.is_alive()
    assert results[0]["status"] == "STOPPED"
    assert results[0]["remaining_process_count"] == 0
    assert len(snapshots) >= 2
    assert traces.count("APP_STOP_REQUESTED_CLOSE") == 1
    assert time.monotonic() - started < 150
    assert not case.paths.onboarding_report_path.exists()
    (case.root.parent / "150-evidence.json").write_text(json.dumps({"status": results[0]["status"],
        "seconds": time.monotonic() - started, "snapshots": len(snapshots),
        "main_rc": 0, "close_trace_count": traces.count("APP_STOP_REQUESTED_CLOSE"),
        "native_process_observation": False}, sort_keys=True), encoding="utf-8")


def test_pending_stop_does_not_mask_preflight_protection_failure(portable_gui, monkeypatch, tmp_path):
    import Label_Match as lm
    import startup_observation
    pending = [False]
    notices = []
    failure = onboarding.CurrentUserOnboardingError("protected refusal", report_path=tmp_path / "report.json",
                                                   cause_code="DATA_GENERATION_REFUSED")

    def refused(_root):
        pending[0] = True
        raise failure

    monkeypatch.setattr(onboarding, "try_window_first_start", lambda *a, **k: None)
    monkeypatch.setattr(lm, "_startup_app_stop_requested", lambda: pending[0])
    monkeypatch.setattr(lm, "validate_pending_worker_install", refused)
    monkeypatch.setattr(lm, "_show_first_run_onboarding_error", notices.append)
    monkeypatch.setattr(startup_observation, "write_startup_result", lambda *_: False)
    assert lm.main(["--old-screen"]) == lm.ONBOARDING_EXIT_CODE
    assert notices == [failure]
    assert failure.cause_code == "DATA_GENERATION_REFUSED"

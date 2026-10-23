"""Onboarding must not starve the actual relay's five-second admission."""
from contextlib import contextmanager, nullcontext
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import subprocess
import sys
import time

import pytest

import auto_update
import current_user_onboarding as onboarding
import user_relay
import writer_session_fence as fence
from tests.test_current_user_onboarding import (
    isolated_data_generation_release,
    _autostart, _credential_loader, _environment, _ledger_factory,
    _legacy_task_quiescent, _profile_loader, _ready_state,
    _scheduled_task_absent,
)


def _onboard(paths, environment, **kwargs):
    return onboarding.onboard_current_user(
        paths.app_root, environ=environment, require_bootstrap_integrity=False,
        profile_loader=_profile_loader, credential_loader=_credential_loader,
        ledger_factory=_ledger_factory, autostart_installer=_autostart,
        scheduled_task_remover=_scheduled_task_absent,
        legacy_task_quiescence_reader=_legacy_task_quiescent, **kwargs,
    )


def _wait_status(path, pid, *, seconds=12):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            payload = {}
        if payload.get("status") == "RUNNING" and payload.get("process_id") == pid:
            return payload
        time.sleep(0.05)
    return None


@contextmanager
def _relay(paths, tmp_path, *, launch=False):
    start, stop, events = (tmp_path / name for name in ("start", "stop", "relay-events.jsonl"))
    if launch:
        start.touch()
    with (tmp_path / "relay-child.log").open("wb") as log:
        process = subprocess.Popen(
            [sys.executable, "-B", str(Path(__file__).with_name("_onboarding_relay_child.py")),
             str(paths.direct_sync_root), str(paths.app_root), str(start), str(stop), str(events)],
            stdout=log, stderr=subprocess.STDOUT, creationflags=subprocess.CREATE_NO_WINDOW,
        )
        try:
            yield process, start, events
        finally:
            stop.touch()
            # Owned child observes this marker; no force termination or host process control.
            assert process.wait(timeout=15) == 0


@pytest.mark.parametrize("parent", [False, True], ids=["no-relay", "parent-lease"])
def test_actual_onboarding_launches_relay_without_holding_parent_admission(tmp_path, monkeypatch, parent):
    environment = _environment(tmp_path)
    monkeypatch.setenv("LABEL_MATCH_SAVE_DIR", environment["LABEL_MATCH_SAVE_DIR"])
    paths = onboarding.resolve_current_user_onboarding_paths(tmp_path / "current", environ=environment)
    monkeypatch.setenv("LABEL_MATCH_DIRECT_SYNC_ROOT", str(paths.direct_sync_root))
    assert onboarding.resolve_current_user_onboarding_paths(paths.app_root).direct_sync_root == paths.direct_sync_root
    _ready_state(paths)
    monkeypatch.setattr(user_relay, "build_user_relay_command", lambda _: ["synthetic-bootstrap", "--bootstrap"])
    # The actual launch/survival algorithm and actual child status writer run;
    # external protected-install ownership observations are supplied by this fixture.
    monkeypatch.setattr(auto_update, "control_root", lambda _: tmp_path / "update-control")
    monkeypatch.setattr(auto_update, "load_policy", lambda _: {"operator_sid": "operator"})
    monkeypatch.setattr(auto_update, "update_lock", lambda *_: nullcontext())
    monkeypatch.setattr(auto_update, "version_bundle", lambda *_: paths.app_root)
    monkeypatch.setattr(auto_update, "read_json", lambda path: {"sha": "1" * 40} if path.name == "active.json" else {"phase": "DEFERRED"})
    monkeypatch.setattr(auto_update, "assert_version_writer", lambda *_args, **_kwargs: None)
    with _relay(paths, tmp_path) as (process, start, events):
        monkeypatch.setattr(auto_update, "process_snapshot", lambda _: [{
            "pid": process.pid, "observable": True, "sha": "1" * 40,
            "sid": "operator", "role": "relay", "ordinary_user": True,
            "created_at": "2000-01-01T00:00:00+00:00",
        }])

        def launch(*_args, **_kwargs):
            start.touch()
            return process

        monkeypatch.setattr(auto_update, "start_ordinary_process", launch)
        launcher = lambda root: user_relay.start_user_relay_process(
            root, survival_seconds=0, bootstrap_readiness_seconds=9)
        started = time.monotonic()
        with fence.writer_admission("gui_settings_save") if parent else nullcontext():
            report = _onboard(paths, environment, relay_launcher=launcher)
        assert report["status"] == "READY"
        assert report["relay_start"]["process_id"] == process.pid
        assert not events.exists(), "relay timed out on onboarding's writer admission"
        (tmp_path / "onboarding-timing.json").write_text(json.dumps({
            "path": "parent-lease" if parent else "no-relay",
            "ready_seconds": time.monotonic() - started, "relay_gate_timeouts": 0,
        }), encoding="utf-8")


def test_old_relay_status_stays_fresh_during_sixty_second_onboarding_readback(tmp_path, monkeypatch):
    environment = _environment(tmp_path)
    monkeypatch.setenv("LABEL_MATCH_SAVE_DIR", environment["LABEL_MATCH_SAVE_DIR"])
    paths = onboarding.resolve_current_user_onboarding_paths(tmp_path / "app", environ=environment)
    monkeypatch.setenv("LABEL_MATCH_DIRECT_SYNC_ROOT", str(paths.direct_sync_root))
    assert onboarding.resolve_current_user_onboarding_paths(paths.app_root).direct_sync_root == paths.direct_sync_root
    _ready_state(paths)
    monkeypatch.setattr(user_relay, "build_user_relay_command", lambda _: ["synthetic-relay"])
    monkeypatch.setattr(auto_update, "start_ordinary_process", lambda *_args, **_kwargs: pytest.fail("live relay was not reused"))
    with _relay(paths, tmp_path, launch=True) as (process, _start, events):
        status = user_relay.user_relay_status_path(paths.direct_sync_root)
        assert _wait_status(status, process.pid) is not None
        original = onboarding.verify_bootstrap_integrity
        observations = []

        def delayed(*args, **kwargs):
            with fence.writer_admission("persistent_relay_status"):
                payload = json.loads(status.read_text(encoding="utf-8"))
                payload["updated_at"] = (datetime.now(timezone.utc) - timedelta(seconds=100)).isoformat()
                user_relay._write_json_atomic(status, payload)
            initial_count = payload["cycle_count"]
            started = time.monotonic()
            time.sleep(60)
            current = json.loads(status.read_text(encoding="utf-8"))
            fresh = user_relay._fresh_persistent_owner(
                current,
                app_root=paths.app_root, direct_sync_root=paths.direct_sync_root)
            observations.append(fresh)
            (tmp_path / "onboarding-timing.json").write_text(json.dumps({
                "path": "100-second-status-60-second-readback",
                "delay_seconds": time.monotonic() - started,
                "relay_cycles_during_delay": current["cycle_count"] - initial_count,
                "status_age_seconds": (datetime.now(timezone.utc) - datetime.fromisoformat(current["updated_at"])).total_seconds(),
            }), encoding="utf-8")
            return original(*args, **kwargs)

        monkeypatch.setattr(onboarding, "verify_bootstrap_integrity", delayed)
        report = _onboard(paths, environment, relay_launcher=lambda root:
            user_relay.start_user_relay_process(root, survival_seconds=0.05))
        assert observations[0] is not None, "100-second status became stale during onboarding"
        assert report["status"] == "READY"
        assert report["relay_start"]["existing_relay_reused"] is True
        assert report["relay_start"]["process_id"] == process.pid
        misses = [json.loads(line) for line in events.read_text(encoding="utf-8").splitlines()] if events.exists() else []
        # Coordinator msg_db8476bebd24: the existing atomic five-root generation
        # batch is a measured cut-after P2, not a reason to weaken READY freshness.
        assert len(misses) <= 1
        assert all(row["code"] == "WRITER_GATE_TIMEOUT" for row in misses)
        assert user_relay._fresh_persistent_owner(
            json.loads(status.read_text(encoding="utf-8")),
            app_root=paths.app_root, direct_sync_root=paths.direct_sync_root) is not None
        timing_path = tmp_path / "onboarding-timing.json"
        timing = json.loads(timing_path.read_text(encoding="utf-8"))
        timing["measured_existing_generation_gate_misses"] = len(misses)
        timing_path.write_text(json.dumps(timing), encoding="utf-8")


def test_ready_publication_reacquires_and_denies_changed_fence(tmp_path, monkeypatch):
    from tests.test_writer_session_fence import _payload
    environment = _environment(tmp_path)
    paths = onboarding.resolve_current_user_onboarding_paths(tmp_path / "app", environ=environment)
    _ready_state(paths)
    root = fence.canonical_control_root()

    def launcher(_root):
        root.mkdir(parents=True, exist_ok=True)
        (root / fence.ACTIVE_FILENAME).write_text(json.dumps(_payload(delegated_sources=[])), encoding="utf-8")
        return {"status": "ALIVE", "process_id": 77}

    with pytest.raises(onboarding.CurrentUserOnboardingError):
        _onboard(paths, environment, relay_launcher=launcher)
    assert not paths.onboarding_report_path.exists(), "READY was published under a changed fence"
    assert onboarding.REQUIRED_ENV not in environment


def test_true_relay_failure_keeps_setup_failure_and_protected_report(tmp_path):
    environment = _environment(tmp_path)
    paths = onboarding.resolve_current_user_onboarding_paths(tmp_path / "app", environ=environment)
    _ready_state(paths)

    def failed(_root):
        raise user_relay.UserRelayError("bootstrap did not prove the protected active-version relay")

    with pytest.raises(onboarding.CurrentUserOnboardingError) as caught:
        _onboard(paths, environment, relay_launcher=failed)
    assert caught.value.cause_code == "SETUP_FAILED"
    report = json.loads(paths.onboarding_report_path.read_text(encoding="utf-8"))
    assert report["status"] == "FAILED"
    assert report["cause_code"] == "SETUP_FAILED"


@pytest.mark.parametrize("marker_present", [False, True])
@pytest.mark.parametrize("typed_error", [False, True])
def test_relay_exit_reason_survives_onboarding_failure_marker(
    tmp_path, monkeypatch, marker_present, typed_error,
):
    environment = _environment(tmp_path)
    paths = onboarding.resolve_current_user_onboarding_paths(tmp_path / "app", environ=environment)
    _ready_state(paths)
    marker = user_relay.user_relay_stop_path(paths.direct_sync_root)
    if marker_present:
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text("preserved marker")
    monkeypatch.setattr(onboarding, "_portable_stop_marker_release_preflight",
                        lambda *a, **k: {"marker_present": False})

    def failed(_root):
        if typed_error:
            raise onboarding.CurrentUserOnboardingError(
                "RELAY_EXITED_BEFORE_RUNNING", report_path=paths.onboarding_report_path,
                cause_code="RELAY_EXITED_BEFORE_RUNNING",
            )
        error = user_relay.UserRelayError("RELAY_EXITED_BEFORE_RUNNING")
        error.cause_code = "RELAY_EXITED_BEFORE_RUNNING"
        raise error

    with pytest.raises(onboarding.CurrentUserOnboardingError) as caught:
        _onboard(paths, environment, relay_launcher=failed)
    assert caught.value.cause_code == "RELAY_EXITED_BEFORE_RUNNING"
    report = json.loads(paths.onboarding_report_path.read_text(encoding="utf-8"))
    assert report["cause_code"] == "RELAY_EXITED_BEFORE_RUNNING"
    from failure_redaction import public_failure_record
    assert public_failure_record(caught.value, "SUPPORT")["reason_code"] == "RELAY_EXITED_BEFORE_RUNNING"
    if marker_present:
        assert marker.read_text() == "preserved marker"


def test_lifecycle_helper_runs_without_admission_and_locked_readback_denies_change(tmp_path, monkeypatch):
    control = tmp_path / "update-control"
    control.mkdir()
    for name in ("policy.json", "active.json", "transition.json"):
        (control / name).write_text("{}", encoding="utf-8")
    monkeypatch.setattr(auto_update, "control_root", lambda _: control)
    observations = []

    def observe(*args, **kwargs):
        # A different process can enter while the helper/observation runs.
        command = [sys.executable, "-B", "-c", "from writer_session_fence import writer_admission; "
                   "\nwith writer_admission('persistent_relay_status'): pass"]
        result = subprocess.run(command, capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
        observations.append(result.returncode)
        (control / "active.json").write_text('{"changed":true}', encoding="utf-8")

    monkeypatch.setattr(auto_update, "assert_version_writer", observe)
    with pytest.raises(fence.WriterFencedError):
        with fence.writer_admission("current_user_onboarding"):
            pytest.fail("changed lifecycle admitted a mutation")
    assert observations == [0]


@pytest.mark.parametrize("registered", [False, True], ids=["created", "reused"])
def test_local_writes_keep_short_admission_and_readers_launch_run_outside(tmp_path, monkeypatch, registered):
    environment = _environment(tmp_path)
    paths = onboarding.resolve_current_user_onboarding_paths(tmp_path / "app", environ=environment)
    if registered:
        _ready_state(paths)
    checks = []

    def protected(label, action):
        def call(*args, **kwargs):
            assert getattr(fence._WRITER_LOCAL, "depth", 0) > 0, label
            checks.append(label)
            return action(*args, **kwargs)
        return call

    def unprotected(label, action):
        def call(*args, **kwargs):
            assert getattr(fence._WRITER_LOCAL, "depth", 0) == 0, label
            checks.append(label)
            return action(*args, **kwargs)
        return call

    write = onboarding._write_json_bytes_atomic
    monkeypatch.setattr(onboarding, "_write_json_bytes_atomic", protected("report/settings", write))
    monkeypatch.setattr(onboarding, "verify_bootstrap_integrity", unprotected("integrity", onboarding.verify_bootstrap_integrity))
    report = onboarding.onboard_current_user(
        paths.app_root, environ=environment, require_bootstrap_integrity=False,
        registration_runner=protected("registration", lambda selected: (_ready_state(selected), 0)[1]),
        profile_loader=unprotected("profile-readback", _profile_loader),
        credential_loader=unprotected("credential-readback", _credential_loader),
        ledger_factory=protected("ledger", _ledger_factory),
        autostart_installer=protected("autostart", _autostart),
        scheduled_task_remover=protected("task-retirement", _scheduled_task_absent),
        legacy_task_quiescence_reader=unprotected("legacy-observation", _legacy_task_quiescent),
        relay_launcher=unprotected("relay-launch", lambda _: {"status": "ALIVE"}),
    )
    assert report["status"] == "READY"
    assert {"integrity", "ledger", "autostart", "task-retirement", "relay-launch", "report/settings"} <= set(checks)
    assert ("registration" in checks) is not registered


def test_caught_probe_resume_denial_cannot_reuse_parent_writer_lease(tmp_path, monkeypatch):
    from tests.test_writer_session_fence import _payload
    root = fence.canonical_control_root()
    monkeypatch.setattr(user_relay, "build_user_relay_command", lambda _: ["synthetic-relay"])

    def launch(_command):
        root.mkdir(parents=True, exist_ok=True)
        (root / fence.ACTIVE_FILENAME).write_text(json.dumps(_payload(delegated_sources=[])), encoding="utf-8")

    with fence.writer_admission("gui_settings_save"):
        with pytest.raises(fence.WriterFencedError):
            user_relay.start_user_relay_process(tmp_path, launcher=launch)
        with pytest.raises(fence.WriterFencedError):
            with fence.writer_admission("gui_settings_save"):
                pytest.fail("a caught resume denial reused the stale parent lease")

"""Relay startup must distinguish a dead child from a live, unready child."""
from contextlib import nullcontext
import json
import subprocess
import sys
from types import SimpleNamespace

import pytest

import auto_update as updates
import current_user_onboarding as onboarding
import user_relay


@pytest.mark.parametrize("returncode", [0, 7])
@pytest.mark.parametrize("bootstrap", [False, True])
def test_started_child_exit_without_running_fails_promptly(monkeypatch, returncode, bootstrap):
    clock = [100.0]
    monkeypatch.setattr(user_relay.time, "monotonic", lambda: clock[0])
    child = SimpleNamespace(pid=42, poll=lambda: returncode)
    with pytest.raises(user_relay.UserRelayError) as caught:
        user_relay._read_started_relay(
            child, lambda: None, 2.0, 700.0, bootstrap,
            lambda seconds: clock.__setitem__(0, clock[0] + seconds),
        )
    assert caught.value.cause_code == "RELAY_EXITED_BEFORE_RUNNING"
    assert caught.value.exit_code == returncode
    assert clock[0] <= 102.1


def test_exited_bootstrap_keeps_proven_running_owner(monkeypatch):
    owner = {"process_id": 77, "updated_at": user_relay._now()}
    result = user_relay._read_started_relay(
        SimpleNamespace(pid=42, poll=lambda: 0), lambda: owner, 0.0,
        user_relay.time.monotonic() + 600, True, lambda _: None,
    )
    assert result["process_id"] == 77


def test_live_unready_bootstrap_keeps_original_deadline(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(user_relay.time, "monotonic", lambda: clock[0])
    with pytest.raises(user_relay.UserRelayError) as caught:
        user_relay._read_started_relay(
            SimpleNamespace(pid=42, poll=lambda: None), lambda: None, 0.0,
            103.0, True, lambda seconds: clock.__setitem__(0, clock[0] + seconds),
        )
    assert not caught.value.cause_code
    assert clock[0] == pytest.approx(103.0)


def test_wait_current_relay_detects_real_child_exit(monkeypatch, tmp_path):
    child = subprocess.Popen([sys.executable, "-B", "-c", "pass"],
                             creationflags=subprocess.CREATE_NO_WINDOW)
    assert child.wait(timeout=10) == 0
    monkeypatch.setattr(updates, "read_json", lambda _: {"sha": "1" * 40})
    monkeypatch.setattr(updates, "current_relay_ready", lambda *a, **k: False)
    monkeypatch.setattr(updates, "_relay_startup_stop_checkpoint", lambda _: None)
    reports = []
    monkeypatch.setattr(updates, "emit_support_record", lambda fields, **kw: reports.append(fields))
    start = updates.time.monotonic()
    # A short deadline also bounds RED on the old implementation.
    with pytest.raises(updates._RelayPreparationUnavailable) as caught:
        updates.wait_current_relay(tmp_path, {}, "1" * 40, started_at=updates.now_utc(),
                                   deadline=start + 0.5, pid=child.pid)
    assert caught.value.code == "RELAY_EXITED_BEFORE_RUNNING"
    assert updates.time.monotonic() - start < 0.5


def test_live_relay_without_running_keeps_timeout(monkeypatch, tmp_path):
    clock = [100.0]
    monkeypatch.setattr(updates.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(updates.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    monkeypatch.setattr(updates, "read_json", lambda _: {"sha": "1" * 40})
    monkeypatch.setattr(updates, "current_relay_ready", lambda *a, **k: False)
    monkeypatch.setattr(updates, "native_process_identity", lambda _: {"status": "LIVE"})
    monkeypatch.setattr(updates, "_relay_startup_stop_checkpoint", lambda _: None)
    assert not updates.wait_current_relay(tmp_path, {}, "1" * 40,
                                         started_at=updates.now_utc(), deadline=103.0, pid=42)
    assert clock[0] == pytest.approx(103.0)


def test_wait_current_relay_detects_pid_reuse_without_running(monkeypatch, tmp_path):
    monkeypatch.setattr(updates, "read_json", lambda _: {"sha": "1" * 40})
    monkeypatch.setattr(updates, "current_relay_ready", lambda *a, **k: False)
    monkeypatch.setattr(updates, "_relay_startup_stop_checkpoint", lambda _: None)
    births = iter(["1", "2"])
    monkeypatch.setattr(updates, "native_process_identity",
                        lambda _: {"status": "LIVE", "creation_identity": next(births)})
    with pytest.raises(updates._RelayPreparationUnavailable) as caught:
        updates.wait_current_relay(tmp_path, {}, "1" * 40, started_at=updates.now_utc(),
                                   deadline=updates.time.monotonic() + 2.0, pid=42)
    assert caught.value.code == "RELAY_EXITED_BEFORE_RUNNING"


def test_fenced_wait_records_fixed_reason_without_mutating_status(monkeypatch, tmp_path):
    from writer_session_fence import WriterFencedError
    status = tmp_path / "status.json"
    status.write_text("preserved")
    reports = []
    monkeypatch.setattr(user_relay, "_record_relay_reason", lambda code, **kwargs: reports.append(code), raising=False)
    def denied(*a, **k):
        raise WriterFencedError("WRITER_FENCED", "test")
    monkeypatch.setattr(user_relay, "writer_admission", denied)
    result = user_relay.run_persistent_relay_loop(
        lambda: pytest.fail("fenced cycle ran"), status_path=status, max_cycles=1,
    )
    assert result["status"] == "FENCED"
    assert "RELAY_WRITER_FENCED" in reports
    assert status.read_text() == "preserved"


@pytest.mark.parametrize("code", ["RELAY_STOP_MARKER_PRESENT", "RELAY_INSTANCE_LEASE_UNAVAILABLE",
                                  "RELAY_WRITER_FENCED", "RELAY_STARTUP_HANDLED"])
def test_reason_record_uses_existing_support_sink_only(monkeypatch, tmp_path, code):
    control = tmp_path / "control"
    stage = tmp_path / "stage"
    monkeypatch.setattr(updates, "control_root", lambda _: control)
    monkeypatch.setattr(updates, "load_policy", lambda _: {"staging_root": str(stage)})
    user_relay._record_relay_reason(code, app_root=tmp_path)
    snapshots = list(stage.glob("failure-*.json"))
    assert len(snapshots) == 1
    assert json.loads(snapshots[0].read_text()) == {"stage": "update_apply", "reason_code": code}
    assert not (stage / "startup-support.json").exists()


def test_reason_snapshot_survives_later_parent_reason(monkeypatch, tmp_path):
    monkeypatch.setattr(updates, "control_root", lambda _: tmp_path / "control")
    monkeypatch.setattr(updates, "load_policy", lambda _: {"staging_root": str(tmp_path / "stage")})
    user_relay._record_relay_reason("RELAY_STOP_MARKER_PRESENT", app_root=tmp_path)
    user_relay._record_relay_reason("RELAY_EXITED_BEFORE_RUNNING", app_root=tmp_path, exit_code=0)
    records = [json.loads(p.read_text()) for p in (tmp_path / "stage").glob("failure-*.json")]
    assert {row["reason_code"] for row in records} == {"RELAY_STOP_MARKER_PRESENT", "RELAY_EXITED_BEFORE_RUNNING"}
    assert next(row for row in records if row["reason_code"] == "RELAY_EXITED_BEFORE_RUNNING")["exit_code"] == 0


@pytest.mark.parametrize("branch", ["stop_marker", "instance_lease", "startup_deferred"])
def test_main_silent_startup_branch_records_reason(monkeypatch, tmp_path, branch):
    import label_data_generation as generation
    import logistics_runtime_profile as profiles
    monkeypatch.setattr(updates, "startup_gate", lambda *a, **k: 0 if branch == "startup_deferred" else None)
    monkeypatch.setattr(generation, "release_identity", lambda _: None)
    monkeypatch.setattr(generation, "normalize_data_arguments", lambda *a: None)
    monkeypatch.setattr(user_relay, "check_user_data_open", lambda *a: None)
    monkeypatch.setattr(user_relay, "record_user_data_open", lambda *a: None)
    monkeypatch.setattr(onboarding, "apply_current_user_runtime_environment", lambda *a: None)
    monkeypatch.setattr(onboarding, "resolve_current_user_onboarding_paths", lambda _: SimpleNamespace(
        direct_sync_root=tmp_path / "relay", data_root=tmp_path / "data",
        ledger_path=tmp_path / "ledger.sqlite3", queue_dir=tmp_path / "queue",
        logistics_profile_path=tmp_path / "profile", settings_path=tmp_path / "settings",
    ))
    monkeypatch.setattr(profiles, "load_logistics_runtime_profile", lambda **k: SimpleNamespace())
    monkeypatch.setattr(user_relay, "_resolve_scan_source_dir", lambda *a, **k: tmp_path / "scan")
    monkeypatch.setattr(user_relay, "_acquire_relay_lease", lambda _: None)
    marker = user_relay.user_relay_stop_path(tmp_path / "relay")
    if branch == "stop_marker":
        marker.parent.mkdir(parents=True)
        marker.write_text("preserved")
    reports = []
    monkeypatch.setattr(user_relay, "_record_relay_reason", lambda code, **k: reports.append(code))
    assert user_relay.main(["--app-root", str(tmp_path)], credential_input=object()) == 0
    assert reports == [{"stop_marker": "RELAY_STOP_MARKER_PRESENT",
                        "instance_lease": "RELAY_INSTANCE_LEASE_UNAVAILABLE",
                        "startup_deferred": "RELAY_STARTUP_HANDLED"}[branch]]
    if marker.exists():
        assert marker.read_text() == "preserved"

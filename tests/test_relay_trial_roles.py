from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import auto_update as updates
import current_user_onboarding
import user_relay


def pending(phase="APPLIED_PENDING_READY"):
    now = datetime.now(timezone.utc) - timedelta(seconds=1)
    stamp = now.isoformat()
    state = updates.prepare_transition(
        previous_sha="a" * 40, candidate_sha="b" * 40,
        packet_sha256="c" * 64, forward_pin="d" * 64, rollback_pin="e" * 64,
        transition_id="f" * 32, nonce="1" * 32, now=stamp,
    )
    state = updates.apply_transition(state, writers_absent=True,
        packet_verified=True, durable_prepared=True, now=stamp)
    if phase == "ROLLBACK_PENDING_READY":
        state = {**state, "phase": phase, "rollback_at": stamp}
    return state


@pytest.mark.parametrize("phase", ["APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"])
@pytest.mark.parametrize("role", ["gui", "relay"])
def test_trial_confirmation_keeps_role_specific_initialization_and_all_data_checks(phase, role):
    state = pending(phase)
    sha = state["previous_sha"] if phase == "ROLLBACK_PENDING_READY" else state["candidate_sha"]
    state = updates.bind_trial(state, sha=sha, pid=713, ordinary_user=True,
                               now=state["applied_at"], role=role)
    checks = updates.RELAY_READY_CHECKS if role == "relay" else updates.READY_CHECKS
    now = datetime.now(timezone.utc).isoformat()
    receipt = {**{name: True for name in checks}, "role": role,
        "pid": state["trial_pid"], "sha": sha, "nonce": state["nonce"],
        "transition_id": state["transition_id"], "ready_at": now}
    assert updates.ready_matches(state, receipt, process_matches=True, relay_matches=True, now=now)
    for check in checks:
        assert not updates.ready_matches(state, {**receipt, check: False},
                                          process_matches=True, relay_matches=True, now=now)
    assert not updates.ready_matches(state, {**receipt, "role": "relay" if role == "gui" else "gui"},
                                      process_matches=True, relay_matches=True, now=now)
    assert not updates.ready_matches(state, receipt, process_matches=False, relay_matches=True, now=now)
    assert not updates.ready_matches(state, receipt, process_matches=True, relay_matches=False, now=now)
    result = updates.confirm_transition(state, receipt, process_matches=True, relay_matches=True, now=now)
    assert result["phase"] == ("ROLLED_BACK" if phase == "ROLLBACK_PENDING_READY" else "CONFIRMED")


@pytest.mark.parametrize("phase", ["ROLLBACK_PENDING_READY", "FAILED", "APPLIED_PENDING_READY"])
def test_recovery_child_without_ready_signal_returns_failure(monkeypatch, tmp_path, phase):
    state = pending()
    state["phase"] = phase
    control = tmp_path / "update-control"
    policy = {"staging_root": str(tmp_path / "stage"), "operator_sid": "test-worker",
              "operator_local_app_data": str(tmp_path / "operator-local")}
    active = {"sha": state["previous_sha"]}
    reports = []
    monkeypatch.setattr(updates, "write_json_durable", lambda path, payload: reports.append(payload))
    monkeypatch.setattr(updates, "assert_ordinary_recovery", lambda *args: None)
    launches = []
    monkeypatch.setattr(updates, "_launch_bootstrap_version", lambda *args, **kwargs: launches.append(args) or 713)
    monkeypatch.setattr(updates, "read_json", lambda *args: active)
    monkeypatch.setattr(updates, "current_relay_ready", lambda *args, **kwargs: False)
    monkeypatch.setattr(updates, "native_process_identity", lambda pid: {
        "status": "LIVE", "creation_identity": "71300"})
    monkeypatch.setattr(updates, "emit_support_record", lambda *args, **kwargs: None)
    clock = [0.0]
    monkeypatch.setattr(updates.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(updates.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    assert updates.start_current(control, policy, state, active, [user_relay.USER_RELAY_MODE],
                                 code="UPDATE_TERMINAL_RECOVERED", deadline=0.3) != 0
    assert len(launches) == 1
    assert reports[-1]["reason_code"] == "UPDATE_APPLY_NO_PROGRESS"


def test_current_relay_requires_native_identity_one_relay_and_fresh_target_cycle(monkeypatch, tmp_path):
    state = pending()
    control = tmp_path / "update-control"
    sha = state["candidate_sha"]
    bundle = updates.version_bundle(control, sha)
    root = tmp_path / "worker"
    policy = {"operator_sid": "test-worker"}
    monkeypatch.setattr(current_user_onboarding, "resolve_current_user_onboarding_paths",
                         lambda _: SimpleNamespace(direct_sync_root=root))
    now = datetime.now(timezone.utc).isoformat()
    status = {"status": "RUNNING", "process_id": 713, "source_commit": sha,
              "ordinary_user": True, "updated_at": now, "code_root": str(bundle)}
    row = {"pid": 713, "role": "relay", "sha": sha, "sid": policy["operator_sid"],
           "ordinary_user": True, "observable": True, "created_at": now}
    monkeypatch.setattr(updates, "optional_record", lambda _: status)
    rows = [row]
    monkeypatch.setattr(updates, "process_snapshot", lambda _: rows)
    started = state["applied_at"]
    assert updates.current_relay_ready(control, policy, sha, started_at=started, pid=713)
    for key, wrong in [("source_commit", state["previous_sha"]), ("ordinary_user", False),
                       ("process_id", 714), ("code_root", str(tmp_path)),
                       ("updated_at", (datetime.now(timezone.utc) - timedelta(days=1)).isoformat())]:
        original = status[key]
        status[key] = wrong
        assert not updates.current_relay_ready(control, policy, sha, started_at=started, pid=713)
        status[key] = original
    for key, wrong in [("sid", "other-worker"), ("sha", state["previous_sha"]),
                       ("role", "gui"), ("ordinary_user", False)]:
        original = row[key]
        row[key] = wrong
        assert not updates.current_relay_ready(control, policy, sha, started_at=started, pid=713)
        row[key] = original
    rows.append(dict(row, pid=714))
    assert not updates.current_relay_ready(control, policy, sha, started_at=started, pid=713)


def test_late_native_readback_cannot_finish_relay_wait_after_its_deadline(monkeypatch, tmp_path):
    clock = [0.0]
    sha = "b" * 40
    monkeypatch.setattr(updates.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(updates, "read_json", lambda _: {"sha": sha})
    monkeypatch.setattr(updates, "relay_stop_requested", lambda policy: False)
    def delayed_proof(*args, **kwargs):
        clock[0] = 1.0
        return True
    monkeypatch.setattr(updates, "current_relay_ready", delayed_proof)
    assert not updates.wait_current_relay(tmp_path, {}, sha,
        started_at=updates.now_utc(), deadline=0.5, pid=713)


def test_old_headless_packet_uses_its_own_product_dispatcher_and_never_gui_main(tmp_path):
    app = tmp_path / "app"
    app.mkdir()
    (app / "label_match_product_host.py").write_text("", encoding="utf-8")
    command = updates.headless_product_arguments(tmp_path, [user_relay.USER_RELAY_MODE])
    assert command == ["-c", updates.LEGACY_HEADLESS_BOOTSTRAP, str(app), user_relay.USER_RELAY_MODE]
    assert "Label_Match" not in command[1] and "main.py" not in command
    (app / "label_match_product_host.py").unlink()
    with pytest.raises(ValueError):
        updates.headless_product_arguments(tmp_path, [user_relay.USER_RELAY_MODE])
    with pytest.raises(ValueError):
        updates.headless_product_arguments(tmp_path, [])


def test_relay_trial_pid_reuse_is_rejected_for_its_bound_creation_time():
    state = pending()
    state = updates.bind_trial(state, sha=state["candidate_sha"], pid=713,
                               ordinary_user=True, now=state["applied_at"], role="relay")
    row = {"pid": 713, "role": "relay", "observable": True, "ordinary_user": True,
           "sha": state["candidate_sha"], "sid": "test-worker", "created_at": state["applied_at"]}
    assert updates.process_matches_trial(row, state=state, sha=row["sha"], sid=row["sid"], role="relay")
    row["created_at"] = datetime.now(timezone.utc).isoformat()
    assert not updates.process_matches_trial(row, state=state, sha=row["sha"], sid=row["sid"], role="relay")

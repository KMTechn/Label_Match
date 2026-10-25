"""Force SYSTEM binding before the relay's first durable RUNNING cycle."""
# Actual-bootstrap GREEN3/cap observations cover CA supervision,480,candidate Popen0 and UTC/monotonic boundaries.
from contextlib import nullcontext
from datetime import datetime, timezone
import sqlite3
from types import SimpleNamespace

import pytest

import auto_update as updates
import current_user_onboarding as onboarding
from tests.test_relay_trial_roles import pending
from tests.test_update_rollback_readiness import write
from user_relay import user_relay_status_path


@pytest.fixture
def bound_relay(tmp_path, monkeypatch):
    def build(phase, proof, *, real_terminal=False, after_confirm=None):
        state = pending(phase)
        control, stage = tmp_path / "control", tmp_path / "stage"
        sha = state["previous_sha"] if phase == "ROLLBACK_PENDING_READY" else state["candidate_sha"]
        app = updates.version_bundle(control, sha) / "app"
        policy = {"operator_sid": "S-1-5-21-111-222-333-1001", "staging_root": str(stage),
                  "operator_local_app_data": str(tmp_path / "operator-local")}
        write(control / "transition.json", state)
        write(control / "active.json", {"sha": sha})
        paths = SimpleNamespace(ledger_path=tmp_path / "ledger.db",
            direct_sync_root=tmp_path / "sync", data_root=tmp_path / "data")
        paths.data_root.mkdir()
        (paths.direct_sync_root / "queue").mkdir(parents=True)
        for database in (paths.ledger_path, paths.direct_sync_root / "queue/direct_sync_relay.sqlite3"):
            with sqlite3.connect(database) as db:
                db.execute("CREATE TABLE qualification (id INTEGER)")
        monkeypatch.setattr(onboarding, "_machine_worker_context", lambda root: {})
        monkeypatch.setattr(onboarding, "resolve_current_user_onboarding_paths", lambda root: paths)
        monkeypatch.setattr(onboarding, "inspect_current_user_state", lambda paths: {"status": "READY"})
        monkeypatch.setattr(updates, "control_root", lambda root: control)
        monkeypatch.setattr(updates, "load_policy", lambda root: policy)
        monkeypatch.setattr(updates, "ordinary_user_context", lambda *args: True)
        monkeypatch.setattr(updates, "version_manifest", lambda root: {"source_commit": sha})
        monkeypatch.setattr(updates, "write_json_durable", write)
        monkeypatch.setattr(updates, "update_lock", lambda *a, **k: nullcontext())
        monkeypatch.setattr(updates, "apply_claim_status", lambda root: ("LIVE" if updates.read_json(control / "transition.json")["phase"] in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"} else "ABSENT"))
        monkeypatch.setattr(updates, "prepared_pending", lambda *a: False)
        monkeypatch.setattr(updates, "request_apply_task", lambda *a: None)
        monkeypatch.setattr(updates, "record_confirmed_transition", lambda root, value: value)
        monkeypatch.setattr(updates, "emit_support_record", lambda *a, **k: None)
        monkeypatch.setattr(updates, "emit_progress_guidance", lambda: None)
        monkeypatch.setattr(updates, "current_generation_status", lambda *a: "ALLOW")
        monkeypatch.setattr(updates, "assert_ordinary_recovery", lambda *a: None)
        rows, launches, events = [], [], []
        monkeypatch.setattr(updates, "process_snapshot", lambda root: list(rows))
        monkeypatch.setattr(updates, "native_process_identity", lambda pid: {
            "status": "LIVE" if any(row["pid"] == pid for row in rows) else "ABSENT",
            "creation_identity": str(pid * 100) if any(row["pid"] == pid for row in rows) else None,
        })

        def launch(root, selected, arguments, **kwargs):
            launches.append((selected, arguments))
            rows.append(dict(pid=33333, role="relay", sha=sha, sid=policy["operator_sid"],
                ordinary_user=True, observable=True, created_at=updates.now_utc()))
            return 33333

        monkeypatch.setattr(updates, "_launch_bootstrap_version", launch)
        if not real_terminal:
            monkeypatch.setattr(updates, "start_current", lambda root, policy, state, *a, **k:
                0 if state["phase"] in {"CONFIRMED", "ROLLED_BACK"} else 20)
        clock = [0.0]
        monkeypatch.setattr(updates.time, "monotonic", lambda: clock[0])
        monkeypatch.setattr(updates, "STARTUP_WAIT_SECONDS", 1.0)
        monkeypatch.setattr(updates, "bootstrap_supervision_deadline",
            lambda state, *, started, now, utc_now, **_observation: started + updates.STARTUP_WAIT_SECONDS)

        def system_observe(seconds):
            clock[0] += seconds
            claim = updates.optional_record(stage / "trial.json")
            state = updates.read_json(control / "transition.json")
            if not claim or state["phase"] not in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"}:
                return
            state, confirmed, exited = updates.observe_trial_ready(control, policy, state, rows,
                claim, updates.optional_record(stage / "ready.json"), deadline=1.0)
            assert not exited
            if not events:
                assert state["trial_pid"] == 33333 and not confirmed
                assert not (stage / "ready.json").exists()
                events.append("SYSTEM_BOUND_BEFORE_RUNNING")
                if proof != "missing":
                    payload = dict(status="RUNNING", source_commit=sha, update_nonce=state["nonce"],
                        process_id=33333, ordinary_user=True, updated_at=updates.now_utc(), code_root=str(app))
                    if proof == "nonce": payload["update_nonce"] = "f" * 32
                    if proof == "sha": payload["source_commit"] = "f" * 40
                    if proof == "role":
                        claim["role"] = "gui"
                        write(stage / "trial.json", claim)
                    write(user_relay_status_path(paths.direct_sync_root), payload)
            elif confirmed:
                events.append(state["phase"])
                if after_confirm is not None:
                    after_confirm(paths, rows)

        monkeypatch.setattr(updates.time, "sleep", system_observe)
        return SimpleNamespace(app=app, control=control, stage=stage, policy=policy,
            sha=sha, rows=rows, launches=launches, events=events)
    return build


@pytest.mark.parametrize("phase", ["APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"])
@pytest.mark.parametrize("proof", ["valid", "missing", "nonce", "sha", "role"])
def test_bound_claim_still_publishes_actual_readiness(bound_relay, phase, proof):
    trial = bound_relay(phase, proof)
    result = updates._bootstrap(trial.app, ["--label-match-user-relay"], role="relay")
    state = updates.read_json(trial.control / "transition.json")
    assert trial.events[0] == "SYSTEM_BOUND_BEFORE_RUNNING"
    if proof == "valid":
        assert result == 0
        assert state["phase"] == ("ROLLED_BACK" if phase == "ROLLBACK_PENDING_READY" else "CONFIRMED")
        assert trial.events[-1] == state["phase"]
        assert state["trial_pid"] == 33333
        assert len(trial.launches) == 1
    else:
        assert result == 20 and state["phase"] == phase
        assert not (trial.stage / "ready.json").exists()


@pytest.mark.parametrize("recheck", ["valid", "missing", "pid", "sha"])
def test_rollback_bootstrap_reuses_only_the_confirmed_ready_pid(bound_relay, recheck):
    def after_confirm(paths, rows):
        status = user_relay_status_path(paths.direct_sync_root)
        if recheck == "missing":
            status.unlink()
        elif recheck in {"pid", "sha"}:
            payload = updates.read_json(status)
            payload["process_id" if recheck == "pid" else "source_commit"] = (
                33334 if recheck == "pid" else "f" * 40)
            write(status, payload)
    trial = bound_relay("ROLLBACK_PENDING_READY", "valid", real_terminal=True, after_confirm=after_confirm)
    result = updates._bootstrap(trial.app, ["--label-match-user-relay"], role="relay")
    assert result == (0 if recheck == "valid" else 20)
    assert trial.events == ["SYSTEM_BOUND_BEFORE_RUNNING", "ROLLED_BACK"]
    assert updates.read_json(trial.control / "transition.json")["trial_pid"] == 33333
    assert len(trial.launches) == 1 and trial.rows[0]["pid"] == 33333

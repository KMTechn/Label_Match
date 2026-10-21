"""REVIEW-119-LM-3: retain real admission/launch bodies through the spawn gate."""
from contextlib import contextmanager, nullcontext
import json
from types import SimpleNamespace
import sys

import pytest

import auto_update
import current_user_onboarding as onboarding
import writer_session_fence


@pytest.fixture
def admitted_launch(monkeypatch, tmp_path):
    def run(mode, starting_point="trial-primary"):
        clock = [1000.0]
        control = tmp_path / "update-control"
        policy = {"operator_sid": "operator", "staging_root": str(tmp_path / "staging")}
        active = {"sha": "a" * 40}
        state = {"schema": auto_update.SCHEMA, "phase": "APPLIED_PENDING_READY", "nonce": "b" * 32,
                 "transition_id": "c" * 32, "candidate_sha": active["sha"], "trial_pid": None}
        parent_pid = 4242
        parent_deadline = 1011.0 if mode == "budget-exhausted" else 1030.0
        invalid_at = None if mode == "within-budget" else 1001.0
        admissions, spawns, reports, calls = [], [], [], {}
        bundle = auto_update.version_bundle(control, active["sha"])
        (bundle / "app").mkdir(parents=True)
        (bundle / "app/label_match_relay_entry.py").write_text("", encoding="utf-8")

        def sleep(seconds):
            clock[0] = round(clock[0] + seconds, 7)

        def identity(pid):
            if pid == parent_pid and mode == "parent-unverified" and clock[0] >= invalid_at:
                return {"status": "UNKNOWN"}
            if pid == parent_pid and mode == "parent-death" and clock[0] >= invalid_at:
                return {"status": "ABSENT"}
            return {"status": "LIVE", "creation_identity": "1" if pid == parent_pid else "2"}

        def read(path):
            if path == control / "transition.json":
                return state
            if path == control / "active.json":
                return active
            return None

        @contextmanager
        def admission(source, **kwargs):
            record = {"entered": clock[0] - 1000}
            admissions.append(record)
            # S5's primary GUI must start while the parent/budget is valid.
            sleep(0 if starting_point == "trial-secondary" and len(admissions) == 1 else 2.0)
            record["granted"] = clock[0] - 1000
            try:
                yield
            finally:
                record["released"] = clock[0] - 1000

        def spawn(command, **kwargs):
            spawns.append(clock[0] - 1000)
            if mode == "within-budget":
                state["phase"] = "CONFIRMED"
            return SimpleNamespace(pid=4343)

        monkeypatch.setattr(auto_update.time, "monotonic", lambda: clock[0])
        monkeypatch.setattr(auto_update.time, "sleep", sleep)
        monkeypatch.setattr(auto_update.os, "getppid", lambda: parent_pid)
        monkeypatch.setattr(auto_update, "native_process_identity", identity)
        monkeypatch.setattr(onboarding, "_machine_worker_context", lambda root: None)
        monkeypatch.setattr(auto_update, "control_root", lambda root: control)
        monkeypatch.setattr(auto_update, "load_policy", lambda root: policy)
        monkeypatch.setattr(auto_update, "ordinary_user_context", lambda sid: True)
        monkeypatch.setattr(auto_update, "update_lock", lambda *a, **k: nullcontext())
        monkeypatch.setattr(auto_update, "read_json", read)
        monkeypatch.setattr(auto_update, "optional_record", read)
        monkeypatch.setattr(auto_update, "apply_claim_status",
                            lambda root: "LIVE" if state["phase"] == "APPLIED_PENDING_READY" else "NONE")
        monkeypatch.setattr(auto_update, "request_apply_task", lambda *a: None)
        monkeypatch.setattr(auto_update, "version_manifest", lambda root: {"source_commit": active["sha"]})
        monkeypatch.setattr(writer_session_fence, "writer_admission", admission)
        monkeypatch.setattr(auto_update, "start_ordinary_process", spawn)
        monkeypatch.setattr(auto_update, "write_json_durable", lambda *a: None)
        monkeypatch.setattr(auto_update, "emit_support_record", lambda fields, **k: reports.append(fields))
        if starting_point.startswith("recovery-"):
            rows = [{"role": "relay" if starting_point == "recovery-gui" else "gui",
                     "pid": 4444, "created_at": "2026-10-20T00:00:00Z"}]
            monkeypatch.setattr(auto_update, "process_snapshot", lambda root: [])
            monkeypatch.setattr(auto_update, "writer_processes", lambda *a, **k: rows)
            monkeypatch.setattr(auto_update, "assert_ordinary_recovery", lambda *a, **k: rows)
            monkeypatch.setenv("KMTECH_LM_UPDATE_DEFERRED_AT", "2026-10-20T00:00:00Z")
        monkeypatch.setattr(auto_update, "wait_current_relay", lambda *a, **k: True)

        def profile(frame, event, arg):
            name = frame.f_code.co_name
            if event == "call" and ((frame.f_code.co_filename == auto_update.__file__ and name in {
                    "_bootstrap", "startup_action", "start_current", "recovery_writer_result",
                    "_launch_bootstrap_version", "launch_version"}) or (
                    frame.f_code.co_filename == writer_session_fence.__file__ and name == "guarded")):
                calls[name] = calls.get(name, 0) + 1

        previous = sys.getprofile()
        sys.setprofile(profile)
        try:
            if starting_point.startswith("trial-"):
                role = "gui" if starting_point == "trial-secondary" else "relay"
                arguments = [] if role == "gui" else ["--label-match-user-relay"]
                result = auto_update._bootstrap(tmp_path / "app", arguments, role=role,
                                                parent_pid=parent_pid, parent_deadline=parent_deadline)
            else:
                deadline = auto_update._relay_preparation_deadline(parent_deadline, parent_pid)
                if starting_point == "start-current":
                    result = auto_update.start_current(control, policy, {"phase": "CONFIRMED"}, active,
                        ["--label-match-user-relay"], code="UPDATE_TERMINAL_RECOVERED", deadline=deadline)
                else:
                    role = "gui" if starting_point == "recovery-gui" else "relay"
                    arguments = [] if role == "gui" else ["--label-match-user-relay"]
                    result = auto_update.recovery_writer_result(control, policy, active, arguments,
                        role=role, deadline=deadline, state={"phase": "FAILED"}, claim_status="ABSENT")
        finally:
            sys.setprofile(previous)
        evidence = {"mode": mode, "starting_point": starting_point, "result": result,
                    "parent_deadline": parent_deadline - 1000, "shared_child_deadline": parent_deadline - 1010,
                    "invalid_at": None if invalid_at is None else invalid_at - 1000,
                    "elapsed": clock[0] - 1000, "admissions": admissions, "spawns": spawns,
                    "actual_function_calls": calls, "reason_codes": [r.get("reason_code") for r in reports]}
        (tmp_path / "successful-admission-observation.json").write_text(
            json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
        return evidence

    return run


@pytest.mark.parametrize("mode", ["parent-death", "budget-exhausted", "within-budget"])
def test_successful_admission_cannot_cross_shared_launch_boundary(admitted_launch, mode):
    evidence = admitted_launch(mode)
    assert evidence["admissions"][0]["granted"] - evidence["admissions"][0]["entered"] < 5
    assert all("released" in record for record in evidence["admissions"])
    assert evidence["actual_function_calls"]["launch_version"] == 1
    if mode == "within-budget":
        assert evidence["result"] == 0 and evidence["spawns"] == [2.0]
    else:
        assert evidence["result"] == 20
        assert not evidence["spawns"]
        assert evidence["reason_codes"][-1] == "UPDATE_APPLY_NO_PROGRESS"


@pytest.mark.parametrize("starting_point", ["start-current", "recovery-gui", "recovery-relay", "trial-secondary"])
@pytest.mark.parametrize("mode", ["parent-death", "budget-exhausted"])
def test_other_starting_points_recheck_after_successful_admission(admitted_launch, mode, starting_point):
    evidence = admitted_launch(mode, starting_point)
    assert all("released" in record for record in evidence["admissions"])
    assert evidence["actual_function_calls"]["launch_version"] == len(evidence["admissions"])
    assert evidence["result"] == 20
    assert evidence["spawns"] == ([0.0] if starting_point == "trial-secondary" else [])
    assert evidence["reason_codes"][-1] == "UPDATE_APPLY_NO_PROGRESS"


def test_successful_admission_with_unverified_parent_does_not_spawn(admitted_launch):
    evidence = admitted_launch("parent-unverified")
    assert evidence["result"] == 20
    assert evidence["spawns"] == []
    assert all("released" in record for record in evidence["admissions"])
    assert evidence["reason_codes"][-1] == "UPDATE_SUPERVISION_UNAVAILABLE"

"""REVIEW-119-LM-2: trial admission and public parent-context boundaries."""
from contextlib import nullcontext
import json
import sys

import pytest

import auto_update
import current_user_onboarding as onboarding
import failure_redaction


@pytest.mark.parametrize("failure_mode", ["parent-death", "budget-exhausted"])
def test_trial_admission_stops_at_shared_parent_boundary(monkeypatch, tmp_path, failure_mode):
    clock = [1000.0]
    control = tmp_path / "update-control"
    policy = {"operator_sid": "operator", "staging_root": str(tmp_path / "staging")}
    active = {"sha": "a" * 40}
    state = {"schema": auto_update.SCHEMA, "phase": "APPLIED_PENDING_READY",
             "nonce": "b" * 32, "transition_id": "c" * 32,
             "candidate_sha": active["sha"], "trial_pid": None}
    attempts, launches, reports = [], [], []
    invalid_at = 1005.0 if failure_mode == "parent-death" else 1020.0
    available_at = 1006.0 if failure_mode == "parent-death" else 1031.0
    parent_pid = 4242

    def sleep(seconds):
        clock[0] = round(clock[0] + seconds, 7)

    def identity(pid):
        if pid == parent_pid and failure_mode == "parent-death" and clock[0] >= invalid_at:
            return {"status": "ABSENT"}
        return {"status": "LIVE", "creation_identity": "1" if pid == parent_pid else "2"}

    def read_record(path):
        if path == control / "transition.json":
            return state
        if path == control / "active.json":
            return active
        return None

    def launch(*args, **kwargs):
        attempts.append(clock[0])
        if clock[0] < available_at:
            raise auto_update.WriterFencedError("WRITER_GATE_TIMEOUT", "contested test admission")
        launches.append(clock[0])
        return 4343

    monkeypatch.setattr(auto_update.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(auto_update.time, "sleep", sleep)
    monkeypatch.setattr(auto_update.os, "getppid", lambda: parent_pid)
    monkeypatch.setattr(auto_update, "native_process_identity", identity)
    monkeypatch.setattr(onboarding, "_machine_worker_context", lambda root: None)
    monkeypatch.setattr(auto_update, "control_root", lambda root: control)
    monkeypatch.setattr(auto_update, "load_policy", lambda root: policy)
    monkeypatch.setattr(auto_update, "ordinary_user_context", lambda sid: True)
    monkeypatch.setattr(auto_update, "update_lock", lambda *a, **k: nullcontext())
    monkeypatch.setattr(auto_update, "read_json", read_record)
    monkeypatch.setattr(auto_update, "optional_record", read_record)
    monkeypatch.setattr(auto_update, "apply_claim_status", lambda root: "LIVE")
    monkeypatch.setattr(auto_update, "request_apply_task", lambda *a: None)
    monkeypatch.setattr(auto_update, "launch_version", launch)
    monkeypatch.setattr(auto_update, "write_json_durable", lambda *a: None)
    monkeypatch.setattr(auto_update, "emit_support_record", lambda fields, **k: reports.append(fields))
    calls = {}

    def profile(frame, event, arg):
        if event == "call" and frame.f_code.co_filename == auto_update.__file__:
            name = frame.f_code.co_name
            if name in {"_bootstrap", "startup_action", "_launch_bootstrap_version",
                        "_relay_preparation_deadline"}:
                calls[name] = calls.get(name, 0) + 1

    previous_profile = sys.getprofile()
    sys.setprofile(profile)
    try:
        result = auto_update._bootstrap(tmp_path / "app", ["--label-match-user-relay"],
                                        role="relay", parent_pid=parent_pid, parent_deadline=1030.0)
    finally:
        sys.setprofile(previous_profile)
    evidence = {"mode": failure_mode, "result": result, "elapsed": clock[0] - 1000,
                "invalid_after": invalid_at - 1000,
                "attempts_after_boundary": sum(t >= invalid_at for t in attempts),
                "launches_after_boundary": [t - 1000 for t in launches if t >= invalid_at],
                "reason_codes": [r.get("reason_code") for r in reports],
                "actual_function_calls": calls}
    (tmp_path / "trial-boundary-observation.json").write_text(
        json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    assert result == 20
    assert not launches, "trial relay started after parent loss or shared budget exhaustion"
    assert not evidence["attempts_after_boundary"]
    assert clock[0] <= invalid_at + 0.1
    assert reports[-1]["reason_code"] == "UPDATE_APPLY_NO_PROGRESS"


@pytest.mark.parametrize("value", ["nan", "inf", "-inf"],
                         ids=["nan", "positive-infinity", "negative-infinity"])
@pytest.mark.parametrize("attached", [False, True], ids=["separate-value", "attached-value"])
def test_public_parent_nonfinite_context_returns_contract_exit(monkeypatch, tmp_path, value, attached):
    reports = []
    monkeypatch.setattr(onboarding, "_machine_worker_context", lambda root: None)
    monkeypatch.setattr(auto_update, "control_root", lambda root: tmp_path)
    monkeypatch.setattr(auto_update, "emit_support_record", lambda fields, **k: reports.append(fields))
    monkeypatch.setattr(failure_redaction, "emit_support_failure", lambda *a, **k: None)
    deadline_args = ["--relay-parent-deadline=" + value] if attached else ["--relay-parent-deadline", value]
    try:
        result = auto_update.main(["--bootstrap", "--role", "relay", "--relay-parent-pid", "4242",
                                   *deadline_args, "--", "--label-match-user-relay"])
    except SystemExit as error:
        result = error.code
    evidence = {"attached": attached, "case": "negative" if value.startswith("-") else "other",
                "exit": result, "reason_codes": [r.get("reason_code") for r in reports]}
    (tmp_path / "argument-boundary-observation.json").write_text(
        json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
    assert result == 20
    assert reports[-1]["reason_code"] == "UPDATE_PREPARATION_FAILED"


@pytest.mark.parametrize("prefix", [["--"], ["product-argument"]], ids=["separator", "positional"])
def test_parent_value_normalization_preserves_forwarded_arguments(monkeypatch, tmp_path, prefix):
    forwarded = []
    monkeypatch.setattr(onboarding, "_machine_worker_context", lambda root: None)
    monkeypatch.setattr(auto_update, "control_root", lambda root: tmp_path)
    monkeypatch.setattr(auto_update, "bootstrap",
                        lambda root, arguments, **kwargs: forwarded.append(arguments) or 0)
    remainder = [*prefix, "--relay-parent-deadline", "-inf"]
    assert auto_update.main(["--bootstrap", "--role", "relay", *remainder]) == 0
    assert forwarded == [remainder[1:] if prefix == ["--"] else remainder]

"""Actual bootstrap observes a LIVE SYSTEM decision after its stage budget."""
from contextlib import contextmanager
import copy
import json
from pathlib import Path

import pytest

from tests.recovery_memory_world import MemoryPath, World


class _DecisionWorldWaitLimit(RuntimeError):
    """Bound only the synthetic world's observation loop, never production."""


def _decision_world(*, decision_at=257.7, root_busy=True, boundary_claim="LIVE", boundary_at=255,
                    existing_relay=False, native_busy=False, terminal_claim=None):
    source = (Path(__file__).resolve().parents[1] / "auto_update.py").read_text(encoding="utf-8-sig")
    world = World("LM-A30-decision", source, phase="PREPARED", writer=None, claim="LIVE")
    world.clock = 0.0
    world.policy.update(schema="label-match-startup-update-policy-v1", settings_sha256="f" * 64,
        feed_url="https://setup.test.invalid/feed", channel="stable", profile_auth=True,
        apply_during_work=False, hiberboot_enabled=None, hiberboot_read_status="UNPROVEN")
    generation = {key: copy.deepcopy(world.state[key]) for key in (
        "generation_before", "generation_previous_release", "generation_candidate_release")}
    generation["generation_previous_release"]["version"] = "v2.0.124"
    generation["generation_candidate_release"]["version"] = "v2.0.125"
    prepared = world.ns["prepare_transition"](previous_sha=world.previous, candidate_sha=world.candidate,
        packet_sha256="c" * 64, forward_pin="d" * 64, rollback_pin="e" * 64,
        transition_id="1" * 32, nonce="2" * 32, now=world.at(0))
    applied = world.ns["apply_transition"]({**prepared, **generation}, writers_absent=True,
        packet_verified=True, durable_prepared=True, now=world.at(0))
    world.ns["persist_switch"](world.control, applied, world.candidate)
    # Generation is unproved until the modeled SYSTEM owner publishes its readback.
    world.guard = "UNKNOWN"
    pending, decided, boundary = [], [], []
    calls, busy, trial_attempts = [], [], []
    root_depth, release_after_read = [0], []
    terminal_claim_reads, claim_settled = [], []

    def advance(seconds):
        target = round(world.clock + seconds, 9)
        if target > 600:
            raise _DecisionWorldWaitLimit("synthetic observation exceeded 600 seconds")
        events = [value for value in (240 if not pending else None,
            boundary_at if not boundary else None, decision_at if not decided else None, 480)
            if value is not None and world.clock < value <= target]
        world.clock = min(events) if events else target
        if release_after_read and not claim_settled:
            world.set_claim("NONE")
            claim_settled.append(world.clock)
        if world.clock >= 240 and not pending:
            state = world.ns["request_rollback"](world.state, reason="candidate readiness timed out")
            world.ns["write_json_durable"](world.control / "transition.json", state)
            pending.append(world.clock)
        if world.clock >= boundary_at and not boundary:
            boundary.append(world.clock)
            if boundary_claim == "ABSENT":
                world.set_claim("ABSENT")
            elif boundary_claim == "UNKNOWN":
                world.owner_status = "UNKNOWN"
        if decision_at is not None and world.clock >= decision_at and not decided:
            # Actual generation decision API chooses retention from the changed marker.
            world.generation["record"] = dict(build_id=world.candidate, version="v2.0.125")
            state = world.ns["decide_trial_failure"](world.state, world.policy)
            world.ns["write_json_durable"](world.control / "transition.json", state)
            world.ns["persist_switch"](world.control, state, world.candidate)
            world.set_claim("NONE" if terminal_claim is None else "LIVE")
            if terminal_claim == "UNKNOWN":
                world.owner_status = "UNKNOWN"
            world.guard = "ALLOW"
            if existing_relay:
                world.rows = [world.row("relay", sha=world.candidate, created=world.clock, pid=7001)]
            decided.append(world.clock)

    @contextmanager
    def lock(*args, **kwargs):
        if kwargs.get("trial"):
            trial_attempts.append(world.clock)
            raise RuntimeError("update mutex ownership is busy")
        if pending and not decided and (root_busy or world.clock < 255):
            busy.append(world.clock)
            advance(0.1)
            raise RuntimeError("update mutex ownership is busy")
        try:
            root_depth[0] += 1
            yield True
        finally:
            root_depth[0] -= 1
            advance(0.25)

    spawn = world.ns["start_ordinary_process"]
    def observed_spawn(argv, **kwargs):
        process = spawn(argv, **kwargs)
        environment = kwargs.get("environment", kwargs.get("env", {}))
        calls.append(dict(at=world.clock, pid=process.pid,
            trial=bool(environment.get("KMTECH_LM_UPDATE_TRIAL_NONCE")),
            ordinary=not bool(environment.get("KMTECH_LM_UPDATE_TRIAL_NONCE"))))
        return process

    world.ns["update_lock"] = lock
    world.ns["start_ordinary_process"] = observed_spawn
    world.ns["subprocess"].Popen = observed_spawn
    world.spawn = observed_spawn
    world.time.sleep = advance
    claim_status = world.ns["apply_claim_status"]
    def observed_claim_status(*args, **kwargs):
        status = claim_status(*args, **kwargs)
        if terminal_claim is not None and decided and root_depth[0]:
            terminal_claim_reads.append(dict(at=world.clock, status=status))
            if status in {"LIVE", "UNKNOWN"} and not release_after_read:
                release_after_read.append(world.clock)
        return status
    world.ns["apply_claim_status"] = observed_claim_status
    native_waits = []
    native_patch = None
    if native_busy:
        # Execute the real mutex body; substitute only native API results and time.
        import ast
        import ctypes
        import hashlib
        import os
        from unittest.mock import patch

        class NativeCall:
            def __init__(self, callback):
                self.callback = callback
            def __call__(self, *args):
                return self.callback(*args)

        handles = {}
        def create(_attributes, name, _flags, _access):
            handle = len(handles) + 1
            handles[handle] = name
            return handle
        def wait(handle, milliseconds):
            contended = handles[handle].endswith(".Trial") or bool(pending and not decided)
            native_waits.append(dict(at=world.clock, milliseconds=milliseconds, busy=contended))
            if contended:
                advance(milliseconds / 1000)
                return 0x102
            return 0
        class NativeAPI:
            CreateMutexExW = NativeCall(create)
            WaitForSingleObject = NativeCall(wait)
            ConvertStringSecurityDescriptorToSecurityDescriptorW = NativeCall(lambda *args: True)
            ReleaseMutex = NativeCall(lambda *args: True)
            CloseHandle = NativeCall(lambda *args: True)
            LocalFree = NativeCall(lambda *args: None)
        body = next(node for node in ast.parse(source).body
                    if isinstance(node, ast.FunctionDef) and node.name == "update_lock")
        namespace = dict(contextmanager=contextmanager, Path=Path, hashlib=hashlib, os=os,
                         task_name=lambda *args: None)
        exec(compile(ast.Module(body=[body], type_ignores=[]), "actual-update-lock", "exec"), namespace)
        # MemoryPath is intentionally filesystem-free; native mutex naming only needs absolute().
        native_patch = patch.multiple(ctypes, WinDLL=lambda *args, **kwargs: NativeAPI())
        native_patch.start()
        world.ns["update_lock"] = namespace["update_lock"]
        absolute_patch = patch.object(MemoryPath, "absolute", lambda self: self, create=True)
        absolute_patch.start()
    starts, retained, recovery = [], [], []
    start = world.ns["start_current"]
    def observed_start(*args, **kwargs):
        starts.append(dict(code=kwargs["code"], owned=kwargs.get("_ownership") is not None))
        return start(*args, **kwargs)
    retain = world.ns["_bootstrap_retained_candidate"]
    def observed_retained(*args, **kwargs):
        retained.append(dict(owned=kwargs.get("ownership") is not None))
        return retain(*args, **kwargs)
    recover = world.ns["recovery_writer_result"]
    def observed_recovery(*args, **kwargs):
        row = dict(owned=kwargs.get("_ownership") is not None)
        recovery.append(row)
        row["result"] = recover(*args, **kwargs)
        return row["result"]
    world.ns.update(start_current=observed_start, _bootstrap_retained_candidate=observed_retained,
        recovery_writer_result=observed_recovery)
    if existing_relay:
        world.ns["version_manifest"] = lambda bundle, **kwargs: dict(source_commit=world.active, version="v2.0.125")
    error_type = None
    try:
        rc = world.ns["_bootstrap"](MemoryPath("/memory/current/app"),
            ["--label-match-user-relay"], role="relay")
    except (ValueError, _DecisionWorldWaitLimit) as error:
        rc, error_type = None, type(error).__name__
    finally:
        if native_patch is not None:
            absolute_patch.stop()
            native_patch.stop()
    report = world.files.get(str(world.stage / "startup-support.json"), {})
    return dict(returncode=rc, exit_clock=world.clock, phase=world.state["phase"],
        calls=calls, ordinary_popen=sum(row["ordinary"] for row in calls),
        trial_popen=sum(row["trial"] for row in calls), pending_at=pending,
        decided_at=decided, root_busy_after_normal_deadline=sum(t >= 255 for t in busy),
        trial_attempts_after_normal_deadline=sum(t >= 255 for t in trial_attempts),
        boundary_claim=boundary_claim, normal_supervision_end=255.0, maximum_supervision_end=480.0,
        support_reason=report.get("reason_code"), starts=starts, retained=retained, recovery=recovery,
        error_type=error_type, remaining_pids=[row["pid"] for row in world.rows], native_waits=native_waits,
        terminal_claim_reads=terminal_claim_reads, claim_settled=claim_settled)


@pytest.mark.parametrize("root_busy", [True, False], ids=["busy", "available"])
def test_live_system_decision_after_stage_plus17_7_opens_ordinary(tmp_path, root_busy):
    facts = _decision_world(root_busy=root_busy)
    (tmp_path / "apply-decision-proof.json").write_text(json.dumps(facts), encoding="utf-8")
    assert facts["returncode"] == 0
    assert facts["ordinary_popen"] == 1 and facts["trial_popen"] == 0
    assert facts["phase"] == "DEFERRED" and facts["decided_at"] == [257.7]
    assert facts["calls"][0]["at"] >= 257.7 and facts["exit_clock"] < 480
    assert facts["trial_attempts_after_normal_deadline"] == 0
    if root_busy:
        assert facts["root_busy_after_normal_deadline"] > 0


def test_live_system_claim_still_stops_at480_without_popen(tmp_path):
    facts = _decision_world(decision_at=None)
    (tmp_path / "apply-decision-cap-proof.json").write_text(json.dumps(facts), encoding="utf-8")
    assert facts["returncode"] == 20 and facts["exit_clock"] == 480
    assert facts["ordinary_popen"] == facts["trial_popen"] == 0
    assert facts["phase"] == "ROLLBACK_PENDING" and not facts["decided_at"]


@pytest.mark.parametrize("claim", ["ABSENT", "UNKNOWN"])
def test_nonlive_apply_claim_does_not_extend_the_original_deadline(tmp_path, claim):
    facts = _decision_world(decision_at=None, boundary_claim=claim)
    (tmp_path / "apply-decision-nonlive-proof.json").write_text(json.dumps(facts), encoding="utf-8")
    # UNKNOWN retains the existing final locked read; the fake busy read costs .1s.
    expected_exit = 255.1 if claim == "UNKNOWN" else 255
    assert facts["returncode"] == 20 and facts["exit_clock"] == expected_exit
    assert facts["ordinary_popen"] == facts["trial_popen"] == 0
    assert facts["phase"] == "ROLLBACK_PENDING" and not facts["decided_at"]

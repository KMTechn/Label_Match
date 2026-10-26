"""Lane 21:3x/21:4x: actual bootstrap before apply and after LIVE loss."""
from contextlib import contextmanager
import copy
import json
from pathlib import Path

import pytest

from tests.recovery_memory_world import MemoryPath, World
from tests.test_bootstrap_apply_decision_wait import _decision_world


def _prepare_world(*, record_present=True, apply_at=260, claim="LIVE"):
    source = (Path(__file__).resolve().parents[1] / "auto_update.py").read_text(encoding="utf-8-sig")
    world = World("LM-live-prepare", source, phase="PREPARED", writer=None, claim=claim)
    world.clock = 0.0
    world.guard = "UNKNOWN"
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
    prepared.update(generation)
    if record_present:
        world.state = prepared
    else:
        del world.files[str(world.control / "transition.json")]
    applied, calls, confirmed_at = [], [], []

    def advance(seconds):
        target = round(world.clock + seconds, 9)
        events = [value for value in (apply_at if not applied else None, world.ns["BOOTSTRAP_OBSERVATION_CAP_SECONDS"])
            if value is not None and world.clock < value <= target]
        world.clock = min(events) if events else target
        if apply_at is not None and world.clock >= apply_at and not applied:
            switched = world.ns["apply_transition"](prepared, writers_absent=True,
                packet_verified=True, durable_prepared=True, now=world.at(world.clock))
            world.ns["persist_switch"](world.control, switched, world.candidate)
            world.guard = "ALLOW"
            applied.append(world.clock)
        if calls and world.state["phase"] == "APPLIED_PENDING_READY":
            world.clock = max(world.clock, applied[0] + 150)
            trial = world.read(world.stage / "trial.json")
            receipt = dict(pid=trial["pid"], role="gui", sha=world.candidate,
                nonce=world.state["nonce"], transition_id=world.state["transition_id"],
                ready_at=world.at(world.clock), **{flag: True for flag in world.ns["READY_CHECKS"]})
            state, confirmed, exited = world.ns["observe_trial_ready"](
                world.control, world.policy, world.state, world.rows, trial, receipt,
                deadline=applied[0] + world.ns["READY_WINDOW_SECONDS"])
            assert confirmed and not exited
            world.ns["persist_switch"](world.control, state, world.candidate)
            world.set_claim("NONE")
            confirmed_at.append(world.clock)

    @contextmanager
    def lock(*args, **kwargs):
        if not applied:
            raise RuntimeError("update mutex ownership is busy")
        yield True

    spawn = world.ns["start_ordinary_process"]
    def observed_spawn(argv, **kwargs):
        process = spawn(argv, **kwargs)
        calls.append(dict(at=world.clock, pid=process.pid))
        return process

    world.ns.update(update_lock=lock, start_ordinary_process=observed_spawn,
        headless_product_arguments=lambda bundle, arguments: ["memory-relay", *arguments],
        version_manifest=lambda bundle, **kwargs: dict(source_commit=world.active, version="v2.0.125"))
    world.ns["subprocess"].Popen = observed_spawn
    world.spawn = observed_spawn
    world.time.sleep = advance
    rc = world.ns["_bootstrap"](MemoryPath("/memory/current/app"), [], role="gui")
    return dict(preparation_end=world.ns["RELAY_PREPARE_SECONDS"], cap=world.ns["BOOTSTRAP_OBSERVATION_CAP_SECONDS"], returncode=rc, exit_clock=world.clock, initial_record_present=record_present,
        applied_at=applied, confirmed_at=confirmed_at, calls=calls,
        phase=world.state["phase"] if str(world.control / "transition.json") in world.files else None)


@pytest.mark.parametrize("record_present", [True, False], ids=["PREPARED", "missing"])
def test_live_prepare_after240_applies_and_opens_this_gui(tmp_path, record_present):
    facts = _prepare_world(record_present=record_present)
    (tmp_path / "live-prepare-proof.json").write_text(json.dumps(facts), encoding="utf-8")
    assert facts["returncode"] == 0 and facts["phase"] == "CONFIRMED"
    assert facts["applied_at"] == [260] and facts["confirmed_at"] == [410]
    assert len(facts["calls"]) == 2 and facts["calls"][0]["at"] >= 260
    assert facts["exit_clock"] < facts["cap"]


def test_absent_prepare_does_not_extend_the_original240(tmp_path):
    facts = _prepare_world(apply_at=None, claim="ABSENT")
    (tmp_path / "absent-prepare-proof.json").write_text(json.dumps(facts), encoding="utf-8")
    assert facts["returncode"] == 20 and facts["exit_clock"] == 240
    assert not facts["calls"] and not facts["applied_at"]


def test_live_prepare_still_stops_at480_without_popen(tmp_path):
    facts = _prepare_world(apply_at=None)
    (tmp_path / "live-prepare-cap-proof.json").write_text(json.dumps(facts), encoding="utf-8")
    assert facts["returncode"] == 20
    assert facts["preparation_end"] <= facts["exit_clock"] < facts["preparation_end"] + 1
    assert facts["phase"] == "PREPARED"
    assert not facts["calls"] and not facts["applied_at"]


@pytest.mark.parametrize("claim", ["UNKNOWN", "ABSENT"])
def test_live_loss_after_extension_exits_while_root_stays_busy(tmp_path, claim):
    facts = _decision_world(decision_at=None, boundary_claim=claim, boundary_at=256)
    (tmp_path / "live-loss-proof.json").write_text(json.dumps(facts), encoding="utf-8")
    assert facts["returncode"] == 20
    assert facts["preparation_end"] <= facts["exit_clock"] < facts["preparation_end"] + 1
    assert facts["ordinary_popen"] == facts["trial_popen"] == 0
    assert facts["phase"] == "APPLIED_PENDING_READY" and not facts["pending_at"] and not facts["decided_at"]

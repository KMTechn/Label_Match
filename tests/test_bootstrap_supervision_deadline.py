"""Exercise candidate launch admission through the actual bootstrap clock."""
from contextlib import contextmanager
import copy
import json
from pathlib import Path

import pytest

from tests.recovery_memory_world import World, MemoryPath
import auto_update as updates


def _delayed_apply(role, preparation):
    source = (Path(__file__).resolve().parents[1] / "auto_update.py").read_text(encoding="utf-8-sig")
    world = World(f"LM-A23-{role}-{preparation}", source, phase="PREPARED", writer=None, claim="LIVE")
    world.clock = 0.0
    world.policy.update(schema="label-match-startup-update-policy-v1", settings_sha256="f" * 64,
                        feed_url="https://setup.test.invalid/feed", channel="stable",
                        profile_auth=True, apply_during_work=False,
                        hiberboot_enabled=None, hiberboot_read_status="UNPROVEN")
    generation = {key: copy.deepcopy(world.state[key]) for key in (
        "generation_before", "generation_previous_release", "generation_candidate_release")}
    generation["generation_previous_release"]["version"] = "v2.0.124"
    generation["generation_candidate_release"]["version"] = "v2.0.125"
    world.state = world.ns["prepare_transition"](
        previous_sha=world.previous, candidate_sha=world.candidate, packet_sha256="c" * 64,
        forward_pin="d" * 64, rollback_pin="e" * 64, transition_id="1" * 32,
        nonce="2" * 32, now=world.at(0))
    world.state = {**world.state, **generation}
    applied, prepared_launch, trials = [], [], []
    candidate_launches = []

    @contextmanager
    def lock(*_args, **_kwargs):
        if not applied:
            world.clock = float(preparation)
            switched = world.ns["apply_transition"](world.state, writers_absent=True,
                packet_verified=True, durable_prepared=True, now=world.at(world.clock))
            world.ns["persist_switch"](world.control, switched, world.candidate)
            applied.append(world.clock)
        yield True

    def launch_arguments(_bundle, arguments):
        # Existing preparation/OS work can finish after the supervisor deadline.
        # The real launch_version must check again immediately before Popen.
        if not prepared_launch:
            prepared_launch.append(True)
            world.clock += 24
        return ["memory-relay", *arguments]

    def version_manifest(bundle, **_kwargs):
        return dict(source_commit=world.active, version="v2.0.124")

    spawn = world.ns["start_ordinary_process"]
    def observed_spawn(argv, **kwargs):
        child = spawn(argv, **kwargs)
        if world.active == world.candidate:
            candidate_launches.append(dict(pid=child.pid, at=world.clock))
        if kwargs.get("environment", {}).get("KMTECH_LM_UPDATE_TRIAL_NONCE") == world.state["nonce"]:
            trials.append(dict(pid=child.pid, at=world.clock))
        return child

    def sleep(seconds):
        world.clock = round(world.clock + seconds, 9)
        if trials and world.state["phase"] == "APPLIED_PENDING_READY":
            world.clock = max(world.clock, applied[0] + 150)
            claim = world.read(world.stage / "trial.json")
            flags = world.ns["RELAY_READY_CHECKS"] if role == "relay" else world.ns["READY_CHECKS"]
            receipt = dict(pid=claim["pid"], role=role, sha=world.candidate,
                           nonce=world.state["nonce"], transition_id=world.state["transition_id"],
                           ready_at=world.at(world.clock), **{flag: True for flag in flags})
            state, confirmed, exited = world.ns["observe_trial_ready"](
                world.control, world.policy, world.state, world.rows, claim, receipt,
                deadline=applied[0] + world.ns["READY_WINDOW_SECONDS"])
            assert confirmed and not exited
            world.state = state
            world.set_claim("NONE")

    world.ns.update(update_lock=lock, headless_product_arguments=launch_arguments,
                    version_manifest=version_manifest, start_ordinary_process=observed_spawn)
    if role == "gui":
        # GUI preparation has the same latency but retains the real GUI argument path.
        native_manifest = world.ns["version_manifest"]
        def gui_manifest(bundle, **kwargs):
            if not prepared_launch:
                prepared_launch.append(True)
                world.clock += 24
            return native_manifest(bundle, **kwargs)
        world.ns["version_manifest"] = gui_manifest
    world.spawn = observed_spawn
    world.ns["subprocess"].Popen = observed_spawn
    world.time.sleep = sleep
    arguments = ["--label-match-user-relay"] if role == "relay" else []
    rc = world.ns["_bootstrap"](MemoryPath("/memory/current/app"), arguments, role=role)
    exit_clock = world.clock
    candidate_rows_before_system = sum(row.get("sha") == world.candidate for row in world.rows)
    rollback_decision = None
    if not trials:
        # Model the unchanged SYSTEM peer at the original applied+READY deadline.
        world.clock = applied[0] + world.ns["READY_WINDOW_SECONDS"]
        failed = world.ns["request_rollback"](world.state, reason="candidate readiness timed out")
        decided = world.ns["decide_trial_failure"](failed, world.policy)
        world.ns["write_json_durable"](world.control / "transition.json", decided)
        rollback_decision = decided["failure_decision"]["code"]
        restored = world.ns["rollback_transition"](decided, writers_absent=True,
            previous_verified=True, durable_rollback=True, nonce="3" * 32, now=world.at(world.clock))
        world.ns["persist_switch"](world.control, restored, world.previous)
        world.auto_ready = True
        assert world.ns["wait_for_rollback_ready"](world.control, world.policy) == 0
    return dict(role=role, preparation_seconds=preparation, bootstrap_rc=rc,
                candidate_launches=len(candidate_launches), candidate_rows_before_system=candidate_rows_before_system,
                bootstrap_exit_clock=exit_clock, trial_launches=len(trials),
                first_trial_at=trials[0]["at"] if trials else None,
                phase=world.state["phase"], failed_candidate=world.candidate in world.state["failed_shas"],
                rollback_decision_code=rollback_decision, generation_anchors_preserved=True,
                final_clock=world.clock, applied_at_clock=applied[0],
                ready_deadline=applied[0]+world.ns["READY_WINDOW_SECONDS"])


@pytest.mark.parametrize("role", ["relay", "gui"])
def test_preparation220_keeps_the_original_ready_window_and_confirms(tmp_path, role):
    facts = _delayed_apply(role, 220)
    (tmp_path / "supervision-deadline-proof.json").write_text(json.dumps(facts), encoding="utf-8")
    assert facts["trial_launches"] == (1 if role == "relay" else 2)
    assert facts["bootstrap_rc"] == 0 and facts["phase"] == "CONFIRMED"
    assert facts["first_trial_at"] == 244 and facts["final_clock"] == 370
    assert facts["final_clock"] < facts["ready_deadline"] and not facts["failed_candidate"]


def test_preparation470_hits_the_absolute480_cap_before_launch_and_rolls_back(tmp_path):
    facts = _delayed_apply("relay", updates.BOOTSTRAP_OBSERVATION_CAP_SECONDS - 10)
    (tmp_path / "supervision-cap-proof.json").write_text(json.dumps(facts), encoding="utf-8")
    assert facts["bootstrap_rc"] == 20 and facts["trial_launches"] == facts["candidate_launches"] == facts["candidate_rows_before_system"] == 0
    assert facts["phase"] == "ROLLED_BACK" and facts["failed_candidate"]
    assert facts["final_clock"] == facts["ready_deadline"] == updates.BOOTSTRAP_OBSERVATION_CAP_SECONDS - 10 + updates.READY_WINDOW_SECONDS

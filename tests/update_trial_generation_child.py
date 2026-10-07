"""Headless real-process trial failure/recovery evidence; no native install or GUI."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from kmtech_shared import data_generation as generation
import label_data_generation as guard
import auto_update as updates

OLD, NEW = "1" * 40, "2" * 40
SID = "S-1-5-21-455"
OLD_VERSION, NEW_VERSION = "v2.0.98", "v2.0.99"


def paths(root, role):
    owner = root / ("gui-data" if role == "gui" else "external-relay-queue")
    return guard.marker_paths([owner], SID, environ={"LOCALAPPDATA": str(root / "local")})


def mark(path, version, build, **options):
    path.parent.mkdir(parents=True, exist_ok=True)
    generation.record_generation(path, version, app_id="Label_Match", subject_sid=SID,
                                 build_id=build, **options)


def run(mode, scenario, role, root):
    slots = paths(root, role)
    if mode == "admission":
        decision = generation.probe_downgrade(slots, NEW_VERSION, app_id="Label_Match",
            subject_sid=SID, target_build_id=NEW)
        notice = (generation.REFUSAL_MESSAGE if decision['status'] == 'REFUSE' else generation.UNKNOWN_MESSAGE) if not decision["allowed"] else None
        print(json.dumps({"status": decision["status"], "message": notice,
            "selected_sha": updates.read_json(root / "control/active.json")["sha"]}))
        return
    if mode == "candidate":
        if scenario in {"before", "unknown-before"}:
            os._exit(17)  # Candidate exits before the first generation write.
        options = ({"data_compatible_min_version": OLD_VERSION, "compatibility_verified": True}
                   if scenario == "compatible" else {})
        for index, path in enumerate(slots):
            mark(path, NEW_VERSION, NEW, **options)
            if index == 0 and scenario == "shared":
                os._exit(17)  # Durable shared marker, no owner marker or data write.
        if scenario == "ready":
            # A real child publishes a headless READY receipt and remains alive
            # until the actual confirmation/pointer readback has completed.
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                state = updates.read_json(root / "control/transition.json")
                if state.get("trial_pid") == os.getpid():
                    break
                time.sleep(0.01)
            else:
                raise RuntimeError("ready child was not bound to its real PID")
            proof = {**{name: True for name in updates.READY_CHECKS}, "pid": os.getpid(),
                "sha": NEW, "nonce": state["nonce"], "transition_id": state["transition_id"],
                "ready_at": updates.now_utc()}
            updates.write_json_durable(root / "ready.json", proof)
            while not (root / "release-child").exists():
                time.sleep(0.01)
            return
        if scenario == "unknown":
            slots[0].write_bytes(b"{unreadable-generation")
        (root / "business.bin").write_bytes(b"candidate data preserved")
        os._exit(17)  # READY was never published, for either trial role.

    control = root / "control"
    policy = {"operator_sid": SID, "operator_local_app_data": str(root / "local")}
    if mode != "resume":
        control.mkdir(parents=True)
        (root / "business.bin").write_bytes(b"original business data")
        for path in slots:
            mark(path, OLD_VERSION, OLD)
        if scenario == "unknown-before":
            slots[0].write_bytes(b"{unknown-before-trial")
        state = updates.prepare_transition(previous_sha=OLD, candidate_sha=NEW, packet_sha256="3" * 64,
            forward_pin="4" * 64, rollback_pin="5" * 64, transition_id="6" * 32,
            nonce="7" * 32, now=updates.now_utc())
        if hasattr(updates, "read_trial_generation"):
            state.update(generation_before=updates.read_trial_generation(policy),
                generation_previous_release={"version": OLD_VERSION, "source_commit": OLD},
                generation_candidate_release={"version": NEW_VERSION, "source_commit": NEW})
        state = updates.apply_transition(state, writers_absent=True, packet_verified=True,
                                         durable_prepared=True, now=updates.now_utc())
        updates.persist_switch(control, state, NEW)
        command = [sys.executable, "-B", __file__, "candidate", scenario, role, str(root)]
        if scenario == "ready":
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
            try:
                state = updates.bind_trial(state, sha=NEW, pid=process.pid,
                    ordinary_user=True, now=updates.now_utc())
                updates.write_json_durable(control / "transition.json", state)
                deadline = time.monotonic() + 10
                while not (root / "ready.json").exists() and time.monotonic() < deadline:
                    if process.poll() is not None:
                        raise RuntimeError("ready child exited before its receipt")
                    time.sleep(0.01)
                proof = updates.read_json(root / "ready.json")
                state = updates.confirm_transition(state, proof,
                    process_matches=process.poll() is None and proof["pid"] == process.pid,
                    relay_matches=proof["ordinary_user_relay"] is True, now=updates.now_utc())
            finally:
                (root / "release-child").write_bytes(b"normal child stop")
                output, errors = process.communicate()
            if process.returncode:
                raise RuntimeError("ready child failed its normal stop")
        else:
            result = subprocess.run(command, capture_output=True,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
            if result.returncode != 17:
                raise RuntimeError("candidate harness did not reach the injected exit")
            state = updates.request_rollback(state, reason="candidate startup failed or readiness timed out")
            updates.write_json_durable(control / "transition.json", state)
            if hasattr(updates, "decide_trial_failure"):
                state = updates.decide_trial_failure(state, policy)
                updates.write_json_durable(control / "transition.json", state)
        if mode == "interrupt":
            os._exit(19)  # Decision is durable; active still points to candidate.
    else:
        state = updates.read_json(control / "transition.json")

    if state["phase"] == "ROLLBACK_PENDING":
        state = updates.rollback_transition(state, writers_absent=True, previous_verified=True,
            durable_rollback=True, nonce="8" * 32, now=updates.now_utc())
    selected = updates.recovery_sha(state, pointer_sha=updates.read_json(control / "active.json")["sha"])
    updates.persist_switch(control, state, selected)
    state = updates.read_json(control / "transition.json")
    probe = lambda version, build: generation.probe_downgrade(slots, version,
        app_id="Label_Match", subject_sid=SID, target_build_id=build)["status"]
    observation = {"selected_sha": updates.read_json(control / "active.json")["sha"],
        "phase": state["phase"], "previous_status": probe(OLD_VERSION, OLD),
        "candidate_status": probe(NEW_VERSION, NEW), "role": role,
        "decision": state.get("failure_decision"), "business": (root / "business.bin").read_text(),
        "shared_build": generation._read(slots[0], "Label_Match", SID)["build_id"]
            if scenario not in {"unknown", "unknown-before"} else None,
        "owner_build": generation._read(slots[1], "Label_Match", SID)["build_id"],
        "owner_root": str(slots[1].parent)}
    raw = json.dumps(observation)
    (root / "observation.json").write_text(raw + "\n", encoding="utf-8")
    print(raw)


if __name__ == "__main__":
    run(sys.argv[1], sys.argv[2], sys.argv[3], Path(sys.argv[4]).absolute())

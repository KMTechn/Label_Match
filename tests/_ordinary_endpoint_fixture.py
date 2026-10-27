"""Normal child and relay observations for updater endpoint fixtures."""
import contextlib
import datetime
import json
import os
from pathlib import Path
import subprocess
import sys
import time


def ordinary_child(app, control, observation, arguments):
    import auto_update as updates
    from kmtech_shared import data_generation as generation
    import writer_session_fence as fence

    app, control, observation = Path(app), Path(control), Path(observation)
    policy = updates.read_json(control / "policy.json")
    updates.ordinary_user_context = lambda *_: True
    updates.runtime_matches = lambda *_: True
    updates.update_lock = lambda *a, **k: contextlib.nullcontext()
    updates.process_snapshot = lambda *_: []
    updates.request_bootstrap = lambda *a, **k: None
    updates.process_security = lambda _pid: dict(
        elevated=False, integrity_rid=8192, session_id=1, sid=policy["operator_sid"])
    peer = "--endpoint-peer-relay" in arguments
    arguments = [v for v in arguments if v != "--endpoint-peer-relay"]
    sha = app.parent.name
    gate = updates.startup_gate(app, arguments)
    admitted = False
    if gate is None:
        def no_inverse_lock(*a, **k):
            raise AssertionError("writer admission must not reacquire update ownership")
        updates.update_lock = no_inverse_lock
        with fence.writer_admission("gui_current_state_save"):
            admitted = True
    release = updates.version_manifest(app.parent)
    observed = updates.read_trial_generation(policy)
    decision = generation.probe_downgrade(
        [observed["marker_path"]], release["version"], app_id="Label_Match",
        subject_sid=policy["operator_sid"], target_build_id=sha)
    relay = "--label-match-user-relay" in arguments
    trial = bool(os.environ.get("KMTECH_LM_UPDATE_TRIAL_NONCE"))
    result = dict(sha=sha, gate=gate, status=decision["status"], ordinary=not trial,
                  relay=relay, writer_admitted=admitted, pid=os.getpid())
    if not peer:
        observation.write_text(json.dumps(result), encoding="utf-8")
    if gate is not None or trial:
        return
    stopped = observation.with_name("ordinary-release")
    if relay:
        import user_relay
        from current_user_onboarding import resolve_current_user_onboarding_paths
        paths = resolve_current_user_onboarding_paths(app)
        user_relay.run_persistent_relay_loop(
            lambda: {"status": "IDLE"},
            status_path=user_relay.user_relay_status_path(paths.direct_sync_root),
            stop_requested=stopped.exists,
            metadata=dict(source_commit=sha, ordinary_user=True,
                          process_id=os.getpid(), code_root=str(app)))
    else:
        command = [sys.executable, "-I", "-B", str(app / "main.py"),
                   "--label-match-user-relay", "--endpoint-peer-relay"]
        child = subprocess.Popen(command, creationflags=subprocess.CREATE_NO_WINDOW)
        assert child.wait() == 0


def relay_snapshot(updates, policy, children, *, wait=time.sleep, clock=time.monotonic):
    """Model Observe using only this fixture's actual native relay process."""
    import user_relay
    root = Path(policy["operator_local_app_data"]) / "KMTech/DirectSync/label_match"
    path = user_relay.user_relay_status_path(root)
    deadline = clock() + 30
    while True:
        status = updates.optional_record(path)
        if status and status.get("status") == "RUNNING":
            pid = status.get("process_id")
            identity = updates.native_process_identity(pid)
            if identity.get("status") == "LIVE":
                stamp = datetime.datetime(1601, 1, 1, tzinfo=datetime.timezone.utc)
                stamp += datetime.timedelta(microseconds=int(identity["creation_identity"]) // 10)
                return [dict(pid=pid, observable=True, role="relay",
                             sha=status["source_commit"], sid=policy["operator_sid"],
                             ordinary_user=True, created_at=stamp.isoformat())]
        if not any(child.poll() is None for child in children) or clock() >= deadline:
            return []
        wait(0.01)

"""File-backed native adapters for isolated cross-version product subprocesses.

Only registry/task OS boundaries and the network cycle are replaced. The product
dispatcher, removal, nested admission, stop-marker protocol and relay loop run
their real implementations in separate processes.
"""

import json
import os
from pathlib import Path
import sys

import current_user_scheduled_task as tasks
import user_relay as relay
import writer_session_fence as fence


def report_exception(kind, value, traceback):
    # Keep the reason ahead of the traceback, whose fixture paths can exceed
    # the production Product helper's bounded 500-character diagnostic.
    print(f"{kind.__name__}:{getattr(value, 'code', '')}", file=sys.stderr)
    sys.__excepthook__(kind, value, traceback)


sys.excepthook = report_exception
state = Path(os.environ["LM_TRANSITION_NATIVE_STATE"]).resolve()
assert os.environ.get("KMTECH_LABEL_WRITER_TEST_MODE") == "1"
assert state.is_relative_to(Path(os.environ["TEMP"]).resolve())
assert fence.writer_admission_mutex_name(fence.canonical_control_root()) != fence.WRITER_MUTEX_NAME
registry = state / "registry.txt"
task = state / "task.txt"
relay._registry_delete = lambda: registry.unlink(missing_ok=True)
relay._registry_get = lambda: registry.read_text(encoding="utf-8") if registry.exists() else ""
relay._registry_set = lambda value: registry.write_text(value, encoding="utf-8")
tasks.CANONICAL_ROOT = Path(tasks.__file__).resolve().parents[1]


def delete_registry():
    registry.unlink(missing_ok=True)
    consumed = state / "removal-fault-consumed"
    if os.environ.get("LM_TRANSITION_FAIL_REMOVAL_ONCE") == "1" and not consumed.exists():
        consumed.write_text("after registry removal", encoding="utf-8")
        raise OSError("injected failure after real registry-adapter removal")


relay._registry_delete = delete_registry


def task_operation(operation, spec, *, runner=None):
    assert operation == "Remove"
    task.unlink(missing_ok=True)
    return {
        "schema": tasks.TASK_CONTRACT_VERSION,
        "status": "ABSENT" if not task.exists() else "PRESENT",
        "manual_start": False,
        "process_or_task_stopped": False,
    }


tasks._run_task_operation = task_operation

if "--onboard-current-user" in sys.argv and os.environ.get("LM_TRANSITION_ACTIVATION_FIXTURE") == "1":
    # Seed only the enrollment result. Real persistence helpers, stop release,
    # child launch and relay readback exercise the canonical success/release path.
    import current_user_onboarding as onboarding

    app_root = Path(tasks.__file__).resolve().parents[1]
    paths = onboarding.resolve_current_user_onboarding_paths(app_root)
    with fence.writer_admission("current_user_onboarding"):
        persistence = relay.install_user_relay_autostart(app_root)
        marker_path = relay.user_relay_stop_path(paths.direct_sync_root)
        if os.environ.get("LM_HEALTHY_LIFECYCLE_FIXTURE") == "1":
            onboarding._portable_stop_marker_release_preflight(paths)
        if marker_path.exists():
            marker, _raw, marker_hash = relay.read_stop_marker(marker_path)
            relay.release_user_relay_stop_marker(
                paths.direct_sync_root,
                expected_request_id=marker["request_id"],
                expected_sha256=marker_hash,
            )
        started = relay.start_user_relay_process(app_root)
        onboarding._write_json_atomic(paths.onboarding_report_path, {
            "status": "READY", "action": "FIXTURE_ENROLLMENT",
            "relay_autostart": persistence, "relay_start": started,
        })
    raise SystemExit(0)

if "--label-match-user-relay" in sys.argv:
    direct = Path(os.environ["LABEL_MATCH_DIRECT_SYNC_ROOT"])
    lease = relay._acquire_relay_lease(direct / "user-relay-instance")
    assert lease is not None
    try:
        (state / "relay.pid").write_text(str(os.getpid()), encoding="ascii")

        def cycle():
            (state / "relay-tick.json").write_text(
                json.dumps({"pid": os.getpid()}), encoding="utf-8"
            )
            return {"status": "IDLE"}

        relay.run_persistent_relay_loop(
            cycle,
            status_path=relay.user_relay_status_path(direct),
            interval_seconds=1,
            stop_requested=lambda: relay.user_relay_stop_path(direct).exists(),
        )
    finally:
        lease.close()
    raise SystemExit(0)

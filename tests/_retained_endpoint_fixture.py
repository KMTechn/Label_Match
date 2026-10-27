"""Normal READY inputs and native observations for the retained endpoint test."""
import json
from pathlib import Path
import sqlite3
import time
from types import SimpleNamespace


def observation_path(observation, pid):
    path = Path(observation)
    return path.with_name(path.stem + "." + str(pid) + path.suffix)


def ready_paths(policy, observation, *, create=False):
    root = Path(observation).parent.resolve()
    local = Path(policy["operator_local_app_data"]).resolve()
    assert local.is_relative_to(root)
    data = local / "ready-data"
    direct = local / "KMTech/DirectSync/label_match"
    paths = SimpleNamespace(data_root=data, events_dir=data / "events",
                            queue_dir=data / "queue", ledger_path=data / "ledger.sqlite3",
                            direct_sync_root=direct)
    if create:
        for folder in (paths.events_dir, paths.queue_dir, direct / "queue"):
            folder.mkdir(parents=True, exist_ok=True)
        for database in (paths.ledger_path, direct / "queue/direct_sync_relay.sqlite3"):
            assert database.resolve().is_relative_to(root)
            with sqlite3.connect(database) as connection:
                connection.execute("CREATE TABLE fixture_integrity (value INTEGER)")
        (data / "onboarding-observation.json").write_text(
            json.dumps({"status": "READY"}), encoding="utf-8")
    return paths


def onboarding_observation(paths):
    return json.loads((paths.data_root / "onboarding-observation.json").read_text(encoding="utf-8"))


def native_rows(updates, policy, observation):
    """Observe only the actual processes started by this endpoint fixture."""
    observation = Path(observation)
    rows = []
    for path in observation.parent.glob(observation.stem + ".*" + observation.suffix):
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        pid = record.get("pid")
        if type(pid) is not int or not record.get("writer_admitted"):
            continue
        identity = updates.native_process_identity(pid)
        if identity.get("status") != "LIVE" or not identity.get("creation_identity"):
            continue
        rows.append(dict(pid=pid, observable=True, sha=record["sha"],
                         sid=policy["operator_sid"], ordinary_user=True,
                         role="relay" if record["relay"] else "gui",
                         created_at=updates._retained_native_created_at(identity).isoformat()))
    return rows


def primary_observation(endpoint, *, role, start):
    matches = []
    for child in endpoint.children[start:]:
        path = observation_path(endpoint[-1], child.pid)
        assert path.is_file(), "a launched child did not publish its observation"
        value = json.loads(path.read_text(encoding="utf-8"))
        assert value["pid"] == child.pid
        if not value.get("peer"):
            matches.append(value)
    assert len(matches) == 1, "exactly one non-peer primary process must publish an observation"
    assert matches[0]["relay"] == (role == "relay")
    return matches[0]


def install_retained_endpoint(monkeypatch, updates, endpoint):
    import current_user_onboarding
    from tests._ordinary_endpoint_fixture import relay_snapshot
    control, _, policy, _, _, observation = endpoint
    paths = ready_paths(policy, observation, create=True)
    endpoint.retained_ready = True
    monkeypatch.setattr(current_user_onboarding, "resolve_current_user_onboarding_paths", lambda *_: paths)
    monkeypatch.setattr(current_user_onboarding, "inspect_current_user_state", onboarding_observation)
    monkeypatch.setattr(updates, "process_snapshot", lambda *_: native_rows(updates, policy, observation))
    original_wait = updates.wait_current_relay

    def wait_for_native_ready(*args, **kwargs):
        relay_snapshot(updates, policy, endpoint.children)
        claim = updates.optional_record(Path(policy["staging_root"]) / "trial.json")
        if claim and claim.get("role") == "gui":
            deadline = time.monotonic() + 30
            while True:
                receipt = updates.optional_record(Path(policy["staging_root"]) / "ready.json")
                if receipt and receipt.get("nonce") == claim.get("nonce") and receipt.get("pid") == claim.get("pid"):
                    break
                if time.monotonic() >= deadline or not any(child.poll() is None for child in endpoint.children):
                    raise AssertionError("the live primary GUI did not publish READY")
                time.sleep(0.01)
        return original_wait(*args, **kwargs)

    monkeypatch.setattr(updates, "wait_current_relay", wait_for_native_ready)


def configure_child_ready(updates, policy, observation):
    import current_user_onboarding
    paths = ready_paths(policy, observation)
    current_user_onboarding.resolve_current_user_onboarding_paths = lambda *_: paths
    current_user_onboarding.inspect_current_user_state = onboarding_observation
    updates.process_snapshot = lambda *_: native_rows(updates, policy, observation)
    # All scheduled work remains the parent's existing fake SYSTEM callback.
    updates.request_apply_task = lambda *args, **kwargs: None
    return paths


def publish_gui_after_running(updates, app, paths, stopped):
    from user_relay import user_relay_status_path
    deadline = time.monotonic() + 30
    while not stopped.exists():
        status = updates.optional_record(user_relay_status_path(paths.direct_sync_root))
        if status and status.get("status") == "RUNNING":
            assert updates.publish_gui_ready(app), "the normal SQLite/onboarding/RUNNING inputs must publish READY"
            while not stopped.exists():
                time.sleep(0.01)
            return
        if time.monotonic() >= deadline:
            raise AssertionError("the trial relay did not reach RUNNING")
        time.sleep(0.01)

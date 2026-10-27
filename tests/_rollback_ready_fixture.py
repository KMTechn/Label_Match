"""Publish ordinary previous-relay observations for the real READY observer."""


def previous_relay_observation(updates, control, stage, state, *, sid, now):
    pid = 43
    sha = state["previous_sha"]
    updates.write_json_durable(stage / "trial.json", {
        "nonce": state["nonce"], "transition_id": state["transition_id"],
        "sha": sha, "pid": pid, "role": "relay", "claimed_at": now,
    })
    updates.write_json_durable(stage / "ready.json", {
        **{check: True for check in updates.RELAY_READY_CHECKS},
        "nonce": state["nonce"], "transition_id": state["transition_id"],
        "sha": sha, "pid": pid, "role": "relay", "ready_at": now,
    })
    return [{"pid": pid, "observable": True, "sha": sha, "sid": sid,
             "role": "relay", "ordinary_user": True, "created_at": now}]


def install_previous_relay_observer(monkeypatch, updates, control, policy):
    """Supply the normal previous-relay READY metadata assumed by old scenes."""
    from pathlib import Path

    written = updates.write_json_durable
    stage = Path(policy["staging_root"])

    def publish(path, value):
        written(path, value)
        if path != control / "transition.json" or value.get("phase") != "ROLLBACK_PENDING_READY":
            return
        rows = previous_relay_observation(updates, control, stage, value,
            sid=policy["operator_sid"], now=updates.now_utc())
        observed = updates.process_snapshot
        base = getattr(observed, "_previous_ready_base_snapshot", observed)

        def snapshot(*args):
            existing = base(*args)
            state = updates.optional_record(control / "transition.json")
            return [*existing, *rows] if (state or {}).get("phase") == "ROLLBACK_PENDING_READY" else existing

        snapshot._previous_ready_base_snapshot = base
        monkeypatch.setattr(updates, "process_snapshot", snapshot)

    monkeypatch.setattr(updates, "write_json_durable", publish)

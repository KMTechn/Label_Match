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

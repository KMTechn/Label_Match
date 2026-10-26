"""The durable READY window is shared by SYSTEM, relay and rollback."""
import ast
import json
from contextlib import contextmanager
from pathlib import Path

import pytest

import auto_update as updates
from tests.test_first_setup_owner_readback_mutex import (
    BUSY, _initial_confirmation_case, _startup_trial_case,
    _system_confirmation_case,
)


def _transition(phase):
    # Match prepare_transition's full base record, including its validated forms.
    record = dict(schema=updates.SCHEMA, phase=phase, previous_sha="a" * 40,
                  candidate_sha="b" * 40, packet_sha256="c" * 64,
                  forward_pin="d" * 64, rollback_pin="e" * 64,
                  nonce="1" * 32, transition_id="2" * 32,
                  started_at="2026-01-01T00:00:00+00:00", failed_shas=[], trial_pid=None)
    if phase == "ROLLBACK_PENDING_READY":
        record["rollback_at"] = record["started_at"]
    return record


def _policy(tmp_path):
    return dict(schema="label-match-startup-update-policy-v1", settings_sha256="f" * 64,
                feed_url="https://setup.test.invalid/feed", channel="stable",
                profile_auth=True, apply_during_work=False,
                operator_sid="S-1-5-21-100-200-300-1001",
                operator_local_app_data=str(tmp_path / "operator"),
                staging_root=str(tmp_path / "stage"), hiberboot_enabled=None,
                hiberboot_read_status="UNPROVEN")


def test_system_apply_waits_for_first_ready_after_the_old_cutoff(tmp_path, monkeypatch):
    case = _system_confirmation_case(tmp_path, monkeypatch, [])
    state = _transition("PREPARED")
    rollbacks = []
    monkeypatch.setattr(updates, "load_policy", lambda *_: _policy(tmp_path))
    monkeypatch.setattr(updates, "optional_record", lambda p: dict(state) if p.name == "transition.json" else None)
    monkeypatch.setattr(updates, "read_json", lambda p: dict(schema=updates.SCHEMA, sha="b" * 40,
                        transition_id=state["transition_id"]) if p.name == "active.json" else dict(state))
    monkeypatch.setattr(updates, "write_json_durable", lambda p, value: state.update(value) if p.name == "transition.json" else None)
    monkeypatch.setattr(updates, "defer_stopped_relay", lambda *_: False)
    monkeypatch.setattr(updates, "authorize_prepared_replay", lambda *_: None)
    monkeypatch.setattr(updates, "version_bundle", lambda *_: tmp_path / "candidate")
    monkeypatch.setattr(updates, "apply_transition", lambda current, **fields:
                        {**current, "phase": "APPLIED_PENDING_READY", "applied_at": fields["now"]})
    monkeypatch.setattr(updates, "persist_switch", lambda _c, value, _s: state.update(value))
    def rollback(current, **_):
        rollbacks.append(True)
        (tmp_path / "ready-window-timeout-proof.json").write_text(json.dumps(dict(
            elapsed_seconds=updates.time.monotonic(), requested_rollback=True,
            phase_before=current["phase"])), encoding="utf-8")
        return {**current, "phase": "ROLLBACK_PENDING"}
    monkeypatch.setattr(updates, "request_rollback", rollback)
    monkeypatch.setattr(updates, "settle_trial_failure", lambda *_: {"retained_candidate": True})
    def ready(_c, _p, current, *_a, deadline):
        confirmed = updates.time.monotonic() >= 150 and updates.time.monotonic() < deadline
        if confirmed:
            state["phase"] = "CONFIRMED"
            (tmp_path / "ready-window-confirmed-proof.json").write_text(json.dumps(dict(
                elapsed_seconds=updates.time.monotonic(), deadline=deadline,
                phase=state["phase"])), encoding="utf-8")
        return dict(state), confirmed, False
    monkeypatch.setattr(updates, "observe_trial_ready", ready)
    assert case.run() == 0, (state["phase"], len(rollbacks))
    assert state["phase"] == "CONFIRMED" and not rollbacks


@pytest.mark.parametrize("phase", ["APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"])
def test_initial_wait_keeps_the_existing_anchor_past_120(tmp_path, monkeypatch, phase):
    case = _initial_confirmation_case(tmp_path, monkeypatch, [RuntimeError(BUSY), None], phase=phase, age=150)
    assert case.run() == 0
    assert case.observations == [updates.READY_WINDOW_SECONDS - 150.0]


@pytest.mark.parametrize("phase", ["APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"])
def test_relay_trial_can_acquire_after_120_with_the_original_anchor(tmp_path, monkeypatch, phase):
    case = _startup_trial_case(tmp_path, monkeypatch, [RuntimeError(BUSY), None], phase=phase, age=150)
    assert case.run() is None
    assert len(case.attempts) == 2 and not case.bootstraps


def test_rollback_default_waits_for_first_ready_after_120(tmp_path, monkeypatch):
    clock, writes = [0.0], []
    state = _transition("ROLLBACK_PENDING_READY")
    @contextmanager
    def lock(*_a, **_kw):
        yield
    monkeypatch.setattr(updates, "update_lock", lock)
    monkeypatch.setattr(updates, "read_json", lambda p: dict(schema=updates.SCHEMA, sha="a" * 40,
                        transition_id=state["transition_id"]) if p.name == "active.json" else dict(state))
    monkeypatch.setattr(updates, "optional_record", lambda *_: None)
    monkeypatch.setattr(updates, "write_json_durable", lambda _p, value: writes.append(value["phase"]))
    monkeypatch.setattr(updates, "process_snapshot", lambda *_: [])
    monkeypatch.setattr(updates.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(updates.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    monkeypatch.setattr(updates, "observe_trial_ready", lambda _c, _p, current, *_a, deadline:
                        (current, 150 <= clock[0] < deadline, False))
    policy = _policy(tmp_path)
    assert updates.wait_for_rollback_ready(tmp_path / "control", policy) == 0, writes
    assert clock[0] == 150 and not writes


def test_inherited_rollback_remaining_window_is_not_clipped_at_120(tmp_path, monkeypatch):
    case = _initial_confirmation_case(tmp_path, monkeypatch, [RuntimeError(BUSY), None],
                                      phase="ROLLBACK_PENDING_READY", age=20)
    assert case.run() == 0
    assert case.observations == [updates.READY_WINDOW_SECONDS - 20.0]


def _assert_one_window(tree):
    constants = [n for n in tree.body if isinstance(n, ast.Assign)
                 and any(isinstance(t, ast.Name) and t.id == "READY_WINDOW_SECONDS" for t in n.targets)]
    assert len(constants) == 1 and ast.literal_eval(constants[0].value) == 900
    expected = {"wait_for_rollback_ready": 1, "_apply_prepared": 1, "_initial_ready_lock": 2,
                "_apply_prepared_once": 1, "_startup_trial_lock": 2}
    for name, references in expected.items():
        function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
        assert sum(isinstance(n, ast.Name) and n.id == "READY_WINDOW_SECONDS" for n in ast.walk(function)) == references
        assert not any(isinstance(n, ast.Constant) and type(n.value) in (int, float) and n.value in (120, 240)
                       for n in ast.walk(function))


def test_every_anchored_window_uses_one_constant_and_rejects_a_single_site_drift():
    source = Path(updates.__file__).read_text(encoding="utf-8-sig")
    tree = ast.parse(source)
    _assert_one_window(tree)
    # A partial rollout must fail even if every other site still uses the constant.
    function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_apply_prepared_once")
    site = next(n for n in ast.walk(function) if isinstance(n, ast.BinOp) and isinstance(n.right, ast.Name)
                and n.right.id == "READY_WINDOW_SECONDS")
    site.right = ast.Constant(value=120)
    with pytest.raises(AssertionError):
        _assert_one_window(tree)

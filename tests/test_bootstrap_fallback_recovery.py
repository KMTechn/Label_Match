"""The last bootstrap fallback preserves the ordinary recovery cohort proof."""
import contextlib
import itertools
from types import SimpleNamespace

import pytest

import auto_update as updates
import current_user_onboarding as onboarding


PAST = "2026-10-25T10:00:00Z"
PRESENT = "2026-10-26T10:00:00Z"


@pytest.fixture
def fallback(monkeypatch, tmp_path):
    control = tmp_path / "control"
    control.mkdir()
    sha = "a" * 40
    policy = {"operator_sid": "synthetic-operator", "staging_root": str(tmp_path)}
    state = {"phase": "PREPARED"}
    rows, launches, guidance, readiness = [], [], [], []
    ticks = itertools.chain([0.0], itertools.repeat(updates.STARTUP_WAIT_SECONDS + 1.0))
    monkeypatch.setattr(updates.time, "monotonic", lambda: next(ticks))
    monkeypatch.setattr(updates, "_relay_preparation_deadline", lambda *a: 2000.0)
    monkeypatch.setattr(updates, "_relay_prepare_failure", lambda *a: None)
    monkeypatch.setattr(updates, "control_root", lambda *a: control)
    monkeypatch.setattr(updates, "load_policy", lambda *a: policy)
    monkeypatch.setattr(updates, "ordinary_user_context", lambda *a: True)
    monkeypatch.setattr(onboarding, "_machine_worker_context", lambda *a: None)
    monkeypatch.setattr(updates, "update_lock", lambda *a, **kw: contextlib.nullcontext())
    monkeypatch.setattr(updates, "optional_record", lambda p: state if p.name == "transition.json" else None)
    monkeypatch.setattr(updates, "read_json", lambda p: {"sha": sha})
    monkeypatch.setattr(updates, "current_generation_status", lambda *a: "ALLOW")
    monkeypatch.setattr(updates, "apply_claim_status", lambda *a: "NONE")
    monkeypatch.setattr(updates, "process_snapshot", lambda *a: list(rows))
    monkeypatch.setattr(updates, "native_process_identity", lambda pid:
                        {"status": "LIVE", "creation_identity": "synthetic-birth"})
    monkeypatch.setattr(updates, "_bootstrap_relay_readback", lambda c, p, s, readback, **kw: readback())
    monkeypatch.setattr(updates, "now_utc", lambda: PRESENT)
    monkeypatch.setattr(updates, "write_json_durable", lambda *a: None)
    monkeypatch.setattr(updates, "emit_support_record", lambda *a, **kw: None)
    monkeypatch.setattr(updates, "emit_progress_guidance", lambda: guidance.append(True))
    monkeypatch.setattr(updates, "launch_version", lambda *a, **kw: launches.append(kw) or 713)

    def ready(*a, **kw):
        readiness.append(kw)
        return True

    monkeypatch.setattr(updates, "wait_current_relay", ready)
    monkeypatch.delenv("KMTECH_LM_UPDATE_DEFERRED_AT", raising=False)
    monkeypatch.delenv("KMTECH_LM_RELAY_PARENT", raising=False)
    row = dict(pid=713, observable=True, bootstrap=False, role="relay", sha=sha,
               sid=policy["operator_sid"], ordinary_user=True, created_at=PAST)
    return SimpleNamespace(control=control, sha=sha, policy=policy, state=state, row=row,
                           rows=rows, launches=launches, guidance=guidance, readiness=readiness)


@pytest.mark.parametrize("role", ["relay", "gui"])
def test_fallback_accepts_resident_process_older_than_update(fallback, role):
    f = fallback
    f.rows.append({**f.row, "role": role})
    arguments = ["--label-match-user-relay"] if role == "relay" else []
    assert updates.bootstrap(f.control, arguments, role=role) == 0
    assert not f.guidance
    if role == "relay":
        assert not f.launches
        assert f.readiness[0]["started_at"] == PAST
    else:
        assert f.launches[0]["deferred_at"] == PAST


@pytest.mark.parametrize("change", [
    {"role": "writer"}, {"sha": "b" * 40}, {"sid": "other-operator"},
    {"ordinary_user": False},
], ids=["onboarding-or-finite-writer", "old-sha", "other-operator", "elevated"])
def test_fallback_rejection_returns_guidance_and_20(fallback, change):
    f = fallback
    f.rows.append({**f.row, **change})
    assert updates.bootstrap(f.control, [], role="gui") == 20
    assert f.guidance
    assert not f.launches


def test_fallback_preserves_inherited_recovery_boundary(fallback, monkeypatch):
    f = fallback
    f.rows.append(f.row)
    monkeypatch.setenv("KMTECH_LM_UPDATE_DEFERRED_AT", "2026-10-26T09:00:00Z")
    assert updates.bootstrap(f.control, [], role="gui") == 20
    assert f.guidance
    assert not f.launches

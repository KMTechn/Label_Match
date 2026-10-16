"""Ordinary recovery must supply the relay requested by its recovered GUI."""
import contextlib
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

import auto_update as updates
import current_user_onboarding as onboarding
import user_relay
import writer_session_fence
from writer_session_fence import WriterFencedError


BOUNDARY = "2026-10-16T10:00:00Z"
CREATED = "2026-10-16T10:00:01Z"
PRESENT = "2026-10-16T10:01:00Z"
CASES = ("absent", "failed", "unknown", "request_failure", "request_absent", "stale")
REAL_LAUNCH = updates.launch_version
REAL_PUBLISH_RELAY_READY = updates.publish_relay_ready


@pytest.fixture
def recovery(tmp_path, monkeypatch):
    control = tmp_path / "update-control"
    stage = tmp_path / "staging"
    control.mkdir()
    stage.mkdir()
    sha = "a" * 40
    policy = {"operator_sid": "operator", "staging_root": str(stage)}
    files = [control / name for name in ("active.json", "transition.json", "apply-claim.json")]
    files += [stage / name for name in ("trial.json", "startup-support.json", "trial-failure.json")]
    for path in files:
        path.write_text(json.dumps({"sha": sha}) if path.name == "active.json" else "{}", encoding="utf-8")
    before = {p: p.read_bytes() for p in files}
    parent = dict(pid=os.getpid() + 100, observable=True, bootstrap=False, role="gui",
                  sha=sha, sid=policy["operator_sid"], ordinary_user=True, created_at=CREATED)
    rows, launches, ready_calls = [parent], [], []
    state = {"schema": updates.SCHEMA, "phase": "FAILED", "nonce": "n" * 32}
    clock = [0.0]
    lock = threading.RLock()

    @contextlib.contextmanager
    def ownership(*args, **kwargs):
        with lock:
            yield

    monkeypatch.setattr(updates, "control_root", lambda *_: control)
    monkeypatch.setattr(updates, "load_policy", lambda *_: policy)
    monkeypatch.setattr(updates, "ordinary_user_context", lambda *_: True)
    monkeypatch.setattr(onboarding, "_machine_worker_context", lambda *_: None)
    monkeypatch.setattr(updates, "update_lock", ownership)
    monkeypatch.setattr(writer_session_fence, "writer_admission", ownership)
    monkeypatch.setattr(updates, "optional_record", lambda p: state if p.name == "transition.json" else None)
    monkeypatch.setattr(updates, "prepared_pending", lambda *_: False)
    monkeypatch.setattr(updates, "current_generation_status", lambda *_: "ALLOW")
    monkeypatch.setattr(updates, "process_snapshot", lambda *_: list(rows))
    monkeypatch.setattr(updates, "now_utc", lambda: PRESENT)
    monkeypatch.setenv("KMTECH_LM_UPDATE_DEFERRED_AT", BOUNDARY)
    monkeypatch.delenv("KMTECH_LM_UPDATE_TRIAL_NONCE", raising=False)
    monkeypatch.setattr(updates.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(updates.time, "sleep", lambda delay: clock.__setitem__(0, clock[0] + delay))
    monkeypatch.setattr(updates, "version_manifest", lambda *_: {"source_commit": sha})
    monkeypatch.setattr(updates, "version_bundle", lambda *_: tmp_path / "version")

    def launch(_control, target, arguments, **kwargs):
        assert lock._is_owned()
        assert target == sha
        assert kwargs == {"deferred_at": BOUNDARY}
        assert arguments[0] in ("--label-match-user-relay", "--label-match-direct-sync-relay")
        launches.append(kwargs)
        rows.append({**parent, "pid": parent["pid"] + 1,
                     "role": "writer" if arguments[0] == "--label-match-direct-sync-relay" else "relay"})
        return rows[-1]["pid"]

    def ready(*args, **kwargs):
        assert lock._is_owned()
        ready_calls.append(kwargs)
        relays = [row for row in rows if row["role"] == "relay"]
        return len(relays) == 1 and kwargs.get("pid", relays[0]["pid"]) == relays[0]["pid"]

    monkeypatch.setattr(updates, "launch_version", launch)
    monkeypatch.setattr(updates, "wait_current_relay", ready)
    monkeypatch.setattr(updates, "current_relay_ready", ready)
    monkeypatch.setattr(updates, "start_current", lambda *a, **kw: pytest.fail("recovery relay must not reset its boundary or write support"))
    monkeypatch.setattr(updates, "publish_relay_ready", lambda *a: pytest.fail("ordinary relay has no trial authority"))
    monkeypatch.setattr(updates, "write_json_durable", lambda *a: pytest.fail("recovery relay is read-only"))

    def configure(case):
        clock[0] = 0
        state.clear()
        state.update(schema=updates.SCHEMA, nonce="n" * 32)
        state["phase"] = "APPLIED_PENDING_READY" if case in ("absent", "request_absent") else "FAILED"
        monkeypatch.setattr(updates, "apply_claim_status", lambda *_: "ABSENT" if case == "absent" else "NONE")
        if case == "unknown":
            state["phase"] = "UNKNOWN"
        if case == "request_failure":
            # Change the observed journal at the preparation seam to reach the
            # defensive APPLIED request-failure branch without apply-start proof.
            state["phase"] = "PREPARED"
            def prepared(*_):
                state["phase"] = "APPLIED_PENDING_READY"
                return False
            monkeypatch.setattr(updates, "prepared_pending", prepared)
            monkeypatch.setattr(updates, "startup_action", lambda *a, **kw: "REQUEST_APPLY")
            def fail_request(*_):
                raise OSError("task unavailable")
            monkeypatch.setattr(updates, "request_apply_task", fail_request)
        if case == "request_absent":
            calls = [0]
            def status(*_):
                calls[0] += 1
                return "ABSENT" if calls[0] >= 3 else "NONE"
            monkeypatch.setattr(updates, "apply_claim_status", status)
            monkeypatch.setattr(updates, "startup_action", lambda *a, **kw: "WAIT_FOR_RECOVERY")
            monkeypatch.setattr(updates, "request_apply_task", lambda *_: None)
        if case == "stale":
            monkeypatch.setattr(updates, "apply_claim_status", lambda *_: "UNKNOWN")
            monkeypatch.setattr(updates.time, "sleep", lambda _: clock.__setitem__(0, clock[0] + 300))
            os.utime(control / "apply-claim.json", (1, 1))

    def unchanged():
        assert {p: p.read_bytes() for p in files} == before

    return SimpleNamespace(control=control, policy=policy, state=state, sha=sha, rows=rows,
                           launches=launches, ready_calls=ready_calls, configure=configure,
                           unchanged=unchanged, clock=clock, parent=parent)


@pytest.mark.parametrize("case", CASES)
@pytest.mark.parametrize("mode", ["--label-match-user-relay", "--label-match-direct-sync-relay"])
def test_recovery_gui_gets_one_ordinary_relay(recovery, monkeypatch, case, mode):
    recovery.configure(case)
    assert updates._bootstrap(recovery.control, [mode], role="relay") == 0
    assert recovery.launches == [{"deferred_at": BOUNDARY}]
    if mode == "--label-match-user-relay":
        assert recovery.ready_calls[-1]["started_at"] == BOUNDARY
    else:
        assert recovery.rows[-1]["role"] == "writer"
        assert not recovery.ready_calls
    with monkeypatch.context() as child:
        child.setattr(updates.os, "getpid", lambda: recovery.rows[-1]["pid"])
        assert updates.startup_gate(recovery.control, [mode]) is None
    recovery.unchanged()


@pytest.mark.parametrize("case", CASES)
@pytest.mark.parametrize("failure", ["missing", "invalid", "future", "earlier", "sid", "elevated", "role", "sha", "generation", "active", "unobservable"])
def test_unproved_recovery_never_reports_success(recovery, monkeypatch, case, failure):
    recovery.configure(case)
    if failure == "missing":
        monkeypatch.delenv("KMTECH_LM_UPDATE_DEFERRED_AT")
    elif failure in ("invalid", "future"):
        monkeypatch.setenv("KMTECH_LM_UPDATE_DEFERRED_AT", "invalid" if failure == "invalid" else "2027-01-01T00:00:00Z")
    elif failure == "generation":
        monkeypatch.setattr(updates, "current_generation_status", lambda *_: "UNKNOWN")
    elif failure == "active":
        reads = [0]
        def active(*_):
            reads[0] += 1
            return {"sha": recovery.sha if reads[0] == 1 else "b" * 40}
        monkeypatch.setattr(updates, "read_json", active)
    else:
        fields = {"earlier": ("created_at", "2026-10-16T09:59:59Z"), "sid": ("sid", "other"),
                  "elevated": ("ordinary_user", False), "role": ("role", "writer"),
                  "sha": ("sha", "b" * 40), "unobservable": ("observable", False)}
        key, value = fields[failure]
        recovery.rows[0][key] = value
    assert updates._bootstrap(recovery.control, ["--label-match-user-relay"], role="relay") == 20
    assert not recovery.launches
    recovery.unchanged()


@pytest.mark.parametrize("case", CASES)
@pytest.mark.parametrize("boundary", [True, False])
def test_live_matching_relay_is_reused(recovery, monkeypatch, case, boundary):
    recovery.configure(case)
    recovery.rows.append({**recovery.parent, "pid": recovery.parent["pid"] + 1, "role": "relay"})
    if not boundary:
        monkeypatch.delenv("KMTECH_LM_UPDATE_DEFERRED_AT")
    assert updates._bootstrap(recovery.control, ["--label-match-user-relay"], role="relay") == 0
    assert not recovery.launches
    recovery.unchanged()


@pytest.mark.parametrize("case", CASES)
def test_gui_duplicate_keeps_existing_result(recovery, case):
    recovery.configure(case)
    assert updates._bootstrap(recovery.control, [], role="gui") == 0
    assert not recovery.launches
    recovery.unchanged()


def test_concurrent_relay_requests_launch_once(recovery):
    recovery.configure("failed")
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: updates._bootstrap(recovery.control, ["--label-match-user-relay"], role="relay"), range(2)))
    assert results == [0, 0]
    assert len(recovery.launches) == 1
    recovery.unchanged()


def test_launch_failure_can_retry_without_mutating_state(recovery, monkeypatch):
    recovery.configure("failed")
    launch = updates.launch_version
    def fail(*a, **kw):
        raise OSError("launch interrupted")
    monkeypatch.setattr(updates, "launch_version", fail)
    assert updates._bootstrap(recovery.control, ["--label-match-user-relay"], role="relay") == 20
    recovery.unchanged()
    monkeypatch.setattr(updates, "launch_version", launch)
    assert updates._bootstrap(recovery.control, ["--label-match-user-relay"], role="relay") == 0
    assert len(recovery.launches) == 1


def test_recovery_relay_timeout_is_failure(recovery, monkeypatch):
    recovery.configure("failed")
    attempts = []
    def timeout(*a, **kw):
        attempts.append(1)
        recovery.clock[0] += 100
        raise WriterFencedError("WRITER_GATE_TIMEOUT", "busy")
    monkeypatch.setattr(updates, "launch_version", timeout)
    assert updates._bootstrap(recovery.control, ["--label-match-user-relay"], role="relay") == 20
    assert len(attempts) == 3
    recovery.unchanged()


def test_recovery_relay_readiness_failure_is_not_success(recovery, monkeypatch):
    recovery.configure("failed")
    monkeypatch.setattr(updates, "wait_current_relay", lambda *a, **kw: False)
    assert updates._bootstrap(recovery.control, ["--label-match-user-relay"], role="relay") == 20
    assert len(recovery.launches) == 1
    recovery.unchanged()


def test_onboarding_launcher_reaches_alive_via_real_bootstrap(recovery, monkeypatch):
    recovery.configure("failed")
    monkeypatch.setattr(onboarding, "resolve_current_user_onboarding_paths", lambda *_: SimpleNamespace(direct_sync_root=recovery.control))
    def owner(*a, **kw):
        relays = [r for r in recovery.rows if r["role"] == "relay"]
        return {"process_id": relays[0]["pid"], "updated_at": PRESENT} if relays else None
    monkeypatch.setattr(user_relay, "_existing_user_relay_owner", owner)
    def start(command, **kwargs):
        assert "--bootstrap" in command and command[command.index("--role") + 1] == "relay"
        result = updates._bootstrap(recovery.control, ["--label-match-user-relay"], role="relay")
        return SimpleNamespace(pid=os.getpid() + 200, poll=lambda: result)
    monkeypatch.setattr(updates, "start_ordinary_process", start)
    def wait(seconds):
        recovery.clock[0] += seconds
    result = user_relay.reuse_or_start_user_relay_process(recovery.control, survival_seconds=0, wait=wait)
    assert result["status"] == "ALIVE"
    assert len(recovery.launches) == 1
    recovery.unchanged()


def test_real_launch_preserves_environment_and_child_startup_gate(recovery, monkeypatch, tmp_path):
    recovery.configure("failed")
    app = tmp_path / "version/app"
    app.mkdir(parents=True)
    (app / "label_match_relay_entry.py").touch()
    started = []
    def start(command, *, cwd, environment):
        started.append(True)
        assert environment.get("KMTECH_LM_UPDATE_DEFERRED_AT") == BOUNDARY
        assert "KMTECH_LM_UPDATE_TRIAL_NONCE" not in environment
        assert command[1:3] == ["-I", "-B"]
        assert command[-1] == "--label-match-user-relay"
        row = {**recovery.parent, "pid": recovery.parent["pid"] + 1, "role": "relay"}
        recovery.rows.append(row)
        monkeypatch.setenv("KMTECH_LM_UPDATE_DEFERRED_AT", environment["KMTECH_LM_UPDATE_DEFERRED_AT"])
        assert updates.startup_gate(app, [command[-1]]) is None
        return SimpleNamespace(pid=row["pid"])
    monkeypatch.setattr(updates, "launch_version", REAL_LAUNCH)
    monkeypatch.setattr(updates, "start_ordinary_process", start)
    assert updates._bootstrap(recovery.control, ["--label-match-user-relay"], role="relay") == 0
    assert started == [True]
    recovery.unchanged()


@pytest.mark.parametrize("mismatch", ["pid", "nonce"])
def test_ordinary_relay_cannot_publish_trial_ready(recovery, monkeypatch, mismatch):
    recovery.configure("absent")
    assert updates._bootstrap(recovery.control, ["--label-match-user-relay"], role="relay") == 0
    pid = recovery.rows[-1]["pid"]
    recovery.state.update(candidate_sha=recovery.sha, transition_id="transition")
    claim = dict(role="relay", pid=pid, nonce=recovery.state["nonce"], transition_id="transition")
    claim[mismatch] = pid + 1 if mismatch == "pid" else "different"
    monkeypatch.setattr(updates, "optional_record", lambda p: recovery.state if p.name == "transition.json" else claim)
    assert REAL_PUBLISH_RELAY_READY(recovery.control, pid) is False
    recovery.unchanged()

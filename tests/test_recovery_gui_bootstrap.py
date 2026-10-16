"""G41/G45 recovery cohorts; all observation and process I/O are in memory."""
import copy
from pathlib import Path

import pytest

from recovery_memory_world import MemoryPath, World
from test_recovery_relay_bootstrap import CASES, recovery, updates


GUI_CASES = [(phase, claim) for phase in ("FAILED", "UNKNOWN", "DEFERRED")
             for claim in ("NONE", "ABSENT")] + [
    ("APPLIED_PENDING_READY", "ABSENT"), ("ROLLBACK_PENDING_READY", "ABSENT")]


def memory_world(phase="FAILED", claim="NONE", writer="relay"):
    source = (Path(__file__).resolve().parents[1] / "auto_update.py").read_text(encoding="utf-8-sig")
    w = World("LM-GUIFIX", source, phase=phase, writer=writer, claim=claim)
    if phase == "APPLIED_PENDING_READY":
        w.files[str(w.control / "active.json")]["sha"] = w.candidate
        for row in w.rows:
            row["sha"] = w.candidate
    w.gui_environments = []
    spawn = w.spawn

    def launch(argv, **kwargs):
        process = spawn(argv, **kwargs)
        if w.launches[-1]["role"] == "gui":
            environment = kwargs.get("environment", kwargs.get("env", {}))
            w.gui_environments.append(dict(environment))
            saved_env, saved_pid = w.os.environ, w.os.getpid
            w.os.environ, w.os.getpid = environment, lambda: process.pid
            try:
                assert w.ns["startup_gate"](MemoryPath("/bundle") / w.active / "app", []) is None
            finally:
                w.os.environ, w.os.getpid = saved_env, saved_pid
        return process

    w.ns["start_ordinary_process"] = launch
    return w


@pytest.mark.parametrize("phase,claim", GUI_CASES)
def test_relay_only_recovery_starts_one_gui_at_verified_relay_boundary(phase, claim):
    w = memory_world(phase, claim)
    boundary = w.rows[0]["created_at"]
    before = copy.deepcopy(w.files)
    result = w.bootstrap()
    assert result["exception"] is None
    assert result["rc"] == 0
    assert [row["role"] for row in w.launches] == ["gui"]
    assert w.gui_environments[0]["KMTECH_LM_UPDATE_DEFERRED_AT"] == boundary
    assert "KMTECH_LM_UPDATE_TRIAL_NONCE" not in w.gui_environments[0]
    assert w.files == before


@pytest.mark.parametrize("phase,claim", GUI_CASES)
def test_existing_gui_is_reused_without_second_gui(phase, claim):
    w = memory_world(phase, claim, writer="gui")
    before = copy.deepcopy(w.files)
    result = w.bootstrap()
    assert result["exception"] is None
    assert result["rc"] == 0
    assert not w.launches
    assert w.files == before


@pytest.mark.parametrize("phase,claim", GUI_CASES)
@pytest.mark.parametrize("bad", ["sha", "sid", "privilege", "unobservable", "role", "two_relays",
                                 "future", "malformed_time", "missing_time", "generation_denied",
                                 "generation_unknown", "snapshot_unreadable"])
def test_unproved_relay_never_authorizes_a_gui(phase, claim, bad):
    w = memory_world(phase, claim)
    row = w.rows[0]
    if bad == "sha": row["sha"] = w.previous if w.active == w.candidate else w.candidate
    elif bad == "sid": row["sid"] = w.policy["operator_sid"] + "-other"
    elif bad == "privilege": row["ordinary_user"] = False
    elif bad == "unobservable": row["observable"] = False
    elif bad == "role": row["role"] = "writer"
    elif bad == "two_relays": w.rows.append({**row, "pid": row["pid"] + 1})
    elif bad == "future": row["created_at"] = w.at(w.clock + 1)
    elif bad == "malformed_time": row["created_at"] = "invalid"
    elif bad == "missing_time": row.pop("created_at")
    elif bad == "generation_denied": w.guard = "REFUSE"
    elif bad == "generation_unknown": w.guard = "UNKNOWN"
    elif bad == "snapshot_unreadable":
        def unreadable(*args): raise OSError("observation unavailable")
        w.ns["process_snapshot"] = unreadable
    before = copy.deepcopy(w.files)
    result = w.bootstrap()
    assert not w.launches
    assert result["rc"] == 20
    assert result["exception"] is None
    assert w.files == before


def test_gui_boundary_comes_from_the_rows_actually_proved():
    w = memory_world()
    snapshot = w.snapshot
    calls = 0

    def observation(control):
        nonlocal calls
        calls += 1
        if calls == 2:
            w.rows[0] = {**w.rows[0], "pid": w.rows[0]["pid"] + 1,
                         "created_at": w.at(w.clock - 1)}
        return snapshot(control)

    w.ns["process_snapshot"] = observation
    result = w.bootstrap()
    assert result["rc"] == 0 and result["exception"] is None
    assert len(w.launches) == 1
    assert w.gui_environments[0]["KMTECH_LM_UPDATE_DEFERRED_AT"] == w.at(w.clock - 1)


def test_run_relay_then_system_deferred_then_gui_uses_same_cohort():
    w = memory_world(writer=None)
    assert w.bootstrap("relay")["rc"] == 0
    assert [row["role"] for row in w.launches] == ["relay"]
    w.clock += 1
    assert w.call("_apply_prepared", w.control)["rc"] == 0
    w.ns["complete_apply_claim"](w.control)
    assert w.state["phase"] == "DEFERRED" and w.state["recovery_pending"] is True
    before = copy.deepcopy(w.files)
    assert w.bootstrap()["rc"] == 0
    assert [row["role"] for row in w.launches] == ["relay", "gui"]
    assert w.files == before


@pytest.mark.parametrize("phase,claim", [("PREPARED", "ABSENT"), ("ROLLBACK_PENDING", "ABSENT"),
                                        ("APPLIED_PENDING_READY", "NONE"), ("APPLIED_PENDING_READY", "LIVE"),
                                        ("ROLLBACK_PENDING_READY", "NONE"), ("ROLLBACK_PENDING_READY", "LIVE")])
def test_excluded_states_never_get_an_ordinary_gui(phase, claim):
    w = memory_world(phase, claim)
    w.request_mode = "nothing"
    result = w.bootstrap()
    assert not any(row["role"] == "gui" and not row["trial"] for row in w.launches)


@pytest.mark.parametrize("phase", ["FAILED", "DEFERRED", "APPLIED_PENDING_READY"])
@pytest.mark.parametrize("generation", ["UNCHANGED", "ADVANCED"])
def test_gui_and_relay_defer_system_until_writer_absence_then_settle(phase, generation):
    w = memory_world(phase, "ABSENT")
    before_gui = copy.deepcopy(w.files)
    assert w.bootstrap()["rc"] == 0
    assert w.files == before_gui
    assert w.call("_apply_prepared", w.control)["rc"] == 0
    w.ns["complete_apply_claim"](w.control)
    assert w.state["phase"] == "DEFERRED" and w.state["recovery_pending"] is True
    preserved = copy.deepcopy(w.state)
    active = w.active
    assert w.call("_apply_prepared", w.control)["rc"] == 0
    w.ns["complete_apply_claim"](w.control)
    assert w.state == preserved and w.active == active
    assert not any(str(path).endswith("ready.json") for path in w.files)
    w.rows = []
    w.clock += 500
    old_rollback = w.state["rollback_at"]
    if generation == "ADVANCED":
        release = w.state["generation_candidate_release"]
        w.generation["record"] = {"build_id": w.candidate, "version": release["version"]}
    w.auto_ready = True
    result = w.call("_apply_prepared", w.control)
    assert result["exception"] is None
    if generation == "UNCHANGED":
        assert result["rc"] == 0
        assert w.state["phase"] == "ROLLED_BACK" and w.active == w.previous
        assert w.state["rollback_at"] != old_rollback
        assert w.state["failure_decision"]["code"] == "TRIAL_GENERATION_UNCHANGED"
    else:
        assert w.state["phase"] == "DEFERRED" and w.state["retained_candidate"] is True
        assert w.active == w.candidate
        assert w.state["failure_decision"]["code"] == "TRIAL_GENERATION_ADVANCED"
    assert not w.state["recovery_pending"]


@pytest.mark.parametrize("generation", ["UNCHANGED", "ADVANCED"])
def test_applied_absent_gui_recovery_keeps_writer_zero_settlement_decision(generation):
    decisions = []
    for with_relay in (False, True):
        w = memory_world("APPLIED_PENDING_READY", "ABSENT", writer="relay" if with_relay else None)
        if with_relay:
            assert w.bootstrap()["rc"] == 0
        assert w.call("_apply_prepared", w.control)["exception"] is None
        w.ns["complete_apply_claim"](w.control)
        w.rows = []
        w.clock += 500
        if generation == "ADVANCED":
            release = w.state["generation_candidate_release"]
            w.generation["record"] = {"build_id": w.candidate, "version": release["version"]}
        w.auto_ready = True
        assert w.call("_apply_prepared", w.control)["exception"] is None
        decisions.append((w.state["failure_decision"]["code"], w.active))
    assert decisions[0] == decisions[1]


@pytest.mark.parametrize("bound_gui", [False, True])
@pytest.mark.parametrize("receipt_present", [False, True])
@pytest.mark.parametrize("extra_trial_relay", [False, True])
def test_real_system_guard_and_ready_consumer_cohort_matrix(bound_gui, receipt_present, extra_trial_relay):
    w = memory_world("APPLIED_PENDING_READY", "LIVE", writer=None)
    w.ready_trial()
    gui, relay = w.rows
    w.state.update(trial_pid=gui["pid"], trial_sha=w.active,
                   trial_started_at=gui["created_at"], trial_role="gui")
    w.rows = ([gui] if bound_gui else []) + [relay]
    if extra_trial_relay:
        w.rows.append({**relay, "pid": relay["pid"] + 1})
    if not receipt_present:
        w.files.pop(str(w.stage / "ready.json"))
    observed = []
    actual = w.ns["observe_trial_ready"]

    def observe(*args, **kwargs):
        observed.append(True)
        return actual(*args, **kwargs)

    w.ns["observe_trial_ready"] = observe
    result = w.call("_apply_prepared_once", w.control)
    assert result["exception"] is None
    if not bound_gui:
        assert not observed
        assert w.state["phase"] == "DEFERRED" and w.state["recovery_pending"] is True
    else:
        assert observed
        assert (w.state["phase"] == "CONFIRMED") is (receipt_present and not extra_trial_relay)


def test_proof_snapshot_new_relay_is_reused_without_second_launch(recovery, monkeypatch):
    r = recovery
    r.configure(CASES[0])
    original_snapshot = updates.process_snapshot
    observations = 0

    def snapshot(control):
        nonlocal observations
        observations += 1
        if observations == 2:
            # Native process observations can change independently of the mutex.
            # The second observation is the actual ordinary-recovery proof.
            sibling = dict(r.parent)
            sibling["role"] = "relay"
            sibling["pid"] = max(row["pid"] for row in r.rows) + 1
            r.rows.append(sibling)
        return original_snapshot(control)

    monkeypatch.setattr(updates, "process_snapshot", snapshot)
    result = updates._bootstrap(
        r.control, ["--label-match-user-relay"], role="relay"
    )
    # Once the real proof sees an already-live cohort relay, launching another
    # violates the singleton contract even if readiness then refuses success.
    assert result == 0
    assert not r.launches
    assert sum(row.get("role") == "relay" for row in r.rows) == 1
    r.unchanged()

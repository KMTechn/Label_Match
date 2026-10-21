"""Review-119 boundaries through the real bootstrap and parent wait algorithms."""
import pytest

import auto_update
from test_bootstrap_relay_prepare_deadline import bootstrap_relay as _bootstrap_relay_fixture


BRANCHES = ["retained_deferred", "request_deferred", "absent_claim", "failed_recovery"]
NO_PARENT = object()


@pytest.fixture
def branch_bootstrap(tmp_path):
    actual_bootstrap = auto_update._bootstrap

    def run(kind, ready_after, *, setup_delay=0, entry_work=0, busy_until=0,
            parent_deadline=NO_PARENT, parent_pid=42, parent_dies_at=None):
        phase = {"retained_deferred": "DEFERRED", "failed_recovery": "FAILED"}.get(kind, "CONFIRMED")

        def enter(monkeypatch, root, arguments, *, role):
            control = auto_update.control_root(root)
            transition = auto_update.optional_record(control / "transition.json")
            active = auto_update.read_json(control / "active.json")
            if kind == "retained_deferred":
                transition.update(retained_candidate=True, previous_sha="a" * 40,
                                  candidate_sha=active["sha"],
                                  failure_decision={"selected_sha": active["sha"]})
            if kind == "absent_claim":
                monkeypatch.setattr(auto_update, "apply_claim_status", lambda _: "ABSENT")
            if kind in {"absent_claim", "failed_recovery"} or busy_until:
                monkeypatch.setattr(auto_update, "current_generation_status", lambda *a: "ALLOW")
            if kind == "request_deferred":
                request = auto_update.request_apply_task

                def defer(*args):
                    request(*args)
                    transition.update(phase="DEFERRED", deferred_at=auto_update.now_utc(),
                                      reason_code="UPDATE_PREPARATION_FAILED")

                monkeypatch.setattr(auto_update, "request_apply_task", defer)
            if setup_delay:
                auto_update.time.sleep(setup_delay)
            if busy_until:
                update_lock = auto_update.update_lock
                available_at = auto_update.time.monotonic() + busy_until

                def contested_lock(*args, **kwargs):
                    if auto_update.time.monotonic() < available_at:
                        raise RuntimeError("update mutex ownership is busy")
                    return update_lock(*args, **kwargs)

                monkeypatch.setattr(auto_update, "update_lock", contested_lock)
            if entry_work:
                load_policy = auto_update.load_policy

                def delayed_policy(*args):
                    auto_update.time.sleep(entry_work)
                    return load_policy(*args)

                monkeypatch.setattr(auto_update, "load_policy", delayed_policy)
            context = {}
            if parent_deadline is not NO_PARENT:
                context = {"parent_deadline": parent_deadline, "parent_pid": parent_pid}
                monkeypatch.setattr(auto_update.os, "getppid", lambda: 42)

                def identity(pid):
                    if pid == 42 and parent_dies_at is not None and auto_update.time.monotonic() >= 1000 + parent_dies_at:
                        return {"status": "ABSENT"}
                    return {"status": "LIVE", "creation_identity": "100" if pid == 42 else "200"}

                monkeypatch.setattr(auto_update, "native_process_identity", identity)
            return actual_bootstrap(root, arguments, role=role, **context)

        # Each attempt starts with the real functions and its own patch scope.
        with pytest.MonkeyPatch.context() as monkeypatch:
            bootstrap_relay = _bootstrap_relay_fixture.__wrapped__(monkeypatch, tmp_path)
            monkeypatch.setattr(auto_update, "_bootstrap",
                                lambda root, arguments, *, role: enter(monkeypatch, root, arguments, role=role))
            return bootstrap_relay(phase, ready_after=ready_after, prepared=kind == "request_deferred")

    return run


@pytest.mark.parametrize("kind", BRANCHES)
@pytest.mark.parametrize("ready_after", [0, 300], ids=["immediate-control", "slow-within-relay-budget"])
def test_current_relay_branches_share_the_bootstrap_budget(branch_bootstrap, kind, ready_after):
    observed = branch_bootstrap(kind, ready_after)
    assert observed.result == 0
    assert observed.elapsed == pytest.approx(ready_after)
    assert len(observed.current_starts) == len(observed.readiness_waits) == 1


@pytest.mark.parametrize("kind", BRANCHES)
def test_dead_current_relay_exhausts_only_one_child_budget(branch_bootstrap, kind):
    observed = branch_bootstrap(kind, float("inf"))
    assert observed.result == 20
    assert observed.elapsed == pytest.approx(600)
    assert len(observed.launches) == len(observed.readiness_waits) == 1


def test_bootstrap_entry_work_consumes_the_shared_budget(branch_bootstrap):
    observed = branch_bootstrap("normal", float("inf"), entry_work=5)
    assert observed.result == 20
    assert observed.elapsed == pytest.approx(600)
    assert observed.readiness_waits[0]["remaining"] == pytest.approx(595)


def test_expired_child_budget_does_not_start_a_new_relay(branch_bootstrap):
    observed = branch_bootstrap("normal", 0, entry_work=601)
    assert observed.result == 20
    assert observed.launches == []
    assert observed.elapsed == pytest.approx(601)


@pytest.mark.parametrize("ready_after,expected", [(500, 0), (float("inf"), 20)],
                         ids=["ready-in-original-budget", "no-budget-renewal"])
def test_repeated_update_contention_keeps_one_relay_deadline(branch_bootstrap, ready_after, expected):
    observed = branch_bootstrap("normal", ready_after, busy_until=300)
    assert observed.result == expected
    assert observed.readiness_waits[0]["started_at"] == pytest.approx(1240)
    assert observed.readiness_waits[0]["deadline"] == pytest.approx(1600)
    assert observed.elapsed == pytest.approx(ready_after if expected == 0 else 600)


@pytest.mark.parametrize("entry_delay", [0, 61, 120])
@pytest.mark.parametrize("prepare_seconds,expected", [(239, 0), (241, 0), (599, 20)])
def test_parent_budget_matrix(branch_bootstrap, entry_delay, prepare_seconds, expected):
    observed = branch_bootstrap("normal", entry_delay + prepare_seconds,
                                setup_delay=entry_delay, parent_deadline="1600")
    assert observed.result == expected
    deadline = observed.current_starts[0]["input_deadline"]
    assert deadline < 1600
    assert observed.readiness_waits[0]["deadline"] == deadline
    assert observed.elapsed <= 600
    if expected == 0:
        assert observed.elapsed == pytest.approx(entry_delay + prepare_seconds)
    else:
        assert observed.elapsed == pytest.approx(deadline - 1000)


@pytest.mark.parametrize("entry_delay", [59.9, 60, 60.1])
def test_parent_budget_has_no_sixty_second_reset(branch_bootstrap, entry_delay):
    observed = branch_bootstrap("normal", entry_delay + 241,
                                setup_delay=entry_delay, parent_deadline="1600")
    assert observed.result == 0
    assert observed.current_starts[0]["input_deadline"] < 1600
    assert observed.elapsed == pytest.approx(entry_delay + 241)


@pytest.mark.parametrize("entry_work,expected", [(5, 0), (480, 20)])
def test_parent_budget_includes_child_initialization(branch_bootstrap, entry_work, expected):
    observed = branch_bootstrap("normal", 61 + entry_work + 241,
                                setup_delay=61, entry_work=entry_work, parent_deadline="1600")
    assert observed.result == expected
    assert observed.elapsed <= 600
    assert observed.current_starts[0]["input_deadline"] < 1600


@pytest.mark.parametrize("value", ["not-a-number", "nan", "inf", "-inf", "", None])
def test_parent_value_errors_never_issue_an_independent_budget(branch_bootstrap, value):
    observed = branch_bootstrap("normal", 0, parent_deadline=value)
    assert observed.result == 20
    assert observed.elapsed == 0
    assert observed.launches == []
    assert observed.current_starts == observed.readiness_waits == []


def test_past_parent_deadline_is_already_exhausted(branch_bootstrap):
    observed = branch_bootstrap("normal", 0, parent_deadline="999")
    assert observed.result == 20
    assert observed.launches == observed.readiness_waits == []
    assert observed.elapsed == 0


def test_parent_death_stops_preparation_before_deadline(branch_bootstrap):
    observed = branch_bootstrap("normal", 300, parent_deadline="1600", parent_dies_at=30)
    assert observed.result == 20
    assert observed.elapsed <= 30.1
    assert len(observed.launches) <= 1


def test_parent_dead_before_entry_cannot_start_a_relay(branch_bootstrap):
    observed = branch_bootstrap("normal", 0, setup_delay=61,
                                parent_deadline="1600", parent_dies_at=30)
    assert observed.result == 20
    assert observed.launches == []
    assert observed.elapsed == pytest.approx(61)


def test_restart_with_same_parent_context_cannot_renew_budget(branch_bootstrap):
    first = branch_bootstrap("normal", float("inf"), parent_deadline="1600")
    assert first.result == 20 and first.elapsed < 600
    restarted = branch_bootstrap("normal", 0, setup_delay=600, parent_deadline="1600")
    assert restarted.result == 20
    assert restarted.launches == restarted.readiness_waits == []
    assert restarted.elapsed == pytest.approx(600)

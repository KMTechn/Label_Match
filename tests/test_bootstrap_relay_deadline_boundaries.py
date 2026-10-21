"""Review-119 boundaries through the real bootstrap and parent wait algorithms."""
import pytest

import auto_update
from test_bootstrap_relay_prepare_deadline import bootstrap_relay


BRANCHES = ["retained_deferred", "request_deferred", "absent_claim", "failed_recovery"]


@pytest.fixture
def branch_bootstrap(monkeypatch, bootstrap_relay):
    actual_bootstrap = auto_update._bootstrap

    def run(kind, ready_after, *, setup_delay=0, entry_work=0, busy_until=0):
        phase = {"retained_deferred": "DEFERRED", "failed_recovery": "FAILED"}.get(kind, "CONFIRMED")

        def enter(root, arguments, *, role):
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
            return actual_bootstrap(root, arguments, role=role)

        monkeypatch.setattr(auto_update, "_bootstrap", enter)
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

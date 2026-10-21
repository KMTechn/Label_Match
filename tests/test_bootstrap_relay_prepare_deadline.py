"""Exercise the first-setup relay wait through the actual bootstrap boundary."""
from contextlib import nullcontext
from datetime import datetime, timezone
import json
from types import SimpleNamespace

import pytest

import auto_update
import current_user_onboarding as onboarding
from user_relay import user_relay_status_path


@pytest.fixture
def bootstrap_relay(monkeypatch, tmp_path):
    def run(phase, *, ready_after, prepared=False):
        control = tmp_path / "update-control"
        direct_sync = tmp_path / "direct-sync"
        sha = "b" * 40
        pid = auto_update.os.getpid() + 1
        policy = {"operator_sid": "test-operator", "staging_root": str(tmp_path / "stage")}
        started_at = datetime(2026, 10, 21, tzinfo=timezone.utc).isoformat()
        state = {"schema": auto_update.SCHEMA, "phase": phase}
        active = {"sha": sha}
        initial = 1000.0
        clock = [initial]
        candidate = [prepared]
        launches, requests, reports, rows = [], [], [], []
        actions, current_starts, readiness_waits = [], [], []
        startup_action = auto_update.startup_action
        start_current = auto_update.start_current
        wait_current_relay = auto_update.wait_current_relay
        status_path = user_relay_status_path(direct_sync)
        status = dict(status="RUNNING", source_commit=sha, ordinary_user=True,
                      process_id=pid, updated_at=started_at,
                      code_root=str(auto_update.version_bundle(control, sha) / "app"))

        def read_record(path):
            if path == control / "active.json":
                return active
            if path == control / "transition.json":
                return state
            if path == status_path and launches and clock[0] >= initial + ready_after:
                return status
            return None

        def sleep(seconds):
            clock[0] = round(clock[0] + seconds, 6)

        def launch(root, selected, arguments, **kwargs):
            assert root == control and selected == sha
            assert kwargs == {"deferred_at": started_at}
            launches.append(list(arguments))
            rows.append(dict(pid=pid, role="relay", sha=sha, sid=policy["operator_sid"],
                             ordinary_user=True, observable=True, created_at=started_at))
            return pid

        def request(root, selected_policy):
            assert root == control and selected_policy is policy
            requests.append(clock[0])
            candidate[0] = False
            state["phase"] = "CONFIRMED"

        def observed_action(transition, **kwargs):
            action = startup_action(transition, **kwargs)
            actions.append(dict(phase=transition["phase"], prepared=kwargs["prepared_present"],
                                action=action))
            return action

        def observed_current(*args, **kwargs):
            current_starts.append(dict(started_at=clock[0], input_deadline=kwargs.get("deadline")))
            return start_current(*args, **kwargs)

        def observed_wait(*args, **kwargs):
            readiness_waits.append(dict(started_at=clock[0], deadline=kwargs["deadline"],
                                        remaining=kwargs["deadline"] - clock[0]))
            return wait_current_relay(*args, **kwargs)

        monkeypatch.setattr(auto_update.time, "monotonic", lambda: clock[0])
        monkeypatch.setattr(auto_update.time, "sleep", sleep)
        monkeypatch.setattr(auto_update, "now_utc", lambda: started_at)
        monkeypatch.setattr(auto_update, "control_root", lambda root: control)
        monkeypatch.setattr(auto_update, "load_policy", lambda root: policy)
        monkeypatch.setattr(auto_update, "ordinary_user_context", lambda sid: sid == policy["operator_sid"])
        monkeypatch.setattr(auto_update, "update_lock", lambda *a, **k: nullcontext())
        monkeypatch.setattr(auto_update, "optional_record", read_record)
        monkeypatch.setattr(auto_update, "read_json", read_record)
        monkeypatch.setattr(auto_update, "startup_action", observed_action)
        monkeypatch.setattr(auto_update, "start_current", observed_current)
        monkeypatch.setattr(auto_update, "wait_current_relay", observed_wait)
        monkeypatch.setattr(auto_update, "prepared_pending", lambda *a: candidate[0])
        monkeypatch.setattr(auto_update, "relay_stop_requested", lambda selected_policy: False)
        monkeypatch.setattr(auto_update, "launch_version", launch)
        monkeypatch.setattr(auto_update, "request_apply_task", request)
        monkeypatch.setattr(auto_update, "process_snapshot", lambda root: list(rows))
        monkeypatch.setattr(auto_update, "write_json_durable", lambda path, payload: reports.append((path, payload)))
        monkeypatch.setattr(auto_update, "emit_support_record", lambda *a, **k: None)
        monkeypatch.setattr(onboarding, "_machine_worker_context", lambda root: None)
        monkeypatch.setattr(onboarding, "resolve_current_user_onboarding_paths",
                            lambda root: SimpleNamespace(direct_sync_root=direct_sync))

        result = auto_update._bootstrap(tmp_path / "current" / "app",
                                       ["--label-match-user-relay"], role="relay")
        observation = dict(phase=phase, prepared_on_entry=prepared, relay_ready_after_s=ready_after,
                           bootstrap_started_at=initial, bootstrap_exit=result,
                           elapsed_virtual_s=clock[0] - initial, actions=actions,
                           current_starts=current_starts, readiness_waits=readiness_waits)
        (tmp_path / "bootstrap-deadline-observation.json").write_text(
            json.dumps(observation, indent=2) + "\n", encoding="utf-8")
        return SimpleNamespace(result=result, elapsed=clock[0] - initial,
                               launches=launches, requests=requests, reports=reports,
                               current_starts=current_starts, readiness_waits=readiness_waits)
    return run


@pytest.mark.parametrize("phase", ["DEFERRED", "CONFIRMED"])
@pytest.mark.parametrize("ready_after,prepared", [(300.0, False), (0.0, False), (0.0, True)],
                         ids=["after-startup-window", "immediately-ready", "prepared-update"])
def test_bootstrap_keeps_relay_preparation_separate_from_update_wait(bootstrap_relay, phase,
                                                                   ready_after, prepared):
    observed = bootstrap_relay(phase, ready_after=ready_after, prepared=prepared)
    assert observed.result == 0
    assert observed.elapsed == pytest.approx(ready_after)
    assert observed.launches == [["--label-match-user-relay"]]
    assert len(observed.requests) == int(prepared)
    assert len(observed.reports) == 1
    assert len(observed.current_starts) == len(observed.readiness_waits) == 1

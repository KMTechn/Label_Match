from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import threading
import time
from types import SimpleNamespace

import pytest

import auto_update as updates
import user_relay
from test_relay_second_start_admission import live_relay, OPERATOR, SHA

pytestmark = pytest.mark.skipif(os.name != "nt", reason="Windows relay admission")
OTHER_OPERATOR = OPERATOR.rsplit("-", 1)[0] + "-" + str(int(OPERATOR.rsplit("-", 1)[1]) + 1)


@pytest.fixture
def recent_relay(live_relay):
    live_relay.status.update(source_commit=SHA, sid=OPERATOR, ordinary_user=True)
    return live_relay


def test_recent_relay_reuse_needs_no_snapshot_or_survival_wait(recent_relay, monkeypatch, record_testsuite_property):
    monkeypatch.setattr(updates, "process_snapshot", lambda _: pytest.fail("snapshot before window"))
    monkeypatch.setattr(user_relay, "_write_json_atomic", lambda *_a: pytest.fail("reuse wrote data"))
    started = time.perf_counter()
    result = user_relay.reuse_or_start_user_relay_process(
        recent_relay.current, wait=lambda _: pytest.fail("survival wait before window"),
    )
    elapsed = time.perf_counter() - started
    assert result["status"] == "ALIVE"
    assert result["existing_relay_reused"] is True
    assert result["fast_path"] is True
    assert elapsed < 1
    record_testsuite_property("fast_path_ms", round(elapsed * 1000, 3))


def test_slow_reuse_keeps_second_observation_when_metadata_arrives_during_wait(recent_relay):
    recent_relay.status.pop("source_commit")
    result = user_relay.reuse_or_start_user_relay_process(
        recent_relay.current, wait=lambda _: recent_relay.status.update(source_commit=SHA),
    )
    assert not result.get("fast_path", False)
    assert len(recent_relay.observed) == 2


@pytest.mark.parametrize("invalid", [
    "trial", "rollback_trial", "old", "future", "missing_sha", "missing_sid",
    "wrong_sha", "wrong_sid", "missing_ordinary", "wrong_code_root",
])
def test_unqualified_status_keeps_observed_reuse(recent_relay, invalid):
    status = recent_relay.status
    if invalid in {"trial", "rollback_trial"}:
        phase = "APPLIED_PENDING_READY" if invalid == "trial" else "ROLLBACK_PENDING_READY"
        state = dict(phase=phase, nonce="a" * 32, applied_at=recent_relay.row["created_at"],
                     rollback_at=recent_relay.row["created_at"], trial_sha=SHA)
        recent_relay.records["transition.json"] = state
        status["update_nonce"] = state["nonce"]
    elif invalid in {"old", "future"}:
        delta = -100 if invalid == "old" else 2
        status["updated_at"] = (datetime.now(timezone.utc) + timedelta(seconds=delta)).isoformat()
    elif invalid.startswith("missing_"):
        status.pop({"missing_sha": "source_commit", "missing_sid": "sid",
                    "missing_ordinary": "ordinary_user"}[invalid])
    elif invalid == "wrong_sha":
        status["source_commit"] = "b" * 40
    elif invalid == "wrong_sid":
        status["sid"] = OTHER_OPERATOR
    else:
        # Keep the old refusal without starting a real child.
        status["code_root"] = str(recent_relay.current)
        assert user_relay._existing_user_relay_owner(
            recent_relay.current, Path(status["direct_sync_root"]), is_bootstrap=True, allow_fast_path=True,
        ) is None
        assert len(recent_relay.observed) == 1
        return
    waits = []
    result = user_relay.reuse_or_start_user_relay_process(recent_relay.current, wait=waits.append)
    assert result["status"] == "ALIVE"
    assert not result.get("fast_path", False)
    assert len(recent_relay.observed) == 2
    assert waits == [2]


@pytest.mark.parametrize("invalid", ["wrong_sha", "wrong_sid", "policy_changed", "wrong_role", "elevated", "reused_pid", "missing"])
def test_deferred_observation_rejects_changed_owner(recent_relay, invalid, monkeypatch):
    if invalid == "reused_pid":
        recent_relay.status["updated_at"] = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
    result = user_relay.reuse_or_start_user_relay_process(recent_relay.current, wait=lambda _: None)
    assert len(recent_relay.observed) == 0
    if invalid == "wrong_sha":
        recent_relay.row["sha"] = "b" * 40
    elif invalid in {"wrong_sid", "policy_changed"}:
        recent_relay.row["sid"] = OTHER_OPERATOR
        if invalid == "policy_changed":
            monkeypatch.setattr(updates, "load_policy", lambda _: {"operator_sid": recent_relay.row["sid"]})
    elif invalid == "wrong_role":
        recent_relay.row["role"] = "gui"
    elif invalid == "elevated":
        recent_relay.row["ordinary_user"] = False
    elif invalid == "reused_pid":
        recent_relay.row["created_at"] = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
        recent_relay.status["updated_at"] = user_relay._now()
    else:
        recent_relay.row["pid"] = 78
    with pytest.raises(user_relay.UserRelayError):
        user_relay.verify_reused_user_relay(recent_relay.current, result)
    assert len(recent_relay.observed) == 1


def test_deferred_observation_accepts_same_owner_once(recent_relay):
    result = user_relay.reuse_or_start_user_relay_process(recent_relay.current)
    user_relay.verify_reused_user_relay(recent_relay.current, result)
    assert len(recent_relay.observed) == 1


def test_steady_relay_publishes_its_own_identity(monkeypatch, tmp_path):
    monkeypatch.setattr(updates, "trial_context", lambda _: None)
    code = tmp_path / "app"
    monkeypatch.setattr(updates, "control_root", lambda path: tmp_path if path == code else None)
    monkeypatch.setattr(updates, "version_manifest", lambda _: {"source_commit": SHA})
    monkeypatch.setattr(updates, "read_json", lambda _: {"source_commit": SHA})
    monkeypatch.setattr(updates, "current_process_sid", lambda: OPERATOR)
    monkeypatch.setattr(updates, "ordinary_user_context", lambda: True)
    metadata = updates.relay_ready_metadata(code)
    assert metadata["source_commit"] == SHA
    assert metadata["sid"] == OPERATOR
    assert metadata["ordinary_user"] is True
    status_path = tmp_path / "status.json"
    user_relay.run_persistent_relay_loop(lambda: {"status": "PASS"}, status_path=status_path,
                                        max_cycles=1, metadata=metadata)
    persisted = user_relay._read_json(status_path)
    assert persisted["sid"] == OPERATOR
    assert persisted["source_commit"] == SHA


def test_trial_relay_keeps_identity_metadata_for_later_steady_reuse(monkeypatch, tmp_path):
    state = {"nonce": "a" * 32}
    monkeypatch.setattr(updates, "trial_context", lambda _: (tmp_path, {}, state))
    monkeypatch.setattr(updates, "version_manifest", lambda _: {"source_commit": SHA})
    monkeypatch.setattr(updates, "current_process_sid", lambda: OPERATOR)
    monkeypatch.setattr(updates, "ordinary_user_context", lambda: True)
    metadata = updates.relay_ready_metadata(tmp_path / "app")
    assert metadata["sid"] == OPERATOR
    assert metadata["source_commit"] == SHA
    assert metadata["update_nonce"] == state["nonce"]


@pytest.mark.parametrize("trial", [False, True])
def test_optional_identity_read_failure_preserves_first_start_metadata(monkeypatch, tmp_path, trial):
    state = {"nonce": "a" * 32}
    monkeypatch.setattr(updates, "trial_context", lambda _: (tmp_path, {}, state) if trial else None)
    monkeypatch.setattr(updates, "control_root", lambda _: tmp_path)
    monkeypatch.setattr(updates, "version_manifest", lambda _: {"source_commit": SHA})
    monkeypatch.setattr(updates, "read_json", lambda _: {"source_commit": SHA})
    monkeypatch.setattr(updates, "ordinary_user_context", lambda: True)

    def unavailable():
        raise OSError("identity read unavailable")

    monkeypatch.setattr(updates, "current_process_sid", unavailable)
    metadata = updates.relay_ready_metadata(tmp_path / "app")
    assert "sid" not in metadata
    if trial:
        assert metadata["update_nonce"] == state["nonce"]
        assert metadata["source_commit"] == SHA
        assert metadata["ordinary_user"] is True


def test_deferred_check_waits_for_visible_window_and_reports_on_ui_thread(monkeypatch, tmp_path):
    import Label_Match as app

    main_thread = threading.get_ident()
    jobs, notices, traces, observed = [], [], [], []
    visible = False
    window = SimpleNamespace(after=lambda delay, callback: jobs.append(callback),
                             winfo_viewable=lambda: visible, _tk_shutdown_requested=False)

    def reject(*_args):
        observed.append(threading.get_ident())
        raise user_relay.UserRelayError("owner differs")

    monkeypatch.setattr(user_relay, "verify_reused_user_relay", reject)
    monkeypatch.setattr(app, "_show_first_run_onboarding_error",
                        lambda failure: notices.append((failure, threading.get_ident())))
    monkeypatch.setattr(app, "_label_match_startup_trace", lambda stage, **details: traces.append((stage, details)))
    report = {"relay_start": {"fast_path": True}, "app_root": str(tmp_path),
              "report_path": str(tmp_path / "report.json")}
    app._schedule_reused_relay_verification(window, report)
    assert not observed
    jobs.pop(0)()
    assert not observed
    visible = True
    jobs.pop(0)()
    deadline = time.monotonic() + 2
    for _ in range(200):
        if notices:
            break
        assert time.monotonic() < deadline
        assert jobs
        jobs.pop(0)()
        time.sleep(0.005)
    assert observed and observed[0] != main_thread
    assert len(notices) == 1
    failure, notice_thread = notices[0]
    assert notice_thread == main_thread
    assert failure.cause_code == "SETUP_FAILED"
    assert failure._startup_check_refusal is True
    assert traces[-1][1]["cause_code"] == "SETUP_FAILED"
    from startup_observation import onboarding_reason
    assert onboarding_reason(failure) == "FIRST_RUN_SETUP_REQUIRED"


def test_main_hands_actual_onboarding_context_to_application(monkeypatch, tmp_path):
    import Label_Match as app

    started = []
    relay = {"fast_path": True, "process_id": 77}
    direct = tmp_path / "direct-sync"
    monkeypatch.setattr(app, "release_identity", lambda _: {})
    monkeypatch.setattr(updates, "startup_gate", lambda *_a: None)
    monkeypatch.setattr(app, "_label_match_runtime_app_root", lambda: str(tmp_path))
    monkeypatch.setattr(app, "dispatch_product_mode", lambda *_a: None)
    monkeypatch.setattr(app, "verify_factory_contract_startup", lambda: None)
    monkeypatch.setattr(app, "validate_pending_worker_install", lambda *_a: None)
    monkeypatch.setattr(app, "check_user_data_open", lambda *_a: None)
    monkeypatch.setattr(app, "_first_run_onboarding_enabled", lambda: True)
    monkeypatch.setattr(app, "requires_bootstrap_integrity", lambda *_a: False)
    monkeypatch.setattr(app, "onboard_current_user", lambda *_a, **_k: {
        "relay_start": relay, "direct_sync_root": str(direct),
    })
    monkeypatch.setattr(app, "_label_match_startup_trace", lambda *_a, **_k: None)
    monkeypatch.setattr(app, "_default_label_match_settings_path", lambda: tmp_path / "settings.json")
    monkeypatch.setattr(app, "resolve_data_scope", lambda **_k: str(tmp_path))
    monkeypatch.setattr(app, "run_guarded_entrypoint", lambda entry, **_k: entry())
    monkeypatch.setattr(app, "_run_label_match_application",
                        lambda **kwargs: started.append(kwargs) or 0)
    assert app.main([]) == 0
    context = started[0]["relay_reuse_report"]
    assert context["app_root"] == str(tmp_path)
    assert context["relay_start"] is relay
    assert context["report_path"] == direct / "status" / "current_user_onboarding.json"


def test_application_attaches_deferred_check_before_mainloop(monkeypatch):
    import Label_Match as app
    import startup_observation

    events = []
    window = SimpleNamespace(title=lambda: "Packaging", state=lambda: "normal",
                             mainloop=lambda: events.append("mainloop"),
                             _finish_after_mainloop=lambda: None)
    context = {"relay_start": {"fast_path": True}}
    monkeypatch.setattr(app, "prepare_startup_item_catalog", lambda: None)
    monkeypatch.setattr(startup_observation, "observe_startup", lambda: None)
    monkeypatch.setattr(app, "Label_Match", lambda: window)
    monkeypatch.setattr(updates, "trial_context", lambda _: None)
    monkeypatch.setattr(app, "_label_match_startup_trace", lambda *_a, **_k: None)
    monkeypatch.setattr(app, "_schedule_reused_relay_verification",
                        lambda actual, report: events.append((actual, report)))
    assert app._run_label_match_application(relay_reuse_report=context) == 0
    assert events == [(window, context), "mainloop"]

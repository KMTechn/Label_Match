"""Second start while the live relay holds writer admission for its cycle."""
from contextlib import contextmanager, nullcontext
from datetime import datetime, timedelta, timezone
import inspect
import json
import os
from pathlib import Path
import subprocess
import threading
import time
from types import SimpleNamespace

import pytest

import auto_update as updates
import current_user_onboarding
import user_relay
import writer_session_fence as fence

pytestmark = pytest.mark.skipif(os.name != "nt", reason="named writer admission mutex is Windows-only")

SHA = "a" * 40
OPERATOR = "S-1-5-21-1-2-3-1001"


@contextmanager
def _relay_cycle_holds_admission():
    """Hold the real writer admission mutex from another thread, like a relay cycle."""
    name = fence.writer_admission_mutex_name(fence.canonical_control_root(os.environ), environ=os.environ)
    held, done = threading.Event(), threading.Event()

    def hold():
        lease = fence._acquire_named_mutex(name, 5)
        held.set()
        try:
            done.wait(60)
        finally:
            if lease is not None:
                lease.release()

    thread = threading.Thread(target=hold, daemon=True)
    thread.start()
    assert held.wait(10)
    try:
        yield
    finally:
        done.set()
        thread.join(15)


@pytest.fixture
def live_relay(monkeypatch, tmp_path):
    current = tmp_path / "current"
    (current / "app").mkdir(parents=True)
    control = tmp_path / "update-control"
    control.mkdir()
    state_root = tmp_path / "user-state"
    state_root.mkdir()
    version = tmp_path / "versions" / SHA
    created = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    status = dict(status="RUNNING", worker_id=user_relay.LABEL_MATCH_WORKER_ID, process_id=77,
                  app_root=str(current), code_root=str(version), direct_sync_root=str(state_root),
                  updated_at=user_relay._now())
    row = dict(pid=77, observable=True, sha=SHA, sid=OPERATOR, role="relay", ordinary_user=True, created_at=created)
    records = {"active.json": {"sha": SHA}, "transition.json": {"schema": updates.SCHEMA, "phase": "CONFIRMED"}}
    monkeypatch.setattr(updates, "control_root", lambda _: control)
    monkeypatch.setattr(updates, "load_policy", lambda _: {"operator_sid": OPERATOR})
    monkeypatch.setattr(updates, "read_json", lambda path: records[path.name])
    monkeypatch.setattr(updates, "update_lock", lambda *a, **k: nullcontext())
    monkeypatch.setattr(updates, "version_bundle", lambda _control, sha: tmp_path / "versions" / sha)
    monkeypatch.setattr(updates, "system_executable", lambda _: "powershell.exe")
    observed = []

    def run(arguments, **_kwargs):
        # Only the fixed helper's Observe is expected; no real child starts.
        assert arguments[arguments.index("-Mode") + 1] == "Observe"
        observed.append(time.monotonic())
        return SimpleNamespace(returncode=0, stdout=json.dumps([row]), stderr="")

    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(current_user_onboarding, "resolve_current_user_onboarding_paths",
                        lambda _: SimpleNamespace(direct_sync_root=state_root))
    monkeypatch.setattr(user_relay, "_read_json", lambda _: dict(status))
    monkeypatch.setattr(user_relay, "_acquire_relay_lease", lambda _: None)
    fresh = user_relay._fresh_persistent_owner
    monkeypatch.setattr(user_relay, "_fresh_persistent_owner",
                        lambda payload, **kwargs: fresh(payload, pid_exists=lambda _: True, **kwargs))
    monkeypatch.setattr(updates, "start_ordinary_process",
                        lambda *_a, **_k: pytest.fail("second start must not launch a relay"))
    return SimpleNamespace(current=current, status=status, row=row, records=records, observed=observed)


def _onboarding_relay_launcher():
    return inspect.signature(current_user_onboarding.onboard_current_user).parameters["relay_launcher"].default


def test_second_start_reuses_live_relay_while_its_cycle_holds_writer_admission(live_relay):
    launcher = _onboarding_relay_launcher()
    with _relay_cycle_holds_admission():
        started = time.monotonic()
        result = launcher(live_relay.current)
        elapsed = time.monotonic() - started
    assert result["status"] == "ALIVE"
    assert result["existing_relay_reused"] is True
    assert result["process_id"] == 77
    # Survival readback is 2 s; one writer-admission wait alone is 5 s.
    assert elapsed < 4.5
    assert len(live_relay.observed) == 2


@pytest.mark.parametrize("invalid", ["wrong_sha", "wrong_sid", "wrong_role", "elevated", "trial_nonce"])
def test_second_start_never_reuses_foreign_relay_without_admission(live_relay, invalid):
    row, records = live_relay.row, live_relay.records
    if invalid == "wrong_sha":
        row["sha"] = "b" * 40
    if invalid == "wrong_sid":
        row["sid"] = "S-1-5-21-1-2-3-1002"
    if invalid == "wrong_role":
        row["role"] = "gui"
    if invalid == "elevated":
        row["ordinary_user"] = False
    if invalid == "trial_nonce":
        records["transition.json"] = {"schema": updates.SCHEMA, "phase": "APPLIED_PENDING_READY",
                                      "nonce": "n" * 32, "applied_at": row["created_at"]}
        live_relay.status["update_nonce"] = "m" * 32
    with _relay_cycle_holds_admission():
        with pytest.raises(fence.WriterFencedError) as caught:
            _onboarding_relay_launcher()(live_relay.current, wait=lambda _: None)
    # Refusal falls through to the existing guarded start, which still waits for admission.
    assert caught.value.code == "WRITER_GATE_TIMEOUT"


def test_startup_trace_records_onboarding_refusal_reason(monkeypatch, tmp_path):
    import Label_Match as app

    traces = []
    monkeypatch.setattr(updates, "startup_gate", lambda *_a: None)
    monkeypatch.setattr(app, "_label_match_runtime_app_root", lambda: str(tmp_path))
    monkeypatch.setattr(app, "dispatch_product_mode", lambda *_a: None)
    monkeypatch.setattr(app, "verify_factory_contract_startup", lambda: None)
    monkeypatch.setattr(app, "validate_pending_worker_install", lambda *_a: None)
    monkeypatch.setattr(app, "check_user_data_open", lambda *_a: None)
    monkeypatch.setattr(app, "_first_run_onboarding_enabled", lambda: True)
    monkeypatch.setattr(app, "requires_bootstrap_integrity", lambda *_a: False)
    monkeypatch.setattr(app, "_label_match_startup_trace", lambda stage, **details: traces.append((stage, details)))

    def refuse(*_a, **_k):
        inner = fence.WriterFencedError("WRITER_GATE_TIMEOUT", "writer admission mutex is held by a deployment operation")
        failure = current_user_onboarding.CurrentUserOnboardingError(
            "Label_Match first-run onboarding failed: " + str(inner), report_path=tmp_path / "report.json")
        raise failure from inner

    monkeypatch.setattr(app, "onboard_current_user", refuse)
    errors = []
    monkeypatch.setattr(app, "_show_first_run_onboarding_error", errors.append)
    assert app.main([]) == app.ONBOARDING_EXIT_CODE
    assert len(errors) == 1
    stage, details = next(item for item in traces if item[0] == "current_user_onboarding_blocked")
    assert details["error_type"] == "CurrentUserOnboardingError"
    assert details["reason_code"] == "WRITER_GATE_TIMEOUT"
    assert details["reason_type"] == "WriterFencedError"
    assert details["cause_code"] == "SETUP_FAILED"
    assert "mutex" not in json.dumps({k: v for k, v in details.items() if k != "report_path"})

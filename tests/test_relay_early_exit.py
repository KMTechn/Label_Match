"""Relay startup must distinguish a dead child from a live, unready child."""
import ast
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

import auto_update as updates
import current_user_onboarding as onboarding
import user_relay


@pytest.mark.parametrize("returncode", [0, 7])
@pytest.mark.parametrize("bootstrap", [False, True])
def test_started_child_exit_without_running_fails_promptly(monkeypatch, returncode, bootstrap):
    clock = [100.0]
    monkeypatch.setattr(user_relay.time, "monotonic", lambda: clock[0])
    child = SimpleNamespace(pid=42, poll=lambda: returncode)
    with pytest.raises(user_relay.UserRelayError) as caught:
        user_relay._read_started_relay(
            child, lambda: None, 2.0, 700.0, bootstrap,
            lambda seconds: clock.__setitem__(0, clock[0] + seconds),
        )
    assert caught.value.cause_code == "RELAY_EXITED_BEFORE_RUNNING"
    assert caught.value.exit_code == returncode
    assert clock[0] <= 102.1


def test_exited_bootstrap_keeps_proven_running_owner(monkeypatch):
    owner = {"process_id": 77, "updated_at": user_relay._now()}
    result = user_relay._read_started_relay(
        SimpleNamespace(pid=42, poll=lambda: 0), lambda: owner, 0.0,
        user_relay.time.monotonic() + 600, True, lambda _: None,
    )
    assert result["process_id"] == 77


def test_live_unready_bootstrap_keeps_original_deadline(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(user_relay.time, "monotonic", lambda: clock[0])
    with pytest.raises(user_relay.UserRelayError) as caught:
        user_relay._read_started_relay(
            SimpleNamespace(pid=42, poll=lambda: None), lambda: None, 0.0,
            103.0, True, lambda seconds: clock.__setitem__(0, clock[0] + seconds),
        )
    assert not getattr(caught.value, "cause_code", "")
    assert clock[0] == pytest.approx(103.0)


@pytest.mark.skipif(sys.platform != "win32", reason="Windows native relay process supervision")
def test_wait_current_relay_detects_real_child_exit(monkeypatch, tmp_path):
    child = subprocess.Popen([sys.executable, "-B", "-c", "pass"],
                             creationflags=subprocess.CREATE_NO_WINDOW)
    assert child.wait(timeout=10) == 0
    monkeypatch.setattr(updates, "read_json", lambda _: {"sha": "1" * 40})
    monkeypatch.setattr(updates, "current_relay_ready", lambda *a, **k: False)
    monkeypatch.setattr(updates, "_relay_startup_stop_checkpoint", lambda _: None)
    reports = []
    monkeypatch.setattr(updates, "emit_support_record", lambda fields, **kw: reports.append(fields))
    start = updates.time.monotonic()
    # A short deadline also bounds RED on the old implementation.
    with pytest.raises(updates._RelayPreparationUnavailable) as caught:
        updates.wait_current_relay(tmp_path, {}, "1" * 40, started_at=updates.now_utc(),
                                   deadline=start + 0.5, pid=child.pid)
    assert caught.value.code == "RELAY_EXITED_BEFORE_RUNNING"
    assert updates.time.monotonic() - start < 0.5


@pytest.mark.skipif(sys.platform != "win32", reason="Windows native relay process supervision")
@pytest.mark.parametrize("entry", ["user_main", "headless_main", "headless_to_user_main"])
def test_unhandled_main_exception_is_recorded_before_first_cycle_and_parent_fails(tmp_path, entry):
    script = """
import json
from pathlib import Path
import sys
import auto_update as updates
import current_user_onboarding as onboarding
import label_data_generation as generation
import label_match_product_host as host
import label_match_relay_entry as headless
import user_relay
root = Path(sys.argv[1])
updates.control_root = lambda _: root / 'control'
updates.load_policy = lambda _: {'staging_root': str(root / 'stage')}
generation.release_identity = lambda _: None
def fail_before_cycle(*args, **kwargs):
    json.loads('unhandled-message-canary C:/private/canary')
updates.startup_gate = fail_before_cycle
arguments = ['--app-root', str(root)]
if sys.argv[2] == 'user_main':
    user_relay.main(arguments, credential_input=object())
else:
    headless.APP_ROOT = root
    onboarding._machine_worker_context = lambda _: None
    if sys.argv[2] == 'headless_to_user_main':
        host.dispatch_product_mode = lambda argv: user_relay.main(argv[1:], credential_input=object())
    headless.main([user_relay.USER_RELAY_MODE, *arguments])
"""
    child = subprocess.Popen(
        [sys.executable, "-B", "-c", script, str(tmp_path), entry],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )
    assert child.wait(timeout=10) == 1
    started = user_relay.time.monotonic()
    with pytest.raises(user_relay.UserRelayError) as caught:
        user_relay._read_started_relay(
            child, lambda: None, 0.0, started + 2.0, True, user_relay.time.sleep,
        )
    assert caught.value.cause_code == "RELAY_EXITED_BEFORE_RUNNING"
    assert caught.value.exit_code == 1
    assert user_relay.time.monotonic() - started < 0.5
    records = [json.loads(path.read_text()) for path in (tmp_path / "stage").glob("failure-*.json")]
    assert records == [{
        "stage": "update_apply", "reason_code": caught.value.cause_code,
        "exception_type": "JSONDecodeError",
        "location_module": "label_match_product_host.py" if entry == "headless_main" else "user_relay.py",
        "location_function": "dispatch_product_mode" if entry == "headless_main" else "_run_user_relay",
    }]


@pytest.mark.parametrize("diagnostic_fails", [False, True])
@pytest.mark.parametrize("running", [False, True])
def test_main_diagnostic_keeps_original_exception(monkeypatch, tmp_path, diagnostic_fails, running):
    error = ValueError("unhandled-message-canary")
    reports = []

    def fail(*args, **kwargs):
        if running:
            user_relay.run_persistent_relay_loop(
                lambda: {"status": "PASS"}, status_path=tmp_path / "status.json", max_cycles=1,
            )
        raise error

    def record(code, **kwargs):
        if "error" not in kwargs:
            return
        reports.append((code, kwargs["error"]))
        if diagnostic_fails:
            raise OSError("diagnostic-message-canary")

    monkeypatch.setattr(updates, "startup_gate", fail)
    import label_data_generation as generation
    monkeypatch.setattr(generation, "release_identity", lambda _: None)
    monkeypatch.setattr(user_relay, "_record_relay_reason", record)
    with pytest.raises(ValueError) as caught:
        user_relay.main(["--app-root", str(tmp_path)], credential_input=object())
    assert caught.value is error
    expected = "RELAY_UNHANDLED_EXCEPTION" if running else "RELAY_EXITED_BEFORE_RUNNING"
    assert reports == [(expected, error)]
    # A previous invocation's successful RUNNING must not taint the next main.
    reports.clear()
    running = False
    with pytest.raises(ValueError) as repeated:
        user_relay.main(["--app-root", str(tmp_path)], credential_input=object())
    assert repeated.value is error
    assert reports == [("RELAY_EXITED_BEFORE_RUNNING", error)]


def test_live_relay_without_running_keeps_timeout(monkeypatch, tmp_path):
    clock = [100.0]
    monkeypatch.setattr(updates.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(updates.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    monkeypatch.setattr(updates, "read_json", lambda _: {"sha": "1" * 40})
    monkeypatch.setattr(updates, "current_relay_ready", lambda *a, **k: False)
    monkeypatch.setattr(updates, "native_process_identity", lambda _: {"status": "LIVE"})
    monkeypatch.setattr(updates, "_relay_startup_stop_checkpoint", lambda _: None)
    assert not updates.wait_current_relay(tmp_path, {}, "1" * 40,
                                         started_at=updates.now_utc(), deadline=103.0, pid=42)
    assert clock[0] == pytest.approx(103.0)


def test_wait_current_relay_detects_pid_reuse_without_running(monkeypatch, tmp_path):
    monkeypatch.setattr(updates, "read_json", lambda _: {"sha": "1" * 40})
    monkeypatch.setattr(updates, "current_relay_ready", lambda *a, **k: False)
    monkeypatch.setattr(updates, "_relay_startup_stop_checkpoint", lambda _: None)
    births = iter(["1", "2"])
    monkeypatch.setattr(updates, "native_process_identity",
                        lambda _: {"status": "LIVE", "creation_identity": next(births)})
    with pytest.raises(updates._RelayPreparationUnavailable) as caught:
        updates.wait_current_relay(tmp_path, {}, "1" * 40, started_at=updates.now_utc(),
                                   deadline=updates.time.monotonic() + 2.0, pid=42)
    assert caught.value.code == "RELAY_EXITED_BEFORE_RUNNING"


def test_fenced_wait_records_fixed_reason_without_mutating_status(monkeypatch, tmp_path):
    from writer_session_fence import WriterFencedError
    status = tmp_path / "status.json"
    status.write_text("preserved")
    reports = []
    monkeypatch.setattr(user_relay, "_record_relay_reason", lambda code, **kwargs: reports.append(code), raising=False)
    def denied(*a, **k):
        raise WriterFencedError("WRITER_FENCED", "test")
    monkeypatch.setattr(user_relay, "writer_admission", denied)
    result = user_relay.run_persistent_relay_loop(
        lambda: pytest.fail("fenced cycle ran"), status_path=status, max_cycles=1,
    )
    assert result["status"] == "FENCED"
    assert "RELAY_WRITER_FENCED" in reports
    assert status.read_text() == "preserved"


@pytest.mark.parametrize("code", ["RELAY_STOP_MARKER_PRESENT", "RELAY_INSTANCE_LEASE_UNAVAILABLE",
                                  "RELAY_WRITER_FENCED", "RELAY_STARTUP_HANDLED"])
def test_reason_record_uses_existing_support_sink_only(monkeypatch, tmp_path, code):
    control = tmp_path / "control"
    stage = tmp_path / "stage"
    monkeypatch.setattr(updates, "control_root", lambda _: control)
    monkeypatch.setattr(updates, "load_policy", lambda _: {"staging_root": str(stage)})
    user_relay._record_relay_reason(code, app_root=tmp_path)
    snapshots = list(stage.glob("failure-*.json"))
    assert len(snapshots) == 1
    assert json.loads(snapshots[0].read_text()) == {"stage": "update_apply", "reason_code": code}
    assert not (stage / "startup-support.json").exists()


def test_reason_snapshot_survives_later_parent_reason(monkeypatch, tmp_path):
    monkeypatch.setattr(updates, "control_root", lambda _: tmp_path / "control")
    monkeypatch.setattr(updates, "load_policy", lambda _: {"staging_root": str(tmp_path / "stage")})
    user_relay._record_relay_reason("RELAY_STOP_MARKER_PRESENT", app_root=tmp_path)
    user_relay._record_relay_reason("RELAY_EXITED_BEFORE_RUNNING", app_root=tmp_path, exit_code=0)
    records = [json.loads(p.read_text()) for p in (tmp_path / "stage").glob("failure-*.json")]
    assert {row["reason_code"] for row in records} == {"RELAY_STOP_MARKER_PRESENT", "RELAY_EXITED_BEFORE_RUNNING"}
    assert next(row for row in records if row["reason_code"] == "RELAY_EXITED_BEFORE_RUNNING")["exit_code"] == 0


@pytest.mark.parametrize("branch", ["stop_marker", "instance_lease", "startup_deferred"])
def test_main_silent_startup_branch_records_reason(monkeypatch, tmp_path, branch):
    import label_data_generation as generation
    import logistics_runtime_profile as profiles
    monkeypatch.setattr(updates, "startup_gate", lambda *a, **k: 0 if branch == "startup_deferred" else None)
    monkeypatch.setattr(generation, "release_identity", lambda _: None)
    monkeypatch.setattr(generation, "normalize_data_arguments", lambda *a: None)
    monkeypatch.setattr(user_relay, "check_user_data_open", lambda *a: None)
    monkeypatch.setattr(user_relay, "record_user_data_open", lambda *a: None)
    monkeypatch.setattr(onboarding, "apply_current_user_runtime_environment", lambda *a: None)
    monkeypatch.setattr(onboarding, "resolve_current_user_onboarding_paths", lambda _: SimpleNamespace(
        direct_sync_root=tmp_path / "relay", data_root=tmp_path / "data",
        ledger_path=tmp_path / "ledger.sqlite3", queue_dir=tmp_path / "queue",
        logistics_profile_path=tmp_path / "profile", settings_path=tmp_path / "settings",
    ))
    monkeypatch.setattr(profiles, "load_logistics_runtime_profile", lambda **k: SimpleNamespace())
    monkeypatch.setattr(user_relay, "_resolve_scan_source_dir", lambda *a, **k: tmp_path / "scan")
    monkeypatch.setattr(user_relay, "_acquire_relay_lease", lambda _: None)
    marker = user_relay.user_relay_stop_path(tmp_path / "relay")
    if branch == "stop_marker":
        marker.parent.mkdir(parents=True)
        marker.write_text("preserved")
    reports = []
    monkeypatch.setattr(user_relay, "_record_relay_reason", lambda code, **k: reports.append(code), raising=False)
    assert user_relay.main(["--app-root", str(tmp_path)], credential_input=object()) == 0
    assert reports == [{"stop_marker": "RELAY_STOP_MARKER_PRESENT",
                        "instance_lease": "RELAY_INSTANCE_LEASE_UNAVAILABLE",
                        "startup_deferred": "RELAY_STARTUP_HANDLED"}[branch]]
    if marker.exists():
        assert marker.read_text() == "preserved"


@pytest.mark.parametrize("startup_check_refusal", [False, True])
def test_onboarding_notice_shows_same_relay_reason_in_existing_message_category(
    tmp_path, monkeypatch, startup_check_refusal,
):
    # Compile only the real notice function/constants; no GUI module import or window.
    source = Path(__file__).resolve().parents[1] / "Label_Match.py"
    tree = ast.parse(source.read_text(encoding="utf-8-sig"))
    names = {"FIRST_RUN_ONBOARDING_ERROR_TITLE", "FIRST_RUN_ONBOARDING_ERROR_MESSAGE",
             "FIRST_RUN_REGISTRATION_RECOVERY_CODES", "FIRST_RUN_REGISTRATION_RECOVERY_MESSAGE",
             "INSTALLATION_RECOVERY_REQUIRED_MESSAGE", "FIRST_RUN_ONBOARDING_CAUSE_MESSAGES"}
    nodes = [node for node in tree.body
             if (isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id in names
                                                     for t in node.targets))
             or (isinstance(node, ast.FunctionDef) and node.name == "_show_first_run_onboarding_error")]
    notices = []
    observed = []
    import startup_observation
    monkeypatch.setattr(startup_observation, "observe_startup",
                        lambda *a, **k: observed.append(k["refused"]))
    namespace = {"_show_stoppable_startup_notice": lambda title, message: notices.append((title, message)),
                 "_label_match_startup_trace": lambda *a, **k: pytest.fail("notice failed"),
                 "_watch_startup_notice_stop": lambda _: None}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source), "exec"), namespace)
    namespace["_show_first_run_onboarding_error"](SimpleNamespace(
        cause_code="RELAY_EXITED_BEFORE_RUNNING", status="FAILED", report_path=tmp_path / "report.json",
        _startup_check_refusal=startup_check_refusal,
    ))
    assert len(notices) == 1
    assert "기록 전송이 멈췄습니다\n현장 관리자에게 알리세요" in notices[0][1]
    assert "코드: RELAY_EXITED_BEFORE_RUNNING" in notices[0][1]
    assert observed == ([False] if startup_check_refusal else [])

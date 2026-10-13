from contextlib import contextmanager
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import time
from types import SimpleNamespace

import pytest

import auto_update as updates
import current_user_onboarding as onboarding
import writer_session_fence as fence
import user_relay
from tests.test_onboarding_lifecycle_scope import managed_onboarding
from tests.test_current_user_onboarding import isolated_data_generation_release
from tests.test_relay_recent_fastpath import recent_relay
from tests.test_relay_second_start_admission import live_relay


@pytest.fixture(autouse=True)
def reset_window_start(monkeypatch):
    monkeypatch.setattr(onboarding, "_WINDOW_FIRST_START", None, raising=False)


@pytest.fixture
def steady_start(managed_onboarding, monkeypatch):
    paths, environment, control, helpers = managed_onboarding
    onboarding.apply_current_user_runtime_environment(paths, environ=environment)
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    paths.ledger_path.parent.mkdir(parents=True, exist_ok=True)
    paths.ledger_path.write_bytes(b"local-ledger")
    paths.settings_path.parent.mkdir(parents=True, exist_ok=True)
    paths.settings_path.write_text("{}", encoding="utf-8")
    manifest = updates.read_json(paths.app_root / "portable-manifest.json")
    manifest.update(schema="label-match-portable-tree-v1", version="v2.0.99", startup_update_protocol=1)
    (paths.app_root / "portable-manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    paths.onboarding_report_path.parent.mkdir(parents=True, exist_ok=True)
    paths.onboarding_report_path.write_text(json.dumps({
        "status": "READY", "state_scope": "current_user", "data_root": str(paths.data_root),
        "direct_sync_root": str(paths.direct_sync_root), "ledger_path": str(paths.ledger_path),
        "settings_path": str(paths.settings_path), "logistics_profile_path": str(paths.logistics_profile_path),
        "bootstrap_integrity": {"status": "VERIFIED"},
    }), encoding="utf-8")
    policy = updates.load_policy(control)
    status = dict(status="RUNNING", worker_id=user_relay.LABEL_MATCH_WORKER_ID,
                  process_id=os.getpid(), app_root=str(control.parent / "current"),
                  code_root=str(paths.app_root), direct_sync_root=str(paths.direct_sync_root),
                  updated_at=datetime.now(timezone.utc).isoformat(), source_commit=manifest["source_commit"],
                  sid=policy["operator_sid"], ordinary_user=True)
    user_relay.user_relay_status_path(paths.direct_sync_root).write_text(json.dumps(status), encoding="utf-8")
    monkeypatch.setattr(user_relay, "_acquire_relay_lease", lambda _: None)
    monkeypatch.setattr(updates, "native_process_identity", lambda pid: {
        "status": "LIVE", "creation_identity": str(pid),
    })
    return SimpleNamespace(paths=paths, environment=environment, control=control, helpers=helpers, status=status)


def test_fast_start_has_no_helpers_and_records_shared_hold(steady_start, monkeypatch, record_testsuite_property):
    from startup_send_gate import startup_send_deferred, startup_send_hold_path
    monkeypatch.setattr(subprocess, "run", lambda *_a, **_k: pytest.fail("child before window"))
    monkeypatch.setattr(onboarding, "verify_bootstrap_integrity", lambda *_a, **_k: pytest.fail("hash before window"))
    started = time.perf_counter()
    result = onboarding.try_window_first_start(steady_start.paths.app_root, environ=steady_start.environment)
    elapsed = time.perf_counter() - started
    assert elapsed < 2
    record_testsuite_property("window_first_admission_ms", round(elapsed * 1000, 3))
    assert result["start_path"] == "FAST_WINDOW_FIRST"
    assert result["relay_start"]["status"] == "ALIVE"
    assert result["existing_relay_reused"] is True and result["fast_path"] is True
    assert steady_start.helpers == []
    marker = json.loads(startup_send_hold_path(steady_start.paths.direct_sync_root).read_text())
    assert set(marker) == {"pid", "creation_identity", "started_at", "start_path"}
    assert marker["pid"] == os.getpid()
    assert startup_send_deferred(steady_start.paths.direct_sync_root)["status"] == "DEFERRED"


@pytest.mark.parametrize("condition", ["first_run", "trial", "pending", "no_relay", "stale_relay", "stop", "held", "address"])
def test_nonsteady_start_keeps_full_path(steady_start, condition):
    from startup_send_gate import create_startup_send_hold
    paths = steady_start.paths
    if condition == "first_run":
        paths.onboarding_report_path.unlink()
    elif condition == "trial":
        state = updates.read_json(steady_start.control / "transition.json")
        state["phase"] = "APPLIED_PENDING_READY"
        (steady_start.control / "transition.json").write_text(json.dumps(state))
    elif condition == "pending":
        (steady_start.control / "apply-claim.json").write_text("{}")
    elif condition == "no_relay":
        user_relay.user_relay_status_path(paths.direct_sync_root).unlink()
    elif condition == "stale_relay":
        status = dict(steady_start.status, updated_at="2000-01-01T00:00:00+00:00")
        user_relay.user_relay_status_path(paths.direct_sync_root).write_text(json.dumps(status))
    elif condition == "stop":
        user_relay.user_relay_stop_path(paths.direct_sync_root).parent.mkdir(parents=True, exist_ok=True)
        user_relay.user_relay_stop_path(paths.direct_sync_root).write_text("{}")
    elif condition == "held":
        create_startup_send_hold(paths.direct_sync_root)
    else:
        registration = onboarding._read_json(paths.registration_report_path, "registration")
        registration["server_base_url"] = "https://registered.invalid"
        paths.registration_report_path.write_text(json.dumps(registration))
        profile = onboarding._read_json(paths.logistics_profile_path, "profile")
        profile["base_url"] = "https://different.invalid"
        paths.logistics_profile_path.write_text(json.dumps(profile))
    assert onboarding.try_window_first_start(paths.app_root, environ=steady_start.environment) is None
    assert steady_start.helpers == []


def test_gui_and_relay_emit_nothing_while_held_then_resume(tmp_path, monkeypatch):
    import Label_Match as gui
    import direct_sync_runtime as runtime
    from startup_send_gate import create_startup_send_hold, release_startup_send_hold
    root = tmp_path / "sync"
    hold = create_startup_send_hold(root)
    config = SimpleNamespace(credential_path=root / "credentials.json")
    sent = []
    monkeypatch.setattr(gui, "_label_match_session_sync_trigger_enabled", lambda: True)
    monkeypatch.setattr(gui, "_label_match_direct_sync_runner_command", lambda *_a, **_k: sent.append("gui") or [])
    for invoke in (lambda: gui._label_match_run_session_direct_sync_once({"program_data_root": str(root)}),
                   lambda: runtime.enqueue_completed_source_file(config, source_file_path=tmp_path / "source.csv"),
                   lambda: runtime.run_relay_once(config)):
        assert invoke()["status"] == "DEFERRED"
    assert sent == []
    assert release_startup_send_hold(root, hold, verified=True)
    assert gui._label_match_run_session_direct_sync_once({"program_data_root": str(root)})["status"] == "SKIPPED"
    assert sent == ["gui"]


@pytest.mark.parametrize("owner", ["absent", "reused_pid", "unknown"])
def test_orphan_never_auto_releases_and_requires_full_verification(tmp_path, monkeypatch, owner):
    from startup_send_gate import create_startup_send_hold, startup_send_deferred, release_startup_send_hold, startup_send_hold_path
    root = tmp_path / "sync"
    hold = create_startup_send_hold(root)
    raw = startup_send_hold_path(root).read_bytes()
    observed = {"status": "ABSENT"} if owner == "absent" else {"status": "UNKNOWN"} if owner == "unknown" else {
        "status": "LIVE", "creation_identity": "different",
    }
    monkeypatch.setattr(updates, "native_process_identity", lambda _: observed)
    assert startup_send_deferred(root)["status"] == "DEFERRED"
    assert not release_startup_send_hold(root, hold, verified=False, full=True)
    assert startup_send_hold_path(root).read_bytes() == raw
    assert release_startup_send_hold(root, hold, verified=True, full=True)
    assert startup_send_deferred(root) is None


def test_bootstrap_hash_reuses_only_identical_bytes(tmp_path, monkeypatch):
    from tests.test_current_user_onboarding import _write_bootstrap_inventory_record
    root = tmp_path / "packet"
    (root / "app").mkdir(parents=True)
    (root / "runtime").mkdir()
    (root / "runtime/pythonw.exe").write_bytes(b"runtime-fixture")
    target = root / "app/main.py"
    target.write_bytes(b"original")
    _write_bootstrap_inventory_record(root)
    paths = SimpleNamespace(app_root=root, bootstrap_integrity_path=root / "bootstrap-integrity.json")
    original = onboarding._file_sha256
    calls = []
    monkeypatch.setattr(onboarding, "_file_sha256", lambda path: calls.append(path) or original(path))
    onboarding.verify_bootstrap_integrity(paths, required=True)
    count = len(calls)
    onboarding.verify_bootstrap_integrity(paths, required=True)
    assert len(calls) == count
    before = target.stat()
    target.write_bytes(b"modified")
    os.utime(target, ns=(before.st_atime_ns, before.st_mtime_ns))
    with pytest.raises(ValueError):
        onboarding.verify_bootstrap_integrity(paths, required=True)
    assert len(calls) > count


def test_writer_inventory_reuses_only_identical_bytes(tmp_path, monkeypatch):
    import writer_sink_inventory as inventory
    root = tmp_path / "packet"
    (root / "app").mkdir(parents=True)
    target = root / "app/sink.py"
    target.write_text('@writer_sink("one")\ndef write():\n    pass\n')
    (root / "INSTALL_THIS_PC.ps1").write_text("# installer")
    original = inventory.derive_writer_sink_inventory
    calls = []
    monkeypatch.setattr(inventory, "derive_writer_sink_inventory", lambda path: calls.append(path) or original(path))
    first = updates.version_writer_inventory_sha256(root)
    assert updates.version_writer_inventory_sha256(root) == first
    assert len(calls) == 1
    mirror = tmp_path / "mirror"
    (mirror / "app").mkdir(parents=True)
    (mirror / "app/sink.py").write_bytes(target.read_bytes())
    (mirror / "INSTALL_THIS_PC.ps1").write_bytes((root / "INSTALL_THIS_PC.ps1").read_bytes())
    assert updates.version_writer_inventory_sha256(mirror) == first
    assert len(calls) == 1
    before = target.stat()
    target.write_text('@writer_sink("two")\ndef write():\n    pass\n')
    os.utime(target, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert updates.version_writer_inventory_sha256(root) != first
    assert len(calls) == 2


def test_fast_main_skips_full_checks_and_reports_top_level_path(steady_start, monkeypatch):
    import Label_Match as gui
    traces = []
    monkeypatch.setattr(gui, "_label_match_runtime_app_root", lambda: steady_start.paths.app_root)
    monkeypatch.setattr(gui, "_first_run_onboarding_enabled", lambda: True)
    monkeypatch.setattr(gui, "verify_factory_contract_startup", lambda: None)
    monkeypatch.setattr(gui, "validate_pending_worker_install", lambda _: None)
    monkeypatch.setattr(gui, "check_user_data_open", lambda _: None)
    monkeypatch.setattr(gui, "release_identity", lambda _: {})
    monkeypatch.setattr(gui, "_label_match_startup_trace", lambda stage, **details: traces.append(details))
    monkeypatch.setattr(gui, "resolve_data_scope", lambda **_: "local")
    monkeypatch.setattr(gui, "run_guarded_entrypoint", lambda callback, **_: callback())
    monkeypatch.setattr(gui, "_run_label_match_application", lambda **_: 0)
    monkeypatch.setattr(gui, "onboard_current_user", lambda *_a, **_k: pytest.fail("full onboarding before window"))
    monkeypatch.setattr(updates, "startup_gate", lambda *_a, **_k: pytest.fail("full gate before window"))
    assert gui.main([]) == 0
    assert any(row.get("start_path") == "FAST_WINDOW_FIRST" for row in traces)


@pytest.mark.parametrize("failed", [False, True])
def test_background_waits_for_window_then_releases_only_after_success(steady_start, monkeypatch, failed):
    import Label_Match as gui
    import threading
    import time
    from startup_send_gate import startup_send_hold_path, startup_send_deferred
    report = onboarding.try_window_first_start(steady_start.paths.app_root, environ=steady_start.environment)
    before = steady_start.paths.ledger_path.read_bytes()
    completed = threading.Event()
    checks, notices, jobs, traces = [], [], [], []
    visible = False
    window = SimpleNamespace(after=lambda delay, callback: jobs.append(callback),
                             winfo_viewable=lambda: visible, _tk_shutdown_requested=False,
                             _render_logistics_connection_footer=lambda: None)

    def full_check(*_args, **_kwargs):
        checks.append(getattr(fence._WRITER_LOCAL, "depth", 0))
        if failed:
            raise onboarding.CurrentUserOnboardingError("verification failed", report_path=report["report_path"],
                                                       cause_code="BOOTSTRAP_INTEGRITY_INVALID")
        return dict(status="READY", direct_sync_root=str(steady_start.paths.direct_sync_root))

    monkeypatch.setattr(onboarding, "onboard_current_user", full_check)
    monkeypatch.setattr(user_relay, "verify_reused_user_relay", lambda *_a: completed.set())
    monkeypatch.setattr(gui, "_label_match_startup_trace", lambda stage, **details: traces.append(details))
    monkeypatch.setattr(gui, "_show_first_run_onboarding_error", notices.append)
    gui._schedule_reused_relay_verification(window, report)
    jobs.pop(0)()
    assert checks == []
    assert startup_send_deferred(steady_start.paths.direct_sync_root)
    visible = True
    jobs.pop(0)()
    for _ in range(100):
        if jobs:
            jobs.pop(0)()
        if checks and (notices if failed else not startup_send_hold_path(steady_start.paths.direct_sync_root).exists()):
            break
        time.sleep(0.01)
    assert checks == [0]
    assert steady_start.paths.ledger_path.read_bytes() == before
    if failed:
        assert notices[0].cause_code == "BOOTSTRAP_INTEGRITY_INVALID"
        assert window._startup_send_state == "FAILED"
        assert startup_send_deferred(steady_start.paths.direct_sync_root)
        assert onboarding.try_window_first_start(steady_start.paths.app_root, environ=steady_start.environment) is None
        assert any(row.get("cause_code") == "BOOTSTRAP_INTEGRITY_INVALID" for row in traces)
    else:
        assert completed.is_set()
        assert window._startup_send_state == "VERIFIED"
        assert startup_send_deferred(steady_start.paths.direct_sync_root) is None


def test_http_already_admitted_finishes_but_next_post_is_blocked(tmp_path):
    from startup_send_gate import StartupGatedSession, create_startup_send_hold, release_startup_send_hold
    root = tmp_path / "sync"
    calls, holds = [], []
    def post(*_a, **_k):
        calls.append("post")
        if not holds:
            holds.append(create_startup_send_hold(root))
        return "completed"
    transport = StartupGatedSession(root, SimpleNamespace(post=post))
    assert transport.post("https://example.invalid") == "completed"
    with pytest.raises(fence.WriterFencedError):
        transport.post("https://example.invalid")
    assert calls == ["post"]
    assert release_startup_send_hold(root, holds[0], verified=True)
    assert transport.post("https://example.invalid") == "completed"
    assert calls == ["post", "post"]


def test_verify_helper_allows_another_writer_during_slow_check(managed_onboarding, monkeypatch):
    import threading
    paths, _environment, control, _helpers = managed_onboarding
    admitted, errors = [], []
    def helper(*_a, **_k):
        assert getattr(fence._WRITER_LOCAL, "depth", 0) == 0
        def write():
            try:
                with fence.writer_admission("gui_settings_save", timeout_seconds=0.1):
                    admitted.append(True)
            except Exception as error:
                errors.append(type(error).__name__)
        thread = threading.Thread(target=write)
        thread.start()
        thread.join(2)
        assert not thread.is_alive()
        return SimpleNamespace(returncode=0, stdout="", stderr="")
    monkeypatch.setattr(subprocess, "run", helper)
    updates.fixed_helper(control, "Verify", code_root=paths.app_root)
    assert admitted == [True] and errors == []


def test_hosted_child_writer_timeout_is_deferred_without_data_changes(tmp_path, monkeypatch):
    import label_match_product_host as host
    from tools import direct_sync_relay_runner as runner
    root = tmp_path / "sync"
    root.mkdir()
    data = root / "source.csv"
    data.write_bytes(b"local-source\n")
    database = root / "queue.sqlite3"
    database.write_bytes(b"durable-queue")
    credential = root / "credential.json"
    credential.write_text("{}")
    manifest = root / "manifest.json"
    manifest.write_text("{}")
    status, log = root / "status.json", root / "relay.jsonl"
    def busy(*_a, **_k):
        raise fence.WriterFencedError("WRITER_GATE_TIMEOUT", "writer admission is busy")
    monkeypatch.setattr(runner, "main", busy)
    result = host.dispatch_product_mode([host.DIRECT_SYNC_RELAY_MODE,
        "--db-path", str(database), "--producer-manifest-path", str(manifest),
        "--credential-path", str(credential), "--spool-dir", str(root / "spool"),
        "--upload-status-dir", str(root / "upload_status"),
        "--enqueue-source-file", str(data), "--runtime-status-path", str(status), "--log-path", str(log)])
    assert result == 0
    recorded = json.loads(status.read_text())
    assert recorded["status"] == "DEFERRED" and recorded["reason_code"] == "WRITER_GATE_TIMEOUT"
    assert json.loads(log.read_text())["event"] == "hosted_relay_deferred"
    assert database.read_bytes() == b"durable-queue" and data.read_bytes() == b"local-source\n"


def test_next_full_start_clears_orphan_only_after_checks_and_instance_admission(steady_start, monkeypatch):
    import Label_Match as gui
    from startup_send_gate import create_startup_send_hold, startup_send_deferred
    root = steady_start.paths.direct_sync_root
    create_startup_send_hold(root)
    monkeypatch.setattr(updates, "native_process_identity", lambda _: {"status": "ABSENT"})
    assert onboarding.try_window_first_start(steady_start.paths.app_root, environ=steady_start.environment) is None
    monkeypatch.setattr(gui, "_label_match_runtime_app_root", lambda: steady_start.paths.app_root)
    monkeypatch.setattr(gui, "_first_run_onboarding_enabled", lambda: True)
    monkeypatch.setattr(gui, "verify_factory_contract_startup", lambda: None)
    monkeypatch.setattr(gui, "validate_pending_worker_install", lambda _: None)
    monkeypatch.setattr(gui, "check_user_data_open", lambda _: None)
    monkeypatch.setattr(gui, "release_identity", lambda _: {})
    monkeypatch.setattr(gui, "resolve_data_scope", lambda **_: "local")
    monkeypatch.setattr(updates, "startup_gate", lambda *_a, **_k: None)
    monkeypatch.setattr(gui, "prepare_startup_item_catalog", lambda: None)
    checks = []
    def full(*_a, **_k):
        assert startup_send_deferred(root)
        checks.append("full")
        return dict(status="READY", start_path="FULL", direct_sync_root=str(root), relay_start={"status": "ALIVE"})
    def admitted(callback, **_k):
        assert checks == ["full"] and startup_send_deferred(root)
        return callback()
    def window():
        assert startup_send_deferred(root) is None
        return SimpleNamespace(mainloop=lambda: None, title=lambda: "window", state=lambda: "normal",
                               _finish_after_mainloop=lambda: None)
    monkeypatch.setattr(gui, "onboard_current_user", full)
    monkeypatch.setattr(gui, "run_guarded_entrypoint", admitted)
    monkeypatch.setattr(gui, "Label_Match", window)
    assert gui.main([]) == 0
    assert startup_send_deferred(root) is None


def test_full_relay_reuse_observes_unchanged_process_only_once(monkeypatch, recent_relay):
    monkeypatch.setattr(updates, "native_process_identity", lambda _: {"status": "LIVE", "creation_identity": "1"})
    result = user_relay.reuse_or_start_user_relay_process(recent_relay.current, allow_recent_reuse=False, wait=lambda _: None)
    assert result["status"] == "ALIVE" and not result.get("fast_path")
    assert len(recent_relay.observed) == 1


def test_hold_created_during_lease_response_blocks_following_source_post(tmp_path, monkeypatch):
    import direct_sync_runtime as runtime
    from startup_send_gate import create_startup_send_hold
    root = tmp_path / "sync"
    root.mkdir()
    queue = root / "queue.sqlite3"
    queue.write_bytes(b"durable-queue")
    config = runtime.DirectSyncRuntimeConfig(queue, root / "spool", root / "manifest.json", root / "credential.json",
        root / "upload_status", root / "status.json", root / "relay.jsonl")
    monkeypatch.setattr(runtime, "_disk_pressure_report", lambda _: {"status": "pass"})
    monkeypatch.setattr(runtime, "reset_stale_relay_leases", lambda **_: 0)
    monkeypatch.setattr(runtime, "_producer_manifest_identity", lambda _: {"status": "PASS", "producer_install_id": "install"})
    def lease(**_kwargs):
        create_startup_send_hold(root)
        return SimpleNamespace(receipt={}, operator_review=False, error_code="")
    monkeypatch.setattr(runtime, "ensure_runtime_authority", lease)
    monkeypatch.setattr(runtime, "drain_one_relay_batch", lambda **_: pytest.fail("source post after hold"))
    assert runtime.run_relay_once(config, credentials=object())["status"] == "DEFERRED"
    assert queue.read_bytes() == b"durable-queue"


def test_full_onboarding_requires_observed_owner_before_window(steady_start, monkeypatch):
    from tests.test_onboarding_writer_admission import _onboard
    observed = []
    def relay(_root, **options):
        observed.append(options)
        return {"status": "ALIVE", "existing_relay_reused": True}
    monkeypatch.setattr(onboarding, "reuse_or_start_user_relay_process", relay)
    result = _onboard(steady_start.paths, steady_start.environment, relay_launcher=relay, _startup=True)
    assert result["status"] == "READY" and result["start_path"] == "FULL"
    assert observed == [{"allow_recent_reuse": False}]


@pytest.mark.parametrize("state,message", [
    ("PENDING", "서버 보내기 확인 중"), ("FAILED", "서버 보내기 멈춤 — 관리자에게 알려 주세요"),
])
def test_startup_notice_uses_the_visible_work_screen_footer(state, message):
    import Label_Match as gui
    rendered = {}
    window = SimpleNamespace(logistics_connection_label=SimpleNamespace(configure=lambda **fields: rendered.update(fields)),
                             _startup_send_state=state, _text_pixel_width=lambda *_args: 0,
                             winfo_width=lambda: 1920)
    gui.Label_Match._render_logistics_connection_footer(window)
    assert rendered["text"] == message
    window._startup_send_state = "VERIFIED"
    gui.Label_Match._render_logistics_connection_footer(window)
    assert rendered["text"] != message

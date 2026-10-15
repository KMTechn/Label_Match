from contextlib import contextmanager, nullcontext
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
from tests.label_screen_entry_double import fresh_screen_launch


@pytest.fixture(autouse=True)
def reset_window_start(monkeypatch):
    monkeypatch.setattr(onboarding, "_WINDOW_FIRST_START", None, raising=False)


def test_duplicate_shortcut_keeps_original_window_sending(steady_start, monkeypatch):
    import Label_Match as gui
    import label_match_single_instance as single
    from startup_send_gate import StartupGatedSession, startup_send_deferred
    sent, activated = [], []
    monkeypatch.setattr(gui, "_label_match_runtime_app_root", lambda: steady_start.paths.app_root)
    monkeypatch.setattr(gui, "_first_run_onboarding_enabled", lambda: True)
    for name in ("verify_factory_contract_startup", "validate_pending_worker_install", "check_user_data_open"):
        monkeypatch.setattr(gui, name, lambda *_: None)
    monkeypatch.setattr(gui, "release_identity", lambda _: {})
    monkeypatch.setattr(gui, "resolve_data_scope", lambda **_: "local")
    monkeypatch.setattr(gui, "_run_label_match_application", lambda **_: pytest.fail("second window"))
    def guarded(callback, **kwargs):
        return single.run_guarded_entrypoint(callback, **kwargs,
            acquire=lambda _: SimpleNamespace(owner=False),
            activate=lambda: activated.append(True) or SimpleNamespace(found=True))
    monkeypatch.setattr(gui, "run_guarded_entrypoint", guarded)
    assert gui.main([]) == 0
    assert activated == [True]
    assert startup_send_deferred(steady_start.paths.direct_sync_root) is None
    session = StartupGatedSession(steady_start.paths.direct_sync_root,
                                 SimpleNamespace(post=lambda *_a, **_k: sent.append(True)))
    session.post("https://example.invalid")
    assert sent == [True]


@pytest.mark.parametrize("operation", ["create", "cancel", "replace", "adopt", "exchange",
    "reconciliation", "print", "print_complete", "activate"])
def test_business_mutations_wait_at_transport_until_release(tmp_path, monkeypatch, operation):
    import uuid
    from package_logistics import PackageClientConfig, PackageLogisticsClient, PackageApiError
    from startup_send_gate import create_startup_send_hold, release_startup_send_hold
    root = tmp_path / "sync"
    monkeypatch.setenv("LABEL_MATCH_DIRECT_SYNC_ROOT", str(root))
    sent = []
    config = PackageClientConfig(base_url="https://example.invalid", token=uuid.uuid4().hex,
        authority_scope_id="scope", source_host_id="host", device_id="device")
    client = PackageLogisticsClient(config, transport=lambda method, *_a: sent.append(method) or {"ok": True, "data": {}})
    command = {"idempotency_key": "intent", "authority_scope_id": "scope", "payload": {"target_bundle_id": "transfer"}}
    operations = {
        "create": lambda: client.create_package(command),
        "cancel": lambda: client.cancel_package(command),
        "replace": lambda: client.replace_and_reseal_transfer(command),
        "adopt": lambda: client.adopt_phs_label(authority_scope_id="scope", qr_payload="label"),
        "exchange": lambda: client.prepare_phs_label_exchange(authority_scope_id="scope", exchange_kind="replace", sources=[], targets=[], idempotency_key="intent"),
        "reconciliation": lambda: client.prepare_phs_reconciliation_label_exchange("reconciliation", authority_scope_id="scope", action_ids=["action"], expected_reconciliation_version=1, idempotency_key="intent"),
        "print": lambda: client.request_phs_label_print("exchange", authority_scope_id="scope", label_id="label", idempotency_key="intent"),
        "print_complete": lambda: client.complete_phs_label_print("attempt", authority_scope_id="scope", succeeded=True),
        "activate": lambda: client.activate_phs_label_exchange("exchange", authority_scope_id="scope", expected_exchange_version=1),
    }
    hold = create_startup_send_hold(root)
    with pytest.raises(PackageApiError) as failure:
        operations[operation]()
    assert failure.value.retryable is True and failure.value.committed is False
    assert "잠시 뒤 다시" in failure.value.message
    assert sent == []
    assert release_startup_send_hold(root, hold, verified=True)
    operations[operation]()
    assert sent == ["POST"]


@pytest.mark.parametrize("operation", ["pin", "name"])
def test_admin_mutations_wait_until_release(tmp_path, monkeypatch, operation):
    import uuid
    from label_admin_pin import AdminPinClient, AdminPinError, operator_message
    from startup_send_gate import create_startup_send_hold, release_startup_send_hold
    root = tmp_path / "sync"
    monkeypatch.setenv("LABEL_MATCH_DIRECT_SYNC_ROOT", str(root))
    sent = []
    credentials = SimpleNamespace(endpoint_url="https://example.invalid/ingest",
        producer_id="producer", key_id="key-id", secret=uuid.uuid4().hex)
    client = AdminPinClient(credentials, transport=lambda *_a: sent.append(True) or (201, b'{"ok":true}'))
    request = {"mode": "create", "authority_scope_id": "scope", "request_id": str(uuid.uuid4()),
        "display_name": "worker", "action_at": "2026-10-13T09:00:00Z", "temp_uuid": str(uuid.uuid4())}
    invoke = lambda: client.post("verifications", {}) if operation == "pin" else client.register_roster(request)
    hold = create_startup_send_hold(root)
    with pytest.raises(AdminPinError) as failure:
        invoke()
    assert "잠시 뒤 다시" in operator_message(failure.value.code)
    assert sent == []
    assert release_startup_send_hold(root, hold, verified=True)
    invoke()
    assert sent == [True]


def _initial_load_stub(gui):
    import queue
    return SimpleNamespace(initial_load_queue=queue.Queue(),
        _reconcile_package_cancellation_local_events=lambda: None,
        _reconcile_phs_replacement_waiting_events=lambda: None,
        _reconcile_post_review_required_events=lambda: None,
        _load_items_data=lambda: {"bundled": True})


def test_fast_catalog_uses_full_offline_fallback(steady_start, monkeypatch):
    import Label_Match as gui
    from item_catalog_sync import ItemCatalogSyncError
    report = onboarding.try_window_first_start(steady_start.paths.app_root, environ=steady_start.environment)
    report["window_visible"].set()
    attempts, notices = [], []
    def unavailable():
        attempts.append(True)
        raise ItemCatalogSyncError("offline", cause_code="SNAPSHOT_UNAVAILABLE_AFTER_VERIFY")
    monkeypatch.setattr(gui, "prepare_startup_item_catalog", unavailable)
    monkeypatch.setattr(gui, "offline_catalog_startable", lambda **_: True)
    monkeypatch.setattr(gui, "_start_without_confirmed_item_catalog", notices.append)
    app = _initial_load_stub(gui)
    gui.Label_Match._async_initial_load(app, report)
    result = app.initial_load_queue.get_nowait()
    assert result["items"] == {"bundled": True}
    assert result["catalog_unconfirmed"] is True
    assert attempts == [True] and len(notices) == 1


def test_fast_catalog_keeps_start_report_after_background_clears_global(steady_start, monkeypatch):
    import Label_Match as gui
    report = onboarding.try_window_first_start(steady_start.paths.app_root, environ=steady_start.environment)
    report["window_visible"].set()
    onboarding._WINDOW_FIRST_START = None
    refreshed = []
    monkeypatch.setattr(gui, "prepare_startup_item_catalog", lambda: refreshed.append(True))
    app = _initial_load_stub(gui)
    gui.Label_Match._async_initial_load(app, report)
    assert refreshed == [True]
    assert "error" not in app.initial_load_queue.get_nowait()


def test_fast_catalog_retry_runs_on_ui_thread_with_same_start_report(steady_start, monkeypatch):
    import Label_Match as gui
    import threading
    from item_catalog_sync import ItemCatalogSyncError
    report = onboarding.try_window_first_start(steady_start.paths.app_root, environ=steady_start.environment)
    report["window_visible"].set()
    attempts, prompts, destroyed, scheduled = [], [], [], []
    def prepare():
        attempts.append(True)
        if len(attempts) == 1:
            raise ItemCatalogSyncError("offline", cause_code="SNAPSHOT_UNAVAILABLE_AFTER_VERIFY")
    monkeypatch.setattr(gui, "prepare_startup_item_catalog", prepare)
    monkeypatch.setattr(gui, "offline_catalog_startable", lambda **_: False)
    monkeypatch.setattr(gui, "_offer_item_catalog_startup_retry", lambda _: prompts.append(threading.get_ident()) or True)
    app = _initial_load_stub(gui)
    app._initial_load_startup_report = report
    app._async_initial_load = lambda saved: gui.Label_Match._async_initial_load(app, saved)
    app._process_initial_load_queue = lambda: None
    app.after = lambda delay, callback: scheduled.append(callback)
    app.hide_loading_overlay = lambda: None
    app.destroy = lambda: destroyed.append(True)
    app._render_operator_workbench = lambda: None
    app._retry_initial_load = lambda: gui.Label_Match._retry_initial_load(app)
    gui.Label_Match._async_initial_load(app, report)
    assert prompts == []
    onboarding._WINDOW_FIRST_START = None
    gui.Label_Match._process_initial_load_queue(app)
    app._initial_load_thread.join(2)
    assert not app._initial_load_thread.is_alive()
    assert prompts == [threading.get_ident()] and attempts == [True, True]
    assert app.initial_load_queue.get_nowait()["items"] == {"bundled": True}
    assert len(scheduled) == 1 and destroyed == []


@pytest.mark.parametrize("operation", ["create", "cancel"])
def test_business_outbox_remains_pending_then_acks_after_release(tmp_path, monkeypatch, operation):
    import uuid
    import package_logistics as package
    from tests.test_package_logistics import _draft, _receipt, _source_evidence, _ack_package_creation, _cancellation_intent, _cancellation_receipt, SCOPE, TRANSFER
    from startup_send_gate import create_startup_send_hold, release_startup_send_hold
    root = tmp_path / "sync"
    monkeypatch.setenv("LABEL_MATCH_DIRECT_SYNC_ROOT", str(root))
    sent = []
    draft = _draft()
    outbox = package.PackageOutbox(tmp_path / "outbox.sqlite3")
    config = package.PackageClientConfig(base_url="https://example.invalid", token=uuid.uuid4().hex,
        authority_scope_id=SCOPE, source_host_id="host", device_id="device")
    receipt = _receipt(draft) if operation == "create" else _cancellation_receipt(draft)
    client = package.PackageLogisticsClient(config,
        transport=lambda method, *_a: sent.append(method) or {"ok": True, "data": receipt})
    monkeypatch.setattr(client, "get_receipt_if_exists", lambda *_a, **_k: None)
    if operation == "create":
        row = outbox.enqueue(draft)
        outbox.mark_local_completion_committed(row["idempotency_key"])
        command = {"contract_version": "logistics-v1", "command_type": "CREATE_PACKAGE",
            "authority_scope_id": SCOPE, "authority_epoch": 5, "ledger_plane": "AUTHORITATIVE",
            "plane_epoch": 3, "idempotency_key": row["idempotency_key"],
            "expected_versions": {f"bundle:{TRANSFER}": 7},
            "payload": {"source_bundle_id": TRANSFER, "package_bundle_id": draft.package_bundle_id,
                        "source_evidence": _source_evidence()}}
        monkeypatch.setattr(client, "build_create_package_command", lambda *_a, **_k: (TRANSFER, command))
        processor = package.PackageOutboxProcessor(outbox, client)
    else:
        _ack_package_creation(outbox, draft)
        outbox = package.PackageCancellationOutbox(tmp_path / "outbox.sqlite3")
        row = outbox.enqueue(_cancellation_intent(draft))
        outbox.mark_local_event_committed(row["cancellation_event_id"])
        processor = package.PackageCancellationOutboxProcessor(outbox, client)
    monkeypatch.setattr(package, "_utc_after", lambda _: package.utc_now())
    def status():
        saved = outbox.get_by_set_id(draft.set_id)
        return (saved if operation == "create" else saved[0])["status"]
    hold = create_startup_send_hold(root)
    assert processor.drain(limit=1)["retry"] == 1
    assert sent == []
    assert status() == "PENDING"
    assert release_startup_send_hold(root, hold, verified=True)
    assert processor.drain(limit=1)["acked"] == 1
    assert sent == ["POST"]
    assert status() == "ACKED"


def test_name_registration_outbox_preserves_pending_request(tmp_path, monkeypatch):
    import uuid
    from label_admin_pin import AdminPinClient, AdminPinError
    from label_worker_roster import WorkerRoster
    from startup_send_gate import create_startup_send_hold, release_startup_send_hold
    root = tmp_path / "sync"
    monkeypatch.setenv("LABEL_MATCH_DIRECT_SYNC_ROOT", str(root))
    store = WorkerRoster(tmp_path / "roster.json", "scope")
    store.register_local("worker")
    request = store.pending_requests()[0]
    sent = []
    credentials = SimpleNamespace(endpoint_url="https://example.invalid/ingest",
        producer_id="producer", key_id="key-id", secret=uuid.uuid4().hex)
    client = AdminPinClient(credentials, transport=lambda *_a: sent.append(True) or (201, b'{"ok":true}'))
    hold = create_startup_send_hold(root)
    with pytest.raises(AdminPinError) as failure:
        client.register_roster(request)
    store.apply_registration(request, error=failure.value)
    assert store.pending_requests() == [request] and sent == []
    assert release_startup_send_hold(root, hold, verified=True)
    client.register_roster(store.pending_requests()[0])
    assert sent == [True]


def test_phs_wait_survives_typed_ui_failure_without_conflict_instruction():
    import Label_Match as gui
    from package_logistics import PackageApiError
    from phs_label_workflow import PHSLabelWorkflowError
    refused = PackageApiError(425, "STARTUP_VERIFICATION_PENDING", "pending", retryable=True, committed=False)
    wrapped = PHSLabelWorkflowError("RECOVERY_FAILED", "recovery failed")
    wrapped.__cause__ = refused
    failure = gui._phs_exchange_failure(wrapped)
    assert failure.retryable is True and failure.commit_state == "unknown"
    assert "잠시 뒤 다시" in gui._startup_send_wait_message(failure, "review")


@pytest.fixture
def failed_startup_window(steady_start, monkeypatch):
    import Label_Match as gui
    import threading
    report = onboarding.try_window_first_start(steady_start.paths.app_root, environ=steady_start.environment)
    onboarding.admit_window_first_start(report)
    jobs, calls = [], []
    state = SimpleNamespace(fail=False, block=False, entered=threading.Event(), proceed=threading.Event())
    app = SimpleNamespace(after=lambda _delay, callback: jobs.append(callback),
        winfo_viewable=lambda: True, _tk_shutdown_requested=False,
        _render_logistics_connection_footer=lambda: None)
    def full_check(*_a, **_k):
        calls.append(threading.get_ident())
        assert getattr(fence._WRITER_LOCAL, "depth", 0) == 0
        if len(calls) > 1 and state.block:
            state.entered.set()
            assert state.proceed.wait(2)
        return {"status": "READY"}
    def observe(*_a, **_k):
        if len(calls) == 1 or state.fail:
            raise RuntimeError("relay verification unavailable")
    monkeypatch.setattr(onboarding, "onboard_current_user", full_check)
    monkeypatch.setattr(user_relay, "verify_reused_user_relay", observe)
    monkeypatch.setattr(gui, "_show_first_run_onboarding_error", lambda _: None)
    gui._schedule_reused_relay_verification(app, report)
    _finish_verification_jobs(app, jobs, "FAILED")
    yield SimpleNamespace(app=app, report=report, jobs=jobs, calls=calls, state=state)
    state.proceed.set()


def _finish_verification_jobs(app, jobs, expected):
    for _ in range(200):
        if jobs:
            jobs.pop(0)()
        if app._startup_send_state == expected and not jobs:
            return
        time.sleep(0.005)
    pytest.fail("startup verification did not finish")


def test_retry_verification_success_releases_same_hold_and_resumes(failed_startup_window):
    import Label_Match as gui
    import threading
    from startup_send_gate import StartupGatedSession, startup_send_deferred
    item = failed_startup_window
    root = item.report["paths"].direct_sync_root
    sent = []
    session = StartupGatedSession(root, SimpleNamespace(post=lambda *_a, **_k: sent.append(True)))
    with pytest.raises(fence.WriterFencedError):
        session.post("https://example.invalid")
    assert gui.Label_Match._retry_startup_send_verification(item.app)
    _finish_verification_jobs(item.app, item.jobs, "VERIFIED")
    assert len(item.calls) == 2 and all(owner != threading.get_ident() for owner in item.calls)
    assert startup_send_deferred(root) is None
    session.post("https://example.invalid")
    assert sent == [True]


def test_retry_verification_failure_keeps_original_hold(failed_startup_window):
    import Label_Match as gui
    from startup_send_gate import startup_send_hold_path, startup_send_deferred
    item = failed_startup_window
    root = item.report["paths"].direct_sync_root
    before = startup_send_hold_path(root).read_bytes()
    item.state.fail = True
    assert gui.Label_Match._retry_startup_send_verification(item.app)
    _finish_verification_jobs(item.app, item.jobs, "FAILED")
    assert len(item.calls) == 2
    assert startup_send_hold_path(root).read_bytes() == before
    assert startup_send_deferred(root)
    assert onboarding.try_window_first_start(item.report["app_root"]) is None


def test_retry_verification_rapid_clicks_run_one_background_check(failed_startup_window):
    import Label_Match as gui
    from startup_send_gate import startup_send_deferred
    item = failed_startup_window
    item.state.block = True
    assert gui.Label_Match._retry_startup_send_verification(item.app)
    assert not gui.Label_Match._retry_startup_send_verification(item.app)
    item.jobs.pop(0)()
    assert item.state.entered.wait(1)
    assert all(not gui.Label_Match._retry_startup_send_verification(item.app) for _ in range(10))
    assert len(item.calls) == 2 and startup_send_deferred(item.report["paths"].direct_sync_root)
    item.state.proceed.set()
    _finish_verification_jobs(item.app, item.jobs, "VERIFIED")
    assert len(item.calls) == 2


def test_pin_startup_deferral_explains_attempt_is_not_counted():
    from label_admin_pin import operator_message
    message = operator_message("STARTUP_VERIFICATION_PENDING")
    assert message.endswith("이 건만 보류 — 틀린 횟수로 세지 않음")


@pytest.fixture
def steady_start(managed_onboarding, monkeypatch):
    import label_data_generation as generation
    paths, environment, control, helpers = managed_onboarding
    onboarding.apply_current_user_runtime_environment(paths, environ=environment)
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    paths.ledger_path.parent.mkdir(parents=True, exist_ok=True)
    paths.ledger_path.write_bytes(b"local-ledger")
    paths.settings_path.parent.mkdir(parents=True, exist_ok=True)
    paths.settings_path.write_text("{}", encoding="utf-8")
    manifest = updates.read_json(paths.app_root / "portable-manifest.json")
    manifest.update(schema="label-match-portable-tree-v1", version=generation.CURRENT_VERSION, startup_update_protocol=1)
    (paths.app_root / "portable-manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    paths.onboarding_report_path.parent.mkdir(parents=True, exist_ok=True)
    paths.onboarding_report_path.write_text(json.dumps({
        "status": "READY", "state_scope": "current_user", "data_root": str(paths.data_root),
        "direct_sync_root": str(paths.direct_sync_root), "ledger_path": str(paths.ledger_path),
        "settings_path": str(paths.settings_path), "logistics_profile_path": str(paths.logistics_profile_path),
        "bootstrap_integrity": {"status": "VERIFIED"},
    }), encoding="utf-8")
    policy = updates.load_policy(control)
    monkeypatch.setattr(updates, "current_process_sid", lambda: policy["operator_sid"])
    metadata = dict(process_id=os.getpid(), worker_id=user_relay.LABEL_MATCH_WORKER_ID,
                    app_root=str(control.parent / "current"), code_root=str(paths.app_root),
                    direct_sync_root=str(paths.direct_sync_root), queue_dir=str(paths.direct_sync_root / "queue"),
                    **updates.relay_ready_metadata(paths.app_root / "app"))
    class Published(Exception):
        pass
    def published(_seconds):
        raise Published
    # Only admission is a facade; keep the real atomic status publisher.
    with monkeypatch.context() as publisher:
        publisher.setattr(user_relay, "writer_admission", lambda *_a, **_k: nullcontext())
        with pytest.raises(Published):
            user_relay.run_persistent_relay_loop(lambda: {"status": "READY"},
                status_path=user_relay.user_relay_status_path(paths.direct_sync_root),
                interval_seconds=1, metadata=metadata, wait=published)
    status = updates.read_json(user_relay.user_relay_status_path(paths.direct_sync_root))
    assert status["status"] == "RUNNING"
    monkeypatch.setattr(user_relay, "_acquire_relay_lease", lambda _: None)
    monkeypatch.setattr(updates, "native_process_identity", lambda pid: {
        "status": "LIVE", "creation_identity": str(pid),
    })
    return SimpleNamespace(paths=paths, environment=environment, control=control, helpers=helpers, status=status)


def _product_apply_claim(item, monkeypatch, *, settled=True):
    # Use the actual durable writer with isolated authority, not host authority.
    with monkeypatch.context() as writer:
        writer.setattr(updates, "write_json_durable", updates.write_json_durable.__wrapped__)
        updates.write_json_durable(item.control / "apply-claim.json", updates.new_apply_claim())
        if settled:
            updates.complete_apply_claim(item.control)
    assert updates.read_json(item.control / "apply-claim.json")["status"] == ("SETTLED" if settled else "RUNNING")


def _watch_window_front(monkeypatch):
    def forbidden(*_a, **_k):
        pytest.fail("full verification or registration before window")
    for name in ("fixed_helper", "_run_fixed_helper", "lifecycle_task_observation",
                 "version_writer_inventory_sha256", "native_process_identity"):
        monkeypatch.setattr(updates, name, forbidden)
    for name in ("verify_bootstrap_integrity", "_calculate_code_root_hash", "_file_sha256", "_registration_runner"):
        monkeypatch.setattr(onboarding, name, forbidden)
    import requests
    monkeypatch.setattr(requests.sessions.Session, "request", forbidden)


@pytest.mark.parametrize("settled_claim", [False, True], ids=["absent", "product-settled"])
def test_fast_start_has_no_helpers_and_records_shared_hold(steady_start, monkeypatch, record_testsuite_property, settled_claim):
    from startup_send_gate import startup_send_deferred, startup_send_hold_path
    monkeypatch.setattr(subprocess, "run", lambda *_a, **_k: pytest.fail("child before window"))
    monkeypatch.setattr(onboarding, "verify_bootstrap_integrity", lambda *_a, **_k: pytest.fail("hash before window"))
    if settled_claim:
        _product_apply_claim(steady_start, monkeypatch)
    started = time.perf_counter()
    with monkeypatch.context() as front:
        _watch_window_front(front)
        result = onboarding.try_window_first_start(steady_start.paths.app_root, environ=steady_start.environment)
    elapsed = time.perf_counter() - started
    assert elapsed < 2
    record_testsuite_property("window_first_admission_ms", round(elapsed * 1000, 3))
    assert result["start_path"] == "FAST_WINDOW_FIRST"
    assert result["relay_start"]["status"] == "ALIVE"
    assert result["existing_relay_reused"] is True and result["fast_path"] is True
    assert steady_start.helpers == []
    assert startup_send_deferred(steady_start.paths.direct_sync_root) is None
    onboarding.admit_window_first_start(result)
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


@pytest.mark.parametrize("settled_claim", [False, True], ids=["absent", "product-settled"])
def test_fast_main_skips_full_checks_and_reports_top_level_path(steady_start, monkeypatch, settled_claim):
    import Label_Match as gui
    if settled_claim:
        _product_apply_claim(steady_start, monkeypatch)
    _watch_window_front(monkeypatch)
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
    onboarding.admit_window_first_start(report)
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


@pytest.mark.parametrize("ui_arguments", [(), ("--ui", "tk"), ("--old-screen",)])
def test_next_full_start_clears_orphan_only_after_checks_and_instance_admission(steady_start, monkeypatch, ui_arguments):
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
    from tests.label_screen_entry_double import install_screen_owner_double
    screen_routes = install_screen_owner_double(monkeypatch, gui)
    assert gui.main(ui_arguments) == 0
    assert startup_send_deferred(root) is None
    assert screen_routes == (["new"] if not ui_arguments else [])


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
    ("PENDING", "서버 보내기 확인 중"), ("FAILED", "서버 보내기가 멈췄습니다. 앱을 껐다 다시 켜 주세요(계속되면 관리자에게)"),
])
def test_startup_notice_uses_the_visible_work_screen_footer(state, message):
    import Label_Match as gui
    rendered, button = {}, {}
    retry_button = SimpleNamespace(configure=lambda **fields: button.update(fields),
        grid=lambda: button.update(visible=True), grid_remove=lambda: button.update(visible=False),
        winfo_reqwidth=lambda: 80)
    window = SimpleNamespace(logistics_connection_label=SimpleNamespace(configure=lambda **fields: rendered.update(fields)),
                             startup_send_retry_button=retry_button,
                             _startup_send_state=state, _text_pixel_width=lambda *_args: 0,
                             winfo_width=lambda: 1920)
    gui.Label_Match._render_logistics_connection_footer(window)
    assert rendered["text"] == message
    assert button == {"visible": True, "state": "normal" if state == "FAILED" else "disabled"}
    if state == "FAILED":
        assert rendered["wraplength"] > 0
    window._startup_send_state = "VERIFIED"
    gui.Label_Match._render_logistics_connection_footer(window)
    assert rendered["text"] != message
    assert button["visible"] is False

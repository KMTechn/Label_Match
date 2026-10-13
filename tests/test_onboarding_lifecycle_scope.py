"""Managed onboarding validates once and cheaply checks every later write."""
import json
from pathlib import Path
import subprocess
import threading
import time

import pytest

import auto_update
import current_user_onboarding as onboarding
import writer_session_fence as fence
import user_relay
from tests.test_current_user_onboarding import (
    isolated_data_generation_release, _environment, _ready_state,
)
from tests.test_onboarding_writer_admission import _onboard
from tests.test_supported_lifecycle import healthy

_STOCK_FIXED_HELPER = auto_update.fixed_helper
_STOCK_TASK_OBSERVATION = auto_update.lifecycle_task_observation


def _stock_helper_counter(monkeypatch, helpers, *, native=False):
    """Retain stock decorators; replace only external helper payloads."""
    actual_run = subprocess.run
    actual_acquire = fence._acquire_named_mutex
    actual_release = fence._NamedMutexLease.release
    actual_suspend = fence._NamedMutexLease.suspend
    actual_resume = fence._NamedMutexLease.resume
    measuring = threading.local()
    acquisitions, holds = {}, []

    def acquire(*args, **kwargs):
        lease = actual_acquire(*args, **kwargs)
        if lease is not None and getattr(measuring, "helper", False):
            acquisitions[id(lease)] = time.monotonic()
        return lease

    def release(lease):
        began = acquisitions.pop(id(lease), None)
        result = actual_release(lease)
        if began is not None:
            holds.append(time.monotonic() - began)
        return result

    def suspend(lease):
        began = acquisitions.pop(id(lease), None)
        result = actual_suspend(lease)
        if began is not None:
            holds.append(time.monotonic() - began)
        return result

    def resume(lease, *args, **kwargs):
        result = actual_resume(lease, *args, **kwargs)
        if getattr(measuring, "helper", False):
            acquisitions[id(lease)] = time.monotonic()
        return result

    def fixed(*args, **kwargs):
        parent_depth = getattr(fence._WRITER_LOCAL, "depth", 0)
        measuring.helper = True
        measuring.parent_depth = parent_depth
        try:
            return _STOCK_FIXED_HELPER(*args, **kwargs)
        finally:
            measuring.helper = False

    def run(arguments, *args, **kwargs):
        if ("-Mode" not in arguments or "-File" not in arguments
                or not str(arguments[arguments.index("-File") + 1]).endswith("apply_prepared_update.ps1")):
            return actual_run(arguments, *args, **kwargs)
        mode = arguments[arguments.index("-Mode") + 1]
        assert mode in {"Verify", "ObserveLifecycle"}
        root = arguments[arguments.index("-CodeRoot") + 1] if "-CodeRoot" in arguments else arguments[arguments.index("-InstallRoot") + 1]
        helpers.append({"mode": mode, "root": str(root), "thread": threading.get_ident(),
            "admission_depth": getattr(fence._WRITER_LOCAL, "depth", 0),
            "admission_source": getattr(fence._WRITER_LOCAL, "source", None),
            "parent_admission_depth": getattr(measuring, "parent_depth", 0) if mode == "Verify" else 0})
        output = '{"task_exists":true}' if mode == "ObserveLifecycle" else "COUNTER"
        if native:
            return actual_run([arguments[0], "-NoLogo", "-NoProfile", "-NonInteractive",
                "-WindowStyle", "Hidden", "-Command", "[Console]::Write('" + output + "')"],
                capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW)
        time.sleep(0.025)
        return subprocess.CompletedProcess(arguments, 0, stdout=output, stderr="")

    monkeypatch.setattr(fence, "_acquire_named_mutex", acquire)
    monkeypatch.setattr(fence._NamedMutexLease, "release", release)
    monkeypatch.setattr(fence._NamedMutexLease, "suspend", suspend)
    monkeypatch.setattr(fence._NamedMutexLease, "resume", resume)
    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(auto_update, "fixed_helper", fixed)
    monkeypatch.setattr(auto_update, "lifecycle_task_observation", _STOCK_TASK_OBSERVATION)
    return holds


@pytest.fixture
def managed_onboarding(tmp_path, monkeypatch):
    fixed, selected, control, _verified = healthy(tmp_path / "machine", monkeypatch)
    environment = _environment(tmp_path)
    paths = onboarding.resolve_current_user_onboarding_paths(selected, environ=environment)
    _ready_state(paths)
    # Use the real installed-source/control-root selection and real resolver.
    # Only the three external helpers are counted, without touching host tasks.
    monkeypatch.setattr(fence, "__file__", str(selected / "app/writer_session_fence.py"))
    monkeypatch.setattr(auto_update, "runtime_matches", lambda _root: True)
    monkeypatch.setattr(auto_update, "ordinary_user_context", lambda *_args: True)
    helpers = []

    _stock_helper_counter(monkeypatch, helpers)
    return paths, environment, control, helpers


@pytest.mark.parametrize("parent", [False, True], ids=["ordinary-start", "parent-admission"])
def test_one_managed_onboarding_uses_one_lifecycle_helper_sequence(managed_onboarding, tmp_path, parent):
    from contextlib import nullcontext
    paths, environment, control, helpers = managed_onboarding
    assert auto_update.control_root(paths.app_root / "app") == control
    started = time.monotonic()
    with fence.writer_admission("gui_settings_save") if parent else nullcontext():
        report = _onboard(paths, environment,
                          relay_launcher=lambda _root: {"status": "ALIVE", "process_id": 77})
    elapsed = time.monotonic() - started
    (tmp_path / "lifecycle-helper-timing.json").write_text(json.dumps({
        "managed_control_exists": control.is_dir(), "status": report["status"],
        "helper_calls": len(helpers), "total_seconds": elapsed,
        "counter_delay_seconds_per_helper": 0.025, "helpers": helpers,
        "actual_powershell_helpers_executed": False,
    }, indent=2), encoding="utf-8")
    assert report["status"] == "READY"
    assert [row["mode"] for row in helpers] == ["Verify", "ObserveLifecycle"]
    assert all(row["parent_admission_depth"] == 0 for row in helpers)
    assert [row["admission_depth"] for row in helpers] == [0, 0]
    assert [row["admission_source"] for row in helpers] == [None, None]
    assert getattr(fence._WRITER_LOCAL, "onboarding_lifecycle", None) is None


@pytest.mark.parametrize("name", ["policy.json", "active.json", "transition.json", "delegation"])
def test_each_write_denies_changed_lifecycle_without_another_helper(managed_onboarding, monkeypatch, name):
    paths, environment, control, helpers = managed_onboarding
    delegation = [None]
    if name == "delegation":
        monkeypatch.setattr(fence, "active_fence", lambda *_args, **_kwargs: delegation[0])
        monkeypatch.setattr(fence, "_delegation_matches", lambda *_args, **_kwargs: True)

    def launcher(_root):
        if name == "delegation":
            delegation[0] = {"delegated_sources": ["current_user_onboarding"]}
        else:
            path = control / name
            if name == "transition.json":
                changed = json.loads(path.read_text(encoding="utf-8"))
                changed["phase"] = "UNKNOWN"
                path.write_text(json.dumps(changed), encoding="utf-8")
            else:
                path.write_bytes(path.read_bytes() + b"\n")
        return {"status": "ALIVE", "process_id": 77}

    with pytest.raises(onboarding.CurrentUserOnboardingError) as caught:
        _onboard(paths, environment, relay_launcher=launcher)
    cause = caught.value
    codes = []
    while cause is not None:
        codes.append(getattr(cause, "code", None))
        cause = cause.__cause__
    assert "WRITER_LIFECYCLE_CHANGED" in codes
    assert len(helpers) == 2
    assert onboarding.REQUIRED_ENV not in environment
    assert not paths.onboarding_report_path.exists()
    assert getattr(fence._WRITER_LOCAL, "onboarding_lifecycle", None) is None


def test_next_onboarding_call_validates_again(managed_onboarding):
    paths, environment, _control, helpers = managed_onboarding
    for call in range(2):
        report = _onboard(paths, environment,
                          relay_launcher=lambda _root: {"status": "ALIVE", "process_id": 77})
        assert report["status"] == "READY"
        assert len(helpers) == (call + 1) * 2
        assert getattr(fence._WRITER_LOCAL, "onboarding_lifecycle", None) is None


def test_scope_does_not_reuse_observation_for_other_thread_or_source(managed_onboarding, monkeypatch):
    paths, environment, _control, helpers = managed_onboarding
    other_sources = []
    original = auto_update.assert_version_writer

    def assertion(*args, **kwargs):
        other_sources.append(kwargs["source"])
        return original(*args, **kwargs)

    monkeypatch.setattr(auto_update, "assert_version_writer", assertion)
    errors = []
    caller_thread = threading.get_ident()

    def launcher(_root):
        for _ in range(2):
            with fence.writer_admission("gui_settings_save"):
                pass

        def other_thread():
            try:
                assert getattr(fence._WRITER_LOCAL, "onboarding_lifecycle", None) is None
                with fence.writer_admission("current_user_onboarding"):
                    pass
            except BaseException as exc:
                errors.append(exc)
        thread = threading.Thread(target=other_thread)
        thread.start()
        thread.join(timeout=10)
        assert not thread.is_alive()
        return {"status": "ALIVE", "process_id": 77}

    assert _onboard(paths, environment, relay_launcher=launcher)["status"] == "READY"
    assert not errors
    assert len([row for row in helpers if row["thread"] == caller_thread]) == 2
    assert len([row for row in helpers if row["thread"] != caller_thread]) == 2
    assert other_sources.count("gui_settings_save") == 2
    assert other_sources.count("current_user_onboarding") == 1


def test_stock_helper_decorator_hold_with_native_powershell_counter(monkeypatch, tmp_path):
    fixed, selected, control, _verified = healthy(tmp_path / "native-machine", monkeypatch)
    environment = _environment(tmp_path / "native-user")
    paths = onboarding.resolve_current_user_onboarding_paths(selected, environ=environment)
    _ready_state(paths)
    monkeypatch.setattr(fence, "__file__", str(selected / "app/writer_session_fence.py"))
    monkeypatch.setattr(auto_update, "runtime_matches", lambda _root: True)
    monkeypatch.setattr(auto_update, "ordinary_user_context", lambda *_args: True)
    helpers = []
    holds = _stock_helper_counter(monkeypatch, helpers, native=True)
    started = time.monotonic()
    report = _onboard(paths, environment,
                      relay_launcher=lambda _root: {"status": "ALIVE", "process_id": 77})
    assert report["status"] == "READY"
    assert [row["mode"] for row in helpers] == ["Verify", "ObserveLifecycle"]
    assert len(holds) == 2
    assert all(row["parent_admission_depth"] == 0 for row in helpers)
    (tmp_path / "stock-helper-admission-timing.json").write_text(json.dumps({
        "status": report["status"], "helper_calls": len(helpers),
        "total_seconds": time.monotonic() - started,
        "verify_admission_seconds": holds, "verify_admission_max_seconds": max(holds),
        "verify_admission_sum_seconds": sum(holds), "native_powershell_children": 2,
        "stock_decorator_retained": True, "stock_verify_payload_executed": False,
        "payload_scope": "isolated native PowerShell counter; installed Verify work not timed",
    }, indent=2), encoding="utf-8")


@pytest.mark.parametrize("existing_stop", [False, True], ids=["after-new-registration", "with-removal-stop"])
def test_next_start_reuses_registration_after_fence_interrupts_before_ready(tmp_path, monkeypatch, existing_stop):
    from tests.test_supported_lifecycle import _canonical_user_setup
    from tests.test_writer_session_fence import _payload
    from tests.test_current_user_onboarding import (
        _profile_loader, _credential_loader, _ledger_factory,
        _scheduled_task_absent, _legacy_task_quiescent,
    )
    helpers = []
    if existing_stop:
        fixed, _selected, environment, paths = _canonical_user_setup(tmp_path / "machine", monkeypatch)
        onboarding.remove_current_user_setup(fixed, environ=environment,
            autostart_remover=lambda: {"status": "ABSENT"},
            scheduled_task_remover=_scheduled_task_absent)
        # The installed resolver stays real; count its three external helpers.
        monkeypatch.setattr(auto_update, "fixed_helper", lambda _control, mode, **kw:
            helpers.append(mode) or "")
        monkeypatch.setattr(auto_update, "lifecycle_task_observation", lambda root:
            helpers.append("ObserveLifecycle") or {"task_exists": True})
        app_root = fixed
    else:
        app_root = tmp_path / "hardened"
        app_root.mkdir()
        (app_root / "Label_Match.exe").write_bytes(b"synthetic-executable")
        environment = _environment(tmp_path)
        paths = onboarding.resolve_current_user_onboarding_paths(app_root, environ=environment)

    marker = user_relay.user_relay_stop_path(paths.direct_sync_root)
    assert marker.exists() is existing_stop
    registrations, autostarts, stored_run = [], [], {}
    control = fence.canonical_control_root()

    def register(selected):
        registrations.append(selected)
        _ready_state(selected)
        return 0

    def autostart(root):
        result = user_relay.install_user_relay_autostart(root,
            setter=lambda value: stored_run.update(value=value),
            getter=lambda: stored_run.get("value", ""))
        autostarts.append(dict(result))
        if len(autostarts) == 1:
            control.mkdir(parents=True, exist_ok=True)
            (control / fence.ACTIVE_FILENAME).write_text(
                json.dumps(_payload(delegated_sources=[])), encoding="utf-8")
        return result

    kwargs = dict(environ=environment, require_bootstrap_integrity=False,
        registration_runner=register, profile_loader=_profile_loader,
        credential_loader=_credential_loader,
        ledger_factory=lambda path: _ledger_factory(path) if not path.exists() else None,
        autostart_installer=autostart, scheduled_task_remover=_scheduled_task_absent,
        legacy_task_quiescence_reader=_legacy_task_quiescent,
        relay_launcher=lambda _root: {"status": "ALIVE", "process_id": 77})
    with pytest.raises(onboarding.CurrentUserOnboardingError) as caught:
        onboarding.onboard_current_user(app_root, **kwargs)
    cause = caught.value
    codes = []
    while cause is not None:
        codes.append(getattr(cause, "code", None))
        cause = cause.__cause__
    assert {"WRITER_LIFECYCLE_CHANGED", "ACTIVE_WRITER_FENCE"}.intersection(codes)
    assert len(registrations) == (0 if existing_stop else 1)
    assert marker.exists() is existing_stop
    assert onboarding.REQUIRED_ENV not in environment
    identity_before = paths.identity_path.read_bytes()
    run_before = stored_run["value"]
    assert run_before == autostarts[0]["command"]
    if existing_stop:
        assert helpers == ["Verify", "ObserveLifecycle"]
    (control / fence.ACTIVE_FILENAME).unlink()

    ready = onboarding.onboard_current_user(app_root, **kwargs)
    assert ready["status"] == "READY"
    assert ready["action"] == "REUSED"
    assert len(registrations) == (0 if existing_stop else 1)
    assert paths.identity_path.read_bytes() == identity_before
    assert not marker.exists()
    assert environment[onboarding.REQUIRED_ENV] == "1"
    assert len(autostarts) == 2
    assert ready["relay_autostart"]["status"] == "PASS"
    assert stored_run["value"] == run_before == ready["relay_autostart"]["command"]
    if existing_stop:
        assert helpers == ["Verify", "ObserveLifecycle"] * 2
    (tmp_path / "interrupted-onboarding-restart.json").write_text(json.dumps({
        "status": ready["status"], "registration_calls": len(registrations),
        "duplicate_registration_calls": 0, "stop_marker_present_after_ready": marker.exists(),
        "run_exact_readback": True, "native_host_registry_mutations": 0,
        "run_setter_getter_fixture": True, "helper_calls": len(helpers),
    }), encoding="utf-8")

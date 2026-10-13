"""Owned child crashes exercise real Windows abandonment without host services."""
import ast
import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import threading

import pytest

import auto_update as updates
import writer_session_fence as fence
from tests.test_supported_lifecycle import healthy

pytestmark = pytest.mark.skipif(os.name != "nt", reason="Windows named mutex")
NATIVE_LOCK = updates.update_lock


def kernel32():
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateMutexExW.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p,
                                     ctypes.c_ulong, ctypes.c_ulong]
    kernel.CreateMutexExW.restype = ctypes.c_void_p
    kernel.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_ulong]
    kernel.WaitForSingleObject.restype = ctypes.c_ulong
    kernel.ReleaseMutex.argtypes = [ctypes.c_void_p]
    kernel.CloseHandle.argtypes = [ctypes.c_void_p]
    return kernel


def crash_owner(mode, root, marker, sid=""):
    native = getattr(subprocess.Popen, "native", subprocess.Popen)
    child = native([sys.executable, "-B", str(Path(__file__).with_name("_abandoned_startup_child.py")),
                    mode, str(root), str(marker), sid],
                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                   creationflags=subprocess.CREATE_NO_WINDOW)
    child.communicate(timeout=15)
    assert child.returncode == 17, "owned crash child did not reach the transport/lock boundary"
    assert marker.read_text() == "owned"


def installation(tmp_path, monkeypatch):
    _fixed, selected, control, verified = healthy(tmp_path / "machine", monkeypatch)
    sid = updates.current_process_sid()
    policy = updates.read_json(control / "policy.json")
    stage = tmp_path / "stage"
    stage.mkdir()
    updates.write_json_durable(control / "policy.json", {**policy, "operator_sid": sid,
                                                       "staging_root": str(stage)})
    monkeypatch.setattr(updates, "ordinary_user_context", lambda *_: True)
    monkeypatch.setattr(updates, "update_lock", NATIVE_LOCK)
    monkeypatch.setattr(updates, "request_bootstrap", lambda *_a, **_k: pytest.fail("unexpected launch"))
    monkeypatch.setattr(updates, "_show_update_mutex_recovery_notice", lambda: None, raising=False)
    monkeypatch.setenv("KMTECH_LM_UPDATE_DEFERRED_AT", "")
    monkeypatch.setenv("KMTECH_LM_UPDATE_TRIAL_NONCE", "")
    identity = hashlib.sha256(os.path.normcase(str(control.absolute())).encode()).hexdigest()[:24]
    name = "Global\\KMTech.LabelMatch.Update." + identity
    return selected / "app", control, stage, sid, name, verified


def test_abandoned_update_mutex_verifies_consistent_installation(tmp_path, monkeypatch, capsys):
    app, control, stage, sid, name, verified = installation(tmp_path, monkeypatch)
    kernel = kernel32()
    survivor = kernel.CreateMutexExW(None, name, 0, 0x00100001)
    assert survivor
    native_verify = updates.fixed_helper

    def verify_with_owned_lock(*args, **kwargs):
        observed = []

        def competing_waiter():
            result = kernel.WaitForSingleObject(survivor, 0)
            observed.append(result)
            if result in {0, 0x80}:
                kernel.ReleaseMutex(survivor)

        waiter = threading.Thread(target=competing_waiter, daemon=True)
        waiter.start()
        waiter.join(2)
        assert observed == [0x102], "installation verification lost update ownership"
        return native_verify(*args, **kwargs)

    monkeypatch.setattr(updates, "fixed_helper", verify_with_owned_lock)
    before = {name: (control / name).read_bytes() for name in ("active.json", "transition.json")}
    try:
        crash_owner("update", control, tmp_path / "owned", sid)
        assert updates.startup_gate(app, []) is None
        # The abandoned owner still performs Verify while holding the mutex;
        # the subsequent unchanged install readback reuses the reviewed byte cache.
        assert [mode for mode, _root in verified] == ["Verify"]
        report = updates.read_json(stage / "startup-support.json")
        assert report["reason_code"] == "UPDATE_MUTEX_ABANDONED_VERIFIED"
        assert report["exit_code"] == 0
        assert all((control / name).read_bytes() == raw for name, raw in before.items())
        capsys.readouterr()
        assert updates.startup_gate(app, []) is None
        assert capsys.readouterr().out == ""
    finally:
        kernel.CloseHandle(survivor)


@pytest.mark.parametrize("damage", ["transition", "active", "installed_bytes", "verification_error", "record_error"])
def test_abandoned_update_mutex_inconsistency_has_notice_and_reason(tmp_path, monkeypatch, capsys, damage):
    app, control, stage, sid, name, _verified = installation(tmp_path, monkeypatch)
    if damage == "transition":
        state = updates.read_json(control / "transition.json")
        updates.write_json_durable(control / "transition.json", {**state, "phase": "APPLIED_PENDING_READY"})
    elif damage == "active":
        updates.write_json_durable(control / "active.json", {"schema": updates.SCHEMA, "sha": "invalid"})
    elif damage == "installed_bytes":
        (app.parent / "portable-manifest.json").write_text("{}")
    else:
        def unavailable(*_args, **_kwargs):
            raise OSError("synthetic unavailable verifier")
        monkeypatch.setattr(updates, "fixed_helper", unavailable)
        if damage == "record_error":
            monkeypatch.setattr(updates, "emit_support_record", unavailable)
    notices = []
    monkeypatch.setattr(updates, "_show_update_mutex_recovery_notice", lambda: notices.append(True), raising=False)
    kernel = kernel32()
    survivor = kernel.CreateMutexExW(None, name, 0, 0x00100001)
    assert survivor
    before = {name: (control / name).read_bytes() for name in ("active.json", "transition.json")}
    try:
        crash_owner("update", control, tmp_path / "owned", sid)
        assert updates.startup_gate(app, []) == 20
        assert notices == [True]
        output = capsys.readouterr()
        assert "설치 복구 필요" in output.err
        assert "UPDATE_MUTEX_ABANDONED_RECOVERY_REQUIRED" in output.err + output.out
        assert all((control / name).read_bytes() == raw for name, raw in before.items())
        if damage != "record_error":
            assert updates.read_json(stage / "startup-support.json")["reason_code"] == "UPDATE_MUTEX_ABANDONED_RECOVERY_REQUIRED"
    finally:
        kernel.CloseHandle(survivor)


def test_bootstrap_abandonment_also_refuses_with_recovery_notice(tmp_path, monkeypatch, capsys):
    app, control, stage, sid, name, _verified = installation(tmp_path, monkeypatch)
    state = updates.read_json(control / "transition.json")
    updates.write_json_durable(control / "transition.json", {**state, "recovery_pending": True})
    notices = []
    monkeypatch.setattr(updates, "_show_update_mutex_recovery_notice", lambda: notices.append(True), raising=False)
    kernel = kernel32()
    survivor = kernel.CreateMutexExW(None, name, 0, 0x00100001)
    assert survivor
    try:
        crash_owner("update", control, tmp_path / "owned", sid)
        assert updates.bootstrap(app, [], role="gui") == 20
        assert notices == [True]
        assert updates.read_json(stage / "startup-support.json")["reason_code"] == "UPDATE_MUTEX_ABANDONED_RECOVERY_REQUIRED"
        assert "설치 복구 필요" in capsys.readouterr().err
    finally:
        kernel.CloseHandle(survivor)


def test_phs_http_crash_notifies_first_start_only(tmp_path, monkeypatch, capsys):
    writer_root = tmp_path / "writer-control"
    monkeypatch.setenv(fence.TEST_MODE_ENV, "1")
    monkeypatch.setenv(fence.CONTROL_ROOT_OVERRIDE_ENV, str(writer_root))
    monkeypatch.setattr(updates, "assert_version_writer", lambda *_a, **_k: None)
    kernel = kernel32()
    survivor = kernel.CreateMutexExW(None, fence.writer_admission_mutex_name(writer_root), 0, 0x00100001)
    assert survivor
    source = Path(updates.__file__).with_name("Label_Match.py")
    tree = ast.parse(source.read_text(encoding="utf-8"))
    selected = [node for node in tree.body if isinstance(node, ast.FunctionDef)
                and node.name in {"main", "_show_installation_recovery_required", "_onboarding_refusal_codes"}]
    assert len(selected) == 3
    notices, traces, started = [], [], []

    def onboard(*_args, **_kwargs):
        with fence.writer_admission("current_user_onboarding"):
            pass
        return {"status": "READY", "bootstrap_integrity": {"status": "PRESENT"}}

    namespace = dict(Path=Path, os=os, sys=sys, re=re, __file__=str(source),
        WriterFenceError=fence.WriterFenceError, CurrentUserOnboardingError=Exception,
        ONBOARDING_EXIT_CODE=20, INSTALLATION_RECOVERY_REQUIRED_MESSAGE="설치 복구 필요",
        dispatch_product_mode=lambda _args: None, verify_factory_contract_startup=lambda: None,
        validate_pending_worker_install=lambda _root: None,
        _first_run_server_base_url=lambda _pending: None,
        _startup_app_stop_requested=lambda: False, check_user_data_open=lambda _root: None,
        _label_match_startup_trace=lambda event, **kw: traces.append((event, kw)),
        _first_run_onboarding_enabled=lambda: True, _label_match_runtime_app_root=lambda: tmp_path,
        onboard_current_user=onboard, requires_bootstrap_integrity=lambda _root: False,
        _show_first_run_onboarding_error=notices.append,
        _default_label_match_settings_path=lambda: tmp_path / "settings.json",
        resolve_data_scope=lambda **kw: str(tmp_path), resource_path=lambda value: value,
        run_guarded_entrypoint=lambda function, **kw: function(),
        _run_label_match_application=lambda: started.append(True) or 0)
    from current_user_onboarding import CurrentUserOnboardingError
    from label_data_generation import DataGenerationError
    namespace.update(CurrentUserOnboardingError=CurrentUserOnboardingError,
                     DataGenerationError=DataGenerationError)
    monkeypatch.setattr(updates, "startup_gate", lambda *_a, **_k: None)
    constants = [node for node in tree.body if isinstance(node, ast.Assign)
                 and any(isinstance(target, ast.Name) and target.id in {
                     "LABEL_MATCH_NEW_SCREEN_ARGUMENT", "LABEL_MATCH_SCREEN_HOST"}
                     for target in node.targets)]
    exec(compile(ast.Module(body=constants, type_ignores=[]), str(source), "exec"), namespace)
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(source), "exec"), namespace)
    try:
        crash_owner("phs", writer_root, tmp_path / "http-owned")
        assert namespace["main"]([]) == 20
        assert len(notices) == 1 and not started
        assert any(kw.get("guard_code") == "WRITER_GATE_ABANDONED" for _event, kw in traces)
        assert "설치 복구 필요" in capsys.readouterr().err
        assert namespace["main"]([]) == 0
        assert len(notices) == 1 and started == [True]
    finally:
        kernel.CloseHandle(survivor)

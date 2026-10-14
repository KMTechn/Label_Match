import json
import os
from pathlib import Path

import pytest
import ntpath
from label_match_instance_win32 import activate_pid, process_matches

import item_catalog_sync as catalog_sync
from label_match_single_instance import (
    ActivationResult,
    ERROR_ALREADY_EXISTS,
    MutexLease,
    SingleInstanceError,
    acquire_data_scope_mutex,
    activate_existing_label_window,
    mutex_name_for_data_scope,
    resolve_data_scope,
    run_guarded_entrypoint,
)
from tests.label_screen_entry_double import fresh_screen_launch


class _FakeKernel32:
    def __init__(self, handle):
        self.handle = handle
        self.closed = []

    def CreateMutexExW(self, _security, _name, _flags, _access):
        return self.handle

    def CloseHandle(self, handle):
        self.closed.append(int(handle))
        return True


class _FakeActivationKernel32:
    def __init__(self, image_path):
        self.image_path = image_path
        self.closed = []

    def OpenProcess(self, _access, _inherit, process_id):
        return 90 + int(process_id)

    def GetExitCodeProcess(self, _handle, code):
        code._obj.value = 259
        return True

    def QueryFullProcessImageNameW(self, _handle, _flags, buffer, _size):
        buffer.value = self.image_path
        return True

    def CloseHandle(self, handle):
        self.closed.append(int(handle))
        return True


class _FakeActivationUser32:
    def __init__(self, *, title, process_id=4242, foreground=True):
        self.title = title
        self.process_id = process_id
        self.foreground = foreground
        self.restored = []
        self.flashes = 0

    def EnumWindows(self, callback, _lparam):
        callback(701, 0)
        return True

    def IsWindowVisible(self, _hwnd):
        return True

    def GetWindowTextLengthW(self, _hwnd):
        return len(self.title)

    def GetWindowTextW(self, _hwnd, buffer, _length):
        buffer.value = self.title
        return len(self.title)

    def GetWindowThreadProcessId(self, _hwnd, process_id_pointer):
        process_id_pointer._obj.value = self.process_id
        return 1

    def IsIconic(self, _hwnd):
        return True

    def ShowWindow(self, hwnd, command):
        self.restored.append((int(hwnd), int(command)))
        return True

    def SetForegroundWindow(self, _hwnd):
        return self.foreground

    def FlashWindowEx(self, _info):
        self.flashes += 1
        return True


def test_mutex_name_is_stable_for_the_same_windows_data_scope():
    left = mutex_name_for_data_scope(r"C:\ProgramData\KMTech\Label_Match\data")
    right = mutex_name_for_data_scope("c:/programdata/kmtech/label_match/data/")

    assert left == right
    assert left.startswith(r"Global\KMTech.LabelMatch.")


def test_data_scope_resolution_preserves_app_custom_root_precedence(tmp_path):
    settings = tmp_path / "app_settings.json"
    settings.write_text(
        json.dumps({"custom_save_path": r"D:\configured"}),
        encoding="utf-8",
    )

    assert resolve_data_scope(
        environment={
            "LABEL_MATCH_SAVE_DIR": r"E:\explicit",
            "ProgramData": r"C:\ProgramData",
        },
        settings_path=settings,
    ) == r"D:\configured"


@pytest.mark.parametrize(
    "settings_payload",
    [{}, {"custom_save_path": ""}, {"custom_save_path": "   "}, {"custom_save_path": None}],
    ids=["missing", "empty", "whitespace", "null"],
)
@pytest.mark.parametrize("with_override", [False, True], ids=["default", "env"])
def test_data_scope_fallback_matches_app_writer(
    monkeypatch, tmp_path, settings_payload, with_override
):
    import Label_Match as app_module

    settings = tmp_path / "app_settings.json"
    settings.write_text(json.dumps(settings_payload), encoding="utf-8")
    monkeypatch.setenv("ProgramData", str(tmp_path / "program-data"))
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "local-app-data"))
    monkeypatch.setenv("LABEL_MATCH_DIRECT_SYNC_ROOT", str(tmp_path / "direct-sync"))
    if with_override:
        expected = tmp_path / "env-data"
        monkeypatch.setenv("LABEL_MATCH_SAVE_DIR", str(expected))
    else:
        expected = tmp_path / "local-app-data" / "KMTech" / "Label_Match" / "data"
        monkeypatch.delenv("LABEL_MATCH_SAVE_DIR", raising=False)

    app = object.__new__(app_module.Label_Match)
    app.app_settings = settings_payload
    writer_root = app_module.Label_Match._resolve_configured_save_path(app)
    guard_root = resolve_data_scope(settings_path=settings)

    assert writer_root == str(expected)
    assert mutex_name_for_data_scope(guard_root) == mutex_name_for_data_scope(writer_root)

    assert not (tmp_path / "program-data").exists()


@pytest.mark.skipif(os.name != "nt", reason="real Windows named mutex contract")
def test_conflicting_environment_roots_exclude_duplicate_writer(monkeypatch, tmp_path):
    import Label_Match as app_module

    shared_root = str(tmp_path / "shared-store")
    settings = tmp_path / "app_settings.json"
    settings_payload = {"custom_save_path": shared_root}
    settings.write_text(json.dumps(settings_payload), encoding="utf-8")
    app = object.__new__(app_module.Label_Match)
    app.app_settings = settings_payload
    scopes = []
    writer_roots = []
    for launch_root in (tmp_path / "launch-a", tmp_path / "launch-b"):
        monkeypatch.setenv("LABEL_MATCH_SAVE_DIR", str(launch_root))
        scopes.append(resolve_data_scope(settings_path=settings))
        writer_roots.append(app_module.Label_Match._resolve_configured_save_path(app))

    events = []
    mutex_names = []

    def acquire(scope):
        lease = acquire_data_scope_mutex(scope)
        mutex_names.append(lease.name)
        return lease

    def start_first():
        events.append("first-writer")
        return run_guarded_entrypoint(
            lambda: events.append("duplicate-writer"),
            data_scope=scopes[1],
            acquire=acquire,
            activate=lambda: events.append("activated")
            or ActivationResult(found=True, foreground=True),
        )

    assert run_guarded_entrypoint(
        start_first,
        data_scope=scopes[0],
        acquire=acquire,
        activate=lambda: pytest.fail("the first writer should own the temporary store"),
    ) == 0
    # The original env-first guard would run both callbacks despite one writer root.
    assert events == ["first-writer", "activated"]
    assert scopes == writer_roots == [shared_root, shared_root]
    assert mutex_names == [mutex_name_for_data_scope(shared_root)] * 2

    assert run_guarded_entrypoint(
        lambda: events.append("reopened-writer"),
        data_scope=scopes[1],
        acquire=acquire,
        activate=lambda: pytest.fail("the temporary store should be released"),
    ) == 0
    assert events == ["first-writer", "activated", "reopened-writer"]


def test_data_scope_resolution_uses_config_then_existing_program_data(tmp_path):
    settings = tmp_path / "app_settings.json"
    settings.write_text(
        json.dumps({"custom_save_path": r"D:\configured"}),
        encoding="utf-8",
    )
    assert resolve_data_scope(
        environment={"ProgramData": r"C:\ProgramData"},
        settings_path=settings,
    ) == r"D:\configured"

    settings.write_text("{broken", encoding="utf-8")
    program_data = tmp_path / "program-data"
    local_app_data = tmp_path / "local-app-data"
    with pytest.raises(SingleInstanceError):
        resolve_data_scope(environment={"ProgramData": str(program_data)}, settings_path=settings)
    assert not program_data.exists()
    assert resolve_data_scope(
        environment={"ProgramData": str(program_data), "LOCALAPPDATA": str(local_app_data)},
        settings_path=settings,
    ) == str(local_app_data / "KMTech" / "Label_Match" / "data")
    assert not program_data.exists()
    # A machine installer may have provisioned this leaf; the user reuses it.
    prepared = program_data / "KMTech" / "Label_Match" / "data"
    prepared.mkdir(parents=True)
    assert resolve_data_scope(
        environment={"ProgramData": str(program_data)}, settings_path=settings,
    ) == str(prepared)


def test_legacy_root_read_failure_is_not_treated_as_absent(monkeypatch, tmp_path):
    import label_match_single_instance as guard

    program = tmp_path / "program-data"
    legacy = program / "KMTech" / "Label_Match" / "data"
    original = os.scandir

    def denied(path):
        if Path(path) == legacy:
            raise PermissionError("synthetic unreadable legacy root")
        return original(path)

    monkeypatch.setattr(guard.os, "scandir", denied)
    with pytest.raises(SingleInstanceError, match="Unable to inspect legacy"):
        resolve_data_scope(
            environment={"LOCALAPPDATA": str(tmp_path / "local"), "ProgramData": str(program)},
        )
    # Onboarding never reads ProgramData, even before registration state exists.
    assert resolve_data_scope(
        environment={"LOCALAPPDATA": str(tmp_path / "local"), "ProgramData": str(program)},
        for_onboarding=True,
    ) == str(tmp_path / "local/KMTech/Label_Match/data")


@pytest.mark.skipif(os.name != "nt", reason="real Windows two-process mutex contract")
def test_two_processes_share_custom_root_and_release_writer_ownership(tmp_path):
    import subprocess
    import sys

    settings = tmp_path / "settings.json"
    custom = tmp_path / "custom-c"
    settings.write_text(json.dumps({"custom_save_path": str(custom)}), encoding="utf-8")
    parent_environment = {
        "LABEL_MATCH_SAVE_DIR": str(tmp_path / "env-a"),
        "LOCALAPPDATA": str(tmp_path / "local"),
    }
    scope = resolve_data_scope(environment=parent_environment, settings_path=settings)
    child_environment = dict(os.environ, LABEL_MATCH_SAVE_DIR=str(tmp_path / "env-b"))
    child_environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    child = """
import json
from pathlib import Path
import sys
from label_match_single_instance import ActivationResult, resolve_data_scope, run_guarded_entrypoint
scope = resolve_data_scope(settings_path=sys.argv[1])
events = []
def writer():
    events.append('writer')
    Path(sys.argv[2]).write_text('owned', encoding='utf-8')
    return 0
result = run_guarded_entrypoint(writer, data_scope=scope,
    activate=lambda: events.append('duplicate') or ActivationResult(found=True, foreground=True))
print(json.dumps({'scope': scope, 'events': events, 'result': result}))
"""
    marker = tmp_path / "writer.txt"

    def run_child():
        completed = subprocess.run(
            [sys.executable, "-c", child, str(settings), str(marker)],
            env=child_environment, cwd=tmp_path, capture_output=True, text=True,
            timeout=15, creationflags=subprocess.CREATE_NO_WINDOW,
        )
        assert completed.returncode == 0, completed.stderr
        return json.loads(completed.stdout.splitlines()[-1])

    with acquire_data_scope_mutex(scope) as lease:
        assert lease.owner
        assert run_child() == {"scope": str(custom), "events": ["duplicate"], "result": 0}
        assert not marker.exists()
    assert run_child() == {"scope": str(custom), "events": ["writer"], "result": 0}
    assert marker.read_text(encoding="utf-8") == "owned"


def test_first_mutex_owner_holds_handle_until_close():
    kernel = _FakeKernel32(handle=41)
    lease = acquire_data_scope_mutex(
        r"C:\data",
        kernel32=kernel,
        last_error_getter=lambda: 0,
    )

    assert lease.owner is True
    assert kernel.closed == []
    lease.close()
    lease.close()
    assert kernel.closed == [41]


def test_existing_mutex_closes_duplicate_handle_and_rejects_ownership():
    kernel = _FakeKernel32(handle=52)
    lease = acquire_data_scope_mutex(
        r"C:\data",
        kernel32=kernel,
        last_error_getter=lambda: ERROR_ALREADY_EXISTS,
    )

    assert lease.owner is False
    assert lease.handle is None
    assert kernel.closed == [52]


@pytest.mark.skipif(os.name != "nt", reason="real Windows named mutex contract")
def test_real_windows_mutex_blocks_second_owner_and_recovers_after_close(tmp_path):
    scope = str(tmp_path / "durable-data")
    first = acquire_data_scope_mutex(scope)
    try:
        second = acquire_data_scope_mutex(scope)
        assert first.owner is True
        assert second.owner is False
    finally:
        first.close()

    third = acquire_data_scope_mutex(scope)
    try:
        assert third.owner is True
    finally:
        third.close()


def test_mutex_api_failure_is_fail_closed():
    kernel = _FakeKernel32(handle=0)

    with pytest.raises(SingleInstanceError, match="acquisition failed"):
        acquire_data_scope_mutex(
            r"C:\data",
            kernel32=kernel,
            last_error_getter=lambda: 5,
        )


def test_duplicate_launch_activates_existing_window_without_starting_app():
    events = []

    def acquire(_scope):
        return MutexLease(owner=False, name="duplicate")

    result = run_guarded_entrypoint(
        lambda: events.append("started"),
        data_scope=r"C:\data",
        acquire=acquire,
        activate=lambda: events.append("activated")
        or ActivationResult(found=True, foreground=True),
    )

    assert result == 0
    assert events == ["activated"]


def test_owner_releases_mutex_after_success_and_constructor_failure():
    events = []

    def acquire(_scope):
        return MutexLease(
            owner=True,
            name="owner",
            handle=77,
            _close_handle=lambda handle: events.append(("closed", handle)),
        )

    assert run_guarded_entrypoint(
        lambda: events.append("started") or 0,
        data_scope=r"C:\data",
        acquire=acquire,
    ) == 0
    assert events == ["started", ("closed", 77)]

    events.clear()

    def fail():
        events.append("started")
        raise RuntimeError("constructor failed")

    with pytest.raises(RuntimeError, match="constructor failed"):
        run_guarded_entrypoint(fail, data_scope=r"C:\data", acquire=acquire)
    assert events == ["started", ("closed", 77)]


def test_owner_validation_requires_live_pid_and_same_absolute_executable():
    kernel = _FakeActivationKernel32("E:/KMTech/test/Label_Match.exe")
    assert process_matches(111, kernel.image_path, kernel32=kernel)
    assert not process_matches(111, "E:/elsewhere/Label_Match.exe", kernel32=kernel)
    assert not process_matches(111, "E:/KMTech/test/Other.exe", kernel32=kernel)
    kernel.GetExitCodeProcess = lambda _handle, code: False
    assert not process_matches(111, kernel.image_path, kernel32=kernel)


@pytest.mark.skipif(os.name != "nt", reason="Win32 window enumeration contract")
def test_duplicate_activation_restores_the_validated_owner_pid_window():
    user = _FakeActivationUser32(
        title="바코드 세트 검증기 (v2.0.54)",
        foreground=True,
    )
    kernel = _FakeActivationKernel32(r"D:\old-install\Label_Match.exe")

    result = (activate_pid(user.process_id, user32=user) if process_matches(user.process_id, ntpath.join(ntpath.dirname(kernel.image_path), "Label_Match.exe"), kernel32=kernel) else ActivationResult(False))

    assert result == ActivationResult(found=True, foreground=True, flashed=False)
    assert user.restored == [(701, 9)]
    assert kernel.closed == [4332]


@pytest.mark.skipif(os.name != "nt", reason="Win32 window enumeration contract")
def test_duplicate_activation_rejects_another_executable():
    user = _FakeActivationUser32(title="바코드 세트 검증기 (v2.0.54)")
    kernel = _FakeActivationKernel32(r"D:\untrusted\python.exe")

    result = (activate_pid(user.process_id, user32=user) if process_matches(user.process_id, ntpath.join(ntpath.dirname(kernel.image_path), "Label_Match.exe"), kernel32=kernel) else ActivationResult(False))

    assert result == ActivationResult(found=False)
    assert user.restored == []


@pytest.mark.skipif(os.name != "nt", reason="Win32 window enumeration contract")
def test_duplicate_activation_flashes_when_windows_denies_foreground():
    user = _FakeActivationUser32(
        title="바코드 세트 검증기 (v2.0.54)",
        foreground=False,
    )
    kernel = _FakeActivationKernel32(r"D:\old-install\Label_Match.exe")

    result = (activate_pid(user.process_id, user32=user) if process_matches(user.process_id, ntpath.join(ntpath.dirname(kernel.image_path), "Label_Match.exe"), kernel32=kernel) else ActivationResult(False))

    assert result == ActivationResult(found=True, foreground=False, flashed=True)
    assert user.flashes == 1


def test_entrypoint_acquires_guard_before_catalog_or_tk_construction():
    import ast
    source = Path(__file__).parents[1].joinpath("Label_Match.py").read_text(encoding="utf-8")
    main = next(node for node in ast.parse(source).body
                if isinstance(node, ast.FunctionDef) and node.name == "main")
    calls = [node for node in ast.walk(main) if isinstance(node, ast.Call)]
    guarded = [node for node in calls if isinstance(node.func, ast.Name)
               and node.func.id == "run_guarded_entrypoint"]
    assert len(guarded) == 1
    callback = guarded[0].args[0]
    assert any(isinstance(node, ast.Name) and node.id == "_run_label_match_application"
               for node in ast.walk(callback))
    for node in calls:
        if isinstance(node.func, ast.Name) and node.func.id == "_run_label_match_application":
            assert any(node is child for child in ast.walk(callback))
    assert not any(isinstance(node.func, ast.Name) and node.func.id in
                   {"Label_Match", "prepare_startup_item_catalog"} for node in calls)


def test_main_warns_and_continues_when_bootstrap_integrity_record_is_absent(
    monkeypatch,
):
    import Label_Match as app_module

    warnings = []
    traces = []
    monkeypatch.setattr(app_module, "verify_factory_contract_startup", lambda: None)
    monkeypatch.setattr(app_module, "_first_run_onboarding_enabled", lambda: True)
    monkeypatch.setattr(app_module, "_label_match_runtime_app_root", lambda: Path.cwd())
    monkeypatch.setattr(
        app_module,
        "onboard_current_user",
        lambda *_args, **_kwargs: {
            "status": "READY",
            "bootstrap_integrity": {"status": "ABSENT", "warning": True},
        },
    )
    monkeypatch.setattr(
        app_module.messagebox,
        "showwarning",
        lambda title, message: warnings.append((title, message)),
    )
    monkeypatch.setattr(
        app_module,
        "_label_match_startup_trace",
        lambda stage, **details: traces.append((stage, details)),
    )
    monkeypatch.setattr(app_module, "resolve_data_scope", lambda **_kwargs: r"C:\data")
    monkeypatch.setattr(
        app_module,
        "run_guarded_entrypoint",
        lambda _start, *, data_scope: 0,
    )

    assert app_module.main([]) == 0
    assert warnings == [
        (
            app_module.BOOTSTRAP_INTEGRITY_ABSENT_WARNING_TITLE,
            app_module.BOOTSTRAP_INTEGRITY_ABSENT_WARNING_MESSAGE,
        )
    ]
    assert ("bootstrap_integrity_absent", {}) in traces


def test_main_reports_catalog_gate_without_sensitive_details(monkeypatch):
    import Label_Match as app_module
    import auto_update
    from types import SimpleNamespace
    from unittest.mock import Mock

    # Retry now owns a hidden parent so its stop poll can dismiss the modal.
    # Keep native Tk blocked while retaining every existing dialog assertion.
    parent = SimpleNamespace(withdraw=Mock(), destroy=Mock(), after=Mock())
    parents = iter((parent, SimpleNamespace(withdraw=Mock(), destroy=Mock(), after=Mock())))
    monkeypatch.setattr(app_module.tk, "Tk", lambda: next(parents))
    # This case tests catalog refusal after successful onboarding, rather than
    # depending on another test's first-run state in the isolated profile.
    monkeypatch.setattr(app_module, "_first_run_onboarding_enabled", lambda: True)
    monkeypatch.setattr(app_module, "onboard_current_user", lambda *a, **k: {
        "status": "READY", "bootstrap_integrity": {"status": "PRESENT"},
    })
    monkeypatch.setattr(app_module, "validate_pending_worker_install", lambda _: None)
    monkeypatch.setattr(app_module, "check_user_data_open", lambda _: None)
    monkeypatch.setattr(auto_update, "startup_gate", lambda *a, **k: None)

    sensitive_marker = "profile-token-must-not-leak"
    dialogs = []
    traces = []

    monkeypatch.setattr(app_module, "resolve_data_scope", lambda **_kwargs: r"C:\data")
    # A failed catalog check reads the transition switch; pin it off, never the host value.
    monkeypatch.setattr(app_module, "legacy_label_transition_enabled", lambda: False)
    monkeypatch.setattr(
        app_module,
        "run_guarded_entrypoint",
        lambda start, *, data_scope: start(),
    )
    monkeypatch.setattr(
        app_module,
        "prepare_startup_item_catalog",
        lambda: (_ for _ in ()).throw(
            app_module.ItemCatalogSyncError(
                sensitive_marker,
                cause_code=catalog_sync.REQUEST_FAILED_NO_CACHE,
                diagnostic_context={
                    "request_sent": True,
                    "http_status_code": 503,
                    "http_reason_phrase": "Service Unavailable",
                    "selected_profile_path": (
                        r"C:\ProgramData\KMTech\Logistics\profiles"
                        r"\Label_Match\runtime-profile.json"
                    ),
                    "selected_authority_scope": "TEST1-STALE-SCOPE",
                    "selected_base_url": "https://stale.example.invalid",
                    "profile_selection_warning": (
                        catalog_sync.PROFILE_WARNING_CURRENT_USER_MISMATCH
                    ),
                    "unexpected_secret": sensitive_marker,
                },
            )
        ),
    )
    diagnostic_calls = []
    monkeypatch.setattr(
        app_module,
        "write_item_catalog_failure_diagnostic",
        lambda path, error: diagnostic_calls.append((path, error.cause_code)),
    )

    class NoTkLabelMatch:
        FILES = app_module.Label_Match.FILES

        def __init__(self):
            raise AssertionError("catalog failure must stop before Tk construction")

    monkeypatch.setattr(app_module, "Label_Match", NoTkLabelMatch)
    monkeypatch.setattr(
        app_module.messagebox,
        "askretrycancel",
        lambda title, message, **options: (dialogs.append((title, message)), False)[1],
    )
    monkeypatch.setattr(
        app_module,
        "_label_match_startup_trace",
        lambda stage, **details: traces.append((stage, details)),
    )

    result = app_module.main([])

    assert result == app_module.ITEM_CATALOG_STARTUP_EXIT_CODE == 3
    assert len(dialogs) == 1
    parent.after.assert_called_once()
    parent.destroy.assert_called_once()
    assert "회사 서버 품목 목록" in dialogs[0][0]
    assert "IT 담당자" in dialogs[0][1]
    assert r"C:\ProgramData\KMTech\Logistics" in dialogs[0][1]
    assert "TEST1-STALE-SCOPE" in dialogs[0][1]
    assert "https://stale.example.invalid" in dialogs[0][1]
    assert "HTTP 503 Service Unavailable" in dialogs[0][1]
    assert catalog_sync.PROFILE_WARNING_CURRENT_USER_MISMATCH in dialogs[0][1]
    assert sensitive_marker not in dialogs[0][0]
    assert sensitive_marker not in dialogs[0][1]
    assert sensitive_marker not in repr(traces)
    assert diagnostic_calls[0][1] == catalog_sync.REQUEST_FAILED_NO_CACHE
    assert (
        "item_catalog_startup_blocked",
        {
            "exit_code": 3,
            "cause_code": catalog_sync.REQUEST_FAILED_NO_CACHE,
            "http_status_code": 503,
            "selected_profile_path": (
                r"C:\ProgramData\KMTech\Logistics\profiles"
                r"\Label_Match\runtime-profile.json"
            ),
            "selected_authority_scope": "TEST1-STALE-SCOPE",
            "selected_base_url": "https://stale.example.invalid",
            "profile_selection_warning": (
                catalog_sync.PROFILE_WARNING_CURRENT_USER_MISMATCH
            ),
        },
    ) in traces


@pytest.mark.parametrize("ui_arguments", [(), ("--ui", "tk"), ("--old-screen",)])
def test_main_warns_and_continues_with_verified_catalog_cache(
    monkeypatch, tmp_path,
    ui_arguments,
):
    import Label_Match as app_module

    monkeypatch.setattr(app_module, "_STARTUP_WORKFLOW_NOTICES", [])
    calls = []
    dialogs = []
    cache_path = tmp_path / "Item.csv"
    diagnostic_path = tmp_path / "status" / "item_catalog_startup_diagnostic.json"
    monkeypatch.setattr(app_module, "resolve_data_scope", lambda **_kwargs: str(tmp_path))
    monkeypatch.setattr(
        app_module,
        "run_guarded_entrypoint",
        lambda start, *, data_scope: start(),
    )
    monkeypatch.setattr(
        app_module,
        "prepare_startup_item_catalog",
        lambda: calls.append("catalog") or str(cache_path),
    )
    monkeypatch.setattr(
        app_module,
        "get_sanitized_catalog_attempt_context",
        lambda: {
            "catalog_source": "VERIFIED_CACHE",
            "cache_used": True,
            "cache_last_modified_utc": "2026-08-28T00:21:44+00:00",
        },
    )
    monkeypatch.setattr(
        app_module,
        "_item_catalog_diagnostic_path",
        lambda: diagnostic_path,
    )
    monkeypatch.setattr(
        app_module,
        "write_item_catalog_startup_diagnostic",
        lambda path: calls.append(("diagnostic", Path(path))),
    )
    monkeypatch.setattr(
        app_module.messagebox,
        "showwarning",
        lambda title, message: dialogs.append((title, message)),
    )
    monkeypatch.setattr(
        app_module,
        "_label_match_startup_trace",
        lambda stage, **details: calls.append((stage, details)),
    )

    class FakeLabelMatch:
        FILES = app_module.Label_Match.FILES

        def __init__(self):
            calls.append("client-created")

        def title(self):
            return "Label Match"

        def state(self):
            return "normal"

        def mainloop(self):
            calls.append("mainloop")

        def _finish_after_mainloop(self):
            calls.append("loop-cleanup")

    monkeypatch.setattr(app_module, "Label_Match", FakeLabelMatch)

    from tests.label_screen_entry_double import install_screen_owner_double
    screen_routes = install_screen_owner_double(monkeypatch, app_module)
    assert app_module.main(ui_arguments) == 0
    assert calls[0] == ("main_enter", {})
    assert calls[1] == "catalog"
    assert calls[2] == (
        "item_catalog_profile_selected",
        {
            "selected_profile_path": None,
            "selected_authority_scope": None,
            "selected_base_url": None,
            "profile_selection_warning": None,
        },
    )
    assert calls[3] == ("diagnostic", diagnostic_path)
    assert "client-created" in calls
    assert "mainloop" in calls
    assert dialogs == []
    dialogs = [(notice.title, notice.message) for notice in app_module._STARTUP_WORKFLOW_NOTICES]
    assert len(dialogs) == 1
    assert dialogs[0][0] == app_module.ITEM_CATALOG_CACHE_WARNING_TITLE
    assert "2026-08-28" in dialogs[0][1]
    assert "UNKNOWN" not in dialogs[0][1]
    assert screen_routes == (["new"] if not ui_arguments else [])


@pytest.mark.parametrize("ui_arguments", [(), ("--ui", "tk"), ("--old-screen",)])
def test_main_warns_and_continues_when_selected_profile_conflicts_with_install(
    monkeypatch, tmp_path,
    ui_arguments,
):
    import Label_Match as app_module

    monkeypatch.setattr(app_module, "_STARTUP_WORKFLOW_NOTICES", [])
    calls = []
    warnings = []
    sensitive_marker = "alternate-profile-token-must-not-leak"
    selected_path = (
        r"C:\ProgramData\KMTech\Logistics\profiles"
        r"\Label_Match\runtime-profile.json"
    )
    context = {
        "catalog_source": "CENTRAL_REFRESH",
        "cache_used": False,
        "cache_last_modified_utc": "UNKNOWN",
        "http_status_code": 200,
        "http_reason_phrase": "OK",
        "selected_profile_path": selected_path,
        "selected_authority_scope": "TEST1-SELECTED-SCOPE",
        "selected_base_url": "https://selected.example.invalid:18456",
        "profile_selection_warning": (
            catalog_sync.PROFILE_WARNING_CURRENT_USER_MISMATCH
        ),
    }
    monkeypatch.setattr(app_module, "resolve_data_scope", lambda **_kwargs: str(tmp_path))
    monkeypatch.setattr(
        app_module,
        "run_guarded_entrypoint",
        lambda start, *, data_scope: start(),
    )
    monkeypatch.setattr(
        app_module,
        "prepare_startup_item_catalog",
        lambda: calls.append("catalog") or str(tmp_path / "Item.csv"),
    )
    monkeypatch.setattr(
        app_module,
        "get_sanitized_catalog_attempt_context",
        lambda: dict(context),
    )
    monkeypatch.setattr(
        app_module,
        "write_item_catalog_startup_diagnostic",
        lambda path: calls.append(("diagnostic", Path(path))),
    )
    monkeypatch.setattr(
        app_module.messagebox,
        "showwarning",
        lambda title, message: warnings.append((title, message)),
    )
    monkeypatch.setattr(
        app_module,
        "_label_match_startup_trace",
        lambda stage, **details: calls.append((stage, details)),
    )

    class FakeLabelMatch:
        FILES = app_module.Label_Match.FILES

        def __init__(self):
            calls.append("client-created")

        def title(self):
            return "Label Match"

        def state(self):
            return "normal"

        def mainloop(self):
            calls.append("mainloop")

        def _finish_after_mainloop(self):
            calls.append("loop-cleanup")

    monkeypatch.setattr(app_module, "Label_Match", FakeLabelMatch)

    from tests.label_screen_entry_double import install_screen_owner_double
    screen_routes = install_screen_owner_double(monkeypatch, app_module)
    assert app_module.main(ui_arguments) == 0
    assert warnings == []
    warnings = [(notice.title, notice.message) for notice in app_module._STARTUP_WORKFLOW_NOTICES]
    assert len(warnings) == 1
    assert warnings[0][0] == app_module.LOGISTICS_PROFILE_WARNING_TITLE
    assert warnings[0][1] == app_module.LOGISTICS_PROFILE_WARNING_MESSAGES[catalog_sync.PROFILE_WARNING_CURRENT_USER_MISMATCH]
    assert ("item_catalog_profile_selected", {
        "selected_profile_path": selected_path,
        "selected_authority_scope": "TEST1-SELECTED-SCOPE",
        "selected_base_url": "https://selected.example.invalid:18456",
        "profile_selection_warning": catalog_sync.PROFILE_WARNING_CURRENT_USER_MISMATCH,
    }) in calls
    assert sensitive_marker not in repr(warnings)
    assert "client-created" in calls
    assert "mainloop" in calls
    assert screen_routes == (["new"] if not ui_arguments else [])

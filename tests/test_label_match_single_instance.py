import json
import os
from pathlib import Path

import pytest

import item_catalog_sync as catalog_sync
from label_match_single_instance import (
    ActivationResult,
    ERROR_ALREADY_EXISTS,
    MutexLease,
    SingleInstanceError,
    _window_candidate_matches,
    acquire_data_scope_mutex,
    activate_existing_label_window,
    mutex_name_for_data_scope,
    resolve_data_scope,
    run_guarded_entrypoint,
)


class _FakeKernel32:
    def __init__(self, handle):
        self.handle = handle
        self.closed = []

    def CreateMutexW(self, _security, _initial_owner, _name):
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


def test_data_scope_resolution_prefers_explicit_environment(tmp_path):
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
    ) == r"E:\explicit"


def test_data_scope_resolution_uses_config_then_program_data(tmp_path):
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
    assert resolve_data_scope(
        environment={"ProgramData": r"C:\ProgramData"},
        settings_path=settings,
    ) == r"C:\ProgramData\KMTech\Label_Match\data"


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


def test_window_validation_requires_label_title_and_same_executable_name():
    assert _window_candidate_matches(
        title="바코드 세트 검증기 (v2.0.58)",
        process_path=r"D:\other-install\Label_Match.exe",
        expected_executable_name=r"C:\current\Label_Match.exe",
    )
    assert not _window_candidate_matches(
        title="바코드 세트 검증기 (v2.0.58)",
        process_path=r"D:\other-install\python.exe",
        expected_executable_name=r"C:\current\Label_Match.exe",
    )
    assert not _window_candidate_matches(
        title="업데이트 발견",
        process_path=r"D:\other-install\Label_Match.exe",
        expected_executable_name=r"C:\current\Label_Match.exe",
    )


@pytest.mark.skipif(os.name != "nt", reason="Win32 window enumeration contract")
def test_duplicate_activation_restores_only_a_validated_label_window():
    user = _FakeActivationUser32(
        title="바코드 세트 검증기 (v2.0.54)",
        foreground=True,
    )
    kernel = _FakeActivationKernel32(r"D:\old-install\Label_Match.exe")

    result = activate_existing_label_window(
        executable_name="Label_Match.exe",
        current_process_id=9999,
        user32=user,
        kernel32=kernel,
    )

    assert result == ActivationResult(found=True, foreground=True, flashed=False)
    assert user.restored == [(701, 9)]
    assert kernel.closed == [4332]


@pytest.mark.skipif(os.name != "nt", reason="Win32 window enumeration contract")
def test_duplicate_activation_rejects_same_title_from_another_executable():
    user = _FakeActivationUser32(title="바코드 세트 검증기 (v2.0.54)")
    kernel = _FakeActivationKernel32(r"D:\untrusted\python.exe")

    result = activate_existing_label_window(
        executable_name="Label_Match.exe",
        current_process_id=9999,
        user32=user,
        kernel32=kernel,
    )

    assert result == ActivationResult(found=False)
    assert user.restored == []


@pytest.mark.skipif(os.name != "nt", reason="Win32 window enumeration contract")
def test_duplicate_activation_flashes_when_windows_denies_foreground():
    user = _FakeActivationUser32(
        title="바코드 세트 검증기 (v2.0.54)",
        foreground=False,
    )
    kernel = _FakeActivationKernel32(r"D:\old-install\Label_Match.exe")

    result = activate_existing_label_window(
        executable_name="Label_Match.exe",
        current_process_id=9999,
        user32=user,
        kernel32=kernel,
    )

    assert result == ActivationResult(found=True, foreground=False, flashed=True)
    assert user.flashes == 1


def test_entrypoint_acquires_guard_before_catalog_or_tk_construction():
    source = Path(__file__).parents[1].joinpath("Label_Match.py").read_text(
        encoding="utf-8"
    )
    main_source = source[source.index("def main(argv=None):") :]

    assert "run_guarded_entrypoint(" in main_source
    assert "_run_label_match_application," in main_source
    assert "prepare_startup_item_catalog()" not in main_source.split(
        "if __name__ == \"__main__\":", 1
    )[0]


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

    sensitive_marker = "profile-token-must-not-leak"
    dialogs = []
    traces = []

    monkeypatch.setattr(app_module, "resolve_data_scope", lambda **_kwargs: r"C:\data")
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
        "showerror",
        lambda title, message: dialogs.append((title, message)),
    )
    monkeypatch.setattr(
        app_module,
        "_label_match_startup_trace",
        lambda stage, **details: traces.append((stage, details)),
    )

    result = app_module.main()

    assert result == app_module.ITEM_CATALOG_STARTUP_EXIT_CODE == 3
    assert len(dialogs) == 1
    assert "중앙 품목 목록" in dialogs[0][0]
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


def test_main_warns_and_continues_with_verified_catalog_cache(
    monkeypatch, tmp_path
):
    import Label_Match as app_module

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

    monkeypatch.setattr(app_module, "Label_Match", FakeLabelMatch)

    assert app_module.main([]) == 0
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
    assert len(dialogs) == 1
    assert dialogs[0][0] == app_module.ITEM_CATALOG_CACHE_WARNING_TITLE
    assert "2026-08-28" in dialogs[0][1]
    assert "UNKNOWN" not in dialogs[0][1]


def test_main_warns_and_continues_when_selected_profile_conflicts_with_install(
    monkeypatch, tmp_path
):
    import Label_Match as app_module

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

    monkeypatch.setattr(app_module, "Label_Match", FakeLabelMatch)

    assert app_module.main([]) == 0
    assert len(warnings) == 1
    assert warnings[0][0] == app_module.LOGISTICS_PROFILE_WARNING_TITLE
    assert selected_path in warnings[0][1]
    assert "TEST1-SELECTED-SCOPE" in warnings[0][1]
    assert "https://selected.example.invalid:18456" in warnings[0][1]
    assert catalog_sync.PROFILE_WARNING_CURRENT_USER_MISMATCH in warnings[0][1]
    assert sensitive_marker not in repr(warnings)
    assert "client-created" in calls
    assert "mainloop" in calls

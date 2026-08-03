import json
import os
from pathlib import Path

import pytest

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
    main_source = source[source.index("def main():") :]

    assert "run_guarded_entrypoint(" in main_source
    assert "_run_label_match_application," in main_source
    assert "prepare_startup_item_catalog()" not in main_source.split(
        "if __name__ == \"__main__\":", 1
    )[0]

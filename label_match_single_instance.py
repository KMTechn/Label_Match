"""Fail-closed single-instance ownership for the packaged Label Match app.

The guard is keyed by the durable data root rather than the executable path so
two installed copies cannot concurrently open the same recovery/outbox state.
Windows owns the mutex lifetime, which also makes updater restarts and crash
recovery safe from stale lock files.
"""

from __future__ import annotations

from dataclasses import dataclass
import ctypes
import hashlib
import json
import ntpath
import os
from pathlib import Path
from typing import Callable, Mapping


ERROR_ALREADY_EXISTS = 183
PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
SW_RESTORE = 9
FLASHW_ALL = 0x00000003
FLASHW_TIMERNOFG = 0x0000000C
LABEL_WINDOW_TITLE_PREFIX = "바코드 세트 검증기 ("


class SingleInstanceError(RuntimeError):
    """Raised when ownership cannot be established safely."""


def canonical_data_scope(path: str | os.PathLike[str]) -> str:
    text = str(path or "").strip()
    if not text:
        raise SingleInstanceError("Label Match data scope is empty")
    return ntpath.normcase(ntpath.normpath(text))


def mutex_name_for_data_scope(path: str | os.PathLike[str]) -> str:
    digest = hashlib.sha256(canonical_data_scope(path).encode("utf-8")).hexdigest()
    return rf"Global\KMTech.LabelMatch.{digest[:32]}"


def resolve_data_scope(
    *,
    environment: Mapping[str, str] | None = None,
    settings_path: str | os.PathLike[str] | None = None,
) -> str:
    """Resolve the same durable root used by the application without writing."""

    env = os.environ if environment is None else environment
    override = str(env.get("LABEL_MATCH_SAVE_DIR", "") or "").strip()
    if override:
        return override

    if settings_path:
        try:
            with open(settings_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            configured = str(
                payload.get("custom_save_path", "") if isinstance(payload, dict) else ""
            ).strip()
            if configured:
                return configured
        except (OSError, UnicodeError, json.JSONDecodeError):
            # Invalid settings are handled by the normal app startup.  The
            # guard still owns the default state before that code can run.
            pass

    program_data = str(env.get("ProgramData", r"C:\ProgramData") or "").strip()
    if not program_data:
        raise SingleInstanceError("ProgramData is empty")
    return ntpath.join(program_data, "KMTech", "Label_Match", "data")


@dataclass
class MutexLease:
    owner: bool
    name: str
    handle: int | None = None
    _close_handle: Callable[[int], object] | None = None

    def close(self) -> None:
        handle = self.handle
        self.handle = None
        if handle and self._close_handle is not None:
            self._close_handle(handle)

    def __enter__(self) -> "MutexLease":
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        self.close()


def acquire_data_scope_mutex(
    data_scope: str | os.PathLike[str],
    *,
    kernel32=None,
    last_error_getter: Callable[[], int] | None = None,
) -> MutexLease:
    name = mutex_name_for_data_scope(data_scope)
    if os.name != "nt" and kernel32 is None:
        # Label Match is packaged for Windows.  A no-op owner keeps source-mode
        # imports and non-Windows unit tests available without pretending that
        # a cross-platform process lock protects production state.
        return MutexLease(owner=True, name=name)

    from ctypes import wintypes

    api = kernel32 or ctypes.WinDLL("kernel32", use_last_error=True)
    create_mutex = api.CreateMutexW
    close_handle = api.CloseHandle
    if kernel32 is None:
        create_mutex.argtypes = [wintypes.LPVOID, wintypes.BOOL, wintypes.LPCWSTR]
        create_mutex.restype = wintypes.HANDLE
        close_handle.argtypes = [wintypes.HANDLE]
        close_handle.restype = wintypes.BOOL

    set_last_error = getattr(ctypes, "set_last_error", None)
    if set_last_error is not None:
        set_last_error(0)
    handle = create_mutex(None, False, name)
    error_reader = last_error_getter or getattr(ctypes, "get_last_error", lambda: 0)
    error = int(error_reader())
    if not handle:
        raise SingleInstanceError(
            f"Label Match single-instance mutex acquisition failed ({error})"
        )
    handle_value = int(handle)
    if error == ERROR_ALREADY_EXISTS:
        close_handle(handle)
        return MutexLease(owner=False, name=name)
    return MutexLease(
        owner=True,
        name=name,
        handle=handle_value,
        _close_handle=close_handle,
    )


def _window_candidate_matches(
    *,
    title: str,
    process_path: str,
    expected_executable_name: str,
) -> bool:
    return bool(
        title.startswith(LABEL_WINDOW_TITLE_PREFIX)
        and ntpath.basename(process_path).casefold()
        == ntpath.basename(expected_executable_name).casefold()
    )


@dataclass(frozen=True)
class ActivationResult:
    found: bool
    foreground: bool = False
    flashed: bool = False


def _query_process_path(kernel32, process_id: int) -> str:
    from ctypes import wintypes

    open_process = kernel32.OpenProcess
    query_image = kernel32.QueryFullProcessImageNameW
    close_handle = kernel32.CloseHandle
    handle = open_process(PROCESS_QUERY_LIMITED_INFORMATION, False, process_id)
    if not handle:
        return ""
    try:
        buffer = ctypes.create_unicode_buffer(32768)
        size = wintypes.DWORD(len(buffer))
        if not query_image(handle, 0, buffer, ctypes.byref(size)):
            return ""
        return buffer.value
    finally:
        close_handle(handle)


def activate_existing_label_window(
    *,
    executable_name: str | None = None,
    current_process_id: int | None = None,
    user32=None,
    kernel32=None,
) -> ActivationResult:
    """Restore/raise a validated existing Label window, then return."""

    if os.name != "nt" and (user32 is None or kernel32 is None):
        return ActivationResult(found=False)

    from ctypes import wintypes

    user = user32 or ctypes.WinDLL("user32", use_last_error=True)
    kernel = kernel32 or ctypes.WinDLL("kernel32", use_last_error=True)
    expected_name = executable_name or sys_executable_name()
    own_pid = int(current_process_id if current_process_id is not None else os.getpid())
    matches: list[tuple[int, int]] = []

    enum_proc_type = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)
    if user32 is None:
        user.EnumWindows.argtypes = [enum_proc_type, wintypes.LPARAM]
        user.EnumWindows.restype = wintypes.BOOL
        user.IsWindowVisible.argtypes = [wintypes.HWND]
        user.IsWindowVisible.restype = wintypes.BOOL
        user.GetWindowTextLengthW.argtypes = [wintypes.HWND]
        user.GetWindowTextLengthW.restype = ctypes.c_int
        user.GetWindowTextW.argtypes = [wintypes.HWND, wintypes.LPWSTR, ctypes.c_int]
        user.GetWindowTextW.restype = ctypes.c_int
        user.GetWindowThreadProcessId.argtypes = [
            wintypes.HWND,
            ctypes.POINTER(wintypes.DWORD),
        ]
        user.GetWindowThreadProcessId.restype = wintypes.DWORD
        user.IsIconic.argtypes = [wintypes.HWND]
        user.IsIconic.restype = wintypes.BOOL
        user.ShowWindow.argtypes = [wintypes.HWND, ctypes.c_int]
        user.ShowWindow.restype = wintypes.BOOL
        user.SetForegroundWindow.argtypes = [wintypes.HWND]
        user.SetForegroundWindow.restype = wintypes.BOOL
    if kernel32 is None:
        kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel.OpenProcess.restype = wintypes.HANDLE
        kernel.QueryFullProcessImageNameW.argtypes = [
            wintypes.HANDLE,
            wintypes.DWORD,
            wintypes.LPWSTR,
            ctypes.POINTER(wintypes.DWORD),
        ]
        kernel.QueryFullProcessImageNameW.restype = wintypes.BOOL
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel.CloseHandle.restype = wintypes.BOOL

    def visit(hwnd, _lparam):
        if not user.IsWindowVisible(hwnd):
            return True
        length = int(user.GetWindowTextLengthW(hwnd))
        if length <= 0:
            return True
        title_buffer = ctypes.create_unicode_buffer(length + 1)
        user.GetWindowTextW(hwnd, title_buffer, len(title_buffer))
        process_id = wintypes.DWORD()
        user.GetWindowThreadProcessId(hwnd, ctypes.byref(process_id))
        pid = int(process_id.value)
        if not pid or pid == own_pid:
            return True
        process_path = _query_process_path(kernel, pid)
        if _window_candidate_matches(
            title=title_buffer.value,
            process_path=process_path,
            expected_executable_name=expected_name,
        ):
            matches.append((int(hwnd), pid))
            return False
        return True

    callback = enum_proc_type(visit)
    user.EnumWindows(callback, 0)
    if not matches:
        return ActivationResult(found=False)

    hwnd, _pid = matches[0]
    if user.IsIconic(hwnd):
        user.ShowWindow(hwnd, SW_RESTORE)
    foreground = bool(user.SetForegroundWindow(hwnd))
    flashed = False
    if not foreground:
        class FLASHWINFO(ctypes.Structure):
            _fields_ = [
                ("cbSize", wintypes.UINT),
                ("hwnd", wintypes.HWND),
                ("dwFlags", wintypes.DWORD),
                ("uCount", wintypes.UINT),
                ("dwTimeout", wintypes.DWORD),
            ]

        info = FLASHWINFO(
            ctypes.sizeof(FLASHWINFO),
            hwnd,
            FLASHW_ALL | FLASHW_TIMERNOFG,
            3,
            0,
        )
        if user32 is None:
            user.FlashWindowEx.argtypes = [ctypes.POINTER(FLASHWINFO)]
            user.FlashWindowEx.restype = wintypes.BOOL
        flashed = bool(user.FlashWindowEx(ctypes.byref(info)))
    return ActivationResult(found=True, foreground=foreground, flashed=flashed)


def sys_executable_name() -> str:
    import sys

    return Path(sys.executable).name


def run_guarded_entrypoint(
    start: Callable[[], int | None],
    *,
    data_scope: str | os.PathLike[str],
    acquire: Callable[[str | os.PathLike[str]], MutexLease] = acquire_data_scope_mutex,
    activate: Callable[[], ActivationResult] = activate_existing_label_window,
) -> int:
    """Run ``start`` only for the data-scope owner."""

    lease = acquire(data_scope)
    if not lease.owner:
        result = activate()
        message = (
            "기존 Label Match 창을 표시했습니다."
            if result.found
            else "Label Match가 이미 실행 중입니다. 기존 창을 확인하세요."
        )
        print(message)
        return 0
    try:
        result = start()
        return int(result or 0)
    finally:
        lease.close()

"""Win32 boundary for GUI mutexes, PID ownership, windows and bounded notices."""
from dataclasses import dataclass
import ctypes
from ctypes import wintypes
import ntpath

APP_NAME = "포장"
OPENING_MESSAGE = "프로그램이 켜지는 중입니다 · 잠시 기다려 주세요"
CLOSING_MESSAGE = "프로그램을 닫는 중입니다 · 잠시 뒤 다시 누르세요"
NOTICE_TIMEOUT_MS = 5000


@dataclass(frozen=True)
class ActivationResult:
    found: bool
    foreground: bool = False
    flashed: bool = False


def mutex_functions(kernel32=None):
    kernel = kernel32 or ctypes.WinDLL("kernel32", use_last_error=True)
    if kernel32 is None:
        kernel.CreateMutexExW.argtypes = [wintypes.LPVOID, wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD]
        kernel.CreateMutexExW.restype = wintypes.HANDLE
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel.CloseHandle.restype = wintypes.BOOL
    return kernel.CreateMutexExW, kernel.CloseHandle


def create_mutex(name, *, kernel32=None, last_error_getter=None):
    create, close = mutex_functions(kernel32)
    ctypes.set_last_error(0)
    handle = create(None, name, 0, 0x00100000)
    error = int((last_error_getter or ctypes.get_last_error)())
    return handle, error, close


def process_matches(pid, executable, *, kernel32=None):
    kernel = kernel32 or ctypes.WinDLL("kernel32", use_last_error=True)
    if kernel32 is None:
        kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel.OpenProcess.restype = wintypes.HANDLE
        kernel.QueryFullProcessImageNameW.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.LPWSTR, ctypes.POINTER(wintypes.DWORD)]
        kernel.QueryFullProcessImageNameW.restype = wintypes.BOOL
        kernel.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
        kernel.GetExitCodeProcess.restype = wintypes.BOOL
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel.CloseHandle.restype = wintypes.BOOL
    handle = kernel.OpenProcess(0x1000, False, pid)
    if not handle:
        return False
    try:
        code = wintypes.DWORD()
        if not kernel.GetExitCodeProcess(handle, ctypes.byref(code)) or code.value != 259:
            return False
        buffer = ctypes.create_unicode_buffer(32768)
        size = wintypes.DWORD(len(buffer))
        if not kernel.QueryFullProcessImageNameW(handle, 0, buffer, ctypes.byref(size)):
            return False
        return ntpath.normcase(ntpath.normpath(buffer.value)) == ntpath.normcase(ntpath.normpath(executable))
    finally:
        kernel.CloseHandle(handle)


def activate_pid(pid, *, user32=None):
    user = user32 or ctypes.WinDLL("user32", use_last_error=True)
    callback_type = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)
    if user32 is None:
        user.EnumWindows.argtypes = [callback_type, wintypes.LPARAM]
        user.EnumWindows.restype = wintypes.BOOL
        user.IsWindowVisible.argtypes = [wintypes.HWND]
        user.IsWindowVisible.restype = wintypes.BOOL
        user.GetWindowThreadProcessId.argtypes = [wintypes.HWND, ctypes.POINTER(wintypes.DWORD)]
        user.GetWindowThreadProcessId.restype = wintypes.DWORD
        user.IsIconic.argtypes = [wintypes.HWND]
        user.IsIconic.restype = wintypes.BOOL
        user.ShowWindow.argtypes = [wintypes.HWND, ctypes.c_int]
        user.ShowWindow.restype = wintypes.BOOL
        user.SetForegroundWindow.argtypes = [wintypes.HWND]
        user.SetForegroundWindow.restype = wintypes.BOOL
    windows = []
    def visit(hwnd, _parameter):
        if user.IsWindowVisible(hwnd):
            owner = wintypes.DWORD()
            user.GetWindowThreadProcessId(hwnd, ctypes.byref(owner))
            if owner.value == pid:
                windows.append(hwnd)
                return False
        return True
    user.EnumWindows(callback_type(visit), 0)
    if not windows:
        return ActivationResult(False)
    hwnd = windows[0]
    if user.IsIconic(hwnd):
        user.ShowWindow(hwnd, 9)
    foreground = bool(user.SetForegroundWindow(hwnd))
    flashed = False
    if not foreground:
        class FlashInfo(ctypes.Structure):
            _fields_ = [('cbSize', wintypes.UINT), ('hwnd', wintypes.HWND), ('dwFlags', wintypes.DWORD), ('uCount', wintypes.UINT), ('dwTimeout', wintypes.DWORD)]
        info = FlashInfo(ctypes.sizeof(FlashInfo), hwnd, 3 | 12, 3, 0)
        if user32 is None:
            user.FlashWindowEx.argtypes = [ctypes.POINTER(FlashInfo)]
            user.FlashWindowEx.restype = wintypes.BOOL
        flashed = bool(user.FlashWindowEx(ctypes.byref(info)))
    return ActivationResult(True, foreground, flashed)


def timed_notice(*, closing=False, user32=None):
    user = user32 or ctypes.WinDLL("user32", use_last_error=True)
    if user32 is None:
        user.MessageBoxTimeoutW.argtypes = [wintypes.HWND, wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.UINT, wintypes.WORD, wintypes.DWORD]
        user.MessageBoxTimeoutW.restype = ctypes.c_int
    # MB_OK (one default-focused button), foreground; no modal wait without timeout.
    user.MessageBoxTimeoutW(None, CLOSING_MESSAGE if closing else OPENING_MESSAGE, APP_NAME, 0x10000, 0, NOTICE_TIMEOUT_MS)


def _file_api(kernel32=None):
    kernel = kernel32 or ctypes.WinDLL("kernel32", use_last_error=True)
    if kernel32 is None:
        kernel.CreateFileW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD, wintypes.LPVOID, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
        kernel.CreateFileW.restype = wintypes.HANDLE
        kernel.ReadFile.argtypes = [wintypes.HANDLE, wintypes.LPVOID, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD), wintypes.LPVOID]
        kernel.ReadFile.restype = wintypes.BOOL
        kernel.WriteFile.argtypes = [wintypes.HANDLE, wintypes.LPCVOID, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD), wintypes.LPVOID]
        kernel.WriteFile.restype = wintypes.BOOL
        kernel.SetFilePointerEx.argtypes = [wintypes.HANDLE, ctypes.c_longlong, wintypes.LPVOID, wintypes.DWORD]
        kernel.SetFilePointerEx.restype = wintypes.BOOL
        kernel.SetEndOfFile.argtypes = [wintypes.HANDLE]
        kernel.SetEndOfFile.restype = wintypes.BOOL
        kernel.FlushFileBuffers.argtypes = [wintypes.HANDLE]
        kernel.FlushFileBuffers.restype = wintypes.BOOL
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel.CloseHandle.restype = wintypes.BOOL
    return kernel


def pin_notice(path, *, kernel32=None):
    """Hold DELETE_ON_CLOSE authority granted while the metadata writer is admitted."""
    kernel = _file_api(kernel32)
    handle = kernel.CreateFileW(str(path), 0x10000 | 0x40000000, 1 | 2 | 4, None, 3, 0x04000000 | 0x00200000, None)
    if not handle or handle == ctypes.c_void_p(-1).value:
        raise OSError('instance owner notice could not be pinned')
    class PinnedNotice:
        def update(self, raw):
            if not handle or len(raw) > 4096:
                raise ValueError('instance owner notice update is invalid')
            data = ctypes.create_string_buffer(raw)
            written = wintypes.DWORD()
            if (not kernel.SetFilePointerEx(handle, 0, None, 0)
                    or not kernel.WriteFile(handle, data, len(raw), ctypes.byref(written), None)
                    or written.value != len(raw)
                    or not kernel.SetEndOfFile(handle) or not kernel.FlushFileBuffers(handle)):
                raise OSError('instance owner notice update failed')

        def close(self):
            nonlocal handle
            if handle:
                kernel.CloseHandle(handle)
                handle = None
    return PinnedNotice()


def read_notice(path, *, limit=4096, kernel32=None):
    kernel = _file_api(kernel32)
    handle = kernel.CreateFileW(str(path), 0x80000000, 1 | 2 | 4, None, 3, 0x00200000, None)
    if not handle or handle == ctypes.c_void_p(-1).value:
        raise OSError('instance owner notice could not be read')
    try:
        buffer = ctypes.create_string_buffer(limit + 1)
        count = wintypes.DWORD()
        if not kernel.ReadFile(handle, buffer, len(buffer), ctypes.byref(count), None) or count.value > limit:
            raise ValueError('instance owner notice is invalid')
        return buffer.raw[:count.value]
    finally:
        kernel.CloseHandle(handle)

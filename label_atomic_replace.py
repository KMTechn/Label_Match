"""Retry only transient Windows file replacement refusals for at most one second."""
import ctypes
from ctypes import wintypes
import os
import time

RETRY_SECONDS = 1.0
RETRY_INTERVAL = 0.05


def _replace_once(source, destination, *, write_through):
    if write_through and os.name == "nt":
        move = ctypes.WinDLL("kernel32", use_last_error=True).MoveFileExW
        move.argtypes = [wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD]
        move.restype = wintypes.BOOL
        if not move(os.fspath(source), os.fspath(destination), 0x1 | 0x8):
            raise ctypes.WinError(ctypes.get_last_error())
    else:
        os.replace(source, destination)


def atomic_replace(source, destination, *, write_through=False, deadline=None):
    """Keep caller admission, temporary bytes and first native exception intact."""
    deadline = min(time.monotonic() + RETRY_SECONDS, deadline) if deadline is not None else time.monotonic() + RETRY_SECONDS
    first_error = None
    first_traceback = None
    while True:
        if first_error is not None and time.monotonic() >= deadline:
            raise first_error.with_traceback(first_traceback) from None
        try:
            return _replace_once(source, destination, write_through=write_through)
        except OSError as error:
            if getattr(error, "winerror", None) not in (5, 32):
                raise
            if first_error is None:
                first_error, first_traceback = error, error.__traceback__
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise first_error.with_traceback(first_traceback) from None
            time.sleep(min(RETRY_INTERVAL, remaining))

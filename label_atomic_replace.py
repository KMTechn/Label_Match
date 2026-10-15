"""Retry only transient Windows file replacement refusals for at most one second."""
import ctypes
from ctypes import wintypes
import os
import time

RETRY_SECONDS = 1.0
RETRY_INTERVAL = 0.05


def _replace_once(source, destination, *, write_through):
    os.replace(source, destination)


def _prepare_write_through():
    move = ctypes.WinDLL("kernel32", use_last_error=True).MoveFileExW
    move.argtypes = [wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD]
    move.restype = wintypes.BOOL
    return move


def atomic_replace(source, destination, *, write_through=False, deadline=None):
    """Keep caller admission, temporary bytes and first native exception intact."""
    deadline = min(time.monotonic() + RETRY_SECONDS, deadline) if deadline is not None else time.monotonic() + RETRY_SECONDS
    first_error = None
    first_traceback = None
    move = _prepare_write_through() if write_through and os.name == "nt" else None
    while True:
        if first_error is not None and time.monotonic() >= deadline:
            raise first_error.with_traceback(first_traceback) from None
        try:
            source_path, destination_path = os.fspath(source), os.fspath(destination)
            if time.monotonic() >= deadline:
                if first_error is not None:
                    raise first_error.with_traceback(first_traceback) from None
                raise TimeoutError("Atomic publication deadline exceeded")
            if move is not None:
                if not move(source_path, destination_path, 0x1 | 0x8):
                    raise ctypes.WinError(ctypes.get_last_error())
                return None
            return _replace_once(source_path, destination_path, write_through=write_through)
        except OSError as error:
            if getattr(error, "winerror", None) not in (5, 32):
                raise
            if first_error is None:
                first_error, first_traceback = error, error.__traceback__
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise first_error.with_traceback(first_traceback) from None
            time.sleep(min(RETRY_INTERVAL, remaining))

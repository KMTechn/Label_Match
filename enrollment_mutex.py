"""Fail-closed Windows named mutex for Label enrollment mutations.

Every supported enrollment entrypoint uses the same Local-session mutex as
the other one-session installers.  A wait that observes an abandoned owner
deliberately fails the current attempt: Windows transfers ownership in that
case, but partial local/server state from the terminated owner must be
inspected before another mutation is allowed.
"""

from __future__ import annotations

import ctypes
import math
import os
import sys
import threading
import time
from typing import Any


ENROLLMENT_MUTEX_NAME = r"Local\KMTech.OneSessionInstall.Enrollment.v1"
ENROLLMENT_MUTEX_CONTRACT_VERSION = "label-enrollment-mutex-v1"
DEFAULT_ENROLLMENT_MUTEX_TIMEOUT_SECONDS = 0.0

_WAIT_OBJECT_0 = 0x00000000
_WAIT_ABANDONED = 0x00000080
_WAIT_TIMEOUT = 0x00000102
_WAIT_FAILED = 0xFFFFFFFF
_ERROR_ALREADY_EXISTS = 183
_MAX_FINITE_WAIT_MILLISECONDS = 0xFFFFFFFE
_OWNERSHIP = threading.local()


class EnrollmentMutexError(RuntimeError):
    """Base error carrying a secret-free machine-readable gate receipt."""

    report_status = "BLOCKED_ENROLLMENT_MUTEX_ERROR"
    reason_code = "enrollment_mutex_error"
    recovery_action = "INSPECT_ENROLLMENT_MUTEX_AND_PARTIAL_STATE"

    def __init__(
        self,
        message: str,
        *,
        disposition: str,
        timeout_milliseconds: int,
        wait_milliseconds: int = 0,
        winerror: int | None = None,
    ) -> None:
        super().__init__(message)
        self.mutex_report: dict[str, Any] = {
            "contract_version": ENROLLMENT_MUTEX_CONTRACT_VERSION,
            "name": ENROLLMENT_MUTEX_NAME,
            "scope": "Local",
            "disposition": disposition,
            "contender_pid": os.getpid(),
            "timeout_milliseconds": timeout_milliseconds,
            "wait_milliseconds": wait_milliseconds,
            "server_call_permitted": False,
        }
        if winerror is not None:
            self.mutex_report["winerror"] = int(winerror)


class EnrollmentMutexTimeout(EnrollmentMutexError):
    report_status = "BLOCKED_ENROLLMENT_MUTEX_TIMEOUT"
    reason_code = "enrollment_mutex_timeout"
    recovery_action = "RETRY_AFTER_ACTIVE_ENROLLMENT_COMPLETES"


class EnrollmentMutexAbandoned(EnrollmentMutexError):
    report_status = "BLOCKED_ENROLLMENT_MUTEX_ABANDONED"
    reason_code = "enrollment_mutex_abandoned"
    recovery_action = "INSPECT_PARTIAL_ENROLLMENT_STATE_BEFORE_RETRY"


class EnrollmentMutexNotOwned(EnrollmentMutexError):
    report_status = "BLOCKED_ENROLLMENT_MUTEX_NOT_OWNED"
    reason_code = "enrollment_mutex_not_owned"
    recovery_action = "ROUTE_THROUGH_GUARDED_ENROLLMENT_ENTRYPOINT"


def _timeout_milliseconds(timeout_seconds: float | int) -> int:
    if isinstance(timeout_seconds, bool):
        raise ValueError("enrollment mutex timeout must be a finite non-negative number")
    try:
        value = float(timeout_seconds)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "enrollment mutex timeout must be a finite non-negative number"
        ) from exc
    if not math.isfinite(value) or value < 0:
        raise ValueError("enrollment mutex timeout must be a finite non-negative number")
    milliseconds = int(math.ceil(value * 1000.0))
    if milliseconds > _MAX_FINITE_WAIT_MILLISECONDS:
        raise ValueError("enrollment mutex timeout exceeds the finite Windows wait boundary")
    return milliseconds


def _owned_depth() -> int:
    return int(getattr(_OWNERSHIP, "depth", 0) or 0)


def _increment_owned_depth() -> None:
    _OWNERSHIP.depth = _owned_depth() + 1


def _decrement_owned_depth() -> None:
    depth = _owned_depth()
    if depth <= 1:
        try:
            del _OWNERSHIP.depth
        except AttributeError:
            pass
        return
    _OWNERSHIP.depth = depth - 1


def require_enrollment_mutex_owned() -> None:
    """Reject any server mutation that bypassed a guarded entrypoint."""

    if _owned_depth() > 0:
        return
    raise EnrollmentMutexNotOwned(
        "enrollment server mutation requires the shared named mutex",
        disposition="NOT_OWNED",
        timeout_milliseconds=0,
    )


class EnrollmentMutex:
    """Own and release the canonical Windows enrollment mutex on one thread."""

    def __init__(
        self,
        timeout_seconds: float | int = DEFAULT_ENROLLMENT_MUTEX_TIMEOUT_SECONDS,
    ) -> None:
        self.timeout_milliseconds = _timeout_milliseconds(timeout_seconds)
        self._handle: int | None = None
        self._kernel32: Any | None = None
        self._owner_thread_id: int | None = None
        self._acquired = False
        self.receipt: dict[str, Any] | None = None

    @staticmethod
    def _load_kernel32() -> Any:
        if sys.platform != "win32" or not hasattr(ctypes, "WinDLL"):
            raise EnrollmentMutexError(
                "the enrollment named mutex requires Windows",
                disposition="UNSUPPORTED_PLATFORM",
                timeout_milliseconds=0,
            )
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateMutexW.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_wchar_p]
        kernel32.CreateMutexW.restype = ctypes.c_void_p
        kernel32.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
        kernel32.WaitForSingleObject.restype = ctypes.c_uint32
        kernel32.ReleaseMutex.argtypes = [ctypes.c_void_p]
        kernel32.ReleaseMutex.restype = ctypes.c_int
        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        kernel32.CloseHandle.restype = ctypes.c_int
        return kernel32

    def acquire(self) -> dict[str, Any]:
        if self._handle is not None or self._acquired:
            raise RuntimeError("this enrollment mutex instance is already active")
        kernel32 = self._load_kernel32()
        ctypes.set_last_error(0)
        handle = kernel32.CreateMutexW(None, False, ENROLLMENT_MUTEX_NAME)
        create_error = ctypes.get_last_error()
        if not handle:
            raise EnrollmentMutexError(
                "CreateMutexW failed for the enrollment mutex",
                disposition="CREATE_FAILED",
                timeout_milliseconds=self.timeout_milliseconds,
                winerror=create_error,
            )

        started = time.perf_counter()
        ctypes.set_last_error(0)
        wait_result = int(
            kernel32.WaitForSingleObject(handle, self.timeout_milliseconds)
        )
        wait_error = ctypes.get_last_error() if wait_result == _WAIT_FAILED else None
        waited = max(0, int(round((time.perf_counter() - started) * 1000.0)))
        if wait_result == _WAIT_OBJECT_0:
            self._kernel32 = kernel32
            self._handle = int(handle)
            self._owner_thread_id = threading.get_ident()
            self._acquired = True
            _increment_owned_depth()
            self.receipt = {
                "contract_version": ENROLLMENT_MUTEX_CONTRACT_VERSION,
                "name": ENROLLMENT_MUTEX_NAME,
                "scope": "Local",
                "disposition": "ACQUIRED",
                "owner_pid": os.getpid(),
                "owner_thread_id": self._owner_thread_id,
                "created_new": create_error != _ERROR_ALREADY_EXISTS,
                "timeout_milliseconds": self.timeout_milliseconds,
                "wait_milliseconds": waited,
                "server_call_permitted": True,
            }
            return dict(self.receipt)

        if wait_result == _WAIT_ABANDONED:
            # WAIT_ABANDONED transfers ownership to this thread.  Release that
            # transferred ownership, but reject this attempt so partial state
            # from the terminated owner is never treated as a clean acquire.
            ctypes.set_last_error(0)
            release_ok = bool(kernel32.ReleaseMutex(handle))
            release_error = ctypes.get_last_error() if not release_ok else None
            kernel32.CloseHandle(handle)
            if not release_ok:
                raise EnrollmentMutexError(
                    "abandoned enrollment mutex ownership could not be released",
                    disposition="ABANDONED_RELEASE_FAILED",
                    timeout_milliseconds=self.timeout_milliseconds,
                    wait_milliseconds=waited,
                    winerror=release_error,
                )
            raise EnrollmentMutexAbandoned(
                "enrollment mutex owner terminated without release; current attempt is blocked",
                disposition="ABANDONED_REJECTED",
                timeout_milliseconds=self.timeout_milliseconds,
                wait_milliseconds=waited,
            )

        kernel32.CloseHandle(handle)
        if wait_result == _WAIT_TIMEOUT:
            raise EnrollmentMutexTimeout(
                "another enrollment attempt owns the shared mutex",
                disposition="TIMEOUT",
                timeout_milliseconds=self.timeout_milliseconds,
                wait_milliseconds=waited,
            )
        raise EnrollmentMutexError(
            f"unexpected enrollment mutex wait result: 0x{wait_result:08X}",
            disposition="WAIT_FAILED" if wait_result == _WAIT_FAILED else "WAIT_INVALID",
            timeout_milliseconds=self.timeout_milliseconds,
            wait_milliseconds=waited,
            winerror=wait_error,
        )

    def release(self) -> None:
        if not self._acquired:
            return
        if self._owner_thread_id != threading.get_ident():
            raise EnrollmentMutexError(
                "enrollment mutex must be released by its owning thread",
                disposition="WRONG_THREAD_RELEASE",
                timeout_milliseconds=self.timeout_milliseconds,
            )
        assert self._kernel32 is not None and self._handle is not None
        handle = self._handle
        kernel32 = self._kernel32
        ctypes.set_last_error(0)
        released = bool(kernel32.ReleaseMutex(handle))
        release_error = ctypes.get_last_error() if not released else None
        ctypes.set_last_error(0)
        closed = bool(kernel32.CloseHandle(handle))
        close_error = ctypes.get_last_error() if not closed else None
        self._handle = None
        self._kernel32 = None
        self._owner_thread_id = None
        self._acquired = False
        _decrement_owned_depth()
        if not released:
            raise EnrollmentMutexError(
                "ReleaseMutex failed for the enrollment mutex",
                disposition="RELEASE_FAILED",
                timeout_milliseconds=self.timeout_milliseconds,
                winerror=release_error,
            )
        if not closed:
            raise EnrollmentMutexError(
                "CloseHandle failed for the enrollment mutex",
                disposition="CLOSE_FAILED",
                timeout_milliseconds=self.timeout_milliseconds,
                winerror=close_error,
            )

    def __enter__(self) -> dict[str, Any]:
        return self.acquire()

    def __exit__(self, _exc_type: Any, _exc: Any, _traceback: Any) -> bool:
        self.release()
        return False


__all__ = [
    "DEFAULT_ENROLLMENT_MUTEX_TIMEOUT_SECONDS",
    "ENROLLMENT_MUTEX_CONTRACT_VERSION",
    "ENROLLMENT_MUTEX_NAME",
    "EnrollmentMutex",
    "EnrollmentMutexAbandoned",
    "EnrollmentMutexError",
    "EnrollmentMutexNotOwned",
    "EnrollmentMutexTimeout",
    "require_enrollment_mutex_owned",
]

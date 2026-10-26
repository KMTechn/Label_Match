"""Bound a test PowerShell invocation and contain its native descendants."""
import ctypes
from ctypes import wintypes
import subprocess
import threading
import time
from unittest.mock import patch

_CREATION_LOCK = threading.Lock()


class _BasicLimits(ctypes.Structure):
    _fields_ = [('process_time', ctypes.c_longlong), ('job_time', ctypes.c_longlong),
                ('flags', wintypes.DWORD), ('min_ws', ctypes.c_size_t),
                ('max_ws', ctypes.c_size_t), ('active_limit', wintypes.DWORD),
                ('affinity', ctypes.c_size_t), ('priority', wintypes.DWORD),
                ('scheduling', wintypes.DWORD)]


class _ExtendedLimits(ctypes.Structure):
    _fields_ = [('basic', _BasicLimits), ('io', ctypes.c_ulonglong * 6),
                ('process_memory', ctypes.c_size_t), ('job_memory', ctypes.c_size_t),
                ('peak_process', ctypes.c_size_t), ('peak_job', ctypes.c_size_t)]


class _Accounting(ctypes.Structure):
    _fields_ = [(name, ctypes.c_longlong) for name in ('user', 'kernel', 'period_user', 'period_kernel')] + [
        (name, wintypes.DWORD) for name in ('faults', 'total', 'active', 'terminated')]


def run(command, *, timeout, capture_output=True, **kwargs):
    """Start suspended, attach a private kill-on-close Job, then resume."""
    import _winapi

    assert capture_output and timeout > 0
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)

    def bind(name, result, *args):
        function = getattr(kernel, name)
        function.restype, function.argtypes = result, args
        return function

    create_job = bind('CreateJobObjectW', wintypes.HANDLE, ctypes.c_void_p, wintypes.LPCWSTR)
    set_job = bind('SetInformationJobObject', wintypes.BOOL, wintypes.HANDLE,
                   ctypes.c_int, ctypes.c_void_p, wintypes.DWORD)
    assign = bind('AssignProcessToJobObject', wintypes.BOOL, wintypes.HANDLE, wintypes.HANDLE)
    query = bind('QueryInformationJobObject', wintypes.BOOL, wintypes.HANDLE,
                 ctypes.c_int, ctypes.c_void_p, wintypes.DWORD, ctypes.c_void_p)
    terminate = bind('TerminateJobObject', wintypes.BOOL, wintypes.HANDLE, wintypes.UINT)
    close = bind('CloseHandle', wintypes.BOOL, wintypes.HANDLE)
    open_thread = bind('OpenThread', wintypes.HANDLE, wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
    resume = bind('ResumeThread', wintypes.DWORD, wintypes.HANDLE)

    def checked(value):
        if not value:
            raise ctypes.WinError(ctypes.get_last_error())
        return value

    job = checked(create_job(None, None))
    process = thread = None
    assigned = False

    def active():
        accounting = _Accounting()
        checked(query(job, 1, ctypes.byref(accounting), ctypes.sizeof(accounting), None))
        return accounting.active

    try:
        limits = _ExtendedLimits()
        limits.basic.flags = 0x2000  # KILL_ON_JOB_CLOSE; no breakaway.
        checked(set_job(job, 9, ctypes.byref(limits), ctypes.sizeof(limits)))
        created = {}
        owner_thread = threading.get_ident()
        original_create = _winapi.CreateProcess

        def remember_thread(*args, **arguments):
            handles = original_create(*args, **arguments)
            if threading.get_ident() == owner_thread:
                created[handles[2]] = handles[3]
            return handles

        kwargs['creationflags'] = kwargs.get('creationflags', 0) | subprocess.CREATE_NO_WINDOW | 0x4
        with _CREATION_LOCK, patch.object(_winapi, 'CreateProcess', remember_thread):
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)
        thread = checked(open_thread(0x2, False, created[process.pid]))
        checked(assign(job, int(process._handle)))
        assigned = True
        if resume(thread) == 0xFFFFFFFF:
            raise ctypes.WinError(ctypes.get_last_error())
        close(thread)
        thread = None
        stdout, stderr = process.communicate(timeout=timeout)
        # Job accounting can trail process exit briefly. This wait is local
        # test teardown, never a product timeout or a new field retry policy.
        deadline = time.monotonic() + min(timeout, 1.0)
        remaining = active()
        while remaining and time.monotonic() < deadline:
            time.sleep(0.01)
            remaining = active()
        result = subprocess.CompletedProcess(command, process.returncode, stdout, stderr)
        result.owned_descendants_after = remaining
        assert remaining == 0, 'PowerShell left an owned process behind'
        return result
    finally:
        try:
            if assigned and active():
                checked(terminate(job, 1))
            if process is not None:
                if process.poll() is None:
                    process.kill()
                process.communicate(timeout=timeout)
        finally:
            if thread:
                close(thread)
            close(job)

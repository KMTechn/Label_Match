"""Native integrity transitions using unique mutexes, without GUI/relay startup."""
import ctypes
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

import pytest

import enrollment_mutex as enrollment
import label_match_single_instance as single
import user_relay
import writer_session_fence as fence

pytestmark = pytest.mark.skipif(os.name != "nt", reason="Windows named mutex security")

_LOW_INTEGRITY_CHILD = r'''
import ctypes, json, sys
from ctypes import wintypes
sys.path.insert(0, sys.argv[1])
import enrollment_mutex as enrollment
import label_match_single_instance as single
import user_relay
import writer_session_fence as fence
class SID_AND_ATTRIBUTES(ctypes.Structure):
    _fields_ = [("Sid", ctypes.c_void_p), ("Attributes", wintypes.DWORD)]
class TOKEN_MANDATORY_LABEL(ctypes.Structure):
    _fields_ = [("Label", SID_AND_ATTRIBUTES)]
k = ctypes.WinDLL("kernel32", use_last_error=True)
a = ctypes.WinDLL("advapi32", use_last_error=True)
k.GetCurrentProcess.restype = wintypes.HANDLE
k.CloseHandle.argtypes = [wintypes.HANDLE]
k.LocalFree.argtypes = [ctypes.c_void_p]
a.OpenProcessToken.argtypes = [wintypes.HANDLE, wintypes.DWORD, ctypes.POINTER(wintypes.HANDLE)]
a.ConvertStringSidToSidW.argtypes = [wintypes.LPCWSTR, ctypes.POINTER(ctypes.c_void_p)]
a.SetTokenInformation.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
a.GetLengthSid.argtypes = [ctypes.c_void_p]
a.GetLengthSid.restype = wintypes.DWORD
token, sid = wintypes.HANDLE(), ctypes.c_void_p()
assert a.OpenProcessToken(k.GetCurrentProcess(), 0x0088, ctypes.byref(token)), ctypes.get_last_error()
try:
    assert a.ConvertStringSidToSidW("S-1-16-4096", ctypes.byref(sid)), ctypes.get_last_error()
    label = TOKEN_MANDATORY_LABEL(SID_AND_ATTRIBUTES(sid, 0x20))
    assert a.SetTokenInformation(token, 25, ctypes.byref(label), ctypes.sizeof(label)+a.GetLengthSid(sid)), ctypes.get_last_error()
finally:
    if sid: k.LocalFree(sid)
    k.CloseHandle(token)
exec(sys.argv[2])
'''


def _kernel32():
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateMutexExW.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p,
                                     ctypes.c_ulong, ctypes.c_ulong]
    kernel.CreateMutexExW.restype = ctypes.c_void_p
    kernel.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_ulong]
    kernel.WaitForSingleObject.restype = ctypes.c_ulong
    kernel.ReleaseMutex.argtypes = [ctypes.c_void_p]
    kernel.CloseHandle.argtypes = [ctypes.c_void_p]
    return kernel


def _child(code, *arguments):
    result = subprocess.run(
        [sys.executable, "-I", "-B", "-c", _LOW_INTEGRITY_CHILD,
         str(Path(__file__).resolve().parents[1]), code, *map(str, arguments)],
        capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def _name():
    return "Local\\LabelMatch.MutexTest." + uuid.uuid4().hex


def _abandon(name):
    # Normal child exit abandons ownership; never kill a host process.
    result = subprocess.run(
        [sys.executable, "-I", "-B", "-c",
         "import ctypes,sys; k=ctypes.WinDLL('kernel32',use_last_error=True); "
         "k.CreateMutexExW.argtypes=[ctypes.c_void_p,ctypes.c_wchar_p,ctypes.c_ulong,ctypes.c_ulong]; "
         "k.CreateMutexExW.restype=ctypes.c_void_p; "
         "h=k.CreateMutexExW(None,sys.argv[1],0,0x00100000); assert h; "
         "k.WaitForSingleObject.argtypes=[ctypes.c_void_p,ctypes.c_ulong]; "
         "k.WaitForSingleObject.restype=ctypes.c_ulong; "
         "assert k.WaitForSingleObject(h,0)==0", name],
        capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW,
    )
    assert result.returncode == 0, result.stderr


def test_existing_gui_and_relay_are_duplicates_across_integrity_levels(tmp_path):
    kernel = _kernel32()
    gui_key, relay_key = tmp_path / "gui", tmp_path / "relay"
    handles = [kernel.CreateMutexExW(None, single.mutex_name_for_data_scope(key), 0, 0x00100000)
               for key in (gui_key, relay_key)]
    assert all(handles), ctypes.get_last_error()
    try:
        result = _child(r'''
gui = single.acquire_data_scope_mutex(sys.argv[3])
try:
    relay = user_relay._acquire_relay_lease(sys.argv[4])
    if relay: relay.close()
    print(json.dumps({"gui_owner": gui.owner, "relay_existing": relay is None}))
finally:
    gui.close()
''', gui_key, relay_key)
        assert result == {"gui_owner": False, "relay_existing": True}
    finally:
        for handle in handles:
            kernel.CloseHandle(handle)
    for key in (gui_key, relay_key):
        with single.acquire_data_scope_mutex(key) as replacement:
            assert replacement.owner


@pytest.mark.parametrize("target", ["writer", "enrollment"])
def test_wait_release_and_abandoned_survive_integrity_transition(target):
    kernel = _kernel32()
    name = _name()
    handles = [kernel.CreateMutexExW(None, name + suffix, 0, 0x00100000)
               for suffix in ("", ".free", ".abandoned")]
    assert all(handles), ctypes.get_last_error()
    assert kernel.WaitForSingleObject(handles[0], 0) == 0
    try:
        _abandon(name + ".abandoned")
        result = _child(r'''
name, target = sys.argv[3:5]
if target == "writer":
    assert fence._acquire_named_mutex(name, 0) is None
    assert fence._named_mutex_held_by_other(name) is True
    lease = fence._acquire_named_mutex(name + ".free", 0)
    assert lease is not None and not lease.abandoned
    lease.release()
    lease = fence._acquire_named_mutex(name + ".abandoned", 0)
    assert lease is not None and lease.abandoned
    lease.release()
else:
    enrollment.ENROLLMENT_MUTEX_NAME = name
    try:
        enrollment.EnrollmentMutex(0).acquire()
        raise AssertionError("busy enrollment admitted")
    except enrollment.EnrollmentMutexTimeout:
        pass
    enrollment.ENROLLMENT_MUTEX_NAME = name + ".free"
    with enrollment.EnrollmentMutex(0) as receipt:
        assert receipt["disposition"] == "ACQUIRED" and not receipt["created_new"]
        enrollment.require_enrollment_mutex_owned()
    enrollment.ENROLLMENT_MUTEX_NAME = name + ".abandoned"
    try:
        enrollment.EnrollmentMutex(0).acquire()
        raise AssertionError("abandoned enrollment admitted")
    except enrollment.EnrollmentMutexAbandoned as exc:
        assert exc.mutex_report["disposition"] == "ABANDONED_REJECTED"
print(json.dumps({"busy": True, "free_released": True, "abandoned": True}))
''', name, target)
        assert result == {"busy": True, "free_released": True, "abandoned": True}
        # Failed release would produce WAIT_ABANDONED after the child exits.
        for handle in handles[1:]:
            assert kernel.WaitForSingleObject(handle, 0) == 0
            assert kernel.ReleaseMutex(handle)
    finally:
        kernel.ReleaseMutex(handles[0])
        for handle in handles:
            kernel.CloseHandle(handle)


@pytest.mark.parametrize("target", ["gui", "relay", "writer", "enrollment"])
def test_actual_dacl_denial_remains_an_error(tmp_path, monkeypatch, target):
    class SECURITY_ATTRIBUTES(ctypes.Structure):
        _fields_ = [("nLength", ctypes.c_ulong), ("lpSecurityDescriptor", ctypes.c_void_p),
                    ("bInheritHandle", ctypes.c_int)]
    kernel = _kernel32()
    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    convert = advapi.ConvertStringSecurityDescriptorToSecurityDescriptorW
    convert.argtypes = [ctypes.c_wchar_p, ctypes.c_ulong,
                       ctypes.POINTER(ctypes.c_void_p), ctypes.c_void_p]
    descriptor = ctypes.c_void_p()
    assert convert("D:P", 1, ctypes.byref(descriptor), None)
    attributes = SECURITY_ATTRIBUTES(ctypes.sizeof(SECURITY_ATTRIBUTES), descriptor, False)
    name = single.mutex_name_for_data_scope(tmp_path) if target in {"gui", "relay"} else _name()
    handle = None
    try:
        handle = kernel.CreateMutexExW(ctypes.byref(attributes), name, 0, 0)
        assert handle, ctypes.get_last_error()
        if target == "gui":
            with pytest.raises(single.SingleInstanceError, match=r"failed \(5\)"):
                single.acquire_data_scope_mutex(tmp_path)
        elif target == "relay":
            with pytest.raises(single.SingleInstanceError, match=r"failed \(5\)"):
                user_relay._acquire_relay_lease(tmp_path)
        elif target == "writer":
            with pytest.raises(fence.WriterFenceError) as caught:
                fence._acquire_named_mutex(name, 0)
            assert caught.value.code == "WRITER_MUTEX_CREATE_FAILED"
        else:
            monkeypatch.setattr(enrollment, "ENROLLMENT_MUTEX_NAME", name)
            with pytest.raises(enrollment.EnrollmentMutexError) as caught:
                enrollment.EnrollmentMutex(0).acquire()
            assert caught.value.mutex_report["disposition"] == "CREATE_FAILED"
            assert caught.value.mutex_report["winerror"] == 5
    finally:
        if handle: kernel.CloseHandle(handle)
        kernel.LocalFree.argtypes = [ctypes.c_void_p]
        kernel.LocalFree(descriptor)

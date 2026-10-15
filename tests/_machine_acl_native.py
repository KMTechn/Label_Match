"""Native ACL fixture; only the host paths above the fixture use a trust facade."""
import ctypes
from ctypes import wintypes as w
import ntpath
from pathlib import Path


def bind_native_acl_probe(program_data, anchor, monkeypatch=None, untrusted_owner_path=''):
    original = ctypes.WinDLL
    kernel = original('kernel32', use_last_error=True)
    native = original('advapi32', use_last_error=True)
    kernel.CreateFileW.argtypes = [w.LPCWSTR, w.DWORD, w.DWORD, w.LPVOID, w.DWORD, w.DWORD, w.HANDLE]
    kernel.CreateFileW.restype = w.HANDLE
    kernel.GetFinalPathNameByHandleW.argtypes = [w.HANDLE, w.LPWSTR, w.DWORD, w.DWORD]
    kernel.GetFinalPathNameByHandleW.restype = w.DWORD
    kernel.LocalFree.argtypes = [w.HLOCAL]
    kernel.LocalFree.restype = w.HLOCAL
    anchor_handle = kernel.CreateFileW(str(anchor), 0x20080, 1, None, 3, 0x02200000, None)
    assert anchor_handle not in (None, ctypes.c_void_p(-1).value)
    fixture = ntpath.normcase(str(Path(program_data).absolute()))
    seen = []
    actual_security_info = native.GetSecurityInfo
    security_types = [w.HANDLE, w.DWORD, w.DWORD, ctypes.POINTER(w.LPVOID),
        w.LPVOID, ctypes.POINTER(w.LPVOID), w.LPVOID, ctypes.POINTER(w.LPVOID)]
    actual_security_info.argtypes = security_types
    owner, dacl, descriptor = w.LPVOID(), w.LPVOID(), w.LPVOID()
    assert not actual_security_info(anchor_handle,1,5,ctypes.byref(owner),None,
        ctypes.byref(dacl),None,ctypes.byref(descriptor))
    actual_convert = native.ConvertSidToStringSidW
    actual_convert.argtypes = [w.LPVOID,ctypes.POINTER(w.LPWSTR)]
    current = w.LPWSTR()
    assert actual_convert(owner,ctypes.byref(current))
    current_sid=current.value
    kernel.LocalFree(current);kernel.LocalFree(descriptor)
    native.ConvertStringSidToSidW.argtypes = [w.LPCWSTR,ctypes.POINTER(w.LPVOID)]
    privileged_sid, ordinary_sid = w.LPVOID(),w.LPVOID()
    assert native.ConvertStringSidToSidW('S-1-5-32-544',ctypes.byref(privileged_sid))
    assert native.ConvertStringSidToSidW('S-1-5-32-545',ctypes.byref(ordinary_sid))
    bad_owner=ntpath.normcase(str(Path(untrusted_owner_path).absolute())) if untrusted_owner_path else ''

    def convert_sid(sid, output):
        actual_convert.argtypes=convert_sid.argtypes
        result=actual_convert(sid,output)
        text=ctypes.cast(output,ctypes.POINTER(w.LPWSTR)).contents
        if result and text.value==current_sid:
            kernel.LocalFree(text)
            return actual_convert(privileged_sid,output)
        return result

    def security_info(handle, *args):
        actual_security_info.argtypes = security_info.argtypes
        buffer = ctypes.create_unicode_buffer(32768)
        assert kernel.GetFinalPathNameByHandleW(handle, buffer, len(buffer), 0)
        path = ntpath.normcase(buffer.value.removeprefix('\\\\?\\'))
        inside = path == fixture or path.startswith(fixture + '\\')
        if inside:
            seen.append(path)
        result=actual_security_info(handle if inside else anchor_handle, *args)
        if not result and path==bad_owner:
            ctypes.cast(args[2],ctypes.POINTER(w.LPVOID)).contents.value=ordinary_sid.value
        return result

    class NativeAclFacade:
        GetSecurityInfo = staticmethod(security_info)
        ConvertSidToStringSidW = staticmethod(convert_sid)
        def __getattr__(self, name):
            return getattr(native, name)

    def dll(name, *args, **kwargs):
        return NativeAclFacade() if str(name).lower() == 'advapi32' else original(name, *args, **kwargs)

    if monkeypatch is None:
        ctypes.WinDLL = dll
    else:
        monkeypatch.setattr(ctypes, 'WinDLL', dll)
    return seen

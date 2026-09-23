"""Handle-checked file access for package recovery and rendered labels.

Directory handles stay open while the final file is read or renamed, so a
checked parent cannot be replaced with a junction between check and use.
"""

from __future__ import annotations

from contextlib import contextmanager, ExitStack
import ctypes
from ctypes import wintypes
import hashlib
import json
import msvcrt
import os
from pathlib import Path
import uuid


_INVALID = ctypes.c_void_p(-1).value
_READ = 0x80000000
_WRITE = 0x40000000
_DELETE = 0x00010000
_SHARE_READ_WRITE = 0x00000001 | 0x00000002
_OPEN_EXISTING = 3
_CREATE_NEW = 1
_BACKUP_SEMANTICS = 0x02000000
_OPEN_REPARSE_POINT = 0x00200000
_REPARSE = 0x00000400
_DIRECTORY = 0x00000010
_FILE_RENAME_INFO = 3
_FILE_DISPOSITION_INFO = 4


class _FileInfo(ctypes.Structure):
    _fields_ = [("attributes", wintypes.DWORD), ("created", wintypes.FILETIME),
                ("accessed", wintypes.FILETIME), ("written", wintypes.FILETIME),
                ("volume", wintypes.DWORD), ("size_high", wintypes.DWORD),
                ("size_low", wintypes.DWORD), ("links", wintypes.DWORD),
                ("index_high", wintypes.DWORD), ("index_low", wintypes.DWORD)]


_kernel = ctypes.WinDLL("kernel32", use_last_error=True)
_create = _kernel.CreateFileW
_create.argtypes = (wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                    wintypes.LPVOID, wintypes.DWORD, wintypes.DWORD,
                    wintypes.HANDLE)
_create.restype = wintypes.HANDLE
_information = _kernel.GetFileInformationByHandle
_information.argtypes = (wintypes.HANDLE, ctypes.POINTER(_FileInfo))
_information.restype = wintypes.BOOL
_final_name = _kernel.GetFinalPathNameByHandleW
_final_name.argtypes = (wintypes.HANDLE, wintypes.LPWSTR, wintypes.DWORD,
                        wintypes.DWORD)
_final_name.restype = wintypes.DWORD
_close = _kernel.CloseHandle
_close.argtypes = (wintypes.HANDLE,)
_close.restype = wintypes.BOOL
_write = _kernel.WriteFile
_write.argtypes = (wintypes.HANDLE, wintypes.LPCVOID, wintypes.DWORD,
                   ctypes.POINTER(wintypes.DWORD), wintypes.LPVOID)
_write.restype = wintypes.BOOL
_flush = _kernel.FlushFileBuffers
_flush.argtypes = (wintypes.HANDLE,)
_flush.restype = wintypes.BOOL
_rename = _kernel.SetFileInformationByHandle
_rename.argtypes = (wintypes.HANDLE, ctypes.c_int, wintypes.LPVOID,
                    wintypes.DWORD)
_rename.restype = wintypes.BOOL


def _path(value):
    raw = Path(value)
    if ".." in raw.parts or not raw.is_absolute():
        raise ValueError("unsafe file path")
    return raw


def _inside(path, root):
    if root is not None and os.path.commonpath(
            (os.path.normcase(str(path)), os.path.normcase(str(_path(root))))) \
            != os.path.normcase(str(_path(root))):
        raise ValueError("file path escapes its allowed root")


def _native_path(path):
    value = str(path)
    if value.startswith("\\\\"):
        return "\\\\?\\UNC\\" + value[2:]
    return "\\\\?\\" + value


def _open(path, access, disposition=_OPEN_EXISTING, flags=_OPEN_REPARSE_POINT):
    handle = _create(_native_path(path), access, _SHARE_READ_WRITE, None,
                     disposition, flags, None)
    if handle == _INVALID:
        raise ctypes.WinError(ctypes.get_last_error())
    return handle


def _checked(handle, path, *, directory=False):
    info = _FileInfo()
    if not _information(handle, ctypes.byref(info)):
        raise ctypes.WinError(ctypes.get_last_error())
    if info.attributes & _REPARSE:
        raise ValueError("file path contains a reparse point")
    if bool(info.attributes & _DIRECTORY) != directory:
        raise ValueError("file path has the wrong entry type")
    if not directory and info.links != 1:
        raise ValueError("file path is a hard link")
    buffer = ctypes.create_unicode_buffer(32768)
    length = _final_name(handle, buffer, len(buffer), 0)
    if not length or length >= len(buffer):
        raise ctypes.WinError(ctypes.get_last_error())
    actual = buffer.value
    if actual.startswith("\\\\?\\UNC\\"):
        actual = "\\\\" + actual[8:]
    elif actual.startswith("\\\\?\\"):
        actual = actual[4:]
    if os.path.normcase(actual) != os.path.normcase(str(path)):
        raise ValueError("file handle resolves outside its checked path")
    return info


@contextmanager
def _parents(path, *, create=False):
    path = _path(path)
    with ExitStack() as stack:
        directory = Path(path.anchor)
        for component in (directory, *[
                directory.joinpath(*path.parts[1:index + 1])
                for index in range(1, len(path.parts) - 1)]):
            if create and not component.exists():
                component.mkdir()
            handle = _open(component, _READ,
                           flags=_BACKUP_SEMANTICS | _OPEN_REPARSE_POINT)
            stack.callback(_close, handle)
            _checked(handle, component, directory=True)
        yield path


def read_checked_bytes(path, *, allowed_root=None):
    path = _path(path)
    _inside(path, allowed_root)
    with _parents(path):
        handle = _open(path, _READ)
        try:
            _checked(handle, path)
            descriptor = msvcrt.open_osfhandle(handle, os.O_RDONLY)
            handle = None
            with os.fdopen(descriptor, "rb") as stream:
                return stream.read()
        finally:
            if handle is not None:
                _close(handle)


def describe_entry(path, *, allowed_root=None):
    """Describe a final entry without reading its target or following a link."""
    path = _path(path)
    _inside(path, allowed_root)
    with _parents(path):
        handle = _open(path, _READ, flags=_BACKUP_SEMANTICS | _OPEN_REPARSE_POINT)
        try:
            info = _FileInfo()
            if not _information(handle, ctypes.byref(info)):
                raise ctypes.WinError(ctypes.get_last_error())
            return {"attributes": int(info.attributes), "links": int(info.links),
                    "volume": int(info.volume),
                    "index": (int(info.index_high) << 32) | int(info.index_low),
                    "size": (int(info.size_high) << 32) | int(info.size_low)}
        finally:
            _close(handle)


def entry_digest(path, description):
    encoded = json.dumps({"path": os.path.normcase(str(_path(path))),
                          "entry": description}, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def quarantine_entry(source, target, *, description, allowed_root=None):
    """Rename an unverified entry itself; never access link target bytes."""
    source, target = _path(source), _path(target)
    _inside(source, allowed_root)
    _inside(target, allowed_root)
    with _parents(source), _parents(target):
        handle = _open(source, _READ | _DELETE,
                       flags=_BACKUP_SEMANTICS | _OPEN_REPARSE_POINT)
        try:
            info = _FileInfo()
            if not _information(handle, ctypes.byref(info)):
                raise ctypes.WinError(ctypes.get_last_error())
            actual = {"attributes": int(info.attributes), "links": int(info.links),
                      "volume": int(info.volume),
                      "index": (int(info.index_high) << 32) | int(info.index_low),
                      "size": (int(info.size_high) << 32) | int(info.size_low)}
            if actual != description:
                raise ValueError("unverified entry changed before quarantine")
            _rename_handle(handle, target, replace=False)
        finally:
            _close(handle)
    if describe_entry(target, allowed_root=allowed_root) != description:
        raise ValueError("unverified entry changed after quarantine")


def _rename_handle(handle, target, *, replace):
    native_target = _native_path(target)
    encoded = native_target.encode("utf-16-le")
    class _RenameInfo(ctypes.Structure):
        _fields_ = [("replace", wintypes.BOOLEAN),
                    ("root", wintypes.HANDLE),
                    ("length", wintypes.DWORD),
                    ("name", ctypes.c_wchar * (len(encoded) // 2 + 1))]
    info = _RenameInfo()
    info.replace = bool(replace)
    info.root = None
    info.length = len(encoded)
    info.name = native_target
    if not _rename(handle, _FILE_RENAME_INFO, ctypes.byref(info), ctypes.sizeof(info)):
        raise ctypes.WinError(ctypes.get_last_error())


def replace_checked(source, target, *, allowed_root=None, replace=True):
    source, target = _path(source), _path(target)
    _inside(source, allowed_root)
    _inside(target, allowed_root)
    with _parents(source), _parents(target):
        handle = _open(source, _READ | _DELETE)
        try:
            _checked(handle, source)
            if os.path.lexists(target):
                destination = _open(target, _READ)
                try:
                    _checked(destination, target)
                finally:
                    _close(destination)
            _rename_handle(handle, target, replace=replace)
        finally:
            _close(handle)


def unlink_checked(path, *, allowed_root=None):
    path = _path(path)
    _inside(path, allowed_root)
    with _parents(path):
        handle = _open(path, _READ | _DELETE)
        try:
            _checked(handle, path)
            deletion = wintypes.BOOL(True)
            if not _rename(handle, _FILE_DISPOSITION_INFO,
                           ctypes.byref(deletion), ctypes.sizeof(deletion)):
                raise ctypes.WinError(ctypes.get_last_error())
        finally:
            _close(handle)


def write_checked_bytes(path, payload, *, allowed_root=None, replace=True):
    """Fsync a new local file, verify its handle, then atomically rename it."""
    path = _path(path)
    _inside(path, allowed_root)
    if not isinstance(payload, bytes):
        raise TypeError("checked file payload must be bytes")
    with _parents(path, create=True):
        temporary = path.with_name(f".{path.name}.tmp.{uuid.uuid4().hex}")
        handle = _open(temporary, _WRITE | _DELETE, _CREATE_NEW)
        try:
            offset = 0
            while offset < len(payload):
                chunk = payload[offset:offset + 1024 * 1024]
                written = wintypes.DWORD()
                buffer = ctypes.create_string_buffer(chunk)
                if not _write(handle, buffer, len(chunk), ctypes.byref(written), None):
                    raise ctypes.WinError(ctypes.get_last_error())
                offset += written.value
            if not _flush(handle):
                raise ctypes.WinError(ctypes.get_last_error())
            _checked(handle, temporary)
            if os.path.lexists(path):
                destination = _open(path, _READ)
                try:
                    _checked(destination, path)
                finally:
                    _close(destination)
            _rename_handle(handle, path, replace=replace)
        finally:
            _close(handle)
            if os.path.lexists(temporary):
                unlink_checked(temporary, allowed_root=allowed_root)

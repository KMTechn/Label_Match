"""Opt-in, protected Windows JSONL diagnostics outside application data roots.

Only the three named mutation functions touch disk. This is best-effort
diagnostics, not a durable business journal; no admission probe or fsync occurs.
"""
from __future__ import annotations

import atexit
import ctypes
from ctypes import wintypes
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import re
import stat
import threading
import time
from typing import Callable, Final, Sequence
import uuid

from kmtech_shared.data_generation import _cause_identifier

if os.name == "nt":
    import msvcrt

FRAMES_ONLY: Final = object()
SCHEMA = "kmtech-diagnostic-log-v1"
MAX_BYTES = 1_048_576
RECORD_BYTES = 16_384
WAIT_SECONDS = 0.050
_ACTIVE = threading.local()
_DROPS = ("dropped_redact", "dropped_lock", "dropped_reentrant",
          "dropped_rotate", "dropped_error")
_TOKEN = re.compile(r'''(?i)(["']?\b(?:runtime_request_token|next_request_token)\b["']?\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^\s,;}\]]+)''')
_AUTHORIZATION = re.compile(r"(?im)(\bauthorization\s*:\s*)[^\r\n]*")


class _RedactionError(Exception):
    pass


def _utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _literal(path: str | Path) -> Path:
    path = Path(path)
    if not path.is_absolute() or str(path).startswith(("\\\\", "\\\\?\\", "\\\\.\\")):
        raise ValueError("diagnostic path must be local, literal and absolute")
    path = Path(os.path.abspath(path))
    for component in (*reversed(path.parents), path):
        try:
            info = component.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
            raise ValueError("diagnostic path contains a reparse point")
        if component != path and not stat.S_ISDIR(info.st_mode):
            raise ValueError("diagnostic ancestor is not a directory")
    return path


def _regular(path: Path) -> None:
    _literal(path)
    try:
        info = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise ValueError("diagnostic file is not an ordinary single-link file")


class _Security:
    """Native descriptor conversion/readback; file mutations stay in the caller."""

    def __init__(self):
        self.kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        self.advapi = ctypes.WinDLL("advapi32", use_last_error=True)
        pointer = ctypes.c_void_p
        out = ctypes.POINTER(pointer)
        signatures = (
            (self.kernel, "GetCurrentProcess", [], wintypes.HANDLE),
            (self.kernel, "GetProcessTimes", [wintypes.HANDLE] + [ctypes.POINTER(wintypes.FILETIME)] * 4, wintypes.BOOL),
            (self.kernel, "CloseHandle", [wintypes.HANDLE], wintypes.BOOL),
            (self.kernel, "LocalFree", [pointer], pointer),
            (self.kernel, "CreateDirectoryW", [wintypes.LPCWSTR, pointer], wintypes.BOOL),
            (self.advapi, "OpenProcessToken", [wintypes.HANDLE, wintypes.DWORD, out], wintypes.BOOL),
            (self.advapi, "GetTokenInformation", [pointer, ctypes.c_int, pointer, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD)], wintypes.BOOL),
            (self.advapi, "ConvertSidToStringSidW", [pointer, out], wintypes.BOOL),
            (self.advapi, "ConvertStringSecurityDescriptorToSecurityDescriptorW", [wintypes.LPCWSTR, wintypes.DWORD, out, pointer], wintypes.BOOL),
            (self.advapi, "ConvertSecurityDescriptorToStringSecurityDescriptorW", [pointer, wintypes.DWORD, wintypes.DWORD, out, pointer], wintypes.BOOL),
            (self.advapi, "GetNamedSecurityInfoW", [wintypes.LPCWSTR, ctypes.c_int, wintypes.DWORD, out, out, out, out, out], wintypes.DWORD),
            (self.advapi, "GetSecurityDescriptorDacl", [pointer, ctypes.POINTER(wintypes.BOOL), out, ctypes.POINTER(wintypes.BOOL)], wintypes.BOOL),
            (self.advapi, "SetNamedSecurityInfoW", [wintypes.LPWSTR, ctypes.c_int, wintypes.DWORD, pointer, pointer, pointer, pointer], wintypes.DWORD),
        )
        for library, name, args, result in signatures:
            function = getattr(library, name)
            function.argtypes, function.restype = args, result

    def sid_text(self, sid) -> str:
        text = ctypes.c_void_p()
        if not self.advapi.ConvertSidToStringSidW(sid, ctypes.byref(text)):
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            return ctypes.wstring_at(text)
        finally:
            self.kernel.LocalFree(text)

    def process_start(self) -> str:
        created, exited, kernel, user = (wintypes.FILETIME() for _ in range(4))
        if not self.kernel.GetProcessTimes(self.kernel.GetCurrentProcess(), ctypes.byref(created), ctypes.byref(exited), ctypes.byref(kernel), ctypes.byref(user)):
            raise ctypes.WinError(ctypes.get_last_error())
        ticks = (created.dwHighDateTime << 32) | created.dwLowDateTime
        return datetime.fromtimestamp(ticks / 10_000_000 - 11_644_473_600, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def user_sid(self) -> str:
        token = ctypes.c_void_p()
        if not self.advapi.OpenProcessToken(self.kernel.GetCurrentProcess(), 8, ctypes.byref(token)):
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            size = wintypes.DWORD()
            self.advapi.GetTokenInformation(token, 1, None, 0, ctypes.byref(size))
            if not size.value:
                raise ctypes.WinError(ctypes.get_last_error())
            buffer = ctypes.create_string_buffer(size.value)
            if not self.advapi.GetTokenInformation(token, 1, buffer, size, ctypes.byref(size)):
                raise ctypes.WinError(ctypes.get_last_error())
            return self.sid_text(ctypes.c_void_p.from_buffer(buffer))
        finally:
            self.kernel.CloseHandle(token)

    def descriptor(self, sddl: str):
        descriptor = ctypes.c_void_p()
        if not self.advapi.ConvertStringSecurityDescriptorToSecurityDescriptorW(sddl, 1, ctypes.byref(descriptor), None):
            raise ctypes.WinError(ctypes.get_last_error())
        return descriptor

    def sddl(self, descriptor) -> str:
        text = ctypes.c_void_p()
        if not self.advapi.ConvertSecurityDescriptorToStringSecurityDescriptorW(descriptor, 1, 4, ctypes.byref(text), None):
            raise ctypes.WinError(ctypes.get_last_error())
        try:
            return ctypes.wstring_at(text)
        finally:
            self.kernel.LocalFree(text)

    def read(self, path: Path) -> tuple[str, str]:
        owner, descriptor = ctypes.c_void_p(), ctypes.c_void_p()
        error = self.advapi.GetNamedSecurityInfoW(str(path), 1, 5, ctypes.byref(owner), None, None, None, ctypes.byref(descriptor))
        if error:
            raise ctypes.WinError(error)
        try:
            return self.sid_text(owner), self.sddl(descriptor)
        finally:
            self.kernel.LocalFree(descriptor)


def _normalized_dacl(sddl: str) -> tuple[str, tuple[str, ...]]:
    # ACE ordering and the auto-inherited bookkeeping flag do not change access.
    match = re.fullmatch(r"D:([A-Z]*)((?:\([^()]+\))+)", sddl)
    if match is None:
        raise ValueError("unexpected diagnostic DACL")
    flags = match[1].replace("AI", "")
    return flags, tuple(sorted(re.findall(r"\([^()]+\)", match[2])))


def _create_protected_root(log: DiagnosticLog) -> None:
    """DL-12 mutation 1: parents, protected leaf, optional repair, lifetime fd."""
    root = _literal(log._root)
    root.parent.mkdir(parents=True, exist_ok=True)
    _literal(root)
    security = _Security()
    sid = security.user_sid()
    log._process_start = security.process_start()
    sddl = f"D:P(A;OICI;FA;;;{sid})(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)"
    descriptor = security.descriptor(sddl)
    try:
        class SecurityAttributes(ctypes.Structure):
            _fields_ = [("nLength", wintypes.DWORD), ("lpSecurityDescriptor", ctypes.c_void_p), ("bInheritHandle", wintypes.BOOL)]
        attributes = SecurityAttributes(ctypes.sizeof(SecurityAttributes), descriptor, False)
        created = security.kernel.CreateDirectoryW(str(root), ctypes.byref(attributes))
        if not created and ctypes.get_last_error() != 183:
            raise ctypes.WinError(ctypes.get_last_error())
        if not _literal(root).is_dir():
            raise ValueError("diagnostic root is not a directory")
        expected = _normalized_dacl(security.sddl(descriptor))
        owners = {sid, "S-1-5-18", "S-1-5-32-544"}
        owner, actual = security.read(root)
        if owner not in owners:
            raise ValueError("diagnostic owner is not trusted")
        if _normalized_dacl(actual) != expected:
            if created:
                raise ValueError("diagnostic DACL differs")
            _literal(root)
            present, defaulted, dacl = wintypes.BOOL(), wintypes.BOOL(), ctypes.c_void_p()
            if not security.advapi.GetSecurityDescriptorDacl(descriptor, ctypes.byref(present), ctypes.byref(dacl), ctypes.byref(defaulted)) or not present.value or not dacl.value:
                raise ValueError("diagnostic DACL is absent")
            error = security.advapi.SetNamedSecurityInfoW(str(root), 1, 0x80000004, None, None, dacl, None)
            if error:
                raise ctypes.WinError(error)
            owner, actual = security.read(root)
            if owner not in owners or _normalized_dacl(actual) != expected:
                raise ValueError("diagnostic DACL repair failed")
        expected_file = _normalized_dacl(f"D:(A;ID;FA;;;{sid})(A;ID;FA;;;SY)(A;ID;FA;;;BA)")
        for name in ("diagnostic.lock", "diagnostic.jsonl", "diagnostic.jsonl.1", "diagnostic.jsonl.2"):
            path = root / name
            _regular(path)
            if not path.exists():
                continue
            owner, _ = security.read(path)
            if owner not in owners:
                raise ValueError("diagnostic file owner is not trusted")
            # Always discard inherited ACEs carried in by a same-volume move.
            # An empty (not NULL) DACL plus UNPROTECTED inherits the leaf ACL.
            empty = security.descriptor("D:")
            try:
                present, defaulted, dacl = wintypes.BOOL(), wintypes.BOOL(), ctypes.c_void_p()
                if not security.advapi.GetSecurityDescriptorDacl(empty, ctypes.byref(present), ctypes.byref(dacl), ctypes.byref(defaulted)) or not present.value or not dacl.value:
                    raise ValueError("diagnostic empty DACL is absent")
                _regular(path)
                error = security.advapi.SetNamedSecurityInfoW(str(path), 1, 0x20000004, None, None, dacl, None)
                if error:
                    raise ctypes.WinError(error)
                owner, actual = security.read(path)
                if owner not in owners or _normalized_dacl(actual) != expected_file:
                    raise ValueError("diagnostic file DACL repair failed")
            finally:
                security.kernel.LocalFree(empty)
        log._lock_file = (root / "diagnostic.lock").open("a+b")
    finally:
        security.kernel.LocalFree(descriptor)


def _rotate_locked(log: DiagnosticLog) -> None:
    """DL-12 mutation 3: size-only rotation under both locks."""
    current = log._root / "diagnostic.jsonl"
    first, second = current.with_name(current.name + ".1"), current.with_name(current.name + ".2")
    for path in (current, first, second):
        _regular(path)
    if current.exists():
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        create = kernel.CreateFileW
        create.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.DWORD,
                           ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, wintypes.HANDLE]
        create.restype = wintypes.HANDLE
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel.CloseHandle.restype = wintypes.BOOL
        handle = create(str(current), 0x10000, 7, None, 3, 0x80, None)
        if handle == ctypes.c_void_p(-1).value:
            raise ctypes.WinError(ctypes.get_last_error())
        if not kernel.CloseHandle(handle):
            raise ctypes.WinError(ctypes.get_last_error())
    if first.exists():
        os.replace(first, second)
    if current.exists():
        os.rename(current, first)


def _append_locked(log: DiagnosticLog, raw: bytes) -> bool:
    """DL-12 mutation 2: one append/flush, with bounded rotation fallback."""
    current = log._root / "diagnostic.jsonl"
    _regular(current)
    size = current.stat().st_size if current.exists() else 0
    if size + len(raw) > log._max_bytes:
        try:
            _rotate_locked(log)
        except Exception:
            # Some renames may already have succeeded. Reobserve the live file.
            _regular(current)
            size = current.stat().st_size if current.exists() else 0
            if size + len(raw) > 2 * log._max_bytes:
                log._counts["dropped_rotate"] += 1
                return False
    with current.open("ab") as handle:
        handle.write(raw)
        handle.flush()
    return True


def _input_bound(value: str) -> str:
    raw = value.encode("utf-8", errors="replace")
    if len(raw) <= 65_536:
        return value
    return f"<omitted:{len(raw)} bytes>"


def _byte_bound(value: str, maximum: int) -> str:
    return value.encode("utf-8", errors="replace")[:maximum].decode("utf-8", errors="ignore")


def _json_line(record: dict) -> bytes:
    return (json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")


def _serialize(record: dict, *, source_identifier: bool = False) -> bytes:
    raw = _json_line(record)
    # Preserve all bounded frame coordinates; shorten already-redacted strings
    # when three long exception chains otherwise exceed the per-record cap.
    limit = 512
    def shorten(value):
        if isinstance(value, str):
            return _byte_bound(value, limit)
        if isinstance(value, list):
            return [shorten(item) for item in value]
        if isinstance(value, dict):
            return {key: item if key in {"schema", "ts", "session", "kind"} else
                    _cause_identifier(_byte_bound(item, limit)) if (key in {"type", "type_module", "module", "function"} or key == "source" and source_identifier) and isinstance(item, str) else
                    shorten(item) for key, item in value.items()}
        return value
    while len(raw) > RECORD_BYTES and limit:
        record = shorten(record)
        raw = _json_line(record)
        limit //= 2
    if len(raw) > RECORD_BYTES:
        raise ValueError("diagnostic record is oversized")
    return raw


class DiagnosticLog:
    def __init__(self):
        self._counts = dict(written=0, **{key: 0 for key in _DROPS}, disabled=1)
        self._reported = {key: 0 for key in _DROPS}
        self._mutex = threading.Lock()
        self._lock_file = None
        self._hooks_installed = False
        self._session = ""
        self._process_start = ""

    def __del__(self):
        try:
            if self._lock_file is not None:
                self._lock_file.close()
        except Exception:
            pass

    def _clean(self, value: str, *, maximum: int = 8192) -> str:
        if type(value) is not str:
            raise _RedactionError()
        if self._redact is FRAMES_ONLY:
            return _cause_identifier(value)
        try:
            value = self._redact(_input_bound(value))
            if type(value) is not str:
                raise _RedactionError()
            value = _AUTHORIZATION.sub(r"\1[redacted]", _TOKEN.sub(r"\1[redacted]", value))
            return _byte_bound(value, maximum)
        except Exception as error:
            raise _RedactionError() from error

    def _identifier(self, value: object) -> str:
        return _cause_identifier(self._clean(_cause_identifier(value), maximum=800))

    def _exception(self, error: BaseException) -> list[dict]:
        chain, seen = [], set()
        while error is not None and len(chain) < 3 and id(error) not in seen:
            seen.add(id(error))
            frames = []
            traceback = error.__traceback__
            while traceback is not None:
                frame = traceback.tb_frame
                line = traceback.tb_lineno
                frames.append({"module": self._identifier(frame.f_globals.get("__name__")),
                               "function": self._identifier(frame.f_code.co_name),
                               "line": line if type(line) is int and 1 <= line <= 10_000_000 else None})
                traceback = traceback.tb_next
            item = {"type": self._identifier(type(error).__qualname__),
                    "type_module": self._identifier(type(error).__module__),
                    "frames": frames if len(frames) <= 30 else frames[:10] + frames[-20:]}
            if self._redact is not FRAMES_ONLY:
                item["message"] = self._clean(str(error))[:500]
            chain.append(item)
            error = error.__cause__ if error.__cause__ is not None else (None if error.__suppress_context__ else error.__context__)
        return chain

    def _submit(self, level, source, message, exc=None, extra=None, *, counts_only=False) -> bool:
        if getattr(_ACTIVE, "recording", False):
            self._counts["dropped_reentrant"] += 1
            return False
        if self._counts["disabled"]:
            self._counts["dropped_error"] += 1
            return False
        _ACTIVE.recording = True
        file_locked = mutex_locked = False
        deadline = time.monotonic() + WAIT_SECONDS
        try:
            snapshot = {key: self._counts[key] for key in _DROPS}
            summary = _json_line({"kind": "drop_counts", **snapshot}) if snapshot != self._reported else b""
            if counts_only:
                if not summary:
                    return True
                raw = summary
            else:
                record = {"schema": SCHEMA, "ts": _utc(), "session": self._session,
                          "pid": os.getpid(), "level": self._clean(level, maximum=200),
                          "source": self._clean(source, maximum=800)}
                if self._redact is not FRAMES_ONLY:
                    record["message"] = self._clean(message() if callable(message) else message)
                if exc is not None:
                    record["exc"] = self._exception(exc)
                if extra:
                    if self._redact is FRAMES_ONLY:
                        record.update({"app_id": _cause_identifier(extra["app_id"]),
                                       "app_version": extra["app_version"] if type(extra["app_version"]) is str and re.fullmatch(r"v?[0-9]{1,9}\.[0-9]{1,9}\.[0-9]{1,9}", extra["app_version"]) else "<invalid>",
                                       "build_id": extra["build_id"] if type(extra["build_id"]) is str and re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", extra["build_id"]) else "<invalid>",
                                       "process_start": self._process_start})
                    else:
                        record.update({key: self._clean(value, maximum=800) for key, value in extra.items()})
                raw = summary + _serialize(record, source_identifier=self._redact is FRAMES_ONLY)
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not self._mutex.acquire(timeout=remaining):
                self._counts["dropped_lock"] += 1
                return False
            mutex_locked = True
            while True:
                try:
                    self._lock_file.seek(0)
                    msvcrt.locking(self._lock_file.fileno(), msvcrt.LK_NBLCK, 1)
                    file_locked = True
                    break
                except OSError:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        self._counts["dropped_lock"] += 1
                        return False
                    time.sleep(min(0.001, remaining))
            if not _append_locked(self, raw):
                return False
            self._reported = snapshot
            self._counts["written"] += 1 if counts_only else 1 + bool(summary)
            return True
        except _RedactionError:
            self._counts["dropped_redact"] += 1
            return False
        except Exception:
            self._counts["dropped_error"] += 1
            return False
        finally:
            try:
                if file_locked:
                    self._lock_file.seek(0)
                    msvcrt.locking(self._lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            except Exception:
                self._counts["dropped_error"] += 1
            finally:
                if mutex_locked:
                    self._mutex.release()
                _ACTIVE.recording = False

    def write(self, level: str, source: str, message: str = "", *, exc: BaseException | None = None) -> bool:
        return self._submit(level, source, message, exc)

    def stats(self) -> dict[str, int]:
        return self._counts.copy()

    def handler(self, level: int = logging.INFO) -> logging.Handler:
        try:
            return _Handler(self, level)
        except Exception:
            self._counts["dropped_error"] += 1
            return _Handler(self, logging.NOTSET)

    def install_hooks(self) -> None:
        try:
            if self._hooks_installed:
                return
            import sys
            previous_sys, previous_thread = sys.excepthook, threading.excepthook
            def system_hook(kind, error, traceback):
                self.write("ERROR", "sys.excepthook", exc=error)
                previous_sys(kind, error, traceback)
            def thread_hook(args):
                self.write("ERROR", "threading.excepthook", exc=args.exc_value)
                previous_thread(args)
            sys.excepthook, threading.excepthook = system_hook, thread_hook
            self._hooks_installed = True
            atexit.register(self._exit_counts)
        except Exception:
            self._counts["dropped_error"] += 1

    def _exit_counts(self) -> None:
        self._submit("", "", "", counts_only=True)

    def tk_callback(self) -> Callable:
        def callback(kind, error, traceback):
            self.write("ERROR", "tk.report_callback_exception", exc=error)
        return callback


class _Handler(logging.Handler):
    def __init__(self, log: DiagnosticLog, level: int):
        super().__init__(level)
        self._log = log

    def handle(self, record):
        # Handler's normal unbounded RLock would precede our bounded lock.
        try:
            if self.filter(record):
                self.emit(record)
                return True
            return False
        except Exception:
            self._log._counts["dropped_error"] += 1
            return False

    def emit(self, record):
        try:
            self._log._submit(record.levelname, record.name, record.getMessage,
                              record.exc_info[1] if record.exc_info else None)
        except Exception:
            self._log._counts["dropped_error"] += 1


def open_diagnostic_log(app_id: str, root: str | Path, *, data_roots: Sequence[str | Path],
                        redact: Callable[[str], str] | object, app_version: str, build_id: str,
                        max_bytes: int = MAX_BYTES, backups: int = 2) -> DiagnosticLog:
    log = DiagnosticLog()
    try:
        if os.name != "nt" or type(app_id) is not str or not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", app_id) or app_id in {".", ".."}:
            raise ValueError("diagnostic application identity is invalid")
        root = _literal(root)
        roots = tuple(_literal(path) for path in data_roots)
        if root.name != app_id or root.parent.name != "Diagnostics" or not roots or any(root.is_relative_to(path) for path in roots):
            raise ValueError("diagnostic root is inside data or has an invalid suffix")
        if redact is not FRAMES_ONLY and not callable(redact):
            raise ValueError("diagnostic redaction must be explicit")
        if type(max_bytes) is not int or not 1 <= max_bytes <= MAX_BYTES or type(backups) is not int or backups != 2:
            raise ValueError("diagnostic size or backup count is invalid")
        log._root, log._redact, log._max_bytes = root, redact, max_bytes
        log._session, log._process_start = uuid.uuid4().hex, _utc()
        _create_protected_root(log)
        log._counts["disabled"] = 0
        if not log._submit("INFO", "session", "", extra={"app_id": app_id, "app_version": app_version,
                            "build_id": build_id, "process_start": log._process_start}):
            log._counts["disabled"] = 1
    except Exception:
        log._counts["disabled"] = 1
        log._counts["dropped_error"] += 1
    return log

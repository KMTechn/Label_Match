"""Attempt-bound, fail-closed admission for every Label_Match writer.

The canonical installer publishes one durable active fence while it replaces
the portable tree.  Runtime writers take the stable app-owned admission mutex
for the whole mutation and either observe no fence (normal operation) or prove
an exact, short-lived delegation from the installer.  A denial is read-only.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from functools import wraps
import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import threading
import time
import weakref
from typing import Any, Callable, Iterator, Mapping, ParamSpec, TypeVar
import unicodedata


APP_ID = "label_match"
ACTIVE_SCHEMA = "label-match-all-writer-fence-active-v1"
SESSION_TUPLE_VERSION = "label-match-deployment-session-authority-v1"
SESSION_MUTEX_PREFIX = r"Local\KMTech.LabelMatch.DeploymentSession."
WRITER_MUTEX_NAME = r"Local\KMTech.LabelMatch.WriterAdmission.v1"
CONTROL_RELATIVE_PATH = (
    Path("KMTech")
    / "DirectSync"
    / APP_ID
    / "control"
    / "writer-session"
)
ACTIVE_FILENAME = "active.json"
MAX_CONTROL_BYTES = 256 * 1024
CONTROL_ROOT_OVERRIDE_ENV = "KMTECH_LABEL_WRITER_CONTROL_ROOT"
TEST_MODE_ENV = "KMTECH_LABEL_WRITER_TEST_MODE"

# The code-derived scanner normalizes this literal to zero before hashing its
# own source closure, so updating the pin is deterministic rather than cyclic.
WRITER_INVENTORY_SHA256 = "36fcd2d6083ad1cefb94b637f0564d644b6529f977b0068964280b5bad037bc7"

DELEGATION_TOKEN_ENV = "KMTECH_LABEL_WRITER_DELEGATION_TOKEN"
DELEGATION_SESSION_ENV = "KMTECH_LABEL_WRITER_DELEGATION_SESSION_ID"
DELEGATION_ATTEMPT_ENV = "KMTECH_LABEL_WRITER_DELEGATION_ATTEMPT_ID"
DELEGATION_TRANSACTION_ENV = "KMTECH_LABEL_WRITER_DELEGATION_TRANSACTION_ID"

_HEX_32 = re.compile(r"^[0-9a-f]{32}$")
_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_WRITER_LOCAL = threading.local()
_WRITER_EXIT_CONDITION = threading.Condition()
_WRITER_EXIT_CLOSED = False
_WRITER_EXIT_COUNT = 0
_LANE_WORKERS = weakref.WeakSet()
EXIT_WRITE_WAIT_S = 120
EXIT_POLL_S = 5


def start_lane_worker(worker: threading.Thread) -> None:
    """Track every lane worker, including one whose lane later becomes BROKEN."""
    with _WRITER_EXIT_CONDITION:
        if _WRITER_EXIT_CLOSED:
            raise WriterFencedError("WRITER_EXIT_CLOSED", "lane worker admission is closed for exit")
        _LANE_WORKERS.add(worker)
        worker.start()


def close_and_wait_writers(timeout_seconds: float) -> bool:
    """Close admissions; wait for writes and every lane worker's actual exit."""
    global _WRITER_EXIT_CLOSED
    deadline = time.monotonic() + max(0.0, timeout_seconds)
    with _WRITER_EXIT_CONDITION:
        _WRITER_EXIT_CLOSED = True
    # A receipt-bound delegation/maintenance session has no normal admission
    # token to drain. Reuse the existing capability validation, not env presence.
    delegated = False
    try:
        active = active_fence(canonical_control_root())
        delegated = active is not None and any(
            _delegation_matches(active, source=source, environ=os.environ)
            for source in active["delegated_sources"]
        )
    except WriterFenceError:
        pass  # Unproven delegation retains the normal exit criterion.
    with _WRITER_EXIT_CONDITION:
        while (not delegated and _WRITER_EXIT_COUNT) or any(worker.is_alive() for worker in tuple(_LANE_WORKERS)):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            _WRITER_EXIT_CONDITION.wait(min(EXIT_POLL_S, remaining))
    if delegated:
        return True
    try:
        lease = _acquire_named_mutex(
            writer_admission_mutex_name(canonical_control_root()),
            max(0.0, deadline - time.monotonic()),
        )
    except WriterFenceError:
        return False
    if lease is None:
        return False
    try:
        return not lease.abandoned
    finally:
        lease.release()


ACTIVE_FIELDS = frozenset(
    {
        "schema",
        "status",
        "app_id",
        "session_id",
        "attempt_id",
        "replacement_transaction_id",
        "session_started_at_utc",
        "orchestrator_sha256",
        "writer_contract_sha256",
        "session_authority_mutex_name",
        "writer_inventory_sha256",
        "owner_kind",
        "delegation_sha256",
        "delegated_sources",
        "delegation_expires_at_utc",
        "activated_at_utc",
        "secret_values_recorded",
    }
)


class WriterFenceError(RuntimeError):
    """The writer fence could not be observed exactly."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = str(code or "WRITER_FENCE_FAILED")


class WriterFencedError(WriterFenceError):
    """A concrete writer was denied before it could mutate state."""


def _canonical_tuple(*values: str) -> str:
    return "\n".join(unicodedata.normalize("NFC", str(value)) for value in values)


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _ascii_lower(value: str) -> str:
    return "".join(
        chr(ord(character) + 32) if "A" <= character <= "Z" else character
        for character in value
    )


def session_authority_mutex_name(
    session_id: str,
    attempt_id: str,
    orchestrator_sha256: str,
    replacement_transaction_id: str,
    writer_contract_sha256: str,
) -> str:
    canonical = _canonical_tuple(
        SESSION_TUPLE_VERSION,
        session_id,
        attempt_id,
        orchestrator_sha256,
        replacement_transaction_id,
        writer_contract_sha256,
    )
    return SESSION_MUTEX_PREFIX + _sha256_text(canonical)


def _normalized_control_root(value: str | os.PathLike[str]) -> str:
    selected = os.path.abspath(os.fspath(Path(value).expanduser()))
    selected = selected.replace("/", "\\").rstrip("\\")
    return _ascii_lower(unicodedata.normalize("NFC", selected))


def canonical_control_root(environ: Mapping[str, str] | None = None) -> Path:
    bound = getattr(_WRITER_LOCAL, "installer_authority_root", None)
    if bound is not None:
        return bound
    values = os.environ if environ is None else environ
    override = str(values.get(CONTROL_ROOT_OVERRIDE_ENV) or "").strip()
    if override:
        if str(values.get(TEST_MODE_ENV) or "") != "1":
            raise WriterFenceError(
                "CONTROL_ROOT_OVERRIDE_REJECTED",
                "writer fence control-root override is test-only",
            )
        return Path(os.path.abspath(os.fspath(Path(override).expanduser())))
    local_app_data = str(values.get("LOCALAPPDATA") or "").strip()
    if not local_app_data:
        raise WriterFenceError(
            "LOCALAPPDATA_UNAVAILABLE",
            "writer fence control root cannot be observed",
        )
    return Path(os.path.abspath(os.fspath(Path(local_app_data).expanduser()))) / CONTROL_RELATIVE_PATH


def writer_admission_mutex_name(
    control_root: str | os.PathLike[str],
    *,
    environ: Mapping[str, str] | None = None,
) -> str:
    selected = _normalized_control_root(control_root)
    if (
        str((os.environ if environ is None else environ).get(TEST_MODE_ENV) or "") == "1"
        and str((os.environ if environ is None else environ).get(CONTROL_ROOT_OVERRIDE_ENV) or "").strip()
    ):
        # Keep explicitly guarded tests on the public path-derived mutex name.
        return f"{WRITER_MUTEX_NAME}.{_sha256_text(selected)[:16]}"
    try:
        production = _normalized_control_root(canonical_control_root(environ))
    except WriterFenceError:
        production = ""
    if production and selected == production:
        return WRITER_MUTEX_NAME
    return f"{WRITER_MUTEX_NAME}.{_sha256_text(selected)[:16]}"


def _assert_no_reparse_components(path: Path) -> None:
    selected = Path(os.path.abspath(os.fspath(path.expanduser())))
    current = Path(selected.anchor)
    for part in selected.parts[1:]:
        current /= part
        try:
            stat = os.lstat(current)
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise WriterFenceError(
                "FENCE_PATH_UNOBSERVABLE",
                "writer fence path cannot be observed",
            ) from exc
        attributes = int(getattr(stat, "st_file_attributes", 0))
        if current.is_symlink() or bool(attributes & 0x400):
            raise WriterFenceError(
                "FENCE_REPARSE_REJECTED",
                "writer fence path contains a reparse point",
            )


def _parse_utc(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _bounded_json(path: Path) -> dict[str, Any]:
    try:
        before = path.stat()
        if before.st_size <= 0 or before.st_size > MAX_CONTROL_BYTES:
            raise WriterFenceError("FENCE_SIZE_INVALID", "writer fence size is invalid")
        raw = path.read_bytes()
        after = path.stat()
    except WriterFenceError:
        raise
    except OSError as exc:
        raise WriterFenceError(
            "FENCE_STATE_UNREADABLE",
            "writer fence state is unreadable",
        ) from exc
    if (
        before.st_size != after.st_size
        or before.st_mtime_ns != after.st_mtime_ns
        or len(raw) != before.st_size
    ):
        raise WriterFenceError(
            "FENCE_STATE_CHANGED",
            "writer fence changed while it was read",
        )
    try:
        value = json.loads(raw.decode("utf-8-sig"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise WriterFenceError("FENCE_JSON_INVALID", "writer fence JSON is invalid") from exc
    if not isinstance(value, dict):
        raise WriterFenceError("FENCE_SHAPE_INVALID", "writer fence must be one object")
    return value


def _validate_active_fence(payload: Mapping[str, Any]) -> dict[str, Any]:
    value = dict(payload)
    delegated_sources = value.get("delegated_sources")
    if set(value) != ACTIVE_FIELDS:
        raise WriterFenceError("FENCE_SHAPE_INVALID", "writer fence fields differ")
    if (
        value.get("schema") != ACTIVE_SCHEMA
        or value.get("app_id") != APP_ID
        or value.get("status") not in {"QUIESCING", "INSTALLING", "RESTORING", "RESTORE_FAILED"}
        or value.get("owner_kind") != "canonical_installer"
        or not _HEX_32.fullmatch(str(value.get("session_id") or ""))
        or not _HEX_32.fullmatch(str(value.get("attempt_id") or ""))
        or not _HEX_32.fullmatch(str(value.get("replacement_transaction_id") or ""))
        or not _HEX_64.fullmatch(str(value.get("orchestrator_sha256") or ""))
        or not _HEX_64.fullmatch(str(value.get("writer_contract_sha256") or ""))
        or value.get("writer_inventory_sha256") != WRITER_INVENTORY_SHA256
        or _parse_utc(value.get("session_started_at_utc")) is None
        or _parse_utc(value.get("activated_at_utc")) is None
        or value.get("secret_values_recorded") is not False
        or not isinstance(delegated_sources, list)
        or any(not isinstance(item, str) or not item for item in delegated_sources)
        or delegated_sources != sorted(set(delegated_sources))
    ):
        raise WriterFenceError("FENCE_BINDING_INVALID", "writer fence binding differs")
    expected_name = session_authority_mutex_name(
        value["session_id"],
        value["attempt_id"],
        value["orchestrator_sha256"],
        value["replacement_transaction_id"],
        value["writer_contract_sha256"],
    )
    if value.get("session_authority_mutex_name") != expected_name:
        raise WriterFenceError(
            "FENCE_AUTHORITY_NAME_INVALID",
            "writer fence authority mutex name differs",
        )
    delegation_sha = value.get("delegation_sha256")
    delegation_expiry = value.get("delegation_expires_at_utc")
    if delegated_sources:
        if (
            not _HEX_64.fullmatch(str(delegation_sha or ""))
            or _parse_utc(delegation_expiry) is None
        ):
            raise WriterFenceError(
                "FENCE_DELEGATION_INVALID",
                "writer fence delegation binding differs",
            )
    elif delegation_sha != "" or delegation_expiry != "":
        raise WriterFenceError(
            "FENCE_DELEGATION_INVALID",
            "empty delegation binding differs",
        )
    return value


def active_fence(
    control_root: str | os.PathLike[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> dict[str, Any] | None:
    root = (
        Path(os.path.abspath(os.fspath(Path(control_root).expanduser())))
        if control_root is not None
        else canonical_control_root(environ)
    )
    _assert_no_reparse_components(root)
    path = root / ACTIVE_FILENAME
    _assert_no_reparse_components(path)
    try:
        present = path.exists()
    except OSError as exc:
        raise WriterFenceError(
            "FENCE_STATE_UNOBSERVABLE",
            "writer fence presence cannot be observed",
        ) from exc
    if not present:
        return None
    return _validate_active_fence(_bounded_json(path))


class _NamedMutexLease:
    def __init__(self, handle: int, *, abandoned: bool = False) -> None:
        self.handle = handle
        self.abandoned = abandoned
        self.owned = bool(handle)

    def suspend(self) -> None:
        """Release ownership without closing the named-mutex handle."""

        if not self.handle or not self.owned:
            return
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        if not kernel32.ReleaseMutex(ctypes.c_void_p(self.handle)):
            raise WriterFenceError(
                "WRITER_MUTEX_RELEASE_FAILED",
                "writer admission mutex ownership could not be released",
            )
        self.owned = False

    def resume(self, timeout_seconds: float) -> None:
        """Reacquire a suspended named-mutex handle on the same thread."""

        if not self.handle or self.owned:
            return
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_ulong]
        kernel32.WaitForSingleObject.restype = ctypes.c_ulong
        milliseconds = max(0, min(0xFFFFFFFE, int(float(timeout_seconds) * 1000)))
        result = int(
            kernel32.WaitForSingleObject(ctypes.c_void_p(self.handle), milliseconds)
        )
        if result in {0, 0x80}:
            self.owned = True
            self.abandoned = result == 0x80
            return
        if result == 0x102:
            raise WriterFencedError(
                "WRITER_GATE_TIMEOUT",
                "writer admission mutex is held by a deployment operation",
            )
        raise WriterFenceError(
            "WRITER_MUTEX_WAIT_FAILED", "writer mutex wait failed"
        )

    def release(self) -> None:
        handle, self.handle = self.handle, 0
        if not handle:
            return
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        try:
            if self.owned:
                kernel32.ReleaseMutex(ctypes.c_void_p(handle))
        finally:
            self.owned = False
            kernel32.CloseHandle(ctypes.c_void_p(handle))


def _acquire_named_mutex(name: str, timeout_seconds: float) -> _NamedMutexLease | None:
    if os.name != "nt":
        return _NamedMutexLease(0)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateMutexExW.argtypes = [
        ctypes.c_void_p, ctypes.c_wchar_p, ctypes.c_ulong, ctypes.c_ulong,
    ]
    kernel32.CreateMutexExW.restype = ctypes.c_void_p
    kernel32.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_ulong]
    kernel32.WaitForSingleObject.restype = ctypes.c_ulong
    # SYNCHRONIZE permits waiting; MUTEX_MODIFY_STATE permits ReleaseMutex.
    # Keep default security when an elevated same-user process holds the mutex.
    handle = kernel32.CreateMutexExW(None, name, 0, 0x00100001)
    if not handle:
        raise WriterFenceError(
            "WRITER_MUTEX_CREATE_FAILED",
            "writer admission mutex could not be opened",
        )
    milliseconds = max(0, min(0xFFFFFFFE, int(float(timeout_seconds) * 1000)))
    result = int(kernel32.WaitForSingleObject(handle, milliseconds))
    if result == 0:
        return _NamedMutexLease(int(handle))
    if result == 0x80:
        return _NamedMutexLease(int(handle), abandoned=True)
    kernel32.CloseHandle(handle)
    if result == 0x102:
        return None
    raise WriterFenceError("WRITER_MUTEX_WAIT_FAILED", "writer mutex wait failed")


def _named_mutex_held_by_other(name: str) -> bool:
    if os.name != "nt":
        return True
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.OpenMutexW.argtypes = [ctypes.c_ulong, ctypes.c_int, ctypes.c_wchar_p]
    kernel32.OpenMutexW.restype = ctypes.c_void_p
    kernel32.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_ulong]
    kernel32.WaitForSingleObject.restype = ctypes.c_ulong
    handle = kernel32.OpenMutexW(0x00100001, False, name)
    if not handle:
        return False
    try:
        result = int(kernel32.WaitForSingleObject(handle, 0))
        if result == 0x102:
            return True
        if result in {0, 0x80}:
            kernel32.ReleaseMutex(handle)
        return False
    finally:
        kernel32.CloseHandle(handle)


def _delegation_matches(
    active: Mapping[str, Any],
    *,
    source: str,
    environ: Mapping[str, str],
) -> bool:
    if source not in active["delegated_sources"]:
        return False
    expires = _parse_utc(active["delegation_expires_at_utc"])
    token = str(environ.get(DELEGATION_TOKEN_ENV) or "")
    return bool(
        len(token) >= 32
        and expires is not None
        and datetime.now(timezone.utc) <= expires
        and environ.get(DELEGATION_SESSION_ENV) == active["session_id"]
        and environ.get(DELEGATION_ATTEMPT_ENV) == active["attempt_id"]
        and environ.get(DELEGATION_TRANSACTION_ENV)
        == active["replacement_transaction_id"]
        and _named_mutex_held_by_other(active["session_authority_mutex_name"])
        and secrets.compare_digest(_sha256_text(token), active["delegation_sha256"])
    )


_INSTALLER_OWNER_SCHEMA = "label-match-installer-owner-v1"
_INSTALLER_TUPLE_FIELDS = (
    "session_id", "attempt_id", "replacement_transaction_id", "writer_inventory_sha256",
    "orchestrator_sha256", "writer_contract_sha256", "session_started_at_utc",
)
_FAILED_INSTALL_STATUSES = frozenset({
    "ROLLBACK_FAILED", "FAILED_ROLLBACK_INCOMPLETE", "FAILED_ROLLED_BACK",
    "FAILED_SUPPORTED_REMOVAL_REQUIRED",
})
# An installer killed mid-run (power loss, reboot, closed window) records no
# failure: its audit keeps the status it had while running and its fence stays.
# The installer itself writes PREIMAGE_SAVED; RUNNING is the generic value.
_UNFINISHED_INSTALL_STATUSES = frozenset({"RUNNING", "PREIMAGE_SAVED"})
# Only forward-run fence states qualify; a rollback in progress (RESTORING,
# RESTORE_FAILED) keeps its fence for the supported rollback resume.
_UNFINISHED_FENCE_STATUSES = frozenset({"QUIESCING", "INSTALLING"})
# A run that saved its recovery context before its first change may have changed
# code, so it is settled only by the supported rollback resume, which also takes
# a dead run killed while it was restoring.
_RESUMABLE_FENCE_STATUSES = frozenset({"QUIESCING", "INSTALLING", "RESTORING"})
_PRIVILEGED_OWNER_SIDS = frozenset({"S-1-5-18", "S-1-5-32-544",
    "S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464"})


def _installer_owner_root() -> Path:
    # The existing protected code parent survives failed placement/rollback.
    # Neither LOCALAPPDATA nor a caller-supplied path supplies this authority.
    return Path(r"C:\KMTech\Apps\Label_Match")


def _installer_program_data_root() -> Path:
    from ctypes import wintypes as w
    shell = ctypes.WinDLL("shell32", use_last_error=True)
    shell.SHGetFolderPathW.argtypes = [w.HWND, ctypes.c_int, w.HANDLE, w.DWORD, w.LPWSTR]
    shell.SHGetFolderPathW.restype = ctypes.c_long
    buffer = ctypes.create_unicode_buffer(32768)
    if shell.SHGetFolderPathW(None, 0x23, None, 0, buffer) != 0:
        raise OSError("Machine ProgramData cannot be observed")
    path = Path(buffer.value)
    if not path.is_absolute():
        raise OSError("Machine ProgramData is not absolute")
    return path.absolute()


def _assert_protected_owner_path(path: Path, *, machine_fence_root: Path | None = None) -> None:
    """Observe held native objects under the existing installer trust rule."""
    from ctypes import wintypes as w

    class Acl(ctypes.Structure):
        _fields_ = [("revision", w.BYTE), ("reserved", w.BYTE),
                    ("size", w.WORD), ("count", w.WORD), ("reserved2", w.WORD)]

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel.CreateFileW.argtypes = [w.LPCWSTR, w.DWORD, w.DWORD, w.LPVOID, w.DWORD, w.DWORD, w.HANDLE]
    kernel.CreateFileW.restype = w.HANDLE
    kernel.CloseHandle.argtypes = [w.HANDLE]
    kernel.LocalFree.argtypes = [w.HLOCAL]
    advapi.GetSecurityInfo.argtypes = [w.HANDLE, w.DWORD, w.DWORD, ctypes.POINTER(w.LPVOID),
        w.LPVOID, ctypes.POINTER(w.LPVOID), w.LPVOID, ctypes.POINTER(w.LPVOID)]
    advapi.GetSecurityInfo.restype = w.DWORD
    advapi.ConvertSidToStringSidW.argtypes = [w.LPVOID, ctypes.POINTER(w.LPWSTR)]
    advapi.GetAce.argtypes = [w.LPVOID, w.DWORD, ctypes.POINTER(w.LPVOID)]
    kernel.GetFileInformationByHandleEx.argtypes = [w.HANDLE, ctypes.c_int, w.LPVOID, w.DWORD]
    handles = []
    boundary = program_data = None
    if machine_fence_root is not None:
        boundary = Path(machine_fence_root).absolute()
        program_data = _installer_program_data_root()
        expected_parent = program_data / "KMTech" / "Label_Match" / "install-audit"
        if (boundary.parent != expected_parent
                or re.fullmatch(r"machine-writer-[0-9]{8}T[0-9]{9}Z-[0-9a-f]{32}", boundary.name) is None
                or boundary not in path.absolute().parents):
            raise OSError("Machine installer fence path is outside its authority")
    advapi.GetSecurityDescriptorControl.argtypes = [w.LPVOID, ctypes.POINTER(w.WORD), ctypes.POINTER(w.DWORD)]
    advapi.GetSecurityDescriptorControl.restype = w.BOOL

    def sid_text(sid):
        text = w.LPWSTR()
        try:
            if not advapi.ConvertSidToStringSidW(sid, ctypes.byref(text)):
                raise OSError("Installer owner SID cannot be observed")
            return text.value
        finally:
            if text:
                kernel.LocalFree(text)

    try:
        selected = path.absolute()
        for component in [*reversed(selected.parents), selected]:
            leaf = component == selected
            # Hold ancestors and leaf against rename/reparse/content replacement.
            handle = kernel.CreateFileW(str(component), 0x20080, 1, None, 3, 0x02200000, None)
            if handle in (None, ctypes.c_void_p(-1).value):
                raise OSError("Protected installer record cannot be held")
            handles.append(handle)
            attributes = (w.DWORD * 2)()
            if not kernel.GetFileInformationByHandleEx(handle, 9, attributes, ctypes.sizeof(attributes)):
                raise OSError("Protected installer record type cannot be observed")
            if attributes[0] & 0x400 or bool(attributes[0] & 0x10) == leaf:
                raise OSError("Protected installer record is redirected or has wrong type")
            owner, dacl, descriptor = w.LPVOID(), w.LPVOID(), w.LPVOID()
            try:
                if advapi.GetSecurityInfo(handle, 1, 5, ctypes.byref(owner), None,
                    ctypes.byref(dacl), None, ctypes.byref(descriptor)):
                    raise OSError("Protected installer record ACL cannot be observed")
                mutation_mask = 0x500D0156 if leaf else 0x500D0150
                if boundary is not None:
                    if component == program_data or component in program_data.parents:
                        mutation_mask = 0  # OS-managed base: privileged owner only.
                    elif component == boundary or boundary in component.parents:
                        mutation_mask = 0x500D0156 if leaf or component == boundary else 0x500D0150
                    else:
                        mutation_mask = 0x500D0040  # Shared ancestor rename/ACL/owner threats.
                if sid_text(owner) not in _PRIVILEGED_OWNER_SIDS or (mutation_mask and not dacl):
                    raise OSError("Protected installer record has untrusted owner or null DACL")
                if component == boundary:
                    control, revision = w.WORD(), w.DWORD()
                    if (not advapi.GetSecurityDescriptorControl(descriptor, ctypes.byref(control), ctypes.byref(revision))
                            or not control.value & 0x1000):
                        raise OSError("Machine installer fence DACL is not protected")
                count = ctypes.cast(dacl, ctypes.POINTER(Acl)).contents.count if mutation_mask else 0
                for index in range(count):
                    ace = w.LPVOID()
                    if not advapi.GetAce(dacl, index, ctypes.byref(ace)):
                        raise OSError("Protected installer ACE cannot be observed")
                    header = (w.BYTE * 4).from_address(ace.value)
                    if header[0] == 1:  # An access-denied ACE grants no mutation.
                        continue
                    if header[0] != 0:
                        raise OSError("Protected installer record has unsupported ACE")
                    if header[1] & 8 and not leaf:  # INHERIT_ONLY, existing ancestor rule
                        continue
                    mask = w.DWORD.from_address(ace.value + 4).value
                    if sid_text(ace.value + 8) not in {"S-1-5-18", "S-1-5-32-544"} and mask & mutation_mask:
                        raise OSError("Protected installer record is user mutable")
            finally:
                if descriptor:
                    kernel.LocalFree(descriptor)
        if selected.stat().st_nlink != 1:
            raise OSError("Protected installer record has multiple links")
    finally:
        for handle in reversed(handles):
            kernel.CloseHandle(handle)


def _installer_process_exited(pid: int, started: str) -> bool:
    from ctypes import wintypes as w
    if type(pid) is not int or not 0 < pid <= 0xFFFFFFFF or not isinstance(started, str) or not re.fullmatch(r"[1-9][0-9]{0,18}", started):
        return False
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.argtypes = [w.DWORD, w.BOOL, w.DWORD]
    kernel.OpenProcess.restype = w.HANDLE
    kernel.WaitForSingleObject.argtypes = [w.HANDLE, w.DWORD]
    kernel.WaitForSingleObject.restype = w.DWORD
    kernel.GetProcessTimes.argtypes = [w.HANDLE, *([ctypes.POINTER(w.FILETIME)] * 4)]
    kernel.CloseHandle.argtypes = [w.HANDLE]
    handle = kernel.OpenProcess(0x00101000, False, pid)  # SYNCHRONIZE + QUERY_LIMITED_INFORMATION
    if not handle:
        return ctypes.get_last_error() == 87  # Only a definitely absent PID.
    try:
        created, exited, system, user = (w.FILETIME() for _ in range(4))
        if not kernel.GetProcessTimes(handle, ctypes.byref(created), ctypes.byref(exited), ctypes.byref(system), ctypes.byref(user)):
            return False
        creation = (created.dwHighDateTime << 32) | created.dwLowDateTime
        return str(creation) == started and int(kernel.WaitForSingleObject(handle, 0)) == 0
    finally:
        kernel.CloseHandle(handle)


def _installer_process_live(pid: int, started: str) -> bool:
    """Positive native identity; absence, PID reuse and unknown all refuse."""
    from ctypes import wintypes as w
    if type(pid) is not int or not 0 < pid <= 0xFFFFFFFF or not isinstance(started, str) or not re.fullmatch(r"[1-9][0-9]{0,18}", started):
        return False
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.argtypes = [w.DWORD, w.BOOL, w.DWORD]
    kernel.OpenProcess.restype = w.HANDLE
    kernel.WaitForSingleObject.argtypes = [w.HANDLE, w.DWORD]
    kernel.GetProcessTimes.argtypes = [w.HANDLE, *([ctypes.POINTER(w.FILETIME)] * 4)]
    kernel.CloseHandle.argtypes = [w.HANDLE]
    handle = kernel.OpenProcess(0x00101000, False, pid)
    if not handle:
        return False
    try:
        created, exited, system, user = (w.FILETIME() for _ in range(4))
        if not kernel.GetProcessTimes(handle, ctypes.byref(created), ctypes.byref(exited), ctypes.byref(system), ctypes.byref(user)):
            return False
        birth = (created.dwHighDateTime << 32) | created.dwLowDateTime
        return str(birth) == started and int(kernel.WaitForSingleObject(handle, 0)) == 0x102
    finally:
        kernel.CloseHandle(handle)


def _validate_installer_authority(root: Path, owner_path: Path, install_root: Path, source: str):
    """Use the protected owner record, never a production environment override."""
    _assert_no_reparse_components(root)
    _assert_no_reparse_components(owner_path)
    _assert_protected_owner_path(owner_path)
    _assert_protected_owner_path(root / ACTIVE_FILENAME, machine_fence_root=root)
    owner = _bounded_json(owner_path)
    active = active_fence(root)
    run = owner.get("run_id")
    if (owner_path.parent != _installer_owner_root().absolute()
            or owner.get("schema") != _INSTALLER_OWNER_SCHEMA
            or not isinstance(run, str) or re.fullmatch(r"[0-9]{8}T[0-9]{9}Z-[0-9a-f]{32}", run) is None
            or active is None
            or not isinstance(owner.get("control_root"), str) or not Path(owner["control_root"]).is_absolute()
            or not isinstance(owner.get("install_root"), str) or not Path(owner["install_root"]).is_absolute()
            or owner_path.name != ".installer-owner-" + active["session_id"] + "-" + run.rsplit("-", 1)[-1] + ".json"
            or any(owner.get(name) != active[name] for name in _INSTALLER_TUPLE_FIELDS)
            or _normalized_control_root(owner.get("control_root", "")) != _normalized_control_root(root)
            or _normalized_control_root(owner.get("install_root", "")) != _normalized_control_root(install_root / "current")
            or not _installer_process_live(owner.get("owner_process_id"), owner.get("owner_process_started_at_filetime"))
            or not _delegation_matches(active, source=source, environ=os.environ)):
        raise WriterFenceError("INSTALLER_AUTHORITY_INVALID", "installer lifecycle authority is unproven")
    return owner, active


@contextmanager
def installer_lifecycle_authority(root, owner_record, install_root, *, source):
    root, owner_path, install_root = (Path(value).absolute() for value in (root, owner_record, install_root))
    before = _validate_installer_authority(root, owner_path, install_root, source)
    previous = getattr(_WRITER_LOCAL, "installer_authority_root", None)
    failure = None
    _WRITER_LOCAL.installer_authority_root = root
    try:
        yield
    except BaseException as error:
        failure = error
        raise
    finally:
        try:
            if _validate_installer_authority(root, owner_path, install_root, source) != before:
                raise WriterFenceError("INSTALLER_AUTHORITY_CHANGED", "installer authority changed during lifecycle")
        except BaseException:
            if failure is None:
                raise
            failure.add_note("INSTALLER_AUTHORITY_RECHECK_FAILED")
        finally:
            if previous is None:
                del _WRITER_LOCAL.installer_authority_root
            else:
                _WRITER_LOCAL.installer_authority_root = previous


def _unfinished_install_run(
    audit: Mapping[str, Any], active: Mapping[str, Any], *, for_resume: bool = False
) -> bool:
    # The machine (worker kit) path has no user-path rollback to resume: a run
    # killed while it restored is settled like one killed while it copied code.
    fence_states = (_RESUMABLE_FENCE_STATUSES
                    if for_resume or _machine_recovery_context(audit) else _UNFINISHED_FENCE_STATUSES)
    return (audit.get("status") in _UNFINISHED_INSTALL_STATUSES
            and active.get("status") in fence_states)


_MACHINE_CONTEXT_KIND = "MACHINE_WORKER_DEFERRED"


def _machine_recovery_context(audit: Mapping[str, Any]) -> bool:
    """The machine path saves only a kind marker (no secrets, no restore inputs)."""
    context = audit.get("recovery_context")
    return isinstance(context, dict) and context.get("kind") == _MACHINE_CONTEXT_KIND


def _saved_recovery_context(audit: Mapping[str, Any]) -> bool:
    # A machine marker is not a resumable context: the supported rollback resume
    # (user path) cannot restore a machine placement, so it never answers for it.
    context = audit.get("recovery_context")
    return isinstance(context, dict) and bool(context) and not _machine_recovery_context(audit)


def _installer_machine_idle(trusted: Mapping[str, Any], environ: Mapping[str, str]) -> bool:
    """Read-only proof that no product writer process and no relay is running.

    A recorded failure already ended both before the installer reported it. An
    installer killed mid-run proves neither, so they are observed with the update
    helper's process snapshot and the relay instance lease. Anything that cannot
    be observed is not idle.
    """
    try:
        from auto_update import control_root, process_snapshot, writer_processes
        from current_user_onboarding import resolve_current_user_onboarding_paths
        from user_relay import _acquire_relay_lease

        app_root = Path(str(trusted["install_root"]))
        control = control_root(app_root / "app")
        if control is None or writer_processes(process_snapshot(control), own_pid=os.getpid()):
            return False
        relay = _acquire_relay_lease(
            resolve_current_user_onboarding_paths(app_root, environ=environ).direct_sync_root
            / "user-relay-instance")
        if relay is None:
            return False
        relay.close()
        return True
    except Exception:
        return False


def _same_file(left: Path, right: str) -> bool:
    try:
        return os.path.normcase(os.path.realpath(left)) == os.path.normcase(os.path.realpath(right))
    except OSError:
        return False


def prove_dead_unfinished_installer(
    audit_path: str, environ: Mapping[str, str] | None = None
) -> bool:
    """Read-only death proof for the supported rollback resume (G99b).

    True only for the exact protected owner record, an ended owner process whose
    start time matches, a free session-authority mutex, a forward or restoring
    fence, an unfinished audit that holds a saved recovery context, and a machine
    with no product writer process and no relay. Nothing is changed.
    """
    values = os.environ if environ is None else environ
    try:
        root = canonical_control_root(values)
        active = active_fence(root, environ=values)
        if active is None:
            return False
        return _failed_installer_removal_sources(
            root, active, values, resume_proof_for=str(audit_path)) is not None
    except (OSError, WriterFenceError, ValueError, TypeError, KeyError):
        return False


def settle_dead_installer_fence() -> dict[str, str] | None:
    """Retire the exact fence of a dead, unfinished installer after user removal.

    Only the admission that proved the death (it left a thread-local record) can
    settle, and only the unchanged fence bytes: they are copied to
    settled/<sha256>.json, read back, and then the active file is removed so the
    official reinstall can start. A recorded failure never reaches this; its
    fence stays for the supported rollback resume.
    """
    scope = getattr(_WRITER_LOCAL, "dead_installer_settlement", None)
    if scope is None or getattr(_WRITER_LOCAL, "failed_removal_authority", None) is None:
        return None
    root, active, original, environ = scope
    path = root / ACTIVE_FILENAME
    if active_fence(root, environ=environ) != active or path.read_bytes() != original:
        raise ValueError("writer fence changed before removal settlement")
    digest = hashlib.sha256(original).hexdigest()
    copy = root / "settled" / (digest + ".json")
    _assert_no_reparse_components(copy)
    copy.parent.mkdir(parents=True, exist_ok=True)
    _assert_no_reparse_components(copy)
    if not copy.exists():
        with copy.open("xb") as handle:
            handle.write(original)
            handle.flush()
            os.fsync(handle.fileno())
    _assert_no_reparse_components(copy)
    if (copy.read_bytes() != original or path.read_bytes() != original
            or active_fence(root, environ=environ) != active):
        raise ValueError("writer fence copy readback differs")
    path.unlink()
    if path.exists():
        raise ValueError("writer fence absence was not proven")
    _WRITER_LOCAL.dead_installer_settlement = None
    return {
        "session_id_sha256": _sha256_text(active["session_id"]),
        "attempt_id_sha256": _sha256_text(active["attempt_id"]),
        "fence_copy_sha256": digest,
        "fence_copy_path": root.name + "/settled/" + digest + ".json",
    }


def _failed_installer_removal_sources(
    root: Path, active: Mapping[str, Any], environ: Mapping[str, str],
    *, resume_proof_for: str | None = None,
) -> frozenset[str] | None:
    """Only a protected, exact dead installer owner permits user cleanup.

    With ``resume_proof_for`` (the audit path) nothing is admitted or settled:
    the same death proof is only answered for the supported rollback resume,
    and only for an unfinished run that saved its recovery context.
    """
    for_resume = resume_proof_for is not None
    _WRITER_LOCAL.resume_rollback_required = False
    local = str(environ.get("LOCALAPPDATA") or "").strip()
    if os.name != "nt" or not local:
        return None
    audit_root = Path(local) / "KMTech" / "Label_Match" / "install-audit"
    claim = _acquire_named_mutex(str(active["session_authority_mutex_name"]), 0)
    if claim is None:
        return None
    try:
        protected_root = _installer_owner_root()
        prefix = '.installer-owner-' + active['session_id']
        records = list(protected_root.glob('.installer-owner-*.json'))
        if len(records) > 128:
            return None
        matching = []
        for path in records:
            record = _bounded_json(path)
            if all(record.get(key) == active[key] for key in _INSTALLER_TUPLE_FIELDS):
                matching.append((path, record))
        if len(matching) != 1:
            return None
        trusted_path, trusted = matching[0]
        _assert_protected_owner_path(trusted_path)
        run_id = trusted.get('run_id')
        if (trusted.get('schema') != _INSTALLER_OWNER_SCHEMA
            or not isinstance(run_id, str) or not re.fullmatch(r"[0-9]{8}T[0-9]{9}Z-[0-9a-f]{32}", run_id)
            or trusted_path.name != prefix + '-' + run_id.rsplit('-', 1)[-1] + '.json'
            or any(trusted.get(key) != active[key] for key in _INSTALLER_TUPLE_FIELDS)
            or _normalized_control_root(str(trusted.get('control_root') or '')) != _normalized_control_root(root)
            or _normalized_control_root(str(trusted.get('install_root') or '')) != _normalized_control_root(r"C:\KMTech\Apps\Label_Match\current")):
            return None
        _assert_no_reparse_components(audit_root)
        files = list(audit_root.glob("canonical-portable-*.json"))
        counted_files = 0
        matches = []
        for path in files:
            try:
                _assert_no_reparse_components(path)
                audit = _bounded_json(path)
            except WriterFenceError:
                if path.name == 'canonical-portable-' + run_id + '.json':
                    return None
                counted_files += 1
                if counted_files > 128:
                    return None
                continue
            # Retain earlier, transaction-free installer evidence in place.
            # It cannot claim this fence and does not consume the current
            # failed installation's cleanup allowance.
            pretransaction = (
                path.name != 'canonical-portable-' + run_id + '.json'
                and audit.get('run_id') != run_id
                and audit.get('schema') == 'label-match-canonical-portable-install-v1'
                and 'writer_fence' not in audit
                and not audit.get('current_user_setup_started')
                and (
                    (audit.get('status') == 'FAILED'
                     and audit.get('preparation_status') == 'FAILED_PREPARATION_ADMISSION'
                     and audit.get('code_placement') == 'NOT_STARTED'
                     and isinstance(audit.get('rollback'), dict)
                     and audit['rollback'].get('applied') is False)
                    or (audit.get('status') in {'PREIMAGE_SAVED', 'FAILED_PREPARATION_ADMISSION', 'FAILED_PRETRANSACTION'}
                        and audit.get('code_placement') == 'PENDING'
                        and audit.get('current_user_setup_started') is False
                        and audit.get('current_user_registration_mode') == 'FIRST_INSTALL')
                )
            )
            if not pretransaction:
                counted_files += 1
                if counted_files > 128:
                    return None
            owner = audit.get("writer_fence")
            if ("writer_fence" not in audit and path.name == 'canonical-portable-' + run_id + '.json'
                    and audit.get('run_id') == run_id):
                # G99c: a run killed after it made the fence but before its audit
                # recorded the tuple carries none. The protected owner record
                # (already equal to this fence) names the same run, so it stands
                # in for the tuple; every other check below is unchanged. An audit
                # of another run or with a different/garbled tuple is not adopted.
                owner = {key: trusted.get(key) for key in (*_INSTALLER_TUPLE_FIELDS,
                         'owner_process_id', 'owner_process_started_at_filetime')}
            if not isinstance(owner, dict) or any(owner.get(key) != active[key] for key in _INSTALLER_TUPLE_FIELDS):
                continue
            # Every record claiming this identity must agree; never discard a
            # live, unknown, differently named/statused or contradictory record.
            if (any(owner.get(key) != trusted.get(key) for key in (*_INSTALLER_TUPLE_FIELDS,
                    'owner_process_id', 'owner_process_started_at_filetime'))
                or audit.get('run_id') != run_id or path.name != 'canonical-portable-' + run_id + '.json'
                or audit.get('schema') != 'label-match-canonical-portable-install-v1'
                or (audit.get('status') not in _FAILED_INSTALL_STATUSES
                    and not _unfinished_install_run(audit, active, for_resume=for_resume))
                or (for_resume and audit.get('status') in _FAILED_INSTALL_STATUSES)
                or _normalized_control_root(str(audit.get('install_root') or '')) != _normalized_control_root(trusted['install_root'])):
                return None
            matches.append((path, audit))
        if len(matches) != 1 or not _installer_process_exited(trusted.get('owner_process_id'), trusted.get('owner_process_started_at_filetime')):
            return None
        # A run that recorded no failure also needs a proven-idle machine.
        unfinished = matches[0][1].get('status') not in _FAILED_INSTALL_STATUSES
        has_context = _saved_recovery_context(matches[0][1])
        if for_resume and (not has_context or not _same_file(matches[0][0], resume_proof_for)):
            return None
        if unfinished and has_context and not for_resume:
            # Code may already be changed: only the supported resume restores it.
            _WRITER_LOCAL.resume_rollback_required = True
            return None
        if unfinished and not _installer_machine_idle(trusted, environ):
            return None
        original = (root / ACTIVE_FILENAME).read_bytes() if unfinished else b''
        _assert_protected_owner_path(trusted_path)
        if (_bounded_json(trusted_path) != trusted or _bounded_json(matches[0][0]) != matches[0][1]
            or set(audit_root.glob('canonical-portable-*.json')) != set(files)
            or set(protected_root.glob('.installer-owner-*.json')) != set(records)
            or active_fence(root, environ=environ) != active):
            return None
        if for_resume:
            return frozenset()
        _WRITER_LOCAL.failed_removal_authority = claim
        claim = None
        if unfinished:
            _WRITER_LOCAL.dead_installer_settlement = (root, dict(active), original, environ)
        return frozenset({"current_user_setup_removal", "scheduled_task_remove",
                          "user_relay_autostart_remove", "user_relay_stop_request"})
    except (OSError, WriterFenceError, ValueError, TypeError):
        return None
    finally:
        if claim is not None:
            claim.release()


def _admitted_writer_sources(
    root: Path,
    *,
    source: str,
    environ: Mapping[str, str],
) -> frozenset[str] | None:
    try:
        active = active_fence(root, environ=environ)
    except WriterFenceError as exc:
        raise WriterFencedError(
            exc.code,
            "writer denied because the active fence is not exact",
        ) from exc
    if active is not None and not _delegation_matches(
        active,
        source=source,
        environ=environ,
    ):
        if source == "current_user_setup_removal":
            cleanup_sources = _failed_installer_removal_sources(root, active, environ)
            if cleanup_sources is not None:
                return cleanup_sources
            if getattr(_WRITER_LOCAL, "resume_rollback_required", False):
                _WRITER_LOCAL.resume_rollback_required = False
                raise WriterFencedError(
                    "RESUME_ROLLBACK_REQUIRED",
                    "the interrupted installation may have changed code; run the supported "
                    "rollback resume (INSTALL_CANONICAL_PORTABLE.ps1 -ResumeRollbackAuditPath "
                    "<audit> -ExpectedRollbackAuditSha256 <sha256>) first",
                )
        raise WriterFencedError(
            "ACTIVE_WRITER_FENCE",
            "writer denied by the active deployment fence",
        )
    return (
        frozenset(active["delegated_sources"]) if active is not None else None
    )


def _lifecycle_digests(control: Path | None) -> dict[Path, bytes | None]:
    observed = {}
    if control is not None:
        for name in ("policy.json", "active.json", "transition.json"):
            path = control / name
            if path.is_file():
                if name == "transition.json":
                    from auto_update import read_json
                    observed[path] = hashlib.sha256(json.dumps(read_json(path), sort_keys=True).encode("utf-8")).digest()
                else:
                    with path.open("rb") as handle:
                        observed[path] = hashlib.file_digest(handle, "sha256").digest()
            else:
                observed[path] = None
    return observed


@contextmanager
def _onboarding_lifecycle_scope(app_root: Any, *, recheck: bool = False) -> Iterator[None]:
    """Validate once outside admission, only for this thread's current call."""
    from auto_update import assert_version_writer, control_root, installation_root, resolve_lifecycle_context

    previous = getattr(_WRITER_LOCAL, "onboarding_lifecycle", None)
    _WRITER_LOCAL.onboarding_lifecycle = None
    try:
        values = os.environ
        root = canonical_control_root(values)
        source_root = Path(__file__).resolve().parent
        selected_root = Path(app_root).absolute()
        managed = installation_root(selected_root)
        source_control = control_root(source_root)
        control = managed / "update-control" if managed is not None and (managed / "update-control").exists() else None
        if source_control is not None and control is not None and source_control != control:
            raise WriterFencedError("WRITER_LIFECYCLE_CHANGED", "onboarding and writer installation roots differ")
        source_managed = installation_root(source_root)
        observed_control = control if control is not None else source_control
        if observed_control is None:
            prospective_root = managed if managed is not None else source_managed
            if prospective_root is not None:
                observed_control = prospective_root / "update-control"
        delegation = active_fence(root, environ=values)
        _admitted_writer_sources(root, source="current_user_onboarding", environ=values)
        observed = _lifecycle_digests(observed_control)
        context = None
        if control is not None:
            if recheck:
                from auto_update import load_policy, update_lock
                with update_lock(control, load_policy(control)["operator_sid"]):
                    context = resolve_lifecycle_context(managed, operation="register")
                    observed = _lifecycle_digests(observed_control)
            else:
                context = resolve_lifecycle_context(managed, operation="register")
        else:
            assert_version_writer(source_root, source="current_user_onboarding", environ=values)
        _WRITER_LOCAL.onboarding_lifecycle = {
            "input_root": selected_root, "source_root": source_root,
            "writer_root": root, "control": observed_control,
            "digests": observed, "delegation": delegation, "context": context,
        }
        yield
    finally:
        _WRITER_LOCAL.onboarding_lifecycle = previous


def onboarding_lifecycle_context(app_root: str | os.PathLike[str]) -> dict[str, Any] | None:
    """Return the protected sink's single observation without another helper."""
    scope = getattr(_WRITER_LOCAL, "onboarding_lifecycle", None)
    if scope is None:
        return None
    context = scope["context"]
    selected_root = Path(app_root).absolute()
    active_root = Path(context["active_code_root"]).absolute() if context is not None else None
    if selected_root not in {scope["input_root"], active_root}:
        raise WriterFencedError("WRITER_LIFECYCLE_CHANGED", "onboarding lifecycle scope is inexact")
    return dict(context) if context is not None else None


def _check_onboarding_lifecycle(root: Path, values: Mapping[str, str]) -> bool:
    scope = getattr(_WRITER_LOCAL, "onboarding_lifecycle", None)
    if scope is None:
        return False
    delegation = active_fence(root, environ=values)
    if (root != scope["writer_root"] or Path(__file__).resolve().parent != scope["source_root"]
            or delegation != scope["delegation"]
            or (delegation is not None and not _delegation_matches(delegation, source="current_user_onboarding", environ=values))
            or _lifecycle_digests(scope["control"]) != scope["digests"]):
        raise WriterFencedError("WRITER_LIFECYCLE_CHANGED", "lifecycle or delegation changed during onboarding")
    return True


@contextmanager
def writer_admission(
    source: str,
    *,
    control_root: str | os.PathLike[str] | None = None,
    timeout_seconds: float = 5.0,
    environ: Mapping[str, str] | None = None,
) -> Iterator[None]:
    global _WRITER_EXIT_COUNT
    nested = bool(getattr(_WRITER_LOCAL, "depth", 0))
    with _WRITER_EXIT_CONDITION:
        if _WRITER_EXIT_CLOSED and not nested:
            raise WriterFencedError("WRITER_EXIT_CLOSED", "writer admission is closed for exit")
        _WRITER_EXIT_COUNT += 1
    try:
        with _writer_admission(source, control_root=control_root,
                               timeout_seconds=timeout_seconds, environ=environ):
            yield
    finally:
        with _WRITER_EXIT_CONDITION:
            _WRITER_EXIT_COUNT -= 1
            _WRITER_EXIT_CONDITION.notify_all()


@contextmanager
def _writer_admission(
    source: str,
    *,
    control_root: str | os.PathLike[str] | None = None,
    timeout_seconds: float = 5.0,
    environ: Mapping[str, str] | None = None,
) -> Iterator[None]:
    """Admit one bounded mutation or deny it without persistent changes."""

    selected_source = str(source or "").strip()
    blocked = getattr(_WRITER_LOCAL, "remote_apply_error", None)
    if blocked is not None:
        raise blocked
    if not selected_source:
        raise WriterFencedError("WRITER_SOURCE_MISSING", "writer source is required")
    depth = int(getattr(_WRITER_LOCAL, "depth", 0))
    if depth < 0:
        # A negative depth is truthy, so without this the nested branch below
        # would admit every later writer on this thread with no admission mutex
        # and no active-fence check.  Deny instead of clamping: a clamp would
        # hide the same accounting defect the next time it appears.
        raise WriterFenceError(
            "WRITER_ADMISSION_DEPTH_UNDERFLOW",
            "writer admission depth accounting underflowed",
        )
    if depth:
        if selected_source == "current_user_onboarding":
            _check_onboarding_lifecycle(_WRITER_LOCAL.control_root, _WRITER_LOCAL.environ)
        allowed = getattr(_WRITER_LOCAL, "allowed_sources", None)
        if allowed is not None and selected_source not in allowed:
            raise WriterFencedError(
                "ACTIVE_WRITER_FENCE",
                "nested writer source is not delegated by the active deployment fence",
            )
        _WRITER_LOCAL.depth = depth + 1
        try:
            yield
        finally:
            _WRITER_LOCAL.depth -= 1
        return

    values = os.environ if environ is None else environ
    root = (
        Path(os.path.abspath(os.fspath(Path(control_root).expanduser())))
        if control_root is not None
        else canonical_control_root(values)
    )
    lease: _NamedMutexLease | None = None
    try:
        lease = _acquire_named_mutex(
            writer_admission_mutex_name(root, environ=values),
            timeout_seconds,
        )
        if lease is None:
            raise WriterFencedError(
                "WRITER_GATE_TIMEOUT",
                "writer admission mutex is held by a deployment operation",
            )
        if lease.abandoned:
            raise WriterFencedError(
                "WRITER_GATE_ABANDONED",
                "writer admission mutex ownership was abandoned",
            )
        onboarding_observed = selected_source == "current_user_onboarding" and _check_onboarding_lifecycle(root, values)
        allowed_sources = _admitted_writer_sources(
            root,
            source=selected_source,
            environ=values,
        )
        if allowed_sources is None and not onboarding_observed:
            from auto_update import assert_version_writer
            if selected_source == "current_user_onboarding":
                # Lifecycle Verify/Observe launches helpers. A relay cannot
                # refresh its status if those helpers retain our writer lease.
                from auto_update import control_root
                lease.suspend()
                control = control_root(Path(__file__).resolve().parent)
                observed = {}
                if control is not None:
                    for name in ("policy.json", "active.json", "transition.json"):
                        path = control / name
                        if path.is_file():
                            with path.open("rb") as handle:
                                observed[path] = hashlib.file_digest(handle, "sha256").digest()
                        else:
                            observed[path] = None
                assert_version_writer(Path(__file__).resolve().parent, source=selected_source, environ=values)
                lease.resume(timeout_seconds)
                if lease.abandoned:
                    raise WriterFencedError("WRITER_GATE_ABANDONED", "writer admission ownership was abandoned")
                if _admitted_writer_sources(root, source=selected_source, environ=values) is not None:
                    raise WriterFencedError("ACTIVE_WRITER_FENCE", "writer delegation changed during lifecycle observation")
                for path, expected in observed.items():
                    if path.is_file():
                        with path.open("rb") as handle:
                            actual = hashlib.file_digest(handle, "sha256").digest()
                    else:
                        actual = None
                    if actual != expected:
                        raise WriterFencedError("WRITER_LIFECYCLE_CHANGED", "lifecycle changed during writer observation")
            else:
                assert_version_writer(Path(__file__).resolve().parent, source=selected_source, environ=values)
    except BaseException:
        if lease is not None:
            lease.release()
        raise

    _WRITER_LOCAL.depth = 1
    _WRITER_LOCAL.allowed_sources = allowed_sources
    _WRITER_LOCAL.lease = lease
    _WRITER_LOCAL.source = selected_source
    _WRITER_LOCAL.control_root = root
    _WRITER_LOCAL.environ = values
    _WRITER_LOCAL.timeout_seconds = float(timeout_seconds)
    try:
        yield
    finally:
        failed_removal_authority = getattr(_WRITER_LOCAL, "failed_removal_authority", None)
        _WRITER_LOCAL.failed_removal_authority = None
        _WRITER_LOCAL.dead_installer_settlement = None
        if failed_removal_authority is not None:
            failed_removal_authority.release()
        _WRITER_LOCAL.remote_apply_error = None
        _WRITER_LOCAL.depth = 0
        _WRITER_LOCAL.allowed_sources = None
        _WRITER_LOCAL.lease = None
        _WRITER_LOCAL.source = None
        _WRITER_LOCAL.control_root = None
        _WRITER_LOCAL.environ = None
        _WRITER_LOCAL.timeout_seconds = None
        lease.release()


@contextmanager
def remote_request() -> Iterator[None]:
    """Suspend a writer lease only for transport, then revalidate admission.

    Callers keep their durable intent and must recheck its identity/version
    before applying a response. A denied resume must propagate to the worker;
    it must never be converted into a local retry write under the old lease.
    """
    blocked = getattr(_WRITER_LOCAL, "remote_apply_error", None)
    if blocked is not None:
        raise blocked
    depth = int(getattr(_WRITER_LOCAL, "depth", 0))
    if not depth:
        yield
        return
    names = ("allowed_sources", "lease", "source", "control_root", "environ", "timeout_seconds")
    saved = {name: getattr(_WRITER_LOCAL, name, None) for name in names}
    lease = saved["lease"]
    if not isinstance(lease, _NamedMutexLease) or not saved["source"] or saved["timeout_seconds"] is None:
        raise WriterFencedError("WRITER_REMOTE_CONTEXT_INVALID", "remote writer context is inexact")
    # Coordinator 2026-10-02: hidden F5 recovery retains admission through HTTP.
    if saved["source"] in {
        "phs_current_recovery", "phs_reconciliation_recovery",
    }:
        yield
        return
    lease.suspend()
    _WRITER_LOCAL.depth = 0
    for name in names:
        setattr(_WRITER_LOCAL, name, None)
    try:
        yield
    finally:
        error = None
        resumed_sources = frozenset()
        try:
            lease.resume(float(saved["timeout_seconds"]))
            if lease.abandoned:
                raise WriterFencedError("WRITER_GATE_ABANDONED", "writer admission mutex was abandoned")
            resumed_sources = _admitted_writer_sources(saved["control_root"],
                source=saved["source"], environ=saved["environ"])
            if resumed_sources is not None and resumed_sources != saved["allowed_sources"]:
                raise WriterFencedError("ACTIVE_WRITER_FENCE", "writer delegation changed during remote request")
        except WriterFenceError as exc:
            error = exc
        finally:
            for name, value in saved.items():
                setattr(_WRITER_LOCAL, name, value)
            _WRITER_LOCAL.depth = depth
            _WRITER_LOCAL.allowed_sources = resumed_sources if error is None else frozenset()
            _WRITER_LOCAL.remote_apply_error = error
        if error is not None:
            raise error


P = ParamSpec("P")
R = TypeVar("R")
_PROBE_ONLY_WRITER_SINKS = frozenset({
    ("relay_child_launch", "user_relay.run_session_direct_sync_once"),
    ("user_relay_process_start", "user_relay.start_user_relay_process"),
    ("current_user_onboarding", "current_user_onboarding.onboard_current_user"),
    ("auto_update_write_download_bytes", "auto_update.write_download_bytes"),
    ("auto_update_write_download_chunk", "auto_update.write_download_chunk"),
    ("auto_update_stage_prepared_copies", "auto_update.stage_prepared_copies"),
    ("auto_update_run_download_attempt", "auto_update._run_download_attempt"),
})

# These probes write only owned staging names. Fence actors do not consume
# them, and SYSTEM defers consumption while an application writer is live.
# Unlike the existing relay launcher, staging must reject inherited leases.
_STAGING_PROBE_ONLY_WRITER_SINKS = frozenset({
    ("auto_update_write_download_bytes", "auto_update.write_download_bytes"),
    ("auto_update_write_download_chunk", "auto_update.write_download_chunk"),
    ("auto_update_stage_prepared_copies", "auto_update.stage_prepared_copies"),
    ("auto_update_run_download_attempt", "auto_update._run_download_attempt"),
})


def _probe_then_call(
    source: str,
    function: Callable[P, R],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> R:
    """Probe admission, release any parent lease, then call the child launcher."""

    outer_depth = int(getattr(_WRITER_LOCAL, "depth", 0))
    blocked = getattr(_WRITER_LOCAL, "remote_apply_error", None)
    if blocked is not None:
        raise blocked
    def call() -> R:
        if source == "current_user_onboarding":
            app_root = args[0] if args else kwargs["app_root"]
            from auto_update import now_utc
            started_at = os.environ.get("KMTECH_LM_UPDATE_DEFERRED_AT") or now_utc()
            for attempt in range(2):
                try:
                    with _onboarding_lifecycle_scope(app_root, recheck=bool(attempt)):
                        with writer_admission(source):
                            pass
                        return function(*args, **kwargs)
                except Exception as error:
                    cause = error
                    while (cause is not None and getattr(cause, "code", None) != "WRITER_LIFECYCLE_CHANGED"
                           and str(cause) not in {"supported lifecycle changed during observation", "supported transition requires removal and reinstall", "update mutex ownership is busy"}):
                        cause = cause.__cause__
                    if not kwargs.get("_startup") or cause is None:
                        raise
            from auto_update import (assert_version_writer, assert_ordinary_recovery, control_root,
                                     emit_support_record, load_policy, optional_record, read_json)
            root = canonical_control_root()
            _admitted_writer_sources(root, source=source, environ=os.environ)
            source_root = Path(__file__).resolve().parent
            control = control_root(source_root)
            if control is None:
                raise WriterFencedError("WRITER_LIFECYCLE_CHANGED", "startup installation context is unobservable")
            state = optional_record(control / "transition.json")
            trial = (state or {}).get("phase") in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"} and os.environ.get("KMTECH_LM_UPDATE_TRIAL_NONCE") and os.environ.get("KMTECH_LM_UPDATE_TRIAL_NONCE") == state.get("nonce")
            if state and state.get("phase") in {"PREPARED", "FAILED", "UNKNOWN", "APPLIED_PENDING_READY"} and not trial:
                assert_ordinary_recovery(control, load_policy(control), read_json(control / "active.json")["sha"], started_at)
                os.environ["KMTECH_LM_UPDATE_DEFERRED_AT"] = started_at
            assert_version_writer(source_root, source="gui_settings_save", environ=os.environ)
            report = {"status": "UNKNOWN", "reason_code": "UPDATE_SUPERVISION_UNAVAILABLE", "exit_code": 0}
            emit_support_record(report, stage="update_apply", policy=load_policy(control))
            return report
        return function(*args, **kwargs)

    if source != "current_user_onboarding" or outer_depth:
        with writer_admission(source):
            pass
    if not outer_depth:
        return call()

    lease = getattr(_WRITER_LOCAL, "lease", None)
    outer_source = str(getattr(_WRITER_LOCAL, "source", "") or "")
    root = getattr(_WRITER_LOCAL, "control_root", None)
    values = getattr(_WRITER_LOCAL, "environ", None)
    timeout_seconds = getattr(_WRITER_LOCAL, "timeout_seconds", None)
    allowed_sources = getattr(_WRITER_LOCAL, "allowed_sources", None)
    if (
        not isinstance(lease, _NamedMutexLease)
        or not outer_source
        or not isinstance(root, Path)
        or values is None
        or timeout_seconds is None
    ):
        raise WriterFencedError(
            "WRITER_PROBE_CONTEXT_INVALID",
            "probe-only admission cannot release an inexact parent writer context",
        )

    lease.suspend()
    _WRITER_LOCAL.depth = 0
    _WRITER_LOCAL.allowed_sources = None
    _WRITER_LOCAL.lease = None
    _WRITER_LOCAL.source = None
    _WRITER_LOCAL.control_root = None
    _WRITER_LOCAL.environ = None
    _WRITER_LOCAL.timeout_seconds = None
    resumed_sources = allowed_sources
    resume_error = None
    try:
        return call()
    finally:
        try:
            lease.resume(float(timeout_seconds))
            if lease.abandoned:
                raise WriterFencedError(
                    "WRITER_GATE_ABANDONED",
                    "writer admission mutex ownership was abandoned",
                )
            resumed_sources = _admitted_writer_sources(
                root,
                source=outer_source,
                environ=values,
            )
        except BaseException as exc:
            resume_error = exc
            raise
        finally:
            _WRITER_LOCAL.depth = outer_depth
            _WRITER_LOCAL.allowed_sources = resumed_sources if resume_error is None else frozenset()
            # A caller catching the resume error must not mutate through the
            # restored parent depth with stale delegation.
            _WRITER_LOCAL.remote_apply_error = resume_error
            _WRITER_LOCAL.lease = lease
            _WRITER_LOCAL.source = outer_source
            _WRITER_LOCAL.control_root = root
            _WRITER_LOCAL.environ = values
            _WRITER_LOCAL.timeout_seconds = float(timeout_seconds)


def writer_sink(
    source: str,
    *,
    probe_only: bool = False,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Guard one sink with a spanning lease or its exact child-launch probe."""

    selected_source = str(source or "").strip()
    if not selected_source:
        raise ValueError("writer sink source is required")

    def decorate(function: Callable[P, R]) -> Callable[P, R]:
        module_name = function.__module__
        if module_name == "__main__":
            definition_path = Path(
                function.__globals__.get("__file__", function.__code__.co_filename)
            ).resolve()
            try:
                app_path = definition_path.relative_to(Path(__file__).resolve().parent)
            except ValueError:
                pass
            else:
                module_name = ".".join(app_path.with_suffix("").parts)
        sink_id = f"{module_name}.{function.__qualname__}"
        approved_probe = (selected_source, sink_id) in _PROBE_ONLY_WRITER_SINKS
        if probe_only and not approved_probe:
            raise WriterFencedError(
                "WRITER_PROBE_ONLY_SINK_NOT_ALLOWED",
                "probe-only admission is not approved for this writer sink",
            )

        @wraps(function)
        def guarded(*args: P.args, **kwargs: P.kwargs) -> R:
            # This fixed, best-effort startup report grants no business write.
            # It must also report a refusal without acquiring writer ownership.
            if (selected_source, sink_id) == (
                "startup_check_result", "startup_observation.write_startup_result"
            ):
                return function(*args, **kwargs)
            # This exact sink is selected here instead of changing its deployed,
            # code-derived inventory source bytes and rotating the inventory pin.
            if approved_probe:
                if (selected_source, sink_id) in _STAGING_PROBE_ONLY_WRITER_SINKS and int(
                    getattr(_WRITER_LOCAL, "depth", 0)
                ) > 0:
                    raise WriterFencedError(
                        "STAGING_WRITER_ADMISSION_HELD",
                        "feed staging cannot run inside a local writer admission",
                    )
                return _probe_then_call(
                    selected_source,
                    function,
                    args,
                    kwargs,
                )
            with writer_admission(selected_source):
                return function(*args, **kwargs)

        setattr(guarded, "__label_writer_sink__", selected_source)
        return guarded

    return decorate


__all__ = [
    "ACTIVE_FILENAME",
    "ACTIVE_SCHEMA",
    "ACTIVE_FIELDS",
    "APP_ID",
    "CONTROL_RELATIVE_PATH",
    "CONTROL_ROOT_OVERRIDE_ENV",
    "DELEGATION_ATTEMPT_ENV",
    "DELEGATION_SESSION_ENV",
    "DELEGATION_TOKEN_ENV",
    "DELEGATION_TRANSACTION_ENV",
    "SESSION_MUTEX_PREFIX",
    "SESSION_TUPLE_VERSION",
    "TEST_MODE_ENV",
    "WRITER_INVENTORY_SHA256",
    "WRITER_MUTEX_NAME",
    "WriterFenceError",
    "WriterFencedError",
    "active_fence",
    "canonical_control_root",
    "session_authority_mutex_name",
    "prove_dead_unfinished_installer",
    "settle_dead_installer_fence",
    "writer_admission",
    "writer_admission_mutex_name",
    "writer_sink",
    "remote_request",
]

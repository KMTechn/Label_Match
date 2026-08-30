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
WRITER_INVENTORY_SHA256 = "00015dcba9c1cb6ca54ba9d051796dc91d425d09cc569d4f42ba0302b710461a"

DELEGATION_TOKEN_ENV = "KMTECH_LABEL_WRITER_DELEGATION_TOKEN"
DELEGATION_SESSION_ENV = "KMTECH_LABEL_WRITER_DELEGATION_SESSION_ID"
DELEGATION_ATTEMPT_ENV = "KMTECH_LABEL_WRITER_DELEGATION_ATTEMPT_ID"
DELEGATION_TRANSACTION_ENV = "KMTECH_LABEL_WRITER_DELEGATION_TRANSACTION_ID"

_HEX_32 = re.compile(r"^[0-9a-f]{32}$")
_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_WRITER_LOCAL = threading.local()

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
    return unicodedata.normalize("NFC", selected.replace("/", "\\").rstrip("\\")).lower()


def canonical_control_root(environ: Mapping[str, str] | None = None) -> Path:
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

    def release(self) -> None:
        handle, self.handle = self.handle, 0
        if not handle:
            return
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        try:
            kernel32.ReleaseMutex(ctypes.c_void_p(handle))
        finally:
            kernel32.CloseHandle(ctypes.c_void_p(handle))


def _acquire_named_mutex(name: str, timeout_seconds: float) -> _NamedMutexLease | None:
    if os.name != "nt":
        return _NamedMutexLease(0)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateMutexW.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_wchar_p]
    kernel32.CreateMutexW.restype = ctypes.c_void_p
    kernel32.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_ulong]
    kernel32.WaitForSingleObject.restype = ctypes.c_ulong
    handle = kernel32.CreateMutexW(None, False, name)
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


@contextmanager
def writer_admission(
    source: str,
    *,
    control_root: str | os.PathLike[str] | None = None,
    timeout_seconds: float = 5.0,
    environ: Mapping[str, str] | None = None,
) -> Iterator[None]:
    """Admit one bounded mutation or deny it without persistent changes."""

    selected_source = str(source or "").strip()
    if not selected_source:
        raise WriterFencedError("WRITER_SOURCE_MISSING", "writer source is required")
    depth = int(getattr(_WRITER_LOCAL, "depth", 0))
    if depth:
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
        try:
            active = active_fence(root, environ=values)
        except WriterFenceError as exc:
            raise WriterFencedError(
                exc.code,
                "writer denied because the active fence is not exact",
            ) from exc
        if active is not None and not _delegation_matches(
            active,
            source=selected_source,
            environ=values,
        ):
            raise WriterFencedError(
                "ACTIVE_WRITER_FENCE",
                "writer denied by the active deployment fence",
            )
    except WriterFenceError:
        if lease is not None:
            lease.release()
        raise

    _WRITER_LOCAL.depth = 1
    _WRITER_LOCAL.allowed_sources = (
        frozenset(active["delegated_sources"]) if active is not None else None
    )
    try:
        yield
    finally:
        _WRITER_LOCAL.depth = 0
        _WRITER_LOCAL.allowed_sources = None
        lease.release()


P = ParamSpec("P")
R = TypeVar("R")


def writer_sink(source: str) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Guard and mark one concrete sink for the code-derived inventory."""

    selected_source = str(source or "").strip()
    if not selected_source:
        raise ValueError("writer sink source is required")

    def decorate(function: Callable[P, R]) -> Callable[P, R]:
        @wraps(function)
        def guarded(*args: P.args, **kwargs: P.kwargs) -> R:
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
    "writer_admission",
    "writer_admission_mutex_name",
    "writer_sink",
]

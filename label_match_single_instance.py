"""Fail-closed single-instance ownership for the packaged Label Match app.

The guard is keyed by the durable data root rather than the executable path so
two installed copies cannot concurrently open the same recovery/outbox state.
Windows owns the mutex lifetime, which also makes updater restarts and crash
recovery safe from stale lock files.
"""

from __future__ import annotations

from dataclasses import dataclass
import ctypes
import hashlib
import json
import logging
import ntpath
import os
import threading
from pathlib import Path
from typing import Callable, Mapping

from writer_session_fence import writer_sink
import label_match_instance_win32 as instance_win32
from label_match_instance_win32 import ActivationResult


ERROR_ALREADY_EXISTS = 183


class SingleInstanceError(RuntimeError):
    """Raised when ownership cannot be established safely."""


_active_instance_lease = threading.local()
_process_instance_lease = None
_instance_notice_lock = threading.RLock()


def _retain_current_instance_lease() -> None:
    """An unfinished writer keeps its native handle until OS process cleanup."""
    lease = getattr(_active_instance_lease, "lease", None) or _process_instance_lease
    if lease is not None:
        lease._retain_on_exit = True


@writer_sink("gui_shutdown_stall_notice")
def _set_shutdown_stall_notice(data_scope, *, stalled: bool) -> None:
    """A best-effort diagnostic uses the same admission as ordinary writers."""
    import sys

    path = Path(data_scope) / "label_match_shutdown_stall.json"
    if stalled:
        temporary = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        try:
            with temporary.open("w", encoding="utf-8") as stream:
                json.dump({"pid": os.getpid(), "executable": sys.executable}, stream)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
    else:
        path.unlink(missing_ok=True)


def _try_update_shutdown_stall_notice(data_scope, *, stalled: bool) -> None:
    try:
        _set_shutdown_stall_notice(data_scope, stalled=stalled)
    except Exception as exc:
        logging.getLogger(__name__).warning("shutdown notice unavailable: %s", type(exc).__name__)


def _shutdown_stall_message(data_scope) -> str:
    import sys
    try:
        path = Path(data_scope) / "label_match_shutdown_stall.json"
        with path.open("rb") as stream:
            raw = stream.read(4097)
        if len(raw) > 4096:
            return ""
        notice = json.loads(raw)
        pid, executable = notice["pid"], notice["executable"]
        if type(pid) is not int or not 0 < pid < 2**32 or not isinstance(executable, str) or not executable:
            return ""
        if (ntpath.normcase(ntpath.normpath(executable)) == ntpath.normcase(ntpath.normpath(sys.executable))
                and instance_win32.process_matches(pid, sys.executable)):
            return "Label Match 종료 정리가 지연되고 있습니다. 앱이 닫히고 다시 안 켜지면 PC를 재시작하세요."
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        pass
    return ""


def canonical_data_scope(path: str | os.PathLike[str]) -> str:
    text = str(path or "").strip()
    if not text:
        raise SingleInstanceError("Label Match data scope is empty")
    return ntpath.normcase(ntpath.normpath(text))


def mutex_name_for_data_scope(path: str | os.PathLike[str]) -> str:
    digest = hashlib.sha256(canonical_data_scope(path).encode("utf-8")).hexdigest()
    return rf"Global\KMTech.LabelMatch.{digest[:32]}"


def prepare_user_directory(path: str | os.PathLike[str]) -> None:
    """User writers may use prepared machine folders, never create them."""
    folder = Path(path).expanduser().resolve(strict=False)
    machine_roots = {Path(r"C:\ProgramData").resolve(strict=False)}
    for name in ("ProgramData", "PROGRAMDATA"):
        value = str(os.environ.get(name) or "").strip()
        if value:
            machine_roots.add(Path(value).expanduser().resolve(strict=False))
    if any(folder == root or root in folder.parents for root in machine_roots):
        if not folder.is_dir():
            raise SingleInstanceError("포장 기계 자료 폴더가 준비되지 않았습니다. 관리자에게 설치 확인을 요청하세요.")
        return
    folder.mkdir(parents=True, exist_ok=True)


def resolve_data_scope(
    *,
    environment: Mapping[str, str] | None = None,
    settings_path: str | os.PathLike[str] | None = None,
    settings: Mapping[str, object] | None = None,
    settings_template_path: str | os.PathLike[str] | None = None,
    for_onboarding: bool = False,
) -> str:
    """Resolve the business root for onboarding, GUI, guard and relay, read-only.

    Custom settings precede the environment. Existing default locations are
    detected without moving data; onboarding always starts in current-user storage.
    A missing user file can use the same template that startup will later copy;
    an existing invalid file never falls back to that template.
    """

    env = os.environ if environment is None else environment
    payload = settings
    if payload is None and settings_path:
        selected_path = Path(settings_path)
        if not selected_path.exists() and settings_template_path:
            selected_path = Path(settings_template_path)
        try:
            with selected_path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, UnicodeError, json.JSONDecodeError):
            # Normal startup diagnoses invalid settings after owning the root.
            pass

    configured = str(
        (payload.get("custom_save_path", "") or "")
        if isinstance(payload, Mapping) else ""
    ).strip()
    override = str(env.get("LABEL_MATCH_SAVE_DIR", "") or "").strip()
    selected = configured or override
    rule = "settings.custom_save_path" if configured else "environment.LABEL_MATCH_SAVE_DIR"
    if not selected:
        local_app_data = str(env.get("LOCALAPPDATA", "") or "").strip()
        if (
            for_onboarding
            and not local_app_data
            and not str(env.get("LABEL_MATCH_LOGISTICS_PROFILE_PATH") or "").strip()
        ):
            # A legacy business folder is not a current-user identity anchor.
            # Stop before resolving or inspecting the ProgramData fallback.
            from logistics_runtime_profile import LogisticsRuntimeConfigurationError

            raise LogisticsRuntimeConfigurationError(
                "LOCALAPPDATA or an explicit data root is required for the current-user logistics profile"
            )
        direct_root = str(
            env.get("LABEL_MATCH_DIRECT_SYNC_ROOT")
            or env.get("LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT") or ""
        ).strip()
        if not direct_root and local_app_data:
            direct_root = str(Path(local_app_data) / "KMTech" / "DirectSync" / "label_match")
        onboarding_exists = bool(direct_root) and any(
            (Path(direct_root) / name).is_file()
            for name in (
                "producer_identity.json", "producer_manifest.json",
                "status/current_user_onboarding.json",
                "status/label_match_worker_pc_registration.json",
            )
        )
        program_data = str(env.get("ProgramData", env.get("PROGRAMDATA", r"C:\ProgramData")) or "").strip()
        legacy_root = Path(program_data) / "KMTech" / "Label_Match" / "data" if program_data else None
        legacy_data_exists = False
        if legacy_root is not None and not (local_app_data and (onboarding_exists or for_onboarding)):
            legacy_root = legacy_root.expanduser().resolve(strict=False)
            try:
                with os.scandir(legacy_root) as entries:
                    legacy_data_exists = any(
                        entry.is_file() and (
                            entry.name.casefold().endswith((".csv", ".sqlite3", ".db"))
                            or entry.name == "_current_set_state_packaging.json"
                        )
                        for entry in entries
                    )
            except FileNotFoundError:
                pass
            except OSError as exc:
                raise SingleInstanceError("Unable to inspect legacy Label Match data root") from exc
        if local_app_data and (onboarding_exists or for_onboarding):
            # Registration and its later consumers share the current-user root,
            # including before the first identity/report exists.
            selected = str(Path(local_app_data) / "KMTech" / "Label_Match" / "data")
            rule = "onboarding_state" if onboarding_exists else "new_current_user_onboarding"
        elif legacy_data_exists:
            # Preserve the fallback only for standalone use without onboarding.
            selected, rule = str(legacy_root), "existing_program_data"
        elif legacy_root is not None and legacy_root.is_dir():
            selected, rule = str(legacy_root), "legacy_standalone_default"
        elif local_app_data:
            selected = str(Path(local_app_data) / "KMTech" / "Label_Match" / "data")
            rule = "new_current_user_data"
        else:
            raise SingleInstanceError("포장 자료 폴더가 준비되지 않았습니다. 관리자에게 설치 확인을 요청하세요.")
    if (
        for_onboarding
        and not str(env.get("LOCALAPPDATA") or "").strip()
        and not str(env.get("LABEL_MATCH_LOGISTICS_PROFILE_PATH") or "").strip()
        and not override
    ):
        # Only this fallback also selects the logistics identity location.
        # Ordinary business storage may continue to use a redirected folder.
        from logistics_runtime_profile import assert_path_has_no_reparse_components

        assert_path_has_no_reparse_components(
            selected, label="current-user profile data root"
        )
    resolved = str(Path(selected).expanduser().resolve(strict=False))
    logging.getLogger(__name__).warning("storage_root_selected: rule=%s data_root=%s", rule, resolved)
    return resolved


@dataclass
class MutexLease:
    owner: bool
    name: str
    handle: int | None = None
    _close_handle: Callable[[int], object] | None = None

    def close(self) -> None:
        with _instance_notice_lock:
            pinned = getattr(self, "_notice_handle", None)
            self._notice_handle = None
            handle = self.handle
            self.handle = None
            try:
                if pinned is not None:
                    pinned.close()  # DELETE_ON_CLOSE was granted under the metadata writer.
            finally:
                if handle and self._close_handle is not None:
                    self._close_handle(handle)

    def __enter__(self) -> "MutexLease":
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        self.close()


def acquire_data_scope_mutex(
    data_scope: str | os.PathLike[str],
    *,
    kernel32=None,
    last_error_getter: Callable[[], int] | None = None,
) -> MutexLease:
    name = mutex_name_for_data_scope(data_scope)
    if os.name != "nt" and kernel32 is None:
        # Label Match is packaged for Windows.  A no-op owner keeps source-mode
        # imports and non-Windows unit tests available without pretending that
        # a cross-platform process lock protects production state.
        return MutexLease(owner=True, name=name)

    handle, error, close_handle = instance_win32.create_mutex(
        name, kernel32=kernel32, last_error_getter=last_error_getter,
    )
    if not handle:
        raise SingleInstanceError(
            f"Label Match single-instance mutex acquisition failed ({error})"
        )
    handle_value = int(handle)
    if error == ERROR_ALREADY_EXISTS:
        close_handle(handle)
        return MutexLease(owner=False, name=name)
    return MutexLease(
        owner=True,
        name=name,
        handle=handle_value,
        _close_handle=close_handle,
    )


def instance_notice_path(data_scope):
    # The mutex keeps its data-root scope; metadata stays in the current user's root.
    local = str(os.environ.get("LOCALAPPDATA") or "").strip()
    if not local:
        raise SingleInstanceError("current-user instance storage is unavailable")
    selected = Path(local).expanduser().resolve(strict=False)
    machine = {Path("C:/ProgramData").resolve(strict=False)}
    for name in ("ProgramData", "PROGRAMDATA"):
        value = str(os.environ.get(name) or "").strip()
        if value:
            machine.add(Path(value).expanduser().resolve(strict=False))
    if any(selected == root or root in selected.parents for root in machine):
        raise SingleInstanceError("instance metadata requires current-user storage")
    suffix = hashlib.sha256(canonical_data_scope(data_scope).encode("utf-8")).hexdigest()[:32]
    return selected / "KMTech" / "Label_Match" / "single-instance" / (suffix + ".json")


def _validated_owner_notice(data_scope):
    import sys
    try:
        notice = json.loads(instance_win32.read_notice(instance_notice_path(data_scope)))
        pid, executable = notice["pid"], notice["executable"]
        if (type(pid) is not int or not 0 < pid < 2**32
                or not isinstance(executable, str) or not ntpath.isabs(executable)
                or ntpath.normcase(ntpath.normpath(executable)) != ntpath.normcase(ntpath.normpath(sys.executable))
                or (notice.get("closing") is not None and type(notice["closing"]) is not bool)
                or not instance_win32.process_matches(pid, sys.executable)):
            return None
        return notice
    except (OSError, ValueError, TypeError, KeyError, AttributeError, SingleInstanceError):
        return None


@writer_sink("gui_instance_owner_notice")
def _write_instance_owner_notice(lease, *, closing=False):
    import sys
    path = instance_notice_path(lease._data_scope)
    prepare_user_directory(path.parent)
    payload = {"pid": os.getpid(), "executable": sys.executable, "closing": bool(closing)}
    previous = getattr(lease, "_notice_handle", None)
    if previous is not None:
        # The Windows DELETE_ON_CLOSE handle pins this inode until lease close.
        # Update through its admitted write authority instead of replacing it.
        previous.update(json.dumps(payload).encode("utf-8"))
        return
    temporary = path.with_name(path.name + "." + str(os.getpid()) + ".tmp")
    try:
        with temporary.open("w", encoding="utf-8") as stream:
            json.dump(payload, stream)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        pinned = instance_win32.pin_notice(path)
        lease._notice_handle = pinned
    finally:
        temporary.unlink(missing_ok=True)


def mark_current_instance_closing():
    with _instance_notice_lock:
        # The shell's hidden Tk owner runs on a different thread from its lease.
        lease = getattr(_active_instance_lease, "lease", None) or _process_instance_lease
        if lease is not None and lease.owner and not getattr(lease, "_closing_notice", False):
            try:
                _write_instance_owner_notice(lease, closing=True)
                lease._closing_notice = True
            except Exception as error:
                logging.getLogger(__name__).warning("instance closing notice unavailable: %s", type(error).__name__)


def activate_existing_label_window(*, data_scope):
    notice = _validated_owner_notice(data_scope)
    if notice is None:
        return ActivationResult(False)
    return instance_win32.activate_pid(notice["pid"])


def _duplicate_entry(data_scope, activate=None):
    notice = _validated_owner_notice(data_scope)
    try:
        result = (activate() if activate is not None else
                  instance_win32.activate_pid(notice["pid"]) if notice is not None else ActivationResult(False))
    except (OSError, AttributeError):
        result = ActivationResult(False)
    if not result.found:
        try:
            instance_win32.timed_notice(closing=notice.get("closing") if notice is not None else None)
        except (OSError, AttributeError):
            pass
    return 0


def _is_gui_entry(arguments):
    non_gui = {"--label-match-lifecycle-context", "--label-match-direct-sync-relay", "--label-match-user-relay",
               "--label-match-scheduled-relay", "--onboard-current-user", "--remove-current-user-setup",
               "--restore-current-user-lifecycle-after-replacement", "--zero-pe-portable-smoke"}
    return not any(value in non_gui or value.startswith("--kmtech-lm-") for value in arguments)


def _early_data_scope(app_root):
    import sys
    explicit = str(os.environ.get("LABEL_MATCH_SETTINGS_PATH") or "").strip()
    local = str(os.environ.get("LOCALAPPDATA") or "").strip()
    if not explicit and not local:
        raise SingleInstanceError("current-user settings storage is unavailable")
    settings = Path(explicit) if explicit else Path(local)/"KMTech"/"Label_Match"/"config"/"app_settings.json"
    root = Path(app_root)
    portable = root.name.casefold() == "app" and (root.parent/"portable-manifest.json").is_file()
    enabled = str(os.environ.get("LABEL_MATCH_ENABLE_FIRST_RUN_ONBOARDING") or "").strip().lower() in {"1","true","yes","on","enabled"}
    template = Path(getattr(sys, "_MEIPASS", root))/"config"/"app_settings.json"
    return resolve_data_scope(settings_path=settings, settings_template_path=template,
                              for_onboarding=bool(getattr(sys,"frozen",False) or portable or enabled))


def end_early_gui_entry(lease):
    global _process_instance_lease
    if lease is None:
        return
    if getattr(_active_instance_lease,"lease",None) is lease:
        _active_instance_lease.lease = getattr(lease,"_previous_lease",None)
    if _process_instance_lease is lease:
        _process_instance_lease = getattr(lease, "_previous_process_lease", None)
    if not getattr(lease,"_retain_on_exit",False):
        lease.close()


def begin_early_gui_entry(arguments, *, app_root):
    global _process_instance_lease
    if not _is_gui_entry(arguments):
        return None
    scope = _early_data_scope(app_root)
    lease = acquire_data_scope_mutex(scope)
    if not lease.owner:
        _duplicate_entry(scope)
        raise SystemExit(0)
    lease._data_scope = scope
    lease._early = True
    lease._previous_lease = getattr(_active_instance_lease,"lease",None)
    lease._previous_process_lease = _process_instance_lease
    _active_instance_lease.lease = lease
    _process_instance_lease = lease
    try:
        _write_instance_owner_notice(lease)
    except BaseException:
        end_early_gui_entry(lease)
        raise
    import atexit
    atexit.register(end_early_gui_entry, lease)
    return lease


def run_early_gui_entry(start, *, arguments, app_root):
    try:
        lease = begin_early_gui_entry(arguments, app_root=app_root)
    except SystemExit as stopped:
        return int(stopped.code or 0)
    try:
        return int(start() or 0)
    finally:
        end_early_gui_entry(lease)


def run_guarded_entrypoint(
    start: Callable[[], int | None],
    *,
    data_scope: str | os.PathLike[str],
    acquire: Callable[[str | os.PathLike[str]], MutexLease] = acquire_data_scope_mutex,
    activate: Callable[[], ActivationResult] | None = None,
) -> int:
    """Run ``start`` only for the data-scope owner."""

    global _process_instance_lease
    held = getattr(_active_instance_lease, "lease", None)
    if held is not None and held.owner and getattr(held, "_early", False):
        if canonical_data_scope(getattr(held,"_data_scope","")) != canonical_data_scope(data_scope):
            raise SingleInstanceError("instance data scope changed during startup")
        return int(start() or 0)
    lease = acquire(data_scope)
    if not lease.owner:
        return _duplicate_entry(data_scope, activate)
    lease._data_scope = data_scope
    previous = getattr(_active_instance_lease, "lease", None)
    previous_process = _process_instance_lease
    _active_instance_lease.lease = lease
    _process_instance_lease = lease
    try:
        _write_instance_owner_notice(lease)
        # Read the previous close evidence before ordinary state/outbox replay.
        notice = Path(data_scope) / "label_match_shutdown_stall.json"
        if notice.is_file():
            print("이전 종료가 끝나지 않았습니다. 저장된 작업과 보낼 것을 다시 확인합니다.")
        # Stale diagnostics never become an additional startup lock.
        _try_update_shutdown_stall_notice(data_scope, stalled=False)
        result = start()
        return int(result or 0)
    finally:
        _active_instance_lease.lease = previous
        _process_instance_lease = previous_process
        if not getattr(lease, "_retain_on_exit", False):
            lease.close()

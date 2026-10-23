"""Current-user persistence for the Label_Match DirectSync relay."""

from __future__ import annotations

import argparse
from contextvars import ContextVar
from datetime import datetime, timezone
import json
import ntpath
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback
from typing import Any, Callable, Mapping, Sequence
import uuid

from label_match_single_instance import acquire_data_scope_mutex, resolve_data_scope
from user_relay_stop_marker import (
    STOP_MARKER_V1,
    StopMarkerLineageError,
    build_successor_marker,
    read_stop_marker,
)
from writer_session_fence import WriterFenceError, WriterFencedError, writer_admission, writer_sink
from label_data_generation import DataGenerationError, check_user_data_open, record_user_data_open

USER_RELAY_MODE = "--label-match-user-relay"
DIRECT_SYNC_RELAY_MODE = "--label-match-direct-sync-relay"
USER_RELAY_RUN_VALUE = "KMTech.LabelMatch.Relay"
USER_RELAY_RUN_KEY = r"Software\Microsoft\Windows\CurrentVersion\Run"
DEFAULT_RETRY_INTERVAL_SECONDS = 30
MAX_RETRY_INTERVAL_SECONDS = 60
USER_RELAY_STATUS_NAME = "label_match_user_relay.json"
USER_RELAY_STOP_NAME = "label_match_user_relay.stop.json"
APP_STOP_NAME = "label_match_app.stop.json"
APP_STOP_SCHEMA = "label-match-app-stop-v1"
APP_STOP_TIMEOUT_SECONDS = 150.0
LABEL_MATCH_SOURCE_GLOB = "포장실작업이벤트로그_*.csv"
LABEL_MATCH_WORKER_ID = "direct-sync-relay-label-match-current-user"
_relay_main_running = ContextVar("relay_main_running", default=False)
_relay_main_active = ContextVar("relay_main_active", default=False)
_relay_entry_running = ContextVar("relay_entry_running", default=None)


from label_atomic_replace import atomic_replace

class UserRelayError(RuntimeError):
    """Raised when current-user relay persistence cannot be proven."""

    def __init__(self, message: str, *, cause_code: str = "", exit_code: int | None = None):
        super().__init__(message)
        self.cause_code = cause_code
        self.exit_code = exit_code


def _record_relay_reason(reason_code: str, *, app_root=None, exit_code=None, error=None,
                         failure_snapshot=True) -> None:
    """Keep fixed diagnostic codes even when ordinary writer admission is denied."""
    try:
        from auto_update import control_root, load_policy, support_record

        fields = {"reason_code": reason_code}
        if type(exit_code) is int:
            fields["exit_code"] = exit_code
        if error is not None:
            fields["exception_type"] = type(error).__name__
            source_root = Path(__file__).resolve().parent
            for frame, _ in traceback.walk_tb(error.__traceback__):
                source_name = frame.f_globals.get("__file__")
                if not source_name:
                    continue
                source = Path(source_name).resolve()
                if source.is_relative_to(source_root):
                    relative = source.relative_to(source_root)
                    if relative.parts and relative.parts[0] not in {"site-packages", "tests"}:
                        # Keep only the innermost product file/function names.
                        fields["location_module"] = Path(frame.f_code.co_filename).name
                        fields["location_function"] = frame.f_code.co_name
        report = support_record(fields, stage="update_apply")
        normal_after_running = _relay_main_running.get() and reason_code in {
            "RELAY_COMPLETED", "RELAY_STOP_REQUESTED", "RELAY_STARTUP_HANDLED",
        }
        if app_root is not None and failure_snapshot and not normal_after_running:
            root = Path(app_root)
            control = control_root(root / "app" if (root / "app").is_dir() else root)
            if control is not None:
                policy = load_policy(control)
                # Existing support snapshots are diagnostics, never recovery input.
                # A later parent failure must not erase this child's original reason.
                stage = Path(policy["staging_root"])
                _write_json_atomic(stage / ("failure-" + uuid.uuid4().hex + ".json"), report)
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    except Exception:
        pass  # Every diagnostic operation preserves the original relay outcome.


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    from failure_redaction import redact_json_for_sink
    serialized = json.dumps(redact_json_for_sink(dict(payload)), ensure_ascii=False,
                            indent=2, sort_keys=True) + "\n"
    _write_json_bytes_atomic(path, serialized)


def _write_machine_json_atomic(path: Path, payload: Mapping[str, Any], *, deadline=None) -> None:
    from failure_redaction import assert_machine_metadata_text
    serialized = json.dumps(dict(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    assert_machine_metadata_text(serialized, environ=os.environ)
    _write_json_bytes_atomic(path, serialized, deadline=deadline)


def _write_json_bytes_atomic(path: Path, serialized: str, *, deadline=None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        if deadline is None:
            atomic_replace(temporary, path)
        else:
            atomic_replace(temporary, path, deadline=deadline)
    finally:
        temporary.unlink(missing_ok=True)


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        if not path.is_file() or path.stat().st_size > 256 * 1024:
            return None
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _preserve_failure_evidence(path: Path, error: BaseException) -> dict[str, Any]:
    from auto_update import launch_failure_details

    evidence = {"failure_evidence_path": str(path), "failure_evidence_status": "PROVEN"}
    try:
        _write_json_atomic(path, {"launch_failure_details": launch_failure_details(error, include_original=True), "failed_at": _now()})
    except Exception as evidence_error:
        # A diagnostic storage failure must not replace the original cycle error.
        evidence.update(failure_evidence_status="FAILED",
                        failure_evidence_error_details=launch_failure_details(evidence_error))
    return evidence


def user_relay_status_path(direct_sync_root: str | os.PathLike[str]) -> Path:
    return (
        Path(direct_sync_root).expanduser().resolve()
        / "status"
        / USER_RELAY_STATUS_NAME
    )


def user_relay_stop_path(direct_sync_root: str | os.PathLike[str]) -> Path:
    return (
        Path(direct_sync_root).expanduser().resolve() / "control" / USER_RELAY_STOP_NAME
    )


def app_stop_request_path(direct_sync_root: str | os.PathLike[str]) -> Path:
    return Path(direct_sync_root).expanduser().resolve() / "control" / APP_STOP_NAME


def _app_stop_process_snapshot(*, timeout_seconds=10.0) -> list[dict[str, Any]]:
    """Read candidate identities without exposing their command lines in receipts."""
    from auto_update import system_executable
    script = r"""$ErrorActionPreference='Stop';$rows=@(Get-CimInstance Win32_Process -Filter "Name='python.exe' OR Name='pythonw.exe' OR Name='Label_Match.exe' OR Name='msedgewebview2.exe'" | ForEach-Object {
 $owner=''
 if (-not $_.ExecutablePath) {
  try { $o=Invoke-CimMethod -InputObject $_ -MethodName GetOwnerSid; if($o.ReturnValue -eq 0){$owner=[string]$o.Sid} } catch {}
 }
 @{pid=[int]$_.ProcessId;parent_pid=[int]$_.ParentProcessId;name=[string]$_.Name;
   image=[string]$_.ExecutablePath;command=[string]$_.CommandLine;sid=$owner;
   session_id=[int]$_.SessionId;created_at=([datetime]$_.CreationDate).ToUniversalTime().ToString('o')}
})
ConvertTo-Json -InputObject $rows -Compress
"""
    result = subprocess.run(
        [system_executable("WindowsPowerShell/v1.0/powershell.exe"), "-NoLogo", "-NoProfile",
         "-NonInteractive", "-WindowStyle", "Hidden", "-Command", script],
        capture_output=True, text=True, timeout=max(0.01, float(timeout_seconds)),
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    if result.returncode:
        raise UserRelayError("app stop process observation failed")
    rows = json.loads(result.stdout)
    if not isinstance(rows, list):
        raise UserRelayError("app stop process observation is incomplete")
    return rows


def _app_stop_path(value) -> str:
    return ntpath.normcase(ntpath.normpath(str(Path(value).resolve())))


def _webview_user_data_dir(command: str) -> str | None:
    # Windows command-line quoting, including paths with spaces and backslashes.
    import ctypes
    from ctypes import wintypes
    shell = ctypes.WinDLL("shell32", use_last_error=True)
    shell.CommandLineToArgvW.argtypes = [wintypes.LPCWSTR, ctypes.POINTER(ctypes.c_int)]
    shell.CommandLineToArgvW.restype = ctypes.POINTER(wintypes.LPWSTR)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.LocalFree.argtypes = [ctypes.c_void_p]
    kernel.LocalFree.restype = ctypes.c_void_p
    count = ctypes.c_int()
    argv = shell.CommandLineToArgvW(command, ctypes.byref(count))
    if not argv:
        raise UserRelayError("WebView process arguments cannot be observed")
    try:
        parts = [argv[i] for i in range(count.value)]
        for index, argument in enumerate(parts):
            if argument.startswith("--user-data-dir="):
                return argument.split("=", 1)[1]
            if argument == "--user-data-dir" and index + 1 < len(parts):
                return parts[index + 1]
    finally:
        kernel.LocalFree(argv)
    return None


def _app_stop_managed_root(app_root):
    bundle = Path(app_root).resolve()
    leaf, parent = bundle.name.casefold(), bundle.parent
    if leaf == "current":
        return parent
    if (parent.name.casefold() == "versions" and len(leaf) == 40
            and all(character in "0123456789abcdef" for character in leaf)):
        return parent.parent
    return None


def _owned_app_stop_processes(rows, app_root, webview_root, known, *, operator_sid="", own_pid=None):
    bundle = _app_stop_path(app_root)
    runtime = ntpath.join(bundle, "runtime")
    executable = ntpath.join(bundle, "label_match.exe")
    managed_root = _app_stop_managed_root(app_root)
    managed = _app_stop_path(managed_root) if managed_root is not None else None
    udf = _app_stop_path(webview_root)
    live = {}
    own_pid = os.getpid() if own_pid is None else own_pid
    for row in rows:
        pid, created = row.get("pid"), row.get("created_at")
        if type(pid) is not int or pid <= 0 or created is None:
            raise UserRelayError("app stop process identity is incomplete")
        if pid == own_pid:
            continue
        live[pid] = row
    owned, udf_arguments = {}, {}
    for pid, row in live.items():
        image, name = row.get("image"), str(row.get("name") or "").casefold()
        if name in ("python.exe", "pythonw.exe", "label_match.exe"):
            if not image:
                if operator_sid and row.get("sid") and row["sid"] != operator_sid:
                    continue
                raise UserRelayError("possible app stop process image is unavailable")
            normalized = _app_stop_path(image)
            image_runtime = ntpath.dirname(normalized)
            version_runtime = False
            if managed and image_runtime.startswith(managed + "\\"):
                parts = image_runtime[len(managed) + 1:].split("\\")
                version_runtime = (parts == ["current", "runtime"] or len(parts) == 3
                    and parts[0] == "versions" and parts[2] == "runtime"
                    and len(parts[1]) == 40
                    and all(character in "0123456789abcdef" for character in parts[1]))
            if normalized == executable or ((image_runtime == runtime or version_runtime)
                                             and name in ("python.exe", "pythonw.exe")):
                owned[pid] = row
        elif name == "msedgewebview2.exe":
            prior = known.get(pid)
            if prior and prior["created_at"] == row["created_at"]:
                owned[pid] = row
                continue
            user_data_dir = _webview_user_data_dir(str(row.get("command") or ""))
            udf_arguments[pid] = user_data_dir
            if user_data_dir and _app_stop_path(user_data_dir) == udf:
                owned[pid] = row
    # UDF-less utility/crashpad processes belong only through a live proven
    # ancestor. Creation ordering prevents a reused parent PID admitting them.
    for _ in range(len(live)):
        changed = False
        for pid, row in live.items():
            if pid in owned or str(row.get("name") or "").casefold() != "msedgewebview2.exe":
                continue
            if udf_arguments.get(pid):
                continue
            parent = owned.get(row.get("parent_pid"))
            if (parent and row["created_at"] >= parent["created_at"]
                    and row.get("session_id") == parent.get("session_id")):
                owned[pid] = row
                changed = True
        if not changed:
            break
    known.clear()
    known.update(owned)
    return list(owned.values())


def request_app_stop(direct_sync_root: str | os.PathLike[str], *, app_root=None,
                     webview_root=None, timeout_seconds=150.0, wait=time.sleep) -> dict[str, Any]:
    """Request the existing preserving GUI close and stop the persistent relay."""
    root = Path(direct_sync_root).expanduser().resolve()
    app_root = Path(app_root or _default_application_root()).resolve()
    if webview_root is None:
        from label_match_screen import shell_data_dir
        owner_environment = dict(os.environ)
        if (root.name.casefold() == "label_match" and root.parent.name.casefold() == "directsync"
                and root.parent.parent.name.casefold() == "kmtech"):
            owner_environment["LOCALAPPDATA"] = str(root.parents[2])
        webview_root = shell_data_dir(environ=owner_environment) / "webview2"
    policy_root = _app_stop_managed_root(app_root) or app_root.parent
    policy = _read_json(policy_root / "update-control" / "policy.json") or {}
    operator_sid = str(policy.get("operator_sid") or "")
    timeout_seconds = max(0.0, float(timeout_seconds))
    deadline = time.monotonic() + timeout_seconds
    known = {}
    # Capture live parent identities before asking the GUI/relay to exit.
    observation_error = None
    try:
        _owned_app_stop_processes(_app_stop_process_snapshot(timeout_seconds=min(10, max(0, deadline - time.monotonic()))), app_root, webview_root, known,
                                 operator_sid=operator_sid)
    except Exception as error:
        observation_error = type(error).__name__
    # Request-only calls still publish after the initial process observation.
    publication_deadline = deadline if timeout_seconds > 0 else time.monotonic() + 1.5
    path = app_stop_request_path(root)
    payload = {
        "schema_version": APP_STOP_SCHEMA,
        "request_id": uuid.uuid4().hex,
        "requested_at": datetime.now(timezone.utc).isoformat(),
    }
    with writer_admission("app_stop_request"):
        _write_machine_json_atomic(path, payload, deadline=publication_deadline)
        if _read_json(path) != payload:
            raise UserRelayError("app stop request readback differs")
    # Do not retain the app-request admission while waiting for relay absence.
    relay = request_user_relay_stop(root, timeout_seconds=0, deadline=publication_deadline)
    remaining = None
    while True:
        try:
            remaining = _owned_app_stop_processes(_app_stop_process_snapshot(timeout_seconds=min(10, max(0, deadline - time.monotonic()))), app_root, webview_root, known,
                                                 operator_sid=operator_sid)
            observation_error = None
        except Exception as error:
            observation_error = type(error).__name__
            remaining = None
        if relay["status"] != "ABSENT":
            lease = _acquire_relay_lease(root / "user-relay-instance")
            if lease is not None:
                (getattr(lease, "close", None) or getattr(lease, "release"))()
                relay = {**relay, "status": "ABSENT"}
                relay.pop("reason", None)
        stopped = remaining == [] and relay["status"] == "ABSENT" and observation_error is None
        if stopped or time.monotonic() >= deadline:
            return {"status": "STOPPED" if stopped else "UNKNOWN", "app_stop_request_path": str(path),
                    "relay": relay, "remaining_process_count": len(remaining) if remaining is not None else None,
                    "process_observation_error": observation_error}
        wait(min(0.25, max(0.0, deadline - time.monotonic())))


def _application_command(
    app_root: str | os.PathLike[str], *, console: bool = False
) -> list[str]:
    root = Path(app_root).expanduser().resolve()
    executable = root / "Label_Match.exe"
    if executable.is_file():
        return [str(executable)]
    portable_runtime = root / "runtime" / ("python.exe" if console else "pythonw.exe")
    portable_entrypoint = root / "app" / "label_match_relay_entry.py"
    if portable_runtime.is_file() and portable_entrypoint.is_file():
        return [
            str(portable_runtime),
            "-I",
            "-B",
            str(portable_entrypoint),
        ]
    source_entrypoint = root / "label_match_relay_entry.py"
    if source_entrypoint.is_file() and not getattr(sys, "frozen", False):
        runtime = Path(sys.executable).resolve()
        if not console:
            runtime = runtime.with_name("pythonw.exe")
            if not runtime.is_file():
                raise UserRelayError("the windowless Label_Match source host is unavailable")
        return [str(runtime), str(source_entrypoint)]
    raise UserRelayError("the hardened Label_Match application host is unavailable")


def _default_application_root() -> Path:
    module_parent = Path(__file__).resolve().parent
    portable_root = module_parent.parent
    if (
        module_parent.name.casefold() == "app"
        and (portable_root / "runtime" / "pythonw.exe").is_file()
        and (portable_root / "portable-manifest.json").is_file()
    ):
        return portable_root
    return module_parent


def _logical_application_root(root: Path) -> Path:
    from auto_update import control_root
    code = root / "app" if (root / "app").is_dir() else root
    control = control_root(code)
    return control.parent / "current" if control is not None else root


def build_user_relay_command(app_root: str | os.PathLike[str]) -> list[str]:
    root = Path(app_root).expanduser().resolve()
    from auto_update import control_root
    code = root / "app" if (root / "app").is_dir() else root
    control = control_root(code)
    if control is not None:
        fixed = control.parent / "current"
        return [str(fixed / "runtime/pythonw.exe"), "-I", "-B", str(fixed / "app/auto_update.py"), "--bootstrap", "--role", "relay", "--", USER_RELAY_MODE]
    return [*_application_command(root), USER_RELAY_MODE]


def user_relay_command_line(app_root: str | os.PathLike[str]) -> str:
    return subprocess.list2cmdline(build_user_relay_command(app_root))


def build_session_direct_sync_command(
    *,
    app_root: str | os.PathLike[str],
    direct_sync_root: str | os.PathLike[str],
    scan_source_dir: str | os.PathLike[str],
    tls_ca_bundle_path: str | os.PathLike[str] = "",
    runtime_status_path: str | os.PathLike[str] = "",
    log_path: str | os.PathLike[str] = "",
    worker_id: str = LABEL_MATCH_WORKER_ID,
) -> list[str]:
    root = Path(direct_sync_root).expanduser().resolve()
    source = Path(scan_source_dir).expanduser().resolve()
    selected_status = (
        Path(runtime_status_path).expanduser().resolve()
        if runtime_status_path
        else root / "status" / "direct_sync_relay_status.json"
    )
    selected_log = (
        Path(log_path).expanduser().resolve()
        if log_path
        else root / "logs" / "direct_sync_relay.jsonl"
    )
    selected_worker = str(worker_id or "").strip()
    if not selected_worker:
        raise ValueError("DirectSync worker id must not be empty")
    command = [
        *_application_command(app_root, console=True),
        DIRECT_SYNC_RELAY_MODE,
        "--db-path",
        str(root / "queue" / "direct_sync_relay.sqlite3"),
        "--spool-dir",
        str(root / "spool"),
        "--producer-manifest-path",
        str(root / "producer_manifest.json"),
        "--credential-path",
        str(root / "credential.json"),
        "--upload-status-dir",
        str(root / "upload_status"),
        "--runtime-status-path",
        str(selected_status),
        "--log-path",
        str(selected_log),
        "--worker-id",
        selected_worker,
        "--timeout-seconds",
        "15",
        "--operator-pause-path",
        str(root / "control" / "pause.json"),
        "--max-active-queue-count",
        "1000",
        "--max-active-queue-age-seconds",
        str(24 * 60 * 60),
        "--scan-source-dir",
        str(source),
        "--source-glob",
        LABEL_MATCH_SOURCE_GLOB,
        "--max-enqueue-files",
        "100",
        "--min-source-file-age-seconds",
        "0",
    ]
    selected_ca = str(tls_ca_bundle_path or "").strip()
    if selected_ca:
        command.extend(["--tls-ca-bundle-path", selected_ca])
    return command


def _run_command(command: list[str], timeout_seconds: int) -> dict[str, Any]:
    from auto_update import child_failure_output, launch_failure_details

    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            timeout=max(10, int(timeout_seconds)),
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except Exception as exc:
        return {
            "status": "UNKNOWN",
            "reason": "relay process did not return an exit code",
            "error_type": exc.__class__.__name__,
            "launch_failure_details": launch_failure_details(exc, include_original=True),
        }
    return {
        "status": "PASS" if completed.returncode == 0 else "FAIL",
        "returncode": completed.returncode,
        **({"child_output": child_failure_output(completed)} if completed.returncode else {}),
    }


@writer_sink("relay_child_launch")
def run_session_direct_sync_once(
    *,
    app_root: str | os.PathLike[str],
    direct_sync_root: str | os.PathLike[str],
    scan_source_dir: str | os.PathLike[str],
    reason: str,
    timeout_seconds: int = 45,
    tls_ca_bundle_path: str | os.PathLike[str] = "",
    runtime_status_path: str | os.PathLike[str] = "",
    log_path: str | os.PathLike[str] = "",
    worker_id: str = LABEL_MATCH_WORKER_ID,
) -> dict[str, Any]:
    try:
        command = build_session_direct_sync_command(
            app_root=app_root,
            direct_sync_root=direct_sync_root,
            scan_source_dir=scan_source_dir,
            tls_ca_bundle_path=tls_ca_bundle_path,
            runtime_status_path=runtime_status_path,
            log_path=log_path,
            worker_id=worker_id,
        )
    except UserRelayError as exc:
        return {"status": "FAIL", "reason": str(exc)}
    result = _run_command(command, timeout_seconds)
    result["reason"] = reason
    return result


def _registry_set(value: str) -> None:
    if os.name != "nt":
        raise UserRelayError("HKCU relay persistence is available only on Windows")
    import winreg

    with winreg.CreateKeyEx(
        winreg.HKEY_CURRENT_USER,
        USER_RELAY_RUN_KEY,
        0,
        winreg.KEY_SET_VALUE | winreg.KEY_QUERY_VALUE,
    ) as key:
        winreg.SetValueEx(key, USER_RELAY_RUN_VALUE, 0, winreg.REG_SZ, value)


def _registry_get() -> str:
    if os.name != "nt":
        raise UserRelayError("HKCU relay persistence is available only on Windows")
    import winreg

    try:
        with winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            USER_RELAY_RUN_KEY,
            0,
            winreg.KEY_QUERY_VALUE,
        ) as key:
            value, value_type = winreg.QueryValueEx(key, USER_RELAY_RUN_VALUE)
    except FileNotFoundError:
        return ""
    if value_type != winreg.REG_SZ:
        raise UserRelayError("the HKCU relay value has an unexpected registry type")
    return str(value)


def _registry_delete() -> None:
    if os.name != "nt":
        raise UserRelayError("HKCU relay persistence is available only on Windows")
    import winreg

    try:
        with winreg.OpenKey(
            winreg.HKEY_CURRENT_USER,
            USER_RELAY_RUN_KEY,
            0,
            winreg.KEY_SET_VALUE,
        ) as key:
            winreg.DeleteValue(key, USER_RELAY_RUN_VALUE)
    except FileNotFoundError:
        return


@writer_sink("user_relay_autostart_install")
def install_user_relay_autostart(
    app_root: str | os.PathLike[str],
    *,
    setter: Callable[[str], None] | None = None,
    getter: Callable[[], str] | None = None,
) -> dict[str, Any]:
    command_line = user_relay_command_line(app_root)
    (setter or _registry_set)(command_line)
    if (getter or _registry_get)() != command_line:
        raise UserRelayError("HKCU relay persistence exact readback failed")
    return {
        "status": "PASS",
        "principal": "current_user",
        "registry_hive": "HKEY_CURRENT_USER",
        "registry_key": USER_RELAY_RUN_KEY,
        "registry_value": USER_RELAY_RUN_VALUE,
        "command": command_line,
    }


@writer_sink("user_relay_autostart_remove")
def remove_user_relay_autostart(
    *,
    deleter: Callable[[], None] | None = None,
    getter: Callable[[], str] | None = None,
) -> dict[str, Any]:
    (deleter or _registry_delete)()
    if (getter or _registry_get)():
        raise UserRelayError("HKCU relay persistence removal readback failed")
    return {
        "status": "ABSENT",
        "registry_hive": "HKEY_CURRENT_USER",
        "registry_key": USER_RELAY_RUN_KEY,
        "registry_value": USER_RELAY_RUN_VALUE,
    }


def _existing_user_relay_owner(
    root: Path, direct_sync_root: Path, *, is_bootstrap: bool, allow_fast_path: bool = False,
    fast_only: bool = False,
    process_observations: dict | None = None,
) -> dict[str, Any] | None:
    from current_user_onboarding import (
        require_install_server_origin_matches, resolve_current_user_onboarding_paths,
    )
    require_install_server_origin_matches(resolve_current_user_onboarding_paths(root))
    if user_relay_stop_path(direct_sync_root).exists():
        return None
    lease = _acquire_relay_lease(direct_sync_root / "user-relay-instance")
    if lease is not None:
        lease.close()
        return None
    status = _read_json(user_relay_status_path(direct_sync_root))
    owner = _fresh_persistent_owner(
        status,
        app_root=_logical_application_root(root),
        direct_sync_root=direct_sync_root,
    )
    if owner is None or not is_bootstrap:
        return owner
    from auto_update import control_root, load_policy, update_lock, read_json, version_bundle, process_snapshot, process_matches_trial, _utc
    code = root / "app" if (root / "app").is_dir() else root
    control = control_root(code)
    if control is None:
        return None
    policy = load_policy(control)
    from contextlib import nullcontext
    with nullcontext() if fast_only else update_lock(control, policy["operator_sid"]):
        active = read_json(control / "active.json")
        state = read_json(control / "transition.json")
        expected_root = version_bundle(control, active["sha"])
        age = (datetime.now(timezone.utc) - _utc(owner["updated_at"])).total_seconds()
        if (allow_fast_path and 0 <= age <= 90
                and state.get("phase") in {"CONFIRMED", "ROLLED_BACK", "DEFERRED"}
                and os.path.normcase(str(status.get("code_root") or "")) == os.path.normcase(str(expected_root))
                and status.get("source_commit") == active["sha"]
                and status.get("sid") == policy["operator_sid"]
                and status.get("ordinary_user") is True):
            return {**owner, "fast_path": True, "source_commit": active["sha"], "sid": policy["operator_sid"]}
        if fast_only:
            return None
        from auto_update import native_process_identity
        process = native_process_identity(owner["process_id"]) if process_observations is not None else {}
        key = (str(control), active["sha"], policy["operator_sid"], owner["process_id"],
               process.get("creation_identity"), json.dumps(state, sort_keys=True))
        cached = process_observations.get(key) if process_observations is not None and process.get("status") == "LIVE" else None
        from startup_timing import startup_progress, startup_timing_step
        if cached is not None:
            rows = cached
        else:
            startup_progress("full_relay_owner_query")
            with startup_timing_step("full_relay_owner_query"):
                rows = [row for row in process_snapshot(control) if row.get("pid") == owner["process_id"]]
        if process_observations is not None and process.get("status") == "LIVE":
            process_observations[key] = rows
        if len(rows) != 1 or os.path.normcase(str(status.get("code_root") or "")) != os.path.normcase(str(expected_root)):
            return None
        row = rows[0]
        if (row.get("observable") is not True or row.get("sha") != active["sha"]
                or row.get("sid") != policy["operator_sid"] or row.get("role") != "relay"
                or row.get("ordinary_user") is not True
                or _utc(row.get("created_at")).replace(microsecond=0) > _utc(status.get("updated_at")).replace(microsecond=0)):
            return None
        if state.get("phase") in {"APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"}:
            if status.get("update_nonce") != state.get("nonce") or not process_matches_trial(row, state=state, sha=active["sha"], sid=policy["operator_sid"], role="relay"):
                return None
        elif state.get("phase") not in {"CONFIRMED", "ROLLED_BACK", "DEFERRED"}:
            return None
    return {**owner, "source_commit": active["sha"], "created_at": row["created_at"], "sid": row["sid"]}


def _reuse_existing_user_relay(
    existing_owner: Callable[[], dict[str, Any] | None],
    survival: float,
    wait: Callable[[float], None],
) -> dict[str, Any] | None:
    # A duplicate persistent relay exits cleanly when the instance lease is held.
    # Reuse only the same fresh live owner across the normal survival window;
    # zero exit by itself also covers stop-marker and ordinary loop termination.
    from startup_timing import startup_stop_checkpoint, startup_wait, startup_progress
    startup_stop_checkpoint()
    owner = existing_owner()
    startup_stop_checkpoint()
    if owner is None:
        return None
    if owner.get("fast_path") is True:
        return {
            "status": "ALIVE", "process_id": owner["process_id"], "survival_seconds": 0.0,
            "existing_relay_reused": True, "fast_path": True,
            "owner_status_updated_at": owner["updated_at"], "source_commit": owner["source_commit"],
            "owner_sid": owner["sid"],
        }
    startup_progress("full_relay_survival_wait")
    startup_wait(survival, wait)
    confirmed_owner = existing_owner()
    if confirmed_owner is None or confirmed_owner["process_id"] != owner["process_id"]:
        raise UserRelayError("existing current-user relay ownership changed during survival readback")
    startup_stop_checkpoint()
    return {
        "status": "ALIVE",
        "process_id": confirmed_owner["process_id"],
        "survival_seconds": survival,
        "existing_relay_reused": True,
        "owner_status_updated_at": confirmed_owner["updated_at"],
    }


def reuse_or_start_user_relay_process(
    app_root: str | os.PathLike[str],
    *,
    launcher: Callable[[Sequence[str]], Any] | None = None,
    survival_seconds: float = 2.0,
    bootstrap_readiness_seconds: float = 600.0,
    wait: Callable[[float], None] = time.sleep,
    allow_recent_reuse: bool = True,
) -> dict[str, Any]:
    """Reuse a proven live relay without writer admission, else start one.

    Reuse launches and writes nothing; it keeps every start_user_relay_process
    ownership check. The relay releases writer admission while its child waits on network
    (run_persistent_relay_loop), so the start probe must not gate reuse.
    """
    from current_user_onboarding import _machine_worker_context
    _machine_worker_context(app_root)
    if launcher is None and os.name == "nt":
        from current_user_onboarding import resolve_current_user_onboarding_paths

        root = Path(app_root).expanduser().resolve()
        direct_sync_root = resolve_current_user_onboarding_paths(root).direct_sync_root
        is_bootstrap = "--bootstrap" in build_user_relay_command(app_root)
        allow_fast_path = allow_recent_reuse
        process_observations = {}

        def existing_owner():
            nonlocal allow_fast_path
            recent = allow_fast_path
            allow_fast_path = False
            return _existing_user_relay_owner(
                root, direct_sync_root, is_bootstrap=is_bootstrap, allow_fast_path=recent,
                process_observations=process_observations,
            )

        reused = _reuse_existing_user_relay(
            existing_owner,
            max(0.0, float(survival_seconds)),
            wait,
        )
        if reused is not None:
            return reused
    return start_user_relay_process(
        app_root,
        launcher=launcher,
        survival_seconds=survival_seconds,
        bootstrap_readiness_seconds=bootstrap_readiness_seconds,
        wait=wait,
    )


def verify_reused_user_relay(app_root: str | os.PathLike[str], relay_start: Mapping[str, Any]) -> None:
    """Observe a fast reuse once after the GUI is visible; never start or stop a process."""
    from auto_update import _utc
    from current_user_onboarding import resolve_current_user_onboarding_paths

    root = Path(app_root).expanduser().resolve()
    direct_sync_root = resolve_current_user_onboarding_paths(root).direct_sync_root
    owner = _existing_user_relay_owner(root, direct_sync_root, is_bootstrap=True)
    if (owner is None or owner["process_id"] != relay_start["process_id"]
            or owner.get("source_commit") != relay_start["source_commit"]
            or owner.get("sid") != relay_start["owner_sid"]
            or _utc(owner["created_at"]) > _utc(relay_start["owner_status_updated_at"])):
        raise UserRelayError("existing user relay ownership did not survive startup verification")


@writer_sink("user_relay_process_start")
def start_user_relay_process(
    app_root: str | os.PathLike[str],
    *,
    launcher: Callable[[Sequence[str]], Any] | None = None,
    survival_seconds: float = 2.0,
    bootstrap_readiness_seconds: float = 600.0,
    wait: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    from current_user_onboarding import _machine_worker_context
    _machine_worker_context(app_root)
    command = build_user_relay_command(app_root)
    if launcher is not None:
        return {"status": "START_REQUESTED", "launcher_result": launcher(command)}
    if os.name != "nt":
        return {"status": "NOT_TESTED", "reason": "Windows-only user relay launch"}
    from current_user_onboarding import resolve_current_user_onboarding_paths

    root = Path(app_root).expanduser().resolve()
    direct_sync_root = resolve_current_user_onboarding_paths(root).direct_sync_root
    survival = max(0.0, float(survival_seconds))
    is_bootstrap = "--bootstrap" in command

    def existing_owner() -> dict[str, Any] | None:
        return _existing_user_relay_owner(root, direct_sync_root, is_bootstrap=is_bootstrap)

    reused = _reuse_existing_user_relay(existing_owner, survival, wait)
    if reused is not None:
        return reused
    from auto_update import RELAY_PREPARE_SECONDS, start_ordinary_process
    parent_deadline = time.monotonic() + max(
        0.0, min(float(RELAY_PREPARE_SECONDS), float(bootstrap_readiness_seconds)))
    if is_bootstrap:
        separator = command.index("--") if "--" in command else len(command)
        command = [*command[:separator], "--relay-parent-pid", str(os.getpid()),
                   "--relay-parent-deadline", repr(parent_deadline), *command[separator:]]
    process = start_ordinary_process(command, cwd=str(Path(app_root).expanduser().resolve()))
    if process.pid <= 0:
        raise UserRelayError("current-user relay launch did not return a process id")
    from startup_timing import StartupStopRequested
    try:
        return _read_started_relay(process, existing_owner, survival, parent_deadline,
                                   is_bootstrap, wait)
    except StartupStopRequested:
        if process.poll() not in (None, 0):
            raise UserRelayError("current-user relay exited unsuccessfully during stop readback") from None
        # Bootstrap may still be finishing a task mutation or protected write.
        # Its existing stop marker is checked at safe boundaries. Keep this
        # product parent (and the removal guard) alive until its child exits.
        while process.poll() is None:
            wait(0.1)
        if process.poll() != 0:
            raise UserRelayError("current-user relay exited unsuccessfully during stop cleanup") from None
        raise
    except UserRelayError as error:
        if error.cause_code:
            _record_relay_reason(error.cause_code, app_root=root, exit_code=error.exit_code)
        raise


def _read_started_relay(process, existing_owner, survival, parent_deadline, is_bootstrap, wait):
    from startup_timing import startup_stop_checkpoint, startup_wait, startup_progress
    startup_progress("full_relay_survival_wait")
    startup_wait(min(survival, max(0.0, parent_deadline - time.monotonic())) if is_bootstrap else survival, wait)
    return_code = process.poll()
    if is_bootstrap:
        owner = None
        completed = return_code == 0
        readback_deadline = min(parent_deadline, time.monotonic() + 2.0) if completed else parent_deadline
        startup_progress("full_relay_bootstrap_wait")
        while time.monotonic() < readback_deadline:
            return_code = process.poll()
            if return_code == 0 and not completed:
                completed = True
                readback_deadline = min(parent_deadline, time.monotonic() + 2.0)
            if return_code not in (None, 0):
                raise UserRelayError(
                    "RELAY_EXITED_BEFORE_RUNNING", cause_code="RELAY_EXITED_BEFORE_RUNNING",
                    exit_code=return_code,
                )
            startup_stop_checkpoint()
            owner = existing_owner()
            startup_stop_checkpoint()
            return_code = process.poll()
            if return_code == 0 and not completed:
                completed = True
                readback_deadline = min(parent_deadline, time.monotonic() + 2.0)
            if return_code == 0 and owner is None:
                owner = existing_owner()
            if return_code not in (None, 0):
                raise UserRelayError(
                    "RELAY_EXITED_BEFORE_RUNNING", cause_code="RELAY_EXITED_BEFORE_RUNNING",
                    exit_code=return_code,
                )
            if owner is not None:
                if time.monotonic() >= readback_deadline:
                    owner = None
                break
            startup_wait(min(0.1, max(0.0, readback_deadline - time.monotonic())), wait)
        if owner is None:
            if completed:
                raise UserRelayError(
                    "RELAY_EXITED_BEFORE_RUNNING", cause_code="RELAY_EXITED_BEFORE_RUNNING",
                    exit_code=0,
                )
            raise UserRelayError("bootstrap did not prove the protected active-version relay")
        if process.poll() not in (None, 0):
            raise UserRelayError("bootstrap exited unsuccessfully before relay ownership readback")
        if time.monotonic() >= parent_deadline:
            raise UserRelayError("bootstrap did not prove the protected active-version relay")
        return {"status": "ALIVE", "process_id": owner["process_id"],
                "bootstrap_process_id": process.pid, "survival_seconds": survival,
                "owner_status_updated_at": owner["updated_at"]}
    if return_code is not None:
        raise UserRelayError(
            "RELAY_EXITED_BEFORE_RUNNING", cause_code="RELAY_EXITED_BEFORE_RUNNING",
            exit_code=return_code,
        )
    return {
        "status": "ALIVE",
        "process_id": process.pid,
        "survival_seconds": survival,
    }


def _wait_with_stop_checks(
    seconds: float,
    stop_requested: Callable[[], bool],
    wait: Callable[[float], None],
) -> bool:
    remaining = max(0.0, float(seconds))
    while remaining > 0:
        if stop_requested():
            return True
        current = min(1.0, remaining)
        wait(current)
        remaining -= current
    return stop_requested()


def run_persistent_relay_loop(
    run_cycle: Callable[..., Mapping[str, Any]],
    *,
    status_path: str | os.PathLike[str],
    interval_seconds: int = DEFAULT_RETRY_INTERVAL_SECONDS,
    stop_requested: Callable[[], bool] | None = None,
    wait: Callable[[float], None] = time.sleep,
    max_cycles: int | None = None,
    metadata: Mapping[str, Any] | None = None,
    prepare_cycle: Callable[[], Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    interval = int(interval_seconds)
    if interval < 0 or interval > MAX_RETRY_INTERVAL_SECONDS:
        raise ValueError("user relay retry interval must be between 0 and 60 seconds")
    if max_cycles is not None and int(max_cycles) < 1:
        raise ValueError("max_cycles must be positive when supplied")
    selected_status_path = Path(status_path).expanduser().resolve()
    should_stop = stop_requested or (lambda: False)
    cycle_count = 0
    last_cycle: dict[str, Any] = {"status": "NOT_TESTED"}
    fixed_metadata = dict(metadata or {})
    reason_root = fixed_metadata.get("code_root") or fixed_metadata.get("app_root")
    fence_recorded = False
    running_written = False
    while not should_stop():
        try:
            update_preparation = None
            if prepare_cycle is not None:
                try:
                    update_preparation = prepare_cycle()
                except Exception as error:
                    update_preparation = error
            with writer_admission("persistent_relay_cycle"):
                cycle_count += 1
                try:
                    raw_cycle = run_cycle(update_preparation) if prepare_cycle is not None else run_cycle()
                    cycle = dict(raw_cycle) if isinstance(raw_cycle, Mapping) else {}
                    if not str(cycle.get("status") or "").strip():
                        cycle = {
                            **cycle,
                            "status": "UNKNOWN",
                            "reason": "relay cycle returned no status value",
                        }
                except (WriterFenceError, DataGenerationError):
                    raise
                except Exception as exc:
                    from auto_update import launch_failure_details
                    failure_path = selected_status_path.with_name("cycle-failure-" + uuid.uuid4().hex + ".json")
                    failure_evidence = _preserve_failure_evidence(failure_path, exc)
                    cycle = {
                        "status": "UNKNOWN",
                        "reason": "relay cycle did not return a result",
                        "error_type": exc.__class__.__name__,
                        **failure_evidence,
                        "exception_frames": [
                            {
                                "file": Path(frame.filename).name,
                                "function": frame.name,
                                "line": frame.lineno,
                            }
                            for frame in traceback.extract_tb(exc.__traceback__, limit=-8)
                        ],
                    }
                last_cycle = cycle
                _write_json_atomic(
                    selected_status_path,
                    {
                        **fixed_metadata,
                        "report_version": "label-match-user-relay-v1",
                        "status": "RUNNING",
                        "principal": "current_user",
                        "persistent_retry": True,
                        "retry_interval_seconds": interval,
                        "cycle_count": cycle_count,
                        "last_cycle": cycle,
                        "updated_at": _now(),
                    },
                )
                running_written = True
                if _relay_main_active.get():
                    _relay_main_running.set(True)
                observer = _relay_entry_running.get()
                if observer is not None:
                    observer[0] = True
        except WriterFencedError:
            if not fence_recorded:
                _record_relay_reason("RELAY_WRITER_FENCED", app_root=reason_root)
                fence_recorded = True
            if max_cycles is not None:
                cycle = {
                    "status": "FENCED",
                    "reason": "deployment writer fence denied the relay cycle",
                }
                last_cycle = cycle
                break
            if _wait_with_stop_checks(interval, should_stop, wait):
                break
            continue
        if max_cycles is not None and cycle_count >= int(max_cycles):
            break
        if _wait_with_stop_checks(interval, should_stop, wait):
            break
    final = {
        **fixed_metadata,
        "report_version": "label-match-user-relay-v1",
        "status": "STOPPED" if should_stop() else "COMPLETED",
        "principal": "current_user",
        "persistent_retry": True,
        "retry_interval_seconds": interval,
        "cycle_count": cycle_count,
        "last_cycle": last_cycle,
        "updated_at": _now(),
    }
    try:
        with writer_admission("persistent_relay_status"):
            _write_json_atomic(selected_status_path, final)
    except WriterFencedError:
        final = {
            **final,
            "status": "FENCED",
            "reason": "deployment writer fence denied the final status mutation",
        }
        if not fence_recorded:
            _record_relay_reason("RELAY_WRITER_FENCED", app_root=reason_root)
    if final["status"] != "FENCED":
        _record_relay_reason("RELAY_STOP_REQUESTED" if should_stop() else "RELAY_COMPLETED",
                             app_root=reason_root, failure_snapshot=not running_written)
    return final


def _resolve_scan_source_dir(
    explicit: str, *, data_root: Path, settings_path: Path
) -> Path:
    # Share the business-root resolver; the explicit per-run scan override and
    # the separately anchored producer identity/queue remain unchanged.
    selected = explicit or resolve_data_scope(
        environment={"LABEL_MATCH_SAVE_DIR": str(data_root)},
        settings_path=settings_path,
    )
    return Path(selected).expanduser().resolve()


def _runtime_cycle(
    *,
    app_root: Path,
    direct_sync_root: Path,
    scan_source_dir: Path,
    tls_ca_bundle_path: str = "",
    runtime_status_path: Path | None = None,
    log_path: Path | None = None,
    worker_id: str = LABEL_MATCH_WORKER_ID,
    reason: str = "PERSISTENT_USER_RELAY",
    update_preparation: Mapping[str, Any] | Exception | None = None,
) -> dict[str, Any]:
    from auto_update import launch_failure_details
    if isinstance(update_preparation, Exception):
        error = update_preparation
        failure_path = direct_sync_root / "logs" / ("update-preparation-failure-" + uuid.uuid4().hex + ".json")
        failure_evidence = _preserve_failure_evidence(failure_path, error)
        update_preparation = {"status": "DEFERRED", "error_type": type(error).__name__,
                              "launch_failure_details": launch_failure_details(error),
                              **failure_evidence}
    elif update_preparation is None:
        update_preparation = {"status": "OFF"}
    selected_status = runtime_status_path or (
        direct_sync_root / "status" / "direct_sync_relay_status.json"
    )
    process_result = run_session_direct_sync_once(
        app_root=app_root,
        direct_sync_root=direct_sync_root,
        scan_source_dir=scan_source_dir,
        reason=reason,
        tls_ca_bundle_path=tls_ca_bundle_path,
        runtime_status_path=selected_status,
        log_path=log_path or (direct_sync_root / "logs" / "direct_sync_relay.jsonl"),
        worker_id=worker_id,
    )
    runtime_status = _read_json(selected_status)
    process_status = str(process_result.get("status") or "").strip() or "UNKNOWN"
    relay_status = str((runtime_status or {}).get("status") or "").strip() or "UNKNOWN"
    return {
        "status": relay_status if process_status == "PASS" else process_status,
        "process_status": process_status,
        "process_returncode": process_result.get("returncode", "UNKNOWN"),
        "relay_status": relay_status,
        **({"process_child_output": process_result["child_output"]} if "child_output" in process_result else {}),
        **({"process_failure_details": process_result["launch_failure_details"]} if "launch_failure_details" in process_result else {}),
        **({"auto_update": update_preparation} if update_preparation["status"] != "OFF" else {}),
    }


def _acquire_relay_lease(key: str | os.PathLike[str]):
    lease = acquire_data_scope_mutex(key)
    if lease.owner:
        return lease
    lease.close()
    return None


def _pid_exists(process_id: int) -> bool:
    try:
        process_id = int(process_id)
    except (TypeError, ValueError, OverflowError):
        return False
    if process_id <= 0 or process_id > 0xFFFFFFFF:
        return False
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.GetExitCodeProcess.argtypes = (wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD))
        kernel32.GetExitCodeProcess.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
        kernel32.CloseHandle.restype = wintypes.BOOL
        handle = kernel32.OpenProcess(0x1000, False, process_id)  # QUERY_LIMITED_INFORMATION
        if not handle:
            return False  # An inaccessible PID supplies no ownership proof.
        try:
            exit_code = wintypes.DWORD()
            return bool(kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))) and exit_code.value == 259
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(process_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except (OSError, ValueError):
        return False
    return True


def _fresh_persistent_owner(
    status: Mapping[str, Any] | None,
    *,
    app_root: Path,
    direct_sync_root: Path,
    pid_exists: Callable[[int], bool] = _pid_exists,
) -> dict[str, Any] | None:
    payload = dict(status or {})
    try:
        process_id = int(payload.get("process_id") or 0)
        updated = datetime.fromisoformat(str(payload.get("updated_at") or ""))
    except (TypeError, ValueError):
        return None
    if updated.tzinfo is None:
        return None
    age = (
        datetime.now(timezone.utc) - updated.astimezone(timezone.utc)
    ).total_seconds()
    if (
        payload.get("status") != "RUNNING"
        or payload.get("worker_id") != LABEL_MATCH_WORKER_ID
        or os.path.normcase(str(payload.get("app_root") or ""))
        != os.path.normcase(str(app_root))
        or os.path.normcase(str(payload.get("direct_sync_root") or ""))
        != os.path.normcase(str(direct_sync_root))
        or age < -5
        or age > (MAX_RETRY_INTERVAL_SECONDS * 2 + 15)
        or not pid_exists(process_id)
    ):
        return None
    return {"process_id": process_id, "updated_at": updated.isoformat()}


@writer_sink("user_relay_stop_request")
def request_user_relay_stop(
    direct_sync_root: str | os.PathLike[str],
    *,
    timeout_seconds: float = 10.0,
    deadline: float | None = None,
    wait: Callable[[float], None] = time.sleep,
    lease_factory: Callable[[str | os.PathLike[str]], Any] = _acquire_relay_lease,
    marker_lease_factory: Callable[
        [str | os.PathLike[str]], Any
    ] = _acquire_relay_lease,
) -> dict[str, Any]:
    timeout_seconds = max(0.0, float(timeout_seconds))
    wait_deadline = time.monotonic() + timeout_seconds
    if deadline is None:
        deadline = wait_deadline if timeout_seconds > 0 else time.monotonic() + 1.5
    wait_deadline = min(wait_deadline, deadline)
    root = Path(direct_sync_root).expanduser().resolve()
    stop_path = user_relay_stop_path(root)
    request_id = uuid.uuid4().hex
    requested_at = _now()
    marker_lease = marker_lease_factory(root / "user-relay-stop-marker")
    if marker_lease is None:
        raise UserRelayError("relay stop marker is being changed by another process")
    try:
        if stop_path.exists():
            marker_payload = build_successor_marker(
                stop_path,
                request_id=request_id,
                requested_at=requested_at,
            )
        else:
            marker_payload = {
                "schema_version": STOP_MARKER_V1,
                "request_id": request_id,
                "requested_at": requested_at,
            }
        _write_machine_json_atomic(stop_path, marker_payload, deadline=deadline)
        marker_value, _marker_raw, marker_hash = read_stop_marker(stop_path)
        if marker_value != marker_payload:
            raise UserRelayError("relay stop marker readback differs")
    except StopMarkerLineageError as exc:
        raise UserRelayError(f"relay stop marker lineage is invalid: {exc}") from exc
    finally:
        close = getattr(marker_lease, "close", None) or getattr(
            marker_lease, "release", None
        )
        if callable(close):
            close()
    lease_key = root / "user-relay-instance"
    while True:
        lease = lease_factory(lease_key)
        if lease is not None:
            close = getattr(lease, "close", None) or getattr(lease, "release", None)
            if callable(close):
                close()
            return {
                "status": "ABSENT",
                "request_id": request_id,
                "stop_request_path": str(stop_path),
                "stop_request_sha256": marker_hash,
                "stop_marker_schema": marker_payload["schema_version"],
                "stop_marker_lineage_depth": int(
                    marker_payload.get("lineage_depth") or 0
                ),
            }
        if time.monotonic() >= wait_deadline:
            return {
                "status": "UNKNOWN",
                "request_id": request_id,
                "stop_request_path": str(stop_path),
                "stop_request_sha256": marker_hash,
                "stop_marker_schema": marker_payload["schema_version"],
                "stop_marker_lineage_depth": int(
                    marker_payload.get("lineage_depth") or 0
                ),
                "reason": "relay process absence was not proven before timeout",
            }
        wait(min(0.25, max(0.0, wait_deadline - time.monotonic())))


@writer_sink("user_relay_stop_release")
def release_user_relay_stop_marker(
    direct_sync_root: str | os.PathLike[str],
    *,
    expected_request_id: str,
    expected_sha256: str,
    marker_lease_factory: Callable[
        [str | os.PathLike[str]], Any
    ] = _acquire_relay_lease,
) -> dict[str, Any]:
    """Remove only the exact marker proven by onboarding under the writer lock."""

    root = Path(direct_sync_root).expanduser().resolve()
    stop_path = user_relay_stop_path(root)
    marker_lease = marker_lease_factory(root / "user-relay-stop-marker")
    if marker_lease is None:
        raise UserRelayError("relay stop marker is being changed by another process")
    try:
        try:
            marker, _raw, marker_hash = read_stop_marker(stop_path)
        except StopMarkerLineageError as exc:
            raise UserRelayError(f"relay stop marker release is unsafe: {exc}") from exc
        if (
            str(marker.get("request_id") or "") != str(expected_request_id or "")
            or marker_hash != str(expected_sha256 or "").lower()
        ):
            raise UserRelayError(
                "relay stop marker changed after canonical preflight"
            )
        stop_path.unlink()
        if stop_path.exists():
            raise UserRelayError("relay stop marker release readback failed")
        return {
            "status": "RELEASED",
            "request_id": str(expected_request_id),
            "sha256": marker_hash,
            "stop_request_path": str(stop_path),
        }
    finally:
        close = getattr(marker_lease, "close", None) or getattr(
            marker_lease, "release", None
        )
        if callable(close):
            close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Label_Match current-user persistent DirectSync relay"
    )
    parser.add_argument("--app-root", default="")
    parser.add_argument("--direct-sync-root", default="")
    parser.add_argument("--scan-source-dir", default="")
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=DEFAULT_RETRY_INTERVAL_SECONDS,
    )
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--request-app-stop", action="store_true")
    return parser






def main(argv: list[str] | None = None, *, credential_input=None) -> int:
    from startup_timing import StartupStopRequested
    app_root = None
    # Per-call state observes only this context's successful RUNNING write.
    # No new status schema or cross-process diagnostic protocol is required.
    running_token = _relay_main_running.set(False)
    active_token = _relay_main_active.set(True)
    try:
        args = build_parser().parse_args(argv)
        app_root = (
            Path(
                args.app_root
                or (
                    Path(sys.executable).parent
                    if getattr(sys, "frozen", False)
                    else _default_application_root()
                )
            )
            .expanduser()
            .resolve()
        )
        return _run_user_relay(args, app_root, argv, credential_input=credential_input)
    except StartupStopRequested:
        return 0
    except Exception as error:
        try:
            reason = "RELAY_UNHANDLED_EXCEPTION" if _relay_main_running.get() else "RELAY_EXITED_BEFORE_RUNNING"
            _record_relay_reason(reason, app_root=app_root, error=error)
            error._relay_exit_recorded = True
        except Exception:
            pass  # Preserve the original exception and the entry point's exit code.
        raise
    finally:
        _relay_main_running.reset(running_token)
        _relay_main_active.reset(active_token)


def _run_user_relay(args, app_root: Path, argv, *, credential_input=None) -> int:
    if args.request_app_stop:
        if args.direct_sync_root:
            direct_sync_root = Path(args.direct_sync_root)
        else:
            from current_user_onboarding import resolve_current_user_onboarding_paths
            direct_sync_root = resolve_current_user_onboarding_paths(app_root).direct_sync_root
        report = request_app_stop(direct_sync_root, app_root=app_root, timeout_seconds=APP_STOP_TIMEOUT_SECONDS)
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        return 0 if report["status"] == "STOPPED" else 1
    try:
        from label_data_generation import release_identity
        release_identity(app_root)
    except Exception:
        pass  # Existing G checks retry and deny failures in the normal flow.
    from auto_update import startup_gate, trial_stop_requested, relay_ready_metadata
    code = app_root / "app" if (app_root / "app").is_dir() else app_root
    update_result = startup_gate(code, [USER_RELAY_MODE, *(argv or [])], role="relay")
    if update_result is not None:
        _record_relay_reason("RELAY_STARTUP_HANDLED", app_root=app_root)
        return update_result
    from current_user_onboarding import (
        apply_current_user_runtime_environment,
        resolve_current_user_onboarding_paths,
    )
    from logistics_runtime_profile import (
        load_logistics_runtime_profile,
        unprotect_current_user_secret,
    )

    from label_data_generation import normalize_data_arguments
    normalize_data_arguments(args, ("direct_sync_root", "scan_source_dir"))
    selected_roots = tuple(Path(value) for value in (args.direct_sync_root, args.scan_source_dir) if value)
    check_user_data_open(app_root, selected_roots)
    paths = resolve_current_user_onboarding_paths(app_root)
    from direct_sync_runtime import prepare_credentials_from_json
    selected_direct_sync_root = Path(args.direct_sync_root) if args.direct_sync_root else paths.direct_sync_root
    if credential_input is None:
        credential_input = prepare_credentials_from_json(selected_direct_sync_root / "credential.json", app_root=app_root)
    record_user_data_open(app_root, (paths.data_root, paths.ledger_path.parent,
                                    paths.direct_sync_root, paths.queue_dir, paths.logistics_profile_path.parent,
                                    *selected_roots))
    apply_current_user_runtime_environment(paths)
    profile = load_logistics_runtime_profile(
        required=True,
        profile_path=paths.logistics_profile_path,
        decryptor=unprotect_current_user_secret,
    )
    if profile is None:
        raise UserRelayError("the current-user logistics profile is unavailable")
    direct_sync_root = (
        Path(args.direct_sync_root).expanduser().resolve()
        if args.direct_sync_root
        else paths.direct_sync_root
    )
    scan_source_dir = _resolve_scan_source_dir(
        args.scan_source_dir,
        data_root=paths.data_root,
        settings_path=paths.settings_path,
    )
    for directory in (
        direct_sync_root / "queue",
        direct_sync_root / "spool",
        direct_sync_root / "upload_status",
        direct_sync_root / "status",
        direct_sync_root / "logs",
        direct_sync_root / "control",
        scan_source_dir,
    ):
        directory.mkdir(parents=True, exist_ok=True)
    stop_path = user_relay_stop_path(direct_sync_root)
    if stop_path.exists():
        _record_relay_reason("RELAY_STOP_MARKER_PRESENT", app_root=app_root)
        return 0
    lease = _acquire_relay_lease(direct_sync_root / "user-relay-instance")
    if lease is None:
        _record_relay_reason("RELAY_INSTANCE_LEASE_UNAVAILABLE", app_root=app_root)
        return 0
    try:
        from auto_update import prepare_feed_packet

        result = run_persistent_relay_loop(
            lambda update_preparation: _runtime_cycle(
                app_root=app_root,
                direct_sync_root=direct_sync_root,
                scan_source_dir=scan_source_dir,
                tls_ca_bundle_path=profile.tls_ca_bundle_path,
                update_preparation=update_preparation,
            ),
            prepare_cycle=lambda: prepare_feed_packet(code),
            status_path=user_relay_status_path(direct_sync_root),
            interval_seconds=args.interval_seconds,
            stop_requested=lambda: stop_path.exists() or trial_stop_requested(code),
            max_cycles=1 if args.once else None,
            metadata={
                "process_id": os.getpid(),
                "worker_id": LABEL_MATCH_WORKER_ID,
                "app_root": str(_logical_application_root(app_root)),
                "code_root": str(app_root),
                "direct_sync_root": str(direct_sync_root),
                "queue_dir": str(direct_sync_root / "queue"),
                **relay_ready_metadata(code),
            },
        )
    finally:
        try:
            from producer_runtime_client import checkpoint_sqlite_database
            checkpoint_sqlite_database(direct_sync_root / "queue" / "direct_sync_relay.sqlite3")
        except Exception as exc:
            import logging
            logging.getLogger(__name__).warning("WAL shutdown cleanup failed (%s)", type(exc).__name__)
        finally:
            lease.close()
    last_status = str((result.get("last_cycle") or {}).get("status") or "UNKNOWN")
    return 1 if last_status in {"FAIL", "UNKNOWN", "runtime_error"} else 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Current-user persistence for the Label_Match DirectSync relay."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any, Callable, Mapping, Sequence
import uuid

from label_match_single_instance import acquire_data_scope_mutex


USER_RELAY_MODE = "--label-match-user-relay"
DIRECT_SYNC_RELAY_MODE = "--label-match-direct-sync-relay"
USER_RELAY_RUN_VALUE = "KMTech.LabelMatch.Relay"
USER_RELAY_RUN_KEY = r"Software\Microsoft\Windows\CurrentVersion\Run"
DEFAULT_RETRY_INTERVAL_SECONDS = 30
MAX_RETRY_INTERVAL_SECONDS = 60
USER_RELAY_STATUS_NAME = "label_match_user_relay.json"
USER_RELAY_STOP_NAME = "label_match_user_relay.stop.json"
LABEL_MATCH_SOURCE_GLOB = "포장실작업이벤트로그_*.csv"
LABEL_MATCH_WORKER_ID = "direct-sync-relay-label-match-current-user"


class UserRelayError(RuntimeError):
    """Raised when current-user relay persistence cannot be proven."""


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(dict(payload), handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
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


def user_relay_status_path(direct_sync_root: str | os.PathLike[str]) -> Path:
    return (
        Path(direct_sync_root).expanduser().resolve()
        / "status"
        / USER_RELAY_STATUS_NAME
    )


def user_relay_stop_path(direct_sync_root: str | os.PathLike[str]) -> Path:
    return (
        Path(direct_sync_root).expanduser().resolve()
        / "control"
        / USER_RELAY_STOP_NAME
    )


def _application_command(app_root: str | os.PathLike[str]) -> list[str]:
    root = Path(app_root).expanduser().resolve()
    executable = root / "Label_Match.exe"
    if executable.is_file():
        return [str(executable)]
    source_entrypoint = root / "Label_Match.py"
    if source_entrypoint.is_file() and not getattr(sys, "frozen", False):
        return [sys.executable, str(source_entrypoint)]
    raise UserRelayError("the hardened Label_Match application host is unavailable")


def build_user_relay_command(app_root: str | os.PathLike[str]) -> list[str]:
    return [*_application_command(app_root), USER_RELAY_MODE]


def user_relay_command_line(app_root: str | os.PathLike[str]) -> str:
    return subprocess.list2cmdline(build_user_relay_command(app_root))


def build_session_direct_sync_command(
    *,
    app_root: str | os.PathLike[str],
    direct_sync_root: str | os.PathLike[str],
    scan_source_dir: str | os.PathLike[str],
    tls_ca_bundle_path: str | os.PathLike[str] = "",
) -> list[str]:
    root = Path(direct_sync_root).expanduser().resolve()
    source = Path(scan_source_dir).expanduser().resolve()
    command = [
        *_application_command(app_root),
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
        str(root / "status" / "direct_sync_relay_status.json"),
        "--log-path",
        str(root / "logs" / "direct_sync_relay.jsonl"),
        "--worker-id",
        LABEL_MATCH_WORKER_ID,
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
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=max(10, int(timeout_seconds)),
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except Exception as exc:
        return {
            "status": "UNKNOWN",
            "reason": "relay process did not return an exit code",
            "error_type": exc.__class__.__name__,
        }
    return {
        "status": "PASS" if completed.returncode == 0 else "FAIL",
        "returncode": completed.returncode,
        "stdout_tail": completed.stdout[-2000:],
        "stderr_tail": completed.stderr[-2000:],
    }


def run_session_direct_sync_once(
    *,
    app_root: str | os.PathLike[str],
    direct_sync_root: str | os.PathLike[str],
    scan_source_dir: str | os.PathLike[str],
    reason: str,
    timeout_seconds: int = 45,
    tls_ca_bundle_path: str | os.PathLike[str] = "",
) -> dict[str, Any]:
    try:
        command = build_session_direct_sync_command(
            app_root=app_root,
            direct_sync_root=direct_sync_root,
            scan_source_dir=scan_source_dir,
            tls_ca_bundle_path=tls_ca_bundle_path,
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


def start_user_relay_process(
    app_root: str | os.PathLike[str],
    *,
    launcher: Callable[[Sequence[str]], Any] | None = None,
) -> dict[str, Any]:
    command = build_user_relay_command(app_root)
    if launcher is not None:
        return {"status": "START_REQUESTED", "launcher_result": launcher(command)}
    if os.name != "nt":
        return {"status": "NOT_TESTED", "reason": "Windows-only user relay launch"}
    creation_flags = (
        getattr(subprocess, "CREATE_NO_WINDOW", 0)
        | getattr(subprocess, "DETACHED_PROCESS", 0)
        | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    )
    process = subprocess.Popen(
        command,
        cwd=str(Path(app_root).expanduser().resolve()),
        close_fds=True,
        creationflags=creation_flags,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if process.pid <= 0:
        raise UserRelayError("current-user relay launch did not return a process id")
    return {"status": "START_REQUESTED", "process_id": process.pid}


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
    run_cycle: Callable[[], Mapping[str, Any]],
    *,
    status_path: str | os.PathLike[str],
    interval_seconds: int = DEFAULT_RETRY_INTERVAL_SECONDS,
    stop_requested: Callable[[], bool] | None = None,
    wait: Callable[[float], None] = time.sleep,
    max_cycles: int | None = None,
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
    while not should_stop():
        cycle_count += 1
        try:
            raw_cycle = run_cycle()
            cycle = dict(raw_cycle) if isinstance(raw_cycle, Mapping) else {}
            if not str(cycle.get("status") or "").strip():
                cycle = {
                    **cycle,
                    "status": "UNKNOWN",
                    "reason": "relay cycle returned no status value",
                }
        except Exception as exc:
            cycle = {
                "status": "UNKNOWN",
                "reason": "relay cycle did not return a result",
                "error_type": exc.__class__.__name__,
            }
        last_cycle = cycle
        _write_json_atomic(
            selected_status_path,
            {
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
        if max_cycles is not None and cycle_count >= int(max_cycles):
            break
        if _wait_with_stop_checks(interval, should_stop, wait):
            break
    final = {
        "report_version": "label-match-user-relay-v1",
        "status": "STOPPED" if should_stop() else "COMPLETED",
        "principal": "current_user",
        "persistent_retry": True,
        "retry_interval_seconds": interval,
        "cycle_count": cycle_count,
        "last_cycle": last_cycle,
        "updated_at": _now(),
    }
    _write_json_atomic(selected_status_path, final)
    return final


def _runtime_cycle(
    *,
    app_root: Path,
    direct_sync_root: Path,
    scan_source_dir: Path,
    tls_ca_bundle_path: str = "",
) -> dict[str, Any]:
    process_result = run_session_direct_sync_once(
        app_root=app_root,
        direct_sync_root=direct_sync_root,
        scan_source_dir=scan_source_dir,
        reason="PERSISTENT_USER_RELAY",
        tls_ca_bundle_path=tls_ca_bundle_path,
    )
    runtime_status = _read_json(
        direct_sync_root / "status" / "direct_sync_relay_status.json"
    )
    process_status = str(process_result.get("status") or "").strip() or "UNKNOWN"
    relay_status = str((runtime_status or {}).get("status") or "").strip() or "UNKNOWN"
    return {
        "status": relay_status if process_status == "PASS" else process_status,
        "process_status": process_status,
        "process_returncode": process_result.get("returncode", "UNKNOWN"),
        "relay_status": relay_status,
    }


def _acquire_relay_lease(key: str | os.PathLike[str]):
    lease = acquire_data_scope_mutex(key)
    if lease.owner:
        return lease
    lease.close()
    return None


def request_user_relay_stop(
    direct_sync_root: str | os.PathLike[str],
    *,
    timeout_seconds: float = 10.0,
    wait: Callable[[float], None] = time.sleep,
    lease_factory: Callable[[str | os.PathLike[str]], Any] = _acquire_relay_lease,
) -> dict[str, Any]:
    root = Path(direct_sync_root).expanduser().resolve()
    stop_path = user_relay_stop_path(root)
    request_id = uuid.uuid4().hex
    _write_json_atomic(
        stop_path,
        {
            "schema_version": "label-match-user-relay-stop-v1",
            "request_id": request_id,
            "requested_at": _now(),
        },
    )
    deadline = time.monotonic() + max(0.0, float(timeout_seconds))
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
            }
        if time.monotonic() >= deadline:
            return {
                "status": "UNKNOWN",
                "request_id": request_id,
                "stop_request_path": str(stop_path),
                "reason": "relay process absence was not proven before timeout",
            }
        wait(0.25)


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
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    app_root = Path(
        args.app_root
        or (
            Path(sys.executable).parent
            if getattr(sys, "frozen", False)
            else Path(__file__).resolve().parent
        )
    ).expanduser().resolve()
    from current_user_onboarding import (
        apply_current_user_runtime_environment,
        resolve_current_user_onboarding_paths,
    )
    from logistics_runtime_profile import (
        load_logistics_runtime_profile,
        unprotect_current_user_secret,
    )

    paths = resolve_current_user_onboarding_paths(app_root)
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
    scan_source_dir = (
        Path(args.scan_source_dir).expanduser().resolve()
        if args.scan_source_dir
        else paths.data_root
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
        return 0
    lease = _acquire_relay_lease(direct_sync_root / "user-relay-instance")
    if lease is None:
        return 0
    try:
        result = run_persistent_relay_loop(
            lambda: _runtime_cycle(
                app_root=app_root,
                direct_sync_root=direct_sync_root,
                scan_source_dir=scan_source_dir,
                tls_ca_bundle_path=profile.tls_ca_bundle_path,
            ),
            status_path=user_relay_status_path(direct_sync_root),
            interval_seconds=args.interval_seconds,
            stop_requested=stop_path.exists,
            max_cycles=1 if args.once else None,
        )
    finally:
        lease.close()
    last_status = str((result.get("last_cycle") or {}).get("status") or "UNKNOWN")
    return 1 if last_status in {"FAIL", "UNKNOWN", "runtime_error"} else 0


if __name__ == "__main__":
    raise SystemExit(main())

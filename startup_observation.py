"""Startup diagnostics consume existing gate outcomes; they never admit work."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import sys
import time
import uuid

from label_data_generation import CURRENT_VERSION
from startup_check import REASON_CATEGORIES, startup_result
from writer_session_fence import writer_sink

START_CHECK_DIRECTORY = Path(r"C:\ProgramData\KMTech\start-check")
STARTUP_BLOCKED_MESSAGE = (
    "이 PC 는 지금 새 프로그램을 열 수 없습니다. 관리자에게 알렸습니다. "
    "다른 작업대로 옮겨 주세요."
)
_identity = {}
_unknown = []
_NOTICE_MODULE_STARTED_AT = datetime.now(timezone.utc)


from label_atomic_replace import atomic_replace

def begin_startup(identity=None, *, unknown=False):
    """Retain only the identity already checked by the normal entrypoint."""
    global _identity, _unknown
    try:
        values = identity or {}
        _identity = {"version": values.get("version", CURRENT_VERSION),
                     "build_id": values.get("build_id", "")}
        _unknown = ["UNKNOWN_INSTALL"] if unknown else []
    except Exception:
        _identity, _unknown = {}, ["UNKNOWN_CHECK"]


@writer_sink("startup_check_result")
def write_startup_result(result):
    """One fixed diagnostic file, no folder creation or business admission."""
    temporary = None
    try:
        from auto_update import _assert_no_reparse_components
        directory = START_CHECK_DIRECTORY
        _assert_no_reparse_components(directory)
        if not directory.is_dir():
            return False
        safe = startup_result(result.get("reasons", ()),
                              version=result.get("version", ""),
                              build_sha=result.get("build_sha", ""))
        date = datetime.now(timezone(timedelta(hours=9))).strftime("%Y%m%d")
        destination = directory / (date + "-LABEL.json")
        _assert_no_reparse_components(destination)
        temporary = directory / ("." + destination.name + "." + uuid.uuid4().hex + ".tmp")
        with temporary.open("x", encoding="utf-8", newline="\n") as stream:
            stream.write(json.dumps(safe, ensure_ascii=True, separators=(",", ":")) + "\n")
        atomic_replace(temporary, destination)
        temporary = None
        return True
    except Exception:
        return False
    finally:
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except Exception:
                pass


def _notice_application_module():
    for name in ("__main__", "Label_Match"):
        module = sys.modules.get(name)
        if hasattr(module, "_APP_PROCESS_STARTED_AT"):
            return module
    return None


def _startup_notice_stop_requested():
    application = _notice_application_module()
    reader = getattr(application, "_startup_app_stop_requested", None)
    if reader is not None:
        return reader()
    # The early update gate runs before Label_Match defines its normal reader.
    # Use the same owner root, schema and process-start cutoff in that notice.
    from current_user_onboarding import LEGACY_DIRECT_SYNC_ROOT_ENV
    from user_relay import APP_STOP_SCHEMA, app_stop_request_path, _read_json
    configured = (os.environ.get("LABEL_MATCH_DIRECT_SYNC_ROOT")
                  or os.environ.get(LEGACY_DIRECT_SYNC_ROOT_ENV)
                  or os.environ.get("LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT"))
    local = os.environ.get("LOCALAPPDATA")
    if not configured and not local:
        return False
    root = Path(configured) if configured else Path(local) / "KMTech" / "DirectSync" / "label_match"
    path = app_stop_request_path(root)
    try:
        if not path.is_file() or path.stat().st_size > 4096:
            return False
        payload = _read_json(path) or {}
        if (payload.get("schema_version") != APP_STOP_SCHEMA
                or re.fullmatch(r"[0-9a-f]{32}", str(payload.get("request_id") or "")) is None):
            return False
        requested_at = datetime.fromisoformat(payload.get("requested_at"))
        started = getattr(application, "_APP_PROCESS_STARTED_AT", _NOTICE_MODULE_STARTED_AT)
        return requested_at.tzinfo is not None and requested_at >= started
    except (OSError, TypeError, ValueError):
        return False


def _trace_startup_notice_close(stage, **details):
    trace = getattr(_notice_application_module(), "_label_match_startup_trace", None)
    if trace is not None:
        trace(stage, **details)
        return
    # The early gate also precedes the usual trace function. Append only the
    # approved fixed fields to its existing file, without importing the app.
    record = {"stage": stage, "message": details["message"]}
    roots = [(os.environ.get("TEMP", ""), ("KMTech-startup-trace",))]
    if not os.environ.get("PYTEST_CURRENT_TEST") and not any("pytest" in str(arg).lower() for arg in sys.argv):
        roots.insert(0, (os.environ.get("LOCALAPPDATA", ""), ("KMTech", "startup-trace")))
    for base, parts in roots:
        if not base or not os.path.isabs(base):
            continue
        try:
            directory = Path(base).joinpath(*parts)
            directory.mkdir(parents=True, exist_ok=True)
            with (directory / f"Label_Match-startup-{os.getpid()}.log").open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        except Exception:
            pass


def watch_startup_notice_stop(parent, *, stop_requested=None, trace=None):
    """Close through the notice's own root; diagnostic failure cannot block it."""
    reader = _startup_notice_stop_requested if stop_requested is None else stop_requested
    record = _trace_startup_notice_close if trace is None else trace
    state = {"stopped": False}

    def poll_stop():
        if reader():
            parent.destroy()
            state["stopped"] = True
            try:
                record("APP_STOP_REQUESTED_CLOSE", message="멈춤 요청으로 닫힘")
            except Exception:
                pass
            return
        parent.after(500, poll_stop)

    parent.after(500, poll_stop)
    return state


def show_stoppable_startup_notice(title, message, *, prepare_root=None):
    """Return False only when Tk cannot be created, for the native fallback."""
    import tkinter as tk
    try:
        parent = tk.Tk()
    except Exception:
        return False
    try:
        parent.title(title)
        parent.resizable(False, False)
        tk.Label(parent, text=message, font=("맑은 고딕", 14), padx=24, pady=18).pack()
        button = tk.Button(parent, text="확인", command=parent.destroy, width=10, default="active")
        button.pack(pady=(0, 16))
        parent.protocol("WM_DELETE_WINDOW", parent.destroy)
        parent.bind("<Return>", lambda _event: parent.destroy())
        parent.bind("<Escape>", lambda _event: parent.destroy())
        parent.attributes("-topmost", True)
        parent.lift()
        parent.focus_force()
        button.focus_set()
        (watch_startup_notice_stop if prepare_root is None else prepare_root)(parent)
        parent.mainloop()
        return True
    finally:
        try:
            parent.destroy()
        except Exception:
            pass


def show_bounded_native_notice(title, message, *, warning=False):
    """Use the existing timed Windows notice API without a retry/continue result."""
    import ctypes
    from ctypes import wintypes
    notice = ctypes.WinDLL("user32", use_last_error=True).MessageBoxTimeoutW
    notice.argtypes = [wintypes.HWND, wintypes.LPCWSTR, wintypes.LPCWSTR,
                       wintypes.UINT, wintypes.WORD, wintypes.DWORD]
    notice.restype = ctypes.c_int
    flags = 0x00040000 | (0x30 if warning else 0x10)  # MB_TOPMOST | MB_OK | icon
    result = notice(None, message, title, flags, 0, 30000)
    if not result:
        raise OSError("startup notice unavailable")
    return result == 1  # Only IDOK is confirmation; timeout/close is cancellation.


def show_startup_blocked_notice(notice=None, *, prepare_root=None):
    """Fixed wording and a close button, only after an existing refusal."""
    parent = None
    try:
        import tkinter as tk
        parent = tk.Tk()
        parent.title("프로그램 시작 안내")
        parent.resizable(False, False)
        message = STARTUP_BLOCKED_MESSAGE if notice is None else notice.message
        tk.Label(parent, text=message, padx=20, pady=18).pack()
        if notice is not None:
            tk.Label(parent, text=notice.code_message, font="TkSmallCaptionFont").pack(pady=(0, 8))
            tk.Label(parent, text=notice.details, padx=20).pack(pady=(0, 16))
        tk.Button(parent, text="닫기", command=parent.destroy, width=10).pack(pady=(0, 16))
        parent.attributes("-topmost", True)
        parent.lift()
        parent.focus_force()
        (watch_startup_notice_stop if prepare_root is None else prepare_root)(parent)
        parent.mainloop()
    except Exception:
        pass
    finally:
        if parent is not None:
            try:
                parent.destroy()
            except Exception:
                pass


def observe_startup(reasons=(), *, identity=None, refused=False, notice=None, prepare_root=None):
    """Formatting/writing failures cannot change the existing startup decision."""
    try:
        started = time.monotonic()
        values = _identity if identity is None else identity
        result = startup_result([*reasons, *(_unknown if identity is None else ())],
                                version=values.get("version", CURRENT_VERSION),
                                build_sha=values.get("build_id", ""))
        if time.monotonic() - started >= 10:
            result = startup_result(["UNKNOWN_SLOW_CHECK"])
    except Exception:
        try:
            result = startup_result(["UNKNOWN_CHECK"])
        except Exception:
            result = None
    try:
        if result is not None:
            write_startup_result(result)
    except Exception:
        pass
    if refused:
        try:
            if prepare_root is not None:
                show_startup_blocked_notice(notice, prepare_root=prepare_root)
            elif notice is None:
                show_startup_blocked_notice()
            else:
                show_startup_blocked_notice(notice)
        except Exception:
            pass
    return result


def onboarding_reason(error):
    cause = getattr(error, "cause_code", "")
    if cause in REASON_CATEGORIES:
        return cause
    return ("INSTALL_MANIFEST_MISMATCH" if cause == "BOOTSTRAP_INTEGRITY_INVALID"
            else "FIRST_RUN_SETUP_REQUIRED")

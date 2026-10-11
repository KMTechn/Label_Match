"""Startup diagnostics consume existing gate outcomes; they never admit work."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
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
        os.replace(temporary, destination)
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


def show_startup_blocked_notice():
    """A single sentence and close button, only after an existing refusal."""
    parent = None
    try:
        import tkinter as tk
        parent = tk.Tk()
        parent.title("프로그램 시작 안내")
        parent.resizable(False, False)
        tk.Label(parent, text=STARTUP_BLOCKED_MESSAGE, padx=20, pady=18).pack()
        tk.Button(parent, text="닫기", command=parent.destroy, width=10).pack(pady=(0, 16))
        parent.attributes("-topmost", True)
        parent.lift()
        parent.focus_force()
        parent.mainloop()
    except Exception:
        pass
    finally:
        if parent is not None:
            try:
                parent.destroy()
            except Exception:
                pass


def observe_startup(reasons=(), *, identity=None, refused=False):
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
            show_startup_blocked_notice()
        except Exception:
            pass
    return result


def onboarding_reason(error):
    cause = getattr(error, "cause_code", "")
    if cause in REASON_CATEGORIES:
        return cause
    return ("INSTALL_MANIFEST_MISMATCH" if cause == "BOOTSTRAP_INTEGRITY_INVALID"
            else "FIRST_RUN_SETUP_REQUIRED")

import os
import sys
import json
import traceback
from datetime import datetime, date, timezone


def _label_match_startup_trace(stage, **details):
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "pid": os.getpid(),
        "stage": stage,
        "version": globals().get("APP_VERSION", "unknown"),
        "frozen": bool(getattr(sys, "frozen", False)),
        "executable": sys.executable,
        "cwd": os.getcwd(),
    }
    payload.update(details)
    filename = f"Label_Match-startup-{os.getpid()}.log"
    candidate_roots = [
        os.path.join(os.environ.get("ProgramData", r"C:\ProgramData"), "KMTech", "startup-trace"),
        os.path.join(os.environ.get("LOCALAPPDATA", ""), "KMTech", "startup-trace"),
        os.path.join(os.environ.get("TEMP", ""), "KMTech-startup-trace"),
        os.path.join(os.path.dirname(os.path.abspath(sys.executable)), "startup-trace"),
    ]
    for root in candidate_roots:
        if not root:
            continue
        try:
            os.makedirs(root, exist_ok=True)
            path = os.path.join(root, filename)
            with open(path, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
        except Exception:
            continue


_label_match_startup_trace("module_pre_imports", argv=sys.argv[:4])
_label_match_startup_trace("before_tkinter_import")
import tkinter as tk
from tkinter import ttk, messagebox, TclError, simpledialog
_label_match_startup_trace("after_tkinter_import")
from collections import defaultdict
import csv
import threading
import time
import hashlib
import re
import shutil
import tkinter.font as tkFont
import queue
import socket
_label_match_startup_trace("before_requests_import")
import requests
_label_match_startup_trace("after_requests_import")
import zipfile
import subprocess
import uuid
from urllib.parse import parse_qsl, urlparse
import base64
import binascii
import unittest

from package_logistics import (
    PackageCommandDraft,
    PackageLogisticsError,
    PackageOutbox,
    PackageOutboxProcessor,
    canonical_barcodes,
    package_client_from_env,
)
from ui.operator_layout import build_operator_layout
from ui.style_tokens import build_style_tokens
from ui.workflow_snapshot_adapter import adapt_workflow_snapshot
from ui.workflow_view_state import WorkflowNotice, present_workflow

LABEL_MATCH_SOURCE_SYSTEM = "label_match"
LABEL_MATCH_SOURCE_TRANSPORT_OR_DATASET = "legacy_packaging_csv"
LABEL_MATCH_SCAN_CONTRACT_VERSION = "label_match_current_v1"
LABEL_MATCH_PRODUCT_SAMPLE_COUNT = 3
LABEL_MATCH_MASTER_SCAN_POSITION = 1
LABEL_MATCH_FINAL_LABEL_SCAN_POSITION = LABEL_MATCH_PRODUCT_SAMPLE_COUNT + 2
LABEL_MATCH_TOTAL_SCAN_COUNT = LABEL_MATCH_FINAL_LABEL_SCAN_POSITION
LABEL_MATCH_RESULT_PASS = "통과"
LABEL_MATCH_RESULT_FAIL_MISMATCH = "불일치"
LABEL_MATCH_SAVE_DIR_ENV = "LABEL_MATCH_SAVE_DIR"
LABEL_MATCH_DEFAULT_SAVE_SUBDIR = ("KMTech", "Label_Match", "data")
LABEL_MATCH_DIRECT_SYNC_BOOTSTRAP_ENV = "LABEL_MATCH_DIRECT_SYNC_BOOTSTRAP"
LABEL_MATCH_DIRECT_SYNC_SERVER_BASE_URL_ENV = "LABEL_MATCH_DIRECT_SYNC_SERVER_BASE_URL"
LABEL_MATCH_DIRECT_SYNC_SOURCE_HOST_ID_ENV = "LABEL_MATCH_DIRECT_SYNC_SOURCE_HOST_ID"
LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT_ENV = "LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT"
LABEL_MATCH_DIRECT_SYNC_TASK_NAME_ENV = "LABEL_MATCH_DIRECT_SYNC_TASK_NAME"
LABEL_MATCH_DIRECT_SYNC_TASK_RUN_USER_ENV = "LABEL_MATCH_DIRECT_SYNC_TASK_RUN_USER"
LABEL_MATCH_DIRECT_SYNC_TASK_RUN_PASSWORD_ENV_ENV = "LABEL_MATCH_DIRECT_SYNC_TASK_RUN_PASSWORD_ENV"
LABEL_MATCH_DIRECT_SYNC_TASK_RUN_PASSWORD_FILE_ENV = "LABEL_MATCH_DIRECT_SYNC_TASK_RUN_PASSWORD_FILE"
LABEL_MATCH_DIRECT_SYNC_BOOTSTRAP_TIMEOUT_ENV = "LABEL_MATCH_DIRECT_SYNC_BOOTSTRAP_TIMEOUT_SECONDS"
LABEL_MATCH_DIRECT_SYNC_ALLOW_INTERACTIVE_TASK_FOR_LOCAL_TEST_ENV = (
    "LABEL_MATCH_DIRECT_SYNC_ALLOW_INTERACTIVE_TASK_FOR_LOCAL_TEST"
)
LABEL_MATCH_SESSION_SYNC_TRIGGER_ENV = "LABEL_MATCH_SESSION_SYNC_TRIGGER"
LABEL_MATCH_SESSION_SYNC_REQUEST_TIMEOUT_SECONDS = 15
LABEL_MATCH_SESSION_SYNC_PROCESS_TIMEOUT_SECONDS = 45
LABEL_MATCH_SESSION_SYNC_TERMINATION_GRACE_SECONDS = 5
LABEL_MATCH_APP_CLOSE_LOG_TIMEOUT_SECONDS = 10
LABEL_MATCH_APP_CLOSE_TOTAL_TIMEOUT_SECONDS = 105
_LABEL_MATCH_SESSION_SYNC_LOCK = threading.Lock()
LABEL_MATCH_AUDIO_ENABLED_ENV = "LABEL_MATCH_AUDIO_ENABLED"
LABEL_MATCH_AUTOMATED_TEST_ENV = "LABEL_MATCH_AUTOMATED_TEST"
LABEL_MATCH_LOGISTICS_MEMBERSHIP_MODE_ENV = "LABEL_MATCH_LOGISTICS_MEMBERSHIP_MODE"
LABEL_MATCH_DIRECT_SYNC_DEFAULT_SERVER_BASE_URL = "https://worker.kmtecherp.com"
LABEL_MATCH_DIRECT_SYNC_REPORT_NAME = "label_match_direct_sync_auto_bootstrap.json"
LABEL_MATCH_DIRECT_SYNC_INSTALL_REPORT_NAME = "label_match_direct_sync_install.json"
CSV_FORMULA_PREFIXES = ("=", "+", "-", "@")


def _default_label_match_save_path():
    env_save_dir = os.environ.get(LABEL_MATCH_SAVE_DIR_ENV, "").strip()
    if env_save_dir:
        return env_save_dir
    program_data_root = os.environ.get("ProgramData", r"C:\ProgramData")
    return os.path.join(program_data_root, *LABEL_MATCH_DEFAULT_SAVE_SUBDIR)


def _label_match_runtime_app_root():
    runtime = sys.executable if getattr(sys, "frozen", False) else __file__
    return os.path.dirname(os.path.abspath(runtime))


def _label_match_safe_token(value, fallback):
    text = str(value or "").strip() or fallback
    text = re.sub(r"[^A-Za-z0-9._-]+", "-", text).strip("._-")
    return (text or fallback)[:96].strip("._-") or fallback


def _label_match_machine_identity():
    if os.name == "nt":
        try:
            import winreg

            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\Cryptography") as key:
                value, _value_type = winreg.QueryValueEx(key, "MachineGuid")
                if value:
                    return str(value)
        except OSError:
            pass
    return f"{socket.gethostname()}|{uuid.getnode():012x}"


def _label_match_direct_sync_source_host_id():
    override = os.environ.get(LABEL_MATCH_DIRECT_SYNC_SOURCE_HOST_ID_ENV, "").strip()
    if override:
        return _label_match_safe_token(override, "label-match-worker").lower()
    pc_id = _label_match_safe_token(os.environ.get("COMPUTERNAME") or socket.gethostname(), "worker-pc")
    suffix = hashlib.sha256(_label_match_machine_identity().encode("utf-8")).hexdigest()[:12]
    return f"label-match-{pc_id}-{suffix}".lower()


def _label_match_direct_sync_bootstrap_enabled():
    value = os.environ.get(LABEL_MATCH_DIRECT_SYNC_BOOTSTRAP_ENV, "on").strip().lower()
    return value not in {"0", "false", "no", "off", "disabled"}


def _label_match_session_sync_trigger_enabled():
    value = os.environ.get(LABEL_MATCH_SESSION_SYNC_TRIGGER_ENV, "on").strip().lower()
    return value not in {"0", "false", "no", "off", "disabled"}


def _label_match_audio_enabled():
    if _label_match_automated_test_mode():
        return False
    value = os.environ.get(LABEL_MATCH_AUDIO_ENABLED_ENV, "on").strip().lower()
    return value not in {"0", "false", "no", "off", "disabled"}


def _label_match_automated_test_mode():
    value = os.environ.get(LABEL_MATCH_AUTOMATED_TEST_ENV, "").strip().lower()
    if value in {"1", "true", "yes", "on", "enabled"}:
        return True
    return bool(os.environ.get("PYTEST_CURRENT_TEST")) or any(
        "pytest" in str(argument or "").lower() for argument in sys.argv
    )


def _label_match_direct_sync_context(scan_source_dir, app_settings_path=""):
    source_host_id = _label_match_direct_sync_source_host_id()
    program_data_root = os.environ.get(LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT_ENV, "").strip()
    if not program_data_root:
        program_data_root = os.path.join(
            os.environ.get("ProgramData", r"C:\ProgramData"),
            "KMTech",
            "DirectSync",
            source_host_id,
        )
    program_data_root = os.path.abspath(program_data_root)
    scan_source_dir = os.path.abspath(scan_source_dir)
    task_name = os.environ.get(LABEL_MATCH_DIRECT_SYNC_TASK_NAME_ENV, "").strip()
    if not task_name:
        task_name = f"direct-sync-relay-{source_host_id}"
    status_dir = os.path.join(program_data_root, "status")
    return {
        "app_root": _label_match_runtime_app_root(),
        "app_settings_path": os.path.abspath(app_settings_path) if app_settings_path else "",
        "program_data_root": os.path.abspath(program_data_root),
        "scan_source_dir": scan_source_dir,
        "server_base_url": os.environ.get(
            LABEL_MATCH_DIRECT_SYNC_SERVER_BASE_URL_ENV,
            LABEL_MATCH_DIRECT_SYNC_DEFAULT_SERVER_BASE_URL,
        ).strip() or LABEL_MATCH_DIRECT_SYNC_DEFAULT_SERVER_BASE_URL,
        "source_host_id": source_host_id,
        "task_name": task_name,
        "status_dir": status_dir,
        "bootstrap_status_path": os.path.join(status_dir, LABEL_MATCH_DIRECT_SYNC_REPORT_NAME),
        "install_report_path": os.path.join(status_dir, LABEL_MATCH_DIRECT_SYNC_INSTALL_REPORT_NAME),
        "registration_report_path": os.path.join(status_dir, "label_match_worker_pc_registration.json"),
        "manifest_path": os.path.join(program_data_root, "producer_manifest.json"),
        "credential_path": os.path.join(program_data_root, "credential.json"),
        "runtime_status_path": os.path.join(status_dir, "direct_sync_relay_status.json"),
    }


def _label_match_bind_current_log_source(context, data_manager):
    bound = dict(context)
    try:
        source_file = os.path.abspath(data_manager._get_log_filepath())
        scan_source_dir = os.path.abspath(context["scan_source_dir"])
    except (AttributeError, KeyError, OSError, TypeError, ValueError):
        bound["scan_source_binding_error"] = "current_log_path_unavailable"
        return bound
    if os.path.dirname(source_file) != scan_source_dir:
        bound["scan_source_binding_error"] = "current_log_outside_scan_source"
        return bound
    source_name = os.path.basename(source_file)
    if not source_name.startswith("포장실작업이벤트로그_") or not source_name.lower().endswith(".csv"):
        bound["scan_source_binding_error"] = "current_log_filename_not_allowed"
        return bound
    bound["scan_source_file"] = source_file
    bound.pop("scan_source_binding_error", None)
    return bound


def _label_match_json_file(path):
    try:
        with open(path, "r", encoding="utf-8-sig") as handle:
            payload = json.load(handle)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _label_match_write_json(path, payload, *, raise_on_error=False):
    temp_path = None
    try:
        directory = os.path.dirname(path) or "."
        os.makedirs(directory, exist_ok=True)
        temp_path = os.path.join(
            directory,
            f".{os.path.basename(path)}.tmp-{os.getpid()}-{uuid.uuid4().hex}",
        )
        with open(temp_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        temp_path = None
    except Exception as exc:
        if temp_path:
            try:
                os.remove(temp_path)
            except OSError:
                pass
        print(f"direct-sync bootstrap status write failed: {exc}")
        if raise_on_error:
            raise


def _label_match_subprocess_creationflags():
    if os.name == "nt":
        return getattr(subprocess, "CREATE_NO_WINDOW", 0)
    return 0


def _label_match_terminate_process_tree(process, *, deadline_monotonic=None):
    if deadline_monotonic is None:
        deadline_monotonic = time.monotonic() + LABEL_MATCH_SESSION_SYNC_TERMINATION_GRACE_SECONDS

    def remaining_seconds():
        return max(0.0, deadline_monotonic - time.monotonic())

    report = {"attempted": True, "tree_terminated": False, "method": ""}
    if process.poll() is not None:
        report.update({"tree_terminated": True, "method": "already_exited"})
        return report
    if os.name == "nt":
        try:
            remaining = remaining_seconds()
            if remaining <= 0:
                raise subprocess.TimeoutExpired("taskkill.exe", 0)
            completed = subprocess.run(
                ["taskkill.exe", "/PID", str(process.pid), "/T", "/F"],
                check=False,
                capture_output=True,
                text=True,
                timeout=remaining,
                creationflags=_label_match_subprocess_creationflags(),
            )
            report.update({
                "method": "taskkill_tree",
                "taskkill_returncode": completed.returncode,
            })
        except Exception as exc:
            report.update({"method": "taskkill_tree_failed", "error_type": exc.__class__.__name__})
    if process.poll() is None:
        try:
            process.kill()
            if not report["method"]:
                report["method"] = "process_kill"
        except Exception as exc:
            report.setdefault("error_type", exc.__class__.__name__)
    remaining = remaining_seconds()
    if remaining > 0:
        try:
            process.wait(timeout=remaining)
        except Exception:
            pass
    report["tree_terminated"] = process.poll() is not None
    return report


def _label_match_run_bounded_subprocess(command, *, timeout_seconds, env):
    def output_text(value):
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return str(value or "")

    started = time.monotonic()
    creationflags = _label_match_subprocess_creationflags()
    if os.name == "nt":
        creationflags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        creationflags=creationflags,
    )
    try:
        stdout, stderr = process.communicate(timeout=max(0.1, float(timeout_seconds)))
        return {
            "returncode": int(process.returncode or 0),
            "stdout": stdout or "",
            "stderr": stderr or "",
            "timed_out": False,
            "process_tree_termination": {"attempted": False, "tree_terminated": True, "method": "not_required"},
            "elapsed_seconds": round(time.monotonic() - started, 3),
        }
    except subprocess.TimeoutExpired as exc:
        termination_deadline = time.monotonic() + LABEL_MATCH_SESSION_SYNC_TERMINATION_GRACE_SECONDS
        termination = _label_match_terminate_process_tree(
            process,
            deadline_monotonic=termination_deadline,
        )
        stdout = output_text(exc.stdout)
        stderr = output_text(exc.stderr)
        remaining = max(0.0, termination_deadline - time.monotonic())
        try:
            if remaining <= 0:
                raise subprocess.TimeoutExpired(command, 0)
            final_stdout, final_stderr = process.communicate(timeout=remaining)
            stdout = output_text(final_stdout) or stdout
            stderr = output_text(final_stderr) or stderr
        except Exception:
            for stream in (process.stdout, process.stderr):
                try:
                    if stream is not None:
                        stream.close()
                except Exception:
                    pass
        return {
            "returncode": int(process.returncode if process.returncode is not None else -1),
            "stdout": stdout or "",
            "stderr": stderr or "",
            "timed_out": True,
            "process_tree_termination": termination,
            "elapsed_seconds": round(time.monotonic() - started, 3),
        }


def _label_match_direct_sync_tool_command(context):
    tools_dir = os.path.join(context["app_root"], "tools")
    install_pack_exe_candidates = [
        os.path.join(
            tools_dir,
            "direct_sync_relay_install_pack",
            "direct_sync_relay_install_pack.exe",
        ),
        os.path.join(tools_dir, "direct_sync_relay_install_pack.exe"),
    ]
    install_pack_script = os.path.join(tools_dir, "direct_sync_relay_install_pack.py")
    for install_pack_exe in install_pack_exe_candidates:
        if os.path.isfile(install_pack_exe):
            return [install_pack_exe]
    if os.path.isfile(install_pack_script):
        python_exe = "" if getattr(sys, "frozen", False) else sys.executable
        if not python_exe:
            python_exe = shutil.which("python") or shutil.which("py") or ""
        if python_exe:
            return [python_exe, install_pack_script]
    return []


def _label_match_python_exe_for_runner():
    candidates = [
        os.environ.get("KMTECH_PYTHON_EXE", ""),
    ]
    if not getattr(sys, "frozen", False):
        candidates.append(sys.executable)
    if os.name == "nt":
        candidates.extend([
            r"C:\Program Files\Python312\python.exe",
            r"C:\Program Files\Python314\python.exe",
        ])
    candidates.extend([
        shutil.which("python") or "",
        shutil.which("py") or "",
    ])
    for candidate in candidates:
        if candidate and os.path.isfile(candidate):
            return os.path.abspath(candidate)
    return ""


def _label_match_optional_tool_exe(context, filename):
    path = os.path.join(context["app_root"], "tools", filename)
    return path if os.path.isfile(path) else ""


def _label_match_registration_verified(context):
    payload = _label_match_json_file(context["registration_report_path"])
    return bool(
        payload.get("server_registration_verified")
        or payload.get("status") in {"SELF_ENROLLMENT_REGISTERED", "already_enrolled"}
        or payload.get("enrollment_status") in {"registered", "already_enrolled"}
    )


def _label_match_recent_runtime_status(context, max_age_seconds=7 * 24 * 60 * 60):
    path = context["runtime_status_path"]
    try:
        if not os.path.isfile(path):
            return False
        if time.time() - os.path.getmtime(path) > max_age_seconds:
            return False
        payload = _label_match_json_file(path)
        return payload.get("error_code", "") == "" and payload.get("status") in {"idle", "ok", "PASS", "running"}
    except Exception:
        return False


def _label_match_existing_direct_sync_task_name(context):
    if os.name != "nt":
        return ""
    powershell = os.path.join(
        os.environ.get("SystemRoot", r"C:\Windows"),
        "System32",
        "WindowsPowerShell",
        "v1.0",
        "powershell.exe",
    )
    target_root = context["program_data_root"].replace("/", "\\").lower()
    source_host_id = context["source_host_id"].lower()
    ps_script = (
        "$items = @(Get-ScheduledTask -TaskName 'direct-sync-relay*' -ErrorAction SilentlyContinue | "
        "ForEach-Object { $taskName = $_.TaskName; foreach ($action in $_.Actions) { "
        "[PSCustomObject]@{ TaskName = $taskName; Execute = $action.Execute; Arguments = $action.Arguments } } }); "
        "@($items) | ConvertTo-Json -Compress"
    )
    if os.path.isfile(powershell):
        try:
            completed = subprocess.run(
                [powershell, "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps_script],
                check=False,
                capture_output=True,
                text=True,
                timeout=8,
                creationflags=_label_match_subprocess_creationflags(),
            )
            if completed.returncode == 0 and completed.stdout.strip():
                payload = json.loads(completed.stdout)
                rows = payload if isinstance(payload, list) else [payload]
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    task_name = str(row.get("TaskName") or "")
                    text = " ".join(
                        str(row.get(key) or "")
                        for key in ("TaskName", "Execute", "Arguments")
                    ).replace("/", "\\").lower()
                    if (
                        task_name == context["task_name"]
                        or source_host_id in text
                        or target_root in text
                    ):
                        return task_name
        except Exception as exc:
            print(f"direct-sync scheduled task query failed: {exc}")
    try:
        completed = subprocess.run(
            ["schtasks.exe", "/Query", "/TN", context["task_name"]],
            check=False,
            capture_output=True,
            text=True,
            timeout=8,
            creationflags=_label_match_subprocess_creationflags(),
        )
        if completed.returncode == 0:
            return context["task_name"]
    except Exception:
        pass
    return ""


def _label_match_direct_sync_ready(context):
    if not (os.path.isfile(context["manifest_path"]) and os.path.isfile(context["credential_path"])):
        return False
    if not _label_match_registration_verified(context):
        return False
    if not _label_match_install_report_ready(context):
        return False
    return bool(
        _label_match_existing_direct_sync_task_name(context)
        or _label_match_recent_runtime_status(context)
    )


def _label_match_install_report_ready(context):
    report = _label_match_json_file(context["install_report_path"])
    if report.get("status") != "PASS":
        return False
    try:
        report_root = os.path.abspath(str(report.get("program_data_root") or ""))
        expected_root = os.path.abspath(context["program_data_root"])
        if os.path.normcase(report_root) != os.path.normcase(expected_root):
            return False
        source_scan = report.get("source_scan") or {}
        report_scan_dir = os.path.abspath(str(source_scan.get("scan_source_dir") or ""))
        expected_scan_dir = os.path.abspath(context["scan_source_dir"])
        if os.path.normcase(report_scan_dir) != os.path.normcase(expected_scan_dir):
            return False
        if source_scan.get("enabled") is not False:
            baseline = report.get("source_scan_baseline_result") or {}
            if not baseline:
                return False
            if int(baseline.get("returncode") or 0) != 0:
                return False
    except Exception:
        return False
    return True


def _label_match_run_direct_sync_task(context):
    if os.name != "nt":
        return {"status": "SKIPPED", "reason": "scheduled tasks are Windows-only"}
    try:
        completed = subprocess.run(
            ["schtasks.exe", "/Run", "/TN", context["task_name"]],
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
            creationflags=_label_match_subprocess_creationflags(),
        )
        return {
            "status": "PASS" if completed.returncode == 0 else "FAIL",
            "returncode": completed.returncode,
            "stdout": completed.stdout[-1000:],
            "stderr": completed.stderr[-1000:],
        }
    except Exception as exc:
        return {"status": "FAIL", "error": str(exc)}


def _label_match_direct_sync_runtime_paths(context):
    root = os.path.abspath(context["program_data_root"])
    return {
        "db_path": os.path.join(root, "queue", "direct_sync_relay.sqlite3"),
        "spool_dir": os.path.join(root, "spool"),
        "upload_status_dir": os.path.join(root, "upload_status"),
        "runtime_status_path": context["runtime_status_path"],
        "log_path": os.path.join(root, "logs", "direct_sync_relay.jsonl"),
        "operator_pause_path": os.path.join(root, "control", "pause.json"),
    }


def _label_match_direct_sync_runner_command(context, *, min_source_file_age_seconds=0):
    if context.get("scan_source_binding_error"):
        raise ValueError("bound direct-sync source is unavailable")
    tools_dir = os.path.join(context["app_root"], "tools")
    runner_exe = os.path.join(tools_dir, "direct_sync_relay_runner.exe")
    runner_script = os.path.join(tools_dir, "direct_sync_relay_runner.py")
    if os.path.isfile(runner_exe):
        command = [runner_exe]
    elif os.path.isfile(runner_script):
        python_exe = _label_match_python_exe_for_runner()
        if not python_exe:
            return []
        command = [python_exe, runner_script]
    else:
        return []

    source_glob = "*.csv"
    scan_source_file = str(context.get("scan_source_file") or "").strip()
    if scan_source_file:
        scan_source_file = os.path.abspath(scan_source_file)
        scan_source_dir = os.path.abspath(context["scan_source_dir"])
        if os.path.dirname(scan_source_file) != scan_source_dir:
            raise ValueError("bound direct-sync source file is outside the scan source directory")
        source_glob = os.path.basename(scan_source_file)
        if not source_glob.startswith("포장실작업이벤트로그_") or not source_glob.lower().endswith(".csv"):
            raise ValueError("bound direct-sync source filename is outside the allowlist")

    paths = _label_match_direct_sync_runtime_paths(context)
    command.extend([
        "--db-path",
        paths["db_path"],
        "--spool-dir",
        paths["spool_dir"],
        "--producer-manifest-path",
        context["manifest_path"],
        "--credential-path",
        context["credential_path"],
        "--upload-status-dir",
        paths["upload_status_dir"],
        "--runtime-status-path",
        paths["runtime_status_path"],
        "--log-path",
        paths["log_path"],
        "--worker-id",
        f"{context['source_host_id']}-session-sync",
        "--timeout-seconds",
        str(LABEL_MATCH_SESSION_SYNC_REQUEST_TIMEOUT_SECONDS),
        "--operator-pause-path",
        paths["operator_pause_path"],
        "--scan-source-dir",
        context["scan_source_dir"],
        "--source-glob",
        source_glob,
        "--max-enqueue-files",
        "1",
        "--min-source-file-age-seconds",
        str(max(0, int(min_source_file_age_seconds or 0))),
    ])
    return command


def _label_match_current_delta_ack_report(context, *, runtime_status_mtime_before_ns=0):
    path = context.get("runtime_status_path", "")
    try:
        stat_result = os.stat(path)
        if runtime_status_mtime_before_ns and stat_result.st_mtime_ns <= runtime_status_mtime_before_ns:
            return {"status": "FAIL", "error_code": "runtime_status_not_fresh"}
        payload = _label_match_json_file(path)
    except Exception as exc:
        return {"status": "FAIL", "error_code": "runtime_status_unavailable", "error_type": exc.__class__.__name__}
    targeted = payload.get("targeted_drain_results")
    if not isinstance(targeted, list) or not targeted:
        return {
            "status": "FAIL",
            "error_code": "current_delta_targeted_ack_missing",
            "scan_enqueued_count": int(payload.get("scan_enqueued_count") or 0),
        }
    scan_enqueued_count = int(payload.get("scan_enqueued_count") or 0)
    summaries = []
    verified = scan_enqueued_count > 0 and len(targeted) == scan_enqueued_count
    for item in targeted:
        target_relay_id = str(item.get("target_relay_id") or "")
        acked_relay_id = str(item.get("acked_relay_id") or "")
        item_status = str(item.get("status") or "")
        current_verified = bool(target_relay_id and acked_relay_id == target_relay_id and item_status == "acked")
        verified = verified and current_verified
        summaries.append({
            "target_relay_id": target_relay_id,
            "acked_relay_id": acked_relay_id,
            "status": item_status,
            "current_target_verified": current_verified,
        })
    return {
        "status": "PASS" if verified else "FAIL",
        "error_code": "" if verified else "current_delta_targeted_ack_failed",
        "scan_enqueued_count": scan_enqueued_count,
        "targeted_drain_results": summaries,
    }


def _label_match_run_session_direct_sync_once(
    context,
    *,
    reason="TRAY_COMPLETE",
    deadline_monotonic=None,
):
    if not _label_match_session_sync_trigger_enabled():
        return {"status": "SKIPPED", "reason": "session sync trigger disabled"}
    try:
        command = _label_match_direct_sync_runner_command(context, min_source_file_age_seconds=0)
    except (KeyError, OSError, ValueError) as exc:
        return {
            "status": "FAIL",
            "reason": reason,
            "error": "direct-sync source binding is invalid",
            "error_code": "direct_sync_source_binding_invalid",
            "error_type": exc.__class__.__name__,
        }
    if not command:
        return {"status": "SKIPPED", "reason": "direct-sync relay runner is missing"}
    env = os.environ.copy()
    env[LABEL_MATCH_SAVE_DIR_ENV] = context["scan_source_dir"]
    runtime_status_path = context.get("runtime_status_path", "")
    try:
        runtime_status_mtime_before_ns = os.stat(runtime_status_path).st_mtime_ns
    except OSError:
        runtime_status_mtime_before_ns = 0
    process_budget_seconds = float(LABEL_MATCH_SESSION_SYNC_PROCESS_TIMEOUT_SECONDS)
    if deadline_monotonic is not None:
        process_budget_seconds = min(
            process_budget_seconds,
            max(0.0, deadline_monotonic - time.monotonic()),
        )
    timeout_seconds = process_budget_seconds - LABEL_MATCH_SESSION_SYNC_TERMINATION_GRACE_SECONDS
    if timeout_seconds <= 0:
        return {
            "status": "FAIL",
            "reason": reason,
            "error": "session sync shutdown deadline exhausted",
            "error_code": "session_sync_deadline_exhausted",
        }
    try:
        completed = _label_match_run_bounded_subprocess(
            command,
            timeout_seconds=timeout_seconds,
            env=env,
        )
        ack_report = _label_match_current_delta_ack_report(
            context,
            runtime_status_mtime_before_ns=runtime_status_mtime_before_ns,
        )
        passed = (
            not completed["timed_out"]
            and completed["returncode"] == 0
            and ack_report.get("status") == "PASS"
        )
        return {
            "status": "PASS" if passed else "FAIL",
            "reason": reason,
            "returncode": completed["returncode"],
            "stdout": completed["stdout"][-1000:],
            "stderr": completed["stderr"][-1000:],
            "timed_out": completed["timed_out"],
            "process_tree_termination": completed["process_tree_termination"],
            "elapsed_seconds": completed["elapsed_seconds"],
            "current_delta_ack": ack_report,
        }
    except Exception as exc:
        return {"status": "FAIL", "reason": reason, "error": str(exc)}


def _label_match_write_session_direct_sync_result(context, *, reason, result, write_latest=True):
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    payload = {
        "report_version": "label-match-session-direct-sync-trigger-v1",
        "app_version": APP_VERSION,
        "generated_at": generated_at,
        "reason": reason,
        "source_host_id": context["source_host_id"],
        "scan_source_dir": context["scan_source_dir"],
        "result": result,
    }
    reason_key = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(reason)).strip("_").lower() or "unknown"
    latest_path = os.path.join(context["status_dir"], "label_match_session_direct_sync_trigger.json")
    reason_path = os.path.join(
        context["status_dir"],
        f"label_match_session_direct_sync_trigger_{reason_key}.json",
    )
    _label_match_write_json(reason_path, payload, raise_on_error=True)
    if write_latest:
        _label_match_write_json(latest_path, payload, raise_on_error=True)
    return {
        "latest_report_path": latest_path if write_latest else "",
        "reason_report_path": reason_path,
    }


def _label_match_run_and_record_session_direct_sync(
    context,
    *,
    reason,
    deadline_monotonic=None,
):
    lock_timeout = float(LABEL_MATCH_SESSION_SYNC_PROCESS_TIMEOUT_SECONDS + 10)
    if deadline_monotonic is not None:
        lock_timeout = min(lock_timeout, max(0.0, deadline_monotonic - time.monotonic()))
    acquired = lock_timeout > 0 and _LABEL_MATCH_SESSION_SYNC_LOCK.acquire(timeout=lock_timeout)
    if not acquired:
        result = {"status": "FAIL", "reason": reason, "error": "session sync sequence lock timeout"}
        try:
            evidence = _label_match_write_session_direct_sync_result(
                context,
                reason=reason,
                result=result,
                write_latest=False,
            )
            return {**result, "evidence": evidence}
        except Exception as exc:
            _label_match_startup_trace(
                "session_direct_sync_evidence_failed",
                reason=reason,
                error=str(exc),
            )
            return {**result, "evidence_error": str(exc)}
    try:
        run_kwargs = {"reason": reason}
        if deadline_monotonic is not None:
            run_kwargs["deadline_monotonic"] = deadline_monotonic
        result = _label_match_run_session_direct_sync_once(context, **run_kwargs)
        try:
            evidence = _label_match_write_session_direct_sync_result(
                context,
                reason=reason,
                result=result,
            )
            return {**result, "evidence": evidence}
        except Exception as exc:
            failed_result = {**result, "status": "FAIL", "evidence_error": str(exc)}
            try:
                _label_match_write_session_direct_sync_result(
                    context,
                    reason=reason,
                    result=failed_result,
                    write_latest=False,
                )
            except Exception:
                pass
            _label_match_startup_trace(
                "session_direct_sync_evidence_failed",
                reason=reason,
                error=str(exc),
            )
            return failed_result
    finally:
        _LABEL_MATCH_SESSION_SYNC_LOCK.release()


def _label_match_start_session_direct_sync(context, *, reason="TRAY_COMPLETE"):
    def worker():
        _label_match_run_and_record_session_direct_sync(context, reason=reason)

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    return thread


def _label_match_auto_bootstrap_direct_sync(context):
    started_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    base_report = {
        "report_version": "label-match-direct-sync-auto-bootstrap-v1",
        "app_version": APP_VERSION,
        "started_at": started_at,
        "source_host_id": context["source_host_id"],
        "task_name": context["task_name"],
        "program_data_root": context["program_data_root"],
        "scan_source_dir": context["scan_source_dir"],
        "server_base_url": context["server_base_url"],
    }
    if not _label_match_direct_sync_bootstrap_enabled():
        _label_match_write_json(context["bootstrap_status_path"], {**base_report, "status": "DISABLED"})
        return
    if os.name != "nt":
        _label_match_write_json(context["bootstrap_status_path"], {**base_report, "status": "SKIPPED", "reason": "windows_only"})
        return
    if _label_match_direct_sync_ready(context):
        _label_match_write_json(context["bootstrap_status_path"], {**base_report, "status": "READY"})
        return

    command = _label_match_direct_sync_tool_command(context)
    if not command:
        _label_match_write_json(
            context["bootstrap_status_path"],
            {**base_report, "status": "BLOCKED", "blocked_reason": "direct-sync install pack tool is missing"},
        )
        return
    runner_exe = _label_match_optional_tool_exe(context, "direct_sync_relay_runner.exe")
    registration_exe = _label_match_optional_tool_exe(context, "register_label_match_worker_pc.exe")
    python_exe = ""
    if not runner_exe or not registration_exe:
        python_exe = _label_match_python_exe_for_runner()
    if (not runner_exe or not registration_exe) and not python_exe:
        _label_match_write_json(
            context["bootstrap_status_path"],
            {
                **base_report,
                "status": "BLOCKED",
                "blocked_reason": "bundled direct-sync executables are incomplete and python.exe fallback is unavailable",
            },
        )
        return
    allow_interactive_task_for_local_test = os.environ.get(
        LABEL_MATCH_DIRECT_SYNC_ALLOW_INTERACTIVE_TASK_FOR_LOCAL_TEST_ENV,
        "",
    ).strip().lower() in {"1", "true", "yes", "on"}
    task_run_user = os.environ.get(LABEL_MATCH_DIRECT_SYNC_TASK_RUN_USER_ENV, "").strip()
    task_run_password_env = os.environ.get(
        LABEL_MATCH_DIRECT_SYNC_TASK_RUN_PASSWORD_ENV_ENV,
        "",
    ).strip()
    task_run_password_file = os.environ.get(
        LABEL_MATCH_DIRECT_SYNC_TASK_RUN_PASSWORD_FILE_ENV,
        "",
    ).strip()
    password_source_count = int(bool(task_run_password_env)) + int(bool(task_run_password_file))
    if not allow_interactive_task_for_local_test and (not task_run_user or password_source_count != 1):
        _label_match_write_json(
            context["bootstrap_status_path"],
            {
                **base_report,
                "status": "BLOCKED",
                "blocked_reason": (
                    "production direct-sync bootstrap requires a task run user and exactly one "
                    "password env-name or password-file setting"
                ),
            },
        )
        return

    args = command + [
        "--self-enroll",
        "--app-root",
        context["app_root"],
        "--server-base-url",
        context["server_base_url"],
        "--program-data-root",
        context["program_data_root"],
        "--scan-source-dir",
        context["scan_source_dir"],
        "--task-name",
        context["task_name"],
        "--report-path",
        context["install_report_path"],
        "--apply",
    ]
    if python_exe:
        args.extend(["--python-exe", python_exe])
    if runner_exe:
        args.extend(["--runner-exe", runner_exe])
    if context["app_settings_path"]:
        args.extend(["--app-settings-path", context["app_settings_path"]])
    if registration_exe:
        args.extend(["--registration-exe", registration_exe])
    if task_run_user:
        args.extend(["--task-run-user", task_run_user])
    if task_run_password_env:
        args.extend(["--task-run-password-env", task_run_password_env])
    if task_run_password_file:
        args.extend(["--task-run-password-file", task_run_password_file])
    if allow_interactive_task_for_local_test:
        args.append("--allow-interactive-task-for-local-test")

    env = os.environ.copy()
    env[LABEL_MATCH_SAVE_DIR_ENV] = context["scan_source_dir"]
    try:
        timeout_seconds = max(30, int(os.environ.get(LABEL_MATCH_DIRECT_SYNC_BOOTSTRAP_TIMEOUT_ENV, "180") or "180"))
    except ValueError:
        timeout_seconds = 180
    try:
        completed = subprocess.run(
            args,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            env=env,
            creationflags=_label_match_subprocess_creationflags(),
        )
        run_task_result = _label_match_run_direct_sync_task(context) if completed.returncode == 0 else None
        _label_match_write_json(
            context["bootstrap_status_path"],
            {
                **base_report,
                "status": "PASS" if completed.returncode == 0 else "BLOCKED",
                "returncode": completed.returncode,
                "install_report_path": context["install_report_path"],
                "stdout_tail": completed.stdout[-2000:],
                "stderr_tail": completed.stderr[-2000:],
                "run_task_result": run_task_result,
            },
        )
    except Exception as exc:
        _label_match_write_json(
            context["bootstrap_status_path"],
            {**base_report, "status": "BLOCKED", "blocked_reason": str(exc)},
        )


def _csv_formula_safe_cell(value):
    text = "" if value is None else str(value)
    formula_probe = text.lstrip()
    if formula_probe and formula_probe[0] in CSV_FORMULA_PREFIXES:
        return "'" + text
    return text


def _plan_b_dispatch_key(event_type):
    return f"{LABEL_MATCH_SOURCE_SYSTEM}|{LABEL_MATCH_SOURCE_TRANSPORT_OR_DATASET}|{event_type}"


def _label_match_barcode_role(scan_position):
    try:
        position = int(scan_position)
    except Exception:
        return "unknown"
    if position == LABEL_MATCH_MASTER_SCAN_POSITION:
        return "material_master_label"
    if LABEL_MATCH_MASTER_SCAN_POSITION < position < LABEL_MATCH_FINAL_LABEL_SCAN_POSITION:
        return "product"
    if position == LABEL_MATCH_FINAL_LABEL_SCAN_POSITION:
        return "final_packaging_label"
    return "unknown"


def _label_match_barcode_projection(raw_barcode, parsed_barcode, scan_position):
    role = _label_match_barcode_role(scan_position)
    product_barcode = raw_barcode if role == "product" else None
    return {
        "scan_contract_version": LABEL_MATCH_SCAN_CONTRACT_VERSION,
        "scan_position": int(scan_position) if str(scan_position or "").isdigit() else scan_position,
        "barcode_role": role,
        "raw_barcode": raw_barcode,
        "parsed_barcode": parsed_barcode,
        "product_barcode": product_barcode,
        "barcode_projection_status": "INCLUDED" if role == "product" else "ROLE_NOT_PRODUCT",
        "barcode_exclusion_reason_code": None if role == "product" else "NON_PRODUCT_BARCODE_ROLE",
    }


def _label_match_packaging_set_identity(pc_id, set_id):
    return f"{LABEL_MATCH_SOURCE_SYSTEM}|{pc_id}|{set_id}"


def _label_match_tray_complete_result(details):
    source = details or {}
    for key in ("final_result", "result_display", "result"):
        value = source.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    if source.get("has_error_or_reset"):
        return LABEL_MATCH_RESULT_FAIL_MISMATCH
    return LABEL_MATCH_RESULT_PASS


def _label_match_tray_complete_passed(details):
    return _label_match_tray_complete_result(details) == LABEL_MATCH_RESULT_PASS


def _label_match_manual_complete_block_reason(current_set_info):
    current = current_set_info or {}
    if current.get("exact_rescan_active"):
        return "manual_complete_blocked_during_exact_rescan"
    scan_count = len(current.get("raw") or current.get("parsed") or [])
    if scan_count < 2:
        return "manual_complete_requires_product_scan"
    if scan_count >= LABEL_MATCH_TOTAL_SCAN_COUNT:
        return "manual_complete_only_for_partial_sets"
    if current.get("has_error_or_reset") or current.get("error_count", 0):
        return "manual_complete_blocked_after_error"
    return None


def _label_match_manual_complete_allowed(current_set_info):
    return _label_match_manual_complete_block_reason(current_set_info) is None


def _label_match_decode_possible_base64_label(raw_value):
    text = str(raw_value or "").strip()
    if not text or "|" in text or len(text) <= 20:
        return text
    try:
        temp_b64 = text.replace('-', '+').replace('_', '/')
        padded_b64 = temp_b64 + '=' * (-len(temp_b64) % 4)
        decoded = base64.b64decode(padded_b64).decode('utf-8')
        return decoded if '|' in decoded and '=' in decoded else text
    except (binascii.Error, UnicodeDecodeError):
        return text


def _label_match_parse_new_format_fields(raw_value):
    decoded = _label_match_decode_possible_base64_label(raw_value)
    if '|' not in decoded or '=' not in decoded:
        return None
    try:
        fields = {
            key.strip().upper(): value.strip()
            for key, value in (
                item.split('=', 1)
                for item in decoded.split('|')
                if '=' in item
            )
        }
    except Exception:
        return None
    if str(fields.get("SRC") or "").strip().upper() == "KMTECH_INPUT_TAG":
        item_code = str(fields.get("CLC") or fields.get("ITEM") or fields.get("ITEM_CODE") or "").strip()
        phase = str(fields.get("PHS") or "").strip()
        if not item_code or not phase:
            return None
        normalized = dict(fields)
        normalized["CLC"] = item_code
        normalized.setdefault("SPC", str(fields.get("ITEM_NAME") or fields.get("ITEM") or item_code).strip())
        fields = normalized
    if str(fields.get("CLC") or "").strip().upper() == "INSPECTION":
        item_code = str(fields.get("ITEM") or fields.get("ITEM_CODE") or "").strip()
        if not item_code:
            return None
        normalized = dict(fields)
        normalized["CLC"] = item_code
        normalized.setdefault("SPC", str(fields.get("ITEM_NAME") or item_code).strip())
        normalized.setdefault("PHS", str(fields.get("PHASE") or "INSPECTION").strip())
        if not normalized.get("QT") and fields.get("QTY"):
            normalized["QT"] = str(fields["QTY"]).strip()
        fields = normalized
    if not all(fields.get(key) for key in ('CLC', 'SPC', 'PHS')):
        return None
    return fields


def _label_match_display_fields(raw_value):
    """Return lenient key/value fields for presentation only."""

    raw_text = str(raw_value or "").strip()
    decoded = (
        _label_match_decode_possible_base64_label(raw_text)
        if raw_text.isascii()
        else raw_text
    )
    if "=" not in decoded:
        return {}
    fields = {}
    for part in decoded.split("|"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        key = key.strip().upper()
        value = value.strip()
        if key and value:
            fields[key] = value
    return fields


def _label_match_compact_display_token(value, max_chars=22):
    text = re.sub(r"\s+", " ", str(value or "").strip())
    limit = max(8, int(max_chars))
    if len(text) <= limit:
        return text
    tail = max(5, limit // 2 - 1)
    head = max(2, limit - tail - 1)
    return f"{text[:head]}…{text[-tail:]}"


def _label_match_operator_item_code(raw_value, parsed_value):
    # ``parsed`` is the already accepted comparison value.  A raw product
    # barcode can contain other CLC-like text, so it must never override the
    # authoritative item code used by the workflow.
    parsed_text = str(parsed_value or "").strip()
    parsed_fields = _label_match_display_fields(parsed_text)
    parsed_item_code = str(
        parsed_fields.get("CLC")
        or parsed_fields.get("ITEM_CODE")
        or parsed_fields.get("ITEM")
        or ""
    ).strip()
    if parsed_item_code:
        return parsed_item_code
    if parsed_text and "|" not in parsed_text and "=" not in parsed_text:
        return parsed_text

    raw_fields = _label_match_display_fields(raw_value)
    raw_item_code = str(
        raw_fields.get("CLC")
        or raw_fields.get("ITEM_CODE")
        or raw_fields.get("ITEM")
        or ""
    ).strip()
    if raw_item_code:
        return raw_item_code

    match = re.search(r"(?<![A-Z0-9])[A-Z]{3}\d{10}(?![A-Z0-9])", str(raw_value or "").upper())
    return match.group(0) if match else ""


def _label_match_operator_scan_identifier(raw_value, item_code, scan_position):
    raw_text = str(raw_value or "").strip()
    decoded = (
        _label_match_decode_possible_base64_label(raw_text)
        if raw_text.isascii()
        else raw_text
    )
    fields = _label_match_display_fields(decoded)
    product_priorities = (
        ("SERIAL", "S/N"),
        ("SN", "S/N"),
        ("SNO", "S/N"),
        ("ITG", "ID"),
        ("LBL", "라벨"),
        ("WID", "ID"),
        ("LOT", "LOT"),
        ("TRACE", "ID"),
        ("6D", "6D"),
        ("BND", "BND"),
    )
    final_priorities = (
        ("LBL", "라벨"),
        ("SERIAL", "S/N"),
        ("SN", "S/N"),
        ("SNO", "S/N"),
        ("6D", "6D"),
        ("LOT", "LOT"),
        ("ITG", "ID"),
        ("WID", "ID"),
        ("BND", "BND"),
        ("TRACE", "ID"),
    )
    priorities = (
        final_priorities
        if int(scan_position or 0) == LABEL_MATCH_FINAL_LABEL_SCAN_POSITION
        else product_priorities
    )
    for key, label in priorities:
        value = str(fields.get(key) or "").strip()
        if value:
            return label, _label_match_compact_display_token(value)

    date_match = re.search(
        r"(?:^|[|\x1d]|<GS>)6D=?([0-9]{6,8})(?=$|[|\x1d]|<GS>)",
        decoded,
        flags=re.IGNORECASE,
    )
    if date_match:
        return "6D", date_match.group(1)

    raw_identifier_source = str(decoded or "").strip()
    if not raw_identifier_source:
        return "", ""
    fingerprint = hashlib.sha256(
        raw_identifier_source.encode("utf-8", errors="replace")
    ).hexdigest()[:12].upper()
    return "ID", f"#{fingerprint}"


def _label_match_operator_scan_summary(raw_value, parsed_value, scan_position):
    """Build a concise list value while keeping the accepted raw value elsewhere."""

    item_code = _label_match_operator_item_code(raw_value, parsed_value)
    if int(scan_position or 0) == LABEL_MATCH_MASTER_SCAN_POSITION:
        return item_code or "-"

    label, identifier = _label_match_operator_scan_identifier(
        raw_value,
        item_code,
        scan_position,
    )
    base = item_code or "-"
    if not identifier:
        return base
    return f"{base} · {label} {identifier}"


def _label_match_parse_sealed_transfer_qr(raw_value):
    """Parse the authoritative sealed-transfer QR without treating it as a product scan."""
    decoded = _label_match_decode_possible_base64_label(raw_value)
    if "|" not in decoded or "=" not in decoded:
        return None
    fields = {
        key.strip().upper(): value.strip()
        for key, value in (
            part.split("=", 1) for part in decoded.split("|") if "=" in part
        )
    }
    if fields.get("TRF") != "1":
        return None
    required = ("BND", "AUTH_SCOPE", "CLC", "QT", "HSH", "EPOCH", "PLANE", "PE")
    if any(not fields.get(key) for key in required):
        raise ValueError("sealed transfer QR is missing required fields")
    try:
        quantity = int(fields["QT"])
        authority_epoch = int(fields["EPOCH"])
        plane_epoch = int(fields["PE"])
    except (TypeError, ValueError) as exc:
        raise ValueError("sealed transfer QR quantity/epoch fields must be integers") from exc
    if quantity < 1 or authority_epoch < 0 or plane_epoch < 1:
        raise ValueError("sealed transfer QR quantity/epoch values are invalid")
    digest = fields["HSH"].lower()
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError("sealed transfer QR membership hash must be SHA-256")
    plane = fields["PLANE"].upper()
    if plane not in {"SHADOW_CANDIDATE", "AUTHORITATIVE"}:
        raise ValueError("sealed transfer QR ledger plane is invalid")
    return {
        **fields,
        "QT": quantity,
        "HSH": digest,
        "EPOCH": authority_epoch,
        "PLANE": plane,
        "PE": plane_epoch,
    }


def _label_match_package_draft(current_set_info, *, item_code):
    current = current_set_info or {}
    raw = list(current.get("raw") or [])
    if len(raw) != LABEL_MATCH_TOTAL_SCAN_COUNT:
        raise PackageLogisticsError("authoritative packaging requires the complete five-scan set")
    transfer = _label_match_parse_sealed_transfer_qr(raw[0])
    configured_mode = os.environ.get(
        LABEL_MATCH_LOGISTICS_MEMBERSHIP_MODE_ENV, "INHERIT_ALL"
    ).strip().upper()
    exact_rescan = tuple(current.get("exact_rescan_barcodes") or ())
    if current.get("exact_rescan_complete"):
        configured_mode = "EXACT_RESCAN"
    elif transfer:
        configured_mode = "INHERIT_ALL"
    if transfer:
        if str(transfer["CLC"]) != str(item_code):
            raise PackageLogisticsError("sealed transfer QR item differs from the packaging item")
        source_bundle_id = str(transfer["BND"])
        source_scope = str(transfer["AUTH_SCOPE"])
        source_label = str(raw[0])
        source_hint = ""
        expected_count = int(transfer["QT"])
        expected_hash = str(transfer["HSH"])
        expected_authority_epoch = int(transfer["EPOCH"])
        expected_plane = str(transfer["PLANE"])
        expected_plane_epoch = int(transfer["PE"])
    else:
        if configured_mode != "EXACT_RESCAN":
            raise PackageLogisticsError(
                "legacy master labels require a separate FULL EXACT_RESCAN; three QA samples are insufficient"
            )
        target_count = int(current.get("exact_rescan_target_count") or 0)
        if (
            not current.get("exact_rescan_complete")
            or target_count < 1
            or len(exact_rescan) != target_count
        ):
            raise PackageLogisticsError("FULL EXACT_RESCAN is incomplete")
        source_bundle_id = str(current.get("exact_rescan_source_bundle_id") or "").strip()
        legacy_fields = _label_match_parse_new_format_fields(raw[0]) or {}
        source_scope = ""
        source_label = str(
            legacy_fields.get("WID") or legacy_fields.get("PHS_EXTERNAL_ID") or ""
        ).strip()
        source_input_tag_id = str(legacy_fields.get("ITG") or "").strip()
        source_hint = str(legacy_fields.get("BND") or "").strip()
        expected_count = target_count
        expected_hash = ""
        expected_authority_epoch = 0
        expected_plane = ""
        expected_plane_epoch = 0
    if transfer:
        source_input_tag_id = ""
    return PackageCommandDraft.build(
        set_id=str(current.get("id") or ""),
        item_code=str(item_code or ""),
        source_bundle_id=source_bundle_id,
        source_external_label=source_label,
        source_input_tag_id=source_input_tag_id,
        source_bundle_hint=source_hint,
        source_authority_scope_id=source_scope,
        expected_member_count=expected_count,
        expected_membership_hash=expected_hash,
        expected_authority_epoch=expected_authority_epoch,
        expected_ledger_plane=expected_plane,
        expected_plane_epoch=expected_plane_epoch,
        external_label=str(raw[-1]),
        membership_mode=configured_mode,
        sample_barcodes=raw[1:1 + LABEL_MATCH_PRODUCT_SAMPLE_COUNT],
        exact_rescan_barcodes=exact_rescan,
    )


def _normalize_barcode_for_exact_rescan(value):
    values = canonical_barcodes((value,))
    return values[0] if values else ""


def _label_match_new_format_identity_key(raw_value):
    try:
        transfer = _label_match_parse_sealed_transfer_qr(raw_value)
    except ValueError:
        transfer = None
    if transfer:
        return (
            f"TRF=1|BND={transfer['BND']}|AUTH_SCOPE={transfer['AUTH_SCOPE']}|"
            f"EPOCH={transfer['EPOCH']}|PLANE={transfer['PLANE']}|PE={transfer['PE']}"
        )
    fields = _label_match_parse_new_format_fields(raw_value)
    if not fields:
        return None
    return f"CLC={fields['CLC']}|SPC={fields['SPC']}|PHS={fields['PHS']}"


def _label_match_inspection_trace_from_master_label(raw_value):
    fields = _label_match_parse_new_format_fields(raw_value) or {}
    trace = {
        "input_tag_id": str(fields.get("ITG") or "").strip(),
        "input_tag_label_id": str(fields.get("LBL") or "").strip(),
        "input_tag_core_hash": str(fields.get("HSH_CORE") or "").strip(),
        "input_tag_label_hash": str(fields.get("HSH_LABEL") or fields.get("HSH") or "").strip(),
        "master_label_phase": str(fields.get("PHS") or "").strip(),
    }
    if trace["input_tag_id"]:
        trace["inspection_session_key"] = trace["input_tag_id"]
    identity_key = _label_match_new_format_identity_key(raw_value)
    if identity_key:
        trace["master_label_identity_key"] = identity_key
    return {key: value for key, value in trace.items() if value}


def _label_match_unique_master_index_keys(raw_value):
    keys = {str(raw_value or "")}
    identity_key = _label_match_new_format_identity_key(raw_value)
    if identity_key:
        keys.add(identity_key)
    return {key for key in keys if key}


def _label_match_reusable_input_master_label(raw_value):
    fields = _label_match_parse_new_format_fields(raw_value) or {}
    return (
        str(fields.get("SRC") or "").strip().upper() == "KMTECH_INPUT_TAG"
        and str(fields.get("PHS") or "").strip() == "2"
    )


def _label_match_duplicate_index_barcodes(details):
    source = details or {}
    if not _label_match_tray_complete_passed(source):
        return set()
    raw_scans = list(source.get("scanned_product_barcodes") or [])
    if not raw_scans:
        return set()
    indexed = set(raw_scans[1:])
    first_scan = raw_scans[0]
    if _label_match_first_scan_is_unique_master(source) and not _label_match_reusable_input_master_label(first_scan):
        indexed.update(_label_match_unique_master_index_keys(first_scan))
    return indexed


def _label_match_first_scan_is_unique_master(details):
    source = details or {}
    raw_scans = list(source.get("scanned_product_barcodes") or [])
    if not raw_scans:
        return False
    first_scan = str(raw_scans[0] or "")
    return bool(
        source.get("is_unique_master_label")
        or source.get("item_name_override")
        or _label_match_new_format_identity_key(first_scan)
    )


def _label_match_unique_master_labels_equivalent(left, right):
    left_keys = _label_match_unique_master_index_keys(left)
    right_keys = _label_match_unique_master_index_keys(right)
    return bool(left_keys & right_keys)


def _label_match_parse_datetime(value):
    if isinstance(value, datetime):
        return value
    if isinstance(value, str) and value.strip():
        return datetime.fromisoformat(value)
    raise TypeError("expected datetime or ISO datetime string")


def _enrich_label_match_event(event_type, details, pc_id):
    enriched = dict(details or {})
    enriched.setdefault("source_system", LABEL_MATCH_SOURCE_SYSTEM)
    enriched.setdefault("source_transport_or_dataset", LABEL_MATCH_SOURCE_TRANSPORT_OR_DATASET)
    enriched.setdefault("raw_event_name", event_type)
    enriched.setdefault("canonical_event_name", event_type)
    enriched.setdefault("dispatch_key", _plan_b_dispatch_key(event_type))
    enriched.setdefault("identity_class", "LEGACY_FALLBACK")
    enriched.setdefault("integrity_requirement", "UNSIGNED_LEGACY_ALLOWED")
    enriched.setdefault("integrity_status", "UNSIGNED_LEGACY")
    enriched.setdefault("parser_mapping_version", "label-match-plan-b-v1")
    if "scan_pos" in enriched:
        projection = _label_match_barcode_projection(
            enriched.get("raw_input") or enriched.get("raw") or "",
            enriched.get("parsed") or enriched.get("raw_input") or enriched.get("raw") or "",
            enriched.get("scan_pos"),
        )
        enriched.update({k: v for k, v in projection.items() if k not in enriched})
    if event_type == "SCAN_OK":
        scan_position = enriched.get("scan_position") or enriched.get("scan_pos")
        if not scan_position:
            scan_position = 0
        projection = _label_match_barcode_projection(
            enriched.get("raw") or enriched.get("raw_input") or "",
            enriched.get("parsed") or enriched.get("raw") or "",
            scan_position,
        )
        enriched.update({k: v for k, v in projection.items() if k not in enriched})
    set_id = enriched.get("set_id") or enriched.get("cancelled_set_id")
    if set_id:
        enriched.setdefault("packaging_set_identity", _label_match_packaging_set_identity(pc_id, set_id))
    if event_type == "TRAY_COMPLETE":
        raw_scans = list(enriched.get("scanned_product_barcodes") or [])
        parsed_scans = list(enriched.get("parsed_product_barcodes") or raw_scans)
        if raw_scans:
            master_label_fields = _label_match_parse_new_format_fields(raw_scans[0]) or {}
            if master_label_fields:
                enriched.setdefault("master_label_fields", master_label_fields)
                enriched.setdefault("master_label_identity_key", _label_match_new_format_identity_key(raw_scans[0]))
            inspection_trace = _label_match_inspection_trace_from_master_label(raw_scans[0])
            if any(inspection_trace.get(key) for key in ("input_tag_id", "input_tag_label_id", "input_tag_core_hash", "input_tag_label_hash")):
                for key in ("input_tag_id", "input_tag_label_id", "input_tag_core_hash", "input_tag_label_hash"):
                    if key in inspection_trace:
                        enriched.setdefault(key, inspection_trace[key])
                if inspection_trace.get("input_tag_id"):
                    enriched.setdefault("source_session_id", inspection_trace["input_tag_id"])
                enriched.setdefault("inspection_trace", inspection_trace)
        barcode_roles = [
            _label_match_barcode_projection(
                raw_scans[index] if index < len(raw_scans) else "",
                parsed_scans[index] if index < len(parsed_scans) else "",
                index + 1,
            )
            for index in range(max(len(raw_scans), len(parsed_scans)))
        ]
        enriched.setdefault("scan_contract_version", LABEL_MATCH_SCAN_CONTRACT_VERSION)
        enriched.setdefault("barcode_roles", barcode_roles)
        enriched.setdefault(
            "product_sample_barcodes",
            [row["product_barcode"] for row in barcode_roles if row.get("product_barcode")],
        )
        if not _label_match_tray_complete_passed(enriched):
            enriched.setdefault("quantity_basis", "PACKAGING_SET")
            enriched.setdefault("measure_code", "PACKAGING_SET_COUNT")
            enriched["packaging_set_count"] = 0
            enriched["downstream_count_excluded"] = True
            enriched.setdefault("downstream_count_exclusion_reason", "LABEL_MATCH_FAILED_OR_MISMATCH")
        elif enriched.get("is_partial_submission"):
            enriched.setdefault("quantity_basis", "PARTIAL_SUBMISSION")
            enriched.setdefault("measure_code", "PACKAGING_SET_COUNT")
            enriched.setdefault("packaging_set_count", 0)
            enriched.setdefault("downstream_count_excluded", True)
            enriched.setdefault("downstream_count_exclusion_reason", "PARTIAL_MANUAL_COMPLETION")
        else:
            enriched.setdefault("quantity_basis", "PACKAGING_SET")
            enriched.setdefault("measure_code", "PACKAGING_SET_COUNT")
            enriched.setdefault("packaging_set_count", 1)
            enriched.setdefault("downstream_count_excluded", False)
        enriched.setdefault("packaging_piece_qty", None)
        enriched.setdefault("confidence", "EVENT_PROJECTION")
    if event_type in {"SET_DELETED", "TRAY_COMPLETION_CANCELLED"}:
        affected = (
            enriched.get("affected_completed_packaging_set_identity")
            or enriched.get("packaging_set_identity")
        )
        original_details = enriched.get("original_details") or enriched.get("details") or {}
        if not affected and isinstance(original_details, dict):
            original_set_id = original_details.get("set_id")
            if original_set_id:
                affected = _label_match_packaging_set_identity(pc_id, original_set_id)
        if affected:
            enriched["affected_completed_packaging_set_identity"] = affected
    return enriched

# #####################################################################
# 자동 업데이트 설정 (Auto-Updater Configuration)
# #####################################################################
REPO_OWNER = "KMTechn"
REPO_NAME = "Label_Match"
APP_VERSION = "v2.0.36" # private update feed release
_label_match_startup_trace("module_loaded", argv=sys.argv[:4])
UPDATE_PROVIDER_ENV = "LABEL_MATCH_UPDATE_PROVIDER"
UPDATE_MANIFEST_URL_ENV = "LABEL_MATCH_UPDATE_MANIFEST_URL"
UPDATE_MANIFEST_SIGNATURE_URL_ENV = "LABEL_MATCH_UPDATE_MANIFEST_SIGNATURE_URL"
UPDATE_MANIFEST_PUBLIC_KEY_ENV = "LABEL_MATCH_UPDATE_MANIFEST_PUBLIC_KEY"
UPDATE_CHANNEL_ENV = "LABEL_MATCH_UPDATE_CHANNEL"
UPDATE_PROVIDER_GITHUB = "github"
UPDATE_PROVIDER_PRIVATE_MANIFEST = "private_manifest"
UPDATE_PROVIDER_OFF = "off"
UPDATE_MANIFEST_SCHEMA_VERSION = "kmtech-private-update-manifest-v1"
UPDATE_MANIFEST_VERSION = 1
UPDATE_DEFAULT_CHANNEL = "stable"
UPDATE_APP_ID = "Label_Match"
UPDATE_PC_ID_ENV = "LABEL_MATCH_UPDATE_PC_ID"
UPDATE_ALLOWED_INSTALL_STRATEGIES = {"manual", "robocopy_backup_then_mirror", "replace_exe", "none"}
UPDATE_DIRECT_GITHUB_ARTIFACT_HOSTS = {"objects.githubusercontent.com", "github-releases.githubusercontent.com"}
UPDATE_GITHUB_UPDATE_HOSTS = {"api.github.com", "github.com", "www.github.com"}
UPDATE_SECRET_QUERY_KEYS = {
    "access_token",
    "api_key",
    "client_secret",
    "github_token",
    "pat",
    "private_key",
    "sig",
    "signature",
    "token",
}
UPDATE_SECRET_QUERY_PREFIXES = ("x_amz_", "x_goog_")


def _parse_update_version(version):
    match = re.match(r"^v?(\d+(?:\.\d+){1,3})", str(version or "").strip(), flags=re.IGNORECASE)
    if not match:
        raise ValueError(f"Invalid update version: {version!r}")
    return tuple(int(part) for part in match.group(1).split("."))


def _is_update_version_newer(candidate_version, current_version=None):
    if current_version is None:
        current_version = APP_VERSION
    left = _parse_update_version(candidate_version)
    right = _parse_update_version(current_version)
    width = max(len(left), len(right))
    return left + (0,) * (width - len(left)) > right + (0,) * (width - len(right))


def _load_update_settings():
    try:
        path_resolver = globals().get("resource_path")
        if callable(path_resolver):
            settings_path = path_resolver(os.path.join("config", "app_settings.json"))
        else:
            settings_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "app_settings.json")
        with open(settings_path, "r", encoding="utf-8") as handle:
            settings = json.load(handle)
        update_settings = settings.get("update_settings", {})
        return update_settings if isinstance(update_settings, dict) else {}
    except Exception:
        return {}


def _get_update_provider():
    settings = _load_update_settings()
    provider = os.environ.get(UPDATE_PROVIDER_ENV) or settings.get("provider") or UPDATE_PROVIDER_OFF
    return str(provider).strip().lower()


def _get_update_channel():
    settings = _load_update_settings()
    channel = os.environ.get(UPDATE_CHANNEL_ENV) or settings.get("channel") or UPDATE_DEFAULT_CHANNEL
    return str(channel).strip().lower()


def _get_update_manifest_url():
    settings = _load_update_settings()
    url = os.environ.get(UPDATE_MANIFEST_URL_ENV) or settings.get("manifest_url") or ""
    return str(url).strip()


def _get_update_manifest_signature_url(manifest_url):
    settings = _load_update_settings()
    url = os.environ.get(UPDATE_MANIFEST_SIGNATURE_URL_ENV) or settings.get("manifest_signature_url") or ""
    return str(url).strip() or f"{manifest_url}.sig"


def _get_update_manifest_public_key():
    settings = _load_update_settings()
    key = os.environ.get(UPDATE_MANIFEST_PUBLIC_KEY_ENV) or settings.get("manifest_public_key") or ""
    return str(key).strip()


def _is_sha256(value):
    return isinstance(value, str) and re.fullmatch(r"[A-Fa-f0-9]{64}", value.strip()) is not None


def _can_apply_updates():
    return bool(getattr(sys, "frozen", False))


def _assert_https_update_url(url, *, require_zip=False):
    parsed = urlparse(str(url or ""))
    if parsed.scheme.lower() != "https" or not parsed.netloc:
        raise ValueError("Update URL must be HTTPS")
    if parsed.username or parsed.password:
        raise ValueError("Update URL must not include userinfo")
    if parsed.fragment:
        raise ValueError("Update URL must not include fragments")
    if require_zip and not parsed.path.lower().endswith(".zip"):
        raise ValueError("Update artifact URL must point to a .zip file")
    for key, _value in parse_qsl(parsed.query, keep_blank_values=True):
        normalized_key = key.lower().replace("-", "_")
        if normalized_key in UPDATE_SECRET_QUERY_KEYS or normalized_key.startswith(UPDATE_SECRET_QUERY_PREFIXES):
            raise ValueError("Update URL must not contain raw token query parameters")


def _is_direct_github_artifact_url(url):
    parsed = urlparse(str(url or ""))
    host = (parsed.hostname or "").lower()
    path = parsed.path.lower()
    if host in {"github.com", "www.github.com"} and "/releases/download/" in path:
        return True
    if host == "api.github.com" and "/releases/assets/" in path:
        return True
    return host in UPDATE_DIRECT_GITHUB_ARTIFACT_HOSTS


def _is_github_hosted_update_url(url):
    parsed = urlparse(str(url or ""))
    host = (parsed.hostname or "").lower()
    return host in UPDATE_GITHUB_UPDATE_HOSTS or host.endswith(".githubusercontent.com")


def _validate_relative_manifest_path(value, field_name):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Update manifest {field_name} must be a non-empty relative path")
    normalized = value.replace("\\", "/")
    if normalized.startswith("/") or re.match(r"^[A-Za-z]:", normalized):
        raise ValueError(f"Update manifest {field_name} must be relative")
    if any(part in {"", ".", ".."} or ":" in part for part in normalized.split("/")):
        raise ValueError(f"Update manifest {field_name} contains an unsafe path")


def _canonical_update_pc_id():
    pc_id = os.environ.get(UPDATE_PC_ID_ENV) or os.environ.get("COMPUTERNAME") or socket.gethostname()
    text = str(pc_id or "").strip().lower()
    if not text:
        raise ValueError("Update rollout requires a non-empty PC id")
    return text


def _rollout_bucket(app_id, channel, version, pc_id):
    seed = f"{app_id}|{channel.lower()}|{version}|{pc_id.strip().lower()}".encode("utf-8")
    return int(hashlib.sha256(seed).hexdigest()[:8], 16) % 100


def _rollout_allows_current_pc(manifest):
    rollout = manifest.get("rollout")
    if not isinstance(rollout, dict):
        raise ValueError("Update manifest rollout must be an object")
    for key in ("allow_pc_ids", "deny_pc_ids"):
        if key not in rollout or not isinstance(rollout.get(key), list) or not all(isinstance(item, str) for item in rollout.get(key)):
            raise ValueError(f"Update manifest rollout.{key} must be a list of strings")
    percentage = rollout.get("percentage")
    if type(percentage) is not int or not 0 <= percentage <= 100:
        raise ValueError("Update manifest rollout.percentage must be an integer from 0 to 100")
    pc_id = _canonical_update_pc_id()
    deny_pc_ids = {item.strip().lower() for item in rollout["deny_pc_ids"] if item.strip()}
    allow_pc_ids = {item.strip().lower() for item in rollout["allow_pc_ids"] if item.strip()}
    if pc_id in deny_pc_ids:
        return False
    if pc_id in allow_pc_ids:
        return True
    if percentage == 0:
        return False
    if percentage == 100:
        return True
    return _rollout_bucket(manifest["app_id"], manifest["channel"], manifest["version"], pc_id) < percentage


def _validate_private_update_manifest_policy(manifest, expected_channel):
    if manifest.get("schema_version") != UPDATE_MANIFEST_SCHEMA_VERSION:
        raise ValueError("Unsupported update manifest schema_version")
    if manifest.get("manifest_version") != UPDATE_MANIFEST_VERSION:
        raise ValueError("Unsupported update manifest_version")
    if manifest.get("app_id") != UPDATE_APP_ID:
        raise ValueError("Update manifest app_id does not match Label_Match")
    if manifest.get("package_id") != UPDATE_APP_ID:
        raise ValueError("Update manifest package_id does not match Label_Match")
    channel = str(manifest.get("channel", "")).strip().lower()
    if not channel:
        raise ValueError("Update manifest channel must be non-empty")
    if channel != expected_channel:
        return None

    artifact = _manifest_artifact(manifest)
    artifact_name = str(artifact.get("name", "")).strip()
    download_url = str(artifact.get("url", "")).strip()
    expected_sha256 = str(artifact.get("sha256", "")).strip().lower()
    latest_version = str(manifest.get("version", "")).strip()
    expected_name = f"{UPDATE_APP_ID}-{latest_version}.zip"
    if artifact_name != expected_name:
        raise ValueError("Update manifest artifact name does not match release version")
    if type(artifact.get("size_bytes")) is not int or artifact["size_bytes"] < 1:
        raise ValueError("Update manifest artifact.size_bytes must be an integer >= 1")
    if not _is_sha256(expected_sha256):
        raise ValueError("Update manifest artifact.sha256 must be 64 hex characters")
    _assert_https_update_url(download_url, require_zip=True)
    if _is_github_hosted_update_url(download_url):
        raise ValueError("Update manifest artifact URL must not point to GitHub-hosted update storage")

    archive = manifest.get("archive")
    if not isinstance(archive, dict):
        raise ValueError("Update manifest archive must be an object")
    if archive.get("format") != "zip":
        raise ValueError("Update manifest archive.format must be zip")
    _validate_relative_manifest_path(archive.get("entrypoint"), "archive.entrypoint")
    required_files = archive.get("required_files")
    if not isinstance(required_files, list) or not required_files or not all(isinstance(item, str) for item in required_files):
        raise ValueError("Update manifest archive.required_files must be a non-empty list of strings")
    for item in required_files:
        _validate_relative_manifest_path(item, "archive.required_files[]")
    if archive.get("top_level") is not None:
        _validate_relative_manifest_path(archive.get("top_level"), "archive.top_level")

    install = manifest.get("install")
    if not isinstance(install, dict):
        raise ValueError("Update manifest install must be an object")
    if install.get("strategy") not in UPDATE_ALLOWED_INSTALL_STRATEGIES:
        raise ValueError("Update manifest install.strategy is unsupported")
    preserve_paths = install.get("preserve_paths", [])
    if not isinstance(preserve_paths, list) or not all(isinstance(item, str) for item in preserve_paths):
        raise ValueError("Update manifest install.preserve_paths must be a list of strings")
    for item in preserve_paths:
        _validate_relative_manifest_path(item, "install.preserve_paths[]")

    return {
        "artifact": artifact,
        "artifact_name": artifact_name,
        "download_url": download_url,
        "sha256": expected_sha256,
        "archive": _archive_policy_from_manifest(archive),
        "version": latest_version,
    }


def _canonical_manifest_bytes(manifest):
    return json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _verify_update_manifest_signature(manifest, signature, public_key_hex):
    try:
        from cryptography.exceptions import InvalidSignature
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
    except ImportError as exc:
        raise ValueError("cryptography is required to verify update manifest signatures") from exc
    try:
        public_key = bytes.fromhex(str(public_key_hex).strip())
    except ValueError as exc:
        raise ValueError("Update manifest public key must be 64 hex characters") from exc
    if len(public_key) != 32:
        raise ValueError("Update manifest public key must be 32 bytes")
    if len(signature) != 64:
        raise ValueError("Update manifest signature must be 64 bytes")
    try:
        Ed25519PublicKey.from_public_bytes(public_key).verify(signature, _canonical_manifest_bytes(manifest))
    except InvalidSignature as exc:
        raise ValueError("Update manifest signature verification failed") from exc


def _sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verify_update_file_hash(path, expected_sha256):
    if not _is_sha256(str(expected_sha256 or "").strip()):
        raise ValueError("Downloaded update SHA256 verification requires a 64-character expected hash")
    actual_sha256 = _sha256_file(path)
    if actual_sha256.lower() != expected_sha256.strip().lower():
        raise ValueError(
            f"Downloaded update SHA256 mismatch: expected {expected_sha256}, got {actual_sha256}"
        )


def _manifest_artifact(manifest):
    artifact = manifest.get("artifact")
    if isinstance(artifact, dict):
        return artifact
    package = manifest.get("package")
    if isinstance(package, dict):
        return package
    return {}


def _update_candidate_from_manifest(manifest, expected_channel):
    policy = _validate_private_update_manifest_policy(manifest, expected_channel)
    if policy is None:
        return None

    latest_version = policy["version"]
    if not _is_update_version_newer(latest_version):
        return None
    if not _rollout_allows_current_pc(manifest):
        return None

    return {
        "url": policy["download_url"],
        "version": latest_version,
        "sha256": policy["sha256"],
        "archive": policy["archive"],
        "provider": UPDATE_PROVIDER_PRIVATE_MANIFEST,
    }


def _check_private_manifest_for_updates():
    manifest_url = _get_update_manifest_url()
    if not manifest_url:
        print("private_manifest updater is enabled, but no manifest URL is configured.")
        return None
    public_key_hex = _get_update_manifest_public_key()
    if not public_key_hex:
        raise ValueError("private_manifest updater requires a manifest public key")
    _assert_https_update_url(manifest_url)
    if _is_github_hosted_update_url(manifest_url):
        raise ValueError("private_manifest updater manifest URL must not point to GitHub-hosted update storage")
    response = requests.get(manifest_url, timeout=5)
    response.raise_for_status()
    manifest = response.json()
    signature_url = _get_update_manifest_signature_url(manifest_url)
    _assert_https_update_url(signature_url)
    if _is_github_hosted_update_url(signature_url):
        raise ValueError("private_manifest updater signature URL must not point to GitHub-hosted update storage")
    signature_response = requests.get(signature_url, timeout=5)
    signature_response.raise_for_status()
    _verify_update_manifest_signature(manifest, signature_response.content, public_key_hex)
    return _update_candidate_from_manifest(manifest, _get_update_channel())


def _find_github_release_asset_pair(assets, latest_version):
    expected_zip_name = f"{UPDATE_APP_ID}-{latest_version}.zip"
    zip_asset = None
    for asset in assets:
        name = str(asset.get("name", "")).strip()
        if name == expected_zip_name:
            zip_asset = asset
            break
        if zip_asset is None and name.endswith(".zip"):
            zip_asset = asset

    if not zip_asset:
        return None, None

    zip_name = str(zip_asset.get("name", "")).strip()
    checksum_names = {f"{zip_name}.sha256"}
    if zip_name.lower().endswith(".zip"):
        checksum_names.add(f"{zip_name[:-4]}.sha256")

    checksum_asset = None
    for asset in assets:
        if str(asset.get("name", "")).strip() in checksum_names:
            checksum_asset = asset
            break
    return zip_asset, checksum_asset


def _parse_github_release_sha256(text, expected_filename):
    match = re.search(r"\b([A-Fa-f0-9]{64})\b", str(text or ""))
    if not match:
        raise ValueError(f"GitHub release SHA256 asset is malformed for {expected_filename}")
    return match.group(1).lower()


def _sha256_from_github_asset_digest(asset):
    digest = str(asset.get("digest", "")).strip().lower()
    prefix = "sha256:"
    if digest.startswith(prefix) and _is_sha256(digest[len(prefix):]):
        return digest[len(prefix):]
    return ""


def _check_github_release_for_updates():
    api_url = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/releases/latest"
    response = requests.get(api_url, timeout=5)
    response.raise_for_status()
    latest_release_data = response.json()
    latest_version = latest_release_data['tag_name']
    if not _is_update_version_newer(latest_version):
        return None
    zip_asset, checksum_asset = _find_github_release_asset_pair(latest_release_data.get("assets") or [], latest_version)
    if not zip_asset:
        return None
    download_url = str(zip_asset.get("browser_download_url") or "").strip()
    _assert_https_update_url(download_url, require_zip=True)
    expected_sha256 = _sha256_from_github_asset_digest(zip_asset)
    if not expected_sha256 and not checksum_asset:
        print("업데이트 확인 중 오류 발생: GitHub 릴리스에 SHA256 asset이 없어 업데이트를 건너뜁니다.")
        return None

    if not expected_sha256:
        checksum_url = str(checksum_asset.get("browser_download_url") or "").strip()
        _assert_https_update_url(checksum_url)
        checksum_response = requests.get(checksum_url, timeout=5)
        checksum_response.raise_for_status()
        checksum_text = checksum_response.content.decode("utf-8")
        expected_sha256 = _parse_github_release_sha256(checksum_text, str(zip_asset.get("name", "")))
    return {
        "url": download_url,
        "version": latest_version,
        "sha256": expected_sha256,
        "provider": UPDATE_PROVIDER_GITHUB,
    }


def _check_update_candidate():
    provider = _get_update_provider()
    try:
        if provider in {"", UPDATE_PROVIDER_OFF, "disabled", "none"}:
            return None
        if provider == UPDATE_PROVIDER_PRIVATE_MANIFEST:
            return _check_private_manifest_for_updates()
        if provider == UPDATE_PROVIDER_GITHUB:
            return _check_github_release_for_updates()
        print(f"지원하지 않는 업데이트 provider입니다: {provider}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"업데이트 확인 중 오류 발생 (네트워크 문제일 수 있음): {e}")
        return None
    except Exception as e:
        print(f"업데이트 manifest 확인 중 오류 발생: {e}")
        return None

def check_for_updates():
    """Return (download_url, version) when an update is available."""
    candidate = _check_update_candidate()
    if not candidate:
        return None, None
    return candidate["url"], candidate["version"]


def _archive_policy_from_manifest(archive):
    policy = {
        "top_level": archive.get("top_level"),
        "required_files": list(archive.get("required_files") or []),
    }
    return policy


def _normalize_update_archive_member_name(member_name):
    normalized = str(member_name or "").replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized.rstrip("/")


UPDATE_WINDOWS_RESERVED_ARCHIVE_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *(f"COM{index}" for index in range(1, 10)),
    *(f"LPT{index}" for index in range(1, 10)),
}


def _is_windows_reserved_archive_segment(segment):
    return segment.split(".", 1)[0].upper() in UPDATE_WINDOWS_RESERVED_ARCHIVE_NAMES


def _is_windows_unsafe_archive_segment(segment):
    return (
        _is_windows_reserved_archive_segment(segment)
        or any(ord(char) < 32 for char in segment)
        or segment.endswith((" ", "."))
    )


def _validate_update_archive_contract(zip_ref, archive_policy=None):
    top_level = None
    required_files = []
    if archive_policy:
        top_level_value = archive_policy.get("top_level")
        if top_level_value is not None:
            top_level = _normalize_update_archive_member_name(top_level_value)
        required_files = [
            _normalize_update_archive_member_name(item).lower()
            for item in archive_policy.get("required_files") or []
        ]

    seen_members = set()
    file_paths = set()
    directory_paths = set()

    for member in zip_ref.infolist():
        member_name = _normalize_update_archive_member_name(member.filename)
        parts = member_name.split("/") if member_name else []
        mode = (member.external_attr >> 16) & 0o170000
        if (
            not member_name
            or "\x00" in member_name
            or member_name.startswith("/")
            or re.match(r"^[A-Za-z]:", member_name)
            or any(part in {"", ".", ".."} or ":" in part or _is_windows_unsafe_archive_segment(part) for part in parts)
            or mode == 0o120000
        ):
            raise ValueError(f"Unsafe update archive member: {member.filename!r}")
        if top_level and member_name != top_level and not member_name.startswith(top_level + "/"):
            raise ValueError(f"Unsafe update archive member outside manifest top_level: {member.filename!r}")

        member_key = member_name.lower()
        if member_key in seen_members:
            raise ValueError(f"Unsafe update archive member duplicate: {member.filename!r}")
        seen_members.add(member_key)

        if member.is_dir():
            directory_paths.add(member_key)
            continue

        file_paths.add(member_key)
        for index in range(1, len(parts)):
            directory_paths.add("/".join(parts[:index]).lower())

    collisions = sorted(file_paths & directory_paths)
    if collisions:
        raise ValueError(f"Unsafe update archive member collision: {collisions[0]}")

    missing = [item for item in required_files if item not in file_paths]
    if missing:
        raise ValueError(f"Update archive is missing required file: {missing[0]}")


def _safe_extract_update_zip(zip_ref, destination_path, archive_policy=None):
    _validate_update_archive_contract(zip_ref, archive_policy)
    destination_abs = os.path.abspath(destination_path)
    destination_prefix = destination_abs + os.sep
    os.makedirs(destination_abs, exist_ok=True)

    for member in zip_ref.infolist():
        member_name = _normalize_update_archive_member_name(member.filename)
        mode = (member.external_attr >> 16) & 0o170000
        if (
            not member_name
            or "\x00" in member_name
            or member_name.startswith("/")
            or member_name == ".."
            or member_name.startswith("../")
            or "/../" in member_name
            or member_name.endswith("/..")
            or any(":" in part or _is_windows_unsafe_archive_segment(part) for part in member_name.split("/"))
            or mode == 0o120000
        ):
            raise ValueError(f"Unsafe update archive member: {member.filename!r}")

        target_path = os.path.abspath(os.path.join(destination_abs, member_name))
        if target_path != destination_abs and not target_path.startswith(destination_prefix):
            raise ValueError(f"Unsafe update archive member: {member.filename!r}")

        if member.is_dir():
            os.makedirs(target_path, exist_ok=True)
            continue

        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        with zip_ref.open(member, "r") as source, open(target_path, "wb") as target:
            shutil.copyfileobj(source, target)


def download_and_apply_update(url, expected_sha256=None, archive_policy=None):
    """업데이트 .zip 파일을 다운로드하고, 압축 해제 후 적용 스크립트를 실행합니다."""
    try:
        if not _can_apply_updates():
            raise RuntimeError("Automatic update apply is only allowed from the packaged executable.")
        _assert_https_update_url(url, require_zip=True)
        if not _is_sha256(str(expected_sha256 or "").strip()):
            raise ValueError("Automatic update apply requires an expected SHA256 hash")
        temp_dir = os.environ.get("TEMP", "C:\\Temp")
        os.makedirs(temp_dir, exist_ok=True)
        zip_path = os.path.join(temp_dir, "update.zip")
        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()
        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        _verify_update_file_hash(zip_path, expected_sha256)
        temp_update_folder = os.path.join(temp_dir, "temp_update")
        if os.path.exists(temp_update_folder):
            shutil.rmtree(temp_update_folder)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            _safe_extract_update_zip(zip_ref, temp_update_folder, archive_policy=archive_policy)
        os.remove(zip_path)
        if getattr(sys, 'frozen', False):
            application_path = os.path.dirname(sys.executable)
        else:
            application_path = os.path.dirname(os.path.abspath(__file__))
        updater_script_path = os.path.join(application_path, "updater.bat")
        extracted_content = os.listdir(temp_update_folder)
        if len(extracted_content) == 1 and os.path.isdir(os.path.join(temp_update_folder, extracted_content[0])):
            new_program_folder_path = os.path.join(temp_update_folder, extracted_content[0])
        else:
            new_program_folder_path = temp_update_folder
        with open(updater_script_path, "w", encoding='utf-8') as bat_file:
            bat_file.write(f"""@echo off
chcp 65001 > nul
echo.
echo ==========================================================
echo    프로그램을 업데이트합니다. 이 창을 닫지 마세요.
echo ==========================================================
echo.
echo 잠시 후 프로그램이 자동으로 종료됩니다...
timeout /t 3 /nobreak > nul
taskkill /F /IM "{os.path.basename(sys.executable)}" > nul
echo.
echo 기존 파일을 백업하고 새 파일로 교체합니다...
xcopy "{new_program_folder_path}" "{application_path}" /E /H /C /I /Y > nul
echo.
echo 임시 업데이트 파일을 삭제합니다...
rmdir /s /q "{temp_update_folder}"
echo.
echo ========================================
echo    업데이트 완료!
echo ========================================
echo.
echo 3초 후에 프로그램을 다시 시작합니다.
timeout /t 3 /nobreak > nul
start "" "{os.path.join(application_path, os.path.basename(sys.executable))}"
del "%~f0"
            """)
        subprocess.Popen(updater_script_path, creationflags=subprocess.CREATE_NEW_CONSOLE)
        sys.exit(0)
    except Exception as e:
        root_alert = tk.Tk()
        root_alert.withdraw()
        messagebox.showerror("업데이트 실패", f"업데이트 파일을 적용하는 중 예상치 못한 오류가 발생했습니다.\n프로그램을 다시 시작하여 업데이트를 재시도해주세요.\n\n[오류 상세 정보]\n{e}", parent=root_alert)
        root_alert.destroy()
        sys.exit(1)

def threaded_update_check():
    """백그라운드에서 업데이트를 확인하고 필요한 경우 UI에 프롬프트를 표시합니다."""
    print("백그라운드 업데이트 확인 시작...")
    candidate = _check_update_candidate()
    if candidate:
        if not _can_apply_updates():
            print(f"업데이트 {candidate['version']} 확인됨. 소스 실행 모드에서는 자동 업데이트 적용을 건너뜁니다.")
            return
        download_url = candidate["url"]
        new_version = candidate["version"]
        root_alert = tk.Tk()
        root_alert.withdraw()
        if messagebox.askyesno("업데이트 발견", f"새로운 버전({new_version})이 있습니다.\n지금 업데이트하시겠습니까? (현재 버전: {APP_VERSION})", parent=root_alert):
            root_alert.destroy()
            download_and_apply_update(
                download_url,
                expected_sha256=candidate.get("sha256"),
                archive_policy=candidate.get("archive"),
            )
        else:
            print("사용자가 업데이트를 거부했습니다.")
            root_alert.destroy()
    else:
        print("업데이트 확인 완료. 최신 버전이거나 확인 중 오류가 발생했습니다.")

# #####################################################################
# 애플리케이션 코드 시작
# #####################################################################
class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        return super().default(o)
def resource_path(relative_path: str) -> str:
    try:
        base_path = sys._MEIPASS
    except AttributeError:
        base_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_path, relative_path)

class CalendarWindow(tk.Toplevel):
    def __init__(self, parent):
        from tkcalendar import Calendar

        super().__init__(parent)
        self.title("날짜 선택")
        self.transient(parent)
        self.grab_set()
        self.result = None
        self.resizable(False, False)

        self.cal = Calendar(self, selectmode='day', year=datetime.now().year, month=datetime.now().month, day=datetime.now().day,
                            locale='ko_KR', background="white", foreground="black", headersbackground="#EAEAEA")
        self.cal.pack(pady=20, padx=20, fill="both", expand=True)

        btn_frame = ttk.Frame(self)
        btn_frame.pack(pady=(0, 10))

        select_btn = ttk.Button(btn_frame, text="선택", command=self.on_select)
        select_btn.pack(side="left", padx=5)
        cancel_btn = ttk.Button(btn_frame, text="취소", command=self.destroy)
        cancel_btn.pack(side="left", padx=5)

        if hasattr(parent, "_center_child_window"):
            width, height = parent._dialog_size("calendar")
            parent._center_child_window(self, width, height)
        self.bind("<Escape>", lambda event: self.destroy())
        self.cal.focus_set()
        self.protocol("WM_DELETE_WINDOW", self.destroy)
        self.wait_window(self)

    def on_select(self):
        self.result = self.cal.selection_get()
        self.destroy()

class DataManager:
    def __init__(self, save_dir, process_name, worker_name, unique_id):
        self.save_directory = save_dir
        self.process_name = process_name
        self.worker_name = worker_name
        self.unique_id = unique_id
        self.log_queue = queue.Queue()
        self._close_lock = threading.Lock()
        self._close_requested = False
        self._writer_errors = []
        self.log_thread = threading.Thread(target=self._log_writer_thread, daemon=True)
        self.log_thread.start()
    def _get_log_filepath(self, target_date=None):
        if target_date is None:
            target_date = datetime.now()
        filename = f"{self.process_name}작업이벤트로그_{self.unique_id}_{target_date.strftime('%Y%m%d')}.csv"
        return os.path.join(self.save_directory, filename)
    def _get_log_filepath_for_item(self, log_item):
        try:
            return self._get_log_filepath(datetime.fromisoformat(str(log_item[0])))
        except Exception:
            return self._get_log_filepath()
    def _log_writer_thread(self):
        while True:
            log_item = None
            got_item = False
            try:
                log_item = self.log_queue.get()
                got_item = True
                if log_item is None: break
                filepath = self._get_log_filepath_for_item(log_item)
                file_exists = os.path.exists(filepath)
                os.makedirs(os.path.dirname(filepath), exist_ok=True)
                with open(filepath, 'a', newline='', encoding='utf-8-sig') as f:
                    writer = csv.writer(f)
                    if not file_exists or os.stat(filepath).st_size == 0:
                        writer.writerow(["timestamp", "worker_name", "event", "details"])
                    writer.writerow(log_item)
            except queue.Empty:
                continue
            except Exception as e:
                self._writer_errors.append(e)
                print(f"로그 쓰기 스레드 오류: {e}")
            finally:
                if got_item:
                    self.log_queue.task_done()
    def log_event(self, event_type, details):
        enriched_details = _enrich_label_match_event(event_type, details or {}, self.unique_id)
        log_item = [
            datetime.now().isoformat(),
            _csv_formula_safe_cell(self.worker_name),
            event_type,
            json.dumps(enriched_details, ensure_ascii=False, cls=DateTimeEncoder),
        ]
        with self._close_lock:
            if self._close_requested:
                raise RuntimeError("DataManager is closing; new log events are not accepted")
            self.log_queue.put(log_item)
    def close(self, timeout=None):
        with self._close_lock:
            if not self._close_requested:
                self._close_requested = True
                if self.log_thread.is_alive():
                    self.log_queue.put(None)
        self.log_thread.join(timeout)
        if self.log_thread.is_alive():
            raise TimeoutError("Log writer did not stop before timeout")
        if self._writer_errors:
            raise RuntimeError(f"Log writer failed: {self._writer_errors[-1]}")
        return True
    def flush(self, timeout=None):
        deadline = None if timeout is None else time.monotonic() + timeout
        while self.log_queue.unfinished_tasks:
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("Log writer did not flush before timeout")
            time.sleep(0.01)
        if self._writer_errors:
            raise RuntimeError(f"Log writer failed: {self._writer_errors[-1]}")
        return True
    def save_current_state(self, state_data):
        state_path = os.path.join(self.save_directory, Label_Match.FILES.CURRENT_STATE)
        temp_path = f"{state_path}.tmp-{os.getpid()}-{threading.get_ident()}"
        try:
            os.makedirs(os.path.dirname(state_path), exist_ok=True)
            state_data_with_worker = {'worker_name': self.worker_name, **state_data}
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(state_data_with_worker, f, ensure_ascii=False, indent=4, cls=DateTimeEncoder)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, state_path)
        except Exception as e:
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception:
                pass
            print(f"임시 상태 저장 실패: {e}")
    def load_current_state(self):
        state_path = os.path.join(self.save_directory, Label_Match.FILES.CURRENT_STATE)
        if not os.path.exists(state_path): return None
        try:
            with open(state_path, 'r', encoding='utf-8') as f: return json.load(f)
        except Exception as e:
            print(f"임시 상태 로드 실패: {e}"); return None
    def delete_current_state(self):
        state_path = os.path.join(self.save_directory, Label_Match.FILES.CURRENT_STATE)
        if os.path.exists(state_path):
            try: os.remove(state_path)
            except Exception as e: print(f"임시 상태 파일 삭제 실패: {e}")

class Label_Match(tk.Tk):
    WORKER_HISTORY_LIMIT = 20

    UI_PROFILES = {
        "small": {
            "outer_padding": 8,
            "card_padding": 10,
            "section_gap": 6,
            "content_gap": 4,
            "bottom_gap": 6,
            "big_display_pady": (4, 7),
            "big_display_ipady": 4,
            "big_display_cap": 28,
            "big_display_wrap_ratio": 0.88,
            "font_scale": 0.82,
            "effective_scale_max": 1.18,
            "button_padding": 6,
            "control_padding": (6, 3),
            "action_padding": 7,
            "action_font_scale": 0.88,
            "header_font_cap": 22,
            "status_font_cap": 18,
            "button_font_cap": 16,
            "control_font_cap": 12,
            "action_font_cap": 14,
            "tree_heading_font_cap": 12,
            "tree_font_cap": 13,
            "tree_row_height_scale": 2.35,
            "detail_text_height": 2,
            "history_ratio": 0.74,
            "history_ratio_min": 0.66,
            "history_ratio_max": 0.86,
            "dialog_scale": 0.84,
        },
        "compact": {
            "outer_padding": 12,
            "card_padding": 14,
            "section_gap": 10,
            "content_gap": 6,
            "bottom_gap": 8,
            "big_display_pady": (8, 12),
            "big_display_ipady": 6,
            "big_display_cap": 34,
            "big_display_wrap_ratio": 0.82,
            "font_scale": 0.86,
            "effective_scale_max": 1.36,
            "button_padding": 8,
            "control_padding": (8, 5),
            "action_padding": 9,
            "action_font_scale": 0.95,
            "header_font_cap": 26,
            "status_font_cap": 21,
            "button_font_cap": 18,
            "control_font_cap": 14,
            "action_font_cap": 17,
            "tree_heading_font_cap": 14,
            "tree_font_cap": 15,
            "tree_row_height_scale": 2.6,
            "detail_text_height": 3,
            "history_ratio": 0.72,
            "history_ratio_min": 0.62,
            "history_ratio_max": 0.84,
            "dialog_scale": 0.88,
        },
        "standard": {
            "outer_padding": 20,
            "card_padding": 22,
            "section_gap": 18,
            "content_gap": 10,
            "bottom_gap": 14,
            "big_display_pady": (14, 22),
            "big_display_ipady": 10,
            "big_display_cap": 60,
            "big_display_wrap_ratio": 0.78,
            "font_scale": 1.0,
            "effective_scale_max": 1.8,
            "button_padding": 11,
            "control_padding": (10, 6),
            "action_padding": 12,
            "action_font_scale": 1.0,
            "header_font_cap": 36,
            "status_font_cap": 28,
            "button_font_cap": 24,
            "control_font_cap": 20,
            "action_font_cap": 22,
            "tree_heading_font_cap": 20,
            "tree_font_cap": 18,
            "tree_row_height_scale": 2.8,
            "detail_text_height": 4,
            "history_ratio": 0.68,
            "history_ratio_min": 0.58,
            "history_ratio_max": 0.82,
            "dialog_scale": 1.0,
        },
        "large": {
            "outer_padding": 30,
            "card_padding": 28,
            "section_gap": 24,
            "content_gap": 14,
            "bottom_gap": 18,
            "big_display_pady": (18, 28),
            "big_display_ipady": 14,
            "big_display_cap": 76,
            "big_display_wrap_ratio": 0.72,
            "font_scale": 1.08,
            "effective_scale_max": 2.2,
            "button_padding": 12,
            "control_padding": (12, 7),
            "action_padding": 14,
            "action_font_scale": 1.0,
            "header_font_cap": 44,
            "status_font_cap": 34,
            "button_font_cap": 30,
            "control_font_cap": 24,
            "action_font_cap": 26,
            "tree_heading_font_cap": 24,
            "tree_font_cap": 20,
            "tree_row_height_scale": 3.0,
            "detail_text_height": 5,
            "history_ratio": 0.66,
            "history_ratio_min": 0.56,
            "history_ratio_max": 0.80,
            "dialog_scale": 1.08,
        },
    }
    PRODUCT_SAMPLE_COUNT = LABEL_MATCH_PRODUCT_SAMPLE_COUNT
    TOTAL_SCAN_COUNT = LABEL_MATCH_TOTAL_SCAN_COUNT
    FINAL_LABEL_SCAN_POSITION = LABEL_MATCH_FINAL_LABEL_SCAN_POSITION
    CURRENT_SET_CANCEL_ACTION_TEXT = "현재 세트 취소"
    COMPLETED_TRAY_CANCEL_ACTION_TEXT = "완료된 트레이 취소"
    MANUAL_COMPLETE_ACTION_TEXT = "현재 세트 수동 완료"
    HISTORY_DELETE_ACTION_TEXT = "선택 항목 삭제"
    CURRENT_SET_CANCEL_BUTTON_TEXT = f"{CURRENT_SET_CANCEL_ACTION_TEXT} (F1)"
    COMPLETED_TRAY_CANCEL_BUTTON_TEXT = f"{COMPLETED_TRAY_CANCEL_ACTION_TEXT} (F2)"
    MANUAL_COMPLETE_BUTTON_TEXT = "현재 세트 완료 (F3)"
    EXACT_RESCAN_BUTTON_TEXT = "전체 재스캔 시작 (F4)"
    CURRENT_SET_CANCEL_BUTTON_STYLE = "Danger.Action.TButton"
    COMPLETED_TRAY_CANCEL_BUTTON_STYLE = "Danger.Action.TButton"
    MANUAL_COMPLETE_BUTTON_STYLE = "Action.TButton"
    STEP_NAMES = (
        "현품표",
        *(f"제품{index}" for index in range(1, LABEL_MATCH_PRODUCT_SAMPLE_COUNT + 1)),
        "라벨지",
    )
    BARCODE_DISPLAY_LIMITS = {
        "small": 14,
        "compact": 16,
        "standard": 24,
        "large": 32,
    }
    HISTORY_HEADING_LABELS = {
        "Set": ("#",),
        **{
            f"Input{index}": (
                ("현품표", "현품")
                if index == LABEL_MATCH_MASTER_SCAN_POSITION
                else ("라벨지", "라벨")
                if index == LABEL_MATCH_FINAL_LABEL_SCAN_POSITION
                else (f"제품{index - 1}", f"P{index - 1}")
            )
            for index in range(1, LABEL_MATCH_TOTAL_SCAN_COUNT + 1)
        },
        "Result": ("결과",),
        "Timestamp": ("시간", "시각"),
    }
    SUMMARY_HEADING_LABELS = {
        "Code": ("기준 코드", "코드"),
        "Phase": ("차수", "차"),
        "Count": ("통과수", "수"),
    }
    MANUAL_COMPLETE_HINTS = {
        "manual_complete_requires_product_scan": "제품 1개 이상 스캔 후 가능",
        "manual_complete_only_for_partial_sets": f"이미 {LABEL_MATCH_TOTAL_SCAN_COUNT}개 완료됨",
        "manual_complete_blocked_after_error": "오류 세트는 불가",
        "manual_complete_blocked_during_exact_rescan": "전체 재스캔 중에는 불가",
    }
    class FILES:
        CURRENT_STATE = "_current_set_state_packaging.json"
        SETTINGS = "app_settings.json"
        ITEMS = "Item.csv"
    class Events:
        APP_START = "APP_START"
        APP_CLOSE = "APP_CLOSE"
        SCAN_OK = "SCAN_OK"
        TRAY_COMPLETE = "TRAY_COMPLETE"
        SET_CANCELLED = "SET_CANCELLED"
        SET_DELETED = "SET_DELETED"
        SET_RESTORED = "SET_RESTORED"
        UI_ERROR = "UI_ERROR"
        ERROR_INPUT = "ERROR_INPUT"
        ERROR_MISMATCH = "ERROR_MISMATCH"
        SCAN_ATTEMPT = "SCAN_ATTEMPT"
        TRAY_COMPLETION_CANCELLED = "TRAY_COMPLETION_CANCELLED"
        BASE64_DECODED = "BASE64_DECODED"
        EXACT_RESCAN_STARTED = "EXACT_RESCAN_STARTED"
        EXACT_RESCAN_OK = "EXACT_RESCAN_OK"
        EXACT_RESCAN_COMPLETED = "EXACT_RESCAN_COMPLETED"
    class Results:
        PASS = LABEL_MATCH_RESULT_PASS
        FAIL_MISMATCH = LABEL_MATCH_RESULT_FAIL_MISMATCH
        FAIL_INPUT_ERROR = "입력오류"
        IN_PROGRESS = "진행중..."
    class Worker:
        PACKAGING = "포장실"

    def __init__(self, run_tests=False):
        _label_match_startup_trace("app_init_before_tk", run_tests=run_tests)
        super().__init__()
        _label_match_startup_trace("app_init_after_tk", title=self.title())
        self.run_tests = run_tests
        self.initialized_successfully = False
        self.audio_ready = False
        self.audio_error = ""
        self.audio_init_finished = False
        self.audio_init_started = False
        self.pygame_module = None
        
        self.is_running_simulation = False
        self.simulation_scenarios = []
        self.current_scenario_index = 0
        self.current_step_index = 0
        _label_match_startup_trace("app_init_before_setup_paths")
        self._setup_paths()
        _label_match_startup_trace(
            "app_init_after_setup_paths",
            app_settings_path=getattr(self, "app_settings_path", ""),
            save_directory=getattr(self, "save_directory", ""),
        )
        self.app_settings = self._load_app_settings()
        _label_match_startup_trace("app_init_after_load_settings")
        self.custom_save_path = self._resolve_configured_save_path()
        self._update_save_directory()
        _label_match_startup_trace("app_init_after_save_directory", save_directory=self.save_directory)
        self.ui_cfg = self.app_settings.get("ui_settings", {})
        self.base_font_size = self.ui_cfg.get("base_font_size", 14)
        default_colors = {
            "background": "#F9FAFB", "card_background": "#FFFFFF", "text": "#111827",
            "text_subtle": "#6B7280", "text_strong": "#000000", "primary": "#3B82F6",
            "primary_active": "#2563EB", "success": "#047857", "success_light": "#D1FAE5",
            "danger": "#B91C1C", "danger_light": "#FEE2E2", "border": "#D1D5DB",
            "heading_background": "#FFFFFF"
        }
        self.colors = {**default_colors, **self.app_settings.get("colors", {})}
        self.sounds = self.app_settings.get("sound_files", {})
        self.sound_objects = {}
        self.items_data = {}
        self.unique_id = socket.gethostname()
        self.worker_name = self.app_settings.get("worker_name", self.Worker.PACKAGING)
        self.data_manager = DataManager(self.save_directory, self.Worker.PACKAGING, self.worker_name, self.unique_id)
        self.package_outbox = PackageOutbox(os.path.join(self.save_directory, "package_logistics_outbox.sqlite3"))
        self.package_logistics_client = package_client_from_env()
        self.package_outbox_processor = (
            PackageOutboxProcessor(self.package_outbox, self.package_logistics_client)
            if self.package_logistics_client is not None
            else None
        )
        self.package_outbox_thread = None
        self.package_outbox_after_id = None
        _label_match_startup_trace("app_init_after_data_manager", worker_name=self.worker_name, unique_id=self.unique_id)
        self.current_set_info = {} 
        self.is_blinking = False
        self.scan_count = defaultdict(lambda: defaultdict(int))
        self.global_scanned_set = set()
        self.set_details_map = {}
        self.history_row_details_map = {}
        self.history_view_updates_active_state = True
        self.history_load_generation = 0
        self.history_load_pending = False
        self.history_active_load_pending = False
        self.is_generating_test_logs = False
        # View-only workflow state. These values never participate in ledger,
        # recovery, package logistics, or duplicate decisions.
        self._workflow_widgets_ready = False
        self._workflow_completion_kind = None
        self._workflow_display_scans = ()
        self._workflow_display_parsed_scans = ()
        self._workflow_last_normal_override = None
        self._workflow_blocking_notice = None
        self._workflow_notice_action = None
        self._workflow_notice_action_text = "확인"
        self._workflow_pending_error = None
        self._workflow_recovered = False
        self._workflow_item_snapshot = None
        self.title(f"바코드 세트 검증기 ({APP_VERSION}) - 로딩 중...")
        _label_match_startup_trace("app_init_after_title", title=self.title())
        self.state('zoomed')
        _label_match_startup_trace("app_init_after_zoomed", state=self.state())
        self.configure(bg=self.colors.get("background", "#ECEFF1"))
        self.ui_profile_name, self.ui_profile = self._select_ui_profile()
        self._responsive_after_id = None
        self._zoom_after_id = None
        self._ui_redraw_after_id = None
        self._clock_after_id = None
        self._applying_responsive_layout = False
        self.scale_factor = 1.2
        self.tree_font_size = 13
        self.summary_col_widths = {}
        self.history_col_widths = {}
        self.sash_position = None
        self._load_ui_persistence_settings()
        self.hist_proportions = {"Set": 4, "Input1": 14}
        for index in range(2, self.TOTAL_SCAN_COUNT + 1):
            self.hist_proportions[f"Input{index}"] = 10
        self.hist_proportions.update({"Result": 8, "Timestamp": 14})
        self.summary_proportions = {"Code": 70, "Phase": 12, "Count": 18}
        self.default_font_name = self.ui_cfg.get("default_font", "Malgun Gothic")
        self.style = ttk.Style(self)
        self._configure_base_styles()
        _label_match_startup_trace("app_init_before_create_widgets")
        self._create_widgets()
        _label_match_startup_trace("app_init_after_create_widgets")
        self._configure_treeview_styles()
        self.show_loading_overlay()
        _label_match_startup_trace("app_init_after_loading_overlay")
        self.initial_load_queue = queue.Queue()
        threading.Thread(target=self._async_initial_load, daemon=True).start()
        _label_match_startup_trace("app_init_after_initial_load_thread")
        self.after(100, self._process_initial_load_queue)
        self.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.bind_all("<Control-MouseWheel>", self.on_ctrl_wheel)
        self.bind("<Button-1>", self._on_root_click)
        self.bind("<Configure>", self._on_window_configure)
        self.after(250, self._start_audio_initialization)
        self.after(300, self._start_package_outbox_drain)
        self.after(1000, lambda: _label_match_startup_trace("app_alive_after_1s", title=self.title(), state=self.state()))
        _label_match_startup_trace("app_init_complete")

    def _start_package_outbox_drain(self):
        processor = self.__dict__.get("package_outbox_processor")
        if processor is None or self.__dict__.get("run_tests", False):
            return None
        current = self.__dict__.get("package_outbox_thread")
        if current is not None and current.is_alive():
            return current

        def worker():
            try:
                processor.drain(limit=20)
            except Exception as exc:
                print(f"포장 물류 outbox 처리 오류: {exc}")
            finally:
                try:
                    if self.__dict__.get("package_outbox_after_id") is None:
                        self.package_outbox_after_id = self.after(
                            30000, self._run_scheduled_package_outbox_drain
                        )
                except TclError:
                    pass

        thread = threading.Thread(target=worker, name="label-match-package-outbox", daemon=True)
        self.package_outbox_thread = thread
        thread.start()
        return thread

    def _run_scheduled_package_outbox_drain(self):
        self.package_outbox_after_id = None
        return self._start_package_outbox_drain()

    def _start_audio_initialization(self):
        if self.run_tests or _label_match_automated_test_mode() or self.audio_init_started or not _label_match_audio_enabled():
            self.audio_init_finished = True
            return
        self.audio_init_started = True

        def initialize_audio():
            error_message = ""
            ready = False
            try:
                import pygame

                pygame.mixer.init()
                self.pygame_module = pygame
                ready = True
            except Exception as exc:
                error_message = str(exc)

            def finish():
                self.audio_ready = ready
                self.audio_error = error_message
                self.audio_init_finished = True
                if ready and self.initialized_successfully:
                    self.sound_objects = self._preload_sounds()
                elif error_message:
                    print(f"오디오 초기화 오류: {error_message}")

            try:
                self.after(0, finish)
            except TclError:
                pass

        threading.Thread(target=initialize_audio, name="label-match-audio-init", daemon=True).start()

    def _on_root_click(self, event):
        if event.widget not in [self.history_tree, self.summary_tree]:
            self.history_tree.selection_remove(self.history_tree.selection())
            self.summary_tree.selection_remove(self.summary_tree.selection())
        widget_class = event.widget.winfo_class()
        interactive_classes = {"Button", "TButton", "Entry", "TEntry", "Treeview", "Scrollbar", "TScrollbar"}
        if "entry" not in self.__dict__:
            return
        if widget_class == "Treeview":
            self.after(80, self._focus_scan_entry_if_available)
        elif widget_class not in interactive_classes:
            self._focus_scan_entry_if_available()

    def _focus_scan_entry_if_available(self):
        entry = self.__dict__.get("entry")
        if self.__dict__.get("operator_workbench_ready"):
            view = self._render_operator_workbench()
        else:
            view = self.__dict__.get("_last_workflow_view")
        if entry is None or not self.__dict__.get("initialized_successfully", False):
            return False
        if view is not None and not view.scan_input_enabled:
            return False
        try:
            if str(entry.cget("state")) != "normal":
                return False
            entry.focus_set()
            return True
        except (TclError, AttributeError):
            return False

    def _async_initial_load(self):
        _label_match_startup_trace("async_initial_load_start")
        try:
            items_data = self._load_items_data()
            loaded_data = {"items": items_data}
            self.initial_load_queue.put(loaded_data)
            _label_match_startup_trace("async_initial_load_ok", item_count=len(items_data or {}))
        except Exception as e:
            self.initial_load_queue.put({"error": str(e)})
            _label_match_startup_trace("async_initial_load_error", error=str(e))

    def _process_initial_load_queue(self):
        try:
            result = self.initial_load_queue.get_nowait()
            _label_match_startup_trace("initial_load_queue_result", keys=sorted(result.keys()))
            if "error" in result:
                self.hide_loading_overlay()
                if not self.run_tests:
                    messagebox.showerror("초기화 오류", f"프로그램 시작에 필요한 중요 파일을 불러올 수 없습니다.\n프로그램이 설치된 폴더가 손상되었거나 파일이 없을 수 있습니다.\n\n[오류 원인]\n{result['error']}\n\n프로그램을 종료합니다.")
                self.destroy()
                return
            self.items_data = result.get('items', {})
            self.sound_objects = self._preload_sounds()
            self.hide_loading_overlay()
            self.entry.config(state='normal')
            self.entry.focus_set()
            self._reset_current_set()
            self.title(f"바코드 세트 검증기 ({APP_VERSION}) - {self.worker_name} ({self.unique_id})")
            self.data_manager.log_event(self.Events.APP_START, {"message": "Application initialized."})
            self.initialized_successfully = True
            self._render_operator_workbench()
            self.history_queue = queue.Queue()
            self._load_history_and_rebuild_summary()
            self._process_history_queue()
            self._load_current_set_state()
            self.after(200, self._update_ui_scaling)
            self._update_clock()
            _label_match_startup_trace("initial_load_ui_ready", title=self.title())
            if not self.run_tests:
                self._start_direct_sync_auto_bootstrap()
                threading.Thread(target=threaded_update_check, daemon=True).start()
        except queue.Empty:
            self.after(100, self._process_initial_load_queue)
        except Exception as e:
            self.hide_loading_overlay()
            if not self.run_tests:
                messagebox.showerror("초기화 오류", f"프로그램을 시작하는 마지막 단계에서 오류가 발생했습니다.\n일시적인 문제일 수 있으니 프로그램을 다시 시작해보세요.\n\n[상세 오류]\n{e}\n\n프로그램을 종료합니다.")
            self.destroy()

    def _start_direct_sync_auto_bootstrap(self):
        context = _label_match_direct_sync_context(self.save_directory, self.app_settings_path)
        self.direct_sync_bootstrap_context = context
        threading.Thread(
            target=_label_match_auto_bootstrap_direct_sync,
            args=(context,),
            daemon=True,
        ).start()

    def show_loading_overlay(self):
        self.loading_overlay.grid(row=0, column=0, rowspan=3, sticky='nsew')
        self.loading_overlay.tkraise()
        self.loading_progressbar.start(10)
        self.update_idletasks()

    def hide_loading_overlay(self):
        self.loading_progressbar.stop()
        self.loading_overlay.grid_forget()

    def _preload_sounds(self):
        if self.run_tests: return {}
        if not self.audio_ready:
            return {}
        sound_objects = {}
        pygame_module = self.pygame_module
        if pygame_module is None:
            try:
                import pygame as pygame_module
            except Exception as e:
                print(f"사운드 모듈 로드 오류: {e}")
                return {}
        for key, filename in self.sounds.items():
            sound_path = resource_path(os.path.join("assets", filename))
            if os.path.exists(sound_path):
                try:
                    sound_objects[key] = pygame_module.mixer.Sound(sound_path)
                except Exception as e:
                    print(f"사운드 로드 오류 ({filename}): {e}")
            else:
                print(f"사운드 파일 없음: {sound_path}")
        return sound_objects

    def _setup_paths(self):
        self.base_path = os.path.dirname(os.path.abspath(sys.executable if getattr(sys, 'frozen', False) else __file__))
        self.config_directory = resource_path("config")
        os.makedirs(self.config_directory, exist_ok=True)
        self.app_settings_path = os.path.join(self.config_directory, self.FILES.SETTINGS)

    def _update_save_directory(self):
        self.save_directory = self.custom_save_path
        os.makedirs(self.save_directory, exist_ok=True)

    def _resolve_configured_save_path(self):
        configured_path = str(self.app_settings.get("custom_save_path", "") or "").strip()
        return configured_path or _default_label_match_save_path()

    def _load_app_settings(self):
        try:
            with open(self.app_settings_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _save_app_settings(self):
        try:
            if not self.initialized_successfully: return
            self._remember_worker_name(self.worker_name)
            self.app_settings['worker_name'] = self.worker_name
            if "ui_persistence" not in self.app_settings:
                self.app_settings["ui_persistence"] = {}
            self.app_settings["ui_persistence"]["scale_factor"] = self.scale_factor
            self.app_settings["ui_persistence"]["tree_font_size"] = self.tree_font_size
            content_pane = self.__dict__.get("content_pane")
            sashpos = getattr(content_pane, "sashpos", None)
            if callable(sashpos):
                self.app_settings["ui_persistence"]["sash_position"] = sashpos(0)
            self.app_settings["ui_persistence"]["summary_col_widths"] = {col: self.summary_tree.column(col, 'width') for col in self.summary_tree['columns']}
            self.app_settings["ui_persistence"]["history_col_widths"] = {col: self.history_tree.column(col, 'width') for col in self.history_tree['columns']}
            with open(self.app_settings_path, 'w', encoding='utf-8') as f:
                json.dump(self.app_settings, f, indent=4, ensure_ascii=False)
        except Exception as e:
            print(f"앱 설정 저장 오류: {e}")

    @staticmethod
    def _worker_history_timestamp(value):
        text = str(value or "").strip()
        if not text:
            return 0.0
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(text).timestamp()
        except ValueError:
            return 0.0

    def _worker_history_entries(self):
        raw_history = self.app_settings.get("worker_history", [])
        if not isinstance(raw_history, list):
            raw_history = []
        by_name = {}
        order = []
        for item in raw_history:
            if isinstance(item, dict):
                name = str(item.get("name") or "").strip()
                last_used_at = str(item.get("last_used_at") or item.get("updated_at") or item.get("created_at") or "").strip()
            else:
                name = str(item or "").strip()
                last_used_at = ""
            if not name:
                continue
            if name not in by_name:
                by_name[name] = {"name": name, "last_used_at": last_used_at}
                order.append(name)
            elif self._worker_history_timestamp(last_used_at) > self._worker_history_timestamp(by_name[name].get("last_used_at")):
                by_name[name] = {"name": name, "last_used_at": last_used_at}
        current_worker = str(getattr(self, "worker_name", "") or "").strip()
        if current_worker and current_worker not in by_name:
            by_name[current_worker] = {"name": current_worker, "last_used_at": ""}
            order.append(current_worker)
        entries = [by_name[name] for name in order]
        entries.sort(key=lambda entry: (-self._worker_history_timestamp(entry.get("last_used_at")), entry["name"]))
        if current_worker:
            entries.sort(key=lambda entry: 0 if entry["name"] == current_worker else 1)
        return entries[:self.WORKER_HISTORY_LIMIT]

    def _recent_worker_names(self):
        return [entry["name"] for entry in self._worker_history_entries()]

    def _remember_worker_name(self, worker_name):
        name = str(worker_name or "").strip()
        if not name:
            return
        entries = [entry for entry in self._worker_history_entries() if entry["name"] != name]
        entries.insert(0, {"name": name, "last_used_at": datetime.now().astimezone().isoformat(timespec="seconds")})
        self.app_settings["worker_history"] = entries[:self.WORKER_HISTORY_LIMIT]

    def _load_ui_persistence_settings(self):
        persistence_settings = self.app_settings.get("ui_persistence", {})
        self.scale_factor = persistence_settings.get("scale_factor", 1.2)
        if not (0.5 <= self.scale_factor <= 3.0): self.scale_factor = 1.2
        self.tree_font_size = persistence_settings.get("tree_font_size", 13)
        if not (6 <= self.tree_font_size <= 20): self.tree_font_size = 13
        self.summary_col_widths = persistence_settings.get("summary_col_widths", {})
        self.history_col_widths = persistence_settings.get("history_col_widths", {})
        self.sash_position = persistence_settings.get("sash_position", None)

    def _screen_diagonal_inches(self):
        try:
            width_mm = self.winfo_screenmmwidth()
            height_mm = self.winfo_screenmmheight()
            if width_mm <= 0 or height_mm <= 0:
                return None
            return ((width_mm ** 2 + height_mm ** 2) ** 0.5) / 25.4
        except TclError:
            return None

    def _select_ui_profile(self, width=None, height=None):
        try:
            current_width = int(width) if width is not None else int(self.winfo_width())
            current_height = int(height) if height is not None else int(self.winfo_height())
            screen_width = (
                current_width if current_width > 100 else int(self.winfo_screenwidth())
            )
            screen_height = (
                current_height if current_height > 100 else int(self.winfo_screenheight())
            )
        except (TclError, TypeError, ValueError):
            screen_width, screen_height = 1920, 1080
        screen_diagonal = self._screen_diagonal_inches()

        if screen_width <= 1366 or screen_height <= 800 or (screen_diagonal and screen_diagonal <= 14.6):
            return "small", self.UI_PROFILES["small"]
        if (screen_diagonal and screen_diagonal <= 15.8) or screen_width <= 1440 or screen_height <= 900:
            return "compact", self.UI_PROFILES["compact"]
        if screen_width >= 2560 and screen_height >= 1400:
            return "large", self.UI_PROFILES["large"]
        return "standard", self.UI_PROFILES["standard"]

    def _on_window_configure(self, event):
        if event.widget is not self or self._applying_responsive_layout:
            return
        if self._responsive_after_id:
            try:
                self.after_cancel(self._responsive_after_id)
            except TclError:
                pass
        self._responsive_after_id = self.after(
            150,
            lambda width=event.width, height=event.height: self._update_responsive_profile(width, height),
        )

    def _update_responsive_profile(self, width=None, height=None, force=False):
        if self._applying_responsive_layout:
            return
        new_name, new_profile = self._select_ui_profile(width, height)
        changed = force or new_name != self.__dict__.get("ui_profile_name")
        self.ui_profile_name = new_name
        self.ui_profile = new_profile
        self._apply_responsive_layout()
        if changed and getattr(self, "initialized_successfully", False):
            self._update_ui_scaling()

    def _apply_responsive_layout(self):
        if "main_frame" not in self.__dict__:
            return
        if self.__dict__.get("operator_workbench_ready"):
            self._apply_operator_responsive_layout()
            return
        profile = getattr(self, "ui_profile", self.UI_PROFILES["standard"])
        self._applying_responsive_layout = True
        try:
            self.main_frame.configure(padding=profile["outer_padding"])
            self.top_card.configure(padding=profile["card_padding"])
            self.top_card.grid_configure(pady=(0, profile["section_gap"]))
            self.top_right_frame.place_configure(x=-profile["card_padding"], y=profile["card_padding"])
            self.big_display_label.grid_configure(
                pady=profile["big_display_pady"],
                ipady=profile["big_display_ipady"],
            )
            current_width = self.winfo_width()
            if current_width <= 200:
                current_width = self.winfo_screenwidth()
            wrap_width = max(420, int(current_width * profile["big_display_wrap_ratio"]))
            self.big_display_label.configure(wraplength=wrap_width)
            self.progress_frame.grid_configure(pady=(profile["content_gap"], 0))
            self.content_pane.grid_configure(pady=(profile["content_gap"], 0))
            self.history_card.configure(padding=profile["card_padding"])
            self.summary_card.configure(padding=profile["card_padding"])
            self.hist_header_frame.grid_configure(pady=(0, profile["content_gap"]))
            if "history_detail_frame" in self.__dict__:
                self.history_detail_frame.grid_configure(pady=(profile["content_gap"], 0))
            self.summary_header_label.grid_configure(pady=(0, profile["content_gap"]))
            self.bottom_frame.grid_configure(pady=(profile["bottom_gap"], 0))
            self.status_label.configure(wraplength=wrap_width)
            self.view_mode_label.configure(wraplength=wrap_width)
            self._apply_content_sash_position()
            self._apply_adaptive_header_fitting()
        finally:
            self._applying_responsive_layout = False

    def _apply_content_sash_position(self):
        if self.__dict__.get("operator_workbench_ready"):
            return
        if "content_pane" not in self.__dict__:
            return
        profile = getattr(self, "ui_profile", self.UI_PROFILES["standard"])
        try:
            pane_width = self.content_pane.winfo_width()
            if pane_width <= 200:
                self.after(80, self._apply_content_sash_position)
                return
            desired = int(pane_width * profile["history_ratio"])
            saved = self.sash_position
            if isinstance(saved, int) and pane_width > 0:
                saved_ratio = saved / pane_width
                if profile["history_ratio_min"] <= saved_ratio <= profile["history_ratio_max"]:
                    desired = saved
            desired = max(int(pane_width * profile["history_ratio_min"]), min(desired, int(pane_width * profile["history_ratio_max"])))
            self.content_pane.sashpos(0, desired)
        except TclError as e:
            print(f"Sash 위치 적용 중 오류 발생 (무시 가능): {e}")

    def _text_pixel_width(self, text, font_tuple):
        try:
            return tkFont.Font(root=self, font=font_tuple).measure(str(text))
        except Exception:
            try:
                font_size = abs(int(font_tuple[1]))
            except Exception:
                font_size = 12
            weighted_length = sum(1.8 if ord(char) > 127 else 1.0 for char in str(text))
            return int(weighted_length * font_size * 0.62)

    def _fit_text_to_width(self, text_options, base_size, available_width, min_size=11, weight="bold", margin=12):
        options = [str(text) for text in text_options if str(text)]
        if not options:
            return "", max(min_size, int(base_size or min_size))
        available_width = max(1, int(available_width or 1))
        base_size = max(min_size, int(base_size or min_size))
        for size in range(base_size, min_size - 1, -1):
            font_tuple = (self.default_font_name, size, weight)
            for text in options:
                if self._text_pixel_width(text, font_tuple) + margin <= available_width:
                    return text, size
        return options[-1], min_size

    def _heading_text_for_width(self, text_options, available_width, font_tuple, margin=14):
        options = [str(text) for text in text_options if str(text)]
        if not options:
            return ""
        available_width = max(1, int(available_width or 1))
        for text in options:
            if self._text_pixel_width(text, font_tuple) + margin <= available_width:
                return text
        return options[-1]

    def _tree_heading_fit_size(self, base_size, min_size=11, margin=14):
        column_specs = []
        for tree_name, label_map in (("history_tree", self.HISTORY_HEADING_LABELS), ("summary_tree", self.SUMMARY_HEADING_LABELS)):
            tree = self.__dict__.get(tree_name)
            if tree is None:
                continue
            for col, labels in label_map.items():
                try:
                    column_specs.append((max(1, int(tree.column(col, "width") or 1)), labels))
                except TclError:
                    continue
        if not column_specs:
            return max(min_size, int(base_size or min_size))

        base_size = max(min_size, int(base_size or min_size))
        for size in range(base_size, min_size - 1, -1):
            font_tuple = (self.default_font_name, size, "bold")
            if all(
                any(self._text_pixel_width(label, font_tuple) + margin <= width for label in labels)
                for width, labels in column_specs
            ):
                return size
        return min_size

    def _scaled_widths_to_total(self, min_widths, total_width, floor=42):
        total_width = max(len(min_widths), int(total_width or 0))
        floor = max(24, min(floor, total_width // max(len(min_widths), 1)))
        min_total = sum(min_widths.values())
        if min_total <= total_width:
            return dict(min_widths)

        scale = total_width / max(min_total, 1)
        widths = {col: max(floor, int(width * scale)) for col, width in min_widths.items()}
        overflow = sum(widths.values()) - total_width
        while overflow > 0:
            candidates = [col for col, width in widths.items() if width > floor]
            if not candidates:
                break
            target = max(candidates, key=lambda col: widths[col])
            widths[target] -= 1
            overflow -= 1
        return widths

    def _history_header_text_options(self):
        full_text = self.__dict__.get("_history_header_full_text")
        if not full_text and "hist_header_label" in self.__dict__:
            full_text = self.hist_header_label.cget("text")
        full_text = full_text or "스캔 기록"
        options = [full_text]
        if full_text.startswith("스캔 기록 (") and full_text.endswith(")"):
            suffix = full_text[full_text.find("(") + 1:-1]
            short_suffix = suffix[5:] if len(suffix) == 10 and suffix[4] == "-" else suffix
            options.extend([f"기록 ({short_suffix})", "기록"])
        elif full_text != "스캔 기록":
            options.extend(["스캔 기록", "기록"])
        else:
            options.append("기록")
        return tuple(dict.fromkeys(options))

    def _fit_section_header_label(self, label, text_options, available_width, base_size, min_size):
        text, size = self._fit_text_to_width(text_options, base_size, available_width, min_size=min_size, weight="bold", margin=16)
        label.configure(text=text, font=(self.default_font_name, size, "bold"), wraplength=max(40, int(available_width or 40)))

    def _configure_history_control_buttons(self, compact=False):
        signature = (
            bool(compact),
            int(self.__dict__.get("_current_font_size", 14)),
        )
        if self.__dict__.get("_history_control_style_signature") == signature:
            return
        style_name = "Compact.Control.TButton" if compact else "Control.TButton"
        if compact:
            font_size = max(10, min(18, int(self.__dict__.get("_current_font_size", 14) * 0.72)))
            self.style.configure(
                "Compact.Control.TButton",
                font=(self.default_font_name, font_size, "bold"),
                padding=(6, 4),
                background=self.colors["card_background"],
                foreground=self.colors["text"],
                relief="groove",
                borderwidth=2,
                bordercolor=self.colors["border"],
                focuscolor=self.colors["primary_active"],
            )
            self.style.map(
                "Compact.Control.TButton",
                background=[('active', self.colors["background"]), ('focus', self.colors["background"])],
                relief=[('focus', 'solid')],
            )
        for button_name, compact_width, normal_width in (
            ("today_button", 5, 8),
            ("date_search_button", 5, 8),
            ("decrease_font_button", 3, 4),
            ("increase_font_button", 3, 4),
        ):
            button = self.__dict__.get(button_name)
            if button is not None:
                button.configure(
                    style=style_name,
                    width=compact_width if compact else normal_width,
                )
        today_button = self.__dict__.get("today_button")
        date_button = self.__dict__.get("date_search_button")
        if today_button is not None:
            today_button.pack_configure(padx=(0, 4 if compact else 5))
        if date_button is not None:
            date_button.pack_configure(padx=(0, 8 if compact else 15))
        self._history_control_style_signature = signature

    def _normal_history_control_requested_width(
        self,
        _control_frame,
        date_button,
        base_header_size,
    ):
        """Measure normal history controls once per font signature.

        A width that ultimately needs compact controls must not probe normal
        styling again on every Configure event.  Reusing this request keeps
        the adaptive decision deterministic without an idle callback.
        """

        key = (
            str(self.default_font_name),
            int(self.__dict__.get("_current_font_size", 14)),
            int(base_header_size),
        )
        cache = self.__dict__.setdefault(
            "_history_normal_control_reqwidth_cache",
            {},
        )
        cached = cache.get(key)
        if cached is not None:
            return int(cached)
        if date_button is not None and date_button.cget("text") != "날짜 조회":
            date_button.configure(text="날짜 조회")
        self._configure_history_control_buttons(compact=False)
        requested = self._history_control_buttons_requested_width()
        cache[key] = requested
        return requested

    def _history_control_buttons_requested_width(self):
        """Sum immediate button requests and pack padding.

        Tk updates each child's request synchronously after a style change,
        while the parent frame can retain the previous request until idle.
        Reading the children avoids caching that one-frame-old parent width.
        """

        def horizontal_padding(widget):
            try:
                raw = widget.pack_info().get("padx", 0)
            except (TclError, AttributeError, TypeError):
                return 0
            if isinstance(raw, (tuple, list)):
                values = tuple(raw)
            else:
                try:
                    values = tuple(self.tk.splitlist(raw))
                except (TclError, AttributeError, TypeError):
                    values = tuple(str(raw).replace("{", "").replace("}", "").split())
            if not values:
                return 0
            try:
                pixels = [max(0, int(float(str(value)))) for value in values]
            except (TypeError, ValueError):
                return 0
            if len(pixels) == 1:
                return pixels[0] * 2
            return pixels[0] + pixels[-1]

        total = 0
        for name in (
            "today_button",
            "date_search_button",
            "decrease_font_button",
            "increase_font_button",
        ):
            button = self.__dict__.get(name)
            if button is None:
                continue
            try:
                total += max(0, int(button.winfo_reqwidth()))
            except (TclError, AttributeError, TypeError, ValueError):
                continue
            total += horizontal_padding(button)
        return total

    def _apply_adaptive_header_fitting(self):
        required_widgets = ("hist_header_label", "hist_header_frame", "summary_header_label", "summary_header_frame", "summary_date_label", "summary_card", "history_tree", "summary_tree")
        if any(widget_name not in self.__dict__ for widget_name in required_widgets):
            return
        if self.__dict__.get("_adaptive_header_fitting_active"):
            return
        self._adaptive_header_fitting_active = True
        try:
            profile = getattr(self, "ui_profile", self.UI_PROFILES["standard"])
            profile_name = self.__dict__.get("ui_profile_name", "standard")
            base_header_size = self.__dict__.get("_current_header_font_size", 18)
            min_header_size = 12 if profile_name in {"small", "compact"} else 13

            hist_header_width = self.hist_header_frame.winfo_width()
            history_notebook = self.__dict__.get("operator_history_notebook")
            if history_notebook is None:
                history_notebook = self.__dict__.get("operator_notebook")
            try:
                history_notebook_width = int(history_notebook.winfo_width())
            except (TclError, AttributeError, TypeError, ValueError):
                history_notebook_width = 0
            if (
                self.__dict__.get("operator_workbench_ready")
                and history_notebook_width > 20
            ):
                # The selected page has 8 px padding plus the notebook border
                # on each side.  Unlike a hidden page's remembered width, the
                # notebook itself is always realized and therefore stable.
                hist_header_width = history_notebook_width - 20
            elif hist_header_width <= 1:
                hist_header_width = self.history_card.winfo_width() - (profile.get("card_padding", 16) * 2)
            hist_header_width = max(120, int(hist_header_width or 120))

            control_frame = self.__dict__.get("hist_control_frame")
            control_width = 0
            if control_frame:
                date_button = self.__dict__.get("date_search_button")
                full_header_width = self._text_pixel_width(
                    self._history_header_text_options()[0],
                    (self.default_font_name, base_header_size, "bold"),
                ) + 16
                # Base the choice only on the realized notebook viewport.  A
                # previous hidden-page width must not make 1920 px overflow
                # while the same controls fit at a smaller or larger size.
                compact_controls = hist_header_width < 620
                if not compact_controls:
                    normal_control_width = (
                        self._normal_history_control_requested_width(
                            control_frame,
                            date_button,
                            base_header_size,
                        )
                    )
                    compact_controls = (
                        full_header_width + normal_control_width + 18
                        > hist_header_width
                    )
                date_text = "조회" if compact_controls else "날짜 조회"
                if date_button is not None and date_button.cget("text") != date_text:
                    date_button.configure(text=date_text)
                self._configure_history_control_buttons(compact=compact_controls)
                control_width = self._history_control_buttons_requested_width()
                self._history_controls_compact = compact_controls
                self.__dict__.pop(
                    "_history_control_compact_decision_width",
                    None,
                )
                should_stack_controls = (
                    control_width + full_header_width + 18
                    > hist_header_width
                )
                if self.__dict__.get("_history_controls_stacked") != should_stack_controls:
                    if should_stack_controls:
                        self.hist_header_label.grid_configure(row=0, column=0, columnspan=3, sticky="w")
                        control_frame.grid_configure(row=1, column=0, columnspan=3, sticky="e", pady=(6, 0))
                    else:
                        self.hist_header_label.grid_configure(row=0, column=0, columnspan=1, sticky="w")
                        control_frame.grid_configure(row=0, column=2, columnspan=1, sticky="e", pady=0)
                    self._history_controls_stacked = should_stack_controls
                hist_label_width = (
                    hist_header_width - 12
                    if should_stack_controls
                    else hist_header_width - control_width - 20
                )
            else:
                hist_label_width = hist_header_width - 12
            self._fit_section_header_label(
                self.hist_header_label,
                self._history_header_text_options(),
                max(70, hist_label_width),
                base_header_size,
                min_header_size,
            )

            summary_width = self.summary_header_frame.winfo_width()
            if summary_width <= 1:
                summary_width = self.summary_card.winfo_width()
            if summary_width <= 1:
                summary_width = self.summary_card.winfo_reqwidth()
            summary_width = max(90, int(summary_width or 120))
            date_width = max(0, self.summary_date_label.winfo_reqwidth())
            should_stack_summary_date = date_width + max(110, int(summary_width * 0.45)) + 12 > summary_width
            if should_stack_summary_date:
                self.summary_header_label.grid_configure(row=0, column=0, columnspan=2, sticky="w")
                self.summary_date_label.grid_configure(row=1, column=0, columnspan=2, sticky="w", padx=0, pady=(4, 0))
                summary_label_width = summary_width - 8
            else:
                self.summary_header_label.grid_configure(row=0, column=0, columnspan=1, sticky="w")
                self.summary_date_label.grid_configure(row=0, column=1, columnspan=1, sticky="e", padx=(8, 0), pady=0)
                summary_label_width = summary_width - date_width - 20
            self._fit_section_header_label(
                self.summary_header_label,
                ("누적 통과 코드", "통과 코드", "누적"),
                summary_label_width,
                base_header_size,
                min_header_size,
            )

            heading_size = self._tree_heading_fit_size(self.__dict__.get("_current_tree_heading_font_size", 14))
            heading_font = (self.default_font_name, heading_size, "bold")
            self._current_effective_tree_heading_font_size = heading_size
            self.style.configure("Treeview.Heading", font=heading_font)
            for col, labels in self.HISTORY_HEADING_LABELS.items():
                width = self.history_tree.column(col, "width")
                self.history_tree.heading(col, text=self._heading_text_for_width(labels, width, heading_font))
            for col, labels in self.SUMMARY_HEADING_LABELS.items():
                width = self.summary_tree.column(col, "width")
                self.summary_tree.heading(col, text=self._heading_text_for_width(labels, width, heading_font))
        except (TclError, KeyError):
            pass
        finally:
            self._adaptive_header_fitting_active = False

    def _dialog_size(self, kind):
        base_sizes = {
            "settings": (600, 240),
            "about": (620, 500),
            "calendar": (430, 430),
            "barcode_detail": (860, 560),
        }
        width, height = base_sizes.get(kind, (560, 360))
        scale = getattr(self, "ui_profile", self.UI_PROFILES["standard"]).get("dialog_scale", 1.0)
        screen_width = max(self.winfo_screenwidth(), 1)
        screen_height = max(self.winfo_screenheight(), 1)
        width = min(int(width * scale), max(420, screen_width - 80))
        height = min(int(height * scale), max(300, screen_height - 100))
        return width, height

    def _center_child_window(self, window, width=None, height=None):
        try:
            window.update_idletasks()
            width = width or window.winfo_width()
            height = height or window.winfo_height()
            parent_x = self.winfo_rootx()
            parent_y = self.winfo_rooty()
            parent_width = max(self.winfo_width(), self.winfo_screenwidth())
            parent_height = max(self.winfo_height(), self.winfo_screenheight())
            x = parent_x + max((parent_width - width) // 2, 0)
            y = parent_y + max((parent_height - height) // 2, 0)
            window.geometry(f"{width}x{height}+{x}+{y}")
        except TclError:
            pass

    def _destroy_modal_and_refocus(self, window):
        try:
            window.destroy()
        finally:
            if "entry" in self.__dict__:
                try:
                    self.after(0, self.entry.focus_set)
                except Exception:
                    self.entry.focus_set()

    def _load_items_data(self):
        items_path = resource_path(os.path.join("assets", self.FILES.ITEMS))
        if not os.path.exists(items_path):
            os.makedirs(os.path.dirname(items_path), exist_ok=True)
            with open(items_path, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(['Item Code', 'Item Name', 'Spec'])
                writer.writerow(['VALID-MASTER1', '테스트제품A', 'SPEC-A'])
                writer.writerow(['VALID-MASTER2', '테스트제품B', 'SPEC-B'])
                writer.writerow(['CLC-001', '고객사-제품1', 'C-SPEC-1'])

        try:
            with open(items_path, 'r', encoding='utf-8-sig') as f:
                return {row['Item Code']: row for row in csv.DictReader(f)}
        except FileNotFoundError:
            if not self.run_tests:
                messagebox.showwarning("기준 정보 파일 없음", f"품목 정보 파일({self.FILES.ITEMS})이 없어 품목명을 표시할 수 없습니다.\n프로그램 폴더 내 'assets' 폴더를 확인해주세요.")
            return {}
        except Exception as e:
            if not self.run_tests:
                messagebox.showerror("기준 정보 로드 오류", f"품목 정보를 불러오는 중 오류가 발생했습니다.\n\n[상세 오류]\n{e}")
            return {}

    def _record_app_close_failure(self, context, result):
        try:
            evidence = _label_match_write_session_direct_sync_result(
                context,
                reason=self.Events.APP_CLOSE,
                result=result,
            )
            return {**result, "evidence": evidence}
        except Exception as exc:
            return {**result, "evidence_error": str(exc)}

    def _run_app_close_direct_sync_worker(self, context, result_queue, deadline_monotonic):
        result = None
        try:
            pending_threads = list(self.__dict__.get("direct_sync_session_threads", []))
            legacy_pending = self.__dict__.get("direct_sync_session_thread")
            if legacy_pending is not None and legacy_pending not in pending_threads:
                pending_threads.append(legacy_pending)
            for pending in pending_threads:
                if pending is None or not pending.is_alive():
                    continue
                remaining = max(0.0, deadline_monotonic - time.monotonic())
                if remaining <= 0:
                    result = self._record_app_close_failure(
                        context,
                        {
                            "status": "FAIL",
                            "reason": self.Events.APP_CLOSE,
                            "error": "tracked TRAY sync exceeded the app-close deadline",
                            "error_code": "TRAY_SYNC_JOIN_TIMEOUT",
                        },
                    )
                    break
                pending.join(timeout=remaining)
                if pending.is_alive():
                    result = self._record_app_close_failure(
                        context,
                        {
                            "status": "FAIL",
                            "reason": self.Events.APP_CLOSE,
                            "error": "tracked TRAY sync exceeded the app-close deadline",
                            "error_code": "TRAY_SYNC_JOIN_TIMEOUT",
                        },
                    )
                    break
            if result is None:
                result = _label_match_run_and_record_session_direct_sync(
                    context,
                    reason=self.Events.APP_CLOSE,
                    deadline_monotonic=deadline_monotonic,
                )
        except Exception as exc:
            result = self._record_app_close_failure(
                context,
                {
                    "status": "FAIL",
                    "reason": self.Events.APP_CLOSE,
                    "error": "APP_CLOSE worker failed",
                    "error_code": "APP_CLOSE_WORKER_ERROR",
                    "error_type": exc.__class__.__name__,
                },
            )
        try:
            result_queue.put_nowait(result)
        except queue.Full:
            pass

    def _poll_app_close_direct_sync(self):
        thread = self.__dict__.get("_app_close_sync_thread")
        if thread is not None and thread.is_alive():
            deadline = self.__dict__.get("_app_close_deadline_monotonic")
            if deadline is not None and time.monotonic() >= deadline:
                context = self.__dict__.get("direct_sync_bootstrap_context") or _label_match_direct_sync_context(
                    self.save_directory,
                    getattr(self, "app_settings_path", ""),
                )
                result = self._record_app_close_failure(
                    context,
                    {
                        "status": "FAIL",
                        "reason": self.Events.APP_CLOSE,
                        "error": "APP_CLOSE shutdown deadline exceeded",
                        "error_code": "APP_CLOSE_SHUTDOWN_DEADLINE_EXCEEDED",
                    },
                )
                self.app_close_direct_sync_result = result
                self._save_app_settings()
                self.destroy()
                return
            self._app_close_poll_after_id = self.after(100, self._poll_app_close_direct_sync)
            return
        result_queue = self.__dict__.get("_app_close_sync_result_queue")
        try:
            result = result_queue.get_nowait() if result_queue is not None else None
        except queue.Empty:
            result = None
        if not isinstance(result, dict):
            result = {
                "status": "FAIL",
                "reason": self.Events.APP_CLOSE,
                "error": "APP_CLOSE direct-sync worker ended without a result",
            }
        self.app_close_direct_sync_result = result
        self._save_app_settings()
        self.destroy()

    def _begin_app_close_direct_sync(self, context):
        deadline_monotonic = time.monotonic() + LABEL_MATCH_APP_CLOSE_TOTAL_TIMEOUT_SECONDS
        result_queue = queue.Queue(maxsize=1)
        thread = threading.Thread(
            target=self._run_app_close_direct_sync_worker,
            args=(context, result_queue, deadline_monotonic),
            daemon=True,
            name="label-match-app-close-direct-sync",
        )
        self._app_close_sync_result_queue = result_queue
        self._app_close_sync_thread = thread
        self._app_close_deadline_monotonic = deadline_monotonic
        thread.start()
        self._app_close_poll_after_id = self.after(100, self._poll_app_close_direct_sync)

    def on_closing(self):
        if self.__dict__.get("_app_close_in_progress", False):
            return
        if not self.initialized_successfully:
            self._cancel_pending_ui_jobs()
            self.destroy()
            return
        if self._has_background_work():
            if not self.run_tests:
                messagebox.showwarning("작업 진행 중", "테스트 시뮬레이션 또는 테스트 로그 생성이 진행 중입니다.\n작업이 끝난 뒤 프로그램을 종료하세요.")
            return
        
        do_close = self.run_tests or messagebox.askokcancel("종료 확인", "프로그램을 종료하시겠습니까?")

        if do_close:
            self._app_close_in_progress = True
            self.is_blinking = False
            self._cancel_pending_ui_jobs()
            entry = self.__dict__.get("entry")
            if entry is not None:
                try:
                    configure_entry = getattr(entry, "configure", None) or getattr(entry, "config")
                    configure_entry(state="disabled")
                except Exception:
                    pass
            try:
                self.data_manager.log_event(self.Events.APP_CLOSE, {"message": "Application closed."})
                self.data_manager.close(timeout=LABEL_MATCH_APP_CLOSE_LOG_TIMEOUT_SECONDS)
            except Exception as e:
                self._app_close_in_progress = False
                if entry is not None:
                    try:
                        configure_entry = getattr(entry, "configure", None) or getattr(entry, "config")
                        configure_entry(state="normal")
                    except Exception:
                        pass
                self._replace_closed_data_manager_after_close_failure(self.data_manager)
                if self.run_tests:
                    raise
                messagebox.showerror("종료 보류", f"작업 로그 저장을 완료하지 못해 종료를 중단했습니다.\n\n[상세 오류]\n{e}")
                return
            if not self.run_tests:
                context = getattr(self, "direct_sync_bootstrap_context", None) or _label_match_direct_sync_context(
                    self.save_directory,
                    getattr(self, "app_settings_path", ""),
                )
                context = _label_match_bind_current_log_source(context, self.data_manager)
                self.direct_sync_bootstrap_context = context
                self._begin_app_close_direct_sync(context)
                return
            self._save_app_settings()
            self.destroy()

    def _cancel_pending_ui_jobs(self):
        for attr_name in ("_responsive_after_id", "_zoom_after_id", "_ui_redraw_after_id", "_clock_after_id"):
            after_id = self.__dict__.get(attr_name)
            if not after_id:
                continue
            try:
                self.after_cancel(after_id)
            except TclError:
                pass
            self.__dict__[attr_name] = None

    def _replace_closed_data_manager_after_close_failure(self, failed_manager):
        if not getattr(failed_manager, '_close_requested', False):
            return False
        log_thread = getattr(failed_manager, 'log_thread', None)
        if log_thread is not None and log_thread.is_alive():
            return False
        try:
            self.data_manager = DataManager(
                failed_manager.save_directory,
                failed_manager.process_name,
                failed_manager.worker_name,
                failed_manager.unique_id,
            )
            return True
        except Exception as replacement_error:
            print(f"로그 매니저 복구 실패: {replacement_error}")
            return False

    def _save_current_set_state(self):
        if not self.initialized_successfully or not self.current_set_info['raw']: return
        state_data = {'current_set_info': self.current_set_info, 'timestamp': datetime.now().isoformat()}
        self.data_manager.save_current_state(state_data)

    def _load_current_set_state(self):
        state_data = self.data_manager.load_current_state()
        if not state_data: return
        try:
            saved_timestamp_str = state_data.get('timestamp')
            if saved_timestamp_str:
                saved_dt = datetime.fromisoformat(saved_timestamp_str)
                if saved_dt.date() != datetime.now().date():
                    if not self.run_tests:
                        messagebox.showinfo("이전 작업 만료", "어제 완료되지 않은 작업 데이터는 자동으로 삭제됩니다.")
                    self.data_manager.delete_current_state()
                    return
        except (ValueError, TypeError) as e:
            print(f"저장된 타임스탬프 파싱 오류: {e}. 이전 작업을 무시합니다.")
            self.data_manager.delete_current_state()
            return

        msg = f"이전에 완료되지 않은 스캔 세트가 있습니다.\n(스캔 수: {len(state_data.get('current_set_info', {}).get('raw', []))})\n\n이어서 진행하시겠습니까?"
        
        should_restore = self.run_tests or messagebox.askyesno("작업 복구", msg)

        if should_restore:
            saved_worker_name = state_data.get('worker_name')
            if saved_worker_name and saved_worker_name != self.worker_name:
                response = True
                if not self.run_tests:
                    response = messagebox.askyesnocancel("작업자 불일치",
                                                       f"이 저장된 세트는 '{saved_worker_name}' 작업자의 것입니다.\n"
                                                       f"현재 '{self.worker_name}' 작업자가 이어서 하시겠습니까?",
                                                       icon='warning')
                if response is None: return
                elif response is False:
                    self.data_manager.delete_current_state()
                    if not self.run_tests:
                        messagebox.showinfo("작업 삭제", "이전 작업이 삭제되었습니다.")
                    return
            saved_set_info = state_data.get('current_set_info', {})
            self.current_set_info.update(saved_set_info)

            if self.current_set_info.get('start_time') and isinstance(self.current_set_info['start_time'], str):
                self.current_set_info['start_time'] = datetime.fromisoformat(self.current_set_info['start_time'])
            self.data_manager.log_event(self.Events.SET_RESTORED, {"restored_set": self.current_set_info, "continued_by": self.worker_name})
            self.progress_bar['value'] = len(self.current_set_info['raw'])
            if self.current_set_info.get("exact_rescan_active"):
                completed = len(self.current_set_info.get("exact_rescan_barcodes") or [])
                target = int(self.current_set_info.get("exact_rescan_target_count") or 0)
                self.update_big_display(f"전체 제품 재스캔 {completed}/{target}", "primary")
            else:
                self.update_big_display(self._next_action_text(len(self.current_set_info.get('parsed', []))), "green")
            self._update_status_label()
            self._update_history_tree_in_progress()
            self._workflow_recovered = True
            self._render_operator_workbench()
        else:
            self.data_manager.delete_current_state()

    def _delete_current_set_state(self):
        self.data_manager.delete_current_state()

    def _flush_data_manager_if_supported(self, timeout=5.0):
        flush = getattr(self.data_manager, "flush", None)
        if callable(flush):
            flush(timeout=timeout)

    def _history_load_updates_active_state(self, target_date=None):
        return target_date is None or target_date.date() == datetime.now().date()

    def _has_background_work(self):
        state = self.__dict__
        return bool(state.get('is_running_simulation', False) or state.get('is_generating_test_logs', False))

    def _load_history_and_rebuild_summary(self, target_date=None):
        print(f"과거 기록 비동기 로드 시작... (대상 날짜: {target_date or '오늘'})")
        self.history_load_generation += 1
        load_generation = self.history_load_generation
        while True:
            try:
                self.history_queue.get_nowait()
            except queue.Empty:
                break
        updates_active_state = self._history_load_updates_active_state(target_date)
        self.history_view_updates_active_state = updates_active_state
        self.history_load_pending = True
        self.history_active_load_pending = updates_active_state
        if self.__dict__.get("operator_workbench_ready"):
            # Apply the loading/read-only gate before the worker starts so a
            # cached enabled view cannot accept a scan or function key.
            self._render_operator_workbench()
        if updates_active_state:
            self.scan_count.clear()
            self.global_scanned_set.clear()
            self.set_details_map.clear()
        self.history_tree.delete(*self.history_tree.get_children())
        self.summary_tree.delete(*self.summary_tree.get_children())

        if target_date:
            date_str = target_date.strftime('%Y-%m-%d')
            self._history_header_full_text = f"스캔 기록 ({date_str})"
            self.hist_header_label.config(text=self._history_header_full_text)
            self._set_summary_date_label(text=f"날짜 {date_str}")
        else:
            self._history_header_full_text = "스캔 기록 (오늘)"
            self.hist_header_label.config(text=self._history_header_full_text)
            self._set_summary_date_label(text=f"날짜 {datetime.now().strftime('%Y-%m-%d')}")
        self._apply_adaptive_header_fitting()

        loading_values = ("", "기록을 불러오는 중입니다...", *[""] * (self.TOTAL_SCAN_COUNT - 1), "", "")
        self.history_tree.insert("", "end", iid="loading", values=loading_values, tags=("in_progress",))
        loader_thread = threading.Thread(target=self._async_load_history_task, args=(self.history_queue, target_date, updates_active_state, load_generation), daemon=True)
        loader_thread.start()

    def _async_load_history_task(self, result_queue, target_date=None, updates_active_state=None, load_generation=None):
        try:
            if updates_active_state is None:
                updates_active_state = self._history_load_updates_active_state(target_date)
            completed_sets = {}
            voided_set_ids = set()
            cancelled_set_ids = set()

            log_filepath = self.data_manager._get_log_filepath(target_date)

            if os.path.exists(log_filepath):
                try:
                    with open(log_filepath, 'r', encoding='utf-8-sig') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            event = row.get('event')
                            details_str = row.get('details', '{}')
                            if not details_str: continue
                            try:
                                details = json.loads(details_str)
                            except json.JSONDecodeError:
                                print(f"경고: JSON 파싱 오류. 건너뜁니다: {details_str}")
                                continue

                            set_id = details.get('set_id')
                            if event == self.Events.SET_DELETED and details.get('set_id'):
                                voided_set_ids.add(details['set_id'])
                                continue
                            if event == self.Events.TRAY_COMPLETION_CANCELLED and details.get('cancelled_set_id'):
                                cancelled_set_ids.add(details.get('cancelled_set_id'))
                                continue

                            if set_id is None: continue

                            if event == self.Events.TRAY_COMPLETE:
                                displays = details.get('parsed_product_barcodes', [])
                                first_scan = displays[0] if displays else "N/A"
                                other_scans = displays[1:self.TOTAL_SCAN_COUNT]

                                timestamp_str = datetime.fromisoformat(row.get('timestamp', '')).strftime('%H:%M:%S')
                                result_display = _label_match_tray_complete_result(details)
                                values_to_display = (
                                    set_id,
                                    first_scan,
                                    *other_scans + [""] * ((self.TOTAL_SCAN_COUNT - 1) - len(other_scans)),
                                    result_display,
                                    timestamp_str,
                                )

                                completed_sets[set_id] = {'values': values_to_display, 'tags': ("success" if _label_match_tray_complete_passed(details) else "error",), 'details': details}

                except Exception as e:
                    print(f"기록 파일 로드 오류 ({log_filepath}): {e}")

            final_sets = {sid: data for sid, data in completed_sets.items() if sid not in voided_set_ids and sid not in cancelled_set_ids}
            def _history_sort_key(item):
                set_id, data = item
                details = data.get('details', {}) if isinstance(data, dict) else {}
                return (
                    details.get('end_time')
                    or details.get('timestamp')
                    or details.get('start_time')
                    or str(set_id)
                )

            sorted_final_sets = sorted(final_sets.items(), key=_history_sort_key)
            temp_scan_count = defaultdict(lambda: defaultdict(int))
            temp_global_scanned_set = set()
            temp_set_details_map = {sid: data['details'] for sid, data in final_sets.items()}
            for set_id, data in sorted_final_sets:
                details = data['details']
                if _label_match_tray_complete_passed(details):
                    passed_code = details.get('item_code')
                    production_date = details.get('production_date')
                    phase = details.get('phase') or '-'
                    if passed_code and production_date:
                        temp_scan_count[production_date][(passed_code, phase)] += 1
                    temp_global_scanned_set.update(_label_match_duplicate_index_barcodes(details))

            result_queue.put({
                'sorted_sets': sorted_final_sets,
                'scan_count': temp_scan_count,
                'global_scanned_set': temp_global_scanned_set,
                'set_details_map': temp_set_details_map,
                'updates_active_state': updates_active_state,
                'load_generation': load_generation,
            })
        except Exception as e:
            print(f"백그라운드 기록 로딩 오류: {e}")
            result_queue.put({'error': str(e), 'load_generation': load_generation})

    def _process_history_queue(self):
        try:
            while True:
                result = self.history_queue.get_nowait()
                if result.get('load_generation') is not None and result.get('load_generation') != self.history_load_generation:
                    continue
                break
            if self.history_tree.exists("loading"): self.history_tree.delete("loading")
            if 'error' in result:
                self.history_load_pending = False
                self.history_active_load_pending = False
                if not self.run_tests:
                    messagebox.showerror("기록 로딩 오류", f"작업 기록을 불러오는 중 오류가 발생했습니다.\n로그 파일이 손상되었을 수 있습니다.\n\n[오류 원인]\n{result['error']}")
                return
            updates_active_state = result.get('updates_active_state', True)
            self.history_view_updates_active_state = updates_active_state
            self.history_load_pending = False
            self.history_active_load_pending = False
            if updates_active_state:
                self.scan_count = result['scan_count']
                self.global_scanned_set = result['global_scanned_set']
                self.set_details_map = result['set_details_map']
            self.history_row_details_map = result.get('set_details_map', {})
            sorted_final_sets = result['sorted_sets']
            for index, (set_id, data) in enumerate(sorted_final_sets, 1):
                values = list(data['values'])
                values[0] = index
                display_values = self._history_values_for_display(values)
                self.history_tree.insert("", "end", iid=str(set_id), values=display_values, tags=data['tags'])
            self._render_summary_tree(result['scan_count'] if not updates_active_state else self.scan_count)
            self._apply_history_view_mode()
            self._render_history_detail()
            self._refresh_session_tree()
            self._render_operator_workbench()
            print("비동기 기록 로드 및 UI 적용 완료.")
        except queue.Empty:
            if self.__dict__.get('history_load_pending', self.__dict__.get('history_active_load_pending', False)):
                self.after(100, self._process_history_queue)
        except Exception as e:
            print(f"UI 업데이트 중 오류 발생: {e}")
            if self.history_tree.exists("loading"): self.history_tree.delete("loading")
            if not self.run_tests:
                messagebox.showerror("UI 업데이트 오류", f"기록을 화면에 표시하는 과정에서 예상치 못한 오류가 발생했습니다.\n프로그램을 다시 시작해주세요.\n\n[상세 오류]\n{e}")

    def _parse_new_format_label(self, raw_input):
        return _label_match_parse_new_format_fields(raw_input)

    def _run_auto_test_simulation(self):
        """사용자 상호작용을 시뮬레이션하는 자동화된 테스트를 시작합니다."""
        if self.is_running_simulation:
            print("시뮬레이션이 이미 실행 중입니다.")
            return

        if not messagebox.askyesno("자동 테스트 시작", "자동화된 UI 테스트 시뮬레이션을 시작하시겠습니까?\n\n테스트 중에는 프로그램을 조작할 수 없습니다."):
            return

        print("\n" + "="*50)
        print("🚀 자동 GUI 테스트 시뮬레이션 시작 🚀")
        print("="*50)

        self.is_running_simulation = True
        self.entry.config(state='disabled')
        self.update_big_display("자동 테스트 시작...", "primary")

        today = datetime.now().strftime('%Y%m%d')
        self.simulation_scenarios = [
            {
                "name": "1. 정상 성공 사이클 (기본)",
                "steps": [
                    ("reset", None),
                    ("scan", "VALID-MASTER1"),
                    *[
                        ("scan", f"PRODUCT_VALID-MASTER1_{index}")
                        for index in range(1, self.PRODUCT_SAMPLE_COUNT + 1)
                    ],
                    ("scan", f"FINAL_LABEL_VALID-MASTER1\x1D6D{today}"),
                    ("check_history_len", 1),
                    ("check_last_history_result", self.Results.PASS),
                    ("check_summary_count", ("VALID-MASTER1", "-", 1)),
                ]
            },
            {
                "name": "2. 제품 불일치 오류 및 복구",
                "steps": [
                    ("reset", None),
                    ("scan", "VALID-MASTER2"),
                    ("scan", "PRODUCT_WRONG-CODE_XYZ"),
                    ("check_history_len", 2),
                    ("check_last_history_result", self.Results.FAIL_MISMATCH),
                    ("check_summary_count", ("VALID-MASTER1", "-", 1)),
                ]
            },
            {
                "name": "3. 세트 내 중복 스캔 오류 (오류 후 정상 완료)",
                "steps": [
                    ("reset", None),
                    ("scan", "VALID-MASTER1"),
                    ("scan", "PRODUCT_DUPE_TEST_1"),
                    ("scan", "PRODUCT_DUPE_TEST_1"),
                    ("check_current_scan_count", 2),
                    ("check_has_error_flag", True),
                    *[
                        ("scan", f"PRODUCT_DUPE_TEST_{index}")
                        for index in range(2, self.PRODUCT_SAMPLE_COUNT + 1)
                    ],
                    ("scan", f"FINAL_LABEL_VALID-MASTER1_DUPE\x1D6D{today}"),
                    ("check_history_len", 3),
                    ("check_last_history_result", self.Results.PASS),
                    ("check_last_history_error_flag", True),
                ]
            },
            {
                "name": "4. 전체 중복 스캔 오류",
                "steps": [
                    ("reset", None),
                    ("scan", "VALID-MASTER2"),
                    ("scan", "PRODUCT_VALID-MASTER1_1"),
                    ("check_current_scan_count", 1),
                    ("check_has_error_flag", True),
                ]
            },
            {
                "name": "5. 신규 Base64 포맷 라벨 정상 처리",
                "steps": [
                    ("reset", None),
                    ("scan", base64.b64encode('CLC=CLC-001|SPC=고객사-제품1|PHS=1'.encode('utf-8')).decode('utf-8')),
                    ("check_current_scan_count", 1),
                    ("check_item_override", "고객사-제품1"),
                    *[
                        ("scan", f"PRODUCT_CLC-001_{index}")
                        for index in range(1, self.PRODUCT_SAMPLE_COUNT + 1)
                    ],
                    ("scan", f"FINAL_LABEL_CLC-001\x1D6D{today}"),
                    ("check_history_len", 4),
                    ("check_summary_count", ("CLC-001", "1", 1)),
                ]
            },
            {
                "name": "6. F1 키 (현재 세트 취소) 시뮬레이션",
                "steps": [
                    ("reset", None),
                    ("scan", "VALID-MASTER1"),
                    ("scan", "PRODUCT_TO_CANCEL_1"),
                    ("action", "reset_set"),
                    ("check_current_scan_count", 0),
                    ("check_history_len", 4),
                ]
            }
        ]

        self.current_scenario_index = 0
        self.current_step_index = 0
        self.after(1000, self._execute_test_step)

    def _execute_test_step(self):
        """테스트 시나리오의 각 단계를 순차적으로 실행합니다."""
        if not self.is_running_simulation:
            return

        if self.current_scenario_index >= len(self.simulation_scenarios):
            self._finalize_simulation()
            return

        scenario = self.simulation_scenarios[self.current_scenario_index]
        steps = scenario["steps"]

        if self.current_step_index >= len(steps):
            print("-" * 50)
            self.current_scenario_index += 1
            self.current_step_index = 0
            self.after(1000, self._execute_test_step)
            return
            
        if self.current_step_index == 0:
            print(f"\n▶️  {scenario['name']}")
        
        action, value = steps[self.current_step_index]
        step_delay_ms = 600

        print(f"  - 스텝 {self.current_step_index + 1}: {action} / 값: {self._truncate_string(str(value), 50)}")
        
        try:
            if action == "scan":
                self.entry.delete(0, tk.END)
                self.entry.insert(0, value)
                self.process_input()
            elif action == "reset":
                if not self._reset_current_set(full_reset=True):
                    self.is_running_simulation = False
                    self.entry.config(state='normal')
                    self.entry.focus_set()
                    return
                self.history_tree.delete(*self.history_tree.get_children())
                self.summary_tree.delete(*self.summary_tree.get_children())
                self.scan_count.clear()
                self.global_scanned_set.clear()
                self.set_details_map.clear()
            elif action == "action":
                if value == "reset_set":
                    if not self._reset_current_set(full_reset=True):
                        self.is_running_simulation = False
                        self.entry.config(state='normal')
                        self.entry.focus_set()
                        return
            elif action.startswith("check_"):
                step_delay_ms = 100
                self._verify_test_step(action, value)
        except Exception as e:
            print(f"  ❌ 테스트 스텝 실행 중 오류 발생: {e}")

        self.current_step_index += 1
        self.after(step_delay_ms, self._execute_test_step)

    def _verify_test_step(self, check_action, expected_value):
        """테스트 단계를 검증하고 결과를 콘솔에 출력합니다."""
        success = False
        actual_value = "N/A"
        try:
            if check_action == "check_history_len":
                actual_value = len(self.history_tree.get_children())
                success = (actual_value == expected_value)
            elif check_action == "check_last_history_result":
                children = self.history_tree.get_children()
                if children:
                    last_item = self.history_tree.item(children[-1])
                    actual_value = self._history_result_value(last_item.get('values') or ())
                    success = (actual_value == expected_value)
            elif check_action == "check_summary_count":
                code, phase, count = expected_value
                actual_value = 0
                for raw_values in self.__dict__.get("summary_row_raw_values", {}).values():
                    if len(raw_values) >= 4 and raw_values[1] == code and raw_values[2] == phase:
                        actual_value = raw_values[3]
                        break
                for item_id in self.summary_tree.get_children():
                    if actual_value == count:
                        break
                    values = self.summary_tree.item(item_id)['values']
                    if len(values) >= 3 and values[0] == code and values[1] == phase:
                        actual_value = values[2]
                        break
                    if len(values) >= 4 and values[1] == code and values[2] == phase:
                        actual_value = values[3]
                        break
                success = (actual_value == count)
            elif check_action == "check_current_scan_count":
                actual_value = len(self.current_set_info['raw'])
                success = (actual_value == expected_value)
            elif check_action == "check_has_error_flag":
                actual_value = self.current_set_info.get('has_error_or_reset', False)
                success = (actual_value == expected_value)
            elif check_action == "check_item_override":
                actual_value = self.current_set_info.get('item_name_override')
                success = (actual_value == expected_value)
            elif check_action == "check_last_history_error_flag":
                children = self.history_tree.get_children()
                if children:
                    last_set_id = children[-1]
                    details = self.set_details_map.get(last_set_id, {})
                    actual_value = details.get('has_error_or_reset', False)
                    success = (actual_value == expected_value)

            if success:
                print(f"    ✅ 통과: {check_action} (기대: {expected_value}, 실제: {actual_value})")
            else:
                print(f"    ❌ 실패: {check_action} (기대: {expected_value}, 실제: {actual_value})")

        except Exception as e:
            print(f"    ❌ 검증 중 예외 발생: {e}")

    def _finalize_simulation(self):
        """테스트 시뮬레이션을 종료하고 상태를 초기화합니다."""
        print("\n" + "="*50)
        print("🎉 자동 GUI 테스트 시뮬레이션 완료 🎉")
        print("="*50)
        messagebox.showinfo("테스트 완료", "자동 테스트 시뮬레이션이 완료되었습니다.")
        self.is_running_simulation = False
        self.entry.config(state='normal')
        self.entry.focus_set()
        self._reset_current_set(full_reset=True)
    
    def _run_demonstration(self):
        """사람이 스캔하는 것처럼 UI를 변경하며 시연을 진행합니다."""
        self.entry.config(state='disabled')
        self._reset_current_set(full_reset=True)

        master_code = "VALID-MASTER1"
        today = datetime.now().strftime('%Y%m%d')
        demo_barcodes = [
            master_code,
            *(f"PRODUCT_{master_code}_DEMO{index}" for index in range(1, self.PRODUCT_SAMPLE_COUNT + 1)),
            f"FINAL_LABEL_{master_code}_DEMO\x1D6D{today}"
        ]

        self.update_big_display("데모 모드를 시작합니다...", "primary")
        self.after(1500, self._demo_step, 0, demo_barcodes)

    def _demo_step(self, index, barcodes):
        """시연의 각 단계를 처리하고, 다음 단계를 예약합니다."""
        if index >= len(barcodes):
            self.update_big_display("데모 완료!", "success")
            self.entry.config(state='normal')
            self.entry.focus_set()
            messagebox.showinfo("시연 완료", "데모 시연이 성공적으로 완료되었습니다.")
            return

        current_barcode = barcodes[index]
        
        self.entry.insert(0, current_barcode)
        self.process_input()
        
        self.after(1500, self._demo_step, index + 1, barcodes)
        
    def process_input(self, event=None):
        if self.__dict__.get("_app_close_in_progress", False):
            return
        raw_input = self.entry.get().strip()
        self.entry.delete(0, tk.END)

        if self.is_blinking or not self.initialized_successfully: return
        if not raw_input: return
        if raw_input in {'_RUN_AUTO_TEST_', '_RUN_DEMO_'}:
            if self._block_view_only_action("테스트 기능을 실행"):
                return
            if self._block_active_history_load_action("테스트 기능을 실행"):
                return

        if raw_input == '_RUN_AUTO_TEST_':
            self._run_auto_test_simulation()
            return
        
        elif raw_input == '_RUN_DEMO_':
            if messagebox.askyesno("시연 모드 시작", "성공 스캔 과정을 시연하시겠습니까?"):
                self._run_demonstration()
            return

        if self._block_view_only_action("스캔"):
            return
        if self._block_active_history_load_action("스캔"):
            return

        if self.current_set_info.get("exact_rescan_active"):
            self._process_exact_rescan_product(raw_input)
            return

        self.data_manager.log_event(self.Events.SCAN_ATTEMPT, {"raw_input": raw_input, "scan_pos": len(self.current_set_info['raw']) + 1})
        scan_pos = len(self.current_set_info['raw']) + 1
        
        processed_input = raw_input
        if scan_pos == 1:
            try:
                if '|' not in raw_input and len(raw_input) > 20:
                    temp_b64 = raw_input.replace('-', '+').replace('_', '/')
                    padded_b64 = temp_b64 + '=' * (-len(temp_b64) % 4)
                    decoded_bytes = base64.b64decode(padded_b64)
                    decoded_string = decoded_bytes.decode('utf-8')
                    if '|' in decoded_string and '=' in decoded_string:
                        processed_input = decoded_string
                        self.data_manager.log_event(self.Events.BASE64_DECODED, {"original": raw_input, "decoded": processed_input})
            except (binascii.Error, UnicodeDecodeError):
                pass

        if scan_pos == 1:
            try:
                transfer_label_data = _label_match_parse_sealed_transfer_qr(processed_input)
            except ValueError as exc:
                self._handle_input_error(
                    raw_input,
                    title="[이적 컨테이너 QR 오류]",
                    reason=str(exc),
                )
                return
            new_label_data = self._parse_new_format_label(processed_input)
            if transfer_label_data:
                duplicate_keys = _label_match_unique_master_index_keys(raw_input)
                duplicate_keys.update(_label_match_unique_master_index_keys(processed_input))
                if duplicate_keys & self.global_scanned_set:
                    self._handle_input_error(
                        raw_input,
                        title="[이적 컨테이너 중복 스캔]",
                        reason="이미 포장 처리된 sealed transfer bundle입니다.",
                    )
                    return
                client_code = str(transfer_label_data["CLC"])
                self.current_set_info["phase"] = "TRANSFER"
                self.current_set_info["sealed_transfer"] = transfer_label_data
                self._update_on_success_scan(raw_input, client_code)
            elif new_label_data:
                reusable_input_master = (
                    _label_match_reusable_input_master_label(raw_input)
                    or _label_match_reusable_input_master_label(processed_input)
                )
                duplicate_keys = _label_match_unique_master_index_keys(raw_input)
                duplicate_keys.update(_label_match_unique_master_index_keys(processed_input))
                if not reusable_input_master and duplicate_keys & self.global_scanned_set:
                    self._handle_input_error(
                        raw_input,
                        title="[현품표 중복 스캔]",
                        reason=f"이미 처리된 현품표입니다.\n\n- 중복 스캔: {self._truncate_string(raw_input)}\n\n→ 새 현품표로 다시 시작하세요."
                    )
                    return
                client_code = new_label_data.get('CLC')
                supplier_code = new_label_data.get('SPC')
                phase = new_label_data.get('PHS')
                self.current_set_info['phase'] = phase
                self.current_set_info['item_name_override'] = supplier_code
                self._update_on_success_scan(raw_input, client_code)
            else:
                MASTER_LABEL_LENGTH = 13
                is_test_code = any(s in raw_input for s in ["DEMO", "VALID-", "TEST_"])
                
                if not is_test_code and len(raw_input) != MASTER_LABEL_LENGTH and not self.items_data.get(raw_input):
                    self._handle_input_error(
                        raw_input,
                        title="[현품표 형식 오류]",
                        reason=f"잘못된 현품표 형식(13자리 아님)이거나 미등록 코드입니다.\n\n- 입력 값: {self._truncate_string(raw_input)}"
                    )
                    return
                if not is_test_code and raw_input not in self.items_data:
                    self._handle_input_error(
                        raw_input,
                        title="[미등록 현품표]",
                        reason=f"미등록 현품표입니다.\n\n- 미등록 코드: {self._truncate_string(raw_input)}\n\n→ Item.csv를 확인하세요."
                    )
                    return
                self._update_on_success_scan(raw_input, raw_input)

        elif 2 <= scan_pos <= self.TOTAL_SCAN_COUNT:
            if scan_pos == 2 and raw_input.upper().startswith("TEST_LOG_"):
                parts = raw_input.split('_')
                if len(parts) == 3 and parts[2].isdigit():
                    num_sets = int(parts[2])
                    master_code = self.current_set_info['parsed'][0]
                    confirm_msg = (f"현재 현품표 기준으로 {num_sets}개의 테스트 기록을 생성하시겠습니까?\n\n"
                                   f"▶ 현품표 코드: {master_code}\n\n"
                                   "(이 작업은 현재 진행중인 세트를 취소하고 시작됩니다.)")
                    should_run_sim = self.run_tests or messagebox.askyesno("테스트 데이터 생성", confirm_msg)
                    if should_run_sim:
                        self._reset_current_set(full_reset=True)
                        self.run_test_log_simulation(master_code, num_sets)
                    return
                else:
                    if not self.run_tests:
                        messagebox.showwarning("입력 형식 오류", "테스트 코드 형식이 올바르지 않습니다.\n(예: TEST_LOG_100)")
                    return

            master_code = self.current_set_info['parsed'][0]
            if scan_pos < self.FINAL_LABEL_SCAN_POSITION and len(raw_input) <= len(master_code):
                self._handle_input_error(
                    raw_input,
                    title="[바코드 종류 오류]",
                    reason=f"잘못된 바코드 종류입니다.\n\n- 스캔 값: {self._truncate_string(raw_input)}\n\n→ 제품 바코드를 스캔하세요."
                )
                return
            if scan_pos == self.FINAL_LABEL_SCAN_POSITION and len(raw_input) < 31:
                self._handle_input_error(
                    raw_input,
                    title="[라벨 형식 오류]",
                    reason=f"포장 라벨 길이가 너무 짧습니다.\n(입력: {len(raw_input)} / 최소: 31)\n\n→ 올바른 라벨을 사용하세요."
                )
                return
            if master_code not in raw_input:
                self._handle_mismatch(raw_input, master_code)
                return
            if raw_input in self.current_set_info['raw']:
                self._handle_input_error(
                    raw_input,
                    title="[세트 내 중복 스캔]",
                    reason=f"세트 내 중복 스캔입니다.\n\n- 중복 제품: {self._truncate_string(raw_input)}\n\n→ 다른 제품을 스캔하세요."
                )
                return
            if raw_input in self.global_scanned_set:
                self._handle_input_error(
                    raw_input,
                    title="[전체 작업 내 중복 스캔]",
                    reason=f"이미 다른 세트에서 처리된 제품입니다.\n\n- 중복 제품: {self._truncate_string(raw_input)}\n\n→ 새 제품으로 교체하세요."
                )
                return
            production_date = None
            if scan_pos == self.FINAL_LABEL_SCAN_POSITION:
                production_date = self._extract_production_date(raw_input)
                if not production_date:
                    self._handle_input_error(
                        raw_input,
                        title="[생산일자 누락]",
                        reason=f"라벨에서 생산일자(6D...)를 찾을 수 없습니다.\n\n- 스캔한 라벨: {self._truncate_string(raw_input)}\n\n→ 올바른 라벨을 사용하세요."
                    )
                    return
                self.current_set_info['production_date'] = production_date
            self._update_on_success_scan(raw_input, master_code)

    def _prompt_exact_rescan(self):
        raw = list(self.current_set_info.get("raw") or [])
        if not raw:
            if not self.run_tests:
                messagebox.showwarning("전체 재스캔", "먼저 현품표를 스캔하세요.", parent=self)
            return False
        try:
            if _label_match_parse_sealed_transfer_qr(raw[0]):
                if not self.run_tests:
                    messagebox.showinfo(
                        "전체 재스캔 불필요",
                        "sealed transfer QR은 서버 exact membership을 전체 상속합니다.",
                        parent=self,
                    )
                return False
        except ValueError as exc:
            if not self.run_tests:
                messagebox.showerror("전체 재스캔", str(exc), parent=self)
            return False
        if len(raw) != 1:
            if not self.run_tests:
                messagebox.showwarning(
                    "전체 재스캔",
                    "제품 샘플 스캔 전에 전체 재스캔을 시작하세요.",
                    parent=self,
                )
            return False
        source_bundle_id = str(
            self.current_set_info.get("exact_rescan_source_bundle_id") or ""
        ).strip()
        if not self.run_tests:
            source_bundle_id = str(
                simpledialog.askstring(
                    "이적 컨테이너 ID",
                    "QR을 사용할 수 없으면 이적 화면의 TRANSFER bundle ID를 스캔하세요.",
                    parent=self,
                    initialvalue=source_bundle_id,
                )
                or ""
            ).strip()
        target = self.current_set_info.get("exact_rescan_target_count")
        if not self.run_tests:
            target = simpledialog.askinteger(
                "전체 재스캔 수량",
                "sealed evidence가 없는 제품 전체 수량을 입력하세요.",
                parent=self,
                minvalue=1,
                maxvalue=100000,
                initialvalue=int(target or 1),
            )
        if not target:
            return False
        self.current_set_info["exact_rescan_active"] = True
        self.current_set_info["exact_rescan_complete"] = False
        self.current_set_info["exact_rescan_target_count"] = int(target)
        self.current_set_info["exact_rescan_source_bundle_id"] = source_bundle_id
        self.current_set_info["exact_rescan_barcodes"] = []
        self.data_manager.log_event(
            self.Events.EXACT_RESCAN_STARTED,
            {
                "set_id": self.current_set_info.get("id"),
                "target_count": int(target),
                "sample_barcodes_are_membership": False,
            },
        )
        self._save_current_set_state()
        self.update_big_display(f"전체 제품 재스캔 0/{int(target)}", "primary")
        self._update_status_label()
        self._render_operator_workbench()
        return True

    def _process_exact_rescan_product(self, raw_input):
        barcode = _normalize_barcode_for_exact_rescan(raw_input)
        target = int(self.current_set_info.get("exact_rescan_target_count") or 0)
        members = list(self.current_set_info.get("exact_rescan_barcodes") or [])
        item_code = str((self.current_set_info.get("parsed") or [""])[0] or "")
        if target < 1:
            self.current_set_info["exact_rescan_active"] = False
            raise PackageLogisticsError("exact rescan target count is invalid")
        if not barcode or item_code not in barcode:
            self._handle_input_error(
                raw_input,
                title="[전체 재스캔 품목 불일치]",
                reason="전체 재스캔 제품이 현재 현품표 품목과 다릅니다.",
            )
            return False
        if barcode in set(members):
            self._handle_input_error(
                raw_input,
                title="[전체 재스캔 중복]",
                reason="이미 전체 재스캔한 제품입니다.",
            )
            return False
        if len(members) >= target:
            self.current_set_info["exact_rescan_active"] = False
            return False
        members.append(barcode)
        self.current_set_info["exact_rescan_barcodes"] = members
        self.data_manager.log_event(
            self.Events.EXACT_RESCAN_OK,
            {
                "set_id": self.current_set_info.get("id"),
                "product_barcode": barcode,
                "rescan_position": len(members),
                "target_count": target,
                "barcode_role": "exact_membership_rescan",
            },
        )
        if len(members) == target:
            self.current_set_info["exact_rescan_active"] = False
            self.current_set_info["exact_rescan_complete"] = True
            self.data_manager.log_event(
                self.Events.EXACT_RESCAN_COMPLETED,
                {
                    "set_id": self.current_set_info.get("id"),
                    "member_count": target,
                    "membership_source": "FULL_EXACT_RESCAN",
                    "sample_barcodes_are_membership": False,
                },
            )
            self.update_big_display("전체 재스캔 완료 - 제품 샘플1 스캔", "green")
        else:
            self.update_big_display(f"전체 제품 재스캔 {len(members)}/{target}", "primary")
        self._save_current_set_state()
        self._update_status_label()
        self._render_operator_workbench()
        return True

    def _extract_production_date(self, raw_input):
        try:
            normalized_input = re.sub(r"<gs>", "\x1D", str(raw_input or ""), flags=re.IGNORECASE)
            fields = normalized_input.split('\x1D')
            for field in fields:
                if field.startswith('6D'):
                    date_str = field[2:]
                    if len(date_str) == 8 and date_str.isdigit():
                        production_date = datetime.strptime(date_str, "%Y%m%d")
                        return production_date.strftime("%Y-%m-%d")
            return None
        except Exception as e:
            print(f"생산 날짜 추출 오류: {e}")
            return None

    def _update_on_success_scan(self, raw, parsed):
        if len(self.current_set_info['raw']) == 0:
            self._clear_workflow_completion()
            self._workflow_recovered = False
            self.current_set_info['id'] = str(time.time_ns())
            self.current_set_info['start_time'] = datetime.now()

        self.current_set_info['raw'].append(raw)
        self.current_set_info['parsed'].append(parsed)

        num_scans = len(self.current_set_info['parsed'])
        next_text = self._next_action_text(num_scans)
        self.update_big_display(next_text, "green" if num_scans < self.TOTAL_SCAN_COUNT else "primary")
        if not self.is_running_simulation:
            sound_key = self._sound_key_for_success_scan(num_scans)
            if sound_key:
                self._play_sound(sound_key)
        self.progress_bar['value'] = num_scans
        self._update_status_label()
        self._update_history_tree_in_progress()
        self.data_manager.log_event(
            self.Events.SCAN_OK,
            {
                "raw": raw,
                "parsed": parsed,
                "set_id": self.current_set_info['id'],
                "scan_position": num_scans,
                "scan_pos": num_scans,
            },
        )
        self._save_current_set_state()
        self._render_operator_workbench()
        if num_scans == self.TOTAL_SCAN_COUNT:
            self._finalize_set(self.Results.PASS)

    @classmethod
    def _sound_key_for_success_scan(cls, scan_position):
        try:
            position = int(scan_position)
        except Exception:
            return None
        if position == LABEL_MATCH_MASTER_SCAN_POSITION:
            return "scan_master"
        if LABEL_MATCH_MASTER_SCAN_POSITION < position < cls.FINAL_LABEL_SCAN_POSITION:
            return f"scan_{position - LABEL_MATCH_MASTER_SCAN_POSITION}"
        return None

    def _queue_authoritative_package(self, *, item_code, is_manual_complete):
        if (
            is_manual_complete
            or self.__dict__.get("run_tests", False)
            or self.__dict__.get("is_running_simulation", False)
        ):
            return None
        current = self.current_set_info or {}
        raw = list(current.get("raw") or [])
        if len(raw) != self.TOTAL_SCAN_COUNT:
            return None
        try:
            sealed_transfer = _label_match_parse_sealed_transfer_qr(raw[0])
        except ValueError as exc:
            raise PackageLogisticsError(str(exc)) from exc
        exact_mode = bool(self.current_set_info.get("exact_rescan_complete"))
        central_enabled = self.__dict__.get("package_logistics_client") is not None
        if not sealed_transfer and not exact_mode:
            if central_enabled:
                raise PackageLogisticsError(
                    "central packaging requires a sealed transfer QR; three product samples are not membership"
                )
            return {"status": "LEGACY_DIRECT_SYNC_ONLY", "sample_barcodes_are_membership": False}
        outbox = self.__dict__.get("package_outbox")
        if outbox is None:
            raise PackageLogisticsError("durable package outbox is unavailable")
        draft = _label_match_package_draft(current, item_code=item_code)
        row = outbox.enqueue(draft)
        return {
            "status": str(row.get("status") or "PENDING"),
            "idempotency_key": row["idempotency_key"],
            "source_bundle_id": draft.source_bundle_id,
            "source_external_label": draft.source_external_label,
            "package_bundle_id": draft.package_bundle_id,
            "membership_mode": draft.membership_mode,
            "sample_barcodes": list(draft.sample_barcodes),
            "sample_barcodes_are_membership": False,
            "exact_rescan_count": len(draft.exact_rescan_barcodes),
            "expected_member_count": draft.expected_member_count,
            "expected_membership_hash": draft.expected_membership_hash,
        }

    def _finalize_set(self, result, error_details="", is_manual_complete=False):
        raw_scans_to_log = self.current_set_info['raw'].copy()
        parsed_scans_to_log = self.current_set_info['parsed'].copy()
        item_code = parsed_scans_to_log[0] if parsed_scans_to_log else "N/A"

        item_name_override = self.current_set_info.get('item_name_override')
        if item_name_override:
            item_info = {"Item Name": item_name_override, "Spec": ""}
        else:
            item_info = self.items_data.get(item_code, {})

        start_time = self.current_set_info.get('start_time')
        work_time_sec = (datetime.now() - start_time).total_seconds() if start_time else 0.0
        production_date = self.current_set_info.get('production_date')
        phase = self.current_set_info.get('phase') or '-'
        set_id_for_log = str(self.current_set_info['id'])
        master_label_raw = raw_scans_to_log[0] if raw_scans_to_log else ""
        master_label_fields = _label_match_parse_new_format_fields(master_label_raw) or {}
        master_label_identity_key = _label_match_new_format_identity_key(master_label_raw)
        inspection_trace = _label_match_inspection_trace_from_master_label(master_label_raw)

        details = {
            'master_label_code': item_code, 'item_code': item_code,
            'item_name': item_info.get("Item Name", "알 수 없음"),
            'spec': item_info.get("Spec", ""),
            'scan_count': len(raw_scans_to_log),
            'scanned_product_barcodes': raw_scans_to_log,
            'parsed_product_barcodes': parsed_scans_to_log,
            'work_time_sec': work_time_sec,
            'error_count': self.current_set_info.get('error_count', 0),
            'has_error_or_reset': self.current_set_info.get('has_error_or_reset', False) or (result != self.Results.PASS),
            'final_result': result,
            'result_display': result,
            'item_name_override': item_name_override,
            'is_unique_master_label': bool(item_name_override),
            'is_partial_submission': is_manual_complete, 'start_time': start_time,
            'end_time': datetime.now(),
            'production_date': production_date,
            'set_id': set_id_for_log,
            'phase': phase
        }
        if master_label_fields:
            details['master_label_fields'] = master_label_fields
        if master_label_identity_key:
            details['master_label_identity_key'] = master_label_identity_key
        if any(inspection_trace.get(key) for key in ("input_tag_id", "input_tag_label_id", "input_tag_core_hash", "input_tag_label_hash")):
            for key in ("input_tag_id", "input_tag_label_id", "input_tag_core_hash", "input_tag_label_hash"):
                if key in inspection_trace:
                    details[key] = inspection_trace[key]
            if inspection_trace.get('input_tag_id'):
                details.setdefault('source_session_id', inspection_trace['input_tag_id'])
            details['inspection_trace'] = inspection_trace
        try:
            package_logistics = (
                self._queue_authoritative_package(
                    item_code=item_code,
                    is_manual_complete=is_manual_complete,
                )
                if result == self.Results.PASS
                else None
            )
        except PackageLogisticsError as exc:
            if self.__dict__.get("operator_workbench_ready"):
                self._play_sound("fail")
                return self._publish_submission_block(exc)
            status_label = self.__dict__.get("status_label")
            if status_label is not None:
                status_label.config(text=f"❌ 중앙 포장 차단: {exc}", style="Error.TLabel")
            self._play_sound("fail")
            if not self.__dict__.get("run_tests", False):
                messagebox.showerror("중앙 포장 차단", str(exc), parent=self)
            return False
        if package_logistics:
            details["package_logistics"] = package_logistics
            details["package_membership_mode"] = package_logistics.get("membership_mode")
            details["sample_barcodes_are_membership"] = False
        if result == self.Results.PASS and not self.is_running_simulation:
            self._play_sound("pass")
        self.data_manager.log_event(self.Events.TRAY_COMPLETE, details)
        self._flush_data_manager_if_supported()
        if package_logistics and package_logistics.get("idempotency_key"):
            self._start_package_outbox_drain()
        state = self.__dict__
        if not state.get("run_tests", False) and not state.get("is_running_simulation", False):
            context = getattr(self, "direct_sync_bootstrap_context", None) or _label_match_direct_sync_context(
                self.save_directory,
                getattr(self, "app_settings_path", ""),
            )
            context = _label_match_bind_current_log_source(context, self.data_manager)
            self.direct_sync_bootstrap_context = context
            active_sync_threads = [
                thread
                for thread in self.__dict__.get("direct_sync_session_threads", [])
                if thread.is_alive()
            ]
            self.direct_sync_session_thread = _label_match_start_session_direct_sync(
                context,
                reason=self.Events.TRAY_COMPLETE,
            )
            active_sync_threads.append(self.direct_sync_session_thread)
            self.direct_sync_session_threads = active_sync_threads

        self.__dict__.setdefault("history_row_details_map", {})[set_id_for_log] = details
        if result == self.Results.PASS:
            if item_code != "N/A" and production_date:
                self.scan_count[production_date][(item_code, phase)] += 1
            self.set_details_map[set_id_for_log] = details
            self.global_scanned_set.update(_label_match_duplicate_index_barcodes(details))

        if self.history_tree.exists(set_id_for_log):
            current_values = list(self.history_tree.item(set_id_for_log, 'values'))
            display_id = current_values[0]
            final_timestamp = datetime.now().strftime('%H:%M:%S')

            first_scan_display = parsed_scans_to_log[0] if parsed_scans_to_log else ""
            other_scans_display = parsed_scans_to_log[1:self.TOTAL_SCAN_COUNT]
            values_to_update = (
                display_id,
                first_scan_display,
                *other_scans_display + [""] * ((self.TOTAL_SCAN_COUNT - 1) - len(other_scans_display)),
                result,
                final_timestamp,
            )
            values_to_update = self._history_values_for_display(values_to_update)

            self.history_tree.item(set_id_for_log, values=values_to_update, tags=("success" if result == self.Results.PASS else "error",))
            self._render_history_detail(set_id_for_log)

        self.save_status_label.config(text=f"✓ 기록됨 ({datetime.now().strftime('%H:%M:%S')})")
        self.after(3000, lambda: self.save_status_label.config(text=""))
        self._update_summary_tree()
        if self.__dict__.get("operator_workbench_ready"):
            self._publish_finalize_completion(
                is_manual_complete=is_manual_complete,
                result=result,
            )
        self._reset_current_set(from_finalize=True)
        if "big_display_label" in self.__dict__:
            if result == self.Results.PASS:
                self._show_completion_progress(result)
                self.update_big_display("통과 완료 - 다음 현품표 스캔", "green")
            else:
                self.update_big_display("오류 처리 완료 - 새 현품표부터 시작", "red")
            if self.__dict__.get("operator_workbench_ready"):
                self._refresh_session_tree()
                self._render_operator_workbench()
            else:
                self.after(1800, self._show_idle_instruction_if_idle)

    def _handle_input_error(self, raw, title="[입력 오류]", reason="알 수 없는 입력 오류가 발생했습니다."):
        set_id = self._ensure_current_set_id()
        self.data_manager.log_event(
            self.Events.ERROR_INPUT,
            {
                "raw": raw,
                "reason": reason,
                "set_id": set_id,
                "scan_pos": len(self.current_set_info.get('raw', [])) + 1,
            },
        )
        self._mark_current_set_error()

        self.update_big_display("입력 오류 - 새 현품표부터 시작", "red")
        self.status_label.config(text=f"{title}: {reason.split(chr(10))[0]} | 확인 후 새 현품표부터 시작", style="Error.TLabel")

        if self.is_running_simulation:
            print(f"  - 시뮬레이션 오류 처리: {title}")
            if not self.current_set_info.get('id'):
                self.current_set_info['id'] = str(time.time_ns())
            self._finalize_set(self.Results.FAIL_INPUT_ERROR, raw)
        elif not self.run_tests and "DEMO" not in raw:
            if self.__dict__.get("operator_workbench_ready"):
                self._present_inline_workflow_error(
                    title,
                    reason,
                    self.Results.FAIL_INPUT_ERROR,
                    raw,
                )
            else:
                self._trigger_modal_error(title, reason, self.Results.FAIL_INPUT_ERROR, raw)

    def _handle_mismatch(self, raw, master):
        set_id = self._ensure_current_set_id()
        self.data_manager.log_event(
            self.Events.ERROR_MISMATCH,
            {
                "raw": raw,
                "master": master,
                "set_id": set_id,
                "scan_pos": len(self.current_set_info.get('raw', [])) + 1,
            },
        )
        self._mark_current_set_error()
        title = "[제품 불일치]"

        truncated_raw = self._middle_ellipsis(raw, 48)
        truncated_master = self._middle_ellipsis(master, 48)
        error_message = (
            f"현품표와 제품이 불일치합니다.\n\n"
            f"- 현품표: {truncated_master}\n"
            f"- 스캔 제품: {truncated_raw}\n\n"
            "→ 이 세트는 오류 처리됩니다. 제품을 제거하고 확인 후 새 현품표부터 다시 스캔하세요."
        )
        self.update_big_display("제품 불일치 - 새 현품표부터 시작", "red")
        self.status_label.config(text=f"불일치: 확인 후 새 현품표부터 시작 | 스캔 제품: {truncated_raw}", style="Error.TLabel")

        if self.is_running_simulation:
            print(f"  - 시뮬레이션 오류 처리: {title}")
            if not self.current_set_info.get('id'):
                self.current_set_info['id'] = str(time.time_ns())
            self._finalize_set(self.Results.FAIL_MISMATCH, raw)
        elif not self.run_tests and "DEMO" not in raw:
            if self.__dict__.get("operator_workbench_ready"):
                self._present_inline_workflow_error(
                    title,
                    error_message,
                    self.Results.FAIL_MISMATCH,
                    raw,
                )
            else:
                self._trigger_modal_error(title, error_message, self.Results.FAIL_MISMATCH, raw)

    def _ensure_current_set_id(self):
        if not self.current_set_info.get('id'):
            self.current_set_info['id'] = str(time.time_ns())
        return self.current_set_info['id']

    def _mark_current_set_error(self):
        self.current_set_info['error_count'] += 1
        self.current_set_info['has_error_or_reset'] = True
        self._update_manual_complete_button_state()
        if self.current_set_info.get('raw'):
            self._save_current_set_state()

    def _dict_value_by_string_key(self, mapping, key):
        if not isinstance(mapping, dict):
            return None
        if key in mapping:
            return mapping[key]
        key_text = str(key)
        if key_text in mapping:
            return mapping[key_text]
        for existing_key, value in mapping.items():
            if str(existing_key) == key_text:
                return value
        return None

    def _dict_pop_by_string_key(self, mapping, key):
        if not isinstance(mapping, dict):
            return None
        if key in mapping:
            return mapping.pop(key)
        key_text = str(key)
        if key_text in mapping:
            return mapping.pop(key_text)
        for existing_key in list(mapping.keys()):
            if str(existing_key) == key_text:
                return mapping.pop(existing_key)
        return None

    def _remove_history_details_for_iid(self, iid):
        self._dict_pop_by_string_key(self.__dict__.get("set_details_map", {}), iid)
        self._dict_pop_by_string_key(self.__dict__.get("history_row_details_map", {}), iid)

    def _show_delete_failure(self, error):
        message = f"기록 삭제 중 오류가 발생했습니다.\n로그 파일 저장 권한 또는 선택된 기록 상태를 확인하세요.\n\n[오류 원인]\n{error}"
        status_label = self.__dict__.get("status_label")
        if status_label is not None:
            status_label.config(text="❌ 기록 삭제 실패", style="Error.TLabel")
        if not self.__dict__.get("run_tests", False):
            messagebox.showerror("삭제 실패", message, parent=self)

    def _delete_selected_row(self):
        if self._block_active_history_load_action("기록 삭제"):
            return
        if not self.history_view_updates_active_state:
            if not self.run_tests:
                messagebox.showwarning("조회 모드", "과거 날짜 조회 중에는 기록 삭제를 할 수 없습니다.\n'오늘' 기록으로 돌아온 뒤 다시 시도하세요.")
            return

        selected_iids = self.history_tree.selection()
        if not selected_iids:
            if not self.run_tests:
                messagebox.showwarning("선택 필요", "삭제할 기록을 목록에서 선택하세요.")
            return

        should_delete = self.run_tests or messagebox.askyesno("삭제 확인", f"선택된 {len(selected_iids)}개의 기록을 정말 삭제(무효화)하시겠습니까?\n이 작업은 되돌릴 수 없습니다.", icon="warning")

        if not should_delete:
            return

        deleted_count = 0
        try:
            for iid in selected_iids:
                if iid == 'loading':
                    continue
                if not self.history_tree.exists(iid):
                    continue

                deleted_details = self._history_details_for_iid(iid)
                values = tuple(self.history_tree.item(iid, 'values') or ())
                log_details = {'set_id': str(iid), 'deleted_values': values, 'original_details': deleted_details}
                self.data_manager.log_event(self.Events.SET_DELETED, log_details)
                self._flush_data_manager_if_supported()

                if deleted_details:
                    result = _label_match_tray_complete_result(deleted_details)
                    if result != self.Results.PASS:
                        result = self._history_result_value(values) or result
                else:
                    result = self._history_result_value(values)
                if deleted_details and result == self.Results.PASS:
                    production_date = deleted_details.get('production_date')
                    passed_code = deleted_details.get('item_code')
                    phase = deleted_details.get('phase') or '-'
                    if production_date and passed_code:
                        key = (passed_code, phase)
                        if production_date in self.scan_count and key in self.scan_count[production_date]:
                            self.scan_count[production_date][key] -= 1
                            if self.scan_count[production_date][key] == 0:
                                del self.scan_count[production_date][key]
                            if not self.scan_count[production_date]:
                                del self.scan_count[production_date]

                self.history_tree.delete(iid)
                self._remove_history_details_for_iid(iid)
                deleted_count += 1
        except Exception as e:
            self._show_delete_failure(e)
            if self.__dict__.get("run_tests", False):
                raise
            return

        if deleted_count == 0:
            if not self.run_tests:
                messagebox.showwarning("선택 필요", "삭제할 수 있는 기록이 없습니다.")
            return

        self._rebuild_global_scanned_set_from_details()
        self._update_summary_tree()
        self._render_history_detail()
        if not self.run_tests:
            messagebox.showinfo("삭제 완료", f"{deleted_count}개의 기록이 삭제 처리되었습니다.")

    def _rebuild_global_scanned_set_from_details(self):
        rebuilt = set()
        for details in self.set_details_map.values():
            rebuilt.update(_label_match_duplicate_index_barcodes(details))
        self.global_scanned_set = rebuilt

    def _block_view_only_action(self, action_name, parent=None):
        state = self.__dict__
        if state.get('history_view_updates_active_state', True):
            return False
        message = f"과거 기록 조회 중에는 {action_name}할 수 없습니다.\n'오늘' 기록으로 돌아온 뒤 다시 시도하세요."
        status_label = state.get('status_label')
        if status_label is not None:
            status_label.config(text=f"❌ {message.splitlines()[0]}", style="Error.TLabel")
        if not state.get('run_tests', False):
            messagebox.showwarning("조회 모드", message, parent=parent or self)
        return True

    def _block_active_history_load_action(self, action_name, parent=None):
        state = self.__dict__
        if not state.get('history_active_load_pending', False):
            return False
        message = f"오늘 기록을 불러오는 중에는 {action_name}할 수 없습니다.\n기록 로딩이 끝난 뒤 다시 시도하세요."
        status_label = state.get('status_label')
        if status_label is not None:
            status_label.config(text=f"❌ {message.splitlines()[0]}", style="Error.TLabel")
        if not state.get('run_tests', False):
            messagebox.showwarning("기록 로딩 중", message, parent=parent or self)
        return True

    def _block_duplicate_history_load(self, parent=None):
        history_load_pending = self.__dict__.get('history_load_pending', False)
        history_active_load_pending = self.__dict__.get('history_active_load_pending', False)
        if not (history_load_pending or history_active_load_pending):
            return False
        if history_active_load_pending:
            message = "오늘 기록을 불러오는 중입니다.\n기록 로딩이 끝난 뒤 다시 시도하세요."
        else:
            message = "기록을 불러오는 중입니다.\n기록 로딩이 끝난 뒤 다시 시도하세요."
        status_label = self.__dict__.get('status_label')
        if status_label is not None:
            status_label.config(text=f"❌ {message.splitlines()[0]}", style="Error.TLabel")
        if not self.__dict__.get('run_tests', False):
            messagebox.showwarning("기록 로딩 중", message, parent=parent or self)
        return True

    def _block_background_history_reload(self, parent=None):
        if not self._has_background_work():
            return False
        message = "테스트 시뮬레이션 또는 테스트 로그 생성 중에는 기록을 다시 불러올 수 없습니다.\n작업이 끝난 뒤 다시 시도하세요."
        status_label = self.__dict__.get('status_label')
        if status_label is not None:
            status_label.config(text=f"❌ {message.splitlines()[0]}", style="Error.TLabel")
        if not self.__dict__.get('run_tests', False):
            messagebox.showwarning("작업 진행 중", message, parent=parent or self)
        return True

    def _reset_current_set(self, full_reset=False, from_finalize=False):
        if self.is_blinking: return False
        if full_reset and not from_finalize and self._block_active_history_load_action("현재 세트를 취소"):
            return False
        if full_reset and not from_finalize and self._block_view_only_action("현재 세트를 취소"):
            return False
        if full_reset and self.current_set_info.get('id'):
            self.data_manager.log_event(self.Events.SET_CANCELLED, {"set_id": self.current_set_info['id'], "cancelled_set": self.current_set_info})
            if self.history_tree.exists(str(self.current_set_info['id'])):
                self.history_tree.delete(str(self.current_set_info['id']))
            self.__dict__.setdefault("history_row_details_map", {}).pop(str(self.current_set_info['id']), None)
            self.current_set_info['has_error_or_reset'] = True
        if from_finalize or full_reset:
            self._delete_current_set_state()
        if full_reset and not from_finalize:
            self._clear_workflow_completion()
            self._workflow_blocking_notice = None
            self._workflow_notice = None
            self._workflow_notice_action = None
            self._workflow_recovered = False

        self.current_set_info = {
            'id': None, 'parsed': [], 'raw': [],
            'start_time': None, 'error_count': 0, 'has_error_or_reset': False,
            'phase': None, 'item_name_override': None, 'production_date': None,
            'sealed_transfer': None,
            'exact_rescan_active': False,
            'exact_rescan_complete': False,
            'exact_rescan_target_count': 0,
            'exact_rescan_source_bundle_id': "",
            'exact_rescan_barcodes': [],
        }
        self.progress_bar['value'] = 0
        if self.initialized_successfully:
            self._update_status_label()
            self.update_big_display(self._idle_instruction_text(), "")
            self.entry.focus_set()
            self._render_operator_workbench()
        return True

    def _show_completion_progress(self, result):
        if result != self.Results.PASS:
            return
        if not getattr(self, "history_view_updates_active_state", True):
            return
        if "progress_bar" in self.__dict__:
            self.progress_bar['value'] = self.TOTAL_SCAN_COUNT
        self._update_step_rail(self.TOTAL_SCAN_COUNT)
        if "status_label" in self.__dict__:
            self.status_label.config(
                text=f"{self.TOTAL_SCAN_COUNT}/{self.TOTAL_SCAN_COUNT} 통과 완료 | 다음 현품표 스캔 대기",
                style="Status.TLabel",
            )

    def _close_popup(self, popup, result, error_details):
        if popup.winfo_exists():
            popup.grab_release()
            popup.destroy()
        self.is_blinking = False
        self.entry.focus_set()
        self.after(50, self.entry.focus_force)
        if not self.current_set_info.get('id'):
            self.current_set_info['id'] = str(time.time_ns())
        self.after(10, lambda: self._finalize_set(result, error_details))

    def _play_error_siren_loop(self):
        if self.__dict__.get("run_tests", False) or _label_match_automated_test_mode():
            return
        sound = self.sound_objects.get("fail")
        if not sound:
            self.after_idle(lambda: messagebox.showwarning("사운드 설정 오류", "경고음 파일을 찾을 수 없습니다.\n(assets 폴더의 fail.wav 파일 확인 필요)\n\n오류 발생 시 경고음이 울리지 않습니다."))
            return
        try:
            sound.play(loops=-1)
            while self.is_blinking:
                time.sleep(0.1)
            sound.stop()
        except Exception as e:
            self.after_idle(lambda: messagebox.showerror("사운드 재생 오류", f"경고음을 재생하는 중 오류가 발생했습니다.\n스피커 또는 사운드 드라이버를 확인해주세요.\n\n[상세 오류]\n{e}"))

    def _trigger_modal_error(self, title, message, result, error_details):
        if self.__dict__.get("operator_workbench_ready"):
            return self._present_inline_workflow_error(
                title,
                message,
                result,
                error_details,
            )
        if self.is_blinking: return
        self.is_blinking = True
        if not self.run_tests and not _label_match_automated_test_mode():
            threading.Thread(target=self._play_error_siren_loop, daemon=True).start()
        self.after(0, self._blink_background_loop)
        try:
            self.update_idletasks()
            popup_width = max(800, int(self.winfo_width()))
            popup_height = max(600, int(self.winfo_height()))
            popup_x = int(self.winfo_rootx())
            popup_y = int(self.winfo_rooty())
            message_font_size = max(24, min(40, popup_width // 34, popup_height // 20))
            button_font_size = max(18, min(28, popup_width // 52))
            message_wraplength = max(560, min(popup_width - 180, int(popup_width * 0.78)))
            popup = tk.Toplevel(self)
            popup.title(f"⚠️ {title}")
            popup.geometry(f"{popup_width}x{popup_height}+{popup_x}+{popup_y}")
            popup.resizable(False, False)
            popup.configure(bg=self.colors.get("danger", "#E74C3C"))
            popup.attributes('-topmost', True)

            popup_frame = tk.Frame(popup, bg=self.colors.get("danger", "#E74C3C"))
            popup_frame.pack(expand=True, fill='both')

            btn_frame = tk.Frame(popup_frame, bg=self.colors.get("danger", "#E74C3C"))
            btn_frame.pack(side=tk.BOTTOM, fill=tk.X, pady=(20, 60))

            btn = tk.Button(btn_frame, text="확인 (Enter / ESC)",
                            command=lambda: self._close_popup(popup, result, error_details),
                            font=("Malgun Gothic", button_font_size, "bold"), bg="yellow", fg="black",
                            relief="raised", borderwidth=5)
            btn.pack(ipady=20, ipadx=50)

            label = tk.Label(popup_frame, text=f"⚠️\n\n{message}",
                                     font=("Malgun Gothic", message_font_size, "bold"), fg='white',
                                     bg=self.colors.get("danger", "#E74C3C"),
                                     anchor='center', justify='center',
                                     wraplength=message_wraplength)
            label.pack(padx=80, pady=(40, 20), expand=True, fill='both')

            popup.focus_force()
            btn.focus_set()

            popup.bind("<Escape>", lambda e: self._close_popup(popup, result, error_details))
            btn.bind("<Return>", lambda e: self._close_popup(popup, result, error_details))
            popup.protocol("WM_DELETE_WINDOW", lambda: self._close_popup(popup, result, error_details))
            self.update_idletasks()
            popup.transient(self)
            popup.grab_set()

        except Exception as e:
            self.data_manager.log_event(self.Events.UI_ERROR, {"context": "modal_popup_creation", "error": str(e), "original_message": message})
            self.is_blinking = False
            fail_sound = self.sound_objects.get("fail")
            if fail_sound: fail_sound.stop()
            if not self.run_tests:
                messagebox.showerror("시스템 오류", f"오류 경고창을 표시하는 데 실패했습니다.\n프로그램을 재시작해야 할 수 있습니다.\n\n[기존 오류 메시지]\n{message}")
            self._reset_current_set(full_reset=True)

    def _prompt_and_cancel_completed_tray(self):
        if not self.initialized_successfully: return
        if self._block_active_history_load_action("완료된 트레이 취소", parent=self):
            return
        if not self.history_view_updates_active_state:
            if not self.run_tests:
                messagebox.showwarning("조회 모드", "과거 날짜 조회 중에는 완료된 트레이 취소를 할 수 없습니다.\n'오늘' 기록으로 돌아온 뒤 다시 시도하세요.", parent=self)
            return
        
        master_label = None
        if not self.run_tests:
            master_label = simpledialog.askstring("완료된 트레이 취소",
                                                  "취소할 트레이의 현품표를 스캔하거나 입력하세요:",
                                                  parent=self)
        if not master_label: return
        master_label = master_label.strip()

        if not master_label:
            if not self.run_tests:
                messagebox.showwarning("입력 오류", "현품표가 입력되지 않았습니다.", parent=self)
            return

        self._cancel_completed_tray_by_label(master_label)
    
    def _prompt_manual_complete(self):
        """사용자에게 현재 세트를 수동으로 완료할지 확인하고 처리합니다."""
        if not self.initialized_successfully:
            return
        if self._block_active_history_load_action("현재 세트를 완료", parent=self):
            return
        if not self.history_view_updates_active_state:
            if not self.run_tests:
                messagebox.showwarning("조회 모드", "과거 기록 조회 중에는 현재 세트를 완료할 수 없습니다.\n'오늘' 기록으로 돌아온 뒤 다시 시도하세요.", parent=self)
            return

        block_reason = _label_match_manual_complete_block_reason(self.current_set_info)
        if block_reason:
            self._update_manual_complete_button_state()
            if not self.run_tests:
                messagebox.showwarning("수동 완료 불가", "현재 세트는 수동 완료할 수 없습니다.", parent=self)
            return

        num_scans = len(self.current_set_info['raw'])
        msg = (f"현재 {num_scans}개만 스캔되었습니다.\n"
               f"이 세트를 '통과'로 즉시 완료하시겠습니까?\n\n"
               f"(샘플 출고 등 소량 작업 시 사용)")

        should_complete = self.run_tests or messagebox.askyesno("수동 완료 확인", msg, icon='question')

        if should_complete:
            self._finalize_set(self.Results.PASS, is_manual_complete=True)

    # [수정됨] 버그가 수정되고 로직이 개선된 최종 버전
    def _cancel_completed_tray_by_label(self, label_to_cancel):
        target_set_id = None
        target_details = None

        # 로직 개선: 고유 현품표(Raw Barcode)를 우선적으로 정확히 찾아냄
        # 이는 Base64 또는 'CLC=...' 와 같은 고유 식별자를 가진 라벨을 위한 것임
        is_unique_label_match = False
        for set_id, details in self.set_details_map.items():
            if not _label_match_tray_complete_passed(details):
                continue
            raw_scans = details.get('scanned_product_barcodes', [])
            if (
                raw_scans
                and _label_match_first_scan_is_unique_master(details)
                and _label_match_unique_master_labels_equivalent(raw_scans[0], label_to_cancel)
            ):
                target_set_id = set_id
                is_unique_label_match = True
                break
        
        # 고유 현품표가 아닐 경우, 일반 코드(13자리 등)로 간주하고 가장 최근 기록을 찾음
        if not is_unique_label_match:
            found_sets = []
            for set_id, details in self.set_details_map.items():
                if not _label_match_tray_complete_passed(details):
                    continue
                # 파싱된 코드(master_label_code)와 일치하는 모든 기록을 찾음
                if details.get('master_label_code') == label_to_cancel:
                    try:
                        end_time_dt = _label_match_parse_datetime(details.get('end_time'))
                        found_sets.append({'set_id': set_id, 'details': details, 'end_time': end_time_dt})
                    except (ValueError, TypeError):
                        continue
            
            if found_sets:
                # 가장 최근에 완료된 기록을 찾기 위해 정렬
                found_sets.sort(key=lambda x: x['end_time'], reverse=True)
                latest_set = found_sets[0]
                target_set_id = latest_set['set_id']

        # 취소할 대상을 찾지 못한 경우
        if not target_set_id:
            if not self.run_tests:
                messagebox.showerror("찾기 실패", f"입력하신 현품표 '{label_to_cancel}'에 해당하는 '통과' 기록을 현재 조회된 내역에서 찾을 수 없습니다.", parent=self)
            return

        # --- 확인 및 취소 절차 (기존과 동일) ---
        target_details = self.set_details_map[target_set_id]
        
        try:
            end_time_dt = _label_match_parse_datetime(target_details.get('end_time'))
            end_time_display = end_time_dt.strftime('%H:%M:%S')
        except (ValueError, TypeError):
            end_time_display = "알 수 없음"

        item_name = target_details.get('item_name', '알 수 없음')

        confirm_msg = (f"다음 기록을 취소하시겠습니까?\n\n"
                       f"현품표: {target_details.get('master_label_code')}\n"
                       f"품명: {item_name}\n"
                       f"완료 시간: {end_time_display}\n\n"
                       f"취소 시 통계와 기록이 모두 변경됩니다.")
        
        should_cancel = self.run_tests or messagebox.askyesno("취소 확인", confirm_msg, icon='warning', parent=self)

        if not should_cancel:
            return

        try:
            self.data_manager.log_event(self.Events.TRAY_COMPLETION_CANCELLED, {
                'cancelled_set_id': target_set_id,
                'cancelled_by_label': label_to_cancel,
                'details': target_details
            })
            self._flush_data_manager_if_supported()

            production_date = target_details.get('production_date')
            item_code = target_details.get('item_code')
            phase = target_details.get('phase') or '-'
            if production_date and item_code:
                key = (item_code, phase)
                if production_date in self.scan_count and key in self.scan_count[production_date]:
                    self.scan_count[production_date][key] -= 1
                    if self.scan_count[production_date][key] <= 0:
                        del self.scan_count[production_date][key]
                    if not self.scan_count[production_date]:
                        del self.scan_count[production_date]

            if target_set_id in self.set_details_map: del self.set_details_map[target_set_id]
            if self.history_tree.exists(target_set_id): self.history_tree.delete(target_set_id)

            self._rebuild_global_scanned_set_from_details()
            self._update_summary_tree()
            
            if not self.run_tests:
                messagebox.showinfo("처리 완료", f"해당 작업이 정상적으로 취소되었습니다.", parent=self)

        except Exception as e:
            if not self.run_tests:
                messagebox.showerror("처리 오류", f"취소 작업을 처리하는 중 오류가 발생했습니다.\n프로그램을 다시 시작하여 확인해주세요.\n\n[상세 오류]\n{e}", parent=self)
            self.data_manager.log_event(self.Events.UI_ERROR, {"context": "tray_cancellation_by_label", "error": str(e)})

    def run_test_log_simulation(self, master_code_to_test, num_sets):
        if self._has_background_work():
            if not self.run_tests:
                messagebox.showwarning("작업 진행 중", "이미 테스트 작업이 진행 중입니다.", parent=self)
            return
        self.is_generating_test_logs = True
        self.entry.config(state='disabled')
        self.update_big_display(f"테스트 데이터 생성 시작...", "primary")
        self.progress_bar['value'] = 0

        sim_thread = threading.Thread(target=self._execute_test_simulation, args=(master_code_to_test, num_sets,), daemon=True)
        sim_thread.start()

    def _execute_test_simulation(self, master_code, num_sets):
        try:
            item_info = self.items_data.get(master_code, {"Item Name": "테스트 품목", "Spec": "T-SPEC"})

            for i in range(num_sets):
                progress_text = f"테스트 진행 중... ({i + 1}/{num_sets})"
                self.after(0, self.update_big_display, progress_text, "primary")

                set_id = f"TEST_{time.time_ns()}"
                start_time = datetime.now()
                time.sleep(0.01)
                end_time = datetime.now()
                production_date = datetime.now().strftime('%Y-%m-%d')
                phase = str((i % 3) + 1)

                scanned_barcodes = [
                    f"CLC={master_code}|SPC={item_info['Item Name']}|PHS={phase}",
                    *(f"PRODUCT_TEST_{master_code}_{set_id}_{index}" for index in range(1, self.PRODUCT_SAMPLE_COUNT + 1)),
                    f"FINAL_LABEL_{master_code}_{set_id}\x1D6D{production_date.replace('-', '')}"
                ]
                parsed_scans = [master_code] * self.TOTAL_SCAN_COUNT

                details = {
                    'set_id': set_id,
                    'master_label_code': master_code, 'item_code': master_code,
                    'item_name': item_info.get("Item Name"), 'spec': item_info.get("Spec"),
                    'scan_count': self.TOTAL_SCAN_COUNT,
                    'scanned_product_barcodes': scanned_barcodes,
                    'parsed_product_barcodes': parsed_scans,
                    'work_time_sec': (end_time - start_time).total_seconds(),
                    'error_count': 0, 'has_error_or_reset': False,
                    'final_result': self.Results.PASS, 'result_display': self.Results.PASS,
                    'is_partial_submission': False, 'start_time': start_time,
                    'end_time': end_time,
                    'production_date': production_date, 'phase': phase
                }

                self.data_manager.log_event(self.Events.TRAY_COMPLETE, details)
                self.scan_count[production_date][(master_code, phase)] += 1
                self.set_details_map[set_id] = details
                self.global_scanned_set.update(_label_match_duplicate_index_barcodes(details))
                self.after(0, self._add_test_set_to_history_ui, set_id, details, i + 1)

            self.after(0, self._finalize_test_simulation, num_sets)
        except Exception as e:
            print(f"테스트 데이터 생성 오류: {e}")
            try:
                self.after(0, self._finalize_test_simulation_error, str(e))
            except Exception:
                self.is_generating_test_logs = False

    def _add_test_set_to_history_ui(self, set_id, details, display_index):
        if not self.history_tree.winfo_exists(): return

        parsed_scans = details['parsed_product_barcodes']
        first_scan = parsed_scans[0] if parsed_scans else ""
        other_scans = parsed_scans[1:self.TOTAL_SCAN_COUNT]

        values_to_display = (
            len(self.history_tree.get_children()) + 1,
            first_scan,
            *other_scans + [""] * ((self.TOTAL_SCAN_COUNT - 1) - len(other_scans)),
            self.Results.PASS,
            details['end_time'].strftime('%H:%M:%S')
        )
        self.__dict__.setdefault("history_row_details_map", {})[set_id] = details
        values_to_display = self._history_values_for_display(values_to_display)
        self.history_tree.insert("", "end", iid=set_id, values=values_to_display, tags=("success",))
        self.history_tree.yview_moveto(1.0)

    def _finalize_test_simulation(self, num_sets):
        self.is_generating_test_logs = False
        if not self.winfo_exists(): return

        self._play_sound("pass")
        self._update_summary_tree()
        self.update_big_display(f"테스트 완료: {num_sets}개 생성", "success")
        if not self.run_tests:
            messagebox.showinfo("테스트 완료", f"{num_sets}개의 테스트 '통과' 기록 생성이 완료되었습니다.")

        self.entry.config(state='normal')
        self.entry.focus_set()
        self._reset_current_set()

    def _finalize_test_simulation_error(self, error_message):
        self.is_generating_test_logs = False
        if not self.winfo_exists(): return

        self.entry.config(state='normal')
        self.entry.focus_set()
        self.update_big_display("테스트 데이터 생성 실패", "red")
        self.status_label.config(text=f"❌ 테스트 데이터 생성 실패: {error_message}", style="Error.TLabel")
        if not self.run_tests:
            messagebox.showerror("테스트 생성 오류", f"테스트 데이터 생성 중 오류가 발생했습니다.\n\n[상세 오류]\n{error_message}", parent=self)

    def open_settings_window(self):
        if self.current_set_info.get('id'):
            if not self.run_tests:
                messagebox.showwarning("작업 중 경고", "현재 스캔 작업이 진행 중입니다.\n설정 변경은 다음 작업부터 적용됩니다.")
        settings_window = tk.Toplevel(self)
        settings_window.title("설정")
        settings_window.resizable(False, False)
        settings_window.transient(self)
        settings_window.grab_set()
        settings_window.configure(bg=self.colors.get("background", "#ECEFF1"))
        self._center_child_window(settings_window, *self._dialog_size("settings"))
        main_frame = ttk.Frame(settings_window, padding=20, style="TFrame")
        main_frame.pack(fill=tk.BOTH, expand=True)
        main_frame.grid_columnconfigure(1, weight=1)
        ttk.Label(main_frame, text="작업자 이름", font=(self.default_font_name, 12, "bold")).grid(row=0, column=0, sticky='w', pady=(8,5), padx=(0, 10))
        ttk.Label(main_frame, text="저장 후 다음 작업부터 로그 작업자명이 변경됩니다.", style="Status.TLabel").grid(row=1, column=0, columnspan=3, sticky='w', pady=(0, 10))
        self.worker_name_var = tk.StringVar(value=self.worker_name)
        worker_entry = ttk.Combobox(
            main_frame,
            textvariable=self.worker_name_var,
            values=self._recent_worker_names(),
            state="normal",
            font=(self.default_font_name, 12),
        )
        worker_entry.grid(row=2, column=0, columnspan=3, sticky='ew')
        button_frame = ttk.Frame(main_frame, padding=(0, 20, 0, 0), style="TFrame")
        button_frame.grid(row=3, column=0, columnspan=3, sticky='e', pady=(20,0))
        save_button = ttk.Button(button_frame, text="저장", command=lambda: self._save_settings_and_close(settings_window, self.worker_name_var.get()))
        save_button.pack(side=tk.LEFT, padx=5)
        cancel_button = ttk.Button(button_frame, text="취소", command=lambda: self._destroy_modal_and_refocus(settings_window))
        cancel_button.pack(side=tk.LEFT)
        settings_window.bind("<Escape>", lambda event: self._destroy_modal_and_refocus(settings_window))
        settings_window.bind("<Return>", lambda event: self._save_settings_and_close(settings_window, self.worker_name_var.get()))
        worker_entry.focus_set()
        if hasattr(worker_entry, "select_range"):
            worker_entry.select_range(0, tk.END)
        elif hasattr(worker_entry, "selection_range"):
            worker_entry.selection_range(0, tk.END)

    def _save_settings_and_close(self, window: tk.Toplevel, new_worker_name: str):
        active_set = bool(getattr(self, 'current_set_info', {}).get('id'))
        if active_set or self._has_background_work():
            if not self.run_tests:
                messagebox.showwarning("작업 중 설정 변경 불가", "현재 스캔 작업 또는 테스트 시뮬레이션이 진행 중입니다.\n현재 작업을 완료하거나 취소한 뒤 설정을 저장하세요.", parent=window)
            return
        if self._block_view_only_action("설정을 저장", parent=window):
            return
        if self._block_duplicate_history_load(parent=window):
            return
        if not new_worker_name.strip():
            if not self.run_tests:
                messagebox.showerror("입력 오류", "작업자 이름은 비워둘 수 없습니다.", parent=window)
            return
        requested_worker_name = new_worker_name.strip()
        try:
            self.data_manager.close(timeout=None)
        except Exception as e:
            self._replace_closed_data_manager_after_close_failure(self.data_manager)
            if self.run_tests:
                raise
            messagebox.showerror("저장 보류", f"작업 로그 저장을 완료하지 못해 설정 변경을 중단했습니다.\n\n[상세 오류]\n{e}", parent=window)
            return

        self.worker_name = requested_worker_name
        self._save_app_settings()
        self._update_save_directory()
        self.data_manager = DataManager(self.save_directory, self.Worker.PACKAGING, self.worker_name, self.unique_id)
        self.title(f"바코드 세트 검증기 ({APP_VERSION}) - {self.worker_name} ({self.unique_id})")
        if not self.run_tests:
            messagebox.showinfo("저장 완료", f"설정이 변경되었습니다.\n- 작업자: {self.worker_name}", parent=self)
        self._destroy_modal_and_refocus(window)

    def _show_about_window(self):
        about_win = tk.Toplevel(self)
        about_win.title("정보")
        about_win.resizable(False, False)
        about_win.transient(self)
        about_win.grab_set()
        about_win.configure(bg=self.colors["background"])
        self._center_child_window(about_win, *self._dialog_size("about"))

        header_font = (self.default_font_name, 18, "bold")
        title_font = (self.default_font_name, 11, "bold")
        text_font = (self.default_font_name, 11)

        main_frame = ttk.Frame(about_win, padding=25)
        main_frame.pack(expand=True, fill=tk.BOTH)

        ttk.Label(main_frame, text="바코드 세트 검증기", font=header_font).pack(pady=(0, 5))
        ttk.Label(main_frame, text=f"Version {APP_VERSION}", font=(self.default_font_name, 10, "italic")).pack(pady=(0, 20))

        info_frame = ttk.Frame(main_frame)
        info_frame.pack(fill=tk.X, pady=5)
        ttk.Label(info_frame, text="제작:", font=title_font, width=12).grid(row=0, column=0, sticky='w')
        ttk.Label(info_frame, text="KMTechn", font=text_font).grid(row=0, column=1, sticky='w')
        ttk.Label(info_frame, text="Copyright:", font=title_font, width=12).grid(row=1, column=0, sticky='w')
        ttk.Label(info_frame, text="© 2024 KMTechn. All rights reserved.", font=text_font).grid(row=1, column=1, sticky='w')

        ttk.Separator(main_frame, orient='horizontal').pack(fill='x', pady=15)

        keys_frame = ttk.Frame(main_frame)
        keys_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(keys_frame, text="주요 단축키", font=title_font).grid(row=0, column=0, columnspan=2, sticky='w', pady=(0, 5))

        key_map = {
            self.CURRENT_SET_CANCEL_ACTION_TEXT: "F1",
            self.COMPLETED_TRAY_CANCEL_ACTION_TEXT: "F2",
            self.MANUAL_COMPLETE_ACTION_TEXT: "F3",
            f"{self.HISTORY_DELETE_ACTION_TEXT} (기록 목록 선택 시)": "Delete",
            "UI 확대/축소": "Ctrl + 마우스 휠"
        }
        
        for i, (desc, key) in enumerate(key_map.items()):
            ttk.Label(keys_frame, text=f"• {desc}", font=text_font).grid(row=i + 1, column=0, sticky='w', padx=10)
            ttk.Label(keys_frame, text=key, font=(self.default_font_name, 11, "bold")).grid(row=i + 1, column=1, sticky='e', padx=10)
        
        keys_frame.grid_columnconfigure(1, weight=1)

        close_button = ttk.Button(main_frame, text="닫기", command=lambda: self._destroy_modal_and_refocus(about_win), style="TButton")
        close_button.pack(side=tk.BOTTOM, pady=(20, 0))
        about_win.bind("<Escape>", lambda event: self._destroy_modal_and_refocus(about_win))
        close_button.focus_set()


    def _configure_base_styles(self):
        self.style.theme_use('clam')
        self.style.layout("Treeview", [('Treeview.treearea', {'sticky': 'nswe'})])
        self.style.configure("TFrame", background=self.colors["background"])
        self.style.configure("Card.TFrame", background=self.colors["card_background"], borderwidth=2, relief='solid', bordercolor=self.colors["border"])
        self.style.configure("Borderless.TFrame", background=self.colors["card_background"], borderwidth=0)
        self.style.configure("ErrorCard.TFrame", background=self.colors["danger"], borderwidth=2, relief='solid', bordercolor=self.colors["danger"])
        self.style.configure("TLabel", background=self.colors["card_background"], foreground=self.colors["text"], font=(self.default_font_name, 14))
        self.style.configure("Header.TLabel", background=self.colors["card_background"], foreground=self.colors["text"], font=(self.default_font_name, 18, "bold"))
        self.style.configure("TButton", padding=12, relief="flat", borderwidth=2, focuscolor=self.colors["text"], background=self.colors["primary"], foreground="white", font=(self.default_font_name, 14, "bold"))
        self.style.map("TButton", background=[('active', self.colors["primary_active"]), ('focus', self.colors["primary_active"]), ('disabled', self.colors["border"])], foreground=[('disabled', self.colors["text_subtle"])], relief=[('focus', 'solid')])
        self.style.configure("Control.TButton", padding=(8, 5), font=(self.default_font_name, 12, "bold"), background=self.colors["card_background"], foreground=self.colors["text"], relief="groove", borderwidth=2, bordercolor=self.colors["border"], focuscolor=self.colors["primary_active"])
        self.style.map("Control.TButton", background=[('active', self.colors["background"]), ('focus', self.colors["background"])], relief=[('focus', 'solid')])
        self.style.configure("Danger.Action.TButton", font=(self.default_font_name, 15, "bold"), padding=15)
        self.style.map("Danger.Action.TButton",
                       foreground=[('disabled', self.colors["text_subtle"]), ('active', 'white'), ('!disabled', 'white')],
                       background=[('disabled', '#E5E7EB'), ('active', '#991B1B'), ('!disabled', self.colors["danger"])])
        self.style.configure("Status.TLabel", background=self.colors["card_background"], foreground=self.colors["text_subtle"], font=(self.default_font_name, 14))
        self.style.configure("SummaryDate.TLabel", background=self.colors["background"], foreground=self.colors["text_subtle"], font=(self.default_font_name, 13, "bold"), padding=(8, 4))
        self.style.configure("Success.TLabel", background=self.colors["card_background"], foreground=self.colors["success"], font=(self.default_font_name, 14, "bold"))
        self.style.configure("Error.TLabel", background=self.colors["card_background"], foreground=self.colors["danger"], font=(self.default_font_name, 14, "bold"))
        self.style.configure("ViewMode.TLabel", background=self.colors["danger_light"], foreground=self.colors["danger"], font=(self.default_font_name, 13, "bold"), padding=(10, 6))
        self.style.configure("Save.Success.TLabel", background=self.colors["background"], foreground=self.colors["success"], font=(self.default_font_name, 12, "bold"))
        self.style.configure("green.Horizontal.TProgressbar", background=self.colors["success"], troughcolor=self.colors["border"], borderwidth=0)
        self.style.configure("TEntry", bordercolor=self.colors["border"], fieldbackground=self.colors["card_background"])
        self.style.configure("TScrollbar", gripcount=0, troughcolor=self.colors["background"], bordercolor=self.colors["background"], lightcolor=self.colors["background"], darkcolor=self.colors["background"], arrowcolor=self.colors["text_subtle"], background=self.colors["border"])
        self.style.map("TScrollbar", background=[('active', self.colors["text_subtle"])])
        overlay_bg = self.colors["background"]
        self.style.configure("Overlay.TFrame", background=overlay_bg)
        self.style.configure("Loading.TLabel", background=overlay_bg, foreground=self.colors["text"], font=(self.default_font_name, 24, "bold"))

        self.style.configure("Action.TButton", font=(self.default_font_name, 15, "bold"), padding=15, focuscolor=self.colors["text"])
        self.style.map("Action.TButton",
                       foreground=[('disabled', self.colors["text_subtle"]), ('active', 'white'), ('!disabled', 'white')],
                       background=[('disabled', '#E5E7EB'), ('active', self.colors["primary_active"]), ('focus', self.colors["primary_active"]), ('!disabled', self.colors["primary"])],
                       relief=[('focus', 'solid')])

    def _configure_treeview_styles(self):
        self.style.configure("Treeview", background=self.colors["card_background"], fieldbackground=self.colors["card_background"], foreground=self.colors["text"], borderwidth=0, relief='flat', rowheight=40)
        self.style.map("Treeview", background=[('selected', self.colors["primary"])], foreground=[('selected', 'white')])
        self.style.configure("Treeview.Heading", background=self.colors["heading_background"], foreground=self.colors["text_subtle"], relief="flat", borderwidth=0, font=(self.default_font_name, 14, "bold"))
        self.style.map("Treeview.Heading", background=[('active', self.colors["background"])])
        self.history_tree.tag_configure("success", background=self.colors["success_light"], foreground=self.colors["text_strong"])
        self.history_tree.tag_configure("error", background=self.colors["danger_light"], foreground=self.colors["text_strong"])
        self.history_tree.tag_configure("in_progress", foreground=self.colors["text_subtle"], background=self.colors["card_background"])

    def _show_history_context_menu(self, event):
        iid = self.history_tree.identify_row(event.y)
        if iid:
            if iid not in self.history_tree.selection():
                self.history_tree.selection_set(iid)
            self._render_history_detail(iid)
            self.history_context_menu.post(event.x_root, event.y_root)

    def _history_tree_has_keyboard_focus(self, event=None):
        history_tree = self.__dict__.get("history_tree")
        if history_tree is None:
            return False
        event_widget = getattr(event, "widget", None)
        if event_widget is history_tree:
            return True
        try:
            focus_widget = self.focus_get()
        except Exception:
            focus_widget = None
        return focus_widget is history_tree

    def _delete_selected_row_from_shortcut(self, event=None):
        if not self._history_tree_has_keyboard_focus(event):
            return None
        self._delete_selected_row()
        return "break"

    def _selected_history_iid(self):
        if "history_tree" not in self.__dict__:
            return None
        selection = self.history_tree.selection()
        return selection[0] if selection else None

    def _render_history_detail(self, iid=None):
        if "history_detail_text" not in self.__dict__:
            return
        iid = iid or self._selected_history_iid()
        details = self._history_details_for_iid(iid)
        text = self._barcode_inline_detail_text(details)
        self.history_detail_text.configure(state="normal")
        self.history_detail_text.delete("1.0", tk.END)
        self.history_detail_text.insert("1.0", text)
        self.history_detail_text.configure(state="disabled")
        button_state = "normal" if details else "disabled"
        self.history_detail_copy_button.configure(state=button_state)
        self.history_detail_modal_button.configure(state=button_state)

    def _on_history_selection_changed(self, event=None):
        self._render_history_detail()

    def _copy_selected_history_barcodes(self):
        iid = self._selected_history_iid()
        details = self._history_details_for_iid(iid)
        if not details:
            return
        text = self._barcode_detail_text(details)
        self.clipboard_clear()
        self.clipboard_append(text)
        self.save_status_label.config(text=f"✓ 바코드 원문 복사됨 ({datetime.now().strftime('%H:%M:%S')})")
        self.after(2500, lambda: self.save_status_label.config(text=""))

    def _show_selected_history_detail_window(self, event=None):
        iid = self._selected_history_iid()
        details = self._history_details_for_iid(iid)
        if not details:
            return
        self._show_barcode_detail_window(details)

    def _show_barcode_detail_window(self, details):
        detail_win = tk.Toplevel(self)
        detail_win.title("바코드 원문")
        detail_win.transient(self)
        detail_win.grab_set()
        detail_win.configure(bg=self.colors["background"])
        width, height = self._dialog_size("barcode_detail")
        self._center_child_window(detail_win, width, height)

        main_frame = ttk.Frame(detail_win, padding=18, style="TFrame")
        main_frame.pack(fill=tk.BOTH, expand=True)
        main_frame.grid_rowconfigure(1, weight=1)
        main_frame.grid_columnconfigure(0, weight=1)

        ttk.Label(main_frame, text="바코드 원문", style="Header.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 10))

        text_frame = ttk.Frame(main_frame, style="Card.TFrame")
        text_frame.grid(row=1, column=0, sticky="nsew")
        text_frame.grid_rowconfigure(0, weight=1)
        text_frame.grid_columnconfigure(0, weight=1)
        scroll = ttk.Scrollbar(text_frame, orient=tk.VERTICAL)
        detail_text = tk.Text(
            text_frame,
            wrap="word",
            font=("Consolas", max(10, self.tree_font_size)),
            bg=self.colors["card_background"],
            fg=self.colors["text"],
            relief="flat",
            padx=12,
            pady=10,
            height=12,
        )
        detail_text.insert("1.0", self._barcode_detail_text(details))
        detail_text.configure(state="disabled", yscrollcommand=scroll.set)
        scroll.config(command=detail_text.yview)
        scroll.pack(side=tk.RIGHT, fill=tk.Y)
        detail_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        button_frame = ttk.Frame(main_frame, style="TFrame")
        button_frame.grid(row=2, column=0, sticky="e", pady=(12, 0))
        copy_button = ttk.Button(button_frame, text="복사", command=lambda: self._copy_text_to_clipboard(self._barcode_detail_text(details)))
        copy_button.pack(side=tk.LEFT, padx=(0, 8))
        close_button = ttk.Button(button_frame, text="닫기", command=lambda: self._destroy_modal_and_refocus(detail_win))
        close_button.pack(side=tk.LEFT)
        detail_win.bind("<Escape>", lambda event: self._destroy_modal_and_refocus(detail_win))
        detail_win.bind("<Control-c>", lambda event: self._copy_text_to_clipboard(self._barcode_detail_text(details)))
        close_button.focus_set()

    def _copy_text_to_clipboard(self, text):
        self.clipboard_clear()
        self.clipboard_append(text)

    def _reload_today_history(self):
        if self._block_background_history_reload(parent=self):
            return
        if self._block_duplicate_history_load(parent=self):
            return
        self._load_history_and_rebuild_summary(None)
        self._process_history_queue()

    def _truncate_string(self, text: str, max_len: int = 35) -> str:
        if len(text) > max_len:
            return text[:max_len] + "..."
        return text

    def _middle_ellipsis(self, text, max_len=None):
        text = str(text or "")
        if max_len is None:
            profile_name = self.__dict__.get("ui_profile_name", "standard")
            max_len = self.BARCODE_DISPLAY_LIMITS.get(profile_name, self.BARCODE_DISPLAY_LIMITS["standard"])
        if len(text) <= max_len:
            return text
        if max_len <= 10:
            return text[:max_len - 3] + "..."
        tail_len = max(6, min(10, max_len // 3))
        head_len = max_len - tail_len - 3
        return f"{text[:head_len]}...{text[-tail_len:]}"

    def _operator_scan_tree_viewport_width(self, tree):
        """Return a realized scan-tree width that matches the live notebook.

        A hidden ttk.Notebook page retains its previous geometry.  During the
        first F4 render after a resize, using that stale width can either
        over-truncate the barcode or let it run beyond the newly visible
        column.  Both scan pages share one notebook viewport, so prefer the
        realized tree whose width is closest to that viewport.
        """

        def widget_width(widget):
            if widget is None:
                return 0
            try:
                return max(0, int(widget.winfo_width()))
            except (TclError, AttributeError, TypeError, ValueError):
                return 0

        def widget_is_mapped(widget):
            if widget is None:
                return False
            try:
                return bool(widget.winfo_ismapped())
            except (TclError, AttributeError, TypeError, ValueError):
                return False

        notebook_width = widget_width(
            self.__dict__.get("live_scan_notebook")
        )
        own_width = widget_width(tree)
        if notebook_width <= 1:
            return own_width

        candidates = []
        seen = set()
        for candidate in (
            tree,
            self.__dict__.get("qa_scan_tree"),
            self.__dict__.get("exact_rescan_tree"),
        ):
            if candidate is None or id(candidate) in seen:
                continue
            seen.add(id(candidate))
            width = widget_width(candidate)
            if width > 1 and abs(notebook_width - width) <= 32:
                candidates.append((widget_is_mapped(candidate), width))
        if not candidates:
            # The configured column width remains the fail-safe when neither
            # page has been realized for the current notebook geometry.
            return 0
        mapped = [width for is_mapped, width in candidates if is_mapped]
        available = mapped or [width for _is_mapped, width in candidates]
        return min(available, key=lambda width: abs(notebook_width - width))

    @staticmethod
    def _operator_scan_summary(raw_value, parsed_value, scan_position):
        return _label_match_operator_scan_summary(
            raw_value,
            parsed_value,
            scan_position,
        )

    def _fit_operator_tree_cell_text(self, tree, column, value, *, padding=20):
        """Fit a scan value to its visible column while retaining both ends.

        The complete accepted value remains in ``_qa_scan_detail_rows`` and the
        lower read-only detail field.  This helper only prevents Treeview from
        hard-cropping a character with no visual indication.
        """

        text = str(value or "")
        if not text:
            return text
        try:
            configured_width = max(1, int(tree.column(column, "width")))
            try:
                stretch = bool(tree.column(column, "stretch"))
            except (TclError, TypeError, ValueError):
                stretch = False
            if stretch:
                # Tk stretches the value column to consume the live widget
                # width after the responsive pass.  Fit against that final
                # width immediately so a settle pass does not leave every
                # barcode one glyph shorter than the visible column.
                columns = tuple(str(value) for value in tree.cget("columns"))
                occupied = sum(
                    max(0, int(tree.column(name, "width")))
                    for name in columns
                    if name != str(column)
                )
                viewport_width = self._operator_scan_tree_viewport_width(tree)
                if viewport_width > 1:
                    stretched_width = max(1, viewport_width - occupied)
                    configured_width = max(configured_width, stretched_width)
            column_width = max(1, configured_width - int(padding))
            style_name = str(tree.cget("style") or "Operator.Treeview")
            style = self.__dict__.get("style")
            if style is None:
                raise AttributeError("Tk style is unavailable")
            font_spec = style.lookup(style_name, "font")
            font = tkFont.Font(root=self, font=font_spec)
            if int(font.measure(text)) <= column_width:
                return text

            low, high = 10, len(text)
            best = self._middle_ellipsis(text, low)
            while low <= high:
                middle = (low + high) // 2
                candidate = self._middle_ellipsis(text, middle)
                if int(font.measure(candidate)) <= column_width:
                    best = candidate
                    low = middle + 1
                else:
                    high = middle - 1
            return best
        except (TclError, AttributeError, RecursionError, TypeError, ValueError):
            return self._middle_ellipsis(text, 72)

    def _compact_operator_notice_message(self, message):
        """Keep one fixed notice region to reason, key value, and next action."""

        lines = [line.strip() for line in str(message or "").splitlines() if line.strip()]
        if len(lines) <= 3:
            return "\n".join(lines)
        detail = next(
            (
                line
                for line in lines[1:-1]
                if any(
                    marker in line
                    for marker in ("스캔 제품", "중복 제품", "중복 스캔", "입력")
                )
            ),
            lines[-2],
        )
        if ":" in detail:
            label, value = detail.split(":", 1)
            detail = f"{label}: {self._middle_ellipsis(value.strip(), 32)}"
        next_action = lines[-1]
        if "오류 처리" in next_action and "새 현품표" in next_action:
            next_action = "→ 제품 제거 후 확인 → 새 현품표 스캔"
        elif "제품을 제거" in next_action and "새 현품표" in next_action:
            next_action = "→ 제품 제거 후 확인 → 새 현품표 스캔"
        return "\n".join((lines[0], detail, next_action))

    def _barcode_cell_display_limit(self, column=None):
        profile_name = self.__dict__.get("ui_profile_name", "standard")
        profile_limit = self.BARCODE_DISPLAY_LIMITS.get(profile_name, self.BARCODE_DISPLAY_LIMITS["standard"])
        if not column or "history_tree" not in self.__dict__:
            return profile_limit
        try:
            width = int(self.history_tree.column(column, "width"))
        except Exception:
            return profile_limit
        font_size = max(8, int(self.__dict__.get("tree_font_size", 13)))
        pixel_limit = max(9, int(width / max(7, font_size * 0.72)))
        return max(9, min(profile_limit, pixel_limit))

    def _format_barcode_cell(self, value, column=None):
        if column == "Input1":
            return str(value or "")
        return self._middle_ellipsis(value, self._barcode_cell_display_limit(column))

    def _summary_code_display_limit(self):
        profile_name = self.__dict__.get("ui_profile_name", "standard")
        profile_limit = self.BARCODE_DISPLAY_LIMITS.get(profile_name, self.BARCODE_DISPLAY_LIMITS["standard"])
        if "summary_tree" not in self.__dict__:
            return profile_limit
        try:
            width = int(self.summary_tree.column("Code", "width"))
        except Exception:
            return profile_limit
        font_size = max(8, int(self.__dict__.get("_current_tree_body_font_size", self.__dict__.get("tree_font_size", 13))))
        pixel_limit = max(8, int(width / max(7, font_size * 0.72)))
        return max(8, min(profile_limit, pixel_limit))

    def _format_summary_code_cell(self, value):
        text = str(value or "")
        decoded_text = _label_match_decode_possible_base64_label(text)
        label_data = self._parse_new_format_label(decoded_text)
        if label_data:
            item_code = label_data.get("CLC", "").strip()
            product_name = label_data.get("SPC", "").strip()
            if item_code and product_name:
                return f"{item_code} | {product_name}"
            return product_name or item_code or text

        item_info = self.__dict__.get("items_data", {}).get(text, {})
        if isinstance(item_info, dict):
            product_name = str(item_info.get("Item Name", "") or "").strip()
            if product_name and product_name != "알 수 없음" and product_name != text:
                return f"{text} | {product_name}"
        return text

    def _summary_positive_dates(self, scan_count):
        dates = []
        for date_str, items in (scan_count or {}).items():
            if any(count > 0 for count in (items or {}).values()):
                dates.append(str(date_str))
        return sorted(set(dates))

    def _summary_date_text(self, scan_count):
        dates = self._summary_positive_dates(scan_count)
        if not dates:
            return "날짜 -"
        if len(dates) == 1:
            return f"날짜 {dates[0]}"
        return f"기간 {dates[0]} ~ {dates[-1]}"

    def _set_summary_date_label(self, scan_count=None, text=None):
        label = self.__dict__.get("summary_date_label")
        if label is None:
            return
        try:
            label.configure(text=text or self._summary_date_text(scan_count or {}))
        except TclError:
            pass

    @classmethod
    def _history_barcode_columns(cls):
        return tuple(f"Input{index}" for index in range(1, cls.TOTAL_SCAN_COUNT + 1))

    @classmethod
    def _history_result_index(cls):
        return 1 + cls.TOTAL_SCAN_COUNT

    @classmethod
    def _history_result_value(cls, values):
        values = tuple(values or ())
        result_index = cls._history_result_index()
        result_labels = {
            cls.Results.PASS,
            cls.Results.FAIL_MISMATCH,
            cls.Results.FAIL_INPUT_ERROR,
            cls.Results.IN_PROGRESS,
        }
        if len(values) > result_index:
            result = values[result_index]
            if result in result_labels:
                return result
        legacy_result_index = 6
        if len(values) > legacy_result_index:
            result = values[legacy_result_index]
            if result in result_labels:
                return result
        if len(values) > result_index:
            return values[result_index]
        return None

    def _idle_instruction_text(self):
        return f"1/{self.TOTAL_SCAN_COUNT} 현품표 스캔"

    def _history_values_for_display(self, values):
        values = list(values or [])
        expected_len = 1 + self.TOTAL_SCAN_COUNT + 2
        while len(values) < expected_len:
            values.append("")
        display_id = values[0]
        barcode_columns = self._history_barcode_columns()
        scans = [
            self._format_barcode_cell(value, barcode_columns[index])
            for index, value in enumerate(values[1:1 + self.TOTAL_SCAN_COUNT])
        ]
        result_index = 1 + self.TOTAL_SCAN_COUNT
        timestamp_index = result_index + 1
        return (display_id, *scans, values[result_index], values[timestamp_index])

    def _refresh_summary_tree_display_values(self):
        raw_rows = self.__dict__.get("summary_row_raw_values", {})
        if not raw_rows or "summary_tree" not in self.__dict__:
            return
        for item_id, raw_values in list(raw_rows.items()):
            if not self.summary_tree.exists(item_id):
                raw_rows.pop(item_id, None)
                continue
            _date_text, code, phase, count = raw_values
            self.summary_tree.item(item_id, values=(self._format_summary_code_cell(code), phase, count))

    def _history_values_from_details(self, iid, current_values=None):
        details = self._history_details_for_iid(iid)
        if not details:
            return current_values
        current_values = list(current_values or [])
        display_id = current_values[0] if current_values else ""
        timestamp_index = 1 + self.TOTAL_SCAN_COUNT + 1
        timestamp = current_values[timestamp_index] if len(current_values) > timestamp_index else ""
        result = _label_match_tray_complete_result(details)
        if result == self.Results.PASS and details.get("final_result") == self.Results.IN_PROGRESS:
            result = self.Results.IN_PROGRESS
        parsed_scans = list(details.get("parsed_product_barcodes") or [])
        scan_values = parsed_scans[:self.TOTAL_SCAN_COUNT]
        values = (
            display_id,
            *scan_values + [""] * (self.TOTAL_SCAN_COUNT - len(scan_values)),
            result,
            timestamp,
        )
        return values

    def _refresh_history_tree_display_values(self):
        if "history_tree" not in self.__dict__:
            return
        try:
            for iid in self.history_tree.get_children():
                if iid == "loading":
                    continue
                current_values = self.history_tree.item(iid, "values")
                source_values = self._history_values_from_details(iid, current_values)
                if source_values:
                    self.history_tree.item(iid, values=self._history_values_for_display(source_values))
        except Exception as e:
            print(f"기록 표시값 갱신 오류: {e}")

    def _details_for_current_set(self):
        current = self.__dict__.get("current_set_info", {}) or {}
        return {
            "set_id": current.get("id"),
            "scanned_product_barcodes": list(current.get("raw") or []),
            "parsed_product_barcodes": list(current.get("parsed") or []),
            "final_result": self.Results.IN_PROGRESS,
            "phase": current.get("phase") or "-",
            "production_date": current.get("production_date"),
        }

    def _history_details_for_iid(self, iid):
        if not iid:
            return None
        details = self._dict_value_by_string_key(self.__dict__.get("history_row_details_map", {}), iid)
        if details:
            return details
        current = self.__dict__.get("current_set_info", {}) or {}
        if str(current.get("id")) == str(iid):
            return self._details_for_current_set()
        return self._dict_value_by_string_key(self.__dict__.get("set_details_map", {}), iid)

    def _barcode_detail_text(self, details):
        if not details:
            return "기록을 선택하면 현품표와 제품 바코드 원문이 여기에 표시됩니다."
        raw_scans = list(details.get("scanned_product_barcodes") or [])
        parsed_scans = list(details.get("parsed_product_barcodes") or [])
        lines = [
            f"결과: {_label_match_tray_complete_result(details)}",
            f"세트 ID: {details.get('set_id') or '-'}",
        ]
        production_date = details.get("production_date")
        phase = details.get("phase") or "-"
        if production_date or phase != "-":
            lines.append(f"생산일/차수: {production_date or '-'} / {phase}")
        lines.append("")
        for index, role in enumerate(self.STEP_NAMES):
            parsed = parsed_scans[index] if index < len(parsed_scans) else ""
            raw = raw_scans[index] if index < len(raw_scans) else ""
            if not parsed and not raw:
                continue
            lines.append(f"[{index + 1}] {role}")
            if parsed:
                lines.append(f"  표시값: {parsed}")
            if raw and raw != parsed:
                lines.append(f"  원문: {raw}")
            elif raw and not parsed:
                lines.append(f"  원문: {raw}")
        return "\n".join(lines).strip()

    def _barcode_inline_detail_text(self, details):
        if not details:
            return "기록을 선택하면 현품표와 제품 바코드 원문이 여기에 표시됩니다."
        raw_scans = list(details.get("scanned_product_barcodes") or [])
        parsed_scans = list(details.get("parsed_product_barcodes") or [])
        lines = []
        for index, role in enumerate(self.STEP_NAMES):
            value = ""
            if index < len(parsed_scans) and parsed_scans[index]:
                value = parsed_scans[index]
            elif index < len(raw_scans):
                value = raw_scans[index]
            if value:
                lines.append(f"{role}: {self._middle_ellipsis(value, 72)}")
        if not lines:
            lines.append(f"결과: {_label_match_tray_complete_result(details)}")
            lines.append(f"세트 ID: {details.get('set_id') or '-'}")
        return "\n".join(lines)

    def _apply_operator_responsive_layout(self, *, settle=False):
        workbench = self.__dict__.get("operator_workbench_frame")
        if workbench is None:
            return
        try:
            # Never enter a nested Tk event loop from a <Configure>-driven
            # layout pass.  It can dispatch another configure callback before
            # this pass raises its re-entry guard and create an event storm.
            root_width = int(self.winfo_width())
            root_height = int(self.winfo_height())
            if root_width <= 100:
                root_width = int(self.winfo_screenwidth())
            if root_height <= 100:
                root_height = int(self.winfo_screenheight())
            profile_name, profile = self._select_ui_profile(root_width, root_height)
            self.ui_profile_name = profile_name
            self.ui_profile = profile
            outer_padding = int(profile["outer_padding"])
            # On short auxiliary displays the former 64 px header pushed the
            # center notebook through the card's bottom padding.  The title,
            # context, clock, and two utility buttons all fit their measured
            # requests within 56 px, returning eight pixels to the operator
            # workbench without shrinking the five-row scan list or raw detail.
            header_height = 56 if root_height <= 800 else 72
            status_height = 32
            section_gap = int(profile["section_gap"])
            bottom_gap = int(profile["bottom_gap"])
            self.main_frame.configure(padding=outer_padding)
            width = max(1, root_width - outer_padding * 2)
            height = max(
                320,
                root_height
                - outer_padding * 2
                - header_height
                - section_gap
                - status_height
                - bottom_gap,
            )
        except (TclError, AttributeError, TypeError, ValueError):
            profile = getattr(self, "ui_profile", self.UI_PROFILES["standard"])
            outer_padding = int(profile["outer_padding"])
            header_height = 72
            status_height = 32
            section_gap = int(profile["section_gap"])
            bottom_gap = int(profile["bottom_gap"])
            root_width, root_height = 0, 0
            width, height = 0, 0
        if width <= 100:
            try:
                width = max(980, min(1920, int(self.winfo_screenwidth())) - outer_padding * 2)
            except (TclError, AttributeError, TypeError, ValueError):
                width = 1440
        if height <= 100:
            try:
                height = max(620, int(self.winfo_height()) - 100)
            except (TclError, AttributeError, TypeError, ValueError):
                height = 800
        scale = float(getattr(self, "scale_factor", 1.0) or 1.0)
        metrics = build_operator_layout(width, height, scale)
        tokens = build_style_tokens(metrics.profile.name, scale)
        self.operator_layout_metrics = metrics
        self.operator_style_tokens = tokens
        self.operator_height_budget = {
            "root_height": root_height,
            "outer_padding": outer_padding,
            "header_height": header_height,
            "section_gap": section_gap,
            "workbench_height": height,
            "bottom_gap": bottom_gap,
            "status_height": status_height,
        }
        panes = metrics.panes
        constrained_large_text = scale >= 1.25 and height < 760
        compact_large_text = scale >= 1.25 and (
            constrained_large_text or panes.center_width < 650
        )
        short_auxiliary_height = height < 660
        self._operator_hide_left_badges = constrained_large_text
        self._operator_constrained_large_text = constrained_large_text
        self._operator_compact_large_text = compact_large_text
        self._operator_short_auxiliary_height = short_auxiliary_height
        card_padding = min(
            int(profile["card_padding"]),
            8 if constrained_large_text else int(profile["card_padding"]),
        )
        pane_widths = {
            "operator_left_pane": panes.left_width,
            "operator_center_pane": panes.center_width,
            "operator_right_pane": panes.right_width,
        }
        self._applying_responsive_layout = True
        try:
            self.main_frame.configure(padding=outer_padding)
            self.operator_header_frame.grid_configure(
                pady=(0, section_gap),
            )
            self.operator_header_frame.configure(height=header_height)
            self.operator_header_frame.grid_propagate(False)
            self.operator_status_frame.grid_configure(
                pady=(bottom_gap, 0),
            )
            self.operator_status_frame.configure(height=status_height)
            self.operator_status_frame.grid_propagate(False)
            workbench.configure(width=width, height=height)
            workbench.grid_propagate(False)
            workbench.grid_columnconfigure(0, minsize=panes.left_width, weight=0)
            workbench.grid_columnconfigure(
                1,
                minsize=panes.center_width + panes.gap * 2,
                weight=0,
            )
            workbench.grid_columnconfigure(2, minsize=panes.right_width, weight=0)
            workbench.grid_rowconfigure(0, minsize=height, weight=1)
            self.operator_center_pane.grid_configure(padx=(panes.gap, panes.gap))
            for pane_name, pane_width in pane_widths.items():
                pane = self.__dict__.get(pane_name)
                if pane is not None:
                    pane.configure(
                        padding=card_padding,
                        width=pane_width,
                        height=height,
                    )
                    pane.grid_propagate(False)
            headline_font_size = min(
                tokens.fonts.headline,
                (
                    26
                    if constrained_large_text
                    else 26 if short_auxiliary_height
                    else 28 if compact_large_text else tokens.fonts.headline
                ),
            )
            if constrained_large_text:
                notice_font_size = min(tokens.fonts.body, 13)
                notice_title_font_size = notice_font_size
            elif compact_large_text:
                notice_font_size = min(tokens.fonts.body, 14)
                notice_title_font_size = min(tokens.fonts.body, 15)
            else:
                notice_font_size = tokens.fonts.body
                notice_title_font_size = notice_font_size
            notice_message_font = (self.default_font_name, notice_font_size)
            notice_title_font = (
                self.default_font_name,
                notice_title_font_size,
                "bold",
            )
            scan_input_font_size = min(
                tokens.fonts.scan_input,
                18 if compact_large_text else tokens.fonts.scan_input,
            )
            live_list_font_size = min(
                tokens.fonts.live_list,
                15 if compact_large_text else tokens.fonts.live_list,
            )
            if constrained_large_text:
                worker = str(self.__dict__.get("worker_name") or "작업자")
                self.operator_title_label.configure(
                    text=f"Label Match · {self._middle_ellipsis(worker, 10)}",
                    font=(self.default_font_name, 18, "bold"),
                )
                self.operator_header_context_label.grid_remove()
            else:
                self.operator_title_label.configure(
                    text="Label Match · 포장 라벨 검증",
                    font=(self.default_font_name, tokens.fonts.section_title, "bold"),
                )
                self.operator_header_context_label.grid()
            operator_caption_size = max(11, min(14, tokens.fonts.caption))
            self.operator_header_context_label.configure(
                font=(self.default_font_name, operator_caption_size),
            )
            self.clock_label.configure(
                font=("Consolas", operator_caption_size),
            )
            self.operator_footer_label.configure(
                font=(self.default_font_name, min(13, operator_caption_size)),
            )
            self.save_status_label.configure(
                font=(self.default_font_name, min(14, tokens.fonts.body), "bold"),
            )
            self.big_display_label.configure(
                font=(self.default_font_name, headline_font_size, "bold"),
                wraplength=max(320, panes.center_width - tokens.spacing.xl * 2),
            )
            vertical_gap = (
                2
                if constrained_large_text
                else 3 if short_auxiliary_height else 8
            )
            self.big_display_label.grid_configure(pady=(0, vertical_gap))
            self.progress_frame.grid_configure(pady=(0, vertical_gap))
            self.workflow_notice_frame.grid_configure(pady=(0, vertical_gap))
            self.operator_input_frame.grid_configure(pady=(0, vertical_gap))
            try:
                notice_message_linespace = int(
                    tkFont.Font(root=self, font=notice_message_font).metrics(
                        "linespace"
                    )
                )
                notice_title_linespace = int(
                    tkFont.Font(root=self, font=notice_title_font).metrics(
                        "linespace"
                    )
                )
            except (TclError, AttributeError, TypeError, ValueError):
                notice_message_linespace = max(20, notice_font_size * 3)
                notice_title_linespace = max(20, notice_title_font_size * 3)
            if compact_large_text:
                title_pad_top, message_pad_top, message_pad_bottom = 4, 0, 4
            else:
                title_pad_top, message_pad_top, message_pad_bottom = 7, 1, 7
            # Tk point sizes are not pixel heights.  Reserve one title plus the
            # compact notice contract's three message lines using the actual
            # font metrics for the window's current monitor DPI.
            label_vertical_chrome = 6
            minimum_notice_height = (
                120
                if short_auxiliary_height and not constrained_large_text
                else 132
            )
            notice_height = max(
                minimum_notice_height,
                title_pad_top
                + notice_title_linespace
                + label_vertical_chrome
                + message_pad_top
                + notice_message_linespace * 3
                + label_vertical_chrome
                + message_pad_bottom
                + 2,
            )
            self._operator_notice_base_height = notice_height
            self.workflow_notice_frame.configure(height=notice_height)
            self.workflow_notice_frame.grid_propagate(False)
            self.workflow_notice_frame.grid_rowconfigure(1, weight=1)
            if constrained_large_text:
                self.workflow_notice_title_label.grid_configure(
                    padx=10,
                    pady=(4, 0),
                )
                self.workflow_notice_label.grid_configure(
                    padx=10,
                    pady=(0, 4),
                )
            else:
                self.workflow_notice_title_label.grid_configure(
                    padx=12,
                    pady=(7, 0),
                )
                self.workflow_notice_label.grid_configure(
                    padx=12,
                    pady=(1, 7),
                )
            self.workflow_notice_label.configure(
                font=notice_message_font,
                wraplength=max(260, panes.center_width - 180),
                anchor="nw",
            )
            self.workflow_notice_title_label.configure(
                font=notice_title_font
            )
            self.style.configure(
                "Operator.NoticeAction.TButton",
                font=(
                    self.default_font_name,
                    13 if compact_large_text else max(11, notice_font_size - 1),
                    "bold",
                ),
                padding=(6, 4),
            )
            self.workflow_notice_action_button.configure(
                style="Operator.NoticeAction.TButton"
            )
            self.entry.configure(
                font=(self.default_font_name, scan_input_font_size),
                width=1,
            )
            self.operator_scan_input_label.configure(
                font=(
                    self.default_font_name,
                    min(18, tokens.fonts.section_title),
                    "bold",
                ),
            )
            self.entry.grid_configure(
                ipady=(
                    5
                    if constrained_large_text
                    else 6 if short_auxiliary_height else 8
                )
            )
            # The five-step rail already expresses progress.  Keeping a second
            # percentage-like bar consumes the exact vertical space needed by
            # the operator's actual five scan rows on a short auxiliary screen.
            self.progress_bar.grid_remove()
            for step_label in self.step_labels:
                step_label.configure(
                    padx=4 if panes.center_width < 650 else 6,
                    pady=3 if constrained_large_text else 5,
                )
            self.operator_last_scan_label.configure(
                font=(self.default_font_name, tokens.fonts.detail),
                wraplength=max(300, panes.center_width - 30),
            )
            self.operator_last_scan_label.grid_remove()
            left_wrap = max(120, panes.left_width - card_padding * 2)
            for name in (
                "operator_item_stage_label",
                "operator_item_code_label",
                "operator_item_name_label",
                "operator_item_spec_label",
                "operator_set_id_label",
                "operator_membership_label",
                "operator_badges_label",
                "operator_left_hint_label",
            ):
                widget = self.__dict__.get(name)
                if widget is not None:
                    widget.configure(wraplength=left_wrap)
            left_body_size = min(
                tokens.fonts.sidebar_body,
                17 if constrained_large_text else tokens.fonts.sidebar_body,
            )
            left_title_size = min(
                tokens.fonts.section_title,
                18 if constrained_large_text else tokens.fonts.section_title,
            )
            self.operator_item_stage_label.configure(
                font=(self.default_font_name, left_title_size, "bold")
            )
            self.operator_left_heading_label.configure(
                font=(self.default_font_name, left_title_size, "bold")
            )
            self.operator_item_code_label.configure(
                font=(self.default_font_name, min(left_title_size, 16), "bold")
            )
            for name in (
                "operator_item_name_label",
                "operator_item_spec_label",
                "operator_item_phase_label",
                "operator_set_id_label",
                "operator_badges_label",
            ):
                widget = self.__dict__.get(name)
                if widget is not None:
                    widget.configure(font=(self.default_font_name, left_body_size))
            self.operator_membership_label.configure(
                font=(
                    self.default_font_name,
                    min(left_body_size, 15 if constrained_large_text else left_body_size),
                    "bold",
                ),
                wraplength=max(120, left_wrap - 8),
            )
            right_title_size = min(20, tokens.fonts.section_title)
            self.operator_session_heading_label.configure(
                font=(self.default_font_name, right_title_size, "bold")
            )
            self.hist_header_label.configure(
                font=(self.default_font_name, right_title_size, "bold")
            )
            self.summary_header_label.configure(
                font=(self.default_font_name, right_title_size, "bold")
            )
            if constrained_large_text:
                self.operator_left_divider.grid_remove()
                self.operator_membership_heading_label.grid_remove()
                self.operator_left_hint_label.grid_remove()
            else:
                self.operator_left_divider.grid()
                self.operator_membership_heading_label.grid()
                self.operator_left_hint_label.grid()
            action_font_size = min(
                tokens.fonts.button,
                15 if panes.right_width < 480 else tokens.fonts.button,
            )
            action_font = (
                self.default_font_name,
                max(11, action_font_size),
                "bold",
            )
            try:
                action_line_height = int(
                    tkFont.Font(root=self, font=action_font).metrics("linespace")
                )
            except (TclError, TypeError, ValueError):
                action_line_height = max(16, int(action_font_size * 1.5))
            action_button_height = max(
                86,
                metrics.center.actions.button_height,
                action_line_height * 2 + 24,
            )
            action_button_height = min(104, action_button_height)
            # Each row has four pixels of external vertical grid padding.  Size
            # the row extent, not just its slot, so the visible/clickable button
            # remains at the intended 86-104 px target without growing the
            # overall two-row action area.
            action_row_extent = action_button_height + 4
            self.operator_action_frame.grid_rowconfigure(
                0, minsize=action_row_extent
            )
            self.operator_action_frame.grid_rowconfigure(
                1, minsize=action_row_extent
            )
            self.style.configure(
                "Operator.Action.TButton",
                font=action_font,
                padding=(4, 6),
            )
            self.style.configure(
                "Operator.Danger.Action.TButton",
                font=action_font,
                padding=(4, 6),
            )
            for button_name, compact_text, compact_style in (
                ("manual_complete_button", "소량\n완료 (F3)", "Operator.Action.TButton"),
                ("exact_rescan_button", "전체\n재스캔 (F4)", "Operator.Action.TButton"),
                ("reset_button", "현재 세트\n취소 (F1)", "Operator.Danger.Action.TButton"),
                ("cancel_tray_button", "완료 트레이\n취소 (F2)", "Operator.Danger.Action.TButton"),
            ):
                button = self.__dict__.get(button_name)
                if button is not None:
                    button.configure(text=compact_text, style=compact_style, width=1)
            right_inner_width = max(220, panes.right_width - card_padding * 2)
            right_inner_height = max(220, height - card_padding * 2)
            action_total_height = action_row_extent * 2
            self.operator_action_frame.configure(
                width=right_inner_width,
                height=action_total_height,
            )
            self.operator_action_frame.grid_propagate(False)
            self.operator_notebook.configure(
                width=right_inner_width,
                height=max(140, right_inner_height - action_total_height - 10),
            )
            tree_row_height = max(
                30,
                int(
                    live_list_font_size
                    * (2.05 if constrained_large_text else 2.35)
                ),
            )
            self.style.configure(
                "Operator.Treeview",
                font=(self.default_font_name, live_list_font_size),
                rowheight=tree_row_height,
            )
            center_inner_width = max(320, panes.center_width - card_padding * 2)
            if compact_large_text:
                stage_width = 180
            elif scale >= 1.25:
                stage_width = min(180, max(165, int(center_inner_width * 0.28)))
            else:
                stage_width = min(146, max(132, int(center_inner_width * 0.21)))
            state_width = min(92, max(72, int(center_inner_width * 0.14)))
            value_width = max(150, center_inner_width - stage_width - state_width - 12)
            self.qa_scan_tree.configure(style="Operator.Treeview", height=5)
            self.qa_scan_tree.column("Stage", width=stage_width, minwidth=80, stretch=False)
            self.qa_scan_tree.column("Value", width=value_width, minwidth=140, stretch=True)
            self.qa_scan_tree.column("State", width=state_width, minwidth=68, stretch=False)
            detail_font_size = max(9, min(12, live_list_font_size - 2))
            detail_title_font_size = max(
                11,
                min(16, live_list_font_size - 1),
            )
            for prefix in ("qa_scan", "exact_rescan"):
                self.__dict__[f"{prefix}_detail_title_label"].configure(
                    font=(self.default_font_name, detail_title_font_size, "bold"),
                )
                self.__dict__[f"{prefix}_detail_metadata_label"].configure(
                    font=(self.default_font_name, min(14, detail_font_size + 1)),
                )
                self.__dict__[f"{prefix}_detail_text"].configure(
                    font=("Consolas", detail_font_size),
                    height=2,
                )
            # ``winfo_reqheight`` reflects each widget's configured font and
            # text request without draining Tk's global idle queue here.
            try:
                detail_header_height = max(
                    int(self.qa_scan_detail_title_label.winfo_reqheight()),
                    int(self.qa_scan_detail_metadata_label.winfo_reqheight()),
                    int(self.exact_rescan_detail_title_label.winfo_reqheight()),
                    int(self.exact_rescan_detail_metadata_label.winfo_reqheight()),
                )
                detail_text_height = max(
                    int(self.qa_scan_detail_text.winfo_reqheight()),
                    int(self.exact_rescan_detail_text.winfo_reqheight()),
                )
                detail_height = detail_header_height + detail_text_height + 16
            except (TclError, AttributeError, TypeError, ValueError):
                detail_height = 90
            detail_height = max(90, min(112, detail_height))
            for prefix in ("qa_scan", "exact_rescan"):
                detail_frame = self.__dict__[f"{prefix}_detail_frame"]
                detail_frame.configure(height=detail_height)
                detail_frame.grid_propagate(False)
            self.exact_rescan_tree.configure(style="Operator.Treeview", height=5)
            self.exact_rescan_tree.column("Order", width=76, minwidth=58, stretch=False)
            self.exact_rescan_tree.column(
                "Value",
                width=max(190, center_inner_width - 88),
                minwidth=170,
                stretch=True,
            )
            tree_content_height = max(
                188,
                5 * tree_row_height + 38,
            )
            live_list_height = tree_content_height + detail_height + (
                39 if scale >= 1.25 else 37
            )
            self.live_scan_notebook.configure(
                width=center_inner_width,
                height=live_list_height,
            )
            self.operator_center_pane.grid_rowconfigure(
                4,
                minsize=live_list_height,
                weight=1,
            )
            self.session_tree.column("Time", width=64, minwidth=54, stretch=False)
            self.session_tree.column("Result", width=68, minwidth=58, stretch=False)
            self.session_tree.column(
                "Item",
                width=max(100, right_inner_width - 148),
                minwidth=90,
                stretch=True,
            )
            if settle:
                # Column widths are final on the settle pass; rerender only the
                # presentation model so visible ellipses match those widths.
                self._render_operator_workbench()
        except (TclError, AttributeError, KeyError, TypeError):
            return
        finally:
            self._applying_responsive_layout = False
        if not settle:
            pending = self.__dict__.get("_operator_layout_settle_after_id")
            if pending:
                try:
                    self.after_cancel(pending)
                except TclError:
                    pass
            try:
                self._operator_layout_settle_after_id = self.after(
                    40,
                    self._settle_operator_responsive_layout,
                )
            except (TclError, AttributeError):
                self._operator_layout_settle_after_id = None

    def _settle_operator_responsive_layout(self):
        """Reapply height metrics after header visibility changes have settled."""

        self._operator_layout_settle_after_id = None
        try:
            self._apply_operator_responsive_layout(settle=True)
        except TclError:
            return

    def _workflow_view_source(self):
        source = dict(self.__dict__.get("current_set_info", {}) or {})
        display_scans = tuple(self.__dict__.get("_workflow_display_scans", ()) or ())
        if self.__dict__.get("_workflow_completion_kind") and display_scans:
            source["raw"] = list(display_scans)
            display_parsed = tuple(
                self.__dict__.get("_workflow_display_parsed_scans", ()) or ()
            )
            source["parsed"] = list(display_parsed or display_scans)
        source.setdefault("raw", [])
        source.setdefault("parsed", [])
        source.setdefault("exact_rescan_barcodes", [])
        return source

    @staticmethod
    def _workflow_state_text(state):
        return {
            "complete": "완료",
            "current": "현재",
            "pending": "대기",
            "error": "오류",
            "readonly": "조회",
        }.get(str(state), str(state or "-"))

    def _selected_qa_scan_iid(self):
        """Return the selected live QA row without changing keyboard focus."""

        tree = self.__dict__.get("qa_scan_tree")
        if tree is None:
            return None
        try:
            selected = tuple(tree.selection())
        except (TclError, AttributeError, TypeError):
            return None
        return str(selected[0]) if selected else None

    def _set_qa_scan_detail_text(self, value):
        detail_text = self.__dict__.get("qa_scan_detail_text")
        if detail_text is None:
            return
        try:
            detail_text.configure(state="normal")
            detail_text.delete("1.0", tk.END)
            detail_text.insert("1.0", str(value))
            detail_text.configure(state="disabled")
            detail_text.yview_moveto(0.0)
        except (TclError, AttributeError, TypeError):
            try:
                detail_text.configure(state="disabled")
            except (TclError, AttributeError):
                pass

    def _render_qa_scan_detail(self, selected_iid=None):
        """Show the selected stage, state, and complete accepted raw value."""

        if selected_iid is None:
            selected_iid = self._selected_qa_scan_iid()
        rows = self.__dict__.get("_qa_scan_detail_rows", {}) or {}
        detail = rows.get(str(selected_iid)) if selected_iid else None
        if detail is None:
            metadata = "단계: -  |  상태: -"
            raw_text = "현재 세트 행을 선택하면 수락된 스캔 원문을 확인할 수 있습니다."
        else:
            metadata = (
                f"단계: {detail['stage']}  |  상태: {detail['state']}"
            )
            raw_value = str(detail.get("raw") or "")
            raw_text = raw_value or "수락된 스캔 값 없음"

        metadata_label = self.__dict__.get("qa_scan_detail_metadata_label")
        if metadata_label is not None:
            try:
                metadata_label.configure(text=metadata)
            except (TclError, AttributeError):
                pass
        self._set_qa_scan_detail_text(raw_text)
        return detail

    def _on_qa_scan_selection_changed(self, _event=None):
        """Refresh details only; the existing scanner-focus policy stays intact."""

        self._render_qa_scan_detail()

    def _selected_exact_rescan_iid(self):
        """Return the selected F4 row without changing scanner keyboard focus."""

        tree = self.__dict__.get("exact_rescan_tree")
        if tree is None:
            return None
        try:
            selected = tuple(tree.selection())
        except (TclError, AttributeError, TypeError):
            return None
        return str(selected[0]) if selected else None

    def _set_exact_rescan_detail_text(self, value):
        detail_text = self.__dict__.get("exact_rescan_detail_text")
        if detail_text is None:
            return
        try:
            detail_text.configure(state="normal")
            detail_text.delete("1.0", tk.END)
            detail_text.insert("1.0", str(value))
            detail_text.configure(state="disabled")
            detail_text.yview_moveto(0.0)
        except (TclError, AttributeError, TypeError):
            try:
                detail_text.configure(state="disabled")
            except (TclError, AttributeError):
                pass

    def _render_exact_rescan_detail(self, selected_iid=None):
        """Show the complete raw value for the selected F4 membership row."""

        if selected_iid is None:
            selected_iid = self._selected_exact_rescan_iid()
        rows = self.__dict__.get("_exact_rescan_detail_rows", {}) or {}
        detail = rows.get(str(selected_iid)) if selected_iid else None
        if detail is None:
            metadata = "순서: -"
            raw_text = "F4 재스캔 행을 선택하면 전체 원문을 확인할 수 있습니다."
        else:
            metadata = f"순서: {detail['order']}"
            raw_text = str(detail.get("raw") or "") or "재스캔 값 없음"

        metadata_label = self.__dict__.get("exact_rescan_detail_metadata_label")
        if metadata_label is not None:
            try:
                metadata_label.configure(text=metadata)
            except (TclError, AttributeError):
                pass
        self._set_exact_rescan_detail_text(raw_text)
        return detail

    def _on_exact_rescan_selection_changed(self, _event=None):
        """Refresh F4 raw detail without moving focus away from scan input."""

        self._render_exact_rescan_detail()

    @staticmethod
    def _workflow_headline_text(view):
        """Keep a notice title unique while the layout itself stays fixed."""
        if view.notice is None:
            return view.current_stage_label
        return {
            "initializing": "작업 준비 중",
            "loading": "작업 준비 중",
            "history_readonly": "기록 조회 중",
            "history_loading": "기록 준비 중",
            "submission_blocked": "제출 대기",
            "blocked": "작업 확인 필요",
            "error": "스캔 확인 필요",
            "completion_full": "다음 세트 준비",
            "completion_partial": "다음 세트 준비",
            "completion_failed": "새 세트 준비",
        }.get(view.current_stage, "상태 확인")

    @staticmethod
    def _workflow_left_stage_text(view):
        if view.notice is None:
            return view.current_stage_label
        return {
            "initializing": "준비 중",
            "loading": "준비 중",
            "history_readonly": "조회 전용",
            "history_loading": "기록 로딩",
            "submission_blocked": "제출 보류",
            "blocked": "작업 보류",
            "error": "확인 필요",
            "completion_full": "정상 기록됨",
            "completion_partial": "부분 기록됨",
            "completion_failed": "실패 기록됨",
        }.get(view.current_stage, "상태 확인")

    def _set_exact_rescan_tab_visible(self, visible, *, select=False):
        notebook = self.__dict__.get("live_scan_notebook")
        frame = self.__dict__.get("exact_rescan_frame")
        if notebook is None or frame is None:
            return
        tabs = getattr(notebook, "tabs", None)
        if callable(tabs):
            try:
                tab_ids = tuple(str(tab_id) for tab_id in tabs())
                if visible and str(frame) not in tab_ids:
                    notebook.add(frame, text="F4 전체 재스캔")
                elif visible:
                    notebook.tab(frame, state="normal")
                elif not visible and str(frame) in tab_ids:
                    notebook.hide(frame)
                target = frame if visible and select else self.qa_scan_frame
                try:
                    selected = str(notebook.select())
                except (TclError, TypeError, AttributeError):
                    # Headless protocol fakes expose only select(target).
                    selected = ""
                if selected != str(target):
                    notebook.select(target)
                return
            except (TclError, AttributeError):
                pass
        # Headless contract fakes do not implement Notebook.tabs/hide.
        try:
            if visible:
                frame.grid()
            else:
                frame.grid_remove()
        except (TclError, AttributeError):
            pass

    def _set_workflow_notice_ui(self, notice, next_action):
        title_label = self.__dict__.get("workflow_notice_title_label")
        message_label = self.__dict__.get("workflow_notice_label")
        frame = self.__dict__.get("workflow_notice_frame")
        if title_label is None or message_label is None or frame is None:
            return
        if notice is None:
            title = "다음 행동"
            message = str(next_action or "현품표를 스캔하세요.")
            foreground = self.colors.get("primary", "#2563EB")
            background = "#EFF6FF"
            border = self.colors.get("primary", "#2563EB")
        else:
            title = str(notice.title)
            message = self._compact_operator_notice_message(notice.message)
            next_action_text = str(next_action or "").strip()
            if next_action_text and next_action_text not in message:
                message = f"{message}\n다음: {next_action_text}"
            message = self._compact_operator_notice_message(message)
            tone = str(notice.tone or "danger")
            palette = {
                "success": (self.colors.get("success", "#047857"), "#ECFDF5", "#10B981"),
                "warning": ("#92400E", "#FFFBEB", "#F59E0B"),
                "info": (self.colors.get("primary", "#2563EB"), "#EFF6FF", "#3B82F6"),
                "muted": (self.colors.get("text_subtle", "#6B7280"), "#F3F4F6", "#9CA3AF"),
                "danger": (self.colors.get("danger", "#B91C1C"), "#FEF2F2", "#DC2626"),
            }
            foreground, background, border = palette.get(tone, palette["danger"])
        try:
            frame.configure(bg=background, highlightbackground=border)
            title_label.configure(text=title, bg=background, fg=foreground)
            message_label.configure(text=message, bg=background, fg=self.colors.get("text", "#111827"))
        except (TclError, AttributeError):
            return

        action_button = self.__dict__.get("workflow_notice_action_button")
        if action_button is None:
            return
        show_action = bool(
            self.__dict__.get("_pending_workflow_error")
            or self.__dict__.get("_workflow_pending_error")
            or callable(self.__dict__.get("_workflow_notice_action"))
        )
        try:
            if show_action:
                action_text = str(
                    self.__dict__.get("_workflow_notice_action_text") or "확인"
                )
                action_button.configure(
                    text=action_text,
                    width=10 if action_text == "제출 재시도" else 4,
                )
                compact = bool(
                    self.__dict__.get("_operator_compact_large_text", False)
                )
                action_button.grid_configure(
                    padx=8 if compact else 10,
                    pady=4 if compact else 7,
                )
                action_button.grid()
                action_button.focus_set()
            else:
                action_button.grid_remove()
        except (TclError, AttributeError):
            pass
        self._fit_operator_notice_geometry(show_action)

    def _fit_operator_notice_geometry(self, show_action):
        """Size the single notice region from its real message/action content."""

        frame = self.__dict__.get("workflow_notice_frame")
        title_label = self.__dict__.get("workflow_notice_title_label")
        message_label = self.__dict__.get("workflow_notice_label")
        action_button = self.__dict__.get("workflow_notice_action_button")
        if frame is None or title_label is None or message_label is None:
            return
        if "tk" not in self.__dict__:
            # Structural unit-test doubles deliberately skip ``tk.Tk.__init__``.
            return

        compact = bool(self.__dict__.get("_operator_compact_large_text", False))
        constrained = bool(
            self.__dict__.get("_operator_constrained_large_text", False)
        )
        message_pad_x = 10 if compact else 12
        action_pad_x = 8 if compact else 10
        title_pad_top = 4 if compact else 7
        message_pad_top = 0 if compact else 1
        message_pad_bottom = 4 if compact else 7
        action_pad_y = 4 if compact else 7

        try:
            frame_width = int(frame.winfo_width())
            if frame_width <= 100:
                metrics = self.__dict__.get("operator_layout_metrics")
                card_padding = int(
                    min(
                        getattr(self, "ui_profile", self.UI_PROFILES["standard"])[
                            "card_padding"
                        ],
                        8 if compact else 10_000,
                    )
                )
                frame_width = max(
                    320,
                    int(metrics.panes.center_width) - card_padding * 2,
                )

            action_width = 0
            if show_action and action_button is not None:
                action_width = int(action_button.winfo_reqwidth()) + action_pad_x * 2
            available_message_width = max(
                220,
                frame_width - message_pad_x * 2 - action_width - 4,
            )
            message_label.configure(wraplength=available_message_width)
            # The configured wrap length updates the widget's requested size
            # without entering a nested Tk idle loop.  Draining idle events
            # here can recursively dispatch <Configure> layout callbacks.

            text_height = (
                title_pad_top
                + int(title_label.winfo_reqheight())
                + message_pad_top
                + int(message_label.winfo_reqheight())
                + message_pad_bottom
            )
            action_height = 0
            if show_action and action_button is not None:
                action_height = int(action_button.winfo_reqheight()) + action_pad_y * 2
            base_height = int(self.__dict__.get("_operator_notice_base_height", 132))
            content_height = max(text_height, action_height)
            self._operator_notice_required_height = content_height
            # Every semantic state shares the same physical notice row.  The
            # display copy is compacted to reason/key value/next action above;
            # expanding this frame would push the scan input and live list.
            frame.configure(height=base_height)
        except (TclError, AttributeError, KeyError, TypeError, ValueError):
            return

    def _update_operator_item_panel(self, view, source):
        scans = list(source.get("parsed") or [])
        item_code = str(scans[0] if scans else "")
        snapshot = self.__dict__.get("_workflow_item_snapshot") or {}
        if not item_code and snapshot:
            item_code = str(snapshot.get("item_code") or "")
        item_name_override = source.get("item_name_override") or snapshot.get("item_name_override")
        item_info = self.__dict__.get("items_data", {}).get(item_code, {}) if item_code else {}
        item_name = str(item_name_override or item_info.get("Item Name") or "-")
        spec = str(item_info.get("Spec") or snapshot.get("spec") or "-")
        phase = str(source.get("phase") or snapshot.get("phase") or "-")
        set_id = str(source.get("id") or snapshot.get("set_id") or "-")
        display_item_code = self._middle_ellipsis(item_code, 14)
        display_set_id = self._middle_ellipsis(set_id, 10)
        stage_text = self._workflow_left_stage_text(view)
        if self.__dict__.get("_operator_hide_left_badges") and "복구됨" in view.badges:
            stage_text = f"{stage_text} · 복구됨"
        updates = {
            "operator_item_stage_label": stage_text,
            "operator_item_code_label": f"현품표 {display_item_code or '-'}",
            "operator_item_name_label": f"품목 {item_name}",
            "operator_item_spec_label": f"규격 {spec}",
            "operator_item_phase_label": f"차수 {phase}",
            "operator_set_id_label": f"세트 {display_set_id}",
        }
        for name, text in updates.items():
            widget = self.__dict__.get(name)
            if widget is not None:
                try:
                    widget.configure(text=text)
                except (TclError, AttributeError):
                    pass

        membership = "일반 QA 5단계"
        if view.exact_rescan.status == "sealed":
            membership = "서버 멤버십 상속"
        elif view.exact_rescan.status == "active":
            membership = f"F4 재스캔 {view.exact_rescan.progress_text}"
        elif view.exact_rescan.status == "complete":
            membership = f"F4 완료 {view.exact_rescan.progress_text}"
        membership_label = self.__dict__.get("operator_membership_label")
        if membership_label is not None:
            try:
                membership_label.configure(text=membership)
            except (TclError, AttributeError):
                pass
        badges_label = self.__dict__.get("operator_badges_label")
        if badges_label is not None:
            try:
                if view.badges and not self.__dict__.get("_operator_hide_left_badges"):
                    badges_label.configure(text=" · ".join(view.badges))
                    badges_label.grid()
                else:
                    badges_label.grid_remove()
            except (TclError, AttributeError):
                pass

    def _render_operator_workbench(self):
        """Render the current runtime through adapter -> pure presenter."""
        if not bool(
            self.__dict__.get("operator_workbench_ready")
            or self.__dict__.get("_workflow_widgets_ready")
        ):
            return None
        source = self._workflow_view_source()
        blocking_notice = (
            self.__dict__.get("_workflow_blocking_notice")
            or self.__dict__.get("_workflow_notice")
        )
        snapshot = adapt_workflow_snapshot(
            source,
            initialized=bool(self.__dict__.get("initialized_successfully", False)),
            loading=False,
            history_readonly=not bool(
                self.__dict__.get("history_view_updates_active_state", True)
            ),
            history_loading=bool(
                self.__dict__.get("history_active_load_pending", False)
            ),
            recovered=bool(self.__dict__.get("_workflow_recovered", False)),
            completion_kind=self.__dict__.get("_workflow_completion_kind"),
            blocking_notice=blocking_notice,
            last_normal_scan_override=self.__dict__.get(
                "_workflow_last_normal_override"
            ),
            has_error=bool(self.__dict__.get("_pending_workflow_error")),
            error_message=str(self.__dict__.get("_workflow_error_message") or ""),
        )
        view = present_workflow(snapshot)
        self._last_workflow_view = view

        view_mode_label = self.__dict__.get("view_mode_label")
        if view_mode_label is not None:
            try:
                view_mode_label.grid_remove()
            except (TclError, AttributeError):
                pass

        headline = self.__dict__.get("big_display_label")
        if headline is not None:
            try:
                headline.configure(text=self._workflow_headline_text(view))
            except (TclError, AttributeError):
                pass
        progress = self.__dict__.get("progress_bar")
        if progress is not None:
            try:
                progress["value"] = view.qa_completed
            except (TclError, AttributeError, TypeError):
                try:
                    progress.configure(value=view.qa_completed)
                except (TclError, AttributeError):
                    pass
        if "step_labels" in self.__dict__:
            try:
                self._update_step_rail(view.qa_completed, error=view.current_stage == "error")
            except (TclError, AttributeError, KeyError):
                pass

        qa_tree = self.__dict__.get("qa_scan_tree")
        selected_qa_iid = self._selected_qa_scan_iid()
        qa_detail_rows = {}
        parsed_scans = tuple(source.get("parsed") or ())
        if qa_tree is not None:
            try:
                existing = tuple(qa_tree.get_children())
                if existing:
                    qa_tree.delete(*existing)
                for slot in view.slots:
                    iid = f"qa-slot-{slot.index}"
                    state_text = self._workflow_state_text(slot.state)
                    raw_value = str(slot.value or "")
                    parsed_value = str(
                        parsed_scans[slot.index - 1]
                        if slot.index <= len(parsed_scans)
                        else ""
                    )
                    summary_value = self._operator_scan_summary(
                        raw_value,
                        parsed_value,
                        slot.index,
                    )
                    display_value = self._fit_operator_tree_cell_text(
                        qa_tree,
                        "Value",
                        summary_value or "-",
                    )
                    qa_tree.insert(
                        "",
                        "end",
                        iid=iid,
                        values=(
                            f"{slot.index}. {slot.label}",
                            display_value,
                            state_text,
                        ),
                        tags=(slot.state,),
                    )
                    qa_detail_rows[iid] = {
                        "stage": f"{slot.index}. {slot.label}",
                        "state": state_text,
                        "raw": raw_value,
                        "summary": summary_value,
                    }
            except (TclError, AttributeError, TypeError):
                pass
        self._qa_scan_detail_rows = qa_detail_rows
        if selected_qa_iid not in qa_detail_rows:
            selected_qa_iid = (
                f"qa-slot-{view.qa_completed}" if view.qa_completed else None
            )
        if qa_tree is not None and selected_qa_iid:
            try:
                qa_tree.selection_set(selected_qa_iid)
                qa_tree.focus(selected_qa_iid)
                qa_tree.see(selected_qa_iid)
            except (TclError, AttributeError):
                pass
        self._render_qa_scan_detail(selected_qa_iid)
        notebook = self.__dict__.get("live_scan_notebook")
        if notebook is not None:
            try:
                notebook.tab(self.qa_scan_frame, text=f"현재 세트 {view.qa_progress_text}")
            except (TclError, AttributeError):
                pass

        exact_tree = self.__dict__.get("exact_rescan_tree")
        exact_values = tuple(source.get("exact_rescan_barcodes") or ())
        selected_exact_iid = self._selected_exact_rescan_iid()
        exact_detail_rows = {}
        if exact_tree is not None:
            try:
                existing = tuple(exact_tree.get_children())
                if existing:
                    exact_tree.delete(*existing)
                for index, value in enumerate(exact_values, 1):
                    raw_value = str(value or "")
                    iid = f"exact-slot-{index}"
                    parsed_value = str(parsed_scans[0] if parsed_scans else "")
                    summary_value = self._operator_scan_summary(
                        raw_value,
                        parsed_value,
                        LABEL_MATCH_MASTER_SCAN_POSITION + 1,
                    )
                    display_value = self._fit_operator_tree_cell_text(
                        exact_tree,
                        "Value",
                        summary_value,
                    )
                    exact_tree.insert(
                        "",
                        "end",
                        iid=iid,
                        values=(index, display_value),
                    )
                    exact_detail_rows[iid] = {
                        "order": index,
                        "raw": raw_value,
                        "summary": summary_value,
                    }
            except (TclError, AttributeError, TypeError):
                pass
        self._exact_rescan_detail_rows = exact_detail_rows
        if selected_exact_iid not in exact_detail_rows:
            selected_exact_iid = (
                f"exact-slot-{len(exact_detail_rows)}" if exact_detail_rows else None
            )
        if exact_tree is not None and selected_exact_iid:
            try:
                exact_tree.selection_set(selected_exact_iid)
                exact_tree.focus(selected_exact_iid)
                exact_tree.see(selected_exact_iid)
            except (TclError, AttributeError):
                pass
        self._render_exact_rescan_detail(selected_exact_iid)
        show_exact = view.exact_rescan.status in {"active", "complete"}
        select_exact = bool(
            view.exact_rescan.status == "active"
            or (
                view.exact_rescan.status == "complete"
                and view.qa_completed <= 1
            )
        )
        self._set_exact_rescan_tab_visible(
            show_exact,
            # Completion must keep the just-scanned F4 list and selected raw
            # value visible.  Once the next QA scan is accepted, the active QA
            # list becomes the source of truth and must regain the same center.
            select=select_exact,
        )
        if notebook is not None and show_exact:
            try:
                notebook.tab(
                    self.exact_rescan_frame,
                    text=f"F4 전체 재스캔 {view.exact_rescan.progress_text}",
                )
            except (TclError, AttributeError):
                pass

        # The central list and its selected-row detail retain the complete last
        # normal scan.  Repeating that raw value in the fixed notice row both
        # duplicates information and can force a short-screen height overflow.
        self._set_workflow_notice_ui(view.notice, view.next_action)
        last_scan_label = self.__dict__.get("operator_last_scan_label")
        if last_scan_label is None:
            last_scan_label = self.__dict__.get("status_label")
        if last_scan_label is not None:
            last_scan = view.last_normal_scan or "-"
            try:
                # The actual value already remains visible as the final filled
                # row in the central list.  Keep this compatibility label
                # updated for legacy callers, but do not duplicate it on the
                # operator surface.
                last_scan_label.configure(text=f"마지막 정상 스캔: {last_scan}")
                last_scan_label.grid_remove()
            except (TclError, AttributeError):
                pass

        operator_notebook = self.__dict__.get("operator_notebook")
        history_card = self.__dict__.get("history_card")
        if view.readonly and operator_notebook is not None and history_card is not None:
            try:
                operator_notebook.select(history_card)
            except (TclError, AttributeError):
                pass

        entry = self.__dict__.get("entry")
        if entry is not None:
            entry_enabled = bool(
                view.scan_input_enabled
                and self.__dict__.get("initialized_successfully", False)
            )
            try:
                entry.configure(state="normal" if entry_enabled else "disabled")
            except (TclError, AttributeError):
                pass
        for name, enabled in (
            ("manual_complete_button", view.f3_enabled),
            ("exact_rescan_button", view.f4_enabled),
            ("reset_button", view.cancel_current_enabled),
            ("cancel_tray_button", view.cancel_completed_enabled),
        ):
            button = self.__dict__.get(name)
            if button is not None:
                try:
                    button.configure(state="normal" if enabled else "disabled")
                except (TclError, AttributeError):
                    pass
        self._update_operator_item_panel(view, source)
        return view

    def _refresh_operator_workbench(self):
        return self._render_operator_workbench()

    def _refresh_workflow_view(self):
        return self._render_operator_workbench()

    def _handle_scan_enter(self, event=None):
        if self.__dict__.get("_pending_workflow_error") or self.__dict__.get(
            "_workflow_pending_error"
        ):
            return self._acknowledge_workflow_notice(event)
        if self.__dict__.get("operator_workbench_ready"):
            view = self._render_operator_workbench()
            if view is None or not view.scan_input_enabled:
                return "break"
        return self.process_input(event)

    def _handle_workflow_shortcut(self, action, event=None):
        """Apply the same presenter gate to keyboard and button actions."""
        action = str(action).lower()
        if self.__dict__.get("operator_workbench_ready"):
            view = self._render_operator_workbench()
        else:
            view = self.__dict__.get("_last_workflow_view")
        allowed = {
            "f1": bool(view and view.cancel_current_enabled),
            "f2": bool(view and view.cancel_completed_enabled),
            "f3": bool(view and view.f3_enabled),
            "f4": bool(view and view.f4_enabled),
        }
        if not allowed.get(str(action).lower(), False):
            return "break"
        if action == "f1":
            self._reset_current_set(full_reset=True)
        elif action == "f2":
            self._prompt_and_cancel_completed_tray()
        elif action == "f3":
            self._prompt_manual_complete()
        elif action == "f4":
            self._prompt_exact_rescan()
        return "break"

    def _handle_workflow_escape(self, event=None):
        if (
            self.__dict__.get("_pending_workflow_error")
            or self.__dict__.get("_workflow_pending_error")
            or callable(self.__dict__.get("_workflow_notice_action"))
        ):
            return self._acknowledge_workflow_notice(event)
        return None

    def _acknowledge_workflow_notice(self, event=None):
        pending = self.__dict__.get("_pending_workflow_error") or self.__dict__.get(
            "_workflow_pending_error"
        )
        if pending:
            self.is_blinking = False
            fail_sound = self.__dict__.get("sound_objects", {}).get("fail")
            if fail_sound is not None:
                try:
                    fail_sound.stop()
                except Exception:
                    pass
            self._pending_workflow_error = None
            self._workflow_pending_error = None
            self._workflow_blocking_notice = None
            self._workflow_notice = None
            result = pending.get("result")
            error_details = pending.get("error_details", "")
            if not self.current_set_info.get("id"):
                self.current_set_info["id"] = str(time.time_ns())
            self._finalize_set(result, error_details)
            return "break"
        action = self.__dict__.get("_workflow_notice_action")
        if callable(action):
            action()
            return "break"
        self._workflow_blocking_notice = None
        self._workflow_notice = None
        self._render_operator_workbench()
        return "break"

    def _present_inline_workflow_error(self, title, message, result, error_details):
        normalized_title = str(title or "입력 오류").strip("[] ") or "입력 오류"
        normalized_message = str(message or "입력을 확인해 주세요.").strip()
        self.is_blinking = True
        pending = {"result": result, "error_details": error_details}
        self._pending_workflow_error = pending
        self._workflow_pending_error = pending
        notice = WorkflowNotice(
            title=normalized_title,
            message=normalized_message,
            kind="error",
            tone="danger",
        )
        self._workflow_blocking_notice = notice
        self._workflow_notice = notice
        self._workflow_error_message = normalized_message
        self._workflow_notice_action = None
        self._workflow_notice_action_text = "확인"
        notice_label = self.__dict__.get("workflow_notice_label")
        if notice_label is not None:
            try:
                notice_label.configure(text=f"{normalized_title}: {normalized_message}")
            except (TclError, AttributeError):
                pass
        if (
            not self.__dict__.get("run_tests", False)
            and not _label_match_automated_test_mode()
            and self.__dict__.get("sound_objects", {}).get("fail") is not None
        ):
            threading.Thread(target=self._play_error_siren_loop, daemon=True).start()
        self._render_operator_workbench()
        button = self.__dict__.get("workflow_notice_action_button")
        if button is not None:
            try:
                button.focus_set()
            except (TclError, AttributeError):
                pass
        return True

    def _publish_workflow_completion(self, kind):
        normalized = str(kind or "").strip().lower()
        if normalized not in {"full", "partial", "failed"}:
            raise ValueError(f"unsupported workflow completion kind: {kind}")
        raw_scans = tuple((self.current_set_info.get("raw") or ()))
        parsed_scans = tuple((self.current_set_info.get("parsed") or ()))
        self._workflow_completion_kind = normalized
        self._workflow_display_scans = raw_scans
        self._workflow_display_parsed_scans = parsed_scans
        self._workflow_last_normal_override = raw_scans[-1] if raw_scans else ""
        self._workflow_blocking_notice = None
        self._workflow_notice = None
        self._workflow_notice_action = None
        self._workflow_recovered = False
        item_code = str(parsed_scans[0] if parsed_scans else "")
        item_info = self.__dict__.get("items_data", {}).get(item_code, {}) if item_code else {}
        self._workflow_item_snapshot = {
            "item_code": item_code,
            "item_name_override": self.current_set_info.get("item_name_override"),
            "spec": item_info.get("Spec", ""),
            "phase": self.current_set_info.get("phase"),
            "set_id": self.current_set_info.get("id"),
        }
        self._render_operator_workbench()
        return normalized

    def _publish_finalize_completion(self, *, is_manual_complete=False, result=None):
        if result is not None and result != self.Results.PASS:
            kind = "failed"
        else:
            kind = "partial" if is_manual_complete else "full"
        return self._publish_workflow_completion(kind)

    def _clear_workflow_completion(self):
        self._workflow_completion_kind = None
        self._workflow_display_scans = ()
        self._workflow_display_parsed_scans = ()
        self._workflow_last_normal_override = None
        self._workflow_item_snapshot = None

    def _publish_submission_block(self, error):
        message = f"오류: {error}"
        notice = WorkflowNotice(
            title="중앙 제출 차단 · 5/5 유지",
            message=message,
            kind="submission_blocked",
            tone="danger",
        )
        self._workflow_blocking_notice = notice
        self._workflow_notice = notice
        self._workflow_notice_action = self._retry_blocked_submission
        self._workflow_notice_action_text = "제출 재시도"
        scans = tuple(self.current_set_info.get("raw") or ())
        self._workflow_last_normal_override = scans[-1] if scans else ""
        notice_label = self.__dict__.get("workflow_notice_label")
        if notice_label is not None:
            try:
                notice_label.configure(text=message)
            except (TclError, AttributeError):
                pass
        self._render_operator_workbench()
        return False

    def _retry_blocked_submission(self):
        self._workflow_blocking_notice = None
        self._workflow_notice = None
        self._workflow_notice_action = None
        self._workflow_notice_action_text = "확인"
        self._finalize_set(self.Results.PASS, "")
        return True

    def _refresh_session_tree(self):
        session_tree = self.__dict__.get("session_tree")
        history_tree = self.__dict__.get("history_tree")
        if session_tree is None or history_tree is None:
            return
        try:
            existing = tuple(session_tree.get_children())
            if existing:
                session_tree.delete(*existing)
            children = tuple(history_tree.get_children())
            for iid in children[-30:]:
                if str(iid) == "loading":
                    continue
                values = list(history_tree.item(iid, "values") or ())
                if len(values) < self.TOTAL_SCAN_COUNT + 3:
                    continue
                item = values[1]
                result = values[1 + self.TOTAL_SCAN_COUNT]
                timestamp = values[2 + self.TOTAL_SCAN_COUNT]
                session_tree.insert(
                    "",
                    "end",
                    iid=f"session-{iid}",
                    values=(timestamp, item, result),
                )
        except (TclError, AttributeError, TypeError):
            pass

    def _create_widgets(self):
        """Create the work-focused three-column operator surface.

        The legacy history widgets and their data contracts remain intact,
        but they now live in right-side tabs so the active five-scan set stays
        visible throughout normal, error, completion, and recovery states.
        """
        profile = getattr(self, "ui_profile", self.UI_PROFILES["standard"])
        outer_padding = int(profile["outer_padding"])
        try:
            # ``winfo_screenwidth`` can be the combined virtual desktop on a
            # multi-monitor station.  Capping the bootstrap size prevents that
            # value from becoming a permanent multi-thousand-pixel Treeview
            # requisition before the first real <Configure> event.
            current_width = int(self.winfo_width())
            bootstrap_width = current_width if current_width > 100 else min(
                1920, int(self.winfo_screenwidth())
            )
            initial_width = max(980, bootstrap_width - outer_padding * 2)
            initial_height = max(640, int(self.winfo_height()) - outer_padding * 2 - 70)
        except (TclError, AttributeError, RecursionError, TypeError, ValueError):
            initial_width, initial_height = 1440, 830
        operator_scale = float(self.__dict__.get("scale_factor", 1.0) or 1.0)
        self.operator_layout_metrics = build_operator_layout(
            initial_width,
            initial_height,
            operator_scale,
        )
        self.operator_style_tokens = build_style_tokens(
            self.operator_layout_metrics.profile.name,
            operator_scale,
        )

        self.main_frame = ttk.Frame(self, padding=outer_padding)
        main_frame = self.main_frame
        main_frame.pack(fill=tk.BOTH, expand=True)
        main_frame.grid_rowconfigure(1, weight=1)
        main_frame.grid_columnconfigure(0, weight=1)

        # Header: context and low-frequency controls stay out of the scan path.
        self.operator_header_frame = ttk.Frame(main_frame, style="Card.TFrame", padding=(14, 8))
        self.operator_header_frame.grid(row=0, column=0, sticky="ew", pady=(0, profile["section_gap"]))
        self.operator_header_frame.grid_columnconfigure(1, weight=1)
        self.operator_title_label = ttk.Label(
            self.operator_header_frame,
            text="Label Match · 포장 라벨 검증",
            style="Header.TLabel",
        )
        self.operator_title_label.grid(row=0, column=0, sticky="w")
        self.operator_header_context_label = ttk.Label(
            self.operator_header_frame,
            text=(
                f"작업자 {self.__dict__.get('worker_name', '-')}  ·  "
                f"{self.__dict__.get('unique_id', '-')}"
            ),
            style="Status.TLabel",
        )
        self.operator_header_context_label.grid(row=0, column=1, sticky="e", padx=(12, 16))
        self.top_right_frame = ttk.Frame(self.operator_header_frame, style="Borderless.TFrame")
        self.top_right_frame.grid(row=0, column=2, sticky="e")
        self.clock_label = ttk.Label(self.top_right_frame, text="", style="Status.TLabel")
        self.clock_label.pack(side=tk.LEFT, padx=(0, 12))
        self.settings_button = ttk.Button(
            self.top_right_frame,
            text="설정",
            command=self.open_settings_window,
            style="Control.TButton",
        )
        self.settings_button.pack(side=tk.LEFT, padx=(0, 6))
        self.about_button = ttk.Button(
            self.top_right_frame,
            text="정보",
            command=self._show_about_window,
            style="Control.TButton",
        )
        self.about_button.pack(side=tk.LEFT)

        # Persistent three-column desk.
        self.operator_workbench_frame = ttk.Frame(main_frame)
        self.operator_workbench_frame.grid(row=1, column=0, sticky="nsew")
        self.operator_workbench_frame.grid_rowconfigure(0, weight=1)
        self.workbench_frame = self.operator_workbench_frame

        panes = self.operator_layout_metrics.panes
        self.operator_workbench_frame.grid_columnconfigure(0, minsize=panes.left_width)
        self.operator_workbench_frame.grid_columnconfigure(1, weight=1, minsize=panes.center_width)
        self.operator_workbench_frame.grid_columnconfigure(2, minsize=panes.right_width)

        self.operator_left_pane = ttk.Frame(
            self.operator_workbench_frame,
            style="Card.TFrame",
            padding=profile["card_padding"],
        )
        self.operator_left_pane.grid(row=0, column=0, sticky="nsew")
        self.left_context_card = self.operator_left_pane
        self.operator_left_pane.grid_columnconfigure(0, weight=1)
        self.operator_left_heading_label = ttk.Label(
            self.operator_left_pane,
            text="현재 작업",
            style="Header.TLabel",
        )
        self.operator_left_heading_label.grid(
            row=0, column=0, sticky="w", pady=(0, 12)
        )
        self.operator_item_stage_label = ttk.Label(
            self.operator_left_pane,
            text="현품표 대기",
            style="Success.TLabel",
            wraplength=max(150, panes.left_width - 40),
        )
        self.operator_item_stage_label.grid(row=1, column=0, sticky="ew", pady=(0, 12))

        self.operator_item_card = ttk.Frame(self.operator_left_pane, style="Borderless.TFrame")
        self.operator_item_card.grid(row=2, column=0, sticky="ew")
        self.operator_item_card.grid_columnconfigure(0, weight=1)
        self.operator_item_code_label = ttk.Label(
            self.operator_item_card,
            text="현품표 -",
            style="Header.TLabel",
            wraplength=max(150, panes.left_width - 44),
        )
        self.operator_item_code_label.grid(row=0, column=0, sticky="w")
        self.operator_item_name_label = ttk.Label(
            self.operator_item_card,
            text="품목 -",
            style="Status.TLabel",
            wraplength=max(150, panes.left_width - 44),
        )
        self.operator_item_name_label.grid(row=1, column=0, sticky="w", pady=(8, 0))
        self.operator_item_spec_label = ttk.Label(
            self.operator_item_card,
            text="규격 -",
            style="Status.TLabel",
            wraplength=max(150, panes.left_width - 44),
        )
        self.operator_item_spec_label.grid(row=2, column=0, sticky="w", pady=(5, 0))
        self.operator_item_phase_label = ttk.Label(
            self.operator_item_card,
            text="차수 -",
            style="Status.TLabel",
        )
        self.operator_item_phase_label.grid(row=3, column=0, sticky="w", pady=(5, 0))
        self.operator_set_id_label = ttk.Label(
            self.operator_item_card,
            text="세트 -",
            style="Status.TLabel",
            wraplength=max(150, panes.left_width - 44),
        )
        self.operator_set_id_label.grid(row=4, column=0, sticky="w", pady=(5, 0))

        self.operator_left_divider = ttk.Frame(
            self.operator_left_pane,
            style="TFrame",
            height=1,
        )
        self.operator_left_divider.grid(row=3, column=0, sticky="ew", pady=16)
        self.operator_membership_heading_label = ttk.Label(
            self.operator_left_pane,
            text="작업 상태",
            style="Status.TLabel",
        )
        self.operator_membership_heading_label.grid(row=4, column=0, sticky="w")
        self.operator_membership_label = ttk.Label(
            self.operator_left_pane,
            text="일반 QA 5단계",
            style="Header.TLabel",
            wraplength=max(150, panes.left_width - 40),
        )
        self.operator_membership_label.grid(row=5, column=0, sticky="ew", pady=(6, 0))
        self.operator_badges_label = ttk.Label(
            self.operator_left_pane,
            text="",
            style="ViewMode.TLabel",
            wraplength=max(150, panes.left_width - 40),
        )
        self.operator_badges_label.grid(row=6, column=0, sticky="ew", pady=(12, 0))
        self.operator_badges_label.grid_remove()
        self.operator_left_hint_label = ttk.Label(
            self.operator_left_pane,
            text="F3은 소량 예외, F4는 QA 5단계와 별도의 전체 재스캔입니다.",
            style="Status.TLabel",
            wraplength=max(150, panes.left_width - 40),
            justify=tk.LEFT,
        )
        self.operator_left_hint_label.grid(row=7, column=0, sticky="sew", pady=(18, 0))
        self.operator_left_pane.grid_rowconfigure(7, weight=1)

        self.operator_center_pane = ttk.Frame(
            self.operator_workbench_frame,
            style="Card.TFrame",
            padding=profile["card_padding"],
        )
        self.operator_center_pane.grid(
            row=0,
            column=1,
            sticky="nsew",
            padx=(panes.gap, panes.gap),
        )
        self.top_card = self.operator_center_pane
        self.operator_center_pane.grid_columnconfigure(0, weight=1)
        self.operator_center_pane.grid_rowconfigure(4, weight=1)

        self.big_display_label = ttk.Label(
            self.operator_center_pane,
            text=self._idle_instruction_text(),
            anchor="center",
            justify=tk.CENTER,
            wraplength=max(420, panes.center_width - 40),
            font=(self.default_font_name, 34, "bold"),
        )
        self.big_display_label.grid(row=0, column=0, sticky="ew", pady=(0, 8))

        self.progress_frame = ttk.Frame(self.operator_center_pane, style="Borderless.TFrame")
        self.progress_frame.grid(row=1, column=0, sticky="ew", pady=(0, 8))
        self.progress_frame.grid_columnconfigure(0, weight=1)
        self.step_rail_frame = ttk.Frame(self.progress_frame, style="Borderless.TFrame")
        self.step_rail_frame.grid(row=0, column=0, sticky="ew")
        self.step_labels = []
        for index, step_name in enumerate(self.STEP_NAMES):
            self.step_rail_frame.grid_columnconfigure(index, weight=1, uniform="scan_steps")
            step_label = tk.Label(
                self.step_rail_frame,
                text=f"{index + 1}. {step_name}",
                font=(self.default_font_name, 11, "bold"),
                padx=6,
                pady=5,
                bd=1,
                relief="solid",
                anchor="center",
            )
            step_label.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            self.step_labels.append(step_label)
        self.progress_bar = ttk.Progressbar(
            self.progress_frame,
            orient="horizontal",
            mode="determinate",
            maximum=self.TOTAL_SCAN_COUNT,
            style="green.Horizontal.TProgressbar",
        )
        self.progress_bar.grid(row=1, column=0, sticky="ew", pady=(6, 0))

        # Exactly one notice location; neutral guidance uses the same region.
        self.workflow_notice_frame = tk.Frame(
            self.operator_center_pane,
            bg="#EFF6FF",
            highlightbackground=self.colors["primary"],
            highlightthickness=1,
            bd=0,
        )
        self.workflow_notice_frame.grid(row=2, column=0, sticky="ew", pady=(0, 8))
        self.workflow_notice_frame.grid_columnconfigure(0, weight=1)
        self.workflow_notice_title_label = tk.Label(
            self.workflow_notice_frame,
            text="스캐너 준비",
            bg="#EFF6FF",
            fg=self.colors["primary"],
            font=(self.default_font_name, 12, "bold"),
            anchor="w",
        )
        self.workflow_notice_title_label.grid(row=0, column=0, sticky="ew", padx=12, pady=(7, 0))
        self.workflow_notice_label = tk.Label(
            self.workflow_notice_frame,
            text="현품표를 스캔하세요.",
            bg="#EFF6FF",
            fg=self.colors["text"],
            font=(self.default_font_name, 11),
            anchor="w",
            justify=tk.LEFT,
            wraplength=max(360, panes.center_width - 170),
        )
        self.workflow_notice_label.grid(row=1, column=0, sticky="ew", padx=12, pady=(1, 7))
        self.workflow_notice_action_button = ttk.Button(
            self.workflow_notice_frame,
            text="확인",
            command=self._acknowledge_workflow_notice,
            style="Action.TButton",
        )
        self.workflow_notice_action_button.grid(row=0, column=1, rowspan=2, sticky="e", padx=10, pady=7)
        self.workflow_notice_action_button.grid_remove()
        self.workflow_notice_action_button.bind(
            "<Return>", self._acknowledge_workflow_notice
        )
        self.workflow_notice_action_button.bind(
            "<KP_Enter>", self._acknowledge_workflow_notice
        )
        self.view_mode_label = ttk.Label(
            self.operator_center_pane,
            text="",
            style="ViewMode.TLabel",
            anchor="center",
        )
        self.view_mode_label.grid(row=2, column=0, sticky="ew")
        self.view_mode_label.grid_remove()

        input_frame = ttk.Frame(self.operator_center_pane, style="Borderless.TFrame")
        self.operator_input_frame = input_frame
        input_frame.grid(row=3, column=0, sticky="ew", pady=(0, 8))
        input_frame.grid_columnconfigure(1, weight=1)
        self.operator_scan_input_label = ttk.Label(
            input_frame,
            text="스캔 입력",
            style="Header.TLabel",
        )
        self.operator_scan_input_label.grid(
            row=0, column=0, padx=(0, 12), sticky="w"
        )
        self.entry = ttk.Entry(
            input_frame,
            style="TEntry",
            state="disabled",
            font=(self.default_font_name, 18),
        )
        self.entry.grid(row=0, column=1, sticky="ew", ipady=8)
        self.entry.bind("<Return>", self._handle_scan_enter)

        self.live_scan_notebook = ttk.Notebook(self.operator_center_pane)
        self.live_scan_notebook.grid(row=4, column=0, sticky="nsew")
        self.qa_scan_frame = ttk.Frame(self.live_scan_notebook, style="Card.TFrame")
        self.qa_scan_frame.grid_rowconfigure(0, weight=1)
        self.qa_scan_frame.grid_columnconfigure(0, weight=1)
        self.live_scan_notebook.add(self.qa_scan_frame, text="현재 세트 0/5")
        self.qa_scan_tree = ttk.Treeview(
            self.qa_scan_frame,
            columns=("Stage", "Value", "State"),
            show="headings",
            selectmode="browse",
            height=5,
            takefocus=True,
        )
        self.qa_scan_tree.heading("Stage", text="단계", anchor="center")
        self.qa_scan_tree.heading("Value", text="실제 스캔 값", anchor="w")
        self.qa_scan_tree.heading("State", text="상태", anchor="center")
        self.qa_scan_tree.column("Stage", width=115, minwidth=90, stretch=False, anchor="center")
        self.qa_scan_tree.column("Value", width=max(280, panes.center_width - 260), minwidth=180, stretch=True, anchor="w")
        self.qa_scan_tree.column("State", width=90, minwidth=72, stretch=False, anchor="center")
        self.qa_scan_tree.grid(row=0, column=0, sticky="nsew")
        self.qa_scan_tree.bind(
            "<<TreeviewSelect>>",
            self._on_qa_scan_selection_changed,
        )
        self.current_set_tree = self.qa_scan_tree

        self.qa_scan_detail_frame = ttk.Frame(
            self.qa_scan_frame,
            style="Borderless.TFrame",
            padding=(6, 5, 6, 4),
        )
        self.qa_scan_detail_frame.grid(
            row=1,
            column=0,
            sticky="nsew",
            pady=(4, 0),
        )
        self.qa_scan_detail_frame.grid_columnconfigure(1, weight=1)
        self.qa_scan_detail_frame.grid_rowconfigure(1, weight=1)
        self.qa_scan_detail_title_label = ttk.Label(
            self.qa_scan_detail_frame,
            text="선택 행 원문",
            style="Header.TLabel",
        )
        self.qa_scan_detail_title_label.grid(row=0, column=0, sticky="w")
        self.qa_scan_detail_metadata_label = ttk.Label(
            self.qa_scan_detail_frame,
            text="단계: -  |  상태: -",
            style="Status.TLabel",
            anchor="w",
        )
        self.qa_scan_detail_metadata_label.grid(
            row=0,
            column=1,
            columnspan=2,
            sticky="ew",
            padx=(12, 0),
        )
        self.qa_scan_detail_text = tk.Text(
            self.qa_scan_detail_frame,
            height=2,
            wrap="char",
            font=("Consolas", 10),
            bg=self.colors["card_background"],
            fg=self.colors["text"],
            relief="solid",
            bd=1,
            padx=6,
            pady=3,
            takefocus=0,
        )
        self.qa_scan_detail_scrollbar = ttk.Scrollbar(
            self.qa_scan_detail_frame,
            orient=tk.VERTICAL,
            command=self.qa_scan_detail_text.yview,
        )
        self.qa_scan_detail_text.configure(
            yscrollcommand=self.qa_scan_detail_scrollbar.set,
        )
        self.qa_scan_detail_text.grid(
            row=1,
            column=0,
            columnspan=2,
            sticky="nsew",
            pady=(3, 0),
        )
        self.qa_scan_detail_scrollbar.grid(
            row=1,
            column=2,
            sticky="ns",
            pady=(3, 0),
        )
        self.qa_scan_detail_text.insert(
            "1.0",
            "현재 세트 행을 선택하면 수락된 스캔 원문을 확인할 수 있습니다.",
        )
        self.qa_scan_detail_text.configure(state="disabled")

        self.exact_rescan_frame = ttk.Frame(self.live_scan_notebook, style="Card.TFrame")
        self.exact_rescan_frame.grid_rowconfigure(0, weight=1)
        self.exact_rescan_frame.grid_columnconfigure(0, weight=1)
        self.live_scan_notebook.add(self.exact_rescan_frame, text="F4 전체 재스캔")
        self.exact_rescan_tree = ttk.Treeview(
            self.exact_rescan_frame,
            columns=("Order", "Value"),
            show="headings",
            selectmode="browse",
        )
        self.exact_rescan_tree.heading("Order", text="순서", anchor="center")
        self.exact_rescan_tree.heading("Value", text="실제 F4 재스캔 값", anchor="w")
        self.exact_rescan_tree.column("Order", width=80, minwidth=60, stretch=False, anchor="center")
        self.exact_rescan_tree.column("Value", width=max(320, panes.center_width - 150), minwidth=220, stretch=True, anchor="w")
        self.exact_rescan_tree.grid(row=0, column=0, sticky="nsew")
        self.exact_rescan_tree.bind(
            "<<TreeviewSelect>>",
            self._on_exact_rescan_selection_changed,
        )
        self.exact_rescan_detail_frame = ttk.Frame(
            self.exact_rescan_frame,
            style="Borderless.TFrame",
            padding=(6, 5, 6, 4),
        )
        self.exact_rescan_detail_frame.grid(
            row=1,
            column=0,
            sticky="nsew",
            pady=(4, 0),
        )
        self.exact_rescan_detail_frame.grid_columnconfigure(1, weight=1)
        self.exact_rescan_detail_frame.grid_rowconfigure(1, weight=1)
        self.exact_rescan_detail_title_label = ttk.Label(
            self.exact_rescan_detail_frame,
            text="선택 F4 원문",
            style="Header.TLabel",
        )
        self.exact_rescan_detail_title_label.grid(row=0, column=0, sticky="w")
        self.exact_rescan_detail_metadata_label = ttk.Label(
            self.exact_rescan_detail_frame,
            text="순서: -",
            style="Status.TLabel",
            anchor="w",
        )
        self.exact_rescan_detail_metadata_label.grid(
            row=0,
            column=1,
            columnspan=2,
            sticky="ew",
            padx=(12, 0),
        )
        self.exact_rescan_detail_text = tk.Text(
            self.exact_rescan_detail_frame,
            height=2,
            wrap="char",
            font=("Consolas", 10),
            bg=self.colors["card_background"],
            fg=self.colors["text"],
            relief="solid",
            bd=1,
            padx=6,
            pady=3,
            takefocus=0,
        )
        self.exact_rescan_detail_scrollbar = ttk.Scrollbar(
            self.exact_rescan_detail_frame,
            orient=tk.VERTICAL,
            command=self.exact_rescan_detail_text.yview,
        )
        self.exact_rescan_detail_text.configure(
            yscrollcommand=self.exact_rescan_detail_scrollbar.set,
        )
        self.exact_rescan_detail_text.grid(
            row=1,
            column=0,
            columnspan=2,
            sticky="nsew",
            pady=(3, 0),
        )
        self.exact_rescan_detail_scrollbar.grid(
            row=1,
            column=2,
            sticky="ns",
            pady=(3, 0),
        )
        self.exact_rescan_detail_text.insert(
            "1.0",
            "F4 재스캔 행을 선택하면 전체 원문을 확인할 수 있습니다.",
        )
        self.exact_rescan_detail_text.configure(state="disabled")
        hide_exact_tab = getattr(self.live_scan_notebook, "hide", None)
        if callable(hide_exact_tab):
            hide_exact_tab(self.exact_rescan_frame)
        self.operator_last_scan_label = ttk.Label(
            self.operator_center_pane,
            text="마지막 정상 스캔: -",
            style="Status.TLabel",
            anchor="w",
            wraplength=max(380, panes.center_width - 30),
        )
        self.operator_last_scan_label.grid(row=5, column=0, sticky="ew", pady=(8, 0))
        self.status_label = self.operator_last_scan_label

        self.operator_right_pane = ttk.Frame(
            self.operator_workbench_frame,
            style="Card.TFrame",
            padding=profile["card_padding"],
        )
        self.operator_right_pane.grid(row=0, column=2, sticky="nsew")
        self.right_activity_card = self.operator_right_pane
        self.operator_right_pane.grid_rowconfigure(0, weight=1)
        self.operator_right_pane.grid_columnconfigure(0, weight=1)
        self.operator_notebook = ttk.Notebook(self.operator_right_pane)
        self.operator_notebook.grid(row=0, column=0, sticky="nsew")
        self.operator_history_notebook = self.operator_notebook

        self.session_tab = ttk.Frame(self.operator_notebook, style="Card.TFrame", padding=8)
        self.operator_session_tab = self.session_tab
        self.session_tab.grid_rowconfigure(1, weight=1)
        self.session_tab.grid_columnconfigure(0, weight=1)
        self.operator_notebook.add(self.session_tab, text="이번 세션")
        self.operator_session_heading_label = ttk.Label(
            self.session_tab,
            text="최근 완료",
            style="Header.TLabel",
        )
        self.operator_session_heading_label.grid(
            row=0, column=0, sticky="w", pady=(0, 6)
        )
        self.session_tree = ttk.Treeview(
            self.session_tab,
            columns=("Time", "Item", "Result"),
            show="headings",
            selectmode="browse",
        )
        for column, text, width in (
            ("Time", "시각", 68),
            ("Item", "현품표", 185),
            ("Result", "결과", 72),
        ):
            self.session_tree.heading(column, text=text, anchor="center")
            self.session_tree.column(column, width=width, minwidth=55, stretch=(column == "Item"), anchor="center")
        self.session_tree.grid(row=1, column=0, sticky="nsew")

        self.history_card = ttk.Frame(self.operator_notebook, style="Card.TFrame", padding=8)
        history_card = self.history_card
        self.history_tab = history_card
        self.operator_history_tab = history_card
        self.operator_notebook.add(history_card, text="스캔 기록")
        history_card.grid_rowconfigure(1, weight=1)
        history_card.grid_columnconfigure(0, weight=1)
        self.hist_header_frame = ttk.Frame(history_card, style="Borderless.TFrame")
        self.hist_header_frame.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        self.hist_header_frame.grid_columnconfigure(1, weight=1)
        self._history_header_full_text = "스캔 기록"
        self.hist_header_label = ttk.Label(
            self.hist_header_frame,
            text=self._history_header_full_text,
            style="Header.TLabel",
        )
        self.hist_header_label.grid(row=0, column=0, sticky="w")
        self.hist_control_frame = ttk.Frame(self.hist_header_frame, style="Borderless.TFrame")
        self.hist_control_frame.grid(row=0, column=2, sticky="e")
        self.today_button = ttk.Button(
            self.hist_control_frame,
            text="오늘",
            style="Control.TButton",
            command=self._reload_today_history,
        )
        self.today_button.pack(side=tk.LEFT, padx=(0, 4))
        self.date_search_button = ttk.Button(
            self.hist_control_frame,
            text="조회",
            style="Control.TButton",
            command=self._prompt_for_date_and_reload,
        )
        self.date_search_button.pack(side=tk.LEFT, padx=(0, 4))
        self.decrease_font_button = ttk.Button(
            self.hist_control_frame,
            text="-",
            style="Control.TButton",
            command=self._decrease_tree_font,
        )
        self.decrease_font_button.pack(side=tk.LEFT)
        self.increase_font_button = ttk.Button(
            self.hist_control_frame,
            text="+",
            style="Control.TButton",
            command=self._increase_tree_font,
        )
        self.increase_font_button.pack(side=tk.LEFT)
        tree_frame_hist = ttk.Frame(history_card, style="Card.TFrame")
        tree_frame_hist.grid(row=1, column=0, sticky="nsew")
        tree_frame_hist.grid_rowconfigure(0, weight=1)
        tree_frame_hist.grid_columnconfigure(0, weight=1)
        hist_cols = list(self.hist_proportions.keys())
        v_scroll_hist = ttk.Scrollbar(tree_frame_hist, orient=tk.VERTICAL)
        h_scroll_hist = ttk.Scrollbar(tree_frame_hist, orient=tk.HORIZONTAL)
        self.history_tree = ttk.Treeview(
            tree_frame_hist,
            columns=hist_cols,
            displaycolumns=("Set", "Input1", "Result", "Timestamp"),
            show="headings",
            yscrollcommand=v_scroll_hist.set,
            xscrollcommand=h_scroll_hist.set,
            selectmode="extended",
        )
        v_scroll_hist.config(command=self.history_tree.yview)
        h_scroll_hist.config(command=self.history_tree.xview)
        for col, labels in self.HISTORY_HEADING_LABELS.items():
            self.history_tree.heading(
                col,
                text=labels[0],
                anchor="center",
                command=lambda c=col: self._treeview_sort_column(self.history_tree, c, False),
            )
            self.history_tree.column(col, anchor="center", minwidth=60, stretch=False)
        v_scroll_hist.grid(row=0, column=1, sticky="ns")
        h_scroll_hist.grid(row=1, column=0, sticky="ew")
        self.history_tree.grid(row=0, column=0, sticky="nsew")
        self.history_tree.bind("<Configure>", self._resize_all_columns)
        self.history_tree.bind("<ButtonRelease-1>", self._on_history_tree_resize_release)
        self.history_tree.bind("<<TreeviewSelect>>", self._on_history_selection_changed)
        self.history_tree.bind("<Double-1>", self._show_selected_history_detail_window)

        self.history_detail_frame = ttk.Frame(history_card, style="Borderless.TFrame")
        self.history_detail_frame.grid(row=2, column=0, sticky="ew", pady=(6, 0))
        self.history_detail_frame.grid_columnconfigure(0, weight=1)
        detail_header_frame = ttk.Frame(self.history_detail_frame, style="Borderless.TFrame")
        detail_header_frame.grid(row=0, column=0, sticky="ew")
        detail_header_frame.grid_columnconfigure(0, weight=1)
        self.history_detail_modal_button = ttk.Button(
            detail_header_frame,
            text="원문",
            style="Control.TButton",
            command=self._show_selected_history_detail_window,
            state="disabled",
        )
        self.history_detail_modal_button.grid(row=0, column=1, sticky="e")
        self.history_detail_copy_button = ttk.Button(
            detail_header_frame,
            text="복사",
            style="Control.TButton",
            command=self._copy_selected_history_barcodes,
            state="disabled",
        )
        self.history_detail_copy_button.grid(row=0, column=2, sticky="e", padx=(4, 0))
        self.history_detail_text = tk.Text(
            self.history_detail_frame,
            height=3,
            wrap="word",
            font=("Consolas", 9),
            bg=self.colors["card_background"],
            fg=self.colors["text"],
            relief="solid",
            bd=1,
            padx=6,
            pady=4,
        )
        self.history_detail_text.grid(row=1, column=0, columnspan=3, sticky="ew", pady=(4, 0))
        self.history_detail_text.insert("1.0", "기록을 선택하면 스캔 원문을 확인할 수 있습니다.")
        self.history_detail_text.configure(state="disabled")

        self.history_context_menu = tk.Menu(self, tearoff=0, font=(self.default_font_name, 14))
        self.history_context_menu.add_command(label="바코드 원문 보기", command=self._show_selected_history_detail_window)
        self.history_context_menu.add_command(label="바코드 원문 복사", command=self._copy_selected_history_barcodes)
        self.history_context_menu.add_separator()
        self.history_context_menu.add_command(label=self.HISTORY_DELETE_ACTION_TEXT, command=self._delete_selected_row)
        self.history_tree.bind("<Button-3>", self._show_history_context_menu)

        self.summary_card = ttk.Frame(self.operator_notebook, style="Card.TFrame", padding=8)
        summary_card = self.summary_card
        self.summary_tab = summary_card
        self.operator_summary_tab = summary_card
        self.operator_notebook.add(summary_card, text="통과 요약")
        summary_card.grid_rowconfigure(1, weight=1)
        summary_card.grid_columnconfigure(0, weight=1)
        self.summary_header_frame = ttk.Frame(summary_card, style="Borderless.TFrame")
        self.summary_header_frame.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        self.summary_header_frame.grid_columnconfigure(0, weight=1)
        self.summary_header_label = ttk.Label(
            self.summary_header_frame,
            text="누적 통과 코드",
            style="Header.TLabel",
        )
        self.summary_header_label.grid(row=0, column=0, sticky="w")
        self.summary_date_label = ttk.Label(
            self.summary_header_frame,
            text="날짜 -",
            style="SummaryDate.TLabel",
            anchor="center",
        )
        self.summary_date_label.grid(row=0, column=1, sticky="e")
        tree_frame_sum = ttk.Frame(summary_card, style="Card.TFrame")
        tree_frame_sum.grid(row=1, column=0, sticky="nsew")
        tree_frame_sum.grid_rowconfigure(0, weight=1)
        tree_frame_sum.grid_columnconfigure(0, weight=1)
        summary_cols = list(self.summary_proportions.keys())
        v_scroll_sum = ttk.Scrollbar(tree_frame_sum, orient=tk.VERTICAL)
        self.summary_tree = ttk.Treeview(
            tree_frame_sum,
            columns=summary_cols,
            show="headings",
            yscrollcommand=v_scroll_sum.set,
        )
        v_scroll_sum.config(command=self.summary_tree.yview)
        for column in summary_cols:
            self.summary_tree.heading(
                column,
                text=self.SUMMARY_HEADING_LABELS[column][0],
                anchor="center",
                command=lambda c=column: self._treeview_sort_column(self.summary_tree, c, False),
            )
        self.summary_tree.column("Code", anchor="w", minwidth=150, stretch=True)
        self.summary_tree.column("Phase", anchor="center", minwidth=55, stretch=False)
        self.summary_tree.column("Count", anchor="center", minwidth=60, stretch=False)
        self.summary_tree.grid(row=0, column=0, sticky="nsew")
        v_scroll_sum.grid(row=0, column=1, sticky="ns")
        self.summary_tree.bind("<Configure>", self._resize_all_columns)
        self.summary_tree.bind("<ButtonRelease-1>", self._on_summary_tree_resize_release)

        self.operator_action_frame = ttk.Frame(self.operator_right_pane)
        self.operator_action_frame.grid(row=1, column=0, sticky="ew", pady=(10, 0))
        self.operator_action_frame.grid_columnconfigure((0, 1), weight=1, uniform="operator_actions")
        self.bottom_frame = self.operator_action_frame
        self.manual_complete_button = ttk.Button(
            self.operator_action_frame,
            text=self.MANUAL_COMPLETE_BUTTON_TEXT,
            command=self._prompt_manual_complete,
            style=self.MANUAL_COMPLETE_BUTTON_STYLE,
            state="disabled",
        )
        self.manual_complete_button.grid(row=0, column=0, sticky="nsew", padx=(0, 4), pady=(0, 4))
        self.exact_rescan_button = ttk.Button(
            self.operator_action_frame,
            text=self.EXACT_RESCAN_BUTTON_TEXT,
            command=self._prompt_exact_rescan,
            style=self.MANUAL_COMPLETE_BUTTON_STYLE,
            state="disabled",
        )
        self.exact_rescan_button.grid(row=0, column=1, sticky="nsew", padx=(4, 0), pady=(0, 4))
        self.reset_button = ttk.Button(
            self.operator_action_frame,
            text=self.CURRENT_SET_CANCEL_BUTTON_TEXT,
            command=lambda: self._reset_current_set(full_reset=True),
            style=self.CURRENT_SET_CANCEL_BUTTON_STYLE,
        )
        self.reset_button.grid(row=1, column=0, sticky="nsew", padx=(0, 4), pady=(4, 0))
        self.cancel_tray_button = ttk.Button(
            self.operator_action_frame,
            text=self.COMPLETED_TRAY_CANCEL_BUTTON_TEXT,
            command=self._prompt_and_cancel_completed_tray,
            style=self.COMPLETED_TRAY_CANCEL_BUTTON_STYLE,
        )
        self.cancel_tray_button.grid(row=1, column=1, sticky="nsew", padx=(4, 0), pady=(4, 0))

        self.bind("<F1>", lambda event: self._handle_workflow_shortcut("f1", event))
        self.bind("<F2>", lambda event: self._handle_workflow_shortcut("f2", event))
        self.bind("<F3>", lambda event: self._handle_workflow_shortcut("f3", event))
        self.bind("<F4>", lambda event: self._handle_workflow_shortcut("f4", event))
        self.bind("<Escape>", self._handle_workflow_escape)
        self.bind("<Delete>", self._delete_selected_row_from_shortcut)

        self.operator_status_frame = ttk.Frame(main_frame)
        self.operator_status_frame.grid(row=2, column=0, sticky="ew", pady=(profile["bottom_gap"], 0))
        self.operator_status_frame.grid_columnconfigure(0, weight=1)
        self.save_status_label = ttk.Label(
            self.operator_status_frame,
            text="",
            style="Save.Success.TLabel",
            background=self.colors["background"],
        )
        self.save_status_label.grid(row=0, column=0, sticky="w")
        self.operator_footer_label = ttk.Label(
            self.operator_status_frame,
            text="상태는 문구와 색상으로 함께 표시됩니다.",
            style="Status.TLabel",
        )
        self.operator_footer_label.grid(row=0, column=1, sticky="e")

        self.loading_overlay = ttk.Frame(main_frame, style="Overlay.TFrame")
        loading_content_frame = ttk.Frame(self.loading_overlay, style="Overlay.TFrame")
        loading_content_frame.pack(expand=True)
        ttk.Label(
            loading_content_frame,
            text="데이터를 불러오는 중입니다...",
            style="Loading.TLabel",
        ).pack(pady=(0, 15))
        self.loading_progressbar = ttk.Progressbar(loading_content_frame, mode="indeterminate", length=400)
        self.loading_progressbar.pack(pady=15)

        self._workflow_widgets_ready = True
        self.operator_workbench_ready = True
        self._update_step_rail(0)
        self._render_operator_workbench()
        self._apply_responsive_layout()

    def _create_legacy_widgets(self):
        profile = getattr(self, "ui_profile", self.UI_PROFILES["standard"])
        self.main_frame = ttk.Frame(self, padding=profile["outer_padding"])
        main_frame = self.main_frame
        main_frame.pack(fill=tk.BOTH, expand=True)
        main_frame.grid_rowconfigure(1, weight=1)
        main_frame.grid_columnconfigure(0, weight=1)
        self.top_card = ttk.Frame(main_frame, style="Card.TFrame", padding=profile["card_padding"])
        self.top_card.grid(row=0, column=0, sticky="ew", pady=(0, profile["section_gap"]))
        self.top_card.grid_columnconfigure(0, weight=1)
        self.big_display_label = ttk.Label(self.top_card, text=self._idle_instruction_text(), anchor="center", wraplength=1400, font=(self.default_font_name, 50, "bold"))
        self.big_display_label.grid(row=0, column=0, sticky="ew", pady=profile["big_display_pady"], ipady=profile["big_display_ipady"])

        top_right_frame = ttk.Frame(self.top_card, style="Borderless.TFrame")
        self.top_right_frame = top_right_frame
        top_right_frame.place(relx=1.0, rely=0.0, x=-profile["card_padding"], y=profile["card_padding"], anchor='ne')

        about_button = ttk.Button(top_right_frame, text="정보", command=self._show_about_window, style='Control.TButton')
        about_button.pack(side=tk.RIGHT, padx=(5, 0))
        settings_button = ttk.Button(top_right_frame, text="설정", command=self.open_settings_window, style='Control.TButton')
        settings_button.pack(side=tk.RIGHT)

        input_frame = ttk.Frame(self.top_card, style='Borderless.TFrame')
        input_frame.grid(row=1, column=0, sticky="ew")
        input_frame.grid_columnconfigure(1, weight=1)
        ttk.Label(input_frame, text="바코드 입력:", style="TLabel", background=self.colors["card_background"]).grid(row=0, column=0, padx=(0, 15), sticky='w')
        self.entry = ttk.Entry(input_frame, style="TEntry", state='disabled', font=(self.default_font_name, 18))
        self.entry.grid(row=0, column=1, sticky="ew")
        self.entry.bind("<Return>", self.process_input)
        self.progress_frame = ttk.Frame(self.top_card, style='Borderless.TFrame')
        progress_frame = self.progress_frame
        progress_frame.grid(row=2, column=0, sticky="ew", pady=(profile["content_gap"], 0))
        progress_frame.grid_columnconfigure(0, weight=1)
        self.status_label = ttk.Label(progress_frame, text="첫 번째 바코드를 스캔하세요...", style="Status.TLabel", background=self.colors["card_background"])
        self.status_label.grid(row=0, column=0, sticky="w", padx=15)
        self.step_rail_frame = ttk.Frame(progress_frame, style="Borderless.TFrame")
        self.step_rail_frame.grid(row=1, column=0, sticky="ew", pady=(8, 0))
        self.step_labels = []
        for index, step_name in enumerate(self.STEP_NAMES):
            self.step_rail_frame.grid_columnconfigure(index, weight=1, uniform="scan_steps")
            step_label = tk.Label(
                self.step_rail_frame,
                text=f"{index + 1}. {step_name}",
                font=(self.default_font_name, 11, "bold"),
                padx=8,
                pady=5,
                bd=1,
                relief="solid",
                anchor="center",
            )
            step_label.grid(row=0, column=index, sticky="ew", padx=(0 if index == 0 else 4, 0))
            self.step_labels.append(step_label)
        self.progress_bar = ttk.Progressbar(progress_frame, orient='horizontal', length=200, mode='determinate', maximum=self.TOTAL_SCAN_COUNT, style="green.Horizontal.TProgressbar")
        self.progress_bar.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(6, 0))
        self.view_mode_label = ttk.Label(self.top_card, text="", style="ViewMode.TLabel", anchor="center")
        self.view_mode_label.grid(row=3, column=0, sticky="ew", pady=(profile["content_gap"], 0))
        self.view_mode_label.grid_remove()
        self.content_pane = ttk.PanedWindow(main_frame, orient=tk.HORIZONTAL)
        self.content_pane.grid(row=1, column=0, sticky="nsew", pady=(profile["content_gap"], 0))
        history_card = ttk.Frame(self.content_pane, style="Card.TFrame", padding=profile["card_padding"])
        self.history_card = history_card
        self.content_pane.add(history_card, weight=3)
        history_card.grid_rowconfigure(1, weight=1)
        history_card.grid_columnconfigure(0, weight=1)
        hist_header_frame = ttk.Frame(history_card, style="Borderless.TFrame")
        self.hist_header_frame = hist_header_frame
        hist_header_frame.grid(row=0, column=0, sticky="ew", pady=(0, profile["content_gap"]))
        hist_header_frame.grid_columnconfigure(1, weight=1)

        self._history_header_full_text = "스캔 기록"
        self.hist_header_label = ttk.Label(hist_header_frame, text=self._history_header_full_text, style="Header.TLabel", background=self.colors["card_background"])
        self.hist_header_label.grid(row=0, column=0, sticky="w")

        self.hist_control_frame = ttk.Frame(hist_header_frame, style="Borderless.TFrame")
        hist_control_frame = self.hist_control_frame
        hist_control_frame.grid(row=0, column=2, sticky="e")

        self.today_button = ttk.Button(hist_control_frame, text="오늘", style="Control.TButton", command=self._reload_today_history)
        self.today_button.pack(side=tk.LEFT, padx=(0, 5))
        self.date_search_button = ttk.Button(hist_control_frame, text="날짜 조회", style="Control.TButton", command=self._prompt_for_date_and_reload)
        self.date_search_button.pack(side=tk.LEFT, padx=(0, 15))

        self.decrease_font_button = ttk.Button(hist_control_frame, text="-", style="Control.TButton", command=self._decrease_tree_font)
        self.decrease_font_button.pack(side=tk.LEFT, padx=(0, 0))
        self.increase_font_button = ttk.Button(hist_control_frame, text="+", style="Control.TButton", command=self._increase_tree_font)
        self.increase_font_button.pack(side=tk.LEFT)

        tree_frame_hist = ttk.Frame(history_card, style="Card.TFrame")
        tree_frame_hist.grid(row=1, column=0, sticky='nsew')
        tree_frame_hist.grid_rowconfigure(0, weight=1)
        tree_frame_hist.grid_columnconfigure(0, weight=1)
        hist_cols = list(self.hist_proportions.keys())
        v_scroll_hist = ttk.Scrollbar(tree_frame_hist, orient=tk.VERTICAL)
        h_scroll_hist = ttk.Scrollbar(tree_frame_hist, orient=tk.HORIZONTAL)
        self.history_tree = ttk.Treeview(tree_frame_hist, columns=hist_cols, show="headings", yscrollcommand=v_scroll_hist.set, xscrollcommand=h_scroll_hist.set, selectmode="extended")
        v_scroll_hist.config(command=self.history_tree.yview)
        h_scroll_hist.config(command=self.history_tree.xview)
        for col, labels in self.HISTORY_HEADING_LABELS.items():
            name = labels[0]
            self.history_tree.heading(col, text=name, anchor="center", command=lambda c=col: self._treeview_sort_column(self.history_tree, c, False))
            self.history_tree.column(col, anchor="center", minwidth=70, stretch=False)
        v_scroll_hist.pack(side=tk.RIGHT, fill=tk.Y)
        h_scroll_hist.pack(side=tk.BOTTOM, fill=tk.X)
        self.history_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.history_tree.bind("<Configure>", self._resize_all_columns)
        self.history_tree.bind("<ButtonRelease-1>", self._on_history_tree_resize_release)
        self.history_tree.bind("<<TreeviewSelect>>", self._on_history_selection_changed)
        self.history_tree.bind("<Double-1>", self._show_selected_history_detail_window)

        detail_frame = ttk.Frame(history_card, style="Borderless.TFrame")
        self.history_detail_frame = detail_frame
        detail_frame.grid(row=2, column=0, sticky="ew", pady=(profile["content_gap"], 0))
        detail_frame.grid_columnconfigure(0, weight=1)

        detail_header_frame = ttk.Frame(detail_frame, style="Borderless.TFrame")
        detail_header_frame.grid(row=0, column=0, sticky="ew")
        detail_header_frame.grid_columnconfigure(1, weight=1)
        ttk.Label(detail_header_frame, text="선택 세트 상세", style="Status.TLabel").grid(row=0, column=0, sticky="w")
        self.history_detail_modal_button = ttk.Button(detail_header_frame, text="원문 보기", style="Control.TButton", command=self._show_selected_history_detail_window, state="disabled")
        self.history_detail_modal_button.grid(row=0, column=2, sticky="e", padx=(8, 0))
        self.history_detail_copy_button = ttk.Button(detail_header_frame, text="복사", style="Control.TButton", command=self._copy_selected_history_barcodes, state="disabled")
        self.history_detail_copy_button.grid(row=0, column=3, sticky="e", padx=(6, 0))

        self.history_detail_text = tk.Text(
            detail_frame,
            height=4,
            wrap="word",
            font=("Consolas", 10),
            bg=self.colors["card_background"],
            fg=self.colors["text"],
            relief="solid",
            bd=1,
            padx=8,
            pady=6,
        )
        self.history_detail_text.grid(row=1, column=0, sticky="ew", pady=(4, 0))
        self.history_detail_text.insert("1.0", "기록을 선택하면 현품표와 제품 바코드 원문이 여기에 표시됩니다.")
        self.history_detail_text.configure(state="disabled")

        self.history_context_menu = tk.Menu(self, tearoff=0, font=(self.default_font_name, 14))
        self.history_context_menu.add_command(label="바코드 원문 보기", command=self._show_selected_history_detail_window)
        self.history_context_menu.add_command(label="바코드 원문 복사", command=self._copy_selected_history_barcodes)
        self.history_context_menu.add_separator()
        self.history_context_menu.add_command(label=self.HISTORY_DELETE_ACTION_TEXT, command=self._delete_selected_row)
        self.history_tree.bind("<Button-3>", self._show_history_context_menu)

        summary_card = ttk.Frame(self.content_pane, style="Card.TFrame", padding=profile["card_padding"])
        self.summary_card = summary_card
        self.content_pane.add(summary_card, weight=1)
        summary_card.grid_rowconfigure(1, weight=1)
        summary_card.grid_columnconfigure(0, weight=1)
        self.summary_header_frame = ttk.Frame(summary_card, style="Borderless.TFrame")
        self.summary_header_frame.grid(row=0, column=0, sticky='ew', pady=(0, profile["content_gap"]))
        self.summary_header_frame.grid_columnconfigure(0, weight=1)
        self.summary_header_label = ttk.Label(self.summary_header_frame, text="누적 통과 코드", style="Header.TLabel")
        self.summary_header_label.grid(row=0, column=0, sticky='w')
        self.summary_date_label = ttk.Label(self.summary_header_frame, text="날짜 -", style="SummaryDate.TLabel", anchor="center")
        self.summary_date_label.grid(row=0, column=1, sticky='e', padx=(8, 0))
        tree_frame_sum = ttk.Frame(summary_card, style="Card.TFrame")
        tree_frame_sum.grid(row=1, column=0, sticky='nsew')
        tree_frame_sum.grid_rowconfigure(0, weight=1)
        tree_frame_sum.grid_columnconfigure(0, weight=1)

        summary_cols = list(self.summary_proportions.keys())
        v_scroll_sum = ttk.Scrollbar(tree_frame_sum, orient=tk.VERTICAL)
        self.summary_tree = ttk.Treeview(tree_frame_sum, columns=summary_cols, show="headings", yscrollcommand=v_scroll_sum.set)
        v_scroll_sum.config(command=self.summary_tree.yview)
        self.summary_tree.heading("Code", text=self.SUMMARY_HEADING_LABELS["Code"][0], anchor="center", command=lambda: self._treeview_sort_column(self.summary_tree, "Code", False))
        self.summary_tree.heading("Phase", text=self.SUMMARY_HEADING_LABELS["Phase"][0], anchor="center", command=lambda: self._treeview_sort_column(self.summary_tree, "Phase", False))
        self.summary_tree.heading("Count", text=self.SUMMARY_HEADING_LABELS["Count"][0], anchor="center", command=lambda: self._treeview_sort_column(self.summary_tree, "Count", False))
        v_scroll_sum.pack(side=tk.RIGHT, fill=tk.Y)
        self.summary_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.summary_tree.column("Code", anchor="w", minwidth=180, stretch=False)
        self.summary_tree.column("Phase", anchor="center", minwidth=60, stretch=False)
        self.summary_tree.column("Count", anchor="center", minwidth=70, stretch=False)

        self.summary_tree.bind("<Configure>", self._resize_all_columns)
        self.summary_tree.bind("<ButtonRelease-1>", self._on_summary_tree_resize_release)

        bottom_frame = ttk.Frame(main_frame)
        self.bottom_frame = bottom_frame
        bottom_frame.grid(row=2, column=0, sticky="ew", pady=(profile["bottom_gap"], 0))
        bottom_frame.grid_columnconfigure(4, weight=1)

        reset_button = ttk.Button(bottom_frame, text=self.CURRENT_SET_CANCEL_BUTTON_TEXT, command=lambda: self._reset_current_set(full_reset=True), style=self.CURRENT_SET_CANCEL_BUTTON_STYLE)
        self.reset_button = reset_button
        reset_button.grid(row=0, column=0, sticky="w")
        self.bind("<F1>", lambda e: self._reset_current_set(full_reset=True))

        cancel_tray_button = ttk.Button(bottom_frame, text=self.COMPLETED_TRAY_CANCEL_BUTTON_TEXT, command=self._prompt_and_cancel_completed_tray, style=self.COMPLETED_TRAY_CANCEL_BUTTON_STYLE)
        self.cancel_tray_button = cancel_tray_button
        cancel_tray_button.grid(row=0, column=1, sticky="w", padx=(20, 0))
        self.bind("<F2>", lambda e: self._prompt_and_cancel_completed_tray())
        
        self.manual_complete_button = ttk.Button(bottom_frame, text=self.MANUAL_COMPLETE_BUTTON_TEXT, command=self._prompt_manual_complete, style=self.MANUAL_COMPLETE_BUTTON_STYLE, state="disabled")
        self.manual_complete_button.grid(row=0, column=2, sticky="w", padx=(20, 0))
        self.bind("<F3>", lambda e: self._prompt_manual_complete())

        self.exact_rescan_button = ttk.Button(
            bottom_frame,
            text=self.EXACT_RESCAN_BUTTON_TEXT,
            command=self._prompt_exact_rescan,
            style=self.MANUAL_COMPLETE_BUTTON_STYLE,
            state="disabled",
        )
        self.exact_rescan_button.grid(row=0, column=3, sticky="w", padx=(20, 0))
        self.bind("<F4>", lambda e: self._prompt_exact_rescan())

        self.bind("<Delete>", self._delete_selected_row_from_shortcut)

        self.save_status_label = ttk.Label(bottom_frame, text="", style="Save.Success.TLabel", background=self.colors["background"])
        self.save_status_label.grid(row=0, column=4, sticky="w", padx=30)
        self.clock_label = ttk.Label(bottom_frame, text="", style="TLabel", background=self.colors["background"])
        self.clock_label.grid(row=0, column=5, sticky="e", padx=30)
        self.loading_overlay = ttk.Frame(main_frame, style="Overlay.TFrame")
        loading_content_frame = ttk.Frame(self.loading_overlay, style="Overlay.TFrame")
        loading_content_frame.pack(expand=True)
        loading_label = ttk.Label(loading_content_frame, text="데이터를 불러오는 중입니다...", style="Loading.TLabel")
        loading_label.pack(pady=(0, 15))
        self.loading_progressbar = ttk.Progressbar(loading_content_frame, mode='indeterminate', length=400)
        self.loading_progressbar.pack(pady=15)
        self._update_step_rail(0)
        self._apply_responsive_layout()

    def _prompt_for_date_and_reload(self):
        if not self.initialized_successfully: return
        if self._block_background_history_reload(parent=self):
            return
        if self._block_duplicate_history_load(parent=self):
            return
        
        selected_date = None
        if not self.run_tests:
            cal_win = CalendarWindow(self)
            selected_date = cal_win.result

        if selected_date:
            try:
                target_datetime = datetime.combine(selected_date, datetime.min.time())
                self._load_history_and_rebuild_summary(target_datetime)
                self._process_history_queue()
            except Exception as e:
                if not self.run_tests:
                    messagebox.showerror("조회 오류", f"기록을 조회하는 중 오류가 발생했습니다.\n\n[상세 오류]\n{e}", parent=self)

    def _increase_tree_font(self):
        if not self.initialized_successfully: return
        self.tree_font_size = min(20, self.tree_font_size + 1)
        self._apply_tree_font_style()
        self._resize_all_columns()
        self._request_ui_redraw()
    def _decrease_tree_font(self):
        if not self.initialized_successfully: return
        self.tree_font_size = max(6, self.tree_font_size - 1)
        self._apply_tree_font_style()
        self._resize_all_columns()
        self._request_ui_redraw()
    def _apply_tree_font_style(self):
        try:
            profile = getattr(self, "ui_profile", self.UI_PROFILES["standard"])
            display_tree_font_size = min(self.tree_font_size, profile.get("tree_font_cap", self.tree_font_size))
            self._current_tree_body_font_size = display_tree_font_size
            tree_font = (self.default_font_name, display_tree_font_size)
            row_height_scale = profile.get("tree_row_height_scale", self.ui_cfg.get("treeview_row_height_scale", 3.0))
            row_height = max(28, int(display_tree_font_size * row_height_scale * 0.8))
            self.style.configure("Treeview", font=tree_font, rowheight=row_height)
        except Exception as e:
            print(f"테이블 폰트 적용 오류: {e}")
    def on_ctrl_wheel(self, event):
        if not self.initialized_successfully: return
        if event.delta > 0: self._zoom_in()
        else: self._zoom_out()
        return "break"
    def _zoom_in(self):
        self.scale_factor = min(3.0, self.scale_factor + 0.1)
        self._request_ui_scaling()
    def _zoom_out(self):
        self.scale_factor = max(0.5, self.scale_factor - 0.1)
        self._request_ui_scaling()

    def _request_ui_scaling(self):
        if not self.initialized_successfully:
            return
        pending_after_id = self.__dict__.get("_zoom_after_id")
        if pending_after_id:
            try:
                self.after_cancel(pending_after_id)
            except TclError:
                pass
        self._zoom_after_id = self.after(45, self._flush_pending_ui_scaling)

    def _flush_pending_ui_scaling(self):
        self._zoom_after_id = None
        self._update_ui_scaling()

    def _request_ui_redraw(self, delay=50):
        if "main_frame" not in self.__dict__:
            return
        pending_after_id = self.__dict__.get("_ui_redraw_after_id")
        if pending_after_id:
            try:
                self.after_cancel(pending_after_id)
            except TclError:
                pass
        self._ui_redraw_after_id = self.after(delay, self._force_ui_redraw)

    def _force_ui_redraw(self):
        self._ui_redraw_after_id = None
        if "main_frame" not in self.__dict__:
            return
        try:
            self.update_idletasks()
            for widget_name in (
                "main_frame", "top_card", "content_pane", "history_card", "summary_card",
                "history_tree", "summary_tree", "history_detail_text", "bottom_frame",
            ):
                widget = self.__dict__.get(widget_name)
                if widget is not None and widget.winfo_exists():
                    widget.event_generate("<Expose>")
                    widget.update_idletasks()
            self._redraw_window_now()
            self.update_idletasks()
        except TclError:
            pass

    def _redraw_window_now(self):
        if sys.platform != "win32":
            return
        try:
            import ctypes
            hwnd = self.winfo_id()
            flags = 0x0001 | 0x0004 | 0x0080 | 0x0100 | 0x0400
            ctypes.windll.user32.RedrawWindow(hwnd, None, None, flags)
        except Exception:
            pass

    def _update_ui_scaling(self):
        if not self.initialized_successfully: return
        profile = getattr(self, "ui_profile", self.UI_PROFILES["standard"])
        effective_scale_max = profile.get("effective_scale_max", 2.4)
        effective_scale = max(0.5, min(effective_scale_max, self.scale_factor * profile.get("font_scale", 1.0)))
        font_size = max(10, int(self.base_font_size * effective_scale))
        header_scale = self.ui_cfg.get("header_font_scale", 1.5)
        status_scale = self.ui_cfg.get("status_font_scale", 1.2)
        big_display_scale = self.ui_cfg.get("big_display_font_scale", 4.5)
        header_size = min(int(font_size * header_scale), profile.get("header_font_cap", int(font_size * header_scale)))
        status_size = min(int(font_size * status_scale), profile.get("status_font_cap", int(font_size * status_scale)))
        button_size = min(font_size, profile.get("button_font_cap", font_size))
        control_size = min(max(10, int(font_size * 0.9)), profile.get("control_font_cap", max(10, int(font_size * 0.9))))
        action_size = min(max(11, int(font_size * profile["action_font_scale"])), profile.get("action_font_cap", max(11, int(font_size * profile["action_font_scale"]))))
        tree_heading_size = min(int(font_size * 1.2), profile.get("tree_heading_font_cap", int(font_size * 1.2)))
        default_font = (self.default_font_name, font_size)
        bold_font = (self.default_font_name, button_size, "bold")
        header_font = (self.default_font_name, header_size, "bold")
        status_font = (self.default_font_name, status_size)
        status_bold_font = (self.default_font_name, status_size, "bold")
        save_status_font = (self.default_font_name, int(font_size * 1.0), "bold")
        tree_heading_font = (self.default_font_name, tree_heading_size, "bold")
        big_display_font = (self.default_font_name, min(int(font_size * big_display_scale), profile["big_display_cap"]), "bold")
        clock_font = ("Consolas", int(font_size * 1.0))
        self._current_font_size = font_size
        self._current_header_font_size = header_font[1]
        self._current_tree_heading_font_size = tree_heading_font[1]
        self.style.configure("TLabel", font=default_font)
        self.style.configure("Header.TLabel", font=header_font)
        self.entry.configure(font=default_font)
        self.style.configure("Treeview.Heading", font=tree_heading_font)
        self.style.configure("TButton", font=bold_font, padding=profile["button_padding"])
        self.style.configure("Status.TLabel", font=status_font)
        self.style.configure("SummaryDate.TLabel", font=(self.default_font_name, max(10, min(control_size, 14)), "bold"), padding=(8, 4))
        self.style.configure("Success.TLabel", font=status_bold_font)
        self.style.configure("Error.TLabel", font=status_bold_font)
        self.style.configure("ViewMode.TLabel", font=status_bold_font)
        self.style.configure("Save.Success.TLabel", font=save_status_font)
        self.style.configure("Control.TButton", font=(self.default_font_name, control_size, "bold"), padding=profile["control_padding"])
        action_font = (self.default_font_name, action_size, "bold")
        self.style.configure("Action.TButton", font=action_font, padding=profile["action_padding"])
        self.style.configure("Danger.Action.TButton", font=action_font, padding=profile["action_padding"])
        self.big_display_label.config(font=big_display_font)
        self.clock_label.config(font=clock_font)
        if "step_labels" in self.__dict__:
            step_font = (self.default_font_name, min(max(10, int(font_size * 0.82)), profile.get("control_font_cap", max(10, int(font_size * 0.82))) + 2), "bold")
            for label in self.step_labels:
                label.configure(font=step_font)
        if "history_detail_text" in self.__dict__:
            detail_font = ("Consolas", max(9, min(int(font_size * 0.82), 13)))
            self.history_detail_text.configure(font=detail_font, height=profile["detail_text_height"])
        self._apply_tree_font_style()
        self._resize_all_columns()
        self._apply_responsive_layout()
        self._request_ui_redraw()
    def _resize_all_columns(self, event=None):
        if not self.initialized_successfully: return
        padding = self.ui_cfg.get("column_padding", 20)
        profile_name = self.__dict__.get("ui_profile_name", "standard")
        is_small_profile = profile_name == "small"
        is_dense_profile = profile_name in {"small", "compact"}
        hist_min_widths = {
            "Set": 44 if is_small_profile else 52,
            "Result": 72 if is_small_profile else 86,
            "Timestamp": 90 if is_small_profile else 100,
        }
        for index in range(1, self.TOTAL_SCAN_COUNT + 1):
            hist_min_widths[f"Input{index}"] = (
                158 if is_small_profile else 172 if is_dense_profile else 190
            ) if index == 1 else (
                116 if is_small_profile else 135 if is_dense_profile else 170
            )
        summary_min_widths = {"Code": 190 if is_small_profile else 220, "Phase": 64, "Count": 76}
        try:
            hist_width = self.history_tree.winfo_width() - padding
            if hist_width > 1:
                total_prop = sum(self.hist_proportions.values())
                for col, prop in self.hist_proportions.items():
                    width = max(hist_min_widths.get(col, 80), int(hist_width * (prop / total_prop)))
                    self.history_tree.column(col, width=width, minwidth=hist_min_widths.get(col, 80), stretch=False)

            summary_width = self.summary_tree.winfo_width() - padding
            if summary_width > 1:
                total_prop = sum(self.summary_proportions.values())
                if sum(summary_min_widths.values()) > summary_width:
                    summary_widths = self._scaled_widths_to_total(summary_min_widths, summary_width, floor=42)
                else:
                    summary_widths = {
                        col: max(summary_min_widths.get(col, 70), int(summary_width * (prop / total_prop)))
                        for col, prop in self.summary_proportions.items()
                    }
                for col, width in summary_widths.items():
                    self.summary_tree.column(col, width=width, minwidth=max(24, min(width, summary_min_widths.get(col, 70))), stretch=False)
            self._refresh_history_tree_display_values()
            self._refresh_summary_tree_display_values()
            self._apply_adaptive_header_fitting()
        except (TclError, KeyError):
            pass
    def _on_summary_tree_resize_release(self, event):
        if not self.initialized_successfully: return
        for col in self.summary_tree['columns']:
            self.summary_col_widths[col] = self.summary_tree.column(col, 'width')
    def _on_history_tree_resize_release(self, event):
        if not self.initialized_successfully: return
        for col in self.history_tree['columns']:
            self.history_col_widths[col] = self.history_tree.column(col, 'width')
    def _treeview_sort_column(self, tv, col, reverse):
        if not self.initialized_successfully: return
        try:
            items = [item for item in tv.get_children('') if item != 'loading']
            if col == 'Set' or col == 'Count' or col == 'Phase':
                l = sorted([(int(tv.set(k, col)), k) for k in items if tv.set(k,col)], reverse=reverse)
            elif col == 'Date':
                l = sorted([(tv.set(k, col), k) for k in items if tv.set(k,col)], reverse=reverse, key=lambda x: datetime.strptime(x[0], '%m/%d'))
            else:
                l = sorted([(tv.set(k, col), k) for k in items], reverse=reverse)
            for index, (val, k) in enumerate(l): tv.move(k, '', index)
            tv.heading(col, command=lambda: self._treeview_sort_column(tv, col, not reverse))
        except (ValueError, TclError) as e:
            print(f"정렬 오류: {e}")
            pass

    def _update_clock(self):
        if not self.winfo_exists():
            return
        if self.initialized_successfully:
            self.clock_label.config(text=time.strftime('%Y-%m-%d %H:%M:%S'))
        self._clock_after_id = self.after(1000, self._update_clock)
    def update_big_display(self, text, color=""):
        fg_color = self.colors.get("text_strong", "#000000")
        if color == "red": fg_color = self.colors.get("danger", "#E57370")
        elif color == "green": fg_color = self.colors.get("success", "#00875A")
        elif color == "primary": fg_color = self.colors.get("primary", "#3B82F6")
        self.big_display_label.config(text=text or "", foreground=fg_color)
    def _play_sound(self, sound_key, block=False):
        if not self.initialized_successfully or self.run_tests or _label_match_automated_test_mode(): return
        sound = self.sound_objects.get(sound_key)
        if sound:
            try:
                sound.play()
            except Exception as e:
                print(f"pygame 사운드 재생 오류: {e}")
        else:
            if sound_key in self.sounds:
                print(f"경고: 사운드 키 '{sound_key}'가 존재하지만, 로드되지 않았습니다. 파일 경로를 확인하세요.")

    def _update_summary_tree(self):
        self._render_summary_tree(self.scan_count)

    def _render_summary_tree(self, scan_count):
        if not self.initialized_successfully: return
        self.summary_tree.delete(*self.summary_tree.get_children())
        self.summary_row_raw_values = {}
        self._set_summary_date_label(scan_count)
        summary_counts = defaultdict(int)
        for date_str, items in (scan_count or {}).items():
            try:
                datetime.strptime(str(date_str), '%Y-%m-%d')
            except (ValueError, TypeError) as e:
                print(f"요약 트리 업데이트 중 날짜 형식 오류: {date_str}, 오류: {e}")
                continue
            for (code, phase), count in (items or {}).items():
                if count > 0:
                    summary_counts[(code, phase or "-")] += count
        sorted_items = sorted(summary_counts.items(), key=lambda item: (-item[1], str(item[0][0]), str(item[0][1])))
        date_text = self._summary_date_text(scan_count)
        for (code, phase), count in sorted_items:
            item_id = self.summary_tree.insert("", "end", values=(self._format_summary_code_cell(code), phase, count))
            self.summary_row_raw_values[item_id] = (date_text, code, phase, count)


    def _next_action_text(self, num_scans=None):
        if not getattr(self, "history_view_updates_active_state", True):
            return "과거 기록 조회 중"
        current = getattr(self, "current_set_info", {}) or {}
        if num_scans is None:
            num_scans = len(current.get('parsed', []))
        if num_scans <= 0:
            return self._idle_instruction_text()
        if num_scans < self.FINAL_LABEL_SCAN_POSITION - 1:
            return f"{num_scans + 1}/{self.TOTAL_SCAN_COUNT} 제품 {num_scans} 스캔"
        if num_scans == self.FINAL_LABEL_SCAN_POSITION - 1:
            return f"{self.FINAL_LABEL_SCAN_POSITION}/{self.TOTAL_SCAN_COUNT} 라벨지 스캔"
        return "통과 완료"

    def _manual_complete_hint(self):
        reason = _label_match_manual_complete_block_reason(getattr(self, "current_set_info", {}))
        return self.MANUAL_COMPLETE_HINTS.get(reason, "F3 소량 완료 가능")

    def _update_step_rail(self, num_scans=None, error=False):
        if "step_labels" not in self.__dict__:
            return
        current = getattr(self, "current_set_info", {}) or {}
        if num_scans is None:
            num_scans = len(current.get('parsed', []))
        if not getattr(self, "history_view_updates_active_state", True):
            num_scans = 0
            error = False
        for index, label in enumerate(self.step_labels):
            if error and index == min(num_scans, len(self.step_labels) - 1):
                bg, fg, relief = self.colors["danger"], "white", "solid"
            elif index < num_scans:
                bg, fg, relief = self.colors["success_light"], self.colors["success"], "solid"
            elif index == num_scans:
                bg, fg, relief = self.colors["primary"], "white", "solid"
            else:
                bg, fg, relief = self.colors["background"], self.colors["text_subtle"], "solid"
            label.configure(background=bg, foreground=fg, relief=relief, highlightthickness=0)

    def _apply_history_view_mode(self):
        if "entry" not in self.__dict__:
            return
        if self.history_view_updates_active_state:
            if self.initialized_successfully:
                self.entry.config(state='normal')
                try:
                    self.entry.focus_set()
                except TclError:
                    pass
            self.view_mode_label.grid_remove()
            current = getattr(self, "current_set_info", {}) or {}
            if not current.get("id") and not current.get("parsed"):
                self.update_big_display(self._next_action_text(0), "")
            self._update_status_label()
            self._render_operator_workbench()
            return
        self.entry.config(state='disabled')
        message = "과거 기록 조회 중 - 스캔 입력 비활성. 오늘 버튼으로 복귀하세요."
        if not self.__dict__.get("operator_workbench_ready"):
            self.view_mode_label.config(text=message)
            self.view_mode_label.grid()
            self.update_big_display("과거 기록 조회 중", "primary")
            self.status_label.config(text=message, style="Error.TLabel")
            self._update_step_rail(0)
        self._render_operator_workbench()

    def _show_idle_instruction_if_idle(self):
        current = getattr(self, "current_set_info", {}) or {}
        if current.get("id") or current.get("parsed"):
            return
        if not getattr(self, "history_view_updates_active_state", True):
            return
        if self.__dict__.get("operator_workbench_ready"):
            self._render_operator_workbench()
            return
        if "big_display_label" in self.__dict__:
            if "progress_bar" in self.__dict__:
                self.progress_bar['value'] = 0
            self.update_big_display(self._idle_instruction_text(), "")
            self._update_status_label()

    def _update_status_label(self):
        if not self.initialized_successfully: return
        if not getattr(self, "history_view_updates_active_state", True):
            self._apply_history_view_mode()
            return
        num_scans = len(self.current_set_info['parsed'])
        status_text = self._next_action_text(num_scans)
        if num_scans:
            last_scan = self._truncate_string(str(self.current_set_info['parsed'][-1]), 28)
            status_text += f" | 최근 스캔: {last_scan}"
        if self.current_set_info.get('has_error_or_reset'):
            status_text += " (오류 발생)"
        if self.current_set_info.get("exact_rescan_active"):
            status_text += (
                f" | 전체 재스캔 {len(self.current_set_info.get('exact_rescan_barcodes') or [])}/"
                f"{int(self.current_set_info.get('exact_rescan_target_count') or 0)}"
            )
        elif self.current_set_info.get("exact_rescan_complete"):
            status_text += " | 전체 재스캔 완료"
        status_text += f" | F3: {self._manual_complete_hint()}"
        self.status_label.config(text=status_text, style="Status.TLabel")
        self._update_step_rail(num_scans, error=self.current_set_info.get('has_error_or_reset', False))
        self._update_manual_complete_button_state()
        self._update_exact_rescan_button_state()
        self._render_operator_workbench()

    def _update_manual_complete_button_state(self):
        if not self.initialized_successfully: return
        if not getattr(self, "history_view_updates_active_state", True):
            state = "disabled"
        else:
            state = "normal" if _label_match_manual_complete_allowed(self.current_set_info) else "disabled"
        self.manual_complete_button.config(state=state)

    def _update_exact_rescan_button_state(self):
        if not self.initialized_successfully or "exact_rescan_button" not in self.__dict__:
            return
        raw = list(self.current_set_info.get("raw") or [])
        sealed = False
        if raw:
            try:
                sealed = bool(_label_match_parse_sealed_transfer_qr(raw[0]))
            except ValueError:
                sealed = True
        enabled = (
            getattr(self, "history_view_updates_active_state", True)
            and len(raw) == 1
            and not sealed
            and not self.current_set_info.get("exact_rescan_active")
            and not self.current_set_info.get("exact_rescan_complete")
        )
        self.exact_rescan_button.config(state="normal" if enabled else "disabled")

    def _update_history_tree_in_progress(self):
        if not self.initialized_successfully: return
        if not self.history_view_updates_active_state: return
        num_scans = len(self.current_set_info['parsed'])
        if num_scans == 0: return
        set_id = str(self.current_set_info['id'])
        timestamp = datetime.now().strftime("%H:%M:%S")

        display_scans = self.current_set_info['parsed']
        first_scan_display = display_scans[0] if display_scans else ""
        other_scans_display = display_scans[1:]
        values = (
            "...",
            first_scan_display,
            *other_scans_display[:self.TOTAL_SCAN_COUNT - 1]
            + [""] * ((self.TOTAL_SCAN_COUNT - 1) - len(other_scans_display[:self.TOTAL_SCAN_COUNT - 1])),
            self.Results.IN_PROGRESS,
            timestamp
        )
        self.__dict__.setdefault("history_row_details_map", {})[set_id] = self._details_for_current_set()
        display_values = self._history_values_for_display(values)
        if self.history_tree.exists(set_id):
            try:
                current_display_id = self.history_tree.item(set_id, 'values')[0]
                display_values = (current_display_id, *display_values[1:])
            except IndexError:
                valid_rows = [item for item in self.history_tree.get_children() if item != 'loading']
                display_values = (len(valid_rows) + 1, *display_values[1:])
            self.history_tree.item(set_id, values=display_values, tags=("in_progress",))
        else:
            valid_rows = [item for item in self.history_tree.get_children() if item != 'loading']
            display_id = len(valid_rows) + 1
            display_values = (display_id, *display_values[1:])
            self.history_tree.insert("", 0, values=display_values, iid=set_id, tags=("in_progress",))
        if set_id in self.history_tree.selection():
            self._render_history_detail(set_id)
        self.history_tree.yview_moveto(0)
    def _blink_background_loop(self):
        if not hasattr(self, 'top_card') or not self.top_card.winfo_exists(): return
        original_style = "Card.TFrame"
        error_style = "ErrorCard.TFrame"
        def blink():
            if not self.is_blinking:
                if self.top_card.winfo_exists(): self.top_card.config(style=original_style)
                return
            try:
                current_style = self.top_card.cget("style")
                next_style = error_style if current_style == original_style else original_style
                if self.top_card.winfo_exists():
                    self.top_card.config(style=next_style)
                    self.after(400, blink)
            except TclError:
                pass
        self.after(0, blink)


if __name__ == "__main__":
    _label_match_startup_trace("main_enter")
    try:
        app = Label_Match()
        _label_match_startup_trace("main_after_app_init", title=app.title(), state=app.state())
        _label_match_startup_trace("mainloop_enter")
        app.mainloop()
        _label_match_startup_trace("mainloop_exit")
    except Exception as exc:
        _label_match_startup_trace(
            "main_exception",
            error=repr(exc),
            traceback=traceback.format_exc(),
        )
        raise

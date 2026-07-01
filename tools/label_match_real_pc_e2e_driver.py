"""Drive the installed Label_Match UI on this worker PC and capture smoke evidence.

This is a state/log smoke driver. It starts the real Tkinter app but runs with
run_tests=True and uses direct app calls, so its output must not be treated as
operator workflow proof. Use label_match_operator_ui_walkthrough.py for
run_tests=False full-monitor operator evidence.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import socket
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from PIL import ImageGrab

try:
    import win32gui
except Exception:  # pragma: no cover - field evidence environment specific
    win32gui = None


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import Label_Match as label_match_module  # noqa: E402


GS = "\x1D"
PRODUCT_SAMPLE_COUNT = 4


def _now_utc() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _parse_geometry(value: str) -> tuple[int, int, int, int]:
    # WIDTHxHEIGHT+X+Y, where Y may be negative for a monitor above primary.
    size, rest = value.lower().split("x", 1)
    width = int(size)
    if "+" in rest:
        height_text, x_text, y_text = rest.split("+", 2)
        x = int(x_text)
        y = int(y_text)
    else:
        raise ValueError(f"unsupported geometry={value!r}")
    return width, int(height_text), x, y


def _safe_window_handle(app: Any) -> int:
    app.update_idletasks()
    hwnd = int(app.winfo_id())
    if win32gui is not None:
        try:
            hwnd = int(win32gui.GetAncestor(hwnd, 2))  # GA_ROOT
        except Exception:
            pass
    return hwnd


def _capture_window(app: Any, output_dir: Path, name: str) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    app.update_idletasks()
    app.update()
    time.sleep(0.15)
    hwnd = _safe_window_handle(app)
    path = output_dir / f"{name}.png"
    capture_error = ""
    try:
        image = ImageGrab.grab(window=hwnd, include_layered_windows=True, all_screens=True)
    except Exception as exc:
        capture_error = f"{exc.__class__.__name__}: {exc}"
        x = app.winfo_rootx()
        y = app.winfo_rooty()
        w = max(1, app.winfo_width())
        h = max(1, app.winfo_height())
        image = ImageGrab.grab(bbox=(x, y, x + w, y + h), include_layered_windows=True, all_screens=True)
    image.save(path)
    return {
        "name": name,
        "path": str(path),
        "sha256": _sha256_file(path),
        "width": image.width,
        "height": image.height,
        "window_handle": hwnd,
        "capture_error": capture_error,
        "window": {
            "x": app.winfo_rootx(),
            "y": app.winfo_rooty(),
            "width": app.winfo_width(),
            "height": app.winfo_height(),
            "title": app.title(),
        },
    }


def _wait_for_app_ready(app: Any, timeout_seconds: float = 20.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        app.update()
        if getattr(app, "initialized_successfully", False):
            return
        time.sleep(0.1)
    raise TimeoutError("Label_Match did not initialize before timeout")


def _wait_for_history_idle(app: Any, timeout_seconds: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        app.update()
        if hasattr(app, "_process_history_queue"):
            try:
                app._process_history_queue()
            except Exception:
                pass
        if not getattr(app, "history_load_pending", False) and not getattr(app, "history_active_load_pending", False):
            return
        time.sleep(0.1)
    raise TimeoutError("Label_Match history load did not finish before timeout")


def _scan(app: Any, value: str) -> None:
    app.entry.delete(0, label_match_module.tk.END)
    app.entry.insert(0, value)
    app.process_input()
    app.update()
    app.data_manager.flush(timeout=5)


def _run_valid_set(app: Any, master: str, today: str) -> None:
    _scan(app, master)
    for index in range(1, PRODUCT_SAMPLE_COUNT + 1):
        _scan(app, f"PRODUCT_{master}_{index}")
    _scan(app, f"FINAL_LABEL_{master}_FIELD_E2E_OK{GS}6D{today}")


def _clear_active_set(app: Any) -> None:
    current = getattr(app, "current_set_info", {}) or {}
    if current.get("id") or current.get("raw"):
        app._reset_current_set(full_reset=True)
        app.data_manager.flush(timeout=5)
        app.update()


def _read_new_csv_rows(path: Path, start_size: int) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("rb") as f:
        f.seek(max(0, start_size))
        new_bytes = f.read()
    if not new_bytes:
        return []
    # If we start in the middle of a file, prepend the header from the full file.
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, [])
    text = new_bytes.decode("utf-8-sig", errors="replace")
    lines = [line for line in text.splitlines() if line.strip()]
    if lines and lines[0].startswith("timestamp,"):
        csv_text = "\n".join(lines)
    else:
        csv_text = ",".join(header) + "\n" + "\n".join(lines)
    return list(csv.DictReader(csv_text.splitlines()))


def _event_counts(rows: list[dict[str, str]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        event = row.get("event", "")
        counts[event] = counts.get(event, 0) + 1
    return counts


def _details_containing(rows: list[dict[str, str]], needle: str) -> list[dict[str, Any]]:
    result = []
    for row in rows:
        detail = row.get("details") or ""
        if needle in detail:
            try:
                parsed = json.loads(detail)
            except Exception:
                parsed = {"raw_details": detail}
            result.append({"timestamp": row.get("timestamp"), "event": row.get("event"), "details": parsed})
    return result


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir).resolve()
    screenshot_dir = output_dir / "screenshots"
    output_dir.mkdir(parents=True, exist_ok=True)

    label_match_module.messagebox.askyesno = lambda *a, **kw: True
    label_match_module.messagebox.askokcancel = lambda *a, **kw: True
    label_match_module.messagebox.askyesnocancel = lambda *a, **kw: True
    label_match_module.messagebox.showinfo = lambda *a, **kw: None
    label_match_module.messagebox.showwarning = lambda *a, **kw: None
    label_match_module.messagebox.showerror = lambda *a, **kw: None
    label_match_module.threaded_update_check = lambda: None

    width, height, x, y = _parse_geometry(args.geometry)
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    today = datetime.now().strftime("%Y%m%d")
    marker = f"CODEX_REALPC_E2E_{stamp}"
    malicious = '<script>alert("codex")</script>"; DROP TABLE label_match; -- ../.. =HYPERLINK("http://bad")'

    app = label_match_module.Label_Match(run_tests=True)
    app.state("normal")
    app.geometry(f"{width}x{height}+{x}+{y}")
    app.update()
    _wait_for_app_ready(app)
    _wait_for_history_idle(app)
    _clear_active_set(app)

    log_path = Path(app.data_manager._get_log_filepath())
    start_size = log_path.stat().st_size if log_path.exists() else 0
    screenshots: list[dict[str, Any]] = []
    actions: list[dict[str, Any]] = []

    screenshots.append(_capture_window(app, screenshot_dir, "00_initialized"))

    master_active = f"VALID-{marker}-A"
    _run_valid_set(app, master_active, today)
    actions.append({"name": "valid_auto_complete", "master": master_active})
    screenshots.append(_capture_window(app, screenshot_dir, "01_valid_auto_complete"))

    master_cancelled = f"VALID-{marker}-CANCEL"
    _run_valid_set(app, master_cancelled, today)
    app._cancel_completed_tray_by_label(master_cancelled)
    app.data_manager.flush(timeout=5)
    actions.append({"name": "completed_tray_cancel", "master": master_cancelled})
    screenshots.append(_capture_window(app, screenshot_dir, "02_completed_tray_cancel"))

    master_reset = f"VALID-{marker}-RESET"
    _scan(app, master_reset)
    _scan(app, f"PRODUCT_{master_reset}_1")
    app._reset_current_set(full_reset=True)
    app.data_manager.flush(timeout=5)
    actions.append({"name": "partial_reset_cancel", "master": master_reset})
    screenshots.append(_capture_window(app, screenshot_dir, "03_partial_reset_cancel"))

    master_mismatch = f"VALID-{marker}-MISMATCH"
    _scan(app, master_mismatch)
    _scan(app, f"PRODUCT_WRONG_{marker}_THIS_BARCODE_IS_LONG_ENOUGH_FOR_MISMATCH_1234567890")
    app.data_manager.flush(timeout=5)
    actions.append({"name": "mismatch_error", "master": master_mismatch})
    screenshots.append(_capture_window(app, screenshot_dir, "04_mismatch_error"))
    _clear_active_set(app)

    _scan(app, malicious)
    app.data_manager.flush(timeout=5)
    actions.append({"name": "malicious_input_as_data", "contains_formula_xss_sql_path": True})
    screenshots.append(_capture_window(app, screenshot_dir, "05_malicious_input"))
    _clear_active_set(app)

    master_restore = f"VALID-{marker}-RESTORE"
    _scan(app, master_restore)
    _scan(app, f"PRODUCT_{master_restore}_1")
    app.data_manager.flush(timeout=5)
    screenshots.append(_capture_window(app, screenshot_dir, "06_before_exit_restore_partial"))
    app.on_closing()

    app = label_match_module.Label_Match(run_tests=True)
    app.state("normal")
    app.geometry(f"{width}x{height}+{x}+{y}")
    app.update()
    _wait_for_app_ready(app)
    _wait_for_history_idle(app)
    screenshots.append(_capture_window(app, screenshot_dir, "07_after_restart_restored"))
    for index in range(2, PRODUCT_SAMPLE_COUNT + 1):
        _scan(app, f"PRODUCT_{master_restore}_{index}")
    _scan(app, f"FINAL_LABEL_{master_restore}_RESTORED_OK{GS}6D{today}")
    app.data_manager.flush(timeout=5)
    actions.append({"name": "exit_restore_complete", "master": master_restore})
    screenshots.append(_capture_window(app, screenshot_dir, "08_restored_complete"))

    yesterday = datetime.now() - timedelta(days=1)
    app._load_history_and_rebuild_summary(yesterday)
    _wait_for_history_idle(app)
    screenshots.append(_capture_window(app, screenshot_dir, "09_past_history_view"))
    app._reload_today_history()
    _wait_for_history_idle(app)
    screenshots.append(_capture_window(app, screenshot_dir, "10_today_history_view"))
    actions.append({"name": "past_today_history_view", "past_date": yesterday.strftime("%Y-%m-%d")})

    app.data_manager.flush(timeout=5)
    rows = _read_new_csv_rows(log_path, start_size)
    app.on_closing()

    event_counts = _event_counts(rows)
    report = {
        "report_version": "label-match-real-pc-state-smoke-v2",
        "status": "SMOKE_PASS",
        "generated_at": _now_utc(),
        "host": socket.gethostname(),
        "marker": marker,
        "app_root": str(ROOT),
        "save_log_path": str(log_path),
        "save_log_start_size": start_size,
        "save_log_end_size": log_path.stat().st_size if log_path.exists() else 0,
        "new_log_sha256": _sha256_file(log_path) if log_path.exists() else "",
        "new_rows_count": len(rows),
        "new_event_counts": event_counts,
        "marker_events": _details_containing(rows, marker),
        "malicious_events": _details_containing(rows, "DROP TABLE"),
        "actions": actions,
        "screenshots": screenshots,
        "display_policy": {
            "requested_geometry": args.geometry,
            "main_monitor_not_used": x != 0 or y != 0,
            "run_tests_true": True,
            "operator_input_mode": "direct_entry_insert_plus_process_input",
            "field_evidence_status": "SMOKE_ONLY_NOT_OPERATOR_WORKFLOW_EVIDENCE",
            "note": "Window was placed using the requested geometry; screenshots are captured by HWND where available.",
        },
    }
    issue_codes: list[str] = []
    if not rows:
        issue_codes.append("NO_NEW_UI_EVENT_ROWS_WRITTEN")
    if event_counts.get("TRAY_COMPLETE", 0) < 2:
        issue_codes.append("TRAY_COMPLETE_EVIDENCE_MISSING")
    if event_counts.get("SET_RESTORED", 0) < 1:
        issue_codes.append("SET_RESTORED_EVIDENCE_MISSING")
    if event_counts.get("SET_CANCELLED", 0) < 1:
        issue_codes.append("SET_CANCELLED_EVIDENCE_MISSING")
    if event_counts.get("ERROR_MISMATCH", 0) < 1:
        issue_codes.append("ERROR_MISMATCH_EVIDENCE_MISSING")
    if not report["marker_events"]:
        issue_codes.append("NO_MARKER_EVENTS_WRITTEN")

    if issue_codes:
        report["status"] = "BLOCKED"
        report["issue_codes"] = issue_codes

    report_path = output_dir / "label_match_real_pc_ui_e2e_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": report["status"], "report_path": str(report_path), "marker": marker, "new_rows_count": len(rows)}, ensure_ascii=False))
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--geometry", default="1280x900+900+-1390")
    args = parser.parse_args()
    report = run(args)
    return 0 if report.get("status") == "SMOKE_PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())

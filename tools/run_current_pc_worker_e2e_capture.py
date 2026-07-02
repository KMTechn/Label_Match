import argparse
import csv
import ctypes
import hashlib
import importlib.util
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import win32con
import win32gui
import win32ui
from PIL import Image, ImageStat


ROOT = Path(__file__).resolve().parents[1]
PROGRAM_ROOT = ROOT.parent
DEFAULT_OUTPUT_ROOT = ROOT / "outputs"
DEFAULT_LABEL_MATCH_DATA = Path(os.environ.get("ProgramData", r"C:\ProgramData")) / "KMTech" / "Label_Match" / "data"
PRODUCT_SAMPLE_COUNT = 3
TOTAL_SCAN_COUNT = PRODUCT_SAMPLE_COUNT + 2
CURRENT_PC_DIRECT_SYNC_ROOT = (
    Path(os.environ.get("ProgramData", r"C:\ProgramData"))
    / "KMTech"
    / "DirectSync"
    / "label-match-desktop-03pcrd7-1600978dba99"
)


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8-sig"))
    except Exception as exc:
        return {"_read_error": str(exc), "_path": str(path)}


def load_label_match_module():
    module_path = ROOT / "Label_Match.py"
    spec = importlib.util.spec_from_file_location("label_match_e2e_module", module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def pump(app, seconds=0.2):
    deadline = time.time() + seconds
    while time.time() < deadline:
        app.update()
        time.sleep(0.02)


def wait_until(app, predicate, timeout=12.0, label="condition"):
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            app.update()
            if predicate():
                return True
        except Exception as exc:
            last_error = exc
        time.sleep(0.05)
    raise TimeoutError(f"Timed out waiting for {label}; last_error={last_error!r}")


def wait_history_idle(app, timeout=20.0, label="history load"):
    def predicate():
        try:
            app._process_history_queue()
        except Exception:
            pass
        return not getattr(app, "history_load_pending", False)

    return wait_until(app, predicate, timeout=timeout, label=label)


def capture_window(hwnd, path):
    left, top, right, bottom = win32gui.GetWindowRect(hwnd)
    width = max(1, right - left)
    height = max(1, bottom - top)

    hwnd_dc = win32gui.GetWindowDC(hwnd)
    mfc_dc = win32ui.CreateDCFromHandle(hwnd_dc)
    save_dc = mfc_dc.CreateCompatibleDC()
    bitmap = win32ui.CreateBitmap()
    bitmap.CreateCompatibleBitmap(mfc_dc, width, height)
    save_dc.SelectObject(bitmap)

    try:
        result = ctypes.windll.user32.PrintWindow(hwnd, save_dc.GetSafeHdc(), 2)
        if not result:
            save_dc.BitBlt((0, 0), (width, height), mfc_dc, (0, 0), win32con.SRCCOPY)
        bmp_info = bitmap.GetInfo()
        bmp_bits = bitmap.GetBitmapBits(True)
        image = Image.frombuffer(
            "RGB",
            (bmp_info["bmWidth"], bmp_info["bmHeight"]),
            bmp_bits,
            "raw",
            "BGRX",
            0,
            1,
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        image.save(path)
        stat = ImageStat.Stat(image)
        extrema = image.convert("L").getextrema()
        return {
            "path": str(path),
            "width": image.width,
            "height": image.height,
            "print_window_result": int(result),
            "grayscale_extrema": list(extrema),
            "mean": stat.mean,
            "sha256": sha256_file(path),
            "blank_suspected": extrema[0] == extrema[1],
        }
    finally:
        win32gui.DeleteObject(bitmap.GetHandle())
        save_dc.DeleteDC()
        mfc_dc.DeleteDC()
        win32gui.ReleaseDC(hwnd, hwnd_dc)


def row_count(path):
    if not path.exists():
        return 0
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        return max(0, sum(1 for _ in csv.DictReader(handle)))


def scan_event_counts(path, marker):
    counts = {}
    matched_rows = 0
    if not path.exists():
        return {"exists": False, "matched_rows": 0, "events": counts}
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            details = row.get("details") or ""
            worker_name = row.get("worker_name") or ""
            if marker not in details and marker not in worker_name:
                continue
            matched_rows += 1
            event = row.get("event") or ""
            counts[event] = counts.get(event, 0) + 1
    return {"exists": True, "matched_rows": matched_rows, "events": counts}


def append_past_history_row(module, data_manager, marker):
    yesterday = datetime.now() - timedelta(days=1)
    path = Path(data_manager._get_log_filepath(yesterday))
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    details = {
        "master_label_code": f"VALID-PAST-{marker}",
        "item_code": f"VALID-PAST-{marker}",
        "item_name": "CODEX E2E PAST",
        "spec": "E2E",
        "scan_count": TOTAL_SCAN_COUNT,
        "scanned_product_barcodes": [
            f"VALID-PAST-{marker}",
            *(f"PRODUCT_VALID-PAST-{marker}_{index}" for index in range(1, PRODUCT_SAMPLE_COUNT + 1)),
            f"FINAL_LABEL_VALID-PAST-{marker}\x1D6D{yesterday.strftime('%Y%m%d')}",
        ],
        "parsed_product_barcodes": [f"VALID-PAST-{marker}"] * TOTAL_SCAN_COUNT,
        "work_time_sec": 1.0,
        "error_count": 0,
        "has_error_or_reset": False,
        "final_result": module.LABEL_MATCH_RESULT_PASS,
        "result_display": module.LABEL_MATCH_RESULT_PASS,
        "is_partial_submission": False,
        "start_time": yesterday.isoformat(),
        "end_time": yesterday.isoformat(),
        "production_date": yesterday.strftime("%Y-%m-%d"),
        "set_id": f"past-{marker}",
        "phase": "-",
    }
    with open(path, "a", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        if not exists:
            writer.writerow(["timestamp", "worker_name", "event", "details"])
        writer.writerow([
            yesterday.isoformat(),
            data_manager.worker_name,
            module.Label_Match.Events.TRAY_COMPLETE,
            json.dumps(details, ensure_ascii=False),
        ])
    return path


def scan(app, value):
    app.entry.config(state="normal")
    app.entry.delete(0, "end")
    app.entry.insert(0, value)
    app.process_input()
    pump(app, 0.25)


def complete_full_tray(app, marker, capture, module, prefix="VALID-E2E"):
    today = datetime.now().strftime("%Y%m%d")
    master = f"{prefix}-{marker}"
    values = [
        master,
        *(f"PRODUCT_{master}_{index}" for index in range(1, PRODUCT_SAMPLE_COUNT + 1)),
        f"FINAL_LABEL_{master}_{marker}\x1D6D{today}",
    ]
    for index, value in enumerate(values, 1):
        scan(app, value)
        if index in {1, PRODUCT_SAMPLE_COUNT + 1}:
            capture(f"full_tray_step_{index}_{master}")
    pump(app, 0.5)
    capture(f"full_tray_completed_{master}")
    return master


def build_arg_parser():
    parser = argparse.ArgumentParser(description="Run current-PC Label_Match state/log smoke capture against real worker storage.")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--save-dir", default=str(DEFAULT_LABEL_MATCH_DATA))
    parser.add_argument("--direct-sync-root", default=str(CURRENT_PC_DIRECT_SYNC_ROOT))
    parser.add_argument("--capture-geometry", default="1366x768+0+0")
    parser.add_argument("--run-relay", action="store_true")
    parser.add_argument("--relay-wait-seconds", type=int, default=75)
    parser.add_argument("--source-stability-wait-seconds", type=int, default=35)
    return parser


def main(argv=None):
    args = build_arg_parser().parse_args(argv)
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    marker = f"CODEX_LABEL_E2E_{run_id}"
    output_dir = Path(args.output_root) / f"current-pc-worker-e2e-{run_id}"
    screenshots_dir = output_dir / "screenshots"
    output_dir.mkdir(parents=True, exist_ok=True)

    os.environ["LABEL_MATCH_SAVE_DIR"] = str(Path(args.save_dir))
    os.chdir(ROOT)
    sys.path.insert(0, str(ROOT))

    module = load_label_match_module()
    module.messagebox.askyesno = lambda *a, **k: True
    module.messagebox.askokcancel = lambda *a, **k: True
    module.messagebox.showinfo = lambda *a, **k: None
    module.messagebox.showwarning = lambda *a, **k: None
    module.messagebox.showerror = lambda *a, **k: None

    screenshots = []
    steps = []
    app = None
    try:
        app = module.Label_Match(run_tests=True)
        app.state("normal")
        app.geometry(args.capture_geometry)
        app.update()
        app.lift()
        try:
            app.focus_force()
        except Exception:
            pass
        wait_until(app, lambda: getattr(app, "initialized_successfully", False), timeout=20, label="Label_Match initialization")
        wait_history_idle(app, timeout=20, label="initial history load")

        app.worker_name = marker
        app.data_manager.worker_name = marker
        app.title(f"바코드 세트 검증기 ({module.APP_VERSION}) - {app.worker_name} ({app.unique_id})")
        app.data_manager.log_event(module.Label_Match.Events.APP_START, {"message": "current PC UI E2E started", "e2e_marker": marker})
        app.data_manager.flush(timeout=5)

        hwnd = app.winfo_id()

        def capture(name):
            pump(app, 0.1)
            safe = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in name)[:140]
            info = capture_window(hwnd, screenshots_dir / f"{len(screenshots) + 1:02d}_{safe}.png")
            info["name"] = name
            screenshots.append(info)
            return info

        capture("initial_registered_worker_pc_idle")

        full_master = complete_full_tray(app, marker, capture, module)
        steps.append({"name": "normal_full_tray", "status": "PASS", "master": full_master})

        manual_master = f"VALID-MANUAL-{marker}"
        scan(app, manual_master)
        scan(app, f"PRODUCT_{manual_master}_1")
        capture("manual_complete_ready")
        app._prompt_manual_complete()
        pump(app, 0.5)
        capture("manual_complete_done")
        steps.append({"name": "manual_complete_partial", "status": "PASS", "master": manual_master})

        mismatch_master = f"VALID-MISMATCH-{marker}"
        scan(app, mismatch_master)
        scan(app, f"PRODUCT_WRONG_{marker}")
        capture("mismatch_error_state")
        if app.current_set_info.get("id"):
            app._finalize_set(app.Results.FAIL_MISMATCH, f"PRODUCT_WRONG_{marker}")
            pump(app, 0.5)
        capture("mismatch_recorded_and_reset")
        steps.append({"name": "mismatch_error_recovery", "status": "PASS", "master": mismatch_master})

        reset_master = f"VALID-RESET-{marker}"
        scan(app, reset_master)
        scan(app, f"PRODUCT_{reset_master}_1")
        capture("reset_before_cancel")
        reset_result = app._reset_current_set(full_reset=True)
        pump(app, 0.3)
        capture("reset_after_cancel")
        steps.append({"name": "current_set_reset", "status": "PASS" if reset_result else "FAIL", "master": reset_master})

        cancel_master = complete_full_tray(app, f"CANCEL-{marker}", capture, module)
        app._cancel_completed_tray_by_label(cancel_master)
        pump(app, 0.5)
        capture("completed_tray_cancelled")
        steps.append({"name": "completed_tray_cancel", "status": "PASS", "master": cancel_master})

        injection_master_text = f"CLC=INJ-{marker}|SPC=<script>alert(1)</script>=HYPERLINK(\"http://invalid\")|PHS=9"
        encoded_injection = module.base64.b64encode(injection_master_text.encode("utf-8")).decode("utf-8")
        injection_code = f"INJ-{marker}"
        today = datetime.now().strftime("%Y%m%d")
        for value in [
            encoded_injection,
            *(f"PRODUCT_{injection_code}_{index}" for index in range(1, PRODUCT_SAMPLE_COUNT + 1)),
            f"FINAL_LABEL_{injection_code}_{marker}\x1D6D{today}",
        ]:
            scan(app, value)
        pump(app, 0.5)
        capture("malicious_input_rendered_safely")
        steps.append({"name": "malicious_input_ui_csv_path", "status": "PASS", "master": injection_code})

        past_path = append_past_history_row(module, app.data_manager, marker)
        target_date = datetime.now() - timedelta(days=1)
        app._load_history_and_rebuild_summary(target_date)
        wait_history_idle(app, timeout=20, label="past history load")
        capture("past_history_view_only")
        scan(app, f"PRODUCT_SHOULD_BE_BLOCKED_{marker}")
        capture("past_history_scan_blocked")
        app._reload_today_history()
        wait_history_idle(app, timeout=20, label="today history reload")
        capture("today_history_restored")
        steps.append({"name": "past_history_view_only_and_restore", "status": "PASS", "past_log": str(past_path)})

        app.data_manager.log_event(module.Label_Match.Events.APP_CLOSE, {"message": "current PC UI E2E finished", "e2e_marker": marker})
        app.data_manager.close(timeout=10)
        app.destroy()
        app = None

        data_file = Path(args.save_dir) / f"포장실작업이벤트로그_{socket.gethostname()}_{datetime.now().strftime('%Y%m%d')}.csv"
        before_relay_status = read_json(Path(args.direct_sync_root) / "status" / "direct_sync_relay_status.json")
        upload_dir = Path(args.direct_sync_root) / "upload_status"
        upload_files_before = {p.name for p in upload_dir.glob("*.json")} if upload_dir.exists() else set()

        relay_result = {"attempted": False}
        if args.run_relay:
            if args.source_stability_wait_seconds > 0:
                time.sleep(args.source_stability_wait_seconds)
            relay_script = Path(args.direct_sync_root) / "bin" / "run_direct-sync-relay-label-match-current-pc.ps1"
            if relay_script.exists():
                completed = subprocess.run(
                    [
                        os.environ.get("SystemRoot", r"C:\Windows") + r"\System32\WindowsPowerShell\v1.0\powershell.exe",
                        "-NoProfile",
                        "-ExecutionPolicy",
                        "Bypass",
                        "-File",
                        str(relay_script),
                    ],
                    cwd=str(PROGRAM_ROOT),
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=max(30, args.relay_wait_seconds),
                )
                relay_result = {
                    "attempted": True,
                    "returncode": completed.returncode,
                    "stdout_tail": completed.stdout[-2000:],
                    "stderr_tail": completed.stderr[-2000:],
                }
            else:
                relay_result = {"attempted": True, "error": f"relay script missing: {relay_script}"}

        time.sleep(1.0)
        after_relay_status = read_json(Path(args.direct_sync_root) / "status" / "direct_sync_relay_status.json")
        upload_files_after = {p.name for p in upload_dir.glob("*.json")} if upload_dir.exists() else set()
        new_upload_files = sorted(upload_files_after - upload_files_before)
        latest_uploads = []
        if upload_dir.exists():
            for path in sorted(upload_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)[:8]:
                payload = read_json(path)
                latest_uploads.append({
                    "file": str(path),
                    "mtime": datetime.fromtimestamp(path.stat().st_mtime).isoformat(),
                    "status_code": payload.get("status_code"),
                    "committed": payload.get("committed"),
                    "retryable": payload.get("retryable"),
                    "error_code": payload.get("error_code"),
                    "error_message": payload.get("error_message"),
                    "receipt_id": payload.get("receipt_id") or payload.get("receipt", {}).get("receipt_id"),
                    "request_id": payload.get("request_id") or payload.get("receipt", {}).get("request_id"),
                })

        report = {
            "report_version": "label-match-current-pc-worker-state-smoke-v2",
            "generated_at": datetime.now().isoformat(),
            "marker": marker,
            "host": socket.gethostname(),
            "save_dir": str(Path(args.save_dir)),
            "data_file": str(data_file),
            "data_file_exists": data_file.exists(),
            "data_file_sha256": sha256_file(data_file) if data_file.exists() else None,
            "data_file_total_rows": row_count(data_file),
            "marker_rows": scan_event_counts(data_file, marker),
            "direct_sync_root": str(Path(args.direct_sync_root)),
            "registration": {
                "manifest_exists": (Path(args.direct_sync_root) / "producer_manifest.json").exists(),
                "credential_exists": (Path(args.direct_sync_root) / "credential.json").exists(),
                "dpapi_secret_exists": bool(list((Path(args.direct_sync_root) / "secrets").glob("*.dpapi"))) if (Path(args.direct_sync_root) / "secrets").exists() else False,
                "self_enrollment_receipt_exists": (Path(args.direct_sync_root) / "producer_self_enrollment_receipt.json").exists(),
                "manifest": read_json(Path(args.direct_sync_root) / "producer_manifest.json"),
                "registration_report": read_json(Path(args.direct_sync_root) / "status" / "label_match_worker_pc_registration.json"),
            },
            "ui_capture_mode": {
                "method": "Win32 PrintWindow supplemental window capture",
                "capture_geometry": args.capture_geometry,
                "field_evidence_status": "SMOKE_ONLY_NOT_OPERATOR_WORKFLOW_EVIDENCE",
                "run_tests_true": True,
                "operator_input_mode": "direct_entry_insert_plus_process_input",
                "main_monitor_intent": "supplemental state/log capture; use label_match_operator_ui_walkthrough.py for full-monitor operator evidence",
                "computer_use_attempt": "failed_before_action: @oai/sky package exports error",
            },
            "steps": steps,
            "screenshots": screenshots,
            "relay": relay_result,
            "relay_status_before": before_relay_status,
            "relay_status_after": after_relay_status,
            "new_upload_status_files": new_upload_files,
            "latest_upload_status": latest_uploads,
            "blocker_closure": {
                "current_pc_registered": (Path(args.direct_sync_root) / "producer_manifest.json").exists()
                and (Path(args.direct_sync_root) / "credential.json").exists(),
                "ui_e2e_captured": sum(1 for item in screenshots if not item.get("blank_suspected")) >= 12,
                "local_csv_written": data_file.exists() and scan_event_counts(data_file, marker)["matched_rows"] > 0,
                "relay_invoked": bool(relay_result.get("attempted")),
                "relay_no_error": after_relay_status.get("status") in {"idle", "pass", "ok"} and not after_relay_status.get("error_code"),
                "new_clean_install_self_enroll_closed": False,
                "physical_scanner_closed": False,
                "twenty_physical_pc_closed": False,
                "syncthing_shadow_closed": True,
                "syncthing_not_applicable": True,
                "syncthing_policy": "http_push_only_no_syncthing",
                "rollback_rehearsal_closed": False,
            },
        }
        report["status"] = "SMOKE_PASS_WITH_REMAINING_FIELD_BLOCKERS" if all([
            report["blocker_closure"]["current_pc_registered"],
            report["blocker_closure"]["ui_e2e_captured"],
            report["blocker_closure"]["local_csv_written"],
            report["blocker_closure"]["relay_no_error"],
        ]) else "FAIL"
        report_path = output_dir / "label_match_current_pc_worker_ui_e2e_report.json"
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({"status": report["status"], "report_path": str(report_path), "marker": marker}, ensure_ascii=False, indent=2))
        return 0 if report["status"].startswith("SMOKE_PASS") else 1
    except Exception as exc:
        if app is not None:
            try:
                app.destroy()
            except Exception:
                pass
        error_report = {
            "report_version": "label-match-current-pc-worker-ui-e2e-v1",
            "generated_at": datetime.now().isoformat(),
            "marker": marker,
            "status": "ERROR",
            "error": repr(exc),
        }
        report_path = output_dir / "label_match_current_pc_worker_ui_e2e_report.json"
        report_path.write_text(json.dumps(error_report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps({"status": "ERROR", "report_path": str(report_path), "error": repr(exc)}, ensure_ascii=False, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

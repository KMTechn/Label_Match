"""Run an operator-style Label_Match UI walkthrough and capture evidence.

This driver is intentionally different from the run_tests=True helpers:
it launches the real Tk UI with run_tests=False, enters scans through the
entry Return binding, invokes visible button flows, captures modal dialogs,
and writes all data to a run-scoped save directory.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from PIL import Image, ImageGrab, ImageStat

import win32api
import win32clipboard
import win32con
import win32gui
import win32process


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

GS = "\x1D"
REAL_MASTER = "AAA2270730100"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _safe_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in value)[:150]


def _set_clipboard_text(text: str) -> None:
    win32clipboard.OpenClipboard()
    try:
        win32clipboard.EmptyClipboard()
        win32clipboard.SetClipboardData(win32con.CF_UNICODETEXT, text)
    finally:
        win32clipboard.CloseClipboard()


def _send_key(vk: int) -> None:
    win32api.keybd_event(vk, 0, 0, 0)
    time.sleep(0.05)
    win32api.keybd_event(vk, 0, win32con.KEYEVENTF_KEYUP, 0)


def _send_ctrl_v() -> None:
    win32api.keybd_event(win32con.VK_CONTROL, 0, 0, 0)
    time.sleep(0.02)
    _send_key(ord("V"))
    time.sleep(0.02)
    win32api.keybd_event(win32con.VK_CONTROL, 0, win32con.KEYEVENTF_KEYUP, 0)


def _click_dialog_button(hwnd: int, prefixes: tuple[str, ...] = ("예", "확인", "OK")) -> bool:
    matches: list[int] = []

    def callback(child: int, _extra: Any) -> bool:
        text = (win32gui.GetWindowText(child) or "").strip()
        cls = win32gui.GetClassName(child) or ""
        if cls.lower() == "button" and any(text.startswith(prefix) for prefix in prefixes):
            matches.append(child)
        return True

    win32gui.EnumChildWindows(hwnd, callback, None)
    if not matches:
        return False
    win32gui.SendMessage(matches[0], win32con.BM_CLICK, 0, 0)
    return True


def _set_dialog_edit_text(hwnd: int, text: str) -> bool:
    matches: list[int] = []

    def callback(child: int, _extra: Any) -> bool:
        cls = win32gui.GetClassName(child) or ""
        if cls.lower() == "edit":
            matches.append(child)
        return True

    win32gui.EnumChildWindows(hwnd, callback, None)
    if not matches:
        return False
    win32gui.SendMessage(matches[0], win32con.WM_SETTEXT, 0, text)
    return True


def _helper_wait_for_window(pid: int, title_contains: str, timeout: float = 10.0) -> int:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        matches: list[int] = []

        def callback(hwnd: int, _extra: Any) -> bool:
            if not win32gui.IsWindowVisible(hwnd):
                return True
            _, found_pid = win32process.GetWindowThreadProcessId(hwnd)
            title = win32gui.GetWindowText(hwnd) or ""
            if found_pid == pid and title_contains in title:
                matches.append(hwnd)
            return True

        win32gui.EnumWindows(callback, None)
        if matches:
            return int(matches[0])
        time.sleep(0.1)
    raise TimeoutError(f"window not found pid={pid} title_contains={title_contains!r}")


def _dialog_helper_main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--screenshot", required=True)
    parser.add_argument("--text", default="")
    parser.add_argument("--delay", type=float, default=0.0)
    parser.add_argument("--prefixes", default="예,확인,OK")
    args = parser.parse_args(argv)
    screenshot_path = Path(args.screenshot)
    json_path = Path(str(args.screenshot) + ".json")
    info: dict[str, Any] = {
        "name": screenshot_path.stem,
        "capture_target": "dialog_helper",
        "pid": args.pid,
        "title_contains": args.title,
    }
    exit_code = 0
    try:
        if args.delay:
            time.sleep(args.delay)
        hwnd = _helper_wait_for_window(args.pid, args.title)
        info["window_text"] = win32gui.GetWindowText(hwnd)
        try:
            win32gui.SetForegroundWindow(hwnd)
            info["foreground_set"] = True
        except Exception as exc:
            info["foreground_set"] = False
            info["foreground_error"] = repr(exc)
        time.sleep(0.1)
        try:
            info.update(_capture_bbox(hwnd, screenshot_path))
        except Exception as exc:
            info["capture_error"] = repr(exc)
        if args.text:
            info["edit_text_set"] = _set_dialog_edit_text(hwnd, args.text)
            if not info["edit_text_set"]:
                try:
                    _set_clipboard_text(args.text)
                    _send_ctrl_v()
                    info["clipboard_paste_attempted"] = True
                except Exception as exc:
                    info["clipboard_paste_error"] = repr(exc)
            time.sleep(0.1)
        prefixes = tuple(part for part in args.prefixes.split(",") if part)
        clicked = _click_dialog_button(hwnd, prefixes=prefixes)
        info["button_clicked"] = clicked
        if not clicked:
            for command_id in (win32con.IDYES, win32con.IDOK):
                try:
                    win32gui.SendMessage(hwnd, win32con.WM_COMMAND, command_id, 0)
                    info.setdefault("fallback_command_ids", []).append(command_id)
                    time.sleep(0.1)
                    break
                except Exception as exc:
                    info.setdefault("fallback_command_errors", []).append(repr(exc))
            if not info.get("fallback_command_ids"):
                _send_key(win32con.VK_RETURN)
                info["return_key_sent"] = True
    except Exception as exc:
        info["helper_error"] = repr(exc)
        exit_code = 1
    finally:
        json_path.parent.mkdir(parents=True, exist_ok=True)
        json_path.write_text(json.dumps(info, ensure_ascii=False, indent=2), encoding="utf-8")
    return exit_code


def _capture_bbox(hwnd: int, path: Path) -> dict[str, Any]:
    left, top, right, bottom = win32gui.GetWindowRect(hwnd)
    if right <= left or bottom <= top:
        raise RuntimeError(f"invalid window rect: {(left, top, right, bottom)}")
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        image = ImageGrab.grab(bbox=(left, top, right, bottom), all_screens=True, include_layered_windows=True)
    except TypeError:
        image = ImageGrab.grab(bbox=(left, top, right, bottom))
    image.save(path)
    stat = ImageStat.Stat(image)
    extrema = image.convert("L").getextrema()
    return {
        "path": str(path),
        "sha256": _sha256_file(path),
        "width": image.width,
        "height": image.height,
        "window_rect": [left, top, right, bottom],
        "grayscale_extrema": list(extrema),
        "mean": stat.mean,
        "blank_suspected": extrema[0] == extrema[1],
    }


def _capture_window(app: Any, output_dir: Path, name: str, note: str = "") -> dict[str, Any]:
    app.update_idletasks()
    app.update()
    time.sleep(0.18)
    hwnd = int(app.winfo_id())
    try:
        hwnd = int(win32gui.GetAncestor(hwnd, win32con.GA_ROOT))
    except Exception:
        pass
    path = output_dir / f"{name}.png"
    info = _capture_bbox(hwnd, path)
    info.update(
        {
            "name": name,
            "note": note,
            "capture_target": "app_window",
            "title": app.title(),
            "big_display": getattr(getattr(app, "big_display_label", None), "cget", lambda _k: "")("text"),
            "status": getattr(getattr(app, "status_label", None), "cget", lambda _k: "")("text"),
        }
    )
    return info


def _find_process_window(title_contains: str | None = None, fallback_foreground: bool = False) -> int:
    current_pid = os.getpid()
    candidates: list[tuple[int, str]] = []

    def callback(hwnd: int, _extra: Any) -> bool:
        if not win32gui.IsWindowVisible(hwnd):
            return True
        _, pid = win32process.GetWindowThreadProcessId(hwnd)
        if pid != current_pid:
            return True
        title = win32gui.GetWindowText(hwnd) or ""
        if title_contains is None or title_contains in title:
            candidates.append((hwnd, title))
        return True

    win32gui.EnumWindows(callback, None)
    if candidates:
        return int(candidates[0][0])
    if fallback_foreground:
        return int(win32gui.GetForegroundWindow())
    raise RuntimeError(f"dialog/window not found for title_contains={title_contains!r}")


def _capture_active(output_dir: Path, name: str, note: str = "", title_contains: str | None = None) -> dict[str, Any]:
    hwnd = _find_process_window(title_contains=title_contains, fallback_foreground=title_contains is None)
    path = output_dir / f"{name}.png"
    info = _capture_bbox(hwnd, path)
    info.update(
        {
            "name": name,
            "note": note,
            "capture_target": "foreground_window",
            "window_text": win32gui.GetWindowText(hwnd),
        }
    )
    return info


def _schedule_dialog_action(
    screenshots: list[dict[str, Any]],
    screenshot_dir: Path,
    name: str,
    delay: float = 0.7,
    text: str | None = None,
    enter: bool = True,
    note: str = "",
    title_contains: str | None = None,
) -> subprocess.Popen[Any] | None:
    if not title_contains:
        return None
    screenshot = screenshot_dir / f"{name}.png"
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--dialog-helper",
        "--pid",
        str(os.getpid()),
        "--title",
        title_contains,
        "--screenshot",
        str(screenshot),
        "--delay",
        str(delay),
    ]
    if text is not None:
        cmd.extend(["--text", text])
    return subprocess.Popen(cmd, cwd=str(ROOT), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def _start_dialog_capture_click_thread(
    screenshots: list[dict[str, Any]],
    screenshot_dir: Path,
    name: str,
    title_contains: str,
    prefixes: tuple[str, ...] = ("예", "확인", "OK"),
    delay: float = 0.1,
) -> threading.Thread:
    def worker() -> None:
        info: dict[str, Any] = {
            "name": name,
            "capture_target": "dialog_thread",
            "pid": os.getpid(),
            "title_contains": title_contains,
        }
        try:
            if delay:
                time.sleep(delay)
            hwnd = _helper_wait_for_window(os.getpid(), title_contains)
            info["window_text"] = win32gui.GetWindowText(hwnd)
            try:
                info.update(_capture_bbox(hwnd, screenshot_dir / f"{name}.png"))
            except Exception as exc:
                info["capture_error"] = repr(exc)
            clicked = _click_dialog_button(hwnd, prefixes=prefixes)
            info["button_clicked"] = clicked
            if not clicked:
                for command_id in (win32con.IDYES, win32con.IDOK):
                    try:
                        win32gui.SendMessage(hwnd, win32con.WM_COMMAND, command_id, 0)
                        info.setdefault("fallback_command_ids", []).append(command_id)
                        break
                    except Exception as exc:
                        info.setdefault("fallback_command_errors", []).append(repr(exc))
        except Exception as exc:
            info["helper_error"] = repr(exc)
        finally:
            screenshots.append(info)
            try:
                (screenshot_dir / f"{name}.png.json").write_text(json.dumps(info, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception:
                pass

    thread = threading.Thread(target=worker, name=f"{name}-dialog-clicker", daemon=True)
    thread.start()
    return thread


def _pump(app: Any, seconds: float = 0.3) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        app.update()
        time.sleep(0.03)


def _wait_until(app: Any, predicate: Any, timeout: float, label: str) -> None:
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            app.update()
            if predicate():
                return
        except Exception as exc:
            last_error = exc
        time.sleep(0.05)
    raise TimeoutError(f"Timed out waiting for {label}; last_error={last_error!r}")


def _iter_tk_descendants(widget: Any) -> Any:
    for child in widget.winfo_children():
        yield child
        yield from _iter_tk_descendants(child)


def _invoke_first_tk_button(widget: Any, prefixes: tuple[str, ...] = ("확인", "예", "OK")) -> bool:
    for child in _iter_tk_descendants(widget):
        try:
            text = str(child.cget("text"))
        except Exception:
            continue
        if any(text.startswith(prefix) for prefix in prefixes):
            try:
                child.invoke()
                return True
            except Exception:
                continue
    return False


def _schedule_tk_toplevel_capture_and_close(
    app: Any,
    screenshots: list[dict[str, Any]],
    screenshot_dir: Path,
    name: str,
    title_contains: str,
    delay_ms: int = 700,
    note: str = "",
) -> None:
    def capture_and_close() -> None:
        try:
            screenshots.append(_capture_active(screenshot_dir, name, note, title_contains=title_contains))
        except Exception as exc:
            screenshots.append({"name": name, "note": note, "capture_error": repr(exc)})
        for child in app.winfo_children():
            try:
                title = child.title()
            except Exception:
                continue
            if title_contains in title:
                try:
                    child.destroy()
                except Exception:
                    pass

    app.after(delay_ms, capture_and_close)


def _schedule_tk_entry_dialog_submit(
    app: Any,
    screenshots: list[dict[str, Any]],
    screenshot_dir: Path,
    name: str,
    title_contains: str,
    text: str,
    delay_ms: int = 700,
    note: str = "",
) -> None:
    def fill_and_submit() -> None:
        dialog = None
        for child in app.winfo_children():
            try:
                title = child.title()
            except Exception:
                continue
            if title_contains in title:
                dialog = child
                break
        if dialog is None:
            screenshots.append({"name": name, "note": note, "capture_error": f"dialog not found: {title_contains}"})
            return
        try:
            hwnd = int(win32gui.GetAncestor(int(dialog.winfo_id()), win32con.GA_ROOT))
            screenshots.append(_capture_bbox(hwnd, screenshot_dir / f"{name}.png") | {"name": name, "note": note, "capture_target": "tk_entry_dialog"})
        except Exception as exc:
            screenshots.append({"name": name, "note": note, "capture_error": repr(exc)})
        entry_set = False
        for child in _iter_tk_descendants(dialog):
            try:
                widget_class = str(child.winfo_class())
            except Exception:
                continue
            if widget_class in {"Entry", "TEntry"}:
                try:
                    child.delete(0, "end")
                    child.insert(0, text)
                    entry_set = True
                    break
                except Exception:
                    continue
        if not entry_set:
            screenshots.append({"name": f"{name}_entry_set", "note": note, "entry_set": False})
        if not _invoke_first_tk_button(dialog, prefixes=("확인", "OK")):
            try:
                dialog.event_generate("<Return>")
            except Exception:
                try:
                    dialog.destroy()
                except Exception:
                    pass

    app.after(delay_ms, fill_and_submit)


def _wait_history_idle(app: Any, timeout: float = 15.0) -> None:
    def predicate() -> bool:
        try:
            app._process_history_queue()
        except Exception:
            pass
        return not getattr(app, "history_load_pending", False) and not getattr(app, "history_active_load_pending", False)

    _wait_until(app, predicate, timeout, "history idle")


def _operator_scan(app: Any, value: str, seconds: float = 0.45) -> None:
    app.entry.focus_set()
    try:
        app.entry.focus_force()
    except Exception:
        pass
    app.update()
    app.entry.delete(0, "end")
    app.entry.insert(0, value)
    app.entry.event_generate("<Return>")
    _pump(app, seconds)
    try:
        app.data_manager.flush(timeout=5)
    except Exception:
        pass


def _invoke_and_confirm_dialog(
    app: Any,
    screenshots: list[dict[str, Any]],
    screenshot_dir: Path,
    action: Any,
    name: str,
    text: str | None = None,
    first_delay: float = 0.7,
    second_name: str | None = None,
    second_delay: float = 1.8,
    first_title: str | None = None,
    second_title: str | None = None,
    third_name: str | None = None,
    third_delay: float = 2.8,
    third_title: str | None = None,
) -> None:
    helpers: list[subprocess.Popen[Any]] = []
    first_helper = _schedule_dialog_action(
        screenshots,
        screenshot_dir,
        name,
        delay=first_delay,
        text=text,
        note="operator confirmation/input dialog",
        title_contains=first_title,
    )
    if first_helper is not None:
        helpers.append(first_helper)
    if second_name:
        second_helper = _schedule_dialog_action(
            screenshots,
            screenshot_dir,
            second_name,
            delay=second_delay,
            note="operator confirmation dialog",
            title_contains=second_title,
        )
        if second_helper is not None:
            helpers.append(second_helper)
    if third_name:
        third_helper = _schedule_dialog_action(
            screenshots,
            screenshot_dir,
            third_name,
            delay=third_delay,
            note="operator information dialog",
            title_contains=third_title,
        )
        if third_helper is not None:
            helpers.append(third_helper)
    action()
    for helper in helpers:
        try:
            helper.wait(timeout=5)
        except subprocess.TimeoutExpired:
            helper.kill()
        json_path = None
        try:
            args = helper.args
            if isinstance(args, list) and "--screenshot" in args:
                json_path = Path(args[args.index("--screenshot") + 1] + ".json")
        except Exception:
            json_path = None
        if json_path and json_path.exists():
            try:
                screenshots.append(json.loads(json_path.read_text(encoding="utf-8")))
            except Exception:
                pass
    _pump(app, 0.8)
    try:
        app.data_manager.flush(timeout=5)
    except Exception:
        pass


def _csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _event_counts(rows: list[dict[str, str]], marker: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        blob = json.dumps(row, ensure_ascii=False)
        if marker not in blob:
            continue
        event = row.get("event", "")
        counts[event] = counts.get(event, 0) + 1
    return counts


def _rows_containing(rows: list[dict[str, str]], needle: str) -> list[dict[str, str]]:
    return [row for row in rows if needle in json.dumps(row, ensure_ascii=False)]


def _write_past_row(label_match_module: Any, app: Any, marker: str) -> Path:
    yesterday = datetime.now() - timedelta(days=1)
    path = Path(app.data_manager._get_log_filepath(yesterday))
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    details = {
        "master_label_code": f"PAST-{marker}",
        "item_code": REAL_MASTER,
        "item_name": "PAST_UI_WALKTHROUGH",
        "spec": "E2E",
        "scan_count": 5,
        "scanned_product_barcodes": [
            f"PAST-{marker}",
            f"PRODUCT_PAST-{marker}_1",
            f"PRODUCT_PAST-{marker}_2",
            f"PRODUCT_PAST-{marker}_3",
            f"FINAL_LABEL_PAST-{marker}{GS}6D{yesterday.strftime('%Y%m%d')}",
        ],
        "parsed_product_barcodes": [f"PAST-{marker}"] * 5,
        "final_result": label_match_module.LABEL_MATCH_RESULT_PASS,
        "result_display": label_match_module.LABEL_MATCH_RESULT_PASS,
        "production_date": yesterday.strftime("%Y-%m-%d"),
        "set_id": f"past-{marker}",
        "phase": "-",
    }
    with path.open("a", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        if not exists:
            writer.writerow(["timestamp", "worker_name", "event", "details"])
        writer.writerow([yesterday.isoformat(), app.data_manager.worker_name, label_match_module.Label_Match.Events.TRAY_COMPLETE, json.dumps(details, ensure_ascii=False)])
    return path


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_dir = Path(args.output_dir).resolve()
    screenshot_dir = output_dir / "screenshots"
    data_dir = output_dir / "isolated_data"
    output_dir.mkdir(parents=True, exist_ok=True)
    screenshot_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    os.environ["LABEL_MATCH_SAVE_DIR"] = str(data_dir)
    os.environ.setdefault("PYGAME_HIDE_SUPPORT_PROMPT", "1")

    config_path = ROOT / "config" / "app_settings.json"
    config_backup = config_path.read_bytes() if config_path.exists() else None
    config_restored = False
    config_restore_error = ""

    def restore_config_file() -> bool:
        nonlocal config_restored, config_restore_error
        if config_restored:
            return True
        try:
            if config_backup is not None:
                config_path.write_bytes(config_backup)
            elif config_path.exists():
                config_path.unlink()
            config_restored = True
            return True
        except Exception as exc:
            config_restore_error = repr(exc)
            return False

    import Label_Match as label_match_module

    label_match_module.threaded_update_check = lambda: None

    width, height, x, y = _parse_geometry(args.geometry)
    stamp = datetime.now().strftime("%Y%m%d%H%M%S")
    marker = f"OP_UI_{stamp}"
    today = datetime.now().strftime("%Y%m%d")
    worker_name = f"신규작업자_{marker[-6:]}"
    app = None
    screenshots: list[dict[str, Any]] = []
    actions: list[dict[str, Any]] = []

    try:
        app = label_match_module.Label_Match(run_tests=False)
        app._play_error_siren_loop = lambda: None
        app.state("normal")
        app.geometry(f"{width}x{height}+{x}+{y}")
        app.update()
        _wait_until(app, lambda: getattr(app, "initialized_successfully", False), 25, "app initialized")
        _wait_history_idle(app)
        screenshots.append(_capture_window(app, screenshot_dir, "00_startup_idle", "startup idle as first operator"))

        app.open_settings_window()
        _pump(app, 0.5)
        screenshots.append(_capture_active(screenshot_dir, "01_settings_dialog", "settings dialog visible", title_contains="설정"))
        settings_window = next((w for w in app.winfo_children() if getattr(w, "title", lambda: "")() == "설정"), None)
        if settings_window is not None:
            app.worker_name_var.set(worker_name)
            screenshots.append(_capture_active(screenshot_dir, "02_settings_worker_name_entered", "worker name typed but not saved during evidence run", title_contains="설정"))
            settings_window.destroy()
            _pump(app, 0.6)
            actions.append({"name": "settings_worker_name_dialog_checked", "worker_name_candidate": worker_name})

        values = [
            REAL_MASTER,
            f"PRODUCT_{REAL_MASTER}_1_{marker}",
            f"PRODUCT_{REAL_MASTER}_2_{marker}",
            f"PRODUCT_{REAL_MASTER}_3_{marker}",
            f"FINAL_LABEL_{REAL_MASTER}_{marker}{GS}6D{today}",
        ]
        for index, value in enumerate(values, 1):
            _operator_scan(app, value)
            screenshots.append(_capture_window(app, screenshot_dir, f"03_normal_scan_step_{index}", f"normal real Item.csv master scan step {index}"))
        actions.append({"name": "normal_full_tray_real_master", "master": REAL_MASTER})

        manual_master = "AAA2270740100"
        _operator_scan(app, manual_master)
        _operator_scan(app, f"PRODUCT_{manual_master}_1_{marker}")
        screenshots.append(_capture_window(app, screenshot_dir, "04_manual_complete_ready", "manual complete button should be enabled"))
        _invoke_and_confirm_dialog(
            app,
            screenshots,
            screenshot_dir,
            lambda: app.manual_complete_button.invoke(),
            "05_manual_complete_confirm",
            first_title="수동 완료 확인",
        )
        screenshots.append(_capture_window(app, screenshot_dir, "06_manual_complete_done", "after manual complete confirmation"))
        actions.append({"name": "manual_complete_via_button", "master": manual_master})

        reset_master = "AAA2270750100"
        _operator_scan(app, reset_master)
        _operator_scan(app, f"PRODUCT_{reset_master}_1_{marker}")
        screenshots.append(_capture_window(app, screenshot_dir, "07_current_set_cancel_before", "before current set cancel button"))
        app.event_generate("<F1>")
        _pump(app, 0.5)
        screenshots.append(_capture_window(app, screenshot_dir, "08_current_set_cancel_after", "after current set cancel button"))
        actions.append({"name": "current_set_cancel_via_f1", "master": reset_master})

        cancel_master = "AAA2270760100"
        cancel_values = [
            cancel_master,
            f"PRODUCT_{cancel_master}_1_{marker}",
            f"PRODUCT_{cancel_master}_2_{marker}",
            f"PRODUCT_{cancel_master}_3_{marker}",
            f"FINAL_LABEL_{cancel_master}_{marker}{GS}6D{today}",
        ]
        for value in cancel_values:
            _operator_scan(app, value)
        screenshots.append(_capture_window(app, screenshot_dir, "09_completed_tray_before_cancel", "completed tray exists before F2 cancel"))
        _schedule_tk_entry_dialog_submit(
            app,
            screenshots,
            screenshot_dir,
            "10_completed_tray_cancel_input",
            "완료된 트레이 취소",
            text=cancel_master,
            delay_ms=700,
            note="completed tray cancel input dialog",
        )
        cancel_helpers = [
            _schedule_dialog_action(screenshots, screenshot_dir, "11_completed_tray_cancel_confirm", delay=1.8, title_contains="취소 확인"),
            _schedule_dialog_action(screenshots, screenshot_dir, "11b_completed_tray_cancel_done_info", delay=2.8, title_contains="처리 완료"),
        ]
        app.event_generate("<F2>")
        for helper in [item for item in cancel_helpers if item is not None]:
            try:
                helper.wait(timeout=5)
            except subprocess.TimeoutExpired:
                helper.kill()
            try:
                helper_args = helper.args
                if isinstance(helper_args, list) and "--screenshot" in helper_args:
                    json_path = Path(helper_args[helper_args.index("--screenshot") + 1] + ".json")
                    if json_path.exists():
                        screenshots.append(json.loads(json_path.read_text(encoding="utf-8")))
            except Exception:
                pass
        _pump(app, 0.8)
        screenshots.append(_capture_window(app, screenshot_dir, "12_completed_tray_cancel_after", "after completed tray cancel"))
        actions.append({"name": "completed_tray_cancel_via_f2", "master": cancel_master})

        mismatch_master = "AAA2287560100"
        _operator_scan(app, mismatch_master)
        _operator_scan(app, f"PRODUCT_WRONG_{marker}_LONG_ENOUGH")
        _pump(app, 0.8)
        modal = next((w for w in app.winfo_children() if w.winfo_class() == "Toplevel"), None)
        if modal is not None:
            screenshots.append(_capture_bbox(int(modal.winfo_id()), screenshot_dir / "13_mismatch_app_modal.png") | {"name": "13_mismatch_app_modal", "note": "mismatch app-local modal"})
            if not _invoke_first_tk_button(modal):
                modal.destroy()
        _pump(app, 0.8)
        screenshots.append(_capture_window(app, screenshot_dir, "14_mismatch_after_confirm", "after confirming mismatch modal"))
        actions.append({"name": "mismatch_modal_confirmed", "master": mismatch_master})

        restore_master = REAL_MASTER
        _operator_scan(app, restore_master)
        _operator_scan(app, f"PRODUCT_{restore_master}_RESTORE_1_{marker}")
        screenshots.append(_capture_window(app, screenshot_dir, "15_restore_before_close_partial", "partial set before close"))
        _invoke_and_confirm_dialog(
            app,
            screenshots,
            screenshot_dir,
            app.on_closing,
            "16_close_confirm_partial_set",
            first_title="종료 확인",
        )
        app = None

        restore_thread = _start_dialog_capture_click_thread(
            screenshots,
            screenshot_dir,
            "17_restore_prompt",
            "작업 복구",
            prefixes=("예", "확인", "OK"),
            delay=0.1,
        )
        app = label_match_module.Label_Match(run_tests=False)
        restore_thread.join(timeout=5)
        app._play_error_siren_loop = lambda: None
        app.state("normal")
        app.geometry(f"{width}x{height}+{x}+{y}")
        app.update()
        _wait_until(app, lambda: getattr(app, "initialized_successfully", False), 25, "app reinitialized")
        _wait_history_idle(app)
        screenshots.append(_capture_window(app, screenshot_dir, "18_after_restore", "after accepting restore prompt"))
        _operator_scan(app, f"PRODUCT_{restore_master}_RESTORE_2_{marker}")
        _operator_scan(app, f"PRODUCT_{restore_master}_RESTORE_3_{marker}")
        _operator_scan(app, f"FINAL_LABEL_{restore_master}_RESTORE_{marker}{GS}6D{today}")
        screenshots.append(_capture_window(app, screenshot_dir, "19_restored_set_completed", "restored set completed"))
        actions.append({"name": "restore_prompt_and_complete", "master": restore_master})

        _write_past_row(label_match_module, app, marker)
        _schedule_tk_toplevel_capture_and_close(
            app,
            screenshots,
            screenshot_dir,
            "20_date_picker_dialog",
            "날짜 선택",
            delay_ms=700,
            note="date picker modal visible",
        )
        app.date_search_button.invoke()
        _pump(app, 0.3)
        target_date = datetime.now() - timedelta(days=1)
        app._load_history_and_rebuild_summary(target_date)
        _wait_history_idle(app)
        screenshots.append(_capture_window(app, screenshot_dir, "21_past_history_view", "past history view-only mode"))
        before_scan_rows = len(_rows_containing(_csv_rows(Path(app.data_manager._get_log_filepath())), "PRODUCT_SHOULD_BE_BLOCKED"))
        _operator_scan(app, f"PRODUCT_SHOULD_BE_BLOCKED_{marker}", seconds=0.35)
        after_scan_rows = len(_rows_containing(_csv_rows(Path(app.data_manager._get_log_filepath())), "PRODUCT_SHOULD_BE_BLOCKED"))
        screenshots.append(_capture_window(app, screenshot_dir, "22_past_history_scan_blocked", "scan attempt blocked during past view"))
        app.today_button.invoke()
        _wait_history_idle(app)
        screenshots.append(_capture_window(app, screenshot_dir, "23_today_restored", "today view restored"))
        actions.append({"name": "past_history_view_and_blocked_scan", "blocked_rows_before": before_scan_rows, "blocked_rows_after": after_scan_rows})

        malicious = 'CLC=BAD<script>alert(1)</script>|SPC=../..;DROP TABLE x;=HYPERLINK("http://bad")|PHS=9'
        _operator_scan(app, malicious)
        _pump(app, 0.8)
        modal = next((w for w in app.winfo_children() if w.winfo_class() == "Toplevel"), None)
        if modal is not None:
            screenshots.append(_capture_bbox(int(modal.winfo_id()), screenshot_dir / "24_malicious_input_modal.png") | {"name": "24_malicious_input_modal", "note": "malicious input shown as text in modal"})
            if not _invoke_first_tk_button(modal):
                modal.destroy()
        _pump(app, 0.6)
        screenshots.append(_capture_window(app, screenshot_dir, "25_malicious_input_after_confirm", "after malicious input confirmation"))
        actions.append({"name": "malicious_input_modal_confirmed"})

        try:
            app.data_manager.flush(timeout=5)
        except Exception:
            pass
        log_path = Path(app.data_manager._get_log_filepath())
        rows = _csv_rows(log_path)
        event_counts = _event_counts(rows, marker)
        app.on_closing = lambda: None
        app.destroy()
        app = None
        config_restore_ok = restore_config_file()

        blank = [item for item in screenshots if item.get("blank_suspected")]
        issue_codes: list[str] = []
        if blank:
            issue_codes.append("BLANK_SCREENSHOT_DETECTED")
        if not log_path.exists():
            issue_codes.append("LOG_FILE_MISSING")
        if event_counts.get("TRAY_COMPLETE", 0) < 3:
            issue_codes.append("TRAY_COMPLETE_UNDER_EXPECTED")
        if event_counts.get("SET_CANCELLED", 0) < 1:
            issue_codes.append("SET_CANCELLED_MISSING")
        if event_counts.get("TRAY_COMPLETION_CANCELLED", 0) < 1:
            issue_codes.append("TRAY_COMPLETION_CANCELLED_MISSING")
        if event_counts.get("SET_RESTORED", 0) < 1:
            issue_codes.append("SET_RESTORED_MISSING")
        if event_counts.get("ERROR_MISMATCH", 0) + event_counts.get("ERROR_INPUT", 0) < 1:
            issue_codes.append("ERROR_EVENT_MISSING")

        report = {
            "report_version": "label-match-operator-ui-walkthrough-v1",
            "status": "PASS" if not issue_codes else "REVIEW_REQUIRED",
            "generated_at": datetime.now().isoformat(),
            "host": socket.gethostname(),
            "marker": marker,
            "app_root": str(ROOT),
            "output_dir": str(output_dir),
            "screenshot_dir": str(screenshot_dir),
            "data_dir": str(data_dir),
            "log_path": str(log_path),
            "log_exists": log_path.exists(),
            "log_sha256": _sha256_file(log_path) if log_path.exists() else "",
            "event_counts": event_counts,
            "malicious_rows": _rows_containing(rows, "DROP TABLE"),
            "actions": actions,
            "screenshots": screenshots,
            "issue_codes": issue_codes,
            "geometry": args.geometry,
            "uses_run_tests_false": True,
            "entry_return_binding_used": True,
            "config_restored": config_restore_ok,
            "config_restore_error": config_restore_error,
        }
        report_path = output_dir / "label_match_operator_ui_walkthrough_report.json"
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        return report
    finally:
        if app is not None:
            try:
                app.destroy()
            except Exception:
                pass
        if not config_restored:
            restore_config_file()


def _parse_geometry(value: str) -> tuple[int, int, int, int]:
    size, rest = value.lower().split("x", 1)
    width = int(size)
    height_text, x_text, y_text = rest.split("+", 2)
    return width, int(height_text), int(x_text), int(y_text)


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "--dialog-helper":
        return _dialog_helper_main(sys.argv[2:])
    parser = argparse.ArgumentParser(description="Run operator-style Label_Match UI walkthrough")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--geometry", default="1280x900+900+-1390")
    args = parser.parse_args()
    report = run(args)
    report_path = Path(args.output_dir).resolve() / "label_match_operator_ui_walkthrough_report.json"
    print(json.dumps({"status": report.get("status"), "report_path": str(report_path), "marker": report.get("marker"), "issue_codes": report.get("issue_codes", [])}, ensure_ascii=False, indent=2))
    return 0 if report.get("status") == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())

"""Fail-closed Label_Match walkthrough through the real Tk Return binding.

The parent process launches two child processes against one run-scoped data
directory.  Phase one exercises normal completion and inline error recovery,
then leaves a two-scan set on disk.  Phase two proves restart restoration and
finishes that set.  The application runs with ``run_tests=False``; only
external integrations and settings/item fixtures are replaced by this
harness.  Every scan still follows::

    ttk.Entry <Return> -> _handle_scan_enter -> Label_Match.process_input

No production ProgramData, configuration, audio, updater, logistics endpoint,
or direct-sync process is used.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import socket
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from PIL import Image, ImageGrab, ImageStat


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = Path(__file__).resolve()
REPORT_NAME = "label_match_actual_input_walkthrough_report.json"
PHASE_ONE_REPORT_NAME = "phase_one_actual_input_report.json"
PHASE_TWO_REPORT_NAME = "phase_two_restart_report.json"
STATE_FILE_NAME = "_current_set_state_packaging.json"
REPORT_VERSION = "label-match-actual-input-walkthrough-v2"
INPUT_MODE = "entry_insert_plus_tk_return_event"
TOTAL_SCAN_COUNT = 5
NEAR_BLACK_LUMA = 16
NEAR_BLACK_FAILURE_RATIO = 0.08
BLACK_LINE_COVERAGE_RATIO = 0.80
BLACK_STRIPE_FAILURE_RATIO = 0.12
BLACK_TILE_FAILURE_RATIO = 0.25
BLACK_EDGE_BAND_FAILURE_RATIO = 0.15
LOW_VARIANCE_STDDEV_MAX = 2.0
DOMINANT_COLOR_RATIO_MAX = 0.997
TILE_COLUMNS = 12
TILE_ROWS = 8
EXPECTED_SCREENSHOT_NAMES = (
    "01_happy_complete",
    "02_duplicate_warning",
    "03_duplicate_after_ack",
    "04_mismatch_warning",
    "05_mismatch_after_ack",
    "06_recovery_before_restart",
    "07_restart_restored",
    "08_restart_completed",
)
EXPECTED_EVENT_COUNTS = {
    "SCAN_ATTEMPT": 15,
    "SCAN_OK": 13,
    "ERROR_INPUT": 1,
    "ERROR_MISMATCH": 1,
    "TRAY_COMPLETE": 4,
    "SET_RESTORED": 1,
}
EXPECTED_TRAY_RESULT_COUNTS = {"통과": 2, "입력오류": 1, "불일치": 1}
REQUIRED_PHASE_CHECKS = {
    "phase_one": {
        "app_uses_run_tests_false",
        "save_directory_is_exact_isolated_dir",
        "entry_return_binding_present",
        "happy_path_has_five_central_values",
        "duplicate_warning_is_inline",
        "duplicate_keeps_two_accepted_rows",
        "duplicate_keeps_last_normal_central_row",
        "duplicate_ack_uses_return_without_rescan",
        "duplicate_ack_restores_entry_focus",
        "duplicate_ack_retains_central_rows",
        "duplicate_ack_resets_business_current_set",
        "mismatch_warning_is_inline",
        "mismatch_keeps_master_row",
        "mismatch_keeps_last_normal_central_row",
        "mismatch_starts_as_independent_current_set",
        "mismatch_ack_restores_entry_focus",
        "mismatch_ack_retains_master_row",
        "mismatch_ack_resets_business_current_set",
        "partial_state_contains_two_real_scans",
        "phase_one_process_input_count_exact",
        "every_dispatch_reached_original_process_input",
        "network_attempt_count_zero",
        "runtime_executable_guard_below_data_dir",
        "runtime_trace_confined_to_guard",
        "external_runtime_trace_unchanged",
        "all_screenshots_strict_pixel_gate",
    },
    "phase_two": {
        "app_uses_run_tests_false",
        "save_directory_is_exact_isolated_dir",
        "entry_return_binding_present",
        "restore_prompt_seen_once",
        "restored_flag_visible",
        "restored_central_rows_exact",
        "restored_last_normal_central_row",
        "restored_entry_has_focus",
        "restored_set_completes_all_five_rows",
        "restored_set_state_file_deleted",
        "phase_two_process_input_count_exact",
        "every_dispatch_reached_original_process_input",
        "network_attempt_count_zero",
        "runtime_executable_guard_below_data_dir",
        "runtime_trace_confined_to_guard",
        "external_runtime_trace_unchanged",
        "all_screenshots_strict_pixel_gate",
    },
}

INTEGRATION_ENV_PREFIXES = (
    "LABEL_MATCH_DIRECT_SYNC_",
    "LABEL_MATCH_LOGISTICS_",
    "WORKER_ANALYSIS_LOGISTICS_",
    "LABEL_MATCH_UPDATE_",
)
INTEGRATION_ENV_NAMES = {
    "WORKER_ANALYSIS_SERVER_URL",
    "WORKER_ANALYSIS_API_URL",
    "UPDATE_PROVIDER",
}


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _file_fingerprint(path: Path) -> dict[str, Any]:
    resolved = path.resolve()
    exists = resolved.is_file()
    return {
        "path": str(resolved),
        "exists": exists,
        "size": resolved.stat().st_size if exists else 0,
        "sha256": _sha256_file(resolved) if exists else "",
    }


def _is_descendant(path: Path, parent: Path) -> bool:
    resolved = path.resolve()
    resolved_parent = parent.resolve()
    return resolved != resolved_parent and resolved.is_relative_to(resolved_parent)


def _assert_descendant(path: Path, parent: Path, label: str) -> Path:
    resolved = path.resolve()
    if not _is_descendant(resolved, parent):
        raise RuntimeError(f"{label} must be below {parent.resolve()}: {resolved}")
    return resolved


def _parse_geometry(value: str) -> tuple[int, int, int, int]:
    import re

    match = re.fullmatch(r"\s*(\d+)x(\d+)([+-]\d+)([+-]\d+)\s*", value)
    if not match:
        raise ValueError(
            f"invalid geometry {value!r}; expected WIDTHxHEIGHT+X+Y"
        )
    width, height, x, y = match.groups()
    return int(width), int(height), int(x), int(y)


def synthetic_fixture(marker: str) -> dict[str, Any]:
    """Return deterministic synthetic values accepted by the real parser."""

    today = datetime.now().strftime("%Y%m%d")

    def set_values(kind: str) -> list[str]:
        master = f"VALID-{marker}-{kind}-MASTER"
        return [
            master,
            f"{master}-PRODUCT-1",
            f"{master}-PRODUCT-2",
            f"{master}-PRODUCT-3",
            f"{master}-FINAL-LABEL<GS>6D{today}",
        ]

    duplicate = set_values("DUPLICATE")
    mismatch = set_values("MISMATCH")
    recovery = set_values("RECOVERY")
    wrong_product = f"WRONG-PRODUCT-{marker}-" + ("X" * len(mismatch[0]))
    return {
        "happy": set_values("HAPPY"),
        "duplicate": duplicate,
        "mismatch": mismatch,
        "wrong_product": wrong_product,
        "recovery": recovery,
    }


def build_child_environment(
    base_environment: Mapping[str, str],
    data_dir: Path,
    marker: str,
) -> dict[str, str]:
    """Build a subprocess-only environment with all runtime writes isolated."""

    resolved_data = data_dir.resolve()
    temp_dir = resolved_data / "temp"
    program_data = resolved_data / "program_data_guard"
    local_app_data = resolved_data / "local_app_data_guard"
    roaming_app_data = resolved_data / "roaming_app_data_guard"
    direct_sync_root = resolved_data / "direct_sync_guard"
    for path in (
        resolved_data,
        temp_dir,
        program_data,
        local_app_data,
        roaming_app_data,
        direct_sync_root,
    ):
        path.mkdir(parents=True, exist_ok=True)

    env = {str(key): str(value) for key, value in base_environment.items()}
    for key in tuple(env):
        if key in INTEGRATION_ENV_NAMES or any(
            key.startswith(prefix) for prefix in INTEGRATION_ENV_PREFIXES
        ):
            env.pop(key, None)

    env.update(
        {
            "LABEL_MATCH_SAVE_DIR": str(resolved_data),
            "LABEL_MATCH_AUTOMATED_TEST": "1",
            "LABEL_MATCH_AUDIO_ENABLED": "off",
            "LABEL_MATCH_DIRECT_SYNC_BOOTSTRAP": "off",
            "LABEL_MATCH_SESSION_SYNC_TRIGGER": "off",
            "LABEL_MATCH_UPDATE_PROVIDER": "off",
            "LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT": str(direct_sync_root),
            "LABEL_MATCH_DIRECT_SYNC_SOURCE_HOST_ID": f"synthetic-{marker}",
            "LABEL_MATCH_DIRECT_SYNC_TASK_NAME": f"Synthetic-{marker}",
            "LABEL_MATCH_DIRECT_SYNC_SERVER_BASE_URL": "https://invalid.invalid",
            "KMTECH_TEST_SILENT_AUDIO": "1",
            "SDL_AUDIODRIVER": "dummy",
            "PYGAME_HIDE_SUPPORT_PROMPT": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONUTF8": "1",
            "TEMP": str(temp_dir),
            "TMP": str(temp_dir),
            "ProgramData": str(program_data),
            "LOCALAPPDATA": str(local_app_data),
            "APPDATA": str(roaming_app_data),
        }
    )
    env.pop("PYTEST_CURRENT_TEST", None)
    return env


def _git_status() -> dict[str, Any]:
    completed = subprocess.run(
        ["git", "status", "--porcelain=v1", "--untracked-files=all", "-z"],
        cwd=str(ROOT),
        check=False,
        capture_output=True,
    )
    raw = completed.stdout
    return {
        "returncode": completed.returncode,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "entry_count": raw.count(b"\x00"),
    }


def _pump(app: Any, milliseconds: int = 180) -> None:
    deadline = time.monotonic() + max(0, milliseconds) / 1000.0
    while time.monotonic() < deadline:
        app.update()
        time.sleep(0.01)
    app.update_idletasks()
    app.update()


def _wait_until(
    app: Any,
    predicate: Any,
    *,
    timeout: float = 20.0,
    label: str,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        app.update()
        if predicate():
            _pump(app, 120)
            return
        time.sleep(0.02)
    raise TimeoutError(f"timed out waiting for {label}")


def _widget_role(app: Any, widget: Any) -> str:
    if widget is None:
        return "none"
    if widget is getattr(app, "entry", None):
        return "scan_entry"
    if widget is getattr(app, "workflow_notice_action_button", None):
        return "notice_action_button"
    return f"{widget.winfo_class()}:{str(widget)}"


def _tree_rows(app: Any) -> list[dict[str, str]]:
    tree = app.qa_scan_tree
    rows: list[dict[str, str]] = []
    for iid in tree.get_children():
        values = tuple(str(value) for value in (tree.item(iid, "values") or ()))
        rows.append(
            {
                "iid": str(iid),
                "stage": values[0] if len(values) > 0 else "",
                "value": values[1] if len(values) > 1 else "",
                "state": values[2] if len(values) > 2 else "",
            }
        )
    return rows


def _filled_values(rows: Iterable[Mapping[str, str]]) -> list[str]:
    return [
        str(row.get("value") or "")
        for row in rows
        if str(row.get("value") or "") not in {"", "-"}
    ]


def _snapshot(app: Any, name: str, process_calls: list[dict[str, Any]]) -> dict[str, Any]:
    app.update_idletasks()
    rows = _tree_rows(app)
    filled = _filled_values(rows)
    view = getattr(app, "_last_workflow_view", None)
    notice = getattr(view, "notice", None) if view is not None else None
    return {
        "name": name,
        "pid": os.getpid(),
        "current_raw": list((getattr(app, "current_set_info", {}) or {}).get("raw") or []),
        "central_rows": rows,
        "central_filled_values": filled,
        "central_last_filled_value": filled[-1] if filled else "",
        "focus_role": _widget_role(app, app.focus_get()),
        "entry_state": str(app.entry.cget("state")),
        "entry_value": str(app.entry.get()),
        "process_input_call_count": len(process_calls),
        "view": {
            "current_stage": str(getattr(view, "current_stage", "")),
            "qa_completed": int(getattr(view, "qa_completed", 0) or 0),
            "qa_progress_text": str(getattr(view, "qa_progress_text", "")),
            "scan_input_enabled": bool(getattr(view, "scan_input_enabled", False)),
            "last_normal_scan": str(getattr(view, "last_normal_scan", "") or ""),
            "notice_title": str(getattr(notice, "title", "") or ""),
            "notice_message": str(getattr(notice, "message", "") or ""),
            "notice_kind": str(getattr(notice, "kind", "") or ""),
        },
    }


def _safe_hwnd(app: Any) -> tuple[int, int]:
    hwnd = int(app.winfo_id())
    pid = os.getpid()
    if os.name == "nt":
        import win32con
        import win32gui
        import win32process

        hwnd = int(win32gui.GetAncestor(hwnd, win32con.GA_ROOT))
        _thread_id, pid = win32process.GetWindowThreadProcessId(hwnd)
    return hwnd, int(pid)


def _longest_true_run(values: Iterable[bool]) -> int:
    longest = 0
    current = 0
    for value in values:
        if value:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def _edge_true_run(values: list[bool]) -> int:
    leading = 0
    for value in values:
        if not value:
            break
        leading += 1
    trailing = 0
    for value in reversed(values):
        if not value:
            break
        trailing += 1
    return max(leading, trailing)


def _near_black_ratio(image: Image.Image) -> float:
    gray = image.convert("L")
    pixels = max(1, gray.width * gray.height)
    return sum(gray.histogram()[: NEAR_BLACK_LUMA + 1]) / pixels


def analyze_capture_image(
    image: Image.Image,
    *,
    expected_size: tuple[int, int] | None = None,
) -> dict[str, Any]:
    """Detect blank, uniform, striped, tiled, and edge-black captures."""

    rgb = image.convert("RGB")
    gray = rgb.convert("L")
    histogram = gray.histogram()
    pixel_count = max(1, gray.width * gray.height)
    exact_black_ratio = histogram[0] / pixel_count
    near_black_ratio = sum(histogram[: NEAR_BLACK_LUMA + 1]) / pixel_count
    extrema = gray.getextrema() or (0, 0)
    stat = ImageStat.Stat(gray)
    luma_stddev = float(stat.stddev[0])

    mask = gray.point([255 if value <= NEAR_BLACK_LUMA else 0 for value in range(256)])

    def line_flags(source: Image.Image) -> list[bool]:
        raw = source.tobytes()
        width = max(1, source.width)
        required = int(width * BLACK_LINE_COVERAGE_RATIO + 0.999999)
        return [
            raw[offset : offset + width].count(255) >= required
            for offset in range(0, len(raw), width)
        ]

    row_flags = line_flags(mask)
    column_flags = line_flags(mask.transpose(Image.Transpose.TRANSPOSE))
    longest_row_ratio = _longest_true_run(row_flags) / max(1, gray.height)
    longest_column_ratio = _longest_true_run(column_flags) / max(1, gray.width)
    edge_row_ratio = _edge_true_run(row_flags) / max(1, gray.height)
    edge_column_ratio = _edge_true_run(column_flags) / max(1, gray.width)

    tile_ratios: list[float] = []
    for tile_y in range(TILE_ROWS):
        top = gray.height * tile_y // TILE_ROWS
        bottom = gray.height * (tile_y + 1) // TILE_ROWS
        for tile_x in range(TILE_COLUMNS):
            left = gray.width * tile_x // TILE_COLUMNS
            right = gray.width * (tile_x + 1) // TILE_COLUMNS
            tile_ratios.append(
                _near_black_ratio(gray.crop((left, top, right, bottom)))
            )
    maximum_tile_ratio = max(tile_ratios, default=1.0)

    edge_width = max(1, int(gray.width * 0.05))
    edge_height = max(1, int(gray.height * 0.05))
    edge_band_ratios = {
        "top": _near_black_ratio(gray.crop((0, 0, gray.width, edge_height))),
        "bottom": _near_black_ratio(
            gray.crop((0, gray.height - edge_height, gray.width, gray.height))
        ),
        "left": _near_black_ratio(gray.crop((0, 0, edge_width, gray.height))),
        "right": _near_black_ratio(
            gray.crop((gray.width - edge_width, 0, gray.width, gray.height))
        ),
    }
    maximum_edge_band_ratio = max(edge_band_ratios.values(), default=1.0)

    sample = rgb.copy()
    sample.thumbnail((256, 256), Image.Resampling.NEAREST)
    colors = sample.getcolors(maxcolors=max(1, sample.width * sample.height)) or []
    dominant_color_ratio = max((count for count, _color in colors), default=0) / max(
        1, sample.width * sample.height
    )

    blank_suspected = bool(extrema[0] == extrema[1])
    excess_black_suspected = near_black_ratio > NEAR_BLACK_FAILURE_RATIO
    edge_black_stripe_suspected = bool(
        edge_row_ratio >= BLACK_STRIPE_FAILURE_RATIO
        or edge_column_ratio >= BLACK_STRIPE_FAILURE_RATIO
        or maximum_edge_band_ratio >= BLACK_EDGE_BAND_FAILURE_RATIO
    )
    contiguous_black_stripe_suspected = bool(
        longest_row_ratio >= BLACK_STRIPE_FAILURE_RATIO
        or longest_column_ratio >= BLACK_STRIPE_FAILURE_RATIO
    )
    black_tile_suspected = maximum_tile_ratio >= BLACK_TILE_FAILURE_RATIO
    uniform_low_variance_suspected = bool(
        luma_stddev <= LOW_VARIANCE_STDDEV_MAX
        or dominant_color_ratio >= DOMINANT_COLOR_RATIO_MAX
    )
    pixel_size_matches = (
        expected_size is None or (rgb.width, rgb.height) == tuple(expected_size)
    )
    valid = bool(
        pixel_size_matches
        and not blank_suspected
        and not excess_black_suspected
        and not edge_black_stripe_suspected
        and not contiguous_black_stripe_suspected
        and not black_tile_suspected
        and not uniform_low_variance_suspected
    )
    return {
        "pixel_size": [rgb.width, rgb.height],
        "expected_pixel_size": list(expected_size) if expected_size else None,
        "pixel_size_matches": pixel_size_matches,
        "blank_suspected": blank_suspected,
        "excess_black_suspected": excess_black_suspected,
        "edge_black_stripe_suspected": edge_black_stripe_suspected,
        "contiguous_black_stripe_suspected": contiguous_black_stripe_suspected,
        "black_tile_suspected": black_tile_suspected,
        "uniform_low_variance_suspected": uniform_low_variance_suspected,
        "capture_pixels_valid": valid,
        "exact_black_ratio": round(exact_black_ratio, 6),
        "near_black_threshold_luma": NEAR_BLACK_LUMA,
        "near_black_ratio": round(near_black_ratio, 6),
        "near_black_failure_ratio": NEAR_BLACK_FAILURE_RATIO,
        "luma_mean": round(float(stat.mean[0]), 3),
        "luma_stddev": round(luma_stddev, 3),
        "low_variance_stddev_threshold": LOW_VARIANCE_STDDEV_MAX,
        "dominant_color_ratio_sampled": round(dominant_color_ratio, 6),
        "dominant_color_ratio_threshold": DOMINANT_COLOR_RATIO_MAX,
        "black_line_coverage_threshold": BLACK_LINE_COVERAGE_RATIO,
        "black_stripe_failure_ratio": BLACK_STRIPE_FAILURE_RATIO,
        "longest_near_black_row_run_ratio": round(longest_row_ratio, 6),
        "longest_near_black_column_run_ratio": round(longest_column_ratio, 6),
        "edge_near_black_row_run_ratio": round(edge_row_ratio, 6),
        "edge_near_black_column_run_ratio": round(edge_column_ratio, 6),
        "tile_grid": [TILE_COLUMNS, TILE_ROWS],
        "maximum_tile_near_black_ratio": round(maximum_tile_ratio, 6),
        "black_tile_failure_ratio": BLACK_TILE_FAILURE_RATIO,
        "edge_band_near_black_ratios": {
            key: round(value, 6) for key, value in edge_band_ratios.items()
        },
        "maximum_edge_band_near_black_ratio": round(
            maximum_edge_band_ratio, 6
        ),
        "black_edge_band_failure_ratio": BLACK_EDGE_BAND_FAILURE_RATIO,
        "grayscale_extrema": [int(extrema[0]), int(extrema[1])],
    }


def _capture(app: Any, screenshot_dir: Path, name: str) -> dict[str, Any]:
    screenshot_dir.mkdir(parents=True, exist_ok=True)
    _pump(app, 160)
    try:
        app.lift()
        app.focus_force()
    except Exception:
        pass
    _pump(app, 80)
    hwnd, hwnd_pid = _safe_hwnd(app)
    path = screenshot_dir / f"{name}.png"
    capture_method = "window_hwnd"
    capture_error = ""
    try:
        image = ImageGrab.grab(
            window=hwnd,
            include_layered_windows=True,
            all_screens=True,
        )
    except Exception as exc:
        capture_method = "client_bbox_fallback"
        capture_error = f"{exc.__class__.__name__}: {exc}"
        x, y = int(app.winfo_rootx()), int(app.winfo_rooty())
        width, height = max(1, int(app.winfo_width())), max(1, int(app.winfo_height()))
        image = ImageGrab.grab(
            bbox=(x, y, x + width, y + height),
            include_layered_windows=True,
            all_screens=True,
        )
    image = image.convert("RGB")
    image.save(path)
    expected_size = (int(app.winfo_width()), int(app.winfo_height()))
    pixel_metrics = analyze_capture_image(image, expected_size=expected_size)
    return {
        "name": name,
        "path": str(path.resolve()),
        "sha256": _sha256_file(path),
        "width": image.width,
        "height": image.height,
        "capture_method": capture_method,
        "capture_error": capture_error,
        "hwnd": hwnd,
        "hwnd_pid": hwnd_pid,
        "process_pid": os.getpid(),
        "hwnd_pid_matches_process": hwnd_pid == os.getpid(),
        **pixel_metrics,
    }


def _install_process_input_probe(app: Any) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []
    original = app.process_input

    def probed(event: Any = None) -> Any:
        calls.append(
            {
                "sequence": len(calls) + 1,
                "pid": os.getpid(),
                "entry_value_before": str(app.entry.get()),
                "event_type": str(getattr(event, "type", "")),
                "event_widget_is_entry": getattr(event, "widget", None) is app.entry,
            }
        )
        return original(event)

    app.process_input = probed
    return calls


def _send_scan(app: Any, value: str, calls: list[dict[str, Any]]) -> dict[str, Any]:
    if str(app.entry.cget("state")) != "normal":
        raise RuntimeError("scan entry is not enabled before Return dispatch")
    before = len(calls)
    app.entry.focus_set()
    _pump(app, 50)
    app.entry.delete(0, "end")
    app.entry.insert(0, value)
    app.entry.event_generate("<Return>", when="tail")
    _wait_until(
        app,
        lambda: len(calls) == before + 1,
        timeout=4.0,
        label="one process_input invocation",
    )
    if app.entry.get():
        raise RuntimeError("process_input did not clear the scan entry")
    return calls[-1]


def _acknowledge_notice(app: Any, calls: list[dict[str, Any]]) -> dict[str, Any]:
    button = app.workflow_notice_action_button
    before_calls = len(calls)
    button.focus_set()
    _pump(app, 60)
    focus_before = _widget_role(app, app.focus_get())
    button.event_generate("<Return>", when="tail")
    _wait_until(
        app,
        lambda: not bool(
            app.__dict__.get("_pending_workflow_error")
            or app.__dict__.get("_workflow_pending_error")
        ),
        timeout=5.0,
        label="inline warning acknowledgement",
    )
    # Do not repair focus in the harness.  Give the application's own
    # callbacks a short settling window, then report the real focus owner.
    focus_deadline = time.monotonic() + 1.0
    while time.monotonic() < focus_deadline and app.focus_get() is None:
        _pump(app, 40)
    return {
        "event": "<Return>",
        "focus_before": focus_before,
        "focus_after": _widget_role(app, app.focus_get()),
        "process_calls_before": before_calls,
        "process_calls_after": len(calls),
        "ack_did_not_resubmit_scan": len(calls) == before_calls,
    }


class _NetworkBlocker:
    def __init__(self) -> None:
        self.attempts: list[dict[str, str]] = []

    def install(self) -> None:
        import urllib.request

        import requests

        blocker = self

        def blocked_socket(sock: Any, address: Any) -> Any:
            blocker.attempts.append({"api": "socket.connect", "target": repr(address)})
            raise RuntimeError("network is forbidden in actual-input walkthrough")

        def blocked_socket_ex(sock: Any, address: Any) -> int:
            blocker.attempts.append({"api": "socket.connect_ex", "target": repr(address)})
            raise RuntimeError("network is forbidden in actual-input walkthrough")

        def blocked_request(session: Any, method: str, url: str, **kwargs: Any) -> Any:
            blocker.attempts.append({"api": "requests", "target": str(url)})
            raise RuntimeError("network is forbidden in actual-input walkthrough")

        def blocked_urlopen(url: Any, *args: Any, **kwargs: Any) -> Any:
            blocker.attempts.append({"api": "urlopen", "target": repr(url)})
            raise RuntimeError("network is forbidden in actual-input walkthrough")

        socket.socket.connect = blocked_socket
        socket.socket.connect_ex = blocked_socket_ex
        requests.sessions.Session.request = blocked_request
        urllib.request.urlopen = blocked_urlopen


def _read_event_rows(data_dir: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for path in sorted(data_dir.glob("포장실작업이벤트로그_*.csv")):
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            rows.extend(dict(row) for row in csv.DictReader(handle))
    return rows


def _event_counts(data_dir: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in _read_event_rows(data_dir):
        event = str(row.get("event") or "")
        counts[event] = counts.get(event, 0) + 1
    return dict(sorted(counts.items()))


def _tray_result_counts(data_dir: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in _read_event_rows(data_dir):
        if str(row.get("event") or "") != "TRAY_COMPLETE":
            continue
        try:
            details = json.loads(str(row.get("details") or "{}"))
        except json.JSONDecodeError:
            result = "<INVALID_JSON>"
        else:
            result = str(details.get("final_result") or "<MISSING>")
        counts[result] = counts.get(result, 0) + 1
    return dict(sorted(counts.items()))


def _state_payload(data_dir: Path) -> dict[str, Any]:
    path = data_dir / STATE_FILE_NAME
    return _read_json(path) if path.exists() else {}


def _app_settings(data_dir: Path, marker: str) -> dict[str, Any]:
    return {
        "custom_save_path": str(data_dir.resolve()),
        "worker_name": f"합성작업자-{marker}",
        "ui_settings": {
            "default_font": "Malgun Gothic",
            "base_font_size": 14,
        },
        "ui_persistence": {"scale_factor": 1.0, "tree_font_size": 13},
        "colors": {},
        "sound_files": {},
        "update_settings": {"provider": "off"},
    }


def _make_app(
    module: Any,
    data_dir: Path,
    marker: str,
    integration_calls: list[dict[str, Any]],
) -> Any:
    settings = _app_settings(data_dir, marker)
    fixture = synthetic_fixture(marker)
    masters = [values[0] for key, values in fixture.items() if isinstance(values, list)]
    synthetic_items = {
        master: {"Item Name": f"SYNTHETIC {index}", "Spec": "TEST ONLY"}
        for index, master in enumerate(masters, 1)
    }

    class WalkthroughLabelMatch(module.Label_Match):
        def _load_app_settings(self) -> dict[str, Any]:
            return json.loads(json.dumps(settings, ensure_ascii=False))

        def _save_app_settings(self) -> None:
            integration_calls.append({"name": "settings_save_suppressed"})

        def _load_items_data(self) -> dict[str, Any]:
            return json.loads(json.dumps(synthetic_items, ensure_ascii=False))

        def _start_direct_sync_auto_bootstrap(self) -> None:
            integration_calls.append({"name": "direct_sync_bootstrap_suppressed"})
            return None

        def _start_package_outbox_drain(self) -> None:
            integration_calls.append({"name": "package_outbox_drain_suppressed"})
            return None

    return WalkthroughLabelMatch(run_tests=False)


def _install_external_guards(module: Any, integration_calls: list[dict[str, Any]]) -> None:
    class SuppressedThread:
        def is_alive(self) -> bool:
            return False

        def join(self, timeout: float | None = None) -> None:
            return None

    def update_suppressed() -> None:
        integration_calls.append({"name": "update_check_suppressed"})

    def session_sync_suppressed(context: Any, *, reason: str = "TRAY_COMPLETE") -> Any:
        integration_calls.append(
            {"name": "session_direct_sync_suppressed", "reason": reason}
        )
        return SuppressedThread()

    module.threaded_update_check = update_suppressed
    module._label_match_start_session_direct_sync = session_sync_suppressed


def _configure_app_geometry(app: Any, geometry: str) -> None:
    width, height, x, y = _parse_geometry(geometry)
    app.state("normal")
    app.resizable(True, True)
    app.geometry(f"{width}x{height}{x:+d}{y:+d}")
    _pump(app, 350)


def _close_app(app: Any) -> None:
    manager = getattr(app, "data_manager", None)
    if manager is not None:
        manager.flush(timeout=8)
        manager.close(timeout=8)
    try:
        app.destroy()
    except Exception:
        pass


def _check(
    checks: list[dict[str, Any]],
    name: str,
    passed: bool,
    actual: Any = None,
    expected: Any = None,
) -> None:
    checks.append(
        {
            "name": name,
            "status": "PASS" if bool(passed) else "FAIL",
            "actual": actual,
            "expected": expected,
        }
    )


def _run_child_phase(
    phase: str,
    data_dir: Path,
    report_path: Path,
    marker: str,
    geometry: str,
) -> int:
    data_dir = data_dir.resolve()
    report_path = report_path.resolve()
    screenshot_dir = report_path.parent / "screenshots"
    phase_started = datetime.now().isoformat()
    checks: list[dict[str, Any]] = []
    snapshots: list[dict[str, Any]] = []
    screenshots: list[dict[str, Any]] = []
    dispatches: list[dict[str, Any]] = []
    acknowledgements: list[dict[str, Any]] = []
    integration_calls: list[dict[str, Any]] = []
    process_calls: list[dict[str, Any]] = []
    prompt_calls: list[dict[str, Any]] = []
    blocker = _NetworkBlocker()
    app = None
    error = ""
    fixture = synthetic_fixture(marker)
    state_path = data_dir / STATE_FILE_NAME
    original_runtime_executable = Path(sys.executable).resolve()
    runtime_executable_guard = _assert_descendant(
        data_dir / "python_runtime_guard" / original_runtime_executable.name,
        data_dir,
        "runtime executable guard",
    )
    runtime_executable_guard.parent.mkdir(parents=True, exist_ok=True)
    external_runtime_trace_path = (
        original_runtime_executable.parent
        / "startup-trace"
        / f"Label_Match-startup-{os.getpid()}.log"
    )
    external_runtime_trace_before = _file_fingerprint(
        external_runtime_trace_path
    )
    guarded_runtime_trace_path = (
        runtime_executable_guard.parent
        / "startup-trace"
        / f"Label_Match-startup-{os.getpid()}.log"
    )
    runtime_isolation: dict[str, Any] = {}
    # Label_Match writes startup traces beside sys.executable in addition to
    # ProgramData/LOCALAPPDATA/TEMP.  Keep that fallback inside this run while
    # the real application module and window are alive.
    sys.executable = str(runtime_executable_guard)

    try:
        if str(Path(os.environ.get("LABEL_MATCH_SAVE_DIR", "")).resolve()) != str(data_dir):
            raise RuntimeError("LABEL_MATCH_SAVE_DIR does not match child data directory")
        if os.environ.get("LABEL_MATCH_AUDIO_ENABLED") != "off":
            raise RuntimeError("audio guard is not active")
        if os.environ.get("LABEL_MATCH_DIRECT_SYNC_BOOTSTRAP") != "off":
            raise RuntimeError("direct-sync bootstrap guard is not active")
        if os.environ.get("LABEL_MATCH_SESSION_SYNC_TRIGGER") != "off":
            raise RuntimeError("session-sync guard is not active")

        if str(ROOT) not in sys.path:
            sys.path.insert(0, str(ROOT))
        import Label_Match as module

        _install_external_guards(module, integration_calls)
        blocker.install()

        def unexpected_prompt(title: str, message: str, **kwargs: Any) -> Any:
            prompt_calls.append(
                {"api": "unexpected", "title": str(title), "message": str(message)}
            )
            raise RuntimeError(f"unexpected modal prompt: {title}")

        def restore_prompt(title: str, message: str, **kwargs: Any) -> bool:
            prompt_calls.append(
                {
                    "api": "askyesno",
                    "title": str(title),
                    "message": str(message),
                    "decision": True,
                }
            )
            if phase != "phase2" or str(title) != "작업 복구":
                raise RuntimeError(f"unexpected yes/no prompt: {title}")
            return True

        module.messagebox.askyesno = restore_prompt
        module.messagebox.askyesnocancel = unexpected_prompt
        module.messagebox.showinfo = unexpected_prompt
        module.messagebox.showwarning = unexpected_prompt
        module.messagebox.showerror = unexpected_prompt

        app = _make_app(module, data_dir, marker, integration_calls)
        _configure_app_geometry(app, geometry)
        _wait_until(
            app,
            lambda: bool(getattr(app, "initialized_successfully", False)),
            timeout=25,
            label="Label_Match initialization",
        )
        _wait_until(
            app,
            lambda: not bool(getattr(app, "history_load_pending", False))
            and not bool(getattr(app, "history_active_load_pending", False)),
            timeout=20,
            label="history load completion",
        )
        process_calls = _install_process_input_probe(app)
        return_binding = str(app.entry.bind("<Return>") or "")
        hwnd, hwnd_pid = _safe_hwnd(app)
        _check(checks, "app_uses_run_tests_false", app.run_tests is False, app.run_tests, False)
        _check(
            checks,
            "save_directory_is_exact_isolated_dir",
            Path(app.save_directory).resolve() == data_dir,
            str(Path(app.save_directory).resolve()),
            str(data_dir),
        )
        _check(checks, "entry_return_binding_present", bool(return_binding), bool(return_binding), True)
        _check(checks, "window_hwnd_belongs_to_child_pid", hwnd_pid == os.getpid(), hwnd_pid, os.getpid())
        _check(checks, "package_logistics_client_absent", app.package_logistics_client is None, app.package_logistics_client is None, True)

        if phase == "phase1":
            _check(checks, "phase_one_starts_without_state", not state_path.exists(), state_path.exists(), False)
            for value in fixture["happy"]:
                dispatches.append(_send_scan(app, value, process_calls))
            happy = _snapshot(app, "happy_complete", process_calls)
            snapshots.append(happy)
            screenshots.append(_capture(app, screenshot_dir, "01_happy_complete"))
            _check(checks, "happy_path_has_five_central_values", happy["central_filled_values"] == fixture["happy"], happy["central_filled_values"], fixture["happy"])
            _check(checks, "happy_path_is_completion", happy["view"]["qa_completed"] == TOTAL_SCAN_COUNT, happy["view"]["qa_completed"], TOTAL_SCAN_COUNT)

            duplicate_values = fixture["duplicate"]
            for value in duplicate_values[:2]:
                dispatches.append(_send_scan(app, value, process_calls))
            dispatches.append(_send_scan(app, duplicate_values[1], process_calls))
            duplicate_warning = _snapshot(app, "duplicate_warning", process_calls)
            snapshots.append(duplicate_warning)
            screenshots.append(_capture(app, screenshot_dir, "02_duplicate_warning"))
            _check(checks, "duplicate_warning_is_inline", "중복" in duplicate_warning["view"]["notice_title"] + duplicate_warning["view"]["notice_message"], duplicate_warning["view"], "contains 중복")
            _check(checks, "duplicate_keeps_two_accepted_rows", duplicate_warning["central_filled_values"] == duplicate_values[:2], duplicate_warning["central_filled_values"], duplicate_values[:2])
            _check(checks, "duplicate_keeps_last_normal_central_row", duplicate_warning["central_last_filled_value"] == duplicate_values[1], duplicate_warning["central_last_filled_value"], duplicate_values[1])
            duplicate_ack = _acknowledge_notice(app, process_calls)
            acknowledgements.append({"name": "duplicate_warning", **duplicate_ack})
            duplicate_after = _snapshot(app, "duplicate_after_ack", process_calls)
            snapshots.append(duplicate_after)
            screenshots.append(_capture(app, screenshot_dir, "03_duplicate_after_ack"))
            _check(checks, "duplicate_ack_uses_return_without_rescan", duplicate_ack["ack_did_not_resubmit_scan"], duplicate_ack, "no process_input increment")
            _check(checks, "duplicate_ack_restores_entry_focus", duplicate_ack["focus_after"] == "scan_entry", duplicate_ack["focus_after"], "scan_entry")
            _check(checks, "duplicate_ack_retains_central_rows", duplicate_after["central_filled_values"] == duplicate_values[:2], duplicate_after["central_filled_values"], duplicate_values[:2])
            _check(checks, "duplicate_ack_resets_business_current_set", duplicate_after["current_raw"] == [], duplicate_after["current_raw"], [])

            mismatch_values = fixture["mismatch"]
            dispatches.append(_send_scan(app, mismatch_values[0], process_calls))
            dispatches.append(_send_scan(app, fixture["wrong_product"], process_calls))
            mismatch_warning = _snapshot(app, "mismatch_warning", process_calls)
            snapshots.append(mismatch_warning)
            screenshots.append(_capture(app, screenshot_dir, "04_mismatch_warning"))
            _check(checks, "mismatch_warning_is_inline", "불일치" in mismatch_warning["view"]["notice_title"] + mismatch_warning["view"]["notice_message"], mismatch_warning["view"], "contains 불일치")
            _check(checks, "mismatch_keeps_master_row", mismatch_warning["central_filled_values"] == mismatch_values[:1], mismatch_warning["central_filled_values"], mismatch_values[:1])
            _check(checks, "mismatch_keeps_last_normal_central_row", mismatch_warning["central_last_filled_value"] == mismatch_values[0], mismatch_warning["central_last_filled_value"], mismatch_values[0])
            _check(checks, "mismatch_starts_as_independent_current_set", mismatch_warning["current_raw"] == mismatch_values[:1], mismatch_warning["current_raw"], mismatch_values[:1])
            mismatch_ack = _acknowledge_notice(app, process_calls)
            acknowledgements.append({"name": "mismatch_warning", **mismatch_ack})
            mismatch_after = _snapshot(app, "mismatch_after_ack", process_calls)
            snapshots.append(mismatch_after)
            screenshots.append(_capture(app, screenshot_dir, "05_mismatch_after_ack"))
            _check(checks, "mismatch_ack_restores_entry_focus", mismatch_ack["focus_after"] == "scan_entry", mismatch_ack["focus_after"], "scan_entry")
            _check(checks, "mismatch_ack_retains_master_row", mismatch_after["central_filled_values"] == mismatch_values[:1], mismatch_after["central_filled_values"], mismatch_values[:1])
            _check(checks, "mismatch_ack_resets_business_current_set", mismatch_after["current_raw"] == [], mismatch_after["current_raw"], [])

            recovery_values = fixture["recovery"]
            for value in recovery_values[:2]:
                dispatches.append(_send_scan(app, value, process_calls))
            recovery_before = _snapshot(app, "recovery_before_restart", process_calls)
            snapshots.append(recovery_before)
            screenshots.append(_capture(app, screenshot_dir, "06_recovery_before_restart"))
            app.data_manager.flush(timeout=8)
            state = _state_payload(data_dir)
            saved_raw = list((state.get("current_set_info") or {}).get("raw") or [])
            _check(checks, "partial_state_file_exists", state_path.exists(), state_path.exists(), True)
            _check(checks, "partial_state_contains_two_real_scans", saved_raw == recovery_values[:2], saved_raw, recovery_values[:2])
            _check(checks, "phase_one_process_input_count_exact", len(process_calls) == 12, len(process_calls), 12)
        elif phase == "phase2":
            recovery_values = fixture["recovery"]
            _wait_until(
                app,
                lambda: list((app.current_set_info or {}).get("raw") or []) == recovery_values[:2],
                timeout=8,
                label="restored two-scan set",
            )
            restored = _snapshot(app, "restart_restored", process_calls)
            snapshots.append(restored)
            screenshots.append(_capture(app, screenshot_dir, "07_restart_restored"))
            _check(checks, "restore_prompt_seen_once", len(prompt_calls) == 1 and prompt_calls[0].get("title") == "작업 복구", prompt_calls, "one 작업 복구 prompt")
            _check(checks, "restored_flag_visible", bool(getattr(app, "_workflow_recovered", False)), bool(getattr(app, "_workflow_recovered", False)), True)
            _check(checks, "restored_central_rows_exact", restored["central_filled_values"] == recovery_values[:2], restored["central_filled_values"], recovery_values[:2])
            _check(checks, "restored_last_normal_central_row", restored["central_last_filled_value"] == recovery_values[1], restored["central_last_filled_value"], recovery_values[1])
            _check(checks, "restored_entry_has_focus", restored["focus_role"] == "scan_entry", restored["focus_role"], "scan_entry")
            for value in recovery_values[2:]:
                dispatches.append(_send_scan(app, value, process_calls))
            completed = _snapshot(app, "restart_completed", process_calls)
            snapshots.append(completed)
            screenshots.append(_capture(app, screenshot_dir, "08_restart_completed"))
            _check(checks, "restored_set_completes_all_five_rows", completed["central_filled_values"] == recovery_values, completed["central_filled_values"], recovery_values)
            _check(checks, "restored_set_state_file_deleted", not state_path.exists(), state_path.exists(), False)
            _check(checks, "phase_two_process_input_count_exact", len(process_calls) == 3, len(process_calls), 3)
        else:
            raise ValueError(f"unsupported phase: {phase}")

        _check(checks, "every_dispatch_reached_original_process_input", all(call.get("event_widget_is_entry") and call.get("event_type") for call in process_calls), process_calls, "every call is an Entry event")
        _check(checks, "network_attempt_count_zero", not blocker.attempts, blocker.attempts, [])
        _check(checks, "audio_not_initialized", not bool(getattr(app, "audio_ready", False)), bool(getattr(app, "audio_ready", False)), False)
        _check(
            checks,
            "all_screenshots_strict_pixel_gate",
            all(bool(item.get("capture_pixels_valid")) for item in screenshots),
            [
                {
                    "name": item.get("name"),
                    "valid": item.get("capture_pixels_valid"),
                    "near_black_ratio": item.get("near_black_ratio"),
                    "maximum_tile_near_black_ratio": item.get(
                        "maximum_tile_near_black_ratio"
                    ),
                    "maximum_edge_band_near_black_ratio": item.get(
                        "maximum_edge_band_near_black_ratio"
                    ),
                }
                for item in screenshots
            ],
            "all capture_pixels_valid true",
        )
        _check(checks, "all_screenshot_hwnds_match_pid", all(item["hwnd_pid_matches_process"] for item in screenshots), [item["hwnd_pid_matches_process"] for item in screenshots], "all true")
    except Exception as exc:
        error = f"{exc.__class__.__name__}: {exc}"
    finally:
        if app is not None:
            try:
                _close_app(app)
            except Exception as exc:
                if not error:
                    error = f"close failed: {exc.__class__.__name__}: {exc}"
        sys.executable = str(original_runtime_executable)

    external_runtime_trace_after = _file_fingerprint(
        external_runtime_trace_path
    )
    guarded_runtime_trace = _file_fingerprint(guarded_runtime_trace_path)
    runtime_isolation = {
        "original_executable": str(original_runtime_executable),
        "guarded_executable": str(runtime_executable_guard),
        "guarded_trace": guarded_runtime_trace,
        "external_trace_before": external_runtime_trace_before,
        "external_trace_after": external_runtime_trace_after,
        "real_executable_restored": Path(sys.executable).resolve()
        == original_runtime_executable,
    }
    _check(
        checks,
        "runtime_executable_guard_below_data_dir",
        _is_descendant(runtime_executable_guard, data_dir),
        str(runtime_executable_guard),
        f"descendant of {data_dir}",
    )
    _check(
        checks,
        "runtime_trace_confined_to_guard",
        guarded_runtime_trace["exists"]
        and _is_descendant(guarded_runtime_trace_path, data_dir),
        guarded_runtime_trace,
        f"startup trace below {data_dir}",
    )
    _check(
        checks,
        "external_runtime_trace_unchanged",
        external_runtime_trace_after == external_runtime_trace_before,
        {
            "before": external_runtime_trace_before,
            "after": external_runtime_trace_after,
        },
        "exactly unchanged",
    )

    failed_checks = [item["name"] for item in checks if item["status"] != "PASS"]
    status = "PASS" if not error and not failed_checks else "FAIL"
    payload = {
        "report_version": REPORT_VERSION,
        "phase": phase,
        "status": status,
        "started_at": phase_started,
        "finished_at": datetime.now().isoformat(),
        "pid": os.getpid(),
        "marker": marker,
        "root": str(ROOT),
        "data_dir": str(data_dir),
        "report_path": str(report_path),
        "geometry": geometry,
        "run_tests": False,
        "input_mode": INPUT_MODE,
        "process_input_implementation": "original Label_Match.process_input via probe wrapper",
        "checks": checks,
        "failed_checks": failed_checks,
        "error": error,
        "process_input_calls": process_calls,
        "dispatches": dispatches,
        "acknowledgements": acknowledgements,
        "snapshots": snapshots,
        "screenshots": screenshots,
        "prompt_calls": prompt_calls,
        "integration_calls": integration_calls,
        "network_attempts": blocker.attempts,
        "runtime_isolation": runtime_isolation,
        "event_counts_at_phase_end": _event_counts(data_dir),
        "state_file": {
            "path": str(state_path),
            "exists": state_path.exists(),
            "sha256": _sha256_file(state_path) if state_path.exists() else "",
        },
    }
    _write_json(report_path, payload)
    return 0 if status == "PASS" else 2


def _inventory(output_dir: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(output_dir.rglob("*")):
        if not path.is_file():
            continue
        rows.append(
            {
                "path": str(path.resolve()),
                "relative_path": path.relative_to(output_dir).as_posix(),
                "size": path.stat().st_size,
                "sha256": _sha256_file(path),
            }
        )
    return rows


def report_issue_codes(report: Mapping[str, Any]) -> list[str]:
    """Fail-closed aggregate verifier used by both the tool and tests."""

    issues: list[str] = []
    phase_one = report.get("phase_one") or {}
    phase_two = report.get("phase_two") or {}
    if phase_one.get("status") != "PASS":
        issues.append("PHASE_ONE_FAILED")
    if phase_two.get("status") != "PASS":
        issues.append("PHASE_TWO_FAILED")
    for phase_name, required in REQUIRED_PHASE_CHECKS.items():
        phase = phase_one if phase_name == "phase_one" else phase_two
        observed = {
            str(item.get("name") or ""): str(item.get("status") or "")
            for item in (phase.get("checks") or [])
        }
        for name in sorted(required):
            if observed.get(name) != "PASS":
                issues.append(f"REQUIRED_CHECK_{phase_name.upper()}_{name.upper()}_MISSING_OR_FAILED")
    try:
        phase_one_pid = int(phase_one.get("pid"))
        phase_two_pid = int(phase_two.get("pid"))
    except (TypeError, ValueError):
        issues.append("CHILD_PID_MISSING")
    else:
        if phase_one_pid == phase_two_pid:
            issues.append("RESTART_PID_NOT_CHANGED")
    if int(report.get("process_input_call_count", -1)) != 15:
        issues.append("PROCESS_INPUT_CALL_COUNT_MISMATCH")
    counts = report.get("event_counts") or {}
    for event, expected in EXPECTED_EVENT_COUNTS.items():
        if int(counts.get(event, -1)) != expected:
            issues.append(f"EVENT_COUNT_{event}_MISMATCH")
    if dict(report.get("tray_result_counts") or {}) != EXPECTED_TRAY_RESULT_COUNTS:
        issues.append("TRAY_RESULT_SEMANTICS_MISMATCH")
    if report.get("state_file_exists_after_phase_two") is not False:
        issues.append("STATE_FILE_REMAINS_AFTER_COMPLETION")
    if report.get("git_status_unchanged") is not True:
        issues.append("REPOSITORY_STATUS_CHANGED")
    if report.get("all_artifacts_below_output") is not True:
        issues.append("ARTIFACT_ESCAPED_OUTPUT_ROOT")
    screenshots = list(report.get("screenshots") or [])
    expected_names = tuple(
        str(value)
        for value in (
            report.get("expected_screenshot_names") or EXPECTED_SCREENSHOT_NAMES
        )
    )
    observed_names = [str(item.get("name") or "") for item in screenshots]
    if (
        len(screenshots) != len(expected_names)
        or len(set(observed_names)) != len(expected_names)
        or set(observed_names) != set(expected_names)
    ):
        issues.append("SCREENSHOT_COUNT_MISMATCH")
        issues.append("SCREENSHOT_NAME_SET_MISMATCH")
    observed_hashes = [str(item.get("sha256") or "") for item in screenshots]
    if (
        len(observed_hashes) != len(expected_names)
        or any(not value for value in observed_hashes)
        or len(set(observed_hashes)) != len(expected_names)
    ):
        issues.append("SCREENSHOT_SHA256_NOT_UNIQUE")
    expected_size_value = report.get("expected_screenshot_size")
    if expected_size_value:
        expected_size = tuple(int(value) for value in expected_size_value)
    else:
        try:
            expected_size = _parse_geometry(
                str(report.get("geometry") or "1366x768+0+0")
            )[:2]
        except (TypeError, ValueError):
            expected_size = (1366, 768)
            issues.append("EXPECTED_SCREENSHOT_SIZE_INVALID")
    for item in screenshots:
        path = Path(str(item.get("path") or ""))
        if (
            not path.is_file()
            or not item.get("hwnd_pid_matches_process")
            or (int(item.get("width") or 0), int(item.get("height") or 0))
            != expected_size
            or item.get("pixel_size_matches") is not True
            or not item.get("capture_pixels_valid")
            or item.get("blank_suspected")
            or item.get("excess_black_suspected")
            or item.get("edge_black_stripe_suspected")
            or item.get("contiguous_black_stripe_suspected")
            or item.get("black_tile_suspected")
            or item.get("uniform_low_variance_suspected")
            or float(item.get("near_black_ratio", 1.0))
            > NEAR_BLACK_FAILURE_RATIO
            or str(item.get("sha256") or "") != (_sha256_file(path) if path.is_file() else "")
        ):
            screenshot_name = str(item.get("name") or "UNKNOWN").upper()
            issues.append(f"SCREENSHOT_INVALID_{screenshot_name}")
    return sorted(set(issues))


def _run_parent(output_dir: Path, geometry: str, timeout: float) -> dict[str, Any]:
    output_dir = output_dir.resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        raise RuntimeError(f"output directory must be new or empty: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    data_dir = _assert_descendant(output_dir / "isolated_data", output_dir, "data directory")
    phase_dir = _assert_descendant(output_dir / "phases", output_dir, "phase directory")
    data_dir.mkdir(parents=True, exist_ok=True)
    phase_dir.mkdir(parents=True, exist_ok=True)
    marker = f"ACTUAL-{datetime.now().strftime('%Y%m%d%H%M%S')}-{os.getpid()}"
    phase_one_path = phase_dir / PHASE_ONE_REPORT_NAME
    phase_two_path = phase_dir / PHASE_TWO_REPORT_NAME
    source_hash_before = _sha256_file(ROOT / "Label_Match.py")
    config_path = ROOT / "config" / "app_settings.json"
    config_hash_before = _sha256_file(config_path) if config_path.exists() else ""
    git_before = _git_status()
    env = build_child_environment(os.environ, data_dir, marker)

    phase_runs: list[dict[str, Any]] = []
    for phase, report_path in (("phase1", phase_one_path), ("phase2", phase_two_path)):
        command = [
            sys.executable,
            str(SCRIPT_PATH),
            "--child-phase",
            phase,
            "--data-dir",
            str(data_dir),
            "--phase-report",
            str(report_path),
            "--marker",
            marker,
            "--geometry",
            geometry,
        ]
        completed = subprocess.run(
            command,
            cwd=str(ROOT),
            env=env,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        phase_runs.append(
            {
                "phase": phase,
                "command": command,
                "returncode": completed.returncode,
                "stdout": completed.stdout[-4000:],
                "stderr": completed.stderr[-4000:],
                "report_path": str(report_path),
                "report_exists": report_path.exists(),
            }
        )
        if not report_path.exists():
            break
        # A phase-one UI assertion may fail while the persisted partial set is
        # still sound.  Continue with a second OS process so the aggregate
        # report preserves independent restart evidence instead of hiding it.
        if (
            completed.returncode != 0
            and phase == "phase1"
            and not (data_dir / STATE_FILE_NAME).exists()
        ):
            break

    phase_one = _read_json(phase_one_path) if phase_one_path.exists() else {}
    phase_two = _read_json(phase_two_path) if phase_two_path.exists() else {}
    event_counts = _event_counts(data_dir)
    tray_result_counts = _tray_result_counts(data_dir)
    process_calls = list(phase_one.get("process_input_calls") or []) + list(
        phase_two.get("process_input_calls") or []
    )
    screenshots = list(phase_one.get("screenshots") or []) + list(
        phase_two.get("screenshots") or []
    )
    git_after = _git_status()
    source_hash_after = _sha256_file(ROOT / "Label_Match.py")
    config_hash_after = _sha256_file(config_path) if config_path.exists() else ""
    state_path = data_dir / STATE_FILE_NAME
    all_paths = [Path(str(item.get("path") or "")) for item in screenshots]
    for phase_payload in (phase_one, phase_two):
        phase_report_path = str(phase_payload.get("report_path") or "")
        if phase_report_path:
            all_paths.append(Path(phase_report_path))
        guarded_trace_path = str(
            (
                phase_payload.get("runtime_isolation")
                or {}
            ).get("guarded_trace", {}).get("path")
            or ""
        )
        if guarded_trace_path:
            all_paths.append(Path(guarded_trace_path))
    all_artifacts_below_output = all(
        path.is_file() and _is_descendant(path, output_dir) for path in all_paths
    )
    aggregate: dict[str, Any] = {
        "report_version": REPORT_VERSION,
        "status": "PENDING_VERIFICATION",
        "generated_at": datetime.now().isoformat(),
        "marker": marker,
        "root": str(ROOT),
        "output_dir": str(output_dir),
        "data_dir": str(data_dir),
        "geometry": geometry,
        "expected_screenshot_names": list(EXPECTED_SCREENSHOT_NAMES),
        "expected_screenshot_size": list(_parse_geometry(geometry)[:2]),
        "input_mode": INPUT_MODE,
        "uses_run_tests_false": True,
        "phase_runs": phase_runs,
        "phase_one": phase_one,
        "phase_two": phase_two,
        "process_input_call_count": len(process_calls),
        "process_input_calls": process_calls,
        "event_counts": event_counts,
        "tray_result_counts": tray_result_counts,
        "screenshots": screenshots,
        "state_file_exists_after_phase_two": state_path.exists(),
        "source_hash_before": source_hash_before,
        "source_hash_after": source_hash_after,
        "source_unchanged": source_hash_before == source_hash_after,
        "config_hash_before": config_hash_before,
        "config_hash_after": config_hash_after,
        "config_unchanged": config_hash_before == config_hash_after,
        "git_status_before": git_before,
        "git_status_after": git_after,
        "git_status_unchanged": git_before == git_after,
        "all_artifacts_below_output": all_artifacts_below_output,
        "integration_guards": {
            "save_dir": env.get("LABEL_MATCH_SAVE_DIR"),
            "program_data": env.get("ProgramData"),
            "local_app_data": env.get("LOCALAPPDATA"),
            "temp": env.get("TEMP"),
            "audio": env.get("LABEL_MATCH_AUDIO_ENABLED"),
            "direct_sync_bootstrap": env.get("LABEL_MATCH_DIRECT_SYNC_BOOTSTRAP"),
            "session_sync": env.get("LABEL_MATCH_SESSION_SYNC_TRIGGER"),
            "update_provider": env.get("LABEL_MATCH_UPDATE_PROVIDER"),
        },
        "source_files": {
            "app": {
                "path": str((ROOT / "Label_Match.py").resolve()),
                "sha256": source_hash_after,
            },
            "driver": {
                "path": str(SCRIPT_PATH),
                "sha256": _sha256_file(SCRIPT_PATH),
            },
        },
    }
    aggregate["issue_codes"] = report_issue_codes(aggregate)
    if not aggregate["source_unchanged"]:
        aggregate["issue_codes"].append("APPLICATION_SOURCE_CHANGED")
    if not aggregate["config_unchanged"]:
        aggregate["issue_codes"].append("APPLICATION_CONFIG_CHANGED")
    aggregate["issue_codes"] = sorted(set(aggregate["issue_codes"]))
    aggregate["status"] = "PASS" if not aggregate["issue_codes"] else "FAIL"
    report_path = output_dir / REPORT_NAME
    _write_json(report_path, aggregate)
    inventory = _inventory(output_dir)
    inventory_path = output_dir / "artifact_manifest.json"
    _write_json(
        inventory_path,
        {
            "generated_at": datetime.now().isoformat(),
            "output_dir": str(output_dir),
            "artifact_count_before_manifest": len(inventory),
            "artifacts": inventory,
        },
    )
    aggregate["report_path"] = str(report_path)
    aggregate["report_sha256"] = _sha256_file(report_path)
    aggregate["artifact_manifest_path"] = str(inventory_path)
    aggregate["artifact_manifest_sha256"] = _sha256_file(inventory_path)
    return aggregate


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run a real Return-binding Label_Match walkthrough in two child processes"
    )
    parser.add_argument("--output-dir")
    parser.add_argument("--geometry", default="1366x768+0+0")
    parser.add_argument("--timeout", type=float, default=90.0)
    parser.add_argument("--child-phase", choices=("phase1", "phase2"))
    parser.add_argument("--data-dir")
    parser.add_argument("--phase-report")
    parser.add_argument("--marker")
    args = parser.parse_args(argv)

    if args.child_phase:
        required = {
            "--data-dir": args.data_dir,
            "--phase-report": args.phase_report,
            "--marker": args.marker,
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            parser.error(f"child phase is missing: {', '.join(missing)}")
        return _run_child_phase(
            args.child_phase,
            Path(args.data_dir),
            Path(args.phase_report),
            str(args.marker),
            str(args.geometry),
        )

    if not args.output_dir:
        parser.error("--output-dir is required")
    report = _run_parent(Path(args.output_dir), str(args.geometry), float(args.timeout))
    print(
        json.dumps(
            {
                "status": report["status"],
                "report_path": report["report_path"],
                "report_sha256": report["report_sha256"],
                "issue_codes": report["issue_codes"],
                "phase_pids": [
                    report.get("phase_one", {}).get("pid"),
                    report.get("phase_two", {}).get("pid"),
                ],
                "process_input_call_count": report["process_input_call_count"],
                "event_counts": report["event_counts"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())

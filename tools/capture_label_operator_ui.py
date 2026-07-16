"""Capture deterministic Label Match operator-workbench evidence.

The harness is deliberately fail-closed.  It renders the real Tk workbench
from the same pure workflow presenter used by the application, captures fixed
client sizes, and records pixel, geometry, content, and resize-round-trip
evidence.  Mutable runtime data is redirected below the selected output
directory; logistics, update, sync, and audio integrations are disabled.

This file does not provide a compatibility path for the legacy two-table
layout.  Until the operator-workbench widget contract exists, the manifest is
written with ``live_contract_ready: false`` and the command exits non-zero.
"""

from __future__ import annotations

import argparse
import ctypes
import datetime as dt
import hashlib
import importlib
import inspect
import json
import math
import os
from pathlib import Path
import sys
import time
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping, Sequence

from PIL import Image, ImageGrab, ImageStat


ROOT = Path(__file__).resolve().parents[1]
REPO_TMP_ROOT = ROOT / "tmp"
DEFAULT_SIZES = ((1366, 768), (1440, 900), (1920, 1080), (2560, 1080))
DEFAULT_STATE_IDS = (
    "waiting",
    "qa_progress",
    "exact_active",
    "exact_complete",
    "sealed",
    "error",
    "full_complete",
    "partial_complete",
    "recovery",
    "history_readonly",
    "submission_blocked",
)
DEFAULT_SCALE = 1.0
MIN_SCALE = 0.7
MAX_SCALE = 2.5
NEAR_BLACK_LUMA = 16
NEAR_BLACK_FAILURE_RATIO = 0.35

REQUIRED_WIDGET_ATTRS = (
    "workbench_frame",
    "left_context_card",
    "top_card",
    "right_activity_card",
    "entry",
    "workflow_notice_frame",
    "workflow_notice_title_label",
    "workflow_notice_label",
    "workflow_notice_action_button",
    "current_set_tree",
    "exact_rescan_tree",
    "operator_history_notebook",
    "session_tree",
    "history_tree",
    "summary_tree",
    "bottom_frame",
    "reset_button",
    "manual_complete_button",
    "exact_rescan_button",
)
CANCEL_BUTTON_ALIASES = ("cancel_button", "cancel_tray_button")
NOARG_REFRESH_METHODS = (
    "_refresh_operator_workbench",
    "_refresh_workflow_view",
    "_update_operator_workbench",
)
VIEW_RENDER_METHODS = (
    "_render_workflow_view",
    "_render_operator_workflow",
    "_apply_workflow_view",
)


@dataclass(frozen=True, slots=True)
class StateFixture:
    state_id: str
    label: str
    qa_scans: tuple[str, ...] = ()
    exact_barcodes: tuple[str, ...] = ()
    exact_target: int = 0
    exact_active: bool = False
    exact_complete: bool = False
    sealed_transfer: bool = False
    has_error: bool = False
    error_message: str = ""
    completion_kind: str | None = None
    recovered: bool = False
    history_readonly: bool = False
    notice_title: str = ""
    notice_message: str = ""
    notice_kind: str = "submission_blocked"
    notice_tone: str = "danger"
    last_normal_scan: str = ""


def build_state_fixtures() -> tuple[StateFixture, ...]:
    """Return the complete deterministic operator-state matrix."""

    master = "AAA2270730100 · 현품표"
    product_1 = "AAA2270730100 · 제품 1"
    product_2 = "AAA2270730100 · 제품 2"
    product_3 = "AAA2270730100 · 제품 3"
    final_label = "AAA2270730100 · 최종 라벨"
    qa_two = (master, product_1)
    qa_three = (*qa_two, product_2)
    qa_full = (*qa_three, product_3, final_label)
    exact_two = (
        "AAA2270730100-EXACT-0001",
        "AAA2270730100-EXACT-0002",
    )
    exact_full = (*exact_two, "AAA2270730100-EXACT-0003", "AAA2270730100-EXACT-0004")
    return (
        StateFixture("waiting", "대기"),
        StateFixture(
            "qa_progress",
            "QA 진행",
            qa_scans=qa_two,
            last_normal_scan=product_1,
        ),
        StateFixture(
            "exact_active",
            "F4 재스캔 진행",
            qa_scans=(master,),
            exact_barcodes=exact_two,
            exact_target=4,
            exact_active=True,
            last_normal_scan=exact_two[-1],
        ),
        StateFixture(
            "exact_complete",
            "F4 재스캔 완료",
            qa_scans=(master,),
            exact_barcodes=exact_full,
            exact_target=4,
            exact_complete=True,
            last_normal_scan=exact_full[-1],
        ),
        StateFixture(
            "sealed",
            "sealed 상속",
            qa_scans=("SEALED TRANSFER · AAA2270730100",),
            sealed_transfer=True,
            last_normal_scan="SEALED TRANSFER · AAA2270730100",
        ),
        StateFixture(
            "error",
            "오류",
            qa_scans=qa_two,
            has_error=True,
            error_message="품목이 일치하지 않습니다.",
            last_normal_scan=product_1,
        ),
        StateFixture(
            "full_complete",
            "정상 완료",
            qa_scans=qa_full,
            completion_kind="full",
            last_normal_scan=final_label,
        ),
        StateFixture(
            "partial_complete",
            "부분 완료",
            qa_scans=qa_three,
            completion_kind="partial",
            last_normal_scan=product_2,
        ),
        StateFixture(
            "recovery",
            "복구",
            qa_scans=qa_three,
            recovered=True,
            last_normal_scan=product_2,
        ),
        StateFixture(
            "history_readonly",
            "과거 기록 조회",
            qa_scans=qa_two,
            history_readonly=True,
            last_normal_scan=product_1,
        ),
        StateFixture(
            "submission_blocked",
            "제출 차단",
            qa_scans=qa_full,
            notice_title="중앙 제출 차단 · 5/5 유지",
            notice_message=(
                "오류: HTTP 503 Service Unavailable: "
                "중앙 포장 API 연결 시간이 초과되었습니다."
            ),
            last_normal_scan=final_label,
        ),
    )


def parse_sizes(value: str) -> tuple[tuple[int, int], ...]:
    result: list[tuple[int, int]] = []
    for raw in str(value or "").split(","):
        item = raw.strip().lower().replace("×", "x")
        if not item:
            continue
        parts = item.split("x")
        if len(parts) != 2:
            raise argparse.ArgumentTypeError(f"invalid capture size: {raw!r}")
        try:
            pair = (int(parts[0]), int(parts[1]))
        except ValueError as exc:
            raise argparse.ArgumentTypeError(f"invalid capture size: {raw!r}") from exc
        if pair[0] < 1024 or pair[1] < 720:
            raise argparse.ArgumentTypeError(
                f"capture size must be at least 1024x720: {pair[0]}x{pair[1]}"
            )
        if pair not in result:
            result.append(pair)
    if not result:
        raise argparse.ArgumentTypeError("at least one capture size is required")
    return tuple(result)


def parse_states(value: str) -> tuple[str, ...]:
    result: list[str] = []
    allowed = set(DEFAULT_STATE_IDS)
    for raw in str(value or "").split(","):
        state_id = raw.strip().lower()
        if not state_id:
            continue
        if state_id not in allowed:
            raise argparse.ArgumentTypeError(
                f"unknown state {raw!r}; choose from {', '.join(DEFAULT_STATE_IDS)}"
            )
        if state_id not in result:
            result.append(state_id)
    if not result:
        raise argparse.ArgumentTypeError("at least one state is required")
    return tuple(result)


def parse_scale(value: object) -> float:
    if isinstance(value, bool):
        raise argparse.ArgumentTypeError("scale must be a finite number")
    try:
        scale = float(value)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError("scale must be a finite number") from exc
    if not math.isfinite(scale):
        raise argparse.ArgumentTypeError("scale must be a finite number")
    if not MIN_SCALE <= scale <= MAX_SCALE:
        raise argparse.ArgumentTypeError(
            f"scale must be between {MIN_SCALE} and {MAX_SCALE}: {scale}"
        )
    return scale


def assert_descendant(path: Path, parent: Path, *, label: str) -> Path:
    resolved = path.resolve()
    resolved_parent = parent.resolve()
    if resolved == resolved_parent or not resolved.is_relative_to(resolved_parent):
        raise RuntimeError(f"{label} must stay below {resolved_parent}: {resolved}")
    return resolved


def prepare_isolated_environment(data_root: Path) -> dict[str, str]:
    resolved = assert_descendant(data_root, REPO_TMP_ROOT, label="capture data root")
    resolved.mkdir(parents=True, exist_ok=True)
    temp_root = resolved / "temp"
    temp_root.mkdir(parents=True, exist_ok=True)
    guards = {
        "LABEL_MATCH_SAVE_DIR": str(resolved),
        "LABEL_MATCH_AUTOMATED_TEST": "1",
        "LABEL_MATCH_AUDIO_ENABLED": "off",
        "LABEL_MATCH_DIRECT_SYNC_BOOTSTRAP": "off",
        "LABEL_MATCH_SESSION_SYNC_TRIGGER": "off",
        "LABEL_MATCH_UPDATE_PROVIDER": "off",
        "KMTECH_TEST_SILENT_AUDIO": "1",
        "SDL_AUDIODRIVER": "dummy",
        "PYGAME_HIDE_SUPPORT_PROMPT": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "TEMP": str(temp_root),
        "TMP": str(temp_root),
    }
    os.environ.update(guards)
    for key in tuple(os.environ):
        if key.startswith("LABEL_MATCH_LOGISTICS_") or key.startswith(
            "WORKER_ANALYSIS_LOGISTICS_"
        ):
            os.environ.pop(key, None)
    return guards


def build_isolated_app_settings(data_root: Path, scale: float) -> dict[str, Any]:
    return {
        "custom_save_path": str(data_root.resolve()),
        "worker_name": "캡처 작업자",
        "ui_settings": {"default_font": "Malgun Gothic", "base_font_size": 14},
        "ui_persistence": {"scale_factor": float(scale), "tree_font_size": 13},
        "colors": {},
        "sound_files": {},
        "update_settings": {"provider": "off"},
    }


def enable_per_monitor_dpi_awareness() -> str:
    if os.name != "nt":
        return "not-windows"
    try:
        ctypes.windll.shcore.SetProcessDpiAwareness(2)
        return "per-monitor-aware"
    except Exception:
        try:
            ctypes.windll.user32.SetProcessDPIAware()
            return "system-aware"
        except Exception:
            return "unchanged"


def pump_tk(root: Any, milliseconds: int = 220) -> None:
    deadline = time.monotonic() + max(0, milliseconds) / 1000.0
    while time.monotonic() < deadline:
        root.update()
        time.sleep(0.012)
    root.update_idletasks()
    root.update()


def _capture_client_with_print_window(root: Any) -> tuple[Image.Image, str]:
    import win32con
    import win32gui
    import win32ui

    hwnd = int(root.winfo_id())
    try:
        hwnd = int(win32gui.GetAncestor(hwnd, win32con.GA_ROOT))
    except Exception:
        while win32gui.GetParent(hwnd):
            hwnd = int(win32gui.GetParent(hwnd))
    left, top, right, bottom = win32gui.GetWindowRect(hwnd)
    window_size = (max(1, right - left), max(1, bottom - top))
    client_left, client_top = win32gui.ClientToScreen(hwnd, (0, 0))
    client_rect = win32gui.GetClientRect(hwnd)
    client_size = (
        max(1, int(client_rect[2] - client_rect[0])),
        max(1, int(client_rect[3] - client_rect[1])),
    )
    hwnd_dc = win32gui.GetWindowDC(hwnd)
    mfc_dc = win32ui.CreateDCFromHandle(hwnd_dc)
    save_dc = mfc_dc.CreateCompatibleDC()
    bitmap = win32ui.CreateBitmap()
    bitmap.CreateCompatibleBitmap(mfc_dc, *window_size)
    save_dc.SelectObject(bitmap)
    try:
        rendered = ctypes.windll.user32.PrintWindow(hwnd, save_dc.GetSafeHdc(), 2)
        if not rendered:
            save_dc.BitBlt((0, 0), window_size, mfc_dc, (0, 0), win32con.SRCCOPY)
        info = bitmap.GetInfo()
        bits = bitmap.GetBitmapBits(True)
        full = Image.frombuffer(
            "RGB",
            (info["bmWidth"], info["bmHeight"]),
            bits,
            "raw",
            "BGRX",
            0,
            1,
        ).copy()
    finally:
        win32gui.DeleteObject(bitmap.GetHandle())
        save_dc.DeleteDC()
        mfc_dc.DeleteDC()
        win32gui.ReleaseDC(hwnd, hwnd_dc)
    crop_left = max(0, int(client_left - left))
    crop_top = max(0, int(client_top - top))
    return (
        full.crop(
            (
                crop_left,
                crop_top,
                min(full.width, crop_left + client_size[0]),
                min(full.height, crop_top + client_size[1]),
            )
        ),
        "PrintWindow(PW_RENDERFULLCONTENT)+client-crop",
    )


def capture_tk_client(root: Any) -> tuple[Image.Image, str]:
    root.update_idletasks()
    root.update()
    fallback = "PrintWindow unavailable"
    if os.name == "nt":
        try:
            return _capture_client_with_print_window(root)
        except Exception as exc:
            fallback = f"PrintWindow failed: {type(exc).__name__}: {exc}"
    left = int(root.winfo_rootx())
    top = int(root.winfo_rooty())
    width = max(1, int(root.winfo_width()))
    height = max(1, int(root.winfo_height()))
    return (
        ImageGrab.grab(
            bbox=(left, top, left + width, top + height), all_screens=True
        ),
        f"ImageGrab(client-bbox); {fallback}",
    )


def analyze_image(image: Image.Image, expected_size: tuple[int, int]) -> dict[str, Any]:
    rgb = image.convert("RGB")
    gray = rgb.convert("L")
    histogram = gray.histogram()
    pixels = max(1, rgb.width * rgb.height)
    extrema = gray.getextrema() or (0, 0)
    stat = ImageStat.Stat(gray)
    sample = rgb.copy()
    sample.thumbnail((256, 256))
    colors = sample.getcolors(maxcolors=max(1, sample.width * sample.height)) or []
    dominant_ratio = max((count for count, _ in colors), default=0) / max(
        1, sample.width * sample.height
    )
    near_black = sum(histogram[: NEAR_BLACK_LUMA + 1])
    blank = bool(
        extrema[1] - extrema[0] <= 2
        or stat.stddev[0] < 0.75
        or dominant_ratio >= 0.997
    )
    return {
        "expected_pixel_size": list(expected_size),
        "pixel_size": [rgb.width, rgb.height],
        "pixel_size_matches": (rgb.width, rgb.height) == expected_size,
        "near_black_ratio": round(near_black / pixels, 6),
        "near_black_threshold_luma": NEAR_BLACK_LUMA,
        "blank_suspected": blank,
        "luma_mean": round(float(stat.mean[0]), 3),
        "luma_stddev": round(float(stat.stddev[0]), 3),
        "dominant_color_ratio_sampled": round(dominant_ratio, 6),
    }


def _find_cancel_button(app: Any) -> tuple[str | None, Any | None]:
    for name in CANCEL_BUTTON_ALIASES:
        value = getattr(app, name, None)
        if value is not None:
            return name, value
    return None, None


def validate_live_contract(app: Any) -> list[str]:
    """Report missing workbench contracts; an empty list means capturable."""

    issues = [
        f"missing_widget:{name}"
        for name in REQUIRED_WIDGET_ATTRS
        if getattr(app, name, None) is None
    ]
    cancel_name, _cancel = _find_cancel_button(app)
    if cancel_name is None:
        issues.append("missing_widget:cancel_button")
    step_labels = getattr(app, "step_labels", None)
    if not isinstance(step_labels, (list, tuple)) or len(step_labels) != 5:
        issues.append("step_labels_must_have_five_widgets")
    render_methods = (*NOARG_REFRESH_METHODS, *VIEW_RENDER_METHODS)
    if not any(callable(getattr(app, name, None)) for name in render_methods):
        issues.append("missing_presenter_refresh_method")
    trees = [
        getattr(app, name, None)
        for name in ("current_set_tree", "exact_rescan_tree", "session_tree", "history_tree", "summary_tree")
    ]
    existing = [tree for tree in trees if tree is not None]
    if len({id(tree) for tree in existing}) != len(existing):
        issues.append("tree_widgets_must_be_distinct")
    return issues


def build_presenter_view(fixture: StateFixture) -> Any:
    from ui.workflow_snapshot_adapter import adapt_workflow_snapshot
    from ui.workflow_view_state import WorkflowNotice, present_workflow

    notice = None
    if fixture.notice_title:
        notice = WorkflowNotice(
            fixture.notice_title,
            fixture.notice_message,
            kind=fixture.notice_kind,
            tone=fixture.notice_tone,
        )
    current = {
        "id": f"capture-{fixture.state_id}",
        "raw": list(fixture.qa_scans),
        "parsed": list(fixture.qa_scans),
        "has_error_or_reset": fixture.has_error,
        "exact_rescan_active": fixture.exact_active,
        "exact_rescan_complete": fixture.exact_complete,
        "exact_rescan_target_count": fixture.exact_target,
        "exact_rescan_barcodes": list(fixture.exact_barcodes),
        "sealed_transfer": fixture.sealed_transfer,
    }
    snapshot = adapt_workflow_snapshot(
        current,
        initialized=True,
        loading=False,
        history_readonly=fixture.history_readonly,
        recovered=fixture.recovered,
        completion_kind=fixture.completion_kind,
        blocking_notice=notice,
        last_normal_scan_override=fixture.last_normal_scan or None,
        has_error=fixture.has_error,
        error_message=fixture.error_message,
    )
    return present_workflow(snapshot)


def _invoke_presenter_refresh(app: Any, view: Any) -> str:
    for name in NOARG_REFRESH_METHODS:
        method = getattr(app, name, None)
        if callable(method):
            method()
            return name
    for name in VIEW_RENDER_METHODS:
        method = getattr(app, name, None)
        if not callable(method):
            continue
        signature = inspect.signature(method)
        required = [
            parameter
            for parameter in signature.parameters.values()
            if parameter.default is inspect.Parameter.empty
            and parameter.kind
            in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        ]
        if len(required) == 0:
            method()
        else:
            method(view)
        return name
    raise RuntimeError("operator workbench has no presenter refresh method")


def _select_activity_tab_for_fixture(app: Any, fixture: StateFixture) -> None:
    """Keep the right activity tab deterministic across capture fixtures."""

    notebook = getattr(app, "operator_history_notebook", None)
    if notebook is None:
        notebook = getattr(app, "operator_notebook", None)
    if notebook is None:
        return

    if fixture.history_readonly:
        aliases = ("operator_history_tab", "history_tab", "history_card")
    else:
        aliases = ("operator_session_tab", "session_tab")
    target = next(
        (getattr(app, name, None) for name in aliases if getattr(app, name, None) is not None),
        None,
    )
    if target is None:
        return
    try:
        notebook.select(target)
    except Exception:
        # Contract validation and the rendered-state checks remain fail-closed;
        # this helper only avoids leaking a previous fixture's selected tab.
        return


def apply_state_fixture(app: Any, fixture: StateFixture) -> tuple[Any, str]:
    """Apply display-only state and ask the application to render its presenter."""

    from ui.workflow_view_state import WorkflowNotice

    current = dict(getattr(app, "current_set_info", {}) or {})
    current.update(
        {
            "id": f"capture-{fixture.state_id}",
            "raw": list(fixture.qa_scans),
            "parsed": list(fixture.qa_scans),
            "has_error_or_reset": fixture.has_error,
            "error_count": 1 if fixture.has_error else 0,
            "exact_rescan_active": fixture.exact_active,
            "exact_rescan_complete": fixture.exact_complete,
            "exact_rescan_target_count": fixture.exact_target,
            "exact_rescan_barcodes": list(fixture.exact_barcodes),
            "sealed_transfer": fixture.sealed_transfer,
        }
    )
    app.current_set_info = current
    app.history_view_updates_active_state = not fixture.history_readonly
    app.history_load_pending = False
    app.history_active_load_pending = False
    app._workflow_completion_kind = fixture.completion_kind
    app._workflow_display_scans = tuple(fixture.qa_scans)
    app._workflow_last_normal_override = fixture.last_normal_scan or None
    app._workflow_recovered = fixture.recovered
    pending_error = fixture.error_message or None
    # Keep every runtime spelling in sync.  The workbench renderer reads the
    # newer pair while a few compatibility paths still inspect the older
    # capture-era alias.
    app._pending_workflow_error = pending_error
    app._workflow_pending_error = pending_error
    app._workflow_error_message = fixture.error_message or ""
    app._workflow_notice_action = (
        (lambda: None) if fixture.state_id == "submission_blocked" else None
    )
    app._workflow_notice_action_text = (
        "제출 재시도" if fixture.state_id == "submission_blocked" else "확인"
    )
    app._workflow_blocking_notice = (
        WorkflowNotice(
            fixture.notice_title,
            fixture.notice_message,
            kind=fixture.notice_kind,
            tone=fixture.notice_tone,
        )
        if fixture.notice_title
        else None
    )
    view = build_presenter_view(fixture)
    # Capture-only mirrors let a renderer with an explicit view parameter and
    # a renderer that rebuilds from runtime state share the same harness.
    app._workflow_view_state = view
    app._last_workflow_view_state = view
    method_name = _invoke_presenter_refresh(app, view)
    _select_activity_tab_for_fixture(app, fixture)
    return view, method_name


def _is_mapped(widget: Any) -> bool:
    try:
        return bool(widget.winfo_ismapped())
    except Exception:
        return False


def _widget_record(
    root: Any,
    widget: Any,
    name: str,
    *,
    critical: bool = True,
    check_requested_width: bool = False,
    check_requested_height: bool = False,
) -> dict[str, Any]:
    root_x = int(root.winfo_rootx())
    root_y = int(root.winfo_rooty())
    x = int(widget.winfo_rootx()) - root_x
    y = int(widget.winfo_rooty()) - root_y
    width = int(widget.winfo_width())
    height = int(widget.winfo_height())
    try:
        requested = [int(widget.winfo_reqwidth()), int(widget.winfo_reqheight())]
    except Exception:
        requested = [width, height]
    try:
        grid_info = dict(widget.grid_info())
    except Exception:
        grid_info = {}
    grid = {}
    for key in ("row", "column", "rowspan", "columnspan"):
        if key in grid_info:
            try:
                grid[key] = int(grid_info[key])
            except (TypeError, ValueError):
                grid[key] = str(grid_info[key])
    if "sticky" in grid_info:
        grid["sticky"] = str(grid_info["sticky"])
    return {
        "name": name,
        "path": str(widget),
        "master_path": str(getattr(widget, "master", "")),
        "mapped": _is_mapped(widget),
        "critical": critical,
        "bbox": [x, y, x + width, y + height],
        "size": [width, height],
        "requested_size": requested,
        "check_requested_width": check_requested_width,
        "check_requested_height": check_requested_height,
        "grid": grid,
    }


def _boxes_overlap(first: Sequence[int], second: Sequence[int]) -> bool:
    return (
        min(first[2], second[2]) - max(first[0], second[0]) > 1
        and min(first[3], second[3]) - max(first[1], second[1]) > 1
    )


def _inside(child: Sequence[int], parent: Sequence[int], tolerance: int = 2) -> bool:
    return (
        child[0] >= parent[0] - tolerance
        and child[1] >= parent[1] - tolerance
        and child[2] <= parent[2] + tolerance
        and child[3] <= parent[3] + tolerance
    )


def evaluate_clipping_proxy(
    records: Sequence[Mapping[str, Any]],
    root_size: tuple[int, int],
    *,
    overlap_pairs: Sequence[tuple[str, str]] = (),
    containment_pairs: Sequence[tuple[str, str]] = (),
) -> dict[str, Any]:
    width, height = root_size
    by_name = {str(record["name"]): record for record in records}
    clipped: list[str] = []
    unmapped: list[str] = []
    compressed_width: list[str] = []
    compressed_height: list[str] = []
    for record in records:
        name = str(record["name"])
        if not record.get("mapped"):
            if record.get("critical", True):
                unmapped.append(name)
            continue
        left, top, right, bottom = map(int, record["bbox"])
        if (
            right - left <= 1
            or bottom - top <= 1
            or left < -1
            or top < -1
            or right > width + 1
            or bottom > height + 1
        ):
            clipped.append(name)
        actual = record.get("size", (0, 0))
        requested = record.get("requested_size", actual)
        if record.get("check_requested_width") and int(requested[0]) > int(actual[0]) + 2:
            compressed_width.append(name)
        if record.get("check_requested_height") and int(requested[1]) > int(actual[1]) + 2:
            compressed_height.append(name)
    overlaps = []
    for first, second in overlap_pairs:
        a, b = by_name.get(first), by_name.get(second)
        if a and b and a.get("mapped") and b.get("mapped") and _boxes_overlap(a["bbox"], b["bbox"]):
            overlaps.append([first, second])
    outside = []
    for child_name, parent_name in containment_pairs:
        child, parent = by_name.get(child_name), by_name.get(parent_name)
        if (
            child
            and parent
            and child.get("mapped")
            and parent.get("mapped")
            and not _inside(child["bbox"], parent["bbox"])
        ):
            outside.append({"widget": child_name, "container": parent_name})
    count = (
        len(clipped)
        + len(unmapped)
        + len(compressed_width)
        + len(compressed_height)
        + len(overlaps)
        + len(outside)
    )
    return {
        "clipped_or_zero_sized_widgets": clipped,
        "unmapped_critical_widgets": unmapped,
        "width_compressed_widgets": compressed_width,
        "height_compressed_widgets": compressed_height,
        "overlaps": overlaps,
        "outside_containers": outside,
        "issue_count": count,
        "suspected": bool(count),
    }


def _tree_rows(tree: Any) -> list[dict[str, Any]]:
    rows = []
    for iid in tree.get_children(""):
        item = tree.item(iid)
        values = [str(value or "") for value in item.get("values", ())]
        rows.append(
            {
                "iid": str(iid),
                "text": str(item.get("text") or ""),
                "values": values,
                "tags": [str(value) for value in item.get("tags", ())],
            }
        )
    return rows


def _row_text(row: Mapping[str, Any]) -> str:
    return " | ".join(
        [str(row.get("text") or ""), *[str(value) for value in row.get("values", ())]]
    )


def expected_presenter_rows(view: Any) -> list[dict[str, Any]]:
    return [
        {
            "index": int(slot.index),
            "label": str(slot.label),
            "value": str(slot.value or ""),
            "state": str(slot.state),
        }
        for slot in view.slots
    ]


def validate_presenter_rows(
    rendered_rows: Sequence[Mapping[str, Any]],
    presenter_rows: Sequence[Mapping[str, Any]],
) -> list[str]:
    issues: list[str] = []
    if len(rendered_rows) != len(presenter_rows):
        return [
            f"qa_row_count_mismatch:{len(rendered_rows)}!={len(presenter_rows)}"
        ]
    for offset, (rendered, expected) in enumerate(zip(rendered_rows, presenter_rows), 1):
        text = _row_text(rendered)
        if str(expected["label"]) not in text:
            issues.append(f"qa_row_{offset}_missing_presenter_label")
        value = str(expected.get("value") or "")
        if value and value not in text:
            issues.append(f"qa_row_{offset}_missing_presenter_value")
        tags = {str(tag) for tag in rendered.get("tags", ())}
        if str(expected["state"]) not in tags:
            issues.append(f"qa_row_{offset}_missing_presenter_state_tag")
    return issues


def validate_exact_rows(
    rendered_rows: Sequence[Mapping[str, Any]], exact_barcodes: Sequence[str]
) -> list[str]:
    issues = []
    if len(rendered_rows) != len(exact_barcodes):
        issues.append(
            f"exact_row_count_mismatch:{len(rendered_rows)}!={len(exact_barcodes)}"
        )
        return issues
    for offset, (row, barcode) in enumerate(zip(rendered_rows, exact_barcodes), 1):
        if str(barcode) not in _row_text(row):
            issues.append(f"exact_row_{offset}_missing_barcode")
    return issues


def _descendants(widget: Any) -> Iterable[Any]:
    for child in widget.winfo_children():
        yield child
        yield from _descendants(child)


def _visible_texts(widget: Any) -> list[str]:
    result: list[str] = []
    for candidate in (widget, *_descendants(widget)):
        if not _is_mapped(candidate):
            continue
        try:
            text = str(candidate.cget("text") or "").strip()
        except Exception:
            text = ""
        if text:
            result.append(text)
    return result


def _count_text(texts: Sequence[str], needle: str) -> int:
    if not needle:
        return 0
    return sum(1 for text in texts if needle in text)


def _resolve_widgets(app: Any) -> dict[str, Any]:
    cancel_name, cancel = _find_cancel_button(app)
    widgets = {name: getattr(app, name) for name in REQUIRED_WIDGET_ATTRS}
    widgets["cancel_button"] = cancel
    widgets["cancel_button_attr"] = cancel_name
    return widgets


def expected_scan_tree_mapping(fixture: StateFixture | None, app: Any) -> dict[str, bool]:
    """Return which central live-list widgets must be mapped for this state."""

    if fixture is not None:
        exact_mode = bool(fixture.exact_active)
    else:
        current = dict(getattr(app, "current_set_info", {}) or {})
        exact_mode = bool(current.get("exact_rescan_active"))
    return {
        "current_set_tree": not exact_mode,
        "exact_rescan_tree": exact_mode,
    }


def collect_ui_geometry(
    app: Any, fixture: StateFixture | None = None
) -> dict[str, Any]:
    widgets = _resolve_widgets(app)
    root_size = (int(app.winfo_width()), int(app.winfo_height()))
    tree_mapping = expected_scan_tree_mapping(fixture, app)
    specs: list[tuple[str, Any, bool, bool]] = [
        ("workbench", widgets["workbench_frame"], True, False),
        ("left_card", widgets["left_context_card"], True, False),
        ("center_card", widgets["top_card"], True, False),
        ("right_card", widgets["right_activity_card"], True, False),
        ("entry", widgets["entry"], True, True),
        ("notice", widgets["workflow_notice_frame"], True, False),
        (
            "current_set_tree",
            widgets["current_set_tree"],
            tree_mapping["current_set_tree"],
            False,
        ),
        (
            "exact_rescan_tree",
            widgets["exact_rescan_tree"],
            tree_mapping["exact_rescan_tree"],
            False,
        ),
        ("history_notebook", widgets["operator_history_notebook"], True, False),
        ("session_tree", widgets["session_tree"], False, False),
        ("history_tree", widgets["history_tree"], False, False),
        ("summary_tree", widgets["summary_tree"], False, False),
        ("bottom_frame", widgets["bottom_frame"], True, False),
        ("reset_button", widgets["reset_button"], True, True),
        ("cancel_button", widgets["cancel_button"], True, True),
        ("manual_complete_button", widgets["manual_complete_button"], True, True),
        ("exact_rescan_button", widgets["exact_rescan_button"], True, True),
    ]
    for index, label in enumerate(app.step_labels, 1):
        specs.append((f"step_{index}", label, True, True))
    records = [
        _widget_record(
            app,
            widget,
            name,
            critical=critical,
            check_requested_width=check_width,
        )
        for name, widget, critical, check_width in specs
    ]
    records.extend(
        (
            _widget_record(
                app,
                widgets["workflow_notice_title_label"],
                "notice_title",
                critical=True,
                check_requested_width=True,
                check_requested_height=True,
            ),
            _widget_record(
                app,
                widgets["workflow_notice_label"],
                "notice_message",
                critical=True,
                check_requested_width=True,
                check_requested_height=True,
            ),
            _widget_record(
                app,
                widgets["workflow_notice_action_button"],
                "notice_action",
                critical=False,
                check_requested_width=True,
                check_requested_height=True,
            ),
        )
    )
    containment = [
        ("left_card", "workbench"),
        ("center_card", "workbench"),
        ("right_card", "workbench"),
        ("entry", "center_card"),
        ("notice", "center_card"),
        ("notice_title", "notice"),
        ("notice_message", "notice"),
        ("notice_action", "notice"),
        ("current_set_tree", "center_card"),
        ("history_notebook", "right_card"),
        ("session_tree", "history_notebook"),
        ("history_tree", "history_notebook"),
        ("summary_tree", "history_notebook"),
        ("reset_button", "bottom_frame"),
        ("cancel_button", "bottom_frame"),
        ("manual_complete_button", "bottom_frame"),
        ("exact_rescan_button", "bottom_frame"),
        *((f"step_{index}", "center_card") for index in range(1, 6)),
    ]
    overlaps = [
        ("left_card", "center_card"),
        ("left_card", "right_card"),
        ("center_card", "right_card"),
        ("notice", "entry"),
        ("entry", "current_set_tree"),
        ("current_set_tree", "bottom_frame"),
    ]
    button_names = (
        "reset_button",
        "cancel_button",
        "manual_complete_button",
        "exact_rescan_button",
    )
    overlaps.extend(
        (button_names[first], button_names[second])
        for first in range(len(button_names))
        for second in range(first + 1, len(button_names))
    )
    by_name = {record["name"]: record for record in records}
    center_list_below_input = (
        by_name["current_set_tree"]["bbox"][1] >= by_name["entry"]["bbox"][3] - 1
    )
    # Count only public/private aliases of the presenter-owned notice surface.
    # Internal action/content frames may legitimately live inside that one
    # surface and must not be mistaken for duplicate notices.
    notice_frame_attrs = {
        id(value)
        for key, value in vars(app).items()
        if key in {"workflow_notice_frame", "_workflow_notice_frame"}
        and value is not None
        and _is_mapped(value)
    }
    current_tree = widgets["current_set_tree"]
    exact_tree = widgets["exact_rescan_tree"]
    return {
        "root_size": list(root_size),
        "widgets": records,
        "clipping_proxy": evaluate_clipping_proxy(
            records,
            root_size,
            overlap_pairs=tuple(overlaps),
            containment_pairs=tuple(containment),
        ),
        "structure": {
            "three_distinct_cards": len(
                {
                    id(widgets["left_context_card"]),
                    id(widgets["top_card"]),
                    id(widgets["right_activity_card"]),
                }
            )
            == 3,
            "current_and_exact_trees_are_distinct": current_tree is not exact_tree,
            "center_current_list_below_scan_input": center_list_below_input,
            "mapped_workflow_notice_frame_count": len(notice_frame_attrs),
            "cancel_button_attr": widgets["cancel_button_attr"],
            "center_list_signature": {
                "path": str(current_tree),
                "master_path": str(getattr(current_tree, "master", "")),
                "mapped": by_name["current_set_tree"]["mapped"],
                "bbox": by_name["current_set_tree"]["bbox"],
                "grid": by_name["current_set_tree"]["grid"],
            },
            "layout_signature": {
                record["name"]: {
                    "path": record["path"],
                    "master_path": record["master_path"],
                    "bbox": record["bbox"],
                    "grid": record["grid"],
                }
                for record in records
                if record["name"]
                in {
                    "left_card",
                    "center_card",
                    "right_card",
                    "entry",
                    "notice",
                    "current_set_tree",
                    "bottom_frame",
                }
            },
        },
    }


def collect_rendered_state(app: Any, fixture: StateFixture, view: Any) -> dict[str, Any]:
    widgets = _resolve_widgets(app)
    current_rows = _tree_rows(widgets["current_set_tree"])
    exact_rows = _tree_rows(widgets["exact_rescan_tree"])
    root_texts = _visible_texts(app)
    center_texts = _visible_texts(widgets["top_card"])
    right_texts = _visible_texts(widgets["right_activity_card"])
    button_states = {}
    for name in (
        "reset_button",
        "cancel_button",
        "manual_complete_button",
        "exact_rescan_button",
    ):
        try:
            button_states[name] = str(widgets[name].cget("state"))
        except Exception:
            button_states[name] = "unknown"
    try:
        entry_state = str(widgets["entry"].cget("state"))
    except Exception:
        entry_state = "unknown"
    notice_action_mapped = _is_mapped(widgets["workflow_notice_action_button"])
    try:
        notice_action_text = str(
            widgets["workflow_notice_action_button"].cget("text") or ""
        )
    except Exception:
        notice_action_text = ""
    notice = view.notice
    current_tree_mapped = _is_mapped(widgets["current_set_tree"])
    exact_tree_mapped = _is_mapped(widgets["exact_rescan_tree"])
    explicit_last_normal_occurrences = _count_text(
        root_texts, fixture.last_normal_scan
    )
    actual_list_last_normal_occurrences = sum(
        1
        for row in (
            *(current_rows if current_tree_mapped else ()),
            *(exact_rows if exact_tree_mapped else ()),
        )
        if fixture.last_normal_scan
        and fixture.last_normal_scan in _row_text(row)
    )
    # Tree rows are not returned by ``cget('text')``.  Prefer the explicit
    # screen-wide widget count (normally the center footer), and fall back to
    # the currently mapped actual scan list when no explicit surface exists.
    last_normal_occurrences_on_screen = (
        explicit_last_normal_occurrences
        if explicit_last_normal_occurrences
        else actual_list_last_normal_occurrences
    )
    return {
        "current_set_rows": current_rows,
        "exact_rescan_rows": exact_rows,
        "session_row_count": len(_tree_rows(widgets["session_tree"])),
        "history_row_count": len(_tree_rows(widgets["history_tree"])),
        "summary_row_count": len(_tree_rows(widgets["summary_tree"])),
        "presenter_rows": expected_presenter_rows(view),
        "presenter_stage": str(view.current_stage),
        "presenter_stage_label": str(view.current_stage_label),
        "presenter_next_action": str(view.next_action),
        "presenter_last_normal_scan": str(view.last_normal_scan or ""),
        "presenter_notice": (
            {
                "title": str(notice.title),
                "message": str(notice.message),
                "kind": str(notice.kind),
                "tone": str(notice.tone),
            }
            if notice is not None
            else None
        ),
        "entry_state": entry_state,
        "notice_action_mapped": notice_action_mapped,
        "notice_action_text": notice_action_text,
        "button_states": button_states,
        "center_visible_texts": center_texts,
        "right_visible_texts": right_texts,
        "notice_title_occurrences": _count_text(root_texts, str(notice.title)) if notice else 0,
        "notice_message_occurrences": _count_text(root_texts, str(notice.message)) if notice else 0,
        "last_normal_occurrences_on_screen": last_normal_occurrences_on_screen,
        "last_normal_occurrences_in_center": _count_text(
            center_texts, fixture.last_normal_scan
        ),
        "last_normal_occurrences_in_actual_list": actual_list_last_normal_occurrences,
        "last_normal_occurrences_in_right": _count_text(
            right_texts, fixture.last_normal_scan
        ),
        "current_tree_mapped": current_tree_mapped,
        "exact_tree_mapped": exact_tree_mapped,
        "history_tree_mapped": _is_mapped(widgets["history_tree"]),
        "session_tree_mapped": _is_mapped(widgets["session_tree"]),
    }


def evaluate_capture(record: Mapping[str, Any]) -> list[str]:
    issues: list[str] = []
    image = record["image_analysis"]
    geometry = record["ui_geometry"]
    structure = geometry["structure"]
    rendered = record["rendered_state"]
    fixture = record["fixture"]
    if not image.get("pixel_size_matches"):
        issues.append("capture_size_mismatch")
    if image.get("blank_suspected"):
        issues.append("blank_image_suspected")
    if float(image.get("near_black_ratio", 1.0)) > NEAR_BLACK_FAILURE_RATIO:
        issues.append("near_black_ratio_exceeded")
    if not math.isclose(
        float(record.get("requested_scale", 0)),
        float(record.get("applied_scale_factor", -1)),
        rel_tol=0,
        abs_tol=0.001,
    ):
        issues.append("scale_factor_not_applied")
    if geometry["clipping_proxy"].get("suspected"):
        issues.append("clipping_or_overlap_suspected")
    if not structure.get("three_distinct_cards"):
        issues.append("three_card_contract_failed")
    if not structure.get("current_and_exact_trees_are_distinct"):
        issues.append("qa_and_exact_lists_are_not_separate")
    if not structure.get("center_current_list_below_scan_input"):
        issues.append("current_scan_list_not_below_input")
    if structure.get("mapped_workflow_notice_frame_count") != 1:
        issues.append("workflow_notice_frame_not_single")
    issues.extend(
        validate_presenter_rows(rendered["current_set_rows"], rendered["presenter_rows"])
    )
    issues.extend(
        validate_exact_rows(
            rendered["exact_rescan_rows"], fixture.get("exact_barcodes", ())
        )
    )
    qa_text = "\n".join(_row_text(row) for row in rendered["current_set_rows"])
    if any(str(value) in qa_text for value in fixture.get("exact_barcodes", ())):
        issues.append("exact_rescan_member_leaked_into_qa_list")
    if fixture.get("last_normal_scan"):
        if rendered.get("presenter_last_normal_scan") != fixture["last_normal_scan"]:
            issues.append("presenter_last_normal_scan_not_preserved")
        if rendered.get("last_normal_occurrences_on_screen") != 1:
            issues.append("last_normal_scan_missing_or_duplicated_on_screen")
    notice = rendered.get("presenter_notice")
    if notice:
        if rendered.get("notice_title_occurrences") != 1:
            issues.append("notice_title_missing_or_duplicated")
        if rendered.get("notice_message_occurrences") != 1:
            issues.append("notice_message_missing_or_duplicated")
    expected_notice_action = record["state"] in {"error", "submission_blocked"}
    if bool(rendered.get("notice_action_mapped")) != expected_notice_action:
        issues.append("notice_action_mapping_mismatch")
    if record["state"] == "error" and "확인" not in str(
        rendered.get("notice_action_text") or ""
    ):
        issues.append("error_notice_action_text_mismatch")
    if record["state"] == "submission_blocked" and "제출 재시도" not in str(
        rendered.get("notice_action_text") or ""
    ):
        issues.append("submission_notice_action_text_mismatch")
    center_text = "\n".join(rendered.get("center_visible_texts", ()))
    if rendered.get("presenter_stage_label") not in center_text:
        issues.append("presenter_stage_label_not_visible_in_center")
    if rendered.get("presenter_next_action") not in center_text:
        issues.append("presenter_next_action_not_visible_in_center")
    blocked = record["state"] in {"error", "history_readonly", "submission_blocked"}
    entry_state = str(rendered.get("entry_state") or "")
    if blocked and entry_state not in {"disabled", "readonly"}:
        issues.append("blocked_state_scan_entry_enabled")
    if not blocked and entry_state != "normal":
        issues.append("active_state_scan_entry_disabled")
    if record["state"] == "history_readonly" and not rendered.get("history_tree_mapped"):
        issues.append("history_readonly_tree_not_visible")
    expected_exact_mapping = record["state"] == "exact_active"
    if bool(rendered.get("exact_tree_mapped")) != expected_exact_mapping:
        issues.append("exact_rescan_tree_mapping_mismatch")
    if bool(rendered.get("current_tree_mapped")) == expected_exact_mapping:
        issues.append("current_set_tree_mapping_mismatch")
    return issues


def _stable_scan_values(rows: Sequence[Mapping[str, Any]]) -> list[list[str]]:
    """Return ordered QA identity/value pairs, excluding mutable status UI."""

    stable: list[list[str]] = []
    for index, row in enumerate(rows, 1):
        text = str(row.get("text") or "")
        values = [str(value) for value in row.get("values", ())]
        if len(values) >= 3:
            # Real workbench rows are (stage, scanned value, status).
            stable.append([values[0], values[1]])
        elif values:
            # Synthetic/compatibility rows keep the stage in ``text`` and the
            # scanned value first.  Any trailing value is display state.
            stable.append([text or str(index), values[0]])
        else:
            stable.append([str(index), text])
    return stable


def _stable_mapped_center_signature(
    signature: Mapping[str, Any]
) -> dict[str, Any] | None:
    """Normalize mapped-list placement while allowing state-driven height."""

    if not signature.get("mapped"):
        return None
    bbox = tuple(signature.get("bbox", ()))
    horizontal = [int(bbox[0]), int(bbox[2])] if len(bbox) == 4 else []
    return {
        "path": signature.get("path"),
        "master_path": signature.get("master_path"),
        "grid": signature.get("grid"),
        "horizontal_bbox": horizontal,
    }


def apply_cross_capture_contracts(captures: list[dict[str, Any]]) -> None:
    """Apply state-pair and stable-layout checks after individual captures."""

    by_size: dict[tuple[int, int], dict[str, dict[str, Any]]] = {}
    for capture in captures:
        by_size.setdefault(tuple(capture["requested_size"]), {})[capture["state"]] = capture
    for group in by_size.values():
        signatures = [
            _stable_mapped_center_signature(
                capture["ui_geometry"]["structure"]["center_list_signature"]
            )
            for capture in group.values()
            if "ui_geometry" in capture
        ]
        signatures = [signature for signature in signatures if signature is not None]
        if signatures:
            first = signatures[0]
            for capture in group.values():
                signature = _stable_mapped_center_signature(
                    capture["ui_geometry"]["structure"]["center_list_signature"]
                )
                if signature is not None and signature != first:
                    capture["issues"].append("center_scan_list_geometry_changed_across_states")
        for normal_id, blocked_id in (
            ("qa_progress", "error"),
            ("full_complete", "submission_blocked"),
        ):
            normal, blocked = group.get(normal_id), group.get(blocked_id)
            if not normal or not blocked:
                continue
            normal_rows = _stable_scan_values(normal["rendered_state"]["current_set_rows"])
            blocked_rows = _stable_scan_values(blocked["rendered_state"]["current_set_rows"])
            if normal_rows != blocked_rows:
                blocked["issues"].append("last_normal_qa_rows_not_preserved")
        for capture in group.values():
            capture["issues"] = list(dict.fromkeys(capture["issues"]))
            capture["passed"] = not capture["issues"]


def compare_layout_signatures(
    before: Mapping[str, Mapping[str, Any]],
    after: Mapping[str, Mapping[str, Any]],
    *,
    tolerance: int = 2,
) -> list[str]:
    issues: list[str] = []
    if set(before) != set(after):
        return ["layout_signature_widget_set_changed"]
    for name in before:
        first, second = before[name], after[name]
        if first.get("path") != second.get("path"):
            issues.append(f"{name}:widget_replaced")
        if first.get("master_path") != second.get("master_path"):
            issues.append(f"{name}:parent_changed")
        if first.get("grid") != second.get("grid"):
            issues.append(f"{name}:grid_changed")
        first_box, second_box = first.get("bbox", ()), second.get("bbox", ())
        if len(first_box) == 4 and len(second_box) == 4 and any(
            abs(int(a) - int(b)) > tolerance for a, b in zip(first_box, second_box)
        ):
            issues.append(f"{name}:geometry_accumulated")
    return issues


def _configure_size(app: Any, size: tuple[int, int]) -> None:
    app.state("normal")
    app.resizable(True, True)
    app.geometry(f"{size[0]}x{size[1]}+0+0")
    pump_tk(app, 320)


def _apply_scale(app: Any, scale: float) -> None:
    app.scale_factor = float(scale)
    for name in ("_update_ui_scaling", "_apply_operator_layout"):
        method = getattr(app, name, None)
        if callable(method):
            method()
            break
    pump_tk(app, 180)


def _wait_until_ready(app: Any, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        app.update()
        if bool(getattr(app, "initialized_successfully", False)):
            pump_tk(app, 220)
            return
        time.sleep(0.02)
    raise TimeoutError("Label Match did not initialize within capture timeout")


def _make_capture_app(module: Any, settings: dict[str, Any]) -> Any:
    class CaptureLabelMatch(module.Label_Match):
        def _load_app_settings(self) -> dict[str, Any]:
            # JSON round-trip detaches nested dictionaries from this harness.
            return json.loads(json.dumps(settings, ensure_ascii=False))

    return CaptureLabelMatch(run_tests=True)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _round_trip_check(
    app: Any,
    compact: tuple[int, int],
    wide: tuple[int, int],
    fixture: StateFixture,
) -> dict[str, Any]:
    _configure_size(app, compact)
    view, _ = apply_state_fixture(app, fixture)
    pump_tk(app, 500)
    before = collect_ui_geometry(app, fixture)["structure"]["layout_signature"]
    _configure_size(app, wide)
    apply_state_fixture(app, fixture)
    pump_tk(app, 500)
    wide_signature = collect_ui_geometry(app, fixture)["structure"]["layout_signature"]
    _configure_size(app, compact)
    apply_state_fixture(app, fixture)
    pump_tk(app, 500)
    after = collect_ui_geometry(app, fixture)["structure"]["layout_signature"]
    issues = compare_layout_signatures(before, after)
    return {
        "fixture": fixture.state_id,
        "compact_size": list(compact),
        "wide_size": list(wide),
        "presenter_stage": str(view.current_stage),
        "before": before,
        "wide": wide_signature,
        "after": after,
        "issues": issues,
        "passed": not issues,
    }


def run_capture_matrix(
    *,
    output_root: Path,
    sizes: Sequence[tuple[int, int]] = DEFAULT_SIZES,
    state_ids: Sequence[str] = DEFAULT_STATE_IDS,
    scale: float = DEFAULT_SCALE,
) -> tuple[Path, dict[str, Any]]:
    requested_scale = parse_scale(scale)
    resolved_output = assert_descendant(output_root, REPO_TMP_ROOT, label="output root")
    resolved_output.mkdir(parents=True, exist_ok=True)
    screenshots = resolved_output / "screenshots"
    screenshots.mkdir(parents=True, exist_ok=True)
    data_root = resolved_output / "_isolated_data"
    guards = prepare_isolated_environment(data_root)
    dpi_mode = enable_per_monitor_dpi_awareness()
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    module = importlib.import_module("Label_Match")
    settings = build_isolated_app_settings(data_root, requested_scale)
    fixture_map = {fixture.state_id: fixture for fixture in build_state_fixtures()}
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "tool": "tools/capture_label_operator_ui.py",
        "generated_at": dt.datetime.now(dt.timezone.utc).astimezone().isoformat(),
        "repository_root": str(ROOT),
        "output_root": str(resolved_output),
        "data_root": str(data_root.resolve()),
        "isolation_guards": guards,
        "dpi_awareness": dpi_mode,
        "requested_sizes": [list(size) for size in sizes],
        "requested_states": list(state_ids),
        "requested_scale": requested_scale,
        "near_black_failure_ratio": NEAR_BLACK_FAILURE_RATIO,
        "captures": [],
    }
    app = None
    manifest_path = resolved_output / "manifest.json"
    try:
        app = _make_capture_app(module, settings)
        _wait_until_ready(app)
        _apply_scale(app, requested_scale)
        contract_issues = validate_live_contract(app)
        manifest["live_contract_ready"] = not contract_issues
        manifest["live_contract_issues"] = contract_issues
        if contract_issues:
            manifest["summary"] = {
                "capture_count": 0,
                "expected_capture_count": len(sizes) * len(state_ids),
                "passed_capture_count": 0,
                "failed_capture_count": 0,
                "passed": False,
                "fatal_error": "operator_workbench_contract_missing",
            }
            return manifest_path, manifest
        manifest["applied_scale_factor"] = float(app.scale_factor)
        compact = min(sizes, key=lambda value: (value[0], value[1]))
        wide = max(sizes, key=lambda value: (value[0], value[1]))
        manifest["compact_wide_compact"] = _round_trip_check(
            app, compact, wide, fixture_map["qa_progress"]
        )
        for size in sizes:
            _configure_size(app, tuple(size))
            size_dir = screenshots / f"{size[0]}x{size[1]}"
            size_dir.mkdir(parents=True, exist_ok=True)
            for state_id in state_ids:
                fixture = fixture_map[state_id]
                view, refresh_method = apply_state_fixture(app, fixture)
                pump_tk(app, 260)
                geometry = collect_ui_geometry(app, fixture)
                rendered = collect_rendered_state(app, fixture, view)
                image, source = capture_tk_client(app)
                path = size_dir / f"{state_id}.png"
                image.save(path, format="PNG", optimize=True)
                record: dict[str, Any] = {
                    "id": f"{size[0]}x{size[1]}-{state_id}",
                    "state": state_id,
                    "state_label": fixture.label,
                    "requested_size": list(size),
                    "requested_scale": requested_scale,
                    "applied_scale_factor": float(app.scale_factor),
                    "path": path.relative_to(resolved_output).as_posix(),
                    "capture_source": source,
                    "sha256": _sha256(path),
                    "file_size_bytes": path.stat().st_size,
                    "presenter_refresh_method": refresh_method,
                    "fixture": asdict(fixture),
                    "image_analysis": analyze_image(image, tuple(size)),
                    "ui_geometry": geometry,
                    "rendered_state": rendered,
                }
                record["issues"] = evaluate_capture(record)
                record["passed"] = not record["issues"]
                manifest["captures"].append(record)
        apply_cross_capture_contracts(manifest["captures"])
        issue_counts: dict[str, int] = {}
        for capture in manifest["captures"]:
            for issue in capture["issues"]:
                issue_counts[issue] = issue_counts.get(issue, 0) + 1
        expected_count = len(sizes) * len(state_ids)
        round_trip_ok = bool(manifest["compact_wide_compact"]["passed"])
        manifest["summary"] = {
            "capture_count": len(manifest["captures"]),
            "expected_capture_count": expected_count,
            "passed_capture_count": sum(
                1 for capture in manifest["captures"] if capture["passed"]
            ),
            "failed_capture_count": sum(
                1 for capture in manifest["captures"] if not capture["passed"]
            ),
            "compact_wide_compact_passed": round_trip_ok,
            "issue_counts": issue_counts,
            "passed": len(manifest["captures"]) == expected_count
            and not issue_counts
            and round_trip_ok,
        }
        return manifest_path, manifest
    except Exception as exc:
        manifest["live_contract_ready"] = False
        manifest.setdefault("live_contract_issues", [])
        manifest["summary"] = {
            "capture_count": len(manifest["captures"]),
            "expected_capture_count": len(sizes) * len(state_ids),
            "passed_capture_count": 0,
            "failed_capture_count": len(manifest["captures"]),
            "passed": False,
            "fatal_error": f"{type(exc).__name__}: {exc}",
        }
        return manifest_path, manifest
    finally:
        if app is not None:
            try:
                app.destroy()
            except Exception:
                pass
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )


def build_parser() -> argparse.ArgumentParser:
    timestamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    parser = argparse.ArgumentParser(
        description=(
            "Capture isolated Label Match operator-workbench states and write "
            "PNG screenshots plus a strict pixel/geometry/content manifest."
        )
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=REPO_TMP_ROOT / f"label_operator_ui_capture_{timestamp}",
        help=f"new/output directory below {REPO_TMP_ROOT}",
    )
    parser.add_argument(
        "--sizes",
        type=parse_sizes,
        default=DEFAULT_SIZES,
        help="comma-separated client sizes, for example 1366x768,1440x900",
    )
    parser.add_argument(
        "--states",
        type=parse_states,
        default=DEFAULT_STATE_IDS,
        help=f"comma-separated states: {','.join(DEFAULT_STATE_IDS)}",
    )
    parser.add_argument(
        "--scale",
        type=parse_scale,
        default=DEFAULT_SCALE,
        help=f"UI scale from {MIN_SCALE} to {MAX_SCALE}",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="return 2 when any completed capture check fails",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    manifest_path, manifest = run_capture_matrix(
        output_root=args.output_root,
        sizes=args.sizes,
        state_ids=args.states,
        scale=args.scale,
    )
    summary = manifest["summary"]
    print(
        json.dumps(
            {
                "manifest": str(manifest_path),
                "live_contract_ready": manifest.get("live_contract_ready", False),
                "capture_count": summary["capture_count"],
                "passed": summary["passed"],
                "fatal_error": summary.get("fatal_error"),
                "issue_counts": summary.get("issue_counts", {}),
            },
            ensure_ascii=False,
        )
    )
    if summary.get("fatal_error"):
        return 3
    return 0 if summary["passed"] or not args.strict else 2


if __name__ == "__main__":
    raise SystemExit(main())

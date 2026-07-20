"""Capture the v2.0.36 Label_Match operator manual in a sealed demo run.

The capture is deliberately local-only:

* scan data and DirectSync status are redirected to a temporary directory;
* bootstrap/session sync and updater checks are disabled;
* every logistics API environment variable is removed before import;
* the repository settings file is restored byte-for-byte if Tk touches it.

Each evidence image is saved twice: an untouched DISPLAY2 monitor capture and
an annotated copy. Annotation rectangles come from Tk ``winfo_root*`` or
Win32 ``GetWindowRect`` coordinates and are preserved in ``manifest.json``.
The application is imported only from a clean, exact commit worktree while the
capture tool and output directory may live elsewhere.
"""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any, Iterable

from PIL import Image, ImageChops, ImageDraw, ImageFont, ImageOps

import win32con
import win32api
import win32gui
import win32process


TOOL_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_ROOT = Path(
    os.environ.get("LABEL_MATCH_MANUAL_SOURCE_ROOT", TOOL_ROOT)
).resolve()
DEFAULT_ASSET_ROOT = Path(
    os.environ.get(
        "LABEL_MATCH_MANUAL_ASSET_ROOT",
        TOOL_ROOT
        / "docs"
        / "assets"
        / "label_match_user_manual_20260716_display2_v2_0_36",
    )
).resolve()
EXPECTED_SOURCE_COMMIT = "faaca1c7783e2e7a91b0fea862e23eefefde09bd"
EXPECTED_SOURCE_TREE = "3d169822fae1cf978b3623cfbb433e5e647615bb"
EXPECTED_APP_VERSION = "v2.0.36"
TARGET_DISPLAY_DEVICE = r"\\.\DISPLAY2"

SOURCE_ROOT = DEFAULT_SOURCE_ROOT
ASSET_ROOT = DEFAULT_ASSET_ROOT
RAW_DIR = ASSET_ROOT / "raw"
ANNOTATED_DIR = ASSET_ROOT / "annotated"
MANIFEST_PATH = ASSET_ROOT / "manifest.json"
CONTACT_SHEET_PATH = ASSET_ROOT / "contact_sheet.png"
CAPTURE_GEOMETRY: tuple[int, int, int, int] | None = None
RED = (220, 24, 24)
ui: Any | None = None

LOGISTICS_ENV_KEYS = (
    "LABEL_MATCH_LOGISTICS_API_BASE_URL",
    "LABEL_MATCH_LOGISTICS_API_TOKEN",
    "LABEL_MATCH_LOGISTICS_AUTHORITY_SCOPE_ID",
    "LABEL_MATCH_LOGISTICS_SOURCE_HOST_ID",
    "LABEL_MATCH_LOGISTICS_DEVICE_ID",
    "WORKER_ANALYSIS_LOGISTICS_API_BASE_URL",
    "WORKER_ANALYSIS_LOGISTICS_API_TOKEN",
    "WORKER_ANALYSIS_LOGISTICS_AUTHORITY_SCOPE_ID",
    "WORKER_ANALYSIS_LOGISTICS_SOURCE_HOST_ID",
)


def _trace(event: str, **fields: Any) -> None:
    print(
        json.dumps(
            {"capture_trace": event, **fields},
            ensure_ascii=False,
            sort_keys=True,
        ),
        flush=True,
    )


def _run_git(source_root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(source_root), *args],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return completed.stdout.strip()


def _source_identity(
    source_root: Path,
    *,
    expected_commit: str = EXPECTED_SOURCE_COMMIT,
    expected_tree: str = EXPECTED_SOURCE_TREE,
) -> dict[str, Any]:
    source_root = source_root.resolve()
    required = (
        source_root / "Label_Match.py",
        source_root / "tools" / "label_match_operator_ui_walkthrough.py",
    )
    missing = [path.name for path in required if not path.is_file()]
    if missing:
        raise RuntimeError(f"exact source root is incomplete: {missing}")
    commit = _run_git(source_root, "rev-parse", "HEAD")
    tree = _run_git(source_root, "rev-parse", "HEAD^{tree}")
    dirty_lines = _run_git(
        source_root,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
    ).splitlines()
    if commit != expected_commit:
        raise RuntimeError(f"source commit mismatch: expected {expected_commit}, got {commit}")
    if tree != expected_tree:
        raise RuntimeError(f"source tree mismatch: expected {expected_tree}, got {tree}")
    if dirty_lines:
        raise RuntimeError(f"exact source worktree is dirty: {dirty_lines[:5]}")
    return {
        "commit": commit,
        "tree": tree,
        "worktree_clean": True,
        "source_ref": "exact-clean-worktree",
    }


def _load_module(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _load_exact_modules(source_root: Path) -> tuple[Any, Any]:
    source_text = str(source_root.resolve())
    sys.path.insert(0, source_text)
    try:
        exact_ui = _load_module(
            "_label_manual_exact_ui",
            source_root / "tools" / "label_match_operator_ui_walkthrough.py",
        )
        exact_app = _load_module(
            "_label_manual_exact_app",
            source_root / "Label_Match.py",
        )
    finally:
        if sys.path and sys.path[0] == source_text:
            sys.path.pop(0)
    return exact_ui, exact_app


def _monitor_dpi(handle: Any) -> tuple[int, int]:
    x_dpi = ctypes.c_uint(0)
    y_dpi = ctypes.c_uint(0)
    try:
        result = ctypes.windll.shcore.GetDpiForMonitor(
            int(handle),
            0,
            ctypes.byref(x_dpi),
            ctypes.byref(y_dpi),
        )
    except Exception as exc:
        raise RuntimeError(f"cannot query monitor DPI: {exc}") from exc
    if result != 0 or not x_dpi.value or not y_dpi.value:
        raise RuntimeError(f"GetDpiForMonitor failed: HRESULT={result}")
    return int(x_dpi.value), int(y_dpi.value)


def _set_per_monitor_dpi_awareness() -> dict[str, Any]:
    requested = 2  # PROCESS_PER_MONITOR_DPI_AWARE
    result = int(ctypes.windll.shcore.SetProcessDpiAwareness(requested))
    observed = ctypes.c_int(-1)
    observed_result = int(
        ctypes.windll.shcore.GetProcessDpiAwareness(0, ctypes.byref(observed))
    )
    if observed_result != 0 or observed.value != requested:
        raise RuntimeError(
            "capture process is not per-monitor DPI aware: "
            f"set_result={result} query_result={observed_result} observed={observed.value}"
        )
    return {
        "requested": "PROCESS_PER_MONITOR_DPI_AWARE",
        "set_hresult": result,
        "observed": int(observed.value),
        "status": "PASS",
    }


def _resolve_capture_monitor(device: str = TARGET_DISPLAY_DEVICE) -> dict[str, Any]:
    matches: list[dict[str, Any]] = []
    for handle, _dc, _rect in win32api.EnumDisplayMonitors():
        info = dict(win32api.GetMonitorInfo(handle))
        if str(info.get("Device") or "").casefold() != device.casefold():
            continue
        monitor = list(map(int, info["Monitor"]))
        work = list(map(int, info["Work"]))
        primary = bool(int(info.get("Flags", 0)) & int(win32con.MONITORINFOF_PRIMARY))
        dpi_x, dpi_y = _monitor_dpi(handle)
        matches.append(
            {
                "device": str(info["Device"]),
                "is_primary": primary,
                "monitor_rect": monitor,
                "work_rect": work,
                "dpi": [dpi_x, dpi_y],
                "capture_size": [monitor[2] - monitor[0], monitor[3] - monitor[1]],
                "window_geometry": [
                    work[2] - work[0],
                    work[3] - work[1],
                    work[0],
                    work[1],
                ],
            }
        )
    if len(matches) != 1:
        raise RuntimeError(f"expected exactly one {device} monitor, found {len(matches)}")
    target = matches[0]
    if target["is_primary"]:
        raise RuntimeError(f"capture target must be non-primary: {device}")
    return target


def _path_is_within(path: Path, root: Path) -> bool:
    resolved_path = path.resolve()
    resolved_root = root.resolve()
    return resolved_path == resolved_root or resolved_root in resolved_path.parents


def _assert_exact_module_origins(source_root: Path, exact_ui: Any, exact_app: Any) -> dict[str, Any]:
    checked: dict[str, str] = {}
    candidates = {
        "exact_ui": exact_ui,
        "exact_app": exact_app,
        **{
            name: module
            for name, module in tuple(sys.modules.items())
            if name == "package_logistics"
            or name.startswith("package_logistics.")
            or name.startswith("ui.")
        },
    }
    for name, module in candidates.items():
        module_file = getattr(module, "__file__", None)
        if not module_file:
            continue
        path = Path(module_file).resolve()
        if not _path_is_within(path, source_root):
            raise RuntimeError(f"local module was imported outside the exact source root: {name}={path}")
        checked[name] = path.relative_to(source_root.resolve()).as_posix()
    if "exact_ui" not in checked or "exact_app" not in checked:
        raise RuntimeError("exact capture modules do not expose source-root file origins")
    return {
        "status": "PASS",
        "checked_modules": dict(sorted(checked.items())),
    }


def _relative(path: Path) -> str:
    return path.relative_to(ASSET_ROOT).as_posix()


def _font(size: int = 25) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    for candidate in (
        Path(os.environ.get("WINDIR", r"C:\Windows")) / "Fonts" / "malgunbd.ttf",
        Path(os.environ.get("WINDIR", r"C:\Windows")) / "Fonts" / "malgun.ttf",
    ):
        try:
            return ImageFont.truetype(str(candidate), size=size)
        except OSError:
            continue
    return ImageFont.load_default()


def _union_rect(rects: Iterable[Iterable[int]]) -> list[int]:
    normalized = [list(map(int, rect)) for rect in rects]
    if not normalized:
        raise ValueError("at least one rectangle is required")
    return [
        min(rect[0] for rect in normalized),
        min(rect[1] for rect in normalized),
        max(rect[2] for rect in normalized),
        max(rect[3] for rect in normalized),
    ]


def _tk_rect(widgets: Iterable[Any]) -> list[int]:
    rects: list[list[int]] = []
    for widget in widgets:
        widget.update_idletasks()
        x = int(widget.winfo_rootx())
        y = int(widget.winfo_rooty())
        width = max(1, int(widget.winfo_width()))
        height = max(1, int(widget.winfo_height()))
        rects.append([x, y, x + width, y + height])
    return _union_rect(rects)


def tk_target(label: str, *widgets: Any) -> dict[str, Any]:
    return {
        "label": label,
        "source": "tk_widget_geometry",
        "absolute_rect": _tk_rect(widgets),
        "widgets": [str(widget) for widget in widgets],
    }


def hwnd_target(label: str, hwnd: int) -> dict[str, Any]:
    return {
        "label": label,
        "source": "win32_window_geometry",
        "absolute_rect": list(map(int, win32gui.GetWindowRect(hwnd))),
        "hwnd": int(hwnd),
        "window_text": win32gui.GetWindowText(hwnd) or "",
    }


def _draw_annotations(raw_path: Path, annotated_path: Path, monitor_rect: list[int], targets: list[dict[str, Any]]) -> None:
    base = Image.open(raw_path).convert("RGBA")
    overlay = Image.new("RGBA", base.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    font = _font()
    monitor_left, monitor_top = monitor_rect[0], monitor_rect[1]
    for target in targets:
        absolute = target["absolute_rect"]
        requested_rect = [
            int(absolute[0] - monitor_left),
            int(absolute[1] - monitor_top),
            int(absolute[2] - monitor_left),
            int(absolute[3] - monitor_top),
        ]
        image_rect = [
            max(0, requested_rect[0]),
            max(0, requested_rect[1]),
            min(base.width - 1, requested_rect[2]),
            min(base.height - 1, requested_rect[3]),
        ]
        if image_rect[0] >= image_rect[2] or image_rect[1] >= image_rect[3]:
            raise ValueError(
                f"annotation target is outside captured monitor: {target['label']!r} "
                f"requested={requested_rect} monitor={monitor_rect}"
            )
        target["requested_image_rect"] = requested_rect
        target["image_rect"] = image_rect
        draw.rectangle(image_rect, outline=(*RED, 255), width=7)
        label = str(target["label"])
        bbox = draw.textbbox((0, 0), label, font=font)
        label_width = bbox[2] - bbox[0] + 20
        label_height = bbox[3] - bbox[1] + 14
        label_x = max(0, min(base.width - label_width, image_rect[0]))
        label_y = image_rect[1] - label_height - 4
        if label_y < 0:
            label_y = min(base.height - label_height, image_rect[3] + 4)
        draw.rectangle(
            [label_x, label_y, label_x + label_width, label_y + label_height],
            fill=(*RED, 255),
        )
        draw.text((label_x + 10, label_y + 5), label, fill=(255, 255, 255, 255), font=font)
    annotated_path.parent.mkdir(parents=True, exist_ok=True)
    Image.alpha_composite(base, overlay).convert("RGB").save(annotated_path, optimize=True)


def _pixel_comparison(raw_path: Path, annotated_path: Path) -> dict[str, Any]:
    """Return lossless raw/annotation QA metrics without NumPy dependencies."""

    raw = Image.open(raw_path).convert("RGB")
    annotated = Image.open(annotated_path).convert("RGB")
    if raw.size != annotated.size:
        raise ValueError(f"raw/annotated dimensions differ: {raw.size} != {annotated.size}")

    raw_channels = raw.split()
    annotated_channels = annotated.split()

    def near_black_count(channels: tuple[Image.Image, ...]) -> int:
        masks = [channel.point(lambda value: 255 if value <= 16 else 0) for channel in channels]
        mask = ImageChops.multiply(ImageChops.multiply(masks[0], masks[1]), masks[2])
        return int(mask.histogram()[255])

    diff = ImageChops.difference(raw, annotated)
    diff_mask = ImageChops.lighter(ImageChops.lighter(*diff.split()[:2]), diff.split()[2])
    histogram = diff_mask.histogram()
    pixel_count = raw.width * raw.height
    changed_pixel_count = pixel_count - int(histogram[0])
    raw_near_black = near_black_count(raw_channels)
    annotated_near_black = near_black_count(annotated_channels)
    near_black_increase = max(0, annotated_near_black - raw_near_black)
    return {
        "changed_pixel_count": changed_pixel_count,
        "changed_pixel_ratio": changed_pixel_count / pixel_count,
        "diff_bbox": list(diff.getbbox()) if diff.getbbox() else None,
        "near_black_threshold_rgb": 16,
        "raw_near_black_pixel_count": raw_near_black,
        "annotated_near_black_pixel_count": annotated_near_black,
        "near_black_increase_pixel_count": near_black_increase,
        "near_black_increase_ratio": near_black_increase / pixel_count,
    }


def _canonical_root_hwnd(hwnd: int) -> int:
    try:
        return int(win32gui.GetAncestor(int(hwnd), win32con.GA_ROOT))
    except Exception:
        return int(hwnd)


def _rect_contains(outer: Iterable[int], inner: Iterable[int]) -> bool:
    outer_rect = list(map(int, outer))
    inner_rect = list(map(int, inner))
    return (
        outer_rect[0] <= inner_rect[0]
        and outer_rect[1] <= inner_rect[1]
        and inner_rect[2] <= outer_rect[2]
        and inner_rect[3] <= outer_rect[3]
    )


def _rect_matches(left: Iterable[int], right: Iterable[int], *, tolerance: int = 1) -> bool:
    return all(
        abs(first - second) <= tolerance
        for first, second in zip(map(int, left), map(int, right))
    )


def _monitor_for_hwnd(hwnd: int) -> dict[str, Any]:
    handle = win32api.MonitorFromWindow(hwnd, win32con.MONITOR_DEFAULTTONEAREST)
    info = dict(win32api.GetMonitorInfo(handle))
    return {
        "device": str(info.get("Device") or ""),
        "is_primary": bool(int(info.get("Flags", 0)) & int(win32con.MONITORINFOF_PRIMARY)),
        "monitor_rect": list(map(int, info["Monitor"])),
        "work_rect": list(map(int, info["Work"])),
    }


def _focus_exact_window(hwnd: int, *, timeout: float = 2.0) -> dict[str, Any]:
    if ui is None:
        raise RuntimeError("exact UI helper is not loaded")
    root_hwnd = _canonical_root_hwnd(hwnd)
    deadline = time.monotonic() + timeout
    latest: dict[str, Any] = {}
    while time.monotonic() < deadline:
        _force_foreground_window(root_hwnd)
        latest = ui._foreground_snapshot(root_hwnd)
        if latest.get("foreground_root_hwnd") == root_hwnd:
            latest["foreground_root_matches_target"] = True
            return latest
        time.sleep(0.08)
    latest["foreground_root_matches_target"] = False
    raise RuntimeError(
        "capture target did not become the foreground root: "
        f"target={root_hwnd} observed={latest}"
    )


def _force_foreground_window(hwnd: int) -> None:
    """Acquire foreground under Windows' thread-input lock, then focus the exact root."""

    root_hwnd = _canonical_root_hwnd(hwnd)
    user32 = ctypes.windll.user32
    current_thread = int(win32api.GetCurrentThreadId())
    foreground_hwnd = int(win32gui.GetForegroundWindow())
    foreground_thread = 0
    if foreground_hwnd:
        foreground_thread, _foreground_pid = win32process.GetWindowThreadProcessId(
            foreground_hwnd
        )
    target_thread, _target_pid = win32process.GetWindowThreadProcessId(root_hwnd)
    attached: list[int] = []
    try:
        for thread_id in {int(foreground_thread), int(target_thread)}:
            if thread_id and thread_id != current_thread:
                if user32.AttachThreadInput(current_thread, thread_id, True):
                    attached.append(thread_id)
        try:
            win32gui.ShowWindow(root_hwnd, win32con.SW_RESTORE)
        except Exception:
            pass
        try:
            win32gui.SetWindowPos(
                root_hwnd,
                win32con.HWND_TOPMOST,
                0,
                0,
                0,
                0,
                win32con.SWP_NOMOVE
                | win32con.SWP_NOSIZE
                | win32con.SWP_SHOWWINDOW,
            )
        except Exception:
            pass
        try:
            win32gui.BringWindowToTop(root_hwnd)
        except Exception:
            pass
        # A synthetic ALT transition is the documented Windows-compatible way
        # to release the foreground lock for an interactive process.
        win32api.keybd_event(win32con.VK_MENU, 0, 0, 0)
        win32api.keybd_event(win32con.VK_MENU, 0, win32con.KEYEVENTF_KEYUP, 0)
        try:
            win32gui.SetForegroundWindow(root_hwnd)
        except Exception:
            pass
        try:
            win32gui.SetActiveWindow(root_hwnd)
        except Exception:
            pass
        try:
            win32gui.SetFocus(root_hwnd)
        except Exception:
            pass
    finally:
        for thread_id in reversed(attached):
            user32.AttachThreadInput(current_thread, thread_id, False)
    time.sleep(0.08)


def _create_app_hidden(label_module: Any, *, run_tests: bool) -> Any:
    """Construct Tk while blocking every constructor-time attempt to show it."""

    tk_class = label_module.tk.Tk
    original_init = tk_class.__init__
    original_state = tk_class.state

    def hidden_init(instance: Any, *args: Any, **kwargs: Any) -> None:
        original_init(instance, *args, **kwargs)
        instance.withdraw()

    def guarded_state(instance: Any, new_state: str | None = None) -> Any:
        if new_state in {"normal", "zoomed"}:
            return original_state(instance)
        if new_state is None:
            return original_state(instance)
        return original_state(instance, new_state)

    tk_class.__init__ = hidden_init
    tk_class.state = guarded_state
    try:
        app = label_module.Label_Match(run_tests=run_tests)
    finally:
        tk_class.__init__ = original_init
        tk_class.state = original_state
    hwnd = _canonical_root_hwnd(int(app.winfo_id()))
    if win32gui.IsWindowVisible(hwnd):
        try:
            app.destroy()
        finally:
            raise RuntimeError("app became visible before DISPLAY2 placement")
    return app


def _place_app_before_first_show(
    app: Any,
    monitor_target: dict[str, Any],
) -> dict[str, Any]:
    if ui is None:
        raise RuntimeError("exact UI helper is not loaded")
    width, height, x, y = map(int, monitor_target["window_geometry"])
    expected_work = list(map(int, monitor_target["work_rect"]))
    app.update_idletasks()
    hwnd = _canonical_root_hwnd(ui._root_hwnd(app))
    visible_before_move = bool(win32gui.IsWindowVisible(hwnd))
    if visible_before_move:
        raise RuntimeError("app root is visible before DISPLAY2 pre-placement")
    app.geometry(ui._tk_geometry(width, height, x, y))
    app.update_idletasks()
    win32gui.MoveWindow(hwnd, x, y, width, height, True)
    rect_while_hidden = list(map(int, win32gui.GetWindowRect(hwnd)))
    visible_after_hidden_move = bool(win32gui.IsWindowVisible(hwnd))
    hidden_placement_ok = (
        not visible_before_move
        and not visible_after_hidden_move
        and _rect_matches(expected_work, rect_while_hidden, tolerance=1)
    )
    if not hidden_placement_ok:
        raise RuntimeError(
            "hidden app root was not placed exactly on DISPLAY2 work area: "
            f"rect={rect_while_hidden} expected={expected_work}"
        )
    app.deiconify()
    win32gui.MoveWindow(hwnd, x, y, width, height, True)
    app.update_idletasks()
    app.update()
    rect_after_show = list(map(int, win32gui.GetWindowRect(hwnd)))
    monitor = _monitor_for_hwnd(hwnd)
    visible_after_show = bool(win32gui.IsWindowVisible(hwnd))
    status = (
        "PASS"
        if visible_after_show
        and _rect_matches(expected_work, rect_after_show, tolerance=1)
        and monitor["device"].casefold() == str(monitor_target["device"]).casefold()
        and monitor["is_primary"] is False
        else "FAIL"
    )
    if status != "PASS":
        raise RuntimeError(
            "first visible app placement contract failed: "
            f"visible={visible_after_show} rect={rect_after_show} monitor={monitor}"
        )
    return {
        "status": status,
        "visible_before_move": visible_before_move,
        "visible_after_hidden_move": visible_after_hidden_move,
        "rect_while_hidden": rect_while_hidden,
        "visible_after_show": visible_after_show,
        "rect_after_show": rect_after_show,
        "monitor_device_after_show": monitor["device"],
        "monitor_is_primary_after_show": monitor["is_primary"],
    }


def _tree_rows(tree: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item_id in tree.get_children(""):
        values = tree.item(item_id, "values") or ()
        tags = tree.item(item_id, "tags") or ()
        rows.append(
            {
                "iid": str(item_id),
                "values": [str(value) for value in values],
                "tags": [str(tag) for tag in tags],
            }
        )
    return rows


def _expected_tree_rows(app: Any, kind: str) -> list[dict[str, Any]]:
    def display_value(tree: Any, value: Any) -> str:
        fitter = getattr(app, "_fit_operator_tree_cell_text", None)
        if callable(fitter):
            return str(fitter(tree, "Value", value))
        return str(value)

    if kind == "qa_scan_tree":
        view = app.__dict__.get("_last_workflow_view")
        if view is None:
            raise RuntimeError("presenter view is missing while validating qa_scan_tree")
        return [
            {
                "iid": f"qa-slot-{slot.index}",
                "values": [
                    f"{slot.index}. {slot.label}",
                    display_value(app.qa_scan_tree, slot.value or "-"),
                    str(app._workflow_state_text(slot.state)),
                ],
                "tags": [str(slot.state)],
            }
            for slot in view.slots
        ]
    source = app._workflow_view_source()
    return [
        {
            "iid": f"exact-slot-{index}",
            "values": [
                str(index),
                display_value(app.exact_rescan_tree, value),
            ],
            "tags": [],
        }
        for index, value in enumerate(source.get("exact_rescan_barcodes") or (), 1)
    ]


def _central_scan_list_evidence(app: Any) -> tuple[Any, dict[str, Any]]:
    qa_tree = app.qa_scan_tree
    exact_tree = app.exact_rescan_tree
    selected = str(app.live_scan_notebook.select() or "")
    exact_frame_id = str(app.exact_rescan_frame)
    tree = exact_tree if selected == exact_frame_id else qa_tree
    kind = "exact_rescan_tree" if tree is exact_tree else "qa_scan_tree"
    app.update_idletasks()
    mapped = bool(tree.winfo_ismapped())
    viewable = bool(tree.winfo_viewable())
    observed_rows = _tree_rows(tree)
    expected_rows = _expected_tree_rows(app, kind)
    absolute_rect = _tk_rect([tree])
    center_rect = _tk_rect([app.operator_center_pane])
    entry_rect = _tk_rect([app.entry])
    positive_geometry = absolute_rect[2] > absolute_rect[0] and absolute_rect[3] > absolute_rect[1]
    below_entry = absolute_rect[1] >= entry_rect[3] - 1
    final_row_visible = True
    if observed_rows:
        final_bbox = tuple(map(int, tree.bbox(observed_rows[-1]["iid"])))
        final_row_visible = bool(
            len(final_bbox) == 4
            and final_bbox[2] > 0
            and final_bbox[3] > 0
            and final_bbox[1] >= 0
            and final_bbox[1] + final_bbox[3] <= int(tree.winfo_height())
        )
    raw_values: list[str]
    if kind == "qa_scan_tree":
        raw_values = [
            str(slot.value or "")
            for slot in app.__dict__["_last_workflow_view"].slots
        ]
    else:
        raw_values = [
            str(value)
            for value in app._workflow_view_source().get("exact_rescan_barcodes") or ()
        ]
    evidence = {
        "required": True,
        "widget": kind,
        "location": "central_lower",
        "mapped": mapped,
        "viewable": viewable,
        "within_center_pane": _rect_contains(center_rect, absolute_rect),
        "below_scan_entry": below_entry,
        "positive_geometry": positive_geometry,
        "final_row_visible": final_row_visible,
        "absolute_rect": absolute_rect,
        "row_count": len(observed_rows),
        "observed_rows": observed_rows,
        "expected_rows": expected_rows,
        "accepted_raw_values": raw_values,
        "display_values_are_width_fitted": True,
        "rows_exact_match": observed_rows == expected_rows,
        "observed_rows_sha256": hashlib.sha256(
            json.dumps(observed_rows, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
        "expected_rows_sha256": hashlib.sha256(
            json.dumps(expected_rows, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        ).hexdigest(),
    }
    if not all(
        (
            mapped,
            viewable,
            evidence["within_center_pane"],
            below_entry,
            positive_geometry,
            final_row_visible,
            observed_rows == expected_rows,
        )
    ):
        raise RuntimeError(f"active central scan-list geometry/data contract failed: {evidence}")
    if kind == "qa_scan_tree" and len(observed_rows) != 5:
        raise RuntimeError(f"qa_scan_tree must expose exactly five rows, got {len(observed_rows)}")
    return tree, evidence


class EvidenceCapture:
    def __init__(self, monitor_target: dict[str, Any]) -> None:
        self.entries: list[dict[str, Any]] = []
        self.lock = threading.Lock()
        self.monitor_target = monitor_target
        self.app_root_hwnd: int | None = None

    def bind_app(self, app: Any) -> None:
        if ui is None:
            raise RuntimeError("exact UI helper is not loaded")
        _trace("bind_app_start")
        app.update_idletasks()
        self.app_root_hwnd = _canonical_root_hwnd(ui._root_hwnd(app))
        _trace("bind_app_done", app_root_hwnd=self.app_root_hwnd)

    def capture_hwnd(
        self,
        hwnd: int,
        name: str,
        note: str,
        targets: list[dict[str, Any]],
        *,
        app: Any | None = None,
        central_scan_list: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        raw_path = RAW_DIR / f"{name}.png"
        annotated_path = ANNOTATED_DIR / f"{name}.png"
        if ui is None:
            raise RuntimeError("exact UI helper is not loaded")
        root_hwnd = _canonical_root_hwnd(int(hwnd))
        _trace("capture_focus_start", name=name, target_root_hwnd=root_hwnd)
        foreground_before = _focus_exact_window(root_hwnd)
        _trace("capture_focus_done", name=name, target_root_hwnd=root_hwnd)
        info = ui._capture_bbox(root_hwnd, raw_path)
        _trace("capture_raw_done", name=name)
        foreground_after = ui._foreground_snapshot(root_hwnd)
        monitor = _monitor_for_hwnd(root_hwnd)
        target_window_rect = list(map(int, info["target_window_rect"]))
        expected_monitor_rect = list(map(int, self.monitor_target["monitor_rect"]))
        expected_work_rect = list(map(int, self.monitor_target["work_rect"]))
        foreground_ok = (
            foreground_before.get("foreground_root_hwnd") == root_hwnd
            and (info.get("foreground") or {}).get("foreground_root_hwnd") == root_hwnd
            and foreground_after.get("foreground_root_hwnd") == root_hwnd
        )
        containment_ok = _rect_contains(expected_monitor_rect, target_window_rect)
        monitor_ok = (
            monitor["device"].casefold() == str(self.monitor_target["device"]).casefold()
            and monitor["is_primary"] is False
            and monitor["monitor_rect"] == expected_monitor_rect
            and list(map(int, info["monitor_rect"])) == expected_monitor_rect
        )
        if self.app_root_hwnd is None:
            raise RuntimeError("main app root was not bound before evidence capture")
        app_root_hwnd = _canonical_root_hwnd(self.app_root_hwnd)
        if not win32gui.IsWindow(app_root_hwnd):
            raise RuntimeError(f"bound app root HWND is not live: {app_root_hwnd}")
        app_root_rect = list(map(int, win32gui.GetWindowRect(app_root_hwnd)))
        app_root_monitor = _monitor_for_hwnd(app_root_hwnd)
        app_root_contract_ok = (
            app_root_monitor["device"].casefold() == str(self.monitor_target["device"]).casefold()
            and app_root_monitor["is_primary"] is False
            and app_root_monitor["monitor_rect"] == expected_monitor_rect
            and _rect_contains(expected_work_rect, app_root_rect)
            and _rect_matches(expected_work_rect, app_root_rect, tolerance=1)
        )
        if not foreground_ok:
            raise RuntimeError(f"foreground contract failed for {name}: {info.get('foreground')}")
        if not containment_ok or not monitor_ok:
            raise RuntimeError(
                f"DISPLAY2 containment contract failed for {name}: "
                f"monitor={monitor} window={target_window_rect} expected={self.monitor_target}"
            )
        if not app_root_contract_ok:
            raise RuntimeError(
                f"main app root escaped the DISPLAY2 work area for {name}: "
                f"root={app_root_rect} monitor={app_root_monitor} expected_work={expected_work_rect}"
            )
        _draw_annotations(raw_path, annotated_path, info["monitor_rect"], targets)
        _trace("capture_annotation_done", name=name)
        pixel_qa = _pixel_comparison(raw_path, annotated_path)
        _trace("capture_pixel_qa_done", name=name)
        entry = {
            "name": name,
            "note": note,
            "raw_path": _relative(raw_path),
            "annotated_path": _relative(annotated_path),
            "raw_sha256": ui._sha256_file(raw_path),
            "annotated_sha256": ui._sha256_file(annotated_path),
            "width": info["width"],
            "height": info["height"],
            "monitor_rect": info["monitor_rect"],
            "monitor_device": monitor["device"],
            "monitor_is_primary": monitor["is_primary"],
            "monitor_contract_ok": monitor_ok,
            "target_window_rect": info["target_window_rect"],
            "target_root_hwnd": root_hwnd,
            "foreground_root_hwnd_before": int(foreground_before.get("foreground_root_hwnd") or 0),
            "foreground_root_hwnd_during": int((info.get("foreground") or {}).get("foreground_root_hwnd") or 0),
            "foreground_root_hwnd_after": int(foreground_after.get("foreground_root_hwnd") or 0),
            "target_is_foreground": foreground_ok,
            "target_contained_in_monitor": containment_ok,
            "app_root_hwnd": app_root_hwnd,
            "app_root_rect": app_root_rect,
            "app_root_monitor_device": app_root_monitor["device"],
            "app_root_monitor_is_primary": app_root_monitor["is_primary"],
            "app_root_matches_work_area": app_root_contract_ok,
            "blank_suspected": info["blank_suspected"],
            "pixel_qa": pixel_qa,
            "annotations": targets,
            "central_scan_list": central_scan_list,
        }
        if app is not None:
            entry.update(
                {
                    "title": app.title(),
                    "big_display": app.big_display_label.cget("text"),
                    "status": app.status_label.cget("text"),
                    "scan_count": len(app.current_set_info.get("raw") or []),
                    "exact_rescan_count": len(app.current_set_info.get("exact_rescan_barcodes") or []),
                }
            )
        with self.lock:
            self.entries.append(entry)
        _trace("capture_entry_done", name=name)
        return entry

    def capture_app(self, app: Any, name: str, note: str, targets: list[dict[str, Any]]) -> dict[str, Any]:
        _trace("capture_app_start", name=name)
        _settle_ui(app, 0.06)
        if ui is None:
            raise RuntimeError("exact UI helper is not loaded")
        self.bind_app(app)
        active_tree, central_scan_list = _central_scan_list_evidence(app)
        targets = list(targets) + [tk_target("중앙 하단 실제 스캔 목록", active_tree)]
        return self.capture_hwnd(
            ui._root_hwnd(app),
            name,
            note,
            targets,
            app=app,
            central_scan_list=central_scan_list,
        )

    def capture_toplevel(self, app: Any, toplevel: Any, name: str, note: str, targets: list[dict[str, Any]]) -> dict[str, Any]:
        _trace("capture_toplevel_start", name=name)
        if ui is None:
            raise RuntimeError("exact UI helper is not loaded")
        self.bind_app(app)
        app.update_idletasks()
        toplevel.update_idletasks()
        return self.capture_hwnd(ui._root_hwnd(toplevel), name, note, targets, app=app)


def _find_toplevel(app: Any, title_contains: str) -> Any | None:
    for child in app.winfo_children():
        try:
            if title_contains in child.title():
                return child
        except Exception:
            continue
    return None


def _entry_widget(dialog: Any) -> Any | None:
    for child in ui._iter_tk_descendants(dialog):
        try:
            if child.winfo_class() in {"Entry", "TEntry", "TSpinbox", "Spinbox"}:
                return child
        except Exception:
            continue
    return None


def _schedule_tk_dialog_submit(
    app: Any,
    evidence: EvidenceCapture,
    *,
    title_contains: str,
    text: str,
    name: str | None = None,
    note: str = "",
    timeout_ms: int = 8000,
    error_sink: list[str] | None = None,
) -> None:
    started = time.monotonic()

    def attempt() -> None:
        dialog = None
        try:
            dialog = _find_toplevel(app, title_contains)
            if dialog is None:
                if (time.monotonic() - started) * 1000 < timeout_ms:
                    app.after(100, attempt)
                    return
                raise TimeoutError(f"Tk dialog not found: {title_contains}")
            entry = _entry_widget(dialog)
            if name:
                targets = [tk_target(title_contains, dialog)]
                if entry is not None:
                    targets.append(tk_target("입력값", entry))
                evidence.capture_toplevel(app, dialog, name, note, targets)
            if entry is not None:
                entry.delete(0, "end")
                entry.insert(0, text)
            if not ui._invoke_first_tk_button(dialog, prefixes=("확인", "OK")):
                dialog.event_generate("<Return>")
        except Exception as exc:
            if error_sink is not None:
                error_sink.append(repr(exc))
            if dialog is not None:
                try:
                    dialog.destroy()
                except Exception:
                    pass

    app.after(100, attempt)


def _start_native_dialog_worker(
    evidence: EvidenceCapture,
    *,
    title_contains: str,
    name: str | None = None,
    note: str = "",
    text: str = "",
    prefixes: tuple[str, ...] = ("예", "확인", "OK"),
) -> threading.Thread:
    def worker() -> None:
        hwnd = ui._helper_wait_for_window(os.getpid(), title_contains, timeout=15)
        if name:
            evidence.capture_hwnd(
                hwnd,
                name,
                note,
                [hwnd_target(title_contains, hwnd)],
            )
        if text:
            ui._set_dialog_edit_text(hwnd, text)
        clicked = ui._click_dialog_button(hwnd, prefixes=prefixes)
        command_id = win32con.IDYES if any(prefix.startswith("예") for prefix in prefixes) else win32con.IDOK
        time.sleep(0.2)
        if win32gui.IsWindow(hwnd):
            try:
                win32gui.PostMessage(hwnd, win32con.WM_COMMAND, command_id, 0)
            except Exception:
                pass
        time.sleep(0.3)
        if win32gui.IsWindow(hwnd):
            try:
                win32gui.PostMessage(hwnd, win32con.WM_CLOSE, 0, 0)
            except Exception:
                pass

    thread = threading.Thread(target=worker, name=f"manual-{title_contains}", daemon=True)
    thread.start()
    return thread


def _find_error_modal(app: Any) -> Any:
    for child in app.winfo_children():
        try:
            if child.winfo_class() == "Toplevel" and "⚠" in child.title():
                return child
        except Exception:
            continue
    raise RuntimeError("error modal not found")


def _settle_ui(app: Any, seconds: float = 0.12) -> None:
    """Flush layout, paint, and finite post-render work after recurring jobs are quiesced."""

    responsive_settle = getattr(app, "_settle_operator_responsive_layout", None)
    if callable(responsive_settle):
        responsive_settle()
    app.update_idletasks()
    try:
        ctypes.windll.user32.UpdateWindow(int(app.winfo_id()))
        ctypes.windll.dwmapi.DwmFlush()
    except Exception:
        pass
    deadline = time.monotonic() + max(0.0, seconds)
    while True:
        app.update()
        if time.monotonic() >= deadline:
            break
        time.sleep(0.02)


def _quiesce_scheduled_jobs(app: Any, *, instance: str) -> dict[str, Any]:
    pending = tuple(app.tk.splitlist(app.tk.call("after", "info")))
    cancelled = 0
    for after_id in pending:
        try:
            app.after_cancel(after_id)
            cancelled += 1
        except Exception:
            pass
    app.update_idletasks()
    remaining = tuple(app.tk.splitlist(app.tk.call("after", "info")))
    if remaining:
        raise RuntimeError(
            f"scheduled jobs remain after capture quiescence ({instance}): {remaining}"
        )
    return {
        "instance": instance,
        "status": "PASS",
        "pending_before": len(pending),
        "cancelled": cancelled,
        "remaining_after": 0,
    }


def _scan(app: Any, value: str, seconds: float = 0.35) -> None:
    """Submit one scan through the real entry/process_input path deterministically.

    OS-level clipboard keystrokes are intentionally avoided here because a capture
    runner may temporarily lose foreground focus while taking the previous image.
    The same Tk entry and ``process_input`` handler used by the Return binding are
    exercised directly.
    """
    app.entry.configure(state="normal")
    app.entry.delete(0, "end")
    app.entry.insert(0, value)
    app.process_input()
    _settle_ui(app, seconds)
    try:
        app.data_manager.flush(timeout=5)
    except Exception:
        pass


def _workflow_state_snapshot(app: Any) -> dict[str, Any]:
    """Return the semantic state that must match the screen being captured."""

    app._render_operator_workbench()
    current = dict(getattr(app, "current_set_info", {}) or {})
    view = getattr(app, "_last_workflow_view", None)
    exact_view = getattr(view, "exact_rescan", None)
    pending_error = (
        getattr(app, "_pending_workflow_error", None)
        or getattr(app, "_workflow_pending_error", None)
    )
    return {
        "raw_count": len(current.get("raw") or ()),
        "parsed_count": len(current.get("parsed") or ()),
        "exact_active": bool(current.get("exact_rescan_active")),
        "exact_complete": bool(current.get("exact_rescan_complete")),
        "exact_target": int(current.get("exact_rescan_target_count") or 0),
        "exact_count": len(current.get("exact_rescan_barcodes") or ()),
        "completion_kind": getattr(app, "_workflow_completion_kind", None),
        "display_scan_count": len(getattr(app, "_workflow_display_scans", ()) or ()),
        "pending_error": bool(pending_error),
        "view_qa_completed": int(getattr(view, "qa_completed", -1)),
        "view_current_stage": str(getattr(view, "current_stage", "")),
        "view_exact_status": str(getattr(exact_view, "status", "")),
        "view_exact_completed": int(getattr(exact_view, "completed", -1)),
        "view_exact_target": int(getattr(exact_view, "target", -1)),
    }


def _require_workflow_state(
    records: list[dict[str, Any]],
    app: Any,
    checkpoint: str,
    **expected: Any,
) -> dict[str, Any]:
    """Fail before capture when the fixture did not reach its declared state."""

    observed = _workflow_state_snapshot(app)
    mismatches = {
        key: {"expected": value, "observed": observed.get(key)}
        for key, value in expected.items()
        if observed.get(key) != value
    }
    record = {
        "checkpoint": checkpoint,
        "status": "PASS" if not mismatches else "FAIL",
        "expected": dict(expected),
        "observed": observed,
        "mismatches": mismatches,
    }
    records.append(record)
    if mismatches:
        raise RuntimeError(
            f"capture fixture state mismatch at {checkpoint}: "
            f"{json.dumps(mismatches, ensure_ascii=False, sort_keys=True)}"
        )
    return record


def _capture_error_modal(evidence: EvidenceCapture, app: Any, name: str, note: str) -> None:
    modal = _find_error_modal(app)
    buttons = []
    labels = []
    for child in ui._iter_tk_descendants(modal):
        try:
            cls = child.winfo_class()
            if cls == "Button":
                buttons.append(child)
            elif cls == "Label":
                labels.append(child)
        except Exception:
            continue
    targets = [tk_target("오류 확인창", modal)]
    if labels:
        targets.append(tk_target("오류 내용", *labels))
    if buttons:
        targets.append(tk_target("확인 버튼", *buttons))
    evidence.capture_toplevel(app, modal, name, note, targets)
    if not ui._invoke_first_tk_button(modal):
        modal.destroy()
    _settle_ui(app, 0.2)


def _close_app_without_sync(app: Any) -> None:
    if app is None:
        return
    try:
        app.data_manager.flush(timeout=5)
    except Exception:
        pass
    try:
        app.data_manager.close(timeout=5)
    except Exception:
        pass
    try:
        for after_id in app.tk.splitlist(app.tk.call("after", "info")):
            app.after_cancel(after_id)
    except Exception:
        pass
    try:
        app.destroy()
    except Exception:
        pass


def _configure_isolation(temp_root: Path) -> dict[str, Any]:
    save_dir = temp_root / "label_match_data"
    direct_sync_root = temp_root / "direct_sync"
    save_dir.mkdir(parents=True, exist_ok=True)
    direct_sync_root.mkdir(parents=True, exist_ok=True)
    os.environ.update(
        {
            "LABEL_MATCH_SAVE_DIR": str(save_dir),
            "LABEL_MATCH_DIRECT_SYNC_BOOTSTRAP": "off",
            "LABEL_MATCH_SESSION_SYNC_TRIGGER": "off",
            "LABEL_MATCH_UPDATE_PROVIDER": "off",
            "LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT": str(direct_sync_root),
            "LABEL_MATCH_AUDIO_ENABLED": "off",
            "LABEL_MATCH_AUTOMATED_TEST": "1",
            "PYGAME_HIDE_SUPPORT_PROMPT": "1",
        }
    )
    for key in LOGISTICS_ENV_KEYS:
        os.environ.pop(key, None)
    return {
        "save_dir": "<isolated-temp>/label_match_data",
        "direct_sync_root": "<isolated-temp>/direct_sync",
        "direct_sync_bootstrap": os.environ["LABEL_MATCH_DIRECT_SYNC_BOOTSTRAP"],
        "session_sync_trigger": os.environ["LABEL_MATCH_SESSION_SYNC_TRIGGER"],
        "update_provider": os.environ["LABEL_MATCH_UPDATE_PROVIDER"],
        "logistics_keys_unset": all(key not in os.environ for key in LOGISTICS_ENV_KEYS),
    }


def _redirect_config_resources(label_module: Any, temp_root: Path) -> dict[str, Any]:
    original_resource_path = label_module.resource_path
    config_root = temp_root / "config"
    config_root.mkdir(parents=True, exist_ok=False)
    (config_root / "app_settings.json").write_text("{}\n", encoding="utf-8")

    def isolated_resource_path(relative_path: str) -> str:
        normalized = Path(str(relative_path).replace("\\", "/"))
        if normalized.parts and normalized.parts[0].casefold() == "config":
            remainder = normalized.parts[1:]
            candidate = config_root.joinpath(*remainder)
            if not _path_is_within(candidate, config_root):
                raise RuntimeError(f"config resource escaped isolation root: {relative_path!r}")
            return str(candidate)
        return str(original_resource_path(relative_path))

    label_module.resource_path = isolated_resource_path
    return {
        "config_redirected": True,
        "config_root": "<isolated-temp>/config",
        "source_config_write_allowed": False,
    }


def _assert_new_asset_root(asset_root: Path) -> None:
    resolved = asset_root.resolve()
    docs_assets = (TOOL_ROOT / "docs" / "assets").resolve()
    if docs_assets not in resolved.parents:
        raise RuntimeError(f"manual asset root must be a new child of {docs_assets}")
    if resolved.exists():
        raise RuntimeError(
            "manual asset root already exists; choose a new output folder instead of deleting evidence: "
            f"{resolved}"
        )
    resolved.mkdir(parents=True, exist_ok=False)
    RAW_DIR.mkdir(parents=False, exist_ok=False)
    ANNOTATED_DIR.mkdir(parents=False, exist_ok=False)


def _privacy_contract(manifest: dict[str, Any]) -> dict[str, Any]:
    hits: list[str] = []
    temp_path = str(Path(tempfile.gettempdir()).resolve()).casefold()

    def visit(value: Any, path: str) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                key_text = str(key)
                if key_text.casefold() in {"host", "hostname", "computer_name", "source_root", "temp_root"}:
                    hits.append(f"forbidden_key:{path}.{key_text}")
                visit(child, f"{path}.{key_text}")
            return
        if isinstance(value, (list, tuple)):
            for index, child in enumerate(value):
                visit(child, f"{path}[{index}]")
            return
        if not isinstance(value, str):
            return
        normalized = value.replace("/", "\\")
        lowered = normalized.casefold()
        is_display_device = lowered == TARGET_DISPLAY_DEVICE.casefold()
        if not is_display_device and (
            re.match(r"^[a-zA-Z]:\\", normalized)
            or normalized.startswith("\\\\")
            or temp_path in lowered
            or "\\users\\" in lowered
            or "\\company\\program" in lowered
            or "desktop-" in lowered
        ):
            hits.append(f"forbidden_value:{path}")

    import re

    visit(manifest, "manifest")
    return {
        "status": "PASS" if not hits else "FAIL",
        "forbidden_hits": sorted(set(hits)),
        "paths_redacted": True,
        "hostname_recorded": False,
    }


def _make_contact_sheet(entries: list[dict[str, Any]]) -> None:
    font = _font(24)
    columns = 3
    card_width, card_height = 780, 488
    thumb_width, thumb_height = 748, 421
    rows = (len(entries) + columns - 1) // columns
    sheet = Image.new("RGB", (columns * card_width, rows * card_height), "#f2f4f7")
    draw = ImageDraw.Draw(sheet)
    for index, entry in enumerate(entries):
        row, column = divmod(index, columns)
        x, y = column * card_width, row * card_height
        image = Image.open(ASSET_ROOT / entry["annotated_path"]).convert("RGB")
        thumb = ImageOps.fit(image, (thumb_width, thumb_height), method=Image.Resampling.LANCZOS)
        sheet.paste(thumb, (x + 16, y + 12))
        draw.text((x + 18, y + 444), f"{index + 1:02d}. {entry['name']}", fill="#111827", font=font)
    sheet.save(CONTACT_SHEET_PATH, optimize=True)


def run(
    *,
    source_root: Path = DEFAULT_SOURCE_ROOT,
    asset_root: Path = DEFAULT_ASSET_ROOT,
    expected_commit: str = EXPECTED_SOURCE_COMMIT,
    expected_tree: str = EXPECTED_SOURCE_TREE,
    display_device: str = TARGET_DISPLAY_DEVICE,
) -> dict[str, Any]:
    global SOURCE_ROOT, ASSET_ROOT, RAW_DIR, ANNOTATED_DIR, MANIFEST_PATH
    global CONTACT_SHEET_PATH, CAPTURE_GEOMETRY, ui

    SOURCE_ROOT = source_root.resolve()
    ASSET_ROOT = asset_root.resolve()
    RAW_DIR = ASSET_ROOT / "raw"
    ANNOTATED_DIR = ASSET_ROOT / "annotated"
    MANIFEST_PATH = ASSET_ROOT / "manifest.json"
    CONTACT_SHEET_PATH = ASSET_ROOT / "contact_sheet.png"
    source_identity = _source_identity(
        SOURCE_ROOT,
        expected_commit=expected_commit,
        expected_tree=expected_tree,
    )
    dpi_awareness = _set_per_monitor_dpi_awareness()
    monitor_target = _resolve_capture_monitor(display_device)
    CAPTURE_GEOMETRY = tuple(map(int, monitor_target["window_geometry"]))
    _assert_new_asset_root(ASSET_ROOT)
    sys.dont_write_bytecode = True
    ui, label_module = _load_exact_modules(SOURCE_ROOT)
    module_origins = _assert_exact_module_origins(SOURCE_ROOT, ui, label_module)
    if str(label_module.APP_VERSION) != EXPECTED_APP_VERSION:
        raise RuntimeError(
            f"app version mismatch: expected {EXPECTED_APP_VERSION}, got {label_module.APP_VERSION}"
        )

    evidence = EvidenceCapture(monitor_target)
    previsible_placements: list[dict[str, Any]] = []
    after_quiescence: list[dict[str, Any]] = []
    workflow_state_contracts: list[dict[str, Any]] = []
    app = None

    with tempfile.TemporaryDirectory(prefix="label-match-manual-v2-0-36-") as temp_name:
        temp_root = Path(temp_name)
        isolation = _configure_isolation(temp_root)
        isolation.update(_redirect_config_resources(label_module, temp_root))

        label_module.threaded_update_check = lambda: None
        label_module._label_match_start_session_direct_sync = lambda *args, **kwargs: threading.Thread()

        marker = time.strftime("CAP%Y%m%d%H%M%S")
        today = time.strftime("%Y%m%d")
        phs_master = (
            f"PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITAG-{marker}|CLC={ui.REAL_MASTER}|"
            f"LBL=LBL-{marker}|HSH={'a' * 32}"
        )

        try:
            app = _create_app_hidden(label_module, run_tests=False)
            first_placement = _place_app_before_first_show(app, monitor_target)
            first_placement["instance"] = "initial"
            previsible_placements.append(first_placement)
            app.attributes("-topmost", True)
            ui._wait_until(app, lambda: app.initialized_successfully, 25, "app initialized")
            ui._wait_history_idle(app)
            after_quiescence.append(
                _quiesce_scheduled_jobs(app, instance="initial")
            )
            _require_workflow_state(
                workflow_state_contracts,
                app,
                "01_startup_1_of_5",
                raw_count=0,
                parsed_count=0,
                exact_active=False,
                exact_complete=False,
                exact_target=0,
                exact_count=0,
                completion_kind=None,
                display_scan_count=0,
                pending_error=False,
                view_qa_completed=0,
            )

            evidence.capture_app(
                app,
                "01_startup_1_of_5",
                "Current v2.0.36 idle screen at 1/5",
                [
                    tk_target("현재 단계 1/5", app.big_display_label),
                    tk_target("바코드 입력", app.entry),
                    tk_target("5단계 진행표", *app.step_labels),
                ],
            )

            app.open_settings_window()
            _settle_ui(app, 0.2)
            settings = _find_toplevel(app, "설정")
            if settings is None:
                raise RuntimeError("settings window not found")
            worker_combo = next(
                child for child in ui._iter_tk_descendants(settings) if child.winfo_class() == "TCombobox"
            )
            evidence.capture_toplevel(
                app,
                settings,
                "02_settings_worker",
                "Worker-name settings; no value is saved during capture",
                [tk_target("작업자 이름", worker_combo), tk_target("설정 창", settings)],
            )
            settings.destroy()
            app.entry.focus_set()
            _settle_ui(app, 0.12)

            _scan(app, phs_master)
            _require_workflow_state(
                workflow_state_contracts,
                app,
                "03_phs_master_f4_ready",
                raw_count=1,
                exact_active=False,
                exact_complete=False,
                exact_target=0,
                exact_count=0,
                completion_kind=None,
                pending_error=False,
                view_qa_completed=1,
            )
            evidence.capture_app(
                app,
                "03_phs_master_f4_ready",
                "Reusable PHS=2 input tag accepted; F4 full membership rescan is enabled",
                [
                    tk_target("PHS 현품표 완료", app.step_labels[0]),
                    tk_target("F4 전체 재스캔", app.exact_rescan_button),
                    tk_target("다음 단계", app.big_display_label),
                ],
            )

            original_askstring = label_module.simpledialog.askstring
            label_module.simpledialog.askstring = (
                lambda *args, **kwargs: f"TRANSFER-{marker}"
            )
            quantity_dialog_errors: list[str] = []
            _schedule_tk_dialog_submit(
                app,
                evidence,
                title_contains="전체 재스캔 수량",
                text="3",
                name="04_f4_target_quantity",
                note="Operator enters current full membership count N=3",
                error_sink=quantity_dialog_errors,
            )
            try:
                app._prompt_exact_rescan()
            finally:
                label_module.simpledialog.askstring = original_askstring
            if quantity_dialog_errors:
                raise RuntimeError(
                    f"F4 quantity dialog automation failed: {quantity_dialog_errors}"
                )
            if int(app.current_set_info.get("exact_rescan_target_count") or 0) != 3:
                raise RuntimeError("F4 capture target must be exactly N=3")
            _settle_ui(app, 0.2)
            _require_workflow_state(
                workflow_state_contracts,
                app,
                "04_f4_target_quantity",
                raw_count=1,
                exact_active=True,
                exact_complete=False,
                exact_target=3,
                exact_count=0,
                completion_kind=None,
                pending_error=False,
                view_qa_completed=1,
                view_exact_status="active",
                view_exact_completed=0,
                view_exact_target=3,
            )

            full_members = [f"{ui.REAL_MASTER}-{marker}-MEMBER-{index}" for index in range(1, 4)]
            _scan(app, full_members[0])
            _require_workflow_state(
                workflow_state_contracts,
                app,
                "05_full_rescan_in_progress",
                raw_count=1,
                exact_active=True,
                exact_complete=False,
                exact_target=3,
                exact_count=1,
                completion_kind=None,
                pending_error=False,
                view_qa_completed=1,
                view_exact_status="active",
                view_exact_completed=1,
                view_exact_target=3,
            )
            evidence.capture_app(
                app,
                "05_full_rescan_in_progress",
                "One of three current members scanned in the separate F4 flow",
                [tk_target("전체 재스캔 1/3", app.big_display_label), tk_target("재스캔 상태", app.status_label)],
            )
            _scan(app, full_members[1])
            _require_workflow_state(
                workflow_state_contracts,
                app,
                "05b_full_rescan_member_2",
                raw_count=1,
                exact_active=True,
                exact_complete=False,
                exact_target=3,
                exact_count=2,
                completion_kind=None,
                pending_error=False,
                view_qa_completed=1,
                view_exact_status="active",
                view_exact_completed=2,
                view_exact_target=3,
            )
            _scan(app, full_members[2])
            _require_workflow_state(
                workflow_state_contracts,
                app,
                "06_full_rescan_complete",
                raw_count=1,
                exact_active=False,
                exact_complete=True,
                exact_target=3,
                exact_count=3,
                completion_kind=None,
                pending_error=False,
                view_qa_completed=1,
                view_exact_status="complete",
                view_exact_completed=3,
                view_exact_target=3,
            )
            evidence.capture_app(
                app,
                "06_full_rescan_complete",
                "All N=3 exact members scanned; normal QA sample flow resumes",
                [tk_target("전체 재스캔 완료", app.big_display_label), tk_target("완료 상태", app.status_label)],
            )

            qa_samples = [f"{ui.REAL_MASTER}-{marker}-QA-{index}" for index in range(1, 4)]
            for index, value in enumerate(qa_samples, 1):
                _scan(app, value)
                _require_workflow_state(
                    workflow_state_contracts,
                    app,
                    f"{index + 6:02d}_qa_sample_{index}",
                    raw_count=index + 1,
                    exact_active=False,
                    exact_complete=True,
                    exact_target=3,
                    exact_count=3,
                    completion_kind=None,
                    pending_error=False,
                    view_qa_completed=index + 1,
                    view_exact_status="complete",
                    view_exact_completed=3,
                    view_exact_target=3,
                )
                label = "QA 3 완료 · 라벨지 대기" if index == 3 else f"QA 샘플 {index}/3"
                targets = [tk_target(label, app.step_labels[index]), tk_target("다음 단계", app.big_display_label)]
                evidence.capture_app(
                    app,
                    f"{index + 6:02d}_qa_sample_{index}",
                    f"QA sample {index} of 3; QA samples are not exact membership",
                    targets,
                )

            final_label = f"FINAL_LABEL_{ui.REAL_MASTER}_{marker}<GS>6D{today}"
            _scan(app, final_label, seconds=0.25)
            _require_workflow_state(
                workflow_state_contracts,
                app,
                "10_complete_5_of_5",
                raw_count=0,
                exact_active=False,
                exact_complete=False,
                exact_target=0,
                exact_count=0,
                completion_kind="full",
                display_scan_count=5,
                pending_error=False,
                view_qa_completed=5,
            )
            evidence.capture_app(
                app,
                "10_complete_5_of_5",
                "Final label accepted; the five UI stages are complete",
                [
                    tk_target("5/5 통과 완료", app.status_label),
                    tk_target("다음 현품표 대기", app.big_display_label),
                    tk_target("완료 진행표", *app.step_labels),
                ],
            )

            mismatch_master = "AAA2287560100"
            _scan(app, mismatch_master)
            _scan(app, f"WRONG-{marker}-PRODUCT-LONG-ENOUGH", seconds=0.25)
            _require_workflow_state(
                workflow_state_contracts,
                app,
                "11_mismatch_error",
                raw_count=1,
                completion_kind=None,
                pending_error=True,
                view_qa_completed=1,
                view_current_stage="error",
            )
            evidence.capture_app(
                app,
                "11_mismatch_error",
                "Product mismatch is shown once in the fixed center notice while accepted rows remain visible",
                [
                    tk_target("중앙 단일 오류", app.workflow_notice_frame),
                    tk_target("확인 후 복구", app.workflow_notice_action_button),
                    tk_target("마지막 정상 스캔 유지", app.operator_last_scan_label),
                ],
            )
            app._acknowledge_workflow_notice()
            _settle_ui(app, 0.16)
            _require_workflow_state(
                workflow_state_contracts,
                app,
                "11_mismatch_error_acknowledged",
                raw_count=0,
                pending_error=False,
                view_qa_completed=0,
            )

            duplicate_master = "AAA2287570100"
            duplicate_product = f"{duplicate_master}-{marker}-DUPLICATE"
            _scan(app, duplicate_master)
            _scan(app, duplicate_product)
            _scan(app, duplicate_product, seconds=0.25)
            _require_workflow_state(
                workflow_state_contracts,
                app,
                "12_duplicate_error",
                raw_count=2,
                completion_kind=None,
                pending_error=True,
                view_qa_completed=2,
                view_current_stage="error",
            )
            evidence.capture_app(
                app,
                "12_duplicate_error",
                "Duplicate scan is shown once in the fixed center notice while the last accepted row remains visible",
                [
                    tk_target("중앙 단일 오류", app.workflow_notice_frame),
                    tk_target("확인 후 복구", app.workflow_notice_action_button),
                    tk_target("마지막 정상 스캔 유지", app.operator_last_scan_label),
                ],
            )
            app._acknowledge_workflow_notice()
            _settle_ui(app, 0.16)
            _require_workflow_state(
                workflow_state_contracts,
                app,
                "12_duplicate_error_acknowledged",
                raw_count=0,
                pending_error=False,
                view_qa_completed=0,
            )

            cancel_master = "AAA2287580100"
            _scan(app, cancel_master)
            _scan(app, f"{cancel_master}-{marker}-CANCEL-1")
            _require_workflow_state(
                workflow_state_contracts,
                app,
                "13_current_set_cancel",
                raw_count=2,
                completion_kind=None,
                pending_error=False,
                view_qa_completed=2,
            )
            evidence.capture_app(
                app,
                "13_current_set_cancel",
                "F1 discards only the active unfinished set",
                [tk_target("현재 세트 취소 F1", app.reset_button), tk_target("진행 중 세트", *app.step_labels)],
            )
            app.reset_button.invoke()
            _settle_ui(app, 0.16)
            _require_workflow_state(
                workflow_state_contracts,
                app,
                "13_current_set_cancel_applied",
                raw_count=0,
                completion_kind=None,
                pending_error=False,
                view_qa_completed=0,
            )

            _require_workflow_state(
                workflow_state_contracts,
                app,
                "14_completed_tray_cancel_input",
                raw_count=0,
                completion_kind=None,
                pending_error=False,
                view_qa_completed=0,
            )
            _schedule_tk_dialog_submit(
                app,
                evidence,
                title_contains="완료된 트레이 취소",
                text="",
                name="14_completed_tray_cancel_input",
                note="F2 asks for the master label of a completed tray; capture exits without changing data",
            )
            app._prompt_and_cancel_completed_tray()
            _settle_ui(app, 0.16)

            restore_master = "AAA2287590100"
            _scan(app, restore_master)
            _scan(app, f"{restore_master}-{marker}-RESTORE-1")
            _require_workflow_state(
                workflow_state_contracts,
                app,
                "15_restore_before_close",
                raw_count=2,
                completion_kind=None,
                pending_error=False,
                view_qa_completed=2,
            )
            evidence.capture_app(
                app,
                "15_restore_before_close",
                "A partial same-day set is durably saved before the app closes",
                [tk_target("저장된 진행 상태", *app.step_labels), tk_target("다음 스캔", app.big_display_label)],
            )
            app.data_manager.flush(timeout=5)
            _close_app_without_sync(app)
            app = None

            app = _create_app_hidden(label_module, run_tests=True)
            restored_placement = _place_app_before_first_show(app, monitor_target)
            restored_placement["instance"] = "restored"
            previsible_placements.append(restored_placement)
            app.attributes("-topmost", True)
            ui._wait_until(app, lambda: app.initialized_successfully, 25, "restored app initialized")
            ui._wait_history_idle(app)
            after_quiescence.append(
                _quiesce_scheduled_jobs(app, instance="restored")
            )
            _require_workflow_state(
                workflow_state_contracts,
                app,
                "16_restore_resumed",
                raw_count=2,
                completion_kind=None,
                pending_error=False,
                view_qa_completed=2,
            )
            evidence.capture_app(
                app,
                "16_restore_resumed",
                "Recovered set resumes at QA sample 2/3",
                [tk_target("복구된 진행 상태", *app.step_labels), tk_target("다음 스캔", app.big_display_label)],
            )
            app.reset_button.invoke()
            _settle_ui(app, 0.12)

            sealed_qr = (
                f"TRF=1|BND=TRANSFER-SEALED-{marker}|AUTH_SCOPE=CAPTURE|CLC={ui.REAL_MASTER}|"
                f"QT=12|HSH={'b' * 64}|EPOCH=1|PLANE=AUTHORITATIVE|PE=1"
            )
            _scan(app, sealed_qr)
            _require_workflow_state(
                workflow_state_contracts,
                app,
                "17_sealed_transfer_qr",
                raw_count=1,
                exact_active=False,
                exact_complete=True,
                exact_target=12,
                exact_count=0,
                completion_kind=None,
                pending_error=False,
                view_qa_completed=1,
                view_exact_status="sealed",
                view_exact_target=12,
            )
            evidence.capture_app(
                app,
                "17_sealed_transfer_qr",
                "Sealed transfer QR inherits QT=12 exact membership while the UI remains a five-scan flow",
                [
                    tk_target("sealed 이적 QR", app.step_labels[0]),
                    tk_target("QA 샘플 1 대기", app.big_display_label),
                    tk_target("F4 비활성", app.exact_rescan_button),
                ],
            )

        finally:
            _close_app_without_sync(app)

        entries = sorted(evidence.entries, key=lambda item: item["name"])
        _make_contact_sheet(entries)
        expected_names = [
            "01_startup_1_of_5",
            "02_settings_worker",
            "03_phs_master_f4_ready",
            "04_f4_target_quantity",
            "05_full_rescan_in_progress",
            "06_full_rescan_complete",
            "07_qa_sample_1",
            "08_qa_sample_2",
            "09_qa_sample_3",
            "10_complete_5_of_5",
            "11_mismatch_error",
            "12_duplicate_error",
            "13_current_set_cancel",
            "14_completed_tray_cancel_input",
            "15_restore_before_close",
            "16_restore_resumed",
            "17_sealed_transfer_qr",
        ]
        actual_names = [entry["name"] for entry in entries]
        main_screen_names = {
            "01_startup_1_of_5",
            "03_phs_master_f4_ready",
            "05_full_rescan_in_progress",
            "06_full_rescan_complete",
            "07_qa_sample_1",
            "08_qa_sample_2",
            "09_qa_sample_3",
            "10_complete_5_of_5",
            "11_mismatch_error",
            "12_duplicate_error",
            "13_current_set_cancel",
            "15_restore_before_close",
            "16_restore_resumed",
            "17_sealed_transfer_qr",
        }
        capture_width, capture_height = map(int, monitor_target["capture_size"])
        expected_monitor_rect = list(map(int, monitor_target["monitor_rect"]))

        def central_entry_ok(entry: dict[str, Any]) -> bool:
            central = entry.get("central_scan_list")
            if entry["name"] not in main_screen_names:
                return central is None
            if not isinstance(central, dict):
                return False
            observed_rows = central.get("observed_rows") or []
            expected_rows = central.get("expected_rows") or []
            observed_sha256 = hashlib.sha256(
                json.dumps(observed_rows, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            ).hexdigest()
            expected_sha256 = hashlib.sha256(
                json.dumps(expected_rows, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            ).hexdigest()
            labels = {str(item.get("label") or "") for item in entry.get("annotations") or []}
            return (
                central.get("required") is True
                and central.get("location") == "central_lower"
                and central.get("widget") in {"qa_scan_tree", "exact_rescan_tree"}
                and central.get("mapped") is True
                and central.get("viewable") is True
                and central.get("within_center_pane") is True
                and central.get("below_scan_entry") is True
                and central.get("positive_geometry") is True
                and central.get("final_row_visible") is True
                and central.get("rows_exact_match") is True
                and observed_rows == expected_rows
                and int(central.get("row_count", -1)) == len(observed_rows)
                and central.get("observed_rows_sha256") == observed_sha256
                and central.get("expected_rows_sha256") == expected_sha256
                and "중앙 하단 실제 스캔 목록" in labels
            )

        central_scan_list_contract_ok = (
            {entry["name"] for entry in entries if entry.get("central_scan_list") is not None}
            == main_screen_names
            and all(central_entry_ok(entry) for entry in entries)
        )
        raw_hashes = [str(entry.get("raw_sha256") or "") for entry in entries]
        raw_image_uniqueness_ok = (
            len(raw_hashes) == len(expected_names)
            and all(raw_hashes)
            and len(set(raw_hashes)) == len(raw_hashes)
        )
        workflow_state_contract_ok = (
            bool(workflow_state_contracts)
            and all(item.get("status") == "PASS" for item in workflow_state_contracts)
        )
        image_contract_ok = all(
            entry["width"] == capture_width
            and entry["height"] == capture_height
            and entry["monitor_rect"] == expected_monitor_rect
            and str(entry["monitor_device"]).casefold() == display_device.casefold()
            and entry["monitor_is_primary"] is False
            and entry["monitor_contract_ok"] is True
            and entry["target_is_foreground"] is True
            and entry["target_contained_in_monitor"] is True
            and entry["app_root_monitor_is_primary"] is False
            and str(entry["app_root_monitor_device"]).casefold() == display_device.casefold()
            and entry["app_root_matches_work_area"] is True
            and not entry["blank_suspected"]
            and entry["raw_sha256"] != entry["annotated_sha256"]
            and entry["pixel_qa"]["changed_pixel_count"] > 0
            and entry["pixel_qa"]["near_black_increase_ratio"] <= 0.005
            and entry["annotations"]
            for entry in entries
        ) and central_scan_list_contract_ok
        source_identity_after = _source_identity(
            SOURCE_ROOT,
            expected_commit=expected_commit,
            expected_tree=expected_tree,
        )
        status = (
            "PASS"
            if actual_names == expected_names
            and image_contract_ok
            and raw_image_uniqueness_ok
            and workflow_state_contract_ok
            and source_identity_after == source_identity
            and module_origins.get("status") == "PASS"
            and dpi_awareness.get("status") == "PASS"
            and len(previsible_placements) == 2
            and all(item.get("status") == "PASS" for item in previsible_placements)
            and len(after_quiescence) == 2
            and all(item.get("status") == "PASS" for item in after_quiescence)
            else "FAIL"
        )
        manifest = {
            "report_version": "label-match-outline-manual-capture-v2",
            "status": status,
            "app_version": label_module.APP_VERSION,
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "asset_folder": ASSET_ROOT.name,
            "source_identity": source_identity_after,
            "source_identity_unchanged": source_identity_after == source_identity,
            "module_origin_contract": module_origins,
            "dpi_awareness": dpi_awareness,
            "monitor_target": monitor_target,
            "previsible_placement_contract": {
                "status": "PASS"
                if len(previsible_placements) == 2
                and all(item.get("status") == "PASS" for item in previsible_placements)
                else "FAIL",
                "instances": previsible_placements,
            },
            "scheduled_job_quiescence": {
                "status": "PASS"
                if len(after_quiescence) == 2
                and all(item.get("status") == "PASS" for item in after_quiescence)
                else "FAIL",
                "instances": after_quiescence,
            },
            "capture_geometry": list(CAPTURE_GEOMETRY),
            "capture_size": [capture_width, capture_height],
            "annotation_contract": {
                "color_rgb": list(RED),
                "rectangle_width_px": 7,
                "coordinate_sources": ["tk_widget_geometry", "win32_window_geometry"],
                "composition": "transparent RGBA overlay composited onto untouched RGB raw capture",
                "near_black_threshold_rgb": 16,
                "maximum_near_black_increase_ratio": 0.005,
            },
            "isolation": isolation,
            "expected_names": expected_names,
            "actual_names": actual_names,
            "image_contract_ok": image_contract_ok,
            "raw_image_uniqueness_contract": {
                "status": "PASS" if raw_image_uniqueness_ok else "FAIL",
                "expected_unique_count": len(expected_names),
                "observed_unique_count": len(set(raw_hashes)),
            },
            "workflow_state_contract": {
                "status": "PASS" if workflow_state_contract_ok else "FAIL",
                "checkpoints": workflow_state_contracts,
            },
            "central_scan_list_contract": {
                "status": "PASS" if central_scan_list_contract_ok else "FAIL",
                "required_location": "central_lower",
                "required_main_screen_names": sorted(main_screen_names),
                "evidenced_main_screen_names": sorted(
                    entry["name"]
                    for entry in entries
                    if entry.get("central_scan_list") is not None
                ),
            },
            "contact_sheet": {
                "path": _relative(CONTACT_SHEET_PATH),
                "sha256": ui._sha256_file(CONTACT_SHEET_PATH),
            },
            "images": entries,
        }
        manifest["privacy_contract"] = _privacy_contract(manifest)
        if manifest["privacy_contract"]["status"] != "PASS":
            manifest["status"] = "FAIL"
        MANIFEST_PATH.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Capture the v2.0.36 Label_Match Outline manual assets on DISPLAY2")
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--asset-root", type=Path, default=DEFAULT_ASSET_ROOT)
    parser.add_argument("--expected-commit", default=EXPECTED_SOURCE_COMMIT)
    parser.add_argument("--expected-tree", default=EXPECTED_SOURCE_TREE)
    parser.add_argument("--display-device", default=TARGET_DISPLAY_DEVICE)
    args = parser.parse_args()
    manifest = run(
        source_root=args.source_root,
        asset_root=args.asset_root,
        expected_commit=args.expected_commit,
        expected_tree=args.expected_tree,
        display_device=args.display_device,
    )
    print(json.dumps({"status": manifest["status"], "manifest": str(MANIFEST_PATH), "images": len(manifest["images"])}, ensure_ascii=False))
    return 0 if manifest["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())

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
import importlib.machinery
import inspect
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping, Sequence

from PIL import Image, ImageGrab, ImageStat


ROOT = Path(__file__).resolve().parents[1]
CAPTURE_OUTPUT_BASE = (ROOT.parent / "tmp" / "label_match_operator_ui").resolve()
DEFAULT_SOURCE_ROOT = Path(
    os.environ.get("LABEL_MATCH_CAPTURE_SOURCE_ROOT", ROOT)
).resolve()
EXPECTED_SOURCE_COMMIT = os.environ.get(
    "LABEL_MATCH_CAPTURE_EXPECTED_COMMIT", ""
).strip()
EXPECTED_SOURCE_TREE = os.environ.get(
    "LABEL_MATCH_CAPTURE_EXPECTED_TREE", ""
).strip()
TARGET_DISPLAY_DEVICE = r"\\.\DISPLAY2"
TARGET_DISPLAY_MONITOR_AREA = (693, -1440, 3253, 0)
TARGET_DISPLAY_WORK_AREA = (693, -1440, 3253, -48)
TARGET_DISPLAY_DPI = (96, 96)
DEFAULT_SIZES = (
    (1366, 768),
    (1440, 900),
    (1920, 1080),
    (2560, 1080),
    (2560, 1392),
)
DEFAULT_STATE_IDS = (
    "waiting",
    "qa_master",
    "exact_first",
    "exact_active",
    "exact_complete",
    "qa_progress",
    "qa_product_2",
    "qa_product_3",
    "cancellation_conflict",
    "sealed",
    "error",
    "full_complete",
    "partial_complete",
    "recovery",
    "history_readonly",
    "submission_blocked",
)
CAPTURE_MANIFEST_SCHEMA_VERSION = 6
APPLICATION_STARTUP_PATH = "disabled_for_inprocess_matrix"
CANCELLATION_CONFLICT_COUNT = 3
CANCELLATION_CONFLICT_CODE = "IMMUTABLE_CAS"
CANCELLATION_CONFLICT_BUNDLE_ID = "PACKAGE-CANCEL-CAPTURE-001"
CANCELLATION_CONFLICT_TITLE = "중앙 취소 확인 필요"
CANCELLATION_CONFLICT_MESSAGE = (
    "미확인 3건 · IMMUTABLE_CAS · PACKAGE-CANCEL-CAPTURE-001. "
    "해당 트레이는 반출하지 말고 관리자에게 중앙 취소 상태 확인을 요청하세요."
)
CANCELLATION_SURFACE_CAPTURE_CONTRACT = {
    "persistent_capture_states": ["cancellation_conflict"],
    "modal_only_exclusions": {
        "cancellation_pending": {
            "runtime_surface": "owned messagebox.showwarning (nonpersistent)",
            "reason": "excluded: root-only visible capture rejects extra PID toplevels",
        },
        "cancellation_acked": {
            "runtime_surface": "owned messagebox.showinfo (nonpersistent)",
            "reason": "excluded: root-only visible capture rejects extra PID toplevels",
        },
    },
    "extra_visible_toplevels_allowed": False,
}
AUTHORITATIVE_CAPTURE_SOURCE = "ImageGrab(visible-client-bbox)"
DIAGNOSTIC_PRINTWINDOW_CAPTURE_SOURCE = (
    "PrintWindow(PW_RENDERFULLCONTENT)+client-crop"
)
HARNESS_ATTESTED_PATHS = (
    "tools/capture_label_operator_ui.py",
    "tests/test_capture_label_operator_ui.py",
)
DEFAULT_SCALE = 1.0
CAPTURE_ITEM_CODE = "AAA2270730100"
CAPTURE_SECONDARY_ITEM_CODE = "AAA2270730200"
MIN_SCALE = 0.7
MAX_SCALE = 2.5
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
TREE_HEADING_SCAN_HEIGHT = 128
TREE_HEADING_IMAGE_GAP_PX = 2
TREE_DATA_CELL_GUTTER_PX = 8
TREE_DATA_VERTICAL_PADDING_PX = 4

REQUIRED_WIDGET_ATTRS = (
    "main_frame",
    "operator_header_frame",
    "operator_title_label",
    "operator_header_context_label",
    "top_right_frame",
    "clock_label",
    "settings_button",
    "about_button",
    "workbench_frame",
    "left_context_card",
    "top_card",
    "right_activity_card",
    "big_display_label",
    "progress_frame",
    "operator_input_frame",
    "entry",
    "workflow_notice_frame",
    "workflow_notice_title_label",
    "workflow_notice_label",
    "workflow_notice_action_button",
    "current_set_tree",
    "exact_rescan_tree",
    "live_scan_notebook",
    "qa_scan_frame",
    "qa_scan_detail_frame",
    "qa_scan_detail_text",
    "exact_rescan_frame",
    "exact_rescan_detail_frame",
    "exact_rescan_detail_text",
    "exact_rescan_detail_metadata_label",
    "exact_rescan_detail_scrollbar",
    "operator_history_notebook",
    "hist_header_frame",
    "hist_header_label",
    "hist_control_frame",
    "session_tree",
    "history_tree",
    "summary_tree",
    "operator_action_frame",
    "bottom_frame",
    "reset_button",
    "manual_complete_button",
    "exact_rescan_button",
    "operator_status_frame",
    "operator_footer_label",
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
TARGET_MODULE_PREFIXES = ("Label_Match", "ui", "core", "package_logistics")


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
    selected_qa_index: int = 0


def fixture_parsed_scans(fixture: StateFixture) -> tuple[str, ...]:
    """Mirror the runtime's accepted comparison values for each raw QA scan."""

    return tuple(CAPTURE_ITEM_CODE for _ in fixture.qa_scans)


@dataclass(slots=True)
class EnvironmentIsolation:
    """Capture-only environment mutation with exact restoration state.

    ``previous`` and ``sensitive_values`` are deliberately never serialized.
    They exist only long enough to restore the host process and redact evidence.
    """

    guards: dict[str, str]
    previous: dict[str, str | None]
    sensitive_values: dict[str, str]
    removed_keys: tuple[str, ...]

    def restore(self) -> dict[str, Any]:
        for key, value in self.previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        mismatches = [
            key
            for key, value in self.previous.items()
            if os.environ.get(key) != value
        ]
        if mismatches:
            raise RuntimeError(
                "capture environment restoration failed: " + ",".join(mismatches)
            )
        return {
            "status": "PASS",
            "restored_key_count": len(self.previous),
            "restored_keys": sorted(self.previous),
            "values_recorded": False,
        }


@dataclass(slots=True)
class ImportIsolation:
    previous_modules: dict[str, Any]
    previous_sys_path: tuple[str, ...]
    previous_meta_path: list[Any]
    previous_meta_path_entries: tuple[Any, ...]
    previous_path_hooks: list[Any]
    previous_path_hook_entries: tuple[Any, ...]
    previous_importer_cache: dict[str, Any]
    previous_importer_cache_entries: dict[str, Any]
    previous_pycache_prefix: str | None
    previous_dont_write_bytecode: bool

    def restore(self) -> dict[str, Any]:
        for name in tuple(sys.modules):
            if _is_target_module_name(name):
                sys.modules.pop(name, None)
        sys.modules.update(self.previous_modules)
        sys.path[:] = list(self.previous_sys_path)
        self.previous_meta_path[:] = list(self.previous_meta_path_entries)
        self.previous_path_hooks[:] = list(self.previous_path_hook_entries)
        self.previous_importer_cache.clear()
        self.previous_importer_cache.update(
            self.previous_importer_cache_entries
        )
        sys.meta_path = self.previous_meta_path
        sys.path_hooks = self.previous_path_hooks
        sys.path_importer_cache = self.previous_importer_cache
        sys.pycache_prefix = self.previous_pycache_prefix
        sys.dont_write_bytecode = self.previous_dont_write_bytecode
        wrong_modules = [
            name
            for name, module in self.previous_modules.items()
            if sys.modules.get(name) is not module
        ]
        meta_path_restored = (
            sys.meta_path is self.previous_meta_path
            and len(sys.meta_path) == len(self.previous_meta_path_entries)
            and all(
                current is previous
                for current, previous in zip(
                    sys.meta_path, self.previous_meta_path_entries
                )
            )
        )
        path_hooks_restored = (
            sys.path_hooks is self.previous_path_hooks
            and len(sys.path_hooks) == len(self.previous_path_hook_entries)
            and all(
                current is previous
                for current, previous in zip(
                    sys.path_hooks, self.previous_path_hook_entries
                )
            )
        )
        importer_cache_restored = (
            sys.path_importer_cache is self.previous_importer_cache
            and set(sys.path_importer_cache)
            == set(self.previous_importer_cache_entries)
            and all(
                sys.path_importer_cache[key] is value
                for key, value in self.previous_importer_cache_entries.items()
            )
        )
        if (
            wrong_modules
            or tuple(sys.path) != self.previous_sys_path
            or not meta_path_restored
            or not path_hooks_restored
            or not importer_cache_restored
            or sys.pycache_prefix != self.previous_pycache_prefix
            or sys.dont_write_bytecode != self.previous_dont_write_bytecode
        ):
            raise RuntimeError(
                "import isolation restoration failed: "
                f"modules={wrong_modules} sys_path="
                f"{tuple(sys.path) == self.previous_sys_path} "
                f"meta_path={meta_path_restored} path_hooks={path_hooks_restored} "
                f"importer_cache={importer_cache_restored} "
                f"pycache_prefix="
                f"{sys.pycache_prefix == self.previous_pycache_prefix} "
                f"dont_write_bytecode="
                f"{sys.dont_write_bytecode == self.previous_dont_write_bytecode}"
            )
        return {
            "status": "PASS",
            "restored_module_count": len(self.previous_modules),
            "sys_path_restored": True,
            "meta_path_restored": True,
            "path_hooks_restored": True,
            "path_importer_cache_restored": True,
            "pycache_prefix_restored": True,
            "dont_write_bytecode_restored": True,
        }


def _realistic_phs_scan(stage: str, serial: int) -> str:
    """Return a long, printable production-like PHS barcode fixture."""

    return (
        f"PHS|CLC={CAPTURE_ITEM_CODE}|SPC=HOUSING ASSY-REAR LH EXPORT "
        "BLACK HIGH-GLOSS|PHS=2|6D=20260716|LOT=KM260716-B02|"
        f"STAGE={stage}|SERIAL=260716{serial:06d}|"
        "TRACE=LINE-04-STATION-PACKAGING-CUSTOMER-VALIDATION-"
        "MEMBERSHIP-CHECK-PRIMARY-SCAN-VALUE-END"
    )


def build_state_fixtures() -> tuple[StateFixture, ...]:
    """Return the complete deterministic operator-state matrix."""

    master = _realistic_phs_scan("MASTER", 1)
    product_1 = _realistic_phs_scan("PRODUCT-1", 2)
    product_2 = _realistic_phs_scan("PRODUCT-2", 3)
    product_3 = _realistic_phs_scan("PRODUCT-3", 4)
    final_label = _realistic_phs_scan("FINAL-LABEL", 5)
    qa_one = (master,)
    qa_two = (*qa_one, product_1)
    qa_three = (*qa_two, product_2)
    qa_four = (*qa_three, product_3)
    qa_full = (*qa_four, final_label)
    exact_one = (_realistic_phs_scan("F4-EXACT-1", 101),)
    exact_two = (
        *exact_one,
        _realistic_phs_scan("F4-EXACT-2", 102),
    )
    exact_full = (*exact_two, _realistic_phs_scan("F4-EXACT-3", 103))
    return (
        StateFixture("waiting", "대기"),
        StateFixture(
            "qa_master",
            "현품표 완료",
            qa_scans=qa_one,
            last_normal_scan=master,
        ),
        StateFixture(
            "exact_first",
            "F4 재스캔 1/3",
            qa_scans=qa_one,
            exact_barcodes=exact_one,
            exact_target=3,
            exact_active=True,
            last_normal_scan=exact_one[-1],
        ),
        StateFixture(
            "exact_active",
            "F4 재스캔 2/3",
            qa_scans=qa_one,
            exact_barcodes=exact_two,
            exact_target=3,
            exact_active=True,
            last_normal_scan=exact_two[-1],
        ),
        StateFixture(
            "exact_complete",
            "F4 재스캔 3/3 완료",
            qa_scans=qa_one,
            exact_barcodes=exact_full,
            exact_target=3,
            exact_complete=True,
            last_normal_scan=exact_full[-1],
        ),
        StateFixture(
            "qa_progress",
            "제품 1 완료",
            qa_scans=qa_two,
            exact_barcodes=exact_full,
            exact_target=3,
            exact_complete=True,
            last_normal_scan=product_1,
        ),
        StateFixture(
            "qa_product_2",
            "제품 2 완료",
            qa_scans=qa_three,
            last_normal_scan=product_2,
        ),
        StateFixture(
            "qa_product_3",
            "제품 3 완료",
            qa_scans=qa_four,
            last_normal_scan=product_3,
            selected_qa_index=4,
        ),
        StateFixture(
            "cancellation_conflict",
            "중앙 취소 작업자 확인",
            qa_scans=qa_four,
            last_normal_scan=product_3,
            selected_qa_index=4,
        ),
        StateFixture(
            "sealed",
            "sealed 상속",
            qa_scans=(f"SEALED TRANSFER · {CAPTURE_ITEM_CODE}",),
            sealed_transfer=True,
            last_normal_scan=f"SEALED TRANSFER · {CAPTURE_ITEM_CODE}",
        ),
        StateFixture(
            "error",
            "오류",
            qa_scans=qa_four,
            has_error=True,
            error_message=(
                "현품표와 제품의 PHS 멤버십이 불일치합니다.\n"
                f"- 현품표: {master}\n"
                f"- 스캔 제품: {_realistic_phs_scan('MISMATCH', 999)}\n"
                "→ 제품을 제거하고 새 현품표부터 다시 스캔하세요."
            ),
            last_normal_scan=product_3,
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


def parse_work_area(value: object) -> tuple[int, int, int, int]:
    if isinstance(value, (tuple, list)) and len(value) == 4:
        parts = list(value)
    else:
        parts = [part.strip() for part in str(value or "").split(",")]
    if len(parts) != 4:
        raise argparse.ArgumentTypeError(
            "work area must be LEFT,TOP,RIGHT,BOTTOM"
        )
    try:
        rect = tuple(int(part) for part in parts)
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError(
            "work area must contain four integers"
        ) from exc
    left, top, right, bottom = rect
    if right - left < 1024 or bottom - top < 720:
        raise argparse.ArgumentTypeError(
            f"work area must be at least 1024x720: {rect}"
        )
    return rect


def validate_capture_matrix_request(
    sizes: Sequence[Sequence[int]], state_ids: Sequence[str]
) -> tuple[tuple[tuple[int, int], ...], tuple[str, ...]]:
    """Require the complete, non-duplicated 5 x 16 evidence matrix."""

    normalized_sizes = tuple(tuple(map(int, size)) for size in sizes)
    normalized_states = tuple(str(state).strip() for state in state_ids)
    if len(normalized_sizes) != len(set(normalized_sizes)):
        raise RuntimeError("capture sizes contain programmatic duplicates")
    if len(normalized_states) != len(set(normalized_states)):
        raise RuntimeError("capture states contain programmatic duplicates")
    if len(normalized_sizes) != len(DEFAULT_SIZES) or set(normalized_sizes) != set(
        DEFAULT_SIZES
    ):
        raise RuntimeError(
            "capture sizes must contain every DEFAULT_SIZES entry exactly once"
        )
    if len(normalized_states) != len(DEFAULT_STATE_IDS) or set(
        normalized_states
    ) != set(DEFAULT_STATE_IDS):
        raise RuntimeError(
            "capture states must contain every DEFAULT_STATE_IDS entry exactly once"
        )
    return normalized_sizes, normalized_states


def validate_cancellation_surface_capture_contract(
    state_ids: Sequence[str],
    contract: Mapping[str, Any] = CANCELLATION_SURFACE_CAPTURE_CONTRACT,
) -> dict[str, Any]:
    """Fail closed if modal-only cancellation outcomes become fake root states."""

    normalized_states = set(str(value or "").strip() for value in state_ids)
    if contract != CANCELLATION_SURFACE_CAPTURE_CONTRACT:
        raise RuntimeError("cancellation surface manifest contract changed")
    exclusions = set(contract["modal_only_exclusions"])
    if "cancellation_conflict" not in normalized_states:
        raise RuntimeError("persistent cancellation conflict state is missing")
    leaked = sorted(set(exclusions).intersection(normalized_states))
    if leaked:
        raise RuntimeError(
            "modal-only cancellation states cannot be persistent captures: "
            + ",".join(leaked)
        )
    return {
        "status": "PASS",
        "persistent_capture_states": ["cancellation_conflict"],
        "modal_only_excluded_states": sorted(exclusions),
        "extra_visible_toplevels_allowed": False,
    }


def _rect_contains(outer: Sequence[int], inner: Sequence[int], tolerance: int = 1) -> bool:
    return (
        int(inner[0]) >= int(outer[0]) - tolerance
        and int(inner[1]) >= int(outer[1]) - tolerance
        and int(inner[2]) <= int(outer[2]) + tolerance
        and int(inner[3]) <= int(outer[3]) + tolerance
    )


def _windows_monitor_inventory() -> list[dict[str, Any]]:
    if os.name != "nt":
        raise RuntimeError("DISPLAY2 capture requires Windows")
    import win32api
    import win32con

    result = []
    for handle, _dc, _rect in win32api.EnumDisplayMonitors():
        info = dict(win32api.GetMonitorInfo(handle))
        x_dpi = ctypes.c_uint(0)
        y_dpi = ctypes.c_uint(0)
        dpi_hresult = int(
            ctypes.windll.shcore.GetDpiForMonitor(
                int(handle),
                0,
                ctypes.byref(x_dpi),
                ctypes.byref(y_dpi),
            )
        )
        if dpi_hresult != 0 or not x_dpi.value or not y_dpi.value:
            raise RuntimeError(
                f"GetDpiForMonitor failed for {info.get('Device')}: "
                f"HRESULT={dpi_hresult}"
            )
        result.append(
            {
                "handle": int(handle),
                "device": str(info.get("Device") or ""),
                "is_primary": bool(
                    int(info.get("Flags", 0)) & int(win32con.MONITORINFOF_PRIMARY)
                ),
                "monitor_rect": list(map(int, info["Monitor"])),
                "work_rect": list(map(int, info["Work"])),
                "dpi": [int(x_dpi.value), int(y_dpi.value)],
                "dpi_hresult": dpi_hresult,
            }
        )
    return result


def resolve_capture_monitor(
    display_device: str,
    expected_work_area: Sequence[int],
    *,
    inventory: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    device = str(display_device or "").strip()
    if device.casefold() != TARGET_DISPLAY_DEVICE.casefold():
        raise RuntimeError(
            f"capture device is locked to {TARGET_DISPLAY_DEVICE}: {device!r}"
        )
    expected = parse_work_area(expected_work_area)
    if expected[:2] == (0, 0):
        raise RuntimeError("primary-origin +0+0 placement is forbidden")
    if expected != TARGET_DISPLAY_WORK_AREA:
        raise RuntimeError(
            "DISPLAY2 work area is locked to "
            f"{TARGET_DISPLAY_WORK_AREA}: requested={expected}"
        )
    candidates = list(inventory) if inventory is not None else _windows_monitor_inventory()
    matches = [
        dict(item)
        for item in candidates
        if str(item.get("device") or "").casefold() == device.casefold()
    ]
    if len(matches) != 1:
        raise RuntimeError(f"expected exactly one {device} monitor, found {len(matches)}")
    target = matches[0]
    target["monitor_rect"] = list(map(int, target["monitor_rect"]))
    target["work_rect"] = list(map(int, target["work_rect"]))
    target["dpi"] = list(map(int, target.get("dpi") or ()))
    target["is_primary"] = bool(target.get("is_primary"))
    if target["is_primary"]:
        raise RuntimeError(f"capture target must be non-primary: {device}")
    if tuple(target["work_rect"]) != expected:
        raise RuntimeError(
            f"{device} work area changed: expected={expected} actual={target['work_rect']}"
        )
    if tuple(target["monitor_rect"]) != TARGET_DISPLAY_MONITOR_AREA:
        raise RuntimeError(
            f"{device} monitor area changed: expected={TARGET_DISPLAY_MONITOR_AREA} "
            f"actual={target['monitor_rect']}"
        )
    if tuple(target["dpi"]) != TARGET_DISPLAY_DPI:
        raise RuntimeError(
            f"{device} DPI changed: expected={TARGET_DISPLAY_DPI} actual={target['dpi']}"
        )
    if not _rect_contains(target["monitor_rect"], target["work_rect"]):
        raise RuntimeError(
            f"work area is outside monitor bounds: {target}"
        )
    target.update(
        {
            "device": device,
            "expected_work_rect": list(expected),
            "work_size": [expected[2] - expected[0], expected[3] - expected[1]],
            "window_geometry": [
                expected[2] - expected[0],
                expected[3] - expected[1],
                expected[0],
                expected[1],
            ],
        }
    )
    return target


def _git_text(source_root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(source_root), *args],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return completed.stdout.strip()


def verify_no_bytecode_artifacts(source_root: Path) -> dict[str, Any]:
    """Reject every source-local bytecode cache before Python can import it."""

    root = source_root.resolve()
    artifacts: list[str] = []

    def on_walk_error(error: OSError) -> None:
        raise RuntimeError(
            f"cannot audit source tree for bytecode: {type(error).__name__}: {error}"
        ) from error

    for current, directories, files in os.walk(
        root, topdown=True, onerror=on_walk_error, followlinks=False
    ):
        current_path = Path(current)
        if current_path == root / ".git":
            directories[:] = []
            continue
        for directory in tuple(directories):
            candidate = current_path / directory
            if directory.casefold() == "__pycache__":
                artifacts.append(candidate.relative_to(root).as_posix() + "/")
                directories.remove(directory)
            elif directory == ".git":
                directories.remove(directory)
        for filename in files:
            if Path(filename).suffix.casefold() in {".pyc", ".pyo"}:
                artifacts.append(
                    (current_path / filename).relative_to(root).as_posix()
                )
    if artifacts:
        raise RuntimeError(
            "source tree contains forbidden Python bytecode artifacts: "
            + ",".join(sorted(artifacts)[:20])
        )
    return {
        "status": "PASS",
        "source_root": str(root),
        "bytecode_artifact_count": 0,
        "pycache_directories_allowed": False,
    }


def verify_source_identity(
    source_root: Path,
    *,
    expected_commit: str,
    expected_tree: str,
) -> dict[str, Any]:
    root = source_root.resolve()
    if not str(expected_commit).strip() or not str(expected_tree).strip():
        raise RuntimeError(
            "expected source commit and tree must be supplied explicitly"
        )
    if not (root / "Label_Match.py").is_file() or not (root / "ui").is_dir():
        raise RuntimeError(f"invalid Label_Match source root: {root}")
    top_level = Path(_git_text(root, "rev-parse", "--show-toplevel")).resolve()
    if top_level != root:
        raise RuntimeError(
            f"source_root must equal git show-toplevel exactly: "
            f"source={root} git={top_level}"
        )
    commit = _git_text(root, "rev-parse", "HEAD")
    tree = _git_text(root, "rev-parse", "HEAD^{tree}")
    status = _git_text(root, "status", "--porcelain=v1", "--untracked-files=all")
    if commit != str(expected_commit).strip():
        raise RuntimeError(
            f"source commit mismatch: expected={expected_commit} actual={commit}"
        )
    if tree != str(expected_tree).strip():
        raise RuntimeError(
            f"source tree mismatch: expected={expected_tree} actual={tree}"
        )
    if status:
        raise RuntimeError(f"source worktree must be clean: {root}")
    return {
        "root": str(root),
        "git_show_toplevel_exact": True,
        "commit": commit,
        "tree": tree,
        "worktree_clean": True,
    }


def verify_harness_identity(tool_root: Path = ROOT) -> dict[str, Any]:
    root = tool_root.resolve()
    top_level = Path(_git_text(root, "rev-parse", "--show-toplevel")).resolve()
    if top_level != root:
        raise RuntimeError(
            f"harness root must equal git show-toplevel: root={root} git={top_level}"
        )
    missing = [path for path in HARNESS_ATTESTED_PATHS if not (root / path).is_file()]
    if missing:
        raise RuntimeError("harness attestation files missing: " + ",".join(missing))
    dirty = _git_text(
        root,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
        "--",
        *HARNESS_ATTESTED_PATHS,
    )
    if dirty:
        raise RuntimeError("harness attestation paths must be clean at HEAD")
    files = {}
    for relative in HARNESS_ATTESTED_PATHS:
        evidence = _verify_tracked_head_file(root, root / relative)
        evidence["sha256"] = _sha256(root / relative)
        files[relative] = evidence
    return {
        "status": "PASS",
        "root": str(root),
        "commit": _git_text(root, "rev-parse", "HEAD"),
        "tree": _git_text(root, "rev-parse", "HEAD^{tree}"),
        "attested_paths_clean": True,
        "files": files,
    }


def validate_execution_source_binding(
    harness_root: Path,
    source_root: Path,
    harness_identity: Mapping[str, Any],
    *,
    expected_commit: str,
    expected_tree: str,
) -> dict[str, Any]:
    resolved_harness = harness_root.resolve()
    resolved_source = source_root.resolve()
    issues = []
    if resolved_harness != resolved_source:
        issues.append("harness_root_does_not_equal_source_root")
    if str(harness_identity.get("commit") or "") != str(expected_commit).strip():
        issues.append("harness_commit_does_not_match_expected_source_commit")
    if str(harness_identity.get("tree") or "") != str(expected_tree).strip():
        issues.append("harness_tree_does_not_match_expected_source_tree")
    if harness_identity.get("status") != "PASS" or not harness_identity.get(
        "attested_paths_clean"
    ):
        issues.append("harness_identity_not_clean_pass")
    if issues:
        raise RuntimeError("capture execution/source binding failed: " + ",".join(issues))
    return {
        "status": "PASS",
        "harness_root_equals_source_root": True,
        "commit": str(expected_commit).strip(),
        "tree": str(expected_tree).strip(),
    }


def _path_is_within(path: Path, parent: Path) -> bool:
    resolved = path.resolve()
    resolved_parent = parent.resolve()
    return resolved == resolved_parent or resolved.is_relative_to(resolved_parent)


def _verify_tracked_head_file(source_root: Path, path: Path) -> dict[str, Any]:
    root = source_root.resolve()
    resolved = path.resolve()
    if not _path_is_within(resolved, root) or resolved == root:
        raise RuntimeError(f"import origin escaped source root: {resolved}")
    relative = resolved.relative_to(root).as_posix()
    tracked = _git_text(root, "ls-files", "--error-unmatch", "--", relative)
    if tracked.replace("\\", "/") != relative:
        raise RuntimeError(f"import origin is not exactly tracked: {relative}")
    head_blob = _git_text(root, "rev-parse", f"HEAD:{relative}")
    filtered_blob = _git_text(
        root,
        "hash-object",
        f"--path={relative}",
        "--",
        relative,
    )
    if not head_blob or filtered_blob != head_blob:
        raise RuntimeError(
            f"imported file differs from HEAD after Git filters: {relative} "
            f"HEAD={head_blob} filtered={filtered_blob}"
        )
    return {
        "path": relative,
        "tracked": True,
        "head_blob": head_blob,
        "filtered_worktree_blob": filtered_blob,
        "head_blob_matches_filtered_worktree": True,
    }


def _is_target_module_name(name: str) -> bool:
    return any(
        name == prefix or name.startswith(f"{prefix}.")
        for prefix in TARGET_MODULE_PREFIXES
    )


def verify_import_origins(source_root: Path) -> dict[str, dict[str, Any]]:
    origins: dict[str, dict[str, Any]] = {}
    for name, module in tuple(sys.modules.items()):
        targeted = _is_target_module_name(name)
        module_file = getattr(module, "__file__", None)
        if not module_file:
            if targeted:
                raise RuntimeError(
                    f"target app module has no concrete source file: {name}"
                )
            continue
        path = Path(module_file).resolve()
        inside = _path_is_within(path, source_root)
        if targeted and not inside:
            raise RuntimeError(f"app module escaped source root: {name}={path}")
        if not inside:
            continue
        evidence = _verify_tracked_head_file(source_root, path)
        if targeted:
            spec = getattr(module, "__spec__", None)
            spec_origin = getattr(spec, "origin", None)
            loader = getattr(spec, "loader", None)
            module_loader = getattr(module, "__loader__", None)
            if not spec or not spec_origin:
                raise RuntimeError(
                    f"target app module has no concrete import spec origin: {name}"
                )
            origin_path = Path(spec_origin).resolve()
            if origin_path != path:
                raise RuntimeError(
                    f"target app module file/spec mismatch: {name} "
                    f"file={path} spec={origin_path}"
                )
            if type(loader) is not importlib.machinery.SourceFileLoader:
                raise RuntimeError(
                    f"target app module must use exact SourceFileLoader: "
                    f"{name}={type(loader).__module__}.{type(loader).__qualname__}"
                )
            if getattr(spec, "name", None) != name or loader.name != name:
                raise RuntimeError(
                    f"target app module spec/loader name mismatch: {name}"
                )
            if module_loader is not loader:
                raise RuntimeError(
                    f"target app module loader/spec mismatch: {name}"
                )
            loader_path = Path(loader.get_filename(name)).resolve()
            if loader_path != path:
                raise RuntimeError(
                    f"target app module loader path mismatch: {name} "
                    f"file={path} loader={loader_path}"
                )
            if not path.name.endswith(
                tuple(importlib.machinery.SOURCE_SUFFIXES)
            ):
                raise RuntimeError(
                    f"target app module is not source-backed: {name}={path}"
                )
            evidence.update(
                {
                    "module_file": str(path),
                    "spec_origin": str(origin_path),
                    "loader_file": str(loader_path),
                    "loader_module": type(loader).__module__,
                    "loader_class": type(loader).__qualname__,
                    "loader_source_exact": True,
                }
            )
        origins[name] = evidence
    required = {"Label_Match", "ui"}
    if (source_root / "core").is_dir():
        required.add("core")
    if (source_root / "package_logistics.py").is_file():
        required.add("package_logistics")
    missing = sorted(required - set(origins))
    if missing:
        raise RuntimeError(
            "required app import origins were not observed: " + ",".join(missing)
        )
    return dict(sorted(origins.items()))


def import_label_match_from_source(
    source_root: Path,
) -> tuple[Any, dict[str, dict[str, Any]], ImportIsolation]:
    root = source_root.resolve()
    verify_no_bytecode_artifacts(root)
    if (
        type(sys.meta_path) is not list
        or type(sys.path_hooks) is not list
        or type(sys.path_importer_cache) is not dict
    ):
        raise RuntimeError(
            "trusted import isolation requires standard list/list/dict globals"
        )
    isolation = ImportIsolation(
        previous_modules={
            name: module
            for name, module in tuple(sys.modules.items())
            if _is_target_module_name(name)
        },
        previous_sys_path=tuple(sys.path),
        previous_meta_path=sys.meta_path,
        previous_meta_path_entries=tuple(sys.meta_path),
        previous_path_hooks=sys.path_hooks,
        previous_path_hook_entries=tuple(sys.path_hooks),
        previous_importer_cache=sys.path_importer_cache,
        previous_importer_cache_entries=dict(sys.path_importer_cache),
        previous_pycache_prefix=sys.pycache_prefix,
        previous_dont_write_bytecode=sys.dont_write_bytecode,
    )
    for name in tuple(sys.modules):
        if _is_target_module_name(name):
            sys.modules.pop(name, None)
    root_text = str(root)
    sys.path[:] = [
        entry
        for entry in sys.path
        if str(Path(entry or ".").resolve()).casefold() != root_text.casefold()
    ]
    sys.path.insert(0, root_text)
    sys.meta_path = [
        importlib.machinery.BuiltinImporter,
        importlib.machinery.FrozenImporter,
        importlib.machinery.PathFinder,
    ]
    sys.path_hooks = [
        importlib.machinery.FileFinder.path_hook(
            (
                importlib.machinery.SourceFileLoader,
                importlib.machinery.SOURCE_SUFFIXES,
            ),
            (
                importlib.machinery.ExtensionFileLoader,
                importlib.machinery.EXTENSION_SUFFIXES,
            ),
        )
    ]
    sys.path_importer_cache = {}
    sys.pycache_prefix = None
    sys.dont_write_bytecode = True
    importlib.invalidate_caches()
    try:
        module = importlib.import_module("Label_Match")
        origins = verify_import_origins(root)
    except Exception:
        isolation.restore()
        raise
    return module, origins, isolation


def assert_descendant(path: Path, parent: Path, *, label: str) -> Path:
    resolved = path.resolve()
    resolved_parent = parent.resolve()
    if resolved == resolved_parent or not resolved.is_relative_to(resolved_parent):
        raise RuntimeError(f"{label} must stay below {resolved_parent}: {resolved}")
    return resolved


def assert_external_capture_descendant(
    path: Path,
    output_base: Path,
    source_root: Path,
    *,
    label: str,
) -> Path:
    resolved_base = output_base.resolve()
    resolved_source = source_root.resolve()
    if _path_is_within(resolved_base, resolved_source):
        raise RuntimeError(
            f"capture output base must stay outside source root: "
            f"base={resolved_base} source={resolved_source}"
        )
    resolved = assert_descendant(path, resolved_base, label=label)
    if _path_is_within(resolved, resolved_source):
        raise RuntimeError(
            f"{label} must stay outside source root: "
            f"path={resolved} source={resolved_source}"
        )
    return resolved


def prepare_isolated_environment(
    data_root: Path,
    *,
    output_base: Path = CAPTURE_OUTPUT_BASE,
    source_root: Path = ROOT,
) -> EnvironmentIsolation:
    resolved = assert_external_capture_descendant(
        data_root,
        output_base,
        source_root,
        label="capture data root",
    )
    resolved.mkdir(parents=True, exist_ok=True)
    temp_root = resolved / "temp"
    program_data = resolved / "programdata"
    local_app_data = resolved / "localappdata"
    roaming_app_data = resolved / "appdata"
    user_profile = resolved / "userprofile"
    for path in (
        temp_root,
        program_data,
        local_app_data,
        roaming_app_data,
        user_profile,
    ):
        path.mkdir(parents=True, exist_ok=True)
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
        "PROGRAMDATA": str(program_data),
        "LOCALAPPDATA": str(local_app_data),
        "APPDATA": str(roaming_app_data),
        "USERPROFILE": str(user_profile),
        "COMPUTERNAME": "CAPTURE-DISPLAY2",
    }
    logistics_keys = tuple(
        key
        for key in os.environ
        if key.startswith("LABEL_MATCH_LOGISTICS_")
        or key.startswith("WORKER_ANALYSIS_LOGISTICS_")
    )
    startup_override_keys = (
        "LABEL_MATCH_CAPTURE_STARTUP_GEOMETRY",
        "LABEL_MATCH_CAPTURE_STARTUP_DPI",
    )
    mutated_keys = tuple(
        dict.fromkeys((*guards, *logistics_keys, *startup_override_keys))
    )
    previous = {key: os.environ.get(key) for key in mutated_keys}
    sensitive_values = {
        key: value
        for key, value in previous.items()
        if key
        in {
            "PROGRAMDATA",
            "LOCALAPPDATA",
            "APPDATA",
            "USERPROFILE",
            "COMPUTERNAME",
        }
        and value
        and value != guards.get(key)
    }
    os.environ.update(guards)
    for key in (*logistics_keys, *startup_override_keys):
        os.environ.pop(key, None)
    return EnvironmentIsolation(
        guards=guards,
        previous=previous,
        sensitive_values=sensitive_values,
        removed_keys=tuple((*logistics_keys, *startup_override_keys)),
    )


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


def redact_sensitive_manifest_values(
    value: Any, sensitive_values: Mapping[str, str]
) -> tuple[Any, tuple[str, ...]]:
    """Remove real host environment values before evidence is serialized."""

    replacements = sorted(
        (
            (label, str(secret))
            for label, secret in sensitive_values.items()
            if str(secret).strip()
        ),
        key=lambda item: len(item[1]),
        reverse=True,
    )
    redacted: set[str] = set()

    def redact_text(candidate: str) -> str:
        result = str(candidate)
        for label, secret in replacements:
            start = 0
            lowered = result.casefold()
            secret_lowered = secret.casefold()
            while True:
                index = lowered.find(secret_lowered, start)
                if index < 0:
                    break
                token = f"<redacted:{label}>"
                result = result[:index] + token + result[index + len(secret) :]
                redacted.add(label)
                lowered = result.casefold()
                start = index + len(token)
        return result

    def visit(candidate: Any) -> Any:
        if isinstance(candidate, dict):
            result = {}
            for key, child in candidate.items():
                sanitized_key = redact_text(str(key))
                if sanitized_key in result:
                    raise RuntimeError(
                        "privacy redaction produced a duplicate manifest key"
                    )
                result[sanitized_key] = visit(child)
            return result
        if isinstance(candidate, list):
            return [visit(child) for child in candidate]
        if isinstance(candidate, tuple):
            return [visit(child) for child in candidate]
        if not isinstance(candidate, str):
            return candidate
        return redact_text(candidate)

    sanitized = visit(value)
    serialized = json.dumps(sanitized, ensure_ascii=False).casefold()
    leaked = [
        label
        for label, secret in replacements
        if secret.casefold() in serialized
    ]
    if leaked:
        raise RuntimeError(
            "real host environment values remain in capture manifest: "
            + ",".join(leaked)
        )
    return sanitized, tuple(sorted(redacted))


def minimal_privacy_failure_manifest(error: BaseException) -> dict[str, Any]:
    """Discard all prior evidence when privacy sanitization itself fails."""

    return {
        "schema_version": CAPTURE_MANIFEST_SCHEMA_VERSION,
        "tool": "capture_label_operator_ui",
        "summary": {
            "capture_count": 0,
            "passed_capture_count": 0,
            "failed_capture_count": 0,
            "passed": False,
            "fatal_error": f"privacy_contract_failed:{type(error).__name__}",
        },
        "privacy_contract": {
            "status": "FAIL",
            "original_manifest_discarded": True,
            "real_environment_values_recorded": False,
        },
    }


def record_cleanup_contract(
    manifest: dict[str, Any], cleanup_failures: Sequence[str]
) -> dict[str, Any]:
    """Bind approval eligibility to successful host-state restoration."""

    failures = [str(value) for value in cleanup_failures]
    summary = manifest.setdefault("summary", {})
    if failures:
        summary["passed"] = False
        summary["fatal_error"] = "cleanup_contract_failed"
        cleanup_contract = {
            "status": "FAIL",
            "failures": failures,
        }
    else:
        cleanup_contract = {"status": "PASS"}
    manifest["cleanup_contract"] = cleanup_contract
    manifest["approval_eligible"] = bool(
        not failures
        and manifest.get("matrix_complete") is True
        and summary.get("passed") is True
    )
    return cleanup_contract


def enable_per_monitor_dpi_awareness(*, shcore: Any | None = None) -> dict[str, Any]:
    """Set and independently observe PROCESS_PER_MONITOR_DPI_AWARE (2)."""

    if shcore is None:
        if os.name != "nt":
            raise RuntimeError("per-monitor DPI capture requires Windows")
        shcore = ctypes.windll.shcore
    requested = 2
    set_hresult = int(shcore.SetProcessDpiAwareness(requested))
    observed = ctypes.c_int(-1)
    query_hresult = int(
        shcore.GetProcessDpiAwareness(0, ctypes.byref(observed))
    )
    if query_hresult != 0 or observed.value != requested:
        raise RuntimeError(
            "capture process is not per-monitor DPI aware: "
            f"set_hresult={set_hresult} query_hresult={query_hresult} "
            f"observed={observed.value}"
        )
    return {
        "requested": requested,
        "set_hresult": set_hresult,
        "query_hresult": query_hresult,
        "observed": int(observed.value),
        "status": "PASS",
    }


def pump_tk(root: Any, milliseconds: int = 220) -> None:
    deadline = time.monotonic() + max(0, milliseconds) / 1000.0
    while time.monotonic() < deadline:
        root.update()
        time.sleep(0.012)
    root.update_idletasks()
    root.update()


def _capture_client_with_print_window(
    root: Any,
) -> tuple[Image.Image, str, list[int]]:
    """Return a diagnostic client crop when visible-screen capture fails.

    PrintWindow is intentionally non-authoritative because it can render stale
    or occluded content that was not actually visible on the operator monitor.
    The caller records the result for diagnosis, while the schema-5 capture
    geometry gate prevents it from passing.
    """

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
    client_width = max(1, int(client_rect[2] - client_rect[0]))
    client_height = max(1, int(client_rect[3] - client_rect[1]))
    hwnd_dc = win32gui.GetWindowDC(hwnd)
    mfc_dc = win32ui.CreateDCFromHandle(hwnd_dc)
    save_dc = mfc_dc.CreateCompatibleDC()
    bitmap = win32ui.CreateBitmap()
    bitmap.CreateCompatibleBitmap(mfc_dc, *window_size)
    save_dc.SelectObject(bitmap)
    try:
        rendered = int(
            ctypes.windll.user32.PrintWindow(hwnd, save_dc.GetSafeHdc(), 2)
        )
        if rendered != 1:
            raise RuntimeError(
                "PrintWindow(PW_RENDERFULLCONTENT) diagnostic fallback failed"
            )
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
    if full.size != window_size:
        raise RuntimeError(
            f"PrintWindow bitmap size mismatch: expected={window_size} actual={full.size}"
        )
    crop_left = max(0, int(client_left - left))
    crop_top = max(0, int(client_top - top))
    crop_right = min(full.width, crop_left + client_width)
    crop_bottom = min(full.height, crop_top + client_height)
    image = full.crop((crop_left, crop_top, crop_right, crop_bottom))
    if image.size != (client_width, client_height):
        raise RuntimeError(
            "PrintWindow client crop size mismatch: "
            f"expected={(client_width, client_height)} actual={image.size}"
        )
    return (
        image,
        DIAGNOSTIC_PRINTWINDOW_CAPTURE_SOURCE,
        [
            int(client_left),
            int(client_top),
            int(client_left + client_width),
            int(client_top + client_height),
        ],
    )


def _capture_outer_with_print_window(root: Any) -> tuple[Image.Image, str]:
    """Compatibility wrapper for older tests and capture-tool callers."""

    image, source, _screen_bbox = _capture_client_with_print_window(root)
    return image, source


def capture_tk_client(
    root: Any,
    *,
    pump_events: bool = True,
) -> tuple[Image.Image, str, list[int]]:
    """Capture the real, visible Tk client area on the target monitor."""

    if pump_events:
        root.update_idletasks()
        root.update()
        pending = _pending_after_ids(root)
        if pending:
            raise RuntimeError(
                "scheduled jobs appeared after pre-capture full update: "
                f"{pending}"
            )
    left = int(root.winfo_rootx())
    top = int(root.winfo_rooty())
    width = max(1, int(root.winfo_width()))
    height = max(1, int(root.winfo_height()))
    screen_bbox = [left, top, left + width, top + height]
    try:
        image = ImageGrab.grab(
            bbox=tuple(screen_bbox),
            all_screens=True,
        )
        return image, AUTHORITATIVE_CAPTURE_SOURCE, screen_bbox
    except Exception as exc:
        visible_capture_failure = (
            f"ImageGrab failed: {type(exc).__name__}: {exc}"
        )
    if os.name == "nt":
        image, diagnostic_source, diagnostic_bbox = (
            _capture_client_with_print_window(root)
        )
        return (
            image,
            f"{diagnostic_source}; {visible_capture_failure}",
            diagnostic_bbox,
        )
    raise RuntimeError(visible_capture_failure)


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


def _edge_true_run(values: Sequence[bool]) -> int:
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


def analyze_image(
    image: Image.Image,
    expected_size: tuple[int, int],
    *,
    content_bbox: Sequence[int] | None = None,
) -> dict[str, Any]:
    rgb = image.convert("RGB")
    if content_bbox is None:
        analysis_bbox = (0, 0, rgb.width, rgb.height)
        analysis_region = "full_outer_window"
    else:
        if len(content_bbox) != 4:
            raise ValueError("content_bbox must contain left, top, right, bottom")
        analysis_bbox = tuple(map(int, content_bbox))
        left, top, right, bottom = analysis_bbox
        if (
            left < 0
            or top < 0
            or right <= left
            or bottom <= top
            or right > rgb.width
            or bottom > rgb.height
        ):
            raise ValueError(
                "content_bbox must be a non-empty rectangle inside the outer image"
            )
        analysis_region = "window_client"
    analysis_rgb = rgb.crop(analysis_bbox)
    gray = analysis_rgb.convert("L")
    histogram = gray.histogram()
    pixels = max(1, analysis_rgb.width * analysis_rgb.height)
    extrema = gray.getextrema() or (0, 0)
    stat = ImageStat.Stat(gray)
    sample = analysis_rgb.copy()
    sample.thumbnail((256, 256))
    colors = sample.getcolors(maxcolors=max(1, sample.width * sample.height)) or []
    dominant_ratio = max((count for count, _ in colors), default=0) / max(
        1, sample.width * sample.height
    )
    exact_black_ratio = histogram[0] / pixels
    near_black_ratio = sum(histogram[: NEAR_BLACK_LUMA + 1]) / pixels
    mask = gray.point(
        [255 if value <= NEAR_BLACK_LUMA else 0 for value in range(256)]
    )
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
    tile_ratios = []
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
    luma_stddev = float(stat.stddev[0])
    blank = bool(extrema[0] == extrema[1])
    excess_black = near_black_ratio > NEAR_BLACK_FAILURE_RATIO
    edge_black = bool(
        edge_row_ratio >= BLACK_STRIPE_FAILURE_RATIO
        or edge_column_ratio >= BLACK_STRIPE_FAILURE_RATIO
        or maximum_edge_band_ratio >= BLACK_EDGE_BAND_FAILURE_RATIO
    )
    contiguous_black = bool(
        longest_row_ratio >= BLACK_STRIPE_FAILURE_RATIO
        or longest_column_ratio >= BLACK_STRIPE_FAILURE_RATIO
    )
    black_tile = maximum_tile_ratio >= BLACK_TILE_FAILURE_RATIO
    uniform = bool(
        luma_stddev <= LOW_VARIANCE_STDDEV_MAX
        or dominant_ratio >= DOMINANT_COLOR_RATIO_MAX
    )
    pixel_size_matches = (rgb.width, rgb.height) == expected_size
    capture_pixels_valid = bool(
        pixel_size_matches
        and not blank
        and not excess_black
        and not edge_black
        and not contiguous_black
        and not black_tile
        and not uniform
    )
    return {
        "expected_pixel_size": list(expected_size),
        "pixel_size": [rgb.width, rgb.height],
        "pixel_size_matches": pixel_size_matches,
        "analysis_region": analysis_region,
        "analysis_bbox": list(analysis_bbox),
        "analysis_pixel_size": [analysis_rgb.width, analysis_rgb.height],
        "capture_pixels_valid": capture_pixels_valid,
        "exact_black_ratio": round(exact_black_ratio, 6),
        "near_black_ratio": round(near_black_ratio, 6),
        "near_black_threshold_luma": NEAR_BLACK_LUMA,
        "near_black_failure_ratio": NEAR_BLACK_FAILURE_RATIO,
        "blank_suspected": blank,
        "excess_black_suspected": excess_black,
        "edge_black_stripe_suspected": edge_black,
        "contiguous_black_stripe_suspected": contiguous_black,
        "black_tile_suspected": black_tile,
        "uniform_low_variance_suspected": uniform,
        "luma_mean": round(float(stat.mean[0]), 3),
        "luma_stddev": round(luma_stddev, 3),
        "low_variance_stddev_threshold": LOW_VARIANCE_STDDEV_MAX,
        "dominant_color_ratio_sampled": round(dominant_ratio, 6),
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
        "parsed": list(fixture_parsed_scans(fixture)),
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


class _CaptureCancellationConflictOutbox:
    """Read-only conflict source used only by the isolated capture process."""

    def __init__(self, rows: Sequence[Mapping[str, Any]]):
        self._rows = tuple(dict(row) for row in rows)

    def list_conflicts(self, *, limit: int = 20) -> list[dict[str, Any]]:
        return [dict(row) for row in self._rows[: max(0, int(limit))]]


def _fixture_cancellation_conflict_rows(
    fixture: StateFixture,
) -> tuple[dict[str, Any], ...]:
    if fixture.state_id != "cancellation_conflict":
        return ()
    return tuple(
        {
            "last_error_code": CANCELLATION_CONFLICT_CODE,
            "package_bundle_id": (
                CANCELLATION_CONFLICT_BUNDLE_ID
                if index == 0
                else f"{CANCELLATION_CONFLICT_BUNDLE_ID}-{index + 1:03d}"
            ),
        }
        for index in range(CANCELLATION_CONFLICT_COUNT)
    )


def _fixture_activity_history_rows(app: Any) -> tuple[dict[str, Any], ...]:
    """Return two display-only completed records for right-pane evidence."""

    return (
        {
            "iid": "capture-activity-001",
            "values": (
                "1",
                CAPTURE_ITEM_CODE,
                f"{CAPTURE_ITEM_CODE} · S/N 260716000002",
                f"{CAPTURE_ITEM_CODE} · S/N 260716000003",
                f"{CAPTURE_ITEM_CODE} · S/N 260716000004",
                f"{CAPTURE_ITEM_CODE} · S/N 260716000005",
                app.Results.PASS,
                "19:41:03",
            ),
            "tags": ("success",),
        },
        {
            "iid": "capture-activity-002",
            "values": (
                "2",
                CAPTURE_SECONDARY_ITEM_CODE,
                f"{CAPTURE_SECONDARY_ITEM_CODE} · S/N 260716000102",
                f"{CAPTURE_SECONDARY_ITEM_CODE} · S/N 260716000103",
                f"{CAPTURE_SECONDARY_ITEM_CODE} · S/N 260716000104",
                f"{CAPTURE_SECONDARY_ITEM_CODE} · S/N 260716000105",
                app.Results.FAIL_MISMATCH,
                "19:42:17",
            ),
            "tags": ("error",),
        },
    )


def _apply_activity_fixture_rows(app: Any) -> None:
    """Seed only live widgets, then exercise the real session-row renderer."""

    history_tree = getattr(app, "history_tree", None)
    session_tree = getattr(app, "session_tree", None)
    if history_tree is None and session_tree is None:
        # Presenter-only protocol tests intentionally have no live widgets.
        return
    if history_tree is None or session_tree is None:
        raise RuntimeError("activity fixture requires both history and session trees")
    formatter = getattr(app, "_history_values_for_display", None)
    refresh_session = getattr(app, "_refresh_session_tree", None)
    if not callable(formatter) or not callable(refresh_session):
        raise RuntimeError("activity fixture requires real history/session renderers")

    history_children = tuple(history_tree.get_children(""))
    if history_children:
        history_tree.delete(*history_children)
    session_children = tuple(session_tree.get_children(""))
    if session_children:
        session_tree.delete(*session_children)

    rows = _fixture_activity_history_rows(app)
    expected_session_values: list[tuple[str, str, str]] = []
    for row in rows:
        displayed_values = tuple(formatter(row["values"]))
        if len(displayed_values) != 8:
            raise RuntimeError("activity history formatter changed its eight-value contract")
        history_tree.insert(
            "",
            "end",
            iid=row["iid"],
            values=displayed_values,
            tags=row["tags"],
        )
        expected_session_values.append(
            (displayed_values[7], displayed_values[1], displayed_values[6])
        )

    refresh_session()
    actual_history = tuple(history_tree.get_children(""))
    actual_session = tuple(session_tree.get_children(""))
    expected_history = tuple(row["iid"] for row in rows)
    expected_session = tuple(f"session-{iid}" for iid in expected_history)
    if actual_history != expected_history or actual_session != expected_session:
        raise RuntimeError(
            "activity fixture renderer changed row identity/order: "
            f"history={actual_history} session={actual_session}"
        )
    for iid, expected_values in zip(actual_session, expected_session_values):
        values = tuple(session_tree.item(iid, "values") or ())
        if values != expected_values:
            raise RuntimeError(
                f"activity session renderer changed values: {iid}={values}"
            )


def _select_qa_detail_for_fixture(app: Any, fixture: StateFixture) -> None:
    index = int(fixture.selected_qa_index or 0)
    if index <= 0:
        return
    iid = f"qa-slot-{index}"
    tree = getattr(app, "__dict__", {}).get("current_set_tree")
    if tree is None:
        # Protocol-only unit fakes do not carry the live widget contract. The
        # real capture path validates that contract before applying fixtures.
        return
    try:
        if hasattr(tree, "exists") and not tree.exists(iid):
            raise RuntimeError(f"selected QA fixture row is missing: {iid}")
        tree.selection_set(iid)
        tree.focus(iid)
        tree.see(iid)
        renderer = getattr(app, "_render_qa_scan_detail", None)
        if callable(renderer):
            renderer(iid)
        else:
            getattr(app, "_on_qa_scan_selection_changed")()
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(f"selected QA fixture could not select {iid}") from exc


def apply_state_fixture(app: Any, fixture: StateFixture) -> tuple[Any, str]:
    """Apply display-only state and ask the application to render its presenter."""

    from ui.workflow_view_state import WorkflowNotice

    current = dict(getattr(app, "current_set_info", {}) or {})
    current.update(
        {
            "id": f"capture-{fixture.state_id}",
            "raw": list(fixture.qa_scans),
            "parsed": list(fixture_parsed_scans(fixture)),
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
    app._workflow_display_parsed_scans = fixture_parsed_scans(fixture)
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
    conflict_rows = _fixture_cancellation_conflict_rows(fixture)
    app.package_cancellation_outbox = _CaptureCancellationConflictOutbox(
        conflict_rows
    )
    review_refresh = getattr(
        app, "_refresh_package_cancellation_review_notice", None
    )
    if callable(review_refresh):
        observed_count = int(review_refresh())
        if observed_count != len(conflict_rows):
            raise RuntimeError(
                "cancellation conflict fixture count did not reach the renderer"
            )
    elif conflict_rows:
        app._package_cancellation_review_notice = WorkflowNotice(
            CANCELLATION_CONFLICT_TITLE,
            CANCELLATION_CONFLICT_MESSAGE,
            kind="package_cancellation_review",
            tone="danger",
        )
        app._package_cancellation_review_rows = conflict_rows
    else:
        app._package_cancellation_review_notice = None
        app._package_cancellation_review_rows = ()
    view = build_presenter_view(fixture)
    # Capture-only mirrors let a renderer with an explicit view parameter and
    # a renderer that rebuilds from runtime state share the same harness.
    app._workflow_view_state = view
    app._last_workflow_view_state = view
    method_name = _invoke_presenter_refresh(app, view)
    _apply_activity_fixture_rows(app)
    _select_qa_detail_for_fixture(app, fixture)
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
    try:
        widget_class = str(widget.winfo_class())
    except Exception:
        widget_class = ""
    try:
        text = str(widget.cget("text") or "")
    except Exception:
        text = ""
    try:
        wraplength = int(float(widget.cget("wraplength") or 0))
    except Exception:
        wraplength = 0
    explicit_text_dimensions = []
    for option in ("width", "height"):
        try:
            configured = widget.cget(option)
            if str(configured).strip() and float(configured) != 0:
                explicit_text_dimensions.append(option)
        except Exception:
            continue
    try:
        (
            text_line_pixel_widths,
            text_line_height,
            text_measurement_source,
        ) = _tk_font_line_metrics_with_source(widget, text)
        text_pixel_width = max(text_line_pixel_widths, default=0)
    except Exception:
        text_line_pixel_widths = (requested[0],)
        text_pixel_width, text_line_height = requested[0], requested[1]
        text_measurement_source = "headless-approximation"
    text_horizontal_inset = {
        "Button": 8,
        "TButton": 8,
        # ttk labels request four pixels more than the measured text on the
        # Windows theme.  Treating that natural border as eight pixels made
        # every correctly sized label look four pixels clipped.
        "Label": 8,
        "TLabel": 4,
    }.get(widget_class, 0)
    text_available_width = max(1, width - text_horizontal_inset)
    return {
        "name": name,
        "path": str(widget),
        "master_path": str(getattr(widget, "master", "")),
        "mapped": _is_mapped(widget),
        "critical": critical,
        "bbox": [x, y, x + width, y + height],
        "size": [width, height],
        "requested_size": requested,
        "widget_class": widget_class,
        "text": text,
        "text_explicit_line_count": max(1, len(text.splitlines())),
        "wraplength": wraplength,
        "text_pixel_width": int(text_pixel_width),
        "text_line_pixel_widths": [
            int(value) for value in text_line_pixel_widths
        ],
        "text_line_height": int(text_line_height),
        "text_measurement_source": str(text_measurement_source),
        "text_explicit_dimensions": explicit_text_dimensions,
        "text_natural_geometry_authoritative": bool(
            widget_class in {"Label", "TLabel"}
            and not explicit_text_dimensions
        ),
        "text_horizontal_inset": text_horizontal_inset,
        "text_available_width": text_available_width,
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


def evaluate_tree_detail_partition(
    tree_record: Mapping[str, Any],
    detail_record: Mapping[str, Any],
    frame_record: Mapping[str, Any],
) -> dict[str, Any]:
    mapped = bool(
        tree_record.get("mapped")
        and detail_record.get("mapped")
        and frame_record.get("mapped")
    )
    if not mapped:
        return {"mapped": False, "issues": [], "passed": True}
    issues = []
    if not _inside(tree_record["bbox"], frame_record["bbox"]):
        issues.append("tree_outside_active_frame")
    if not _inside(detail_record["bbox"], frame_record["bbox"]):
        issues.append("detail_outside_active_frame")
    if _boxes_overlap(tree_record["bbox"], detail_record["bbox"]):
        issues.append("tree_detail_overlap")
    if int(tree_record["bbox"][3]) > int(detail_record["bbox"][1]) + 1:
        issues.append("tree_detail_vertical_order_invalid")
    return {
        "mapped": True,
        "tree_bbox": list(tree_record["bbox"]),
        "detail_bbox": list(detail_record["bbox"]),
        "frame_bbox": list(frame_record["bbox"]),
        "issues": issues,
        "passed": not issues,
    }


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
        if (
            record.get("check_requested_width")
            and int(requested[0]) > int(actual[0]) + 2
        ):
            widget_class = str(record.get("widget_class") or "")
            text = str(record.get("text") or "").strip()
            wraplength = int(record.get("wraplength") or 0)
            text_width = int(record.get("text_pixel_width") or 0)
            text_available_width = int(
                record.get("text_available_width")
                or max(
                    1,
                    int(actual[0])
                    - (
                        8
                        if widget_class
                        in {"Button", "Label", "TButton", "TLabel"}
                        else 0
                    ),
                )
            )
            safe_text_control = widget_class in {
                "Button",
                "Label",
                "TButton",
                "TLabel",
            }
            safe_unwrapped_fit = (
                wraplength <= 0 and text_width <= text_available_width
            )
            safe_wrapped_fit = 0 < wraplength <= text_available_width
            if not (
                safe_text_control
                and text
                and (safe_unwrapped_fit or safe_wrapped_fit)
            ):
                # Only labels/buttons may intentionally use a smaller compact
                # slot when their measured text still fits.  Structural and
                # input controls remain fail-closed on requested compression.
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


def evaluate_text_clipping_proxy(
    records: Sequence[Mapping[str, Any]], *, tolerance: int = 2
) -> dict[str, Any]:
    width_compressed: list[str] = []
    height_compressed: list[str] = []
    wrap_exceeds_widget: list[str] = []
    non_authoritative: list[str] = []
    for record in records:
        if not record.get("mapped") or not str(record.get("text") or "").strip():
            continue
        name = str(record.get("name") or "")
        actual = list(record.get("size") or (0, 0))
        requested = list(record.get("requested_size") or actual)
        if len(actual) != 2 or len(requested) != 2:
            width_compressed.append(name)
            height_compressed.append(name)
            continue
        wraplength = int(record.get("wraplength") or 0)
        text_pixel_width = int(record.get("text_pixel_width", requested[0]) or 0)
        widget_class = str(record.get("widget_class") or "")
        text_available_width = int(
            record.get("text_available_width")
            or max(
                1,
                int(actual[0])
                - (
                    8
                    if widget_class in {"Button", "Label", "TButton"}
                    else 4
                    if widget_class == "TLabel"
                    else 0
                ),
            )
        )
        line_pixel_widths = tuple(
            int(value)
            for value in (
                record.get("text_line_pixel_widths") or (text_pixel_width,)
            )
        )
        if str(record.get("text_measurement_source") or "") != "tk":
            non_authoritative.append(name)
        natural_width_fits = int(requested[0]) <= int(actual[0]) + tolerance
        natural_height_fits = int(requested[1]) <= int(actual[1]) + tolerance
        natural_geometry_is_authoritative = bool(
            record.get("text_natural_geometry_authoritative")
        )
        if (
            not (
                natural_geometry_is_authoritative
                and natural_width_fits
                and natural_height_fits
            )
            and wraplength > text_available_width
            and any(
                width > text_available_width
                for width in line_pixel_widths
            )
        ):
            # Tk wraps at ``wraplength``, not at the final grid-compressed
            # widget width.  If the configured wrap boundary is wider than
            # the widget and even one explicit line is wider than the widget,
            # horizontal clipping is possible regardless of requested height.
            wrap_exceeds_widget.append(name)
        if (
            wraplength <= 0
            and text_pixel_width > text_available_width
        ):
            width_compressed.append(name)
        if int(requested[1]) > int(actual[1]) + tolerance:
            height_compressed.append(name)
    issue_count = (
        len(width_compressed)
        + len(height_compressed)
        + len(wrap_exceeds_widget)
        + len(non_authoritative)
    )
    return {
        "width_compressed_text_widgets": width_compressed,
        "height_compressed_text_widgets": height_compressed,
        "wraplength_exceeds_widget": wrap_exceeds_widget,
        "non_authoritative_text_measurements": non_authoritative,
        "issue_count": issue_count,
        "suspected": bool(issue_count),
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


def evaluate_tree_text_fit_proxy(
    records: Sequence[Mapping[str, Any]], *, tolerance: int = 2
) -> dict[str, Any]:
    invisible_cells: list[str] = []
    overflowing_fixed_text: list[str] = []
    short_rows: list[str] = []
    non_authoritative: list[str] = []
    for record in records:
        name = str(record.get("name") or "")
        if record.get("measurement_applicable") is False:
            continue
        if record.get("visible") is not True:
            invisible_cells.append(name)
            continue
        width = int(record.get("width") or 0)
        height = int(record.get("height") or 0)
        text_width = int(record.get("text_width") or 0)
        line_height = int(record.get("line_height") or 0)
        if (
            record.get("text_nonblank")
            and str(record.get("measurement_source") or "") != "tk"
        ):
            non_authoritative.append(name)
        available_width = int(
            record.get("available_text_width")
            if record.get("available_text_width") is not None
            else width - TREE_DATA_CELL_GUTTER_PX
        )
        if not record.get("allow_overflow") and text_width > available_width + tolerance:
            overflowing_fixed_text.append(name)
        if line_height and line_height > height - 2 + tolerance:
            short_rows.append(name)
    issue_count = (
        len(invisible_cells)
        + len(overflowing_fixed_text)
        + len(short_rows)
        + len(non_authoritative)
    )
    return {
        "invisible_cells": invisible_cells,
        "overflowing_fixed_text": overflowing_fixed_text,
        "short_rows": short_rows,
        "non_authoritative_text_measurements": non_authoritative,
        "issue_count": issue_count,
        "suspected": bool(issue_count),
    }


def evaluate_middle_ellipsis_fit(
    raw: str,
    displayed: str,
    *,
    measured_width: int,
    available_width: int,
    marker: str = "...",
) -> list[str]:
    """Validate loss-signalled scan display while preserving raw endpoints."""

    raw_text = str(raw or "")
    display_text = str(displayed or "")
    issues: list[str] = []
    if int(measured_width) > int(available_width):
        issues.append("display_text_exceeds_value_column")
    if not raw_text:
        if display_text not in {"", "-"}:
            issues.append("empty_raw_has_nonempty_display")
        return issues
    if display_text == raw_text:
        return issues
    if display_text.count(marker) != 1:
        issues.append("middle_ellipsis_marker_missing_or_duplicated")
        return issues
    prefix, suffix = display_text.split(marker, 1)
    if not prefix or not raw_text.startswith(prefix):
        issues.append("middle_ellipsis_start_not_preserved")
    if not suffix or not raw_text.endswith(suffix):
        issues.append("middle_ellipsis_end_not_preserved")
    if len(display_text) >= len(raw_text):
        issues.append("middle_ellipsis_did_not_shorten_value")
    return issues


def validate_qa_detail_contract(
    expected_raws: Sequence[str],
    detail_rows: Mapping[str, Mapping[str, Any]],
    selected_texts: Mapping[str, str],
) -> list[str]:
    """Require every accepted QA raw value in mapping and selected Text."""

    issues: list[str] = []
    for index, expected in enumerate(expected_raws, 1):
        iid = f"qa-slot-{index}"
        detail = detail_rows.get(iid)
        if detail is None:
            issues.append(f"qa_detail_{index}_mapping_missing")
            continue
        if str(detail.get("raw") or "") != str(expected):
            issues.append(f"qa_detail_{index}_raw_parity_mismatch")
        if str(selected_texts.get(iid, "")) != str(expected):
            issues.append(f"qa_detail_{index}_selected_text_mismatch")
    return issues


def _text_widget_value(widget: Any) -> str:
    try:
        return str(widget.get("1.0", "end-1c"))
    except Exception:
        try:
            return str(widget.get("1.0", "end")).rstrip("\n")
        except Exception:
            return ""


def collect_qa_detail_contract(
    app: Any, fixture: StateFixture, view: Any
) -> dict[str, Any]:
    tree = getattr(app, "current_set_tree")
    detail_widget = getattr(app, "qa_scan_detail_text")
    detail_rows = dict(getattr(app, "_qa_scan_detail_rows", {}) or {})
    expected_raws = tuple(str(value) for value in fixture.qa_scans)
    try:
        previous_selection = tuple(tree.selection())
    except Exception:
        previous_selection = ()
    selected_texts: dict[str, str] = {}
    for index in range(1, len(expected_raws) + 1):
        iid = f"qa-slot-{index}"
        try:
            tree.selection_set(iid)
            renderer = getattr(app, "_render_qa_scan_detail", None)
            if callable(renderer):
                renderer(iid)
            else:
                getattr(app, "_on_qa_scan_selection_changed")()
            app.update_idletasks()
            selected_texts[iid] = _text_widget_value(detail_widget)
        except Exception as exc:
            selected_texts[iid] = f"<capture-error:{type(exc).__name__}>"
    selected_iid = None
    try:
        if previous_selection:
            tree.selection_set(previous_selection)
            selected_iid = str(previous_selection[0])
        else:
            tree.selection_remove(tree.selection())
            selected_iid = None
        renderer = getattr(app, "_render_qa_scan_detail", None)
        if callable(renderer):
            renderer(selected_iid)
    except Exception:
        pass
    issues = validate_qa_detail_contract(
        expected_raws,
        detail_rows,
        selected_texts,
    )
    all_presenter_values = tuple(
        str(row.get("value") or "") for row in expected_presenter_rows(view)
    )
    presenter_values = all_presenter_values[: len(expected_raws)]
    if presenter_values != expected_raws:
        issues.append("qa_fixture_presenter_raw_parity_mismatch")
    for index, expected in enumerate(all_presenter_values, 1):
        detail = detail_rows.get(f"qa-slot-{index}")
        if detail is None:
            issues.append(f"qa_detail_{index}_slot_mapping_missing")
        elif str(detail.get("raw") or "") != expected:
            issues.append(f"qa_detail_{index}_slot_raw_parity_mismatch")
    selected_raw = str(
        (detail_rows.get(str(selected_iid)) or {}).get("raw") or ""
    )
    selected_text = _text_widget_value(detail_widget)
    return {
        "expected_raws": list(expected_raws),
        "detail_rows": {
            str(key): {
                "stage": str(value.get("stage") or ""),
                "state": str(value.get("state") or ""),
                "raw": str(value.get("raw") or ""),
            }
            for key, value in detail_rows.items()
        },
        "selected_texts": selected_texts,
        "selected_iid": selected_iid,
        "selected_raw": selected_raw,
        "selected_detail_text": selected_text,
        "issues": list(dict.fromkeys(issues)),
        "passed": not issues,
    }


def collect_exact_detail_contract(
    app: Any, fixture: StateFixture
) -> dict[str, Any]:
    tree = getattr(app, "exact_rescan_tree")
    required = (
        "exact_rescan_detail_frame",
        "exact_rescan_detail_text",
        "exact_rescan_detail_metadata_label",
        "exact_rescan_detail_scrollbar",
    )
    missing = [name for name in required if getattr(app, name, None) is None]
    if missing:
        return {
            "available": False,
            "missing_widgets": missing,
            "issues": [f"missing_widget:{name}" for name in missing],
            "passed": False,
        }
    detail_widget = getattr(app, "exact_rescan_detail_text")
    expected_raws = tuple(str(value) for value in fixture.exact_barcodes)
    mapping = dict(getattr(app, "_exact_rescan_detail_rows", {}) or {})
    try:
        children = tuple(str(iid) for iid in tree.get_children(""))
        previous_selection = tuple(tree.selection())
    except Exception:
        children = ()
        previous_selection = ()
    selected_texts: dict[str, str] = {}
    issues: list[str] = []
    renderer = getattr(app, "_render_exact_rescan_detail", None)
    for index, expected in enumerate(expected_raws, 1):
        iid = children[index - 1] if index <= len(children) else ""
        if not iid:
            issues.append(f"exact_detail_{index}_tree_row_missing")
            continue
        try:
            tree.selection_set(iid)
            if callable(renderer):
                renderer(iid)
            else:
                callback = getattr(app, "_on_exact_rescan_selection_changed", None)
                if callable(callback):
                    callback()
            app.update_idletasks()
            selected_texts[iid] = _text_widget_value(detail_widget)
        except Exception as exc:
            selected_texts[iid] = f"<capture-error:{type(exc).__name__}>"
        detail = mapping.get(iid)
        if detail is None:
            issues.append(f"exact_detail_{index}_mapping_missing")
        elif str(detail.get("raw") or "") != expected:
            issues.append(f"exact_detail_{index}_raw_parity_mismatch")
        if selected_texts.get(iid) != expected:
            issues.append(f"exact_detail_{index}_selected_text_mismatch")
    try:
        if previous_selection:
            tree.selection_set(previous_selection)
            if callable(renderer):
                renderer(str(previous_selection[0]))
        else:
            tree.selection_remove(tree.selection())
            if callable(renderer):
                renderer(None)
    except Exception:
        pass
    return {
        "available": True,
        "expected_raws": list(expected_raws),
        "mapping_present": bool(mapping),
        "detail_rows": {
            str(key): {
                "order": int(value.get("order") or 0),
                "raw": str(value.get("raw") or ""),
            }
            for key, value in mapping.items()
        },
        "selected_texts": selected_texts,
        "issues": list(dict.fromkeys(issues)),
        "passed": not issues,
    }


def collect_scan_display_contract(
    tree: Any,
    raw_values: Sequence[str],
    *,
    value_column: str,
    expected_display_values: Sequence[str],
    iid_prefix: str | None = None,
    padding: int = 20,
    empty_display: str = "",
    allow_headless_approximation: bool = False,
    viewport_width: int | None = None,
    display_source_values: Sequence[str] | None = None,
    required_prefix_values: Sequence[str] | None = None,
) -> dict[str, Any]:
    try:
        children = tuple(str(iid) for iid in tree.get_children(""))
    except Exception:
        children = ()
    try:
        columns = tuple(str(value) for value in tree.cget("columns"))
        value_index = columns.index(str(value_column))
        available = max(
            1,
            effective_tree_column_width(
                tree,
                value_column,
                viewport_width=viewport_width,
            )
            - int(padding),
        )
    except Exception:
        value_index = 0
        available = 0
    rows: list[dict[str, Any]] = []
    aggregate_issues: list[str] = []
    if len(expected_display_values) != len(raw_values):
        aggregate_issues.append(
            "expected_display_value_count_mismatch:"
            f"{len(expected_display_values)}!={len(raw_values)}"
        )
    display_sources = tuple(
        str(value) for value in (
            raw_values if display_source_values is None else display_source_values
        )
    )
    if len(display_sources) != len(raw_values):
        aggregate_issues.append(
            "display_source_value_count_mismatch:"
            f"{len(display_sources)}!={len(raw_values)}"
        )
    required_prefixes = tuple(
        str(value) for value in (required_prefix_values or ())
    )
    if required_prefixes and len(required_prefixes) != len(raw_values):
        aggregate_issues.append(
            "required_prefix_count_mismatch:"
            f"{len(required_prefixes)}!={len(raw_values)}"
        )
    for index, raw in enumerate(raw_values, 1):
        iid = f"{iid_prefix}{index}" if iid_prefix is not None else (
            children[index - 1] if index <= len(children) else ""
        )
        try:
            values = tuple(tree.item(iid, "values") or ())
            displayed = str(values[value_index] if value_index < len(values) else "")
        except Exception:
            displayed = ""
        measured, _line_height, measurement_source = _tk_font_metrics_with_source(
            tree, displayed
        )
        expected_display = str(
            expected_display_values[index - 1]
            if index <= len(expected_display_values)
            else ""
        )
        display_source = str(
            display_sources[index - 1]
            if index <= len(display_sources)
            else ""
        )
        row_issues = evaluate_middle_ellipsis_fit(
            display_source,
            displayed,
            measured_width=measured,
            available_width=available,
        )
        if not str(raw) and displayed == str(empty_display):
            row_issues = [
                issue
                for issue in row_issues
                if issue != "empty_raw_has_nonempty_display"
            ]
        if displayed != expected_display:
            row_issues.append("pixel_fitted_display_value_mismatch")
        required_prefix = str(
            required_prefixes[index - 1]
            if index <= len(required_prefixes)
            else ""
        )
        if required_prefix and not displayed.startswith(required_prefix):
            row_issues.append("item_code_not_preserved_in_display")
        if measurement_source != "tk" and not allow_headless_approximation:
            row_issues.append("non_authoritative_text_measurement")
        if len(str(raw)) >= 160 and displayed == str(raw):
            row_issues.append("long_fixture_was_not_middle_ellipsized")
        rows.append(
            {
                "index": index,
                "iid": iid,
                "raw": str(raw),
                "display_source": display_source,
                "required_prefix": required_prefix,
                "displayed": displayed,
                "expected_displayed": expected_display,
                "measured_width": measured,
                "available_width": available,
                "measurement_source": measurement_source,
                "issues": row_issues,
            }
        )
        aggregate_issues.extend(f"row_{index}:{issue}" for issue in row_issues)
    if len(children) < len(raw_values):
        aggregate_issues.append(
            f"rendered_row_count_too_small:{len(children)}<{len(raw_values)}"
        )
    return {
        "value_column": str(value_column),
        "rows": rows,
        "issues": list(dict.fromkeys(aggregate_issues)),
        "passed": not aggregate_issues,
    }


def effective_tree_column_width(
    tree: Any,
    column: str,
    *,
    viewport_width: int | None = None,
) -> int:
    """Mirror the app's live stretch-width calculation for pixel contracts."""

    configured_width = max(1, int(tree.column(column, "width")))
    try:
        stretch = bool(tree.column(column, "stretch"))
    except Exception:
        stretch = False
    if not stretch:
        return configured_width
    try:
        columns = tuple(str(value) for value in tree.cget("columns"))
        occupied = sum(
            max(0, int(tree.column(name, "width")))
            for name in columns
            if name != str(column)
        )
        shared_viewport_width = int(viewport_width or 0)
        # A mapped sibling is authoritative for a hidden notebook page.  Its
        # own realized width can be a stale value from the previous wide
        # profile, so taking max() here would silently weaken compact checks.
        realized_width = (
            shared_viewport_width
            if shared_viewport_width > 1
            else int(tree.winfo_width())
        )
        stretched_width = max(1, realized_width - occupied)
    except Exception:
        return configured_width
    return max(configured_width, stretched_width)


def build_last_normal_scan_contract(
    fixture: StateFixture,
    current_rows: Sequence[Mapping[str, Any]],
    exact_rows: Sequence[Mapping[str, Any]],
    qa_detail_contract: Mapping[str, Any],
    exact_detail_contract: Mapping[str, Any],
    expected_qa_display_values: Sequence[str],
    expected_exact_display_values: Sequence[str],
) -> dict[str, Any]:
    target = str(fixture.last_normal_scan or "")
    if not target:
        return {"required": False, "passed": True}
    qa_indices = [
        index
        for index, raw in enumerate(fixture.qa_scans, 1)
        if str(raw) == target
    ]
    exact_indices = [
        index
        for index, raw in enumerate(fixture.exact_barcodes, 1)
        if str(raw) == target
    ]
    source_count = len(qa_indices) + len(exact_indices)
    raw_detail_matches = 0
    selected_text_matches = 0
    fitted_cell_matches = 0
    source = ""
    source_index = 0
    if source_count == 1 and qa_indices:
        source = "qa"
        source_index = qa_indices[0]
        iid = f"qa-slot-{source_index}"
        detail = (qa_detail_contract.get("detail_rows") or {}).get(iid) or {}
        raw_detail_matches = int(str(detail.get("raw") or "") == target)
        selected = (qa_detail_contract.get("selected_texts") or {}).get(iid)
        selected_text_matches = int(str(selected or "") == target)
        expected_display = str(expected_qa_display_values[source_index - 1])
        values = [
            str(value)
            for value in current_rows[source_index - 1].get("values", ())
        ] if source_index <= len(current_rows) else []
        fitted_cell_matches = int(expected_display in values)
    elif source_count == 1 and exact_indices:
        source = "f4"
        source_index = exact_indices[0]
        iid = f"exact-slot-{source_index}"
        detail_rows = exact_detail_contract.get("detail_rows", {}) or {}
        detail = detail_rows.get(iid) or {}
        raw_detail_matches = int(str(detail.get("raw") or "") == target)
        selected = (exact_detail_contract.get("selected_texts") or {}).get(iid)
        selected_text_matches = int(str(selected or "") == target)
        expected_display = str(expected_exact_display_values[source_index - 1])
        values = [
            str(value)
            for value in exact_rows[source_index - 1].get("values", ())
        ] if source_index <= len(exact_rows) else []
        fitted_cell_matches = int(expected_display in values)
    issues = []
    if source_count != 1:
        issues.append(f"last_normal_fixture_source_count:{source_count}!=1")
    if raw_detail_matches != 1:
        issues.append("last_normal_raw_detail_exact_mismatch")
    if selected_text_matches != 1:
        issues.append("last_normal_selected_text_exact_mismatch")
    if fitted_cell_matches != 1:
        issues.append("last_normal_fitted_cell_mismatch")
    return {
        "required": True,
        "source": source,
        "source_index": source_index,
        "fixture_source_count": source_count,
        "raw_detail_exact_count": raw_detail_matches,
        "selected_text_exact_count": selected_text_matches,
        "fitted_cell_exact_count": fitted_cell_matches,
        "issues": issues,
        "passed": not issues,
    }


def expected_scan_display_values(
    app: Any,
    tree: Any,
    raw_values: Sequence[str],
    *,
    value_column: str,
    empty_display: str = "",
) -> tuple[str, ...]:
    fitter = getattr(app, "_fit_operator_tree_cell_text", None)
    if not callable(fitter):
        raise RuntimeError("pixel-fitted tree display method is unavailable")
    result = []
    for raw in raw_values:
        source = str(raw) if str(raw) else str(empty_display)
        result.append(str(fitter(tree, value_column, source)))
    return tuple(result)


def expected_operator_scan_summaries(
    app: Any,
    raw_values: Sequence[str],
    parsed_values: Sequence[str],
    scan_positions: Sequence[int],
) -> tuple[str, ...]:
    summarizer = getattr(app, "_operator_scan_summary", None)
    if not callable(summarizer):
        raise RuntimeError("operator scan summary method is unavailable")
    if not (
        len(raw_values) == len(parsed_values) == len(scan_positions)
    ):
        raise RuntimeError("operator scan summary inputs must have equal lengths")
    return tuple(
        str(summarizer(raw, parsed, position))
        for raw, parsed, position in zip(
            raw_values,
            parsed_values,
            scan_positions,
        )
    )


def validate_operator_scan_summaries(
    raw_values: Sequence[str],
    parsed_values: Sequence[str],
    scan_positions: Sequence[int],
    summaries: Sequence[str],
) -> list[str]:
    """Independently reject full raw values on the compact operator list."""

    issues: list[str] = []
    lengths = {
        len(raw_values),
        len(parsed_values),
        len(scan_positions),
        len(summaries),
    }
    if len(lengths) != 1:
        return ["operator_scan_summary_count_mismatch"]
    for index, (raw, parsed, position, summary) in enumerate(
        zip(raw_values, parsed_values, scan_positions, summaries),
        1,
    ):
        raw_text = str(raw or "")
        item_code = str(parsed or "").strip()
        display_text = str(summary or "")
        prefix = f"summary_{index}:"
        if len(display_text) > 64:
            issues.append(prefix + "too_long")
        if any(marker in display_text for marker in ("|", "=", "\r", "\n")):
            issues.append(prefix + "raw_delimiter_exposed")
        if raw_text and raw_text != item_code and display_text == raw_text:
            issues.append(prefix + "full_raw_exposed")
        if int(position) == 1:
            if display_text != (item_code or "-"):
                issues.append(prefix + "master_must_equal_item_code")
            continue
        expected_product_prefix = f"{item_code} · " if item_code else ""
        if item_code and not display_text.startswith(expected_product_prefix):
            issues.append(prefix + "item_code_missing")
        if raw_text:
            if " · " not in display_text:
                issues.append(prefix + "short_identifier_missing")
            else:
                identifier = display_text.split(" · ", 1)[1].strip()
                identifier_parts = identifier.split(maxsplit=1)
                if (
                    len(identifier_parts) != 2
                    or not identifier_parts[0].strip()
                    or not identifier_parts[1].strip()
                ):
                    issues.append(prefix + "short_identifier_missing")
    return issues


def _tk_font_line_metrics_with_source(
    widget: Any, text: str, *, heading: bool = False
) -> tuple[tuple[int, ...], int, str]:
    font_name: Any = ""
    font_resolution = ""
    if not heading:
        try:
            # Direct widget fonts override ttk style fonts.  The workbench
            # applies responsive fonts this way, so ignoring this option makes
            # otherwise perfectly fitted labels look clipped in the manifest.
            font_name = widget.cget("font")
            if font_name:
                font_resolution = "direct-widget-font"
        except Exception:
            font_name = ""
    try:
        style_name = str(widget.cget("style") or "")
    except Exception:
        style_name = ""
    try:
        widget_class = str(widget.winfo_class() or "")
    except Exception:
        widget_class = ""
    base_style_name = {
        "TLabel": "TLabel",
        "TButton": "TButton",
        "TEntry": "TEntry",
        "Treeview": "Treeview",
    }.get(widget_class, "")
    style_candidates: list[str] = []
    for candidate in (style_name, base_style_name):
        if not candidate:
            continue
        if heading:
            candidate = (
                "Treeview.Heading"
                if candidate == "Treeview"
                else f"{candidate}.Heading"
            )
        if candidate not in style_candidates:
            style_candidates.append(candidate)
    if heading and "Treeview.Heading" not in style_candidates:
        style_candidates.append("Treeview.Heading")
    if not font_name:
        for candidate in style_candidates:
            try:
                resolved = widget.tk.call(
                    "ttk::style", "lookup", candidate, "-font"
                )
            except Exception:
                resolved = ""
            if resolved:
                font_name = resolved
                font_resolution = f"ttk-style:{candidate}"
                break
    if not font_name:
        font_name = "TkDefaultFont"
        font_resolution = "unresolved-default-fallback"
    lines = str(text).splitlines() or [""]
    try:
        widths = tuple(
            int(widget.tk.call("font", "measure", font_name, line))
            for line in lines
        )
        linespace = int(widget.tk.call("font", "metrics", font_name, "-linespace"))
        source = (
            "tk"
            if font_resolution != "unresolved-default-fallback"
            else "tk-unresolved-default"
        )
    except Exception:
        widths = tuple(len(line) * 8 for line in lines)
        linespace = 16
        source = "headless-approximation"
    return widths, linespace, source


def _tk_font_metrics_with_source(
    widget: Any, text: str, *, heading: bool = False
) -> tuple[int, int, str]:
    widths, linespace, source = _tk_font_line_metrics_with_source(
        widget, text, heading=heading
    )
    return max(widths, default=0), linespace, source


def _tk_font_metrics(widget: Any, text: str, *, heading: bool = False) -> tuple[int, int]:
    width, linespace, _source = _tk_font_metrics_with_source(
        widget, text, heading=heading
    )
    return width, linespace


def _widget_tcl_list(widget: Any, value: Any) -> list[Any]:
    if value in (None, ""):
        return []
    if isinstance(value, (tuple, list)):
        return list(value)
    try:
        return list(widget.tk.splitlist(value))
    except Exception:
        return [value]


def _widget_pixel_value(widget: Any, value: Any) -> int:
    try:
        return int(round(float(widget.winfo_pixels(value))))
    except Exception:
        return int(round(float(value)))


def _horizontal_style_padding(widget: Any, value: Any) -> tuple[int, int]:
    parts = _widget_tcl_list(widget, value)
    if not parts:
        return 0, 0
    pixels = [_widget_pixel_value(widget, part) for part in parts]
    if len(pixels) == 1:
        return pixels[0], pixels[0]
    if len(pixels) == 2:
        return pixels[0], pixels[0]
    return pixels[0], pixels[2]


def _displayed_tree_columns(tree: Any) -> list[str]:
    columns = [
        str(value) for value in _widget_tcl_list(tree, tree.cget("columns"))
    ]
    display_columns = [
        str(value)
        for value in _widget_tcl_list(tree, tree.cget("displaycolumns"))
    ]
    if not display_columns or display_columns == ["#all"]:
        return columns
    return display_columns


def _heading_image_width(tree: Any, image_value: Any) -> int:
    image_names = _widget_tcl_list(tree, image_value)
    if not image_names:
        return 0
    image_name = str(image_names[0]).strip()
    if not image_name:
        return 0
    return max(0, int(tree.tk.call("image", "width", image_name)))


def _tree_heading_pixels(tree: Any) -> dict[str, Any]:
    """Measure each actually visible Treeview heading cell in screen pixels."""

    width = max(0, int(tree.winfo_width()))
    height = max(0, int(tree.winfo_height()))
    if width <= 1 or height <= 1:
        raise RuntimeError("tree has no measurable viewport")
    probe_xs = sorted(
        {
            value
            for value in (
                0,
                1,
                2,
                3,
                width // 4,
                width // 2,
                (width * 3) // 4,
                width - 2,
            )
            if 0 <= value < width
        }
    )
    header_y: int | None = None
    for y in range(min(height, TREE_HEADING_SCAN_HEIGHT)):
        if any(str(tree.identify_region(x, y)) == "heading" for x in probe_xs):
            header_y = y
            break
    if header_y is None:
        raise RuntimeError("tree heading row is not visible")
    visible_heading_widths: dict[str, int] = {}
    first_heading_or_separator_x: int | None = None
    for x in range(width):
        region = str(tree.identify_region(x, header_y))
        if region in {"heading", "separator"} and first_heading_or_separator_x is None:
            first_heading_or_separator_x = x
        if region != "heading":
            continue
        display_position = str(tree.identify_column(x))
        visible_heading_widths[display_position] = (
            visible_heading_widths.get(display_position, 0) + 1
        )
    if first_heading_or_separator_x is None:
        raise RuntimeError("tree heading viewport is not measurable")
    outer_inset = max(0, int(first_heading_or_separator_x))
    return {
        "header_y": header_y,
        "tree_widget_width_px": width,
        "outer_inset_px": outer_inset,
        "viewport_width_px": max(0, width - (outer_inset * 2)),
        "visible_heading_widths_px": visible_heading_widths,
    }


def _ttk_style_lookup(widget: Any, style_name: str, option: str) -> Any:
    try:
        return widget.tk.call("ttk::style", "lookup", style_name, f"-{option}")
    except Exception:
        return ""


def collect_tree_text_fit(
    tree: Any,
    name: str,
) -> dict[str, Any]:
    if not _is_mapped(tree):
        return {
            "tree": name,
            "mapped": False,
            "measurement_applicable": False,
            "records": [],
            "checks": {},
            "proxy": {"suspected": False, "issue_count": 0},
            "passed": True,
        }
    try:
        columns = tuple(_displayed_tree_columns(tree))
        raw_columns = tuple(
            str(value)
            for value in _widget_tcl_list(tree, tree.cget("columns"))
        )
        tree_style = str(tree.cget("style") or "Treeview")
    except Exception as exc:
        return {
            "tree": name,
            "mapped": True,
            "measurement_applicable": True,
            "records": [],
            "checks": {"measurement_completed": False},
            "proxy": {"suspected": True, "issue_count": 1},
            "passed": False,
            "error": f"{type(exc).__name__}: {exc}",
        }
    records: list[dict[str, Any]] = []
    tree_width = max(1, int(tree.winfo_width()))
    tree_height = max(1, int(tree.winfo_height()))
    heading_pixels = _tree_heading_pixels(tree)
    heading_style = f"{tree_style}.Heading"
    heading_padding = _ttk_style_lookup(tree, heading_style, "padding")
    heading_padding_left, heading_padding_right = _horizontal_style_padding(
        tree, heading_padding
    )
    configured_row_height_value = _ttk_style_lookup(
        tree, tree_style, "rowheight"
    )
    try:
        configured_row_height = max(
            0, int(round(float(configured_row_height_value or 0)))
        )
    except (TypeError, ValueError):
        configured_row_height = 0
    _empty_width, body_line_height, body_measurement_source = (
        _tk_font_metrics_with_source(tree, "")
    )
    minimum_data_row_height = (
        max(1, int(body_line_height)) + TREE_DATA_VERTICAL_PADDING_PX
    )
    all_headings_measured = bool(columns)
    all_heading_text_fits = bool(columns)
    heading_records: list[dict[str, Any]] = []
    for position, column in enumerate(columns, 1):
        heading_text = str(tree.heading(column, "text") or "")
        (
            text_width,
            line_height,
            measurement_source,
        ) = _tk_font_metrics_with_source(tree, heading_text, heading=True)
        try:
            heading_image = tree.heading(column, "image")
        except Exception:
            heading_image = ""
        image_width = _heading_image_width(tree, heading_image)
        image_gap = TREE_HEADING_IMAGE_GAP_PX if image_width else 0
        visible_width = int(
            heading_pixels["visible_heading_widths_px"].get(f"#{position}", 0)
        )
        available_width = max(
            0,
            visible_width
            - heading_padding_left
            - heading_padding_right
            - image_width
            - image_gap,
        )
        heading_fits = bool(
            visible_width > 0
            and measurement_source == "tk"
            and text_width <= available_width
        )
        all_headings_measured = all_headings_measured and visible_width > 0
        all_heading_text_fits = all_heading_text_fits and heading_fits
        heading_record = {
            "name": f"{name}:heading:{column}",
            "column": column,
            "visible": visible_width > 0,
            "measurement_applicable": True,
            "width": visible_width,
            "height": line_height + 6,
            "text": heading_text,
            "text_width": text_width,
            "available_text_width": available_width,
            "fit_slack_px": available_width - text_width,
            "line_height": line_height,
            "text_nonblank": bool(heading_text.strip()),
            "measurement_source": measurement_source,
            "padding_left_px": heading_padding_left,
            "padding_right_px": heading_padding_right,
            "heading_image_width_px": image_width,
            "heading_image_gap_px": image_gap,
            "allow_overflow": False,
            "passed": heading_fits,
        }
        records.append(heading_record)
        heading_records.append(heading_record)

    tree_children = tuple(tree.get_children(""))
    visible_row_count = 0
    expected_visible_cell_count = 0
    measured_visible_cell_count = 0
    all_data_text_fits = True
    all_actual_row_heights_fit = True
    data_cell_records: list[dict[str, Any]] = []
    visible_row_heights: list[int] = []
    for iid in tree_children:
        try:
            row_bbox = tuple(int(value) for value in (tree.bbox(iid) or ()))
        except Exception:
            row_bbox = ()
        row_visible = bool(
            len(row_bbox) == 4
            and row_bbox[2] > 0
            and row_bbox[3] > 0
            and min(row_bbox[1] + row_bbox[3], tree_height) - max(row_bbox[1], 0) > 0
        )
        if row_visible:
            visible_row_count += 1
            expected_visible_cell_count += len(columns)
            visible_row_heights.append(int(row_bbox[3]))
            all_actual_row_heights_fit = bool(
                all_actual_row_heights_fit
                and int(row_bbox[3]) >= minimum_data_row_height
            )
        values = list(tree.item(iid, "values") or ())
        for column in columns:
            try:
                value_index = raw_columns.index(column)
            except ValueError:
                value_index = -1
            try:
                bbox = tuple(
                    int(value) for value in (tree.bbox(iid, column) or ())
                )
            except Exception:
                bbox = ()
            cell_measured = bool(
                row_visible
                and value_index >= 0
                and len(bbox) == 4
                and bbox[2] > 0
                and bbox[3] > 0
            )
            visible_width = (
                max(0, min(bbox[0] + bbox[2], tree_width) - max(bbox[0], 0))
                if cell_measured
                else 0
            )
            visible_height = (
                max(0, min(bbox[1] + bbox[3], tree_height) - max(bbox[1], 0))
                if cell_measured
                else 0
            )
            cell_measured = bool(
                cell_measured and visible_width > 0 and visible_height > 0
            )
            if cell_measured:
                measured_visible_cell_count += 1
            text = str(
                values[value_index]
                if value_index >= 0 and value_index < len(values)
                else ""
            )
            (
                text_width,
                line_height,
                measurement_source,
            ) = _tk_font_metrics_with_source(tree, text)
            available_width = max(0, visible_width - TREE_DATA_CELL_GUTTER_PX)
            data_fits = bool(
                not row_visible
                or (
                    cell_measured
                    and measurement_source == "tk"
                    and text_width <= available_width
                    and int(bbox[3]) >= minimum_data_row_height
                )
            )
            if row_visible:
                all_data_text_fits = all_data_text_fits and data_fits
            cell_record = {
                "name": f"{name}:row:{iid}:{column}",
                "item_id": str(iid),
                "column": column,
                "cell_bbox": list(bbox),
                "visible": cell_measured,
                "measurement_applicable": row_visible,
                "width": visible_width,
                "height": int(bbox[3]) if len(bbox) == 4 else 0,
                "visible_cell_width_px": visible_width,
                "visible_cell_height_px": visible_height,
                "text": text,
                "text_width": text_width,
                "available_text_width": available_width,
                "fit_slack_px": available_width - text_width,
                "line_height": line_height,
                "text_nonblank": bool(text.strip()),
                "measurement_source": measurement_source,
                "allow_overflow": False,
                "passed": data_fits,
            }
            records.append(cell_record)
            data_cell_records.append(cell_record)
    visible_data_rows_measured = bool(
        not tree_children or visible_row_count > 0
    )
    all_data_cells_measured = bool(
        visible_data_rows_measured
        and expected_visible_cell_count == measured_visible_cell_count
    )
    all_actual_row_heights_fit = bool(
        visible_data_rows_measured and all_actual_row_heights_fit
    )
    checks = {
        "display_columns_present": bool(columns),
        "all_headings_measured": all_headings_measured,
        "all_heading_text_fits": all_heading_text_fits,
        "visible_data_rows_measured": visible_data_rows_measured,
        "all_data_cells_measured": all_data_cells_measured,
        "all_data_text_fits": all_data_text_fits,
        "configured_row_height_fits_body_font": (
            configured_row_height >= minimum_data_row_height
        ),
        "actual_row_heights_fit_body_font": all_actual_row_heights_fit,
    }
    proxy = evaluate_tree_text_fit_proxy(records)
    return {
        "tree": name,
        "mapped": True,
        "measurement_applicable": True,
        "style": tree_style,
        "heading_style": heading_style,
        "tree_widget_size_px": [tree_width, tree_height],
        "heading_viewport": heading_pixels,
        "heading_padding": str(heading_padding),
        "configured_row_height_px": configured_row_height,
        "body_font_linespace_px": body_line_height,
        "body_font_measurement_source": body_measurement_source,
        "minimum_data_row_height_px": minimum_data_row_height,
        "data_row_count": len(tree_children),
        "visible_data_row_count": visible_row_count,
        "visible_row_heights_px": visible_row_heights,
        "heading_records": heading_records,
        "data_cell_records": data_cell_records,
        "records": records,
        "checks": checks,
        "proxy": proxy,
        "passed": bool(all(checks.values()) and not proxy.get("suspected")),
    }


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
    expected_display_values: Sequence[str],
) -> list[str]:
    issues: list[str] = []
    if len(rendered_rows) != len(presenter_rows):
        return [
            f"qa_row_count_mismatch:{len(rendered_rows)}!={len(presenter_rows)}"
        ]
    if len(expected_display_values) != len(presenter_rows):
        return [
            "qa_expected_display_count_mismatch:"
            f"{len(expected_display_values)}!={len(presenter_rows)}"
        ]
    for offset, (rendered, expected) in enumerate(zip(rendered_rows, presenter_rows), 1):
        text = _row_text(rendered)
        if str(expected["label"]) not in text:
            issues.append(f"qa_row_{offset}_missing_presenter_label")
        values = [str(value) for value in rendered.get("values", ())]
        expected_display = str(expected_display_values[offset - 1])
        if expected_display not in values:
            issues.append(f"qa_row_{offset}_missing_presenter_value")
        tags = {str(tag) for tag in rendered.get("tags", ())}
        if str(expected["state"]) not in tags:
            issues.append(f"qa_row_{offset}_missing_presenter_state_tag")
    return issues


def validate_exact_rows(
    rendered_rows: Sequence[Mapping[str, Any]],
    exact_barcodes: Sequence[str],
    expected_display_values: Sequence[str],
) -> list[str]:
    issues = []
    if len(rendered_rows) != len(exact_barcodes):
        issues.append(
            f"exact_row_count_mismatch:{len(rendered_rows)}!={len(exact_barcodes)}"
        )
        return issues
    if len(expected_display_values) != len(exact_barcodes):
        return [
            "exact_expected_display_count_mismatch:"
            f"{len(expected_display_values)}!={len(exact_barcodes)}"
        ]
    for offset, row in enumerate(rendered_rows, 1):
        values = [str(value) for value in row.get("values", ())]
        if str(expected_display_values[offset - 1]) not in values:
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


def _effective_workflow_notice(app: Any, view: Any) -> Any | None:
    """Mirror the exact runtime priority used by _render_operator_workbench."""

    return view.notice or getattr(app, "_package_cancellation_review_notice", None)


def collect_notice_display_contract(
    app: Any, view: Any, widgets: Mapping[str, Any]
) -> dict[str, Any]:
    notice = _effective_workflow_notice(app, view)
    if notice is None:
        return {"required": False, "passed": True}
    compactor = getattr(app, "_compact_operator_notice_message", None)
    if not callable(compactor):
        return {
            "required": True,
            "issues": ["notice_compactor_missing"],
            "passed": False,
        }
    expected_message = str(compactor(notice.message))
    next_action = str(view.next_action or "").strip()
    if next_action and next_action not in expected_message:
        expected_message = f"{expected_message}\n다음: {next_action}"
    expected_message = str(compactor(expected_message))
    expected_title = str(notice.title)
    try:
        actual_message = str(widgets["workflow_notice_label"].cget("text") or "")
        actual_title = str(
            widgets["workflow_notice_title_label"].cget("text") or ""
        )
    except Exception as exc:
        return {
            "required": True,
            "issues": [f"notice_widget_read_failed:{type(exc).__name__}"],
            "passed": False,
        }
    mapped_texts = _visible_texts(app)
    title_occurrences = sum(text == expected_title for text in mapped_texts)
    message_occurrences = sum(text == expected_message for text in mapped_texts)
    issues = []
    if actual_title != expected_title:
        issues.append("notice_title_display_mismatch")
    if actual_message != expected_message:
        issues.append("notice_compact_message_display_mismatch")
    if title_occurrences != 1:
        issues.append(f"notice_title_occurrence_count:{title_occurrences}!=1")
    if message_occurrences != 1:
        issues.append(f"notice_message_occurrence_count:{message_occurrences}!=1")
    return {
        "required": True,
        "expected_title": expected_title,
        "actual_title": actual_title,
        "expected_compact_message": expected_message,
        "actual_compact_message": actual_message,
        "title_occurrences": title_occurrences,
        "message_occurrences": message_occurrences,
        "expected_nonblank_lines": len(
            [line for line in expected_message.splitlines() if line.strip()]
        ),
        "actual_nonblank_lines": len(
            [line for line in actual_message.splitlines() if line.strip()]
        ),
        "issues": issues,
        "passed": not issues,
    }


def _resolve_widgets(app: Any) -> dict[str, Any]:
    cancel_name, cancel = _find_cancel_button(app)
    widgets = {name: getattr(app, name) for name in REQUIRED_WIDGET_ATTRS}
    widgets["cancel_button"] = cancel
    widgets["cancel_button_attr"] = cancel_name
    for name in (
        "exact_rescan_detail_frame",
        "exact_rescan_detail_text",
        "exact_rescan_detail_metadata_label",
        "exact_rescan_detail_scrollbar",
    ):
        widgets[name] = getattr(app, name, None)
    return widgets


def expected_scan_tree_mapping(fixture: StateFixture | None, app: Any) -> dict[str, bool]:
    """Return which central live-list widgets must be mapped for this state."""

    if fixture is not None:
        exact_mode = bool(
            fixture.exact_active
            or (fixture.exact_complete and len(fixture.qa_scans) <= 1)
        )
    else:
        current = dict(getattr(app, "current_set_info", {}) or {})
        qa_count = max(
            len(tuple(current.get("raw") or ())),
            len(tuple(current.get("parsed") or ())),
        )
        exact_mode = bool(
            current.get("exact_rescan_active")
            or (
                current.get("exact_rescan_complete")
                and qa_count <= 1
            )
        )
    return {
        "current_set_tree": not exact_mode,
        "exact_rescan_tree": exact_mode,
    }


def _pairwise_widget_names(names: Sequence[str]) -> tuple[tuple[str, str], ...]:
    ordered = tuple(str(name) for name in names)
    return tuple(
        (ordered[first], ordered[second])
        for first in range(len(ordered))
        for second in range(first + 1, len(ordered))
    )


def collect_ui_geometry(
    app: Any, fixture: StateFixture | None = None
) -> dict[str, Any]:
    widgets = _resolve_widgets(app)
    root_size = (int(app.winfo_width()), int(app.winfo_height()))
    tree_mapping = expected_scan_tree_mapping(fixture, app)
    history_required = bool(fixture is not None and fixture.history_readonly)
    specs: list[tuple[str, Any, bool, bool]] = [
        ("main", widgets["main_frame"], True, False),
        ("header", widgets["operator_header_frame"], True, False),
        ("header_title", widgets["operator_title_label"], True, True),
        (
            "header_context",
            widgets["operator_header_context_label"],
            False,
            True,
        ),
        ("header_top_right", widgets["top_right_frame"], True, False),
        ("header_clock", widgets["clock_label"], True, True),
        ("header_settings", widgets["settings_button"], True, True),
        ("header_about", widgets["about_button"], True, True),
        ("workbench", widgets["workbench_frame"], True, False),
        ("left_card", widgets["left_context_card"], True, False),
        ("center_card", widgets["top_card"], True, False),
        ("right_card", widgets["right_activity_card"], True, False),
        ("headline", widgets["big_display_label"], True, True),
        ("progress", widgets["progress_frame"], True, False),
        ("input_frame", widgets["operator_input_frame"], True, False),
        ("entry", widgets["entry"], True, True),
        ("notice", widgets["workflow_notice_frame"], True, False),
        ("live_scan_notebook", widgets["live_scan_notebook"], True, False),
        (
            "qa_scan_frame",
            widgets["qa_scan_frame"],
            tree_mapping["current_set_tree"],
            False,
        ),
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
        (
            "exact_rescan_frame",
            widgets["exact_rescan_frame"],
            tree_mapping["exact_rescan_tree"],
            False,
        ),
        (
            "qa_scan_detail_frame",
            widgets["qa_scan_detail_frame"],
            tree_mapping["current_set_tree"],
            False,
        ),
        (
            "qa_scan_detail_text",
            widgets["qa_scan_detail_text"],
            tree_mapping["current_set_tree"],
            False,
        ),
        ("history_notebook", widgets["operator_history_notebook"], True, False),
        (
            "history_header_frame",
            widgets["hist_header_frame"],
            history_required,
            False,
        ),
        (
            "history_header_label",
            widgets["hist_header_label"],
            history_required,
            True,
        ),
        (
            "history_controls",
            widgets["hist_control_frame"],
            history_required,
            False,
        ),
        ("session_tree", widgets["session_tree"], False, False),
        ("history_tree", widgets["history_tree"], False, False),
        ("summary_tree", widgets["summary_tree"], False, False),
        ("action_frame", widgets["operator_action_frame"], True, False),
        ("bottom_frame", widgets["bottom_frame"], True, False),
        ("reset_button", widgets["reset_button"], True, True),
        ("cancel_button", widgets["cancel_button"], True, True),
        ("manual_complete_button", widgets["manual_complete_button"], True, True),
        ("exact_rescan_button", widgets["exact_rescan_button"], True, True),
        ("status_frame", widgets["operator_status_frame"], True, False),
        ("footer", widgets["operator_footer_label"], True, True),
    ]
    history_button_names = (
        ("history_today_button", "today_button"),
        ("history_date_search_button", "date_search_button"),
        ("history_decrease_font_button", "decrease_font_button"),
        ("history_increase_font_button", "increase_font_button"),
    )
    present_history_buttons = []
    for record_name, attribute_name in history_button_names:
        button = getattr(app, attribute_name, None)
        if button is None:
            if history_required:
                raise RuntimeError(
                    f"history read-only capture is missing {attribute_name}"
                )
            continue
        specs.append((record_name, button, history_required, True))
        present_history_buttons.append(record_name)
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
    for name in (
        "exact_rescan_detail_frame",
        "exact_rescan_detail_text",
        "exact_rescan_detail_metadata_label",
        "exact_rescan_detail_scrollbar",
    ):
        widget = widgets.get(name)
        if widget is not None:
            records.append(
                _widget_record(
                    app,
                    widget,
                    name,
                    critical=tree_mapping["exact_rescan_tree"],
                    # Read-only Text widgets intentionally wrap/scroll inside
                    # their container; character-based requested width is not
                    # a clipping contract.  Height and containment remain
                    # fail-closed below.
                    check_requested_width=name not in {
                        "exact_rescan_detail_frame",
                        "exact_rescan_detail_text",
                    },
                    check_requested_height=name == "exact_rescan_detail_text",
                )
            )
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
    recorded_widget_ids = {id(widget) for _name, widget, _critical, _check in specs}
    recorded_widget_ids.update(
        {
            id(widgets["workflow_notice_title_label"]),
            id(widgets["workflow_notice_label"]),
            id(widgets["workflow_notice_action_button"]),
        }
    )
    recorded_widget_ids.update(
        id(widgets[name])
        for name in (
            "exact_rescan_detail_frame",
            "exact_rescan_detail_text",
            "exact_rescan_detail_metadata_label",
            "exact_rescan_detail_scrollbar",
        )
        if widgets.get(name) is not None
    )
    for index, candidate in enumerate(_descendants(app), 1):
        if id(candidate) in recorded_widget_ids:
            continue
        try:
            text = str(candidate.cget("text") or "").strip()
        except Exception:
            continue
        if not text:
            continue
        records.append(
            _widget_record(
                app,
                candidate,
                f"visible_text_{index}:{candidate}",
                critical=False,
                check_requested_width=True,
                check_requested_height=True,
            )
        )
    containment = [
        ("header", "main"),
        ("workbench", "main"),
        ("status_frame", "main"),
        ("header_title", "header"),
        ("header_context", "header"),
        ("header_top_right", "header"),
        ("header_clock", "header_top_right"),
        ("header_settings", "header_top_right"),
        ("header_about", "header_top_right"),
        ("left_card", "workbench"),
        ("center_card", "workbench"),
        ("right_card", "workbench"),
        ("headline", "center_card"),
        ("progress", "center_card"),
        ("notice", "center_card"),
        ("input_frame", "center_card"),
        ("entry", "input_frame"),
        ("notice_title", "notice"),
        ("notice_message", "notice"),
        ("notice_action", "notice"),
        ("live_scan_notebook", "center_card"),
        ("qa_scan_frame", "live_scan_notebook"),
        ("current_set_tree", "qa_scan_frame"),
        ("exact_rescan_frame", "live_scan_notebook"),
        ("exact_rescan_tree", "exact_rescan_frame"),
        ("qa_scan_detail_frame", "qa_scan_frame"),
        ("qa_scan_detail_text", "qa_scan_detail_frame"),
        ("history_notebook", "right_card"),
        ("history_header_frame", "history_notebook"),
        ("history_header_label", "history_header_frame"),
        ("history_controls", "history_header_frame"),
        ("action_frame", "right_card"),
        ("session_tree", "history_notebook"),
        ("history_tree", "history_notebook"),
        ("summary_tree", "history_notebook"),
        ("reset_button", "bottom_frame"),
        ("cancel_button", "bottom_frame"),
        ("manual_complete_button", "bottom_frame"),
        ("exact_rescan_button", "bottom_frame"),
        ("footer", "status_frame"),
        *((f"step_{index}", "center_card") for index in range(1, 6)),
    ]
    containment.extend(
        (record_name, "history_controls")
        for record_name in present_history_buttons
    )
    if widgets.get("exact_rescan_detail_frame") is not None:
        containment.append(("exact_rescan_detail_frame", "exact_rescan_frame"))
    if widgets.get("exact_rescan_detail_text") is not None:
        containment.append(("exact_rescan_detail_text", "exact_rescan_detail_frame"))
    if widgets.get("exact_rescan_detail_metadata_label") is not None:
        containment.append(
            ("exact_rescan_detail_metadata_label", "exact_rescan_detail_frame")
        )
    if widgets.get("exact_rescan_detail_scrollbar") is not None:
        containment.append(
            ("exact_rescan_detail_scrollbar", "exact_rescan_detail_frame")
        )
    overlaps = [
        ("header", "workbench"),
        ("header", "status_frame"),
        ("workbench", "status_frame"),
        ("header_title", "header_context"),
        ("header_title", "header_top_right"),
        ("header_context", "header_top_right"),
        ("left_card", "center_card"),
        ("left_card", "right_card"),
        ("center_card", "right_card"),
        ("headline", "progress"),
        ("progress", "notice"),
        ("notice", "entry"),
        ("notice", "input_frame"),
        ("input_frame", "live_scan_notebook"),
        ("current_set_tree", "qa_scan_detail_frame"),
        ("exact_rescan_tree", "exact_rescan_detail_frame"),
        ("history_header_label", "history_controls"),
        ("history_notebook", "action_frame"),
    ]
    button_names = (
        "reset_button",
        "cancel_button",
        "manual_complete_button",
        "exact_rescan_button",
    )
    overlaps.extend(_pairwise_widget_names(button_names))
    overlaps.extend(_pairwise_widget_names(present_history_buttons))
    by_name = {record["name"]: record for record in records}
    by_name["qa_scan_detail_text"]["check_requested_height"] = True
    active_tree_name = (
        "current_set_tree" if tree_mapping["current_set_tree"] else "exact_rescan_tree"
    )
    active_frame_name = (
        "qa_scan_frame" if tree_mapping["current_set_tree"] else "exact_rescan_frame"
    )
    active_detail_name = (
        "qa_scan_detail_frame"
        if tree_mapping["current_set_tree"]
        else "exact_rescan_detail_frame"
    )
    active_partition = evaluate_tree_detail_partition(
        by_name[active_tree_name],
        by_name[active_detail_name],
        by_name[active_frame_name],
    )
    center_list_below_input = (
        by_name[active_tree_name]["bbox"][1] >= by_name["entry"]["bbox"][3] - 1
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
    tree_text_fit = [
        collect_tree_text_fit(current_tree, "current_set_tree"),
        collect_tree_text_fit(exact_tree, "exact_rescan_tree"),
    ]
    tree_text_fit.extend(
        collect_tree_text_fit(widgets[name], name)
        for name in ("session_tree", "history_tree", "summary_tree")
        if _is_mapped(widgets[name])
    )
    detail_frame_record = by_name["qa_scan_detail_frame"]
    detail_text_record = by_name["qa_scan_detail_text"]
    detail_contract_active = bool(detail_text_record.get("mapped"))
    detail_text_bottom_ok = bool(
        not detail_contract_active
        or int(detail_text_record["bbox"][3])
        <= int(detail_frame_record["bbox"][3]) - 4
    )
    detail_text_height_ok = bool(
        not detail_contract_active
        or int(detail_text_record["requested_size"][1])
        <= int(detail_text_record["size"][1]) + 2
    )
    exact_detail_frame_record = by_name.get("exact_rescan_detail_frame")
    exact_detail_text_record = by_name.get("exact_rescan_detail_text")
    exact_detail_available = bool(
        exact_detail_frame_record is not None and exact_detail_text_record is not None
    )
    exact_detail_active = bool(
        exact_detail_available and exact_detail_text_record.get("mapped")
    )
    exact_detail_bottom_ok = bool(
        not exact_detail_active
        or int(exact_detail_text_record["bbox"][3])
        <= int(exact_detail_frame_record["bbox"][3]) - 4
    )
    exact_detail_height_ok = bool(
        not exact_detail_active
        or int(exact_detail_text_record["requested_size"][1])
        <= int(exact_detail_text_record["size"][1]) + 2
    )
    action_heights = {
        name: int(by_name[name]["size"][1])
        for name in button_names
        if by_name[name].get("mapped")
    }
    right_action_height_contract = bool(
        len(action_heights) == len(button_names)
        and all(86 <= height <= 104 for height in action_heights.values())
    )
    status_height = int(by_name["status_frame"]["size"][1])
    footer_height = int(by_name["footer"]["size"][1])
    status_footer_height_contract = bool(
        status_height <= 32
        and footer_height <= 32
        and int(by_name["footer"]["requested_size"][1]) <= footer_height + 2
    )
    try:
        rendered_notice_text = str(
            widgets["workflow_notice_label"].cget("text") or ""
        )
    except Exception:
        rendered_notice_text = ""
    rendered_notice_lines = [
        line.strip() for line in rendered_notice_text.splitlines() if line.strip()
    ]
    original_notice_text = ""
    if fixture is not None:
        original_notice_text = fixture.error_message or fixture.notice_message
    original_notice_lines = [
        line.strip() for line in str(original_notice_text).splitlines() if line.strip()
    ]
    notice_message_record = by_name["notice_message"]
    notice_record = by_name["notice"]
    notice_reqheight_fits = bool(
        int(notice_message_record["requested_size"][1])
        <= int(notice_message_record["size"][1]) + 2
        and int(notice_message_record["bbox"][3])
        <= int(notice_record["bbox"][3]) + 1
    )
    mismatch_notice_contract = bool(
        fixture is None
        or fixture.state_id != "error"
        or (
            len(original_notice_lines) == 4
            and len(rendered_notice_lines) == 3
            and notice_reqheight_fits
        )
    )
    return {
        "root_size": list(root_size),
        "widgets": records,
        "clipping_proxy": evaluate_clipping_proxy(
            records,
            root_size,
            overlap_pairs=tuple(overlaps),
            containment_pairs=tuple(containment),
        ),
        "text_clipping_proxy": evaluate_text_clipping_proxy(records),
        "tree_text_fit": tree_text_fit,
        "tree_text_clipping_suspected": any(
            item.get("measurement_applicable") is True
            and item.get("passed") is not True
            for item in tree_text_fit
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
            "active_tree_detail_partition": active_partition,
            "detail_text_bottom_within_frame": detail_text_bottom_ok,
            "detail_text_requested_height_fits": detail_text_height_ok,
            "exact_detail_available": exact_detail_available,
            "exact_detail_text_bottom_within_frame": exact_detail_bottom_ok,
            "exact_detail_text_requested_height_fits": exact_detail_height_ok,
            "right_action_heights": action_heights,
            "right_action_height_contract_86_to_104": right_action_height_contract,
            "status_frame_height": status_height,
            "footer_height": footer_height,
            "status_footer_height_contract_max_32": status_footer_height_contract,
            "notice_original_nonblank_line_count": len(original_notice_lines),
            "notice_rendered_nonblank_line_count": len(rendered_notice_lines),
            "notice_message_reqheight_fits": notice_reqheight_fits,
            "mismatch_notice_4_to_3_line_contract": mismatch_notice_contract,
            "mapped_workflow_notice_frame_count": len(notice_frame_attrs),
            "cancel_button_attr": widgets["cancel_button_attr"],
            "center_list_signature": {
                "path": str(current_tree),
                "master_path": str(getattr(current_tree, "master", "")),
                "mapped": by_name["current_set_tree"]["mapped"],
                "bbox": by_name["current_set_tree"]["bbox"],
                "grid": by_name["current_set_tree"]["grid"],
            },
            "active_scan_tree_signature": {
                "mode": "qa" if tree_mapping["current_set_tree"] else "f4",
                "tree_path": by_name[active_tree_name]["path"],
                "tree_bbox": by_name[active_tree_name]["bbox"],
                "detail_bbox": by_name[active_detail_name]["bbox"],
                "tree_mapped": by_name[active_tree_name]["mapped"],
                "logical_frame_path": by_name[active_frame_name]["path"],
                "logical_bbox": by_name[active_frame_name]["bbox"],
                "logical_frame_mapped": by_name[active_frame_name]["mapped"],
                "notebook_bbox": by_name["live_scan_notebook"]["bbox"],
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
                    "header",
                    "workbench",
                    "status_frame",
                    "left_card",
                    "center_card",
                    "right_card",
                    "headline",
                    "progress",
                    "entry",
                    "notice",
                    "input_frame",
                    "live_scan_notebook",
                    "qa_scan_frame",
                    "current_set_tree",
                    "exact_rescan_frame",
                    "exact_rescan_tree",
                    "qa_scan_detail_frame",
                    "action_frame",
                    "bottom_frame",
                    "footer",
                }
            },
        },
    }


def collect_rendered_state(app: Any, fixture: StateFixture, view: Any) -> dict[str, Any]:
    widgets = _resolve_widgets(app)
    current_rows = _tree_rows(widgets["current_set_tree"])
    exact_rows = _tree_rows(widgets["exact_rescan_tree"])
    session_rows = _tree_rows(widgets["session_tree"])
    history_rows = _tree_rows(widgets["history_tree"])
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
    presenter_notice = view.notice
    notice = _effective_workflow_notice(app, view)
    notice_display_contract = collect_notice_display_contract(app, view, widgets)
    current_tree_mapped = _is_mapped(widgets["current_set_tree"])
    exact_tree_mapped = _is_mapped(widgets["exact_rescan_tree"])
    mapped_scan_tree = (
        widgets["exact_rescan_tree"]
        if exact_tree_mapped
        else widgets["current_set_tree"]
        if current_tree_mapped
        else None
    )
    try:
        live_scan_viewport_width = int(mapped_scan_tree.winfo_width())
    except (AttributeError, TypeError, ValueError):
        live_scan_viewport_width = 0
    qa_detail_contract = collect_qa_detail_contract(app, fixture, view)
    presenter_rows = expected_presenter_rows(view)
    qa_raw_values = tuple(str(row.get("value") or "") for row in presenter_rows)
    accepted_parsed_values = fixture_parsed_scans(fixture)
    qa_parsed_values = tuple(
        accepted_parsed_values[index]
        if index < len(accepted_parsed_values)
        else ""
        for index in range(len(qa_raw_values))
    )
    qa_scan_positions = tuple(range(1, len(qa_raw_values) + 1))
    qa_summary_values = expected_operator_scan_summaries(
        app,
        qa_raw_values,
        qa_parsed_values,
        qa_scan_positions,
    )
    qa_summary_issues = validate_operator_scan_summaries(
        qa_raw_values,
        qa_parsed_values,
        qa_scan_positions,
        qa_summary_values,
    )
    expected_qa_display_values = expected_scan_display_values(
        app,
        widgets["current_set_tree"],
        qa_summary_values,
        value_column="Value",
        empty_display="-",
    )
    qa_display_contract = collect_scan_display_contract(
        widgets["current_set_tree"],
        qa_raw_values,
        value_column="Value",
        expected_display_values=expected_qa_display_values,
        iid_prefix="qa-slot-",
        empty_display="-",
        viewport_width=live_scan_viewport_width,
        display_source_values=qa_summary_values,
        required_prefix_values=qa_parsed_values,
    )
    exact_parsed_values = tuple(
        CAPTURE_ITEM_CODE for _ in fixture.exact_barcodes
    )
    exact_scan_positions = tuple(
        2 for _ in fixture.exact_barcodes
    )
    exact_summary_values = expected_operator_scan_summaries(
        app,
        fixture.exact_barcodes,
        exact_parsed_values,
        exact_scan_positions,
    )
    exact_summary_issues = validate_operator_scan_summaries(
        fixture.exact_barcodes,
        exact_parsed_values,
        exact_scan_positions,
        exact_summary_values,
    )
    expected_exact_display_values = expected_scan_display_values(
        app,
        widgets["exact_rescan_tree"],
        exact_summary_values,
        value_column="Value",
    )
    exact_display_contract = collect_scan_display_contract(
        widgets["exact_rescan_tree"],
        fixture.exact_barcodes,
        value_column="Value",
        expected_display_values=expected_exact_display_values,
        viewport_width=live_scan_viewport_width,
        display_source_values=exact_summary_values,
        required_prefix_values=exact_parsed_values,
    )
    exact_detail_contract = collect_exact_detail_contract(app, fixture)
    last_normal_contract = build_last_normal_scan_contract(
        fixture,
        current_rows,
        exact_rows,
        qa_detail_contract,
        exact_detail_contract,
        expected_qa_display_values,
        expected_exact_display_values,
    )
    last_normal_source = str(last_normal_contract.get("source") or "")
    last_normal_source_mapped = bool(
        (last_normal_source == "qa" and current_tree_mapped)
        or (last_normal_source == "f4" and exact_tree_mapped)
    )
    preserved_last_normal_occurrences = int(
        last_normal_contract.get("fitted_cell_exact_count", 0)
    )
    actual_list_last_normal_occurrences = int(
        preserved_last_normal_occurrences if last_normal_source_mapped else 0
    )
    last_normal_occurrences_on_screen = int(
        last_normal_source_mapped and last_normal_contract.get("passed", False)
    )
    return {
        "current_set_rows": current_rows,
        "exact_rescan_rows": exact_rows,
        "session_rows": session_rows,
        "history_rows": history_rows,
        "session_row_count": len(session_rows),
        "history_row_count": len(history_rows),
        "summary_row_count": len(_tree_rows(widgets["summary_tree"])),
        "presenter_rows": presenter_rows,
        "expected_qa_display_values": list(expected_qa_display_values),
        "expected_exact_display_values": list(expected_exact_display_values),
        "qa_summary_values": list(qa_summary_values),
        "exact_summary_values": list(exact_summary_values),
        "qa_summary_contract": {
            "issues": qa_summary_issues,
            "passed": not qa_summary_issues,
        },
        "exact_summary_contract": {
            "issues": exact_summary_issues,
            "passed": not exact_summary_issues,
        },
        "presenter_stage": str(view.current_stage),
        "presenter_stage_label": str(view.current_stage_label),
        "presenter_next_action": str(view.next_action),
        "presenter_last_normal_scan": str(view.last_normal_scan or ""),
        "presenter_notice": (
            {
                "title": str(presenter_notice.title),
                "message": str(presenter_notice.message),
                "kind": str(presenter_notice.kind),
                "tone": str(presenter_notice.tone),
            }
            if presenter_notice is not None
            else None
        ),
        "display_notice": (
            {
                "title": str(notice.title),
                "message": str(notice.message),
                "kind": str(notice.kind),
                "tone": str(notice.tone),
            }
            if notice is not None
            else None
        ),
        "presenter_action_gates": {
            "scan_input_enabled": bool(view.scan_input_enabled),
            "f1_cancel_current_enabled": bool(view.cancel_current_enabled),
            "f2_cancel_completed_enabled": bool(view.cancel_completed_enabled),
            "f3_enabled": bool(view.f3_enabled),
            "f4_enabled": bool(view.f4_enabled),
        },
        "entry_state": entry_state,
        "notice_action_mapped": notice_action_mapped,
        "notice_action_text": notice_action_text,
        "button_states": button_states,
        "center_visible_texts": center_texts,
        "right_visible_texts": right_texts,
        "notice_title_occurrences": int(
            notice_display_contract.get("title_occurrences", 0)
        ),
        "notice_message_occurrences": int(
            notice_display_contract.get("message_occurrences", 0)
        ),
        "notice_display_contract": notice_display_contract,
        "last_normal_occurrences_on_screen": last_normal_occurrences_on_screen,
        "last_normal_occurrences_in_center": actual_list_last_normal_occurrences,
        "last_normal_occurrences_in_actual_list": actual_list_last_normal_occurrences,
        "last_normal_preserved_in_source_list": preserved_last_normal_occurrences,
        "last_normal_source_list_mapped": last_normal_source_mapped,
        "last_normal_occurrences_in_right": 0,
        "current_tree_mapped": current_tree_mapped,
        "exact_tree_mapped": exact_tree_mapped,
        "qa_detail_contract": qa_detail_contract,
        "qa_display_contract": qa_display_contract,
        "exact_display_contract": exact_display_contract,
        "exact_detail_contract": exact_detail_contract,
        "last_normal_contract": last_normal_contract,
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
    if record.get("capture_source") != AUTHORITATIVE_CAPTURE_SOURCE:
        issues.append("non_authoritative_capture_source")
    capture_geometry_gate = record.get("capture_geometry_gate")
    if not isinstance(capture_geometry_gate, Mapping):
        issues.append("capture_geometry_gate_missing")
        capture_geometry_gate = {}
    elif capture_geometry_gate.get("passed") is not True:
        failed_gate_checks = [
            str(name)
            for name, passed in (capture_geometry_gate.get("checks") or {}).items()
            if passed is not True
        ]
        if failed_gate_checks:
            issues.extend(
                f"capture_geometry_gate_{name}" for name in failed_gate_checks
            )
        else:
            issues.append("capture_geometry_gate_failed")
    window_contract = record.get("window_capture_contract", {})
    if window_contract.get("status") != "PASS":
        issues.append("window_capture_contract_failed")
    client_bbox = list(record.get("client_outer_bbox") or ())
    client_capture_bbox = list(record.get("client_capture_bbox") or client_bbox)
    before_window = window_contract.get("before", {})
    attested_client_size = list(before_window.get("client_size") or ())
    expected_client_bbox: list[int] = []
    if len(attested_client_size) == 2:
        expected_client_bbox = [
            0,
            0,
            int(attested_client_size[0]),
            int(attested_client_size[1]),
        ]
    if image.get("analysis_region") != "window_client":
        issues.append("image_analysis_region_not_window_client")
    if client_bbox != expected_client_bbox or len(expected_client_bbox) != 4:
        issues.append("client_outer_bbox_not_attested_client")
    if client_capture_bbox != expected_client_bbox or len(expected_client_bbox) != 4:
        issues.append("client_capture_bbox_not_attested_client")
    if (
        list(image.get("analysis_bbox") or ()) != expected_client_bbox
        or len(expected_client_bbox) != 4
    ):
        issues.append("image_analysis_bbox_not_attested_client")
    if (
        list(image.get("analysis_pixel_size") or ()) != attested_client_size
        or len(attested_client_size) != 2
    ):
        issues.append("image_analysis_size_not_attested_client")
    if not image.get("pixel_size_matches"):
        issues.append("capture_size_mismatch")
    if image.get("blank_suspected"):
        issues.append("blank_image_suspected")
    if float(image.get("near_black_ratio", 1.0)) > NEAR_BLACK_FAILURE_RATIO:
        issues.append("near_black_ratio_exceeded")
    if image.get("excess_black_suspected"):
        issues.append("excess_black_suspected")
    if image.get("edge_black_stripe_suspected"):
        issues.append("edge_black_stripe_suspected")
    if image.get("contiguous_black_stripe_suspected"):
        issues.append("contiguous_black_stripe_suspected")
    if image.get("black_tile_suspected"):
        issues.append("black_tile_suspected")
    if image.get("uniform_low_variance_suspected"):
        issues.append("uniform_low_variance_suspected")
    if image.get("capture_pixels_valid") is not True:
        issues.append("capture_pixels_invalid")
    if not math.isclose(
        float(record.get("requested_scale", 0)),
        float(record.get("applied_scale_factor", -1)),
        rel_tol=0,
        abs_tol=0.001,
    ):
        issues.append("scale_factor_not_applied")
    if geometry["clipping_proxy"].get("suspected"):
        issues.append("clipping_or_overlap_suspected")
    if geometry.get("text_clipping_proxy", {}).get("suspected"):
        issues.append("requested_vs_actual_text_clipping_suspected")
    if geometry.get("tree_text_clipping_suspected"):
        issues.append("tree_text_clipping_suspected")
    if not structure.get("three_distinct_cards"):
        issues.append("three_card_contract_failed")
    if not structure.get("current_and_exact_trees_are_distinct"):
        issues.append("qa_and_exact_lists_are_not_separate")
    if not structure.get("center_current_list_below_scan_input"):
        issues.append("current_scan_list_not_below_input")
    if not structure.get("active_tree_detail_partition", {}).get("passed"):
        issues.extend(
            f"active_tree_detail_partition:{issue}"
            for issue in structure.get("active_tree_detail_partition", {}).get(
                "issues", ()
            )
        )
    if not structure.get("detail_text_bottom_within_frame"):
        issues.append("detail_text_overruns_detail_frame")
    if not structure.get("detail_text_requested_height_fits"):
        issues.append("detail_text_height_compressed")
    if structure.get("exact_detail_available") and not structure.get(
        "exact_detail_text_bottom_within_frame"
    ):
        issues.append("exact_detail_text_overruns_detail_frame")
    if structure.get("exact_detail_available") and not structure.get(
        "exact_detail_text_requested_height_fits"
    ):
        issues.append("exact_detail_text_height_compressed")
    if not structure.get("right_action_height_contract_86_to_104"):
        issues.append("right_action_height_outside_86_to_104")
    if not structure.get("status_footer_height_contract_max_32"):
        issues.append("status_or_footer_height_exceeds_32")
    if not structure.get("mismatch_notice_4_to_3_line_contract"):
        issues.append("mismatch_notice_4_to_3_line_contract_failed")
    if not structure.get("notice_message_reqheight_fits"):
        issues.append("notice_message_reqheight_does_not_fit")
    if structure.get("mapped_workflow_notice_frame_count") != 1:
        issues.append("workflow_notice_frame_not_single")
    issues.extend(
        validate_presenter_rows(
            rendered["current_set_rows"],
            rendered["presenter_rows"],
            rendered.get("expected_qa_display_values", ()),
        )
    )
    if not rendered.get("qa_detail_contract", {}).get("passed"):
        issues.extend(
            f"qa_detail_contract:{issue}"
            for issue in rendered.get("qa_detail_contract", {}).get("issues", ())
        )
    qa_summary_contract = rendered.get("qa_summary_contract", {})
    if qa_summary_contract.get("passed") is not True:
        summary_issues = tuple(qa_summary_contract.get("issues", ()))
        if summary_issues:
            issues.extend(
                f"qa_summary_contract:{issue}" for issue in summary_issues
            )
        else:
            issues.append("qa_summary_contract_missing_or_failed")
    exact_summary_contract = rendered.get("exact_summary_contract", {})
    if exact_summary_contract.get("passed") is not True:
        summary_issues = tuple(exact_summary_contract.get("issues", ()))
        if summary_issues:
            issues.extend(
                f"exact_summary_contract:{issue}" for issue in summary_issues
            )
        else:
            issues.append("exact_summary_contract_missing_or_failed")
    if not rendered.get("qa_display_contract", {}).get("passed"):
        issues.extend(
            f"qa_display_contract:{issue}"
            for issue in rendered.get("qa_display_contract", {}).get("issues", ())
        )
    if not rendered.get("exact_display_contract", {}).get("passed"):
        issues.extend(
            f"exact_display_contract:{issue}"
            for issue in rendered.get("exact_display_contract", {}).get("issues", ())
        )
    if not rendered.get("exact_detail_contract", {}).get("passed"):
        issues.extend(
            f"exact_detail_contract:{issue}"
            for issue in rendered.get("exact_detail_contract", {}).get("issues", ())
        )
    issues.extend(
        validate_exact_rows(
            rendered["exact_rescan_rows"],
            fixture.get("exact_barcodes", ()),
            rendered.get("expected_exact_display_values", ()),
        )
    )
    qa_cell_values = {
        str(value)
        for row in rendered["current_set_rows"]
        for value in row.get("values", ())
    }
    if any(
        str(value) in qa_cell_values
        for value in rendered.get("expected_exact_display_values", ())
    ):
        issues.append("exact_rescan_member_leaked_into_qa_list")
    if fixture.get("last_normal_scan"):
        if rendered.get("presenter_last_normal_scan") != fixture["last_normal_scan"]:
            issues.append("presenter_last_normal_scan_not_preserved")
        if not rendered.get("last_normal_contract", {}).get("passed"):
            issues.extend(
                f"last_normal_contract:{issue}"
                for issue in rendered.get("last_normal_contract", {}).get(
                    "issues", ()
                )
            )
        if rendered.get("last_normal_occurrences_on_screen") != 1:
            issues.append("last_normal_scan_not_visible_in_active_center_list")
    notice = rendered.get("display_notice") or rendered.get("presenter_notice")
    if notice:
        notice_contract = rendered.get("notice_display_contract", {})
        if not notice_contract.get("passed"):
            issues.extend(
                f"notice_display_contract:{issue}"
                for issue in notice_contract.get("issues", ())
            )
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
    if record["state"] == "cancellation_conflict":
        expected_notice = {
            "title": CANCELLATION_CONFLICT_TITLE,
            "message": CANCELLATION_CONFLICT_MESSAGE,
            "kind": "package_cancellation_review",
            "tone": "danger",
        }
        if rendered.get("display_notice") != expected_notice:
            issues.append("cancellation_conflict_notice_mismatch")
        if rendered.get("presenter_notice") is not None:
            issues.append("cancellation_conflict_became_presenter_blocking")
        if len(rendered.get("current_set_rows") or ()) != 5:
            issues.append("cancellation_conflict_five_row_scan_list_missing")
        expected_gates = {
            "scan_input_enabled": True,
            "f1_cancel_current_enabled": True,
            "f2_cancel_completed_enabled": True,
            "f3_enabled": True,
            "f4_enabled": False,
        }
        if rendered.get("presenter_action_gates") != expected_gates:
            issues.append("cancellation_conflict_action_gates_changed")
        expected_selected_iid = "qa-slot-4"
        expected_selected_raw = str(
            tuple(fixture.get("qa_scans") or ())[3]
            if len(tuple(fixture.get("qa_scans") or ())) >= 4
            else ""
        )
        qa_detail = rendered.get("qa_detail_contract", {})
        if (
            qa_detail.get("selected_iid") != expected_selected_iid
            or qa_detail.get("selected_raw") != expected_selected_raw
            or qa_detail.get("selected_detail_text") != expected_selected_raw
        ):
            issues.append("cancellation_conflict_selected_raw_detail_changed")
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
    session_rows = tuple(rendered.get("session_rows") or ())
    history_rows = tuple(rendered.get("history_rows") or ())
    if int(rendered.get("session_row_count", -1)) != len(session_rows):
        issues.append("session_activity_row_count_mismatch")
    if int(rendered.get("history_row_count", -1)) != len(history_rows):
        issues.append("history_activity_row_count_mismatch")
    history_mode = record["state"] == "history_readonly"
    if bool(rendered.get("history_tree_mapped")) != history_mode:
        issues.append("history_activity_tree_mapping_mismatch")
    if bool(rendered.get("session_tree_mapped")) == history_mode:
        issues.append("session_activity_tree_mapping_mismatch")

    expected_history_identity = (
        ("capture-activity-001", CAPTURE_ITEM_CODE, "통과", "19:41:03"),
        (
            "capture-activity-002",
            CAPTURE_SECONDARY_ITEM_CODE,
            "불일치",
            "19:42:17",
        ),
    )
    actual_history_identity = tuple(
        (
            str(row.get("iid") or ""),
            str((row.get("values") or [""] * 8)[1]),
            str((row.get("values") or [""] * 8)[6]),
            str((row.get("values") or [""] * 8)[7]),
        )
        for row in history_rows
        if len(row.get("values") or ()) == 8
    )
    expected_session_identity = tuple(
        (f"session-{iid}", timestamp, item, result)
        for iid, item, result, timestamp in expected_history_identity
    )
    actual_session_identity = tuple(
        (
            str(row.get("iid") or ""),
            *tuple(str(value) for value in (row.get("values") or ())),
        )
        for row in session_rows
        if len(row.get("values") or ()) == 3
    )
    if actual_history_identity != expected_history_identity:
        issues.append("history_activity_fixture_identity_mismatch")
    if actual_session_identity != expected_session_identity:
        issues.append("session_activity_fixture_identity_mismatch")
    if history_mode:
        if len(history_rows) != 2:
            issues.append("history_activity_rows_missing")
        for row in history_rows:
            values = tuple(str(value) for value in row.get("values", ()))
            if (
                not str(row.get("iid") or "").startswith("capture-activity-")
                or len(values) != 8
                or not values[1]
                or not values[6]
                or not values[7]
            ):
                issues.append("history_activity_row_shape_invalid")
                break
    else:
        if len(session_rows) != 2:
            issues.append("session_activity_rows_missing")
        for row in session_rows:
            values = tuple(str(value) for value in row.get("values", ()))
            if (
                not str(row.get("iid") or "").startswith(
                    "session-capture-activity-"
                )
                or len(values) != 3
                or not all(values)
            ):
                issues.append("session_activity_row_shape_invalid")
                break
    expected_exact_mapping = bool(
        fixture.get("exact_active")
        or (
            fixture.get("exact_complete")
            and len(tuple(fixture.get("qa_scans") or ())) <= 1
        )
    )
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
    """Keep the full mapped live-list rectangle for cross-state stability."""

    if not signature.get("mapped"):
        return None
    bbox = tuple(signature.get("bbox", ()))
    return {
        "path": signature.get("path"),
        "master_path": signature.get("master_path"),
        "grid": signature.get("grid"),
        "bbox": [int(value) for value in bbox] if len(bbox) == 4 else [],
    }


def _center_signatures_match(
    first: Mapping[str, Any], second: Mapping[str, Any], *, tolerance: int = 2
) -> bool:
    if any(
        first.get(key) != second.get(key)
        for key in ("path", "master_path", "grid")
    ):
        return False
    first_box = list(first.get("bbox") or ())
    second_box = list(second.get("bbox") or ())
    return bool(
        len(first_box) == len(second_box) == 4
        and all(
            abs(int(left) - int(right)) <= tolerance
            for left, right in zip(first_box, second_box)
        )
    )


def _active_logical_bboxes_match(
    first: Mapping[str, Any], second: Mapping[str, Any], *, tolerance: int = 2
) -> bool:
    box_pairs = [
        (
            list(first.get(key) or ()),
            list(second.get(key) or ()),
        )
        for key in ("logical_bbox", "tree_bbox", "detail_bbox")
    ]
    return bool(
        first.get("logical_frame_mapped") is True
        and second.get("logical_frame_mapped") is True
        and first.get("tree_mapped") is True
        and second.get("tree_mapped") is True
        and all(
            len(first_box) == len(second_box) == 4
            and all(
                abs(int(left) - int(right)) <= tolerance
                for left, right in zip(first_box, second_box)
            )
            for first_box, second_box in box_pairs
        )
    )


def apply_cross_capture_contracts(captures: list[dict[str, Any]]) -> None:
    """Apply state-pair and stable-layout checks after individual captures."""

    by_size: dict[tuple[int, int], dict[str, dict[str, Any]]] = {}
    for capture in captures:
        by_size.setdefault(tuple(capture["requested_size"]), {})[capture["state"]] = capture
    for group in by_size.values():
        active_signatures = [
            capture.get("ui_geometry", {})
            .get("structure", {})
            .get("active_scan_tree_signature", {})
            for capture in group.values()
        ]
        if active_signatures:
            first_active = active_signatures[0]
            for capture in group.values():
                active = (
                    capture.get("ui_geometry", {})
                    .get("structure", {})
                    .get("active_scan_tree_signature", {})
                )
                if not _active_logical_bboxes_match(first_active, active):
                    capture["issues"].append(
                        "active_qa_f4_logical_bbox_changed_across_states"
                    )
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
                if signature is not None and not _center_signatures_match(first, signature):
                    capture["issues"].append("center_scan_list_geometry_changed_across_states")
        for normal_id, blocked_id in (
            ("qa_product_3", "error"),
            ("qa_product_3", "cancellation_conflict"),
            ("full_complete", "submission_blocked"),
        ):
            normal, blocked = group.get(normal_id), group.get(blocked_id)
            if not normal or not blocked:
                continue
            normal_rows = _stable_scan_values(normal["rendered_state"]["current_set_rows"])
            blocked_rows = _stable_scan_values(blocked["rendered_state"]["current_set_rows"])
            if normal_rows != blocked_rows:
                blocked["issues"].append("last_normal_qa_rows_not_preserved")
            if blocked_id == "cancellation_conflict":
                normal_rendered = normal["rendered_state"]
                conflict_rendered = blocked["rendered_state"]
                normal_detail = normal_rendered.get("qa_detail_contract", {})
                conflict_detail = conflict_rendered.get("qa_detail_contract", {})
                for key in (
                    "selected_iid",
                    "selected_raw",
                    "selected_detail_text",
                ):
                    if normal_detail.get(key) != conflict_detail.get(key):
                        blocked["issues"].append(
                            f"cancellation_conflict_{key}_changed"
                        )
                if normal_rendered.get("presenter_action_gates") != conflict_rendered.get(
                    "presenter_action_gates"
                ):
                    blocked["issues"].append(
                        "cancellation_conflict_presenter_gates_changed"
                    )
                if normal_rendered.get("button_states") != conflict_rendered.get(
                    "button_states"
                ):
                    blocked["issues"].append(
                        "cancellation_conflict_fkey_button_states_changed"
                    )
        for hash_field, issue_prefix in (
            ("sha256", "raw_sha256"),
            ("workbench_sha256", "workbench_sha256"),
        ):
            seen: dict[str, dict[str, Any]] = {}
            for capture in group.values():
                digest = str(capture.get(hash_field) or "")
                if not digest:
                    capture["issues"].append(f"{issue_prefix}_missing")
                    continue
                previous = seen.get(digest)
                if previous is not None:
                    first_state = str(previous.get("state") or "")
                    second_state = str(capture.get("state") or "")
                    issue = (
                        f"{issue_prefix}_reused_across_states:"
                        f"{first_state},{second_state}"
                    )
                    previous["issues"].append(issue)
                    capture["issues"].append(issue)
                else:
                    seen[digest] = capture
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


def _root_hwnd_with_user32(user32: Any, hwnd: int) -> int:
    return int(user32.GetAncestor(int(hwnd), 2) or int(hwnd))


def _window_thread_pid_with_user32(user32: Any, hwnd: int) -> tuple[int, int]:
    if not hwnd:
        return 0, 0
    pid = ctypes.c_ulong(0)
    thread_id = user32.GetWindowThreadProcessId(int(hwnd), ctypes.byref(pid))
    return int(thread_id or 0), int(pid.value)


def _foreground_observation(
    user32: Any,
    *,
    target_hwnd: int,
    target_pid: int,
) -> dict[str, Any]:
    foreground_hwnd = int(user32.GetForegroundWindow() or 0)
    foreground_root_hwnd = (
        _root_hwnd_with_user32(user32, foreground_hwnd)
        if foreground_hwnd
        else 0
    )
    _thread_id, foreground_pid = _window_thread_pid_with_user32(
        user32, foreground_root_hwnd
    )
    return {
        "foreground_hwnd": foreground_hwnd,
        "foreground_root_hwnd": foreground_root_hwnd,
        "foreground_pid": foreground_pid,
        "hwnd_matches": foreground_root_hwnd == int(target_hwnd),
        "pid_matches": foreground_pid == int(target_pid),
    }


def _attempt_foreground_acquisition(
    user32: Any,
    *,
    target_hwnd: int,
    target_pid: int,
    phase: str,
    ordinal: int,
) -> dict[str, Any]:
    api_results: dict[str, Any] = {}
    api_errors: dict[str, str] = {}
    for name, args in (
        ("ShowWindow", (int(target_hwnd), 9)),
        ("BringWindowToTop", (int(target_hwnd),)),
        ("SetForegroundWindow", (int(target_hwnd),)),
        ("SetFocus", (int(target_hwnd),)),
    ):
        try:
            # Win32 return values have API-specific meanings. They are retained
            # only as telemetry; acceptance uses the observation below.
            api_results[name] = int(getattr(user32, name)(*args) or 0)
        except Exception as exc:
            api_results[name] = 0
            api_errors[name] = f"{type(exc).__name__}: {exc}"
    observation = _foreground_observation(
        user32,
        target_hwnd=target_hwnd,
        target_pid=target_pid,
    )
    return {
        "ordinal": int(ordinal),
        "phase": str(phase),
        "api_results": api_results,
        "api_errors": api_errors,
        **observation,
        "passed": bool(observation["hwnd_matches"] and observation["pid_matches"]),
    }


def acquire_win32_foreground(
    target_hwnd: int,
    target_pid: int,
    *,
    user32: Any | None = None,
) -> dict[str, Any]:
    """Acquire foreground with a bounded, key-free Win32 strategy.

    One direct attempt is followed, only when necessary, by one attempt while
    the Tk and current foreground input queues are attached. Detachment always
    runs in ``finally`` and is part of the acceptance result.
    """

    if user32 is None:
        if os.name != "nt":
            return {
                "gate_applicable": False,
                "target_hwnd": int(target_hwnd),
                "target_pid": int(target_pid),
                "strategy": "direct_then_bounded_attach_thread_input",
                "attempt_limit": 2,
                "attempt_count": 0,
                "attempts": [],
                "thread_input": {
                    "attach_attempted": False,
                    "attach_succeeded": False,
                    "detach_attempted": False,
                    "detach_succeeded": False,
                },
                "ownership_acquired": False,
                "thread_input_cleanup_passed": True,
                "passed": False,
                "failure_reason": "foreground acquisition requires Windows",
            }
        user32 = ctypes.windll.user32
    started = time.perf_counter()
    attempts = [
        _attempt_foreground_acquisition(
            user32,
            target_hwnd=target_hwnd,
            target_pid=target_pid,
            phase="direct",
            ordinal=1,
        )
    ]
    thread_input: dict[str, Any] = {
        "attach_attempted": False,
        "attach_succeeded": False,
        "detach_attempted": False,
        "detach_succeeded": False,
        "target_thread_id": 0,
        "foreground_thread_id": 0,
        "attach_error": "",
        "detach_error": "",
    }
    cleanup_passed = True
    if attempts[-1]["passed"] is not True:
        foreground_hwnd = int(user32.GetForegroundWindow() or 0)
        target_thread_id, _target_window_pid = _window_thread_pid_with_user32(
            user32, target_hwnd
        )
        foreground_thread_id, _foreground_window_pid = (
            _window_thread_pid_with_user32(user32, foreground_hwnd)
        )
        thread_input["target_thread_id"] = target_thread_id
        thread_input["foreground_thread_id"] = foreground_thread_id
        attached = False
        same_thread = bool(
            target_thread_id
            and foreground_thread_id
            and target_thread_id == foreground_thread_id
        )
        try:
            if not target_thread_id or not foreground_thread_id:
                raise RuntimeError("cannot resolve input queue thread ids")
            if same_thread:
                thread_input["attach_succeeded"] = True
            else:
                thread_input["attach_attempted"] = True
                attached = bool(
                    user32.AttachThreadInput(
                        int(target_thread_id), int(foreground_thread_id), True
                    )
                )
                thread_input["attach_succeeded"] = attached
                if not attached:
                    raise RuntimeError("AttachThreadInput returned false")
            attempts.append(
                _attempt_foreground_acquisition(
                    user32,
                    target_hwnd=target_hwnd,
                    target_pid=target_pid,
                    phase="attached_input_queues" if not same_thread else "same_input_queue",
                    ordinal=2,
                )
            )
        except Exception as exc:
            thread_input["attach_error"] = f"{type(exc).__name__}: {exc}"
        finally:
            if attached:
                thread_input["detach_attempted"] = True
                try:
                    detached = bool(
                        user32.AttachThreadInput(
                            int(target_thread_id),
                            int(foreground_thread_id),
                            False,
                        )
                    )
                    thread_input["detach_succeeded"] = detached
                    cleanup_passed = detached
                    if not detached:
                        thread_input["detach_error"] = (
                            "AttachThreadInput detach returned false"
                        )
                except Exception as exc:
                    cleanup_passed = False
                    thread_input["detach_error"] = f"{type(exc).__name__}: {exc}"
            elif same_thread:
                thread_input["detach_succeeded"] = True
            else:
                cleanup_passed = not thread_input["attach_succeeded"]
    final_observation = _foreground_observation(
        user32,
        target_hwnd=target_hwnd,
        target_pid=target_pid,
    )
    ownership_acquired = bool(
        final_observation["hwnd_matches"] and final_observation["pid_matches"]
    )
    passed = bool(ownership_acquired and cleanup_passed)
    failure_reason = ""
    if not ownership_acquired:
        failure_reason = (
            "foreground ownership not acquired: "
            f"observed_hwnd={final_observation['foreground_root_hwnd']} "
            f"observed_pid={final_observation['foreground_pid']}"
        )
    elif not cleanup_passed:
        failure_reason = "AttachThreadInput cleanup failed"
    return {
        "gate_applicable": True,
        "target_hwnd": int(target_hwnd),
        "target_pid": int(target_pid),
        "strategy": "direct_then_bounded_attach_thread_input",
        "attempt_limit": 2,
        "attempt_count": len(attempts),
        "duration_ms": int(round((time.perf_counter() - started) * 1000)),
        "attempts": attempts,
        "thread_input": thread_input,
        "final_observation": final_observation,
        "ownership_acquired": ownership_acquired,
        "thread_input_cleanup_passed": cleanup_passed,
        "passed": passed,
        "failure_reason": failure_reason,
    }


def _widget_is_owned_by_root(widget: Any, root: Any) -> bool:
    current = widget
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        if current is root:
            return True
        seen.add(id(current))
        current = getattr(current, "master", None)
    return False


def resolve_capture_focus_target(
    app: Any,
    state_id: str | None = None,
) -> dict[str, Any]:
    """Resolve the exact state-authoritative Tk focus target."""

    entry = getattr(app, "entry", None)
    try:
        entry_enabled = entry is not None and str(entry.cget("state")) not in {
            "disabled",
            "readonly",
        }
    except Exception:
        entry_enabled = False
    if entry_enabled:
        return {
            "kind": "scan_entry",
            "widget": entry,
            "path": str(entry),
            "state": str(state_id or ""),
        }
    history_tree = getattr(app, "history_tree", None)
    if (
        str(state_id or "") == "history_readonly"
        and history_tree is not None
        and _is_mapped(history_tree)
    ):
        return {
            "kind": "history_tree",
            "widget": history_tree,
            "path": str(history_tree),
            "state": str(state_id or ""),
        }
    notice_action = getattr(app, "workflow_notice_action_button", None)
    if notice_action is not None and _is_mapped(notice_action):
        return {
            "kind": "notice_action",
            "widget": notice_action,
            "path": str(notice_action),
            "state": str(state_id or ""),
        }
    if history_tree is not None and _is_mapped(history_tree):
        return {
            "kind": "history_tree",
            "widget": history_tree,
            "path": str(history_tree),
            "state": str(state_id or ""),
        }
    return {
        "kind": "capture_root",
        "widget": app,
        "path": str(app),
        "state": str(state_id or ""),
    }


def settle_capture_foreground(
    app: Any,
    state_id: str | None = None,
) -> dict[str, Any]:
    """Acquire Win32 foreground without injecting input and retain Tk focus."""

    app.deiconify()
    app.lift()
    focus_target = resolve_capture_focus_target(app, state_id)
    focus_widget = focus_target["widget"]
    try:
        focus_widget.focus_force()
    except Exception as exc:
        raise RuntimeError(f"Tk focus setup failed before capture: {exc}") from exc
    pump_tk(app, 80)
    hwnd = _window_root_hwnd(app)
    acquisition = acquire_win32_foreground(hwnd, os.getpid())
    # SetFocus on the native root may move Tk focus away from the scanner
    # entry. Restore the state-authoritative Tk target without typing a key.
    focus_widget.focus_force()
    pump_tk(app, 80)
    if acquisition.get("passed") is not True:
        raise RuntimeError(
            "foreground acquisition failed: "
            + json.dumps(acquisition, ensure_ascii=False, sort_keys=True)
        )
    acquisition["post_foreground_callback_quiescence"] = (
        quiesce_post_foreground_responsive_callbacks(app)
    )
    observed_focus = app.focus_get()
    if not _widget_is_owned_by_root(observed_focus, app):
        raise RuntimeError(
            f"Tk focus escaped capture root: focus={observed_focus!s}"
        )
    if observed_focus is not focus_widget:
        raise RuntimeError(
            "state-authoritative widget did not regain Tk focus before capture: "
            f"kind={focus_target['kind']} expected={focus_widget!s} "
            f"actual={observed_focus!s}"
        )
    acquisition["tk_focus"] = {
        "expected_focus_kind": focus_target["kind"],
        "expected_path": str(focus_widget),
        "observed_path": str(observed_focus),
        "owned_by_capture_root": True,
        "matches_expected_target": observed_focus is focus_widget,
    }
    return acquisition


def _window_root_hwnd(app: Any, *, win32gui_module: Any | None = None) -> int:
    import win32con

    if win32gui_module is None:
        import win32gui as win32gui_module
    hwnd = int(app.winfo_id())
    root_hwnd = int(win32gui_module.GetAncestor(hwnd, win32con.GA_ROOT))
    if not root_hwnd:
        raise RuntimeError(f"cannot resolve GA_ROOT for Tk HWND {hwnd}")
    return root_hwnd


def _monitor_for_window(hwnd: int) -> dict[str, Any]:
    import win32api
    import win32con

    handle = win32api.MonitorFromWindow(hwnd, win32con.MONITOR_DEFAULTTONEAREST)
    info = dict(win32api.GetMonitorInfo(handle))
    x_dpi = ctypes.c_uint(0)
    y_dpi = ctypes.c_uint(0)
    dpi_hresult = int(
        ctypes.windll.shcore.GetDpiForMonitor(
            int(handle), 0, ctypes.byref(x_dpi), ctypes.byref(y_dpi)
        )
    )
    if dpi_hresult != 0:
        raise RuntimeError(
            f"GetDpiForMonitor failed for window {hwnd}: HRESULT={dpi_hresult}"
        )
    return {
        "handle": int(handle),
        "device": str(info.get("Device") or ""),
        "is_primary": bool(
            int(info.get("Flags", 0)) & int(win32con.MONITORINFOF_PRIMARY)
        ),
        "monitor_rect": list(map(int, info["Monitor"])),
        "work_rect": list(map(int, info["Work"])),
        "dpi": [int(x_dpi.value), int(y_dpi.value)],
        "dpi_hresult": dpi_hresult,
    }


def _rect_matches(
    first: Sequence[int], second: Sequence[int], *, tolerance: int = 1
) -> bool:
    return len(first) == len(second) == 4 and all(
        abs(int(left) - int(right)) <= tolerance
        for left, right in zip(first, second)
    )


def visible_pid_toplevel_hwnds(
    pid: int,
    *,
    win32gui_module: Any,
    win32process_module: Any,
) -> tuple[int, ...]:
    handles: list[int] = []

    def callback(hwnd: int, _extra: Any) -> bool:
        _thread, owner_pid = win32process_module.GetWindowThreadProcessId(hwnd)
        if (
            int(owner_pid) == int(pid)
            and win32gui_module.IsWindowVisible(hwnd)
            and int(win32gui_module.GetAncestor(hwnd, 2)) == int(hwnd)
        ):
            handles.append(int(hwnd))
        return True

    win32gui_module.EnumWindows(callback, None)
    return tuple(sorted(handles))


def validate_root_only_toplevels(
    root_hwnd: int, visible_hwnds: Sequence[int]
) -> None:
    normalized = tuple(sorted(int(hwnd) for hwnd in visible_hwnds))
    if normalized != (int(root_hwnd),):
        raise RuntimeError(
            "authoritative root-only capture rejects extra visible PID toplevels: "
            f"root={root_hwnd} visible={normalized}"
        )


def collect_window_capture_contract(
    app: Any,
    monitor_target: Mapping[str, Any],
    *,
    state_id: str | None = None,
    win32gui_module: Any | None = None,
    win32process_module: Any | None = None,
    current_pid: int | None = None,
    monitor_resolver: Any | None = None,
) -> dict[str, Any]:
    """Prove root, foreground, client, and visible toplevels on DISPLAY2."""

    import win32con

    if win32gui_module is None:
        import win32gui as win32gui_module
    if win32process_module is None:
        import win32process as win32process_module
    if current_pid is None:
        current_pid = os.getpid()
    if monitor_resolver is None:
        monitor_resolver = _monitor_for_window
    hwnd = _window_root_hwnd(app, win32gui_module=win32gui_module)
    if not win32gui_module.IsWindow(hwnd):
        raise RuntimeError(f"current Tk root HWND is invalid: {hwnd}")
    _thread_id, root_pid = win32process_module.GetWindowThreadProcessId(hwnd)
    if int(root_pid) != int(current_pid):
        raise RuntimeError(
            f"Tk root HWND belongs to wrong PID: hwnd={hwnd} "
            f"expected={current_pid} actual={root_pid}"
        )
    foreground_hwnd = int(win32gui_module.GetForegroundWindow() or 0)
    foreground_root_hwnd = int(
        win32gui_module.GetAncestor(foreground_hwnd, win32con.GA_ROOT)
        if foreground_hwnd
        else 0
    )
    if not foreground_root_hwnd:
        raise RuntimeError("cannot resolve the current foreground root HWND")
    _foreground_thread, foreground_pid = (
        win32process_module.GetWindowThreadProcessId(foreground_root_hwnd)
    )
    if (
        int(foreground_root_hwnd) != int(hwnd)
        or int(foreground_pid) != int(current_pid)
    ):
        raise RuntimeError(
            "capture root does not own the foreground: "
            f"root={hwnd} foreground_root={foreground_root_hwnd} "
            f"expected_pid={current_pid} foreground_pid={foreground_pid}"
        )
    focus_widget = app.focus_get()
    tk_focus_owned_by_root = _widget_is_owned_by_root(focus_widget, app)
    if not tk_focus_owned_by_root:
        raise RuntimeError(
            f"Tk focus is not owned by the capture root: focus={focus_widget!s}"
        )
    focus_target = resolve_capture_focus_target(app, state_id)
    focus_target_contract_passed = focus_widget is focus_target["widget"]
    if not focus_target_contract_passed:
        raise RuntimeError(
            "Tk focus does not match the state-authoritative target: "
            f"state={state_id!r} kind={focus_target['kind']} "
            f"expected={focus_target['path']} actual={focus_widget!s}"
        )
    entry = getattr(app, "entry", None)
    try:
        scan_entry_enabled = entry is not None and str(entry.cget("state")) not in {
            "disabled",
            "readonly",
        }
    except Exception:
        scan_entry_enabled = False
    tk_focus_matches_scan_entry = bool(
        not scan_entry_enabled or focus_widget is entry
    )
    if not tk_focus_matches_scan_entry:
        raise RuntimeError(
            "enabled scan entry does not own Tk focus: "
            f"entry={entry!s} focus={focus_widget!s}"
        )
    window_rect = list(map(int, win32gui_module.GetWindowRect(hwnd)))
    client_local = list(map(int, win32gui_module.GetClientRect(hwnd)))
    client_left, client_top = win32gui_module.ClientToScreen(
        hwnd, (client_local[0], client_local[1])
    )
    client_right, client_bottom = win32gui_module.ClientToScreen(
        hwnd, (client_local[2], client_local[3])
    )
    client_rect = [
        int(client_left),
        int(client_top),
        int(client_right),
        int(client_bottom),
    ]
    work = list(map(int, monitor_target["work_rect"]))
    monitor = monitor_resolver(hwnd)
    if (
        str(monitor.get("device") or "").casefold()
        != str(monitor_target.get("device") or "").casefold()
        or monitor.get("is_primary") is not False
        or list(monitor.get("work_rect") or ()) != work
        or list(monitor.get("dpi") or ()) != list(TARGET_DISPLAY_DPI)
    ):
        raise RuntimeError(f"current Tk root is not on locked DISPLAY2: {monitor}")
    if not _rect_contains(work, client_rect) or not _rect_contains(
        window_rect, client_rect
    ):
        raise RuntimeError(
            f"client escaped DISPLAY2 or its root window: window={window_rect} "
            f"client={client_rect} work={work}"
        )

    visible_toplevels: list[dict[str, Any]] = []

    def enum_callback(candidate: int, _extra: Any) -> bool:
        try:
            _tid, pid = win32process_module.GetWindowThreadProcessId(candidate)
            if int(pid) != int(current_pid):
                return True
            if not win32gui_module.IsWindowVisible(candidate):
                return True
            if int(win32gui_module.GetAncestor(candidate, 2)) != int(candidate):
                return True
            rect = list(map(int, win32gui_module.GetWindowRect(candidate)))
            candidate_client_local = list(
                map(int, win32gui_module.GetClientRect(candidate))
            )
            candidate_client_left, candidate_client_top = (
                win32gui_module.ClientToScreen(
                    candidate,
                    (candidate_client_local[0], candidate_client_local[1]),
                )
            )
            candidate_client_right, candidate_client_bottom = (
                win32gui_module.ClientToScreen(
                    candidate,
                    (candidate_client_local[2], candidate_client_local[3]),
                )
            )
            candidate_client_rect = [
                int(candidate_client_left),
                int(candidate_client_top),
                int(candidate_client_right),
                int(candidate_client_bottom),
            ]
            candidate_monitor = monitor_resolver(int(candidate))
            contained = bool(
                _rect_contains(work, candidate_client_rect)
                and _rect_contains(rect, candidate_client_rect)
                and str(candidate_monitor.get("device") or "").casefold()
                == str(monitor_target.get("device") or "").casefold()
                and candidate_monitor.get("is_primary") is False
                and list(candidate_monitor.get("work_rect") or ()) == work
                and list(candidate_monitor.get("dpi") or ())
                == list(TARGET_DISPLAY_DPI)
            )
            visible_toplevels.append(
                {
                    "hwnd": int(candidate),
                    "rect": rect,
                    "client_rect": candidate_client_rect,
                    "contained_on_display2": contained,
                }
            )
        except Exception as exc:
            visible_toplevels.append(
                {
                    "hwnd": int(candidate),
                    "contained_on_display2": False,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
        return True

    win32gui_module.EnumWindows(enum_callback, None)
    visible_toplevels.sort(key=lambda item: int(item["hwnd"]))
    if not any(item["hwnd"] == hwnd for item in visible_toplevels):
        raise RuntimeError("current Tk root missing from visible PID toplevel inventory")
    escaped = [
        item for item in visible_toplevels if not item.get("contained_on_display2")
    ]
    if escaped:
        raise RuntimeError(
            f"visible current-PID toplevel escaped DISPLAY2: {escaped}"
        )
    validate_root_only_toplevels(
        hwnd, [item["hwnd"] for item in visible_toplevels]
    )
    client_size = [
        client_rect[2] - client_rect[0],
        client_rect[3] - client_rect[1],
    ]
    logical_client_size = [int(app.winfo_width()), int(app.winfo_height())]
    if logical_client_size != client_size:
        raise RuntimeError(
            f"Tk/Win32 client size mismatch at 96 DPI: "
            f"tk={logical_client_size} win32={client_size}"
        )
    tk_scaling = observe_target_tk_scaling(
        app,
        int(monitor_target["dpi"][0]),
        hwnd=hwnd,
    )
    return {
        "status": "PASS",
        "current_pid": int(current_pid),
        "root_hwnd": hwnd,
        "root_pid": int(root_pid),
        "foreground_hwnd": foreground_hwnd,
        "foreground_root_hwnd": foreground_root_hwnd,
        "foreground_pid": int(foreground_pid),
        "foreground_root_matches_capture_root": True,
        "foreground_pid_matches_process": True,
        "tk_focus_path": str(focus_widget or ""),
        "tk_focus_owned_by_root": True,
        "expected_focus_kind": str(focus_target["kind"]),
        "expected_focus_path": str(focus_target["path"]),
        "focus_target_contract_passed": True,
        "scan_entry_path": str(entry or ""),
        "scan_entry_enabled": scan_entry_enabled,
        "tk_focus_matches_scan_entry_when_enabled": tk_focus_matches_scan_entry,
        "window_rect": window_rect,
        "window_size": [
            window_rect[2] - window_rect[0],
            window_rect[3] - window_rect[1],
        ],
        "client_rect": client_rect,
        "client_size": client_size,
        "tk_scaling": tk_scaling,
        "client_offset_in_window": [
            client_rect[0] - window_rect[0],
            client_rect[1] - window_rect[1],
        ],
        "tk_client_size": logical_client_size,
        "monitor": monitor,
        "visible_pid_toplevels": visible_toplevels,
        "all_visible_pid_toplevels_contained": True,
    }


def validate_window_capture_pair(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    *,
    captured_pixel_size: Sequence[int],
    captured_screen_bbox: Sequence[int],
    requested_client_size: Sequence[int] | None = None,
    requested_outer_size: Sequence[int] | None = None,
    capture_source: str = AUTHORITATIVE_CAPTURE_SOURCE,
) -> dict[str, Any]:
    # ``requested_outer_size`` remains a keyword-only compatibility alias for
    # older unit tests. Schema 5 always interprets requested matrix dimensions
    # as client pixels.
    requested_size = (
        requested_client_size
        if requested_client_size is not None
        else requested_outer_size
    )
    if requested_size is None:
        raise ValueError("requested_client_size is required")
    stable_keys = (
        "current_pid",
        "root_hwnd",
        "root_pid",
        "foreground_root_hwnd",
        "foreground_pid",
        "tk_focus_path",
        "tk_focus_owned_by_root",
        "expected_focus_kind",
        "expected_focus_path",
        "focus_target_contract_passed",
        "scan_entry_path",
        "scan_entry_enabled",
        "tk_focus_matches_scan_entry_when_enabled",
        "window_rect",
        "window_size",
        "client_rect",
        "client_size",
        "client_offset_in_window",
        "visible_pid_toplevels",
    )
    client_geometry_keys = (
        "root_hwnd",
        "window_rect",
        "client_rect",
        "client_size",
        "client_offset_in_window",
    )
    checks = {
        "before_client_size_matches_request": (
            list(before.get("client_size") or ()) == list(requested_size)
        ),
        "after_client_size_matches_request": (
            list(after.get("client_size") or ()) == list(requested_size)
        ),
        "client_geometry_stable_during_capture": all(
            before.get(key) == after.get(key) for key in client_geometry_keys
        ),
        "captured_pixels_match_client_size": (
            list(captured_pixel_size) == list(before.get("client_size") or ())
        ),
        "capture_screen_bbox_matches_before_client_rect": (
            list(captured_screen_bbox) == list(before.get("client_rect") or ())
        ),
        "capture_screen_bbox_matches_after_client_rect": (
            list(captured_screen_bbox) == list(after.get("client_rect") or ())
        ),
        "visible_screen_capture_used": (
            str(capture_source) == AUTHORITATIVE_CAPTURE_SOURCE
        ),
        "root_hwnd_stable": before.get("root_hwnd") == after.get("root_hwnd"),
        "foreground_root_hwnd_stable": (
            before.get("foreground_root_hwnd")
            == after.get("foreground_root_hwnd")
        ),
        "foreground_root_hwnd_matches_capture_root": bool(
            before.get("foreground_root_matches_capture_root") is True
            and after.get("foreground_root_matches_capture_root") is True
            and before.get("foreground_root_hwnd") == before.get("root_hwnd")
            and after.get("foreground_root_hwnd") == after.get("root_hwnd")
        ),
        "foreground_pid_stable": (
            before.get("foreground_pid") == after.get("foreground_pid")
        ),
        "foreground_pid_matches_process": bool(
            before.get("foreground_pid") == before.get("current_pid")
            and after.get("foreground_pid") == after.get("current_pid")
        ),
        "foreground_pid_stable_and_matches_process": bool(
            before.get("foreground_pid") == after.get("foreground_pid")
            and before.get("foreground_pid") == before.get("current_pid")
            and after.get("foreground_pid") == after.get("current_pid")
        ),
        "tk_focus_owned_by_capture_root": bool(
            before.get("tk_focus_owned_by_root") is True
            and after.get("tk_focus_owned_by_root") is True
        ),
        "tk_focus_path_stable": (
            before.get("tk_focus_path") == after.get("tk_focus_path")
        ),
        "expected_focus_target_stable": bool(
            before.get("expected_focus_kind") == after.get("expected_focus_kind")
            and before.get("expected_focus_path") == after.get("expected_focus_path")
        ),
        "focus_target_contract_passed": bool(
            before.get("focus_target_contract_passed") is True
            and after.get("focus_target_contract_passed") is True
            and before.get("tk_focus_path") == before.get("expected_focus_path")
            and after.get("tk_focus_path") == after.get("expected_focus_path")
        ),
        "scan_entry_focus_contract_passed": bool(
            before.get("tk_focus_matches_scan_entry_when_enabled") is True
            and after.get("tk_focus_matches_scan_entry_when_enabled") is True
        ),
        "window_contract_fields_stable": all(
            before.get(key) == after.get(key) for key in stable_keys
        ),
        "before_pid_toplevels_contained": (
            before.get("all_visible_pid_toplevels_contained") is True
        ),
        "after_pid_toplevels_contained": (
            after.get("all_visible_pid_toplevels_contained") is True
        ),
    }
    passed = all(checks.values())
    return {
        "status": "PASS" if passed else "FAIL",
        "gate_applicable": True,
        "requested_size_semantics": "client-area-pixels",
        "requested_client_size": list(map(int, requested_size)),
        "captured_pixel_semantics": "client-area-pixels",
        "capture_source": str(capture_source),
        "captured_client_pixel_size": list(map(int, captured_pixel_size)),
        "captured_screen_bbox": list(map(int, captured_screen_bbox)),
        "checks": checks,
        "passed": passed,
        "before": dict(before),
        "after": dict(after),
    }


def _pending_after_ids(app: Any) -> tuple[str, ...]:
    try:
        return tuple(str(value) for value in app.tk.splitlist(app.tk.call("after", "info")))
    except Exception as exc:
        raise RuntimeError(
            f"cannot query Tcl scheduled jobs: {type(exc).__name__}: {exc}"
        ) from exc


def quiesce_scheduled_jobs(app: Any) -> dict[str, Any]:
    """Cancel capture-irrelevant scheduled jobs and require an empty queue."""

    pending = _pending_after_ids(app)
    cancelled: list[str] = []
    failures: list[str] = []
    for after_id in pending:
        try:
            app.after_cancel(after_id)
            cancelled.append(after_id)
        except Exception as exc:
            failures.append(f"{after_id}:{type(exc).__name__}")
    app.update_idletasks()
    remaining = _pending_after_ids(app)
    if failures or remaining:
        raise RuntimeError(
            "scheduled job quiescence failed: "
            f"failures={failures} remaining={remaining}"
        )
    return {
        "status": "PASS",
        "pending_before": len(pending),
        "cancelled": len(cancelled),
        "remaining_after": 0,
    }


def cancel_pending_responsive_callbacks(app: Any) -> dict[str, Any]:
    """Clear both responsive timers before any direct layout or event update."""

    attributes = (
        "_operator_layout_settle_after_id",
        "_responsive_after_id",
    )
    cancelled: list[str] = []
    failures: list[str] = []
    seen: set[str] = set()
    for attribute in attributes:
        pending = app.__dict__.get(attribute)
        try:
            if pending and str(pending) not in seen:
                app.after_cancel(pending)
                cancelled.append(str(pending))
                seen.add(str(pending))
        except Exception as exc:
            failures.append(f"{attribute}:{type(exc).__name__}:{exc}")
        finally:
            app.__dict__[attribute] = None
    if failures:
        raise RuntimeError(
            "responsive callback cancellation failed before direct settle: "
            + ",".join(failures)
        )
    return {
        "status": "PASS",
        "attributes_cleared": list(attributes),
        "cancelled_ids": cancelled,
    }


def quiesce_post_foreground_responsive_callbacks(
    app: Any, *, attempt_limit: int = 3
) -> dict[str, Any]:
    """Cancel only known Configure/layout timers created while raising the window."""

    if int(attempt_limit) < 1:
        raise ValueError("post-foreground callback attempt_limit must be positive")
    attempts: list[dict[str, Any]] = []
    attributes = (
        "_operator_layout_settle_after_id",
        "_responsive_after_id",
    )
    for ordinal in range(1, int(attempt_limit) + 1):
        pending = _pending_after_ids(app)
        if not pending:
            return {
                "status": "PASS",
                "attempt_limit": int(attempt_limit),
                "attempt_count": len(attempts),
                "attempts": attempts,
                "remaining_after": 0,
            }
        known = {
            str(value)
            for attribute in attributes
            if (value := app.__dict__.get(attribute))
        }
        unexpected = sorted(set(pending) - known)
        if unexpected:
            raise RuntimeError(
                "unexpected scheduled jobs appeared after foreground acquisition: "
                f"pending={pending} known={sorted(known)} unexpected={unexpected}"
            )
        cancellation = cancel_pending_responsive_callbacks(app)
        app.update_idletasks()
        attempts.append(
            {
                "ordinal": ordinal,
                "pending_before": list(pending),
                "known_responsive_ids": sorted(known),
                "cancellation": cancellation,
            }
        )
    remaining = _pending_after_ids(app)
    if remaining:
        raise RuntimeError(
            "known responsive callbacks did not quiesce after foreground acquisition: "
            f"attempt_limit={attempt_limit} remaining={remaining}"
        )
    return {
        "status": "PASS",
        "attempt_limit": int(attempt_limit),
        "attempt_count": len(attempts),
        "attempts": attempts,
        "remaining_after": 0,
    }


def settle_responsive_layout(
    app: Any,
    *,
    update_window: Any | None = None,
    dwm_flush: Any | None = None,
    invalidate_rect: Any | None = None,
    hwnd: int | None = None,
) -> dict[str, Any]:
    responsive_callback_cancellation = cancel_pending_responsive_callbacks(app)
    method_name = ""
    method = getattr(app, "_apply_operator_responsive_layout", None)
    if callable(method):
        method(settle=True)
        method_name = "_apply_operator_responsive_layout(settle=True)"
    else:
        method = getattr(app, "_settle_operator_responsive_layout", None)
        if not callable(method):
            raise RuntimeError("direct responsive settle method is unavailable")
        method()
        method_name = "_settle_operator_responsive_layout"
    scheduled_job_quiescence = quiesce_scheduled_jobs(app)
    app.update()
    app.update_idletasks()
    remaining_after_full_update = _pending_after_ids(app)
    if remaining_after_full_update:
        raise RuntimeError(
            "scheduled jobs appeared after post-quiescence full update: "
            f"{remaining_after_full_update}"
        )
    if hwnd is None and os.name == "nt":
        hwnd = _window_root_hwnd(app)
    if update_window is None and os.name == "nt":
        update_window = ctypes.windll.user32.UpdateWindow
    if invalidate_rect is None and os.name == "nt":
        invalidate_rect = ctypes.windll.user32.InvalidateRect
    if dwm_flush is None and os.name == "nt":
        dwm_flush = ctypes.windll.dwmapi.DwmFlush
    update_result = None
    invalidate_result = None
    flush_result = None
    if callable(invalidate_rect):
        invalidate_result = int(invalidate_rect(int(hwnd or 0), None, False))
        if invalidate_result != 1:
            raise RuntimeError(
                f"InvalidateRect failed before UpdateWindow: result={invalidate_result}"
            )
    if callable(update_window):
        update_result = int(update_window(int(hwnd or 0)))
        if update_result != 1:
            raise RuntimeError(f"UpdateWindow failed: result={update_result}")
    if callable(dwm_flush):
        flush_result = int(dwm_flush())
        if flush_result != 0:
            raise RuntimeError(f"DwmFlush failed: HRESULT={flush_result}")
    app.update_idletasks()
    remaining_after_paint = _pending_after_ids(app)
    if remaining_after_paint:
        raise RuntimeError(
            "scheduled jobs appeared after authoritative paint: "
            f"{remaining_after_paint}"
        )
    return {
        "method": method_name,
        "responsive_callback_cancellation": responsive_callback_cancellation,
        "scheduled_job_quiescence": scheduled_job_quiescence,
        "full_app_update_called": True,
        "pending_after_full_update": 0,
        "invalidate_rect_result": invalidate_result,
        "update_window_result": update_result,
        "dwm_flush_hresult": flush_result,
        "pending_after_authoritative_paint": 0,
        "status": "PASS",
    }


def observe_target_tk_scaling(
    app: Any,
    target_dpi: int,
    *,
    hwnd: int | None = None,
    user32: Any | None = None,
) -> dict[str, Any]:
    """Fail closed unless Tk pixels-per-point matches the capture monitor.

    Tk initializes its global point-to-pixel conversion from the monitor that
    owns the root at interpreter creation.  Moving a withdrawn root from a
    144-DPI primary display to DISPLAY2 does not update that conversion, even
    though ``GetDpiForWindow`` changes to 96.  The capture contract therefore
    observes both values independently.
    """

    dpi = int(target_dpi)
    if dpi <= 0:
        raise RuntimeError(f"invalid target DPI for Tk scaling: {target_dpi}")
    expected_scaling = dpi / 72.0
    observed_scaling = float(app.tk.call("tk", "scaling"))
    pixels_per_inch = float(app.winfo_fpixels("1i"))
    if abs(observed_scaling - expected_scaling) > 0.01:
        raise RuntimeError(
            "Tk scaling does not match DISPLAY2 DPI: "
            f"expected={expected_scaling:.6f} actual={observed_scaling:.6f}"
        )
    if abs(pixels_per_inch - dpi) > 0.75:
        raise RuntimeError(
            "Tk physical-inch conversion does not match DISPLAY2 DPI: "
            f"expected={dpi} actual={pixels_per_inch:.6f}"
        )
    window_dpi = None
    if hwnd is not None:
        if user32 is None:
            if os.name != "nt":
                raise RuntimeError("GetDpiForWindow attestation requires Windows")
            user32 = ctypes.windll.user32
        window_dpi = int(user32.GetDpiForWindow(int(hwnd)))
        if window_dpi != dpi:
            raise RuntimeError(
                "Tk window DPI does not match DISPLAY2 DPI: "
                f"expected={dpi} actual={window_dpi} hwnd={hwnd}"
            )
    return {
        "status": "PASS",
        "target_dpi": dpi,
        "expected_tk_scaling": expected_scaling,
        "observed_tk_scaling": observed_scaling,
        "pixels_per_inch": pixels_per_inch,
        "window_dpi": window_dpi,
    }


def configure_target_tk_scaling(app: Any, target_dpi: int) -> dict[str, Any]:
    """Pin Tk scaling before the application creates any cached fonts."""

    before_scaling = float(app.tk.call("tk", "scaling"))
    before_pixels_per_inch = float(app.winfo_fpixels("1i"))
    app.tk.call("tk", "scaling", int(target_dpi) / 72.0)
    observed = observe_target_tk_scaling(app, int(target_dpi))
    return {
        **observed,
        "before_tk_scaling": before_scaling,
        "before_pixels_per_inch": before_pixels_per_inch,
        "configured_before_widget_creation": True,
    }


def place_hidden_on_work_area(
    app: Any,
    monitor_target: Mapping[str, Any],
    *,
    win32gui_module: Any | None = None,
    win32process_module: Any | None = None,
) -> dict[str, Any]:
    if win32gui_module is None:
        import win32gui as win32gui_module
    if win32process_module is None:
        import win32process as win32process_module
    work = list(map(int, monitor_target["work_rect"]))
    left, top, right, bottom = work
    if (left, top) == (0, 0):
        raise RuntimeError("primary-origin +0+0 placement is forbidden")
    width, height = right - left, bottom - top
    hwnd = _window_root_hwnd(app, win32gui_module=win32gui_module)
    visible_before = bool(win32gui_module.IsWindowVisible(hwnd))
    if visible_before:
        raise RuntimeError("app became visible before DISPLAY2 placement")
    visible_pid_before = visible_pid_toplevel_hwnds(
        os.getpid(),
        win32gui_module=win32gui_module,
        win32process_module=win32process_module,
    )
    if visible_pid_before:
        raise RuntimeError(
            "startup transient toplevel became visible before root placement: "
            f"{visible_pid_before}"
        )
    app.geometry(f"{width}x{height}{left:+d}{top:+d}")
    app.update_idletasks()
    win32gui_module.MoveWindow(hwnd, left, top, width, height, True)
    hidden_rect = list(map(int, win32gui_module.GetWindowRect(hwnd)))
    visible_after_hidden_move = bool(win32gui_module.IsWindowVisible(hwnd))
    if visible_after_hidden_move or not _rect_matches(hidden_rect, work):
        raise RuntimeError(
            f"hidden DISPLAY2 placement failed: visible={visible_after_hidden_move} "
            f"rect={hidden_rect} expected={work}"
        )
    hidden_tk_scaling = observe_target_tk_scaling(
        app,
        int(monitor_target["dpi"][0]),
        hwnd=hwnd,
    )
    toplevel_guard = release_previsible_toplevel_guard(
        app, reject_created=True
    )
    app.deiconify()
    win32gui_module.MoveWindow(hwnd, left, top, width, height, True)
    app.update_idletasks()
    visible_rect = list(map(int, win32gui_module.GetWindowRect(hwnd)))
    visible_after_show = bool(win32gui_module.IsWindowVisible(hwnd))
    visible_pid_after = visible_pid_toplevel_hwnds(
        os.getpid(),
        win32gui_module=win32gui_module,
        win32process_module=win32process_module,
    )
    validate_root_only_toplevels(hwnd, visible_pid_after)
    monitor = _monitor_for_window(hwnd)
    monitor_ok = bool(
        monitor["device"].casefold()
        == str(monitor_target["device"]).casefold()
        and monitor["is_primary"] is False
        and monitor["work_rect"] == work
        and monitor["monitor_rect"] == list(TARGET_DISPLAY_MONITOR_AREA)
        and monitor["dpi"] == list(TARGET_DISPLAY_DPI)
    )
    if (
        not visible_after_show
        or not _rect_matches(visible_rect, work)
        or not monitor_ok
    ):
        raise RuntimeError(
            f"first visible DISPLAY2 placement failed: visible={visible_after_show} "
            f"rect={visible_rect} expected={work} monitor={monitor}"
        )
    visible_tk_scaling = observe_target_tk_scaling(
        app,
        int(monitor_target["dpi"][0]),
        hwnd=hwnd,
    )
    return {
        "status": "PASS",
        "hwnd": hwnd,
        "visible_before_move": visible_before,
        "visible_after_hidden_move": visible_after_hidden_move,
        "hidden_rect": hidden_rect,
        "visible_after_show": visible_after_show,
        "visible_pid_toplevels_before_show": list(visible_pid_before),
        "visible_pid_toplevels_after_show": list(visible_pid_after),
        "previsible_toplevel_guard": toplevel_guard,
        "visible_rect": visible_rect,
        "device": monitor["device"],
        "is_primary": monitor["is_primary"],
        "hidden_tk_scaling": hidden_tk_scaling,
        "visible_tk_scaling": visible_tk_scaling,
    }


def _win32_window_client_geometry(
    hwnd: int,
    *,
    win32gui_module: Any,
) -> dict[str, Any]:
    window_rect = list(map(int, win32gui_module.GetWindowRect(hwnd)))
    client_local = list(map(int, win32gui_module.GetClientRect(hwnd)))
    client_left, client_top = win32gui_module.ClientToScreen(
        hwnd, (client_local[0], client_local[1])
    )
    client_right, client_bottom = win32gui_module.ClientToScreen(
        hwnd, (client_local[2], client_local[3])
    )
    client_rect = [
        int(client_left),
        int(client_top),
        int(client_right),
        int(client_bottom),
    ]
    return {
        "hwnd": int(hwnd),
        "window_rect": window_rect,
        "window_size": [
            window_rect[2] - window_rect[0],
            window_rect[3] - window_rect[1],
        ],
        "client_rect": client_rect,
        "client_size": [
            client_rect[2] - client_rect[0],
            client_rect[3] - client_rect[1],
        ],
        "nonclient_insets": [
            client_rect[0] - window_rect[0],
            client_rect[1] - window_rect[1],
            window_rect[2] - client_rect[2],
            window_rect[3] - client_rect[3],
        ],
    }


def _align_requested_client_to_work_area(
    app: Any,
    size: tuple[int, int],
    work: Sequence[int],
    *,
    win32gui_module: Any,
    attempt_limit: int = 4,
) -> dict[str, Any]:
    width, height = map(int, size)
    left, top = int(work[0]), int(work[1])
    attempts: list[dict[str, Any]] = []
    for ordinal in range(1, int(attempt_limit) + 1):
        hwnd = _window_root_hwnd(app, win32gui_module=win32gui_module)
        before = _win32_window_client_geometry(
            hwnd, win32gui_module=win32gui_module
        )
        inset_left, inset_top, inset_right, inset_bottom = before[
            "nonclient_insets"
        ]
        desired_window_rect = [
            left - inset_left,
            top - inset_top,
            left + width + inset_right,
            top + height + inset_bottom,
        ]
        moved = bool(
            win32gui_module.MoveWindow(
                hwnd,
                desired_window_rect[0],
                desired_window_rect[1],
                desired_window_rect[2] - desired_window_rect[0],
                desired_window_rect[3] - desired_window_rect[1],
                True,
            )
        )
        app.update_idletasks()
        fresh_hwnd = _window_root_hwnd(app, win32gui_module=win32gui_module)
        after = _win32_window_client_geometry(
            fresh_hwnd, win32gui_module=win32gui_module
        )
        passed = bool(
            after["client_rect"] == [left, top, left + width, top + height]
            and after["client_size"] == [width, height]
        )
        attempts.append(
            {
                "ordinal": ordinal,
                "source_hwnd": int(hwnd),
                "fresh_hwnd": int(fresh_hwnd),
                "move_window_returned": moved,
                "before": before,
                "desired_window_rect": desired_window_rect,
                "after": after,
                "passed": passed,
            }
        )
        if passed:
            return {
                "status": "PASS",
                "requested_client_size": [width, height],
                "requested_client_rect": [left, top, left + width, top + height],
                "attempt_limit": int(attempt_limit),
                "attempt_count": len(attempts),
                "attempts": attempts,
                "final": after,
                "passed": True,
            }
    raise RuntimeError(
        "client-area alignment failed: "
        + json.dumps(attempts, ensure_ascii=False, sort_keys=True)
    )


def _configure_size(
    app: Any,
    size: tuple[int, int],
    monitor_target: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    import win32gui

    if monitor_target is None:
        monitor_target = resolve_capture_monitor(
            TARGET_DISPLAY_DEVICE,
            TARGET_DISPLAY_WORK_AREA,
        )
    work = list(map(int, monitor_target["work_rect"]))
    work_width, work_height = work[2] - work[0], work[3] - work[1]
    width, height = map(int, size)
    if width > work_width or height > work_height:
        raise RuntimeError(
            f"requested client size {width}x{height} exceeds DISPLAY2 work area "
            f"{work_width}x{work_height}"
        )
    hwnd = _window_root_hwnd(app, win32gui_module=win32gui)
    previsible_placement = None
    if not win32gui.IsWindowVisible(hwnd):
        previsible_placement = place_hidden_on_work_area(
            app,
            monitor_target,
            win32gui_module=win32gui,
        )
    app.state("normal")
    app.resizable(True, True)
    app.geometry(f"{width}x{height}")
    app.update_idletasks()
    # Tk can recreate the native wrapper while changing to normal/resizable.
    # Never reuse the pre-state-change HWND for the visible move.
    hwnd = _window_root_hwnd(app, win32gui_module=win32gui)
    if not win32gui.IsWindow(hwnd):
        raise RuntimeError("Tk root HWND is invalid after normal-state transition")
    initial_alignment = _align_requested_client_to_work_area(
        app,
        size,
        work,
        win32gui_module=win32gui,
    )
    pump_tk(app, 160)
    hwnd = _window_root_hwnd(app, win32gui_module=win32gui)
    settle = settle_responsive_layout(app, hwnd=hwnd)
    final_alignment = _align_requested_client_to_work_area(
        app,
        size,
        work,
        win32gui_module=win32gui,
    )
    app.update_idletasks()
    hwnd = _window_root_hwnd(app, win32gui_module=win32gui)
    geometry = _win32_window_client_geometry(
        hwnd, win32gui_module=win32gui
    )
    monitor = _monitor_for_window(hwnd)
    placement_ok = bool(
        geometry["client_size"] == [width, height]
        and _rect_contains(work, geometry["client_rect"])
        and _rect_contains(geometry["window_rect"], geometry["client_rect"])
        and monitor["device"].casefold()
        == str(monitor_target["device"]).casefold()
        and monitor["is_primary"] is False
        and monitor["work_rect"] == work
        and monitor["monitor_rect"] == list(TARGET_DISPLAY_MONITOR_AREA)
        and monitor["dpi"] == list(TARGET_DISPLAY_DPI)
    )
    if not placement_ok:
        raise RuntimeError(
            "DISPLAY2 client size placement failed: "
            f"geometry={geometry} requested={[width, height]} work={work} "
            f"monitor={monitor}"
        )
    return {
        "status": "PASS",
        "requested_size_semantics": "client-area-pixels",
        "requested_client_size": [width, height],
        "window_rect": geometry["window_rect"],
        "client_rect": geometry["client_rect"],
        "client_size": geometry["client_size"],
        "client_contained_in_work_area": True,
        "outer_window_may_extend_beyond_work_area": True,
        "monitor": monitor,
        "previsible_placement": previsible_placement,
        "initial_client_alignment": initial_alignment,
        "final_client_alignment": final_alignment,
        "settle": settle,
    }


def _apply_scale(app: Any, scale: float) -> None:
    app.scale_factor = float(scale)
    for name in ("_update_ui_scaling", "_apply_operator_layout"):
        method = getattr(app, name, None)
        if callable(method):
            method()
            break
    pump_tk(app, 180)
    settle_responsive_layout(app)


def _wait_until_ready(app: Any, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        app.update()
        if bool(getattr(app, "initialized_successfully", False)):
            pump_tk(app, 220)
            return
        time.sleep(0.02)
    raise TimeoutError("Label Match did not initialize within capture timeout")


def _make_capture_app(
    module: Any,
    settings: dict[str, Any],
    *,
    target_dpi: int | None = None,
) -> Any:
    class CaptureLabelMatch(module.Label_Match):
        def _load_app_settings(self) -> dict[str, Any]:
            # JSON round-trip detaches nested dictionaries from this harness.
            return json.loads(json.dumps(settings, ensure_ascii=False))

    tk_class = module.tk.Tk
    original_init = tk_class.__init__
    original_state = tk_class.state
    original_deiconify = tk_class.deiconify
    toplevel_class = module.tk.Toplevel
    original_toplevel_init = toplevel_class.__init__
    original_toplevel_state = toplevel_class.state
    original_toplevel_deiconify = toplevel_class.deiconify
    constructor_toplevels: list[Any] = []

    def hidden_init(instance: Any, *args: Any, **kwargs: Any) -> None:
        original_init(instance, *args, **kwargs)
        instance.withdraw()
        if target_dpi is not None:
            # This must run before Label_Match.__init__ creates any tuple-font
            # or ttk style.  Tcl caches those point-font pixel metrics and a
            # later DISPLAY2 move cannot repair an already cached 144-DPI font.
            instance._capture_constructor_tk_scaling = (
                configure_target_tk_scaling(instance, int(target_dpi))
            )

    def guarded_state(instance: Any, new_state: str | None = None) -> Any:
        if new_state in {"normal", "zoomed"}:
            return original_state(instance)
        if new_state is None:
            return original_state(instance)
        return original_state(instance, new_state)

    def guarded_deiconify(instance: Any) -> None:
        return None

    def hidden_toplevel_init(instance: Any, *args: Any, **kwargs: Any) -> None:
        original_toplevel_init(instance, *args, **kwargs)
        constructor_toplevels.append(instance)
        instance.withdraw()

    def guarded_toplevel_state(
        instance: Any, new_state: str | None = None
    ) -> Any:
        if new_state in {"normal", "zoomed"}:
            return original_toplevel_state(instance)
        if new_state is None:
            return original_toplevel_state(instance)
        return original_toplevel_state(instance, new_state)

    def guarded_toplevel_deiconify(instance: Any) -> None:
        return None

    tk_class.__init__ = hidden_init
    tk_class.state = guarded_state
    tk_class.deiconify = guarded_deiconify
    toplevel_class.__init__ = hidden_toplevel_init
    toplevel_class.state = guarded_toplevel_state
    toplevel_class.deiconify = guarded_toplevel_deiconify
    app = None
    try:
        app = CaptureLabelMatch(run_tests=True)
    finally:
        tk_class.__init__ = original_init
        tk_class.state = original_state
        tk_class.deiconify = original_deiconify
        if app is None:
            toplevel_class.__init__ = original_toplevel_init
            toplevel_class.state = original_toplevel_state
            toplevel_class.deiconify = original_toplevel_deiconify
    app._capture_previsible_toplevel_guard = {
        "class": toplevel_class,
        "original_init": original_toplevel_init,
        "original_state": original_toplevel_state,
        "original_deiconify": original_toplevel_deiconify,
        "created": constructor_toplevels,
    }
    if constructor_toplevels:
        try:
            release_previsible_toplevel_guard(app, reject_created=True)
        finally:
            try:
                app.destroy()
            except Exception:
                pass
    try:
        if os.name == "nt":
            import win32gui

            hwnd = _window_root_hwnd(app, win32gui_module=win32gui)
            if win32gui.IsWindowVisible(hwnd):
                raise RuntimeError("app became visible before DISPLAY2 placement")
    except Exception:
        release_previsible_toplevel_guard(app, reject_created=False)
        try:
            app.destroy()
        finally:
            raise
    return app


def release_previsible_toplevel_guard(
    app: Any, *, reject_created: bool
) -> dict[str, Any]:
    guard = app.__dict__.pop("_capture_previsible_toplevel_guard", None)
    if not guard:
        return {"status": "NOT_ACTIVE", "created_toplevel_count": 0}
    toplevel_class = guard["class"]
    toplevel_class.__init__ = guard["original_init"]
    toplevel_class.state = guard["original_state"]
    toplevel_class.deiconify = guard["original_deiconify"]
    created = tuple(guard.get("created") or ())
    if reject_created and created:
        raise RuntimeError(
            "constructor/previsible phase created forbidden extra Toplevels: "
            f"count={len(created)}"
        )
    return {
        "status": "PASS",
        "created_toplevel_count": len(created),
        "constructor_toplevels_rejected": bool(reject_created),
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def image_region_sha256(image: Image.Image, bbox: Sequence[int]) -> str:
    if len(bbox) != 4:
        raise ValueError(f"invalid image region bbox: {bbox}")
    left, top, right, bottom = map(int, bbox)
    if (
        left < 0
        or top < 0
        or right <= left
        or bottom <= top
        or right > image.width
        or bottom > image.height
    ):
        raise ValueError(
            f"image region is outside capture: bbox={bbox} image={image.size}"
        )
    crop = image.convert("RGB").crop((left, top, right, bottom))
    digest = hashlib.sha256()
    digest.update(f"RGB:{crop.width}x{crop.height}\n".encode("ascii"))
    digest.update(crop.tobytes())
    return digest.hexdigest()


def _round_trip_check(
    app: Any,
    compact: tuple[int, int],
    wide: tuple[int, int],
    fixture: StateFixture,
    monitor_target: Mapping[str, Any],
) -> dict[str, Any]:
    _configure_size(app, compact, monitor_target)
    view, _ = apply_state_fixture(app, fixture)
    pump_tk(app, 500)
    settle_responsive_layout(app)
    before = collect_ui_geometry(app, fixture)["structure"]["layout_signature"]
    before_rendered = collect_rendered_state(app, fixture, view)
    _configure_size(app, wide, monitor_target)
    wide_view, _ = apply_state_fixture(app, fixture)
    pump_tk(app, 500)
    settle_responsive_layout(app)
    wide_signature = collect_ui_geometry(app, fixture)["structure"]["layout_signature"]
    wide_rendered = collect_rendered_state(app, fixture, wide_view)
    _configure_size(app, compact, monitor_target)
    after_view, _ = apply_state_fixture(app, fixture)
    pump_tk(app, 500)
    settle_responsive_layout(app)
    after = collect_ui_geometry(app, fixture)["structure"]["layout_signature"]
    after_rendered = collect_rendered_state(app, fixture, after_view)
    issues = compare_layout_signatures(before, after)

    def scan_fit_signature(rendered: Mapping[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for name in ("qa_display_contract", "exact_display_contract"):
            contract = dict(rendered.get(name, {}) or {})
            result[name] = {
                "passed": bool(contract.get("passed")),
                "rows": [
                    {
                        "displayed": str(row.get("displayed") or ""),
                        "expected_displayed": str(
                            row.get("expected_displayed") or ""
                        ),
                        "available_width": int(row.get("available_width") or 0),
                        "measurement_source": str(
                            row.get("measurement_source") or ""
                        ),
                    }
                    for row in contract.get("rows", ())
                ],
            }
        return result

    scan_fits = {
        "before_compact": scan_fit_signature(before_rendered),
        "wide": scan_fit_signature(wide_rendered),
        "after_compact": scan_fit_signature(after_rendered),
    }
    for phase, phase_contracts in scan_fits.items():
        for contract_name, contract in phase_contracts.items():
            if not contract["passed"]:
                issues.append(f"{phase}:{contract_name}:failed")
    if scan_fits["before_compact"] != scan_fits["after_compact"]:
        issues.append("compact_scan_fit_signature_changed_after_round_trip")
    return {
        "fixture": fixture.state_id,
        "compact_size": list(compact),
        "wide_size": list(wide),
        "presenter_stage": str(view.current_stage),
        "before": before,
        "wide": wide_signature,
        "after": after,
        "scan_fit_contracts": scan_fits,
        "issues": issues,
        "passed": not issues,
    }


def prepare_state_for_capture(
    app: Any,
    fixture: StateFixture,
) -> tuple[Any, str, dict[str, Any], dict[str, Any]]:
    """Apply one fixture, settle its final layout, then read rendered state."""

    view, refresh_method = apply_state_fixture(app, fixture)
    settle = settle_responsive_layout(app)
    rendered = collect_rendered_state(app, fixture, view)
    return view, refresh_method, settle, rendered


def run_capture_matrix(
    *,
    output_root: Path,
    sizes: Sequence[tuple[int, int]] = DEFAULT_SIZES,
    state_ids: Sequence[str] = DEFAULT_STATE_IDS,
    scale: float = DEFAULT_SCALE,
    source_root: Path = DEFAULT_SOURCE_ROOT,
    expected_source_commit: str = EXPECTED_SOURCE_COMMIT,
    expected_source_tree: str = EXPECTED_SOURCE_TREE,
    display_device: str = TARGET_DISPLAY_DEVICE,
    work_area: Sequence[int] = TARGET_DISPLAY_WORK_AREA,
) -> tuple[Path, dict[str, Any]]:
    requested_scale = parse_scale(scale)
    requested_work_area = parse_work_area(work_area)
    if requested_work_area != TARGET_DISPLAY_WORK_AREA:
        raise RuntimeError(
            f"work area must equal locked DISPLAY2 constant: {TARGET_DISPLAY_WORK_AREA}"
        )
    sizes, state_ids = validate_capture_matrix_request(sizes, state_ids)
    cancellation_contract_validation = (
        validate_cancellation_surface_capture_contract(state_ids)
    )
    resolved_output = assert_external_capture_descendant(
        output_root,
        CAPTURE_OUTPUT_BASE,
        source_root,
        label="output root",
    )
    if resolved_output.exists():
        raise RuntimeError(f"capture output root already exists: {resolved_output}")
    resolved_output.mkdir(parents=True, exist_ok=False)
    screenshots = resolved_output / "screenshots"
    screenshots.mkdir(parents=True, exist_ok=True)
    data_root = resolved_output / "_isolated_data"
    manifest: dict[str, Any] = {
        "schema_version": CAPTURE_MANIFEST_SCHEMA_VERSION,
        "tool": "tools/capture_label_operator_ui.py",
        "generated_at": dt.datetime.now(dt.timezone.utc).astimezone().isoformat(),
        "tool_repository_root": str(ROOT),
        "requested_source_root": str(source_root.resolve()),
        "expected_source_commit": str(expected_source_commit),
        "expected_source_tree": str(expected_source_tree),
        "output_root": str(resolved_output),
        "data_root": str(data_root.resolve()),
        "application_startup_path": APPLICATION_STARTUP_PATH,
        "requested_size_semantics": "client-area-pixels",
        "capture_size_semantics": "client-pixels",
        "matrix_complete": False,
        "approval_eligible": False,
        "requested_display_device": str(display_device),
        "requested_work_area": list(requested_work_area),
        "requested_sizes": [list(size) for size in sizes],
        "requested_states": list(state_ids),
        "cancellation_surface_capture_contract": json.loads(
            json.dumps(
                CANCELLATION_SURFACE_CAPTURE_CONTRACT,
                ensure_ascii=False,
                sort_keys=True,
            )
        ),
        "cancellation_surface_contract_validation": (
            cancellation_contract_validation
        ),
        "requested_scale": requested_scale,
        "near_black_failure_ratio": NEAR_BLACK_FAILURE_RATIO,
        "captures": [],
    }
    app = None
    module = None
    environment_isolation = None
    import_isolation = None
    original_hostname_resolver = None
    previous_dont_write_bytecode = sys.dont_write_bytecode
    manifest_path = resolved_output / "manifest.json"
    try:
        environment_isolation = prepare_isolated_environment(
            data_root,
            output_base=CAPTURE_OUTPUT_BASE,
            source_root=source_root,
        )
        guards = environment_isolation.guards
        manifest["isolation_guards"] = {
            "keys": sorted(guards),
            "programdata_isolated": True,
            "localappdata_isolated": True,
            "computername_isolated": True,
            "removed_logistics_key_count": len(environment_isolation.removed_keys),
            "startup_override_keys_removed": [
                "LABEL_MATCH_CAPTURE_STARTUP_DPI",
                "LABEL_MATCH_CAPTURE_STARTUP_GEOMETRY",
            ],
            "startup_override_restore_tracked": True,
            "values_recorded": False,
        }
        settings = build_isolated_app_settings(data_root, requested_scale)
        fixture_map = {
            fixture.state_id: fixture for fixture in build_state_fixtures()
        }
        harness_identity = verify_harness_identity(ROOT)
        manifest["harness_identity"] = harness_identity
        manifest["execution_source_binding"] = validate_execution_source_binding(
            ROOT,
            source_root,
            harness_identity,
            expected_commit=expected_source_commit,
            expected_tree=expected_source_tree,
        )
        source_identity = verify_source_identity(
            source_root,
            expected_commit=expected_source_commit,
            expected_tree=expected_source_tree,
        )
        manifest["source_identity"] = source_identity
        manifest["bytecode_artifacts_before_import"] = (
            verify_no_bytecode_artifacts(source_root)
        )
        dpi_mode = enable_per_monitor_dpi_awareness()
        manifest["dpi_awareness"] = dpi_mode
        monitor_target = resolve_capture_monitor(
            display_device,
            requested_work_area,
        )
        manifest["monitor_target"] = monitor_target
        sys.dont_write_bytecode = True
        module, initial_origins, import_isolation = import_label_match_from_source(
            source_root
        )
        manifest["initial_import_origins"] = initial_origins
        original_hostname_resolver = module.socket.gethostname
        module.socket.gethostname = lambda: "CAPTURE-DISPLAY2"
        manifest["identity_fixture"] = {
            "worker_name": "캡처 작업자",
            "host_name": "CAPTURE-DISPLAY2",
            "real_host_name_recorded": False,
        }
        app = _make_capture_app(
            module,
            settings,
            target_dpi=int(monitor_target["dpi"][0]),
        )
        manifest["constructor_tk_scaling"] = dict(
            app.__dict__.get("_capture_constructor_tk_scaling") or {}
        )
        manifest["previsible_placement"] = place_hidden_on_work_area(
            app, monitor_target
        )
        _wait_until_ready(app)
        _apply_scale(app, requested_scale)
        manifest["tk_scaling_after_app_scale"] = observe_target_tk_scaling(
            app,
            int(monitor_target["dpi"][0]),
            hwnd=_window_root_hwnd(app),
        )
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
            app, compact, wide, fixture_map["qa_progress"], monitor_target
        )
        for size in sizes:
            size_placement = _configure_size(app, tuple(size), monitor_target)
            size_dir = screenshots / f"{size[0]}x{size[1]}"
            size_dir.mkdir(parents=True, exist_ok=True)
            for state_id in state_ids:
                fixture = fixture_map[state_id]
                view, refresh_method, settle, rendered = prepare_state_for_capture(
                    app,
                    fixture,
                )
                foreground_acquisition = settle_capture_foreground(app, state_id)
                pending_before_capture = _pending_after_ids(app)
                if pending_before_capture:
                    raise RuntimeError(
                        "scheduled jobs appeared after foreground acquisition: "
                        f"{pending_before_capture}"
                    )
                geometry = collect_ui_geometry(app, fixture)
                before_window = collect_window_capture_contract(
                    app, monitor_target, state_id=state_id
                )
                image, source, capture_screen_bbox = capture_tk_client(
                    app, pump_events=False
                )
                after_window = collect_window_capture_contract(
                    app, monitor_target, state_id=state_id
                )
                capture_geometry_gate = validate_window_capture_pair(
                    before_window,
                    after_window,
                    requested_client_size=size,
                    captured_pixel_size=image.size,
                    captured_screen_bbox=capture_screen_bbox,
                    capture_source=source,
                )
                capture_geometry_gate["foreground_acquisition"] = (
                    foreground_acquisition
                )
                path = size_dir / f"{state_id}.png"
                image.save(path, format="PNG", optimize=True)
                workbench_record = next(
                    record
                    for record in geometry["widgets"]
                    if record["name"] == "workbench"
                )
                client_size = before_window["client_size"]
                client_bbox = [0, 0, int(client_size[0]), int(client_size[1])]
                workbench_bbox = list(map(int, workbench_record["bbox"]))
                record: dict[str, Any] = {
                    "id": f"{size[0]}x{size[1]}-{state_id}",
                    "capture_gate_schema_version": CAPTURE_MANIFEST_SCHEMA_VERSION,
                    "state": state_id,
                    "state_label": fixture.label,
                    "requested_size": list(size),
                    "actual_client_size": list(geometry["root_size"]),
                    "actual_outer_size": list(before_window["window_size"]),
                    "actual_capture_size": list(image.size),
                    "requested_size_semantics": "client-area-pixels",
                    "capture_pixel_semantics": "client-area-pixels",
                    "requested_scale": requested_scale,
                    "applied_scale_factor": float(app.scale_factor),
                    "path": path.relative_to(resolved_output).as_posix(),
                    "capture_source": source,
                    "capture_screen_bbox": list(capture_screen_bbox),
                    "capture_source_authoritative": (
                        source == AUTHORITATIVE_CAPTURE_SOURCE
                    ),
                    "sha256": _sha256(path),
                    "workbench_sha256": image_region_sha256(
                        image, workbench_bbox
                    ),
                    "client_outer_bbox": client_bbox,
                    "client_capture_bbox": client_bbox,
                    "workbench_outer_bbox": workbench_bbox,
                    "workbench_capture_bbox": workbench_bbox,
                    "file_size_bytes": path.stat().st_size,
                    "display_placement": size_placement,
                    "responsive_settle": settle,
                    "window_capture_contract": capture_geometry_gate,
                    "capture_geometry_gate": capture_geometry_gate,
                    "presenter_refresh_method": refresh_method,
                    "fixture": asdict(fixture),
                    "image_analysis": analyze_image(
                        image,
                        tuple(size),
                        content_bbox=client_bbox,
                    ),
                    "ui_geometry": geometry,
                    "rendered_state": rendered,
                }
                record["issues"] = evaluate_capture(record)
                record["passed"] = not record["issues"]
                manifest["captures"].append(record)
        apply_cross_capture_contracts(manifest["captures"])
        manifest["final_import_origins"] = verify_import_origins(source_root)
        manifest["bytecode_artifacts_after_capture"] = (
            verify_no_bytecode_artifacts(source_root)
        )
        manifest["source_identity_after"] = verify_source_identity(
            source_root,
            expected_commit=expected_source_commit,
            expected_tree=expected_source_tree,
        )
        harness_identity_after = verify_harness_identity(ROOT)
        manifest["harness_identity_after"] = harness_identity_after
        if harness_identity_after != harness_identity:
            raise RuntimeError("capture harness identity changed during matrix run")
        validate_execution_source_binding(
            ROOT,
            source_root,
            harness_identity_after,
            expected_commit=expected_source_commit,
            expected_tree=expected_source_tree,
        )
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
        manifest["matrix_complete"] = len(manifest["captures"]) == expected_count
        manifest["approval_eligible"] = bool(
            manifest["matrix_complete"] and manifest["summary"]["passed"]
        )
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
        cleanup_failures: list[str] = []
        if original_hostname_resolver is not None:
            try:
                module.socket.gethostname = original_hostname_resolver
                if module.socket.gethostname is not original_hostname_resolver:
                    raise RuntimeError("hostname resolver identity was not restored")
            except Exception as exc:
                cleanup_failures.append(
                    f"hostname_restore:{type(exc).__name__}:{exc}"
                )
        if app is not None:
            try:
                manifest["previsible_toplevel_guard_restore"] = (
                    release_previsible_toplevel_guard(
                        app, reject_created=False
                    )
                )
            except Exception as exc:
                cleanup_failures.append(
                    f"toplevel_guard_restore:{type(exc).__name__}:{exc}"
                )
            try:
                app.destroy()
            except Exception as exc:
                cleanup_failures.append(f"app_destroy:{type(exc).__name__}:{exc}")
        if import_isolation is not None:
            try:
                manifest["import_environment_restore"] = import_isolation.restore()
            except Exception as exc:
                cleanup_failures.append(
                    f"import_restore:{type(exc).__name__}:{exc}"
                )
        sys.dont_write_bytecode = previous_dont_write_bytecode
        if environment_isolation is not None:
            try:
                manifest["environment_restore"] = environment_isolation.restore()
            except Exception as exc:
                manifest["environment_restore"] = {
                    "status": "FAIL",
                    "error": f"{type(exc).__name__}: {exc}",
                    "values_recorded": False,
                }
                cleanup_failures.append(
                    f"environment_restore:{type(exc).__name__}:{exc}"
                )
        else:
            manifest["environment_restore"] = {
                "status": "NOT_STARTED",
                "values_recorded": False,
            }
        record_cleanup_contract(manifest, cleanup_failures)
        try:
            sanitized, redacted_labels = redact_sensitive_manifest_values(
                manifest,
                (
                    environment_isolation.sensitive_values
                    if environment_isolation is not None
                    else {}
                ),
            )
            manifest.clear()
            manifest.update(sanitized)
            manifest["privacy_contract"] = {
                "status": "PASS",
                "real_environment_values_recorded": False,
                "real_computer_name_recorded": False,
                "redacted_labels": list(redacted_labels),
            }
        except Exception as exc:
            minimal = minimal_privacy_failure_manifest(exc)
            manifest.clear()
            manifest.update(minimal)
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
        default=CAPTURE_OUTPUT_BASE / f"capture_{timestamp}",
        help=(
            f"new output directory below external base {CAPTURE_OUTPUT_BASE}; "
            "the directory must remain outside --source-root"
        ),
    )
    parser.add_argument(
        "--source-root",
        type=Path,
        default=DEFAULT_SOURCE_ROOT,
        help="exact clean Label_Match worktree used for all app imports",
    )
    parser.add_argument(
        "--expected-source-commit",
        default=EXPECTED_SOURCE_COMMIT,
    )
    parser.add_argument(
        "--expected-source-tree",
        default=EXPECTED_SOURCE_TREE,
    )
    parser.add_argument(
        "--display-device",
        default=TARGET_DISPLAY_DEVICE,
        help=f"locked non-primary capture device ({TARGET_DISPLAY_DEVICE})",
    )
    parser.add_argument(
        "--work-area",
        type=parse_work_area,
        default=TARGET_DISPLAY_WORK_AREA,
        help="locked DISPLAY2 LEFT,TOP,RIGHT,BOTTOM work area",
    )
    parser.add_argument(
        "--sizes",
        type=parse_sizes,
        default=DEFAULT_SIZES,
        help="comma-separated client-area pixel sizes within DISPLAY2 work area",
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
        help="deprecated compatibility flag; capture failures are always non-zero",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    manifest_path, manifest = run_capture_matrix(
        output_root=args.output_root,
        sizes=args.sizes,
        state_ids=args.states,
        scale=args.scale,
        source_root=args.source_root,
        expected_source_commit=args.expected_source_commit,
        expected_source_tree=args.expected_source_tree,
        display_device=args.display_device,
        work_area=args.work_area,
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
    return 0 if summary["passed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())

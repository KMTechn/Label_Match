#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Publish the Label_Match worker manual to Outline with guarded verification."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import mimetypes
import os
import re
import sys
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin, urlsplit
from urllib.parse import parse_qs

import requests
from PIL import Image, ImageChops
from requests.auth import AuthBase


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANUAL = ROOT / "docs" / "LABEL_MATCH_WORKER_GUIDE.md"
ASSET_FOLDER = "label_match_user_manual_20260716_display2_v2_0_36"
DEFAULT_ASSET_DIR = ROOT / "docs" / "assets" / ASSET_FOLDER
DEFAULT_OUTLINE_URL = "https://wiki.kmtecherp.com"
DEFAULT_DOCUMENT_ID = "4115be8b-488a-4934-80af-f0f9e4ee721b"
DEFAULT_TITLE = "Label_Match(포장실 프로그램)"
M7_EXTERNAL_CAPTURE_BUNDLE_SCHEMA = "M7 external capture bundle v1"
M7_APP_ID = "Label_Match"
M7_EXTERNAL_CAPTURE_ROOT = "E:/requal-evidence/capture-bundle-v1/Label_Match/"
M7_HANDOVER_INDEX = (
    "E:/KMTech/production-readiness-20260830/HANDOVER/HANDOVER-INDEX.md"
)
M7_CANONICAL_HANDOVER_DIR = Path(
    "E:/KMTech/production-readiness-20260830/HANDOVER"
).resolve()
M7_CAPTURE_TOOL_PATH = "tools/capture_label_operator_ui.py"
M7_APPROVAL_PLACEHOLDER = "미정 — 조직 확정 필요(Q1)"
M7_REQUIRED_STATE_IDS = (
    "phs2_admitted_busy",
    "phs2_rejected_input_preserved",
    "f4_admitted_busy",
    "f4_rejected_input_preserved",
    "f3_admitted_busy",
    "f3_rejected_input_preserved",
    "central_submission_wait",
    "central_submission_conflict",
    "broken_fail_closed_warning",
)
ASSET_PREFIX = f"assets/{ASSET_FOLDER}/"
EXPECTED_UNIQUE_WORKER_IMAGES = 17
EXPECTED_MARKDOWN_IMAGE_REFS = 17
EXPECTED_CAPTURE_REPORT_VERSION = "label-match-outline-manual-capture-v2"
EXPECTED_APP_VERSION = "v2.0.36"
EXPECTED_SOURCE_COMMIT = "faaca1c7783e2e7a91b0fea862e23eefefde09bd"
EXPECTED_SOURCE_TREE = "3d169822fae1cf978b3623cfbb433e5e647615bb"
EXPECTED_DISPLAY_DEVICE = r"\\.\DISPLAY2"
EXPECTED_MONITOR_RECT = [693, -1440, 3253, 0]
EXPECTED_WORK_RECT = [693, -1440, 3253, -48]
EXPECTED_CAPTURE_SIZE = (2560, 1440)
USER_AGENT = "LabelMatchManualPublisher/20260716"
TRUSTED_OUTLINE_ORIGINS = frozenset({"https://wiki.kmtecherp.com:443"})
EXPECTED_IMAGE_NAMES = (
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
)
EXPECTED_MAIN_SCREEN_NAMES = frozenset(
    {
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
)


def _load_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _candidate_env_files(explicit: str = "") -> list[Path]:
    candidates: list[Path] = []
    if explicit:
        candidates.append(Path(explicit))
    if os.environ.get("OUTLINE_ENV_FILE"):
        candidates.append(Path(os.environ["OUTLINE_ENV_FILE"]))
    candidates.extend(
        [
            ROOT / ".outline_env",
            ROOT.parent / ".outline_env",
            Path.home() / ".outline_env",
            Path("/root/.outline_env"),
        ]
    )
    seen: set[str] = set()
    unique: list[Path] = []
    for candidate in candidates:
        key = str(candidate)
        if key not in seen:
            unique.append(candidate)
            seen.add(key)
    return unique


def _load_outline_config(args: argparse.Namespace) -> tuple[str, str]:
    env_values: dict[str, str] = {}
    for env_file in _candidate_env_files(args.env_file):
        env_values.update(_load_env_file(env_file))
    outline_url = args.outline_url or os.environ.get("OUTLINE_URL") or env_values.get("OUTLINE_URL") or DEFAULT_OUTLINE_URL
    token = os.environ.get("OUTLINE_API_TOKEN") or env_values.get("OUTLINE_API_TOKEN") or ""
    return outline_url.rstrip("/"), token


def _manual_image_paths(text: str) -> list[str]:
    return re.findall(
        rf"!\[[^\]]*\]\(({re.escape(ASSET_PREFIX)}(?:annotated|raw)/[^)\s]+\.png)\)",
        text,
    )


def _asset_path(asset_dir: Path, relative_link: str) -> Path:
    if not relative_link.startswith(ASSET_PREFIX):
        raise ValueError(f"manual image is outside the approved asset prefix: {relative_link}")
    return _contained_asset_path(asset_dir, relative_link[len(ASSET_PREFIX) :])


def _contained_asset_path(asset_dir: Path, relative_path: str) -> Path:
    if not relative_path or "\\" in relative_path or "\x00" in relative_path:
        raise ValueError(f"invalid manual asset path: {relative_path!r}")
    relative_asset = Path(relative_path)
    if relative_asset.is_absolute() or any(part in {"", ".", ".."} for part in relative_asset.parts):
        raise ValueError(f"manual asset path is not a safe relative path: {relative_path!r}")
    candidate = (asset_dir / relative_asset).resolve()
    approved_root = asset_dir.resolve()
    if candidate != approved_root and approved_root not in candidate.parents:
        raise ValueError(f"manual image escapes the approved asset directory: {relative_path}")
    return candidate


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class ExternalBundleValidationError(RuntimeError):
    """Typed rejection for canonical external-bundle validation failures."""

    def __init__(self, code: str, message: str) -> None:
        self.code = str(code)
        super().__init__(f"{self.code}: {message}")


def _require_lower_hex(value: Any, length: int, *, label: str) -> str:
    normalized = str(value or "").strip()
    if not re.fullmatch(rf"[0-9a-f]{{{length}}}", normalized):
        raise ExternalBundleValidationError(
            "INVALID_MANIFEST_FIELD",
            f"{label} must be lowercase {length}-hex",
        )
    return normalized


def _relative_bundle_path(value: Any, *, label: str) -> str:
    raw = str(value or "")
    if not raw or raw != raw.strip() or "\\" in raw or "\x00" in raw:
        raise ExternalBundleValidationError(
            "INVALID_BUNDLE_PATH", f"{label} must be a clean POSIX relative path"
        )
    path = Path(raw)
    if path.is_absolute() or path.drive or any(
        part in {"", ".", ".."} for part in path.parts
    ):
        raise ExternalBundleValidationError(
            "INVALID_BUNDLE_PATH", f"{label} must not escape the bundle"
        )
    return path.as_posix()


def _bundle_file(bundle_root: Path, value: Any, *, label: str) -> tuple[str, Path]:
    relative = _relative_bundle_path(value, label=label)
    candidate = (bundle_root / relative).resolve()
    resolved_root = bundle_root.resolve()
    if candidate == resolved_root or resolved_root not in candidate.parents:
        raise ExternalBundleValidationError(
            "INVALID_BUNDLE_PATH", f"{label} escapes the bundle"
        )
    return relative, candidate


def _require_rfc3339_utc(value: Any, *, label: str) -> str:
    normalized = str(value or "").strip()
    if not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z", normalized
    ):
        raise ExternalBundleValidationError(
            "INVALID_MANIFEST_FIELD", f"{label} must be RFC 3339 UTC with Z"
        )
    try:
        dt.datetime.fromisoformat(normalized.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise ExternalBundleValidationError(
            "INVALID_MANIFEST_FIELD", f"{label} is not a real UTC timestamp"
        ) from exc
    return normalized


def _read_bounded_json(path: Path, *, label: str) -> tuple[bytes, dict[str, Any]]:
    if not path.is_file():
        raise ExternalBundleValidationError(
            "BUNDLE_FILE_MISSING", f"{label} is missing: {path}"
        )
    size = path.stat().st_size
    if size <= 0 or size > 1024 * 1024:
        raise ExternalBundleValidationError(
            "INVALID_BUNDLE_JSON", f"{label} must be between 1 byte and 1 MiB"
        )
    payload = path.read_bytes()
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExternalBundleValidationError(
            "INVALID_BUNDLE_JSON", f"{label} is not UTF-8 JSON"
        ) from exc
    if not isinstance(value, dict):
        raise ExternalBundleValidationError(
            "INVALID_BUNDLE_JSON", f"{label} root must be an object"
        )
    return payload, value


def _approval_is_pending(value: Any) -> bool:
    normalized = str(value or "").strip()
    return not normalized or normalized == M7_APPROVAL_PLACEHOLDER


def validate_external_bundle_manifest(
    manifest_path: Path, expected_manifest_sha256: str
) -> dict[str, Any]:
    """Validate one final canonical bundle without copying its raster into docs."""

    expected_digest = _require_lower_hex(
        expected_manifest_sha256, 64, label="expected manifest SHA-256"
    )
    path = manifest_path.resolve()
    if path.name != "manifest.json":
        raise ExternalBundleValidationError(
            "INVALID_BUNDLE_TOPOLOGY", "external bundle input must be manifest.json"
        )
    bundle_root = path.parent
    if bundle_root.parent.name != M7_APP_ID or not re.fullmatch(
        r"Label_Match__[0-9a-f]{12}__\d{8}T\d{6}Z__[0-9a-f]{8}",
        bundle_root.name,
    ):
        raise ExternalBundleValidationError(
            "INVALID_BUNDLE_TOPOLOGY",
            "manifest must use Label_Match/<canonical-bundle-id>/manifest.json",
        )
    for name in ("captures", "states", "approval"):
        if not (bundle_root / name).is_dir():
            raise ExternalBundleValidationError(
                "INVALID_BUNDLE_TOPOLOGY", f"canonical {name}/ directory is missing"
            )
    manifest_bytes, manifest = _read_bounded_json(path, label="manifest.json")
    actual_digest = hashlib.sha256(manifest_bytes).hexdigest()
    if actual_digest != expected_digest:
        raise ExternalBundleValidationError(
            "MANIFEST_DIGEST_MISMATCH",
            "manifest bytes do not match --expected-manifest-sha256",
        )
    if manifest.get("schema") != M7_EXTERNAL_CAPTURE_BUNDLE_SCHEMA:
        raise ExternalBundleValidationError(
            "SCHEMA_MISMATCH", "manifest schema is not the canonical exact string"
        )
    if manifest.get("app") != M7_APP_ID:
        raise ExternalBundleValidationError(
            "APP_MISMATCH", "manifest app must be Label_Match"
        )

    app_source = manifest.get("app_source")
    portable = manifest.get("portable_artifact")
    capture_tool = manifest.get("capture_tool")
    approval = manifest.get("approval")
    captures = manifest.get("captures")
    if not all(
        isinstance(value, dict)
        for value in (app_source, portable, capture_tool, approval)
    ) or not isinstance(captures, list):
        raise ExternalBundleValidationError(
            "INVALID_MANIFEST_FIELD", "canonical nested manifest groups are missing"
        )
    app_commit = _require_lower_hex(
        app_source.get("commit"), 40, label="app_source.commit"
    )
    _require_lower_hex(app_source.get("tree"), 40, label="app_source.tree")
    if bundle_root.name.split("__", 2)[1] != app_commit[:12]:
        raise ExternalBundleValidationError(
            "SOURCE_IDENTITY_MISMATCH",
            "bundle-id commit prefix does not match app_source.commit",
        )
    _relative_bundle_path(portable.get("file"), label="portable_artifact.file")
    _require_lower_hex(
        portable.get("sha256"), 64, label="portable_artifact.sha256"
    )
    if capture_tool.get("path") != M7_CAPTURE_TOOL_PATH:
        raise ExternalBundleValidationError(
            "CAPTURE_TOOL_MISMATCH",
            f"capture_tool.path must be {M7_CAPTURE_TOOL_PATH}",
        )
    _require_lower_hex(
        capture_tool.get("commit"), 40, label="capture_tool.commit"
    )
    _require_lower_hex(
        capture_tool.get("blob_sha256"),
        64,
        label="capture_tool.blob_sha256",
    )

    state_ids: list[str] = []
    verified_images: list[str] = []
    for index, capture in enumerate(captures):
        if not isinstance(capture, dict):
            raise ExternalBundleValidationError(
                "INVALID_CAPTURE", f"captures[{index}] must be an object"
            )
        state_id = str(capture.get("state_id") or "")
        state_ids.append(state_id)
        viewport = capture.get("viewport")
        if not isinstance(viewport, dict):
            raise ExternalBundleValidationError(
                "INVALID_CAPTURE", f"{state_id} viewport is missing"
            )
        width = viewport.get("width_px")
        height = viewport.get("height_px")
        dpi = capture.get("dpi")
        if any(isinstance(value, bool) for value in (width, height, dpi)) or not all(
            isinstance(value, int) and value > 0 for value in (width, height, dpi)
        ):
            raise ExternalBundleValidationError(
                "INVALID_CAPTURE", f"{state_id} viewport and dpi must be positive integers"
            )
        _require_rfc3339_utc(
            capture.get("generated_at"), label=f"captures[{index}].generated_at"
        )
        expected_image_file = (
            f"captures/{state_id}__{width}x{height}__{dpi}dpi.png"
        )
        image_file, image_path = _bundle_file(
            bundle_root, capture.get("image_file"), label=f"{state_id}.image_file"
        )
        if image_file != expected_image_file:
            raise ExternalBundleValidationError(
                "INVALID_CAPTURE_FILENAME",
                f"{state_id} image_file must be {expected_image_file}",
            )
        expected_image_digest = _require_lower_hex(
            capture.get("image_sha256"), 64, label=f"{state_id}.image_sha256"
        )
        if not image_path.is_file() or _sha256_file(image_path) != expected_image_digest:
            raise ExternalBundleValidationError(
                "IMAGE_DIGEST_MISMATCH", f"{state_id} PNG digest does not match"
            )
        image_info = _png_info(image_path)
        if image_info["format"] != "PNG" or image_info["size"] != (width, height):
            raise ExternalBundleValidationError(
                "IMAGE_DIMENSION_MISMATCH",
                f"{state_id} PNG format/dimensions do not match viewport",
            )
        verified_images.append(image_file)
    if len(state_ids) != len(M7_REQUIRED_STATE_IDS) or set(state_ids) != set(
        M7_REQUIRED_STATE_IDS
    ):
        raise ExternalBundleValidationError(
            "REQUIRED_STATE_SET_MISMATCH",
            "captures must contain every required state exactly once and no extras",
        )

    capture_set_path = bundle_root / "capture-set.json"
    _capture_set_bytes, capture_set = _read_bounded_json(
        capture_set_path, label="capture-set.json"
    )
    for key in (
        "schema",
        "app",
        "app_source",
        "portable_artifact",
        "capture_tool",
        "captures",
    ):
        if capture_set.get(key) != manifest.get(key):
            raise ExternalBundleValidationError(
                "CAPTURE_SET_MISMATCH", f"capture-set.json differs at {key}"
            )

    approval_keys = {
        "approver",
        "approval_receipt_file",
        "approval_receipt_sha256",
        "custody_receipt_file",
        "custody_receipt_sha256",
    }
    if set(approval) != approval_keys:
        raise ExternalBundleValidationError(
            "INVALID_APPROVAL", "approval must contain five canonical fields exactly"
        )
    if any(_approval_is_pending(approval.get(key)) for key in approval_keys):
        raise ExternalBundleValidationError(
            "APPROVAL_PENDING", "approval or custody still contains a placeholder"
        )
    receipt_specs = (
        (
            "approval_receipt_file",
            "approval_receipt_sha256",
            "approval/approval-receipt.json",
        ),
        (
            "custody_receipt_file",
            "custody_receipt_sha256",
            "approval/custody-receipt.json",
        ),
    )
    verified_receipts: list[str] = []
    for file_key, digest_key, expected_file in receipt_specs:
        receipt_file, receipt_path = _bundle_file(
            bundle_root, approval[file_key], label=f"approval.{file_key}"
        )
        if receipt_file != expected_file:
            raise ExternalBundleValidationError(
                "INVALID_APPROVAL", f"approval.{file_key} must be {expected_file}"
            )
        receipt_digest = _require_lower_hex(
            approval[digest_key], 64, label=f"approval.{digest_key}"
        )
        if not receipt_path.is_file() or _sha256_file(receipt_path) != receipt_digest:
            raise ExternalBundleValidationError(
                "RECEIPT_DIGEST_MISMATCH", f"{receipt_file} digest does not match"
            )
        verified_receipts.append(receipt_file)

    return {
        "schema": M7_EXTERNAL_CAPTURE_BUNDLE_SCHEMA,
        "app": M7_APP_ID,
        "manifest_path": str(path),
        "manifest_sha256": actual_digest,
        "bundle_id": bundle_root.name,
        "verified_state_ids": state_ids,
        "verified_image_files": verified_images,
        "verified_receipt_files": verified_receipts,
        "approval_status": "APPROVED",
    }


def _png_info(path: Path) -> dict[str, Any]:
    with Image.open(path) as image:
        image.load()
        return {
            "format": image.format,
            "size": tuple(map(int, image.size)),
            "mode": image.mode,
        }


def _pixel_comparison(raw_path: Path, annotated_path: Path) -> dict[str, Any]:
    with Image.open(raw_path) as raw_image, Image.open(annotated_path) as annotated_image:
        raw = raw_image.convert("RGB")
        annotated = annotated_image.convert("RGB")
    if raw.size != annotated.size:
        raise RuntimeError(f"raw/annotated dimensions differ: {raw.size} != {annotated.size}")

    def near_black_count(image: Image.Image) -> int:
        masks = [channel.point(lambda value: 255 if value <= 16 else 0) for channel in image.split()]
        mask = ImageChops.multiply(ImageChops.multiply(masks[0], masks[1]), masks[2])
        return int(mask.histogram()[255])

    diff = ImageChops.difference(raw, annotated)
    channels = diff.split()
    diff_mask = ImageChops.lighter(ImageChops.lighter(channels[0], channels[1]), channels[2])
    changed_pixel_count = raw.width * raw.height - int(diff_mask.histogram()[0])
    near_black_increase = max(0, near_black_count(annotated) - near_black_count(raw))
    pixel_count = raw.width * raw.height
    return {
        "changed_pixel_count": changed_pixel_count,
        "near_black_increase_ratio": near_black_increase / pixel_count,
    }


def _rows_sha256(rows: Any) -> str:
    return hashlib.sha256(
        json.dumps(rows, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _enforce_current_capture_publish_contract() -> None:
    """Reject the retired tracked-image path and name its replacement."""

    raise ExternalBundleValidationError(
        "LEGACY_TRACKED_IMAGE_PUBLISHING_DEPRECATED",
        "use --external-bundle-manifest <path> "
        "--expected-manifest-sha256 <64hex>; tracked raster is historical only",
    )


def _validate_capture_manifest(asset_dir: Path, unique_links: list[str]) -> dict[str, Any]:
    manifest_path = asset_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"capture manifest is missing: {manifest_path}")
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes.decode("utf-8"))
    images = manifest.get("images") or []
    if manifest.get("status") != "PASS" or manifest.get("image_contract_ok") is not True:
        raise RuntimeError("capture manifest is not PASS")
    if manifest.get("report_version") != EXPECTED_CAPTURE_REPORT_VERSION:
        raise RuntimeError("capture manifest report version is not the approved v2 contract")
    if manifest.get("app_version") != EXPECTED_APP_VERSION:
        raise RuntimeError(f"capture app version is not {EXPECTED_APP_VERSION}")

    source = manifest.get("source_identity") or {}
    if (
        source.get("commit") != EXPECTED_SOURCE_COMMIT
        or source.get("tree") != EXPECTED_SOURCE_TREE
        or source.get("worktree_clean") is not True
        or manifest.get("source_identity_unchanged") is not True
    ):
        raise RuntimeError("capture source identity is not the exact clean v2.0.36 tree")
    module_origins = manifest.get("module_origin_contract") or {}
    dpi_awareness = manifest.get("dpi_awareness") or {}
    if module_origins.get("status") != "PASS" or not module_origins.get("checked_modules"):
        raise RuntimeError("capture modules are not bound to the exact source root")
    if dpi_awareness.get("status") != "PASS" or dpi_awareness.get("observed") != 2:
        raise RuntimeError("capture process was not per-monitor DPI aware")

    monitor = manifest.get("monitor_target") or {}
    if (
        str(monitor.get("device") or "").casefold() != EXPECTED_DISPLAY_DEVICE.casefold()
        or monitor.get("is_primary") is not False
        or monitor.get("monitor_rect") != EXPECTED_MONITOR_RECT
        or monitor.get("work_rect") != EXPECTED_WORK_RECT
        or tuple(monitor.get("capture_size") or ()) != EXPECTED_CAPTURE_SIZE
    ):
        raise RuntimeError("capture monitor is not the locked non-primary DISPLAY2 target")
    previsible = manifest.get("previsible_placement_contract") or {}
    placements = previsible.get("instances") or []
    if (
        previsible.get("status") != "PASS"
        or len(placements) != 2
        or any(
            item.get("status") != "PASS"
            or item.get("visible_before_move") is not False
            or item.get("visible_after_hidden_move") is not False
            or item.get("visible_after_show") is not True
            or str(item.get("monitor_device_after_show") or "").casefold()
            != EXPECTED_DISPLAY_DEVICE.casefold()
            or item.get("monitor_is_primary_after_show") is not False
            for item in placements
        )
    ):
        raise RuntimeError("app first-visible placement was not proven on DISPLAY2")
    quiescence = manifest.get("scheduled_job_quiescence") or {}
    quiesced_instances = quiescence.get("instances") or []
    if (
        quiescence.get("status") != "PASS"
        or len(quiesced_instances) != 2
        or any(
            item.get("status") != "PASS"
            or item.get("remaining_after") != 0
            for item in quiesced_instances
        )
    ):
        raise RuntimeError("capture scheduled jobs were not deterministically quiesced")

    privacy = manifest.get("privacy_contract") or {}
    if (
        privacy.get("status") != "PASS"
        or privacy.get("forbidden_hits") not in ([], ())
        or privacy.get("hostname_recorded") is not False
    ):
        raise RuntimeError("capture manifest privacy contract is not PASS")
    if len(images) != EXPECTED_UNIQUE_WORKER_IMAGES:
        raise RuntimeError(
            f"capture manifest expected {EXPECTED_UNIQUE_WORKER_IMAGES} images, got {len(images)}"
        )
    expected_names = tuple(manifest.get("expected_names") or ())
    actual_names = tuple(manifest.get("actual_names") or ())
    if expected_names != EXPECTED_IMAGE_NAMES or actual_names != EXPECTED_IMAGE_NAMES:
        raise RuntimeError("capture image names/order differ from the approved 17-state packet")
    manifest_links = [ASSET_PREFIX + str(item.get("annotated_path") or "") for item in images]
    if set(manifest_links) != set(unique_links):
        raise RuntimeError("manual image references differ from the PASS capture manifest")

    central_contract = manifest.get("central_scan_list_contract") or {}
    if (
        central_contract.get("status") != "PASS"
        or set(central_contract.get("required_main_screen_names") or ()) != EXPECTED_MAIN_SCREEN_NAMES
        or set(central_contract.get("evidenced_main_screen_names") or ()) != EXPECTED_MAIN_SCREEN_NAMES
        or central_contract.get("required_location") != "central_lower"
    ):
        raise RuntimeError("central-lower scan-list capture contract is not PASS")

    raw_uniqueness = manifest.get("raw_image_uniqueness_contract") or {}
    if (
        raw_uniqueness.get("status") != "PASS"
        or int(raw_uniqueness.get("expected_unique_count", -1))
        != EXPECTED_UNIQUE_WORKER_IMAGES
        or int(raw_uniqueness.get("observed_unique_count", -1))
        != EXPECTED_UNIQUE_WORKER_IMAGES
    ):
        raise RuntimeError("capture raw-image uniqueness contract is not PASS")
    workflow_states = manifest.get("workflow_state_contract") or {}
    workflow_checkpoints = workflow_states.get("checkpoints") or []
    if (
        workflow_states.get("status") != "PASS"
        or not workflow_checkpoints
        or any(item.get("status") != "PASS" for item in workflow_checkpoints)
    ):
        raise RuntimeError("capture workflow-state contract is not PASS")

    maximum_near_black_increase = 0.0
    verified_files = 0
    observed_names: list[str] = []
    observed_raw_hashes: list[str] = []
    for item in images:
        name = str(item.get("name") or "")
        observed_names.append(name)
        raw_rel = str(item.get("raw_path") or "")
        annotated_rel = str(item.get("annotated_path") or "")
        if raw_rel != f"raw/{name}.png" or annotated_rel != f"annotated/{name}.png":
            raise RuntimeError(f"capture paths do not match state name: {name}")
        raw_path = _contained_asset_path(asset_dir, raw_rel)
        annotated_path = _contained_asset_path(asset_dir, annotated_rel)
        if not raw_path.is_file() or not annotated_path.is_file():
            raise FileNotFoundError(f"capture image pair is missing: {name}")
        raw_sha256 = _sha256_file(raw_path)
        annotated_sha256 = _sha256_file(annotated_path)
        observed_raw_hashes.append(raw_sha256)
        if raw_sha256 != item.get("raw_sha256") or annotated_sha256 != item.get("annotated_sha256"):
            raise RuntimeError(f"capture image bytes do not match manifest hashes: {name}")
        if raw_sha256 == annotated_sha256:
            raise RuntimeError(f"capture annotation did not change image bytes: {name}")
        raw_info = _png_info(raw_path)
        annotated_info = _png_info(annotated_path)
        if (
            raw_info["format"] != "PNG"
            or annotated_info["format"] != "PNG"
            or raw_info["size"] != EXPECTED_CAPTURE_SIZE
            or annotated_info["size"] != EXPECTED_CAPTURE_SIZE
            or item.get("width") != EXPECTED_CAPTURE_SIZE[0]
            or item.get("height") != EXPECTED_CAPTURE_SIZE[1]
        ):
            raise RuntimeError(f"capture image format/dimensions are invalid: {name}")
        pixel_qa = _pixel_comparison(raw_path, annotated_path)
        recorded_pixel_qa = item.get("pixel_qa") or {}
        if (
            pixel_qa["changed_pixel_count"] <= 0
            or int(recorded_pixel_qa.get("changed_pixel_count", -1)) != pixel_qa["changed_pixel_count"]
            or abs(
                float(recorded_pixel_qa.get("near_black_increase_ratio", 1.0))
                - pixel_qa["near_black_increase_ratio"]
            )
            > 1e-12
        ):
            raise RuntimeError(f"capture pixel QA does not match actual bytes: {name}")
        maximum_near_black_increase = max(
            maximum_near_black_increase,
            pixel_qa["near_black_increase_ratio"],
        )
        if (
            str(item.get("monitor_device") or "").casefold() != EXPECTED_DISPLAY_DEVICE.casefold()
            or item.get("monitor_is_primary") is not False
            or item.get("monitor_rect") != EXPECTED_MONITOR_RECT
            or item.get("monitor_contract_ok") is not True
            or item.get("target_is_foreground") is not True
            or item.get("target_contained_in_monitor") is not True
            or str(item.get("app_root_monitor_device") or "").casefold()
            != EXPECTED_DISPLAY_DEVICE.casefold()
            or item.get("app_root_monitor_is_primary") is not False
            or item.get("app_root_matches_work_area") is not True
            or item.get("blank_suspected") is not False
        ):
            raise RuntimeError(f"capture monitor/foreground contract failed: {name}")

        central = item.get("central_scan_list")
        if name in EXPECTED_MAIN_SCREEN_NAMES:
            labels = {str(entry.get("label") or "") for entry in item.get("annotations") or []}
            observed_rows = (central or {}).get("observed_rows") or []
            expected_rows = (central or {}).get("expected_rows") or []
            if (
                not isinstance(central, dict)
                or central.get("required") is not True
                or central.get("location") != "central_lower"
                or central.get("widget") not in {"qa_scan_tree", "exact_rescan_tree"}
                or central.get("mapped") is not True
                or central.get("viewable") is not True
                or central.get("within_center_pane") is not True
                or central.get("below_scan_entry") is not True
                or central.get("positive_geometry") is not True
                or central.get("final_row_visible") is not True
                or central.get("rows_exact_match") is not True
                or observed_rows != expected_rows
                or int(central.get("row_count", -1)) != len(observed_rows)
                or central.get("observed_rows_sha256") != _rows_sha256(observed_rows)
                or central.get("expected_rows_sha256") != _rows_sha256(expected_rows)
                or "중앙 하단 실제 스캔 목록" not in labels
            ):
                raise RuntimeError(f"central scan-list evidence is invalid: {name}")
        elif central is not None:
            raise RuntimeError(f"dialog capture unexpectedly claims central-list evidence: {name}")
        verified_files += 2

    if tuple(observed_names) != EXPECTED_IMAGE_NAMES:
        raise RuntimeError("manifest image array is not in the approved order")
    if len(set(observed_raw_hashes)) != EXPECTED_UNIQUE_WORKER_IMAGES:
        raise RuntimeError("capture contains repeated raw state images")
    if maximum_near_black_increase > 0.005:
        raise RuntimeError(
            f"capture near-black increase exceeds 0.5%: {maximum_near_black_increase:.6f}"
        )

    contact = manifest.get("contact_sheet") or {}
    contact_path = _contained_asset_path(asset_dir, str(contact.get("path") or ""))
    if contact_path.name != "contact_sheet.png" or not contact_path.is_file():
        raise RuntimeError("capture contact sheet is missing or misnamed")
    if _sha256_file(contact_path) != contact.get("sha256"):
        raise RuntimeError("capture contact sheet bytes do not match its manifest hash")
    if _png_info(contact_path)["format"] != "PNG":
        raise RuntimeError("capture contact sheet is not a PNG")
    return {
        "capture_manifest_path": f"{ASSET_FOLDER}/manifest.json",
        "capture_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "capture_manifest_status": manifest.get("status"),
        "capture_manifest_image_count": len(images),
        "capture_manifest_image_contract_ok": manifest.get("image_contract_ok"),
        "capture_manifest_max_near_black_increase_ratio": maximum_near_black_increase,
        "capture_manifest_verified_image_files": verified_files,
        "capture_manifest_source_commit": source.get("commit"),
        "capture_manifest_source_tree": source.get("tree"),
        "capture_manifest_display_device": monitor.get("device"),
        "capture_manifest_display_is_primary": monitor.get("is_primary"),
        "capture_manifest_central_scan_list_status": central_contract.get("status"),
        "capture_manifest_raw_image_uniqueness_status": raw_uniqueness.get("status"),
        "capture_manifest_workflow_state_status": workflow_states.get("status"),
        "capture_manifest_privacy_status": privacy.get("status"),
    }


def _count_today_button_typo(text: str) -> int:
    return len(re.findall(r"(?<!오)늘 버튼으로", text))


def _build_outline_text(manual_path: Path, asset_dir: Path, attachment_urls: dict[str, str] | None = None) -> tuple[str, dict[str, Any]]:
    text = manual_path.read_text(encoding="utf-8")
    links = _manual_image_paths(text)
    unique_links = list(dict.fromkeys(links))
    missing = [rel for rel in unique_links if not _asset_path(asset_dir, rel).exists()]
    report: dict[str, Any] = {
        "manual_path": str(manual_path),
        "asset_dir": str(asset_dir),
        "markdown_image_refs": len(links),
        "unique_image_refs": len(unique_links),
        "missing_images": missing,
        "mermaid_block_count": text.count("```mermaid"),
        "local_file_upload_text_count": text.count("파일 업로드"),
        "local_typo_count": _count_today_button_typo(text),
        "local_today_phrase_count": text.count("`오늘` 버튼으로") + text.count("오늘 버튼으로"),
    }
    if len(unique_links) != EXPECTED_UNIQUE_WORKER_IMAGES:
        raise RuntimeError(f"expected {EXPECTED_UNIQUE_WORKER_IMAGES} unique worker images, got {len(unique_links)}")
    if len(links) != EXPECTED_MARKDOWN_IMAGE_REFS:
        raise RuntimeError(f"expected {EXPECTED_MARKDOWN_IMAGE_REFS} markdown image references, got {len(links)}")
    if missing:
        raise FileNotFoundError(f"missing manual images: {missing}")
    if report["local_file_upload_text_count"] != 0:
        raise RuntimeError("local manual unexpectedly contains '파일 업로드'")
    if report["local_typo_count"] != 0:
        raise RuntimeError("local manual unexpectedly contains '늘 버튼으로'")
    if report["local_today_phrase_count"] < 1:
        raise RuntimeError("local manual does not contain the expected today-button phrase")
    report.update(_validate_capture_manifest(asset_dir, unique_links))
    if attachment_urls:
        for rel in unique_links:
            text = text.replace(rel, attachment_urls[rel])
        report.update(
            {
                "outline_attachment_refs": text.count("/api/attachments.redirect"),
                "relative_image_refs_after_replace": text.count(ASSET_PREFIX),
            }
        )
    return text, report


def _build_reference_only_outline_text(
    manual_path: Path,
) -> tuple[str, dict[str, Any]]:
    """Prepare current guidance that points to custody evidence without embedding it."""

    text = manual_path.read_text(encoding="utf-8")
    required_references = (
        M7_EXTERNAL_CAPTURE_BUNDLE_SCHEMA,
        M7_APP_ID,
        M7_EXTERNAL_CAPTURE_ROOT,
        M7_HANDOVER_INDEX,
        "captures[].state_id",
    )
    missing = [value for value in required_references if value not in text]
    missing.extend(state_id for state_id in M7_REQUIRED_STATE_IDS if state_id not in text)
    if missing:
        raise ExternalBundleValidationError(
            "DOCUMENT_REFERENCE_MISSING",
            "manual is missing canonical references: " + ", ".join(missing),
        )
    markdown_rasters = re.findall(r"!\[[^\]]*\]\([^)]*\)", text)
    digest_values = re.findall(r"(?i)(?<![0-9a-f])[0-9a-f]{64}(?![0-9a-f])", text)
    if markdown_rasters or digest_values:
        raise ExternalBundleValidationError(
            "DOCUMENT_NOT_REFERENCE_ONLY",
            "current manual must not embed raster links or bundle digest values",
        )
    return text, {
        "manual_path": str(manual_path),
        "document_reference_only": True,
        "document_markdown_image_refs": 0,
        "document_digest_values": 0,
        "canonical_handover_index": M7_HANDOVER_INDEX,
        "canonical_external_root": M7_EXTERNAL_CAPTURE_ROOT,
        "required_state_ids": list(M7_REQUIRED_STATE_IDS),
    }


def _reject_unsafe_url_text(value: str, *, label: str) -> str:
    if value != value.strip() or not value:
        raise ValueError(f"{label} must be a non-empty URL without surrounding whitespace")
    if "\\" in value or any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError(f"{label} contains unsafe characters")
    if value.startswith("//"):
        raise ValueError(f"{label} must not be a protocol-relative URL")
    return value


def _canonical_origin(parsed: Any, *, label: str) -> str:
    if parsed.scheme.casefold() != "https":
        raise ValueError(f"{label} must use HTTPS")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError(f"{label} must not contain userinfo")
    if not parsed.hostname:
        raise ValueError(f"{label} is missing a hostname")
    try:
        hostname = parsed.hostname.encode("idna").decode("ascii").casefold()
        port = parsed.port or 443
    except (UnicodeError, ValueError) as exc:
        raise ValueError(f"{label} has an invalid hostname or port") from exc
    return f"https://{hostname}:{port}"


def _validate_root_https_origin(value: str, *, label: str) -> tuple[str, str]:
    value = _reject_unsafe_url_text(value, label=label)
    parsed = urlsplit(value)
    origin = _canonical_origin(parsed, label=label)
    if parsed.query or parsed.fragment or parsed.path not in {"", "/"}:
        raise ValueError(f"{label} must be an origin without path, query, or fragment")
    hostname = parsed.hostname.encode("idna").decode("ascii").casefold()
    port = parsed.port or 443
    normalized = f"https://{hostname}" + (f":{port}" if port != 443 else "")
    return normalized, origin


def _validate_outline_base_url(value: str) -> tuple[str, str]:
    normalized, origin = _validate_root_https_origin(value, label="Outline base URL")
    if origin not in TRUSTED_OUTLINE_ORIGINS:
        raise ValueError(f"Outline API origin is not code-approved: {origin}")
    return normalized, origin


def _trusted_upload_origins(base_origin: str, values: Iterable[str]) -> frozenset[str]:
    origins = {base_origin}
    for value in values:
        _normalized, origin = _validate_root_https_origin(value, label="trusted upload origin")
        origins.add(origin)
    return frozenset(origins)


def _resolve_upload_url(base_url: str, value: str, trusted_origins: frozenset[str]) -> str:
    value = _reject_unsafe_url_text(value, label="attachment upload URL")
    parsed = urlsplit(value)
    if parsed.scheme or parsed.netloc:
        resolved = value
    else:
        if not value.startswith("/"):
            raise ValueError("relative attachment upload URL must be root-relative")
        resolved = urljoin(base_url + "/", value)
    resolved_parsed = urlsplit(resolved)
    origin = _canonical_origin(resolved_parsed, label="attachment upload URL")
    if resolved_parsed.fragment:
        raise ValueError("attachment upload URL must not contain a fragment")
    if origin not in trusted_origins:
        raise ValueError(f"attachment upload origin is not explicitly trusted: {origin}")
    return resolved


def _validate_attachment_url(value: str) -> str:
    value = _reject_unsafe_url_text(value, label="attachment URL")
    parsed = urlsplit(value)
    if parsed.scheme or parsed.netloc or not value.startswith("/"):
        raise ValueError("attachment URL must be a root-relative Outline redirect URL")
    if parsed.fragment or parsed.path != "/api/attachments.redirect":
        raise ValueError("attachment URL must use /api/attachments.redirect without a fragment")
    query = parse_qs(parsed.query, keep_blank_values=True)
    if set(query) != {"id"} or len(query["id"]) != 1 or not query["id"][0].strip():
        raise ValueError("attachment URL must contain exactly one non-empty id query value")
    return value


class _NoAuth(AuthBase):
    def __call__(self, request: Any) -> Any:
        return request


class OutlineClient:
    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        trusted_upload_origins: Iterable[str] = (),
        tls_ca_bundle_path: str = "",
    ) -> None:
        if not token:
            raise RuntimeError("OUTLINE_API_TOKEN is required for publish mode")
        self.base_url, self.base_origin = _validate_outline_base_url(base_url)
        self.trusted_upload_origins = _trusted_upload_origins(
            self.base_origin,
            trusted_upload_origins,
        )
        self.tls_ca_bundle_path = str(tls_ca_bundle_path or "").strip()
        if self.tls_ca_bundle_path and not Path(self.tls_ca_bundle_path).is_file():
            raise ValueError("Outline TLS CA bundle is unavailable")
        self.api_session = requests.Session()
        self.upload_session = requests.Session()
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        }

    def api(self, method: str, payload: dict[str, Any]) -> dict[str, Any]:
        if not re.fullmatch(r"[a-z][a-z0-9.]*", method):
            raise ValueError(f"invalid Outline API method: {method!r}")
        api_url = f"{self.base_url}/api/{method}"
        request_kwargs: dict[str, Any] = {
            "headers": self.headers,
            "json": payload,
            "auth": _NoAuth(),
            "allow_redirects": False,
            "timeout": 60,
        }
        if self.tls_ca_bundle_path:
            request_kwargs["verify"] = self.tls_ca_bundle_path
        response = self.api_session.post(api_url, **request_kwargs)
        if not 200 <= response.status_code < 300:
            raise RuntimeError(f"{method} HTTP {response.status_code}: {response.text[:500]}")
        data = response.json()
        if data.get("ok") is False:
            raise RuntimeError(f"{method} not ok: {data}")
        return data

    def upload_image(self, document_id: str, path: Path) -> str:
        content_type = mimetypes.guess_type(path.name)[0] or "image/png"
        created = self.api(
            "attachments.create",
            {
                "name": path.name,
                "contentType": content_type,
                "size": path.stat().st_size,
                "documentId": document_id,
            },
        )["data"]
        upload_url = _resolve_upload_url(
            self.base_url,
            str(created["uploadUrl"]),
            self.trusted_upload_origins,
        )
        attachment_url = _validate_attachment_url(str(created["attachment"]["url"]))
        upload_kwargs: dict[str, Any] = {
            "headers": {
                "Accept": "application/json,*/*",
                "User-Agent": USER_AGENT,
            },
            "auth": _NoAuth(),
            "data": created.get("form") or {},
            "allow_redirects": False,
            "timeout": 180,
        }
        if (
            self.tls_ca_bundle_path
            and _canonical_origin(urlsplit(upload_url), label="attachment upload URL")
            == self.base_origin
        ):
            upload_kwargs["verify"] = self.tls_ca_bundle_path
        with path.open("rb") as image_file:
            upload_kwargs["files"] = {
                "file": (path.name, image_file, content_type)
            }
            uploaded = self.upload_session.post(
                upload_url,
                **upload_kwargs,
            )
        if not 200 <= uploaded.status_code < 300:
            raise RuntimeError(f"upload failed {path.name}: HTTP {uploaded.status_code}: {uploaded.text[:500]}")
        return attachment_url


def _write_report(path: str, report: dict[str, Any]) -> None:
    if not path:
        return
    target = Path(path).resolve()
    if target == M7_CANONICAL_HANDOVER_DIR or M7_CANONICAL_HANDOVER_DIR in target.parents:
        raise ExternalBundleValidationError(
            "HANDOVER_WRITE_FORBIDDEN",
            "publisher reports must not modify the canonical HANDOVER directory",
        )
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish the Label_Match worker manual to Outline")
    parser.add_argument("--manual", default=str(DEFAULT_MANUAL))
    parser.add_argument(
        "--asset-dir",
        default=str(DEFAULT_ASSET_DIR),
        help="deprecated historical tracked-image argument; no raster is uploaded",
    )
    parser.add_argument("--external-bundle-manifest", default="")
    parser.add_argument("--expected-manifest-sha256", default="")
    parser.add_argument("--outline-url", default="")
    parser.add_argument("--document-id", default=DEFAULT_DOCUMENT_ID)
    parser.add_argument("--title", default=DEFAULT_TITLE)
    parser.add_argument("--env-file", default="")
    parser.add_argument(
        "--trusted-upload-origin",
        action="append",
        default=[],
        help="Exact HTTPS object-storage origin allowed for attachment bytes; repeat as needed",
    )
    parser.add_argument("--report-path", default="")
    parser.add_argument(
        "--tls-ca-bundle-path",
        default="",
        help="Explicit private CA bundle for the approved Outline HTTPS origin",
    )
    parser.add_argument("--dry-run", action="store_true", help="Validate local manual and prepared payload without network writes")
    args = parser.parse_args(argv)

    manual_path = Path(args.manual)

    try:
        if not args.external_bundle_manifest and not args.expected_manifest_sha256:
            _enforce_current_capture_publish_contract()
        if not args.external_bundle_manifest or not args.expected_manifest_sha256:
            raise ExternalBundleValidationError(
                "EXTERNAL_BUNDLE_ARGUMENTS_REQUIRED",
                "both --external-bundle-manifest and --expected-manifest-sha256 are required",
            )
        bundle_report = validate_external_bundle_manifest(
            Path(args.external_bundle_manifest), args.expected_manifest_sha256
        )
        source_text, document_report = _build_reference_only_outline_text(manual_path)
        outline_url, token = _load_outline_config(args)
        if args.dry_run:
            normalized_outline_url, normalized_outline_origin = _validate_outline_base_url(outline_url)
            report = {
                **bundle_report,
                **document_report,
                "status": "PASS",
                "mode": "dry-run",
                "outline_url": normalized_outline_url,
                "outline_origin": normalized_outline_origin,
                "document_id": args.document_id,
                "title": args.title,
                "token_present": bool(token),
                "network_writes": 0,
                "file_writes": 0,
                "report_write_skipped": bool(args.report_path),
            }
            print(json.dumps(report, ensure_ascii=False, sort_keys=True))
            return 0

        client = OutlineClient(
            outline_url,
            token,
            trusted_upload_origins=args.trusted_upload_origin,
            tls_ca_bundle_path=args.tls_ca_bundle_path,
        )
        client.api(
            "documents.update",
            {
                "id": args.document_id,
                "title": args.title,
                "text": source_text,
                "editMode": "replace",
                "publish": True,
            },
        )
        info = client.api("documents.info", {"id": args.document_id})
        data = info.get("data") or {}
        doc = data.get("document") or data
        doc_text = doc.get("text") or ""
        missing_references = [
            value
            for value in (
                M7_EXTERNAL_CAPTURE_BUNDLE_SCHEMA,
                M7_APP_ID,
                M7_EXTERNAL_CAPTURE_ROOT,
                M7_HANDOVER_INDEX,
                *M7_REQUIRED_STATE_IDS,
            )
            if value not in doc_text
        ]
        report = {
            **bundle_report,
            **document_report,
            "status": "PASS",
            "mode": "publish",
            "outline_url": client.base_url + str(doc.get("url", "")),
            "document_id": args.document_id,
            "unique_images_uploaded": 0,
            "document_markdown_image_refs": doc_text.count("!["),
            "document_digest_values": len(
                re.findall(
                    r"(?i)(?<![0-9a-f])[0-9a-f]{64}(?![0-9a-f])", doc_text
                )
            ),
            "missing_canonical_references": missing_references,
            "document_text_length": len(doc_text),
        }
        if (
            report["document_markdown_image_refs"] != 0
            or report["document_digest_values"] != 0
            or missing_references
        ):
            report["status"] = "FAIL"
            raise ExternalBundleValidationError(
                "PUBLISHED_DOCUMENT_NOT_REFERENCE_ONLY",
                "Outline document contains raster/digest or lacks canonical references",
            )
        _write_report(args.report_path, report)
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        return 0
    except Exception as exc:
        report = {"status": "FAIL", "error": str(exc), "mode": "dry-run" if args.dry_run else "publish"}
        if not args.dry_run and not (
            isinstance(exc, ExternalBundleValidationError)
            and exc.code == "HANDOVER_WRITE_FORBIDDEN"
        ):
            _write_report(args.report_path, report)
        print(json.dumps(report, ensure_ascii=False, sort_keys=True), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

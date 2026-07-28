"""PHS=2 active-label overlay and durable label exchange.

The logistics server remains authoritative for input-tag identity, exact
membership, planning instructions, print attempts, and activation.  This
module keeps the immutable canonical input-tag QR separate from the physical
label scanned by the operator, maintains one bounded local recovery journal,
and only reports print success after Windows GDI accepted and closed a real
spool job.  Reconciliation-driven SINGLE/BATCH/SPLIT/MERGE operations consume
only the server-owned topology returned for a scanned physical label.
"""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import re
import tempfile
import threading
from ctypes import wintypes
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping


PHS_LABEL_EXCHANGE_JOURNAL_VERSION = "label-match-phs-label-exchange-v1"
_TERMINAL_STATES = frozenset({"COMMITTED", "CANCELLED"})
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_RECONCILIATION_ACTION_TYPES = frozenset(
    {"EXCHANGE_DATE", "SPLIT", "MERGE"}
)
_PACKAGING_PROCESS_SIGNATURES = frozenset(
    {
        ("PACKAGE", "TRANSFER", "AVAILABLE", "TRANSFER", "AVAILABLE"),
        (
            "PACKAGE",
            "PACKAGE",
            "AVAILABLE",
            "SHIPPING-WAIT",
            "AVAILABLE",
        ),
        (
            "PACKAGE",
            "PACKAGE",
            "AVAILABLE",
            "PACKAGED",
            "AVAILABLE",
        ),
    }
)


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _stable_key(prefix: str, *parts: Any) -> str:
    digest = hashlib.sha256(
        "|".join(str(value or "") for value in parts).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:{digest}"


def _canonical_members(
    values: Iterable[Any],
    field_name: str = "member_ids",
) -> tuple[str, ...]:
    raw = [str(value or "").strip() for value in values]
    if (
        not raw
        or any(not value for value in raw)
        or len(raw) != len(set(raw))
    ):
        raise PHSLabelWorkflowError(
            "PHS_RECONCILIATION_MEMBERSHIP_INVALID",
            f"{field_name}에 빈 값이나 중복이 있습니다.",
        )
    return tuple(sorted(raw))


def _membership_hash(values: Iterable[Any]) -> str:
    members = _canonical_members(values)
    payload = json.dumps(
        members,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _positive_integer(value: Any, field_name: str) -> int:
    if isinstance(value, bool):
        raise PHSLabelWorkflowError(
            "PHS_LABEL_EVIDENCE_INVALID",
            f"{field_name} 값이 올바르지 않습니다.",
        )
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise PHSLabelWorkflowError(
            "PHS_LABEL_EVIDENCE_INVALID",
            f"{field_name} 값이 올바르지 않습니다.",
        ) from exc
    if parsed < 1:
        raise PHSLabelWorkflowError(
            "PHS_LABEL_EVIDENCE_INVALID",
            f"{field_name} 값이 올바르지 않습니다.",
        )
    return parsed


def parse_compact_phs2(raw_value: Any) -> dict[str, str]:
    value = str(raw_value or "").strip()
    expected_keys = ("PHS", "SRC", "ITG", "CLC", "LBL", "HSH")
    parts = value.split("|") if value else []
    if len(parts) != len(expected_keys):
        raise PHSLabelWorkflowError(
            "PHS2_FORMAT_INVALID",
            "PHS2는 여섯 개의 표준 필드여야 합니다.",
        )
    fields: dict[str, str] = {}
    ordered: list[str] = []
    for part in parts:
        if part.count("=") != 1:
            raise PHSLabelWorkflowError(
                "PHS2_FORMAT_INVALID",
                "PHS2 필드 형식이 올바르지 않습니다.",
            )
        key, field_value = part.split("=", 1)
        key = key.strip().upper()
        field_value = field_value.strip()
        if not key or not field_value or key in fields:
            raise PHSLabelWorkflowError(
                "PHS2_FORMAT_INVALID",
                "PHS2 필드는 비어 있거나 중복될 수 없습니다.",
            )
        ordered.append(key)
        fields[key] = field_value
    if tuple(ordered) != expected_keys:
        raise PHSLabelWorkflowError(
            "PHS2_FORMAT_INVALID",
            "PHS2 필드 순서가 표준과 다릅니다.",
        )
    if (
        fields["PHS"] != "2"
        or fields["SRC"].upper() != "KMTECH_INPUT_TAG"
        or not re.fullmatch(r"[0-9a-fA-F]{16}", fields["HSH"])
    ):
        raise PHSLabelWorkflowError(
            "PHS2_FORMAT_INVALID",
            "중앙 KMTECH_INPUT_TAG PHS2 형식이 아닙니다.",
        )
    fields["SRC"] = "KMTECH_INPUT_TAG"
    fields["HSH"] = fields["HSH"].lower()
    return fields


class PHSLabelWorkflowError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = str(code or "PHS_LABEL_WORKFLOW_ERROR")
        self.retryable = bool(retryable)
        self.details = dict(details or {})


class PHSPhysicalPrintError(PHSLabelWorkflowError):
    def __init__(self, message: str) -> None:
        super().__init__("LOCAL_PRINTER_ERROR", message, retryable=True)


@dataclass(frozen=True)
class PackagingPHSLabelEvidence:
    canonical_input_tag_qr: str
    physical_scanned_qr_payload: str
    active_label_qr_payload: str
    active_label_id: str
    active_label_business_date: str
    active_label_worker_code: str
    active_label_instruction_id: str
    active_label_version: int
    active_membership_version: int
    active_label_resolution: str
    authority_scope_id: str
    input_tag_id: str
    item_id: str
    member_count: int
    membership_hash: str
    replaced_scan: bool = False

    def state_fields(self) -> dict[str, Any]:
        return {
            "canonical_input_tag_qr": self.canonical_input_tag_qr,
            "physical_scanned_qr_payload": self.physical_scanned_qr_payload,
            "active_label_qr_payload": self.active_label_qr_payload,
            "active_label_id": self.active_label_id,
            "active_label_business_date": self.active_label_business_date,
            "active_label_worker_code": self.active_label_worker_code,
            "active_label_instruction_id": self.active_label_instruction_id,
            "active_label_version": self.active_label_version,
            "active_membership_version": self.active_membership_version,
            "active_label_resolution": self.active_label_resolution,
            "phs_label_replaced_scan": self.replaced_scan,
        }


def _validate_active_label(
    label: Mapping[str, Any],
    *,
    canonical_fields: Mapping[str, str],
) -> tuple[dict[str, Any], dict[str, str]]:
    active = dict(label)
    active_qr = str(active.get("qr_payload") or "").strip()
    active_fields = parse_compact_phs2(active_qr)
    if (
        str(active.get("state") or "").strip().upper() != "ACTIVE"
        or str(active.get("label_id") or "").strip()
        != active_fields["LBL"]
        or str(active.get("hash_prefix") or "").strip().lower()
        != active_fields["HSH"]
        or str(active.get("scan_anchor_input_tag_id") or "").strip()
        != canonical_fields["ITG"]
        or str(active.get("item_id") or "").strip()
        != canonical_fields["CLC"]
        or active_fields["ITG"] != canonical_fields["ITG"]
        or active_fields["CLC"] != canonical_fields["CLC"]
    ):
        raise PHSLabelWorkflowError(
            "PHS2_ACTIVE_LABEL_INVALID",
            "현재 ACTIVE 현품표가 immutable input-tag 원본과 일치하지 않습니다.",
        )
    return active, active_fields


def normalize_packaging_phs_label_evidence(
    scanned_qr_payload: str,
    response: Mapping[str, Any],
) -> PackagingPHSLabelEvidence:
    """Validate one exact package source and normalize its label overlay."""

    scanned_qr = str(scanned_qr_payload or "").strip()
    scanned_fields = parse_compact_phs2(scanned_qr)
    data = dict(response or {})
    bundle_value = data.get("bundle")
    input_tag_value = data.get("input_tag")
    if not isinstance(bundle_value, Mapping) or not isinstance(
        input_tag_value, Mapping
    ):
        raise PHSLabelWorkflowError(
            "PHS2_PACKAGE_SOURCE_INVALID",
            "중앙 package source/input-tag 증거가 없습니다.",
        )
    bundle = dict(bundle_value)
    input_tag = dict(input_tag_value)
    canonical_qr = str(input_tag.get("qr_payload") or "").strip()
    canonical_fields = parse_compact_phs2(canonical_qr)
    input_tag_id = str(input_tag.get("input_tag_id") or "").strip()
    item_id = str(input_tag.get("item_id") or "").strip()
    if (
        scanned_fields["ITG"] != canonical_fields["ITG"]
        or scanned_fields["CLC"] != canonical_fields["CLC"]
        or input_tag_id != canonical_fields["ITG"]
        or item_id != canonical_fields["CLC"]
        or str(input_tag.get("label_id") or "").strip()
        != canonical_fields["LBL"]
        or str(input_tag.get("hash_prefix") or "").strip().lower()
        != canonical_fields["HSH"]
    ):
        raise PHSLabelWorkflowError(
            "PHS2_CANONICAL_IDENTITY_MISMATCH",
            "스캔 현품표와 중앙 immutable input-tag 원본이 일치하지 않습니다.",
        )

    try:
        member_count = int(bundle.get("member_count"))
    except (TypeError, ValueError) as exc:
        raise PHSLabelWorkflowError(
            "PHS2_PACKAGE_SOURCE_INVALID",
            "중앙 package source 수량이 올바르지 않습니다.",
        ) from exc
    membership_digest = str(
        bundle.get("membership_hash") or ""
    ).strip().lower()
    authority_scope_id = str(
        bundle.get("authority_scope_id")
        or data.get("authority_scope_id")
        or ""
    ).strip()
    if (
        int(bundle.get("candidate_count") or data.get("candidate_count") or 0)
        != 1
        or str(bundle.get("bundle_role") or "").strip()
        != "PACKAGE_SOURCE"
        or str(bundle.get("bundle_state") or "").strip().upper()
        != "AVAILABLE"
        or str(bundle.get("item_id") or "").strip() != item_id
        or str(bundle.get("source_session_id") or "").strip()
        != input_tag_id
        or member_count < 1
        or not _SHA256.fullmatch(membership_digest)
        or not authority_scope_id
    ):
        raise PHSLabelWorkflowError(
            "PHS2_PACKAGE_SOURCE_INVALID",
            "중앙 package source의 역할·상태·품목·수량 증거가 일치하지 않습니다.",
        )

    resolution_value = data.get("phs_label_resolution")
    if resolution_value is None:
        if (
            scanned_fields["LBL"] != canonical_fields["LBL"]
            or scanned_fields["HSH"] != canonical_fields["HSH"]
        ):
            raise PHSLabelWorkflowError(
                "PHS2_LABEL_OVERLAY_REQUIRED",
                "교체 현품표를 확인할 중앙 active-label overlay가 없습니다.",
            )
        active = {
            "label_id": canonical_fields["LBL"],
            "qr_payload": canonical_qr,
            "hash_prefix": canonical_fields["HSH"],
            "scan_anchor_input_tag_id": canonical_fields["ITG"],
            "item_id": canonical_fields["CLC"],
            "state": "ACTIVE",
        }
        resolution_kind = "LEGACY_ACTIVE"
        replaced = False
    else:
        if not isinstance(resolution_value, Mapping):
            raise PHSLabelWorkflowError(
                "PHS2_LABEL_RESOLUTION_CORRUPT",
                "중앙 active-label resolution 형식이 올바르지 않습니다.",
            )
        resolution = dict(resolution_value)
        resolution_kind = str(
            resolution.get("resolution") or ""
        ).strip().upper()
        status = str(resolution.get("status") or "").strip().upper()
        scanned = resolution.get("scanned_label")
        if not isinstance(scanned, Mapping):
            raise PHSLabelWorkflowError(
                "PHS2_LABEL_RESOLUTION_CORRUPT",
                "중앙 resolution에 실제 scanned-label 증거가 없습니다.",
            )
        scanned_label = dict(scanned)
        resolved_scanned_qr = str(
            scanned_label.get("qr_payload") or ""
        ).strip()
        resolved_scanned_fields = parse_compact_phs2(resolved_scanned_qr)
        if (
            resolved_scanned_qr != scanned_qr
            or any(
                resolved_scanned_fields[key] != scanned_fields[key]
                for key in ("ITG", "CLC", "LBL", "HSH")
            )
            or str(scanned_label.get("label_id") or "").strip()
            != scanned_fields["LBL"]
            or str(scanned_label.get("hash_prefix") or "").strip().lower()
            != scanned_fields["HSH"]
        ):
            raise PHSLabelWorkflowError(
                "PHS2_LABEL_RESOLUTION_MISMATCH",
                "실제 스캔 현품표와 중앙 scanned-label 증거가 다릅니다.",
            )
        if resolution_kind == "OVERLAY_NOT_ACTIVE" or status in {
            "PENDING_ACTIVATION",
            "PRINT_FAILED",
        }:
            raise PHSLabelWorkflowError(
                "PHS2_LABEL_NOT_ACTIVE",
                "아직 ACTIVE가 아닌 현품표는 포장에 사용할 수 없습니다.",
            )
        if resolution_kind not in {
            "OVERLAY_ACTIVE",
            "OVERLAY_REPLACED",
            "LEGACY_ACTIVE",
        }:
            raise PHSLabelWorkflowError(
                "PHS2_LABEL_RESOLUTION_CORRUPT",
                "중앙 현품표 상태를 확정할 수 없습니다.",
            )
        effective = resolution.get("effective_labels")
        if (
            not isinstance(effective, list)
            or len(effective) != 1
            or not isinstance(effective[0], Mapping)
        ):
            raise PHSLabelWorkflowError(
                "PHS2_ACTIVE_LABEL_AMBIGUOUS",
                "현재 ACTIVE successor를 정확히 하나로 확정하지 못했습니다.",
                details={
                    "active_label_count": (
                        len(effective)
                        if isinstance(effective, list)
                        else None
                    )
                },
            )
        active = dict(effective[0])
        replaced = resolution_kind == "OVERLAY_REPLACED"
        if replaced and status != "REPLACED":
            raise PHSLabelWorkflowError(
                "PHS2_LABEL_RESOLUTION_CORRUPT",
                "교체된 현품표의 중앙 상태가 일치하지 않습니다.",
            )
        if not replaced and status != "ACTIVE":
            raise PHSLabelWorkflowError(
                "PHS2_LABEL_NOT_ACTIVE",
                "현재 ACTIVE가 아닌 현품표는 포장에 사용할 수 없습니다.",
            )

    active, active_fields = _validate_active_label(
        active,
        canonical_fields=canonical_fields,
    )
    if not replaced and (
        active_fields["LBL"] != scanned_fields["LBL"]
        or active_fields["HSH"] != scanned_fields["HSH"]
    ):
        raise PHSLabelWorkflowError(
            "PHS2_ACTIVE_LABEL_INVALID",
            "현재 ACTIVE 현품표와 실제 스캔 현품표가 일치하지 않습니다.",
        )
    try:
        label_version = int(active.get("label_version") or 0)
        membership_version = int(active.get("membership_version") or 0)
    except (TypeError, ValueError):
        label_version = membership_version = 0
    return PackagingPHSLabelEvidence(
        canonical_input_tag_qr=canonical_qr,
        physical_scanned_qr_payload=scanned_qr,
        active_label_qr_payload=str(active["qr_payload"]),
        active_label_id=str(active["label_id"]),
        active_label_business_date=str(
            active.get("business_date") or ""
        ).strip(),
        active_label_worker_code=str(
            active.get("worker_code") or ""
        ).strip(),
        active_label_instruction_id=str(
            active.get("instruction_id") or ""
        ).strip(),
        active_label_version=label_version,
        active_membership_version=membership_version,
        active_label_resolution=resolution_kind,
        authority_scope_id=authority_scope_id,
        input_tag_id=input_tag_id,
        item_id=item_id,
        member_count=member_count,
        membership_hash=membership_digest,
        replaced_scan=replaced,
    )


class PHSLabelExchangeJournal:
    """Atomic, fail-closed recovery state for one in-flight exchange."""

    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path)
        self._lock = threading.Lock()

    def load(self) -> dict[str, Any]:
        with self._lock:
            if not self.path.is_file():
                return {}
            try:
                with self.path.open("r", encoding="utf-8") as handle:
                    loaded = json.load(handle)
            except (OSError, json.JSONDecodeError) as exc:
                raise PHSLabelWorkflowError(
                    "PHS_LABEL_JOURNAL_CORRUPT",
                    "현품표 교환 복구 journal을 읽을 수 없습니다.",
                ) from exc
        if (
            not isinstance(loaded, dict)
            or loaded.get("schema_version")
            != PHS_LABEL_EXCHANGE_JOURNAL_VERSION
            or not isinstance(loaded.get("state"), dict)
        ):
            raise PHSLabelWorkflowError(
                "PHS_LABEL_JOURNAL_CORRUPT",
                "현품표 교환 복구 journal 형식이 올바르지 않습니다.",
            )
        return dict(loaded["state"])

    def save(self, state: Mapping[str, Any]) -> dict[str, Any]:
        bounded = dict(state or {})
        bounded["updated_at"] = _utc_now()
        payload = {
            "schema_version": PHS_LABEL_EXCHANGE_JOURNAL_VERSION,
            "state": bounded,
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            descriptor, temporary = tempfile.mkstemp(
                prefix=f"{self.path.name}.",
                suffix=".tmp",
                dir=str(self.path.parent),
            )
            try:
                with os.fdopen(
                    descriptor, "w", encoding="utf-8"
                ) as handle:
                    json.dump(
                        payload,
                        handle,
                        ensure_ascii=False,
                        indent=2,
                        sort_keys=True,
                    )
                    handle.write("\n")
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, self.path)
            finally:
                try:
                    if os.path.exists(temporary):
                        os.remove(temporary)
                except OSError:
                    pass
        return dict(bounded)


@dataclass(frozen=True)
class PhysicalPrintEvidence:
    printer_name: str
    spool_job_id: int
    document_name: str
    submitted_at: str

    def to_server_proof(self) -> dict[str, Any]:
        return {
            "attached": True,
            "proof_kind": "WINDOWS_GDI_SPOOL",
            "local_printer_name": self.printer_name,
            "spool_job_id": int(self.spool_job_id),
            "document_name": self.document_name,
            "submitted_at": self.submitted_at,
            "windows_gdi_end_doc": True,
        }


class WindowsGDIPhysicalLabelPrinter:
    """Submit a rendered PNG to the Windows default printer."""

    _HORZRES = 8
    _VERTRES = 10

    class _DOCINFOW(ctypes.Structure):
        _fields_ = (
            ("cbSize", ctypes.c_int),
            ("lpszDocName", wintypes.LPCWSTR),
            ("lpszOutput", wintypes.LPCWSTR),
            ("lpszDatatype", wintypes.LPCWSTR),
            ("fwType", wintypes.DWORD),
        )

    @staticmethod
    def _default_printer_name() -> str:
        if os.name != "nt":
            raise PHSPhysicalPrintError(
                "실물 현품표 출력은 Windows 프린터에서만 지원됩니다."
            )
        winspool = ctypes.WinDLL("winspool.drv", use_last_error=True)
        get_default = winspool.GetDefaultPrinterW
        get_default.argtypes = (
            wintypes.LPWSTR,
            ctypes.POINTER(wintypes.DWORD),
        )
        get_default.restype = wintypes.BOOL
        size = wintypes.DWORD(0)
        get_default(None, ctypes.byref(size))
        if size.value < 2:
            raise PHSPhysicalPrintError(
                "Windows 기본 프린터가 설정되지 않았습니다."
            )
        buffer = ctypes.create_unicode_buffer(size.value)
        if not get_default(buffer, ctypes.byref(size)):
            raise PHSPhysicalPrintError(
                "Windows 기본 프린터를 확인하지 못했습니다"
                f"({ctypes.get_last_error()})."
            )
        name = str(buffer.value or "").strip()
        if not name:
            raise PHSPhysicalPrintError(
                "Windows 기본 프린터 이름이 비어 있습니다."
            )
        return name

    def print_png(
        self,
        filepath: str,
        *,
        document_name: str,
    ) -> PhysicalPrintEvidence:
        path = Path(str(filepath or "")).resolve()
        if not path.is_file():
            raise PHSPhysicalPrintError(
                "출력할 현품표 PNG 파일이 없습니다."
            )
        printer_name = self._default_printer_name()
        try:
            from PIL import Image, ImageWin
        except Exception as exc:
            raise PHSPhysicalPrintError(
                "실물 현품표 출력에 필요한 Pillow GDI 모듈을 사용할 수 없습니다."
            ) from exc

        gdi32 = ctypes.WinDLL("gdi32", use_last_error=True)
        create_dc = gdi32.CreateDCW
        create_dc.argtypes = (
            wintypes.LPCWSTR,
            wintypes.LPCWSTR,
            wintypes.LPCWSTR,
            ctypes.c_void_p,
        )
        create_dc.restype = wintypes.HDC
        delete_dc = gdi32.DeleteDC
        delete_dc.argtypes = (wintypes.HDC,)
        delete_dc.restype = wintypes.BOOL
        start_doc = gdi32.StartDocW
        start_doc.argtypes = (
            wintypes.HDC,
            ctypes.POINTER(self._DOCINFOW),
        )
        start_doc.restype = ctypes.c_int
        end_doc = gdi32.EndDoc
        end_doc.argtypes = (wintypes.HDC,)
        end_doc.restype = ctypes.c_int
        abort_doc = gdi32.AbortDoc
        abort_doc.argtypes = (wintypes.HDC,)
        abort_doc.restype = ctypes.c_int
        start_page = gdi32.StartPage
        start_page.argtypes = (wintypes.HDC,)
        start_page.restype = ctypes.c_int
        end_page = gdi32.EndPage
        end_page.argtypes = (wintypes.HDC,)
        end_page.restype = ctypes.c_int
        get_caps = gdi32.GetDeviceCaps
        get_caps.argtypes = (wintypes.HDC, ctypes.c_int)
        get_caps.restype = ctypes.c_int

        hdc = create_dc("WINSPOOL", printer_name, None, None)
        if not hdc:
            raise PHSPhysicalPrintError(
                f"프린터 DC를 열지 못했습니다({ctypes.get_last_error()})."
            )
        job_started = False
        try:
            doc_name = str(document_name or path.stem)[:240]
            doc_info = self._DOCINFOW(
                ctypes.sizeof(self._DOCINFOW),
                doc_name,
                None,
                None,
                0,
            )
            job_id = int(start_doc(hdc, ctypes.byref(doc_info)))
            if job_id <= 0:
                raise PHSPhysicalPrintError(
                    "프린터 작업을 시작하지 못했습니다"
                    f"({ctypes.get_last_error()})."
                )
            job_started = True
            if start_page(hdc) <= 0:
                raise PHSPhysicalPrintError(
                    "프린터 페이지를 시작하지 못했습니다"
                    f"({ctypes.get_last_error()})."
                )
            with Image.open(path) as source:
                image = source.convert("RGB")
                page_width = int(get_caps(hdc, self._HORZRES))
                page_height = int(get_caps(hdc, self._VERTRES))
                if page_width <= 0 or page_height <= 0:
                    raise PHSPhysicalPrintError(
                        "프린터 출력 영역을 확인하지 못했습니다."
                    )
                scale = min(
                    page_width / max(1, image.width),
                    page_height / max(1, image.height),
                )
                output_width = max(1, int(round(image.width * scale)))
                output_height = max(
                    1, int(round(image.height * scale))
                )
                left = max(0, (page_width - output_width) // 2)
                top = max(0, (page_height - output_height) // 2)
                ImageWin.Dib(image).draw(
                    hdc,
                    (
                        left,
                        top,
                        left + output_width,
                        top + output_height,
                    ),
                )
            if end_page(hdc) <= 0:
                raise PHSPhysicalPrintError(
                    "프린터 페이지를 완료하지 못했습니다"
                    f"({ctypes.get_last_error()})."
                )
            if end_doc(hdc) <= 0:
                raise PHSPhysicalPrintError(
                    "프린터 작업을 완료하지 못했습니다"
                    f"({ctypes.get_last_error()})."
                )
            job_started = False
            return PhysicalPrintEvidence(
                printer_name=printer_name,
                spool_job_id=job_id,
                document_name=doc_name,
                submitted_at=_utc_now(),
            )
        finally:
            if job_started:
                try:
                    abort_doc(hdc)
                except Exception:
                    pass
            delete_dc(hdc)


@dataclass(frozen=True)
class RenderedPHSLabel:
    path: str
    sha256: str


class PHSLabelRenderer:
    def __init__(self, output_root: str | os.PathLike[str]):
        self.output_root = Path(output_root)

    @staticmethod
    def _font(size: int, *, bold: bool = False):
        from PIL import ImageFont

        candidates = (
            (
                r"C:\Windows\Fonts\malgunbd.ttf",
                r"C:\Windows\Fonts\malgun.ttf",
            )
            if bold
            else (
                r"C:\Windows\Fonts\malgun.ttf",
                r"C:\Windows\Fonts\malgunbd.ttf",
            )
        )
        for candidate in candidates:
            try:
                return ImageFont.truetype(candidate, size=size)
            except OSError:
                continue
        try:
            return ImageFont.truetype(
                "DejaVuSans.ttf", size=size
            )
        except OSError:
            return ImageFont.load_default()

    def render(
        self,
        current_set: Mapping[str, Any],
        target: Mapping[str, Any],
    ) -> RenderedPHSLabel:
        try:
            import qrcode
            from PIL import Image, ImageDraw
        except Exception as exc:
            raise PHSPhysicalPrintError(
                "현품표 PNG 생성에 필요한 qrcode/Pillow 모듈이 없습니다."
            ) from exc
        label_id = str(target.get("label_id") or "").strip()
        qr_payload = str(target.get("qr_payload") or "").strip()
        business_date = str(
            target.get("business_date") or ""
        ).strip()
        worker_code = str(target.get("worker_code") or "").strip()
        if not all((label_id, qr_payload, business_date, worker_code)):
            raise PHSLabelWorkflowError(
                "PHS_TARGET_LABEL_INVALID",
                "중앙 target label의 QR/date/worker-code 증거가 불완전합니다.",
            )
        parse_compact_phs2(qr_payload)
        safe_label = re.sub(
            r"[^A-Za-z0-9._-]+", "_", label_id
        )[:120]
        folder = (
            self.output_root
            / business_date
            / "phs_label_exchange"
        )
        folder.mkdir(parents=True, exist_ok=True)
        output_path = folder / f"{safe_label}.png"

        qr = qrcode.QRCode(
            version=None,
            error_correction=qrcode.constants.ERROR_CORRECT_M,
            box_size=10,
            border=4,
        )
        qr.add_data(qr_payload)
        qr.make(fit=True)
        qr_image = qr.make_image(
            fill_color="black", back_color="white"
        ).convert("RGB")
        qr_image.thumbnail((440, 440))

        parsed = list(current_set.get("parsed") or [])
        item_code = str(
            target.get("item_id")
            or (parsed[0] if parsed else "")
        )
        item_name = str(
            current_set.get("item_name_override") or ""
        )
        source = current_set.get("package_source_snapshot")
        source = dict(source) if isinstance(source, Mapping) else {}
        quantity = int(
            target.get("member_count")
            or target.get("qty_pcs")
            or source.get("member_count")
            or 0
        )
        canvas = Image.new("RGB", (1100, 600), "white")
        draw = ImageDraw.Draw(canvas)
        draw.rectangle((8, 8, 1091, 591), outline="black", width=5)
        canvas.paste(qr_image, (40, 80))
        draw.text(
            (530, 55),
            "PHS 현품표",
            fill="black",
            font=self._font(48, bold=True),
        )
        draw.text(
            (530, 145),
            f"작업일  {business_date}",
            fill="black",
            font=self._font(38, bold=True),
        )
        draw.text(
            (530, 215),
            f"작업코드  {worker_code}",
            fill="black",
            font=self._font(34, bold=True),
        )
        draw.text(
            (530, 290),
            f"품목  {item_code}",
            fill="black",
            font=self._font(29),
        )
        if item_name:
            draw.text(
                (530, 345),
                item_name[:28],
                fill="black",
                font=self._font(27),
            )
        draw.text(
            (530, 410),
            f"수량  {quantity} Pcs",
            fill="black",
            font=self._font(31, bold=True),
        )
        draw.text(
            (530, 490),
            f"Label  {label_id[:38]}",
            fill="black",
            font=self._font(18),
        )

        descriptor, temporary = tempfile.mkstemp(
            prefix=f"{output_path.stem}.",
            suffix=".png",
            dir=str(folder),
        )
        os.close(descriptor)
        try:
            canvas.save(temporary, format="PNG", optimize=True)
            os.replace(temporary, output_path)
        finally:
            try:
                if os.path.exists(temporary):
                    os.remove(temporary)
            except OSError:
                pass
        digest = hashlib.sha256(
            output_path.read_bytes()
        ).hexdigest()
        return RenderedPHSLabel(str(output_path), digest)


@dataclass(frozen=True)
class PHSLabelExchangeResult:
    status: str
    success: bool
    message: str
    error_code: str = ""
    retryable: bool = False
    exchange_id: str = ""
    journal_state: dict[str, Any] = field(default_factory=dict)


class PHSLabelExchangeCoordinator:
    """Run or resume one server-owned SINGLE label exchange."""

    def __init__(
        self,
        client: Any,
        journal: PHSLabelExchangeJournal,
        renderer: PHSLabelRenderer,
        printer: Any | None = None,
    ) -> None:
        self.client = client
        self.journal = journal
        self.renderer = renderer
        self.printer = printer or WindowsGDIPhysicalLabelPrinter()

    def available(self) -> bool:
        required = (
            "list_phs_work_instruction_candidates",
            "resolve_active_phs_label",
            "adopt_phs_label",
            "prepare_phs_label_exchange",
            "get_phs_label_exchange",
            "request_phs_label_print",
            "complete_phs_label_print",
            "activate_phs_label_exchange",
        )
        return self.client is not None and all(
            callable(getattr(self.client, name, None))
            for name in required
        )

    def reconciliation_available(self) -> bool:
        required = (
            "resolve_phs_reconciliation_actions",
            "prepare_phs_reconciliation_label_exchange",
            "get_phs_label_exchange",
            "request_phs_label_print",
            "complete_phs_label_print",
            "activate_phs_label_exchange",
        )
        return self.client is not None and all(
            callable(getattr(self.client, name, None))
            for name in required
        )

    @staticmethod
    def _reconciliation_resolution(
        response: Mapping[str, Any],
        *,
        authority_scope_id: str,
        scan_payload: str,
    ) -> dict[str, Any]:
        try:
            snapshot = json.loads(
                json.dumps(
                    dict(response or {}),
                    ensure_ascii=False,
                    allow_nan=False,
                )
            )
        except (TypeError, ValueError) as exc:
            raise PHSLabelWorkflowError(
                "PHS_RECONCILIATION_RESPONSE_INVALID",
                "중앙 reconciliation 응답을 복구 journal에 저장할 수 없습니다.",
            ) from exc
        scope = str(authority_scope_id or "").strip()
        scanned_qr = str(scan_payload or "").strip()
        scanned_fields = parse_compact_phs2(scanned_qr)
        scan = (
            dict(snapshot.get("scan"))
            if isinstance(snapshot.get("scan"), Mapping)
            else {}
        )
        reconciliation = (
            dict(snapshot.get("reconciliation"))
            if isinstance(snapshot.get("reconciliation"), Mapping)
            else {}
        )
        selection = (
            dict(snapshot.get("selection"))
            if isinstance(snapshot.get("selection"), Mapping)
            else {}
        )
        actions_value = snapshot.get("actions")
        if (
            not str(snapshot.get("contract_version") or "").strip()
            or str(snapshot.get("authority_scope_id") or "").strip()
            != scope
            or str(snapshot.get("process_context") or "")
            .strip()
            .lower()
            != "packaging"
            or not isinstance(actions_value, list)
            or not actions_value
        ):
            raise PHSLabelWorkflowError(
                "PHS_RECONCILIATION_RESPONSE_INVALID",
                "중앙 action 응답의 scope/process/cardinality가 올바르지 않습니다.",
            )

        active_qr = str(scan.get("active_qr_payload") or "").strip()
        active_fields = parse_compact_phs2(active_qr)
        scanned_label_id = str(
            scan.get("scanned_label_id") or ""
        ).strip()
        active_label_id = str(
            scan.get("active_label_id") or ""
        ).strip()
        if (
            scanned_label_id != scanned_fields["LBL"]
            or active_label_id != active_fields["LBL"]
            or any(
                scanned_fields[key] != active_fields[key]
                for key in ("ITG", "CLC")
            )
            or bool(scan.get("replacement_required"))
            != (scanned_label_id != active_label_id)
            or str(scan.get("resolution") or "").strip().upper()
            not in {
                "OVERLAY_ACTIVE",
                "OVERLAY_REPLACED",
                "LEGACY_ACTIVE",
            }
        ):
            raise PHSLabelWorkflowError(
                "PHS_RECONCILIATION_SCAN_INVALID",
                "스캔 현품표와 중앙 ACTIVE successor 증거가 일치하지 않습니다.",
            )

        reconciliation_id = str(
            reconciliation.get("reconciliation_id") or ""
        ).strip()
        reconciliation_state = str(
            reconciliation.get("state") or ""
        ).strip().upper()
        reconciliation_version = _positive_integer(
            reconciliation.get("entity_version"),
            "reconciliation_entity_version",
        )
        try:
            reconciliation_date = date.fromisoformat(
                str(reconciliation.get("business_date") or "")
            )
        except ValueError as exc:
            raise PHSLabelWorkflowError(
                "PHS_RECONCILIATION_RESPONSE_INVALID",
                "reconciliation 작업일이 올바르지 않습니다.",
            ) from exc
        if (
            not reconciliation_id
            or reconciliation_state not in {"PROPOSED", "APPROVED"}
            or reconciliation_date.isoformat()
            != str(reconciliation.get("business_date") or "")
        ):
            raise PHSLabelWorkflowError(
                "PHS_RECONCILIATION_RESPONSE_INVALID",
                "reconciliation id/state/date가 올바르지 않습니다.",
            )

        normalized_actions: list[dict[str, Any]] = []
        action_ids: list[str] = []
        selected_member_ids: list[str] = []
        source_label_ids: set[str] = set()
        for action_value in actions_value:
            if not isinstance(action_value, Mapping):
                raise PHSLabelWorkflowError(
                    "PHS_RECONCILIATION_ACTION_INVALID",
                    "중앙 action 형식이 올바르지 않습니다.",
                )
            action = dict(action_value)
            action_id = str(action.get("action_id") or "").strip()
            action_type = str(
                action.get("action_type") or ""
            ).strip().upper()
            action_state = str(
                action.get("action_state") or ""
            ).strip().upper()
            exchange_id = str(action.get("exchange_id") or "").strip()
            item_id = str(action.get("item_id") or "").strip()
            sources_value = action.get("sources")
            targets_value = action.get("targets")
            process_value = action.get("process_membership")
            if (
                not action_id
                or action_id in action_ids
                or action_type not in _RECONCILIATION_ACTION_TYPES
                or action_state not in {"PROPOSED", "APPROVED"}
                or (action_state == "PROPOSED" and exchange_id)
                or (action_state == "APPROVED" and not exchange_id)
                or not item_id
                or not isinstance(sources_value, list)
                or not sources_value
                or not isinstance(targets_value, list)
                or not targets_value
                or not isinstance(process_value, list)
                or not process_value
            ):
                raise PHSLabelWorkflowError(
                    "PHS_RECONCILIATION_ACTION_INVALID",
                    "중앙 action의 id/type/state/topology가 올바르지 않습니다.",
                )

            sources: list[dict[str, Any]] = []
            action_members: list[str] = []
            for source_value in sources_value:
                if not isinstance(source_value, Mapping):
                    raise PHSLabelWorkflowError(
                        "PHS_RECONCILIATION_SOURCE_INVALID",
                        "중앙 source label 형식이 올바르지 않습니다.",
                    )
                source = dict(source_value)
                label_id = str(
                    source.get("source_label_id") or ""
                ).strip()
                source_item = str(source.get("item_id") or "").strip()
                qr_payload = str(source.get("qr_payload") or "").strip()
                qr_fields = parse_compact_phs2(qr_payload)
                members_value = source.get("member_ids")
                if not isinstance(members_value, list):
                    raise PHSLabelWorkflowError(
                        "PHS_RECONCILIATION_SOURCE_INVALID",
                        "중앙 source의 exact member_ids가 없습니다.",
                    )
                members = _canonical_members(
                    members_value,
                    "source.member_ids",
                )
                membership_digest = str(
                    source.get("membership_hash") or ""
                ).strip().lower()
                try:
                    source_date = date.fromisoformat(
                        str(source.get("business_date") or "")
                    )
                except ValueError as exc:
                    raise PHSLabelWorkflowError(
                        "PHS_RECONCILIATION_SOURCE_INVALID",
                        "중앙 source 작업일이 올바르지 않습니다.",
                    ) from exc
                if (
                    not label_id
                    or label_id in source_label_ids
                    or not str(source.get("group_id") or "").strip()
                    or not str(source.get("instruction_id") or "").strip()
                    or source_item != item_id
                    or qr_fields["LBL"] != label_id
                    or qr_fields["CLC"] != item_id
                    or source_date.isoformat()
                    != str(source.get("business_date") or "")
                    or _positive_integer(
                        source.get("item_daily_ordinal"),
                        "source.item_daily_ordinal",
                    )
                    < 1
                    or not str(source.get("worker_code") or "").strip()
                    or _positive_integer(
                        source.get("qty_pcs"),
                        "source.qty_pcs",
                    )
                    != len(members)
                    or _positive_integer(
                        source.get("label_version"),
                        "source.label_version",
                    )
                    < 1
                    or _positive_integer(
                        source.get("membership_version"),
                        "source.membership_version",
                    )
                    < 1
                    or not _SHA256.fullmatch(membership_digest)
                    or membership_digest != _membership_hash(members)
                ):
                    raise PHSLabelWorkflowError(
                        "PHS_RECONCILIATION_SOURCE_INVALID",
                        "중앙 source의 label/item/date/quantity/membership이 다릅니다.",
                    )
                source["member_ids"] = list(members)
                source["membership_hash"] = membership_digest
                sources.append(source)
                source_label_ids.add(label_id)
                action_members.extend(members)

            targets: list[dict[str, Any]] = []
            target_instruction_ids: set[str] = set()
            for target_value in targets_value:
                if not isinstance(target_value, Mapping):
                    raise PHSLabelWorkflowError(
                        "PHS_RECONCILIATION_TARGET_INVALID",
                        "중앙 target instruction 형식이 올바르지 않습니다.",
                    )
                target = dict(target_value)
                instruction_id = str(
                    target.get("instruction_id") or ""
                ).strip()
                try:
                    target_date = date.fromisoformat(
                        str(target.get("business_date") or "")
                    )
                except ValueError as exc:
                    raise PHSLabelWorkflowError(
                        "PHS_RECONCILIATION_TARGET_INVALID",
                        "중앙 target 작업일이 올바르지 않습니다.",
                    ) from exc
                if (
                    not instruction_id
                    or instruction_id in target_instruction_ids
                    or str(target.get("item_id") or "").strip()
                    != item_id
                    or target_date.isoformat()
                    != str(target.get("business_date") or "")
                    or _positive_integer(
                        target.get("item_daily_ordinal"),
                        "target.item_daily_ordinal",
                    )
                    < 1
                    or not str(target.get("worker_code") or "").strip()
                    or _positive_integer(
                        target.get("qty_pcs"),
                        "target.qty_pcs",
                    )
                    < 1
                ):
                    raise PHSLabelWorkflowError(
                        "PHS_RECONCILIATION_TARGET_INVALID",
                        "중앙 target의 instruction/item/date/quantity가 다릅니다.",
                    )
                target_instruction_ids.add(instruction_id)
                targets.append(target)

            action_union = _canonical_members(
                action_members,
                "action.source_member_ids",
            )
            source_union_value = action.get("source_member_ids")
            if not isinstance(source_union_value, list):
                raise PHSLabelWorkflowError(
                    "PHS_RECONCILIATION_MEMBERSHIP_INVALID",
                    "중앙 action source member union이 없습니다.",
                )
            response_union = _canonical_members(
                source_union_value,
                "action.source_member_ids",
            )
            if (
                response_union != action_union
                or _positive_integer(
                    action.get("source_member_union_count"),
                    "source_member_union_count",
                )
                != len(action_union)
                or str(
                    action.get("source_member_union_hash") or ""
                ).strip().lower()
                != _membership_hash(action_union)
            ):
                raise PHSLabelWorkflowError(
                    "PHS_RECONCILIATION_MEMBERSHIP_INVALID",
                    "중앙 action의 exact member union/hash가 일치하지 않습니다.",
                )

            topology_ok = (
                action_type == "EXCHANGE_DATE"
                and len(sources) == 1
                and len(targets) == 1
                and int(sources[0]["qty_pcs"])
                == int(targets[0]["qty_pcs"])
            ) or (
                action_type == "SPLIT"
                and len(sources) == 1
                and len(targets) >= 2
                and sum(int(value["qty_pcs"]) for value in targets)
                == len(action_union)
            ) or (
                action_type == "MERGE"
                and len(sources) >= 2
                and len(targets) == 1
                and sum(int(value["qty_pcs"]) for value in sources)
                == int(targets[0]["qty_pcs"])
            )
            split_value = action.get("split_member_ids_by_target")
            normalized_split: dict[str, list[str]] = {}
            if action_type == "SPLIT":
                if not isinstance(split_value, Mapping):
                    topology_ok = False
                else:
                    partition: list[str] = []
                    for target in targets:
                        instruction_id = str(target["instruction_id"])
                        raw_partition = split_value.get(instruction_id)
                        if not isinstance(raw_partition, list):
                            topology_ok = False
                            break
                        exact_partition = _canonical_members(
                            raw_partition,
                            "split.member_ids",
                        )
                        if len(exact_partition) != int(
                            target["qty_pcs"]
                        ):
                            topology_ok = False
                            break
                        normalized_split[instruction_id] = list(
                            exact_partition
                        )
                        partition.extend(exact_partition)
                    if topology_ok:
                        try:
                            topology_ok = (
                                _canonical_members(
                                    partition,
                                    "split.partition",
                                )
                                == action_union
                            )
                        except PHSLabelWorkflowError:
                            topology_ok = False
            elif split_value not in ({}, None):
                topology_ok = False
            if not topology_ok:
                raise PHSLabelWorkflowError(
                    "PHS_RECONCILIATION_TOPOLOGY_INVALID",
                    "중앙 action의 SINGLE/SPLIT/MERGE topology가 올바르지 않습니다.",
                )

            process_members: list[str] = []
            for process_value_row in process_value:
                if not isinstance(process_value_row, Mapping):
                    raise PHSLabelWorkflowError(
                        "PHS_RECONCILIATION_PROCESS_INVALID",
                        "중앙 process membership 형식이 올바르지 않습니다.",
                    )
                row = dict(process_value_row)
                unit_id = str(row.get("unit_id") or "").strip()
                signature = (
                    str(row.get("owner_type") or "").strip().upper(),
                    str(row.get("bundle_type") or "").strip().upper(),
                    str(row.get("bundle_state") or "").strip().upper(),
                    str(row.get("location_code") or "").strip().upper(),
                    str(row.get("unit_state") or "").strip().upper(),
                )
                if (
                    not unit_id
                    or not str(row.get("owner_id") or "").strip()
                    or signature not in _PACKAGING_PROCESS_SIGNATURES
                ):
                    raise PHSLabelWorkflowError(
                        "PHS_RECONCILIATION_PROCESS_INVALID",
                        "중앙 source membership이 포장 공정 위치/상태와 다릅니다.",
                    )
                process_members.append(unit_id)
            if (
                _canonical_members(
                    process_members,
                    "process_membership",
                )
                != action_union
            ):
                raise PHSLabelWorkflowError(
                    "PHS_RECONCILIATION_PROCESS_INVALID",
                    "중앙 포장 process membership cardinality가 다릅니다.",
                )

            action["sources"] = sources
            action["targets"] = targets
            action["source_member_ids"] = list(action_union)
            action["source_member_union_hash"] = _membership_hash(
                action_union
            )
            action["split_member_ids_by_target"] = normalized_split
            normalized_actions.append(action)
            action_ids.append(action_id)
            selected_member_ids.extend(action_union)

        try:
            _canonical_members(
                selected_member_ids,
                "selection.source_member_ids",
            )
        except PHSLabelWorkflowError as exc:
            raise PHSLabelWorkflowError(
                "PHS_RECONCILIATION_TOPOLOGY_INVALID",
                "선택된 action들의 source membership이 서로 중복됩니다.",
            ) from exc

        selection_ids_value = selection.get("action_ids")
        if not isinstance(selection_ids_value, list):
            selection_ids: list[str] = []
        else:
            selection_ids = [
                str(value or "").strip()
                for value in selection_ids_value
            ]
        action_types = {
            str(action.get("action_type") or "").strip().upper()
            for action in normalized_actions
        }
        if action_types == {"EXCHANGE_DATE"}:
            expected_mode = (
                "MULTI_EXCHANGE_DATE"
                if len(normalized_actions) > 1
                else "SINGLE_EXCHANGE_DATE"
            )
        elif (
            len(normalized_actions) == 1
            and action_types <= {"SPLIT", "MERGE"}
        ):
            expected_mode = "SINGLE_TOPOLOGY"
        else:
            expected_mode = ""
        if (
            not expected_mode
            or str(selection.get("mode") or "").strip().upper()
            != expected_mode
            or selection_ids != action_ids
            or len(selection_ids) != len(set(selection_ids))
            or str(selection.get("reconciliation_id") or "").strip()
            != reconciliation_id
            or _positive_integer(
                selection.get("expected_reconciliation_version"),
                "expected_reconciliation_version",
            )
            != reconciliation_version
        ):
            raise PHSLabelWorkflowError(
                "PHS_RECONCILIATION_SELECTION_INVALID",
                "중앙 selection과 action topology가 일치하지 않습니다.",
            )
        if active_label_id not in source_label_ids:
            raise PHSLabelWorkflowError(
                "PHS_RECONCILIATION_SCAN_INVALID",
                "스캔한 ACTIVE label이 선택 action source에 없습니다.",
            )
        active_source = next(
            source
            for action in normalized_actions
            for source in action["sources"]
            if str(source["source_label_id"]) == active_label_id
        )
        if str(active_source.get("qr_payload") or "").strip() != active_qr:
            raise PHSLabelWorkflowError(
                "PHS_RECONCILIATION_SCAN_INVALID",
                "스캔 ACTIVE label QR과 action source snapshot이 다릅니다.",
            )

        snapshot["scan"] = scan
        snapshot["reconciliation"] = reconciliation
        snapshot["selection"] = selection
        snapshot["actions"] = normalized_actions
        snapshot["scan_payload"] = scanned_qr
        return snapshot

    def resolve_reconciliation_actions(
        self,
        *,
        authority_scope_id: str,
        scan_payload: str,
        limit: int = 20,
    ) -> dict[str, Any]:
        if not self.reconciliation_available():
            raise PHSLabelWorkflowError(
                "PHS_LABEL_API_UNAVAILABLE",
                "중앙 reconciliation 현품표 교체 API가 설정되지 않았습니다.",
            )
        scope = str(authority_scope_id or "").strip()
        if not scope:
            scope = str(
                getattr(
                    getattr(self.client, "config", None),
                    "authority_scope_id",
                    "",
                )
                or ""
            ).strip()
        if not scope:
            raise PHSLabelWorkflowError(
                "PHS_RECONCILIATION_SCOPE_REQUIRED",
                "현품표 교체 authority scope가 없습니다.",
            )
        response = self.client.resolve_phs_reconciliation_actions(
            authority_scope_id=scope,
            scan_payload=str(scan_payload or "").strip(),
            process_context="packaging",
            limit=min(20, max(1, int(limit))),
        )
        return self._reconciliation_resolution(
            response,
            authority_scope_id=scope,
            scan_payload=scan_payload,
        )

    @staticmethod
    def _current_identity(
        current_set: Mapping[str, Any],
    ) -> tuple[dict[str, str], str, int, str]:
        raw = list(current_set.get("raw") or [])
        parsed = list(current_set.get("parsed") or [])
        canonical = str(
            current_set.get("canonical_input_tag_qr")
            or (raw[0] if raw else "")
        ).strip()
        fields = parse_compact_phs2(canonical)
        if (
            len(raw) != 1
            or len(parsed) != 1
            or str(parsed[0] or "").strip() != fields["CLC"]
            or not current_set.get("central_inherit_all")
        ):
            raise PHSLabelWorkflowError(
                "PHS_LABEL_EXCHANGE_PRECONDITION",
                "포장 중인 중앙 PHS2 한 장이 필요합니다.",
            )
        snapshot_value = current_set.get("package_source_snapshot")
        snapshot = (
            dict(snapshot_value)
            if isinstance(snapshot_value, Mapping)
            else {}
        )
        scope = str(
            snapshot.get("authority_scope_id") or ""
        ).strip()
        quantity = _positive_integer(
            snapshot.get("member_count"), "member_count"
        )
        membership_hash = str(
            snapshot.get("membership_hash") or ""
        ).strip().lower()
        if not scope or not _SHA256.fullmatch(membership_hash):
            raise PHSLabelWorkflowError(
                "PHS_LABEL_SOURCE_SNAPSHOT_INVALID",
                "포장 source의 scope/member 증거가 없습니다.",
            )
        return fields, scope, quantity, membership_hash

    def list_candidates(
        self,
        current_set: Mapping[str, Any],
        business_date: str,
    ) -> list[dict[str, Any]]:
        if not self.available():
            raise PHSLabelWorkflowError(
                "PHS_LABEL_API_UNAVAILABLE",
                "중앙 현품표 교환 API가 설정되지 않았습니다.",
            )
        target_date = str(business_date or "").strip()
        try:
            parsed_date = date.fromisoformat(target_date)
        except ValueError as exc:
            raise PHSLabelWorkflowError(
                "PHS_BUSINESS_DATE_INVALID",
                "작업일은 YYYY-MM-DD 실제 달력 날짜여야 합니다.",
            ) from exc
        if parsed_date.isoformat() != target_date:
            raise PHSLabelWorkflowError(
                "PHS_BUSINESS_DATE_INVALID",
                "작업일은 YYYY-MM-DD 형식이어야 합니다.",
            )
        fields, scope, quantity, _membership_hash = (
            self._current_identity(current_set)
        )
        response = self.client.list_phs_work_instruction_candidates(
            authority_scope_id=scope,
            business_date=target_date,
            item_id=fields["CLC"],
            target_qty_pcs=quantity,
            limit=50,
        )
        candidates = response.get("candidates")
        try:
            candidate_count = int(
                response.get("candidate_count") or 0
            )
        except (TypeError, ValueError):
            candidate_count = -1
        if (
            str(response.get("authority_scope_id") or "").strip()
            != scope
            or str(response.get("business_date") or "").strip()
            != target_date
            or str(response.get("item_id") or "").strip()
            != fields["CLC"]
            or str(response.get("uom") or "").strip().upper()
            != "PCS"
            or int(response.get("target_qty_pcs") or 0)
            != quantity
            or not isinstance(candidates, list)
            or candidate_count != len(candidates)
        ):
            raise PHSLabelWorkflowError(
                "PHS_TARGET_CANDIDATES_INVALID",
                "중앙 후보의 scope/date/item/quantity/cardinality가 현재 포장과 다릅니다.",
            )
        current_instruction_id = str(
            current_set.get("active_label_instruction_id") or ""
        ).strip()
        exact: list[dict[str, Any]] = []
        for candidate_value in candidates:
            if not isinstance(candidate_value, Mapping):
                raise PHSLabelWorkflowError(
                    "PHS_TARGET_CANDIDATES_INVALID",
                    "중앙 후보 형식이 올바르지 않습니다.",
                )
            candidate = dict(candidate_value)
            if (
                not str(candidate.get("instruction_id") or "").strip()
                or str(candidate.get("business_date") or "").strip()
                != target_date
                or str(candidate.get("item_id") or "").strip()
                != fields["CLC"]
                or str(candidate.get("uom") or "").strip().upper()
                != "PCS"
                or _positive_integer(
                    candidate.get("target_qty_pcs"),
                    "target_qty_pcs",
                )
                != quantity
                or _positive_integer(
                    candidate.get("item_daily_ordinal"),
                    "item_daily_ordinal",
                )
                < 1
                or _positive_integer(
                    candidate.get("entity_version"),
                    "entity_version",
                )
                < 1
                or str(candidate.get("state") or "").strip().upper()
                != "PLANNED"
                or not str(candidate.get("worker_code") or "").strip()
            ):
                raise PHSLabelWorkflowError(
                    "PHS_TARGET_CANDIDATES_INVALID",
                    "중앙 후보에 exact PLANNED 작업지시가 아닌 항목이 있습니다.",
                )
            if (
                str(candidate.get("instruction_id") or "").strip()
                != current_instruction_id
            ):
                exact.append(candidate)
        return exact

    def _save(
        self,
        state: Mapping[str, Any],
        **updates: Any,
    ) -> dict[str, Any]:
        return self.journal.save({**dict(state or {}), **updates})

    def blocks_other_action(
        self,
        current_set: Mapping[str, Any],
    ) -> bool:
        state = self.journal.load()
        if not state:
            return False
        status = str(state.get("status") or "").strip().upper()
        if status in _TERMINAL_STATES:
            return False
        return (
            str(state.get("set_id") or "").strip()
            == str(current_set.get("id") or "").strip()
            and str(state.get("canonical_input_tag_qr") or "").strip()
            == str(
                current_set.get("canonical_input_tag_qr")
                or ((current_set.get("raw") or [""])[0])
            ).strip()
        )

    @staticmethod
    def _active_from_projection(
        projection: Mapping[str, Any],
        canonical_fields: Mapping[str, str],
    ) -> dict[str, Any]:
        resolution = dict(projection or {})
        status = str(resolution.get("status") or "").strip().upper()
        kind = str(
            resolution.get("resolution") or ""
        ).strip().upper()
        if status != "ACTIVE" or kind not in {
            "LEGACY_ACTIVE",
            "OVERLAY_ACTIVE",
            "OVERLAY_REPLACED",
        }:
            raise PHSLabelWorkflowError(
                "PHS2_LABEL_NOT_ACTIVE",
                "현재 ACTIVE 현품표를 확정하지 못했습니다.",
            )
        input_tag = resolution.get("input_tag")
        if isinstance(input_tag, Mapping):
            projected_canonical = parse_compact_phs2(
                str(input_tag.get("qr_payload") or "")
            )
            if any(
                projected_canonical[key] != canonical_fields[key]
                for key in ("ITG", "CLC", "LBL", "HSH")
            ):
                raise PHSLabelWorkflowError(
                    "PHS2_CANONICAL_IDENTITY_MISMATCH",
                    "중앙 active-label 원본이 현재 포장 원본과 다릅니다.",
                )
        active_value = resolution.get("active_label")
        if not isinstance(active_value, Mapping):
            effective = resolution.get("effective_labels")
            if (
                not isinstance(effective, list)
                or len(effective) != 1
                or not isinstance(effective[0], Mapping)
            ):
                raise PHSLabelWorkflowError(
                    "PHS2_ACTIVE_LABEL_AMBIGUOUS",
                    "현재 ACTIVE 현품표를 정확히 하나로 확정하지 못했습니다.",
                )
            active_value = effective[0]
        active, _fields = _validate_active_label(
            active_value,
            canonical_fields=canonical_fields,
        )
        return active

    def _refresh_active_source(
        self,
        current_set: dict[str, Any],
        *,
        persist_current_set: Callable[[], bool],
    ) -> tuple[dict[str, str], str, int, str, dict[str, Any]]:
        fields, scope, quantity, membership_hash = (
            self._current_identity(current_set)
        )
        projection = self.client.resolve_active_phs_label(
            fields["ITG"],
            authority_scope_id=scope,
        )
        active = self._active_from_projection(projection, fields)
        active_qr = str(active.get("qr_payload") or "").strip()
        changed = any(
            (
                str(
                    current_set.get("active_label_qr_payload") or ""
                ).strip()
                != active_qr,
                str(current_set.get("active_label_id") or "").strip()
                != str(active.get("label_id") or "").strip(),
                str(
                    current_set.get("active_label_business_date")
                    or ""
                ).strip()
                != str(active.get("business_date") or "").strip(),
                str(
                    current_set.get("active_label_worker_code") or ""
                ).strip()
                != str(active.get("worker_code") or "").strip(),
            )
        )
        updates = {
            "active_label_qr_payload": active_qr,
            "active_label_id": str(
                active.get("label_id") or ""
            ).strip(),
            "active_label_business_date": str(
                active.get("business_date") or ""
            ).strip(),
            "active_label_worker_code": str(
                active.get("worker_code") or ""
            ).strip(),
            "active_label_instruction_id": str(
                active.get("instruction_id") or ""
            ).strip(),
            "active_label_version": int(
                active.get("label_version") or 0
            ),
            "active_membership_version": int(
                active.get("membership_version") or 0
            ),
            "active_label_resolution": str(
                projection.get("resolution") or ""
            ).strip().upper(),
        }
        previous = {
            key: (key in current_set, current_set.get(key))
            for key in updates
        }
        current_set.update(updates)
        if changed and not persist_current_set():
            for key, (existed, value) in previous.items():
                if existed:
                    current_set[key] = value
                else:
                    current_set.pop(key, None)
            raise PHSLabelWorkflowError(
                "PHS_LOCAL_STATE_WRITE_FAILED",
                "현재 ACTIVE 현품표를 로컬 복구 상태에 저장하지 못했습니다.",
            )
        return fields, scope, quantity, membership_hash, active

    @staticmethod
    def _source_versions(
        active: Mapping[str, Any],
    ) -> tuple[str, int, int]:
        label_id = str(active.get("label_id") or "").strip()
        label_version = _positive_integer(
            active.get("label_version"), "label_version"
        )
        membership_version = _positive_integer(
            active.get("membership_version"),
            "membership_version",
        )
        return label_id, label_version, membership_version

    @staticmethod
    def _exchange_projection(
        central: Mapping[str, Any],
        *,
        state: Mapping[str, Any],
        current_set: Mapping[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        exchange = (
            dict(central.get("exchange"))
            if isinstance(central.get("exchange"), Mapping)
            else {}
        )
        targets = central.get("target_labels")
        sources = central.get("source_labels")
        if (
            str(exchange.get("exchange_id") or "").strip()
            != str(state.get("exchange_id") or "").strip()
            or str(exchange.get("exchange_kind") or "")
            .strip()
            .upper()
            != "SINGLE"
            or not isinstance(targets, list)
            or len(targets) != 1
            or not isinstance(targets[0], Mapping)
            or not isinstance(sources, list)
            or len(sources) != 1
            or not isinstance(sources[0], Mapping)
        ):
            raise PHSLabelWorkflowError(
                "PHS_EXCHANGE_ACK_INVALID",
                "중앙 SINGLE exchange/source/target cardinality가 일치하지 않습니다.",
            )
        target = dict(targets[0])
        source = dict(sources[0])
        target_fields = parse_compact_phs2(
            str(target.get("qr_payload") or "")
        )
        canonical_fields = parse_compact_phs2(
            str(state.get("canonical_input_tag_qr") or "")
        )
        instruction = (
            dict(state.get("target_instruction"))
            if isinstance(state.get("target_instruction"), Mapping)
            else {}
        )
        source_snapshot = current_set.get("package_source_snapshot")
        source_snapshot = (
            dict(source_snapshot)
            if isinstance(source_snapshot, Mapping)
            else {}
        )
        exchange_state = str(exchange.get("state") or "").strip().upper()
        expected_source_state = (
            "SUPERSEDED" if exchange_state == "COMMITTED" else "ACTIVE"
        )
        allowed_target_states = (
            {"ACTIVE"}
            if exchange_state == "COMMITTED"
            else {"PENDING_ACTIVATION", "PRINT_FAILED"}
        )
        source_label_version = _positive_integer(
            source.get("label_version"), "source_label_version"
        )
        source_membership_version = _positive_integer(
            source.get("membership_version"),
            "source_membership_version",
        )
        saved_source_label_version = _positive_integer(
            state.get("source_label_version"),
            "saved_source_label_version",
        )
        saved_source_membership_version = _positive_integer(
            state.get("source_membership_version"),
            "saved_source_membership_version",
        )
        source_versions_match = (
            source_label_version >= saved_source_label_version
            and source_membership_version
            >= saved_source_membership_version
            if exchange_state == "COMMITTED"
            else source_label_version == saved_source_label_version
            and source_membership_version
            == saved_source_membership_version
        )
        source_membership_hash = str(
            source.get("membership_hash") or ""
        ).strip().lower()
        target_membership_hash = str(
            target.get("membership_hash") or ""
        ).strip().lower()
        if (
            str(source.get("label_id") or "").strip()
            != str(state.get("source_label_id") or "").strip()
            or str(source.get("state") or "").strip().upper()
            != expected_source_state
            or str(source.get("scan_anchor_input_tag_id") or "").strip()
            != canonical_fields["ITG"]
            or str(source.get("item_id") or "").strip()
            != canonical_fields["CLC"]
            or not source_versions_match
            or _positive_integer(
                source.get("member_count"), "source_label_member_count"
            )
            != _positive_integer(
                source_snapshot.get("member_count"),
                "source_member_count",
            )
            or not _SHA256.fullmatch(source_membership_hash)
            or source_membership_hash
            != str(
                source_snapshot.get("membership_hash") or ""
            ).strip().lower()
            or target_fields["ITG"] != canonical_fields["ITG"]
            or target_fields["CLC"] != canonical_fields["CLC"]
            or str(target.get("scan_anchor_input_tag_id") or "").strip()
            != canonical_fields["ITG"]
            or str(target.get("item_id") or "").strip()
            != canonical_fields["CLC"]
            or str(target.get("label_id") or "").strip()
            != target_fields["LBL"]
            or str(target.get("hash_prefix") or "").strip().lower()
            != target_fields["HSH"]
            or str(target.get("instruction_id") or "").strip()
            != str(instruction.get("instruction_id") or "").strip()
            or str(target.get("business_date") or "").strip()
            != str(instruction.get("business_date") or "").strip()
            or str(target.get("worker_code") or "").strip()
            != str(instruction.get("worker_code") or "").strip()
            or str(target.get("state") or "").strip().upper()
            not in allowed_target_states
            or _positive_integer(
                target.get("label_version"), "target_label_version"
            )
            < 1
            or _positive_integer(
                target.get("membership_version"),
                "target_membership_version",
            )
            < 1
            or _positive_integer(
                target.get("member_count"), "target_member_count"
            )
            != _positive_integer(
                source_snapshot.get("member_count"),
                "source_member_count",
            )
            or not _SHA256.fullmatch(target_membership_hash)
            or target_membership_hash != source_membership_hash
        ):
            raise PHSLabelWorkflowError(
                "PHS_TARGET_LABEL_INVALID",
                "중앙 source/target 상태·QR·date·code·anchor·membership이 "
                "현재 포장과 다릅니다.",
            )
        return exchange, target

    @staticmethod
    def _reconciliation_edges(
        resolution: Mapping[str, Any],
    ) -> tuple[
        str,
        list[dict[str, Any]],
        dict[str, dict[str, Any]],
        dict[str, dict[str, Any]],
        dict[str, tuple[str, ...]],
    ]:
        actions = list(resolution.get("actions") or [])
        action_types = {
            str(action.get("action_type") or "").strip().upper()
            for action in actions
            if isinstance(action, Mapping)
        }
        if action_types == {"EXCHANGE_DATE"}:
            exchange_kind = "BATCH" if len(actions) > 1 else "SINGLE"
        elif len(actions) == 1 and action_types in ({"SPLIT"}, {"MERGE"}):
            exchange_kind = next(iter(action_types))
        else:
            raise PHSLabelWorkflowError(
                "PHS_RECONCILIATION_TOPOLOGY_INVALID",
                "저장된 action topology가 올바르지 않습니다.",
            )
        edges: list[dict[str, Any]] = []
        sources: dict[str, dict[str, Any]] = {}
        targets: dict[str, dict[str, Any]] = {}
        target_members: dict[str, tuple[str, ...]] = {}
        for action in actions:
            action_type = str(action["action_type"]).upper()
            action_sources = list(action["sources"])
            action_targets = list(action["targets"])
            for source in action_sources:
                sources[str(source["source_label_id"])] = dict(source)
            for target in action_targets:
                targets[str(target["instruction_id"])] = dict(target)
            if action_type == "EXCHANGE_DATE":
                source = dict(action_sources[0])
                target = dict(action_targets[0])
                members = tuple(source["member_ids"])
                target_members[str(target["instruction_id"])] = members
                edges.append(
                    {
                        "edge_role": "PAIR",
                        "source": source,
                        "target": target,
                        "members": members,
                    }
                )
            elif action_type == "SPLIT":
                source = dict(action_sources[0])
                partitions = dict(
                    action.get("split_member_ids_by_target") or {}
                )
                for target_value in action_targets:
                    target = dict(target_value)
                    members = tuple(
                        partitions[str(target["instruction_id"])]
                    )
                    target_members[str(target["instruction_id"])] = members
                    edges.append(
                        {
                            "edge_role": "SPLIT_SUCCESSOR",
                            "source": source,
                            "target": target,
                            "members": members,
                        }
                    )
            else:
                target = dict(action_targets[0])
                merged: list[str] = []
                for source_value in action_sources:
                    source = dict(source_value)
                    members = tuple(source["member_ids"])
                    merged.extend(members)
                    edges.append(
                        {
                            "edge_role": "MERGE_SOURCE",
                            "source": source,
                            "target": target,
                            "members": members,
                        }
                    )
                target_members[str(target["instruction_id"])] = (
                    _canonical_members(merged, "merge.target_members")
                )
        return exchange_kind, edges, sources, targets, target_members

    @classmethod
    def _reconciliation_exchange_projection(
        cls,
        central: Mapping[str, Any],
        *,
        state: Mapping[str, Any],
    ) -> tuple[
        dict[str, Any],
        list[dict[str, Any]],
        dict[str, dict[str, Any]],
        list[dict[str, Any]],
    ]:
        resolution_value = state.get("action_resolution")
        if not isinstance(resolution_value, Mapping):
            raise PHSLabelWorkflowError(
                "PHS_RECONCILIATION_JOURNAL_INVALID",
                "복구 journal에 action snapshot이 없습니다.",
            )
        resolution = dict(resolution_value)
        (
            expected_kind,
            expected_edges,
            expected_sources,
            expected_targets,
            target_members,
        ) = cls._reconciliation_edges(resolution)
        exchange = (
            dict(central.get("exchange"))
            if isinstance(central.get("exchange"), Mapping)
            else {}
        )
        source_values = central.get("source_labels")
        target_values = central.get("target_labels")
        item_values = central.get("items")
        exchange_id = str(state.get("exchange_id") or "").strip()
        exchange_state = str(exchange.get("state") or "").strip().upper()
        if (
            str(exchange.get("exchange_id") or "").strip() != exchange_id
            or str(exchange.get("exchange_kind") or "").strip().upper()
            != expected_kind
            or exchange_state
            not in {
                "PREPARED",
                "PRINT_FAILED",
                "PRINT_PARTIAL",
                "READY",
                "COMMITTED",
            }
            or not isinstance(source_values, list)
            or not isinstance(target_values, list)
            or not isinstance(item_values, list)
            or len(source_values) != len(expected_sources)
            or len(target_values) != len(expected_targets)
            or len(item_values) != len(expected_edges)
        ):
            raise PHSLabelWorkflowError(
                "PHS_RECONCILIATION_EXCHANGE_INVALID",
                "중앙 exchange kind/state/source/target/cardinality가 다릅니다.",
            )

        source_labels: dict[str, dict[str, Any]] = {}
        for source_value in source_values:
            if not isinstance(source_value, Mapping):
                raise PHSLabelWorkflowError(
                    "PHS_RECONCILIATION_EXCHANGE_INVALID",
                    "중앙 source label 형식이 올바르지 않습니다.",
                )
            source = dict(source_value)
            label_id = str(source.get("label_id") or "").strip()
            expected = expected_sources.get(label_id)
            qr_fields = parse_compact_phs2(
                str(source.get("qr_payload") or "")
            )
            full_hash = str(
                source.get("label_instance_hash") or ""
            ).strip().lower()
            hash_prefix = str(
                source.get("hash_prefix") or ""
            ).strip().lower()
            expected_state = (
                "SUPERSEDED" if exchange_state == "COMMITTED" else "ACTIVE"
            )
            label_version = _positive_integer(
                source.get("label_version"),
                "source.label_version",
            )
            membership_version = _positive_integer(
                source.get("membership_version"),
                "source.membership_version",
            )
            committed_versions_ok = bool(
                expected
                and label_version
                >= int(expected["label_version"])
                and membership_version
                >= int(expected["membership_version"])
            )
            pending_versions_ok = bool(
                expected
                and label_version == int(expected["label_version"])
                and membership_version
                == int(expected["membership_version"])
            )
            if (
                expected is None
                or label_id in source_labels
                or str(source.get("state") or "").strip().upper()
                != expected_state
                or qr_fields["LBL"] != label_id
                or qr_fields["ITG"]
                != parse_compact_phs2(expected["qr_payload"])["ITG"]
                or qr_fields["CLC"] != str(expected["item_id"])
                or str(source.get("group_id") or "").strip()
                != str(expected["group_id"])
                or str(source.get("instruction_id") or "").strip()
                != str(expected["instruction_id"])
                or str(source.get("business_date") or "").strip()
                != str(expected["business_date"])
                or _positive_integer(
                    source.get("member_count"),
                    "source.member_count",
                )
                != len(expected["member_ids"])
                or str(
                    source.get("membership_hash") or ""
                ).strip().lower()
                != str(expected["membership_hash"]).lower()
                or not _SHA256.fullmatch(full_hash)
                or full_hash[:16] != hash_prefix
                or hash_prefix != qr_fields["HSH"]
                or not (
                    committed_versions_ok
                    if exchange_state == "COMMITTED"
                    else pending_versions_ok
                )
            ):
                raise PHSLabelWorkflowError(
                    "PHS_RECONCILIATION_SOURCE_INVALID",
                    "중앙 source label identity/version/membership이 action과 다릅니다.",
                )
            source_labels[label_id] = source

        target_labels: dict[str, dict[str, Any]] = {}
        target_labels_by_id: dict[str, dict[str, Any]] = {}
        for target_value in target_values:
            if not isinstance(target_value, Mapping):
                raise PHSLabelWorkflowError(
                    "PHS_RECONCILIATION_EXCHANGE_INVALID",
                    "중앙 target label 형식이 올바르지 않습니다.",
                )
            target = dict(target_value)
            instruction_id = str(
                target.get("instruction_id") or ""
            ).strip()
            expected = expected_targets.get(instruction_id)
            members = target_members.get(instruction_id)
            label_id = str(target.get("label_id") or "").strip()
            qr_fields = parse_compact_phs2(
                str(target.get("qr_payload") or "")
            )
            full_hash = str(
                target.get("label_instance_hash") or ""
            ).strip().lower()
            hash_prefix = str(
                target.get("hash_prefix") or ""
            ).strip().lower()
            if exchange_state == "COMMITTED":
                allowed_states = {"ACTIVE"}
            else:
                allowed_states = {"PENDING_ACTIVATION", "PRINT_FAILED"}
            if (
                expected is None
                or members is None
                or instruction_id in target_labels
                or not label_id
                or label_id in target_labels_by_id
                or str(target.get("state") or "").strip().upper()
                not in allowed_states
                or qr_fields["LBL"] != label_id
                or qr_fields["CLC"] != str(expected["item_id"])
                or str(target.get("item_id") or "").strip()
                != str(expected["item_id"])
                or str(target.get("business_date") or "").strip()
                != str(expected["business_date"])
                or str(target.get("worker_code") or "").strip()
                != str(expected["worker_code"])
                or _positive_integer(
                    target.get("item_daily_ordinal"),
                    "target.item_daily_ordinal",
                )
                != int(expected["item_daily_ordinal"])
                or _positive_integer(
                    target.get("member_count"),
                    "target.member_count",
                )
                != len(members)
                or str(
                    target.get("membership_hash") or ""
                ).strip().lower()
                != _membership_hash(members)
                or _positive_integer(
                    target.get("label_version"),
                    "target.label_version",
                )
                < 1
                or _positive_integer(
                    target.get("membership_version"),
                    "target.membership_version",
                )
                < 1
                or not _SHA256.fullmatch(full_hash)
                or full_hash[:16] != hash_prefix
                or hash_prefix != qr_fields["HSH"]
            ):
                raise PHSLabelWorkflowError(
                    "PHS_RECONCILIATION_TARGET_INVALID",
                    "중앙 target label identity/date/code/membership이 action과 다릅니다.",
                )
            target_labels[instruction_id] = target
            target_labels_by_id[label_id] = target

        expected_item_keys = {
            (
                str(edge["source"]["source_label_id"]),
                str(edge["target"]["instruction_id"]),
            ): edge
            for edge in expected_edges
        }
        seen_item_keys: set[tuple[str, str]] = set()
        for item_value in item_values:
            if not isinstance(item_value, Mapping):
                raise PHSLabelWorkflowError(
                    "PHS_RECONCILIATION_EXCHANGE_INVALID",
                    "중앙 exchange item 형식이 올바르지 않습니다.",
                )
            item = dict(item_value)
            key = (
                str(item.get("source_label_id") or "").strip(),
                str(item.get("after_instruction_id") or "").strip(),
            )
            edge = expected_item_keys.get(key)
            target = target_labels.get(key[1])
            if (
                edge is None
                or target is None
                or key in seen_item_keys
                or str(item.get("edge_role") or "").strip().upper()
                != str(edge["edge_role"])
                or str(item.get("target_label_id") or "").strip()
                != str(target["label_id"])
                or str(item.get("source_group_id") or "").strip()
                != str(edge["source"]["group_id"])
                or str(item.get("before_instruction_id") or "").strip()
                != str(edge["source"]["instruction_id"])
                or str(item.get("before_business_date") or "").strip()
                != str(edge["source"]["business_date"])
                or str(item.get("after_business_date") or "").strip()
                != str(edge["target"]["business_date"])
                or _positive_integer(
                    item.get("member_count"),
                    "exchange_item.member_count",
                )
                != len(edge["members"])
                or str(
                    item.get("membership_hash") or ""
                ).strip().lower()
                != _membership_hash(edge["members"])
            ):
                raise PHSLabelWorkflowError(
                    "PHS_RECONCILIATION_EXCHANGE_INVALID",
                    "중앙 exchange item edge/membership이 action과 다릅니다.",
                )
            seen_item_keys.add(key)
        if seen_item_keys != set(expected_item_keys):
            raise PHSLabelWorkflowError(
                "PHS_RECONCILIATION_EXCHANGE_INVALID",
                "중앙 exchange item edge cardinality가 다릅니다.",
            )
        ordered_targets = [
            target_labels[str(target["instruction_id"])]
            for action in resolution["actions"]
            for target in action["targets"]
        ]
        return exchange, ordered_targets, target_labels_by_id, [
            dict(value) for value in item_values
        ]

    @staticmethod
    def _validate_reconciliation_prepare_link(
        prepared: Mapping[str, Any],
        *,
        state: Mapping[str, Any],
    ) -> None:
        selection = dict(
            (state.get("action_resolution") or {}).get("selection")
            or {}
        )
        approved = prepared.get("approved_action_ids")
        reconciliation = (
            dict(prepared.get("reconciliation"))
            if isinstance(prepared.get("reconciliation"), Mapping)
            else {}
        )
        if (
            not isinstance(approved, list)
            or [
                str(value or "").strip()
                for value in approved
            ]
            != list(selection.get("action_ids") or [])
            or str(
                reconciliation.get("reconciliation_id") or ""
            ).strip()
            != str(selection.get("reconciliation_id") or "").strip()
            or str(reconciliation.get("state") or "").strip().upper()
            != "APPROVED"
            or _positive_integer(
                reconciliation.get("entity_version"),
                "prepared.reconciliation_entity_version",
            )
            < _positive_integer(
                selection.get("expected_reconciliation_version"),
                "expected_reconciliation_version",
            )
        ):
            raise PHSLabelWorkflowError(
                "PHS_RECONCILIATION_PREPARE_ACK_INVALID",
                "중앙 prepare ACK의 approved action/reconciliation이 다릅니다.",
            )

    @staticmethod
    def _print_proof(value: Any) -> dict[str, Any]:
        if callable(getattr(value, "to_server_proof", None)):
            proof = dict(value.to_server_proof())
        elif isinstance(value, Mapping):
            proof = dict(value)
        else:
            proof = {}
        if (
            proof.get("attached") is not True
            or not proof.get("spool_job_id")
            or str(proof.get("proof_kind") or "").strip()
            != "WINDOWS_GDI_SPOOL"
            or proof.get("windows_gdi_end_doc") is not True
        ):
            raise PHSPhysicalPrintError(
                "실제 Windows GDI spool 완료 증거가 불완전합니다."
            )
        return proof

    @staticmethod
    def _apply_target(
        current_set: dict[str, Any],
        target: Mapping[str, Any],
        *,
        persist_current_set: Callable[[], bool],
    ) -> None:
        canonical = str(
            current_set.get("canonical_input_tag_qr")
            or ((current_set.get("raw") or [""])[0])
        ).strip()
        canonical_fields = parse_compact_phs2(canonical)
        target_fields = parse_compact_phs2(
            str(target.get("qr_payload") or "")
        )
        before_raw = list(current_set.get("raw") or [])
        before_parsed = list(current_set.get("parsed") or [])
        before_snapshot = dict(
            current_set.get("package_source_snapshot") or {}
        )
        if (
            target_fields["ITG"] != canonical_fields["ITG"]
            or target_fields["CLC"] != canonical_fields["CLC"]
            or len(before_raw) != 1
            or before_raw[0] != canonical
            or len(before_parsed) != 1
        ):
            raise PHSLabelWorkflowError(
                "PHS_LOCAL_TARGET_PRECONDITION",
                "로컬 PHS2 원본/진행 상태가 교환 중 변경됐습니다.",
            )
        updates = {
            "active_label_qr_payload": str(
                target.get("qr_payload") or ""
            ).strip(),
            "active_label_id": str(
                target.get("label_id") or ""
            ).strip(),
            "active_label_business_date": str(
                target.get("business_date") or ""
            ).strip(),
            "active_label_worker_code": str(
                target.get("worker_code") or ""
            ).strip(),
            "active_label_instruction_id": str(
                target.get("instruction_id") or ""
            ).strip(),
            "active_label_version": _positive_integer(
                target.get("label_version"), "label_version"
            ),
            "active_membership_version": _positive_integer(
                target.get("membership_version"),
                "membership_version",
            ),
            "active_label_resolution": "OVERLAY_REPLACED",
            "phs_label_replaced_scan": True,
            "phs_label_guidance": (
                "현재 ACTIVE 현품표 "
                f"{target.get('worker_code')} "
                f"({target.get('business_date')})"
            ),
        }
        previous = {
            key: (key in current_set, current_set.get(key))
            for key in updates
        }
        current_set.update(updates)
        if (
            list(current_set.get("raw") or []) != before_raw
            or list(current_set.get("parsed") or []) != before_parsed
            or dict(
                current_set.get("package_source_snapshot") or {}
            )
            != before_snapshot
        ):
            for key, (existed, value) in previous.items():
                if existed:
                    current_set[key] = value
                else:
                    current_set.pop(key, None)
            raise PHSLabelWorkflowError(
                "PHS_LOCAL_MEMBERSHIP_CHANGED",
                "현품표 교환이 포장 raw/membership을 변경했습니다.",
            )
        if not persist_current_set():
            for key, (existed, value) in previous.items():
                if existed:
                    current_set[key] = value
                else:
                    current_set.pop(key, None)
            raise PHSLabelWorkflowError(
                "PHS_LOCAL_STATE_WRITE_FAILED",
                "중앙 COMMITTED 후 로컬 ACTIVE 현품표를 저장하지 못했습니다.",
                retryable=True,
            )

    @classmethod
    def _apply_reconciliation_local(
        cls,
        current_set: dict[str, Any] | None,
        *,
        state: Mapping[str, Any],
        targets_by_id: Mapping[str, Mapping[str, Any]],
        items: list[dict[str, Any]],
        persist_current_set: Callable[[], bool] | None,
    ) -> None:
        if not current_set or persist_current_set is None:
            return
        raw = list(current_set.get("raw") or [])
        parsed = list(current_set.get("parsed") or [])
        snapshot_value = current_set.get("package_source_snapshot")
        if (
            len(raw) != 1
            or len(parsed) != 1
            or not isinstance(snapshot_value, Mapping)
        ):
            return
        snapshot = dict(snapshot_value)
        canonical = str(
            current_set.get("canonical_input_tag_qr") or raw[0]
        ).strip()
        canonical_fields = parse_compact_phs2(canonical)
        resolution = dict(state.get("action_resolution") or {})
        (
            exchange_kind,
            _edges,
            expected_sources,
            _expected_targets,
            _target_members,
        ) = cls._reconciliation_edges(resolution)
        source_matches = []
        for source_id, source in expected_sources.items():
            source_fields = parse_compact_phs2(source["qr_payload"])
            if (
                source_fields["ITG"] == canonical_fields["ITG"]
                and source_fields["CLC"] == canonical_fields["CLC"]
                and int(source["qty_pcs"])
                == int(snapshot.get("member_count") or 0)
                and str(source["membership_hash"]).lower()
                == str(snapshot.get("membership_hash") or "").strip().lower()
                and str(snapshot.get("authority_scope_id") or "").strip()
                == str(state.get("authority_scope_id") or "").strip()
            ):
                source_matches.append(source_id)
        if not source_matches:
            return
        if len(source_matches) != 1:
            raise PHSLabelWorkflowError(
                "PHS_LOCAL_TARGET_PRECONDITION",
                "현재 포장 set이 여러 reconciliation source와 중복 일치합니다.",
            )
        current_source_id = source_matches[0]
        if exchange_kind in {"SINGLE", "BATCH"}:
            matching_items = [
                item
                for item in items
                if str(item.get("source_label_id") or "").strip()
                == current_source_id
            ]
            if len(matching_items) != 1:
                raise PHSLabelWorkflowError(
                    "PHS_LOCAL_TARGET_PRECONDITION",
                    "현재 포장 source의 1:1 successor를 확정하지 못했습니다.",
                )
            target_id = str(
                matching_items[0].get("target_label_id") or ""
            ).strip()
            target = targets_by_id.get(target_id)
            if target is None:
                raise PHSLabelWorkflowError(
                    "PHS_LOCAL_TARGET_PRECONDITION",
                    "현재 포장 source의 target label이 없습니다.",
                )
            cls._apply_target(
                current_set,
                target,
                persist_current_set=persist_current_set,
            )
            return

        successor_ids = list(
            dict.fromkeys(
                str(item.get("target_label_id") or "").strip()
                for item in items
                if str(item.get("source_label_id") or "").strip()
                == current_source_id
            )
        )
        successors = [
            dict(targets_by_id[value])
            for value in successor_ids
            if value in targets_by_id
        ]
        if not successors:
            raise PHSLabelWorkflowError(
                "PHS_LOCAL_TARGET_PRECONDITION",
                "SPLIT/MERGE successor label을 확정하지 못했습니다.",
            )
        before_raw = list(raw)
        before_parsed = list(parsed)
        before_snapshot = dict(snapshot)
        updates = {
            "active_label_qr_payload": "",
            "active_label_id": "",
            "active_label_business_date": "",
            "active_label_worker_code": "",
            "active_label_instruction_id": "",
            "active_label_version": 0,
            "active_membership_version": 0,
            "active_label_resolution": "TOPOLOGY_REFRESH_REQUIRED",
            "phs_label_topology_refresh_required": True,
            "phs_label_topology_successors": successors,
            "phs_label_exchange_id": str(
                state.get("exchange_id") or ""
            ).strip(),
            "phs_label_guidance": (
                f"{exchange_kind} 완료: successor 현품표 "
                f"{len(successors)}장이 생성됐습니다. 현재 포장은 "
                "완료 차단 상태로 보존되며 새 현품표별 작업을 확인해야 합니다."
            ),
        }
        previous = {
            key: (key in current_set, current_set.get(key))
            for key in updates
        }
        current_set.update(updates)
        if (
            list(current_set.get("raw") or []) != before_raw
            or list(current_set.get("parsed") or []) != before_parsed
            or dict(
                current_set.get("package_source_snapshot") or {}
            )
            != before_snapshot
        ):
            for key, (existed, value) in previous.items():
                if existed:
                    current_set[key] = value
                else:
                    current_set.pop(key, None)
            raise PHSLabelWorkflowError(
                "PHS_LOCAL_MEMBERSHIP_CHANGED",
                "현품표 topology 교체가 포장 원장 상태를 변경했습니다.",
            )
        if not persist_current_set():
            for key, (existed, value) in previous.items():
                if existed:
                    current_set[key] = value
                else:
                    current_set.pop(key, None)
            raise PHSLabelWorkflowError(
                "PHS_LOCAL_STATE_WRITE_FAILED",
                "중앙 COMMITTED 후 topology refresh 상태를 저장하지 못했습니다.",
                retryable=True,
            )

    def has_pending_reconciliation(self) -> bool:
        state = self.journal.load()
        return bool(
            state
            and str(state.get("workflow_mode") or "").strip().upper()
            == "RECONCILIATION"
            and str(state.get("status") or "").strip().upper()
            not in _TERMINAL_STATES
        )

    @staticmethod
    def _result(
        state: Mapping[str, Any],
        *,
        success: bool,
        message: str,
        error_code: str = "",
        retryable: bool = False,
    ) -> PHSLabelExchangeResult:
        return PHSLabelExchangeResult(
            status=str(state.get("status") or ""),
            success=success,
            message=message,
            error_code=error_code,
            retryable=retryable,
            exchange_id=str(state.get("exchange_id") or ""),
            journal_state=dict(state),
        )

    def execute_single(
        self,
        current_set: dict[str, Any],
        target_instruction: Mapping[str, Any] | None,
        *,
        persist_current_set: Callable[[], bool],
        status_callback: Callable[[str], None] | None = None,
        confirm_ambiguous_reprint: bool = False,
    ) -> PHSLabelExchangeResult:
        if not self.available():
            raise PHSLabelWorkflowError(
                "PHS_LABEL_API_UNAVAILABLE",
                "중앙 현품표 교환 API가 설정되지 않았습니다.",
            )
        notify = status_callback or (lambda _message: None)
        state = self.journal.load()
        journal_status = str(
            state.get("status") or ""
        ).strip().upper()
        canonical = str(
            current_set.get("canonical_input_tag_qr")
            or ((current_set.get("raw") or [""])[0])
        ).strip()
        set_id = str(current_set.get("id") or "").strip()
        recoverable = bool(
            state
            and journal_status not in _TERMINAL_STATES
            and str(state.get("canonical_input_tag_qr") or "").strip()
            == canonical
            and str(state.get("set_id") or "").strip() == set_id
        )
        if state and journal_status not in _TERMINAL_STATES and not recoverable:
            raise PHSLabelWorkflowError(
                "PHS_LABEL_RECOVERY_CONFLICT",
                "다른 포장 세트의 미완료 현품표 교환이 남아 있습니다.",
            )
        requested_instruction_id = str(
            (target_instruction or {}).get("instruction_id") or ""
        ).strip()
        if recoverable:
            saved_instruction_id = str(
                state.get("target_instruction_id") or ""
            ).strip()
            if (
                requested_instruction_id
                and requested_instruction_id != saved_instruction_id
            ):
                raise PHSLabelWorkflowError(
                    "PHS_LABEL_RECOVERY_CONFLICT",
                    "선택한 작업지시가 로컬 복구 journal과 다릅니다.",
                )
        else:
            if not requested_instruction_id:
                raise PHSLabelWorkflowError(
                    "PHS_TARGET_INSTRUCTION_REQUIRED",
                    "교환할 작업지시를 선택해야 합니다.",
                )
            requested_date = str(
                (target_instruction or {}).get("business_date") or ""
            ).strip()
            candidates = self.list_candidates(
                current_set, requested_date
            )
            exact_target = next(
                (
                    dict(candidate)
                    for candidate in candidates
                    if str(
                        candidate.get("instruction_id") or ""
                    ).strip()
                    == requested_instruction_id
                    and int(candidate.get("entity_version") or 0)
                    == int(
                        (target_instruction or {}).get(
                            "entity_version"
                        )
                        or 0
                    )
                ),
                None,
            )
            if exact_target is None:
                raise PHSLabelWorkflowError(
                    "PHS_TARGET_INSTRUCTION_STALE",
                    "선택한 작업지시가 더 이상 exact PLANNED 후보가 아닙니다.",
                )
            (
                canonical_fields,
                scope,
                _quantity,
                membership_hash,
                active,
            ) = self._refresh_active_source(
                current_set,
                persist_current_set=persist_current_set,
            )
            if (
                str(
                    current_set.get("active_label_resolution") or ""
                ).strip().upper()
                == "LEGACY_ACTIVE"
            ):
                notify("기존 PHS2를 중앙 active-label overlay에 등록합니다.")
                adopted = self.client.adopt_phs_label(
                    authority_scope_id=scope,
                    qr_payload=canonical,
                    business_date=str(
                        current_set.get(
                            "active_label_business_date"
                        )
                        or ""
                    ),
                )
                active_value = adopted.get("label")
                if not isinstance(active_value, Mapping):
                    raise PHSLabelWorkflowError(
                        "PHS_LABEL_ADOPT_ACK_INVALID",
                        "중앙 overlay 등록 ACK에 ACTIVE label이 없습니다.",
                    )
                active, _active_fields = _validate_active_label(
                    active_value,
                    canonical_fields=canonical_fields,
                )
            source_label_id, label_version, membership_version = (
                self._source_versions(active)
            )
            prepare_key = _stable_key(
                "label-match-phs-label-single-prepare",
                scope,
                canonical_fields["ITG"],
                source_label_id,
                label_version,
                membership_version,
                requested_instruction_id,
                exact_target.get("entity_version"),
            )
            state = self._save(
                {},
                status="PREPARE_PENDING",
                set_id=set_id,
                authority_scope_id=scope,
                input_tag_id=canonical_fields["ITG"],
                canonical_input_tag_qr=canonical,
                source_membership_hash=membership_hash,
                source_label_id=source_label_id,
                source_label_version=label_version,
                source_membership_version=membership_version,
                target_instruction_id=requested_instruction_id,
                target_instruction=exact_target,
                prepare_idempotency_key=prepare_key,
                print_attempt_no=0,
            )

        scope = str(
            state.get("authority_scope_id") or ""
        ).strip()
        exchange_id = str(state.get("exchange_id") or "").strip()
        notify("중앙 prepare/복구 상태를 확인합니다.")
        if not exchange_id:
            prepared = self.client.prepare_phs_label_exchange(
                authority_scope_id=scope,
                exchange_kind="SINGLE",
                sources=[
                    {
                        "source_label_id": str(
                            state.get("source_label_id") or ""
                        ).strip(),
                        "expected_label_version": _positive_integer(
                            state.get("source_label_version"),
                            "source_label_version",
                        ),
                        "expected_membership_version": (
                            _positive_integer(
                                state.get(
                                    "source_membership_version"
                                ),
                                "source_membership_version",
                            )
                        ),
                    }
                ],
                targets=[
                    {
                        "target_instruction_id": str(
                            state.get("target_instruction_id") or ""
                        ).strip()
                    }
                ],
                idempotency_key=str(
                    state.get("prepare_idempotency_key") or ""
                ).strip(),
            )
            exchange_value = (
                dict(prepared.get("exchange"))
                if isinstance(prepared.get("exchange"), Mapping)
                else {}
            )
            exchange_id = str(
                exchange_value.get("exchange_id") or ""
            ).strip()
            if not exchange_id:
                raise PHSLabelWorkflowError(
                    "PHS_PREPARE_ACK_INVALID",
                    "중앙 prepare ACK에 exchange id가 없습니다.",
                    retryable=True,
                )
            state = self._save(
                state,
                status="PREPARED",
                exchange_id=exchange_id,
                prepare_ack=dict(prepared),
            )
            central = prepared
        else:
            central = self.client.get_phs_label_exchange(
                exchange_id,
                authority_scope_id=scope,
            )
        exchange, target = self._exchange_projection(
            central,
            state=state,
            current_set=current_set,
        )
        state = self._save(
            state,
            target_label=target,
            exchange_entity_version=_positive_integer(
                exchange.get("entity_version"),
                "exchange_entity_version",
            ),
        )
        exchange_state = str(
            exchange.get("state") or ""
        ).strip().upper()
        if exchange_state == "COMMITTED":
            try:
                self._apply_target(
                    current_set,
                    target,
                    persist_current_set=persist_current_set,
                )
            except PHSLabelWorkflowError:
                self._save(
                    state,
                    status="COMMITTED_LOCAL_REFRESH_PENDING",
                    committed_ack=dict(central),
                )
                raise
            committed = self._save(
                state,
                status="COMMITTED",
                committed_ack=dict(central),
            )
            return self._result(
                committed,
                success=True,
                message=(
                    "현품표 날짜 교환을 복구했습니다: "
                    f"{target.get('business_date')} · "
                    f"{target.get('worker_code')}"
                ),
            )

        journal_status = str(
            state.get("status") or ""
        ).strip().upper()
        if journal_status == "PRINT_FAILURE_ACK_PENDING":
            failed_ack = self.client.complete_phs_label_print(
                str(state.get("print_attempt_id") or ""),
                authority_scope_id=scope,
                succeeded=False,
                error_code=str(
                    state.get("print_error_code")
                    or "LOCAL_PRINTER_ERROR"
                ),
                error_message=str(
                    state.get("print_error_message")
                    or "Local physical printer failed."
                )[:1024],
            )
            failed = self._save(
                state,
                status="PRINT_FAILED",
                print_failure_ack=dict(failed_ack),
            )
            return self._result(
                failed,
                success=False,
                message=(
                    "기존 현품표는 ACTIVE입니다. 프린터를 확인한 뒤 "
                    "같은 교환을 재시도하세요."
                ),
                error_code="LOCAL_PRINTER_ERROR",
                retryable=True,
            )

        if exchange_state != "READY":
            print_attempt_id = str(
                state.get("print_attempt_id") or ""
            ).strip()
            journal_status = str(
                state.get("status") or ""
            ).strip().upper()
            if journal_status == "PRINT_FAILED":
                print_attempt_id = ""
            if not print_attempt_id:
                if (
                    journal_status == "PRINT_REQUEST_PENDING"
                    and str(
                        state.get("print_idempotency_key") or ""
                    ).strip()
                ):
                    attempt_no = _positive_integer(
                        state.get("print_attempt_no"),
                        "print_attempt_no",
                    )
                    print_key = str(
                        state.get("print_idempotency_key") or ""
                    ).strip()
                else:
                    attempt_no = (
                        int(state.get("print_attempt_no") or 0) + 1
                    )
                    print_key = _stable_key(
                        "label-match-phs-label-single-print",
                        exchange_id,
                        target.get("label_id"),
                        attempt_no,
                    )
                    state = self._save(
                        state,
                        status="PRINT_REQUEST_PENDING",
                        print_attempt_no=attempt_no,
                        print_idempotency_key=print_key,
                        print_attempt_id="",
                    )
                notify("중앙 print-attempt를 요청합니다.")
                requested = self.client.request_phs_label_print(
                    exchange_id,
                    authority_scope_id=scope,
                    label_id=str(
                        target.get("label_id") or ""
                    ),
                    idempotency_key=print_key,
                )
                attempt = (
                    dict(requested.get("print_attempt"))
                    if isinstance(
                        requested.get("print_attempt"), Mapping
                    )
                    else {}
                )
                print_attempt_id = str(
                    attempt.get("print_attempt_id") or ""
                ).strip()
                if (
                    not print_attempt_id
                    or str(attempt.get("label_id") or "").strip()
                    != str(target.get("label_id") or "").strip()
                    or str(attempt.get("state") or "")
                    .strip()
                    .upper()
                    != "REQUESTED"
                ):
                    raise PHSLabelWorkflowError(
                        "PHS_PRINT_REQUEST_ACK_INVALID",
                        "중앙 REQUESTED print-attempt 증거가 일치하지 않습니다.",
                    )
                state = self._save(
                    state,
                    status="PRINT_REQUESTED",
                    print_attempt_id=print_attempt_id,
                    print_request_ack=dict(requested),
                )
                journal_status = "PRINT_REQUESTED"

            if (
                journal_status == "LOCAL_PRINT_STARTING"
                and not confirm_ambiguous_reprint
            ):
                raise PHSLabelWorkflowError(
                    "PHS_PRINT_REPRINT_CONFIRMATION_REQUIRED",
                    "이전 실행이 실제 프린터 제출 중 종료됐습니다. 실물 출력 "
                    "여부를 확인한 뒤 재출력을 명시적으로 승인하세요.",
                    retryable=True,
                )
            if journal_status not in {
                "LOCAL_PRINT_SUCCEEDED",
                "PRINT_COMPLETE_PENDING",
            }:
                notify(
                    "새 현품표를 생성하고 Windows 기본 프린터로 출력합니다."
                )
                try:
                    rendered = self.renderer.render(
                        current_set, target
                    )
                    if not _SHA256.fullmatch(rendered.sha256):
                        raise PHSPhysicalPrintError(
                            "출력 artifact SHA-256이 올바르지 않습니다."
                        )
                    state = self._save(
                        state,
                        status="LOCAL_PRINT_STARTING",
                        rendered_path=rendered.path,
                        rendered_artifact_hash=rendered.sha256,
                    )
                    evidence = self.printer.print_png(
                        rendered.path,
                        document_name=(
                            "PHS "
                            + str(target.get("worker_code") or "")
                        ),
                    )
                    state = self._save(
                        state,
                        status="LOCAL_PRINT_SUCCEEDED",
                        local_print_proof=self._print_proof(
                            evidence
                        ),
                    )
                except Exception as exc:
                    error_message = (
                        str(exc) or exc.__class__.__name__
                    )
                    state = self._save(
                        state,
                        status="PRINT_FAILURE_ACK_PENDING",
                        print_error_code="LOCAL_PRINTER_ERROR",
                        print_error_message=error_message[:1024],
                    )
                    try:
                        failed_ack = (
                            self.client.complete_phs_label_print(
                                str(
                                    state.get("print_attempt_id")
                                    or ""
                                ),
                                authority_scope_id=scope,
                                succeeded=False,
                                error_code="LOCAL_PRINTER_ERROR",
                                error_message=error_message[:1024],
                            )
                        )
                    except Exception as ack_error:
                        raise PHSPhysicalPrintError(
                            "실제 출력과 중앙 실패 ACK가 모두 복구 대기 중입니다."
                        ) from ack_error
                    failed = self._save(
                        state,
                        status="PRINT_FAILED",
                        print_failure_ack=dict(failed_ack),
                    )
                    return self._result(
                        failed,
                        success=False,
                        message=(
                            "실제 출력에 실패했습니다. 기존 현품표는 "
                            "ACTIVE로 유지됩니다."
                        ),
                        error_code="LOCAL_PRINTER_ERROR",
                        retryable=True,
                    )

            state = self._save(
                state, status="PRINT_COMPLETE_PENDING"
            )
            notify("실제 spool 증거를 중앙에 완료 기록합니다.")
            completed = self.client.complete_phs_label_print(
                str(state.get("print_attempt_id") or ""),
                authority_scope_id=scope,
                succeeded=True,
                rendered_artifact_hash=str(
                    state.get("rendered_artifact_hash") or ""
                ),
                proof=dict(
                    state.get("local_print_proof") or {}
                ),
            )
            completed_attempt = (
                dict(completed.get("print_attempt"))
                if isinstance(
                    completed.get("print_attempt"), Mapping
                )
                else {}
            )
            completed_exchange = (
                dict(completed.get("exchange"))
                if isinstance(completed.get("exchange"), Mapping)
                else {}
            )
            if (
                str(completed_attempt.get("state") or "")
                .strip()
                .upper()
                != "SUCCEEDED"
                or str(completed_exchange.get("state") or "")
                .strip()
                .upper()
                != "READY"
            ):
                raise PHSLabelWorkflowError(
                    "PHS_PRINT_COMPLETE_ACK_INVALID",
                    "중앙 SUCCEEDED print/READY exchange 증거가 없습니다.",
                )
            exchange = completed_exchange
            exchange_state = "READY"
            state = self._save(
                state,
                status="PRINT_COMPLETED",
                print_complete_ack=dict(completed),
                exchange_entity_version=_positive_integer(
                    exchange.get("entity_version"),
                    "exchange_entity_version",
                ),
            )

        if exchange_state != "READY":
            raise PHSLabelWorkflowError(
                "PHS_LABEL_EXCHANGE_NOT_READY",
                "모든 target의 실제 출력 성공 전에는 활성화할 수 없습니다.",
            )
        expected_version = _positive_integer(
            exchange.get("entity_version")
            or state.get("exchange_entity_version"),
            "exchange_entity_version",
        )
        state = self._save(
            state,
            status="ACTIVATE_PENDING",
            exchange_entity_version=expected_version,
        )
        notify("출력 성공을 확인하고 새 현품표를 ACTIVE로 전환합니다.")
        try:
            activated = self.client.activate_phs_label_exchange(
                exchange_id,
                authority_scope_id=scope,
                expected_exchange_version=expected_version,
            )
        except Exception:
            activated = self.client.get_phs_label_exchange(
                exchange_id,
                authority_scope_id=scope,
            )
        activated_exchange = (
            dict(activated.get("exchange"))
            if isinstance(activated.get("exchange"), Mapping)
            else {}
        )
        if (
            str(
                activated.get("status")
                or activated_exchange.get("state")
                or ""
            )
            .strip()
            .upper()
            != "COMMITTED"
            or str(
                activated_exchange.get("exchange_id") or ""
            ).strip()
            != exchange_id
        ):
            raise PHSLabelWorkflowError(
                "PHS_ACTIVATE_ACK_INVALID",
                "중앙 COMMITTED exchange 증거가 없습니다.",
                retryable=True,
            )
        try:
            self._apply_target(
                current_set,
                target,
                persist_current_set=persist_current_set,
            )
        except PHSLabelWorkflowError:
            self._save(
                state,
                status="COMMITTED_LOCAL_REFRESH_PENDING",
                committed_ack=dict(activated),
            )
            raise
        committed = self._save(
            state,
            status="COMMITTED",
            committed_ack=dict(activated),
        )
        return self._result(
            committed,
            success=True,
            message=(
                "현품표 날짜 교환 완료: "
                f"{target.get('business_date')} · "
                f"{target.get('worker_code')} · "
                "포장 raw/membership/progress는 유지됩니다."
            ),
        )

    def execute_reconciliation(
        self,
        action_resolution: Mapping[str, Any] | None,
        *,
        current_set: dict[str, Any] | None = None,
        persist_current_set: Callable[[], bool] | None = None,
        status_callback: Callable[[str], None] | None = None,
        retry_failed_target_ids: Iterable[str] = (),
        confirm_ambiguous_reprint_target_ids: Iterable[str] = (),
    ) -> PHSLabelExchangeResult:
        if not self.reconciliation_available():
            raise PHSLabelWorkflowError(
                "PHS_LABEL_API_UNAVAILABLE",
                "중앙 reconciliation 현품표 교체 API가 설정되지 않았습니다.",
            )
        notify = status_callback or (lambda _message: None)
        retry_ids = {
            str(value or "").strip()
            for value in retry_failed_target_ids
            if str(value or "").strip()
        }
        confirm_ids = {
            str(value or "").strip()
            for value in confirm_ambiguous_reprint_target_ids
            if str(value or "").strip()
        }
        state = self.journal.load()
        journal_status = str(
            state.get("status") or ""
        ).strip().upper()
        recoverable = bool(
            state
            and journal_status not in _TERMINAL_STATES
            and str(state.get("workflow_mode") or "").strip().upper()
            == "RECONCILIATION"
        )
        if (
            state
            and journal_status not in _TERMINAL_STATES
            and not recoverable
        ):
            raise PHSLabelWorkflowError(
                "PHS_LABEL_RECOVERY_CONFLICT",
                "기존 SINGLE 현품표 교체가 복구 대기 중입니다.",
            )

        if recoverable:
            saved_resolution = state.get("action_resolution")
            if not isinstance(saved_resolution, Mapping):
                raise PHSLabelWorkflowError(
                    "PHS_RECONCILIATION_JOURNAL_INVALID",
                    "복구 journal에 action snapshot이 없습니다.",
                )
            if action_resolution is not None:
                configured_scope = str(
                    getattr(
                        getattr(self.client, "config", None),
                        "authority_scope_id",
                        "",
                    )
                    or action_resolution.get("authority_scope_id")
                    or ""
                ).strip()
                normalized = self._reconciliation_resolution(
                    action_resolution,
                    authority_scope_id=configured_scope,
                    scan_payload=str(
                        action_resolution.get("scan_payload") or ""
                    ).strip(),
                )
                if (
                    dict(normalized.get("selection") or {})
                    != dict(saved_resolution.get("selection") or {})
                ):
                    raise PHSLabelWorkflowError(
                        "PHS_LABEL_RECOVERY_CONFLICT",
                        "스캔한 action selection이 복구 journal과 다릅니다.",
                    )
        else:
            if action_resolution is None:
                raise PHSLabelWorkflowError(
                    "PHS_RECONCILIATION_ACTION_REQUIRED",
                    "교체할 reconciliation action을 먼저 스캔해야 합니다.",
                )
            configured_scope = str(
                getattr(
                    getattr(self.client, "config", None),
                    "authority_scope_id",
                    "",
                )
                or action_resolution.get("authority_scope_id")
                or ""
            ).strip()
            normalized = self._reconciliation_resolution(
                action_resolution,
                authority_scope_id=configured_scope,
                scan_payload=str(
                    action_resolution.get("scan_payload") or ""
                ).strip(),
            )
            selection = dict(normalized["selection"])
            actions = list(normalized["actions"])
            action_states = {
                str(action.get("action_state") or "").strip().upper()
                for action in actions
            }
            linked_exchange_ids = {
                str(action.get("exchange_id") or "").strip()
                for action in actions
                if str(action.get("exchange_id") or "").strip()
            }
            if action_states == {"APPROVED"}:
                if len(linked_exchange_ids) != 1:
                    raise PHSLabelWorkflowError(
                        "PHS_RECONCILIATION_SELECTION_INVALID",
                        "APPROVED action들의 linked exchange가 다릅니다.",
                    )
                linked_exchange_id = next(iter(linked_exchange_ids))
            elif action_states == {"PROPOSED"}:
                if linked_exchange_ids:
                    raise PHSLabelWorkflowError(
                        "PHS_RECONCILIATION_SELECTION_INVALID",
                        "PROPOSED action에 linked exchange가 있습니다.",
                    )
                linked_exchange_id = ""
            else:
                raise PHSLabelWorkflowError(
                    "PHS_RECONCILIATION_SELECTION_INVALID",
                    "선택 action들의 중앙 상태가 서로 다릅니다.",
                )
            prepare_key = _stable_key(
                "label-match-phs-reconciliation-prepare",
                normalized["authority_scope_id"],
                selection["reconciliation_id"],
                *selection["action_ids"],
                selection["expected_reconciliation_version"],
            )
            render_context = {
                "parsed": [
                    str(actions[0].get("item_id") or "")
                ],
                "item_name_override": str(
                    (current_set or {}).get("item_name_override") or ""
                ),
                "package_source_snapshot": {},
            }
            state = self._save(
                {},
                workflow_mode="RECONCILIATION",
                status=(
                    "PREPARED"
                    if linked_exchange_id
                    else "PREPARE_PENDING"
                ),
                authority_scope_id=str(
                    normalized["authority_scope_id"]
                ),
                process_context="packaging",
                scan_payload=str(normalized["scan_payload"]),
                active_scan_label_id=str(
                    normalized["scan"]["active_label_id"]
                ),
                action_resolution=normalized,
                reconciliation_id=str(
                    selection["reconciliation_id"]
                ),
                action_ids=list(selection["action_ids"]),
                expected_reconciliation_version=int(
                    selection["expected_reconciliation_version"]
                ),
                prepare_idempotency_key=prepare_key,
                exchange_id=linked_exchange_id,
                target_prints={},
                render_context=render_context,
                set_id=str((current_set or {}).get("id") or ""),
            )

        scope = str(state.get("authority_scope_id") or "").strip()
        resolution = dict(state.get("action_resolution") or {})
        selection = dict(resolution.get("selection") or {})
        exchange_id = str(state.get("exchange_id") or "").strip()
        notify("중앙 reconciliation prepare/복구 상태를 확인합니다.")
        if not exchange_id:
            prepared = (
                self.client.prepare_phs_reconciliation_label_exchange(
                    str(selection.get("reconciliation_id") or ""),
                    authority_scope_id=scope,
                    action_ids=list(selection.get("action_ids") or []),
                    expected_reconciliation_version=_positive_integer(
                        selection.get(
                            "expected_reconciliation_version"
                        ),
                        "expected_reconciliation_version",
                    ),
                    idempotency_key=str(
                        state.get("prepare_idempotency_key") or ""
                    ).strip(),
                )
            )
            self._validate_reconciliation_prepare_link(
                prepared,
                state=state,
            )
            prepared_exchange = (
                dict(prepared.get("exchange"))
                if isinstance(prepared.get("exchange"), Mapping)
                else {}
            )
            exchange_id = str(
                prepared_exchange.get("exchange_id") or ""
            ).strip()
            if not exchange_id:
                raise PHSLabelWorkflowError(
                    "PHS_RECONCILIATION_PREPARE_ACK_INVALID",
                    "중앙 prepare ACK에 exchange id가 없습니다.",
                    retryable=True,
                )
            state = self._save(
                state,
                status="PREPARED",
                exchange_id=exchange_id,
                prepare_ack=dict(prepared),
            )
            central = prepared
        else:
            central = self.client.get_phs_label_exchange(
                exchange_id,
                authority_scope_id=scope,
            )

        (
            exchange,
            ordered_targets,
            targets_by_id,
            exchange_items,
        ) = self._reconciliation_exchange_projection(
            central,
            state=state,
        )
        state = self._save(
            state,
            target_labels=ordered_targets,
            exchange_items=exchange_items,
            exchange_entity_version=_positive_integer(
                exchange.get("entity_version"),
                "exchange_entity_version",
            ),
        )
        exchange_state = str(
            exchange.get("state") or ""
        ).strip().upper()
        if exchange_state == "COMMITTED":
            try:
                self._apply_reconciliation_local(
                    current_set,
                    state=state,
                    targets_by_id=targets_by_id,
                    items=exchange_items,
                    persist_current_set=persist_current_set,
                )
            except PHSLabelWorkflowError:
                self._save(
                    state,
                    status="COMMITTED_LOCAL_REFRESH_PENDING",
                    committed_ack=dict(central),
                )
                raise
            committed = self._save(
                state,
                status="COMMITTED",
                committed_ack=dict(central),
            )
            return self._result(
                committed,
                success=True,
                message="중앙 현품표 topology와 로컬 상태를 복구했습니다.",
            )

        if exchange_state != "READY":
            for target in ordered_targets:
                target_id = str(target.get("label_id") or "").strip()
                target_prints = {
                    str(key): dict(value)
                    for key, value in dict(
                        state.get("target_prints") or {}
                    ).items()
                    if isinstance(value, Mapping)
                }
                print_state = dict(target_prints.get(target_id) or {})
                target_status = str(
                    print_state.get("status") or ""
                ).strip().upper()

                if target_status == "PRINT_FAILURE_ACK_PENDING":
                    failed_ack = self.client.complete_phs_label_print(
                        str(
                            print_state.get("print_attempt_id") or ""
                        ),
                        authority_scope_id=scope,
                        succeeded=False,
                        error_code=str(
                            print_state.get("print_error_code")
                            or "LOCAL_PRINTER_ERROR"
                        ),
                        error_message=str(
                            print_state.get("print_error_message")
                            or "Local physical printer failed."
                        )[:1024],
                    )
                    failed_attempt = (
                        dict(failed_ack.get("print_attempt"))
                        if isinstance(
                            failed_ack.get("print_attempt"), Mapping
                        )
                        else {}
                    )
                    if (
                        str(
                            failed_attempt.get("print_attempt_id")
                            or ""
                        ).strip()
                        != str(
                            print_state.get("print_attempt_id") or ""
                        ).strip()
                        or str(
                            failed_attempt.get("label_id") or ""
                        ).strip()
                        != target_id
                        or str(
                            failed_attempt.get("state") or ""
                        ).strip().upper()
                        != "FAILED"
                    ):
                        raise PHSLabelWorkflowError(
                            "PHS_PRINT_COMPLETE_ACK_INVALID",
                            "중앙 FAILED print-attempt 증거가 다릅니다.",
                        )
                    print_state.update(
                        {
                            "status": "PRINT_FAILED",
                            "print_failure_ack": dict(failed_ack),
                        }
                    )
                    target_prints[target_id] = print_state
                    state = self._save(
                        state,
                        status="PRINT_FAILED",
                        failed_target_label_id=target_id,
                        target_prints=target_prints,
                    )
                    target_status = "PRINT_FAILED"

                if target_status == "SUCCEEDED":
                    continue
                if (
                    target_status == "PRINT_FAILED"
                    and target_id not in retry_ids
                ):
                    return self._result(
                        state,
                        success=False,
                        message=(
                            "실제 출력 실패 target만 확인 후 F5로 "
                            "명시 재시도하세요."
                        ),
                        error_code="PHS_TARGET_PRINT_RETRY_REQUIRED",
                        retryable=True,
                    )
                if target_status == "PRINT_FAILED":
                    print_state = {
                        "attempt_no": int(
                            print_state.get("attempt_no") or 0
                        )
                    }
                    target_status = ""

                print_attempt_id = str(
                    print_state.get("print_attempt_id") or ""
                ).strip()
                if not print_attempt_id:
                    if (
                        target_status == "PRINT_REQUEST_PENDING"
                        and str(
                            print_state.get("print_idempotency_key")
                            or ""
                        ).strip()
                    ):
                        attempt_no = _positive_integer(
                            print_state.get("attempt_no"),
                            "print_attempt_no",
                        )
                        print_key = str(
                            print_state["print_idempotency_key"]
                        )
                    else:
                        attempt_no = int(
                            print_state.get("attempt_no") or 0
                        ) + 1
                        print_key = _stable_key(
                            "label-match-phs-reconciliation-print",
                            exchange_id,
                            target_id,
                            attempt_no,
                        )
                        print_state.update(
                            {
                                "status": "PRINT_REQUEST_PENDING",
                                "attempt_no": attempt_no,
                                "print_idempotency_key": print_key,
                                "print_attempt_id": "",
                            }
                        )
                        target_prints[target_id] = print_state
                        state = self._save(
                            state,
                            status="PRINTING",
                            target_prints=target_prints,
                        )
                    notify("중앙 target print-attempt를 요청합니다.")
                    requested = self.client.request_phs_label_print(
                        exchange_id,
                        authority_scope_id=scope,
                        label_id=target_id,
                        idempotency_key=print_key,
                    )
                    attempt = (
                        dict(requested.get("print_attempt"))
                        if isinstance(
                            requested.get("print_attempt"), Mapping
                        )
                        else {}
                    )
                    print_attempt_id = str(
                        attempt.get("print_attempt_id") or ""
                    ).strip()
                    if (
                        not print_attempt_id
                        or str(attempt.get("label_id") or "").strip()
                        != target_id
                        or str(attempt.get("state") or "")
                        .strip()
                        .upper()
                        != "REQUESTED"
                    ):
                        raise PHSLabelWorkflowError(
                            "PHS_PRINT_REQUEST_ACK_INVALID",
                            "중앙 REQUESTED target print-attempt가 다릅니다.",
                        )
                    print_state.update(
                        {
                            "status": "PRINT_REQUESTED",
                            "print_attempt_id": print_attempt_id,
                            "print_request_ack": dict(requested),
                        }
                    )
                    target_prints[target_id] = print_state
                    state = self._save(
                        state,
                        status="PRINTING",
                        target_prints=target_prints,
                    )
                    target_status = "PRINT_REQUESTED"

                if (
                    target_status == "LOCAL_PRINT_STARTING"
                    and target_id not in confirm_ids
                ):
                    raise PHSLabelWorkflowError(
                        "PHS_PRINT_REPRINT_CONFIRMATION_REQUIRED",
                        "이전 실행이 실제 프린터 제출 중 종료됐습니다. "
                        "실물을 확인한 뒤 해당 target 재출력을 승인하세요.",
                        retryable=True,
                        details={"target_label_id": target_id},
                    )
                if target_status not in {
                    "LOCAL_PRINT_SUCCEEDED",
                    "PRINT_COMPLETE_PENDING",
                }:
                    notify("새 target 현품표를 실제 프린터로 출력합니다.")
                    try:
                        render_context = (
                            current_set
                            if current_set
                            else dict(state.get("render_context") or {})
                        )
                        rendered = self.renderer.render(
                            render_context,
                            target,
                        )
                        if not _SHA256.fullmatch(rendered.sha256):
                            raise PHSPhysicalPrintError(
                                "출력 artifact SHA-256이 올바르지 않습니다."
                            )
                        print_state.update(
                            {
                                "status": "LOCAL_PRINT_STARTING",
                                "rendered_path": rendered.path,
                                "rendered_artifact_hash": rendered.sha256,
                            }
                        )
                        target_prints[target_id] = print_state
                        state = self._save(
                            state,
                            status="PRINTING",
                            target_prints=target_prints,
                        )
                        evidence = self.printer.print_png(
                            rendered.path,
                            document_name=(
                                "PHS "
                                + str(target.get("worker_code") or "")
                            ),
                        )
                        print_state.update(
                            {
                                "status": "LOCAL_PRINT_SUCCEEDED",
                                "local_print_proof": self._print_proof(
                                    evidence
                                ),
                            }
                        )
                        target_prints[target_id] = print_state
                        state = self._save(
                            state,
                            status="PRINTING",
                            target_prints=target_prints,
                        )
                    except Exception as exc:
                        error_message = (
                            str(exc) or exc.__class__.__name__
                        )
                        print_state.update(
                            {
                                "status": "PRINT_FAILURE_ACK_PENDING",
                                "print_error_code": "LOCAL_PRINTER_ERROR",
                                "print_error_message": error_message[:1024],
                            }
                        )
                        target_prints[target_id] = print_state
                        state = self._save(
                            state,
                            status="PRINT_FAILURE_ACK_PENDING",
                            failed_target_label_id=target_id,
                            target_prints=target_prints,
                        )
                        try:
                            failed_ack = (
                                self.client.complete_phs_label_print(
                                    print_attempt_id,
                                    authority_scope_id=scope,
                                    succeeded=False,
                                    error_code="LOCAL_PRINTER_ERROR",
                                    error_message=error_message[:1024],
                                )
                            )
                        except Exception as ack_error:
                            raise PHSPhysicalPrintError(
                                "실제 출력과 중앙 실패 ACK가 모두 복구 대기 중입니다."
                            ) from ack_error
                        failed_attempt = (
                            dict(failed_ack.get("print_attempt"))
                            if isinstance(
                                failed_ack.get("print_attempt"), Mapping
                            )
                            else {}
                        )
                        if (
                            str(
                                failed_attempt.get("print_attempt_id")
                                or ""
                            ).strip()
                            != print_attempt_id
                            or str(
                                failed_attempt.get("label_id") or ""
                            ).strip()
                            != target_id
                            or str(
                                failed_attempt.get("state") or ""
                            ).strip().upper()
                            != "FAILED"
                        ):
                            raise PHSLabelWorkflowError(
                                "PHS_PRINT_COMPLETE_ACK_INVALID",
                                "중앙 FAILED target print-attempt가 다릅니다.",
                            )
                        print_state.update(
                            {
                                "status": "PRINT_FAILED",
                                "print_failure_ack": dict(failed_ack),
                            }
                        )
                        target_prints[target_id] = print_state
                        failed = self._save(
                            state,
                            status="PRINT_FAILED",
                            failed_target_label_id=target_id,
                            target_prints=target_prints,
                        )
                        return self._result(
                            failed,
                            success=False,
                            message=(
                                "해당 target 출력에 실패했습니다. "
                                "다른 성공 target 증거는 유지됩니다."
                            ),
                            error_code="LOCAL_PRINTER_ERROR",
                            retryable=True,
                        )

                print_state["status"] = "PRINT_COMPLETE_PENDING"
                target_prints[target_id] = print_state
                state = self._save(
                    state,
                    status="PRINTING",
                    target_prints=target_prints,
                )
                notify("target spool 증거를 중앙에 완료 기록합니다.")
                completed = self.client.complete_phs_label_print(
                    print_attempt_id,
                    authority_scope_id=scope,
                    succeeded=True,
                    rendered_artifact_hash=str(
                        print_state.get("rendered_artifact_hash") or ""
                    ),
                    proof=dict(
                        print_state.get("local_print_proof") or {}
                    ),
                )
                completed_attempt = (
                    dict(completed.get("print_attempt"))
                    if isinstance(
                        completed.get("print_attempt"), Mapping
                    )
                    else {}
                )
                completed_exchange = (
                    dict(completed.get("exchange"))
                    if isinstance(completed.get("exchange"), Mapping)
                    else {}
                )
                if (
                    str(
                        completed_attempt.get("print_attempt_id") or ""
                    ).strip()
                    != print_attempt_id
                    or str(
                        completed_attempt.get("label_id") or ""
                    ).strip()
                    != target_id
                    or str(completed_attempt.get("state") or "")
                    .strip()
                    .upper()
                    != "SUCCEEDED"
                    or str(
                        completed_exchange.get("exchange_id") or ""
                    ).strip()
                    != exchange_id
                    or str(completed_exchange.get("state") or "")
                    .strip()
                    .upper()
                    not in {"PRINT_PARTIAL", "READY"}
                ):
                    raise PHSLabelWorkflowError(
                        "PHS_PRINT_COMPLETE_ACK_INVALID",
                        "중앙 SUCCEEDED target print 증거가 다릅니다.",
                    )
                print_state.update(
                    {
                        "status": "SUCCEEDED",
                        "print_complete_ack": dict(completed),
                    }
                )
                target_prints[target_id] = print_state
                state = self._save(
                    state,
                    status="PRINTING",
                    target_prints=target_prints,
                    exchange_entity_version=_positive_integer(
                        completed_exchange.get("entity_version"),
                        "exchange_entity_version",
                    ),
                )

            central = self.client.get_phs_label_exchange(
                exchange_id,
                authority_scope_id=scope,
            )
            (
                exchange,
                ordered_targets,
                targets_by_id,
                exchange_items,
            ) = self._reconciliation_exchange_projection(
                central,
                state=state,
            )
            exchange_state = str(
                exchange.get("state") or ""
            ).strip().upper()

        if exchange_state != "READY":
            raise PHSLabelWorkflowError(
                "PHS_LABEL_EXCHANGE_NOT_READY",
                "모든 target의 실제 출력 성공 전에는 활성화할 수 없습니다.",
                retryable=True,
            )
        expected_version = _positive_integer(
            exchange.get("entity_version"),
            "exchange_entity_version",
        )
        state = self._save(
            state,
            status="ACTIVATE_PENDING",
            exchange_entity_version=expected_version,
        )
        notify("모든 출력 성공 후 topology를 한 번만 ACTIVE로 전환합니다.")
        try:
            activated = self.client.activate_phs_label_exchange(
                exchange_id,
                authority_scope_id=scope,
                expected_exchange_version=expected_version,
            )
        except Exception:
            activated = self.client.get_phs_label_exchange(
                exchange_id,
                authority_scope_id=scope,
            )
        (
            activated_exchange,
            _activated_targets,
            activated_targets_by_id,
            activated_items,
        ) = self._reconciliation_exchange_projection(
            activated,
            state=state,
        )
        if (
            str(
                activated.get("status")
                or activated_exchange.get("state")
                or ""
            )
            .strip()
            .upper()
            != "COMMITTED"
        ):
            raise PHSLabelWorkflowError(
                "PHS_ACTIVATE_ACK_INVALID",
                "중앙 COMMITTED exchange 증거가 없습니다.",
                retryable=True,
            )
        try:
            self._apply_reconciliation_local(
                current_set,
                state=state,
                targets_by_id=activated_targets_by_id,
                items=activated_items,
                persist_current_set=persist_current_set,
            )
        except PHSLabelWorkflowError:
            self._save(
                state,
                status="COMMITTED_LOCAL_REFRESH_PENDING",
                committed_ack=dict(activated),
            )
            raise
        committed = self._save(
            state,
            status="COMMITTED",
            committed_ack=dict(activated),
        )
        exchange_kind = str(
            activated_exchange.get("exchange_kind") or ""
        ).strip().upper()
        return self._result(
            committed,
            success=True,
            message=(
                f"{exchange_kind} 현품표 교체 완료: "
                f"{len(activated_targets_by_id)}장 출력·ACTIVE, "
                "포장 원장/progress 유지"
            ),
        )

    def recover_reconciliation(
        self,
        *,
        current_set: dict[str, Any] | None = None,
        persist_current_set: Callable[[], bool] | None = None,
        status_callback: Callable[[str], None] | None = None,
        retry_failed_target_ids: Iterable[str] = (),
        confirm_ambiguous_reprint_target_ids: Iterable[str] = (),
    ) -> PHSLabelExchangeResult | None:
        if not self.has_pending_reconciliation():
            return None
        return self.execute_reconciliation(
            None,
            current_set=current_set,
            persist_current_set=persist_current_set,
            status_callback=status_callback,
            retry_failed_target_ids=retry_failed_target_ids,
            confirm_ambiguous_reprint_target_ids=(
                confirm_ambiguous_reprint_target_ids
            ),
        )

    def recover_current(
        self,
        current_set: dict[str, Any],
        *,
        persist_current_set: Callable[[], bool],
        status_callback: Callable[[str], None] | None = None,
    ) -> PHSLabelExchangeResult | None:
        state = self.journal.load()
        status = str(state.get("status") or "").strip().upper()
        if (
            not state
            or status in _TERMINAL_STATES
            or str(state.get("set_id") or "").strip()
            != str(current_set.get("id") or "").strip()
            or str(
                state.get("canonical_input_tag_qr") or ""
            ).strip()
            != str(
                current_set.get("canonical_input_tag_qr")
                or ((current_set.get("raw") or [""])[0])
            ).strip()
        ):
            return None
        target = (
            dict(state.get("target_instruction"))
            if isinstance(
                state.get("target_instruction"), Mapping
            )
            else None
        )
        return self.execute_single(
            current_set,
            target,
            persist_current_set=persist_current_set,
            status_callback=status_callback,
        )


__all__ = [
    "PackagingPHSLabelEvidence",
    "PHSLabelExchangeCoordinator",
    "PHSLabelExchangeJournal",
    "PHSLabelExchangeResult",
    "PHSLabelRenderer",
    "PHSLabelWorkflowError",
    "PHSPhysicalPrintError",
    "PhysicalPrintEvidence",
    "RenderedPHSLabel",
    "WindowsGDIPhysicalLabelPrinter",
    "normalize_packaging_phs_label_evidence",
    "parse_compact_phs2",
]

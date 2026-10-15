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

import hashlib
import ctypes
from ctypes import wintypes
import json
import os
import re
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from kmtech_zero_pe import (
    FontSpec,
    GdiPrinter,
    MarginsMM,
    PaperSpec,
    PrintSpec,
    RasterCanvas,
    RasterImage,
)
from writer_session_fence import WriterFenceError, writer_sink, writer_admission, _probe_then_call
import carrier_identity_port as carrier_identity
from label_data_manager import read_recovery_file
from label_recovery_schema import valid_recovery_business_date
from label_safe_path import read_checked_bytes, write_checked_bytes


PHS_LABEL_EXCHANGE_JOURNAL_VERSION = "label-match-phs-label-exchange-v1"
_TERMINAL_STATES = frozenset({"COMMITTED", "CANCELLED"})
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


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
    return carrier_identity.parse_raw_compact_carrier(
        raw_value, error_type=PHSLabelWorkflowError,
    )


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
            '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: ACTIVE · immutable · input-tag',
        )
    return active, active_fields


def _validate_package_source_input_tags(
    data: Mapping[str, Any],
    bundle: Mapping[str, Any],
    input_tag: Mapping[str, Any],
    *,
    canonical_fields: Mapping[str, str],
    source_session_id: Any,
) -> None:
    """Validate the exact input-tag origins of one work-group transfer."""

    source_values = data.get("source_input_tags")
    group_value = data.get("phs_work_group")
    has_work_group_evidence = (
        source_values is not None or group_value is not None
    )
    multi_source = source_session_id is None
    if not has_work_group_evidence and not multi_source:
        # Backward compatibility for the original single-source projection.
        return
    if (
        data.get("source_resolution_basis")
        != "PHS_WORK_GROUP_EXACT_MEMBERSHIP"
        or not isinstance(source_values, list)
        or not isinstance(group_value, Mapping)
    ):
        raise PHSLabelWorkflowError(
            "PHS2_PACKAGE_SOURCE_INVALID",
            '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: package · source · exact',
        )

    group = dict(group_value)
    item_id = str(bundle.get("item_id") or "").strip()
    uom = str(bundle.get("uom") or "").strip().upper()
    anchor_input_tag_id = str(
        group.get("scan_anchor_input_tag_id") or ""
    ).strip()
    if (
        not item_id
        or not uom
        or anchor_input_tag_id != canonical_fields["ITG"]
        or str(group.get("item_id") or "").strip() != item_id
        or str(group.get("uom") or "").strip().upper() != uom
        or (multi_source and len(source_values) < 2)
        or (not multi_source and len(source_values) != 1)
    ):
        raise PHSLabelWorkflowError(
            "PHS2_PACKAGE_SOURCE_INVALID",
            '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: package · source · work-group',
        )

    source_identities: dict[str, tuple[str, ...]] = {}
    for value in source_values:
        if not isinstance(value, Mapping):
            raise PHSLabelWorkflowError(
                "PHS2_PACKAGE_SOURCE_INVALID",
                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: package · source',
            )
        source = dict(value)
        qr_payload = str(source.get("qr_payload") or "").strip()
        try:
            fields = parse_compact_phs2(qr_payload)
        except PHSLabelWorkflowError as exc:
            raise PHSLabelWorkflowError(
                "PHS2_PACKAGE_SOURCE_INVALID",
                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: package · source · identity',
            ) from exc
        input_tag_id = str(source.get("input_tag_id") or "").strip()
        hash_prefix = str(source.get("hash_prefix") or "").strip().lower()
        label_hash = str(
            source.get("label_instance_hash") or ""
        ).strip().lower()
        identity = (
            input_tag_id,
            str(source.get("label_id") or "").strip(),
            str(source.get("item_id") or "").strip(),
            hash_prefix,
            qr_payload,
        )
        if (
            str(source.get("lifecycle") or "").strip().upper()
            != "INSPECTION_COMPLETED"
            or identity[0] != fields["ITG"]
            or identity[1] != fields["LBL"]
            or identity[2] != fields["CLC"]
            or identity[2] != item_id
            or identity[3] != fields["HSH"]
            or str(source.get("uom") or "").strip().upper() != uom
            or str(source.get("source_marker") or "").strip().upper()
            != "KMTECH_INPUT_TAG"
            or not _SHA256.fullmatch(label_hash)
            or not label_hash.startswith(hash_prefix)
            or input_tag_id in source_identities
        ):
            raise PHSLabelWorkflowError(
                "PHS2_PACKAGE_SOURCE_INVALID",
                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: package · source · lifecycle · identity',
            )
        source_identities[input_tag_id] = identity

    anchor_identity = source_identities.get(anchor_input_tag_id)
    input_identity = (
        str(input_tag.get("input_tag_id") or "").strip(),
        str(input_tag.get("label_id") or "").strip(),
        str(input_tag.get("item_id") or "").strip(),
        str(input_tag.get("hash_prefix") or "").strip().lower(),
        str(input_tag.get("qr_payload") or "").strip(),
    )
    if anchor_identity is None or anchor_identity != input_identity:
        raise PHSLabelWorkflowError(
            "PHS2_PACKAGE_SOURCE_INVALID",
            '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: anchor · exact',
        )
    if not multi_source and tuple(source_identities) != (
        str(source_session_id),
    ):
        raise PHSLabelWorkflowError(
            "PHS2_PACKAGE_SOURCE_INVALID",
            '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: package · source · identity',
        )


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
            '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: package · source/input-tag',
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
            '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: immutable · input-tag',
        )

    try:
        member_count = int(bundle.get("member_count"))
    except (TypeError, ValueError) as exc:
        raise PHSLabelWorkflowError(
            "PHS2_PACKAGE_SOURCE_INVALID",
            '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: package · source',
        ) from exc
    membership_digest = str(
        bundle.get("membership_hash") or ""
    ).strip().lower()
    authority_scope_id = str(
        bundle.get("authority_scope_id")
        or data.get("authority_scope_id")
        or ""
    ).strip()
    source_session_id = bundle.get("source_session_id")
    if (
        int(bundle.get("candidate_count") or data.get("candidate_count") or 0)
        != 1
        or str(bundle.get("bundle_role") or "").strip()
        != "PACKAGE_SOURCE"
        or str(bundle.get("bundle_state") or "").strip().upper()
        != "AVAILABLE"
        or str(bundle.get("item_id") or "").strip() != item_id
        or (
            source_session_id is not None
            and (
                not isinstance(source_session_id, str)
                or source_session_id.strip() != input_tag_id
            )
        )
        or member_count < 1
        or not _SHA256.fullmatch(membership_digest)
        or not authority_scope_id
    ):
        raise PHSLabelWorkflowError(
            "PHS2_PACKAGE_SOURCE_INVALID",
            '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: package · source',
        )

    _validate_package_source_input_tags(
        data,
        bundle,
        input_tag,
        canonical_fields=canonical_fields,
        source_session_id=source_session_id,
    )

    resolution_value = data.get("phs_label_resolution")
    if resolution_value is None:
        if (
            scanned_fields["LBL"] != canonical_fields["LBL"]
            or scanned_fields["HSH"] != canonical_fields["HSH"]
        ):
            raise PHSLabelWorkflowError(
                "PHS2_LABEL_OVERLAY_REQUIRED",
                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: active-label · overlay',
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
                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: active-label · resolution',
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
                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: resolution · scanned-label',
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
                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: scanned-label',
            )
        if resolution_kind == "OVERLAY_NOT_ACTIVE" or status in {
            "PENDING_ACTIVATION",
            "PRINT_FAILED",
        }:
            raise PHSLabelWorkflowError(
                "PHS2_LABEL_NOT_ACTIVE",
                '아직 사용할 수 없는 현품표입니다\n현장 관리자에게 알리세요\n코드: ACTIVE',
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
                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: ACTIVE · successor',
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
                '현재 사용할 수 없는 현품표입니다\n현장 관리자에게 알리세요\n코드: ACTIVE',
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
            '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: ACTIVE',
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
        self._observed = threading.local()

    def load(self) -> dict[str, Any]:
        with self._lock:
            evidence = read_recovery_file(
                self.path, journal_schema=PHS_LABEL_EXCHANGE_JOURNAL_VERSION
            )
            self._observed.sha256 = evidence["sha256"]
            if evidence["reason"] == "missing":
                return {}
        if not evidence["verified"]:
            raise PHSLabelWorkflowError(
                "PHS_LABEL_JOURNAL_CORRUPT",
                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: journal',
            )
        return dict(evidence["value"])

    def save(self, state: Mapping[str, Any]) -> dict[str, Any]:
        bounded = dict(state or {})
        bounded["updated_at"] = _utc_now()
        payload = {
            "schema_version": PHS_LABEL_EXCHANGE_JOURNAL_VERSION,
            "state": bounded,
        }
        from label_recovery_schema import require_recovery_record
        require_recovery_record("journal", payload,
                                journal_version=PHS_LABEL_EXCHANGE_JOURNAL_VERSION)
        with self._lock:
            current = read_recovery_file(
                self.path, journal_schema=PHS_LABEL_EXCHANGE_JOURNAL_VERSION
            )
            if not current["verified"] and current["reason"] != "missing":
                raise PHSLabelWorkflowError(
                    "PHS_LABEL_JOURNAL_CORRUPT", '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: journal'
                )
            if (hasattr(self._observed, "sha256")
                    and self._observed.sha256 != current["sha256"]):
                raise PHSLabelWorkflowError(
                    "PHS_LABEL_JOURNAL_CHANGED",
                    '현품표 교환 기록이 바뀌었습니다.\n현재 결과를 다시 확인하세요.',
                )
            encoded = (json.dumps(payload, ensure_ascii=False, indent=2,
                                  sort_keys=True) + "\n").encode("utf-8")
            write_checked_bytes(self.path, encoded, allowed_root=self.path.parent)
            self._observed.sha256 = hashlib.sha256(encoded).hexdigest()
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


_NON_LABEL_PRINTER_MARKERS = ("portprompt:", "file:", "xpsport", "nul:", "pdf", "xps", "onenote", "fax")
PRINT_SUBMIT_TIMEOUT_SECONDS = 60.0

class _PrinterInfo2W(ctypes.Structure):
    _fields_ = [(name, ctypes.c_void_p) for name in (
        "pServerName", "pPrinterName", "pShareName", "pPortName", "pDriverName", "pComment",
        "pLocation", "pDevMode", "pSepFile", "pPrintProcessor", "pDatatype", "pParameters", "pSecurityDescriptor"
    )] + [(name, wintypes.DWORD) for name in (
        "Attributes", "Priority", "DefaultPriority", "StartTime", "UntilTime", "Status", "cJobs", "AveragePPM")]

class WindowsGDIPhysicalLabelPrinter:
    """Submit one label with the shared, job-local DEVMODE contract."""

    _PAPER = PaperSpec(
        orientation="landscape",
        paper_size_code=9,
        driver_scale_percent=100,
    )
    _MARGINS = MarginsMM(left=12.0, top=12.0, right=12.0, bottom=12.0)

    def __init__(self, output_root: str | os.PathLike[str]):
        self.output_root = Path(output_root)

    _submissions, _submission_lock = {}, threading.Lock()

    @staticmethod
    def _printer_profile(printer_name):
        spool = ctypes.WinDLL("winspool.drv", use_last_error=True)
        for name, arguments in (
            ("OpenPrinterW", (wintypes.LPWSTR, ctypes.POINTER(wintypes.HANDLE), ctypes.c_void_p)),
            ("GetPrinterW", (wintypes.HANDLE, wintypes.DWORD, ctypes.c_void_p, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD))),
            ("ClosePrinter", (wintypes.HANDLE,))):
            api = getattr(spool, name)
            api.argtypes, api.restype = arguments, wintypes.BOOL
        handle, size = wintypes.HANDLE(), wintypes.DWORD()
        if not spool.OpenPrinterW(printer_name, ctypes.byref(handle), None):
            raise PHSPhysicalPrintError("기본 프린터 정보를 확인하지 못했습니다.")
        try:
            spool.GetPrinterW(handle, 2, None, 0, ctypes.byref(size))
            if not ctypes.sizeof(_PrinterInfo2W) <= size.value <= 1024 * 1024:
                raise PHSPhysicalPrintError("기본 프린터 정보를 확인하지 못했습니다.")
            buffer = ctypes.create_string_buffer(size.value)
            if not spool.GetPrinterW(handle, 2, buffer, size.value, ctypes.byref(size)):
                raise PHSPhysicalPrintError("기본 프린터 정보를 확인하지 못했습니다.")
            info = ctypes.cast(buffer, ctypes.POINTER(_PrinterInfo2W)).contents
            return tuple(ctypes.wstring_at(value) if value else "" for value in (info.pPortName, info.pDriverName))
        finally:
            spool.ClosePrinter(handle)

    def print_png(self, filepath, *, document_name, late_record=print):
        key = os.path.normcase(os.path.abspath(filepath))
        ready, decided, result, unknown = threading.Event(), threading.Event(), [], [False]
        def submit():
            try: result.append((self._submit_print_png(filepath, document_name=document_name), None))
            except Exception as exc: result.append((None, exc))
            ready.set()
            decided.wait()
            try:
                if unknown[0]:
                    value, error = result[0]
                    record = {"result": "ERROR" if error else "SUCCESS", "error_type": type(error).__name__ if error else "", "at": _utc_now()}
                    late_record(record)
            except Exception as exc: print(f"[PHS print] late record failed: {type(exc).__name__}")
            finally:
                with self._submission_lock:
                    if self._submissions.get(key) is worker: self._submissions.pop(key, None)
        worker = threading.Thread(target=submit, name="LabelPrintSubmission", daemon=True)
        with self._submission_lock:
            previous = self._submissions.get(key)
            if previous is not None and previous.is_alive():
                error = PHSPhysicalPrintError('이전 인쇄가 아직 프린터 대기열에 있습니다\n프린터 대기열을 확인한 뒤 다시 누르세요')
                error.operator_message = str(error)
                raise error
            self._submissions[key] = worker
            worker.start()
        try:
            if not ready.wait(PRINT_SUBMIT_TIMEOUT_SECONDS):
                unknown[0] = True
                error = PHSPhysicalPrintError('인쇄 제출이 끝나지 않았습니다\n출력물을 확인한 뒤 재인쇄하세요\n자세히: 인쇄 대기로 남겼습니다')
                error.code = "LOCAL_PRINT_UNKNOWN"
                raise error
            value, error = result[0]
            if error: raise error
            return value
        finally:
            decided.set()

    def _submit_print_png(
        self,
        filepath: str,
        *,
        document_name: str,
    ) -> PhysicalPrintEvidence:
        path = Path(str(filepath or ""))
        try:
            image = RasterImage.from_png_bytes(read_checked_bytes(
                path, allowed_root=self.output_root
            ))
            printer = GdiPrinter()
            try:
                profile = " ".join((printer.printer_name, *self._printer_profile(printer.printer_name))).casefold()
                if any(marker in profile for marker in _NON_LABEL_PRINTER_MARKERS):
                    raise ValueError("non-label printer")
                printer.validate_paper(self._PAPER)
            except Exception as exc:
                error = PHSPhysicalPrintError('기본 프린터가 라벨 프린터가 아닙니다.\n현장 관리자에게 알리세요.')
                error.operator_message = f"""기본 프린터가 라벨 프린터가 아닙니다({printer.printer_name}).\n현장 관리자에게 알리세요."""
                raise error from exc
            receipt = printer.print_image(
                image,
                PrintSpec(
                    document_name=str(document_name or path.stem)[:240],
                    paper=self._PAPER,
                    margins_mm=self._MARGINS,
                    content_scale_percent=100,
                ),
            )
        except PHSPhysicalPrintError:
            raise
        except Exception as exc:
            raise PHSPhysicalPrintError(
                f"""현품표를 인쇄하지 못했습니다\n프린터를 확인하고 다시 시도하세요\n코드: GDI · {exc}"""
            ) from exc
        if not receipt.default_printer_unchanged or not receipt.default_devmode_unchanged:
            raise PHSPhysicalPrintError(
                "현품표 출력 뒤 기본 프린터 설정이 달라졌습니다."
            )
        return PhysicalPrintEvidence(
            printer_name=receipt.printer_name,
            spool_job_id=receipt.job_id,
            document_name=receipt.document_name,
            submitted_at=_utc_now(),
        )

def _submit_physical_print(printer, path, document_name, journal, attempt):
    options = {"document_name": document_name}
    if isinstance(printer, WindowsGDIPhysicalLabelPrinter):
        attempt = json.loads(json.dumps(attempt))
        record_path = journal.path.with_name(journal.path.name + ".late-print-" + uuid.uuid4().hex + ".json")
        def record(result):
            with writer_admission("general_sink"):
                PHSLabelExchangeJournal(record_path).save({**attempt, "status": "PRINT_OUTCOME_UNKNOWN", "print_error_message": json.dumps(result)})
        options["late_record"] = record
    return _probe_then_call("general_sink", printer.print_png, (path,), options)


@dataclass(frozen=True)
class RenderedPHSLabel:
    path: str
    sha256: str


class PHSLabelRenderer:
    def __init__(self, output_root: str | os.PathLike[str]):
        self.output_root = Path(output_root)

    @staticmethod
    def _font(size: int, *, bold: bool = False) -> FontSpec:
        return FontSpec(size, family="Malgun Gothic", bold=bold)

    def render(
        self,
        current_set: Mapping[str, Any],
        target: Mapping[str, Any],
    ) -> RenderedPHSLabel:
        label_id = str(target.get("label_id") or "").strip()
        qr_payload = str(target.get("qr_payload") or "").strip()
        business_date = str(
            target.get("business_date") or ""
        ).strip()
        worker_code = str(target.get("worker_code") or "").strip()
        if not all((label_id, qr_payload, business_date, worker_code)):
            raise PHSLabelWorkflowError(
                "PHS_TARGET_LABEL_INVALID",
                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: target · label · QR/date/worker-code',
            )
        if not valid_recovery_business_date(business_date):
            raise PHSLabelWorkflowError(
                "PHS_TARGET_LABEL_INVALID", '입력한 값을 확인하세요\n코드: target · label'
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
        output_path = folder / f"{safe_label}.png"

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
        try:
            with RasterCanvas(1100, 600, background=(255, 255, 255)) as canvas:
                canvas.rectangle((8, 8, 1092, 592), outline=(0, 0, 0), width=5)
                canvas.qr(qr_payload, (40, 80, 480, 520), error_correction="M", border=4)
                canvas.text((530, 55), '현품표\n코드: PHS', font=self._font(48, bold=True))
                canvas.text(
                    (530, 145),
                    f"작업일  {business_date}",
                    font=self._font(38, bold=True),
                )
                canvas.text(
                    (530, 215),
                    f"작업코드  {worker_code}",
                    font=self._font(34, bold=True),
                )
                canvas.text((530, 290), f"품목  {item_code}", font=self._font(29))
                if item_name:
                    canvas.text((530, 345), item_name[:28], font=self._font(27))
                canvas.text(
                    (530, 410),
                    f"""수량 {quantity}개""",
                    font=self._font(31, bold=True),
                )
                canvas.text(
                    (530, 490),
                    f"Label  {label_id[:38]}",
                    font=self._font(18),
                )
                image = canvas.snapshot()
            write_checked_bytes(output_path, image.to_png_bytes(dpi=(300, 300)),
                                allowed_root=self.output_root)
        except Exception as exc:
            raise PHSPhysicalPrintError(
                f"""현품표를 생성하지 못했습니다\n잠시 후 다시 시도하세요\n코드: GDI · {exc}"""
            ) from exc
        digest = hashlib.sha256(read_checked_bytes(
            output_path, allowed_root=self.output_root
        )).hexdigest()
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
    operator_message: str = ""


class PHSLabelExchangeCoordinator:
    """Resume saved server-owned label exchanges; never start a new exchange."""

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
        self.printer = printer or WindowsGDIPhysicalLabelPrinter(renderer.output_root)

    def available(self) -> bool:
        required = (
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
                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: SINGLE · exchange/source/target · cardinality',
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
                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: source/target · date · code · anchor · membership',
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
                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: action · topology',
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
                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: journal · action · snapshot',
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
                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: exchange · kind/state/source/target/cardinality',
            )

        source_labels: dict[str, dict[str, Any]] = {}
        for source_value in source_values:
            if not isinstance(source_value, Mapping):
                raise PHSLabelWorkflowError(
                    "PHS_RECONCILIATION_EXCHANGE_INVALID",
                    '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: source · label',
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
                    '다른 곳에서 먼저 바뀌었습니다\n새로 불러온 뒤 다시 하세요\n코드: source · label · identity/version/membership · action',
                )
            source_labels[label_id] = source

        target_labels: dict[str, dict[str, Any]] = {}
        target_labels_by_id: dict[str, dict[str, Any]] = {}
        for target_value in target_values:
            if not isinstance(target_value, Mapping):
                raise PHSLabelWorkflowError(
                    "PHS_RECONCILIATION_EXCHANGE_INVALID",
                    '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: target · label',
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
                    '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: target · label · identity/date/code/membership · action',
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
                    '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: exchange · item',
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
                    '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: exchange · item · edge/membership · action',
                )
            seen_item_keys.add(key)
        if seen_item_keys != set(expected_item_keys):
            raise PHSLabelWorkflowError(
                "PHS_RECONCILIATION_EXCHANGE_INVALID",
                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: exchange · item · edge · cardinality',
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
                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: prepare · ACK · approved · action/reconciliation',
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
                '현품표 인쇄 완료를 확인하지 못했습니다\n프린터를 확인하세요\n코드: GDI · spool'
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
                '다른 곳에서 먼저 바뀌었습니다\n새로 불러온 뒤 다시 하세요\n코드: PHS2',
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
                f"""현재 현품표 {target.get('worker_code')} ({target.get('business_date')})\n코드: ACTIVE"""
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
                '현품표 교환 중 포장 제품 목록이 바뀌었습니다\n현장 관리자에게 알리세요\n코드: raw/membership',
            )
        if not persist_current_set():
            for key, (existed, value) in previous.items():
                if existed:
                    current_set[key] = value
                else:
                    current_set.pop(key, None)
            raise PHSLabelWorkflowError(
                "PHS_LOCAL_STATE_WRITE_FAILED",
                '현품표 교환 뒤 이 PC에 저장하지 못했습니다\n현장 관리자에게 알리세요\n코드: COMMITTED · ACTIVE',
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
                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: set · reconciliation · source',
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
                    '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: source · successor\n자세히: 1:1',
                )
            target_id = str(
                matching_items[0].get("target_label_id") or ""
            ).strip()
            target = targets_by_id.get(target_id)
            if target is None:
                raise PHSLabelWorkflowError(
                    "PHS_LOCAL_TARGET_PRECONDITION",
                    '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: source · target · label',
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
                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: SPLIT/MERGE · successor · label',
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
                f"""현품표 교환을 완료했습니다\n새 현품표별 작업을 확인하세요\n자세히: 현품표 {len(successors)}장 생성\n현재 포장은 완료 차단 상태로 보존합니다\n코드: {exchange_kind} · successor"""
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
                '현품표 교환 중 포장 기록이 바뀌었습니다\n현장 관리자에게 알리세요\n코드: topology',
            )
        if not persist_current_set():
            for key, (existed, value) in previous.items():
                if existed:
                    current_set[key] = value
                else:
                    current_set.pop(key, None)
            raise PHSLabelWorkflowError(
                "PHS_LOCAL_STATE_WRITE_FAILED",
                '현품표 교환 뒤 이 PC에 저장하지 못했습니다\n현장 관리자에게 알리세요\n코드: COMMITTED · topology · refresh',
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
        operator_message: str = "",
    ) -> PHSLabelExchangeResult:
        return PHSLabelExchangeResult(
            status=str(state.get("status") or ""),
            success=success,
            message=message,
            error_code=error_code,
            retryable=retryable,
            exchange_id=str(state.get("exchange_id") or ""),
            journal_state=dict(state),
            operator_message=operator_message,
        )

    @writer_sink("phs_current_recovery")
    def _resume_current(
        self,
        current_set: dict[str, Any],
        *,
        persist_current_set: Callable[[], bool],
        status_callback: Callable[[str], None] | None = None,
        confirm_ambiguous_reprint: bool = False,
    ) -> PHSLabelExchangeResult | None:
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
        if not recoverable:
            return None

        if not self.available():
            raise PHSLabelWorkflowError(
                "PHS_LABEL_API_UNAVAILABLE",
                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: API',
            )

        scope = str(
            state.get("authority_scope_id") or ""
        ).strip()
        exchange_id = str(state.get("exchange_id") or "").strip()
        notify('현품표 교환 복구 상태를 확인합니다\n코드: prepare')
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
                    '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: prepare · ACK · exchange · id',
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
                    '기존 현품표를 계속 사용할 수 있습니다\n프린터를 확인한 뒤 같은 교환을 다시 시도하세요\n코드: ACTIVE'
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
                notify('현품표 인쇄를 요청합니다\n코드: print-attempt')
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
                        '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: REQUESTED · print-attempt',
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
                    '이전 실행이 실제 프린터 제출 중 종료됐습니다.\n실물 출력 여부를 확인한 뒤 재출력을 명시적으로 승인하세요.',
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
                            '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: artifact · SHA-256'
                        )
                    state = self._save(
                        state,
                        status="LOCAL_PRINT_STARTING",
                        rendered_path=rendered.path,
                        rendered_artifact_hash=rendered.sha256,
                    )
                    evidence = _submit_physical_print(self.printer,
                        rendered.path,
                        document_name=(
                            "PHS "
                            + str(target.get("worker_code") or "")
                        ),
                        journal=self.journal, attempt=state,
                    )
                    state = self._save(
                        state,
                        status="LOCAL_PRINT_SUCCEEDED",
                        local_print_proof=self._print_proof(
                            evidence
                        ),
                    )
                except WriterFenceError:
                    raise
                except Exception as exc:
                    if getattr(exc, "code", "") == "LOCAL_PRINT_UNKNOWN":
                        return self._result(state, success=False, message=str(exc), error_code=exc.code, retryable=True)
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
                    except WriterFenceError:
                        raise
                    except Exception as ack_error:
                        raise PHSPhysicalPrintError(
                            '현품표 인쇄와 실패 확인이 복구 대기 중입니다\n현장 관리자에게 알리세요\n코드: ACK'
                        ) from ack_error
                    failed = self._save(
                        state,
                        status="PRINT_FAILED",
                        print_failure_ack=dict(failed_ack),
                    )
                    return self._result(
                        failed,
                        success=False,
                        operator_message=getattr(exc, "operator_message", ""),
                        message=(
                            '현품표를 인쇄하지 못했습니다\n기존 현품표를 계속 사용하세요\n코드: ACTIVE'
                        ),
                        error_code="LOCAL_PRINTER_ERROR",
                        retryable=True,
                    )

            state = self._save(
                state, status="PRINT_COMPLETE_PENDING"
            )
            notify('현품표 인쇄 완료를 기록합니다\n코드: spool')
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
                    '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: SUCCEEDED · print/READY · exchange',
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
                '모든 현품표를 인쇄해야 새 현품표를 사용할 수 있습니다\n코드: target',
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
        notify('인쇄를 확인하고 새 현품표를 사용할 수 있도록 바꿉니다\n코드: ACTIVE')
        try:
            activated = self.client.activate_phs_label_exchange(
                exchange_id,
                authority_scope_id=scope,
                expected_exchange_version=expected_version,
            )
        except WriterFenceError:
            raise
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
                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: COMMITTED · exchange',
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
                f"""현품표 날짜 교환 완료: {target.get('business_date')} · {target.get('worker_code')}\n포장 제품과 진행은 그대로입니다\n코드: raw/membership/progress"""
            ),
        )

    @writer_sink("phs_reconciliation_recovery")
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
        if not self.reconciliation_available():
            raise PHSLabelWorkflowError(
                "PHS_LABEL_API_UNAVAILABLE",
                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: reconciliation · API',
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
        if not recoverable:
            return None
        if not isinstance(state.get("action_resolution"), Mapping):
            raise PHSLabelWorkflowError(
                "PHS_RECONCILIATION_JOURNAL_INVALID",
                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: journal · action · snapshot',
            )

        scope = str(state.get("authority_scope_id") or "").strip()
        resolution = dict(state.get("action_resolution") or {})
        selection = dict(resolution.get("selection") or {})
        exchange_id = str(state.get("exchange_id") or "").strip()
        notify('현품표 교환 복구 상태를 확인합니다\n코드: reconciliation · prepare')
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
                    '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: prepare · ACK · exchange · id',
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
                message='회사 서버 현품표와 이 PC의 작업 상태를 복구했습니다\n코드: topology',
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
                            '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: FAILED · print-attempt',
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
                            '인쇄하지 못한 현품표를 확인한 뒤 F5 키를 눌러 다시 인쇄하세요\n코드: target'
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
                    notify('현품표 인쇄를 요청합니다\n코드: target · print-attempt')
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
                            '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: REQUESTED · target · print-attempt',
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
                        '이전 인쇄 결과를 확인해야 합니다\n출력물을 확인한 뒤 재인쇄를 승인하세요\n코드: target',
                        retryable=True,
                        details={"target_label_id": target_id},
                    )
                if target_status not in {
                    "LOCAL_PRINT_SUCCEEDED",
                    "PRINT_COMPLETE_PENDING",
                }:
                    notify('새 현품표를 프린터로 인쇄합니다\n코드: target')
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
                                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: artifact · SHA-256'
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
                        evidence = _submit_physical_print(self.printer,
                            rendered.path,
                            document_name=(
                                "PHS "
                                + str(target.get("worker_code") or "")
                            ),
                            journal=self.journal, attempt=state,
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
                    except WriterFenceError:
                        raise
                    except Exception as exc:
                        if getattr(exc, "code", "") == "LOCAL_PRINT_UNKNOWN":
                            return self._result(state, success=False, message=str(exc), error_code=exc.code, retryable=True)
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
                        except WriterFenceError:
                            raise
                        except Exception as ack_error:
                            raise PHSPhysicalPrintError(
                                '현품표 인쇄와 실패 확인이 복구 대기 중입니다\n현장 관리자에게 알리세요\n코드: ACK'
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
                                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: FAILED · target · print-attempt',
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
                            operator_message=getattr(exc, "operator_message", ""),
                            message=(
                                '해당 현품표를 인쇄하지 못했습니다\n다른 인쇄 완료 기록은 유지합니다\n코드: target · target'
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
                notify('현품표 인쇄 완료를 기록합니다\n코드: target · spool')
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
                        '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: SUCCEEDED · target · print',
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
                '모든 현품표를 인쇄해야 새 현품표를 사용할 수 있습니다\n코드: target',
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
        notify('모두 인쇄한 뒤 새 현품표를 사용할 수 있도록 한 번만 바꿉니다\n코드: topology · ACTIVE')
        try:
            activated = self.client.activate_phs_label_exchange(
                exchange_id,
                authority_scope_id=scope,
                expected_exchange_version=expected_version,
            )
        except WriterFenceError:
            raise
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
                '처리하지 못했습니다\n잠시 후 다시 시도하세요\n자세히: 계속되면 현장 관리자에게 알리세요\n코드: COMMITTED · exchange',
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
                f"""현품표 교체를 완료했습니다\n자세히: {len(activated_targets_by_id)}장 인쇄\n포장 기록과 진행은 유지합니다\n코드: {exchange_kind} · ACTIVE · progress"""
            ),
        )


    def recover_current(
        self,
        current_set: dict[str, Any],
        *,
        persist_current_set: Callable[[], bool],
        status_callback: Callable[[str], None] | None = None,
    ) -> PHSLabelExchangeResult | None:
        return self._resume_current(
            current_set,
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

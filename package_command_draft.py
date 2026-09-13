"""Immutable package draft and exact-membership command evidence."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import re
from typing import Any, Iterable, Mapping
import unicodedata

import carrier_identity_port as carrier_identity
from package_errors import PackageLogisticsError

PACKAGE_CONTRACT_VERSION = "logistics-v1"


MEMBERSHIP_MODES = {"INHERIT_ALL", "EXACT_RESCAN"}


def canonical_member_ids(values: Iterable[Any]) -> tuple[str, ...]:
    return tuple(sorted({str(value or "").strip() for value in values if str(value or "").strip()}))


def canonical_barcodes(values: Iterable[Any]) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                unicodedata.normalize("NFKC", str(value or "")).strip().upper()
                for value in values
                if str(value or "").strip()
            }
        )
    )


def canonical_member_barcodes(values: Any) -> tuple[tuple[str, str], ...]:
    if not isinstance(values, (list, tuple)):
        return ()
    rows: list[tuple[str, str]] = []
    for value in values:
        if not isinstance(value, Mapping):
            return ()
        unit_id = str(value.get("unit_id") or "").strip()
        barcode = _normalize_barcode(value.get("normalized_barcode"))
        if not unit_id or not barcode:
            return ()
        rows.append((unit_id, barcode))
    result = tuple(sorted(rows))
    if (
        len({unit_id for unit_id, _barcode in result}) != len(result)
        or len({barcode for _unit_id, barcode in result}) != len(result)
    ):
        return ()
    return result


def membership_hash(values: Iterable[Any]) -> str:
    body = json.dumps(canonical_member_ids(values), ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def barcode_membership_hash(values: Iterable[Any]) -> str:
    body = json.dumps(canonical_barcodes(values), ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def stable_id(prefix: str, *values: str) -> str:
    digest = hashlib.sha256("|".join(str(value) for value in values).encode("utf-8")).hexdigest()[:24]
    return f"{prefix}-{digest}"


def canonical_json(value: Any) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise PackageLogisticsError(
            "package work-group evidence is not canonical JSON"
        ) from exc


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _attach_operation_lease(
    payload: dict[str, Any], draft: "PackageCommandDraft"
) -> None:
    if not draft.operation_lease_id:
        return
    payload["operation_lease"] = {
        "token": draft.operation_lease_token,
        "lease_id": draft.operation_lease_id,
        "fence": draft.operation_lease_fence,
        "snapshot_hash": draft.operation_lease_snapshot_hash,
        "operation_completed_at": draft.operation_lease_completed_at,
    }


def _json_clone_mapping(
    value: Mapping[str, Any] | None, *, field_name: str
) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise PackageLogisticsError(f"{field_name} must be an object")
    try:
        cloned = json.loads(canonical_json(dict(value)))
    except json.JSONDecodeError as exc:  # pragma: no cover - canonical JSON is readable
        raise PackageLogisticsError(f"{field_name} is invalid") from exc
    if not isinstance(cloned, dict):
        raise PackageLogisticsError(f"{field_name} must be an object")
    return cloned


def _strict_int(value: Any, field_name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise PackageLogisticsError(f"{field_name} is invalid")
    return value


def _strict_member_ids(
    value: Any, field_name: str, *, allow_empty: bool = False
) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        raise PackageLogisticsError(f"{field_name} is missing")
    raw = tuple(str(item or "").strip() for item in value)
    normalized = canonical_member_ids(raw)
    if (
        any(not item for item in raw)
        or raw != normalized
        or (not allow_empty and not normalized)
    ):
        raise PackageLogisticsError(f"{field_name} is not exact canonical membership")
    return normalized


def _strict_entity_versions(
    value: Any, field_name: str = "entity_versions"
) -> dict[str, int]:
    if not isinstance(value, Mapping):
        raise PackageLogisticsError(f"{field_name} is missing")
    result: dict[str, int] = {}
    for raw_key, raw_version in value.items():
        key = str(raw_key or "").strip()
        if not key or key != raw_key or key in result:
            raise PackageLogisticsError(f"{field_name} contains an invalid entity key")
        result[key] = _strict_int(
            raw_version, f"{field_name}.{key}", minimum=0
        )
    if not result:
        raise PackageLogisticsError(f"{field_name} is empty")
    return result


def _normalize_barcode(value: Any) -> str:
    return unicodedata.normalize("NFKC", str(value or "")).strip().upper()


@dataclass(frozen=True)
class PackageCommandDraft:
    set_id: str
    item_code: str
    source_bundle_id: str
    source_external_label: str
    source_input_tag_id: str
    source_bundle_hint: str
    source_authority_scope_id: str
    expected_member_count: int
    expected_membership_hash: str
    expected_authority_epoch: int
    expected_ledger_plane: str
    expected_plane_epoch: int
    package_bundle_id: str
    external_label: str
    membership_mode: str
    sample_barcodes: tuple[str, ...]
    source_input_tag_label_id: str = ""
    source_input_tag_hash_prefix: str = ""
    source_canonical_input_tag_qr: str = ""
    source_active_label_qr_payload: str = ""
    source_active_label_business_date: str = ""
    source_active_label_worker_code: str = ""
    source_active_label_instruction_id: str = ""
    source_active_label_version: int = 0
    source_active_membership_version: int = 0
    exact_rescan_barcodes: tuple[str, ...] = ()
    expected_seal_id: str = ""
    expected_seal_revision: int = 0
    expected_seal_token: str = ""
    expected_seal_qr_payload: str = ""
    source_resolution_basis: str = ""
    phs_work_group: Mapping[str, Any] = field(default_factory=dict)
    work_group_source: Mapping[str, Any] = field(default_factory=dict)
    source_session_ids: tuple[str, ...] = ()
    operation_lease_id: str = ""
    operation_lease_token: str = ""
    operation_lease_fence: int = 0
    operation_lease_snapshot_hash: str = ""
    operation_lease_completed_at: str = ""

    @classmethod
    def build(
        cls,
        *,
        set_id: str,
        item_code: str,
        source_bundle_id: str = "",
        source_external_label: str = "",
        source_input_tag_id: str = "",
        source_input_tag_label_id: str = "",
        source_input_tag_hash_prefix: str = "",
        source_canonical_input_tag_qr: str = "",
        source_active_label_qr_payload: str = "",
        source_active_label_business_date: str = "",
        source_active_label_worker_code: str = "",
        source_active_label_instruction_id: str = "",
        source_active_label_version: int = 0,
        source_active_membership_version: int = 0,
        source_bundle_hint: str = "",
        source_authority_scope_id: str = "",
        expected_member_count: int = 0,
        expected_membership_hash: str = "",
        expected_authority_epoch: int = 0,
        expected_ledger_plane: str = "",
        expected_plane_epoch: int = 0,
        package_bundle_id: str = "",
        external_label: str,
        membership_mode: str = "INHERIT_ALL",
        sample_barcodes: Iterable[str] = (),
        exact_rescan_barcodes: Iterable[str] = (),
        expected_seal_id: str = "",
        expected_seal_revision: int = 0,
        expected_seal_token: str = "",
        expected_seal_qr_payload: str = "",
        source_resolution_basis: str = "",
        phs_work_group: Mapping[str, Any] | None = None,
        work_group_source: Mapping[str, Any] | None = None,
        source_session_ids: Iterable[str] = (),
        operation_lease_id: str = "",
        operation_lease_token: str = "",
        operation_lease_fence: int = 0,
        operation_lease_snapshot_hash: str = "",
        operation_lease_completed_at: str = "",
    ) -> "PackageCommandDraft":
        normalized_set_id = str(set_id or "").strip()
        normalized_item = str(item_code or "").strip()
        source_id = str(source_bundle_id or "").strip()
        source_label = str(source_external_label or "").strip()
        source_input_tag = str(source_input_tag_id or "").strip()
        source_input_tag_label = str(source_input_tag_label_id or "").strip()
        source_input_tag_hash = str(source_input_tag_hash_prefix or "").strip().lower()
        canonical_input_tag_qr = str(
            source_canonical_input_tag_qr or ""
        ).strip()
        active_label_qr = str(
            source_active_label_qr_payload or ""
        ).strip()
        source_hint = str(source_bundle_hint or "").strip()
        source_scope = str(source_authority_scope_id or "").strip()
        final_label = str(external_label or "").strip()
        mode = str(membership_mode or "").strip().upper()
        resolution_basis = str(source_resolution_basis or "").strip().upper()
        frozen_group = _json_clone_mapping(
            phs_work_group, field_name="phs_work_group"
        )
        frozen_source = _json_clone_mapping(
            work_group_source, field_name="work_group_source"
        )
        sessions = canonical_member_ids(source_session_ids)
        lease_id = str(operation_lease_id or "").strip()
        lease_token = str(operation_lease_token or "").strip()
        lease_fence = int(operation_lease_fence or 0)
        lease_snapshot_hash = str(operation_lease_snapshot_hash or "").strip().lower()
        lease_completed_at = str(operation_lease_completed_at or "").strip()
        raw_samples = tuple(_normalize_barcode(value) for value in sample_barcodes)
        raw_exact = tuple(_normalize_barcode(value) for value in exact_rescan_barcodes)
        if not normalized_set_id or not normalized_item or not final_label:
            raise PackageLogisticsError("set_id, item_code, and external_label are required")
        if (
            not source_id
            and not source_input_tag
            and not source_hint
            and resolution_basis != "PHS_WORK_GROUP_EXACT_MEMBERSHIP"
        ):
            raise PackageLogisticsError(
                "sealed transfer QR or structured PHS BND/ITG identity is required"
            )
        if resolution_basis:
            if resolution_basis != "PHS_WORK_GROUP_EXACT_MEMBERSHIP":
                raise PackageLogisticsError(
                    "unsupported package source resolution basis"
                )
            if (
                not frozen_group
                or not frozen_source
                or not source_input_tag
                or not sessions
            ):
                raise PackageLogisticsError(
                    "package work-group draft requires frozen group, source topology, and origins"
                )
            if source_id or source_hint:
                raise PackageLogisticsError(
                    "package work-group draft cannot collapse to one transfer identity"
                )
        elif frozen_group or frozen_source or sessions:
            raise PackageLogisticsError(
                "package work-group evidence requires its exact resolution basis"
            )
        lease_values_present = (
            bool(lease_id),
            bool(lease_token),
            lease_fence > 0,
            bool(lease_snapshot_hash),
            bool(lease_completed_at),
        )
        if any(lease_values_present) and not all(lease_values_present):
            raise PackageLogisticsError(
                "operation lease fields must be supplied together"
            )
        if lease_id and (
            not lease_token.isascii()
            or len(lease_token.encode("ascii")) > 32_768
            or len(lease_snapshot_hash) != 64
            or any(value not in "0123456789abcdef" for value in lease_snapshot_hash)
            or re.fullmatch(
                r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z",
                lease_completed_at,
            )
            is None
        ):
            raise PackageLogisticsError("operation lease evidence is invalid")
        if bool(source_input_tag_label) != bool(source_input_tag_hash):
            raise PackageLogisticsError(
                "structured PHS2 LBL and HSH identity must be supplied together"
            )
        if source_input_tag_hash and (
            len(source_input_tag_hash) != 16
            or any(value not in "0123456789abcdef" for value in source_input_tag_hash)
        ):
            raise PackageLogisticsError("structured PHS2 HSH must be a 16-character hex prefix")
        if any(
            len(value.encode("utf-8")) > 2048
            or any(ord(character) < 32 for character in value)
            for value in (canonical_input_tag_qr, active_label_qr)
        ):
            raise PackageLogisticsError(
                "PHS2 canonical/active QR recovery evidence is invalid"
            )
        if mode not in MEMBERSHIP_MODES:
            raise PackageLogisticsError("membership_mode must be INHERIT_ALL or EXACT_RESCAN")
        sample_error = carrier_identity.legacy_qa_sample_error(raw_samples)
        if sample_error:
            raise PackageLogisticsError(sample_error)
        exact = canonical_barcodes(raw_exact)
        membership_error = carrier_identity.package_membership_error(
            mode, exact, len(raw_exact),
            has_source=bool(source_id or source_input_tag or source_hint),
        )
        if membership_error:
            raise PackageLogisticsError(membership_error)
        package_id = str(package_bundle_id or "").strip()
        if resolution_basis:
            if (
                not package_id
                or package_id
                != str(frozen_source.get("package_bundle_id") or "").strip()
                or final_label
                != str(frozen_source.get("package_external_label") or "").strip()
            ):
                raise PackageLogisticsError(
                    "package work-group deterministic package identity differs from preflight"
                )
        else:
            package_id = package_id or stable_id(
                "PACKAGE",
                source_id or source_hint or source_input_tag or source_label,
                normalized_set_id,
                final_label,
            )
        return cls(
            set_id=normalized_set_id,
            item_code=normalized_item,
            source_bundle_id=source_id,
            source_external_label=source_label,
            source_input_tag_id=source_input_tag,
            source_bundle_hint=source_hint,
            source_authority_scope_id=source_scope,
            expected_member_count=max(0, int(expected_member_count or 0)),
            expected_membership_hash=str(expected_membership_hash or "").strip().lower(),
            expected_authority_epoch=max(0, int(expected_authority_epoch or 0)),
            expected_ledger_plane=str(expected_ledger_plane or "").strip().upper(),
            expected_plane_epoch=max(0, int(expected_plane_epoch or 0)),
            package_bundle_id=package_id,
            external_label=final_label,
            membership_mode=mode,
            sample_barcodes=canonical_barcodes(raw_samples),
            source_input_tag_label_id=source_input_tag_label,
            source_input_tag_hash_prefix=source_input_tag_hash,
            source_canonical_input_tag_qr=canonical_input_tag_qr,
            source_active_label_qr_payload=active_label_qr,
            source_active_label_business_date=str(
                source_active_label_business_date or ""
            ).strip(),
            source_active_label_worker_code=str(
                source_active_label_worker_code or ""
            ).strip(),
            source_active_label_instruction_id=str(
                source_active_label_instruction_id or ""
            ).strip(),
            source_active_label_version=max(
                0, int(source_active_label_version or 0)
            ),
            source_active_membership_version=max(
                0, int(source_active_membership_version or 0)
            ),
            exact_rescan_barcodes=exact,
            expected_seal_id=str(expected_seal_id or "").strip(),
            expected_seal_revision=max(0, int(expected_seal_revision or 0)),
            expected_seal_token=str(expected_seal_token or "").strip(),
            expected_seal_qr_payload=str(expected_seal_qr_payload or "").strip(),
            source_resolution_basis=resolution_basis,
            phs_work_group=frozen_group,
            work_group_source=frozen_source,
            source_session_ids=sessions,
            operation_lease_id=lease_id,
            operation_lease_token=lease_token,
            operation_lease_fence=lease_fence,
            operation_lease_snapshot_hash=lease_snapshot_hash,
            operation_lease_completed_at=lease_completed_at,
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "PackageCommandDraft":
        if not isinstance(value, Mapping):
            raise PackageLogisticsError("saved CREATE_PACKAGE draft is invalid")
        data = dict(value)
        data["sample_barcodes"] = tuple(data.get("sample_barcodes") or ())
        data["exact_rescan_barcodes"] = tuple(
            data.get("exact_rescan_barcodes") or ()
        )
        data["source_session_ids"] = tuple(
            data.get("source_session_ids") or ()
        )
        try:
            return cls.build(**data)
        except (TypeError, ValueError, PackageLogisticsError) as exc:
            raise PackageLogisticsError(
                "saved CREATE_PACKAGE draft is invalid"
            ) from exc

    def fingerprint(self) -> str:
        return hashlib.sha256(
            json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "set_id": self.set_id,
            "item_code": self.item_code,
            "source_bundle_id": self.source_bundle_id,
            "source_external_label": self.source_external_label,
            "source_input_tag_id": self.source_input_tag_id,
            "source_input_tag_label_id": self.source_input_tag_label_id,
            "source_input_tag_hash_prefix": self.source_input_tag_hash_prefix,
            "source_canonical_input_tag_qr": self.source_canonical_input_tag_qr,
            "source_active_label_qr_payload": self.source_active_label_qr_payload,
            "source_active_label_business_date": self.source_active_label_business_date,
            "source_active_label_worker_code": self.source_active_label_worker_code,
            "source_active_label_instruction_id": self.source_active_label_instruction_id,
            "source_active_label_version": self.source_active_label_version,
            "source_active_membership_version": self.source_active_membership_version,
            "source_bundle_hint": self.source_bundle_hint,
            "source_authority_scope_id": self.source_authority_scope_id,
            "expected_member_count": self.expected_member_count,
            "expected_membership_hash": self.expected_membership_hash,
            "expected_authority_epoch": self.expected_authority_epoch,
            "expected_ledger_plane": self.expected_ledger_plane,
            "expected_plane_epoch": self.expected_plane_epoch,
            "package_bundle_id": self.package_bundle_id,
            "external_label": self.external_label,
            "membership_mode": self.membership_mode,
            "sample_barcodes": list(self.sample_barcodes),
            "exact_rescan_barcodes": list(self.exact_rescan_barcodes),
            "expected_seal_id": self.expected_seal_id,
            "expected_seal_revision": self.expected_seal_revision,
            "expected_seal_token": self.expected_seal_token,
            "expected_seal_qr_payload": self.expected_seal_qr_payload,
            "source_resolution_basis": self.source_resolution_basis,
            "phs_work_group": dict(self.phs_work_group),
            "work_group_source": dict(self.work_group_source),
            "source_session_ids": list(self.source_session_ids),
            "operation_lease_id": self.operation_lease_id,
            "operation_lease_token": self.operation_lease_token,
            "operation_lease_fence": self.operation_lease_fence,
            "operation_lease_snapshot_hash": self.operation_lease_snapshot_hash,
            "operation_lease_completed_at": self.operation_lease_completed_at,
        }

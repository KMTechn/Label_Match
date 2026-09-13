"""Pure package source validation; transport and error translation stay in the client."""

from __future__ import annotations

import hashlib
from typing import Any, Callable, Mapping

from package_command_draft import (
    PackageCommandDraft,
    _json_clone_mapping,
    _normalize_barcode,
    _strict_entity_versions,
    _strict_int,
    _strict_member_ids,
    barcode_membership_hash,
    canonical_barcodes,
    canonical_json,
    canonical_member_barcodes,
    canonical_member_ids,
    canonical_sha256,
    membership_hash,
)
from package_errors import PackageLogisticsError

def _validate_transfer_seal(
    value: Any,
    *,
    bundle_id: str,
    bundle_version: int,
    member_ids: tuple[str, ...],
    item_id: str,
    expected_state: str,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PackageLogisticsError("transfer seal evidence is missing")
    seal = dict(value)
    sealed_members = _strict_member_ids(
        seal.get("sealed_member_ids"),
        "transfer seal member_ids",
    )
    pairs = canonical_member_barcodes(seal.get("sealed_members"))
    raw_barcodes = seal.get("sealed_normalized_barcodes")
    if not isinstance(raw_barcodes, (list, tuple)):
        raise PackageLogisticsError(
            "transfer seal barcode membership is missing"
        )
    barcodes = canonical_barcodes(raw_barcodes)
    token = str(seal.get("seal_token") or "").strip()
    if (
        seal.get("seal_contract_version") != "transfer-seal-qr-v1"
        or str(seal.get("seal_state") or "").strip().upper()
        != expected_state
        or not str(seal.get("seal_id") or "").strip()
        or _strict_int(
            seal.get("seal_revision"),
            "transfer seal revision",
            minimum=1,
        )
        < 1
        or not token
        or str(seal.get("seal_token_hash") or "").strip().lower()
        != hashlib.sha256(token.encode("utf-8")).hexdigest()
        or not str(seal.get("seal_qr_payload") or "").strip()
        or str(seal.get("sealed_bundle_id") or "").strip()
        != bundle_id
        or _strict_int(
            seal.get("sealed_bundle_version"),
            "transfer seal bundle version",
            minimum=1,
        )
        != bundle_version
        or sealed_members != member_ids
        or _strict_int(
            seal.get("sealed_member_count"),
            "transfer seal member count",
            minimum=1,
        )
        != len(member_ids)
        or str(seal.get("sealed_membership_hash") or "").strip().lower()
        != membership_hash(member_ids)
        or len(pairs) != len(member_ids)
        or tuple(unit_id for unit_id, _barcode in pairs) != member_ids
        or not barcodes
        or len(raw_barcodes) != len(barcodes)
        or tuple(sorted(barcode for _unit_id, barcode in pairs))
        != barcodes
        or str(
            seal.get("sealed_barcode_membership_hash") or ""
        ).strip().lower()
        != barcode_membership_hash(barcodes)
    ):
        raise PackageLogisticsError(
            f"{item_id} transfer seal exact evidence is invalid"
        )
    return seal


def _validate_work_group_source(
    resolved: Mapping[str, Any],
    draft: PackageCommandDraft,
    *,
    expected_scope: str,
    validate_transfer_seal: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    if not isinstance(resolved, Mapping):
        raise PackageLogisticsError(
            "package work-group resolver response is invalid"
        )
    if (
        resolved.get("source_resolution_basis")
        != "PHS_WORK_GROUP_EXACT_MEMBERSHIP"
        or _strict_int(
            resolved.get("candidate_count"),
            "package work-group candidate_count",
            minimum=1,
        )
        != 1
    ):
        raise PackageLogisticsError(
            "PACKAGE_SOURCE resolver did not select one exact work group"
        )
    group_value = resolved.get("phs_work_group")
    source_value = resolved.get("work_group_source")
    bundle_value = resolved.get("bundle")
    if (
        not isinstance(group_value, Mapping)
        or not isinstance(source_value, Mapping)
        or not isinstance(bundle_value, Mapping)
    ):
        raise PackageLogisticsError(
            "package work-group resolver topology is missing"
        )
    group = _json_clone_mapping(
        group_value, field_name="phs_work_group"
    )
    source = _json_clone_mapping(
        source_value, field_name="work_group_source"
    )
    bundle = _json_clone_mapping(
        bundle_value, field_name="bundle"
    )
    scope = str(source.get("authority_scope_id") or "").strip()
    ledger_plane = str(source.get("ledger_plane") or "").strip().upper()
    plane_epoch = _strict_int(
        source.get("plane_epoch"),
        "work_group_source.plane_epoch",
        minimum=1,
    )
    authority_epoch = _strict_int(
        bundle.get("authority_epoch"),
        "bundle.authority_epoch",
        minimum=0,
    )
    item_id = str(source.get("item_id") or "").strip()
    uom = str(source.get("uom") or "").strip()
    normalized_uom = uom.upper()
    if (
        not scope
        or scope != expected_scope
        or str(bundle.get("authority_scope_id") or "").strip() != scope
        or str(bundle.get("ledger_plane") or "").strip().upper()
        != ledger_plane
        or _strict_int(
            bundle.get("plane_epoch"),
            "bundle.plane_epoch",
            minimum=1,
        )
        != plane_epoch
        or ledger_plane not in {"AUTHORITATIVE", "SHADOW_CANDIDATE"}
        or str(bundle.get("bundle_role") or "").strip().upper()
        != "PACKAGE_SOURCE"
        or str(bundle.get("bundle_type") or "").strip().upper()
        != "TRANSFER"
        or str(bundle.get("bundle_state") or "").strip().upper()
        != "AVAILABLE"
        or str(bundle.get("current_location") or "").strip().upper()
        != "TRANSFER"
        or item_id != draft.item_code
        or str(bundle.get("item_id") or "").strip() != item_id
        or str(bundle.get("uom") or "").strip().upper()
        != normalized_uom
        or not uom
    ):
        raise PackageLogisticsError(
            "package work-group authority, item, or location identity is invalid"
        )

    members = _strict_member_ids(
        source.get("member_ids"), "work_group_source.member_ids"
    )
    group_members = _strict_member_ids(
        group.get("member_ids"), "phs_work_group.member_ids"
    )
    bundle_members = _strict_member_ids(
        bundle.get("member_ids"), "bundle.member_ids"
    )
    member_digest = membership_hash(members)
    if (
        group_members != members
        or bundle_members != members
        or _strict_int(
            source.get("member_count"),
            "work_group_source.member_count",
            minimum=1,
        )
        != len(members)
        or _strict_int(
            group.get("member_count"),
            "phs_work_group.member_count",
            minimum=1,
        )
        != len(members)
        or _strict_int(
            bundle.get("member_count"),
            "bundle.member_count",
            minimum=1,
        )
        != len(members)
        or any(
            str(value or "").strip().lower() != member_digest
            for value in (
                source.get("membership_hash"),
                group.get("membership_hash"),
                bundle.get("membership_hash"),
            )
        )
    ):
        raise PackageLogisticsError(
            "package work-group exact membership proof is inconsistent"
        )
    group_id = str(group.get("group_id") or "").strip()
    label_id = str(group.get("label_id") or "").strip()
    scan_payload = str(group.get("scan_payload") or "").strip()
    anchor_input_tag_id = str(
        group.get("scan_anchor_input_tag_id") or ""
    ).strip()
    if (
        not group_id
        or not label_id
        or str(group.get("state") or "").strip().upper() != "ACTIVE"
        or str(group.get("item_id") or "").strip() != item_id
        or str(group.get("uom") or "").strip().upper()
        != normalized_uom
        or not scan_payload
        or not anchor_input_tag_id
        or (
            draft.source_input_tag_id
            and anchor_input_tag_id != draft.source_input_tag_id
        )
        or (
            draft.source_active_label_qr_payload
            and scan_payload != draft.source_active_label_qr_payload
        )
    ):
        raise PackageLogisticsError(
            "package work-group physical label proof is invalid"
        )
    group_entity_version = _strict_int(
        group.get("group_entity_version"),
        "phs_work_group.group_entity_version",
        minimum=1,
    )
    membership_version = _strict_int(
        group.get("membership_version"),
        "phs_work_group.membership_version",
        minimum=1,
    )
    label_version = _strict_int(
        group.get("label_version"),
        "phs_work_group.label_version",
        minimum=1,
    )
    label_entity_version = _strict_int(
        group.get("label_entity_version"),
        "phs_work_group.label_entity_version",
        minimum=1,
    )

    rows = source.get("members")
    if not isinstance(rows, list) or len(rows) != len(members):
        raise PackageLogisticsError(
            "package work-group selected barcode mapping is incomplete"
        )
    barcode_to_unit: dict[str, str] = {}
    unit_to_barcode: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise PackageLogisticsError(
                "package work-group selected barcode row is invalid"
            )
        unit_id = str(row.get("unit_id") or "").strip()
        barcode = _normalize_barcode(row.get("normalized_barcode"))
        if (
            not unit_id
            or not barcode
            or unit_id in unit_to_barcode
            or barcode in barcode_to_unit
        ):
            raise PackageLogisticsError(
                "package work-group selected barcode mapping is ambiguous"
            )
        unit_to_barcode[unit_id] = barcode
        barcode_to_unit[barcode] = unit_id
    barcodes = canonical_barcodes(unit_to_barcode.values())
    barcode_digest = barcode_membership_hash(barcodes)
    if (
        canonical_member_ids(unit_to_barcode) != members
        or _strict_int(
            source.get("barcode_member_count"),
            "work_group_source.barcode_member_count",
            minimum=1,
        )
        != len(barcodes)
        or str(
            source.get("barcode_membership_hash") or ""
        ).strip().lower()
        != barcode_digest
        or _strict_int(
            bundle.get("barcode_member_count"),
            "bundle.barcode_member_count",
            minimum=1,
        )
        != len(barcodes)
        or str(
            bundle.get("barcode_membership_hash") or ""
        ).strip().lower()
        != barcode_digest
    ):
        raise PackageLogisticsError(
            "package work-group barcode membership proof is inconsistent"
        )

    raw_sources = source.get("source_transfers")
    if not isinstance(raw_sources, list) or not raw_sources:
        raise PackageLogisticsError(
            "package work-group source transfers are missing"
        )
    sources: list[dict[str, Any]] = []
    all_source_members: set[str] = set()
    selected_union: set[str] = set()
    remainder_union: set[str] = set()
    source_ids: list[str] = []
    source_seals: list[dict[str, Any]] = []
    source_iin = str(source.get("source_iin") or "").strip()
    for raw_source in raw_sources:
        if not isinstance(raw_source, Mapping):
            raise PackageLogisticsError(
                "package work-group source transfer is invalid"
            )
        source_spec = _json_clone_mapping(
            raw_source, field_name="source_transfer"
        )
        source_id = str(source_spec.get("bundle_id") or "").strip()
        source_members = _strict_member_ids(
            source_spec.get("source_member_ids"),
            "source_transfer.source_member_ids",
        )
        selected_members = _strict_member_ids(
            source_spec.get("selected_member_ids"),
            "source_transfer.selected_member_ids",
        )
        remainder_members = _strict_member_ids(
            source_spec.get("remainder_member_ids"),
            "source_transfer.remainder_member_ids",
            allow_empty=True,
        )
        source_version = _strict_int(
            source_spec.get("entity_version"),
            "source_transfer.entity_version",
            minimum=1,
        )
        if (
            not source_id
            or source_id in source_ids
            or str(source_spec.get("bundle_type") or "").strip().upper()
            != "TRANSFER"
            or str(source_spec.get("bundle_state") or "").strip().upper()
            != "AVAILABLE"
            or str(
                source_spec.get("accounting_inbound_iin") or ""
            ).strip()
            != source_iin
            or not source_iin
            or _strict_int(
                source_spec.get("source_member_count"),
                "source_transfer.source_member_count",
                minimum=1,
            )
            != len(source_members)
            or str(
                source_spec.get("source_membership_hash") or ""
            ).strip().lower()
            != membership_hash(source_members)
            or _strict_int(
                source_spec.get("selected_member_count"),
                "source_transfer.selected_member_count",
                minimum=1,
            )
            != len(selected_members)
            or str(
                source_spec.get("selected_membership_hash") or ""
            ).strip().lower()
            != membership_hash(selected_members)
            or _strict_int(
                source_spec.get("remainder_member_count"),
                "source_transfer.remainder_member_count",
                minimum=0,
            )
            != len(remainder_members)
            or (
                str(
                    source_spec.get("remainder_membership_hash") or ""
                ).strip().lower()
                if remainder_members
                else source_spec.get("remainder_membership_hash")
            )
            != (
                membership_hash(remainder_members)
                if remainder_members
                else None
            )
            or set(selected_members).intersection(remainder_members)
            or canonical_member_ids(
                (*selected_members, *remainder_members)
            )
            != source_members
            or set(source_members).intersection(all_source_members)
            or set(selected_members).intersection(selected_union)
        ):
            raise PackageLogisticsError(
                "package work-group source partition is inconsistent"
            )
        expected_remainder_id = (
            "TRANSFER-WORK-REMAINDER-"
            + canonical_sha256(
                {
                    "source_transfer_bundle_id": source_id,
                    "member_ids": list(remainder_members),
                }
            )[:24].upper()
            if remainder_members
            else None
        )
        if (
            source_spec.get("remainder_transfer_bundle_id")
            != expected_remainder_id
        ):
            raise PackageLogisticsError(
                "package work-group remainder identity is not deterministic"
            )
        cover_ids = _strict_member_ids(
            source_spec.get("remainder_cover_group_ids"),
            "source_transfer.remainder_cover_group_ids",
            allow_empty=True,
        )
        if bool(cover_ids) != bool(remainder_members):
            raise PackageLogisticsError(
                "package work-group remainder cover proof is incomplete"
            )
        seal = validate_transfer_seal(
            source_spec.get("active_seal"),
            bundle_id=source_id,
            bundle_version=source_version,
            member_ids=source_members,
            item_id=item_id,
            expected_state="ACTIVE",
        )
        source_ids.append(source_id)
        all_source_members.update(source_members)
        selected_union.update(selected_members)
        remainder_union.update(remainder_members)
        source_seals.append(seal)
        sources.append(source_spec)
    if (
        tuple(source_ids) != tuple(sorted(source_ids))
        or _strict_int(
            source.get("source_transfer_count"),
            "work_group_source.source_transfer_count",
            minimum=1,
        )
        != len(sources)
        or source.get("source_transfer_bundle_ids") != source_ids
        or canonical_member_ids(selected_union) != members
    ):
        raise PackageLogisticsError(
            "package work-group source transfer union is inconsistent"
        )

    raw_covers = source.get("remainder_cover_groups")
    if not isinstance(raw_covers, list):
        raise PackageLogisticsError(
            "package work-group remainder cover groups are missing"
        )
    covers: list[dict[str, Any]] = []
    cover_ids: list[str] = []
    covered_union: set[str] = set()
    cover_by_id: dict[str, tuple[str, ...]] = {}
    for raw_cover in raw_covers:
        if not isinstance(raw_cover, Mapping):
            raise PackageLogisticsError(
                "package work-group remainder cover is invalid"
            )
        cover = _json_clone_mapping(
            raw_cover, field_name="remainder_cover_group"
        )
        cover_id = str(cover.get("group_id") or "").strip()
        cover_label_id = str(cover.get("label_id") or "").strip()
        cover_members = _strict_member_ids(
            cover.get("member_ids"), "remainder_cover_group.member_ids"
        )
        covered_members = _strict_member_ids(
            cover.get("covered_member_ids"),
            "remainder_cover_group.covered_member_ids",
        )
        if (
            not cover_id
            or cover_id == group_id
            or cover_id in cover_ids
            or not cover_label_id
            or cover_members != covered_members
            or set(covered_members).intersection(covered_union)
            or _strict_int(
                cover.get("member_count"),
                "remainder_cover_group.member_count",
                minimum=1,
            )
            != len(cover_members)
            or str(
                cover.get("membership_hash") or ""
            ).strip().lower()
            != membership_hash(cover_members)
            or _strict_int(
                cover.get("covered_member_count"),
                "remainder_cover_group.covered_member_count",
                minimum=1,
            )
            != len(covered_members)
            or str(
                cover.get("covered_membership_hash") or ""
            ).strip().lower()
            != membership_hash(covered_members)
            or str(cover.get("item_id") or "").strip() != item_id
            or str(cover.get("uom") or "").strip().upper()
            != normalized_uom
            or not str(cover.get("scan_payload") or "").strip()
            or not str(
                cover.get("scan_anchor_input_tag_id") or ""
            ).strip()
        ):
            raise PackageLogisticsError(
                "package work-group remainder cover membership is inconsistent"
            )
        _strict_int(
            cover.get("membership_version"),
            "remainder_cover_group.membership_version",
            minimum=1,
        )
        _strict_int(
            cover.get("label_version"),
            "remainder_cover_group.label_version",
            minimum=1,
        )
        _strict_int(
            cover.get("group_entity_version"),
            "remainder_cover_group.group_entity_version",
            minimum=1,
        )
        _strict_int(
            cover.get("label_entity_version"),
            "remainder_cover_group.label_entity_version",
            minimum=1,
        )
        cover_ids.append(cover_id)
        covered_union.update(covered_members)
        cover_by_id[cover_id] = covered_members
        covers.append(cover)
    if (
        tuple(cover_ids) != tuple(sorted(cover_ids))
        or canonical_member_ids(covered_union)
        != canonical_member_ids(remainder_union)
    ):
        raise PackageLogisticsError(
            "package work-group remainder topology is not exactly covered"
        )
    for source_spec in sources:
        expected_cover_ids = tuple(
            sorted(
                cover_id
                for cover_id, cover_members in cover_by_id.items()
                if set(cover_members).intersection(
                    source_spec["remainder_member_ids"]
                )
            )
        )
        if tuple(source_spec["remainder_cover_group_ids"]) != expected_cover_ids:
            raise PackageLogisticsError(
                "package work-group source-to-cover topology is inconsistent"
            )

    package_id = str(source.get("package_bundle_id") or "").strip()
    package_external_label = str(
        source.get("package_external_label") or ""
    ).strip()
    expected_package_id = (
        "PACKAGE-WORK-"
        + canonical_sha256(
            {
                "group_id": group_id,
                "label_id": label_id,
                "member_ids": list(members),
            }
        )[:24].upper()
    )
    if (
        package_id != expected_package_id
        or not package_external_label
        or (
            draft.source_resolution_basis
            and draft.package_bundle_id != package_id
        )
        or (
            draft.source_resolution_basis
            and draft.external_label != package_external_label
        )
    ):
        raise PackageLogisticsError(
            "package work-group package identity is inconsistent"
        )
    expected_versions = {
        f"phs_work_group:{group_id}": group_entity_version,
        f"phs_work_membership:{group_id}": membership_version,
        f"phs_work_label_version:{group_id}": label_version,
        f"phs_label:{label_id}": label_entity_version,
        **{
            f"bundle:{source_spec['bundle_id']}": int(
                source_spec["entity_version"]
            )
            for source_spec in sources
        },
        f"bundle:{package_id}": 0,
    }
    for source_spec in sources:
        remainder_id = source_spec.get(
            "remainder_transfer_bundle_id"
        )
        if remainder_id:
            expected_versions[f"bundle:{remainder_id}"] = 0
    for cover in covers:
        cover_id = str(cover["group_id"])
        expected_versions.update(
            {
                f"phs_work_group:{cover_id}": int(
                    cover["group_entity_version"]
                ),
                f"phs_work_membership:{cover_id}": int(
                    cover["membership_version"]
                ),
                f"phs_work_label_version:{cover_id}": int(
                    cover["label_version"]
                ),
                f"phs_label:{cover['label_id']}": int(
                    cover["label_entity_version"]
                ),
            }
        )
    actual_versions = _strict_entity_versions(
        source.get("entity_versions"),
        "work_group_source.entity_versions",
    )
    if (
        actual_versions != expected_versions
        or _strict_entity_versions(
            resolved.get("entity_versions"), "entity_versions"
        )
        != expected_versions
        or _strict_entity_versions(
            bundle.get("entity_versions"), "bundle.entity_versions"
        )
        != expected_versions
    ):
        raise PackageLogisticsError(
            "package work-group expected_versions are not the full topology"
        )
    topology_hash = canonical_sha256(
        {
            "phs_work_group": group,
            "source_transfers": sources,
            "remainder_cover_groups": covers,
            "source_iin": source_iin,
            "barcode_membership_hash": barcode_digest,
            "package_bundle_id": package_id,
        }
    )
    if (
        str(source.get("topology_hash") or "").strip().lower()
        != topology_hash
        or str(resolved.get("topology_hash") or "").strip().lower()
        != topology_hash
    ):
        raise PackageLogisticsError(
            "package work-group topology hash is invalid"
        )
    sessions = _strict_member_ids(
        source.get("source_session_ids"),
        "work_group_source.source_session_ids",
    )
    if (
        bundle.get("active_seals") != source_seals
        or (
            len(sources) == 1
            and bundle.get("active_seal") != source_seals[0]
        )
        or (
            len(sources) != 1
            and bundle.get("active_seal") is not None
        )
    ):
        raise PackageLogisticsError(
            "package work-group plural transfer seals are inconsistent"
        )
    full_single_transfer = bool(
        len(sources) == 1
        and sources[0]["selected_member_ids"]
        == sources[0]["source_member_ids"]
        and not sources[0]["remainder_member_ids"]
        and sources[0].get("remainder_transfer_bundle_id") is None
        and not covers
    )
    if bool(bundle.get("controlled_reseal_eligible")) != (
        len(sources) == 1
    ):
        raise PackageLogisticsError(
            "package work-group reseal eligibility evidence is invalid"
        )
    if draft.source_resolution_basis:
        if (
            draft.source_resolution_basis
            != "PHS_WORK_GROUP_EXACT_MEMBERSHIP"
            or canonical_json(dict(draft.phs_work_group))
            != canonical_json(group)
            or canonical_json(dict(draft.work_group_source))
            != canonical_json(source)
            or draft.source_session_ids != sessions
        ):
            raise PackageLogisticsError(
                "package work-group topology changed after local preflight"
            )
    if (
        draft.expected_member_count
        and draft.expected_member_count != len(members)
    ) or (
        draft.expected_membership_hash
        and draft.expected_membership_hash != member_digest
    ) or (
        draft.expected_authority_epoch
        and draft.expected_authority_epoch != authority_epoch
    ) or (
        draft.expected_ledger_plane
        and draft.expected_ledger_plane != ledger_plane
    ) or (
        draft.expected_plane_epoch
        and draft.expected_plane_epoch != plane_epoch
    ):
        raise PackageLogisticsError(
            "package work-group identity differs from its frozen draft"
        )
    return {
        "authority_scope_id": scope,
        "authority_epoch": authority_epoch,
        "ledger_plane": ledger_plane,
        "plane_epoch": plane_epoch,
        "item_id": item_id,
        "uom": uom,
        "member_ids": members,
        "membership_hash": member_digest,
        "barcodes": barcodes,
        "barcode_membership_hash": barcode_digest,
        "barcode_to_unit": barcode_to_unit,
        "phs_work_group": group,
        "work_group_source": source,
        "source_transfers": sources,
        "remainder_cover_groups": covers,
        "source_session_ids": sessions,
        "source_transfer_bundle_ids": tuple(source_ids),
        "package_bundle_id": package_id,
        "package_external_label": package_external_label,
        "topology_hash": topology_hash,
        "entity_versions": expected_versions,
        "full_single_transfer": full_single_transfer,
        "active_seal": source_seals[0] if full_single_transfer else None,
    }


def _validate_projection(
    projection: Mapping[str, Any],
    draft: PackageCommandDraft,
    *,
    expected_scope: str = "",
    require_package_source_role: bool = False,
) -> dict[str, Any]:
    if not isinstance(projection, Mapping):
        raise PackageLogisticsError("sealed transfer projection must be an object")
    if require_package_source_role and str(projection.get("bundle_role") or "").upper() != "PACKAGE_SOURCE":
        raise PackageLogisticsError("PACKAGE_SOURCE resolver returned the wrong bundle role")
    if str(projection.get("bundle_type") or "").upper() != "TRANSFER":
        raise PackageLogisticsError("package source must be a TRANSFER bundle")
    if str(projection.get("bundle_state") or "").upper() != "AVAILABLE":
        raise PackageLogisticsError("sealed transfer bundle is not available")
    if str(projection.get("current_location") or "").upper() != "TRANSFER":
        raise PackageLogisticsError("package source is not at TRANSFER location")
    item_id = str(projection.get("item_id") or "").strip()
    if not item_id or item_id != draft.item_code:
        raise PackageLogisticsError("sealed transfer item does not match the packaging master label")
    scope = str(projection.get("authority_scope_id") or "").strip()
    if not scope or (expected_scope and scope != expected_scope):
        raise PackageLogisticsError("sealed transfer authority scope does not match the request")
    authority_epoch = projection.get("authority_epoch")
    plane_epoch = projection.get("plane_epoch")
    entity_version = projection.get("entity_version")
    ledger_plane = str(projection.get("ledger_plane") or "").strip().upper()
    if (
        isinstance(authority_epoch, bool)
        or not isinstance(authority_epoch, int)
        or authority_epoch < 0
        or isinstance(plane_epoch, bool)
        or not isinstance(plane_epoch, int)
        or plane_epoch < 1
        or isinstance(entity_version, bool)
        or not isinstance(entity_version, int)
        or entity_version < 1
        or ledger_plane not in {"AUTHORITATIVE", "SHADOW_CANDIDATE"}
    ):
        raise PackageLogisticsError("sealed transfer authority/ledger identity is invalid")
    raw_member_ids = projection.get("member_ids")
    if not isinstance(raw_member_ids, list):
        raise PackageLogisticsError("sealed transfer exact member IDs are missing")
    normalized_member_ids = tuple(str(value or "").strip() for value in raw_member_ids)
    member_ids = canonical_member_ids(normalized_member_ids)
    member_count = projection.get("member_count")
    if (
        not member_ids
        or any(not value for value in normalized_member_ids)
        or len(normalized_member_ids) != len(member_ids)
        or isinstance(member_count, bool)
        or not isinstance(member_count, int)
        or len(member_ids) != member_count
    ):
        raise PackageLogisticsError("sealed transfer exact member count is invalid")
    expected_membership_hash = membership_hash(member_ids)
    if str(projection.get("membership_hash") or "").lower() != expected_membership_hash:
        raise PackageLogisticsError("sealed transfer membership hash is invalid")
    member_rows = projection.get("members")
    if not isinstance(member_rows, list) or len(member_rows) != len(member_ids):
        raise PackageLogisticsError("sealed transfer barcode mapping is partial")
    row_unit_ids: list[str] = []
    row_barcodes: list[str] = []
    for row in member_rows:
        if not isinstance(row, Mapping):
            raise PackageLogisticsError("sealed transfer barcode mapping row is invalid")
        unit_id = str(row.get("unit_id") or "").strip()
        barcode = _normalize_barcode(row.get("normalized_barcode"))
        if not unit_id or not barcode:
            raise PackageLogisticsError("sealed transfer barcode mapping identifier is missing")
        row_unit_ids.append(unit_id)
        row_barcodes.append(barcode)
    if (
        len(set(row_unit_ids)) != len(row_unit_ids)
        or len(set(row_barcodes)) != len(row_barcodes)
        or set(row_unit_ids) != set(member_ids)
    ):
        raise PackageLogisticsError("sealed transfer barcode mapping is ambiguous")
    barcode_member_count = projection.get("barcode_member_count")
    expected_barcode_hash = barcode_membership_hash(row_barcodes)
    barcode_projection_valid = not (
        isinstance(barcode_member_count, bool)
        or not isinstance(barcode_member_count, int)
        or barcode_member_count != len(row_barcodes)
        or str(projection.get("barcode_membership_hash") or "").lower()
        != expected_barcode_hash
    )
    if not barcode_projection_valid:
        active_seal_fallback = projection.get("active_seal")
        barcode_projection_valid = bool(
            draft.expected_seal_id
            and isinstance(active_seal_fallback, Mapping)
            and active_seal_fallback.get("sealed_member_count")
            == len(row_barcodes)
            and str(
                active_seal_fallback.get("sealed_barcode_membership_hash") or ""
            ).lower()
            == expected_barcode_hash
        )
    if not barcode_projection_valid:
        raise PackageLogisticsError("sealed transfer barcode membership evidence is invalid")
    if draft.source_authority_scope_id and (
        str(projection.get("authority_scope_id") or "") != draft.source_authority_scope_id
    ):
        raise PackageLogisticsError("sealed transfer authority scope differs from its QR")
    if draft.expected_member_count and len(member_ids) != draft.expected_member_count:
        raise PackageLogisticsError("sealed transfer quantity differs from its QR")
    if draft.expected_membership_hash and (
        str(projection.get("membership_hash") or "").lower()
        != draft.expected_membership_hash
    ):
        raise PackageLogisticsError("sealed transfer membership hash differs from its QR")
    if draft.expected_authority_epoch and (
        int(projection.get("authority_epoch") or 0) != draft.expected_authority_epoch
    ):
        raise PackageLogisticsError("sealed transfer authority epoch differs from its QR")
    if draft.expected_ledger_plane and (
        str(projection.get("ledger_plane") or "").upper() != draft.expected_ledger_plane
    ):
        raise PackageLogisticsError("sealed transfer ledger plane differs from its QR")
    if draft.expected_plane_epoch and (
        int(projection.get("plane_epoch") or 0) != draft.expected_plane_epoch
    ):
        raise PackageLogisticsError("sealed transfer plane epoch differs from its QR")
    if any(
        (
            draft.expected_seal_id,
            draft.expected_seal_revision,
            draft.expected_seal_token,
            draft.expected_seal_qr_payload,
        )
    ):
        active_seal = projection.get("active_seal")
        if not isinstance(active_seal, Mapping):
            raise PackageLogisticsError("sealed transfer active seal evidence is missing")
        if (
            active_seal.get("seal_contract_version") != "transfer-seal-qr-v1"
            or active_seal.get("seal_state") != "ACTIVE"
            or str(active_seal.get("seal_id") or "") != draft.expected_seal_id
            or active_seal.get("seal_revision") != draft.expected_seal_revision
            or str(active_seal.get("seal_token") or "")
            != draft.expected_seal_token
            or str(active_seal.get("seal_qr_payload") or "")
            != draft.expected_seal_qr_payload
            or str(active_seal.get("sealed_bundle_id") or "")
            != str(projection.get("bundle_id") or "")
            or active_seal.get("sealed_bundle_version") != entity_version
            or canonical_member_ids(active_seal.get("sealed_member_ids") or ())
            != member_ids
            or canonical_member_barcodes(active_seal.get("sealed_members"))
            != tuple(sorted(zip(row_unit_ids, row_barcodes, strict=True)))
            or active_seal.get("sealed_member_count") != len(member_ids)
            or str(active_seal.get("sealed_membership_hash") or "").lower()
            != expected_membership_hash
            or canonical_barcodes(
                active_seal.get("sealed_normalized_barcodes") or ()
            )
            != canonical_barcodes(row_barcodes)
            or str(
                active_seal.get("sealed_barcode_membership_hash") or ""
            ).lower()
            != expected_barcode_hash
        ):
            raise PackageLogisticsError(
                "printed transfer seal is stale; scan the active resealed QR"
            )
    return {
        "member_ids": member_ids,
        "membership_hash": expected_membership_hash,
        "barcodes": canonical_barcodes(row_barcodes),
        "barcode_membership_hash": expected_barcode_hash,
        "barcode_to_unit": dict(zip(row_barcodes, row_unit_ids, strict=True)),
    }

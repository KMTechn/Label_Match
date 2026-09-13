"""Package delivery and receipt verification; facade owns writer admission."""

from __future__ import annotations

import json
import re
import threading
from typing import Any, Callable, Mapping, TYPE_CHECKING

from package_command_draft import (
    PACKAGE_CONTRACT_VERSION,
    PackageCommandDraft,
    _attach_operation_lease,
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
from package_errors import PackageApiError, PackageLogisticsError, PackageTransportError

if TYPE_CHECKING:
    from package_logistics import PackageLogisticsClient, PackageOutbox

class PackageOutboxProcessor:
    def __init__(
        self,
        outbox: PackageOutbox,
        client: PackageLogisticsClient,
    ):
        self.outbox = outbox
        self.client = client
        self._drain_lock = threading.Lock()

    def drain(self, *, limit: int = 20) -> dict[str, int]:
        counts = {"acked": 0, "retry": 0, "conflict": 0}
        with self._drain_lock:
            attempted_keys: set[str] = set()
            for _ in range(max(0, int(limit))):
                row = self.outbox.claim_next(exclude_keys=attempted_keys)
                if row is None:
                    break
                key = row["idempotency_key"]
                attempted_keys.add(str(key))
                draft: PackageCommandDraft | None = None
                try:
                    draft_data = json.loads(row["draft_json"])
                    draft = PackageCommandDraft.from_dict(draft_data)
                    if row.get("command_json"):
                        command = json.loads(row["command_json"])
                        source_id = str(row.get("resolved_source_bundle_id") or "").strip()
                        if not source_id:
                            raise PackageLogisticsError("saved package command lost its source bundle ID")
                        scope = str(command.get("authority_scope_id") or "").strip()
                        receipt = self.client.get_receipt_if_exists(
                            key, authority_scope_id=scope
                        )
                        if receipt is None:
                            receipt = self.client.create_package(command)
                    else:
                        source_id, command = self.client.build_create_package_command(
                            draft, idempotency_key=key
                        )
                        self.outbox.save_command(key, source_id, command)
                        receipt = self.client.create_package(command)
                    self._validate_receipt(draft, source_id, receipt, command=command)
                    consumption = (
                        receipt.get("data", receipt).get(
                            "operation_lease_consumption"
                        )
                        if isinstance(receipt.get("data", receipt), Mapping)
                        else None
                    )
                    self.outbox.mark_acked(
                        key,
                        receipt,
                        operation_lease_id=draft.operation_lease_id,
                        operation_lease_consumption=consumption,
                    )
                    counts["acked"] += 1
                except PackageApiError as exc:
                    if exc.committed is True:
                        self.outbox.mark_conflict(
                            key,
                            exc,
                            operation_lease_id=(
                                draft.operation_lease_id if draft else ""
                            ),
                        )
                        counts["conflict"] += 1
                    elif (
                        exc.status_code not in {409, 412}
                        and (
                            exc.status_code in {408, 425, 429}
                            or exc.status_code >= 500
                            or exc.retryable is True
                        )
                    ):
                        self.outbox.mark_retry(key, exc)
                        counts["retry"] += 1
                    else:
                        self.outbox.mark_conflict(
                            key,
                            exc,
                            operation_lease_id=(
                                draft.operation_lease_id if draft else ""
                            ),
                        )
                        counts["conflict"] += 1
                except PackageTransportError as exc:
                    self.outbox.mark_retry(key, exc)
                    counts["retry"] += 1
                except PackageLogisticsError as exc:
                    self.outbox.mark_conflict(
                        key,
                        exc,
                        operation_lease_id=(
                            draft.operation_lease_id if draft else ""
                        ),
                    )
                    counts["conflict"] += 1
        return counts

    @staticmethod
    def _validate_operation_lease_receipt(
        draft: PackageCommandDraft,
        receipt: Mapping[str, Any],
    ) -> None:
        if not draft.operation_lease_id:
            return
        data = (
            receipt.get("data")
            if isinstance(receipt.get("data"), Mapping)
            else receipt
        )
        consumption = (
            data.get("operation_lease_consumption")
            if isinstance(data, Mapping)
            else None
        )
        if not isinstance(consumption, Mapping) or set(consumption) != {
            "contract_version",
            "lease_id",
            "status",
            "fence",
            "operation_result_id",
            "consumed_at",
        }:
            raise PackageLogisticsError(
                "package receipt operation lease consumption is missing"
            )
        receipt_id = str(
            data.get("receipt_id")
            or receipt.get("receipt_id")
            or ""
        ).strip()
        if (
            consumption.get("contract_version")
            != "terminal-operation-lease-consume-v1"
            or consumption.get("status") != "CONSUMED"
            or str(consumption.get("lease_id") or "")
            != draft.operation_lease_id
            or int(consumption.get("fence") or 0)
            != draft.operation_lease_fence
            or not receipt_id
            or str(consumption.get("operation_result_id") or "")
            != receipt_id
            or re.fullmatch(
                r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z",
                str(consumption.get("consumed_at") or ""),
            )
            is None
        ):
            raise PackageLogisticsError(
                "package receipt operation lease consumption is invalid"
            )

    @staticmethod
    def _validate_work_group_receipt(
        draft: PackageCommandDraft,
        source_identity: str,
        receipt: Mapping[str, Any],
        *,
        command: Mapping[str, Any] | None,
        validate_transfer_seal: Callable[..., dict[str, Any]],
    ) -> None:
        if not isinstance(receipt, Mapping) or not isinstance(command, Mapping):
            raise PackageLogisticsError(
                "package work-group command/receipt is invalid"
            )
        payload = command.get("payload")
        command_versions = command.get("expected_versions")
        if not isinstance(payload, Mapping):
            raise PackageLogisticsError(
                "saved package work-group command payload is invalid"
            )
        group = _json_clone_mapping(
            draft.phs_work_group, field_name="phs_work_group"
        )
        source = _json_clone_mapping(
            draft.work_group_source, field_name="work_group_source"
        )
        sources = source.get("source_transfers")
        covers = source.get("remainder_cover_groups")
        if not isinstance(sources, list) or not isinstance(covers, list):
            raise PackageLogisticsError(
                "saved package work-group topology is invalid"
            )
        group_id = str(group.get("group_id") or "").strip()
        package_id = str(source.get("package_bundle_id") or "").strip()
        expected_versions = _strict_entity_versions(
            source.get("entity_versions"),
            "work_group_source.entity_versions",
        )
        members = _strict_member_ids(
            source.get("member_ids"), "work_group_source.member_ids"
        )
        member_digest = membership_hash(members)
        expected_payload = {
            "source_resolution_basis": (
                "PHS_WORK_GROUP_EXACT_MEMBERSHIP"
            ),
            "phs_work_group": group,
            "source_transfers": sources,
            "remainder_cover_groups": covers,
            "topology_hash": str(source.get("topology_hash") or ""),
            "package_bundle_id": package_id,
            "external_label": str(
                source.get("package_external_label") or ""
            ),
            "item_id": str(source.get("item_id") or ""),
            "uom": str(source.get("uom") or ""),
            "membership_mode": draft.membership_mode,
            "member_ids": list(members),
            "membership_hash": member_digest,
            "sample_barcodes": list(draft.sample_barcodes),
        }
        if draft.membership_mode == "EXACT_RESCAN":
            expected_payload["exact_rescan_barcodes"] = list(
                draft.exact_rescan_barcodes
            )
            expected_payload["barcode_membership_hash"] = (
                barcode_membership_hash(draft.exact_rescan_barcodes)
            )
        _attach_operation_lease(expected_payload, draft)
        if (
            not group_id
            or source_identity != group_id
            or str(command.get("contract_version") or "")
            != PACKAGE_CONTRACT_VERSION
            or str(command.get("command_type") or "")
            != "CREATE_PACKAGE"
            or not str(command.get("idempotency_key") or "").strip()
            or str(command.get("authority_scope_id") or "")
            != str(source.get("authority_scope_id") or "")
            or _strict_int(
                command.get("authority_epoch"),
                "command.authority_epoch",
                minimum=0,
            )
            != draft.expected_authority_epoch
            or str(command.get("ledger_plane") or "").upper()
            != str(source.get("ledger_plane") or "").upper()
            or _strict_int(
                command.get("plane_epoch"),
                "command.plane_epoch",
                minimum=1,
            )
            != int(source.get("plane_epoch") or 0)
            or _strict_entity_versions(
                command_versions, "command.expected_versions"
            )
            != expected_versions
            or canonical_json(dict(payload))
            != canonical_json(expected_payload)
        ):
            raise PackageLogisticsError(
                "saved package work-group command differs from frozen preflight"
            )

        receipt_id = str(receipt.get("receipt_id") or "").strip()
        if (
            not receipt_id
            or str(receipt.get("contract_version") or "")
            != PACKAGE_CONTRACT_VERSION
            or str(receipt.get("command_type") or "")
            != "CREATE_PACKAGE"
            or str(receipt.get("status") or "").strip().upper()
            != "COMMITTED"
            or str(receipt.get("authority_scope_id") or "")
            != str(command.get("authority_scope_id") or "")
            or _strict_int(
                receipt.get("authority_epoch"),
                "receipt.authority_epoch",
                minimum=0,
            )
            != int(command.get("authority_epoch") or 0)
            or str(
                receipt.get("resolved_ledger_plane") or ""
            ).strip().upper()
            != str(command.get("ledger_plane") or "").strip().upper()
            or _strict_int(
                receipt.get("resolved_plane_epoch"),
                "receipt.resolved_plane_epoch",
                minimum=1,
            )
            != int(command.get("plane_epoch") or 0)
            or not str(receipt.get("committed_at") or "").strip()
            or not isinstance(receipt.get("event_ids"), (list, tuple))
            or len(receipt.get("event_ids") or ()) != 1
            or not str((receipt.get("event_ids") or ("",))[0] or "").strip()
            or not isinstance(receipt.get("outbox_ids"), (list, tuple))
            or len(receipt.get("outbox_ids") or ()) != 1
            or not str((receipt.get("outbox_ids") or ("",))[0] or "").strip()
        ):
            raise PackageLogisticsError(
                "package work-group receipt identity is invalid"
            )
        data = receipt.get("data")
        if not isinstance(data, Mapping):
            raise PackageLogisticsError(
                "package work-group receipt data is missing"
            )
        source_ids = [str(value.get("bundle_id") or "") for value in sources]
        source_sessions = list(draft.source_session_ids)
        selected_rows = source.get("members")
        if not isinstance(selected_rows, list):
            raise PackageLogisticsError(
                "package work-group selected members are missing"
            )
        expected_member_rows = [
            {
                "unit_id": str(row.get("unit_id") or ""),
                "normalized_barcode": _normalize_barcode(
                    row.get("normalized_barcode")
                ),
            }
            for row in selected_rows
            if isinstance(row, Mapping)
        ]
        if len(expected_member_rows) != len(members):
            raise PackageLogisticsError(
                "package work-group selected member rows are invalid"
            )

        expected_transitions: list[dict[str, Any]] = []
        expected_remainder_bases: list[dict[str, Any]] = []
        remainder_ids: list[str] = []
        source_seals_consumed: list[dict[str, Any]] = []
        cover_remainder_roots: set[tuple[str, str]] = set()
        for source_spec in sources:
            source_id = str(source_spec.get("bundle_id") or "")
            source_members = _strict_member_ids(
                source_spec.get("source_member_ids"),
                "source_transfer.source_member_ids",
            )
            selected = _strict_member_ids(
                source_spec.get("selected_member_ids"),
                "source_transfer.selected_member_ids",
            )
            remainder = _strict_member_ids(
                source_spec.get("remainder_member_ids"),
                "source_transfer.remainder_member_ids",
                allow_empty=True,
            )
            before = _strict_int(
                source_spec.get("entity_version"),
                "source_transfer.entity_version",
                minimum=1,
            )
            remainder_id = source_spec.get(
                "remainder_transfer_bundle_id"
            )
            expected_transitions.append(
                {
                    "source_transfer_bundle_id": source_id,
                    "entity_version_before": before,
                    "entity_version_after": before + 1,
                    "state_before": "AVAILABLE",
                    "state_after": "CONSUMED",
                    "source_member_ids": list(source_members),
                    "source_member_count": len(source_members),
                    "source_membership_hash": membership_hash(
                        source_members
                    ),
                    "selected_member_ids": list(selected),
                    "selected_member_count": len(selected),
                    "selected_membership_hash": membership_hash(selected),
                    "remainder_transfer_bundle_id": remainder_id,
                }
            )
            active_seal = _json_clone_mapping(
                source_spec.get("active_seal"),
                field_name="source_transfer.active_seal",
            )
            source_seals_consumed.append(
                {**active_seal, "seal_state": "CONSUMED"}
            )
            if remainder_id:
                remainder_id = str(remainder_id)
                remainder_ids.append(remainder_id)
                pair_map = dict(
                    canonical_member_barcodes(
                        active_seal.get("sealed_members")
                    )
                )
                remainder_rows = [
                    {
                        "unit_id": unit_id,
                        "normalized_barcode": pair_map.get(unit_id, ""),
                    }
                    for unit_id in remainder
                ]
                if any(
                    not row["normalized_barcode"]
                    for row in remainder_rows
                ):
                    raise PackageLogisticsError(
                        "saved remainder barcode mapping is incomplete"
                    )
                expected_remainder_bases.append(
                    {
                        "source_transfer_bundle_id": source_id,
                        "remainder_transfer_bundle_id": remainder_id,
                        "member_ids": list(remainder),
                        "members": remainder_rows,
                        "member_count": len(remainder),
                        "membership_hash": membership_hash(remainder),
                        "entity_version": 1,
                    }
                )
                for cover_id in source_spec.get(
                    "remainder_cover_group_ids"
                ) or ():
                    cover_remainder_roots.add(
                        (str(cover_id), remainder_id)
                    )
        if data.get("source_transitions") != expected_transitions:
            raise PackageLogisticsError(
                "package work-group receipt source transitions are invalid"
            )

        remainder_values = data.get("remainder_transfers")
        remainder_seals = data.get("remainder_transfer_seals")
        if (
            not isinstance(remainder_values, list)
            or len(remainder_values) != len(expected_remainder_bases)
            or not isinstance(remainder_seals, list)
            or len(remainder_seals) != len(expected_remainder_bases)
        ):
            raise PackageLogisticsError(
                "package work-group receipt remainder evidence is invalid"
            )
        seal_keys = {
            "seal_contract_version",
            "seal_state",
            "seal_id",
            "seal_revision",
            "seal_token",
            "seal_token_hash",
            "seal_qr_payload",
            "sealed_bundle_id",
            "sealed_bundle_version",
            "sealed_member_ids",
            "sealed_members",
            "sealed_member_count",
            "sealed_membership_hash",
            "sealed_normalized_barcodes",
            "sealed_barcode_membership_hash",
        }
        validated_remainder_seals: list[dict[str, Any]] = []
        for actual, expected_base, listed_seal in zip(
            remainder_values,
            expected_remainder_bases,
            remainder_seals,
            strict=True,
        ):
            if not isinstance(actual, Mapping) or not isinstance(
                listed_seal, Mapping
            ):
                raise PackageLogisticsError(
                    "package work-group remainder receipt row is invalid"
                )
            if any(actual.get(key) != value for key, value in expected_base.items()):
                raise PackageLogisticsError(
                    "package work-group remainder membership differs from topology"
                )
            actual_seal = {
                key: actual.get(key)
                for key in seal_keys
                if key in actual
            }
            if (
                set(actual_seal) != seal_keys
                or actual_seal != dict(listed_seal)
            ):
                raise PackageLogisticsError(
                    "package work-group remainder seal receipt is inconsistent"
                )
            validate_transfer_seal(
                actual_seal,
                bundle_id=str(
                    expected_base["remainder_transfer_bundle_id"]
                ),
                bundle_version=1,
                member_ids=tuple(expected_base["member_ids"]),
                item_id=draft.item_code,
                expected_state="ACTIVE",
            )
            validated_remainder_seals.append(actual_seal)
        if (
            data.get("remainder_transfer_bundle_ids") != remainder_ids
            or data.get("remainder_transfer_seals")
            != validated_remainder_seals
            or data.get("source_transfer_seals_consumed")
            != source_seals_consumed
        ):
            raise PackageLogisticsError(
                "package work-group plural seal evidence is invalid"
            )

        expected_roots = [
            {
                "group_id": group_id,
                "root_type": "PACKAGE",
                "root_id": package_id,
                "root_role": "SOURCE",
                "added_receipt_id": receipt_id,
            },
            *[
                {
                    "group_id": cover_id,
                    "root_type": "TRANSFER_BUNDLE",
                    "root_id": remainder_id,
                    "root_role": "SOURCE",
                    "added_receipt_id": receipt_id,
                }
                for cover_id, remainder_id in sorted(
                    cover_remainder_roots
                )
            ],
        ]
        expected_roots.sort(
            key=lambda value: (
                value["group_id"],
                value["root_type"],
                value["root_id"],
            )
        )
        if data.get("root_proof") != expected_roots:
            raise PackageLogisticsError(
                "package work-group root proof is invalid"
            )
        group_versions_after = {
            group_id: int(group["group_entity_version"]) + 1,
            **{
                str(cover["group_id"]): int(
                    cover["group_entity_version"]
                )
                + 1
                for cover in covers
            },
        }
        topology_after = canonical_sha256(
            {
                "topology_hash_before": str(source["topology_hash"]),
                "package_bundle_id": package_id,
                "remainder_transfer_bundle_ids": remainder_ids,
                "root_proof": expected_roots,
                "group_entity_versions": group_versions_after,
            }
        )
        receipt_versions = dict(expected_versions)
        for source_spec in sources:
            source_id = str(source_spec["bundle_id"])
            receipt_versions[f"bundle:{source_id}"] = (
                int(source_spec["entity_version"]) + 1
            )
        receipt_versions[f"bundle:{package_id}"] = 1
        for remainder_id in remainder_ids:
            receipt_versions[f"bundle:{remainder_id}"] = 1
        for after_group_id, version in group_versions_after.items():
            receipt_versions[f"phs_work_group:{after_group_id}"] = version
        if (
            data.get("atomic") is not True
            or data.get("receipt_contract_version")
            != "PHS_WORK_GROUP_PACKAGE_V1"
            or data.get("source_resolution_basis")
            != "PHS_WORK_GROUP_EXACT_MEMBERSHIP"
            or data.get("phs_work_group") != group
            or data.get("source_transfers") != sources
            or data.get("remainder_cover_groups") != covers
            or data.get("source_bundle_id")
            != (source_ids[0] if len(source_ids) == 1 else None)
            or data.get("source_bundle_ids") != source_ids
            or data.get("source_bundle_count") != len(source_ids)
            or data.get("source_session_ids") != source_sessions
            or str(data.get("package_bundle_id") or "") != package_id
            or str(data.get("membership_mode") or "").upper()
            != draft.membership_mode
            or data.get("member_ids") != list(members)
            or data.get("members") != expected_member_rows
            or data.get("member_count") != len(members)
            or str(data.get("membership_hash") or "").lower()
            != member_digest
            or data.get("source_location") != "TRANSFER"
            or data.get("destination_location") != "SHIPPING-WAIT"
            or not str(data.get("movement_id") or "").strip()
            or data.get("sample_barcodes")
            != list(draft.sample_barcodes)
            or str(data.get("inbound_iin") or "")
            != str(source.get("source_iin") or "")
            or str(data.get("item_id") or "") != draft.item_code
            or str(data.get("uom") or "").upper()
            != str(source.get("uom") or "").upper()
            or data.get("topology_hash_before")
            != str(source.get("topology_hash") or "")
            or data.get("topology_hash_after") != topology_after
            or data.get("group_entity_versions_after")
            != group_versions_after
            or _strict_entity_versions(
                receipt.get("entity_versions"),
                "receipt.entity_versions",
            )
            != receipt_versions
        ):
            raise PackageLogisticsError(
                "package work-group receipt aggregate proof is invalid"
            )
        expected_exact = (
            list(draft.exact_rescan_barcodes)
            if draft.membership_mode == "EXACT_RESCAN"
            else []
        )
        expected_barcode_hash = (
            barcode_membership_hash(draft.exact_rescan_barcodes)
            if draft.membership_mode == "EXACT_RESCAN"
            else None
        )
        if (
            data.get("exact_rescan_barcodes") != expected_exact
            or data.get("exact_rescan_count") != len(expected_exact)
            or data.get("barcode_membership_hash")
            != expected_barcode_hash
        ):
            raise PackageLogisticsError(
                "package work-group receipt barcode evidence is invalid"
            )

    @staticmethod
    def _validate_receipt(
        draft: PackageCommandDraft,
        source_bundle_id: str,
        receipt: Mapping[str, Any],
        *,
        command: Mapping[str, Any] | None = None,
        validate_operation_lease_receipt: Callable[..., None],
        validate_work_group_receipt: Callable[..., None],
    ) -> None:
        validate_operation_lease_receipt(
            draft, receipt
        )
        if draft.source_resolution_basis:
            validate_work_group_receipt(
                draft,
                source_bundle_id,
                receipt,
                command=command,
            )
            return
        data = receipt.get("data") if isinstance(receipt.get("data"), Mapping) else receipt
        if not isinstance(data, Mapping):
            raise PackageLogisticsError("package receipt data is invalid")
        if str(data.get("source_bundle_id") or "") != source_bundle_id:
            raise PackageLogisticsError("package receipt source bundle does not match")
        if str(data.get("package_bundle_id") or "") != draft.package_bundle_id:
            raise PackageLogisticsError("package receipt package bundle does not match")
        raw_members = data.get("member_ids")
        if not isinstance(raw_members, list):
            raise PackageLogisticsError("package receipt member IDs are missing")
        normalized_members = tuple(str(value or "").strip() for value in raw_members)
        members = canonical_member_ids(normalized_members)
        member_count = data.get("member_count")
        if (
            not members
            or any(not value for value in normalized_members)
            or len(normalized_members) != len(members)
            or isinstance(member_count, bool)
            or not isinstance(member_count, int)
            or len(members) != member_count
        ):
            raise PackageLogisticsError("package receipt member count is invalid")
        if str(data.get("membership_hash") or "") != membership_hash(members):
            raise PackageLogisticsError("package receipt membership hash is invalid")
        if str(data.get("source_bundle_type") or "").upper() != "TRANSFER":
            raise PackageLogisticsError("package receipt source bundle type is invalid")
        if str(data.get("membership_mode") or "").upper() != draft.membership_mode:
            raise PackageLogisticsError("package receipt membership mode does not match")
        if draft.membership_mode == "INHERIT_ALL":
            command_payload = command.get("payload") if isinstance(command, Mapping) else None
            expected_evidence = (
                command_payload.get("source_evidence")
                if isinstance(command_payload, Mapping)
                else None
            )
            actual_evidence = data.get("source_evidence")
            if not isinstance(expected_evidence, Mapping):
                raise PackageLogisticsError(
                    "saved INHERIT_ALL command is missing immutable source evidence"
                )
            if not isinstance(actual_evidence, Mapping):
                raise PackageLogisticsError(
                    "package receipt is missing inherited source evidence"
                )
            expected_raw_ids = expected_evidence.get("member_ids")
            actual_raw_ids = actual_evidence.get("member_ids")
            if not isinstance(expected_raw_ids, list) or not isinstance(actual_raw_ids, list):
                raise PackageLogisticsError("package source evidence member IDs are invalid")
            expected_ids_normalized = tuple(str(value or "").strip() for value in expected_raw_ids)
            actual_ids_normalized = tuple(str(value or "").strip() for value in actual_raw_ids)
            expected_ids = canonical_member_ids(expected_ids_normalized)
            actual_ids = canonical_member_ids(actual_ids_normalized)
            expected_digest = membership_hash(expected_ids) if expected_ids else ""
            if (
                not expected_ids
                or any(not value for value in expected_ids_normalized + actual_ids_normalized)
                or len(expected_ids_normalized) != len(expected_ids)
                or len(actual_ids_normalized) != len(actual_ids)
                or actual_ids != expected_ids
                or members != expected_ids
                or str(expected_evidence.get("membership_hash") or "").lower()
                != expected_digest
                or str(actual_evidence.get("membership_hash") or "").lower()
                != expected_digest
                or str(actual_evidence.get("barcode_membership_hash") or "").lower()
                != str(expected_evidence.get("barcode_membership_hash") or "").lower()
                or not str(expected_evidence.get("barcode_membership_hash") or "").strip()
            ):
                raise PackageLogisticsError(
                    "package receipt inherited membership differs from source evidence"
                )
        if draft.membership_mode == "EXACT_RESCAN":
            raw_exact = tuple(
                _normalize_barcode(value)
                for value in (data.get("exact_rescan_barcodes") or [])
            )
            exact = canonical_barcodes(raw_exact)
            if (
                any(not value for value in raw_exact)
                or len(raw_exact) != len(exact)
                or exact != draft.exact_rescan_barcodes
            ):
                raise PackageLogisticsError("package receipt exact rescan membership is invalid")
            if int(data.get("exact_rescan_count") or 0) != len(exact):
                raise PackageLogisticsError("package receipt exact rescan count is invalid")
            if str(data.get("barcode_membership_hash") or "") != barcode_membership_hash(exact):
                raise PackageLogisticsError("package receipt barcode membership hash is invalid")

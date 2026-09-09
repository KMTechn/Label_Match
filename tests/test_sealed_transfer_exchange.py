from copy import deepcopy
import hashlib
import json

import pytest

from package_logistics import (
    PackageApiError,
    PackageClientConfig,
    PackageCommandDraft,
    PackageLogisticsClient,
    PackageLogisticsError,
    PackageTransportError,
    barcode_membership_hash,
    membership_hash,
)
from sealed_transfer_exchange import (
    SealedTransferExchangeCoordinator,
    SealedTransferExchangeStore,
)
from Label_Match import (
    Label_Match,
    _label_match_apply_sealed_exchange_state,
    _label_match_recover_central_state_from_package_row,
)


SCOPE = "scope-main"
TARGET = "TRANSFER-001"
SOURCE = "PHS-GOOD-002"
ITEM = "ITEM-001"
OLD_IDS = ("unit-keep", "unit-old")
OLD_BARCODES = ("BC-KEEP", "BC-OLD")
SOURCE_IDS = ("unit-new",)
SOURCE_BARCODES = ("BC-NEW",)


def _qr(*, ids=OLD_IDS, revision=1, seal_id="seal-1", token="token-1"):
    return "|".join(
        (
            "TRF=1",
            f"BND={TARGET}",
            f"AUTH_SCOPE={SCOPE}",
            f"CLC={ITEM}",
            f"QT={len(ids)}",
            f"HSH={membership_hash(ids)}",
            "EPOCH=7",
            "PLANE=AUTHORITATIVE",
            "PE=3",
            f"SID={seal_id}",
            f"SREV={revision}",
            f"STK={token}",
        )
    )


OLD_QR = _qr()


def _fields(qr=OLD_QR):
    raw = dict(part.split("=", 1) for part in qr.split("|"))
    return {
        **raw,
        "QT": int(raw["QT"]),
        "EPOCH": int(raw["EPOCH"]),
        "PE": int(raw["PE"]),
        "SREV": int(raw["SREV"]),
    }


def _target_projection(*, qr=OLD_QR, seal_id="seal-1", token="token-1", revision=1):
    members = [
        {"unit_id": unit_id, "normalized_barcode": barcode}
        for unit_id, barcode in zip(OLD_IDS, OLD_BARCODES, strict=True)
    ]
    return {
        "authority_scope_id": SCOPE,
        "authority_epoch": 7,
        "ledger_plane": "AUTHORITATIVE",
        "plane_epoch": 3,
        "bundle_id": TARGET,
        "bundle_type": "TRANSFER",
        "bundle_state": "AVAILABLE",
        "current_location": "TRANSFER",
        "item_id": ITEM,
        "source_iin": "IIN-001",
        "uom": "EA",
        "member_ids": list(OLD_IDS),
        "member_count": len(OLD_IDS),
        "membership_hash": membership_hash(OLD_IDS),
        "barcode_member_count": len(OLD_BARCODES),
        "barcode_membership_hash": barcode_membership_hash(OLD_BARCODES),
        "entity_version": 1,
        "members": members,
        "active_seal": {
            "seal_contract_version": "transfer-seal-qr-v1",
            "seal_state": "ACTIVE",
            "seal_id": seal_id,
            "seal_revision": revision,
            "seal_token": token,
            "seal_token_hash": hashlib.sha256(token.encode()).hexdigest(),
            "seal_qr_payload": qr,
            "sealed_bundle_id": TARGET,
            "sealed_bundle_version": 1,
            "sealed_member_ids": list(OLD_IDS),
            "sealed_members": members,
            "sealed_member_count": len(OLD_IDS),
            "sealed_membership_hash": membership_hash(OLD_IDS),
            "sealed_normalized_barcodes": list(OLD_BARCODES),
            "sealed_barcode_membership_hash": barcode_membership_hash(OLD_BARCODES),
        },
    }


def _good_resolver(*, multi_member=False):
    source_ids = SOURCE_IDS + (("unit-remain",) if multi_member else ())
    source_barcodes = SOURCE_BARCODES + (("BC-REMAIN",) if multi_member else ())
    members = [
        {"unit_id": unit_id, "normalized_barcode": barcode}
        for unit_id, barcode in zip(source_ids, source_barcodes, strict=True)
    ]
    return {
        "contract_version": "logistics-good-replacement-source-v1",
        "candidate_count": 1,
        "authority_scope_id": SCOPE,
        "authority_epoch": 7,
        "ledger_plane": "AUTHORITATIVE",
        "plane_epoch": 3,
        "inbound_iin": "IIN-001",
        "source_bundle_id": SOURCE,
        "source_bundle_entity_version": 5,
        "unit_id": "unit-new",
        "normalized_barcode": "BC-NEW",
        "unit": {
            "unit_id": "unit-new",
            "normalized_barcode": "BC-NEW",
            "current_location": "PHS_GOOD",
            "state": "CONSUMED",
        },
        "source_bundle": {
            "bundle_id": SOURCE,
            "bundle_type": "PHS",
            "bundle_state": "AVAILABLE",
            "entity_version": 5,
            "item_id": ITEM,
            "uom": "EA",
            "member_ids": list(source_ids),
            "member_count": len(source_ids),
            "membership_hash": membership_hash(source_ids),
            "members": members,
        },
        "replacement_evidence": {
            "new_unit_id": "unit-new",
            "new_source_bundle_id": SOURCE,
            "expected_source_bundle_version": 5,
            "source_member_ids": list(source_ids),
            "source_membership_hash": membership_hash(source_ids),
            "inbound_iin": "IIN-001",
            "item_id": ITEM,
            "uom": "EA",
        },
    }


def _receipt(command):
    damage_id = command["payload"]["damage_bundle_id"]
    new_ids = ("unit-keep", "unit-new")
    new_barcodes = ("BC-KEEP", "BC-NEW")
    new_qr = _qr(
        ids=new_ids,
        revision=2,
        seal_id="seal-2",
        token="token-2",
    )
    pair = command["payload"]["pairs"][0]
    data = {
        "receipt_contract_version": "sealed-transfer-member-replacement-v1",
        "idempotency_key": command["idempotency_key"],
        "target_bundle_id": TARGET,
        "target_bundle_type": "TRANSFER",
        "target_version_before": 1,
        "target_version_after": 2,
        "old_seal_id": "seal-1",
        "old_seal_revision": 1,
        "old_seal_token_hash": hashlib.sha256(b"token-1").hexdigest(),
        "old_seal_qr_payload": OLD_QR,
        "old_member_ids": list(OLD_IDS),
        "old_members": [
            {"unit_id": unit_id, "normalized_barcode": barcode}
            for unit_id, barcode in zip(OLD_IDS, OLD_BARCODES, strict=True)
        ],
        "old_member_count": len(OLD_IDS),
        "old_membership_hash": membership_hash(OLD_IDS),
        "old_normalized_barcodes": list(OLD_BARCODES),
        "old_barcode_membership_hash": barcode_membership_hash(OLD_BARCODES),
        "new_member_ids": list(new_ids),
        "new_members": [
            {"unit_id": "unit-keep", "normalized_barcode": "BC-KEEP"},
            {"unit_id": "unit-new", "normalized_barcode": "BC-NEW"},
        ],
        "new_member_count": len(new_ids),
        "new_membership_hash": membership_hash(new_ids),
        "new_normalized_barcodes": list(new_barcodes),
        "new_barcode_membership_hash": barcode_membership_hash(new_barcodes),
        "member_ids": list(new_ids),
        "members": [
            {"unit_id": "unit-keep", "normalized_barcode": "BC-KEEP"},
            {"unit_id": "unit-new", "normalized_barcode": "BC-NEW"},
        ],
        "member_count": len(new_ids),
        "membership_hash": membership_hash(new_ids),
        "normalized_barcodes": list(new_barcodes),
        "barcode_membership_hash": barcode_membership_hash(new_barcodes),
        "pairs": [
            {
                "old_unit_id": pair["old_unit_id"],
                "new_unit_id": pair["new_unit_id"],
                "new_source_bundle_id": pair["new_source_bundle_id"],
            }
        ],
        "pair_count": 1,
        "sources": [
            {
                "source_bundle_id": SOURCE,
                "source_version_before": 5,
                "source_version_after": 6,
                "source_member_ids_before": list(SOURCE_IDS),
                "source_members_before": [
                    {"unit_id": unit_id, "normalized_barcode": barcode}
                    for unit_id, barcode in zip(
                        SOURCE_IDS, SOURCE_BARCODES, strict=True
                    )
                ],
                "source_member_count_before": len(SOURCE_IDS),
                "source_membership_hash_before": membership_hash(SOURCE_IDS),
                "source_normalized_barcodes_before": list(SOURCE_BARCODES),
                "source_barcode_membership_hash_before": barcode_membership_hash(
                    SOURCE_BARCODES
                ),
                "selected_member_ids": ["unit-new"],
                "selected_members": [
                    {"unit_id": "unit-new", "normalized_barcode": "BC-NEW"}
                ],
                "remainder_member_ids": [],
                "remainder_members": [],
                "remainder_member_count": 0,
                "remainder_membership_hash": membership_hash(()),
                "remainder_normalized_barcodes": [],
                "remainder_barcode_membership_hash": barcode_membership_hash(()),
                "source_bundle_state_after": "CONSUMED",
            }
        ],
        "damage_bundle_id": damage_id,
        "damage_member_ids": ["unit-old"],
        "damage_members": [
            {"unit_id": "unit-old", "normalized_barcode": "BC-OLD"}
        ],
        "damage_membership_hash": membership_hash(["unit-old"]),
        "damage_location": "PROCESS_DAMAGE_HOLD",
        "movement_ids": ["move-old", "move-new"],
        "atomic": True,
        "requires_reseal": True,
        "resealed": True,
        "seal_contract_version": "transfer-seal-qr-v1",
        "seal_state": "ACTIVE",
        "seal_id": "seal-2",
        "seal_revision": 2,
        "seal_token": "token-2",
        "seal_token_hash": hashlib.sha256(b"token-2").hexdigest(),
        "seal_qr_payload": new_qr,
        "new_seal_id": "seal-2",
        "new_seal_revision": 2,
        "new_seal_token": "token-2",
        "new_seal_token_hash": hashlib.sha256(b"token-2").hexdigest(),
        "new_seal_qr_payload": new_qr,
        "sealed_bundle_id": TARGET,
        "sealed_bundle_version": 2,
        "sealed_member_ids": list(new_ids),
        "sealed_members": [
            {"unit_id": "unit-keep", "normalized_barcode": "BC-KEEP"},
            {"unit_id": "unit-new", "normalized_barcode": "BC-NEW"},
        ],
        "sealed_member_count": len(new_ids),
        "sealed_membership_hash": membership_hash(new_ids),
        "sealed_normalized_barcodes": list(new_barcodes),
        "sealed_barcode_membership_hash": barcode_membership_hash(new_barcodes),
    }
    return {
        "receipt_id": "receipt-reseal-1",
        "contract_version": "logistics-v1",
        "command_type": "REPLACE_SEALED_TRANSFER_MEMBERS",
        "status": "COMMITTED",
        "authority_scope_id": SCOPE,
        "authority_epoch": 7,
        "resolved_ledger_plane": "AUTHORITATIVE",
        "resolved_plane_epoch": 3,
        "idempotency_key": command["idempotency_key"],
        "committed_at": "2026-07-21T00:00:00Z",
        "event_ids": ["event-1"],
        "outbox_ids": ["outbox-1"],
        "entity_versions": {
            f"bundle:{TARGET}": 2,
            f"bundle:{SOURCE}": 6,
            f"bundle:{damage_id}": 1,
        },
        "data": data,
    }


class FakeClient:
    def __init__(self):
        self.commands = []

    def get_capabilities(self):
        return {
            "capability_ids": ["sealed_transfer_member_replacement_v1"],
            "capabilities": {
                "sealed_transfer_member_replacement_v1": {
                    "enabled": True,
                    "command_type": "REPLACE_SEALED_TRANSFER_MEMBERS",
                    "endpoint_template": "/logistics/api/v1/transfers/{target_bundle_id}/members/replace-and-reseal",
                    "receipt_contract_version": "sealed-transfer-member-replacement-v1",
                    "seal_qr_contract_version": "transfer-seal-qr-v1",
                    "max_pairs": 2,
                    "atomic": True,
                    "fail_closed_when_unavailable": True,
                    "disabled_server_behavior": "REJECT_COMMAND_DO_NOT_MUTATE_LOCAL_STATE",
                    "client_rollout_gate": "REQUIRE_ENABLED_CAPABILITY_AND_EXACT_RECEIPT",
                    "replacement_source_bundle_cardinality": "EXACTLY_ONE_ACTIVE_MEMBER",
                    "multi_member_source_policy": "REJECT_STALE_PHYSICAL_LABEL",
                    "multi_member_source_error_code": "REPLACEMENT_SOURCE_NOT_SINGLETON",
                }
            },
        }

    def get_bundle(self, bundle_id, *, authority_scope_id=""):
        assert (bundle_id, authority_scope_id) == (TARGET, SCOPE)
        return _target_projection()

    def resolve_good_source(self, *, authority_scope_id, barcode):
        assert (authority_scope_id, barcode) == (SCOPE, "BC-NEW")
        return _good_resolver()

    def replace_and_reseal_transfer(self, command):
        self.commands.append(command)
        return _receipt(command)


def _prepare(coordinator):
    return coordinator.prepare(
        set_id="set-1",
        old_seal_qr_payload=OLD_QR,
        old_seal_fields=_fields(),
        operator="packer",
        old_barcodes=["BC-OLD"],
        new_barcodes=["BC-NEW"],
    )


def test_atomic_replacement_command_and_receipt_are_durable(tmp_path):
    client = FakeClient()
    store = SealedTransferExchangeStore(tmp_path / "package.db")
    coordinator = SealedTransferExchangeCoordinator(store, client)

    result = coordinator.attempt(_prepare(coordinator).intent_id)

    assert result.status == "ACKED"
    assert result.new_seal_qr_payload.endswith("SID=seal-2|SREV=2|STK=token-2")
    command = client.commands[0]
    assert command["expected_versions"][f"bundle:{TARGET}"] == 1
    assert command["expected_versions"][f"bundle:{SOURCE}"] == 5
    assert command["expected_versions"][f"bundle:{result.damage_bundle_id}"] == 0
    assert command["payload"]["target_evidence"]["seal_qr_payload"] == OLD_QR
    assert store.load(result.intent_id)["receipt_json"]


def test_different_current_accounting_iin_preserves_exact_exchange_command(tmp_path):
    baseline_client = FakeClient()
    baseline = SealedTransferExchangeCoordinator(
        SealedTransferExchangeStore(tmp_path / "baseline.db"), baseline_client
    )
    assert baseline.attempt(_prepare(baseline).intent_id).status == "ACKED"

    resolved = _good_resolver()
    resolved["inbound_iin"] = "IIN-DONOR-CURRENT"
    resolved["source_bundle"]["accounting_inbound_iin"] = "IIN-DONOR-CURRENT"
    resolved["replacement_evidence"]["inbound_iin"] = "IIN-DONOR-CURRENT"
    resolved["unit"].update(
        origin_inbound_iin="IIN-DONOR-ORIGIN",
        current_inbound_iin="IIN-DONOR-CURRENT",
    )
    original_projection = deepcopy(resolved)

    class CrossAccountingClient(FakeClient):
        def resolve_good_source(self, *, authority_scope_id, barcode):
            assert (authority_scope_id, barcode) == (SCOPE, "BC-NEW")
            return resolved

    client = CrossAccountingClient()
    store = SealedTransferExchangeStore(tmp_path / "cross-accounting.db")
    coordinator = SealedTransferExchangeCoordinator(store, client)
    result = coordinator.attempt(_prepare(coordinator).intent_id)

    assert result.status == "ACKED"
    # The server derives each source's accounting IIN and performs the bound
    # movement/rebind. The client sends the same exact membership/CAS/seal
    # command, without rewriting the resolver's accounting or origin fields.
    assert client.commands == baseline_client.commands
    assert resolved == original_projection
    assert result.seal_verification_status == "PENDING"
    assert result.local_apply_status == "PENDING"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("authority_scope_id", "other-scope"),
        ("authority_epoch", 8),
        ("ledger_plane", "REHEARSAL"),
        ("plane_epoch", 4),
        ("item_id", "OTHER-ITEM"),
        ("uom", "KG"),
    ],
)
@pytest.mark.parametrize("legacy_review", [False, True])
def test_cross_accounting_replacement_keeps_real_identity_guards(
    tmp_path, field, value, legacy_review
):
    class MismatchedClient(FakeClient):
        def resolve_good_source(self, *, authority_scope_id, barcode):
            resolved = super().resolve_good_source(
                authority_scope_id=authority_scope_id, barcode=barcode
            )
            resolved["inbound_iin"] = "IIN-DONOR-CURRENT"
            if field in {"item_id", "uom"}:
                resolved["source_bundle"][field] = value
            else:
                resolved[field] = value
            return resolved

    client = MismatchedClient()
    store = SealedTransferExchangeStore(tmp_path / "mismatched.db")
    coordinator = SealedTransferExchangeCoordinator(store, client)
    prepared = _prepare(coordinator)
    if legacy_review:
        store.record_error(
            prepared.intent_id,
            PackageLogisticsError(
                "replacement good must have the same lot/item/uom/ledger identity"
            ),
        )
    result = coordinator.attempt(prepared.intent_id)

    assert result.status == "OPERATOR_REVIEW"
    assert client.commands == []
    row = store.load(result.intent_id)
    assert row["command_json"] is None and row["receipt_json"] is None
    assert row["attempt_count"] == (2 if legacy_review else 1)


def test_legacy_iin_review_recovers_original_intent_through_normal_drain(tmp_path):
    db_path = tmp_path / "legacy-iin.db"
    first = SealedTransferExchangeCoordinator(
        SealedTransferExchangeStore(db_path), FakeClient()
    )
    intent_id = _prepare(first).intent_id
    first.store.record_error(
        intent_id,
        PackageLogisticsError(
            "replacement good must have the same lot/item/uom/ledger identity"
        ),
    )
    before = dict(first.store.load(intent_id))
    calls = []

    class RecoveryClient(FakeClient):
        def get_capabilities(self):
            calls.append("capabilities")
            assert dict(first.store.load(intent_id)) == before
            return super().get_capabilities()

        def get_bundle(self, bundle_id, *, authority_scope_id=""):
            calls.append("target")
            assert dict(first.store.load(intent_id)) == before
            return super().get_bundle(bundle_id, authority_scope_id=authority_scope_id)

        def resolve_good_source(self, *, authority_scope_id, barcode):
            calls.append("source")
            assert dict(first.store.load(intent_id)) == before
            resolved = super().resolve_good_source(
                authority_scope_id=authority_scope_id, barcode=barcode
            )
            resolved["inbound_iin"] = "IIN-DONOR-CURRENT"
            return resolved

        def replace_and_reseal_transfer(self, command):
            calls.append("post")
            durable = first.store.load(intent_id)
            assert durable["status"] == "COMMAND_READY"
            assert json.loads(durable["command_json"]) == command
            assert durable["command_hash"] == hashlib.sha256(
                durable["command_json"].encode()
            ).hexdigest()
            assert durable["attempt_count"] == 1
            return super().replace_and_reseal_transfer(command)

    client = RecoveryClient()
    restarted = SealedTransferExchangeCoordinator(
        SealedTransferExchangeStore(db_path), client
    )
    recovered = restarted.drain_pending()

    assert [(row.intent_id, row.status) for row in recovered] == [(intent_id, "ACKED")]
    assert calls == ["capabilities", "target", "source", "post"]
    after = dict(restarted.store.load(intent_id))
    changed = {key for key in before if before[key] != after[key]}
    assert changed == {
        "status", "command_id", "command_json", "command_hash", "receipt_json",
        "new_seal_qr_payload", "last_error_code", "last_error_message",
        "attempt_count", "updated_at",
    }
    assert after["attempt_count"] == 2
    assert after["seal_verification_status"] == after["local_apply_status"] == "PENDING"
    with restarted.store._connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM sealed_transfer_exchange_intents").fetchone()[0] == 1
    assert restarted.drain_pending() == []
    assert len(client.commands) == 1


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("last_error_code", "HTTP_500"),
        ("last_error_code", None),
        ("last_error_message", "network failure before a command was saved"),
        ("last_error_message", "replacement good must have the same lot/item/uom/ledger identity "),
        ("receipt_json", "{}"),
        ("new_seal_qr_payload", ""),
        ("seal_verified_at", "2026-01-01T00:00:00Z"),
        ("local_apply_receipt_json", "{}"),
        ("seal_verification_status", "VERIFIED"),
        ("local_apply_status", "APPLIED"),
    ],
)
def test_legacy_iin_recovery_refuses_other_errors_and_later_stage_evidence(
    tmp_path, field, value
):
    store = SealedTransferExchangeStore(tmp_path / "blocked.db")
    coordinator = SealedTransferExchangeCoordinator(store, FakeClient())
    intent_id = _prepare(coordinator).intent_id
    store.record_error(
        intent_id,
        PackageLogisticsError(
            "replacement good must have the same lot/item/uom/ledger identity"
        ),
    )
    # Deliberately inconsistent fixture evidence must not become a recovery reset.
    with store._connect() as conn:
        conn.execute(
            f"UPDATE sealed_transfer_exchange_intents SET {field}=? WHERE intent_id=?",
            (value, intent_id),
        )
        conn.commit()
    before = dict(store.load(intent_id))

    class NoNetworkClient(FakeClient):
        def get_capabilities(self):
            raise AssertionError("unrecognized review must not start fresh validation")

    client = NoNetworkClient()
    restarted = SealedTransferExchangeCoordinator(store, client)
    assert [row.status for row in restarted.drain_pending()] == ["OPERATOR_REVIEW"]
    assert dict(store.load(intent_id)) == before
    assert client.commands == []


@pytest.mark.parametrize(
    "stale",
    ["capability", "target_seal", "target_version", "source_version", "source_membership", "singleton"],
)
def test_legacy_iin_recovery_revalidates_current_source_and_seal(tmp_path, stale):
    class StaleClient(FakeClient):
        def get_capabilities(self):
            data = super().get_capabilities()
            if stale == "capability":
                data["capability_ids"] = []
            return data

        def get_bundle(self, bundle_id, *, authority_scope_id=""):
            data = super().get_bundle(bundle_id, authority_scope_id=authority_scope_id)
            if stale == "target_seal":
                data["active_seal"]["seal_state"] = "REVOKED"
            if stale == "target_version":
                data["entity_version"] += 1
            return data

        def resolve_good_source(self, *, authority_scope_id, barcode):
            data = _good_resolver(multi_member=stale == "singleton")
            data["inbound_iin"] = "IIN-DONOR-CURRENT"
            if stale == "source_version":
                data["source_bundle"]["entity_version"] += 1
            if stale == "source_membership":
                data["source_bundle"]["membership_hash"] = "0" * 64
            return data

    store = SealedTransferExchangeStore(tmp_path / "stale.db")
    client = StaleClient()
    coordinator = SealedTransferExchangeCoordinator(store, client)
    intent_id = _prepare(coordinator).intent_id
    store.record_error(
        intent_id,
        PackageLogisticsError(
            "replacement good must have the same lot/item/uom/ledger identity"
        ),
    )
    before = dict(store.load(intent_id))
    assert [row.status for row in coordinator.drain_pending()] == ["OPERATOR_REVIEW"]
    after = dict(store.load(intent_id))
    assert after["attempt_count"] == 2
    assert after["command_json"] is None and after["receipt_json"] is None
    assert client.commands == []
    assert {key for key in before if before[key] != after[key]} <= {
        "last_error_code", "last_error_message", "attempt_count", "updated_at"
    }
    assert [row.status for row in coordinator.drain_pending()] == ["OPERATOR_REVIEW"]
    assert dict(store.load(intent_id)) == after


def test_legacy_error_with_durable_command_remains_receipt_only(tmp_path):
    store = SealedTransferExchangeStore(tmp_path / "durable-review.db")
    client = FakeClient()
    coordinator = SealedTransferExchangeCoordinator(store, client)
    intent_id = _prepare(coordinator).intent_id
    store.bind_command(intent_id, coordinator._build_command(store.load(intent_id)))
    store.record_error(
        intent_id,
        PackageLogisticsError(
            "replacement good must have the same lot/item/uom/ledger identity"
        ),
    )
    before = dict(store.load(intent_id))
    assert [row.status for row in coordinator.drain_pending()] == ["OPERATOR_REVIEW"]
    assert dict(store.load(intent_id)) == before
    assert client.commands == []


_INSTRUCTION_CONFLICT = "PHS_REPLACEMENT_INSTRUCTION_CONFLICT"
_INSTRUCTION_DETAIL = "The PHS work group differs from its completed plan instruction."


def _instruction_review(tmp_path):
    class RejectedClient(FakeClient):
        def replace_and_reseal_transfer(self, command):
            self.commands.append(command)
            raise PackageApiError(
                409, _INSTRUCTION_CONFLICT, _INSTRUCTION_DETAIL, committed=False
            )

    store = SealedTransferExchangeStore(tmp_path / "instruction-review.db")
    client = RejectedClient()
    coordinator = SealedTransferExchangeCoordinator(store, client)
    intent_id = _prepare(coordinator).intent_id
    store.record_error(
        intent_id,
        PackageLogisticsError(
            "replacement good must have the same lot/item/uom/ledger identity"
        ),
    )
    assert coordinator.attempt(intent_id).status == "OPERATOR_REVIEW"
    before = dict(store.load(intent_id))
    assert before["attempt_count"] == 2
    assert len(client.commands) == 1
    assert before["last_error_code"] == _INSTRUCTION_CONFLICT
    return store, intent_id, before


class InstructionRecoveryClient(FakeClient):
    def get_receipt(self, key, *, authority_scope_id):
        assert key.startswith("label-sealed-transfer-exchange:")
        assert authority_scope_id == SCOPE
        raise PackageApiError(
            404, "RECEIPT_NOT_FOUND", "No committed receipt exists.", committed=False
        )


def test_instruction_review_recovers_same_durable_command_without_rebinding(tmp_path, monkeypatch):
    store, intent_id, before = _instruction_review(tmp_path)
    calls = []
    saved = json.loads(before["command_json"])

    class RecoveryClient(InstructionRecoveryClient):
        def get_receipt(self, key, *, authority_scope_id):
            calls.append("receipt")
            assert key == before["command_id"]
            assert dict(store.load(intent_id)) == before
            return super().get_receipt(key, authority_scope_id=authority_scope_id)

        def get_capabilities(self):
            calls.append("capabilities")
            assert dict(store.load(intent_id)) == before
            return super().get_capabilities()

        def get_bundle(self, bundle_id, *, authority_scope_id=""):
            calls.append("target")
            return super().get_bundle(bundle_id, authority_scope_id=authority_scope_id)

        def resolve_good_source(self, *, authority_scope_id, barcode):
            calls.append("source")
            return super().resolve_good_source(authority_scope_id=authority_scope_id, barcode=barcode)

        def replace_and_reseal_transfer(self, command):
            calls.append("post")
            assert dict(store.load(intent_id)) == before
            assert command == saved
            return super().replace_and_reseal_transfer(command)

    def no_rebind(*args, **kwargs):
        raise AssertionError("the original durable command must never be rebound")

    monkeypatch.setattr(store, "bind_command", no_rebind)
    client = RecoveryClient()
    restarted = SealedTransferExchangeCoordinator(store, client)
    assert [(r.intent_id, r.status) for r in restarted.drain_pending()] == [(intent_id, "ACKED")]
    assert calls == ["receipt", "capabilities", "target", "source", "post"]
    after = dict(store.load(intent_id))
    assert {key for key in before if before[key] != after[key]} == {
        "status", "receipt_json", "new_seal_qr_payload", "last_error_code",
        "last_error_message", "attempt_count", "updated_at",
    }
    assert after["attempt_count"] == 3
    assert after["seal_verification_status"] == after["local_apply_status"] == "PENDING"
    assert restarted.drain_pending() == []
    assert client.commands == [saved]
    with store._connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM sealed_transfer_exchange_intents").fetchone()[0] == 1


@pytest.mark.parametrize("valid", [True, False])
def test_instruction_review_prefers_exact_receipt_and_never_posts_found_receipt(tmp_path, valid):
    store, intent_id, before = _instruction_review(tmp_path)
    receipt = _receipt(json.loads(before["command_json"]))
    if not valid:
        receipt["data"]["new_members"][0]["normalized_barcode"] = "WRONG"

    class ReceiptClient(InstructionRecoveryClient):
        def get_receipt(self, key, *, authority_scope_id):
            assert (key, authority_scope_id) == (before["command_id"], SCOPE)
            return receipt

        def get_capabilities(self):
            raise AssertionError("a found receipt must precede fresh source validation")

    client = ReceiptClient()
    result = SealedTransferExchangeCoordinator(store, client).drain_pending()
    assert [r.status for r in result] == ["ACKED" if valid else "OPERATOR_REVIEW"]
    assert client.commands == []
    if not valid:
        assert dict(store.load(intent_id)) == before


@pytest.mark.parametrize("lookup", [
    "generic_404", "wrong_status", "unknown_commit", "committed", "forbidden",
    "transport", "malformed", "none", "missing",
])
def test_instruction_review_requires_authoritative_receipt_absence(tmp_path, lookup):
    store, intent_id, before = _instruction_review(tmp_path)

    class UnknownClient(InstructionRecoveryClient):
        def get_receipt(self, key, *, authority_scope_id):
            failures = {
                "generic_404": PackageApiError(404, "HTTP_404", "Not found", committed=False),
                "wrong_status": PackageApiError(500, "RECEIPT_NOT_FOUND", "Unknown", committed=False),
                "unknown_commit": PackageApiError(404, "RECEIPT_NOT_FOUND", "Unknown", committed=None),
                "committed": PackageApiError(404, "RECEIPT_NOT_FOUND", "Contradiction", committed=True),
                "forbidden": PackageApiError(403, "FORBIDDEN", "Denied", committed=False),
                "transport": PackageTransportError("connection lost"),
                "malformed": ValueError("malformed receipt response"),
            }
            if lookup in failures:
                raise failures[lookup]
            return None

        def get_capabilities(self):
            raise AssertionError("unknown receipt outcome must not permit fresh validation")

    client = UnknownClient()
    if lookup == "missing":
        client.get_receipt = None
    assert [r.status for r in SealedTransferExchangeCoordinator(store, client).drain_pending()] == ["OPERATOR_REVIEW"]
    assert dict(store.load(intent_id)) == before
    assert client.commands == []


@pytest.mark.parametrize(("field", "value"), [
    ("last_error_code", "OTHER_REVIEW"),
    ("last_error_message", _INSTRUCTION_CONFLICT + ": " + _INSTRUCTION_DETAIL + " "),
    ("receipt_json", "{}"),
    ("new_seal_qr_payload", ""),
    ("seal_verified_at", "2026-01-01T00:00:00Z"),
    ("local_apply_receipt_json", "{}"),
    ("seal_verification_status", "VERIFIED"),
    ("local_apply_status", "APPLIED"),
    ("command_id", "different-key"),
    ("command_hash", "0" * 64),
    ("command_json", "noncanonical"),
])
def test_instruction_review_refuses_other_states_and_inconsistent_saved_identity(tmp_path, monkeypatch, field, value):
    store, intent_id, _ = _instruction_review(tmp_path)
    if value == "noncanonical":
        value = json.dumps(json.loads(store.load(intent_id)["command_json"]), indent=2)
    # SQL prevents editing a bound command. Inject only a corrupt read projection
    # for those fields, retaining the real schema and its immutability trigger.
    if field.startswith("command_"):
        projection = dict(store.load(intent_id))
        projection[field] = value
        monkeypatch.setattr(store, "load", lambda _intent_id: projection)
    else:
        with store._connect() as conn:
            conn.execute(f"UPDATE sealed_transfer_exchange_intents SET {field}=? WHERE intent_id=?", (value, intent_id))
            conn.commit()
    before = dict(store.load(intent_id))
    client = InstructionRecoveryClient()
    assert [r.status for r in SealedTransferExchangeCoordinator(store, client).drain_pending()] == ["OPERATOR_REVIEW"]
    assert dict(store.load(intent_id)) == before
    assert client.commands == []


@pytest.mark.parametrize("changed", ["capability", "seal", "source_version", "scope", "singleton", "transport"])
def test_instruction_review_fresh_validation_cannot_replace_saved_command(tmp_path, changed):
    store, intent_id, before = _instruction_review(tmp_path)

    class ChangedClient(InstructionRecoveryClient):
        def get_capabilities(self):
            if changed == "transport":
                raise PackageTransportError("fresh read disconnected")
            data = super().get_capabilities()
            if changed == "capability":
                data["capability_ids"] = []
            return data

        def get_bundle(self, bundle_id, *, authority_scope_id=""):
            data = super().get_bundle(bundle_id, authority_scope_id=authority_scope_id)
            if changed == "seal":
                data["active_seal"]["seal_state"] = "REVOKED"
            return data

        def resolve_good_source(self, *, authority_scope_id, barcode):
            data = _good_resolver(multi_member=changed == "singleton")
            if changed == "source_version":
                data["source_bundle"]["entity_version"] += 1
                data["source_bundle_entity_version"] += 1
                data["replacement_evidence"]["expected_source_bundle_version"] += 1
            if changed == "scope":
                data["authority_scope_id"] = "other-scope"
            return data

    client = ChangedClient()
    assert [r.status for r in SealedTransferExchangeCoordinator(store, client).drain_pending()] == ["OPERATOR_REVIEW"]
    assert dict(store.load(intent_id)) == before
    assert client.commands == []


def test_instruction_recovery_repeated_terminal_rejection_stops_automatic_exception(tmp_path):
    store, intent_id, before = _instruction_review(tmp_path)

    class StillRejectedClient(InstructionRecoveryClient):
        def replace_and_reseal_transfer(self, command):
            self.commands.append(command)
            raise PackageApiError(409, _INSTRUCTION_CONFLICT, _INSTRUCTION_DETAIL, committed=False)

        def get_receipt_if_exists(self, key, *, authority_scope_id):
            assert (key, authority_scope_id) == (before["command_id"], SCOPE)
            return None

    client = StillRejectedClient()
    coordinator = SealedTransferExchangeCoordinator(store, client)
    assert [r.status for r in coordinator.drain_pending()] == ["OPERATOR_REVIEW"]
    after = dict(store.load(intent_id))
    assert after["last_error_code"] == "SEALED_TRANSFER_EXCHANGE_RECOVERY_REJECTED"
    assert _INSTRUCTION_CONFLICT + ": " + _INSTRUCTION_DETAIL in after["last_error_message"]
    assert after["attempt_count"] == 3
    for field in ("intent_id", "intent_hash", "created_at", "command_id", "command_json", "command_hash"):
        assert after[field] == before[field]
    assert [r.status for r in SealedTransferExchangeCoordinator(store, client).drain_pending()] == ["OPERATOR_REVIEW"]
    assert dict(store.load(intent_id)) == after
    assert len(client.commands) == 1


def test_instruction_recovery_unknown_post_outcome_uses_existing_receipt_recovery(tmp_path):
    store, intent_id, before = _instruction_review(tmp_path)

    class LostAckClient(InstructionRecoveryClient):
        def replace_and_reseal_transfer(self, command):
            self.commands.append(command)
            raise PackageApiError(500, "HTTP_500", "lost ACK", committed=None)

        def get_receipt_if_exists(self, key, *, authority_scope_id):
            assert (key, authority_scope_id) == (before["command_id"], SCOPE)
            return _receipt(json.loads(before["command_json"]))

    client = LostAckClient()
    assert [r.status for r in SealedTransferExchangeCoordinator(store, client).drain_pending()] == ["RETRY_WAIT"]
    assert [r.status for r in SealedTransferExchangeCoordinator(store, client).drain_pending()] == ["ACKED"]
    assert len(client.commands) == 1


def test_new_seal_must_be_scanned_before_atomic_local_apply_and_recovers(tmp_path):
    store = SealedTransferExchangeStore(tmp_path / "package.db")
    coordinator = SealedTransferExchangeCoordinator(store, FakeClient())
    result = coordinator.attempt(_prepare(coordinator).intent_id)

    with pytest.raises(PackageLogisticsError):
        store.mark_seal_verified(result.intent_id, OLD_QR)
    store.mark_seal_verified(result.intent_id, result.new_seal_qr_payload)
    before = {
        "id": "set-1",
        "raw": [OLD_QR, "BC-OLD", "BC-KEEP"],
        "parsed": [ITEM, ITEM, ITEM],
        "sealed_transfer": {**_fields(), "_seal_qr_payload": OLD_QR},
    }
    after = _label_match_apply_sealed_exchange_state(
        before,
        old_seal_qr_payload=OLD_QR,
        new_seal_qr_payload=result.new_seal_qr_payload,
        old_barcodes=result.old_barcodes,
        new_barcodes=result.new_barcodes,
    )
    assert after["raw"][0] == result.new_seal_qr_payload
    assert after["raw"][1:] == ["BC-NEW", "BC-KEEP"]
    # Crash after state save but before local outbox ACK: applying to the
    # already-new state remains idempotent after restart.
    recovered = _label_match_apply_sealed_exchange_state(
        after,
        old_seal_qr_payload=OLD_QR,
        new_seal_qr_payload=result.new_seal_qr_payload,
        old_barcodes=result.old_barcodes,
        new_barcodes=result.new_barcodes,
    )
    assert recovered["raw"] == after["raw"]
    reopened = SealedTransferExchangeStore(tmp_path / "package.db")
    assert len(reopened.pending_local(set_id="set-1")) == 1
    reopened.mark_local_applied(
        result.intent_id, {"set_id": "set-1", "new_master_qr": result.new_seal_qr_payload}
    )
    assert reopened.pending_local(set_id="set-1") == []


def _local_apply_app(coordinator, current_set_info, save_results):
    class DataManager:
        def __init__(self):
            self.events = []

        def log_event(self, event_type, details):
            self.events.append((event_type, details))

    app = Label_Match.__new__(Label_Match)
    app.current_set_info = deepcopy(current_set_info)
    app.sealed_transfer_exchange_store = coordinator.store
    app.sealed_transfer_exchange_coordinator = coordinator
    app.data_manager = DataManager()
    app.run_tests = True
    app.saved_states = []
    app._save_results = iter(save_results)

    def save_current_set_state():
        app.saved_states.append(deepcopy(app.current_set_info))
        return next(app._save_results)

    app._save_current_set_state = save_current_set_state
    app._update_history_tree_in_progress = lambda: None
    app._update_status_label = lambda: None
    app._render_operator_workbench = lambda: None
    app.update_big_display = lambda *_args, **_kwargs: None
    return app


def test_local_save_false_keeps_receipt_pending_and_retry_never_posts_again(tmp_path):
    client = FakeClient()
    coordinator = SealedTransferExchangeCoordinator(
        SealedTransferExchangeStore(tmp_path / "local-save-retry.db"), client
    )
    result = coordinator.attempt(_prepare(coordinator).intent_id)
    coordinator.store.mark_seal_verified(
        result.intent_id, result.new_seal_qr_payload
    )
    phs2 = (
        "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-LOCAL-RETRY|CLC=ITEM-001|"
        "LBL=LBL-LOCAL-RETRY|HSH=0123456789abcdef"
    )
    before = {
        "id": "set-1",
        "raw": [phs2],
        "parsed": [ITEM],
        "central_inherit_all": True,
        "sealed_transfer": {**_fields(), "_seal_qr_payload": OLD_QR},
        "package_source_snapshot": {"bundle_id": TARGET, "entity_version": 1},
    }
    app = _local_apply_app(coordinator, before, [False, True])

    with pytest.raises(PackageLogisticsError, match="could not be saved"):
        app._apply_acked_sealed_transfer_exchange(result.intent_id)

    failed_row = coordinator.store.load(result.intent_id)
    assert failed_row["status"] == "ACKED"
    assert failed_row["seal_verification_status"] == "VERIFIED"
    assert failed_row["local_apply_status"] == "PENDING"
    assert failed_row["local_apply_receipt_json"] is None
    saved_receipt = failed_row["receipt_json"]
    assert saved_receipt
    assert app.current_set_info == before
    assert app.data_manager.events == []
    assert len(client.commands) == 1

    app._save_results = iter([True])
    assert app._reconcile_pending_sealed_transfer_exchanges() is True

    applied_row = coordinator.store.load(result.intent_id)
    assert applied_row["status"] == "ACKED"
    assert applied_row["seal_verification_status"] == "VERIFIED"
    assert applied_row["local_apply_status"] == "APPLIED"
    assert applied_row["receipt_json"] == saved_receipt
    assert applied_row["local_apply_receipt_json"]
    assert len(app.data_manager.events) == 1
    assert len(client.commands) == 1
    event, details = app.data_manager.events[0]
    assert event == app.Events.SEALED_TRANSFER_EXCHANGE_APPLIED
    assert details == {
        "set_id": result.set_id,
        "intent_id": result.intent_id,
        "receipt_id": result.receipt_id,
        "target_bundle_id": result.target_bundle_id,
        "damage_bundle_id": result.damage_bundle_id,
        "old_barcodes": list(result.old_barcodes),
        "new_barcodes": list(result.new_barcodes),
        "entity_versions": dict(result.entity_versions),
        "atomic_local_apply": True,
    }
    assert app.current_set_info["raw"] == [phs2]
    assert app.current_set_info["sealed_transfer"]["_seal_qr_payload"] == result.new_seal_qr_payload
    for field in ("old_seal_qr_payload", "new_seal_qr_payload"):
        assert applied_row[field] == failed_row[field]


def test_local_save_and_rollback_false_fail_closed_for_operator_review(tmp_path):
    client = FakeClient()
    coordinator = SealedTransferExchangeCoordinator(
        SealedTransferExchangeStore(tmp_path / "local-save-review.db"), client
    )
    result = coordinator.attempt(_prepare(coordinator).intent_id)
    coordinator.store.mark_seal_verified(
        result.intent_id, result.new_seal_qr_payload
    )
    before = {
        "id": "set-1",
        "raw": [OLD_QR, "BC-OLD", "BC-KEEP"],
        "parsed": [ITEM, ITEM, ITEM],
        "sealed_transfer": {**_fields(), "_seal_qr_payload": OLD_QR},
    }
    app = _local_apply_app(coordinator, before, [False, False])

    with pytest.raises(PackageLogisticsError, match="could not be saved"):
        app._apply_acked_sealed_transfer_exchange(result.intent_id)

    review_row = coordinator.store.load(result.intent_id)
    assert review_row["status"] == "ACKED"
    assert review_row["seal_verification_status"] == "VERIFIED"
    assert review_row["local_apply_status"] == "OPERATOR_REVIEW"
    assert review_row["local_apply_receipt_json"] is None
    assert review_row["last_error_code"] == "LOCAL_APPLY_CONFLICT"
    assert app.current_set_info == before
    assert app.data_manager.events == []
    assert len(client.commands) == 1


def test_reseal_keeps_original_phs2_physical_identity_and_invalidates_snapshot(tmp_path):
    coordinator = SealedTransferExchangeCoordinator(
        SealedTransferExchangeStore(tmp_path / "package.db"), FakeClient()
    )
    result = coordinator.attempt(_prepare(coordinator).intent_id)
    phs2 = (
        "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-PACK|CLC=ITEM-001|"
        "LBL=LBL-PACK|HSH=0123456789abcdef"
    )
    before = {
        "id": "phs2-set",
        "raw": [phs2],
        "parsed": [ITEM],
        "central_inherit_all": True,
        "sealed_transfer": {**_fields(), "_seal_qr_payload": OLD_QR},
        "package_source_snapshot": {"bundle_id": TARGET, "entity_version": 1},
    }

    after = _label_match_apply_sealed_exchange_state(
        before,
        old_seal_qr_payload=OLD_QR,
        new_seal_qr_payload=result.new_seal_qr_payload,
        old_barcodes=result.old_barcodes,
        new_barcodes=result.new_barcodes,
    )

    assert after["raw"] == [phs2]
    assert after["sealed_transfer"]["_seal_qr_payload"] == result.new_seal_qr_payload
    assert after["package_source_snapshot"] is None
    assert after["resolved_transfer_bundle_id"] == ""

    recovered = _label_match_apply_sealed_exchange_state(
        after,
        old_seal_qr_payload=OLD_QR,
        new_seal_qr_payload=result.new_seal_qr_payload,
        old_barcodes=result.old_barcodes,
        new_barcodes=result.new_barcodes,
    )
    assert recovered["raw"] == [phs2]


def test_package_projection_rejects_rotated_old_seal_even_if_membership_is_same():
    projection = _target_projection(
        qr=_qr(revision=2, seal_id="seal-2", token="token-2"),
        seal_id="seal-2",
        token="token-2",
        revision=2,
    )
    draft = PackageCommandDraft.build(
        set_id="set-1",
        item_code=ITEM,
        source_bundle_id=TARGET,
        source_authority_scope_id=SCOPE,
        expected_member_count=len(OLD_IDS),
        expected_membership_hash=membership_hash(OLD_IDS),
        expected_authority_epoch=7,
        expected_ledger_plane="AUTHORITATIVE",
        expected_plane_epoch=3,
        expected_seal_id="seal-1",
        expected_seal_revision=1,
        expected_seal_token="token-1",
        expected_seal_qr_payload=OLD_QR,
        external_label="PACKAGE-LABEL",
        sample_barcodes=(),
    )
    with pytest.raises(PackageLogisticsError, match="stale"):
        PackageLogisticsClient._validate_projection(
            projection, draft, expected_scope=SCOPE
        )


def test_orphan_package_command_recovers_the_original_physical_phs2_slot():
    draft = PackageCommandDraft.build(
        set_id="set-orphan",
        item_code=ITEM,
        source_bundle_id=TARGET,
        source_input_tag_id="ITG-ORPHAN",
        source_input_tag_label_id="LBL-ORPHAN",
        source_input_tag_hash_prefix="0123456789abcdef",
        source_authority_scope_id=SCOPE,
        expected_member_count=len(OLD_IDS),
        expected_membership_hash=membership_hash(OLD_IDS),
        expected_authority_epoch=7,
        expected_ledger_plane="AUTHORITATIVE",
        expected_plane_epoch=3,
        expected_seal_id="seal-1",
        expected_seal_revision=1,
        expected_seal_token="token-1",
        expected_seal_qr_payload=OLD_QR,
        external_label="PKG-PHS2-ORPHAN",
        sample_barcodes=(),
    )
    state = _label_match_recover_central_state_from_package_row(
        {
            "set_id": draft.set_id,
            "idempotency_key": "package-orphan-key",
            "status": "ACKED",
            "created_at": "2026-07-22T10:00:00+00:00",
            "draft_json": json.dumps(draft.to_dict()),
        }
    )

    assert state["raw"] == [
        "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-ORPHAN|CLC=ITEM-001|"
        "LBL=LBL-ORPHAN|HSH=0123456789abcdef"
    ]
    assert state["parsed"] == [ITEM]
    assert state["central_inherit_all"] is True
    assert state["resolved_transfer_bundle_id"] == TARGET
    assert state["sealed_transfer"]["_seal_qr_payload"] == OLD_QR
    assert state["package_submission_status"] == "ACKED"


def test_more_than_two_pairs_are_rejected_before_network(tmp_path):
    client = FakeClient()
    coordinator = SealedTransferExchangeCoordinator(
        SealedTransferExchangeStore(tmp_path / "package.db"), client
    )
    with pytest.raises(PackageLogisticsError, match="one or two"):
        coordinator.prepare(
            set_id="set-1",
            old_seal_qr_payload=OLD_QR,
            old_seal_fields=_fields(),
            operator="packer",
            old_barcodes=["OLD-1", "OLD-2", "OLD-3"],
            new_barcodes=["NEW-1", "NEW-2", "NEW-3"],
        )
    assert client.commands == []


def test_receipt_unit_barcode_mapping_mismatch_requires_operator_review(tmp_path):
    class BadReceiptClient(FakeClient):
        def replace_and_reseal_transfer(self, command):
            self.commands.append(command)
            receipt = _receipt(command)
            receipt["data"]["new_members"][0]["normalized_barcode"] = "WRONG"
            return receipt

    store = SealedTransferExchangeStore(tmp_path / "package.db")
    coordinator = SealedTransferExchangeCoordinator(store, BadReceiptClient())
    result = coordinator.attempt(_prepare(coordinator).intent_id)

    assert result.status == "OPERATOR_REVIEW"
    assert result.error_code == "SEALED_TRANSFER_EXCHANGE_ERROR"
    assert [row["intent_id"] for row in store.blocking_rows(set_id="set-1")] == [
        result.intent_id
    ]


def test_unknown_non_json_500_recovers_receipt_without_duplicate_post():
    posts = []
    receipt_gets = []
    receipt = {"receipt_id": "receipt-after-lost-500"}

    def transport(method, url, _headers, body, _timeout):
        if method == "POST":
            posts.append(json.loads(body.decode("utf-8")))
            raise PackageApiError(
                500,
                "HTTP_500",
                "package API rejected the request",
                committed=None,
            )
        if method == "GET" and "/receipts/" in url:
            receipt_gets.append(url)
            return {"ok": True, "data": receipt}
        raise AssertionError((method, url))

    client = PackageLogisticsClient(
        PackageClientConfig(
            "https://logistics.test", "token", SCOPE, "host", "device"
        ),
        transport=transport,
    )
    command = {
        "idempotency_key": "lost-non-json-500",
        "authority_scope_id": SCOPE,
        "payload": {"target_bundle_id": TARGET},
    }

    assert client.replace_and_reseal_transfer(command) == receipt
    assert posts == [command]
    assert len(receipt_gets) == 1


def test_restart_recovers_exact_receipt_before_posting_saved_command(tmp_path):
    class LostAckClient(FakeClient):
        def replace_and_reseal_transfer(self, command):
            self.commands.append(command)
            raise PackageApiError(
                500,
                "HTTP_500",
                "non-JSON response after commit",
                committed=None,
            )

    db_path = tmp_path / "restart.db"
    first_client = LostAckClient()
    first = SealedTransferExchangeCoordinator(
        SealedTransferExchangeStore(db_path), first_client
    )
    retry = first.attempt(_prepare(first).intent_id)

    assert retry.status == "RETRY_WAIT"
    assert len(first_client.commands) == 1
    saved_command = json.loads(first.store.load(retry.intent_id)["command_json"])

    class ReceiptOnlyClient:
        def __init__(self):
            self.receipt_gets = 0

        def get_receipt_if_exists(self, key, *, authority_scope_id):
            self.receipt_gets += 1
            assert key == saved_command["idempotency_key"]
            assert authority_scope_id == SCOPE
            return _receipt(saved_command)

        def replace_and_reseal_transfer(self, _command):
            raise AssertionError("restart receipt recovery must not POST again")

    recovery_client = ReceiptOnlyClient()
    restarted = SealedTransferExchangeCoordinator(
        SealedTransferExchangeStore(db_path), recovery_client
    )
    recovered = restarted.drain_pending()

    assert [attempt.status for attempt in recovered] == ["ACKED"]
    assert recovery_client.receipt_gets == 1


def test_invalid_receipt_stays_review_blocked_across_restart_without_repost(tmp_path):
    class InvalidReceiptClient(FakeClient):
        def replace_and_reseal_transfer(self, command):
            self.commands.append(command)
            receipt = _receipt(command)
            receipt["data"]["new_members"][0]["normalized_barcode"] = "WRONG"
            return receipt

    db_path = tmp_path / "invalid-receipt.db"
    first = SealedTransferExchangeCoordinator(
        SealedTransferExchangeStore(db_path), InvalidReceiptClient()
    )
    review = first.attempt(_prepare(first).intent_id)
    saved_command = json.loads(first.store.load(review.intent_id)["command_json"])
    invalid_receipt = _receipt(saved_command)
    invalid_receipt["data"]["new_members"][0]["normalized_barcode"] = "WRONG"

    class InvalidReceiptOnlyClient:
        def get_receipt_if_exists(self, _key, *, authority_scope_id):
            assert authority_scope_id == SCOPE
            return invalid_receipt

        def replace_and_reseal_transfer(self, _command):
            raise AssertionError("operator review must never repost")

    restarted = SealedTransferExchangeCoordinator(
        SealedTransferExchangeStore(db_path), InvalidReceiptOnlyClient()
    )
    retried = restarted.drain_pending()

    assert [attempt.status for attempt in retried] == ["OPERATOR_REVIEW"]
    assert restarted.store.blocking_rows(set_id="set-1")
    app = Label_Match.__new__(Label_Match)
    app.current_set_info = {"id": "set-1"}
    app.sealed_transfer_exchange_store = restarted.store
    app.sealed_transfer_exchange_coordinator = restarted
    app.run_tests = True
    assert app._sealed_transfer_exchange_blocks_local_action("다음 스캔") is True


def test_multi_member_donor_is_rejected_before_reseal_command(tmp_path):
    class MultiMemberDonorClient(FakeClient):
        def resolve_good_source(self, *, authority_scope_id, barcode):
            assert (authority_scope_id, barcode) == (SCOPE, "BC-NEW")
            return _good_resolver(multi_member=True)

    client = MultiMemberDonorClient()
    coordinator = SealedTransferExchangeCoordinator(
        SealedTransferExchangeStore(tmp_path / "package.db"), client
    )

    result = coordinator.attempt(_prepare(coordinator).intent_id)

    assert result.status == "OPERATOR_REVIEW"
    assert result.error_code == "REPLACEMENT_SOURCE_NOT_SINGLETON"
    assert client.commands == []

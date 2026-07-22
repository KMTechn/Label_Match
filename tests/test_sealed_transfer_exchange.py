import hashlib
import json

import pytest

from package_logistics import (
    PackageApiError,
    PackageClientConfig,
    PackageCommandDraft,
    PackageLogisticsClient,
    PackageLogisticsError,
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

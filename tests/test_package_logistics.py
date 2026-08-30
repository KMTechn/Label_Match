from __future__ import annotations

import base64
import copy
import csv
import io
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlsplit

import pytest
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import (
    decode_dss_signature,
)

import Label_Match as label_module
import package_logistics as package_module
import terminal_operation_lease as lease_module
from package_logistics import (
    PackageClientConfig,
    PackageApiError,
    PackageCancellationIntent,
    PackageCancellationOutbox,
    PackageCancellationOutboxProcessor,
    PackageCommandDraft,
    PackageLogisticsClient,
    PackageLogisticsError,
    PackageOutbox,
    PackageOutboxProcessor,
    PackageTransportError,
    barcode_membership_hash,
    membership_hash,
)


SCOPE = "SCOPE-PACKAGE-1"
TRANSFER = "TRANSFER-SEALED-1"
UNITS = ("unit-a", "unit-b", "unit-c", "unit-d")
BARCODES = ("ITEM000000001-A", "ITEM000000001-B", "ITEM000000001-C", "ITEM000000001-D")
MEMBERSHIP_HASH = membership_hash(UNITS)
P256_ORDER = int(
    "FFFFFFFF00000000FFFFFFFFFFFFFFFFBCE6FAADA7179E84F3B9CAC2FC632551",
    16,
)


class _OperationLeaseTestSigner:
    def __init__(self):
        self.kid = "label-match-test-key"
        self.key = ec.generate_private_key(ec.SECP256R1())

    @property
    def jwk(self):
        numbers = self.key.public_key().public_numbers()
        encode = lambda value: base64.urlsafe_b64encode(value).rstrip(
            b"="
        ).decode("ascii")
        return {
            "kty": "EC",
            "crv": "P-256",
            "x": encode(numbers.x.to_bytes(32, "big")),
            "y": encode(numbers.y.to_bytes(32, "big")),
        }

    def sign(self, claims):
        encode = lambda value: base64.urlsafe_b64encode(value).rstrip(
            b"="
        ).decode("ascii")
        header = {
            "alg": "ES256",
            "kid": self.kid,
            "typ": "terminal-operation-lease+jws",
        }
        head = encode(lease_module.canonical_json_bytes(header))
        body = encode(lease_module.canonical_json_bytes(claims))
        signing_input = f"{head}.{body}".encode("ascii")
        r, s = decode_dss_signature(
            self.key.sign(signing_input, ec.ECDSA(hashes.SHA256()))
        )
        if s > P256_ORDER // 2:
            s = P256_ORDER - s
        signature = r.to_bytes(32, "big") + s.to_bytes(32, "big")
        return f"{head}.{body}.{encode(signature)}"

    def artifact(self, *, binding, operation_snapshot):
        now = datetime.now(timezone.utc).replace(microsecond=0)
        claims = {
            "contract_version": lease_module.LEASE_CONTRACT_VERSION,
            "lease_id": "LEASE-APP-001",
            "site_id": "site-main",
            **binding,
            "issued_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "expires_at": (now + timedelta(hours=1)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            "fence": 11,
            "snapshot_hash": lease_module.canonical_sha256(
                operation_snapshot
            ),
        }
        token = self.sign(claims)
        public_jwk = self.jwk
        return {
            "contract_version": lease_module.ARTIFACT_CONTRACT_VERSION,
            "lease_id": claims["lease_id"],
            "status": "ACTIVE",
            "replayed": False,
            "token": token,
            "kid": self.kid,
            "expires_at": claims["expires_at"],
            "fence": claims["fence"],
            "snapshot_hash": claims["snapshot_hash"],
            "operation_snapshot": operation_snapshot,
            "keyring": {
                "contract_version": lease_module.KEYRING_CONTRACT_VERSION,
                "site_id": claims["site_id"],
                "current_kid": self.kid,
                "keys": [
                    {
                        "kid": self.kid,
                        "status": "current",
                        "public_jwk": public_jwk,
                        "thumbprint": lease_module.jwk_thumbprint(public_jwk),
                    }
                ],
            },
        }


def _source_evidence():
    return {
        "member_ids": list(UNITS),
        "membership_hash": MEMBERSHIP_HASH,
        "barcode_membership_hash": barcode_membership_hash(BARCODES),
    }


def _qr(*, plane="AUTHORITATIVE", count=4, digest=MEMBERSHIP_HASH):
    return (
        f"TRF=1|BND={TRANSFER}|AUTH_SCOPE={SCOPE}|CLC=ITEM000000001|QT={count}|"
        f"HSH={digest}|EPOCH=5|PLANE={plane}|PE=3"
    )


def _draft(*, mode="INHERIT_ALL", exact=()):
    return PackageCommandDraft.build(
        set_id="SET-1",
        item_code="ITEM000000001",
        source_bundle_id=TRANSFER,
        source_external_label=_qr(),
        source_authority_scope_id=SCOPE,
        expected_member_count=4,
        expected_membership_hash=MEMBERSHIP_HASH,
        expected_authority_epoch=5,
        expected_ledger_plane="AUTHORITATIVE",
        expected_plane_epoch=3,
        external_label="FINAL-ITEM000000001-LABEL",
        membership_mode=mode,
        sample_barcodes=BARCODES[:3],
        exact_rescan_barcodes=exact,
    )


def _draft_for_set(set_id):
    draft = _draft()
    return replace(
        draft,
        set_id=set_id,
        package_bundle_id=package_module.stable_id(
            "PACKAGE", TRANSFER, set_id, draft.external_label
        ),
    )


def _projection():
    return {
        "authority_scope_id": SCOPE,
        "authority_epoch": 5,
        "ledger_plane": "AUTHORITATIVE",
        "plane_epoch": 3,
        "bundle_id": TRANSFER,
        "bundle_type": "TRANSFER",
        "bundle_state": "AVAILABLE",
        "current_location": "TRANSFER",
        "item_id": "ITEM000000001",
        "entity_version": 7,
        "member_ids": list(UNITS),
        "member_count": 4,
        "membership_hash": MEMBERSHIP_HASH,
        "barcode_member_count": len(BARCODES),
        "barcode_membership_hash": barcode_membership_hash(BARCODES),
        "members": [
            {"unit_id": unit_id, "normalized_barcode": barcode}
            for unit_id, barcode in zip(UNITS, BARCODES, strict=True)
        ],
    }


def _resolved_projection():
    return {
        "candidate_count": 1,
        "bundle": {**_projection(), "bundle_role": "PACKAGE_SOURCE"},
    }


def _receipt(draft):
    return {
        "contract_version": "logistics-v1",
        "receipt_id": "receipt-package",
        "command_type": "CREATE_PACKAGE",
        "status": "COMMITTED",
        "authority_scope_id": SCOPE,
        "authority_epoch": 5,
        "resolved_ledger_plane": "AUTHORITATIVE",
        "resolved_plane_epoch": 3,
        "committed_at": "2026-07-15T00:00:00Z",
        "event_ids": ["event-package-create"],
        "outbox_ids": ["outbox-package-create"],
        "entity_versions": {f"bundle:{TRANSFER}": 8, f"bundle:{draft.package_bundle_id}": 1},
        "data": {
            "source_bundle_id": TRANSFER,
            "source_bundle_type": "TRANSFER",
            "package_bundle_id": draft.package_bundle_id,
            "membership_mode": draft.membership_mode,
            "member_ids": list(UNITS),
            "member_count": 4,
            "membership_hash": MEMBERSHIP_HASH,
            "source_evidence": _source_evidence(),
            "exact_rescan_barcodes": list(draft.exact_rescan_barcodes),
            "exact_rescan_count": len(draft.exact_rescan_barcodes),
            "barcode_membership_hash": (
                barcode_membership_hash(draft.exact_rescan_barcodes)
                if draft.exact_rescan_barcodes
                else None
            ),
        },
    }


def _ack_package_creation(outbox, draft):
    row = outbox.enqueue(draft)
    outbox.mark_local_completion_committed(row["idempotency_key"])
    claimed = outbox.claim_next()
    assert claimed["idempotency_key"] == row["idempotency_key"]
    command = {
        "contract_version": "logistics-v1",
        "command_type": "CREATE_PACKAGE",
        "authority_scope_id": SCOPE,
        "authority_epoch": 5,
        "ledger_plane": "AUTHORITATIVE",
        "plane_epoch": 3,
        "idempotency_key": row["idempotency_key"],
        "expected_versions": {f"bundle:{TRANSFER}": 7},
        "payload": {
            "source_bundle_id": TRANSFER,
            "package_bundle_id": draft.package_bundle_id,
            "source_evidence": _source_evidence(),
        },
    }
    outbox.save_command(row["idempotency_key"], TRANSFER, command)
    outbox.mark_acked(row["idempotency_key"], _receipt(draft))
    return outbox.get_by_set_id(draft.set_id)


def _cancellation_intent(draft, *, event_type="TRAY_COMPLETION_CANCELLED"):
    return PackageCancellationIntent.build(
        set_id=draft.set_id,
        event_type=event_type,
        reason=(
            "LOCAL_TRAY_COMPLETION_CANCELLED"
            if event_type == "TRAY_COMPLETION_CANCELLED"
            else "LOCAL_COMPLETED_SET_DELETED"
        ),
        evidence={"operator_action": "test-cancel"},
    )


def _cancellation_receipt(draft, *, intent=None, expected_version=1):
    intent = intent or _cancellation_intent(draft)
    package_version = expected_version + 1
    create_key = f"label-package-{package_module.stable_id('cmd', draft.set_id, draft.package_bundle_id)}"
    cancellation_evidence = {
        **dict(intent.evidence),
        "cancellation_event_id": intent.cancellation_event_id,
        "event_type": intent.event_type,
        "set_id": intent.set_id,
        "create_package_idempotency_key": create_key,
    }
    return {
        "contract_version": "logistics-v1",
        "receipt_id": "receipt-package-cancel",
        "command_type": "CANCEL_PACKAGE",
        "status": "COMMITTED",
        "authority_scope_id": SCOPE,
        "authority_epoch": 5,
        "resolved_ledger_plane": "AUTHORITATIVE",
        "resolved_plane_epoch": 3,
        "entity_versions": {f"bundle:{draft.package_bundle_id}": package_version},
        "event_ids": ["event-package-cancel"],
        "outbox_ids": ["outbox-package-cancel"],
        "committed_at": "2026-07-15T00:00:00Z",
        "data": {
            "package_bundle_id": draft.package_bundle_id,
            "package_state": "CANCELLED",
            "bundle_state": "AVAILABLE",
            "invalidated": True,
            "current_location": "SHIPPING-WAIT",
            "member_ids": list(UNITS),
            "member_count": len(UNITS),
            "membership_hash": MEMBERSHIP_HASH,
            "package_entity_version": package_version,
            "reason": intent.reason,
            "evidence": cancellation_evidence,
        },
    }


def test_sealed_transfer_qr_contract_and_plane_are_strict():
    parsed = label_module._label_match_parse_sealed_transfer_qr(_qr())
    assert parsed["BND"] == TRANSFER
    assert parsed["QT"] == 4
    assert parsed["PLANE"] == "AUTHORITATIVE"
    with pytest.raises(ValueError, match="ledger plane"):
        label_module._label_match_parse_sealed_transfer_qr(_qr(plane="LIVE"))
    with pytest.raises(ValueError, match="SHA-256"):
        label_module._label_match_parse_sealed_transfer_qr(_qr(digest="bad"))


def test_five_scan_draft_keeps_three_samples_out_of_exact_membership(monkeypatch):
    monkeypatch.setenv(label_module.LABEL_MATCH_LOGISTICS_MEMBERSHIP_MODE_ENV, "INHERIT_ALL")
    current = {
        "id": "SET-SEALED-1",
        "raw": [_qr(), *BARCODES[:3], "FINAL-ITEM000000001-LABEL-LONG-1234567890"],
    }
    draft = label_module._label_match_package_draft(current, item_code="ITEM000000001")
    assert draft.membership_mode == "INHERIT_ALL"
    assert draft.sample_barcodes == BARCODES[:3]
    assert draft.exact_rescan_barcodes == ()
    assert draft.expected_member_count == 4
    assert draft.expected_membership_hash == MEMBERSHIP_HASH


@pytest.mark.parametrize(
    "master",
    [
        _qr(),
        (
            "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-CENTRAL-2SCAN|"
            "CLC=ITEM000000001|LBL=LBL-CENTRAL-2SCAN|HSH=0123456789abcdef"
        ),
    ],
)
def test_central_one_phs2_scan_draft_inherits_all_without_product_samples(master):
    current = {
        "id": "SET-CENTRAL-ONE-SCAN",
        "raw": [master],
        "central_inherit_all": True,
    }

    draft = label_module._label_match_package_draft(
        current,
        item_code="ITEM000000001",
    )

    assert draft.membership_mode == "INHERIT_ALL"
    assert draft.sample_barcodes == ()
    assert draft.exact_rescan_barcodes == ()
    assert draft.external_label.startswith("PKG-PHS2-")
    assert draft.external_label != master
    assert draft.external_label == label_module._label_match_system_package_external_label(
        master, "SET-CENTRAL-ONE-SCAN"
    )


def _replacement_waiting_payload(*, marked_at="2026-08-01T10:00:00"):
    dedupe_key = "phs-replacement-waiting-test-key"
    return {
        "intent_version": "phs-replacement-waiting-v1",
        "intent_id": dedupe_key,
        "dedupe_key": dedupe_key,
        "set_id": "SET-REPLACEMENT-1",
        "session_id": "ITG-REPLACEMENT-1",
        "old_label_id": "LBL-OLD-1",
        "new_label_id": "LBL-NEW-1",
        "process": "PACKAGING",
        "location": "PACKAGING",
        "current_process": "PACKAGING",
        "current_location": "PACKAGING",
        "source_system": "label_match",
        "source_pc_id": "PACK-PC-1",
        "marked_at": marked_at,
    }
    if master.startswith("TRF=1"):
        assert draft.source_bundle_id == TRANSFER
        assert draft.expected_member_count == 4
        assert draft.expected_membership_hash == MEMBERSHIP_HASH
    else:
        assert draft.source_input_tag_id == "ITG-CENTRAL-2SCAN"
        assert draft.source_input_tag_label_id == "LBL-CENTRAL-2SCAN"
        assert draft.source_input_tag_hash_prefix == "0123456789abcdef"


def test_central_one_scan_workflow_waits_for_explicit_package_complete():
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app.package_logistics_client = object()
    app.current_set_info = {
        "raw": [_qr()],
        "parsed": ["ITEM000000001"],
        "central_inherit_all": True,
    }
    app.history_view_updates_active_state = True

    assert app._workflow_total_scan_count() == 1
    assert app._workflow_final_label_position() == 1
    assert app._next_action_text(1) == "랩핑 후 F3 포장 완료"
    assert label_module._label_match_manual_complete_block_reason(app.current_set_info) is None


@pytest.mark.parametrize(
    "invalid",
    [
        "PHS=1|SRC=KMTECH_INPUT_TAG|ITG=ITG-1|CLC=ITEM000000001|LBL=LBL-1|HSH=0123456789abcdef",
        "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-1|CLC=ITEM000000001|HSH=0123456789abcdef",
        "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-1|CLC=ITEM000000001|LBL=LBL-1|HSH=0123456789abcdef|QT=4",
        "SRC=KMTECH_INPUT_TAG|PHS=2|ITG=ITG-1|CLC=ITEM000000001|LBL=LBL-1|HSH=0123456789abcdef",
        "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-1|CLC=ITEM000000001|LBL=LBL-1|HSH=not-hex-prefix",
    ],
)
def test_compact_phs2_contract_rejects_phase1_partial_extra_or_noncanonical(invalid):
    with pytest.raises(ValueError):
        label_module._label_match_parse_compact_phs2(invalid)
    assert label_module._label_match_has_central_source_identity(invalid) is False


def test_phs2_create_draft_requires_and_binds_resolved_transfer_snapshot():
    master = (
        "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-SNAPSHOT|CLC=ITEM000000001|"
        "LBL=LBL-SNAPSHOT|HSH=0123456789abcdef"
    )
    current = {
        "id": "SET-SNAPSHOT",
        "raw": [master],
        "central_inherit_all": True,
    }
    with pytest.raises(PackageLogisticsError, match="preflight"):
        label_module._label_match_package_draft(
            current,
            item_code="ITEM000000001",
            require_source_snapshot=True,
        )

    current["package_source_snapshot"] = {
        "bundle_id": TRANSFER,
        "authority_scope_id": SCOPE,
        "member_count": 4,
        "membership_hash": MEMBERSHIP_HASH,
        "authority_epoch": 5,
        "ledger_plane": "AUTHORITATIVE",
        "plane_epoch": 3,
        "entity_version": 7,
    }
    draft = label_module._label_match_package_draft(
        current,
        item_code="ITEM000000001",
        require_source_snapshot=True,
    )

    assert draft.source_bundle_id == TRANSFER
    assert draft.source_authority_scope_id == SCOPE
    assert draft.expected_member_count == 4
    assert draft.expected_membership_hash == MEMBERSHIP_HASH
    assert draft.expected_authority_epoch == 5
    assert draft.expected_ledger_plane == "AUTHORITATIVE"
    assert draft.expected_plane_epoch == 3


def test_legacy_inherit_is_blocked_and_full_exact_rescan_is_separate(monkeypatch):
    current = {
        "id": "SET-LEGACY",
        "raw": ["ITEM000000001", *BARCODES[:3], "FINAL-ITEM000000001-LABEL-LONG-1234567890"],
        "exact_rescan_complete": True,
        "exact_rescan_target_count": 4,
        "exact_rescan_source_bundle_id": TRANSFER,
        "exact_rescan_barcodes": list(BARCODES),
    }
    monkeypatch.setenv(label_module.LABEL_MATCH_LOGISTICS_MEMBERSHIP_MODE_ENV, "INHERIT_ALL")
    with pytest.raises(PackageLogisticsError, match="FULL EXACT_RESCAN"):
        label_module._label_match_package_draft(
            {**current, "exact_rescan_complete": False, "exact_rescan_barcodes": []},
            item_code="ITEM000000001",
        )
    monkeypatch.delenv(label_module.LABEL_MATCH_LOGISTICS_MEMBERSHIP_MODE_ENV, raising=False)
    draft = label_module._label_match_package_draft(current, item_code="ITEM000000001")
    assert draft.sample_barcodes == BARCODES[:3]
    assert draft.exact_rescan_barcodes == BARCODES


def test_legacy_minimal_input_tag_qr_uses_itg_without_raw_qr_as_external_identity():
    master = "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-MINIMAL-1|CLC=ITEM000000001"
    current = {
        "id": "SET-ITG-ONLY",
        "raw": [master, *BARCODES[:3], "FINAL-LABEL"],
        "exact_rescan_complete": True,
        "exact_rescan_target_count": 4,
        "exact_rescan_barcodes": list(BARCODES),
    }
    draft = label_module._label_match_package_draft(current, item_code="ITEM000000001")
    assert draft.source_input_tag_id == "ITG-MINIMAL-1"
    assert draft.source_external_label == ""
    assert draft.source_bundle_hint == ""
    assert draft.source_external_label != master


def test_structured_phs2_itg_defaults_to_server_inherited_membership():
    master = (
        "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-PACKAGE-1|CLC=ITEM000000001|"
        "LBL=LBL-PACKAGE-1|HSH=fedcba9876543210"
    )
    current = {
        "id": "SET-PHS-INHERIT",
        "raw": [master, *BARCODES[:3], "FINAL-LABEL"],
    }

    draft = label_module._label_match_package_draft(
        current, item_code="ITEM000000001"
    )

    assert draft.membership_mode == "INHERIT_ALL"
    assert draft.source_bundle_id == ""
    assert draft.source_input_tag_id == "ITG-PACKAGE-1"
    assert draft.source_input_tag_label_id == "LBL-PACKAGE-1"
    assert draft.source_input_tag_hash_prefix == "fedcba9876543210"
    assert draft.source_external_label == ""
    assert draft.exact_rescan_barcodes == ()
    assert draft.sample_barcodes == BARCODES[:3]


def test_input_tag_qr_does_not_promote_compat_wid_to_resolver_identity():
    decoded = (
        "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-WID-1|CLC=ITEM000000001|"
        "WID=PHS-EXTERNAL-WID-1"
    )
    master = base64.urlsafe_b64encode(decoded.encode("utf-8")).decode("ascii")
    current = {
        "id": "SET-WID",
        "raw": [master, *BARCODES[:3], "FINAL-LABEL"],
        "exact_rescan_complete": True,
        "exact_rescan_target_count": 4,
        "exact_rescan_barcodes": list(BARCODES),
    }
    draft = label_module._label_match_package_draft(current, item_code="ITEM000000001")
    assert draft.source_input_tag_id == "ITG-WID-1"
    assert draft.source_external_label == ""
    assert draft.source_external_label != master


def test_external_label_without_structured_lineage_is_rejected():
    with pytest.raises(PackageLogisticsError, match="BND/ITG"):
        PackageCommandDraft.build(
            set_id="SET-AMBIGUOUS-LABEL",
            item_code="ITEM000000001",
            source_external_label="PRINTED-LABEL-ONLY",
            external_label="FINAL-LABEL",
            membership_mode="INHERIT_ALL",
            sample_barcodes=BARCODES[:3],
        )


def test_exact_rescan_operational_input_is_durably_recoverable(tmp_path):
    manager = label_module.DataManager(str(tmp_path), "포장실", "tester", "PC")
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app.run_tests = True
    app.initialized_successfully = True
    app.current_set_info = {
        "id": "SET-RECOVER",
        "raw": ["ITEM000000001"],
        "parsed": ["ITEM000000001"],
        "exact_rescan_target_count": 2,
        "exact_rescan_source_bundle_id": TRANSFER,
        "exact_rescan_barcodes": [],
    }
    app.data_manager = manager
    app.update_big_display = lambda *args: None
    app._update_status_label = lambda: None
    app._handle_input_error = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError(kwargs))
    assert label_module.Label_Match._prompt_exact_rescan(app)
    assert label_module.Label_Match._process_exact_rescan_product(app, "ITEM000000001-A")
    saved = manager.load_current_state()["current_set_info"]
    assert saved["exact_rescan_active"] is True
    assert saved["exact_rescan_barcodes"] == ["ITEM000000001-A"]
    assert label_module.Label_Match._process_exact_rescan_product(app, "ITEM000000001-B")
    saved = manager.load_current_state()["current_set_info"]
    assert saved["exact_rescan_complete"] is True
    assert saved["exact_rescan_active"] is False
    assert saved["exact_rescan_barcodes"] == ["ITEM000000001-A", "ITEM000000001-B"]
    manager.close(timeout=5)


def test_outbox_enqueue_and_immutable_command_cas(tmp_path):
    outbox = PackageOutbox(tmp_path / "outbox.sqlite3")
    draft = _draft()
    first = outbox.enqueue(draft)
    assert outbox.enqueue(draft)["idempotency_key"] == first["idempotency_key"]
    assert outbox.claim_next() is None
    outbox.mark_local_completion_committed(first["idempotency_key"])
    claimed = outbox.claim_next()
    command = {"authority_scope_id": SCOPE, "idempotency_key": claimed["idempotency_key"], "payload": {"x": 1}}
    outbox.save_command(claimed["idempotency_key"], TRANSFER, command)
    outbox.save_command(claimed["idempotency_key"], TRANSFER, command)
    with pytest.raises(PackageLogisticsError, match="immutable"):
        outbox.save_command(
            claimed["idempotency_key"], TRANSFER, {**command, "payload": {"x": 2}}
        )


def test_package_outbox_lists_only_locally_uncommitted_completions(tmp_path):
    outbox = PackageOutbox(tmp_path / "outbox.sqlite3")
    draft = _draft()
    row = outbox.enqueue(draft)

    pending = outbox.list_local_completion_pending()
    assert [item["idempotency_key"] for item in pending] == [
        row["idempotency_key"]
    ]

    outbox.mark_local_completion_committed(row["idempotency_key"])
    assert outbox.list_local_completion_pending() == []


def test_package_outbox_never_claims_before_durable_local_completion(tmp_path):
    outbox = PackageOutbox(tmp_path / "local-first-claim.sqlite3")
    draft = _draft_for_set("LOCAL-FIRST-CLAIM")
    queued = outbox.enqueue(draft)

    # PENDING is the delivery state on both sides of the local commit boundary.
    # PREPARED is therefore documentation shorthand for this existing field
    # combination, not a persisted status value.
    assert queued["local_completion_committed"] == 0
    assert queued["status"] == "PENDING"
    assert outbox.claim_next() is None
    with sqlite3.connect(tmp_path / "local-first-claim.sqlite3") as conn:
        table_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' "
            "AND name='package_command_outbox'"
        ).fetchone()[0]
    assert "PREPARED" not in table_sql

    outbox.mark_local_completion_committed(queued["idempotency_key"])
    committed = outbox.get_by_set_id(draft.set_id)
    assert committed["status"] == "PENDING"
    assert committed["local_completion_committed"] == 1
    claimed = outbox.claim_next()

    assert claimed["idempotency_key"] == queued["idempotency_key"]
    assert claimed["status"] == "SENDING"


@pytest.mark.parametrize("outbox_type", [PackageOutbox, PackageCancellationOutbox])
def test_current_schema_is_complete_before_version_is_stamped(tmp_path, outbox_type):
    db_path = tmp_path / f"schema-{outbox_type.__name__}.sqlite3"
    outbox_type(db_path)
    conn = sqlite3.connect(db_path)
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        cancellation_columns = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(package_cancellation_outbox)"
            ).fetchall()
        }
        command_columns = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(package_command_outbox)"
            ).fetchall()
        }
        version = conn.execute(
            "SELECT value FROM package_outbox_schema_info WHERE key='schema_version'"
        ).fetchone()[0]
    finally:
        conn.close()
    assert {
        "package_command_outbox",
        "package_cancellation_outbox",
        "package_replacement_waiting_outbox",
        "package_post_review_outbox",
    }.issubset(tables)
    assert {
        "local_event_committed",
        "local_event_committed_at",
        "retry_after_at",
    }.issubset(cancellation_columns)
    assert {
        "retry_after_at",
        "review_status",
        "last_attempt_at",
        "local_completion_committed",
        "local_completion_committed_at",
    }.issubset(command_columns)
    with sqlite3.connect(db_path) as conn:
        replacement_columns = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(package_replacement_waiting_outbox)"
            ).fetchall()
        }
    assert {
        "dedupe_key",
        "event_fingerprint",
        "event_json",
        "local_csv_committed",
        "local_csv_committed_at",
        "created_at",
        "updated_at",
    }.issubset(replacement_columns)
    with sqlite3.connect(db_path) as conn:
        post_review_columns = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(package_post_review_outbox)"
            ).fetchall()
        }
    assert {
        "review_event_id",
        "package_idempotency_key",
        "event_fingerprint",
        "event_json",
        "local_csv_committed",
        "local_csv_committed_at",
        "created_at",
        "updated_at",
    }.issubset(post_review_columns)
    assert version == package_module.OUTBOX_SCHEMA_VERSION


def test_replacement_waiting_ledger_is_immutable_and_projection_is_exact_once(
    tmp_path,
):
    db_path = tmp_path / "replacement-waiting.sqlite3"
    first = PackageOutbox(db_path)
    second = PackageOutbox(db_path)
    payload = _replacement_waiting_payload()

    queued = first.enqueue_replacement_waiting_event(payload)
    replayed = second.enqueue_replacement_waiting_event(
        _replacement_waiting_payload(marked_at="2026-08-01T10:05:00")
    )

    assert replayed["dedupe_key"] == queued["dedupe_key"]
    assert json.loads(replayed["event_json"])["marked_at"] == payload["marked_at"]
    changed = {**payload, "location": "SHIPPING"}
    with pytest.raises(PackageLogisticsError, match="immutable"):
        second.enqueue_replacement_waiting_event(changed)

    projected = []
    barrier = threading.Barrier(8)

    def commit(instance):
        barrier.wait()
        return instance.commit_replacement_waiting_csv_projection(
            payload["dedupe_key"],
            lambda saved: projected.append(saved["dedupe_key"]),
        )

    instances = [PackageOutbox(db_path) for _ in range(8)]
    results = []
    threads = [
        threading.Thread(target=lambda box=box: results.append(commit(box)))
        for box in instances
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert all(not thread.is_alive() for thread in threads)
    assert results.count(True) == 1
    assert results.count(False) == 7
    assert projected == [payload["dedupe_key"]]
    row = first.get_replacement_waiting_event(payload["dedupe_key"])
    assert row["local_csv_committed"] == 1


def test_replacement_waiting_projection_failure_keeps_durable_retry(tmp_path):
    outbox = PackageOutbox(tmp_path / "replacement-retry.sqlite3")
    payload = _replacement_waiting_payload()
    outbox.enqueue_replacement_waiting_event(payload)
    writes = []

    def crash_after_write(saved):
        writes.append(saved["dedupe_key"])
        raise RuntimeError("crash after CSV fsync")

    with pytest.raises(RuntimeError, match="crash after CSV fsync"):
        outbox.commit_replacement_waiting_csv_projection(
            payload["dedupe_key"], crash_after_write
        )

    pending = outbox.list_replacement_waiting_csv_pending()
    assert [row["dedupe_key"] for row in pending] == [payload["dedupe_key"]]
    assert outbox.commit_replacement_waiting_csv_projection(
        payload["dedupe_key"], lambda saved: writes.append(saved["dedupe_key"])
    )
    assert writes == [payload["dedupe_key"], payload["dedupe_key"]]


def test_real_v1_database_migration_preserves_create_rows_and_states(tmp_path):
    db_path = tmp_path / "real-v1.sqlite3"
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE package_command_outbox (
                idempotency_key TEXT PRIMARY KEY,
                set_id TEXT NOT NULL UNIQUE,
                command_fingerprint TEXT NOT NULL,
                draft_json TEXT NOT NULL,
                resolved_source_bundle_id TEXT,
                command_json TEXT,
                status TEXT NOT NULL CHECK(status IN ('PENDING','SENDING','ACKED','CONFLICT')),
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_error_code TEXT,
                last_error_message TEXT,
                receipt_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE package_outbox_schema_info (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            INSERT INTO package_outbox_schema_info(key,value)
            VALUES ('schema_version','label-match-package-outbox-v1');
            """
        )
        expected = {}
        for index, status in enumerate(("PENDING", "SENDING", "ACKED", "CONFLICT"), 1):
            draft = _draft_for_set(f"V1-SET-{index}")
            key = f"v1-key-{index}"
            command_json = json.dumps(
                {"idempotency_key": key, "payload": {"ordinal": index}},
                sort_keys=True,
            )
            receipt_json = json.dumps(
                {"receipt_id": f"v1-receipt-{index}", "ordinal": index},
                sort_keys=True,
            )
            conn.execute(
                """INSERT INTO package_command_outbox(
                       idempotency_key,set_id,command_fingerprint,draft_json,
                       resolved_source_bundle_id,command_json,status,attempt_count,
                       last_error_code,last_error_message,receipt_json,created_at,updated_at
                   ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    key,
                    draft.set_id,
                    draft.fingerprint(),
                    json.dumps(draft.to_dict(), sort_keys=True),
                    TRANSFER,
                    command_json,
                    status,
                    index,
                    f"V1_{status}",
                    f"v1 message {index}",
                    receipt_json,
                    f"2026-07-15T00:00:0{index}Z",
                    f"2026-07-15T00:00:0{index}Z",
                ),
            )
            expected[key] = {
                "status": status,
                "command_json": command_json,
                "receipt_json": receipt_json,
                "attempt_count": index,
                "last_error_code": f"V1_{status}",
                "last_error_message": f"v1 message {index}",
            }
        conn.commit()
    finally:
        conn.close()

    PackageCancellationOutbox(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        migrated = {
            row["idempotency_key"]: dict(row)
            for row in conn.execute(
                "SELECT * FROM package_command_outbox ORDER BY idempotency_key"
            ).fetchall()
        }
        version = conn.execute(
            "SELECT value FROM package_outbox_schema_info WHERE key='schema_version'"
        ).fetchone()[0]
        cancellation_columns = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(package_cancellation_outbox)"
            ).fetchall()
        }
    finally:
        conn.close()
    assert set(migrated) == set(expected)
    for key, values in expected.items():
        for field, value in values.items():
            assert migrated[key][field] == value
        assert migrated[key]["local_completion_committed"] == 0
    assert "retry_after_at" in cancellation_columns
    assert version == package_module.OUTBOX_SCHEMA_VERSION


def test_local_completion_marker_is_durable_before_central_ack_and_idempotent(tmp_path):
    outbox = PackageOutbox(tmp_path / "local-completion.sqlite3")
    draft = _draft_for_set("LOCAL-COMPLETION-SET")
    pending = outbox.enqueue(draft)

    outbox.mark_local_completion_committed(pending["idempotency_key"])
    outbox.mark_local_completion_committed(pending["idempotency_key"])

    committed = outbox.get_by_set_id(draft.set_id)
    assert committed["status"] == "PENDING"
    assert committed["local_completion_committed"] == 1
    assert committed["local_completion_committed_at"]


def test_concurrent_initialization_is_atomic_and_leaves_complete_schema(tmp_path):
    db_path = tmp_path / "concurrent-init.sqlite3"
    barrier = threading.Barrier(8)
    failures = []

    def initialize(index):
        try:
            barrier.wait(timeout=5)
            outbox_type = PackageOutbox if index % 2 == 0 else PackageCancellationOutbox
            outbox_type(db_path)
        except Exception as exc:  # pragma: no cover - asserted below
            failures.append(exc)

    threads = [threading.Thread(target=initialize, args=(index,)) for index in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=15)

    assert not any(thread.is_alive() for thread in threads)
    assert failures == []
    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute(
            "SELECT value FROM package_outbox_schema_info WHERE key='schema_version'"
        ).fetchone()[0] == package_module.OUTBOX_SCHEMA_VERSION
        columns = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(package_cancellation_outbox)"
            ).fetchall()
        }
    finally:
        conn.close()
    assert {"local_event_committed", "local_event_committed_at", "retry_after_at"}.issubset(
        columns
    )


def test_second_initializer_does_not_requeue_a_live_sending_claim(tmp_path):
    draft = _draft()
    db_path = tmp_path / "live-claim-second-initializer.sqlite3"
    package_outbox = PackageOutbox(db_path)
    _ack_package_creation(package_outbox, draft)
    cancellation_outbox = PackageCancellationOutbox(db_path)
    intent = _cancellation_intent(draft)
    cancellation_outbox.enqueue(intent)
    cancellation_outbox.mark_local_event_committed(intent.cancellation_event_id)
    claimed = cancellation_outbox.claim_next()
    assert claimed["status"] == "SENDING"

    PackageOutbox(db_path)
    second_cancellation_outbox = PackageCancellationOutbox(db_path)
    still_claimed = second_cancellation_outbox.get_by_event_id(
        intent.cancellation_event_id
    )
    assert still_claimed["status"] == "SENDING"
    assert second_cancellation_outbox.claim_next() is None


def test_real_v2_five_state_migration_preserves_every_state_and_payload(
    tmp_path,
):
    db_path = tmp_path / "real-v2-five-state.sqlite3"
    package_outbox = PackageOutbox(db_path)
    cancellation_outbox = PackageCancellationOutbox(db_path)
    statuses = ("DEFERRED", "PENDING", "SENDING", "ACKED", "CONFLICT")
    expected = {}
    for index, status in enumerate(statuses, 1):
        draft = _draft_for_set(f"V2-SET-{index}")
        _ack_package_creation(package_outbox, draft)
        intent = _cancellation_intent(draft)
        queued = cancellation_outbox.enqueue(intent)
        cancellation_outbox.mark_local_event_committed(intent.cancellation_event_id)
        command_json = json.dumps(
            {"preserved_command": index, "idempotency_key": queued["idempotency_key"]},
            sort_keys=True,
        )
        receipt_json = json.dumps(
            {"preserved_receipt": index, "receipt_id": f"v2-receipt-{index}"},
            sort_keys=True,
        )
        conn = sqlite3.connect(db_path)
        try:
            conn.execute(
                """UPDATE package_cancellation_outbox
                      SET status=?,attempt_count=?,last_error_code=?,
                          last_error_message=?,command_json=?,receipt_json=?,
                          local_event_committed=1,
                          local_event_committed_at=?,updated_at=?
                    WHERE cancellation_event_id=?""",
                (
                    status,
                    index,
                    f"V2_{status}",
                    f"v2 message {index}",
                    command_json,
                    receipt_json,
                    f"2026-07-15T00:00:0{index}Z",
                    f"2026-07-15T00:00:0{index}Z",
                    intent.cancellation_event_id,
                ),
            )
            conn.commit()
        finally:
            conn.close()
        expected[intent.cancellation_event_id] = {
            "status": status,
            "attempt_count": index,
            "last_error_code": f"V2_{status}",
            "last_error_message": f"v2 message {index}",
            "command_json": command_json,
            "receipt_json": receipt_json,
            "local_event_committed": 1,
        }

    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            PRAGMA foreign_keys=OFF;
            BEGIN IMMEDIATE;
            ALTER TABLE package_cancellation_outbox
                RENAME TO package_cancellation_outbox_v3_old;
            CREATE TABLE package_cancellation_outbox (
                idempotency_key TEXT PRIMARY KEY,
                cancellation_event_id TEXT NOT NULL UNIQUE,
                set_id TEXT NOT NULL,
                package_idempotency_key TEXT NOT NULL,
                package_bundle_id TEXT NOT NULL,
                intent_fingerprint TEXT NOT NULL,
                intent_json TEXT NOT NULL,
                authority_scope_id TEXT,
                authority_epoch INTEGER,
                ledger_plane TEXT,
                plane_epoch INTEGER,
                expected_bundle_version INTEGER,
                command_json TEXT,
                status TEXT NOT NULL
                    CHECK(status IN ('DEFERRED','PENDING','SENDING','ACKED','CONFLICT')),
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_error_code TEXT,
                last_error_message TEXT,
                receipt_json TEXT,
                local_event_committed INTEGER NOT NULL DEFAULT 0
                    CHECK(local_event_committed IN (0,1)),
                local_event_committed_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(package_idempotency_key)
                    REFERENCES package_command_outbox(idempotency_key)
            );
            INSERT INTO package_cancellation_outbox(
                idempotency_key,cancellation_event_id,set_id,package_idempotency_key,
                package_bundle_id,intent_fingerprint,intent_json,authority_scope_id,
                authority_epoch,ledger_plane,plane_epoch,expected_bundle_version,
                command_json,status,attempt_count,last_error_code,last_error_message,
                receipt_json,local_event_committed,local_event_committed_at,
                created_at,updated_at
            )
            SELECT idempotency_key,cancellation_event_id,set_id,package_idempotency_key,
                   package_bundle_id,intent_fingerprint,intent_json,authority_scope_id,
                   authority_epoch,ledger_plane,plane_epoch,expected_bundle_version,
                   command_json,status,attempt_count,last_error_code,last_error_message,
                   receipt_json,local_event_committed,local_event_committed_at,
                   created_at,updated_at
              FROM package_cancellation_outbox_v3_old;
            DROP TABLE package_cancellation_outbox_v3_old;
            CREATE INDEX ix_package_cancellation_outbox_status
                ON package_cancellation_outbox(status,created_at);
            CREATE INDEX ix_package_cancellation_outbox_set
                ON package_cancellation_outbox(set_id,created_at);
            INSERT OR REPLACE INTO package_outbox_schema_info(key,value)
                VALUES ('schema_version','label-match-package-outbox-v2');
            COMMIT;
            """
        )
    finally:
        conn.close()

    migrated_outbox = PackageCancellationOutbox(db_path)
    for event_id, values in expected.items():
        row = migrated_outbox.get_by_event_id(event_id)
        for field, value in values.items():
            assert row[field] == value
        assert row["retry_after_at"] is None
    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute(
            "SELECT value FROM package_outbox_schema_info WHERE key='schema_version'"
        ).fetchone()[0] == package_module.OUTBOX_SCHEMA_VERSION
        columns = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(package_cancellation_outbox)"
            ).fetchall()
        }
    finally:
        conn.close()
    assert "retry_after_at" in columns



def test_stale_sending_lease_is_reclaimed_by_claim_next_not_initializer(tmp_path):
    draft = _draft()
    db_path = tmp_path / "stale-cancellation-lease.sqlite3"
    package_outbox = PackageOutbox(db_path)
    _ack_package_creation(package_outbox, draft)
    cancellation_outbox = PackageCancellationOutbox(db_path)
    intent = _cancellation_intent(draft)
    cancellation_outbox.enqueue(intent)
    cancellation_outbox.mark_local_event_committed(intent.cancellation_event_id)
    first_claim = cancellation_outbox.claim_next()
    assert first_claim["attempt_count"] == 1
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """UPDATE package_cancellation_outbox
                  SET updated_at='2000-01-01T00:00:00Z'
                WHERE cancellation_event_id=?""",
            (intent.cancellation_event_id,),
        )
        conn.commit()
    finally:
        conn.close()

    initialized_only = PackageCancellationOutbox(db_path)
    assert initialized_only.get_by_event_id(intent.cancellation_event_id)["status"] == "SENDING"
    reclaimed = initialized_only.claim_next()
    assert reclaimed["cancellation_event_id"] == intent.cancellation_event_id
    assert reclaimed["status"] == "SENDING"
    assert reclaimed["attempt_count"] == 2


def test_separate_process_cannot_reclaim_recent_live_create_or_cancel_claim(tmp_path):
    db_path = tmp_path / "cross-process-live-leases.sqlite3"
    package_outbox = PackageOutbox(db_path)
    cancellation_draft = _draft_for_set("LIVE-CANCEL-SET")
    _ack_package_creation(package_outbox, cancellation_draft)
    cancellation_outbox = PackageCancellationOutbox(db_path)
    intent = _cancellation_intent(cancellation_draft)
    cancellation_outbox.enqueue(intent)
    cancellation_outbox.mark_local_event_committed(intent.cancellation_event_id)
    assert cancellation_outbox.claim_next()["status"] == "SENDING"

    create_draft = _draft_for_set("LIVE-CREATE-SET")
    create_row = package_outbox.enqueue(create_draft)
    package_outbox.mark_local_completion_committed(
        create_row["idempotency_key"]
    )
    assert package_outbox.claim_next()["status"] == "SENDING"
    probe = """
import json
import sys
from package_logistics import PackageCancellationOutbox, PackageOutbox

db_path, set_id, event_id = sys.argv[1:]
package_outbox = PackageOutbox(db_path)
cancellation_outbox = PackageCancellationOutbox(db_path)
print(json.dumps({
    "create_status": package_outbox.get_by_set_id(set_id)["status"],
    "cancel_status": cancellation_outbox.get_by_event_id(event_id)["status"],
    "create_claim": package_outbox.claim_next() is None,
    "cancel_claim": cancellation_outbox.claim_next() is None,
}, sort_keys=True))
"""
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            probe,
            str(db_path),
            create_draft.set_id,
            intent.cancellation_event_id,
        ],
        cwd=Path(package_module.__file__).resolve().parent,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    result = json.loads(completed.stdout.strip())
    assert result == {
        "cancel_claim": True,
        "cancel_status": "SENDING",
        "create_claim": True,
        "create_status": "SENDING",
    }
    assert package_outbox.get_by_set_id(create_draft.set_id)["status"] == "SENDING"
    assert cancellation_outbox.get_by_event_id(intent.cancellation_event_id)["status"] == "SENDING"


def test_outbox_explicitly_closes_every_connection_before_immediate_file_cleanup(
    tmp_path, monkeypatch
):
    real_connect = sqlite3.connect
    opened = []
    explicitly_closed = set()

    class TrackingConnection(sqlite3.Connection):
        def close(self):
            explicitly_closed.add(id(self))
            return super().close()

    def tracked_connect(*args, **kwargs):
        kwargs["factory"] = TrackingConnection
        conn = real_connect(*args, **kwargs)
        opened.append(conn)
        return conn

    monkeypatch.setattr(package_module.sqlite3, "connect", tracked_connect)
    db_path = tmp_path / "handles.sqlite3"
    outbox = PackageOutbox(db_path)
    draft = _draft()
    row = outbox.enqueue(draft)
    outbox.mark_local_completion_committed(row["idempotency_key"])
    claimed = outbox.claim_next()
    assert claimed["idempotency_key"] == row["idempotency_key"]
    command = {
        "contract_version": "logistics-v1",
        "command_type": "CREATE_PACKAGE",
        "authority_scope_id": SCOPE,
        "authority_epoch": 5,
        "ledger_plane": "AUTHORITATIVE",
        "plane_epoch": 3,
        "idempotency_key": row["idempotency_key"],
        "expected_versions": {f"bundle:{TRANSFER}": 7},
        "payload": {
            "source_bundle_id": TRANSFER,
            "package_bundle_id": draft.package_bundle_id,
        },
    }
    outbox.save_command(row["idempotency_key"], TRANSFER, command)
    outbox.mark_acked(row["idempotency_key"], _receipt(draft))
    assert outbox.get_by_set_id(draft.set_id)["status"] == "ACKED"
    assert outbox.counts()["ACKED"] == 1

    cancellation_outbox = PackageCancellationOutbox(db_path)
    intent = _cancellation_intent(draft)
    assert cancellation_outbox.enqueue(intent)["status"] == "PENDING"
    cancellation_outbox.mark_local_event_committed(intent.cancellation_event_id)
    assert cancellation_outbox.get_by_event_id(intent.cancellation_event_id)
    assert cancellation_outbox.get_by_set_id(draft.set_id)
    assert cancellation_outbox.uncommitted_local_events() == []
    assert cancellation_outbox.list_conflicts() == []
    assert cancellation_outbox.counts()["PENDING"] == 1

    restarted = PackageOutbox(db_path)
    assert restarted.get_by_set_id(draft.set_id)["status"] == "ACKED"
    assert opened
    assert {id(conn) for conn in opened} == explicitly_closed

    # Holding every connection object above prevents destructor/GC cleanup from
    # masking a missing close(). Windows will reject these moves if any SQLite
    # handle is still live. Materialize absent sidecars so all three names are
    # exercised even after a clean final WAL checkpoint removes them.
    candidates = [Path(f"{db_path}-wal"), Path(f"{db_path}-shm"), db_path]
    for candidate in candidates:
        candidate.touch(exist_ok=True)
        moved = candidate.with_name(f"{candidate.name}.moved")
        candidate.replace(moved)
        moved.unlink()


def test_acked_package_cancellation_enqueues_and_posts_exact_server_contract(tmp_path):
    draft = _draft()
    db_path = tmp_path / "cancel-after-ack.sqlite3"
    package_outbox = PackageOutbox(db_path)
    package_row = _ack_package_creation(package_outbox, draft)
    # WorkerAnalysis CommandResult.to_dict() intentionally does not duplicate
    # the command idempotency key in the receipt body. The saved CREATE command
    # and its outbox row retain that identity instead.
    assert "idempotency_key" not in json.loads(package_row["receipt_json"])
    cancellation_outbox = PackageCancellationOutbox(db_path)
    intent = _cancellation_intent(draft)

    queued = cancellation_outbox.enqueue(intent)
    assert queued["status"] == "PENDING"
    assert queued["expected_bundle_version"] == 1
    cancellation_outbox.mark_local_event_committed(intent.cancellation_event_id)
    calls = []

    def transport(method, url, headers, body, timeout):
        calls.append((method, url, dict(headers), json.loads(body.decode("utf-8"))))
        return {"ok": True, "data": _cancellation_receipt(draft, intent=intent)}

    client = PackageLogisticsClient(
        PackageClientConfig("https://logistics.test", "token", SCOPE, "host", "device"),
        transport=transport,
    )
    result = PackageCancellationOutboxProcessor(cancellation_outbox, client).drain(limit=1)

    assert result == {"acked": 1, "retry": 0, "conflict": 0, "deferred": 0}
    assert len(calls) == 1
    method, url, headers, command = calls[0]
    assert method == "POST"
    assert url.endswith("/logistics/api/v1/packages/cancel")
    assert headers["Idempotency-Key"] == queued["idempotency_key"]
    assert command["command_type"] == "CANCEL_PACKAGE"
    assert command["expected_versions"] == {f"bundle:{draft.package_bundle_id}": 1}
    assert command["payload"]["package_bundle_id"] == draft.package_bundle_id
    assert command["payload"]["reason"] == "LOCAL_TRAY_COMPLETION_CANCELLED"
    assert command["payload"]["evidence"]["cancellation_event_id"] == intent.cancellation_event_id
    assert command["payload"]["evidence"]["set_id"] == draft.set_id
    assert cancellation_outbox.get_by_event_id(intent.cancellation_event_id)["status"] == "ACKED"


def test_package_cancellation_before_create_ack_stays_deferred_then_promotes(tmp_path):
    draft = _draft()
    db_path = tmp_path / "cancel-deferred.sqlite3"
    package_outbox = PackageOutbox(db_path)
    package_row = package_outbox.enqueue(draft)
    package_outbox.mark_local_completion_committed(
        package_row["idempotency_key"]
    )
    cancellation_outbox = PackageCancellationOutbox(db_path)
    intent = _cancellation_intent(draft, event_type="SET_DELETED")
    queued = cancellation_outbox.enqueue(intent)
    assert queued["status"] == "DEFERRED"
    cancellation_outbox.mark_local_event_committed(intent.cancellation_event_id)

    calls = []
    client = PackageLogisticsClient(
        PackageClientConfig("https://logistics.test", "token", SCOPE, "host", "device"),
        transport=lambda *args: calls.append(args)
        or {"ok": True, "data": _cancellation_receipt(draft, intent=intent)},
    )
    processor = PackageCancellationOutboxProcessor(cancellation_outbox, client)
    assert processor.drain(limit=1) == {
        "acked": 0,
        "retry": 0,
        "conflict": 0,
        "deferred": 1,
    }
    assert calls == []

    _ack_package_creation(package_outbox, draft)
    assert processor.drain(limit=1)["acked"] == 1
    assert len(calls) == 1
    assert cancellation_outbox.get_by_event_id(intent.cancellation_event_id)["status"] == "ACKED"


def test_package_cancellation_retry_recovers_saved_command_receipt_without_duplicate_post(tmp_path):
    draft = _draft()
    db_path = tmp_path / "cancel-retry.sqlite3"
    package_outbox = PackageOutbox(db_path)
    _ack_package_creation(package_outbox, draft)
    cancellation_outbox = PackageCancellationOutbox(db_path)
    intent = _cancellation_intent(draft)
    queued = cancellation_outbox.enqueue(intent)
    cancellation_outbox.mark_local_event_committed(intent.cancellation_event_id)
    builder = PackageLogisticsClient(
        PackageClientConfig("https://logistics.test", "token", SCOPE, "host", "device"),
        transport=lambda *args: (_ for _ in ()).throw(AssertionError("unexpected transport")),
    )

    class LostAckClient:
        def __init__(self):
            self.build_calls = 0
            self.cancel_calls = 0

        def build_cancel_package_command(self, intent, row, *, idempotency_key):
            self.build_calls += 1
            return builder.build_cancel_package_command(
                intent, row, idempotency_key=idempotency_key
            )

        def cancel_package(self, command):
            self.cancel_calls += 1
            raise PackageTransportError("lost ACK")

        def get_receipt_if_exists(self, key, *, authority_scope_id):
            return None

    first_client = LostAckClient()
    first = PackageCancellationOutboxProcessor(cancellation_outbox, first_client).drain(limit=1)
    assert first == {"acked": 0, "retry": 1, "conflict": 0, "deferred": 0}
    pending = cancellation_outbox.get_by_event_id(intent.cancellation_event_id)
    saved_command = pending["command_json"]
    assert saved_command
    assert first_client.build_calls == 1
    assert first_client.cancel_calls == 1

    class RecoveryClient(LostAckClient):
        def get_receipt_if_exists(self, key, *, authority_scope_id):
            assert key == queued["idempotency_key"]
            assert authority_scope_id == SCOPE
            return _cancellation_receipt(draft, intent=intent)

        def cancel_package(self, command):
            raise AssertionError("receipt replay must not repost")

    restarted = PackageCancellationOutbox(db_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """UPDATE package_cancellation_outbox SET retry_after_at=NULL
                 WHERE cancellation_event_id=?""",
            (intent.cancellation_event_id,),
        )
        conn.commit()
    finally:
        conn.close()
    recovery_client = RecoveryClient()
    recovered = PackageCancellationOutboxProcessor(restarted, recovery_client).drain(limit=1)
    assert recovered == {"acked": 1, "retry": 0, "conflict": 0, "deferred": 0}
    acked = restarted.get_by_event_id(intent.cancellation_event_id)
    assert acked["status"] == "ACKED"
    assert acked["command_json"] == saved_command
    assert recovery_client.build_calls == 0


def test_http_429_preserves_retry_metadata_and_retry_after_header(monkeypatch):
    payload = json.dumps(
        {
            "ok": False,
            "error": {
                "code": "PACKAGE_RATE_LIMITED",
                "message": "slow down",
            },
            "retryable": True,
            "committed": False,
        }
    ).encode("utf-8")

    def reject(*args, **kwargs):
        raise package_module.HTTPError(
            "https://logistics.test/cancel",
            429,
            "Too Many Requests",
            {"Retry-After": "73"},
            io.BytesIO(payload),
        )

    monkeypatch.setattr(package_module, "urlopen", reject)
    with pytest.raises(PackageApiError) as raised:
        package_module._default_transport(
            "POST", "https://logistics.test/cancel", {}, b"{}", 1.0
        )
    error = raised.value
    assert error.status_code == 429
    assert error.code == "PACKAGE_RATE_LIMITED"
    assert error.retryable is True
    assert error.committed is False
    assert error.retry_after_seconds == 73.0


def test_custom_transport_error_normalizes_top_level_retry_metadata():
    with pytest.raises(PackageApiError) as raised:
        PackageLogisticsClient._data(
            {
                "ok": False,
                "error": {
                    "status_code": 429,
                    "code": "PACKAGE_RATE_LIMITED",
                    "message": "slow down",
                    "retry_after_seconds": "invalid",
                    "retryable": "invalid",
                    "committed": "invalid",
                },
                "retryable": "true",
                "committed": "false",
                "retry_after_seconds": 0,
            }
        )
    error = raised.value
    assert error.status_code == 429
    assert error.retryable is True
    assert error.committed is False
    assert error.retry_after_seconds == 0.0


@pytest.mark.parametrize(
    "invalid_ok",
    ["false", "0", "", "null", None, 0, 1, [], {}],
    ids=[
        "string-false",
        "string-zero",
        "empty-string",
        "string-null",
        "json-null",
        "integer-zero",
        "integer-one",
        "array",
        "object",
    ],
)
def test_package_api_envelope_rejects_non_boolean_ok_sentinels(invalid_ok):
    with pytest.raises(
        PackageTransportError, match="package API ok must be a JSON boolean"
    ):
        PackageLogisticsClient._data(
            {"ok": invalid_ok, "data": {"package_bundle_id": "PKG-TEST"}}
        )


def test_package_api_envelope_accepts_literal_true_positive_control():
    assert PackageLogisticsClient._data(
        {"ok": True, "data": {"package_bundle_id": "PKG-TEST"}}
    ) == {"package_bundle_id": "PKG-TEST"}


def test_package_api_envelope_rejects_missing_ok_field():
    with pytest.raises(
        PackageTransportError, match="package API ok must be a JSON boolean"
    ):
        PackageLogisticsClient._data({"data": {"package_bundle_id": "PKG-TEST"}})


def test_incomplete_post_body_recovers_committed_cancel_receipt(monkeypatch):
    receipt = {"receipt_id": "cancel-recovered-after-incomplete-read"}
    methods = []

    class Response:
        def __init__(self, payload=None, *, incomplete=False):
            self.payload = payload
            self.incomplete = incomplete

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def read(self):
            if self.incomplete:
                raise package_module.IncompleteRead(b'{"ok":true', 200)
            return json.dumps(self.payload).encode("utf-8")

    def fake_urlopen(request, *, timeout):
        methods.append(request.get_method())
        if request.get_method() == "POST":
            return Response(incomplete=True)
        return Response({"ok": True, "data": receipt})

    monkeypatch.setattr(package_module, "urlopen", fake_urlopen)
    client = PackageLogisticsClient(
        PackageClientConfig(
            "https://logistics.test", "token", SCOPE, "host", "device"
        )
    )
    recovered = client.cancel_package(
        {
            "idempotency_key": "cancel-lost-ack",
            "authority_scope_id": SCOPE,
        }
    )
    assert recovered == receipt
    assert methods == ["POST", "GET"]


def test_invalid_utf8_response_is_transport_error(monkeypatch):
    class Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def read(self):
            return b"\xff\xfe"

    monkeypatch.setattr(package_module, "urlopen", lambda *args, **kwargs: Response())
    with pytest.raises(PackageTransportError, match="UTF-8"):
        package_module._default_transport(
            "GET", "https://logistics.test/receipt", {}, None, 1.0
        )


@pytest.mark.parametrize(
    ("body_retry_after", "header_retry_after", "expected"),
    (("invalid", "61", 61.0), (0, "61", 0.0), ("inf", "999999", 1800.0)),
)
def test_retry_after_uses_first_valid_value_preserves_zero_and_clamps(
    monkeypatch, body_retry_after, header_retry_after, expected
):
    payload = json.dumps(
        {
            "ok": False,
            "error": {
                "code": "PACKAGE_RATE_LIMITED",
                "message": "slow down",
                "retryable": True,
                "retry_after_seconds": body_retry_after,
            },
        }
    ).encode("utf-8")

    def reject(*args, **kwargs):
        raise package_module.HTTPError(
            "https://logistics.test/cancel",
            429,
            "Too Many Requests",
            {"Retry-After": header_retry_after},
            io.BytesIO(payload),
        )

    monkeypatch.setattr(package_module, "urlopen", reject)
    with pytest.raises(PackageApiError) as raised:
        package_module._default_transport(
            "POST", "https://logistics.test/cancel", {}, b"{}", 1.0
        )
    assert raised.value.retry_after_seconds == expected


@pytest.mark.parametrize("status_code", [400, 408, 425, 429])
def test_transient_cancellation_api_statuses_remain_pending_with_due_backoff(
    tmp_path, status_code
):
    draft = _draft()
    db_path = tmp_path / f"cancel-transient-{status_code}.sqlite3"
    package_outbox = PackageOutbox(db_path)
    _ack_package_creation(package_outbox, draft)
    cancellation_outbox = PackageCancellationOutbox(db_path)
    intent = _cancellation_intent(draft)
    cancellation_outbox.enqueue(intent)
    cancellation_outbox.mark_local_event_committed(intent.cancellation_event_id)
    builder = PackageLogisticsClient(
        PackageClientConfig("https://logistics.test", "token", SCOPE, "host", "device"),
        transport=lambda *args: (_ for _ in ()).throw(AssertionError("unexpected transport")),
    )

    class ThrottledClient:
        def build_cancel_package_command(self, intent, row, *, idempotency_key):
            return builder.build_cancel_package_command(
                intent, row, idempotency_key=idempotency_key
            )

        def cancel_package(self, command):
            raise PackageApiError(
                status_code,
                "TRANSIENT_TEST",
                "retry later",
                retryable=True,
                committed=False,
                retry_after_seconds=90,
            )

    result = PackageCancellationOutboxProcessor(
        cancellation_outbox, ThrottledClient()
    ).drain(limit=2)
    row = cancellation_outbox.get_by_event_id(intent.cancellation_event_id)
    assert result == {"acked": 0, "retry": 1, "conflict": 0, "deferred": 0}
    assert row["status"] == "PENDING"
    assert row["last_error_code"] == "TRANSIENT_TEST"
    assert row["retry_after_at"] > package_module.utc_now()
    assert row["attempt_count"] == 1
    assert cancellation_outbox.claim_next() is None


def test_non_finite_retry_after_cannot_leave_cancellation_sending(tmp_path):
    draft = _draft()
    db_path = tmp_path / "cancel-non-finite-retry.sqlite3"
    package_outbox = PackageOutbox(db_path)
    _ack_package_creation(package_outbox, draft)
    cancellation_outbox = PackageCancellationOutbox(db_path)
    intent = _cancellation_intent(draft)
    cancellation_outbox.enqueue(intent)
    cancellation_outbox.mark_local_event_committed(intent.cancellation_event_id)
    builder = PackageLogisticsClient(
        PackageClientConfig("https://logistics.test", "token", SCOPE, "host", "device"),
        transport=lambda *args: (_ for _ in ()).throw(AssertionError("unexpected transport")),
    )

    class NonFiniteClient:
        def build_cancel_package_command(self, intent, row, *, idempotency_key):
            return builder.build_cancel_package_command(
                intent, row, idempotency_key=idempotency_key
            )

        def cancel_package(self, command):
            raise PackageApiError(
                503,
                "TRANSIENT_NON_FINITE",
                "retry later",
                retryable=True,
                retry_after_seconds=float("nan"),
            )

    result = PackageCancellationOutboxProcessor(
        cancellation_outbox, NonFiniteClient()
    ).drain(limit=1)
    row = cancellation_outbox.get_by_event_id(intent.cancellation_event_id)
    assert result == {"acked": 0, "retry": 1, "conflict": 0, "deferred": 0}
    assert row["status"] == "PENDING"
    assert row["retry_after_at"] > package_module.utc_now()


def test_committed_api_error_is_operator_conflict_and_never_reposted(tmp_path):
    draft = _draft()
    db_path = tmp_path / "cancel-committed-error.sqlite3"
    package_outbox = PackageOutbox(db_path)
    _ack_package_creation(package_outbox, draft)
    cancellation_outbox = PackageCancellationOutbox(db_path)
    intent = _cancellation_intent(draft)
    cancellation_outbox.enqueue(intent)
    cancellation_outbox.mark_local_event_committed(intent.cancellation_event_id)
    builder = PackageLogisticsClient(
        PackageClientConfig("https://logistics.test", "token", SCOPE, "host", "device"),
        transport=lambda *args: (_ for _ in ()).throw(AssertionError("unexpected transport")),
    )

    class CommittedClient:
        def __init__(self):
            self.cancel_calls = 0

        def build_cancel_package_command(self, intent, row, *, idempotency_key):
            return builder.build_cancel_package_command(
                intent, row, idempotency_key=idempotency_key
            )

        def cancel_package(self, command):
            self.cancel_calls += 1
            raise PackageApiError(
                503,
                "COMMITTED_ACK_UNAVAILABLE",
                "committed but receipt unavailable",
                retryable=True,
                committed=True,
                retry_after_seconds=10,
            )

    client = CommittedClient()
    processor = PackageCancellationOutboxProcessor(cancellation_outbox, client)
    first = processor.drain(limit=1)
    second = processor.drain(limit=1)
    row = cancellation_outbox.get_by_event_id(intent.cancellation_event_id)
    assert first == {"acked": 0, "retry": 0, "conflict": 1, "deferred": 0}
    assert second == {"acked": 0, "retry": 0, "conflict": 0, "deferred": 0}
    assert client.cancel_calls == 1
    assert row["status"] == "CONFLICT"
    assert row["last_error_code"] == "COMMITTED_ACK_UNAVAILABLE"
    assert row["retry_after_at"] is None


@pytest.mark.parametrize("status_code", [409, 412])
def test_immutable_cas_api_statuses_are_conflicts_even_when_marked_retryable(
    tmp_path, status_code
):
    draft = _draft()
    db_path = tmp_path / f"cancel-cas-{status_code}.sqlite3"
    package_outbox = PackageOutbox(db_path)
    _ack_package_creation(package_outbox, draft)
    cancellation_outbox = PackageCancellationOutbox(db_path)
    intent = _cancellation_intent(draft)
    cancellation_outbox.enqueue(intent)
    cancellation_outbox.mark_local_event_committed(intent.cancellation_event_id)

    class CasClient:
        def build_cancel_package_command(self, intent, row, *, idempotency_key):
            return {"idempotency_key": idempotency_key}

        def cancel_package(self, command):
            raise PackageApiError(
                status_code, "IMMUTABLE_CAS", "version changed", retryable=True
            )

    result = PackageCancellationOutboxProcessor(
        cancellation_outbox, CasClient()
    ).drain(limit=1)
    assert result == {"acked": 0, "retry": 0, "conflict": 1, "deferred": 0}
    conflict = cancellation_outbox.list_conflicts(limit=1)
    assert conflict == [
        {
            "cancellation_event_id": intent.cancellation_event_id,
            "set_id": draft.set_id,
            "package_bundle_id": draft.package_bundle_id,
            "last_error_code": "IMMUTABLE_CAS",
            "last_error": "version changed",
            "updated_at": conflict[0]["updated_at"],
            "status": "CONFLICT",
        }
    ]


def test_retry_backoff_prevents_first_cancellation_from_starving_next_row(tmp_path):
    first_draft = _draft_for_set("SET-STARVE-1")
    second_draft = _draft_for_set("SET-STARVE-2")
    db_path = tmp_path / "cancel-starvation.sqlite3"
    package_outbox = PackageOutbox(db_path)
    for draft in (first_draft, second_draft):
        _ack_package_creation(package_outbox, draft)
    cancellation_outbox = PackageCancellationOutbox(db_path)
    intents = [_cancellation_intent(draft) for draft in (first_draft, second_draft)]
    for intent in intents:
        cancellation_outbox.enqueue(intent)
        cancellation_outbox.mark_local_event_committed(intent.cancellation_event_id)
    builder = PackageLogisticsClient(
        PackageClientConfig("https://logistics.test", "token", SCOPE, "host", "device"),
        transport=lambda *args: (_ for _ in ()).throw(AssertionError("unexpected transport")),
    )

    class OrderedClient:
        def build_cancel_package_command(self, intent, row, *, idempotency_key):
            return builder.build_cancel_package_command(
                intent, row, idempotency_key=idempotency_key
            )

        def cancel_package(self, command):
            bundle_id = command["payload"]["package_bundle_id"]
            if bundle_id == first_draft.package_bundle_id:
                raise PackageTransportError("first row temporarily unavailable")
            return _cancellation_receipt(second_draft, intent=intents[1])

    result = PackageCancellationOutboxProcessor(
        cancellation_outbox, OrderedClient()
    ).drain(limit=2)
    assert result == {"acked": 1, "retry": 1, "conflict": 0, "deferred": 0}
    first = cancellation_outbox.get_by_event_id(intents[0].cancellation_event_id)
    second = cancellation_outbox.get_by_event_id(intents[1].cancellation_event_id)
    assert first["status"] == "PENDING"
    assert first["retry_after_at"] > package_module.utc_now()
    assert second["status"] == "ACKED"


def test_package_cancellation_event_is_deduplicated_and_immutable(tmp_path):
    draft = _draft()
    db_path = tmp_path / "cancel-dedupe.sqlite3"
    package_outbox = PackageOutbox(db_path)
    _ack_package_creation(package_outbox, draft)
    cancellation_outbox = PackageCancellationOutbox(db_path)
    intent = _cancellation_intent(draft)

    first = cancellation_outbox.enqueue(intent)
    replay = cancellation_outbox.enqueue(intent)
    assert replay["idempotency_key"] == first["idempotency_key"]
    assert len(cancellation_outbox.get_by_set_id(draft.set_id)) == 1

    changed = PackageCancellationIntent.build(
        set_id=draft.set_id,
        event_type=intent.event_type,
        reason=intent.reason,
        evidence={"operator_action": "changed"},
        cancellation_event_id=intent.cancellation_event_id,
    )
    with pytest.raises(PackageLogisticsError, match="different data"):
        cancellation_outbox.enqueue(changed)
    assert len(cancellation_outbox.get_by_set_id(draft.set_id)) == 1


def test_deferred_cancellation_becomes_actionable_conflict_when_create_is_terminal(tmp_path):
    draft = _draft()
    db_path = tmp_path / "cancel-create-conflict.sqlite3"
    package_outbox = PackageOutbox(db_path)
    package_row = package_outbox.enqueue(draft)
    package_outbox.mark_local_completion_committed(
        package_row["idempotency_key"]
    )
    cancellation_outbox = PackageCancellationOutbox(db_path)
    intent = _cancellation_intent(draft)
    assert cancellation_outbox.enqueue(intent)["status"] == "DEFERRED"
    claimed = package_outbox.claim_next()
    package_outbox.mark_conflict(
        claimed["idempotency_key"], PackageLogisticsError("invalid CREATE_PACKAGE receipt")
    )
    cancellation_outbox.promote_deferred()
    row = cancellation_outbox.get_by_event_id(intent.cancellation_event_id)

    assert row["status"] == "CONFLICT"
    assert row["last_error_code"] == "CREATE_PACKAGE_CONFLICT"
    assert "invalid CREATE_PACKAGE receipt" in row["last_error_message"]
    assert cancellation_outbox.counts()["DEFERRED"] == 0


def test_cancellation_receipt_requires_command_identity_versions_and_exact_members(tmp_path):
    draft = _draft()
    db_path = tmp_path / "cancel-strict-receipt.sqlite3"
    package_outbox = PackageOutbox(db_path)
    _ack_package_creation(package_outbox, draft)
    cancellation_outbox = PackageCancellationOutbox(db_path)
    intent = _cancellation_intent(draft)
    cancellation_outbox.enqueue(intent)
    cancellation_outbox.mark_local_event_committed(intent.cancellation_event_id)
    row = cancellation_outbox.claim_next()
    client = PackageLogisticsClient(
        PackageClientConfig("https://logistics.test", "token", SCOPE, "host", "device"),
        transport=lambda *args: (_ for _ in ()).throw(AssertionError("unexpected transport")),
    )
    command = client.build_cancel_package_command(
        intent, row, idempotency_key=row["idempotency_key"]
    )
    cancellation_outbox.save_command(row["idempotency_key"], command)
    saved = cancellation_outbox.get_by_event_id(intent.cancellation_event_id)
    valid = _cancellation_receipt(draft, intent=intent)
    PackageCancellationOutboxProcessor._validate_receipt(saved, valid)

    mutations = (
        (lambda value: value.pop("entity_versions"), "entity versions"),
        (lambda value: value["data"].pop("member_ids"), "member IDs"),
        (
            lambda value: value["data"].update(
                {"member_ids": list(reversed(value["data"]["member_ids"]))}
            ),
            "member count",
        ),
        (
            lambda value: value["data"].update({"membership_hash": "0" * 64}),
            "membership hash",
        ),
        (lambda value: value.update({"receipt_id": ""}), "receipt identity"),
        (lambda value: value.update({"command_type": "CREATE_PACKAGE"}), "receipt identity"),
        (lambda value: value.update({"event_ids": [""]}), "receipt identity"),
        (lambda value: value.update({"outbox_ids": [""]}), "receipt identity"),
    )
    for mutate, expected in mutations:
        changed = json.loads(json.dumps(valid))
        mutate(changed)
        with pytest.raises(PackageLogisticsError, match=expected):
            PackageCancellationOutboxProcessor._validate_receipt(saved, changed)

    bad_command_row = dict(saved)
    bad_command = json.loads(bad_command_row["command_json"])
    bad_command["idempotency_key"] = "different-key"
    bad_command_row["command_json"] = json.dumps(bad_command)
    with pytest.raises(PackageLogisticsError, match="command identity"):
        PackageCancellationOutboxProcessor._validate_receipt(
            bad_command_row, valid
        )


def test_cancellation_receipt_membership_must_match_linked_create_receipt(tmp_path):
    draft = _draft()
    db_path = tmp_path / "cancel-linked-create-members.sqlite3"
    package_outbox = PackageOutbox(db_path)
    _ack_package_creation(package_outbox, draft)
    replacement_members = ("unit-a", "unit-b", "unit-c", "unit-z")

    cancellation_outbox = PackageCancellationOutbox(db_path)
    intent = _cancellation_intent(draft)
    cancellation_outbox.enqueue(intent)
    cancellation_outbox.mark_local_event_committed(intent.cancellation_event_id)
    row = cancellation_outbox.claim_next()
    client = PackageLogisticsClient(
        PackageClientConfig("https://logistics.test", "token", SCOPE, "host", "device"),
        transport=lambda *args: (_ for _ in ()).throw(AssertionError("unexpected transport")),
    )
    command = client.build_cancel_package_command(
        intent, row, idempotency_key=row["idempotency_key"]
    )
    cancellation_outbox.save_command(row["idempotency_key"], command)
    saved = cancellation_outbox.get_by_event_id(intent.cancellation_event_id)
    changed_cancellation_receipt = _cancellation_receipt(draft, intent=intent)
    changed_cancellation_receipt["data"]["member_ids"] = list(replacement_members)
    changed_cancellation_receipt["data"]["member_count"] = len(replacement_members)
    changed_cancellation_receipt["data"]["membership_hash"] = membership_hash(
        replacement_members
    )
    with pytest.raises(PackageLogisticsError, match="does not match linked"):
        PackageCancellationOutboxProcessor._validate_receipt(
            saved, changed_cancellation_receipt
        )


@pytest.mark.parametrize(
    "damage",
    [
        "missing",
        "identity",
        "membership",
        "contract_version",
        "command_type",
        "status",
        "authority_scope_id",
        "authority_epoch",
        "resolved_ledger_plane",
        "resolved_plane_epoch",
        "committed_at",
        "event_ids",
        "outbox_ids",
        "saved_command_authority_scope_id",
        "saved_command_idempotency_key",
    ],
)
def test_invalid_linked_create_receipt_blocks_cancel_before_any_client_call(
    tmp_path, damage
):
    draft = _draft()
    db_path = tmp_path / f"cancel-preflight-{damage}.sqlite3"
    package_outbox = PackageOutbox(db_path)
    package_row = _ack_package_creation(package_outbox, draft)
    cancellation_outbox = PackageCancellationOutbox(db_path)
    intent = _cancellation_intent(draft)
    cancellation_outbox.enqueue(intent)
    cancellation_outbox.mark_local_event_committed(intent.cancellation_event_id)

    changed = _receipt(draft)
    changed_command = None
    if damage == "missing":
        encoded = None
    elif damage == "identity":
        changed["receipt_id"] = ""
        encoded = json.dumps(changed)
    elif damage == "membership":
        replacement_members = ("unit-a", "unit-b", "unit-c", "unit-z")
        changed["data"]["member_ids"] = list(replacement_members)
        changed["data"]["member_count"] = len(replacement_members)
        changed["data"]["membership_hash"] = membership_hash(replacement_members)
        encoded = json.dumps(changed)
    else:
        encoded = json.dumps(changed)
    if damage in {
        "contract_version",
        "command_type",
        "status",
        "authority_scope_id",
        "authority_epoch",
        "resolved_ledger_plane",
        "resolved_plane_epoch",
        "committed_at",
        "event_ids",
        "outbox_ids",
    }:
        invalid_values = {
            "contract_version": "wrong-contract",
            "command_type": "CANCEL_PACKAGE",
            "status": "REJECTED",
            "authority_scope_id": "wrong-scope",
            "authority_epoch": 999,
            "resolved_ledger_plane": "WRONG",
            "resolved_plane_epoch": 999,
            "committed_at": "",
            "event_ids": [],
            "outbox_ids": [],
        }
        changed[damage] = invalid_values[damage]
        encoded = json.dumps(changed)
    elif damage == "saved_command_authority_scope_id":
        changed_command = json.loads(package_row["command_json"])
        changed_command["authority_scope_id"] = "wrong-scope"
        encoded = json.dumps(changed)
    elif damage == "saved_command_idempotency_key":
        changed_command = json.loads(package_row["command_json"])
        changed_command["idempotency_key"] = "wrong-key"
        encoded = json.dumps(changed)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "UPDATE package_command_outbox SET receipt_json=? WHERE idempotency_key=?",
            (encoded, package_row["idempotency_key"]),
        )
        if changed_command is not None:
            conn.execute(
                "UPDATE package_command_outbox SET command_json=? WHERE idempotency_key=?",
                (json.dumps(changed_command), package_row["idempotency_key"]),
            )
        conn.commit()
    finally:
        conn.close()

    class ForbiddenClient:
        def build_cancel_package_command(self, *args, **kwargs):
            raise AssertionError("invalid CREATE evidence must block command build")

        def get_receipt_if_exists(self, *args, **kwargs):
            raise AssertionError("invalid CREATE evidence must block receipt lookup")

        def cancel_package(self, *args, **kwargs):
            raise AssertionError("invalid CREATE evidence must block cancel POST")

    result = PackageCancellationOutboxProcessor(
        cancellation_outbox, ForbiddenClient()
    ).drain(limit=1)
    row = cancellation_outbox.get_by_event_id(intent.cancellation_event_id)
    assert result == {"acked": 0, "retry": 0, "conflict": 1, "deferred": 0}
    assert row["status"] == "CONFLICT"


def test_startup_reconciles_prelogged_cancellation_intent_once_after_crash(tmp_path):
    draft = _draft()
    db_path = tmp_path / "package_logistics_outbox.sqlite3"
    package_outbox = PackageOutbox(db_path)
    _ack_package_creation(package_outbox, draft)
    cancellation_outbox = PackageCancellationOutbox(db_path)
    manager = label_module.DataManager(str(tmp_path), "포장실", "tester", "PC-CANCEL")
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app.save_directory = str(tmp_path)
    app.unique_id = "PC-CANCEL"
    app.data_manager = manager
    app.package_outbox = package_outbox
    app.package_cancellation_outbox = cancellation_outbox
    local_details = {
        "cancelled_set_id": draft.set_id,
        "cancelled_by_label": "PHS-CANCEL",
        "details": {"set_id": draft.set_id, "final_result": "통과"},
    }
    cancellation = label_module.Label_Match._queue_authoritative_package_cancellation(
        app,
        set_id=draft.set_id,
        event_type=label_module.Label_Match.Events.TRAY_COMPLETION_CANCELLED,
        reason="LOCAL_TRAY_COMPLETION_CANCELLED",
        evidence={"cancelled_by_label": "PHS-CANCEL"},
        local_event_details=local_details,
    )
    row = cancellation_outbox.get_by_event_id(cancellation["cancellation_event_id"])
    assert row["local_event_committed"] == 0
    assert cancellation_outbox.claim_next() is None
    manager.close(timeout=5)

    restarted_manager = label_module.DataManager(
        str(tmp_path), "포장실", "tester", "PC-CANCEL"
    )
    restarted = label_module.Label_Match.__new__(label_module.Label_Match)
    restarted.save_directory = str(tmp_path)
    restarted.unique_id = "PC-CANCEL"
    restarted.data_manager = restarted_manager
    restarted.package_cancellation_outbox = PackageCancellationOutbox(db_path)
    assert label_module.Label_Match._reconcile_package_cancellation_local_events(restarted) == 1
    restarted_manager.close(timeout=5)
    committed = restarted.package_cancellation_outbox.get_by_event_id(
        cancellation["cancellation_event_id"]
    )
    assert committed["local_event_committed"] == 1

    # Simulate a crash after CSV flush but before the SQLite committed flag.
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """UPDATE package_cancellation_outbox
                  SET local_event_committed=0,local_event_committed_at=NULL
                WHERE cancellation_event_id=?""",
            (cancellation["cancellation_event_id"],),
        )
        conn.commit()
    finally:
        conn.close()
    replay_manager = label_module.DataManager(
        str(tmp_path), "포장실", "tester", "PC-CANCEL"
    )
    replay = label_module.Label_Match.__new__(label_module.Label_Match)
    replay.save_directory = str(tmp_path)
    replay.unique_id = "PC-CANCEL"
    replay.data_manager = replay_manager
    replay.package_cancellation_outbox = PackageCancellationOutbox(db_path)
    assert label_module.Label_Match._reconcile_package_cancellation_local_events(replay) == 1
    replay_manager.close(timeout=5)

    log_path = replay_manager._get_log_filepath()
    with open(log_path, "r", encoding="utf-8-sig", newline="") as handle:
        matching = [
            json.loads(record["details"])
            for record in csv.DictReader(handle)
            if record["event"]
            == label_module.Label_Match.Events.TRAY_COMPLETION_CANCELLED
        ]
    assert [
        details["cancellation_event_id"] for details in matching
    ] == [cancellation["cancellation_event_id"]]


@pytest.mark.parametrize(
    "event_type,reason",
    [
        (
            label_module.Label_Match.Events.SET_DELETED,
            "LOCAL_COMPLETED_SET_DELETED",
        ),
        (
            label_module.Label_Match.Events.TRAY_COMPLETION_CANCELLED,
            "LOCAL_TRAY_COMPLETION_CANCELLED",
        ),
    ],
)
def test_cancellation_fsync_failure_does_not_commit_local_event(
    tmp_path, monkeypatch, event_type, reason
):
    draft = _draft()
    db_path = tmp_path / "package_logistics_outbox.sqlite3"
    package_outbox = PackageOutbox(db_path)
    _ack_package_creation(package_outbox, draft)
    cancellation_outbox = PackageCancellationOutbox(db_path)
    manager = label_module.DataManager(
        str(tmp_path), "포장실", "tester", "PC-CANCEL-FSYNC"
    )
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app.save_directory = str(tmp_path)
    app.unique_id = "PC-CANCEL-FSYNC"
    app.data_manager = manager
    app.package_outbox = package_outbox
    app.package_cancellation_outbox = cancellation_outbox
    local_details = {
        "cancelled_set_id": draft.set_id,
        "details": {"set_id": draft.set_id, "final_result": "통과"},
    }
    cancellation = label_module.Label_Match._queue_authoritative_package_cancellation(
        app,
        set_id=draft.set_id,
        event_type=event_type,
        reason=reason,
        local_event_details=local_details,
    )

    def fail_fsync(_file_descriptor):
        raise OSError("simulated cancellation fsync failure")

    monkeypatch.setattr(label_module.os, "fsync", fail_fsync)
    with pytest.raises(RuntimeError, match="simulated cancellation fsync failure"):
        label_module.Label_Match._commit_package_cancellation_local_event(
            app,
            event_type=event_type,
            local_event_details=local_details,
            cancellation=cancellation,
        )

    row = cancellation_outbox.get_by_event_id(
        cancellation["cancellation_event_id"]
    )
    assert row["local_event_committed"] == 0
    assert row["local_event_committed_at"] is None
    assert cancellation_outbox.claim_next() is None
    with pytest.raises(RuntimeError, match="simulated cancellation fsync failure"):
        manager.close(timeout=5)


def test_dynamic_qr_scope_builds_inherit_command_without_sample_membership():
    calls = []

    def transport(method, url, headers, body, timeout):
        calls.append((method, url, dict(headers), body))
        return {"ok": True, "data": _projection()}

    client = PackageLogisticsClient(
        PackageClientConfig("https://logistics.test", "token", "", "host", "device"),
        transport=transport,
    )
    source_id, command = client.build_create_package_command(_draft(), idempotency_key="package-key")
    assert source_id == TRANSFER
    assert f"/bundles/{SCOPE}/{TRANSFER}" in calls[0][1]
    assert calls[0][2]["User-Agent"] == package_module.PACKAGE_HTTP_USER_AGENT
    assert calls[0][2]["X-KMTech-Client"] == package_module.PACKAGE_HTTP_CLIENT_HEADER
    assert "python-urllib" not in calls[0][2]["User-Agent"].lower()
    assert command["authority_scope_id"] == SCOPE
    assert command["expected_versions"] == {f"bundle:{TRANSFER}": 7}
    assert command["payload"]["sample_barcodes"] == list(BARCODES[:3])
    assert "member_ids" not in command["payload"]
    assert "membership_hash" not in command["payload"]
    assert "exact_rescan_barcodes" not in command["payload"]
    assert "barcode_membership_hash" not in command["payload"]


def test_phs_reconciliation_machine_routes_send_only_exact_action_contract():
    calls = []
    scan_payload = (
        "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITAG-001|"
        "CLC=AAA2270730200|LBL=LBL-001|HSH=aaaaaaaaaaaaaaaa"
    )

    def transport(method, url, headers, body, timeout):
        calls.append((method, url, dict(headers), body))
        return {"ok": True, "data": {"status": "ok"}}

    client = PackageLogisticsClient(
        PackageClientConfig(
            "https://logistics.test",
            "token",
            SCOPE,
            "host",
            "device",
        ),
        transport=transport,
    )

    client.resolve_phs_reconciliation_actions(
        authority_scope_id=SCOPE,
        scan_payload=scan_payload,
        process_context="packaging",
        limit=20,
    )
    client.prepare_phs_reconciliation_label_exchange(
        "PHSR-001",
        authority_scope_id=SCOPE,
        action_ids=["PHSA-2", "PHSA-1"],
        expected_reconciliation_version=7,
        idempotency_key="reconciliation-prepare-001",
    )

    assert calls[0][0] == "GET"
    assert calls[0][1].startswith(
        "https://logistics.test/logistics/api/v1/"
        "phs-work-reconciliations/actions/resolve?"
    )
    assert parse_qs(urlsplit(calls[0][1]).query) == {
        "authority_scope_id": [SCOPE],
        "scan_payload": [scan_payload],
        "process_context": ["packaging"],
        "limit": ["20"],
    }
    assert calls[1][0] == "POST"
    assert calls[1][1].endswith(
        "/phs-work-reconciliations/PHSR-001/label-exchange/prepare"
    )
    assert calls[1][2]["Idempotency-Key"] == "reconciliation-prepare-001"
    assert json.loads(calls[1][3].decode("utf-8")) == {
        "authority_scope_id": SCOPE,
        "action_ids": ["PHSA-2", "PHSA-1"],
        "expected_reconciliation_version": 7,
    }


def test_production_transport_has_no_test1_ack_loss_hook():
    source = Path(package_module.__file__).read_text(encoding="utf-8")

    assert "KMTECH_TEST1_DROP_" not in source


@pytest.mark.parametrize(
    "method,kwargs",
    [
        (
            "resolve",
            {
                "authority_scope_id": SCOPE,
                "scan_payload": "PHS2",
                "process_context": "inspection",
                "limit": 20,
            },
        ),
        (
            "resolve",
            {
                "authority_scope_id": SCOPE,
                "scan_payload": "PHS2",
                "process_context": "packaging",
                "limit": 21,
            },
        ),
        (
            "prepare",
            {
                "authority_scope_id": SCOPE,
                "action_ids": ["PHSA-1", "PHSA-1"],
                "expected_reconciliation_version": 7,
                "idempotency_key": "key",
            },
        ),
    ],
)
def test_phs_reconciliation_client_rejects_wrong_process_or_selection(
    method,
    kwargs,
):
    client = PackageLogisticsClient(
        PackageClientConfig(
            "https://logistics.test",
            "token",
            SCOPE,
            "host",
            "device",
        ),
        transport=lambda *_args: pytest.fail(
            "invalid request must not reach transport"
        ),
    )

    with pytest.raises(PackageLogisticsError):
        if method == "resolve":
            client.resolve_phs_reconciliation_actions(**kwargs)
        else:
            client.prepare_phs_reconciliation_label_exchange(
                "PHSR-001",
                **kwargs,
            )


def test_client_lost_ack_recovers_receipt_in_command_scope():
    calls = []
    draft = _draft()
    receipt = _receipt(draft)

    def transport(method, url, headers, body, timeout):
        calls.append((method, url, dict(headers), body))
        if method == "POST":
            raise PackageTransportError("lost ACK")
        return {"ok": True, "data": receipt}

    client = PackageLogisticsClient(
        PackageClientConfig("https://logistics.test", "token", "", "host", "device"),
        transport=transport,
    )
    command = {
        "authority_scope_id": SCOPE,
        "idempotency_key": "lost-ack-package",
        "payload": {"source_bundle_id": TRANSFER, "package_bundle_id": draft.package_bundle_id},
    }
    assert client.create_package(command) == receipt
    assert calls[0][0] == "POST"
    assert calls[1][0] == "GET"
    assert calls[1][1].endswith(f"/receipts/{SCOPE}/lost-ack-package")
    for _method, _url, headers, _body in calls:
        assert headers["User-Agent"] == package_module.PACKAGE_HTTP_USER_AGENT
        assert headers["X-KMTech-Client"] == package_module.PACKAGE_HTTP_CLIENT_HEADER
        assert "python-urllib" not in headers["User-Agent"].lower()
    assert calls[0][2]["Idempotency-Key"] == "lost-ack-package"
    assert "Idempotency-Key" not in calls[1][2]


def test_committed_create_error_recovers_receipt_without_reposting():
    calls = []
    draft = _draft()
    receipt = _receipt(draft)

    def transport(method, url, headers, body, timeout):
        calls.append((method, url))
        if method == "POST":
            return {
                "ok": False,
                "error": {
                    "status_code": 409,
                    "code": "COMMITTED_RESPONSE_LOST",
                    "message": "command committed; fetch receipt",
                    "committed": True,
                    "retryable": False,
                },
            }
        return {"ok": True, "data": receipt}

    client = PackageLogisticsClient(
        PackageClientConfig("https://logistics.test", "token", SCOPE, "host", "device"),
        transport=transport,
    )
    command = {
        "authority_scope_id": SCOPE,
        "idempotency_key": "committed-create",
        "payload": {
            "source_bundle_id": TRANSFER,
            "package_bundle_id": draft.package_bundle_id,
            "source_evidence": _source_evidence(),
        },
    }

    assert client.create_package(command) == receipt
    assert [method for method, _url in calls] == ["POST", "GET"]


def test_legacy_exact_rescan_resolver_uses_package_source_lineage_role():
    calls = []

    def transport(method, url, headers, body, timeout):
        calls.append((url, dict(headers)))
        if "/bundles/resolve?" in url:
            query = parse_qs(urlsplit(url).query, keep_blank_values=True)
            assert query == {
                "input_tag_id": ["ITG-LEGACY-RESOLVE"],
                "item_id": ["ITEM000000001"],
                "authority_scope_id": [SCOPE],
                "bundle_role": ["PACKAGE_SOURCE"],
                "member_count": ["4"],
                "barcode_membership_hash": [barcode_membership_hash(BARCODES)],
            }
            return {"ok": True, "data": _resolved_projection()}
        return {"ok": True, "data": _projection()}

    client = PackageLogisticsClient(
        PackageClientConfig("https://logistics.test", "token", SCOPE, "host", "device"),
        transport=transport,
    )
    draft = PackageCommandDraft.build(
        set_id="SET-LEGACY-RESOLVE",
        item_code="ITEM000000001",
        source_external_label="LEGACY-PHS-LABEL",
        source_input_tag_id="ITG-LEGACY-RESOLVE",
        external_label="FINAL-LABEL",
        membership_mode="EXACT_RESCAN",
        sample_barcodes=BARCODES[:3],
        exact_rescan_barcodes=BARCODES,
    )
    source_id, command = client.build_create_package_command(draft, idempotency_key="legacy-resolve")
    assert source_id == TRANSFER
    assert len(calls) == 2
    for _url, headers in calls:
        assert headers["User-Agent"] == package_module.PACKAGE_HTTP_USER_AGENT
        assert headers["X-KMTech-Client"] == package_module.PACKAGE_HTTP_CLIENT_HEADER
    assert command["payload"]["member_ids"] == list(UNITS)
    assert command["payload"]["exact_rescan_barcodes"] == list(BARCODES)
    assert command["payload"]["barcode_membership_hash"] == barcode_membership_hash(BARCODES)


def test_default_transport_preserves_explicit_client_identity(monkeypatch):
    captured = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def read(self):
            return json.dumps({"ok": True, "data": _projection()}).encode("utf-8")

    def fake_urlopen(request, timeout):
        captured["headers"] = {key.lower(): value for key, value in request.header_items()}
        captured["timeout"] = timeout
        return Response()

    monkeypatch.setattr(package_module, "urlopen", fake_urlopen)
    client = PackageLogisticsClient(
        PackageClientConfig("https://logistics.test", "token", SCOPE, "host", "device")
    )

    assert client.get_bundle(TRANSFER)["bundle_id"] == TRANSFER
    assert captured["headers"]["user-agent"] == package_module.PACKAGE_HTTP_USER_AGENT
    assert captured["headers"]["x-kmtech-client"] == package_module.PACKAGE_HTTP_CLIENT_HEADER
    assert "python-urllib" not in captured["headers"]["user-agent"].lower()
    assert captured["timeout"] == 8.0


def test_default_transport_uses_profile_private_ca_context(monkeypatch, tmp_path):
    captured = {}
    ca_bundle = tmp_path / "profile" / "tls" / "ca-bundle.pem"
    ca_bundle.parent.mkdir(parents=True)
    ca_bundle.write_bytes(b"private-ca-fixture")
    sentinel_context = object()

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def read(self):
            return json.dumps({"ok": True, "data": _projection()}).encode("utf-8")

    monkeypatch.setattr(
        package_module.ssl,
        "create_default_context",
        lambda *, cafile: captured.update(cafile=cafile) or sentinel_context,
    )

    def fake_urlopen(request, **kwargs):
        captured["open_kwargs"] = kwargs
        return Response()

    monkeypatch.setattr(package_module, "urlopen", fake_urlopen)
    client = PackageLogisticsClient(
        PackageClientConfig(
            "https://logistics.test",
            "token",
            SCOPE,
            "host",
            "device",
            tls_ca_bundle_path=str(ca_bundle),
        )
    )

    assert client.get_bundle(TRANSFER)["bundle_id"] == TRANSFER
    assert captured["cafile"] == str(ca_bundle)
    assert captured["open_kwargs"]["context"] is sentinel_context


def test_minimal_itg_only_identity_resolves_without_raw_external_label():
    def transport(method, url, headers, body, timeout):
        if "/bundles/resolve?" in url:
            query = parse_qs(urlsplit(url).query, keep_blank_values=True)
            assert query["input_tag_id"] == ["ITG-ONLY-RESOLVE"]
            assert "external_label" not in query
            assert query["item_id"] == ["ITEM000000001"]
            assert query["authority_scope_id"] == [SCOPE]
            assert query["bundle_role"] == ["PACKAGE_SOURCE"]
            assert query["member_count"] == ["4"]
            assert query["barcode_membership_hash"] == [barcode_membership_hash(BARCODES)]
            return {"ok": True, "data": _resolved_projection()}
        return {"ok": True, "data": _projection()}

    client = PackageLogisticsClient(
        PackageClientConfig("https://logistics.test", "token", SCOPE, "host", "device"),
        transport=transport,
    )
    draft = PackageCommandDraft.build(
        set_id="SET-ITG-ONLY-RESOLVE",
        item_code="ITEM000000001",
        source_input_tag_id="ITG-ONLY-RESOLVE",
        external_label="FINAL-LABEL",
        membership_mode="EXACT_RESCAN",
        sample_barcodes=BARCODES[:3],
        exact_rescan_barcodes=BARCODES,
    )
    source_id, _command = client.build_create_package_command(
        draft, idempotency_key="itg-only-resolve"
    )
    assert source_id == TRANSFER


def test_original_phs_itg_resolves_one_transfer_and_inherits_exact_server_membership():
    calls = []

    def transport(method, url, headers, body, timeout):
        calls.append((method, url, body))
        if "/bundles/resolve?" in url:
            query = parse_qs(urlsplit(url).query)
            assert query["input_tag_id"] == ["ITG-PHS-INHERIT"]
            assert query["input_tag_label_id"] == ["LBL-PHS-INHERIT"]
            assert query["input_tag_hash_prefix"] == ["0123456789abcdef"]
            assert query["bundle_role"] == ["PACKAGE_SOURCE"]
            assert "external_label" not in query
            return {"ok": True, "data": _resolved_projection()}
        return {"ok": True, "data": _projection()}

    client = PackageLogisticsClient(
        PackageClientConfig("https://logistics.test", "token", SCOPE, "host", "device"),
        transport=transport,
    )
    draft = PackageCommandDraft.build(
        set_id="SET-PHS-INHERIT-COMMAND",
        item_code="ITEM000000001",
        source_input_tag_id="ITG-PHS-INHERIT",
        source_input_tag_label_id="LBL-PHS-INHERIT",
        source_input_tag_hash_prefix="0123456789abcdef",
        external_label="FINAL-LABEL",
        membership_mode="INHERIT_ALL",
        sample_barcodes=BARCODES[:3],
    )

    source_id, command = client.build_create_package_command(
        draft, idempotency_key="phs-inherit-command"
    )

    assert source_id == TRANSFER
    assert len(calls) == 2
    assert command["expected_versions"] == {f"bundle:{TRANSFER}": 7}
    assert command["payload"]["source_evidence"] == _source_evidence()
    assert "member_ids" not in command["payload"]


@pytest.mark.parametrize(
    "mutation", ["top_level", "missing_candidate_count", "partial", "duplicate_barcode"]
)
def test_package_source_resolver_rejects_ambiguous_or_partial_projection(mutation):
    source = {**_projection(), "bundle_role": "PACKAGE_SOURCE"}
    if mutation == "partial":
        source["members"] = source["members"][:-1]
    elif mutation == "duplicate_barcode":
        source["members"][1]["normalized_barcode"] = source["members"][0][
            "normalized_barcode"
        ]
    if mutation == "top_level":
        response = source
    elif mutation == "missing_candidate_count":
        response = {"bundle": source}
    else:
        response = {"candidate_count": 1, "bundle": source}
    calls = []

    def transport(method, url, headers, body, timeout):
        calls.append(url)
        if "/bundles/resolve?" in url:
            return {"ok": True, "data": response}
        raise AssertionError("invalid resolver projection must fail before bundle GET")

    client = PackageLogisticsClient(
        PackageClientConfig("https://logistics.test", "token", SCOPE, "host", "device"),
        transport=transport,
    )
    draft = PackageCommandDraft.build(
        set_id=f"SET-INVALID-{mutation}",
        item_code="ITEM000000001",
        source_input_tag_id="ITG-INVALID",
        external_label="FINAL-LABEL",
        membership_mode="INHERIT_ALL",
        sample_barcodes=BARCODES[:3],
    )

    with pytest.raises(PackageLogisticsError):
        client.build_create_package_command(
            draft, idempotency_key=f"invalid-{mutation}"
        )
    assert len(calls) == 1


def test_inherit_receipt_must_echo_the_immutable_source_evidence():
    client = PackageLogisticsClient(
        PackageClientConfig("https://logistics.test", "token", SCOPE, "host", "device"),
        transport=lambda *args: {"ok": True, "data": _projection()},
    )
    draft = _draft()
    source_id, command = client.build_create_package_command(
        draft, idempotency_key="source-evidence-receipt"
    )
    receipt = _receipt(draft)
    PackageOutboxProcessor._validate_receipt(
        draft, source_id, receipt, command=command
    )
    receipt["data"]["source_evidence"]["member_ids"] = list(UNITS[:-1])
    with pytest.raises(PackageLogisticsError, match="source evidence"):
        PackageOutboxProcessor._validate_receipt(
            draft, source_id, receipt, command=command
        )


def test_producer_ingest_accepted_receipt_is_not_a_package_logistics_receipt():
    draft = _draft()
    producer_receipt = {
        "request_id": "producer-ingest-request",
        "client_batch_id": "relay-label-1",
        "server_source_file_id": (
            "label-host/label_match/label_match_events/legacy_csv/file.csv"
        ),
        "committed": True,
        "status": "accepted",
        "retryable": False,
        "next_retry_after": None,
        "totals": {
            "inserted": 1,
            "replayed": 0,
            "quarantined": 0,
            "errors": 0,
        },
    }

    with pytest.raises(PackageLogisticsError):
        PackageOutboxProcessor._validate_receipt(
            draft, TRANSFER, producer_receipt
        )


def test_exact_rescan_receipt_count_hash_and_membership_are_fail_closed():
    draft = _draft(mode="EXACT_RESCAN", exact=BARCODES)
    receipt = _receipt(draft)
    PackageOutboxProcessor._validate_receipt(draft, TRANSFER, receipt)
    for field, value, expected in (
        ("exact_rescan_count", 3, "count"),
        ("barcode_membership_hash", "0" * 64, "barcode membership hash"),
        ("exact_rescan_barcodes", list(BARCODES[:-1]), "membership"),
    ):
        changed = json.loads(json.dumps(receipt))
        changed["data"][field] = value
        with pytest.raises(PackageLogisticsError, match=expected):
            PackageOutboxProcessor._validate_receipt(draft, TRANSFER, changed)


def test_exact_rescan_command_evidence_is_saved_immutably_in_outbox(tmp_path):
    draft = _draft(mode="EXACT_RESCAN", exact=BARCODES)
    client = PackageLogisticsClient(
        PackageClientConfig("https://logistics.test", "token", SCOPE, "host", "device"),
        transport=lambda *args: {"ok": True, "data": _projection()},
    )
    outbox = PackageOutbox(tmp_path / "exact-evidence.sqlite3")
    row = outbox.enqueue(draft)
    outbox.mark_local_completion_committed(row["idempotency_key"])
    outbox.claim_next()
    source_id, command = client.build_create_package_command(
        draft, idempotency_key=row["idempotency_key"]
    )
    outbox.save_command(row["idempotency_key"], source_id, command)
    saved = json.loads(outbox.get_by_set_id(draft.set_id)["command_json"])
    assert saved["payload"]["exact_rescan_barcodes"] == list(BARCODES)
    assert saved["payload"]["barcode_membership_hash"] == barcode_membership_hash(BARCODES)
    changed = json.loads(json.dumps(saved))
    changed["payload"]["exact_rescan_barcodes"] = list(BARCODES[:-1])
    with pytest.raises(PackageLogisticsError, match="immutable"):
        outbox.save_command(row["idempotency_key"], source_id, changed)


def test_qr_projection_quantity_hash_item_and_scope_mismatches_fail_closed():
    projection = _projection()
    for field, value, expected in (
        ("membership_hash", "0" * 64, "membership hash"),
        ("item_id", "OTHER", "item"),
        ("authority_scope_id", "OTHER", "scope"),
    ):
        changed = {**projection, field: value}

        def transport(method, url, headers, body, timeout, response=changed):
            return {"ok": True, "data": response}

        client = PackageLogisticsClient(
            PackageClientConfig("https://logistics.test", "token", "", "host", "device"),
            transport=transport,
        )
        with pytest.raises(PackageLogisticsError, match=expected):
            client.build_create_package_command(_draft(), idempotency_key=f"bad-{field}")
    client = PackageLogisticsClient(
        PackageClientConfig("https://logistics.test", "token", "", "host", "device"),
        transport=lambda *args: {"ok": True, "data": projection},
    )
    with pytest.raises(PackageLogisticsError, match="quantity"):
        client.build_create_package_command(
            replace(_draft(), expected_member_count=3), idempotency_key="bad-qr-quantity"
        )


def test_packaging_refuses_stale_seal_qr_after_exact_membership_replacement():
    projection = _projection()
    replacement_units = ["unit-z", *list(UNITS[1:])]
    replacement_barcodes = ["ITEM000000001-Z", *list(BARCODES[1:])]
    projection["member_ids"] = replacement_units
    projection["membership_hash"] = membership_hash(replacement_units)
    projection["barcode_membership_hash"] = barcode_membership_hash(
        replacement_barcodes
    )
    projection["members"] = [
        {"unit_id": unit_id, "normalized_barcode": barcode}
        for unit_id, barcode in zip(
            replacement_units, replacement_barcodes, strict=True
        )
    ]
    client = PackageLogisticsClient(
        PackageClientConfig("https://logistics.test", "token", SCOPE, "host", "device"),
        transport=lambda *args: {"ok": True, "data": projection},
    )

    with pytest.raises(
        PackageLogisticsError, match="membership hash differs from its QR"
    ):
        client.build_create_package_command(
            _draft(), idempotency_key="stale-seal-after-replacement"
        )


class RestartClient:
    def __init__(self, draft, *, receipt=None, lose_ack=False):
        self.draft = draft
        self.receipt = receipt
        self.lose_ack = lose_ack
        self.build_calls = 0
        self.create_calls = 0
        self.commands = []

    def build_create_package_command(self, draft, *, idempotency_key):
        self.build_calls += 1
        return TRANSFER, {
            "authority_scope_id": SCOPE,
            "idempotency_key": idempotency_key,
            "expected_versions": {f"bundle:{TRANSFER}": 7},
            "payload": {
                "source_bundle_id": TRANSFER,
                "package_bundle_id": draft.package_bundle_id,
                "source_evidence": _source_evidence(),
            },
        }

    def create_package(self, command):
        self.create_calls += 1
        self.commands.append(json.loads(json.dumps(command)))
        if self.lose_ack:
            raise PackageTransportError("lost ACK")
        return _receipt(self.draft)

    def get_receipt_if_exists(self, key, *, authority_scope_id):
        return self.receipt


def test_restart_uses_saved_command_and_recovers_server_receipt_without_rebuild(tmp_path):
    draft = _draft()
    outbox = PackageOutbox(tmp_path / "restart.sqlite3")
    queued = outbox.enqueue(draft)
    outbox.mark_local_completion_committed(queued["idempotency_key"])
    first_client = RestartClient(draft, lose_ack=True)
    first = PackageOutboxProcessor(outbox, first_client).drain(limit=1)
    assert first == {"acked": 0, "retry": 1, "conflict": 0}
    pending = outbox.get_by_set_id(draft.set_id)
    saved_command = pending["command_json"]
    assert saved_command
    assert pending["local_completion_committed"] == 1

    restarted = PackageOutbox(tmp_path / "restart.sqlite3")
    recovery_client = RestartClient(draft, receipt=_receipt(draft))
    recovered = PackageOutboxProcessor(restarted, recovery_client).drain(limit=1)
    assert recovered == {"acked": 1, "retry": 0, "conflict": 0}
    assert recovery_client.build_calls == 0
    assert recovery_client.create_calls == 0
    acked = restarted.get_by_set_id(draft.set_id)
    assert acked["command_json"] == saved_command
    assert acked["status"] == "ACKED"
    assert acked["idempotency_key"] == queued["idempotency_key"]
    assert acked["local_completion_committed"] == 1


def test_offline_drain_attempts_multiple_locally_completed_packages_once_each(tmp_path):
    outbox = PackageOutbox(tmp_path / "offline-multiple.sqlite3")
    drafts = [_draft_for_set("SET-OFFLINE-1"), _draft_for_set("SET-OFFLINE-2")]
    keys = []
    for draft in drafts:
        row = outbox.enqueue(draft)
        keys.append(row["idempotency_key"])
        outbox.mark_local_completion_committed(row["idempotency_key"])

    class OfflineClient(RestartClient):
        def __init__(self):
            super().__init__(drafts[0])
            self.attempted_keys = []

        def build_create_package_command(self, draft, *, idempotency_key):
            self.attempted_keys.append(idempotency_key)
            return super().build_create_package_command(
                draft, idempotency_key=idempotency_key
            )

        def create_package(self, command):
            raise PackageTransportError("offline")

    client = OfflineClient()
    result = PackageOutboxProcessor(outbox, client).drain(limit=20)

    assert result == {"acked": 0, "retry": 2, "conflict": 0}
    assert client.attempted_keys == keys
    assert [outbox.get_by_set_id(draft.set_id)["status"] for draft in drafts] == [
        "PENDING",
        "PENDING",
    ]
    assert all(
        outbox.get_by_set_id(draft.set_id)["local_completion_committed"] == 1
        for draft in drafts
    )


def test_offline_retry_fairness_survives_limit_and_restart_without_starvation(
    tmp_path,
):
    db_path = tmp_path / "offline-fairness.sqlite3"
    outbox = PackageOutbox(db_path)
    drafts = [_draft_for_set(f"SET-OFFLINE-{index:02d}") for index in range(21)]
    keys = []
    for draft in drafts:
        row = outbox.enqueue(draft)
        keys.append(row["idempotency_key"])
        outbox.mark_local_completion_committed(row["idempotency_key"])

    class OfflineClient:
        def __init__(self):
            self.attempted_keys = []

        @staticmethod
        def build_create_package_command(draft, *, idempotency_key):
            return TRANSFER, {
                "authority_scope_id": SCOPE,
                "idempotency_key": idempotency_key,
                "payload": {"package_bundle_id": draft.package_bundle_id},
            }

        @staticmethod
        def get_receipt_if_exists(key, *, authority_scope_id):
            return None

        def create_package(self, command):
            self.attempted_keys.append(command["idempotency_key"])
            raise PackageTransportError("offline")

    client = OfflineClient()
    first = PackageOutboxProcessor(outbox, client).drain(limit=20)
    first_attempts = list(client.attempted_keys)
    restarted = PackageOutbox(db_path)
    second = PackageOutboxProcessor(restarted, client).drain(limit=20)
    second_attempts = client.attempted_keys[len(first_attempts):]

    assert first == {"acked": 0, "retry": 20, "conflict": 0}
    assert second == {"acked": 0, "retry": 20, "conflict": 0}
    assert first_attempts == keys[:20]
    assert second_attempts[0] == keys[20]
    assert set(keys).issubset(client.attempted_keys)


def test_duplicate_replay_reuses_one_key_and_yields_one_central_effect(tmp_path):
    draft = _draft_for_set("SET-DUPLICATE-REPLAY")
    outbox = PackageOutbox(tmp_path / "duplicate-replay.sqlite3")
    queued = outbox.enqueue(draft)
    outbox.mark_local_completion_committed(queued["idempotency_key"])

    class DeduplicatingServerClient(RestartClient):
        def __init__(self):
            super().__init__(draft)
            self.effects = set()

        def create_package(self, command):
            key = command["idempotency_key"]
            self.commands.append(json.loads(json.dumps(command)))
            self.effects.add(key)
            if len(self.commands) == 1:
                raise PackageTransportError("ACK lost after commit")
            return _receipt(draft)

        def get_receipt_if_exists(self, key, *, authority_scope_id):
            return None

    client = DeduplicatingServerClient()
    assert PackageOutboxProcessor(outbox, client).drain(limit=1)["retry"] == 1
    assert PackageOutboxProcessor(outbox, client).drain(limit=1)["acked"] == 1

    assert [command["idempotency_key"] for command in client.commands] == [
        queued["idempotency_key"],
        queued["idempotency_key"],
    ]
    assert client.effects == {queued["idempotency_key"]}
    assert outbox.get_by_set_id(draft.set_id)["status"] == "ACKED"


def test_saved_command_reposts_identical_payload_when_receipt_not_yet_visible(tmp_path):
    draft = _draft()
    outbox = PackageOutbox(tmp_path / "repost.sqlite3")
    row = outbox.enqueue(draft)
    outbox.mark_local_completion_committed(row["idempotency_key"])
    claimed = outbox.claim_next()
    command = {
        "authority_scope_id": SCOPE,
        "idempotency_key": row["idempotency_key"],
        "payload": {
            "source_bundle_id": TRANSFER,
            "package_bundle_id": draft.package_bundle_id,
            "source_evidence": _source_evidence(),
        },
    }
    outbox.save_command(row["idempotency_key"], TRANSFER, command)
    outbox.mark_retry(row["idempotency_key"], PackageTransportError("restart"))
    client = RestartClient(draft, receipt=None)
    result = PackageOutboxProcessor(PackageOutbox(tmp_path / "repost.sqlite3"), client).drain(limit=1)
    assert result["acked"] == 1
    assert client.build_calls == 0
    assert client.commands == [command]


def test_deterministic_local_validation_is_conflict_not_retry(tmp_path):
    draft = _draft()
    outbox = PackageOutbox(tmp_path / "conflict.sqlite3")
    queued = outbox.enqueue(draft)
    outbox.mark_local_completion_committed(queued["idempotency_key"])

    class InvalidClient(RestartClient):
        def build_create_package_command(self, draft, *, idempotency_key):
            raise PackageLogisticsError("QR quantity mismatch")

    result = PackageOutboxProcessor(outbox, InvalidClient(draft)).drain(limit=1)
    assert result == {"acked": 0, "retry": 0, "conflict": 1}
    conflict = outbox.get_by_set_id(draft.set_id)
    assert conflict["status"] == "CONFLICT"
    assert conflict["review_status"] == "OPERATOR_REVIEW"
    assert conflict["local_completion_committed"] == 1


@pytest.mark.parametrize(
    ("failure_kind", "expected_origin", "expected_code"),
    [
        ("api", "PACKAGE_API_CONFLICT", "STALE_VERSION"),
        (
            "local_validation",
            "LOCAL_VALIDATION_OR_RECEIPT_CONFLICT",
            "LOCAL_VALIDATION_CONFLICT",
        ),
        (
            "receipt_validation",
            "LOCAL_VALIDATION_OR_RECEIPT_CONFLICT",
            "LOCAL_VALIDATION_CONFLICT",
        ),
    ],
)
def test_every_post_local_package_conflict_creates_one_durable_review_case(
    tmp_path,
    failure_kind,
    expected_origin,
    expected_code,
):
    draft = _draft_for_set(f"SET-POST-REVIEW-{failure_kind}")
    db_path = tmp_path / f"post-review-{failure_kind}.sqlite3"
    outbox = PackageOutbox(db_path)
    queued = outbox.enqueue(draft)
    outbox.mark_local_completion_committed(queued["idempotency_key"])

    class ConflictClient(RestartClient):
        def build_create_package_command(self, draft, *, idempotency_key):
            if failure_kind == "local_validation":
                raise PackageLogisticsError("local membership validation failed")
            return super().build_create_package_command(
                draft, idempotency_key=idempotency_key
            )

        def create_package(self, command):
            if failure_kind == "api":
                raise PackageApiError(
                    409,
                    "STALE_VERSION",
                    "source transfer changed",
                    retryable=False,
                    committed=False,
                )
            receipt = super().create_package(command)
            if failure_kind == "receipt_validation":
                receipt["data"]["package_bundle_id"] = "WRONG-PACKAGE"
            return receipt

    result = PackageOutboxProcessor(
        outbox, ConflictClient(draft)
    ).drain(limit=1)

    assert result == {"acked": 0, "retry": 0, "conflict": 1}
    conflict = outbox.get_by_set_id(draft.set_id)
    assert conflict["status"] == "CONFLICT"
    assert conflict["local_completion_committed"] == 1
    pending = outbox.list_post_review_csv_pending()
    assert len(pending) == 1
    event = json.loads(pending[0]["event_json"])
    assert event == {
        "case_id": pending[0]["review_event_id"],
        "case_status": "OPEN",
        "case_type": "PACKAGE_CREATE_POST_LOCAL_CONFLICT",
        "conflict_code": expected_code,
        "conflict_origin": expected_origin,
        "dedupe_key": pending[0]["review_event_id"],
        "event_version": "label-match-post-review-required-v1",
        "local_completion_committed": True,
        "package_bundle_id": draft.package_bundle_id,
        "package_idempotency_key": queued["idempotency_key"],
        "required_at": conflict["updated_at"],
        "review_event_id": pending[0]["review_event_id"],
        "review_status": "OPERATOR_REVIEW",
        "set_id": draft.set_id,
    }

    restarted = PackageOutbox(db_path)
    assert restarted.get_by_set_id(draft.set_id)[
        "local_completion_committed"
    ] == 1
    assert [
        row["review_event_id"]
        for row in restarted.list_post_review_csv_pending()
    ] == [pending[0]["review_event_id"]]
    assert PackageOutboxProcessor(
        restarted, ConflictClient(draft)
    ).drain(limit=1) == {"acked": 0, "retry": 0, "conflict": 0}
    assert len(restarted.list_post_review_csv_pending()) == 1


def test_post_review_projection_is_exact_once_across_instances(tmp_path):
    draft = _draft_for_set("SET-POST-REVIEW-EXACT-ONCE")
    db_path = tmp_path / "post-review-exact-once.sqlite3"
    outbox = PackageOutbox(db_path)
    queued = outbox.enqueue(draft)
    outbox.mark_local_completion_committed(queued["idempotency_key"])

    class InvalidClient(RestartClient):
        def build_create_package_command(self, draft, *, idempotency_key):
            raise PackageLogisticsError("receipt proof cannot be validated")

    assert PackageOutboxProcessor(
        outbox, InvalidClient(draft)
    ).drain(limit=1)["conflict"] == 1
    pending = outbox.list_post_review_csv_pending()
    review_event_id = pending[0]["review_event_id"]
    projected = []
    barrier = threading.Barrier(8)

    def commit(instance):
        barrier.wait()
        return instance.commit_post_review_csv_projection(
            review_event_id,
            lambda saved: projected.append(saved["review_event_id"]),
        )

    instances = [PackageOutbox(db_path) for _ in range(8)]
    results = []
    threads = [
        threading.Thread(
            target=lambda box=box: results.append(commit(box))
        )
        for box in instances
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert all(not thread.is_alive() for thread in threads)
    assert results.count(True) == 1
    assert results.count(False) == 7
    assert projected == [review_event_id]
    assert outbox.get_post_review_event(review_event_id)[
        "local_csv_committed"
    ] == 1


def test_v8_post_local_conflict_is_backfilled_as_pending_review_case(tmp_path):
    draft = _draft_for_set("SET-V8-POST-REVIEW")
    db_path = tmp_path / "v8-post-review-backfill.sqlite3"
    outbox = PackageOutbox(db_path)
    queued = outbox.enqueue(draft)
    outbox.mark_local_completion_committed(queued["idempotency_key"])

    class InvalidClient(RestartClient):
        def build_create_package_command(self, draft, *, idempotency_key):
            raise PackageLogisticsError("legacy receipt proof mismatch")

    assert PackageOutboxProcessor(
        outbox, InvalidClient(draft)
    ).drain(limit=1)["conflict"] == 1
    with sqlite3.connect(db_path) as conn:
        conn.execute("DROP TABLE package_post_review_outbox")
        conn.execute(
            """UPDATE package_outbox_schema_info
                  SET value='label-match-package-outbox-v8'
                WHERE key='schema_version'"""
        )

    restarted = PackageOutbox(db_path)
    pending = restarted.list_post_review_csv_pending()

    assert len(pending) == 1
    event = json.loads(pending[0]["event_json"])
    assert event["package_idempotency_key"] == queued["idempotency_key"]
    assert event["conflict_origin"] == "RECOVERED_EXISTING_CONFLICT"
    assert restarted.get_by_set_id(draft.set_id)[
        "local_completion_committed"
    ] == 1


def test_f1_dismisses_only_local_recovery_and_preserves_conflict_evidence(
    tmp_path,
):
    draft = _draft()
    db_path = tmp_path / "dismiss-recoverable-conflict.sqlite3"
    outbox = PackageOutbox(db_path)
    queued = outbox.enqueue(draft)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """UPDATE package_command_outbox
                  SET status='SENDING',attempt_count=1,updated_at=?
                WHERE idempotency_key=?""",
            (package_module.utc_now(), queued["idempotency_key"]),
        )
    claimed = outbox.get_by_set_id(draft.set_id)
    assert claimed["idempotency_key"] == queued["idempotency_key"]
    command = {
        "command_type": "CREATE_PACKAGE",
        "authority_scope_id": SCOPE,
        "idempotency_key": queued["idempotency_key"],
        "payload": {
            "source_bundle_id": TRANSFER,
            "package_bundle_id": draft.package_bundle_id,
        },
    }
    outbox.save_command(
        queued["idempotency_key"],
        TRANSFER,
        command,
    )

    class PrewriteConflict(Exception):
        code = "PHS_WORK_GROUP_COMMAND_CONFLICT"
        message = "exact preflight differs"

    outbox.mark_conflict(
        queued["idempotency_key"],
        PrewriteConflict(),
    )
    before = outbox.get_by_set_id(draft.set_id)
    assert outbox.list_conflicts(limit=1)[0]["status"] == "CONFLICT"
    assert outbox.list_local_completion_pending(limit=1)
    assert outbox.list_post_review_csv_pending() == []

    dismissed = outbox.dismiss_recoverable_prewrite_conflict(
        queued["idempotency_key"]
    )
    restarted = PackageOutbox(db_path)
    after = restarted.get_by_set_id(draft.set_id)

    assert dismissed["local_recovery_dismissed"] == 1
    assert after["status"] == "CONFLICT"
    assert after["last_error_code"] == before["last_error_code"]
    assert after["command_json"] == before["command_json"]
    assert after["receipt_json"] is None
    assert after["local_completion_committed"] == 0
    assert after["local_recovery_dismissed"] == 1
    assert after["local_recovery_dismissed_at"]
    assert restarted.counts()["CONFLICT"] == 1
    assert restarted.list_conflicts() == []
    assert restarted.list_local_completion_pending() == []
    assert restarted.list_all_conflicts(limit=1)[0]["idempotency_key"] == (
        queued["idempotency_key"]
    )


def _prepare_superseded_prewrite_conflict(db_path):
    outbox = PackageOutbox(db_path)
    stale_draft = _draft_for_set("SET-STALE-CONFLICT")
    stale = outbox.enqueue(stale_draft)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """UPDATE package_command_outbox
                  SET status='SENDING',attempt_count=1,updated_at=?
                WHERE idempotency_key=?""",
            (package_module.utc_now(), stale["idempotency_key"]),
        )
    claimed = outbox.get_by_set_id(stale_draft.set_id)
    assert claimed["idempotency_key"] == stale["idempotency_key"]
    stale_command = {
        "command_type": "CREATE_PACKAGE",
        "authority_scope_id": SCOPE,
        "idempotency_key": stale["idempotency_key"],
        "payload": {
            "source_bundle_id": TRANSFER,
            "package_bundle_id": stale_draft.package_bundle_id,
        },
    }
    outbox.save_command(stale["idempotency_key"], TRANSFER, stale_command)

    class PrewriteConflict(Exception):
        code = "PHS_WORK_GROUP_COMMAND_CONFLICT"
        message = "stale exact preflight"

    outbox.mark_conflict(stale["idempotency_key"], PrewriteConflict())

    completed_draft = _draft_for_set("SET-LATER-COMPLETION")
    completed = _ack_package_creation(outbox, completed_draft)
    outbox.mark_local_completion_committed(completed["idempotency_key"])
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """UPDATE package_command_outbox SET created_at=?
                 WHERE idempotency_key=?""",
            ("2026-07-30T08:00:00Z", stale["idempotency_key"]),
        )
        conn.execute(
            """UPDATE package_command_outbox SET created_at=?
                 WHERE idempotency_key=?""",
            ("2026-07-30T08:05:00Z", completed["idempotency_key"]),
        )
    return outbox, stale, completed


def test_later_completed_source_hides_stale_conflict_without_deleting_evidence(
    tmp_path,
):
    db_path = tmp_path / "superseded-conflict.sqlite3"
    outbox, stale, _completed = _prepare_superseded_prewrite_conflict(db_path)
    before = outbox.get_by_set_id("SET-STALE-CONFLICT")

    assert outbox.dismiss_superseded_recoverable_prewrite_conflicts() == 1
    assert outbox.dismiss_superseded_recoverable_prewrite_conflicts() == 0

    restarted = PackageOutbox(db_path)
    after = restarted.get_by_set_id("SET-STALE-CONFLICT")
    assert after["status"] == "CONFLICT"
    assert after["last_error_code"] == before["last_error_code"]
    assert after["last_error_message"] == before["last_error_message"]
    assert after["command_json"] == before["command_json"]
    assert after["receipt_json"] is None
    assert after["local_completion_committed"] == 0
    assert after["local_recovery_dismissed"] == 1
    assert after["local_recovery_dismissed_at"]
    assert restarted.counts()["CONFLICT"] == 1
    assert restarted.list_conflicts() == []
    assert restarted.list_all_conflicts(limit=1)[0]["idempotency_key"] == (
        stale["idempotency_key"]
    )


@pytest.mark.parametrize(
    "mutation",
    [
        "nonrecoverable_error",
        "missing_stale_source",
        "different_source",
        "stale_has_receipt",
        "stale_local_completion",
        "completed_missing_receipt",
        "completed_local_pending",
        "completed_not_acked",
        "completed_not_newer",
    ],
)
def test_stale_conflict_dismissal_requires_exact_later_completion_evidence(
    tmp_path,
    mutation,
):
    db_path = tmp_path / f"superseded-negative-{mutation}.sqlite3"
    outbox, stale, completed = _prepare_superseded_prewrite_conflict(db_path)
    updates = {
        "nonrecoverable_error": (
            "UPDATE package_command_outbox SET last_error_code=? WHERE idempotency_key=?",
            ("PACKAGE_MEMBERSHIP_CONFLICT", stale["idempotency_key"]),
        ),
        "missing_stale_source": (
            "UPDATE package_command_outbox SET resolved_source_bundle_id=NULL WHERE idempotency_key=?",
            (stale["idempotency_key"],),
        ),
        "different_source": (
            "UPDATE package_command_outbox SET resolved_source_bundle_id=? WHERE idempotency_key=?",
            ("TRANSFER-DIFFERENT", completed["idempotency_key"]),
        ),
        "stale_has_receipt": (
            "UPDATE package_command_outbox SET receipt_json=? WHERE idempotency_key=?",
            ('{"receipt_id":"unexpected"}', stale["idempotency_key"]),
        ),
        "stale_local_completion": (
            "UPDATE package_command_outbox SET local_completion_committed=1 WHERE idempotency_key=?",
            (stale["idempotency_key"],),
        ),
        "completed_missing_receipt": (
            "UPDATE package_command_outbox SET receipt_json=NULL WHERE idempotency_key=?",
            (completed["idempotency_key"],),
        ),
        "completed_local_pending": (
            "UPDATE package_command_outbox SET local_completion_committed=0 WHERE idempotency_key=?",
            (completed["idempotency_key"],),
        ),
        "completed_not_acked": (
            "UPDATE package_command_outbox SET status='PENDING' WHERE idempotency_key=?",
            (completed["idempotency_key"],),
        ),
        "completed_not_newer": (
            "UPDATE package_command_outbox SET created_at=? WHERE idempotency_key=?",
            ("2026-07-30T07:59:59Z", completed["idempotency_key"]),
        ),
    }
    sql, params = updates[mutation]
    with sqlite3.connect(db_path) as conn:
        conn.execute(sql, params)

    assert outbox.dismiss_superseded_recoverable_prewrite_conflicts() == 0
    assert outbox.list_conflicts(limit=1)[0]["idempotency_key"] == (
        stale["idempotency_key"]
    )


def test_create_package_429_waits_until_retry_after_instead_of_conflicting(tmp_path):
    draft = _draft()
    outbox = PackageOutbox(tmp_path / "create-retry-after.sqlite3")
    queued = outbox.enqueue(draft)
    outbox.mark_local_completion_committed(queued["idempotency_key"])

    class ThrottledClient(RestartClient):
        def create_package(self, command):
            raise PackageApiError(
                429,
                "RATE_LIMITED",
                "too many concurrent terminals",
                retryable=True,
                committed=False,
                retry_after_seconds=120,
            )

    result = PackageOutboxProcessor(outbox, ThrottledClient(draft)).drain(limit=1)
    row = outbox.get_by_set_id(draft.set_id)

    assert result == {"acked": 0, "retry": 1, "conflict": 0}
    assert row["status"] == "PENDING"
    assert row["last_error_code"] == "RATE_LIMITED"
    assert row["retry_after_at"]
    assert outbox.claim_next() is None


@pytest.mark.parametrize("status_code", [409, 412])
def test_create_package_cas_conflict_is_terminal_even_if_server_marks_retryable(
    tmp_path, status_code
):
    draft = _draft()
    outbox = PackageOutbox(tmp_path / f"create-cas-{status_code}.sqlite3")
    queued = outbox.enqueue(draft)
    outbox.mark_local_completion_committed(queued["idempotency_key"])

    class ConflictingClient(RestartClient):
        def create_package(self, command):
            raise PackageApiError(
                status_code,
                "STALE_VERSION",
                "source transfer changed",
                retryable=True,
                committed=False,
            )

    result = PackageOutboxProcessor(outbox, ConflictingClient(draft)).drain(limit=1)

    assert result == {"acked": 0, "retry": 0, "conflict": 1}
    assert outbox.get_by_set_id(draft.set_id)["status"] == "CONFLICT"


def test_automated_test_mode_is_completely_silent(monkeypatch):
    monkeypatch.setenv(label_module.LABEL_MATCH_AUTOMATED_TEST_ENV, "1")
    assert label_module._label_match_audio_enabled() is False
    played = []
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app.initialized_successfully = True
    app.run_tests = False
    app.sound_objects = {"pass": type("Sound", (), {"play": lambda self: played.append(True)})()}
    app.sounds = {"pass": "pass.wav"}
    label_module.Label_Match._play_sound(app, "pass")
    assert played == []


def _work_group_seal(bundle_id, version, members, barcode_by_unit, *, suffix):
    token = f"token-{suffix}"
    barcodes = tuple(sorted(barcode_by_unit[unit_id] for unit_id in members))
    return {
        "seal_contract_version": "transfer-seal-qr-v1",
        "seal_state": "ACTIVE",
        "seal_id": f"seal-{suffix}",
        "seal_revision": 1,
        "seal_token": token,
        "seal_token_hash": package_module.hashlib.sha256(
            token.encode("utf-8")
        ).hexdigest(),
        "seal_qr_payload": f"TRF=1|BND={bundle_id}|SID=seal-{suffix}",
        "sealed_bundle_id": bundle_id,
        "sealed_bundle_version": version,
        "sealed_member_ids": list(members),
        "sealed_members": [
            {
                "unit_id": unit_id,
                "normalized_barcode": barcode_by_unit[unit_id],
            }
            for unit_id in members
        ],
        "sealed_member_count": len(members),
        "sealed_membership_hash": membership_hash(members),
        "sealed_normalized_barcodes": list(barcodes),
        "sealed_barcode_membership_hash": barcode_membership_hash(
            barcodes
        ),
    }


def _work_group_response(*, split=False):
    item_id = "ITEM000000001"
    group_id = "PHSG-WORK-PACKAGE"
    label_id = "LBL-WORK-PACKAGE"
    scan_payload = (
        "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-WORK-ONE|"
        f"CLC={item_id}|LBL={label_id}|HSH=aaaaaaaaaaaaaaaa"
    )
    barcode_by_unit = dict(zip(UNITS, BARCODES, strict=True))
    if split:
        selected = UNITS[:2]
        source_specs = [
            {
                "bundle_id": "TRANSFER-WORK-A",
                "entity_version": 7,
                "members": UNITS,
                "selected": selected,
                "session": "ITG-WORK-ONE",
            }
        ]
    else:
        selected = UNITS
        source_specs = [
            {
                "bundle_id": "TRANSFER-WORK-A",
                "entity_version": 7,
                "members": UNITS[:2],
                "selected": UNITS[:2],
                "session": "ITG-WORK-ONE",
            },
            {
                "bundle_id": "TRANSFER-WORK-B",
                "entity_version": 4,
                "members": UNITS[2:],
                "selected": UNITS[2:],
                "session": "ITG-WORK-TWO",
            },
        ]
    covers = []
    sources = []
    for index, spec in enumerate(source_specs):
        source_members = tuple(spec["members"])
        selected_members = tuple(spec["selected"])
        remainder = tuple(
            unit_id
            for unit_id in source_members
            if unit_id not in selected_members
        )
        remainder_id = (
            "TRANSFER-WORK-REMAINDER-"
            + package_module.canonical_sha256(
                {
                    "source_transfer_bundle_id": spec["bundle_id"],
                    "member_ids": list(remainder),
                }
            )[:24].upper()
            if remainder
            else None
        )
        cover_ids = ["PHSG-WORK-COVER"] if remainder else []
        sources.append(
            {
                "bundle_id": spec["bundle_id"],
                "bundle_type": "TRANSFER",
                "bundle_state": "AVAILABLE",
                "entity_version": spec["entity_version"],
                "source_session_id": spec["session"],
                "external_label": f"TRANSFER-LABEL-{index}",
                "accounting_inbound_iin": "IIN-WORK-1",
                "source_member_ids": list(source_members),
                "source_member_count": len(source_members),
                "source_membership_hash": membership_hash(source_members),
                "selected_member_ids": list(selected_members),
                "selected_member_count": len(selected_members),
                "selected_membership_hash": membership_hash(
                    selected_members
                ),
                "remainder_member_ids": list(remainder),
                "remainder_member_count": len(remainder),
                "remainder_membership_hash": (
                    membership_hash(remainder) if remainder else None
                ),
                "remainder_transfer_bundle_id": remainder_id,
                "origin_receipt_id": f"receipt-transfer-{index}",
                "origin_receipt_contract_version": (
                    "PHS_WORK_GROUP_TRANSFER_V1"
                ),
                "active_seal": _work_group_seal(
                    spec["bundle_id"],
                    spec["entity_version"],
                    source_members,
                    barcode_by_unit,
                    suffix=str(index),
                ),
                "remainder_cover_group_ids": cover_ids,
            }
        )
    if split:
        remainder = UNITS[2:]
        covers = [
            {
                "group_id": "PHSG-WORK-COVER",
                "label_id": "LBL-WORK-COVER",
                "scan_payload": (
                    "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-WORK-COVER|"
                    f"CLC={item_id}|LBL=LBL-WORK-COVER|"
                    "HSH=bbbbbbbbbbbbbbbb"
                ),
                "scan_anchor_input_tag_id": "ITG-WORK-COVER",
                "item_id": item_id,
                "uom": "PCS",
                "member_ids": list(remainder),
                "member_count": len(remainder),
                "membership_hash": membership_hash(remainder),
                "covered_member_ids": list(remainder),
                "covered_member_count": len(remainder),
                "covered_membership_hash": membership_hash(remainder),
                "membership_version": 3,
                "label_version": 2,
                "group_entity_version": 6,
                "label_entity_version": 4,
            }
        ]
    group = {
        "group_id": group_id,
        "label_id": label_id,
        "state": "ACTIVE",
        "scan_payload": scan_payload,
        "scan_anchor_input_tag_id": "ITG-WORK-ONE",
        "item_id": item_id,
        "uom": "PCS",
        "member_ids": list(selected),
        "member_count": len(selected),
        "membership_hash": membership_hash(selected),
        "membership_version": 4,
        "label_version": 2,
        "group_entity_version": 7,
        "label_entity_version": 3,
    }
    package_id = (
        "PACKAGE-WORK-"
        + package_module.canonical_sha256(
            {
                "group_id": group_id,
                "label_id": label_id,
                "member_ids": list(selected),
            }
        )[:24].upper()
    )
    versions = {
        f"phs_work_group:{group_id}": 7,
        f"phs_work_membership:{group_id}": 4,
        f"phs_work_label_version:{group_id}": 2,
        f"phs_label:{label_id}": 3,
        **{
            f"bundle:{source['bundle_id']}": source["entity_version"]
            for source in sources
        },
        f"bundle:{package_id}": 0,
    }
    for source in sources:
        if source["remainder_transfer_bundle_id"]:
            versions[f"bundle:{source['remainder_transfer_bundle_id']}"] = 0
    for cover in covers:
        cover_id = cover["group_id"]
        versions.update(
            {
                f"phs_work_group:{cover_id}": cover[
                    "group_entity_version"
                ],
                f"phs_work_membership:{cover_id}": cover[
                    "membership_version"
                ],
                f"phs_work_label_version:{cover_id}": cover[
                    "label_version"
                ],
                f"phs_label:{cover['label_id']}": cover[
                    "label_entity_version"
                ],
            }
        )
    selected_barcodes = tuple(barcode_by_unit[unit_id] for unit_id in selected)
    sessions = sorted({spec["session"] for spec in source_specs})
    work_source = {
        "authority_scope_id": SCOPE,
        "ledger_plane": "AUTHORITATIVE",
        "plane_epoch": 3,
        "item_id": item_id,
        "uom": "PCS",
        "source_iin": "IIN-WORK-1",
        "member_ids": list(selected),
        "member_count": len(selected),
        "membership_hash": membership_hash(selected),
        "barcode_member_count": len(selected),
        "barcode_membership_hash": barcode_membership_hash(
            selected_barcodes
        ),
        "members": [
            {
                "unit_id": unit_id,
                "normalized_barcode": barcode_by_unit[unit_id],
                "inbound_iin": "IIN-WORK-1",
            }
            for unit_id in selected
        ],
        "source_transfers": sources,
        "source_transfer_count": len(sources),
        "source_transfer_bundle_ids": [
            source["bundle_id"] for source in sources
        ],
        "source_session_ids": sessions,
        "package_bundle_id": package_id,
        "package_external_label": package_id,
        "remainder_cover_groups": covers,
        "entity_versions": versions,
    }
    topology_hash = package_module.canonical_sha256(
        {
            "phs_work_group": group,
            "source_transfers": sources,
            "remainder_cover_groups": covers,
            "source_iin": "IIN-WORK-1",
            "barcode_membership_hash": work_source[
                "barcode_membership_hash"
            ],
            "package_bundle_id": package_id,
        }
    )
    work_source["topology_hash"] = topology_hash
    singular = sources[0] if len(sources) == 1 else None
    bundle = {
        "authority_scope_id": SCOPE,
        "mode": "LIVE",
        "authority_epoch": 5,
        "ledger_plane": "AUTHORITATIVE",
        "plane_epoch": 3,
        "bundle_id": singular["bundle_id"] if singular else None,
        "candidate_count": 1,
        "bundle_role": "PACKAGE_SOURCE",
        "bundle_type": "TRANSFER",
        "bundle_state": "AVAILABLE",
        "external_label": scan_payload,
        "source_session_id": sessions[0] if len(sessions) == 1 else None,
        "item_id": item_id,
        "uom": "PCS",
        "source_iin": "IIN-WORK-1",
        "current_location": "TRANSFER",
        "member_ids": list(selected),
        "member_count": len(selected),
        "membership_hash": membership_hash(selected),
        "barcode_member_count": len(selected),
        "barcode_membership_hash": work_source[
            "barcode_membership_hash"
        ],
        "entity_version": singular["entity_version"] if singular else None,
        "entity_versions": versions,
        "members": work_source["members"],
        "active_seal": singular["active_seal"] if singular else None,
        "active_seals": [source["active_seal"] for source in sources],
        "controlled_reseal_eligible": singular is not None,
    }
    return {
        **bundle,
        "source_resolution_basis": (
            "PHS_WORK_GROUP_EXACT_MEMBERSHIP"
        ),
        "phs_work_group": group,
        "work_group_source": work_source,
        "bundle": bundle,
        "topology_hash": topology_hash,
        "entity_versions": versions,
    }


def _work_group_draft(response, *, set_id="SET-WORK-GROUP"):
    source = response["work_group_source"]
    group = response["phs_work_group"]
    return PackageCommandDraft.build(
        set_id=set_id,
        item_code=source["item_id"],
        source_input_tag_id=group["scan_anchor_input_tag_id"],
        source_input_tag_label_id=group["label_id"],
        source_input_tag_hash_prefix="aaaaaaaaaaaaaaaa",
        source_canonical_input_tag_qr=group["scan_payload"],
        source_active_label_qr_payload=group["scan_payload"],
        source_authority_scope_id=source["authority_scope_id"],
        expected_member_count=source["member_count"],
        expected_membership_hash=source["membership_hash"],
        expected_authority_epoch=5,
        expected_ledger_plane=source["ledger_plane"],
        expected_plane_epoch=source["plane_epoch"],
        package_bundle_id=source["package_bundle_id"],
        external_label=source["package_external_label"],
        membership_mode="INHERIT_ALL",
        source_resolution_basis=(
            "PHS_WORK_GROUP_EXACT_MEMBERSHIP"
        ),
        phs_work_group=group,
        work_group_source=source,
        source_session_ids=source["source_session_ids"],
    )


def _work_group_receipt(draft, command):
    source = draft.work_group_source
    group = draft.phs_work_group
    sources = list(source["source_transfers"])
    covers = list(source["remainder_cover_groups"])
    package_id = draft.package_bundle_id
    receipt_id = "receipt-work-group-package"
    selected_rows = [
        {
            "unit_id": row["unit_id"],
            "normalized_barcode": row["normalized_barcode"],
        }
        for row in source["members"]
    ]
    transitions = []
    remainders = []
    remainder_seals = []
    remainder_ids = []
    consumed_seals = []
    root_specs = {
        (group["group_id"], "PACKAGE", package_id),
    }
    for index, source_spec in enumerate(sources):
        source_id = source_spec["bundle_id"]
        before = source_spec["entity_version"]
        transitions.append(
            {
                "source_transfer_bundle_id": source_id,
                "entity_version_before": before,
                "entity_version_after": before + 1,
                "state_before": "AVAILABLE",
                "state_after": "CONSUMED",
                "source_member_ids": source_spec["source_member_ids"],
                "source_member_count": source_spec["source_member_count"],
                "source_membership_hash": source_spec[
                    "source_membership_hash"
                ],
                "selected_member_ids": source_spec["selected_member_ids"],
                "selected_member_count": source_spec[
                    "selected_member_count"
                ],
                "selected_membership_hash": source_spec[
                    "selected_membership_hash"
                ],
                "remainder_transfer_bundle_id": source_spec[
                    "remainder_transfer_bundle_id"
                ],
            }
        )
        consumed_seals.append(
            {**source_spec["active_seal"], "seal_state": "CONSUMED"}
        )
        remainder_id = source_spec["remainder_transfer_bundle_id"]
        if not remainder_id:
            continue
        remainder_members = tuple(source_spec["remainder_member_ids"])
        barcode_by_unit = dict(
            package_module.canonical_member_barcodes(
                source_spec["active_seal"]["sealed_members"]
            )
        )
        remainder_seal = _work_group_seal(
            remainder_id,
            1,
            remainder_members,
            barcode_by_unit,
            suffix=f"remainder-{index}",
        )
        remainder_base = {
            "source_transfer_bundle_id": source_id,
            "remainder_transfer_bundle_id": remainder_id,
            "member_ids": list(remainder_members),
            "members": [
                {
                    "unit_id": unit_id,
                    "normalized_barcode": barcode_by_unit[unit_id],
                }
                for unit_id in remainder_members
            ],
            "member_count": len(remainder_members),
            "membership_hash": membership_hash(remainder_members),
            "entity_version": 1,
        }
        remainders.append({**remainder_base, **remainder_seal})
        remainder_seals.append(remainder_seal)
        remainder_ids.append(remainder_id)
        for cover_id in source_spec["remainder_cover_group_ids"]:
            root_specs.add(
                (cover_id, "TRANSFER_BUNDLE", remainder_id)
            )
    roots = [
        {
            "group_id": group_id,
            "root_type": root_type,
            "root_id": root_id,
            "root_role": "SOURCE",
            "added_receipt_id": receipt_id,
        }
        for group_id, root_type, root_id in sorted(root_specs)
    ]
    group_versions = {
        group["group_id"]: group["group_entity_version"] + 1,
        **{
            cover["group_id"]: cover["group_entity_version"] + 1
            for cover in covers
        },
    }
    topology_after = package_module.canonical_sha256(
        {
            "topology_hash_before": source["topology_hash"],
            "package_bundle_id": package_id,
            "remainder_transfer_bundle_ids": remainder_ids,
            "root_proof": roots,
            "group_entity_versions": group_versions,
        }
    )
    receipt_versions = dict(source["entity_versions"])
    for source_spec in sources:
        receipt_versions[f"bundle:{source_spec['bundle_id']}"] = (
            source_spec["entity_version"] + 1
        )
    receipt_versions[f"bundle:{package_id}"] = 1
    for remainder_id in remainder_ids:
        receipt_versions[f"bundle:{remainder_id}"] = 1
    for group_id, version in group_versions.items():
        receipt_versions[f"phs_work_group:{group_id}"] = version
    return {
        "contract_version": "logistics-v1",
        "receipt_id": receipt_id,
        "command_type": "CREATE_PACKAGE",
        "status": "COMMITTED",
        "authority_scope_id": SCOPE,
        "authority_epoch": 5,
        "resolved_ledger_plane": "AUTHORITATIVE",
        "resolved_plane_epoch": 3,
        "entity_versions": receipt_versions,
        "event_ids": ["event-work-group-package"],
        "outbox_ids": ["outbox-work-group-package"],
        "committed_at": "2026-07-30T00:00:00Z",
        "data": {
            "source_bundle_id": (
                sources[0]["bundle_id"] if len(sources) == 1 else None
            ),
            "source_bundle_ids": [
                source_spec["bundle_id"] for source_spec in sources
            ],
            "source_bundle_count": len(sources),
            "source_session_ids": list(draft.source_session_ids),
            "package_bundle_id": package_id,
            "membership_mode": draft.membership_mode,
            "member_ids": list(source["member_ids"]),
            "members": selected_rows,
            "member_count": source["member_count"],
            "membership_hash": source["membership_hash"],
            "source_location": "TRANSFER",
            "destination_location": "SHIPPING-WAIT",
            "movement_id": "movement-work-group-package",
            "sample_barcodes": list(draft.sample_barcodes),
            "exact_rescan_barcodes": [],
            "exact_rescan_count": 0,
            "barcode_membership_hash": None,
            "inbound_iin": source["source_iin"],
            "item_id": source["item_id"],
            "uom": source["uom"],
            "source_transitions": transitions,
            "remainder_transfers": remainders,
            "remainder_transfer_bundle_ids": remainder_ids,
            "atomic": True,
            "receipt_contract_version": "PHS_WORK_GROUP_PACKAGE_V1",
            "source_resolution_basis": (
                "PHS_WORK_GROUP_EXACT_MEMBERSHIP"
            ),
            "phs_work_group": dict(group),
            "source_transfers": sources,
            "remainder_cover_groups": covers,
            "source_transfer_seals_consumed": consumed_seals,
            "remainder_transfer_seals": remainder_seals,
            "topology_hash_before": source["topology_hash"],
            "topology_hash_after": topology_after,
            "root_proof": roots,
            "group_entity_versions_after": group_versions,
        },
    }


@pytest.mark.parametrize("split", [False, True])
def test_work_group_package_build_uses_one_resolver_get_and_full_topology(
    split,
):
    response = _work_group_response(split=split)
    draft = _work_group_draft(response)
    calls = []

    def transport(method, url, headers, body, timeout):
        calls.append((method, url, body))
        return {"ok": True, "data": response}

    client = PackageLogisticsClient(
        PackageClientConfig(
            "https://logistics.test", "token", SCOPE, "host", "device"
        ),
        transport=transport,
    )
    source_identity, command = client.build_create_package_command(
        draft, idempotency_key=f"work-group-{split}"
    )

    assert source_identity == response["phs_work_group"]["group_id"]
    assert [method for method, _url, _body in calls] == ["GET"]
    assert "/bundles/resolve?" in calls[0][1]
    assert f"/bundles/{SCOPE}/" not in calls[0][1]
    assert command["expected_versions"] == response[
        "work_group_source"
    ]["entity_versions"]
    assert command["payload"]["source_transfers"] == response[
        "work_group_source"
    ]["source_transfers"]
    assert command["payload"]["remainder_cover_groups"] == response[
        "work_group_source"
    ]["remainder_cover_groups"]

    receipt = _work_group_receipt(draft, command)
    PackageOutboxProcessor._validate_receipt(
        draft,
        source_identity,
        receipt,
        command=command,
    )


def test_work_group_package_preserves_server_uom_spelling_in_command():
    response = _work_group_response(split=False)
    response["work_group_source"]["uom"] = "Pcs"
    response["phs_work_group"]["uom"] = "Pcs"
    response["bundle"]["uom"] = "Pcs"
    topology_hash = package_module.canonical_sha256(
        {
            "phs_work_group": response["phs_work_group"],
            "source_transfers": response["work_group_source"][
                "source_transfers"
            ],
            "remainder_cover_groups": response["work_group_source"][
                "remainder_cover_groups"
            ],
            "source_iin": response["work_group_source"]["source_iin"],
            "barcode_membership_hash": response["work_group_source"][
                "barcode_membership_hash"
            ],
            "package_bundle_id": response["work_group_source"][
                "package_bundle_id"
            ],
        }
    )
    response["work_group_source"]["topology_hash"] = topology_hash
    response["topology_hash"] = topology_hash
    draft = _work_group_draft(response)
    client = PackageLogisticsClient(
        PackageClientConfig(
            "https://logistics.test", "token", SCOPE, "host", "device"
        ),
        transport=lambda *_args: {"ok": True, "data": response},
    )

    _source_identity, command = client.build_create_package_command(
        draft, idempotency_key="work-group-preserve-server-uom"
    )
    snapshot = label_module._label_match_package_source_snapshot(response)

    assert command["payload"]["uom"] == "Pcs"
    assert snapshot["uom"] == "Pcs"


def test_initial_phs_scan_accepts_server_deterministic_work_group_identity():
    response = _work_group_response(split=False)
    group = response["phs_work_group"]
    provisional = PackageCommandDraft.build(
        set_id="phs-scan-provisional",
        item_code=group["item_id"],
        source_input_tag_id=group["scan_anchor_input_tag_id"],
        source_input_tag_label_id=group["label_id"],
        source_input_tag_hash_prefix="aaaaaaaaaaaaaaaa",
        source_canonical_input_tag_qr=group["scan_payload"],
        source_active_label_qr_payload=group["scan_payload"],
        external_label="LOCAL-PROVISIONAL-PACKAGE-ID",
        membership_mode="INHERIT_ALL",
    )
    assert (
        provisional.package_bundle_id
        != response["work_group_source"]["package_bundle_id"]
    )
    client = PackageLogisticsClient(
        PackageClientConfig(
            "https://logistics.test", "token", SCOPE, "host", "device"
        ),
        transport=lambda *_args: {"ok": True, "data": response},
    )

    evidence = client.resolve_package_source_evidence(provisional)
    snapshot = label_module._label_match_package_source_snapshot(evidence)

    assert snapshot["package_bundle_id"] == response[
        "work_group_source"
    ]["package_bundle_id"]
    assert snapshot["source_transfer_count"] == 2


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value["work_group_source"].update(
            {"topology_hash": "0" * 64}
        ),
        lambda value: value["work_group_source"]["source_transfers"][0].update(
            {"selected_member_count": 99}
        ),
        lambda value: value["work_group_source"]["entity_versions"].pop(
            next(
                key
                for key in value["work_group_source"]["entity_versions"]
                if key.startswith("phs_work_membership:")
            )
        ),
    ],
)
def test_work_group_package_corrupt_preflight_blocks_before_command_post(
    mutate,
):
    frozen = _work_group_response(split=True)
    draft = _work_group_draft(frozen)
    current = json.loads(json.dumps(frozen))
    mutate(current)
    calls = []

    def transport(method, url, headers, body, timeout):
        calls.append(method)
        return {"ok": True, "data": current}

    client = PackageLogisticsClient(
        PackageClientConfig(
            "https://logistics.test", "token", SCOPE, "host", "device"
        ),
        transport=transport,
    )
    with pytest.raises(PackageLogisticsError):
        client.build_create_package_command(
            draft, idempotency_key="work-group-corrupt"
        )
    assert calls == ["GET"]


@pytest.mark.parametrize(
    "mutate",
    [
        lambda receipt: receipt["data"]["source_transitions"].pop(),
        lambda receipt: receipt["data"]["remainder_transfers"][0].update(
            {"member_count": 99}
        ),
        lambda receipt: receipt["data"]["root_proof"][0].update(
            {"root_role": "TARGET"}
        ),
        lambda receipt: receipt["data"].update(
            {"topology_hash_after": "f" * 64}
        ),
        lambda receipt: receipt["entity_versions"].update(
            {
                next(
                    key
                    for key in receipt["entity_versions"]
                    if key.startswith("phs_work_group:")
                ): 999
            }
        ),
    ],
)
def test_work_group_package_receipt_plural_proof_is_fail_closed(mutate):
    response = _work_group_response(split=True)
    draft = _work_group_draft(response)
    client = PackageLogisticsClient(
        PackageClientConfig(
            "https://logistics.test", "token", SCOPE, "host", "device"
        ),
        transport=lambda *_args: {"ok": True, "data": response},
    )
    source_identity, command = client.build_create_package_command(
        draft, idempotency_key="work-group-receipt"
    )
    receipt = _work_group_receipt(draft, command)
    mutate(receipt)

    with pytest.raises(PackageLogisticsError):
        PackageOutboxProcessor._validate_receipt(
            draft,
            source_identity,
            receipt,
            command=command,
        )


@pytest.mark.parametrize(
    ("receipt_visible", "expected_post_count"),
    [(True, 0), (False, 1)],
)
def test_work_group_restart_is_receipt_first_without_rebuild(
    tmp_path,
    receipt_visible,
    expected_post_count,
):
    response = _work_group_response(split=False)
    draft = _work_group_draft(response)
    builder = PackageLogisticsClient(
        PackageClientConfig(
            "https://logistics.test", "token", SCOPE, "host", "device"
        ),
        transport=lambda *_args: {"ok": True, "data": response},
    )
    outbox = PackageOutbox(tmp_path / "work-group-restart.sqlite3")
    row = outbox.enqueue(draft)
    outbox.mark_local_completion_committed(row["idempotency_key"])
    claimed = outbox.claim_next()
    assert claimed["idempotency_key"] == row["idempotency_key"]
    source_identity, command = builder.build_create_package_command(
        draft, idempotency_key=row["idempotency_key"]
    )
    receipt = _work_group_receipt(draft, command)
    outbox.save_command(row["idempotency_key"], source_identity, command)
    outbox.mark_retry(
        row["idempotency_key"], PackageTransportError("lost ACK")
    )

    class ReceiptFirstClient:
        def __init__(self):
            self.receipt_get_count = 0
            self.post_count = 0
            self.build_count = 0
            self.posted_commands = []

        def build_create_package_command(self, *_args, **_kwargs):
            self.build_count += 1
            pytest.fail("saved command must not be rebuilt")

        def get_receipt_if_exists(self, key, *, authority_scope_id):
            self.receipt_get_count += 1
            assert key == row["idempotency_key"]
            assert authority_scope_id == SCOPE
            return receipt if receipt_visible else None

        def create_package(self, saved_command):
            self.post_count += 1
            self.posted_commands.append(saved_command)
            return receipt

    recovery = ReceiptFirstClient()
    result = PackageOutboxProcessor(
        PackageOutbox(tmp_path / "work-group-restart.sqlite3"),
        recovery,
    ).drain(limit=1)

    assert result == {"acked": 1, "retry": 0, "conflict": 0}
    assert recovery.receipt_get_count == 1
    assert recovery.post_count == expected_post_count
    assert recovery.build_count == 0
    assert recovery.posted_commands in ([], [command])


@pytest.mark.parametrize("split", [False, True])
def test_label_match_work_group_snapshot_draft_and_f4_block(split):
    response = _work_group_response(split=split)
    snapshot = label_module._label_match_package_source_snapshot(response)
    group = response["phs_work_group"]
    current = {
        "id": f"SET-LABEL-WORK-{split}",
        "raw": [group["scan_payload"]],
        "parsed": [group["item_id"]],
        "central_inherit_all": True,
        "canonical_input_tag_qr": group["scan_payload"],
        "active_label_qr_payload": group["scan_payload"],
        "package_source_snapshot": snapshot,
        "sealed_transfer": None,
    }
    draft = label_module._label_match_package_draft(
        current,
        item_code=group["item_id"],
        require_source_snapshot=True,
    )

    assert draft.source_resolution_basis == (
        "PHS_WORK_GROUP_EXACT_MEMBERSHIP"
    )
    assert draft.source_bundle_id == ""
    assert draft.source_bundle_hint == ""
    assert draft.package_bundle_id == response["work_group_source"][
        "package_bundle_id"
    ]
    assert draft.source_session_ids == tuple(
        response["work_group_source"]["source_session_ids"]
    )

    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app.current_set_info = current
    app.run_tests = True
    app._sealed_transfer_exchange_blocks_local_action = (
        lambda _action: False
    )
    app.update_big_display = lambda *_args: None
    app._render_operator_workbench = lambda: None
    network_starts = []
    app._start_central_phs2_exchange = (
        lambda: network_starts.append(True)
    )

    assert label_module.Label_Match._handle_f4_action(app) is False
    assert network_starts == []


def test_label_match_full_single_work_group_keeps_f4_path():
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app.current_set_info = {
        "package_source_snapshot": {
            "source_resolution_basis": (
                "PHS_WORK_GROUP_EXACT_MEMBERSHIP"
            ),
            "full_single_transfer": True,
        },
        "sealed_transfer": {"SID": "seal-full-single"},
    }
    app.run_tests = True
    app._sealed_transfer_exchange_blocks_local_action = (
        lambda _action: False
    )
    prompts = []
    app._prompt_sealed_transfer_exchange = (
        lambda: prompts.append(True) or True
    )

    assert label_module.Label_Match._handle_f4_action(app) is True
    assert prompts == [True]


def test_initial_phs2_scan_is_read_only_so_f4_remains_available(tmp_path):
    response = _work_group_response(split=False)
    group = response["phs_work_group"]
    issue_calls = []
    resolve_calls = []

    class Client:
        config = PackageClientConfig(
            base_url="https://logistics.example.test",
            token="secret",
            authority_scope_id=SCOPE,
            source_host_id="HOST-PACK-01",
            device_id="PACK-01",
        )

        def issue_operation_lease(self, **kwargs):
            issue_calls.append(kwargs)
            pytest.fail("initial scan must not take the exclusive F3 lease")

        def resolve_package_source_evidence(self, _draft):
            resolve_calls.append(True)
            return response

    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app.package_logistics_client = Client()
    app.package_operation_lease_store = label_module.OperationLeaseStore(
        tmp_path / "read-only-scan.sqlite3"
    )
    app.package_operation_lease_keyring = (
        label_module.PinnedOperationLeaseKeyring(tmp_path / "lease-keys.json")
    )
    sealed = {
        "BND": "TRANSFER-1",
        "AUTH_SCOPE": SCOPE,
        "CLC": group["item_id"],
        "SID": "seal-1",
        "SREV": 1,
        "STK": "key-1",
    }
    snapshot = {
        "source_resolution_basis": "PHS_WORK_GROUP_EXACT_MEMBERSHIP",
        "full_single_transfer": True,
    }
    app._central_phs2_response_parts = (
        lambda _physical_qr, _response: ("evidence", snapshot, sealed)
    )

    _evidence, resolved, resolved_seal, lease = (
        app._resolve_central_phs2_scan_overlay(
            group["scan_payload"], group["item_id"]
        )
    )

    assert resolve_calls == [True]
    assert issue_calls == []
    assert lease is None
    assert resolved == snapshot
    assert resolved_seal == sealed
    app.current_set_info = {
        "id": "SET-F4-BEFORE-LEASE",
        "raw": [group["scan_payload"]],
        "parsed": [group["item_id"]],
        "physical_scanned_qr_payload": group["scan_payload"],
        "sealed_transfer": sealed,
        "package_source_snapshot": snapshot,
    }
    app.run_tests = True
    app._current_sealed_transfer_exchange_attempt = lambda: None
    assert app._prompt_sealed_transfer_exchange() is True
    assert issue_calls == []


def test_f3_restart_offline_reuses_exact_durable_prefetched_lease(tmp_path):
    response = _work_group_response(split=False)
    snapshot = label_module._label_match_package_source_snapshot(response)
    group = response["phs_work_group"]
    current = {
        "id": "SET-LEASE-RESTART",
        "raw": [group["scan_payload"]],
        "parsed": [group["item_id"]],
        "central_inherit_all": True,
        "canonical_input_tag_qr": group["scan_payload"],
        "physical_scanned_qr_payload": group["scan_payload"],
        "active_label_qr_payload": group["scan_payload"],
        "active_label_id": group["label_id"],
        "package_source_snapshot": snapshot,
        "sealed_transfer": None,
        "exact_rescan_complete": False,
        "operation_lease_id": "",
    }
    database = tmp_path / "restart-lease.sqlite3"
    keyring_path = tmp_path / "operation-lease-keys.json"
    config = PackageClientConfig(
        base_url="https://logistics.example.test",
        token="secret",
        authority_scope_id=SCOPE,
        authority_epoch=5,
        ledger_plane="AUTHORITATIVE",
        plane_epoch=3,
        source_host_id="HOST-PACK-01",
        device_id="PACK-01",
    )
    operation_snapshot = {"server_snapshot": "exact-v1"}
    signer = _OperationLeaseTestSigner()
    lease_binding = label_module._label_match_operation_lease_binding(
        group["scan_payload"], snapshot, config
    )
    assert lease_binding["resource_id"] == (
        "phs-work-group:" + group["group_id"]
    )
    artifact = signer.artifact(
        binding=lease_binding,
        operation_snapshot=operation_snapshot,
    )
    online_issue_calls = []

    class OnlineClient:
        def __init__(self):
            self.config = config

        def issue_operation_lease(self, **kwargs):
            online_issue_calls.append(kwargs)
            return artifact

    preparing = label_module.Label_Match.__new__(label_module.Label_Match)
    preparing.package_logistics_client = OnlineClient()
    preparing.package_operation_lease_store = label_module.OperationLeaseStore(
        database
    )
    preparing.package_operation_lease_keyring = (
        label_module.PinnedOperationLeaseKeyring(keyring_path)
    )
    preparing._central_phs2_response_parts = (
        lambda _physical_qr, _response: (object(), snapshot, None)
    )

    prepared = preparing._acquire_operation_lease(
        group["scan_payload"], expected_snapshot=snapshot
    )

    lease_id = prepared[3]["lease_id"]
    stored = preparing.package_operation_lease_store.get(lease_id=lease_id)
    assert stored["status"] == "PREFETCHED"
    assert stored["set_id"] is None
    assert len(online_issue_calls) == 1

    offline_issue_calls = []

    class OfflineClient:
        def __init__(self):
            self.config = config

        def issue_operation_lease(self, **kwargs):
            offline_issue_calls.append(kwargs)
            raise PackageTransportError("offline")

    restarted = label_module.Label_Match.__new__(label_module.Label_Match)
    restarted.current_set_info = copy.deepcopy(current)
    restarted.run_tests = False
    restarted.is_running_simulation = False
    restarted.initialized_successfully = False
    restarted._logistics_authoritative_required = True
    restarted._central_inherit_all_active = lambda: True
    restarted.package_logistics_client = OfflineClient()
    restarted.package_outbox = PackageOutbox(database)
    restarted.package_operation_lease_store = label_module.OperationLeaseStore(
        database
    )
    restarted.package_operation_lease_keyring = (
        label_module.PinnedOperationLeaseKeyring(keyring_path)
    )
    restarted._central_phs2_response_parts = (
        lambda _physical_qr, _response: (object(), snapshot, None)
    )

    metadata = restarted._queue_authoritative_package(
        item_code=group["item_id"],
        is_manual_complete=False,
    )

    assert offline_issue_calls == []
    assert metadata["operation_lease_id"] == lease_id
    replayed_metadata = restarted._queue_authoritative_package(
        item_code=group["item_id"],
        is_manual_complete=False,
    )
    assert replayed_metadata["operation_lease_id"] == lease_id
    assert replayed_metadata["operation_lease_completed_at"] == metadata[
        "operation_lease_completed_at"
    ]
    row = restarted.package_outbox.get_by_set_id(current["id"])
    restarted.package_outbox.mark_local_completion_committed(
        row["idempotency_key"],
        operation_lease_id=lease_id,
        operation_completed_at=replayed_metadata[
            "operation_lease_completed_at"
        ],
    )
    assert restarted.package_operation_lease_store.get(
        lease_id=lease_id
    )["status"] == "LOCAL_COMPLETED"


def test_label_match_offline_f4_fails_closed_without_local_package_mutation():
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    original = {
        "id": "SET-OFFLINE-F4",
        "raw": ["PHS2|offline"],
        "parsed": ["ITEM-1"],
        "sealed_transfer": {
            "BND": "TRANSFER-1",
            "AUTH_SCOPE": SCOPE,
            "CLC": "cycle-1",
            "SID": "seal-1",
            "SREV": 1,
            "STK": "key-1",
        },
        "package_source_snapshot": {
            "source_resolution_basis": "PHS_WORK_GROUP_EXACT_MEMBERSHIP",
            "full_single_transfer": True,
        },
    }
    app.current_set_info = copy.deepcopy(original)
    app.run_tests = True
    app.package_logistics_client = None
    app._current_sealed_transfer_exchange_attempt = lambda: None

    assert (
        label_module.Label_Match._prompt_sealed_transfer_exchange(app)
        is False
    )
    assert app.current_set_info == original


def test_label_match_offline_f3_without_prefetched_lease_fails_before_mutation(
    tmp_path,
):
    response = _work_group_response(split=False)
    snapshot = label_module._label_match_package_source_snapshot(response)
    group = response["phs_work_group"]
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app.current_set_info = {
        "id": "SET-NO-LEASE",
        "raw": [group["scan_payload"]],
        "parsed": [group["item_id"]],
        "central_inherit_all": True,
        "canonical_input_tag_qr": group["scan_payload"],
        "active_label_qr_payload": group["scan_payload"],
        "active_label_id": group["label_id"],
        "package_source_snapshot": snapshot,
        "sealed_transfer": None,
        "exact_rescan_complete": False,
    }
    app.run_tests = False
    app.is_running_simulation = False
    app._logistics_authoritative_required = True
    app._central_inherit_all_active = lambda: True
    database = tmp_path / "no-lease.sqlite3"
    app.package_outbox = PackageOutbox(database)
    app.package_operation_lease_store = label_module.OperationLeaseStore(
        database
    )
    app.package_operation_lease_keyring = (
        label_module.PinnedOperationLeaseKeyring(tmp_path / "lease-keys.json")
    )

    class LeaseCapableClient:
        config = PackageClientConfig(
            base_url="https://logistics.example.test",
            token="secret",
            authority_scope_id=SCOPE,
            authority_epoch=5,
            ledger_plane="AUTHORITATIVE",
            plane_epoch=7,
            source_host_id="HOST-PACK-01",
            device_id="PACK-01",
        )

        def issue_operation_lease(self, **_kwargs):
            raise PackageTransportError("offline")

    app.package_logistics_client = LeaseCapableClient()

    before = copy.deepcopy(app.current_set_info)
    with pytest.raises(
        label_module.OperationLeaseError,
        match="offline",
    ) as blocked:
        label_module.Label_Match._queue_authoritative_package(
            app,
            item_code=group["item_id"],
            is_manual_complete=False,
        )

    assert blocked.value.code == "OPERATION_LEASE_ISSUE_FAILED"
    assert app.package_outbox.get_by_set_id("SET-NO-LEASE") is None
    assert app.current_set_info == before
    _scope, fingerprint = app._operation_lease_request_context(
        group["scan_payload"]
    )
    attempt = app.package_operation_lease_store.get_issue_attempt(
        request_fingerprint=fingerprint
    )
    assert attempt["status"] == "ACTIVE"


def test_label_match_lease_failure_ui_hides_technical_details(capsys):
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app.current_set_info = {"raw": ["PHS2-PHYSICAL"]}
    app._workflow_total_scan_count = lambda: 1
    app._render_operator_workbench = lambda: None

    result = label_module.Label_Match._publish_durable_commit_block(
        app,
        label_module.OperationLeaseError(
            "OPERATION_LEASE_SIGNATURE_INVALID",
            "token=a.b.c device=PACK-OTHER",
        ),
    )

    assert result is False
    message = app._workflow_notice.message
    assert "token=" not in message
    assert "PACK-OTHER" not in message
    assert "OPERATION_LEASE" not in message
    assert "관리자" in message
    assert "token=a.b.c" in capsys.readouterr().out


def test_label_match_work_group_orphan_recovery_and_plural_origins():
    response = _work_group_response(split=False)
    draft = _work_group_draft(response, set_id="SET-WORK-ORPHAN")
    state = label_module._label_match_recover_central_state_from_package_row(
        {
            "set_id": draft.set_id,
            "idempotency_key": "work-orphan-key",
            "status": "PENDING",
            "created_at": "2026-07-30T00:00:00Z",
            "draft_json": json.dumps(
                draft.to_dict(), ensure_ascii=False
            ),
        }
    )

    assert state["sealed_transfer"] is None
    assert state["resolved_transfer_bundle_id"] == ""
    assert state["package_source_snapshot"][
        "source_transfer_bundle_ids"
    ] == response["work_group_source"]["source_transfer_bundle_ids"]
    details = {"source_session_id": "anchor-only"}
    label_module._label_match_apply_package_source_origins(
        details, state["package_source_snapshot"]
    )
    assert details["source_session_ids"] == [
        "ITG-WORK-ONE",
        "ITG-WORK-TWO",
    ]
    assert "source_session_id" not in details
    assert details["source_transfer_bundle_ids"] == [
        "TRANSFER-WORK-A",
        "TRANSFER-WORK-B",
    ]

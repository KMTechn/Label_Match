from __future__ import annotations

import base64
import csv
import io
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
from dataclasses import replace
from urllib.parse import parse_qs, urlsplit

import pytest

import Label_Match as label_module
import package_logistics as package_module
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


def test_structured_phs_itg_defaults_to_server_inherited_membership():
    master = "PHS=2|SRC=KMTECH_INPUT_TAG|ITG=ITG-PACKAGE-1|CLC=ITEM000000001"
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
    claimed = outbox.claim_next()
    command = {"authority_scope_id": SCOPE, "idempotency_key": claimed["idempotency_key"], "payload": {"x": 1}}
    outbox.save_command(claimed["idempotency_key"], TRANSFER, command)
    outbox.save_command(claimed["idempotency_key"], TRANSFER, command)
    with pytest.raises(PackageLogisticsError, match="immutable"):
        outbox.save_command(
            claimed["idempotency_key"], TRANSFER, {**command, "payload": {"x": 2}}
        )


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
    assert {"package_command_outbox", "package_cancellation_outbox"}.issubset(tables)
    assert {
        "local_event_committed",
        "local_event_committed_at",
        "retry_after_at",
    }.issubset(cancellation_columns)
    assert "retry_after_at" in command_columns
    assert version == package_module.OUTBOX_SCHEMA_VERSION


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
    assert "retry_after_at" in cancellation_columns
    assert version == package_module.OUTBOX_SCHEMA_VERSION


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
    package_outbox.enqueue(create_draft)
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
    package_outbox.enqueue(draft)
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
    package_outbox.enqueue(draft)
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
    outbox.enqueue(draft)
    first_client = RestartClient(draft, lose_ack=True)
    first = PackageOutboxProcessor(outbox, first_client).drain(limit=1)
    assert first == {"acked": 0, "retry": 1, "conflict": 0}
    pending = outbox.get_by_set_id(draft.set_id)
    saved_command = pending["command_json"]
    assert saved_command

    restarted = PackageOutbox(tmp_path / "restart.sqlite3")
    recovery_client = RestartClient(draft, receipt=_receipt(draft))
    recovered = PackageOutboxProcessor(restarted, recovery_client).drain(limit=1)
    assert recovered == {"acked": 1, "retry": 0, "conflict": 0}
    assert recovery_client.build_calls == 0
    assert recovery_client.create_calls == 0
    acked = restarted.get_by_set_id(draft.set_id)
    assert acked["command_json"] == saved_command
    assert acked["status"] == "ACKED"


def test_saved_command_reposts_identical_payload_when_receipt_not_yet_visible(tmp_path):
    draft = _draft()
    outbox = PackageOutbox(tmp_path / "repost.sqlite3")
    row = outbox.enqueue(draft)
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
    outbox.enqueue(draft)

    class InvalidClient(RestartClient):
        def build_create_package_command(self, draft, *, idempotency_key):
            raise PackageLogisticsError("QR quantity mismatch")

    result = PackageOutboxProcessor(outbox, InvalidClient(draft)).drain(limit=1)
    assert result == {"acked": 0, "retry": 0, "conflict": 1}
    assert outbox.get_by_set_id(draft.set_id)["status"] == "CONFLICT"


def test_create_package_429_waits_until_retry_after_instead_of_conflicting(tmp_path):
    draft = _draft()
    outbox = PackageOutbox(tmp_path / "create-retry-after.sqlite3")
    outbox.enqueue(draft)

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
    outbox.enqueue(draft)

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

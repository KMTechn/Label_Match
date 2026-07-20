from __future__ import annotations

import base64
import csv
import json
from pathlib import Path
import sqlite3
from dataclasses import replace
from urllib.parse import parse_qs, urlsplit

import pytest

import Label_Match as label_module
import package_logistics as package_module
from package_logistics import (
    PackageClientConfig,
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
        "members": [
            {"unit_id": unit_id, "normalized_barcode": barcode}
            for unit_id, barcode in zip(UNITS, BARCODES, strict=True)
        ],
    }


def _receipt(draft):
    return {
        "receipt_id": "receipt-package",
        "entity_versions": {f"bundle:{TRANSFER}": 8, f"bundle:{draft.package_bundle_id}": 1},
        "data": {
            "source_bundle_id": TRANSFER,
            "source_bundle_type": "TRANSFER",
            "package_bundle_id": draft.package_bundle_id,
            "membership_mode": draft.membership_mode,
            "member_ids": list(UNITS),
            "member_count": 4,
            "membership_hash": MEMBERSHIP_HASH,
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


def test_legacy_base64_wid_qr_separates_actual_external_label_and_input_tag():
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
    assert draft.source_external_label == "PHS-EXTERNAL-WID-1"
    assert draft.source_external_label != master


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
def test_v2_schema_is_complete_before_version_is_stamped(tmp_path, outbox_type):
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
        columns = {
            row[1]
            for row in conn.execute(
                "PRAGMA table_info(package_cancellation_outbox)"
            ).fetchall()
        }
        version = conn.execute(
            "SELECT value FROM package_outbox_schema_info WHERE key='schema_version'"
        ).fetchone()[0]
    finally:
        conn.close()
    assert {"package_command_outbox", "package_cancellation_outbox"}.issubset(tables)
    assert {"local_event_committed", "local_event_committed_at"}.issubset(columns)
    assert version == package_module.OUTBOX_SCHEMA_VERSION


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
        "authority_scope_id": SCOPE,
        "idempotency_key": row["idempotency_key"],
        "payload": {
            "source_bundle_id": TRANSFER,
            "package_bundle_id": draft.package_bundle_id,
        },
    }
    outbox.save_command(row["idempotency_key"], TRANSFER, command)
    outbox.mark_acked(row["idempotency_key"], _receipt(draft))
    assert outbox.get_by_set_id(draft.set_id)["status"] == "ACKED"
    assert outbox.counts()["ACKED"] == 1

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
    _ack_package_creation(package_outbox, draft)
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
    recovery_client = RecoveryClient()
    recovered = PackageCancellationOutboxProcessor(restarted, recovery_client).drain(limit=1)
    assert recovered == {"acked": 1, "retry": 0, "conflict": 0, "deferred": 0}
    acked = restarted.get_by_event_id(intent.cancellation_event_id)
    assert acked["status"] == "ACKED"
    assert acked["command_json"] == saved_command
    assert recovery_client.build_calls == 0


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


def test_legacy_exact_rescan_resolver_uses_package_source_lineage_role():
    calls = []

    def transport(method, url, headers, body, timeout):
        calls.append((url, dict(headers)))
        if "/bundles/resolve?" in url:
            query = parse_qs(urlsplit(url).query, keep_blank_values=True)
            assert query == {
                "external_label": ["LEGACY-PHS-LABEL"],
                "input_tag_id": ["ITG-LEGACY-RESOLVE"],
                "bundle_id": [""],
                "item_id": ["ITEM000000001"],
                "authority_scope_id": [SCOPE],
                "bundle_role": ["PACKAGE_SOURCE"],
                "member_count": ["4"],
                "barcode_membership_hash": [barcode_membership_hash(BARCODES)],
            }
            return {"ok": True, "data": {"bundle_id": TRANSFER}}
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
            assert query["external_label"] == [""]
            assert query["item_id"] == ["ITEM000000001"]
            assert query["authority_scope_id"] == [SCOPE]
            assert query["bundle_role"] == ["PACKAGE_SOURCE"]
            assert query["member_count"] == ["4"]
            assert query["barcode_membership_hash"] == [barcode_membership_hash(BARCODES)]
            return {"ok": True, "data": {"bundle_id": TRANSFER}}
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
            "payload": {"source_bundle_id": TRANSFER, "package_bundle_id": draft.package_bundle_id},
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
        "payload": {"source_bundle_id": TRANSFER, "package_bundle_id": draft.package_bundle_id},
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

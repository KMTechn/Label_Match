from __future__ import annotations

import base64
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

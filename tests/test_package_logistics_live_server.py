from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import sys
from urllib.parse import urlsplit

import pytest


WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
WEB_ROOT = WORKSPACE_ROOT / "WorkerAnalysisGUI-web"
if not WEB_ROOT.is_dir():
    pytest.skip(
        "cross-repository WorkerAnalysisGUI-web checkout is unavailable",
        allow_module_level=True,
    )

sys.path.insert(0, str(WEB_ROOT))
sys.path.insert(0, str(WEB_ROOT / "tests"))

import Label_Match as label_module  # noqa: E402
from package_logistics import PackageClientConfig, PackageLogisticsClient  # noqa: E402
from test_logistics_api_v1 import SCOPE, TOKEN, _app, _command, _headers  # noqa: E402
from test_logistics_p3_transfer_package import _complete_phs  # noqa: E402


def test_two_scan_central_package_inherits_every_server_member_atomically(tmp_path):
    app, db_path = _app(tmp_path / "server", opening_qty=2)
    web = app.test_client()
    source_id, source_version, source_members = _complete_phs(
        web,
        session_id="LABEL-TWO-SCAN-PHS",
        count=2,
        label="PHS-LABEL-TWO-SCAN",
    )
    source = web.get(
        f"/logistics/api/v1/bundles/{SCOPE}/{source_id}",
        headers=_headers(),
    ).get_json()["data"]
    barcodes = [member["normalized_barcode"] for member in source["members"]]
    sealed = web.post(
        "/logistics/api/v1/transfers/seal",
        json=_command(
            "SEAL_TRANSFER_BUNDLE",
            key="label-two-scan-seal",
            versions={f"bundle:{source_id}": source_version},
            payload={
                "source_bundle_id": source_id,
                "transfer_bundle_id": "TRANSFER-LABEL-TWO-SCAN",
                "external_label": "TRANSFER-LABEL-TWO-SCAN",
                "item_id": "ITEM-API",
                "member_ids": source_members,
                "membership_hash": source["membership_hash"],
                "scanned_barcodes": barcodes,
            },
        ),
        headers=_headers(),
    )
    assert sealed.status_code == 200, sealed.get_json()
    seal_data = sealed.get_json()["data"]["data"]
    seal_qr = seal_data["seal_qr_payload"]

    def transport(method, url, headers, body, _timeout):
        parsed = urlsplit(url)
        path = parsed.path + (("?" + parsed.query) if parsed.query else "")
        payload = json.loads(body.decode("utf-8")) if body else None
        return web.open(
            path,
            method=method,
            headers=dict(headers),
            json=payload,
        ).get_json()

    client = PackageLogisticsClient(
        PackageClientConfig(
            base_url="https://logistics.test.invalid",
            token=TOKEN,
            authority_scope_id=SCOPE,
            source_host_id="label-two-scan-host",
            device_id="label-two-scan-device",
            authority_epoch=2,
            authority_plane="AUTHORITATIVE",
            ledger_plane="SHADOW_CANDIDATE",
            plane_epoch=1,
        ),
        transport=transport,
    )
    final_label = "FINAL-ITEM-API-PACKAGE-LABEL-20260722\x1d6D20260722"
    draft = label_module._label_match_package_draft(
        {
            "id": "LABEL-TWO-SCAN-SET",
            "raw": [seal_qr, final_label],
            "central_inherit_all": True,
        },
        item_code="ITEM-API",
    )
    source_bundle_id, command = client.build_create_package_command(
        draft,
        idempotency_key="label-two-scan-package",
    )

    assert source_bundle_id == "TRANSFER-LABEL-TWO-SCAN"
    assert command["payload"]["membership_mode"] == "INHERIT_ALL"
    assert "sample_barcodes" not in command["payload"]
    assert command["payload"]["source_evidence"]["member_ids"] == sorted(source_members)

    receipt = client.create_package(command)

    assert receipt["data"]["member_ids"] == sorted(source_members)
    assert receipt["data"]["member_count"] == 2
    assert receipt["data"]["membership_mode"] == "INHERIT_ALL"
    with sqlite3.connect(db_path) as conn:
        package = conn.execute(
            "SELECT bundle_type,bundle_state,member_count,membership_hash "
            "FROM logistics_bundles WHERE bundle_id=?",
            (draft.package_bundle_id,),
        ).fetchone()
        assert package == ("PACKAGE", "AVAILABLE", 2, source["membership_hash"])
        ownership = conn.execute(
            "SELECT COUNT(*),COUNT(DISTINCT unit_id) FROM logistics_active_memberships "
            "WHERE owner_type='PACKAGE' AND owner_id=?",
            (draft.package_bundle_id,),
        ).fetchone()
        assert ownership == (2, 2)
        locations = conn.execute(
            """SELECT DISTINCT l.location_code
                 FROM logistics_units u
                 JOIN logistics_locations l
                   ON l.authority_scope_id=u.authority_scope_id
                  AND l.ledger_plane=u.ledger_plane AND l.plane_epoch=u.plane_epoch
                  AND l.location_id=u.current_location_id
                WHERE u.unit_id IN (?,?)""",
            tuple(source_members),
        ).fetchall()
        assert locations == [("SHIPPING-WAIT",)]
        movement = conn.execute(
            "SELECT quantity FROM logistics_movements WHERE movement_type='CREATE_PACKAGE'"
        ).fetchone()
        assert movement == (2,)

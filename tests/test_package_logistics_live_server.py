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
from test_logistics_input_tags import HASH_PREFIX, _claim, _register  # noqa: E402
from test_logistics_p3_transfer_package import _post  # noqa: E402


def test_one_scan_central_package_inherits_every_server_member_atomically(tmp_path):
    app, db_path = _app(tmp_path / "server", opening_qty=2)
    web = app.test_client()
    phs2_qr, _registered = _register(
        web,
        db_path,
        input_tag_id="ITG-LABEL-LIVE",
        label_id="LBL-PHS2",
        qty=2,
    )
    claimed = _claim(
        web,
        "ITG-LABEL-LIVE",
        version=1,
        claim_id="CLAIM-LABEL-LIVE",
    )
    assert claimed.status_code == 200, claimed.get_json()

    session_version = 1
    for index in range(2):
        staged = _post(
            web,
            "/logistics/api/v1/sessions/ITG-LABEL-LIVE/members/stage",
            "STAGE_DIRECT_MEMBER",
            f"label-live-stage-{index}",
            {"session:ITG-LABEL-LIVE": session_version},
            {
                "barcode": f"LABEL-LIVE-{index:03d}",
                "disposition": "GOOD",
                "input_tag_claim_id": "CLAIM-LABEL-LIVE",
            },
        )
        assert staged.status_code == 200, staged.get_json()
        session_version = staged.get_json()["data"]["entity_versions"][
            "session:ITG-LABEL-LIVE"
        ]

    completed = _post(
        web,
        "/logistics/api/v1/sessions/ITG-LABEL-LIVE/complete",
        "COMPLETE_SESSION",
        "label-live-complete",
        {"session:ITG-LABEL-LIVE": session_version},
        {
            "phs_bundle_id": "PHS-ITG-LABEL-LIVE",
            "input_tag_claim_id": "CLAIM-LABEL-LIVE",
        },
    )
    assert completed.status_code == 200, completed.get_json()
    completion = completed.get_json()["data"]
    source_id = "PHS-ITG-LABEL-LIVE"
    source_version = completion["entity_versions"][f"bundle:{source_id}"]
    source_members = completion["data"]["phs_member_ids"]
    source = web.get(
        f"/logistics/api/v1/bundles/{SCOPE}/{source_id}",
        headers=_headers(),
    ).get_json()["data"]
    barcodes = [member["normalized_barcode"] for member in source["members"]]
    sealed = web.post(
        "/logistics/api/v1/transfers/seal",
        json=_command(
            "SEAL_TRANSFER_BUNDLE",
            key="label-phs2-seal",
            versions={f"bundle:{source_id}": source_version},
            payload={
                "source_bundle_id": source_id,
                "transfer_bundle_id": "TRANSFER-LABEL-LIVE",
                "external_label": "TRANSFER-LABEL-LIVE",
                "item_id": "ITEM-API",
                "member_ids": source_members,
                "membership_hash": source["membership_hash"],
                "scanned_barcodes": barcodes,
            },
        ),
        headers=_headers(),
    )
    assert sealed.status_code == 200, sealed.get_json()

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
            source_host_id="label-phs2-host",
            device_id="label-phs2-device",
            authority_epoch=2,
            authority_plane="AUTHORITATIVE",
            ledger_plane="SHADOW_CANDIDATE",
            plane_epoch=1,
        ),
        transport=transport,
    )
    draft = label_module._label_match_package_draft(
        {
            "id": "LABEL-ONE-SCAN-SET",
            "raw": [phs2_qr],
            "central_inherit_all": True,
        },
        item_code="ITEM-API",
    )
    projection = client.resolve_package_source_projection(draft)
    sealed_transfer = label_module._label_match_active_seal_from_package_source(
        projection
    )
    source_snapshot = label_module._label_match_package_source_snapshot(projection)
    draft = label_module._label_match_package_draft(
        {
            "id": "LABEL-ONE-SCAN-SET",
            "raw": [phs2_qr],
            "central_inherit_all": True,
            "sealed_transfer": sealed_transfer,
            "package_source_snapshot": source_snapshot,
        },
        item_code="ITEM-API",
        require_source_snapshot=True,
    )
    source_bundle_id, command = client.build_create_package_command(
        draft,
        idempotency_key="label-phs2-package",
    )

    assert source_bundle_id == "TRANSFER-LABEL-LIVE"
    assert command["payload"]["membership_mode"] == "INHERIT_ALL"
    assert command["payload"]["external_label"].startswith("PKG-PHS2-")
    assert draft.source_input_tag_id == "ITG-LABEL-LIVE"
    assert draft.source_input_tag_label_id == "LBL-PHS2"
    assert draft.source_input_tag_hash_prefix == HASH_PREFIX
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

from __future__ import annotations

import json
from pathlib import Path
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

from package_logistics import PackageClientConfig, PackageLogisticsClient  # noqa: E402
from sealed_transfer_exchange import (  # noqa: E402
    SealedTransferExchangeCoordinator,
    SealedTransferExchangeStore,
    _qr_fields,
)
from test_logistics_api_v1 import SCOPE, TOKEN, _app, _headers  # noqa: E402
from test_logistics_p3_transfer_package import _complete_phs  # noqa: E402
from test_sealed_transfer_replacement import _seed  # noqa: E402


def test_packaging_exchange_traverses_live_capability_resolver_and_reseal(tmp_path):
    app, _db_path = _app(tmp_path / "server", opening_qty=3)
    web = app.test_client()
    seed = _seed(web, source_count=1)

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
            source_host_id="label-live-host",
            device_id="label-live-device",
            authority_epoch=2,
            authority_plane="SHADOW_CANDIDATE",
            plane_epoch=1,
        ),
        transport=transport,
    )
    old_qr = seed["target_evidence"]["seal_qr_payload"]
    coordinator = SealedTransferExchangeCoordinator(
        SealedTransferExchangeStore(tmp_path / "desktop" / "exchange.sqlite3"),
        client,
    )
    prepared = coordinator.prepare(
        set_id="LIVE-LABEL-SET",
        old_seal_qr_payload=old_qr,
        old_seal_fields=_qr_fields(old_qr),
        operator="live-contract-test",
        old_barcodes=[seed["target_evidence"]["normalized_barcodes"][0]],
        new_barcodes=[seed["source_evidence"]["normalized_barcodes"][0]],
    )

    result = coordinator.attempt(prepared.intent_id)

    assert result.status == "ACKED", result
    assert result.new_seal_qr_payload
    assert result.new_seal_qr_payload != old_qr


def test_two_pair_live_reseal_accepts_aggregated_movement_receipts(tmp_path):
    app, _db_path = _app(tmp_path / "server", opening_qty=4)
    web = app.test_client()
    seed = _seed(web, source_count=1)
    source_two_id, _source_two_version, _source_two_members = _complete_phs(
        web,
        session_id="LABEL-LIVE-SOURCE-2",
        count=1,
        label="PHS-LABEL-LIVE-SOURCE-2",
        start_index=20,
    )
    source_two = web.get(
        f"/logistics/api/v1/bundles/{SCOPE}/{source_two_id}",
        headers=_headers(),
    ).get_json()["data"]

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
            source_host_id="label-live-host",
            device_id="label-live-device",
            authority_epoch=2,
            authority_plane="SHADOW_CANDIDATE",
            plane_epoch=1,
        ),
        transport=transport,
    )
    old_qr = seed["target_evidence"]["seal_qr_payload"]
    store = SealedTransferExchangeStore(
        tmp_path / "desktop" / "exchange-two-pair.sqlite3"
    )
    coordinator = SealedTransferExchangeCoordinator(store, client)
    prepared = coordinator.prepare(
        set_id="LIVE-LABEL-TWO-PAIR",
        old_seal_qr_payload=old_qr,
        old_seal_fields=_qr_fields(old_qr),
        operator="live-contract-test",
        old_barcodes=seed["target_evidence"]["normalized_barcodes"],
        new_barcodes=[
            seed["source_evidence"]["normalized_barcodes"][0],
            source_two["members"][0]["normalized_barcode"],
        ],
    )

    result = coordinator.attempt(prepared.intent_id)
    receipt = json.loads(store.load(prepared.intent_id)["receipt_json"])

    assert result.status == "ACKED", result
    assert receipt["data"]["pair_count"] == 2
    assert len(receipt["data"]["movement_ids"]) == 2

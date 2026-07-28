from item_catalog_sync import refresh_item_catalog, resolve_catalog_url


CATALOG = (
    b"Item Code,Item Name,Spec,Tray Image\r\n"
    b"AAA0000000001,Alpha,S1,assets/a.png\r\n"
    b"BBB0000000002,Beta,S2,assets/b.png\r\n"
)


class FakeResponse:
    def __init__(self, content):
        self.content = content

    def raise_for_status(self):
        return None


def test_refresh_success_and_offline_last_good(tmp_path):
    bundle = tmp_path / "bundle.csv"
    cache = tmp_path / "cache" / "Item.csv"
    bundle.write_bytes(CATALOG)
    assert refresh_item_catalog(
        bundle,
        cache_path=cache,
        get=lambda *_args, **_kwargs: FakeResponse(CATALOG),
    ) == cache
    assert cache.read_bytes() == CATALOG

    assert refresh_item_catalog(
        bundle,
        cache_path=cache,
        get=lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("offline")),
    ) == cache
    assert cache.read_bytes() == CATALOG


def test_invalid_response_keeps_bundle_and_url_contract(tmp_path):
    bundle = tmp_path / "bundle.csv"
    cache = tmp_path / "cache.csv"
    bundle.write_bytes(CATALOG)
    assert refresh_item_catalog(
        bundle,
        cache_path=cache,
        get=lambda *_args, **_kwargs: FakeResponse(b"wrong,header\n"),
    ) == bundle
    assert not cache.exists()
    assert resolve_catalog_url({"WORKER_ANALYSIS_SERVER_URL": "https://worker.example/"}) == (
        "https://worker.example/inbound/api/item-catalog.csv"
    )
    assert resolve_catalog_url({"KMTECH_ITEM_CATALOG_URL": "http://test/catalog.csv"}) == "http://test/catalog.csv"

import logging
from types import SimpleNamespace

import pytest

import item_catalog_sync as sync
import logistics_runtime_profile
from item_catalog_sync import refresh_item_catalog, resolve_catalog_url


CATALOG = (
    b"Item Code,Item Name,Spec,Tray Image\r\n"
    b"AAA0000000001,Alpha,S1,assets/a.png\r\n"
    b"BBB0000000002,Beta,S2,assets/b.png\r\n"
)
PRODUCTION_CATALOG_URL = (
    "https://worker.kmtecherp.com/inbound/api/item-catalog.csv"
)
PRODUCTION_CATALOG_URL_WITH_PORT = (
    "https://worker.kmtecherp.com:443/inbound/api/item-catalog.csv"
)
UNAUTHENTICATED_OVERRIDE_URL = "https://worker.example/inbound/api/item-catalog.csv"
SECRET_MARKER = "0123456789abcdef" * 3 + "fedcba9876543210"
SOURCE_HOST_ID = "factory-source-host"
DEVICE_ID = "factory-device"
UNTRUSTED_AUTHENTICATED_URLS = (
    pytest.param(
        "http://worker.kmtecherp.com/inbound/api/item-catalog.csv",
        id="http",
    ),
    pytest.param(
        "https://worker.example/inbound/api/item-catalog.csv",
        id="foreign-origin",
    ),
    pytest.param(
        "https://worker.kmtecherp.com:444/inbound/api/item-catalog.csv",
        id="unexpected-port",
    ),
    pytest.param(
        "https://worker.kmtecherp.com:/inbound/api/item-catalog.csv",
        id="empty-port",
    ),
    pytest.param(
        "https://operator@worker.kmtecherp.com/inbound/api/item-catalog.csv",
        id="userinfo",
    ),
    pytest.param("https://worker.kmtecherp.com/catalog.csv", id="wrong-path"),
    pytest.param(
        "https://worker.kmtecherp.com/inbound/api/item-catalog.csv?download=1",
        id="query",
    ),
    pytest.param(
        "https://worker.kmtecherp.com/inbound/api/item-catalog.csv#catalog",
        id="fragment",
    ),
)


class FakeResponse:
    def __init__(self, content, *, status_code=200):
        self.content = content
        self.status_code = status_code

    def raise_for_status(self):
        return None


def _profile():
    return SimpleNamespace(
        bearer_token=SECRET_MARKER,
        source_host_id=SOURCE_HOST_ID,
        device_id=DEVICE_ID,
    )


@pytest.fixture(autouse=True)
def _no_logistics_runtime_profile(monkeypatch):
    monkeypatch.setattr(
        logistics_runtime_profile,
        "load_logistics_runtime_profile",
        lambda required=None: None,
    )


@pytest.mark.parametrize(
    "catalog_url",
    (PRODUCTION_CATALOG_URL, PRODUCTION_CATALOG_URL_WITH_PORT),
)
def test_refresh_uses_provisioned_logistics_profile_headers(
    monkeypatch, tmp_path, catalog_url
):
    bundle = tmp_path / "bundle.csv"
    cache = tmp_path / "cache" / "Item.csv"
    bundle.write_bytes(CATALOG)
    profile_calls = []
    calls = []

    def load_profile(*, required):
        profile_calls.append(required)
        return _profile()

    monkeypatch.setattr(
        logistics_runtime_profile,
        "load_logistics_runtime_profile",
        load_profile,
    )

    def fake_get(url, timeout, allow_redirects, headers):
        assert allow_redirects is False
        calls.append((url, timeout, headers))
        return FakeResponse(CATALOG)

    result = refresh_item_catalog(
        bundle,
        cache_path=cache,
        url=catalog_url,
        get=fake_get,
    )

    assert result == cache
    assert profile_calls == [None]
    assert calls == [
        (
            catalog_url,
            2.0,
            {
                "Authorization": f"Bearer {SECRET_MARKER}",
                "X-Logistics-Source-Host-Id": SOURCE_HOST_ID,
                "X-Logistics-Device-Id": DEVICE_ID,
                "X-Logistics-Program": "Label_Match",
            },
        )
    ]


@pytest.mark.parametrize("url", UNTRUSTED_AUTHENTICATED_URLS)
def test_profile_rejects_untrusted_url_without_transport(
    monkeypatch, tmp_path, caplog, url
):
    bundle = tmp_path / "bundle.csv"
    bundle.write_bytes(CATALOG)
    monkeypatch.setattr(
        logistics_runtime_profile,
        "load_logistics_runtime_profile",
        lambda required=None: _profile(),
    )
    calls = []

    def unexpected_get(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("transport must not be called")

    with caplog.at_level(logging.WARNING, logger=sync.__name__):
        result = refresh_item_catalog(
            bundle,
            cache_path=tmp_path / "cache.csv",
            url=url,
            get=unexpected_get,
        )

    assert result == bundle
    assert calls == []
    assert SECRET_MARKER not in caplog.text


def test_absent_profile_preserves_unauthenticated_override(monkeypatch, tmp_path):
    bundle = tmp_path / "bundle.csv"
    cache = tmp_path / "cache.csv"
    bundle.write_bytes(CATALOG)
    profile_calls = []
    calls = []

    def load_profile(*, required):
        profile_calls.append(required)
        return None

    monkeypatch.setattr(
        logistics_runtime_profile,
        "load_logistics_runtime_profile",
        load_profile,
    )

    def fake_get(url, timeout, allow_redirects):
        assert allow_redirects is False
        calls.append((url, timeout))
        return FakeResponse(CATALOG)

    result = refresh_item_catalog(
        bundle,
        cache_path=cache,
        url=UNAUTHENTICATED_OVERRIDE_URL,
        get=fake_get,
    )

    assert result == cache
    assert profile_calls == [None]
    assert calls == [(UNAUTHENTICATED_OVERRIDE_URL, 2.0)]


def test_unavailable_profile_preserves_unauthenticated_override_without_leak(
    monkeypatch, tmp_path, caplog
):
    bundle = tmp_path / "bundle.csv"
    bundle.write_bytes(CATALOG)
    calls = []

    def unavailable_profile(*, required):
        raise RuntimeError(f"profile unavailable: {SECRET_MARKER}")

    monkeypatch.setattr(
        logistics_runtime_profile,
        "load_logistics_runtime_profile",
        unavailable_profile,
    )

    def fake_get(url, timeout, allow_redirects):
        assert allow_redirects is False
        calls.append((url, timeout))
        return FakeResponse(CATALOG)

    with caplog.at_level(logging.WARNING, logger=sync.__name__):
        result = refresh_item_catalog(
            bundle,
            cache_path=tmp_path / "cache.csv",
            url=UNAUTHENTICATED_OVERRIDE_URL,
            get=fake_get,
        )

    assert result == tmp_path / "cache.csv"
    assert calls == [(UNAUTHENTICATED_OVERRIDE_URL, 2.0)]
    assert SECRET_MARKER not in caplog.text
    assert "profile unavailable" not in caplog.text


def test_authenticated_request_failure_does_not_log_exception_message(
    monkeypatch, tmp_path, caplog
):
    bundle = tmp_path / "bundle.csv"
    bundle.write_bytes(CATALOG)
    monkeypatch.setattr(
        logistics_runtime_profile,
        "load_logistics_runtime_profile",
        lambda required=None: _profile(),
    )
    calls = []

    def failing_get(url, timeout, allow_redirects, headers):
        assert allow_redirects is False
        calls.append((url, timeout, headers))
        raise OSError(f"transport failed: {SECRET_MARKER}")

    with caplog.at_level(logging.WARNING, logger=sync.__name__):
        result = refresh_item_catalog(
            bundle,
            cache_path=tmp_path / "cache.csv",
            url=PRODUCTION_CATALOG_URL,
            get=failing_get,
        )

    assert result == bundle
    assert calls == [
        (
            PRODUCTION_CATALOG_URL,
            2.0,
            {
                "Authorization": f"Bearer {SECRET_MARKER}",
                "X-Logistics-Source-Host-Id": SOURCE_HOST_ID,
                "X-Logistics-Device-Id": DEVICE_ID,
                "X-Logistics-Program": "Label_Match",
            },
        )
    ]
    assert SECRET_MARKER not in caplog.text
    assert "transport failed" not in caplog.text


def test_redirect_response_is_rejected_and_uses_last_good_cache(
    monkeypatch, tmp_path, caplog
):
    bundle = tmp_path / "bundle.csv"
    cache = tmp_path / "cache" / "Item.csv"
    cached_catalog = CATALOG.replace(b"Alpha", b"Cached")
    redirected_catalog = CATALOG.replace(b"Alpha", b"Redirected")
    bundle.write_bytes(CATALOG)
    cache.parent.mkdir()
    cache.write_bytes(cached_catalog)
    monkeypatch.setattr(
        logistics_runtime_profile,
        "load_logistics_runtime_profile",
        lambda required=None: _profile(),
    )
    calls = []

    def redirecting_get(url, timeout, allow_redirects, headers):
        calls.append((url, timeout, allow_redirects, headers["Authorization"]))
        return FakeResponse(redirected_catalog, status_code=302)

    with caplog.at_level(logging.WARNING, logger=sync.__name__):
        result = refresh_item_catalog(
            bundle,
            cache_path=cache,
            url=PRODUCTION_CATALOG_URL,
            get=redirecting_get,
        )

    assert result == cache
    assert cache.read_bytes() == cached_catalog
    assert calls == [
        (PRODUCTION_CATALOG_URL, 2.0, False, f"Bearer {SECRET_MARKER}")
    ]
    assert "Item catalog sync skipped" in caplog.text


def test_replace_race_returns_cache_created_before_permission_error(
    monkeypatch, tmp_path
):
    bundle = tmp_path / "bundle.csv"
    cache = tmp_path / "cache" / "Item.csv"
    bundle.write_bytes(CATALOG)
    real_atomic_write = sync._atomic_write

    def write_cache_then_fail(path, payload):
        real_atomic_write(path, payload)
        raise PermissionError("simulated shared-cache replace race")

    monkeypatch.setattr(sync, "_atomic_write", write_cache_then_fail)

    result = refresh_item_catalog(
        bundle,
        cache_path=cache,
        get=lambda *_args, **_kwargs: FakeResponse(CATALOG),
    )

    assert result == cache
    assert cache.read_bytes() == CATALOG


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

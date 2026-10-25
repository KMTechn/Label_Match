"""Lane33 first-start budget through the actual startup snapshot gate."""
import ast
from functools import lru_cache
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

import item_catalog_sync as sync
import logistics_runtime_profile
from tests.test_item_catalog_sync import CATALOG, FakeResponse, _profile


@lru_cache(maxsize=1)
def _startup_code():
    source = Path(__file__).resolve().parents[1] / "Label_Match.py"
    tree = ast.parse(source.read_text(encoding="utf-8-sig"))
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                and n.name == "prepare_startup_item_catalog")
    return compile(ast.Module(body=[node], type_ignores=[]), str(source), "exec")


class _Clock:
    def __init__(self):
        self.elapsed = 0.0
        self.sleeps = []

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.elapsed += seconds


def _case(monkeypatch, tmp_path, *, cache_kind="absent", enrolled=True):
    bundle, cache = tmp_path / "bundle.csv", tmp_path / "cache/Item.csv"
    bundle.write_bytes(CATALOG)
    profile = _profile() if enrolled else None
    monkeypatch.setattr(logistics_runtime_profile, "load_logistics_runtime_profile",
                        lambda **kwargs: profile)
    monkeypatch.setattr(sync, "default_cache_path", lambda: cache)
    if cache_kind in {"primary", "recovery"}:
        sync.refresh_item_catalog(bundle, get=lambda *args, **kwargs: FakeResponse(CATALOG))
        if cache_kind == "recovery":
            cache.write_bytes(b"damaged catalog\n")
    elif cache_kind == "unauthenticated":
        cache.parent.mkdir()
        cache.write_bytes(CATALOG)
    clock, calls = _Clock(), []
    monkeypatch.setattr(sync, "time", SimpleNamespace(sleep=clock.sleep,
                        monotonic=lambda: clock.elapsed), raising=False)
    namespace = dict(resource_path=lambda path: str(bundle),
        Label_Match=SimpleNamespace(FILES=SimpleNamespace(ITEMS="Item.csv")),
        os=SimpleNamespace(path=os.path, environ={}), refresh_item_catalog=sync.refresh_item_catalog,
        requires_verified_catalog_snapshot=sync.requires_verified_catalog_snapshot,
        get_verified_catalog_snapshot=sync.get_verified_catalog_snapshot,
        ItemCatalogSyncError=sync.ItemCatalogSyncError,
        SNAPSHOT_UNAVAILABLE_AFTER_VERIFY=sync.SNAPSHOT_UNAVAILABLE_AFTER_VERIFY,
        ACTIVE_PATH_ENV=sync.ACTIVE_PATH_ENV)
    exec(_startup_code(), namespace)

    def run(transport):
        def observed(url, **kwargs):
            calls.append(dict(timeout=kwargs["timeout"]))
            return transport(len(calls), kwargs["timeout"])
        monkeypatch.setattr(sync, "_hardened_get", observed)
        monkeypatch.setattr(sync.requests, "get", observed)
        started, error = None, None
        try:
            started = namespace["prepare_startup_item_catalog"]()
        except sync.ItemCatalogSyncError as caught:
            error = type(caught).__name__
        facts = dict(started=started is not None, error_type=error,
            attempts=len(calls), timeouts=[row["timeout"] for row in calls],
            synthetic_elapsed_seconds=clock.elapsed, sleeps=clock.sleeps,
            verified_snapshot=started is not None and sync.get_verified_catalog_snapshot(started) is not None,
            cache_kind=cache_kind, enrolled=enrolled,
            path_is_bundle=started == str(bundle),
            actual_startup_function="Label_Match.prepare_startup_item_catalog")
        (tmp_path / "catalog-first-start-proof.json").write_text(json.dumps(facts), encoding="utf-8")
        return facts
    return run, clock


@pytest.mark.parametrize("cache_kind", ["absent", "unauthenticated"])
def test_slow_first_response_starts_with_verified_snapshot(monkeypatch, tmp_path, cache_kind):
    run, clock = _case(monkeypatch, tmp_path, cache_kind=cache_kind)
    def slow(_attempt, timeout):
        clock.elapsed += min(3.0, timeout)
        if timeout < 3.0:
            raise requests.ReadTimeout()
        return FakeResponse(CATALOG)
    facts = run(slow)
    assert facts["started"] and facts["verified_snapshot"]
    assert facts["timeouts"] == [10.0] and facts["synthetic_elapsed_seconds"] == 3.0


@pytest.mark.parametrize("error_type", [requests.ConnectionError,
    requests.exceptions.SSLError, requests.ReadTimeout])
def test_two_transport_errors_then_third_success(monkeypatch, tmp_path, error_type):
    run, _clock = _case(monkeypatch, tmp_path)
    def transport(attempt, timeout):
        if attempt < 3:
            raise error_type()
        return FakeResponse(CATALOG)
    facts = run(transport)
    assert facts["started"] and facts["verified_snapshot"] and facts["attempts"] == 3
    assert facts["timeouts"] == [10.0] * 3 and facts["sleeps"] == [0.25, 0.25]


@pytest.mark.parametrize("failure", ["http400", "http503", "invalid_catalog", "response_on_transport_error"])
def test_server_response_or_validation_failure_never_retries(monkeypatch, tmp_path, failure):
    run, _clock = _case(monkeypatch, tmp_path)
    def response(_attempt, _timeout):
        if failure == "invalid_catalog":
            return FakeResponse(b"invalid catalog\n")
        value = requests.Response()
        value.status_code = 400 if failure == "http400" else 503
        value._content = CATALOG
        if failure == "response_on_transport_error":
            raise requests.ConnectionError(response=value)
        return value
    facts = run(response)
    assert not facts["started"] and facts["attempts"] == 1 and facts["sleeps"] == []


@pytest.mark.parametrize("cache_kind", ["primary", "recovery"])
def test_usable_verified_cache_keeps_original_time_and_one_attempt(monkeypatch, tmp_path, cache_kind):
    run, clock = _case(monkeypatch, tmp_path, cache_kind=cache_kind)
    def unavailable(_attempt, timeout):
        clock.elapsed += timeout
        raise requests.ReadTimeout()
    facts = run(unavailable)
    assert facts["started"] and facts["verified_snapshot"]
    assert facts["timeouts"] == [2.0] and facts["synthetic_elapsed_seconds"] == 2.0
    assert facts["sleeps"] == []


def test_remaining_window_stops_after_two_slow_combined_timeouts(monkeypatch, tmp_path):
    run, clock = _case(monkeypatch, tmp_path)
    def unavailable(_attempt, timeout):
        clock.elapsed += 2 * timeout
        raise requests.ReadTimeout()
    facts = run(unavailable)
    assert not facts["started"] and facts["attempts"] == 2
    assert facts["timeouts"] == [10.0] * 2 and facts["sleeps"] == [0.25]
    assert facts["synthetic_elapsed_seconds"] == 40.25


def test_immediate_transport_failure_uses_exactly_three_attempts(monkeypatch, tmp_path):
    run, _clock = _case(monkeypatch, tmp_path)
    def unavailable(_attempt, _timeout):
        raise requests.ConnectionError()
    facts = run(unavailable)
    assert not facts["started"] and facts["attempts"] == 3
    assert facts["timeouts"] == [10.0] * 3 and facts["sleeps"] == [0.25, 0.25]
    assert facts["synthetic_elapsed_seconds"] == 0.5


def test_unenrolled_bundled_path_keeps_original_time(monkeypatch, tmp_path):
    run, clock = _case(monkeypatch, tmp_path, enrolled=False)
    def unavailable(_attempt, timeout):
        clock.elapsed += timeout
        raise requests.ReadTimeout()
    facts = run(unavailable)
    assert facts["started"] and facts["path_is_bundle"]
    assert facts["timeouts"] == [2.0] and facts["synthetic_elapsed_seconds"] == 2.0
    assert facts["sleeps"] == []


def test_explicit_nondefault_timeout_is_preserved(monkeypatch, tmp_path):
    bundle, cache = tmp_path / "bundle.csv", tmp_path / "cache/Item.csv"
    bundle.write_bytes(CATALOG)
    profile = _profile()
    monkeypatch.setattr(logistics_runtime_profile, "load_logistics_runtime_profile", lambda **kwargs: profile)
    calls = []
    def successful(_url, **kwargs):
        calls.append(kwargs["timeout"])
        return FakeResponse(CATALOG)
    assert sync.refresh_item_catalog(bundle, cache_path=cache, timeout_seconds=0.5, get=successful) == cache
    assert calls == [0.5]

"""First-catalog response/retry boundaries through the real HTTP helper."""
import json
import os
from types import SimpleNamespace

import pytest
import requests
from urllib3.exceptions import ReadTimeoutError

import item_catalog_sync as sync
import logistics_runtime_profile
from tests.test_item_catalog_first_start_budget import _case, _startup_code
from tests.test_item_catalog_sync import CATALOG, _profile


class _Raw:
    def __init__(self, clock, *, body_timeout=False, slow=False):
        self.clock = clock
        self.body_timeout = body_timeout
        self.slow = slow
        self.closed = False

    def stream(self, amount, decode_content=True):
        if self.body_timeout:
            raise ReadTimeoutError(None, None, "simulated response body timeout")
        count = 6 if self.slow else 1
        for index in range(count):
            if self.slow:
                self.clock.elapsed += 8.1
            part = CATALOG[len(CATALOG) * index // count:len(CATALOG) * (index + 1) // count]
            for offset in range(0, len(part), amount):
                yield part[offset:offset + amount]

    def close(self):
        self.closed = True

    def release_conn(self):
        self.closed = True


class _Adapter(requests.adapters.HTTPAdapter):
    def __init__(self, clock, *, failure_times=(), status=200, body_timeout=False, slow=False):
        super().__init__()
        self.clock = clock
        self.failure_times = failure_times
        self.status = status
        self.body_timeout = body_timeout
        self.slow = slow
        self.timeouts = []
        self.raws = []
        self.closed = False

    def send(self, request, **kwargs):
        self.timeouts.append(kwargs["timeout"])
        if self.failure_times:
            self.clock.elapsed += self.failure_times[min(len(self.timeouts) - 1, len(self.failure_times) - 1)]
            raise requests.ReadTimeout("simulated failure before response")
        response = requests.Response()
        response.status_code = self.status
        response.url = request.url
        response.request = request
        response.raw = _Raw(self.clock, body_timeout=self.body_timeout, slow=self.slow)
        self.raws.append(response.raw)
        return response

    def close(self):
        self.closed = True
        super().close()


def _real_startup(monkeypatch, tmp_path, *, cache_kind="absent", **adapter_options):
    _unused, clock = _case(monkeypatch, tmp_path, cache_kind=cache_kind)
    profile = _profile(base_url="https://setup.test.invalid")
    monkeypatch.setattr(logistics_runtime_profile, "load_logistics_runtime_profile", lambda **kwargs: profile)
    adapter = _Adapter(clock, **adapter_options)
    original_session = requests.Session

    def session_factory():
        session = original_session()
        session.mount("https://", adapter)
        return session

    monkeypatch.setattr(sync.requests, "Session", session_factory)
    bundle = tmp_path / "bundle.csv"
    namespace = dict(resource_path=lambda path: str(bundle),
        Label_Match=SimpleNamespace(FILES=SimpleNamespace(ITEMS="Item.csv")),
        os=SimpleNamespace(path=os.path, environ={}), refresh_item_catalog=sync.refresh_item_catalog,
        requires_verified_catalog_snapshot=sync.requires_verified_catalog_snapshot,
        get_verified_catalog_snapshot=sync.get_verified_catalog_snapshot,
        ItemCatalogSyncError=sync.ItemCatalogSyncError,
        SNAPSHOT_UNAVAILABLE_AFTER_VERIFY=sync.SNAPSHOT_UNAVAILABLE_AFTER_VERIFY,
        ACTIVE_PATH_ENV=sync.ACTIVE_PATH_ENV)
    exec(_startup_code(), namespace)

    def run(*, explicit_timeout=None):
        try:
            selected = (namespace["prepare_startup_item_catalog"]() if explicit_timeout is None else
                sync.refresh_item_catalog(bundle, timeout_seconds=explicit_timeout))
            started = selected is not None
        except sync.ItemCatalogSyncError:
            started = False
        facts = dict(started=started, sends=len(adapter.timeouts), timeouts=adapter.timeouts,
            elapsed=clock.elapsed, sleeps=clock.sleeps, session_adapter_closed=adapter.closed,
            response_streams_closed=[raw.closed for raw in adapter.raws],
            actual_http_helper="item_catalog_sync._hardened_get", network_requests=0)
        (tmp_path / "response-budget-proof.json").write_text(json.dumps(facts), encoding="utf-8")
        return facts
    return run, clock


@pytest.mark.parametrize("status", [200, 503])
def test_body_timeout_after_response_never_retries(monkeypatch, tmp_path, status):
    run, _clock = _real_startup(monkeypatch, tmp_path, status=status, body_timeout=True)
    facts = run()
    assert not facts["started"]
    assert facts["sends"] == 1 and facts["sleeps"] == []
    assert facts["session_adapter_closed"] and facts["response_streams_closed"] == [True]


def test_twenty_second_failure_cannot_start_after_retry_sleep(monkeypatch, tmp_path):
    run, _clock = _real_startup(monkeypatch, tmp_path, failure_times=(20.0,))
    facts = run()
    assert not facts["started"] and facts["sends"] == 1
    assert facts["sleeps"] == [0.25] and facts["elapsed"] == 20.25


def test_exact_remaining_retry_boundary_stops_at_forty(monkeypatch, tmp_path):
    run, _clock = _real_startup(monkeypatch, tmp_path, failure_times=(19.75, 20.0))
    facts = run()
    assert not facts["started"] and facts["sends"] == 2
    assert facts["sleeps"] == [0.25] and facts["elapsed"] == 40.0


def test_retry_sleep_overshoot_prevents_next_send(monkeypatch, tmp_path):
    run, clock = _real_startup(monkeypatch, tmp_path, failure_times=(19.75,))
    original_sleep = clock.sleep
    monkeypatch.setattr(sync.time, "sleep", lambda seconds: original_sleep(seconds + 0.01))
    facts = run()
    assert not facts["started"] and facts["sends"] == 1
    assert facts["elapsed"] == pytest.approx(20.01)


def test_slow_body_stops_at_first_chunk_after_forty(monkeypatch, tmp_path):
    run, _clock = _real_startup(monkeypatch, tmp_path, slow=True)
    facts = run()
    assert not facts["started"] and facts["sends"] == 1 and facts["sleeps"] == []
    assert facts["elapsed"] == pytest.approx(40.5)
    assert facts["session_adapter_closed"] and facts["response_streams_closed"] == [True]


def test_immediate_failures_keep_three_sends(monkeypatch, tmp_path):
    run, _clock = _real_startup(monkeypatch, tmp_path, failure_times=(0.0,))
    facts = run()
    assert not facts["started"] and facts["sends"] == 3
    assert facts["sleeps"] == [0.25, 0.25] and facts["elapsed"] == 0.5


def test_explicit_timeout_keeps_original_one_send(monkeypatch, tmp_path):
    run, _clock = _real_startup(monkeypatch, tmp_path, failure_times=(0.0,))
    facts = run(explicit_timeout=0.5)
    assert not facts["started"] and facts["sends"] == 1
    assert facts["timeouts"] == [0.5] and facts["sleeps"] == []

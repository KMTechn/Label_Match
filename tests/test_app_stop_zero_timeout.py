"""Request-only stops publish both controls after the initial observation."""
import json
from types import SimpleNamespace

import pytest

import label_atomic_replace
import user_relay


@pytest.mark.parametrize("timeout_seconds", (0, "0"))
def test_zero_timeout_publishes_both_controls_after_slow_observation(tmp_path, monkeypatch, timeout_seconds):
    clock = [0.0]
    monkeypatch.setattr(user_relay.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(label_atomic_replace.time, "monotonic", lambda: clock[0])

    def observe(**_kwargs):
        clock[0] += 2.0
        return []

    monkeypatch.setattr(user_relay, "_app_stop_process_snapshot", observe)
    monkeypatch.setattr(user_relay, "_acquire_relay_lease", lambda _root: None)
    stop_relay = user_relay.request_user_relay_stop
    monkeypatch.setattr(
        user_relay, "request_user_relay_stop",
        lambda *args, **kwargs: stop_relay(
            *args, lease_factory=lambda _root: None,
            marker_lease_factory=lambda _root: SimpleNamespace(close=lambda: None), **kwargs),
    )
    waits = []
    result = user_relay.request_app_stop(
        tmp_path, app_root=tmp_path / "app", webview_root=tmp_path / "webview",
        timeout_seconds=timeout_seconds, wait=waits.append)

    application = json.loads(user_relay.app_stop_request_path(tmp_path).read_text(encoding="utf-8"))
    relay = json.loads(user_relay.user_relay_stop_path(tmp_path).read_text(encoding="utf-8"))
    assert application["schema_version"] == user_relay.APP_STOP_SCHEMA
    assert len(application["request_id"]) == 32
    assert relay["schema_version"] == user_relay.STOP_MARKER_V1
    assert relay["request_id"] == result["relay"]["request_id"]
    assert len(relay["request_id"]) == 32
    assert result["status"] == "UNKNOWN" and result["relay"]["status"] == "UNKNOWN"
    assert not waits

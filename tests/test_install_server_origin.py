"""Installer origin changes must preserve registration and queued work."""
import json

import pytest

import current_user_onboarding as onboarding
import user_relay
from tests.test_current_user_onboarding import (
    _credential_loader, _profile_loader, _ready_state, _write_json,
    isolated_data_generation_release,  # noqa: F401
    resolve_current_user_onboarding_paths,
)
from tests.test_relay_second_start_admission import live_relay  # noqa: F401
from tests.test_supported_lifecycle import _canonical_user_setup, _resume_user


def _origin(host="registered", *, scheme="https", port=""):
    return scheme + "://" + host + ".invalid" + (":" + port if port else "")


def _bind_origins(paths, installed, registered):
    from auto_update import installation_root
    marker = installation_root(paths.app_root) / "update-control" / "worker-pending.json"
    _write_json(marker, {"schema": "label-match-machine-worker-pending-v1",
                         "server_base_url": installed})
    credential = json.loads(paths.credential_path.read_text(encoding="utf-8"))
    credential["endpoint_url"] = registered + "/api/direct-sync"
    _write_json(paths.credential_path, credential)
    return marker


def _inspect(paths):
    return onboarding.inspect_current_user_state(
        paths, profile_loader=_profile_loader, credential_loader=_credential_loader)


def _live_origins(live, tmp_path, monkeypatch, installed, registered):
    paths = resolve_current_user_onboarding_paths(
        live.current, environ={"LOCALAPPDATA": str(tmp_path / "local")})
    _ready_state(paths)
    # The fast-path fixture's lease and live owner stay at this isolated root.
    from dataclasses import replace
    paths = replace(paths, direct_sync_root=live.current.parent / "user-state")
    _bind_origins(paths, installed, registered)
    monkeypatch.setattr(onboarding, "resolve_current_user_onboarding_paths", lambda _: paths)
    return paths


def test_equal_origin_keeps_ready_and_live_relay_reuse(live_relay, tmp_path, monkeypatch):
    paths = _live_origins(live_relay, tmp_path, monkeypatch,
                          _origin().upper() + "/", _origin(port="443"))
    profile = json.loads(paths.logistics_profile_path.read_text())
    profile["server_base_url"] = _origin("server-returned")
    _write_json(paths.logistics_profile_path, profile)
    assert _inspect(paths)["status"] == "READY"
    result = user_relay.reuse_or_start_user_relay_process(live_relay.current, wait=lambda _: None)
    assert result["existing_relay_reused"] is True


def test_changed_origin_refuses_onboarding_without_sending_or_mutating_work(tmp_path, monkeypatch):
    fixed, selected, environment, paths = _canonical_user_setup(tmp_path / "managed", monkeypatch)
    marker = _bind_origins(paths, _origin("installed"), _origin())
    # Pending completion has already been recorded: the machine marker still applies.
    monkeypatch.setattr(onboarding, "validate_pending_worker_install", lambda _: None)
    paths.queue_dir.mkdir(parents=True, exist_ok=True)
    (paths.queue_dir / "pending.json").write_bytes(b"queued-work")
    preserved = [marker, paths.identity_path, paths.producer_manifest_path,
                 paths.credential_path, paths.registration_report_path,
                 paths.logistics_profile_path, paths.logistics_secret_path,
                 paths.ledger_path, paths.queue_dir / "pending.json"]
    before = {path: path.read_bytes() for path in preserved}
    state = _inspect(paths)
    assert state["status"] == "RECOVERY_REQUIRED"
    assert state["cause_code"] == "INSTALL_SERVER_ORIGIN_CHANGED"
    monkeypatch.setattr(onboarding, "_registration_runner", lambda *_a, **_k: pytest.fail("sent enrollment"))
    with pytest.raises(onboarding.CurrentUserOnboardingError) as caught:
        _resume_user(selected, environment,
                     launcher=lambda _: pytest.fail("activated relay"),
                     autostart=lambda _: pytest.fail("changed persistence"))
    assert caught.value.status == "RECOVERY_REQUIRED"
    assert caught.value.cause_code == "INSTALL_SERVER_ORIGIN_CHANGED"
    report = json.loads(paths.onboarding_report_path.read_text(encoding="utf-8"))
    assert report["status"] == "RECOVERY_REQUIRED"
    assert report["server_registration_verified"] is False
    assert {path: path.read_bytes() for path in preserved} == before


def test_reinstalled_origin_never_reuses_running_old_relay(live_relay, tmp_path, monkeypatch):
    paths = _live_origins(live_relay, tmp_path, monkeypatch,
                          _origin("installed"), _origin())
    before = paths.credential_path.read_bytes()
    with pytest.raises(onboarding.CurrentUserOnboardingError) as caught:
        user_relay.reuse_or_start_user_relay_process(live_relay.current, wait=lambda _: None)
    assert caught.value.status == "RECOVERY_REQUIRED"
    assert caught.value.cause_code == "INSTALL_SERVER_ORIGIN_CHANGED"
    assert live_relay.observed == []
    assert paths.credential_path.read_bytes() == before


@pytest.mark.parametrize("change", ["scheme", "port"])
def test_scheme_and_port_changes_require_recovery(live_relay, tmp_path, monkeypatch, change):
    registered = _origin(scheme="http") if change == "scheme" else _origin(port="444")
    paths = _live_origins(live_relay, tmp_path, monkeypatch, _origin(), registered)
    assert _inspect(paths)["status"] == "RECOVERY_REQUIRED"


@pytest.mark.parametrize("unknown", ["missing-marker", "broken-marker", "missing-installed",
                                   "invalid-installed", "missing-registered", "invalid-registered",
                                   "unreadable-marker", "malformed-registered-host"])
def test_unavailable_origin_preserves_existing_ready_and_reuse(live_relay, tmp_path, monkeypatch, unknown):
    paths = _live_origins(live_relay, tmp_path, monkeypatch, _origin("installed"), _origin())
    marker = live_relay.current.parent / "update-control" / "worker-pending.json"
    if unknown == "unreadable-marker":
        from logistics_runtime_profile import LogisticsRuntimeConfigurationError
        def unreadable(*_args, **_kwargs):
            raise LogisticsRuntimeConfigurationError("record unreadable")
        monkeypatch.setattr(onboarding, "assert_path_has_no_reparse_components", unreadable)
    elif unknown == "missing-marker":
        marker.unlink()
    elif unknown == "broken-marker":
        marker.write_bytes(b"{")
    elif unknown in {"missing-installed", "invalid-installed"}:
        payload = json.loads(marker.read_text())
        if unknown == "missing-installed":
            payload.pop("server_base_url")
        else:
            payload["server_base_url"] = "unreadable"
        _write_json(marker, payload)
    else:
        payload = json.loads(paths.credential_path.read_text())
        if unknown == "missing-registered":
            payload.pop("endpoint_url")
        elif unknown == "malformed-registered-host":
            payload["endpoint_url"] = _origin() + "\\invalid/api/direct-sync"
        else:
            payload["endpoint_url"] = "unreadable"
        _write_json(paths.credential_path, payload)
    assert _inspect(paths)["status"] == "READY"
    assert user_relay.reuse_or_start_user_relay_process(
        live_relay.current, wait=lambda _: None)["existing_relay_reused"] is True

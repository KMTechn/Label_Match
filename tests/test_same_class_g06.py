"""G06: server-issued profile aliases must retain the ordinary fast reuse path."""
import json

import current_user_onboarding as onboarding
from auto_update import installation_root
from tests.test_window_first_startup import steady_start, reset_window_start
from tests.test_onboarding_lifecycle_scope import managed_onboarding
from tests.test_current_user_onboarding import (
    isolated_data_generation_release, _profile_loader, _credential_loader,
)


def test_fast_reuse_does_not_compare_registration_origin_with_profile_alias(steady_start, monkeypatch, tmp_path):
    item = steady_start
    misses = []
    monkeypatch.setattr(onboarding, "startup_timing_event", lambda _event, code, _started: misses.append(code))
    baseline = onboarding.try_window_first_start(item.paths.app_root, environ=item.environment)
    baseline_misses = list(misses)
    misses.clear()
    registration = json.loads(item.paths.registration_report_path.read_text(encoding="utf-8"))
    credential = json.loads(item.paths.credential_path.read_text(encoding="utf-8"))
    registration["server_base_url"] = "https://same-class-registration.invalid"
    credential["endpoint_url"] = registration["server_base_url"] + "/ingest"
    profile = json.loads(item.paths.logistics_profile_path.read_text(encoding="utf-8"))
    profile["base_url"] = "https://same-class-profile-alias.invalid"
    item.paths.logistics_profile_path.write_text(json.dumps(profile), encoding="utf-8")
    item.paths.registration_report_path.write_text(json.dumps(registration), encoding="utf-8")
    item.paths.credential_path.write_text(json.dumps(credential), encoding="utf-8")
    (installation_root(item.paths.app_root) / "update-control/worker-pending.json").write_text(
        json.dumps({"schema": "label-match-machine-worker-pending-v1",
                    "server_base_url": registration["server_base_url"]}), encoding="utf-8")
    slow_state = onboarding.inspect_current_user_state(
        item.paths, profile_loader=_profile_loader, credential_loader=_credential_loader)
    report = onboarding.try_window_first_start(item.paths.app_root, environ=item.environment)
    observed = {"fast_reused": report is not None,
                "baseline_fast_reused": baseline is not None, "baseline_misses": baseline_misses,
                "misses": misses, "slow_status": slow_state["status"],
                "profile_alias_matches_registration": False}
    ((tmp_path / "g06-observation.json")).write_text(
        json.dumps(observed), encoding="utf-8")
    assert baseline is not None, observed
    assert slow_state["status"] == "READY", observed
    assert report is not None, observed


def test_fast_reuse_still_refuses_a_changed_install_credential_origin(steady_start):
    from tests.test_install_server_origin import _bind_origins, _inspect

    item = steady_start
    marker = _bind_origins(
        item.paths, "https://installation.invalid", "https://registration.invalid"
    )
    preserved = {
        path: path.read_bytes()
        for path in (marker, item.paths.credential_path, item.paths.ledger_path)
    }
    assert onboarding.try_window_first_start(
        item.paths.app_root, environ=item.environment
    ) is None
    state = _inspect(item.paths)
    assert state["status"] == "RECOVERY_REQUIRED"
    assert state["cause_code"] == "INSTALL_SERVER_ORIGIN_CHANGED"
    assert all(path.read_bytes() == raw for path, raw in preserved.items())

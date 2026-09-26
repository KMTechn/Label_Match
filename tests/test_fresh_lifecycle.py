"""Exercise archive -> real registration publication -> deferred activation."""

import json
from pathlib import Path

import pytest

import current_user_onboarding as onboarding
import direct_sync_runtime
import logistics_runtime_profile
from tools import install_logistics_runtime_profile as profiles
from tools import register_label_match_worker_pc as registration
from tests.test_fresh_server_transition import fresh, write_json
from tests.test_fresh_enrollment import candidate, response, Reply, Peer
from tests.test_current_user_onboarding import _ready_state


@pytest.fixture
def lifecycle(fresh, candidate, monkeypatch):
    transition, env, paths, target, archive = fresh
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(transition, "_candidate", registration.prepare_fresh_candidate)

    def protect(value):
        return b"FAKE-DPAPI:" + value.encode()

    def unprotect(value):
        assert value.startswith(b"FAKE-DPAPI:")
        return value.removeprefix(b"FAKE-DPAPI:").decode()

    monkeypatch.setattr(registration, "_dpapi_protect_current_user", protect)
    monkeypatch.setattr(registration, "_dpapi_unprotect_current_user", unprotect)
    monkeypatch.setattr(profiles, "protect_current_user_secret", protect)
    monkeypatch.setattr(profiles, "unprotect_current_user_secret", unprotect)
    monkeypatch.setattr(onboarding, "unprotect_current_user_secret", unprotect)
    monkeypatch.setattr(direct_sync_runtime, "_dpapi_unprotect_current_user", lambda value: unprotect(value).encode())
    _ready_state(paths)
    paths.data_root.mkdir(parents=True, exist_ok=True)
    (paths.data_root / "old.csv").write_bytes(b"OLD-BUSINESS-MUST-NOT-UPLOAD")
    write_json(paths.settings_path, {"custom_save_path": str(paths.data_root),
        "ui_settings": {"base_font_size": 15, "default_font": "Segoe UI", "producer_id": "old-id", "path": "C:/old"},
        "ui_persistence": {"scale_factor": 1.6, "old_session": {"lease": "old"}}})
    state = transition.prepare_fresh_server_registration(paths, target, archive, environ=env,
                confirm=True, confirm_unsent=True)
    peer = Peer([Reply(200, response(state))])
    monkeypatch.setattr(registration, "_fresh_session", lambda *a: peer)
    return transition, env, paths, state, peer


def test_real_publication_never_reuses_old_secrets_and_pins_all_consumers(lifecycle, monkeypatch):
    transition, env, paths, before, peer = lifecycle
    result = transition.register_fresh_server(paths, environ=env)
    assert result["phase"] == "REGISTERED"
    new = transition._bound_paths(result, new=True)
    assert onboarding.inspect_current_user_state(new)["status"] == "READY"
    credential = direct_sync_runtime.load_credentials_from_json(new.credential_path)
    assert credential.secret == "synthetic-new-producer-secret"
    assert credential.producer_id == result["candidate"]["producer_id"]
    assert not (new.data_root / "old.csv").exists()
    assert transition.archived_path(result, str(paths.data_root / "old.csv")).read_bytes() == b"OLD-BUSINESS-MUST-NOT-UPLOAD"
    assert all("OLD-BUSINESS" not in json.dumps(call) for call in peer.calls)
    # A stale process override, machine fallback or old settings cannot win.
    alternate = {**env, "LABEL_MATCH_SAVE_DIR": str(paths.data_root.parent / "old-root"),
                 "KM_LOGISTICS_PROFILE_PATH": str(Path(env["PROGRAMDATA"]) / "old-profile.json")}
    selected = onboarding.resolve_current_user_onboarding_paths(paths.app_root, environ=alternate)
    assert selected.data_root == new.data_root
    assert selected.logistics_profile_path == new.logistics_profile_path
    assert logistics_runtime_profile._runtime_environment(alternate)["KM_LOGISTICS_PROFILE_PATH"] == str(new.logistics_profile_path)
    assert len(peer.calls) == 1
    assert result["possession_key"] == before["possession_key"]
    preferences = json.loads(new.settings_path.read_text(encoding="utf-8"))
    assert preferences == {"custom_save_path": str(paths.data_root),
                           "ui_settings": {"base_font_size": 15, "default_font": "Segoe UI"},
                           "ui_persistence": {"scale_factor": 1.6}}


def test_partial_publication_reuses_protected_response_and_preserves_partial_bytes(lifecycle, monkeypatch):
    transition, env, paths, before, peer = lifecycle
    original = registration._write_json
    attempts = []

    def fail_once(path, payload):
        if Path(path) == paths.credential_path and not attempts:
            attempts.append(True)
            raise OSError("publication interrupted")
        original(path, payload)

    monkeypatch.setattr(registration, "_write_json", fail_once)
    with pytest.raises(OSError, match="publication interrupted"):
        transition.register_fresh_server(paths, environ=env)
    assert paths.identity_path.exists()
    result = transition.register_fresh_server(paths, environ=env)
    assert result["phase"] == "REGISTERED"
    assert len(peer.calls) == 1  # no token rotation / second registration
    assert result["partial_publications"]
    for entry in result["partial_publications"]:
        assert transition._byte_shape(transition._snapshot(Path(entry["archive"]))) == transition._byte_shape(entry["snapshot"])
    assert onboarding.inspect_current_user_state(transition._bound_paths(result, new=True))["status"] == "READY"


@pytest.mark.parametrize("failure", ["lease", "relay", "run", "none"])
def test_activation_defers_run_until_first_lease_and_relay(lifecycle, monkeypatch, failure):
    import current_user_scheduled_task as tasks
    import user_relay
    transition, env, paths, before, peer = lifecycle
    transition.register_fresh_server(paths, environ=env)
    events = []
    real_onboard = onboarding.onboard_current_user

    def deferred(*args, **kwargs):
        assert kwargs["defer_activation"] is True
        kwargs["require_bootstrap_integrity"] = False
        kwargs["autostart_installer"] = lambda *a: pytest.fail("Run enabled by deferred onboarding")
        kwargs["relay_launcher"] = lambda *a: pytest.fail("relay started by deferred onboarding")
        result = real_onboard(*args, **kwargs)
        events.append("onboard")
        return result

    def lease(*args):
        events.append("lease")
        if failure == "lease":
            raise OSError("first lease unavailable")
        return {"status": "ACTIVE", "server_grant_accepted": True}

    monkeypatch.setattr(onboarding, "onboard_current_user", deferred)
    monkeypatch.setattr(transition, "_require_quiescence", lambda *a: {"status": "QUIESCED", "relay": {}})
    monkeypatch.setattr(transition, "_first_lease", lease)
    monkeypatch.setattr(tasks, "remove_current_user_scheduled_task", lambda *a: {"status": "ABSENT"})
    monkeypatch.setattr(user_relay, "start_user_relay_process", lambda *a: events.append("relay") or
                        {"status": "UNKNOWN" if failure == "relay" else "ALIVE"})
    monkeypatch.setattr(user_relay, "install_user_relay_autostart", lambda *a: events.append("run") or
                        {"status": "UNKNOWN" if failure == "run" else "PASS"})
    monkeypatch.setattr(user_relay, "remove_user_relay_autostart", lambda *a: events.append("remove-run") or {"status": "PASS"})
    monkeypatch.setattr(user_relay, "request_user_relay_stop", lambda *a: events.append("stop-relay") or {"status": "ABSENT"})
    if failure == "none":
        result = transition.activate_fresh_server(paths, environ=env)
        assert result["phase"] == "ACTIVATED"
        assert events == ["onboard", "lease", "relay", "run"]
        transition.activate_fresh_server(paths, environ=env)
        assert events == ["onboard", "lease", "relay", "run"]
    else:
        with pytest.raises((OSError, transition.FreshTransitionError)):
            transition.activate_fresh_server(paths, environ=env)
        if failure != "run":
            assert "run" not in events
        assert events[-2:] == ["remove-run", "stop-relay"]
        state = transition._json(transition.transition_record_path(paths, env), 16 * 1024 * 1024)
        assert state["phase"] == "REGISTERED"
        assert state["attempts"][-1]["status"] == "UNKNOWN"
        failure = "none"
        resumed = transition.activate_fresh_server(paths, environ=env)
        assert resumed["phase"] == "ACTIVATED"
        assert resumed["attempts"][-1]["status"] == "PASS"

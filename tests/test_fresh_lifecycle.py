"""Exercise archive -> real registration publication -> deferred activation."""

from dataclasses import replace
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
    return _archived(fresh, monkeypatch)


@pytest.fixture
def nested_lifecycle(fresh, candidate, monkeypatch):
    """The supported LABEL_MATCH_SETTINGS_PATH=<data root>/app_settings.json placement."""
    transition, env, paths, target, archive = fresh
    paths = replace(paths, settings_path=paths.data_root / "app_settings.json")
    env = dict(env, LABEL_MATCH_SETTINGS_PATH=str(paths.settings_path))
    return _archived((transition, env, paths, target, archive), monkeypatch)


def _archived(fresh, monkeypatch):
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


def _activate_with_boundaries(transition, env, paths, monkeypatch):
    """Real deferred onboarding; only OS task/relay/Run and the network lease are replaced."""
    import current_user_scheduled_task as tasks
    import user_relay
    real_onboard = onboarding.onboard_current_user

    def deferred(*args, **kwargs):
        assert kwargs["defer_activation"] is True
        kwargs["require_bootstrap_integrity"] = False
        kwargs["autostart_installer"] = lambda *a: pytest.fail("Run enabled by deferred onboarding")
        kwargs["relay_launcher"] = lambda *a: pytest.fail("relay started by deferred onboarding")
        return real_onboard(*args, **kwargs)

    monkeypatch.setattr(onboarding, "onboard_current_user", deferred)
    monkeypatch.setattr(transition, "_require_quiescence", lambda *a: {"status": "QUIESCED", "relay": {}})
    monkeypatch.setattr(transition, "_first_lease", lambda *a: {"status": "ACTIVE", "server_grant_accepted": True})
    monkeypatch.setattr(tasks, "remove_current_user_scheduled_task", lambda *a: {"status": "ABSENT"})
    monkeypatch.setattr(user_relay, "start_user_relay_process", lambda *a: {"status": "ALIVE"})
    monkeypatch.setattr(user_relay, "install_user_relay_autostart", lambda *a: {"status": "PASS"})
    monkeypatch.setattr(user_relay, "remove_user_relay_autostart", lambda *a: {"status": "PASS"})
    monkeypatch.setattr(user_relay, "request_user_relay_stop", lambda *a: {"status": "ABSENT"})
    return transition.activate_fresh_server(paths, environ=env)


@pytest.mark.parametrize("interruption", ["none", "after_settings", "atomic_copy_left"])
def test_settings_published_inside_the_data_root_reach_activation(nested_lifecycle, monkeypatch, interruption):
    transition, env, paths, before, peer = nested_lifecycle
    if interruption == "after_settings":
        # Crash after the new settings exist but before REGISTERED is durable.
        publish = transition._publish_runtime_binding
        calls = []

        def interrupted(*args):
            calls.append(1)
            if len(calls) == 1:
                raise OSError("interrupted after settings publication")
            return publish(*args)

        monkeypatch.setattr(transition, "_publish_runtime_binding", interrupted)
    elif interruption == "atomic_copy_left":
        # A locked target can leave the complete atomic copy beside the settings.
        write = transition._write_json_atomic

        def left_copy(path, payload):
            if Path(path) == paths.settings_path and not left:
                write(path.with_name("." + path.name + ".4242.aaaa.tmp"), payload)
                left.append(path)
                raise PermissionError("settings target locked")
            return write(path, payload)

        left = []
        monkeypatch.setattr(transition, "_write_json_atomic", left_copy)
    if interruption != "none":
        with pytest.raises(OSError):
            transition.register_fresh_server(paths, environ=env)
        assert transition._json(transition.transition_record_path(paths, env), 16 * 1024 * 1024)["phase"] == "REGISTERING"
    registered = transition.register_fresh_server(paths, environ=env)
    new = transition._bound_paths(registered, new=True)
    assert registered["phase"] == "REGISTERED" and len(peer.calls) == 1
    assert new.settings_path.parent == new.data_root and new.settings_path.is_file()
    assert not (new.data_root / "old.csv").exists()
    result = _activate_with_boundaries(transition, env, paths, monkeypatch)
    assert result["phase"] == "ACTIVATED"
    assert json.loads(new.settings_path.read_text(encoding="utf-8"))["custom_save_path"] == str(new.data_root)


@pytest.mark.parametrize("new_work", ["business_csv", "settings_changed", "partial_atomic_copy"])
def test_real_new_local_work_beside_published_settings_is_still_refused(nested_lifecycle, monkeypatch, new_work):
    transition, env, paths, before, peer = nested_lifecycle
    registered = transition.register_fresh_server(paths, environ=env)
    new = transition._bound_paths(registered, new=True)
    if new_work == "business_csv":
        (new.data_root / "포장실작업이벤트로그_new_20260927.csv").write_bytes(b"NEW-BUSINESS\r\n")
    elif new_work == "settings_changed":
        settings = json.loads(new.settings_path.read_text(encoding="utf-8"))
        settings["custom_save_path"] = str(new.data_root.parent / "elsewhere")
        new.settings_path.write_text(json.dumps(settings), encoding="utf-8")
    else:
        (new.data_root / ("." + new.settings_path.name + ".4242.bbbb.tmp")).write_bytes(new.settings_path.read_bytes()[:7])
    with pytest.raises(transition.FreshTransitionError, match="새 로컬 파일"):
        _activate_with_boundaries(transition, env, paths, monkeypatch)
    state = transition._json(transition.transition_record_path(paths, env), 16 * 1024 * 1024)
    assert state["phase"] == "REGISTERED"


def test_status_keeps_the_completed_record_in_progress_until_its_fence_is_released(lifecycle, fresh, monkeypatch, tmp_path):
    import writer_session_fence as fence
    from tests.test_writer_session_fence import _payload, _write_active
    transition, env, paths, before, peer = lifecycle
    _transition, _env, _paths, target, archive = fresh
    transition.register_fresh_server(paths, environ=env)
    activated = _activate_with_boundaries(transition, env, paths, monkeypatch)
    payload = _payload(delegated_sources=["fresh_server_transition"], token="t" * 64)
    payload.update(activated["attempts"][-1]["authority"])
    payload["session_authority_mutex_name"] = fence.session_authority_mutex_name(
        payload["session_id"], payload["attempt_id"], payload["orchestrator_sha256"],
        payload["replacement_transaction_id"], payload["writer_contract_sha256"])
    control = fence.canonical_control_root()
    _write_active(control, payload)

    def status():
        result = tmp_path / "status.json"
        assert transition.transition_main(["--app-root", str(paths.app_root), "--transition-input", str(target),
                                           "--archive-volume", str(archive), "--action", "status",
                                           "--result-path", str(result)]) == 0
        return json.loads(result.read_text(encoding="utf-8"))

    pending = status()
    assert pending["phase"] == "ACTIVATED"
    assert pending["completion"] == "FENCE_RELEASE_PENDING" and "Resume" in pending["next_action"]
    assert pending["terminal_authority"] == activated["attempts"][-1]["authority"]
    (control / "active.json").unlink()
    assert status()["completion"] == "COMPLETE"

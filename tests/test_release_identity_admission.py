"""Installed-packet hashing must precede admission, with G still checked per open."""
import json
from pathlib import Path
import runpy
import shutil
import sys
from types import SimpleNamespace

import pytest

import current_user_onboarding as onboarding
import label_data_generation as generation
import label_match_product_host as host
import protected_admin
import writer_session_fence as fence
from tests.test_current_user_onboarding import _write_bootstrap_inventory_record


@pytest.fixture
def packet(monkeypatch, tmp_path):
    root = tmp_path / "packet"
    (root / "app").mkdir(parents=True)
    (root / "runtime").mkdir()
    (root / "app" / "main.py").write_text("pass\n", encoding="utf-8")
    (root / "runtime" / "pythonw.exe").write_bytes(b"synthetic runtime")
    (root / "portable-manifest.json").write_text(json.dumps({
        "schema": "label-match-portable-tree-v1", "version": generation.CURRENT_VERSION,
        "source_commit": "a" * 40,
    }), encoding="utf-8")
    _write_bootstrap_inventory_record(root)
    monkeypatch.setattr(protected_admin, "current_process_sid", lambda: "S-1-5-21-900-901-902-1001")
    monkeypatch.setattr(host, "_default_product_root", lambda: root)
    return root


@pytest.fixture
def verification_call_sources():
    return []


@pytest.fixture
def verification_calls(monkeypatch, verification_call_sources):
    calls = []
    verify = onboarding.verify_bootstrap_integrity

    def counted(paths, *, required):
        calls.append(bool(getattr(fence._WRITER_LOCAL, "depth", 0)))
        verification_call_sources.append(sys._getframe(1).f_code.co_name)
        return verify(paths, required=required)

    monkeypatch.setattr(onboarding, "verify_bootstrap_integrity", counted)
    return calls


def test_direct_runner_entry_hashes_once_before_admission(packet, verification_calls, monkeypatch, tmp_path):
    import direct_sync_runtime
    from tools import direct_sync_relay_runner as runner

    monkeypatch.setattr(direct_sync_runtime, "prepare_credentials_from_json", lambda *_a, **_kw: object())
    scan = tmp_path / "scan"
    scan.mkdir()
    arguments = [str(runner.__file__), "--db-path", str(tmp_path / "relay.sqlite3"),
        "--spool-dir", str(tmp_path / "spool"), "--producer-manifest-path", str(tmp_path / "manifest.json"),
        "--credential-path", str(tmp_path / "credential.json"), "--scan-source-dir", str(scan),
        "--upload-status-dir", str(tmp_path / "upload"), "--log-path", str(tmp_path / "relay.jsonl"),
        "--runtime-status-path", str(tmp_path / "status.json"), "--baseline-existing-source-files"]
    monkeypatch.setattr(sys, "argv", arguments)
    with pytest.raises(SystemExit) as exit_info:
        runpy.run_path(str(runner.__file__), run_name="__main__")
    assert exit_info.value.code == 0
    print("runner verification total/inside:", len(verification_calls), sum(verification_calls))
    assert verification_calls == [False]
    assert (scan / "data-generation.json").is_file()


def test_resident_feed_profile_cycles_reuse_startup_verification(packet, verification_calls, monkeypatch, tmp_path):
    import auto_update
    import direct_sync_runtime
    import logistics_runtime_profile as profile_module
    import requests
    import user_relay
    from tests.test_logistics_runtime_profile import _profile

    paths = onboarding.resolve_current_user_onboarding_paths(packet)
    _profile(tmp_path, profile_path=paths.logistics_profile_path)
    # Match the module location in an installed packet; only native decryption
    # and the external process/HTTP boundaries use synthetic dependencies.
    monkeypatch.setattr(profile_module, "__file__", str(packet / "app" / "logistics_runtime_profile.py"))
    monkeypatch.setattr(profile_module, "unprotect_current_user_secret", lambda _value: "fixture-auth")
    monkeypatch.setattr(onboarding, "_default_profile_loader", lambda path:
        profile_module.load_logistics_runtime_profile(required=True, profile_path=path,
            decryptor=lambda _value: "fixture-auth"))
    monkeypatch.setattr(onboarding, "apply_current_user_runtime_environment", lambda _paths: None)
    monkeypatch.setattr(direct_sync_runtime, "prepare_credentials_from_json", lambda *_a, **_kw: object())
    monkeypatch.setattr(auto_update, "startup_gate", lambda *_a, **_kw: None)
    monkeypatch.setattr(auto_update, "trial_stop_requested", lambda _code: False)
    monkeypatch.setattr(auto_update, "relay_ready_metadata", lambda _code: {})
    control = tmp_path / "update-control"
    config = control.parent / "current" / "app" / "update-manifest-key-config.json"
    config.parent.mkdir(parents=True)
    config.write_text(json.dumps({"manifest_public_key": "synthetic"}), encoding="utf-8")
    control.mkdir()
    (control / "active.json").write_text(json.dumps({"sha": "a" * 40}), encoding="utf-8")
    monkeypatch.setattr(auto_update, "load_policy", lambda _control: {
        "feed_url": "https://logistics.example.invalid/feed", "profile_auth": True})
    monkeypatch.setattr(auto_update, "version_bundle", lambda *_a: packet)
    monkeypatch.setattr(auto_update, "version_manifest", lambda _root: {"version": generation.CURRENT_VERSION})
    monkeypatch.setattr(auto_update, "fixed_helper", lambda *_a, **_kw: None)
    runtime_cycle = user_relay._runtime_cycle
    cycle_counts = []

    def configured_cycle(**kwargs):
        before = len(verification_calls)
        try:
            with monkeypatch.context() as update_configuration:
                update_configuration.setattr(auto_update, "control_root", lambda _root: control)
                return runtime_cycle(**kwargs)
        finally:
            sample = verification_calls[before:]
            cycle_counts.append((len(sample), sum(sample)))

    monkeypatch.setattr(user_relay, "_runtime_cycle", configured_cycle)

    class OfflineSession:
        cookies = SimpleNamespace(clear=lambda: None)

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def get(self, *_args, **_kwargs):
            raise OSError("synthetic offline feed")

    monkeypatch.setattr(requests, "Session", OfflineSession)
    monkeypatch.setattr(user_relay, "run_session_direct_sync_once", lambda **_kwargs: {"status": "IDLE"})
    loop = user_relay.run_persistent_relay_loop

    def three_cycles(run_cycle, **kwargs):
        kwargs.update(max_cycles=3, interval_seconds=0)
        return loop(run_cycle, **kwargs)

    monkeypatch.setattr(user_relay, "run_persistent_relay_loop", three_cycles)
    assert user_relay.main(["--app-root", str(packet), "--direct-sync-root", str(tmp_path / "sync"),
        "--scan-source-dir", str(tmp_path / "scan"), "--once"]) == 0
    result = json.loads(user_relay.user_relay_status_path(tmp_path / "sync").read_text(encoding="utf-8"))
    assert result["cycle_count"] == 3
    assert result["last_cycle"]["auto_update"]["status"] == "DEFERRED"
    print("resident verification total/inside:", len(verification_calls), sum(verification_calls))
    print("resident cycle verification total/inside:", cycle_counts)
    assert verification_calls == [False]
    assert cycle_counts == [(0, 0)] * 3


@pytest.mark.parametrize("with_onboarding", [False, True])
def test_gui_startup_hashes_before_guarded_data_open(
    packet, verification_calls, verification_call_sources, monkeypatch, tmp_path, with_onboarding,
):
    import auto_update
    import Label_Match as app

    monkeypatch.setattr(auto_update, "startup_gate", lambda *_a, **_kw: None)
    monkeypatch.setattr(app, "dispatch_product_mode", lambda _argv: None)
    monkeypatch.setattr(app, "verify_factory_contract_startup", lambda: None)
    monkeypatch.setattr(app, "_label_match_startup_trace", lambda *_a, **_kw: None)
    monkeypatch.setattr(app, "_first_run_onboarding_enabled", lambda: with_onboarding)
    monkeypatch.setattr(app, "_label_match_runtime_app_root", lambda: packet)
    monkeypatch.setattr(app, "_default_label_match_settings_path", lambda: tmp_path / "settings.json")
    data = tmp_path / "gui-data"
    monkeypatch.setattr(app, "resolve_data_scope", lambda **_kw: str(data))
    if with_onboarding:
        from tests.test_current_user_onboarding import (
            _ready_state, _profile_loader, _credential_loader, _ledger_factory,
            _autostart, _scheduled_task_absent, _legacy_task_quiescent, _relay_start,
        )
        paths = onboarding.resolve_current_user_onboarding_paths(packet)
        _ready_state(paths)
        monkeypatch.setattr(onboarding, "prepare_credentials_from_json", lambda path, **_kw:
            SimpleNamespace(error=None, payload=json.loads(Path(path).read_text(encoding="utf-8"))))
        onboard = onboarding.onboard_current_user
        monkeypatch.setattr(app, "onboard_current_user", lambda root, **kwargs: onboard(
            root, **kwargs, profile_loader=_profile_loader, credential_loader=_credential_loader,
            ledger_factory=_ledger_factory, autostart_installer=_autostart,
            scheduled_task_remover=_scheduled_task_absent,
            legacy_task_quiescence_reader=_legacy_task_quiescent, relay_launcher=_relay_start,
        ))

    def open_data():
        generation.record_user_data_open(packet, (data,))
        assert (data / "data-generation.json").is_file()
        return 0

    # Keep the real single-instance entry and data_generation_record admission;
    # replace only the GUI/business constructor to avoid a real desktop window.
    monkeypatch.setattr(app, "_run_label_match_application", open_data)
    assert app.main([]) == 0
    print("GUI verification total/inside:", len(verification_calls), sum(verification_calls))
    derived = [inside for inside, caller in zip(verification_calls, verification_call_sources)
               if caller == "release_identity"]
    direct = [inside for inside, caller in zip(verification_calls, verification_call_sources)
              if caller == "onboard_current_user"]
    assert derived == [False]
    # User decision: the existing onboarding check is a separate startup cost.
    # Integrated sub-474 keeps this direct check outside its short admissions.
    assert direct == ([False] if with_onboarding else [])
    assert len(verification_calls) == len(derived) + len(direct)


@pytest.mark.parametrize("failure", ["absent", "tampered", "non_file"])
def test_failed_verification_is_retried_and_keeps_the_same_failure(packet, verification_calls, failure):
    if failure == "absent":
        (packet / "bootstrap-integrity.json").unlink()
    elif failure == "non_file":
        (packet / "bootstrap-integrity.json").unlink()
        (packet / "bootstrap-integrity.json").mkdir()
    else:
        (packet / "app" / "main.py").write_text("changed\n", encoding="utf-8")
    messages = []
    for _ in range(2):
        with pytest.raises(generation.DataGenerationError) as error:
            generation.check_user_data_open(packet)
        messages.append(str(error.value))
    assert messages == [generation.UNKNOWN_MESSAGE] * 2
    assert verification_calls == [False, False]
    if failure == "non_file":
        (packet / "bootstrap-integrity.json").rmdir()
    _write_bootstrap_inventory_record(packet)
    generation.check_user_data_open(packet)
    generation.check_user_data_open(packet)
    assert verification_calls == [False, False, False]


def test_identity_copy_and_entrypoint_scoped_cache(packet, verification_calls, monkeypatch):
    monkeypatch.setattr(sys, "argv", [str(packet / "app" / "main.py")])
    first = generation.release_identity(packet)
    first["build_id"] = "b" * 40
    alias = packet.parent / "missing" / ".." / packet.name
    assert generation.release_identity(alias)["build_id"] == "a" * 40
    assert verification_calls == [False]
    other = packet.with_name("other-packet")
    shutil.copytree(packet, other)
    _write_bootstrap_inventory_record(other)
    assert generation.release_identity(other)["build_id"] == "a" * 40
    assert verification_calls == [False, False]
    monkeypatch.setattr(sys, "argv", [str(packet / "app" / "relay.py")])
    assert generation.release_identity(packet)["build_id"] == "a" * 40
    assert verification_calls == [False, False, False]

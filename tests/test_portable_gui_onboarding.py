import os
from pathlib import Path
import sys

import pytest

import Label_Match as app
import current_user_onboarding as onboarding
import label_match_product_host as product_host
import logistics_runtime_profile as profiles


pytestmark = pytest.mark.skipif(os.name != "nt", reason="Windows GUI onboarding")


@pytest.fixture
def portable_gui(monkeypatch, tmp_path):
    release = tmp_path / "installed"
    (release / "app").mkdir(parents=True)
    (release / "runtime").mkdir()
    (release / "runtime" / "pythonw.exe").write_bytes(b"runtime fixture")
    (release / "portable-manifest.json").write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(app, "__file__", str(release / "app" / "Label_Match.py"))
    monkeypatch.setattr(
        product_host, "__file__", str(release / "app" / "label_match_product_host.py")
    )
    monkeypatch.setattr(sys, "executable", str(release / "runtime" / "pythonw.exe"))
    monkeypatch.setattr(sys, "frozen", False, raising=False)
    monkeypatch.delenv(app.LABEL_MATCH_ENABLE_FIRST_RUN_ONBOARDING_ENV, raising=False)
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "operator-local"))
    for name in (
        "LABEL_MATCH_SAVE_DIR", "LABEL_MATCH_SETTINGS_PATH",
        "LABEL_MATCH_DIRECT_SYNC_ROOT", "LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT",
        "KM_LOGISTICS_PROFILE_PATH", "KM_LOGISTICS_REQUIRED",
    ):
        # Track these keys before the real activation function replaces them.
        monkeypatch.setenv(name, "")
    monkeypatch.setattr(profiles, "_machine_environment_value", lambda _name: "")
    monkeypatch.setattr(app, "verify_factory_contract_startup", lambda: None)
    monkeypatch.setattr(app, "_label_match_startup_trace", lambda *_args, **_kwargs: None)
    return release.resolve()


def test_normal_portable_gui_onboards_before_selecting_data_scope_and_workflow(
    monkeypatch, portable_gui
):
    calls = []
    paths = onboarding.resolve_current_user_onboarding_paths(portable_gui)

    def normal_onboarding(root, **kwargs):
        assert Path(root) == portable_gui
        assert kwargs["require_bootstrap_integrity"] is True
        calls.append("onboarding")
        onboarding.apply_current_user_runtime_environment(paths)
        return {"status": "READY", "bootstrap_integrity": {"status": "PASS"}}

    def guarded(start, *, data_scope):
        assert calls == ["onboarding"], "portable GUI bypassed normal onboarding"
        assert Path(data_scope) == paths.data_root
        calls.append("guard")
        return start()

    def application():
        assert Path(app._default_label_match_settings_path()) == paths.settings_path
        assert Path(os.environ["LABEL_MATCH_SAVE_DIR"]) == paths.data_root
        assert profiles.selected_logistics_runtime_profile_path() == paths.logistics_profile_path
        assert profiles.logistics_runtime_required() is True
        calls.append("application")
        return 0

    monkeypatch.setattr(app, "onboard_current_user", normal_onboarding)
    monkeypatch.setattr(app, "run_guarded_entrypoint", guarded)
    monkeypatch.setattr(app, "_run_label_match_application", application)

    assert app.main([]) == 0
    assert calls == ["onboarding", "guard", "application"]


def test_portable_onboarding_failure_prevents_data_scope_and_gui(
    monkeypatch, portable_gui
):
    failures = []
    reached_gui = []
    failure = onboarding.CurrentUserOnboardingError(
        "readback failed", report_path=portable_gui / "failure.json", status="FAILED"
    )

    def reject(root, **kwargs):
        assert Path(root) == portable_gui
        assert kwargs["require_bootstrap_integrity"] is True
        raise failure

    monkeypatch.setattr(app, "onboard_current_user", reject)
    monkeypatch.setattr(app, "_show_first_run_onboarding_error", failures.append)
    monkeypatch.setattr(
        app, "run_guarded_entrypoint", lambda *_args, **_kwargs: reached_gui.append(True) or 0
    )

    assert app.main([]) == app.ONBOARDING_EXIT_CODE
    assert failures == [failure]
    assert reached_gui == []


def test_source_gui_remains_opt_in_and_uses_source_root(monkeypatch, tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    monkeypatch.setattr(sys, "frozen", False, raising=False)
    monkeypatch.setattr(app, "__file__", str(source / "Label_Match.py"))
    monkeypatch.setattr(product_host, "__file__", str(source / "label_match_product_host.py"))
    monkeypatch.delenv(app.LABEL_MATCH_ENABLE_FIRST_RUN_ONBOARDING_ENV, raising=False)
    monkeypatch.setenv("LABEL_MATCH_PORTABLE_ROOT", str(source))

    assert app._first_run_onboarding_enabled() is False
    assert Path(app._label_match_runtime_app_root()) == source
    monkeypatch.setenv(app.LABEL_MATCH_ENABLE_FIRST_RUN_ONBOARDING_ENV, "1")
    assert app._first_run_onboarding_enabled() is True


def test_frozen_gui_retains_executable_root_and_automatic_onboarding(monkeypatch, tmp_path):
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    monkeypatch.setattr(sys, "executable", str(tmp_path / "Label_Match.exe"))
    monkeypatch.delenv(app.LABEL_MATCH_ENABLE_FIRST_RUN_ONBOARDING_ENV, raising=False)

    assert app._first_run_onboarding_enabled() is True
    assert Path(app._label_match_runtime_app_root()) == tmp_path

import os
import json
from tests.test_current_user_onboarding import isolated_data_generation_release  # noqa: F401
from pathlib import Path
import sys

import pytest

import Label_Match as app
import current_user_onboarding as onboarding
import label_match_product_host as product_host
import logistics_runtime_profile as profiles
import startup_observation as observation


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




@pytest.mark.parametrize("cause,registration_status", [
    ("POSSESSION_PROOF_REATTACH_REQUIRED", "POSSESSION_PROOF_REATTACH_REQUIRED"),
    ("ADMIN_RECOVERY_ACTION", "ADMIN_RECOVERY_REQUIRED"),
])
@pytest.mark.parametrize("arguments", [[], ["--old-screen"]], ids=["new-screen", "old-screen"])
def test_actual_first_setup_and_report_only_restart_show_registration_recovery(
    monkeypatch, portable_gui, tmp_path, cause, registration_status, arguments,
):
    from tests.test_current_user_onboarding import _write_json, _legacy_task_quiescent

    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "local"))
    monkeypatch.setenv("APPDATA", str(tmp_path / "roaming"))
    paths = onboarding.resolve_current_user_onboarding_paths(portable_gui)
    attempts, windows, records = [], [], []

    def forbidden(*_args, **_kwargs):
        pytest.fail("recovery refusal reached a later startup stage")

    def register(selected):
        attempts.append(1)
        _write_json(selected.registration_report_path, {
            "report_version": "label-match-worker-pc-registration-v1",
            "status": registration_status, "recovery_action": registration_status,
            "secret_material_persisted": False, "server_http_status": 409,
        })
        return 2

    def first_setup(root, **kwargs):
        assert kwargs["_startup"] is True
        return onboarding.onboard_current_user(
            root, require_bootstrap_integrity=False, registration_runner=register,
            legacy_task_quiescence_reader=_legacy_task_quiescent,
            autostart_installer=forbidden, relay_launcher=forbidden,
        )

    class Parent:
        def __init__(self):
            self.labels, self.buttons = [], []
            windows.append(self)

        def __getattr__(self, _name):
            return lambda *_args, **_kwargs: None

        def mainloop(self):
            assert [item.values["text"] for item in self.labels] == [
                "이 PC는 이미 등록돼 있습니다\n현장 관리자에게 알리세요",
                "코드: " + cause,
                "현장 관리자: 관리자 계정으로 이 PC 등록 회복을 실행하세요",
            ]
            assert self.labels[1].values["font"][1] < self.labels[0].values["font"][1]
            assert [item.values["text"] for item in self.buttons] == ["자세히", "닫기"]
            assert not self.labels[2].packed
            self.buttons[0].values["command"]()
            assert self.labels[2].packed
            self.buttons[1].values["command"]()

    class Widget:
        def __init__(self, parent, **kwargs):
            self.values, self.packed = kwargs, False
            (parent.buttons if "command" in kwargs else parent.labels).append(self)

        def pack(self, **_kwargs):
            self.packed = True

    monkeypatch.setattr(app.tk, "Tk", Parent)
    monkeypatch.setattr(app.tk, "Label", Widget)
    monkeypatch.setattr(app.tk, "Button", Widget)
    monkeypatch.setattr(app.messagebox, "showerror", forbidden)
    monkeypatch.setattr(observation, "show_startup_blocked_notice", forbidden)
    monkeypatch.setattr(observation, "write_startup_result", lambda result: records.append(result) or True)
    monkeypatch.setattr(app, "onboard_current_user", first_setup)
    monkeypatch.setattr(app, "run_guarded_entrypoint", forbidden)
    monkeypatch.setattr(app, "_run_label_match_application", forbidden)

    for _ in range(2):
        previous_report = paths.registration_report_path.read_bytes() if paths.registration_report_path.exists() else None
        assert app.main(arguments) == app.ONBOARDING_EXIT_CODE
        report = json.loads(paths.onboarding_report_path.read_text(encoding="utf-8"))
        assert report["status"] == "RECOVERY_REQUIRED" and report["cause_code"] == cause
        assert report["administrator_guidance"] == "현장 관리자: 관리자 계정으로 이 PC 등록 회복을 실행하세요"
        if previous_report is not None:
            assert paths.registration_report_path.read_bytes() == previous_report
        assert not paths.credential_path.exists() and not paths.identity_path.exists()
        assert not paths.ledger_path.exists()
    assert attempts == [1]  # No enrollment after the first 409.
    assert len(windows) == len(records) == 2
    assert all(cause in record["reasons"] and "FIRST_RUN_SETUP_REQUIRED" not in record["reasons"]
               and record["startable"] is False for record in records)


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

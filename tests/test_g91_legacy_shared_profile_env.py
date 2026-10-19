"""G91: an app-specific profile env naming the old shared profile is ignored."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

import logistics_runtime_profile as lrp
from current_user_onboarding import onboard_current_user, resolve_current_user_onboarding_paths
from tests import test_current_user_onboarding as base
from tests.test_current_user_onboarding import isolated_data_generation_release  # noqa: F401  (autouse)

ENV = "LABEL_MATCH_LOGISTICS_PROFILE_PATH"
FAKE_PROFILE = b"legacy-shared-profile-fixture"
FAKE_SECRET = b"legacy-shared-secret-fixture"
SHELLS = [name for name in ("powershell.exe", "pwsh.exe") if shutil.which(name)]


def _fixture(tmp_path):
    program_data = tmp_path / "ProgramData"
    shared = program_data / "KMTech" / "Logistics"
    (shared / "secrets").mkdir(parents=True)
    profile = shared / "runtime-profile.json"
    secret = shared / "secrets" / "bearer-token.dpapi"
    profile.write_bytes(FAKE_PROFILE)
    secret.write_bytes(FAKE_SECRET)
    os.utime(profile, (1_600_000_000, 1_600_000_000))
    os.utime(secret, (1_600_000_000, 1_600_000_000))
    local = tmp_path / "Local"
    local.mkdir()
    app_root = tmp_path / "hardened-app"
    app_root.mkdir()
    return program_data, shared, profile, secret, local, app_root


def _forms(tmp_path, shared):
    profile = str(shared / "runtime-profile.json")
    link = tmp_path / "junction-to-shared"
    import _winapi

    _winapi.CreateJunction(str(shared), str(link))
    return {
        "case": profile.swapcase(),
        "slash": profile.replace("\\", "/"),
        "trailing": profile + "\\",
        "verbatim": chr(92) * 2 + "?" + chr(92) + profile,
        "secret_dir_file": str(shared / "secrets" / "bearer-token.dpapi"),
        "junction": str(link / "runtime-profile.json"),
        "none": "",
    }


def _snap(*paths):
    return [(p.read_bytes(), p.stat().st_mtime_ns) for p in paths]


@pytest.mark.skipif(os.name != "nt", reason="Windows path rules")
@pytest.mark.parametrize(
    "form", ["case", "slash", "trailing", "verbatim", "secret_dir_file", "junction", "none"]
)
def test_first_setup_ignores_legacy_shared_profile_env(tmp_path, form, monkeypatch):
    program_data, shared, profile, secret, local, app_root = _fixture(tmp_path)
    forms = _forms(tmp_path, shared)
    import writer_session_fence

    monkeypatch.setattr(writer_session_fence, "_installer_program_data_root", lambda: program_data)
    before = _snap(profile, secret)
    environment = {
        "LOCALAPPDATA": str(local),
        "LABEL_MATCH_SAVE_DIR": str(tmp_path / "state" / "data"),
        ENV: forms[form],
    }
    paths = resolve_current_user_onboarding_paths(app_root, environ=environment)

    def register(selected):
        base._ready_state(selected)
        return 0

    result = onboard_current_user(
        app_root,
        environ=environment,
        require_bootstrap_integrity=False,
        registration_runner=register,
        profile_loader=base._profile_loader,
        credential_loader=base._credential_loader,
        ledger_factory=base._ledger_factory,
        autostart_installer=base._autostart,
        scheduled_task_remover=base._scheduled_task_absent,
        legacy_task_quiescence_reader=base._legacy_task_quiescent,
        relay_launcher=base._relay_start,
    )
    assert result["status"] == "READY"
    assert paths.logistics_profile_path.is_relative_to(local.resolve())
    assert paths.logistics_profile_path.is_file()
    assert _snap(profile, secret) == before


def test_sibling_and_app_specific_paths_are_respected(tmp_path, monkeypatch):
    program_data, shared, *_ = _fixture(tmp_path)
    import writer_session_fence

    monkeypatch.setattr(writer_session_fence, "_installer_program_data_root", lambda: program_data)
    env = {"ProgramData": str(program_data), "LOCALAPPDATA": str(tmp_path / "Local")}
    for sibling in (
        program_data / "KMTech" / "Logistics2" / "runtime-profile.json",
        shared / "profiles" / "Label_Match" / "runtime-profile.json",
        shared / "secretsX" / "a.dpapi",
    ):
        env[ENV] = str(sibling)
        assert not lrp.is_legacy_shared_profile_path(str(sibling), env)
        assert lrp.app_profile_env_value(env) == str(sibling)
        assert lrp.default_logistics_profile_path(env) == sibling.resolve()


@pytest.mark.skipif(os.name != "nt" or not SHELLS, reason="Windows PowerShell")
@pytest.mark.parametrize("shell", SHELLS)
def test_installer_profile_env_judgement_matches_python(tmp_path, shell):
    repo = Path(__file__).resolve().parents[1]
    output = tmp_path / "result.json"
    root = tmp_path / "ps-root"
    root.mkdir()
    completed = subprocess.run(
        [shell, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
         "-File", str(repo / "tests/_g91_profile_env_child.ps1"), "-Repo", str(repo),
         "-Root", str(root), "-Output", str(output)],
        capture_output=True, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    if completed.returncode:
        (tmp_path / "process-stderr.log").write_bytes(completed.stderr)
    assert completed.returncode == 0, "definition-only installer child failed"
    data = json.loads(output.read_text(encoding="utf-8-sig"))
    for name in ("exact", "case", "slash", "trailing", "verbatim", "devicepath", "secret", "junction"):
        assert data[name]["legacy"] is True, name
        assert data[name]["value"] == "", name
    for name in ("sibling", "appspecific", "secretsx", "verbatimsibling", "verbatimapp", "uncplain", "uncverbatim"):
        assert data[name]["legacy"] is False, name
        assert data[name]["value"] == data["cases"][name], name
    assert data["unset"]["value"] == ""

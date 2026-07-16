from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys

import pytest


MODULE_PATH = Path(__file__).resolve().parents[1] / "tools" / "verify_staged_release_installer.py"
SPEC = importlib.util.spec_from_file_location("verify_staged_release_installer_for_tests", MODULE_PATH)
assert SPEC and SPEC.loader
verifier = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = verifier
SPEC.loader.exec_module(verifier)


def _package(tmp_path: Path) -> Path:
    root = tmp_path / "Label_Match"
    (root / "tools/direct_sync_relay_install_pack/_internal").mkdir(parents=True)
    (root / "install_label_match_direct_sync.ps1").write_text("# fixture\n", encoding="utf-8")
    (root / "tools/direct_sync_relay_install_pack/direct_sync_relay_install_pack.exe").write_bytes(b"install")
    (root / "tools/direct_sync_relay_install_pack/_internal/python312.dll").write_bytes(b"runtime")
    (root / "tools/direct_sync_relay_runner.exe").write_bytes(b"runner")
    (root / "tools/register_label_match_worker_pc.exe").write_bytes(b"register")
    return root


class _Completed:
    returncode = 0
    stdout = "dry run"
    stderr = ""


def _fake_run_with_report(command, **_kwargs):
    command = [str(part) for part in command]
    program_data = Path(command[command.index("-ProgramDataRoot") + 1])
    staged_root = Path(command[command.index("-File") + 1]).parent
    runner = staged_root / "tools/direct_sync_relay_runner.exe"
    registration = staged_root / "tools/register_label_match_worker_pc.exe"
    settings = staged_root / "config/app_settings.json"
    settings.parent.mkdir(parents=True, exist_ok=True)
    settings.write_text(json.dumps({"custom_save_path": str(Path(command[command.index("-ScanSourceDir") + 1]))}), encoding="utf-8")
    report = {
        "status": "DRY_RUN",
        "runner_exe": str(runner),
        "runner_command": [str(runner), "--help"],
        "app_settings_path": str(settings),
        "self_enrollment": {
            "registration_command_mode": "bundled_executable",
            "registration_executable": str(registration),
        },
    }
    target = program_data / "status/label_match_direct_sync_install.json"
    target.parent.mkdir(parents=True)
    target.write_text(json.dumps(report), encoding="utf-8")
    return _Completed()


@pytest.mark.skipif(verifier.os.name != "nt", reason="Windows-only staged installer verifier")
def test_verify_staged_installer_proves_exe_only_paths_without_mutating_package(tmp_path, monkeypatch):
    package = _package(tmp_path)
    before = verifier._inventory(package)
    monkeypatch.setattr(verifier.shutil, "which", lambda _name: "C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe")
    monkeypatch.setattr(verifier.subprocess, "run", _fake_run_with_report)

    report = verifier.verify_staged_installer(package)

    assert report["status"] == "PASS"
    assert report["system_python_required"] is False
    assert report["runner"]["selected"] is True
    assert report["registration"]["selected"] is True
    assert report["app_save_path_matches_relay_scan_source"] is True
    assert verifier._inventory(package) == before


@pytest.mark.skipif(verifier.os.name != "nt", reason="Windows-only staged installer verifier")
def test_verify_staged_installer_rejects_python_runner_fallback(tmp_path, monkeypatch):
    package = _package(tmp_path)
    monkeypatch.setattr(verifier.shutil, "which", lambda _name: "C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe")

    def fake_python_report(command, **kwargs):
        completed = _fake_run_with_report(command, **kwargs)
        program_data = Path(command[command.index("-ProgramDataRoot") + 1])
        report_path = program_data / "status/label_match_direct_sync_install.json"
        payload = json.loads(report_path.read_text(encoding="utf-8"))
        payload["runner_command"][0] = "C:/Python312/python.exe"
        report_path.write_text(json.dumps(payload), encoding="utf-8")
        return completed

    monkeypatch.setattr(verifier.subprocess, "run", fake_python_report)
    with pytest.raises(verifier.StagedInstallerVerificationError, match="bundled runner"):
        verifier.verify_staged_installer(package)

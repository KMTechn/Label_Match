import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest


STAGED_ROOT_ENV = "LABEL_MATCH_STAGED_PACKAGE_ROOT"
REQUIRE_STAGED_TEST_ENV = "LABEL_MATCH_REQUIRE_STAGED_INSTALLER_TEST"


def _staged_package_root() -> Path:
    configured = os.environ.get(STAGED_ROOT_ENV, "").strip()
    required = os.environ.get(REQUIRE_STAGED_TEST_ENV, "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if not configured:
        if required:
            pytest.fail(f"{STAGED_ROOT_ENV} is required for the staged installer gate")
        pytest.skip("staged Label_Match package is not available before the release build")
    root = Path(configured).resolve()
    if not root.is_dir():
        if required:
            pytest.fail(f"staged Label_Match package is missing: {root}")
        pytest.skip(f"staged Label_Match package is missing: {root}")
    return root


def _normalized(path: str | os.PathLike[str]) -> str:
    return os.path.normcase(os.path.abspath(os.fspath(path)))


@pytest.mark.skipif(os.name != "nt", reason="Label_Match release installers are Windows-only")
def test_staged_release_installer_dry_run_uses_nested_helper_and_bundled_runner(tmp_path):
    staged_root = _staged_package_root()
    powershell = shutil.which("powershell.exe") or shutil.which("pwsh.exe") or shutil.which("pwsh")
    if not powershell:
        pytest.fail("PowerShell is required for the staged installer gate")

    source_tools = staged_root / "tools"
    source_installer = staged_root / "install_label_match_direct_sync.ps1"
    source_install_helper = source_tools / "direct_sync_relay_install_pack"
    source_runner_exe = source_tools / "direct_sync_relay_runner.exe"
    source_registration_exe = source_tools / "register_label_match_worker_pc.exe"
    source_runner_script = source_tools / "direct_sync_relay_runner.py"
    required_paths = (
        source_installer,
        source_install_helper / "direct_sync_relay_install_pack.exe",
        source_runner_exe,
        source_registration_exe,
        source_runner_script,
    )
    missing = [str(path) for path in required_paths if not path.exists()]
    assert not missing, f"staged installer inputs are missing: {missing}"

    # Run against a disposable copy: the installer writes app settings even in
    # DryRun, and the signed/staged package must remain byte-for-byte untouched.
    package_root = tmp_path / "package" / "Label_Match"
    tools_dir = package_root / "tools"
    tools_dir.mkdir(parents=True)
    shutil.copy2(source_installer, package_root / source_installer.name)
    shutil.copytree(source_install_helper, tools_dir / source_install_helper.name)
    shutil.copy2(source_runner_exe, tools_dir / source_runner_exe.name)
    shutil.copy2(source_registration_exe, tools_dir / source_registration_exe.name)
    shutil.copy2(source_runner_script, tools_dir / source_runner_script.name)

    nested_helper = tools_dir / "direct_sync_relay_install_pack" / "direct_sync_relay_install_pack.exe"
    flat_helper = tools_dir / "direct_sync_relay_install_pack.exe"
    runner_exe = tools_dir / "direct_sync_relay_runner.exe"
    assert nested_helper.is_file()
    assert not flat_helper.exists(), "the gate must prove the nested onedir helper path, not a flat compatibility copy"

    runner_probe = subprocess.run(
        [str(runner_exe), "--help"],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert runner_probe.returncode == 0, runner_probe.stderr or runner_probe.stdout
    assert "Label_Match direct-sync relay runner" in runner_probe.stdout

    program_data_root = tmp_path / "runtime" / "direct-sync"
    scan_source_dir = tmp_path / "runtime" / "scan-data"
    environment = os.environ.copy()
    environment.pop("KMTECH_PYTHON_EXE", None)
    system_root = Path(environment.get("SystemRoot", r"C:\Windows"))
    environment["PATH"] = os.pathsep.join((str(system_root / "System32"), str(system_root)))
    completed = subprocess.run(
        [
            powershell,
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(package_root / source_installer.name),
            "-DryRun",
            "-ProgramDataRoot",
            str(program_data_root),
            "-ScanSourceDir",
            str(scan_source_dir),
            "-TaskName",
            "codex-label-match-staged-dry-run",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
        env=environment,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert "The term 'C' is not recognized" not in completed.stderr

    status_dir = program_data_root / "status"
    install_report = json.loads(
        (status_dir / "label_match_direct_sync_install.json").read_text(encoding="utf-8-sig")
    )
    installer_summary = json.loads(
        (status_dir / "label_match_one_step_install_summary.json").read_text(encoding="utf-8-sig")
    )

    assert install_report["status"] == "DRY_RUN"
    assert install_report["apply"] is False
    assert install_report["self_enrollment"]["enabled"] is True
    assert _normalized(install_report["runner_exe"]) == _normalized(runner_exe)
    assert _normalized(install_report["runner_command"][0]) == _normalized(runner_exe)
    assert installer_summary["status"] == "DRY_RUN"
    assert installer_summary["exit_code"] == 0
    assert installer_summary["bundled_runner_exe_present"] is True
    assert installer_summary["python_runner_script_present"] is True
    assert installer_summary["bundled_registration_exe_present"] is True
    assert installer_summary["python_exe"] == ""
    assert not (program_data_root / "bin").exists(), "DryRun must not install scheduled-task launchers"

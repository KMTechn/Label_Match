import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest


STAGED_ROOT_ENV = "LABEL_MATCH_STAGED_PACKAGE_ROOT"
REQUIRE_STAGED_TEST_ENV = "LABEL_MATCH_REQUIRE_STAGED_INSTALLER_TEST"
ROOT = Path(__file__).resolve().parents[1]


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


def _assert_isolated_file_hash_authority(
    powershell: Path, script_path: Path, probe_root: Path
) -> None:
    source = script_path.read_text(encoding="utf-8")
    assert "get-filehash" not in source.casefold()
    probe_root.mkdir(parents=True)
    payload_path = probe_root / "hash-fixture.bin"
    payload_path.write_bytes(b"abc")
    missing_path = probe_root / "missing.bin"
    isolated_modules = probe_root / "isolated-modules"
    isolated_modules.mkdir()
    command = r'''
$ErrorActionPreference = "Stop"
$PSModuleAutoLoadingPreference = "None"
if (
    $PSVersionTable.PSEdition -cne "Desktop" -or
    $PSVersionTable.PSVersion.Major -ne 5 -or
    $PSVersionTable.PSVersion.Minor -ne 1
) { throw "The isolated hash regression requires Windows PowerShell Desktop 5.1." }
$commandWasUnavailable = $false
try { $unexpected = Get-FileHash -LiteralPath $env:LABEL_MATCH_HASH_FIXTURE -Algorithm SHA256 }
catch [System.Management.Automation.CommandNotFoundException] { $commandWasUnavailable = $true }
if (-not $commandWasUnavailable) { throw "Get-FileHash unexpectedly resolved in the isolated regression." }

$tokens = $null
$errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    $env:LABEL_MATCH_INSTALLER_SCRIPT,
    [ref]$tokens,
    [ref]$errors
)
if ($errors.Count) { throw "Installer PowerShell AST is invalid." }
$functionAst = $null
foreach ($candidate in $ast.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -ceq "Get-FileSha256"
}, $true)) {
    $functionAst = $candidate
    break
}
if ($null -eq $functionAst) { throw "Get-FileSha256 is missing." }
. ([scriptblock]::Create($functionAst.Extent.Text))

$actual = Get-FileSha256 $env:LABEL_MATCH_HASH_FIXTURE
if ($actual -cne $env:LABEL_MATCH_EXPECTED_SHA256) { throw "Module-independent SHA-256 mismatch." }
$missingRejected = $false
try { $unexpected = Get-FileSha256 $env:LABEL_MATCH_MISSING_HASH_FIXTURE }
catch { $missingRejected = $true }
if (-not $missingRejected) { throw "Missing file did not fail closed." }
[Console]::Out.Write("PASS")
'''
    environment = {
        **os.environ,
        "PATH": "",
        "PSModulePath": str(isolated_modules),
        "LABEL_MATCH_INSTALLER_SCRIPT": str(script_path),
        "LABEL_MATCH_HASH_FIXTURE": str(payload_path),
        "LABEL_MATCH_MISSING_HASH_FIXTURE": str(missing_path),
        "LABEL_MATCH_EXPECTED_SHA256": (
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        ),
    }
    completed = subprocess.run(
        [
            str(powershell),
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            command,
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
        env=environment,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == "PASS"


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell hashing is Windows-only")
@pytest.mark.parametrize(
    "script_name", ("INSTALL_THIS_PC.ps1", "install_label_match_direct_sync.ps1")
)
def test_installer_file_hash_authority_does_not_require_module_autoload(tmp_path, script_name):
    powershell = (
        Path(os.environ.get("SystemRoot", r"C:\Windows"))
        / "System32/WindowsPowerShell/v1.0/powershell.exe"
    )
    if not powershell.is_file():
        pytest.skip("Windows PowerShell 5.1 is unavailable")
    _assert_isolated_file_hash_authority(
        powershell, ROOT / script_name, tmp_path / Path(script_name).stem
    )


@pytest.mark.skipif(os.name != "nt", reason="Label_Match release installers are Windows-only")
def test_staged_release_public_entrypoint_self_stages_manifest_bound_payload(tmp_path):
    staged_root = _staged_package_root()
    powershell = shutil.which("powershell.exe") or shutil.which("pwsh.exe") or shutil.which("pwsh")
    if not powershell:
        pytest.fail("PowerShell is required for the staged installer gate")

    required_paths = (
        staged_root / "INSTALL_THIS_PC.ps1",
        staged_root / "install_label_match_direct_sync.ps1",
        staged_root / "build-manifest.json",
        staged_root / "_internal/python312.dll",
        staged_root / "_internal/base_library.zip",
        staged_root / "tools/invoke_embedded_python.ps1",
        staged_root / "tools/direct_sync_relay_install_pack.py",
        staged_root / "tools/direct_sync_relay_runner.py",
        staged_root / "tools/register_label_match_worker_pc.py",
    )
    missing = [str(path) for path in required_paths if not path.is_file()]
    assert not missing, f"staged installer inputs are missing: {missing}"
    windows_powershell = (
        Path(os.environ.get("SystemRoot", r"C:\Windows"))
        / "System32/WindowsPowerShell/v1.0/powershell.exe"
    )
    assert windows_powershell.is_file(), "Windows PowerShell 5.1 is required for the staged gate"
    for staged_script in required_paths[:2]:
        _assert_isolated_file_hash_authority(
            windows_powershell,
            staged_script,
            tmp_path / "staged-hash-authority" / staged_script.stem,
        )

    extracted_root = tmp_path / "ordinary-extraction" / "Label_Match"
    shutil.copytree(staged_root, extracted_root)
    install_root = tmp_path / "installed" / "current"
    program_data_root = tmp_path / "runtime" / "direct-sync"
    scan_source_dir = tmp_path / "runtime" / "scan-data"
    common_programs = tmp_path / "common-programs"
    receipt_root = tmp_path / "installer-receipts"
    environment = os.environ.copy()
    environment["KMTECH_FACTORY_INSTALL_TEST_MODE"] = "1"
    environment.pop("KMTECH_PYTHON_EXE", None)
    system_root = Path(environment.get("SystemRoot", r"C:\Windows"))
    environment["PATH"] = os.pathsep.join((str(system_root / "System32"), str(system_root)))
    completed = subprocess.run(
        [
            powershell,
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(extracted_root / "INSTALL_THIS_PC.ps1"),
            "-DryRun",
            "-AllowNoncanonicalLayoutForTest",
            "-InstallRootForTest",
            str(install_root),
            "-ProgramDataRoot",
            str(program_data_root),
            "-ScanSourceDir",
            str(scan_source_dir),
            "-CommonProgramsRootForTest",
            str(common_programs),
            "-RollbackReceiptRootForTest",
            str(receipt_root),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
        env=environment,
    )
    assert completed.returncode == 0, (completed.stderr or completed.stdout)[:2048]
    assert len(completed.stdout.encode("utf-8", errors="replace")) <= 64 * 1024
    assert completed.stderr == ""
    assert install_root.is_dir()
    assert not (install_root / "Label_Match").exists()
    assert (install_root / "Label_Match.exe").is_file()

    public_report = json.loads(
        (receipt_root / "label_match_public_install_report.json").read_text(encoding="utf-8-sig")
    )
    install_report = json.loads(
        (program_data_root / "status/label_match_direct_sync_install.json").read_text(
            encoding="utf-8-sig"
        )
    )
    summary = json.loads(
        (program_data_root / "status/label_match_one_step_install_summary.json").read_text(
            encoding="utf-8-sig"
        )
    )

    assert public_report["status"] == "DRY_RUN_STAGED"
    assert public_report["staging"]["ordinary_extracted_root_supported"] is True
    assert public_report["staging"]["candidate_byte_parity_verified"] is True
    assert public_report["staging"]["safe_relative_paths_verified"] is True
    assert public_report["launcher_contract"]["scope"] == "all_users"
    assert public_report["launcher_contract"]["count"] == 1
    assert public_report["removal_contract"]["uninstall"] == "DATA_PRESERVING_UNINSTALL"
    assert public_report["removal_contract"]["rollback"] == "EXACT_FRESH_TARGET_ROLLBACK"
    assert public_report["removal_contract"]["task_operations"] == ["stop", "delete", "absence"]
    assert public_report["removal_contract"]["app_inventory_contract"] == (
        "label-match-app-immutable-inventory-v1"
    )
    assert public_report["removal_contract"]["mutable_app_relative_paths"] == [
        "_internal/config/app_settings.json"
    ]
    assert public_report["removal_contract"]["immutable_app_drift_rejected"] is True
    assert install_report["status"] == "DRY_RUN"
    assert install_report["field_layout_contract"]["local_test_override_enabled"] is True
    assert install_report["runner_exe"] == ""
    assert install_report["runner_command_mode"] == "in_process_source"
    assert _normalized(install_report["runner_command"][0]) == _normalized(
        install_root / "tools/direct_sync_relay_runner.py"
    )
    assert summary["installer_execution_mode"] == "in_process_embedded_python"
    assert not any(
        (install_root / relative).exists()
        for relative in (
            "tools/direct_sync_relay_install_pack/direct_sync_relay_install_pack.exe",
            "tools/direct_sync_relay_runner.exe",
            "tools/register_label_match_worker_pc.exe",
        )
    )
    assert summary["installer_report_version"] == "label-match-direct-sync-one-step-install-v2"
    assert summary["status"] == "DRY_RUN"
    assert summary["lifecycle_contract"]["task_removal_order"] == ["stop", "delete", "absence"]
    assert summary["resources"]["app_root"]["inventory_contract"] == (
        "label-match-app-immutable-inventory-v1"
    )
    assert summary["resources"]["app_root"]["mutable_relative_paths"] == [
        "_internal/config/app_settings.json"
    ]
    assert not (program_data_root / "bin").exists(), "DryRun must not install task launchers"
    assert not (common_programs / "KMTech/Label Match.lnk").exists(), "DryRun must not create shell links"

import json
import os
from pathlib import Path
import subprocess

import pytest

from tools import verify_staged_release_installer as verifier


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
        pytest.skip("staged Label_Match package is unavailable before release build")
    root = Path(configured).resolve()
    if not root.is_dir():
        if required:
            pytest.fail(f"staged Label_Match package is missing: {root}")
        pytest.skip(f"staged Label_Match package is missing: {root}")
    return root


def _powershell() -> Path:
    path = (
        Path(os.environ.get("SystemRoot", r"C:\Windows"))
        / "System32"
        / "WindowsPowerShell"
        / "v1.0"
        / "powershell.exe"
    )
    if not path.is_file():
        pytest.skip("Windows PowerShell 5.1 is unavailable")
    return path


@pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell hashing is Windows-only")
def test_public_bootstrap_hash_authority_does_not_require_get_file_hash(tmp_path):
    script = ROOT / "INSTALL_THIS_PC.ps1"
    source = script.read_text(encoding="utf-8-sig")
    assert "get-filehash" not in source.casefold()
    fixture = tmp_path / "hash.bin"
    fixture.write_bytes(b"abc")
    isolated_modules = tmp_path / "modules"
    isolated_modules.mkdir()
    command = r'''
$ErrorActionPreference = "Stop"
$PSModuleAutoLoadingPreference = "None"
$tokens = $null
$errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    $env:LABEL_MATCH_INSTALLER_SCRIPT, [ref]$tokens, [ref]$errors
)
if ($errors.Count) { throw "Installer AST is invalid." }
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
if ((Get-FileSha256 $env:LABEL_MATCH_HASH_FIXTURE) -cne $env:LABEL_MATCH_EXPECTED_SHA256) {
    throw "SHA-256 mismatch."
}
[Console]::Out.Write("PASS")
'''
    completed = subprocess.run(
        [
            str(_powershell()),
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
        env={
            **os.environ,
            "PATH": "",
            "PSModulePath": str(isolated_modules),
            "LABEL_MATCH_INSTALLER_SCRIPT": str(script),
            "LABEL_MATCH_HASH_FIXTURE": str(fixture),
            "LABEL_MATCH_EXPECTED_SHA256": (
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
            ),
        },
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == "PASS"


@pytest.mark.skipif(os.name != "nt", reason="staged package gate is Windows-only")
def test_sealed_staged_package_preserves_onedir_product_host_topology(tmp_path):
    staged_root = _staged_package_root()
    report_path = staged_root / "staged-installer-verification.json"
    manifest_path = staged_root / "build-manifest.json"
    assert report_path.is_file()
    assert manifest_path.is_file()
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))

    assert report["schema_version"] == verifier.REPORT_SCHEMA
    assert report["status"] == "PASS"
    assert report["runtime_host"]["path"] == "Label_Match.exe"
    assert report["runtime_host"]["package_layout"] == "onedir"
    assert report["runtime_host"]["relay_execution_boundary"] == "product_host"
    assert report["state_contract"]["relay_persistence"] == "HKCU_RUN"
    assert report["state_contract"]["relay_port_contract"] == 18456
    assert report["state_contract"]["source_host_override_required"] is False
    assert report["legacy_authority_contract"]["system_scheduled_task_supported"] is False
    assert (staged_root / "_internal" / "python312.dll").is_file()
    paths = {
        path.relative_to(staged_root).as_posix()
        for path in staged_root.rglob("*")
        if path.is_file()
    }
    assert not (verifier.FORBIDDEN_ACTIVE_AUTHORITY_MEMBERS & paths)

    install_root = tmp_path / "installed" / "current"
    completed = subprocess.run(
        [
            str(_powershell()),
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(staged_root / "INSTALL_THIS_PC.ps1"),
            "-DryRun",
            "-SourceRoot",
            str(staged_root),
            "-InstallRoot",
            str(install_root),
            "-AllowNoncanonicalLayoutForTest",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
        env={**os.environ, "KMTECH_FACTORY_INSTALL_TEST_MODE": "1"},
    )
    assert completed.returncode == 0, completed.stderr
    assert "bootstrap_status=DRY_RUN" in completed.stdout
    assert "identity_profile_created=false" in completed.stdout
    assert completed.stderr == ""
    assert not install_root.exists()

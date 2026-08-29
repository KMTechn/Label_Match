import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

from tools import build_portable_release_candidate as portable_builder


ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / "INSTALL_CANONICAL_PORTABLE.ps1"
HELPER = ROOT / "INSTALL_THIS_PC.ps1"
INTEGRITY_HELPER = ROOT / "tools" / "bootstrap_integrity.ps1"


def _source(path: Path = INSTALLER) -> str:
    return path.read_text(encoding="utf-8")


def test_installer_exposes_inspection_equivalent_v2_interface() -> None:
    source = _source()
    parameter_block = source[
        source.index("param(") : source.index(")", source.index("param("))
    ]

    for name in (
        "SourceRoot",
        "InstallRoot",
        "EvidencePath",
        "PlanOnly",
        "AllowNoncanonicalLayoutForTest",
        "SkipSignatureValidationForTest",
    ):
        assert re.search(rf"\${name}\b", parameter_block)
    assert not re.search(r"\$CodePlacementOnly\b", parameter_block)
    assert not re.search(r"\$Rollback\b", parameter_block)
    assert not re.search(r"__[A-Z0-9_]+__", source)


def test_top_level_installer_owns_current_user_lifecycle_and_preimage() -> None:
    source = _source()

    for token in (
        "C:\\KMTech\\Apps\\Label_Match\\current",
        "KMTech.LabelMatch.Relay",
        "--label-match-user-relay",
        "label-match-portable-tree-v1",
        "label-match-canonical-portable-install-v1",
        "INSTALL_THIS_PC.ps1",
        "Product $install '--remove-current-user-setup'",
        "Product $install '--onboard-current-user'",
        "PREIMAGE_SAVED",
        "FAILED_ROLLED_BACK",
        "stop_marker_preimage",
        "REUSED_VERIFIED",
        "label-match-exact-clone-resolution-v2",
        "label-match-portable-full-inventory-v1",
        "PortableInventory $Root",
        "ReceiptSource $source $sourceManifest",
    ):
        assert token in source
    assert "(Arg $Root)" in source
    assert "Register-ScheduledTask" not in source
    assert "Start-ScheduledTask" not in source
    assert "Stop-ScheduledTask" not in source
    assert "schtasks /run" not in source.lower()
    assert source.index("ReceiptSource $source $sourceManifest") < source.index(
        "InvokeFrozenPlacementHelper $frozenPlacement $helperParameters"
    )


def test_code_helper_owns_privileged_placement_and_exact_rollback() -> None:
    source = _source(HELPER)

    for name in (
        "DryRun",
        "Uninstall",
        "SourceRoot",
        "InstallRoot",
        "ElevationLogPath",
        "ExpectedBootstrapScriptSha256",
        "ExpectedSourceAggregateSha256",
        "ExpectedSourceFileCount",
        "ExpectedSourceByteCount",
        "ReplaceExistingVerifiedPortable",
    ):
        parameter_block = source[
            source.index("param(") : source.index(")", source.index("param("))
        ]
        assert re.search(rf"\${name}\b", parameter_block)
    for token in (
        "tools\\bootstrap_integrity.ps1",
        ".current.rollback.",
        "REPLACED_VERIFIED",
        "replacement_rollback_status=PRESERVED",
        "Write-ElevationLog",
        "Portable source inventory differs from its trusted caller pins.",
        "Bootstrap script SHA-256 differs from its trusted caller pin.",
    ):
        assert token in source
    assert "Register-ScheduledTask" not in source
    assert INTEGRITY_HELPER.is_file()


def test_top_level_freezes_and_pins_the_uac_helper_before_copy() -> None:
    source = _source()

    for token in (
        "$frozenPlacement = FreezePlacementHelper",
        "$receiptSource.critical_file_sha256",
        "InvokeFrozenIntegrityProbe $frozenPlacement $install",
        "current_user_writable = $false",
        "ExpectedBootstrapScriptSha256 =",
        "ExpectedSourceAggregateSha256 =",
        "ExpectedSourceFileCount =",
        "ExpectedSourceByteCount =",
        "[string]$frozenPlacement.helper_path",
    ):
        assert token in source
    final_receipt_read = source.rindex("ReceiptSource $source $sourceManifest")
    helper_invoke = source.index(
        "InvokeFrozenPlacementHelper $frozenPlacement $helperParameters"
    )
    assert final_receipt_read < helper_invoke
    assert "(Join-Path $PSScriptRoot 'INSTALL_THIS_PC.ps1')" not in source


def test_encoded_elevated_launcher_binds_named_helper_parameters(tmp_path: Path) -> None:
    source = _source()
    match = re.search(
        r"\$launcher = @'\r?\n(?P<body>.*?)\r?\n'@\.Replace"
        r"\('@@payload-base64@@', \$payloadBase64\)",
        source,
        re.DOTALL,
    )
    assert match is not None

    helper = tmp_path / "helper.ps1"
    integrity = tmp_path / "integrity.ps1"
    helper.write_text(
        """param(
    [string]$SourceRoot,
    [string]$InstallRoot,
    [string]$ElevationLogPath,
    [string]$ExpectedBootstrapScriptSha256,
    [string]$VerifiedBootstrapScriptPath,
    [switch]$BootstrapIntegrityPreloaded,
    [string]$ExpectedSourceAggregateSha256,
    [int]$ExpectedSourceFileCount,
    [uint64]$ExpectedSourceByteCount,
    [switch]$AllowNoncanonicalLayoutForTest,
    [switch]$ReplaceExistingVerifiedPortable
)
[ordered]@{
    source_root = $SourceRoot
    install_root = $InstallRoot
    elevation_log_path = $ElevationLogPath
    expected_bootstrap_script_sha256 = $ExpectedBootstrapScriptSha256
    verified_bootstrap_script_path = $VerifiedBootstrapScriptPath
    bootstrap_integrity_preloaded = $BootstrapIntegrityPreloaded.IsPresent
    expected_source_aggregate_sha256 = $ExpectedSourceAggregateSha256
    expected_source_file_count = $ExpectedSourceFileCount
    expected_source_byte_count = $ExpectedSourceByteCount
    allow_noncanonical_layout_for_test = $AllowNoncanonicalLayoutForTest.IsPresent
    replace_existing_verified_portable = $ReplaceExistingVerifiedPortable.IsPresent
} | ConvertTo-Json -Compress
""",
        encoding="utf-8",
    )
    integrity.write_bytes(b"# verified integrity helper\n")
    helper_sha256 = hashlib.sha256(helper.read_bytes()).hexdigest()
    integrity_sha256 = hashlib.sha256(integrity.read_bytes()).hexdigest()
    parameters = {
        "SourceRoot": r"E:\source root",
        "InstallRoot": r"E:\install root",
        "ElevationLogPath": r"E:\audit\elevation.log",
        "ExpectedBootstrapScriptSha256": helper_sha256,
        "VerifiedBootstrapScriptPath": str(helper),
        "BootstrapIntegrityPreloaded": True,
        "ExpectedSourceAggregateSha256": "a" * 64,
        "ExpectedSourceFileCount": 3380,
        "ExpectedSourceByteCount": 77580497,
        "AllowNoncanonicalLayoutForTest": True,
        "ReplaceExistingVerifiedPortable": False,
    }
    payload = {
        "helper_path": str(helper),
        "helper_sha256": helper_sha256,
        "integrity_path": str(integrity),
        "integrity_sha256": integrity_sha256,
        "parameters": parameters,
    }
    payload_base64 = base64.b64encode(
        json.dumps(payload, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
    launcher = match.group("body").replace("@@payload-base64@@", payload_base64)
    encoded_launcher = base64.b64encode(launcher.encode("utf-16-le")).decode("ascii")
    powershell = (
        Path(os.environ["SystemRoot"])
        / "System32"
        / "WindowsPowerShell"
        / "v1.0"
        / "powershell.exe"
    )
    completed = subprocess.run(
        [
            str(powershell),
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-EncodedCommand",
            encoded_launcher,
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout.strip().splitlines()[-1])
    assert result == {
        "source_root": parameters["SourceRoot"],
        "install_root": parameters["InstallRoot"],
        "elevation_log_path": parameters["ElevationLogPath"],
        "expected_bootstrap_script_sha256": helper_sha256,
        "verified_bootstrap_script_path": str(helper),
        "bootstrap_integrity_preloaded": True,
        "expected_source_aggregate_sha256": "a" * 64,
        "expected_source_file_count": 3380,
        "expected_source_byte_count": 77580497,
        "allow_noncanonical_layout_for_test": True,
        "replace_existing_verified_portable": False,
    }


def test_portable_builder_packages_v2_installer_helper_and_integrity_tool() -> None:
    source = _source(ROOT / "tools" / "build_portable_release_candidate.py")

    assert portable_builder.CANONICAL_INSTALLER_FILENAME == INSTALLER.name
    assert portable_builder.LEGACY_INSTALLER_FILENAME == HELPER.name
    assert portable_builder.BOOTSTRAP_INTEGRITY_HELPER.as_posix() == (
        "tools/bootstrap_integrity.ps1"
    )
    assert "canonical_installer_sha256" in source
    assert "runtime_python_sha256" in source
    assert "shutil.copy2(installer_source" in source
    assert "shutil.copy2(legacy_installer_source" in source
    assert "shutil.copy2(bootstrap_helper_source" in source


def test_plan_only_contract_is_stdout_only_and_non_mutating() -> None:
    source = _source()
    plan_block = source[source.index("if ($PlanOnly)") : source.index("$runId =")]

    assert "install_status=PLAN_ONLY" in plan_block
    assert "registry_changed=false" in plan_block
    assert "Save " not in plan_block
    assert "Start-Process" not in plan_block
    assert "INSTALL_THIS_PC.ps1" not in plan_block

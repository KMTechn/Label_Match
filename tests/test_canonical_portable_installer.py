import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

import pytest

from tools import build_portable_release_candidate as portable_builder


ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / "INSTALL_CANONICAL_PORTABLE.ps1"
HELPER = ROOT / "INSTALL_THIS_PC.ps1"
INTEGRITY_HELPER = ROOT / "tools" / "bootstrap_integrity.ps1"


def _source(path: Path = INSTALLER) -> str:
    return path.read_text(encoding="utf-8")


def _run_rollback_relay_harness(tmp_path: Path, scenario: str) -> dict[str, object]:
    source = _source()
    start = source.index("function Relays")
    end = source.index("function Product", start)
    functions = source[start:end]
    harness = tmp_path / f"rollback-relays-{scenario}.ps1"
    harness.write_text(
        rf"""
function Same([string]$A, [string]$B) {{
    return [StringComparer]::OrdinalIgnoreCase.Equals($A, $B)
}}

{functions}
$script:scenario = '{scenario}'
$script:lastActualType = ''
function Get-CimInstance {{
    param([string]$ClassName, [object]$ErrorAction)
    if ($script:scenario -eq 'query_error') {{
        throw [InvalidOperationException]::new('synthetic CIM failure')
    }}
    $commandLine = if ($script:scenario -eq 'mismatch') {{
        'pythonw.exe --label-match-user-relay --different'
    }}
    else {{
        'pythonw.exe --label-match-user-relay --expected'
    }}
    $row = [ordered]@{{
        ExecutablePath = 'C:\runtime\pythonw.exe'
        CommandLine = $commandLine
    }}
    $script:lastActualType = $row.GetType().FullName
    return $row
}}

$expected = if ($script:scenario -eq 'extra') {{
    @()
}}
else {{
    @([ordered]@{{
        ExecutablePath = 'C:\runtime\pythonw.exe'
        CommandLine = 'pythonw.exe --label-match-user-relay --expected'
    }})
}}
$status = 'PASS'
$message = ''
try {{ [void](Assert-RollbackRelayPreimage -ExpectedRelays $expected) }}
catch {{
    $status = 'FAIL'
    $message = [string]$_.Exception.Message
}}
[pscustomobject]@{{
    status = $status
    message = $message
    actual_row_type = $script:lastActualType
}} |
    ConvertTo-Json -Compress
""",
        encoding="utf-8-sig",
    )
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
            "-File",
            str(harness),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return json.loads(completed.stdout)


def _run_relay_persistent_retry_guard(
    tmp_path: Path, persistent_retry: object
) -> subprocess.CompletedProcess[str]:
    source = _source()
    functions = source[
        source.index("function Get-RequiredExternalBoolean") : source.index(
            "function Full"
        )
    ]
    payload = base64.b64encode(
        json.dumps({"persistent_retry": persistent_retry}).encode("utf-8")
    ).decode("ascii")
    harness = tmp_path / "relay-persistent-retry-guard.ps1"
    harness.write_text(
        f"""
{functions}
$json = (New-Object Text.UTF8Encoding($false, $true)).GetString(
    [Convert]::FromBase64String('{payload}')
)
$relay = $json | ConvertFrom-Json
try {{
    $result = Test-RelayPersistentRetry $relay
    Write-Output ('guard_result=' + ([string]$result).ToLowerInvariant())
    exit 0
}}
catch {{
    Write-Output ('guard_error=' + [string]$_.Exception.Message)
    exit 7
}}
""",
        encoding="utf-8-sig",
    )
    powershell = (
        Path(os.environ["SystemRoot"])
        / "System32"
        / "WindowsPowerShell"
        / "v1.0"
        / "powershell.exe"
    )
    return subprocess.run(
        [
            str(powershell),
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(harness),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_relay_persistent_retry_guard_accepts_literal_booleans(tmp_path: Path) -> None:
    true_result = _run_relay_persistent_retry_guard(tmp_path, True)
    false_result = _run_relay_persistent_retry_guard(tmp_path, False)

    assert true_result.returncode == 0, true_result.stderr or true_result.stdout
    assert "guard_result=true" in true_result.stdout
    assert false_result.returncode == 0, false_result.stderr or false_result.stdout
    assert "guard_result=false" in false_result.stdout


@pytest.mark.parametrize(
    "invalid_value",
    ["false", "0", "", "null", None, 0, 1, [], {}],
    ids=[
        "string-false",
        "string-zero",
        "empty-string",
        "string-null",
        "json-null",
        "integer-zero",
        "integer-one",
        "array",
        "object",
    ],
)
def test_relay_persistent_retry_guard_rejects_actual_non_boolean_sentinels(
    tmp_path: Path, invalid_value: object
) -> None:
    completed = _run_relay_persistent_retry_guard(tmp_path, invalid_value)

    assert completed.returncode == 7
    assert "guard_error=External boolean has invalid type: persistent_retry" in (
        completed.stdout
    )


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


def test_rollback_is_fail_closed_and_persists_explicit_failure() -> None:
    source = _source()
    rollback = source[source.index("catch {\n    $original = $_") :]

    assert "try { Product $install '--remove-current-user-setup' } catch {}" not in rollback
    product = rollback.index("Product $install '--remove-current-user-setup'")
    zero_readback = rollback.index(
        "Assert-RollbackRelayPreimage -ExpectedRelays @()"
    )
    restore = rollback.index("Restore $before")
    exact_readback = rollback.index(
        "Assert-RollbackRelayPreimage -ExpectedRelays $old"
    )
    restored_true = rollback.index("$audit.rollback.runtime_restored = $true")
    assert product < zero_readback < restore < exact_readback < restored_true
    for token in (
        "$audit.status = 'ROLLBACK_FAILED'",
        "$audit.rollback.runtime_restored = $false",
        "$audit.rollback.failure_type = $rollbackFailure.Exception.GetType().Name",
        "ROLLBACK_AUDIT_PERSISTENCE_FAILED",
        "AUTOSTART_ROLLBACK_FAILED",
    ):
        assert token in rollback


def test_rollback_relay_readback_rejects_query_failure_extra_and_mismatch(
    tmp_path: Path,
) -> None:
    scenarios = {
        "query_error": "synthetic CIM failure",
        "extra": "process-count readback failed",
        "mismatch": "executable/command readback failed",
    }
    for scenario, message in scenarios.items():
        result = _run_rollback_relay_harness(tmp_path, scenario)
        assert result["status"] == "FAIL"
        assert message in result["message"]
        if scenario != "query_error":
            assert result["actual_row_type"] == (
                "System.Collections.Specialized.OrderedDictionary"
            )


def test_rollback_relay_readback_accepts_only_exact_preimage(tmp_path: Path) -> None:
    assert _run_rollback_relay_harness(tmp_path, "exact") == {
        "status": "PASS",
        "message": "",
        "actual_row_type": "System.Collections.Specialized.OrderedDictionary",
    }


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
    [switch]$ReplaceExistingVerifiedPortable,
    [switch]$DryRun
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
    dry_run = $DryRun.IsPresent
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
        "DryRun": True,
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
        "dry_run": True,
    }


@pytest.mark.parametrize(
    ("field", "invalid_value", "expected_message"),
    [
        (
            "BootstrapIntegrityPreloaded",
            value,
            "External boolean has invalid type: BootstrapIntegrityPreloaded",
        )
        for value in ("false", "0", "", "null", None, 0, 1, [], {})
    ]
    + [
        (
            "AllowNoncanonicalLayoutForTest",
            "false",
            "External boolean has invalid type: AllowNoncanonicalLayoutForTest",
        ),
        (
            "ReplaceExistingVerifiedPortable",
            "0",
            "External boolean has invalid type: ReplaceExistingVerifiedPortable",
        ),
        (
            "DryRun",
            "null",
            "External boolean has invalid type: DryRun",
        ),
        (
            "ExpectedSourceFileCount",
            "3380",
            "External integer has invalid type: ExpectedSourceFileCount",
        ),
        (
            "ExpectedSourceByteCount",
            "77580497",
            "External integer has invalid type: ExpectedSourceByteCount",
        ),
    ],
    ids=[
        "bootstrap-string-false",
        "bootstrap-string-zero",
        "bootstrap-empty-string",
        "bootstrap-string-null",
        "bootstrap-json-null",
        "bootstrap-integer-zero",
        "bootstrap-integer-one",
        "bootstrap-array",
        "bootstrap-object",
        "layout-string-false",
        "replace-string-zero",
        "dry-run-string-null",
        "file-count-numeric-string",
        "byte-count-numeric-string",
    ],
)
def test_encoded_elevated_launcher_rejects_actual_non_scalar_sentinels(
    tmp_path: Path,
    field: str,
    invalid_value: object,
    expected_message: str,
) -> None:
    source = _source()
    match = re.search(
        r"\$launcher = @'\r?\n(?P<body>.*?)\r?\n'@\.Replace"
        r"\('@@payload-base64@@', \$payloadBase64\)",
        source,
        re.DOTALL,
    )
    assert match is not None

    helper = tmp_path / "must-not-run.ps1"
    integrity = tmp_path / "integrity.ps1"
    helper.write_text("throw 'HELPER_MUST_NOT_RUN'\n", encoding="utf-8")
    integrity.write_bytes(b"# verified integrity helper\n")
    helper_sha256 = hashlib.sha256(helper.read_bytes()).hexdigest()
    integrity_sha256 = hashlib.sha256(integrity.read_bytes()).hexdigest()
    parameters: dict[str, object] = {
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
        "DryRun": True,
    }
    parameters[field] = invalid_value
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

    combined = completed.stdout + completed.stderr
    assert completed.returncode != 0
    assert expected_message in combined
    assert "HELPER_MUST_NOT_RUN" not in combined


def test_encoded_launcher_runs_actual_helper_with_preloaded_pinned_integrity(
    tmp_path: Path,
) -> None:
    source = _source()
    match = re.search(
        r"\$launcher = @'\r?\n(?P<body>.*?)\r?\n'@\.Replace"
        r"\('@@payload-base64@@', \$payloadBase64\)",
        source,
        re.DOTALL,
    )
    assert match is not None

    source_root = tmp_path / "portable"
    files = {
        "runtime/python.exe": b"signed-runtime-fixture",
        "runtime/pythonw.exe": b"signed-runtime-window-fixture",
        "app/main.py": b"print('fixture')\n",
        "launch-label-match.cmd": b"@echo off\r\n",
        "INSTALL_CANONICAL_PORTABLE.ps1": INSTALLER.read_bytes(),
        "INSTALL_THIS_PC.ps1": HELPER.read_bytes(),
        "tools/bootstrap_integrity.ps1": INTEGRITY_HELPER.read_bytes(),
    }
    for relative, content in files.items():
        target = source_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    manifest = {
        "schema": "label-match-portable-tree-v1",
        "entrypoint": "runtime/pythonw.exe app/main.py",
        "launcher": "launch-label-match.cmd",
        "allowed_unsigned_app_pe": [],
        "forbidden_package_roots": [],
        "runtime_pythonw_sha256": hashlib.sha256(
            files["runtime/pythonw.exe"]
        ).hexdigest(),
        "launcher_sha256": hashlib.sha256(
            files["launch-label-match.cmd"]
        ).hexdigest(),
        "file_count_before_manifest": len(files),
        "byte_count_before_manifest": sum(len(content) for content in files.values()),
    }
    (source_root / "portable-manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )

    helper_sha256 = hashlib.sha256(HELPER.read_bytes()).hexdigest()
    integrity_sha256 = hashlib.sha256(INTEGRITY_HELPER.read_bytes()).hexdigest()
    parameters = {
        "SourceRoot": str(source_root),
        "InstallRoot": str(tmp_path / "install"),
        "ElevationLogPath": str(tmp_path / "elevation.log"),
        "ExpectedBootstrapScriptSha256": helper_sha256,
        "VerifiedBootstrapScriptPath": str(HELPER),
        "BootstrapIntegrityPreloaded": True,
        "ExpectedSourceAggregateSha256": "",
        "ExpectedSourceFileCount": 0,
        "ExpectedSourceByteCount": 0,
        "AllowNoncanonicalLayoutForTest": True,
        "ReplaceExistingVerifiedPortable": False,
        "DryRun": True,
    }
    payload = {
        "helper_path": str(HELPER),
        "helper_sha256": helper_sha256,
        "integrity_path": str(INTEGRITY_HELPER),
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
    environment = os.environ.copy()
    environment["KMTECH_FACTORY_INSTALL_TEST_MODE"] = "1"
    environment["LOCALAPPDATA"] = str(tmp_path / "local-app-data")
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
        env=environment,
    )
    assert completed.returncode == 0, completed.stderr
    assert "bootstrap_status=DRY_RUN" in completed.stdout
    assert "release_layout=PORTABLE_CPYTHON" in completed.stdout
    assert not (tmp_path / "install").exists()


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

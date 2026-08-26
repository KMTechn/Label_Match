import json
import hashlib
import os
from pathlib import Path
import shutil
import subprocess

import pytest
from tools import install_logistics_runtime_profile as machine_profiles
from tools import verify_frozen_release_assets as frozen_verifier

ROOT = Path(__file__).resolve().parents[1]


def _assert_powershell_ast(path: Path) -> None:
    escaped = str(path).replace("'", "''")
    command = (
        "$tokens=$null;$errors=$null;"
        f"[void][System.Management.Automation.Language.Parser]::ParseFile('{escaped}',[ref]$tokens,[ref]$errors);"
        "if($errors.Count){$errors|ForEach-Object{$_.Message}|Write-Error;exit 1}"
    )
    subprocess.run(
        ["powershell", "-NoProfile", "-NonInteractive", "-Command", command],
        check=True,
        capture_output=True,
        text=True,
    )


def _powershell_function(source: str, name: str) -> str:
    start = source.index(f"function {name}")
    end = source.index("\n}\n", start)
    return source[start : end + 2]


def _machine_bundle():
    return {
        "key_id": "label-producer-key-1",
        "secret": "label-producer-secret-1",
        "machine_credential_bundle": {
            "contract_version": "producer-self-enrollment-machine-credentials-v1",
            "bindings": {"app": "LabelMatch", "program": "Label_Match", "source_host_id": "label-host-1", "device_id": "LABEL-PC-1", "authority_scope_id": "PROD-SCOPE"},
            "credentials": {
                "producer_ingest": {"audience": "producer-ingest-hmac-v1", "auth_scheme": "hmac-sha256", "key_id": "label-producer-key-1", "secret": "label-producer-secret-1"},
                "logistics": {"audience": "worker-analysis-logistics-v1", "auth_scheme": "bearer", "token_header": "X-Logistics-API-Token", "token": "kmta1.label-secret"},
            },
            "profiles": {"logistics": {"contract_version": "km-logistics-runtime-profile-v1", "base_url": "https://worker.kmtecherp.com", "authority_scope": "PROD-SCOPE", "authority_epoch": 7, "authority_plane": "AUTHORITATIVE", "ledger_plane": "SHADOW_CANDIDATE", "plane_epoch": 3, "device_id": "LABEL-PC-1", "source_host_id": "label-host-1", "timeout_seconds": 10}},
        }
    }


def test_common_package_entrypoint_forwards_to_proven_one_step_installer():
    alias = (ROOT / "INSTALL_THIS_PC.ps1").read_text(encoding="utf-8")
    installer = (ROOT / "install_label_match_direct_sync.ps1").read_text(
        encoding="utf-8"
    )

    assert _powershell_function(alias, "Get-FileSha256") == _powershell_function(
        installer, "Get-FileSha256"
    )
    assert "#Requires -RunAsAdministrator" not in alias
    assert "Invoke-SelfElevated $MyInvocation.MyCommand.Path $PSBoundParameters" in alias
    assert "$isRollback = $Rollback.IsPresent" in alias
    assert "Test-ArgumentSwitch" not in alias
    assert "WindowsBuiltInRole]::Administrator" in alias
    assert "-Verb RunAs" in alias
    assert "-Wait -PassThru" in alias
    assert "exit $process.ExitCode" in alias
    _assert_powershell_ast(ROOT / "INSTALL_THIS_PC.ps1")
    assert "install_label_match_direct_sync.ps1" in alias
    assert '$nestedParameters["ManagedInstallRoot"] = $installRoot' in alias
    assert 'if ($entry.Key -in @("InstallRootForTest", "RollbackReceiptRootForTest"))' in alias
    assert '$nestedParameters["PublicWrapperExitCode"]' not in alias
    assert '$nestedParameters["PublicWrapperFailureDiagnostic"]' not in alias
    assert "-PublicWrapperExitCode ([ref]$nestedExitCode)" in alias
    assert "-PublicWrapperFailureDiagnostic ([ref]$nestedFailureDiagnostic)" in alias
    assert 'throw "Nested installer did not return its typed exit code to the public wrapper."' in alias
    assert "$exitCode = [int]$nestedExitCode" in alias
    assert "Assert-ManifestBoundInstalledExecutable $installRoot $manifest" in alias
    assert "Successful nested install lacks verified executable" in alias
    assert 'status = "FAILED"' in alias
    assert "tokenless self-enrollment" in alias
    assert "#Requires -RunAsAdministrator" not in installer
    assert "[System.Management.Automation.PSReference]$PublicWrapperExitCode" in installer
    assert "[System.Management.Automation.PSReference]$PublicWrapperFailureDiagnostic" in installer
    assert installer.count("if ($null -ne $PublicWrapperExitCode)") == 3
    assert installer.count("$PublicWrapperExitCode.Value =") == 3
    normalized_installer = "\n".join(line.strip() for line in installer.splitlines())
    for exit_value in ("2", "0"):
        assert (
            "if ($null -ne $PublicWrapperExitCode) {\n"
            f"$PublicWrapperExitCode.Value = {exit_value}\n"
            "return\n"
            "}\n"
            f"exit {exit_value.replace('[int]', '')}"
        ) in normalized_installer
    assert "$PublicWrapperFailureDiagnostic.Value = $failureDiagnostic" in normalized_installer
    assert (
        "$PublicWrapperExitCode.Value = [int]$exitCode\n"
        "return\n"
        "}\n"
        "exit $exitCode"
    ) in normalized_installer
    assert "Administrator privileges are required for installation or removal" in installer
    assert "Start-ScheduledTask -TaskName $TaskName -ErrorAction Stop" in installer
    assert '$taskStartStatus = "FAILED"' in installer
    assert "Remove-NewMachineProfilesFromRegistrationReport" in installer
    assert "created_paths" in installer
    assert '"--task-removal-phase", "full"' in installer
    assert '"--task-removal-phase", $Phase' in installer
    assert "scheduled_task_lifecycle" in (ROOT / "tools/direct_sync_relay_install_pack.py").read_text(encoding="utf-8")
    assert '[string]$AppRunUser = "*S-1-5-32-545"' in installer
    assert "Read-Host" not in installer
    assert "Producer enrollment token" not in installer
    assert "ExistingProducerManifestPath" in installer
    assert "ExistingCredentialPath" in installer
    assert '"--producer-manifest-path", $ExistingProducerManifestPath' in installer
    assert '"--credential-path", $ExistingCredentialPath' in installer
    assert '"--source-host-id", $resolvedSourceHostId' in installer
    assert (
        'C:\\ProgramData\\KMTech\\Logistics\\profiles\\Label_Match'
        '\\runtime-profile.json'
    ) in installer
    assert '"--logistics-profile-path", $LogisticsProfilePath' in installer
    assert '"C:\\KMTech\\Apps\\Label_Match\\current"' in installer
    assert '"C:\\ProgramData\\KMTech\\DirectSync\\label_match"' in installer
    assert '"bin\\run_direct-sync-relay-label-match.vbs"' in installer
    assert '"queue\\direct_sync_relay.sqlite3"' in installer
    assert "field_layout_contract" in installer
    assert "production_layout_matches" in installer
    assert "AllowNoncanonicalLayoutForTest" in installer
    assert "--allow-noncanonical-layout-for-test" in installer
    assert "KMTECH_FACTORY_INSTALL_TEST_MODE" in installer
    assert "Assert-PackageRoot" in alias
    assert "Copy-ManifestBoundPackage" in alias
    assert "manifest_hashes_and_sizes_verified" in alias
    assert "safe_relative_paths_verified" in alias
    assert "candidate_byte_parity_verified" in alias
    assert '"KMTech\\Label Match.lnk"' in installer
    assert 'scope = "all_users"' in installer
    assert "Ensure-AllUsersLauncher" in installer
    assert "DATA_PRESERVING_UNINSTALL" in installer
    assert "EXACT_FRESH_TARGET_ROLLBACK" in installer
    assert "Copy-BoundedRollbackEvidence" in installer
    assert "Rollback EvidenceArchiveRoot must be a fresh absent path" in installer
    assert "Assert-NoReparsePath" in installer
    assert "created_directory_paths" in installer
    assert "created_install_parent_paths" in alias
    assert "Candidate manifest changed after source validation" in alias
    assert "report is stale, mismatched, or not PASS" in installer
    assert "Install ownership summary does not bind the exact removal resource set" in installer
    assert '$resourceReport.pre_install_parity_claimed = $false' in installer
    assert '$rollbackResourceReport.pre_install_parity_claimed = $true' in alias
    assert "Existing all-users launcher drifted from the owned install summary" in installer
    assert "Nested removal resource report is stale, mismatched, or incomplete" in alias
    ancestry_guard = "EvidenceArchiveRoot overlaps an installer-created directory ancestry path"
    assert ancestry_guard in alias
    assert alias.index(ancestry_guard) < alias.index("& $installer @nestedParameters")
    assert alias.count("Confirm-BoundedRollbackEvidence") >= 3
    assert "evidence_inventory_sha256 = $finalEvidenceVerification.inventory_sha256" in alias
    assert "resource_report_sha256 = $finalResourceReportSha256" in alias
    assert "Rollback evidence file hash changed after preservation" in alias
    assert "Add-Member -NotePropertyName inventory_sha256" in alias
    assert "Add-Member -NotePropertyName archived_bytes_reverified_by_public_wrapper" in alias
    assert 'task_removal_order = @("stop", "delete", "absence")' in installer
    for source in (alias, installer):
        assert "get-filehash" not in source.casefold()
        assert "function Get-FileSha256" in source
        assert "[System.IO.FileShare]::Read" in source
        assert 'throw "SHA-256 authority returned a malformed digest."' in source
        assert '"_internal/config/app_settings.json"' in source
        assert '"label-match-app-immutable-inventory-v1"' in source
        assert "Get-ImmutableAppInventoryIdentity" in source
        assert "mutable_relative_paths" in source
        assert "immutable_inventory_sha256" in source
    assert "UNINSTALLED_DATA_PRESERVED" in alias
    assert "exact_fresh_target_parity" in alias
    _assert_powershell_ast(ROOT / "install_label_match_direct_sync.ps1")


def test_public_uninstall_matches_common_source_contract():
    source = (ROOT / "INSTALL_THIS_PC.ps1").read_text(encoding="utf-8")
    _assert_powershell_ast(ROOT / "INSTALL_THIS_PC.ps1")

    for declaration in (
        '$AppId = "Label_Match"',
        '$AppExecutableName = "Label_Match.exe"',
        '$OwnedScheduledTaskName = "direct-sync-relay-label-match"',
        '$AllUsersShortcutName = "Label Match.lnk"',
    ):
        assert declaration in source

    process_lookup_start = source.index("function Get-OwnedAppProcesses")
    process_lookup_end = source.index("function Get-OwnedScheduledTasks", process_lookup_start)
    process_lookup = source[process_lookup_start:process_lookup_end]
    assert "Get-CimInstance -ClassName Win32_Process" in process_lookup
    assert "Test-SamePath ([string]$_.ExecutablePath) $expectedExecutablePath" in process_lookup

    common_start = source.index("function Invoke-CommonUninstall")
    common_end = source.index("\n$isDryRun =", common_start)
    common = source[common_start:common_end]
    assert "Stop-Process -Id ([int]$ownedProcess.ProcessId)" in common
    assert "Stop-ScheduledTask -InputObject $ownedTask" in common
    assert "Unregister-ScheduledTask -InputObject $ownedTask" in common
    assert "Remove-Item -LiteralPath $AllUsersShortcutPath -Force -ErrorAction Stop" in common
    assert "Remove-Item -LiteralPath $InstallRoot -Recurse -Force -ErrorAction Stop" in common
    assert [
        line.strip() for line in common.splitlines() if "Remove-Item" in line
    ] == [
        "Remove-Item -LiteralPath $AllUsersShortcutPath -Force -ErrorAction Stop",
        "Remove-Item -LiteralPath $InstallRoot -Recurse -Force -ErrorAction Stop",
    ]
    assert common.count("Get-OwnedAppProcesses $InstallRoot $ExecutableName") == 2
    assert common.count("Get-OwnedScheduledTasks $ScheduledTaskName") == 2
    assert 'throw "Owned all-users shortcut remains after uninstall."' in common
    assert 'throw "Replaceable app payload remains after uninstall."' in common
    assert 'throw "Preserved data root was removed during uninstall:' in common
    assert common.rstrip().endswith(
        'Write-Output "uninstall_status=PASS_DATA_PRESERVED"\n}'
    )

    public_branch_start = source.index("\nif ($isUninstall) {", source.index("$isUninstall ="))
    source_validation_start = source.index("\n$sourceRoot =", public_branch_start)
    public_branch = source[public_branch_start:source_validation_start]
    assert "Invoke-CommonUninstall" in public_branch
    assert "$programDataRoot, $scanSourceDir, $logisticsProfileRoot, $receiptRoot" in public_branch
    assert "$env:LOCALAPPDATA" in public_branch
    assert "exit 0" in public_branch
    assert "Assert-PackageRoot" not in public_branch
    assert "public install receipt" not in public_branch.lower()


_NESTED_INSTALLER_STUB = r'''param(
    [switch]$DryRun,
    [switch]$Uninstall,
    [switch]$Rollback,
    [string]$EvidenceArchiveRoot = "",
    [switch]$AllowNoncanonicalLayoutForTest,
    [string]$ManagedInstallRoot = "",
    [string]$SourceManifestSha256 = "",
    [string]$InstallPrestate = "absent",
    [string]$CommonProgramsRootForTest = "",
    [string]$ServerBaseUrl = "https://worker.kmtecherp.com",
    [string]$SourceHostId = "",
    [string]$ProgramDataRoot = "",
    [string]$ScanSourceDir = "",
    [string]$EnrollmentTokenFile = "",
    [string]$ExistingProducerManifestPath = "",
    [string]$ExistingCredentialPath = "",
    [string]$TaskName = "",
    [string]$LogisticsProfilePath = "",
    [string]$TaskRunUser = "",
    [string]$AppRunUser = "",
    [string]$TaskRunPasswordEnv = "",
    [string]$TaskRunPasswordFile = "",
    [switch]$AllowInteractiveTaskForLocalTest,
    [System.Management.Automation.PSReference]$PublicWrapperExitCode = $null,
    [System.Management.Automation.PSReference]$PublicWrapperFailureDiagnostic = $null
)
$ErrorActionPreference = "Stop"
if ($env:LABEL_MATCH_INSTALL_STUB_MODE -ceq "nested_failure") {
    if ($null -eq $PublicWrapperExitCode) { exit 9 }
    $PublicWrapperFailureDiagnostic.Value = [ordered]@{
        diagnostic_version = "label-match-child-failure-v1"
        command_identity = "tools.register_label_match_worker_pc.main"
        child_exit_code = $null
        failure_code = "CHILD_IMPORT_FAILED"
        inner_exception_type = "ImportError"
        inner_exception_message = "synthetic import failure token=seq184-public-secret " + ("x" * 800)
        raw_output = "seq184-raw-output-secret"
    }
    $PublicWrapperExitCode.Value = 9
    return
}
if ($env:LABEL_MATCH_INSTALL_STUB_MODE -ceq "missing_executable") {
    Remove-Item -LiteralPath (Join-Path $ManagedInstallRoot "Label_Match.exe") -Force
}
$summaryStatus = "DRY_RUN"
if ($env:LABEL_MATCH_INSTALL_STUB_MODE -ceq "production_success") {
    # The outer invocation remains a safe isolated DryRun through elevation and
    # staging; switch only its post-child branch to exercise PASS finalization.
    Set-Variable -Scope 1 -Name isDryRun -Value $false
    $summaryStatus = "PASS"
}
$entries = @(
    Get-ChildItem -LiteralPath $ManagedInstallRoot -Force -File -Recurse |
        ForEach-Object {
            $relative = $_.FullName.Substring($ManagedInstallRoot.TrimEnd('\').Length + 1).Replace('\', '/')
            if ($relative -cne "_internal/config/app_settings.json") {
                [ordered]@{
                    path = $relative
                    size = [long]$_.Length
                    sha256 = ([BitConverter]::ToString(([Security.Cryptography.SHA256]::Create()).ComputeHash([IO.File]::OpenRead($_.FullName)))).Replace('-', '').ToLowerInvariant()
                }
            }
        } | Sort-Object @{ Expression = { $_.path.ToLowerInvariant() } }, @{ Expression = { $_.path } }
)
$builder = New-Object Text.StringBuilder
foreach ($entry in $entries) {
    [void]$builder.Append($entry.path).Append("`t").Append($entry.size).Append("`t").Append($entry.sha256).Append("`n")
}
$sha = [Security.Cryptography.SHA256]::Create()
try {
    $identity = ([BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes($builder.ToString())))).Replace('-', '').ToLowerInvariant()
}
finally { $sha.Dispose() }
$summaryPath = Join-Path $ProgramDataRoot "status\label_match_one_step_install_summary.json"
New-Item -ItemType Directory -Path (Split-Path -Parent $summaryPath) -Force | Out-Null
[ordered]@{
    installer_report_version = "label-match-direct-sync-one-step-install-v2"
    status = $summaryStatus
    source_manifest_sha256 = $SourceManifestSha256
    resources = [ordered]@{
        app_root = [ordered]@{
            path = [IO.Path]::GetFullPath($ManagedInstallRoot)
            inventory_contract = "label-match-app-immutable-inventory-v1"
            mutable_relative_paths = @("_internal/config/app_settings.json")
            immutable_file_count = $entries.Count
            immutable_inventory_sha256 = $identity
        }
    }
} | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $summaryPath -Encoding UTF8
if ($null -eq $PublicWrapperExitCode) { exit 0 }
$PublicWrapperExitCode.Value = 0
return
'''


def _write_public_installer_fixture(root: Path) -> None:
    root.mkdir()
    shutil.copy2(ROOT / "INSTALL_THIS_PC.ps1", root / "INSTALL_THIS_PC.ps1")
    (root / "install_label_match_direct_sync.ps1").write_text(
        _NESTED_INSTALLER_STUB, encoding="utf-8"
    )
    (root / "Label_Match.exe").write_bytes(b"manifest-bound-executable")
    (root / "_internal/config").mkdir(parents=True)
    (root / "_internal/config/app_settings.json").write_text("{}\n", encoding="utf-8")
    (root / "_internal/python312.dll").write_bytes(b"embedded-runtime")
    (root / "_internal/base_library.zip").write_bytes(b"embedded-library")
    (root / "tools").mkdir(parents=True)
    (root / "tools/invoke_embedded_python.ps1").write_text("# in-process host\n", encoding="utf-8")
    (root / "tools/direct_sync_relay_install_pack.py").write_text("# install pack\n", encoding="utf-8")
    (root / "tools/direct_sync_relay_runner.py").write_text("# runner\n", encoding="utf-8")
    (root / "tools/direct_sync_relay_runner.exe").write_bytes(b"packaged runner executable")
    (root / "tools/register_label_match_worker_pc.py").write_text("# registration\n", encoding="utf-8")
    inventory = []
    for path in sorted(
        (item for item in root.rglob("*") if item.is_file()),
        key=lambda item: (item.relative_to(root).as_posix().casefold(), item.relative_to(root).as_posix()),
    ):
        relative = path.relative_to(root).as_posix()
        inventory.append(
            {
                "path": relative,
                "size": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    (root / "build-manifest.json").write_text(
        json.dumps(
            {"build_manifest_schema_version": 1, "payload_inventory": inventory},
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def _run_public_installer_fixture(tmp_path: Path, mode: str):
    powershell = shutil.which("powershell") or shutil.which("pwsh")
    if not powershell:
        pytest.skip("PowerShell is unavailable")
    package = tmp_path / f"ordinary-extraction-{mode}"
    _write_public_installer_fixture(package)
    if mode == "staging_failure":
        (package / "Label_Match.exe").write_bytes(b"changed-after-manifest")
    roots = {name: tmp_path / f"{mode}-{name}" for name in ("app", "programs", "receipts", "data", "scan", "profiles")}
    command = [
        powershell,
        "-NoProfile",
        "-NonInteractive",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(package / "INSTALL_THIS_PC.ps1"),
        "-DryRun",
        "-AllowNoncanonicalLayoutForTest",
        "-InstallRootForTest",
        str(roots["app"]),
        "-CommonProgramsRootForTest",
        str(roots["programs"]),
        "-RollbackReceiptRootForTest",
        str(roots["receipts"]),
        "-ProgramDataRoot",
        str(roots["data"]),
        "-ScanSourceDir",
        str(roots["scan"]),
        "-LogisticsProfilePath",
        str(roots["profiles"] / "runtime-profile.json"),
    ]
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env={
            **os.environ,
            "KMTECH_FACTORY_INSTALL_TEST_MODE": "1",
            "LABEL_MATCH_INSTALL_STUB_MODE": mode,
        },
    )
    return completed, roots


def test_public_installer_cannot_false_succeed_after_ordinary_extraction(tmp_path):
    completed, roots = _run_public_installer_fixture(tmp_path, "missing_executable")

    assert completed.returncode != 0
    assert not (roots["app"] / "Label_Match.exe").exists()
    receipt = json.loads(
        (roots["receipts"] / "label_match_public_install_report.json").read_text(encoding="utf-8-sig")
    )
    assert receipt["status"] == "FAILED"
    assert "canonical installed executable is missing" in receipt["failure"].lower()


def test_public_installer_success_binds_executable_summary_and_receipt(tmp_path):
    completed, roots = _run_public_installer_fixture(tmp_path, "production_success")

    assert completed.returncode == 0, completed.stderr
    executable = roots["app"] / "Label_Match.exe"
    assert executable.read_bytes() == b"manifest-bound-executable"
    receipt = json.loads(
        (roots["receipts"] / "label_match_public_install_report.json").read_text(encoding="utf-8-sig")
    )
    assert receipt["status"] == "PASS"
    assert receipt["installed_executable"]["sha256"] == hashlib.sha256(executable.read_bytes()).hexdigest()
    assert receipt["install_summary"]["status"] == "PASS"
    assert receipt["install_summary"]["source_manifest_sha256"] == receipt["source_manifest_sha256"]


def test_public_installer_nested_failure_is_nonzero_without_success_receipt(tmp_path):
    completed, roots = _run_public_installer_fixture(tmp_path, "nested_failure")

    assert completed.returncode == 9
    assert not roots["app"].exists()
    receipt = json.loads(
        (roots["receipts"] / "label_match_public_install_report.json").read_text(encoding="utf-8-sig")
    )
    assert receipt["status"] == "FAILED"
    assert receipt["nested_exit_code"] == 9
    diagnostic = receipt["failure_diagnostic"]
    assert diagnostic["diagnostic_version"] == "label-match-child-failure-v1"
    assert diagnostic["command_identity"] == "tools.register_label_match_worker_pc.main"
    assert diagnostic["child_exit_code"] is None
    assert diagnostic["failure_code"] == "CHILD_IMPORT_FAILED"
    assert diagnostic["inner_exception_type"] == "ImportError"
    assert diagnostic["inner_exception_message"].startswith(
        "synthetic import failure token=[redacted]"
    )
    assert len(diagnostic["inner_exception_message"]) <= 512
    serialized = json.dumps(receipt)
    assert "seq184-public-secret" not in serialized
    assert "seq184-raw-output-secret" not in serialized


def test_public_installer_staging_failure_is_nonzero_without_success_receipt(tmp_path):
    completed, roots = _run_public_installer_fixture(tmp_path, "staging_failure")

    assert completed.returncode != 0
    assert not roots["app"].exists()
    assert not (roots["receipts"] / "label_match_public_install_report.json").exists()
    assert "manifest" in completed.stderr.lower()


def test_production_pass_and_removal_ownership_remain_postcondition_guarded():
    public_installer = (ROOT / "INSTALL_THIS_PC.ps1").read_text(encoding="utf-8")

    nested_call = public_installer.index(
        "-PublicWrapperExitCode ([ref]$nestedExitCode)"
    )
    postcondition = public_installer.index(
        "Assert-ManifestBoundInstalledExecutable $installRoot $manifest",
        nested_call,
    )
    pass_receipt = public_installer.index(
        '$publicReport.status = if ($isDryRun) { "DRY_RUN_STAGED" } else { "PASS" }',
        postcondition,
    )
    assert nested_call < postcondition < pass_receipt
    assert (
        "[void](Assert-OwnedInstalledTarget $installRoot $summaryPath "
        "$sourceManifestSha256 $expectedSummaryStatus)"
    ) in public_installer
    assert (
        '$priorPublicReport.status -cne "PASS"' in public_installer
        and "Assert-OwnedInstalledTarget $installRoot $summaryPath $sourceManifestSha256" in public_installer
    )
    assert 'throw "Removal requires an owned successful public install receipt."' in public_installer


def test_nonproduction_server_and_identity_override_is_public_and_documented():
    public_installer = (ROOT / "INSTALL_THIS_PC.ps1").read_text(encoding="utf-8")
    nested_installer = (ROOT / "install_label_match_direct_sync.ps1").read_text(
        encoding="utf-8"
    )

    for source in (public_installer, nested_installer):
        assert '[string]$ServerBaseUrl = "https://worker.kmtecherp.com"' in source
        assert '[string]$SourceHostId = ""' in source

    assert "$nestedParameters[$entry.Key] = $entry.Value" in public_installer
    assert "Get-SafeToken $SourceHostId \"\"" in nested_installer
    assert '"--server-base-url", $ServerBaseUrl' in nested_installer
    assert '"--source-host-id", $resolvedSourceHostId' in nested_installer
    assert "<NON_PRODUCTION_SERVER_BASE_URL>" in public_installer
    assert "<NON_PRODUCTION_SOURCE_HOST_ID>" in public_installer
    assert "LABEL_MATCH_DIRECT_SYNC_SERVER_BASE_URL" in public_installer
    assert "LABEL_MATCH_DIRECT_SYNC_SOURCE_HOST_ID" in public_installer
    _assert_powershell_ast(ROOT / "INSTALL_THIS_PC.ps1")
    _assert_powershell_ast(ROOT / "install_label_match_direct_sync.ps1")


def test_mutable_runtime_settings_do_not_mask_immutable_app_drift(tmp_path):
    powershell = shutil.which("powershell") or shutil.which("pwsh")
    if not powershell:
        pytest.skip("PowerShell is unavailable")
    app_root = tmp_path / "installed-app"
    mutable_settings = app_root / "_internal/config/app_settings.json"
    root_settings = app_root / "config/app_settings.json"
    mutable_settings.parent.mkdir(parents=True)
    root_settings.parent.mkdir(parents=True)
    mutable_settings.write_text("{\"worker\":\"before\"}\n", encoding="utf-8")
    root_settings.write_text("{\"provider\":\"github\"}\n", encoding="utf-8")
    (app_root / "Label_Match.exe").write_bytes(b"owned-executable")
    command = r"""
$ErrorActionPreference = "Stop"
Import-Module Microsoft.PowerShell.Utility -ErrorAction Stop
$tokens = $null
$errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
  $env:LABEL_MATCH_PUBLIC_INSTALLER,
  [ref]$tokens,
  [ref]$errors
)
if ($errors.Count) { throw "public installer AST is invalid" }
$wanted = @(
  "Test-PathInside",
  "Get-RelativeFilePath",
  "Get-Sha256HexFromText",
  "Get-FileSha256",
  "Get-ImmutableAppInventoryIdentity"
)
foreach ($name in $wanted) {
  $node = $ast.FindAll({
    param($candidate)
    $candidate -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
      $candidate.Name -ceq $name
  }, $true) | Select-Object -First 1
  if ($null -eq $node) { throw "missing function: $name" }
  Invoke-Expression $node.Extent.Text
}
$allowlist = @("_internal/config/app_settings.json")
$root = $env:LABEL_MATCH_INVENTORY_ROOT
$before = Get-ImmutableAppInventoryIdentity $root $allowlist
[IO.File]::WriteAllText(
  (Join-Path $root "_internal\config\app_settings.json"),
  '{"worker":"after"}',
  [Text.UTF8Encoding]::new($false)
)
$afterMutable = Get-ImmutableAppInventoryIdentity $root $allowlist
[IO.File]::WriteAllText(
  (Join-Path $root "config\app_settings.json"),
  '{"provider":"changed"}',
  [Text.UTF8Encoding]::new($false)
)
$afterRootConfig = Get-ImmutableAppInventoryIdentity $root $allowlist
[IO.File]::WriteAllText(
  (Join-Path $root "unexpected.bin"),
  'unexpected',
  [Text.UTF8Encoding]::new($false)
)
$afterExtra = Get-ImmutableAppInventoryIdentity $root $allowlist
Remove-Item -LiteralPath (Join-Path $root "_internal\config\app_settings.json") -Force
$missingRejected = $false
try { Get-ImmutableAppInventoryIdentity $root $allowlist | Out-Null }
catch { $missingRejected = $true }
[ordered]@{
  mutable_same = (
    $before.immutable_file_count -eq $afterMutable.immutable_file_count -and
    $before.immutable_sha256 -ceq $afterMutable.immutable_sha256
  )
  root_config_changed = $before.immutable_sha256 -cne $afterRootConfig.immutable_sha256
  extra_changed = (
    $afterRootConfig.immutable_file_count -ne $afterExtra.immutable_file_count -and
    $afterRootConfig.immutable_sha256 -cne $afterExtra.immutable_sha256
  )
  missing_mutable_rejected = $missingRejected
} | ConvertTo-Json -Compress
"""
    completed = subprocess.run(
        [powershell, "-NoProfile", "-NonInteractive", "-Command", command],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env={
            **os.environ,
            "LABEL_MATCH_PUBLIC_INSTALLER": str(ROOT / "INSTALL_THIS_PC.ps1"),
            "LABEL_MATCH_INVENTORY_ROOT": str(app_root),
        },
    )
    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout)
    assert result == {
        "mutable_same": True,
        "root_config_changed": True,
        "extra_changed": True,
        "missing_mutable_rejected": True,
    }


def test_frozen_release_exact_manifest_preserves_common_package_entrypoint():
    loaded_validator = frozen_verifier._load_release_archive_validator()

    assert "INSTALL_THIS_PC.ps1" in frozen_verifier.REQUIRED_MEMBERS
    assert "INSTALL_THIS_PC.ps1" in loaded_validator.REQUIRED_PACKAGE_MEMBERS
    assert callable(loaded_validator.validate_release_evidence)
    assert (ROOT / "INSTALL_THIS_PC.ps1").is_file()


def test_enrollment_bundle_installs_dpapi_profile_and_rejects_scope_mismatch(monkeypatch, tmp_path):
    observed = {}
    monkeypatch.setattr(machine_profiles, "install_runtime_profile", lambda **kwargs: observed.update(kwargs) or {"status": "installed", "created_paths": []})
    result = machine_profiles.ensure_runtime_profile_from_enrollment_bundle(
        _machine_bundle(), expected_app="LabelMatch", expected_program="Label_Match",
        expected_source_host_id="label-host-1", expected_device_id="LABEL-PC-1",
        profile_path=tmp_path / "runtime-profile.json",
    )
    assert result["status"] == "installed"
    assert observed["ledger_plane"] == "SHADOW_CANDIDATE"
    assert observed["bearer_token"] == "kmta1.label-secret"
    invalid = _machine_bundle()
    invalid["machine_credential_bundle"]["bindings"]["authority_scope_id"] = "OTHER"
    with pytest.raises(ValueError, match="profile identity mismatch"):
        machine_profiles.ensure_runtime_profile_from_enrollment_bundle(
            invalid, expected_app="LabelMatch", expected_program="Label_Match",
            expected_source_host_id="label-host-1", expected_device_id="LABEL-PC-1",
            profile_path=tmp_path / "other.json",
        )


@pytest.mark.parametrize(
    ("case", "message"),
    [
        ("bundle_extra", "bundle fields"),
        ("bindings_extra", "binding fields"),
        ("profiles_extra", "profile sections"),
        ("credentials_extra", "credential sections"),
        ("producer_extra", "producer ingest credential fields"),
        ("producer_contract", "producer ingest credential contract"),
        ("producer_key_mismatch", "producer ingest credential contract"),
        ("producer_secret_mismatch", "producer ingest credential contract"),
        ("logistics_extra", "logistics credential fields"),
        ("logistics_contract", "logistics credential contract"),
        ("shared_secret", "distinct secrets"),
    ],
)
def test_enrollment_bundle_rejects_nonfinal_server_shapes(
    monkeypatch, tmp_path, case, message
):
    invalid = _machine_bundle()
    bundle = invalid["machine_credential_bundle"]
    producer = bundle["credentials"]["producer_ingest"]
    logistics = bundle["credentials"]["logistics"]
    if case == "bundle_extra":
        bundle["unexpected"] = True
    elif case == "bindings_extra":
        bundle["bindings"]["unexpected"] = True
    elif case == "profiles_extra":
        bundle["profiles"]["unexpected"] = {}
    elif case == "credentials_extra":
        bundle["credentials"]["unexpected"] = {}
    elif case == "producer_extra":
        producer["unexpected"] = True
    elif case == "producer_contract":
        producer["auth_scheme"] = "bearer"
    elif case == "producer_key_mismatch":
        producer["key_id"] = "other-key"
    elif case == "producer_secret_mismatch":
        producer["secret"] = "other-secret"
    elif case == "logistics_extra":
        logistics["unexpected"] = True
    elif case == "logistics_contract":
        logistics["token_header"] = "Authorization"
    elif case == "shared_secret":
        logistics["token"] = invalid["secret"]
    monkeypatch.setattr(
        machine_profiles,
        "install_runtime_profile",
        lambda **_kwargs: pytest.fail("invalid bundle reached profile installer"),
    )
    with pytest.raises(ValueError, match=message):
        machine_profiles.ensure_runtime_profile_from_enrollment_bundle(
            invalid,
            expected_app="LabelMatch",
            expected_program="Label_Match",
            expected_source_host_id="label-host-1",
            expected_device_id="LABEL-PC-1",
            profile_path=tmp_path / f"{case}.json",
        )

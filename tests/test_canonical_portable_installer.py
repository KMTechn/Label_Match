import base64
from tests.test_placement_launcher_transport import transport_command
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

import pytest

from current_user_onboarding import (
    _portable_stop_marker_release_preflight,
    resolve_current_user_onboarding_paths,
)
from label_exact_clone_resolution import capture_conflict_preimage
from tools import build_portable_release_candidate as portable_builder
from tests.test_label_exact_clone_resolution import _paths as _exact_clone_paths
from tests._shared_portable_fixture import shared_bootstrap_files


ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / "INSTALL_CANONICAL_PORTABLE.ps1"
HELPER = ROOT / "INSTALL_THIS_PC.ps1"
INTEGRITY_HELPER = ROOT / "tools" / "bootstrap_integrity.ps1"
WRITER_FENCE_HELPER = ROOT / "tools" / "label_writer_fence.ps1"
WRITER_FENCE_CONTRACT = ROOT / "tools" / "label_writer_fence_contract.json"
PRISTINE_RESIDUE_KINDS = (
    "canonical-tree",
    "run-autostart",
    "relay",
    "scheduled-task",
    "stop-marker",
    "credential",
    "data-root",
    "settings",
    "profile",
    "profile-secret",
    "bootstrap-ca",
)


def _source(path: Path = INSTALLER) -> str:
    return path.read_text(encoding="utf-8")


def _run_clean_install_receipt_gate_harness(
    tmp_path: Path,
    *,
    extra_environment: dict[str, str] | None = None,
    extra_arguments: list[str] | None = None,
) -> subprocess.CompletedProcess[str]:
    source_root = tmp_path / "portable"
    install_root = tmp_path / "canonical" / "current"
    local_app_data = tmp_path / "local-app-data"
    source_root.mkdir(parents=True)

    installer_source = _source()
    sentinel = "$runId = (Get-Date).ToUniversalTime()"
    assert sentinel in installer_source
    assert installer_source.index(
        "if ($null -eq $receiptSource)"
    ) < installer_source.index(sentinel)
    installer_source = installer_source.replace(
        sentinel,
        "Write-Output 'receipt_gate_status=PASS'\nexit 0\n\n" + sentinel,
        1,
    )
    # The receipt gate uses the real packet/G checks, with native state supplied
    # by an absent-machine double instead of the host's Run/tasks/relay.
    installer_source = installer_source.replace("$legacyRelayBefore=Get-LabelOfficialLegacyTaskSnapshot", """
function Snapshot { [pscustomobject]@{exists=$false;kind='';data=''} }
function Relays { @() }
function Get-ScheduledTask { @() }
function Get-LabelCurrentUserScheduledTasks { @() }
function Get-LabelOfficialLegacyTaskSnapshot { return $null }
$legacyRelayBefore=Get-LabelOfficialLegacyTaskSnapshot
""", 1)
    from tools.build_portable_release_candidate import _copy_application, RUNTIME_ROOT_FILES
    from tests.test_current_user_onboarding import _write_bootstrap_inventory_record
    from label_data_generation import CURRENT_VERSION
    import shutil
    import zipfile
    python_home = Path(sys.base_prefix)
    runtime_root = source_root / "runtime"
    (runtime_root / "DLLs").mkdir(parents=True)
    for name in RUNTIME_ROOT_FILES:
        shutil.copy2(python_home / name, runtime_root / name)
    for extension in (python_home / "DLLs").iterdir():
        if extension.is_file() and extension.suffix.lower() in {".dll", ".pyd"}:
            shutil.copy2(extension, runtime_root / "DLLs" / extension.name)
    # CPython's supported stdlib ZIP keeps the real runtime self-contained
    # while the receipt gate's full inventory stays within its existing budget.
    with zipfile.ZipFile(runtime_root / "python312.zip", "w", zipfile.ZIP_STORED) as archive:
        for directory, directories, files in os.walk(python_home / "Lib"):
            directories[:] = sorted(name for name in directories
                                    if name not in {"site-packages", "__pycache__", "test", "ensurepip", "idlelib", "turtledemo"})
            for name in sorted(files):
                path = Path(directory) / name
                archive.write(path, path.relative_to(python_home / "Lib").as_posix())
    runtime_check = subprocess.run([str(runtime_root / "python.exe"), "-I", "-B", "-c",
                                   "import encodings, sys; assert sys.version_info[:3] == (3, 12, 10)"],
                                  check=False, capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    assert runtime_check.returncode == 0, "Isolated CPython fixture did not start"
    _copy_application(ROOT, source_root / "app")
    files = {
        "runtime/python.exe": (source_root / "runtime/python.exe").read_bytes(),
        "runtime/pythonw.exe": (source_root / "runtime/pythonw.exe").read_bytes(),
        "app/main.py": b"raise SystemExit('must not run')\n",
        "launch-label-match.cmd": b"@echo off\r\nexit /b 99\r\n",
        "INSTALL_CANONICAL_PORTABLE.ps1": installer_source.encode("utf-8"),
        "INSTALL_THIS_PC.ps1": (ROOT / "INSTALL_THIS_PC.ps1").read_bytes(),
        "tools/bootstrap_integrity.ps1": (ROOT / "tools/bootstrap_integrity.ps1").read_bytes(),
        "tools/label_writer_fence.ps1": WRITER_FENCE_HELPER.read_bytes(),
        "tools/label_writer_fence_contract.json": (ROOT / "tools/label_writer_fence_contract.json").read_bytes(),
        **shared_bootstrap_files("app/"),
    }
    for relative, content in files.items():
        target = source_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)

    manifest = {
        "schema": "label-match-portable-tree-v1",
        "version": CURRENT_VERSION,
        "entrypoint": "runtime/pythonw.exe app/main.py",
        "launcher": "launch-label-match.cmd",
        "source_commit": "1" * 40,
        "source_tree": "2" * 40,
        "allowed_unsigned_app_pe": [],
        "forbidden_package_roots": [],
        "runtime_pythonw_sha256": hashlib.sha256(
            files["runtime/pythonw.exe"]
        ).hexdigest(),
        "launcher_sha256": hashlib.sha256(
            files["launch-label-match.cmd"]
        ).hexdigest(),
    }
    (source_root / "portable-manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    _write_bootstrap_inventory_record(source_root)

    powershell = (
        Path(os.environ["SystemRoot"])
        / "System32"
        / "WindowsPowerShell"
        / "v1.0"
        / "powershell.exe"
    )
    environment = os.environ.copy()
    environment["KMTECH_FACTORY_INSTALL_TEST_MODE"] = "1"
    environment["LOCALAPPDATA"] = str(local_app_data)
    for name in (
        "KMTECH_LABEL_CONFLICT_RESOLUTION_RECEIPT_PATH",
        "KMTECH_LABEL_CONFLICT_RESOLUTION_RECEIPT_SHA256",
        "LABEL_MATCH_DIRECT_SYNC_ROOT",
        "LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT",
        "LABEL_MATCH_SAVE_DIR",
        "LABEL_MATCH_SETTINGS_PATH",
        "LABEL_MATCH_LOGISTICS_PROFILE_PATH",
    ):
        environment.pop(name, None)
    if extra_environment:
        environment.update(extra_environment)
    return subprocess.run(
        [
            str(powershell),
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(source_root / "INSTALL_CANONICAL_PORTABLE.ps1"),
            "-SourceRoot",
            str(source_root),
            "-InstallRoot",
            str(install_root),
            "-AllowNoncanonicalLayoutForTest",
            "-SkipSignatureValidationForTest",
            *(extra_arguments or []),
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="oem",
        timeout=30,
        env=environment,
    )


def _run_pristine_removal_gate_harness(
    tmp_path: Path, *, pristine_install: bool = True, unexpected_writer: bool = False
) -> dict[str, object]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    source = _source()
    removal_anchor = "    $removalRoot = if ($existingVerified) { $install } else { $source }"
    removal_anchor_index = source.index(removal_anchor)
    guarded_start = source.rfind(
        "    if (-not $pristineInstall", 0, removal_anchor_index
    )
    assert guarded_start >= 0
    block_start = guarded_start
    block_end = source.index(
        "    $unquiesced = @(UnquiescedProductWriters)", removal_anchor_index
    )
    removal_block = source[block_start:block_end]
    wait_start = source.index("function Wait-LabelRelayPreimageExit")
    relay_exit = source[wait_start:source.index("\nfunction HealthyLifecycle", wait_start)]
    harness = tmp_path / "pristine-removal-gate.ps1"
    marker = tmp_path / "label_match_user_relay.stop.json"
    removal_report = tmp_path / "current_user_removal.json"
    harness.write_text(
        rf"""
$ErrorActionPreference = 'Stop'
$script:removalCalls = 0
$script:unexpectedWriter = {'$true' if unexpected_writer else '$false'}
$old = @()
$legacyFirstRegistration = $false
$healthyWithoutCode = $false
$pristineInstall = {'$true' if pristine_install else '$false'}
$healthyLifecycle = $null
$healthyRemovedInstall = $false
$existingVerified = $false
$source = '{str(tmp_path / "source").replace("'", "''")}'
$install = '{str(tmp_path / "install").replace("'", "''")}'
$stop = '{str(marker).replace("'", "''")}'
$removalPath = '{str(removal_report).replace("'", "''")}'
$CanonicalTaskName = 'direct-sync-relay-label-match'

function Product([string]$Root, [string]$Mode) {{
    $script:removalCalls += 1
    [IO.File]::WriteAllText($stop, '{{}}')
    [IO.File]::WriteAllText(
        $removalPath,
        '{{"status":"PASS_DATA_PRESERVED","relay_process":{{"status":"ABSENT"}}}}'
    )
}}
function Snapshot {{ return [ordered]@{{ exists = $false; kind = ''; data = '' }} }}
function Get-ScheduledTask {{ return $null }}
function Get-LabelCurrentUserScheduledTasks {{ return $null }}
function Get-LabelOfficialLegacyTaskSnapshot {{ return $null }}
function UnquiescedProductWriters {{
    if ($script:unexpectedWriter) {{
        return [pscustomobject]@{{ ExecutablePath = 'C:\isolated\pythonw.exe'; CommandLine = 'pythonw.exe --unexpected' }}
    }}
    return @()
}}
{relay_exit}

{removal_block}

[pscustomobject][ordered]@{{
    removal_calls = $script:removalCalls
    marker_exists = Test-Path -LiteralPath $stop
}} | ConvertTo-Json -Compress
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
        encoding="oem",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return json.loads(completed.stdout)


def _run_pristine_predicate_harness(tmp_path: Path, residue: str) -> bool:
    tmp_path.mkdir(parents=True, exist_ok=True)
    source = _source()
    start = source.index("function Test-PristineInstallState")
    end = source.index("\nif (-not $SourceRoot)", start)
    predicate = source[start:end]

    install = tmp_path / "canonical" / "current"
    direct_sync = tmp_path / "direct-sync"
    stop_marker = direct_sync / "control" / "label_match_user_relay.stop.json"
    credential = direct_sync / "credential.json"
    data_root = tmp_path / "data"
    settings = tmp_path / "config" / "app_settings.json"
    profile = tmp_path / "profile" / "runtime-profile.json"
    secret = profile.parent / "secrets" / "bearer-token.dpapi"
    bootstrap_ca = tmp_path / "bootstrap" / "ca-bundle.pem"
    path_residue = {
        "canonical-tree": install,
        "stop-marker": stop_marker,
        "credential": credential,
        "data-root": data_root,
        "settings": settings,
        "profile": profile,
        "profile-secret": secret,
        "bootstrap-ca": bootstrap_ca,
    }
    if residue in path_residue:
        selected = path_residue[residue]
        if residue in {"canonical-tree", "data-root"}:
            selected.mkdir(parents=True)
        else:
            selected.parent.mkdir(parents=True, exist_ok=True)
            selected.write_text("residue\n", encoding="utf-8")

    quoted_paths = ",\n    ".join(
        "'" + str(path).replace("'", "''") + "'"
        for path in (
            direct_sync,
            stop_marker,
            credential,
            data_root,
            settings,
            profile,
            secret,
            bootstrap_ca,
        )
    )
    run_exists = "$true" if residue == "run-autostart" else "$false"
    relay_values = (
        "@([pscustomobject]@{ ProcessId = 123 })" if residue == "relay" else "@()"
    )
    scheduled_tasks = (
        "@([pscustomobject]@{ TaskName = 'direct-sync-relay-label-match' })"
        if residue == "scheduled-task"
        else "@()"
    )
    harness = tmp_path / f"pristine-predicate-{residue}.ps1"
    harness.write_text(
        rf"""
$ErrorActionPreference = 'Stop'
{predicate}
$install = '{str(install).replace("'", "''")}'
$runSnapshot = [pscustomobject]@{{ exists = {run_exists} }}
$relaySnapshot = {relay_values}
$scheduledTaskSnapshot = {scheduled_tasks}
$residuePaths = @(
    {quoted_paths}
)
$result = Test-PristineInstallState `
    -InstallRootValue $install `
    -RunSnapshotValue $runSnapshot `
    -RelaySnapshotValue $relaySnapshot `
    -ScheduledTaskValues $scheduledTaskSnapshot `
    -ResiduePaths $residuePaths
[pscustomobject]@{{ pristine = [bool]$result }} | ConvertTo-Json -Compress
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
        encoding="oem",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return bool(json.loads(completed.stdout)["pristine"])


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
        encoding="oem",
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
        encoding="oem",
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


def test_clean_install_without_conflict_receipt_reaches_post_gate_sentinel(
    tmp_path: Path,
) -> None:
    completed = _run_clean_install_receipt_gate_harness(tmp_path)

    combined = completed.stdout + completed.stderr
    assert not (tmp_path / "canonical" / "current").exists()
    if completed.returncode != 0:
        assert (
            "Pinned conflict-resolution receipt source validation is required."
            in combined
        )
        raise RuntimeError(combined)

    assert completed.returncode == 0, combined
    assert "receipt_gate_status=PASS" in completed.stdout


def test_old_pc_wide_logistics_anchor_neither_blocks_nor_touches_clean_install(
    tmp_path: Path,
) -> None:
    # TEST1 09-30: Machine KM_LOGISTICS_* from the old PC-wide rollout pointed at
    # a shared profile, and the clean install refused as noncanonical state.
    shared_profile = tmp_path / "ProgramData" / "KMTech" / "Logistics" / "runtime-profile.json"
    shared_secret = shared_profile.parent / "secrets" / "bearer-token.dpapi"
    shared_secret.parent.mkdir(parents=True)
    shared_profile.write_bytes(b'{"program": "other"}\n')
    shared_secret.write_bytes(b"other-program-secret")

    completed = _run_clean_install_receipt_gate_harness(
        tmp_path / "installer",
        extra_environment={
            "KM_LOGISTICS_PROFILE_PATH": str(shared_profile),
            "KM_LOGISTICS_REQUIRED": "1",
        },
    )

    combined = completed.stdout + completed.stderr
    assert completed.returncode == 0, combined
    assert "receipt_gate_status=PASS" in completed.stdout
    assert shared_profile.read_bytes() == b'{"program": "other"}\n'
    assert shared_secret.read_bytes() == b"other-program-secret"


@pytest.mark.parametrize(
    "residue",
    PRISTINE_RESIDUE_KINDS,
)
def test_pristine_predicate_rejects_every_residue_kind(
    tmp_path: Path, residue: str
) -> None:
    assert _run_pristine_predicate_harness(tmp_path, residue) is False


def test_pristine_predicate_accepts_only_an_empty_machine(tmp_path: Path) -> None:
    assert _run_pristine_predicate_harness(tmp_path, "none") is True


def test_pristine_install_skips_removal_without_creating_stop_marker(
    tmp_path: Path,
) -> None:
    assert _run_pristine_removal_gate_harness(tmp_path) == {
        "removal_calls": 0,
        "marker_exists": False,
    }


def test_pristine_removal_wait_refuses_unexpected_writer(tmp_path: Path) -> None:
    with pytest.raises(AssertionError, match="Unexpected writer remains after normal removal"):
        _run_pristine_removal_gate_harness(tmp_path, unexpected_writer=True)


@pytest.mark.parametrize("residue", PRISTINE_RESIDUE_KINDS)
def test_every_residue_kind_keeps_removal_enabled(
    tmp_path: Path, residue: str
) -> None:
    pristine = _run_pristine_predicate_harness(tmp_path / "predicate", residue)
    assert pristine is False
    assert _run_pristine_removal_gate_harness(
        tmp_path / "removal", pristine_install=pristine
    ) == {
        "removal_calls": 1,
        "marker_exists": True,
    }


def test_pristine_absence_keeps_onboarding_preflight_not_required(
    tmp_path: Path,
) -> None:
    environment = {"LOCALAPPDATA": str(tmp_path / "local-app-data")}
    paths = resolve_current_user_onboarding_paths(
        tmp_path / "canonical" / "current", environ=environment
    )

    assert _portable_stop_marker_release_preflight(
        paths, environ=environment
    ) == {
        "status": "NOT_REQUIRED",
        "marker_present": False,
    }


def test_exact_clone_conflict_without_receipt_remains_gated(tmp_path: Path) -> None:
    conflict_paths = _exact_clone_paths(tmp_path / "exact-clone")
    preimage = capture_conflict_preimage(**conflict_paths)
    assert preimage["status"] == "CONFLICT_CONFIRMED"
    direct_sync_root = conflict_paths["client_db_path"].parents[1]

    completed = _run_clean_install_receipt_gate_harness(
        tmp_path / "installer",
        extra_environment={
            "LABEL_MATCH_DIRECT_SYNC_ROOT": str(direct_sync_root),
        },
    )

    combined = completed.stdout + completed.stderr
    assert completed.returncode != 0
    assert "receipt_gate_status=PASS" not in completed.stdout
    assert (
        "Pinned conflict-resolution receipt source validation is required." in combined
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
        "Product $removalRoot '--remove-current-user-setup'",
        "Product $productInstall '--onboard-current-user' @($onboardingArguments + '--defer-canonical-relay-start')",
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
    assert "(Arg $LogicalAppRoot)" in source
    assert "Register-ScheduledTask" in source
    assert "Start-ScheduledTask" not in source
    assert "Stop-ScheduledTask" not in source
    assert "schtasks /run" not in source.lower()
    assert source.index("Product $removalRoot '--remove-current-user-setup'") < source.index(
        "$placementExitCode = InvokeFrozenPlacementHelper $frozenPlacement $helperParameters"
    )
    assert source.index("ReceiptSource $source $sourceManifest") < source.index(
        "$placementExitCode = InvokeFrozenPlacementHelper $frozenPlacement $helperParameters"
    )


def test_rollback_is_fail_closed_and_persists_explicit_failure() -> None:
    source = _source()
    start = source.index("function Invoke-LabelCanonicalRollback")
    end = source.index("\nfunction ", start + 1)
    rollback = source[start:end] + source[source.index("    $original = $_"): ]

    assert "try { Product $install '--remove-current-user-setup' } catch {}" not in rollback
    product = rollback.index("Product $rollbackProductRoot '--remove-current-user-setup'")
    zero_readback = rollback.index(
        "Assert-RollbackRelayPreimage -ExpectedRelays @()"
    )
    restore = rollback.index("Restore $before")
    restore_task = rollback.index("RestoreScheduledTask $taskBefore")
    exact_readback = rollback.index(
        "Assert-RollbackRelayPreimage -ExpectedRelays $old"
    )
    restored_true = rollback.index("$audit.rollback.runtime_restored = $true")
    assert product < zero_readback < restore < restore_task < exact_readback < restored_true
    for token in (
        "$audit.status = if ($legacyFirstRegistration) { 'FAILED_ROLLBACK_INCOMPLETE' } else { 'ROLLBACK_FAILED' }",
        "$audit.rollback.runtime_restored = $false",
        "$audit.rollback.failure_type = $rollbackFailure.Exception.GetType().Name",
        "ROLLBACK_AUDIT_PERSISTENCE_FAILED",
        "AUTOSTART_ROLLBACK_FAILED",
    ):
        assert token in rollback


@pytest.mark.parametrize("status", ["FAILED_ROLLBACK_INCOMPLETE", "ROLLBACK_FAILED"])
def test_rollback_status_contract_refuses_changed_branch(monkeypatch, status):
    source = _source()
    assignment = "$audit.status = if ($legacyFirstRegistration) { 'FAILED_ROLLBACK_INCOMPLETE' } else { 'ROLLBACK_FAILED' }"
    assert source.count(assignment) == 1
    changed = source.replace(assignment, assignment.replace("'" + status + "'", "'PASS'"), 1)
    monkeypatch.setattr(__import__(__name__, fromlist=["_source"]), "_source", lambda: changed)
    with pytest.raises(AssertionError):
        test_rollback_is_fail_closed_and_persists_explicit_failure()


@pytest.mark.parametrize(
    ("installer", "source_name", "destination_name"),
    [
        (HELPER, "sourceRootFull", "stagingRoot"),
        (INSTALLER, "replacementRollbackRoot", "rollbackSource"),
    ],
)
def test_robocopy_copy_accepts_success_variants_and_rejects_failure(
    tmp_path: Path, installer: Path, source_name: str, destination_name: str
) -> None:
    source_text = _source(installer)
    start = source_text.index(
        "$robocopy = Join-Path ([Environment]::SystemDirectory) 'robocopy.exe'"
    )
    copy_block = "\n".join(source_text[start:].splitlines()[:3])
    exit_guard = copy_block.splitlines()[2]
    boundary_harness = tmp_path / "robocopy-exit-guard.ps1"
    boundary_harness.write_text(
        "$results = @()\nforeach ($code in 0..8) {\n"
        "    $global:LASTEXITCODE = $code\n"
        f"    try {{ {exit_guard}; $results += \"PASS:$code\" }} "
        "catch { $results += \"FAIL:$code\" }\n}\n"
        "$results -join ','\n",
        encoding="utf-8-sig",
    )
    boundary = subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-File", str(boundary_harness)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert boundary.returncode == 0, boundary.stderr
    assert boundary.stdout.strip() == ",".join(
        [f"PASS:{code}" for code in range(8)] + ["FAIL:8"]
    )
    for scenario, expected_code in (
        ("unchanged", 0),
        ("copied", 1),
        ("extra", 2),
        ("copied-extra", 3),
        ("missing-source", 16),
    ):
        case = tmp_path / scenario
        source = case / "source"
        destination = case / "destination"
        destination.mkdir(parents=True)
        if scenario != "missing-source":
            source.mkdir()
            (source / "bootstrap-integrity.json").write_text("exclude", encoding="utf-8")
        if scenario in {"copied", "copied-extra"}:
            (source / "file.txt").write_text("copy", encoding="utf-8")
        if scenario in {"extra", "copied-extra"}:
            (destination / "extra.txt").write_text("retain", encoding="utf-8")
        qsource = str(source).replace("'", "''")
        qdestination = str(destination).replace("'", "''")
        harness = case / "copy.ps1"
        harness.write_text(
            f"$ErrorActionPreference = 'Stop'\n"
            f"${source_name} = '{qsource}'\n"
            f"${destination_name} = '{qdestination}'\n"
            "$IntegrityFileName = 'bootstrap-integrity.json'\n"
            f"try {{\n{copy_block}\nWrite-Output \"PASS:$LASTEXITCODE\"\n}} "
            "catch { Write-Output \"FAIL:$LASTEXITCODE\" }\n",
            encoding="utf-8-sig",
        )
        completed = subprocess.run(
            ["powershell.exe", "-NoProfile", "-NonInteractive", "-File", str(harness)],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert completed.returncode == 0, completed.stderr
        expected = "FAIL" if expected_code >= 8 else "PASS"
        assert completed.stdout.strip() == f"{expected}:{expected_code}", completed
        if expected == "PASS":
            assert not (destination / "bootstrap-integrity.json").exists()
            assert (destination / "file.txt").exists() == (
                scenario in {"copied", "copied-extra"}
            )


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
        "WriterFenceFunctionsPreloaded",
        "WriterFenceControlRoot",
        "WriterFenceSessionId",
        "WriterFenceAttemptId",
        "WriterFenceReplacementTransactionId",
        "WriterFenceDelegationToken",
        "ReplaceExistingVerifiedPortable",
    ):
        parameter_block = source[
            source.index("param(") : source.index(")", source.index("param("))
        ]
        assert re.search(rf"\${name}\b", parameter_block)
    for token in (
        "tools\\bootstrap_integrity.ps1",
        "Preloaded writer fence helper is incomplete.",
        ".current.rollback.",
        "REPLACED_VERIFIED",
        "replacement_rollback_status=PRESERVED",
        "Write-ElevationLog",
        "Portable source inventory differs from its trusted caller pins.",
        "Bootstrap script SHA-256 differs from its trusted caller pin.",
    ):
        assert token in source
    assert "Enter-LabelWriterDelegatedOperation" in source
    assert INTEGRITY_HELPER.is_file()


def _freeze_helper_functions() -> str:
    source = _source()
    loader = source[source.index("function Get-LabelSharedPortableLeafPath"):source.index("function Full(")]
    sha_start = source.index("function Sha([string]$Path) {")
    sha_end = source.index("function UInt64BE([uint64]$Value)")
    freeze_start = source.index("function FreezePlacementHelper(")
    freeze_end = source.index("\nfunction InvokeFrozenIntegrityProbe")
    return loader + source[sha_start:sha_end] + source[freeze_start:freeze_end]


def _run_freeze_placement_helper_harness(tmp_path: Path) -> dict[str, object]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    source_root = tmp_path / "source"
    tools = source_root / "tools"
    tools.mkdir(parents=True)
    helpers = {
        "INSTALL_THIS_PC.ps1": HELPER,
        "tools/bootstrap_integrity.ps1": INTEGRITY_HELPER,
        "tools/label_writer_fence.ps1": WRITER_FENCE_HELPER,
    }
    hashes: dict[str, str] = {}
    for relative, data in shared_bootstrap_files().items():
        target = source_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
    for relative, original in helpers.items():
        target = source_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        data = original.read_bytes()
        target.write_bytes(data)
        hashes[relative] = hashlib.sha256(data).hexdigest()

    audit_root = tmp_path / "audit"
    audit_root.mkdir()
    harness = tmp_path / "freeze-placement-helper.ps1"
    harness.write_text(
        rf"""
$ErrorActionPreference = 'Stop'
{_freeze_helper_functions()}
$source = '{str(source_root).replace("'", "''")}'
. (Get-LabelSharedPortableLeafPath $source)
$audit = '{str(audit_root).replace("'", "''")}'
$expected = [pscustomobject]@{{
    placement_helper = '{hashes["INSTALL_THIS_PC.ps1"]}'
    bootstrap_integrity_helper = '{hashes["tools/bootstrap_integrity.ps1"]}'
    writer_fence_helper = '{hashes["tools/label_writer_fence.ps1"]}'
}}
function Set-Acl {{
    param([string]$LiteralPath, $AclObject)
    if ($AclObject -is [Security.AccessControl.DirectorySecurity]) {{
        [IO.Directory]::SetAccessControl($LiteralPath, $AclObject)
        return
    }}
    if ($AclObject -is [Security.AccessControl.FileSecurity]) {{
        [IO.File]::SetAccessControl($LiteralPath, $AclObject)
        return
    }}
    throw 'Unsupported ACL object.'
}}
$userSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$writeData = [int][Security.AccessControl.FileSystemRights]::WriteData
function Inspect([string]$Path) {{
    $acl = [IO.File]::GetAccessControl($Path)
    $denyWrite = $false
    $access = @()
    $rules = $acl.GetAccessRules(
        $true,
        $true,
        [type][Security.Principal.SecurityIdentifier]
    )
    foreach ($rule in @($rules)) {{
        $sid = [string]$rule.IdentityReference.Value
        $access += [ordered]@{{
            sid = $sid
            rights = [string]$rule.FileSystemRights
            type = [string]$rule.AccessControlType
            inherited = [bool]$rule.IsInherited
        }}
        if (
            $sid -ceq $userSid -and
            $rule.AccessControlType -eq [Security.AccessControl.AccessControlType]::Deny -and
            (([int]$rule.FileSystemRights) -band $writeData) -eq $writeData
        ) {{ $denyWrite = $true }}
    }}
    $probe = $false
    try {{
        $stream = [IO.File]::Open(
            $Path,
            [IO.FileMode]::Open,
            [IO.FileAccess]::Write,
            [IO.FileShare]::Read
        )
        $stream.Dispose()
        $probe = $true
    }} catch [UnauthorizedAccessException] {{}}
    return [ordered]@{{
        path = $Path
        write_probe_succeeded = $probe
        has_current_user_deny_write = $denyWrite
        access = $access
    }}
}}
$status = 'OK'
$writable = $null
$paths = @()
try {{
    $frozen = FreezePlacementHelper $source $audit 'repro-freeze' $expected
    $writable = [bool]$frozen.current_user_writable
    $paths = @(
        [string]$frozen.helper_path,
        (Join-Path ([string]$frozen.root) 'tools\bootstrap_integrity.ps1'),
        [string]$frozen.writer_fence_path
        (Join-Path ([string]$frozen.root) 'kmtech_shared.lock.json')
        (Join-Path ([string]$frozen.root) 'kmtech_shared.manifest.json')
        (Join-Path ([string]$frozen.root) 'kmtech_shared\powershell\portable.ps1')
    )
}}
catch {{
    $status = [string]$_.Exception.Message
    $paths = @(Get-ChildItem -LiteralPath $audit -Recurse -File |
        ForEach-Object {{ $_.FullName }})
}}
$observation = [pscustomobject][ordered]@{{
    status = $status
    current_user_writable = $writable
    files = @($paths | ForEach-Object {{ Inspect $_ }})
}}
$frozenRoot = Join-Path $audit 'canonical-portable-repro-freeze-helper'
if (Test-Path -LiteralPath $frozenRoot) {{
    foreach ($file in @(Get-ChildItem -LiteralPath $frozenRoot -File -Force -Recurse)) {{
        $fileAcl = [IO.File]::GetAccessControl($file.FullName)
        foreach ($rule in @($fileAcl.GetAccessRules($true, $false, [type][Security.Principal.SecurityIdentifier]))) {{
            if ([string]$rule.IdentityReference.Value -ceq $userSid -and
                $rule.AccessControlType -eq [Security.AccessControl.AccessControlType]::Deny) {{
                [void]$fileAcl.RemoveAccessRuleSpecific($rule)
            }}
        }}
        [IO.File]::SetAccessControl($file.FullName, $fileAcl)
    }}
    $rootAcl = [IO.Directory]::GetAccessControl($frozenRoot)
    foreach ($rule in @($rootAcl.GetAccessRules($true, $false, [type][Security.Principal.SecurityIdentifier]))) {{
        if ([string]$rule.IdentityReference.Value -ceq $userSid -and
            $rule.AccessControlType -eq [Security.AccessControl.AccessControlType]::Deny) {{
            [void]$rootAcl.RemoveAccessRuleSpecific($rule)
        }}
    }}
    $rootAcl.SetAccessRuleProtection($false, $false)
    [IO.Directory]::SetAccessControl($frozenRoot, $rootAcl)
}}
$observation | ConvertTo-Json -Depth 6 -Compress
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
        encoding="oem",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return json.loads(completed.stdout)


def test_frozen_placement_helper_denies_current_user_write(tmp_path: Path) -> None:
    result = _run_freeze_placement_helper_harness(tmp_path)
    assert result["status"] == "OK", result
    assert result["current_user_writable"] is False, result
    files = result["files"]
    assert len(files) == 6, result
    for frozen in files:
        assert frozen["write_probe_succeeded"] is False, frozen
        assert frozen["has_current_user_deny_write"] is True, frozen


def _product_functions() -> str:
    source = _source()
    start = source.index("function Arg([string]$Value) {")
    end = source.index("\n$WriterDelegationEnvironmentNames = @(")
    return f". '{(ROOT / 'tools/failure_redaction.ps1').as_posix()}'\n" + source[start:end]


def _run_product_stderr_harness(tmp_path: Path) -> str:
    tmp_path.mkdir(parents=True, exist_ok=True)
    root = tmp_path / "portable"
    runtime = root / "runtime"
    app = root / "app"
    runtime.mkdir(parents=True)
    app.mkdir(parents=True)
    (app / "main.py").write_text(
        "raise SystemExit('stub pythonw.exe must run instead')\n",
        encoding="utf-8",
        newline="\n",
    )
    token = "repro-delegation-token-value-32ok"
    pythonw = runtime / "pythonw.exe"
    harness = tmp_path / "product-stderr.ps1"
    harness.write_text(
        rf"""
$ErrorActionPreference = 'Stop'
{_product_functions()}
Add-Type -OutputAssembly '{str(pythonw).replace("'", "''")}' -OutputType ConsoleApplication -TypeDefinition @'
using System;
public class ProductStub {{
  public static int Main(string[] args) {{
    Console.Error.WriteLine("token=" + (Environment.GetEnvironmentVariable("KMTECH_LABEL_WRITER_DELEGATION_TOKEN") ?? ""));
    return 3;
  }}
}}
'@
[Environment]::SetEnvironmentVariable(
    'KMTECH_LABEL_WRITER_DELEGATION_TOKEN',
    '{token}',
    'Process'
)
try {{
    Product '{str(root).replace("'", "''")}' '--onboard-current-user'
    Write-Output 'product_status=UNEXPECTED_SUCCESS'
    exit 0
}}
catch {{
    Write-Output ('product_error=' + [string]$_.Exception.Message)
    exit 0
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
        encoding="oem",
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return completed.stdout


def test_product_failure_includes_child_stderr_and_delegation_env(
    tmp_path: Path,
) -> None:
    output = _run_product_stderr_harness(tmp_path)
    assert "product_error=Product mode failed: --onboard-current-user/3" in output
    assert "repro-delegation-token-value-32ok" not in output
    assert "token=[REDACTED:KMTECH_LABEL_WRITER_DELEGATION_TOKEN]" in output


def test_portable_main_imports_label_match_under_isolated_python(
    tmp_path: Path,
) -> None:
    app = tmp_path / "app"
    app.mkdir()
    (app / "main.py").write_bytes((ROOT / "portable" / "main.py").read_bytes())
    for name in ("auto_update.py", "writer_session_fence.py"):
        (app / name).write_bytes((ROOT / name).read_bytes())
    (app / "Label_Match.py").write_text(
        "def main() -> int:\n    return 0\n",
        encoding="utf-8",
        newline="\n",
    )
    completed = subprocess.run(
        [
            sys.executable,
            "-I",
            "-B",
            str(app / "main.py"),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
        cwd=tmp_path,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout


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
        "[string]$frozenPlacement.writer_fence_path",
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
        r"\$launcher = @'\r?\n(?P<body>.*?)\r?\n'@",
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
    [switch]$WriterFenceFunctionsPreloaded,
    [string]$WriterFenceControlRoot,
    [string]$DataGenerationSubjectSid,
    [string[]]$DataGenerationMarkerPaths,
    [string]$WriterFenceSessionId,
    [string]$WriterFenceAttemptId,
    [string]$WriterFenceReplacementTransactionId,
    [string]$WriterFenceDelegationToken,
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
    writer_fence_functions_preloaded = $WriterFenceFunctionsPreloaded.IsPresent
    writer_fence_control_root = $WriterFenceControlRoot
    writer_fence_session_id = $WriterFenceSessionId
    writer_fence_attempt_id = $WriterFenceAttemptId
    writer_fence_replacement_transaction_id = $WriterFenceReplacementTransactionId
    writer_fence_delegation_token = $WriterFenceDelegationToken
    allow_noncanonical_layout_for_test = $AllowNoncanonicalLayoutForTest.IsPresent
    replace_existing_verified_portable = $ReplaceExistingVerifiedPortable.IsPresent
    dry_run = $DryRun.IsPresent
} | ConvertTo-Json -Compress
""",
        encoding="utf-8",
    )
    integrity.write_bytes(b"# verified integrity helper\n")
    writer_fence = tmp_path / "writer-fence.ps1"
    writer_fence.write_bytes(WRITER_FENCE_HELPER.read_bytes())
    helper_sha256 = hashlib.sha256(helper.read_bytes()).hexdigest()
    integrity_sha256 = hashlib.sha256(integrity.read_bytes()).hexdigest()
    writer_fence_sha256 = hashlib.sha256(writer_fence.read_bytes()).hexdigest()
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
        "WriterFenceFunctionsPreloaded": True,
        "WriterFenceControlRoot": str(tmp_path / "writer-control"),
        "DataGenerationSubjectSid": "S-1-5-21-1-2-3-1001",
        "DataGenerationMarkerPaths": [str(tmp_path / "data-generation.json")],
        "WriterFenceSessionId": "1" * 32,
        "WriterFenceAttemptId": "2" * 32,
        "WriterFenceReplacementTransactionId": "3" * 32,
        "WriterFenceDelegationToken": "4" * 64,
        "AllowNoncanonicalLayoutForTest": True,
        "ReplaceExistingVerifiedPortable": False,
        "DryRun": True,
    }
    payload = {
        "helper_path": str(helper),
        "helper_sha256": helper_sha256,
        "integrity_path": str(integrity),
        "integrity_sha256": integrity_sha256,
        "writer_fence_path": str(writer_fence),
        "writer_fence_sha256": writer_fence_sha256,
        "data_generation_path": str(ROOT / "kmtech_shared/powershell/data_generation.ps1"),
        "data_generation_sha256": hashlib.sha256((ROOT / "kmtech_shared/powershell/data_generation.ps1").read_bytes()).hexdigest(),
        "parameters": parameters,
    }
    launcher = match.group("body")
    encoded_launcher = transport_command(tmp_path, launcher, payload)
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
        encoding="oem",
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
        "writer_fence_functions_preloaded": True,
        "writer_fence_control_root": str(tmp_path / "writer-control"),
        "writer_fence_session_id": "1" * 32,
        "writer_fence_attempt_id": "2" * 32,
        "writer_fence_replacement_transaction_id": "3" * 32,
        "writer_fence_delegation_token": "4" * 64,
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
        r"\$launcher = @'\r?\n(?P<body>.*?)\r?\n'@",
        source,
        re.DOTALL,
    )
    assert match is not None

    helper = tmp_path / "must-not-run.ps1"
    integrity = tmp_path / "integrity.ps1"
    helper.write_text("throw 'HELPER_MUST_NOT_RUN'\n", encoding="utf-8")
    integrity.write_bytes(b"# verified integrity helper\n")
    writer_fence = tmp_path / "writer-fence.ps1"
    writer_fence.write_bytes(WRITER_FENCE_HELPER.read_bytes())
    helper_sha256 = hashlib.sha256(helper.read_bytes()).hexdigest()
    integrity_sha256 = hashlib.sha256(integrity.read_bytes()).hexdigest()
    writer_fence_sha256 = hashlib.sha256(writer_fence.read_bytes()).hexdigest()
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
        "WriterFenceFunctionsPreloaded": True,
        "WriterFenceControlRoot": str(tmp_path / "writer-control"),
        "DataGenerationSubjectSid": "S-1-5-21-1-2-3-1001",
        "DataGenerationMarkerPaths": [str(tmp_path / "data-generation.json")],
        "WriterFenceSessionId": "1" * 32,
        "WriterFenceAttemptId": "2" * 32,
        "WriterFenceReplacementTransactionId": "3" * 32,
        "WriterFenceDelegationToken": "4" * 64,
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
        "writer_fence_path": str(writer_fence),
        "writer_fence_sha256": writer_fence_sha256,
        "data_generation_path": str(ROOT / "kmtech_shared/powershell/data_generation.ps1"),
        "data_generation_sha256": hashlib.sha256((ROOT / "kmtech_shared/powershell/data_generation.ps1").read_bytes()).hexdigest(),
        "parameters": parameters,
    }
    launcher = match.group("body")
    encoded_launcher = transport_command(tmp_path, launcher, payload)
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
        encoding="oem",
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
        r"\$launcher = @'\r?\n(?P<body>.*?)\r?\n'@",
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
        "tools/label_writer_fence.ps1": WRITER_FENCE_HELPER.read_bytes(),
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
    writer_fence_sha256 = hashlib.sha256(WRITER_FENCE_HELPER.read_bytes()).hexdigest()
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
        "WriterFenceFunctionsPreloaded": True,
        "WriterFenceControlRoot": str(tmp_path / "writer-control"),
        "DataGenerationSubjectSid": "S-1-5-21-1-2-3-1001",
        "DataGenerationMarkerPaths": [str(tmp_path / "data-generation.json")],
        "WriterFenceSessionId": "1" * 32,
        "WriterFenceAttemptId": "2" * 32,
        "WriterFenceReplacementTransactionId": "3" * 32,
        "WriterFenceDelegationToken": "4" * 64,
        "AllowNoncanonicalLayoutForTest": True,
        "ReplaceExistingVerifiedPortable": False,
        "DryRun": True,
    }
    payload = {
        "helper_path": str(HELPER),
        "helper_sha256": helper_sha256,
        "integrity_path": str(INTEGRITY_HELPER),
        "integrity_sha256": integrity_sha256,
        "writer_fence_path": str(WRITER_FENCE_HELPER),
        "writer_fence_sha256": writer_fence_sha256,
        "data_generation_path": str(ROOT / "kmtech_shared/powershell/data_generation.ps1"),
        "data_generation_sha256": hashlib.sha256((ROOT / "kmtech_shared/powershell/data_generation.ps1").read_bytes()).hexdigest(),
        "parameters": parameters,
    }
    launcher = match.group("body")
    encoded_launcher = transport_command(tmp_path, launcher, payload)
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
        encoding="oem",
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
    assert portable_builder.WRITER_FENCE_HELPER.as_posix() == (
        "tools/label_writer_fence.ps1"
    )
    assert portable_builder.WRITER_FENCE_CONTRACT.as_posix() == (
        "tools/label_writer_fence_contract.json"
    )
    assert WRITER_FENCE_CONTRACT.is_file()
    assert "canonical_installer_sha256" in source
    assert "runtime_python_sha256" in source
    assert "shutil.copy2(installer_source" in source
    assert "shutil.copy2(legacy_installer_source" in source
    assert "shutil.copy2(bootstrap_helper_source" in source
    assert "shutil.copy2(writer_fence_source" in source
    assert "writer_fence_contract_source" in source


def test_plan_only_contract_is_stdout_only_and_non_mutating() -> None:
    source = _source()
    plan_start = source.index("if ($PlanOnly)")
    plan_block = source[plan_start : source.index("$runId =", plan_start)]

    assert "install_status=PLAN_ONLY" in plan_block
    assert "registry_changed=false" in plan_block
    assert "Save " not in plan_block
    assert "Start-Process" not in plan_block
    assert "INSTALL_THIS_PC.ps1" not in plan_block


SERVER_BASE_URL = "https://isolated.example.invalid:8443"
INVALID_SERVER_BASE_URLS = (
    "http://isolated.example.invalid",
    "ftp://isolated.example.invalid",
    "isolated.example.invalid",
    "https://",
    "https://user:secret@isolated.example.invalid",
    "https://isolated.example.invalid/api/producer-ingest",
    "https://isolated.example.invalid?scope=1",
    "https://isolated.example.invalid#fragment",
    "https://isolated.example.invalid:0",
    "https://isolated.example.invalid:70000",
    "https://-isolated.example.invalid",
    "https://isolated.example.invalid --app-root C:\\evil",
    'https://isolated.example.invalid"',
    "https://isolated.example.invalid\\path",
    "https://isolated.example.invalid//",
)


def _run_windows_powershell_file(
    harness: Path, *, timeout: int = 60
) -> subprocess.CompletedProcess[str]:
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
        encoding="oem",
        timeout=timeout,
    )


def _run_server_base_url_origin_harness(
    tmp_path: Path, values: list[str]
) -> list[dict[str, object]]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    payload = base64.b64encode(json.dumps(values).encode("utf-8")).decode("ascii")
    harness = tmp_path / "server-base-url-origin.ps1"
    harness.write_text(
        f"""
$ErrorActionPreference = 'Stop'
{_product_functions()}
$json = (New-Object Text.UTF8Encoding($false, $true)).GetString(
    [Convert]::FromBase64String('{payload}')
)
$values = $json | ConvertFrom-Json
$results = @()
foreach ($value in $values) {{
    $row = [ordered]@{{
        value = [string]$value
        status = ''
        origin = ''
        arguments = @()
        error = ''
    }}
    try {{
        $origin = ServerBaseUrlOrigin ([string]$value)
        $row.status = 'PASS'
        $row.origin = $origin
        $row.arguments = [string[]](OnboardingArguments $origin)
    }}
    catch {{
        $row.status = 'REJECTED'
        $row.error = [string]$_.Exception.Message
    }}
    $results += [pscustomobject]$row
}}
ConvertTo-Json -InputObject $results -Depth 4 -Compress
""",
        encoding="utf-8-sig",
    )
    completed = _run_windows_powershell_file(harness)
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return json.loads(completed.stdout)


def _run_onboarding_product_argv_harness(
    tmp_path: Path, server_base_url: str
) -> tuple[Path, list[str], str]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    root = tmp_path / "portable"
    runtime = root / "runtime"
    app = root / "app"
    runtime.mkdir(parents=True)
    app.mkdir(parents=True)
    (app / "main.py").write_text(
        "raise SystemExit('stub pythonw.exe must run instead')\n",
        encoding="utf-8",
        newline="\n",
    )
    argv_path = tmp_path / "product-argv.txt"
    pythonw = runtime / "pythonw.exe"
    harness = tmp_path / "product-argv.ps1"
    harness.write_text(
        rf"""
$ErrorActionPreference = 'Stop'
{_product_functions()}
Add-Type -OutputAssembly '{str(pythonw).replace("'", "''")}' -OutputType ConsoleApplication -TypeDefinition @'
using System;
using System.IO;
public class ProductArgvStub {{
  public static int Main(string[] args) {{
    File.WriteAllLines(Environment.GetEnvironmentVariable("LABEL_PRODUCT_ARGV_PATH"), args);
    return 0;
  }}
}}
'@
[Environment]::SetEnvironmentVariable(
    'LABEL_PRODUCT_ARGV_PATH',
    '{str(argv_path).replace("'", "''")}',
    'Process'
)
$origin = ServerBaseUrlOrigin '{server_base_url.replace("'", "''")}'
$onboardingArguments = OnboardingArguments $origin
Product '{str(root).replace("'", "''")}' '--onboard-current-user' $onboardingArguments
Write-Output ('origin=' + $origin)
Write-Output 'product_status=PASS'
""",
        encoding="utf-8-sig",
    )
    completed = _run_windows_powershell_file(harness)
    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert "product_status=PASS" in completed.stdout, completed.stdout
    argv = argv_path.read_text(encoding="utf-8-sig").splitlines()
    return root, argv, completed.stdout


def test_server_base_url_origin_accepts_only_credential_free_https_origins(
    tmp_path: Path,
) -> None:
    values = [
        "",
        SERVER_BASE_URL,
        "https://isolated.example.invalid/",
        "https://isolated.example.invalid",
        *INVALID_SERVER_BASE_URLS,
    ]
    results = {
        str(row["value"]): row
        for row in _run_server_base_url_origin_harness(tmp_path, values)
    }

    assert len(results) == len(values)
    assert results[""] == {
        "value": "",
        "status": "PASS",
        "origin": "",
        "arguments": [],
        "error": "",
    }
    assert results[SERVER_BASE_URL]["status"] == "PASS"
    assert results[SERVER_BASE_URL]["origin"] == SERVER_BASE_URL
    assert results[SERVER_BASE_URL]["arguments"] == ["--server-base-url", SERVER_BASE_URL]
    assert results["https://isolated.example.invalid/"]["origin"] == (
        "https://isolated.example.invalid"
    )
    assert results["https://isolated.example.invalid"]["arguments"] == [
        "--server-base-url",
        "https://isolated.example.invalid",
    ]
    for value in INVALID_SERVER_BASE_URLS:
        row = results[value]
        assert row["status"] == "REJECTED", row
        assert str(row["error"]).startswith("ServerBaseUrl "), row
        assert row["origin"] == "", row
        assert row["arguments"] == [], row


def test_onboarding_product_call_forwards_explicit_server_base_url(
    tmp_path: Path,
) -> None:
    root, argv, stdout = _run_onboarding_product_argv_harness(
        tmp_path, SERVER_BASE_URL + "/"
    )

    assert f"origin={SERVER_BASE_URL}" in stdout
    assert argv == [
        "-I",
        "-B",
        str(root / "app" / "main.py"),
        "--onboard-current-user",
        "--app-root",
        str(root),
        "--server-base-url",
        SERVER_BASE_URL,
    ]


def test_onboarding_product_call_omits_server_base_url_when_not_supplied(
    tmp_path: Path,
) -> None:
    root, argv, stdout = _run_onboarding_product_argv_harness(tmp_path, "")

    assert "origin=\n" in stdout.replace("\r\n", "\n")
    assert argv == [
        "-I",
        "-B",
        str(root / "app" / "main.py"),
        "--onboard-current-user",
        "--app-root",
        str(root),
    ]
    assert "--server-base-url" not in argv


def test_invalid_server_base_url_fails_closed_before_plan_or_side_effects(
    tmp_path: Path,
) -> None:
    scenarios = {
        "install": ["-ServerBaseUrl", "http://isolated.example.invalid"],
        "plan-only": [
            "-PlanOnly",
            "-ServerBaseUrl",
            "https://isolated.example.invalid/api/producer-ingest",
        ],
    }
    for name, extra_arguments in scenarios.items():
        completed = _run_clean_install_receipt_gate_harness(
            tmp_path / name, extra_arguments=extra_arguments
        )
        combined = completed.stdout + completed.stderr

        assert completed.returncode != 0, combined
        assert (
            "ServerBaseUrl must be a credential-free https://host[:port] origin."
            in combined
        ), combined
        assert "install_status=" not in completed.stdout, combined
        assert "receipt_gate_status=PASS" not in completed.stdout, combined
        assert not (tmp_path / name / "local-app-data").exists()
        assert not (tmp_path / name / "canonical").exists()


def test_plan_only_reports_explicit_or_product_default_server_base_url(
    tmp_path: Path,
) -> None:
    explicit = _run_clean_install_receipt_gate_harness(
        tmp_path / "explicit",
        extra_arguments=["-PlanOnly", "-ServerBaseUrl", SERVER_BASE_URL + "/"],
    )
    explicit_stdout = explicit.stdout.replace("\r\n", "\n")
    assert explicit.returncode == 0, explicit.stderr or explicit.stdout
    assert "install_status=PLAN_ONLY\n" in explicit_stdout
    assert "onboarding_server_base_url_source=explicit\n" in explicit_stdout
    assert f"onboarding_server_base_url={SERVER_BASE_URL}\n" in explicit_stdout
    assert "registry_changed=false\n" in explicit_stdout

    default = _run_clean_install_receipt_gate_harness(
        tmp_path / "default", extra_arguments=["-PlanOnly"]
    )
    default_stdout = default.stdout.replace("\r\n", "\n")
    assert default.returncode == 0, default.stderr or default.stdout
    assert "install_status=PLAN_ONLY\n" in default_stdout
    assert "onboarding_server_base_url_source=product_default\n" in default_stdout
    assert "onboarding_server_base_url=" not in default_stdout
    for name in ("explicit", "default"):
        assert not (tmp_path / name / "local-app-data").exists()
        assert not (tmp_path / name / "canonical").exists()


def test_server_base_url_is_validated_first_and_reaches_only_onboarding() -> None:
    source = _source()
    parameter_block = source[
        source.index("param(") : source.index(")", source.index("param("))
    ]

    assert re.search(r"\$ServerBaseUrl\b", parameter_block)
    validation = source.index("$serverBaseUrl = ServerBaseUrlOrigin $ServerBaseUrl")
    assert validation < source.index("$sourceManifest = Manifest $source")
    assert validation < source.index("if ($PlanOnly)")
    assert validation < source.index("$before = Snapshot")
    assert source.count("$onboardingArguments") == 2
    assert "Product $productInstall '--onboard-current-user' @($onboardingArguments + '--defer-canonical-relay-start')" in source
    onboarding = source.index("try { Product $productInstall '--onboard-current-user' @($onboardingArguments + '--defer-canonical-relay-start') }")
    assert source.index("$placementExitCode = InvokeFrozenPlacementHelper $frozenPlacement $helperParameters") < onboarding < source.index("$after = Snapshot", onboarding)
    assert "'--remove-current-user-setup' $onboardingArguments" not in source
    assert "onboarding_server_base_url = [ordered]@{" in source
    launcher_parameters = source[
        source.index("$parameterNames = @(") : source.index("$actualParameterNames")
    ]
    assert "ServerBaseUrl" not in launcher_parameters
    assert "ServerBaseUrl" not in _source(HELPER)


@pytest.mark.parametrize("damage", ("old-marker", "old-onboarding", "onboarding-before-placement"))
def test_integrated_lifecycle_static_contract_refuses_previous_shape(monkeypatch, damage):
    source = _source()
    onboarding = "try { Product $productInstall '--onboard-current-user' @($onboardingArguments + '--defer-canonical-relay-start') }"
    if damage == "old-marker":
        source = source.replace("REUSED_VERIFIED", "REUSED")
    elif damage == "old-onboarding":
        source = source.replace(onboarding, "try { Product $install '--onboard-current-user' $onboardingArguments }")
    else:
        assert source.count(onboarding) == 1
        source = source.replace(onboarding, "")
        placement = "$placementExitCode = InvokeFrozenPlacementHelper $frozenPlacement $helperParameters"
        source = source.replace(placement, onboarding + "\n        " + placement, 1)
    helper = _source(HELPER)
    monkeypatch.setattr(__name__ + "._source", lambda path=INSTALLER: source if path == INSTALLER else helper)
    with pytest.raises(AssertionError):
        if damage == "old-marker":
            test_top_level_installer_owns_current_user_lifecycle_and_preimage()
        else:
            test_server_base_url_is_validated_first_and_reaches_only_onboarding()


@pytest.mark.parametrize("engine", ("powershell.exe", "pwsh.exe"))
@pytest.mark.parametrize("scenario", (
    "public-ca", "private-key", "identity", "credential", "extra-file",
    "empty-directory", "invalid-certificate", "receipt", "credential-only", "absent-ca",
))
def test_pristine_public_bootstrap_ca_preserves_other_residue(tmp_path, engine, scenario):
    from tests.test_logistics_runtime_profile import _private_ca_pem
    from tests.test_shared_ancestor_protection import run_functions

    ca = tmp_path / "bootstrap" / "ca-bundle.pem"
    if scenario not in {"credential-only", "absent-ca"}:
        ca.parent.mkdir()
    if scenario not in {"empty-directory", "credential-only", "absent-ca"}:
        content = _private_ca_pem()
        if scenario == "private-key":
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.primitives.asymmetric import ec
            content += ec.generate_private_key(ec.SECP256R1()).private_bytes(
                serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        elif scenario == "invalid-certificate":
            content = b"-----BEGIN CERTIFICATE-----\nYWJj\n-----END CERTIFICATE-----\n"
        ca.write_bytes(content)
    identity = tmp_path / "direct-sync" / "producer_identity.json"
    credential = tmp_path / "direct-sync" / "credential.json"
    receipt = tmp_path / "install-audit" / "receipt.json"
    if scenario in {"identity", "credential", "credential-only", "receipt", "extra-file"}:
        residue = {"identity": identity, "credential": credential, "receipt": receipt,
                   "credential-only": credential, "extra-file": ca.parent / "unexpected.json"}[scenario]
        residue.parent.mkdir(parents=True, exist_ok=True)
        residue.write_text("{}", encoding="utf-8")
    before = {p.relative_to(tmp_path).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
              for p in tmp_path.rglob("*") if p.is_file()}
    result = run_functions(tmp_path, INSTALLER, ["Test-PristineInstallState"], r'''
$ca=$env:LM_PRISTINE_CA
$paths=@((Split-Path -Parent $ca),$ca,$env:LM_PRISTINE_IDENTITY,
 $env:LM_PRISTINE_CREDENTIAL,(Split-Path -Parent $env:LM_PRISTINE_RECEIPT))
$actual=Test-PristineInstallState $env:LM_PRISTINE_INSTALL `
 ([pscustomobject]@{exists=$false}) @() @() $paths `
 $ca $env:LM_PRISTINE_PYTHON $env:LM_PRISTINE_APP
[pscustomobject]@{pristine=[bool]$actual}|ConvertTo-Json -Compress
''', engine=engine, values={
        "LM_PRISTINE_CA": str(ca), "LM_PRISTINE_INSTALL": str(tmp_path / "current"),
        "LM_PRISTINE_IDENTITY": str(identity), "LM_PRISTINE_CREDENTIAL": str(credential),
        "LM_PRISTINE_RECEIPT": str(receipt), "LM_PRISTINE_PYTHON": sys.executable,
        "LM_PRISTINE_APP": str(ROOT),
    })
    assert result.returncode == 0, result.stderr or result.stdout
    assert json.loads(result.stdout)["pristine"] is (scenario in {"public-ca", "absent-ca"})
    assert {name: hashlib.sha256((tmp_path / name).read_bytes()).hexdigest()
            for name in before} == before


@pytest.mark.parametrize("engine", ("powershell.exe", "pwsh.exe"))
@pytest.mark.parametrize("tamper", (False, True))
def test_pinned_bom_integrity_helper_executes_verified_bytes(tmp_path, engine, tamper):
    from tests.test_shared_ancestor_protection import run_functions

    expected = hashlib.sha256(INTEGRITY_HELPER.read_bytes()).hexdigest()
    result = run_functions(tmp_path, INSTALLER, ["ByteSha", "PinnedFileBytes"], r'''
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile($env:LM_CONTRACT_SOURCE,[ref]$tokens,[ref]$errors)
$statement=@($ast.FindAll({param($n)
 $n -is [Management.Automation.Language.PipelineAst] -and
 $n.Extent.Text.StartsWith('. ([ScriptBlock]::Create', [StringComparison]::Ordinal) -and
 $n.Extent.Text.Contains('preparationIntegrityBytes')
},$true))
if($statement.Count -ne 1){throw 'Ordinary verified-byte execution site missing.'}
$preparationIntegrityBytes=PinnedFileBytes $env:LM_BOM_HELPER $env:LM_BOM_SHA
$frozenPlacement=[pscustomobject]@{root=$env:LM_BOM_ROOT}
# Execute the actual product statement, never the installer entrypoint.
. ([ScriptBlock]::Create($statement[0].Extent.Text))
if(-not (Get-Command Assert-BootstrapIntegrityRecord -CommandType Function -ErrorAction SilentlyContinue)) {
 throw 'Actual BOM helper did not define its integrity function.'
}
'EXECUTED'
''', engine=engine, values={
        "LM_BOM_HELPER": str(INTEGRITY_HELPER), "LM_BOM_ROOT": str(ROOT),
        "LM_BOM_SHA": ("0" * 64) if tamper else expected,
    })
    if tamper:
        assert result.returncode != 0
        assert "EXECUTED" not in result.stdout
        assert "pin" in result.stderr.lower()
    else:
        assert result.returncode == 0, result.stdout + result.stderr
        assert "EXECUTED" in result.stdout


@pytest.mark.parametrize("engine", ("powershell.exe", "pwsh.exe"))
@pytest.mark.parametrize("existing", (False, True))
@pytest.mark.parametrize("frozen_acl", (False, True))
def test_pretransaction_failure_preserves_evidence_and_next_install_state(tmp_path, engine, existing, frozen_acl):
    from tests.test_shared_ancestor_protection import run_functions

    result = run_functions(tmp_path, INSTALLER, ["Save", "Archive-PretransactionFailure", "Test-PristineInstallState"], r'''
$auditRoot=Join-Path $env:LM_ARCHIVE_ROOT 'install-audit'
[void][IO.Directory]::CreateDirectory($auditRoot)
$runId='regression-run'
$auditPath=Join-Path $auditRoot ('canonical-portable-'+$runId+'.json')
$audit=[ordered]@{status='FAILED_PRETRANSACTION';code_placement='PENDING';rollback=@{applied=$false}}
Save $auditPath $audit
[IO.File]::WriteAllText(($auditPath+'.tmp.123'),'unique interrupted audit write')
$frozen=Join-Path $auditRoot ('canonical-portable-'+$runId+'-helper')
[void][IO.Directory]::CreateDirectory($frozen)
$helper=Join-Path $frozen 'helper.ps1'
[IO.File]::WriteAllText($helper,'unique original failure evidence')
if($env:LM_ARCHIVE_FROZEN -eq '1'){
 $sid=[Security.Principal.WindowsIdentity]::GetCurrent().User
 $acl=Get-Acl -LiteralPath $frozen
 $rights=[Security.AccessControl.FileSystemRights]::Write -bor [Security.AccessControl.FileSystemRights]::Delete
 $inherit=[Security.AccessControl.InheritanceFlags]::ContainerInherit -bor [Security.AccessControl.InheritanceFlags]::ObjectInherit
 [void]$acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new($sid,$rights,$inherit,[Security.AccessControl.PropagationFlags]::None,[Security.AccessControl.AccessControlType]::Deny))
 Set-Acl -LiteralPath $frozen -AclObject $acl
}
if($env:LM_ARCHIVE_EXISTING -eq '1'){
 [IO.File]::WriteAllText((Join-Path $auditRoot 'existing-receipt.json'),'preserve unrelated receipt')
}
$archive=Archive-PretransactionFailure $auditRoot $runId
if(-not (Test-Path -LiteralPath (Join-Path $archive ('canonical-portable-'+$runId+'.json')))) {throw 'Audit lost.'}
if([IO.File]::ReadAllText((Join-Path $archive ('canonical-portable-'+$runId+'.json.tmp.123'))) -cne 'unique interrupted audit write') {throw 'Interrupted audit bytes lost.'}
if([IO.File]::ReadAllText((Join-Path $archive ('canonical-portable-'+$runId+'-helper\helper.ps1'))) -cne 'unique original failure evidence') {throw 'Helper evidence changed.'}
$pristine=Test-PristineInstallState (Join-Path $env:LM_ARCHIVE_ROOT 'current') ([pscustomobject]@{exists=$false}) @() @() @($auditRoot)
if($pristine -ne ($env:LM_ARCHIVE_EXISTING -ne '1')) {throw 'Next install residue contract differs.'}
if($env:LM_ARCHIVE_EXISTING -eq '1' -and [IO.File]::ReadAllText((Join-Path $auditRoot 'existing-receipt.json')) -cne 'preserve unrelated receipt') {throw 'Existing preimage changed.'}
# Supported removal has no new code, Run, task, relay or registration to undo;
# forensic archive remains a preserved local diagnostic, outside active audit.
[pscustomobject]@{pristine=$pristine;archived=$true}|ConvertTo-Json -Compress
''', engine=engine, values={"LM_ARCHIVE_ROOT": str(tmp_path), "LM_ARCHIVE_EXISTING": "1" if existing else "0", "LM_ARCHIVE_FROZEN": "1" if frozen_acl else "0"})
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["archived"] is True


@pytest.mark.parametrize("engine", ("powershell.exe", "pwsh.exe"))
@pytest.mark.parametrize("phase", ("PIN_VALIDATION", "HELPER_EXECUTION"))
def test_placement_child_preserves_original_failure_without_delegation_secret(tmp_path, engine, phase):
    import shutil

    executable = shutil.which(engine)
    if not executable:
        pytest.skip(f"{engine} is required")
    body = re.search(r"\$launcher = @'\r?\n(.*?)\r?\n'@", _source(), re.DOTALL).group(1)
    integrity = tmp_path / "integrity.ps1"
    writer = tmp_path / "writer.ps1"
    generation = tmp_path / "generation.ps1"
    helper = tmp_path / "helper.ps1"
    failure = tmp_path / "child-failure.json"
    integrity.write_text("param([string]$SharedCodeRoot)\n", encoding="utf-8-sig")
    writer.write_bytes(WRITER_FENCE_HELPER.read_bytes())
    generation.write_bytes((ROOT / "kmtech_shared/powershell/data_generation.ps1").read_bytes())
    parameters = {name: str(tmp_path) for name in (
        "SourceRoot", "InstallRoot", "ElevationLogPath", "ExpectedBootstrapScriptSha256",
        "VerifiedBootstrapScriptPath", "ExpectedSourceAggregateSha256", "WriterFenceControlRoot",
        "WriterFenceSessionId", "WriterFenceAttemptId", "WriterFenceReplacementTransactionId",
        "WriterFenceDelegationToken",
    )}
    parameters.update(ExpectedSourceFileCount=1, ExpectedSourceByteCount=1,
        BootstrapIntegrityPreloaded=True, WriterFenceFunctionsPreloaded=True,
        AllowNoncanonicalLayoutForTest=True, ReplaceExistingVerifiedPortable=False, DryRun=False)
    parameters.update(InstallerRunId='20261004T000000000Z-' + 'a' * 32,
        InstallerOwnerProcessId=os.getpid(), InstallerOwnerProcessStartedAtFileTime='1')
    from protected_admin import current_process_sid
    parameters.update(DataGenerationSubjectSid=current_process_sid(),
        DataGenerationMarkerPaths=[str(tmp_path / "data-generation.json")])
    parameters["WriterFenceDelegationToken"] = "synthetic-private-delegation-" + "x" * 64
    declarations = [f"[{'switch' if isinstance(value, bool) else 'long' if isinstance(value, int) else 'string[]' if isinstance(value, list) else 'string'}]${name}"
                    for name, value in parameters.items()]
    declarations.extend(("[string]$ExpectedActiveCommit", "[string]$StartupUpdateRestorePreimage", "[string]$StartupUpdateRestoreSha256"))
    helper.write_text("param(" + ",".join(declarations) + ")\nthrow ('ORIGINAL_PLACEMENT_FAILURE ' + $WriterFenceDelegationToken)\n", encoding="utf-8-sig")
    payload = dict(helper_path=str(helper), helper_sha256=hashlib.sha256(helper.read_bytes()).hexdigest(),
        integrity_path=str(integrity), integrity_sha256=hashlib.sha256(integrity.read_bytes()).hexdigest(),
        writer_fence_path=str(writer), writer_fence_sha256=hashlib.sha256(writer.read_bytes()).hexdigest(),
        data_generation_path=str(generation), data_generation_sha256=hashlib.sha256(generation.read_bytes()).hexdigest(),
        failure_path=str(failure), parameters=parameters)
    if phase == "PIN_VALIDATION":
        payload["integrity_sha256"] = "0" * 64
    script = tmp_path / "child.ps1"
    script.write_text(base64.b64decode(transport_command(tmp_path, body, payload)).decode("utf-16-le"), encoding="utf-8-sig")
    child = subprocess.run([executable, "-NoLogo", "-NoProfile", "-NonInteractive", "-File", str(script)],
        capture_output=True, encoding="utf-8", creationflags=subprocess.CREATE_NO_WINDOW)
    assert child.returncode != 0
    assert failure.exists(), "Original elevated child failure was not preserved"
    record = json.loads(failure.read_text(encoding="utf-8"))
    assert record["phase"] == phase
    assert record["exit_code"] == child.returncode
    assert record["exception_type"] and record["hresult"] is not None
    expected = "Elevated helper byte pin differs" if phase == "PIN_VALIDATION" else "ORIGINAL_PLACEMENT_FAILURE"
    assert expected in record["message"]
    assert parameters["WriterFenceDelegationToken"] not in failure.read_text(encoding="utf-8")
    assert parameters["WriterFenceDelegationToken"] not in child.stderr


@pytest.mark.parametrize("engine", ("powershell.exe", "pwsh.exe"))
@pytest.mark.parametrize("wrapper", ("probe", "placement"))
def test_pinned_bom_integrity_encoded_wrappers_without_native_launch(tmp_path, engine, wrapper):
    from tests.test_shared_ancestor_protection import run_functions

    result = run_functions(tmp_path, INSTALLER,
        ["ByteSha", "PinnedFileBytes", "InvokeFrozenIntegrityProbe", "InvokeFrozenPlacementHelper"], r'''
$root=$env:LM_BOM_ROOT
$integrity=Join-Path $root 'tools\bootstrap_integrity.ps1'
$writer=Join-Path $root 'tools\label_writer_fence.ps1'
$helper=Join-Path $root 'INSTALL_THIS_PC.ps1'
$generation=Join-Path $root 'kmtech_shared\powershell\data_generation.ps1'
$frozen=[pscustomobject]@{root=$root;helper_path=$helper;writer_fence_path=$writer;
 data_generation_path=$generation;data_generation_sha256=(Get-FileHash -LiteralPath $generation).Hash.ToLowerInvariant();
 integrity_sha256=(Get-FileHash -LiteralPath $integrity).Hash.ToLowerInvariant();
 writer_fence_sha256=(Get-FileHash -LiteralPath $writer).Hash.ToLowerInvariant();
 helper_sha256=(Get-FileHash -LiteralPath $helper).Hash.ToLowerInvariant()}
function Join-Path {param($Path,$ChildPath)
 if($ChildPath -ceq 'WindowsPowerShell\v1.0\powershell.exe'){return 'Capture-Encoded'}
 Microsoft.PowerShell.Management\Join-Path -Path $Path -ChildPath $ChildPath
}
function Capture-Encoded {param([switch]$NoLogo,[switch]$NoProfile,[switch]$NonInteractive,$ExecutionPolicy,$EncodedCommand)
 $global:captured=[Text.Encoding]::Unicode.GetString([Convert]::FromBase64String($EncodedCommand))
 $global:LASTEXITCODE=0
}
function Start-Process {param($FilePath,$Verb,$ArgumentList,$WindowStyle,[switch]$Wait,[switch]$PassThru)
 $global:captured=[IO.File]::ReadAllText($launcherPath)
 $global:capturedPayload=[IO.File]::ReadAllBytes($payloadPath)
 [pscustomobject]@{ExitCode=0}
}
if($env:LM_BOM_WRAPPER -eq 'probe'){
 InvokeFrozenIntegrityProbe $frozen (Join-Path $env:LM_BOM_TMP 'absent-install')
 $boundary=$captured.IndexOf('$probeRoot =')
}else{
 [void](InvokeFrozenPlacementHelper $frozen @{})
 $boundary=$captured.IndexOf('$parameterNames =')
}
if($boundary -le 0){throw 'Encoded product helper boundary missing.'}
Remove-Item Function:\Join-Path
Remove-Item Function:\Start-Process
# Execute the generated byte-read/hash/decode prefix from the product wrapper.
# The domain operation and actual placement helper entrypoint are excluded.
$env:LM_BOM_PREFIX_SOURCE=Join-Path $env:LM_BOM_TMP 'verified-prefix.ps1'
$env:LM_BOM_PREFIX_PAYLOAD=Join-Path $env:LM_BOM_TMP 'verified-payload.dat'
[IO.File]::WriteAllText($env:LM_BOM_PREFIX_SOURCE,$captured.Substring(0,$boundary),(New-Object Text.UTF8Encoding($false)))
if($env:LM_BOM_WRAPPER -eq 'placement'){[IO.File]::WriteAllBytes($env:LM_BOM_PREFIX_PAYLOAD,$capturedPayload)}
$prefixRunner=Join-Path $env:LM_BOM_TMP 'prefix-runner.ps1'
[IO.File]::WriteAllText($prefixRunner,@'
$ErrorActionPreference='Stop'
$prefixParameters=@{}
if($env:LM_BOM_WRAPPER -eq 'placement'){$prefixParameters.PayloadBytes=[IO.File]::ReadAllBytes($env:LM_BOM_PREFIX_PAYLOAD)}
. ([ScriptBlock]::Create([IO.File]::ReadAllText($env:LM_BOM_PREFIX_SOURCE))) @prefixParameters
if(-not (Get-Command Assert-BootstrapIntegrityRecord -CommandType Function -ErrorAction SilentlyContinue)){throw 'Actual integrity helper did not execute.'}
if($env:LM_BOM_WRAPPER -eq 'placement'){
 if(-not (Get-Command Enter-LabelWriterSessionAuthority -CommandType Function -ErrorAction SilentlyContinue)){throw 'Actual writer helper did not execute.'}
 if(-not (Get-Command Get-KmtechDataDowngradeDecision -CommandType Function -ErrorAction SilentlyContinue)){throw 'Actual G helper did not execute.'}
 if($null -eq $helper.Ast.ParamBlock){throw 'Actual placement helper BOM hides its parameters.'}
}
'EXECUTED'
'@,(New-Object Text.UTF8Encoding($false)))
$start=New-Object Diagnostics.ProcessStartInfo
$start.FileName=$env:LM_BOM_ENGINE
$start.Arguments='-NoLogo -NoProfile -NonInteractive -File "'+$prefixRunner+'"'
$start.UseShellExecute=$false;$start.CreateNoWindow=$true
$start.RedirectStandardOutput=$true;$start.RedirectStandardError=$true
$child=[Diagnostics.Process]::Start($start)
$stdout=$child.StandardOutput.ReadToEndAsync();$stderr=$child.StandardError.ReadToEndAsync()
$child.WaitForExit()
Write-Output $stdout.GetAwaiter().GetResult()
if($child.ExitCode -ne 0){throw $stderr.GetAwaiter().GetResult()}
$child.Dispose()
''', engine="powershell.exe", values={"LM_BOM_ROOT": str(ROOT), "LM_BOM_TMP": str(tmp_path), "LM_BOM_WRAPPER": wrapper, "LM_BOM_ENGINE": engine})
    assert result.returncode == 0, result.stdout + result.stderr
    assert "EXECUTED" in result.stdout


def test_pretransaction_failure_diagnostics_survive_supported_user_removal(tmp_path, monkeypatch):
    import auto_update
    from current_user_onboarding import remove_current_user_setup

    monkeypatch.setattr(auto_update, "installation_root", lambda _root: None)
    archive = tmp_path / "local/KMTech/Label_Match/install-audit-pretransaction-failures/run"
    archive.mkdir(parents=True)
    original = archive / "canonical-portable-run.json"
    original.write_bytes(b'{"status":"FAILED_PRETRANSACTION","unique_failure":true}')
    before = original.read_bytes()
    # Call the unchanged removal domain with all three native persistence
    # callbacks substituted; writer admission/native CLI is the guest's scope.
    report = remove_current_user_setup.__wrapped__(
        tmp_path / "app", environ={"LOCALAPPDATA": str(tmp_path / "local"),
                                  "LABEL_MATCH_SAVE_DIR": str(tmp_path / "data")},
        autostart_remover=lambda: {"status": "ABSENT"},
        scheduled_task_remover=lambda _root: {"status": "ABSENT"},
        relay_stopper=lambda _root: {"status": "ABSENT"},
    )
    assert report["status"] == "PASS_DATA_PRESERVED"
    assert original.read_bytes() == before


@pytest.mark.parametrize("engine", ("powershell.exe", "pwsh.exe"))
@pytest.mark.parametrize("failure_phase", ("frozen", "external-evidence"))
def test_pretransaction_failure_boundary_rethrows_and_retires_only_own_run(tmp_path, engine, failure_phase):
    from tests.test_shared_ancestor_protection import run_functions

    result = run_functions(tmp_path, INSTALLER, ["Save", "Archive-PretransactionFailure", "Full"], r'''
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile($env:LM_CONTRACT_SOURCE,[ref]$tokens,[ref]$errors)
$boundary=@($ast.FindAll({param($n)
 $n -is [Management.Automation.Language.TryStatementAst] -and
 $n.Body.Extent.Text.Contains('$frozenPlacement = FreezePlacementHelper')
},$true))
if($boundary.Count -ne 1){throw 'Pretransaction recovery boundary missing.'}
$runId='boundary-run';$localAuditRoot=Join-Path $env:LM_BOUNDARY_ROOT 'install-audit'
. $env:LM_BOUNDARY_SHARED
[void][IO.Directory]::CreateDirectory($localAuditRoot)
$auditPath=Join-Path $localAuditRoot ('canonical-portable-'+$runId+'.json')
$EvidencePath=Join-Path $env:LM_BOUNDARY_ROOT 'external-evidence.json'
if($env:LM_BOUNDARY_PHASE -eq 'external-evidence'){
 $blocked=Join-Path $env:LM_BOUNDARY_ROOT 'blocked'
 [IO.File]::WriteAllText($blocked,'preserve path obstruction')
 $EvidencePath=Join-Path $blocked 'external-evidence.json'
}
$audit=[ordered]@{status='PREIMAGE_SAVED';code_placement='PENDING';rollback=@{applied=$false}}
Save $auditPath $audit
$source=$env:LM_BOUNDARY_ROOT;$receiptSource=@{critical_file_sha256=@{}}
[void][IO.Directory]::CreateDirectory((Join-Path $source 'app\kmtech_shared\powershell'))
[IO.File]::WriteAllBytes((Join-Path $source 'app\kmtech_shared\powershell\data_generation.ps1'),
 [IO.File]::ReadAllBytes((Join-Path $env:LM_BOUNDARY_SOURCE 'kmtech_shared\powershell\data_generation.ps1')))
function FreezePlacementHelper {param($Source,$AuditRoot,$RunId,$Expected)
 $script:freezeCalls++
 $frozen=Join-Path $AuditRoot ('canonical-portable-'+$RunId+'-helper')
 [void][IO.Directory]::CreateDirectory($frozen)
 [IO.File]::WriteAllText((Join-Path $frozen 'failure.ps1'),'unique failed input')
 throw 'ORIGINAL_FROZEN_HELPER_FAILURE'
}
function Enter-LabelWriterSessionAuthority {throw 'Writer authority must remain untouched.'}
function Start-LabelWriterFence {throw 'Writer fence must remain untouched.'}
$rejected=$false
$script:freezeCalls=0
try{. ([ScriptBlock]::Create($boundary[0].Extent.Text))}
catch{
 $originalFailure=$_
 if($env:LM_BOUNDARY_PHASE -eq 'frozen' -and $_.Exception.Message -cne 'ORIGINAL_FROZEN_HELPER_FAILURE'){throw}
 if($env:LM_BOUNDARY_PHASE -eq 'external-evidence' -and $script:freezeCalls -ne 0){throw 'Evidence failure did not precede helper evaluation.'}
 $rejected=$true
}
if(-not $rejected){throw 'Original failure was swallowed.'}
$retained=Join-Path (Join-Path ($localAuditRoot+'-pretransaction-failures') $runId) (Split-Path -Leaf $auditPath)
$evidence=Get-Content -LiteralPath $retained -Raw | ConvertFrom-Json
if($evidence.failure.message -cne $originalFailure.Exception.Message){throw 'Original failure differs from retained audit.'}
if($env:LM_BOUNDARY_PHASE -eq 'external-evidence' -and [IO.File]::ReadAllText($blocked) -cne 'preserve path obstruction'){throw 'Existing path obstruction changed.'}
if($evidence.status -cne 'FAILED_PRETRANSACTION' -or
 $evidence.recovery.transaction_started -or -not $evidence.recovery.artifacts_archived -or
 $evidence.rollback.applied -or (Test-Path -LiteralPath $localAuditRoot)) {throw 'Pretransaction recovery state differs.'}
'RECOVERED'
''', engine=engine, values={"LM_BOUNDARY_ROOT": str(tmp_path), "LM_BOUNDARY_SOURCE": str(ROOT), "LM_BOUNDARY_SHARED": str(ROOT / "kmtech_shared/powershell/portable.ps1"), "LM_BOUNDARY_PHASE": failure_phase})
    assert result.returncode == 0, result.stdout + result.stderr
    assert "RECOVERED" in result.stdout

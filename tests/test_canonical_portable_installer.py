import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

import pytest

from current_user_onboarding import (
    _portable_stop_marker_release_preflight,
    resolve_current_user_onboarding_paths,
)
from label_exact_clone_resolution import capture_conflict_preimage
from tools import build_portable_release_candidate as portable_builder
from tests.test_label_exact_clone_resolution import _paths as _exact_clone_paths


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
    files = {
        "runtime/python.exe": b"unsigned-python-fixture\n",
        "runtime/pythonw.exe": b"unsigned-pythonw-fixture\n",
        "app/main.py": b"raise SystemExit('must not run')\n",
        "launch-label-match.cmd": b"@echo off\r\nexit /b 99\r\n",
        "INSTALL_CANONICAL_PORTABLE.ps1": installer_source.encode("utf-8"),
        "INSTALL_THIS_PC.ps1": b"throw 'placement helper must not run'\n",
        "tools/bootstrap_integrity.ps1": b"throw 'integrity helper must not run'\n",
        "tools/label_writer_fence.ps1": b"throw 'writer fence must not run'\n",
    }
    for relative, content in files.items():
        target = source_root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)

    manifest = {
        "schema": "label-match-portable-tree-v1",
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
        "KM_LOGISTICS_PROFILE_PATH",
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
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
        env=environment,
    )


def _run_pristine_removal_gate_harness(
    tmp_path: Path, *, pristine_install: bool = True
) -> dict[str, object]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    source = _source()
    removal_anchor = "    $removalRoot = if ($existingVerified) { $install } else { $source }"
    removal_anchor_index = source.index(removal_anchor)
    guarded_start = source.rfind(
        "    if (-not $pristineInstall) {", 0, removal_anchor_index
    )
    block_start = (
        guarded_start
        if guarded_start >= 0 and removal_anchor_index - guarded_start < 256
        else removal_anchor_index
    )
    block_end = source.index(
        "    $unquiesced = @(UnquiescedProductWriters)", removal_anchor_index
    )
    removal_block = source[block_start:block_end]
    harness = tmp_path / "pristine-removal-gate.ps1"
    marker = tmp_path / "label_match_user_relay.stop.json"
    removal_report = tmp_path / "current_user_removal.json"
    harness.write_text(
        rf"""
$ErrorActionPreference = 'Stop'
$script:removalCalls = 0
$pristineInstall = {'$true' if pristine_install else '$false'}
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
    assert "Register-ScheduledTask" in source
    assert "Start-ScheduledTask" not in source
    assert "Stop-ScheduledTask" not in source
    assert "schtasks /run" not in source.lower()
    assert source.index("Product $removalRoot '--remove-current-user-setup'") < source.index(
        "InvokeFrozenPlacementHelper $frozenPlacement $helperParameters"
    )
    assert source.index("ReceiptSource $source $sourceManifest") < source.index(
        "InvokeFrozenPlacementHelper $frozenPlacement $helperParameters"
    )


def test_rollback_is_fail_closed_and_persists_explicit_failure() -> None:
    source = _source()
    rollback = source[source.index("catch {\n    $original = $_") :]

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
    sha_start = source.index("function Sha([string]$Path) {")
    sha_end = source.index("function UInt64BE([uint64]$Value)")
    freeze_start = source.index("function FreezePlacementHelper(")
    freeze_end = source.index("\nfunction InvokeFrozenIntegrityProbe")
    return source[sha_start:sha_end] + source[freeze_start:freeze_end]


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
    )
}}
catch {{
    $status = [string]$_.Exception.Message
    $paths = @(Get-ChildItem -LiteralPath $audit -Recurse -File |
        ForEach-Object {{ $_.FullName }})
}}
[pscustomobject][ordered]@{{
    status = $status
    current_user_writable = $writable
    files = @($paths | ForEach-Object {{ Inspect $_ }})
}} | ConvertTo-Json -Depth 6 -Compress
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


def test_frozen_placement_helper_denies_current_user_write(tmp_path: Path) -> None:
    result = _run_freeze_placement_helper_harness(tmp_path)
    assert result["status"] == "OK", result
    assert result["current_user_writable"] is False, result
    files = result["files"]
    assert len(files) == 3, result
    for frozen in files:
        assert frozen["write_probe_succeeded"] is False, frozen
        assert frozen["has_current_user_deny_write"] is True, frozen


def _product_functions() -> str:
    source = _source()
    start = source.index("function Arg([string]$Value) {")
    end = source.index("\n$WriterDelegationEnvironmentNames = @(")
    return source[start:end]


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
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return completed.stdout


def test_product_failure_includes_child_stderr_and_delegation_env(
    tmp_path: Path,
) -> None:
    output = _run_product_stderr_harness(tmp_path)
    assert "product_error=Product mode failed: --onboard-current-user/3" in output
    assert "token=repro-delegation-token-value-32ok" in output


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
    [switch]$WriterFenceFunctionsPreloaded,
    [string]$WriterFenceControlRoot,
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
    plan_block = source[source.index("if ($PlanOnly)") : source.index("$runId =")]

    assert "install_status=PLAN_ONLY" in plan_block
    assert "registry_changed=false" in plan_block
    assert "Save " not in plan_block
    assert "Start-Process" not in plan_block
    assert "INSTALL_THIS_PC.ps1" not in plan_block

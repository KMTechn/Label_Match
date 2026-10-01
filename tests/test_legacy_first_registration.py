"""Headless admission probes: never run an installer, relay or credential helper."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / "INSTALL_CANONICAL_PORTABLE.ps1"


def _quote(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _probe(tmp_path: Path, *, residue: str = "", compatibility: str = "PINNED_W9NEXTFIX_LABEL_RECOVERY",
           running: bool = False, verified: bool = True, race: bool = False) -> subprocess.CompletedProcess[str]:
    source = INSTALLER.read_text(encoding="utf-8-sig")
    definitions = source[:source.index("if (-not $SourceRoot) { $SourceRoot = $PSScriptRoot }")]
    preflight = source[source.index("$pristineResiduePaths = @("):source.index("$healthyWithoutCode =")]
    removal_start = source.rindex("    if (-not $pristineInstall", 0, source.index("    $unquiesced ="))
    phase_start = source.index("    if ($legacyFirstRegistration) {") if "    if ($legacyFirstRegistration) {" in source else removal_start
    phase_end = source.index("    $after = Snapshot", source.index("    $onboardingFailure = $null"))
    phase = source[phase_start:phase_end + len("    $after = Snapshot")]
    tmp_path.mkdir(parents=True, exist_ok=True)
    installed = tmp_path / "installed"
    installed.mkdir()
    local = tmp_path / "local"
    direct = local / "KMTech/DirectSync/label_match"
    profile = local / "KMTech/Logistics/profiles/Label_Match/runtime-profile.json"
    paths = {
        "credential": direct / "credential.json",
        "credential-directory": direct / "credential.json",
        "identity": direct / "producer_identity.json",
        "manifest": direct / "producer_manifest.json",
        "registration-report": direct / "status/label_match_worker_pc_registration.json",
        "onboarding-report": direct / "status/current_user_onboarding.json",
        "profile": profile,
        "profile-secret": profile.parent / "secrets/bearer-token.dpapi",
        "queue": direct / "queue/direct_sync_relay.sqlite3",
        "stop-marker": direct / "control/label_match_user_relay.stop.json",
    }
    if residue in paths:
        path = paths[residue]
        path.parent.mkdir(parents=True, exist_ok=True)
        if residue == "credential-directory":
            path.mkdir()
        else:
            path.write_text("corrupt partial state", encoding="utf-8")
    if residue == "custom-ledger":
        custom = tmp_path / "custom-business"
        custom.mkdir()
        (custom / "package_logistics_outbox.sqlite3").write_bytes(b"preserved ledger")
        settings = local / "KMTech/Label_Match/config/app_settings.json"
        settings.parent.mkdir(parents=True)
        settings.write_text(json.dumps({"custom_save_path": str(custom)}), encoding="utf-8")
    # A shared legacy file and its inherited Machine-style values are tripwires.
    shared = tmp_path / "shared-profile.json"
    shared.write_bytes(b"legacy shared state must remain untouched\n")
    bootstrap = local / "KMTech/Bootstrap/Label_Match/ca-bundle.pem"
    bootstrap.parent.mkdir(parents=True)
    bootstrap.write_bytes(b"pre-provisioned public CA\n")
    setup = f"""
$source = {_quote(tmp_path / 'candidate')}
$install = {_quote(installed)}
$defaultDirectSyncRoot = {_quote(direct)}
$selectedDirectSyncRoot = $defaultDirectSyncRoot
$defaultDataRoot = {_quote(local / 'KMTech/Label_Match/data')}
$selectedDataRoot = $defaultDataRoot
$defaultSettingsPath = {_quote(local / 'KMTech/Label_Match/config/app_settings.json')}
$selectedSettingsPath = $defaultSettingsPath
$defaultProfilePath = {_quote(profile)}
$selectedProfilePath = $defaultProfilePath
$bootstrapCaPath = {_quote(bootstrap)}
$stop = Join-Path $defaultDirectSyncRoot 'control/label_match_user_relay.stop.json'
$before = [pscustomobject]@{{exists={'$true' if residue == 'run' else '$false'}}}
$old = @({'[pscustomobject]@{pid=1}' if residue == 'relay' else ''})
$scheduledTasksAtPreflight = @({'[pscustomobject]@{name="old"}' if residue == 'task' else ''})
$sourceManifest = [pscustomobject]@{{source_commit='candidate'}}
$receiptSource = $null
$conflictReceiptSupplied = $false
$SkipSignatureValidationForTest = $true
$script:registrations = 0
$script:removals = 0
$script:placements = 0
$audit = @{{writer_fence=@{{status='QUIESCING'}}}}
$frozenPlacement = [pscustomobject]@{{helper_sha256='checked';integrity_sha256='checked';writer_fence_sha256='checked'}}
$writerFenceControlRoot = Join-Path $defaultDirectSyncRoot 'control/writer-session'
$onboardingPath = Join-Path $defaultDirectSyncRoot 'status/current_user_onboarding.json'
function Snapshot {{ return [pscustomobject]@{{exists=$false}} }}
function Relays {{}}
function Get-ScheduledTask {{}}
function Get-AuthenticodeSignature {{ return [pscustomobject]@{{Status='fixture'}} }}
function Sha {{ return 'checked' }}
function Save {{}}
function Same($Left, $Right) {{ return $Left -ceq $Right }}
function Assert-HealthyLifecycleOwnership {{}}
function Manifest {{ {'return [pscustomobject]@{source_commit=$(if($script:placements){"candidate"}else{"legacy"})}' if verified else "throw 'unverified installed code'"} }}
function InvokeFrozenIntegrityProbe {{}}
function PortableInventory {{ return [pscustomobject]@{{sha256='checked';schema_version='fixture';algorithm='fixture';file_count=1;byte_count=1;
    critical_file_sha256=@{{bootstrap_integrity_helper='checked';placement_helper='checked';writer_fence_helper='checked'}}}} }}
function Assert-WriterTransition {{ return [pscustomobject]@{{compatibility={_quote(compatibility)}}} }}
function HealthyLifecycle {{ throw 'current-user credential absent or unhealthy' }}
function UnquiescedProductWriters {{ {'[pscustomobject]@{pid=1}' if running else ''} }}
function Set-LabelWriterFenceInstalledTree {{}}
function Set-LabelWriterFenceDelegation {{}}
function SetWriterDelegationEnvironment {{}}
function InvokeFrozenPlacementHelper {{ $script:placements++; return 0 }}
function ReplacementPreimage {{ return {_quote(tmp_path / 'preserved-old-code')} }}
function Product($Root, $Mode) {{
    if ($Mode -ceq '--remove-current-user-setup') {{ $script:removals++; throw 'legacy removal must not run' }}
    if ($Mode -ceq '--onboard-current-user') {{
        if ($script:placements -ne 1 -or $Root -cne $install) {{ throw 'registration did not follow candidate placement' }}
        $script:registrations++
        New-Item -ItemType Directory -Path (Split-Path -Parent $onboardingPath) -Force | Out-Null
        [IO.File]::WriteAllText((Join-Path $defaultDirectSyncRoot 'credential.json'), 'synthetic self-registration marker')
        [IO.File]::WriteAllText($onboardingPath, '{{"status":"READY"}}')
    }}
}}
"""
    if residue == "access-denied":
        setup += "\nfunction Get-Item { throw [UnauthorizedAccessException]::new('unreadable user state') }\n"
    elif residue == "reparse":
        setup += "\nfunction Get-Item { return [pscustomobject]@{Attributes=[IO.FileAttributes]::ReparsePoint} }\n"
    script = tmp_path / "admission.ps1"
    before_phase = """
$healthyWithoutCode = $false
# A race is injected after admission and before the owned phase recheck.
""" + ("New-Item -ItemType Directory -Path (Split-Path -Parent $onboardingPath) -Force | Out-Null\n" if race else "")
    script.write_text(definitions + setup + "\ntry {\n" + preflight + before_phase + phase + """
    [pscustomobject]@{ admitted=$true; pristine=[bool]$pristineInstall;
        first=[bool]$legacyFirstRegistration; removals=$script:removals;
        placements=$script:placements; registrations=$script:registrations } | ConvertTo-Json -Compress
} catch {
    [pscustomobject]@{ admitted=$false; reason=$_.Exception.Message;
        removals=$script:removals; placements=$script:placements;
        registrations=$script:registrations } | ConvertTo-Json -Compress
}
""", encoding="utf-8-sig")
    environment = os.environ.copy()
    environment.update({"KMTECH_FACTORY_INSTALL_TEST_MODE": "1", "LOCALAPPDATA": str(local),
                        "KM_LOGISTICS_PROFILE_PATH": str(shared), "KM_LOGISTICS_SHARED_SENTINEL": "unchanged"})
    result = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-File", str(script)],
                            cwd=tmp_path, env=environment, capture_output=True, text=True,
                            encoding="utf-8", errors="replace", creationflags=subprocess.CREATE_NO_WINDOW)
    assert shared.read_bytes() == b"legacy shared state must remain untouched\n"
    if json.loads(result.stdout.strip()).get("admitted"):
        assert (direct / "credential.json").read_text() == "synthetic self-registration marker"
    if residue == "custom-ledger":
        assert (tmp_path / "custom-business/package_logistics_outbox.sqlite3").read_bytes() == b"preserved ledger"
    return result


def _payload(result: subprocess.CompletedProcess[str]) -> dict:
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout.strip())


def test_pinned_legacy_code_with_absent_user_state_admits_first_registration(tmp_path):
    result = _payload(_probe(tmp_path))
    assert result == {"admitted": True, "pristine": False, "first": True, "removals": 0,
                      "placements": 1, "registrations": 1}


@pytest.mark.parametrize("residue", ["credential", "credential-directory", "identity", "manifest",
                                    "registration-report", "onboarding-report", "profile", "profile-secret",
                                    "queue", "stop-marker", "run", "relay", "task", "custom-ledger",
                                    "access-denied", "reparse"])
def test_partial_or_persisted_user_state_keeps_healthy_gate(tmp_path, residue):
    result = _payload(_probe(tmp_path, residue=residue))
    assert result["admitted"] is False
    assert result["removals"] == 0
    assert result["placements"] == result["registrations"] == 0


@pytest.mark.parametrize("compatibility", ["UNCHANGED_PRODUCTION_AST_AND_CONTRACTS", "UNKNOWN",
                                           "PINNED_W9NEXTFIX_LABEL_RECOVERY_REVERSE"])
def test_unpinned_or_reverse_transition_does_not_admit_absent_user(tmp_path, compatibility):
    assert _payload(_probe(tmp_path, compatibility=compatibility))["admitted"] is False


def test_unverified_code_is_rejected_before_first_registration(tmp_path):
    assert _payload(_probe(tmp_path, verified=False))["admitted"] is False


def test_running_legacy_writer_is_rejected_before_mutation(tmp_path):
    result = _payload(_probe(tmp_path, running=True))
    assert result["admitted"] is False
    assert result["removals"] == 0


def test_user_state_race_is_rejected_before_placement(tmp_path):
    result = _payload(_probe(tmp_path, race=True))
    assert result["admitted"] is False
    assert result["placements"] == result["registrations"] == result["removals"] == 0


@pytest.mark.parametrize("installed_preimage,expected_removals", [(True, 0), (False, 1)])
def test_first_registration_rollback_only_removes_new_code_setup(tmp_path, installed_preimage, expected_removals):
    source = INSTALLER.read_text(encoding="utf-8-sig")
    start = source.index("            $rollbackProductRoot =")
    end = source.index("            if ($existingVerified -and -not $rollbackUsesInstalled)", start)
    installed = tmp_path / "installed"
    (installed / "runtime").mkdir(parents=True)
    (installed / "runtime/pythonw.exe").write_bytes(b"not an executable")
    script = tmp_path / "rollback.ps1"
    script.write_text(f"""
$install = {_quote(installed)}
$source = {_quote(tmp_path / 'candidate')}
$existingVerified = $true
$legacyFirstRegistration = $true
$writerFenceStarted = $true
$installedPreimageInventory = [pscustomobject]@{{sha256='old'}}
$receiptSource = [pscustomobject]@{{sha256='new'}}
$script:removed = 0
function Same($Left, $Right) {{ return $Left -ceq $Right }}
function InvokeFrozenIntegrityProbe {{}}
function PortableInventory {{ return [pscustomobject]@{{sha256={'"old"' if installed_preimage else '"new"'}}} }}
function Set-LabelWriterFenceInstalledTree {{}}
function Product($Root, $Mode) {{
    if ($Root -cne $install -or $Mode -cne '--remove-current-user-setup') {{ throw 'unexpected cleanup' }}
    $script:removed++
}}
function Assert-RollbackRelayPreimage {{}}
{source[start:end]}
[pscustomobject]@{{removed=$script:removed}} | ConvertTo-Json -Compress
""", encoding="utf-8-sig")
    result = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-File", str(script)],
                            cwd=tmp_path, capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW)
    assert _payload(result) == {"removed": expected_removals}


@pytest.mark.parametrize("stage", ["placement", "settings", "identity", "credential", "profile", "ledger", "relay"])
def test_registration_stage_failure_restores_absence_and_preserves_new_state(tmp_path, stage):
    source = INSTALLER.read_text(encoding="utf-8-sig")
    definitions = source[:source.index("if (-not $SourceRoot) { $SourceRoot = $PSScriptRoot }")]
    definitions += "\n. (Get-LabelSharedPortableLeafPath " + _quote(ROOT) + ")\n"
    catch_start = source.index("catch {\n    $original = $_")
    catch_end = source.index("\nfinally {", catch_start)
    installed = tmp_path / "installed"
    backup = tmp_path / "old-code"
    for root, version in ((installed, "new"), (backup, "old")):
        (root / "runtime").mkdir(parents=True)
        (root / "runtime/pythonw.exe").write_bytes(b"not executable")
        (root / "version.txt").write_text(version)
    direct = tmp_path / "local/DirectSync/label_match"
    profile = tmp_path / "local/Logistics/Label_Match"
    settings = tmp_path / "local/Label_Match/config/app_settings.json"
    ledger = tmp_path / "local/Label_Match/data/package_logistics_outbox.sqlite3"
    absent_paths = [direct, profile, settings, ledger, Path(str(ledger) + "-wal"),
                    Path(str(ledger) + "-shm"), Path(str(ledger) + "-journal")]
    assert all(not path.exists() for path in absent_paths)
    business = ledger.parent / "legacy-business.csv"
    business.parent.mkdir(parents=True)
    business.write_bytes(b"preserved old business\n")
    shared = tmp_path / "shared-profile.json"
    shared.write_bytes(b"preserved old shared identity\n")
    audit_root = tmp_path / "audit"
    audit_root.mkdir()
    # Simulate successive product writes; all native enrollment/relay I/O is excluded.
    generated = [direct / "control/writer-session/attempt.json"]
    steps = [settings, direct / "producer_identity.json", direct / "credential.json",
             profile / "runtime-profile.json", ledger, direct / "status/label_match_user_relay.json"]
    generated += steps[:["placement", "settings", "identity", "credential", "profile", "ledger", "relay"].index(stage)]
    for path in generated:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"synthetic attempt state\n")
    script = tmp_path / "failed-registration.ps1"
    setup = f"""
$install = {_quote(installed)}
$source = {_quote(tmp_path / 'candidate')}
$existingVerified = $true
$legacyFirstRegistration = $true
$firstRegistrationStateVerified = $true
$firstRegistrationStarted = {'$false' if stage == 'placement' else '$true'}
$firstRegistrationQuarantinePaths = @({','.join(_quote(path) for path in absent_paths)})
$localAuditRoot = {_quote(audit_root)}
$auditPath = Join-Path $localAuditRoot 'audit.json'
$runId = 'attempt-1'
$writerFenceControlRoot = {_quote(direct / 'control/writer-session')}
$writerFenceStarted = $true
$mutated = $true
$replacementRollbackRoot = {_quote(backup)}
$installedPreimageInventory = [pscustomobject]@{{sha256='old';bootstrap_aggregate_sha256='checked';file_count=2;byte_count=16}}
$receiptSource = [pscustomobject]@{{sha256='new'}}
$helperParameters = @{{SourceRoot=$source}}
$before = [pscustomobject]@{{exists=$false;kind='';data=''}}
$old = @()
$stopBefore = [pscustomobject]@{{exists=$false}}
$stop = {_quote(direct / 'control/label_match_user_relay.stop.json')}
$removalPath = {_quote(direct / 'status/current_user_removal.json')}
$removalReportExisted = $false
$healthyRemovalReportBefore = $null
$audit = @{{rollback=@{{}};writer_fence=@{{status='RESTORING'}}}}
function Set-LabelWriterFenceDelegation {{}}
function SetWriterDelegationEnvironment {{}}
function Set-LabelWriterFenceInstalledTree {{}}
function InvokeFrozenIntegrityProbe {{}}
function PortableInventory($Root) {{ return [pscustomobject]@{{sha256=[IO.File]::ReadAllText((Join-Path $Root 'version.txt'))}} }}
function Product {{}}
function Assert-RollbackRelayPreimage {{}}
function InvokeFrozenPlacementHelper($Frozen, $Parameters) {{
    [IO.File]::WriteAllText((Join-Path $install 'version.txt'), [IO.File]::ReadAllText((Join-Path $Parameters.SourceRoot 'version.txt')))
    return 0
}}
$script:admissionDepth = 0
function Enter-LabelWriterAdmission {{ $script:admissionDepth++; return [pscustomobject]@{{fixture=$true}} }}
function Exit-LabelWriterAdmission {{ $script:admissionDepth-- }}
function Move-Item {{
    [CmdletBinding()] param([string]$LiteralPath, [string]$Destination, [switch]$Force)
    if ($firstRegistrationQuarantinePaths -contains $LiteralPath -and $script:admissionDepth -lt 1) {{
        throw 'self state move was not protected by writer admission'
    }}
    Microsoft.PowerShell.Management\\Move-Item -LiteralPath $LiteralPath -Destination $Destination -Force:$Force -ErrorAction Stop
}}
function Restore {{}}
function RestoreScheduledTask {{}}
function Stop-LabelWriterFence {{ return [pscustomobject]@{{writer_inventory_sha256='checked'}} }}
function Snapshot {{ return [pscustomobject]@{{exists=$false;kind='';data=''}} }}
try {{
    try {{ throw 'injected registration stage failure' }}
    {source[catch_start:catch_end]}
}} catch {{ $audit.fixture_caught_error = $_.Exception.Message }}
$audit | ConvertTo-Json -Depth 10 -Compress
if ($script:admissionDepth -ne 0) {{ throw 'writer admission lease leaked' }}
"""
    script.write_text(definitions + setup, encoding="utf-8-sig")
    environment = os.environ.copy()
    environment.update({"KMTECH_FACTORY_INSTALL_TEST_MODE": "1", "KM_LOGISTICS_PROFILE_PATH": str(shared)})
    result = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-File", str(script)],
                            cwd=tmp_path, env=environment, capture_output=True, text=True,
                            creationflags=subprocess.CREATE_NO_WINDOW)
    audit = _payload(result)
    assert audit["status"] == "FAILED_ROLLED_BACK", audit
    assert audit["rollback"]["code_placement"] == "RESTORED_PREIMAGE"
    assert all(not path.exists() for path in absent_paths), "new self state survived code rollback"
    recovery = audit["rollback"]["first_registration_state"]
    assert recovery["restored_absent"] is True
    assert recovery["server_registration_may_exist"] is (stage != "placement")
    assert recovery["server_rollback_performed"] is False
    assert (installed / "version.txt").read_text() == "old"
    assert shared.read_bytes() == b"preserved old shared identity\n"
    assert business.read_bytes() == b"preserved old business\n"
    quarantine = Path(recovery["root"])
    assert (quarantine / "state-moves.json").is_file()
    assert len(list(quarantine.rglob("attempt.json"))) == 1
    # Every generated file is preserved, including later-stage identities and ledgers.
    assert sum(path.is_file() for path in quarantine.rglob("*")) == len(generated) + 1

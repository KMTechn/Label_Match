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
    definitions += "\n. (Get-LabelSharedPortableLeafPath " + _quote(ROOT) + ")\n"
    preflight = source[source.index("$pristineResiduePaths = @("):source.index("$healthyWithoutCode =")]
    removal_start = source.rindex("    if (-not $pristineInstall", 0, source.index("    $unquiesced ="))
    phase_marker = "    if ($legacyFirstRegistration) {\n        # Repeat absence"
    phase_start = source.index(phase_marker) if phase_marker in source else removal_start
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
$startupLifecycle = $null
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
$firstRegistrationAttempt = @{state_verified=$false;registration_started=$false}
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
                      "placements": 1, "registrations": 1}, result


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
    end = source.index("\n            }\n            . Invoke-LabelRollbackStep 'writer_absence'", start)
    installed = tmp_path / "installed"
    (installed / "runtime").mkdir(parents=True)
    (installed / "runtime/pythonw.exe").write_bytes(b"not an executable")
    script = tmp_path / "rollback.ps1"
    script.write_text(f"""
$startupLifecycle = $null
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
    catch_start = source.rindex("\ncatch {\n") + 1
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
$startupLifecycle = $null
$install = {_quote(installed)}
$source = {_quote(tmp_path / 'candidate')}
$existingVerified = $true
$legacyFirstRegistration = $true
$firstRegistrationStateVerified = $true
$firstRegistrationStarted = {'$false' if stage == 'placement' else '$true'}
$firstRegistrationQuarantinePaths = @({','.join(_quote(path) for path in absent_paths)})
$defaultDirectSyncRoot = {_quote(direct)}
$selectedProfilePath = {_quote(profile / 'runtime-profile.json')}
$firstRegistrationStatePaths = @({_quote(direct / 'credential.json')},{_quote(direct / 'producer_identity.json')},
    {_quote(direct / 'producer_manifest.json')},{_quote(direct / 'status')},{_quote(direct / 'secrets')},
    {','.join(_quote(path) for path in absent_paths[1:])},{_quote(direct)})
$firstRegistrationAbsentAncestors = @()
$localAuditRoot = {_quote(audit_root)}
$auditPath = Join-Path $localAuditRoot 'audit.json'
$runId = 'attempt-1'
$firstRegistrationAttemptPath = Join-Path $localAuditRoot 'attempt.json'
$firstRegistrationAttempt = @{{status='RUNNING';state_verified=$true;registration_started=$firstRegistrationStarted;
    quarantine_root=(Join-Path $localAuditRoot ('canonical-portable-' + $runId + '-first-registration-quarantine'));
    remaining_paths=@();old_app_state='ABSENT'}}
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
    if ($firstRegistrationStatePaths -contains $LiteralPath -and $script:admissionDepth -lt 1) {{
        throw 'authentication move was not protected by writer admission'
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


@pytest.mark.parametrize("failures", [0, 2, 3])
def test_credential_first_quarantine_retries_and_resumes_without_overwrite(tmp_path, failures):
    source = INSTALLER.read_text(encoding="utf-8-sig")
    definitions = source[:source.index("if (-not $SourceRoot) { $SourceRoot = $PSScriptRoot }")]
    definitions += "\n. (Get-LabelSharedPortableLeafPath " + _quote(ROOT) + ")\n"
    direct = tmp_path / "local/DirectSync/label_match"
    credential = direct / "credential.json"
    profile = tmp_path / "local/profile"
    quarantine = tmp_path / "audit/quarantine"
    direct.mkdir(parents=True)
    profile.mkdir(parents=True)
    credential.write_bytes(b"synthetic protected credential")
    (profile / "runtime-profile.json").write_bytes(b"synthetic profile")
    script = tmp_path / "retry.ps1"
    script.write_text(definitions + f"""
$credential = {_quote(credential)}
$direct = {_quote(direct)}
$paths = @($credential,{_quote(profile)},$direct)
$quarantine = {_quote(quarantine)}
$script:limit = {failures}
$script:credentialAttempts = 0
$script:order = @()
function Move-Item {{
    [CmdletBinding()] param([string]$LiteralPath,[string]$Destination,[switch]$Force)
    if ($paths -contains $LiteralPath) {{ $script:order += $LiteralPath }}
    if ($LiteralPath -ceq $credential) {{
        $script:credentialAttempts++
        if ($script:credentialAttempts -le $script:limit) {{ throw [IO.IOException]::new('injected credential move failure') }}
    }}
    Microsoft.PowerShell.Management\\Move-Item -LiteralPath $LiteralPath -Destination $Destination -Force:$Force -ErrorAction Stop
}}
try {{
    $first = Move-LegacyFirstRegistrationState $paths $quarantine $true @($direct)
    $firstAttempts = $script:credentialAttempts
    $firstCredentialPresent = Test-Path -LiteralPath $credential
    $script:limit = 0
    $resumed = Move-LegacyFirstRegistrationState $paths $quarantine $true @($direct)
    $final = Move-LegacyFirstRegistrationState $paths $quarantine $true
    [ordered]@{{first_absent=$first.restored_absent; first_credential_present=$firstCredentialPresent;
        first_attempts=$firstAttempts; resumed_absent=$resumed.restored_absent; final_absent=$final.restored_absent;
        credential_first=($script:order[0] -ceq $credential)}} | ConvertTo-Json -Compress
}} catch {{ [ordered]@{{error=$_.Exception.GetType().Name;message=$_.Exception.Message}} | ConvertTo-Json -Compress }}
""", encoding="utf-8-sig")
    result = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-File", str(script)],
                            cwd=tmp_path, capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW)
    assert _payload(result) == {"first_absent": failures < 3, "first_credential_present": failures == 3,
                                "first_attempts": min(failures + 1, 3), "resumed_absent": True,
                                "final_absent": True, "credential_first": True}
    assert not credential.exists() and not direct.exists() and not profile.exists()
    records = json.loads((quarantine / "state-moves.json").read_text(encoding="utf-8-sig"))
    credential_copy = Path(records["state_paths"][0]["quarantined_path"])
    assert credential_copy.read_bytes() == b"synthetic protected credential"
    assert sum(path.name == "runtime-profile.json" for path in quarantine.rglob("*")) == 1


@pytest.mark.parametrize("verified,failures,move_root", [(False, 0, False), (True, 0, False), (True, 2, False), (True, 3, False), (True, 3, True), (True, 4, True)])
def test_real_fence_failure_boundaries_and_recorded_cleanup_resume(tmp_path, monkeypatch, verified, failures, move_root, *, entry_fence=""):
    """Real Start/Stop and committed catch; native enrollment/code are adapters."""
    # Real Windows PowerShell I/O has the normal path-length limit. Keep the
    # fixture case leaf short while retaining every run under its D task root.
    tmp_path = tmp_path.parent / (f"f{int(verified)}{failures}{int(move_root)}" + entry_fence[:1])
    run = subprocess.run
    captured = {}
    stage = "credential" if failures else "placement"

    def intercept(args, **kwargs):
        script = Path(args[-1])
        source = script.read_text(encoding="utf-8-sig")
        helper = ROOT / "tools/label_writer_fence.ps1"
        binding = f"""
. {_quote(helper)}
$script:realStart = (Get-Command Start-LabelWriterFence).ScriptBlock
$script:realStop = (Get-Command Stop-LabelWriterFence).ScriptBlock
$writerDelegationToken = ('d' * 64)
$rollbackSources = @('current_user_setup_removal')
$frozenPlacement = [pscustomobject]@{{}}
$healthyRemovedInstall = $false
$taskBefore = [pscustomobject]@{{exists=$false;enabled=$false;backup_path=''}}
$EvidencePath = ''
$writerStartedAt = [DateTime]::UtcNow.ToString('o')
$writerOrchestratorSha256 = ('a' * 64)
$writerContractSha256 = ('b' * 64)
$script:injectedFailures = {failures if failures < 4 else 0}
"""
        source = source.replace("$install = ", binding + "$install = ", 1)
        source = source.replace("$runId = 'attempt-1'", "$runId = '20261001T120000000Z-" + "1" * 32 + "'", 1)
        source = source.replace("$firstRegistrationAttemptPath = Join-Path $localAuditRoot 'attempt.json'",
                                "$firstRegistrationAttemptPath = Join-Path $localAuditRoot ('canonical-portable-' + $runId + '-first-registration-attempt.json')", 1)
        if not verified:
            source = source.replace("$firstRegistrationStateVerified = $true", "$firstRegistrationStateVerified = $false", 1)
        if not failures:
            (tmp_path / "installed/version.txt").write_text("old")
        (tmp_path / "local/DirectSync/label_match/control/writer-session/attempt.json").unlink()
        real_fence = """function Stop-LabelWriterFence {
    param([string]$ControlRoot,[string]$SessionId,[string]$AttemptId,
          [string]$ReplacementTransactionId,[int]$TimeoutMilliseconds=90000)
    & $script:realStop @PSBoundParameters
}
$writerSessionId = ('1' * 32)
$writerAttemptId = ('2' * 32)
$writerTransactionId = ('3' * 32)
[void](& $script:realStart -ControlRoot $writerFenceControlRoot -Status 'QUIESCING' `
    -SessionId $writerSessionId -AttemptId $writerAttemptId -ReplacementTransactionId $writerTransactionId `
    -SessionStartedAtUtc ([DateTime]::UtcNow.ToString('o')) -OrchestratorSha256 ('a' * 64) `
    -WriterContractSha256 ('b' * 64) -AuthorityOwnedByCaller)
$firstRegistrationAttempt.schema = 'label-match-first-registration-attempt-v1'
$firstRegistrationAttempt.run_id = $runId
$firstRegistrationAttempt.install_root = $install
$firstRegistrationAttempt.source_commit = 'candidate-commit'
$firstRegistrationAttempt.orchestrator_sha256 = ('a' * 64)
$firstRegistrationAttempt.writer_contract_sha256 = ('b' * 64)
$firstRegistrationAttempt.writer_session_id = $writerSessionId
$firstRegistrationAttempt.writer_attempt_id = $writerAttemptId
$firstRegistrationAttempt.writer_transaction_id = $writerTransactionId
$firstRegistrationAttempt.absent_paths = $firstRegistrationStatePaths
$firstRegistrationAttempt.absent_ancestors = @()
"""
        source = source.replace("function Stop-LabelWriterFence { return [pscustomobject]@{writer_inventory_sha256='checked'} }", real_fence, 1)
        if entry_fence:
            installer = INSTALLER.read_text(encoding="utf-8-sig")
            begin = installer.index("try {\n    $writerAuthority = Enter-LabelWriterSessionAuthority")
            entry = installer[begin:installer.index("\n    [void](Set-LabelWriterFenceDelegation", begin)] + "\n}"
            source = source.replace("try { throw 'injected registration stage failure' }", entry, 1)
            setup = """
$firstRegistrationAttempt.state_verified = $firstRegistrationStateVerified
$firstRegistrationAttempt.status = 'PREFLIGHT_RECORDED'
Save-FirstRegistrationAttempt $firstRegistrationAttempt $firstRegistrationAttemptPath
$firstRegistrationResume = Read-FirstRegistrationAttempt $localAuditRoot $install 'candidate-commit' ('a' * 64) $firstRegistrationStatePaths
$firstRegistrationAttempt = $firstRegistrationResume
[void](Stop-LabelWriterFence -ControlRoot $writerFenceControlRoot -SessionId $writerSessionId `
    -AttemptId $writerAttemptId -ReplacementTransactionId $writerTransactionId)
$writerAuthority = $null
$writerFenceStarted = $false
$mutated = $false
$elevationLogPath = ''
$frozenPlacement = [pscustomobject]@{helper_sha256='synthetic';helper_path='synthetic'}
"""
            if entry_fence == "foreign":
                setup += """
[void](& $script:realStart -ControlRoot $writerFenceControlRoot -Status 'QUIESCING' `
    -SessionId ('9' * 32) -AttemptId $writerAttemptId -ReplacementTransactionId $writerTransactionId `
    -SessionStartedAtUtc $writerStartedAt -OrchestratorSha256 $writerOrchestratorSha256 `
    -WriterContractSha256 $writerContractSha256 -AuthorityOwnedByCaller)
$foreignFenceSha = (Get-FileHash -LiteralPath (Join-Path $writerFenceControlRoot 'active.json')).Hash
"""
            source = source.replace("try {\n    try {", setup + "\ntry {\n    try {", 1)
            tail = """
if ($null -ne $writerAuthority) { Exit-LabelWriterSessionAuthority $writerAuthority }
$active = Join-Path $writerFenceControlRoot 'active.json'
[ordered]@{status=$audit['status'];error=$audit['fixture_caught_error'];mutated=$mutated;
    direct_present=(Test-Path -LiteralPath $defaultDirectSyncRoot);
    credential_present=(Test-Path -LiteralPath (Join-Path $defaultDirectSyncRoot 'credential.json'));
    foreign_unchanged=$(if (Test-Path -LiteralPath $active) { (Get-FileHash -LiteralPath $active).Hash -ceq $foreignFenceSha } else { $false })
} | ConvertTo-Json -Compress
"""
            source = source.replace("$audit | ConvertTo-Json -Depth 10 -Compress", tail, 1)
            script.write_text(source, encoding="utf-8-sig")
            result = run(args, **kwargs)
            assert result.returncode == 0, result.stderr
            captured.update(json.loads(result.stdout))
            return result
        if failures == 4:
            source = source.replace("$firstRegistrationAttempt.schema = ", """
$script:realMoveState = (Get-Command Move-LegacyFirstRegistrationState).ScriptBlock
$script:throwFinalCleanup = $true
function Move-LegacyFirstRegistrationState {
    param($AbsentPaths,$QuarantineRoot,$ServerRegistrationMayExist,$DeferredPaths=@(),$AbsentAncestors=@(),$AuditRoot='')
    if ($DeferredPaths.Count -eq 0 -and $script:throwFinalCleanup) {
        $script:throwFinalCleanup = $false
        throw [IO.IOException]::new('injected final cleanup exception')
    }
    & $script:realMoveState @PSBoundParameters
}
$firstRegistrationAttempt.schema = """, 1)
        move_target = "$defaultDirectSyncRoot" if move_root else "(Join-Path $defaultDirectSyncRoot 'credential.json')"
        source = source.replace("    if ($firstRegistrationQuarantinePaths -contains $LiteralPath",
                                f"    if ($LiteralPath -ceq {move_target} -and $script:injectedFailures -gt 0) {{ $script:injectedFailures--; throw [IO.IOException]::new('injected quarantine failure') }}\n"
                                "    if ($firstRegistrationQuarantinePaths -contains $LiteralPath", 1)
        tail = """
$firstAudit = $audit | ConvertTo-Json -Depth 10 | ConvertFrom-Json
$firstCode = [IO.File]::ReadAllText((Join-Path $install 'version.txt'))
$firstCredentialPresent = Test-Path -LiteralPath (Join-Path $defaultDirectSyncRoot 'credential.json')
$firstFencePresent = Test-Path -LiteralPath (Join-Path $writerFenceControlRoot 'active.json')
$resumeFound = $false
"""
        if failures >= 3:
            installer = INSTALLER.read_text(encoding="utf-8-sig")
            begin = installer.rindex("\ncatch {\n") + 1
            end = installer.index("\nfinally {", begin)
            tail += """
$pending = Read-FirstRegistrationAttempt $localAuditRoot $install 'candidate-commit' ('a' * 64) $firstRegistrationStatePaths
$resumeFound = $null -ne $pending
if (-not $resumeFound) { throw 'same-command cleanup record was not found' }
$firstRegistrationAttempt = $pending
$script:injectedFailures = 0
$audit = @{rollback=@{};writer_fence=@{status='RESTORING'}}
try {
    try { throw 'RESUME_FIRST_REGISTRATION_ROLLBACK' }
""" + installer[begin:end] + """
} catch { $audit.fixture_caught_error = $_.Exception.Message }
"""
        tail += """
[ordered]@{first_status=$firstAudit.status;first_error=$firstAudit.fixture_caught_error;first_code=$firstCode;first_credential=$firstCredentialPresent;
    first_fence=$firstFencePresent;resume_found=$resumeFound;final_status=$audit.status;
    final_code=[IO.File]::ReadAllText((Join-Path $install 'version.txt'));
    final_direct_present=(Test-Path -LiteralPath $defaultDirectSyncRoot)} | ConvertTo-Json -Compress
"""
        source = source.replace("$audit | ConvertTo-Json -Depth 10 -Compress", tail, 1)
        script.write_text(source, encoding="utf-8-sig")
        result = run(args, **kwargs)
        assert result.returncode == 0, result.stderr
        captured.update(json.loads(result.stdout))
        return result

    monkeypatch.setattr(subprocess, "run", intercept)
    with pytest.raises((AssertionError, KeyError)):
        test_registration_stage_failure_restores_absence_and_preserves_new_state(tmp_path, stage)
    if entry_fence:
        if entry_fence == "absent":
            assert captured["status"] == "FAILED_ROLLED_BACK", captured
            assert captured["error"] == "RESUME_FIRST_REGISTRATION_ROLLBACK", captured
            assert captured["direct_present"] is captured["credential_present"] is False
        else:
            assert captured["error"] == "LABEL_WRITER_FENCE_OWNER_MISMATCH", captured
            assert captured["mutated"] is False
            assert captured["foreign_unchanged"] is True
        return
    assert captured["first_status"] == ("FAILED_ROLLBACK_INCOMPLETE" if failures >= 3 else "FAILED_ROLLED_BACK"), json.dumps(captured)
    assert captured["first_credential"] is (failures == 3 and not move_root)
    assert captured["first_fence"] is (failures >= 3)
    assert captured["first_code"] == ("new" if failures == 3 and not move_root else "old")
    assert captured["resume_found"] is (failures >= 3)
    assert captured["final_status"] == "FAILED_ROLLED_BACK", json.dumps(captured)
    assert captured["final_code"] == "old"
    assert captured["final_direct_present"] is False


@pytest.mark.parametrize("fence", ["absent", "foreign"])
def test_pending_retry_enters_normal_fence_and_preserves_other_writer(tmp_path, monkeypatch, fence):
    test_real_fence_failure_boundaries_and_recorded_cleanup_resume(
        tmp_path, monkeypatch, False, 0, False, entry_fence=fence)


@pytest.mark.parametrize("mutation", ["", "source", "paths", "ancestor", "boolean"])
def test_cleanup_record_requires_exact_source_and_owned_paths(tmp_path, mutation):
    tmp_path = tmp_path.parent / ("m-" + (mutation or "valid"))
    tmp_path.mkdir()
    source = INSTALLER.read_text(encoding="utf-8-sig")
    definitions = source[:source.index("if (-not $SourceRoot) { $SourceRoot = $PSScriptRoot }")]
    definitions += "\n. (Get-LabelSharedPortableLeafPath " + _quote(ROOT) + ")\n"
    audit = tmp_path / "audit"
    audit.mkdir()
    direct = tmp_path / "local/DirectSync/label_match"
    credential = direct / "credential.json"
    paths = [str(credential), str(direct)]
    run_id = "20261001T120000000Z-" + "1" * 32
    record = {"schema": "label-match-first-registration-attempt-v1", "run_id": run_id,
              "status": "FAILED_ROLLBACK_INCOMPLETE", "install_root": str(tmp_path / "installed"),
              "source_commit": "candidate", "orchestrator_sha256": "a" * 64,
              "writer_session_id": "1" * 32, "writer_attempt_id": "2" * 32, "writer_transaction_id": "3" * 32,
              "quarantine_root": str(audit / ("canonical-portable-" + run_id + "-first-registration-quarantine")),
              "absent_paths": paths, "absent_ancestors": [], "state_verified": True, "registration_started": True}
    if mutation == "source":
        record["source_commit"] = "other-source"
    elif mutation == "paths":
        record["absent_paths"] = [str(tmp_path / "foreign-state")]
    elif mutation == "ancestor":
        record["absent_ancestors"] = [str(tmp_path.parent)]
    elif mutation == "boolean":
        record["state_verified"] = "false"
    record_path = audit / ("canonical-portable-" + run_id + "-first-registration-attempt.json")
    record_path.write_text(json.dumps(record), encoding="utf-8")
    script = tmp_path / "record.ps1"
    script.write_text(definitions + f"""
try {{
    $record = Read-FirstRegistrationAttempt {_quote(audit)} {_quote(tmp_path / 'installed')} 'candidate' ('a' * 64) @({','.join(_quote(path) for path in paths)})
    [ordered]@{{accepted=($null -ne $record)}} | ConvertTo-Json -Compress
}} catch {{ [ordered]@{{accepted=$false}} | ConvertTo-Json -Compress }}
""", encoding="utf-8-sig")
    environment = os.environ.copy()
    environment["LOCALAPPDATA"] = str(tmp_path / "local")
    result = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-File", str(script)],
                            cwd=tmp_path, env=environment, capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW)
    assert _payload(result) == {"accepted": not mutation}
    assert not direct.exists()
    assert not Path(record["quarantine_root"]).exists()

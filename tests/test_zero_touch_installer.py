from pathlib import Path
import subprocess

import pytest
from tools import install_logistics_runtime_profile as machine_profiles

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

    assert "#Requires -RunAsAdministrator" not in alias
    assert "Invoke-SelfElevated $MyInvocation.MyCommand.Path $PSBoundParameters $args" in alias
    assert "WindowsBuiltInRole]::Administrator" in alias
    assert "-Verb RunAs" in alias
    assert "-Wait -PassThru" in alias
    _assert_powershell_ast(ROOT / "INSTALL_THIS_PC.ps1")
    assert "install_label_match_direct_sync.ps1" in alias
    assert "@args" in alias
    assert "tokenless self-enrollment" in alias
    assert "Start-ScheduledTask -TaskName $TaskName -ErrorAction Stop" in installer
    assert '$taskStartStatus = "FAILED"' in installer
    assert "Remove-NewMachineProfilesFromRegistrationReport" in installer
    assert "created_paths" in installer
    assert "Unregister-ScheduledTask -TaskName $TaskName" in installer
    assert '[string]$AppRunUser = "*S-1-5-32-545"' in installer
    assert "Read-Host" not in installer
    assert "Producer enrollment token" not in installer
    assert (
        'C:\\ProgramData\\KMTech\\Logistics\\profiles\\Label_Match'
        '\\runtime-profile.json'
    ) in installer
    assert '"--logistics-profile-path", $LogisticsProfilePath' in installer
    _assert_powershell_ast(ROOT / "install_label_match_direct_sync.ps1")


def test_release_contains_common_package_entrypoint():
    workflow = (ROOT / ".github" / "workflows" / "release.yml").read_text(
        encoding="utf-8"
    )

    assert "Copy-Item INSTALL_THIS_PC.ps1 -Destination dist/Label_Match" in workflow
    assert '"INSTALL_THIS_PC.ps1"' in workflow
    assert '"Label_Match/INSTALL_THIS_PC.ps1"' in workflow


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

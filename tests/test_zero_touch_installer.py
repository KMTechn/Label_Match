import json
import hashlib
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import shutil
import subprocess

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / "INSTALL_THIS_PC.ps1"


def _powershell() -> str:
    executable = shutil.which("powershell.exe")
    if not executable:
        pytest.skip("Windows PowerShell is required")
    return executable


def _run_legacy_task_removal_harness(tmp_path: Path, scenario: str):
    installer_text = INSTALLER.read_text(encoding="utf-8")
    start = installer_text.index("function Get-LegacyTaskByNameFailClosed")
    end = installer_text.index("function Test-CurrentUserRelayPersistencePresent")
    functions = installer_text[start:end]
    harness = tmp_path / f"legacy-task-{scenario}.ps1"
    harness.write_text(
        functions
        + rf"""
$script:scenario = '{scenario}'
$script:queryCalls = 0
$script:stopCalls = 0
$script:unregisterCalls = 0
$script:lastRowType = ''
$script:stopTaskPath = ''
$script:unregisterTaskPath = ''

function New-SyntheticOwnedTask([string]$TaskPath) {{
    $row = [ordered]@{{
        TaskName = 'legacy-label-task'
        TaskPath = $TaskPath
        Actions = @([ordered]@{{
            Execute = 'powershell.exe'
            Arguments = '-File C:\expected\current\relay.ps1'
        }})
    }}
    $script:lastRowType = $row.GetType().FullName
    return $row
}}

function Get-ScheduledTask {{
    param([object]$ErrorAction)
    $script:queryCalls++
    if ($script:scenario -eq 'query_error') {{
        throw [InvalidOperationException]::new('synthetic inventory failure')
    }}
    if ($script:scenario -eq 'duplicate') {{
        return @(
            (New-SyntheticOwnedTask '\One\'),
            (New-SyntheticOwnedTask '\Two\')
        )
    }}
    if ($script:scenario -eq 'readback_error') {{
        if ($script:queryCalls -eq 1) {{ return New-SyntheticOwnedTask '\Owned\' }}
        throw [InvalidOperationException]::new('synthetic readback failure')
    }}
    if ($script:scenario -eq 'still_present') {{
        return New-SyntheticOwnedTask '\Owned\'
    }}
    return @()
}}

function Stop-ScheduledTask {{
    param([string]$TaskName, [string]$TaskPath, [object]$ErrorAction)
    $script:stopCalls++
    $script:stopTaskPath = $TaskPath
}}

function Unregister-ScheduledTask {{
    param(
        [string]$TaskName,
        [string]$TaskPath,
        [switch]$Confirm,
        [object]$ErrorAction
    )
    $script:unregisterCalls++
    $script:unregisterTaskPath = $TaskPath
}}

$status = 'PASS'
$message = ''
try {{
    Remove-OwnedLegacyTask 'legacy-label-task' 'C:\expected\current'
}}
catch {{
    $status = 'FAIL'
    $message = [string]$_.Exception.Message
}}
[pscustomobject]@{{
    status = $status
    message = $message
    query_calls = $script:queryCalls
    stop_calls = $script:stopCalls
    unregister_calls = $script:unregisterCalls
    row_type = $script:lastRowType
    stop_task_path = $script:stopTaskPath
    unregister_task_path = $script:unregisterTaskPath
}} | ConvertTo-Json -Compress
""",
        encoding="utf-8-sig",
    )
    completed = subprocess.run(
        [
            _powershell(),
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


def _run_installer(
    source_root: Path,
    install_root: Path,
    *extra: str,
    timeout: int = 60,
):
    environment = dict(os.environ)
    environment["KMTECH_FACTORY_INSTALL_TEST_MODE"] = "1"
    return subprocess.run(
        [
            _powershell(),
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(INSTALLER),
            "-SourceRoot",
            str(source_root),
            "-InstallRoot",
            str(install_root),
            "-AllowNoncanonicalLayoutForTest",
            *extra,
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=environment,
    )


def _release_fixture(root: Path) -> Path:
    release = root / "frozen-release"
    (release / "_internal").mkdir(parents=True)
    (release / "Label_Match.exe").write_bytes(b"label-match-frozen-exe")
    (release / "contract.lock.json").write_text(
        '{"lock_schema_version": 1}\n',
        encoding="utf-8",
    )
    (release / "_internal" / "python312.dll").write_bytes(b"reachable-runtime")
    return release


def _seal_release_fixture(release: Path) -> Path:
    inventory: list[dict[str, object]] = []
    for path in sorted(item for item in release.rglob("*") if item.is_file()):
        relative = path.relative_to(release).as_posix()
        if relative.casefold() == "bootstrap-integrity.json":
            continue
        payload = path.read_bytes()
        inventory.append(
            {
                "path": relative,
                "size": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        )
    aggregate = hashlib.sha256(
        "".join(
            f"{row['sha256']} {row['size']} {row['path']}\n" for row in inventory
        ).encode("utf-8")
    )
    record_path = release / "bootstrap-integrity.json"
    record_path.write_text(
        json.dumps(
            {
                "schema_version": "label-match-bootstrap-integrity-v1",
                "status": "PASS",
                "code_root": ".",
                "installed_at": "2026-08-28T00:00:00Z",
                "file_count": len(inventory),
                "aggregate_sha256": aggregate.hexdigest(),
                "files": inventory,
                "identity_profile_created": False,
                "state_scope": "current_user_first_run",
            },
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    return record_path


def _private_ca_pem() -> bytes:
    private_key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, "Label Match Test Private CA")]
    )
    now = datetime.now(timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .sign(private_key, hashes.SHA256())
    )
    return certificate.public_bytes(serialization.Encoding.PEM)


def test_bootstrap_is_minimal_code_only_onedir_contract():
    text = INSTALLER.read_text(encoding="utf-8")

    assert "label-match-bootstrap-integrity-v1" in text
    assert "tools\\bootstrap_integrity.ps1" in text
    assert "identity_profile_created=false" in text
    assert "elevation_points=1:code_placement" in text
    assert "Write-BootstrapIntegrityRecord" in text
    assert "Set-HardenedCodeAcl" in text
    assert "Assert-HardenedCodeAcl" in text
    assert "'/setowner', '*S-1-5-32-544'" in text
    assert "'/reset', '/L'" in text
    assert "acl_readback_status=UNKNOWN" in text
    reuse_index = text.index("$bootstrapStatus = 'REUSED'")
    final_acl_index = text.index("Set-HardenedCodeAcl $installRootFull -Recursive")
    success_index = text.index('Write-Output "bootstrap_status=$bootstrapStatus"')
    assert reuse_index < final_acl_index < success_index
    assert "Register-ScheduledTask" not in text
    assert "New-ScheduledTask" not in text
    assert "Start-ScheduledTask" not in text
    assert "self-enroll" not in text
    assert "SourceHostId" not in text
    assert "Remove-OwnedLegacyTask" in text
    elevation = text.index("Assert-AlreadyElevated")
    uninstall = text.index("if ($Uninstall.IsPresent)")
    assert elevation < uninstall
    assert "-Verb RunAs" not in text
    assert "-File $BootstrapScriptPath" not in text
    assert "Test-CurrentUserRelayPersistencePresent" in text
    assert "--remove-current-user-setup" in text


def test_bootstrap_powershell_parses():
    escaped = str(INSTALLER).replace("'", "''")
    completed = subprocess.run(
        [
            _powershell(),
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            (
                "$tokens=$null;$errors=$null;"
                "[void][System.Management.Automation.Language.Parser]::ParseFile("
                f"'{escaped}',[ref]$tokens,[ref]$errors);"
                "if($errors.Count){$errors|ForEach-Object{$_.ToString()};exit 1}"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout


@pytest.mark.parametrize(
    ("scenario", "message_fragment", "expected_unregister_calls"),
    [
        ("query_error", "observation failed", 0),
        ("duplicate", "non-unique", 0),
        ("readback_error", "observation failed", 1),
        ("still_present", "removal readback failed", 1),
    ],
)
def test_legacy_task_removal_fails_closed_without_host_mutation(
    tmp_path,
    scenario,
    message_fragment,
    expected_unregister_calls,
):
    result = _run_legacy_task_removal_harness(tmp_path, scenario)

    assert result["status"] == "FAIL"
    assert message_fragment in result["message"]
    assert result["unregister_calls"] == expected_unregister_calls
    assert result["stop_calls"] == expected_unregister_calls
    if scenario != "query_error":
        assert result["row_type"] == (
            "System.Collections.Specialized.OrderedDictionary"
        )
    if expected_unregister_calls:
        assert result["stop_task_path"] == "\\Owned\\"
        assert result["unregister_task_path"] == "\\Owned\\"


def test_legacy_task_absence_is_the_only_clean_noop(tmp_path):
    result = _run_legacy_task_removal_harness(tmp_path, "absent")

    assert result == {
        "status": "PASS",
        "message": "",
        "query_calls": 1,
        "stop_calls": 0,
        "unregister_calls": 0,
        "row_type": "",
        "stop_task_path": "",
        "unregister_task_path": "",
    }


def test_bootstrap_dry_run_does_not_create_identity_profile_or_target(tmp_path):
    source = _release_fixture(tmp_path)
    install = tmp_path / "apps" / "current"

    completed = _run_installer(source, install, "-DryRun")

    assert completed.returncode == 0, completed.stderr
    assert "bootstrap_status=DRY_RUN" in completed.stdout
    assert "identity_profile_created=false" in completed.stdout
    assert "elevation_points=1:code_placement" in completed.stdout
    assert not install.exists()
    assert list(tmp_path.rglob("producer_identity.json")) == []
    assert list(tmp_path.rglob("runtime-profile.json")) == []


def test_bootstrap_places_exact_onedir_bytes_records_integrity_and_reuses(tmp_path):
    source = _release_fixture(tmp_path)
    install = tmp_path / "apps" / "current"

    first = _run_installer(source, install)
    second = _run_installer(source, install)

    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    assert "bootstrap_status=PASS" in first.stdout
    assert "bootstrap_status=REUSED" in second.stdout
    assert "acl_readback_status=NOT_TESTED" in first.stdout
    assert "acl_readback_status=NOT_TESTED" in second.stdout
    record = json.loads((install / "bootstrap-integrity.json").read_text(encoding="utf-8"))
    assert record["schema_version"] == "label-match-bootstrap-integrity-v1"
    assert record["status"] == "PASS"
    assert record["identity_profile_created"] is False
    assert record["state_scope"] == "current_user_first_run"
    assert record["file_count"] == 3
    assert len(record["files"]) == 3
    assert len(record["aggregate_sha256"]) == 64
    assert (install / "bootstrap-integrity.json").stat().st_size < 4096
    for relative_path in (
        "Label_Match.exe",
        "contract.lock.json",
        "_internal/python312.dll",
    ):
        assert (install / relative_path).read_bytes() == (source / relative_path).read_bytes()
    (install / "_internal" / "python312.dll").write_bytes(b"tampered")
    damaged = _run_installer(source, install)
    assert damaged.returncode != 0
    assert "different or damaged hardened code placement" in (
        damaged.stderr + damaged.stdout
    )


def test_bootstrap_reseals_current_source_and_excludes_packaged_record(tmp_path):
    source = _release_fixture(tmp_path)
    source_record = _seal_release_fixture(source)
    first_root = tmp_path / "apps" / "first"
    first = _run_installer(source, first_root)

    assert first.returncode == 0, first.stderr or first.stdout
    first_record = json.loads(
        (first_root / "bootstrap-integrity.json").read_text(encoding="utf-8")
    )
    assert first_record["code_root"] == str(first_root.resolve())
    assert all(row["path"] != source_record.name for row in first_record["files"])

    (source / "_internal" / "python312.dll").write_bytes(b"tampered-source")
    second_root = tmp_path / "apps" / "second"
    second = _run_installer(source, second_root)

    assert second.returncode == 0, second.stderr or second.stdout
    second_record = json.loads(
        (second_root / "bootstrap-integrity.json").read_text(encoding="utf-8")
    )
    assert second_record["aggregate_sha256"] != first_record["aggregate_sha256"]


def test_bootstrap_reuse_rejects_invalid_integrity_aggregate(tmp_path):
    source = _release_fixture(tmp_path)
    install = tmp_path / "apps" / "current"
    first = _run_installer(source, install)
    assert first.returncode == 0, first.stderr or first.stdout
    record_path = install / "bootstrap-integrity.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    record["aggregate_sha256"] = "0" * 64
    record_path.write_text(json.dumps(record), encoding="utf-8")

    reused = _run_installer(source, install)

    assert reused.returncode != 0
    assert "different or damaged hardened code placement" in (
        reused.stderr + reused.stdout
    )


def test_bootstrap_reuse_rejects_oversized_integrity_record(tmp_path):
    source = _release_fixture(tmp_path)
    install = tmp_path / "apps" / "current"
    first = _run_installer(source, install)
    assert first.returncode == 0, first.stderr or first.stdout
    (install / "bootstrap-integrity.json").write_bytes(b" " * (1024 * 1024 + 1))

    reused = _run_installer(source, install)

    assert reused.returncode != 0
    assert "different or damaged hardened code placement" in (
        reused.stderr + reused.stdout
    )


def test_bootstrap_record_has_complete_inventory_for_4354_file_onedir(tmp_path):
    source = _release_fixture(tmp_path)
    runtime = source / "_internal" / "runtime"
    runtime.mkdir()
    for index in range(4351):
        (runtime / f"member-{index:04d}.bin").write_bytes(
            f"runtime-member-{index}".encode("ascii")
        )
    install = tmp_path / "apps" / "current"

    completed = _run_installer(source, install, timeout=180)

    assert completed.returncode == 0, completed.stderr or completed.stdout
    record_path = install / "bootstrap-integrity.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["file_count"] == 4354
    assert len(record["files"]) == 4354
    assert record_path.stat().st_size < 2 * 1024 * 1024


def test_bootstrap_copies_opt_in_tls_ca_for_current_user_onboarding(tmp_path):
    source = _release_fixture(tmp_path)
    install = tmp_path / "apps" / "current"
    local_app_data = tmp_path / "operator" / "LocalAppData"
    ca_source = tmp_path / "operator stage" / "private-ca.cert.pem"
    ca_source.parent.mkdir(parents=True)
    ca_payload = _private_ca_pem()
    ca_source.write_bytes(ca_payload)

    completed = _run_installer(
        source,
        install,
        "-TlsCaBundlePath",
        str(ca_source),
        "-OperatorLocalAppDataRoot",
        str(local_app_data),
    )

    expected = (
        local_app_data / "KMTech" / "Bootstrap" / "Label_Match" / "ca-bundle.pem"
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert "tls_ca_bootstrap_status=PASS" in completed.stdout
    assert f"tls_ca_bootstrap_path={expected}" in completed.stdout
    assert expected.read_bytes() == ca_payload


def test_bootstrap_inverse_removes_only_code_and_preserves_user_state(tmp_path):
    source = _release_fixture(tmp_path)
    install = tmp_path / "apps" / "current"
    user_state = tmp_path / "LocalAppData" / "KMTech" / "Label_Match" / "ledger.db"
    user_state.parent.mkdir(parents=True)
    user_state.write_bytes(b"preserve-me")
    assert _run_installer(source, install).returncode == 0

    removed = _run_installer(source, install, "-Uninstall")

    assert removed.returncode == 0, removed.stderr
    assert "uninstall_status=PASS_CODE_REMOVED_STATE_PRESERVED" in removed.stdout
    assert not install.exists()
    assert user_state.read_bytes() == b"preserve-me"

import json
import hashlib
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
import shutil
import subprocess
from types import SimpleNamespace

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from current_user_onboarding import verify_bootstrap_integrity


ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / "INSTALL_THIS_PC.ps1"


def _powershell() -> str:
    executable = shutil.which("powershell.exe")
    if not executable:
        pytest.skip("Windows PowerShell is required")
    return executable


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
    inventory = []
    for path in sorted(item for item in release.rglob("*") if item.is_file()):
        relative = path.relative_to(release).as_posix()
        if relative.casefold() == "bootstrap-integrity.json":
            continue
        payload = path.read_bytes()
        inventory.append((relative, len(payload), hashlib.sha256(payload).hexdigest()))
    entries = sorted(
        f"{digest} {size} {relative.encode('utf-8').hex()}\n".encode("ascii")
        for relative, size, digest in inventory
    )
    root_hash = hashlib.sha256(b"label-match-code-root-v1\n" + b"".join(entries)).hexdigest()
    record_path = release / "bootstrap-integrity.json"
    record_path.write_text(
        json.dumps(
            {
                "schema_version": "label-match-bootstrap-integrity-v2",
                "status": "PASS",
                "code_root": ".",
                "installed_at": "2026-08-28T00:00:00Z",
                "file_count": len(inventory),
                "inventory_algorithm": "sha256-file-hash-size-utf8-path-v1",
                "root_sha256": root_hash,
                "identity_profile_created": False,
                "state_scope": "current_user_first_run",
                "package_layout": "onedir",
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

    assert len(text.splitlines()) <= 500
    assert "label-match-bootstrap-integrity-v2" in text
    assert "sha256-file-hash-size-utf8-path-v1" in text
    assert "identity_profile_created=false" in text
    assert "elevation_points=1:code_placement" in text
    assert "package_layout = 'onedir'" in text
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
    elevation = text.index("Invoke-SelfElevated\n    Remove-OwnedLegacyTask")
    uninstall = text.index("if ($Uninstall.IsPresent)")
    assert elevation < uninstall
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
    assert record["schema_version"] == "label-match-bootstrap-integrity-v2"
    assert record["status"] == "PASS"
    assert record["identity_profile_created"] is False
    assert record["state_scope"] == "current_user_first_run"
    assert record["package_layout"] == "onedir"
    assert record["inventory_algorithm"] == "sha256-file-hash-size-utf8-path-v1"
    assert record["file_count"] == 3
    assert "files" not in record
    assert len(record["root_sha256"]) == 64
    assert (install / "bootstrap-integrity.json").stat().st_size < 1024
    verified = verify_bootstrap_integrity(
        SimpleNamespace(
            app_root=install.resolve(),
            bootstrap_integrity_path=install / "bootstrap-integrity.json",
        ),
        required=True,
    )
    assert verified["root_sha256"] == record["root_sha256"]
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


def test_bootstrap_validates_packaged_record_before_resealing_install(tmp_path):
    source = _release_fixture(tmp_path)
    _seal_release_fixture(source)
    first = _run_installer(source, tmp_path / "apps" / "first")

    assert first.returncode == 0, first.stderr or first.stdout
    assert "source_integrity_status=PASS" in first.stdout

    (source / "_internal" / "python312.dll").write_bytes(b"tampered-source")
    blocked = _run_installer(source, tmp_path / "apps" / "tampered")

    assert blocked.returncode != 0
    assert "source code root" in (blocked.stderr + blocked.stdout)


@pytest.mark.parametrize(
    ("field", "invalid_value"),
    [
        ("schema_version", "label-match-bootstrap-integrity-v1"),
        ("status", "FAILED"),
        ("package_layout", "onefile"),
        ("inventory_algorithm", "other"),
        ("file_count", "3"),
        ("code_root", r"C:\wrong-root"),
        ("files", []),
    ],
)
def test_bootstrap_reuse_rejects_invalid_integrity_metadata(
    tmp_path, field, invalid_value
):
    source = _release_fixture(tmp_path)
    install = tmp_path / "apps" / "current"
    first = _run_installer(source, install)
    assert first.returncode == 0, first.stderr or first.stdout
    record_path = install / "bootstrap-integrity.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    record[field] = invalid_value
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


def test_bootstrap_record_stays_bounded_for_4354_file_onedir(tmp_path):
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
    assert "files" not in record
    assert record_path.stat().st_size < 1024


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

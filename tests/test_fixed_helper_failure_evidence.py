"""Verify failures remain protected and reachable before TLS/enrollment."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
from types import SimpleNamespace

import pytest

import auto_update
import current_user_onboarding

ROOT = Path(__file__).resolve().parents[1]


def _fresh_verify(tmp_path, engine, corruption=None, *, native_case="legacy"):
    current = tmp_path / "managed/current"
    for leaf in ("tools", "runtime", "app/tools", "../update-control"):
        (current / leaf).mkdir(parents=True, exist_ok=True)
    quote = lambda path: "'" + str(path).replace("'", "''") + "'"
    helper = current / "tools/apply_prepared_update.ps1"
    helper.write_bytes((ROOT / "tools/apply_prepared_update.ps1").read_bytes())
    # The whole Verify script runs in a new native process. Only platform
    # acceptance (ACL, installed receipt, signature) has controlled providers;
    # none of the installer's private functions enters the child scope.
    portable = current / "tools/portable-dependency.ps1"
    portable.write_text(". " + quote(ROOT / "kmtech_shared/powershell/portable.ps1") +
        "\nfunction Assert-KmtechCPythonSignature {param($Root,$UnsignedOk)}\n", encoding="utf-8-sig")
    bootstrap = current / "tools/bootstrap_integrity.ps1"
    bootstrap.write_text(". " + quote(ROOT / "tools/bootstrap_integrity.ps1") + "\n" +
        "function Protect-KmtechCodeAncestors {param($ApplicationRoot,[switch]$AdmissionOnly,[switch]$RequireProtected)}\n" +
        "function Get-LabelSharedPortableLeafPath {param($CodeRoot) return " + quote(portable) + "}\n" +
        "function Assert-BootstrapIntegrityRecord {param($Root) if(-not (Test-Path -LiteralPath (Join-Path $Root 'bootstrap-integrity.json'))){throw 'Missing controlled receipt'}}\n",
        encoding="utf-8-sig")
    redactor = current / "app/tools/failure_redaction.ps1"
    redactor.write_text(". " + quote(ROOT / "tools/failure_redaction.ps1") + "\n", encoding="utf-8-sig")
    pins = {"launch-label-match.cmd": "launcher_sha256", "INSTALL_CANONICAL_PORTABLE.ps1": "canonical_installer_sha256",
            "INSTALL_THIS_PC.ps1": "helper_sha256", "tools/bootstrap_integrity.ps1": "integrity_helper_sha256",
            "tools/label_writer_fence.ps1": "writer_fence_helper_sha256", "tools/label_writer_fence_contract.json": "writer_session_contract_sha256",
            "tools/apply_prepared_update.ps1": "startup_update_helper_sha256", "app/update-manifest-key-config.json": "update_key_config_sha256",
            "runtime/pythonw.exe": "runtime_pythonw_sha256"}
    manifest = {"schema": "label-match-portable-tree-v1", "startup_update_protocol": 1, "source_commit": "a" * 40,
                "entrypoint": "runtime/pythonw.exe app/main.py", "allowed_unsigned_app_pe": [], "forbidden_package_roots": [],
                "writer_sink_inventory_contract_sha256": "b" * 64}
    from tools import build_portable_release_candidate as builder
    natives = sorted(builder.APP_NATIVE_UNSIGNED)
    if native_case != "legacy":
        for relative in natives:
            destination = current / "app" / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(builder.SHELL_RUNTIME_SITE_PACKAGES / Path(relative).relative_to("site-packages"), destination)
            assert hashlib.sha256(destination.read_bytes()).hexdigest() == builder.APP_NATIVE_ALLOWLIST[relative][1]
        manifest["allowed_unsigned_app_pe"] = natives.copy()
        if native_case == "unknown":
            manifest["allowed_unsigned_app_pe"][0] = "site-packages/unknown.dll"
        elif native_case == "extra":
            manifest["allowed_unsigned_app_pe"].append("site-packages/extra.dll")
        elif native_case == "duplicate":
            manifest["allowed_unsigned_app_pe"][0] = natives[1]
        elif native_case == "partial":
            manifest["allowed_unsigned_app_pe"].pop()
        elif native_case == "missing":
            (current / "app" / natives[0]).unlink()
        elif native_case == "tampered":
            path = current / "app" / natives[0]
            raw = bytearray(path.read_bytes())
            raw[-1] ^= 1
            path.write_bytes(raw)
        elif native_case == "fake":
            for relative in natives:
                (current / "app" / relative).write_bytes(b"MZ untrusted native")
        elif native_case == "escape":
            manifest["allowed_unsigned_app_pe"][0] = "../" + natives[0]
        elif native_case == "case":
            manifest["allowed_unsigned_app_pe"][0] = natives[0].upper()
        elif native_case == "backslash":
            manifest["allowed_unsigned_app_pe"][0] = natives[0].replace("/", "\\")
        else:
            assert native_case == "stock"
    for leaf, field in pins.items():
        path = current / leaf
        if not path.exists():
            path.write_bytes(b"controlled-read-only-fixture")
        manifest[field] = hashlib.sha256(path.read_bytes()).hexdigest()
    if corruption == "manifest_pin":
        manifest["helper_sha256"] = "f" * 64
    (current / "portable-manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    for leaf in ("runtime/python.exe", "app/main.py", "bootstrap-integrity.json"):
        (current / leaf).write_text("controlled-fixture", encoding="utf-8")
    if corruption == "integrity_record":
        (current / "bootstrap-integrity.json").unlink()
    (current / "app/writer_session_fence.py").write_text('WRITER_INVENTORY_SHA256 = "' + "b" * 64 + '"\n', encoding="utf-8")
    boundary = tmp_path / "native-boundary.psm1"
    boundary.write_text(r'''function Get-Acl {
param($LiteralPath)
$acl=[Security.AccessControl.DirectorySecurity]::new()
$acl.SetOwner([Security.Principal.SecurityIdentifier]::new('S-1-5-32-544'))
$acl.SetAccessRuleProtection($true,$false)
foreach($sid in @('S-1-5-18','S-1-5-32-544')){$acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new([Security.Principal.SecurityIdentifier]::new($sid),'FullControl','Allow'))}
return $acl
}
Export-ModuleMember -Function Get-Acl
''', encoding="utf-8-sig")
    driver = tmp_path / "fresh-child.ps1"
    policy = current.parent / "update-control/policy.json"
    driver.write_text("$ErrorActionPreference='Stop'\n$env:TEMP=" + quote(tmp_path) + "\n$env:TMP=$env:TEMP\nImport-Module " + quote(boundary) + "\n" +
        "@{operator_sid=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value}|ConvertTo-Json|Set-Content -LiteralPath " + quote(policy) + " -Encoding UTF8\n" +
        "& " + quote(helper) + " -InstallRoot " + quote(current) + " -Mode Verify -CodeRoot " + quote(current) + "\nexit $LASTEXITCODE\n",
        encoding="utf-8-sig")
    environment = os.environ.copy()
    environment.update(TEMP=str(tmp_path), TMP=str(tmp_path))
    result = subprocess.run([shutil.which(engine), "-NoLogo", "-NoProfile", "-NonInteractive", "-WindowStyle", "Hidden", "-File", str(driver)],
                            cwd=tmp_path, env=environment, capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    (tmp_path / "full-verify.stdout").write_bytes(result.stdout)
    (tmp_path / "full-verify.stderr").write_bytes(result.stderr)
    result.driver = driver
    return result, current


@pytest.mark.parametrize("engine", ("powershell.exe", "pwsh.exe"))
@pytest.mark.parametrize("corruption", (None, "manifest_pin", "integrity_record"))
@pytest.mark.parametrize("native_case", ("legacy", "stock"))
def test_full_verify_in_fresh_child_with_only_dependency_modules(tmp_path, engine, corruption, native_case):
    result, _ = _fresh_verify(tmp_path, engine, corruption, native_case=native_case)
    if corruption is None:
        assert result.returncode == 0, result.stderr.decode("utf-8", errors="replace")
        assert not result.stderr
    else:
        assert result.returncode == 1
        line = next(line for line in result.stderr.decode("utf-8", errors="replace").splitlines() if line.startswith("LABEL_MATCH_INSTALLER_FAILURE="))
        record = json.loads(line.split("=", 1)[1])
        evidence = json.loads(Path(record["report_path"]).read_bytes())
        if corruption == "manifest_pin":
            assert evidence["check"]["check"] == "MANIFEST_FILE_PIN"
            assert evidence["check"]["expected_sha256"] == "f" * 64
            assert evidence["check"]["actual_sha256"] != "f" * 64
        else:
            assert evidence["check"]["check"] == "BOOTSTRAP_INTEGRITY"
            assert evidence["check"]["record_present"] is False
            assert evidence["check"]["expected_status"] == "PASS"
            assert "Missing controlled receipt" in evidence["trace"]


@pytest.mark.parametrize("engine", ("powershell.exe", "pwsh.exe"))
@pytest.mark.parametrize("native_case", ("stock", "legacy", "unknown", "extra", "duplicate", "partial",
                                       "missing", "tampered", "fake", "escape", "case", "backslash"))
def test_fresh_verify_native_stock_and_refusals(tmp_path, engine, native_case):
    result, _ = _fresh_verify(tmp_path, engine, native_case=native_case)
    if native_case in {"stock", "legacy"}:
        assert result.returncode == 0
        assert not result.stderr
    else:
        assert result.returncode == 1
        line = next(line for line in result.stderr.decode("utf-8", errors="replace").splitlines()
                    if line.startswith("LABEL_MATCH_INSTALLER_FAILURE="))
        record = json.loads(line.split("=", 1)[1])
        evidence = json.loads(Path(record["report_path"]).read_bytes())
        assert evidence["check"]["check"] == "MANIFEST"
        assert "STARTUP_UPDATE_PORTABLE_MANIFEST_DIFFERS" in evidence["trace"]


@pytest.mark.parametrize("engine", ("powershell.exe", "pwsh.exe"))
def test_early_native_acl_verify_failure_has_typed_protected_sidecar(tmp_path, engine):
    environment = os.environ.copy()
    environment.update(TEMP=str(tmp_path), TMP=str(tmp_path))
    quote = lambda path: "'" + str(path).replace("'", "''") + "'"
    driver = tmp_path / "early-acl.ps1"
    driver.write_text("$env:TEMP=" + quote(tmp_path) + "\n$env:TMP=$env:TEMP\n& " +
        quote(ROOT / "tools/apply_prepared_update.ps1") + " -InstallRoot " + quote(tmp_path / "current") +
        " -Mode Verify -CodeRoot " + quote(tmp_path / "current") + "\nexit $LASTEXITCODE\n", encoding="utf-8-sig")
    result = subprocess.run([shutil.which(engine), "-NoLogo", "-NoProfile", "-NonInteractive",
        "-WindowStyle", "Hidden", "-File", str(driver)],
        cwd=tmp_path, env=environment, capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    (tmp_path / "native.stdout").write_bytes(result.stdout)
    (tmp_path / "native.stderr").write_bytes(result.stderr)
    assert result.returncode == 1
    lines = result.stderr.decode("utf-8", errors="replace").splitlines()
    record = json.loads(next(line.split("=", 1)[1] for line in lines if line.startswith("LABEL_MATCH_INSTALLER_FAILURE=")))
    assert record["reason_code"] == "INSTALLER_FAILED"
    assert "report_path" in record
    path = Path(record["report_path"])
    raw = path.read_bytes()
    assert path.is_relative_to(tmp_path)
    assert hashlib.sha256(raw).hexdigest() == record["report_sha256"]
    evidence = json.loads(raw)
    assert evidence["schema"] == "label-match-startup-verify-failure-v1"
    assert evidence["mode"] == "Verify"
    assert evidence["check"]["check"] == "ACL"
    assert evidence["check"]["owner_sid"].startswith("S-1-")
    assert type(evidence["check"]["inheritance_protected"]) is bool
    assert "owner_sid" not in result.stderr.decode("utf-8", errors="replace")


def test_fixed_helper_and_onboarding_keep_original_child_report(tmp_path, monkeypatch, capsys):
    original = tmp_path / "verify-check.json"
    original.write_text(json.dumps({"check": "MANIFEST_FILE_PIN", "expected_sha256": "a" * 64,
                                    "actual_sha256": "b" * 64}), encoding="utf-8")
    child = {"status": "FAILED", "reason_code": "INSTALLER_FAILED", "report_path": str(original),
             "report_sha256": hashlib.sha256(original.read_bytes()).hexdigest()}
    completed = SimpleNamespace(returncode=1, stdout="", stderr="LABEL_MATCH_INSTALLER_FAILURE=" + json.dumps(child))
    monkeypatch.setattr(subprocess, "run", lambda *_a, **_k: completed)
    # The real writer guard is outside this child-output contract and is covered
    # by the native parent/child rollback regression.
    def fail_before_tls(*_args, **_kwargs):
        auto_update.fixed_helper.__wrapped__(tmp_path / "update-control", "Verify", code_root=tmp_path / "current")
    monkeypatch.setattr(current_user_onboarding, "onboard_current_user", fail_before_tls)
    monkeypatch.setattr(current_user_onboarding, "requires_bootstrap_integrity", lambda *_: False)
    assert current_user_onboarding.onboarding_main(["--app-root", str(tmp_path)]) == 4
    captured = capsys.readouterr()
    record = json.loads(captured.err.split("LABEL_MATCH_INSTALLER_FAILURE=", 1)[1])
    assert record["stage"] == "CURRENT_USER_ONBOARDING"
    assert record["reason_code"] == "INSTALLER_FAILED"
    report = Path(record["report_path"])
    assert hashlib.sha256(report.read_bytes()).hexdigest() == record["report_sha256"]
    payload = json.loads(report.read_text(encoding="utf-8"))
    assert payload["mode"] == "Verify"
    nested = json.loads(payload["child_output"]["stderr"].split("=", 1)[1])
    assert nested["report_path"] == str(original)
    assert nested["report_sha256"] == child["report_sha256"]
    assert "Traceback" not in captured.out + captured.err
    assert "actual_sha256" not in captured.out + captured.err

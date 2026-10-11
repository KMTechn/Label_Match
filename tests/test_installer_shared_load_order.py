"""Fresh installer admission must work before pinned shared code is imported."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest

from tests._installer_host_isolation import host_program_data_guard
from tests._shared_portable_fixture import shared_bootstrap_files
from tests.test_shared_ancestor_protection import run_functions


ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / "INSTALL_CANONICAL_PORTABLE.ps1"


def _audit_load_order(tmp_path, engine, source, library_root, mutation=""):
    audit_path = tmp_path / ("pre-shared-load-" + (mutation or "actual") + ".json")
    environment = os.environ.copy()
    environment.update({"KMTECH_FACTORY_INSTALL_TEST_MODE": "1", "PROGRAMDATA": str(tmp_path / "program-data"),
                        "TEMP": str(tmp_path), "TMP": str(tmp_path)})
    result = subprocess.run(
        [shutil.which(engine), "-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
         "-File", str(ROOT / "tests/installer_load_order_audit.ps1"), "-Source", str(source),
         "-LibraryRoot", str(library_root), "-AuditPath", str(audit_path), "-Mutation", mutation],
        cwd=tmp_path, env=environment, capture_output=True, encoding="utf-8", errors="replace",
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    assert audit_path.exists(), "Static audit did not complete; isolated output retained"
    return result, json.loads(audit_path.read_text(encoding="utf-8-sig"))


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
def test_pre_shared_load_call_graph_has_no_shared_only_dependency(tmp_path, engine, host_program_data_guard):
    if not shutil.which(engine):
        pytest.skip(f"{engine} is required")
    host_program_data_guard.assert_unchanged()
    result, audit = _audit_load_order(tmp_path, engine, INSTALLER, ROOT)
    assert result.returncode == 0, "pre-import call graph has an unavailable dependency; audit retained"
    assert {row["scenario"] for row in audit["frontiers"]} == {
        "no-arguments", "plan-only", "worker-inventory", "worker-evidence", "worker-evidence-plan-only",
        "machine-recovery", "restore", "retired-removal", "rollback-resume"}
    assert all(row["dependencies"] == [] for row in audit["frontiers"])
    assert {row["leaf"] for row in audit["frontiers"]} >= {"portable", "data"}
    mutant, changed = _audit_load_order(tmp_path, engine, INSTALLER, ROOT, "portable")
    assert mutant.returncode == 1, "Audit failed to detect an injected early shared helper call"
    assert any("ConvertTo-KmtechFullPath" in row["dependencies"] for row in changed["frontiers"])


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("caller", ["same-worker", "other-worker"])
def test_explicit_worker_evidence_plan_only_in_fresh_process(tmp_path, engine, caller, host_program_data_guard):
    executable = shutil.which(engine)
    if not executable:
        pytest.skip(f"{engine} is required")
    packet = tmp_path / "portable"
    profile = tmp_path / "worker-profile"
    program_data = tmp_path / "program-data"
    local = profile / "AppData/Local"
    inventory = tmp_path / "inventory.json"
    evidence = program_data / "KMTech/Label_Match/install-audit/plan.json"
    install = tmp_path / "machine/current"
    worker_sid = "S-1-5-21-" + "-".join(map(str, (100, 200, 300, 1001)))
    caller_sid = worker_sid if caller == "same-worker" else worker_sid[:-1] + "2"
    inventory.write_text(json.dumps({"status": "READY", "target_sid": worker_sid,
                                    "target_profile_root": str(profile)}), encoding="utf-8")

    # Keep the complete real installer and its pinned shared leaves. Only native
    # caller/ProfileList observations use the isolated synthetic worker profile.
    product = INSTALLER.read_text(encoding="utf-8-sig")
    caller_start = product.index("function Get-LabelInstallerCaller {")
    caller_end = product.index("function Resolve-LabelWorkerSid", caller_start)
    product = (product[:caller_start] + "function Get-LabelInstallerCaller {\n"
               " return [pscustomobject]@{sid=$env:LM_PLAN_CALLER;session_id=0;elevated=$true}\n}\n\n"
               + product[caller_end:])
    profile_start = product.index("function Get-LabelWorkerProfileRoot(")
    profile_end = product.index("function Assert-LabelMachineEvidencePath", profile_start)
    product = (product[:profile_start] + "function Get-LabelWorkerProfileRoot([string]$Sid) {\n"
               " return $env:LM_PLAN_PROFILE\n}\n\n" + product[profile_end:])
    runtime = b"PlanOnly must never execute this runtime fixture.\n"
    launcher = b"@echo off\r\nexit /b 99\r\n"
    files = {
        "INSTALL_CANONICAL_PORTABLE.ps1": product.encode("utf-8-sig"),
        "INSTALL_THIS_PC.ps1": (ROOT / "INSTALL_THIS_PC.ps1").read_bytes(),
        "tools/bootstrap_integrity.ps1": (ROOT / "tools/bootstrap_integrity.ps1").read_bytes(),
        "tools/label_writer_fence.ps1": (ROOT / "tools/label_writer_fence.ps1").read_bytes(),
        "runtime/python.exe": runtime,
        "runtime/pythonw.exe": runtime,
        "app/main.py": b"raise SystemExit('PlanOnly must never start the application')\n",
        "launch-label-match.cmd": launcher,
        **shared_bootstrap_files("app/"),
    }
    manifest = {"schema": "label-match-portable-tree-v1", "version": "v2.0.99",
                "entrypoint": "runtime/pythonw.exe app/main.py", "launcher": "launch-label-match.cmd",
                "allowed_unsigned_app_pe": [], "forbidden_package_roots": [], "startup_update_protocol": 1,
                "runtime_pythonw_sha256": hashlib.sha256(runtime).hexdigest(),
                "launcher_sha256": hashlib.sha256(launcher).hexdigest()}
    files["portable-manifest.json"] = json.dumps(manifest).encode("utf-8")
    for relative, contents in files.items():
        path = packet / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(contents)
    temporary = tmp_path / "temp"
    temporary.mkdir()
    environment = os.environ.copy()
    environment.update({"KMTECH_FACTORY_INSTALL_TEST_MODE": "1", "PROGRAMDATA": str(program_data),
                        "LOCALAPPDATA": str(local), "APPDATA": str(profile / "AppData/Roaming"),
                        "TEMP": str(temporary), "TMP": str(temporary),
                        "LM_PLAN_PROFILE": str(profile), "LM_PLAN_CALLER": caller_sid})
    for name in ("KMTECH_LABEL_CONFLICT_RESOLUTION_RECEIPT_PATH", "KMTECH_LABEL_CONFLICT_RESOLUTION_RECEIPT_SHA256"):
        environment.pop(name, None)

    # Observe the original native failure before Resolve wraps it in the public
    # PENDING_TARGET_INVALID record. This child loads no shared path helper.
    probe = run_functions(tmp_path, INSTALLER, ["Full", "Assert-LabelMachineEvidencePath"], r'''
$testMode=$true;$EvidencePath=$env:LM_PLAN_EVIDENCE
try{Assert-LabelMachineEvidencePath;$cause=''}catch{$cause=$_.Exception.GetBaseException().GetType().Name}
@{exception_type=$cause;engine_version=$PSVersionTable.PSVersion.ToString()}|ConvertTo-Json -Compress
''', engine=engine, values={"PROGRAMDATA": str(program_data), "LM_PLAN_EVIDENCE": str(evidence)})
    assert probe.returncode == 0, "isolated admission cause probe failed"
    cause = json.loads(probe.stdout)
    before = {p.relative_to(tmp_path): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    directories_before = {p.relative_to(tmp_path) for p in tmp_path.rglob("*") if p.is_dir()}
    host_program_data_guard.assert_unchanged()
    result = subprocess.run(
        [executable, "-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
         "-File", str(packet / INSTALLER.name), "-SourceRoot", str(packet), "-InstallRoot", str(install),
         "-WorkerAccount", worker_sid, "-WorkerSurveyInventory", str(inventory), "-EvidencePath", str(evidence),
         "-PlanOnly", "-AllowNoncanonicalLayoutForTest", "-SkipSignatureValidationForTest"],
        cwd=tmp_path, env=environment, capture_output=True, encoding="utf-8", errors="replace",
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    host_program_data_guard.assert_unchanged()
    after = {p.relative_to(tmp_path): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    directories_after = {p.relative_to(tmp_path) for p in tmp_path.rglob("*") if p.is_dir()}
    failure = next((line.split("=", 1)[1] for line in result.stderr.splitlines()
                    if line.startswith("LABEL_MATCH_INSTALLER_FAILURE=")), "{}")
    summary = {"exit_code": result.returncode, "reason_code": json.loads(failure).get("reason_code", ""),
               "admission_exception_type": cause["exception_type"], "engine_version": cause["engine_version"],
               "files_unchanged": before == after, "directories_unchanged": directories_before == directories_after}
    (tmp_path / "fresh-plan-result.json").write_text(json.dumps(summary), encoding="utf-8")
    assert result.returncode == 0, "fresh worker/evidence PlanOnly failed; isolated result retained"
    assert cause["exception_type"] == "", "admission still requires an unloaded helper"
    assert result.stderr == ""
    assert "install_status=PLAN_ONLY" in result.stdout
    assert "registry_changed=false" in result.stdout
    assert before == after, "PlanOnly changed the isolated files"
    assert directories_before == directories_after, "PlanOnly allocated a directory"
    assert not profile.exists() and not program_data.exists() and not install.exists()

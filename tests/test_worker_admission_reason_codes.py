"""Worker refusals publish fixed reasons before imports or installation writes."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import uuid

import pytest

from tests._installer_host_isolation import HostProgramDataGuard


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def host_program_data_guard():
    guard = HostProgramDataGuard()
    yield guard
    guard.assert_unchanged()


CASES = [
    ("ordinary-args", "WORKER_ARGS_REQUIRE_ADMIN"),
    ("unelevated-session-zero", "WORKER_ARGS_REQUIRE_ADMIN"),
    ("missing-account", "WORKER_ARGS_INCOMPLETE"),
    ("missing-inventory", "WORKER_ARGS_INCOMPLETE"),
    ("account-translate", "WORKER_ACCOUNT_UNRESOLVED"),
    ("account-sid-malformed", "WORKER_ACCOUNT_UNRESOLVED"),
    ("sid-unsupported", "WORKER_SID_UNSUPPORTED"),
    ("sid-nonstring", "WORKER_SID_UNSUPPORTED"),
    ("inventory-relative", "INVENTORY_PATH_INVALID"),
    ("inventory-absent", "INVENTORY_FILE_INVALID"),
    ("inventory-directory", "INVENTORY_FILE_INVALID"),
    ("inventory-empty", "INVENTORY_FILE_INVALID"),
    ("inventory-oversized", "INVENTORY_FILE_INVALID"),
    ("inventory-grown", "INVENTORY_FILE_INVALID"),
    ("inventory-json", "INVENTORY_DECODE_INVALID"),
    ("inventory-utf8", "INVENTORY_DECODE_INVALID"),
    ("inventory-not-ready", "INVENTORY_BINDING_DIFFERS"),
    ("inventory-other-sid", "INVENTORY_BINDING_DIFFERS"),
    ("inventory-relative-profile", "INVENTORY_BINDING_DIFFERS"),
    ("profile-absent", "WORKER_PROFILE_ABSENT"),
    ("profile-invalid", "WORKER_PROFILE_DIFFERS"),
    ("profile-different", "WORKER_PROFILE_DIFFERS"),
    ("profile-drive-root", "WORKER_PROFILE_DIFFERS"),
    ("evidence-outside", "EVIDENCE_PATH_INVALID"),
    ("evidence-directory", "EVIDENCE_PATH_INVALID"),
    ("evidence-parent-file", "EVIDENCE_PATH_INVALID"),
    ("console-evidence-outside", "EVIDENCE_PATH_INVALID"),
    ("console-absent", "CONSOLE_WORKER_ABSENT"),
    ("console-empty", "CONSOLE_WORKER_ABSENT"),
    ("console-ambiguous", "CONSOLE_WORKER_AMBIGUOUS"),
    ("console-changed", "CONSOLE_WORKER_AMBIGUOUS"),
    ("console-second-absent", "CONSOLE_WORKER_ABSENT"),
    ("console-sid-unsupported", "WORKER_SID_UNSUPPORTED"),
    ("console-profile-invalid", "WORKER_PROFILE_DIFFERS"),
    ("console-profile-root", "WORKER_PROFILE_DIFFERS"),
    ("unclassified", "UNCLASSIFIED"),
    ("poisoned-reason", "UNCLASSIFIED"),
]


def _replace_function(source, name, following, definition):
    start = source.index("function " + name)
    end = source.index("function " + following, start)
    return source[:start] + definition + "\n\n" + source[end:]


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("case, expected", CASES, ids=[case for case, _reason in CASES])
def test_fresh_worker_refusal_exposes_only_fixed_reason(
        tmp_path, engine, case, expected, host_program_data_guard):
    executable = shutil.which(engine)
    if not executable:
        pytest.skip(f"{engine} is required")
    marker = "private-fixture-" + uuid.uuid4().hex
    profile = tmp_path / (marker + "-profile")
    other_profile = tmp_path / (marker + "-other-profile")
    inventory = tmp_path / (marker + "-inventory.json")
    worker_sid = "S-1-5-21-" + "-".join(map(str, (100, 200, 300, 1001)))
    other_sid = worker_sid[:-1] + "2"
    unsupported_sid = "S-1-12-1-" + "-".join(map(str, (100, 200, 300, 400)))
    account = unsupported_sid if case == "sid-unsupported" else worker_sid
    if case == "account-translate":
        account = marker
    if case == "account-sid-malformed":
        account = "S-1-invalid-" + marker
    payload = {"status": "READY", "target_sid": account, "target_profile_root": str(profile)}
    if case == "inventory-not-ready":
        payload["status"] = "BLOCKED"
    if case == "inventory-other-sid":
        payload["target_sid"] = other_sid
    if case == "inventory-relative-profile":
        payload["target_profile_root"] = marker
    if case == "profile-drive-root":
        payload["target_profile_root"] = profile.anchor
    contents = json.dumps(payload).encode()
    if case == "inventory-empty":
        contents = b""
    elif case in {"inventory-oversized", "inventory-grown"}:
        contents = b" " * (1048576 + 1)
    elif case == "inventory-json":
        contents = b"{" + marker.encode()
    elif case == "inventory-utf8":
        contents = b"\xff\xfe" + marker.encode()
    if case == "inventory-directory":
        inventory.mkdir()
    elif case != "inventory-absent":
        inventory.write_bytes(contents)

    product = (ROOT / "INSTALL_CANONICAL_PORTABLE.ps1").read_text(encoding="utf-8-sig")
    product = _replace_function(product, "Get-LabelInstallerCaller {", "Resolve-LabelWorkerSid", r'''
function Get-LabelInstallerCaller {
 $elevated=$env:LM_REASON_CASE -notin @('ordinary-args','unelevated-session-zero')
 $session=if($env:LM_REASON_CASE -ceq 'ordinary-args'){9}else{0}
 return [pscustomobject]@{sid=$env:LM_REASON_SID;session_id=$session;elevated=$elevated}
}''')
    product = _replace_function(product, "Get-LabelWorkerProfileRoot(", "Assert-LabelMachineEvidencePath", r'''
function Get-LabelWorkerProfileRoot([string]$Sid) {
 switch($env:LM_REASON_CASE){
  'profile-absent'{throw $env:LM_REASON_PRIVATE}
  'profile-invalid'{return $env:LM_REASON_PRIVATE}
  'console-profile-invalid'{return $env:LM_REASON_PRIVATE}
  'profile-different'{return $env:LM_REASON_OTHER_PROFILE}
  'profile-drive-root'{return [IO.Path]::GetPathRoot($env:LM_REASON_PROFILE)}
  'console-profile-root'{return [IO.Path]::GetPathRoot($env:LM_REASON_PROFILE)}
 }
 return $env:LM_REASON_PROFILE
}''')
    product = _replace_function(product, "Get-LabelConsoleWorkerObservation {", "Get-LabelConsoleWorker {", r'''
function Get-LabelConsoleWorkerObservation {
 $script:reasonObservation++
 $case=$env:LM_REASON_CASE
 if($case -ceq 'console-absent' -or ($case -ceq 'console-second-absent' -and $script:reasonObservation -eq 2)){
  throw $env:LM_REASON_PRIVATE
 }
 if($case -ceq 'console-empty'){return @()}
 $account=if($case -ceq 'console-sid-unsupported'){$env:LM_REASON_UNSUPPORTED_SID}else{$env:LM_REASON_SID}
 if($case -ceq 'console-changed' -and $script:reasonObservation -eq 2){$account=$env:LM_REASON_OTHER_SID}
 $row=[pscustomobject]@{session_id=7;account=$account}
 if($case -ceq 'console-ambiguous'){return @($row,$row)}
 return $row
}
$script:reasonObservation=0''')
    injection = ""
    if case == "account-translate":
        injection = "function Resolve-LabelWorkerSid([string]$Account){throw $env:LM_REASON_PRIVATE}\n"
    elif case == "sid-nonstring":
        injection = "function Resolve-LabelWorkerSid([string]$Account){return @{}}\n"
    elif case == "inventory-grown":
        injection = r'''
function Get-Item {
 [CmdletBinding()]param([string]$LiteralPath,[switch]$Force)
 if($LiteralPath -ceq $env:LM_REASON_INVENTORY){
  return [pscustomobject]@{PSIsContainer=$false;Attributes=[IO.FileAttributes]::Normal;Length=1;FullName=$LiteralPath}
 }
 Microsoft.PowerShell.Management\Get-Item @PSBoundParameters
}
'''
    elif case in {"unclassified", "poisoned-reason"}:
        injection = r'''
function ByteSha([byte[]]$Bytes){
 $failure=[InvalidOperationException]::new($env:LM_REASON_PRIVATE)
 if($env:LM_REASON_CASE -ceq 'poisoned-reason'){$failure.Data['worker_pending_reason']=$env:LM_REASON_PRIVATE}
 throw $failure
}
'''
    call = "$machineWorker = Resolve-LabelMachineWorker"
    assert product.count(call) == 1
    product = product.replace(call, injection + call)
    driver = tmp_path / "reason-entry.ps1"
    driver.write_text(product, encoding="utf-8-sig")
    program_data = tmp_path / "program-data"
    environment = os.environ.copy()
    environment.update({"KMTECH_FACTORY_INSTALL_TEST_MODE": "1", "PROGRAMDATA": str(program_data),
                        "LOCALAPPDATA": str(tmp_path / "local"), "APPDATA": str(tmp_path / "roaming"),
                        "TEMP": str(tmp_path), "TMP": str(tmp_path), "LM_REASON_CASE": case,
                        "LM_REASON_SID": worker_sid, "LM_REASON_OTHER_SID": other_sid,
                        "LM_REASON_UNSUPPORTED_SID": unsupported_sid,
                        "LM_REASON_PROFILE": str(profile), "LM_REASON_OTHER_PROFILE": str(other_profile),
                        "LM_REASON_INVENTORY": str(inventory), "LM_REASON_PRIVATE": marker})
    command = [executable, "-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
               "-File", str(driver), "-SourceRoot", str(tmp_path / "unused-source"),
               "-InstallRoot", str(tmp_path / "machine/current")]
    if not case.startswith("console-"):
        path = inventory.name if case == "inventory-relative" else str(inventory)
        command += ["-WorkerAccount", "" if case == "missing-account" else account,
                    "-WorkerSurveyInventory", "" if case == "missing-inventory" else path, "-PlanOnly"]
    if "evidence-" in case:
        evidence = program_data / "KMTech/Label_Match/install-audit/evidence.json"
        if case.endswith("evidence-outside"):
            evidence = profile / "evidence.json"
        elif case == "evidence-parent-file":
            parent = evidence.parent / "file-parent"
            parent.parent.mkdir(parents=True)
            parent.write_bytes(b"ordinary parent file witness")
            evidence = parent / "evidence.json"
        else:
            evidence.mkdir(parents=True)
        command += ["-EvidencePath", str(evidence)]
    before = {path.relative_to(tmp_path): hashlib.sha256(path.read_bytes()).hexdigest()
              for path in tmp_path.rglob("*") if path.is_file()}
    directories = {path.relative_to(tmp_path) for path in tmp_path.rglob("*") if path.is_dir()}
    host_program_data_guard.assert_unchanged()
    result = subprocess.run(command, cwd=tmp_path, env=environment, capture_output=True,
                            encoding="utf-8", errors="replace",
                            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    host_program_data_guard.assert_unchanged()
    assert result.returncode == 1, "Refusal exit code changed"
    assert result.stdout == "", "Refusal wrote an unexpected public output"
    lines = result.stderr.strip().splitlines()
    assert len(lines) == 1 and lines[0].startswith("LABEL_MATCH_INSTALLER_FAILURE=")
    record = json.loads(lines[0].split("=", 1)[1])
    assert record["status"] == "FAILED"
    assert record["reason_code"] == "PENDING_TARGET_INVALID"
    assert record.get("reason") == expected, "Fixed worker reason differs"
    assert record.get("summary") == "PENDING_TARGET_INVALID reason=" + expected
    for private in (marker, worker_sid, other_sid, unsupported_sid, str(profile), str(inventory)):
        assert private not in result.stdout + result.stderr, "Private worker input reached public output"
    assert before == {path.relative_to(tmp_path): hashlib.sha256(path.read_bytes()).hexdigest()
                      for path in tmp_path.rglob("*") if path.is_file()}, "Refusal changed fixture files"
    assert directories == {path.relative_to(tmp_path) for path in tmp_path.rglob("*") if path.is_dir()}

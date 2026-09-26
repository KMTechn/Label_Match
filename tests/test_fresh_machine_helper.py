"""Run the pinned helper's bounded file operation, with no elevation or OS tasks."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import uuid

import pytest

import writer_session_fence as fence
from fresh_server_transition import SCHEMA, _snapshot
from tests.test_writer_session_fence import _payload, _write_active


ROOT = Path(__file__).resolve().parents[1]
TOKEN = "d" * 64


def _current_fence(state):
    authority = state["attempts"][-1]["authority"]
    return {"WriterFenceSessionId": authority["session_id"], "WriterFenceAttemptId": authority["attempt_id"],
            "WriterFenceReplacementTransactionId": authority["replacement_transaction_id"]}


def invoke_helper(tmp_path, state, action="Preserve", *, sid=None, guard="fake", inputs=None, test_override=True):
    journal = Path(state["paths"]["control_dir"]) / "fresh-server-transition.json"
    journal.parent.mkdir(parents=True, exist_ok=True)
    journal.write_text(json.dumps(state), encoding="utf-8")
    local = tmp_path / "local"
    spec_values = {
        "FreshMachineAction": action, "FreshMachineJournal": str(journal),
        "FreshMachineJournalSha256": hashlib.sha256(journal.read_bytes()).hexdigest(),
        "FreshOperatorSid": sid or state["target"]["sid"], "SourceRoot": str(tmp_path / "packet"),
        "OperatorLocalAppDataRoot": str(local),
        "WriterFenceControlRoot": str(local / "KMTech/DirectSync/label_match/control/writer-session"),
        "WriterFenceDelegationToken": TOKEN, **_current_fence(state), **(inputs or {})}
    spec = tmp_path / "helper-input.json"
    spec.write_text(json.dumps(spec_values), encoding="utf-8")
    guard_functions = (". $Guard" if guard == "real" else r'''
function Enter-LabelWriterDelegatedOperation {
    param($ControlRoot,$SessionId,$AttemptId,$ReplacementTransactionId,$DelegationToken,$Source)
    if($Source -cne 'fresh_machine_preservation') { throw 'wrong guard' }
    if($RootProbe) { throw 'CANONICAL_ROOT_ACCEPTED' }
    $script:admitted=$true
    return 'synthetic-admission'
}
function Exit-LabelWriterAdmission($lease) { if($lease -cne 'synthetic-admission') { throw 'wrong lease' } }''')
    guard_functions = "$RootProbe=$" + str(guard == "root_probe").lower() + "\n" + guard_functions
    harness = tmp_path / "helper-harness.ps1"
    harness.write_text(r'''
param([string]$Helper,[string]$Guard,[string]$Spec)
$ErrorActionPreference='Stop'
''' + guard_functions + r'''
$ast=[System.Management.Automation.Language.Parser]::ParseFile($Helper,[ref]$null,[ref]$null)
$names=@('Get-StrictFullPath','Test-SamePath','Assert-NoReparsePoint','Get-FileSha256','Invoke-FreshMachinePreservation')
foreach($node in $ast.FindAll({param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst]},$true)) {
    if($node.Name -cin $names) { . ([ScriptBlock]::Create($node.Extent.Text)) }
}
function Get-BootstrapFileSha256([string]$Path) { (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant() }
foreach($p in (Get-Content -LiteralPath $Spec -Raw | ConvertFrom-Json).PSObject.Properties) { Set-Variable -Name $p.Name -Value $p.Value }
$DryRun=$false
$Uninstall=$false
$testOverride=$''' + str(test_override).lower() + r'''
$WriterFenceFunctionsPreloaded=$true
$BootstrapIntegrityPreloaded=$true
Invoke-FreshMachinePreservation
''' + ("" if guard == "real" else "if(-not $script:admitted) { throw 'guard was not entered' }\n"), encoding="utf-8-sig")
    env = dict(os.environ, PSModulePath=r"C:\Windows\System32\WindowsPowerShell\v1.0\Modules",
               KMTECH_LABEL_WRITER_TEST_MODE="1", KMTECH_LABEL_WRITER_CONTROL_ROOT=spec_values["WriterFenceControlRoot"])
    return subprocess.run([r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe",
                           "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(harness),
                           "-Helper", str(ROOT / "INSTALL_THIS_PC.ps1"), "-Guard", str(ROOT / "tools/label_writer_fence.ps1"),
                           "-Spec", str(spec)], env=env, capture_output=True, text=True, timeout=45,
                          creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))


@pytest.fixture
def machine_state(tmp_path):
    sid = subprocess.check_output([
        r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe", "-NoProfile", "-NonInteractive", "-Command",
        "[Security.Principal.WindowsIdentity]::GetCurrent().User.Value"], text=True).strip()
    packet = tmp_path / "packet/portable-manifest.json"
    packet.parent.mkdir()
    packet.write_bytes(b'{"fixture":"packet"}')
    source = tmp_path / "machine/KMTech/Logistics/profiles/Label_Match"
    source.mkdir(parents=True)
    (source / "runtime-profile.json").write_bytes(b"original-app-profile")
    (source / "secrets").mkdir()
    (source / "secrets/bearer-token.dpapi").write_bytes(b"original-encrypted-token")
    archive = tmp_path / "a/KMTech/server-transition/Label_Match/run001"
    archive.mkdir(parents=True)
    control_dir = tmp_path / "local/KMTech/DirectSync/label_match/control"
    authority = {"session_id": uuid.uuid4().hex, "attempt_id": uuid.uuid4().hex,
                 "replacement_transaction_id": uuid.uuid4().hex}
    state = {"schema": SCHEMA, "transition_id": "a" * 32,
             "target": {"sid": sid, "packet": hashlib.sha256(packet.read_bytes()).hexdigest()},
             "phase": "QUIESCED", "machine_root": str(tmp_path / "machine"), "archive_root": str(archive),
             "paths": {"control_dir": str(control_dir)},
             "attempts": [{"id": uuid.uuid4().hex, "authority": authority, "status": "UNKNOWN"}],
             "entries": [{"source": str(source), "inactive": str(source.with_name(".Label_Match.fresh-" + "a" * 32)),
                          "archive": str(archive / "items/0"), "snapshot": _snapshot(source), "machine": True}]}
    pointer = tmp_path / "local/KMTech/Label_Match/server-transition/active-transition.json"
    pointer.parent.mkdir(parents=True)
    pointer.write_text(json.dumps({"schema": SCHEMA, "journal": str(control_dir / "fresh-server-transition.json")}),
                       encoding="utf-8")
    return state


def test_native_helper_preserves_retries_and_restores_exact_bytes_acl(tmp_path, machine_state):
    entry = machine_state["entries"][0]
    result = invoke_helper(tmp_path, machine_state)
    assert result.returncode == 0, result.stderr
    assert not Path(entry["source"]).exists()
    assert (Path(entry["archive"]) / "secrets/bearer-token.dpapi").read_bytes() == b"original-encrypted-token"
    result = invoke_helper(tmp_path, machine_state)
    assert result.returncode == 0, result.stderr
    machine_state["attempts"].append({"id": uuid.uuid4().hex, "kind": "restore", "status": "UNKNOWN",
                                      "authority": machine_state["attempts"][-1]["authority"]})
    result = invoke_helper(tmp_path, machine_state, "Restore")
    assert result.returncode == 0, result.stderr
    assert _snapshot(Path(entry["source"])) == entry["snapshot"]


@pytest.mark.parametrize("corruption", ["sid", "root", "archive"])
def test_machine_helper_rejects_wrong_binding_or_copy_before_detach(tmp_path, machine_state, corruption):
    entry = machine_state["entries"][0]
    if corruption == "root":
        machine_state["machine_root"] = str(tmp_path / "different")
    if corruption == "archive":
        target = Path(entry["archive"])
        target.mkdir(parents=True)
        (target / "unexpected").write_bytes(b"must-preserve")
    result = invoke_helper(tmp_path, machine_state, sid="S-1-5-21-100-200-300-9999" if corruption == "sid" else None)
    assert result.returncode != 0
    assert _snapshot(Path(entry["source"])) == entry["snapshot"]
    assert not Path(entry["inactive"]).exists()


@pytest.fixture
def live_fence(tmp_path, machine_state):
    """The installer's canonical fence delegates this helper and still holds its authority."""
    authority = machine_state["attempts"][-1]["authority"]
    payload = _payload(delegated_sources=["canonical_placement", "fresh_machine_preservation"], token=TOKEN)
    payload.update(authority)
    payload["session_authority_mutex_name"] = fence.session_authority_mutex_name(
        payload["session_id"], payload["attempt_id"], payload["orchestrator_sha256"],
        payload["replacement_transaction_id"], payload["writer_contract_sha256"])
    _write_active(tmp_path / "local/KMTech/DirectSync/label_match/control/writer-session", payload)
    owner = fence._acquire_named_mutex(payload["session_authority_mutex_name"], 0)  # noqa: SLF001
    assert owner is not None
    yield payload, owner
    owner.release()


def test_real_guard_admits_the_bound_delegated_helper_and_restores(tmp_path, machine_state, live_fence):
    entry = machine_state["entries"][0]
    result = invoke_helper(tmp_path, machine_state, guard="real")
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "fresh_machine_preservation=PASS"
    assert not Path(entry["source"]).exists()
    assert (Path(entry["archive"]) / "runtime-profile.json").read_bytes() == b"original-app-profile"
    machine_state["attempts"].append({"id": uuid.uuid4().hex, "kind": "restore", "status": "UNKNOWN",
                                      "authority": machine_state["attempts"][-1]["authority"]})
    result = invoke_helper(tmp_path, machine_state, "Restore", guard="real")
    assert result.returncode == 0, result.stderr
    assert _snapshot(Path(entry["source"])) == entry["snapshot"]


@pytest.mark.parametrize("unbound", ["empty_token", "other_control_root", "journal_outside",
                                     "attempt_not_current", "authority_not_live"])
def test_real_guard_rejects_unbound_helper_inputs_before_any_move(tmp_path, machine_state, live_fence, unbound):
    payload, owner = live_fence
    entry = machine_state["entries"][0]
    inputs = {}
    if unbound == "empty_token":
        inputs["WriterFenceDelegationToken"] = ""
    elif unbound == "other_control_root":
        # A valid-format, delegated fence elsewhere must not stand in for the user's canonical one.
        other = tmp_path / "unrelated-control"
        _write_active(other, payload)
        inputs["WriterFenceControlRoot"] = str(other)
    elif unbound == "journal_outside":
        journal = Path(machine_state["paths"]["control_dir"]) / "fresh-server-transition.json"
        journal.parent.mkdir(parents=True, exist_ok=True)
        journal.write_text(json.dumps(machine_state), encoding="utf-8")
        outside = tmp_path / "outside/fresh-server-transition.json"
        outside.parent.mkdir()
        shutil.copy2(journal, outside)
        inputs["FreshMachineJournal"] = str(outside)
    elif unbound == "attempt_not_current":
        inputs.update(_current_fence(machine_state))
        machine_state["attempts"][-1]["authority"] = dict(machine_state["attempts"][-1]["authority"],
                                                          attempt_id=uuid.uuid4().hex)
    else:
        owner.release()
    result = invoke_helper(tmp_path, machine_state, guard="real", inputs=inputs)
    assert result.returncode != 0, result.stdout
    assert "fresh_machine_preservation=PASS" not in result.stdout
    assert _snapshot(Path(entry["source"])) == entry["snapshot"]
    assert not Path(entry["inactive"]).exists()
    assert not Path(entry["archive"]).exists()


@pytest.mark.parametrize("control_root", ["profile_default", "caller_choice"])
def test_production_helper_takes_the_control_root_from_the_profile_list(tmp_path, machine_state, control_root):
    """Outside test mode the caller cannot choose the root; nothing at the real root is read or changed."""
    real_local = subprocess.check_output([
        r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe", "-NoProfile", "-NonInteractive", "-Command",
        "[Environment]::GetFolderPath('LocalApplicationData')"], text=True).strip()
    root = (Path(real_local) if control_root == "profile_default" else tmp_path / "local") / \
        "KMTech/DirectSync/label_match/control/writer-session"
    entry = machine_state["entries"][0]
    result = invoke_helper(tmp_path, machine_state, guard="root_probe", test_override=False,
                           inputs={"WriterFenceControlRoot": str(root)})
    assert result.returncode != 0
    accepted = "CANONICAL_ROOT_ACCEPTED" in result.stderr
    assert accepted is (control_root == "profile_default"), result.stderr
    assert _snapshot(Path(entry["source"])) == entry["snapshot"]
    assert not Path(entry["inactive"]).exists()


def test_canonical_audit_directory_protection_needs_no_sacl_privilege(tmp_path):
    """The real PS 5.1 ACL boundary must work on first use and resume."""
    source = (ROOT / "INSTALL_CANONICAL_PORTABLE.ps1").read_text(encoding="utf-8-sig")
    start = source.index("    $freshAcl = New-Object Security.AccessControl.DirectorySecurity")
    end = source.index("    $freshOwnerPath =", start)
    harness = tmp_path / "audit-acl.ps1"
    harness.write_text(r'''
param([string]$AuditRoot)
$ErrorActionPreference='Stop'
$freshAudit=$AuditRoot
[void](New-Item -ItemType Directory -Path $freshAudit)
$before=Get-Acl -LiteralPath $freshAudit
$freshSid=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value
foreach($attempt in @(1,2)) {
''' + source[start:end] + r'''
    $acl=Get-Acl -LiteralPath $freshAudit
    if(-not $acl.AreAccessRulesProtected -or $acl.Owner -ne $before.Owner -or $acl.Group -ne $before.Group) {
        throw 'Protection or owner/group changed incorrectly'
    }
    $rules=@($acl.GetAccessRules($true,$true,[Security.Principal.SecurityIdentifier]))
    if($rules.Count -ne 3) { throw 'Unexpected access rule count' }
    foreach($rule in $rules) {
        if($rule.IdentityReference.Value -notin @('S-1-5-18','S-1-5-32-544',$freshSid) -or
           $rule.FileSystemRights -ne [Security.AccessControl.FileSystemRights]::FullControl -or
           $rule.AccessControlType -ne [Security.AccessControl.AccessControlType]::Allow) {
            throw 'Unexpected directory access rule'
        }
    }
}
'PASS'
''', encoding="utf-8-sig")
    result = subprocess.run([
        r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe", "-NoProfile", "-NonInteractive",
        "-ExecutionPolicy", "Bypass", "-File", str(harness), "-AuditRoot", str(tmp_path / "audit"),
    ], env=dict(os.environ, PSModulePath=r"C:\Windows\System32\WindowsPowerShell\v1.0\Modules"),
        capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "PASS"

"""Run the pinned helper's bounded file operation, with no elevation or OS tasks."""

import hashlib
import json
import os
from pathlib import Path
import subprocess

import pytest

from fresh_server_transition import _snapshot


ROOT = Path(__file__).resolve().parents[1]


def invoke_helper(tmp_path, state, action="Preserve", *, sid=None):
    journal = tmp_path / "journal.json"
    journal.write_text(json.dumps(state), encoding="utf-8")
    spec = tmp_path / "helper-input.json"
    spec.write_text(json.dumps({
        "FreshMachineAction": action, "FreshMachineJournal": str(journal),
        "FreshMachineJournalSha256": hashlib.sha256(journal.read_bytes()).hexdigest(),
        "FreshOperatorSid": sid or state["target"]["sid"], "SourceRoot": str(tmp_path / "packet"),
    }), encoding="utf-8")
    harness = tmp_path / "helper-harness.ps1"
    harness.write_text(r'''
param([string]$Helper,[string]$Spec)
$ErrorActionPreference='Stop'
$ast=[System.Management.Automation.Language.Parser]::ParseFile($Helper,[ref]$null,[ref]$null)
$names=@('Get-StrictFullPath','Test-SamePath','Assert-NoReparsePoint','Get-FileSha256','Invoke-FreshMachinePreservation')
foreach($node in $ast.FindAll({param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst]},$true)) {
    if($node.Name -cin $names) { . ([ScriptBlock]::Create($node.Extent.Text)) }
}
function Get-BootstrapFileSha256([string]$Path) { (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant() }
function Enter-LabelWriterDelegatedOperation {
    param($ControlRoot,$SessionId,$AttemptId,$ReplacementTransactionId,$DelegationToken,$Source)
    if($Source -cne 'fresh_machine_preservation') { throw 'wrong guard' }
    $script:admitted=$true
    return 'synthetic-admission'
}
function Exit-LabelWriterAdmission($lease) { if($lease -cne 'synthetic-admission') { throw 'wrong lease' } }
foreach($p in (Get-Content -LiteralPath $Spec -Raw | ConvertFrom-Json).PSObject.Properties) { Set-Variable -Name $p.Name -Value $p.Value }
$testOverride=$true
$WriterFenceFunctionsPreloaded=$true
$BootstrapIntegrityPreloaded=$true
Invoke-FreshMachinePreservation
if(-not $script:admitted) { throw 'guard was not entered' }
''', encoding="utf-8-sig")
    env = dict(os.environ, PSModulePath=r"C:\Windows\System32\WindowsPowerShell\v1.0\Modules")
    return subprocess.run([r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe",
                           "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(harness),
                           "-Helper", str(ROOT / "INSTALL_THIS_PC.ps1"), "-Spec", str(spec)],
                          env=env, capture_output=True, text=True, timeout=45)


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
    state = {"schema": "label-match-fresh-server-transition-v1", "transition_id": "a" * 32,
             "target": {"sid": sid, "packet": hashlib.sha256(packet.read_bytes()).hexdigest()},
             "phase": "QUIESCED", "machine_root": str(tmp_path / "machine"), "archive_root": str(archive),
             "entries": [{"source": str(source), "inactive": str(source.with_name(".Label_Match.fresh-" + "a" * 32)),
                          "archive": str(archive / "items/0"), "snapshot": _snapshot(source), "machine": True}]}
    return state


def test_native_helper_preserves_retries_and_restores_exact_bytes_acl(tmp_path, machine_state):
    entry = machine_state["entries"][0]
    result = invoke_helper(tmp_path, machine_state)
    assert result.returncode == 0, result.stderr
    assert not Path(entry["source"]).exists()
    assert (Path(entry["archive"]) / "secrets/bearer-token.dpapi").read_bytes() == b"original-encrypted-token"
    result = invoke_helper(tmp_path, machine_state)
    assert result.returncode == 0, result.stderr
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

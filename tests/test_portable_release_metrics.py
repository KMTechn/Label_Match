"""Exercise the placement installer's actual portable tree boundary."""
import hashlib
import json
from pathlib import Path

import pytest
from tests.test_shared_ancestor_protection import run_functions

ROOT = Path(__file__).resolve().parents[1]


def run_release_metrics(driver_root, packet, *, installer=None):
    return run_functions(
        driver_root, installer or ROOT / "INSTALL_THIS_PC.ps1",
        ["Get-StrictFullPath", "Test-SamePath", "Get-FileSha256", "Assert-RequiredRelease"],
        r'''
function Get-BootstrapFileSha256([string]$Path) {
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}
if ((Assert-RequiredRelease $env:LM_METRICS_PACKET $true) -cne 'PORTABLE_CPYTHON') {
    throw 'Portable release layout changed.'
}
'PASS actual placement tree metrics'
''', values={"LM_METRICS_PACKET": str(packet)}
    )


@pytest.mark.parametrize("damage", ("none", "root-extra", "nested-integrity", "one-byte"))
def test_actual_release_metrics_exclude_only_root_integrity_record(tmp_path, damage):
    packet = tmp_path / "packet"
    for relative in (
        "runtime/python.exe", "runtime/pythonw.exe", "app/main.py",
        "launch-label-match.cmd", "INSTALL_CANONICAL_PORTABLE.ps1",
        "INSTALL_THIS_PC.ps1", "tools/bootstrap_integrity.ps1",
    ):
        target = packet / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"original payload\n")
    files = [path for path in packet.rglob("*") if path.is_file()]
    manifest = {
        "schema": "label-match-portable-tree-v1",
        "entrypoint": "runtime/pythonw.exe app/main.py",
        "launcher": "launch-label-match.cmd",
        "allowed_unsigned_app_pe": [], "forbidden_package_roots": [],
        "runtime_pythonw_sha256": hashlib.sha256((packet / "runtime/pythonw.exe").read_bytes()).hexdigest(),
        "launcher_sha256": hashlib.sha256((packet / "launch-label-match.cmd").read_bytes()).hexdigest(),
        "file_count_before_manifest": len(files),
        "byte_count_before_manifest": sum(path.stat().st_size for path in files),
    }
    (packet / "portable-manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (packet / "bootstrap-integrity.json").write_bytes(b"separately pinned root record")
    if damage == "root-extra":
        (packet / "extra.bin").write_bytes(b"extra")
    elif damage == "nested-integrity":
        (packet / "app/bootstrap-integrity.json").write_bytes(b"extra")
    elif damage == "one-byte":
        with (packet / "app/main.py").open("ab") as stream:
            stream.write(b"X")
    result = run_release_metrics(tmp_path, packet)
    if damage == "none":
        assert result.returncode == 0, result.stdout + result.stderr
    else:
        assert result.returncode != 0
        assert "Portable release tree metrics differ from the manifest." in result.stderr


@pytest.mark.parametrize("engine", ("powershell.exe", "pwsh.exe"))
def test_failed_placement_retained_code_parent_admits_retry(tmp_path, engine):
    result = run_functions(
        tmp_path, ROOT / "INSTALL_THIS_PC.ps1",
        ["Initialize-LabelProtectedInstallBase", "Assert-ProtectedCodeAncestors"], r'''
$tokens=$null;$errors=$null
$helper=[Management.Automation.Language.Parser]::ParseFile($env:LM_RETRY_HELPER,[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'Shipped helper parse failed.'}
foreach($name in @('Assert-LabelProtectedInstallPath','Get-InstallationPreparationExitCode')) {
    $definitions=@($helper.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $name},$true))
    if($definitions.Count -ne 1){throw 'Actual retry admission function missing.'}
    . ([scriptblock]::Create($definitions[0].Extent.Text))
}
$script:created=$false;$script:creates=0;$script:hardens=0;$script:admissions=0
$script:leaf=[Security.AccessControl.DirectorySecurity]::new()
$script:leaf.SetSecurityDescriptorSddlForm('O:BAG:BAD:P(A;OICI;FA;;;BA)(A;OICI;FA;;;SY)(A;OICI;0x1200a9;;;BU)')
$script:shared=[Security.AccessControl.DirectorySecurity]::new()
$script:shared.SetSecurityDescriptorSddlForm('O:BAG:BAD:P(A;OICI;FA;;;BA)(A;OICI;FA;;;SY)(A;OICI;0x1200a9;;;BU)(A;;0x1200af;;;AU)(A;OICIIO;0xe0010000;;;AU)')
function Test-Path {param($LiteralPath) if($LiteralPath -ceq 'C:\KMTech\Apps\Label_Match'){return $script:created};return $true}
function Get-Item {param($LiteralPath,$ErrorAction) return [pscustomobject]@{Attributes=[IO.FileAttributes]::Directory}}
function Get-Acl {param($LiteralPath,$ErrorAction)
    if($LiteralPath -in @('C:\KMTech','C:\KMTech\Apps')){return $script:shared}
    return $script:leaf
}
function New-Item {param($ItemType,$Path,$ErrorAction)
    if($Path -cne 'C:\KMTech\Apps\Label_Match'){throw 'Creation escaped memory app parent.'}
    $script:creates++;$script:created=$true
}
function Set-HardenedCodeAcl {param($Path)
    if($Path -cne 'C:\KMTech\Apps\Label_Match'){throw 'Hardening escaped memory app parent.'}
    $script:hardens++
}
function Protect-KmtechCodeAncestors {param($CodeParent,[switch]$BeforeAppCreation,[switch]$AdmissionOnly,$AncestorPath,[switch]$RequireProtected)
    $script:admissions++
}
$failed=$false
try { Initialize-LabelProtectedInstallBase 'C:\KMTech\Apps\Label_Match\current';throw 'injected placement failure after preparation' }
catch { if($_.Exception.Message -cne 'injected placement failure after preparation'){throw};$failed=$true }
if(-not $failed -or -not $script:created){throw 'Failure did not leave the prepared app parent.'}
# An installer-owner evidence file can make that parent nonempty. Admission
# inspects owner/DACL/reparse state without rejecting existing child evidence.
Initialize-LabelProtectedInstallBase 'C:\KMTech\Apps\Label_Match\current'
if($script:creates -ne 1 -or $script:hardens -ne 1 -or $script:admissions -lt 2){throw 'Retry recreated or rejected the existing protected parent.'}
'PASS failed placement retains an admissible code parent'
''', engine=engine, values={"LM_RETRY_HELPER": str(ROOT / "tools/bootstrap_integrity.ps1")}
    )
    assert result.returncode == 0, result.stdout + result.stderr

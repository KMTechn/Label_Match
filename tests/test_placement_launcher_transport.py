"""Real child checks for the elevation transport; never request elevation."""
import base64
import hashlib
import json
import re
import subprocess

import pytest
from pathlib import Path

from tests.test_shared_ancestor_protection import run_functions

ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / "INSTALL_CANONICAL_PORTABLE.ps1"
# Measured PS5.1 ShellExecute/Open limit on Windows 26200, 57-char exe path.
SHELL_ARGUMENT_LIMIT = 32705


def transport_command(tmp_path, launcher, payload):
    """Prepare inputs, then use the actual product bootstrap without elevation."""
    code = tmp_path / "transport-code.ps1"
    data = tmp_path / "transport-data.json"
    # Literal launcher extraction excludes the common definitions that the
    # real producer composes after its param block. Load that same dependency.
    if "ConvertTo-KmtechRedactedSinkText" in launcher and "function ConvertTo-KmtechRedactedSinkText" not in launcher:
        boundary = launcher.index("\n") + 1
        redactor = str(ROOT / "tools/failure_redaction.ps1").replace("'", "''")
        launcher = launcher[:boundary] + ". '" + redactor + "'\n" + launcher[boundary:]
    code.write_bytes(launcher.encode("utf-8"))
    data.write_bytes(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    source = INSTALLER.read_text(encoding="utf-8-sig")
    body = re.search(r"\$bootstrap = @'\r?\n(.*?)\r?\n'@", source, re.DOTALL).group(1)
    for name, path in (("launcher", code), ("payload", data)):
        body = body.replace(f"@@{name}-path@@", str(path).replace("'", "''"))
        body = body.replace(f"@@{name}-sha@@", hashlib.sha256(path.read_bytes()).hexdigest())
    body = body.replace("@@failure-path@@", str(tmp_path / 'transport-failure.json').replace("'", "''"))
    message = json.loads((ROOT / 'tools/failure_redaction_rules.json').read_text())['safe_messages'][0]
    body = body.replace("@@safe-message@@", message.replace("'", "''"))
    return base64.b64encode(body.encode("utf-16-le")).decode("ascii")


@pytest.mark.parametrize("target", ("launcher", "payload"))
@pytest.mark.parametrize("fault", ("byte", "missing", "reparse"))
def test_transport_rejects_changed_missing_or_reparse_files_before_execution(tmp_path, target, fault):
    marker = tmp_path / "mutation.txt"
    launcher = "param([byte[]]$PayloadBytes)\n[IO.File]::WriteAllText('%s','mutated')" % str(marker).replace("'", "''")
    command = transport_command(tmp_path, launcher, {"valid": True})
    path = tmp_path / ("transport-code.ps1" if target == "launcher" else "transport-data.json")
    if fault == "byte":
        raw = bytearray(path.read_bytes())
        raw[-1] ^= 1
        path.write_bytes(raw)
    elif fault == "missing":
        path.unlink()
    else:
        # Native hard-to-fake directory reparse point, without symlink privilege.
        original = tmp_path / "original"
        original.mkdir()
        saved = original / path.name
        path.replace(saved)
        junction = tmp_path / "junction"
        made = subprocess.run(["cmd.exe", "/d", "/c", "mklink", "/J", str(junction), str(original)],
            capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
        assert made.returncode == 0
        command = transport_command(tmp_path, launcher, {"valid": True})
        decoded = base64.b64decode(command).decode("utf-16-le").replace(str(path), str(junction / path.name))
        command = base64.b64encode(decoded.encode("utf-16-le")).decode("ascii")
    child = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-EncodedCommand", command],
        capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    assert child.returncode == 1
    assert not marker.exists()
    public = json.loads(next(line.split('=', 1)[1] for line in child.stderr.decode('utf-8').splitlines() if line.startswith('LABEL_MATCH_INSTALLER_FAILURE=')))
    assert public['placement_code'] == 'PLACEMENT_TRANSPORT_REJECTED'
    assert public['reason_code'] == 'INSTALLER_FAILED'
    report = Path(public['report_path']).read_bytes()
    assert hashlib.sha256(report).hexdigest() == public['report_sha256']
    assert 'report_status' not in public


@pytest.mark.parametrize("target", ("launcher", "payload"))
@pytest.mark.parametrize("operation", ("write", "replace"))
def test_transport_keeps_verified_bytes_and_rejects_swap_until_execution_finishes(tmp_path, target, operation):
    code = tmp_path / "transport-code.ps1"
    data = tmp_path / "transport-data.json"
    chosen = code if target == "launcher" else data
    replacement = tmp_path / "malicious.ps1"
    marker = tmp_path / "malicious-executed.txt"
    replacement.write_text("[IO.File]::WriteAllText('%s','bad')" % marker, encoding="utf-8")
    action = ("[IO.File]::WriteAllText('%s','bad')" % chosen if operation == "write" else
              "[IO.File]::Delete('%s');[IO.File]::Move('%s','%s')" % (chosen, replacement, chosen))
    launcher = """param([byte[]]$PayloadBytes)
$denied=$false
try{%s}catch [IO.IOException]{$denied=$true}
if(-not $denied){throw 'Verified input was replaceable during execution.'}
$payload=([Text.UTF8Encoding]::new($false,$true)).GetString($PayloadBytes)|ConvertFrom-Json
if($payload.expected -cne 'original'){throw 'Changed payload executed.'}
'ORIGINAL_VERIFIED_BYTES'
""" % action
    command = transport_command(tmp_path, launcher, {"expected": "original"})
    child = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-EncodedCommand", command],
        capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    assert child.returncode == 0, child.stderr.decode(errors="replace")
    assert b"ORIGINAL_VERIFIED_BYTES" in child.stdout
    assert not marker.exists()


def test_uac_cancellation_rethrows_original_failure_and_retires_private_transport(tmp_path):
    result = run_functions(tmp_path, INSTALLER,
        ["ByteSha", "InvokeFrozenPlacementHelper"], r'''
$root=$env:LM_TRANSPORT_ROOT
$frozen=[pscustomobject]@{root=$root;helper_path=(Join-Path $root 'helper.ps1');helper_sha256=('a'*64);
 integrity_sha256=('b'*64);writer_fence_path=(Join-Path $root 'writer.ps1');writer_fence_sha256=('c'*64);
 data_generation_path=(Join-Path $root 'generation.ps1');data_generation_sha256=('d'*64)}
function Start-Process {param($FilePath,$Verb,$ArgumentList,$WindowStyle,[switch]$Wait,[switch]$PassThru)
 throw [ComponentModel.Win32Exception]::new(1223)
}
$rejected=$false
try{[void](InvokeFrozenPlacementHelper $frozen @{ElevationLogPath=(Join-Path $root 'placement')})}
catch{
 $ex=$_.Exception
 while($ex -and $ex -isnot [ComponentModel.Win32Exception]){$ex=$ex.InnerException}
 if(-not $ex -or $ex.NativeErrorCode -ne 1223){throw 'Cancellation error changed.'}
 $rejected=$true
}
if(-not $rejected){throw 'Cancellation was admitted.'}
if(@(Get-ChildItem -LiteralPath $root -Filter 'placement-child-*').Count){throw 'Private transport was retained.'}
'CANCELLED_WITHOUT_MUTATION'
''', values={"LM_TRANSPORT_ROOT": str(tmp_path)})
    assert result.returncode == 0, result.stderr
    assert "CANCELLED_WITHOUT_MUTATION" in result.stdout


@pytest.mark.parametrize("fault", ("byte", "missing", "reparse", "swap"))
def test_actual_placement_launcher_pins_helper_and_keeps_it_immutable(tmp_path, fault):
    files = {name: tmp_path / (name + ".ps1") for name in ("helper", "integrity", "writer", "generation")}
    marker = tmp_path / "executed.txt"
    files["helper"].write_text("""param([string]$SourceRoot)
$denied=$false
try{[IO.File]::WriteAllText($SourceRoot,'changed')}catch [IO.IOException]{$denied=$true}
if(-not $denied){throw 'Pinned helper could change during execution.'}
[IO.File]::WriteAllText('%s','original')
""" % marker, encoding="utf-8")
    files["integrity"].write_text("param([string]$SharedCodeRoot)\n", encoding="utf-8")
    for name in ("writer", "generation"):
        files[name].write_text("# isolated helper input\n", encoding="utf-8")
    parameters = {name: "" for name in (
        "SourceRoot", "InstallRoot", "ElevationLogPath", "ExpectedBootstrapScriptSha256",
        "VerifiedBootstrapScriptPath", "ExpectedSourceAggregateSha256", "WriterFenceControlRoot",
        "DataGenerationSubjectSid", "WriterFenceSessionId", "WriterFenceAttemptId",
        "WriterFenceReplacementTransactionId", "WriterFenceDelegationToken")}
    parameters.update(SourceRoot=str(files["helper"]), BootstrapIntegrityPreloaded=True,
        WriterFenceFunctionsPreloaded=True, ExpectedSourceFileCount=0, ExpectedSourceByteCount=0,
        DataGenerationMarkerPaths=[], AllowNoncanonicalLayoutForTest=True,
        ReplaceExistingVerifiedPortable=False, DryRun=False)
    payload = {"parameters": parameters, "failure_path": str(tmp_path / "failure.json")}
    for name, key in (("helper", "helper"), ("integrity", "integrity"), ("writer", "writer_fence"), ("generation", "data_generation")):
        payload[key + "_path"] = str(files[name])
        payload[key + "_sha256"] = hashlib.sha256(files[name].read_bytes()).hexdigest()
    if fault == "byte":
        raw = bytearray(files["helper"].read_bytes()); raw[-1] ^= 1
        files["helper"].write_bytes(raw)
    elif fault == "missing":
        files["helper"].unlink()
    elif fault == "reparse":
        original = tmp_path / "original-helper"; original.mkdir()
        files["helper"].replace(original / files["helper"].name)
        junction = tmp_path / "helper-junction"
        made = subprocess.run(["cmd.exe", "/d", "/c", "mklink", "/J", str(junction), str(original)],
            capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
        assert made.returncode == 0
        payload["helper_path"] = str(junction / files["helper"].name)
    source = INSTALLER.read_text(encoding="utf-8-sig")
    launcher = re.search(r"\$launcher = @'\r?\n(.*?)\r?\n'@", source, re.DOTALL).group(1)
    command = transport_command(tmp_path, launcher, payload)
    child = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-EncodedCommand", command],
        capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    if fault == "swap":
        assert child.returncode == 0, child.stderr.decode(errors="replace")
        assert marker.read_text() == "original"
    else:
        assert child.returncode == 1
        assert not marker.exists()
        record = json.loads((tmp_path / "failure.json").read_text())
        assert record["phase"] == "PIN_VALIDATION"
        assert record["exit_code"] == 1


@pytest.mark.parametrize("owner_change_denied", (False, True))
def test_placement_elevation_command_is_bounded_at_longest_supported_paths(tmp_path, owner_change_denied):
    result = run_functions(tmp_path, INSTALLER,
        ["ByteSha", "InvokeFrozenPlacementHelper"], r'''
$root=$env:LM_TRANSPORT_ROOT
$user='u'*20
$audit=Join-Path $root $user
while($audit.Length -lt 175){$audit=Join-Path $audit ('segment'+('x'*12))}
[void][IO.Directory]::CreateDirectory($audit)
if($env:LM_TRANSPORT_OWNER_DENIED -ceq '1'){
 $transportOwnerSid=[Security.Principal.WindowsIdentity]::GetCurrent().User
 $acl=[Security.AccessControl.DirectorySecurity]::new()
 $acl.SetAccessRuleProtection($true,$false)
 $acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new($transportOwnerSid,'TakeOwnership',
  ([Security.AccessControl.InheritanceFlags]::ContainerInherit -bor [Security.AccessControl.InheritanceFlags]::ObjectInherit),
  [Security.AccessControl.PropagationFlags]::None,'Deny'))
 $acl.AddAccessRule([Security.AccessControl.FileSystemAccessRule]::new($transportOwnerSid,'FullControl',
  ([Security.AccessControl.InheritanceFlags]::ContainerInherit -bor [Security.AccessControl.InheritanceFlags]::ObjectInherit),
  [Security.AccessControl.PropagationFlags]::None,'Allow'))
 [IO.Directory]::SetAccessControl($audit,$acl)
}
$long='C:\'+('p'*256)
$frozen=[pscustomobject]@{root=$root;helper_path=(Join-Path $root 'helper.ps1');helper_sha256=('a'*64);
 integrity_sha256=('b'*64);writer_fence_path=(Join-Path $root 'writer.ps1');writer_fence_sha256=('c'*64);
 data_generation_path=(Join-Path $root 'generation.ps1');data_generation_sha256=('d'*64)}
function Start-Process {param($FilePath,$Verb,$ArgumentList,$WindowStyle,[switch]$Wait,[switch]$PassThru)
 if($Verb -cne 'RunAs' -or $WindowStyle -cne 'Hidden'){throw 'Elevation dispatch shape differs.'}
 $global:transportLength=($ArgumentList -join ' ').Length
 [pscustomobject]@{ExitCode=0}
}
$parameters=@{ElevationLogPath=(Join-Path $audit 'placement');SourceRoot=$long;InstallRoot=$long;
 WriterFenceControlRoot=$long;VerifiedBootstrapScriptPath=$long;StartupUpdateRestorePreimage=$long;
 DataGenerationMarkerPaths=@(1..12|ForEach-Object{$long})}
[void](InvokeFrozenPlacementHelper $frozen $parameters)
@{argument_chars=$transportLength;username_chars=$user.Length;longest_path_chars=$long.Length}|ConvertTo-Json -Compress
''', values={"LM_TRANSPORT_ROOT": str(tmp_path), "LM_TRANSPORT_OWNER_DENIED": "1" if owner_change_denied else "0"})
    assert result.returncode == 0, result.stderr
    record = json.loads(result.stdout.strip().splitlines()[-1])
    assert record["username_chars"] == 20
    assert record["longest_path_chars"] == 259
    assert record["argument_chars"] < SHELL_ARGUMENT_LIMIT // 2, record


@pytest.mark.parametrize("exit_code", (1, 70))
def test_parent_preserves_native_exit_when_child_cannot_publish_diagnostic(tmp_path, exit_code):
    result = run_functions(tmp_path, INSTALLER, ["ByteSha", "InvokeFrozenPlacementHelper"], r'''
$root=$env:LM_TRANSPORT_ROOT
$frozen=[pscustomobject]@{root=$root;helper_path='';helper_sha256='';integrity_sha256='';writer_fence_path='';writer_fence_sha256='';data_generation_path='';data_generation_sha256=''}
function Start-Process {param($FilePath,$Verb,$ArgumentList,$WindowStyle,[switch]$Wait,[switch]$PassThru)
 [pscustomobject]@{ExitCode=[int]$env:LM_TRANSPORT_EXIT}
}
$exitCode=InvokeFrozenPlacementHelper $frozen @{ElevationLogPath=(Join-Path $root 'placement')}
if($exitCode -ne [int]$env:LM_TRANSPORT_EXIT -or $Script:LabelPlacementChildFailure.exit_code -ne $exitCode){throw 'Native child exit changed'}
if($Script:LabelPlacementChildFailure.placement_code -cne 'PLACEMENT_DIAGNOSTIC_UNAVAILABLE'){throw 'Missing child diagnosis was not classified'}
$reference=$Script:LabelPlacementChildFailure.full_exception_trace
if((Get-FileHash -LiteralPath $reference.path -Algorithm SHA256).Hash.ToLowerInvariant() -cne $reference.sha256){throw 'Parent diagnosis bytes differ'}
'PRESERVED'
''', values={"LM_TRANSPORT_ROOT": str(tmp_path), "LM_TRANSPORT_EXIT": str(exit_code)})
    assert result.returncode == 0, result.stderr
    assert "PRESERVED" in result.stdout

@pytest.mark.parametrize('engine', ['powershell.exe', 'pwsh.exe'])
def test_missing_trace_does_not_mask_child_failure(tmp_path, engine):
    result = run_functions(tmp_path, INSTALLER, ['Get-LabelLaunchFailure'], r'''
Set-StrictMode -Version Latest
function Write-KmtechProtectedFailureSidecar {throw 'Synthetic protected-report storage is unavailable'}
try {
 $failure=[InvalidOperationException]::new('Synthetic placement failed')
 $failure.Data['placement_child_failure']=[pscustomobject]@{placement_code='PLACEMENT_CHILD_FAILED';phase='HELPER_EXECUTION';exit_code=1;exception_type='System.InvalidOperationException'}
 throw $failure
} catch { $record=Get-LabelLaunchFailure $_ 'forward' }
if($record.placement_code -cne 'PLACEMENT_CHILD_FAILED' -or $record.report_status -cne 'REPORT_UNAVAILABLE'){throw 'Original placement cause was lost'}
'PRESERVED'
''', engine=engine)
    assert result.returncode == 0, result.stderr
    assert 'PRESERVED' in result.stdout

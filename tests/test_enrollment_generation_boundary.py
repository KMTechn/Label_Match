"""Real enrollment admission and generation records in isolated native children."""
import json
import os
from pathlib import Path
import sys
import subprocess

import pytest

from tests.test_shared_ancestor_protection import run_functions

ROOT = Path(os.environ.get("LM_B1_REPO", Path(__file__).resolve().parents[1]))
INSTALLER = ROOT / "INSTALL_CANONICAL_PORTABLE.ps1"


def run_python(tmp_path, script):
    child = tmp_path / "generation-child.py"
    child.write_text(script, encoding="utf-8")
    result = subprocess.run([sys.executable, "-B", str(child), str(ROOT), str(tmp_path)],
                            capture_output=True, text=True,
                            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    (tmp_path / "generation.stdout").write_text(result.stdout, encoding="utf-8")
    (tmp_path / "generation.stderr").write_text(result.stderr, encoding="utf-8")
    assert result.returncode == 0, f"actual generation boundary failed; evidence: {tmp_path}"


@pytest.mark.parametrize("engine", ("powershell.exe", "pwsh.exe"))
def test_installer_delegates_actual_nested_enrollment_before_tls(tmp_path, engine):
    child = tmp_path / "enrollment.py"
    child.write_text(r'''
import sys
sys.path.insert(0, sys.argv[1])
from tools import register_label_match_worker_pc as registration
from writer_session_fence import writer_admission
import requests

calls = []
def transport(url, **kwargs):
    calls.append(url)
    raise requests.exceptions.SSLError("isolated TLS rejection")
registration.requests.post = transport
try:
    with writer_admission("current_user_onboarding"), registration.EnrollmentMutex():
        registration._enroll({}, enrollment_url="https://enrollment.example.invalid/",
                             enrollment_token="", timeout_seconds=1)
except requests.exceptions.SSLError:
    pass
else:
    raise AssertionError("TLS boundary failure was lost")
assert len(calls) == 1
print("PASS actual enrollment reached isolated TLS boundary")
''', encoding="utf-8")
    result = run_functions(tmp_path, INSTALLER, ["Arg"], r'''
Set-StrictMode -Version Latest
. $env:LM_B1_LIBRARY
$assignments=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.AssignmentStatementAst] -and $n.Left.Extent.Text -ceq '$restoreSources'},$true))
if($assignments.Count -ne 1){throw 'Restore source declaration is ambiguous'}
. ([scriptblock]::Create($assignments[0].Extent.Text))
$session='1'*32;$attempt='2'*32;$transaction='3'*32;$orchestrator='4'*64;$contract='5'*64
$synthetic='isolated-'+('6'*64);$control=Join-Path $env:LM_B1_ROOT 'control'
$authority=Enter-LabelWriterSessionAuthority $session $attempt $orchestrator $transaction $contract
try {
 $payload=New-LabelWriterFencePayload -Status RESTORING -SessionId $session -AttemptId $attempt -ReplacementTransactionId $transaction -SessionStartedAtUtc ([DateTime]::UtcNow.ToString('o')) -OrchestratorSha256 $orchestrator -WriterContractSha256 $contract -DelegationToken $synthetic -DelegatedSources $restoreSources -DelegationExpiresAtUtc ([DateTime]::UtcNow.AddMinutes(5).ToString('o'))
 $payload.activated_at_utc=[DateTime]::UtcNow.ToString('o');[void](Write-LabelWriterFenceAtomic $control $payload)
 $info=[Diagnostics.ProcessStartInfo]::new();$info.FileName=$env:LM_B1_PYTHON
 $info.Arguments='-B "'+$env:LM_B1_CHILD+'" "'+$env:LM_B1_REPO+'"'
 $info.UseShellExecute=$false;$info.CreateNoWindow=$true;$info.RedirectStandardOutput=$true;$info.RedirectStandardError=$true
 $info.EnvironmentVariables['KMTECH_LABEL_WRITER_TEST_MODE']='1'
 $info.EnvironmentVariables['KMTECH_LABEL_WRITER_CONTROL_ROOT']=$control
 $info.EnvironmentVariables['KMTECH_LABEL_WRITER_DELEGATION_TOKEN']=$synthetic
 $info.EnvironmentVariables['KMTECH_LABEL_WRITER_DELEGATION_SESSION_ID']=$session
 $info.EnvironmentVariables['KMTECH_LABEL_WRITER_DELEGATION_ATTEMPT_ID']=$attempt
 $info.EnvironmentVariables['KMTECH_LABEL_WRITER_DELEGATION_TRANSACTION_ID']=$transaction
 $process=[Diagnostics.Process]::Start($info)
 $stdout=$process.StandardOutput.ReadToEnd();$stderr=$process.StandardError.ReadToEnd();$process.WaitForExit()
 [IO.File]::WriteAllText((Join-Path $env:LM_B1_ROOT 'enrollment.stdout'),$stdout)
 [IO.File]::WriteAllText((Join-Path $env:LM_B1_ROOT 'enrollment.stderr'),$stderr)
 if($process.ExitCode -ne 0){throw 'Actual nested enrollment failed before isolated TLS boundary'}
 $process.Dispose();'PASS'
}finally{Exit-LabelWriterSessionAuthority $authority}
''', engine=engine, values={"LM_B1_LIBRARY": str(ROOT / "tools/label_writer_fence.ps1"),
                            "LM_B1_ROOT": str(tmp_path), "LM_B1_REPO": str(ROOT),
                            "LM_B1_PYTHON": sys.executable, "LM_B1_CHILD": str(child)})
    (tmp_path / "parent.stdout").write_text(result.stdout, encoding="utf-8")
    (tmp_path / "parent.stderr").write_text(result.stderr, encoding="utf-8")
    assert result.returncode == 0, f"actual enrollment admission failed; evidence: {tmp_path}"


def test_failed_real_registration_does_not_commit_generation(tmp_path):
    """Read-only registration rejection must not create a data generation."""
    run_python(tmp_path, r'''
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from tools import register_label_match_worker_pc as registration
from label_data_generation import marker_paths
from protected_admin import current_process_sid
root=Path(sys.argv[2])
result=registration.main([
    "--apply", "--data-dir", str(root / "direct"), "--sync-dir", str(root / "data"),
    "--producer-install-id", "invalid/install/id", "--report-path", str(root / "report.json")])
assert result != 0
assert (root / "report.json").is_file(), "CLI did not reach registration failure handling"
paths=marker_paths((root / "data", root / "direct", root / "direct/queue"), current_process_sid())
assert all(not path.exists() for path in paths), "failed enrollment committed generation"
''')


def test_failed_onboarding_does_not_commit_generation(tmp_path):
    run_python(tmp_path, r'''
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from current_user_onboarding import CurrentUserOnboardingError, onboard_current_user
root=Path(sys.argv[2]);calls=[]
def registration(paths):
    calls.append(True)
    return 2
try:
    onboard_current_user(Path(sys.argv[1]), require_bootstrap_integrity=False,
        registration_runner=registration, legacy_task_quiescence_reader=lambda: {
            "schema": "label-match-legacy-task-quiescence-v1", "status": "PASS",
            "required_state": "ABSENT_OR_DISABLED", "read_only": True,
            "task_or_process_mutated": False})
except CurrentUserOnboardingError as error:
    assert error.cause_code == "SETUP_FAILED", "onboarding did not reach registration"
else:
    raise AssertionError("failed registration was reported as success")
assert calls == [True], "registration failure boundary was not reached"
assert not list(root.rglob("data-generation.json")), "failed onboarding committed generation"
''')


def test_failed_enrollment_reuses_same_named_current_user_key(monkeypatch):
    """Run actual provisioning and policy checks with an in-memory NCrypt API."""
    import ctypes
    import struct
    from types import SimpleNamespace
    from kmtech_zero_pe import possession_key as cng
    from tests._possession_fixture import TEST_POSSESSION_JWK
    from tools import register_label_match_worker_pc as registration
    import requests

    metadata = {"exists": False, "created": 0, "deleted": 0, "names": []}
    properties = {
        cng.NCRYPT_ALGORITHM_PROPERTY: (cng.BCRYPT_ECDSA_P256_ALGORITHM + "\0").encode("utf-16-le"),
        cng.NCRYPT_UNIQUE_NAME_PROPERTY: "isolated-key\0".encode("utf-16-le"),
        cng.NCRYPT_KEY_TYPE_PROPERTY: struct.pack("<I", 0),
    }

    def handle(pointer, value):
        ctypes.cast(pointer, ctypes.POINTER(cng._NCRYPT_HANDLE))[0] = value

    def open_provider(pointer, _name, _flags):
        handle(pointer, 1)
        return cng.ERROR_SUCCESS

    def open_key(_provider, pointer, name, _legacy, _flags):
        metadata["names"].append(name)
        if not metadata["exists"]:
            return cng.NTE_BAD_KEYSET
        handle(pointer, 2)
        return cng.ERROR_SUCCESS

    def create_key(_provider, pointer, _algorithm, name, _legacy, flags):
        assert flags == 0, "initial enrollment may not overwrite or create a machine key"
        assert not metadata["exists"], "existing key must be reopened"
        metadata.update(exists=True, created=metadata["created"] + 1)
        metadata["names"].append(name)
        handle(pointer, 2)
        return cng.ERROR_SUCCESS

    def set_property(_key, name, buffer, length, _flags):
        properties[name] = ctypes.string_at(buffer, length)
        return cng.ERROR_SUCCESS

    def output(raw, buffer, length, written):
        ctypes.cast(written, ctypes.POINTER(ctypes.c_ulong))[0] = len(raw)
        if buffer is not None:
            assert length >= len(raw)
            ctypes.memmove(buffer, raw, len(raw))
        return cng.ERROR_SUCCESS

    def get_property(_key, name, buffer, length, written, _flags):
        return output(properties[name], buffer, length, written)

    def export_key(_key, _export, kind, _parameters, buffer, length, written, _flags):
        assert kind == cng.BCRYPT_ECCPUBLIC_BLOB, "private key export is forbidden"
        raw = struct.pack("<II", cng.BCRYPT_ECDSA_PUBLIC_P256_MAGIC, cng.P256_BYTES)
        raw += cng.b64url_decode(TEST_POSSESSION_JWK["x"], field="x")
        raw += cng.b64url_decode(TEST_POSSESSION_JWK["y"], field="y")
        return output(raw, buffer, length, written)

    def delete_key(*_args):
        metadata["deleted"] += 1
        metadata["exists"] = False
        return cng.ERROR_SUCCESS

    monkeypatch.setattr(cng, "_ncrypt", SimpleNamespace(
        NCryptOpenStorageProvider=open_provider, NCryptOpenKey=open_key,
        NCryptCreatePersistedKey=create_key, NCryptSetProperty=set_property,
        NCryptGetProperty=get_property, NCryptExportKey=export_key,
        NCryptFinalizeKey=lambda *_args: cng.ERROR_SUCCESS,
        NCryptFreeObject=lambda *_args: cng.ERROR_SUCCESS, NCryptDeleteKey=delete_key))
    first = registration._prepare_possession_key({"producer_identity_source": "generated"})
    monkeypatch.setattr(registration.requests, "post", lambda *_args, **_kwargs: (
        _ for _ in ()).throw(requests.exceptions.SSLError("isolated TLS rejection")))
    with registration.EnrollmentMutex(), pytest.raises(requests.exceptions.SSLError):
        registration._enroll({}, enrollment_url="https://enrollment.example.invalid/",
                             enrollment_token="", timeout_seconds=1)
    second = registration._prepare_possession_key({"producer_identity_source": "generated"})
    assert metadata["exists"] and metadata["created"] == 1 and metadata["deleted"] == 0
    assert first["created"] and not second["created"]
    assert len(set(metadata["names"])) == 1
    assert first["fingerprint"] == second["fingerprint"]
    assert first["scope"] == second["scope"] == "current_user"
    assert first["machine_key"] is second["machine_key"] is False


@pytest.mark.parametrize("engine", ("powershell.exe", "pwsh.exe"))
@pytest.mark.parametrize("stop_present", (False, True))
def test_actual_generation_recovery_optional_stop_proof_is_strict_safe(tmp_path, engine, stop_present):
    """Execute the product's optional-proof statements, without replacing them."""
    result = run_functions(tmp_path, INSTALLER, ["Arg"], r'''
Set-StrictMode -Version Latest
$audit=[ordered]@{rollback=[ordered]@{}}
if($env:LM_B1_STOP_PRESENT -ceq '1'){$audit.rollback.new_relay_stop=[pscustomobject]@{stop_request_sha256=('a'*64)}}
$fn=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Invoke-LabelGenerationRefusalRecovery'},$true))[0]
$block=@($fn.FindAll({param($n) $n -is [Management.Automation.Language.CommandAst] -and $n.GetCommandName() -ceq 'Invoke-LabelRollbackStep' -and $n.CommandElements[1].Extent.Text -ceq "'retained_candidate_readback'"},$true))[0].CommandElements[2].ScriptBlock
$statements=@($block.EndBlock.Statements)
$first=-1;$last=-1
for($i=0;$i -lt $statements.Count;$i++){
 if($statements[$i].Extent.Text -match '^\$stopProof\s*='){$first=$i}
 if($statements[$i].Extent.Text -match '^\$state\s*=\s*HealthyLifecycle'){$last=$i}
}
if($first -lt 0 -or $last -lt $first){throw 'Actual recovery proof statements are missing'}
for($i=$first;$i -lt $last;$i++){. ([scriptblock]::Create($statements[$i].Extent.Text))}
# Execute the actual argument expression without calling the installation
# lifecycle command, which is forbidden on the host.
$call=$statements[$last].Right.PipelineElements[0]
$argument=$call.CommandElements[$call.CommandElements.Count-1].Extent.Text
$digest=. ([scriptblock]::Create($argument))
if($env:LM_B1_STOP_PRESENT -ceq '1'){
 if($mode -cne 'inspect_recovery' -or $digest -cne ('a'*64)){throw 'Existing stop proof was lost'}
}elseif($mode -cne 'inspect' -or $digest -cne ''){throw 'No-relay inspection needs an empty proof'}
'PASS'
''', engine=engine, values={"LM_B1_STOP_PRESENT": "1" if stop_present else "0"})
    (tmp_path / "strict.stdout").write_text(result.stdout, encoding="utf-8")
    (tmp_path / "strict.stderr").write_text(result.stderr, encoding="utf-8")
    assert result.returncode == 0, f"optional relay proof is not StrictMode safe; evidence: {tmp_path}"

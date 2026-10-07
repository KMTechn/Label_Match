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
pytestmark = pytest.mark.skipif(os.name != "nt", reason="native Windows enrollment and rollback boundaries")


def run_python(tmp_path, script):
    child = tmp_path / "generation-child.py"
    child.write_text(script, encoding="utf-8")
    result = subprocess.run([sys.executable, "-B", str(child), str(ROOT), str(tmp_path), str(Path(__file__))],
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
import importlib.util, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import pytest, requests
from current_user_onboarding import CurrentUserOnboardingError, onboard_current_user
from tools import register_label_match_worker_pc as registration
spec=importlib.util.spec_from_file_location("native_fixture", sys.argv[3])
fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)
root=Path(sys.argv[2]);calls=[]
def transport(*args, **kwargs):
    calls.append(True)
    raise requests.exceptions.SSLError("isolated TLS rejection")
with pytest.MonkeyPatch.context() as patch:
    metadata=fixture._synthetic_ncrypt(patch)
    patch.setattr(registration.requests, "post", transport)
    patch.delenv("PRODUCER_SELF_ENROLL_TOKEN", raising=False)
    try:
        onboard_current_user(Path(sys.argv[1]), require_bootstrap_integrity=False,
            legacy_task_quiescence_reader=lambda: {
                "schema": "label-match-legacy-task-quiescence-v1", "status": "PASS",
                "required_state": "ABSENT_OR_DISABLED", "read_only": True,
                "task_or_process_mutated": False})
    except CurrentUserOnboardingError as error:
        assert error.cause_code == "NETWORK_OR_SERVER_UNAVAILABLE", "TLS failure classification was lost"
    else:
        raise AssertionError("failed registration was reported as success")
    assert calls == [True], "actual registration did not reach TLS failure"
    assert metadata["exists"] and metadata["created"] == 1
assert not list(root.rglob("data-generation.json")), "failed onboarding committed generation"
''')


def _synthetic_ncrypt(monkeypatch):
    """Supply only the native keystore API; product provisioning stays real."""
    import ctypes
    import struct
    import base64
    from types import SimpleNamespace
    from kmtech_zero_pe import possession_key as cng
    from kmtech_zero_pe.cng_p256 import generate_public_jwk

    # BCrypt's temporary in-memory key supplies a valid public curve point;
    # the persistent NCrypt store below is entirely synthetic.
    public_jwk = generate_public_jwk()
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
        raw += base64.urlsafe_b64decode(public_jwk["x"] + "=")
        raw += base64.urlsafe_b64decode(public_jwk["y"] + "=")
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
    return metadata


def test_failed_enrollment_reuses_same_named_current_user_key(monkeypatch):
    """Run actual provisioning and policy checks with an in-memory NCrypt API."""
    from tools import register_label_match_worker_pc as registration
    import requests
    metadata = _synthetic_ncrypt(monkeypatch)
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
def test_legacy_manifest_version_fallback_is_strict_safe(tmp_path, engine):
    (tmp_path / "app").mkdir()
    (tmp_path / "app/label_data_generation.py").write_text('CURRENT_VERSION = "v2.0.38"\n', encoding="utf-8")
    result = run_functions(tmp_path, INSTALLER, ["Get-LabelPortableDataVersion"], r'''
Set-StrictMode -Version Latest
$version=Get-LabelPortableDataVersion $env:LM_B1_ROOT ([pscustomobject]@{source_commit=('a'*40)})
if($version -cne 'v2.0.38'){throw 'Verified legacy static version fallback was lost'}
'PASS'
''', engine=engine, values={"LM_B1_ROOT": str(tmp_path)})
    (tmp_path / "fallback.stdout").write_text(result.stdout, encoding="utf-8")
    (tmp_path / "fallback.stderr").write_text(result.stderr, encoding="utf-8")
    assert result.returncode == 0, f"legacy manifest optional version is not StrictMode safe; evidence: {tmp_path}"


@pytest.mark.parametrize("engine", ("powershell.exe", "pwsh.exe"))
@pytest.mark.parametrize("business_opened", (False, True))
def test_failed_registration_generation_gate_and_actual_legacy_folder_restore(tmp_path, engine, business_opened, monkeypatch):
    wrapper = os.environ.get("LM_B1_WRAPPER")
    if not wrapper:
        pytest.skip("the independently pinned PC wrapper is required for legacy folder restoration")
    monkeypatch.setenv("LM_B1_BUSINESS_OPENED", "1" if business_opened else "0")
    run_python(tmp_path, r'''
import importlib.util, json, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import pytest, requests
from current_user_onboarding import CurrentUserOnboardingError, onboard_current_user, resolve_current_user_onboarding_paths
from tools import register_label_match_worker_pc as registration
from label_data_generation import marker_paths, record_user_data_open
from protected_admin import current_process_sid
spec=importlib.util.spec_from_file_location("native_fixture", sys.argv[3])
fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)
root=Path(sys.argv[2]);calls=[]
def transport(*args, **kwargs):
    calls.append(True)
    raise requests.exceptions.SSLError("isolated TLS rejection")
with pytest.MonkeyPatch.context() as patch:
    metadata=fixture._synthetic_ncrypt(patch)
    patch.setattr(registration.requests, "post", transport)
    patch.delenv("PRODUCER_SELF_ENROLL_TOKEN", raising=False)
    try:
        onboard_current_user(Path(sys.argv[1]), require_bootstrap_integrity=False,
            legacy_task_quiescence_reader=lambda: {
                "schema": "label-match-legacy-task-quiescence-v1", "status": "PASS",
                "required_state": "ABSENT_OR_DISABLED", "read_only": True,
                "task_or_process_mutated": False})
    except CurrentUserOnboardingError as error:
        assert error.cause_code == "NETWORK_OR_SERVER_UNAVAILABLE"
    else:
        raise AssertionError("failed registration was reported as success")
    assert calls == [True], "actual registration did not reach TLS failure"
    assert metadata["exists"] and metadata["created"] == 1
paths=resolve_current_user_onboarding_paths(Path(sys.argv[1]))
import os
if os.environ["LM_B1_BUSINESS_OPENED"] == "1":
    record_user_data_open(Path(sys.argv[1]), (paths.data_root,))
    (paths.data_root / "one-business-record.json").write_text('{"quantity":1}\n', encoding="utf-8")
markers=marker_paths((paths.data_root, paths.ledger_path.parent, paths.direct_sync_root,
                     paths.queue_dir, paths.logistics_profile_path.parent), current_process_sid())
(root / "generation-input.json").write_text(json.dumps({"marker_paths":[str(p) for p in markers]}), encoding="utf-8")
''')
    result = run_functions(tmp_path, INSTALLER, ["Assert-LabelRollbackGeneration",
                          "Invoke-LabelRollbackStep", "Get-LabelLaunchFailure", "Protect-LabelFailureText"], r'''
Set-StrictMode -Version Latest
$root=$env:LM_B1_ROOT;$existingVerified=$false
. (Join-Path $env:LM_B1_REPO 'kmtech_shared/powershell/data_generation.ps1')
$generationSubjectSid=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$generationMarkerPaths=@((Get-Content -LiteralPath (Join-Path $root 'generation-input.json') -Raw|ConvertFrom-Json).marker_paths)
$markerHashes=@{};foreach($path in $generationMarkerPaths){if(Test-Path -LiteralPath $path){$markerHashes[$path]=(Get-FileHash -LiteralPath $path).Hash}}
$Script:LabelRollbackSteps=[Collections.Generic.List[object]]::new()
$Script:LabelRollbackErrors=[Collections.Generic.List[object]]::new()
$Script:LabelRollbackFailures=[Collections.Generic.List[object]]::new()
. Invoke-LabelRollbackStep 'generation_before' {Assert-LabelRollbackGeneration}
$wrapperRoot=Split-Path -Parent $env:LM_B1_WRAPPER
. (Join-Path $wrapperRoot 'Redaction.ps1')
. (Join-Path $wrapperRoot 'Connection.ps1')
. $env:LM_B1_WRAPPER
$entryAst=[Management.Automation.Language.Parser]::ParseFile((Join-Path $wrapperRoot 'Install-KmtechPc.ps1'),[ref]$tokens,[ref]$errors)
foreach($name in @('Assert-KmtechOrdinaryPath','Write-KmtechPcSink')){
 $defs=@($entryAst.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $name},$true))
 if($defs.Count -ne 1){throw 'Actual wrapper dependency is ambiguous'}
 . ([scriptblock]::Create($defs[0].Extent.Text))
}
$old=Join-Path $root 'LegacyLM';$renamed=$old+'.retained';$backup=Join-Path $root 'backup'
[void][IO.Directory]::CreateDirectory($renamed);[void][IO.Directory]::CreateDirectory($backup)
$oldFile=Join-Path $old 'old-file.dat';$retained=Join-Path $renamed 'old-file.dat';$copy=Join-Path $backup 'old-file.dat'
[IO.File]::WriteAllBytes($retained,[byte[]]@(0,255,13,10,128,17));Copy-Item -LiteralPath $retained -Destination $copy
$manifest=Join-Path $backup 'manifest.jsonl'
@{source_path=$oldFile;backup_path=$copy;bytes=(Get-Item -LiteralPath $copy).Length;sha256=(Get-FileHash -LiteralPath $copy).Hash}|ConvertTo-Json -Compress|Set-Content -LiteralPath $manifest -Encoding UTF8
$state=@{app='LM';install_phase='UNINSTALLED';backup_root=$backup;log_root=$root;
 entry=@{path=$old;app_dirs=@($old);never_touch=@();untouched_candidate_dirs=@();data_paths=@();config_files=@();backup_required=@();rename_targets=@($old);processes=@();autostart=@()};
 renames=@(@{from_path=$old;to_path=$renamed;phase='RENAMED'});autostart=@();manifest_path=$manifest;
 manifest_sha256=(Get-FileHash -LiteralPath $manifest).Hash;code_retained=$false;status='FAILED';steps=@()}
if($env:LM_B1_BUSINESS_OPENED -ceq '1'){
 if($Script:LabelRollbackErrors.Count -ne 1 -or $Script:LabelRollbackSteps[0].status -cne 'FAILED'){throw 'New generation was allowed to downgrade'}
 $state.install_phase='RECOVERY_REQUIRED';$refused=$false
 try{[void](Invoke-KmtechLegacyRollback $state @{})}catch{$refused=$true}
 if(-not $refused -or (Test-Path -LiteralPath $old) -or -not (Test-Path -LiteralPath $retained)){throw 'Recovery refusal did not preserve old code'}
 foreach($path in $markerHashes.Keys){if((Get-FileHash -LiteralPath $path).Hash -cne $markerHashes[$path]){throw 'Generation refusal changed a marker'}}
}else{
 if($Script:LabelRollbackErrors.Count -ne 0 -or $Script:LabelRollbackSteps[0].status -cne 'PROVEN'){throw 'Failed enrollment unexpectedly advanced generation'}
 # Candidate removal is a separate native installation boundary. This fixture
 # starts after its absence has been confirmed, then runs the real wrapper
 # journal, name restoration and byte verification without product mocks.
 $restored=Invoke-KmtechLegacyRollback $state @{}
 if($restored.Status -cne 'ROLLED_BACK' -or (Test-Path -LiteralPath $renamed) -or -not (Test-Path -LiteralPath $oldFile)){throw 'Actual wrapper did not restore the old folder name'}
 if((Get-FileHash -LiteralPath $oldFile).Hash -cne (Get-FileHash -LiteralPath $copy).Hash){throw 'Actual wrapper did not restore original bytes'}
 if(-not (Test-Path -LiteralPath (Join-Path $backup 'recovery.json'))){throw 'Actual wrapper recovery journal is missing'}
}
'PASS'
''', engine=engine, values={"LM_B1_ROOT": str(tmp_path), "LM_B1_REPO": str(ROOT),
                            "LM_B1_WRAPPER": wrapper, "LM_B1_BUSINESS_OPENED": "1" if business_opened else "0"})
    (tmp_path / "wrapper.stdout").write_text(result.stdout, encoding="utf-8")
    (tmp_path / "wrapper.stderr").write_text(result.stderr, encoding="utf-8")
    assert result.returncode == 0, f"actual generation/wrapper restoration failed; evidence: {tmp_path}"


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

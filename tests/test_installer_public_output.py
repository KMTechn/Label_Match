"""Public installer diagnostics cannot carry input text or stale reports."""
import hashlib
import json
from pathlib import Path
import uuid

import pytest

import current_user_onboarding as onboarding

ROOT = Path(__file__).resolve().parents[1]
VECTORS = json.loads((ROOT / "tests/fixtures/public_output_vectors.json").read_text(encoding="utf-8"))["vectors"]


@pytest.mark.parametrize("cli,status,cause", [
    ("onboarding_main", "FAILED", "SETUP_FAILED"),
    ("onboarding_main", "RECOVERY_REQUIRED", "DATA_GENERATION_REFUSED"),
    ("removal_main", "UNKNOWN", "DATA_GENERATION_REFUSED"),
])
def test_cli_failure_does_not_publish_old_report_or_exception_text(
    monkeypatch, tmp_path, capsys, cli, status, cause,
):
    marker = uuid.uuid4().hex
    old = tmp_path / "old.json"
    old.write_text('{"previous":true}', encoding="utf-8")
    error = onboarding.CurrentUserOnboardingError(
        marker, report_path=old, status=status, cause_code=cause,
    )
    def fail(*args, **kwargs):
        raise error
    monkeypatch.setattr(onboarding, "requires_bootstrap_integrity", lambda root: False)
    monkeypatch.setattr(onboarding, "onboard_current_user" if cli == "onboarding_main"
                        else "remove_current_user_setup", fail)
    assert getattr(onboarding, cli)(["--app-root", str(tmp_path)]) == onboarding.ONBOARDING_EXIT_CODE
    streams = capsys.readouterr()
    assert marker not in streams.out + streams.err, "free-form cause was public"
    assert str(old) not in streams.out + streams.err, "stale report was public"
    assert streams.err.startswith("LABEL_MATCH_INSTALLER_FAILURE=")
    record = json.loads(streams.err.split("=", 1)[1])
    assert record["status"] == status
    assert record["report_status"] == "REPORT_UNAVAILABLE"


def test_atomic_report_reference_matches_published_utf8(tmp_path):
    path = tmp_path / "report.json"
    reference = onboarding._write_json_atomic(path, {"status": "FAILED", "failure": "원인"})
    assert reference == {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def test_public_report_is_bound_only_after_this_attempt_publishes(monkeypatch, tmp_path, capsys):
    error = onboarding.CurrentUserOnboardingError("input", report_path=tmp_path / "report.json")
    onboarding._publish_failure_report(error.report_path, {"status": "FAILED"}, error)
    onboarding._emit_installer_failure(error, "CURRENT_USER_ONBOARDING")
    record = json.loads(capsys.readouterr().err.split("=", 1)[1])
    assert record["report_path"] == str(error.report_path)
    assert record["report_sha256"] == hashlib.sha256(error.report_path.read_bytes()).hexdigest()
    assert "Traceback" in error.report_path.read_text(encoding="utf-8") or "CurrentUserOnboardingError" in error.report_path.read_text(encoding="utf-8")


def test_failed_publication_never_reuses_old_bytes(monkeypatch, tmp_path, capsys):
    path = tmp_path / "report.json"
    original = b'{"previous":true}'
    path.write_bytes(original)
    error = onboarding.CurrentUserOnboardingError("input", report_path=path)
    def refuse(*args, **kwargs):
        raise OSError("publication failed")
    monkeypatch.setattr(onboarding, "_write_json_atomic", refuse)
    onboarding._publish_failure_report(path, {"status": "FAILED"}, error)
    onboarding._emit_installer_failure(error, "CURRENT_USER_REMOVAL")
    record = json.loads(capsys.readouterr().err.split("=", 1)[1])
    assert record["report_status"] == "REPORT_UNAVAILABLE"
    assert "report_path" not in record and "report_sha256" not in record
    assert path.read_bytes() == original


def test_public_record_exact_keys_precede_suffixes_and_reject_free_text():
    from failure_redaction import public_record
    assert public_record({"exit_code": 4, "location_line": 2, "status": "FAILED",
                          "message": uuid.uuid4().hex, "pin": 1234,
                          "bad key": "ignored"}, {"FAILED"}, ()) == {
        "exit_code": 4, "location_line": 2, "status": "FAILED",
        "nonconforming_fields": ["message", "pin"],
        "nonconforming_invalid_key_count": 1,
        "public_record_code": "PUBLIC_FIELD_NONCONFORMING",
    }


def test_public_record_rejects_newlines_and_nested_values():
    from failure_redaction import public_record
    record = public_record({"report_path": "/allowed\n", "exception_type": "Error\n",
                            "stage": {"status": "FAILED"}, "verified": True}, (), ())
    assert record == {"verified": True,
                      "nonconforming_fields": ["report_path", "exception_type", "stage"],
                      "public_record_code": "PUBLIC_FIELD_NONCONFORMING"}


@pytest.mark.parametrize("vector", VECTORS, ids=lambda vector: vector["id"])
def test_python_public_output_contract_vector(vector):
    from failure_redaction import public_record
    actual = public_record(vector["fields"], vector["allowed_codes"], vector["safe_messages"])
    assert list(actual.items()) == list(vector["expected"].items()), "public output contract differs"


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
def test_powershell_public_output_contract_vectors(tmp_path, engine):
    from tests.test_shared_ancestor_protection import run_functions
    output = tmp_path / "counts.json"
    result = run_functions(tmp_path, ROOT / "INSTALL_CANONICAL_PORTABLE.ps1", ["Protect-LabelFailureText"], r'''
$jsonOptions=@{}
if((Get-Command ConvertFrom-Json).Parameters.ContainsKey('DateKind')){$jsonOptions.DateKind='String'}
$vectors=Get-Content -LiteralPath $env:LM_PUBLIC_VECTOR_FILE -Raw -Encoding UTF8 | ConvertFrom-Json @jsonOptions
$passed=0;$failed=0
foreach($vector in $vectors.vectors) {
    $fields=[ordered]@{}
    foreach($property in $vector.fields.PSObject.Properties){$fields[$property.Name]=$property.Value}
    $actual=ConvertTo-KmtechPublicRecord -Fields $fields -AllowedCodes @($vector.allowed_codes) -SafeMessages @($vector.safe_messages)
    if(($actual|ConvertTo-Json -Depth 10 -Compress) -ceq ($vector.expected|ConvertTo-Json -Depth 10 -Compress)){$passed++}else{$failed++}
}
[IO.File]::WriteAllText($env:LM_PUBLIC_COUNTS,([ordered]@{passed=$passed;failed=$failed}|ConvertTo-Json -Compress),[Text.UTF8Encoding]::new($false))
''', engine=engine, values={"LM_PUBLIC_VECTOR_FILE": str(ROOT / "tests/fixtures/public_output_vectors.json"),
                            "LM_PUBLIC_COUNTS": str(output)})
    assert result.returncode == 0, "public vector runner failed"
    counts = json.loads(output.read_text(encoding="utf-8"))
    assert counts == {"passed": len(VECTORS), "failed": 0}


def test_update_notice_never_returns_arbitrary_error_text():
    from auto_update import safe_update_text
    from failure_redaction import RULES
    marker = uuid.uuid4().hex
    assert marker not in safe_update_text(marker)
    assert safe_update_text(marker) == RULES["safe_messages"][0]


def test_public_diagnostic_tree_keeps_recovery_metadata_and_pins_private_cause(tmp_path, monkeypatch):
    from failure_redaction import public_diagnostic_tree
    monkeypatch.setattr("tempfile.tempdir", str(tmp_path))
    marker=uuid.uuid4().hex
    result=public_diagnostic_tree({"transaction_id": "a"*32, "data_preserved": True,
                                   "failure_details": {"message": marker, "type": "RuntimeError"}})
    assert result["transaction_id"] == "a"*32 and result["data_preserved"] is True
    public=result["failure_details"]
    assert marker not in json.dumps(public)
    raw=Path(public["report_path"]).read_bytes()
    assert public["report_sha256"] == hashlib.sha256(raw).hexdigest()
    assert marker.encode() in raw


@pytest.mark.parametrize("operation", ["onboarding", "removal"])
def test_actual_callback_failure_keeps_first_cause_private_and_publishes_digest(monkeypatch, tmp_path, capsys, operation):
    import label_data_generation as generation
    from tests.test_current_user_onboarding import _legacy_task_quiescent
    monkeypatch.setattr(generation, "_installed_identity", lambda root: {
        "version": generation.CURRENT_VERSION, "build_id": "a" * 40,
        "data_compatible_min_version": generation.CURRENT_VERSION, "compatibility_verified": False,
    })
    marker=uuid.uuid4().hex
    def callback_failure(*args):
        raise RuntimeError(marker)
    env={"LOCALAPPDATA": str(tmp_path / "local"), "ProgramData": str(tmp_path / "program")}
    with pytest.raises(onboarding.CurrentUserOnboardingError) as caught:
        if operation == "onboarding":
            onboarding.onboard_current_user(tmp_path / "app", environ=env,
                require_bootstrap_integrity=False, registration_runner=callback_failure,
                legacy_task_quiescence_reader=_legacy_task_quiescent)
        else:
            onboarding.remove_current_user_setup(tmp_path / "app", environ=env,
                autostart_remover=callback_failure, relay_stopper=lambda root: {"status": "ABSENT"},
                scheduled_task_remover=lambda root: {"status": "ABSENT"})
    onboarding._emit_installer_failure(caught.value, "CURRENT_USER_ONBOARDING" if operation == "onboarding" else "CURRENT_USER_REMOVAL")
    public=json.loads(capsys.readouterr().err.split("=",1)[1])
    raw=Path(public["report_path"]).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == public["report_sha256"]
    assert marker not in json.dumps(public)
    protected=json.loads(raw)
    assert "callback_failure" in protected["failure_traceback"]
    assert marker in protected["failure_traceback"]
    assert public["exception_type"] == "builtins.RuntimeError"


def test_argument_parser_preserves_exit_two_without_printing_rejected_input(tmp_path, monkeypatch, capsys):
    import tempfile
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    marker=uuid.uuid4().hex
    with pytest.raises(SystemExit) as caught:
        onboarding.onboarding_main(["--rejected-argument",marker])
    assert caught.value.code == 2
    captured=capsys.readouterr()
    assert marker not in captured.out+captured.err
    assert captured.err.startswith("LABEL_MATCH_SUPPORT_FAILURE=")


@pytest.mark.parametrize("tool", ["check", "install"])
def test_success_profile_summary_omits_url_credentials_and_secret_fields(monkeypatch, tmp_path, capsys, tool):
    import label_data_generation as generation
    import logistics_runtime_profile as profile_module
    from tools import check_logistics_runtime_profile as check
    from tools import install_logistics_runtime_profile as install
    marker=uuid.uuid4().hex
    monkeypatch.setenv("LM_SUMMARY_SECRET", marker)
    monkeypatch.setenv("LM_SUMMARY_TEST_TOKEN", marker)
    monkeypatch.setattr(generation, "check_user_data_open", lambda *args, **kwargs: None)
    monkeypatch.setattr(generation, "record_user_data_open", lambda *args, **kwargs: None)
    malicious_url="https://user:"+marker+"@example.invalid/api?private="+marker+"#"+marker
    if tool == "check":
        profile=profile_module.LogisticsRuntimeProfile(base_url=malicious_url,
            authority_scope="factory",authority_epoch=1,authority_plane="AUTHORITATIVE",
            ledger_plane="AUTHORITATIVE",plane_epoch=1,device_id="device",source_host_id=marker,
            bearer_token=marker,profile_path=str(tmp_path / "profile.json"))
        monkeypatch.setattr(check, "load_logistics_runtime_profile", lambda **kwargs: profile)
        result=check.main(["--profile-path",str(tmp_path / "profile.json")])
    else:
        report={"status":"installed","base_url":malicious_url,"password":marker,
                "delegation_token":marker,"profile_path":str(tmp_path / "profile.json"),
                "secret_values_recorded":False}
        monkeypatch.setattr(install, "install_runtime_profile", lambda **kwargs: report)
        result=install.main(["--profile-path",str(tmp_path / "profile.json"),"--base-url","https://example.invalid",
            "--authority-scope","factory","--authority-epoch","1","--plane-epoch","1",
            "--device-id","device","--source-host-id","host","--token-env","LM_SUMMARY_TEST_TOKEN"])
    assert result == 0
    public=capsys.readouterr().out
    assert marker not in public, "public profile summary exposed synthetic input"
    parsed=json.loads(public)
    assert parsed["base_url"] == "https://example.invalid/api"
    assert parsed["profile_path"] == str(tmp_path / "profile.json")
    if tool == "install":
        assert list(parsed) == sorted(report)
        assert parsed["secret_values_recorded"] is False

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
def test_cli_failure_preserves_fixed_lines_and_rejects_stale_report_binding(
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
    # The baseline status/path lines are category B; only the typed failure
    # record is a binding to same-attempt published diagnostic bytes.
    prefix = "onboarding" if cli == "onboarding_main" else "current_user_removal"
    assert streams.out == f"{prefix}_status={status}\n{prefix}_report={old}\n"
    assert str(old) not in streams.err, "stale report binding was public"
    assert streams.err.startswith("LABEL_MATCH_INSTALLER_FAILURE=")
    record = json.loads(streams.err.split("=", 1)[1])
    assert record["status"] == status
    assert record["report_status"] == "REPORT_UNAVAILABLE"
    assert "report_path" not in record and "report_sha256" not in record


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


@pytest.mark.parametrize("lane", ["support", "update", "legacy"])
def test_actual_failure_boundary_has_no_free_sentence_and_pins_private_report(monkeypatch, tmp_path, capsys, lane):
    import tempfile
    import label_data_generation as generation
    import auto_update
    from tools import check_logistics_runtime_profile as check
    from tools import label_legacy_task_quiescence as legacy
    marker=uuid.uuid4().hex
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    monkeypatch.setattr(generation, "check_user_data_open", lambda *args, **kwargs: None)
    monkeypatch.setattr(generation, "record_user_data_open", lambda *args, **kwargs: None)
    def fail(*args, **kwargs):
        raise RuntimeError(marker)
    if lane == "support":
        monkeypatch.setattr(check, "load_logistics_runtime_profile", fail)
        assert check.main(["--profile-path", str(tmp_path / "profile.json")]) == 2
        public=json.loads(capsys.readouterr().err.split("=",1)[1])
    elif lane == "update":
        monkeypatch.setattr(auto_update, "resolve_lifecycle_context", fail)
        assert auto_update.lifecycle_context_main(["--install-root", "C:/KMTech/Apps/Label_Match"]) == 1
        public=json.loads(capsys.readouterr().err.split("=",1)[1])
    else:
        path=tmp_path / "legacy-report.json"
        assert legacy.main(["--report-path",str(path)],reader=fail) == 4
        assert marker not in capsys.readouterr().out
        report=json.loads(path.read_text())
        assert report["status"] == "FAIL" and report["read_only"] is True
        public=report["failure"]
    assert marker not in json.dumps(public), "failure sentence reached public output"
    raw=Path(public["report_path"]).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == public["report_sha256"]
    assert marker.encode() in raw


def test_secondary_evidence_failure_keeps_original_failure_code(monkeypatch):
    from tools import direct_sync_relay_install_pack as module
    value={"failure_code":"CHILD_EXCEPTION", "inner_exception_type":"RuntimeError",
           "full_exception_trace":{"exception_type":"OSError", "report_status":"REPORT_UNAVAILABLE"}}
    result=module._normalize_failure_diagnostic(value)
    assert result["failure_code"] == "CHILD_EXCEPTION"
    assert result["report_status"] == "REPORT_UNAVAILABLE"


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("fresh", [False, True])
def test_canonical_parent_consumes_typed_reference_and_rejects_stale_digest(tmp_path, engine, fresh):
    from tests.test_shared_ancestor_protection import run_functions
    marker=uuid.uuid4().hex
    report=tmp_path / "onboarding.json"
    report.write_text(marker,encoding="utf-8")
    original=report.read_bytes()
    fields={"status":"FAILED","stage":"CURRENT_USER_ONBOARDING","reason_code":"SETUP_FAILED",
            "exception_type":"builtins.RuntimeError", "location_module":"probe", "location_function":"fail",
            "location_line":7, "report_path":str(report),
            "report_sha256":hashlib.sha256(original).hexdigest() if fresh else "0"*64}
    source=tmp_path / "typed.json"
    source.write_text(json.dumps(fields,separators=(",",":")),encoding="utf-8")
    output=tmp_path / "public.json"
    result=run_functions(tmp_path,ROOT / "INSTALL_CANONICAL_PORTABLE.ps1",
        ["New-LabelChildFailure","Get-LabelLaunchFailure"], r'''
$text='LABEL_MATCH_INSTALLER_FAILURE='+[IO.File]::ReadAllText($env:LM_TYPED_INPUT)
$stdout=[ordered]@{text='';byte_count=0;truncated=$false}
$stderr=[ordered]@{text=$text;byte_count=[Text.Encoding]::UTF8.GetByteCount($text);truncated=$false}
$typedChildFailure=New-LabelChildFailure '--onboard-current-user' 17 $stdout $stderr
$public=Get-LabelLaunchFailure ([pscustomobject]@{Exception=$typedChildFailure}) 'forward'
[IO.File]::WriteAllText($env:LM_TYPED_OUTPUT,($public | ConvertTo-Json -Compress),[Text.UTF8Encoding]::new($false))
''',engine=engine,values={"LM_TYPED_INPUT":str(source),"LM_TYPED_OUTPUT":str(output)})
    assert result.returncode == 0, "typed parent reference consumer failed"
    public=json.loads(output.read_text())
    assert public["exit_code"] == 17 and public["exception_type"] == "builtins.RuntimeError"
    assert marker not in json.dumps(public)
    assert "report_status" not in public
    published=Path(public["report_path"]).read_bytes()
    assert hashlib.sha256(published).hexdigest() == public["report_sha256"]
    assert (Path(public["report_path"]) == report) is fresh
    assert report.read_bytes() == original



@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
def test_not_required_rollback_step_survives_actual_audit_publication(tmp_path, engine):
    from tests.test_shared_ancestor_protection import run_functions
    output = tmp_path / 'audit.json'
    result = run_functions(tmp_path, ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1', ['Save'], r'''
function Get-KmtechSinkEnvironment { return @{} }
$record=[ordered]@{status='FAILED_ROLLED_BACK';current_user_setup_started=$false;
    rollback=[ordered]@{steps=@([ordered]@{phase='current_user_removal';status='NOT_REQUIRED';
        reason_code='NO_CURRENT_USER_SETUP_CREATED'});step_failures=@()}}
Save $env:LM_TYPED_OUTPUT $record
''', engine=engine, values={'LM_TYPED_OUTPUT': str(output)})
    assert result.returncode == 0, 'Isolated audit publication failed'
    record = json.loads(output.read_text(encoding='utf-8'))
    assert record['status'] == 'FAILED_ROLLED_BACK'
    assert record['current_user_setup_started'] is False
    assert record['rollback']['steps'] == [{'phase': 'current_user_removal', 'status': 'NOT_REQUIRED',
                                           'reason_code': 'NO_CURRENT_USER_SETUP_CREATED'}]
    assert record['rollback']['step_failures'] == []


def test_fixed_diagnostic_messages_keep_their_existing_schema():
    from failure_redaction import RULES, public_diagnostic_tree
    original={"status":"PASS", "message":RULES["safe_messages"][0],
              "transaction_id":"a"*32,"data_preserved":True}
    assert public_diagnostic_tree(original) == original


def test_registration_report_privatizes_free_reason_without_changing_recovery_fields(tmp_path):
    from tools import register_label_match_worker_pc as register
    marker=uuid.uuid4().hex
    original={"status":"BLOCKED","blocked_reason":marker,"producer_install_id":"a"*32,
              "server_credential_rotated":False,"local_rollback_performed":False}
    path=tmp_path / "registration.json"
    public=register._write_registration_report(path,original)
    stored=json.loads(path.read_text())
    assert stored==public
    assert all(stored[k]==original[k] for k in original if k!='blocked_reason')
    assert marker not in json.dumps(stored)
    reference=stored['blocked_reason']
    raw=Path(reference['report_path']).read_bytes()
    assert hashlib.sha256(raw).hexdigest()==reference['report_sha256']
    assert marker.encode() in raw



def test_untrusted_server_error_code_stays_in_same_attempt_private_report(tmp_path):
    from tools import register_label_match_worker_pc as register
    marker=uuid.uuid4().hex
    report={"status":"BLOCKED", "server_error_code":marker,"server_http_status":503}
    public=register._write_registration_report(tmp_path / "report.json",report)
    assert public['status']=='BLOCKED' and public['server_http_status']==503
    assert marker not in json.dumps(public)
    reference=public['server_error_code']
    raw=Path(reference['report_path']).read_bytes()
    assert hashlib.sha256(raw).hexdigest()==reference['report_sha256']
    assert marker.encode() in raw


@pytest.mark.parametrize("dry_run", [True, False])
def test_protected_admin_failure_preserves_computed_type_without_exception_body(monkeypatch, tmp_path, capsys, dry_run):
    import getpass
    import label_data_generation as generation
    from tools import install_protected_admin as installer
    marker = uuid.uuid4().hex
    monkeypatch.setattr(generation, "resolve_data_path", lambda value: Path(value))
    monkeypatch.setattr(generation, "check_user_data_open", lambda *args, **kwargs: None)
    monkeypatch.setattr(generation, "record_user_data_open", lambda *args, **kwargs: None)
    monkeypatch.setattr(getpass, "getpass", lambda prompt: uuid.NAMESPACE_DNS.hex)
    def fail(*args, **kwargs):
        raise RuntimeError(marker)
    monkeypatch.setattr(installer, "install_protected_admin_profile", fail)
    arguments = ["--profile-path", str(tmp_path / "profile.json"), "--reader-principal", "S-1-5-21-100-200-300-400"]
    if dry_run:
        arguments.append("--dry-run")
    assert installer.main(arguments) == 2
    streams = capsys.readouterr()
    assert streams.out == "" and streams.err == "BLOCKED: RuntimeError\n"
    assert marker not in streams.out + streams.err

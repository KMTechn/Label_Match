"""Cloud-review regressions; credential material is generated only at runtime."""
import contextlib
import hashlib
import json
from pathlib import Path
import secrets
import shutil
import subprocess

import pytest

import auto_update
import current_user_onboarding
import failure_redaction

ROOT = Path(__file__).resolve().parents[1]
GAPS = ("\n", "\n ", "\n\t", "\r\n", "\r\n ", "\r\n\t", "\t\r\n", "\n\n", "\r\n\r\n")
HEADERS = ("Authorization", "Proxy-Authorization", "HTTP_AUTHORIZATION", "X-App-Authorization")


def escape_cases():
    cases = []
    for shape in ("plain_backslash", "json_escaped_slash", "json_quoted_basic", "json_trailing_backslash"):
        fragments = [secrets.token_urlsafe(24), secrets.token_urlsafe(24)] if shape in ("plain_backslash", "json_escaped_slash") else [secrets.token_urlsafe(32)]
        material = ("\\" if shape == "plain_backslash" else "/").join(fragments)
        if shape == "json_trailing_backslash":
            material += "\\"
        scheme = "Bearer" if shape in ("plain_backslash", "json_escaped_slash") else "Basic"
        text = "Authorization: " + scheme + " " + material
        if shape.startswith("json"):
            text = json.dumps({"detail": 'curl -H "' + text + '" x'})
            if shape == "json_escaped_slash":
                text = text.replace("/", r"\/")
        cases.append(dict(kind=shape, shape=shape, text=text, material=material, fragments=fragments,
                          suffix="", json=shape.startswith("json")))
    return cases


def review_cases():
    cases = []
    for header in HEADERS:
        for gap in GAPS:
            material = secrets.token_urlsafe(32)
            cases.append(dict(kind="newline", text=header + ": Bearer" + gap + material,
                              material=material, suffix="", json=False))
    for scheme, key in (("Token", "token"), ("Bearer", "access_token"), ("Basic", "password")):
        for quote in ('"', "'"):
            material = secrets.token_urlsafe(32)
            text = "Authorization: " + scheme + " " + key + "=" + quote + material + quote
            cases.append(dict(kind="nested", text=text, material=material, suffix="", json=False))
    for scheme in ("Basic", "Bearer", "Token", "Negotiate"):
        material = secrets.token_urlsafe(32)
        text = json.dumps({"detail": 'curl -H "Authorization: ' + scheme + " " + material + '" x'})
        cases.append(dict(kind="json", text=text, material=material, suffix="", json=True))
        material = secrets.token_urlsafe(32)
        text = json.dumps({"detail": "Authorization: " + scheme + ' password="' + material + '"'})
        cases.append(dict(kind="nested_json", text=text, material=material, suffix="", json=True))
    for key, suffix in (("proxy_authorization", " winerror=5 exit_code=7"),
                        ("authorization_status", " errno=13")):
        cases.append(dict(kind="diagnostic", text=key + ": denied" + suffix,
                          material="", suffix=suffix, json=False))
    cases.extend(escape_cases())
    for case in cases:
        case.setdefault("fragments", [case["material"]] if case["material"] else [])
    return cases


def require_absent(case, safe):
    raw = safe if isinstance(safe, bytes) else safe.encode("utf-8")
    if any(fragment.encode("utf-8") in raw for fragment in case["fragments"]):
        pytest.fail("credential residue", pytrace=False)


def require_case(case, safe):
    require_absent(case, safe)
    if case["suffix"] and not safe.endswith(case["suffix"]):
        pytest.fail("diagnostic changed", pytrace=False)
    if case["json"]:
        decoded = json.loads(safe)
        assert isinstance(decoded, dict)
        if case["kind"] == "json_trailing_backslash":
            assert "\\" not in decoded["detail"]


@pytest.mark.parametrize("case", review_cases(), ids=lambda case: case["kind"])
def test_review_regressions_python(case):
    require_case(case, failure_redaction.redact_for_sink(case["text"], {}))
    # A known but unrelated value exercises the JSON validity refusal path.
    require_case(case, failure_redaction.redact_for_sink(
        case["text"], {}, redact_values=(secrets.token_urlsafe(32),)))


@pytest.mark.parametrize("case", review_cases(), ids=lambda case: case["kind"])
def test_review_failure_streams_and_support(tmp_path, monkeypatch, case):
    error = OSError(5, case["text"])
    error.winerror, error.exit_code, error.returncode = 32, 7, 7
    # subprocess text mode normalizes CRLF to LF before these real sinks run.
    error.stdout = error.stderr = case["text"].replace("\r\n", "\n")
    streams = auto_update.child_failure_output(error)
    assert streams["returncode"] == 7
    for stream in (streams["stdout"], streams["stderr"]):
        require_absent(case, stream)
    middle, outer = RuntimeError("middle"), RuntimeError("outer")
    outer.__cause__, middle.__context__ = middle, error
    details = failure_redaction.protected_failure_details(outer, {})
    require_absent(case, json.dumps(details))
    launch_details = auto_update.launch_failure_details(outer, include_original=True)
    assert len(launch_details) == 3
    for detail in launch_details:
        raw = Path(detail["report_path"]).read_bytes()
        require_absent(case, raw)
        assert hashlib.sha256(raw).hexdigest() == detail["report_sha256"]
        reference = json.loads(raw)["full_exception_trace"]
        trace = Path(reference["path"]).read_bytes()
        require_absent(case, trace)
    path = tmp_path / "support.json"
    monkeypatch.setattr(current_user_onboarding, "writer_admission", lambda *_a, **_k: contextlib.nullcontext())
    support_error = RuntimeError(case["text"])
    current_user_onboarding._publish_failure_report(path, {"status": "FAILED"}, support_error)
    raw = path.read_bytes()
    require_absent(case, raw)
    assert json.loads(raw)["status"] == "FAILED"
    assert hashlib.sha256(raw).hexdigest() == support_error._failure_report_ref["sha256"]
    assert error.errno == 5 and error.winerror == 32 and error.exit_code == 7


@pytest.mark.parametrize("engine", ("powershell", "pwsh"))
def test_review_regressions_powershell(tmp_path, engine):
    executable = shutil.which(engine)
    assert executable is not None
    cases_path = tmp_path / "cases.json"
    cases_path.write_text(json.dumps(review_cases()), encoding="utf-8")
    script = tmp_path / "probe.ps1"
    script.write_text(r'''
param([string]$Repo,[string]$Cases)
$ErrorActionPreference='Stop'
trap {
    $diagnostic=@{exception_type=$_.Exception.GetType().FullName;line=$_.InvocationInfo.ScriptLineNumber}
    [IO.File]::WriteAllText([IO.Path]::Combine($PSScriptRoot,'driver-error.json'),($diagnostic|ConvertTo-Json -Compress))
    exit 9
}
. ([IO.Path]::Combine($Repo,'tools/invoke_embedded_python.ps1'))
function Test-CaseAbsent([string]$Text,$Case){
    foreach($fragment in $Case.fragments){if($Text.Contains($fragment)){return $false}}
    return $true
}
$inputs=ConvertFrom-Json -InputObject ([IO.File]::ReadAllText($Cases))
$rows=@(foreach($case in $inputs){
    $safe=ConvertTo-KmtechRedactedSinkText -Text $case.text -Environment @{}
    $native=[ComponentModel.Win32Exception]::new(32,[string]$case.text)
    $middle=[InvalidOperationException]::new('middle',$native)
    $outer=[Exception]::new('outer',$middle)
    $details=New-InstallerFailureDiagnostic -CommandIdentity 'probe' -ChildExitCode 7 -FailureCode 'CHILD_EXCEPTION' -Exception $outer
    $trace=[IO.File]::ReadAllText($details.full_exception_trace.path)
    $sink=Write-InstallerFailureEvidence -Text $case.text
    $readback=[IO.File]::ReadAllText($sink.path)
    $valid=$true
    $trailing=$true
    if($case.json){
        try{
            $decoded=ConvertFrom-Json -InputObject $safe -ErrorAction Stop
            if($case.kind -eq 'json_trailing_backslash'){$trailing=-not $decoded.detail.Contains([string][char]92)}
        }catch{$valid=$false;$trailing=$false}
    }
    [pscustomobject]@{
        kind=$case.kind
        absent=(Test-CaseAbsent $safe $case)
        deep_absent=(Test-CaseAbsent $trace $case)
        support_absent=(Test-CaseAbsent $readback $case)
        native=($native.NativeErrorCode -eq 32 -and $details.child_exit_code -eq 7)
        sha=((Get-FileHash -LiteralPath $sink.path -Algorithm SHA256).Hash.ToLowerInvariant() -ceq $sink.sha256)
        diagnostic=(-not $case.suffix -or $safe.EndsWith($case.suffix))
        valid_json=$valid
        trailing_backslash_absent=$trailing
    }
})
ConvertTo-Json -InputObject $rows -Compress
''', encoding="utf-8")
    completed = subprocess.run(
        [executable, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(script),
         "-Repo", str(ROOT), "-Cases", str(cases_path)], capture_output=True,
        text=True, encoding="utf-8", errors="replace", timeout=290)
    if completed.returncode:
        (tmp_path / "driver-error.json").write_text(json.dumps({
            "rc": completed.returncode,
            "security_exception": "PSSecurityException" in completed.stderr,
            "command_missing": "CommandNotFoundException" in completed.stderr,
            "runtime_exception": "RuntimeException" in completed.stderr,
        }), encoding="utf-8")
        pytest.fail("PowerShell probe failed", pytrace=False)
    rows = json.loads(completed.stdout)
    (tmp_path / "booleans.json").write_text(json.dumps(rows), encoding="utf-8")
    if not all(all(row[field] for field in ("absent", "deep_absent", "support_absent", "native", "sha", "diagnostic", "valid_json", "trailing_backslash_absent")) for row in rows):
        pytest.fail("PowerShell regression", pytrace=False)

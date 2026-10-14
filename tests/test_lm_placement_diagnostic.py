"""Execute the production parent diagnostic parser under StrictMode."""
import json
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _parse(tmp_path, engine, payload, *, child_exit=73, writer_fails=False):
    executable = shutil.which(engine)
    if not executable:
        pytest.fail("PowerShell engine unavailable")
    source = (ROOT / "INSTALL_CANONICAL_PORTABLE.ps1").read_text(encoding="utf-8-sig")
    start = source.index("    if (Test-Path -LiteralPath $failurePath -PathType Leaf)",
                         source.index("function InvokeFrozenPlacementHelper"))
    end = source.index("    return $childExit", start)
    parser = source[start:end]
    failure = tmp_path / "failure.json"
    original = payload if isinstance(payload, str) else json.dumps(payload)
    failure.write_text(original, encoding="utf-8")
    quote = lambda value: "'" + str(value).replace("'", "''") + "'"
    driver = tmp_path / "parse.ps1"
    driver.write_text("Set-StrictMode -Version Latest\n$ErrorActionPreference='Stop'\n"
        "$Script:LabelPlacementChildFailure=$null\n"
        "$Script:KmtechRedactionRules=@{safe_messages=@('support')}\n"
        "$Script:saved=$null\n"
        "function Write-KmtechProtectedFailureSidecar([string]$Text){"
        + ("throw [IO.IOException]::new('evidence unavailable')" if writer_fails else
           "$Script:saved=$Text;return @{path='protected';sha256=('a'*64)}") + "}\n"
        + "$failurePath=" + quote(failure) + "\n$childExit=" + str(child_exit) + "\n"
        + "try {\n" + parser + "\n"
        + "@{exit=$childExit;record=$Script:LabelPlacementChildFailure;original=$Script:saved}"
        "|ConvertTo-Json -Depth 12 -Compress\n} catch {"
        "@{failure_type=$_.Exception.GetType().Name;exit=$childExit}|ConvertTo-Json -Compress}\n",
        encoding="utf-8-sig")
    child = subprocess.run([executable, "-NoLogo", "-NoProfile", "-NonInteractive",
        "-File", str(driver)], cwd=tmp_path, capture_output=True,
        creationflags=subprocess.CREATE_NO_WINDOW)
    assert child.returncode == 0
    return json.loads(child.stdout.decode("utf-8-sig")), original


@pytest.mark.parametrize("engine", ["powershell", "pwsh"])
@pytest.mark.parametrize("payload", [None, [], {}, {"exit_code": 73},
    {"schema": "label-match-placement-child-failure-v1"},
    {"schema": "label-match-placement-child-failure-v1", "exit_code": None},
    {"schema": "label-match-placement-child-failure-v1", "exit_code": "73"},
    {"status": "FAILED", "reason_code": "INSTALLER_FAILED", "exception_type": "FirstCause"},
    "{broken-json"])
def test_unknown_diagnostic_preserves_child_exit_and_first_cause(tmp_path, engine, payload):
    result, original = _parse(tmp_path, engine, payload)
    assert "failure_type" not in result
    assert result["exit"] == result["record"]["exit_code"] == 73
    assert result["record"]["diagnostic_format"] == "진단 형식 모름"
    assert result["original"] == original
    assert result["record"]["full_exception_trace"]["path"] == "protected"


@pytest.mark.parametrize("engine", ["powershell", "pwsh"])
def test_known_diagnostic_retains_original_reason_and_type(tmp_path, engine):
    payload = {"schema": "label-match-placement-child-failure-v1", "exit_code": 73,
               "placement_code": "PLACEMENT_LAUNCH_FAILED", "exception_type": "FirstCause"}
    result, _ = _parse(tmp_path, engine, payload)
    assert result["record"] == payload
    assert result["original"] is None


@pytest.mark.parametrize("engine", ["powershell", "pwsh"])
def test_secondary_writer_failure_does_not_replace_child_failure(tmp_path, engine):
    result, _ = _parse(tmp_path, engine, {}, writer_fails=True)
    assert "failure_type" not in result
    assert result["exit"] == result["record"]["exit_code"] == 73
    assert result["record"]["diagnostic_format"] == "진단 형식 모름"
    assert result["record"]["trace_failure_type"] == "IOException"


@pytest.mark.parametrize("engine", ["powershell", "pwsh"])
@pytest.mark.parametrize("payload,child_exit", [
    ({"schema": "label-match-placement-child-failure-v1", "exit_code": 72}, 73),
    ({}, 0),
])
def test_diagnostic_binding_guards_still_reject(tmp_path, engine, payload, child_exit):
    result, _ = _parse(tmp_path, engine, payload, child_exit=child_exit)
    assert result["failure_type"] != "PropertyNotFoundException"
    assert result["exit"] == child_exit

"""Public placement diagnostics accept the reader's existing optional fields."""
import json
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("engine", ["powershell", "pwsh"])
@pytest.mark.parametrize("missing", ["phase", "exception_type", "placement_code", "trace_path", "trace_sha", "none", "valid_trace"])
def test_optional_child_fields_preserve_exit_and_available_first_cause(tmp_path, engine, missing):
    executable = shutil.which(engine)
    assert executable is not None
    source = (ROOT / "INSTALL_CANONICAL_PORTABLE.ps1").read_text(encoding="utf-8-sig")
    start = source.index("function ConvertTo-LabelPlacementPublicFailure(")
    end = source.index("\nfunction InvokeFrozenPlacementHelper", start)
    record = {"phase": "CHILD_EXIT", "placement_code": "PLACEMENT_LAUNCH_FAILED",
              "exit_code": 73, "exception_type": "FirstCause"}
    if missing == "trace_path":
        record["full_exception_trace"] = {"sha256": "a" * 64}
    elif missing == "trace_sha":
        record["full_exception_trace"] = {"path": "protected-reference"}
    elif missing == "valid_trace":
        record["full_exception_trace"] = {"path": "protected-reference", "sha256": "a" * 64}
    elif missing != "none":
        del record[missing]
    payload = tmp_path / "diagnostic.json"
    payload.write_text(json.dumps(record), encoding="utf-8")
    quote = lambda value: "'" + str(value).replace("'", "''") + "'"
    driver = tmp_path / "optional.ps1"
    driver.write_text("Set-StrictMode -Version Latest\n$ErrorActionPreference='Stop'\n"
        "function ConvertTo-KmtechPublicRecord($Fields){return $Fields}\n"
        + source[start:end] + "\n$record=Get-Content -LiteralPath " + quote(payload)
        + " -Raw|ConvertFrom-Json\n$first=ConvertTo-LabelPlacementPublicFailure $record\n"
        "$second=ConvertTo-LabelPlacementPublicFailure $record\n"
        "@{first=$first;second=$second}|ConvertTo-Json -Depth 10 -Compress\n", encoding="utf-8-sig")
    child = subprocess.run([executable, "-NoLogo", "-NoProfile", "-NonInteractive", "-File", str(driver)],
                           cwd=tmp_path, capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    assert child.returncode == 0
    result = json.loads(child.stdout.decode("utf-8-sig"))
    assert result["first"] == result["second"]
    assert json.loads(payload.read_bytes()) == record
    public = result["first"]
    assert public["exit_code"] == 73
    assert public["location_function"] == ("PLACEMENT_CHILD" if missing == "phase" else "CHILD_EXIT")
    assert public["placement_code"] == ("PLACEMENT_DIAGNOSTIC_UNAVAILABLE" if missing == "placement_code" else "PLACEMENT_LAUNCH_FAILED")
    assert public["exception_type"] == ("System.InvalidOperationException" if missing == "exception_type" else "FirstCause")
    if missing == "valid_trace":
        assert public["report_path"] == "protected-reference" and public["report_sha256"] == "a" * 64
        assert "report_status" not in public
    else:
        assert public["report_status"] == "REPORT_UNAVAILABLE"
        assert "report_path" not in public and "report_sha256" not in public

import base64
import json
import os
from pathlib import Path
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
RELEASE_GATE = ROOT / "tools" / "prepublish_release_gate.ps1"
BOOTSTRAP_INTEGRITY = ROOT / "tools" / "bootstrap_integrity.ps1"
POWERSHELL = (
    Path(os.environ["SystemRoot"])
    / "System32"
    / "WindowsPowerShell"
    / "v1.0"
    / "powershell.exe"
)


def _ps_literal(value: Path) -> str:
    return str(value).replace("'", "''")


def _run_windows_powershell(script: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            str(POWERSHELL),
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(script),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )


def _run_release_boolean_guard(
    tmp_path: Path, field: str, value: object
) -> subprocess.CompletedProcess[str]:
    source = RELEASE_GATE.read_text(encoding="utf-8-sig")
    functions = source[
        source.index("function Get-RequiredExternalBoolean") : source.index(
            "$resolvedWorkRoot ="
        )
    ]
    release = {
        "id": "release-1",
        "tag_name": "v1.2.3",
        "name": "Release v1.2.3",
        "draft": False,
        "prerelease": True,
        "immutable": True,
        "target_commitish": "a" * 40,
        "body": "frozen release",
        "assets": [
            {
                "name": "Label.zip",
                "id": "asset-1",
                "size": 123,
                "digest": "sha256:" + "b" * 64,
                "state": "uploaded",
            },
            {
                "name": "Label.zip.sha256",
                "id": "asset-2",
                "size": 64,
                "digest": "sha256:" + "c" * 64,
                "state": "uploaded",
            },
        ],
    }
    state = {
        "release_id": release["id"],
        "target_commitish": release["target_commitish"],
        "body": release["body"],
        "assets": release["assets"],
    }
    release[field] = value
    payload = base64.b64encode(
        json.dumps({"release": release, "state": state}).encode("utf-8")
    ).decode("ascii")
    harness = tmp_path / "release-boolean-guard.ps1"
    harness.write_text(
        f"""
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$Tag = 'v1.2.3'
{functions}
$json = (New-Object Text.UTF8Encoding($false, $true)).GetString(
    [Convert]::FromBase64String('{payload}')
)
$inputObject = $json | ConvertFrom-Json
try {{
    Assert-ReleaseMatchesState `
        -Release $inputObject.release `
        -State $inputObject.state `
        -ExpectedDraft $false `
        -ExpectedImmutable $true
    Write-Output 'guard_result=accepted'
    exit 0
}}
catch {{
    Write-Output ('guard_error=' + [string]$_.Exception.Message)
    exit 7
}}
""",
        encoding="utf-8-sig",
    )
    return _run_windows_powershell(harness)


@pytest.mark.parametrize(
    ("field", "value"),
    [("draft", False), ("prerelease", True), ("immutable", True)],
)
def test_release_gate_accepts_literal_boolean_controls(
    tmp_path: Path, field: str, value: bool
) -> None:
    completed = _run_release_boolean_guard(tmp_path, field, value)

    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert "guard_result=accepted" in completed.stdout


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("draft", value)
        for value in ("false", "0", "", "null", None, 0, 1, [], {})
    ]
    + [("prerelease", "false"), ("immutable", "0")],
    ids=[
        "draft-string-false",
        "draft-string-zero",
        "draft-empty-string",
        "draft-string-null",
        "draft-json-null",
        "draft-integer-zero",
        "draft-integer-one",
        "draft-array",
        "draft-object",
        "prerelease-string-false",
        "immutable-string-zero",
    ],
)
def test_release_gate_rejects_actual_non_boolean_sentinels(
    tmp_path: Path, field: str, value: object
) -> None:
    completed = _run_release_boolean_guard(tmp_path, field, value)

    assert completed.returncode == 7
    assert f"guard_error=External boolean has invalid type: {field}" in (
        completed.stdout
    )


def _run_bootstrap_file_count_guard(
    tmp_path: Path, value: object | None, *, use_record_value: bool = False
) -> subprocess.CompletedProcess[str]:
    root = tmp_path / "portable"
    (root / "app").mkdir(parents=True)
    (root / "runtime").mkdir()
    (root / "app" / "main.py").write_text("print('fixture')\n", encoding="utf-8")
    (root / "runtime" / "pythonw.exe").write_bytes(b"signed-runtime-fixture")
    payload = base64.b64encode(json.dumps({"value": value}).encode("utf-8")).decode(
        "ascii"
    )
    harness = tmp_path / "bootstrap-file-count-guard.ps1"
    mutation = "" if use_record_value else "$record.file_count = $sentinel.value"
    harness.write_text(
        f"""
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
. '{_ps_literal(BOOTSTRAP_INTEGRITY)}'
$root = '{_ps_literal(root)}'
$record = Write-BootstrapIntegrityRecord -Root $root -CodeRoot '.'
$json = (New-Object Text.UTF8Encoding($false, $true)).GetString(
    [Convert]::FromBase64String('{payload}')
)
$sentinel = $json | ConvertFrom-Json
{mutation}
Write-BootstrapUtf8Json `
    -Path (Join-Path $root 'bootstrap-integrity.json') `
    -Payload $record
try {{
    Assert-BootstrapIntegrityRecord -Root $root | Out-Null
    Write-Output 'guard_result=accepted'
    exit 0
}}
catch {{
    Write-Output ('guard_error=' + [string]$_.Exception.Message)
    exit 7
}}
""",
        encoding="utf-8-sig",
    )
    return _run_windows_powershell(harness)


def test_bootstrap_integrity_accepts_literal_integer_control(tmp_path: Path) -> None:
    completed = _run_bootstrap_file_count_guard(
        tmp_path, None, use_record_value=True
    )

    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert "guard_result=accepted" in completed.stdout


@pytest.mark.parametrize(
    "value",
    ["false", "0", "", "null", None, False, 1.0, [], {}],
    ids=[
        "string-false",
        "string-zero",
        "empty-string",
        "string-null",
        "json-null",
        "boolean-false",
        "floating-point",
        "array",
        "object",
    ],
)
def test_bootstrap_integrity_rejects_actual_non_integer_sentinels(
    tmp_path: Path, value: object
) -> None:
    completed = _run_bootstrap_file_count_guard(tmp_path, value)

    assert completed.returncode == 7
    assert "guard_error=External integer has invalid type: file_count" in (
        completed.stdout
    )


def test_bootstrap_integrity_rejects_wrong_literal_integer(tmp_path: Path) -> None:
    completed = _run_bootstrap_file_count_guard(tmp_path, 0)

    assert completed.returncode == 7
    assert "guard_error=Bootstrap integrity record file count is invalid." in (
        completed.stdout
    )

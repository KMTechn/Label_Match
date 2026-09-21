import base64
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
RELEASE_GATE = ROOT / "tools" / "prepublish_release_gate.ps1"
BOOTSTRAP_INTEGRITY = ROOT / "tools" / "bootstrap_integrity.ps1"


def _trusted_windows_powershell() -> Path:
    unavailable = "Windows PowerShell unavailable: "
    if os.name != "nt":
        pytest.skip(unavailable + "Windows is required")
    system_root = os.environ.get("SystemRoot", "")
    if not system_root.strip() or not Path(system_root).is_absolute():
        pytest.skip(unavailable + "SystemRoot must be an absolute Windows root")

    # Ask Windows itself; neither an environment root nor PATH is authority.
    import ctypes
    from ctypes import wintypes

    try:
        kernel32 = ctypes.WinDLL(
            "kernel32.dll", use_last_error=True, winmode=0x00000800
        )  # LOAD_LIBRARY_SEARCH_SYSTEM32
        get_system_directory = kernel32.GetSystemDirectoryW
        get_system_directory.argtypes = [wintypes.LPWSTR, wintypes.UINT]
        get_system_directory.restype = wintypes.UINT
        buffer = ctypes.create_unicode_buffer(32768)
        length = get_system_directory(buffer, len(buffer))
        if not 0 < length < len(buffer):
            pytest.skip(unavailable + "Windows system directory lookup failed")
        system_directory = Path(buffer.value)
        if (
            not system_directory.is_absolute()
            or Path(system_root) / "System32" != system_directory
        ):
            pytest.skip(
                unavailable + "SystemRoot differs from the Windows system directory"
            )
        executable = system_directory / "WindowsPowerShell" / "v1.0" / "powershell.exe"
        if executable.resolve(strict=True) != executable or not executable.is_file():
            pytest.skip(unavailable + "system executable is missing or redirected")
    except (OSError, ValueError, RuntimeError):
        pytest.skip(unavailable + "system executable cannot be verified")
    return executable


def _ps_literal(value: Path) -> str:
    return str(value).replace("'", "''")


def _run_windows_powershell(script: Path) -> subprocess.CompletedProcess[str]:
    executable = _trusted_windows_powershell()
    # Preserve full diagnostics on disk, without unbounded PIPE accumulation.
    stdout_path = script.with_suffix(".stdout.txt")
    stderr_path = script.with_suffix(".stderr.txt")
    with stdout_path.open("xb") as stdout, stderr_path.open("xb") as stderr:
        completed = subprocess.run(
            [
                str(executable),
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(script),
            ],
            check=False,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            cwd=script.parent,
            timeout=30,
        )
    if max(stdout_path.stat().st_size, stderr_path.stat().st_size) > 65536:
        pytest.fail("PowerShell diagnostics exceeded 64 KiB; preserved beside the script")
    return subprocess.CompletedProcess(
        completed.args,
        completed.returncode,
        stdout_path.read_text(),
        stderr_path.read_text(),
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
. '{_ps_literal(BOOTSTRAP_INTEGRITY)}' -SharedCodeRoot '{_ps_literal(ROOT)}'
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


@pytest.mark.parametrize("root_kind", ["missing", "empty", "relative", "forged"])
def test_unavailable_powershell_import_refuses_polluted_paths(
    tmp_path: Path, root_kind: str
) -> None:
    # A new interpreter sees the bad environment before it imports this module.
    # An audit hook rejects launch attempts even if a decoy is not a valid PE.
    forged_root = tmp_path / "forged-windows"
    path_directory = tmp_path / "polluted-path"
    for executable in (
        tmp_path / "powershell.exe",
        path_directory / "powershell.exe",
        tmp_path / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe",
        forged_root / "System32" / "WindowsPowerShell" / "v1.0" / "powershell.exe",
    ):
        executable.parent.mkdir(parents=True, exist_ok=True)
        executable.write_text("untrusted executable sentinel\n", encoding="ascii")
    environment = {
        key: value
        for key, value in os.environ.items()
        if key.upper() not in {"SYSTEMROOT", "PSMODULEPATH"}
    }
    if root_kind != "missing":
        environment["SystemRoot"] = {
            "empty": "",
            "relative": forged_root.name,
            "forged": str(forged_root),
        }[root_kind]
    environment.update(
        PATH=str(path_directory),
        TEMP=str(tmp_path),
        TMP=str(tmp_path),
        PSModulePath=str(tmp_path / "empty-modules"),
    )
    (tmp_path / "empty-modules").mkdir()
    probe = tmp_path / "import-capability.py"
    probe.write_text(
        """
import importlib.util
from pathlib import Path
import sys

attempts = []
def forbid_launch(event, args):
    if event in ("subprocess.Popen", "os.system", "os.spawn", "os.exec"):
        attempts.append(event)
        raise RuntimeError("unexpected executable launch")
sys.addaudithook(forbid_launch)

import pytest
spec = importlib.util.spec_from_file_location("scalar_probe", sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
print("import=ok")
try:
    module._run_windows_powershell(Path(sys.argv[2]))
except pytest.skip.Exception as error:
    assert str(error).startswith("Windows PowerShell unavailable:"), str(error)
    assert not attempts, attempts
    print("capability=unavailable;launch_attempts=0")
else:
    raise AssertionError("untrusted environment was accepted")
""",
        encoding="utf-8",
    )
    harness = tmp_path / "must-not-run.ps1"
    harness.write_text("throw 'untrusted environment was executed'\n", encoding="ascii")
    stdout_path = tmp_path / "probe.stdout.txt"
    stderr_path = tmp_path / "probe.stderr.txt"
    with stdout_path.open("xb") as stdout, stderr_path.open("xb") as stderr:
        # A Windows venv launcher may itself wait for the base interpreter.
        # Retain it through natural completion; killing it could orphan that child.
        with subprocess.Popen(
            [
                sys.executable, "-I", "-B", str(probe),
                str(Path(__file__).resolve()), str(harness),
            ],
            env=environment,
            cwd=tmp_path,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
        ) as process:
            try:
                returncode = process.wait()
            finally:
                process.wait()
    assert max(stdout_path.stat().st_size, stderr_path.stat().st_size) <= 65536
    output, error = stdout_path.read_text(), stderr_path.read_text()
    assert returncode == 0, error or output
    assert error == ""
    assert output == "import=ok\ncapability=unavailable;launch_attempts=0\n"

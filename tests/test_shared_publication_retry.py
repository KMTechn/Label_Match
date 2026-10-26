"""Marker publication retries a locked target for one second (Windows errors 5/32 only)."""
import json
from pathlib import Path
import subprocess

import pytest

from kmtech_shared import data_generation as generation

APP = "Label_Match"
SID = "S-1-5-21-219"
BUILD = "a" * 40
ROOT = Path(__file__).resolve().parents[1]


def record(path, version="v2.0.99", **options):
    return generation.record_generation(path, version, app_id=APP, subject_sid=SID, build_id=options.pop("build_id", BUILD), **options)


def run_powershell(arguments):
    return subprocess.run(arguments, capture_output=True, text=True, encoding="utf-8", timeout=120,
                          creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))


def _lock_error(code):
    # OSError(errno, strerror, filename, winerror) sets .winerror on Windows.
    return OSError(0, "synthetic native lock", None, code)


@pytest.mark.parametrize("code", [5, 32])
def test_publication_retries_native_lock_then_succeeds(tmp_path, monkeypatch, code):
    path = tmp_path / "marker.json"
    record(path, "v2.0.98")
    original = generation.os.replace
    calls = []

    def locked_once(source, target):
        calls.append(code)
        if len(calls) == 1:
            raise _lock_error(code)
        return original(source, target)

    monkeypatch.setattr(generation.os, "replace", locked_once)
    result = record(path)
    assert len(calls) == 2
    assert result["version"] == "v2.0.99"
    assert json.loads(path.read_bytes())["version"] == "v2.0.99"
    assert not list(tmp_path.glob("*.tmp.*"))


def test_publication_other_errors_fail_at_once(tmp_path, monkeypatch):
    path = tmp_path / "marker.json"
    record(path, "v2.0.98")
    before = path.read_bytes()
    calls = []

    def other(source, target):
        calls.append(1)
        raise _lock_error(87)

    monkeypatch.setattr(generation.os, "replace", other)
    with pytest.raises(OSError) as raised:
        record(path)
    assert raised.value.winerror == 87
    assert len(calls) == 1
    assert path.read_bytes() == before
    assert not list(tmp_path.glob("*.tmp.*"))


def test_publication_gives_up_after_one_second_with_last_native_error(tmp_path, monkeypatch):
    path = tmp_path / "marker.json"
    record(path, "v2.0.98")
    before = path.read_bytes()
    calls = []

    def always_locked(source, target):
        calls.append(1)
        raise _lock_error(32)

    monkeypatch.setattr(generation.os, "replace", always_locked)
    started = generation.time.monotonic()
    with pytest.raises(OSError) as raised:
        record(path)
    elapsed = generation.time.monotonic() - started
    assert raised.value.winerror == 32
    assert 1.0 <= elapsed < 3.0
    assert len(calls) >= 2
    assert generation.ATOMIC_REPLACE_RETRY_SECONDS == 1.0
    assert path.read_bytes() == before
    assert not list(tmp_path.glob("*.tmp.*"))


def _publish_driver(tmp_path, body):
    helper = str(ROOT / "kmtech_shared/powershell/data_generation.ps1").replace("'", "''")
    driver = tmp_path / "publish.ps1"
    driver.write_text(f"""
$ErrorActionPreference='Stop'
. '{helper}'
{body}
""", encoding="utf-8-sig")
    return driver


def _literal(path):
    return str(path).replace("'", "''")


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
def test_powershell_write_retries_locked_publication_then_succeeds(tmp_path, engine):
    marker = tmp_path / "marker.json"
    record(marker, "v2.0.98")
    driver = _publish_driver(tmp_path, f"""
$script:originalRead=(Get-Item Function:Read-KmtechDataGeneration).ScriptBlock
$script:readCalls=0
$script:holder=$null
function Read-KmtechDataGeneration([string]$Path,[string]$AppId,[string]$SubjectSid) {{
    $script:readCalls++
    $value = & $script:originalRead $Path $AppId $SubjectSid
    if ($script:readCalls -eq 1) {{
        # Lock the target after the pre-publication read and release it from another runspace.
        $stream=[IO.File]::Open('{_literal(marker)}',[IO.FileMode]::Open,[IO.FileAccess]::Read,[IO.FileShare]::None)
        $script:holder=[powershell]::Create()
        [void]$script:holder.AddScript({{ param($s) Start-Sleep -Milliseconds 300; $s.Dispose() }}).AddArgument($stream)
        [void]$script:holder.BeginInvoke()
    }}
    return $value
}}
$result = Write-KmtechDataGeneration '{_literal(marker)}' 'v2.0.99' '{APP}' '{SID}' -BuildId '{BUILD}'
@{{version=$result.version;reads=$script:readCalls}} | ConvertTo-Json -Compress
""")
    result = run_powershell([engine, "-NoProfile", "-NonInteractive", "-File", str(driver)])
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["version"] == "v2.0.99"
    assert json.loads(marker.read_bytes())["version"] == "v2.0.99"
    assert not list(tmp_path.glob("*.tmp.*"))


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
def test_powershell_publication_other_errors_fail_at_once_and_lock_gives_up(tmp_path, engine):
    marker = tmp_path / "marker.json"
    record(marker, "v2.0.98")
    before = marker.read_bytes()
    missing = tmp_path / "absent.tmp"
    temporary = tmp_path / "pending.tmp"
    temporary.write_bytes(b"{}")
    driver = _publish_driver(tmp_path, f"""
$watch=[Diagnostics.Stopwatch]::StartNew()
$other='none'
try {{ Publish-KmtechDataGenerationDocument '{_literal(missing)}' '{_literal(marker)}' }}
catch {{ $other=$_.Exception.GetType().Name }}
$otherSeconds=$watch.Elapsed.TotalSeconds
$stream=[IO.File]::Open('{_literal(marker)}',[IO.FileMode]::Open,[IO.FileAccess]::Read,[IO.FileShare]::None)
$watch.Restart()
$locked='none'
try {{ Publish-KmtechDataGenerationDocument '{_literal(temporary)}' '{_literal(marker)}' }}
catch {{ $locked=$_.Exception.GetType().Name }}
finally {{ $stream.Dispose() }}
@{{other=$other;otherSeconds=$otherSeconds;locked=$locked;lockedSeconds=$watch.Elapsed.TotalSeconds}} | ConvertTo-Json -Compress
""")
    result = run_powershell([engine, "-NoProfile", "-NonInteractive", "-File", str(driver)])
    assert result.returncode == 0, result.stderr
    observed = json.loads(result.stdout)
    assert observed["other"] != "none" and observed["otherSeconds"] < 0.5
    assert observed["locked"] != "none" and 0.9 <= observed["lockedSeconds"] < 3.0
    assert marker.read_bytes() == before

"""Exercise the installer transport with real, hidden Python processes only."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys

import pytest

INSTALLER = Path(__file__).resolve().parents[1] / "INSTALL_CANONICAL_PORTABLE.ps1"


def _quote(value):
    return "'" + str(value).replace("'", "''") + "'"


@pytest.fixture(params=["powershell.exe", "pwsh.exe"])
def engine(request):
    executable = shutil.which(request.param)
    if executable is None:
        pytest.skip(request.param + " is unavailable")
    return executable


def _run(tmp_path, engine, probe, *, expected=None, python=None, args=()):
    # Read only the two function definitions, never evaluate installer entrypoints.
    source = INSTALLER.read_text(encoding="utf-8-sig")
    assert "function Invoke-LabelPinnedProbe(" in source
    payload = tmp_path / "probe.bin"
    payload.write_bytes(probe)
    script = tmp_path / "run.ps1"
    arguments = ",".join(_quote(arg) for arg in args)
    script.write_text(
        "$ErrorActionPreference='Stop'\n"
        "$tokens=$null; $errors=$null\n"
        f"$ast=[Management.Automation.Language.Parser]::ParseFile({_quote(INSTALLER)},[ref]$tokens,[ref]$errors)\n"
        "if ($errors.Count) { throw 'INSTALLER_PARSE_FAILED' }\n"
        "$ast.FindAll({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst] "
        "-and $n.Name -in @('Arg','Invoke-LabelPinnedProbe')},$false) | "
        "ForEach-Object { . ([scriptblock]::Create($_.Extent.Text)) }\n"
        f"$bytes=[IO.File]::ReadAllBytes({_quote(payload)})\n"
        f"Invoke-LabelPinnedProbe -Python {_quote(python or sys.executable)} -ProbeBytes $bytes "
        f"-ExpectedSha256 {_quote(expected or hashlib.sha256(probe).hexdigest())} "
        f"-ProbeArguments @({arguments})\n",
        encoding="utf-8-sig",
    )
    return subprocess.run(
        [engine, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(script)],
        capture_output=True, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )


def test_large_probe_uses_exact_utf8_bytes_and_preserves_argv(tmp_path, engine):
    probe = ("#" + "검사" * 66000 + "\nimport json, sys\n"
             "print(json.dumps({'value':'포장','argv':sys.argv[1:]}))\n").encode("utf-8")
    args = ["space and path", "끝", "한국어"]
    result = _run(tmp_path, engine, probe, args=args)
    assert result.returncode == 0, result.stderr.decode(errors="replace")
    assert json.loads(result.stdout) == {"value": "포장", "argv": args}


def test_hash_mismatch_rejects_before_process_start(tmp_path, engine):
    marker = tmp_path / "executed"
    probe = f"from pathlib import Path\nPath({str(marker)!r}).write_text('bad')\n".encode()
    result = _run(tmp_path, engine, probe, expected="0" * 64, python=tmp_path / "missing-python.exe")
    assert result.returncode != 0
    assert b"PINNED_PROBE_SHA256_MISMATCH" in result.stderr
    assert not marker.exists()


def test_probe_result_matches_commandline_and_error_is_preserved(tmp_path, engine):
    probe = b"import json,sys\nprint(json.dumps({'accepted':sys.argv[1]=='yes'}))\n"
    result = _run(tmp_path, engine, probe, args=["yes"])
    legacy = subprocess.run([sys.executable, "-I", "-B", "-c", probe.decode(), "yes"],
                            capture_output=True, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    assert result.returncode == legacy.returncode == 0
    assert json.loads(result.stdout) == json.loads(legacy.stdout)
    rejected = _run(tmp_path, engine, b"import sys\nsys.stderr.write('REJECTED_PROBE')\nsys.exit(7)\n")
    assert rejected.returncode != 0
    assert b"REJECTED_PROBE" in rejected.stderr


def test_installer_probe_bytes_match_transport_pins():
    source = INSTALLER.read_text(encoding="utf-8-sig")
    for name in ("Assert-WriterTransition", "HealthyLifecycle"):
        function = source[source.index("function " + name + "("):]
        start = function.index("$probe = @'\n") + len("$probe = @'\n")
        end = function.index("\n'@", start)
        probe = function[start:end].encode("utf-8")
        tail = function[end: function.index("\nfunction ", end)]
        pin = re.search(r"-ExpectedSha256 '([0-9a-f]{64})'", tail)
        assert pin is not None, name
        assert hashlib.sha256(probe).hexdigest() == pin.group(1), name
        assert "Invoke-LabelPinnedProbe" in tail
        assert "-c " not in tail

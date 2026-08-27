from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]
RUNNER_RELATIVE_PATH = Path("tools/run_frozen_release_candidate_once.ps1")
RELEASE_SURFACES = (
    RUNNER_RELATIVE_PATH,
    Path("tools/build_frozen_release_candidate.ps1"),
    Path("INSTALL_THIS_PC.ps1"),
)


def _windows_powershell_51() -> Path:
    if os.name != "nt":
        pytest.skip("Windows PowerShell 5.1 prelaunch proof is Windows-only")
    powershell = (
        Path(os.environ.get("SystemRoot", r"C:\Windows"))
        / "System32/WindowsPowerShell/v1.0/powershell.exe"
    )
    if not powershell.is_file():
        pytest.skip("Windows PowerShell 5.1 is unavailable")
    return powershell


def _copy_release_surfaces(destination: Path) -> Path:
    source_root = destination / "source"
    for relative_path in RELEASE_SURFACES:
        target = source_root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / relative_path, target)
    return source_root


def _preflight_fixture(tmp_path: Path) -> dict[str, Path]:
    source_root = _copy_release_surfaces(tmp_path)
    wheelhouse = tmp_path / "wheelhouse"
    mirror = tmp_path / "mirror.git"
    log_root = tmp_path / "logs"
    process_temp = tmp_path / "process-temp"
    for directory in (wheelhouse, mirror, log_root, process_temp):
        directory.mkdir()

    child_marker = tmp_path / "child-launched.txt"
    child = tmp_path / "must-not-launch.cmd"
    child.write_text(
        "@echo off\r\n"
        f'> "{child_marker}" echo launched\r\n'
        "exit /b 99\r\n",
        encoding="ascii",
    )
    return {
        "source_root": source_root,
        "runner": source_root / RUNNER_RELATIVE_PATH,
        "wheelhouse": wheelhouse,
        "mirror": mirror,
        "output": tmp_path / "candidate",
        "stdout": log_root / "builder.stdout.log",
        "stderr": log_root / "builder.stderr.log",
        "process_temp": process_temp,
        "child": child,
        "child_marker": child_marker,
    }


def _run_preflight(powershell: Path, fixture: dict[str, Path]) -> subprocess.CompletedProcess[str]:
    environment = {
        **os.environ,
        "TEMP": str(fixture["process_temp"]),
        "TMP": str(fixture["process_temp"]),
    }
    return subprocess.run(
        [
            str(powershell),
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(fixture["runner"]),
            "-PowerShellPath",
            str(fixture["child"]),
            "-OutputRoot",
            str(fixture["output"]),
            "-Tag",
            "v2.0.89",
            "-PythonPath",
            sys.executable,
            "-Wheelhouse",
            str(fixture["wheelhouse"]),
            "-MirrorRoot",
            str(fixture["mirror"]),
            "-StdoutPath",
            str(fixture["stdout"]),
            "-StderrPath",
            str(fixture["stderr"]),
            "-PreflightOnly",
        ],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=30,
        env=environment,
    )


def _assert_child_was_not_launched(fixture: dict[str, Path]) -> None:
    assert not fixture["child_marker"].exists()
    assert not fixture["output"].exists()


def test_release_runner_registers_every_governing_powershell_surface_for_ast_scan():
    source = (ROOT / RUNNER_RELATIVE_PATH).read_text(encoding="utf-8")

    assert "System.Management.Automation.Language.Parser]::ParseFile" in source
    assert "System.Management.Automation.Language.CommandParameterAst" in source
    assert "System.Management.Automation.Language.BinaryExpressionAst" in source
    assert "Ambiguous Test-Path Boolean expression is forbidden" in source
    for relative_path in RELEASE_SURFACES[1:]:
        assert relative_path.name in source


def test_windows_powershell_51_preflight_scans_exact_clean_surfaces_without_launch(tmp_path):
    powershell = _windows_powershell_51()
    fixture = _preflight_fixture(tmp_path)

    completed = _run_preflight(powershell, fixture)

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == "release_runner_prelaunch=PASS surfaces=3"
    assert completed.stderr == ""
    _assert_child_was_not_launched(fixture)
    assert not fixture["stdout"].exists()
    assert not fixture["stderr"].exists()


def test_windows_powershell_51_preflight_rejects_test_path_boolean_parameter_without_launch(
    tmp_path,
):
    powershell = _windows_powershell_51()
    fixture = _preflight_fixture(tmp_path)
    installer = fixture["source_root"] / "INSTALL_THIS_PC.ps1"
    installer.write_text(
        installer.read_text(encoding="utf-8")
        + "\nif (Test-Path $env:TEMP -or Test-Path $env:TMP) { throw 'unreachable' }\n",
        encoding="utf-8",
    )

    completed = _run_preflight(powershell, fixture)

    assert completed.returncode != 0
    assert "Ambiguous Test-Path Boolean expression is forbidden" in (
        completed.stdout + completed.stderr
    )
    _assert_child_was_not_launched(fixture)
    assert not fixture["stdout"].exists()
    assert not fixture["stderr"].exists()


@pytest.mark.parametrize("existing_log", ("stdout", "stderr"))
def test_windows_powershell_51_preflight_rejects_each_existing_log_without_launch(
    tmp_path, existing_log
):
    powershell = _windows_powershell_51()
    fixture = _preflight_fixture(tmp_path)
    fixture[existing_log].write_text("preexisting\n", encoding="utf-8")

    completed = _run_preflight(powershell, fixture)

    assert completed.returncode != 0
    assert f"builder {existing_log} log must be absent" in (
        completed.stdout + completed.stderr
    )
    _assert_child_was_not_launched(fixture)
    other_log = "stderr" if existing_log == "stdout" else "stdout"
    assert not fixture[other_log].exists()

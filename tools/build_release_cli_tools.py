#!/usr/bin/env python
"""Build and fail-closed verify the three Label_Match release CLI tools."""

from __future__ import annotations

import argparse
import ctypes
from ctypes import wintypes
from dataclasses import dataclass, field
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
from typing import Sequence
import uuid


REPO_ROOT = Path(__file__).resolve().parents[1]
REPORT_NAME = "release_cli_tools_manifest.json"
POST_SIGN_REPORT_NAME = "release_cli_tools_post_sign_manifest.json"
LOGISTICS_PROFILE_EXECUTABLE_NAMES = (
    "KMTech_Logistics_Profile_Install.exe",
    "KMTech_Logistics_Profile_Check.exe",
)


class ReleaseCliBuildError(RuntimeError):
    """Raised when an artifact cannot be proven safe to package."""


@dataclass(frozen=True)
class ToolSpec:
    name: str
    source_rel: str
    help_marker: str
    mode: str

    @property
    def executable_name(self) -> str:
        return f"{self.name}.exe"


@dataclass
class BuiltTool:
    spec: ToolSpec
    payload_root: Path
    executable: Path
    executable_sha256: str
    executable_size: int
    payload_inventory: list[dict[str, object]]
    help_runs: list[dict[str, object]]
    archive_verification: dict[str, object] = field(default_factory=dict)


TOOL_SPECS = (
    ToolSpec(
        "direct_sync_relay_runner",
        "tools/direct_sync_relay_runner.py",
        "Label_Match direct-sync relay runner",
        "onefile",
    ),
    ToolSpec(
        "direct_sync_relay_install_pack",
        "tools/direct_sync_relay_install_pack.py",
        "Label_Match direct-sync relay scheduled-task install pack",
        "onedir",
    ),
    ToolSpec(
        "register_label_match_worker_pc",
        "tools/register_label_match_worker_pc.py",
        "Register this Label_Match PC as an HTTPS producer",
        "onefile",
    ),
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _payload_inventory(payload_root: Path) -> list[dict[str, object]]:
    if payload_root.is_file():
        paths = [payload_root]
        base = payload_root.parent
    else:
        paths = sorted((path for path in payload_root.rglob("*") if path.is_file()), key=lambda path: path.relative_to(payload_root).as_posix().lower())
        base = payload_root
    return [
        {
            "path": path.relative_to(base).as_posix(),
            "size": path.stat().st_size,
            "sha256": _sha256(path),
        }
        for path in paths
    ]


def _run_checked(command: Sequence[str], *, cwd: Path, timeout: float) -> subprocess.CompletedProcess[bytes]:
    completed = subprocess.run(
        [str(part) for part in command],
        cwd=cwd,
        check=False,
        capture_output=True,
        timeout=timeout,
    )
    if completed.returncode != 0:
        stderr = completed.stderr.decode("utf-8", errors="replace").strip()
        stdout = completed.stdout.decode("utf-8", errors="replace").strip()
        raise ReleaseCliBuildError(stderr or stdout or f"command failed with exit code {completed.returncode}")
    return completed


def _verify_pe(executable: Path) -> None:
    with executable.open("rb") as handle:
        header = handle.read(64)
        if len(header) < 64 or header[:2] != b"MZ":
            raise ReleaseCliBuildError(f"not a Windows PE executable: {executable}")
        pe_offset = int.from_bytes(header[0x3C:0x40], "little")
        handle.seek(pe_offset)
        if handle.read(4) != b"PE\0\0":
            raise ReleaseCliBuildError(f"missing PE signature: {executable}")


def _verify_carchive(spec: ToolSpec, executable: Path, *, repo_root: Path) -> dict[str, object]:
    completed = _run_checked(
        [sys.executable, "-m", "PyInstaller.utils.cliutils.archive_viewer", "-l", str(executable)],
        cwd=repo_root,
        timeout=60,
    )
    stderr = completed.stderr.decode("utf-8", errors="replace")
    stdout = completed.stdout.decode("utf-8", errors="replace")
    if stderr.strip():
        raise ReleaseCliBuildError(f"archive viewer wrote stderr for {spec.name}: {stderr.strip()}")
    required_markers = (
        "Options in",
        "(PKG/CArchive)",
        "Contents of",
        f"'s', '{spec.name}'",
    )
    missing = [marker for marker in required_markers if marker not in stdout]
    if missing:
        raise ReleaseCliBuildError(f"CArchive markers missing for {spec.name}: {missing}")
    if spec.mode == "onefile" and "'z', 'PYZ.pyz'" not in stdout:
        raise ReleaseCliBuildError(f"onefile PYZ payload missing for {spec.name}")
    return {
        "status": "PASS",
        "viewer_stdout_sha256": hashlib.sha256(completed.stdout).hexdigest(),
        "viewer_stderr_bytes": len(completed.stderr),
    }


def _normalize_path(path: Path | str) -> str:
    return os.path.normcase(os.path.abspath(os.fspath(path)))


class _ProcessEntry32W(ctypes.Structure):
    _fields_ = [
        ("dwSize", wintypes.DWORD),
        ("cntUsage", wintypes.DWORD),
        ("th32ProcessID", wintypes.DWORD),
        ("th32DefaultHeapID", ctypes.c_size_t),
        ("th32ModuleID", wintypes.DWORD),
        ("cntThreads", wintypes.DWORD),
        ("th32ParentProcessID", wintypes.DWORD),
        ("pcPriClassBase", ctypes.c_long),
        ("dwFlags", wintypes.DWORD),
        ("szExeFile", wintypes.WCHAR * 260),
    ]


def _process_entries() -> list[tuple[int, str]]:
    if os.name != "nt":
        raise ReleaseCliBuildError("release CLI process verification requires Windows")
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_snapshot = kernel32.CreateToolhelp32Snapshot
    create_snapshot.argtypes = [wintypes.DWORD, wintypes.DWORD]
    create_snapshot.restype = wintypes.HANDLE
    process_first = kernel32.Process32FirstW
    process_first.argtypes = [wintypes.HANDLE, ctypes.POINTER(_ProcessEntry32W)]
    process_first.restype = wintypes.BOOL
    process_next = kernel32.Process32NextW
    process_next.argtypes = [wintypes.HANDLE, ctypes.POINTER(_ProcessEntry32W)]
    process_next.restype = wintypes.BOOL
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [wintypes.HANDLE]
    close_handle.restype = wintypes.BOOL

    snapshot = create_snapshot(0x00000002, 0)
    invalid_handle = ctypes.c_void_p(-1).value
    if snapshot == invalid_handle:
        raise ReleaseCliBuildError(f"CreateToolhelp32Snapshot failed: {ctypes.get_last_error()}")
    entries: list[tuple[int, str]] = []
    try:
        entry = _ProcessEntry32W()
        entry.dwSize = ctypes.sizeof(entry)
        ctypes.set_last_error(0)
        has_entry = bool(process_first(snapshot, ctypes.byref(entry)))
        if not has_entry:
            error = ctypes.get_last_error()
            if error not in (0, 18):  # ERROR_NO_MORE_FILES
                raise ReleaseCliBuildError(f"Process32FirstW failed: {error}")
        while has_entry:
            entries.append((int(entry.th32ProcessID), str(entry.szExeFile)))
            ctypes.set_last_error(0)
            has_entry = bool(process_next(snapshot, ctypes.byref(entry)))
            if not has_entry:
                error = ctypes.get_last_error()
                if error not in (0, 18):  # ERROR_NO_MORE_FILES
                    raise ReleaseCliBuildError(f"Process32NextW failed: {error}")
    finally:
        close_handle(snapshot)
    return entries


def _image_pids_for_path(executable: Path, *, ignored_pids: Sequence[int] = ()) -> list[int]:
    if os.name != "nt":
        raise ReleaseCliBuildError("release CLI process verification requires Windows")
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    open_process = kernel32.OpenProcess
    open_process.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    open_process.restype = wintypes.HANDLE
    query_name = kernel32.QueryFullProcessImageNameW
    query_name.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.LPWSTR, ctypes.POINTER(wintypes.DWORD)]
    query_name.restype = wintypes.BOOL
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [wintypes.HANDLE]
    close_handle.restype = wintypes.BOOL

    target = _normalize_path(executable)
    target_name = executable.name.casefold()
    ignored = {int(pid) for pid in ignored_pids}
    matches: list[int] = []
    for pid, image_name in _process_entries():
        if pid in ignored or image_name.casefold() != target_name:
            continue
        ctypes.set_last_error(0)
        handle = open_process(0x1000, False, pid)
        if not handle:
            raise ReleaseCliBuildError(
                f"OpenProcess failed for new matching executable {image_name} pid={pid}: "
                f"{ctypes.get_last_error()}"
            )
        try:
            size = wintypes.DWORD(32768)
            buffer = ctypes.create_unicode_buffer(size.value)
            ctypes.set_last_error(0)
            if not query_name(handle, 0, buffer, ctypes.byref(size)):
                raise ReleaseCliBuildError(
                    f"QueryFullProcessImageNameW failed for new pid={pid}: {ctypes.get_last_error()}"
                )
            if _normalize_path(buffer.value) == target:
                matches.append(pid)
        finally:
            close_handle(handle)
    return sorted(set(matches))


def _taskkill(pid: int) -> None:
    subprocess.run(
        ["taskkill.exe", "/PID", str(int(pid)), "/T", "/F"],
        check=False,
        capture_output=True,
    )


def _terminate_and_verify_zero(
    executable: Path,
    root_pid: int | None,
    *,
    baseline_pids: Sequence[int] = (),
) -> list[int]:
    if root_pid:
        _taskkill(root_pid)
    observed: set[int] = set()
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        pids = _image_pids_for_path(executable, ignored_pids=baseline_pids)
        observed.update(pids)
        if not pids:
            return sorted(observed)
        for pid in pids:
            _taskkill(pid)
        time.sleep(0.2)
    remaining = _image_pids_for_path(executable, ignored_pids=baseline_pids)
    if remaining:
        raise ReleaseCliBuildError(f"residual processes remain for {executable}: {remaining}")
    return sorted(observed)


def _copy_probe_payload(built: BuiltTool, probe_root: Path) -> Path:
    if built.spec.mode == "onefile":
        probe_executable = probe_root / built.spec.executable_name
        shutil.copy2(built.executable, probe_executable)
    else:
        probe_payload = probe_root / built.spec.name
        shutil.copytree(built.payload_root, probe_payload)
        probe_executable = probe_payload / built.spec.executable_name
    if _sha256(probe_executable) != built.executable_sha256:
        raise ReleaseCliBuildError(f"probe copy hash mismatch for {built.spec.name}")
    return probe_executable


def _run_isolated_help(
    built: BuiltTool,
    *,
    run_no: int,
    timeout_seconds: float,
    smoke_root: Path,
) -> dict[str, object]:
    probe_root = smoke_root / f"{built.spec.name}-{run_no}-{uuid.uuid4().hex}"
    probe_root.mkdir(parents=True)
    probe_executable = _copy_probe_payload(built, probe_root)
    env_root = probe_root / "env"
    env = dict(os.environ)
    for key in list(env):
        if key.startswith("_PYI_") or key in {"PYTHONHOME", "PYTHONPATH"}:
            env.pop(key, None)
    for key in ("APPDATA", "LOCALAPPDATA", "PROGRAMDATA", "TEMP", "TMP"):
        target = env_root / key
        target.mkdir(parents=True, exist_ok=True)
        env[key] = str(target)

    baseline_pids = [
        pid
        for pid, image_name in _process_entries()
        if image_name.casefold() == probe_executable.name.casefold()
    ]

    started = time.monotonic()
    process = subprocess.Popen(
        [str(probe_executable), "--help"],
        cwd=probe_root,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    timed_out = False
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        timed_out = True
        stdout = b""
        stderr = b""
    elapsed_ms = round((time.monotonic() - started) * 1000)
    observed_residuals = _terminate_and_verify_zero(
        probe_executable,
        process.pid if timed_out or process.poll() is None else None,
        baseline_pids=baseline_pids,
    )
    if timed_out:
        raise ReleaseCliBuildError(
            f"{built.spec.name} help probe {run_no} timed out after {elapsed_ms} ms"
        )
    if observed_residuals:
        raise ReleaseCliBuildError(
            f"{built.spec.name} help probe {run_no} left residual processes: {observed_residuals}"
        )
    if process.returncode != 0:
        raise ReleaseCliBuildError(
            f"{built.spec.name} help probe {run_no} exited {process.returncode}"
        )
    if stderr.strip():
        raise ReleaseCliBuildError(f"{built.spec.name} help probe {run_no} wrote stderr")
    text = stdout.decode("utf-8", errors="replace")
    if "usage:" not in text.lower() or built.spec.help_marker not in text:
        raise ReleaseCliBuildError(f"{built.spec.name} help probe {run_no} missing expected usage text")
    return {
        "run": run_no,
        "status": "PASS",
        "elapsed_ms": elapsed_ms,
        "returncode": process.returncode,
        "stdout_bytes": len(stdout),
        "stderr_bytes": len(stderr),
        "probe_executable_sha256": _sha256(probe_executable),
        "residual_process_count": 0,
    }


def _pyinstaller_command(
    spec: ToolSpec,
    *,
    repo_root: Path,
    source: Path,
    work_path: Path,
    dist_path: Path,
    spec_path: Path,
) -> list[str]:
    mode_flag = "--onefile" if spec.mode == "onefile" else "--onedir"
    return [
        sys.executable,
        "-m",
        "PyInstaller",
        "--name",
        spec.name,
        mode_flag,
        "--console",
        "--paths",
        str(repo_root),
        "--clean",
        "--noupx",
        "--noconfirm",
        "--workpath",
        str(work_path),
        "--distpath",
        str(dist_path),
        "--specpath",
        str(spec_path),
        str(source),
    ]


def _build_candidate(spec: ToolSpec, *, repo_root: Path, tool_root: Path) -> BuiltTool:
    source = repo_root / spec.source_rel
    if not source.is_file():
        raise ReleaseCliBuildError(f"source missing for {spec.name}: {source}")
    work_path = tool_root / "work"
    dist_path = tool_root / "dist"
    spec_path = tool_root / "spec"
    for path in (work_path, dist_path, spec_path):
        path.mkdir(parents=True, exist_ok=False)
    command = _pyinstaller_command(
        spec,
        repo_root=repo_root,
        source=source,
        work_path=work_path,
        dist_path=dist_path,
        spec_path=spec_path,
    )
    _run_checked(command, cwd=repo_root, timeout=900)
    if spec.mode == "onefile":
        payload_root = dist_path / spec.executable_name
        executable = payload_root
    else:
        payload_root = dist_path / spec.name
        executable = payload_root / spec.executable_name
    if not executable.is_file():
        raise ReleaseCliBuildError(f"PyInstaller output missing for {spec.name}: {executable}")
    _verify_pe(executable)
    archive_verification = _verify_carchive(spec, executable, repo_root=repo_root)
    return BuiltTool(
        spec=spec,
        payload_root=payload_root,
        executable=executable,
        executable_sha256=_sha256(executable),
        executable_size=executable.stat().st_size,
        payload_inventory=_payload_inventory(payload_root),
        help_runs=[],
        archive_verification=archive_verification,
    )


def _git_value(repo_root: Path, *args: str) -> str:
    return _run_checked(["git", *args], cwd=repo_root, timeout=30).stdout.decode("utf-8").strip()


def _verify_clean_checkout(repo_root: Path) -> None:
    status = _git_value(repo_root, "status", "--porcelain=v1", "--untracked-files=all")
    if status:
        raise ReleaseCliBuildError("release CLI tools require a clean Git checkout")


def _app_version(repo_root: Path) -> str:
    import ast

    tree = ast.parse((repo_root / "Label_Match.py").read_text(encoding="utf-8"))
    values = [
        node.value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "APP_VERSION" for target in node.targets)
        and isinstance(node.value, ast.Constant)
        and isinstance(node.value.value, str)
    ]
    if len(values) != 1:
        raise ReleaseCliBuildError(f"expected one literal APP_VERSION, found {len(values)}")
    return values[0]


def _copy_and_compare_payload(built: BuiltTool, stage: Path) -> Path:
    if built.spec.mode == "onefile":
        target = stage / built.spec.executable_name
        shutil.copy2(built.executable, target)
    else:
        target = stage / built.spec.name
        shutil.copytree(built.payload_root, target)
    if _payload_inventory(target) != built.payload_inventory:
        raise ReleaseCliBuildError(f"payload changed while publishing {built.spec.name}")
    return target


def _publish_atomically(
    built_tools: Sequence[BuiltTool],
    *,
    destination: Path,
    repo_root: Path,
    help_timeout_seconds: float = 15.0,
    probe_count: int = 3,
) -> dict[str, object]:
    destination = destination.resolve()
    if destination.exists():
        raise ReleaseCliBuildError(f"destination already exists: {destination}")
    if not destination.parent.is_dir():
        raise ReleaseCliBuildError(f"destination parent does not exist: {destination.parent}")
    stage = Path(tempfile.mkdtemp(prefix=f".{destination.name}.release-stage-", dir=destination.parent))
    try:
        tool_reports: list[dict[str, object]] = []
        for built in built_tools:
            if built.archive_verification.get("status") != "PASS":
                raise ReleaseCliBuildError(f"CArchive verification evidence is missing for {built.spec.name}")
            if len(built.help_runs) != probe_count or any(
                run.get("status") != "PASS" for run in built.help_runs
            ):
                raise ReleaseCliBuildError(f"isolated help probe evidence is incomplete for {built.spec.name}")
            _copy_and_compare_payload(built, stage)
            tool_reports.append(
                {
                    "name": built.spec.name,
                    "source": built.spec.source_rel,
                    "mode": built.spec.mode,
                    "executable_sha256": built.executable_sha256,
                    "executable_size": built.executable_size,
                    "payload_inventory": built.payload_inventory,
                    "help_runs": built.help_runs,
                    "archive_verification": built.archive_verification,
                }
            )
        report = {
            "schema_version": "label-match-release-cli-tools-v1",
            "status": "PASS",
            "artifact_phase": "unsigned_pre_sign",
            "commit": _git_value(repo_root, "rev-parse", "HEAD"),
            "tree": _git_value(repo_root, "rev-parse", "HEAD^{tree}"),
            "app_version": _app_version(repo_root),
            "python_version": sys.version.split()[0],
            "pyinstaller_version": importlib.metadata.version("pyinstaller"),
            "probe_policy": {
                "probe_count": int(probe_count),
                "help_timeout_seconds": float(help_timeout_seconds),
                "fresh_copy_per_probe": True,
                "isolated_environment_per_probe": True,
                "residual_process_policy": "fail_closed_new_exact_executable_path_with_baseline",
            },
            "tools": tool_reports,
        }
        (stage / REPORT_NAME).write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        os.replace(stage, destination)
        return report
    except Exception:
        shutil.rmtree(stage, ignore_errors=True)
        raise


def build_release_cli_tools(
    destination: Path,
    *,
    help_timeout_seconds: float = 15.0,
    probe_count: int = 3,
    repo_root: Path = REPO_ROOT,
) -> dict[str, object]:
    if os.name != "nt":
        raise ReleaseCliBuildError("Label_Match release CLI tools must be built and verified on Windows")
    if help_timeout_seconds <= 0 or help_timeout_seconds > 60:
        raise ReleaseCliBuildError("help timeout must be greater than 0 and no more than 60 seconds")
    if probe_count < 3:
        raise ReleaseCliBuildError("at least three isolated help probes are required")
    repo_root = repo_root.resolve()
    _verify_clean_checkout(repo_root)
    built_tools: list[BuiltTool] = []
    with tempfile.TemporaryDirectory(prefix="label-match-release-cli-") as temp_dir:
        build_root = Path(temp_dir)
        smoke_root = build_root / "smoke"
        smoke_root.mkdir()
        for index, spec in enumerate(TOOL_SPECS, start=1):
            built = _build_candidate(spec, repo_root=repo_root, tool_root=build_root / f"{index}-{spec.name}")
            for run_no in range(1, probe_count + 1):
                built.help_runs.append(
                    _run_isolated_help(
                        built,
                        run_no=run_no,
                        timeout_seconds=help_timeout_seconds,
                        smoke_root=smoke_root,
                    )
                )
            built_tools.append(built)
        return _publish_atomically(
            built_tools,
            destination=destination,
            repo_root=repo_root,
            help_timeout_seconds=help_timeout_seconds,
            probe_count=probe_count,
        )


def _load_existing_tool(spec: ToolSpec, destination: Path, *, repo_root: Path) -> BuiltTool:
    if spec.mode == "onefile":
        payload_root = destination / spec.executable_name
        executable = payload_root
    else:
        payload_root = destination / spec.name
        executable = payload_root / spec.executable_name
    if not executable.is_file():
        raise ReleaseCliBuildError(f"published helper is missing: {executable}")
    _verify_pe(executable)
    archive_verification = _verify_carchive(spec, executable, repo_root=repo_root)
    return BuiltTool(
        spec=spec,
        payload_root=payload_root,
        executable=executable,
        executable_sha256=_sha256(executable),
        executable_size=executable.stat().st_size,
        payload_inventory=_payload_inventory(payload_root),
        help_runs=[],
        archive_verification=archive_verification,
    )


def _read_authenticode_signature(executable: Path) -> dict[str, str]:
    powershell = shutil.which("pwsh") or shutil.which("powershell")
    if not powershell:
        raise ReleaseCliBuildError("PowerShell is required for independent Authenticode verification")
    env = dict(os.environ)
    env["LABEL_MATCH_SIGNATURE_TARGET"] = str(executable.resolve())
    script = """
$ErrorActionPreference = 'Stop'
$signature = Get-AuthenticodeSignature -LiteralPath $env:LABEL_MATCH_SIGNATURE_TARGET
[ordered]@{
  status = $signature.Status.ToString()
  signer_thumbprint = if ($signature.SignerCertificate) { $signature.SignerCertificate.Thumbprint } else { '' }
  timestamp_thumbprint = if ($signature.TimeStamperCertificate) { $signature.TimeStamperCertificate.Thumbprint } else { '' }
} | ConvertTo-Json -Compress
"""
    completed = subprocess.run(
        [powershell, "-NoProfile", "-NonInteractive", "-Command", script],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
        timeout=30,
    )
    if completed.returncode != 0 or completed.stderr.strip():
        detail = completed.stderr.strip() or completed.stdout.strip() or f"exit {completed.returncode}"
        raise ReleaseCliBuildError(f"Authenticode verification failed for {executable}: {detail}")
    try:
        payload = json.loads(completed.stdout)
    except ValueError as exc:
        raise ReleaseCliBuildError(f"Authenticode verification returned invalid JSON for {executable}") from exc
    return {
        "status": str(payload.get("status") or ""),
        "signer_thumbprint": str(payload.get("signer_thumbprint") or "").upper(),
        "timestamp_thumbprint": str(payload.get("timestamp_thumbprint") or "").upper(),
    }


def _signed_executable_paths(destination: Path, built_tools: Sequence[BuiltTool]) -> dict[str, Path]:
    paths = {"Label_Match.exe": destination.parent / "Label_Match.exe"}
    for executable_name in LOGISTICS_PROFILE_EXECUTABLE_NAMES:
        paths[executable_name] = destination.parent / executable_name
    for built in built_tools:
        if built.spec.mode == "onefile":
            relative = f"tools/{built.spec.executable_name}"
        else:
            relative = f"tools/{built.spec.name}/{built.spec.executable_name}"
        if relative in paths:
            raise ReleaseCliBuildError(f"duplicate signed executable path: {relative}")
        paths[relative] = built.executable
    return paths


def _verify_authenticode_manifest(
    destination: Path,
    built_tools: Sequence[BuiltTool],
    *,
    expected_signer_thumbprint: str,
) -> tuple[Path, str, list[dict[str, object]]]:
    manifest_path = destination.parent / "authenticode-manifest.json"
    if not manifest_path.is_file():
        raise ReleaseCliBuildError(f"Authenticode manifest is missing: {manifest_path}")
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError) as exc:
        raise ReleaseCliBuildError(f"Authenticode manifest is invalid: {exc}") from exc
    if payload.get("status") != "PASS":
        raise ReleaseCliBuildError("Authenticode manifest status is not PASS")
    entries = payload.get("executables")
    if not isinstance(entries, list):
        raise ReleaseCliBuildError("Authenticode manifest executable list is missing")
    by_path: dict[str, dict[str, object]] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            raise ReleaseCliBuildError("Authenticode manifest contains a non-object executable entry")
        relative = str(entry.get("path") or "").replace("\\", "/")
        if not relative or relative in by_path:
            raise ReleaseCliBuildError(f"Authenticode manifest contains a missing or duplicate path: {relative!r}")
        by_path[relative] = entry

    expected = _signed_executable_paths(destination, built_tools)
    if set(by_path) != set(expected) or len(entries) != len(expected):
        raise ReleaseCliBuildError(
            f"Authenticode manifest paths differ: expected {sorted(expected)}, got {sorted(by_path)}"
        )
    expected_signer = str(expected_signer_thumbprint or "").replace(" ", "").upper()
    if not re.fullmatch(r"[0-9A-F]{40}", expected_signer):
        raise ReleaseCliBuildError("approved Authenticode signer thumbprint is invalid")
    manifest_signer = str(payload.get("signer_thumbprint") or "").replace(" ", "").upper()
    if manifest_signer != expected_signer:
        raise ReleaseCliBuildError("Authenticode manifest signer differs from the approved thumbprint")

    verification: list[dict[str, object]] = []
    for relative, executable in expected.items():
        if not executable.is_file():
            raise ReleaseCliBuildError(f"signed executable is missing: {relative}")
        entry = by_path[relative]
        if entry.get("status") != "Valid":
            raise ReleaseCliBuildError(f"valid Authenticode entry is missing for {relative}")
        actual_hash = _sha256(executable)
        if str(entry.get("sha256") or "").lower() != actual_hash:
            raise ReleaseCliBuildError(f"post-sign hash mismatch for {relative}")
        if int(entry.get("size") or -1) != executable.stat().st_size:
            raise ReleaseCliBuildError(f"post-sign size mismatch for {relative}")
        entry_signer = str(entry.get("signer_thumbprint") or "").replace(" ", "").upper()
        entry_timestamp = str(entry.get("timestamp_thumbprint") or "").replace(" ", "").upper()
        if entry_signer != expected_signer:
            raise ReleaseCliBuildError(f"manifest signer thumbprint mismatch for {relative}")
        if not re.fullmatch(r"[0-9A-F]{40}", entry_timestamp):
            raise ReleaseCliBuildError(f"manifest timestamp thumbprint is invalid for {relative}")
        live_signature = _read_authenticode_signature(executable)
        if live_signature["status"] != "Valid":
            raise ReleaseCliBuildError(f"live Authenticode status is not Valid for {relative}")
        if live_signature["signer_thumbprint"] != expected_signer:
            raise ReleaseCliBuildError(f"live signer thumbprint mismatch for {relative}")
        if live_signature["timestamp_thumbprint"] != entry_timestamp:
            raise ReleaseCliBuildError(f"live timestamp thumbprint mismatch for {relative}")
        verification.append(
            {
                "path": relative,
                "size": executable.stat().st_size,
                "sha256": actual_hash,
                "status": live_signature["status"],
                "signer_thumbprint": live_signature["signer_thumbprint"],
                "timestamp_thumbprint": live_signature["timestamp_thumbprint"],
            }
        )
    return manifest_path, _sha256(manifest_path), verification


def _verify_pre_sign_manifest(destination: Path, *, repo_root: Path) -> tuple[Path, str]:
    path = destination / REPORT_NAME
    if not path.is_file():
        raise ReleaseCliBuildError(f"pre-sign helper manifest is missing: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError) as exc:
        raise ReleaseCliBuildError(f"pre-sign helper manifest is invalid: {exc}") from exc
    expected_identity = {
        "status": "PASS",
        "artifact_phase": "unsigned_pre_sign",
        "commit": _git_value(repo_root, "rev-parse", "HEAD"),
        "tree": _git_value(repo_root, "rev-parse", "HEAD^{tree}"),
        "app_version": _app_version(repo_root),
    }
    for name, expected_value in expected_identity.items():
        if payload.get(name) != expected_value:
            raise ReleaseCliBuildError(f"pre-sign helper manifest {name} mismatch")
    tools = payload.get("tools")
    if not isinstance(tools, list):
        raise ReleaseCliBuildError("pre-sign helper manifest tool list is missing")
    actual_specs = [(item.get("name"), item.get("mode")) for item in tools if isinstance(item, dict)]
    expected_specs = [(spec.name, spec.mode) for spec in TOOL_SPECS]
    if actual_specs != expected_specs:
        raise ReleaseCliBuildError("pre-sign helper manifest tool order or mode mismatch")
    return path, _sha256(path)


def verify_existing_release_cli_tools(
    destination: Path,
    *,
    help_timeout_seconds: float = 15.0,
    probe_count: int = 3,
    repo_root: Path = REPO_ROOT,
    expected_signer_thumbprint: str = "",
) -> dict[str, object]:
    if os.name != "nt":
        raise ReleaseCliBuildError("Label_Match release CLI tools must be verified on Windows")
    if help_timeout_seconds <= 0 or help_timeout_seconds > 60:
        raise ReleaseCliBuildError("help timeout must be greater than 0 and no more than 60 seconds")
    if probe_count < 3:
        raise ReleaseCliBuildError("at least three isolated help probes are required")
    destination = destination.resolve()
    repo_root = repo_root.resolve()
    _verify_clean_checkout(repo_root)
    if not destination.is_dir():
        raise ReleaseCliBuildError(f"published helper directory is missing: {destination}")
    with tempfile.TemporaryDirectory(prefix="label-match-signed-cli-") as temp_dir:
        smoke_root = Path(temp_dir) / "smoke"
        smoke_root.mkdir()
        built_tools = [
            _load_existing_tool(spec, destination, repo_root=repo_root)
            for spec in TOOL_SPECS
        ]
        pre_sign_path, pre_sign_sha256 = _verify_pre_sign_manifest(destination, repo_root=repo_root)
        authenticode_path, authenticode_sha256, authenticode_verification = _verify_authenticode_manifest(
            destination,
            built_tools,
            expected_signer_thumbprint=expected_signer_thumbprint,
        )
        for built in built_tools:
            for run_no in range(1, probe_count + 1):
                built.help_runs.append(
                    _run_isolated_help(
                        built,
                        run_no=run_no,
                        timeout_seconds=help_timeout_seconds,
                        smoke_root=smoke_root,
                    )
                )
        report = {
            "schema_version": "label-match-release-cli-tools-v1",
            "status": "PASS",
            "artifact_phase": "signed_post_sign",
            "commit": _git_value(repo_root, "rev-parse", "HEAD"),
            "tree": _git_value(repo_root, "rev-parse", "HEAD^{tree}"),
            "app_version": _app_version(repo_root),
            "python_version": sys.version.split()[0],
            "pyinstaller_version": importlib.metadata.version("pyinstaller"),
            "authenticode_manifest": authenticode_path.name,
            "authenticode_manifest_sha256": authenticode_sha256,
            "authenticode_verification": authenticode_verification,
            "pre_sign_manifest": pre_sign_path.name,
            "pre_sign_manifest_sha256": pre_sign_sha256,
            "probe_policy": {
                "probe_count": int(probe_count),
                "help_timeout_seconds": float(help_timeout_seconds),
                "fresh_copy_per_probe": True,
                "isolated_environment_per_probe": True,
                "residual_process_policy": "fail_closed_new_exact_executable_path_with_baseline",
            },
            "tools": [
                {
                    "name": built.spec.name,
                    "source": built.spec.source_rel,
                    "mode": built.spec.mode,
                    "executable_sha256": built.executable_sha256,
                    "executable_size": built.executable_size,
                    "payload_inventory": built.payload_inventory,
                    "help_runs": built.help_runs,
                    "archive_verification": built.archive_verification,
                }
                for built in built_tools
            ],
        }
        report_path = destination / POST_SIGN_REPORT_NAME
        temp_report = destination / f".{POST_SIGN_REPORT_NAME}.{uuid.uuid4().hex}.tmp"
        temp_report.write_text(
            json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        os.replace(temp_report, report_path)
        return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build and verify Label_Match release CLI tools")
    parser.add_argument("--destination", required=True)
    parser.add_argument("--help-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--probe-count", type=int, default=3)
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--verify-existing", action="store_true")
    parser.add_argument(
        "--expected-signer-thumbprint",
        default=os.getenv("WINDOWS_CODE_SIGNING_CERT_THUMBPRINT", ""),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.verify_existing:
            report = verify_existing_release_cli_tools(
                Path(args.destination),
                help_timeout_seconds=args.help_timeout_seconds,
                probe_count=args.probe_count,
                repo_root=Path(args.repo_root),
                expected_signer_thumbprint=args.expected_signer_thumbprint,
            )
        else:
            report = build_release_cli_tools(
                Path(args.destination),
                help_timeout_seconds=args.help_timeout_seconds,
                probe_count=args.probe_count,
                repo_root=Path(args.repo_root),
            )
    except (OSError, ReleaseCliBuildError, subprocess.SubprocessError) as exc:
        print(f"release_cli_tools=DENY reason={exc}")
        return 2
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

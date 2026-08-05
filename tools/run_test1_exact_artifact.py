"""Launch a TEST1 package only after exact archive and executable attestation."""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
import re
import subprocess
import time
import zipfile
from ctypes import wintypes
from datetime import datetime
from pathlib import Path
from typing import Any, Callable


SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
REPORT_VERSION = "kmtech-test1-exact-artifact-v1"


class ArtifactIdentityError(RuntimeError):
    """Raised when the approved package identity cannot be proven exactly."""


def _timestamp() -> str:
    return datetime.now().astimezone().isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _expected_sha256(value: str, label: str) -> str:
    value = value.strip()
    if not SHA256_RE.fullmatch(value):
        raise ArtifactIdentityError(f"{label} must be exactly 64 hexadecimal characters")
    return value.lower()


def _archive_member_sha256(archive_path: Path, member: str) -> str:
    if (
        not member
        or "\\" in member
        or member.startswith("/")
        or any(part in {"", ".", ".."} for part in member.split("/"))
    ):
        raise ArtifactIdentityError("archive member must be one safe POSIX relative path")
    try:
        with zipfile.ZipFile(archive_path, "r") as archive:
            matches = [item for item in archive.infolist() if item.filename == member]
            if len(matches) != 1 or matches[0].is_dir():
                raise ArtifactIdentityError(
                    f"archive must contain exactly one file named {member!r}"
                )
            digest = hashlib.sha256()
            with archive.open(matches[0], "r") as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(chunk)
            return digest.hexdigest()
    except (OSError, zipfile.BadZipFile) as exc:
        raise ArtifactIdentityError(f"cannot inspect release archive: {exc}") from exc


def preflight_artifact_identity(
    *,
    archive_path: Path,
    expected_archive_sha256: str,
    executable_path: Path,
    expected_executable_sha256: str,
    archive_member: str,
) -> dict[str, Any]:
    """Verify all immutable identities before any process is created."""

    archive_path = archive_path.resolve(strict=True)
    executable_path = executable_path.resolve(strict=True)
    if not archive_path.is_file() or not executable_path.is_file():
        raise ArtifactIdentityError("archive and installed executable must both be files")

    expected_archive = _expected_sha256(
        expected_archive_sha256, "expected archive SHA-256"
    )
    expected_executable = _expected_sha256(
        expected_executable_sha256, "expected executable SHA-256"
    )
    actual_archive = sha256_file(archive_path)
    actual_executable = sha256_file(executable_path)
    archived_executable = _archive_member_sha256(archive_path, archive_member)

    if actual_archive != expected_archive:
        raise ArtifactIdentityError("release archive SHA-256 mismatch")
    if actual_executable != expected_executable:
        raise ArtifactIdentityError("installed executable SHA-256 mismatch")
    if archived_executable != actual_executable:
        raise ArtifactIdentityError(
            "installed executable does not match the executable inside the approved archive"
        )

    return {
        "report_version": REPORT_VERSION,
        "status": "PRELAUNCH_PASS",
        "started_at": _timestamp(),
        "archive": {
            "path": str(archive_path),
            "expected_sha256": expected_archive,
            "sha256": actual_archive,
        },
        "archive_member": {
            "path": archive_member,
            "sha256": archived_executable,
            "matches_installed_executable": True,
        },
        "installed_executable": {
            "path": str(executable_path),
            "expected_sha256": expected_executable,
            "sha256": actual_executable,
        },
        "process": {
            "pid": None,
            "executable_path": None,
            "matches_installed_executable": False,
        },
    }


def query_process_executable_path(pid: int) -> Path:
    """Return the OS-reported executable image path for a Windows process."""

    if os.name != "nt":
        raise ArtifactIdentityError(
            "OS process-image attestation is supported only on physical Windows TEST1"
        )
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    open_process = kernel32.OpenProcess
    open_process.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    open_process.restype = wintypes.HANDLE
    query_image = kernel32.QueryFullProcessImageNameW
    query_image.argtypes = [
        wintypes.HANDLE,
        wintypes.DWORD,
        wintypes.LPWSTR,
        ctypes.POINTER(wintypes.DWORD),
    ]
    query_image.restype = wintypes.BOOL
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [wintypes.HANDLE]
    close_handle.restype = wintypes.BOOL

    handle = open_process(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
    if not handle:
        raise ArtifactIdentityError(
            f"cannot open launched process {pid}: winerror={ctypes.get_last_error()}"
        )
    try:
        buffer = ctypes.create_unicode_buffer(32768)
        size = wintypes.DWORD(len(buffer))
        if not query_image(handle, 0, buffer, ctypes.byref(size)):
            raise ArtifactIdentityError(
                f"cannot query launched process {pid}: winerror={ctypes.get_last_error()}"
            )
        return Path(buffer.value).resolve(strict=True)
    finally:
        close_handle(handle)


def _same_path(first: Path, second: Path) -> bool:
    return os.path.normcase(str(first.resolve(strict=True))) == os.path.normcase(
        str(second.resolve(strict=True))
    )


def attest_process_identity(
    identity: dict[str, Any],
    *,
    pid: int,
    query_process_path: Callable[[int], Path] = query_process_executable_path,
    timeout_seconds: float = 5.0,
    process_poll: Callable[[], int | None] | None = None,
) -> None:
    """Bind the launched PID to the installed executable before UI acceptance."""

    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    actual_path: Path | None = None
    while time.monotonic() <= deadline:
        try:
            actual_path = query_process_path(pid).resolve(strict=True)
            break
        except (OSError, ArtifactIdentityError) as exc:
            last_error = exc
            if process_poll is not None and process_poll() is not None:
                break
            time.sleep(0.05)
    if actual_path is None:
        raise ArtifactIdentityError(
            f"cannot attest launched process image path: {last_error or 'timeout'}"
        )

    expected_path = Path(identity["installed_executable"]["path"])
    matches = _same_path(actual_path, expected_path)
    identity["process"] = {
        "pid": pid,
        "executable_path": str(actual_path),
        "matches_installed_executable": matches,
    }
    if not matches:
        raise ArtifactIdentityError(
            "OS-reported process executable path does not match the attested executable"
        )
    identity["status"] = "PROCESS_IDENTITY_PASS"


def finalize_artifact_identity(identity: dict[str, Any]) -> None:
    """Fail if the archive or executable changed while the package was running."""

    archive = identity["archive"]
    executable = identity["installed_executable"]
    archive["postrun_sha256"] = sha256_file(Path(archive["path"]))
    executable["postrun_sha256"] = sha256_file(Path(executable["path"]))
    unchanged = (
        archive["postrun_sha256"] == archive["sha256"]
        and executable["postrun_sha256"] == executable["sha256"]
    )
    identity["postrun_hashes_unchanged"] = unchanged
    if not unchanged:
        raise ArtifactIdentityError("archive or executable changed during TEST1 execution")


def write_identity_evidence(path: Path, identity: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(identity, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _stop_untrusted_process(process: Any) -> None:
    try:
        process.terminate()
        process.wait(timeout=5)
    except Exception:
        try:
            process.kill()
        except Exception:
            pass


def launch_exact_artifact(
    *,
    archive_path: Path,
    expected_archive_sha256: str,
    executable_path: Path,
    expected_executable_sha256: str,
    archive_member: str,
    evidence_json: Path,
    application_args: list[str] | None = None,
    popen_factory: Callable[..., Any] = subprocess.Popen,
    query_process_path: Callable[[int], Path] = query_process_executable_path,
    process_identity_timeout: float = 5.0,
) -> dict[str, Any]:
    """Launch, attest, wait, and write the durable exact-artifact identity JSON."""

    if evidence_json.exists():
        raise ArtifactIdentityError("refusing to overwrite existing TEST1 evidence JSON")
    identity: dict[str, Any] = {
        "report_version": REPORT_VERSION,
        "status": "BLOCKED",
        "started_at": _timestamp(),
    }
    process: Any | None = None
    try:
        identity = preflight_artifact_identity(
            archive_path=archive_path,
            expected_archive_sha256=expected_archive_sha256,
            executable_path=executable_path,
            expected_executable_sha256=expected_executable_sha256,
            archive_member=archive_member,
        )
        executable_path = Path(identity["installed_executable"]["path"])
        process = popen_factory(
            [str(executable_path), *(application_args or [])],
            cwd=str(executable_path.parent),
        )
        try:
            attest_process_identity(
                identity,
                pid=process.pid,
                query_process_path=query_process_path,
                timeout_seconds=process_identity_timeout,
                process_poll=process.poll,
            )
        except Exception:
            _stop_untrusted_process(process)
            raise
        identity["status"] = "RUNNING_VERIFIED"
        write_identity_evidence(evidence_json, identity)
        exit_code = process.wait()
        identity["process"]["exit_code"] = exit_code
        finalize_artifact_identity(identity)
        identity["status"] = "PASS" if exit_code == 0 else "FAIL"
        identity["finished_at"] = _timestamp()
        write_identity_evidence(evidence_json, identity)
        return identity
    except Exception as exc:
        if process is not None and process.poll() is None:
            _stop_untrusted_process(process)
        identity["status"] = "BLOCKED"
        identity["error"] = f"{exc.__class__.__name__}: {exc}"
        identity["finished_at"] = _timestamp()
        write_identity_evidence(evidence_json, identity)
        if isinstance(exc, ArtifactIdentityError):
            raise
        raise ArtifactIdentityError(str(exc)) from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Launch an exact TEST1 release artifact with fail-closed identity evidence."
    )
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--expected-archive-sha256", required=True)
    parser.add_argument("--exe", required=True, type=Path)
    parser.add_argument("--expected-exe-sha256", required=True)
    parser.add_argument("--archive-member", required=True)
    parser.add_argument("--evidence-json", required=True, type=Path)
    parser.add_argument("--app-arg", action="append", default=[])
    parser.add_argument("--process-identity-timeout", type=float, default=5.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        identity = launch_exact_artifact(
            archive_path=args.archive,
            expected_archive_sha256=args.expected_archive_sha256,
            executable_path=args.exe,
            expected_executable_sha256=args.expected_exe_sha256,
            archive_member=args.archive_member,
            evidence_json=args.evidence_json,
            application_args=args.app_arg,
            process_identity_timeout=args.process_identity_timeout,
        )
    except ArtifactIdentityError as exc:
        print(f"TEST1 BLOCKED: {exc}", file=os.sys.stderr)
        return 2
    return 0 if identity["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())

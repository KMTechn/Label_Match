#!/usr/bin/env python
"""Create, attest, and mirror the one canonical local release tag exactly once."""

from __future__ import annotations

import argparse
import ctypes
from ctypes import wintypes
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import threading
import time
from typing import Sequence
from urllib.parse import unquote, urlparse
from urllib.request import url2pathname


ACTIVE_RELEASE_TAG = "v2.0.85"
SCHEMA_VERSION = "label-match-local-tag-burn-v1"
OID_RE = re.compile(r"^[0-9a-f]{40}$")
MAX_CAPTURE_BYTES = 64 * 1024
MAX_CAPTURE_AGGREGATE_BYTES = 96 * 1024
CAPTURE_READ_CHUNK_BYTES = 4096
MAX_RECEIPT_BYTES = 64 * 1024


class TagBurnError(RuntimeError):
    """Fail closed without exposing raw command output."""

    def __init__(
        self,
        stage: str,
        code: str,
        *,
        state_query_safe: bool = True,
    ):
        super().__init__(f"{stage}:{code}")
        self.stage = stage
        self.code = code
        self.state_query_safe = state_query_safe


@dataclass(frozen=True)
class BurnConfig:
    repo_root: Path
    mirror_root: Path
    evidence_root: Path
    tag: str
    expected_commit: str
    expected_tree: str


@dataclass(frozen=True)
class GitAuthority:
    path: Path
    sha256: str
    taskkill_path: Path
    taskkill_sha256: str


@dataclass(frozen=True)
class CaptureResult:
    returncode: int
    stdout: bytes


@dataclass(frozen=True)
class MutationResult:
    returncode: int | None
    abort_reason: str | None
    stdout: dict[str, object]
    stderr: dict[str, object]
    tree_cleanup: dict[str, object]


class _StreamCapture:
    def __init__(self) -> None:
        self.total_bytes = 0
        self.digest = hashlib.sha256()
        self.retained = bytearray()

    def add(self, chunk: bytes) -> None:
        self.total_bytes += len(chunk)
        self.digest.update(chunk)
        remaining = max(0, MAX_CAPTURE_BYTES - len(self.retained))
        self.retained.extend(chunk[:remaining])

    def raw(self) -> bytes:
        return bytes(self.retained)

    def evidence(self) -> dict[str, object]:
        return {
            "bytes": self.total_bytes,
            "sha256": self.digest.hexdigest(),
            "content_preserved": False,
        }


@dataclass(frozen=True)
class _ProcessResult:
    returncode: int | None
    abort_reason: str | None
    stdout: bytes
    stdout_evidence: dict[str, object]
    stderr_evidence: dict[str, object]
    tree_cleanup: dict[str, object]


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


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_bytes(payload: dict[str, object]) -> bytes:
    encoded = (
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    ).encode("ascii")
    if len(encoded) > MAX_RECEIPT_BYTES:
        raise TagBurnError("evidence", "receipt_size_limit")
    return encoded


def _write_bytes_create_new(path: Path, content: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())


def _write_json_create_new(path: Path, payload: dict[str, object]) -> None:
    _write_bytes_create_new(path, _json_bytes(payload))


def _windows_system_executable(name: str) -> Path:
    if os.name != "nt":
        raise TagBurnError("authority", "windows_required")
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    get_system_directory = kernel32.GetSystemDirectoryW
    get_system_directory.argtypes = [wintypes.LPWSTR, wintypes.UINT]
    get_system_directory.restype = wintypes.UINT
    buffer = ctypes.create_unicode_buffer(32768)
    ctypes.set_last_error(0)
    length = int(get_system_directory(buffer, len(buffer)))
    if length <= 0 or length >= len(buffer):
        raise TagBurnError("authority", "system_directory_unavailable")
    path = (Path(buffer.value) / name).resolve(strict=True)
    if not path.is_absolute() or not path.is_file():
        raise TagBurnError("authority", "system_executable_invalid")
    return path


def _resolve_git_authority() -> GitAuthority:
    discovered = shutil.which("git")
    if not discovered:
        raise TagBurnError("authority", "git_not_found")
    path = Path(discovered).resolve(strict=True)
    if not path.is_absolute() or not path.is_file():
        raise TagBurnError("authority", "git_not_absolute_file")
    taskkill_path = _windows_system_executable("taskkill.exe")
    return GitAuthority(
        path=path,
        sha256=_sha256(path),
        taskkill_path=taskkill_path,
        taskkill_sha256=_sha256(taskkill_path),
    )


def _assert_git_unchanged(git: GitAuthority) -> None:
    if not git.path.is_file() or _sha256(git.path) != git.sha256:
        raise TagBurnError("authority", "git_bytes_changed")
    if (
        not git.taskkill_path.is_file()
        or _sha256(git.taskkill_path) != git.taskkill_sha256
    ):
        raise TagBurnError("authority", "taskkill_bytes_changed")


def _process_parent_map() -> dict[int, int]:
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
    if snapshot == ctypes.c_void_p(-1).value:
        raise OSError(ctypes.get_last_error(), "CreateToolhelp32Snapshot")
    parents: dict[int, int] = {}
    try:
        entry = _ProcessEntry32W()
        entry.dwSize = ctypes.sizeof(entry)
        ctypes.set_last_error(0)
        has_entry = bool(process_first(snapshot, ctypes.byref(entry)))
        if not has_entry and ctypes.get_last_error() not in (0, 18):
            raise OSError(ctypes.get_last_error(), "Process32FirstW")
        while has_entry:
            parents[int(entry.th32ProcessID)] = int(entry.th32ParentProcessID)
            ctypes.set_last_error(0)
            has_entry = bool(process_next(snapshot, ctypes.byref(entry)))
            if not has_entry and ctypes.get_last_error() not in (0, 18):
                raise OSError(ctypes.get_last_error(), "Process32NextW")
    finally:
        close_handle(snapshot)
    return parents


def _process_tree_pids(root_pid: int, parents: dict[int, int]) -> set[int]:
    tree = {int(root_pid)}
    while True:
        added = {pid for pid, parent in parents.items() if parent in tree} - tree
        if not added:
            return tree
        tree.update(added)


def _terminate_and_prove_process_tree(
    git: GitAuthority,
    root_pid: int,
) -> dict[str, object]:
    _assert_git_unchanged(git)
    try:
        parents_before = _process_parent_map()
    except OSError as exc:
        raise TagBurnError(
            "cleanup",
            "process_snapshot_failed",
            state_query_safe=False,
        ) from exc
    if root_pid not in parents_before:
        raise TagBurnError(
            "cleanup",
            "process_root_disappeared_before_tree_kill",
            state_query_safe=False,
        )
    observed = _process_tree_pids(root_pid, parents_before)
    try:
        completed = subprocess.run(
            [
                str(git.taskkill_path),
                "/PID",
                str(int(root_pid)),
                "/T",
                "/F",
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
            timeout=15,
            shell=False,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise TagBurnError(
            "cleanup",
            f"taskkill_{type(exc).__name__}",
            state_query_safe=False,
        ) from exc
    deadline = time.monotonic() + 10
    remaining = set(observed)
    while remaining and time.monotonic() < deadline:
        try:
            current = _process_parent_map()
        except OSError as exc:
            raise TagBurnError(
                "cleanup",
                "process_recheck_failed",
                state_query_safe=False,
            ) from exc
        remaining.intersection_update(current)
        if remaining:
            time.sleep(0.05)
    if remaining:
        raise TagBurnError(
            "cleanup",
            (
                "taskkill_nonzero_with_process_tree_residual"
                if completed.returncode != 0
                else "process_tree_residual"
            ),
            state_query_safe=False,
        )
    return {
        "status": "PASS",
        "method": "taskkill_pid_tree_force",
        "taskkill_returncode": completed.returncode,
        "root_pid": int(root_pid),
        "observed_process_count": len(observed),
        "remaining_process_count": 0,
    }


def _run_git_bounded(
    git: GitAuthority,
    repo: Path,
    args: Sequence[str],
    *,
    stage: str,
    timeout_seconds: float,
) -> _ProcessResult:
    _assert_git_unchanged(git)
    try:
        process = subprocess.Popen(
            [str(git.path), *map(str, args)],
            cwd=repo,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
            shell=False,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except OSError as exc:
        raise TagBurnError(stage, f"git_{type(exc).__name__}") from exc
    if process.stdout is None or process.stderr is None:
        _terminate_and_prove_process_tree(git, process.pid)
        process.wait(timeout=5)
        raise TagBurnError(stage, "git_pipes_unavailable")

    stdout_capture = _StreamCapture()
    stderr_capture = _StreamCapture()
    aggregate_bytes = 0
    capture_limit_exceeded = False
    read_error_types: list[str] = []
    state_lock = threading.Lock()
    state_changed = threading.Event()

    def drain(pipe, capture: _StreamCapture) -> None:
        nonlocal aggregate_bytes, capture_limit_exceeded
        try:
            while True:
                chunk = pipe.read(CAPTURE_READ_CHUNK_BYTES)
                if not chunk:
                    break
                capture.add(chunk)
                with state_lock:
                    aggregate_bytes += len(chunk)
                    if (
                        capture.total_bytes > MAX_CAPTURE_BYTES
                        or aggregate_bytes > MAX_CAPTURE_AGGREGATE_BYTES
                    ):
                        capture_limit_exceeded = True
                        state_changed.set()
        except Exception as exc:  # pragma: no cover - OS pipe errors are rare
            with state_lock:
                read_error_types.append(type(exc).__name__)
                state_changed.set()
        finally:
            try:
                pipe.close()
            except Exception:
                pass

    readers = (
        threading.Thread(
            target=drain,
            args=(process.stdout, stdout_capture),
            name="release-tag-git-stdout",
            daemon=True,
        ),
        threading.Thread(
            target=drain,
            args=(process.stderr, stderr_capture),
            name="release-tag-git-stderr",
            daemon=True,
        ),
    )
    for reader in readers:
        reader.start()

    abort_reason: str | None = None
    tree_cleanup: dict[str, object] = {
        "status": "NOT_APPLICABLE",
        "method": "taskkill_pid_tree_force",
    }

    def terminate_tree() -> None:
        nonlocal tree_cleanup
        if tree_cleanup["status"] == "PASS":
            return
        tree_cleanup = _terminate_and_prove_process_tree(git, process.pid)

    deadline = time.monotonic() + timeout_seconds
    while process.poll() is None:
        with state_lock:
            if capture_limit_exceeded:
                abort_reason = "capture_limit_exceeded"
            elif read_error_types:
                abort_reason = "stream_read_error"
        if abort_reason is not None:
            terminate_tree()
            break
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            abort_reason = "timeout"
            terminate_tree()
            break
        state_changed.wait(min(0.01, remaining))
        state_changed.clear()

    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        if abort_reason is None:
            abort_reason = "process_wait_timeout"
        terminate_tree()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired as exc:
            raise TagBurnError(
                stage,
                "git_process_cleanup_failed",
                state_query_safe=False,
            ) from exc

    for reader in readers:
        reader.join(5)
    alive_readers = [reader for reader in readers if reader.is_alive()]
    if alive_readers:
        try:
            process.stdout.close()
            process.stderr.close()
        except Exception:
            pass
        for reader in alive_readers:
            reader.join(1)
    if any(reader.is_alive() for reader in readers):
        raise TagBurnError(
            stage,
            "git_stream_cleanup_failed",
            state_query_safe=False,
        )
    with state_lock:
        if capture_limit_exceeded and abort_reason is None:
            abort_reason = "capture_limit_exceeded"
        elif read_error_types and abort_reason is None:
            abort_reason = "stream_read_error"
    if abort_reason is not None and tree_cleanup["status"] != "PASS":
        if process.poll() is None:
            terminate_tree()
        else:
            tree_cleanup = {
                "status": "UNPROVEN",
                "method": "taskkill_pid_tree_force",
                "reason": "root_disappeared_before_post_stream_abort_cleanup",
            }

    return _ProcessResult(
        returncode=process.returncode,
        abort_reason=abort_reason,
        stdout=stdout_capture.raw(),
        stdout_evidence=stdout_capture.evidence(),
        stderr_evidence=stderr_capture.evidence(),
        tree_cleanup=tree_cleanup,
    )


def _capture_git(
    git: GitAuthority,
    repo: Path,
    args: Sequence[str],
    *,
    stage: str,
    allowed_returncodes: Sequence[int] = (0,),
) -> CaptureResult:
    completed = _run_git_bounded(
        git, repo, args, stage=stage, timeout_seconds=30
    )
    if completed.abort_reason is not None:
        raise TagBurnError(
            stage,
            f"git_{completed.abort_reason}",
            state_query_safe=completed.tree_cleanup["status"] == "PASS",
        )
    if completed.returncode not in allowed_returncodes:
        raise TagBurnError(stage, "git_nonzero")
    return CaptureResult(completed.returncode, completed.stdout)


def _git_text(
    git: GitAuthority,
    repo: Path,
    *args: str,
    stage: str = "preflight",
) -> str:
    raw = _capture_git(git, repo, args, stage=stage).stdout
    try:
        return raw.decode("utf-8").strip()
    except UnicodeDecodeError as exc:
        raise TagBurnError(stage, "git_output_not_utf8") from exc


def _tag_oid_or_none(
    git: GitAuthority,
    repo: Path,
    tag_ref: str,
    *,
    stage: str,
) -> str | None:
    result = _capture_git(
        git,
        repo,
        ("rev-parse", "--verify", "--quiet", tag_ref),
        stage=stage,
        allowed_returncodes=(0, 1),
    )
    if result.returncode == 1:
        return None
    oid = result.stdout.decode("ascii", errors="strict").strip().lower()
    if OID_RE.fullmatch(oid) is None:
        raise TagBurnError(stage, "tag_oid_invalid")
    return oid


def _refs(
    git: GitAuthority,
    repo: Path,
    *,
    stage: str,
) -> dict[str, str]:
    text = _git_text(
        git,
        repo,
        "for-each-ref",
        "--format=%(refname) %(objectname)",
        stage=stage,
    )
    result: dict[str, str] = {}
    for line in text.splitlines() if text else ():
        ref, separator, oid = line.partition(" ")
        if not separator or ref in result or OID_RE.fullmatch(oid) is None:
            raise TagBurnError(stage, "ref_inventory_invalid")
        result[ref] = oid
    return result


def _normalize_local_remote(value: str) -> Path:
    if value.lower().startswith("file://"):
        parsed = urlparse(value)
        if parsed.scheme.lower() != "file" or parsed.netloc not in ("", "localhost"):
            raise TagBurnError("preflight", "origin_not_local")
        raw_path = url2pathname(unquote(parsed.path))
        if os.name == "nt" and re.match(r"^/[A-Za-z]:/", raw_path):
            raw_path = raw_path[1:]
        path = Path(raw_path)
    else:
        if "://" in value:
            raise TagBurnError("preflight", "origin_not_local")
        path = Path(value)
    if not path.is_absolute():
        raise TagBurnError("preflight", "origin_not_absolute")
    return path.resolve(strict=True)


def _same_path(left: Path, right: Path) -> bool:
    return os.path.normcase(os.path.realpath(left)) == os.path.normcase(
        os.path.realpath(right)
    )


def _is_within(path: Path, parent: Path) -> bool:
    return path == parent or parent in path.parents


def _validate_paths(config: BurnConfig) -> BurnConfig:
    for value, label in (
        (config.repo_root, "repo"),
        (config.mirror_root, "mirror"),
        (config.evidence_root, "evidence"),
    ):
        if not value.is_absolute():
            raise TagBurnError("arguments", f"{label}_not_absolute")
    repo = config.repo_root.resolve(strict=True)
    mirror = config.mirror_root.resolve(strict=True)
    evidence = config.evidence_root.resolve(strict=False)
    if not repo.is_dir() or not mirror.is_dir():
        raise TagBurnError("arguments", "repository_path_invalid")
    if (
        _same_path(repo, mirror)
        or _is_within(repo, mirror)
        or _is_within(mirror, repo)
    ):
        raise TagBurnError("arguments", "repositories_not_isolated")
    if not evidence.parent.is_dir() or evidence.exists():
        raise TagBurnError("arguments", "evidence_root_not_fresh")
    if _is_within(evidence, repo) or _is_within(evidence, mirror):
        raise TagBurnError("arguments", "evidence_root_not_isolated")
    return BurnConfig(
        repo_root=repo,
        mirror_root=mirror,
        evidence_root=evidence,
        tag=config.tag,
        expected_commit=config.expected_commit,
        expected_tree=config.expected_tree,
    )


def _preflight(
    config: BurnConfig,
    git: GitAuthority,
) -> dict[str, object]:
    if _git_text(git, config.repo_root, "rev-parse", "--is-bare-repository") != "false":
        raise TagBurnError("preflight", "work_repo_not_nonbare")
    if _git_text(git, config.mirror_root, "rev-parse", "--is-bare-repository") != "true":
        raise TagBurnError("preflight", "mirror_not_bare")
    if _git_text(git, config.repo_root, "branch", "--show-current") != "main":
        raise TagBurnError("preflight", "branch_not_main")
    if (
        _git_text(git, config.repo_root, "rev-parse", "--abbrev-ref", "@{upstream}")
        != "origin/main"
    ):
        raise TagBurnError("preflight", "upstream_not_origin_main")

    refs = {
        "head": _git_text(git, config.repo_root, "rev-parse", "HEAD").lower(),
        "local_main": _git_text(
            git, config.repo_root, "rev-parse", "refs/heads/main"
        ).lower(),
        "origin_main": _git_text(
            git, config.repo_root, "rev-parse", "refs/remotes/origin/main"
        ).lower(),
        "mirror_main": _git_text(
            git, config.mirror_root, "rev-parse", "refs/heads/main"
        ).lower(),
    }
    if any(value != config.expected_commit for value in refs.values()):
        raise TagBurnError("preflight", "main_ref_identity_mismatch")
    tree = _git_text(git, config.repo_root, "rev-parse", "HEAD^{tree}").lower()
    if tree != config.expected_tree:
        raise TagBurnError("preflight", "tree_identity_mismatch")
    if _git_text(
        git,
        config.repo_root,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
    ):
        raise TagBurnError("preflight", "worktree_not_clean")

    fetch_urls = _git_text(
        git, config.repo_root, "remote", "get-url", "--all", "origin"
    ).splitlines()
    push_urls = _git_text(
        git,
        config.repo_root,
        "remote",
        "get-url",
        "--push",
        "--all",
        "origin",
    ).splitlines()
    if len(fetch_urls) != 1 or len(push_urls) != 1:
        raise TagBurnError("preflight", "origin_url_count_invalid")
    fetch_path = _normalize_local_remote(fetch_urls[0])
    push_path = _normalize_local_remote(push_urls[0])
    if not _same_path(fetch_path, config.mirror_root) or not _same_path(
        push_path, config.mirror_root
    ):
        raise TagBurnError("preflight", "origin_mirror_mismatch")

    tag_ref = f"refs/tags/{config.tag}"
    if _tag_oid_or_none(
        git, config.repo_root, tag_ref, stage="preflight"
    ) is not None or _tag_oid_or_none(
        git, config.mirror_root, tag_ref, stage="preflight"
    ) is not None:
        raise TagBurnError("preflight", "target_tag_present")
    if not _git_text(git, config.repo_root, "config", "user.name") or not _git_text(
        git, config.repo_root, "config", "user.email"
    ):
        raise TagBurnError("preflight", "tagger_identity_missing")

    mirror_refs = _refs(git, config.mirror_root, stage="preflight")
    return {
        "refs": refs,
        "tree": tree,
        "origin_fetch": str(fetch_path),
        "origin_push": str(push_path),
        "mirror_refs": mirror_refs,
        "worktree_clean": True,
        "target_tag_absent_both": True,
    }


def _run_logged_mutation(
    git: GitAuthority,
    repo: Path,
    args: Sequence[str],
) -> MutationResult:
    completed = _run_git_bounded(
        git, repo, args, stage="mutation", timeout_seconds=60
    )
    return MutationResult(
        completed.returncode,
        completed.abort_reason,
        completed.stdout_evidence,
        completed.stderr_evidence,
        completed.tree_cleanup,
    )


def _attest_tag(
    git: GitAuthority,
    repo: Path,
    *,
    tag: str,
    expected_commit: str,
    stage: str,
) -> dict[str, object]:
    tag_ref = f"refs/tags/{tag}"
    tag_object = _tag_oid_or_none(git, repo, tag_ref, stage=stage)
    if tag_object is None:
        raise TagBurnError(stage, "tag_missing")
    if _git_text(git, repo, "cat-file", "-t", tag_ref, stage=stage) != "tag":
        raise TagBurnError(stage, "tag_not_annotated")
    peeled = _git_text(
        git, repo, "rev-parse", "--verify", f"{tag_ref}^{{commit}}", stage=stage
    ).lower()
    if peeled != expected_commit:
        raise TagBurnError(stage, "tag_peel_mismatch")
    raw = _capture_git(
        git, repo, ("cat-file", "tag", tag_ref), stage=stage
    ).stdout
    if b"\r" in raw or b"\x00" in raw:
        raise TagBurnError(stage, "tag_object_not_canonical_text")
    header, separator, message = raw.partition(b"\n\n")
    if not separator:
        raise TagBurnError(stage, "tag_message_missing")
    try:
        header_text = header.decode("utf-8")
        message.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise TagBurnError(stage, "tag_object_not_utf8") from exc
    headers: dict[str, str] = {}
    for line in header_text.splitlines():
        key, space, value = line.partition(" ")
        if not space or key in headers:
            raise TagBurnError(stage, "tag_header_invalid")
        headers[key] = value
    if set(headers) != {"object", "type", "tag", "tagger"}:
        raise TagBurnError(stage, "tag_headers_not_exact")
    if (
        headers["object"].lower() != expected_commit
        or headers["type"] != "commit"
        or headers["tag"] != tag
    ):
        raise TagBurnError(stage, "tag_header_identity_mismatch")
    expected_message = f"Release {tag}\n".encode("ascii")
    if message != expected_message:
        raise TagBurnError(stage, "tag_message_not_canonical")
    return {
        "status": "PASS",
        "tag": tag,
        "tag_object": tag_object,
        "tag_object_type": "tag",
        "peeled_commit": peeled,
        "message_bytes": len(message),
        "message_sha256": hashlib.sha256(message).hexdigest(),
        "message_final_lf": message.endswith(b"\n"),
    }


def _safe_state(
    config: BurnConfig,
    git: GitAuthority,
) -> dict[str, object]:
    tag_ref = f"refs/tags/{config.tag}"
    try:
        return {
            "status": "PROVEN",
            "work_tag_object": _tag_oid_or_none(
                git, config.repo_root, tag_ref, stage="failure_state"
            ),
            "mirror_tag_object": _tag_oid_or_none(
                git, config.mirror_root, tag_ref, stage="failure_state"
            ),
            "work_head": _git_text(
                git, config.repo_root, "rev-parse", "HEAD", stage="failure_state"
            ).lower(),
            "mirror_main": _git_text(
                git,
                config.mirror_root,
                "rev-parse",
                "refs/heads/main",
                stage="failure_state",
            ).lower(),
            "worktree_clean": not bool(
                _git_text(
                    git,
                    config.repo_root,
                    "status",
                    "--porcelain=v1",
                    "--untracked-files=all",
                    stage="failure_state",
                )
            ),
        }
    except Exception as exc:
        return {"status": "UNPROVEN", "error_type": type(exc).__name__}


def _tree_termination_authority(git: GitAuthority) -> dict[str, object]:
    return {
        "method": "taskkill_pid_tree_force",
        "path": str(git.taskkill_path),
        "sha256": git.taskkill_sha256,
    }


def _record_failure(
    config: BurnConfig,
    git: GitAuthority,
    context: dict[str, object],
    error: TagBurnError,
) -> None:
    path = config.evidence_root / "tag-burn-failure.json"
    if path.exists():
        return
    payload = {
        "schema_version": SCHEMA_VERSION,
        "status": "FAIL",
        "stage": error.stage,
        "error_code": error.code,
        "tag": config.tag,
        "expected_commit": config.expected_commit,
        "expected_tree": config.expected_tree,
        "git": {"path": str(git.path), "sha256": git.sha256},
        "process_tree_termination": _tree_termination_authority(git),
        "mutation_counts": context.get(
            "mutation_counts", {"tag_create": 0, "tag_push": 0}
        ),
        "command_evidence": context.get("command_evidence", []),
        "state": (
            _safe_state(config, git)
            if error.state_query_safe
            else {
                "status": "UNPROVEN",
                "reason": "process_tree_cleanup_not_proven",
            }
        ),
        "retry_allowed": False,
        "automatic_ref_cleanup_performed": False,
    }
    try:
        _write_json_create_new(path, payload)
    except Exception:
        return


def burn_local_release_tag_once(config: BurnConfig) -> dict[str, object]:
    if config.tag != ACTIVE_RELEASE_TAG:
        raise TagBurnError("arguments", "tag_not_active_release")
    if OID_RE.fullmatch(config.expected_commit) is None or OID_RE.fullmatch(
        config.expected_tree
    ) is None:
        raise TagBurnError("arguments", "expected_identity_invalid")
    config = _validate_paths(config)
    git = _resolve_git_authority()
    config.evidence_root.mkdir(exist_ok=False)
    canonical_message = f"Release {config.tag}\n".encode("ascii")
    claim = {
        "schema_version": SCHEMA_VERSION,
        "status": "CLAIMED",
        "tag": config.tag,
        "expected_commit": config.expected_commit,
        "expected_tree": config.expected_tree,
        "repo_root": str(config.repo_root),
        "mirror_root": str(config.mirror_root),
        "git": {"path": str(git.path), "sha256": git.sha256},
        "process_tree_termination": _tree_termination_authority(git),
        "canonical_message_bytes": len(canonical_message),
        "canonical_message_sha256": hashlib.sha256(canonical_message).hexdigest(),
        "retry_allowed": False,
    }
    _write_json_create_new(config.evidence_root / "burn-claim.json", claim)
    context: dict[str, object] = {
        "mutation_counts": {"tag_create": 0, "tag_push": 0},
        "command_evidence": [],
    }
    try:
        preflight = _preflight(config, git)
        _write_json_create_new(
            config.evidence_root / "preflight.json",
            {"schema_version": SCHEMA_VERSION, "status": "PASS", **preflight},
        )
        mirror_refs_before = dict(preflight["mirror_refs"])

        message_path = config.evidence_root / "tag-message.txt"
        _write_bytes_create_new(message_path, canonical_message)
        raw_message = message_path.read_bytes()
        if (
            raw_message != canonical_message
            or raw_message.startswith(b"\xef\xbb\xbf")
            or not raw_message.endswith(b"\n")
        ):
            raise TagBurnError("message", "canonical_message_attestation_failed")

        tag_args = (
            "-c",
            "tag.gpgSign=false",
            "tag",
            "-a",
            "--cleanup=verbatim",
            "-F",
            str(message_path),
            config.tag,
            config.expected_commit,
        )
        _write_json_create_new(
            config.evidence_root / "tag-create-attempt.json",
            {
                "schema_version": SCHEMA_VERSION,
                "invocation_count": 1,
                "git_path": str(git.path),
                "arguments": list(tag_args),
            },
        )
        context["mutation_counts"]["tag_create"] = 1
        tag_result = _run_logged_mutation(
            git,
            config.repo_root,
            tag_args,
        )
        tag_evidence = {
            "operation": "tag_create",
            "returncode": tag_result.returncode,
            "abort_reason": tag_result.abort_reason,
            "stdout": tag_result.stdout,
            "stderr": tag_result.stderr,
            "tree_cleanup": tag_result.tree_cleanup,
        }
        context["command_evidence"].append(tag_evidence)
        if tag_result.abort_reason is not None or tag_result.returncode != 0:
            raise TagBurnError(
                "tag_create",
                "tag_create_failed",
                state_query_safe=(
                    tag_result.abort_reason is None
                    or tag_result.tree_cleanup["status"] == "PASS"
                ),
            )

        local_attestation = _attest_tag(
            git,
            config.repo_root,
            tag=config.tag,
            expected_commit=config.expected_commit,
            stage="local_attestation",
        )
        _write_json_create_new(
            config.evidence_root / "local-tag-attestation.json",
            {"schema_version": SCHEMA_VERSION, **local_attestation},
        )
        if _tag_oid_or_none(
            git,
            config.mirror_root,
            f"refs/tags/{config.tag}",
            stage="prepush",
        ) is not None:
            raise TagBurnError("prepush", "mirror_target_tag_appeared")
        if _refs(git, config.mirror_root, stage="prepush") != mirror_refs_before:
            raise TagBurnError("prepush", "mirror_refs_changed")
        if _preflight_after_local_tag(config, git) is not True:
            raise TagBurnError("prepush", "topology_changed")

        tag_ref = f"refs/tags/{config.tag}"
        push_args = ("push", "origin", f"{tag_ref}:{tag_ref}")
        _write_json_create_new(
            config.evidence_root / "tag-push-attempt.json",
            {
                "schema_version": SCHEMA_VERSION,
                "invocation_count": 1,
                "git_path": str(git.path),
                "arguments": list(push_args),
            },
        )
        context["mutation_counts"]["tag_push"] = 1
        push_result = _run_logged_mutation(
            git,
            config.repo_root,
            push_args,
        )
        push_evidence = {
            "operation": "tag_push",
            "returncode": push_result.returncode,
            "abort_reason": push_result.abort_reason,
            "stdout": push_result.stdout,
            "stderr": push_result.stderr,
            "tree_cleanup": push_result.tree_cleanup,
        }
        context["command_evidence"].append(push_evidence)
        if push_result.abort_reason is not None or push_result.returncode != 0:
            raise TagBurnError(
                "tag_push",
                "tag_push_failed",
                state_query_safe=(
                    push_result.abort_reason is None
                    or push_result.tree_cleanup["status"] == "PASS"
                ),
            )

        mirror_attestation = _attest_tag(
            git,
            config.mirror_root,
            tag=config.tag,
            expected_commit=config.expected_commit,
            stage="mirror_attestation",
        )
        if mirror_attestation["tag_object"] != local_attestation["tag_object"]:
            raise TagBurnError("mirror_attestation", "tag_object_mismatch")
        expected_mirror_refs = dict(mirror_refs_before)
        expected_mirror_refs[tag_ref] = str(local_attestation["tag_object"])
        if _refs(git, config.mirror_root, stage="final") != expected_mirror_refs:
            raise TagBurnError("final", "tag_only_push_not_proven")
        if _preflight_after_local_tag(config, git) is not True:
            raise TagBurnError("final", "topology_changed")

        receipt = {
            "schema_version": SCHEMA_VERSION,
            "status": "PASS",
            "tag": config.tag,
            "expected_commit": config.expected_commit,
            "expected_tree": config.expected_tree,
            "git": {"path": str(git.path), "sha256": git.sha256},
            "process_tree_termination": _tree_termination_authority(git),
            "canonical_message": {
                "bytes": len(canonical_message),
                "sha256": hashlib.sha256(canonical_message).hexdigest(),
                "bom_absent": not canonical_message.startswith(b"\xef\xbb\xbf"),
                "final_lf": canonical_message.endswith(b"\n"),
            },
            "local_attestation": local_attestation,
            "mirror_attestation": mirror_attestation,
            "mutation_counts": context["mutation_counts"],
            "command_evidence": context["command_evidence"],
            "tag_only_mirror_push": True,
            "retry_allowed": False,
        }
        _write_json_create_new(
            config.evidence_root / "tag-burn-receipt.json", receipt
        )
        return receipt
    except TagBurnError as exc:
        _record_failure(config, git, context, exc)
        raise
    except Exception as exc:
        error = TagBurnError("unexpected", f"unexpected_{type(exc).__name__}")
        _record_failure(config, git, context, error)
        raise error from exc


def _preflight_after_local_tag(config: BurnConfig, git: GitAuthority) -> bool:
    values = (
        _git_text(git, config.repo_root, "rev-parse", "HEAD").lower(),
        _git_text(
            git, config.repo_root, "rev-parse", "refs/heads/main"
        ).lower(),
        _git_text(
            git, config.repo_root, "rev-parse", "refs/remotes/origin/main"
        ).lower(),
        _git_text(
            git, config.mirror_root, "rev-parse", "refs/heads/main"
        ).lower(),
    )
    if any(value != config.expected_commit for value in values):
        return False
    if (
        _git_text(git, config.repo_root, "rev-parse", "HEAD^{tree}").lower()
        != config.expected_tree
    ):
        return False
    if _git_text(
        git,
        config.repo_root,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
    ):
        return False
    fetch_urls = _git_text(
        git, config.repo_root, "remote", "get-url", "--all", "origin"
    ).splitlines()
    push_urls = _git_text(
        git,
        config.repo_root,
        "remote",
        "get-url",
        "--push",
        "--all",
        "origin",
    ).splitlines()
    if len(fetch_urls) != 1 or len(push_urls) != 1:
        return False
    fetch = _normalize_local_remote(fetch_urls[0])
    push = _normalize_local_remote(push_urls[0])
    return _same_path(fetch, config.mirror_root) and _same_path(
        push, config.mirror_root
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Burn and locally mirror the canonical Label_Match tag once"
    )
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--mirror-root", required=True)
    parser.add_argument("--evidence-root", required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--expected-tree", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        receipt = burn_local_release_tag_once(
            BurnConfig(
                repo_root=Path(args.repo_root),
                mirror_root=Path(args.mirror_root),
                evidence_root=Path(args.evidence_root),
                tag=args.tag,
                expected_commit=str(args.expected_commit).lower(),
                expected_tree=str(args.expected_tree).lower(),
            )
        )
    except TagBurnError as exc:
        print(f"release_tag_burn=DENY stage={exc.stage} code={exc.code}")
        return 2
    print(
        "release_tag_burn=PASS "
        f"tag={receipt['tag']} object={receipt['local_attestation']['tag_object']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

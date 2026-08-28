from __future__ import annotations

import ast
import ctypes
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / "tools" / "burn_local_release_tag_once.py"
TAG = "v2.0.94"
MESSAGE = b"Release v2.0.94\n"
MESSAGE_SHA256 = "7dda2dcfbbacdf10c24c8d45cd3475ce77e3e591f5c0685fa2a5f99c9401f031"


def _git(
    repo: Path,
    *args: str,
    input_bytes: bytes | None = None,
    allowed_returncodes: tuple[int, ...] = (0,),
) -> subprocess.CompletedProcess[bytes]:
    git = shutil.which("git")
    assert git is not None
    completed = subprocess.run(
        [git, "-C", str(repo), *args],
        input=input_bytes,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=30,
        shell=False,
    )
    assert completed.returncode in allowed_returncodes, completed.stderr.decode(
        "utf-8", errors="replace"
    )
    return completed


def _refs(repo: Path) -> dict[str, str]:
    raw = _git(
        repo, "for-each-ref", "--format=%(refname) %(objectname)"
    ).stdout.decode("ascii")
    return dict(line.split(" ", 1) for line in raw.splitlines())


def _synthetic_release_repositories(tmp_path: Path) -> dict[str, object]:
    seed = tmp_path / "seed"
    mirror = tmp_path / "mirror.git"
    work = tmp_path / "work"
    seed.mkdir()
    _git(seed, "init", "-b", "main")
    _git(seed, "config", "user.name", "Label Match Test")
    _git(seed, "config", "user.email", "label-match-test@example.invalid")
    (seed / "candidate.txt").write_text("candidate\n", encoding="ascii")
    _git(seed, "add", "candidate.txt")
    _git(seed, "commit", "-m", "Create synthetic candidate")

    mirror.mkdir()
    _git(mirror, "init", "--bare")
    _git(seed, "remote", "add", "origin", str(mirror.resolve()))
    _git(seed, "push", "-u", "origin", "main")
    _git(mirror, "symbolic-ref", "HEAD", "refs/heads/main")
    _git(tmp_path, "clone", str(mirror.resolve()), str(work.resolve()))
    _git(work, "config", "user.name", "Label Match Test")
    _git(work, "config", "user.email", "label-match-test@example.invalid")

    commit = _git(work, "rev-parse", "HEAD").stdout.decode("ascii").strip()
    tree = _git(work, "rev-parse", "HEAD^{tree}").stdout.decode("ascii").strip()
    return {
        "work": work.resolve(),
        "mirror": mirror.resolve(),
        "commit": commit,
        "tree": tree,
        "mirror_refs_before": _refs(mirror),
    }


def _invoke(
    repos: dict[str, object],
    evidence: Path,
) -> subprocess.CompletedProcess[bytes]:
    environment = os.environ.copy()
    environment["LABEL_MATCH_GIT_EXECUTABLE"] = str(evidence.parent / "bogus-git")
    environment["GIT_EXECUTABLE"] = str(evidence.parent / "bogus-git")
    return subprocess.run(
        [
            sys.executable,
            "-I",
            str(TOOL),
            "--repo-root",
            str(repos["work"]),
            "--mirror-root",
            str(repos["mirror"]),
            "--evidence-root",
            str(evidence.resolve()),
            "--tag",
            TAG,
            "--expected-commit",
            str(repos["commit"]),
            "--expected-tree",
            str(repos["tree"]),
        ],
        env=environment,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=60,
        shell=False,
    )


def _json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="ascii"))


def _windows_pid_exists(pid: int) -> bool:
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    open_process = kernel32.OpenProcess
    open_process.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32]
    open_process.restype = ctypes.c_void_p
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = [ctypes.c_void_p]
    close_handle.restype = ctypes.c_int
    handle = open_process(0x1000, 0, int(pid))
    if not handle:
        return False
    close_handle(handle)
    return True


def test_temp_only_success_creates_and_mirrors_one_canonical_tag(tmp_path: Path):
    repos = _synthetic_release_repositories(tmp_path)
    evidence = tmp_path / "evidence-success"

    completed = _invoke(repos, evidence)

    assert completed.returncode == 0, completed.stdout.decode("ascii")
    assert completed.stderr == b""
    assert (evidence / "burn-claim.json").is_file()
    assert (evidence / "tag-message.txt").read_bytes() == MESSAGE
    assert len(MESSAGE) == 16
    assert hashlib.sha256(MESSAGE).hexdigest() == MESSAGE_SHA256
    assert not MESSAGE.startswith(b"\xef\xbb\xbf")
    assert MESSAGE.endswith(b"\n") and b"\r" not in MESSAGE

    work = repos["work"]
    mirror = repos["mirror"]
    assert isinstance(work, Path) and isinstance(mirror, Path)
    work_oid = _git(work, "rev-parse", f"refs/tags/{TAG}").stdout.strip()
    mirror_oid = _git(mirror, "rev-parse", f"refs/tags/{TAG}").stdout.strip()
    assert work_oid == mirror_oid
    assert _git(work, "cat-file", "-t", f"refs/tags/{TAG}").stdout == b"tag\n"
    assert _git(mirror, "cat-file", "-t", f"refs/tags/{TAG}").stdout == b"tag\n"
    assert (
        _git(work, "rev-parse", f"refs/tags/{TAG}^{{commit}}").stdout.decode(
            "ascii"
        ).strip()
        == repos["commit"]
    )
    raw_tag = _git(work, "cat-file", "tag", f"refs/tags/{TAG}").stdout
    assert raw_tag.partition(b"\n\n")[2] == MESSAGE
    expected_refs = dict(repos["mirror_refs_before"])
    expected_refs[f"refs/tags/{TAG}"] = work_oid.decode("ascii")
    assert _refs(mirror) == expected_refs

    receipt = _json(evidence / "tag-burn-receipt.json")
    git_path = Path(receipt["git"]["path"])
    assert git_path.is_absolute() and git_path.is_file()
    assert receipt["git"]["sha256"] == hashlib.sha256(git_path.read_bytes()).hexdigest()
    assert receipt["status"] == "PASS"
    assert receipt["canonical_message"] == {
        "bom_absent": True,
        "bytes": 16,
        "final_lf": True,
        "sha256": MESSAGE_SHA256,
    }
    assert receipt["mutation_counts"] == {"tag_create": 1, "tag_push": 1}
    assert receipt["tag_only_mirror_push"] is True


def test_malformed_preexisting_temp_tag_is_preserved_and_never_pushed(tmp_path: Path):
    repos = _synthetic_release_repositories(tmp_path)
    work = repos["work"]
    mirror = repos["mirror"]
    assert isinstance(work, Path) and isinstance(mirror, Path)
    _git(work, "tag", "-a", "-m", "malformed", TAG, str(repos["commit"]))
    original_oid = _git(work, "rev-parse", f"refs/tags/{TAG}").stdout

    first_evidence = tmp_path / "evidence-malformed-first"
    first = _invoke(repos, first_evidence)

    assert first.returncode == 2
    assert _git(work, "rev-parse", f"refs/tags/{TAG}").stdout == original_oid
    assert (
        _git(
            mirror,
            "rev-parse",
            "--verify",
            "--quiet",
            f"refs/tags/{TAG}",
            allowed_returncodes=(1,),
        ).stdout
        == b""
    )
    failure = _json(first_evidence / "tag-burn-failure.json")
    assert failure["stage"] == "preflight"
    assert failure["error_code"] == "target_tag_present"
    assert failure["mutation_counts"] == {"tag_create": 0, "tag_push": 0}
    assert failure["automatic_ref_cleanup_performed"] is False
    assert failure["retry_allowed"] is False

    second_evidence = tmp_path / "evidence-malformed-second"
    second = _invoke(repos, second_evidence)
    assert second.returncode == 2
    assert _git(work, "rev-parse", f"refs/tags/{TAG}").stdout == original_oid
    second_failure = _json(second_evidence / "tag-burn-failure.json")
    assert second_failure["error_code"] == "target_tag_present"
    assert second_failure["mutation_counts"] == {"tag_create": 0, "tag_push": 0}


def test_rejected_temp_push_burns_local_tag_and_second_invocation_refuses(tmp_path: Path):
    repos = _synthetic_release_repositories(tmp_path)
    work = repos["work"]
    mirror = repos["mirror"]
    assert isinstance(work, Path) and isinstance(mirror, Path)
    hook = mirror / "hooks" / "pre-receive"
    hook.write_text("#!/bin/sh\necho rejected-by-temp-test >&2\nexit 1\n", encoding="ascii")
    hook.chmod(0o755)

    first_evidence = tmp_path / "evidence-rejected-first"
    first = _invoke(repos, first_evidence)

    assert first.returncode == 2
    local_oid = _git(work, "rev-parse", f"refs/tags/{TAG}").stdout
    assert _git(work, "cat-file", "-t", f"refs/tags/{TAG}").stdout == b"tag\n"
    assert (
        _git(
            mirror,
            "rev-parse",
            "--verify",
            "--quiet",
            f"refs/tags/{TAG}",
            allowed_returncodes=(1,),
        ).stdout
        == b""
    )
    first_failure = _json(first_evidence / "tag-burn-failure.json")
    assert first_failure["stage"] == "tag_push"
    assert first_failure["error_code"] == "tag_push_failed"
    assert first_failure["mutation_counts"] == {"tag_create": 1, "tag_push": 1}
    assert first_failure["state"]["work_tag_object"] == local_oid.decode("ascii").strip()
    assert first_failure["state"]["mirror_tag_object"] is None
    assert first_failure["automatic_ref_cleanup_performed"] is False

    second_evidence = tmp_path / "evidence-rejected-second"
    second = _invoke(repos, second_evidence)

    assert second.returncode == 2
    assert _git(work, "rev-parse", f"refs/tags/{TAG}").stdout == local_oid
    second_failure = _json(second_evidence / "tag-burn-failure.json")
    assert second_failure["error_code"] == "target_tag_present"
    assert second_failure["mutation_counts"] == {"tag_create": 0, "tag_push": 0}


def test_noisy_rejected_temp_push_is_incrementally_bounded_and_not_preserved(
    tmp_path: Path,
):
    repos = _synthetic_release_repositories(tmp_path)
    mirror = repos["mirror"]
    assert isinstance(mirror, Path)
    hook = mirror / "hooks" / "pre-receive"
    hook.write_text(
        "#!/bin/sh\n"
        "i=0\n"
        "while [ $i -lt 5000 ]; do\n"
        "  printf 'PRIVATE-TEMP-SENTINEL-0123456789\\n' >&2\n"
        "  i=$((i + 1))\n"
        "done\n"
        "exit 1\n",
        encoding="ascii",
    )
    hook.chmod(0o755)
    evidence = tmp_path / "evidence-noisy-rejection"

    completed = _invoke(repos, evidence)

    assert completed.returncode == 2
    failure = _json(evidence / "tag-burn-failure.json")
    assert failure["stage"] == "tag_push"
    assert failure["error_code"] == "tag_push_failed"
    push_evidence = failure["command_evidence"][-1]
    assert push_evidence["operation"] == "tag_push"
    assert push_evidence["abort_reason"] == "capture_limit_exceeded"
    assert push_evidence["stderr"]["bytes"] > 64 * 1024
    assert push_evidence["stderr"]["content_preserved"] is False
    cleanup_status = push_evidence["tree_cleanup"]["status"]
    assert cleanup_status in {"PASS", "UNPROVEN"}
    if cleanup_status == "PASS":
        assert failure["state"]["status"] == "PROVEN"
    else:
        assert failure["state"] == {
            "reason": "process_tree_cleanup_not_proven",
            "status": "UNPROVEN",
        }
    assert not (
        failure["state"]["status"] == "PROVEN"
        and cleanup_status != "PASS"
    )
    assert not list(evidence.glob("*.log"))
    for path in evidence.iterdir():
        if path.is_file():
            assert b"PRIVATE-TEMP-SENTINEL" not in path.read_bytes()


def test_long_lived_temp_hook_tree_is_gone_before_failure_state_is_recorded(
    tmp_path: Path,
):
    repos = _synthetic_release_repositories(tmp_path)
    mirror = repos["mirror"]
    assert isinstance(mirror, Path)
    powershell = shutil.which("powershell.exe")
    assert powershell is not None
    hook_pid = tmp_path / "long-hook.pid"
    late_marker = tmp_path / "long-hook-finished.txt"
    hook_script = tmp_path / "long-hook.ps1"
    hook_script.write_text(
        "$ErrorActionPreference = 'Stop'\n"
        f"[IO.File]::WriteAllText('{hook_pid}', [string]$PID)\n"
        "Start-Sleep -Seconds 30\n"
        f"[IO.File]::WriteAllText('{late_marker}', 'unexpected')\n"
        "exit 1\n",
        encoding="utf-8",
    )
    hook = mirror / "hooks" / "pre-receive"
    hook.write_text(
        "#!/bin/sh\n"
        f'"{Path(powershell).resolve().as_posix()}" '
        "-NoProfile -NonInteractive -ExecutionPolicy Bypass "
        f'-File "{hook_script.resolve().as_posix()}" &\n'
        "child=$!\n"
        "i=0\n"
        f'while [ ! -f "{hook_pid.resolve().as_posix()}" ] && '
        "[ $i -lt 200 ]; do\n"
        "  sleep 0.05\n"
        "  i=$((i + 1))\n"
        "done\n"
        "i=0\n"
        "while [ $i -lt 5000 ]; do\n"
        "  printf 'LONG-HOOK-NOISE-0123456789\\n' >&2\n"
        "  i=$((i + 1))\n"
        "done\n"
        "wait $child\n"
        "exit 1\n",
        encoding="ascii",
    )
    hook.chmod(0o755)
    evidence = tmp_path / "evidence-long-hook"

    completed = _invoke(repos, evidence)

    assert completed.returncode == 2
    assert hook_pid.is_file()
    pid = int(hook_pid.read_text(encoding="ascii"))
    assert not _windows_pid_exists(pid)
    failure = _json(evidence / "tag-burn-failure.json")
    push_evidence = failure["command_evidence"][-1]
    cleanup = push_evidence["tree_cleanup"]
    assert cleanup["status"] == "PASS"
    assert cleanup["method"] == "taskkill_pid_tree_force"
    assert cleanup["observed_process_count"] >= 2
    assert cleanup["remaining_process_count"] == 0
    assert failure["state"]["status"] == "PROVEN"
    assert failure["state"]["mirror_tag_object"] is None
    mirror_refs_after = _refs(mirror)
    assert mirror_refs_after == repos["mirror_refs_before"]
    time.sleep(1)
    assert not _windows_pid_exists(pid)
    assert not late_marker.exists()
    assert _refs(mirror) == mirror_refs_after


def test_tool_parser_and_mutation_surface_are_static_and_fail_closed():
    source = TOOL.read_text(encoding="utf-8")
    tree = ast.parse(source)
    literals = {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }

    assert "shell=True" not in source
    assert 'shell=False' in source
    assert "process.kill" not in source
    assert "--git" not in source
    assert "LABEL_MATCH_GIT_EXECUTABLE" not in source
    assert "GIT_EXECUTABLE" not in source
    assert {"fetch", "--force", "--delete", "delete", "reset", "rollback"}.isdisjoint(
        literals
    )
    assert 'push_args = ("push", "origin", f"{tag_ref}:{tag_ref}")' in source
    assert '"--cleanup=verbatim"' in source
    assert '"tag.gpgSign=false"' in source
    assert source.count("_run_logged_mutation(") == 3
    assert 'str(git.taskkill_path),\n                "/PID"' in source
    assert '"/T"' in source and '"/F"' in source

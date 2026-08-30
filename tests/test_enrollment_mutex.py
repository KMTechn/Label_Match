from __future__ import annotations

import ctypes
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

from enrollment_mutex import (
    ENROLLMENT_MUTEX_NAME,
    EnrollmentMutex,
    EnrollmentMutexNotOwned,
)
from tests.enrollment_entrypoint_inventory import (
    derive_enrollment_entrypoint_inventory,
)
from tools import register_label_match_worker_pc as registration


ROOT = Path(__file__).resolve().parents[1]
CHILD = Path(__file__).resolve().with_name("_enrollment_mutex_child.py")


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _wait_for_file(path: Path, timeout_seconds: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while not path.is_file():
        if time.monotonic() >= deadline:
            raise AssertionError(f"timed out waiting for child evidence: {path.name}")
        time.sleep(0.01)


def _spawn_child(
    root: Path,
    label: str,
    *,
    hold_seconds: float,
    mutex_timeout_seconds: float,
) -> tuple[subprocess.Popen[str], dict[str, Path]]:
    paths = {
        name: root / f"{label}-{name}.json"
        for name in ("attempt", "entered", "result")
    }
    process = subprocess.Popen(
        [
            sys.executable,
            str(CHILD),
            "--label",
            label,
            "--start-path",
            str(root / "start.json"),
            "--attempt-path",
            str(paths["attempt"]),
            "--entered-path",
            str(paths["entered"]),
            "--result-path",
            str(paths["result"]),
            "--hold-seconds",
            str(hold_seconds),
            "--mutex-timeout-seconds",
            str(mutex_timeout_seconds),
        ],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return process, paths


def _finish(
    process: subprocess.Popen[str], timeout_seconds: float = 15.0
) -> tuple[int, str, str]:
    stdout, stderr = process.communicate(timeout=timeout_seconds)
    assert len(stdout) < 20_000
    assert len(stderr) < 20_000
    return int(process.returncode), stdout, stderr


def _evidence_root(tmp_path: Path, name: str) -> Path:
    configured = str(os.environ.get("LABEL_ENROLLMENT_MUTEX_EVIDENCE_DIR", "")).strip()
    root = (Path(configured) / name) if configured else (tmp_path / name)
    root.mkdir(parents=True, exist_ok=True)
    return root


def _write_evidence(root: Path, payload: dict) -> None:
    _write_json(
        root / "evidence.json",
        {
            "schema": "label-enrollment-mutex-negative-proof-v1",
            "mutex_name": ENROLLMENT_MUTEX_NAME,
            **payload,
        },
    )


def test_mutex_name_is_local_and_label_specific():
    assert ENROLLMENT_MUTEX_NAME == r"Local\KMTech.Enrollment.LabelMatch.v1"
    assert ENROLLMENT_MUTEX_NAME.startswith("Local\\")
    assert "OneSessionInstall" not in ENROLLMENT_MUTEX_NAME


@pytest.mark.skipif(sys.platform != "win32", reason="Windows named mutex proof")
def test_two_real_child_pids_allow_exactly_one_enrollment_body(tmp_path):
    root = _evidence_root(tmp_path, "two-child-contention")
    first, first_paths = _spawn_child(
        root, "first", hold_seconds=1.25, mutex_timeout_seconds=0.0
    )
    second, second_paths = _spawn_child(
        root, "second", hold_seconds=1.25, mutex_timeout_seconds=0.0
    )
    child_pids = [first.pid, second.pid]
    try:
        assert len(set(child_pids)) == 2
        assert os.getpid() not in child_pids
        _write_json(root / "start.json", {"status": "RELEASED"})
        first_exit, first_stdout, first_stderr = _finish(first)
        second_exit, second_stdout, second_stderr = _finish(second)
    finally:
        for process in (first, second):
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)

    results = [_read_json(first_paths["result"]), _read_json(second_paths["result"])]
    assert sorted(result["status"] for result in results) == [
        "BLOCKED_ENROLLMENT_MUTEX_TIMEOUT",
        "PASSED",
    ]
    assert sum(
        path.is_file() for path in (first_paths["entered"], second_paths["entered"])
    ) == 1
    assert sorted([first_exit, second_exit]) == [0, 2]
    _write_evidence(
        root,
        {
            "case": "two_real_child_pids",
            "child_pids": child_pids,
            "distinct_child_pids": True,
            "body_entry_count": 1,
            "results": results,
            "exits": [first_exit, second_exit],
            "stdout_bytes": [len(first_stdout.encode()), len(second_stdout.encode())],
            "stderr_bytes": [len(first_stderr.encode()), len(second_stderr.encode())],
            "status": "PASS",
        },
    )


@pytest.mark.skipif(sys.platform != "win32", reason="Windows named mutex proof")
def test_abandoned_owner_is_rejected_then_clean_retry_can_acquire(tmp_path):
    root = _evidence_root(tmp_path, "abandoned-owner")
    owner, owner_paths = _spawn_child(
        root, "owner", hold_seconds=30.0, mutex_timeout_seconds=0.0
    )
    observer = None
    kernel32 = None
    follower = None
    recovery = None
    try:
        _write_json(root / "start.json", {"status": "RELEASED"})
        _wait_for_file(owner_paths["entered"])

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateMutexW.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_wchar_p]
        kernel32.CreateMutexW.restype = ctypes.c_void_p
        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        kernel32.CloseHandle.restype = ctypes.c_int
        observer = kernel32.CreateMutexW(None, False, ENROLLMENT_MUTEX_NAME)
        assert observer

        killed_owner_pid = owner.pid
        owner.kill()
        owner.wait(timeout=5)
        follower, follower_paths = _spawn_child(
            root, "follower", hold_seconds=0.0, mutex_timeout_seconds=0.0
        )
        follower_exit, follower_stdout, follower_stderr = _finish(follower)
        follower_result = _read_json(follower_paths["result"])
        assert follower_exit == 2
        assert follower_result["status"] == "BLOCKED_ENROLLMENT_MUTEX_ABANDONED"
        assert follower_result["mutex"]["disposition"] == "ABANDONED_REJECTED"
        assert not follower_paths["entered"].exists()

        assert kernel32.CloseHandle(observer)
        observer = None
        recovery, recovery_paths = _spawn_child(
            root, "recovery", hold_seconds=0.0, mutex_timeout_seconds=0.0
        )
        recovery_exit, recovery_stdout, recovery_stderr = _finish(recovery)
        recovery_result = _read_json(recovery_paths["result"])
        assert recovery_exit == 0
        assert recovery_result["status"] == "PASSED"
        assert recovery_paths["entered"].is_file()
        _write_evidence(
            root,
            {
                "case": "abandoned_owner",
                "forced_terminated_owner_pid": killed_owner_pid,
                "follower_pid": follower.pid,
                "recovery_pid": recovery.pid,
                "all_distinct": len({killed_owner_pid, follower.pid, recovery.pid}) == 3,
                "follower": follower_result,
                "recovery": recovery_result,
                "stdout_bytes": [len(follower_stdout.encode()), len(recovery_stdout.encode())],
                "stderr_bytes": [len(follower_stderr.encode()), len(recovery_stderr.encode())],
                "status": "PASS",
            },
        )
    finally:
        if observer and kernel32 is not None:
            kernel32.CloseHandle(observer)
        for process in (owner, follower, recovery):
            if process is not None and process.poll() is None:
                process.kill()
                process.wait(timeout=5)


@pytest.mark.skipif(sys.platform != "win32", reason="Windows named mutex proof")
def test_positive_timeout_is_bounded_and_never_enters_body(tmp_path):
    root = _evidence_root(tmp_path, "positive-timeout")
    owner, owner_paths = _spawn_child(
        root, "owner", hold_seconds=1.5, mutex_timeout_seconds=0.0
    )
    waiter = None
    try:
        _write_json(root / "start.json", {"status": "RELEASED"})
        _wait_for_file(owner_paths["entered"])
        waiter, waiter_paths = _spawn_child(
            root, "waiter", hold_seconds=0.0, mutex_timeout_seconds=0.2
        )
        waiter_exit, waiter_stdout, waiter_stderr = _finish(waiter)
        waiter_result = _read_json(waiter_paths["result"])
        owner_exit, owner_stdout, owner_stderr = _finish(owner)
        assert owner_exit == 0
        assert waiter_exit == 2
        assert waiter_result["status"] == "BLOCKED_ENROLLMENT_MUTEX_TIMEOUT"
        assert waiter_result["mutex"]["timeout_milliseconds"] == 200
        assert 150 <= waiter_result["mutex"]["wait_milliseconds"] < 1500
        assert not waiter_paths["entered"].exists()
        _write_evidence(
            root,
            {
                "case": "positive_timeout",
                "owner_pid": owner.pid,
                "waiter_pid": waiter.pid,
                "distinct_child_pids": owner.pid != waiter.pid,
                "waiter": waiter_result,
                "stdout_bytes": [len(owner_stdout.encode()), len(waiter_stdout.encode())],
                "stderr_bytes": [len(owner_stderr.encode()), len(waiter_stderr.encode())],
                "status": "PASS",
            },
        )
    finally:
        for process in (owner, waiter):
            if process is not None and process.poll() is None:
                process.kill()
                process.wait(timeout=5)


@pytest.mark.skipif(sys.platform != "win32", reason="Windows named mutex proof")
def test_same_thread_reentry_is_bounded_and_inner_release_keeps_outer_owned(tmp_path):
    root = _evidence_root(tmp_path, "same-thread-reentry")
    _write_json(root / "start.json", {"status": "RELEASED"})
    contender = None
    recovery = None
    try:
        started = time.perf_counter()
        with EnrollmentMutex(timeout_seconds=0.2) as outer:
            with EnrollmentMutex(timeout_seconds=0.2) as inner:
                assert outer["owner_pid"] == inner["owner_pid"] == os.getpid()
                assert outer["owner_thread_id"] == inner["owner_thread_id"]
                assert inner["disposition"] == "ACQUIRED"
            nested_elapsed_milliseconds = int(
                round((time.perf_counter() - started) * 1000.0)
            )
            assert nested_elapsed_milliseconds < 1000

            contender, contender_paths = _spawn_child(
                root, "contender", hold_seconds=0.0, mutex_timeout_seconds=0.0
            )
            contender_exit, contender_stdout, contender_stderr = _finish(contender)
            contender_result = _read_json(contender_paths["result"])
            assert contender_exit == 2
            assert contender_result["status"] == "BLOCKED_ENROLLMENT_MUTEX_TIMEOUT"
            assert not contender_paths["entered"].exists()

        recovery, recovery_paths = _spawn_child(
            root, "recovery", hold_seconds=0.0, mutex_timeout_seconds=0.0
        )
        recovery_exit, recovery_stdout, recovery_stderr = _finish(recovery)
        recovery_result = _read_json(recovery_paths["result"])
        assert recovery_exit == 0
        assert recovery_result["status"] == "PASSED"
        assert recovery_paths["entered"].is_file()
        _write_evidence(
            root,
            {
                "case": "same_thread_reentry",
                "owner_pid": os.getpid(),
                "owner_thread_id": outer["owner_thread_id"],
                "nested_elapsed_milliseconds": nested_elapsed_milliseconds,
                "contender_pid": contender.pid,
                "contender": contender_result,
                "recovery_pid": recovery.pid,
                "recovery": recovery_result,
                "stdout_bytes": [
                    len(contender_stdout.encode()),
                    len(recovery_stdout.encode()),
                ],
                "stderr_bytes": [
                    len(contender_stderr.encode()),
                    len(recovery_stderr.encode()),
                ],
                "status": "PASS",
            },
        )
    finally:
        for process in (contender, recovery):
            if process is not None and process.poll() is None:
                process.kill()
                process.wait(timeout=5)


def test_direct_enrollment_transport_cannot_bypass_mutex(monkeypatch):
    calls = []
    monkeypatch.setattr(
        registration.requests,
        "post",
        lambda *_args, **_kwargs: calls.append(True),
    )
    with pytest.raises(EnrollmentMutexNotOwned):
        registration._enroll(
            {"contract_version": registration.ENROLLMENT_CONTRACT_VERSION},
            enrollment_url="https://worker.example.invalid/api/producer-ingest/v2/enroll",
            enrollment_token="",
            timeout_seconds=1,
        )
    assert calls == []


def test_direct_admin_recovery_cannot_mutate_before_mutex_ownership():
    with pytest.raises(EnrollmentMutexNotOwned):
        registration._admin_recover(registration.argparse.Namespace(), {}, {})


@pytest.mark.skipif(sys.platform != "win32", reason="Windows named mutex proof")
def test_direct_transport_is_allowed_only_while_current_thread_owns_mutex(monkeypatch):
    response = type(
        "Response",
        (),
        {"status_code": 200, "json": lambda self: {"status": "local-test"}},
    )()
    calls = []
    monkeypatch.setattr(
        registration.requests,
        "post",
        lambda *_args, **_kwargs: calls.append(True) or response,
    )
    with EnrollmentMutex():
        result = registration._enroll(
            {"contract_version": registration.ENROLLMENT_CONTRACT_VERSION},
            enrollment_url="https://worker.example.invalid/api/producer-ingest/v2/enroll",
            enrollment_token="",
            timeout_seconds=1,
        )
    assert result == {"status": "local-test"}
    assert calls == [True]


def test_logical_entrypoint_inventory_is_derived_from_executable_code():
    inventory = derive_enrollment_entrypoint_inventory(ROOT)
    logical_ids = [row.logical_id for row in inventory]

    assert inventory
    assert len(logical_ids) == len(set(logical_ids))
    assert all(row.guard_path for row in inventory), [
        row.logical_id for row in inventory if not row.guard_path
    ]
    assert all(not row.source_path.startswith("tests/") for row in inventory)
    for row in inventory:
        source_path = ROOT / row.source_path
        source_lines = source_path.read_text(encoding="utf-8-sig").splitlines()
        assert 1 <= row.source_line <= len(source_lines)
        assert source_lines[row.source_line - 1].strip() == row.source_text
        assert hashlib.sha256(source_path.read_bytes()).hexdigest() == (
            row.source_sha256
        )


def test_registration_transport_guard_cardinality_is_unchanged():
    registration_source = (
        ROOT / "tools" / "register_label_match_worker_pc.py"
    ).read_text(encoding="utf-8-sig")

    assert ENROLLMENT_MUTEX_NAME == r"Local\KMTech.Enrollment.LabelMatch.v1"
    assert "EnrollmentMutex(args.enrollment_mutex_timeout_seconds)" in registration_source
    assert "with guard as receipt" in registration_source
    assert registration_source.count("require_enrollment_mutex_owned()") == 2
    assert registration_source.count("requests.post(") == 1
    assert registration_source.count("session.post(") == 1

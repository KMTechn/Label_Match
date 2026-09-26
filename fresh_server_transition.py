"""Explicit, resumable preservation of one Label_Match PC before fresh enrollment.

The canonical installer owns the existing writer fence.  This module never resets
an identity in ordinary onboarding and never edits an old business/relay database.
"""

from __future__ import annotations

import argparse
from contextlib import ExitStack, closing, contextmanager
import ctypes
from ctypes import wintypes
from datetime import datetime, timezone
from dataclasses import replace
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import sqlite3
import stat
import subprocess
import tempfile
from typing import Any, Mapping
from urllib.parse import urlsplit
import uuid

from current_user_onboarding import (
    CurrentUserOnboardingPaths, _file_sha256, _write_json_atomic,
    inspect_current_user_state, resolve_current_user_onboarding_paths,
)
from enrollment_mutex import EnrollmentMutex
from logistics_runtime_profile import assert_path_has_no_reparse_components
from writer_session_fence import WriterFenceError, _named_mutex_held_by_other, active_fence, writer_admission


SCHEMA = "label-match-fresh-server-transition-v1"
INPUT_SCHEMA = "kmtech.fresh-transition.v1"
JOURNAL_NAME = "fresh-server-transition.json"
TERMINAL_PHASES = {"ACTIVATED", "RESTORED"}
UNSENT_CONFIRMATION = "옛 미전송 자료는 보관만 하며 새 서버에 보내지 않습니다"
AUTHORITY_FIELDS = ("session_id", "attempt_id", "replacement_transaction_id")


class FreshTransitionError(ValueError):
    """A bounded, secret-free reason that leaves the old evidence intact."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json(path: Path, maximum: int = 65536) -> dict:
    from tools.register_label_match_worker_pc import _load_json_no_duplicate_keys
    assert_path_has_no_reparse_components(path, label="전환 입력")
    with path.open("rb") as handle:
        raw = handle.read(maximum + 1)
    if not raw or len(raw) > maximum:
        raise FreshTransitionError("지원 입력/기록 크기가 올바르지 않습니다")
    value = _load_json_no_duplicate_keys(raw)
    if not isinstance(value, dict):
        raise FreshTransitionError("지원 입력/기록은 JSON 객체여야 합니다")
    return value


def _runtime_identity() -> tuple[str, str, str]:
    from tools.register_label_match_worker_pc import _current_machine_guid, _current_user_sid
    pc = os.environ.get("COMPUTERNAME", "").strip()
    if not pc:
        raise FreshTransitionError("현재 Windows PC 이름을 확인할 수 없습니다")
    return pc, _current_user_sid(), _current_machine_guid()


def _possession_descriptor() -> dict:
    from kmtech_zero_pe import PersistentPossessionKey, SCOPE_CURRENT_USER
    # A fresh *server* is not permission to create/repair the shared OS key.
    with PersistentPossessionKey.open_existing(scope=SCOPE_CURRENT_USER) as key:
        return key.descriptor().as_dict()


def load_transition_input(path: Path, app_root: Path) -> dict:
    value = _json(Path(path))
    fields = {"schema", "origin", "scope", "deployment_id", "packet", "pc", "sid", "acceptance"}
    if set(value) != fields or value["schema"] != INPUT_SCHEMA:
        raise FreshTransitionError("fresh 전환 입력 v1 필드가 올바르지 않습니다")
    for field in fields - {"acceptance"}:
        if not isinstance(value[field], str) or not value[field].strip() or any(ord(c) < 32 for c in value[field]):
            raise FreshTransitionError("fresh 전환 입력 문자열이 올바르지 않습니다")
    origin = urlsplit(value["origin"])
    try:
        port = origin.port
    except ValueError as exc:
        raise FreshTransitionError("새 HTTPS origin이 올바르지 않습니다") from exc
    if (origin.scheme != "https" or not origin.hostname or origin.username is not None
            or origin.password is not None or origin.path not in {"", "/"} or origin.query
            or origin.fragment or "\\" in value["origin"] or any(c.isspace() for c in value["origin"])
            or (port is not None and not 1 <= port <= 65535)):
        raise FreshTransitionError("새 HTTPS origin만 지정하세요")
    value["origin"] = value["origin"].rstrip("/")
    if (not value["deployment_id"].startswith("/") or ".." in value["deployment_id"].split("/")
            or not re.fullmatch(r"[0-9a-f]{64}", value["packet"])):
        raise FreshTransitionError("배포 root 또는 packet 결속이 올바르지 않습니다")
    pc, sid, _guid = _runtime_identity()
    if pc.casefold() != value["pc"].casefold() or sid != value["sid"]:
        raise FreshTransitionError("전환 입력의 PC/SID가 현재 사용자와 다릅니다")
    packet = Path(app_root) / "portable-manifest.json"
    assert_path_has_no_reparse_components(packet, label="packet")
    if _file_sha256(packet) != value["packet"]:
        raise FreshTransitionError("전환 입력과 실제 packet이 다릅니다")
    acceptance = value["acceptance"]
    if (not isinstance(acceptance, dict) or set(acceptance) != {
            "record_path", "record_sha256", "source_commit", "accepted_by"}
            or any(not isinstance(v, str) or not v.strip() for v in acceptance.values())
            or not re.fullmatch(r"[0-9a-f]{64}", acceptance["record_sha256"])
            or not re.fullmatch(r"[0-9a-f]{40}", acceptance["source_commit"])):
        raise FreshTransitionError("새 서버 수용 기록 결속이 올바르지 않습니다")
    record = Path(acceptance["record_path"])
    if not record.is_absolute():
        raise FreshTransitionError("서버 수용 기록은 로컬 절대경로여야 합니다")
    accepted = _json(record)
    if (_file_sha256(record) != acceptance["record_sha256"] or accepted.get("phase") != "ACCEPTED"
            or accepted.get("ready") is not True or accepted.get("client_writes_blocked") is not False
            or accepted.get("snapshot_rollback_allowed") is not False
            or accepted.get("source_commit") != acceptance["source_commit"]):
        raise FreshTransitionError("새 서버 수용 기록이 승인된 상태와 다릅니다")
    return value


def _security_descriptor(path: Path) -> str:
    if os.name != "nt":
        return ""
    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    advapi.GetNamedSecurityInfoW.argtypes = [wintypes.LPWSTR, wintypes.DWORD, wintypes.DWORD,
        ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]
    advapi.GetNamedSecurityInfoW.restype = wintypes.DWORD
    advapi.ConvertSecurityDescriptorToStringSecurityDescriptorW.argtypes = [
        ctypes.c_void_p, wintypes.DWORD, wintypes.DWORD, ctypes.POINTER(wintypes.LPWSTR),
        ctypes.POINTER(wintypes.DWORD)]
    advapi.ConvertSecurityDescriptorToStringSecurityDescriptorW.restype = wintypes.BOOL
    kernel.LocalFree.argtypes = [ctypes.c_void_p]
    kernel.LocalFree.restype = ctypes.c_void_p
    buffer = ctypes.c_void_p()
    error = advapi.GetNamedSecurityInfoW(str(path), 1, 7, None, None, None, None, ctypes.byref(buffer))
    if error:
        raise ctypes.WinError(error)
    text = wintypes.LPWSTR()
    try:
        if not advapi.ConvertSecurityDescriptorToStringSecurityDescriptorW(buffer, 1, 7, ctypes.byref(text), None):
            raise ctypes.WinError(ctypes.get_last_error())
        return text.value
    finally:
        if text:
            kernel.LocalFree(text)
        kernel.LocalFree(buffer)


def _set_security(path: Path, sddl: str, *, restore: bool = False) -> None:
    if os.name != "nt":
        path.chmod(0o700 if path.is_dir() else 0o600)
        return
    advapi = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    advapi.ConvertStringSecurityDescriptorToSecurityDescriptorW.argtypes = [
        wintypes.LPCWSTR, wintypes.DWORD, ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(wintypes.DWORD)]
    advapi.ConvertStringSecurityDescriptorToSecurityDescriptorW.restype = wintypes.BOOL
    advapi.SetFileSecurityW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, ctypes.c_void_p]
    advapi.SetFileSecurityW.restype = wintypes.BOOL
    kernel.LocalFree.argtypes = [ctypes.c_void_p]
    kernel.LocalFree.restype = ctypes.c_void_p
    descriptor = ctypes.c_void_p()
    if not advapi.ConvertStringSecurityDescriptorToSecurityDescriptorW(sddl, 1, ctypes.byref(descriptor), None):
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        flags = 4 | (0x20000000 if restore and "D:P" not in sddl else 0x80000000)
        if not advapi.SetFileSecurityW(str(path), flags, descriptor):
            raise ctypes.WinError(ctypes.get_last_error())
    finally:
        kernel.LocalFree(descriptor)


def _protect_directory(path: Path, sid: str) -> None:
    assert_path_has_no_reparse_components(path, label="보호 위치")
    path.mkdir(parents=True, exist_ok=True)
    _set_security(path, f"D:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;OICI;FA;;;{sid})")


def _protect_inactive(path: Path, sid: str) -> None:
    snapshot = _snapshot(path)
    _set_security(path, f"D:P(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)(A;OICI;FA;;;{sid})")
    if snapshot["directory"]:
        for name in snapshot["children"]:
            _protect_inactive(path / name, sid)


def _restore_metadata(path: Path, snapshot: dict) -> None:
    if snapshot["directory"]:
        for name, child in snapshot["children"].items():
            _restore_metadata(path / name, child)
    _set_security(path, snapshot["sddl"], restore=True)
    if os.name == "nt":
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.SetFileAttributesW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD]
        kernel.SetFileAttributesW.restype = wintypes.BOOL
        if not kernel.SetFileAttributesW(str(path), snapshot["attributes"]):
            raise ctypes.WinError(ctypes.get_last_error())
    else:
        path.chmod(snapshot["mode"])


def _snapshot(path: Path) -> dict:
    assert_path_has_no_reparse_components(path, label="보관 원본")
    info = path.lstat()
    if stat.S_ISLNK(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
        raise FreshTransitionError("재분석 경로는 지원 전환에서 보존 확인이 필요합니다")
    if not path.is_dir() and (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1):
        raise FreshTransitionError("일반 단일 파일이 아닌 보관 대상입니다")
    result = {"directory": path.is_dir(), "attributes": getattr(info, "st_file_attributes", 0),
              "mode": stat.S_IMODE(info.st_mode), "sddl": _security_descriptor(path)}
    if path.is_dir():
        result["children"] = {child.name: _snapshot(child) for child in sorted(path.iterdir())}
    else:
        result.update(size=info.st_size, sha256=_file_sha256(path))
    return result


def _byte_shape(snapshot: dict) -> dict:
    if snapshot["directory"]:
        return {name: _byte_shape(item) for name, item in snapshot["children"].items()}
    return {"size": snapshot["size"], "sha256": snapshot["sha256"]}


def _copy_tree(source: Path, destination: Path, snapshot: dict) -> None:
    if snapshot["directory"]:
        destination.mkdir(parents=True)
        for name, child in snapshot["children"].items():
            _copy_tree(source / name, destination / name, child)
    else:
        destination.parent.mkdir(parents=True, exist_ok=True)
        with source.open("rb") as src, destination.open("xb") as dest:
            shutil.copyfileobj(src, dest, 1024 * 1024)
            dest.flush()
            os.fsync(dest.fileno())


def _copy_verified(source: Path, destination: Path, snapshot: dict) -> None:
    if destination.exists():
        if _byte_shape(_snapshot(destination)) == _byte_shape(snapshot):
            return
        raise FreshTransitionError("보관 위치에 원본과 다른 자료가 있습니다; 덮어쓰지 않습니다")
    # An interrupted partial copy never occupies the verified destination. Keep
    # every abandoned attempt in the protected archive for support inspection.
    staging = destination.with_name(".c-" + uuid.uuid4().hex[:12])
    _copy_tree(source, staging, snapshot)
    if _byte_shape(_snapshot(staging)) != _byte_shape(snapshot):
        raise FreshTransitionError("보관 사본 바이트 대조가 실패했습니다")
    staging.rename(destination)


def _under(path: Path, root: Path) -> bool:
    return path == root or root in path.parents


def _state_roots(paths: CurrentUserOnboardingPaths, env: Mapping[str, str]) -> list[Path]:
    local = Path(env["LOCALAPPDATA"])
    machine = Path(env.get("PROGRAMDATA") or env.get("ProgramData") or r"C:\ProgramData")
    shared_profiles = {machine / "KMTech/Logistics/runtime-profile.json", local / "KMTech/Logistics/runtime-profile.json"}
    roots = [paths.data_root, paths.ledger_path.parent, paths.settings_path,
             paths.bootstrap_tls_ca_bundle_path,
             local / "KMTech/Label_Match/data", local / "KMTech/Label_Match/config/app_settings.json",
             local / "KMTech/Logistics/profiles/Label_Match",
             machine / "KMTech/Label_Match/data", machine / "KMTech/Label_Match/config/app_settings.json",
             machine / "KMTech/Logistics/profiles/Label_Match",
             machine / "KMTech/DirectSync/label_match",
             machine / "KMTech/DirectSync/label-match-margin-r2"]
    if paths.logistics_profile_path not in shared_profiles:
        roots.extend((paths.logistics_profile_path, paths.logistics_secret_path,
                      paths.logistics_profile_path.parent / "tls/ca-bundle.pem"))
    direct_roots = {paths.direct_sync_root, local / "KMTech/DirectSync/label_match"}
    for name in ("LABEL_MATCH_SAVE_DIR",):
        if env.get(name):
            roots.append(Path(env[name]))
    if paths.producer_manifest_path.is_file():
        manifest = _json(paths.producer_manifest_path, 1024 * 1024)
        if manifest.get("apps") not in (None, ["LabelMatch"]):
            raise FreshTransitionError("다른 앱의 manifest는 전환하지 않습니다")
        sections = (manifest.get("paths", {}), manifest.get("sync", {}),
                    manifest.get("sync", {}).get("queue", {}))
        for section, fields in zip(sections, (("data_dir", "evidence_dir", "rollback_dir"),
                                             ("sync_dir",), ("client_state_db", "queue_dir"))):
            for field in fields:
                if section.get(field):
                    selected = Path(section[field])
                    if not selected.is_absolute():
                        raise FreshTransitionError("옛 manifest의 소비 경로가 절대경로가 아닙니다")
                    if field == "data_dir":
                        direct_roots.add(selected)
                    else:
                        roots.append(selected)
    for direct in direct_roots:
        assert_path_has_no_reparse_components(direct, label="producer root")
        if direct.is_dir():
            for child in direct.iterdir():
                if child.name == "control":
                    # Live fencing and recovery journals are never moved.
                    roots.extend(p for p in child.iterdir() if p.name not in {
                        "writer-session", "label_match_user_relay.stop.json", JOURNAL_NAME})
                else:
                    roots.append(child)
    protected = [paths.app_root, local, machine, local / "KMTech", machine / "KMTech",
                 paths.control_dir, machine / "KMTech/Logistics",
                 local / "KMTech/Logistics", local / "KMTech/Label_Match/server-transition"]
    selected = []
    for root in roots:
        root = Path(os.path.abspath(root))
        assert_path_has_no_reparse_components(root, label="보관 root")
        if root == Path(root.anchor) or any(_under(item, root) for item in protected):
            raise FreshTransitionError("보관 대상이 코드/공유 상태/제어 root와 겹칩니다")
        if root.exists():
            selected.append(root)
    selected = sorted(set(selected), key=lambda p: (len(p.parts), str(p).casefold()))
    return [p for index, p in enumerate(selected) if not any(_under(p, q) for q in selected[:index])]


def _count_snapshot(root: Path) -> dict:
    result = {"pending": 0, "unknown": 0, "files": 0, "bytes": 0, "observations": []}
    files = [root] if root.is_file() else sorted(p for p in root.rglob("*") if p.is_file())
    for path in files:
        result["files"] += 1
        result["bytes"] += path.stat().st_size
        if path.suffix.lower() in {".db", ".sqlite", ".sqlite3"}:
            try:
                with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as conn:
                    conn.execute("PRAGMA query_only=ON")
                    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
                    for table in tables:
                        quoted = '"' + table.replace('"', '""') + '"'
                        columns = {r[1] for r in conn.execute(f"PRAGMA table_info({quoted})")}
                        status = next((c for c in ("status", "state") if c in columns), None)
                        if status:
                            for state, count in conn.execute(f'SELECT "{status}", COUNT(*) FROM {quoted} GROUP BY "{status}"'):
                                text = str(state or "UNKNOWN").upper()
                                if text in {"ACKED", "COMMITTED", "COMPLETED", "CANCELLED", "EXPIRED", "CLOSED"}:
                                    continue
                                bucket = "unknown" if text in {"UNKNOWN", "IN_FLIGHT", "CLAIMED"} else "pending"
                                result[bucket] += count
                                result["observations"].append({"file": path.name, "table": table,
                                                               "state": text, "count": count})
                        elif not table.startswith("sqlite_"):
                            count = conn.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()[0]
                            if count:
                                result["unknown"] += count
                                result["observations"].append({"file": path.name, "table": table,
                                                               "state": "ACK_NOT_PROVEN", "count": count})
            except sqlite3.Error:
                result["unknown"] += 1
                result["observations"].append({"file": path.name, "state": "UNREADABLE_DATABASE", "count": None})
        elif path.suffix.lower() == ".csv" or any(t in path.name.lower() for t in ("session", "current_set", "journal", "parked")):
            # A file's existence does not prove its source rows have an ACK.
            result["unknown"] += 1
            result["observations"].append({"file": path.name, "state": "ACK_NOT_PROVEN", "count": None})
    return result


def _require_authority() -> dict:
    from writer_session_fence import _delegation_matches
    active = active_fence()
    if active is None or not _delegation_matches(active, source="fresh_server_transition", environ=os.environ):
        raise FreshTransitionError("canonical 설치기의 현재 전환 권한이 필요합니다")
    return {name: active[name] for name in ("session_id", "attempt_id", "replacement_transaction_id")}


@contextmanager
def _transition_writer():
    _require_authority()
    with writer_admission("fresh_server_transition"):
        yield


def _powershell(script: str) -> Any:
    env = os.environ.copy()
    system = Path(env.get("SystemRoot", r"C:\Windows"))
    env["PSModulePath"] = str(system / "System32/WindowsPowerShell/v1.0/Modules") + ";" + str(
        Path(env.get("ProgramFiles", r"C:\Program Files")) / "WindowsPowerShell/Modules")
    result = subprocess.run([str(system / "System32/WindowsPowerShell/v1.0/powershell.exe"),
                             "-NoProfile", "-NonInteractive", "-Command", script],
                            env=env, capture_output=True, text=True, timeout=45,
                            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    if result.returncode != 0:
        raise FreshTransitionError("Windows 지원 상태 조회/변경이 실패했습니다; 상태는 UNKNOWN입니다")
    return json.loads(result.stdout) if result.stdout.strip() else None


def _capture_persistence() -> dict:
    import winreg
    from user_relay import USER_RELAY_RUN_KEY, USER_RELAY_RUN_VALUE
    run = {"exists": False}
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, USER_RELAY_RUN_KEY) as key:
            value, kind = winreg.QueryValueEx(key, USER_RELAY_RUN_VALUE)
            if kind not in (winreg.REG_SZ, winreg.REG_EXPAND_SZ) or not isinstance(value, str):
                raise FreshTransitionError("Run 값 형식을 확인하세요")
            run = {"exists": True, "kind": kind, "data": value}
    except FileNotFoundError:
        pass
    tasks = _powershell("$ErrorActionPreference='Stop'; @((Get-ScheduledTask | Where-Object { "
                        "$_.TaskName -in @('direct-sync-relay-label-match','direct-sync-relay-label-match-current-pc') }) | ForEach-Object { "
                        "@{name=$_.TaskName;path=$_.TaskPath;state=[string]$_.State;"
                        "xml=(Export-ScheduledTask -TaskName $_.TaskName -TaskPath $_.TaskPath)} }) | ConvertTo-Json -Depth 8 -Compress")
    return {"run": run, "tasks": [] if tasks is None else tasks if isinstance(tasks, list) else [tasks]}


def _disable_persistence(persistence: dict) -> None:
    from user_relay import _registry_delete
    _registry_delete()
    # SYSTEM tasks must already be retired/disabled by their existing owned-task
    # support path. Do not broaden task ownership to names alone.
    after = _capture_persistence()
    if after["run"]["exists"] or any(t["state"] != "Disabled" for t in after["tasks"]):
        raise FreshTransitionError("Run/task 비활성화 readback이 실패했습니다")


def _restore_persistence(persistence: dict, archive_root: Path) -> None:
    import winreg
    from user_relay import USER_RELAY_RUN_KEY, USER_RELAY_RUN_VALUE, _registry_delete
    if _capture_persistence()["tasks"] != persistence["tasks"]:
        # Preparation requires tasks to be absent/disabled and never rewrites
        # their definitions. A concurrent task edit is not ours to overwrite.
        raise FreshTransitionError("원 task 정의가 달라졌습니다; 보호된 원 정의로 관리자 지원 필요")
    run = persistence["run"]
    if run["exists"]:
        with winreg.CreateKeyEx(winreg.HKEY_CURRENT_USER, USER_RELAY_RUN_KEY, 0, winreg.KEY_SET_VALUE) as key:
            winreg.SetValueEx(key, USER_RELAY_RUN_VALUE, 0, run["kind"], run["data"])
    else:
        _registry_delete()
    if _capture_persistence() != persistence:
        raise FreshTransitionError("원 Run/task 바이트 복원이 확인되지 않았습니다")


def _require_quiescence(paths: CurrentUserOnboardingPaths, roots: list[Path]) -> dict:
    from current_user_scheduled_task import read_legacy_system_task_quiescence
    from label_match_single_instance import acquire_data_scope_mutex
    from user_relay import request_user_relay_stop
    _require_authority()
    tasks = read_legacy_system_task_quiescence()
    if tasks.get("status") != "PASS":
        raise FreshTransitionError("옛 SYSTEM task를 먼저 지원 절차로 정지하세요")
    stopped = request_user_relay_stop(paths.direct_sync_root)
    if stopped.get("status") != "ABSENT":
        raise FreshTransitionError("relay 정지 UNKNOWN; 다시 확인하세요")
    with ExitStack() as stack:
        for root in set(roots + [paths.data_root, paths.ledger_path.parent]):
            lease = stack.enter_context(acquire_data_scope_mutex(root))
            if not lease.owner:
                raise FreshTransitionError("Label_Match 작업 화면을 정상 종료한 뒤 다시 확인하세요")
        pattern = _owned_process_pattern(paths, roots).replace("'", "''")
        processes = _powershell("$ErrorActionPreference='Stop'; @((Get-CimInstance Win32_Process) | "
                               "Where-Object { $_.ProcessId -ne " + str(os.getpid()) + " -and "
                               "($_.Name -eq 'Label_Match.exe' -or ($_.Name -match '^pythonw?\\.exe$' -and "
                               "$_.CommandLine -match '" + pattern + "')) } | "
                               "Select-Object ProcessId) | ConvertTo-Json -Compress")
        if processes:
            raise FreshTransitionError("앱/relay/외부 writer 정지가 확인되지 않았습니다")
    return {"status": "QUIESCED", "relay": stopped, "legacy_tasks": tasks}


def _owned_process_pattern(paths, roots: list[Path]) -> str:
    # Other KMTech apps use the same relay runner filename. Its name alone is
    # not evidence of a Label_Match writer and must not stop their operation.
    owned = {paths.app_root, paths.direct_sync_root, paths.data_root, *roots}
    return r"(?i)Label[_-]?Match|" + "|".join(
        re.escape(str(root)) + r'(?=[\\/"\s]|$)' for root in sorted(owned))


def _candidate(paths: CurrentUserOnboardingPaths, target: dict, transition_id: str) -> dict:
    from tools.register_label_match_worker_pc import prepare_fresh_candidate
    return prepare_fresh_candidate(paths, target, transition_id)


def _server_preflight(paths: CurrentUserOnboardingPaths, target: dict, candidate: dict) -> dict:
    from tools.register_label_match_worker_pc import preflight_fresh_server
    return preflight_fresh_server(paths, target, candidate)


def _detach_entry(entry: dict) -> None:
    source, inactive = Path(entry["source"]), Path(entry["inactive"])
    if inactive.exists():
        if source.exists() or _byte_shape(_snapshot(inactive)) != _byte_shape(entry["snapshot"]):
            raise FreshTransitionError("분리 중단 원본이 달라졌습니다; 두 사본을 보존합니다")
        _protect_inactive(inactive, entry["sid"])
        return
    if _snapshot(source) != entry["snapshot"]:
        raise FreshTransitionError("정지 이후 원본이 달라졌습니다; 다시 확인하세요")
    source.rename(inactive)  # same parent/volume, never a cross-volume move
    if source.exists() or _snapshot(inactive) != entry["snapshot"]:
        raise FreshTransitionError("활성 경로 분리의 원본 대조가 실패했습니다")
    _protect_inactive(inactive, entry["sid"])


def _save(paths: CurrentUserOnboardingPaths, state: dict) -> None:
    _write_json_atomic(paths.control_dir / JOURNAL_NAME, state)
    _set_security(paths.control_dir / JOURNAL_NAME,
                  f"D:P(A;;FA;;;SY)(A;;FA;;;BA)(A;;FA;;;{state['target']['sid']})")
    _write_json_atomic(Path(state["archive_root"]) / "transition-result.json", state)


def transition_record_path(paths: CurrentUserOnboardingPaths, env: Mapping[str, str] | None = None) -> Path:
    values = os.environ if env is None else env
    local = str(values.get("LOCALAPPDATA") or "")
    pointer = Path(local) / "KMTech/Label_Match/server-transition/active-transition.json" if local else None
    if pointer is not None and pointer.exists():
        record = _json(pointer)
        if (set(record) != {"schema", "journal"} or record["schema"] != SCHEMA
                or not isinstance(record["journal"], str) or not Path(record["journal"]).is_absolute()):
            raise FreshTransitionError("전환 지원 기록의 위치가 올바르지 않습니다")
        return Path(record["journal"])
    return paths.control_dir / JOURNAL_NAME


def archived_path(state: dict, source: str) -> Path:
    selected = Path(source)
    for entry in state["entries"]:
        root = Path(entry["source"])
        if _under(selected, root):
            return Path(entry["archive"]) / selected.relative_to(root)
    raise FreshTransitionError("원본 경로가 보관 목록에 없습니다")


def prepare_fresh_server_registration(
    paths: CurrentUserOnboardingPaths, transition_input: Path, archive_volume: Path, *,
    environ: Mapping[str, str] | None = None, confirm: bool = False,
    confirm_unsent: bool = False, plan_only: bool = False, installed_app_root: Path | None = None,
    defer_machine: bool = False,
) -> dict:
    env = os.environ if environ is None else environ
    target = load_transition_input(transition_input, paths.app_root)
    _reject_machine_environment_anchors()
    descriptor = _possession_descriptor()
    if (descriptor.get("scope") != "current_user" or descriptor.get("machine_key") is not False
            or descriptor.get("created") is not False):
        raise FreshTransitionError("기존 current-user 공유 소유 키만 사용할 수 있습니다")
    journal = transition_record_path(paths, env)
    previous = _json(journal, 16 * 1024 * 1024) if journal.exists() else None
    if previous:
        if (previous.get("schema") != SCHEMA or previous.get("target") != target
                or previous.get("machine_guid") != _runtime_identity()[2]
                or previous.get("possession_key") != descriptor
                or previous.get("app_root") != str(installed_app_root or paths.app_root)):
            raise FreshTransitionError("다른 사용자/대상/packet/소유 키의 전환은 재개하지 않습니다")
        paths = _bound_paths(previous)
        if previous["phase"] == "RESTORED" and not plan_only:
            raise FreshTransitionError("복원 완료된 전환은 재사용하지 않습니다; 새 지원 전환 기록이 필요합니다")
        if previous["phase"] in {"DETACHED", "REGISTERING", "REGISTERED", "ACTIVATED"}:
            if previous["phase"] == "ACTIVATED":
                _completed_readback(previous)
            return previous
    # New work is recognized only beside the exactly recorded settings. Other
    # publications inside a new-work root could not be told apart from business.
    publication = Path(env["LOCALAPPDATA"]) / "KMTech/Logistics/profiles/Label_Match"
    if any(_under(publication, root) for root in (paths.data_root, paths.queue_dir, paths.spool_dir)):
        raise FreshTransitionError("새 서버 profile 게시 위치가 업무 폴더 안이라 새 업무와 구분할 수 없습니다; "
                                   "원본을 분리하기 전에 관리자 지원이 필요합니다")
    roots = _state_roots(paths, env) if previous is None else [Path(e["source"]) for e in previous["entries"]]
    volume = Path(os.path.abspath(archive_volume))
    assert_path_has_no_reparse_components(volume, label="보존 볼륨")
    if (str(volume).startswith("\\\\") or volume.drive.casefold() == "e:"
            or any(_under(volume, root) for root in roots)
            or _under(volume, paths.app_root) or _under(volume, paths.direct_sync_root)):
        raise FreshTransitionError("보관 위치는 로컬 보존 볼륨의 소비 root 밖이어야 합니다")
    if previous is None:
        transition_id = uuid.uuid4().hex
        archive = volume / "KMTech/server-transition/Label_Match" / (
            datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + transition_id)
        candidate = _candidate(paths, target, transition_id)
        state = {"schema": SCHEMA, "transition_id": transition_id, "phase": "PLANNED", "target": target,
                 "machine_guid": _runtime_identity()[2], "possession_key": descriptor,
                 "app_root": str(installed_app_root or paths.app_root), "archive_root": str(archive), "candidate": candidate,
                 "machine_root": str(Path(env.get("PROGRAMDATA") or r"C:\ProgramData")),
                 "old_uploads_permitted": False, "attempts": [], "entries": [], "captured_at": _now(),
                 "paths": {name: str(getattr(paths, name)) for name in paths.__dataclass_fields__}}
        state["paths"]["app_root"] = state["app_root"]
        state["paths"]["bootstrap_integrity_path"] = str(Path(state["app_root"]) / "bootstrap-integrity.json")
        if any(_under(archive, root) or _under(root, archive) for root in roots):
            raise FreshTransitionError("실제 보관 위치가 소비 root와 겹칩니다")
        # Counts are read only from copies, with WAL/SHM siblings kept together.
        with tempfile.TemporaryDirectory(prefix="label-transition-plan-") as temporary:
            copied = Path(temporary)
            _protect_directory(copied, target["sid"])
            for index, root in enumerate(roots):
                snapshot = _snapshot(root)
                copy = copied / str(index) / root.name
                _copy_verified(root, copy, snapshot)
                entry = {"source": str(root), "archive": str(archive / "items" / str(index)),
                         "inactive": str(root.with_name("." + root.name + ".fresh-" + transition_id)),
                         "snapshot": snapshot, "counts": _count_snapshot(copy), "detached": False, "sid": target["sid"]}
                entry["machine"] = _under(root, Path(state["machine_root"]))
                state["entries"].append(entry)
        state["pending"] = sum(e["counts"]["pending"] for e in state["entries"])
        state["unknown"] = sum(e["counts"]["unknown"] for e in state["entries"])
        state["bytes"] = sum(e["counts"]["bytes"] for e in state["entries"])
        existing_volume = volume
        while not existing_volume.exists():
            existing_volume = existing_volume.parent
        state["free_bytes"] = shutil.disk_usage(existing_volume).free
        if state["free_bytes"] < state["bytes"] + 1024 * 1024:
            raise FreshTransitionError("보존 볼륨의 여유 공간이 부족합니다")
        state["confirmation"] = (f"이 PC의 옛 자료를 보관하고 {target['origin']} / {target['scope']} / "
                                 f"{target['deployment_id']}에 새로 등록합니다")
        state["unsent_confirmation"] = UNSENT_CONFIRMATION
        if plan_only:
            return state
        if not confirm:
            raise FreshTransitionError("새 서버 대상과 옛 자료 보관을 명시 승인하세요")
        if (state["pending"] or state["unknown"]) and not confirm_unsent:
            raise FreshTransitionError("미전송/UNKNOWN 자료가 있습니다: " + UNSENT_CONFIRMATION)
        state["unsent_acknowledged"] = bool(confirm_unsent)
        state["persistence"] = _capture_persistence()
        stop = paths.control_dir / "label_match_user_relay.stop.json"
        state["stop_preimage"] = {"exists": stop.exists(), "snapshot": _snapshot(stop) if stop.exists() else None}
    else:
        state = previous
        if plan_only:
            return state
    authority = _require_authority()
    with EnrollmentMutex(), _transition_writer():
        _protect_directory(Path(state["archive_root"]), target["sid"])
        paths.control_dir.mkdir(parents=True, exist_ok=True)
        pointer_root = Path(env["LOCALAPPDATA"]) / "KMTech/Label_Match/server-transition"
        _protect_directory(pointer_root, target["sid"])
        _write_json_atomic(pointer_root / "active-transition.json", {"schema": SCHEMA, "journal": str(paths.control_dir / JOURNAL_NAME)})
        attempt = {"id": uuid.uuid4().hex, "authority": authority, "started_at": _now(), "status": "UNKNOWN"}
        state["attempts"].append(attempt)
        _save(paths, state)  # owner/candidate binding precedes every mutation
        try:
            if state["stop_preimage"]["exists"]:
                original_stop = paths.control_dir / "label_match_user_relay.stop.json"
                saved_stop = Path(state["archive_root"]) / "control-preimage/stop.json"
                if not saved_stop.exists():
                    _copy_verified(original_stop, saved_stop, state["stop_preimage"]["snapshot"])
            if "server_preflight" not in state:
                state["server_preflight"] = _server_preflight(paths, target, state["candidate"])
                _save(paths, state)
            _disable_persistence(state["persistence"])
            state["quiescence"] = _require_quiescence(paths, roots)
            if state["phase"] == "PLANNED":
                state["phase"] = "QUIESCED"
                _save(paths, state)
            if defer_machine and any(e.get("machine") and Path(e["source"]).exists() for e in state["entries"]):
                state["machine_required"] = True
                _save(paths, state)
                return state
            for entry in state["entries"]:
                source = Path(entry["inactive"] if Path(entry["inactive"]).exists() else entry["source"])
                if (_byte_shape(_snapshot(source)) != _byte_shape(entry["snapshot"])
                        or (source == Path(entry["source"]) and _snapshot(source) != entry["snapshot"])):
                    raise FreshTransitionError("보관 전 원본 상태가 달라졌습니다")
                _copy_verified(source, Path(entry["archive"]), entry["snapshot"])
            state["phase"] = "ARCHIVE_VERIFIED"
            _save(paths, state)
            for entry in state["entries"]:
                entry["detach_requested"] = True
                _save(paths, state)
                _detach_entry(entry)
                entry["detached"] = True
                _save(paths, state)
            if inspect_current_user_state(paths)["status"] != "ABSENT":
                raise FreshTransitionError("보관 후 활성 신원 ABSENT가 확인되지 않았습니다")
            state["phase"] = "DETACHED"
            state["identity_state"] = "ABSENT"
            attempt["status"] = "PASS"
            _save(paths, state)
        except Exception as exc:
            attempt["status"] = "UNKNOWN"
            attempt["error_type"] = type(exc).__name__
            _save(paths, state)
            raise
        return state


def _bound_paths(state: dict, *, new: bool = False) -> CurrentUserOnboardingPaths:
    paths = CurrentUserOnboardingPaths(**{k: Path(v) for k, v in state["paths"].items()})
    if new:
        # Never revive the previous split onboarding ledger or a shared profile.
        profile = Path(state["new_profile_path"])
        paths = replace(paths, ledger_path=paths.data_root / "package_logistics_outbox.sqlite3",
                        logistics_profile_path=profile,
                        logistics_secret_path=profile.parent / "secrets/bearer-token.dpapi")
    return paths


def _completed_readback(state: dict) -> None:
    from current_user_onboarding import _default_profile_loader
    _validate_local_owner(state)
    paths = _bound_paths(state, new=True)
    from logistics_runtime_profile import fresh_server_runtime_binding
    if fresh_server_runtime_binding() != {
            "LABEL_MATCH_SAVE_DIR": str(paths.data_root), "LABEL_MATCH_SETTINGS_PATH": str(paths.settings_path),
            "LABEL_MATCH_DIRECT_SYNC_ROOT": str(paths.direct_sync_root),
            "KM_LOGISTICS_PROFILE_PATH": str(paths.logistics_profile_path)}:
        raise FreshTransitionError("완료 전환의 소비 경로 binding이 다릅니다; 관리자 지원 필요")
    ready = inspect_current_user_state(paths)
    if (ready["status"] != "READY" or ready.get("manifest_hash") != state["candidate"]["manifest_hash"]
            or ready.get("producer_install_id") != state["candidate"]["producer_install_id"]
            or state["possession_key"] != _possession_descriptor()):
        raise FreshTransitionError("완료 전환의 로컬 신원 readback이 다릅니다; 관리자 지원 필요")
    profile = _default_profile_loader(paths.logistics_profile_path)
    if (profile.authority_scope != state["target"]["scope"]
            or profile.base_url.rstrip("/") != state["target"]["origin"]):
        raise FreshTransitionError("완료 전환의 서버 대상 readback이 다릅니다")


def _completion(state: dict) -> dict:
    """A completed record is final only once the writer fence that wrote it is released."""
    kind = {"ACTIVATED": "activation", "RESTORED": "restore"}.get(state["phase"])
    attempt = state["attempts"][-1] if state.get("attempts") else {}
    authority = attempt.get("authority") if kind and attempt.get("kind") == kind else None
    authority = {name: authority.get(name) for name in AUTHORITY_FIELDS} if isinstance(authority, dict) else None
    result = {"completion": "IN_PROGRESS", "next_action": "", "terminal_authority": authority}
    if kind is None:
        return result
    try:
        fence = active_fence()
    except WriterFenceError:
        fence = {}
    if fence is None:
        result["completion"] = "COMPLETE"
        if kind == "restore":
            result["next_action"] = "복원이 끝난 전환입니다. 다시 새 서버에 등록하려면 새 지원 전환 기록이 필요합니다(관리자 지원)"
    elif not fence or authority is None or any(fence.get(name) != authority[name] for name in AUTHORITY_FIELDS):
        result.update(completion="FENCE_BLOCKED", next_action=(
            "이 전환이 남긴 잠금이 아닌 쓰기 잠금이 있습니다. 풀지 않고 그대로 둡니다. "
            "그 설치 작업을 마무리하거나 관리자 지원을 요청하세요"))
    elif _named_mutex_held_by_other(fence["session_authority_mutex_name"]):
        result.update(completion="FENCE_RELEASE_PENDING",
                      next_action="전환 설치기가 아직 실행 중입니다. 끝날 때까지 기다린 뒤 Status로 다시 확인하세요")
    else:
        result.update(completion="FENCE_RELEASE_PENDING", next_action=(
            "이 전환의 쓰기 잠금이 남아 있습니다. 같은 packet과 입력으로 "
            + ("Restore를" if kind == "restore" else "Resume을") + " 실행해 마무리하세요"))
    return result


def _validate_local_owner(state: dict) -> None:
    pc, sid, guid = _runtime_identity()
    if (sid != state["target"]["sid"] or guid != state["machine_guid"]
            or pc.casefold() != state["target"]["pc"].casefold()
            or state["possession_key"] != _possession_descriptor()):
        raise FreshTransitionError("원 PC/사용자와 같은 소유 키에서만 전환을 계속할 수 있습니다")


def _atomic_bytes(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name("." + path.name + "." + uuid.uuid4().hex)
    with temporary.open("xb") as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


def _publish_runtime_binding(paths, state: dict, env: Mapping[str, str]) -> None:
    binding = Path(env["LOCALAPPDATA"]) / "KMTech/Label_Match/server-transition/runtime-binding.json"
    _protect_directory(binding.parent, state["target"]["sid"])
    value = {"schema": "label-match-fresh-runtime-binding-v1", "transition_id": state["transition_id"],
             "paths": {"LABEL_MATCH_SAVE_DIR": str(paths.data_root),
                       "LABEL_MATCH_SETTINGS_PATH": str(paths.settings_path),
                       "LABEL_MATCH_DIRECT_SYNC_ROOT": str(paths.direct_sync_root),
                       "KM_LOGISTICS_PROFILE_PATH": str(paths.logistics_profile_path)}}
    _write_json_atomic(binding, value)
    if _json(binding) != value:
        raise FreshTransitionError("새 앱 전용 소비 경로 게시가 확인되지 않았습니다")
    state["runtime_binding_path"] = str(binding)


def _assert_no_new_work(paths: CurrentUserOnboardingPaths, published: dict | None = None) -> None:
    from contextlib import closing
    settings = Path(published["path"]) if published else None
    for root in {paths.data_root, paths.queue_dir, paths.spool_dir}:
        if not root.exists():
            continue
        for file in root.rglob("*"):
            assert_path_has_no_reparse_components(file, label="새 업무 확인")
            if not file.is_file():
                continue
            if settings is not None and (file == settings or (
                    file.parent == settings.parent and file.name.startswith("." + settings.name + ".")
                    and file.name.endswith(".tmp"))) and _file_sha256(file) == published["sha256"]:
                # Exactly the settings bytes this transition published (or their unreplaced atomic copy).
                continue
            if file.suffix.lower() in {".db", ".sqlite3"}:
                with closing(sqlite3.connect(file.as_uri() + "?mode=ro", uri=True)) as connection:
                    tables = [r[0] for r in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")]
                    for table in tables:
                        if table.startswith("sqlite_") or table == "direct_sync_runtime_authority":
                            continue
                        if table == "package_outbox_schema_info":
                            if connection.execute("SELECT key FROM package_outbox_schema_info").fetchall() != [("schema_version",)]:
                                raise FreshTransitionError("새 업무 DB의 schema metadata가 다릅니다")
                            continue
                        quoted = '"' + table.replace('"', '""') + '"'
                        if connection.execute(f"SELECT COUNT(*) FROM {quoted}").fetchone()[0]:
                            raise FreshTransitionError("새 로컬 업무가 있습니다; 복원 대신 앞으로 복구하세요")
            elif file.name.endswith(("-wal", "-shm", "-journal")):
                continue
            else:
                raise FreshTransitionError("새 로컬 파일이 있습니다; 업무 0건을 먼저 확인하세요")


def _preserve_partial_publication(paths, state: dict) -> None:
    """Retain incomplete *new* credential publication; never rotate an old token."""
    archive = Path(state["archive_root"]) / ("partial-new-" + uuid.uuid4().hex[:16])
    candidates = [paths.identity_path, paths.producer_manifest_path, paths.credential_path,
                  paths.registration_receipt_path, paths.registration_report_path,
                  paths.logistics_profile_path.parent, paths.direct_sync_root / "secrets"]
    entries = []
    for index, source in enumerate(candidates):
        if not source.exists():
            continue
        snapshot = _snapshot(source)
        destination = archive / str(index)
        _copy_verified(source, destination, snapshot)
        inactive = source.with_name("." + source.name + ".fresh-partial-" + uuid.uuid4().hex)
        entry = {"source": str(source), "inactive": str(inactive), "snapshot": snapshot, "sid": state["target"]["sid"],
                 "archive": str(destination)}
        entries.append(entry)
        state.setdefault("partial_publications", []).append(entry)
        _save(paths, state)
        _detach_entry(entry)
    # Every partial artifact remains in the protected preservation tree and its
    # inactive original. Normal profile installation keeps token rotation off.


def _safe_ui_preferences(settings: dict) -> dict:
    """Carry explicit display scalars only; never paths, identities or sessions."""
    result = {}
    groups = {
        "ui_settings": ("base_font_size", "header_font_scale", "status_font_scale", "big_display_font_scale",
                        "treeview_row_height_scale", "column_padding"),
        "ui_persistence": ("scale_factor", "tree_font_size", "sash_position"),
    }
    for group, names in groups.items():
        old = settings.get(group, {})
        if isinstance(old, dict):
            result[group] = {name: old[name] for name in names if type(old.get(name)) in (int, float)
                             and math.isfinite(old[name]) and 0 <= old[name] <= 10000}
    face = settings.get("ui_settings", {}).get("default_font") if isinstance(settings.get("ui_settings"), dict) else None
    if isinstance(face, str) and re.fullmatch(r"[\w -]{1,80}", face):
        result.setdefault("ui_settings", {})["default_font"] = face
    old_colors = settings.get("colors", {})
    if isinstance(old_colors, dict):
        names = ("background", "card_background", "border", "text", "text_subtle", "text_strong", "primary",
                 "primary_active", "success", "success_light", "danger", "danger_light", "heading_background")
        result["colors"] = {name: old_colors[name] for name in names
                            if isinstance(old_colors.get(name), str) and re.fullmatch(r"#[0-9a-fA-F]{6}", old_colors[name])}
    return {name: values for name, values in result.items() if values}


def register_fresh_server(paths: CurrentUserOnboardingPaths, *, environ: Mapping[str, str] | None = None) -> dict:
    from tools import register_label_match_worker_pc as registration
    from current_user_onboarding import _configured_tls_ca_bundle_source
    env = os.environ if environ is None else environ
    state = _json(transition_record_path(paths, env), 16 * 1024 * 1024)
    if state["phase"] == "ACTIVATED":
        _completed_readback(state)
        return state
    if state["phase"] not in {"DETACHED", "REGISTERING", "REGISTERED"}:
        raise FreshTransitionError("원본 보관/활성 신원 분리를 먼저 완료하세요")
    _validate_local_owner(state)
    _require_authority()
    state.setdefault("new_profile_path", str(Path(env["LOCALAPPDATA"]) / "KMTech/Logistics/profiles/Label_Match/runtime-profile.json"))
    paths = _bound_paths(state, new=True)
    with EnrollmentMutex(), _transition_writer():
        if state["phase"] == "REGISTERED":
            return state
        _assert_no_new_work(paths, state.get("published_settings"))
        state["phase"] = "REGISTERING"
        attempt = {"id": uuid.uuid4().hex, "kind": "registration", "started_at": _now(), "status": "UNKNOWN"}
        state["attempts"].append(attempt)
        _save(paths, state)
        try:
            tls = _configured_tls_ca_bundle_source(paths, env)
            if not tls and any(_under(paths.bootstrap_tls_ca_bundle_path, Path(e["source"])) for e in state["entries"]):
                tls = str(archived_path(state, str(paths.bootstrap_tls_ca_bundle_path)))
            response_path = Path(state["archive_root"]) / "new-response.dpapi"
            if response_path.exists():
                response = registration._load_json_no_duplicate_keys(
                    registration._dpapi_unprotect_current_user(response_path.read_bytes()).encode("utf-8"))
                registration._validate_fresh_response(response, state["candidate"], state["target"], state["possession_key"]["fingerprint"])
            else:
                response = registration.enroll_fresh_candidate(paths, state, tls_ca_bundle_path=tls)
                protected = registration._dpapi_protect_current_user(json.dumps(response, ensure_ascii=True))
                _atomic_bytes(response_path, protected)
                if registration._load_json_no_duplicate_keys(
                        registration._dpapi_unprotect_current_user(response_path.read_bytes()).encode("utf-8")) != response:
                    raise FreshTransitionError("새 등록 응답 보호 저장이 확인되지 않았습니다")
            _preserve_partial_publication(paths, state)
            report = registration.publish_fresh_registration(paths, state, response, tls_ca_bundle_path=tls)
            readback = inspect_current_user_state(paths)
            if readback["status"] != "READY":
                raise FreshTransitionError("새 신원/manifest/profile/credential readback이 완료되지 않았습니다")
            # Only path/identity-free UI preferences may cross the server boundary.
            settings = {"custom_save_path": str(paths.data_root)}
            try:
                old_settings = _json(archived_path(state, str(paths.settings_path)))
            except (FreshTransitionError, OSError, ValueError):
                old_settings = {}
            settings.update(_safe_ui_preferences(old_settings))
            # Record the exact bytes before they exist, so a supported settings
            # path inside the data root is never counted as new local work.
            raw = (json.dumps(settings, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
            state["published_settings"] = {"path": str(paths.settings_path), "sha256": hashlib.sha256(raw).hexdigest()}
            _save(paths, state)
            _write_json_atomic(paths.settings_path, settings)
            if _file_sha256(paths.settings_path) != state["published_settings"]["sha256"]:
                raise FreshTransitionError("새 설정 게시 bytes가 기록과 다릅니다")
            _publish_runtime_binding(paths, state, env)
            state.update(phase="REGISTERED", registration={"credential_epoch": report["credential_epoch"],
                         "action": report["registration_action"], "manifest_hash": report["manifest_hash"],
                         "scope": state["target"]["scope"]}, identity_state="READY")
            attempt["status"] = "PASS"
            _save(paths, state)
        except Exception as exc:
            attempt["status"] = "UNKNOWN"
            attempt["error_type"] = type(exc).__name__
            _save(paths, state)
            raise
    return state


def _first_lease(paths, state: dict) -> dict:
    from direct_sync_runtime import load_credentials_from_json
    from producer_runtime_client import ensure_runtime_authority
    from tools.register_label_match_worker_pc import _fresh_session
    from current_user_onboarding import _default_profile_loader
    profile = _default_profile_loader(paths.logistics_profile_path)
    with _fresh_session(profile.tls_ca_bundle_path) as session:
        result = ensure_runtime_authority(
            db_path=paths.queue_dir / "direct_sync_relay.sqlite3",
            credentials=load_credentials_from_json(paths.credential_path),
            producer_install_id=state["candidate"]["producer_install_id"], session=session,
            tls_ca_bundle_path=profile.tls_ca_bundle_path)
    receipt = result.receipt
    if (result.error_code or result.retryable or result.operator_review or receipt.get("status") != "ACTIVE"
            or receipt.get("server_grant_accepted") is not True
            or receipt.get("producer_install_id") != state["candidate"]["producer_install_id"]):
        raise FreshTransitionError("새 서버 첫 lease가 확인되지 않았습니다; 이어서 등록으로 재확인하세요")
    return receipt


def activate_fresh_server(paths, *, environ: Mapping[str, str] | None = None) -> dict:
    from current_user_onboarding import onboard_current_user
    from current_user_scheduled_task import remove_current_user_scheduled_task
    from user_relay import (install_user_relay_autostart, release_user_relay_stop_marker,
                            start_user_relay_process, user_relay_stop_path)
    env = os.environ if environ is None else environ
    state = _json(transition_record_path(paths, env), 16 * 1024 * 1024)
    if state["phase"] == "ACTIVATED":
        _completed_readback(state)
        return state
    if state["phase"] != "REGISTERED":
        raise FreshTransitionError("새 등록 readback을 먼저 완료하세요")
    _validate_local_owner(state)
    _require_authority()
    paths = _bound_paths(state, new=True)
    with _transition_writer(), _activation_attempt(paths, state):
        _assert_no_new_work(paths, state.get("published_settings"))
        state["quiescence"] = _require_quiescence(paths, [])
        _save(paths, state)
        onboard_current_user(paths.app_root, environ=dict(env), server_base_url=state["target"]["origin"],
                             fresh_transition_id=state["transition_id"], defer_activation=True)
        state["first_lease"] = _first_lease(paths, state)
        _save(paths, state)
        task = remove_current_user_scheduled_task(paths.app_root)
        if task.get("status") != "ABSENT":
            raise FreshTransitionError("옛 task 부재가 확인되지 않았습니다")
        stop = state["quiescence"]["relay"]
        if user_relay_stop_path(paths.direct_sync_root).exists():
            release_user_relay_stop_marker(paths.direct_sync_root,
                                          expected_request_id=stop["request_id"],
                                          expected_sha256=stop["stop_request_sha256"])
        started = start_user_relay_process(paths.app_root)
        if started.get("status") != "ALIVE":
            raise FreshTransitionError("새 relay 기동 UNKNOWN; 자동시작을 활성화하지 않습니다")
        state["relay"] = started
        state["run"] = install_user_relay_autostart(paths.app_root)
        if state["run"].get("status") != "PASS":
            raise FreshTransitionError("새 canonical Run readback 실패")
        if _possession_descriptor() != state["possession_key"]:
            raise FreshTransitionError("전환 뒤 공유 KSP가 달라졌습니다")
        state.update(phase="ACTIVATED", completed_at=_now())
        _save(paths, state)
    return state


@contextmanager
def _activation_attempt(paths, state):
    attempt = {"id": uuid.uuid4().hex, "kind": "activation", "authority": _require_authority(),
               "started_at": _now(), "status": "UNKNOWN"}
    state["attempts"].append(attempt)
    _save(paths, state)
    try:
        yield
        attempt["status"] = "PASS"
    except Exception as exc:
        attempt["error_type"] = type(exc).__name__
        from user_relay import remove_user_relay_autostart, request_user_relay_stop
        try:
            attempt["activation_cleanup"] = {"run": remove_user_relay_autostart(),
                "relay": request_user_relay_stop(paths.direct_sync_root)}
        except Exception as cleanup_error:
            attempt["activation_cleanup"] = {"status": "UNKNOWN", "error_type": type(cleanup_error).__name__}
        raise
    finally:
        _save(paths, state)


def restore_fresh_server(paths, *, confirm_old_server_ready: bool = False) -> dict:
    state = _json(transition_record_path(paths), 16 * 1024 * 1024)
    if state["phase"] == "RESTORED":
        return state
    if state["phase"] in {"REGISTERING", "REGISTERED", "ACTIVATED"}:
        raise FreshTransitionError("새 서버 등록이 시작됐을 수 있어 되돌릴 수 없음 — 관리자 지원 필요. 보관 위치: " + state["archive_root"])
    if not confirm_old_server_ready:
        raise FreshTransitionError("원 서버/client 쌍의 복귀 가능 여부를 먼저 확인하세요")
    _require_authority()
    _validate_local_owner(state)
    paths = _bound_paths(state)
    with EnrollmentMutex(), _transition_writer():
        state["quiescence"] = _require_quiescence(paths, [])
        _save(paths, state)
        for entry in state["entries"]:
            source, inactive = Path(entry["source"]), Path(entry["inactive"])
            if source.exists():
                if inactive.exists():
                    raise FreshTransitionError("새 활성 자료가 있습니다; 덮어쓰지 않습니다")
                if entry.get("restore_requested") and _byte_shape(_snapshot(source)) == _byte_shape(entry["snapshot"]):
                    _restore_metadata(source, entry["snapshot"])
                if _snapshot(source) != entry["snapshot"]:
                    raise FreshTransitionError("새 활성 자료가 있습니다; 덮어쓰지 않습니다")
            else:
                if _byte_shape(_snapshot(Path(entry["archive"]))) != _byte_shape(entry["snapshot"]):
                    raise FreshTransitionError("원본 보관 사본 대조가 실패했습니다")
                if _byte_shape(_snapshot(inactive)) != _byte_shape(entry["snapshot"]):
                    raise FreshTransitionError("원본 bytes/ACL/속성이 달라졌습니다")
                entry["restore_requested"] = True
                _save(paths, state)
                inactive.rename(source)
                _restore_metadata(source, entry["snapshot"])
                if _snapshot(source) != entry["snapshot"]:
                    raise FreshTransitionError("복원 bytes/ACL/속성 readback 실패")
            entry["restored"] = True
            _save(paths, state)
        stop = paths.control_dir / "label_match_user_relay.stop.json"
        current_stop = _json(stop) if stop.exists() else {}
        expected_stop = state.get("quiescence", {}).get("relay", {})
        if stop.exists() and current_stop.get("request_id") != expected_stop.get("request_id"):
            raise FreshTransitionError("다른 요청의 stop marker는 복원으로 제거하지 않습니다")
        if state["stop_preimage"]["exists"]:
            saved_stop = Path(state["archive_root"]) / "control-preimage/stop.json"
            if not saved_stop.exists():
                raise FreshTransitionError("원 stop marker 보관이 없습니다; 관리자 지원 필요")
            _atomic_bytes(stop, saved_stop.read_bytes())
            _restore_metadata(stop, state["stop_preimage"]["snapshot"])
            if _snapshot(stop) != state["stop_preimage"]["snapshot"]:
                raise FreshTransitionError("원 stop marker 복원 readback 실패")
        elif stop.exists():
            stop_snapshot = _snapshot(stop)
            saved_stop = Path(state["archive_root"]) / "restored-new-stop.json"
            _copy_verified(stop, saved_stop, stop_snapshot)
            stop.rename(stop.with_name(".restored-" + state["transition_id"] + ".stop.json"))
        _restore_persistence(state["persistence"], Path(state["archive_root"]))
        if state["attempts"][-1].get("kind") == "restore":
            state["attempts"][-1]["status"] = "PASS"
        state.update(phase="RESTORED", restored_at=_now())
        _save(paths, state)
    return state


def prepare_restore(paths, *, confirm_old_server_ready: bool = False) -> dict:
    state = _json(transition_record_path(paths), 16 * 1024 * 1024)
    if state["phase"] in {"REGISTERING", "REGISTERED", "ACTIVATED"} or not confirm_old_server_ready:
        raise FreshTransitionError("REGISTERING 전 원 server/client 복귀 확인이 있어야 복원할 수 있습니다")
    _validate_local_owner(state)
    authority = _require_authority()
    paths = _bound_paths(state)
    with EnrollmentMutex(), _transition_writer():
        state["quiescence"] = _require_quiescence(paths, [])
        state["attempts"].append({"id": uuid.uuid4().hex, "kind": "restore", "authority": authority,
                                  "started_at": _now(), "status": "UNKNOWN"})
        _save(paths, state)
    return state


def _reject_machine_environment_anchors() -> None:
    from logistics_runtime_profile import _machine_environment_value, PROFILE_PATH_ENV, REQUIRED_ENV
    if any(_machine_environment_value(name) for name in (
            PROFILE_PATH_ENV, REQUIRED_ENV, "LABEL_MATCH_SAVE_DIR", "LABEL_MATCH_SETTINGS_PATH",
            "LABEL_MATCH_DIRECT_SYNC_ROOT", "KMTECH_DIRECT_SYNC_ROOT")):
        raise FreshTransitionError("HKLM 환경 앵커가 있습니다; 보관 전 관리자 지원 필요 (값 비공개)")


def transition_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="옛 자료 보관 / 새 서버 등록 지원")
    parser.add_argument("--app-root", required=True)
    parser.add_argument("--installed-app-root", default="")
    parser.add_argument("--transition-input", required=True)
    parser.add_argument("--archive-volume", required=True)
    parser.add_argument("--action", choices=("plan", "prepare", "register", "activate", "status", "restore", "restore-prepare"), default="plan")
    parser.add_argument("--confirm", action="store_true")
    parser.add_argument("--confirm-unsent", action="store_true")
    parser.add_argument("--confirm-old-server-ready", action="store_true")
    parser.add_argument("--defer-machine", action="store_true")
    parser.add_argument("--result-path", default="")
    args = parser.parse_args(argv)
    paths = resolve_current_user_onboarding_paths(args.app_root)
    try:
        if args.action in {"plan", "prepare"}:
            result = prepare_fresh_server_registration(
                paths, Path(args.transition_input), Path(args.archive_volume),
                confirm=args.confirm, confirm_unsent=args.confirm_unsent, plan_only=args.action == "plan",
                defer_machine=args.defer_machine,
                installed_app_root=Path(args.installed_app_root) if args.installed_app_root else None)
        else:
            target = load_transition_input(Path(args.transition_input), paths.app_root)
            state = _json(transition_record_path(paths), 16 * 1024 * 1024)
            if state["target"] != target:
                raise FreshTransitionError("저장된 전환과 지원 입력이 다릅니다")
            if args.action == "status":
                if state["phase"] == "ACTIVATED":
                    _completed_readback(state)
                result = state
            elif args.action == "register":
                result = register_fresh_server(paths)
            elif args.action == "activate":
                result = activate_fresh_server(paths)
            elif args.action == "restore-prepare":
                result = prepare_restore(paths, confirm_old_server_ready=args.confirm_old_server_ready)
            else:
                result = restore_fresh_server(paths, confirm_old_server_ready=args.confirm_old_server_ready)
        summary = {name: result.get(name) for name in (
            "schema", "transition_id", "phase", "archive_root", "pending", "unknown", "bytes", "confirmation", "unsent_confirmation")}
        summary["paths"] = [{"source": e["source"], "archive": e["archive"]} for e in result["entries"]]
        summary["target"] = {name: result["target"][name] for name in ("pc", "sid", "origin", "scope", "deployment_id", "packet")}
        summary["journal"] = str(_bound_paths(result).control_dir / JOURNAL_NAME)
        summary["machine_required"] = any(e.get("machine") for e in result["entries"])
        summary["runtime_roots"] = [str(getattr(_bound_paths(result), name)) for name in (
            "app_root", "data_root", "direct_sync_root")]
        summary["last_attempt"] = {key: result.get("attempts", [{}])[-1].get(key) for key in (
            "kind", "status", "error_type")} if result.get("attempts") else None
        summary["first_lease_status"] = result.get("first_lease", {}).get("status")
        summary["restore_possible"] = result["phase"] not in {"REGISTERING", "REGISTERED", "ACTIVATED"}
        summary["restore_reason"] = ("원 server/client 복귀 확인 필요" if summary["restore_possible"] else
                                     "새 서버 등록이 시작됐을 수 있어 되돌릴 수 없음 — 관리자 지원 필요")
        summary.update(_completion(result))
        if args.result_path:
            _write_json_atomic(Path(args.result_path), summary)
        print(json.dumps(summary, ensure_ascii=True))
        return 0
    except Exception as exc:
        # HTTP/DPAPI exception strings can contain credentials. Only our own
        # bounded operator reasons and exception class names reach public output.
        print("fresh_transition_status=UNKNOWN")
        print(str(exc) if isinstance(exc, FreshTransitionError) else "fresh_transition_error=" + type(exc).__name__)
        return 4

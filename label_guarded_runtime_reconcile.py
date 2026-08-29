"""Guarded, product-owned repair for one exact-clone runtime conflict.

This is deliberately narrower than normal runtime onboarding.  It accepts a
previously captured exact-clone preimage, proves that the same two authorities
and stop fence still exist, retires the stale local authority with a single
compare-and-swap transaction, and asks the normal authenticated runtime client
to obtain a fresh server fence for the already-quarantined current identity.

The server transition is forward-only.  Before a grant is issued the client
database can be restored from its verified SQLite backup.  After a higher
server fence exists, restoring the old client authority would violate the
protocol; recovery must continue forward under a newer fence instead.
"""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import ipaddress
import json
import math
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import uuid
from typing import Any, Callable, Mapping
from urllib.parse import urlparse, urlunparse

from direct_sync_runtime import load_credentials_from_json
from label_exact_clone_resolution import (
    CONFLICT_CODE,
    ExactCloneResolutionError,
    capture_conflict_preimage,
    client_authorities,
    create_resolution_receipt,
    file_sha256,
    relay_batches_digest,
    sqlite_logical_digest,
    sqlite_logical_digest_on_connection,
)
from producer_runtime_client import RuntimePreparation, ensure_runtime_authority


RECONCILE_REPORT_SCHEMA = "label-match-guarded-runtime-reconcile-v1"
LIVE_CLIENT_PROOF_SCHEMA = "label-match-old-fence-liveness-proof-v1"
SERVER_INITIALIZER_PROOF_SCHEMA = "label-match-server-initializer-noop-proof-v1"
RECONCILE_EXIT_CODE = 4
_SERVER_SOURCE_FILES = frozenset(
    {
        "producer_ingest.py",
        "producer_self_enrollment.py",
        "producer_runtime_lease.py",
        "producer_authz_provisioning.py",
    }
)
_SERVER_PROCESS_SOURCE_FILES = _SERVER_SOURCE_FILES | {
    "app.py",
    "deployment_identity.py",
}


class GuardedRuntimeReconcileError(RuntimeError):
    """The exact guarded cutover could not be completed safely."""

    def __init__(self, message: str, *, server_forward: bool = False) -> None:
        super().__init__(message)
        self.server_forward = bool(server_forward)


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def _same_path(left: str | os.PathLike[str], right: str | os.PathLike[str]) -> bool:
    return os.path.normcase(str(Path(left).resolve(strict=False))) == os.path.normcase(
        str(Path(right).resolve(strict=False))
    )


def _canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            dict(value),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _write_new_json(path: str | os.PathLike[str], value: Mapping[str, Any]) -> None:
    selected = Path(path).expanduser().resolve(strict=False)
    selected.parent.mkdir(parents=True, exist_ok=True)
    payload = _canonical_json_bytes(value)
    temporary = selected.with_name(f".{selected.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, selected)
    finally:
        temporary.unlink(missing_ok=True)


def _preflight_new_output(path: Path) -> None:
    if path.exists():
        raise GuardedRuntimeReconcileError(f"output already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    token = uuid.uuid4().hex
    probe = path.with_name(f".{path.name}.{token}.probe")
    linked = path.with_name(f".{path.name}.{token}.linked")
    try:
        with probe.open("xb") as handle:
            handle.write(b"output-preflight\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.link(probe, linked)
    finally:
        linked.unlink(missing_ok=True)
        probe.unlink(missing_ok=True)


def _read_json_once(
    path: str | os.PathLike[str], *, label: str
) -> tuple[dict[str, Any], str]:
    selected = Path(path).expanduser().resolve(strict=True)
    size = selected.stat().st_size
    if size <= 0 or size > 1024 * 1024:
        raise GuardedRuntimeReconcileError(f"{label} size is invalid")
    raw = selected.read_bytes()
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise GuardedRuntimeReconcileError(f"{label} is invalid UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise GuardedRuntimeReconcileError(f"{label} must be a JSON object")
    return value, hashlib.sha256(raw).hexdigest()


def _required_preimage(
    path: str | os.PathLike[str],
) -> tuple[dict[str, Any], str]:
    value, digest = _read_json_once(path, label="exact-clone conflict preimage")
    if (
        value.get("schema_version")
        != "label-match-exact-clone-conflict-preimage-v1"
        or value.get("status") != "CONFLICT_CONFIRMED"
        or value.get("conflict_code") != CONFLICT_CODE
        or value.get("secret_material_included") is not False
    ):
        raise GuardedRuntimeReconcileError("conflict preimage contract differs")
    return value, digest


def _capture_comparable(value: Mapping[str, Any]) -> dict[str, Any]:
    client = dict(value.get("client") or {})
    server = dict(value.get("server") or {})
    return {
        "producer_install_id": value.get("producer_install_id"),
        "current_credential": value.get("current_credential"),
        "identity_input": value.get("identity_input"),
        "credential_input": value.get("credential_input"),
        "client": {
            "database_path": client.get("database_path"),
            "database_logical_digest": client.get("database_logical_digest"),
            "relay_batches": client.get("relay_batches"),
            "candidate_authority": client.get("candidate_authority"),
            "prior_authority": client.get("prior_authority"),
            "authority_count": client.get("authority_count"),
        },
        "server": {
            "database_path": server.get("database_path"),
            "quarantine": server.get("quarantine"),
            "prior_active_lease": server.get("prior_active_lease"),
        },
        "stop_marker": value.get("stop_marker"),
        "portable": value.get("portable"),
    }


def verify_conflict_preimage_still_current(
    *,
    preimage: Mapping[str, Any],
    client_db_path: str | os.PathLike[str],
    server_db_path: str | os.PathLike[str],
    identity_path: str | os.PathLike[str],
    credential_path: str | os.PathLike[str],
    stop_marker_path: str | os.PathLike[str],
    portable_root: str | os.PathLike[str],
) -> dict[str, Any]:
    try:
        current = capture_conflict_preimage(
            client_db_path=client_db_path,
            server_db_path=server_db_path,
            identity_path=identity_path,
            credential_path=credential_path,
            stop_marker_path=stop_marker_path,
            portable_root=portable_root,
        )
    except ExactCloneResolutionError as exc:
        raise GuardedRuntimeReconcileError(
            f"current conflict topology is not exact: {exc}"
        ) from exc
    if _capture_comparable(current) != _capture_comparable(preimage):
        raise GuardedRuntimeReconcileError(
            "current conflict topology differs from the authorized preimage"
        )
    return current


def verify_client_backup(
    *,
    backup_path: str | os.PathLike[str],
    client_db_path: str | os.PathLike[str],
    preimage: Mapping[str, Any],
    expected_sha256: str,
    require_live_match: bool = True,
) -> dict[str, Any]:
    backup = Path(backup_path).expanduser().resolve(strict=False)
    if backup.drive.casefold() != "e:" or not backup.is_file():
        raise GuardedRuntimeReconcileError(
            "client backup must be an existing E: SQLite file"
        )
    live = Path(client_db_path).expanduser().resolve(strict=True)
    if _same_path(backup, live) or os.path.samefile(backup, live):
        raise GuardedRuntimeReconcileError("client backup must differ from the live DB")
    expected_hash = str(expected_sha256 or "").strip().lower()
    if (
        len(expected_hash) != 64
        or any(character not in "0123456789abcdef" for character in expected_hash)
        or file_sha256(backup) != expected_hash
    ):
        raise GuardedRuntimeReconcileError("client backup SHA-256 pin differs")
    connection = sqlite3.connect(
        f"file:{backup.as_posix()}?mode=ro", uri=True, timeout=30
    )
    try:
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    finally:
        connection.close()
    if integrity != "ok":
        raise GuardedRuntimeReconcileError("client backup integrity_check failed")
    expected_client = dict(preimage.get("client") or {})
    backup_logical = sqlite_logical_digest(backup)
    live_logical = sqlite_logical_digest(live) if require_live_match else None
    if (
        (require_live_match and client_authorities(backup) != client_authorities(live))
        or client_authorities(backup)
        != sorted(
            [
                dict(expected_client.get("candidate_authority") or {}),
                dict(expected_client.get("prior_authority") or {}),
            ],
            key=lambda row: str(row.get("authority_scope") or ""),
        )
        or relay_batches_digest(backup) != expected_client.get("relay_batches")
        or backup_logical != expected_client.get("database_logical_digest")
        or (
            require_live_match
            and live_logical != expected_client.get("database_logical_digest")
        )
    ):
        raise GuardedRuntimeReconcileError(
            "client backup does not reproduce the authorized client preimage"
        )
    return {
        "status": "PASS",
        "path": str(backup),
        "sha256": file_sha256(backup),
        "logical_digest": backup_logical,
        "integrity_check": integrity,
    }


def validate_live_client_proof(
    proof: Mapping[str, Any],
    *,
    preimage_sha256: str,
    stop_marker_sha256: str,
    server_db_path: str | os.PathLike[str],
    client_db_path: str | os.PathLike[str],
    producer_install_id: str,
    old_lease_id: str,
    old_runtime_instance_id: str,
    old_fence: int,
    producer_id: str,
    key_id: str,
    now: datetime | None = None,
) -> dict[str, Any]:
    expected_keys = {
        "schema_version",
        "query_version",
        "snapshot_isolation",
        "status",
        "preimage_sha256",
        "server_db_path",
        "client_db_path",
        "producer_install_id",
        "old_lease_id",
        "old_runtime_instance_id",
        "old_fence",
        "producer_id",
        "key_id",
        "observed_from",
        "observed_until",
        "observation_seconds",
        "old_fence_logically_expired",
        "server_target_state_unchanged",
        "server_request_activity_absent",
        "client_target_state_unchanged",
        "old_fence_nonterminal_rows",
        "target_process_count",
        "target_runnable_launcher_count",
        "stop_marker_unchanged",
        "evidence",
    }


    if set(proof) != expected_keys:
        raise GuardedRuntimeReconcileError("live-client proof fields differ")
    expected_hash = str(preimage_sha256 or "").lower()
    if (
        proof.get("schema_version") != LIVE_CLIENT_PROOF_SCHEMA
        or proof.get("query_version") != "label-match-old-fence-liveness-query-v3"
        or proof.get("snapshot_isolation") != "sqlite-explicit-read-transaction"
        or proof.get("status") != "PASS"
        or proof.get("preimage_sha256") != expected_hash
        or not _same_path(proof.get("server_db_path", ""), server_db_path)
        or not _same_path(proof.get("client_db_path", ""), client_db_path)
        or proof.get("producer_install_id") != producer_install_id
        or proof.get("old_lease_id") != old_lease_id
        or proof.get("old_runtime_instance_id") != old_runtime_instance_id
        or proof.get("old_fence") != old_fence
        or proof.get("producer_id") != producer_id
        or proof.get("key_id") != key_id
        or isinstance(proof.get("observation_seconds"), bool)
        or not isinstance(proof.get("observation_seconds"), (int, float))
        or not math.isfinite(float(proof.get("observation_seconds") or 0))
        or float(proof["observation_seconds"]) < 300
        or proof.get("old_fence_logically_expired") is not True
        or proof.get("server_target_state_unchanged") is not True
        or proof.get("server_request_activity_absent") is not True
        or proof.get("client_target_state_unchanged") is not True
        or proof.get("old_fence_nonterminal_rows") != 0
        or proof.get("target_process_count") != 0
        or proof.get("target_runnable_launcher_count") != 0
        or proof.get("stop_marker_unchanged") is not True
        or not isinstance(proof.get("evidence"), Mapping)
    ):
        raise GuardedRuntimeReconcileError(
            "a live legitimate client may still be using the old fence"
        )
    try:
        observed_from = datetime.fromisoformat(
            str(proof.get("observed_from") or "").replace("Z", "+00:00")
        )
        observed_until = datetime.fromisoformat(
            str(proof.get("observed_until") or "").replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise GuardedRuntimeReconcileError(
            "live-client proof time is invalid"
        ) from exc
    if observed_from.tzinfo is None or observed_until.tzinfo is None:
        raise GuardedRuntimeReconcileError("live-client proof time has no timezone")
    measured_seconds = (
        observed_until.astimezone(timezone.utc)
        - observed_from.astimezone(timezone.utc)
    ).total_seconds()
    claimed_seconds = float(proof["observation_seconds"])
    if measured_seconds < 300 or abs(measured_seconds - claimed_seconds) > 1:
        raise GuardedRuntimeReconcileError("live-client proof interval differs")
    current = now or datetime.now(timezone.utc)
    age = (current - observed_until.astimezone(timezone.utc)).total_seconds()
    if age < -5 or age > 120:
        raise GuardedRuntimeReconcileError("live-client proof is stale")
    evidence = dict(proof["evidence"])
    if set(evidence) != {"t0", "t1"}:
        raise GuardedRuntimeReconcileError("live-client proof evidence fields differ")
    snapshot_keys = {
        "lease_guard_sha256",
        "topology_guard_sha256",
        "quarantine_guard_sha256",
        "anchors_guard_sha256",
        "request_audit_guard_sha256",
        "credential_nonce_guard_sha256",
        "producer_nonce_guard_sha256",
        "client_authority_guard_sha256",
        "stop_marker_sha256",
        "old_fence_nonterminal_rows",
        "target_process_count",
        "target_runnable_launcher_count",
        "server_schema_version",
        "client_schema_version",
    }
    t0 = dict(evidence.get("t0") or {})
    t1 = dict(evidence.get("t1") or {})
    if set(t0) != snapshot_keys or set(t1) != snapshot_keys:
        raise GuardedRuntimeReconcileError("live-client proof snapshot fields differ")
    guard_keys = {name for name in snapshot_keys if name.endswith("_sha256")}
    for name in guard_keys:
        for snapshot in (t0, t1):
            value = str(snapshot.get(name) or "")
            if len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
                raise GuardedRuntimeReconcileError("live-client proof guard is invalid")
        if t0[name] != t1[name]:
            raise GuardedRuntimeReconcileError("live-client proof guard changed")
    if t1["stop_marker_sha256"] != str(stop_marker_sha256 or "").lower():
        raise GuardedRuntimeReconcileError("live-client proof marker is not preimage-bound")
    count_keys = {
        "old_fence_nonterminal_rows", "target_process_count",
        "target_runnable_launcher_count",
    }
    for name in count_keys:
        if t0[name] != 0 or t1[name] != 0:
            raise GuardedRuntimeReconcileError("live-client proof runnable count differs")
    for name in {"server_schema_version", "client_schema_version"}:
        if (
            isinstance(t0[name], bool)
            or not isinstance(t0[name], int)
            or t0[name] < 1
            or t0[name] != t1[name]
        ):
            raise GuardedRuntimeReconcileError("live-client proof schema changed")
    return {
        "status": "PASS",
        "observed_from": observed_from.astimezone(timezone.utc).isoformat(),
        "observed_until": observed_until.astimezone(timezone.utc).isoformat(),
        "observation_seconds": claimed_seconds,
        "age_seconds": age,
    }


def _post_acquire_prunable_count(
    connection: sqlite3.Connection, producer_install_id: str
) -> int:
    return int(
        connection.execute(
            """
            SELECT COUNT(*)
              FROM producer_runtime_leases AS candidate
             WHERE candidate.producer_install_id=?
               AND candidate.status='EXPIRED'
               AND EXISTS (
                   SELECT 1 FROM producer_runtime_leases AS newer
                    WHERE newer.producer_install_id=candidate.producer_install_id
                      AND newer.fence>candidate.fence
                      AND newer.status IN ('EXPIRED','ACTIVE')
               )
               AND NOT EXISTS (
                   SELECT 1 FROM producer_runtime_issue_anchors AS anchor
                    WHERE anchor.lease_id=candidate.lease_id
               )
               AND NOT EXISTS (
                   SELECT 1 FROM producer_runtime_quarantine_audit AS audit
                    WHERE audit.active_lease_id=candidate.lease_id
               )
               AND NOT EXISTS (
                   SELECT 1 FROM producer_runtime_request_audit AS audit
                    WHERE audit.lease_id=candidate.lease_id
               )
            """,
            (producer_install_id,),
        ).fetchone()[0]
    )


def _git_source_binding(
    source_root: Path,
    *,
    source_commit: str,
    names: set[str] | frozenset[str],
) -> dict[str, str]:
    """Prove selected deployed files are the exact bytes in ``HEAD``."""

    ordered = sorted(names)
    try:
        head = subprocess.run(
            ["git", "-C", str(source_root), "rev-parse", "HEAD^{commit}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip().lower()
        worktree = subprocess.run(
            ["git", "-C", str(source_root), "diff", "--quiet", "HEAD", "--", *ordered],
            check=False,
            capture_output=True,
        )
        index = subprocess.run(
            ["git", "-C", str(source_root), "diff", "--cached", "--quiet", "HEAD", "--", *ordered],
            check=False,
            capture_output=True,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise GuardedRuntimeReconcileError("server source Git binding is unavailable") from exc
    if head != source_commit or worktree.returncode != 0 or index.returncode != 0:
        raise GuardedRuntimeReconcileError(
            "server source files are not the exact clean committed bytes"
        )
    result: dict[str, str] = {}
    for name in ordered:
        selected = (source_root / name).resolve(strict=True)
        if selected.parent != source_root:
            raise GuardedRuntimeReconcileError("server source file escaped its root")
        try:
            expected_blob = subprocess.run(
                ["git", "-C", str(source_root), "rev-parse", f"{source_commit}:{name}"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip().lower()
            actual_blob = subprocess.run(
                ["git", "-C", str(source_root), "hash-object", f"--path={name}", str(selected)],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip().lower()
        except (OSError, subprocess.SubprocessError) as exc:
            raise GuardedRuntimeReconcileError(
                f"server source commit does not contain {name}"
            ) from exc
        if actual_blob != expected_blob:
            raise GuardedRuntimeReconcileError(
                f"server source file differs from committed bytes: {name}"
            )
        result[name] = file_sha256(selected)
    return result


def _git_full_runtime_source_binding(
    source_root: Path,
    *,
    source_commit: str,
    process_created_at: float,
) -> dict[str, Any]:
    """Bind every tracked runtime source and reject untracked shadow modules."""

    try:
        status = subprocess.run(
            [
                "git", "-C", str(source_root), "status", "--porcelain=v1",
                "--untracked-files=all",
            ],
            check=True,
            capture_output=True,
        ).stdout
        tree = subprocess.run(
            ["git", "-C", str(source_root), "rev-parse", f"{source_commit}^{{tree}}"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip().lower()
        python_output = subprocess.run(
            ["git", "-C", str(source_root), "ls-files", "-z", "--", "*.py"],
            check=True,
            capture_output=True,
        ).stdout
    except (OSError, subprocess.SubprocessError) as exc:
        raise GuardedRuntimeReconcileError(
            "full server runtime source binding is unavailable"
        ) from exc
    if status:
        raise GuardedRuntimeReconcileError(
            "server runtime source worktree is not completely clean"
        )
    names = [
        value.decode("utf-8")
        for value in python_output.split(b"\0")
        if value
    ]
    if not names:
        raise GuardedRuntimeReconcileError("server runtime source has no tracked Python files")
    newest_mtime = 0.0
    for name in names:
        selected = (source_root / name).resolve(strict=True)
        try:
            selected.relative_to(source_root)
        except ValueError as exc:
            raise GuardedRuntimeReconcileError(
                "tracked server runtime source escaped its root"
            ) from exc
        newest_mtime = max(newest_mtime, selected.stat().st_mtime)
    if newest_mtime > process_created_at + 1:
        raise GuardedRuntimeReconcileError(
            "a tracked server runtime source changed after process start"
        )
    return {
        "source_tree": tree,
        "worktree_clean": True,
        "tracked_python_file_count": len(names),
        "newest_tracked_python_mtime_epoch": newest_mtime,
    }


def _independent_initializer_replay(
    *,
    snapshot: Path,
    source_root: Path,
    producer_install_id: str,
) -> dict[str, Any]:
    """Actually rerun the deployed initializers on a new E: snapshot copy."""

    if snapshot.drive.casefold() != "e:":
        raise GuardedRuntimeReconcileError("independent initializer replay must stay on E:")
    evidence_root = snapshot.parent / f"independent-initializer-{uuid.uuid4().hex}"
    tool = Path(__file__).resolve().parent / "tools" / "label_server_initializer_rehearsal.py"
    copied_snapshot = evidence_root / "source-snapshot.sqlite3"
    rehearsal = evidence_root / "initializer-rehearsal.sqlite3"
    output = evidence_root / "initializer-proof.json"
    try:
        completed = subprocess.run(
            [
                sys.executable,
                str(tool),
                "--live-server-db", str(snapshot),
                "--server-source-root", str(source_root),
                "--producer-install-id", producer_install_id,
                "--snapshot", str(copied_snapshot),
                "--rehearsal", str(rehearsal),
                "--output", str(output),
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=120,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise GuardedRuntimeReconcileError(
            "independent deployed initializer replay could not run"
        ) from exc
    if completed.returncode != 0:
        raise GuardedRuntimeReconcileError(
            "independent deployed initializer replay did not prove a no-op"
        )
    replay, replay_hash = _read_json_once(
        output, label="independent deployed initializer replay"
    )
    if (
        replay.get("schema_version") != SERVER_INITIALIZER_PROOF_SCHEMA
        or replay.get("status") != "PASS"
        or replay.get("initializer_calls")
        != ["init_self_enrollment_schema", "init_producer_ingest_schema"]
        or replay.get("total_changes") != 0
        or dict(replay.get("before_logical_digest") or {})
        != dict(replay.get("after_logical_digest") or {})
        or replay.get("integrity_check") != "ok"
        or replay.get("post_acquire_prunable_count") != 0
        or replay.get("secret_material_included") is not False
    ):
        raise GuardedRuntimeReconcileError(
            "independent deployed initializer replay is not a no-op"
        )
    return {
        "status": "PASS",
        "proof_path": str(output.resolve(strict=True)),
        "proof_sha256": replay_hash,
        "before_logical_digest": dict(replay["before_logical_digest"]),
        "after_logical_digest": dict(replay["after_logical_digest"]),
        "total_changes": int(replay["total_changes"]),
    }


def _server_live_snapshot_readback(
    *,
    server_db_path: str | os.PathLike[str],
    producer_install_id: str,
    expected_logical_digest: Mapping[str, Any],
    expected_schema_version: int,
) -> dict[str, Any]:
    """Atomically re-read the full live server image used by the rehearsal."""

    selected = Path(server_db_path).resolve(strict=True)
    try:
        connection = sqlite3.connect(
            f"file:{selected.as_posix()}?mode=ro", uri=True, timeout=30
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        logical = sqlite_logical_digest_on_connection(connection)
        schema_version = int(connection.execute("PRAGMA schema_version").fetchone()[0])
        journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()
        runtime_schema_version = int(
            connection.execute(
                "SELECT schema_version FROM producer_runtime_lease_schema_meta WHERE singleton=1"
            ).fetchone()[0]
        )
        prunable = _post_acquire_prunable_count(connection, producer_install_id)
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        connection.commit()
    except (sqlite3.Error, TypeError, ValueError, ExactCloneResolutionError) as exc:
        raise GuardedRuntimeReconcileError(
            "live server initializer state re-read failed"
        ) from exc
    finally:
        if "connection" in locals():
            connection.close()
    if (
        logical != dict(expected_logical_digest)
        or schema_version != int(expected_schema_version)
        or journal_mode != "wal"
        or runtime_schema_version != 3
        or prunable != 0
        or integrity != "ok"
    ):
        raise GuardedRuntimeReconcileError(
            "live server DB drifted from the rehearsed no-op snapshot"
        )
    return {
        "status": "PASS",
        "logical_digest": logical,
        "schema_version": schema_version,
        "journal_mode": journal_mode,
        "runtime_schema_version": runtime_schema_version,
        "post_acquire_prunable_count": prunable,
        "integrity_check": integrity,
    }


def validate_server_endpoint_binding(
    *,
    endpoint_url: str,
    server_db_path: str | os.PathLike[str],
    source_root: str | os.PathLike[str],
    source_commit: str,
    expected_launcher_path: str | os.PathLike[str],
    expected_launcher_sha256: str,
    expected_executable_path: str | os.PathLike[str],
    expected_executable_sha256: str,
) -> dict[str, Any]:
    """Bind the HTTPS endpoint to the exact local listener process and DB."""

    try:
        import psutil  # type: ignore[import-not-found]
    except ImportError as exc:
        raise GuardedRuntimeReconcileError(
            "server endpoint process binding dependencies are unavailable"
        ) from exc
    parsed = urlparse(str(endpoint_url or ""))
    if parsed.scheme.lower() != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise GuardedRuntimeReconcileError("server endpoint is not credential-free HTTPS")
    try:
        host = str(ipaddress.ip_address(parsed.hostname))
    except ValueError as exc:
        raise GuardedRuntimeReconcileError(
            "guarded reconcile requires an exact local IP endpoint"
        ) from exc
    port = int(parsed.port or 443)
    local_addresses = {
        str(address.address).split("%", 1)[0]
        for addresses in psutil.net_if_addrs().values()
        for address in addresses
    }
    if host not in local_addresses:
        raise GuardedRuntimeReconcileError("server endpoint IP is not assigned to this host")
    listeners = [
        connection
        for connection in psutil.net_connections(kind="tcp")
        if connection.status == psutil.CONN_LISTEN
        and connection.laddr
        and str(connection.laddr.ip).split("%", 1)[0] == host
        and int(connection.laddr.port) == port
        and connection.pid
    ]
    if len(listeners) != 1:
        raise GuardedRuntimeReconcileError(
            "server endpoint does not map to exactly one local listener process"
        )
    process = psutil.Process(int(listeners[0].pid))
    environment = process.environ()
    selected_root = Path(source_root).resolve(strict=True)
    selected_db = Path(server_db_path).resolve(strict=True)
    commit = str(source_commit or "").strip().lower()
    safe_environment = {
        name: str(environment.get(name) or "")
        for name in (
            "WORKER_ANALYSIS_SOURCE_ROOT",
            "WORKER_ANALYSIS_SOURCE_COMMIT",
            "WORKER_ANALYSIS_DB_PATH",
            "WORKER_ANALYSIS_BIND_HOST",
            "WORKER_ANALYSIS_PORT",
            "COMMON_INGEST_WRITE_ENABLED",
        )
    }
    if (
        not _same_path(safe_environment["WORKER_ANALYSIS_SOURCE_ROOT"], selected_root)
        or safe_environment["WORKER_ANALYSIS_SOURCE_COMMIT"].lower() != commit
        or not _same_path(safe_environment["WORKER_ANALYSIS_DB_PATH"], selected_db)
        or safe_environment["WORKER_ANALYSIS_BIND_HOST"] != host
        or safe_environment["WORKER_ANALYSIS_PORT"] != str(port)
        or safe_environment["COMMON_INGEST_WRITE_ENABLED"] != "1"
    ):
        raise GuardedRuntimeReconcileError(
            "local listener process environment is not bound to the expected server DB/source"
        )
    source_files = _git_source_binding(
        selected_root,
        source_commit=commit,
        names=_SERVER_PROCESS_SOURCE_FILES,
    )
    created_at = float(process.create_time())
    if any((selected_root / name).stat().st_mtime > created_at + 1 for name in source_files):
        raise GuardedRuntimeReconcileError(
            "a deployed source file changed after the listener process started"
        )
    full_source = _git_full_runtime_source_binding(
        selected_root,
        source_commit=commit,
        process_created_at=created_at,
    )
    command_line = process.cmdline()
    launcher = Path(expected_launcher_path).resolve(strict=True)
    executable = Path(expected_executable_path).resolve(strict=True)
    launcher_hash = str(expected_launcher_sha256 or "").strip().lower()
    executable_hash = str(expected_executable_sha256 or "").strip().lower()
    if (
        len(command_line) != 3
        or not _same_path(command_line[0], executable)
        or command_line[1] != "-u"
        or not _same_path(command_line[2], launcher)
        or not _same_path(process.exe(), executable)
        or file_sha256(launcher) != launcher_hash
        or file_sha256(executable) != executable_hash
        or launcher.stat().st_mtime > created_at + 1
    ):
        raise GuardedRuntimeReconcileError(
            "server listener executable/launcher argv is not the exact pinned command"
        )
    return {
        "status": "PASS",
        "endpoint_origin": urlunparse((parsed.scheme, parsed.netloc, "", "", "", "")),
        "listener_pid": int(process.pid),
        "listener_created_at_epoch": created_at,
        "server_db_path": str(selected_db),
        "server_source_root": str(selected_root),
        "server_source_commit": commit,
        "executable_path": str(executable),
        "executable_sha256": executable_hash,
        "launcher_path": str(launcher),
        "launcher_sha256": launcher_hash,
        "source_files": source_files,
        "full_runtime_source": full_source,
        "secret_material_included": False,
    }


class _SourceCommitBoundSession:
    """Validate process/DB binding at dispatch and the actual POST response."""

    def __init__(
        self,
        *,
        expected_binding: Mapping[str, Any],
        expected_source_commit: str,
        endpoint_validator: Callable[..., Mapping[str, Any]],
        endpoint_validator_kwargs: Mapping[str, Any],
        pre_dispatch_guard: Callable[[], Mapping[str, Any]],
    ) -> None:
        import requests

        self._session = requests.Session()
        self._session.trust_env = False
        self._expected_binding = dict(expected_binding)
        self._expected_source_commit = expected_source_commit.strip().lower()
        self._endpoint_validator = endpoint_validator
        self._endpoint_validator_kwargs = dict(endpoint_validator_kwargs)
        self._pre_dispatch_guard = pre_dispatch_guard
        self.last_pre_dispatch_guard: dict[str, Any] = {}
        self.post_count = 0

    def post(self, url: str, **kwargs: Any) -> Any:
        if self.post_count != 0:
            raise GuardedRuntimeReconcileError(
                "guarded runtime acquire permits exactly one POST attempt"
            )
        self.post_count = 1
        before = dict(self._endpoint_validator(**self._endpoint_validator_kwargs))
        if before != self._expected_binding:
            raise GuardedRuntimeReconcileError(
                "server endpoint binding changed immediately before runtime acquire"
            )
        self.last_pre_dispatch_guard = dict(self._pre_dispatch_guard())
        if self.last_pre_dispatch_guard.get("status") != "PASS":
            raise GuardedRuntimeReconcileError("immediate pre-dispatch guard did not pass")
        response = self._session.post(url, **kwargs)
        if (
            response.headers.get("X-KMTech-Source-Commit", "").strip().lower()
            != self._expected_source_commit
        ):
            raise GuardedRuntimeReconcileError(
                "runtime acquire response lacks the pinned server source commit"
            )
        after = dict(self._endpoint_validator(**self._endpoint_validator_kwargs))
        if after != self._expected_binding:
            raise GuardedRuntimeReconcileError(
                "server endpoint binding changed during runtime acquire"
            )
        return response


def validate_server_initializer_proof(
    proof: Mapping[str, Any],
    *,
    server_db_path: str | os.PathLike[str],
    producer_install_id: str,
    expected_source_root: str | os.PathLike[str],
    expected_source_commit: str,
    initializer_replayer: Callable[..., Mapping[str, Any]] = _independent_initializer_replay,
    now: datetime | None = None,
) -> dict[str, Any]:
    expected_keys = {
        "schema_version", "status", "observed_at", "live_server_db_path",
        "server_source_root", "server_source_commit", "snapshot_path",
        "server_source_files", "snapshot_sha256", "rehearsal_path",
        "rehearsal_sha256", "initializer_calls",
        "before_logical_digest", "after_logical_digest", "total_changes",
        "schema_version_before", "schema_version_after", "journal_mode_before",
        "journal_mode_after", "runtime_schema_version", "live_schema_version",
        "live_journal_mode", "live_runtime_schema_version", "integrity_check",
        "producer_install_id", "post_acquire_prunable_count",
        "secret_material_included",
    }
    if set(proof) != expected_keys:
        raise GuardedRuntimeReconcileError("server initializer proof fields differ")
    snapshot = Path(str(proof.get("snapshot_path") or "")).resolve(strict=True)
    rehearsal = Path(str(proof.get("rehearsal_path") or "")).resolve(strict=True)
    source_root = Path(str(proof.get("server_source_root") or "")).resolve(strict=True)
    snapshot_hash = str(proof.get("snapshot_sha256") or "").lower()
    rehearsal_hash = str(proof.get("rehearsal_sha256") or "").lower()
    source_commit = str(proof.get("server_source_commit") or "").lower()
    source_files = dict(proof.get("server_source_files") or {})
    expected_source_names = _SERVER_SOURCE_FILES
    try:
        snapshot_logical = sqlite_logical_digest(snapshot)
        rehearsal_logical = sqlite_logical_digest(rehearsal)
        live_logical = sqlite_logical_digest(server_db_path)
        with sqlite3.connect(
            f"file:{snapshot.as_posix()}?mode=ro", uri=True, timeout=30
        ) as snapshot_db:
            snapshot_schema_version = int(
                snapshot_db.execute("PRAGMA schema_version").fetchone()[0]
            )
            snapshot_journal_mode = str(
                snapshot_db.execute("PRAGMA journal_mode").fetchone()[0]
            ).lower()
            runtime_schema_version = int(
                snapshot_db.execute(
                    "SELECT schema_version FROM producer_runtime_lease_schema_meta WHERE singleton=1"
                ).fetchone()[0]
            )
            snapshot_prunable = _post_acquire_prunable_count(
                snapshot_db, producer_install_id
            )
        with sqlite3.connect(
            f"file:{rehearsal.as_posix()}?mode=ro", uri=True, timeout=30
        ) as rehearsal_db:
            rehearsal_schema_version = int(
                rehearsal_db.execute("PRAGMA schema_version").fetchone()[0]
            )
            rehearsal_journal_mode = str(
                rehearsal_db.execute("PRAGMA journal_mode").fetchone()[0]
            ).lower()
        live_path = Path(server_db_path).resolve(strict=True)
        with sqlite3.connect(
            f"file:{live_path.as_posix()}?mode=ro", uri=True, timeout=30
        ) as live_db:
            live_schema_version = int(live_db.execute("PRAGMA schema_version").fetchone()[0])
            live_journal_mode = str(live_db.execute("PRAGMA journal_mode").fetchone()[0]).lower()
            live_runtime_schema_version = int(
                live_db.execute(
                    "SELECT schema_version FROM producer_runtime_lease_schema_meta WHERE singleton=1"
                ).fetchone()[0]
            )
            live_prunable = _post_acquire_prunable_count(
                live_db, producer_install_id
            )
    except (sqlite3.Error, TypeError, ValueError, ExactCloneResolutionError) as exc:
        raise GuardedRuntimeReconcileError(
            "server initializer SQLite proof is invalid"
        ) from exc
    try:
        actual_source_commit = subprocess.run(
            ["git", "-C", str(source_root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip().lower()
    except (OSError, subprocess.SubprocessError) as exc:
        raise GuardedRuntimeReconcileError("server source commit is unavailable") from exc
    committed_source_files = _git_source_binding(
        source_root,
        source_commit=source_commit,
        names=expected_source_names,
    )
    if (
        proof.get("schema_version") != SERVER_INITIALIZER_PROOF_SCHEMA
        or proof.get("status") != "PASS"
        or not _same_path(proof.get("live_server_db_path", ""), server_db_path)
        or snapshot.drive.casefold() != "e:"
        or rehearsal.drive.casefold() != "e:"
        or not source_root.is_dir()
        or not _same_path(source_root, expected_source_root)
        or len(source_commit) != 40
        or any(c not in "0123456789abcdef" for c in source_commit)
        or source_commit != actual_source_commit
        or source_commit != str(expected_source_commit or "").strip().lower()
        or _same_path(snapshot, rehearsal)
        or os.path.samefile(snapshot, rehearsal)
        or len(snapshot_hash) != 64
        or any(c not in "0123456789abcdef" for c in snapshot_hash)
        or file_sha256(snapshot) != snapshot_hash
        or len(rehearsal_hash) != 64
        or any(c not in "0123456789abcdef" for c in rehearsal_hash)
        or file_sha256(rehearsal) != rehearsal_hash
        or set(source_files) != expected_source_names
        or any(
            file_sha256(source_root / name) != str(source_files.get(name) or "").lower()
            for name in expected_source_names
        )
        or source_files != committed_source_files
        or snapshot_logical != dict(proof.get("before_logical_digest") or {})
        or rehearsal_logical != dict(proof.get("after_logical_digest") or {})
        or live_logical != snapshot_logical
        or proof.get("initializer_calls")
        != ["init_self_enrollment_schema", "init_producer_ingest_schema"]
        or dict(proof.get("before_logical_digest") or {})
        != dict(proof.get("after_logical_digest") or {})
        or proof.get("total_changes") != 0
        or proof.get("schema_version_before") != proof.get("schema_version_after")
        or proof.get("schema_version_before") != snapshot_schema_version
        or proof.get("schema_version_after") != rehearsal_schema_version
        or proof.get("runtime_schema_version") != 3
        or runtime_schema_version != 3
        or proof.get("live_schema_version") != live_schema_version
        or str(proof.get("live_journal_mode") or "").lower() != live_journal_mode
        or proof.get("live_runtime_schema_version") != live_runtime_schema_version
        or live_runtime_schema_version != 3
        or proof.get("producer_install_id") != producer_install_id
        or proof.get("post_acquire_prunable_count") != 0
        or snapshot_prunable != 0
        or live_prunable != 0
        or str(proof.get("journal_mode_before") or "").lower() != "wal"
        or str(proof.get("journal_mode_after") or "").lower() != "wal"
        or snapshot_journal_mode != "wal"
        or rehearsal_journal_mode != "wal"
        or proof.get("integrity_check") != "ok"
        or proof.get("secret_material_included") is not False
    ):
        raise GuardedRuntimeReconcileError("server initializer rehearsal is not a no-op")
    try:
        observed = datetime.fromisoformat(
            str(proof.get("observed_at") or "").replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise GuardedRuntimeReconcileError("server initializer proof time is invalid") from exc
    if observed.tzinfo is None:
        raise GuardedRuntimeReconcileError("server initializer proof time has no timezone")
    age = (
        (now or datetime.now(timezone.utc)) - observed.astimezone(timezone.utc)
    ).total_seconds()
    if age < -5 or age > 120:
        raise GuardedRuntimeReconcileError("server initializer proof is stale")
    replay = dict(
        initializer_replayer(
            snapshot=snapshot,
            source_root=source_root,
            producer_install_id=producer_install_id,
        )
    )
    if (
        replay.get("status") != "PASS"
        or replay.get("total_changes") != 0
        or dict(replay.get("before_logical_digest") or {}) != snapshot_logical
        or dict(replay.get("after_logical_digest") or {}) != snapshot_logical
    ):
        raise GuardedRuntimeReconcileError(
            "independent deployed initializer replay did not match the snapshot"
        )
    live_after_replay = _server_live_snapshot_readback(
        server_db_path=server_db_path,
        producer_install_id=producer_install_id,
        expected_logical_digest=snapshot_logical,
        expected_schema_version=live_schema_version,
    )
    return {
        "status": "PASS", "observed_at": observed.astimezone(timezone.utc).isoformat(),
        "age_seconds": age, "snapshot_sha256": snapshot_hash,
        "server_source_commit": source_commit,
        "committed_source_files": committed_source_files,
        "independent_replay": replay,
        "live_after_independent_replay": live_after_replay,
    }


def _row_thumbprint(raw_json: Any) -> str:
    try:
        value = json.loads(str(raw_json or ""))
    except json.JSONDecodeError as exc:
        raise GuardedRuntimeReconcileError("client runtime JWK is invalid") from exc
    required = {"crv", "kty", "x", "y"}
    if not isinstance(value, dict) or not required.issubset(value):
        raise GuardedRuntimeReconcileError("client runtime JWK is incomplete")
    public = {name: str(value[name]) for name in sorted(required)}
    canonical = json.dumps(
        public,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    import base64

    return base64.urlsafe_b64encode(hashlib.sha256(canonical).digest()).decode(
        "ascii"
    ).rstrip("=")


def _safe_authority(row: sqlite3.Row) -> dict[str, Any]:
    fields = (
        "authority_scope",
        "endpoint_url",
        "producer_id",
        "key_id",
        "producer_install_id",
        "runtime_instance_id",
        "lease_id",
        "fence",
        "next_request_sequence",
        "expires_at",
        "assigned_relay_id",
        "pending_issue_idempotency_key",
        "status",
        "last_error_code",
        "created_at",
        "updated_at",
    )
    value = {name: row[name] for name in fields}
    value["runtime_public_jwk_thumbprint"] = _row_thumbprint(
        row["runtime_public_jwk_json"]
    )
    value["runtime_public_jwk_sha256"] = hashlib.sha256(
        str(row["runtime_public_jwk_json"] or "").encode("utf-8")
    ).hexdigest()
    value["next_request_token_present"] = bool(row["next_request_token"])
    value["next_request_token_sha256"] = (
        hashlib.sha256(str(row["next_request_token"]).encode("utf-8")).hexdigest()
        if row["next_request_token"]
        else ""
    )
    value["pending_request_present"] = bool(row["pending_request_json"])
    value["pending_request_sha256"] = (
        hashlib.sha256(str(row["pending_request_json"]).encode("utf-8")).hexdigest()
        if row["pending_request_json"]
        else ""
    )
    return value


def prepare_client_compare_and_swap(
    *,
    client_db_path: str | os.PathLike[str],
    preimage: Mapping[str, Any],
    now_text: str | None = None,
) -> dict[str, Any]:
    """Retire the stale scope and reopen only the exact conflict candidate."""

    client = dict(preimage.get("client") or {})
    candidate_before = dict(client.get("candidate_authority") or {})
    prior_before = dict(client.get("prior_authority") or {})
    selected_path = Path(client_db_path).expanduser().resolve(strict=True)
    if not _same_path(client.get("database_path"), selected_path):
        raise GuardedRuntimeReconcileError("client database path differs")
    changed_at = now_text or _utc_now()
    connection = sqlite3.connect(selected_path, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA busy_timeout=30000")
    try:
        connection.execute("BEGIN IMMEDIATE")
        journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0])
        index_rows = connection.execute(
            "PRAGMA index_info(idx_direct_sync_runtime_assignment)"
        ).fetchall()
        if journal_mode.lower() != "wal" or [row[2] for row in index_rows] != [
            "assigned_relay_id"
        ]:
            raise GuardedRuntimeReconcileError(
                "client runtime schema would mutate during authority acquisition"
            )
        if sqlite_logical_digest_on_connection(connection) != dict(
            client.get("database_logical_digest") or {}
        ):
            raise GuardedRuntimeReconcileError(
                "client logical database changed before compare-and-swap"
            )
        rows = connection.execute(
            "SELECT * FROM direct_sync_runtime_authority ORDER BY authority_scope"
        ).fetchall()
        current = [_safe_authority(row) for row in rows]
        expected = sorted(
            [candidate_before, prior_before],
            key=lambda row: str(row.get("authority_scope") or ""),
        )
        if current != expected:
            raise GuardedRuntimeReconcileError(
                "client authority rows changed before compare-and-swap"
            )
        if any(row["assigned_relay_id"] for row in rows):
            raise GuardedRuntimeReconcileError(
                "a client authority is assigned to a live relay request"
            )
        prior_scope = str(prior_before.get("authority_scope") or "")
        candidate_scope = str(candidate_before.get("authority_scope") or "")
        if not prior_scope or not candidate_scope or prior_scope == candidate_scope:
            raise GuardedRuntimeReconcileError("preimage authority scopes are invalid")
        connection.execute(
            """
            UPDATE direct_sync_runtime_authority
               SET lease_id=NULL, fence=NULL, next_request_token=NULL,
                   next_request_sequence=NULL, expires_at=NULL,
                   assigned_relay_id=NULL, pending_request_json=NULL,
                   pending_issue_idempotency_key=NULL,
                   status='LEGACY_DISABLED',
                   last_error_code='exact_clone_reconciled', updated_at=?
             WHERE authority_scope=? AND status='ACTIVE'
            """,
            (changed_at, prior_scope),
        )
        if connection.execute("SELECT changes()").fetchone()[0] != 1:
            raise GuardedRuntimeReconcileError(
                "prior authority compare-and-swap changed no row"
            )
        connection.execute(
            """
            UPDATE direct_sync_runtime_authority
               SET lease_id=NULL, fence=NULL, next_request_token=NULL,
                   next_request_sequence=NULL, expires_at=NULL,
                   assigned_relay_id=NULL, pending_request_json=NULL,
                   pending_issue_idempotency_key=NULL,
                   status='PENDING', last_error_code=NULL, updated_at=?
             WHERE authority_scope=? AND status='OPERATOR_REVIEW'
               AND last_error_code=?
            """,
            (changed_at, candidate_scope, CONFLICT_CODE),
        )
        if connection.execute("SELECT changes()").fetchone()[0] != 1:
            raise GuardedRuntimeReconcileError(
                "candidate authority compare-and-swap changed no row"
            )
        connection.commit()
    except BaseException:
        if connection.in_transaction:
            connection.rollback()
        raise
    finally:
        connection.close()
    return {
        "status": "PREPARED",
        "prior_authority_scope": prior_scope,
        "candidate_authority_scope": candidate_scope,
        "changed_at": changed_at,
    }


def _server_forward_state(
    server_db_path: str | os.PathLike[str],
    *,
    producer_install_id: str,
    candidate_runtime_id: str,
    prior_lease_id: str,
    prior_fence: int,
) -> dict[str, Any]:
    selected = Path(server_db_path).expanduser().resolve(strict=True)
    connection = sqlite3.connect(
        f"file:{selected.as_posix()}?mode=ro", uri=True, timeout=30
    )
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """SELECT lease_id,runtime_instance_id,fence,status,expires_at
                 FROM producer_runtime_leases
                WHERE producer_install_id=? ORDER BY fence""",
            (producer_install_id,),
        ).fetchall()
    finally:
        connection.close()
    active = [dict(row) for row in rows if row["status"] == "ACTIVE"]
    prior = [dict(row) for row in rows if row["lease_id"] == prior_lease_id]
    newer = [dict(row) for row in rows if int(row["fence"]) > int(prior_fence)]
    forward = bool(
        len(active) == 1
        and active[0]["runtime_instance_id"] == candidate_runtime_id
        and int(active[0]["fence"]) > int(prior_fence)
        and len(newer) == 1
        and newer[0]["lease_id"] == active[0]["lease_id"]
        and len(prior) == 1
        and prior[0]["status"] == "EXPIRED"
    )
    unchanged = bool(
        len(active) == 1
        and active[0]["lease_id"] == prior_lease_id
        and int(active[0]["fence"]) == int(prior_fence)
        and len(newer) == 0
        and len(prior) == 1
        and prior[0]["status"] == "ACTIVE"
    )
    return {
        "forward": forward,
        "unchanged": unchanged,
        "active_count": len(active),
        "active_lease_id": active[0]["lease_id"] if len(active) == 1 else "",
        "active_runtime_instance_id": (
            active[0]["runtime_instance_id"] if len(active) == 1 else ""
        ),
        "active_fence": int(active[0]["fence"]) if len(active) == 1 else 0,
        "prior_status": prior[0]["status"] if len(prior) == 1 else "",
        "newer_fence_count": len(newer),
    }


def run_guarded_runtime_reconcile(
    *,
    preimage_path: str | os.PathLike[str],
    client_backup_path: str | os.PathLike[str],
    client_backup_sha256: str,
    live_client_proof_path: str | os.PathLike[str],
    live_client_proof_sha256: str,
    server_initializer_proof_path: str | os.PathLike[str],
    server_initializer_proof_sha256: str,
    server_source_root: str | os.PathLike[str],
    server_source_commit: str,
    server_launcher_path: str | os.PathLike[str],
    server_launcher_sha256: str,
    server_executable_path: str | os.PathLike[str],
    server_executable_sha256: str,
    client_db_path: str | os.PathLike[str],
    server_db_path: str | os.PathLike[str],
    identity_path: str | os.PathLike[str],
    credential_path: str | os.PathLike[str],
    stop_marker_path: str | os.PathLike[str],
    portable_root: str | os.PathLike[str],
    receipt_output_path: str | os.PathLike[str],
    tls_ca_bundle_path: str | os.PathLike[str] = "",
    tls_ca_bundle_sha256: str = "",
    credential_loader: Callable[[str | os.PathLike[str]], Any] = (
        load_credentials_from_json
    ),
    authority_acquirer: Callable[..., RuntimePreparation] = ensure_runtime_authority,
    initializer_replayer: Callable[..., Mapping[str, Any]] = _independent_initializer_replay,
    endpoint_validator: Callable[..., Mapping[str, Any]] = validate_server_endpoint_binding,
) -> dict[str, Any]:
    """Execute the approved forward-only authority cutover and emit its receipt."""

    started_at = _utc_now()
    preimage, preimage_hash = _required_preimage(preimage_path)
    if not _same_path(
        dict(preimage.get("client") or {}).get("database_path", ""), client_db_path
    ):
        raise GuardedRuntimeReconcileError("preimage client database path differs")
    if not _same_path(
        dict(preimage.get("server") or {}).get("database_path", ""), server_db_path
    ):
        raise GuardedRuntimeReconcileError("preimage server database path differs")
    receipt_target = Path(receipt_output_path).expanduser().resolve(strict=False)
    _preflight_new_output(receipt_target)
    proof, proof_hash = _read_json_once(
        live_client_proof_path, label="old-fence liveness proof"
    )
    if proof_hash != str(live_client_proof_sha256 or "").strip().lower():
        raise GuardedRuntimeReconcileError("old-fence liveness proof SHA-256 pin differs")
    proof_validation_kwargs = {
        "preimage_sha256": preimage_hash,
        "stop_marker_sha256": str(dict(preimage.get("stop_marker") or {}).get("sha256") or ""),
        "server_db_path": server_db_path,
        "client_db_path": client_db_path,
        "producer_install_id": str(preimage.get("producer_install_id") or ""),
        "old_lease_id": str(dict(preimage["server"])["prior_active_lease"]["lease_id"]),
        "old_runtime_instance_id": str(
            dict(preimage["server"])["prior_active_lease"]["runtime_instance_id"]
        ),
        "old_fence": int(dict(preimage["server"])["prior_active_lease"]["fence"]),
        "producer_id": str(dict(preimage["current_credential"])["producer_id"]),
        "key_id": str(dict(preimage["current_credential"])["key_id"]),
    }
    proof_readback = validate_live_client_proof(proof, **proof_validation_kwargs)
    initializer_proof, initializer_proof_hash = _read_json_once(
        server_initializer_proof_path, label="server initializer no-op proof"
    )
    if initializer_proof_hash != str(
        server_initializer_proof_sha256 or ""
    ).strip().lower():
        raise GuardedRuntimeReconcileError("server initializer proof SHA-256 pin differs")
    initializer_readback = validate_server_initializer_proof(
        initializer_proof,
        server_db_path=server_db_path,
        producer_install_id=str(preimage.get("producer_install_id") or ""),
        expected_source_root=server_source_root,
        expected_source_commit=server_source_commit,
        initializer_replayer=initializer_replayer,
    )
    backup_readback = verify_client_backup(
        backup_path=client_backup_path,
        client_db_path=client_db_path,
        preimage=preimage,
        expected_sha256=client_backup_sha256,
    )
    verify_conflict_preimage_still_current(
        preimage=preimage,
        client_db_path=client_db_path,
        server_db_path=server_db_path,
        identity_path=identity_path,
        credential_path=credential_path,
        stop_marker_path=stop_marker_path,
        portable_root=portable_root,
    )
    credentials = credential_loader(credential_path)
    expected_credential = dict(preimage.get("current_credential") or {})
    if (
        str(getattr(credentials, "runtime_lease_mode", "")) != "enforce"
        or str(getattr(credentials, "producer_id", ""))
        != str(expected_credential.get("producer_id") or "")
        or str(getattr(credentials, "key_id", ""))
        != str(expected_credential.get("key_id") or "")
        or str(getattr(credentials, "endpoint_url", ""))
        != str(expected_credential.get("endpoint_url") or "")
    ):
        raise GuardedRuntimeReconcileError(
            "loaded credential does not match the enforced preimage authority"
        )
    ca_path = Path(tls_ca_bundle_path).expanduser().resolve(strict=True)
    expected_ca_hash = str(tls_ca_bundle_sha256 or "").strip().lower()
    if (
        not ca_path.is_file()
        or len(expected_ca_hash) != 64
        or any(character not in "0123456789abcdef" for character in expected_ca_hash)
        or file_sha256(ca_path) != expected_ca_hash
    ):
        raise GuardedRuntimeReconcileError("TLS CA bundle SHA-256 pin differs")
    endpoint_validator_kwargs = {
        "endpoint_url": str(getattr(credentials, "endpoint_url", "")),
        "server_db_path": server_db_path,
        "source_root": server_source_root,
        "source_commit": server_source_commit,
        "expected_launcher_path": server_launcher_path,
        "expected_launcher_sha256": server_launcher_sha256,
        "expected_executable_path": server_executable_path,
        "expected_executable_sha256": server_executable_sha256,
    }
    endpoint_binding_before = dict(endpoint_validator(**endpoint_validator_kwargs))
    if endpoint_binding_before.get("status") != "PASS":
        raise GuardedRuntimeReconcileError("server endpoint binding did not pass")
    final_initializer_readback = _server_live_snapshot_readback(
        server_db_path=server_db_path,
        producer_install_id=str(preimage.get("producer_install_id") or ""),
        expected_logical_digest=dict(initializer_proof["before_logical_digest"]),
        expected_schema_version=int(initializer_proof["live_schema_version"]),
    )
    proof_readback = validate_live_client_proof(proof, **proof_validation_kwargs)
    def _immediate_pre_dispatch_guard() -> dict[str, Any]:
        server_readback = _server_live_snapshot_readback(
            server_db_path=server_db_path,
            producer_install_id=str(preimage.get("producer_install_id") or ""),
            expected_logical_digest=dict(initializer_proof["before_logical_digest"]),
            expected_schema_version=int(initializer_proof["live_schema_version"]),
        )
        liveness_readback = validate_live_client_proof(
            proof, **proof_validation_kwargs
        )
        return {
            "status": "PASS",
            "server": server_readback,
            "liveness": liveness_readback,
        }
    bound_session = _SourceCommitBoundSession(
        expected_binding=endpoint_binding_before,
        expected_source_commit=server_source_commit,
        endpoint_validator=endpoint_validator,
        endpoint_validator_kwargs=endpoint_validator_kwargs,
        pre_dispatch_guard=_immediate_pre_dispatch_guard,
    )
    client_prepare = prepare_client_compare_and_swap(
        client_db_path=client_db_path,
        preimage=preimage,
    )
    install_id = str(preimage.get("producer_install_id") or "")
    candidate_before = dict(dict(preimage["client"])["candidate_authority"])
    prior_server = dict(dict(preimage["server"])["prior_active_lease"])
    preparation: RuntimePreparation | None = None
    try:
        for _attempt in range(1):
            preparation = authority_acquirer(
                db_path=client_db_path,
                credentials=credentials,
                producer_install_id=install_id,
                session=bound_session,
                tls_ca_bundle_path=str(ca_path),
            )
            if (
                not preparation.operator_review
                and preparation.receipt.get("server_grant_accepted") is True
                and preparation.receipt.get("status") == "ACTIVE"
            ):
                break
            if not preparation.retryable:
                break
    except Exception as exc:
        raise GuardedRuntimeReconcileError(
            f"runtime acquire became transport-ambiguous; client retained fail-closed: {exc}",
            server_forward=True,
        ) from exc
    assert preparation is not None
    if (
        bound_session.post_count != 1
        or bound_session.last_pre_dispatch_guard.get("status") != "PASS"
    ):
        raise GuardedRuntimeReconcileError(
            "runtime acquirer did not use the once-only guarded HTTPS session",
            server_forward=True,
        )
    try:
        endpoint_binding_after = dict(endpoint_validator(**endpoint_validator_kwargs))
        if endpoint_binding_after != endpoint_binding_before:
            raise GuardedRuntimeReconcileError(
                "server endpoint process/DB/source binding changed during acquire"
            )
        server_state = _server_forward_state(
            server_db_path,
            producer_install_id=install_id,
            candidate_runtime_id=str(candidate_before["runtime_instance_id"]),
            prior_lease_id=str(prior_server["lease_id"]),
            prior_fence=int(prior_server["fence"]),
        )
    except Exception as exc:
        raise GuardedRuntimeReconcileError(
            f"runtime acquire completed but server readback is ambiguous: {exc}",
            server_forward=True,
        ) from exc
    if not (
        not preparation.operator_review
        and preparation.receipt.get("server_grant_accepted") is True
        and preparation.receipt.get("status") == "ACTIVE"
        and server_state["forward"] is True
        and preparation.receipt.get("lease_id") == server_state["active_lease_id"]
        and preparation.receipt.get("fence") == server_state["active_fence"]
    ):
        raise GuardedRuntimeReconcileError(
            "runtime acquire did not prove the exact forward server state; old authority was not restored",
            server_forward=True,
        )
    try:
        receipt = create_resolution_receipt(
            preimage=preimage,
            client_db_path=client_db_path,
            server_db_path=server_db_path,
            identity_path=identity_path,
            credential_path=credential_path,
            stop_marker_path=stop_marker_path,
            portable_root=portable_root,
        )
        _write_new_json(receipt_output_path, receipt)
        receipt_hash = file_sha256(receipt_output_path)
    except Exception as exc:
        raise GuardedRuntimeReconcileError(
            f"server fence advanced but resolution receipt failed: {exc}",
            server_forward=True,
        ) from exc
    return {
        "schema_version": RECONCILE_REPORT_SCHEMA,
        "status": "RESOLVED",
        "started_at": started_at,
        "completed_at": _utc_now(),
        "preimage_path": str(Path(preimage_path).resolve(strict=False)),
        "preimage_sha256": preimage_hash,
        "client_backup": backup_readback,
        "tls_ca_bundle": {
            "path": str(ca_path),
            "sha256": expected_ca_hash,
        },
        "live_client_proof": proof_readback,
        "server_initializer_proof": initializer_readback,
        "server_initializer_final_readback": final_initializer_readback,
        "server_endpoint_binding": endpoint_binding_after,
        "immediate_pre_dispatch_guard": bound_session.last_pre_dispatch_guard,
        "client_prepare": client_prepare,
        "server_transition": {
            **server_state,
            "rollback_contract": (
                "forward-only; obtain a newer fence instead of restoring an old fence"
            ),
        },
        "receipt_path": str(Path(receipt_output_path).resolve(strict=False)),
        "receipt_sha256": receipt_hash,
        "selected_lease_id": preparation.receipt.get("lease_id"),
        "selected_fence": preparation.receipt.get("fence"),
        "stop_marker_removed": False,
        "client_state_mutated": True,
        "server_state_mutated": True,
        "secret_material_included": False,
    }


__all__ = [
    "GuardedRuntimeReconcileError",
    "LIVE_CLIENT_PROOF_SCHEMA",
    "RECONCILE_REPORT_SCHEMA",
    "prepare_client_compare_and_swap",
    "run_guarded_runtime_reconcile",
    "validate_live_client_proof",
    "validate_server_initializer_proof",
    "verify_client_backup",
    "verify_conflict_preimage_still_current",
]

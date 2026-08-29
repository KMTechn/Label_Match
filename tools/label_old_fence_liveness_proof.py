#!/usr/bin/env python3
"""Capture an atomic, 300-second proof that Label's old fence is quiescent."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import time
import uuid


SCHEMA = "label-match-old-fence-liveness-proof-v1"
QUERY_VERSION = "label-match-old-fence-liveness-query-v2"


def _canonical(value: object) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str
    ).encode("utf-8")


def _guard(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _rows(connection: sqlite3.Connection, sql: str, params: tuple[object, ...]) -> list[dict]:
    return [dict(row) for row in connection.execute(sql, params).fetchall()]


def _server_snapshot(path: Path, preimage: dict) -> dict:
    install_id = str(preimage["producer_install_id"])
    prior = dict(preimage["server"]["prior_active_lease"])
    current = dict(preimage["current_credential"])
    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    try:
        connection.execute("BEGIN")
        schema_version = int(connection.execute("PRAGMA schema_version").fetchone()[0])
        db_now = str(
            connection.execute(
                "SELECT strftime('%Y-%m-%dT%H:%M:%fZ','now')"
            ).fetchone()[0]
        )
        lease = _rows(
            connection,
            """SELECT lease_id,producer_install_id,runtime_instance_id,
                      public_jwk_thumbprint,issue_idempotency_key,request_fingerprint,
                      fence,current_request_token_hash,current_request_sequence,status,
                      issued_at,expires_at,last_rotated_at,response_hash
                 FROM producer_runtime_leases
                WHERE producer_install_id=? AND lease_id=?
                  AND runtime_instance_id=? AND fence=?""",
            (
                install_id,
                prior["lease_id"],
                prior["runtime_instance_id"],
                prior["fence"],
            ),
        )
        topology = _rows(
            connection,
            """SELECT lease_id,runtime_instance_id,public_jwk_thumbprint,fence,
                      current_request_sequence,status,issued_at,expires_at,
                      last_rotated_at,response_hash
                 FROM producer_runtime_leases WHERE producer_install_id=?
                ORDER BY fence,lease_id""",
            (install_id,),
        )
        quarantine = _rows(
            connection,
            """SELECT audit_id,producer_install_id,runtime_instance_id,
                      public_jwk_thumbprint,issue_idempotency_key,request_fingerprint,
                      active_lease_id,active_runtime_instance_id,
                      active_public_jwk_thumbprint,active_fence,reason_code,occurred_at
                 FROM producer_runtime_quarantine_audit WHERE producer_install_id=?
                ORDER BY occurred_at,audit_id""",
            (install_id,),
        )
        anchors = _rows(
            connection,
            """SELECT producer_install_id,issue_idempotency_key,request_fingerprint,
                      outcome,response_hash,lease_id,committed_at
                 FROM producer_runtime_issue_anchors WHERE producer_install_id=?
                ORDER BY committed_at,issue_idempotency_key""",
            (install_id,),
        )
        audits = _rows(
            connection,
            """SELECT audit_id,lease_id,runtime_instance_id,public_jwk_thumbprint,
                      fence,presented_sequence,next_sequence,request_fingerprint,
                      receipt_request_id,outcome,reason_code,occurred_at
                 FROM producer_runtime_request_audit
                WHERE producer_install_id=?
                  AND (lease_id=? OR runtime_instance_id=? OR fence=?)
                ORDER BY occurred_at,audit_id""",
            (
                install_id,
                prior["lease_id"],
                prior["runtime_instance_id"],
                prior["fence"],
            ),
        )
        credential_nonces = _rows(
            connection,
            """SELECT producer_id,key_id,nonce,producer_timestamp,created_at
                 FROM producer_ingest_nonces WHERE producer_id=? AND key_id=?
                ORDER BY key_id,created_at,nonce""",
            (current["producer_id"], current["key_id"]),
        )
        producer_nonces = _rows(
            connection,
            """SELECT producer_id,key_id,nonce,producer_timestamp,created_at
                 FROM producer_ingest_nonces WHERE producer_id=?
                ORDER BY key_id,created_at,nonce""",
            (current["producer_id"],),
        )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return {
        "db_now": db_now,
        "server_schema_version": schema_version,
        "lease_guard_sha256": _guard(lease),
        "topology_guard_sha256": _guard(topology),
        "quarantine_guard_sha256": _guard(quarantine),
        "anchors_guard_sha256": _guard(anchors),
        "request_audit_guard_sha256": _guard(audits),
        "credential_nonce_guard_sha256": _guard(credential_nonces),
        "producer_nonce_guard_sha256": _guard(producer_nonces),
        "old_fence_logically_expired": bool(
            len(lease) == 1
            and datetime.fromisoformat(
                str(lease[0]["expires_at"]).replace("Z", "+00:00")
            )
            < datetime.fromisoformat(db_now.replace("Z", "+00:00"))
        ),
        "active_count": sum(row["status"] == "ACTIVE" for row in topology),
    }


def _client_snapshot(path: Path) -> dict:
    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    try:
        connection.execute("BEGIN")
        schema_version = int(connection.execute("PRAGMA schema_version").fetchone()[0])
        authorities = _rows(
            connection,
            "SELECT * FROM direct_sync_runtime_authority ORDER BY authority_scope",
            (),
        )
        nonterminal = int(
            connection.execute(
                """SELECT COUNT(*) FROM direct_sync_runtime_authority
                    WHERE assigned_relay_id IS NOT NULL
                       OR pending_request_json IS NOT NULL"""
            ).fetchone()[0]
        )
        connection.commit()
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()
    return {
        "client_schema_version": schema_version,
        "client_authority_guard_sha256": _guard(authorities),
        "old_fence_nonterminal_rows": nonterminal,
    }


def _host_counts(canonical_root: Path, direct_sync_root: Path) -> dict[str, int]:
    root_one = str(canonical_root).replace("'", "''")
    root_two = str(direct_sync_root).replace("'", "''")
    script = rf"""
$roots=@('{root_one}','{root_two}')
$processCount=0
Get-CimInstance Win32_Process -ErrorAction Stop | ForEach-Object {{
  $value=([string]$_.ExecutablePath)+' '+([string]$_.CommandLine)
  if (($value -like ('*'+$roots[0]+'*') -or $value -like ('*'+$roots[1]+'*')) -and
      ($value -match 'label-match-(user|direct-sync)-relay')) {{ $processCount++ }}
}}
$launcherCount=0
Get-ScheduledTask -ErrorAction Stop | ForEach-Object {{
  foreach($action in $_.Actions) {{
    $value=([string]$action.Execute)+' '+([string]$action.Arguments)+' '+([string]$action.WorkingDirectory)
    if ($value -like ('*'+$roots[0]+'*') -or $value -like ('*'+$roots[1]+'*')) {{ $launcherCount++; break }}
  }}
}}
Get-CimInstance Win32_Service -ErrorAction Stop | ForEach-Object {{
  $value=[string]$_.PathName
  if ($value -like ('*'+$roots[0]+'*') -or $value -like ('*'+$roots[1]+'*')) {{ $launcherCount++ }}
}}
[Console]::Write((@{{process_count=$processCount;launcher_count=$launcherCount}}|ConvertTo-Json -Compress))
"""
    result = subprocess.run(
        ["powershell", "-NoProfile", "-NonInteractive", "-Command", script],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    value = json.loads(result.stdout)
    return {
        "target_process_count": int(value["process_count"]),
        "target_runnable_launcher_count": int(value["launcher_count"]),
    }


def _snapshot(args: argparse.Namespace, preimage: dict) -> dict:
    server = _server_snapshot(args.server_db, preimage)
    client = _client_snapshot(args.client_db)
    marker_hash = _sha256(args.stop_marker)
    host = _host_counts(args.canonical_root, args.direct_sync_root)
    return {**server, **client, **host, "stop_marker_sha256": marker_hash}


def _write_new(path: Path, value: dict) -> None:
    payload = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preimage", type=Path, required=True)
    parser.add_argument("--server-db", type=Path, required=True)
    parser.add_argument("--client-db", type=Path, required=True)
    parser.add_argument("--stop-marker", type=Path, required=True)
    parser.add_argument("--canonical-root", type=Path, required=True)
    parser.add_argument("--direct-sync-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seconds", type=int, default=300)
    return parser.parse_args()


def main() -> int:
    args = _args()
    args.preimage = args.preimage.resolve(strict=True)
    args.server_db = args.server_db.resolve(strict=True)
    args.client_db = args.client_db.resolve(strict=True)
    args.stop_marker = args.stop_marker.resolve(strict=True)
    args.canonical_root = args.canonical_root.resolve(strict=False)
    args.direct_sync_root = args.direct_sync_root.resolve(strict=True)
    args.output = args.output.resolve(strict=False)
    if args.output.drive.casefold() != "e:" or args.output.exists() or args.seconds < 300:
        raise RuntimeError("proof output must be new on E: and window must be >=300s")
    preimage = json.loads(args.preimage.read_text(encoding="utf-8"))
    t0 = _snapshot(args, preimage)
    time.sleep(args.seconds)
    t1 = _snapshot(args, preimage)
    guard_names = [name for name in t0 if name.endswith("_sha256")]
    counts = ("old_fence_nonterminal_rows", "target_process_count", "target_runnable_launcher_count")
    elapsed = (
        datetime.fromisoformat(t1["db_now"].replace("Z", "+00:00"))
        - datetime.fromisoformat(t0["db_now"].replace("Z", "+00:00"))
    ).total_seconds()
    passed = bool(
        elapsed >= 300
        and all(t0[name] == t1[name] for name in guard_names)
        and all(t0[name] == 0 and t1[name] == 0 for name in counts)
        and t0["old_fence_logically_expired"] is True
        and t1["old_fence_logically_expired"] is True
        and t0["active_count"] == t1["active_count"] == 1
        and t0["server_schema_version"] == t1["server_schema_version"]
        and t0["client_schema_version"] == t1["client_schema_version"]
    )
    evidence_keys = {
        "lease_guard_sha256", "topology_guard_sha256", "quarantine_guard_sha256",
        "anchors_guard_sha256", "request_audit_guard_sha256",
        "credential_nonce_guard_sha256", "producer_nonce_guard_sha256",
        "client_authority_guard_sha256", "stop_marker_sha256",
        "old_fence_nonterminal_rows", "target_process_count",
        "target_runnable_launcher_count", "server_schema_version",
        "client_schema_version",
    }
    prior = dict(preimage["server"]["prior_active_lease"])
    credential = dict(preimage["current_credential"])
    proof = {
        "schema_version": SCHEMA,
        "query_version": QUERY_VERSION,
        "snapshot_isolation": "sqlite-explicit-read-transaction",
        "status": "PASS" if passed else "FAIL",
        "preimage_sha256": _sha256(args.preimage),
        "server_db_path": str(args.server_db),
        "client_db_path": str(args.client_db),
        "producer_install_id": preimage["producer_install_id"],
        "old_lease_id": prior["lease_id"],
        "old_runtime_instance_id": prior["runtime_instance_id"],
        "old_fence": prior["fence"],
        "producer_id": credential["producer_id"],
        "key_id": credential["key_id"],
        "observed_from": t0["db_now"],
        "observed_until": t1["db_now"],
        "observation_seconds": elapsed,
        "old_fence_logically_expired": t1["old_fence_logically_expired"],
        "server_target_state_unchanged": all(
            t0[name] == t1[name]
            for name in ("lease_guard_sha256", "topology_guard_sha256", "quarantine_guard_sha256", "anchors_guard_sha256")
        ),
        "server_request_activity_absent": all(
            t0[name] == t1[name]
            for name in ("request_audit_guard_sha256", "credential_nonce_guard_sha256", "producer_nonce_guard_sha256")
        ),
        "client_target_state_unchanged": t0["client_authority_guard_sha256"] == t1["client_authority_guard_sha256"],
        "old_fence_nonterminal_rows": t1["old_fence_nonterminal_rows"],
        "target_process_count": t1["target_process_count"],
        "target_runnable_launcher_count": t1["target_runnable_launcher_count"],
        "stop_marker_unchanged": t0["stop_marker_sha256"] == t1["stop_marker_sha256"],
        "evidence": {
            "t0": {name: t0[name] for name in evidence_keys},
            "t1": {name: t1[name] for name in evidence_keys},
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    _write_new(args.output, proof)
    print(json.dumps({"status": proof["status"], "output": str(args.output), "seconds": elapsed}, sort_keys=True))
    return 0 if passed else 4


if __name__ == "__main__":
    raise SystemExit(main())

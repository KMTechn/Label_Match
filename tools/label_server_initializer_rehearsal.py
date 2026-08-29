#!/usr/bin/env python3
"""Prove on an E: SQLite snapshot that deployed server initializers are no-ops."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import uuid


SCHEMA = "label-match-server-initializer-noop-proof-v1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _logical(connection: sqlite3.Connection) -> dict[str, object]:
    digest = hashlib.sha256(b"label-match-sqlite-logical-itertdump-v1\n")
    count = 0
    for statement in connection.iterdump():
        digest.update(statement.encode("utf-8"))
        digest.update(b"\n")
        count += 1
    return {
        "algorithm": "sha256-sqlite-itertdump-v1",
        "statement_count": count,
        "sha256": digest.hexdigest(),
    }


def _write_new(path: Path, value: dict[str, object]) -> None:
    payload = (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
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
    parser.add_argument("--live-server-db", type=Path, required=True)
    parser.add_argument("--server-source-root", type=Path, required=True)
    parser.add_argument("--producer-install-id", required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--rehearsal", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = _args()
    live = args.live_server_db.expanduser().resolve(strict=True)
    source_root = args.server_source_root.expanduser().resolve(strict=True)
    snapshot = args.snapshot.expanduser().resolve(strict=False)
    rehearsal = args.rehearsal.expanduser().resolve(strict=False)
    output = args.output.expanduser().resolve(strict=False)
    for selected in (snapshot, rehearsal, output):
        if selected.drive.casefold() != "e:" or selected.exists():
            raise RuntimeError("outputs must be new E: paths")
        selected.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(f"file:{live.as_posix()}?mode=ro", uri=True) as source:
        source.execute("PRAGMA query_only=ON")
        with sqlite3.connect(snapshot) as target:
            source.backup(target, pages=256, sleep=0.025)
        live_schema_version = int(source.execute("PRAGMA schema_version").fetchone()[0])
        live_journal_mode = str(source.execute("PRAGMA journal_mode").fetchone()[0])
        live_runtime_schema_version = int(
            source.execute(
                "SELECT schema_version FROM producer_runtime_lease_schema_meta WHERE singleton=1"
            ).fetchone()[0]
        )
    shutil.copy2(snapshot, rehearsal)

    source_text = str(source_root)
    if source_text not in sys.path:
        sys.path.insert(0, source_text)
    sys.dont_write_bytecode = True
    from producer_ingest import init_producer_ingest_schema  # noqa: PLC0415
    from producer_self_enrollment import init_self_enrollment_schema  # noqa: PLC0415

    connection = sqlite3.connect(rehearsal, timeout=30)
    try:
        before_integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        before_logical = _logical(connection)
        schema_before = int(connection.execute("PRAGMA schema_version").fetchone()[0])
        journal_before = str(connection.execute("PRAGMA journal_mode").fetchone()[0])
        changes_before = connection.total_changes
        init_self_enrollment_schema(connection)
        init_producer_ingest_schema(connection)
        total_changes = connection.total_changes - changes_before
        schema_after = int(connection.execute("PRAGMA schema_version").fetchone()[0])
        journal_after = str(connection.execute("PRAGMA journal_mode").fetchone()[0])
        after_logical = _logical(connection)
        runtime_schema_version = int(
            connection.execute(
                "SELECT schema_version FROM producer_runtime_lease_schema_meta WHERE singleton=1"
            ).fetchone()[0]
        )
        post_acquire_prunable_count = int(
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
                (args.producer_install_id,),
            ).fetchone()[0]
        )
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
    finally:
        connection.close()
    commit = subprocess.run(
        ["git", "-C", str(source_root), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip().lower()
    passed = bool(
        before_integrity == "ok"
        and integrity == "ok"
        and before_logical == after_logical
        and total_changes == 0
        and schema_before == schema_after
        and journal_before.lower() == "wal"
        and journal_after.lower() == "wal"
        and post_acquire_prunable_count == 0
    )
    proof: dict[str, object] = {
        "schema_version": SCHEMA,
        "status": "PASS" if passed else "FAIL",
        "observed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "live_server_db_path": str(live),
        "server_source_root": str(source_root),
        "server_source_commit": commit,
        "server_source_files": {
            name: _sha256(source_root / name)
            for name in (
                "producer_ingest.py",
                "producer_self_enrollment.py",
                "producer_runtime_lease.py",
                "producer_authz_provisioning.py",
            )
        },
        "snapshot_path": str(snapshot),
        "snapshot_sha256": _sha256(snapshot),
        "rehearsal_path": str(rehearsal),
        "rehearsal_sha256": _sha256(rehearsal),
        "initializer_calls": [
            "init_self_enrollment_schema",
            "init_producer_ingest_schema",
        ],
        "before_logical_digest": before_logical,
        "after_logical_digest": after_logical,
        "total_changes": total_changes,
        "schema_version_before": schema_before,
        "schema_version_after": schema_after,
        "live_schema_version": live_schema_version,
        "live_journal_mode": live_journal_mode,
        "live_runtime_schema_version": live_runtime_schema_version,
        "journal_mode_before": journal_before,
        "journal_mode_after": journal_after,
        "runtime_schema_version": runtime_schema_version,
        "producer_install_id": args.producer_install_id,
        "post_acquire_prunable_count": post_acquire_prunable_count,
        "integrity_check": integrity,
        "secret_material_included": False,
    }
    _write_new(output, proof)
    print(json.dumps({"status": proof["status"], "output": str(output)}, sort_keys=True))
    return 0 if passed else 4


if __name__ == "__main__":
    raise SystemExit(main())

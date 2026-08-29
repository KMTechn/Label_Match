import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import shutil
import sqlite3
import subprocess
from types import SimpleNamespace

import pytest

from label_exact_clone_resolution import (
    CONFLICT_CODE,
    ExactCloneResolutionError,
    capture_conflict_preimage,
    create_resolution_receipt,
    sqlite_logical_digest,
    validate_resolution_receipt,
)
from label_guarded_runtime_reconcile import (
    GuardedRuntimeReconcileError,
    LIVE_CLIENT_PROOF_SCHEMA,
    prepare_client_compare_and_swap,
    run_guarded_runtime_reconcile,
    validate_live_client_proof,
)
from producer_runtime_client import RuntimePreparation, init_runtime_schema
from user_relay_stop_marker import build_successor_marker, canonical_marker_bytes


INSTALL_ID = "install-label-fixture"
PRODUCER_ID = "producer-label-fixture"
ENDPOINT = "https://worker.example.test/api/producer-ingest/v1/source-file"
OLD_KEY = "key-old"
NEW_KEY = "key-current"
OLD_RUNTIME = "runtime-old"
NEW_RUNTIME = "runtime-current"
OLD_LEASE = "lease-old"
NEW_LEASE = "lease-current"
OLD_SCOPE = "a" * 64
NEW_SCOPE = "b" * 64
OLD_ISSUE = "runtime-lease-old"
CONFLICT_ISSUE = "runtime-lease-conflict"
NEW_ISSUE = "runtime-lease-current"
OLD_THUMBPRINT = "Q-XElAAqQ9DvHeC_qt8y3GnW4zcmxLe_8KK5rqaMAno"
NEW_THUMBPRINT = "FWj2hgcjRY93adE9AeEJivvzktpAHZ0jpGSKZw3vOco"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _live_proof_evidence(marker_sha256: str) -> dict:
    snapshot = {
        "lease_guard_sha256": "1" * 64,
        "topology_guard_sha256": "2" * 64,
        "quarantine_guard_sha256": "3" * 64,
        "anchors_guard_sha256": "4" * 64,
        "request_audit_guard_sha256": "5" * 64,
        "credential_nonce_guard_sha256": "6" * 64,
        "producer_nonce_guard_sha256": "7" * 64,
        "client_authority_guard_sha256": "8" * 64,
        "stop_marker_sha256": marker_sha256,
        "old_fence_nonterminal_rows": 0,
        "target_process_count": 0,
        "target_runnable_launcher_count": 0,
        "server_schema_version": 1,
        "client_schema_version": 1,
    }
    return {"t0": dict(snapshot), "t1": dict(snapshot)}


def _server_initializer_proof(tmp_path: Path, server_db_path: Path) -> Path:
    snapshot = tmp_path / "server-snapshot.sqlite3"
    rehearsal = tmp_path / "server-rehearsal.sqlite3"
    with sqlite3.connect(server_db_path) as source, sqlite3.connect(snapshot) as target:
        source.backup(target)
    shutil.copy2(snapshot, rehearsal)
    source_root = tmp_path / "server-source"
    source_root.mkdir()
    source_names = (
        "producer_ingest.py",
        "producer_self_enrollment.py",
        "producer_runtime_lease.py",
        "producer_authz_provisioning.py",
    )
    for name in source_names:
        (source_root / name).write_text(f"# {name} fixture\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q", str(source_root)], check=True)
    subprocess.run(["git", "-C", str(source_root), "add", "."], check=True)
    subprocess.run(
        [
            "git", "-C", str(source_root), "-c", "user.name=Fixture",
            "-c", "user.email=fixture@example.test", "commit", "-qm", "fixture",
        ],
        check=True,
    )
    source_commit = subprocess.run(
        ["git", "-C", str(source_root), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    logical = sqlite_logical_digest(snapshot)
    with sqlite3.connect(server_db_path) as live_connection:
        live_schema_version = int(
            live_connection.execute("PRAGMA schema_version").fetchone()[0]
        )
        live_journal_mode = str(
            live_connection.execute("PRAGMA journal_mode").fetchone()[0]
        )
        live_runtime_schema_version = int(
            live_connection.execute(
                "SELECT schema_version FROM producer_runtime_lease_schema_meta WHERE singleton=1"
            ).fetchone()[0]
        )
    proof_path = tmp_path / "server-init-proof.json"
    _write_json(
        proof_path,
        {
            "schema_version": "label-match-server-initializer-noop-proof-v1",
            "status": "PASS",
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "live_server_db_path": str(server_db_path.resolve()),
            "server_source_root": str(source_root.resolve()),
            "server_source_commit": source_commit,
            "server_source_files": {
                name: hashlib.sha256((source_root / name).read_bytes()).hexdigest()
                for name in source_names
            },
            "snapshot_path": str(snapshot.resolve()),
            "snapshot_sha256": hashlib.sha256(snapshot.read_bytes()).hexdigest(),
            "rehearsal_path": str(rehearsal.resolve()),
            "rehearsal_sha256": hashlib.sha256(rehearsal.read_bytes()).hexdigest(),
            "initializer_calls": [
                "init_self_enrollment_schema",
                "init_producer_ingest_schema",
            ],
            "before_logical_digest": logical,
            "after_logical_digest": logical,
            "total_changes": 0,
            "schema_version_before": 1,
            "schema_version_after": 1,
            "live_schema_version": live_schema_version,
            "live_journal_mode": live_journal_mode,
            "live_runtime_schema_version": live_runtime_schema_version,
            "journal_mode_before": "wal",
            "journal_mode_after": "wal",
            "runtime_schema_version": 3,
            "producer_install_id": INSTALL_ID,
            "post_acquire_prunable_count": 0,
            "integrity_check": "ok",
            "secret_material_included": False,
        },
    )
    return proof_path


def _fixture_initializer_replay(*, snapshot: Path, **_kwargs) -> dict:
    logical = sqlite_logical_digest(snapshot)
    return {
        "status": "PASS",
        "total_changes": 0,
        "before_logical_digest": logical,
        "after_logical_digest": logical,
    }


def _fixture_endpoint_binding(**_kwargs) -> dict:
    return {
        "status": "PASS",
        "listener_pid": 1234,
        "listener_created_at_epoch": 1.0,
        "server_db_path": "fixture",
        "server_source_commit": "fixture",
        "secret_material_included": False,
    }


def _public_jwk(seed: str) -> str:
    value = {
        "crv": "P-256",
        "kty": "EC",
        "x": f"x-{seed}",
        "y": f"y-{seed}",
    }
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _create_client_db(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        init_runtime_schema(connection)
        connection.execute(
            """
            CREATE TABLE direct_sync_relay_batches(
                relay_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                metadata_json TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "INSERT INTO direct_sync_relay_batches VALUES('relay-1','pending','{}')"
        )
        connection.execute(
            """
            INSERT INTO direct_sync_runtime_authority(
                authority_scope, endpoint_url, producer_id, key_id,
                producer_install_id, runtime_instance_id,
                runtime_public_jwk_json, lease_id, fence,
                next_request_token, next_request_sequence, expires_at,
                status, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 1, ?, 53, ?, 'ACTIVE', ?, ?)
            """,
            (
                OLD_SCOPE,
                ENDPOINT,
                PRODUCER_ID,
                OLD_KEY,
                INSTALL_ID,
                OLD_RUNTIME,
                _public_jwk("old"),
                OLD_LEASE,
                "old-request-token",
                "2026-08-29T00:00:00Z",
                "2026-08-28T00:00:00Z",
                "2026-08-28T00:00:00Z",
            ),
        )
        connection.execute(
            """
            INSERT INTO direct_sync_runtime_authority(
                authority_scope, endpoint_url, producer_id, key_id,
                producer_install_id, runtime_instance_id,
                runtime_public_jwk_json, pending_request_json,
                pending_issue_idempotency_key, status, last_error_code,
                created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, '{}', ?, 'OPERATOR_REVIEW', ?, ?, ?)
            """,
            (
                NEW_SCOPE,
                ENDPOINT,
                PRODUCER_ID,
                NEW_KEY,
                INSTALL_ID,
                NEW_RUNTIME,
                _public_jwk("current"),
                CONFLICT_ISSUE,
                CONFLICT_CODE,
                "2026-08-28T01:00:00Z",
                "2026-08-28T01:00:00Z",
            ),
        )
        connection.commit()


def _create_server_db(path: Path) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.executescript(
            """
            CREATE TABLE producer_runtime_lease_schema_meta(
                singleton INTEGER PRIMARY KEY,
                schema_version INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE producer_runtime_leases(
                lease_id TEXT PRIMARY KEY,
                producer_install_id TEXT,
                runtime_instance_id TEXT,
                public_jwk_thumbprint TEXT,
                issue_idempotency_key TEXT,
                request_fingerprint TEXT,
                fence INTEGER,
                current_request_sequence INTEGER,
                status TEXT,
                issued_at TEXT,
                expires_at TEXT,
                last_rotated_at TEXT,
                response_hash TEXT
            );
            CREATE TABLE producer_runtime_quarantine_audit(
                audit_id TEXT PRIMARY KEY,
                producer_install_id TEXT,
                runtime_instance_id TEXT,
                public_jwk_thumbprint TEXT,
                issue_idempotency_key TEXT,
                request_fingerprint TEXT,
                active_lease_id TEXT,
                active_runtime_instance_id TEXT,
                active_public_jwk_thumbprint TEXT,
                active_fence INTEGER,
                reason_code TEXT,
                occurred_at TEXT
            );
            CREATE TABLE producer_runtime_issue_anchors(
                producer_install_id TEXT,
                issue_idempotency_key TEXT,
                request_fingerprint TEXT,
                outcome TEXT,
                response_hash TEXT,
                lease_id TEXT,
                committed_at TEXT
            );
            CREATE TABLE producer_runtime_request_audit(
                audit_id TEXT PRIMARY KEY,
                producer_install_id TEXT,
                lease_id TEXT
            );
            """
        )
        connection.execute(
            "INSERT INTO producer_runtime_lease_schema_meta VALUES(1,3,'2026-08-28T00:00:00Z')"
        )
        connection.execute(
            """
            INSERT INTO producer_runtime_leases VALUES(
                ?, ?, ?, ?, ?, ?, 1, 53, 'ACTIVE', ?, ?, ?, ?
            )
            """,
            (
                OLD_LEASE,
                INSTALL_ID,
                OLD_RUNTIME,
                OLD_THUMBPRINT,
                OLD_ISSUE,
                "1" * 64,
                "2026-08-28T00:00:00Z",
                "2026-08-29T00:00:00Z",
                "2026-08-28T01:00:00Z",
                "2" * 64,
            ),
        )
        connection.execute(
            """
            INSERT INTO producer_runtime_quarantine_audit VALUES(
                'audit-1', ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?
            )
            """,
            (
                INSTALL_ID,
                NEW_RUNTIME,
                NEW_THUMBPRINT,
                CONFLICT_ISSUE,
                "3" * 64,
                OLD_LEASE,
                OLD_RUNTIME,
                OLD_THUMBPRINT,
                CONFLICT_CODE,
                "2026-08-28T01:00:00Z",
            ),
        )
        connection.execute(
            """
            INSERT INTO producer_runtime_issue_anchors VALUES(
                ?, ?, ?, 'ACTIVE', ?, ?, ?
            )
            """,
            (
                INSTALL_ID,
                OLD_ISSUE,
                "1" * 64,
                "4" * 64,
                OLD_LEASE,
                "2026-08-28T00:00:00Z",
            ),
        )
        connection.execute(
            """
            INSERT INTO producer_runtime_issue_anchors VALUES(
                ?, ?, ?, 'QUARANTINED', ?, NULL, ?
            )
            """,
            (
                INSTALL_ID,
                CONFLICT_ISSUE,
                "3" * 64,
                "5" * 64,
                "2026-08-28T01:00:00Z",
            ),
        )
        connection.commit()


def _paths(tmp_path: Path) -> dict[str, Path]:
    direct = tmp_path / "direct"
    client = direct / "queue" / "direct_sync_relay.sqlite3"
    client.parent.mkdir(parents=True)
    _create_client_db(client)
    server = tmp_path / "server.sqlite3"
    _create_server_db(server)
    identity = direct / "producer_identity.json"
    credential = direct / "credential.json"
    marker = direct / "control" / "label_match_user_relay.stop.json"
    _write_json(
        identity,
        {
            "schema_version": "label-match-producer-identity-v1",
            "producer_install_id": INSTALL_ID,
        },
    )
    _write_json(
        credential,
        {
            "producer_id": PRODUCER_ID,
            "key_id": NEW_KEY,
            "endpoint_url": ENDPOINT,
        },
    )
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_bytes(
        canonical_marker_bytes(
            {
            "schema_version": "label-match-user-relay-stop-v1",
            "request_id": "1" * 32,
            "requested_at": "2026-08-29T01:00:00+00:00",
            }
        )
    )
    portable = tmp_path / "portable"
    portable.mkdir()
    installer = portable / "INSTALL_CANONICAL_PORTABLE.ps1"
    installer.write_text("# fixture\n", encoding="utf-8")
    _write_json(
        portable / "portable-manifest.json",
        {
            "schema": "label-match-portable-tree-v1",
            "source_commit": "c" * 40,
            "source_tree": "d" * 40,
            "canonical_installer_sha256": hashlib.sha256(
                installer.read_bytes()
            ).hexdigest(),
        },
    )
    return {
        "client_db_path": client,
        "server_db_path": server,
        "identity_path": identity,
        "credential_path": credential,
        "stop_marker_path": marker,
        "portable_root": portable,
    }


def _resolve_fixture(paths: dict[str, Path]) -> None:
    with sqlite3.connect(paths["client_db_path"]) as connection:
        connection.execute(
            """
            UPDATE direct_sync_runtime_authority
               SET status='LEGACY_DISABLED', next_request_token=NULL,
                   next_request_sequence=NULL, assigned_relay_id=NULL,
                   pending_request_json=NULL,
                   pending_issue_idempotency_key=NULL, last_error_code=NULL
             WHERE authority_scope=?
            """,
            (OLD_SCOPE,),
        )
        connection.execute(
            """
            UPDATE direct_sync_runtime_authority
               SET lease_id=?, fence=2, next_request_token='current-token',
                   next_request_sequence=1, expires_at=?,
                   assigned_relay_id=NULL, pending_request_json=NULL,
                   pending_issue_idempotency_key=NULL,
                   status='ACTIVE', last_error_code=NULL
             WHERE authority_scope=?
            """,
            (NEW_LEASE, "2026-08-30T00:00:00Z", NEW_SCOPE),
        )
        connection.commit()
    with sqlite3.connect(paths["server_db_path"]) as connection:
        connection.execute(
            "UPDATE producer_runtime_leases SET status='EXPIRED' WHERE lease_id=?",
            (OLD_LEASE,),
        )
        connection.execute(
            """
            INSERT INTO producer_runtime_leases VALUES(
                ?, ?, ?, ?, ?, ?, 2, 1, 'ACTIVE', ?, ?, ?, ?
            )
            """,
            (
                NEW_LEASE,
                INSTALL_ID,
                NEW_RUNTIME,
                NEW_THUMBPRINT,
                NEW_ISSUE,
                "6" * 64,
                "2026-08-29T01:00:00Z",
                "2026-08-30T00:00:00Z",
                "2026-08-29T01:00:00Z",
                "7" * 64,
            ),
        )
        connection.execute(
            """
            INSERT INTO producer_runtime_issue_anchors VALUES(
                ?, ?, ?, 'ACTIVE', ?, ?, ?
            )
            """,
            (
                INSTALL_ID,
                NEW_ISSUE,
                "6" * 64,
                "8" * 64,
                NEW_LEASE,
                "2026-08-29T01:00:00Z",
            ),
        )
        connection.commit()


def test_capture_then_receipt_requires_exact_two_sided_resolution(tmp_path):
    paths = _paths(tmp_path)
    preimage = capture_conflict_preimage(**paths)

    assert preimage["status"] == "CONFLICT_CONFIRMED"
    assert preimage["client"]["candidate_authority"]["status"] == "OPERATOR_REVIEW"
    assert preimage["server"]["quarantine"]["reason_code"] == CONFLICT_CODE

    with pytest.raises(ExactCloneResolutionError, match="selected local authority"):
        create_resolution_receipt(preimage=preimage, **paths)

    _resolve_fixture(paths)
    receipt = create_resolution_receipt(preimage=preimage, **paths)
    readback = validate_resolution_receipt(
        receipt,
        client_db_path=paths["client_db_path"],
        identity_path=paths["identity_path"],
        credential_path=paths["credential_path"],
        stop_marker_path=paths["stop_marker_path"],
        portable_root=paths["portable_root"],
    )

    assert receipt["status"] == "RESOLVED"
    assert receipt["server"]["active_lease_count_after"] == 1
    assert receipt["server"]["prior_lease_after"]["status"] == "EXPIRED"
    assert receipt["invariants"]["relay_batches_unchanged"] is True
    assert readback["selected_lease_id"] == NEW_LEASE
    assert readback["selected_fence"] == 2


def test_receipt_accepts_only_a_verified_bounded_successor_marker(tmp_path):
    paths = _paths(tmp_path)
    preimage = capture_conflict_preimage(**paths)
    _resolve_fixture(paths)
    receipt = create_resolution_receipt(preimage=preimage, **paths)
    successor = build_successor_marker(
        paths["stop_marker_path"],
        request_id="2" * 32,
        requested_at="2026-08-29T02:00:00+00:00",
    )
    paths["stop_marker_path"].write_bytes(canonical_marker_bytes(successor))

    readback = validate_resolution_receipt(
        receipt,
        client_db_path=paths["client_db_path"],
        identity_path=paths["identity_path"],
        credential_path=paths["credential_path"],
        stop_marker_path=paths["stop_marker_path"],
        portable_root=paths["portable_root"],
    )
    assert readback["stop_marker_lineage"]["status"] == "SUCCESSOR"
    assert readback["stop_marker_lineage"]["successor_hops"] == 1

    successor["predecessor_sha256"] = "0" * 64
    paths["stop_marker_path"].write_bytes(canonical_marker_bytes(successor))
    with pytest.raises(ExactCloneResolutionError, match="lineage"):
        validate_resolution_receipt(
            receipt,
            client_db_path=paths["client_db_path"],
            identity_path=paths["identity_path"],
            credential_path=paths["credential_path"],
            stop_marker_path=paths["stop_marker_path"],
            portable_root=paths["portable_root"],
        )


def test_receipt_accepts_exact_portable_copy_at_canonical_root(tmp_path):
    paths = _paths(tmp_path)
    preimage = capture_conflict_preimage(**paths)
    _resolve_fixture(paths)
    receipt = create_resolution_receipt(preimage=preimage, **paths)
    canonical_root = tmp_path / "canonical" / "current"
    shutil.copytree(paths["portable_root"], canonical_root)

    with pytest.raises(ExactCloneResolutionError, match="packet binding differs"):
        validate_resolution_receipt(
            receipt,
            client_db_path=paths["client_db_path"],
            identity_path=paths["identity_path"],
            credential_path=paths["credential_path"],
            stop_marker_path=paths["stop_marker_path"],
            portable_root=canonical_root,
        )

    readback = validate_resolution_receipt(
        receipt,
        client_db_path=paths["client_db_path"],
        identity_path=paths["identity_path"],
        credential_path=paths["credential_path"],
        stop_marker_path=paths["stop_marker_path"],
        portable_root=canonical_root,
        allow_portable_relocation=True,
    )

    assert readback["portable_relocated"] is True
    assert Path(readback["portable_receipt_root"]) == paths["portable_root"].resolve()
    assert Path(readback["portable_validated_root"]) == canonical_root.resolve()

    (canonical_root / "INSTALL_CANONICAL_PORTABLE.ps1").write_text(
        "# tampered canonical copy\n",
        encoding="utf-8",
    )
    with pytest.raises(ExactCloneResolutionError, match="installer hash"):
        validate_resolution_receipt(
            receipt,
            client_db_path=paths["client_db_path"],
            identity_path=paths["identity_path"],
            credential_path=paths["credential_path"],
            stop_marker_path=paths["stop_marker_path"],
            portable_root=canonical_root,
            allow_portable_relocation=True,
        )

    shutil.copy2(
        paths["portable_root"] / "INSTALL_CANONICAL_PORTABLE.ps1",
        canonical_root / "INSTALL_CANONICAL_PORTABLE.ps1",
    )
    (paths["portable_root"] / "INSTALL_CANONICAL_PORTABLE.ps1").write_text(
        "# tampered receipt source\n",
        encoding="utf-8",
    )
    with pytest.raises(ExactCloneResolutionError, match="source portable"):
        validate_resolution_receipt(
            receipt,
            client_db_path=paths["client_db_path"],
            identity_path=paths["identity_path"],
            credential_path=paths["credential_path"],
            stop_marker_path=paths["stop_marker_path"],
            portable_root=canonical_root,
            allow_portable_relocation=True,
        )


def test_receipt_validation_rejects_secret_bearing_extra_field(tmp_path):
    paths = _paths(tmp_path)
    preimage = capture_conflict_preimage(**paths)
    _resolve_fixture(paths)
    receipt = create_resolution_receipt(preimage=preimage, **paths)
    receipt["server"]["runtime_request_token"] = "must-not-be-recorded"

    with pytest.raises(ExactCloneResolutionError, match="fields differ|forbidden"):
        validate_resolution_receipt(
            receipt,
            client_db_path=paths["client_db_path"],
            identity_path=paths["identity_path"],
            credential_path=paths["credential_path"],
            stop_marker_path=paths["stop_marker_path"],
            portable_root=paths["portable_root"],
        )


def test_guarded_client_prepare_is_exact_and_clears_old_request_authority(tmp_path):
    paths = _paths(tmp_path)
    preimage = capture_conflict_preimage(**paths)

    prepared = prepare_client_compare_and_swap(
        client_db_path=paths["client_db_path"],
        preimage=preimage,
        now_text="2026-08-29T02:00:00Z",
    )
    with sqlite3.connect(paths["client_db_path"]) as connection:
        connection.row_factory = sqlite3.Row
        old = connection.execute(
            "SELECT * FROM direct_sync_runtime_authority WHERE authority_scope=?",
            (OLD_SCOPE,),
        ).fetchone()
        current = connection.execute(
            "SELECT * FROM direct_sync_runtime_authority WHERE authority_scope=?",
            (NEW_SCOPE,),
        ).fetchone()

    assert prepared["status"] == "PREPARED"
    assert old["status"] == "LEGACY_DISABLED"
    assert old["lease_id"] is None
    assert old["next_request_token"] is None
    assert current["status"] == "PENDING"
    assert current["runtime_instance_id"] == NEW_RUNTIME
    assert current["pending_request_json"] is None

    with pytest.raises(Exception, match="changed before compare-and-swap"):
        prepare_client_compare_and_swap(
            client_db_path=paths["client_db_path"],
            preimage=preimage,
        )


def test_guarded_reconcile_uses_normal_acquirer_and_emits_real_receipt(tmp_path):
    paths = _paths(tmp_path)
    preimage = capture_conflict_preimage(**paths)
    preimage_path = tmp_path / "preimage.json"
    _write_json(preimage_path, preimage)
    backup_path = tmp_path / "client-backup.sqlite3"
    with sqlite3.connect(paths["client_db_path"]) as source, sqlite3.connect(
        backup_path
    ) as target:
        source.backup(target)
    now = datetime.now(timezone.utc)
    proof = {
        "schema_version": LIVE_CLIENT_PROOF_SCHEMA,
        "query_version": "label-match-old-fence-liveness-query-v3",
        "snapshot_isolation": "sqlite-explicit-read-transaction",
        "status": "PASS",
        "preimage_sha256": hashlib.sha256(preimage_path.read_bytes()).hexdigest(),
        "server_db_path": str(paths["server_db_path"].resolve()),
        "client_db_path": str(paths["client_db_path"].resolve()),
        "producer_install_id": INSTALL_ID,
        "old_lease_id": OLD_LEASE,
        "old_runtime_instance_id": OLD_RUNTIME,
        "old_fence": 1,
        "producer_id": PRODUCER_ID,
        "key_id": NEW_KEY,
        "observed_from": (now - timedelta(seconds=301)).isoformat(),
        "observed_until": now.isoformat(),
        "observation_seconds": 301,
        "old_fence_logically_expired": True,
        "server_target_state_unchanged": True,
        "server_request_activity_absent": True,
        "client_target_state_unchanged": True,
        "old_fence_nonterminal_rows": 0,
        "target_process_count": 0,
        "target_runnable_launcher_count": 0,
        "stop_marker_unchanged": True,
        "evidence": _live_proof_evidence(preimage["stop_marker"]["sha256"]),
    }
    proof_path = tmp_path / "proof.json"
    _write_json(proof_path, proof)
    receipt_path = tmp_path / "receipt.json"
    server_initializer_proof = _server_initializer_proof(
        tmp_path, paths["server_db_path"]
    )
    initializer_value = json.loads(server_initializer_proof.read_text(encoding="utf-8"))
    ca_path = tmp_path / "ca-bundle.pem"
    ca_path.write_text("fixture-ca\n", encoding="utf-8")

    def acquire(**_kwargs):
        _kwargs["session"].last_pre_dispatch_guard = {"status": "PASS"}
        _kwargs["session"].post_count = 1
        _resolve_fixture(paths)
        return RuntimePreparation(
            status_code=200,
            receipt={
                "status": "ACTIVE",
                "server_grant_accepted": True,
                "lease_id": NEW_LEASE,
                "fence": 2,
            },
        )

    result = run_guarded_runtime_reconcile(
        preimage_path=preimage_path,
        client_backup_path=backup_path,
        client_backup_sha256=hashlib.sha256(backup_path.read_bytes()).hexdigest(),
        live_client_proof_path=proof_path,
        live_client_proof_sha256=hashlib.sha256(proof_path.read_bytes()).hexdigest(),
        server_initializer_proof_path=server_initializer_proof,
        server_initializer_proof_sha256=hashlib.sha256(
            server_initializer_proof.read_bytes()
        ).hexdigest(),
        server_source_root=initializer_value["server_source_root"],
        server_source_commit=initializer_value["server_source_commit"],
        server_launcher_path=initializer_value["server_source_root"],
        server_launcher_sha256="1" * 64,
        server_executable_path=initializer_value["server_source_root"],
        server_executable_sha256="2" * 64,
        client_db_path=paths["client_db_path"],
        server_db_path=paths["server_db_path"],
        identity_path=paths["identity_path"],
        credential_path=paths["credential_path"],
        stop_marker_path=paths["stop_marker_path"],
        portable_root=paths["portable_root"],
        receipt_output_path=receipt_path,
        tls_ca_bundle_path=ca_path,
        tls_ca_bundle_sha256=hashlib.sha256(ca_path.read_bytes()).hexdigest(),
        credential_loader=lambda _path: SimpleNamespace(
            runtime_lease_mode="enforce",
            producer_id=PRODUCER_ID,
            key_id=NEW_KEY,
            endpoint_url=ENDPOINT,
        ),
        authority_acquirer=acquire,
        initializer_replayer=_fixture_initializer_replay,
        endpoint_validator=_fixture_endpoint_binding,
    )

    assert result["status"] == "RESOLVED"
    assert result["server_transition"]["forward"] is True
    assert result["selected_fence"] == 2
    assert result["stop_marker_removed"] is False
    assert json.loads(receipt_path.read_text(encoding="utf-8"))["status"] == "RESOLVED"


def test_live_client_proof_rejects_a_claimed_window_that_does_not_match_times():
    now = datetime.now(timezone.utc)
    proof = {
        "schema_version": LIVE_CLIENT_PROOF_SCHEMA,
        "query_version": "label-match-old-fence-liveness-query-v3",
        "snapshot_isolation": "sqlite-explicit-read-transaction",
        "status": "PASS",
        "preimage_sha256": "a" * 64,
        "server_db_path": r"E:\fixture-server.sqlite3",
        "client_db_path": r"E:\fixture-client.sqlite3",
        "producer_install_id": INSTALL_ID,
        "old_lease_id": OLD_LEASE,
        "old_runtime_instance_id": OLD_RUNTIME,
        "old_fence": 1,
        "producer_id": PRODUCER_ID,
        "key_id": NEW_KEY,
        "observed_from": (now - timedelta(seconds=10)).isoformat(),
        "observed_until": now.isoformat(),
        "observation_seconds": 301,
        "old_fence_logically_expired": True,
        "server_target_state_unchanged": True,
        "server_request_activity_absent": True,
        "client_target_state_unchanged": True,
        "old_fence_nonterminal_rows": 0,
        "target_process_count": 0,
        "target_runnable_launcher_count": 0,
        "stop_marker_unchanged": True,
        "evidence": _live_proof_evidence("b" * 64),
    }

    with pytest.raises(GuardedRuntimeReconcileError, match="interval differs"):
        validate_live_client_proof(
            proof,
            preimage_sha256="a" * 64,
            stop_marker_sha256="b" * 64,
            server_db_path=r"E:\fixture-server.sqlite3",
            client_db_path=r"E:\fixture-client.sqlite3",
            producer_install_id=INSTALL_ID,
            old_lease_id=OLD_LEASE,
            old_runtime_instance_id=OLD_RUNTIME,
            old_fence=1,
            producer_id=PRODUCER_ID,
            key_id=NEW_KEY,
            now=now,
        )


def test_guarded_reconcile_keeps_client_fail_closed_after_dispatch_ambiguity(tmp_path):
    paths = _paths(tmp_path)
    preimage = capture_conflict_preimage(**paths)
    preimage_path = tmp_path / "preimage.json"
    _write_json(preimage_path, preimage)
    backup_path = tmp_path / "client-backup.sqlite3"
    with sqlite3.connect(paths["client_db_path"]) as source, sqlite3.connect(
        backup_path
    ) as target:
        source.backup(target)
    now = datetime.now(timezone.utc)
    proof_path = tmp_path / "proof.json"
    _write_json(
        proof_path,
        {
            "schema_version": LIVE_CLIENT_PROOF_SCHEMA,
            "query_version": "label-match-old-fence-liveness-query-v3",
            "snapshot_isolation": "sqlite-explicit-read-transaction",
            "status": "PASS",
            "preimage_sha256": hashlib.sha256(preimage_path.read_bytes()).hexdigest(),
            "server_db_path": str(paths["server_db_path"].resolve()),
            "client_db_path": str(paths["client_db_path"].resolve()),
            "producer_install_id": INSTALL_ID,
            "old_lease_id": OLD_LEASE,
            "old_runtime_instance_id": OLD_RUNTIME,
            "old_fence": 1,
            "producer_id": PRODUCER_ID,
            "key_id": NEW_KEY,
            "observed_from": (now - timedelta(seconds=301)).isoformat(),
            "observed_until": now.isoformat(),
            "observation_seconds": 301,
            "old_fence_logically_expired": True,
            "server_target_state_unchanged": True,
            "server_request_activity_absent": True,
            "client_target_state_unchanged": True,
            "old_fence_nonterminal_rows": 0,
            "target_process_count": 0,
            "target_runnable_launcher_count": 0,
            "stop_marker_unchanged": True,
            "evidence": _live_proof_evidence(preimage["stop_marker"]["sha256"]),
        },
    )
    server_initializer_proof = _server_initializer_proof(
        tmp_path, paths["server_db_path"]
    )
    initializer_value = json.loads(server_initializer_proof.read_text(encoding="utf-8"))
    ca_path = tmp_path / "ca-bundle.pem"
    ca_path.write_text("fixture-ca\n", encoding="utf-8")

    def reject(**_kwargs):
        _kwargs["session"].last_pre_dispatch_guard = {"status": "PASS"}
        _kwargs["session"].post_count = 1
        return RuntimePreparation(
            operator_review=True,
            error_code="fixture_rejected",
            error_message="fixture rejection before server advance",
        )

    with pytest.raises(
        GuardedRuntimeReconcileError, match="did not prove the exact forward"
    ) as caught:
        run_guarded_runtime_reconcile(
            preimage_path=preimage_path,
            client_backup_path=backup_path,
            client_backup_sha256=hashlib.sha256(backup_path.read_bytes()).hexdigest(),
            live_client_proof_path=proof_path,
            live_client_proof_sha256=hashlib.sha256(proof_path.read_bytes()).hexdigest(),
            server_initializer_proof_path=server_initializer_proof,
            server_initializer_proof_sha256=hashlib.sha256(
                server_initializer_proof.read_bytes()
            ).hexdigest(),
            server_source_root=initializer_value["server_source_root"],
            server_source_commit=initializer_value["server_source_commit"],
            server_launcher_path=initializer_value["server_source_root"],
            server_launcher_sha256="1" * 64,
            server_executable_path=initializer_value["server_source_root"],
            server_executable_sha256="2" * 64,
            client_db_path=paths["client_db_path"],
            server_db_path=paths["server_db_path"],
            identity_path=paths["identity_path"],
            credential_path=paths["credential_path"],
            stop_marker_path=paths["stop_marker_path"],
            portable_root=paths["portable_root"],
            receipt_output_path=tmp_path / "receipt.json",
            tls_ca_bundle_path=ca_path,
            tls_ca_bundle_sha256=hashlib.sha256(ca_path.read_bytes()).hexdigest(),
            credential_loader=lambda _path: SimpleNamespace(
                runtime_lease_mode="enforce",
                producer_id=PRODUCER_ID,
                key_id=NEW_KEY,
                endpoint_url=ENDPOINT,
            ),
            authority_acquirer=reject,
            initializer_replayer=_fixture_initializer_replay,
            endpoint_validator=_fixture_endpoint_binding,
        )

    assert caught.value.server_forward is True
    with sqlite3.connect(paths["client_db_path"]) as connection:
        statuses = dict(
            connection.execute(
                "SELECT authority_scope,status FROM direct_sync_runtime_authority"
            ).fetchall()
        )
    assert statuses == {OLD_SCOPE: "LEGACY_DISABLED", NEW_SCOPE: "PENDING"}
    assert not (tmp_path / "receipt.json").exists()

import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

from label_exact_clone_resolution import (
    CONFLICT_CODE,
    ExactCloneResolutionError,
    capture_conflict_preimage,
    create_resolution_receipt,
    validate_resolution_receipt,
)
from producer_runtime_client import init_runtime_schema


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
        connection.executescript(
            """
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
            """
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
    _write_json(marker, {"request_id": "stop-request-1"})
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

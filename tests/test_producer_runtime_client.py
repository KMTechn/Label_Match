from __future__ import annotations

import hashlib
import hmac
import json
import re
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import direct_sync_push
import producer_runtime_client as runtime_client
from direct_sync_push import ProducerCredentials, canonical_json, init_relay_queue_schema


WORKER_ANALYSIS_GUI_ROOT = Path(__file__).resolve().parents[2] / "WorkerAnalysisGUI-web"
PRODUCER_RUNTIME_LEASE_MODULE = WORKER_ANALYSIS_GUI_ROOT / "producer_runtime_lease.py"


@dataclass
class _Response:
    status_code: int
    payload: dict
    headers: dict | None = None

    def json(self):
        return self.payload


class _LeaseSession:
    def __init__(self, *, failures: int = 0):
        self.failures = failures
        self.calls = []

    def post(self, url, **kwargs):
        body = json.loads(bytes(kwargs["data"]).decode("utf-8"))
        self.calls.append((url, body, dict(kwargs["headers"])))
        if len(self.calls) <= self.failures:
            raise TimeoutError("lost lease ACK")
        sequence = int(body.get("runtime_request_sequence") or 0) + 1
        return _Response(
            200,
            {
                "ok": True,
                "status": "ACTIVE",
                "contract_version": runtime_client.CONTRACT_VERSION,
                "operation": "renewed" if "runtime_fence" in body else "issued",
                "lease_id": "lease-test",
                "producer_install_id": "install-test",
                "runtime_instance_id": body["runtime_instance_id"],
                "public_jwk_thumbprint": runtime_client._jwk_thumbprint(body["public_jwk"]),
                "issue_idempotency_key": body["issue_idempotency_key"],
                "fence": int(body.get("runtime_fence") or 1),
                "issued_at": "2026-08-06T00:00:00Z",
                "expires_at": "2099-08-06T00:00:00Z",
                "next_request_token": ("B" if sequence > 1 else "A") * 43,
                "next_request_sequence": sequence,
            },
            {},
        )


class _RelaySession(_LeaseSession):
    def __init__(self, *, source_failures: int = 0, lease_failures: int = 0):
        super().__init__(failures=lease_failures)
        self.source_failures = source_failures
        self.source_calls = []

    def post(self, url, **kwargs):
        if str(url).endswith(runtime_client.ENDPOINT_PATH):
            return super().post(url, **kwargs)
        uploaded = json.loads(kwargs["data"]["metadata"])
        self.source_calls.append((uploaded, dict(kwargs["headers"])))
        if len(self.source_calls) <= self.source_failures:
            raise TimeoutError("lost source ACK")
        source_file_id = (
            f"{uploaded['source_host_id']}/{uploaded['producer_role']}/"
            f"{uploaded['stream_name']}/{uploaded['relative_path']}"
        )
        return _Response(
            200,
            {
                "request_id": "request-relay-a",
                "upload_id": "request-relay-a",
                "producer_install_id": "install-test",
                "client_batch_id": uploaded["client_batch_id"],
                "server_source_file_id": source_file_id,
                "committed": True,
                "status": "accepted",
                "retryable": False,
                "next_retry_after": None,
                "totals": {"inserted": 0, "replayed": 0, "quarantined": 0, "errors": 0},
                "runtime_lease": {
                    "contract_version": runtime_client.CONTRACT_VERSION,
                    "validation_status": "consumed",
                    "lease_id": "lease-test",
                    "fence": uploaded["runtime_fence"],
                    "next_request_token": "R" * 43,
                    "next_request_sequence": uploaded["runtime_request_sequence"] + 1,
                    "expires_at": "2099-08-06T00:00:00Z",
                },
            },
            {},
        )


class _DefinitiveRejectSession(_LeaseSession):
    def __init__(self):
        super().__init__()
        self.source_calls = []

    def post(self, url, **kwargs):
        if str(url).endswith(runtime_client.ENDPOINT_PATH):
            return super().post(url, **kwargs)
        uploaded = json.loads(kwargs["data"]["metadata"])
        self.source_calls.append(uploaded)
        return _Response(
            422,
            {
                "committed": False,
                "retryable": False,
                "status": "rejected",
                "error": {"code": "source_file_invalid", "message": "invalid source file"},
            },
            {},
        )


class _GenericLegacyRejectSession(_LeaseSession):
    def __init__(self, error_code: str):
        super().__init__()
        self.error_code = error_code
        self.source_calls = []

    def post(self, url, **kwargs):
        if str(url).endswith(runtime_client.ENDPOINT_PATH):
            return super().post(url, **kwargs)
        self.source_calls.append(json.loads(kwargs["data"]["metadata"]))
        return _Response(
            409 if self.error_code == "idempotency_conflict" else 400,
            {
                "committed": False,
                "retryable": False,
                "error": {"code": self.error_code, "message": "source request rejected"},
            },
            {},
        )


class _LegacyReceiptSession(_LeaseSession):
    def __init__(self, *, source_failures: int = 0):
        super().__init__()
        self.source_failures = source_failures
        self.source_calls = []
        self.source_urls = []

    def post(self, url, **kwargs):
        if str(url).endswith(runtime_client.ENDPOINT_PATH):
            return super().post(url, **kwargs)
        uploaded = json.loads(kwargs["data"]["metadata"])
        self.source_calls.append(uploaded)
        self.source_urls.append(str(url))
        if len(self.source_calls) <= self.source_failures:
            raise TimeoutError("lost legacy source ACK")
        source_file_id = (
            f"{uploaded['source_host_id']}/{uploaded['producer_role']}/"
            f"{uploaded['stream_name']}/{uploaded['relative_path']}"
        )
        receipt = {
                "request_id": f"request-{uploaded['client_batch_id']}",
                "upload_id": f"request-{uploaded['client_batch_id']}",
                "producer_install_id": "install-test",
                "client_batch_id": uploaded["client_batch_id"],
                "server_source_file_id": source_file_id,
                "committed": True,
                "status": "accepted",
                "retryable": False,
                "next_retry_after": None,
                "totals": {"inserted": 0, "replayed": 0, "quarantined": 0, "errors": 0},
            }
        if "runtime_request_token" not in uploaded:
            receipt["runtime_lease"] = {
                "contract_version": runtime_client.CONTRACT_VERSION,
                "validation_status": "observed",
                "reason_code": "RUNTIME_LEASE_MISSING_OBSERVED",
            }
        return _Response(200, receipt, {})


def _credentials(
    *, key_id: str = "key-test", runtime_lease_mode: str = "enforce"
) -> ProducerCredentials:
    return ProducerCredentials(
        producer_id="producer-test",
        key_id=key_id,
        secret=b"runtime-client-secret",
        endpoint_url="https://producer.example/api/producer-ingest/v1/source-file",
        runtime_lease_mode=runtime_lease_mode,
    )


def _metadata(relay_id: str) -> dict:
    return {
        "contract_version": "producer-ingest-source-file-v1",
        "producer_install_id": "install-test",
        "client_batch_id": relay_id,
        "idempotency_key": relay_id,
        "source_host_id": "host-test",
        "producer_role": "test-role",
        "manifest_hash": "a" * 64,
        "stream_name": "test-stream",
        "source_system": "test-system",
        "source_transport": "test-transport",
        "relative_path": f"events/{relay_id}.csv",
        "batch_kind": "whole_file",
        "row_count": 0,
        "first_row_number": 0,
        "last_row_number": 0,
        "content_sha256": "b" * 64,
        "byte_length": 0,
    }


def test_idle_liveness_issues_once_and_renews_only_when_due_without_consuming_token(tmp_path):
    db_path = tmp_path / "relay.sqlite3"
    session = _LeaseSession()

    issued = runtime_client.ensure_runtime_authority(
        db_path=db_path,
        credentials=_credentials(),
        producer_install_id="install-test",
        session=session,
        now="2026-08-06T00:00:00Z",
    )
    assert issued.error_code == ""
    assert issued.receipt["server_grant_accepted"] is True
    assert issued.receipt["request_sent"] is True
    assert len(session.calls) == 1
    assert "runtime_request_token" not in session.calls[0][1]

    current = runtime_client.ensure_runtime_authority(
        db_path=db_path,
        credentials=_credentials(),
        producer_install_id="install-test",
        session=session,
        now="2026-08-06T00:01:00Z",
    )
    assert current.error_code == ""
    assert current.receipt["request_sent"] is False
    assert len(session.calls) == 1
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT next_request_token, next_request_sequence, assigned_relay_id "
            "FROM direct_sync_runtime_authority"
        ).fetchone()
    assert row == ("A" * 43, 1, None)

    renewed = runtime_client.ensure_runtime_authority(
        db_path=db_path,
        credentials=_credentials(),
        producer_install_id="install-test",
        session=session,
        now="2099-08-05T23:59:30Z",
        renewal_margin_seconds=60,
    )
    assert renewed.error_code == ""
    assert renewed.receipt["request_sent"] is True
    assert len(session.calls) == 2
    renewal_body = session.calls[1][1]
    assert renewal_body["runtime_request_token"] == "A" * 43
    assert renewal_body["runtime_request_sequence"] == 1
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT next_request_token, next_request_sequence, assigned_relay_id "
            "FROM direct_sync_runtime_authority"
        ).fetchone()
    assert row == ("B" * 43, 2, None)


def _insert_claimed_row(db_path: Path, relay_id: str, *, owner: str = "worker") -> None:
    init_relay_queue_schema(db_path)
    metadata = _metadata(relay_id)
    now = "2026-08-06T00:00:00Z"
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            INSERT INTO direct_sync_relay_batches(
                relay_id, status, source_file_path, spooled_file_path,
                producer_manifest_path, relative_path, content_sha256,
                byte_length, attempt_count, lease_owner, lease_expires_at,
                next_attempt_at, metadata_json, producer_id, key_id,
                endpoint_url, created_at, updated_at
            ) VALUES(?, 'leased', '', '', '', ?, ?, 0, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                relay_id,
                metadata["relative_path"],
                metadata["content_sha256"],
                owner,
                "2099-08-06T00:00:00Z",
                now,
                canonical_json(metadata),
                "producer-test",
                "key-test",
                _credentials().endpoint_url,
                now,
                now,
            ),
        )


def _prepare(db_path: Path, relay_id: str, session, *, owner: str = "worker"):
    return runtime_client.prepare_runtime_metadata(
        db_path=db_path,
        relay_id=relay_id,
        metadata=_metadata(relay_id),
        credentials=_credentials(),
        expected_lease_owner=owner,
        expected_attempt_count=1,
        session=session,
        timeout=5,
        now="2026-08-06T00:00:00Z",
    )


def _make_pending_relay(
    db_path: Path,
    tmp_path: Path,
    relay_id: str,
    *,
    attempt_count: int = 0,
    runtime_fencing_policy: str = runtime_client.RUNTIME_FENCING_POLICY_RUNTIME_REQUIRED,
) -> None:
    _insert_claimed_row(db_path, relay_id)
    spool_path = tmp_path / f"{relay_id}.csv"
    spool_path.write_bytes(b"")
    empty_sha256 = hashlib.sha256(b"").hexdigest()
    metadata = _metadata(relay_id)
    metadata["content_sha256"] = empty_sha256
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            UPDATE direct_sync_relay_batches
            SET status='pending', attempt_count=?, lease_owner=NULL, lease_expires_at=NULL,
                next_attempt_at='2000-01-01T00:00:00Z',
                source_file_path=?, spooled_file_path=?, content_sha256=?,
                metadata_json=?, runtime_fencing_policy=?
            WHERE relay_id=?
            """,
            (
                attempt_count,
                str(spool_path),
                str(spool_path),
                empty_sha256,
                canonical_json(metadata),
                runtime_fencing_policy,
                relay_id,
            ),
        )


def _assert_signed_lease_call(call) -> None:
    url, body, headers = call
    assert url.endswith(runtime_client.ENDPOINT_PATH)
    canonical = runtime_client._canonical_request(
        timestamp=headers["X-Producer-Timestamp"],
        nonce=headers["X-Producer-Nonce"],
        producer_id=headers["X-Producer-Id"],
        key_id=headers["X-Producer-Key-Id"],
        body=body,
    )
    expected = hmac.new(
        _credentials().secret, canonical.encode("utf-8"), hashlib.sha256
    ).hexdigest()
    assert hmac.compare_digest(expected, headers["X-Producer-Signature"])


def test_lease_is_hmac_signed_and_runtime_metadata_is_persisted_all_or_none(tmp_path):
    db_path = tmp_path / "relay.sqlite3"
    _insert_claimed_row(db_path, "relay-a")
    session = _LeaseSession()

    prepared = _prepare(db_path, "relay-a", session)
    replay = _prepare(db_path, "relay-a", session)

    assert prepared.metadata is not None
    assert replay.metadata == prepared.metadata
    assert len(session.calls) == 1
    _assert_signed_lease_call(session.calls[0])
    assert set(runtime_client.METADATA_FIELDS).issubset(prepared.metadata)
    assert runtime_client._metadata_shape_error(prepared.metadata) == ""
    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        batch = connection.execute(
            "SELECT metadata_json FROM direct_sync_relay_batches WHERE relay_id='relay-a'"
        ).fetchone()
        state = connection.execute(
            "SELECT * FROM direct_sync_runtime_authority"
        ).fetchone()
    assert json.loads(batch["metadata_json"]) == prepared.metadata
    assert state["assigned_relay_id"] == "relay-a"
    assert state["next_request_token"] is None
    assert state["endpoint_url"] == _credentials().endpoint_url
    assert state["producer_id"] == _credentials().producer_id
    assert state["key_id"] == _credentials().key_id
    assert state["producer_install_id"] == "install-test"


def test_partial_runtime_metadata_fails_closed_before_any_network_call(tmp_path):
    db_path = tmp_path / "relay.sqlite3"
    _insert_claimed_row(db_path, "relay-a")
    partial = _metadata("relay-a")
    partial["runtime_instance_id"] = "runtime-partial"
    session = _LeaseSession()

    result = runtime_client.prepare_runtime_metadata(
        db_path=db_path,
        relay_id="relay-a",
        metadata=partial,
        credentials=_credentials(),
        expected_lease_owner="worker",
        expected_attempt_count=1,
        session=session,
    )

    assert result.metadata is None
    assert result.operator_review is True
    assert result.error_code == "runtime_lease_metadata_invalid"
    assert session.calls == []


def test_lost_lease_ack_reuses_exact_body_with_a_fresh_hmac_nonce(tmp_path):
    db_path = tmp_path / "relay.sqlite3"
    _insert_claimed_row(db_path, "relay-a")
    session = _LeaseSession(failures=1)

    first = _prepare(db_path, "relay-a", session)
    second = _prepare(db_path, "relay-a", session)

    assert first.retryable is True
    assert second.metadata is not None
    assert session.calls[0][1] == session.calls[1][1]
    assert session.calls[0][2]["X-Producer-Nonce"] != session.calls[1][2]["X-Producer-Nonce"]
    _assert_signed_lease_call(session.calls[0])
    _assert_signed_lease_call(session.calls[1])


def test_expiring_authority_is_renewed_with_the_current_fence_token_and_sequence(tmp_path):
    db_path = tmp_path / "relay.sqlite3"
    _insert_claimed_row(db_path, "seed")
    seed = _prepare(db_path, "seed", _LeaseSession())
    assert seed.metadata is not None
    rotation = {
        "contract_version": runtime_client.CONTRACT_VERSION,
        "validation_status": "consumed",
        "lease_id": "lease-test",
        "fence": seed.metadata["runtime_fence"],
        "next_request_token": "C" * 43,
        "next_request_sequence": 2,
        "expires_at": "2026-08-06T00:00:20Z",
    }
    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("UPDATE direct_sync_relay_batches SET status='acked' WHERE relay_id='seed'")
        runtime_client.apply_runtime_receipt_in_transaction(
            connection,
            relay_id="seed",
            metadata=seed.metadata,
            credentials=_credentials(),
            runtime_lease=rotation,
            now="2026-08-06T00:00:01Z",
        )
        connection.commit()
    _insert_claimed_row(db_path, "relay-renew", owner="worker-renew")
    session = _LeaseSession()

    renewed = runtime_client.prepare_runtime_metadata(
        db_path=db_path,
        relay_id="relay-renew",
        metadata=_metadata("relay-renew"),
        credentials=_credentials(),
        expected_lease_owner="worker-renew",
        expected_attempt_count=1,
        session=session,
        timeout=5,
        now="2026-08-06T00:00:02Z",
    )

    assert renewed.metadata is not None
    renewal_body = session.calls[0][1]
    assert renewal_body["runtime_fence"] == seed.metadata["runtime_fence"]
    assert renewal_body["runtime_request_token"] == "C" * 43
    assert renewal_body["runtime_request_sequence"] == 2
    assert renewal_body["runtime_instance_id"] == seed.metadata["runtime_instance_id"]
    assert renewal_body["public_jwk"] == seed.metadata["runtime_public_jwk"]
    assert renewed.metadata["runtime_request_sequence"] == 3
    _assert_signed_lease_call(session.calls[0])


def test_ack_and_next_token_commit_together_and_status_redaction_removes_tokens(tmp_path):
    db_path = tmp_path / "relay.sqlite3"
    _insert_claimed_row(db_path, "relay-a")
    prepared = _prepare(db_path, "relay-a", _LeaseSession())
    assert prepared.metadata is not None
    current_token = prepared.metadata["runtime_request_token"]
    next_token = "N" * 43
    rotation = {
        "contract_version": runtime_client.CONTRACT_VERSION,
        "validation_status": "consumed",
        "lease_id": "lease-test",
        "fence": prepared.metadata["runtime_fence"],
        "next_request_token": next_token,
        "next_request_sequence": prepared.metadata["runtime_request_sequence"] + 1,
        "expires_at": "2099-08-06T00:00:00Z",
    }
    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "UPDATE direct_sync_relay_batches SET status='acked' WHERE relay_id='relay-a'"
        )
        runtime_client.apply_runtime_receipt_in_transaction(
            connection,
            relay_id="relay-a",
            metadata=prepared.metadata,
            credentials=_credentials(),
            runtime_lease=rotation,
            now="2026-08-06T00:00:01Z",
        )
        connection.commit()
    with sqlite3.connect(db_path) as connection:
        batch_status = connection.execute(
            "SELECT status FROM direct_sync_relay_batches WHERE relay_id='relay-a'"
        ).fetchone()[0]
        state = connection.execute(
            "SELECT next_request_token, next_request_sequence, assigned_relay_id "
            "FROM direct_sync_runtime_authority"
        ).fetchone()
    assert batch_status == "acked"
    assert state == (next_token, 2, None)
    artifact = {
        "metadata": runtime_client.redact_runtime_secrets(prepared.metadata),
        "receipt": runtime_client.redact_runtime_secrets(
            {"runtime_lease": rotation, "message": f"rotated to {next_token}"}
        ),
    }
    artifact_text = json.dumps(artifact, sort_keys=True)
    assert current_token not in artifact_text
    assert next_token not in artifact_text
    assert artifact["metadata"]["runtime_request_token"] == "[redacted]"
    assert artifact["receipt"]["runtime_lease"]["next_request_token"] == "[redacted]"
    assert artifact["receipt"]["message"] == "rotated to [redacted]"


def test_real_relay_drain_rotates_authority_in_the_ack_transaction(tmp_path, monkeypatch):
    db_path = tmp_path / "relay.sqlite3"
    _insert_claimed_row(db_path, "relay-a")
    spool_path = tmp_path / "relay-a.csv"
    spool_path.write_bytes(b"")
    empty_sha256 = hashlib.sha256(b"").hexdigest()
    metadata = _metadata("relay-a")
    metadata["content_sha256"] = empty_sha256
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            UPDATE direct_sync_relay_batches
            SET status='pending', attempt_count=0, lease_owner=NULL, lease_expires_at=NULL,
                next_attempt_at='2000-01-01T00:00:00Z',
                source_file_path=?, spooled_file_path=?, content_sha256=?,
                metadata_json=?
            WHERE relay_id='relay-a'
            """,
            (str(spool_path), str(spool_path), empty_sha256, canonical_json(metadata)),
        )

    session = _RelaySession()
    status_dir = tmp_path / "status"
    observed_transaction = []
    original_apply = direct_sync_push.apply_runtime_receipt_in_transaction

    def apply_while_ack_is_uncommitted(connection, **kwargs):
        observed_transaction.append(
            (
                connection.in_transaction,
                connection.execute(
                    "SELECT status FROM direct_sync_relay_batches WHERE relay_id='relay-a'"
                ).fetchone()[0],
            )
        )
        return original_apply(connection, **kwargs)

    monkeypatch.setattr(
        direct_sync_push,
        "apply_runtime_receipt_in_transaction",
        apply_while_ack_is_uncommitted,
    )
    result = direct_sync_push.drain_one_relay_batch(
        db_path=db_path,
        credentials=_credentials(),
        worker_id="worker-real",
        session=session,
        status_dir=status_dir,
    )

    assert result is not None and result.success is True
    assert observed_transaction == [(True, "acked")]
    assert len(session.calls) == len(session.source_calls) == 1
    uploaded_metadata = session.source_calls[0][0]
    assert set(runtime_client.METADATA_FIELDS).issubset(uploaded_metadata)
    assert uploaded_metadata["runtime_request_token"] not in json.dumps(result.receipt)
    with sqlite3.connect(db_path) as connection:
        batch = connection.execute(
            "SELECT status, metadata_json, receipt_json FROM direct_sync_relay_batches "
            "WHERE relay_id='relay-a'"
        ).fetchone()
        state = connection.execute(
            "SELECT next_request_token, next_request_sequence, assigned_relay_id "
            "FROM direct_sync_runtime_authority"
        ).fetchone()
    assert batch[0] == "acked"
    assert state == ("R" * 43, 2, None)
    terminal_metadata = json.loads(batch[1])
    assert "runtime_request_token" not in terminal_metadata
    assert terminal_metadata["runtime_request_token_sha256"] == hashlib.sha256(
        uploaded_metadata["runtime_request_token"].encode("utf-8")
    ).hexdigest()
    assert uploaded_metadata["runtime_request_token"] not in json.dumps(
        direct_sync_push.relay_queue_status(db_path)
    )
    status_text = next(status_dir.glob("*.json")).read_text(encoding="utf-8")
    assert uploaded_metadata["runtime_request_token"] not in status_text
    assert "R" * 43 not in status_text
    assert "R" * 43 not in str(batch[2])


def test_runtime_acked_retention_requires_terminal_safe_hash_and_exact_artifact(tmp_path):
    db_path = tmp_path / "relay.sqlite3"
    _make_pending_relay(db_path, tmp_path, "relay-retention")
    result = direct_sync_push.drain_one_relay_batch(
        db_path=db_path,
        credentials=_credentials(),
        worker_id="worker-real",
        session=_RelaySession(),
        status_dir=tmp_path / "status",
    )
    assert result is not None and result.success is True
    candidates = direct_sync_push.acked_relay_retention_candidates(db_path)
    assert len(candidates) == 1

    with sqlite3.connect(db_path) as connection:
        row = connection.execute(
            "SELECT metadata_json, upload_status_path FROM direct_sync_relay_batches"
        ).fetchone()
    terminal_metadata = json.loads(row[0])
    artifact_path = Path(row[1])
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    token_hash = terminal_metadata["runtime_request_token_sha256"]
    assert re.fullmatch(r"[0-9a-f]{64}", token_hash)
    assert "runtime_request_token" not in terminal_metadata
    assert artifact["metadata"]["runtime_request_token"] == "[redacted]"
    assert artifact["metadata"]["runtime_request_token_sha256"] == token_hash
    assert "A" * 43 not in artifact_path.read_text(encoding="utf-8")
    pristine_artifact = json.loads(json.dumps(artifact))

    def assert_artifact_mutation_rejected(mutator):
        changed = json.loads(json.dumps(pristine_artifact))
        mutator(changed)
        artifact_path.write_text(json.dumps(changed), encoding="utf-8")
        assert direct_sync_push.acked_relay_retention_candidates(db_path) == ()
        artifact_path.write_text(json.dumps(pristine_artifact), encoding="utf-8")

    assert_artifact_mutation_rejected(
        lambda value: value["metadata"].__setitem__(
            "runtime_request_token_sha256", "f" * 64
        )
    )
    assert_artifact_mutation_rejected(
        lambda value: value["metadata"].__setitem__(
            "runtime_request_sequence", value["metadata"]["runtime_request_sequence"] + 1
        )
    )
    assert_artifact_mutation_rejected(
        lambda value: value["metadata"]["runtime_public_jwk"].__setitem__("x", "A" * 43)
    )
    assert_artifact_mutation_rejected(
        lambda value: value["metadata"].__setitem__("source_host_id", "tampered-host")
    )
    assert_artifact_mutation_rejected(
        lambda value: value["metadata"].__setitem__("runtime_request_token", "Z" * 43)
    )
    assert_artifact_mutation_rejected(
        lambda value: value["receipt"].__setitem__("request_id", "tampered-request")
    )

    with sqlite3.connect(db_path) as connection:
        changed_terminal = dict(terminal_metadata)
        changed_terminal["runtime_request_token_sha256"] = "e" * 64
        connection.execute(
            "UPDATE direct_sync_relay_batches SET metadata_json=?",
            (canonical_json(changed_terminal),),
        )
    assert direct_sync_push.acked_relay_retention_candidates(db_path) == ()


def test_lost_source_ack_retries_exact_metadata_and_idempotency_with_new_hmac_nonce(tmp_path):
    db_path = tmp_path / "relay.sqlite3"
    _insert_claimed_row(db_path, "relay-a")
    spool_path = tmp_path / "relay-a.csv"
    spool_path.write_bytes(b"")
    empty_sha256 = hashlib.sha256(b"").hexdigest()
    metadata = _metadata("relay-a")
    metadata["content_sha256"] = empty_sha256
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            UPDATE direct_sync_relay_batches
            SET status='pending', attempt_count=0, lease_owner=NULL, lease_expires_at=NULL,
                next_attempt_at='2000-01-01T00:00:00Z',
                source_file_path=?, spooled_file_path=?, content_sha256=?,
                metadata_json=?
            WHERE relay_id='relay-a'
            """,
            (str(spool_path), str(spool_path), empty_sha256, canonical_json(metadata)),
        )
    session = _RelaySession(source_failures=1)

    first = direct_sync_push.drain_one_relay_batch(
        db_path=db_path,
        credentials=_credentials(),
        worker_id="worker-real",
        session=session,
        retry_base_seconds=1,
    )
    assert first is not None and first.retryable is True

    idle_liveness = runtime_client.ensure_runtime_authority(
        db_path=db_path,
        credentials=_credentials(),
        producer_install_id="install-test",
        session=session,
    )
    assert idle_liveness.error_code == ""
    assert idle_liveness.receipt["status"] == "ACTIVE"
    assert idle_liveness.receipt["server_grant_accepted"] is True
    assert idle_liveness.receipt["request_in_flight"] is True
    assert idle_liveness.receipt["request_sent"] is False
    assert len(session.calls) == 1
    with sqlite3.connect(db_path) as connection:
        first_snapshot = connection.execute(
            "SELECT metadata_json FROM direct_sync_relay_batches WHERE relay_id='relay-a'"
        ).fetchone()[0]
        authority_snapshot = connection.execute(
            "SELECT assigned_relay_id, next_request_token, next_request_sequence "
            "FROM direct_sync_runtime_authority"
        ).fetchone()
        connection.execute(
            "UPDATE direct_sync_relay_batches SET next_attempt_at='2000-01-01T00:00:00Z' "
            "WHERE relay_id='relay-a'"
        )
    assert authority_snapshot == ("relay-a", None, None)
    second = direct_sync_push.drain_one_relay_batch(
        db_path=db_path,
        credentials=_credentials(),
        worker_id="worker-real",
        session=session,
        retry_base_seconds=1,
    )

    assert second is not None and second.success is True
    assert len(session.calls) == 1
    assert len(session.source_calls) == 2
    first_metadata, first_headers = session.source_calls[0]
    second_metadata, second_headers = session.source_calls[1]
    assert first_metadata == second_metadata == json.loads(first_snapshot)
    assert first_metadata["runtime_request_token"] in first_snapshot
    assert first_metadata["idempotency_key"] == second_metadata["idempotency_key"] == "relay-a"
    assert first_headers["X-Producer-Nonce"] != second_headers["X-Producer-Nonce"]


def test_stale_runtime_token_is_operator_review_and_never_leaks_to_status(tmp_path):
    db_path = tmp_path / "relay.sqlite3"
    _insert_claimed_row(db_path, "relay-a")
    spool_path = tmp_path / "relay-a.csv"
    spool_path.write_bytes(b"")
    empty_sha256 = hashlib.sha256(b"").hexdigest()
    metadata = _metadata("relay-a")
    metadata["content_sha256"] = empty_sha256
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            UPDATE direct_sync_relay_batches
            SET status='pending', attempt_count=0, lease_owner=NULL, lease_expires_at=NULL,
                next_attempt_at='2000-01-01T00:00:00Z',
                source_file_path=?, spooled_file_path=?, content_sha256=?, metadata_json=?
            WHERE relay_id='relay-a'
            """,
            (str(spool_path), str(spool_path), empty_sha256, canonical_json(metadata)),
        )

    class _StaleSession(_LeaseSession):
        def __init__(self):
            super().__init__()
            self.presented_token = ""

        def post(self, url, **kwargs):
            if str(url).endswith(runtime_client.ENDPOINT_PATH):
                return super().post(url, **kwargs)
            uploaded = json.loads(kwargs["data"]["metadata"])
            self.presented_token = uploaded["runtime_request_token"]
            return _Response(
                409,
                {
                    "committed": False,
                    "retryable": False,
                    "status": "operator_review",
                    "runtime_request_token": self.presented_token,
                    "error": {
                        "code": "STALE_RUNTIME_REQUEST_TOKEN",
                        "message": f"stale authority {self.presented_token}",
                    },
                },
                {},
            )

    session = _StaleSession()
    status_dir = tmp_path / "status"
    result = direct_sync_push.drain_one_relay_batch(
        db_path=db_path,
        credentials=_credentials(),
        worker_id="worker-real",
        session=session,
        status_dir=status_dir,
    )

    assert result is not None
    assert result.error_code == "STALE_RUNTIME_REQUEST_TOKEN"
    assert result.retryable is False
    assert session.presented_token not in result.error_message
    assert session.presented_token not in json.dumps(result.receipt)
    with sqlite3.connect(db_path) as connection:
        status, metadata_json, receipt_json = connection.execute(
            "SELECT status, metadata_json, receipt_json FROM direct_sync_relay_batches "
            "WHERE relay_id='relay-a'"
        ).fetchone()
        authority = connection.execute(
            "SELECT status, next_request_token, next_request_sequence, assigned_relay_id, "
            "pending_request_json FROM direct_sync_runtime_authority"
        ).fetchone()
    assert status == "operator_review"
    assert authority == ("OPERATOR_REVIEW", None, None, None, None)
    terminal_metadata = json.loads(metadata_json)
    assert "runtime_request_token" not in terminal_metadata
    assert terminal_metadata["runtime_request_token_sha256"] == hashlib.sha256(
        session.presented_token.encode("utf-8")
    ).hexdigest()
    assert session.presented_token not in str(receipt_json)
    assert session.presented_token not in json.dumps(direct_sync_push.relay_queue_status(db_path))
    assert session.presented_token not in next(status_dir.glob("*.json")).read_text(encoding="utf-8")


def test_definitive_noncommitted_rejection_releases_token_for_the_next_row(tmp_path):
    db_path = tmp_path / "relay.sqlite3"
    _make_pending_relay(db_path, tmp_path, "relay-a")
    session = _DefinitiveRejectSession()

    result = direct_sync_push.drain_one_relay_batch(
        db_path=db_path,
        credentials=_credentials(),
        worker_id="worker-real",
        session=session,
    )

    assert result is not None
    assert result.committed is False and result.retryable is False
    presented_token = session.source_calls[0]["runtime_request_token"]
    with sqlite3.connect(db_path) as connection:
        batch_status, metadata_json = connection.execute(
            "SELECT status, metadata_json FROM direct_sync_relay_batches WHERE relay_id='relay-a'"
        ).fetchone()
        authority = connection.execute(
            "SELECT status, next_request_token, next_request_sequence, assigned_relay_id, "
            "pending_request_json FROM direct_sync_runtime_authority"
        ).fetchone()
    assert batch_status == "failed_permanent"
    assert "runtime_request_token" not in json.loads(metadata_json)
    assert authority == ("ACTIVE", presented_token, 1, None, None)

    _insert_claimed_row(db_path, "relay-b", owner="worker-b")
    next_preparation = runtime_client.prepare_runtime_metadata(
        db_path=db_path,
        relay_id="relay-b",
        metadata=_metadata("relay-b"),
        credentials=_credentials(),
        expected_lease_owner="worker-b",
        expected_attempt_count=1,
        session=session,
        timeout=5,
    )
    assert next_preparation.metadata is not None
    assert next_preparation.metadata["runtime_request_token"] == presented_token


def test_local_terminal_exception_quarantines_and_clears_reserved_authority(tmp_path, monkeypatch):
    db_path = tmp_path / "relay.sqlite3"
    _make_pending_relay(db_path, tmp_path, "relay-a")
    session = _LeaseSession()

    def fail_locally(*args, **kwargs):
        raise RuntimeError("local parser failure")

    monkeypatch.setattr(direct_sync_push, "upload_source_file", fail_locally)
    result = direct_sync_push.drain_one_relay_batch(
        db_path=db_path,
        credentials=_credentials(),
        worker_id="worker-real",
        session=session,
    )

    assert result is not None and result.error_code == "upload_unhandled_exception"
    with sqlite3.connect(db_path) as connection:
        batch_status, metadata_json = connection.execute(
            "SELECT status, metadata_json FROM direct_sync_relay_batches WHERE relay_id='relay-a'"
        ).fetchone()
        authority = connection.execute(
            "SELECT status, next_request_token, next_request_sequence, assigned_relay_id, "
            "pending_request_json FROM direct_sync_runtime_authority"
        ).fetchone()
    assert batch_status == "operator_review"
    assert "runtime_request_token" not in json.loads(metadata_json)
    assert authority == ("OPERATOR_REVIEW", None, None, None, None)


def test_enforce_mode_fails_closed_on_committed_legacy_receipt(tmp_path):
    db_path = tmp_path / "relay.sqlite3"
    _make_pending_relay(db_path, tmp_path, "relay-a")
    session = _LegacyReceiptSession()

    result = direct_sync_push.drain_one_relay_batch(
        db_path=db_path,
        credentials=_credentials(runtime_lease_mode="enforce"),
        worker_id="worker-real",
        session=session,
    )

    assert result is not None and result.committed is True and result.success is False
    assert result.error_code == "runtime_lease_receipt_missing"
    with sqlite3.connect(db_path) as connection:
        batch_status, metadata_json = connection.execute(
            "SELECT status, metadata_json FROM direct_sync_relay_batches WHERE relay_id='relay-a'"
        ).fetchone()
        authority = connection.execute(
            "SELECT status, next_request_token, assigned_relay_id, pending_request_json "
            "FROM direct_sync_runtime_authority"
        ).fetchone()
    assert batch_status == "operator_review"
    assert "runtime_request_token" not in json.loads(metadata_json)
    assert authority == ("OPERATOR_REVIEW", None, None, None)


def test_observe_mode_legacy_receipt_disables_authority_and_future_runtime_metadata(tmp_path):
    db_path = tmp_path / "relay.sqlite3"
    _make_pending_relay(db_path, tmp_path, "relay-a")
    session = _LegacyReceiptSession()
    credentials = _credentials(runtime_lease_mode="observe")

    first = direct_sync_push.drain_one_relay_batch(
        db_path=db_path,
        credentials=credentials,
        worker_id="worker-real",
        session=session,
    )
    assert first is not None and first.success is True
    assert first.receipt["_local_runtime_lease_status"] == "legacy_accepted"
    assert set(runtime_client.METADATA_FIELDS).issubset(session.source_calls[0])
    with sqlite3.connect(db_path) as connection:
        authority = connection.execute(
            "SELECT status, lease_id, fence, next_request_token, next_request_sequence, "
            "assigned_relay_id, pending_request_json FROM direct_sync_runtime_authority"
        ).fetchone()
    assert authority == (runtime_client.LEGACY_DISABLED_STATUS, None, None, None, None, None, None)

    _make_pending_relay(db_path, tmp_path, "relay-b")
    second = direct_sync_push.drain_one_relay_batch(
        db_path=db_path,
        credentials=credentials,
        worker_id="worker-real",
        session=session,
    )
    assert second is not None and second.success is True
    assert len(session.calls) == 1
    assert not set(runtime_client.METADATA_FIELDS).intersection(session.source_calls[1])


def test_legacy_exact_replay_is_receipt_first_in_observe_and_enforce(tmp_path):
    observe_db = tmp_path / "observe.sqlite3"
    _make_pending_relay(
        observe_db,
        tmp_path,
        "relay-observe",
        attempt_count=1,
        runtime_fencing_policy=runtime_client.RUNTIME_FENCING_POLICY_LEGACY_EXACT_REPLAY,
    )
    with sqlite3.connect(observe_db) as connection:
        legacy_metadata = json.loads(
            connection.execute(
                "SELECT metadata_json FROM direct_sync_relay_batches WHERE relay_id='relay-observe'"
            ).fetchone()[0]
        )
    observe_session = _LegacyReceiptSession()
    observe = direct_sync_push.drain_one_relay_batch(
        db_path=observe_db,
        credentials=_credentials(runtime_lease_mode="observe"),
        worker_id="worker-real",
        session=observe_session,
    )
    assert observe is not None and observe.success is True
    assert observe_session.calls == []
    assert observe_session.source_calls == [legacy_metadata]
    assert not set(runtime_client.METADATA_FIELDS).intersection(legacy_metadata)
    with sqlite3.connect(observe_db) as connection:
        assert connection.execute(
            "SELECT status FROM direct_sync_runtime_authority"
        ).fetchone()[0] == runtime_client.LEGACY_DISABLED_STATUS

    enforce_db = tmp_path / "enforce.sqlite3"
    _make_pending_relay(
        enforce_db,
        tmp_path,
        "relay-enforce",
        attempt_count=1,
        runtime_fencing_policy=runtime_client.RUNTIME_FENCING_POLICY_LEGACY_EXACT_REPLAY,
    )
    enforce_session = _LegacyReceiptSession()
    enforce = direct_sync_push.drain_one_relay_batch(
        db_path=enforce_db,
        credentials=_credentials(runtime_lease_mode="enforce"),
        worker_id="worker-real",
        session=enforce_session,
    )
    assert enforce is not None and enforce.success is True
    assert enforce_session.calls == []
    assert len(enforce_session.source_calls) == 1
    assert not set(runtime_client.METADATA_FIELDS).intersection(enforce_session.source_calls[0])
    with sqlite3.connect(enforce_db) as connection:
        assert connection.execute(
            "SELECT status FROM direct_sync_runtime_authority"
        ).fetchone()[0] == runtime_client.LEGACY_DISABLED_STATUS


def test_runtime_fencing_policy_migration_is_atomic_and_backfills_only_attempted_rows(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "legacy-relay.sqlite3"
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE direct_sync_relay_batches (
                relay_id TEXT PRIMARY KEY, status TEXT NOT NULL,
                source_file_path TEXT NOT NULL, spooled_file_path TEXT NOT NULL,
                producer_manifest_path TEXT NOT NULL, relative_path TEXT NOT NULL,
                content_sha256 TEXT NOT NULL, byte_length INTEGER NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0, lease_owner TEXT,
                lease_expires_at TEXT, next_attempt_at TEXT, last_error_code TEXT,
                last_error_message TEXT, receipt_json TEXT, upload_status_path TEXT,
                metadata_json TEXT, producer_id TEXT, key_id TEXT, endpoint_url TEXT,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            )
            """
        )
        for relay_id, attempt_count in (("fresh", 0), ("attempted", 1)):
            connection.execute(
                """
                INSERT INTO direct_sync_relay_batches(
                    relay_id, status, source_file_path, spooled_file_path,
                    producer_manifest_path, relative_path, content_sha256,
                    byte_length, attempt_count, created_at, updated_at
                ) VALUES(?, 'pending', '', '', '', '', ?, 0, ?, ?, ?)
                """,
                (relay_id, "0" * 64, attempt_count, "2026-08-06T00:00:00Z", "2026-08-06T00:00:00Z"),
            )

    original_init_runtime_schema = direct_sync_push.init_runtime_schema

    def fail_after_column_migration(connection):
        assert connection.in_transaction is True
        raise RuntimeError("migration interruption")

    monkeypatch.setattr(direct_sync_push, "init_runtime_schema", fail_after_column_migration)
    with pytest.raises(RuntimeError, match="migration interruption"):
        direct_sync_push.init_relay_queue_schema(db_path)
    with sqlite3.connect(db_path) as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(direct_sync_relay_batches)")}
    assert "runtime_fencing_policy" not in columns

    monkeypatch.setattr(direct_sync_push, "init_runtime_schema", original_init_runtime_schema)
    direct_sync_push.init_relay_queue_schema(db_path)
    with sqlite3.connect(db_path) as connection:
        policies = dict(
            connection.execute(
                "SELECT relay_id, runtime_fencing_policy FROM direct_sync_relay_batches"
            ).fetchall()
        )
    assert policies == {
        "fresh": runtime_client.RUNTIME_FENCING_POLICY_RUNTIME_REQUIRED,
        "attempted": runtime_client.RUNTIME_FENCING_POLICY_LEGACY_EXACT_REPLAY,
    }


def test_runtime_required_lease_timeout_retries_the_exact_pending_issue_request(tmp_path):
    db_path = tmp_path / "relay.sqlite3"
    _make_pending_relay(db_path, tmp_path, "relay-timeout")
    session = _RelaySession(lease_failures=1)

    first = direct_sync_push.drain_one_relay_batch(
        db_path=db_path,
        credentials=_credentials(),
        worker_id="worker-real",
        session=session,
        retry_base_seconds=1,
    )
    assert first is not None and first.retryable is True
    assert session.source_calls == []
    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        relay = connection.execute(
            "SELECT status, runtime_fencing_policy FROM direct_sync_relay_batches"
        ).fetchone()
        authority = connection.execute(
            "SELECT status, pending_request_json FROM direct_sync_runtime_authority"
        ).fetchone()
        connection.execute(
            "UPDATE direct_sync_relay_batches SET next_attempt_at='2000-01-01T00:00:00Z'"
        )
    assert relay["status"] == direct_sync_push.RELAY_STATUS_RETRY_WAIT
    assert relay["runtime_fencing_policy"] == runtime_client.RUNTIME_FENCING_POLICY_RUNTIME_REQUIRED
    assert authority["status"] == "PENDING"
    pending_request = json.loads(authority["pending_request_json"])

    second = direct_sync_push.drain_one_relay_batch(
        db_path=db_path,
        credentials=_credentials(),
        worker_id="worker-real",
        session=session,
        retry_base_seconds=1,
    )
    assert second is not None and second.success is True
    assert len(session.calls) == 2
    assert session.calls[0][1] == pending_request == session.calls[1][1]
    assert session.calls[0][2]["X-Producer-Nonce"] != session.calls[1][2]["X-Producer-Nonce"]
    with sqlite3.connect(db_path) as connection:
        relay = connection.execute(
            "SELECT status, runtime_fencing_policy FROM direct_sync_relay_batches"
        ).fetchone()
    assert relay == (
        direct_sync_push.RELAY_STATUS_ACKED,
        runtime_client.RUNTIME_FENCING_POLICY_RUNTIME_REQUIRED,
    )


def test_legacy_lost_ack_replays_exactly_across_mode_and_endpoint_change(tmp_path):
    db_path = tmp_path / "relay.sqlite3"
    _make_pending_relay(
        db_path,
        tmp_path,
        "relay-legacy",
        attempt_count=1,
        runtime_fencing_policy=runtime_client.RUNTIME_FENCING_POLICY_LEGACY_EXACT_REPLAY,
    )
    original_credentials = _credentials(runtime_lease_mode="observe")
    session = _LegacyReceiptSession(source_failures=1)

    first = direct_sync_push.drain_one_relay_batch(
        db_path=db_path,
        credentials=original_credentials,
        worker_id="worker-real",
        session=session,
        retry_base_seconds=1,
    )
    assert first is not None and first.retryable is True
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "UPDATE direct_sync_relay_batches SET next_attempt_at='2000-01-01T00:00:00Z'"
        )
        policy = connection.execute(
            "SELECT runtime_fencing_policy FROM direct_sync_relay_batches"
        ).fetchone()[0]
    assert policy == runtime_client.RUNTIME_FENCING_POLICY_LEGACY_EXACT_REPLAY

    changed_credentials = ProducerCredentials(
        producer_id=original_credentials.producer_id,
        key_id=original_credentials.key_id,
        secret=original_credentials.secret,
        endpoint_url="https://changed.example/api/producer-ingest/v1/source-file",
        runtime_lease_mode="enforce",
    )
    second = direct_sync_push.drain_one_relay_batch(
        db_path=db_path,
        credentials=changed_credentials,
        worker_id="worker-real",
        session=session,
        retry_base_seconds=1,
    )
    assert second is not None and second.success is True
    assert session.calls == []
    assert session.source_calls[0] == session.source_calls[1]
    assert session.source_urls == [original_credentials.endpoint_url, original_credentials.endpoint_url]
    assert not set(runtime_client.METADATA_FIELDS).intersection(session.source_calls[0])


def test_enforce_quarantines_explicitly_noncommitted_legacy_exact_replay(tmp_path):
    db_path = tmp_path / "relay.sqlite3"
    _make_pending_relay(
        db_path,
        tmp_path,
        "relay-legacy",
        attempt_count=1,
        runtime_fencing_policy=runtime_client.RUNTIME_FENCING_POLICY_LEGACY_EXACT_REPLAY,
    )
    result = direct_sync_push.drain_one_relay_batch(
        db_path=db_path,
        credentials=_credentials(runtime_lease_mode="enforce"),
        worker_id="worker-real",
        session=_DefinitiveRejectSession(),
    )
    assert result is not None and result.success is False
    with sqlite3.connect(db_path) as connection:
        relay_status = connection.execute(
            "SELECT status FROM direct_sync_relay_batches"
        ).fetchone()[0]
        authority_status = connection.execute(
            "SELECT status FROM direct_sync_runtime_authority"
        ).fetchone()[0]
    assert relay_status == direct_sync_push.RELAY_STATUS_OPERATOR_REVIEW
    assert authority_status == "OPERATOR_REVIEW"


@pytest.mark.parametrize("runtime_mode", ["observe", "enforce"])
@pytest.mark.parametrize("error_code", ["metadata_invalid", "idempotency_conflict"])
def test_legacy_terminal_source_rejection_clears_any_assigned_runtime_authority(
    tmp_path, runtime_mode, error_code
):
    db_path = tmp_path / f"{runtime_mode}-{error_code}.sqlite3"
    relay_id = "relay-legacy-assigned"
    _insert_claimed_row(db_path, relay_id)
    prepared = _prepare(db_path, relay_id, _LeaseSession())
    assert prepared.metadata is not None

    spool_path = tmp_path / f"{runtime_mode}-{error_code}.csv"
    spool_path.write_bytes(b"")
    empty_sha256 = hashlib.sha256(b"").hexdigest()
    legacy_metadata = _metadata(relay_id)
    legacy_metadata["content_sha256"] = empty_sha256
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            UPDATE direct_sync_relay_batches
            SET status='pending', attempt_count=1, lease_owner=NULL,
                lease_expires_at=NULL, next_attempt_at='2000-01-01T00:00:00Z',
                source_file_path=?, spooled_file_path=?, content_sha256=?,
                metadata_json=?, runtime_fencing_policy=?
            WHERE relay_id=?
            """,
            (
                str(spool_path),
                str(spool_path),
                empty_sha256,
                canonical_json(legacy_metadata),
                runtime_client.RUNTIME_FENCING_POLICY_LEGACY_EXACT_REPLAY,
                relay_id,
            ),
        )

    session = _GenericLegacyRejectSession(error_code)
    result = direct_sync_push.drain_one_relay_batch(
        db_path=db_path,
        credentials=_credentials(runtime_lease_mode=runtime_mode),
        worker_id="worker-real",
        session=session,
    )
    assert result is not None and result.success is False
    assert session.calls == []
    assert session.source_calls == [legacy_metadata]
    with sqlite3.connect(db_path) as connection:
        relay_status = connection.execute(
            "SELECT status FROM direct_sync_relay_batches WHERE relay_id=?",
            (relay_id,),
        ).fetchone()[0]
        authority = connection.execute(
            """
            SELECT status, next_request_token, next_request_sequence,
                   assigned_relay_id, pending_request_json, pending_issue_idempotency_key
            FROM direct_sync_runtime_authority
            """
        ).fetchone()
    expected_relay_status = (
        direct_sync_push.RELAY_STATUS_OPERATOR_REVIEW
        if runtime_mode == "enforce"
        else direct_sync_push.RELAY_STATUS_FAILED_PERMANENT
    )
    assert relay_status == expected_relay_status
    assert authority == ("OPERATOR_REVIEW", None, None, None, None, None)


def test_two_workers_can_reserve_only_one_rotating_token(tmp_path):
    db_path = tmp_path / "relay.sqlite3"
    _insert_claimed_row(db_path, "seed")
    seed = _prepare(db_path, "seed", _LeaseSession())
    rotation = {
        "contract_version": runtime_client.CONTRACT_VERSION,
        "validation_status": "consumed",
        "lease_id": "lease-test",
        "fence": seed.metadata["runtime_fence"],
        "next_request_token": "C" * 43,
        "next_request_sequence": 2,
        "expires_at": "2099-08-06T00:00:00Z",
    }
    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("UPDATE direct_sync_relay_batches SET status='acked' WHERE relay_id='seed'")
        runtime_client.apply_runtime_receipt_in_transaction(
            connection,
            relay_id="seed",
            metadata=seed.metadata,
            credentials=_credentials(),
            runtime_lease=rotation,
            now="2026-08-06T00:00:01Z",
        )
        connection.commit()
    _insert_claimed_row(db_path, "relay-a", owner="worker-a")
    _insert_claimed_row(db_path, "relay-b", owner="worker-b")

    def reserve(relay_id, owner):
        return runtime_client.prepare_runtime_metadata(
            db_path=db_path,
            relay_id=relay_id,
            metadata=_metadata(relay_id),
            credentials=_credentials(),
            expected_lease_owner=owner,
            expected_attempt_count=1,
            session=_LeaseSession(),
            timeout=5,
            now="2026-08-06T00:00:02Z",
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda args: reserve(*args), [("relay-a", "worker-a"), ("relay-b", "worker-b")]))
    winners = [result for result in results if result.metadata is not None]
    blocked = [result for result in results if result.metadata is None]
    assert len(winners) == len(blocked) == 1
    assert blocked[0].error_code == "runtime_request_in_flight"
    assert winners[0].metadata["runtime_request_token"] == "C" * 43


@pytest.mark.skipif(
    not PRODUCER_RUNTIME_LEASE_MODULE.is_file(),
    reason="requires sibling WorkerAnalysisGUI-web/producer_runtime_lease.py external workspace",
)
def test_cloned_relay_databases_get_one_server_commit_and_one_stale_token(tmp_path):
    server_root = WORKER_ANALYSIS_GUI_ROOT
    sys.path.insert(0, str(server_root))
    try:
        from producer_runtime_lease import (
            STALE_RUNTIME_REQUEST_TOKEN,
            ProducerRuntimeLeaseError,
            ProducerRuntimeLeaseService,
            initialize_schema,
        )
    finally:
        sys.path.remove(str(server_root))

    now = datetime.now(UTC)
    runtime_id, public_jwk = runtime_client.new_runtime_identity()
    server_db = tmp_path / "server.sqlite3"
    initialize_schema(server_db, now=now)
    service = ProducerRuntimeLeaseService(server_db)
    grant = service.acquire(
        producer_install_id="install-test",
        runtime_instance_id=runtime_id,
        public_jwk=public_jwk,
        issue_idempotency_key="clone-seed",
        ttl_seconds=600,
        now=now,
    )

    first_db = tmp_path / "first.sqlite3"
    _insert_claimed_row(first_db, "relay-a", owner="worker-a")
    scope = runtime_client._scope_values(_credentials(), "install-test")
    with sqlite3.connect(first_db) as connection:
        connection.execute(
            """
            INSERT INTO direct_sync_runtime_authority(
                authority_scope, endpoint_url, producer_id, key_id,
                producer_install_id, runtime_instance_id,
                runtime_public_jwk_json, lease_id, fence,
                next_request_token, next_request_sequence, expires_at,
                status, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?)
            """,
            (
                runtime_client._scope_key(scope),
                scope["endpoint_url"], scope["producer_id"], scope["key_id"],
                scope["producer_install_id"], runtime_id, canonical_json(public_jwk),
                grant["lease_id"], grant["fence"], grant["next_request_token"],
                grant["next_request_sequence"], grant["expires_at"],
                now.isoformat(), now.isoformat(),
            ),
        )
    second_db = tmp_path / "second.sqlite3"
    with sqlite3.connect(first_db) as source, sqlite3.connect(second_db) as target:
        source.backup(target)
    with sqlite3.connect(second_db) as connection:
        metadata = _metadata("relay-b")
        connection.execute(
            """
            UPDATE direct_sync_relay_batches
            SET relay_id='relay-b', lease_owner='worker-b',
                relative_path=?, metadata_json=?
            WHERE relay_id='relay-a'
            """,
            (metadata["relative_path"], canonical_json(metadata)),
        )
    first = runtime_client.prepare_runtime_metadata(
        db_path=first_db, relay_id="relay-a", metadata=_metadata("relay-a"),
        credentials=_credentials(), expected_lease_owner="worker-a",
        expected_attempt_count=1, session=_LeaseSession(), timeout=5,
    )
    second = runtime_client.prepare_runtime_metadata(
        db_path=second_db, relay_id="relay-b", metadata=_metadata("relay-b"),
        credentials=_credentials(), expected_lease_owner="worker-b",
        expected_attempt_count=1, session=_LeaseSession(), timeout=5,
    )
    assert first.metadata["runtime_request_token"] == second.metadata["runtime_request_token"]
    assert first.metadata["idempotency_key"] != second.metadata["idempotency_key"]

    outcomes = []
    for index, metadata in enumerate((first.metadata, second.metadata), start=1):
        with sqlite3.connect(server_db, isolation_level=None) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute("BEGIN IMMEDIATE")
            try:
                service.consume_request_in_transaction(
                    connection,
                    producer_install_id="install-test",
                    runtime_instance_id=runtime_id,
                    public_jwk=public_jwk,
                    fence=metadata["runtime_fence"],
                    runtime_request_token=metadata["runtime_request_token"],
                    runtime_request_sequence=metadata["runtime_request_sequence"],
                    request_fingerprint=hashlib.sha256(metadata["idempotency_key"].encode()).hexdigest(),
                    receipt_request_id=f"receipt-{index}",
                    now=now + timedelta(seconds=index),
                )
            except ProducerRuntimeLeaseError as exc:
                if exc.audit_recorded:
                    connection.commit()
                else:
                    connection.rollback()
                outcomes.append(exc.code)
            else:
                connection.commit()
                outcomes.append("accepted")
    assert outcomes == ["accepted", STALE_RUNTIME_REQUEST_TOKEN]

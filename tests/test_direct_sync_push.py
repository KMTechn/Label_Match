import hashlib
import json
import sqlite3
from pathlib import Path

from direct_sync_push import (
    DEFAULT_ENDPOINT_PATH,
    ProducerCredentials,
    RELAY_STATUS_ACKED,
    RELAY_STATUS_LEASED,
    RELAY_STATUS_PENDING,
    RELAY_STATUS_RETRY_WAIT,
    build_source_file_plan,
    canonical_json,
    canonical_request_string,
    claim_next_relay_batch,
    count_csv_data_rows,
    drain_one_relay_batch,
    enqueue_source_file_for_relay,
    manifest_hash,
    relay_queue_status,
    reset_stale_relay_leases,
    signed_headers,
    upload_source_file,
)


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def post(self, url, *, data, files, headers, timeout):
        file_name, file_handle, content_type = files["file"]
        self.calls.append(
            {
                "url": url,
                "metadata": data["metadata"],
                "headers": dict(headers),
                "timeout": timeout,
                "file_name": file_name,
                "file_bytes": file_handle.read(),
                "content_type": content_type,
            }
        )
        return self.response


class SequenceFakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def post(self, url, *, data, files, headers, timeout):
        file_name, file_handle, content_type = files["file"]
        self.calls.append(
            {
                "url": url,
                "metadata": data["metadata"],
                "headers": dict(headers),
                "timeout": timeout,
                "file_name": file_name,
                "file_bytes": file_handle.read(),
                "content_type": content_type,
            }
        )
        return self.responses.pop(0)


def make_manifest(tmp_path):
    manifest = {
        "schema_version": "producer-onboarding-manifest-v1",
        "pc_identity": {
            "pc_id": "LABEL-PC01",
            "source_host_id": "label-host-1",
            "producer_install_id": "install-label-1",
        },
        "apps": ["LabelMatch"],
        "streams": [
            {
                "producer_role": "label_match",
                "stream_name": "label_match_events",
                "source_system": "label_match",
                "source_transport": "legacy_packaging_csv",
            }
        ],
    }
    path = tmp_path / "producer_manifest.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
    return manifest, path


def write_csv(tmp_path):
    path = tmp_path / "패키징작업이벤트로그_fixture_20260621.csv"
    path.write_text(
        "timestamp,worker_name,event,details\n"
        "2026-06-21T00:00:00,worker,LABEL_MATCHED,\"{ \"\"product_barcode\"\": \"\"BC-1\"\" }\"\n",
        encoding="utf-8",
    )
    return path


def make_credentials():
    return ProducerCredentials(
        producer_id="producer-label",
        key_id="key-label",
        secret="label-secret",
        endpoint_url="https://worker.example.invalid/api/producer-ingest/v1/source-file",
    )


def test_build_plan_uses_label_match_stream_and_csv_rows(tmp_path):
    manifest, manifest_path = make_manifest(tmp_path)
    csv_path = write_csv(tmp_path)
    credentials = make_credentials()

    plan = build_source_file_plan(
        source_file_path=csv_path,
        producer_manifest_path=manifest_path,
        credentials=credentials,
    )

    content = csv_path.read_bytes()
    assert plan.content_sha256 == hashlib.sha256(content).hexdigest()
    assert plan.byte_length == len(content)
    assert count_csv_data_rows(csv_path) == 1
    assert plan.metadata["manifest_hash"] == manifest_hash(manifest)
    assert plan.metadata["producer_role"] == "label_match"
    assert plan.metadata["stream_name"] == "label_match_events"
    assert plan.metadata["source_system"] == "label_match"
    assert plan.metadata["source_transport"] == "legacy_packaging_csv"
    assert plan.metadata["relative_path"] == f"legacy_csv/{csv_path.name}"
    assert plan.metadata["row_count"] == 1
    assert plan.metadata["first_row_number"] == 2
    assert plan.metadata["last_row_number"] == 2


def test_signed_headers_match_server_contract(tmp_path):
    _manifest, manifest_path = make_manifest(tmp_path)
    csv_path = write_csv(tmp_path)
    credentials = make_credentials()
    plan = build_source_file_plan(
        source_file_path=csv_path,
        producer_manifest_path=manifest_path,
        credentials=credentials,
    )

    headers = signed_headers(
        credentials,
        plan.metadata,
        timestamp="2026-06-21T00:00:00Z",
        nonce="nonce-label-1",
    )
    canonical = canonical_request_string(
        method="POST",
        path=DEFAULT_ENDPOINT_PATH,
        query_string="",
        timestamp="2026-06-21T00:00:00Z",
        nonce="nonce-label-1",
        producer_id=credentials.producer_id,
        key_id=credentials.key_id,
        metadata=plan.metadata,
        content_sha256=plan.metadata["content_sha256"],
        byte_length=plan.metadata["byte_length"],
        content_type="multipart/form-data",
    )
    import hmac

    expected = hmac.new(b"label-secret", canonical.encode("utf-8"), hashlib.sha256).hexdigest()
    assert headers["X-Producer-Signature"] == expected


def test_upload_writes_status_without_storing_secret(tmp_path):
    _manifest, manifest_path = make_manifest(tmp_path)
    csv_path = write_csv(tmp_path)
    credentials = make_credentials()
    plan = build_source_file_plan(
        source_file_path=csv_path,
        producer_manifest_path=manifest_path,
        credentials=credentials,
    )
    session = FakeSession(
        FakeResponse(
            200,
            {
                "request_id": "request-label-1",
                "client_batch_id": plan.metadata["client_batch_id"],
                "server_source_file_id": "label-host-1/label_match/label_match_events/legacy_csv/file.csv",
                "committed": True,
                "status": "accepted",
                "totals": {"inserted": 1, "replayed": 0, "quarantined": 0, "errors": 0},
            },
        )
    )

    result = upload_source_file(plan, credentials, session=session, status_dir=tmp_path / "status")

    assert result.success is True
    assert result.committed is True
    assert Path(result.status_path).is_file()
    assert json.loads(session.calls[0]["metadata"]) == plan.metadata
    assert session.calls[0]["metadata"] == canonical_json(plan.metadata)
    assert session.calls[0]["file_bytes"] == csv_path.read_bytes()
    status_text = Path(result.status_path).read_text(encoding="utf-8")
    assert "label-secret" not in status_text
    assert "X-Producer-Signature" not in status_text


def test_relay_enqueue_spools_file_without_storing_auth_secret_or_signature(tmp_path):
    _manifest, manifest_path = make_manifest(tmp_path)
    csv_path = write_csv(tmp_path)
    credentials = make_credentials()

    row = enqueue_source_file_for_relay(
        db_path=tmp_path / "relay.sqlite3",
        spool_dir=tmp_path / "spool",
        source_file_path=csv_path,
        producer_manifest_path=manifest_path,
        credentials=credentials,
    )

    assert row.status == RELAY_STATUS_PENDING
    assert Path(row.spooled_file_path).read_bytes() == csv_path.read_bytes()
    status = relay_queue_status(tmp_path / "relay.sqlite3")
    assert status["counts"][RELAY_STATUS_PENDING] == 1
    db_bytes = (tmp_path / "relay.sqlite3").read_bytes()
    assert b"label-secret" not in db_bytes
    assert b"X-Producer-Signature" not in db_bytes
    assert b"PRODUCER-HMAC-SHA256-V1" not in db_bytes


def test_relay_enqueue_dedupes_same_completed_file_but_allows_changed_content(tmp_path):
    _manifest, manifest_path = make_manifest(tmp_path)
    csv_path = write_csv(tmp_path)
    credentials = make_credentials()
    db_path = tmp_path / "relay.sqlite3"
    spool_dir = tmp_path / "spool"

    first = enqueue_source_file_for_relay(
        db_path=db_path,
        spool_dir=spool_dir,
        source_file_path=csv_path,
        producer_manifest_path=manifest_path,
        credentials=credentials,
        dedupe_existing=True,
    )
    duplicate = enqueue_source_file_for_relay(
        db_path=db_path,
        spool_dir=spool_dir,
        source_file_path=csv_path,
        producer_manifest_path=manifest_path,
        credentials=credentials,
        dedupe_existing=True,
    )

    assert duplicate.relay_id == first.relay_id
    assert duplicate.deduped_existing is True
    assert relay_queue_status(db_path)["counts"][RELAY_STATUS_PENDING] == 1
    assert len(list(spool_dir.iterdir())) == 1

    csv_path.write_text(
        "timestamp,worker_name,event,details\n"
        "2026-06-22T00:01:00,worker,LABEL_MATCHED,\"{ \"\"product_barcode\"\": \"\"BC-2\"\" }\"\n",
        encoding="utf-8",
    )
    changed = enqueue_source_file_for_relay(
        db_path=db_path,
        spool_dir=spool_dir,
        source_file_path=csv_path,
        producer_manifest_path=manifest_path,
        credentials=credentials,
        dedupe_existing=True,
    )

    assert changed.relay_id != first.relay_id
    assert changed.deduped_existing is False
    assert relay_queue_status(db_path)["counts"][RELAY_STATUS_PENDING] == 2


def test_relay_enqueue_keeps_legacy_duplicate_behavior_without_dedupe(tmp_path):
    _manifest, manifest_path = make_manifest(tmp_path)
    csv_path = write_csv(tmp_path)
    credentials = make_credentials()
    db_path = tmp_path / "relay.sqlite3"

    first = enqueue_source_file_for_relay(
        db_path=db_path,
        spool_dir=tmp_path / "spool",
        source_file_path=csv_path,
        producer_manifest_path=manifest_path,
        credentials=credentials,
    )
    second = enqueue_source_file_for_relay(
        db_path=db_path,
        spool_dir=tmp_path / "spool",
        source_file_path=csv_path,
        producer_manifest_path=manifest_path,
        credentials=credentials,
    )

    assert second.relay_id != first.relay_id
    assert second.deduped_existing is False
    assert relay_queue_status(db_path)["counts"][RELAY_STATUS_PENDING] == 2


def test_relay_claim_and_stale_lease_reset(tmp_path):
    _manifest, manifest_path = make_manifest(tmp_path)
    csv_path = write_csv(tmp_path)
    credentials = make_credentials()
    db_path = tmp_path / "relay.sqlite3"
    enqueue_source_file_for_relay(
        db_path=db_path,
        spool_dir=tmp_path / "spool",
        source_file_path=csv_path,
        producer_manifest_path=manifest_path,
        credentials=credentials,
    )

    claimed = claim_next_relay_batch(
        db_path=db_path,
        worker_id="worker-1",
        lease_seconds=1,
        now="2999-06-21T00:00:00Z",
    )

    assert claimed is not None
    assert claimed.status == RELAY_STATUS_LEASED
    assert claimed.attempt_count == 1
    assert claim_next_relay_batch(db_path=db_path, worker_id="worker-2", now="2999-06-21T00:00:00Z") is None
    assert reset_stale_relay_leases(db_path=db_path, now="2999-06-21T00:00:02Z") == 1
    status = relay_queue_status(db_path)
    assert status["counts"][RELAY_STATUS_PENDING] == 1


def test_relay_retry_then_success_uses_fresh_signed_request_and_marks_acked(tmp_path):
    manifest, manifest_path = make_manifest(tmp_path)
    csv_path = write_csv(tmp_path)
    credentials = make_credentials()
    db_path = tmp_path / "relay.sqlite3"
    row = enqueue_source_file_for_relay(
        db_path=db_path,
        spool_dir=tmp_path / "spool",
        source_file_path=csv_path,
        producer_manifest_path=manifest_path,
        credentials=credentials,
    )
    server_source_file_id = f"{manifest['pc_identity']['source_host_id']}/label_match/label_match_events/{row.relative_path}"
    session = SequenceFakeSession(
        [
            FakeResponse(
                503,
                {
                    "committed": False,
                    "retryable": True,
                    "error": {"code": "temporary_unavailable", "message": "try later"},
                },
            ),
            FakeResponse(
                200,
                {
                    "request_id": "request-relay-2",
                    "client_batch_id": row.relay_id,
                    "server_source_file_id": server_source_file_id,
                    "committed": True,
                    "status": "accepted",
                    "totals": {"inserted": 1, "replayed": 0, "quarantined": 0, "errors": 0},
                },
            ),
        ]
    )

    first = drain_one_relay_batch(
        db_path=db_path,
        credentials=credentials,
        session=session,
        status_dir=tmp_path / "status",
        retry_base_seconds=1,
    )

    assert first.success is False
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        current = conn.execute("SELECT status FROM direct_sync_relay_batches WHERE relay_id = ?", (row.relay_id,)).fetchone()
        assert current["status"] == RELAY_STATUS_RETRY_WAIT
        conn.execute(
            "UPDATE direct_sync_relay_batches SET next_attempt_at = ? WHERE relay_id = ?",
            ("2026-06-21T00:00:00Z", row.relay_id),
        )
        conn.commit()

    second = drain_one_relay_batch(
        db_path=db_path,
        credentials=credentials,
        session=session,
        status_dir=tmp_path / "status",
        retry_base_seconds=1,
    )

    assert second.success is True
    assert len(session.calls) == 2
    assert session.calls[0]["headers"]["X-Producer-Nonce"] != session.calls[1]["headers"]["X-Producer-Nonce"]
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        current = conn.execute(
            """
            SELECT status, receipt_json, upload_status_path
            FROM direct_sync_relay_batches
            WHERE relay_id = ?
            """,
            (row.relay_id,),
        ).fetchone()
    assert current["status"] == RELAY_STATUS_ACKED
    assert json.loads(current["receipt_json"])["request_id"] == "request-relay-2"
    assert Path(current["upload_status_path"]).is_file()

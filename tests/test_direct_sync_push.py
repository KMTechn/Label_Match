import hashlib
import json
import sqlite3
import typing
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import direct_sync_push as direct_sync_push_module
from direct_sync_push import (
    DEFAULT_ENDPOINT_PATH,
    DEFAULT_PRODUCER_USER_AGENT,
    DirectSyncPushError,
    ProducerCredentials,
    RELAY_STATUS_ACKED,
    RELAY_STATUS_FAILED_PERMANENT,
    RELAY_STATUS_LEASED,
    RELAY_STATUS_OPERATOR_REVIEW,
    RELAY_STATUS_PENDING,
    RELAY_STATUS_RETRY_WAIT,
    acked_relay_retention_candidates,
    build_raw_artifact_restore_url,
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
    restore_metadata_from_upload_metadata,
    restore_raw_artifact_to_file,
    signed_headers,
    upload_source_file,
)


def test_retry_after_seconds_uses_stable_bounded_jitter():
    assert direct_sync_push_module._retry_after_seconds(3, 10) == 30
    delays = {
        direct_sync_push_module._retry_after_seconds(3, 10, f"relay-{index:02d}")
        for index in range(20)
    }

    assert min(delays) >= 30
    assert max(delays) <= 36
    assert len(delays) > 1
    assert direct_sync_push_module._retry_after_seconds(3, 10, "relay-03") == (
        direct_sync_push_module._retry_after_seconds(3, 10, "relay-03")
    )


def test_relay_conflict_type_hints_resolve():
    hints = typing.get_type_hints(direct_sync_push_module._find_conflicting_relay_batch)

    assert hints["plan"] is direct_sync_push_module.SourceFilePlan


def test_upload_status_atomic_json_write_uses_unique_temp_paths(tmp_path, monkeypatch):
    target = tmp_path / "status.json"
    observed_temp_names = []
    original_replace = direct_sync_push_module.os.replace

    def capture_replace(src, dst):
        observed_temp_names.append(Path(src).name)
        return original_replace(src, dst)

    monkeypatch.setattr(direct_sync_push_module.os, "replace", capture_replace)

    direct_sync_push_module._write_json_atomic(target, {"attempt": 1})
    direct_sync_push_module._write_json_atomic(target, {"attempt": 2})

    assert len(observed_temp_names) == 2
    assert len(set(observed_temp_names)) == 2
    assert all(name.startswith("status.json.tmp.") for name in observed_temp_names)
    assert not list(tmp_path.glob("status.json.tmp.*"))
    assert json.loads(target.read_text(encoding="utf-8")) == {"attempt": 2}


class FakeResponse:
    def __init__(self, status_code, payload, *, headers=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = dict(headers or {})

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def post(self, url, *, data, files, headers, timeout, allow_redirects=False):
        file_name, file_handle, content_type = files["file"]
        self.calls.append(
            {
                "url": url,
                "metadata": data["metadata"],
                "headers": dict(headers),
                "timeout": timeout,
                "allow_redirects": allow_redirects,
                "file_name": file_name,
                "file_bytes": file_handle.read(),
                "content_type": content_type,
            }
        )
        return self.response


class FakeRestoreResponse:
    def __init__(self, status_code, body=b"", *, headers=None, payload=None):
        self.status_code = status_code
        self.content = body
        self.headers = dict(headers or {})
        self._payload = payload if payload is not None else {}

    def iter_content(self, chunk_size=1024 * 1024):
        for index in range(0, len(self.content), chunk_size):
            yield self.content[index : index + chunk_size]

    def json(self):
        return self._payload


class FakeRestoreSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, url, *, headers, timeout, stream=False, allow_redirects=False):
        self.calls.append(
            {
                "url": url,
                "headers": dict(headers),
                "timeout": timeout,
                "stream": stream,
                "allow_redirects": allow_redirects,
            }
        )
        return self.response


class SequenceFakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def post(self, url, *, data, files, headers, timeout, allow_redirects=False):
        file_name, file_handle, content_type = files["file"]
        self.calls.append(
            {
                "url": url,
                "metadata": data["metadata"],
                "headers": dict(headers),
                "timeout": timeout,
                "allow_redirects": allow_redirects,
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


def test_restore_raw_artifact_downloads_verified_payload(tmp_path):
    _manifest, manifest_path = make_manifest(tmp_path)
    csv_path = write_csv(tmp_path)
    credentials = make_credentials()
    plan = build_source_file_plan(
        source_file_path=csv_path,
        producer_manifest_path=manifest_path,
        credentials=credentials,
    )
    body = csv_path.read_bytes()
    destination = tmp_path / "spool" / "restored.csv"
    session = FakeRestoreSession(
        FakeRestoreResponse(
            200,
            body,
            headers={
                "X-Content-SHA256": plan.content_sha256,
                "X-Byte-Length": str(plan.byte_length),
            },
        )
    )

    result = restore_raw_artifact_to_file(
        credentials=credentials,
        metadata=plan.metadata,
        destination_path=destination,
        session=session,
    )

    assert result.success is True
    assert destination.read_bytes() == body
    assert session.calls[0]["url"] == build_raw_artifact_restore_url(
        credentials.endpoint_url,
        content_sha256=plan.content_sha256,
        byte_length=plan.byte_length,
    )
    assert json.loads(session.calls[0]["headers"]["X-Producer-Restore-Metadata"]) == (
        restore_metadata_from_upload_metadata(plan.metadata)
    )


def test_restore_raw_artifact_falls_back_when_hardlink_is_unavailable(tmp_path, monkeypatch):
    _manifest, manifest_path = make_manifest(tmp_path)
    csv_path = write_csv(tmp_path)
    credentials = make_credentials()
    plan = build_source_file_plan(
        source_file_path=csv_path,
        producer_manifest_path=manifest_path,
        credentials=credentials,
    )
    body = csv_path.read_bytes()
    destination = tmp_path / "spool" / "restored.csv"
    session = FakeRestoreSession(
        FakeRestoreResponse(
            200,
            body,
            headers={
                "X-Content-SHA256": plan.content_sha256,
                "X-Byte-Length": str(plan.byte_length),
            },
        )
    )

    def hardlink_unavailable(_src, _dst):
        raise OSError("hard links disabled")

    monkeypatch.setattr(direct_sync_push_module.os, "link", hardlink_unavailable)

    result = restore_raw_artifact_to_file(
        credentials=credentials,
        metadata=plan.metadata,
        destination_path=destination,
        session=session,
    )

    assert result.success is True
    assert destination.read_bytes() == body
    assert not list(destination.parent.glob("restored.csv.tmp.*"))


def test_restore_raw_artifact_does_not_overwrite_file_created_during_download(tmp_path, monkeypatch):
    _manifest, manifest_path = make_manifest(tmp_path)
    csv_path = write_csv(tmp_path)
    credentials = make_credentials()
    plan = build_source_file_plan(
        source_file_path=csv_path,
        producer_manifest_path=manifest_path,
        credentials=credentials,
    )
    destination = tmp_path / "spool" / "restored.csv"
    session = FakeRestoreSession(
        FakeRestoreResponse(
            200,
            csv_path.read_bytes(),
            headers={
                "X-Content-SHA256": plan.content_sha256,
                "X-Byte-Length": str(plan.byte_length),
            },
        )
    )

    def race_create_destination(_src, dst):
        Path(dst).write_text("operator-race-copy\n", encoding="utf-8")
        raise FileExistsError

    monkeypatch.setattr(direct_sync_push_module.os, "link", race_create_destination)

    result = restore_raw_artifact_to_file(
        credentials=credentials,
        metadata=plan.metadata,
        destination_path=destination,
        session=session,
    )

    assert result.success is False
    assert result.error_code == "restore_destination_exists"
    assert destination.read_text(encoding="utf-8") == "operator-race-copy\n"
    assert not list(destination.parent.glob("restored.csv.tmp.*"))


def expect_push_error(callable_obj):
    try:
        callable_obj()
    except DirectSyncPushError:
        return
    raise AssertionError("expected DirectSyncPushError")


def test_upload_rejects_unsafe_endpoint_before_signing_or_posting(tmp_path):
    _manifest, manifest_path = make_manifest(tmp_path)
    csv_path = write_csv(tmp_path)
    for endpoint_url in (
        "http://localhost/api/producer-ingest/v1/source-file",
        "https://producer:secret@worker.example.invalid/api/producer-ingest/v1/source-file",
        "https://10.0.0.5/api/producer-ingest/v1/source-file",
        "https://192.168.1.20/api/producer-ingest/v1/source-file",
        "https://169.254.169.254/api/producer-ingest/v1/source-file",
        "https://[fe80::1]/api/producer-ingest/v1/source-file",
        "https://240.0.0.1/api/producer-ingest/v1/source-file",
    ):
        credentials = ProducerCredentials(
            producer_id="producer-label",
            key_id="key-label",
            secret="label-secret",
            endpoint_url=endpoint_url,
        )
        plan = build_source_file_plan(
            source_file_path=csv_path,
            producer_manifest_path=manifest_path,
            credentials=credentials,
        )
        session = FakeSession(FakeResponse(200, {"committed": True}))

        expect_push_error(lambda: upload_source_file(plan, credentials, session=session))

        assert session.calls == []


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
    assert headers["User-Agent"] == DEFAULT_PRODUCER_USER_AGENT
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
                "server_source_file_id": (
                    f"{plan.metadata['source_host_id']}/"
                    f"{plan.metadata['producer_role']}/"
                    f"{plan.metadata['stream_name']}/"
                    f"{plan.metadata['relative_path']}"
                ),
                "committed": True,
                "status": "accepted",
                "retryable": False,
                "next_retry_after": None,
                "totals": {"inserted": 1, "replayed": 0, "quarantined": 0, "errors": 0},
            },
        )
    )

    result = upload_source_file(plan, credentials, session=session, status_dir=tmp_path / "status")

    assert result.success is True
    assert result.committed is True
    assert Path(result.status_path).is_file()
    assert json.loads(session.calls[0]["metadata"]) == plan.metadata
    assert session.calls[0]["headers"]["User-Agent"] == DEFAULT_PRODUCER_USER_AGENT
    assert session.calls[0]["metadata"] == canonical_json(plan.metadata)
    assert session.calls[0]["file_bytes"] == csv_path.read_bytes()
    status_text = Path(result.status_path).read_text(encoding="utf-8")
    assert "label-secret" not in status_text
    assert "X-Producer-Signature" not in status_text


def test_upload_blocks_redirect_without_following_location(tmp_path):
    _manifest, manifest_path = make_manifest(tmp_path)
    csv_path = write_csv(tmp_path)
    credentials = make_credentials()
    plan = build_source_file_plan(
        source_file_path=csv_path,
        producer_manifest_path=manifest_path,
        credentials=credentials,
    )
    session = FakeSession(FakeResponse(302, {"committed": False, "error": {"code": "redirect"}}))

    result = upload_source_file(plan, credentials, session=session, status_dir=tmp_path / "status")

    assert result.success is False
    assert result.committed is False
    assert result.retryable is False
    assert result.error_code == "producer_redirect_not_allowed"
    assert session.calls[0]["allow_redirects"] is False


def test_upload_remote_failure_redacts_server_echoed_sensitive_payload(tmp_path):
    _manifest, manifest_path = make_manifest(tmp_path)
    csv_path = write_csv(tmp_path)
    credentials = make_credentials()
    plan = build_source_file_plan(
        source_file_path=csv_path,
        producer_manifest_path=manifest_path,
        credentials=credentials,
    )

    class EchoingFailureSession:
        def __init__(self):
            self.calls = []

        def post(self, url, *, data, files, headers, timeout, allow_redirects=False):
            file_name, file_handle, content_type = files["file"]
            self.calls.append(
                {
                    "url": url,
                    "metadata": data["metadata"],
                    "headers": dict(headers),
                    "timeout": timeout,
                    "allow_redirects": allow_redirects,
                    "file_name": file_name,
                    "file_bytes": file_handle.read(),
                    "content_type": content_type,
                }
            )
            signature = headers["X-Producer-Signature"]
            return FakeResponse(
                503,
                {
                    "committed": False,
                    "retryable": True,
                    "error": {
                        "code": "ingest_write_disabled",
                        "message": (
                            "disabled Authorization: Bearer SHOULD-NOT-LEAK "
                            f"{signature} X-Producer-Signature "
                            f"{direct_sync_push_module.SIGNATURE_VERSION} {credentials.secret}\nnext"
                        ),
                    },
                    "echo": ["Authorization: Bearer SHOULD-NOT-LEAK", signature],
                    "X-Producer-Signature": "server-echo-key",
                },
            )

    session = EchoingFailureSession()

    result = upload_source_file(plan, credentials, session=session, status_dir=tmp_path / "status")

    assert result.success is False
    assert result.committed is False
    assert result.retryable is True
    assert result.error_code == "ingest_write_disabled"
    assert "\n" not in result.error_message
    status_text = Path(result.status_path).read_text(encoding="utf-8")
    combined_text = json.dumps(result.receipt, ensure_ascii=False) + status_text + result.error_message
    for leaked in (
        "SHOULD-NOT-LEAK",
        credentials.secret,
        session.calls[0]["headers"]["X-Producer-Signature"],
        "X-Producer-Signature",
        direct_sync_push_module.SIGNATURE_VERSION,
        "Authorization",
    ):
        assert leaked not in combined_text


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


def test_relay_enqueue_dedupes_same_completed_file_and_blocks_changed_content(tmp_path):
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
    with pytest.raises(DirectSyncPushError, match="source file content conflict"):
        enqueue_source_file_for_relay(
            db_path=db_path,
            spool_dir=spool_dir,
            source_file_path=csv_path,
            producer_manifest_path=manifest_path,
            credentials=credentials,
            dedupe_existing=True,
        )

    assert relay_queue_status(db_path)["counts"][RELAY_STATUS_PENDING] == 1
    assert len(list(spool_dir.iterdir())) == 1


def test_relay_enqueue_repairs_missing_pending_spool_on_dedupe(tmp_path):
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
    Path(first.spooled_file_path).unlink()

    repaired = enqueue_source_file_for_relay(
        db_path=db_path,
        spool_dir=spool_dir,
        source_file_path=csv_path,
        producer_manifest_path=manifest_path,
        credentials=credentials,
        dedupe_existing=True,
    )

    assert repaired.relay_id == first.relay_id
    assert repaired.deduped_existing is True
    assert Path(repaired.spooled_file_path).read_bytes() == csv_path.read_bytes()
    assert relay_queue_status(db_path)["counts"][RELAY_STATUS_PENDING] == 1
    assert len(list(spool_dir.iterdir())) == 1


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


def test_stale_relay_worker_cannot_overwrite_reclaimed_lease(tmp_path):
    _manifest, manifest_path = make_manifest(tmp_path)
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

    first = claim_next_relay_batch(
        db_path=db_path,
        worker_id="worker-1",
        lease_seconds=1,
        now="2999-06-21T00:00:00Z",
    )
    assert first is not None
    assert reset_stale_relay_leases(db_path=db_path, now="2999-06-21T00:00:02Z") == 1
    second = claim_next_relay_batch(
        db_path=db_path,
        worker_id="worker-2",
        lease_seconds=300,
        now="2999-06-21T00:00:03Z",
    )
    assert second is not None

    with pytest.raises(DirectSyncPushError, match="lease is no longer owned"):
        direct_sync_push_module._set_relay_status(
            db_path=db_path,
            relay_id=row.relay_id,
            lease_owner="worker-1",
            status=RELAY_STATUS_ACKED,
            receipt={"request_id": "late-worker-1"},
        )

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        current = conn.execute(
            """
            SELECT status, lease_owner, receipt_json
            FROM direct_sync_relay_batches
            WHERE relay_id = ?
            """,
            (row.relay_id,),
        ).fetchone()
    assert current["status"] == RELAY_STATUS_LEASED
    assert current["lease_owner"] == "worker-2"
    assert current["receipt_json"] is None


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
                    "retryable": False,
                    "next_retry_after": None,
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


def test_drain_retry_wait_uses_retry_after_header(tmp_path):
    _manifest, manifest_path = make_manifest(tmp_path)
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
    session = FakeSession(
        FakeResponse(
            503,
            {
                "committed": False,
                "retryable": True,
                "error": {"code": "temporary_unavailable", "message": "try later"},
            },
            headers={"Retry-After": "120"},
        )
    )

    before = datetime.now(timezone.utc)
    result = drain_one_relay_batch(
        db_path=db_path,
        credentials=credentials,
        session=session,
        status_dir=tmp_path / "status",
        retry_base_seconds=1,
    )
    after = datetime.now(timezone.utc)

    assert result is not None
    assert result.success is False
    assert result.retry_after_seconds == 120
    status_artifact = json.loads(Path(result.status_path).read_text(encoding="utf-8"))
    assert status_artifact["retry_after_seconds"] == 120
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        current = conn.execute(
            "SELECT status, next_attempt_at FROM direct_sync_relay_batches WHERE relay_id = ?",
            (row.relay_id,),
        ).fetchone()
    assert current["status"] == RELAY_STATUS_RETRY_WAIT
    scheduled = datetime.fromisoformat(current["next_attempt_at"].replace("Z", "+00:00"))
    assert before + timedelta(seconds=120) <= scheduled <= after + timedelta(seconds=120)


def test_drain_retry_wait_preserves_zero_retry_after_header(tmp_path):
    _manifest, manifest_path = make_manifest(tmp_path)
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
    session = FakeSession(
        FakeResponse(
            503,
            {
                "committed": False,
                "retryable": True,
                "error": {"code": "temporary_unavailable", "message": "try now"},
            },
            headers={"Retry-After": "0"},
        )
    )

    before = datetime.now(timezone.utc)
    result = drain_one_relay_batch(
        db_path=db_path,
        credentials=credentials,
        session=session,
        status_dir=tmp_path / "status",
        retry_base_seconds=60,
    )
    after = datetime.now(timezone.utc)

    assert result is not None
    assert result.retry_after_seconds == 0
    status_artifact = json.loads(Path(result.status_path).read_text(encoding="utf-8"))
    assert status_artifact["retry_after_seconds"] == 0
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        current = conn.execute(
            "SELECT status, next_attempt_at FROM direct_sync_relay_batches WHERE relay_id = ?",
            (row.relay_id,),
        ).fetchone()
    assert current["status"] == RELAY_STATUS_RETRY_WAIT
    scheduled = datetime.fromisoformat(current["next_attempt_at"].replace("Z", "+00:00"))
    assert before <= scheduled <= after


def test_drain_non_2xx_committed_response_requires_operator_review_without_retry(tmp_path):
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
    session = FakeSession(
        FakeResponse(
            503,
            {
                "request_id": "request-claimed-committed",
                "client_batch_id": row.relay_id,
                "server_source_file_id": server_source_file_id,
                "committed": True,
                "status": "accepted",
                "retryable": False,
                "next_retry_after": None,
                "totals": {"inserted": 1, "replayed": 0, "quarantined": 0, "errors": 0},
            },
        )
    )

    result = drain_one_relay_batch(
        db_path=db_path,
        credentials=credentials,
        session=session,
        status_dir=tmp_path / "status",
        retry_base_seconds=1,
    )

    assert result is not None
    assert result.success is False
    assert result.committed is True
    assert result.retryable is False
    assert result.error_code == "producer_committed_status_mismatch"
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        current = conn.execute(
            """
            SELECT status, next_attempt_at, last_error_code, receipt_json
            FROM direct_sync_relay_batches
            WHERE relay_id = ?
            """,
            (row.relay_id,),
        ).fetchone()
    assert current["status"] == RELAY_STATUS_OPERATOR_REVIEW
    assert current["next_attempt_at"] is None
    assert current["last_error_code"] == "producer_committed_status_mismatch"
    assert json.loads(current["receipt_json"])["request_id"] == "request-claimed-committed"


def test_drain_uses_enqueued_metadata_snapshot_after_manifest_changes(tmp_path):
    manifest, manifest_path = make_manifest(tmp_path)
    csv_path = write_csv(tmp_path)
    credentials = make_credentials()
    db_path = tmp_path / "relay.sqlite3"
    original_manifest_hash = manifest_hash(manifest)
    row = enqueue_source_file_for_relay(
        db_path=db_path,
        spool_dir=tmp_path / "spool",
        source_file_path=csv_path,
        producer_manifest_path=manifest_path,
        credentials=credentials,
    )
    manifest["pc_identity"]["source_host_id"] = "changed-host-after-enqueue"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")

    class EchoAcceptedSession:
        def __init__(self):
            self.calls = []

        def post(self, url, *, data, files, headers, timeout, allow_redirects=False):
            metadata = json.loads(data["metadata"])
            self.calls.append(metadata)
            return FakeResponse(
                200,
                {
                    "request_id": "request-snapshot",
                    "client_batch_id": metadata["client_batch_id"],
                    "server_source_file_id": (
                        f"{metadata['source_host_id']}/"
                        f"{metadata['producer_role']}/"
                        f"{metadata['stream_name']}/"
                        f"{metadata['relative_path']}"
                    ),
                    "committed": True,
                    "status": "accepted",
                    "retryable": False,
                    "next_retry_after": None,
                    "totals": {"inserted": 1, "replayed": 0, "quarantined": 0, "errors": 0},
                },
            )

    session = EchoAcceptedSession()

    result = drain_one_relay_batch(
        db_path=db_path,
        credentials=credentials,
        session=session,
        status_dir=tmp_path / "status",
    )

    assert result is not None
    assert result.success is True
    assert session.calls[0]["client_batch_id"] == row.relay_id
    assert session.calls[0]["source_host_id"] == "label-host-1"
    assert session.calls[0]["manifest_hash"] == original_manifest_hash
    assert session.calls[0]["idempotency_key"].startswith("source-file:label-host-1/")


def test_drain_rejects_non_integer_relay_metadata_byte_length_without_upload(tmp_path):
    _manifest, manifest_path = make_manifest(tmp_path)
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
    metadata = dict(row.metadata)
    metadata["byte_length"] = str(row.byte_length)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE direct_sync_relay_batches SET metadata_json = ? WHERE relay_id = ?",
            (json.dumps(metadata, ensure_ascii=False, sort_keys=True), row.relay_id),
        )
        conn.commit()
    session = FakeSession(FakeResponse(200, {"committed": True}))

    result = drain_one_relay_batch(
        db_path=db_path,
        credentials=credentials,
        session=session,
        status_dir=tmp_path / "status",
    )

    assert result is not None
    assert result.error_code == "relay_metadata_invalid"
    assert session.calls == []
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        current = conn.execute(
            "SELECT status, last_error_code, receipt_json FROM direct_sync_relay_batches WHERE relay_id = ?",
            (row.relay_id,),
        ).fetchone()
    assert current["status"] == RELAY_STATUS_OPERATOR_REVIEW
    assert current["last_error_code"] == "relay_metadata_invalid"
    assert json.loads(current["receipt_json"]) == {"client_batch_id": row.relay_id}


def test_drain_rejects_changed_producer_credentials_without_upload(tmp_path):
    _manifest, manifest_path = make_manifest(tmp_path)
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
    changed_credentials = ProducerCredentials(
        producer_id=credentials.producer_id,
        key_id="key-label-rotated",
        secret=credentials.secret,
        endpoint_url=credentials.endpoint_url,
    )
    session = FakeSession(FakeResponse(200, {"committed": True}))

    result = drain_one_relay_batch(
        db_path=db_path,
        credentials=changed_credentials,
        session=session,
        status_dir=tmp_path / "status",
    )

    assert result is not None
    assert result.error_code == "relay_credentials_changed"
    assert session.calls == []
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        current = conn.execute(
            "SELECT status, last_error_code, receipt_json FROM direct_sync_relay_batches WHERE relay_id = ?",
            (row.relay_id,),
        ).fetchone()
    assert current["status"] == RELAY_STATUS_OPERATOR_REVIEW
    assert current["last_error_code"] == "relay_credentials_changed"
    assert json.loads(current["receipt_json"]) == {"client_batch_id": row.relay_id}


def test_drain_marks_missing_spooled_file_failed_permanent(tmp_path):
    _manifest, manifest_path = make_manifest(tmp_path)
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
    Path(row.spooled_file_path).unlink()
    session = FakeSession(FakeResponse(200, {"committed": True}))

    result = drain_one_relay_batch(
        db_path=db_path,
        credentials=credentials,
        session=session,
        status_dir=tmp_path / "status",
    )

    assert result is not None
    assert result.success is False
    assert result.retryable is False
    assert result.error_code == "spooled_file_missing"
    assert session.calls == []
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        current = conn.execute(
            "SELECT status, last_error_code, receipt_json FROM direct_sync_relay_batches WHERE relay_id = ?",
            (row.relay_id,),
        ).fetchone()
    assert current["status"] == RELAY_STATUS_FAILED_PERMANENT
    assert current["last_error_code"] == "spooled_file_missing"
    assert json.loads(current["receipt_json"]) == {"client_batch_id": row.relay_id}


def test_drain_upload_exception_after_claim_releases_lease_to_operator_review(tmp_path, monkeypatch):
    _manifest, manifest_path = make_manifest(tmp_path)
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

    def fake_upload(*args, **kwargs):
        raise RuntimeError("runtime-secret C:\\sensitive\\path")

    monkeypatch.setattr(direct_sync_push_module, "upload_source_file", fake_upload)

    result = drain_one_relay_batch(
        db_path=db_path,
        credentials=credentials,
        worker_id="worker-1",
        status_dir=tmp_path / "status",
    )

    assert result is not None
    assert result.success is False
    assert result.committed is False
    assert result.retryable is False
    assert result.error_code == "upload_unhandled_exception"
    assert "RuntimeError" in result.error_message
    assert "runtime-secret" not in result.error_message
    assert "sensitive" not in result.error_message
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        current = conn.execute(
            """
            SELECT status, lease_owner, attempt_count, last_error_code, last_error_message, receipt_json
            FROM direct_sync_relay_batches
            WHERE relay_id = ?
            """,
            (row.relay_id,),
        ).fetchone()
    assert current["status"] == RELAY_STATUS_OPERATOR_REVIEW
    assert current["lease_owner"] is None
    assert current["attempt_count"] == 1
    assert current["last_error_code"] == "upload_unhandled_exception"
    assert current["last_error_message"] == result.error_message
    assert "runtime-secret" not in current["last_error_message"]
    assert "sensitive" not in current["last_error_message"]
    assert json.loads(current["receipt_json"]) == {"client_batch_id": row.relay_id}


def test_drain_acks_committed_upload_when_status_artifact_write_fails(tmp_path):
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
    status_dir = tmp_path / "status-is-a-file"
    status_dir.write_text("not a directory", encoding="utf-8")
    server_source_file_id = f"{manifest['pc_identity']['source_host_id']}/label_match/label_match_events/{row.relative_path}"
    session = FakeSession(
        FakeResponse(
            200,
            {
                "request_id": "request-status-write-failed",
                "client_batch_id": row.relay_id,
                "server_source_file_id": server_source_file_id,
                "committed": True,
                "status": "accepted",
                "retryable": False,
                "next_retry_after": None,
                "totals": {"inserted": 1, "replayed": 0, "quarantined": 0, "errors": 0},
            },
        )
    )

    result = drain_one_relay_batch(
        db_path=db_path,
        credentials=credentials,
        session=session,
        status_dir=status_dir,
    )

    assert result is not None
    assert result.success is True
    assert result.committed is True
    assert result.status_path == ""
    assert result.error_code == "upload_status_write_failed"
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        current = conn.execute(
            """
            SELECT status, upload_status_path, last_error_code, receipt_json
            FROM direct_sync_relay_batches
            WHERE relay_id = ?
            """,
            (row.relay_id,),
        ).fetchone()
    assert current["status"] == RELAY_STATUS_ACKED
    assert current["upload_status_path"] == ""
    assert current["last_error_code"] == "upload_status_write_failed"
    assert json.loads(current["receipt_json"])["request_id"] == "request-status-write-failed"


def test_acked_relay_retention_report_is_read_only_and_candidates_require_full_evidence(tmp_path):
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
    receipt = {
        "request_id": "request-retention",
        "upload_id": "request-retention",
        "client_batch_id": row.relay_id,
        "server_source_file_id": server_source_file_id,
        "committed": True,
        "status": "accepted",
        "retryable": False,
        "next_retry_after": None,
        "totals": {"inserted": 1, "replayed": 0, "quarantined": 0, "errors": 0},
    }
    result = drain_one_relay_batch(
        db_path=db_path,
        credentials=credentials,
        session=FakeSession(FakeResponse(200, receipt)),
        status_dir=tmp_path / "status",
    )

    assert result.success is True
    retention = relay_queue_status(db_path)["acked_retention"]
    assert retention["status"] == "RETAIN_REQUIRED"
    assert retention["cleanup_safe"] is False
    assert retention["acked_row_delete_allowed"] is False
    assert retention["acked_spool_delete_allowed"] is False
    assert retention["acked_upload_status_delete_allowed"] is False
    assert retention["acked_count"] == 1
    assert retention["acked_spool_total_bytes"] == Path(row.spooled_file_path).stat().st_size
    assert retention["missing_acked_spool_count"] == 0
    assert retention["missing_acked_upload_status_count"] == 0

    candidates = acked_relay_retention_candidates(db_path)
    assert len(candidates) == 1
    assert candidates[0].relay_id == row.relay_id
    assert candidates[0].receipt == receipt
    assert len(
        acked_relay_retention_candidates(
            db_path,
            spool_roots=[tmp_path / "spool"],
            artifact_roots=[tmp_path / "status"],
        )
    ) == 1
    assert acked_relay_retention_candidates(db_path, spool_roots=[tmp_path / "wrong-spool"]) == ()
    assert acked_relay_retention_candidates(db_path, artifact_roots=[tmp_path / "wrong-status"]) == ()

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE direct_sync_relay_batches SET receipt_json = ? WHERE relay_id = ?",
            (json.dumps({"committed": True, "client_batch_id": row.relay_id}), row.relay_id),
        )
        conn.commit()
    assert acked_relay_retention_candidates(db_path) == ()

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE direct_sync_relay_batches SET receipt_json = ? WHERE relay_id = ?",
            (json.dumps(receipt, ensure_ascii=False, sort_keys=True), row.relay_id),
        )
        conn.commit()
    status_receipt_mismatch = dict(receipt)
    status_receipt_mismatch["request_id"] = "request-wrong-status-artifact"
    Path(candidates[0].upload_status_path).write_text(
        json.dumps(
            {
                "success": True,
                "committed": True,
                "retryable": False,
                "receipt": status_receipt_mismatch,
                "metadata": candidates[0].metadata,
                "source_file_path": candidates[0].spooled_file_path,
            }
        ),
        encoding="utf-8",
    )
    assert acked_relay_retention_candidates(db_path) == ()

    Path(candidates[0].upload_status_path).write_text(
        json.dumps({"success": True, "committed": True, "receipt": receipt, "metadata": {"relative_path": "wrong"}}),
        encoding="utf-8",
    )
    assert acked_relay_retention_candidates(db_path) == ()


def test_relay_status_and_retention_candidates_do_not_create_missing_db(tmp_path):
    db_path = tmp_path / "missing-relay.sqlite3"

    status = relay_queue_status(db_path)

    assert status["counts"] == {}
    assert status["acked_retention"]["read_only"] is True
    assert status["acked_retention"]["cleanup_safe"] is False
    assert acked_relay_retention_candidates(db_path) == ()
    assert not db_path.exists()

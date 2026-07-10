import concurrent.futures
import hashlib
import json
import sqlite3
import threading
from pathlib import Path

import pytest

import direct_sync_runtime
from direct_sync_operator import pause_relay, resume_relay
from direct_sync_push import (
    DirectSyncPushError,
    RELAY_STATUS_ACKED,
    RELAY_STATUS_FAILED_PERMANENT,
    RELAY_STATUS_LEASED,
    RELAY_STATUS_OPERATOR_REVIEW,
    RELAY_STATUS_PENDING,
    RELAY_STATUS_RETRY_WAIT,
    ProducerCredentials,
    build_source_file_plan,
    claim_next_relay_batch,
    manifest_hash,
    relay_queue_status,
    upload_source_file,
)
from direct_sync_runtime import DirectSyncRuntimeConfig, enqueue_completed_source_file, load_credentials_from_json, run_relay_once


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


class RaisingSession:
    def __init__(self):
        self.calls = []

    def post(self, url, *, data, files, headers, timeout, allow_redirects=False):
        self.calls.append({"url": url, "headers": dict(headers), "timeout": timeout, "allow_redirects": allow_redirects})
        raise TimeoutError("Authorization: Bearer SHOULD-NOT-LEAK raw_payload")


class EchoAcceptedSession:
    def __init__(self):
        self.calls = []

    def post(self, url, *, data, files, headers, timeout, allow_redirects=False):
        file_name, file_handle, content_type = files["file"]
        metadata = json.loads(data["metadata"])
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
        return FakeResponse(
            200,
            {
                "request_id": f"request-{metadata['client_batch_id']}",
                "client_batch_id": metadata["client_batch_id"],
                "server_source_file_id": (
                    f"{metadata['source_host_id']}/{metadata['producer_role']}/"
                    f"{metadata['stream_name']}/{metadata['relative_path']}"
                ),
                "committed": True,
                "status": "accepted",
                "retryable": False,
                "next_retry_after": None,
                "totals": {"inserted": 1, "replayed": 0, "quarantined": 0, "errors": 0},
            },
        )


class QuarantineAcceptedSession:
    def __init__(self):
        self.calls = []

    def post(self, url, *, data, files, headers, timeout, allow_redirects=False):
        file_name, file_handle, content_type = files["file"]
        metadata = json.loads(data["metadata"])
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
        return FakeResponse(
            200,
            {
                "request_id": f"request-{metadata['client_batch_id']}",
                "client_batch_id": metadata["client_batch_id"],
                "server_source_file_id": (
                    f"{metadata['source_host_id']}/{metadata['producer_role']}/"
                    f"{metadata['stream_name']}/{metadata['relative_path']}"
                ),
                "committed": True,
                "status": "accepted",
                "retryable": False,
                "next_retry_after": None,
                "totals": {"inserted": 0, "replayed": 0, "quarantined": 1, "errors": 0},
            },
        )


def test_runtime_status_atomic_json_write_uses_unique_temp_paths(tmp_path, monkeypatch):
    target = tmp_path / "runtime_status.json"
    observed_temp_names = []
    original_replace = direct_sync_runtime.os.replace

    def capture_replace(src, dst):
        observed_temp_names.append(Path(src).name)
        return original_replace(src, dst)

    monkeypatch.setattr(direct_sync_runtime.os, "replace", capture_replace)

    direct_sync_runtime._write_json_atomic(target, {"attempt": 1})
    direct_sync_runtime._write_json_atomic(target, {"attempt": 2})

    assert len(observed_temp_names) == 2
    assert len(set(observed_temp_names)) == 2
    assert all(name.startswith("runtime_status.json.tmp.") for name in observed_temp_names)
    assert not list(tmp_path.glob("runtime_status.json.tmp.*"))
    assert json.loads(target.read_text(encoding="utf-8")) == {"attempt": 2}


class ReplayAwareAcceptedSession:
    def __init__(self):
        self.calls = []
        self._seen_by_idempotency_key = {}
        self._lock = threading.Lock()

    def post(self, url, *, data, files, headers, timeout, allow_redirects=False):
        file_name, file_handle, content_type = files["file"]
        metadata = json.loads(data["metadata"])
        file_bytes = file_handle.read()
        idempotency_key = metadata["idempotency_key"]
        with self._lock:
            prior_request_id = self._seen_by_idempotency_key.get(idempotency_key)
            replayed = prior_request_id is not None
            request_id = prior_request_id or f"request-{metadata['client_batch_id']}"
            self._seen_by_idempotency_key.setdefault(idempotency_key, request_id)
            self.calls.append(
                {
                    "url": url,
                    "metadata": data["metadata"],
                    "headers": dict(headers),
                    "timeout": timeout,
                    "allow_redirects": allow_redirects,
                    "file_name": file_name,
                    "file_bytes": file_bytes,
                    "content_type": content_type,
                    "replayed": replayed,
                }
            )
        return FakeResponse(
            200,
            {
                "request_id": request_id,
                "client_batch_id": metadata["client_batch_id"],
                "server_source_file_id": (
                    f"{metadata['source_host_id']}/{metadata['producer_role']}/"
                    f"{metadata['stream_name']}/{metadata['relative_path']}"
                ),
                "committed": True,
                "status": "accepted",
                "retryable": False,
                "next_retry_after": None,
                "totals": {"inserted": 0 if replayed else 1, "replayed": 1 if replayed else 0, "quarantined": 0, "errors": 0},
            },
        )


def make_manifest(
    tmp_path,
    *,
    pc_id="LABEL-PC01",
    source_host_id="label-runtime-host-1",
    producer_install_id="install-label-runtime-1",
):
    manifest = {
        "schema_version": "producer-onboarding-manifest-v1",
        "pc_identity": {
            "pc_id": pc_id,
            "source_host_id": source_host_id,
            "producer_install_id": producer_install_id,
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


def write_csv(tmp_path, *, name="label_runtime.csv", barcode="BC-1"):
    path = tmp_path / name
    path.write_text(
        "timestamp,worker_name,event,details\n"
        f"2026-06-22T00:00:00,worker,LABEL_MATCHED,\"{{ \"\"product_barcode\"\": \"\"{barcode}\"\" }}\"\n",
        encoding="utf-8",
    )
    return path


def write_credential_file(
    tmp_path,
    *,
    producer_id="producer-runtime-1",
    key_id="key-runtime-1",
    secret="runtime-secret",
):
    path = tmp_path / "credential.json"
    path.write_text(
        json.dumps(
            {
                "producer_id": producer_id,
                "key_id": key_id,
                "secret": secret,
                "endpoint_url": "https://worker.example.invalid/api/producer-ingest/v1/source-file",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path


def test_load_credentials_supports_env_secret_ref(monkeypatch, tmp_path):
    monkeypatch.setenv("LABEL_RUNTIME_SECRET", "runtime-secret-from-env")
    path = tmp_path / "credential-ref.json"
    path.write_text(
        json.dumps(
            {
                "producer_id": "producer-runtime-1",
                "key_id": "key-runtime-1",
                "secret_ref": "env:LABEL_RUNTIME_SECRET",
                "endpoint_url": "https://worker.example.invalid/api/producer-ingest/v1/source-file",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    credentials = load_credentials_from_json(path)

    assert credentials.secret == "runtime-secret-from-env"
    assert "runtime-secret-from-env" not in path.read_text(encoding="utf-8")


@pytest.mark.parametrize(
    "secret_ref",
    [
        "env:bad/name",
        "env:CON",
        "dpapi:.hidden",
        "wincred:producer-key.",
    ],
)
def test_load_credentials_blocks_unsafe_secret_ref_target_before_resolution(tmp_path, secret_ref):
    path = tmp_path / "credential-ref.json"
    path.write_text(
        json.dumps(
            {
                "producer_id": "producer-runtime-1",
                "key_id": "key-runtime-1",
                "secret_ref": secret_ref,
                "endpoint_url": "https://worker.example.invalid/api/producer-ingest/v1/source-file",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    with pytest.raises(DirectSyncPushError, match="secret_ref target name is unsafe"):
        load_credentials_from_json(path)


def test_load_credentials_blocks_raw_secret_in_production_profile(monkeypatch, tmp_path):
    path = write_credential_file(tmp_path)
    monkeypatch.setenv("APP_ENV", "production")

    with pytest.raises(DirectSyncPushError, match="raw credential secret is disabled in production"):
        load_credentials_from_json(path)


def test_load_credentials_blocks_env_secret_ref_in_production_profile(monkeypatch, tmp_path):
    monkeypatch.setenv("LABEL_RUNTIME_SECRET", "runtime-secret-from-env")
    monkeypatch.setenv("APP_ENV", "production")
    path = tmp_path / "credential-ref.json"
    path.write_text(
        json.dumps(
            {
                "producer_id": "producer-runtime-1",
                "key_id": "key-runtime-1",
                "secret_ref": "env:LABEL_RUNTIME_SECRET",
                "endpoint_url": "https://worker.example.invalid/api/producer-ingest/v1/source-file",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    with pytest.raises(DirectSyncPushError, match="env secret_ref is disabled in production"):
        load_credentials_from_json(path)


def make_config(tmp_path, *, min_free_bytes=0, max_active_queue_count=0, max_active_queue_age_seconds=0):
    _manifest, manifest_path = make_manifest(tmp_path)
    credential_path = write_credential_file(tmp_path)
    return DirectSyncRuntimeConfig(
        db_path=tmp_path / "direct_sync_relay.sqlite3",
        spool_dir=tmp_path / "spool",
        producer_manifest_path=manifest_path,
        credential_path=credential_path,
        upload_status_dir=tmp_path / "upload_status",
        runtime_status_path=tmp_path / "runtime_status" / "status.json",
        log_path=tmp_path / "logs" / "relay.jsonl",
        min_free_bytes=min_free_bytes,
        retry_base_seconds=1,
        timeout_seconds=5,
        operator_pause_path=tmp_path / "control" / "pause.json",
        max_active_queue_count=max_active_queue_count,
        max_active_queue_age_seconds=max_active_queue_age_seconds,
    )


def test_runtime_rejects_unsafe_endpoint_before_posting_or_claiming(tmp_path):
    config = make_config(tmp_path)
    csv_path = write_csv(tmp_path)
    valid_credentials = ProducerCredentials(
        producer_id="producer-runtime-1",
        key_id="key-runtime-1",
        secret="runtime-secret",
        endpoint_url="https://worker.example.invalid/api/producer-ingest/v1/source-file",
    )
    enqueue_status = enqueue_completed_source_file(
        config,
        source_file_path=csv_path,
        credentials=valid_credentials,
    )
    assert enqueue_status["status"] == "enqueued"
    Path(config.credential_path).write_text(
        json.dumps(
            {
                "producer_id": "producer-runtime-1",
                "key_id": "key-runtime-1",
                "secret": "runtime-secret",
                "endpoint_url": "http://localhost/api/producer-ingest/v1/source-file",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    session = EchoAcceptedSession()

    status = run_relay_once(config, session=session)

    assert status["status"] == "runtime_error"
    assert "endpoint_url" in status["error_message"]
    assert session.calls == []
    queue = relay_queue_status(config.db_path)
    assert queue["counts"][RELAY_STATUS_PENDING] == 1
    assert queue["counts"].get(RELAY_STATUS_LEASED, 0) == 0


def test_runtime_corrupt_relay_db_records_runtime_error_without_posting(tmp_path):
    config = make_config(tmp_path)
    Path(config.db_path).write_text("not a sqlite database", encoding="utf-8")
    session = EchoAcceptedSession()

    status = run_relay_once(config, session=session)

    assert status["status"] == "runtime_error"
    assert status["error_code"] == "relay_queue_db_error"
    assert status["error_message"] == "relay queue database error: DatabaseError"
    assert status["queue"]["status"] == "unavailable"
    assert status["queue"]["error_code"] == "relay_queue_db_error"
    assert "not a sqlite database" not in json.dumps(status)
    assert session.calls == []


def assert_runtime_artifacts_are_redacted(config):
    status_bytes = Path(config.runtime_status_path).read_bytes()
    log_bytes = Path(config.log_path).read_bytes()
    assert b"runtime-secret" not in status_bytes
    assert b"runtime-secret" not in log_bytes
    assert b"X-Producer-Signature" not in status_bytes
    assert b"X-Producer-Signature" not in log_bytes
    assert b"PRODUCER-HMAC-SHA256-V1" not in status_bytes
    assert b"PRODUCER-HMAC-SHA256-V1" not in log_bytes


def test_runtime_empty_queue_writes_idle_status_without_posting(tmp_path):
    config = make_config(tmp_path)
    session = EchoAcceptedSession()

    status = run_relay_once(config, session=session)

    assert status["status"] == "idle"
    assert status["queue"]["counts"] == {}
    assert session.calls == []
    assert Path(config.runtime_status_path).is_file()
    assert Path(config.log_path).is_file()


def test_runtime_status_and_log_bind_manifest_source_identity(tmp_path):
    config = make_config(tmp_path)
    manifest = json.loads(Path(config.producer_manifest_path).read_text(encoding="utf-8"))
    manifest_sha256 = hashlib.sha256(Path(config.producer_manifest_path).read_bytes()).hexdigest()
    expected_manifest_hash = manifest_hash(manifest)
    expected_source_scope = "label-runtime-host-1/label_match/label_match_events"
    source_file = write_csv(tmp_path)

    enqueue_completed_source_file(config, source_file_path=source_file)
    status = run_relay_once(config, session=EchoAcceptedSession())

    persisted = json.loads(Path(config.runtime_status_path).read_text(encoding="utf-8"))
    log_entry = json.loads(Path(config.log_path).read_text(encoding="utf-8").strip().splitlines()[-1])
    assert status["status"] == "acked"
    assert status["producer_manifest_sha256"] == manifest_sha256
    assert status["manifest_hash"] == expected_manifest_hash
    assert status["source_identity"]["status"] == "PASS"
    assert status["source_identity"]["source_scope_key"] == expected_source_scope
    assert status["source_identity"]["source_scope_key_sha256"] == hashlib.sha256(
        expected_source_scope.encode("utf-8")
    ).hexdigest()
    assert persisted["source_identity"]["source_scope_key"] == expected_source_scope
    assert persisted["producer_manifest_sha256"] == manifest_sha256
    assert log_entry["source_identity"]["source_scope_key"] == expected_source_scope
    assert log_entry["producer_manifest_sha256"] == manifest_sha256
    assert log_entry["manifest_hash"] == expected_manifest_hash


def test_runtime_operator_pause_blocks_enqueue_and_drain_before_credentials(tmp_path):
    base_config = make_config(tmp_path)
    config = DirectSyncRuntimeConfig(
        db_path=base_config.db_path,
        spool_dir=base_config.spool_dir,
        producer_manifest_path=base_config.producer_manifest_path,
        credential_path=tmp_path / "missing_credential.json",
        upload_status_dir=base_config.upload_status_dir,
        runtime_status_path=base_config.runtime_status_path,
        log_path=base_config.log_path,
        min_free_bytes=base_config.min_free_bytes,
        retry_base_seconds=base_config.retry_base_seconds,
        timeout_seconds=base_config.timeout_seconds,
        operator_pause_path=base_config.operator_pause_path,
    )
    source_file = write_csv(tmp_path)
    pause_relay(pause_path=config.operator_pause_path, operator_id="operator-a", reason="local maintenance")
    session = EchoAcceptedSession()

    enqueue_status = enqueue_completed_source_file(config, source_file_path=source_file)
    run_status = run_relay_once(config, session=session)

    assert enqueue_status["status"] == "paused_by_operator"
    assert run_status["status"] == "paused_by_operator"
    assert run_status["operator_control"]["paused"] is True
    assert run_status["disk"]["status"] == "not_checked"
    assert relay_queue_status(config.db_path)["counts"] == {}
    assert session.calls == []


def test_runtime_operator_resume_allows_enqueue_and_drain(tmp_path):
    config = make_config(tmp_path)
    source_file = write_csv(tmp_path)
    pause_relay(pause_path=config.operator_pause_path, operator_id="operator-a", reason="local maintenance")
    resume_relay(pause_path=config.operator_pause_path, operator_id="operator-a", reason="maintenance complete")

    enqueue_status = enqueue_completed_source_file(config, source_file_path=source_file)
    run_status = run_relay_once(config, session=EchoAcceptedSession())

    assert enqueue_status["status"] == "enqueued"
    assert run_status["status"] == "acked"
    assert relay_queue_status(config.db_path)["counts"][RELAY_STATUS_ACKED] == 1


def test_runtime_enqueue_writes_status_and_redacted_log(tmp_path):
    config = make_config(tmp_path)
    source_file = write_csv(tmp_path)

    status = enqueue_completed_source_file(config, source_file_path=source_file)

    assert status["status"] == "enqueued"
    assert status["queue"]["counts"][RELAY_STATUS_PENDING] == 1
    assert Path(config.runtime_status_path).is_file()
    assert Path(config.log_path).is_file()
    assert_runtime_artifacts_are_redacted(config)


def test_runtime_invalid_utf8_source_file_writes_enqueue_error_status(tmp_path):
    config = make_config(tmp_path)
    source_file = tmp_path / "label_runtime_bad.csv"
    source_file.write_bytes(
        b"timestamp,worker_name,event,details\n"
        b"2026-06-22T00:00:00,worker,LABEL_MATCHED,\xff\n"
    )

    status = enqueue_completed_source_file(config, source_file_path=source_file)

    assert status["status"] == "enqueue_error"
    assert status["error_code"] == "direct_sync_source_file_error"
    assert "UnicodeDecodeError" in status["error_message"]
    assert status["queue"]["counts"] == {}
    assert Path(config.runtime_status_path).is_file()
    assert Path(config.log_path).is_file()
    assert relay_queue_status(config.db_path)["counts"] == {}
    assert_runtime_artifacts_are_redacted(config)


def test_runtime_repeated_source_scan_reuses_existing_relay_row(tmp_path):
    config = make_config(tmp_path)
    source_file = write_csv(tmp_path)

    first = enqueue_completed_source_file(config, source_file_path=source_file)
    duplicate = enqueue_completed_source_file(config, source_file_path=source_file)

    assert duplicate["status"] == "enqueued"
    assert duplicate["last_result"]["relay_id"] == first["last_result"]["relay_id"]
    assert duplicate["last_result"]["deduped_existing"] is True
    assert relay_queue_status(config.db_path)["counts"][RELAY_STATUS_PENDING] == 1
    assert len(list(Path(config.spool_dir).iterdir())) == 1
    assert_runtime_artifacts_are_redacted(config)


def test_runtime_backpressure_blocks_enqueue_before_credentials_and_allows_drain(tmp_path):
    config = make_config(tmp_path)
    source_file = write_csv(tmp_path)
    enqueue_completed_source_file(config, source_file_path=source_file)
    blocked_config = DirectSyncRuntimeConfig(
        **{
            **config.__dict__,
            "credential_path": tmp_path / "missing_credential.json",
            "max_active_queue_count": 1,
        }
    )

    blocked = enqueue_completed_source_file(blocked_config, source_file_path=source_file)
    drained = run_relay_once(
        DirectSyncRuntimeConfig(**{**config.__dict__, "max_active_queue_count": 1}),
        session=EchoAcceptedSession(),
    )

    assert blocked["status"] == "blocked_queue_backpressure"
    assert blocked["queue_backpressure"]["status"] == "blocked"
    assert blocked["queue_backpressure"]["reasons"] == ["active_queue_count_threshold"]
    assert blocked["disk"]["status"] == "not_checked"
    assert drained["status"] == "acked"
    assert relay_queue_status(config.db_path)["counts"][RELAY_STATUS_ACKED] == 1
    assert_runtime_artifacts_are_redacted(blocked_config)


def test_runtime_backpressure_recovers_old_active_queue_before_current_enqueue(tmp_path):
    config = make_config(tmp_path)
    stale_source = write_csv(tmp_path, name="stale_label.csv", barcode="BC-STALE")
    current_source = write_csv(tmp_path, name="current_label.csv", barcode="BC-CURRENT")
    enqueue_completed_source_file(config, source_file_path=stale_source)
    with sqlite3.connect(config.db_path) as conn:
        conn.execute(
            "UPDATE direct_sync_relay_batches SET created_at = ?",
            ("2000-01-01T00:00:00Z",),
        )
    aged_config = DirectSyncRuntimeConfig(
        **{
            **config.__dict__,
            "max_active_queue_age_seconds": 1,
        }
    )
    session = EchoAcceptedSession()

    current = enqueue_completed_source_file(
        aged_config,
        source_file_path=current_source,
        backpressure_recovery_session=session,
    )

    assert current["status"] == "enqueued"
    assert current["queue_backpressure"]["status"] == "pass"
    assert current["queue_backpressure"]["recovery"]["attempted"] is True
    assert current["queue_backpressure"]["recovery"]["attempt_count"] == 1
    assert current["queue_backpressure"]["recovery"]["resolved"] is True
    assert len(session.calls) == 1
    counts = relay_queue_status(config.db_path)["counts"]
    assert counts[RELAY_STATUS_ACKED] == 1
    assert counts[RELAY_STATUS_PENDING] == 1
    assert_runtime_artifacts_are_redacted(aged_config)


def test_runtime_backpressure_keeps_current_enqueue_blocked_when_aged_recovery_is_retryable(tmp_path):
    config = make_config(tmp_path)
    stale_source = write_csv(tmp_path, name="stale_label.csv", barcode="BC-STALE")
    current_source = write_csv(tmp_path, name="current_label.csv", barcode="BC-CURRENT")
    enqueue_completed_source_file(config, source_file_path=stale_source)
    with sqlite3.connect(config.db_path) as conn:
        conn.execute(
            "UPDATE direct_sync_relay_batches SET created_at = ?",
            ("2000-01-01T00:00:00Z",),
        )
    aged_config = DirectSyncRuntimeConfig(
        **{
            **config.__dict__,
            "max_active_queue_age_seconds": 1,
        }
    )
    session = RaisingSession()

    blocked = enqueue_completed_source_file(
        aged_config,
        source_file_path=current_source,
        backpressure_recovery_session=session,
    )

    assert blocked["status"] == "blocked_queue_backpressure"
    assert blocked["queue_backpressure"]["recovery"]["attempted"] is True
    assert blocked["queue_backpressure"]["recovery"]["attempt_count"] == 1
    assert blocked["queue_backpressure"]["recovery"]["resolved"] is False
    assert len(session.calls) == 1
    counts = relay_queue_status(config.db_path)["counts"]
    assert counts[RELAY_STATUS_RETRY_WAIT] == 1
    assert counts.get(RELAY_STATUS_PENDING, 0) == 0
    assert_runtime_artifacts_are_redacted(aged_config)


def test_runtime_repeated_source_scan_after_ack_does_not_requeue(tmp_path):
    config = make_config(tmp_path)
    source_file = write_csv(tmp_path)
    enqueued = enqueue_completed_source_file(config, source_file_path=source_file)
    session = EchoAcceptedSession()

    acked = run_relay_once(config, session=session)
    duplicate = enqueue_completed_source_file(config, source_file_path=source_file)

    assert acked["status"] == "acked"
    assert duplicate["last_result"]["relay_id"] == enqueued["last_result"]["relay_id"]
    assert duplicate["last_result"]["relay_status"] == RELAY_STATUS_ACKED
    assert duplicate["last_result"]["deduped_existing"] is True
    assert relay_queue_status(config.db_path)["counts"][RELAY_STATUS_ACKED] == 1
    assert len(session.calls) == 1
    assert_runtime_artifacts_are_redacted(config)


def test_runtime_once_acks_batch_and_records_local_status(tmp_path):
    config = make_config(tmp_path)
    source_file = write_csv(tmp_path)
    enqueued = enqueue_completed_source_file(config, source_file_path=source_file)
    session = EchoAcceptedSession()

    status = run_relay_once(config, session=session)

    assert status["status"] == "acked"
    assert status["last_result"]["success"] is True
    assert status["last_result"]["relay_id"] == enqueued["last_result"]["relay_id"]
    assert relay_queue_status(config.db_path)["counts"][RELAY_STATUS_ACKED] == 1
    assert relay_queue_status(config.db_path)["oldest_active_created_at"] == ""
    assert len(session.calls) == 1
    assert session.calls[0]["headers"]["X-Producer-Nonce"]
    assert_runtime_artifacts_are_redacted(config)


def test_runtime_spool_digest_mismatch_blocks_before_post(tmp_path):
    config = make_config(tmp_path)
    source_file = write_csv(tmp_path)
    enqueue_completed_source_file(config, source_file_path=source_file)
    spooled_file = next(Path(config.spool_dir).iterdir())
    spooled_file.write_bytes(spooled_file.read_bytes() + b"\n# tampered after enqueue\n")
    session = EchoAcceptedSession()

    status = run_relay_once(config, session=session)

    assert status["status"] == "failed_permanent"
    assert status["last_result"]["error_code"] == "spooled_file_digest_mismatch"
    assert relay_queue_status(config.db_path)["counts"][RELAY_STATUS_FAILED_PERMANENT] == 1
    assert session.calls == []


def test_runtime_retryable_failure_records_retry_wait_and_skips_early_retry(tmp_path):
    config = make_config(tmp_path)
    source_file = write_csv(tmp_path)
    enqueue_completed_source_file(config, source_file_path=source_file)
    retry_session = FakeSession(
        FakeResponse(
            503,
            {
                "committed": False,
                "retryable": True,
                "error": {"code": "temporary_unavailable", "message": "try later"},
            },
        )
    )

    status = run_relay_once(config, session=retry_session)

    assert status["status"] == "retry_wait"
    assert status["last_result"]["retryable"] is True
    assert relay_queue_status(config.db_path)["counts"][RELAY_STATUS_RETRY_WAIT] == 1
    with sqlite3.connect(config.db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT next_attempt_at, last_error_code, upload_status_path
            FROM direct_sync_relay_batches
            """
        ).fetchone()
    assert row["next_attempt_at"]
    assert row["last_error_code"] == "temporary_unavailable"
    assert Path(row["upload_status_path"]).is_file()

    early_success = EchoAcceptedSession()
    idle = run_relay_once(config, session=early_success)

    assert idle["status"] == "idle"
    assert early_success.calls == []


def test_runtime_transport_exception_records_retry_wait_and_redacts_status(tmp_path):
    config = make_config(tmp_path)
    source_file = write_csv(tmp_path)
    enqueue_completed_source_file(config, source_file_path=source_file)
    session = RaisingSession()

    status = run_relay_once(config, session=session)

    assert status["status"] == "retry_wait"
    assert status["last_result"]["retryable"] is True
    assert status["last_result"]["status_code"] == 0
    assert status["last_result"]["error_code"] == "transport_error"
    assert "SHOULD-NOT-LEAK" not in json.dumps(status)
    assert relay_queue_status(config.db_path)["counts"][RELAY_STATUS_RETRY_WAIT] == 1
    with sqlite3.connect(config.db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT last_error_code, upload_status_path
            FROM direct_sync_relay_batches
            """
        ).fetchone()
    assert row["last_error_code"] == "transport_error"
    status_text = Path(row["upload_status_path"]).read_text(encoding="utf-8")
    assert "TimeoutError" in status_text
    assert "SHOULD-NOT-LEAK" not in status_text


def test_runtime_credential_error_redacts_sensitive_exception_text(monkeypatch, tmp_path):
    config = make_config(tmp_path)
    source_file = write_csv(tmp_path)
    enqueue_completed_source_file(config, source_file_path=source_file)
    session = EchoAcceptedSession()

    def fake_load_credentials(_path):
        raise DirectSyncPushError(
            "Authorization: Bearer SHOULD-NOT-LEAK\n"
            "secret=SHOULD-NOT-LEAK token=SHOULD-NOT-LEAK "
            "X-Producer-Signature PRODUCER-HMAC-SHA256-V1"
        )

    monkeypatch.setattr(direct_sync_runtime, "load_credentials_from_json", fake_load_credentials)

    status = run_relay_once(config, session=session)

    assert status["status"] == "runtime_error"
    assert status["error_code"] == "direct_sync_runtime_error"
    assert "secret=[redacted]" in status["error_message"]
    assert "\n" not in status["error_message"]
    assert session.calls == []
    status_text = Path(config.runtime_status_path).read_text(encoding="utf-8")
    log_text = Path(config.log_path).read_text(encoding="utf-8")
    combined_text = json.dumps(status, ensure_ascii=False) + status_text + log_text
    for leaked in (
        "SHOULD-NOT-LEAK",
        "Authorization",
        "X-Producer-Signature",
        "PRODUCER-HMAC-SHA256-V1",
    ):
        assert leaked not in combined_text


def test_runtime_enqueue_credential_error_redacts_sensitive_exception_text(monkeypatch, tmp_path):
    config = make_config(tmp_path)
    source_file = write_csv(tmp_path)

    def fake_load_credentials(_path):
        raise DirectSyncPushError(
            "credential load failed secret=DO_NOT_EXPOSE token=DO_NOT_EXPOSE "
            "signature=DO_NOT_EXPOSE\nnext"
        )

    monkeypatch.setattr(direct_sync_runtime, "load_credentials_from_json", fake_load_credentials)

    status = enqueue_completed_source_file(config, source_file_path=source_file)

    assert status["status"] == "enqueue_error"
    assert status["error_code"] == "direct_sync_enqueue_error"
    assert "DO_NOT_EXPOSE" not in status["error_message"]
    assert "\n" not in status["error_message"]
    assert "secret=[redacted]" in status["error_message"]
    assert "token=[redacted]" in status["error_message"]
    assert "signature=[redacted]" in status["error_message"]
    combined_text = (
        json.dumps(status, ensure_ascii=False)
        + Path(config.runtime_status_path).read_text(encoding="utf-8")
        + Path(config.log_path).read_text(encoding="utf-8")
    )
    assert "DO_NOT_EXPOSE" not in combined_text


def test_runtime_committed_with_conflict_moves_to_operator_review(tmp_path):
    config = make_config(tmp_path)
    source_file = write_csv(tmp_path)
    enqueue_completed_source_file(config, source_file_path=source_file)
    session = QuarantineAcceptedSession()

    status = run_relay_once(config, session=session)

    assert status["status"] == "operator_review"
    assert status["error_code"] == "operator_review_required"
    assert "quarantined=1" in status["error_message"]
    assert status["last_result"]["committed"] is True
    assert status["last_result"]["error_code"] == "operator_review_required"
    assert status["last_result"]["receipt_totals"]["quarantined"] == 1
    assert relay_queue_status(config.db_path)["counts"][RELAY_STATUS_OPERATOR_REVIEW] == 1


def test_runtime_permanent_failure_moves_to_failed_permanent(tmp_path):
    config = make_config(tmp_path)
    source_file = write_csv(tmp_path)
    enqueue_completed_source_file(config, source_file_path=source_file)
    session = FakeSession(
        FakeResponse(
            400,
            {
                "committed": False,
                "retryable": False,
                "error": {"code": "metadata_invalid", "message": "bad metadata"},
            },
        )
    )

    status = run_relay_once(config, session=session)

    assert status["status"] == "failed_permanent"
    assert status["last_result"]["error_code"] == "metadata_invalid"
    assert relay_queue_status(config.db_path)["counts"][RELAY_STATUS_FAILED_PERMANENT] == 1


def test_runtime_disk_pressure_blocks_without_claiming_pending_batch(tmp_path):
    normal_config = make_config(tmp_path)
    source_file = write_csv(tmp_path)
    enqueue_completed_source_file(normal_config, source_file_path=source_file)
    blocked_config = make_config(tmp_path, min_free_bytes=10**20)
    session = EchoAcceptedSession()

    status = run_relay_once(blocked_config, session=session)

    assert status["status"] == "blocked_disk_pressure"
    assert relay_queue_status(normal_config.db_path)["counts"][RELAY_STATUS_PENDING] == 1
    assert session.calls == []
    assert_runtime_artifacts_are_redacted(blocked_config)


def test_runtime_resets_stale_lease_after_reboot_like_pause(tmp_path):
    config = make_config(tmp_path)
    source_file = write_csv(tmp_path)
    enqueue_completed_source_file(config, source_file_path=source_file)
    claimed = claim_next_relay_batch(
        db_path=config.db_path,
        worker_id="previous-process",
        lease_seconds=1,
        now="2099-01-01T00:00:00Z",
    )
    assert claimed is not None
    session = EchoAcceptedSession()

    status = run_relay_once(config, session=session, now="2099-01-01T00:00:02Z")

    assert status["status"] == "acked"
    assert status["stale_leases_reset"] == 1
    assert relay_queue_status(config.db_path)["counts"][RELAY_STATUS_ACKED] == 1


def test_runtime_lost_ack_retry_reuses_same_batch_and_idempotency_after_stale_lease(tmp_path):
    config = make_config(tmp_path)
    source_file = write_csv(tmp_path)
    enqueue_completed_source_file(config, source_file_path=source_file)
    claimed = claim_next_relay_batch(
        db_path=config.db_path,
        worker_id="crashed-process",
        lease_seconds=1,
        now="2099-01-01T00:00:00Z",
    )
    assert claimed is not None
    credentials = ProducerCredentials(
        producer_id="producer-runtime-1",
        key_id="key-runtime-1",
        secret="runtime-secret",
        endpoint_url="https://worker.example.invalid/api/producer-ingest/v1/source-file",
    )
    plan = build_source_file_plan(
        source_file_path=claimed.spooled_file_path,
        producer_manifest_path=claimed.producer_manifest_path,
        credentials=credentials,
        relative_path=claimed.relative_path,
        client_batch_id=claimed.relay_id,
    )
    committed_but_unacked = EchoAcceptedSession()
    upload = upload_source_file(
        plan,
        credentials,
        session=committed_but_unacked,
        status_dir=tmp_path / "crash_status",
    )
    assert upload.success is True

    retry_session = EchoAcceptedSession()
    retry = run_relay_once(config, session=retry_session, now="2099-01-01T00:00:02Z")

    assert retry["status"] == "acked"
    assert retry["stale_leases_reset"] == 1
    first_metadata = json.loads(committed_but_unacked.calls[0]["metadata"])
    retry_metadata = json.loads(retry_session.calls[0]["metadata"])
    assert first_metadata["client_batch_id"] == retry_metadata["client_batch_id"] == claimed.relay_id
    assert first_metadata["idempotency_key"] == retry_metadata["idempotency_key"]
    assert first_metadata["content_sha256"] == retry_metadata["content_sha256"]
    assert relay_queue_status(config.db_path)["counts"][RELAY_STATUS_ACKED] == 1


def test_runtime_twenty_label_match_pcs_lost_ack_replay_preserves_https_identity_scope(tmp_path):
    session = ReplayAwareAcceptedSession()
    pc_roots = [tmp_path / f"pc_{index:03d}" for index in range(1, 21)]
    upload_start_barrier = threading.Barrier(len(pc_roots))

    def run_pc(index, pc_root):
        pc_root.mkdir(parents=True, exist_ok=True)
        manifest, manifest_path = make_manifest(
            pc_root,
            pc_id=f"LABEL-PC-{index:03d}",
            source_host_id=f"label-runtime-host-{index:03d}",
            producer_install_id=f"install-label-runtime-{index:03d}",
        )
        credential_path = write_credential_file(
            pc_root,
            producer_id=f"producer-label-{index:03d}",
            key_id=f"key-label-{index:03d}",
            secret=f"runtime-secret-label-{index:03d}",
        )
        config = DirectSyncRuntimeConfig(
            db_path=pc_root / "direct_sync_relay.sqlite3",
            spool_dir=pc_root / "spool",
            producer_manifest_path=manifest_path,
            credential_path=credential_path,
            upload_status_dir=pc_root / "upload_status",
            runtime_status_path=pc_root / "runtime_status" / "status.json",
            log_path=pc_root / "logs" / "relay.jsonl",
            retry_base_seconds=1,
            timeout_seconds=5,
            operator_pause_path=pc_root / "control" / "pause.json",
        )
        source_file = write_csv(
            pc_root,
            name=f"label_runtime_{index:03d}.csv",
            barcode=f"BC-LABEL-{index:03d}",
        )
        enqueued = enqueue_completed_source_file(config, source_file_path=source_file)
        assert enqueued["status"] == "enqueued"
        claimed = claim_next_relay_batch(
            db_path=config.db_path,
            worker_id=f"crashed-process-{index:03d}",
            lease_seconds=1,
            now="2099-01-01T00:00:00Z",
        )
        assert claimed is not None
        credentials = ProducerCredentials(
            producer_id=f"producer-label-{index:03d}",
            key_id=f"key-label-{index:03d}",
            secret=f"runtime-secret-label-{index:03d}",
            endpoint_url="https://worker.example.invalid/api/producer-ingest/v1/source-file",
        )
        plan = build_source_file_plan(
            source_file_path=claimed.spooled_file_path,
            producer_manifest_path=claimed.producer_manifest_path,
            credentials=credentials,
            relative_path=claimed.relative_path,
            client_batch_id=claimed.relay_id,
        )
        upload_start_barrier.wait(timeout=30)
        first_upload = upload_source_file(
            plan,
            credentials,
            session=session,
            status_dir=pc_root / "crash_status",
        )
        assert first_upload.success is True
        retry = run_relay_once(config, session=session, now="2099-01-01T00:00:02Z")
        assert retry["status"] == "acked"
        assert retry["stale_leases_reset"] == 1
        assert retry["last_result"]["relay_id"] == claimed.relay_id
        assert relay_queue_status(config.db_path)["counts"][RELAY_STATUS_ACKED] == 1
        return {
            "relay_id": claimed.relay_id,
            "source_host_id": manifest["pc_identity"]["source_host_id"],
            "producer_install_id": manifest["pc_identity"]["producer_install_id"],
            "producer_id": credentials.producer_id,
        }

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(pc_roots)) as executor:
        results = list(executor.map(lambda item: run_pc(*item), enumerate(pc_roots, start=1)))

    calls_by_key = {}
    for call in session.calls:
        metadata = json.loads(call["metadata"])
        calls_by_key.setdefault(metadata["idempotency_key"], []).append((call, metadata))

    assert len(results) == 20
    assert len(calls_by_key) == 20
    assert len(session.calls) == 40
    assert len({row["source_host_id"] for row in results}) == 20
    assert len({row["producer_install_id"] for row in results}) == 20
    assert len({row["producer_id"] for row in results}) == 20
    for idempotency_key, keyed_calls in calls_by_key.items():
        assert len(keyed_calls) == 2
        first_call, first_metadata = keyed_calls[0]
        retry_call, retry_metadata = keyed_calls[1]
        assert first_call["url"] == retry_call["url"] == "https://worker.example.invalid/api/producer-ingest/v1/source-file"
        assert first_call["replayed"] is False
        assert retry_call["replayed"] is True
        assert first_metadata["idempotency_key"] == retry_metadata["idempotency_key"] == idempotency_key
        assert first_metadata["client_batch_id"] == retry_metadata["client_batch_id"]
        assert first_metadata["content_sha256"] == retry_metadata["content_sha256"]
        assert first_metadata["source_host_id"] == retry_metadata["source_host_id"]
        assert first_metadata["producer_install_id"] == retry_metadata["producer_install_id"]
        assert first_metadata["producer_role"] == retry_metadata["producer_role"] == "label_match"
        assert first_metadata["stream_name"] == retry_metadata["stream_name"] == "label_match_events"
        assert first_metadata["source_transport"] == retry_metadata["source_transport"] == "legacy_packaging_csv"

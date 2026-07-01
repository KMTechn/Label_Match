import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import direct_sync_operator as direct_sync_operator_module
from direct_sync_operator import operator_status, retry_dead_relay_batch
from direct_sync_push import (
    RELAY_STATUS_ACKED,
    RELAY_STATUS_FAILED_PERMANENT,
    RELAY_STATUS_PENDING,
    RELAY_STATUS_RETRY_WAIT,
    relay_queue_status,
)
from direct_sync_runtime import enqueue_completed_source_file, load_credentials_from_json, run_relay_once
from tests.test_direct_sync_runtime import EchoAcceptedSession, FakeResponse, FakeSession, make_config, write_csv
from tools.direct_sync_relay_operator import main


class RestoreResponse:
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


class RestoreSession:
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


def _acked_restore_case(tmp_path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    config = make_config(tmp_path)
    source_file = write_csv(tmp_path)
    enqueued = enqueue_completed_source_file(config, source_file_path=source_file)
    relay_id = enqueued["last_result"]["relay_id"]
    assert run_relay_once(config, session=EchoAcceptedSession())["status"] == "acked"
    with sqlite3.connect(config.db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT spooled_file_path
            FROM direct_sync_relay_batches
            WHERE relay_id = ?
            """,
            (relay_id,),
        ).fetchone()
    return config, relay_id, load_credentials_from_json(config.credential_path), Path(row["spooled_file_path"])


def _set_relay_spool_path(db_path, relay_id, path):
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE direct_sync_relay_batches SET spooled_file_path = ? WHERE relay_id = ?",
            (str(path), relay_id),
        )
        conn.commit()


def test_operator_status_pause_and_resume_write_redacted_evidence(tmp_path):
    config = make_config(tmp_path)
    audit_log_path = tmp_path / "logs" / "operator.jsonl"
    pause_report_path = tmp_path / "reports" / "pause.json"
    status_report_path = tmp_path / "reports" / "status.json"

    assert main(["pause", "--operator-pause-path", str(config.operator_pause_path), "--operator-id", "operator-a", "--reason", "local maintenance", "--audit-log-path", str(audit_log_path), "--report-path", str(pause_report_path)]) == 0
    pause_report = json.loads(pause_report_path.read_text(encoding="utf-8-sig"))
    assert pause_report["status"] == "PASS"
    assert pause_report["pause"]["paused"] is True

    assert main(["status", "--db-path", str(config.db_path), "--operator-pause-path", str(config.operator_pause_path), "--report-path", str(status_report_path)]) == 0
    status_report = json.loads(status_report_path.read_text(encoding="utf-8-sig"))
    assert status_report["status"] == "PASS"
    assert status_report["pause"]["paused"] is True
    assert status_report["queue"]["counts"] == {}

    assert main(["resume", "--operator-pause-path", str(config.operator_pause_path), "--operator-id", "operator-a", "--reason", "maintenance complete", "--audit-log-path", str(audit_log_path)]) == 0
    assert not Path(config.operator_pause_path).exists()
    audit_bytes = audit_log_path.read_bytes()
    assert b"runtime-secret" not in audit_bytes
    assert b"X-Producer-Signature" not in audit_bytes


def test_operator_relay_db_connection_uses_busy_timeout(tmp_path):
    config = make_config(tmp_path)
    source_file = write_csv(tmp_path)
    enqueue_completed_source_file(config, source_file_path=source_file)

    conn = direct_sync_operator_module._connect_relay_db(config.db_path)
    try:
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] >= direct_sync_operator_module.SQLITE_BUSY_TIMEOUT_MS
    finally:
        conn.close()


def test_operator_retry_dead_only_allows_failed_permanent_rows(tmp_path):
    config = make_config(tmp_path)
    source_file = write_csv(tmp_path)
    enqueued = enqueue_completed_source_file(config, source_file_path=source_file)
    relay_id = enqueued["last_result"]["relay_id"]
    failed = run_relay_once(
        config,
        session=FakeSession(
            FakeResponse(
                400,
                {
                    "committed": False,
                    "retryable": False,
                    "error": {"code": "metadata_invalid", "message": "bad metadata"},
                },
            )
        ),
    )
    assert failed["status"] == "failed_permanent"
    assert relay_queue_status(config.db_path)["counts"][RELAY_STATUS_FAILED_PERMANENT] == 1

    retry_report = retry_dead_relay_batch(
        db_path=config.db_path,
        relay_id=relay_id,
        operator_id="operator-a",
        reason="server contract fixed",
        audit_log_path=tmp_path / "logs" / "operator.jsonl",
    )

    assert retry_report["status"] == "PASS"
    assert retry_report["previous_status"] == RELAY_STATUS_FAILED_PERMANENT
    assert retry_report["new_status"] == RELAY_STATUS_PENDING
    with sqlite3.connect(config.db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT status, lease_owner, lease_expires_at, next_attempt_at, last_error_code
            FROM direct_sync_relay_batches
            WHERE relay_id = ?
            """,
            (relay_id,),
        ).fetchone()
    assert row["status"] == RELAY_STATUS_PENDING
    assert row["lease_owner"] is None
    assert row["lease_expires_at"] is None
    assert row["next_attempt_at"] is None
    assert row["last_error_code"] is None

    acked = run_relay_once(config, session=EchoAcceptedSession())
    assert acked["status"] == "acked"
    assert relay_queue_status(config.db_path)["counts"][RELAY_STATUS_ACKED] == 1


def test_operator_restore_spool_downloads_missing_acked_file_from_server(tmp_path):
    config = make_config(tmp_path)
    source_file = write_csv(tmp_path)
    enqueued = enqueue_completed_source_file(config, source_file_path=source_file)
    relay_id = enqueued["last_result"]["relay_id"]
    assert run_relay_once(config, session=EchoAcceptedSession())["status"] == "acked"
    with sqlite3.connect(config.db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT spooled_file_path, content_sha256, byte_length
            FROM direct_sync_relay_batches
            WHERE relay_id = ?
            """,
            (relay_id,),
        ).fetchone()
    spooled_path = Path(row["spooled_file_path"])
    body = spooled_path.read_bytes()
    spooled_path.unlink()
    session = RestoreSession(
        RestoreResponse(
            200,
            body,
            headers={
                "X-Content-SHA256": row["content_sha256"],
                "X-Byte-Length": str(row["byte_length"]),
            },
        )
    )

    report = direct_sync_operator_module.restore_relay_spool_from_server(
        db_path=config.db_path,
        relay_id=relay_id,
        spool_root=config.spool_dir,
        credentials=load_credentials_from_json(config.credential_path),
        operator_id="operator-a",
        reason="restore deleted local spool",
        audit_log_path=tmp_path / "logs" / "operator.jsonl",
        session=session,
    )

    assert report["status"] == "PASS"
    assert report["operation"] == "restore-spool"
    assert report["restored"] is True
    assert spooled_path.read_bytes() == body
    assert session.calls[0]["stream"] is True
    assert session.calls[0]["allow_redirects"] is False


def test_operator_restore_spool_blocks_guard_violations_without_server_call(tmp_path, monkeypatch):
    session = RestoreSession(RestoreResponse(200, b""))
    config, relay_id, credentials, _spooled_path = _acked_restore_case(tmp_path / "credential")
    report = direct_sync_operator_module.restore_relay_spool_from_server(
        db_path=config.db_path,
        relay_id=relay_id,
        spool_root=config.spool_dir,
        credentials=replace(credentials, endpoint_url="https://other.example.invalid/api/producer-ingest/v1/source-file"),
        operator_id="operator-a",
        reason="credential mismatch",
        session=session,
    )
    assert report["status"] == "BLOCKED"
    assert report["error_code"] == "relay_credential_binding_mismatch"
    assert session.calls == []

    config, relay_id, credentials, _spooled_path = _acked_restore_case(tmp_path / "outside")
    _set_relay_spool_path(config.db_path, relay_id, tmp_path / "outside-root" / "payload.bin")
    report = direct_sync_operator_module.restore_relay_spool_from_server(
        db_path=config.db_path,
        relay_id=relay_id,
        spool_root=config.spool_dir,
        credentials=credentials,
        operator_id="operator-a",
        reason="outside root",
        session=session,
    )
    assert report["status"] == "BLOCKED"
    assert report["error_code"] == "spooled_file_outside_spool_root"

    config, relay_id, credentials, spooled_path = _acked_restore_case(tmp_path / "symlink")
    link_path = spooled_path.with_name(f"{spooled_path.name}.link")
    _set_relay_spool_path(config.db_path, relay_id, link_path)
    original_is_symlink = Path.is_symlink

    def fake_is_symlink(path):
        if path == link_path:
            return True
        return original_is_symlink(path)

    monkeypatch.setattr(Path, "is_symlink", fake_is_symlink)
    report = direct_sync_operator_module.restore_relay_spool_from_server(
        db_path=config.db_path,
        relay_id=relay_id,
        spool_root=config.spool_dir,
        credentials=credentials,
        operator_id="operator-a",
        reason="symlink",
        session=session,
    )
    assert report["status"] == "BLOCKED"
    assert report["error_code"] == "spooled_file_symlink"

    config, relay_id, credentials, spooled_path = _acked_restore_case(tmp_path / "mismatch")
    spooled_path.write_bytes(spooled_path.read_bytes() + b"changed\n")
    report = direct_sync_operator_module.restore_relay_spool_from_server(
        db_path=config.db_path,
        relay_id=relay_id,
        spool_root=config.spool_dir,
        credentials=credentials,
        operator_id="operator-a",
        reason="existing mismatch",
        session=session,
    )
    assert report["status"] == "BLOCKED"
    assert report["error_code"] == "spooled_file_already_exists_mismatch"
    assert session.calls == []


def test_operator_retry_dead_blocks_operator_review_rows(tmp_path):
    config = make_config(tmp_path)
    source_file = write_csv(tmp_path)
    enqueued = enqueue_completed_source_file(config, source_file_path=source_file)
    relay_id = enqueued["last_result"]["relay_id"]
    reviewed = run_relay_once(
        config,
        session=FakeSession(
            FakeResponse(
                200,
                {
                    "request_id": "request-review",
                    "client_batch_id": relay_id,
                    "committed": True,
                    "status": "accepted",
                    "retryable": False,
                    "next_retry_after": None,
                    "totals": {"inserted": 0, "replayed": 0, "quarantined": 1, "errors": 0},
                },
            )
        ),
    )

    retry_report = retry_dead_relay_batch(
        db_path=config.db_path,
        relay_id=relay_id,
        operator_id="operator-a",
        reason="operator review needs server reconcile",
    )

    assert reviewed["status"] == "operator_review"
    assert retry_report["status"] == "BLOCKED"
    assert retry_report["previous_status"] == "operator_review"
    assert relay_queue_status(config.db_path)["counts"].get("operator_review") == 1


def test_operator_retry_dead_blocks_live_pending_retry_wait_and_missing_rows(tmp_path):
    config = make_config(tmp_path)
    source_file = write_csv(tmp_path)
    enqueued = enqueue_completed_source_file(config, source_file_path=source_file)
    relay_id = enqueued["last_result"]["relay_id"]

    pending_report = retry_dead_relay_batch(db_path=config.db_path, relay_id=relay_id, operator_id="operator-a", reason="not allowed")
    missing_report = retry_dead_relay_batch(db_path=config.db_path, relay_id="relay-missing", operator_id="operator-a", reason="not allowed")
    with sqlite3.connect(config.db_path) as conn:
        conn.execute(
            """
            UPDATE direct_sync_relay_batches
            SET status = ?, next_attempt_at = ?
            WHERE relay_id = ?
            """,
            (RELAY_STATUS_RETRY_WAIT, "2999-01-01T00:00:00Z", relay_id),
        )
        conn.commit()
    retry_wait_report = retry_dead_relay_batch(db_path=config.db_path, relay_id=relay_id, operator_id="operator-a", reason="not allowed")

    assert pending_report["status"] == "BLOCKED"
    assert pending_report["error_code"] == "relay_status_not_retryable_by_operator"
    assert missing_report["status"] == "BLOCKED"
    assert missing_report["error_code"] == "relay_not_found"
    assert retry_wait_report["status"] == "BLOCKED"
    assert retry_wait_report["previous_status"] == RELAY_STATUS_RETRY_WAIT
    assert operator_status(db_path=config.db_path, pause_path=config.operator_pause_path)["status"] == "PASS"


def test_operator_status_does_not_create_missing_queue_db(tmp_path):
    db_path = tmp_path / "missing" / "relay.sqlite3"

    report = operator_status(db_path=db_path, pause_path=tmp_path / "control" / "pause.json")

    assert report["status"] == "PASS"
    assert report["queue"]["status"] == "not_initialized"
    assert report["queue"]["counts"] == {}
    assert not db_path.exists()


def test_operator_status_error_messages_are_redacted(tmp_path, monkeypatch):
    db_path = tmp_path / "relay.sqlite3"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db_path.write_text("placeholder", encoding="utf-8")

    def fail_open(*args, **kwargs):
        raise sqlite3.OperationalError("SECRET Authorization: Bearer SHOULD-NOT-LEAK")

    monkeypatch.setattr(direct_sync_operator_module.sqlite3, "connect", fail_open)

    report = operator_status(db_path=db_path, pause_path=tmp_path / "control" / "pause.json")

    assert report["queue"]["error_code"] == "relay_db_open_failed"
    assert report["queue"]["error_message"] == "relay queue database error: OperationalError"
    assert "SHOULD-NOT-LEAK" not in json.dumps(report, ensure_ascii=False)


def test_operator_status_schema_error_message_is_redacted(tmp_path, monkeypatch):
    db_path = tmp_path / "relay.sqlite3"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db_path.write_text("placeholder", encoding="utf-8")

    class FailingConnection:
        row_factory = None

        def execute(self, query, *args, **kwargs):
            if str(query).lstrip().upper().startswith("PRAGMA"):
                return self
            raise sqlite3.DatabaseError("schema SECRET token=SHOULD-NOT-LEAK")

        def close(self):
            pass

    monkeypatch.setattr(direct_sync_operator_module.sqlite3, "connect", lambda *args, **kwargs: FailingConnection())

    report = operator_status(db_path=db_path, pause_path=tmp_path / "control" / "pause.json")

    assert report["queue"]["error_code"] == "relay_db_schema_unavailable"
    assert report["queue"]["error_message"] == "relay queue database error: DatabaseError"
    assert "SHOULD-NOT-LEAK" not in json.dumps(report, ensure_ascii=False)

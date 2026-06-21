import json
import sqlite3
from pathlib import Path

from direct_sync_operator import operator_status, retry_dead_relay_batch
from direct_sync_push import (
    RELAY_STATUS_ACKED,
    RELAY_STATUS_FAILED_PERMANENT,
    RELAY_STATUS_PENDING,
    RELAY_STATUS_RETRY_WAIT,
    relay_queue_status,
)
from direct_sync_runtime import enqueue_completed_source_file, run_relay_once
from tests.test_direct_sync_runtime import EchoAcceptedSession, FakeResponse, FakeSession, make_config, write_csv
from tools.direct_sync_relay_operator import main


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

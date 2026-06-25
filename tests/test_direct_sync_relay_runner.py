import importlib.util
import json
import os
import sqlite3
import time
from pathlib import Path

import pytest

from direct_sync_push import (
    RELAY_STATUS_ACKED,
    RELAY_STATUS_OPERATOR_REVIEW,
    RELAY_STATUS_PENDING,
    relay_queue_status,
)
import tools.direct_sync_relay_runner as runner
from tools.direct_sync_relay_runner import _scan_source_files, main


def load_label_match_module():
    module_path = Path(__file__).resolve().parents[1] / "Label_Match.py"
    spec = importlib.util.spec_from_file_location("label_match_app_for_runner_tests", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_manifest(
    tmp_path,
    *,
    pc_id="LABEL-PC01",
    source_host_id="label-runner-host-1",
    producer_install_id="install-label-runner-1",
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
    return path


def write_credential(
    tmp_path,
    *,
    producer_id="producer-label-runner",
    key_id="key-label-runner",
    secret="runner-secret",
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


def write_label_csv(
    sync_dir,
    *,
    name="포장실작업이벤트로그_runner_20260622.csv",
    worker_name="worker",
    barcode="BC-1",
):
    sync_dir.mkdir(parents=True, exist_ok=True)
    path = sync_dir / name
    path.write_text(
        "timestamp,worker_name,event,details\n"
        f"2026-06-22T00:00:00,{worker_name},LABEL_MATCHED,\"{{ \"\"product_barcode\"\": \"\"{barcode}\"\" }}\"\n",
        encoding="utf-8",
    )
    return path


def runner_args(
    tmp_path,
    *,
    scan_dir,
    pc_id="LABEL-PC01",
    source_host_id="label-runner-host-1",
    producer_install_id="install-label-runner-1",
    producer_id="producer-label-runner",
    key_id="key-label-runner",
    secret="runner-secret",
):
    return [
        "--db-path",
        str(tmp_path / "relay.sqlite3"),
        "--spool-dir",
        str(tmp_path / "spool"),
        "--producer-manifest-path",
        str(
            write_manifest(
                tmp_path,
                pc_id=pc_id,
                source_host_id=source_host_id,
                producer_install_id=producer_install_id,
            )
        ),
        "--credential-path",
        str(write_credential(tmp_path, producer_id=producer_id, key_id=key_id, secret=secret)),
        "--upload-status-dir",
        str(tmp_path / "status"),
        "--runtime-status-path",
        str(tmp_path / "runtime" / "status.json"),
        "--log-path",
        str(tmp_path / "logs" / "relay.jsonl"),
        "--scan-source-dir",
        str(scan_dir),
        "--source-glob",
        "포장실작업이벤트로그_*.csv",
    ]


def relay_rows(db_path):
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(
            "SELECT * FROM direct_sync_relay_batches ORDER BY created_at, relay_id"
        ).fetchall()
    finally:
        conn.close()


def source_scan_state(db_path, source_file):
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(
            "SELECT * FROM direct_sync_source_scan_state WHERE source_file_path = ?",
            (str(source_file.resolve()),),
        ).fetchone()
    finally:
        conn.close()


def disable_scan_drain(monkeypatch, status="idle"):
    calls = []

    def fake_run_relay_once(config):
        calls.append(config)
        return {"status": status}

    monkeypatch.setattr(runner, "run_relay_once", fake_run_relay_once)
    return calls


def test_scan_state_connection_uses_busy_timeout_and_wal(tmp_path):
    conn = runner._scan_state_connect(tmp_path / "relay.sqlite3")
    try:
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] >= runner.SQLITE_BUSY_TIMEOUT_MS
        assert str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower() == "wal"
    finally:
        conn.close()


def test_runner_scan_source_dir_enqueues_matching_csv_idempotently(tmp_path, capsys, monkeypatch):
    disable_scan_drain(monkeypatch)
    sync_dir = tmp_path / "sync"
    write_label_csv(sync_dir)
    (sync_dir / "ignore.txt").write_text("not a csv", encoding="utf-8")
    args = runner_args(tmp_path, scan_dir=sync_dir)

    assert main(args) == 0
    output = capsys.readouterr().out
    assert "direct_sync_relay_status=idle" in output
    assert "direct_sync_scan_status=enqueued" in output
    assert "direct_sync_scan_enqueued_count=1" in output
    assert relay_queue_status(tmp_path / "relay.sqlite3")["counts"][RELAY_STATUS_PENDING] == 1

    assert main(args) == 0
    assert relay_queue_status(tmp_path / "relay.sqlite3")["counts"][RELAY_STATUS_PENDING] == 1
    output = capsys.readouterr().out
    assert "direct_sync_scan_status=scan_no_new_rows" in output


def test_runner_scan_real_data_manager_output_ack_then_second_scan_has_no_new_rows(tmp_path, capsys, monkeypatch):
    label_match = load_label_match_module()
    scan_dir = tmp_path / "ProgramData" / "KMTech" / "Label_Match" / "data"
    manager = label_match.DataManager(
        str(scan_dir),
        "포장실",
        "operator-bridge",
        "DATA-PC01",
    )
    try:
        manager.log_event(
            "TRAY_COMPLETE",
            {
                "set_id": "bridge-set-1",
                "raw_inputs": ["MASTER", "BC-1", "BC-2"],
                "final_result": "PASS",
            },
        )
        manager.close(timeout=5)
    except Exception:
        try:
            manager.close(timeout=5)
        except Exception:
            pass
        raise
    csv_files = sorted(scan_dir.glob("포장실작업이벤트로그_DATA-PC01_*.csv"))
    assert len(csv_files) == 1
    csv_path = csv_files[0]
    args = runner_args(tmp_path, scan_dir=scan_dir)

    def fake_acked_relay_once(config):
        pending = [
            row for row in relay_rows(config.db_path)
            if row["status"] == RELAY_STATUS_PENDING
        ]
        if not pending:
            return {"status": "idle"}
        relay_id = pending[0]["relay_id"]
        with sqlite3.connect(config.db_path) as conn:
            conn.execute(
                "UPDATE direct_sync_relay_batches SET status = ? WHERE relay_id = ?",
                (RELAY_STATUS_ACKED, relay_id),
            )
            conn.commit()
        return {
            "status": "acked",
            "last_result": {
                "relay_id": relay_id,
                "status": "acked",
                "committed": True,
                "retryable": False,
            },
        }

    monkeypatch.setattr(runner, "run_relay_once", fake_acked_relay_once)

    assert main(args) == 0
    output = capsys.readouterr().out
    assert "direct_sync_scan_status=enqueued" in output
    assert "direct_sync_relay_status=acked" in output
    assert relay_queue_status(tmp_path / "relay.sqlite3")["counts"][RELAY_STATUS_ACKED] == 1
    state = source_scan_state(tmp_path / "relay.sqlite3", csv_path)
    assert state is not None
    assert state["sent_byte_count"] == csv_path.stat().st_size

    assert main(args) == 0
    output = capsys.readouterr().out
    assert "direct_sync_scan_status=scan_no_new_rows" in output
    assert relay_queue_status(tmp_path / "relay.sqlite3")["counts"][RELAY_STATUS_ACKED] == 1


def test_runner_scan_twenty_pcs_same_filename_and_worker_keep_identity_isolated(tmp_path, capsys, monkeypatch):
    disable_scan_drain(monkeypatch)
    same_filename = "포장실작업이벤트로그_20260625.csv"
    source_host_ids = set()
    install_ids = set()
    producer_ids = set()
    idempotency_keys = set()

    for index in range(1, 21):
        pc_root = tmp_path / f"pc_{index:03d}"
        sync_dir = pc_root / "sync"
        write_label_csv(
            sync_dir,
            name=same_filename,
            worker_name="same-worker",
            barcode=f"BC-LABEL-{index:03d}",
        )
        args = runner_args(
            pc_root,
            scan_dir=sync_dir,
            pc_id=f"LABEL-PC-{index:03d}",
            source_host_id=f"label-runner-host-{index:03d}",
            producer_install_id=f"install-label-runner-{index:03d}",
            producer_id=f"producer-label-runner-{index:03d}",
            key_id=f"key-label-runner-{index:03d}",
            secret=f"runner-secret-{index:03d}",
        )

        assert main(args) == 0
        output = capsys.readouterr().out
        assert "direct_sync_scan_status=enqueued" in output
        rows = relay_rows(pc_root / "relay.sqlite3")
        assert len(rows) == 1
        metadata = json.loads(rows[0]["metadata_json"])
        assert metadata["relative_path"].startswith(f"legacy_csv_deltas/{same_filename}/")
        assert metadata["source_host_id"] == f"label-runner-host-{index:03d}"
        assert metadata["producer_install_id"] == f"install-label-runner-{index:03d}"
        assert rows[0]["producer_id"] == f"producer-label-runner-{index:03d}"
        assert rows[0]["key_id"] == f"key-label-runner-{index:03d}"
        assert rows[0]["endpoint_url"] == "https://worker.example.invalid/api/producer-ingest/v1/source-file"
        source_host_ids.add(metadata["source_host_id"])
        install_ids.add(metadata["producer_install_id"])
        producer_ids.add(rows[0]["producer_id"])
        idempotency_keys.add(metadata["idempotency_key"])

    assert len(source_host_ids) == 20
    assert len(install_ids) == 20
    assert len(producer_ids) == 20
    assert len(idempotency_keys) == 20


def test_runner_scan_source_invalid_utf8_csv_writes_enqueue_error(tmp_path, capsys, monkeypatch):
    drain_calls = disable_scan_drain(monkeypatch)
    sync_dir = tmp_path / "sync"
    sync_dir.mkdir(parents=True)
    bad_csv = sync_dir / "포장실작업이벤트로그_bad_utf8.csv"
    bad_csv.write_bytes(
        b"timestamp,worker_name,event,details\n"
        b"2026-06-22T00:00:00,worker,LABEL_MATCHED,\xff\n"
    )
    args = runner_args(tmp_path, scan_dir=sync_dir)

    assert main(args) == 1
    output = capsys.readouterr().out
    assert "direct_sync_relay_status=enqueue_error" in output
    assert "direct_sync_scan_status=enqueue_error" in output
    assert "direct_sync_scan_attempted_count=1" in output
    assert f"direct_sync_scan_failed_source_file={bad_csv}" in output
    status = json.loads((tmp_path / "runtime" / "status.json").read_text(encoding="utf-8"))
    assert status["status"] == "enqueue_error"
    assert status["error_code"] == "direct_sync_source_file_error"
    assert "UnicodeDecodeError" in status["error_message"]
    assert relay_queue_status(tmp_path / "relay.sqlite3")["counts"] == {}
    assert drain_calls == []


def test_runner_scan_source_watermark_does_not_advance_before_ack(tmp_path, capsys, monkeypatch):
    disable_scan_drain(monkeypatch)
    sync_dir = tmp_path / "sync"
    csv_path = write_label_csv(sync_dir)
    args = runner_args(tmp_path, scan_dir=sync_dir)

    assert main(args) == 0

    assert source_scan_state(tmp_path / "relay.sqlite3", csv_path) is None
    assert main(args) == 0
    output = capsys.readouterr().out
    assert "direct_sync_scan_status=scan_no_new_rows" in output
    assert relay_queue_status(tmp_path / "relay.sqlite3")["counts"][RELAY_STATUS_PENDING] == 1


def test_runner_scan_source_records_watermark_after_durable_ack(tmp_path, capsys, monkeypatch):
    sync_dir = tmp_path / "sync"
    csv_path = write_label_csv(sync_dir)
    args = runner_args(tmp_path, scan_dir=sync_dir)

    def fake_acked_relay_once(config):
        relay_id = relay_rows(config.db_path)[0]["relay_id"]
        with sqlite3.connect(config.db_path) as conn:
            conn.execute(
                "UPDATE direct_sync_relay_batches SET status = ? WHERE relay_id = ?",
                (RELAY_STATUS_ACKED, relay_id),
            )
            conn.commit()
        return {
            "status": "acked",
            "last_result": {
                "relay_id": relay_id,
                "status": "acked",
                "committed": True,
                "retryable": False,
            },
        }

    monkeypatch.setattr(runner, "run_relay_once", fake_acked_relay_once)

    assert main(args) == 0
    capsys.readouterr()

    state = source_scan_state(tmp_path / "relay.sqlite3", csv_path)
    assert state is not None
    assert state["sent_byte_count"] == csv_path.stat().st_size
    assert state["sent_prefix_sha256"] == runner._file_prefix_sha256(csv_path, csv_path.stat().st_size)
    status = json.loads((tmp_path / "runtime" / "status.json").read_text(encoding="utf-8"))
    assert status["status"] == "acked"
    assert status["scan_status"] == "enqueued"
    assert status["scan_enqueued_count"] == 1


def test_runner_scan_source_records_watermark_after_committed_operator_review(tmp_path, capsys, monkeypatch):
    sync_dir = tmp_path / "sync"
    csv_path = write_label_csv(sync_dir)
    args = runner_args(tmp_path, scan_dir=sync_dir)

    def fake_operator_review_relay_once(config):
        relay_id = relay_rows(config.db_path)[0]["relay_id"]
        receipt = {
            "client_batch_id": relay_id,
            "status": "accepted",
            "committed": True,
            "retryable": False,
            "totals": {"inserted": 1, "quarantined": 1, "errors": 0, "replayed": 0},
        }
        with sqlite3.connect(config.db_path) as conn:
            conn.execute(
                """
                UPDATE direct_sync_relay_batches
                SET status = ?, receipt_json = ?
                WHERE relay_id = ?
                """,
                (RELAY_STATUS_OPERATOR_REVIEW, json.dumps(receipt, sort_keys=True), relay_id),
            )
            conn.commit()
        return {
            "status": "operator_review",
            "last_result": {
                "relay_id": relay_id,
                "status": "operator_review",
                "committed": True,
                "retryable": False,
            },
        }

    monkeypatch.setattr(runner, "run_relay_once", fake_operator_review_relay_once)

    assert main(args) == 0
    capsys.readouterr()

    state = source_scan_state(tmp_path / "relay.sqlite3", csv_path)
    assert state is not None
    assert state["sent_byte_count"] == csv_path.stat().st_size
    assert state["sent_prefix_sha256"] == runner._file_prefix_sha256(csv_path, csv_path.stat().st_size)

    assert main(args) == 0
    output = capsys.readouterr().out
    assert "direct_sync_scan_status=scan_no_new_rows" in output
    assert relay_queue_status(tmp_path / "relay.sqlite3")["counts"][RELAY_STATUS_OPERATOR_REVIEW] == 1
    status = json.loads((tmp_path / "runtime" / "status.json").read_text(encoding="utf-8"))
    assert status["status"] == "operator_review"
    assert status["scan_status"] == "scan_no_new_rows"
    assert status["scan_no_new_count"] == 1


def test_runner_scan_source_recovers_checkpoint_from_committed_operator_review_row(
    tmp_path,
    capsys,
    monkeypatch,
):
    disable_scan_drain(monkeypatch)
    sync_dir = tmp_path / "sync"
    csv_path = write_label_csv(sync_dir)
    args = runner_args(tmp_path, scan_dir=sync_dir)

    assert main(args) == 0
    capsys.readouterr()
    first_row = relay_rows(tmp_path / "relay.sqlite3")[0]
    receipt = {
        "client_batch_id": first_row["relay_id"],
        "status": "accepted",
        "committed": True,
        "retryable": False,
        "totals": {"inserted": 1, "quarantined": 1, "errors": 0, "replayed": 0},
    }
    with sqlite3.connect(tmp_path / "relay.sqlite3") as conn:
        conn.execute(
            """
            UPDATE direct_sync_relay_batches
            SET status = ?, receipt_json = ?
            WHERE relay_id = ?
            """,
            (
                RELAY_STATUS_OPERATOR_REVIEW,
                json.dumps(receipt, sort_keys=True),
                first_row["relay_id"],
            ),
        )
        conn.commit()

    with csv_path.open("a", encoding="utf-8") as file:
        file.write("2026-06-22T00:01:00,worker,LABEL_MATCHED,\"{ \"\"product_barcode\"\": \"\"BC-2\"\" }\"\n")

    assert main(args) == 0
    output = capsys.readouterr().out
    assert "direct_sync_scan_status=enqueued" in output
    assert relay_queue_status(tmp_path / "relay.sqlite3")["counts"][RELAY_STATUS_OPERATOR_REVIEW] == 1
    assert relay_queue_status(tmp_path / "relay.sqlite3")["counts"][RELAY_STATUS_PENDING] == 1
    rows = relay_rows(tmp_path / "relay.sqlite3")
    second_payload = Path(rows[1]["spooled_file_path"]).read_text(encoding="utf-8")
    assert "BC-2" in second_payload
    assert "BC-1" not in second_payload
    assert "/bytes-0-" not in rows[1]["relative_path"].replace("\\", "/")


def test_runner_scan_source_dir_runs_relay_drain_after_scan(tmp_path, capsys, monkeypatch):
    calls = disable_scan_drain(monkeypatch, status="acked")
    sync_dir = tmp_path / "sync"
    write_label_csv(sync_dir)
    args = runner_args(tmp_path, scan_dir=sync_dir)

    assert main(args) == 0
    output = capsys.readouterr().out

    assert "direct_sync_relay_status=acked" in output
    assert "direct_sync_scan_status=enqueued" in output
    assert len(calls) == 1


def test_runner_scan_source_content_change_enqueues_new_delta(tmp_path, capsys, monkeypatch):
    disable_scan_drain(monkeypatch)
    sync_dir = tmp_path / "sync"
    csv_path = write_label_csv(sync_dir)
    args = runner_args(tmp_path, scan_dir=sync_dir)

    assert main(args) == 0
    with csv_path.open("a", encoding="utf-8") as file:
        file.write("2026-06-22T00:01:00,worker,LABEL_MATCHED,\"{ \"\"product_barcode\"\": \"\"BC-2\"\" }\"\n")

    assert main(args) == 0
    output = capsys.readouterr().out
    assert "direct_sync_scan_status=enqueued" in output
    assert "direct_sync_scan_enqueued_count=1" in output
    assert "direct_sync_scan_attempted_count=1" in output
    assert relay_queue_status(tmp_path / "relay.sqlite3")["counts"][RELAY_STATUS_PENDING] == 2
    rows = relay_rows(tmp_path / "relay.sqlite3")
    assert rows[0]["relative_path"].endswith(".csv")
    assert "/bytes-0-" in rows[0]["relative_path"].replace("\\", "/")
    assert "/bytes-0-" not in rows[1]["relative_path"].replace("\\", "/")
    second_payload = Path(rows[1]["spooled_file_path"]).read_text(encoding="utf-8")
    assert "BC-2" in second_payload
    assert "BC-1" not in second_payload


def test_runner_scan_source_recovers_checkpoint_from_queued_delta_after_crash(tmp_path, capsys, monkeypatch):
    disable_scan_drain(monkeypatch)
    sync_dir = tmp_path / "sync"
    csv_path = write_label_csv(sync_dir)
    args = runner_args(tmp_path, scan_dir=sync_dir)

    assert main(args) == 0
    conn = sqlite3.connect(str(tmp_path / "relay.sqlite3"))
    try:
        conn.execute("DELETE FROM direct_sync_source_scan_state")
        conn.commit()
    finally:
        conn.close()
    with csv_path.open("a", encoding="utf-8") as file:
        file.write("2026-06-22T00:01:00,worker,LABEL_MATCHED,\"{ \"\"product_barcode\"\": \"\"BC-2\"\" }\"\n")

    assert main(args) == 0
    output = capsys.readouterr().out
    assert "direct_sync_scan_status=enqueued" in output
    assert relay_queue_status(tmp_path / "relay.sqlite3")["counts"][RELAY_STATUS_PENDING] == 2
    rows = relay_rows(tmp_path / "relay.sqlite3")
    second_payload = Path(rows[1]["spooled_file_path"]).read_text(encoding="utf-8")
    assert "BC-2" in second_payload
    assert "BC-1" not in second_payload
    assert "/bytes-0-" not in rows[1]["relative_path"].replace("\\", "/")


def test_runner_scan_source_queued_checkpoint_recovery_resets_on_replacement(tmp_path, capsys, monkeypatch):
    disable_scan_drain(monkeypatch)
    sync_dir = tmp_path / "sync"
    csv_path = write_label_csv(sync_dir)
    args = runner_args(tmp_path, scan_dir=sync_dir)

    assert main(args) == 0
    with csv_path.open("a", encoding="utf-8") as file:
        file.write("2026-06-22T00:01:00,worker,LABEL_MATCHED,\"{ \"\"product_barcode\"\": \"\"BC-2\"\" }\"\n")
    assert main(args) == 0
    conn = sqlite3.connect(str(tmp_path / "relay.sqlite3"))
    try:
        conn.execute("DELETE FROM direct_sync_source_scan_state")
        conn.commit()
    finally:
        conn.close()
    csv_path.write_text(
        csv_path.read_text(encoding="utf-8").replace("BC-1", "BC-X")
        + "2026-06-22T00:02:00,worker,LABEL_MATCHED,\"{ \"\"product_barcode\"\": \"\"BC-3\"\" }\"\n",
        encoding="utf-8",
    )

    assert main(args) == 0
    output = capsys.readouterr().out
    assert "direct_sync_scan_status=enqueued" in output
    rows = relay_rows(tmp_path / "relay.sqlite3")
    third_payload = Path(rows[2]["spooled_file_path"]).read_text(encoding="utf-8")
    assert "BC-X" in third_payload
    assert "BC-2" in third_payload
    assert "BC-3" in third_payload
    assert "/bytes-0-" in rows[2]["relative_path"].replace("\\", "/")


def test_runner_scan_source_append_after_quiet_window_does_not_stall(tmp_path, capsys, monkeypatch):
    disable_scan_drain(monkeypatch)
    sync_dir = tmp_path / "sync"
    csv_path = write_label_csv(sync_dir)
    old_time = time.time() - 120
    os.utime(csv_path, (old_time, old_time))
    args = runner_args(tmp_path, scan_dir=sync_dir) + ["--min-source-file-age-seconds", "60"]

    assert main(args) == 0

    with csv_path.open("a", encoding="utf-8") as file:
        file.write("2026-06-22T00:02:00,worker,LABEL_MATCHED,\"{ \"\"product_barcode\"\": \"\"BC-3\"\" }\"\n")
    os.utime(csv_path, (old_time + 1, old_time + 1))

    assert main(args) == 0
    output = capsys.readouterr().out
    assert "direct_sync_scan_status=enqueued" in output
    assert "direct_sync_scan_enqueued_count=1" in output
    assert "direct_sync_scan_failed_source_file=" not in output
    assert relay_queue_status(tmp_path / "relay.sqlite3")["counts"][RELAY_STATUS_PENDING] == 2
    rows = relay_rows(tmp_path / "relay.sqlite3")
    second_payload = Path(rows[1]["spooled_file_path"]).read_text(encoding="utf-8")
    assert "BC-3" in second_payload
    assert "BC-1" not in second_payload


def test_runner_scan_source_defers_trailing_partial_csv_row_until_newline(tmp_path, capsys, monkeypatch):
    disable_scan_drain(monkeypatch)
    sync_dir = tmp_path / "sync"
    sync_dir.mkdir(parents=True, exist_ok=True)
    csv_path = sync_dir / "포장실작업이벤트로그_runner_20260622.csv"
    csv_path.write_text(
        "timestamp,worker_name,event,details\n"
        "2026-06-22T00:01:00,worker,LABEL_MATCHED,\"{ \"\"product_barcode\"\": \"\"BC-PART",
        encoding="utf-8",
    )
    args = runner_args(tmp_path, scan_dir=sync_dir)

    assert main(args) == 0
    output = capsys.readouterr().out
    assert "direct_sync_scan_status=scan_no_new_rows" in output
    assert "direct_sync_scan_enqueued_count=0" in output
    assert relay_queue_status(tmp_path / "relay.sqlite3")["counts"] == {}

    with csv_path.open("a", encoding="utf-8") as file:
        file.write("IAL\"\" }\"\n")

    assert main(args) == 0
    output = capsys.readouterr().out
    assert "direct_sync_scan_status=enqueued" in output
    assert "direct_sync_scan_enqueued_count=1" in output
    rows = relay_rows(tmp_path / "relay.sqlite3")
    assert len(rows) == 1
    payload = Path(rows[0]["spooled_file_path"]).read_text(encoding="utf-8")
    assert "BC-PARTIAL" in payload
    assert payload.endswith("\n")


def test_runner_scan_source_limit_skips_no_new_files_to_reach_new_delta(tmp_path, capsys, monkeypatch):
    disable_scan_drain(monkeypatch)
    sync_dir = tmp_path / "sync"
    first = write_label_csv(sync_dir, name="포장실작업이벤트로그_001_20260622.csv")
    second = write_label_csv(sync_dir, name="포장실작업이벤트로그_002_20260622.csv")
    now = time.time() - 120
    os.utime(first, (now, now))
    os.utime(second, (now + 1, now + 1))
    args = runner_args(tmp_path, scan_dir=sync_dir) + ["--max-enqueue-files", "1"]

    assert main(args) == 0
    assert relay_queue_status(tmp_path / "relay.sqlite3")["counts"][RELAY_STATUS_PENDING] == 1

    assert main(args) == 0
    output = capsys.readouterr().out
    assert "direct_sync_scan_status=enqueued" in output
    rows = relay_rows(tmp_path / "relay.sqlite3")
    assert len(rows) == 2
    assert any("002_20260622" in row["relative_path"] for row in rows)


def test_runner_scan_source_replace_or_truncate_resets_delta_state(tmp_path, capsys, monkeypatch):
    disable_scan_drain(monkeypatch)
    sync_dir = tmp_path / "sync"
    csv_path = write_label_csv(sync_dir)
    args = runner_args(tmp_path, scan_dir=sync_dir)

    assert main(args) == 0
    csv_path.write_text(
        "timestamp,worker_name,event,details\n"
        "2026-06-22T00:03:00,w,L,\"{ \"\"product_barcode\"\": \"\"NEW\"\" }\"\n",
        encoding="utf-8",
    )

    assert main(args) == 0
    output = capsys.readouterr().out
    assert "direct_sync_scan_status=enqueued" in output
    assert relay_queue_status(tmp_path / "relay.sqlite3")["counts"][RELAY_STATUS_PENDING] == 2
    rows = relay_rows(tmp_path / "relay.sqlite3")
    second_payload = Path(rows[1]["spooled_file_path"]).read_text(encoding="utf-8")
    assert "NEW" in second_payload
    assert "BC-1" not in second_payload
    assert "/bytes-0-" in rows[1]["relative_path"].replace("\\", "/")


def test_runner_scan_source_larger_replacement_does_not_skip_new_first_row(tmp_path, capsys, monkeypatch):
    disable_scan_drain(monkeypatch)
    sync_dir = tmp_path / "sync"
    csv_path = write_label_csv(sync_dir)
    args = runner_args(tmp_path, scan_dir=sync_dir)

    assert main(args) == 0
    csv_path.write_text(
        "timestamp,worker_name,event,details\n"
        "2026-06-22T00:03:00,worker,LABEL_MATCHED,\"{ \"\"product_barcode\"\": \"\"NEW-FIRST\"\" }\"\n"
        "2026-06-22T00:04:00,worker,LABEL_MATCHED,\"{ \"\"product_barcode\"\": \"\"NEW-SECOND\"\" }\"\n",
        encoding="utf-8",
    )

    assert main(args) == 0
    output = capsys.readouterr().out
    assert "direct_sync_scan_status=enqueued" in output
    rows = relay_rows(tmp_path / "relay.sqlite3")
    second_payload = Path(rows[1]["spooled_file_path"]).read_text(encoding="utf-8")
    assert "NEW-FIRST" in second_payload
    assert "NEW-SECOND" in second_payload
    assert "BC-1" not in second_payload
    assert "/bytes-0-" in rows[1]["relative_path"].replace("\\", "/")


def test_runner_scan_source_dir_filters_broad_csv_glob_to_label_logs(tmp_path, capsys, monkeypatch):
    disable_scan_drain(monkeypatch)
    sync_dir = tmp_path / "sync"
    write_label_csv(sync_dir)
    write_label_csv(sync_dir, name="unrelated.csv")
    args = runner_args(tmp_path, scan_dir=sync_dir) + ["--source-glob", "*.csv"]

    assert main(args) == 0
    output = capsys.readouterr().out

    assert "direct_sync_scan_enqueued_count=1" in output
    assert relay_queue_status(tmp_path / "relay.sqlite3")["counts"][RELAY_STATUS_PENDING] == 1


def test_runner_scan_source_dir_rejects_recursive_or_path_globs(tmp_path):
    sync_dir = tmp_path / "sync"
    write_label_csv(sync_dir)
    args = runner_args(tmp_path, scan_dir=sync_dir) + ["--source-glob", "**/*.csv"]

    try:
        main(args)
    except SystemExit as exc:
        assert "source glob must be a direct-child file pattern" in str(exc)
        return
    raise AssertionError("expected SystemExit for recursive source glob")


def test_runner_scan_source_dir_handles_no_matching_files(tmp_path, capsys):
    sync_dir = tmp_path / "sync"
    sync_dir.mkdir()

    assert main(runner_args(tmp_path, scan_dir=sync_dir)) == 0
    output = capsys.readouterr().out

    assert "direct_sync_scan_status=scan_no_files" in output
    assert "direct_sync_scan_enqueued_count=0" in output
    assert relay_queue_status(tmp_path / "relay.sqlite3")["counts"] == {}


def test_runner_scan_source_dir_defers_recent_matching_files(tmp_path, capsys):
    sync_dir = tmp_path / "sync"
    csv_path = write_label_csv(sync_dir)
    now = time.time()
    os.utime(csv_path, (now, now))
    args = runner_args(tmp_path, scan_dir=sync_dir) + ["--min-source-file-age-seconds", "60"]

    assert main(args) == 0
    output = capsys.readouterr().out

    assert "direct_sync_scan_status=scan_deferred_sources" in output
    assert "direct_sync_scan_enqueued_count=0" in output
    assert "direct_sync_scan_attempted_count=0" in output
    assert "direct_sync_scan_deferred_count=1" in output
    assert relay_queue_status(tmp_path / "relay.sqlite3")["counts"] == {}


def test_runner_scan_source_dir_enqueues_old_files_with_quiet_time(tmp_path, capsys, monkeypatch):
    disable_scan_drain(monkeypatch)
    sync_dir = tmp_path / "sync"
    csv_path = write_label_csv(sync_dir)
    old_time = time.time() - 120
    os.utime(csv_path, (old_time, old_time))
    args = runner_args(tmp_path, scan_dir=sync_dir) + ["--min-source-file-age-seconds", "60"]

    assert main(args) == 0
    output = capsys.readouterr().out

    assert "direct_sync_scan_status=enqueued" in output
    assert "direct_sync_scan_enqueued_count=1" in output
    assert "direct_sync_scan_deferred_count=0" in output
    assert relay_queue_status(tmp_path / "relay.sqlite3")["counts"][RELAY_STATUS_PENDING] == 1


def test_scan_source_files_counts_duplicate_recent_file_once(tmp_path):
    sync_dir = tmp_path / "sync"
    csv_path = write_label_csv(sync_dir)
    now = time.time()
    os.utime(csv_path, (now, now))

    selected, deferred_count = _scan_source_files(
        str(sync_dir),
        ["*.csv", "포장실작업이벤트로그_*.csv"],
        max_files=100,
        min_file_age_seconds=60,
        now=now,
    )

    assert selected == []
    assert deferred_count == 1


def test_scan_source_files_skips_symlinked_matching_csv(tmp_path):
    sync_dir = tmp_path / "sync"
    sync_dir.mkdir()
    target = tmp_path / "outside.csv"
    target.write_text(
        "timestamp,worker_name,event,details\n"
        "2026-06-22T00:00:00,worker,SCAN_OK,\"{}\"\n",
        encoding="utf-8",
    )
    link = sync_dir / "포장실작업이벤트로그_link_20260622.csv"
    try:
        link.symlink_to(target)
    except (NotImplementedError, OSError) as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")

    selected, deferred_count = _scan_source_files(str(sync_dir), ["*.csv"], max_files=100)

    assert selected == []
    assert deferred_count == 0


def test_runner_rechecks_quiet_window_before_enqueue(tmp_path, capsys, monkeypatch):
    disable_scan_drain(monkeypatch)
    sync_dir = tmp_path / "sync"
    csv_path = write_label_csv(sync_dir)
    old_time = time.time() - 120
    os.utime(csv_path, (old_time, old_time))

    def scan_then_make_recent(*args, **kwargs):
        now = time.time()
        os.utime(csv_path, (now, now))
        return [csv_path], 0

    monkeypatch.setattr(runner, "_scan_source_files", scan_then_make_recent)
    args = runner_args(tmp_path, scan_dir=sync_dir) + ["--min-source-file-age-seconds", "60"]

    assert main(args) == 0
    output = capsys.readouterr().out

    assert "direct_sync_scan_status=scan_deferred_sources" in output
    assert "direct_sync_scan_enqueued_count=0" in output
    assert "direct_sync_scan_deferred_count=1" in output
    assert relay_queue_status(tmp_path / "relay.sqlite3")["counts"] == {}


def test_runner_runtime_error_returns_failure_exit_code(tmp_path, capsys):
    sync_dir = tmp_path / "sync"
    sync_dir.mkdir()
    args = runner_args(tmp_path, scan_dir=sync_dir)
    scan_index = args.index("--scan-source-dir")
    del args[scan_index : scan_index + 2]
    glob_index = args.index("--source-glob")
    del args[glob_index : glob_index + 2]
    (tmp_path / "relay.sqlite3").write_text("not a sqlite database", encoding="utf-8")

    assert main(args) == 1
    output = capsys.readouterr().out
    assert "direct_sync_relay_status=runtime_error" in output


def test_runner_scan_source_corrupt_db_writes_runtime_error_status(tmp_path, capsys):
    sync_dir = tmp_path / "sync"
    write_label_csv(sync_dir)
    args = runner_args(tmp_path, scan_dir=sync_dir)
    (tmp_path / "relay.sqlite3").write_text("not a sqlite database", encoding="utf-8")

    assert main(args) == 1
    output = capsys.readouterr().out
    assert "direct_sync_relay_status=runtime_error" in output

    status = json.loads((tmp_path / "runtime" / "status.json").read_text(encoding="utf-8"))
    assert status["status"] == "runtime_error"
    assert status["scan_error"] is True
    assert status["error_code"] == "DatabaseError"

    log_text = (tmp_path / "logs" / "relay.jsonl").read_text(encoding="utf-8")
    assert "scan_runtime_error" in log_text


def test_runner_direct_enqueue_corrupt_db_writes_runtime_error_status(tmp_path, capsys):
    sync_dir = tmp_path / "sync"
    source_file = write_label_csv(sync_dir)
    args = runner_args(tmp_path, scan_dir=sync_dir)
    scan_index = args.index("--scan-source-dir")
    del args[scan_index : scan_index + 2]
    glob_index = args.index("--source-glob")
    del args[glob_index : glob_index + 2]
    args.extend(["--enqueue-source-file", str(source_file), "--relative-path", source_file.name])
    (tmp_path / "relay.sqlite3").write_text("not a sqlite database", encoding="utf-8")

    assert main(args) == 1
    output = capsys.readouterr().out
    assert "direct_sync_relay_status=runtime_error" in output

    status = json.loads((tmp_path / "runtime" / "status.json").read_text(encoding="utf-8"))
    assert status["status"] == "runtime_error"
    assert status["error_code"] == "relay_queue_db_error"

    log_text = (tmp_path / "logs" / "relay.jsonl").read_text(encoding="utf-8")
    assert "enqueue_runtime_error" in log_text


def test_runner_scan_source_runtime_error_is_not_masked_by_drain(tmp_path, capsys, monkeypatch):
    sync_dir = tmp_path / "sync"
    write_label_csv(sync_dir)
    args = runner_args(tmp_path, scan_dir=sync_dir)
    drain_calls = []

    def fake_enqueue(*args, **kwargs):
        return {"status": "runtime_error", "error_code": "relay_queue_db_error"}

    def fake_run_relay_once(config):
        drain_calls.append(config)
        return {"status": "idle"}

    monkeypatch.setattr(runner, "enqueue_completed_source_file", fake_enqueue)
    monkeypatch.setattr(runner, "run_relay_once", fake_run_relay_once)

    assert main(args) == 1
    output = capsys.readouterr().out
    assert "direct_sync_relay_status=runtime_error" in output
    assert "direct_sync_scan_status=runtime_error" in output
    assert drain_calls == []


def test_runner_scan_source_dir_drains_after_backpressure(tmp_path, capsys, monkeypatch):
    calls = disable_scan_drain(monkeypatch)
    sync_dir = tmp_path / "sync"
    write_label_csv(sync_dir)
    write_label_csv(sync_dir, name="포장실작업이벤트로그_runner2_20260622.csv")
    args = runner_args(tmp_path, scan_dir=sync_dir) + ["--max-active-queue-count", "1"]

    assert main(args) == 0
    output = capsys.readouterr().out
    assert "direct_sync_relay_status=idle" in output
    assert "direct_sync_scan_status=blocked_queue_backpressure" in output
    assert "direct_sync_scan_enqueued_count=1" in output
    assert len(calls) == 1
    assert relay_queue_status(tmp_path / "relay.sqlite3")["counts"][RELAY_STATUS_PENDING] == 1


def test_runner_honors_operator_pause_before_scan_enqueue(tmp_path, capsys):
    sync_dir = tmp_path / "sync"
    write_label_csv(sync_dir)
    pause_path = tmp_path / "control" / "pause.json"
    pause_path.parent.mkdir(parents=True)
    pause_path.write_text(
        json.dumps(
            {
                "schema_version": "direct-sync-relay-operator-pause-v1",
                "status": "paused",
                "operator_id": "operator-a",
                "reason_redacted": "sha256:test",
                "reason_sha256": "0" * 64,
                "reason_length": 11,
                "created_at": "2026-06-22T00:00:00Z",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    args = runner_args(tmp_path, scan_dir=sync_dir) + ["--operator-pause-path", str(pause_path)]

    assert main(args) == 0
    output = capsys.readouterr().out
    assert "direct_sync_relay_status=paused_by_operator" in output
    assert "direct_sync_scan_enqueued_count=0" in output
    assert relay_queue_status(tmp_path / "relay.sqlite3")["counts"] == {}

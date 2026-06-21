import json

from direct_sync_push import RELAY_STATUS_PENDING, relay_queue_status
from tools.direct_sync_relay_runner import main


def write_manifest(tmp_path):
    manifest = {
        "schema_version": "producer-onboarding-manifest-v1",
        "pc_identity": {
            "pc_id": "LABEL-PC01",
            "source_host_id": "label-runner-host-1",
            "producer_install_id": "install-label-runner-1",
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


def write_credential(tmp_path):
    path = tmp_path / "credential.json"
    path.write_text(
        json.dumps(
            {
                "producer_id": "producer-label-runner",
                "key_id": "key-label-runner",
                "secret": "runner-secret",
                "endpoint_url": "https://worker.example.invalid/api/producer-ingest/v1/source-file",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path


def write_label_csv(sync_dir):
    sync_dir.mkdir(parents=True, exist_ok=True)
    path = sync_dir / "포장실작업이벤트로그_runner_20260622.csv"
    path.write_text(
        "timestamp,worker_name,event,details\n"
        "2026-06-22T00:00:00,worker,LABEL_MATCHED,\"{ \"\"product_barcode\"\": \"\"BC-1\"\" }\"\n",
        encoding="utf-8",
    )
    return path


def runner_args(tmp_path, *, scan_dir):
    return [
        "--db-path",
        str(tmp_path / "relay.sqlite3"),
        "--spool-dir",
        str(tmp_path / "spool"),
        "--producer-manifest-path",
        str(write_manifest(tmp_path)),
        "--credential-path",
        str(write_credential(tmp_path)),
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


def test_runner_scan_source_dir_enqueues_matching_csv_idempotently(tmp_path, capsys):
    sync_dir = tmp_path / "sync"
    write_label_csv(sync_dir)
    (sync_dir / "ignore.txt").write_text("not a csv", encoding="utf-8")
    args = runner_args(tmp_path, scan_dir=sync_dir)

    assert main(args) == 0
    output = capsys.readouterr().out
    assert "direct_sync_relay_status=enqueued" in output
    assert "direct_sync_scan_enqueued_count=1" in output
    assert relay_queue_status(tmp_path / "relay.sqlite3")["counts"][RELAY_STATUS_PENDING] == 1

    assert main(args) == 0
    assert relay_queue_status(tmp_path / "relay.sqlite3")["counts"][RELAY_STATUS_PENDING] == 1


def test_runner_scan_source_dir_handles_no_matching_files(tmp_path, capsys):
    sync_dir = tmp_path / "sync"
    sync_dir.mkdir()

    assert main(runner_args(tmp_path, scan_dir=sync_dir)) == 0
    output = capsys.readouterr().out

    assert "direct_sync_relay_status=scan_no_files" in output
    assert "direct_sync_scan_enqueued_count=0" in output
    assert relay_queue_status(tmp_path / "relay.sqlite3")["counts"] == {}

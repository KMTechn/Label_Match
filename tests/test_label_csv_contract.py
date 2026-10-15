import csv
import json
import sqlite3
from pathlib import Path

import pytest

import Label_Match as label_module
from event_stream_policy import (
    CONTRACT_CANDIDATE_EVENT_TYPES,
    LOCAL_ONLY_EVENT_TYPES,
)
from package_logistics import PackageOutbox
from tools.direct_sync_relay_runner import _scan_source_files


@pytest.mark.parametrize("event_type", ["APP_CLOSE_CANCELLED", "멈춤 요청 미룸"])
@pytest.mark.parametrize("legacy_route", [False, True], ids=["local-audit", "historical-quarantine"])
def test_close_audits_preserve_local_evidence_and_do_not_block_relay(
    tmp_path, monkeypatch, event_type, legacy_route,
):
    import direct_sync_push as push
    import label_data_manager
    from producer_runtime_client import RuntimePreparation
    from tests.test_direct_sync_push import FakeResponse, FakeSession, make_credentials, make_manifest

    if legacy_route:
        monkeypatch.setattr(label_data_manager, "LOCAL_ONLY_EVENT_TYPES", frozenset())
    monkeypatch.setattr(push, "prepare_runtime_metadata", lambda **kwargs: RuntimePreparation(metadata=dict(kwargs["metadata"])))
    monkeypatch.setattr(push, "client_runtime_lease_mode", lambda _credentials: "observe")
    scan_dir = tmp_path / "Label_Match/data"
    manager = label_module.DataManager(str(scan_dir), "포장실", "audit-operator", "AUDIT-PC")
    try:
        manager.log_event(event_type, {"message": "close postponed"})
        manager.log_event("SCAN_OK", {"set_id": "business-control"})
    finally:
        manager.close(timeout=5)
    selected, deferred = _scan_source_files(str(scan_dir), ["*.csv"], max_files=100)
    assert deferred == 0
    assert len(selected) == 1
    events = _events(selected[0])
    root = Path(__file__).resolve().parents[1]
    catalogs = [
        json.loads((root / "tests/fixtures/server_stream_catalog_8544774e.json").read_bytes()),
        json.loads((root / "kmtech_factory_contracts/bundle/v1/catalogs/canonical-stream-catalog.json").read_bytes()),
    ]
    quarantined_counts = []
    for catalog in catalogs:
        accepted = next(row["raw_event_names"] for row in catalog["streams"] if row["app_id"] == "label_match")
        quarantined_counts.append(sum(event not in accepted for event in events))
    assert quarantined_counts[0] == quarantined_counts[1]
    quarantined = quarantined_counts[0]
    manifest, manifest_path = make_manifest(tmp_path)
    credentials = make_credentials()
    db_path = tmp_path / "relay.sqlite3"
    row = push.enqueue_source_file_for_relay(
        db_path=db_path, spool_dir=tmp_path / "spool", source_file_path=selected[0],
        producer_manifest_path=manifest_path, credentials=credentials,
    )
    # Model the server's catalog dispatch and quarantine-derived disposition;
    # the CSV writer, source selector, receipt validation and relay DB are real.
    receipt = {
        "request_id": "close-audit-regression", "client_batch_id": row.relay_id,
        "server_source_file_id": f"{manifest['pc_identity']['source_host_id']}/label_match/label_match_events/{row.relative_path}",
        "committed": True, "status": "accepted", "retryable": False, "next_retry_after": None,
        "projection_disposition": "UNKNOWN" if quarantined else "COMPLETE",
        "totals": {"inserted": len(events) - quarantined, "replayed": 0, "quarantined": quarantined, "errors": 0},
    }
    result = push.drain_one_relay_batch(
        db_path=db_path, credentials=credentials,
        session=FakeSession(FakeResponse(200, receipt)), status_dir=tmp_path / "status",
    )
    assert result is not None
    counts = push.relay_queue_status(db_path)["counts"]
    if legacy_route:
        assert quarantined == 1
        assert result.success is False
        assert result.error_code == "producer_projection_incomplete"
        assert counts[push.RELAY_STATUS_OPERATOR_REVIEW] == 1
        assert Path(row.spooled_file_path).is_file()
    else:
        assert result.success is True
        assert quarantined == 0
        assert counts[push.RELAY_STATUS_ACKED] == 1
        assert events == ["SCAN_OK"]
        local_files = list((tmp_path / "Label_Match/local_events").glob("*.csv"))
        assert len(local_files) == 1
        assert _events(local_files[0]) == [event_type]
        # Reopening the store must preserve the already written audit bytes.
        before = local_files[0].read_bytes()
        reopened = label_module.DataManager(str(scan_dir), "포장실", "audit-operator", "AUDIT-PC")
        reopened.close(timeout=5)
        assert local_files[0].read_bytes() == before


def _events(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [str(row["event"]) for row in csv.DictReader(handle)]


def _table_counts(db_path: Path) -> dict[str, int]:
    tables = (
        "deferred_intents",
        "deferred_intent_validation_steps",
        "deferred_intent_transition_audit",
    )
    with sqlite3.connect(db_path) as conn:
        return {
            table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            for table in tables
        }


def test_b_events_split_while_a_and_catalog_events_remain_direct_sync_eligible(
    tmp_path,
):
    scan_dir = tmp_path / "Label_Match" / "data"
    manager = label_module.DataManager(
        str(scan_dir),
        "포장실",
        "operator-contract",
        "CSV-PC01",
    )
    for event_type in sorted(LOCAL_ONLY_EVENT_TYPES):
        manager.log_event(event_type, {"set_id": "local-progress"})
    for event_type in sorted(CONTRACT_CANDIDATE_EVENT_TYPES):
        manager.log_event(event_type, {"set_id": "business-fact"})
    manager.log_event(
        label_module.Label_Match.Events.SCAN_OK,
        {"set_id": "catalog-negative-control"},
    )
    manager.close(timeout=5)

    primary_files = sorted(
        scan_dir.glob("포장실작업이벤트로그_CSV-PC01_*.csv")
    )
    local_dir = tmp_path / "Label_Match" / "local_events"
    local_files = sorted(
        local_dir.glob("포장실로컬이벤트로그_CSV-PC01_*.csv")
    )

    assert len(primary_files) == 1
    assert len(local_files) == 1
    assert set(_events(local_files[0])) == set(LOCAL_ONLY_EVENT_TYPES)
    assert set(_events(primary_files[0])) == (
        set(CONTRACT_CANDIDATE_EVENT_TYPES)
        | {label_module.Label_Match.Events.SCAN_OK}
    )
    assert not (set(_events(primary_files[0])) & set(LOCAL_ONLY_EVENT_TYPES))

    selected, deferred_count = _scan_source_files(
        str(scan_dir),
        ["*.csv", "포장실작업이벤트로그_*.csv"],
        max_files=100,
    )
    assert selected == primary_files
    assert deferred_count == 0
    assert local_files[0] not in selected


def test_local_event_split_does_not_touch_capture_only_sqlite_tables(tmp_path):
    scan_dir = tmp_path / "Label_Match" / "data"
    db_path = scan_dir / "package_logistics_outbox.sqlite3"
    PackageOutbox(db_path)
    before = _table_counts(db_path)

    manager = label_module.DataManager(
        str(scan_dir),
        "포장실",
        "operator-contract",
        "CSV-PC02",
    )
    manager.log_event("EXACT_RESCAN_STARTED", {"set_id": "local-progress"})
    manager.close(timeout=5)

    assert before == {
        "deferred_intents": 0,
        "deferred_intent_validation_steps": 0,
        "deferred_intent_transition_audit": 0,
    }
    assert _table_counts(db_path) == before
    assert not list(scan_dir.glob("포장실작업이벤트로그_*.csv"))
    assert len(
        list(
            (tmp_path / "Label_Match" / "local_events").glob(
                "포장실로컬이벤트로그_CSV-PC02_*.csv"
            )
        )
    ) == 1

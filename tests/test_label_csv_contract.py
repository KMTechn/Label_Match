import csv
import sqlite3
from pathlib import Path

import Label_Match as label_module
from event_stream_policy import (
    CONTRACT_CANDIDATE_EVENT_TYPES,
    LOCAL_ONLY_EVENT_TYPES,
)
from package_logistics import PackageOutbox
from tools.direct_sync_relay_runner import _scan_source_files


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

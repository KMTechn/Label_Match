import json
from pathlib import Path

from event_stream_policy import (
    AUDITED_OUT_OF_CATALOG_EVENT_TYPES,
    CONTRACT_CANDIDATE_EVENT_TYPES,
    FORBIDDEN_EVENT_TYPES,
    LOCAL_ONLY_EVENT_TYPES,
    local_only_event_log_path,
)


EXPECTED_CONTRACT_CANDIDATES = frozenset(
    {
        "PHS_LABEL_ACTIVE_RESOLVED",
        "PHS_LABEL_EXCHANGE_RESULT",
        "PHS_RECONCILIATION_EXCHANGE_RESULT",
        "SEALED_TRANSFER_EXCHANGE_APPLIED",
        "SEALED_TRANSFER_EXCHANGE_ACKED",
    }
)


EXPECTED_LOCAL_ONLY = frozenset(
    {
        "EXACT_RESCAN_STARTED",
        "EXACT_RESCAN_OK",
        "EXACT_RESCAN_COMPLETED",
    }
)


def test_audited_label_csv_values_have_one_frozen_disposition():
    assert CONTRACT_CANDIDATE_EVENT_TYPES == EXPECTED_CONTRACT_CANDIDATES
    assert LOCAL_ONLY_EVENT_TYPES == EXPECTED_LOCAL_ONLY
    assert FORBIDDEN_EVENT_TYPES == frozenset()
    assert AUDITED_OUT_OF_CATALOG_EVENT_TYPES == (
        EXPECTED_CONTRACT_CANDIDATES | EXPECTED_LOCAL_ONLY
    )
    assert len(AUDITED_OUT_OF_CATALOG_EVENT_TYPES) == 8
    assert not (CONTRACT_CANDIDATE_EVENT_TYPES & LOCAL_ONLY_EVENT_TYPES)


def test_local_only_path_is_a_sibling_of_the_direct_sync_scan_source(tmp_path):
    scan_dir = tmp_path / "data"
    contract_path = scan_dir / "포장실작업이벤트로그_PC01_20260829.csv"
    local_dir = tmp_path / "local_events"

    local_path = local_only_event_log_path(
        contract_path,
        local_events_dir=local_dir,
    )

    assert local_path == (
        local_dir / "포장실로컬이벤트로그_PC01_20260829.csv"
    )
    assert local_path.parent != scan_dir


def test_catalog_stream_does_not_admit_local_only_events():
    root = Path(__file__).resolve().parents[1]
    catalog = json.loads(
        (
            root
            / "kmtech_factory_contracts"
            / "bundle"
            / "v1"
            / "catalogs"
            / "canonical-stream-catalog.json"
        ).read_text(encoding="utf-8")
    )
    stream = next(
        row for row in catalog["streams"] if row.get("app_id") == "label_match"
    )
    catalog_events = set(stream["raw_event_names"])

    assert not (catalog_events & LOCAL_ONLY_EVENT_TYPES)
    assert not (catalog_events & CONTRACT_CANDIDATE_EVENT_TYPES)


def test_all_eight_reviewed_values_remain_present_in_label_source():
    source = (
        Path(__file__).resolve().parents[1] / "Label_Match.py"
    ).read_text(encoding="utf-8")

    for event_type in AUDITED_OUT_OF_CATALOG_EVENT_TYPES:
        assert event_type in source

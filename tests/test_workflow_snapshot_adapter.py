from copy import deepcopy

import pytest

from ui.workflow_snapshot_adapter import adapt_workflow_snapshot
from ui.workflow_view_state import WorkflowNotice, present_workflow


def _current(*scans, **updates):
    current = {
        "raw": [f"RAW:{scan}" for scan in scans],
        "parsed": list(scans),
        "has_error_or_reset": False,
        "sealed_transfer": None,
        "exact_rescan_active": False,
        "exact_rescan_complete": False,
        "exact_rescan_target_count": 0,
        "exact_rescan_barcodes": [],
    }
    current.update(updates)
    return current


def test_adapter_copies_accepted_raw_scans_and_all_runtime_fields():
    current = _current(
        "MASTER",
        exact_rescan_active=True,
        exact_rescan_complete=False,
        exact_rescan_target_count=3,
        exact_rescan_barcodes=["EXACT-1", "EXACT-2"],
        has_error_or_reset=True,
    )

    snapshot = adapt_workflow_snapshot(
        current,
        initialized=False,
        loading=True,
        loading_message="복구 상태 준비 중",
        history_readonly=True,
        history_loading=True,
        recovered=True,
        completion_kind="failed",
        error_message="제품 불일치",
        last_normal_scan_override="LAST-NORMAL",
    )

    assert snapshot.qa_scans == ("RAW:MASTER",)
    assert snapshot.exact_rescan_active is True
    assert snapshot.exact_rescan_complete is False
    assert snapshot.exact_rescan_target == 3
    assert snapshot.exact_rescan_barcodes == ("EXACT-1", "EXACT-2")
    assert snapshot.has_error is True
    assert snapshot.error_message == "제품 불일치"
    assert snapshot.initialized is False
    assert snapshot.loading is True
    assert snapshot.loading_message == "복구 상태 준비 중"
    assert snapshot.history_readonly is True
    assert snapshot.history_loading is True
    assert snapshot.recovered is True
    assert snapshot.completion_kind == "failed"
    assert snapshot.last_normal_scan_override == "LAST-NORMAL"


@pytest.mark.parametrize(
    ("raw", "parsed"),
    [
        (["RAW-M"], []),
        ([], ["MASTER"]),
        (["RAW-M", "RAW-P"], ["MASTER"]),
    ],
)
def test_adapter_fails_fast_when_raw_and_parsed_accepted_counts_differ(
    raw,
    parsed,
):
    with pytest.raises(ValueError, match="accepted scan count mismatch"):
        adapt_workflow_snapshot({"raw": raw, "parsed": parsed})


@pytest.mark.parametrize("field", ["raw", "parsed"])
def test_adapter_fails_fast_when_accepted_scan_count_exceeds_five(field):
    current = _current("1", "2", "3", "4", "5", "6")
    assert len(current[field]) == 6

    with pytest.raises(ValueError, match="cannot exceed 5"):
        adapt_workflow_snapshot(current)


def test_adapter_never_mutates_or_aliases_runtime_mapping_and_lists():
    current = _current(
        "MASTER",
        exact_rescan_target_count=2,
        exact_rescan_barcodes=["EXACT-1"],
        sealed_transfer={"BND": "bundle-1", "members": ["P1", "P2"]},
    )
    notice = {
        "title": "중앙 제출 차단",
        "message": "ACK 확인 필요",
        "kind": "submission_blocked",
        "tone": "danger",
    }
    current_before = deepcopy(current)
    notice_before = deepcopy(notice)

    snapshot = adapt_workflow_snapshot(current, blocking_notice=notice)

    assert current == current_before
    assert notice == notice_before
    current["parsed"].append("LATE-MUTATION")
    current["exact_rescan_barcodes"].append("EXACT-2")
    notice["message"] = "CHANGED"
    assert snapshot.qa_scans == ("RAW:MASTER",)
    assert snapshot.exact_rescan_barcodes == ("EXACT-1",)
    assert snapshot.blocking_notice.message == "ACK 확인 필요"


def test_recovered_sample_preserves_live_stage_and_recovery_flag():
    snapshot = adapt_workflow_snapshot(
        _current("MASTER", "PRODUCT-1"),
        recovered=True,
    )
    view = present_workflow(snapshot)

    assert snapshot.recovered is True
    assert view.current_stage == "product_2"
    assert "복구됨" in view.badges


def test_f4_sample_keeps_exact_progress_separate_from_five_step_qa():
    snapshot = adapt_workflow_snapshot(
        _current(
            "MASTER",
            exact_rescan_active=True,
            exact_rescan_target_count=3,
            exact_rescan_barcodes=["EXACT-1", "EXACT-2"],
        )
    )
    view = present_workflow(snapshot)

    assert snapshot.qa_scans == ("RAW:MASTER",)
    assert snapshot.exact_rescan_barcodes == ("EXACT-1", "EXACT-2")
    assert view.qa_progress_text == "1/5"
    assert view.exact_rescan.progress_text == "2/3"


def test_sealed_sample_forwards_presence_without_parsing_business_payload():
    sealed_payload = {"BND": "bundle-1", "AUTH_SCOPE": "PACKAGING"}
    snapshot = adapt_workflow_snapshot(
        _current("SEALED-TRANSFER", sealed_transfer=sealed_payload)
    )
    view = present_workflow(snapshot)

    assert snapshot.sealed_transfer is True
    assert view.exact_rescan.status == "sealed"
    assert view.f4_enabled is True


@pytest.mark.parametrize(
    ("scans", "completion_kind", "stage"),
    [
        (("MASTER", "PRODUCT-1"), "partial", "completion_partial"),
        (
            ("MASTER", "PRODUCT-1", "PRODUCT-2", "PRODUCT-3", "FINAL"),
            "full",
            "completion_full",
        ),
    ],
)
def test_f3_partial_and_full_completion_samples_are_forwarded_distinctly(
    scans,
    completion_kind,
    stage,
):
    snapshot = adapt_workflow_snapshot(
        _current(*scans),
        completion_kind=completion_kind,
    )

    assert snapshot.completion_kind == completion_kind
    assert present_workflow(snapshot).current_stage == stage


def test_error_sample_uses_runtime_flag_and_transient_message():
    snapshot = adapt_workflow_snapshot(
        _current("MASTER", "PRODUCT-1", has_error_or_reset=True),
        error_message="제품 불일치",
    )
    view = present_workflow(snapshot)

    assert snapshot.has_error is True
    assert snapshot.error_message == "제품 불일치"
    assert view.current_stage == "error"
    assert view.last_normal_scan == "RAW:PRODUCT-1"


def test_transient_error_flag_can_precede_runtime_state_write():
    snapshot = adapt_workflow_snapshot(
        _current("MASTER", "PRODUCT-1"),
        has_error=True,
        error_message="형식 오류",
    )

    assert snapshot.has_error is True
    assert present_workflow(snapshot).current_stage == "error"


def test_transient_false_cannot_hide_a_recorded_runtime_error():
    snapshot = adapt_workflow_snapshot(
        _current("MASTER", has_error_or_reset=True),
        has_error=False,
    )

    assert snapshot.has_error is True


def test_submission_blocked_sample_forwards_one_notice_and_last_normal_scan():
    notice = WorkflowNotice(
        "중앙 제출 차단",
        "ACK와 readback을 확인하세요.",
        "submission_blocked",
        "danger",
    )
    snapshot = adapt_workflow_snapshot(
        _current("MASTER", "P1", "P2", "P3", "FINAL"),
        blocking_notice=notice,
        last_normal_scan_override="LAST-ACCEPTED",
    )
    view = present_workflow(snapshot)

    assert snapshot.blocking_notice is notice
    assert snapshot.last_normal_scan_override == "LAST-ACCEPTED"
    assert view.current_stage == "submission_blocked"
    assert view.notice == notice
    assert view.last_normal_scan == "LAST-ACCEPTED"


@pytest.mark.parametrize("value", [[], (), "notice"])
def test_adapter_rejects_multiple_or_non_mapping_notice_shapes(value):
    with pytest.raises(TypeError, match="one WorkflowNotice or Mapping"):
        adapt_workflow_snapshot(_current(), blocking_notice=value)


@pytest.mark.parametrize("field", ["raw", "parsed", "exact_rescan_barcodes"])
def test_adapter_rejects_scalar_scan_collections(field):
    current = _current()
    current[field] = "NOT-A-LIST"

    with pytest.raises(TypeError, match=field):
        adapt_workflow_snapshot(current)

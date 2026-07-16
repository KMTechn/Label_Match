import pytest

from ui.workflow_view_state import WorkflowNotice, WorkflowSnapshot, present_workflow


@pytest.mark.parametrize(
    (
        "count",
        "stage",
        "stage_label",
        "next_action",
        "f3_enabled",
        "f4_enabled",
    ),
    [
        (0, "master_label", "현품표", "1/5 현품표 스캔", False, False),
        (1, "product_1", "제품1", "2/5 제품 1 스캔", False, True),
        (2, "product_2", "제품2", "3/5 제품 2 스캔", True, False),
        (3, "product_3", "제품3", "4/5 제품 3 스캔", True, False),
        (4, "final_label", "최종 라벨", "5/5 최종 라벨 스캔", True, False),
        (5, "complete", "통과 완료", "다음 현품표를 스캔하세요.", False, False),
    ],
)
def test_five_slot_qa_progress_and_actions(
    count,
    stage,
    stage_label,
    next_action,
    f3_enabled,
    f4_enabled,
):
    scans = tuple(f"SCAN-{index}" for index in range(1, count + 1))

    view = present_workflow(WorkflowSnapshot(qa_scans=scans))

    assert view.qa_completed == count
    assert view.qa_total == 5
    assert view.qa_progress_text == f"{count}/5"
    assert view.current_stage == stage
    assert view.current_stage_label == stage_label
    assert view.next_action == next_action
    assert view.f3_enabled is f3_enabled
    assert view.f4_enabled is f4_enabled
    assert len(view.slots) == 5
    assert [slot.filled for slot in view.slots] == [index < count for index in range(5)]
    assert view.last_successful_scan == (scans[-1] if scans else "")


def test_f3_partial_requires_a_clean_product_scan_and_stops_at_five():
    one_scan = present_workflow(WorkflowSnapshot(qa_scans=("MASTER",)))
    partial = present_workflow(WorkflowSnapshot(qa_scans=("MASTER", "PRODUCT-1")))
    errored = present_workflow(
        WorkflowSnapshot(qa_scans=("MASTER", "PRODUCT-1"), has_error=True)
    )

    assert one_scan.f3_enabled is False
    assert one_scan.f3_hint == "제품 1개 이상 스캔 후 가능"
    assert partial.f3_enabled is True
    assert partial.f3_hint == "F3 소량 완료 가능"
    assert errored.f3_enabled is False
    assert errored.f3_hint == "오류 세트는 불가"


@pytest.mark.parametrize(
    ("active", "complete", "members", "status", "progress", "stage", "last_scan"),
    [
        (True, False, (), "active", "0/3", "exact_rescan", "MASTER"),
        (True, False, ("EXACT-1",), "active", "1/3", "exact_rescan", "EXACT-1"),
        (
            False,
            True,
            ("EXACT-1", "EXACT-2", "EXACT-3"),
            "complete",
            "3/3",
            "product_1",
            "EXACT-3",
        ),
    ],
)
def test_f4_exact_rescan_has_separate_progress(
    active,
    complete,
    members,
    status,
    progress,
    stage,
    last_scan,
):
    view = present_workflow(
        WorkflowSnapshot(
            qa_scans=("MASTER",),
            exact_rescan_active=active,
            exact_rescan_complete=complete,
            exact_rescan_target=3,
            exact_rescan_barcodes=members,
        )
    )

    assert view.qa_progress_text == "1/5"
    assert view.exact_rescan.status == status
    assert view.exact_rescan.progress_text == progress
    assert view.current_stage == stage
    assert view.last_successful_scan == last_scan
    assert view.f3_enabled is False
    assert view.f4_enabled is False


def test_sealed_transfer_marks_inherited_membership_and_disables_f4():
    view = present_workflow(
        WorkflowSnapshot(qa_scans=("SEALED-TRANSFER",), sealed_transfer=True)
    )

    assert view.current_stage == "product_1"
    assert view.exact_rescan.status == "sealed"
    assert view.exact_rescan.progress_text == "서버 상속"
    assert view.f4_enabled is False
    assert view.f4_hint == "sealed 멤버십 상속으로 불필요"
    assert "sealed 멤버십" in view.badges


def test_error_marks_current_slot_and_preserves_last_successful_scan():
    view = present_workflow(
        WorkflowSnapshot(
            qa_scans=("MASTER", "PRODUCT-1"),
            has_error=True,
            error_message="제품 불일치",
        )
    )

    assert view.current_stage == "error"
    assert view.current_stage_label == "오류 발생"
    assert view.slots[2].state == "error"
    assert view.last_successful_scan == "PRODUCT-1"
    assert view.qa_progress_text == "2/5"
    assert view.f3_enabled is False
    assert view.f4_enabled is False
    assert view.tone == "danger"
    assert view.notice is not None
    assert "제품 불일치" in view.notice.message
    assert view.next_action == "새 현품표부터 다시 스캔하세요."
    assert "오류" in view.badges


def test_recovered_state_keeps_stage_and_actions_with_recovery_badge():
    view = present_workflow(
        WorkflowSnapshot(
            qa_scans=("MASTER", "PRODUCT-1"),
            recovered=True,
        )
    )

    assert view.current_stage == "product_2"
    assert view.next_action == "3/5 제품 2 스캔"
    assert view.last_successful_scan == "PRODUCT-1"
    assert view.f3_enabled is True
    assert "복구됨" in view.badges


def test_history_readonly_disables_actions_without_losing_live_values():
    view = present_workflow(
        WorkflowSnapshot(
            qa_scans=("MASTER", "PRODUCT-1", "PRODUCT-2"),
            history_readonly=True,
        )
    )

    assert view.current_stage == "history_readonly"
    assert view.current_stage_label == "과거 기록 조회"
    assert view.next_action == "오늘 기록으로 돌아오세요."
    assert view.qa_progress_text == "3/5"
    assert view.last_successful_scan == "PRODUCT-2"
    assert view.f3_enabled is False
    assert view.f4_enabled is False
    assert all(slot.state == "readonly" for slot in view.slots)
    assert [slot.value for slot in view.slots[:3]] == ["MASTER", "PRODUCT-1", "PRODUCT-2"]
    assert "조회 전용" in view.badges


def test_history_loading_blocks_f3_and_f4_until_today_state_is_ready():
    f3_view = present_workflow(
        WorkflowSnapshot(
            qa_scans=("MASTER", "PRODUCT-1"),
            history_loading=True,
        )
    )
    f4_view = present_workflow(
        WorkflowSnapshot(qa_scans=("MASTER",), history_loading=True)
    )

    assert f3_view.current_stage == "history_loading"
    assert f3_view.f3_enabled is False
    assert f4_view.f4_enabled is False


def test_rejects_more_than_five_qa_scans():
    with pytest.raises(ValueError, match="more than 5"):
        present_workflow(
            WorkflowSnapshot(qa_scans=("1", "2", "3", "4", "5", "6"))
        )


@pytest.mark.parametrize(
    ("snapshot", "stage", "notice_kind", "notice_title"),
    [
        (
            WorkflowSnapshot(
                initialized=False,
                history_readonly=True,
                blocking_notice=WorkflowNotice("중앙 제출 차단", "서버 확인 필요", "submission_blocked"),
                has_error=True,
                completion_kind="full",
                exact_rescan_active=True,
                exact_rescan_target=3,
            ),
            "initializing",
            "initializing",
            "초기화 중",
        ),
        (
            WorkflowSnapshot(
                history_readonly=True,
                blocking_notice=WorkflowNotice("중앙 제출 차단", "서버 확인 필요", "submission_blocked"),
                has_error=True,
                completion_kind="full",
                exact_rescan_active=True,
                exact_rescan_target=3,
            ),
            "history_readonly",
            "history_readonly",
            "과거 기록 조회",
        ),
        (
            WorkflowSnapshot(
                history_loading=True,
                blocking_notice=WorkflowNotice("중앙 제출 차단", "서버 확인 필요", "submission_blocked"),
                has_error=True,
                completion_kind="full",
                exact_rescan_active=True,
                exact_rescan_target=3,
            ),
            "history_loading",
            "history_loading",
            "오늘 기록 불러오는 중",
        ),
        (
            WorkflowSnapshot(
                blocking_notice=WorkflowNotice("중앙 제출 차단", "서버 확인 필요", "submission_blocked"),
                has_error=True,
                completion_kind="full",
                exact_rescan_active=True,
                exact_rescan_target=3,
            ),
            "submission_blocked",
            "submission_blocked",
            "중앙 제출 차단",
        ),
        (
            WorkflowSnapshot(
                has_error=True,
                completion_kind="full",
                exact_rescan_active=True,
                exact_rescan_target=3,
            ),
            "error",
            "error",
            "오류 발생",
        ),
        (
            WorkflowSnapshot(
                completion_kind="partial",
                exact_rescan_active=True,
                exact_rescan_target=3,
            ),
            "completion_partial",
            "completion_partial",
            "부분 완료",
        ),
        (
            WorkflowSnapshot(
                qa_scans=("MASTER",),
                exact_rescan_active=True,
                exact_rescan_target=3,
            ),
            "exact_rescan",
            None,
            None,
        ),
        (
            WorkflowSnapshot(qa_scans=("MASTER",)),
            "product_1",
            None,
            None,
        ),
    ],
)
def test_primary_state_priority_produces_at_most_one_notice(
    snapshot,
    stage,
    notice_kind,
    notice_title,
):
    view = present_workflow(snapshot)

    assert view.current_stage == stage
    if notice_kind is None:
        assert view.notice is None
    else:
        assert view.notice is not None
        assert view.notice.kind == notice_kind
        assert view.notice.title == notice_title


@pytest.mark.parametrize(
    ("initialized", "loading", "stage", "title"),
    [
        (False, False, "initializing", "초기화 중"),
        (True, True, "loading", "로딩 중"),
    ],
)
def test_initialization_and_loading_disable_all_change_actions(
    initialized,
    loading,
    stage,
    title,
):
    view = present_workflow(
        WorkflowSnapshot(
            qa_scans=("MASTER", "PRODUCT-1"),
            initialized=initialized,
            loading=loading,
            loading_message="상태 준비 중",
        )
    )

    assert view.current_stage == stage
    assert view.notice is not None
    assert view.notice.title == title
    assert view.notice.message == "상태 준비 중"
    assert view.scan_input_enabled is False
    assert view.cancel_current_enabled is False
    assert view.cancel_completed_enabled is False
    assert view.f3_enabled is False
    assert view.f4_enabled is False


@pytest.mark.parametrize(
    ("kind", "stage", "title", "tone"),
    [
        ("full", "completion_full", "통과 완료", "success"),
        ("partial", "completion_partial", "부분 완료", "warning"),
        ("failed", "completion_failed", "오류 처리 완료", "danger"),
    ],
)
def test_completion_kinds_are_explicit_and_allow_the_next_master_scan(
    kind,
    stage,
    title,
    tone,
):
    scans = ("MASTER", "PRODUCT-1") if kind != "full" else (
        "MASTER",
        "PRODUCT-1",
        "PRODUCT-2",
        "PRODUCT-3",
        "FINAL",
    )
    view = present_workflow(
        WorkflowSnapshot(qa_scans=scans, completion_kind=kind)
    )

    assert view.completion_kind == kind
    assert view.current_stage == stage
    assert view.current_stage_label == title
    assert view.notice is not None
    assert view.notice.title == title
    assert view.tone == tone
    assert view.scan_input_enabled is True
    assert view.cancel_current_enabled is False
    assert view.cancel_completed_enabled is True
    assert view.f3_enabled is False
    assert view.f4_enabled is False


def test_partial_completion_is_not_presented_as_full_completion():
    full = present_workflow(
        WorkflowSnapshot(qa_scans=("M", "1", "2", "3", "F"), completion_kind="full")
    )
    partial = present_workflow(
        WorkflowSnapshot(qa_scans=("M", "1"), completion_kind="partial")
    )

    assert full.current_stage_label == "통과 완료"
    assert partial.current_stage_label == "부분 완료"
    assert full.notice != partial.notice
    assert partial.notice is not None
    assert partial.notice.title == "부분 완료"
    assert "다음 현품표" in partial.next_action


def test_submission_blocked_preserves_five_of_five_and_last_normal_override():
    notice = WorkflowNotice(
        title="중앙 제출 차단",
        message="ACK와 readback을 확인하세요.",
        kind="submission_blocked",
        tone="danger",
    )
    view = present_workflow(
        WorkflowSnapshot(
            qa_scans=("MASTER", "P1", "P2", "P3", "FINAL"),
            blocking_notice=notice,
            last_normal_scan_override="LAST-ACCEPTED-BARCODE",
        )
    )

    assert view.current_stage == "submission_blocked"
    assert view.qa_progress_text == "5/5"
    assert view.notice == notice
    assert view.last_normal_scan == "LAST-ACCEPTED-BARCODE"
    assert view.last_successful_scan == "LAST-ACCEPTED-BARCODE"
    assert view.scan_input_enabled is False
    assert view.cancel_current_enabled is False
    assert view.cancel_completed_enabled is False
    assert view.f3_enabled is False
    assert view.f4_enabled is False


def test_normal_action_states_distinguish_current_and_completed_cancellation():
    idle = present_workflow(WorkflowSnapshot())
    active = present_workflow(WorkflowSnapshot(qa_scans=("MASTER",)))

    assert idle.scan_input_enabled is True
    assert idle.cancel_current_enabled is False
    assert idle.cancel_completed_enabled is True
    assert active.scan_input_enabled is True
    assert active.cancel_current_enabled is True
    assert active.cancel_completed_enabled is True


def test_invalid_completion_kind_and_empty_blocking_notice_fail_fast():
    with pytest.raises(ValueError, match="completion_kind"):
        present_workflow(WorkflowSnapshot(completion_kind="unknown"))
    with pytest.raises(ValueError, match="title and message"):
        present_workflow(
            WorkflowSnapshot(blocking_notice=WorkflowNotice("", "missing title"))
        )

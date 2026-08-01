"""Pure workflow presentation state for legacy QA and central PHS2 packaging.

This module deliberately has no Tk, persistence, logistics, or API imports.  It
turns an already-validated snapshot of the active set into values that a UI can
render without changing any business or durability contract.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


QA_TOTAL = 5
SLOT_DEFINITIONS = (
    ("master_label", "현품표"),
    ("product_1", "제품1"),
    ("product_2", "제품2"),
    ("product_3", "제품3"),
    ("final_label", "최종 라벨"),
)
CENTRAL_INHERIT_ALL_SLOT_DEFINITIONS = (
    ("master_label", "PHS2 현품표"),
)
COMPLETION_KINDS = frozenset({"full", "partial", "failed"})

_OPERATOR_INTERNAL_LANGUAGE = (
    "authority",
    "cas",
    "error_code",
    "hash",
    "http",
    "https",
    "idempotency",
    "journal",
    "ledger",
    "manifest",
    "outbox",
    "package status",
    "package_",
    "principal",
    "readback",
    "receipt",
    "reconciliation",
    "registry",
    "relay",
    "row version",
    "scope",
    "status=",
    "token",
    "uuid",
    "writer claim",
    "writer_claim",
)
_OPERATOR_INTERNAL_ENUMS = frozenset(
    {"ACK", "ACKED", "CONFLICT", "OPERATOR_REVIEW", "PENDING", "SENDING"}
)
_OPERATOR_ERROR_CODE_PREFIX = re.compile(r"(?:^|\s)[A-Z][A-Z0-9_]{2,}\s*:")
_OPERATOR_ENUM_TOKEN = re.compile(
    r"(?<![A-Z0-9_])[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+(?![A-Z0-9_])"
)
_OPERATOR_UUID_TOKEN = re.compile(
    r"(?i)(?<![0-9a-f])[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-"
    r"[89ab][0-9a-f]{3}-[0-9a-f]{12}(?![0-9a-f])"
)
_OPERATOR_INTERNAL_ID_TOKEN = re.compile(
    r"(?i)(?<![A-Z0-9])(?:AUTH|LBL|PHSI|PHSG|CLAIM|SESSION|BUNDLE|EVENT|"
    r"OUTBOX|RECEIPT|WRITER)-[A-Z0-9][A-Z0-9._:-]*(?![A-Z0-9])"
)
_OPERATOR_SHA256_TOKEN = re.compile(r"(?i)(?<![0-9a-f])[0-9a-f]{64}(?![0-9a-f])")


def operator_safe_message(message: object, *, fallback: str) -> str:
    """Return worker guidance without central/API diagnostic evidence."""

    safe_fallback = str(fallback or "다시 확인해 주세요.").strip() or "다시 확인해 주세요."
    candidate = str(message or "").strip()
    if not candidate:
        return safe_fallback
    lowered = candidate.casefold()
    tokens = set(re.findall(r"[A-Z][A-Z0-9_]*", candidate))
    if (
        _OPERATOR_ERROR_CODE_PREFIX.search(candidate)
        or _OPERATOR_ENUM_TOKEN.search(candidate)
        or _OPERATOR_UUID_TOKEN.search(candidate)
        or _OPERATOR_INTERNAL_ID_TOKEN.search(candidate)
        or _OPERATOR_SHA256_TOKEN.search(candidate)
        or bool(tokens & _OPERATOR_INTERNAL_ENUMS)
        or any(marker in lowered for marker in _OPERATOR_INTERNAL_LANGUAGE)
    ):
        return safe_fallback
    return candidate


@dataclass(frozen=True)
class WorkflowNotice:
    """One transient message shown in the workflow's single notice region."""

    title: str
    message: str
    kind: str = "blocking"
    tone: str = "danger"
    allow_current_set_cancel: bool = False


@dataclass(frozen=True)
class WorkflowSnapshot:
    """Business-neutral snapshot used to calculate the operator view."""

    qa_scans: tuple[str, ...] = ()
    initialized: bool = True
    loading: bool = False
    loading_message: str = ""
    has_error: bool = False
    error_message: str = ""
    completion_kind: str | None = None
    blocking_notice: WorkflowNotice | None = None
    last_normal_scan_override: str | None = None
    recovered: bool = False
    history_readonly: bool = False
    history_loading: bool = False
    sealed_transfer: bool = False
    central_inherit_all: bool = False
    exact_rescan_active: bool = False
    exact_rescan_complete: bool = False
    exact_rescan_target: int = 0
    exact_rescan_barcodes: tuple[str, ...] = ()


@dataclass(frozen=True)
class ScanSlotView:
    index: int
    key: str
    label: str
    value: str
    filled: bool
    state: str


@dataclass(frozen=True)
class ExactRescanView:
    status: str
    completed: int
    target: int
    progress_text: str
    message: str


@dataclass(frozen=True)
class WorkflowViewState:
    slots: tuple[ScanSlotView, ...]
    current_stage: str
    current_stage_label: str
    next_action: str
    last_successful_scan: str
    last_normal_scan: str
    qa_completed: int
    qa_total: int
    qa_progress_text: str
    exact_rescan: ExactRescanView
    f3_enabled: bool
    f3_hint: str
    f4_enabled: bool
    f4_hint: str
    badges: tuple[str, ...]
    notice: WorkflowNotice | None
    tone: str
    readonly: bool
    scan_input_enabled: bool
    cancel_current_enabled: bool
    cancel_completed_enabled: bool
    completion_kind: str | None


@dataclass(frozen=True)
class _PrimaryView:
    stage: str
    label: str
    next_action: str
    notice: WorkflowNotice | None
    tone: str


def present_workflow(snapshot: WorkflowSnapshot) -> WorkflowViewState:
    """Return the complete, immutable operator presentation for ``snapshot``."""

    scans = tuple(str(value or "") for value in snapshot.qa_scans)
    if len(scans) > QA_TOTAL:
        raise ValueError(f"qa_scans cannot contain more than {QA_TOTAL} entries")

    exact_members = tuple(str(value or "") for value in snapshot.exact_rescan_barcodes)
    exact_target = max(int(snapshot.exact_rescan_target or 0), 0)
    exact_completed = len(exact_members)
    qa_completed = len(scans)
    slot_definitions = (
        CENTRAL_INHERIT_ALL_SLOT_DEFINITIONS
        if snapshot.central_inherit_all
        else SLOT_DEFINITIONS
    )
    qa_total = len(slot_definitions)
    if qa_completed > qa_total:
        raise ValueError(f"qa_scans cannot contain more than {qa_total} entries for this workflow")
    readonly = bool(snapshot.history_readonly)
    completion_kind = _normalize_completion_kind(snapshot.completion_kind)
    interaction_blocked = (
        not snapshot.initialized
        or snapshot.loading
        or readonly
        or snapshot.history_loading
        or snapshot.blocking_notice is not None
        or snapshot.has_error
    )
    action_gate_reason = _action_gate_reason(snapshot, completion_kind)

    f3_enabled = bool(
        action_gate_reason is None
        and not snapshot.exact_rescan_active
        and (
            (
                snapshot.central_inherit_all
                and qa_completed == qa_total == 1
            )
            or (
                not snapshot.central_inherit_all
                and 2 <= qa_completed < qa_total
            )
        )
    )
    f3_hint = _f3_hint(
        qa_completed=qa_completed,
        qa_total=qa_total,
        central_inherit_all=snapshot.central_inherit_all,
        exact_rescan_active=snapshot.exact_rescan_active,
        gate_reason=action_gate_reason,
    )

    inherited_membership = bool(
        snapshot.sealed_transfer or snapshot.central_inherit_all
    )
    f4_enabled = (
        action_gate_reason is None
        and (
            (
                snapshot.central_inherit_all
                and qa_completed == qa_total == 1
            )
            or (
                snapshot.sealed_transfer
                and 1 <= qa_completed < qa_total
            )
            or (
                not inherited_membership
                and qa_completed == 1
            )
        )
        and not snapshot.exact_rescan_active
        and not snapshot.exact_rescan_complete
    )
    f4_hint = _f4_hint(
        qa_completed=qa_completed,
        sealed_transfer=snapshot.sealed_transfer,
        central_inherit_all=snapshot.central_inherit_all,
        exact_rescan_active=snapshot.exact_rescan_active,
        exact_rescan_complete=snapshot.exact_rescan_complete,
        gate_reason=action_gate_reason,
    )

    exact_rescan = _exact_rescan_view(
        sealed_transfer=inherited_membership,
        active=snapshot.exact_rescan_active,
        complete=snapshot.exact_rescan_complete,
        completed=exact_completed,
        target=exact_target,
        available=f4_enabled,
    )
    primary = _primary_view(
        snapshot=snapshot,
        qa_completed=qa_completed,
        qa_total=qa_total,
        slot_definitions=slot_definitions,
        completion_kind=completion_kind,
        exact_rescan=exact_rescan,
    )
    slots = _slot_views(
        scans=scans,
        slot_definitions=slot_definitions,
        readonly=readonly,
        has_error=snapshot.has_error,
    )
    badges = _badges(snapshot, exact_rescan)
    if snapshot.last_normal_scan_override is not None:
        last_successful_scan = str(snapshot.last_normal_scan_override or "")
    else:
        last_successful_scan = _last_successful_scan(scans, exact_members)

    scan_input_enabled = bool(
        not interaction_blocked
        and not (
            snapshot.central_inherit_all
            and qa_completed == qa_total == 1
        )
    )
    cancelable_active_set = bool(
        0 < qa_completed < qa_total
        or (
            snapshot.central_inherit_all
            and qa_completed == qa_total == 1
        )
    )
    recoverable_blocked_cancel = bool(
        snapshot.blocking_notice is not None
        and snapshot.blocking_notice.allow_current_set_cancel
        and snapshot.central_inherit_all
        and qa_completed == qa_total == 1
        and snapshot.initialized
        and not snapshot.loading
        and not readonly
        and not snapshot.history_loading
        and not snapshot.has_error
    )
    cancel_current_enabled = (
        completion_kind is None
        and cancelable_active_set
        and (not interaction_blocked or recoverable_blocked_cancel)
    )
    cancel_completed_enabled = not interaction_blocked

    return WorkflowViewState(
        slots=slots,
        current_stage=primary.stage,
        current_stage_label=primary.label,
        next_action=primary.next_action,
        last_successful_scan=last_successful_scan,
        last_normal_scan=last_successful_scan,
        qa_completed=qa_completed,
        qa_total=qa_total,
        qa_progress_text=f"{qa_completed}/{qa_total}",
        exact_rescan=exact_rescan,
        f3_enabled=f3_enabled,
        f3_hint=f3_hint,
        f4_enabled=f4_enabled,
        f4_hint=f4_hint,
        badges=badges,
        notice=primary.notice,
        tone=primary.tone,
        readonly=readonly,
        scan_input_enabled=scan_input_enabled,
        cancel_current_enabled=cancel_current_enabled,
        cancel_completed_enabled=cancel_completed_enabled,
        completion_kind=completion_kind,
    )


def _slot_views(
    *,
    scans: tuple[str, ...],
    slot_definitions: tuple[tuple[str, str], ...],
    readonly: bool,
    has_error: bool,
) -> tuple[ScanSlotView, ...]:
    completed = len(scans)
    qa_total = len(slot_definitions)
    rows: list[ScanSlotView] = []
    for offset, (key, label) in enumerate(slot_definitions):
        filled = offset < completed
        value = scans[offset] if filled else ""
        if readonly:
            state = "readonly"
        elif filled:
            state = "complete"
        elif has_error and offset == min(completed, qa_total - 1):
            state = "error"
        elif offset == completed:
            state = "current"
        else:
            state = "pending"
        rows.append(
            ScanSlotView(
                index=offset + 1,
                key=key,
                label=label,
                value=value,
                filled=filled,
                state=state,
            )
        )
    return tuple(rows)


def _primary_view(
    *,
    snapshot: WorkflowSnapshot,
    qa_completed: int,
    qa_total: int,
    slot_definitions: tuple[tuple[str, str], ...],
    completion_kind: str | None,
    exact_rescan: ExactRescanView,
) -> _PrimaryView:
    """Resolve the only headline/notice using the documented priority order."""

    if not snapshot.initialized:
        message = operator_safe_message(
            snapshot.loading_message,
            fallback="앱을 초기화하고 있습니다.",
        )
        notice = WorkflowNotice("초기화 중", message, kind="initializing", tone="info")
        return _PrimaryView("initializing", notice.title, "잠시 기다리세요.", notice, "muted")
    if snapshot.loading:
        message = operator_safe_message(
            snapshot.loading_message,
            fallback="작업 화면을 준비하고 있습니다.",
        )
        notice = WorkflowNotice("로딩 중", message, kind="loading", tone="info")
        return _PrimaryView("loading", notice.title, "로딩이 끝날 때까지 기다리세요.", notice, "muted")
    if snapshot.history_readonly:
        notice = WorkflowNotice(
            "과거 기록 조회",
            "조회 전용입니다. 오늘 기록으로 돌아오세요.",
            kind="history_readonly",
            tone="info",
        )
        return _PrimaryView(
            "history_readonly",
            notice.title,
            "오늘 기록으로 돌아오세요.",
            notice,
            "muted",
        )
    if snapshot.history_loading:
        notice = WorkflowNotice(
            "오늘 기록 불러오는 중",
            "오늘 기록이 준비될 때까지 스캔과 변경 작업을 멈춥니다.",
            kind="history_loading",
            tone="info",
        )
        return _PrimaryView(
            "history_loading",
            notice.title,
            "기록 로딩이 끝날 때까지 기다리세요.",
            notice,
            "muted",
        )
    if snapshot.blocking_notice is not None:
        notice = _normalized_notice(snapshot.blocking_notice)
        stage = "submission_blocked" if notice.kind == "submission_blocked" else "blocked"
        return _PrimaryView(stage, notice.title, notice.message, notice, notice.tone)
    if snapshot.has_error:
        detail = operator_safe_message(
            snapshot.error_message,
            fallback="잘못된 입력을 확인하고 새 현품표부터 다시 시작하세요.",
        )
        action = "새 현품표부터 다시 스캔하세요."
        notice = WorkflowNotice(
            "오류 발생",
            detail,
            kind="error",
            tone="danger",
        )
        return _PrimaryView("error", notice.title, action, notice, "danger")
    if completion_kind is not None:
        return _completion_view(
            completion_kind,
            central_inherit_all=snapshot.central_inherit_all,
        )
    if exact_rescan.status == "active":
        return _PrimaryView(
            "exact_rescan",
            "F4 전체 재스캔",
            f"전체 제품을 재스캔하세요: {exact_rescan.completed}/{exact_rescan.target}",
            None,
            "primary",
        )
    if (
        snapshot.central_inherit_all
        and qa_completed == qa_total == 1
    ):
        return _PrimaryView(
            "package_ready",
            "포장 준비",
            "랩핑 후 F3 포장 완료",
            None,
            "primary",
        )
    if qa_completed >= qa_total:
        return _PrimaryView(
            "complete",
            "통과 완료",
            "다음 현품표를 스캔하세요.",
            None,
            "success",
        )

    key, label = slot_definitions[qa_completed]
    if qa_completed == 0:
        action = f"1/{qa_total} 현품표 스캔"
    elif snapshot.central_inherit_all:
        action = "랩핑 후 F3 포장 완료"
    elif qa_completed < qa_total - 1:
        action = f"{qa_completed + 1}/{qa_total} 제품 {qa_completed} 스캔"
    else:
        action = f"{qa_total}/{qa_total} 최종 라벨 스캔"
    tone = "success" if exact_rescan.status == "complete" else "primary"
    return _PrimaryView(key, label, action, None, tone)


def _completion_view(
    completion_kind: str,
    *,
    central_inherit_all: bool = False,
) -> _PrimaryView:
    if completion_kind == "full":
        notice = WorkflowNotice(
            "통과 완료",
            (
                "서버 전체 멤버십 상속 포장을 기록했습니다."
                if central_inherit_all
                else "정상 5단계를 기록했습니다."
            ),
            kind="completion_full",
            tone="success",
        )
        return _PrimaryView(
            "completion_full",
            notice.title,
            "다음 현품표를 스캔하세요.",
            notice,
            "success",
        )
    if completion_kind == "partial":
        notice = WorkflowNotice(
            "부분 완료",
            "F3 소량 예외를 기록했습니다.",
            kind="completion_partial",
            tone="warning",
        )
        return _PrimaryView(
            "completion_partial",
            notice.title,
            "다음 현품표를 스캔하세요.",
            notice,
            "warning",
        )
    notice = WorkflowNotice(
        "오류 처리 완료",
        "실패 세트를 기록했습니다. 실물을 확인하세요.",
        kind="completion_failed",
        tone="danger",
    )
    return _PrimaryView(
        "completion_failed",
        notice.title,
        "새 현품표부터 다시 스캔하세요.",
        notice,
        "danger",
    )


def _exact_rescan_view(
    *,
    sealed_transfer: bool,
    active: bool,
    complete: bool,
    completed: int,
    target: int,
    available: bool,
) -> ExactRescanView:
    progress_text = f"{completed}/{target}" if target else "-"
    if sealed_transfer:
        return ExactRescanView(
            status="sealed",
            completed=0,
            target=0,
            progress_text="서버 상속",
            message="sealed 이적 QR의 exact membership을 상속합니다.",
        )
    if active:
        return ExactRescanView(
            status="active",
            completed=completed,
            target=target,
            progress_text=progress_text,
            message=f"전체 제품 재스캔 {progress_text}",
        )
    if complete:
        return ExactRescanView(
            status="complete",
            completed=completed,
            target=target,
            progress_text=progress_text,
            message=f"전체 재스캔 완료 {progress_text}",
        )
    if available:
        return ExactRescanView(
            status="available",
            completed=0,
            target=0,
            progress_text="시작 전",
            message="QA 제품 스캔 전에 F4 전체 재스캔을 시작할 수 있습니다.",
        )
    return ExactRescanView(
        status="not_started",
        completed=completed,
        target=target,
        progress_text=progress_text,
        message="F4 전체 재스캔 없음",
    )


def _last_successful_scan(
    qa_scans: tuple[str, ...],
    exact_members: tuple[str, ...],
) -> str:
    # F4 can only run after the master label and before QA product samples.
    # Once a QA product exists it is necessarily newer than the F4 members.
    if exact_members and len(qa_scans) <= 1:
        return exact_members[-1]
    return qa_scans[-1] if qa_scans else ""


def _badges(snapshot: WorkflowSnapshot, exact_rescan: ExactRescanView) -> tuple[str, ...]:
    badges: list[str] = []
    if snapshot.history_readonly:
        badges.append("조회 전용")
    if snapshot.history_loading:
        badges.append("기록 로딩")
    if snapshot.has_error:
        badges.append("오류")
    if snapshot.recovered:
        badges.append("복구됨")
    if snapshot.sealed_transfer:
        badges.append("sealed 멤버십")
    elif snapshot.central_inherit_all:
        badges.append("PHS2 서버 멤버십")
    if exact_rescan.status == "active":
        badges.append(f"F4 {exact_rescan.progress_text}")
    elif exact_rescan.status == "complete":
        badges.append("F4 완료")
    return tuple(badges)


def _normalize_completion_kind(value: str | None) -> str | None:
    if value is None or not str(value).strip():
        return None
    normalized = str(value).strip().lower()
    if normalized not in COMPLETION_KINDS:
        allowed = ", ".join(sorted(COMPLETION_KINDS))
        raise ValueError(f"completion_kind must be one of: {allowed}")
    return normalized


def _normalized_notice(notice: WorkflowNotice) -> WorkflowNotice:
    title = str(notice.title or "").strip()
    message = str(notice.message or "").strip()
    if not title or not message:
        raise ValueError("blocking_notice requires a title and message")
    kind = str(notice.kind or "blocking").strip().lower() or "blocking"
    title_fallback = (
        "중앙 제출 확인 필요"
        if kind == "submission_blocked"
        else "작업 확인 필요"
    )
    message_fallback = (
        "중앙 제출 상태를 자동으로 확인하지 못했습니다. 현재 세트를 유지하고 "
        "재시도하세요. 계속 실패하면 관리자에게 확인을 요청하세요."
        if kind == "submission_blocked"
        else "현재 작업을 유지하고 관리자에게 확인을 요청하세요."
    )
    return WorkflowNotice(
        title=operator_safe_message(title, fallback=title_fallback),
        message=operator_safe_message(message, fallback=message_fallback),
        kind=kind,
        tone=str(notice.tone or "danger").strip().lower() or "danger",
        allow_current_set_cancel=bool(notice.allow_current_set_cancel),
    )


def _action_gate_reason(
    snapshot: WorkflowSnapshot,
    completion_kind: str | None,
) -> str | None:
    if not snapshot.initialized:
        return "초기화 완료 후 가능"
    if snapshot.loading:
        return "로딩 완료 후 가능"
    if snapshot.history_readonly:
        return "과거 기록 조회 중에는 불가"
    if snapshot.history_loading:
        return "오늘 기록 로딩 중에는 불가"
    if snapshot.blocking_notice is not None:
        return "차단 사유 해결 후 가능"
    if snapshot.has_error:
        return "오류 세트는 불가"
    if completion_kind is not None:
        return "완료 처리된 세트에서는 불가"
    return None


def _f3_hint(
    *,
    qa_completed: int,
    qa_total: int,
    central_inherit_all: bool,
    exact_rescan_active: bool,
    gate_reason: str | None,
) -> str:
    if gate_reason:
        return gate_reason
    if exact_rescan_active:
        return "전체 재스캔 중에는 불가"
    if central_inherit_all:
        if qa_completed == qa_total == 1:
            return "랩핑 후 포장 완료"
        return "PHS2 스캔 후 가능"
    if qa_completed < 2:
        return "제품 1개 이상 스캔 후 가능"
    if qa_completed >= QA_TOTAL:
        return f"이미 {QA_TOTAL}개 완료됨"
    return "F3 소량 완료 가능"


def _f4_hint(
    *,
    qa_completed: int,
    sealed_transfer: bool,
    central_inherit_all: bool,
    exact_rescan_active: bool,
    exact_rescan_complete: bool,
    gate_reason: str | None,
) -> str:
    if gate_reason:
        return gate_reason
    if central_inherit_all:
        if qa_completed == 1:
            return "포장 확정 전 제품 1~2개 교체"
        return "PHS2 스캔 후 가능"
    if sealed_transfer:
        if 1 <= qa_completed < QA_TOTAL:
            return "포장 확정 전 제품 1~2개 교체"
        return "포장 확정 후에는 교체 불가"
    if exact_rescan_active:
        return "전체 재스캔 진행 중"
    if exact_rescan_complete:
        return "전체 재스캔 완료"
    if qa_completed == 0:
        return "현품표 스캔 후 가능"
    if qa_completed > 1:
        return "제품 샘플 스캔 전에만 가능"
    return "F4 전체 재스캔 시작 가능"


__all__ = [
    "COMPLETION_KINDS",
    "ExactRescanView",
    "QA_TOTAL",
    "SLOT_DEFINITIONS",
    "ScanSlotView",
    "WorkflowSnapshot",
    "WorkflowNotice",
    "WorkflowViewState",
    "present_workflow",
]

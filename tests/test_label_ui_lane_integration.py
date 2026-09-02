from __future__ import annotations

import inspect
from types import SimpleNamespace
import threading
import time
from unittest.mock import Mock

import pytest

import Label_Match as label_module
from deferred_intent_capture import (
    DeferredValidationClaim,
    DeferredValidationResult,
)
from tests.test_label_operator_action_gates import FakeWidget, _render_app
from tests.test_tk_serial_ui_lane import FakeTkRoot
from tk_serial_ui_lane import (
    CoalescingTrigger,
    Failure,
    LaneState,
    LaneTask,
    TkSerialUiLane,
)


def _app_with_lane():
    root = FakeTkRoot()
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app._ui_lane_generation = 0
    app._ui_lane_busy_label = ""
    app._ui_lane_busy_task = ""
    app._tk_shutdown_requested = False
    app.operator_workbench_ready = False
    app.current_set_info = {"id": "set-1", "raw": [], "parsed": []}
    app.update_big_display = lambda *_args: None
    app._render_operator_workbench = lambda: None
    app._focus_scan_entry_if_available = lambda: None
    app.ui_lane = TkSerialUiLane(
        root,
        poll_ms=1,
        generation_provider=lambda: app._ui_lane_generation,
    )
    return app, root


def _close_lane(app, root):
    if app.ui_lane.is_busy():
        root.run_until(lambda: not app.ui_lane.is_busy())
    app.ui_lane.close_idle()
    assert str(app.ui_lane.state) == "CLOSED"


def test_label_exports_canonical_ui_lane_contract():
    assert label_module.UI_LANE_SPEC == "kmtech-tk-ui-lane-v1"


def test_phs2_capture_validation_runs_off_tk_and_materializes_on_tk():
    app, root = _app_with_lane()
    owner = root.owner_thread_id
    thread_trace = []
    capture = SimpleNamespace(intent_id="intent-1")
    claim = DeferredValidationClaim(
        intent_id="intent-1",
        worker_id="validator",
        fence=1,
        validation_generation=1,
        validation_attempt_count=1,
        claim_expires_at="2099-01-01T00:00:00Z",
        payload={},
    )
    result = DeferredValidationResult(
        intent_id="intent-1",
        state="VALIDATED",
        outcome="VALID",
        reason_code="ORDERED_LABEL_VALIDATION_VALID",
        observed_at="2026-09-01T00:00:00Z",
    )
    materialization = {
        "intent_id": "intent-1",
        "local_work_identity": "set-1",
        "evidence": object(),
        "snapshot": {},
        "sealed": None,
        "operation_lease": {},
    }

    def capture_scan(*_args):
        thread_trace.append(("capture", threading.get_ident()))
        return capture

    def prepare(_capture):
        thread_trace.append(("prepare", threading.get_ident()))
        return claim

    def execute(_claim, *, return_materialization=False):
        assert return_materialization is True
        thread_trace.append(("execute", threading.get_ident()))
        return result, materialization

    def materialize(value):
        thread_trace.append(("materialize", threading.get_ident()))
        assert value is result
        assert app._deferred_label_materialization is materialization
        return True

    app._capture_central_phs2_scan = capture_scan
    app._prepare_deferred_label_validation = prepare
    app._execute_deferred_label_validation = execute
    app._materialize_validated_deferred_label = materialize
    app._show_deferred_validation_result = lambda _value: pytest.fail(
        "validated result should materialize"
    )
    app._show_deferred_capture_failure = lambda error: pytest.fail(str(error))
    app._show_deferred_capture_pending = lambda *_args, **_kwargs: None
    app.deferred_intent_capture = SimpleNamespace(
        validation_status=lambda _intent_id: None
    )
    app._phs_label_scan_lookup_in_progress = False

    started_at = time.perf_counter()
    assert app._begin_central_phs2_scan_overlay_on_lane(
        "PHS2-RAW",
        "ITEM-1",
        on_capture_committed=lambda: thread_trace.append(
            ("input-consumed", threading.get_ident())
        ),
    ) is True
    assert time.perf_counter() - started_at < 0.1
    assert app._ui_lane_busy_label == "현품표 저장 · 중앙 확인 중"
    root.run_until(lambda: not app.ui_lane.is_busy())

    worker_ids = {
        thread_id
        for name, thread_id in thread_trace
        if name in {"capture", "prepare", "execute"}
    }
    assert worker_ids == {app.ui_lane.worker_thread_id}
    assert ("input-consumed", owner) in thread_trace
    assert thread_trace.index(("capture", app.ui_lane.worker_thread_id)) < (
        thread_trace.index(("input-consumed", owner))
    )
    assert ("materialize", owner) in thread_trace
    assert app._phs_label_scan_lookup_in_progress is False
    assert app._ui_lane_busy_label == ""
    _close_lane(app, root)


def test_deferred_scheduler_candidate_and_prepare_run_off_tk():
    app, root = _app_with_lane()
    owner = root.owner_thread_id
    trace = []
    result = DeferredValidationResult(
        intent_id="intent-scheduler",
        state="WAITING_DEPENDENCY",
        outcome="REQUIRED_ABSENT",
        reason_code="DEPENDENCY_PENDING",
        observed_at="2026-09-01T00:00:00Z",
    )

    class Store:
        @staticmethod
        def next_validation_candidate():
            trace.append(("candidate", threading.get_ident()))
            return "intent-scheduler"

    def prepare(intent_id):
        assert intent_id == "intent-scheduler"
        trace.append(("prepare", threading.get_ident()))
        return result

    app.run_tests = False
    app.deferred_intent_capture = Store()
    app._deferred_observability_read_in_progress = False
    app._prepare_deferred_intent_validation = prepare
    app._materialize_validated_deferred_label = lambda _value: False
    app._show_deferred_validation_result = lambda _value: trace.append(
        ("render", threading.get_ident())
    )
    app._refresh_deferred_observability = lambda: trace.append(
        ("observe", threading.get_ident())
    )
    app._schedule_deferred_validation_worker = (
        lambda _delay: trace.append(("schedule", threading.get_ident()))
    )
    app._deferred_validation_lane_trigger = CoalescingTrigger(
        app.ui_lane,
        app._build_deferred_validation_lane_task,
        on_admitted=app._on_deferred_validation_lane_admitted,
    )

    app._run_deferred_validation_worker_once()
    root.run_until(lambda: not app.ui_lane.is_busy())

    assert ("candidate", app.ui_lane.worker_thread_id) in trace
    assert ("prepare", app.ui_lane.worker_thread_id) in trace
    assert ("render", owner) in trace
    assert ("observe", owner) in trace
    assert ("schedule", owner) in trace
    assert app._ui_lane_busy_label == ""
    _close_lane(app, root)


def test_deferred_scheduler_failure_readback_stays_off_tk():
    app, root = _app_with_lane()
    owner = root.owner_thread_id
    trace = []
    claim = DeferredValidationClaim(
        intent_id="intent-failure",
        worker_id="validator",
        fence=1,
        validation_generation=1,
        validation_attempt_count=1,
        claim_expires_at="2099-01-01T00:00:00Z",
        payload={},
    )
    durable = DeferredValidationResult(
        intent_id="intent-failure",
        state="WAITING_DEPENDENCY",
        outcome="REQUIRED_ABSENT",
        reason_code="DEPENDENCY_PENDING",
        observed_at="2026-09-01T00:00:00Z",
    )

    class Store:
        @staticmethod
        def next_validation_candidate():
            return "intent-failure"

        @staticmethod
        def validation_status(intent_id):
            assert intent_id == "intent-failure"
            trace.append(("readback", threading.get_ident()))
            return durable

    app.deferred_intent_capture = Store()
    app._prepare_deferred_intent_validation = lambda _intent_id: claim

    def execute(*_args, **_kwargs):
        raise RuntimeError("offline")

    app._execute_deferred_label_validation = execute
    app._show_deferred_validation_result = lambda value: trace.append(
        ("render", threading.get_ident(), value)
    )
    app._refresh_deferred_observability = lambda: None
    app._schedule_deferred_validation_worker = lambda _delay: None

    task = app._build_deferred_validation_lane_task()
    assert app.ui_lane.submit(task).accepted
    root.run_until(lambda: not app.ui_lane.is_busy())

    assert ("readback", app.ui_lane.worker_thread_id) in trace
    assert ("render", owner, durable) in trace
    _close_lane(app, root)


def test_f4_central_lookup_runs_off_tk_and_applies_on_tk():
    app, root = _app_with_lane()
    owner = root.owner_thread_id
    trace = []
    app.current_set_info = {
        "id": "set-f4",
        "raw": ["PHS2-F4"],
        "parsed": ["ITEM-F4"],
    }
    app._central_seal_lookup_in_progress = False
    app._resolve_central_phs2_seal_for_exchange = (
        lambda _captured: (
            trace.append(("resolve", threading.get_ident()))
            or ({"seal": "ok"}, {"bundle_id": "bundle"}, {})
        )
    )
    app._apply_resolved_central_phs2_seal = (
        lambda *_args: trace.append(("apply", threading.get_ident()))
    )
    app._prompt_sealed_transfer_exchange = (
        lambda: trace.append(("prompt", threading.get_ident())) or True
    )

    assert app._start_central_phs2_exchange_on_lane() is True
    assert app._ui_lane_busy_label == "제품 교체 · 중앙 확인 중"
    root.run_until(lambda: not app.ui_lane.is_busy())

    assert trace == [
        ("resolve", app.ui_lane.worker_thread_id),
        ("apply", owner),
        ("prompt", owner),
    ]
    assert app._central_seal_lookup_in_progress is False
    _close_lane(app, root)


def test_f4_lease_and_pending_exchange_gate_runs_off_tk():
    app, root = _app_with_lane()
    owner = root.owner_thread_id
    trace = []
    captured = {
        "id": "set-f4-gate",
        "raw": ["PHS2-F4-GATE"],
        "parsed": ["ITEM-F4-GATE"],
    }
    app.current_set_info = dict(captured)
    app._operation_lease_blocks_f4 = lambda value: (
        trace.append(("lease-gate", threading.get_ident(), value["id"]))
        or False
    )
    app._current_sealed_transfer_exchange_attempt = lambda value: (
        trace.append(("pending-gate", threading.get_ident(), value["id"]))
        or None
    )
    app._prompt_sealed_transfer_exchange = lambda **kwargs: (
        trace.append(("prompt", threading.get_ident(), kwargs)) or True
    )

    assert app._begin_f4_operation_lease_gate_on_lane(captured) is True
    assert app._ui_lane_busy_label == "제품 교체 · 포장 상태 확인 중"
    root.run_until(lambda: not app.ui_lane.is_busy())

    worker_id = app.ui_lane.worker_thread_id
    assert trace == [
        ("lease-gate", worker_id, "set-f4-gate"),
        ("pending-gate", worker_id, "set-f4-gate"),
        (
            "prompt",
            owner,
            {
                "_lease_gate_checked": True,
                "_pending_checked": True,
            },
        ),
    ]
    _close_lane(app, root)


def test_f3_lease_outbox_and_flush_run_off_tk_before_ui_apply(monkeypatch):
    app, root = _app_with_lane()
    owner = root.owner_thread_id
    trace = []
    app.initialized_successfully = True
    app.is_running_simulation = False
    app.run_tests = False
    app.current_set_info = {
        "id": "set-f3",
        "raw": ["PHS2-F3"],
        "parsed": ["ITEM-F3"],
        "operation_lease_id": "",
    }

    class DataManager:
        @staticmethod
        def save_current_state(_state):
            trace.append(("state-save", threading.get_ident()))
            return True

        @staticmethod
        def log_event(_event, _details):
            trace.append(("event", threading.get_ident()))

        @staticmethod
        def flush(timeout=None):
            assert timeout == 5.0
            trace.append(("flush", threading.get_ident()))
            return True

    class Outbox:
        @staticmethod
        def mark_local_completion_committed(*_args, **_kwargs):
            trace.append(("outbox-marker", threading.get_ident()))

    def queue_package(**kwargs):
        trace.append(("lease-outbox", threading.get_ident()))
        snapshot = kwargs["current_set_info"]
        snapshot["operation_lease_id"] = "lease-f3"
        snapshot["operation_lease_fence"] = 4
        snapshot["operation_lease_snapshot_hash"] = "a" * 64
        snapshot["operation_lease_expires_at"] = "2099-01-01T00:00:00Z"
        snapshot["operation_lease_completed_at"] = "2026-09-01T00:00:00Z"
        assert kwargs["persist_current_state"](snapshot) is True
        return {
            "status": "PENDING",
            "idempotency_key": "label-package-f3",
            "operation_lease_id": "lease-f3",
            "operation_lease_completed_at": "2026-09-01T00:00:00Z",
        }

    def finalize_ui(*_args, **kwargs):
        trace.append(("ui-apply", threading.get_ident()))
        assert kwargs["_durable_completion"]["package_logistics"][
            "idempotency_key"
        ] == "label-package-f3"
        return True

    monkeypatch.setattr(
        label_module,
        "_label_match_local_completion_event_exists",
        lambda *_args: False,
    )
    app.data_manager = DataManager()
    app.package_outbox = Outbox()
    app._queue_authoritative_package = queue_package
    app._finalize_set = finalize_ui
    app._publish_durable_commit_block = lambda error: pytest.fail(str(error))

    assert app._submit_finalized_set_on_lane(
        result=app.Results.PASS,
        error_details="",
        is_manual_complete=False,
        details={"set_id": "set-f3"},
        item_code="ITEM-F3",
        central_inherit_all=True,
        set_id_for_log="set-f3",
    ) is True
    assert app._ui_lane_busy_label == "포장 완료 · 중앙 저장 중"
    root.run_until(lambda: not app.ui_lane.is_busy())

    worker_id = app.ui_lane.worker_thread_id
    for name in (
        "state-save",
        "lease-outbox",
        "event",
        "flush",
        "outbox-marker",
    ):
        assert (name, worker_id) in trace
    assert trace[-1] == ("ui-apply", owner)
    assert app.current_set_info["operation_lease_id"] == "lease-f3"
    assert app._ui_lane_busy_label == ""
    _close_lane(app, root)


def test_scan_is_not_cleared_while_lane_is_busy():
    app, root = _app_with_lane()
    gate = threading.Event()
    deleted = []
    rejected = []
    app._app_close_in_progress = False
    app.entry = SimpleNamespace(
        get=lambda: "PHS2-PRESERVE",
        delete=lambda *_args: deleted.append(True),
    )
    app._show_ui_lane_rejection = lambda reason: rejected.append(reason)
    assert app.ui_lane.submit(
        LaneTask(
            "blocking",
            0,
            lambda: gate.wait(timeout=2.0),
            lambda _value: None,
            pytest.fail,
        )
    ).accepted

    app.process_input()

    assert deleted == []
    assert rejected == ["busy"]
    gate.set()
    root.run_until(lambda: not app.ui_lane.is_busy())
    _close_lane(app, root)


def test_broken_lane_keeps_critical_warning_and_scan_entry_fail_closed():
    app = _render_app()
    root = FakeTkRoot()
    raw_input = "PHS=2|CLC=ITEM-001|PROBE=L5"
    deleted = []
    capture_work = Mock()
    gate = threading.Event()
    app._ui_lane_generation = 0
    app._ui_lane_busy_label = ""
    app._ui_lane_busy_task = ""
    app._app_close_in_progress = False
    app.status_label = FakeWidget()
    app.entry.get = lambda: raw_input
    app.entry.delete = lambda *_args: deleted.append(True)
    app._capture_central_phs2_scan = capture_work
    app.ui_lane = TkSerialUiLane(
        root,
        poll_ms=1,
        generation_provider=lambda: app._ui_lane_generation,
        on_runner_fault=app._handle_ui_lane_fault,
    )

    admission = app._submit_ui_lane_task(
        name="break-lane",
        busy_text="처리 중",
        work=lambda: gate.wait(timeout=2.0),
        finish=lambda _value: None,
        fail=pytest.fail,
    )
    assert admission.accepted is True
    assert app.entry.options["state"] == "disabled"
    assert app.ui_lane.break_for_shutdown_timeout(
        RuntimeError("forced BROKEN lane")
    ) is True

    scan_event_return = app._handle_scan_enter()
    path_result = app._begin_central_phs2_scan_overlay_on_lane(
        raw_input,
        "ITEM-001",
    )
    admission = app.ui_lane.submit(
        LaneTask("after-broken", 0, lambda: None, lambda _value: None, pytest.fail)
    )

    assert scan_event_return is None
    assert app.big_display_label.options["text"] == "처리 상태 확인 필요"
    assert app.big_display_label.options["foreground"] == app.colors["danger"]
    assert app.status_label.options["text"] == (
        "처리 상태를 확인할 수 없습니다. 추가 스캔을 중지하고 관리자에게 문의하세요."
    )
    assert app.status_label.options["style"] == "Error.TLabel"
    assert app.status_label.mapped is True
    assert app.entry.options["state"] == "disabled"
    assert app.entry.get() == raw_input
    assert deleted == []
    assert path_result is False
    capture_work.assert_not_called()
    assert admission.accepted is False
    assert admission.reason == "broken"
    gate.set()
    _close_lane(app, root)


def test_scan_is_not_cleared_when_non_lane_gate_rejects_it():
    app, root = _app_with_lane()
    deleted = []
    app._app_close_in_progress = False
    app.is_blinking = False
    app.initialized_successfully = True
    app.entry = SimpleNamespace(
        get=lambda: "PHS2-PRESERVE-BLOCKED",
        delete=lambda *_args: deleted.append(True),
    )
    app._sealed_transfer_exchange_blocks_local_action = (
        lambda action: action == "다음 스캔"
    )

    app.process_input()

    assert deleted == []
    _close_lane(app, root)


def test_current_set_reset_advances_ui_generation():
    app, root = _app_with_lane()
    app.is_blinking = False
    app.initialized_successfully = False
    app.progress_bar = {}
    initial_generation = app._ui_lane_generation

    assert app._reset_current_set() is True

    assert app._ui_lane_generation == initial_generation + 1
    _close_lane(app, root)


def test_generation_transition_settles_stale_task_and_clears_busy_ui():
    app, root = _app_with_lane()
    gate = threading.Event()
    settled = []
    rendered = []

    admission = app._submit_ui_lane_task(
        name="stale-generation",
        busy_text="중앙 확인 중",
        work=lambda: gate.wait(timeout=2.0) or "receipt",
        finish=rendered.append,
        fail=lambda error: rendered.append(error),
        settle=lambda value, error: settled.append((value, error)),
    )
    assert admission.accepted is True
    assert app._ui_lane_busy_label == "중앙 확인 중"

    app._advance_ui_lane_generation()
    gate.set()
    root.run_until(lambda: not app.ui_lane.is_busy())

    assert settled == [(True, None)]
    assert rendered == []
    assert app._ui_lane_busy_label == ""
    _close_lane(app, root)


def test_stale_phs2_checkpoint_cannot_mutate_new_current_set():
    app, root = _app_with_lane()
    app._ui_lane_generation = 1
    release_checkpoint = threading.Event()
    capture = SimpleNamespace(intent_id="intent-from-generation-1")
    capture_committed = []
    settled = []
    app._show_deferred_capture_pending = (
        lambda *_args, **_kwargs: pytest.fail("stale checkpoint rendered")
    )

    def work():
        assert release_checkpoint.wait(timeout=2.0)
        return app.ui_lane.call_ui_sync(
            app._apply_captured_central_phs2_scan,
            capture,
            central_check_pending=True,
            local_work_identity="current-set-generation-1",
            on_capture_committed=lambda: capture_committed.append("called"),
        )

    app._submit_ui_lane_task(
        name="stale-phs2-checkpoint",
        busy_text="현품표 저장 중",
        work=work,
        finish=pytest.fail,
        fail=pytest.fail,
        settle=lambda value, error: settled.append((value, error)),
    )
    app._advance_ui_lane_generation()
    current_set = {
        "id": "current-set-generation-2",
        "raw": ["GENERATION-2"],
        "parsed": ["ITEM-002"],
    }
    app.current_set_info = current_set
    current_set_before = dict(current_set)
    release_checkpoint.set()
    root.run_until(lambda: not app.ui_lane.is_busy())

    assert app.current_set_info is current_set
    assert app.current_set_info == current_set_before
    assert capture_committed == []
    assert len(settled) == 1
    assert settled[0][0] is None
    assert isinstance(settled[0][1], Failure)
    assert settled[0][1].cause_type == "RuntimeError"
    assert app._ui_lane_busy_label == ""
    _close_lane(app, root)


def test_current_generation_keeps_busy_visible_through_terminal_apply():
    app, root = _app_with_lane()
    observed_busy = []

    admission = app._submit_ui_lane_task(
        name="terminal-busy-order",
        busy_text="중앙 확인 중",
        work=lambda: "receipt",
        finish=lambda _value: observed_busy.append(
            app._ui_lane_busy_label
        ),
        fail=pytest.fail,
    )
    assert admission.accepted is True
    root.run_until(lambda: not app.ui_lane.is_busy())

    assert observed_busy == ["중앙 확인 중"]
    assert app._ui_lane_busy_label == ""
    _close_lane(app, root)


def test_failed_durable_capture_preserves_input_and_exposes_only_failure(
    capsys,
):
    app, root = _app_with_lane()
    consumed = []
    failures = []
    app._phs_label_scan_lookup_in_progress = False

    def capture(*_args):
        raise RuntimeError(
            "Authorization: Bearer DO-NOT-LEAK; "
            "https://secret.invalid/capture"
        )

    app._capture_central_phs2_scan = capture
    app._show_deferred_capture_failure = failures.append
    app._show_deferred_capture_pending = lambda *_args, **_kwargs: None
    app._render_operator_workbench = lambda: None

    assert app._begin_central_phs2_scan_overlay_on_lane(
        "PHS2-RAW",
        "ITEM-1",
        on_capture_committed=lambda: consumed.append(True),
    ) is True
    root.run_until(lambda: not app.ui_lane.is_busy())

    assert consumed == []
    assert len(failures) == 1
    assert failures[0].safe_operator_code == "PHS2_CAPTURE_FAILED"
    diagnostic = capsys.readouterr().out + repr(failures[0])
    assert "DO-NOT-LEAK" not in diagnostic
    assert "secret.invalid" not in diagnostic
    _close_lane(app, root)


def test_program_close_bypasses_f5_pending_gate_for_lane_drain():
    app, root = _app_with_lane()
    app.run_tests = True
    app._phs_label_exchange_pending = True
    app._phs_label_candidate_pending = True
    app._phs_reconciliation_lookup_pending = True
    app._phs_label_scan_lookup_in_progress = True

    assert (
        app._sealed_transfer_exchange_blocks_local_action("프로그램 종료")
        is False
    )
    _close_lane(app, root)


@pytest.mark.parametrize("admission_kind", ("f5", "deferred"))
def test_confirmed_close_blocks_f5_and_deferred_admission_while_active_drains(
    admission_kind,
):
    app, root = _app_with_lane()
    active_gate = threading.Event()
    entry_states = []
    settings_states = []
    deferred_timer_id = root.after(60000, lambda: None)
    app.after = root.after
    app.after_cancel = root.after_cancel
    app.initialized_successfully = True
    app.run_tests = True
    app.is_blinking = True
    app.entry = SimpleNamespace(
        configure=lambda **kwargs: entry_states.append(kwargs["state"])
    )
    app.settings_button = SimpleNamespace(
        configure=lambda **kwargs: settings_states.append(kwargs["state"])
    )
    app._deferred_validation_after_id = deferred_timer_id
    app._has_background_work = lambda: False
    app._arm_app_close_lane_drain_watchdog = lambda _lane: None

    app.ui_lane.submit(
        LaneTask(
            "active-at-close-confirmation",
            0,
            lambda: active_gate.wait(timeout=2.0),
            lambda _value: None,
            pytest.fail,
        )
    )

    try:
        label_module.Label_Match.on_closing(app)
        app.on_closing = lambda _confirmed=False: None
        active_gate.set()
        root.run_until(lambda: not app.ui_lane.is_busy())

        if admission_kind == "f5":
            app._phs_reconciliation_lookup_pending = False
            app._phs_reconciliation_scope = lambda: "PACKAGING"
            app.phs_label_exchange_coordinator = SimpleNamespace(
                resolve_reconciliation_actions=lambda **_kwargs: {"actions": []}
            )
            app._show_phs_replacement_required_notice_once = lambda _value: None
            app._show_phs_reconciliation_action_window = (
                lambda _value, **_kwargs: None
            )
            admitted_during_close = app._begin_phs_reconciliation_lookup(
                "PHS2-DURING-CLOSE"
            )
            rejection_reason = "closing"
        else:
            trigger_options = {}
            submit_deferred_method = getattr(
                label_module.Label_Match,
                "_submit_deferred_validation_lane_task",
                None,
            )
            if callable(submit_deferred_method):
                trigger_options["submit_task"] = submit_deferred_method.__get__(
                    app,
                    label_module.Label_Match,
                )
            trigger = CoalescingTrigger(
                app.ui_lane,
                lambda: LaneTask(
                    "deferred-tick-during-close",
                    0,
                    lambda: None,
                    lambda _value: None,
                    pytest.fail,
                ),
                **trigger_options,
            )
            deferred_admission = trigger.trigger()
            admitted_during_close = deferred_admission.accepted
            rejection_reason = deferred_admission.reason

        assert admitted_during_close is False
        assert rejection_reason == "closing"
        assert app._app_close_in_progress is True
        assert entry_states == ["disabled"]
        assert settings_states == ["disabled"]
        assert app._deferred_validation_after_id is None
        assert deferred_timer_id in root.cancelled
        current_before_blocked_f1 = dict(app.current_set_info)
        assert app._reset_current_set(full_reset=True) is False
        assert app.current_set_info == current_before_blocked_f1
        assert app.open_settings_window() is None
        assert app._save_settings_and_close(object(), "new-worker") is None
    finally:
        active_gate.set()
        if app.ui_lane.is_busy():
            root.run_until(lambda: not app.ui_lane.is_busy())
        app._app_close_in_progress = False
        if app.ui_lane.state is not LaneState.CLOSED:
            _close_lane(app, root)


def test_close_failure_keeps_lane_alive_for_resumed_f5_submit():
    app, root = _app_with_lane()
    entry_states = []
    settings_states = []
    outbox_restarts = []
    cancelled_ui_jobs = []

    class CloseFailingDataManager:
        def __init__(self):
            self._close_requested = False
            self.log_thread = SimpleNamespace(is_alive=lambda: False)

        @staticmethod
        def log_event(_event, _details):
            return None

        def close(self, timeout=None):
            self._close_requested = True
            raise RuntimeError("forced close failure")

    class ReplacementDataManager:
        def __init__(self):
            self.events = []

        def log_event(self, event, details):
            self.events.append((event, details))

        @staticmethod
        def flush(timeout=None):
            return True

    app.initialized_successfully = True
    app.run_tests = True
    app.is_blinking = True
    app.entry = SimpleNamespace(
        configure=lambda **kwargs: entry_states.append(kwargs["state"])
    )
    app.settings_button = SimpleNamespace(
        configure=lambda **kwargs: settings_states.append(kwargs["state"])
    )
    app.data_manager = CloseFailingDataManager()
    app._sealed_transfer_exchange_blocks_local_action = lambda _action: False
    app._has_background_work = lambda: False
    app._cancel_pending_ui_jobs = lambda: cancelled_ui_jobs.append(True)
    app.after = root.after
    app.after_cancel = root.after_cancel
    deferred_timer_id = root.after(60000, lambda: None)
    app._deferred_validation_after_id = deferred_timer_id
    replacement_manager = ReplacementDataManager()

    def replace_closed_manager(_manager):
        app.data_manager = replacement_manager
        return True

    app._replace_closed_data_manager_after_close_failure = replace_closed_manager
    app._start_package_outbox_drain = (
        lambda: outbox_restarts.append(True)
    )
    app._save_app_settings = lambda: pytest.fail("settings should not save")
    app.destroy = lambda: pytest.fail("window should not close")
    app._phs_reconciliation_lookup_pending = False
    app._phs_reconciliation_scope = lambda: "PACKAGING"
    app.phs_label_exchange_coordinator = SimpleNamespace(
        resolve_reconciliation_actions=lambda **_kwargs: {"actions": []}
    )
    app._show_phs_replacement_required_notice_once = lambda _value: None
    app._show_phs_reconciliation_action_window = (
        lambda _value, **_kwargs: None
    )

    admitted_after_resume = False
    lane_state_after_failure = None
    close_error = None
    try:
        try:
            app.on_closing()
        except RuntimeError as error:
            close_error = error

        lane_state_after_failure = app.ui_lane.state
        deferred_jobs = [
            job
            for job in root.jobs
            if job[0] not in root.cancelled
            and job[1] == 1000
            and getattr(job[2], "__name__", "") == "run_once"
        ]
        assert len(deferred_jobs) == 1
        assert app._deferred_validation_after_id == deferred_jobs[0][0]
        assert deferred_timer_id in root.cancelled
        assert "_app_close_resume_deferred_validation" not in app.__dict__
        root.after_cancel(app._deferred_validation_after_id)
        app._deferred_validation_after_id = None
        admitted_after_resume = app._begin_phs_reconciliation_lookup(
            "PHS2-AFTER-CLOSE-FAILURE"
        )
        if admitted_after_resume:
            root.run_until(lambda: not app.ui_lane.is_busy())

        assert lane_state_after_failure is LaneState.IDLE
        assert admitted_after_resume is True
        assert str(close_error) == "forced close failure"
        assert app.data_manager is replacement_manager
    finally:
        deferred_after_id = app.__dict__.get("_deferred_validation_after_id")
        if deferred_after_id is not None:
            root.after_cancel(deferred_after_id)
            app._deferred_validation_after_id = None
        if app.ui_lane.is_busy():
            root.run_until(lambda: not app.ui_lane.is_busy())
        if app.ui_lane.state is not LaneState.CLOSED:
            _close_lane(app, root)


def test_close_failure_compensates_durable_app_close_before_resume():
    app, root = _app_with_lane()
    durable_events = []
    flush_timeouts = []

    class CloseFailingDataManager:
        def __init__(self):
            self._close_requested = False
            self.log_thread = SimpleNamespace(is_alive=lambda: False)

        def log_event(self, event, details):
            durable_events.append((event, details))

        def close(self, timeout=None):
            self._close_requested = True
            raise RuntimeError("forced close failure after APP_CLOSE")

    class ReplacementDataManager:
        def log_event(self, event, details):
            durable_events.append((event, details))

        def flush(self, timeout=None):
            flush_timeouts.append(timeout)
            return True

    app.initialized_successfully = True
    app.run_tests = True
    app.is_blinking = True
    app.entry = SimpleNamespace(configure=lambda **_kwargs: None)
    app.settings_button = SimpleNamespace(configure=lambda **_kwargs: None)
    app.data_manager = CloseFailingDataManager()
    app._sealed_transfer_exchange_blocks_local_action = lambda _action: False
    app._has_background_work = lambda: False
    app._start_package_outbox_drain = lambda: None
    app.after = root.after
    app.after_cancel = root.after_cancel
    deferred_timer_id = root.after(60000, lambda: None)
    app._deferred_validation_after_id = deferred_timer_id
    replacement_manager = ReplacementDataManager()

    def replace_closed_manager(_manager):
        app.data_manager = replacement_manager
        return True

    app._replace_closed_data_manager_after_close_failure = replace_closed_manager
    close_error = None
    try:
        try:
            app.on_closing()
        except RuntimeError as error:
            close_error = error

        event_types = [event for event, _details in durable_events]
        assert event_types == [
            label_module.Label_Match.Events.APP_CLOSE,
            "APP_CLOSE_CANCELLED",
        ]
        assert durable_events[0][1]["close_attempt_id"] == (
            durable_events[1][1]["close_attempt_id"]
        )
        assert flush_timeouts == [
            label_module.LABEL_MATCH_APP_CLOSE_LOG_TIMEOUT_SECONDS
        ]
        deferred_jobs = [
            job
            for job in root.jobs
            if job[0] not in root.cancelled
            and job[1] == 1000
            and getattr(job[2], "__name__", "") == "run_once"
        ]
        assert len(deferred_jobs) == 1
        assert app._deferred_validation_after_id == deferred_jobs[0][0]
        assert deferred_timer_id in root.cancelled
        assert "_app_close_resume_deferred_validation" not in app.__dict__
        assert app._app_close_in_progress is False
        assert str(close_error) == "forced close failure after APP_CLOSE"
    finally:
        app._app_close_in_progress = False
        deferred_after_id = app.__dict__.get("_deferred_validation_after_id")
        if deferred_after_id is not None:
            root.after_cancel(deferred_after_id)
            app._deferred_validation_after_id = None
        if app.ui_lane.state is not LaneState.CLOSED:
            _close_lane(app, root)


def test_close_cancel_rearms_deferred_validation_through_real_scheduler():
    app, root = _app_with_lane()

    class RecordingStateWidget:
        def __init__(self):
            self.transitions = []

        def configure(self, **kwargs):
            state = kwargs.get("state")
            if state is not None:
                caller = inspect.currentframe().f_back.f_code.co_name
                self.transitions.append(
                    (
                        state,
                        caller,
                        bool(
                            app.__dict__.get(
                                "_app_close_in_progress",
                                False,
                            )
                        ),
                    )
                )

        config = configure

    class CloseFailingDataManager:
        def __init__(self):
            self._close_requested = False
            self.log_thread = SimpleNamespace(is_alive=lambda: False)

        @staticmethod
        def log_event(_event, _details):
            return None

        def close(self, timeout=None):
            self._close_requested = True
            raise RuntimeError("forced close failure for deferred rearm")

    class ReplacementDataManager:
        @staticmethod
        def log_event(_event, _details):
            return None

        @staticmethod
        def flush(timeout=None):
            return True

    app.__dict__.pop("_render_operator_workbench")
    assert (
        app._render_operator_workbench.__func__
        is label_module.Label_Match._render_operator_workbench
    )
    assert "_schedule_deferred_validation_worker" not in app.__dict__
    app.initialized_successfully = True
    app.run_tests = True
    app.is_blinking = True
    app.operator_workbench_ready = True
    app.colors = {}
    app.entry = RecordingStateWidget()
    app.settings_button = RecordingStateWidget()
    app.data_manager = CloseFailingDataManager()
    app.after = root.after
    app.after_cancel = root.after_cancel
    deferred_timer_id = root.after(60000, lambda: None)
    app._deferred_validation_after_id = deferred_timer_id
    app._sealed_transfer_exchange_blocks_local_action = lambda _action: False
    app._has_background_work = lambda: False
    app._start_package_outbox_drain = lambda: None
    replacement_manager = ReplacementDataManager()

    def replace_closed_manager(_manager):
        app.data_manager = replacement_manager
        return True

    app._replace_closed_data_manager_after_close_failure = replace_closed_manager
    close_error = None
    try:
        try:
            app.on_closing()
        except RuntimeError as error:
            close_error = error

        deferred_jobs = [
            job
            for job in root.jobs
            if job[0] not in root.cancelled
            and job[1] == 1000
            and getattr(job[2], "__name__", "") == "run_once"
        ]
        assert len(deferred_jobs) == 1
        assert app._deferred_validation_after_id == deferred_jobs[0][0]
        assert deferred_timer_id in root.cancelled
        assert "_app_close_resume_deferred_validation" not in app.__dict__
        assert app._app_close_in_progress is False
        assert (
            "normal",
            "on_closing",
            True,
        ) in app.entry.transitions
        assert app.settings_button.transitions[-1] == (
            "normal",
            "on_closing",
            True,
        )
        assert app.entry.transitions[-1] == (
            "normal",
            "_render_operator_workbench",
            False,
        )
        assert app.__dict__.get("_last_workflow_view") is not None
        assert app.data_manager is replacement_manager
        assert str(close_error) == "forced close failure for deferred rearm"
    finally:
        app._app_close_in_progress = False
        deferred_after_id = app.__dict__.get("_deferred_validation_after_id")
        if deferred_after_id is not None:
            root.after_cancel(deferred_after_id)
            app._deferred_validation_after_id = None
        if app.ui_lane.state is not LaneState.CLOSED:
            _close_lane(app, root)


def test_close_cancel_rearm_rejection_relatches_and_preserves_resume_flag(
    monkeypatch,
):
    app, root = _app_with_lane()
    cleanup_delays = []

    class CloseFailingDataManager:
        def __init__(self):
            self._close_requested = False
            self.log_thread = SimpleNamespace(is_alive=lambda: False)

        @staticmethod
        def log_event(_event, _details):
            return None

        def close(self, timeout=None):
            self._close_requested = True
            raise RuntimeError("forced close failure before rearm rejection")

    class ReplacementDataManager:
        @staticmethod
        def log_event(_event, _details):
            return None

        @staticmethod
        def flush(timeout=None):
            return True

    app.initialized_successfully = True
    app.run_tests = False
    app.is_blinking = True
    app.entry = SimpleNamespace(configure=lambda **_kwargs: None)
    app.settings_button = SimpleNamespace(configure=lambda **_kwargs: None)
    app.data_manager = CloseFailingDataManager()
    app.after = root.after
    app.after_cancel = root.after_cancel
    deferred_timer_id = root.after(60000, lambda: None)
    app._deferred_validation_after_id = deferred_timer_id
    app._tk_shutdown_requested = True
    app._sealed_transfer_exchange_blocks_local_action = lambda _action: False
    app._has_background_work = lambda: False
    app._start_package_outbox_drain = lambda: None
    app._show_app_close_cleanup_delay = lambda: cleanup_delays.append(True)
    replacement_manager = ReplacementDataManager()

    def replace_closed_manager(_manager):
        app.data_manager = replacement_manager
        return True

    app._replace_closed_data_manager_after_close_failure = replace_closed_manager
    monkeypatch.setattr(
        label_module.messagebox,
        "askokcancel",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        label_module.messagebox,
        "showerror",
        lambda *_args, **_kwargs: None,
    )

    try:
        app.on_closing()

        deferred_jobs = [
            job
            for job in root.jobs
            if job[0] not in root.cancelled
            and getattr(job[2], "__name__", "") == "run_once"
        ]
        recovery_jobs = [
            job
            for job in root.jobs
            if job[0] not in root.cancelled
            and job[1] == 1000
            and getattr(job[2], "__name__", "") == "retry"
        ]
        assert deferred_jobs == []
        assert len(recovery_jobs) == 1
        assert app._app_close_recovery_after_id == recovery_jobs[0][0]
        assert deferred_timer_id in root.cancelled
        assert app._deferred_validation_after_id is None
        assert app._app_close_resume_deferred_validation is True
        assert app._app_close_in_progress is True
        assert cleanup_delays == [True]
    finally:
        app._app_close_in_progress = False
        recovery_after_id = app.__dict__.get("_app_close_recovery_after_id")
        if recovery_after_id is not None:
            root.after_cancel(recovery_after_id)
            app._app_close_recovery_after_id = None
        if app.ui_lane.state is not LaneState.CLOSED:
            _close_lane(app, root)


def test_close_cancel_restore_failure_stays_fail_closed_and_retries(monkeypatch):
    app, root = _app_with_lane()
    entry_states = []
    settings_states = []
    cleanup_delays = []

    class RestoreFailingEntry:
        def configure(self, *, state):
            entry_states.append(state)
            if state == "normal":
                raise RuntimeError("forced entry restore failure")

    class CloseFailingDataManager:
        def __init__(self):
            self._close_requested = False
            self.log_thread = SimpleNamespace(is_alive=lambda: False)

        @staticmethod
        def log_event(_event, _details):
            return None

        def close(self, timeout=None):
            self._close_requested = True
            raise RuntimeError("forced close failure")

    class ReplacementDataManager:
        @staticmethod
        def log_event(_event, _details):
            return None

        @staticmethod
        def flush(timeout=None):
            return True

    app.initialized_successfully = True
    app.run_tests = False
    app.is_blinking = True
    app.entry = RestoreFailingEntry()
    app.settings_button = SimpleNamespace(
        configure=lambda **kwargs: settings_states.append(kwargs["state"])
    )
    app.data_manager = CloseFailingDataManager()
    app._sealed_transfer_exchange_blocks_local_action = lambda _action: False
    app._has_background_work = lambda: False
    app._start_package_outbox_drain = lambda: None
    app._show_app_close_cleanup_delay = lambda: cleanup_delays.append(True)
    app.after = root.after
    app.after_cancel = root.after_cancel
    deferred_timer_id = root.after(60000, lambda: None)
    app._deferred_validation_after_id = deferred_timer_id
    replacement_manager = ReplacementDataManager()

    def replace_closed_manager(_manager):
        app.data_manager = replacement_manager
        return True

    app._replace_closed_data_manager_after_close_failure = replace_closed_manager
    monkeypatch.setattr(
        label_module.messagebox,
        "askokcancel",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        label_module.messagebox,
        "showerror",
        lambda *_args, **_kwargs: None,
    )

    try:
        app.on_closing()

        assert app._app_close_in_progress is True
        assert cleanup_delays == [True]
        recovery_jobs = [
            job
            for job in root.jobs
            if job[0] not in root.cancelled
            and job[1] == 1000
            and getattr(job[2], "__name__", "") == "retry"
        ]
        assert len(recovery_jobs) == 1
        assert app._app_close_recovery_after_id == recovery_jobs[0][0]
        assert deferred_timer_id in root.cancelled
        assert app._deferred_validation_after_id is None
        assert app._app_close_resume_deferred_validation is True
        assert entry_states == ["disabled", "normal"]
        assert settings_states == ["disabled"]
        assert app.ui_lane.state is LaneState.IDLE
    finally:
        app._app_close_in_progress = False
        recovery_after_id = app.__dict__.get("_app_close_recovery_after_id")
        if recovery_after_id is not None:
            root.after_cancel(recovery_after_id)
            app._app_close_recovery_after_id = None
        if app.ui_lane.state is not LaneState.CLOSED:
            _close_lane(app, root)


def test_close_retries_manager_recovery_before_rolling_back(monkeypatch):
    app, root = _app_with_lane()
    entry_states = []
    settings_states = []
    replacement_attempts = []

    class TemporarilyUnrecoverableManager:
        def __init__(self):
            self._close_requested = False

        @staticmethod
        def log_event(_event, _details):
            return None

        def close(self, timeout=None):
            assert timeout == label_module.LABEL_MATCH_APP_CLOSE_LOG_TIMEOUT_SECONDS
            self._close_requested = True
            raise RuntimeError("forced writer close failure")

    failed_manager = TemporarilyUnrecoverableManager()
    replacement_manager = SimpleNamespace(
        events=[],
        log_event=lambda event, details: replacement_manager.events.append(
            (event, details)
        ),
        flush=lambda timeout=None: True,
    )
    app.initialized_successfully = True
    app.run_tests = False
    app.is_blinking = True
    app.entry = SimpleNamespace(
        configure=lambda **kwargs: entry_states.append(kwargs["state"])
    )
    app.settings_button = SimpleNamespace(
        configure=lambda **kwargs: settings_states.append(kwargs["state"])
    )
    app.data_manager = failed_manager
    app.after = root.after
    app.after_cancel = root.after_cancel
    deferred_timer_id = root.after(60000, lambda: None)
    app._deferred_validation_after_id = deferred_timer_id
    app._has_background_work = lambda: False
    app._start_package_outbox_drain = lambda: None

    def replace_manager(_manager):
        replacement_attempts.append(True)
        if len(replacement_attempts) == 1:
            return False
        app.data_manager = replacement_manager
        return True

    app._replace_closed_data_manager_after_close_failure = replace_manager
    monkeypatch.setattr(
        label_module.messagebox,
        "askokcancel",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        label_module.messagebox,
        "showerror",
        lambda *_args, **_kwargs: None,
    )

    app.on_closing()

    assert app._app_close_in_progress is True
    assert app.ui_lane.state is LaneState.IDLE
    recovery_jobs = [
        job
        for job in root.jobs
        if job[0] not in root.cancelled
        and job[1] == 1000
        and getattr(job[2], "__name__", "") == "retry"
    ]
    assert len(recovery_jobs) == 1
    assert app._app_close_recovery_after_id == recovery_jobs[0][0]

    root.run_until(lambda: len(replacement_attempts) == 2)

    assert app._app_close_in_progress is False
    assert app.data_manager is replacement_manager
    assert app.ui_lane.state is LaneState.IDLE
    assert entry_states == ["disabled", "normal"]
    assert settings_states == ["disabled", "normal"]
    deferred_jobs = [
        job
        for job in root.jobs
        if job[0] not in root.cancelled
        and job[1] == 1000
        and getattr(job[2], "__name__", "") == "run_once"
    ]
    assert len(deferred_jobs) == 1
    assert app._deferred_validation_after_id == deferred_jobs[0][0]
    assert deferred_timer_id in root.cancelled
    assert "_app_close_resume_deferred_validation" not in app.__dict__
    root.after_cancel(app._deferred_validation_after_id)
    app._deferred_validation_after_id = None
    _close_lane(app, root)


def test_close_drain_deadline_breaks_lane_and_shows_cleanup_delay():
    app, root = _app_with_lane()
    active_gate = threading.Event()
    active_started = threading.Event()
    delays = []
    app._app_close_in_progress = True
    app._app_close_drain_watchdog_after_id = None
    app._show_app_close_cleanup_delay = lambda: delays.append(True)

    class InjectedClock:
        def __init__(self):
            self.now_ms = 0
            self.jobs = []
            self.next_id = 0

        def after(self, delay_ms, callback):
            self.next_id += 1
            job_id = f"clock-{self.next_id}"
            self.jobs.append((self.now_ms + int(delay_ms), job_id, callback))
            return job_id

        def advance(self, elapsed_ms):
            self.now_ms += int(elapsed_ms)
            due = [job for job in self.jobs if job[0] <= self.now_ms]
            self.jobs = [job for job in self.jobs if job[0] > self.now_ms]
            for _due_at, _job_id, callback in sorted(due):
                callback()

    def blocking_work():
        active_started.set()
        return active_gate.wait(timeout=2.0)

    clock = InjectedClock()
    app.after = clock.after
    app.ui_lane.submit(
        LaneTask(
            "overdue-close-drain",
            0,
            blocking_work,
            lambda _value: None,
            pytest.fail,
        )
    )
    active_started.wait(timeout=1.0)
    timeout_ms = int(
        label_module.LABEL_MATCH_APP_CLOSE_TOTAL_TIMEOUT_SECONDS * 1000
    )

    try:
        app._arm_app_close_lane_drain_watchdog(app.ui_lane)
        clock.advance(timeout_ms - 1)

        assert app.ui_lane.state is LaneState.BUSY

        clock.advance(1)

        assert app.ui_lane.state is LaneState.BROKEN
        assert delays == [True]
    finally:
        active_gate.set()
        app._app_close_in_progress = False
        if app.ui_lane.state is not LaneState.CLOSED:
            _close_lane(app, root)


def test_destroy_closes_domain_workers_before_lane_and_root(monkeypatch):
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    sequence = []
    lane_thread = SimpleNamespace(is_alive=lambda: True, name="lane-worker")
    lane = SimpleNamespace(
        state=LaneState.IDLE,
        is_busy=lambda: False,
        worker_thread=lane_thread,
    )

    def close_lane():
        sequence.append("close-lane")
        lane.state = LaneState.CLOSED

    lane.close_idle = close_lane
    app.ui_lane = lane
    app._cancel_app_close_lane_drain_watchdog = lambda: None
    app._stop_error_siren = lambda: None
    app._cancel_pending_ui_jobs = lambda: sequence.append("cancel-jobs")

    def join_domain_threads(*, exclude_threads=()):
        assert lane_thread in exclude_threads
        sequence.append("join-domain")
        return ()

    app._join_tk_shutdown_threads = join_domain_threads
    app._close_data_manager_before_tk_destroy = (
        lambda: sequence.append("close-data-manager") or True
    )
    monkeypatch.setattr(
        label_module.tk.Tk,
        "destroy",
        lambda _self: sequence.append("destroy-root"),
    )

    label_module.Label_Match.destroy(app)

    assert sequence == [
        "cancel-jobs",
        "join-domain",
        "close-data-manager",
        "close-lane",
        "destroy-root",
    ]
    assert app._tk_destroy_complete is True


def test_pending_job_cleanup_preserves_lane_pump_until_final_close():
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    cancelled = []
    app.ui_lane = SimpleNamespace(_after_id="lane-pump")
    app.tk = SimpleNamespace(
        call=lambda *_args: ("lane-pump", "ordinary-app-job"),
        splitlist=lambda value: tuple(value),
    )
    app.after_cancel = lambda after_id: cancelled.append(str(after_id))

    app._cancel_pending_ui_jobs()

    assert cancelled == ["ordinary-app-job"]


def test_f5_popup_busy_rejection_preserves_popup_and_raw(monkeypatch):
    app, root = _app_with_lane()
    gate = threading.Event()
    popup = Mock()
    scan_entry = Mock()
    scan_entry.get.return_value = "PHS2-F5-PRESERVE"
    monkeypatch.setattr(
        label_module.tk,
        "Toplevel",
        lambda _parent: popup,
    )
    monkeypatch.setattr(
        label_module.tk,
        "StringVar",
        lambda **_kwargs: Mock(),
    )
    monkeypatch.setattr(
        label_module.ttk,
        "Frame",
        lambda *_args, **_kwargs: Mock(),
    )
    monkeypatch.setattr(
        label_module.ttk,
        "Label",
        lambda *_args, **_kwargs: Mock(),
    )
    monkeypatch.setattr(
        label_module.ttk,
        "Entry",
        lambda *_args, **_kwargs: scan_entry,
    )

    app.default_font_name = "Test Font"
    app.colors = {}
    app._phs_reconciliation_lookup_pending = False
    app._phs_reconciliation_scope = lambda: "PACKAGING"
    app.phs_label_exchange_coordinator = SimpleNamespace(
        resolve_reconciliation_actions=lambda **_kwargs: {"actions": []}
    )
    assert app.ui_lane.submit(
        LaneTask(
            "blocking-deferred-validation",
            0,
            lambda: gate.wait(timeout=2.0),
            lambda _value: None,
            pytest.fail,
        )
    ).accepted

    popup_after_rejection = None
    raw_after_rejection = None
    try:
        assert app._show_phs_reconciliation_scan_window() is True
        submit = next(
            call.args[1]
            for call in scan_entry.bind.call_args_list
            if call.args[0] == "<Return>"
        )
        submit()
        popup_after_rejection = app._phs_reconciliation_scan_window
        raw_after_rejection = scan_entry.get()
    finally:
        gate.set()
        root.run_until(lambda: not app.ui_lane.is_busy())
        _close_lane(app, root)

    popup.destroy.assert_not_called()
    popup.grab_release.assert_not_called()
    assert popup_after_rejection is popup
    assert raw_after_rejection == "PHS2-F5-PRESERVE"


@pytest.mark.parametrize(
    "method_name",
    (
        "_begin_phs_reconciliation_lookup",
        "_start_phs_reconciliation_exchange",
        "_begin_phs_label_candidate_lookup",
        "_start_phs_label_exchange",
    ),
)
def test_f5_slow_paths_have_no_per_operation_thread(method_name):
    source = inspect.getsource(getattr(label_module.Label_Match, method_name))

    assert "threading.Thread" not in source
    assert "_submit_ui_lane_task" in source


def test_all_four_f5_slow_paths_share_the_tracked_lane_worker():
    app, root = _app_with_lane()
    owner = root.owner_thread_id
    trace = []
    app.after = root.after
    app.run_tests = True
    app.worker_name = "operator"
    app._phs_label_guidance_notice = None
    app._phs_reconciliation_lookup_pending = False
    app._phs_label_candidate_pending = False
    app._phs_label_exchange_pending = False
    app._phs_reconciliation_scope = lambda: "PACKAGING"
    app._show_phs_replacement_required_notice_once = (
        lambda value: trace.append(("resolve-apply", threading.get_ident(), value))
    )
    app._show_phs_reconciliation_action_window = lambda value, **_kwargs: (
        trace.append(("resolve-window", threading.get_ident(), value))
    )
    app._show_phs_label_candidate_window = lambda value: trace.append(
        ("candidate-window", threading.get_ident(), value)
    )
    app._focus_scan_entry_if_available = lambda: trace.append(
        ("focus", threading.get_ident())
    )

    result = SimpleNamespace(
        success=True,
        status="ACKED",
        exchange_id="exchange-1",
        error_code="",
    )

    class Coordinator:
        @staticmethod
        def resolve_reconciliation_actions(**_kwargs):
            trace.append(("resolve-work", threading.get_ident()))
            return {"actions": []}

        @staticmethod
        def execute_reconciliation(
            _resolution,
            *,
            current_set,
            persist_current_set,
            **_kwargs,
        ):
            trace.append(("reconcile-work", threading.get_ident()))
            assert persist_current_set() is True
            current_set["active_label_qr_payload"] = "PHS=2|CLC=ITEM"
            return result

        @staticmethod
        def list_candidates(_current, _business_date):
            trace.append(("candidate-work", threading.get_ident()))
            return [{"instruction_id": "instruction-1"}]

        @staticmethod
        def execute_single(
            current_set,
            _target,
            *,
            persist_current_set,
            **_kwargs,
        ):
            trace.append(("single-work", threading.get_ident()))
            assert persist_current_set() is True
            current_set["active_label_qr_payload"] = "PHS=2|CLC=ITEM"
            return result

    class DataManager:
        @staticmethod
        def save_current_state(_state):
            trace.append(("persist", threading.get_ident()))
            return True

        @staticmethod
        def log_event(_event, _details):
            trace.append(("log", threading.get_ident()))

    app.phs_label_exchange_coordinator = Coordinator()
    app.data_manager = DataManager()

    assert app._begin_phs_reconciliation_lookup("PHS2-SCAN") is True
    root.run_until(
        lambda: not app._phs_reconciliation_lookup_pending
        and any(row[0] == "resolve-window" for row in trace)
    )
    assert app._start_phs_reconciliation_exchange({"actions": []}) is True
    root.run_until(
        lambda: not app._phs_label_exchange_pending
        and any(row[0] == "reconcile-work" for row in trace)
    )
    assert app._begin_phs_label_candidate_lookup("2026-09-02") is True
    root.run_until(
        lambda: not app._phs_label_candidate_pending
        and any(row[0] == "candidate-window" for row in trace)
    )
    assert app._start_phs_label_exchange(
        {"instruction_id": "instruction-1"}
    ) is True
    root.run_until(
        lambda: not app._phs_label_exchange_pending
        and any(row[0] == "single-work" for row in trace)
    )

    worker_id = app.ui_lane.worker_thread_id
    assert {
        row[0]
        for row in trace
        if len(row) > 1 and row[1] == worker_id
    } >= {
        "resolve-work",
        "reconcile-work",
        "candidate-work",
        "single-work",
        "persist",
    }
    for row in trace:
        if row[0] in {"resolve-apply", "resolve-window", "candidate-window"}:
            assert row[1] == owner
    assert app.ui_lane.worker_thread in app._tracked_tk_shutdown_threads()
    _close_lane(app, root)

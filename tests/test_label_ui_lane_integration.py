from __future__ import annotations

import inspect
from types import SimpleNamespace
import threading
import time

import pytest

import Label_Match as label_module
from deferred_intent_capture import (
    DeferredValidationClaim,
    DeferredValidationResult,
)
from tests.test_tk_serial_ui_lane import FakeTkRoot
from tk_serial_ui_lane import CoalescingTrigger, LaneTask, TkSerialUiLane


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

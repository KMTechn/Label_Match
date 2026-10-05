"""P1-1/P1-5: one refused read-only source check stops only its own label.

A 409 from the read-only source step is cancellable (BLOCKED_INVALID); a
command-less review capture closes with F1 or the transition fallback; a
capture that cannot close leaves the set; other labels keep validating.
"""

import json
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import deferred_intent_capture as capture_module
import terminal_operation_lease as lease_module
from deferred_intent_capture import DeferredIntentCaptureError
from package_logistics import PackageApiError, PackageLogisticsError
from tests.test_deferred_intent_capture import (
    _capture, _claim_and_plan, _draft, _row, _source_evidence, _store,
)
from tests.test_deferred_lease_clock_retry import _first_scan, _queue_f3, clock_case  # noqa: F401
from tests.test_label_capture_cancellation import _app, _cancel, _validated
from tests.test_legacy_label_transition import (
    MASTER, PHS2_LABEL, _app_for_recovery, _close, _events, _legacy_set, _packaging_app, _scan,
    _transition, load_label_match_module,
)

NOW = "2026-08-29T01:00:00Z"
OTHER_PHS2 = PHS2_LABEL.replace("ITG-OFF-1", "ITG-OFF-2").replace("LBL-OFF-1", "LBL-OFF-2")
WAITING_EVIDENCE = {
    "contract_version": "label-validation-evidence-v1",
    "dependency": {
        "kind": "OPERATION_GRANT",
        "identity": "CREATE_PACKAGE@SCOPE-LABEL-MEASURED",
        "authority_scope_id": "SCOPE-LABEL-MEASURED",
        "operation": "CREATE_PACKAGE",
        "status": "PENDING",
    },
    "http_status": 409,
    "committed": False,
    "error_code": "OPERATION_LEASE_ALREADY_LEASED",
    "observed_at": "2026-08-29T01:00:02Z",
}


def _source_finish(store, captured, outcome, *, reason="PHS_WORK_GROUP_TRANSFER_NOT_READY"):
    claim = store.claim_validation(captured.intent_id, worker_id="p1fix", now=NOW)
    checked = store.verify_local_integrity(claim, now=NOW)
    store.plan_label_validation(checked, now=NOW)
    evidence = ({"transport": "UNAVAILABLE", "observed_at": "2026-08-29T01:00:02Z"}
                if outcome == "RETRYABLE_UNAVAILABLE" else
                {"step_id": "label-package-source", "http_status": 409, "error_code": reason,
                 "observed_at": "2026-08-29T01:00:02Z"})
    return store.finish_validation(checked, step_id="label-package-source", outcome=outcome,
                                   reason_code=reason, evidence=evidence,
                                   now="2026-08-29T01:00:02Z")


def _lease_step_finish(store, captured, outcome, *, set_id, scan):
    """The retained two-step plan: the mutating lease step answered."""
    claim = _claim_and_plan(store, captured.intent_id)
    store.record_validation_step_valid(claim, step_id="label-package-source",
                                       evidence=_source_evidence(set_id=set_id, scan=scan),
                                       now="2026-08-29T01:00:01Z")
    store.record_validation_mutation_attempt(claim, step_id="label-operation-lease",
                                             now="2026-08-29T01:00:01Z")
    evidence = WAITING_EVIDENCE if outcome == "REQUIRED_ABSENT" else {
        "step_id": "label-operation-lease", "http_status": 409,
        "error_code": "OPERATION_LEASE_CONFLICT", "observed_at": "2026-08-29T01:00:02Z"}
    return store.finish_validation(claim, step_id="label-operation-lease", outcome=outcome,
                                   reason_code=str(evidence["error_code"]), evidence=evidence,
                                   now="2026-08-29T01:00:02Z")


def _head(store, state, *, set_id, scan):
    captured = _capture(store, set_id=set_id, scan=scan)
    if state == "OPERATOR_REVIEW":
        result = _source_finish(store, captured, "CONFLICT")
    elif state == "BLOCKED_INVALID":
        result = _source_finish(store, captured, "INVALID")
    elif state == "RETRY_WAIT_VALIDATION":
        result = _source_finish(store, captured, "RETRYABLE_UNAVAILABLE", reason="PACKAGE_TRANSPORT_UNAVAILABLE")
    else:
        result = _lease_step_finish(store, captured, "REQUIRED_ABSENT", set_id=set_id, scan=scan)
    assert result.state == state
    return captured


# --- store: cancellable review, preservation, per-label order -------------------------------


@pytest.mark.parametrize("state", ["OPERATOR_REVIEW", "BLOCKED_INVALID", "WAITING_DEPENDENCY",
                                   "RETRY_WAIT_VALIDATION"])
def test_later_phs2_validation_not_blocked_by_earlier_review_capture(tmp_path, state):
    """Any unfinished head, including ALREADY_LEASED -> WAITING_DEPENDENCY,
    stops only its own label; the same label still keeps its order."""
    _db, _outbox, store = _store(tmp_path)
    first = _head(store, state, set_id="SET-A", scan="PHS2-LABEL-A")
    other = _capture(store, set_id="SET-B", scan="PHS2-LABEL-B")
    same = _capture(store, set_id="SET-C", scan="PHS2-LABEL-A")
    assert store.next_validation_candidate(now="2099-01-01T00:00:00Z") in {other.intent_id, first.intent_id}
    claim = store.claim_validation(other.intent_id, worker_id="p1fix-other", now=NOW)
    assert claim is not None and claim.intent_id == other.intent_id
    assert store.claim_validation(same.intent_id, worker_id="p1fix-same", now=NOW) is None
    assert _row(store.db_path, same.intent_id)["state"] == "CAPTURED_UNVERIFIED"
    assert _row(store.db_path, first.intent_id)["state"] == state


@pytest.mark.parametrize("state", ["OPERATOR_REVIEW", "BLOCKED_INVALID"])
def test_f1_store_cancels_review_capture_without_command(tmp_path, state):
    db_path, _outbox, store = _store(tmp_path)
    captured = _head(store, state, set_id="SET-R", scan="PHS2-LABEL-R")
    before = _row(db_path, captured.intent_id)
    assert _cancel(store, captured, scan="PHS2-LABEL-R")["state"] == "CANCELLED"
    after = _row(db_path, captured.intent_id)
    assert after["last_reason_code"] == "OPERATOR_CANCELLED_LOCAL_CAPTURE"
    for field in ("payload_ciphertext", "authenticated_seal", "payload_hash"):
        assert after[field] == before[field]
    with closing(sqlite3.connect(db_path)) as conn:
        steps = conn.execute("SELECT status,validation_outcome FROM deferred_intent_validation_steps WHERE intent_id=?",
                             (captured.intent_id,)).fetchall()
    assert steps and all(step[0] in {"OPERATOR_REVIEW", "BLOCKED_INVALID"} for step in steps)


@pytest.mark.parametrize("sent", ["outbox", "submit-attempt", "mutation-step"])
def test_review_capture_with_command_or_outbox_is_not_cancelled(tmp_path, sent):
    db_path, outbox, store = _store(tmp_path)
    if sent == "mutation-step":
        captured = _capture(store, set_id="SET-SENT", scan="PHS2-MEASURED")
        assert _lease_step_finish(store, captured, "CONFLICT", set_id="SET-SENT",
                                  scan="PHS2-MEASURED").state == "OPERATOR_REVIEW"
    else:
        captured = _head(store, "OPERATOR_REVIEW", set_id="SET-SENT", scan="PHS2-MEASURED")
    if sent == "outbox":
        outbox.enqueue(_draft("SET-SENT"))
    elif sent == "submit-attempt":
        with closing(sqlite3.connect(db_path)) as conn, conn:
            conn.execute("UPDATE deferred_intents SET submit_attempt_count=1 WHERE intent_id=?",
                         (captured.intent_id,))
    before = _row(db_path, captured.intent_id)
    with pytest.raises(DeferredIntentCaptureError):
        _cancel(store, captured, scan="PHS2-MEASURED")
    assert _row(db_path, captured.intent_id) == before


def test_existing_database_gains_the_review_cancel_edge_on_initialize(tmp_path):
    """A 92abdaa database keeps OPERATOR_REVIEW -> SUPERSEDED only until the
    ordinary initialize rewrites the edge guard."""
    db_path, _outbox, store = _store(tmp_path)
    captured = _head(store, "OPERATOR_REVIEW", set_id="SET-OLD-DB", scan="PHS2-MEASURED")
    with closing(sqlite3.connect(db_path)) as conn, conn:
        current = conn.execute("SELECT sql FROM sqlite_master WHERE type='trigger' "
                               "AND name='trg_deferred_intent_state_edge_guard'").fetchone()[0]
        old = current.replace("(OLD.state='OPERATOR_REVIEW' AND NEW.state IN ('SUPERSEDED','CANCELLED'))",
                              "(OLD.state='OPERATOR_REVIEW' AND NEW.state='SUPERSEDED')")
        assert old != current  # the 92abdaa line, exactly
        conn.execute("DROP TRIGGER trg_deferred_intent_state_edge_guard")
        conn.execute(old)
    with pytest.raises(DeferredIntentCaptureError):
        _cancel(store, captured)
    assert _row(db_path, captured.intent_id)["state"] == "OPERATOR_REVIEW"
    store.initialize()
    assert _cancel(store, captured)["state"] == "CANCELLED"


# --- app: 409 classification, F1 on the capture-only set, detaching ------------------------


def _conflict(case):
    case.app.package_logistics_client.resolve_package_source_evidence = Mock(
        side_effect=PackageApiError(409, "PHS_WORK_GROUP_TRANSFER_NOT_READY", "not ready", committed=False)
    )
    intent_id = _first_scan(case)
    return intent_id, case.app.deferred_intent_capture.validation_status(intent_id)


def _old_review(case):
    """A capture a 92abdaa PC already moved to OPERATOR_REVIEW."""
    case.app.package_logistics_client.resolve_package_source_evidence = Mock(
        side_effect=PackageApiError(503, "UNAVAILABLE", "offline", committed=False)
    )
    intent_id = _first_scan(case)
    store = case.app.deferred_intent_capture
    claim = store.claim_validation(intent_id, worker_id="p1fix-old", now="2099-01-01T00:00:00Z")
    checked = store.verify_local_integrity(claim, now="2099-01-01T00:00:00Z")
    store.plan_label_validation(checked, now="2099-01-01T00:00:00Z")
    result = store.finish_validation(
        checked, step_id="label-package-source", outcome="CONFLICT",
        reason_code="PHS_WORK_GROUP_TRANSFER_NOT_READY",
        evidence={"step_id": "label-package-source", "http_status": 409,
                  "error_code": "PHS_WORK_GROUP_TRANSFER_NOT_READY", "observed_at": "2099-01-01T00:00:01Z"},
        now="2099-01-01T00:00:01Z",
    )
    assert result.state == "OPERATOR_REVIEW"
    return intent_id, result


def _transition_ui(app, monkeypatch):
    app.tk = SimpleNamespace()
    app.history_view_updates_active_state = True
    app.data_manager = SimpleNamespace(log_event=lambda *event: None)
    app.run_tests = False
    app.is_running_simulation = False
    app._legacy_label_transition_enabled = True
    app.worker_name = "worker-lt"
    for name in ("update_big_display", "_update_status_label", "_update_history_tree_in_progress",
                 "_render_operator_workbench", "_play_sound", "_clear_workflow_completion"):
        setattr(app, name, lambda *a, **k: None)
    app._save_current_set_state = lambda: True
    module = load_label_match_module()
    monkeypatch.setattr(module.messagebox, "showwarning", lambda *a, **k: None)


@pytest.mark.parametrize("origin", ["source-409", "old-operator-review"])
def test_transition_accepts_phs2_whose_source_check_returned_409(clock_case, monkeypatch, origin):
    case = clock_case
    intent_id, outcome = (_conflict if origin == "source-409" else _old_review)(case)
    assert outcome.state == ("BLOCKED_INVALID" if origin == "source-409" else "OPERATOR_REVIEW")
    app = case.app
    assert app.current_set_info["raw"] == []
    app.items_data = {case.group["item_id"]: {"Item Name": "품목", "Spec": ""}}
    from tests.test_label_match_core import _FakeProgressBar
    app.progress_bar = _FakeProgressBar()
    _transition_ui(app, monkeypatch)
    assert app._transition_accept_blocked_phs2(outcome, case.group["scan_payload"], case.group["item_id"]) is True
    assert _row(case.database, intent_id)["state"] == "CANCELLED"
    assert app.current_set_info["raw"] == [case.group["scan_payload"]]
    assert "deferred_intent_id" not in app.current_set_info
    assert _transition(app.current_set_info)["transition_class"] == "PHS2_LOCAL"


@pytest.mark.parametrize("origin", ["source-409", "old-operator-review"])
def test_f1_cancels_review_capture_without_command(clock_case, origin):
    """The capture-only set (no scan rows yet) proves its own label."""
    case = clock_case
    intent_id, _outcome = (_conflict if origin == "source-409" else _old_review)(case)
    shell = dict(case.app.current_set_info)
    assert shell["raw"] == [] and shell["deferred_intent_id"] == intent_id
    app = _app(case.app.deferred_intent_capture, SimpleNamespace(intent_id=intent_id),
               scan=case.group["scan_payload"])
    app.package_logistics_client = case.app.package_logistics_client
    app.package_outbox = case.app.package_outbox
    app.current_set_info = shell
    assert app._reset_current_set(full_reset=True) is True
    assert _row(case.database, intent_id)["state"] == "CANCELLED"
    assert not app.current_set_info["id"] and not app.current_set_info.get("deferred_intent_id")


def _registered_app(module, tmp_path, monkeypatch, *, transition, outcome_for):
    app, _syncs = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=transition)
    _db, _outbox, store = _store(tmp_path)
    app.deferred_intent_capture = store
    app.package_logistics_client.config = SimpleNamespace(
        tls_ca_bundle_path="", authority_scope_id=store.binding.authority_scope_id,
        device_id="PC-LT", source_host_id=store.binding.source_host_id,
    )

    def prepare(capture):
        row = _row(store.db_path, capture.intent_id)
        if row["state"] != "CAPTURED_UNVERIFIED":
            return store.validation_status(capture.intent_id)
        outcome = outcome_for(row)
        return _source_finish(store, SimpleNamespace(intent_id=capture.intent_id), outcome,
                              reason=("PACKAGE_TRANSPORT_UNAVAILABLE" if outcome == "RETRYABLE_UNAVAILABLE"
                                      else "PHS_WORK_GROUP_TRANSFER_NOT_READY"))

    app._prepare_deferred_label_validation = prepare
    return app, store


def test_next_legacy_set_after_blocked_capture_completes(tmp_path, monkeypatch):
    """A PHS2 capture that cannot close stays for review, but the next
    old-label set is its own and completes."""
    module = load_label_match_module()
    app, store = _registered_app(module, tmp_path, monkeypatch, transition=True,
                                 outcome_for=lambda _row: "INVALID")
    closes = []

    def cannot_close(current, **_kwargs):
        closes.append(current.get("deferred_intent_id"))
        raise DeferredIntentCaptureError("CANCEL_CAPTURE_REVIEW_REQUIRED", "not safely cancellable")

    app._cancel_deferred_capture_for_set = cannot_close
    try:
        _scan(module, app, PHS2_LABEL)
        [intent_id] = [row[0] for row in sqlite3.connect(store.db_path).execute(
            "SELECT intent_id FROM deferred_intents")]
        blocked = _row(store.db_path, intent_id)
        assert blocked["state"] == "BLOCKED_INVALID" and closes == [intent_id]
        assert not app.current_set_info.get("deferred_intent_id")
        assert app.current_set_info.get("id") != blocked["local_work_identity"]
        for value in _legacy_set(31):
            _scan(module, app, value)
    finally:
        _close(app)
    [(_event, details)] = _events(tmp_path, "TRAY_COMPLETE")
    assert _transition(details)["transition_class"] == "LEGACY"
    assert details["scanned_product_barcodes"] == _legacy_set(31)
    assert app.blocks == []
    assert closes == [intent_id]  # the old-label set never tried to close it
    assert _row(store.db_path, intent_id)["state"] == "BLOCKED_INVALID"  # preserved for review


def test_other_label_detaches_a_waiting_capture_and_the_same_label_resumes_it(tmp_path, monkeypatch):
    module = load_label_match_module()
    app, store = _registered_app(module, tmp_path, monkeypatch, transition=False,
                                 outcome_for=lambda _row: "RETRYABLE_UNAVAILABLE")

    def rows():
        with closing(sqlite3.connect(store.db_path)) as conn:
            return conn.execute("SELECT intent_id,local_work_identity,state,partition_key "
                                "FROM deferred_intents ORDER BY created_at,intent_id").fetchall()

    try:
        _scan(module, app, PHS2_LABEL)
        [(first_id, first_set, state, first_partition)] = rows()
        assert state == "RETRY_WAIT_VALIDATION" and app.current_set_info["deferred_intent_id"] == first_id
        _scan(module, app, OTHER_PHS2)
        assert len(rows()) == 2
        second = next(row for row in rows() if row[0] != first_id)
        assert second[1] != first_set and second[3] != first_partition
        assert app.current_set_info["deferred_intent_id"] == second[0]
        assert _row(store.db_path, first_id)["state"] == "RETRY_WAIT_VALIDATION"  # kept, not cancelled
        _scan(module, app, PHS2_LABEL)  # the same label again: continue, never a second capture
        assert len(rows()) == 2
        assert app.current_set_info["deferred_intent_id"] == first_id
        assert app.current_set_info["id"] == first_set
        notice = app._deferred_capture_pending_notice()
        assert "다시 스캔하면 이어" in notice.message
    finally:
        _close(app)


@pytest.mark.parametrize("spelling", ["same", "variant"])
@pytest.mark.parametrize(("head", "after"), [
    ("CAPTURED_UNVERIFIED", "RETRY_WAIT_VALIDATION"),
    ("RETRY_WAIT_VALIDATION", "RETRY_WAIT_VALIDATION"),
    ("BLOCKED_INVALID", "BLOCKED_INVALID"),
])
def test_rescanning_the_label_on_screen_continues_its_own_capture(tmp_path, monkeypatch, head, after, spelling):
    """The label scanned again over its own capture-only set continues that
    capture (review R2-1/R2-2): no second capture, no payload mismatch, no
    repeated '저장 실패', and F1 still closes it with the label as captured."""
    module = load_label_match_module()
    app, store = _registered_app(module, tmp_path, monkeypatch, transition=False,
                                 outcome_for=lambda _row: "INVALID" if head == "BLOCKED_INVALID"
                                 else "RETRYABLE_UNAVAILABLE")
    validate = app._prepare_deferred_label_validation
    if head == "CAPTURED_UNVERIFIED":  # the first check has not run yet
        app._prepare_deferred_label_validation = lambda capture: store.validation_status(capture.intent_id)
    again = PHS2_LABEL.replace("|", " | ").replace("PHS=", "phs=") if spelling == "variant" else PHS2_LABEL
    try:
        _scan(module, app, PHS2_LABEL)
        with closing(sqlite3.connect(store.db_path)) as conn:
            [(intent_id, set_id)] = conn.execute("SELECT intent_id,local_work_identity FROM deferred_intents").fetchall()
        assert _row(store.db_path, intent_id)["state"] == head
        app._prepare_deferred_label_validation = validate
        _scan(module, app, again)
        with closing(sqlite3.connect(store.db_path)) as conn:
            assert conn.execute("SELECT intent_id FROM deferred_intents").fetchall() == [(intent_id,)]
        assert _row(store.db_path, intent_id)["state"] == after
        assert _row(store.db_path, intent_id)["last_reason_code"] != "DUPLICATE_IDENTITY_PAYLOAD_MISMATCH"
        assert app.current_set_info["id"] == set_id and app.current_set_info["deferred_intent_id"] == intent_id
        assert app.current_set_info["physical_scanned_qr_payload"] == PHS2_LABEL
        assert "저장 실패" not in app._deferred_capture_ui["status"], app._deferred_capture_ui
        message = app._deferred_capture_pending_notice().message
        assert ("F1" if head == "BLOCKED_INVALID" else "다시 스캔하면 이어") in message, message
        app._cancel_deferred_capture_for_set(app.current_set_info)
        assert _row(store.db_path, intent_id)["state"] == "CANCELLED"
    finally:
        _close(app)


# --- floating validated capture, recovery list, texts ---------------------------------------


def test_validated_floating_capture_does_not_break_lane_while_another_set_runs(tmp_path):
    _db, _outbox, store = _store(tmp_path)
    captured = _validated(store, set_id="SET-FLOAT", scan="PHS2-FLOAT")
    app = _app(store, captured, scan="PHS2-FLOAT")
    other = {"id": "SET-OTHER", "raw": ["OTHER-LABEL", "P-1"], "parsed": ["OTHER"]}
    app.current_set_info = dict(other)
    app._deferred_label_materialization = {"intent_id": captured.intent_id}
    try:
        materialized = app._materialize_validated_deferred_label(
            SimpleNamespace(state="VALIDATED", intent_id=captured.intent_id))
    except PackageLogisticsError as error:  # the lane task would break here
        pytest.fail(f"materialization broke the lane: {error}")
    assert materialized is False
    assert app.current_set_info == other
    assert _row(store.db_path, captured.intent_id)["state"] == "VALIDATED"


def _recovery_app(store, current):
    from package_logistics import PackageOutbox
    app = _app(store, SimpleNamespace(intent_id=current.get("deferred_intent_id") or ""))
    app.current_set_info = current
    app.package_outbox = PackageOutbox(store.db_path)
    app.sealed_transfer_exchange_store = SimpleNamespace(blocking_rows=lambda set_id=None: [])
    app._active_label_recovery_state = lambda: {}
    app._package_current_state_path = lambda: ""
    return app


@pytest.mark.parametrize("state", ["OPERATOR_REVIEW", "BLOCKED_INVALID"])
def test_review_capture_set_is_listed_for_hold_and_continue(tmp_path, state):
    _db, _outbox, store = _store(tmp_path)
    captured = _head(store, state, set_id="SET-REVIEW", scan="PHS2-REVIEW")
    floating = _head(store, state, set_id="SET-FLOATING", scan="PHS2-FLOATING")
    app = _recovery_app(store, {"id": "SET-REVIEW", "raw": [], "deferred_intent_id": captured.intent_id,
                                "physical_scanned_qr_payload": "PHS2-REVIEW"})
    candidates = {row["set_id"]: row for row in app._package_recovery_candidates()}
    assert "SET-REVIEW" in candidates and candidates["SET-REVIEW"]["current"]["deferred_intent_id"] == captured.intent_id
    assert candidates["SET-FLOATING"]["capture"]["intent_id"] == floating.intent_id  # no silent row
    assert candidates["SET-FLOATING"]["capture"]["state"] == state


@pytest.mark.parametrize(("status", "state", "own", "says", "never"), [
    ("관리자확인", "BLOCKED_INVALID", True, ("F1", "CA 봉인 뒤", "같은 현품표를 다시 스캔", "건별 복구"), ()),
    ("관리자확인", "OPERATOR_REVIEW", True, ("건별 복구", "다음 현품표"), ("F1",)),
    ("관리자확인", "BLOCKED_INVALID", False, ("건별 복구", "다음 현품표"), ("F1",)),
    ("저장됨-검증대기", "RETRY_WAIT_VALIDATION", True, ("다시 스캔하면 이어",), ()),
    ("저장됨-검증대기", "RETRY_WAIT_VALIDATION", False, (), ("이어",)),
])
def test_review_notice_names_the_next_step(status, state, own, says, never):
    """F1 is offered only where it closes the capture; resuming by rescan
    only for the set's own capture."""
    module = load_label_match_module()
    app = module.Label_Match.__new__(module.Label_Match)
    app.initialized_successfully = True
    app.current_set_info = {"id": "SET-N", "raw": [], "deferred_intent_id": "di_own"}
    app._deferred_capture_ui = {"status": status, "state": state,
                                "intent_id": "di_own" if own else "di_other"}
    message = app._deferred_capture_pending_notice().message
    assert all(text in message for text in says), message
    assert not any(text in message for text in never), message


@pytest.mark.parametrize("action", ["key", "button"])
def test_refused_owned_capture_f1_cancels_then_same_label_validates(clock_case, action):
    from tests.test_label_operator_action_gates import _render_app

    case = clock_case
    app = case.app
    intent_id, _outcome = _conflict(case)
    before = _row(case.database, intent_id)
    rendered = _render_app()
    app.__dict__.update({key: value for key, value in rendered.__dict__.items()
                         if key != "current_set_info"})
    app.is_blinking = False
    app.worker_name = "F1 test operator"
    app.history_tree = SimpleNamespace(exists=lambda _key: False)
    app.data_manager = SimpleNamespace(log_event=lambda *args: None, delete_current_state=lambda: None)
    app._update_status_label = lambda: None
    app.update_big_display = lambda *args: None
    view = app._render_operator_workbench()
    assert "F1" in app._deferred_capture_pending_notice().message
    assert view.cancel_current_enabled is False  # capture-only presenter stays unchanged
    assert app.reset_button.options["state"] == "normal"
    if action == "key":
        assert app._handle_workflow_shortcut("f1") == "break"
    else:
        # The real button command is the same guarded reset, once enabled.
        assert app._reset_current_set(full_reset=True) is True
    cancelled = _row(case.database, intent_id)
    assert cancelled["state"] == "CANCELLED"
    assert cancelled["payload_ciphertext"] == before["payload_ciphertext"]
    with sqlite3.connect(case.database) as conn:
        assert conn.execute("SELECT transition_code,reason_code FROM deferred_intent_transition_audit "
                            "WHERE intent_id=? AND transition_code='TC_CANCEL'", (intent_id,)).fetchall() == [
                                ("TC_CANCEL", "OPERATOR_CANCELLED_LOCAL_CAPTURE")]
    assert not app.current_set_info["id"] and app.current_set_info["raw"] == []
    assert not app.current_set_info["deferred_intent_id"]
    assert app.progress_bar.options["value"] == 0
    assert app._last_workflow_view.qa_completed == 0
    assert all(not slot.filled and not slot.value for slot in app._last_workflow_view.slots)
    assert app._deferred_capture_pending_notice() is None
    app.package_logistics_client.resolve_package_source_evidence = lambda _draft: case.response
    new_id = _first_scan(case)
    assert new_id != intent_id
    assert _row(case.database, new_id)["state"] == "VALIDATED"
    assert _row(case.database, intent_id)["state"] == "CANCELLED"
    assert app.current_set_info["raw"] == [case.group["scan_payload"]]
    assert app.current_set_info["central_inherit_all"] is True
    assert len(case.accepted) == 1


@pytest.mark.parametrize("boundary", ["audit-failure", "other-notice"])
def test_refused_capture_reset_preserves_unclosed_or_unrelated_notice(clock_case, boundary):
    case = clock_case
    intent_id, _outcome = _conflict(case)
    current = dict(case.app.current_set_info)
    before = _row(case.database, intent_id)
    app = _app(case.app.deferred_intent_capture, SimpleNamespace(intent_id=intent_id),
               scan=case.group["scan_payload"])
    app.current_set_info = current
    displays = []
    app.update_big_display = lambda *args: displays.append(args)
    app._render_operator_workbench = lambda: None
    notice = {"status": "관리자확인", "state": "BLOCKED_INVALID",
              "intent_id": intent_id if boundary == "audit-failure" else "di_other"}
    app._deferred_capture_ui = dict(notice)
    if boundary == "audit-failure":
        with sqlite3.connect(case.database) as conn:
            conn.execute("CREATE TRIGGER refuse_f1_cancel BEFORE INSERT ON deferred_intent_transition_audit "
                         "WHEN NEW.transition_code='TC_CANCEL' BEGIN SELECT RAISE(ABORT,'test cancel failure'); END")
        assert app._reset_current_set(full_reset=True) is False
        assert _row(case.database, intent_id) == before
        assert app.current_set_info == current
        assert app.events == []
        assert displays and app._phs_label_guidance_notice.kind == 'scanner_notice'
    else:
        assert app._reset_current_set(full_reset=True) is True
        assert _row(case.database, intent_id)["state"] == "CANCELLED"
    assert app._deferred_capture_ui == notice


@pytest.mark.parametrize("gate", [
    "ready", "review", "other-capture", "retry", "validated", "materializing",
    "lane", "initializing", "history", "readonly", "error", "completion", "closing", "preflight",
])
def test_refused_capture_notice_key_and_button_share_the_safe_gate(gate):
    from tests.test_label_operator_action_gates import _render_app

    app = _render_app(deferred_intent_id="di_own")
    app._deferred_capture_ui = {"status": "관리자확인", "state": "BLOCKED_INVALID", "intent_id": "di_own"}
    if gate == "review":
        app._deferred_capture_ui["state"] = "OPERATOR_REVIEW"
    elif gate == "other-capture":
        app._deferred_capture_ui["intent_id"] = "di_other"
    elif gate in {"retry", "validated", "materializing"}:
        app._deferred_capture_ui.update(state={"retry": "RETRY_WAIT_VALIDATION", "validated": "VALIDATED",
                                              "materializing": "LOCAL_EFFECT_PENDING"}[gate],
                                        status={"retry": "저장됨-검증대기", "validated": "검증완료-전송대기",
                                                "materializing": "서버확정-로컬반영대기"}[gate])
    elif gate == "lane":
        app._ui_lane_busy_task = "validation"
    elif gate == "initializing":
        app.initialized_successfully = False
    elif gate == "history":
        app.history_active_load_pending = True
    elif gate == "readonly":
        app.history_view_updates_active_state = False
    elif gate == "error":
        app._pending_workflow_error = True
    elif gate == "completion":
        app._workflow_completion_kind = "full"
    elif gate == "closing":
        app._app_close_in_progress = True
    elif gate == "preflight":
        app._central_package_preflight_in_progress = True
    reset = Mock()
    app._reset_current_set = reset
    app._render_operator_workbench()
    message = app._deferred_capture_pending_notice().message
    expected = gate == "ready"
    assert ("F1" in message) is expected
    assert (app.reset_button.options["state"] == "normal") is expected
    app._handle_workflow_shortcut("f1")
    assert bool(reset.call_count) is expected
    if expected:
        reset.assert_called_once_with(full_reset=True)


def test_spellings_of_one_label_share_its_partition(tmp_path):
    """The parser reads key case and spaces around separators as one label."""
    from deferred_intent_capture import label_partition_key
    variant = PHS2_LABEL.replace("|", " | ").replace("PHS=", "phs=").replace("KMTECH_INPUT_TAG", "kmtech_input_tag")
    assert variant != PHS2_LABEL
    assert label_partition_key(variant) == label_partition_key(PHS2_LABEL)
    assert label_partition_key(OTHER_PHS2) != label_partition_key(PHS2_LABEL)
    _db, _outbox, store = _store(tmp_path)
    first = _capture(store, set_id="SET-SPELL-1", scan=PHS2_LABEL)
    second = _capture(store, set_id="SET-SPELL-2", scan=variant)
    assert _row(store.db_path, first.intent_id)["partition_key"] == _row(store.db_path, second.intent_id)["partition_key"]
    assert store.claim_validation(second.intent_id, worker_id="p1fix-spell", now=NOW) is None  # FIFO inside the label
    assert store.open_label_capture(variant)["physical_qr_payload"] == PHS2_LABEL  # the label as captured


def test_unfinished_capture_in_the_old_shared_partition_is_listed(tmp_path):
    """A 92abdaa capture waiting in the old partition never stays silent."""
    from deferred_intent_capture import LABEL_PARTITION_KEY
    _db, _outbox, store = _store(tmp_path)
    old = store.capture_label_package_source(local_work_identity="SET-OLD-PART", physical_qr_payload="PHS2-OLD",
                                             item_code="ITEM-LABEL-1", partition_key=LABEL_PARTITION_KEY)
    app = _recovery_app(store, {"id": None, "raw": [], "deferred_intent_id": old.intent_id})  # nothing on screen
    candidates = {row["set_id"]: row for row in app._package_recovery_candidates()}
    assert candidates["SET-OLD-PART"]["capture"]["intent_id"] == old.intent_id
    assert candidates["SET-OLD-PART"]["capture"]["state"] == "CAPTURED_UNVERIFIED"


# --- 92abdaa saved state the upgraded PC loads (w9lmp1fix3, lab w9labp1 lm-p1-1-state) -----

LAB_SET = "1790680070079588800"


def _saved_state(module, tmp_path, monkeypatch, scans, *, capture_scan=PHS2_LABEL, central=False):
    """A 92abdaa set naming the review capture of the PHS2 label scanned
    before it: the lab's LEGACY 5/5 took over its id and capture; a
    capture-only set kept no QR."""
    app = _app_for_recovery(module, tmp_path, monkeypatch)
    app.entry = SimpleNamespace(focus_set=lambda: None)  # F1 and a hold refocus the entry
    _db, _outbox, store = _store(tmp_path)
    app.deferred_intent_capture = store
    captured = _head(store, "OPERATOR_REVIEW", set_id=LAB_SET, scan=capture_scan)
    saved = {
        "id": LAB_SET, "raw": list(scans), "parsed": [MASTER] * len(scans),
        "start_time": datetime.now().isoformat(), "error_count": 0, "has_error_or_reset": False,
        "phase": "2", "item_name_override": None, "production_date": None,
        "physical_scanned_qr_payload": PHS2_LABEL if central else "",
        "deferred_intent_id": captured.intent_id, "central_inherit_all": central,
    }
    if scans and not central:
        saved.update(transition_class="LEGACY", transition_reasons=[], transition_duplicate=False,
                     transition_row_key="")
    assert app.data_manager.save_current_state({"current_set_info": saved, "worker_name": "worker-lt",
                                                "timestamp": datetime.now().isoformat()})
    return app, store, captured, saved


def test_full_old_label_set_holding_another_labels_capture_loads_and_completes(tmp_path, monkeypatch):
    """Was: the start load refused it, every new label reopened 건별 복구 and
    F1 was off. Now the capture leaves the set as it is and the set finishes
    with the retry its refused save offered; the next label starts."""
    module = load_label_match_module()
    app, store, captured, _saved = _saved_state(module, tmp_path, monkeypatch, _legacy_set(41))
    before = _row(store.db_path, captured.intent_id)
    try:
        app._load_current_set_state()
        notice = app.__dict__.get("_workflow_blocking_notice")
        assert notice is not None and app._workflow_notice_action_text == "저장 재시도", notice
        assert notice.title.startswith("5/5 유지") and "건별 복구" in notice.message
        assert app.current_set_info["id"] == LAB_SET and app.current_set_info["raw"] == _legacy_set(41)
        assert not app.current_set_info.get("deferred_intent_id")
        on_disk = app.data_manager.load_current_state()["current_set_info"]
        assert "deferred_intent_id" not in on_disk and on_disk["raw"] == _legacy_set(41)
        assert _row(store.db_path, captured.intent_id) == before  # kept as it is
        app._acknowledge_workflow_notice()  # 저장 재시도
        [(_event, details)] = _events(tmp_path, "TRAY_COMPLETE")
        assert details["set_id"] == LAB_SET and details["scanned_product_barcodes"] == _legacy_set(41)
        assert _transition(details)["transition_class"] == "LEGACY"
        assert app.blocks == [] and app.__dict__.get("_workflow_blocking_notice") is None
        [row] = [row for row in app._package_recovery_candidates() if row["set_id"] == LAB_SET]
        assert row["capture"]["intent_id"] == captured.intent_id and row["current"] is None
        _scan(module, app, _legacy_set(42)[0])
        assert app.current_set_info["raw"] == [_legacy_set(42)[0]]
        assert app.current_set_info["id"] != LAB_SET and not app.current_set_info.get("deferred_intent_id")
        assert _row(store.db_path, captured.intent_id) == before
    finally:
        _close(app)


def test_partial_old_label_set_holding_another_labels_capture_cancels_with_f1(tmp_path, monkeypatch):
    module = load_label_match_module()
    app, store, captured, _saved = _saved_state(module, tmp_path, monkeypatch, _legacy_set(43)[:3])
    before = _row(store.db_path, captured.intent_id)
    try:
        app._load_current_set_state()
        assert app.__dict__.get("_workflow_blocking_notice") is None
        assert app.current_set_info["raw"] == _legacy_set(43)[:3]
        assert not app.current_set_info.get("deferred_intent_id")
        assert app._reset_current_set(full_reset=True) is True  # F1
        assert not Path(app._package_current_state_path()).exists()
        app.data_manager.log_queue.join()  # Observe the asynchronous CSV writer after task_done.
        [(_event, cancelled)] = _events(tmp_path, "SET_CANCELLED")
        assert cancelled["set_id"] == LAB_SET
        _scan(module, app, _legacy_set(44)[0])
        assert app.current_set_info["raw"] == [_legacy_set(44)[0]] and app.current_set_info["id"] != LAB_SET
        assert _row(store.db_path, captured.intent_id) == before
    finally:
        _close(app)


def test_capture_only_set_without_its_qr_frees_the_screen(tmp_path, monkeypatch):
    """A 92abdaa capture-only set (physical QR empty) held the start load."""
    module = load_label_match_module()
    app, store, captured, _saved = _saved_state(module, tmp_path, monkeypatch, [])
    before = _row(store.db_path, captured.intent_id)
    try:
        app._load_current_set_state()
        assert app.__dict__.get("_workflow_blocking_notice") is None
        assert not Path(app._package_current_state_path()).exists()
        _scan(module, app, _legacy_set(45)[0])
        assert app.current_set_info["raw"] == [_legacy_set(45)[0]] and app.current_set_info["id"] != LAB_SET
        assert _row(store.db_path, captured.intent_id) == before
        assert LAB_SET in {row["set_id"] for row in app._package_recovery_candidates()}
    finally:
        _close(app)


def test_hold_and_continue_accepts_a_listed_capture_no_set_holds(tmp_path, monkeypatch):
    """'보류 후 계속' on a capture-only row answered '대상 또는 작업자가
    바뀌었습니다': it now records the hold and leaves the capture as it is."""
    module = load_label_match_module()
    app = _app_for_recovery(module, tmp_path, monkeypatch)
    _db, _outbox, store = _store(tmp_path)
    app.deferred_intent_capture = store
    captured = _head(store, "OPERATOR_REVIEW", set_id=LAB_SET, scan=PHS2_LABEL)
    before = _row(store.db_path, captured.intent_id)
    try:
        target, _source = app._package_pin_target("LABEL.SET_HOLD", LAB_SET)
        assert target["id"] == LAB_SET
        app._package_recovery_manager = lambda code=None: "admin-p1fix3"
        assert app._hold_package_recovery_set(LAB_SET) is True
        held = app.package_outbox.get_workbench_hold(LAB_SET)
        assert json.loads(held["snapshot_json"])["capture"]["intent_id"] == captured.intent_id
        assert app._package_set_hold_complete(LAB_SET, held)
        assert _row(store.db_path, captured.intent_id) == before
    finally:
        _close(app)


def test_hold_target_reads_the_set_on_screen_with_its_start_time(tmp_path):
    """A set on screen keeps start_time as a datetime; its fingerprint raised
    and '보류 후 계속' answered '대상 또는 작업자가 바뀌었습니다'."""
    _db, _outbox, store = _store(tmp_path)
    captured = _head(store, "OPERATOR_REVIEW", set_id="SET-LIVE", scan="PHS2-LIVE")
    app = _recovery_app(store, {"id": "SET-LIVE", "raw": ["PHS2-LIVE"], "parsed": ["ITEM-LABEL-1"],
                                "deferred_intent_id": captured.intent_id,
                                "physical_scanned_qr_payload": "PHS2-LIVE", "start_time": datetime.now()})
    target, _source = app._package_pin_target("LABEL.SET_HOLD", "SET-LIVE")
    assert target["id"] == "SET-LIVE"
    assert app._package_pin_target("LABEL.SET_HOLD", "SET-LIVE")[1] == _source  # stable for the write


def test_central_set_whose_capture_differs_is_listed_for_hold_and_the_pc_continues(tmp_path, monkeypatch):
    """A set that needs its capture is not run without it: it stays as saved,
    is listed with its own evidence, and a hold frees the PC."""
    module = load_label_match_module()
    app, store, captured, saved = _saved_state(module, tmp_path, monkeypatch, [PHS2_LABEL],
                                               capture_scan=OTHER_PHS2, central=True)
    before = _row(store.db_path, captured.intent_id)
    current_path = Path(app._package_current_state_path())
    try:
        app._load_current_set_state()
        assert app._workflow_blocking_notice.title == "포장 기록 저장 확인 필요"
        assert "건별 복구에서 이 세트를 보류" in app._workflow_blocking_notice.message
        [row] = [row for row in app._package_recovery_candidates() if row["set_id"] == LAB_SET]
        assert row["current"] == saved
        app._package_pin_target("LABEL.SET_HOLD", LAB_SET)
        app._package_recovery_manager = lambda code=None: "admin-p1fix3"
        assert app._hold_package_recovery_set(LAB_SET) is True
        assert not current_path.exists() and app.package_outbox.get_workbench_hold(LAB_SET)
        assert app.__dict__.get("_workflow_blocking_notice") is None
        assert _row(store.db_path, captured.intent_id) == before
    finally:
        _close(app)


# --- P1-5: 30 seconds of clock tolerance, the same as the server ---------------------------


def test_lease_issued_one_second_ahead_of_pc_clock_is_accepted(clock_case):
    case = clock_case
    _first_scan(case)
    issued = lease_module.parse_utc(case.claims["issued_at"], field="issued_at")
    case.instant[0] = issued - timedelta(seconds=1)
    prepared = _queue_f3(case)
    assert prepared["status"] == "PENDING"
    # The completion is never earlier than the grant the server issued.
    row = case.app.package_outbox.get_by_set_id(case.app.current_set_info["id"])
    assert json.loads(row["draft_json"])["operation_lease_completed_at"] == case.claims["issued_at"]


def test_lease_more_than_30_seconds_ahead_is_refused_and_names_the_clock(clock_case):
    case = clock_case
    _first_scan(case)
    issued = lease_module.parse_utc(case.claims["issued_at"], field="issued_at")
    case.instant[0] = issued - timedelta(seconds=31)
    with pytest.raises(lease_module.OperationLeaseError) as refused:
        _queue_f3(case)
    assert refused.value.code == "OPERATION_LEASE_NOT_YET_VALID"
    app = case.app
    app._workflow_total_scan_count = lambda: 5
    app._render_operator_workbench = lambda: None
    app._publish_durable_commit_block(refused.value)
    assert "시계" in app._workflow_blocking_notice.message
    assert "시계" in app._workflow_blocking_notice.title

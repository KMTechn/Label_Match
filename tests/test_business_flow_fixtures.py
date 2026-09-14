"""W7's three missing business-fixture rows, without a GUI or live service."""

from copy import deepcopy
import csv
from datetime import datetime, timezone
import json
import os
import socket
from types import SimpleNamespace

import pytest
import sealed_transfer_exchange as exchange_module

from tests._business_flow_fixture import (
    BusinessCase, BusinessClock, BusinessProvider, lm, package, lease, package_fixture,
)
from sealed_transfer_exchange import (
    SealedTransferExchangeAdmissionError, SealedTransferExchangeDraft,
)


@pytest.fixture
def business_factory(tmp_path, monkeypatch):
    """Returns isolated cases; finalization closes every real event writer."""
    BusinessClock.instant = datetime(2026, 9, 14, 23, 59, tzinfo=timezone.utc)
    for module in (lm, package, lease, package_fixture, exchange_module):
        monkeypatch.setattr(module, "datetime", BusinessClock)
    # Telemetry delivery and UI only. Authentication, lease, source validation,
    # writer admission, event flush/fsync, marker and receipt checks are real.
    monkeypatch.setattr(lm, "_label_match_direct_sync_context", lambda *a, **k: {})
    monkeypatch.setattr(lm, "_label_match_bind_current_log_source", lambda c, m: c)
    monkeypatch.setattr(lm, "_label_match_start_session_direct_sync",
                        lambda *a, **k: SimpleNamespace(is_alive=lambda: False))
    for name in ("showwarning", "showerror", "showinfo"):
        monkeypatch.setattr(lm.messagebox, name, lambda *a, **k: None)
    monkeypatch.setattr(lm.messagebox, "askyesno", lambda *a, **k: True)

    def forbidden(*args, **kwargs):
        raise AssertionError("Business fixtures must not open a socket or GUI")

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(lm.tk, "Toplevel", forbidden)
    cases = []

    def factory(*, exchange=False, pair_count=2):
        root = tmp_path / str(len(cases))
        root.mkdir()
        case = BusinessCase(root, BusinessProvider(exchange=exchange, pair_count=pair_count))
        cases.append(case)
        return case

    yield factory
    for case in reversed(cases):
        case.close()


def completion_events(case):
    events = []
    for path in sorted(case.root.glob("포장실작업이벤트로그_BUSINESS_*.csv")):
        with path.open(encoding="utf-8-sig", newline="") as stream:
            for row in csv.DictReader(stream):
                if row["event"] == "TRAY_COMPLETE":
                    events.append((row["timestamp"], json.loads(row["details"])))
    return events


def prepare_exchange(case):
    case.accept()
    assert not case.app._operation_lease_blocks_f4()
    draft = SealedTransferExchangeDraft(case.provider.exchange.old_barcodes)
    for old, new in zip(case.provider.exchange.old_barcodes,
                        case.provider.exchange.new_barcodes, strict=True):
        draft.accept(old)
        draft.accept(new)
    # Correct a pair, remove a pair, then restore it through normal scan input.
    old, new = draft.snapshot()[0]
    assert draft.begin_edit(0) == old
    draft.accept(old)
    draft.accept(new)
    draft.remove(0)
    draft.accept(old)
    draft.accept(new)
    ordered = draft.snapshot()
    assert case.provider.effects == 0
    assert case.app.sealed_transfer_exchange_store.blocking_rows() == []
    current = case.app.current_set_info
    attempt = case.app.sealed_transfer_exchange_coordinator.prepare(
        set_id=current["id"], old_seal_qr_payload=case.provider.exchange.qr,
        old_seal_fields=current["sealed_transfer"], operator=case.app.worker_name,
        old_barcodes=[pair[0] for pair in ordered], new_barcodes=[pair[1] for pair in ordered],
    )
    return attempt, ordered


@pytest.mark.parametrize("state", ["PREPARED", "OPERATOR_REVIEW", "ACKED"])
def test_f4_saved_pairs_restore_with_real_store(business_factory, state, monkeypatch):
    case = business_factory(exchange=True)
    attempt, ordered = prepare_exchange(case)
    if state != "PREPARED":
        case.provider.mode = "conflict" if state == "OPERATOR_REVIEW" else "online"
        attempt = case.app.sealed_transfer_exchange_coordinator.attempt(attempt.intent_id)
    assert attempt.status == state
    original = dict(case.app.sealed_transfer_exchange_store.load(attempt.intent_id))
    raw = deepcopy(case.app.current_set_info["raw"])
    prompts = []
    # Observe the request to open the QR dialog; do not invent confirmation.
    monkeypatch.setattr(lm.Label_Match, "_prompt_new_seal_verification",
                        lambda app, value: prompts.append(value) or False)
    case.restart()
    restored = case.app._current_sealed_transfer_exchange_attempt()
    assert (restored.intent_id, restored.status) == (attempt.intent_id, state)
    assert tuple(zip(restored.old_barcodes, restored.new_barcodes, strict=True)) == ordered
    assert dict(case.app.sealed_transfer_exchange_store.load(attempt.intent_id)) == original
    assert case.app.current_set_info["raw"] == raw == [case.provider.raw]
    assert case.app.current_set_info["id"] == "SET-BUSINESS"
    assert case.provider.effects == int(state == "ACKED")
    assert case.app._sealed_transfer_exchange_blocks_local_action("포장 완료")
    if state == "ACKED":
        assert restored.seal_verification_status == restored.local_apply_status == "PENDING"
        assert prompts and all(value.intent_id == attempt.intent_id for value in prompts)
        store = case.app.sealed_transfer_exchange_store
        with pytest.raises(package.PackageLogisticsError):
            store.mark_seal_verified(attempt.intent_id, "incorrect-qr")
        store.mark_seal_verified(attempt.intent_id, restored.new_seal_qr_payload)
        assert case.app._apply_acked_sealed_transfer_exchange(attempt.intent_id)
        assert case.app._apply_acked_sealed_transfer_exchange(attempt.intent_id)
        assert case.app.current_set_info["raw"] == raw
        assert case.app.current_set_info["sealed_transfer"]["SREV"] == 2
        assert case.app.current_set_info["package_source_snapshot"] is None
        assert not store.blocking_rows(set_id="SET-BUSINESS")
        assert case.provider.effects == 1


def test_f4_three_pairs_require_extension_before_durable_intent(business_factory):
    case = business_factory(exchange=True, pair_count=3)
    with pytest.raises(SealedTransferExchangeAdmissionError):
        prepare_exchange(case)
    assert case.app.sealed_transfer_exchange_store.blocking_rows() == []
    assert case.provider.effects == 0
    assert case.app.current_set_info["raw"] == [case.provider.raw]


@pytest.mark.parametrize("refusal", ["lease", "missing_profile"])
def test_f4_authority_boundaries_preserve_input_without_exchange(business_factory, refusal):
    case = business_factory(exchange=True)
    case.accept()
    current = deepcopy(case.app.current_set_info)
    if refusal == "lease":
        case.app._acquire_operation_lease(
            case.provider.raw, expected_snapshot=current["package_source_snapshot"],
        )
        assert case.app._operation_lease_blocks_f4()
    else:
        case.app.package_logistics_client = None
    # These real F4 refusals occur before opening a popup, with run_tests=False.
    assert case.app._prompt_sealed_transfer_exchange() is False
    assert case.app.current_set_info == current
    assert not case.app.sealed_transfer_exchange_store.blocking_rows()
    assert case.app.package_outbox.get_by_set_id(current["id"]) is None
    assert case.provider.effects == 0


@pytest.mark.parametrize("mode,status", [("offline", "PENDING"), ("online", "ACKED"),
                                         ("conflict", "CONFLICT")])
def test_f3_durable_completion_precedes_pending_ack_or_conflict(
    business_factory, monkeypatch, mode, status,
):
    case = business_factory()
    case.accept()
    assert not case.app._operation_lease_blocks_f4()
    assert case.app.package_outbox.claim_next() is None
    synced = []
    real_fsync = os.fsync

    def fsync(fd):
        result = real_fsync(fd)
        for path in case.root.glob("포장실작업이벤트로그_BUSINESS_*.csv"):
            if os.path.samestat(os.fstat(fd), path.stat()):
                synced.append(path)
        return result

    monkeypatch.setattr(os, "fsync", fsync)
    marked = []
    real_marker = case.app.package_outbox.mark_local_completion_committed

    def marker(key, **kwargs):
        before = case.app.package_outbox.get_by_set_id("SET-BUSINESS")
        assert before["local_completion_committed"] == 0
        assert case.app.package_outbox.claim_next() is None
        assert synced and len(completion_events(case)) == 1
        assert case.actions == [] and case.provider.effects == 0
        assert case.app.package_operation_lease_store.get(lease_id=kwargs["operation_lease_id"])["status"] == "PREFETCHED"
        value = real_marker(key, **kwargs)
        marked.append(key)
        return value

    monkeypatch.setattr(case.app.package_outbox, "mark_local_completion_committed", marker)
    assert case.app._begin_central_package_submission()
    row = case.app.package_outbox.get_by_set_id("SET-BUSINESS")
    assert marked == [row["idempotency_key"]]
    assert (row["status"], row["local_completion_committed"]) == ("PENDING", 1)
    assert "sound:pass" in case.actions
    assert "로컬 완료 저장됨 · 중앙 전송 대기" in case.app.save_status_label.kwargs["text"]
    assert case.app.current_set_info["raw"] == []
    original_events = completion_events(case)
    details = original_events[0][1]
    assert details["scanned_product_barcodes"] == [case.provider.raw]
    assert details["scan_count"] == 1 and details["product_sample_barcodes"] == []
    lease_id = json.loads(row["draft_json"])["operation_lease_id"]
    assert case.app.package_operation_lease_store.get(lease_id=lease_id)["status"] == "LOCAL_COMPLETED"
    case.provider.mode = mode
    counts = case.drain()
    assert counts[{"PENDING": "retry", "ACKED": "acked", "CONFLICT": "conflict"}[status]] == 1
    final = case.app.package_outbox.get_by_set_id("SET-BUSINESS")
    assert (final["status"], final["local_completion_committed"]) == (status, 1)
    assert final["idempotency_key"] == row["idempotency_key"]
    assert completion_events(case) == original_events
    case.restart()
    assert case.app.current_set_info["raw"] == []
    assert completion_events(case) == original_events
    if status == "CONFLICT":
        reviews = case.app.package_outbox.list_post_review_csv_pending()
        assert len(reviews) == 1
        assert final["review_status"] == "OPERATOR_REVIEW"
        assert case.app.package_operation_lease_store.get(lease_id=lease_id)["status"] == "OPERATOR_REVIEW"
    elif status == "ACKED":
        assert case.provider.effects == 1
        assert case.app.package_operation_lease_store.get(lease_id=lease_id)["status"] == "ACKED"
    else:
        # A locally committed pending package does not own the next input slot.
        case.accept(set_id="SET-NEXT")
        assert case.app.current_set_info["id"] == "SET-NEXT"
        assert case.app.package_outbox.get_by_set_id("SET-BUSINESS")["status"] == "PENDING"


@pytest.mark.parametrize("refusal", ["signature", "unauthorized", "profile"])
def test_f3_invalid_authority_never_commits_local_success(business_factory, refusal):
    case = business_factory()
    case.accept()
    if refusal == "signature":
        case.provider.bad_signature = True
    elif refusal == "unauthorized":
        case.provider.mode = "unauthorized"
    else:
        from dataclasses import replace
        case.app.package_logistics_client.config = replace(case.provider.config, authority_scope_id="WRONG-SCOPE")
    assert case.app._begin_central_package_submission() is False
    assert not completion_events(case)
    assert case.app.package_outbox.get_by_set_id("SET-BUSINESS") is None
    assert not any(action.startswith("sound:") for action in case.actions)
    assert case.app.current_set_info["raw"] == [case.provider.raw]
    assert case.provider.effects == 0


@pytest.mark.parametrize("exchange", [False, True])
def test_midnight_lost_ack_restarts_receipt_first_without_second_effect(
    business_factory, exchange,
):
    case = business_factory(exchange=exchange)
    if exchange:
        attempt, pairs = prepare_exchange(case)
        case.provider.mode = "lost_ack"
        result = case.app.sealed_transfer_exchange_coordinator.attempt(attempt.intent_id)
        assert result.status == "RETRY_WAIT"
        original = dict(case.app.sealed_transfer_exchange_store.load(attempt.intent_id))
        saved = deepcopy(case.app.current_set_info)
    else:
        case.accept()
        assert case.app._begin_central_package_submission()
        case.provider.mode = "lost_ack"
        assert case.drain()["retry"] == 1
        original = case.app.package_outbox.get_by_set_id("SET-BUSINESS")
        saved = completion_events(case)
        assert saved[0][0].startswith("2026-09-14T23:59:")
    assert case.provider.effects == 1
    case.provider.mode = "online"
    case.restart(at=datetime(2026, 9, 15, 0, 1, tzinfo=timezone.utc))
    offset = len(case.provider.calls)
    if exchange:
        state_path = case.root / lm.Label_Match.FILES.CURRENT_STATE
        assert json.loads(state_path.read_text(encoding="utf-8"))["timestamp"].startswith("2026-09-14T23:59:")
        assert case.app.current_set_info["id"] == saved["id"]
        assert case.app.current_set_info["raw"] == saved["raw"]
        result = case.app.sealed_transfer_exchange_coordinator.attempt(attempt.intent_id)
        assert result.status == "ACKED"
        assert tuple(zip(result.old_barcodes, result.new_barcodes, strict=True)) == pairs
        final = dict(case.app.sealed_transfer_exchange_store.load(attempt.intent_id))
        for field in ("intent_id", "intent_hash", "command_id", "command_hash", "command_json"):
            assert final[field] == original[field]
        assert result.seal_verification_status == "PENDING"
    else:
        assert case.drain()["acked"] == 1
        final = case.app.package_outbox.get_by_set_id("SET-BUSINESS")
        for field in ("idempotency_key", "draft_json", "command_json", "local_completion_committed_at"):
            assert final[field] == original[field]
        assert final["local_completion_committed"] == 1
        assert completion_events(case) == saved
    recovery_calls = case.provider.calls[offset:]
    assert recovery_calls and "/receipts/" in recovery_calls[0][1]
    assert all(method == "GET" for method, _, _ in recovery_calls)
    assert case.provider.effects == 1


def test_signed_f3_marker_failure_recovers_same_completion_after_midnight(
    business_factory, monkeypatch,
):
    case = business_factory()
    case.accept()
    refused = []

    def refuse_marker(key, **kwargs):
        assert len(completion_events(case)) == 1
        refused.append((key, kwargs["operation_lease_id"]))
        raise OSError("fixture marker write refused after CSV durability")

    monkeypatch.setattr(case.app.package_outbox, "mark_local_completion_committed", refuse_marker)
    assert case.app._begin_central_package_submission() is False
    row = case.app.package_outbox.get_by_set_id("SET-BUSINESS")
    assert row["local_completion_committed"] == 0
    assert case.app.package_outbox.claim_next() is None
    assert not any(action.startswith("sound:") for action in case.actions)
    assert case.provider.effects == 0
    original_events = completion_events(case)
    case.restart(at=datetime(2026, 9, 15, 0, 1, tzinfo=timezone.utc))
    final = case.app.package_outbox.get_by_set_id("SET-BUSINESS")
    assert final["idempotency_key"] == row["idempotency_key"] == refused[0][0]
    assert final["draft_json"] == row["draft_json"]
    assert final["local_completion_committed"] == 1
    assert completion_events(case) == original_events
    assert case.app.package_operation_lease_store.get(lease_id=refused[0][1])["status"] == "LOCAL_COMPLETED"
    assert case.app.current_set_info["raw"] == []
    assert case.drain()["acked"] == 1
    assert case.provider.effects == 1

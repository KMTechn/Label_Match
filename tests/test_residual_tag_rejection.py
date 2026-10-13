"""Residual tags never start packaging; server refusals never retry."""

import base64
import copy
import hashlib
import json
from datetime import datetime
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest

import carrier_identity_port as carrier
import label_completion
import label_transition
from label_recovery_schema import validate_recovery_record
from package_logistics import (
    PackageApiError, PackageCancellationOutbox, PackageCancellationOutboxProcessor,
    PackageClientConfig, PackageLogisticsClient, PackageOutbox, PackageOutboxProcessor,
)
from phs_label_workflow import PHSLabelWorkflowError, parse_compact_phs2
from sealed_transfer_exchange import SealedTransferExchangeCoordinator, SealedTransferExchangeStore
from tests.test_label_match_core import _FakeEntry, load_label_match_module
from tests.test_legacy_label_transition import (
    MASTER, _app_for_recovery, _close, _legacy_set, _outbox_rows, _packaging_app, _scan,
)
from tests.test_local_dayend_recovery import dayend_case  # noqa: F401
from tests.test_package_logistics import _ack_package_creation, _cancellation_intent, _draft
from tests.test_sealed_transfer_exchange import FakeClient, _prepare


PACKAGE_CREATE_PATH = "/logistics/api/v1/packages"
REJECTION_CODE = "RESIDUAL_TAG_NOT_ALLOWED"
REJECTION_MESSAGE = "잔량표는 포장에 쓸 수 없습니다 — 검사실로 돌려보내세요"


def _tag(variant="plain"):
    number = "RT" + str(12).zfill(8)
    digest = hashlib.sha256(number.encode()).hexdigest()[:16]
    qr = f"PHS=2|SRC=KMTECH_RESIDUAL_TAG|ITG={number}|CLC=RESIDUAL|LBL={number}|HSH={digest}"
    if variant == "legacy-fields":
        qr += "|SPC=residual|QT=10"
    elif variant == "mixed-source":
        qr += "|SRC=KMTECH_INPUT_TAG|SPC=residual"
    elif variant == "case-space":
        qr = qr.replace("SRC=KMTECH_RESIDUAL_TAG", " src = kmtech_residual_tag ")
    elif variant == "encoded":
        qr = base64.urlsafe_b64encode(qr.encode()).decode().rstrip("=")
    return qr


@pytest.mark.parametrize("variant", ["plain", "legacy-fields", "mixed-source", "case-space", "encoded"])
@pytest.mark.parametrize("parser", ["compact", "legacy", "workflow"])
def test_residual_tag_parser_boundaries(variant, parser):
    qr = _tag(variant)
    if parser == "compact":
        with pytest.raises(ValueError, match=REJECTION_MESSAGE):
            carrier.parse_compact_carrier(qr)
    elif parser == "legacy":
        assert carrier.parse_legacy_fields(qr) is None
    else:
        with pytest.raises(PHSLabelWorkflowError) as rejected:
            parse_compact_phs2(qr)
        assert rejected.value.code == REJECTION_CODE
        assert rejected.value.retryable is False
        assert str(rejected.value) == REJECTION_MESSAGE


@pytest.mark.parametrize("registered,transition", [(False, False), (False, True), (True, False), (True, True)])
@pytest.mark.parametrize("variant", ["plain", "legacy-fields", "mixed-source", "case-space", "encoded"])
def test_residual_scan_precedes_local_work(tmp_path, monkeypatch, registered, transition, variant):
    module = load_label_match_module()
    app, _ = _packaging_app(module, tmp_path, monkeypatch, registered=registered, transition=transition)
    events, saves, notices = [], [], []
    monkeypatch.setattr(app.data_manager, "log_event", lambda *a, **k: events.append(a))
    app._save_current_set_state = lambda: saves.append(True) or True
    app._show_scanner_notice = lambda title, message, **k: notices.append(message)
    before = copy.deepcopy(app.current_set_info)
    app.entry = _FakeEntry(_tag(variant))
    try:
        app.process_input()
        assert app.current_set_info == before
        assert events == saves == []
        assert _outbox_rows(tmp_path) == []
        assert app.entry.get() == ""
        assert app.sounds == ["fail"]
        assert notices == [REJECTION_MESSAGE]
    finally:
        _close(app)


@pytest.mark.parametrize("phase", ["scanner_exchange_phase", "shortage_confirmation_pending", "exact_rescan_active"])
def test_residual_scan_preserves_active_tray(tmp_path, monkeypatch, phase):
    module = load_label_match_module()
    app, _ = _packaging_app(module, tmp_path, monkeypatch, registered=False, transition=True)
    app.current_set_info.update(raw=_legacy_set(3)[:2], parsed=["item", "item"])
    app.current_set_info[phase] = "TARGET" if phase == "scanner_exchange_phase" else True
    app._show_scanner_notice = lambda *a, **k: None
    events, saves = [], []
    app._save_current_set_state = lambda: saves.append(True) or True
    monkeypatch.setattr(app.data_manager, "log_event", lambda *a, **k: events.append(a))
    before = copy.deepcopy(app.current_set_info)
    app.entry = _FakeEntry(_tag())
    try:
        app.process_input()
        assert app.current_set_info == before
        assert events == saves == []
        assert app.sounds == ["fail"]
    finally:
        _close(app)


@pytest.mark.parametrize("variant", ["plain", "encoded", "legacy-fields"])
def test_residual_recovery_preserves_original_before_replay(dayend_case, variant):
    module, app, path, _save = dayend_case
    saved = {"timestamp": module.datetime.now().isoformat(),
             "current_set_info": {"id": "residual-recovery", "raw": [_tag(variant)],
                                  "parsed": ["RESIDUAL"], "transition_class": "PHS2_MALFORMED"}}
    original = json.dumps(saved).encode()
    path.write_bytes(original)
    replay, events = [], []
    app._replay_scanner_audits = lambda **k: replay.append(True)
    app.data_manager.log_event = lambda *a, **k: events.append(a)
    app._load_current_set_state_checked()
    assert not app.current_set_info["raw"]
    assert path.read_bytes() == original
    assert replay == events == []
    assert app.sounds == ["fail"]
    assert REJECTION_MESSAGE in app._workflow_blocking_notice.message
    assert _outbox_rows(path.parent) == []


@pytest.mark.parametrize("variant", ["plain", "encoded", "legacy-fields"])
@pytest.mark.parametrize("slot", ["first", "product", "shortage-undo"])
def test_residual_held_snapshot_cannot_restore(variant, slot):
    current = {"id": "held-residual", "raw": [_tag(variant)]}
    if slot == "product":
        current["raw"].insert(0, _legacy_set(3)[0])
    elif slot == "shortage-undo":
        current = {"id": "held-residual", "raw": _legacy_set(3)[:2],
                   "shortage_undo_snapshot": current}
    snapshot = {"current_set_info": current}
    assert validate_recovery_record("current", snapshot) is False


def test_residual_recovery_scan_precedes_manager_and_audit():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    calls, sounds = [], []
    app._package_recovery_manager = lambda *a: calls.append("manager") or "manager"
    app._audit_package_recovery_action = lambda *a: calls.append("audit")
    app._play_sound = sounds.append
    app._package_pin_ui = lambda action, *a, **k: action(*a)
    assert app._recheck_package_recovery_physical("held-residual", _tag()) == REJECTION_MESSAGE
    assert calls == []
    assert sounds == ["fail"]


def test_residual_cancellation_scan_precedes_history_and_records():
    class NoLookup(dict):
        def items(self):
            pytest.fail("residual cancellation must not look up completed work")

    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.set_details_map = NoLookup()
    sounds, notices, events = [], [], []
    app._play_sound = sounds.append
    app._show_scanner_notice = lambda title, message, **k: notices.append(message)
    app.data_manager = SimpleNamespace(log_event=lambda *a, **k: events.append(a))
    app._cancel_completed_tray_by_label(_tag())
    assert sounds == ["fail"]
    assert notices == [REJECTION_MESSAGE]
    assert events == []


def test_residual_enter_does_not_acknowledge_an_existing_error():
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.entry = _FakeEntry(_tag())
    app._pending_workflow_error = True
    events, sounds, notices = [], [], []
    app._clear_scanner_notice = lambda: None
    app._acknowledge_workflow_notice = lambda *a: events.append("acknowledge") or "break"
    app._play_sound = sounds.append
    app._show_scanner_notice = lambda title, message, **k: notices.append(message)
    assert app._handle_scan_enter() == "break"
    assert events == []
    assert sounds == ["fail"]
    assert notices == [REJECTION_MESSAGE]
    assert app.entry.get() == ""


def _server(status):
    calls = []

    def transport(method, url, headers, body, timeout):
        calls.append((method, urlsplit(url).path))
        return {"ok": False, "error": {"status_code": status, "code": REJECTION_CODE,
                "message": REJECTION_MESSAGE, "retryable": False}}

    client = PackageLogisticsClient(PackageClientConfig(
        "https://residual.invalid", hashlib.sha256(b"residual-test").hexdigest(),
        "residual-scope", "residual-host", "residual-device"), transport=transport)
    return client, calls


@pytest.mark.parametrize("status", [400, 409, 503])
@pytest.mark.parametrize("step", ["label-package-source", "label-operation-lease"])
def test_fake_server_refusal_is_final_even_without_commit_metadata(status, step):
    client, calls = _server(status)
    with pytest.raises(PackageApiError) as rejected:
        client.create_package({"idempotency_key": "residual-command", "authority_scope_id": "residual-scope"})
    assert calls == [("POST", PACKAGE_CREATE_PATH)]
    assert rejected.value.retryable is False
    wrapped = RuntimeError("validation wrapper")
    wrapped.__cause__ = rejected.value
    module = load_label_match_module()
    app = object.__new__(module.Label_Match)
    app.package_logistics_client = client
    result = app._classify_deferred_validation_error(wrapped, step_id=step, dispatch_record={
        "idempotency_key": "residual-lease", "request_hash": hashlib.sha256(b"dispatch").hexdigest(),
        "recorded_at": datetime.now().isoformat()})
    assert result["outcome"] == "INVALID"
    assert result["reason_code"] == REJECTION_CODE
    assert result["retry_after_seconds"] is None


@pytest.mark.parametrize("status", [400, 409, 503])
def test_fake_server_package_outbox_refusal_never_reposts(tmp_path, status):
    server, calls = _server(status)
    outbox = PackageOutbox(tmp_path / "packages.sqlite3")
    draft = _draft()
    queued = outbox.enqueue(draft)
    outbox.mark_local_completion_committed(queued["idempotency_key"])
    client = SimpleNamespace(
        build_create_package_command=lambda draft, **k: ("source", {
            "idempotency_key": k["idempotency_key"], "authority_scope_id": "residual-scope", "payload": {}}),
        create_package=server.create_package)
    processor = PackageOutboxProcessor(outbox, client)
    assert processor.drain(limit=1) == {"acked": 0, "retry": 0, "conflict": 1}
    terminal = outbox.get_by_set_id(draft.set_id)
    assert terminal["last_error_code"] == REJECTION_CODE
    outbox.request_receipt_recheck(terminal, {"ok": False})
    client.get_receipt_if_exists = lambda *a, **k: calls.append(("GET", "receipt"))
    assert processor.drain(limit=1) == {"acked": 0, "retry": 0, "conflict": 0}
    restarted = PackageOutboxProcessor(PackageOutbox(tmp_path / "packages.sqlite3"), client)
    assert restarted.drain(limit=1) == {"acked": 0, "retry": 0, "conflict": 0}
    assert calls == [("POST", PACKAGE_CREATE_PATH)]


def test_fake_server_cancellation_refusal_never_retries(tmp_path):
    server, calls = _server(503)
    path = tmp_path / "cancellation.sqlite3"
    draft = _draft()
    _ack_package_creation(PackageOutbox(path), draft)
    outbox = PackageCancellationOutbox(path)
    intent = _cancellation_intent(draft)
    outbox.enqueue(intent)
    outbox.mark_local_event_committed(intent.cancellation_event_id)
    client = SimpleNamespace(
        build_cancel_package_command=lambda intent, row, **k: {
            "idempotency_key": k["idempotency_key"], "authority_scope_id": "residual-scope"},
        cancel_package=server.create_package)
    processor = PackageCancellationOutboxProcessor(outbox, client)
    assert processor.drain(limit=1) == {"acked": 0, "retry": 0, "conflict": 1, "deferred": 0}
    assert processor.drain(limit=1)["retry"] == 0
    restarted = PackageCancellationOutboxProcessor(PackageCancellationOutbox(path), client)
    assert restarted.drain(limit=1)["retry"] == 0
    assert calls == [("POST", PACKAGE_CREATE_PATH)]


def test_server_refused_work_can_be_held_then_next_tray_started_after_restart(tmp_path, monkeypatch):
    module = load_label_match_module()
    server, calls = _server(503)
    app = _app_for_recovery(module, tmp_path, monkeypatch)
    app.entry = _FakeEntry("")
    app.entry.focus_set = lambda: None
    draft = _draft()
    queued = app.package_outbox.enqueue(draft)
    app.package_outbox.mark_local_completion_committed(queued["idempotency_key"])
    client = SimpleNamespace(
        build_create_package_command=lambda draft, **k: ("source", {
            "idempotency_key": k["idempotency_key"], "authority_scope_id": "residual-scope", "payload": {}}),
        create_package=server.create_package)
    processor = PackageOutboxProcessor(app.package_outbox, client)
    restarted = None
    closed = False
    try:
        assert processor.drain(limit=1) == {"acked": 0, "retry": 0, "conflict": 1}
        app.current_set_info.update(id=draft.set_id, raw=_legacy_set(7)[:2], parsed=[MASTER, MASTER],
                                    start_time=module.datetime.now())
        assert app._save_current_set_state()
        original = copy.deepcopy(app.current_set_info)
        row = app.package_outbox.get_by_set_id(draft.set_id)
        app._package_recovery_manager = lambda *a: "manager"
        app._package_recovery_candidates = lambda: [{
            "set_id": draft.set_id, "current": app.current_set_info, "package": row, "exchange": None}]
        assert app._hold_package_recovery_set(draft.set_id)
        assert not app.current_set_info["raw"]
        held = app.package_outbox.get_workbench_hold(draft.set_id)
        assert json.loads(held["snapshot_json"])["current_set_info"]["raw"] == original["raw"]
        _close(app)
        closed = True
        restarted = _app_for_recovery(module, tmp_path, monkeypatch)
        assert restarted.package_outbox.get_by_set_id(draft.set_id)["status"] == "CONFLICT"
        assert PackageOutboxProcessor(restarted.package_outbox, client).drain(limit=1) == {
            "acked": 0, "retry": 0, "conflict": 0}
        restarted._load_current_set_state_checked()
        _scan(module, restarted, _legacy_set(8)[0])
        assert restarted.current_set_info["raw"] == [_legacy_set(8)[0]]
        assert restarted.package_outbox.get_workbench_hold(draft.set_id) == held
        assert calls == [("POST", PACKAGE_CREATE_PATH)]
    finally:
        if restarted is not None:
            _close(restarted)
        elif not closed:
            _close(app)


def test_server_refusal_never_falls_back_to_transitional_local_completion(monkeypatch):
    module = load_label_match_module()
    server, calls = _server(503)
    current = {"id": "refused-transition", "raw": [_legacy_set(9)[0]],
               "central_inherit_all": True, "transition_class": label_transition.PHS2_CENTRAL}
    app = SimpleNamespace(package_outbox=SimpleNamespace(get_by_set_id=lambda *a: None))
    local = []
    monkeypatch.setattr(label_completion, "_decide_transition_local", lambda *a, **k: local.append(True) or True)
    before = copy.deepcopy(current)
    with pytest.raises(PackageApiError) as rejected:
        label_completion._queue_transition_package(app, current,
            package=lambda: server.create_package({"idempotency_key": "refused-transition",
                                                  "authority_scope_id": "residual-scope"}),
            is_manual_complete=False, parse_sealed=module._label_match_parse_sealed_transfer_qr,
            errors=(PackageApiError,), reason=lambda error: error.code,
            PackageLogisticsError=module.PackageLogisticsError)
    assert rejected.value.code == REJECTION_CODE
    assert local == [] and current == before
    assert calls == [("POST", PACKAGE_CREATE_PATH)]


def test_fake_server_exchange_refusal_does_not_query_receipt():
    client, calls = _server(400)
    with pytest.raises(PackageApiError) as rejected:
        client.replace_and_reseal_transfer({"idempotency_key": "residual-exchange",
            "authority_scope_id": "residual-scope", "payload": {"target_bundle_id": "target"}})
    assert rejected.value.code == REJECTION_CODE
    assert len(calls) == 1 and calls[0][0] == "POST"


def test_residual_exchange_refusal_is_not_scheduled_again(tmp_path):
    store = SealedTransferExchangeStore(tmp_path / "exchange.sqlite3")
    prepared = _prepare(SealedTransferExchangeCoordinator(store, FakeClient()))
    row = store.record_error(prepared.intent_id, PackageApiError(
        503, REJECTION_CODE, REJECTION_MESSAGE, retryable=False))
    assert row["status"] == "OPERATOR_REVIEW"
    assert row["last_error_code"] == REJECTION_CODE
    assert store.pending_ids() == []

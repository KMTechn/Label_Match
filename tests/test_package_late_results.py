"""Remote outcomes belong to one claim; late success permits receipt-only recovery."""
import json
import sqlite3
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import pytest

import package_logistics as package
from tests.test_package_logistics import (
    SCOPE, _ack_package_creation, _cancellation_intent, _cancellation_receipt, _draft_for_set,
    _draft, _projection, _receipt,
)


def _saved(tmp_path, kind, *, identity=None):
    draft = _draft() if identity is None else _draft_for_set(identity)
    create = package.PackageOutbox(tmp_path / "p.db")
    client = package.PackageLogisticsClient(
        package.PackageClientConfig("https://synthetic.invalid", "synthetic", SCOPE, "host", "device",
                                    authority_epoch=5, ledger_plane="AUTHORITATIVE", plane_epoch=3),
        transport=lambda *args: {"ok": True, "data": _projection()},
    )
    if kind == "create":
        queued = create.enqueue(draft)
        create.mark_local_completion_committed(queued["idempotency_key"])
        claimed = create.claim_next()
        source, command = client.build_create_package_command(draft, idempotency_key=claimed["idempotency_key"])
        create.save_command(claimed["idempotency_key"], source, command)
        store, receipt = create, _receipt(draft)
        read = lambda: store.get_by_set_id(draft.set_id)
    else:
        _ack_package_creation(create, draft)
        store = package.PackageCancellationOutbox(create.db_path)
        intent = _cancellation_intent(draft)
        store.enqueue(intent)
        store.mark_local_event_committed(intent.cancellation_event_id)
        claimed = store.claim_next()
        command = client.build_cancel_package_command(intent, claimed, idempotency_key=claimed["idempotency_key"])
        store.save_command(claimed["idempotency_key"], command)
        receipt = _cancellation_receipt(draft, intent=intent)
        read = lambda: store.get_by_event_id(intent.cancellation_event_id)
    return draft, create, store, read, receipt


@pytest.mark.parametrize("kind", ["create", "cancel"])
@pytest.mark.parametrize("result", ["acked", "retry", "conflict"])
@pytest.mark.parametrize("changed", ["attempt", "command", "hold"])
def test_result_cas_checks_each_claim_component(tmp_path, kind, result, changed):
    draft, create, store, read, receipt = _saved(tmp_path, kind)
    expected = dict(read())
    if changed == "attempt":
        expected["attempt_count"] -= 1
    elif changed == "command":
        command = json.loads(expected["command_json"])
        command["payload"]["package_bundle_id"] = "DIFFERENT-PACKAGE"
        expected["command_json"] = package.canonical_json(command)
    else:
        create.hold_workbench_set(set_id=draft.set_id, source_phs2="", source_input_tag_id="",
                                  snapshot={"set_id": draft.set_id}, reason="manager hold", held_by="manager")
    before = read()
    if result == "acked":
        applied = store.mark_acked(expected["idempotency_key"], receipt, expected_claim=expected)
    else:
        applied = getattr(store, "mark_" + result)(
            expected["idempotency_key"], package.PackageTransportError("old outcome"), expected_claim=expected)
    assert applied is False
    assert read() == before
    with sqlite3.connect(store.db_path) as conn:
        assert conn.execute("SELECT count(*) FROM package_post_review_outbox").fetchone()[0] == 0
        assert conn.execute("SELECT count(*) FROM package_receipt_rechecks").fetchone()[0] == 0


@pytest.mark.parametrize("kind", ["create", "cancel"])
@pytest.mark.parametrize("new_result", ["conflict", "retry", "success"])
def test_late_success_recovers_new_result_by_receipt_only_after_restart(tmp_path, monkeypatch, kind, new_result):
    _draft_value, _create, store, read, receipt = _saved(tmp_path, kind)
    if kind == "cancel":
        monkeypatch.setattr(package, "_utc_after", lambda seconds: "2000-01-01T00:00:00Z")
    store.mark_retry(read()["idempotency_key"], package.PackageTransportError("initial retry"))
    entered, release = threading.Event(), threading.Event()
    effects, posts, errors = set(), [], []

    class Central:
        def get_receipt_if_exists(self, key, **kwargs):
            return receipt if key in effects else None

        def create_package(self, command):
            posts.append(command["idempotency_key"])
            if len(posts) == 1:
                entered.set()
                from writer_session_fence import remote_request
                with remote_request():
                    assert release.wait(10)
            elif new_result != "success":
                if new_result == "retry":
                    raise package.PackageTransportError("new timeout")
                raise package.PackageApiError(403, "NEW_REJECTED", "new rejection", committed=False)
            effects.add(command["idempotency_key"])
            return receipt

        cancel_package = create_package

    central = Central()
    processor = package.PackageOutboxProcessor if kind == "create" else package.PackageCancellationOutboxProcessor
    first, second = processor(store, central), processor(store, central)
    def old_request():
        try:
            first.drain(limit=1)
        except BaseException as exc:
            errors.append(exc)
    thread = threading.Thread(target=old_request)
    thread.start()
    try:
        assert entered.wait(5)
        if kind == "create":
            store._utc_before = lambda seconds: "9999-01-01T00:00:00Z"
        else:
            monkeypatch.setattr(package, "_utc_before", lambda seconds: "9999-01-01T00:00:00Z")
        second.drain(limit=1)
        before = read()
        release.set()
        thread.join(10)
        assert not thread.is_alive() and not errors
        assert read() == before
        reopened = type(store)(store.db_path)
        assert processor(reopened, central).drain(limit=1)["acked"] == (0 if new_result == "success" else 1)
        assert read()["status"] == "ACKED"
        assert len(effects) == 1 and len(posts) == 2
        with sqlite3.connect(store.db_path) as conn:
            reviews = conn.execute("SELECT count(*) FROM package_post_review_outbox").fetchone()[0]
            assert reviews == (1 if new_result == "conflict" and kind == "create" else 0)
            if new_result != "success":
                audit = conn.execute("SELECT previous_status,verified_at FROM package_receipt_rechecks").fetchone()
                assert audit[0] == before["status"] and audit[1]
    finally:
        release.set()
        thread.join(10)


@pytest.mark.parametrize("kind", ["create", "cancel"])
def test_late_success_for_another_command_cannot_schedule_recheck(tmp_path, kind):
    _draft_value, _create, store, read, receipt = _saved(tmp_path, kind)
    old = dict(read())
    old["command_json"] = package.canonical_json({"idempotency_key": "other-command"})
    store.request_receipt_recheck(old, receipt)
    with sqlite3.connect(store.db_path) as conn:
        assert conn.execute("SELECT count(*) FROM package_receipt_rechecks").fetchone()[0] == 0


@pytest.mark.parametrize("kind", ["create", "cancel"])
def test_receipt_recheck_survives_process_exit_during_lookup(tmp_path, kind):
    _draft_value, _create, store, read, receipt = _saved(tmp_path, kind)
    expected = read()
    store.mark_retry(expected["idempotency_key"], package.PackageTransportError("lost response"), expected_claim=expected)
    store.request_receipt_recheck(expected, receipt)
    before = read()
    name = "PackageOutbox" if kind == "create" else "PackageCancellationOutbox"
    code = f"""
import os, sys
from package_logistics import {name}, {name}Processor
from writer_session_fence import remote_request
class Interrupted:
    def get_receipt_if_exists(self, *args, **kwargs):
        with remote_request():
            os._exit(77)
{name}Processor({name}(sys.argv[1]), Interrupted()).drain(limit=1)
"""
    child = subprocess.Popen([sys.executable, "-s", "-B", "-c", code, store.db_path],
                             creationflags=subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP)
    assert child.wait() == 77
    assert read() == before
    class Central:
        def get_receipt_if_exists(self, *args, **kwargs):
            return receipt
        def create_package(self, command):
            raise AssertionError("receipt recheck must never POST")
        cancel_package = create_package
    processor = package.PackageOutboxProcessor if kind == "create" else package.PackageCancellationOutboxProcessor
    assert processor(type(store)(store.db_path), Central()).drain(limit=1)["acked"] == 1
    assert read()["status"] == "ACKED"


@pytest.mark.parametrize("kind", ["create", "cancel"])
def test_two_failures_preserve_new_retry_and_next_drain_progresses(tmp_path, monkeypatch, kind):
    _draft_value, _create, store, read, receipt = _saved(tmp_path, kind)
    monkeypatch.setattr(package, "_utc_after", lambda seconds: "2000-01-01T00:00:00Z")
    store.mark_retry(read()["idempotency_key"], package.PackageTransportError("initial retry"))
    entered, release = threading.Event(), threading.Event()
    posts, effects, errors = [], set(), []
    class Central:
        def get_receipt_if_exists(self, *args, **kwargs):
            return None
        def create_package(self, command):
            posts.append(command["idempotency_key"])
            if len(posts) == 1:
                from writer_session_fence import remote_request
                entered.set()
                with remote_request():
                    assert release.wait(10)
                raise package.PackageApiError(403, "OLD_REJECTED", "old rejection", committed=False)
            if len(posts) == 2:
                raise package.PackageTransportError("new timeout")
            effects.add(command["idempotency_key"])
            return receipt
        cancel_package = create_package
    processor = package.PackageOutboxProcessor if kind == "create" else package.PackageCancellationOutboxProcessor
    first, second = processor(store, Central()), processor(store, Central())
    def old_request():
        try:
            first.drain(limit=1)
        except BaseException as exc:
            errors.append(exc)
    thread = threading.Thread(target=old_request)
    thread.start()
    try:
        assert entered.wait(5)
        if kind == "create":
            store._utc_before = lambda seconds: "9999-01-01T00:00:00Z"
        else:
            monkeypatch.setattr(package, "_utc_before", lambda seconds: "9999-01-01T00:00:00Z")
        assert second.drain(limit=1)["retry"] == 1
        newer = read()
        release.set()
        thread.join(10)
        assert not thread.is_alive() and not errors
        assert read() == newer and newer["status"] == "PENDING"
        assert second.drain(limit=1)["acked"] == 1
        assert len(effects) == 1 and len(posts) == 3
    finally:
        release.set()
        thread.join(10)


@pytest.mark.parametrize("kind", ["create", "cancel"])
def test_absent_receipt_preserves_marked_sending_claim_without_post(tmp_path, monkeypatch, kind):
    _draft_value, _create, store, read, receipt = _saved(tmp_path, kind)
    old = dict(read())
    old["attempt_count"] -= 1
    store.request_receipt_recheck(old, receipt)
    if kind == "create":
        store._utc_before = lambda seconds: "9999-01-01T00:00:00Z"
    else:
        monkeypatch.setattr(package, "_utc_before", lambda seconds: "9999-01-01T00:00:00Z")
    before = read()
    class Central:
        def get_receipt_if_exists(self, *args, **kwargs):
            return None
        def create_package(self, command):
            raise AssertionError("marked row must never POST")
        cancel_package = create_package
    processor = package.PackageOutboxProcessor if kind == "create" else package.PackageCancellationOutboxProcessor
    assert processor(store, Central()).drain(limit=1)["acked"] == 0
    assert read() == before


def test_f5_late_save_preserves_new_journal_bytes(tmp_path):
    from phs_label_workflow import PHSLabelExchangeJournal, PHSLabelWorkflowError
    path = tmp_path / "journal.json"
    first, second = PHSLabelExchangeJournal(path), PHSLabelExchangeJournal(path)
    first.save({"status": "PREPARE_PENDING"})
    original = first.load()
    second.load()
    second.save({"status": "COMMITTED"})
    before = path.read_bytes()
    with pytest.raises(PHSLabelWorkflowError, match="현품표 교환 기록이 바뀌었습니다"):
        first.save({**original, "status": "PRINT_FAILED"})
    assert path.read_bytes() == before


@pytest.mark.parametrize("route", ["liveness", "delivery"])
def test_runtime_late_invalid_grant_preserves_new_authority(tmp_path, monkeypatch, route):
    import producer_runtime_client as runtime
    from tests.test_producer_runtime_client import (
        _LeaseSession, _credentials, _insert_claimed_row, _prepare,
    )
    db = tmp_path / "runtime.db"
    session = _LeaseSession()
    if route == "delivery":
        _insert_claimed_row(db, "relay")
        invoke = lambda: _prepare(db, "relay", session)
    else:
        invoke = lambda: runtime.ensure_runtime_authority(
            db_path=db, credentials=_credentials(), producer_install_id="install-test",
            session=session, now="2026-08-06T00:00:00Z")
    original = runtime._post_lease_request
    calls = []
    def replaced_request(**kwargs):
        grant, error = original(**kwargs)
        calls.append(1)
        if len(calls) == 1:
            nested = invoke()
            assert nested.error_code == ""
            # The first response is malformed after another writer consumed its request.
            grant = {**grant, "lease_id": ""}
        return grant, error
    monkeypatch.setattr(runtime, "_post_lease_request", replaced_request)
    assert invoke().error_code == ""
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT status,last_error_code FROM direct_sync_runtime_authority").fetchone() == ("ACTIVE", None)


def test_exchange_stale_success_only_schedules_receipt_recheck(tmp_path):
    from sealed_transfer_exchange import SealedTransferExchangeStore, SealedTransferExchangeCoordinator
    from tests.test_sealed_transfer_exchange import FakeClient, _prepare, _receipt as exchange_receipt
    central_receipt = []
    class Client(FakeClient):
        def get_receipt_if_exists(self, *args, **kwargs):
            return central_receipt[0]
        def replace_and_reseal_transfer(self, command):
            raise AssertionError("recheck must never POST")
    store = SealedTransferExchangeStore(tmp_path / "exchange.db")
    coordinator = SealedTransferExchangeCoordinator(store, Client())
    intent = _prepare(coordinator).intent_id
    old = store.load(intent)
    command = coordinator._build_command(old)
    old = store.bind_command(intent, command, expected_state=old)
    newer = store.record_error(intent, package.PackageApiError(403, "REJECTED", "new failure", committed=False), expected_state=old)
    receipt = exchange_receipt(command)
    qr = coordinator._validate_receipt(command, receipt)
    assert dict(store.record_receipt(intent, receipt, qr, expected_state=old)) == dict(newer)
    assert store.receipt_recheck_pending(intent)
    central_receipt.append(receipt)
    assert coordinator.attempt(intent).status == "ACKED"
    assert not store.receipt_recheck_pending(intent)


def test_exchange_numbers_post_before_waiting_for_results(tmp_path):
    from sealed_transfer_exchange import SealedTransferExchangeStore, SealedTransferExchangeCoordinator
    from tests.test_sealed_transfer_exchange import FakeClient, _prepare, _receipt as exchange_receipt
    from writer_session_fence import remote_request
    entered = [threading.Event(), threading.Event()]
    release = [threading.Event(), threading.Event()]
    posts, errors, effects = [], [], set()
    class Client(FakeClient):
        def get_receipt_if_exists(self, *args, **kwargs):
            return None
        def replace_and_reseal_transfer(self, command):
            index = len(posts)
            posts.append(command["idempotency_key"])
            with remote_request():
                entered[index].set()
                assert release[index].wait(10)
            if index == 0:
                raise package.PackageApiError(403, "OLD_REJECTED", "old failure", committed=False)
            effects.add(command["idempotency_key"])
            return exchange_receipt(command)
    store = SealedTransferExchangeStore(tmp_path / "exchange.db")
    actors = [SealedTransferExchangeCoordinator(store, Client()) for _ in range(2)]
    intent = _prepare(actors[0]).intent_id
    def run(actor):
        try:
            actor.attempt(intent)
        except BaseException as error:
            errors.append(error)
    threads = [threading.Thread(target=run, args=(actor,)) for actor in actors]
    try:
        threads[0].start()
        assert entered[0].wait(5)
        old = dict(store.load(intent))
        threads[1].start()
        assert entered[1].wait(5)
        newer = dict(store.load(intent))
        assert newer["attempt_count"] == old["attempt_count"] + 1
        release[0].set()
        threads[0].join(10)
        assert dict(store.load(intent)) == newer
        release[1].set()
        threads[1].join(10)
        assert not errors and store.load(intent)["status"] == "ACKED"
        assert len(effects) == 1
    finally:
        for event in release:
            event.set()
        for thread in threads:
            if thread.ident:
                thread.join(10)


@pytest.mark.parametrize("changed", ["set", "claim", "new_read"])
def test_source_read_callback_cannot_apply_to_new_generation(changed):
    import Label_Match as app_module
    callbacks, writes = [], []
    entered, release = threading.Event(), threading.Event()
    claim = [None]
    app = SimpleNamespace(current_set_info={"id": "SET", "raw": ["physical", "invalid"]})
    app.ui_lane = None
    app.package_outbox = SimpleNamespace(get_by_set_id=lambda set_id: claim[0], get_workbench_hold=lambda set_id: None)
    app._detach_deferred_capture_shell = lambda *args: None
    app._resumable_label_capture = lambda *args: None
    app._capture_or_resume_central_phs2_scan = lambda *args: SimpleNamespace(intent_id="capture")
    app._update_on_success_scan = lambda *args, prepare_durable_state: prepare_durable_state(app.current_set_info)
    app.after = lambda ms, callback: callbacks.append(callback)
    app._known_sample_members = lambda *args: {"known"}
    app._save_current_set_state = lambda **kwargs: writes.append("checkpoint") or True
    app.data_manager = SimpleNamespace(prepare_audit_event=lambda *args: writes.append("audit"))
    app.Events = SimpleNamespace(POST_REVIEW_REQUIRED="review")
    def resolve(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return SimpleNamespace(state_fields=lambda: {"central_verified": True}), {}, None, None
    app._resolve_central_phs2_scan_overlay = resolve
    begin = app_module.Label_Match._begin_scanner_phs2
    assert begin(app, "physical", "item") is True
    assert entered.wait(5)
    original_queue = app._phs2_background_pending["SET"]
    try:
        if changed == "set":
            app.current_set_info = {"id": "OTHER", "raw": ["other"]}
        elif changed == "claim":
            claim[0] = {"idempotency_key": "new-claim", "status": "SENDING", "attempt_count": 1}
        else:
            assert begin(app, "physical", "item") is True
        release.set()
        deadline = time.monotonic() + 5
        while original_queue.empty() and time.monotonic() < deadline:
            time.sleep(0.001)
        assert not original_queue.empty()
        callbacks[0]()
        assert not writes and "central_verified" not in app.current_set_info
        if changed == "new_read":
            assert app._phs2_background_pending["SET"] is not original_queue
    finally:
        release.set()


class PackageLateAdapter:
    """Drive public processors through the shared controlled transport."""
    late_success_policy = "reconcile"

    def __init__(self, root, kind, transport, monkeypatch):
        self.root, self.kind, self.transport = root, kind, transport
        self.rows, self.workers, self.errors = {}, {}, []
        monkeypatch.setattr(package, "_utc_after", lambda seconds: "2000-01-01T00:00:00Z")

    def create_command(self, label):
        folder = self.root / str(len(self.rows))
        folder.mkdir(parents=True)
        _draft_value, _create, store, read, receipt = _saved(folder, self.kind, identity=label)
        key = read()["idempotency_key"]
        store.mark_retry(key, package.PackageTransportError("initial retry"))
        self.rows[key] = (store, read, receipt)
        return key

    def _processor(self, command, captures=None):
        from tests.late_result_guard import ResultKind
        from writer_session_fence import remote_request
        adapter = self
        store, read, receipt = self.rows[command]
        class Central:
            def get_receipt_if_exists(self, key, **kwargs):
                return receipt if adapter.transport.central_effects(key) else None

            def create_package(self, value):
                with remote_request():
                    attempt = adapter.transport.hold(command, read()["attempt_count"])
                    if captures is not None:
                        captures.put(attempt)
                    result = adapter.transport.wait(attempt, timeout=10)
                if result == ResultKind.SUCCESS:
                    return receipt
                if result == ResultKind.NONRETRY_FAILURE:
                    raise package.PackageApiError(403, "SHARED_NONRETRY_FAILURE", "controlled rejection", committed=False)
                if result == ResultKind.RETRY_FAILURE:
                    raise package.PackageApiError(503, "SHARED_RETRY_FAILURE", "controlled retry", retryable=True, committed=False)
                error = package.PackageTransportError("controlled transport failure")
                error.code = "SHARED_" + result.name
                raise error
            cancel_package = create_package
        processor = package.PackageOutboxProcessor if self.kind == "create" else package.PackageCancellationOutboxProcessor
        return processor(store, Central())

    def start_attempt(self, command):
        import queue
        captures = queue.Queue()
        processor = self._processor(command, captures)
        def run():
            try:
                processor.drain(limit=1)
            except BaseException as error:
                self.errors.append(error)
        thread = threading.Thread(target=run)
        thread.start()
        attempt = captures.get(timeout=10)
        self.workers[attempt] = thread
        return attempt

    def deliver(self, attempt, result):
        self.transport.release(attempt, result)
        thread = self.workers[attempt]
        thread.join(10)
        assert not thread.is_alive() and not self.errors

    def expire_attempt(self, attempt):
        store = self.rows[attempt.command][0]
        if self.kind == "create":
            store._utc_before = lambda seconds: "9999-01-01T00:00:00Z"
        else:
            package._utc_before = lambda seconds: "9999-01-01T00:00:00Z"

    def background_once(self):
        from tests.late_result_guard import DrainResult
        work = 0
        for command in self.rows:
            counts = self._processor(command).drain(limit=1)
            work += sum(counts[name] for name in ("acked", "retry", "conflict"))
        return DrainResult(work)

    def snapshot(self, command):
        from tests.late_result_guard import LocalState, ResultKind, Snapshot
        row = self.rows[command][1]()
        states = {"ACKED": LocalState.COMPLETE, "PENDING": LocalState.PENDING,
                  "SENDING": LocalState.SENDING, "CONFLICT": LocalState.NEEDS_REVIEW}
        code = row["last_error_code"] or ""
        reason = ResultKind[code.removeprefix("SHARED_")] if code.startswith("SHARED_") else None
        return Snapshot(states[row["status"]], row["attempt_count"], reason)

    def central_effects(self, command):
        return self.transport.central_effects(command)

    def request_count(self, command):
        return self.transport.request_count(command)

    def audit(self, command):
        from tests.late_result_guard import AuditEvent, AuditKind, ResultKind
        store, read, _receipt_value = self.rows[command]
        row = read()
        with sqlite3.connect(store.db_path) as conn:
            conn.row_factory = sqlite3.Row
            recheck = conn.execute("SELECT * FROM package_receipt_rechecks WHERE idempotency_key=?", (command,)).fetchone()
            review = conn.execute("SELECT 1 FROM package_post_review_outbox WHERE package_idempotency_key=?", (command,)).fetchone()
        code = row["last_error_code"] or (recheck["previous_error_code"] if recheck else "") or ""
        events = []
        if review or code.startswith("SHARED_"):
            reason = ResultKind[code.removeprefix("SHARED_")] if code.startswith("SHARED_") else ResultKind.NONRETRY_FAILURE
            kind = AuditKind.REVIEW if reason == ResultKind.NONRETRY_FAILURE else AuditKind.FAILURE
            events.append(AuditEvent(row["attempt_count"], kind, reason))
        if recheck and recheck["verified_at"]:
            events.append(AuditEvent(row["attempt_count"], AuditKind.CORRECTION, ResultKind.SUCCESS))
        return tuple(events)

    def reconcile(self, command):
        self._processor(command).drain(limit=1)


@pytest.mark.parametrize("scenario", ["A1", "A2", "A3", "A4", "B1", "C1", "D1"])
def test_shared_late_result_contract(late_result_adapter_factory, scenario):
    from tests.late_result_guard import run_late_result_scenarios
    assert run_late_result_scenarios(late_result_adapter_factory, scenario) == (scenario,)

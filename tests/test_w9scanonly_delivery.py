"""Scanner state and background delivery boundaries from CONTRACT v1.2."""
import copy
import json
from types import SimpleNamespace

import pytest

import carrier_identity_port as carrier
from package_logistics import PackageApiError, PackageOutboxProcessor
from tests.test_legacy_label_transition import MASTER, PHS2_LABEL
from tests.test_w9scanonly_packaging import packaging, scan, products, completions  # noqa: F401
from tests.test_w9scanonly_central import central  # noqa: F401
from tests.test_deferred_lease_clock_retry import clock_case  # noqa: F401


@pytest.mark.parametrize("quantity", ["1", "60", "9999"])
def test_seven_field_label_accepts_quantity(quantity):
    assert carrier.parse_compact_carrier(PHS2_LABEL + "|QT=" + quantity)["QT"] == quantity


@pytest.mark.parametrize("quantity", ["0", "01", "+1", " 1", "1 ", "1.0", "10000", "１"])
def test_seven_field_label_rejects_invalid_quantity(quantity):
    with pytest.raises(ValueError):
        carrier.parse_compact_carrier(PHS2_LABEL + "|QT=" + quantity)


def test_new_label_four_short_products_have_no_final_date_condition(central):
    app, dialogs, case, root, _releases = central
    app.package_logistics_client.resolve_package_source_evidence = lambda draft: (_ for _ in ()).throw(PackageApiError(503, "OFFLINE", "offline"))
    for value in [case.group["scan_payload"], *[case.group["item_id"] + f"-P{i}" for i in range(4)]]:
        scan(app, value)
    rows = completions(app)
    assert len(rows) == 1 and rows[0][1]["final_result"] == app.Results.PASS
    assert len(rows[0][1]["product_sample_barcodes"]) == 4
    assert [row["barcode_role"] for row in rows[0][1]["barcode_roles"]] == ["material_master_label", *["product"] * 4]
    assert dialogs == []
    root.run_until(lambda: not app.__dict__.get("_phs2_background_pending"), timeout=5)


def test_undo_unfinished_product_preserves_the_label_and_order(packaging):
    app, dialogs = packaging
    values = products()
    for value in [MASTER, *values[:2], "KMCMD:UNDO"]:
        scan(app, value)
    assert app.current_set_info["raw"] == [MASTER, values[0]]
    assert completions(app) == [] and dialogs == []


def test_unsubmitted_legacy_phs2_save_completes_in_the_scanner_outbox(central):
    app, dialogs, case, root, _releases = central
    master = case.group["scan_payload"]
    values = [row["normalized_barcode"] for row in case.snapshot["work_group_source"]["members"]]
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get("_phs2_background_pending"), timeout=5)
    identity = app.current_set_info["id"]
    app.current_set_info.pop("scanner_only")
    app.current_set_info["operation_lease_id"] = "preserved-old-prefetch"
    app.package_logistics_client.issue_operation_lease = lambda **kw: pytest.fail("local completion must not acquire a lease")
    for value in values:
        scan(app, value)
    assert app.blocks == []
    assert len(completions(app)) == 1
    row = app.package_outbox.get_by_set_id(identity)
    assert row is not None and not row.get("command_json")
    draft = json.loads(row["draft_json"])
    assert draft["scanner_only"] is True
    assert draft["sample_barcodes"] == values
    assert draft["scan_payload"] == master and draft["local_completed_at"]
    assert not draft.get("operation_lease_id")
    assert dialogs == []


@pytest.mark.parametrize("trigger", ["admission", "scheduled_worker"])
def test_restored_unsubmitted_phs2_does_not_admit_a_blocking_validation_lane(central, trigger):
    import threading
    from tk_serial_ui_lane import LaneTask
    app, dialogs, case, root, releases = central
    master = case.group["scan_payload"]
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get("_phs2_background_pending"), timeout=5)
    saved = copy.deepcopy(app.current_set_info)
    saved.pop("scanner_only")
    app.current_set_info = app._migrate_restored_central_package_state(saved)
    app._last_scan_enter_at = None
    app._scans_behind_completion = False
    entered, release = threading.Event(), threading.Event()
    releases.append(release)
    def offline_validation():
        entered.set()
        release.wait()
    task = LaneTask(name="deferred-validation", generation=app._ui_lane_generation,
                    work=offline_validation, finish=lambda value: None, fail=lambda error: None)
    rescheduled = []
    app._schedule_deferred_validation_worker = rescheduled.append
    if trigger == "admission":
        admission = app._submit_deferred_validation_lane_task(task)
        assert admission.accepted is False
    else:
        app._run_deferred_validation_worker_once()
    assert rescheduled == [1000]
    assert not app.ui_lane.is_busy() and not entered.is_set()
    for member in case.snapshot["work_group_source"]["members"]:
        scan(app, member["normalized_barcode"])
    assert len(completions(app)) == 1 and app.blocks == [] and dialogs == []


def test_submitted_legacy_phs2_save_preserves_its_original_outbox_draft(central):
    from Label_Match import _label_match_package_draft
    app, dialogs, case, root, _releases = central
    master = case.group["scan_payload"]
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get("_phs2_background_pending"), timeout=5)
    app.current_set_info.pop("scanner_only")
    identity = app.current_set_info["id"]
    original = app.package_outbox.enqueue(_label_match_package_draft(app.current_set_info, item_code=case.group["item_id"]))
    scan(app, case.snapshot["work_group_source"]["members"][0]["normalized_barcode"])
    assert app.current_set_info["raw"] == [master]
    assert not app.current_set_info.get("scanner_only")
    assert app.package_outbox.get_by_set_id(identity)["draft_json"] == original["draft_json"]
    assert completions(app) == [] and dialogs == []


def test_unknown_command_never_becomes_a_product_or_an_error_gate(packaging):
    app, dialogs = packaging
    for value in [MASTER, "KMCMD:OTHER", products()[0]]:
        scan(app, value)
    assert app.current_set_info["raw"] == [MASTER, products()[0]]
    assert not app.__dict__.get("_pending_workflow_error")
    assert dialogs == []


def test_short_confirmation_has_no_outbox_until_next_label(central):
    app, dialogs, case, root, _releases = central
    master = case.group["scan_payload"]
    values = [row["normalized_barcode"] for row in case.snapshot["work_group_source"]["members"]]
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get("_phs2_background_pending"), timeout=5)
    identity = app.current_set_info["id"]
    scan(app, values[0])
    scan(app, master)
    assert len(completions(app)) == 1
    assert app.package_outbox.get_by_set_id(identity) is None
    scan(app, "KMCMD:UNDO")
    assert app.current_set_info["raw"] == [master, values[0]]
    assert app.package_outbox.get_by_set_id(identity) is None
    for value in values[1:]:
        scan(app, value)
    assert app.blocks == [], [str(error) for error in app.blocks]
    assert len(completions(app)) == 2, (app.current_set_info, app.errors)
    assert completions(app)[-1][1]["scanned_product_barcodes"] == [master, *values]
    assert dialogs == []


def test_delivery_carries_four_samples_and_retries_upstream_dependency(central):
    app, dialogs, case, root, _releases = central
    master = case.group["scan_payload"]
    values = [row["normalized_barcode"] for row in case.snapshot["work_group_source"]["members"]]
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get("_phs2_background_pending"), timeout=5)
    identity = app.current_set_info["id"]
    for value in values:
        scan(app, value)
    calls = []
    def prepare(draft):
        calls.append("prepare")
        return draft
    def build(draft, **kwargs):
        calls.append("build")
        return "TRANSFER-SYNTHETIC", {"idempotency_key": kwargs["idempotency_key"], "payload": {"sample_barcodes": list(draft.sample_barcodes)}}
    def send(command):
        calls.append(copy.deepcopy(command))
        raise PackageApiError(409, "UPSTREAM_COMPLETION_PENDING", "waiting", retryable=True, committed=False)
    client = SimpleNamespace(build_create_package_command=build, create_package=send)
    processor = PackageOutboxProcessor(app.package_outbox, client, prepare_scanner_delivery=prepare)
    assert processor.drain(limit=1) == {"acked": 0, "retry": 1, "conflict": 0}
    row = app.package_outbox.get_by_set_id(identity)
    payload = json.loads(row["command_json"])["payload"]
    assert payload["sample_barcodes"] == sorted(values)
    assert payload["deferred_reflection"] is True and payload["short_confirmed"] is False
    assert payload["local_completion_id"] == identity and payload["local_completed_at"]
    assert payload["scan_payload"] == master
    assert row["local_completion_committed"] == 1 and row["status"] == "PENDING"
    assert calls[:2] == ["prepare", "build"] and dialogs == []


def test_background_delivery_attaches_a_verified_signed_lease_after_local_completion(central):
    from datetime import datetime, timezone
    from package_command_draft import PackageCommandDraft
    app, dialogs, case, root, _releases = central
    case.instant[0] = datetime(2026, 9, 5, 3, 28, 54, tzinfo=timezone.utc)
    master = case.group["scan_payload"]
    for value in [master, *[row["normalized_barcode"] for row in case.snapshot["work_group_source"]["members"]]]:
        scan(app, value)
    row = app.package_outbox.get_by_set_id(completions(app)[-1][1]["set_id"])
    assert row["local_completion_committed"] == 1 and case.calls == []
    draft = PackageCommandDraft.from_dict(json.loads(row["draft_json"]))
    prepared = app._prepare_scanner_package_delivery(draft)
    assert prepared.operation_lease_id == case.claims["lease_id"]
    assert prepared.scan_payload == master and prepared.sample_barcodes == draft.sample_barcodes
    assert case.calls[0]["deferred_reflection"] is True
    assert case.calls[0]["local_completion_id"] == draft.set_id
    assert case.calls[0]["local_completed_at"] == draft.local_completed_at
    assert app.package_operation_lease_store.get(lease_id=prepared.operation_lease_id)["status"] == "LOCAL_COMPLETED"
    assert dialogs == []
    root.run_until(lambda: not app.__dict__.get("_phs2_background_pending"), timeout=5)




@pytest.mark.parametrize("short", [False, True])
def test_background_capture_scheduler_preserves_unfinished_scanner_work(central, short):
    from tk_serial_ui_lane import LaneTask
    app, _dialogs, case, root, _releases = central
    master = case.group["scan_payload"]
    scan(app, master)
    if short:
        scan(app, case.snapshot["work_group_source"]["members"][0]["normalized_barcode"])
        app._scanner_previous_scan = None
        scan(app, master)
        assert app.current_set_info["shortage_undo_snapshot"]
    calls = []
    app._schedule_deferred_validation_worker = calls.append
    app._last_scan_enter_at = None
    task = LaneTask("deferred-validation", 0, lambda: None, lambda value: None, lambda error: None)
    admission = app._submit_deferred_validation_lane_task(task)
    assert not admission.accepted and admission.reason == "scan_in_progress"
    app._run_deferred_validation_worker_once()
    assert calls == [1000, 1000]
    assert case.calls == []
    root.run_until(lambda: not app.__dict__.get("_phs2_background_pending"), timeout=5)


def test_short_confirmation_checkpoint_failure_retries_one_csv_without_server_effect(central):
    app, dialogs, case, root, _releases = central
    master = case.group["scan_payload"]
    sample = case.snapshot["work_group_source"]["members"][0]["normalized_barcode"]
    for value in (master, sample):
        scan(app, value)
    root.run_until(lambda: not app.__dict__.get("_phs2_background_pending"), timeout=5)
    identity = app.current_set_info["id"]
    saved = app.data_manager.save_current_state
    attempts = []
    def fail_idle(state):
        current = state["current_set_info"]
        attempts.append(copy.deepcopy(current))
        return False if not current.get("raw") else saved(state)
    app.data_manager.save_current_state = fail_idle
    scan(app, master)
    assert len(completions(app)) == 1 and app.package_outbox.get_by_set_id(identity) is None
    pending = app.data_manager.load_current_state()["current_set_info"]
    assert pending["shortage_confirmation_pending"] and pending["shortage_undo_snapshot"]["raw"] == [master, sample]
    assert not app.set_details_map
    assert "pass" not in app.sounds
    app.data_manager.save_current_state = saved
    # Recover the durable decision with the same completion ID and CSV idempotency.
    app.current_set_info = pending
    from types import MethodType
    app._return_to_idle_after_finalized_set = MethodType(type(app)._return_to_idle_after_finalized_set, app)
    app._refresh_session_tree = lambda: None
    app._idle_instruction_text = lambda: "다음 현품표"
    app.entry.focus_set = lambda: None
    if isinstance(pending.get("start_time"), str):
        from datetime import datetime
        pending["start_time"] = datetime.fromisoformat(pending["start_time"])
    app._scanner_shortage_undo = pending["shortage_undo_snapshot"]
    assert app._finalize_set(app.Results.PASS)
    assert len(completions(app)) == 1
    assert app.current_set_info["shortage_undo_snapshot"]["raw"] == [master, sample]
    assert app.package_outbox.get_by_set_id(identity) is None and case.calls == []
    scan(app, "KMCMD:UNDO")
    assert app.current_set_info["raw"] == [master, sample] and dialogs == []


@pytest.mark.parametrize("split", [False, True])
@pytest.mark.parametrize("corrupt", [None, "old_seal_token_hash", "new_members", "seal_token_hash", "target_version_after", "new_source_kind", "lost_ack"])
def test_scanner_exchange_preserves_exact_receipt_checks(tmp_path, corrupt, split):
    from sealed_transfer_exchange import SealedTransferExchangeStore, SealedTransferExchangeCoordinator
    from package_logistics import PackageLogisticsError
    from tests.test_sealed_transfer_exchange import _target_projection, _receipt, ITEM, SCOPE
    key = "scanner-exchange-fixed-key"
    pair = {"idempotency_key": key, "target_barcode": "BC-OLD", "replacement_barcode": "BC-NEW",
            "operator": "worker", "occurred_at": "2026-09-30T14:00:00Z"}
    commands, saved_receipts = [], []
    def send(command):
        commands.append(copy.deepcopy(command))
        legacy = copy.deepcopy(command)
        legacy["payload"]["pairs"] = [{"old_unit_id": "unit-old", "new_unit_id": "unit-new", "new_source_bundle_id": None}]
        receipt = _receipt(legacy)
        data = receipt["data"]
        data["pairs"][0].update(old_barcode="BC-OLD", new_barcode="BC-NEW", new_source_kind="EXCHANGE_GOOD_REGISTERED")
        data["source_membership_changes"] = []
        if corrupt == "new_source_kind":
            data["pairs"][0][corrupt] = "UNKNOWN"
        elif corrupt and corrupt != "lost_ack":
            data[corrupt] = [] if corrupt == "new_members" else "wrong"
        saved_receipts.append(copy.deepcopy(receipt))
        if corrupt == "lost_ack":
            from package_logistics import PackageTransportError
            raise PackageTransportError("ACK lost after server commit")
        return receipt
    client = SimpleNamespace(resolve_package_source_projection=lambda draft: _target_projection(),
        get_receipt_if_exists=lambda *args, **kwargs: saved_receipts[-1] if corrupt == "lost_ack" and saved_receipts else None, replace_and_reseal_transfer=send)
    if split:
        projection = _target_projection()
        target_unit = next(member["unit_id"] for member in projection["members"] if member["normalized_barcode"] == pair["target_barcode"])
        group = {"authority_scope_id": SCOPE, "members": projection["members"], "source_transfers": [
            {"bundle_id": projection["bundle_id"], "selected_member_ids": [target_unit],
             "source_member_ids": projection["member_ids"], "entity_version": projection["entity_version"],
             "active_seal": projection["active_seal"]},
            {"bundle_id": "another-transfer", "selected_member_ids": ["another-unit"]}]}
        client.resolve_package_source_evidence = lambda draft: {"source_resolution_basis": "PHS_WORK_GROUP_EXACT_MEMBERSHIP", "work_group_source": group}
        def get_bundle(bundle_id, **kwargs):
            assert bundle_id == projection["bundle_id"] and kwargs["authority_scope_id"] == SCOPE
            return projection
        client.get_bundle = get_bundle
    store = SealedTransferExchangeStore(tmp_path / "exchange.sqlite3")
    coordinator = SealedTransferExchangeCoordinator(store, client)
    args = dict(physical_qr=f"PHS=2|SRC=KMTECH_INPUT_TAG|ITG=TAG-1|CLC={ITEM}|LBL=LABEL-1|HSH=0123456789abcdef",
        item_code=ITEM, local_completion_id="local-scanner-1", local_completed_at="2026-09-30T14:01:00Z")
    if corrupt == "lost_ack":
        from package_logistics import PackageTransportError
        with pytest.raises(PackageTransportError):
            coordinator.deliver_scanner_exchange(pair, **args)
        assert store.blocking_rows(set_id="local-scanner-1")[0]["status"] == "RETRY_WAIT"
        assert [attempt.status for attempt in coordinator.drain_pending()] == ["ACKED"]
        assert store.pending_local(set_id="local-scanner-1") == []
        assert coordinator.deliver_scanner_exchange(pair, **args)["local_apply_status"] == "APPLIED"
        assert len(commands) == 1
    elif corrupt:
        with pytest.raises(PackageLogisticsError):
            coordinator.deliver_scanner_exchange(pair, **args)
        assert store.blocking_rows(set_id="local-scanner-1")[0]["status"] == "OPERATOR_REVIEW"
    else:
        result = coordinator.deliver_scanner_exchange(pair, **args)
        assert result["status"] == "ACKED" and result["local_apply_status"] == "APPLIED"
        assert coordinator.deliver_scanner_exchange(pair, **args)["command_id"] == key
        assert len(commands) == 1
        assert commands[0]["payload"]["replacement_mode"] == "SCAN_REGISTER_GOOD"
        assert commands[0]["payload"]["target_evidence"]["member_count"] == 2

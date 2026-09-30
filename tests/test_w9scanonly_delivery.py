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


def test_background_delivery_attaches_a_verified_signed_lease_after_local_completion(central, monkeypatch):
    from datetime import datetime, timezone
    from package_command_draft import PackageCommandDraft
    app, dialogs, case, root, _releases = central
    case.instant[0] = datetime(2026, 9, 5, 3, 28, 54, tzinfo=timezone.utc)
    monkeypatch.setitem(app._prepare_scanner_package_delivery.__func__.__globals__, "datetime",
                        case.app._acquire_operation_lease.__func__.__globals__["datetime"])
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

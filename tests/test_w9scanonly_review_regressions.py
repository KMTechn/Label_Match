"""Independent review reproductions of atomic scanner exchange and undo."""

import copy
import json
import threading

import pytest

from tests.test_w9scanonly_central import central  # noqa: F401
from tests.test_deferred_lease_clock_retry import clock_case  # noqa: F401
from tests.test_w9scanonly_packaging import packaging, scan, completions, products  # noqa: F401
from tests.test_legacy_label_transition import MASTER, _events


def test_unsampled_exchange_append_failure_restores_an_undoable_sample(central):
    app, dialogs, case, root, releases = central
    master=case.group['scan_payload']
    target=case.snapshot['work_group_source']['members'][0]['normalized_barcode']
    replacement=case.group['item_id']+'-NEW-SAMPLE'
    scan(app,master)
    root.run_until(lambda: not app.__dict__.get('_phs2_background_pending'),timeout=5)
    scan(app,'KMCMD:EXCHANGE')
    scan(app,target)
    original=app.data_manager.log_event
    def append_failure(event,details):
        if details.get('scanner_exchange'):
            raise OSError('review injected event append failure after pair checkpoint')
        return original(event,details)
    app.data_manager.log_event=append_failure
    scan(app,replacement)
    app.data_manager.log_event=original
    saved=app.data_manager.load_current_state()['current_set_info']
    assert saved['scanner_exchanges'][0]['replacement_barcode']==replacement
    app.current_set_info=saved
    app._workflow_blocking_notice=None
    app._undo_scanner_input()
    assert app.current_set_info['raw']==[master]
    assert not app.current_set_info.get('scanner_exchanges')
    assert dialogs==[]


@pytest.mark.parametrize('member_index',[0,1])
def test_label_exchange_member_cannot_replace_itself(central,member_index):
    app, dialogs, case, root, releases=central
    master=case.group['scan_payload']
    target=case.snapshot['work_group_source']['members'][0]['normalized_barcode']
    scan(app,master)
    root.run_until(lambda: not app.__dict__.get('_phs2_background_pending'),timeout=5)
    scan(app,'KMCMD:EXCHANGE')
    scan(app,target)
    scan(app,case.snapshot['work_group_source']['members'][member_index]['normalized_barcode'])
    assert not app.current_set_info.get('scanner_exchanges'), 'same sealed source member accepted as its own replacement'
    assert app.current_set_info['raw']==[master]


def test_preserved_five_scans_can_be_undone_after_completion_save_failure(central):
    app, dialogs, case, root, releases=central
    master=case.group['scan_payload']
    members=[r['normalized_barcode'] for r in case.snapshot['work_group_source']['members']]
    scan(app,master)
    root.run_until(lambda: not app.__dict__.get('_phs2_background_pending'),timeout=5)
    for value in members[:3]: scan(app,value)
    commit=app._commit_finalized_set_durable
    app._commit_finalized_set_durable=lambda **kw: (_ for _ in ()).throw(OSError('review injected precommit failure'))
    scan(app,members[3])
    app._commit_finalized_set_durable=commit
    assert app.current_set_info['raw']==[master,*members]
    app._undo_scanner_input()
    assert app.current_set_info['raw']==[master,*members[:3]]
    assert completions(app)==[]


@pytest.mark.parametrize("sampled", [False, True])
@pytest.mark.parametrize("cut", ["checkpoint", "event", "flush"])
def test_exchange_checkpoint_contains_the_product_and_undo_position(central, monkeypatch, sampled, cut):
    app, dialogs, case, root, _releases = central
    master = case.group["scan_payload"]
    target = case.snapshot["work_group_source"]["members"][0]["normalized_barcode"]
    replacement = case.group["item_id"] + "-ATOMIC-REPLACEMENT"
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get("_phs2_background_pending"), timeout=5)
    if sampled:
        scan(app, target)
    scan(app, "KMCMD:EXCHANGE")
    scan(app, target)
    before = copy.deepcopy(app.current_set_info)
    feedback = []
    monkeypatch.setattr(app, "_play_sound", lambda sound: feedback.append(sound))
    monkeypatch.setattr(app, "update_big_display", lambda *args: feedback.append(args))
    event = app.data_manager.log_event
    flush = app.data_manager.flush
    save = app._save_current_set_state

    def fail_event(name, details):
        if details.get("scanner_exchange"):
            raise OSError("exchange event interrupted")
        return event(name, details)

    def fail_flush(**kwargs):
        raise OSError("exchange flush interrupted")

    if cut == "checkpoint":
        monkeypatch.setattr(app, "_save_current_set_state", lambda: False)
    elif cut == "event":
        monkeypatch.setattr(app.data_manager, "log_event", fail_event)
    else:
        monkeypatch.setattr(app.data_manager, "flush", fail_flush)
    scan(app, replacement)
    monkeypatch.setattr(app.data_manager, "log_event", event)
    monkeypatch.setattr(app.data_manager, "flush", flush)
    monkeypatch.setattr(app, "_save_current_set_state", save)
    saved = app.data_manager.load_current_state()["current_set_info"]
    assert feedback == [], "interrupted exchange must not report success"
    if cut == "checkpoint":
        assert app.current_set_info == before
        assert saved["raw"] == before["raw"]
        assert not saved.get("scanner_exchanges")
    else:
        assert saved["raw"] == [master, replacement]
        assert len(saved["parsed"]) == len(saved["raw"])
        pair = saved["scanner_exchanges"][-1]
        assert pair["sample_index"] == 1 and pair["replaced_sample"] is sampled
        assert saved["scanner_undo_exchange"] == pair["idempotency_key"]
        app.current_set_info = saved
        app._workflow_blocking_notice = None
        app._undo_scanner_input()
        assert app.current_set_info["raw"] == ([master, target] if sampled else [master])
        assert not app.current_set_info.get("scanner_exchanges")
    assert dialogs == []


def test_undo_accepts_a_historical_pair_only_checkpoint(central):
    app, _dialogs, case, root, _releases = central
    master = case.group["scan_payload"]
    target = case.snapshot["work_group_source"]["members"][0]["normalized_barcode"]
    replacement = case.group["item_id"] + "-HISTORICAL-REPLACEMENT"
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get("_phs2_background_pending"), timeout=5)
    for value in ["KMCMD:EXCHANGE", target, replacement]:
        scan(app, value)
    # The rejected build could persist a pair before appending its product.
    app.current_set_info["raw"] = [master]
    app.current_set_info["parsed"] = [case.group["item_id"]]
    assert app._save_current_set_state()
    app._undo_scanner_input()
    assert app.current_set_info["raw"] == [master]
    assert not app.current_set_info.get("scanner_exchanges")


def test_exchanged_out_member_is_not_a_second_exchange_target(central):
    app, _dialogs, case, root, _releases = central
    master = case.group["scan_payload"]
    target = case.snapshot["work_group_source"]["members"][0]["normalized_barcode"]
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get("_phs2_background_pending"), timeout=5)
    for value in ["KMCMD:EXCHANGE", target, case.group["item_id"] + "-NEW", "KMCMD:EXCHANGE"]:
        scan(app, value)
    before = copy.deepcopy(app.current_set_info)
    scan(app, target)
    assert app.current_set_info == before


def test_prior_defective_target_cannot_return_as_another_replacement(central):
    app, _dialogs, case, root, _releases = central
    master = case.group["scan_payload"]
    members = case.snapshot["work_group_source"]["members"]
    old, other = [row["normalized_barcode"] for row in members[:2]]
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get("_phs2_background_pending"), timeout=5)
    for value in ["KMCMD:EXCHANGE", old, case.group["item_id"] + "-NEW",
                  "KMCMD:EXCHANGE", other]:
        scan(app, value)
    before = copy.deepcopy(app.current_set_info)
    scan(app, old)
    assert app.current_set_info == before


def test_late_source_rejects_an_exchanged_out_product_rescanned_while_offline(central):
    app, dialogs, case, root, releases = central
    entered, release = threading.Event(), threading.Event()
    releases.append(release)
    resolve = app.package_logistics_client.resolve_package_source_evidence

    def delayed(draft):
        entered.set()
        release.wait()
        return resolve(draft)

    app.package_logistics_client.resolve_package_source_evidence = delayed
    master = case.group["scan_payload"]
    target = case.snapshot["work_group_source"]["members"][0]["normalized_barcode"]
    replacement = case.group["item_id"] + "-NEW-GOOD"
    scan(app, master)
    root.run_until(entered.is_set, timeout=5)
    for value in (target, "KMCMD:EXCHANGE", target, replacement, target):
        scan(app, value)
    assert app.current_set_info["raw"] == [master, replacement, target]
    release.set()
    root.run_until(lambda: not app.__dict__.get("_phs2_background_pending"), timeout=5)
    assert app.data_manager.flush(timeout=5)
    reviews = _events(app.save_directory, app.Events.POST_REVIEW_REQUIRED)
    assert any(details.get("reason_code") == "PACKAGE_SAMPLE_NOT_MEMBER" for _, details in reviews)
    assert app.current_set_info["raw"] == [master, replacement, target]
    assert dialogs == []


def test_undo_event_failure_keeps_memory_at_the_saved_checkpoint(packaging, monkeypatch):
    app, _dialogs = packaging
    for value in [MASTER, products()[0]]:
        scan(app, value)
    original = app.data_manager.log_event
    def interrupt(name, details):
        if name == app.Events.SET_RESTORED:
            raise OSError("undo event interrupted after checkpoint")
        return original(name, details)
    monkeypatch.setattr(app.data_manager, "log_event", interrupt)
    scan(app, "KMCMD:UNDO")
    saved = app.data_manager.load_current_state()["current_set_info"]
    assert saved["raw"] == [MASTER]
    assert app.current_set_info["raw"] == saved["raw"]
    assert app.blocks


def test_exchange_audit_failure_cannot_lose_the_replacement_to_a_later_scan(central, monkeypatch):
    app, dialogs, case, root, _releases = central
    master = case.group["scan_payload"]
    members = [row["normalized_barcode"] for row in case.snapshot["work_group_source"]["members"]]
    replacement = case.group["item_id"] + "-NEW-GOOD"
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get("_phs2_background_pending"), timeout=5)
    for value in ("KMCMD:EXCHANGE", members[0]):
        scan(app, value)
    log = app.data_manager.log_event

    def interrupted(event, details):
        if details.get("scanner_exchange"):
            raise OSError("synthetic interrupted exchange audit")
        return log(event, details)

    monkeypatch.setattr(app.data_manager, "log_event", interrupted)
    scan(app, replacement)
    pair = copy.deepcopy(app.current_set_info["scanner_exchanges"][0])
    monkeypatch.setattr(app.data_manager, "log_event", log)
    scan(app, members[1])
    assert app.current_set_info["raw"] == [master, replacement, members[1]]
    assert app.current_set_info["scanner_exchanges"] == [pair]
    assert app.data_manager.load_current_state()["current_set_info"]["scanner_exchanges"] == [pair]
    assert dialogs == []


def test_six_field_label_completes_when_unsampled_exchange_fills_the_fourth_product(central):
    app, dialogs, case, root, _releases = central
    master = case.group["scan_payload"]
    assert "|QT=" not in master and len(master.split("|")) == 6
    members = [row["normalized_barcode"] for row in case.snapshot["work_group_source"]["members"]]
    replacement = case.group["item_id"] + "-NEW-GOOD"
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get("_phs2_background_pending"), timeout=5)
    for value in (*members[:3], "KMCMD:EXCHANGE", members[3], replacement):
        scan(app, value)
    rows = completions(app)
    assert len(rows) == 1 and rows[0][1]["final_result"] == app.Results.PASS
    assert rows[0][1]["scanned_product_barcodes"] == [master, *members[:3], replacement]
    assert rows[0][1]["scanner_exchanges"][0]["replacement_barcode"] == replacement
    assert app.current_set_info["raw"] == []
    assert dialogs == []


def test_restored_exchange_survives_later_products_and_is_republished_to_delivery(central, tmp_path, monkeypatch):
    import Label_Match as lm
    from package_logistics import PackageCommandDraft, PackageTransportError
    from types import SimpleNamespace
    from tests.test_legacy_label_transition import _app_for_recovery, _close

    app, _dialogs, case, root, _releases = central
    master = case.group["scan_payload"]
    members = [row["normalized_barcode"] for row in case.snapshot["work_group_source"]["members"]]
    replacement = case.group["item_id"] + "-NEW-GOOD"
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get("_phs2_background_pending"), timeout=5)
    for value in ("KMCMD:EXCHANGE", members[0], replacement):
        scan(app, value)
    pair = copy.deepcopy(app.current_set_info["scanner_exchanges"][0])
    set_id = app.current_set_info["id"]
    _close(app)
    restored = _app_for_recovery(lm, tmp_path, monkeypatch)
    for name in ("deferred_intent_capture", "package_logistics_client", "package_outbox",
                 "package_operation_lease_store", "package_operation_lease_keyring"):
        setattr(restored, name, getattr(case.app, name))
    restored.items_data.update(app.items_data)
    restored.operator_workbench_ready = True
    restored._render_operator_workbench = lambda: SimpleNamespace(scan_input_enabled=True)
    restored._start_package_outbox_drain = lambda: None
    try:
        restored._load_current_set_state()
        assert restored.current_set_info["scanner_exchanges"] == [pair]
        for value in members[1:]:
            scan(restored, value)
        assert len(completions(restored)) == 1
        row = restored.package_outbox.get_by_set_id(set_id)
        draft = PackageCommandDraft.from_dict(json.loads(row["draft_json"]))
        assert list(draft.scanner_exchanges) == [pair] and row["local_completion_committed"] == 1
        sent = []

        def unavailable(saved_pair, **context):
            sent.append((saved_pair, context))
            raise PackageTransportError("synthetic disconnected delivery after restored intent")

        restored.sealed_transfer_exchange_coordinator = SimpleNamespace(deliver_scanner_exchange=unavailable)
        with pytest.raises(PackageTransportError):
            restored._prepare_scanner_package_delivery(draft)
        assert sent[0][0] == pair and sent[0][1]["local_completion_id"] == set_id
        assert restored.package_outbox.get_by_set_id(set_id)["idempotency_key"] == row["idempotency_key"]
    finally:
        _close(restored)


def test_retry_after_undo_audit_failure_does_not_complete_an_unfinished_set(packaging, monkeypatch):
    app, _dialogs = packaging
    for value in (MASTER, products()[0]):
        scan(app, value)
    log = app.data_manager.log_event

    def interrupted(event, details):
        if event == app.Events.SET_RESTORED:
            raise OSError("synthetic interrupted undo audit")
        return log(event, details)

    monkeypatch.setattr(app.data_manager, "log_event", interrupted)
    scan(app, "KMCMD:UNDO")
    monkeypatch.setattr(app.data_manager, "log_event", log)
    app._retry_blocked_submission()
    assert completions(app) == []
    assert app.current_set_info["raw"] == [MASTER]
    scan(app, products()[1])
    assert app.current_set_info["raw"] == [MASTER, products()[1]]




@pytest.mark.parametrize('action',['sample','exchange'])
def test_saved_completion_intent_blocks_later_input_without_mutating_outbox(central,action):
    from package_logistics import PackageTransportError
    app, dialogs, case, root, releases=central
    master=case.group['scan_payload']
    members=[r['normalized_barcode'] for r in case.snapshot['work_group_source']['members']]
    app.package_logistics_client.resolve_package_source_evidence=lambda draft: (_ for _ in ()).throw(PackageTransportError('review offline membership'))
    scan(app,master)
    root.run_until(lambda: not app.__dict__.get('_phs2_background_pending'),timeout=5)
    for value in members[:3]: scan(app,value)
    identity=app.current_set_info['id']
    original=app.data_manager.log_event
    def append_failure(event,details):
        if event==app.Events.TRAY_COMPLETE:
            raise OSError('review injected completion CSV failure after outbox intent')
        return original(event,details)
    app.data_manager.log_event=append_failure
    scan(app,members[3])
    app.data_manager.log_event=original
    row=app.package_outbox.get_by_set_id(identity)
    assert row is not None and not row['local_completion_committed']
    assert completions(app)==[]
    app._undo_scanner_input()
    replacement=case.group['item_id']+'-ACTUAL-FOURTH-SAMPLE'
    if action=='sample':
        scan(app,replacement)
    else:
        for value in ['KMCMD:EXCHANGE',members[0],replacement,members[3]]: scan(app,value)
    rows=completions(app)
    # The original hypothesis assumed the following scans were admitted. The
    # product's pending-command gate rejects them, preserving the old intent.
    assert rows==[]
    assert app.current_set_info['raw']==[master,*members[:3]]
    draft=json.loads(app.package_outbox.get_by_set_id(identity)['draft_json'])
    assert set(draft['sample_barcodes'])==set(members)
    assert app.package_outbox.get_by_set_id(identity)['local_completion_committed']==0

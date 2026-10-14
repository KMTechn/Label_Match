"""U4 pairs with a U3 contract double; no real server or stock movement claim."""
import copy
from datetime import datetime, timedelta, timezone
import json
import sqlite3
import uuid

import pytest

import Label_Match as lm
import package_logistics as logistics
from label_screen_bridge import LabelMatchScreenProcessor, _approval_id, _pointer_pending
from package_logistics import (
    PackageApiError, PackageCancellationOutbox, PackageCancellationOutboxProcessor,
    PackageClientConfig, PackageCommandDraft, PackageLogisticsClient, PackageOutbox,
    PackageOutboxProcessor, PackageTransportError,
)
from sealed_transfer_exchange import SealedTransferExchangeCoordinator, SealedTransferExchangeStore
from tests.test_lm_scan_cancel import SelectableTree, app_for_delete
from tests.test_package_logistics import (
    _projection, _qr, _receipt, _cancellation_receipt, _cancellation_intent,
    _draft_for_set, _ack_package_creation,
)
from tests.test_sealed_transfer_exchange import FakeClient, _prepare, _target_projection
from tests.test_u4_cancellation_order import queued
from tests.test_deferred_lease_clock_retry import clock_case  # noqa: F401
from tests.test_w9scanonly_packaging import packaging, scan, completions  # noqa: F401
from tests.test_w9scanonly_central import central  # noqa: F401


class ReopenContractDouble:
    def __init__(self, source=None, *, mode='new'):
        self.source = copy.deepcopy(source or _projection())
        self.mode = mode
        self.online = True
        self.receipts = {}
        self.drafts = {}
        self.sent = []
        self.resolved_versions = []
        self.cancelled = set()

    def advance_source(self):
        self.source['entity_version'] += 1
        if self.source.get('active_seal'):
            self.source['active_seal']['sealed_bundle_version'] = self.source['entity_version']

    def transport(self, method, url, headers, body, timeout):
        if not self.online:
            raise PackageTransportError('synthetic disconnection')
        if method == 'GET':
            self.resolved_versions.append(self.source['entity_version'])
            if self.cancelled and self.mode != 'new':
                raise PackageApiError(409, 'COMPLETION_REVIEW_REQUIRED', 'synthetic old-server repack refusal')
            return {'ok': True, 'data': {**copy.deepcopy(self.source), 'bundle_role': 'PACKAGE_SOURCE'}}
        command = json.loads(body.decode('utf-8'))
        key = command['idempotency_key']
        if key in self.receipts:
            return {'ok': True, 'data': self.receipts[key]}
        payload = command['payload']
        package_id = payload['package_bundle_id']
        draft = self.drafts[package_id]
        self.sent.append((command['command_type'], package_id))
        if command['command_type'] == 'CANCEL_PACKAGE':
            receipt = _cancellation_receipt(draft)
            receipt['data'].update(reason=payload['reason'], evidence=copy.deepcopy(payload['evidence']))
            self.cancelled.add(package_id)
            if self.mode == 'new':
                self.advance_source()
                self.source['bundle_state'] = 'AVAILABLE'
                receipt['data'].update(reopened_transfer_bundle_id=self.source['bundle_id'],
                                       transfer_entity_version=self.source['entity_version'], reversal_movement_id='REVERSAL-U4')
            elif self.mode == 'refused':
                receipt['data'].update(reopen=False, reopen_reason='REOPEN_LINEAGE_AMBIGUOUS')
        else:
            assert self.source['bundle_state'] == 'AVAILABLE'
            assert command['expected_versions']['bundle:' + self.source['bundle_id']] == self.source['entity_version']
            receipt = _receipt(draft)
            receipt['data'].update(source_bundle_id=self.source['bundle_id'],
                                   source_evidence=copy.deepcopy(payload['source_evidence']))
            self.advance_source()
            self.source['bundle_state'] = 'CONSUMED'
            receipt['entity_versions'] = {'bundle:' + self.source['bundle_id']: self.source['entity_version'],
                                          'bundle:' + package_id: 1}
        receipt.update(authority_scope_id=self.source['authority_scope_id'], authority_epoch=self.source['authority_epoch'],
                       resolved_ledger_plane=self.source['ledger_plane'], resolved_plane_epoch=self.source['plane_epoch'])
        receipt['data'].update(member_ids=list(self.source['member_ids']), member_count=self.source['member_count'],
                               membership_hash=self.source['membership_hash'])
        self.receipts[key] = receipt
        return {'ok': True, 'data': receipt}


@pytest.mark.parametrize("legacy", [False, True])
def test_offline_two_repacks_retry_create_and_restart_preserve_causal_delivery(tmp_path, monkeypatch, legacy):
    clock = [datetime(2026, 10, 14, tzinfo=timezone.utc)]
    stamp = lambda value: value.isoformat().replace("+00:00", "Z")
    monkeypatch.setattr(logistics, "utc_now", lambda: stamp(clock[0]))
    monkeypatch.setattr(logistics, "_utc_after", lambda seconds: stamp(clock[0] + timedelta(seconds=seconds)))
    monkeypatch.setattr(logistics, "_utc_before", lambda seconds: stamp(clock[0] - timedelta(seconds=seconds)))
    server = ReopenContractDouble()
    outbox = PackageOutbox(tmp_path / "causal.db")
    cancels = PackageCancellationOutbox(outbox.db_path)
    drafts = [draft_for(server, name, _qr()) for name in ("A", "B", "C")]
    intents = []
    for draft in drafts:
        server.drafts[draft.package_bundle_id] = draft
        queued(outbox, draft)
        if draft is not drafts[-1]:
            intent = _cancellation_intent(draft)
            intents.append(intent)
            assert cancels.enqueue(intent)["status"] == "DEFERRED"
            cancels.mark_local_event_committed(intent.cancellation_event_id)
    if legacy:
        with sqlite3.connect(outbox.db_path) as conn:
            conn.execute("DROP INDEX ix_package_local_completion_order")
            conn.execute("ALTER TABLE package_command_outbox DROP COLUMN local_completion_order")
            conn.execute("ALTER TABLE package_command_outbox DROP COLUMN predecessor_package_key")
            conn.execute("UPDATE package_outbox_schema_info SET value='label-match-package-outbox-v11' WHERE key='schema_version'")
    outbox = PackageOutbox(outbox.db_path)
    cancels = PackageCancellationOutbox(outbox.db_path)
    if legacy:
        # §13: v11 has no durable completion order; do not infer the chain.
        create_review = outbox.list_conflicts()
        assert len(create_review) == 3
        assert all(row["last_error_code"] == "REPACK_PREDECESSOR_REVIEW_REQUIRED" for row in create_review)
        assert all(row["local_completion_order"] is None and row["predecessor_package_key"] is None for row in create_review)
        assert outbox.claim_next() is None
        assert cancels.claim_next() is None
        assert not server.sent
        return
    initial_keys = [outbox.get_by_set_id(draft.set_id)["idempotency_key"] for draft in drafts]
    assert [outbox.get_by_set_id(draft.set_id)["predecessor_package_key"] for draft in drafts] == [None, *initial_keys[:2]]
    source = server.source
    first_attempt = [True]

    def transport(method, url, headers, body, timeout):
        if method == "POST" and first_attempt[0]:
            command = json.loads(body.decode("utf-8"))
            assert command["command_type"] == "CREATE_PACKAGE"
            assert command["payload"]["package_bundle_id"] == drafts[0].package_bundle_id
            first_attempt[0] = False
            error = PackageTransportError("offline original create")
            error.retry_after_seconds = 60
            raise error
        return server.transport(method, url, headers, body, timeout)

    client = PackageLogisticsClient(PackageClientConfig(
        "https://unreachable.invalid", uuid.uuid4().hex, source["authority_scope_id"], "host", "device",
        authority_epoch=source["authority_epoch"], ledger_plane=source["ledger_plane"],
        plane_epoch=source["plane_epoch"]), transport=transport)
    client.get_receipt_if_exists = lambda key, **kwargs: server.receipts.get(key)
    def get_receipt(key, **kwargs):
        if key not in server.receipts:
            raise PackageApiError(404, "RECEIPT_NOT_FOUND", "receipt not received")
        return server.receipts[key]
    client.get_receipt = get_receipt
    create = PackageOutboxProcessor(outbox, client)
    result = create.drain()
    assert result["retry"] == 1
    assert result == {"acked": 0, "retry": 1, "conflict": 0}
    original = outbox.get_by_set_id("A")
    assert original["command_json"] and original["attempt_count"] == 1
    assert not server.sent
    other = _draft_for_set("OTHER", independent_source=True)
    assert _ack_package_creation(outbox, other)["status"] == "ACKED"
    assert PackageCancellationOutboxProcessor(cancels, client).drain()["acked"] == 0

    # All three local completions, both cancellation intents and the retry survive.
    outbox = PackageOutbox(outbox.db_path)
    cancels = PackageCancellationOutbox(outbox.db_path)
    create = PackageOutboxProcessor(outbox, client)
    cancel = PackageCancellationOutboxProcessor(cancels, client)
    keys = [outbox.get_by_set_id(draft.set_id)["idempotency_key"] for draft in drafts]
    assert [outbox.get_by_set_id(draft.set_id)["predecessor_package_key"] for draft in drafts] == [None, *keys[:2]]
    assert outbox.get_by_set_id("A")["command_json"] == original["command_json"]
    clock[0] += timedelta(seconds=61)
    for _ in range(2):
        assert create.drain() == {"acked": 1, "retry": 0, "conflict": 0}
        assert cancel.drain()["acked"] == 1
    assert create.drain() == {"acked": 1, "retry": 0, "conflict": 0}
    assert server.sent == [
        ("CREATE_PACKAGE", drafts[0].package_bundle_id),
        ("CANCEL_PACKAGE", drafts[0].package_bundle_id),
        ("CREATE_PACKAGE", drafts[1].package_bundle_id),
        ("CANCEL_PACKAGE", drafts[1].package_bundle_id),
        ("CREATE_PACKAGE", drafts[2].package_bundle_id),
    ]
    assert [outbox.get_by_set_id(draft.set_id)["status"] for draft in drafts] == ["ACKED"] * 3
    assert [outbox.get_by_set_id(draft.set_id)["attempt_count"] for draft in drafts] == [2, 1, 1]
    assert outbox.get_repack_cancellation_receipt("A") is None
    for draft, intent in zip(drafts[1:], intents):
        receipt = json.loads(cancels.get_by_event_id(intent.cancellation_event_id)["receipt_json"])
        assert outbox.get_repack_cancellation_receipt(draft.set_id) == receipt


def draft_for(server, set_id, master):
    source = server.source
    fields = lm._label_match_parse_sealed_transfer_qr(master) or {}
    return PackageCommandDraft.build(
        set_id=set_id, item_code=source['item_id'], source_bundle_id=source['bundle_id'], source_external_label=master,
        source_authority_scope_id=source['authority_scope_id'], expected_member_count=source['member_count'],
        expected_membership_hash=source['membership_hash'], expected_authority_epoch=source['authority_epoch'],
        expected_ledger_plane=source['ledger_plane'], expected_plane_epoch=source['plane_epoch'],
        external_label=lm._label_match_system_package_external_label(master, set_id), membership_mode='INHERIT_ALL',
        sample_barcodes=[row['normalized_barcode'] for row in source['members']][:3],
        expected_seal_id=fields.get('SID', ''), expected_seal_revision=int(fields.get('SREV') or 0),
        expected_seal_token=fields.get('STK', ''), expected_seal_qr_payload=master if fields.get('SID') else '',
    )


def pair(tmp_path, *, mode='new', source=None, master=None):
    server = ReopenContractDouble(source, mode=mode)
    master = master or _qr()
    app = app_for_delete(tmp_path, central=False)
    app.set_details_map.pop('SET-2')
    app.history_tree.delete('SET-2')
    app._rebuild_global_scanned_set_from_details()
    app.package_outbox = PackageOutbox(tmp_path / 'paired.sqlite3')
    app.package_cancellation_outbox = PackageCancellationOutbox(app.package_outbox.db_path)
    config = PackageClientConfig('https://pair.invalid', uuid.uuid4().hex, server.source['authority_scope_id'], 'synthetic-host', 'synthetic-device',
                                 authority_epoch=server.source['authority_epoch'], ledger_plane=server.source['ledger_plane'],
                                 plane_epoch=server.source['plane_epoch'])
    client = PackageLogisticsClient(config, transport=server.transport)
    client.get_receipt_if_exists = lambda key, **kwargs: server.receipts.get(key)
    create = PackageOutboxProcessor(app.package_outbox, client)
    cancel = PackageCancellationOutboxProcessor(app.package_cancellation_outbox, client)
    original = draft_for(server, 'SET-1', master)
    server.drafts[original.package_bundle_id] = original
    queued(app.package_outbox, original)
    assert create.drain()['acked'] == 1
    return app, server, create, cancel, original, master


def cancel_local(app, path):
    if path == 'f2':
        app.run_tests = True
        app._cancel_completed_tray_by_label(app.set_details_map['SET-1']['master_label_code'])
    else:
        processor = LabelMatchScreenProcessor(app)
        try:
            assert processor.command('scan_cancel', {'rows': ['SET-1']})['ok']
            result = processor.command('approve', {'id': _approval_id(_pointer_pending(app))})
            assert result['ok'] and result['deleted'] == 1
        finally:
            processor.uninstall()
    assert 'SET-1' not in app.set_details_map
    assert app.package_cancellation_outbox.get_by_set_id('SET-1')[0]['local_event_committed'] == 1


def enqueue_repack(app, server, master):
    repack = draft_for(server, 'REPACK', master)
    server.drafts[repack.package_bundle_id] = repack
    queued(app.package_outbox, repack)
    return repack


@pytest.mark.parametrize('path', ['f2', 'scan_cancel'])
def test_f2_then_repack_same_tag_creates_new_package_central(tmp_path, path):
    app, server, create, cancel, original, master = pair(tmp_path)
    cancel_local(app, path)
    repack = enqueue_repack(app, server, master)
    assert create.drain()['acked'] == 0
    assert cancel.drain()['acked'] == 1
    reopened_version = server.source['entity_version']
    assert create.drain()['acked'] == 1
    assert server.sent == [('CREATE_PACKAGE', original.package_bundle_id), ('CANCEL_PACKAGE', original.package_bundle_id), ('CREATE_PACKAGE', repack.package_bundle_id)]
    assert repack.package_bundle_id != original.package_bundle_id
    assert repack.external_label != original.external_label
    assert server.resolved_versions[-1] == reopened_version
    assert app.package_outbox.get_by_set_id('REPACK')['status'] == 'ACKED'
    assert app.package_outbox.get_by_set_id('SET-1')['receipt_json']


def test_f2_offline_then_repack_sends_cancel_before_create(tmp_path, monkeypatch):
    app, server, create, cancel, original, master = pair(tmp_path)
    server.online = False
    cancel_local(app, 'f2')
    repack = enqueue_repack(app, server, master)
    assert cancel.drain()['retry'] == 1
    assert create.drain()['acked'] == 0
    assert app.package_outbox.get_by_set_id('REPACK')['attempt_count'] == 0
    server.online = True
    future = (datetime.now(timezone.utc) + timedelta(minutes=10)).strftime('%Y-%m-%dT%H:%M:%SZ')
    monkeypatch.setattr(logistics, 'utc_now', lambda: future)
    assert cancel.drain()['acked'] == 1
    assert create.drain()['acked'] == 1
    assert server.sent[-2:] == [('CANCEL_PACKAGE', original.package_bundle_id), ('CREATE_PACKAGE', repack.package_bundle_id)]


def test_repack_waits_for_lost_cancel_ack_recheck_without_reposting_cancel(tmp_path, monkeypatch):
    app, server, create, cancel, original, master = pair(tmp_path)
    cancel_local(app, 'f2')
    enqueue_repack(app, server, master)
    original_mark = app.package_cancellation_outbox.mark_acked
    lost = []

    def lose_first_ack(key, receipt, **kwargs):
        if not lost:
            lost.append(True)
            app.package_cancellation_outbox.mark_retry(key, PackageTransportError('synthetic late-result claim change'),
                                                        expected_claim=kwargs['expected_claim'])
        return original_mark(key, receipt, **kwargs)

    monkeypatch.setattr(app.package_cancellation_outbox, 'mark_acked', lose_first_ack)
    assert cancel.drain()['acked'] == 0
    assert app.package_cancellation_outbox.get_by_set_id('SET-1')[0]['status'] == 'PENDING'
    assert app.package_outbox.claim_next() is None
    assert app.package_outbox.get_by_set_id('REPACK')['attempt_count'] == 0
    cancel.drain()
    assert create.drain()['acked'] == 1
    assert server.sent.count(('CANCEL_PACKAGE', original.package_bundle_id)) == 1


def test_f2_exchange_then_repack_inherits_new_seal(tmp_path):
    from tests.test_sealed_transfer_exchange import OLD_QR, TARGET

    app, server, create, cancel, original, master = pair(tmp_path, source=_target_projection(), master=OLD_QR)
    cancel_local(app, 'f2')
    assert cancel.drain()['acked'] == 1

    class ReopenedExchangeClient(FakeClient):
        def get_bundle(self, bundle_id, **kwargs):
            return copy.deepcopy(server.source) if bundle_id == TARGET else super().get_bundle(bundle_id, **kwargs)

        def replace_and_reseal_transfer(self, command):
            receipt = super().replace_and_reseal_transfer(command)
            versions = {key: value + 1 for key, value in command['expected_versions'].items()}
            receipt['entity_versions'] = versions
            data = receipt['data']
            data['target_version_before'] = command['expected_versions']['bundle:' + TARGET]
            data['target_version_after'] = versions['bundle:' + TARGET]
            data['sealed_bundle_version'] = versions['bundle:' + TARGET]
            server.source = _target_projection(qr=data['new_seal_qr_payload'], seal_id=data['new_seal_id'],
                                               token=data['new_seal_token'], revision=data['new_seal_revision'],
                                               ids=data['new_member_ids'], barcodes=data['new_normalized_barcodes'])
            server.source['entity_version'] = data['sealed_bundle_version']
            server.source['active_seal']['sealed_bundle_version'] = data['sealed_bundle_version']
            return receipt

    store = SealedTransferExchangeStore(tmp_path / 'exchange.sqlite3')
    coordinator = SealedTransferExchangeCoordinator(store, ReopenedExchangeClient())
    prepared = _prepare(coordinator)
    exchanged = coordinator.attempt(prepared.intent_id)
    assert exchanged.status == 'ACKED'
    state = lm._label_match_apply_sealed_exchange_state(
        {'raw': [master], 'parsed': [server.source['item_id']], 'package_source_snapshot': {'stale': True}},
        old_seal_qr_payload=master, new_seal_qr_payload=exchanged.new_seal_qr_payload,
        old_barcodes=exchanged.old_barcodes, new_barcodes=exchanged.new_barcodes,
    )
    assert state['package_source_snapshot'] is None
    repack = enqueue_repack(app, server, state['raw'][0])
    assert create.drain()['acked'] == 1
    command = json.loads(app.package_outbox.get_by_set_id('REPACK')['command_json'])
    assert command['payload']['source_evidence']['membership_hash'] == server.source['membership_hash']
    assert repack.expected_seal_qr_payload == exchanged.new_seal_qr_payload
    assert repack.expected_seal_qr_payload != original.expected_seal_qr_payload


def test_repack_against_old_server_goes_review_with_notice(tmp_path):
    app, server, create, cancel, original, master = pair(tmp_path, mode='old')
    cancel_local(app, 'f2')
    repack = enqueue_repack(app, server, master)
    assert cancel.drain()['acked'] == 1
    assert create.drain()['conflict'] == 1
    row = app.package_outbox.get_by_set_id('REPACK')
    assert row['status'] == 'CONFLICT' and row['local_completion_committed'] == 1
    assert row['last_error_code'] == 'COMPLETION_REVIEW_REQUIRED'
    snapshot = app._read_package_review_snapshot()
    app._refresh_package_cancellation_review_notice(snapshot)
    assert '다시 포장을 지원하지 않습니다' in app._package_create_review_notice.message
    assert app.package_outbox.get_by_set_id('SET-1')['receipt_json']


@pytest.mark.parametrize('path', ['f2', 'scan_cancel'])
def test_cancel_then_same_phs2_five_scans_complete_locally_without_f3_or_confirmation(central, path):
    app, dialogs, case, root, _releases = central
    app.package_cancellation_outbox = PackageCancellationOutbox(case.database)
    master = case.group['scan_payload']
    values = [member['normalized_barcode'] for member in case.snapshot['work_group_source']['members']]
    assert len(values) == 4
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get('_phs2_background_pending'), timeout=5)
    for value in values:
        scan(app, value)
    assert len(completions(app)) == 1
    original_set = next(iter(app.set_details_map))
    original = app.package_outbox.get_by_set_id(original_set)
    assert original['local_completion_committed'] == 1
    if path == 'f2':
        app.run_tests = True
        app._cancel_completed_tray_by_label(master)
    else:
        app.run_tests = False
        app._worker_roster_choices = lambda: []
        app.history_tree = SelectableTree()
        app.history_tree.insert('', 'end', iid=original_set, values=(1, master, *values, app.Results.PASS, 'synthetic time'))
        app.history_row_details_map = dict(app.set_details_map)
        app._render_history_detail = lambda *args, **kwargs: None
        app._delete_selected_row = lm.Label_Match._delete_selected_row.__get__(app)
        app._request_pointer_confirmation = lm.Label_Match._request_pointer_confirmation.__get__(app)
        processor = LabelMatchScreenProcessor(app)
        try:
            request = processor.command('scan_cancel', {'rows': [original_set]})
            assert request['ok'] and request['code'] == 'SCAN_CANCEL_CONFIRMATION_REQUIRED'
            approval = processor.command('approve', {'id': _approval_id(_pointer_pending(app))})
            assert approval['ok'], approval.get('code')
            assert approval['deleted'] == 1
        finally:
            processor.uninstall()
    assert original_set not in app.set_details_map
    assert app.package_cancellation_outbox.get_by_set_id(original_set)[0]['local_event_committed'] == 1
    app.run_tests = True  # Restore the scanner fixture's posture after pointer approval.
    scan(app, master)
    root.run_until(lambda: not app.__dict__.get('_phs2_background_pending'), timeout=5)
    for value in values:
        scan(app, value)
    events = completions(app)
    assert len(events) == 2
    assert events[-1][1]['scanned_product_barcodes'] == [master, *values]
    assert events[-1][1]['scan_count'] == 5
    assert events[-1][1]['final_result'] == app.Results.PASS
    repack_set = next(iter(app.set_details_map))
    assert repack_set != original_set
    repack = app.package_outbox.get_by_set_id(repack_set)
    assert repack['local_completion_committed'] == 1
    assert repack['idempotency_key'] != original['idempotency_key']
    assert app.package_outbox.claim_next()['idempotency_key'] == original['idempotency_key']
    assert app.package_outbox.claim_next() is None
    assert dialogs == []

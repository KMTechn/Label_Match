"""Explicit integration pack: iwrun + LABEL_MATCH_TEST_U3_SOURCE archive path.

No backend or desktop doubles: only offline/lost-response fault injection.
This file is selected explicitly, outside the default test_ collection.
"""
from contextlib import contextmanager
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import sqlite3
from urllib.parse import urlsplit

import pytest
import Label_Match as lm
import package_logistics as logistics
from package_logistics import (
    PackageClientConfig, PackageLogisticsClient, PackageOutbox, PackageCancellationOutbox,
    PackageOutboxProcessor, PackageCancellationOutboxProcessor, PackageTransportError,
)
from tests.test_u4_repack_pair import draft_for, cancel_local
from tests.test_u4_cancellation_order import queued
from tests.test_lm_scan_cancel import app_for_delete
from tests.test_package_logistics import _cancellation_intent
from tests.test_u4_repack_pair import test_cancel_then_same_phs2_five_scans_complete_locally_without_f3_or_confirmation as physical_pair
from tests.test_deferred_lease_clock_retry import clock_case  # noqa: F401
from tests.test_w9scanonly_packaging import packaging  # noqa: F401
from tests.test_w9scanonly_central import central  # noqa: F401
from datetime import datetime, timedelta, timezone
from sealed_transfer_exchange import SealedTransferExchangeStore, SealedTransferExchangeCoordinator
from tests.test_u4_repack_visibility import prepare_recovery_app, assert_visible, business_snapshot


class LocalServer:
    def __init__(self, data):
        self.data = data
        self.evidence = copy.deepcopy(data['projection'])
        self.source = copy.deepcopy(self.evidence.get('bundle', self.evidence))
        self.online = True
        self.fail_create_once = False
        self.sent = []
        self.create_posts = []
        self.lost_create_receipt = None
        self.resolved_versions = []
        self.source_reads = 0
        headers = data['headers']
        self.config = PackageClientConfig(
            base_url='https://127.0.0.1:' + str(data['port']), token=data['credential'],
            source_host_id=headers['X-Logistics-Source-Host-Id'], device_id=headers['X-Logistics-Device-Id'],
            authority_scope_id=self.source['authority_scope_id'], authority_epoch=self.source['authority_epoch'],
            ledger_plane=self.source['ledger_plane'], plane_epoch=self.source['plane_epoch'],
            tls_ca_bundle_path=data['tls_ca_bundle_path'])
        self.client = PackageLogisticsClient(self.config, transport=self.transport)

    def transport(self, method, url, headers, body, timeout):
        if not self.online:
            raise PackageTransportError('isolated offline fault')
        assert urlsplit(url).hostname == '127.0.0.1'
        lose_response = self.fail_create_once and method == "POST" and urlsplit(url).path == "/logistics/api/v1/packages"
        response = logistics._default_transport(method, url, headers, body, timeout,
                                               tls_ca_bundle_path=self.config.tls_ca_bundle_path)
        if method == 'GET' and urlsplit(url).path == '/logistics/api/v1/bundles/resolve':
            self.source_reads += 1
        if method == 'POST' and urlsplit(url).path in (
                '/logistics/api/v1/packages', '/logistics/api/v1/packages/cancel'):
            command = json.loads(body.decode('utf-8'))
            self.sent.append((command['command_type'], command['payload']['package_bundle_id']))
            if command["command_type"] == "CREATE_PACKAGE":
                self.create_posts.append(copy.deepcopy(command))
        data = response.get('data', {})
        if method == 'GET' and 'entity_version' in data:
            self.resolved_versions.append(data['entity_version'])
        if lose_response:
            # The real API returned only after its transaction committed.
            self.fail_create_once = False
            self.lost_create_receipt = copy.deepcopy(response["data"])
            self.online = False
            raise PackageTransportError("CREATE committed; response and connection lost")
        return response


@contextmanager
def local_server(tmp_path, mode='legacy'):
    source = Path(os.environ['LABEL_MATCH_TEST_U3_SOURCE']).resolve()
    assert source.is_dir()
    ready = tmp_path / 'ready-private.json'
    state = tmp_path / 'server-state'
    with (tmp_path / 'server.log').open('wb') as output:
        process = subprocess.Popen([sys.executable, '-B', str(Path(__file__).with_name('u4_real_server_child.py')),
                                    str(source), str(state), str(ready), mode],
                                   stdout=output, stderr=output, cwd=tmp_path)
        try:
            until = time.monotonic() + 30
            while not ready.exists() and process.poll() is None and time.monotonic() < until:
                time.sleep(0.05)
            assert ready.exists(), 'local U3 startup failed; see exception type/site evidence'
            yield LocalServer(json.loads(ready.read_text(encoding='utf-8')))
        finally:
            if process.poll() is None:
                process.terminate()
            process.wait(timeout=10)


def pair(tmp_path, server):
    app = app_for_delete(tmp_path, central=False)
    app.set_details_map.pop('SET-2')
    app.history_tree.delete('SET-2')
    app._rebuild_global_scanned_set_from_details()
    app.package_outbox = PackageOutbox(tmp_path / 'app.db')
    app.package_cancellation_outbox = PackageCancellationOutbox(app.package_outbox.db_path)
    create = PackageOutboxProcessor(app.package_outbox, server.client)
    cancel = PackageCancellationOutboxProcessor(app.package_cancellation_outbox, server.client)
    master = server.source['active_seal']['seal_qr_payload']
    original = draft_for(server, 'SET-1', master)
    app.set_details_map['SET-1']['scan_payload'] = [master, *original.sample_barcodes]
    app.set_details_map['SET-1']['master_label'] = master
    app.set_details_map['SET-1']['raw_first_scan'] = master
    app.set_details_map['SET-1']['unique_master_label'] = master
    queued(app.package_outbox, original)
    assert create.drain() == {'acked': 1, 'retry': 0, 'conflict': 0}
    return app, create, cancel, original, master


@pytest.mark.parametrize('path', ['f2', 'scan_cancel'])
def test_real_f2_then_repack_same_tag_creates_new_package_central(tmp_path, path):
    with local_server(tmp_path) as server:
        app, create, cancel, original, master = pair(tmp_path, server)
        cancel_local(app, path)
        repack = draft_for(server, 'SET-REPACK', master)
        queued(app.package_outbox, repack)
        assert create.drain()['acked'] == 0
        assert cancel.drain()['acked'] == 1
        assert create.drain() == {'acked': 1, 'retry': 0, 'conflict': 0}
        assert server.sent == [('CREATE_PACKAGE', original.package_bundle_id),
                               ('CANCEL_PACKAGE', original.package_bundle_id),
                               ('CREATE_PACKAGE', repack.package_bundle_id)]
        assert original.package_bundle_id != repack.package_bundle_id


def test_real_f2_offline_then_repack_sends_cancel_before_create(tmp_path, monkeypatch):
    with local_server(tmp_path) as server:
        app, create, cancel, original, master = pair(tmp_path, server)
        server.online = False
        cancel_local(app, 'f2')
        repack = draft_for(server, 'SET-REPACK', master)
        queued(app.package_outbox, repack)
        assert cancel.drain()['retry'] == 1
        assert create.drain()['acked'] == 0
        assert app.package_outbox.get_by_set_id(repack.set_id)['attempt_count'] == 0
        server.online = True
        future = (datetime.now(timezone.utc) + timedelta(minutes=2)).isoformat().replace('+00:00', 'Z')
        monkeypatch.setattr(logistics, 'utc_now', lambda: future)
        assert cancel.drain()['acked'] == 1
        assert create.drain() == {'acked': 1, 'retry': 0, 'conflict': 0}
        assert server.sent[-2:] == [('CANCEL_PACKAGE', original.package_bundle_id), ('CREATE_PACKAGE', repack.package_bundle_id)]


def test_real_repack_waits_for_lost_cancel_ack_recheck_without_reposting_cancel(tmp_path, monkeypatch):
    with local_server(tmp_path) as server:
        app, create, cancel, original, master = pair(tmp_path, server)
        cancel_local(app, 'f2')
        repack = draft_for(server, 'SET-REPACK', master)
        queued(app.package_outbox, repack)
        mark = app.package_cancellation_outbox.mark_acked
        def late_ack(key, receipt, **kwargs):
            app.package_cancellation_outbox.mark_retry(key, PackageTransportError('local claim changed'))
            return mark(key, receipt, **kwargs)
        monkeypatch.setattr(app.package_cancellation_outbox, 'mark_acked', late_ack)
        assert cancel.drain()['acked'] == 0
        assert create.drain()['acked'] == 0
        monkeypatch.setattr(app.package_cancellation_outbox, 'mark_acked', mark)
        assert cancel.drain()['acked'] == 1
        assert create.drain()['acked'] == 1
        assert server.sent.count(('CANCEL_PACKAGE', original.package_bundle_id)) == 1


def test_real_refused_repack_remains_visible_after_cancel_replay_and_restart_without_release(tmp_path):
    with local_server(tmp_path) as server:
        app, create, cancel, original, master = pair(tmp_path, server)
        # Existing backend refusal condition: source evidence changed after CREATE.
        # This changes only the isolated fixture ledger, never a server response.
        with sqlite3.connect(server.data['database']) as connection:
            connection.execute('UPDATE logistics_bundles SET entity_version=entity_version+1 WHERE bundle_id=?',
                               (server.source['bundle_id'],))
        cancel_local(app,'f2')
        assert cancel.drain()['acked']==1
        cancellation=app.package_cancellation_outbox.get_by_set_id(original.set_id)[0]
        frozen_receipt=json.loads(cancellation['receipt_json'])
        assert frozen_receipt['data']['reopen'] is False
        assert frozen_receipt['data']['reopen_reason']=='SOURCE_TRANSFER_CHANGED'
        repack=draft_for(server,'review-only',master)
        queued(app.package_outbox,repack)
        assert create.drain()=={'acked':0,'retry':0,'conflict':0}
        prepare_recovery_app(app,app.package_outbox,tmp_path/'empty-current')
        before=business_snapshot(app.package_outbox)
        assert_visible(app,[repack.set_id],master)
        repeated=server.client.cancel_package(json.loads(cancellation['command_json']))
        assert repeated==frozen_receipt
        app.package_outbox=PackageOutbox(app.package_outbox.db_path)
        assert_visible(app,[repack.set_id],master)
        assert app.package_outbox.claim_next() is None
        assert business_snapshot(app.package_outbox)==before
        assert server.sent.count(('CREATE_PACKAGE',original.package_bundle_id))==1
        assert ('CREATE_PACKAGE',repack.package_bundle_id) not in server.sent
        with sqlite3.connect(server.data['database']) as connection:
            assert connection.execute("""SELECT COUNT(*) FROM w9scanonly_operator_reviews
                WHERE idempotency_key=? AND entity_id=? AND reason_code='SOURCE_TRANSFER_CHANGED'""",
                (cancellation['idempotency_key'],original.package_bundle_id)).fetchone()[0]==1
            assert connection.execute("""SELECT COUNT(*) FROM w9scanonly_review_events WHERE review_id IN (
                SELECT review_id FROM w9scanonly_operator_reviews WHERE idempotency_key=? AND entity_id=?)""",
                (cancellation['idempotency_key'],original.package_bundle_id)).fetchone()[0]==1
            assert connection.execute("SELECT COUNT(*) FROM logistics_movements WHERE movement_type='CANCEL_PACKAGE_REOPEN_TRANSFER'").fetchone()[0]==0


def create_effect_counts(server, draft):
    with sqlite3.connect(server.data["database"]) as connection:
        package = connection.execute("SELECT COUNT(*) FROM logistics_bundles WHERE bundle_id=?", (draft.package_bundle_id,)).fetchone()[0]
        movement = connection.execute("SELECT COUNT(*) FROM logistics_movements WHERE bundle_id=? AND reversal_of_movement_id IS NULL", (draft.package_bundle_id,)).fetchone()[0]
        event = connection.execute("SELECT COUNT(*) FROM logistics_domain_events WHERE event_type='PACKAGE_CREATED' AND aggregate_id=?", (draft.package_bundle_id,)).fetchone()[0]
        return package, movement, event


def test_real_two_offline_repacks_restart_and_retry_original_create(tmp_path):
    with local_server(tmp_path) as server:
        outbox = PackageOutbox(tmp_path / 'chain.db')
        cancels = PackageCancellationOutbox(outbox.db_path)
        master = server.source['active_seal']['seal_qr_payload']
        drafts = [draft_for(server, name, master) for name in ('A', 'B', 'C')]
        for draft in drafts:
            queued(outbox, draft)
            if draft is not drafts[-1]:
                intent = _cancellation_intent(draft)
                assert cancels.enqueue(intent)['status'] == 'DEFERRED'
                cancels.mark_local_event_committed(intent.cancellation_event_id)
        server.fail_create_once = True
        assert PackageOutboxProcessor(outbox, server.client).drain()['retry'] == 1
        original_command = outbox.get_by_set_id('A')['command_json']
        assert original_command
        original_row = outbox.get_by_set_id(drafts[0].set_id)
        assert original_row["status"] == "PENDING" and original_row["receipt_json"] is None
        assert server.create_posts == [json.loads(original_command)]
        assert create_effect_counts(server, drafts[0]) == (1, 1, 1)
        committed_receipt = copy.deepcopy(server.lost_create_receipt)
        outbox = PackageOutbox(outbox.db_path)
        cancels = PackageCancellationOutbox(outbox.db_path)
        create = PackageOutboxProcessor(outbox, server.client)
        cancel = PackageCancellationOutboxProcessor(cancels, server.client)
        assert outbox.get_by_set_id(drafts[0].set_id)["command_json"] == original_command
        server.online = True
        # Retry the restarted outbox's immutable command through the real app HTTP client.
        # Normal drain then confirms this receipt before sending the next cancel.
        recovered = server.client.create_package(json.loads(outbox.get_by_set_id(drafts[0].set_id)["command_json"]))
        assert recovered == committed_receipt
        assert server.create_posts == [json.loads(original_command)] * 2
        assert create_effect_counts(server, drafts[0]) == (1, 1, 1)
        for _ in range(2):
            assert create.drain() == {'acked': 1, 'retry': 0, 'conflict': 0}
            assert cancel.drain()['acked'] == 1
        assert create.drain() == {'acked': 1, 'retry': 0, 'conflict': 0}
        assert server.sent == [
            ("CREATE_PACKAGE",drafts[0].package_bundle_id), ("CREATE_PACKAGE",drafts[0].package_bundle_id),
            ("CANCEL_PACKAGE",drafts[0].package_bundle_id), ("CREATE_PACKAGE",drafts[1].package_bundle_id),
            ("CANCEL_PACKAGE",drafts[1].package_bundle_id), ("CREATE_PACKAGE",drafts[2].package_bundle_id)]
        assert [outbox.get_by_set_id(draft.set_id)['status'] for draft in drafts] == ['ACKED'] * 3
        assert [outbox.get_by_set_id(draft.set_id)['attempt_count'] for draft in drafts] == [2, 1, 1]
        assert outbox.get_by_set_id('A')['command_json'] == original_command
        assert json.loads(outbox.get_by_set_id(drafts[0].set_id)["receipt_json"]) == committed_receipt
        assert all(create_effect_counts(server, draft) == (1, 1, 1) for draft in drafts)
        assert len(server.create_posts) == 4
        with sqlite3.connect(server.data['database']) as connection:
            assert connection.execute("SELECT COUNT(*) FROM logistics_domain_events WHERE event_type='PACKAGE_REOPENED_TO_TRANSFER'").fetchone()[0] == 2
            assert connection.execute("SELECT COUNT(*) FROM logistics_movements WHERE movement_type='PACKAGE_REOPENED_TO_TRANSFER'").fetchone()[0] == 2


@pytest.mark.parametrize('path', ['f2', 'scan_cancel'])
def test_real_cancel_then_same_phs2_five_scans_complete_locally(tmp_path, central, path, monkeypatch):
    with local_server(tmp_path, mode='physical') as server:
        app, dialogs, case, root, releases = central
        # Preserve the dictionaries closed over by the original clock fixture.
        case.group.clear()
        case.group.update(server.evidence['phs_work_group'])
        case.snapshot.clear()
        case.snapshot.update(lm._label_match_package_source_snapshot(server.evidence))
        case.response.clear()
        case.response.update(server.evidence)
        case.app.package_logistics_client = server.client
        app.package_logistics_client = server.client
        for name, method in tuple(app.__dict__.items()):
            if getattr(method, '__self__', None) is case.app and hasattr(method, '__func__'):
                setattr(app, name, method.__func__.__get__(app, type(app)))
        item = case.group['item_id']
        app.items_data[item] = {'name': item, 'spec': item}
        scan = physical_pair.__globals__['scan']
        def idle(owner):
            return (not owner._ui_lane_is_busy() and not owner.is_blinking
                    and not owner.__dict__.get('_phs_label_scan_lookup_in_progress')
                    and not owner.__dict__.get('_phs2_background_pending'))
        def settled_scan(owner, value):
            # The contract fixture resets run_tests after pointer approval;
            # HTTPS integration keeps the actual production scanner path.
            owner.run_tests = False
            root.run_until(lambda: idle(owner), timeout=15)
            scan(owner, value)
            root.run_until(lambda: idle(owner), timeout=15)
        monkeypatch.setitem(physical_pair.__globals__, 'scan', settled_scan)
        physical_pair(central, path)
        assert server.source_reads >= 2


def test_real_f2_exchange_then_repack_inherits_new_seal(tmp_path):
    with local_server(tmp_path) as server:
        app, create, cancel, original, master = pair(tmp_path, server)
        cancel_local(app, 'f2')
        assert cancel.drain()['acked'] == 1
        fields = lm._label_match_parse_sealed_transfer_qr(master)
        target = server.data['seed']['target_evidence']
        donor = server.data['seed']['source_evidence']
        old = target['normalized_barcodes'][0]
        new = donor['normalized_barcodes'][0]
        coordinator = SealedTransferExchangeCoordinator(
            SealedTransferExchangeStore(tmp_path / 'exchange.db'), server.client)
        prepared = coordinator.prepare(set_id='EXCHANGE-REPACK', old_seal_qr_payload=master,
            old_seal_fields=fields, operator='test operator', old_barcodes=[old], new_barcodes=[new])
        exchanged = coordinator.attempt(prepared.intent_id)
        assert exchanged.status == 'ACKED'
        result = server.client._data(server.client._request('GET',
            '/logistics/api/v1/bundles/' + server.config.authority_scope_id + '/' + server.source['bundle_id']))
        server.source = result
        state = lm._label_match_apply_sealed_exchange_state(
            {'raw': [master], 'parsed': [fields], 'sealed_transfer_exchange_intent_id': prepared.intent_id},
            old_seal_qr_payload=master, new_seal_qr_payload=exchanged.new_seal_qr_payload,
            old_barcodes=exchanged.old_barcodes, new_barcodes=exchanged.new_barcodes)
        assert state['sealed_transfer_exchange_intent_id'] == ''
        repack = draft_for(server, 'SET-REPACK', exchanged.new_seal_qr_payload)
        queued(app.package_outbox, repack)
        assert create.drain() == {'acked': 1, 'retry': 0, 'conflict': 0}
        assert repack.expected_seal_qr_payload == exchanged.new_seal_qr_payload
        assert repack.expected_seal_qr_payload != original.expected_seal_qr_payload
        command = json.loads(app.package_outbox.get_by_set_id(repack.set_id)['command_json'])
        assert command['expected_versions']['bundle:' + server.source['bundle_id']] == server.source['entity_version']


def test_real_u3_reopen_fields_keep_legacy_fields_and_avoid_old_server_notice(tmp_path):
    with local_server(tmp_path) as server:
        app, create, cancel, original, master = pair(tmp_path, server)
        cancel_local(app, 'f2')
        repack = draft_for(server, 'SET-REPACK', master)
        queued(app.package_outbox, repack)
        assert cancel.drain()['acked'] == 1
        receipt = app.package_outbox.get_repack_cancellation_receipt(repack.set_id)
        assert receipt['data']['reopen'] is True
        assert receipt['data']['reopened_transfer_bundle_id'] == server.source['bundle_id']
        assert receipt['data']['reversal_movement_id']
        assert receipt['data']['current_location'] == 'SHIPPING-WAIT'
        assert receipt['data']['bundle_state'] == 'AVAILABLE'
        assert create.drain()['acked'] == 1
        app._refresh_package_cancellation_review_notice(app._read_package_review_snapshot())
        assert not app.package_outbox.list_conflicts()
        assert app.package_outbox.get_by_set_id(repack.set_id)['local_completion_committed'] == 1

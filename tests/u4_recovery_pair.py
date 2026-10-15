import copy
from dataclasses import replace
import json
import sqlite3
import pytest
import Label_Match as lm
from package_logistics import PackageApiError
from tests.u4_real_server_pair import local_server, pair, draft_for, cancel_local, central, clock_case
from tests.test_u4_cancellation_order import queued
from tests.test_w9scanonly_packaging import packaging, scan, completions
from tests.test_u4_repack_visibility import business_snapshot
from package_logistics import PackageOutbox, PackageCancellationOutbox, PackageOutboxProcessor, PackageCancellationOutboxProcessor

def test_real_recovery_read_closes_once_then_same_physical_products_create_new_package_ack(tmp_path, central, monkeypatch):
    from datetime import datetime
    import terminal_operation_lease
    import deferred_intent_capture
    monkeypatch.setattr(lm, 'datetime', datetime)
    monkeypatch.setattr(terminal_operation_lease, 'datetime', datetime)
    monkeypatch.setattr(deferred_intent_capture, 'datetime', datetime)
    with local_server(tmp_path, mode='recovery') as server:
        app, dialogs, case, root, _releases = central

        def bind(evidence):
            case.group.clear()
            case.group.update(evidence['phs_work_group'])
            case.snapshot.clear()
            case.snapshot.update(lm._label_match_package_source_snapshot(evidence))
            case.response.clear()
            case.response.update(evidence)
            case.app.package_logistics_client = server.client
            app.package_logistics_client = server.client
            for name, method in tuple(app.__dict__.items()):
                if getattr(method, '__self__', None) is case.app and hasattr(method, '__func__'):
                    setattr(app, name, method.__func__.__get__(app, type(app)))
            item = case.group['item_id']
            app.items_data[item] = {'name': item, 'spec': item}

        def idle(owner):
            return not owner._ui_lane_is_busy() and (not owner.is_blinking) and (not owner.__dict__.get('_phs_label_scan_lookup_in_progress')) and (not owner.__dict__.get('_phs2_background_pending'))

        def settled_scan(value):
            app.run_tests = False
            root.run_until(lambda: idle(app), timeout=5)
            scan(app, value)
            root.run_until(lambda: idle(app), timeout=5)
        bind(server.evidence)
        app.package_cancellation_outbox = PackageCancellationOutbox(app.package_outbox.db_path)

        def prepare(draft):
            try:
                return app._prepare_scanner_package_delivery(draft)
            except Exception as error:
                import traceback
                (tmp_path / 'delivery-failure.safe.json').write_text(json.dumps({'type': type(error).__name__, 'frames': [{'file': frame.filename, 'line': frame.lineno} for frame in traceback.extract_tb(error.__traceback__)]}))
                raise
        create = PackageOutboxProcessor(app.package_outbox, server.client, prepare_scanner_delivery=prepare)
        cancel = PackageCancellationOutboxProcessor(app.package_cancellation_outbox, server.client)
        master = case.group['scan_payload']
        held_values = [m['normalized_barcode'] for m in case.snapshot['work_group_source']['members']]
        settled_scan(master)
        for value in held_values:
            settled_scan(value)
        original_set = next(iter(app.set_details_map))
        assert create.drain()['acked'] == 1
        assert app.package_outbox.get_by_set_id(original_set)['status'] == 'ACKED'
        source_ids = server.evidence['work_group_source']['source_transfer_bundle_ids']
        with sqlite3.connect(server.data['database']) as conn:
            for source_id in source_ids:
                conn.execute('UPDATE logistics_bundles SET entity_version=entity_version+1 WHERE bundle_id=?', (source_id,))
        app.run_tests = True
        app._cancel_completed_tray_by_label(master)
        assert cancel.drain()['acked'] == 1
        receipt = json.loads(app.package_cancellation_outbox.get_by_set_id(original_set)[0]['receipt_json'])
        assert receipt['data']['reopen'] is False
        from package_recovery import AWAITING_NOTICE, RECOVERED_NOTICE
        from package_logistics import PackageOutbox
        from label_screen_bridge import LabelMatchScreenProcessor
        from urllib.request import Request, urlopen
        from urllib.error import HTTPError
        from urllib.parse import urlencode
        import ssl
        from tests.test_u4_repack_visibility import prepare_recovery_app
        from pathlib import Path
        idle_state=copy.deepcopy(app.current_set_info)
        prepare_recovery_app(app,app.package_outbox,Path(app.save_directory))
        app.current_set_info=idle_state
        before = business_snapshot(app.package_outbox)
        target = app.package_outbox.package_recovery_target(original_set)
        assert target is not None
        assert app._package_recovery_candidates()[0]['repack_review']['label'] == '관리자 회수 대기'
        results = []
        app._recheck_package_recovery(original_set, finish=results.append)
        root.run_until(lambda: idle(app), timeout=15)
        assert results[-1] == dict(state='AWAITING', text=AWAITING_NOTICE)
        assert business_snapshot(app.package_outbox) == before
        assert app.package_outbox.cancelled_package_for_barcode(held_values[0])

        def admin_post(action, body):
            url = server.config.base_url + '/dashboard/api/logistics/packages/recovery/' + action + '?' + urlencode({'authority_scope_id': server.config.authority_scope_id})
            headers = dict(server.data['recovery_admin_csrf'])
            headers.update({'Cookie': server.data['recovery_admin_cookie'], 'Content-Type': 'application/json'})
            request = Request(url, data=json.dumps(body).encode(), headers=headers, method='POST')
            context = ssl.create_default_context(cafile=server.config.tls_ca_bundle_path)
            try:
                with urlopen(request, context=context, timeout=10) as reply:
                    assert reply.status == 200
                    return json.loads(reply.read())['data']
            except HTTPError as error:
                import re
                data = json.loads(error.read())
                code = data.get('error', {}).get('code', '')
                assert re.fullmatch('[A-Z0-9_]{1,100}', code)
                (tmp_path / 'recovery-admin-failure.safe.json').write_text(json.dumps({'status': error.code, 'code': code}))
                raise AssertionError('loopback recovery admin response rejected') from None
        preview = admin_post('preview', dict(package_bundle_id=target['expected']['package_bundle_id'], cancel_receipt_id=target['expected']['cancel_receipt_id'], scanned_barcodes=held_values, label_scans=[master]))
        confirmed = admin_post('confirm', dict(preview_fingerprint=preview['preview_fingerprint'], idempotency_key='recovery-pair-admin', reason='현장 회수 확인', current_pin=server.data['recovery_admin_pin']))
        assert confirmed
        app._recheck_package_recovery(original_set, finish=results.append)
        root.run_until(lambda: idle(app), timeout=15)
        assert results[-1] == dict(state='RECOVERED', text=RECOVERED_NOTICE)
        assert business_snapshot(app.package_outbox) == before
        assert app.package_outbox.list_repack_reviews() == []
        assert app.package_outbox.cancelled_package_for_barcode(held_values[0]) is None
        assert app.package_outbox.claim_next() is None
        app.package_outbox = PackageOutbox(app.package_outbox.db_path)
        assert app.package_outbox.list_repack_reviews() == []
        app._recheck_package_recovery(original_set, finish=results.append)
        root.run_until(lambda: idle(app), timeout=15)
        assert app.package_outbox.claim_next() is None
        with sqlite3.connect(app.package_outbox.db_path) as conn:
            assert conn.execute('SELECT count(*) FROM package_recovery_evidence').fetchone()[0] == 1
            assert conn.execute('SELECT count(*) FROM package_recovery_review_closures').fetchone()[0] == 1
        draft = json.loads(app.package_outbox.get_by_set_id(original_set)['draft_json'])
        fresh = server.client.resolve_transfer_bundle(external_label=draft['source_external_label'], input_tag_id=draft['source_input_tag_id'], input_tag_label_id=draft['source_input_tag_label_id'], input_tag_hash_prefix=draft['source_input_tag_hash_prefix'], item_id=draft['item_code'], authority_scope_id=draft['source_authority_scope_id'])
        assert fresh['work_group_source']['package_bundle_id'] != draft['package_bundle_id']
        assert any((row['origin_receipt_contract_version'] == 'PACKAGE_RECOVERY_TRANSFER_V1' for row in fresh['work_group_source']['source_transfers']))
        bind(fresh)
        reads = server.source_reads
        settled_scan(master)
        assert app.current_set_info.get('raw')==[master]
        assert app.current_set_info.get('scanner_only')
        for value in held_values:
            settled_scan(value)
        new_set = next(iter(app.set_details_map))
        assert new_set != original_set
        assert app.durable_commits[-1]['result'] == app.Results.PASS and dialogs == []
        create = PackageOutboxProcessor(app.package_outbox, server.client, prepare_scanner_delivery=prepare)
        assert create.drain() == dict(acked=1, conflict=0, retry=0)
        new_row = app.package_outbox.get_by_set_id(new_set)
        assert new_row['status'] == 'ACKED'
        new_receipt = json.loads(new_row['receipt_json'])
        assert new_receipt['data']['package_bundle_id'] != target['expected']['package_bundle_id']
        assert sorted(new_receipt['data']['member_ids']) == target['expected']['member_ids']
        assert server.source_reads > reads
        assert create.drain() == dict(acked=0, conflict=0, retry=0)
        assert [kind for kind, _ in server.sent] == ['CREATE_PACKAGE', 'CANCEL_PACKAGE', 'CREATE_PACKAGE']
        with sqlite3.connect(server.data['database']) as conn:
            assert conn.execute("SELECT count(*) FROM logistics_command_receipts WHERE command_type='PHS_ADMIN_RECOVER_PACKAGE_TRANSFER'").fetchone()[0] == 1

"""Real U3 investigation of a cancelled product appearing in another label's QA."""
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


def cancelled_source(tmp_path):
    return local_server(tmp_path,mode='spill')


def test_real_foreign_cancelled_member_quarantines_a_different_completed_label(tmp_path):
    with cancelled_source(tmp_path) as server:
        app,create,cancel,original,master=pair(tmp_path,server)
        with sqlite3.connect(server.data['database']) as conn:
            conn.execute('UPDATE logistics_bundles SET entity_version=entity_version+1 WHERE bundle_id=?',
                         (server.source['bundle_id'],))
        cancel_local(app,'f2')
        assert cancel.drain()['acked']==1
        receipt=json.loads(app.package_cancellation_outbox.get_by_set_id(original.set_id)[0]['receipt_json'])
        assert receipt['data']['reopen'] is False
        other=server.data['other_projection']
        server.source=copy.deepcopy(other)
        good=draft_for(server,'other-label',other['active_seal']['seal_qr_payload'])
        bad=replace(good,sample_barcodes=(original.sample_barcodes[0],*good.sample_barcodes[1:]))
        queued(app.package_outbox,bad)
        assert create.drain()=={'acked':0,'retry':0,'conflict':1}
        blocked=app.package_outbox.get_by_set_id(bad.set_id)
        assert blocked['local_completion_committed']==1 and blocked['status']=='CONFLICT'
        assert blocked['last_error_code']=='LOCAL_VALIDATION_CONFLICT'
        assert ('CREATE_PACKAGE',bad.package_bundle_id) not in server.sent
        # The actual server also rejects this evidence when supplied directly.
        _source,command=server.client.build_create_package_command(good,idempotency_key='spill-direct-proof')
        command['payload'].update(sample_barcodes=list(bad.sample_barcodes))
        try:
            observed=server.client.create_package(command)
        except PackageApiError as error:
            assert error.code in {'QA_EVIDENCE_CONFLICT','COMPLETION_REVIEW_REQUIRED'}
        else:
            assert observed.get('status')=='OBSERVED_REJECTED'


def test_real_refused_cancel_member_is_blocked_before_other_label_and_other_products_ack(tmp_path,central):
    with local_server(tmp_path,mode='spill-physical') as server:
        app,dialogs,case,root,_releases=central
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
            app.run_tests=False
            root.run_until(lambda: idle(app),timeout=5)
            scan(app,value)
            root.run_until(lambda: idle(app),timeout=5)
        bind(server.evidence)
        app.package_cancellation_outbox=PackageCancellationOutbox(app.package_outbox.db_path)
        create=PackageOutboxProcessor(app.package_outbox,server.client)
        cancel=PackageCancellationOutboxProcessor(app.package_cancellation_outbox,server.client)
        master=case.group['scan_payload']
        held_values=[m['normalized_barcode'] for m in case.snapshot['work_group_source']['members']]
        settled_scan(master)
        for value in held_values: settled_scan(value)
        original_set=next(iter(app.set_details_map))
        assert create.drain()['acked']==1
        assert app.package_outbox.get_by_set_id(original_set)['status']=='ACKED'
        source_ids=server.evidence['work_group_source']['source_transfer_bundle_ids']
        with sqlite3.connect(server.data['database']) as conn:
            for source_id in source_ids:
                conn.execute('UPDATE logistics_bundles SET entity_version=entity_version+1 WHERE bundle_id=?',(source_id,))
        app.run_tests=True
        app._cancel_completed_tray_by_label(master)
        assert cancel.drain()['acked']==1
        receipt=json.loads(app.package_cancellation_outbox.get_by_set_id(original_set)[0]['receipt_json'])
        assert receipt['data']['reopen'] is False
        bind(server.data['other_evidence'])
        other_master=case.group['scan_payload']
        values=[m['normalized_barcode'] for m in case.snapshot['work_group_source']['members']]
        assert len(values)==4 and not set(values).intersection(held_values)
        settled_scan(other_master)
        assert app.current_set_info['raw']==[other_master]
        before=copy.deepcopy(app.current_set_info)
        business=business_snapshot(app.package_outbox)
        sent=list(server.sent)
        reads=server.source_reads
        for _ in range(2):
            settled_scan(held_values[0])
            assert app._phs_label_guidance_notice.title=='이 제품은 관리자 회수 대기 중입니다'
            assert app._phs_label_guidance_notice.message=='따로 두고 현장 관리자에게 알리세요'
            assert app.current_set_info==before and business_snapshot(app.package_outbox)==business
            assert server.sent==sent and server.source_reads==reads
        app.package_outbox=PackageOutbox(app.package_outbox.db_path)
        settled_scan(held_values[-1])
        assert app.current_set_info==before and business_snapshot(app.package_outbox)==business
        for value in values: settled_scan(value)
        rows=completions(app)
        assert len(rows)==2 and rows[-1][1]["scanned_product_barcodes"]==[other_master,*values]
        assert app.durable_commits[-1]['result']==app.Results.PASS and dialogs==[]
        other_set=next(iter(app.set_details_map))
        assert other_set!=original_set
        assert create.drain()=={'acked':1,'retry':0,'conflict':0}
        completed=app.package_outbox.get_by_set_id(other_set)
        assert completed['status']=='ACKED' and completed['local_completion_committed']==1
        assert not app.package_outbox.list_conflicts()
        assert server.sent==sent+[('CREATE_PACKAGE',json.loads(completed['draft_json'])['package_bundle_id'])]

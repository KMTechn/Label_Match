"""Real U3 investigation of a cancelled product appearing in another label's QA."""
import copy
from dataclasses import replace
import json
import sqlite3
import pytest
import Label_Match as lm
from package_logistics import PackageApiError
from tests.u4_real_server_pair import local_server, pair, draft_for, cancel_local
from tests.test_u4_cancellation_order import queued
from tests.test_w9scanonly_packaging import packaging, scan, completions
from tests.test_u4_repack_visibility import business_snapshot
from package_logistics import PackageOutbox


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


def test_real_refused_cancel_member_is_blocked_before_other_label_and_other_products_ack(tmp_path,packaging):
    with cancelled_source(tmp_path) as server:
        owner,create,cancel,original,master=pair(tmp_path,server)
        with sqlite3.connect(server.data['database']) as conn:
            conn.execute('UPDATE logistics_bundles SET entity_version=entity_version+1 WHERE bundle_id=?',
                         (server.source['bundle_id'],))
        cancel_local(owner,'tk')
        assert cancel.drain()['acked']==1
        receipt=json.loads(owner.package_cancellation_outbox.get_by_set_id(original.set_id)[0]['receipt_json'])
        assert receipt['data']['reopen'] is False
        app,dialogs=packaging
        app.package_outbox=owner.package_outbox
        other=server.data['other_projection']
        server.source=copy.deepcopy(other)
        item=other['item_code']
        app.items_data[item]={'item_name':item,'spec':item}
        scan(app,item)
        before=copy.deepcopy(app.current_set_info)
        business=business_snapshot(app.package_outbox)
        sent=list(server.sent)
        reads=server.source_reads
        for _ in range(2):
            scan(app,original.sample_barcodes[0])
            assert app._phs_label_guidance_notice.title=='이 제품은 관리자 회수 대기 중입니다'
            assert app._phs_label_guidance_notice.message=='따로 두고 현장 관리자에게 알리세요'
            assert app.current_set_info==before and business_snapshot(app.package_outbox)==business
            assert server.sent==sent and server.source_reads==reads
        app.package_outbox=PackageOutbox(app.package_outbox.db_path)
        scan(app,original.sample_barcodes[0])
        assert app.current_set_info==before and business_snapshot(app.package_outbox)==business
        values=[member['normalized_barcode'] for member in other['members']]
        for value in values:
            scan(app,value)
        rows=completions(app)
        assert len(rows)==1 and rows[0][1]["scanned_product_barcodes"]==[item,*values]
        assert app.durable_commits[-1]["result"]==app.Results.PASS and dialogs==[]
        good=draft_for(server,'guarded-other-label',other['active_seal']['seal_qr_payload'])
        good=replace(good,sample_barcodes=tuple(rows[0][1]["scanned_product_barcodes"][1:4]))
        queued(owner.package_outbox,good)
        assert create.drain()=={'acked':1,'retry':0,'conflict':0}
        assert owner.package_outbox.get_by_set_id(good.set_id)['status']=='ACKED'
        assert not owner.package_outbox.list_conflicts()
        assert server.sent==sent+[('CREATE_PACKAGE',good.package_bundle_id)]

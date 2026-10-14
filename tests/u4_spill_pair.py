"""Real U3 investigation of a cancelled product appearing in another label's QA."""
import copy
from dataclasses import replace
import json
import sqlite3
import pytest
from package_logistics import PackageApiError
from tests.u4_real_server_pair import local_server, pair, draft_for, cancel_local
from tests.test_u4_cancellation_order import queued


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

"""Reject only locally known refused-cancel members before they contaminate another label."""
import copy
import json

import pytest
from package_logistics import PackageOutbox, PackageCancellationOutbox
from tests.test_package_logistics import (
    _draft_for_set, _ack_package_creation, _cancellation_intent, _cancellation_receipt, UNITS,
)
from tests.test_u4_repack_visibility import business_snapshot
from tests.test_w9scanonly_packaging import packaging, scan, products, completions
from tests.test_legacy_label_transition import MASTER
import Label_Match as lm
from label_screen_bridge import LabelMatchScreenProcessor, _alerts


def seed_cancelled_members(outbox, barcode, *, reopen=False, evidence='source', ack=True):
    source={'members': [{'unit_id': UNITS[0], 'normalized_barcode': barcode},
                        {'unit_id': 'unselected-unit', 'normalized_barcode': barcode+'-remainder'}]}
    draft=_draft_for_set('cancelled-product')
    original=_ack_package_creation(outbox,draft)
    if evidence in {'sample', 'exact'}:
        # Model an acknowledged immutable command carrying the validated scan evidence.
        with outbox._connect() as conn:
            command=json.loads(original['command_json'])
            command['payload']['sample_barcodes' if evidence=='sample' else 'exact_rescan_barcodes']=[barcode]
            conn.execute('UPDATE package_command_outbox SET command_json=? WHERE idempotency_key=?',
                         (json.dumps(command),original['idempotency_key']))
            conn.commit()
    cancels=PackageCancellationOutbox(outbox.db_path)
    intent=_cancellation_intent(draft)
    row=cancels.enqueue(intent)
    cancels.mark_local_event_committed(intent.cancellation_event_id)
    claim=cancels.claim_next()
    assert claim['idempotency_key']==row['idempotency_key']
    receipt=_cancellation_receipt(draft,intent=intent)
    if reopen is not None:
        receipt['data']['reopen']=reopen
    if ack:
        assert cancels.mark_acked(row['idempotency_key'],receipt,expected_claim=claim)
    if evidence=='source':
        # Fixture a persisted hydrated unit-to-barcode map after the ACK binding.
        with outbox._connect() as conn:
            saved=json.loads(original['draft_json'])
            saved['work_group_source']=source
            conn.execute('UPDATE package_command_outbox SET draft_json=? WHERE idempotency_key=?',
                         (json.dumps(saved),original['idempotency_key']))
            conn.commit()
    return draft


@pytest.mark.parametrize('evidence',['source','sample','exact'])
def test_refused_cancelled_product_scan_preserves_other_label_and_normal_products_complete(packaging,tmp_path,evidence):
    app,dialogs=packaging
    app.package_outbox=PackageOutbox(tmp_path/'cancel-scan.sqlite')
    barcode=products(prefix='held')[0]
    seed_cancelled_members(app.package_outbox,barcode,evidence=evidence)
    scan(app,MASTER)
    before=copy.deepcopy(app.current_set_info)
    business=business_snapshot(app.package_outbox)
    for value in (barcode, ' '+barcode.lower()+' '):
        scan(app,value)
        assert app._phs_label_guidance_notice.title=='이 제품은 관리자 회수 대기 중입니다'
        assert app._phs_label_guidance_notice.message=='따로 두고 현장 관리자에게 알리세요'
        assert app.current_set_info==before
        assert business_snapshot(app.package_outbox)==business
    app.package_outbox=PackageOutbox(app.package_outbox.db_path)
    scan(app,barcode)
    assert app.current_set_info==before and business_snapshot(app.package_outbox)==business
    processor=LabelMatchScreenProcessor(app,module=lm)
    try:
        band=_alerts(processor,None,app._phs_label_guidance_notice)[0]
        assert band['text']=='이 제품은 관리자 회수 대기 중입니다\n따로 두고 현장 관리자에게 알리세요'
        assert 'choices' not in band
    finally:
        processor.uninstall()
    for value in products():
        scan(app,value)
    rows=completions(app)
    assert len(rows)==1 and rows[0][1]["scanned_product_barcodes"]==[MASTER,*products()]
    assert app.durable_commits[-1]["result"]==app.Results.PASS
    assert not app.package_outbox.list_conflicts() and dialogs==[]
    assert business_snapshot(app.package_outbox)==business


@pytest.mark.parametrize('reopen,ack',[(True,True),(None,True),(False,False)])
def test_reopened_unknown_or_unacknowledged_cancel_is_not_a_proven_refused_member(tmp_path,reopen,ack):
    outbox=PackageOutbox(tmp_path/'eligibility.sqlite')
    barcode=products(prefix='held')[0]
    seed_cancelled_members(outbox,barcode,reopen=reopen,ack=ack)
    before=business_snapshot(outbox)
    assert outbox.cancelled_package_for_barcode(barcode) is None
    assert business_snapshot(outbox)==before


def test_known_source_rows_exclude_unselected_members_and_unknown_barcodes(tmp_path):
    outbox=PackageOutbox(tmp_path/'selected.sqlite')
    barcode=products(prefix='held')[0]
    seed_cancelled_members(outbox,barcode)
    before=business_snapshot(outbox)
    assert outbox.cancelled_package_for_barcode(barcode)
    assert outbox.cancelled_package_for_barcode(barcode+'-remainder') is None
    assert outbox.cancelled_package_for_barcode(products()[0]) is None
    assert outbox.cancelled_package_for_barcode('') is None
    assert business_snapshot(outbox)==before

"""§15 display-only recovery: finished rows stay visible and never release a repack."""
import copy
import json
import sqlite3
from dataclasses import replace

import pytest
import Label_Match as lm
from label_screen_bridge import LabelMatchScreenProcessor, _alerts
from package_logistics import PackageApiError, PackageOutbox, PackageCancellationOutbox
from sealed_transfer_exchange import SealedTransferExchangeStore
from tests.test_lm_scan_cancel import app_for_delete
from tests.test_package_logistics import _draft_for_set, _cancellation_intent
from tests.test_u4_cancellation_order import queued
from tests.test_u4_repack_pair import pair, cancel_local, enqueue_repack
from tests.test_w9scanonly_central import central  # noqa: F401
from tests.test_w9scanonly_packaging import packaging, scan  # noqa: F401
from tests.test_deferred_lease_clock_retry import clock_case  # noqa: F401


def business_snapshot(outbox):
    with sqlite3.connect(outbox.db_path) as conn:
        return [conn.execute('SELECT * FROM '+table+' ORDER BY rowid').fetchall() for table in (
            'package_command_outbox','package_cancellation_outbox',
            'package_post_review_outbox','package_workbench_hold_audit')]


def prepare_recovery_app(app, outbox, root):
    root.mkdir(parents=True,exist_ok=True)
    app.run_tests = True
    app.save_directory = str(root)
    app.package_outbox = outbox
    app.package_cancellation_outbox = PackageCancellationOutbox(outbox.db_path)
    app.sealed_transfer_exchange_store = SealedTransferExchangeStore(outbox.db_path)
    app.current_set_info = {'id':None,'raw':[],'normalized':[]}
    app._package_recovery_state_data = None
    app.phs_label_exchange_coordinator = None
    app._admin_pin_store = None
    return app


def assert_visible(app, expected_ids, full_label):
    candidates = app._show_package_recovery_workbench()
    selected = [row for row in candidates if row.get('repack_review')]
    assert {row['set_id'] for row in selected} == set(expected_ids)
    assert all(row['current'] is None and row['package']['local_completion_committed']==1 for row in selected)
    assert all(row['repack_review']['label']=='관리자 회수 대기' for row in selected)
    assert all(row['repack_review']['source']=='현품표 '+full_label for row in selected)
    processor = LabelMatchScreenProcessor(app,module=lm)
    try:
        processor._load_recovery()
        visible = [row for row in processor._recovery['rows'] if row['set_id'] in expected_ids]
        assert len(visible)==len(expected_ids)
        for row in visible:
            assert row['tag']=='관리자 회수 대기' and row['item']=='현품표 '+full_label
            assert row['status'] in {'회사 서버가 다시 포장을 거절했습니다\n웹 사무실에서 현장 관리자가 제품을 새 이적 묶음으로 회수해야 합니다','이전 포장 순서를 확인할 수 없습니다\n웹 사무실에서 현장 관리자가 제품을 새 이적 묶음으로 회수해야 합니다'}
            assert row['actions']==[]
    finally:
        processor.uninstall()


@pytest.mark.parametrize('kind',['v11_pending','history_acked','history_conflict','refused'])
def test_completed_review_is_visible_without_current_file_and_reads_preserve_all_business_evidence(tmp_path,kind):
    if kind=='refused':
        app,server,create,cancel,original,master = pair(tmp_path,mode='refused')
        cancel_local(app,'f2')
        assert cancel.drain()['acked']==1
        repack = enqueue_repack(app,server,master)
        outbox=app.package_outbox
        expected=[repack.set_id]
    else:
        outbox=PackageOutbox(tmp_path/'outbox.sqlite3')
        cancels=PackageCancellationOutbox(outbox.db_path)
        drafts=[_draft_for_set(name) for name in ('A','B','C')]
        for draft in reversed(drafts):
            outbox.enqueue(draft)
        for draft in drafts:
            outbox.mark_local_completion_committed(outbox.get_by_set_id(draft.set_id)['idempotency_key'])
            if draft is not drafts[-1]:
                intent=_cancellation_intent(draft)
                cancels.enqueue(intent)
                cancels.mark_local_event_committed(intent.cancellation_event_id)
        with sqlite3.connect(outbox.db_path) as conn:
            conn.execute('DROP INDEX ix_package_local_completion_order')
            conn.execute('ALTER TABLE package_command_outbox DROP COLUMN predecessor_package_key')
            conn.execute('ALTER TABLE package_command_outbox DROP COLUMN local_completion_order')
            conn.execute("UPDATE package_outbox_schema_info SET value='label-match-package-outbox-v11' WHERE key='schema_version'")
            if kind!='v11_pending':
                conn.execute("UPDATE package_command_outbox SET status=?,last_error_code='OLD_EVIDENCE'",
                             ('ACKED' if kind=='history_acked' else 'CONFLICT',))
        outbox=PackageOutbox(outbox.db_path)
        expected=[draft.set_id for draft in drafts]
        if kind!='v11_pending':
            draft=_draft_for_set('later')
            queued(outbox,draft)
            expected=[draft.set_id]
        master=drafts[0].source_external_label
        app=app_for_delete(tmp_path,central=False)
    prepare_recovery_app(app,outbox,tmp_path/'empty-current')
    assert outbox.list_local_completion_pending()==[]
    assert not list((tmp_path/'empty-current').iterdir())
    before=business_snapshot(outbox)
    assert_visible(app,expected,master)
    assert business_snapshot(outbox)==before
    app.package_outbox=PackageOutbox(outbox.db_path)
    assert_visible(app,expected,master)
    assert app.package_outbox.claim_next() is None
    assert business_snapshot(outbox)==before


@pytest.mark.parametrize('match',['source_label','input_tag_id'])
def test_same_phs2_scan_shows_fixed_band_without_new_set_network_pin_or_queue_writes(central,match):
    app,dialogs,case,root,_releases=central
    master=case.group['scan_payload']
    fields=lm._label_match_parse_compact_phs2(master)
    draft=replace(_draft_for_set('blocked'),
                  source_input_tag_id=fields['ITG'] if match=='input_tag_id' else '',
                  source_external_label=master if match=='source_label' else 'prior-label')
    row=queued(app.package_outbox,draft)
    claim=app.package_outbox.claim_next()
    assert claim['idempotency_key']==row['idempotency_key']
    app.package_outbox.mark_conflict(row['idempotency_key'],
        PackageApiError(409,'REPACK_REOPEN_REFUSED','explicit refusal'),expected_claim=claim)
    before=business_snapshot(app.package_outbox)
    current=copy.deepcopy(app.current_set_info)
    def forbidden(*a,**k):
        pytest.fail('display-only review must not request network approval or open a set')
    app.package_logistics_client.resolve_package_source_evidence=forbidden
    app._submit_package_pin_action=forbidden
    for _ in range(2):
        scan(app,master)
        notice=app._phs_label_guidance_notice
        assert notice.title=='이 현품표는 관리자 회수 대기입니다'
        assert notice.message=='제품은 따로 두고 현장 관리자에게 알리세요\n다른 현품표 작업은 계속할 수 있습니다'
        assert app.current_set_info==current
        assert not app._ui_lane_is_busy()
    assert dialogs==[]
    processor=LabelMatchScreenProcessor(app,module=lm)
    try:
        bands=_alerts(processor,None,app._phs_label_guidance_notice)
        assert bands[0]['text']=='이 현품표는 관리자 회수 대기입니다\n제품은 따로 두고 현장 관리자에게 알리세요\n다른 현품표 작업은 계속할 수 있습니다'
        assert 'choices' not in bands[0]
    finally:
        processor.uninstall()
    assert business_snapshot(app.package_outbox)==before
    assert app.package_outbox.repack_review_for_source('another-label','another-tag') is None

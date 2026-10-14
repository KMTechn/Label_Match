import copy
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from package_logistics import PackageOutbox, PackageCancellationOutbox
from package_errors import PackageLogisticsError
from tests.test_package_logistics import _draft_for_set, _cancellation_intent
from tests.test_u4_cancellation_order import queued
from tests.test_package_recovery_read import evidence


def held(tmp_path):
    outbox=PackageOutbox(tmp_path/'recovery.db')
    original=_draft_for_set('recovery-original')
    queued(outbox,original)
    row=outbox.claim_next()
    expected,response=evidence()
    creation=dict(receipt_id='creation-a',data={k:v for k,v in expected.items() if k!='cancel_receipt_id'})
    cancellation=dict(receipt_id=expected['cancel_receipt_id'],data=dict(creation['data'],reopen=False))
    outbox.mark_acked(row['idempotency_key'],creation)
    cancels=PackageCancellationOutbox(outbox.db_path)
    cancels.enqueue(_cancellation_intent(original))
    with sqlite3.connect(outbox.db_path) as conn:
        conn.execute("UPDATE package_cancellation_outbox SET status='ACKED',receipt_json=?",(json.dumps(cancellation),))
    blocked=_draft_for_set('recovery-blocked')
    queued(outbox,blocked)
    assert outbox.get_by_set_id(blocked.set_id)['status']=='CONFLICT'
    return outbox,original,blocked,response


def immutable(outbox):
    with sqlite3.connect(outbox.db_path) as conn:
        return [conn.execute('SELECT * FROM '+table+' ORDER BY 1').fetchall() for table in
                ('package_command_outbox','package_cancellation_outbox','package_post_review_outbox')]


def test_recovery_closes_once_with_audit_preserves_originals_and_never_requeues_old_create(tmp_path):
    outbox,original,blocked,response=held(tmp_path)
    target=outbox.package_recovery_target(blocked.set_id)
    before=immutable(outbox)
    assert outbox.close_package_recovery(target,response,operator_id='worker-a',operator_name='작업자')
    assert not outbox.close_package_recovery(target,response,operator_id='worker-a')
    assert immutable(outbox)==before
    assert outbox.list_repack_reviews()==[]
    assert outbox.claim_next() is None
    restarted=PackageOutbox(outbox.db_path)
    assert restarted.list_repack_reviews()==[]
    assert immutable(restarted)==before
    with sqlite3.connect(outbox.db_path) as conn:
        assert conn.execute('SELECT count(*) FROM package_recovery_evidence').fetchone()[0]==1
        assert conn.execute('SELECT count(*) FROM package_recovery_review_closures').fetchone()[0]==2
        assert conn.execute('SELECT operator_id FROM package_recovery_evidence').fetchone()[0]=='worker-a'
    next_draft=_draft_for_set('recovery-new')
    queued(restarted,next_draft)
    claimed=restarted.claim_next()
    assert claimed['set_id']==next_draft.set_id and claimed['predecessor_package_key'] is None


def test_two_concurrent_rechecks_close_once(tmp_path):
    outbox,_,blocked,response=held(tmp_path)
    target=outbox.package_recovery_target(blocked.set_id)
    def close(_):
        return outbox.close_package_recovery(target,response,operator_id='worker-a')
    with ThreadPoolExecutor(max_workers=2) as workers:
        assert sorted(workers.map(close,range(2)))==[False,True]


@pytest.mark.parametrize('change',['review','cancel'])
def test_local_cas_change_while_reading_does_not_release(tmp_path,change):
    outbox,_,blocked,response=held(tmp_path)
    target=outbox.package_recovery_target(blocked.set_id)
    with sqlite3.connect(outbox.db_path) as conn:
        if change=='review':
            conn.execute("UPDATE package_command_outbox SET updated_at='changed' WHERE set_id=?",(blocked.set_id,))
        else:
            conn.execute("UPDATE package_cancellation_outbox SET updated_at='changed'")
    before=immutable(outbox)
    assert not outbox.close_package_recovery(target,response,operator_id='worker-a')
    assert immutable(outbox)==before and outbox.list_repack_reviews()


def test_v12_migration_is_additive_repeatable_and_corrupt_recovery_stays_closed(tmp_path):
    outbox,_,blocked,response=held(tmp_path)
    before=immutable(outbox)
    with sqlite3.connect(outbox.db_path) as conn:
        conn.execute("UPDATE package_outbox_schema_info SET value='label-match-package-outbox-v12' WHERE key='schema_version'")
        conn.execute('DROP TABLE package_recovery_review_closures')
        conn.execute('DROP TABLE package_recovery_evidence')
    outbox=PackageOutbox(outbox.db_path)
    outbox=PackageOutbox(outbox.db_path)
    assert immutable(outbox)==before
    assert outbox.close_package_recovery(outbox.package_recovery_target(blocked.set_id),response,operator_id='worker-a')
    with sqlite3.connect(outbox.db_path) as conn:
        conn.execute("UPDATE package_recovery_evidence SET contract_sha256='unknown'")
    with pytest.raises(PackageLogisticsError):
        PackageOutbox(outbox.db_path)

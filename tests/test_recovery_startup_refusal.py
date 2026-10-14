import json
import sqlite3

import pytest
import Label_Match as lm
import startup_observation
from package_errors import PackageRecoveryEvidenceError
from package_logistics import PackageOutbox, PackageCancellationOutbox
from tests.test_package_recovery_ledger import held, immutable
from tests.test_package_logistics import _draft_for_set
from tests.test_u4_cancellation_order import queued
from tests.recovery_startup_helper import configure_main


def test_fresh_v13_review_rows_always_keep_their_completion_predecessor(tmp_path):
    outbox,original,blocked,response=held(tmp_path)
    assert outbox.get_by_set_id(original.set_id)['predecessor_package_key'] is None
    original_key=outbox.get_by_set_id(original.set_id)['idempotency_key']
    previous=outbox.get_by_set_id(blocked.set_id)
    assert previous['predecessor_package_key']==original_key
    for index in range(3):
        draft=_draft_for_set('fresh-v13-following-'+str(index))
        queued(outbox,draft)
        current=outbox.get_by_set_id(draft.set_id)
        assert current['status']=='CONFLICT'
        assert current['last_error_code']=='REPACK_PREDECESSOR_REVIEW_REQUIRED'
        assert current['predecessor_package_key']==previous['idempotency_key']
        assert current['local_completion_order']>previous['local_completion_order']
        previous=current
        outbox=PackageOutbox(outbox.db_path)
        assert outbox.get_by_set_id(draft.set_id)==current
    assert all(row['predecessor_package_key'] for row in outbox.list_repack_reviews())
    before=immutable(outbox)
    assert outbox.close_package_recovery(outbox.package_recovery_target(blocked.set_id),response,operator_id='worker-a')
    assert immutable(outbox)==before and outbox.list_repack_reviews()==[]
    outbox=PackageOutbox(outbox.db_path)
    draft=_draft_for_set('fresh-after-verified-recovery')
    queued(outbox,draft)
    current=outbox.get_by_set_id(draft.set_id)
    assert current['predecessor_package_key'] is None and current['status']=='PENDING'
    assert current['local_completion_order'] is not None
    assert outbox.claim_next()['set_id']==draft.set_id


def corrupt(outbox,kind):
    with sqlite3.connect(outbox.db_path) as conn:
        if kind=='relation':
            conn.execute("UPDATE package_recovery_evidence SET package_key='unrelated'")
        elif kind=='hash':
            saved=json.loads(conn.execute('SELECT evidence_json FROM package_recovery_evidence').fetchone()[0])
            saved['membership_hash']='corrupt'
            conn.execute('UPDATE package_recovery_evidence SET evidence_json=?',(json.dumps(saved),))
        elif kind in ('evidence-json','evidence-array','expectation-json'):
            field='expectation_json' if kind=='expectation-json' else 'evidence_json'
            conn.execute('UPDATE package_recovery_evidence SET '+field+'=?',('[]' if kind=='evidence-array' else '{',))
        elif kind in ('snapshot-json','snapshot-array','snapshot-missing'):
            conn.execute('UPDATE package_recovery_review_closures SET review_snapshot_json=?',
                         ({'snapshot-json':'{','snapshot-array':'[]','snapshot-missing':'{}'}[kind],))
        elif kind=='original-json':
            conn.execute("UPDATE package_command_outbox SET receipt_json='{' WHERE status='ACKED'")
        else:
            conn.execute("UPDATE package_cancellation_outbox SET receipt_json='{}'")


@pytest.mark.parametrize('kind',['relation','hash','evidence-json','evidence-array','expectation-json',
                                  'snapshot-json','snapshot-array','snapshot-missing','original-json','cancel-missing'])
def test_corrupt_recovery_refuses_main_with_fixed_notice_exit_and_unchanged_db(tmp_path,monkeypatch,capsys,kind):
    outbox,_,blocked,response=held(tmp_path)
    assert outbox.close_package_recovery(outbox.package_recovery_target(blocked.set_id),response,operator_id='worker-a')
    corrupt(outbox,kind)
    path=tmp_path/'recovery.db'
    before=path.read_bytes()
    rows=immutable(outbox)
    for store in (PackageOutbox,PackageCancellationOutbox):
        with pytest.raises(PackageRecoveryEvidenceError) as failure:
            store(path)
        assert failure.value.code==PackageRecoveryEvidenceError.code
        assert str(failure.value)==str(PackageRecoveryEvidenceError())
        assert path.read_bytes()==before and immutable(outbox)==rows

    observe=startup_observation.observe_startup
    configure_main(monkeypatch,tmp_path,path)
    notices,observations=[],[]
    startup_observation.begin_startup({})
    monkeypatch.setattr(startup_observation,'observe_startup',observe)
    monkeypatch.setattr(startup_observation,'write_startup_result',lambda result:observations.append(result))
    monkeypatch.setattr(startup_observation,'show_startup_blocked_notice',lambda:notices.append(startup_observation.STARTUP_BLOCKED_MESSAGE))
    assert lm.main([])==lm.ONBOARDING_EXIT_CODE
    assert observations[-1]['reasons']==['DATA_GENERATION_REFUSED']
    assert observations[-1]['startable'] is False
    assert notices==[startup_observation.STARTUP_BLOCKED_MESSAGE]
    assert capsys.readouterr().err==str(PackageRecoveryEvidenceError())+'\n'
    assert path.read_bytes()==before and immutable(outbox)==rows


def test_valid_recovery_reopens_and_preserves_business_records_and_recovery_proof(tmp_path):
    outbox,_,blocked,response=held(tmp_path)
    assert outbox.close_package_recovery(outbox.package_recovery_target(blocked.set_id),response,operator_id='worker-a')
    path=tmp_path/'recovery.db'
    rows=immutable(outbox)
    def proof_rows():
        with sqlite3.connect(path) as conn:
            return [conn.execute('SELECT * FROM '+table+' ORDER BY 1').fetchall() for table in
                    ('package_recovery_evidence','package_recovery_review_closures')]
    proofs=proof_rows()
    for store in (PackageOutbox,PackageCancellationOutbox):
        store(path)
        assert immutable(outbox)==rows and proof_rows()==proofs

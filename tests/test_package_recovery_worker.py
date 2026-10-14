import copy
import json
import sqlite3
from types import SimpleNamespace

import pytest
import Label_Match as lm
from label_screen_bridge import LabelMatchScreenProcessor
from package_logistics import PackageApiError, PackageOutbox, PackageTransportError
from package_recovery import AWAITING_NOTICE, RECOVERED_NOTICE, HELD_NOTICE
from tests.test_package_recovery_ledger import held, immutable
from tests.test_lm_scan_cancel import app_for_delete
from tests.test_u4_repack_visibility import prepare_recovery_app


def app_case(tmp_path):
    outbox,original,blocked,response=held(tmp_path)
    app=prepare_recovery_app(app_for_delete(tmp_path,central=True),outbox,tmp_path/'app')
    app.worker_id='worker-a'
    app.worker_name='작업자'
    notices=[]
    app._show_scanner_notice=lambda title,message:notices.append(title+'\n'+message)
    calls=[]
    def read(expected):
        calls.append(copy.deepcopy(expected))
        return copy.deepcopy(response)
    app.package_logistics_client=SimpleNamespace(get_package_recovery=read)
    return app,blocked,response,notices,calls


def test_worker_bridge_recheck_needs_no_pin_closes_once_and_returns_exact_text(tmp_path):
    app,blocked,response,notices,calls=app_case(tmp_path)
    def forbidden(*args,**kwargs):
        pytest.fail('worker recovery must not enter administrator PIN/write path')
    app._submit_package_pin_action=forbidden
    before=immutable(app.package_outbox)
    screen=LabelMatchScreenProcessor(app,module=lm)
    try:
        assert screen.command('LABEL.RECHECK',{'target':blocked.set_id})['ok']
        assert screen._admin_result['text']==RECOVERED_NOTICE
        assert notices==[RECOVERED_NOTICE]
        assert len(calls)==1 and calls[0]['cancel_receipt_id']=='cancel-a'
        assert not screen.command('LABEL.RECHECK',{'target':blocked.set_id})['ok']
        assert immutable(app.package_outbox)==before and app.package_outbox.claim_next() is None
    finally:
        screen.uninstall()


@pytest.mark.parametrize('fault',['404','timeout','unknown','wrong-cancel','wrong-members','consumed'])
def test_missing_error_unknown_and_mismatched_read_keep_exact_hold_without_audit(tmp_path,fault):
    app,blocked,response,notices,calls=app_case(tmp_path)
    if fault in ('404','timeout'):
        def fail(_):
            if fault=='404':
                raise PackageApiError(404,'PACKAGE_NOT_FOUND','missing')
            raise PackageTransportError('timeout')
        app.package_logistics_client.get_package_recovery=fail
    elif fault=='unknown': response['contract_version']='unknown'
    elif fault=='wrong-cancel': response['cancel_receipt_id']='wrong'
    elif fault=='wrong-members': response['member_ids']=['unit-a']
    else: response['transfer_state']='CONSUMED'
    before=immutable(app.package_outbox)
    result=app._recheck_package_recovery(blocked.set_id)
    assert result==dict(state='AWAITING',text=AWAITING_NOTICE)
    assert notices==[AWAITING_NOTICE] and immutable(app.package_outbox)==before
    assert app.package_outbox.list_repack_reviews()
    with sqlite3.connect(app.package_outbox.db_path) as conn:
        assert conn.execute('SELECT count(*) FROM package_recovery_evidence').fetchone()[0]==0
        assert conn.execute("SELECT count(*) FROM package_workbench_hold_audit WHERE action LIKE 'PACKAGE_RECOVERY%'").fetchone()[0]==0


@pytest.mark.parametrize('reason',['PACKAGE_CREATION_MOVEMENT_CONFLICT','UNKNOWN'])
def test_only_known_v1_hold_is_named_unrecoverable_and_survives_restart(tmp_path,reason):
    app,blocked,response,notices,calls=app_case(tmp_path)
    response.update(recovery_state='AWAITING_RECOVERY',recovery_receipt_id=None,
                    transfer_bundle_id=None,reopen_reason=reason)
    before=immutable(app.package_outbox)
    result=app._recheck_package_recovery(blocked.set_id)
    known=reason!='UNKNOWN'
    assert result['state']==('HELD' if known else 'AWAITING')
    assert notices==[HELD_NOTICE if known else AWAITING_NOTICE]
    assert immutable(app.package_outbox)==before
    app.package_outbox=PackageOutbox(app.package_outbox.db_path)
    candidates=app._package_recovery_candidates()
    review=next(row['repack_review'] for row in candidates if row['set_id']==blocked.set_id)
    assert review['label']==('회수 불가' if known else '관리자 회수 대기')
    assert app.package_outbox.claim_next() is None

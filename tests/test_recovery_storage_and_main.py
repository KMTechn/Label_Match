import json
import sqlite3
from types import SimpleNamespace

import pytest
import Label_Match as lm
import auto_update
import label_match_single_instance as single_instance
import startup_observation
from package_errors import PackageRecoveryEvidenceError, PackageRecoveryStorageError
from package_logistics import PackageOutbox, PackageCancellationOutbox
from tests.recovery_startup_helper import configure_main
from tests.test_package_recovery_ledger import held, immutable
from tests.test_package_logistics import _draft_for_set
from tests.test_u4_cancellation_order import queued


def recovered(tmp_path):
    outbox,_,blocked,response=held(tmp_path)
    assert outbox.close_package_recovery(outbox.package_recovery_target(blocked.set_id),response,operator_id='worker-a')
    return outbox


def logical_snapshot(outbox):
    with sqlite3.connect(outbox.db_path) as conn:
        return immutable(outbox)+[conn.execute('SELECT * FROM '+table+' ORDER BY 1').fetchall() for table in
                                  ('package_recovery_evidence','package_recovery_review_closures')]


def fail_recovery_query(monkeypatch):
    connect=sqlite3.connect
    reached=[]
    class ReadFailure(sqlite3.Connection):
        def execute(self,sql,*args,**kwargs):
            if sql.strip()=='SELECT * FROM package_recovery_evidence':
                reached.append(True)
                raise sqlite3.OperationalError('injected storage read failure')
            return super().execute(sql,*args,**kwargs)
    def isolated_connect(*args,**kwargs):
        return connect(*args,**dict(kwargs,factory=ReadFailure))
    monkeypatch.setattr(sqlite3,'connect',isolated_connect)
    return reached


def test_valid_evidence_sql_read_error_has_its_own_fixed_code_and_main_refusal(tmp_path,monkeypatch,capsys):
    outbox=recovered(tmp_path)
    before=logical_snapshot(outbox)
    path=tmp_path/'recovery.db'
    reached=fail_recovery_query(monkeypatch)
    for store in (PackageOutbox,PackageCancellationOutbox):
        with pytest.raises(PackageRecoveryStorageError) as failure:
            store(path)
        assert not isinstance(failure.value,PackageRecoveryEvidenceError)
        assert failure.value.code=='PACKAGE_RECOVERY_STORAGE_UNAVAILABLE'
        assert str(failure.value)==str(PackageRecoveryStorageError())
    notices=configure_main(monkeypatch,tmp_path,path)
    assert lm.main([])==lm.ONBOARDING_EXIT_CODE
    assert notices==[(['DATA_GENERATION_REFUSED'],{'refused':True})]
    assert capsys.readouterr().err==str(PackageRecoveryStorageError())+'\n'
    assert PackageRecoveryStorageError.message=='관리자에게 알리세요'
    assert len(reached)==3 and logical_snapshot(outbox)==before


@pytest.mark.parametrize('bad_id',[[],{},1,None,''])
def test_bad_snapshot_binding_is_evidence_failure_before_query(tmp_path,monkeypatch,bad_id):
    outbox=recovered(tmp_path)
    with sqlite3.connect(outbox.db_path) as conn:
        saved=json.loads(conn.execute('SELECT review_snapshot_json FROM package_recovery_review_closures LIMIT 1').fetchone()[0])
        saved['set_id']=bad_id
        conn.execute('UPDATE package_recovery_review_closures SET review_snapshot_json=?',(json.dumps(saved),))
    # If invalid input reached sqlite's binder it would become ProgrammingError.
    # The domain check must reject it before recovery_target executes this query.
    import package_recovery_ledger as ledger
    target=ledger.recovery_target
    reached=[]
    def checked_target(conn,set_id,**kwargs):
        reached.append(set_id)
        return target(conn,set_id,**kwargs)
    monkeypatch.setattr(ledger,'recovery_target',checked_target)
    before=logical_snapshot(outbox)
    with pytest.raises(PackageRecoveryEvidenceError):
        PackageOutbox(outbox.db_path)
    assert reached==[] and logical_snapshot(outbox)==before


@pytest.mark.parametrize('state',['recovered','no-evidence','first-completion','after-recovery-first'])
def test_real_main_guarded_body_opens_valid_storage_without_refusal(tmp_path,monkeypatch,capsys,state):
    if state in ('recovered','after-recovery-first'):
        outbox=recovered(tmp_path)
    else:
        outbox=PackageOutbox(tmp_path/'recovery.db')
    if state in ('first-completion','after-recovery-first'):
        draft=_draft_for_set('positive-first-completion')
        queued(outbox,draft)
        assert outbox.get_by_set_id(draft.set_id)['predecessor_package_key'] is None
    before=logical_snapshot(outbox)
    path=tmp_path/'recovery.db'
    actual_main=lm.main
    actual_body=lm._run_label_match_application
    actual_guard=single_instance.run_guarded_entrypoint
    actual_observe=startup_observation.observe_startup
    configure_main(monkeypatch,tmp_path,path)
    # Replace only external startup inputs and the UI surface. The real main,
    # guarded callback, body, and both store admission implementations run.
    monkeypatch.setattr(lm,'_run_label_match_application',actual_body)
    monkeypatch.setattr(lm,'release_identity',lambda *a,**k:{})
    monkeypatch.setattr(lm,'_startup_app_stop_requested',lambda:False)
    monkeypatch.setattr(lm,'_prepare_startup_catalog_attempt',lambda:(None,False))
    monkeypatch.setattr(auto_update,'trial_context',lambda *a,**k:None)
    monkeypatch.setattr(startup_observation,'observe_startup',actual_observe)
    observations,notices,calls,traces=[],[],[],[]
    monkeypatch.setattr(startup_observation,'write_startup_result',lambda value:observations.append(value))
    monkeypatch.setattr(startup_observation,'show_startup_blocked_notice',lambda:notices.append(True))
    monkeypatch.setattr(lm,'_label_match_startup_trace',lambda event,**kw:traces.append(event))
    monkeypatch.setattr(single_instance,'_write_instance_owner_notice',lambda lease:None)
    monkeypatch.setattr(single_instance,'_try_update_shutdown_stall_notice',lambda *a,**k:None)
    monkeypatch.setattr(single_instance._active_instance_lease,'lease',None,raising=False)
    monkeypatch.setattr(single_instance,'_process_instance_lease',None)
    lease=SimpleNamespace(owner=True,close=lambda:calls.append('lease-close'))
    def guarded(start,**kwargs):
        assert start is actual_body
        calls.append('guard')
        return actual_guard(start,acquire=lambda scope:lease,**kwargs)
    monkeypatch.setattr(lm,'run_guarded_entrypoint',guarded)
    class HeadlessSurface:
        def __init__(self):
            calls.append('ui-init')
            self.package_outbox=PackageOutbox(path)
            self.package_cancellation_outbox=PackageCancellationOutbox(path)
        def title(self): return 'headless'
        def state(self): return 'withdrawn'
        def mainloop(self): calls.append('mainloop')
        def _finish_after_mainloop(self): calls.append('finish')
    monkeypatch.setattr(lm,'Label_Match',HeadlessSurface)
    assert lm.main is actual_main
    assert lm.main(['--old-screen'])==0
    assert calls==['guard','ui-init','mainloop','finish','lease-close']
    assert 'main_after_app_init' in traces and 'mainloop_exit' in traces
    assert observations and all(value['startable'] and value['reasons']==[] for value in observations)
    assert notices==[] and capsys.readouterr().err==''
    assert logical_snapshot(outbox)==before

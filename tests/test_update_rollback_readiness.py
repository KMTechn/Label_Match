from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

import auto_update as updates
import current_user_onboarding
from tests.test_relay_trial_roles import pending


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(json.dumps(value).encode())


def row(state, sid):
    return {'pid':71,'role':'relay','sha':state['previous_sha'],'sid':sid,
            'ordinary_user':True,'observable':True,
            'created_at':(datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat()}


@pytest.fixture
def recovery(tmp_path, monkeypatch):
    control=tmp_path/'update-control'; stage=tmp_path/'stage'
    state=pending('ROLLBACK_PENDING_READY')
    policy={'operator_sid':'S-1-5-21-1-2-3-4','staging_root':str(stage)}
    native=row(state,policy['operator_sid'])
    claim={'pid':native['pid'],'role':'relay','nonce':state['nonce'],
           'transition_id':state['transition_id'],'claimed_at':datetime.now(timezone.utc).isoformat()}
    receipt={**{name:True for name in updates.RELAY_READY_CHECKS},'role':'relay','pid':native['pid'],
             'sha':state['previous_sha'],'nonce':state['nonce'],'transition_id':state['transition_id'],
             'ready_at':datetime.now(timezone.utc).isoformat()}
    write(control/'transition.json',state);write(control/'active.json',{'sha':state['previous_sha']})
    write(stage/'trial.json',claim)
    depth=[]
    @contextmanager
    def lock(*args,**kwargs):
        assert not depth
        depth.append(True)
        try:yield
        finally:depth.pop()
    monkeypatch.setattr(updates,'update_lock',lock)
    monkeypatch.setattr(updates,'write_json_durable',write)
    monkeypatch.setattr(updates,'load_policy',lambda root:policy)
    monkeypatch.setattr(updates,'process_snapshot',lambda root:[native])
    monkeypatch.setattr(updates,'record_confirmed_transition',lambda root,value:value)
    monkeypatch.setattr(updates,'launch_version',lambda *a,**kw:pytest.fail('SYSTEM launched a user process'))
    return control,stage,state,policy,native,receipt,depth


@pytest.mark.parametrize('proof',['ready','no_receipt','wrong_role','wrong_sid','wrong_sha','not_ordinary'])
def test_system_recovery_requires_actual_bound_ready(recovery,proof):
    control,stage,state,policy,native,receipt,depth=recovery
    if proof == 'wrong_role':receipt['role']='gui'
    if proof == 'wrong_sid':native['sid']='S-1-5-21-9-8-7-6'
    if proof == 'wrong_sha':native['sha']=state['candidate_sha']
    if proof == 'not_ordinary':native['ordinary_user']=False
    if proof != 'no_receipt':write(stage/'ready.json',receipt)
    result=updates.wait_for_rollback_ready(control,policy,timeout_seconds=.08)
    actual=updates.read_json(control/'transition.json')
    assert result == (0 if proof == 'ready' else 20)
    assert actual['phase'] == ('ROLLED_BACK' if proof == 'ready' else 'FAILED')
    assert updates.read_json(control/'active.json')['sha'] == state['previous_sha']
    assert depth == []


def test_apply_unwinds_ownership_before_waiting_for_recovery(recovery,monkeypatch):
    control,stage,state,policy,native,receipt,depth=recovery
    def apply_once(root):
        with updates.update_lock(root,policy['operator_sid']):
            raise updates._RollbackReadyPending
    monkeypatch.setattr(updates,'_apply_prepared_once',apply_once)
    write(stage/'ready.json',receipt)
    assert updates._apply_prepared(control) == 0
    assert depth == []


def test_pointer_restoration_stays_pending_until_relay_ready():
    state=pending()
    state=updates.request_rollback(state,reason='candidate failed')
    state={**state,'failure_decision':{'selected_sha':state['previous_sha']}}
    result=updates.rollback_transition(state,writers_absent=True,previous_verified=True,
        durable_rollback=True,nonce='9'*32,now=datetime.now(timezone.utc).isoformat())
    assert result['phase'] == 'ROLLBACK_PENDING_READY'
    assert result['trial_pid'] is None and 'confirmed_at' not in result


@pytest.mark.parametrize('phase',['APPLIED_PENDING_READY','ROLLBACK_PENDING_READY'])
@pytest.mark.parametrize('proof',['valid','wrong_claim_role','wrong_pid','second_relay','wrong_source','not_ordinary'])
def test_parent_bootstrap_publishes_readonly_relay_ready_without_self_nonce(tmp_path,monkeypatch,phase,proof):
    state=pending(phase)
    control=tmp_path/'update-control';stage=tmp_path/'stage'; app=tmp_path/'version/app'
    policy={'operator_sid':'S-1-5-21-1-2-3-4','staging_root':str(stage)}
    sha=state['previous_sha'] if phase == 'ROLLBACK_PENDING_READY' else state['candidate_sha']
    native=row(state,policy['operator_sid']);native['sha']=sha
    claim={'pid':71,'role':'relay','nonce':state['nonce'],'transition_id':state['transition_id'],
           'claimed_at':datetime.now(timezone.utc).isoformat()}
    if proof == 'wrong_claim_role':claim['role']='gui'
    if proof == 'wrong_pid':claim['pid']=72
    if proof == 'wrong_source':native['sha']='f'*40
    if proof == 'not_ordinary':native['ordinary_user']=False
    paths=SimpleNamespace(ledger_path=tmp_path/'ledger.sqlite3',direct_sync_root=tmp_path/'relay',data_root=tmp_path/'data')
    paths.data_root.mkdir();(paths.direct_sync_root/'queue').mkdir(parents=True)
    for database in (paths.ledger_path,paths.direct_sync_root/'queue/direct_sync_relay.sqlite3'):
        with sqlite3.connect(database) as connection:connection.execute('create table sample (id integer)')
    from user_relay import user_relay_status_path
    write(user_relay_status_path(paths.direct_sync_root),{'status':'RUNNING','source_commit':sha,
        'update_nonce':state['nonce'],'ordinary_user':True,'process_id':71,'updated_at':datetime.now(timezone.utc).isoformat()})
    write(control/'transition.json',state);write(control/'active.json',{'sha':sha});write(stage/'trial.json',claim)
    monkeypatch.setattr(updates,'trial_context',lambda root:pytest.fail('parent used its self-trial context'))
    monkeypatch.setattr(updates,'control_root',lambda root:control)
    monkeypatch.setattr(updates,'load_policy',lambda root:policy)
    monkeypatch.setattr(updates,'ordinary_user_context',lambda:True)
    monkeypatch.setattr(updates,'version_manifest',lambda root:{'source_commit':sha})
    monkeypatch.setattr(updates,'process_snapshot',lambda root:[native]*(2 if proof == 'second_relay' else 1))
    monkeypatch.setattr(updates,'write_json_durable',write)
    monkeypatch.setattr(current_user_onboarding,'resolve_current_user_onboarding_paths',lambda root:paths)
    monkeypatch.setattr(current_user_onboarding,'inspect_current_user_state',lambda value:{'status':'READY'})
    before={p:p.read_bytes() for p in tmp_path.rglob('*') if p.is_file()}
    assert updates.publish_relay_ready(app,71) is (proof == 'valid')
    if proof == 'valid':
        receipt=updates.read_json(stage/'ready.json')
        assert receipt['role'] == 'relay' and receipt['pid'] == 71 and receipt['sha'] == sha
        assert all(receipt[name] is True for name in updates.RELAY_READY_CHECKS)
        assert 'gui_initialized' not in receipt
    else:assert not (stage/'ready.json').exists()
    assert all(p.read_bytes() == data for p,data in before.items())

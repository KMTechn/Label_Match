"""Deferred envelopes bind the original completion to the real signed lease snapshot."""
import copy
import json
import sqlite3
from datetime import datetime,timedelta,timezone
from types import SimpleNamespace

import pytest
import Label_Match as lm
import terminal_operation_lease as lease_module
from terminal_operation_lease import (
    ARTIFACT_CONTRACT_VERSION, OperationLeaseError, OperationLeaseStore,
    PinnedOperationLeaseKeyring, canonical_sha256, normalize_issue_artifact,
)
from tests.test_terminal_operation_lease import Signer, binding, keyring, payload


@pytest.fixture
def deferred(tmp_path):
    now=datetime(2026,10,15,1,0,tzinfo=timezone.utc)
    local={'deferred_reflection':True,'local_completion_id':'original-local-completion',
           'local_completed_at':(now-timedelta(days=2)).isoformat(timespec='microseconds'),
           'server_received_at':now.strftime('%Y-%m-%dT%H:%M:%SZ')}
    snapshot={'source':{'member_ids':['u1','u2','u3']},**local}
    signer=Signer()
    claims=payload(snapshot,now=now)
    artifact={'contract_version':ARTIFACT_CONTRACT_VERSION,'lease_id':claims['lease_id'],
              'status':'ACTIVE','replayed':False,'token':signer.sign(claims),'kid':signer.kid,
              'expires_at':claims['expires_at'],'fence':claims['fence'],
              'snapshot_hash':claims['snapshot_hash'],'operation_snapshot':snapshot,
              'keyring':keyring(signer),**local}
    verifier=PinnedOperationLeaseKeyring(tmp_path/'pins.json')
    verifier.bootstrap_authenticated(artifact['keyring'],authenticated_online=True)
    return artifact,signer,claims,verifier,now


def verify(deferred,artifact):
    _original,_signer,_claims,verifier,now=deferred
    normalized=normalize_issue_artifact(artifact)
    claims=verifier.verify(normalized['token'],expected=binding(),
                           operation_snapshot=normalized['operation_snapshot'],now=now)
    return normalized,claims


def test_deferred_issue_accepts_offline_completion_before_issue_and_retains_original_fields(deferred):
    artifact,*_=deferred
    normalized,claims=verify(deferred,artifact)
    assert normalized==artifact
    assert claims['snapshot_hash']==canonical_sha256(artifact['operation_snapshot'])
    assert claims['fence']==artifact['fence']


@pytest.mark.parametrize('position',['envelope','claims'])
def test_unknown_deferred_field_remains_rejected(deferred,position):
    artifact,signer,claims,*_=deferred
    artifact=copy.deepcopy(artifact)
    if position=='envelope':
        artifact['future_unrecognized_field']=True
    else:
        claims={**claims,'deferred_reflection':True}
        artifact['token']=signer.sign(claims)
    with pytest.raises(OperationLeaseError):
        verify(deferred,artifact)


def test_deferred_envelope_must_match_the_snapshot_completion(deferred):
    artifact,*_=deferred
    changed=copy.deepcopy(artifact)
    changed['operation_snapshot']['local_completion_id']='another-completion'
    with pytest.raises(OperationLeaseError) as denied:
        verify(deferred,changed)
    assert denied.value.code=='OPERATION_LEASE_ARTIFACT_MISMATCH'


@pytest.mark.parametrize('field,value',[
    ('deferred_reflection',False),('deferred_reflection',1),
    ('local_completion_id',''),('local_completion_id',' completion '),
    ('local_completed_at','2026-10-15T01:00:00'),
    ('local_completed_at','2026-10-15T01:00:00+01:60'),
    ('local_completed_at','2026-02-30T01:00:00Z'),
    ('server_received_at','2026-10-15T01:00:00+00:00'),
    ('server_received_at',False),
])
def test_known_deferred_fields_require_exact_types_and_aware_valid_times(deferred,field,value):
    artifact,*_=deferred
    artifact=copy.deepcopy(artifact)
    artifact[field]=value
    artifact['operation_snapshot'][field]=value
    with pytest.raises(OperationLeaseError):
        verify(deferred,artifact)


@pytest.mark.parametrize('field',['deferred_reflection','local_completion_id','local_completed_at','server_received_at'])
def test_partial_deferred_group_is_rejected(deferred,field):
    artifact,*_=deferred
    artifact=copy.deepcopy(artifact)
    del artifact[field]
    with pytest.raises(OperationLeaseError):
        verify(deferred,artifact)


def test_deferred_snapshot_substitution_and_signature_tamper_are_rejected(deferred):
    artifact,*_=deferred
    changed=copy.deepcopy(artifact)
    changed['local_completion_id']='another-completion'
    changed['operation_snapshot']['local_completion_id']=changed['local_completion_id']
    with pytest.raises(OperationLeaseError) as mismatch:
        verify(deferred,changed)
    assert mismatch.value.code=='OPERATION_LEASE_SNAPSHOT_MISMATCH'
    changed=copy.deepcopy(artifact)
    pieces=changed['token'].split('.')
    pieces[-1]=('A' if pieces[-1][0]!='A' else 'B')+pieces[-1][1:]
    changed['token']='.'.join(pieces)
    with pytest.raises(OperationLeaseError) as tamper:
        verify(deferred,changed)
    assert tamper.value.code=='OPERATION_LEASE_SIGNATURE_INVALID'


@pytest.mark.parametrize('field,offset',[
    ('local_completed_at',31),('server_received_at',31),('server_received_at',-31),
])
def test_even_a_valid_signature_cannot_authorize_inconsistent_completion_times(deferred,field,offset):
    artifact,signer,claims,_verifier,now=deferred
    artifact=copy.deepcopy(artifact)
    value=(now+timedelta(seconds=offset)).strftime('%Y-%m-%dT%H:%M:%SZ')
    artifact[field]=value
    artifact['operation_snapshot'][field]=value
    claims={**claims,'snapshot_hash':canonical_sha256(artifact['operation_snapshot'])}
    artifact.update(snapshot_hash=claims['snapshot_hash'],token=signer.sign(claims))
    with pytest.raises(OperationLeaseError) as denied:
        verify(deferred,artifact)
    assert denied.value.code=='OPERATION_LEASE_TIME_INVALID'


@pytest.mark.parametrize('fence',[True,0,-1,'1'])
def test_deferred_fence_requires_a_positive_integer(deferred,fence):
    artifact,*_=deferred
    artifact={**artifact,'fence':fence}
    with pytest.raises(OperationLeaseError):
        normalize_issue_artifact(artifact)


@pytest.mark.parametrize('field',['fence','expires_at','snapshot_hash','lease_id'])
def test_app_verifier_rejects_deferred_envelope_metadata_different_from_valid_signature(deferred,monkeypatch,field):
    artifact,_signer,_claims,verifier,now=deferred
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):
            return now.astimezone(tz)
    monkeypatch.setattr(lease_module,'datetime',Clock)
    # Only domain extraction uses a unit fixture; the app's actual envelope,
    # pinned ES256 signature, snapshot and metadata checks all execute.
    app=lm.Label_Match.__new__(lm.Label_Match)
    app.package_logistics_client=SimpleNamespace(config=None)
    app.package_operation_lease_store=object()
    app.package_operation_lease_keyring=verifier
    app._central_phs2_response_parts=lambda *args:(None,{},None)
    monkeypatch.setattr(lm,'_label_match_operation_lease_binding',lambda *args:binding())
    changed=copy.deepcopy(artifact)
    changed[field]={'fence':artifact['fence']+1,
                    'expires_at':(datetime.fromisoformat(artifact['expires_at'].replace('Z','+00:00'))
                                  +timedelta(seconds=1)).strftime('%Y-%m-%dT%H:%M:%SZ'),
                    'snapshot_hash':'a'*64,'lease_id':'another-lease'}[field]
    with pytest.raises(OperationLeaseError) as denied:
        app._verify_operation_lease_artifact(physical_qr='physical-label',artifact=changed,
            issue_idempotency_key='original-issue',authenticated_online=False,persist_artifact=False)
    assert denied.value.code=='OPERATION_LEASE_ARTIFACT_MISMATCH'


def test_non_deferred_issue_remains_accepted_and_snapshot_only_deferred_fields_are_rejected(deferred):
    artifact,signer,claims,*_=deferred
    ordinary=copy.deepcopy(artifact)
    for name in ('deferred_reflection','local_completion_id','local_completed_at','server_received_at'):
        del ordinary[name]
        del ordinary['operation_snapshot'][name]
    claims={**claims,'snapshot_hash':canonical_sha256(ordinary['operation_snapshot'])}
    ordinary.update(snapshot_hash=claims['snapshot_hash'],token=signer.sign(claims))
    normalized,verified=verify(deferred,ordinary)
    assert normalized==ordinary and verified==claims
    changed=copy.deepcopy(artifact)
    for name in ('deferred_reflection','local_completion_id','local_completed_at','server_received_at'):
        del changed[name]
    with pytest.raises(OperationLeaseError):
        normalize_issue_artifact(changed)


@pytest.mark.parametrize('changed',['none','id','time','uncommitted','absent'])
def test_attach_requires_the_original_durable_completion_before_binding_the_set(deferred,tmp_path,changed):
    artifact,*_=deferred
    normalized,claims=verify(deferred,artifact)
    store=OperationLeaseStore(tmp_path/'lease.sqlite3')
    store.save_prefetched(artifact={**normalized,'claims':claims},binding=binding(),issue_idempotency_key='original-issue')
    with sqlite3.connect(store.path) as conn:
        conn.execute('CREATE TABLE package_command_outbox(set_id TEXT,draft_json TEXT,local_completion_committed INTEGER)')
        if changed!='absent':
            conn.execute('INSERT INTO package_command_outbox VALUES (?,?,?)',(
                artifact['local_completion_id'],json.dumps({'local_completed_at':
                    '2026-10-14T01:00:00Z' if changed=='time' else artifact['local_completed_at']}),
                0 if changed=='uncommitted' else 1))
    target='different-completion' if changed=='id' else artifact['local_completion_id']
    if changed=='none':
        result=store.attach_set(claims['lease_id'],target)
        assert result['set_id']==target
        repeated=store.attach_set(claims['lease_id'],target)
        assert repeated['set_id']==target and repeated['artifact_json']==result['artifact_json']
    else:
        before=store.get(lease_id=claims['lease_id'])
        with pytest.raises(OperationLeaseError) as denied:
            store.attach_set(claims['lease_id'],target)
        assert denied.value.code=='OPERATION_LEASE_BINDING_MISMATCH'
        assert store.get(lease_id=claims['lease_id'])==before

import copy
import secrets
from urllib.parse import parse_qs, urlsplit

import pytest

from package_command_draft import canonical_sha256
from package_logistics import PackageClientConfig, PackageLogisticsClient
from package_errors import PackageLogisticsError
from tests.test_package_logistics import _work_group_response, _work_group_draft, SCOPE


def recovered_source():
    response=_work_group_response(split=False)
    source=response['work_group_source']
    group=response['phs_work_group']
    original=source['package_bundle_id']
    for transfer in source['source_transfers']:
        transfer.update(origin_receipt_contract_version='PACKAGE_RECOVERY_TRANSFER_V1',origin_receipt_id='recovery-source-receipt')
    identity=dict(group_id=group['group_id'],label_id=group['label_id'],member_ids=source['member_ids'],
                  package_recovery_receipt_ids=['recovery-source-receipt'])
    fresh='PACKAGE-WORK-'+canonical_sha256(identity)[:24].upper()
    def replace(value):
        if isinstance(value,dict):
            return {replace(k):replace(v) for k,v in value.items()}
        if isinstance(value,list): return [replace(v) for v in value]
        if isinstance(value,str): return value.replace(original,fresh)
        return value
    response=replace(response)
    source=response['work_group_source']
    topology=canonical_sha256(dict(phs_work_group=response['phs_work_group'],source_transfers=source['source_transfers'],
        remainder_cover_groups=source['remainder_cover_groups'],source_iin=source['source_iin'],
        barcode_membership_hash=source['barcode_membership_hash'],package_bundle_id=fresh))
    source['topology_hash']=response['topology_hash']=topology
    return response,original


def client_for(response,calls):
    def transport(method,url,headers,body,timeout):
        calls.append((method,url,body))
        return dict(ok=True,data=copy.deepcopy(response))
    config=PackageClientConfig('https://unreachable.invalid',secrets.token_urlsafe(32),SCOPE,1,'host','device')
    return PackageLogisticsClient(config,transport=transport)


def test_recovery_receipt_binds_new_package_identity_and_preflight_is_read_again():
    response,original=recovered_source()
    calls=[]
    client=client_for(response,calls)
    draft=_work_group_draft(response)
    _,command=client.build_create_package_command(draft,idempotency_key='new-recovered-package')
    assert command['payload']['package_bundle_id']==draft.package_bundle_id!=original
    assert len(calls)==1 and calls[0][0]=='GET'
    assert parse_qs(urlsplit(calls[0][1]).query)['bundle_role']==['PACKAGE_SOURCE']
    response['work_group_source']['source_transfers'][0]['origin_receipt_id']='another-recovery'
    with pytest.raises(PackageLogisticsError):
        client.build_create_package_command(draft,idempotency_key='new-recovered-package')
    assert len(calls)==2 and all(method=='GET' for method,_,_ in calls)


def test_machine_recovery_reads_only_original_cancel_receipt_identity():
    calls=[]
    client=client_for({},calls)
    expected=dict(authority_scope_id=SCOPE,ledger_plane='TEST',plane_epoch=1,
                  package_bundle_id='original/package',cancel_receipt_id='original?cancel')
    client.get_package_recovery(expected)
    method,url,body=calls[0]
    assert method=='GET' and body is None
    assert urlsplit(url).path.endswith('/original%2Fpackage/recovery')
    assert parse_qs(urlsplit(url).query)=={'cancel_receipt_id':['original?cancel']}

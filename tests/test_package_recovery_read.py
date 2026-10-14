import copy
import hashlib
from pathlib import Path

import pytest

from package_errors import PackageLogisticsError
from package_recovery import (CONTRACT_SHA256, CONTRACT_VERSION, HELD_CODES,
                              membership_hash, recovery_expectation, validate_recovery_read)


def evidence():
    members = ['unit-a', 'unit-b']
    common = dict(authority_scope_id='scope', ledger_plane='TEST', plane_epoch=1,
                  package_bundle_id='package-a', item_id='item', inbound_iin='iin',
                  member_ids=members, member_count=2, membership_hash=membership_hash(members))
    cancel = dict(common, reopen=False)
    expected = recovery_expectation(common, dict(receipt_id='cancel-a', data=cancel))
    label = dict(group_id='group', label_id='label', scan_payload='physical-label',
                 group_entity_version=2, membership_version=1, label_version=1,
                 label_entity_version=1, group_member_count=2,
                 group_membership_hash=common['membership_hash'], member_ids=members,
                 membership_hash=common['membership_hash'])
    response = dict(expected, contract_version=CONTRACT_VERSION, reopen=False,
                    recovery_state='RECOVERED', recovery_receipt_id='recovery-a',
                    transfer_bundle_id='transfer-new', transfer_state='AVAILABLE',
                    transfer_entity_version=1, package_source_ready=True,
                    current_transfer_member_ids=members, current_transfer_projection_ids=members,
                    current_transfer_membership_hash=common['membership_hash'], current_labels=[label])
    return expected, response


def test_contract_bytes_are_pinned():
    assert hashlib.sha256((Path(__file__).parents[1]/'docs/spec/package-recovery.md').read_bytes()).hexdigest() == CONTRACT_SHA256


def test_exact_bound_current_recovery_is_accepted_without_mutating_evidence():
    expected, response = evidence()
    before = copy.deepcopy(response)
    assert validate_recovery_read(response, expected) == 'RECOVERED'
    assert response == before


@pytest.mark.parametrize('field,value', [
    ('contract_version','package-recovery-read-v2'), ('cancel_receipt_id','another'),
    ('authority_scope_id','other'), ('plane_epoch',True), ('package_bundle_id','other'),
    ('member_ids',['unit-a']), ('membership_hash','wrong'), ('item_id','other'),
    ('inbound_iin','other'), ('transfer_state','CONSUMED'), ('transfer_entity_version',True),
    ('package_source_ready',False), ('current_transfer_member_ids',['unit-a']),
    ('current_transfer_projection_ids',['unit-a']), ('current_transfer_membership_hash','wrong'),
    ('recovery_receipt_id',''), ('recovery_state','RESOLVED'), ('reopen',True),
])
def test_unknown_mismatched_or_consumed_evidence_cannot_release(field,value):
    expected,response=evidence()
    response[field]=value
    with pytest.raises(PackageLogisticsError):
        validate_recovery_read(response,expected)


@pytest.mark.parametrize('field', ['group_entity_version','membership_version','label_version','label_entity_version',
                                  'membership_hash','group_membership_hash','member_ids'])
def test_current_label_proof_is_checked(field):
    expected,response=evidence()
    response['current_labels'][0][field]=None
    with pytest.raises(PackageLogisticsError):
        validate_recovery_read(response,expected)


@pytest.mark.parametrize('reason', sorted(HELD_CODES)+['UNKNOWN',None,'SOURCE_TRANSFER_CHANGED'])
def test_only_explicit_v1_evidence_defect_is_unrecoverable(reason):
    expected,response=evidence()
    response.update(recovery_state='AWAITING_RECOVERY',recovery_receipt_id=None,
                    transfer_bundle_id=None,reopen_reason=reason)
    assert validate_recovery_read(response,expected) == ('HELD' if reason in HELD_CODES else 'AWAITING')

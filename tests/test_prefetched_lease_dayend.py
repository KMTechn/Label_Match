"""Expired saved leases remain available only for server-adjudicated completion."""

import base64
from copy import deepcopy
from datetime import timedelta
import json
from unittest.mock import Mock

import pytest

import Label_Match as label_module
import terminal_operation_lease as lease_module
from package_logistics import (
    PackageApiError, PackageLogisticsClient, PackageOutboxProcessor,
)
from tests.test_deferred_lease_clock_retry import clock_case, _first_scan, _queue_f3
from tests.test_package_logistics import _draft_for_set


def _prefetch(case):
    case.instant[0] = lease_module.parse_utc(
        case.claims['issued_at'], field='issued_at',
    ) + timedelta(seconds=1)
    physical = case.group['scan_payload']
    parts = case.app._acquire_operation_lease(
        physical, expected_snapshot=case.snapshot,
    )
    row = case.app.package_operation_lease_store.get(lease_id=case.claims['lease_id'])
    assert row['status'] == 'PREFETCHED'
    assert len(case.calls) == 1
    original_accept = case.app._accept_resolved_central_phs2_scan

    def accept(evidence, *args, **kwargs):
        result = original_accept(evidence, *args, **kwargs)
        case.app.current_set_info.update(evidence.state_fields())
        return result

    case.app._accept_resolved_central_phs2_scan = accept
    return parts, row


def _expire(case):
    case.instant[0] = lease_module.parse_utc(
        case.claims['expires_at'], field='expires_at',
    ) + timedelta(days=1)


@pytest.mark.parametrize('expired', [False, True])
def test_prefetched_scan_preserves_row_and_completion_reuses_original(clock_case, expired):
    case = clock_case
    _parts, before = _prefetch(case)
    if expired:
        _expire(case)
    resolver = Mock(return_value=case.response)
    case.app.package_logistics_client.resolve_package_source_evidence = resolver
    accepted_leases = []
    original_accept = case.app._accept_resolved_central_phs2_scan

    def accept(evidence, snapshot, sealed, operation_lease, **kwargs):
        accepted_leases.append(operation_lease)
        return original_accept(evidence, snapshot, sealed, operation_lease, **kwargs)

    case.app._accept_resolved_central_phs2_scan = accept
    _first_scan(case)
    assert resolver.call_count == int(expired)
    assert not accepted_leases[0] if expired else accepted_leases[0]['lease_id'] == before['lease_id']
    assert case.app.package_operation_lease_store.get(lease_id=before['lease_id']) == before
    attempt_before = case.app.package_operation_lease_store.get_issue_attempt(
        issue_idempotency_key=before['issue_idempotency_key'],
    )
    _queue_f3(case)
    queued = case.app.package_outbox.get_by_set_id(case.app.current_set_info['id'])
    draft = json.loads(queued['draft_json'])
    assert draft['operation_lease_id'] == before['lease_id']
    assert draft['operation_lease_token'] == case.artifact['token']
    assert len(case.calls) == 1
    attempt = case.app.package_operation_lease_store.get_issue_attempt(
        issue_idempotency_key=before['issue_idempotency_key'],
    )
    assert attempt['issue_idempotency_key'] == attempt_before['issue_idempotency_key']


def test_existing_prefetched_set_completes_after_overnight_expiry(clock_case):
    case = clock_case
    _parts, before = _prefetch(case)
    _first_scan(case)
    case.app.current_set_info['operation_lease_id'] = before['lease_id']
    case.app._acquire_operation_lease = Mock(
        side_effect=AssertionError('An existing set must reuse its stored lease'),
    )
    _expire(case)
    _queue_f3(case)
    queued = case.app.package_outbox.get_by_set_id(case.app.current_set_info['id'])
    assert json.loads(queued['draft_json'])['operation_lease_id'] == before['lease_id']
    assert len(case.calls) == 1


def test_expired_prefetched_completion_posts_old_lease_and_409_conflicts_only_its_set(clock_case):
    case = clock_case
    _parts, before = _prefetch(case)
    _expire(case)
    _first_scan(case)
    _queue_f3(case)
    outbox = case.app.package_outbox
    primary = outbox.get_by_set_id(case.app.current_set_info['id'])
    primary_draft = json.loads(primary['draft_json'])
    outbox.mark_local_completion_committed(
        primary['idempotency_key'], operation_lease_id=before['lease_id'],
        operation_completed_at=primary_draft['operation_lease_completed_at'],
    )
    control = outbox.enqueue(_draft_for_set('independent-prefetch-control'))
    outbox.mark_local_completion_committed(control['idempotency_key'])
    untouched = outbox.get_by_set_id('independent-prefetch-control')
    posted = []

    def transport(method, _url, _headers, body, _timeout):
        if method == 'GET':
            return {'ok': True, 'data': case.response}
        posted.append(json.loads(body))
        raise PackageApiError(409, 'OPERATION_LEASE_CONFLICT', 'fixture conflict', retryable=False)

    client = PackageLogisticsClient(case.app.package_logistics_client.config, transport=transport)
    result = PackageOutboxProcessor(outbox, client).drain(limit=1)
    assert result['conflict'] == 1
    assert len(posted) == 1
    wire_lease = posted[0]['payload']['operation_lease']
    assert wire_lease['lease_id'] == before['lease_id']
    assert wire_lease['token'] == case.artifact['token']
    queued = outbox.get_by_set_id(case.app.current_set_info['id'])
    assert queued['status'] == 'CONFLICT'
    assert case.app.package_operation_lease_store.get(lease_id=before['lease_id'])['status'] == 'OPERATOR_REVIEW'
    assert outbox.get_by_set_id('independent-prefetch-control') == untouched
    assert len(case.calls) == 1


def test_newly_issued_expired_lease_remains_rejected(clock_case):
    case = clock_case
    _expire(case)
    with pytest.raises(lease_module.OperationLeaseError) as blocked:
        case.app._acquire_operation_lease(
            case.group['scan_payload'],
            expected_snapshot=case.snapshot,
        )
    assert blocked.value.code == 'OPERATION_LEASE_EXPIRED'
    assert len(case.calls) == 1
    assert case.app.package_operation_lease_store.get(lease_id=case.claims['lease_id']) is None


@pytest.mark.parametrize('invalid', ['signature', 'binding', 'snapshot', 'future'])
def test_expiry_exception_preserves_signature_binding_snapshot_and_clock_checks(clock_case, invalid):
    case = clock_case
    _prefetch(case)
    _expire(case)
    physical = case.group['scan_payload']
    binding = label_module._label_match_operation_lease_binding(
        physical, case.snapshot, case.app.package_logistics_client.config,
    )
    token = case.artifact['token']
    snapshot = deepcopy(case.response)
    if invalid == 'signature':
        parts = token.split('.')
        signature = bytearray(base64.urlsafe_b64decode(parts[2] + '=='))
        signature[0] ^= 1
        parts[2] = base64.urlsafe_b64encode(signature).rstrip(b'=').decode('ascii')
        token = '.'.join(parts)
    elif invalid == 'binding':
        binding['device_id'] = 'different-fixture-device'
    elif invalid == 'snapshot':
        snapshot['unexpected_fixture_field'] = True
    else:
        case.instant[0] = lease_module.parse_utc(case.claims['issued_at'], field='issued_at') - timedelta(seconds=31)
    with pytest.raises(lease_module.OperationLeaseError) as blocked:
        case.app.package_operation_lease_keyring.verify(
            token, expected=binding, operation_snapshot=snapshot, allow_expired=True,
        )
    assert blocked.value.code != 'OPERATION_LEASE_EXPIRED'

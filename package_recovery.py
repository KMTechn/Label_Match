"""Pinned, read-only evidence for recovery of an immutable refused cancellation."""
from __future__ import annotations

import hashlib
import json
from typing import Mapping

from package_errors import PackageLogisticsError

CONTRACT_VERSION = 'package-recovery-read-v1'
CONTRACT_SHA256 = '995524d110c0ce30191a5c4506d82edf71ad78e65469fc9c41a66b9dfe99620d'
SERVER_COMMIT = '8c3f5208'
HELD_CODES = frozenset({
    'SOURCE_TRANSFER_MISSING', 'PACKAGE_CREATION_RECEIPT_INVALID',
    'PACKAGE_CREATION_RECEIPT_MISSING', 'PACKAGE_CREATION_EVIDENCE_CONFLICT',
    'PACKAGE_CREATION_MOVEMENT_MISSING', 'PACKAGE_CREATION_MOVEMENT_CONFLICT',
})
AWAITING_NOTICE = '아직 회수되지 않았습니다\n웹 사무실에서 회수한 뒤 다시 확인하세요'
RECOVERED_NOTICE = '회수 확인 완료\n이 현품표를 다시 스캔해 포장하세요'
HELD_NOTICE = '이 현품표는 회수할 수 없는 기록입니다\n제품은 따로 두고 현장 관리자에게 알리세요'


def _require(condition):
    if not condition:
        raise PackageLogisticsError('PACKAGE_RECOVERY_EVIDENCE_INVALID')


def _text(value):
    _require(isinstance(value, str) and bool(value) and value == value.strip())
    return value


def _positive(value):
    _require(type(value) is int and value > 0)
    return value


def _members(value):
    _require(isinstance(value, list) and bool(value))
    result = tuple(_text(member) for member in value)
    _require(len(result) == len(set(result)))
    return tuple(sorted(result))


def membership_hash(members):
    return hashlib.sha256(json.dumps(sorted(members), ensure_ascii=False,
                                    separators=(',', ':')).encode('utf-8')).hexdigest()


def recovery_expectation(creation, cancellation):
    """Freeze identity from the ACKed original package and cancel, never a review ID."""
    _require(isinstance(creation, Mapping) and isinstance(cancellation, Mapping))
    package = creation.get('data', creation)
    cancel = cancellation.get('data', cancellation)
    _require(isinstance(package, Mapping) and isinstance(cancel, Mapping))
    _require(cancel.get('reopen') is False)
    result = {name: _text(package.get(name)) for name in
              ('package_bundle_id', 'item_id', 'inbound_iin')}
    envelope_names = dict(ledger_plane='resolved_ledger_plane',plane_epoch='resolved_plane_epoch')
    for name in ('authority_scope_id', 'ledger_plane', 'plane_epoch'):
        value = creation.get(envelope_names.get(name,name),creation.get(name,package.get(name)))
        result[name] = _positive(value) if name == 'plane_epoch' else _text(value)
        for source in (creation,package):
            if name in source:
                _require(type(source[name]) is type(value) and source[name] == value)
    result['cancel_receipt_id'] = _text(cancellation.get('receipt_id') or cancel.get('receipt_id'))
    for name in ('authority_scope_id', 'ledger_plane', 'plane_epoch', 'package_bundle_id'):
        value = cancellation.get(envelope_names.get(name,name),cancellation.get(name,cancel.get(name)))
        _require(type(value) is type(result[name]) and value == result[name])
        if name in cancel:
            _require(cancel[name] == result[name])
    members = _members(package.get('member_ids'))
    _require(_members(cancel.get('member_ids')) == members)
    digest = membership_hash(members)
    _require(package.get('membership_hash') == digest and cancel.get('membership_hash') == digest)
    _require(package.get('member_count') == len(members) and cancel.get('member_count') == len(members))
    result.update(member_ids=list(members), member_count=len(members), membership_hash=digest)
    return result


def validate_recovery_read(response, expected):
    """Return AWAITING/HELD/RECOVERED; malformed or stale evidence raises closed."""
    _require(isinstance(response, Mapping) and response.get('contract_version') == CONTRACT_VERSION)
    for name in ('authority_scope_id', 'ledger_plane', 'plane_epoch', 'package_bundle_id', 'cancel_receipt_id'):
        _require(type(response.get(name)) is type(expected[name]) and response.get(name) == expected[name])
    _require(response.get('reopen') is False)
    state = response.get('recovery_state')
    if state == 'AWAITING_RECOVERY':
        _require(response.get('recovery_receipt_id') is None and response.get('transfer_bundle_id') is None)
        return 'HELD' if response.get('reopen_reason') in HELD_CODES else 'AWAITING'
    _require(state == 'RECOVERED')
    for name in ('member_count', 'membership_hash', 'item_id', 'inbound_iin'):
        _require(type(response.get(name)) is type(expected[name]) and response.get(name) == expected[name])
    members = _members(response.get('member_ids'))
    _require(members == tuple(expected['member_ids']))
    _text(response.get('recovery_receipt_id'))
    transfer = _text(response.get('transfer_bundle_id'))
    _require(transfer != expected['package_bundle_id'])
    _require(response.get('transfer_state') == 'AVAILABLE' and response.get('package_source_ready') is True)
    _positive(response.get('transfer_entity_version'))
    _require(_members(response.get('current_transfer_member_ids')) == members)
    _require(_members(response.get('current_transfer_projection_ids')) == members)
    _require(response.get('current_transfer_membership_hash') == expected['membership_hash'])
    labels = response.get('current_labels')
    _require(isinstance(labels, list))
    covered, groups, scans = [], set(), set()
    for label in labels:
        _require(isinstance(label, Mapping))
        group, scan = _text(label.get('group_id')), _text(label.get('scan_payload'))
        _require(group not in groups and scan not in scans)
        groups.add(group)
        scans.add(scan)
        _text(label.get('label_id'))
        for name in ('group_entity_version', 'membership_version', 'label_version', 'label_entity_version', 'group_member_count'):
            _positive(label.get(name))
        selected = _members(label.get('member_ids'))
        _require(label.get('group_member_count') >= len(selected))
        _require(label.get('membership_hash') == membership_hash(selected))
        digest = label.get('group_membership_hash')
        _require(isinstance(digest, str) and len(digest) == 64 and all(c in '0123456789abcdef' for c in digest))
        if label['group_member_count'] == len(selected):
            _require(digest == membership_hash(selected))
        covered.extend(selected)
    if labels:
        _require(tuple(sorted(covered)) == members)
    return 'RECOVERED'

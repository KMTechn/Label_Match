"""A reopened tray must wait for its cancellation, without holding other trays."""
from dataclasses import replace

import pytest

from package_logistics import PackageCancellationOutbox, PackageLogisticsError, PackageOutbox
from tests.test_package_logistics import (
    _ack_package_creation, _cancellation_intent, _cancellation_receipt, _draft_for_set,
)


def queued(outbox, draft):
    row = outbox.enqueue(draft)
    outbox.mark_local_completion_committed(row['idempotency_key'])
    return row


@pytest.mark.parametrize('status', ['PENDING', 'SENDING', 'CONFLICT'])
@pytest.mark.parametrize('identity', ['transfer', 'input_tag'])
def test_repack_waits_for_same_source_cancel_ack_and_other_tray_can_send(tmp_path, status, identity):
    outbox = PackageOutbox(tmp_path / 'outbox.sqlite3')
    original = _draft_for_set('ORIGINAL')
    if identity == 'input_tag':
        original = replace(original, source_input_tag_id='ITG-REPACK')
    _ack_package_creation(outbox, original)
    cancels = PackageCancellationOutbox(outbox.db_path)
    intent = _cancellation_intent(original)
    cancels.enqueue(intent)
    cancels.mark_local_event_committed(intent.cancellation_event_id)
    if status != 'PENDING':
        claim = cancels.claim_next()
        if status == 'CONFLICT':
            cancels.mark_conflict(claim['idempotency_key'], PackageLogisticsError('synthetic cancel conflict'))
    repack = replace(_draft_for_set('REPACK'), source_input_tag_id=original.source_input_tag_id)
    if identity == 'input_tag':
        repack = replace(repack, source_bundle_id='')
    repack_row = queued(outbox, repack)
    other = replace(_draft_for_set('OTHER'), source_bundle_id='TRANSFER-OTHER', source_input_tag_id='ITG-OTHER')
    other_row = queued(outbox, other)
    assert outbox.claim_next()['idempotency_key'] == other_row['idempotency_key']
    assert outbox.claim_next() is None
    assert outbox.get_by_set_id(repack.set_id)['attempt_count'] == 0
    assert outbox.get_by_set_id(repack.set_id)['status'] == 'PENDING'
    if status == 'CONFLICT':
        assert cancels.get_by_event_id(intent.cancellation_event_id)['status'] == 'CONFLICT'
        return
    if status == 'PENDING':
        cancels.claim_next()
    assert cancels.mark_acked(cancels.get_by_event_id(intent.cancellation_event_id)['idempotency_key'], _cancellation_receipt(original, intent=intent))
    assert outbox.claim_next()['idempotency_key'] == repack_row['idempotency_key']


def test_offline_original_create_can_send_before_its_deferred_cancel_but_repack_cannot(tmp_path):
    outbox = PackageOutbox(tmp_path / 'outbox.sqlite3')
    original = _draft_for_set('ORIGINAL')
    original_row = queued(outbox, original)
    cancels = PackageCancellationOutbox(outbox.db_path)
    intent = _cancellation_intent(original)
    assert cancels.enqueue(intent)['status'] == 'DEFERRED'
    cancels.mark_local_event_committed(intent.cancellation_event_id)
    queued(outbox, _draft_for_set('REPACK'))
    assert outbox.claim_next()['idempotency_key'] == original_row['idempotency_key']
    assert outbox.claim_next() is None
    assert outbox.get_by_set_id('REPACK')['attempt_count'] == 0

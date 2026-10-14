"""Keep local completion and distinguish a server that cannot reopen a tray."""
import pytest

import Label_Match as lm
from package_logistics import PackageApiError
from tests.test_lm_scan_cancel import app_for_delete
from tests.test_package_logistics import _cancellation_intent, _cancellation_receipt, _draft, _draft_for_set
from tests.test_u4_cancellation_order import queued


@pytest.mark.parametrize('mode,expected', [('old', 'OLD_SERVER'), ('refused', 'REOPEN_REFUSED'), ('new', None)])
def test_repack_review_notice_reads_ack_without_changing_receipt_or_local_completion(tmp_path, mode, expected):
    app = app_for_delete(tmp_path)
    outbox = app.package_outbox
    original = _draft()
    cancels = app.package_cancellation_outbox
    intent = _cancellation_intent(original)
    cancels.enqueue(intent)
    cancels.mark_local_event_committed(intent.cancellation_event_id)
    cancel = cancels.claim_next()
    receipt = _cancellation_receipt(original, intent=intent)
    if mode == 'new':
        receipt['data'].update(reopened_transfer_bundle_id=original.source_bundle_id, transfer_entity_version=9, reversal_movement_id='REVERSAL-U4')
    elif mode == 'refused':
        receipt['data'].update(reopen=False, reopen_reason='REOPEN_LINEAGE_AMBIGUOUS')
    cancels.mark_acked(cancel['idempotency_key'], receipt)
    frozen = cancels.get_by_event_id(intent.cancellation_event_id)['receipt_json']
    repack = _draft_for_set('REPACK')
    queued(outbox, repack)
    row = outbox.claim_next()
    outbox.mark_conflict(row['idempotency_key'], PackageApiError(409, 'COMPLETION_REVIEW_REQUIRED', 'synthetic repack review'))
    snapshot = app._read_package_review_snapshot()
    app._refresh_package_cancellation_review_notice(snapshot)
    assert app._package_create_review_rows[0].get('repack_server_notice') == expected
    notice = app._package_create_review_notice.message
    if mode == 'old':
        assert '다시 포장을 지원하지 않습니다' in notice
    elif mode == 'refused':
        assert '되돌리지 못했습니다' in notice
    else:
        assert '다시 포장을 지원하지 않습니다' not in notice
        assert '되돌리지 못했습니다' not in notice
    assert outbox.get_by_set_id(repack.set_id)['local_completion_committed'] == 1
    assert outbox.get_by_set_id(repack.set_id)['status'] == 'CONFLICT'
    assert cancels.get_by_event_id(intent.cancellation_event_id)['receipt_json'] == frozen
    assert outbox.get_repack_cancellation_receipt(repack.set_id) == receipt


@pytest.mark.parametrize('cancellation', [None, {'status': 'ACKED'}, {'status': 'PENDING'}])
def test_f2_result_allows_same_tag_repack_and_keeps_pending_shipping_warning(cancellation):
    message = lm.Label_Match._package_cancellation_completion_message(cancellation)
    assert '같은 현품표로 다시 포장할 수 있습니다' in message
    if cancellation and cancellation['status'] == 'PENDING':
        assert '중앙 취소 전송·확인 중' in message
        assert '반출하지 마세요' in message

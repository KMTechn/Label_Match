"""M102 result and cancellation copy contains facts in field language only."""
import re
from types import SimpleNamespace

import pytest

import Label_Match as lm
from label_screen_bridge import LabelMatchScreenProcessor
from tests.test_lm_scan_cancel import app_for_delete
from tests.test_lm_scan_cancel_contract import cancel_screen, assert_state
from tests.test_label_screen_bridge import call


def assert_field_copy(text):
    assert text
    assert not any(word in text for word in ['전송', '중앙', '충돌', '관리자', '하세요', '마세요', '알리세요',
                                            '요청하세요', 'outbox', 'ACK', 'CONFLICT', 'DEFERRED', 'SENDING'])
    assert re.search(r'\b[A-Z][A-Z0-9]*_[A-Z0-9_]+\b', text) is None


@pytest.mark.parametrize('code', ['BAD_INPUT', 'TARGET_CHANGED', 'BUSY', 'READ_ONLY', 'CONFIRMATION_PENDING',
                                 'IN_PROGRESS', 'NOT_APPLIED', 'FAILED', 'APPLIED'])
def test_cancel_reply_copy_has_no_codes_technical_words_or_tasks(tmp_path, code):
    app = app_for_delete(tmp_path, central=False)
    processor = LabelMatchScreenProcessor(app, module=lm)
    try:
        result = processor._scan_cancel_reply(code == 'APPLIED', 'SCAN_CANCEL_' + code, int(code == 'APPLIED'))
        assert_field_copy(result['result']['text'])
        if code == 'APPLIED':
            assert result['result'] == {'kind': 'saved', 'text': '기록을 취소했습니다'}
        else:
            assert result['result']['kind'] == 'warn'
    finally:
        processor.uninstall()


@pytest.mark.parametrize('status', ['DEFERRED', 'PENDING', 'SENDING', 'ACKED', 'CONFLICT'])
def test_cancel_queue_copy_reports_actual_wait_sending_or_confirmation(cancel_screen, status):
    app = cancel_screen.app
    app.package_cancellation_outbox = SimpleNamespace(get_by_set_id=lambda _sid:
        [{'status': status, 'local_event_committed': True}] if _sid == 'SET-1' else [])
    state = call(cancel_screen, cancel_screen.bridge.get_state)
    assert_state(state)
    row = next(row for row in state['today']['rows'] if row['row'] == 'SET-1')
    assert row['status'] == 'cancelled'
    actual = row['cancellation']
    assert_field_copy(actual['text'])
    if status in {'DEFERRED', 'PENDING', 'SENDING'}:
        assert actual['state'] == 'pending' and '보내는' in actual['text']
        if status == 'SENDING':
            assert actual['text'] == '취소 요청을 회사 서버에 보내는 중입니다'
        else:
            assert actual['text'] == '취소 요청을 회사 서버에 보내는 순서를 기다리고 있습니다'
    elif status == 'ACKED':
        assert actual == {'state': 'acknowledged', 'text': '회사 서버에서 기록 취소를 확인했습니다'}
    else:
        assert actual == {'state': 'review', 'text': '회사 서버에서 기록 취소를 확인하지 못했습니다'}

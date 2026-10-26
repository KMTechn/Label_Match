"""M99/M100: exact UI schema, actual shell state, and legacy cancellation semantics."""
import copy
import csv
import hashlib
import io
import json
from pathlib import Path
import types
from queue import Queue
from datetime import datetime
from unittest import mock

import pytest

import Label_Match as lm
import label_history
from label_screen_bridge import LabelMatchScreenProcessor, HISTORY_LIMIT, _approval_id, _pointer_pending
from tests.test_label_screen_bridge import Bridge, call, schema_errors
from tests.test_lm_scan_cancel import app_for_delete


CONTRACT_BYTES = (Path(__file__).parent / 'fixtures/lm_cancel_contract_e89e2066.json').read_bytes()
CONTRACT = json.loads(CONTRACT_BYTES)
PREVIOUS_CONTRACT_BYTES = (Path(__file__).parent / 'fixtures/lm_cancel_contract_c6d5f907.json').read_bytes()
CURRENT_CONTRACT_BYTES = (
    Path(__file__).resolve().parents[1] / "ui/parts/app/lm/contract.json"
).read_bytes()
CURRENT_CONTRACT = json.loads(CURRENT_CONTRACT_BYTES)


def assert_state(state):
    assert schema_errors(CURRENT_CONTRACT['state'], state) == []
    assert schema_errors(CURRENT_CONTRACT['state'], {**state, 'unlisted_cancel_field': None})


@pytest.fixture
def cancel_screen(tmp_path):
    app = app_for_delete(tmp_path / 'data')
    bridge = Bridge(tmp_path / 'shell', 'lm', sound=mock.Mock(), owner='external')
    processor = LabelMatchScreenProcessor(app, bridge=bridge, module=lm)
    app.after_idle = lambda callback: callback()
    app.flush_idle = lambda: None
    bridge.attach_owner(processor)
    screen = types.SimpleNamespace(app=app, bridge=bridge, processor=processor)
    yield screen
    processor.uninstall()
    bridge.close()


def command(screen, name, args):
    reply = call(screen, screen.bridge.command, name, args)
    assert reply['ok'] is True
    assert_state(reply['state'])
    result = reply['result']
    assert_state(result['state'])
    return result


def test_exact_e89e2066_schema_preserves_c6d5f907_and_shell_completed_rows(cancel_screen):
    assert hashlib.sha256(CURRENT_CONTRACT_BYTES).hexdigest() == 'f6a25fc95b5bc63b3fe901615c746cc7a0ed0376c7d9f318b8418fbfe95ea7e1'
    assert json.loads(CURRENT_CONTRACT_BYTES)["state"]["properties"]["alerts"]["maxItems"] == 5
    assert hashlib.sha256(CONTRACT_BYTES).hexdigest() == '86aa690d83ecd8a67f37e43780d00f02e657fb34450efd7f7a071887cdbc200a'
    assert hashlib.sha256(PREVIOUS_CONTRACT_BYTES).hexdigest() == 'fa255a368d378b43fe13fd670cd88eedb7a4fb9e0752c74093aab683db3cd862'
    assert CONTRACT['state'] == json.loads(PREVIOUS_CONTRACT_BYTES)['state']
    state = call(cancel_screen, cancel_screen.bridge.get_state)
    assert_state(state)
    assert type(state['events_dropped']) is int
    assert len(state['today']['rows']) == 2
    assert all(row['status'] == 'completed' and row['products'] == 1 for row in state['today']['rows'])
    result = command(cancel_screen, 'scan_cancel', {'rows': [state['today']['rows'][0]['row']]})
    assert result['code'] == 'SCAN_CANCEL_CONFIRMATION_REQUIRED' and result['deleted'] == 0
    assert 'result' not in result
    approved = command(cancel_screen, 'approve', {'id': result['state']['approval']['id']})
    assert approved['deleted'] == 1 and approved['result'] == {'kind': 'saved', 'text': '기록을 취소했습니다'}
    assert len(approved['state']['today']['rows']) == 1


@pytest.mark.parametrize('action_text', ['삭제', '스캔 취소', '확인'])
def test_m107_approval_is_bound_to_row_independently_of_action_text(cancel_screen, monkeypatch, action_text):
    app = cancel_screen.app
    original = app._request_pointer_confirmation
    def request(title, message, _action_text, action):
        return original(title, message, action_text, action)
    monkeypatch.setattr(app, '_request_pointer_confirmation', request)
    before = call(cancel_screen, cancel_screen.bridge.get_state)
    assert before['approval'] is None
    requested = command(cancel_screen, 'scan_cancel', {'rows': ['SET-1']})
    approval = requested['state']['approval']
    assert approval['id'] and approval['action'] == action_text
    assert requested['code'] == 'SCAN_CANCEL_CONFIRMATION_REQUIRED'
    app._workflow_notice_action_text = '다른 확인 글'
    approved = command(cancel_screen, 'approve', {'id': approval['id']})
    assert approved['ok'] is True and approved['code'] == 'SCAN_CANCEL_APPLIED'
    assert approved['deleted'] == 1 and approved['result'] == {'kind': 'saved', 'text': '기록을 취소했습니다'}
    assert not app.history_tree.exists('SET-1') and app.history_tree.exists('SET-2')
    assert len(app.data_manager.events) == 1 and app.data_manager.flushed


def test_in_progress_rejection_has_no_ledger_or_outbox_effect(cancel_screen):
    app = cancel_screen.app
    app.set_details_map['SET-1']['final_result'] = lm.Label_Match.Results.IN_PROGRESS
    state = call(cancel_screen, cancel_screen.bridge.get_state)
    assert_state(state)
    row = next(row for row in state['today']['rows'] if row['row'] == 'SET-1')
    assert row['status'] == 'in_progress'
    result = command(cancel_screen, 'scan_cancel', {'rows': ['SET-1']})
    assert result['ok'] is False and result['code'] == 'SCAN_CANCEL_IN_PROGRESS'
    assert result['deleted'] == 0 and result['result']['kind'] == 'warn'
    assert app.data_manager.events == [] and app.package_cancellation_outbox.get_by_set_id('SET-1') == []
    assert app.history_tree.exists('SET-1') and _pointer_pending(app) is None


def test_rows_above_history_limit_are_rejected_without_writes(cancel_screen):
    assert HISTORY_LIMIT == 100
    result = command(cancel_screen, 'scan_cancel', {'rows': [str(i) for i in range(HISTORY_LIMIT + 1)]})
    assert result['code'] == 'SCAN_CANCEL_BAD_INPUT' and result['ok'] is False
    assert result['deleted'] == 0 and result['result']['kind'] == 'warn'
    assert cancel_screen.app.data_manager.events == []


def test_exact_history_limit_accepts_one_hundred_visible_rows(tmp_path):
    app = app_for_delete(tmp_path, central=False)
    for i in range(3, HISTORY_LIMIT + 1):
        row = f'SET-{i}'
        details = copy.deepcopy(app.set_details_map['SET-1'])
        details['set_id'] = row
        app.set_details_map[row] = app.history_row_details_map[row] = details
        app.history_tree.insert('', 'end', iid=row, values=(i, f'MASTER-{i}', f'PRODUCT-{i}', '', '', '', app.Results.PASS, '12:00'))
    processor = LabelMatchScreenProcessor(app, module=lm)
    try:
        rows = [row['row'] for row in processor.state()['today']['rows']]
        assert len(rows) == HISTORY_LIMIT
        result = processor.command('scan_cancel', {'rows': rows})
        assert result['ok'] is True and result['code'] == 'SCAN_CANCEL_CONFIRMATION_REQUIRED'
        assert result['deleted'] == 0 and 'result' not in result
        assert len(result['state']['today']['rows']) == HISTORY_LIMIT
    finally:
        processor.uninstall()


def test_approval_rejects_a_row_that_became_in_progress(cancel_screen):
    first = command(cancel_screen, 'scan_cancel', {'rows': ['SET-1']})
    app = cancel_screen.app
    app.set_details_map['SET-1']['final_result'] = app.Results.IN_PROGRESS
    result = command(cancel_screen, 'approve', {'id': first['state']['approval']['id']})
    assert result['code'] == 'SCAN_CANCEL_IN_PROGRESS' and result['ok'] is False
    assert result['deleted'] == 0 and result['result']['kind'] == 'warn'
    assert app.data_manager.events == [] and app.package_cancellation_outbox.get_by_set_id('SET-1') == []


def test_cancelled_row_is_also_absent_after_legacy_history_replay(cancel_screen):
    app = cancel_screen.app
    completed = copy.deepcopy(app.set_details_map)
    first = command(cancel_screen, 'scan_cancel', {'rows': ['SET-1']})
    assert command(cancel_screen, 'approve', {'id': first['state']['approval']['id']})['deleted'] == 1
    source = io.StringIO()
    writer = csv.DictWriter(source, fieldnames=['timestamp', 'event', 'details'])
    writer.writeheader()
    for details in completed.values():
        writer.writerow({'timestamp': '2026-10-14T12:00:00', 'event': app.Events.TRAY_COMPLETE, 'details': json.dumps(details)})
    for event, details in app.data_manager.events:
        writer.writerow({'timestamp': '2026-10-14T12:01:00', 'event': event, 'details': json.dumps(details)})
    app.data_manager._get_log_filepath = lambda _date: 'isolated-history.csv'
    queue = Queue()
    label_history._async_load_history_task(app, queue, updates_active_state=True,
        open_file=lambda *_a, **_kw: io.StringIO(source.getvalue()), clock=lambda: datetime,
        tray_complete_result=lm._label_match_tray_complete_result,
        tray_complete_passed=lm._label_match_tray_complete_passed,
        summary_date=lm._label_match_summary_date, duplicate_index_barcodes=lm._label_match_duplicate_index_barcodes)
    rebuilt = queue.get_nowait()
    assert 'error' not in rebuilt
    assert set(rebuilt['set_details_map']) == {'SET-2'}
    assert [row[0] for row in rebuilt['sorted_sets']] == ['SET-2']
    assert rebuilt['global_scanned_set'] == app.global_scanned_set
    assert dict(rebuilt['scan_count']) == dict(app.scan_count)


@pytest.mark.parametrize('changed', ['selection', 'values', 'pending_callback'])
def test_approval_remains_bound_to_the_original_row_and_snapshot(cancel_screen, changed):
    screen, app = cancel_screen, cancel_screen.app
    first = command(screen, 'scan_cancel', {'rows': ['SET-1']})
    approval = first['state']['approval']['id']
    if changed == 'selection':
        app.history_tree.selection_set(['SET-2'])
    elif changed == 'values':
        app.history_tree.rows['SET-1']['values'] = (1, 'DIFFERENT', 'PRODUCT-1', '', '', '', app.Results.PASS, '12:00')
    else:
        pending = _pointer_pending(app)
        replacement = (pending[0], pending[1], mock.Mock(side_effect=AssertionError('replacement callback ran')))
        app._pointer_confirmation = replacement
    result = command(screen, 'approve', {'id': approval})
    assert result['ok'] is False and result['code'] == 'SCAN_CANCEL_TARGET_CHANGED'
    assert result['deleted'] == 0 and result['result']['kind'] == 'warn'
    assert app.data_manager.events == [] and len(app.history_tree.rows) == 2


def test_partial_failure_counts_only_rows_actually_removed(cancel_screen, monkeypatch):
    app = cancel_screen.app
    original = app._queue_authoritative_package_cancellation
    def queue(**kwargs):
        if kwargs['set_id'] == 'SET-2':
            raise OSError('isolated second row failure')
        return original(**kwargs)
    monkeypatch.setattr(app, '_queue_authoritative_package_cancellation', queue)
    first = command(cancel_screen, 'scan_cancel', {'rows': ['SET-1', 'SET-2']})
    result = command(cancel_screen, 'approve', {'id': first['state']['approval']['id']})
    assert result['ok'] is False and result['code'] == 'SCAN_CANCEL_FAILED'
    assert result['deleted'] == 1 and result['result']['kind'] == 'warn'
    assert not app.history_tree.exists('SET-1') and app.history_tree.exists('SET-2')
    assert len(app.data_manager.events) == 1 and len(app.package_cancellation_outbox.get_by_set_id('SET-1')) == 1


@pytest.mark.parametrize('status, expected', [('PENDING', 'pending'), ('ACKED', 'acknowledged'), ('CONFLICT', 'review')])
def test_remaining_cancelled_row_uses_actual_outbox_state_and_fact_only_text(cancel_screen, status, expected):
    app = cancel_screen.app
    cancellation = app._queue_authoritative_package_cancellation(set_id='SET-1', event_type=app.Events.SET_DELETED,
        reason='LOCAL_COMPLETED_SET_DELETED', local_event_details={'original_details': app.set_details_map['SET-1']})
    outbox = app.package_cancellation_outbox
    outbox.mark_local_event_committed(cancellation['cancellation_event_id'])
    row = outbox.claim_next()
    assert row['set_id'] == 'SET-1'
    if status == 'ACKED':
        assert outbox.mark_acked(row['idempotency_key'], {'isolated_receipt': True}, expected_claim=row)
    elif status == 'CONFLICT':
        assert outbox.mark_conflict(row['idempotency_key'], ValueError('isolated conflict'), expected_claim=row)
    else:
        assert outbox.mark_retry(row['idempotency_key'], OSError('isolated retry'), expected_claim=row)
    assert outbox.get_by_set_id('SET-1')[0]['status'] == status
    state = call(cancel_screen, cancel_screen.bridge.get_state)
    assert_state(state)
    actual = next(row for row in state['today']['rows'] if row['row'] == 'SET-1')
    assert actual['status'] == 'cancelled' and actual['products'] == 1
    assert actual['cancellation']['state'] == expected
    assert actual['cancellation']['text']
    assert not any(word in actual['cancellation']['text'] for word in ['관리자', '하세요', '마세요', '요청하세요', '알리세요'])

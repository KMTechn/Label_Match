"""The web scan-cancel entry uses the existing Tk deletion transaction."""
from collections import defaultdict
import json
import types
from unittest import mock

import pytest

import Label_Match as lm
from label_screen_bridge import LabelMatchScreenProcessor, _approval_id, _pointer_pending
from package_logistics import PackageOutbox, PackageCancellationOutbox
from tests.test_label_match_core import _RecordingTree, _completed_details, _FakeLoggingDataManager
from tests.test_package_logistics import _draft, _ack_package_creation


class SelectableTree(_RecordingTree):
    def __init__(self):
        super().__init__()
        self.selected = ()

    def selection(self):
        return self.selected

    def selection_set(self, items):
        self.selected = tuple(items)

    def item(self, iid, option=None):
        return self.rows[iid]['values'] if option == 'values' else self.rows[iid]


def app_for_delete(root, *, central=True):
    app = object.__new__(lm.Label_Match)
    app.run_tests = False
    app.initialized_successfully = True
    app.history_view_updates_active_state = True
    app.history_load_pending = app.history_active_load_pending = False
    app.current_set_info = {'raw': [], 'parsed': []}
    app.history_tree = SelectableTree()
    app.scan_count = defaultdict(lambda: defaultdict(int))
    app.set_details_map = {}
    for index, set_id in enumerate(('SET-1', 'SET-2'), 1):
        details = _completed_details(set_id, 'ITEM', '2026-01-01T10:00:00', [f'MASTER-{index}', f'PRODUCT-{index}'])
        app.set_details_map[set_id] = details
        app.scan_count[details['production_date']][('ITEM', details['phase'])] += 1
        app.history_tree.insert('', 'end', iid=set_id, values=(index, f'MASTER-{index}', f'PRODUCT-{index}', '', '', '', lm.Label_Match.Results.PASS, '2026-01-01T10:00:00'))
    app.history_row_details_map = dict(app.set_details_map)
    app._rebuild_global_scanned_set_from_details()
    app.data_manager = _FakeLoggingDataManager()
    app._update_summary_tree = lambda: None
    app._render_history_detail = lambda *_a, **_kw: None
    app._render_operator_workbench = lambda: None
    app._render_logistics_connection_footer = lambda: None
    app._focus_scan_entry_if_available = lambda: None
    app._play_sound = lambda *_a, **_kw: None
    app.update_big_display = lambda *_a, **_kw: None
    app.open_settings_window = lambda: None
    app._start_package_outbox_drain = lambda: None
    app._refresh_package_cancellation_status = lambda: None
    app._workflow_view_source = lambda: dict(app.current_set_info)
    app._worker_roster_choices = lambda: []
    app._roster_worker_allowed = lambda: False
    if central:
        app.package_outbox = PackageOutbox(root/'outbox.sqlite3')
        _ack_package_creation(app.package_outbox, _draft())
        app.package_cancellation_outbox = PackageCancellationOutbox(root/'outbox.sqlite3')
    return app


def snapshot(app):
    outbox = app.__dict__.get('package_cancellation_outbox')
    rows = outbox.get_by_set_id('SET-1') if outbox else []
    return {'labels': sorted(app.global_scanned_set), 'sets': sorted(app.set_details_map),
            'counts': {day: dict(values) for day, values in app.scan_count.items()},
            'events': app.data_manager.events, 'flushed': app.data_manager.flushed,
            'outbox': [(row['status'], row['local_event_committed'], json.loads(row['intent_json'])) for row in rows]}


@pytest.mark.parametrize('central', [False, True])
def test_web_and_tk_delete_have_identical_counts_audit_and_outbox(tmp_path, monkeypatch, central):
    from tests.test_lm_scan_cancel_contract import assert_state
    native = mock.Mock(side_effect=AssertionError('native dialog created'))
    monkeypatch.setattr(lm.tk, 'Toplevel', native)
    monkeypatch.setattr(lm.messagebox, 'showwarning', lambda *_a, **_kw: None)
    old = app_for_delete(tmp_path/'tk', central=central)
    web = app_for_delete(tmp_path/'web', central=central)
    old.history_tree.selection_set(['SET-1'])
    # Tk uses its real pointer callback; only its display boundary is headless.
    lm.Label_Match._delete_selected_row(old)
    assert _pointer_pending(old) is not None
    _pointer_pending(old)[2]()
    processor = LabelMatchScreenProcessor(web, module=lm)
    try:
        result = processor.command('scan_cancel', {'rows': ['SET-1']})
        assert_state(result['state'])
        assert result['ok'] is True and result['code'] == 'SCAN_CANCEL_CONFIRMATION_REQUIRED'
        assert result['deleted'] == 0 and 'SET-1' in web.set_details_map
        approved = processor.command('approve', {'id': _approval_id(_pointer_pending(web))})
        assert_state(approved['state'])
        assert approved['ok'] is True
        assert approved['deleted'] == 1
        assert approved['result'] == {'kind': 'saved', 'text': '기록을 취소했습니다'}
        assert 'SET-1' not in {row['row'] for row in approved['state']['today']['rows']}
        expected, actual = snapshot(old), snapshot(web)
        # IDs/timestamps of independently queued receipts are evidence, not quantities.
        for value in (expected, actual):
            for _kind, details in value['events']:
                details.pop('cancellation_event_id', None)
                details.get('package_cancellation', {}).pop('idempotency_key', None)
            for _status, _committed, intent in value['outbox']:
                intent.pop('cancellation_event_id', None)
                intent.pop('created_at', None)
        assert actual == expected
        assert len(actual['labels']) == 1 and len(actual['sets']) == 1
        assert sum(sum(v.values()) for v in actual['counts'].values()) == 1
        assert len(actual['events']) == 1 and actual['flushed'] is True
        assert len(actual['outbox']) == int(central)
        native.assert_not_called()
    finally:
        processor.uninstall()


def test_changed_selection_after_web_confirmation_preserves_rows(tmp_path):
    app = app_for_delete(tmp_path)
    processor = LabelMatchScreenProcessor(app, module=lm)
    try:
        assert processor.command('scan_cancel', {'rows': ['SET-1']})['ok'] is True
        pending = _pointer_pending(app)
        app.set_details_map['SET-1']['item_code'] = 'CHANGED'
        app._show_scanner_notice = mock.Mock()
        result = processor.command('approve', {'id': _approval_id(pending)})
        assert result['ok'] is False and result['code'] == 'SCAN_CANCEL_TARGET_CHANGED'
        assert result['deleted'] == 0 and result['result']['kind'] == 'warn'
        assert result['state']['approval'] is None
        assert len(app.history_tree.rows) == 2 and len(app.set_details_map) == 2
        assert app.data_manager.events == []
        assert app.package_cancellation_outbox.get_by_set_id('SET-1') == []
        app._show_scanner_notice.assert_called_once()
    finally:
        processor.uninstall()


@pytest.mark.parametrize('args', [{}, {'rows': []}, {'rows': ['missing']}, {'rows': ['SET-1', 'SET-1']}, {'rows': ['SET-1'], 'date': '2025-01-01'}])
def test_scan_cancel_rejects_bad_or_stale_targets_without_writes(tmp_path, args):
    app = app_for_delete(tmp_path)
    processor = LabelMatchScreenProcessor(app, module=lm)
    try:
        assert processor.command('scan_cancel', args)['ok'] is False
        assert len(app.history_tree.rows) == 2
        assert app.data_manager.events == []
        assert app.package_cancellation_outbox.get_by_set_id('SET-1') == []
    finally:
        processor.uninstall()


def test_scan_cancel_durable_outbox_failure_preserves_row_and_reports_failure(tmp_path, monkeypatch):
    app = app_for_delete(tmp_path)
    processor = LabelMatchScreenProcessor(app, module=lm)
    monkeypatch.setattr(app.package_cancellation_outbox, 'enqueue', mock.Mock(side_effect=OSError('outbox unavailable')))
    try:
        assert processor.command('scan_cancel', {'rows': ['SET-1']})['ok'] is True
        result = processor.command('approve', {'id': _approval_id(_pointer_pending(app))})
        assert result['ok'] is False and result['code'] == 'SCAN_CANCEL_FAILED'
        assert result['deleted'] == 0 and result['result']['kind'] == 'warn'
        assert len(result['state']['today']['rows']) == 2
        assert len(app.history_tree.rows) == 2 and len(app.global_scanned_set) == 2
        assert app.data_manager.events == []
        assert any(row['kind'] == 'bad' for row in processor._dialogs)
    finally:
        processor.uninstall()


def test_rebuild_failure_with_two_existing_dialogs_never_reports_success(tmp_path, monkeypatch):
    app = app_for_delete(tmp_path, central=False)
    processor = LabelMatchScreenProcessor(app, module=lm)
    native = mock.Mock(side_effect=AssertionError('native dialog created'))
    monkeypatch.setattr(lm.tk, 'Toplevel', native)
    processor._dialog('warn', '기존 안내', '첫 안내')
    processor._dialog('warn', '기존 안내', '둘째 안내')
    failure = OSError('summary rebuild failed')
    monkeypatch.setattr(app, '_update_summary_tree', mock.Mock(side_effect=failure))
    try:
        requested = processor.command('scan_cancel', {'rows': ['SET-1']})
        assert requested['code'] == 'SCAN_CANCEL_CONFIRMATION_REQUIRED'
        assert len(processor._dialogs) == 2
        reply = processor.command('approve', {'id': _approval_id(_pointer_pending(app))})
        assert reply['ok'] is False and reply['code'] == 'SCAN_CANCEL_FAILED'
        assert reply['deleted'] == 1 and reply['result']['kind'] == 'warn'
        assert len(processor._dialogs) == 2
        assert any(row['kind'] == 'bad' for row in processor._dialogs)
        assert not app.history_tree.exists('SET-1')
        assert len(app.data_manager.events) == 1 and app.data_manager.flushed
        native.assert_not_called()
    finally:
        processor.uninstall()


@pytest.mark.parametrize('value', [None, False, 1, '업무 오류'])
def test_removed_row_without_explicit_business_success_is_failure(tmp_path, value):
    app = app_for_delete(tmp_path, central=False)
    processor = LabelMatchScreenProcessor(app, module=lm)
    def callback():
        app.history_tree.delete('SET-1')
        return value
    try:
        reply = processor._run_scan_cancel(['SET-1'], callback)
        assert reply['ok'] is False and reply['code'] == 'SCAN_CANCEL_FAILED'
        assert reply['deleted'] == 1 and reply['result']['kind'] == 'warn'
    finally:
        processor.uninstall()


def test_business_central_conflict_preserves_warning_and_reports_failure(tmp_path, monkeypatch):
    app = app_for_delete(tmp_path)
    processor = LabelMatchScreenProcessor(app, module=lm)
    original = app._queue_authoritative_package_cancellation
    def conflict(**kwargs):
        return {**original(**kwargs), 'status': 'CONFLICT'}
    monkeypatch.setattr(app, '_queue_authoritative_package_cancellation', conflict)
    try:
        requested = processor.command('scan_cancel', {'rows': ['SET-1']})
        reply = processor.command('approve', {'id': requested['state']['approval']['id']})
        assert reply['ok'] is False and reply['code'] == 'SCAN_CANCEL_FAILED'
        assert reply['deleted'] == 1 and reply['result']['kind'] == 'warn'
        assert any(row['kind'] == 'warn' for row in processor._dialogs)
        assert len(app.data_manager.events) == 1 and app.data_manager.flushed
    finally:
        processor.uninstall()


@pytest.mark.parametrize('blocked', ['busy', 'read_only', 'loading'])
def test_scan_cancel_blocks_mutation_during_processing_or_read_only_view(tmp_path, blocked):
    app = app_for_delete(tmp_path)
    if blocked == 'busy':
        app._ui_lane_is_busy = lambda: True
    elif blocked == 'read_only':
        app.history_view_updates_active_state = False
    else:
        app.history_active_load_pending = True
    processor = LabelMatchScreenProcessor(app, module=lm)
    try:
        assert processor.command('scan_cancel', {'rows': ['SET-1']})['ok'] is False
        assert len(app.history_tree.rows) == 2 and len(app.global_scanned_set) == 2
        assert app.data_manager.events == []
        assert app.package_cancellation_outbox.get_by_set_id('SET-1') == []
    finally:
        processor.uninstall()

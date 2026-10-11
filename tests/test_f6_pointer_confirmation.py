"""F6: real application callbacks with widgets/services replaced, never a Tk root."""
import copy
import datetime
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest
import Label_Match as lm
from tests.test_label_match_core import _completed_details


class Widget:
    def __init__(self, *args, **options):
        self.options = {'state': 'normal', **options}
        self.bindings = {}
        self.children = []
        self.content = ''
        self.alive = True
        if args and hasattr(args[0], 'children'):
            args[0].children.append(self)

    def bind(self, key, callback):
        self.bindings[key] = callback

    def configure(self, **options):
        self.options.update(options)

    config = configure
    def cget(self, key): return self.options.get(key)
    def instate(self, states): return 'disabled' in states and self.options['state'] == 'disabled'
    def state(self, states): self.options['button_states'] = list(states)
    def winfo_width(self): return 100
    def winfo_height(self): return 30
    def winfo_children(self): return self.children
    def winfo_exists(self): return self.alive
    def destroy(self): self.alive = False
    def grid(self, **options): pass
    def grid_configure(self, **options): pass
    def grid_remove(self): pass
    def pack(self, **options): pass
    def focus_set(self): pass
    def delete(self, *args): self.content = ''
    def insert(self, _index, text): self.content += text
    def get(self): return self.content
    def selection_get(self): return datetime.date(2026, 10, 4)


@pytest.fixture
def app(monkeypatch):
    instance = lm.Label_Match.__new__(lm.Label_Match)
    instance.initialized_successfully = True
    instance.run_tests = False
    instance.operator_workbench_ready = False
    instance.history_view_updates_active_state = True
    instance.current_set_info = {'id': 'current', 'raw': ['MASTER', 'PRODUCT']}
    instance.set_details_map = {'completed': _completed_details(
        'completed', 'ITEM1', '2026-10-05T12:00:00', ['ITEM1', 'PRODUCT'])}
    instance.scan_count = {}
    instance.history_tree = NS(selection=lambda: ('completed',), exists=lambda iid: True,
                               item=lambda *args: ('PASS',), delete=Mock())
    instance.history_detail_text = Widget()
    instance.history_detail_frame = Widget()
    instance.workflow_notice_action_button = Widget()
    instance.entry = Widget()
    instance._render_operator_workbench = Mock()
    instance._focus_scan_entry_if_available = Mock()
    instance._app_close_blocks_local_action = Mock(return_value=False)
    instance._block_active_history_load_action = Mock(return_value=False)
    instance._block_background_history_reload = Mock(return_value=False)
    instance._block_duplicate_history_load = Mock(return_value=False)
    instance._has_background_work = Mock(return_value=False)
    instance._sealed_transfer_exchange_blocks_local_action = Mock(return_value=False)
    instance._release_shortage_confirmation = Mock(return_value=True)
    instance._save_current_set_state = Mock(return_value=True)
    instance._record_pending_app_close_cancellations = Mock()
    instance._suspend_deferred_validation_for_close = Mock()
    instance._cancel_app_close_lane_drain_watchdog = Mock()
    instance._show_ui_lane_rejection = Mock()
    instance._cancel_pending_ui_jobs = Mock()
    instance._begin_app_close_direct_sync = Mock()
    instance.direct_sync_bootstrap_context = {'synthetic': True}
    instance.is_blinking = False
    instance.data_manager = NS(log_event=Mock(), close=Mock())
    instance._queue_authoritative_package_cancellation = Mock(return_value=None)
    instance._commit_package_cancellation_local_event = Mock()
    instance._history_details_for_iid = lambda iid: instance.set_details_map.get(iid)
    instance._history_result_value = lambda values: lm.Label_Match.Results.PASS
    instance._remove_history_details_for_iid = Mock()
    instance._rebuild_global_scanned_set_from_details = Mock()
    instance._update_summary_tree = Mock()
    instance._render_history_detail = Mock()
    instance._package_cancellation_completion_message = Mock(return_value='완료')
    instance._play_sound = Mock()
    instance.update_big_display = Mock()
    instance._load_history_and_rebuild_summary = Mock()
    instance._process_history_queue = Mock()
    for name in ('showinfo', 'showwarning', 'showerror'):
        monkeypatch.setattr(lm.messagebox, name, Mock())
    for name in ('askyesno', 'askokcancel'):
        monkeypatch.setattr(lm.messagebox, name, Mock(side_effect=AssertionError('native approval forbidden')))
    return instance


def click(app):
    event = NS(widget=app.workflow_notice_action_button, num=1, x=10, y=10)
    pressed = app.workflow_notice_action_button.bindings.get('<ButtonPress-1>')
    if pressed is not None:
        pressed(event)
    return app._confirm_pointer_action(event)


def start(app, place):
    if place == 'delete':
        app._delete_selected_row()
    elif place == 'cancel':
        app._cancel_completed_tray_by_label('ITEM1')
    elif place == 'close':
        app.on_closing()
    else:
        app._label_recovery_source_is_held = Mock(return_value=False)
        def resume(_working, **kwargs):
            if not kwargs['confirm_ambiguous_reprint']:
                raise lm.PHSLabelWorkflowError('PHS_PRINT_REPRINT_CONFIRMATION_REQUIRED', 'synthetic')
            return None
        app.phs_label_exchange_coordinator = NS(_resume_current=Mock(side_effect=resume))
        def submit(**kwargs):
            payload = kwargs['work']()
            kwargs['settle'](payload, None)
            kwargs['finish'](payload)
            # This double settles synchronously; no task remains pending.
            return None
        app._submit_ui_lane_task = submit
        app._start_phs_label_recovery()


def approved(app, place):
    if place in ('delete', 'cancel'):
        return app._commit_package_cancellation_local_event.call_count
    if place == 'close':
        return app.data_manager.close.call_count
    return sum(bool(call.kwargs['confirm_ambiguous_reprint'])
               for call in app.phs_label_exchange_coordinator._resume_current.call_args_list)


@pytest.mark.parametrize('place', ['delete', 'cancel', 'reprint', 'close'])
@pytest.mark.parametrize('keys', ['CLC=AAA2270730100|SPC=YYY|PHS=1\r', '\r', 'Y', 'N', ' '])
def test_scanner_enter_mnemonics_and_space_never_approve(app, place, keys):
    start(app, place)
    before = copy.deepcopy(app.set_details_map)
    for char in keys:
        key = 'Return' if char == '\r' else 'space' if char == ' ' else char
        assert app._pointer_confirmation_key(NS(keysym=key, char=char)) == 'break'
    # A ttk class binding invoking command also cannot authorize anything.
    app._acknowledge_workflow_notice()
    assert approved(app, place) == 0
    assert app.set_details_map == before
    assert '받지 않았습니다' in app.update_big_display.call_args.args[0]
    assert app._phs_label_guidance_notice.kind == 'pointer_confirmation'
    assert click(app) == 'break'
    assert approved(app, place) == 1


@pytest.mark.parametrize('place', ['delete', 'cancel', 'reprint', 'close'])
def test_pointer_approval_runs_once(app, place):
    start(app, place)
    assert approved(app, place) == 0
    assert click(app) == 'break'
    assert approved(app, place) == 1
    click(app)
    assert approved(app, place) == 1


@pytest.mark.parametrize("status", [None, "ACKED", "PENDING", "UNKNOWN", "CONFLICT"])
def test_cancel_success_uses_status_line_after_target_approval(app, status):
    cancellation = None if status is None else {
        "status": status, "central_cancellation_acked": status == "ACKED",
    }
    app._queue_authoritative_package_cancellation.return_value = cancellation
    label = next(iter(app.set_details_map.values()))["master_label_code"]
    app._cancel_completed_tray_by_label(label)
    app._commit_package_cancellation_local_event.assert_not_called()
    click(app)
    app._commit_package_cancellation_local_event.assert_called_once()
    lm.messagebox.showinfo.assert_not_called()
    if status in {None, "ACKED"}:
        lm.messagebox.showwarning.assert_not_called()
        app.update_big_display.assert_called_with(
            app._package_cancellation_completion_message.return_value, "green")
    else:
        lm.messagebox.showwarning.assert_called_once()
        assert not any(len(call.args) > 1 and call.args[1] == "green"
                       for call in app.update_big_display.call_args_list)


@pytest.mark.parametrize('place', ['delete', 'cancel', 'reprint', 'close'])
def test_changed_target_cannot_use_old_approval(app, place):
    start(app, place)
    if place in ('delete', 'cancel'):
        app.set_details_map['completed']['item_name'] = 'CHANGED'
    else:
        app.current_set_info['raw'].append('CHANGED')
    click(app)
    assert approved(app, place) == 0


def test_scan_during_confirmation_reaches_business_input_once(app):
    start(app, 'delete')
    payload = 'CLC=AAA2270730100|SPC=YYY|PHS=1'
    app.entry.content = payload
    accepted = []
    def process(_event):
        accepted.append(app.entry.get())
        app.entry.delete(0, 'end')
        return 'break'
    app.process_input = process
    app._handle_scan_enter(NS(widget=app.entry, keysym='Return'))
    assert accepted == [payload]
    assert app._commit_package_cancellation_local_event.call_count == 0


@pytest.mark.parametrize('keys', ['SCAN\r', '\r', 'Y', 'N', ' '])
def test_inline_detail_keeps_keys_out_of_business_actions(app, keys):
    app._barcode_detail_text = Mock(return_value='full raw text')
    app._show_barcode_detail_window({'synthetic': True})
    assert app.history_detail_text.content == 'full raw text'
    for char in keys:
        assert app.history_detail_text.bindings['<KeyPress>'](NS(char=char)) == 'break'
    assert app._commit_package_cancellation_local_event.call_count == 0
    assert '받지 않았습니다' in app.update_big_display.call_args.args[0]
    app._focus_scan_entry_if_available.assert_called()


@pytest.fixture
def calendar(app, monkeypatch):
    import sys
    calendar_module = NS(Calendar=Widget)
    monkeypatch.setitem(sys.modules, 'tkcalendar', calendar_module)
    monkeypatch.setattr(lm.ttk.Frame, '__init__', Widget.__init__)
    for name in ('grid', 'winfo_children'):
        monkeypatch.setattr(lm.ttk.Frame, name, getattr(Widget, name))
    monkeypatch.setattr(lm.ttk.Frame, 'bind', Widget.bind)
    monkeypatch.setattr(lm.ttk.Frame, 'destroy', Widget.destroy)
    monkeypatch.setattr(lm.ttk, 'Frame', Widget)
    monkeypatch.setattr(lm.ttk, 'Button', Widget)
    return lm.CalendarWindow(app)


@pytest.mark.parametrize('keys', ['SCAN\r', '\r', 'Y', 'N', ' '])
def test_inline_calendar_only_mouse_selection_loads_history(app, calendar, keys):
    pending = [calendar]
    while pending:
        widget = pending.pop()
        for char in keys:
            assert widget.bindings['<KeyPress>'](NS(char=char)) == 'break'
            assert widget.bindings['<KeyRelease>'](NS(char=char)) == 'break'
        pending.extend(widget.children)
    app._load_history_and_rebuild_summary.assert_not_called()
    assert '받지 않았습니다' in app.update_big_display.call_args.args[0]
    calendar.on_select()
    app._load_history_and_rebuild_summary.assert_called_once_with(datetime.datetime(2026, 10, 4))
    assert not calendar.alive


def test_pointer_notice_keeps_entry_focus_and_warning_style(app):
    start(app, 'delete')
    app.workflow_notice_title_label = Widget()
    app.workflow_notice_label = Widget()
    app.workflow_notice_frame = Widget()
    app.colors = {'text': 'black'}
    app._compact_operator_notice_message = lambda value: value
    app._fit_operator_notice_geometry = Mock()
    button = app.workflow_notice_action_button
    button.focus_set = Mock()
    app._set_workflow_notice_ui(app._phs_label_guidance_notice, '')
    button.focus_set.assert_not_called()
    assert button.options['takefocus'] is False
    assert app.workflow_notice_frame.options['bg'] == '#FFFBEB'


@pytest.mark.parametrize('disabled,outside', [(True, False), (False, True)])
def test_disabled_or_outside_pointer_release_cannot_approve(app, disabled, outside):
    start(app, 'delete')
    button = app.workflow_notice_action_button
    button.configure(state='disabled' if disabled else 'normal')
    app._confirm_pointer_action(NS(widget=button, num=1, x=120 if outside else 10, y=10))
    assert approved(app, 'delete') == 0
    button.configure(state='normal')
    click(app)
    assert approved(app, 'delete') == 1


def test_press_on_old_notice_cannot_release_into_new_approval(app):
    start(app, 'delete')
    button = app.workflow_notice_action_button
    event = NS(widget=button, num=1, x=10, y=10)
    pressed = button.bindings.get('<ButtonPress-1>')
    if pressed is not None:
        pressed(event)
    # The same button now represents a different confirmation before mouse-up.
    app.set_details_map['completed']['item_name'] = 'NEW TARGET'
    start(app, 'delete')
    app._confirm_pointer_action(event)
    assert approved(app, 'delete') == 0
    click(app)
    assert approved(app, 'delete') == 1


def test_another_notice_cannot_show_an_old_approval_button(app):
    start(app, 'delete')
    app.workflow_notice_title_label = Widget()
    app.workflow_notice_label = Widget()
    app.workflow_notice_frame = Widget()
    app.colors = {'text': 'black'}
    app._compact_operator_notice_message = lambda value: value
    app._fit_operator_notice_geometry = Mock()
    app._set_workflow_notice_ui(lm.WorkflowNotice('다른 안내', '다른 작업'), '')
    assert app._pointer_confirmation is None
    click(app)
    assert approved(app, 'delete') == 0

"""G86 scanner ownership regressions; source methods run without starting the app."""
"""G86 RED probes: compile unchanged production method AST, no app startup or I/O.

Entry models Tk's disabled delete/insert. Pump is bounded to 200 callbacks and
0.5 virtual seconds. The final success sink records only in memory.
"""
import ast
import copy
import importlib.util
import sys
import time
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
ITEM = '1234567890123'
FIRST = ITEM + '-OLD'
SECOND = ITEM + '-NEW'


def module_at(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


LM_CARRIER = module_at('carrier_identity_port', 'carrier_identity_port.py')
LM_TRANSITION = module_at('_g86_lm_transition', 'label_transition.py')


class TclError(Exception):
    pass


class Entry:
    def __init__(self):
        self.value = ''
        self.state = 'normal'
        self.fail_delete = 0

    def get(self):
        return self.value

    def cget(self, key):
        return self.state

    def configure(self, **kw):
        self.state = kw.get('state', self.state)

    def delete(self, *args):
        if self.fail_delete:
            self.fail_delete -= 1
            raise TclError('injected delete failure')
        if self.state == 'normal':
            self.value = ''

    def insert(self, at, value):
        if self.state == 'normal':
            self.value = (self.value + value) if at == 'end' else (value + self.value)


class Pump:
    def __init__(self):
        self.jobs = []

    def after(self, ms, fn, *args):
        self.jobs.append((ms / 1000, fn, args))

    def drain(self):
        elapsed = 0.0
        for _ in range(200):
            if not self.jobs:
                return
            delay, fn, args = self.jobs.pop(0)
            elapsed += delay
            assert elapsed <= 0.5, 'virtual-time cap'
            fn(*args)
        assert not self.jobs, 'callback-count cap'


def methods(relative, names, extra=None):
    path = ROOT / relative
    tree = ast.parse(path.read_text(encoding='utf-8-sig'))
    names = set(names)
    if relative == "Label_Match.py":
        names.update(node.name for node in ast.walk(tree)
                     if isinstance(node, ast.FunctionDef)
                     and node.name == "_refuse_broken_scan_entry")
    selected = []
    for owner in tree.body:
        if isinstance(owner, ast.ClassDef):
            selected.extend(n for n in owner.body if isinstance(n, ast.FunctionDef) and n.name in names)
    assert {n.name for n in selected} == set(names)
    namespace = dict(time=time, copy=copy, Any=object, Optional=NS(__getitem__=lambda _: object),
                     tk=NS(END='end', NORMAL='normal', DISABLED='disabled', TclError=TclError),
                     TclError=TclError, LANE_DRAINING='DRAINING', LANE_BROKEN='BROKEN', LANE_CLOSED='CLOSED', LANE_BUSY='BUSY',
                     LaneState=NS(BROKEN='BROKEN'), NOTICE_SCAN_REFUSAL_TEXT='scan refused')
    namespace.update(extra or {})
    module = ast.Module(body=[ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0), *selected], type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), str(path), 'exec'), namespace)
    return {name: namespace[name] for name in names}


LM_NAMES = {'_handle_scan_enter','intake_scan','process_input','_consume_scan_entry','_hold_scan_behind_completion',
            '_sealed_transfer_exchange_blocks_local_action','_block_view_only_action','_block_active_history_load_action',
            '_acknowledge_workflow_notice','_refuse_dialog_scan'}
LM = methods('Label_Match.py', LM_NAMES, dict(
    label_match_test_tools_enabled=lambda **kw: False,
    _label_match_has_central_source_identity=lambda value: False,
    carrier_identity=LM_CARRIER,
    label_transition=LM_TRANSITION,
    PHSLabelWorkflowError=type('PHSLabelWorkflowError', (Exception,), {}),
    messagebox=NS(showwarning=lambda *a,**k: None)))
LM_BRIDGE = methods('label_screen_bridge.py', {'scan'},dict(
    _view_and_source=lambda app: (None,None), _result=lambda *args: {'text':'','kind':'ok'}))


def lm_app():
    app = type('LMProbe', (), LM)()
    app.entry, app.accepted = Entry(), []
    app.current_set_info = {'raw':['MASTER'], 'parsed':[ITEM], 'id':'set'}
    app.initialized_successfully, app.is_blinking = True, False
    app.global_scanned_set = set()
    app.data_manager = NS(log_event=lambda *args: None)
    app.Events = NS(SCAN_ATTEMPT='attempt')
    app.run_tests = True
    app._ui_lane_is_busy = lambda: False
    app._partial_submit_confirm_state = lambda: None
    app._render_operator_workbench = lambda: NS(scan_input_enabled=True, notice=None)
    for name in ('_clear_scanner_notice','_return_to_scanner_workbench','_clear_partial_submit_confirmation',
        '_play_sound','_show_ui_lane_rejection','_show_scanner_notice','_stop_error_siren','_finalize_set',
        '_show_package_recovery_block','_set_active_package_submission_notice','_reconcile_active_package_submission',
        '_package_pin_ui','_show_package_recovery_workbench','update_big_display','_accept_background_exchange_receipt'):
        setattr(app, name, lambda *a, **k: None)
    app._reject_residual_tag_input = lambda *a, **k: False
    app._app_close_blocks_local_action = lambda: False
    app._completion_scan_queue_open = lambda: False
    app._roster_worker_allowed = lambda: True
    app._csv_completed_set_blocks_edit = lambda _: False
    app._replay_scanner_audits = lambda: None
    app._current_sealed_transfer_exchange_attempt = lambda: None
    app._workflow_total_scan_count = lambda: 6
    app._workflow_final_label_position = lambda: 6
    app._legacy_label_needs_transition = lambda: False
    app._sample_barcode = lambda value: value
    app._storage_held_duplicate_keys = lambda: set()
    app._known_sample_members = lambda *a: None
    app._update_on_success_scan = lambda value, *a, **k: app.accepted.append(value)
    return app


LM_RED = ['startup','closing','busy','blinking','not_initialized','view_only','history_pending','history_error',
          'pending_error','workflow_pending_error','screen_gate','screen_missing_view','submission_blocked',
          'outbox_hold','label_lookup','label_exchange','label_coordinator_pending','label_coordinator_error',
          'recovery_review','seal_lookup','package_preflight','package_pending','package_sending','package_acked','package_conflict',
          'exchange_pending','exchange_review','seal_pending']


def set_lm_case(app, case):
    app.operator_workbench_ready = False
    if case in {'startup','closing'}:
        setattr(app, '_startup_update_pending' if case == 'startup' else '_app_close_in_progress', True)
    elif case == 'busy': app._ui_lane_is_busy = lambda: True
    elif case == 'blinking': app.is_blinking = True
    elif case == 'not_initialized': app.initialized_successfully = False
    elif case == 'view_only': app.history_view_updates_active_state = False
    elif case == 'history_pending': app.history_active_load_pending = True
    elif case == 'history_error': app._history_active_load_error = 'injected'
    elif case in {'pending_error','workflow_pending_error'}:
        setattr(app, '_pending_workflow_error' if case == 'pending_error' else '_workflow_pending_error', {'result':'fail'})
    elif case in {'screen_gate','screen_missing_view'}:
        app.operator_workbench_ready = True
        app._render_operator_workbench = lambda: NS(scan_input_enabled=False, notice=None) if case == 'screen_gate' else None
    elif case == 'submission_blocked':
        app._workflow_blocking_notice = NS(kind='submission_blocked')
        app._workflow_notice_action = app._show_package_recovery_workbench
    elif case == 'outbox_hold': app.package_outbox = NS(get_workbench_hold=lambda _: True)
    elif case in {'label_lookup','label_exchange','seal_lookup','package_preflight'}:
        setattr(app, {'label_lookup':'_phs_label_scan_lookup_in_progress','label_exchange':'_phs_label_exchange_pending',
                      'seal_lookup':'_central_seal_lookup_in_progress','package_preflight':'_central_package_preflight_in_progress'}[case], True)
    elif case.startswith('label_coordinator'):
        def pending():
            if case.endswith('error'): raise LM['process_input'].__globals__['PHSLabelWorkflowError']()
            return True
        app.phs_label_exchange_coordinator = NS(has_pending_reconciliation=pending,blocks_other_action=lambda _:False)
    elif case == 'recovery_review': app.current_set_info['recovery_operator_review'] = True
    elif case.startswith('package_'):
        app.package_outbox = NS(get_by_set_id=lambda _: {'status':case.split('_')[1].upper()})
    elif case in {'exchange_pending','exchange_review','seal_pending'}:
        app._current_sealed_transfer_exchange_attempt = lambda: NS(status='ACKED' if case == 'seal_pending' else 'OPERATOR_REVIEW' if case == 'exchange_review' else 'PENDING',
            local_apply_status='',seal_verification_status='PENDING' if case == 'seal_pending' else '')


def recover_lm(app):
    fresh = lm_app()
    entry, accepted = app.entry, app.accepted
    app.__dict__.clear()
    app.__dict__.update(fresh.__dict__)
    app.entry, app.accepted = entry, accepted
    app._update_on_success_scan = lambda value,*a,**k: app.accepted.append(value)
    app.entry.configure(state='normal')


@pytest.mark.parametrize('case', LM_RED)
def test_lm_tk_refusal_then_next_scan_isolated(case):
    app = lm_app()
    set_lm_case(app, case)
    app.entry.insert('end', FIRST)
    app._handle_scan_enter()
    assert app.accepted == []
    recover_lm(app)
    app.entry.insert('end', SECOND)
    app._handle_scan_enter()
    assert app.accepted == [SECOND]
    assert app.entry.get() == ''


@pytest.mark.parametrize('case', LM_RED)
def test_lm_bridge_refusal_then_next_scan_isolated(case):
    app = lm_app()
    set_lm_case(app, case)
    bridge = type('LMBridgeProbe', (), LM_BRIDGE)()
    bridge.app, bridge._serial, bridge._dialogs = app, 0, []
    bridge.scan(FIRST,{})
    assert app.accepted == []
    recover_lm(app)
    bridge.scan(SECOND,{})
    assert app.accepted == [SECOND]
    assert app.entry.get() == ''


def test_lm_tk_normal_input_consumed_before_success():
    app = lm_app()
    app.entry.insert('end', SECOND)
    app._handle_scan_enter()
    assert app.accepted == [SECOND]
    assert app.entry.get() == ''


@pytest.mark.parametrize('state', ['normal', 'disabled'])
def test_lm_tk_snapshot_is_cleared_before_intake_and_state_restored(state):
    app = lm_app()
    app.entry.insert(0, FIRST)
    app.entry.configure(state=state)
    received = []

    def intake(text, *, event=None, from_entry=False):
        assert app.entry.get() == ''
        assert app.entry.cget('state') == state
        assert from_entry is False
        received.append(text)

    app.intake_scan = intake
    app._handle_scan_enter()
    assert received == [FIRST]


def test_lm_tk_permanent_delete_failure_refuses_following_enters():
    app = lm_app()
    app.entry.insert(0, FIRST)
    app.entry.fail_delete = 100
    received, sounds, notices = [], [], []
    app.intake_scan = lambda *args, **kwargs: received.append(args)
    app._play_sound = sounds.append
    app.update_big_display = lambda text, *args: notices.append(text)
    app._handle_scan_enter()
    app.entry.fail_delete = 0
    app.entry.value = SECOND
    app._handle_scan_enter()
    assert received == []
    assert sounds == ['fail', 'fail']
    assert len(notices) == 2
    assert notices == ['입력 칸 오류 — 프로그램을 다시 켜세요'] * 2


@pytest.mark.parametrize('entry_case', ['normal','disabled','delete_exception'])
def test_lm_tk_fifo_then_next_scan_isolated(entry_case):
    app = lm_app()
    app.entry.insert('end', FIRST)
    app._ui_lane_is_busy = lambda: True
    app._completion_scan_queue_open = lambda: True
    if entry_case == 'disabled': app.entry.configure(state='disabled')
    if entry_case == 'delete_exception': app.entry.fail_delete = 1
    app._handle_scan_enter()
    assert app._scans_behind_completion == [FIRST]
    app._ui_lane_is_busy = lambda: False
    app.process_input(_scan_behind_completion=app._scans_behind_completion.pop(0))
    assert app.accepted == [FIRST]
    app.entry.configure(state='normal')
    app.entry.insert('end', SECOND)
    app._handle_scan_enter()
    assert app.accepted == [FIRST, SECOND]


@pytest.mark.parametrize('case', ['pending_pointer','read_dialog'])
def test_lm_dialog_refusal_does_not_populate_entry(case):
    app = lm_app()
    if case == 'pending_pointer':
        callback = lambda: None
        app._pointer_confirmation = (None, callback, None)
        app._workflow_notice_action = callback
    app._refuse_dialog_scan(NS(char='A'))
    app.entry.insert('end', SECOND)
    app._handle_scan_enter()
    assert app.accepted == [SECOND]


@pytest.mark.parametrize('transport', ['tk','bridge'])
@pytest.mark.parametrize('case', ['render_exception','audit_exception'])
def test_lm_exception_then_next_scan_isolated(case, transport):
    app = lm_app()
    def fail(*args, **kwargs): raise OSError('injected helper failure')
    if case == 'render_exception':
        app.operator_workbench_ready = True
        app._render_operator_workbench = fail
    else:
        app._replay_scanner_audits = fail
    bridge = type('LMBridgeProbe', (), LM_BRIDGE)()
    bridge.app, bridge._serial, bridge._dialogs = app, 0, []
    if transport == 'tk':
        app.entry.insert('end', FIRST)
        with pytest.raises(OSError): app._handle_scan_enter()
    else:
        with pytest.raises(OSError): bridge.scan(FIRST,{})
    recover_lm(app)
    if transport == 'tk':
        app.entry.insert('end', SECOND)
        app._handle_scan_enter()
    else:
        bridge.scan(SECOND,{})
    assert app.accepted == [SECOND]


@pytest.mark.parametrize('transport', ['tk','bridge'])
@pytest.mark.parametrize('case', ['label_hold_lookup_exception','central_label_duplicate'])
def test_lm_first_label_refusal_then_next_scan_isolated(monkeypatch, transport, case):
    app = lm_app()
    app.current_set_info = {'raw':[], 'parsed':[]}
    app._transition_mode_active = lambda: False
    app.package_logistics_client = object()
    namespace = LM['process_input'].__globals__
    for name, value in {
        '_label_match_unique_master_index_keys': lambda value: {value},
        '_label_match_parse_sealed_transfer_qr': lambda value: None,
        '_label_match_display_fields': lambda value: {'SRC':'KMTECH_INPUT_TAG','ITG':'synthetic'},
        '_label_match_parse_compact_phs2': lambda value: {'CLC':ITEM,'ITG':'synthetic'},
        '_label_match_has_central_source_identity': lambda value: True,
        '_label_match_reusable_input_master_label': lambda value: False,
    }.items(): monkeypatch.setitem(namespace,name,value)
    if case == 'label_hold_lookup_exception':
        app.package_outbox = NS()
        def fail(): raise OSError('injected hold lookup')
        app._recover_unknown_package_hold_identities = fail
    else:
        app.global_scanned_set = {FIRST}
        app._handle_input_error = lambda *args, **kw: None
        app._truncate_string = lambda value: ''
    bridge = type('LMBridgeProbe', (), LM_BRIDGE)()
    bridge.app, bridge._serial, bridge._dialogs = app, 0, []
    if transport == 'tk':
        app.entry.insert('end',FIRST)
        app._handle_scan_enter()
    else:
        bridge.scan(FIRST,{})
    assert app.accepted == []
    recover_lm(app)
    if transport == 'tk':
        app.entry.insert('end',SECOND)
        app._handle_scan_enter()
    else:
        bridge.scan(SECOND,{})
    assert app.accepted == [SECOND]

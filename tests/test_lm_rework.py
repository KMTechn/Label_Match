"""Headless regressions for pre-owner cleanup and web recovery outcomes."""
import hashlib
import secrets
import types
import weakref
from unittest import mock

import pytest

import Label_Match as lm
import auto_update
import label_match_screen as screen
import writer_session_fence as fence
from label_admin_pin import AdminPinError
from label_data_generation import DataGenerationError
from label_screen_bridge import LabelMatchScreenProcessor
from tests.test_label_admin_pin import _pin_app
from tests.test_label_screen_bridge import FakeApp
from tests.test_tk_serial_ui_lane import FakeTkRoot
from tk_serial_ui_lane import TkSerialUiLane


@pytest.fixture(autouse=True)
def fresh_workers(monkeypatch):
    monkeypatch.setattr(fence, '_WRITER_EXIT_CLOSED', False)
    monkeypatch.setattr(fence, '_WRITER_EXIT_COUNT', 0)
    monkeypatch.setattr(fence, '_LANE_WORKERS', weakref.WeakSet())


@pytest.mark.parametrize('stage', ['relay', 'trial', 'after'])
def test_pre_owner_initialization_failure_stops_actual_lane(monkeypatch, stage):
    root = FakeTkRoot()
    app = types.SimpleNamespace(ui_lane=TkSerialUiLane(root))
    app._finish_after_mainloop = mock.Mock(side_effect=app.ui_lane.close_idle)
    app.attributes = lambda *_a: None
    app.after = mock.Mock(side_effect=RuntimeError('pre-owner after failed'))
    report = {'start_path': 'FAST_WINDOW_FIRST'}
    host = {'requested': True, 'active': True, 'run': None, 'catalog_unconfirmed': False, 'relay_reuse_report': report}
    monkeypatch.setattr(lm, 'LABEL_MATCH_SCREEN_HOST', host)
    monkeypatch.setattr(lm, 'Label_Match', lambda **_kw: app)
    monkeypatch.setattr(lm, '_startup_app_stop_requested', lambda: False)
    monkeypatch.setattr(lm, '_label_match_startup_trace', lambda *_a, **_kw: None)
    monkeypatch.setattr(lm, '_schedule_reused_relay_verification',
                        mock.Mock(side_effect=RuntimeError('pre-owner relay failed') if stage == 'relay' else None))
    monkeypatch.setattr(auto_update, 'trial_context',
                        mock.Mock(side_effect=RuntimeError('pre-owner trial failed') if stage == 'trial' else None,
                                  return_value={} if stage == 'after' else None))
    bridge = mock.Mock()
    try:
        screen.owner_main_for(lm)(bridge)
        assert isinstance(bridge.fail_owner.call_args_list[0].args[0], RuntimeError)
        assert 'pre-owner' in str(bridge.fail_owner.call_args_list[0].args[0])
        bridge.attach_owner.assert_not_called()
        app._finish_after_mainloop.assert_called_once()
        assert app.ui_lane.worker_thread.daemon is False
        assert not app.ui_lane.worker_thread.is_alive()
        assert fence.close_and_wait_writers(0) is True
    finally:
        app.ui_lane._signal_worker_stop()
        app.ui_lane.worker_thread.join(2)


@pytest.fixture
def recovery(tmp_path, monkeypatch, lock_remote):
    app, journal, server = _pin_app(tmp_path, monkeypatch)
    root = FakeTkRoot()
    app._ui_lane_generation = 0
    app.after = root.after
    app.after_idle = lambda callback: root.after(0, callback)
    app.winfo_exists = lambda: not root.destroyed
    app._focus_scan_entry_if_available = lambda: None
    app._render_operator_workbench = lambda: None
    app._render_logistics_connection_footer = lambda: None
    app.update_big_display = lambda *_a: None
    app._play_sound = lambda *_a, **_kw: None
    app.open_settings_window = lambda: None
    app._show_ui_lane_rejection = lambda *_a: None
    app.ui_lane = TkSerialUiLane(root, poll_ms=1, generation_provider=lambda: app._ui_lane_generation)
    target = 'F5:' + hashlib.sha256(journal.path.read_bytes()).hexdigest()
    # Retain an unused issued request; the real reconcile method sees EXPIRED.
    action = 'LABEL.F5_HOLD'
    pin_target, source_sha = app._package_pin_target(action, target)
    operation = secrets.token_hex(16)
    app._admin_pin_store.prepare(operation_key=operation, verification_id=secrets.token_hex(16),
                                action=action, target=pin_target, operator=app._package_pin_operator(),
                                admin_id='review-admin', source_evidence_sha256=source_sha)
    app._admin_pin_store.transition(operation, 'UNVERIFIED_OPERATOR_HOLD', error_code='VERIFY_UNAVAILABLE')
    monkeypatch.setattr(app._admin_pin_client, 'status', lambda *_a: 'EXPIRED')
    native = mock.Mock(side_effect=AssertionError('native prompt created'))
    monkeypatch.setattr(lm.tk, 'Toplevel', native)
    # Force the native branch on old code if the web adapter fails to replace it.
    app.run_tests = False
    app._prompt_package_admin_pin = types.MethodType(lm.Label_Match._prompt_package_admin_pin, app)
    processor = LabelMatchScreenProcessor(app, module=lm)
    yield types.SimpleNamespace(app=app, root=root, target=target, operation=operation,
                                processor=processor, native=native, server=server)
    processor.uninstall()
    if app.ui_lane.is_busy():
        root.run_until(lambda: not app.ui_lane.is_busy())
    app.ui_lane.close_idle()


def test_retry_rejects_empty_credentials_before_processing(recovery, monkeypatch):
    p = recovery
    retry = mock.Mock()
    monkeypatch.setattr(p.app, '_retry_package_pin_item', retry)
    assert p.processor.command('recovery_retry_pin', {'target': p.target})['ok'] is False
    retry.assert_not_called()
    p.native.assert_not_called()


def test_expired_web_request_returns_authentication_need_without_toplevel(recovery, monkeypatch):
    p = recovery
    original = p.app._prepare_package_pin_action
    def prepare(*args):
        result = original(*args)
        assert result[0] is False
        # The one-shot credentials are no longer present when a new auth is needed.
        p.app.__dict__.pop('_screen_admin_credentials', None)
        return result
    monkeypatch.setattr(p.app, '_prepare_package_pin_action', prepare)
    args = {'target': p.target, 'admin_id': 'review-admin', 'pin': secrets.token_hex(3)}
    assert p.processor.command('recovery_retry_pin', args)['ok'] is True
    p.root.run_until(lambda: p.processor._admin_result is not None)
    assert p.processor._admin_result['ok'] is False
    assert '인증' in p.processor._admin_result['text']
    assert p.app._admin_pin_store.get(p.operation)['state'] == 'SUPERSEDED'
    assert p.server.verify_calls == p.server.redeem_calls == 0
    p.native.assert_not_called()


@pytest.mark.parametrize('failure', ['generation', 'status'])
def test_real_recovery_error_strings_are_failures(recovery, monkeypatch, failure):
    p = recovery
    if failure == 'generation':
        monkeypatch.setattr(p.app, '_package_pin_client', mock.Mock(side_effect=DataGenerationError('generation rejected')))
    else:
        monkeypatch.setattr(p.app._admin_pin_client, 'status', mock.Mock(side_effect=AdminPinError('VERIFY_UNAVAILABLE')))
    args = {'target': p.target, 'admin_id': 'review-admin', 'pin': secrets.token_hex(3)}
    assert p.processor.command('recovery_retry_pin', args)['ok'] is True
    p.root.run_until(lambda: p.processor._admin_result is not None)
    assert p.processor._admin_result['ok'] is False
    assert p.processor._admin_result['text']
    p.native.assert_not_called()


def test_confirmed_durable_recovery_message_is_success(recovery, monkeypatch):
    p = recovery
    monkeypatch.setattr(p.app, '_package_pin_effect', lambda _pending: True)
    assert p.processor.command('recovery_retry_pin', {'target': p.target, 'admin_id': 'review-admin', 'pin': secrets.token_hex(3)})['ok'] is True
    p.root.run_until(lambda: p.processor._admin_result is not None)
    assert p.app._admin_pin_store.get(p.operation)['state'] == 'APPLIED'
    assert p.processor._admin_outcome == 'success'
    assert p.processor._admin_result['ok'] is True
    assert p.processor._admin_result['text']
    p.native.assert_not_called()


def test_uninstall_restores_the_legacy_native_prompt(recovery):
    p = recovery
    p.processor.uninstall()
    with pytest.raises(AssertionError, match='native prompt created'):
        p.app._prompt_package_admin_pin()
    p.native.assert_called_once_with(p.app)


@pytest.mark.parametrize('value,expected', [(True, 'success'), (False, 'failure'), (None, 'unknown'), ({}, 'unknown'), ('lookup failed', 'failure')])
def test_recovery_completion_distinguishes_success_failure_and_unknown(value, expected):
    app = FakeApp()
    app._package_recovery_candidates = lambda: [{'set_id': 'target', 'pin_intent': {'action': 'LABEL.RECHECK'}}]
    app._retry_package_pin_item = lambda _item, **kw: kw['finish'](value) or True
    processor = LabelMatchScreenProcessor(app, module=lm)
    try:
        assert processor.command('recovery_retry_pin', {'target': 'target', 'admin_id': 'review-admin', 'pin': secrets.token_hex(3)})['ok'] is True
        assert processor._admin_outcome == expected
        assert processor._admin_result['ok'] is (expected == 'success')
    finally:
        processor.uninstall()

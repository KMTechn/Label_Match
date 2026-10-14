"""Headless regressions for the shell exit and processing adapter contracts."""
from contextlib import nullcontext
import queue
import runpy
import sys
import threading
import types
import weakref
from pathlib import Path
from unittest import mock

import pytest

import Label_Match as lm
import label_match_screen as screen
import writer_session_fence as fence
from label_match_single_instance import SingleInstanceError, prepare_user_directory, resolve_data_scope
from label_screen_bridge import LabelMatchScreenProcessor
from tests.test_label_screen_bridge import FakeApp
from tk_serial_ui_lane import LaneState, TkSerialUiLane


@pytest.fixture(autouse=True)
def fresh_exit_state(monkeypatch):
    monkeypatch.setattr(fence, '_WRITER_EXIT_CLOSED', False)
    monkeypatch.setattr(fence, '_WRITER_EXIT_COUNT', 0)
    monkeypatch.setattr(fence, '_LANE_WORKERS', weakref.WeakSet())
    monkeypatch.setattr(lm, '_label_match_startup_trace', lambda *_a, **_kw: None)


class Root:
    def after(self, *_args):
        return 'pump'

    def after_cancel(self, *_args):
        pass


def test_exit_waits_for_actual_broken_lane_and_refuses_late_worker():
    lane = TkSerialUiLane(Root())
    try:
        lane._break_lane(RuntimeError('runner unavailable'))
        assert lane.state is LaneState.BROKEN and lane.worker_thread.is_alive()
        assert fence.close_and_wait_writers(0) is False
        late = threading.Thread(target=lambda: None)
        with pytest.raises(fence.WriterFencedError, match='closed'):
            fence.start_lane_worker(late)
        assert not late.is_alive()
        lane.close_idle()
        assert fence.close_and_wait_writers(0.2) is True
    finally:
        lane._signal_worker_stop()
        lane.worker_thread.join(2)
        assert not lane.worker_thread.is_alive()


def test_exit_waits_for_normal_writer_and_never_admits_new_write(monkeypatch):
    entered, release = threading.Event(), threading.Event()
    monkeypatch.setattr(fence, '_admitted_writer_sources', lambda *_a, **_kw: None)
    import auto_update
    monkeypatch.setattr(auto_update, 'assert_version_writer', lambda *_a, **_kw: None)
    errors = []

    def write():
        try:
            with fence.writer_admission('gui_settings_save'):
                entered.set()
                assert release.wait(5)
        except BaseException as error:
            errors.append(error)

    worker = threading.Thread(target=write)
    worker.start()
    try:
        assert entered.wait(2)
        assert fence.close_and_wait_writers(0) is False
        with pytest.raises(fence.WriterFencedError) as refused:
            with fence.writer_admission('gui_settings_save'):
                pytest.fail('a new writer was admitted')
        assert refused.value.code == 'WRITER_EXIT_CLOSED'
    finally:
        release.set()
        worker.join(2)
    assert not errors and not worker.is_alive()
    assert fence.close_and_wait_writers(0.2) is True


@pytest.mark.parametrize('kind', ['delegation', 'maintenance'])
def test_exit_with_valid_deployment_capability_requires_workers_only(monkeypatch, kind):
    monkeypatch.setattr(fence, '_WRITER_EXIT_COUNT', 1)
    monkeypatch.setattr(fence, 'active_fence', lambda *_a, **_kw: {'delegated_sources': [kind]})
    monkeypatch.setattr(fence, '_delegation_matches', lambda *_a, **_kw: True)
    monkeypatch.setattr(fence, '_acquire_named_mutex', lambda *_a: pytest.fail('normal token queried'))
    assert fence.close_and_wait_writers(0) is True


def test_lane_constructor_failure_stops_the_real_worker():
    class FailedRoot(Root):
        def after(self, *_args):
            raise RuntimeError('scheduler failed')
    with pytest.raises(RuntimeError, match='scheduler failed'):
        TkSerialUiLane(FailedRoot(), worker_name='lm-assembly-constructor-failure')
    assert not any(t.name == 'lm-assembly-constructor-failure' and t.is_alive() for t in threading.enumerate())


@pytest.mark.parametrize('stage', ['processor', 'attach', 'schedule'])
def test_owner_initialization_failure_stops_its_real_lane(monkeypatch, stage):
    import label_screen_bridge
    app = types.SimpleNamespace()
    app.ui_lane = TkSerialUiLane(Root())
    app._finish_after_mainloop = app.ui_lane.close_idle
    app.after = mock.Mock(side_effect=RuntimeError('scheduler failed') if stage == 'schedule' else None)
    app.mainloop = lambda: None
    module = types.SimpleNamespace(LABEL_MATCH_SCREEN_HOST={'active': False, 'run': None})
    module._run_label_match_application = lambda: module.LABEL_MATCH_SCREEN_HOST['run'](app)
    monkeypatch.setattr(label_screen_bridge, 'LabelMatchScreenProcessor',
                        mock.Mock(side_effect=RuntimeError('processor failed') if stage == 'processor' else None))
    bridge = mock.Mock()
    if stage == 'attach':
        bridge.attach_owner.side_effect = RuntimeError('attach failed')
    if stage == 'schedule':
        with pytest.raises(RuntimeError, match='scheduler failed'):
            screen.owner_main_for(module)(bridge)
    else:
        screen.owner_main_for(module)(bridge)
    assert not app.ui_lane.worker_thread.is_alive()
    bridge.request_exit.assert_called_once()


def test_machine_folders_are_only_used_after_installation_prepares_them(monkeypatch, tmp_path):
    machine = tmp_path / 'machine'
    monkeypatch.setenv('ProgramData', str(machine))
    monkeypatch.setenv('PROGRAMDATA', str(machine))
    leaf = machine / 'KMTech' / 'Label_Match' / 'data'
    with pytest.raises(SingleInstanceError):
        prepare_user_directory(leaf)
    assert not machine.exists()
    leaf.mkdir(parents=True)
    prepare_user_directory(leaf)
    assert leaf.is_dir()
    local = tmp_path / 'local' / 'KMTech' / 'data'
    prepare_user_directory(local)
    assert local.is_dir()


def test_missing_machine_default_uses_user_storage_without_creating_machine_root(tmp_path):
    machine, local = tmp_path / 'machine', tmp_path / 'local'
    assert resolve_data_scope(environment={'PROGRAMDATA': str(machine), 'LOCALAPPDATA': str(local)}) == str(local / 'KMTech' / 'Label_Match' / 'data')
    assert not machine.exists() and not local.exists()
    with pytest.raises(SingleInstanceError):
        resolve_data_scope(environment={'PROGRAMDATA': str(machine)})
    assert not machine.exists()


def test_direct_sync_default_and_json_refuse_unprepared_machine_folder(monkeypatch, tmp_path):
    monkeypatch.setenv('LOCALAPPDATA', str(tmp_path / 'local'))
    monkeypatch.setenv('ProgramData', str(tmp_path / 'machine'))
    monkeypatch.setenv('PROGRAMDATA', str(tmp_path / 'machine'))
    monkeypatch.delenv(lm.LABEL_MATCH_DIRECT_SYNC_ROOT_ENV, raising=False)
    monkeypatch.delenv(lm.LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT_ENV, raising=False)
    monkeypatch.setattr(lm, '_label_match_direct_sync_source_host_id', lambda: 'test-host')
    context = lm._label_match_direct_sync_context(str(tmp_path / 'scan'))
    assert context['program_data_root'] == str(tmp_path / 'local' / 'KMTech' / 'DirectSync' / 'label_match')
    destination = tmp_path / 'machine' / 'KMTech' / 'DirectSync' / 'label_match' / 'status' / 'result.json'
    with pytest.raises(SingleInstanceError):
        lm._label_match_write_json(str(destination), {}, raise_on_error=True)
    assert not (tmp_path / 'machine').exists()


def test_start_retry_reuses_initial_load_and_reports_catalog_failure(monkeypatch):
    app = FakeApp()
    app._startup_catalog_error = RuntimeError('catalog unavailable')
    calls = []
    app._retry_initial_load = lambda: calls.append('initial_load') or app.__dict__.pop('_startup_catalog_error', None) is not None
    processor = LabelMatchScreenProcessor(app, module=lm)
    try:
        assert processor.state()['start']['phase'] == 'catalog_failed'
        assert processor.state()['start']['retry'] is True
        assert processor.command('start_retry', {})['ok'] is True
        assert calls == ['initial_load']
        assert processor.state()['start'] is None
    finally:
        processor.uninstall()


def test_recovery_commands_use_current_candidates_and_existing_processing_methods():
    app = FakeApp()
    item = {'set_id': 'set-a', 'pin_intent': {'action': 'LABEL.RECHECK'}}
    app._package_recovery_candidates = lambda: [item]
    app._ui_lane_is_busy = lambda: False
    app._park_unverified_pin_item = mock.Mock(return_value=True)
    app._retry_package_pin_item = mock.Mock(return_value=True)
    processor = LabelMatchScreenProcessor(app, module=lm)
    try:
        assert processor.command('recovery_park', {'target': 'missing'})['ok'] is False
        app._park_unverified_pin_item.assert_not_called()
        assert processor.command('recovery_park', {'target': 'set-a'})['ok'] is True
        app._park_unverified_pin_item.assert_called_once_with('set-a')
        assert processor.command('recovery_retry_pin', {'target': 'set-a'})['ok'] is True
        assert app._retry_package_pin_item.call_args.args == (item,)
        app._retry_package_pin_item.call_args.kwargs['finish']('confirmed')
        assert processor.state()['admin']['result']['ok'] is True
    finally:
        processor.uninstall()


@pytest.mark.parametrize('code', ['WRITER_GATE_ABANDONED', 'ACTIVE_WRITER_FENCE'])
def test_only_abandoned_writer_has_the_reopen_guidance(monkeypatch, capsys, code):
    notices = []
    monkeypatch.setattr(lm, '_show_first_run_onboarding_error', notices.append)
    lm._show_installation_recovery_required(fence.WriterFencedError(code, 'denied'))
    expected = '포장 다시 열기 — 한 번 더 여세요' if code == 'WRITER_GATE_ABANDONED' else lm.INSTALLATION_RECOVERY_REQUIRED_MESSAGE
    assert str(notices[0]) == expected
    assert capsys.readouterr().err.strip() == expected


def test_real_portable_entry_and_bootstrap_admission_show_abandoned_guidance(monkeypatch, tmp_path, capsys):
    import auto_update as updates
    import startup_observation
    control = tmp_path / 'update-control'
    control.mkdir()
    notices = []
    monkeypatch.setattr(lm, '_show_first_run_onboarding_error', notices.append)
    monkeypatch.setattr(updates, 'control_root', lambda *_a: control)
    monkeypatch.setattr(updates, 'load_policy', lambda *_a: {'operator_sid': 'test-worker'})
    monkeypatch.setattr(updates, 'ordinary_user_context', lambda *_a: True)
    monkeypatch.setattr(updates, 'update_lock', lambda *_a, **_kw: nullcontext())
    monkeypatch.setattr(updates, 'optional_record', lambda *_a: {'phase': 'CONFIRMED'})
    monkeypatch.setattr(updates, 'read_json', lambda *_a: {'sha': 'a' * 40})
    monkeypatch.setattr(updates, 'version_manifest', lambda *_a: {'source_commit': 'b' * 40})
    monkeypatch.setattr(updates, 'prepared_pending', lambda *_a: False)
    monkeypatch.setattr(startup_observation, 'observe_startup', lambda *_a, **_kw: None)
    class Abandoned:
        abandoned = True
        def release(self):
            pass
    monkeypatch.setattr(fence, '_acquire_named_mutex', lambda *_a: Abandoned())
    monkeypatch.setattr(updates, 'start_ordinary_process', lambda *_a, **_kw: pytest.fail('bootstrap was launched'))
    monkeypatch.setattr(sys, 'argv', ['main.py', '--ui', 'web'])
    entry = runpy.run_path(str(Path(lm.__file__).parent / 'portable' / 'main.py'))
    assert entry['main']() == 1
    assert str(notices[0]) == '포장 다시 열기 — 한 번 더 여세요'
    assert capsys.readouterr().err.strip() == '포장 다시 열기 — 한 번 더 여세요'

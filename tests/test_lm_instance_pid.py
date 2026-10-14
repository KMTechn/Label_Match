"""PID ownership across Tk/web/preparation windows; only Win32 boundary doubles."""
import ctypes
import json
import os
from pathlib import Path
import runpy
import sys
import time
from unittest import mock

import pytest

import label_match_instance_win32 as native
import label_match_single_instance as instance
import writer_session_fence as fence


class Windows:
    def __init__(self, *, visible=True, foreground=True, minimized=True, pid=1234):
        self.visible, self.foreground, self.minimized, self.pid = visible, foreground, minimized, pid
        self.restored, self.foregrounds, self.flashes = [], [], []

    def EnumWindows(self, callback, _parameter):
        # A visible unrelated application must not receive activation.
        for hwnd in (7, 8):
            if not callback(hwnd, 0):
                break
        return True

    def IsWindowVisible(self, hwnd):
        return self.visible if hwnd == 8 else True

    def GetWindowThreadProcessId(self, hwnd, result):
        result._obj.value = self.pid if hwnd == 8 else self.pid + 1
        return 1

    def IsIconic(self, _hwnd):
        return self.minimized

    def ShowWindow(self, hwnd, command):
        self.restored.append((hwnd, command))
        return True

    def SetForegroundWindow(self, hwnd):
        self.foregrounds.append(hwnd)
        return self.foreground

    def FlashWindowEx(self, info):
        self.flashes.append(info._obj.hwnd)
        return True


@pytest.mark.parametrize('window_kind', ['tk', 'web', 'preparation', 'app_notice'])
def test_owner_pid_window_is_activated_without_title_lookup(window_kind):
    windows = Windows()
    # The double deliberately provides no GetWindowText functions.
    result = native.activate_pid(windows.pid, user32=windows)
    assert result == native.ActivationResult(True, True, False)
    assert windows.restored == [(8, 9)] and windows.foregrounds == [8]


def test_foreground_rejection_flashes_only_the_owner_window():
    windows = Windows(foreground=False)
    result = native.activate_pid(windows.pid, user32=windows)
    assert result == native.ActivationResult(True, False, True)
    assert windows.flashes == [8]


@pytest.mark.parametrize('closing', [False, True])
def test_no_window_notice_has_final_copy_one_default_button_and_five_second_timeout(closing):
    user = mock.Mock()
    native.timed_notice(closing=closing, user32=user)
    parent, text, title, flags, language, timeout = user.MessageBoxTimeoutW.call_args.args
    assert parent is None and title == '포장'
    assert text == ('프로그램을 닫는 중입니다 · 잠시 뒤 다시 누르세요' if closing else '프로그램이 켜지는 중입니다 · 잠시 기다려 주세요')
    assert flags & 0xF == 0 and flags & 0x300 == 0  # MB_OK, first button default
    assert timeout == 5000 and language == 0


@pytest.fixture
def ownership(tmp_path, monkeypatch):
    scope = tmp_path/'data'
    monkeypatch.setenv('LOCALAPPDATA', str(tmp_path/'local'))
    monkeypatch.setattr(instance._active_instance_lease, 'lease', None, raising=False)
    record = {'pid': 1234, 'executable': sys.executable, 'closing': False}
    monkeypatch.setattr(native, 'read_notice', lambda _path: json.dumps(record).encode())
    process = mock.Mock(return_value=True)
    activation = mock.Mock(return_value=native.ActivationResult(True, True))
    notice = mock.Mock()
    monkeypatch.setattr(native, 'process_matches', process)
    monkeypatch.setattr(native, 'activate_pid', activation)
    monkeypatch.setattr(native, 'timed_notice', notice)
    yield scope, record, process, activation, notice


@pytest.mark.parametrize('window_kind', ['tk', 'web', 'preparation', 'app_notice'])
def test_duplicate_returns_zero_and_never_constructs_a_second_app(ownership, window_kind):
    scope, record, process, activation, notice = ownership
    start = mock.Mock()
    assert instance.run_guarded_entrypoint(start, data_scope=scope,
                                         acquire=lambda _scope: instance.MutexLease(False,'duplicate')) == 0
    start.assert_not_called()
    activation.assert_called_once_with(record['pid'])
    process.assert_called_once_with(record['pid'], record['executable'])
    notice.assert_not_called()


@pytest.mark.parametrize('closing', [False, True])
def test_duplicate_without_window_has_bounded_notice_and_zero_exit(ownership, closing):
    scope, record, process, activation, notice = ownership
    record['closing'] = closing
    activation.return_value = native.ActivationResult(False)
    start = mock.Mock()
    before = time.monotonic()
    assert instance.run_guarded_entrypoint(start, data_scope=scope,
                                         acquire=lambda _scope: instance.MutexLease(False,'duplicate')) == 0
    assert time.monotonic() - before < 1
    start.assert_not_called()
    notice.assert_called_once_with(closing=closing)


@pytest.mark.parametrize('stale', ['dead_pid', 'different_executable', 'relative_executable', 'bad_pid'])
def test_stale_owner_notice_is_never_used_for_activation(ownership, stale):
    scope, record, process, activation, notice = ownership
    if stale == 'dead_pid':
        process.return_value = False
    elif stale == 'different_executable':
        record['executable'] = str(Path(sys.executable).with_name('another.exe'))
    elif stale == 'relative_executable':
        record['executable'] = Path(sys.executable).name
    else:
        record['pid'] = True
    assert instance.run_guarded_entrypoint(lambda: pytest.fail('second app created'), data_scope=scope,
                                         acquire=lambda _scope: instance.MutexLease(False,'duplicate')) == 0
    activation.assert_not_called()
    notice.assert_called_once_with(closing=False)


@pytest.mark.parametrize('owner_thread', ['main', 'web_owner'])
def test_actual_owner_notice_is_user_scoped_and_removed_after_exit_admission_closes(tmp_path, monkeypatch, owner_thread):
    monkeypatch.setenv('LOCALAPPDATA', str(tmp_path/'local'))
    monkeypatch.setattr(instance._active_instance_lease, 'lease', None, raising=False)
    monkeypatch.setattr(fence, '_WRITER_EXIT_CLOSED', False)
    scope = tmp_path/'data'
    marker = instance.instance_notice_path(scope)
    original_write = instance._write_instance_owner_notice
    write_errors = []
    def write(*args, **kwargs):
        try:
            return original_write(*args, **kwargs)
        except Exception as error:
            write_errors.append((type(error).__name__, getattr(error, 'winerror', None)))
            raise
    monkeypatch.setattr(instance, '_write_instance_owner_notice', write)
    def start():
        record = json.loads(native.read_notice(marker))
        assert record['pid'] == os.getpid() and record['executable'] == sys.executable
        assert record['closing'] is False
        assert instance._active_instance_lease.lease.owner is True
        assert not getattr(instance._active_instance_lease.lease, '_closing_notice', False)
        if owner_thread == 'web_owner':
            import threading
            worker = threading.Thread(target=instance.mark_current_instance_closing)
            worker.start()
            worker.join(2)
            assert not worker.is_alive()
        else:
            instance.mark_current_instance_closing()
        assert write_errors == []
        assert getattr(instance._active_instance_lease.lease, '_closing_notice', False) is True
        assert json.loads(native.read_notice(marker))['closing'] is True
        monkeypatch.setattr(fence, '_WRITER_EXIT_CLOSED', True)
        return 0
    assert instance.run_guarded_entrypoint(start, data_scope=scope) == 0
    assert not marker.exists()
    assert not scope.exists()  # Metadata did not create the business data root.


def test_early_duplicate_precedes_portable_startup_and_application_import(ownership, monkeypatch):
    scope, record, process, activation, notice = ownership
    monkeypatch.setattr(instance, 'acquire_data_scope_mutex', lambda _scope: instance.MutexLease(False,'duplicate'))
    monkeypatch.setattr(sys, 'argv', ['main.py', '--ui', 'web'])
    entry = runpy.run_path(str(Path(__file__).resolve().parents[1]/'portable/main.py'))
    heavy = mock.Mock(side_effect=AssertionError('startup or application body ran'))
    entry['main'].__globals__['_main_after_instance'] = heavy
    assert entry['main']() == 0
    heavy.assert_not_called()
    notice.assert_not_called()


def test_source_duplicate_precedes_heavy_processing_imports(ownership, monkeypatch):
    import builtins
    scope, record, process, activation, notice = ownership
    monkeypatch.setattr(instance, 'acquire_data_scope_mutex', lambda _scope: instance.MutexLease(False,'duplicate'))
    original = builtins.__import__
    heavy = []
    def importing(name, *args, **kwargs):
        if name in ('carrier_identity_port', 'label_history', 'tkinter'):
            heavy.append(name)
            raise AssertionError('heavy import preceded duplicate refusal')
        return original(name, *args, **kwargs)
    monkeypatch.setattr(builtins, '__import__', importing)
    monkeypatch.setattr(sys, 'argv', ['Label_Match.py', '--ui', 'web'])
    with pytest.raises(SystemExit) as stopped:
        runpy.run_path(str(Path(__file__).resolve().parents[1]/'Label_Match.py'), run_name='__main__')
    assert stopped.value.code == 0 and heavy == []
    notice.assert_not_called()


def test_instance_metadata_refuses_even_an_existing_machine_root(tmp_path, monkeypatch):
    machine = tmp_path/'machine'
    machine.mkdir()
    monkeypatch.setenv('ProgramData', str(machine))
    monkeypatch.setenv('PROGRAMDATA', str(machine))
    monkeypatch.setenv('LOCALAPPDATA', str(machine))
    with pytest.raises(instance.SingleInstanceError):
        instance.instance_notice_path(tmp_path/'business')
    assert list(machine.iterdir()) == []


def test_early_owner_reuses_one_lease_in_the_later_application_guard(tmp_path, monkeypatch):
    monkeypatch.setenv('LOCALAPPDATA', str(tmp_path/'local'))
    monkeypatch.setenv('LABEL_MATCH_SAVE_DIR', str(tmp_path/'data'))
    monkeypatch.setattr(instance._active_instance_lease, 'lease', None, raising=False)
    lease = instance.begin_early_gui_entry(['--ui','web'], app_root=tmp_path)
    acquire = mock.Mock(side_effect=AssertionError('early lease reacquired'))
    try:
        assert lease.owner is True
        assert instance.run_guarded_entrypoint(lambda: 3, data_scope=tmp_path/'data', acquire=acquire) == 3
        acquire.assert_not_called()
        assert json.loads(native.read_notice(instance.instance_notice_path(tmp_path/'data')))['pid'] == os.getpid()
    finally:
        instance.end_early_gui_entry(lease)
    assert not instance.instance_notice_path(tmp_path/'data').exists()


@pytest.mark.parametrize('mode', ['--label-match-direct-sync-relay', '--label-match-user-relay', '--onboard-current-user', '--remove-current-user-setup', '--kmtech-lm-update-bootstrap'])
def test_operational_children_keep_their_existing_non_gui_entry(mode, monkeypatch, tmp_path):
    acquire = mock.Mock(side_effect=AssertionError('GUI lease taken by operational child'))
    monkeypatch.setattr(instance, 'acquire_data_scope_mutex', acquire)
    body = mock.Mock(return_value=19)
    assert instance.run_early_gui_entry(body, arguments=[mode], app_root=tmp_path) == 19
    body.assert_called_once_with()
    acquire.assert_not_called()

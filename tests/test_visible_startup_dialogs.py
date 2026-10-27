"""Standalone startup dialogs use fake Tk only; workers never enter Tk."""

import queue
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import Label_Match as lm
import auto_update
import startup_observation


@pytest.fixture
def visible_owner(monkeypatch):
    owners, dialogs, tk_threads, answers = [], [], [], []

    class Owner:
        def __init__(self):
            self.visible = False
            self.destroyed = False
            self.caption = None
            self.events = []
            owners.append(self)
            tk_threads.append(threading.current_thread().name)

        def title(self, value):
            self.caption = value

        def deiconify(self):
            self.visible = True
            self.events.append('deiconify')

        def withdraw(self):
            self.visible = False

        def lift(self):
            self.events.append('lift')

        def attributes(self, *args):
            self.events.append(args)

        def focus_force(self):
            self.events.append('focus_force')

        def after_idle(self, callback):
            self.release_topmost = callback

        def destroy(self):
            self.destroyed = True

        def __getattr__(self, name):
            return Mock()

    def dialog(title, message, **options):
        tk_threads.append(threading.current_thread().name)
        owner = options['parent']
        dialogs.append((title, message, options))
        assert owner.visible, 'the dialog must have a visible owner'
        assert owner.caption == '포장 시작 안내'
        assert owner.events[:4] == ['deiconify', 'lift', ('-topmost', True), 'focus_force']
        assert options['default'] in ('retry', 'ok')
        # Model Enter on the requested native default button.
        if hasattr(owner, 'release_topmost'):
            owner.release_topmost()
            assert owner.events[-1] == ('-topmost', False)
        if answers:
            answer = answers.pop(0)
            if isinstance(answer, Exception):
                raise answer
            return answer
        return True if options['default'] == 'retry' else 'ok'

    monkeypatch.setattr(lm.tk, 'Tk', Owner)
    monkeypatch.setattr(lm.tk, 'Label', lambda *args, **kwargs: SimpleNamespace(pack=Mock()))
    monkeypatch.setattr(lm, '_startup_app_stop_requested', lambda: False)
    monkeypatch.setattr(lm, '_label_match_startup_trace', Mock())
    monkeypatch.setattr(lm, '_watch_startup_notice_stop', lambda parent: {'stopped': False})
    monkeypatch.setattr(startup_observation, 'watch_startup_notice_stop',
                        lambda parent: {'stopped': False})
    for name in ('askretrycancel', 'showerror', 'showwarning', 'showinfo'):
        monkeypatch.setattr(lm.messagebox, name, dialog)
    return SimpleNamespace(owners=owners, dialogs=dialogs, tk_threads=tk_threads, answers=answers)


def test_catalog_retry_enter_and_owner_cleanup(monkeypatch, visible_owner):
    monkeypatch.setitem(lm.LABEL_MATCH_SCREEN_HOST, 'active', True)
    error = lm.ItemCatalogSyncError('offline', cause_code='REQUEST_FAILED_NO_CACHE')
    assert lm._offer_item_catalog_startup_retry(error) is True
    assert len(visible_owner.dialogs) == 1
    assert visible_owner.dialogs[0][2]['default'] == 'retry'
    assert visible_owner.owners[0].destroyed


def test_update_mutex_notice_enter_and_owner_cleanup(monkeypatch, visible_owner):
    monkeypatch.setitem(lm.LABEL_MATCH_SCREEN_HOST, 'active', True)
    auto_update._show_update_mutex_recovery_notice()
    assert len(visible_owner.dialogs) == 1
    assert visible_owner.dialogs[0][1] == auto_update.UPDATE_MUTEX_RECOVERY_MESSAGE
    assert visible_owner.dialogs[0][2]['default'] == 'ok'
    assert visible_owner.owners[0].destroyed


@pytest.mark.parametrize('answer', [False, RuntimeError('dialog unavailable')])
def test_retry_cancel_or_dialog_failure_cleans_owner(visible_owner, answer):
    visible_owner.answers.append(answer)
    error = lm.ItemCatalogSyncError('offline', cause_code='REQUEST_FAILED_NO_CACHE')
    assert lm._offer_item_catalog_startup_retry(error) is False
    assert visible_owner.owners[0].destroyed
    if answer is False:
        assert error._catalog_retry_dialog_shown is True


def test_retry_official_stop_overrides_enter(monkeypatch, visible_owner):
    monkeypatch.setattr(lm, '_watch_startup_notice_stop', lambda parent: {'stopped': True})
    error = lm.ItemCatalogSyncError('offline', cause_code='REQUEST_FAILED_NO_CACHE')
    assert lm._offer_item_catalog_startup_retry(error) is False
    assert visible_owner.owners[0].destroyed
    assert not getattr(error, '_catalog_retry_dialog_shown', False)


@pytest.mark.parametrize('web', [False, True])
@pytest.mark.parametrize('error', [FileNotFoundError('missing'), OSError('unreadable')])
def test_item_load_error_stays_on_owner_thread(monkeypatch, visible_owner, web, error):
    from label_screen_bridge import _ScreenDialogs

    app = lm.Label_Match.__new__(lm.Label_Match)
    app.run_tests = False
    app.initial_load_queue = queue.Queue()
    scheduled, bands, worker_errors = [], [], []
    app.after = lambda delay, callback: scheduled.append((threading.current_thread().name, callback))
    monkeypatch.setitem(lm.LABEL_MATCH_SCREEN_HOST, 'active', web)
    monkeypatch.setattr(lm, 'get_verified_catalog_snapshot', lambda path: None)
    monkeypatch.setattr(lm, 'requires_verified_catalog_snapshot', lambda path: False)
    monkeypatch.setattr(lm, 'open', Mock(side_effect=error), raising=False)
    monkeypatch.setattr(lm, '_label_match_startup_trace', lambda *args, **kwargs: None)
    if web:
        processor = SimpleNamespace(_dialog=lambda *args: bands.append(
            (threading.current_thread().name, args)))
        monkeypatch.setattr(lm, 'messagebox', _ScreenDialogs(processor))

    def load():
        try:
            assert app._load_items_data() == {}
        except BaseException as exc:
            worker_errors.append(exc)

    worker = threading.Thread(target=load, name='label-match-initial-load')
    worker.start()
    worker.join()
    assert not worker_errors
    assert visible_owner.tk_threads == []
    assert bands == []
    assert scheduled == []
    assert not app.initial_load_queue.empty()
    app._process_initial_load_queue()
    assert len(scheduled) == 1
    assert scheduled[0][0] == threading.main_thread().name
    if web:
        assert len(bands) == 1
        assert bands[0][0] == threading.main_thread().name
        assert visible_owner.owners == []
    else:
        assert len(visible_owner.dialogs) == 1
        assert visible_owner.owners[0].destroyed
        assert all(name == threading.main_thread().name for name in visible_owner.tk_threads)


def test_item_load_notice_is_suppressed_after_shutdown(monkeypatch, visible_owner):
    app = lm.Label_Match.__new__(lm.Label_Match)
    app.initial_load_queue = queue.Queue()
    app.initial_load_queue.put({'item_load_notice': ('error', '기준 정보 로드 오류', '안내')})
    app._tk_shutdown_requested = True
    app._process_initial_load_queue()
    assert visible_owner.owners == []
    assert visible_owner.dialogs == []


def test_shutdown_during_item_notice_does_not_schedule_tk_again(monkeypatch, visible_owner):
    app = lm.Label_Match.__new__(lm.Label_Match)
    app.initial_load_queue = queue.Queue()
    app.initial_load_queue.put({'item_load_notice': ('error', '기준 정보 로드 오류', '안내')})
    app.after = Mock(side_effect=AssertionError('shutdown root must not schedule Tk'))
    def close_app(*args):
        app._tk_shutdown_requested = True
    app._notify_item_load_error = close_app
    app._process_initial_load_queue()
    app.after.assert_not_called()


def test_mutex_tk_failure_keeps_stderr_path_without_native_fallback(monkeypatch):
    monkeypatch.setattr(lm.tk, 'Tk', Mock(side_effect=lm.tk.TclError('unavailable')))
    native = Mock(side_effect=AssertionError('native fallback is outside this change'))
    monkeypatch.setattr(startup_observation, 'show_bounded_native_notice', native)
    auto_update._show_update_mutex_recovery_notice()
    native.assert_not_called()

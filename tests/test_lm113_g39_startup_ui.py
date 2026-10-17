"""G39 startup update mutex / non-serviced UI reproduction."""
import ctypes
import queue
import threading
import time
from types import SimpleNamespace

import auto_update as updates
from tests.test_window_first_startup import steady_start, reset_window_start
from tests.test_onboarding_lifecycle_scope import managed_onboarding
from tests.test_current_user_onboarding import isolated_data_generation_release

REAL_UPDATE_LOCK = updates.update_lock


def test_g39_startup_gate_releases_update_mutex_without_servicing_ui(steady_start, monkeypatch):
    import tkinter

    held, waits, released, ui_callbacks = [], [], [], []

    def acquire(_handle, milliseconds):
        waits.append(milliseconds)
        held.append(True)
        return 0

    def release(_handle):
        assert held
        held.pop()
        released.append(True)
        return 1

    def native_function(result):
        def call(*_args):
            return result
        return call

    kernel = SimpleNamespace(
        CreateMutexExW=native_function(1), WaitForSingleObject=acquire,
        ReleaseMutex=release, CloseHandle=native_function(1), LocalFree=native_function(0),
    )
    advapi = SimpleNamespace(ConvertStringSecurityDescriptorToSecurityDescriptorW=native_function(1))

    class IdleUI:
        def after(self, _milliseconds, callback, *args):
            ui_callbacks.append((callback, args))
            return "queued-without-service"

        def mainloop(self, *_args, **_kwargs):
            if held:
                raise AssertionError("UI mainloop awaited while update mutex is held")

    ui = IdleUI()
    code_root = steady_start.paths.app_root
    if updates.control_root(code_root) is None:
        code_root = code_root / "app"
    assert updates.control_root(code_root) == steady_start.control
    original_get, original_wait = queue.Queue.get, threading.Event.wait

    def guarded_get(self, *args, **kwargs):
        if held:
            raise AssertionError("UI queue awaited while update mutex is held")
        return original_get(self, *args, **kwargs)

    def guarded_wait(self, *args, **kwargs):
        if held:
            raise AssertionError("UI event awaited while update mutex is held")
        return original_wait(self, *args, **kwargs)

    with monkeypatch.context() as native:
        native.setattr(updates, "update_lock", REAL_UPDATE_LOCK)
        native.setattr(ctypes, "WinDLL", lambda name, **_kwargs: kernel if name == "kernel32" else advapi)
        native.setattr(tkinter, "Tk", lambda *_args, **_kwargs: ui)
        native.setattr(tkinter, "mainloop", ui.mainloop)
        native.setattr(queue.Queue, "get", guarded_get)
        native.setattr(threading.Event, "wait", guarded_wait)
        started = time.monotonic()
        result = updates.startup_gate(code_root, [], role="gui")
        elapsed = time.monotonic() - started

    assert result is None
    assert waits == [5000]  # Existing update_lock native wait bound, unchanged.
    assert released == [True] and not held
    assert elapsed < 5

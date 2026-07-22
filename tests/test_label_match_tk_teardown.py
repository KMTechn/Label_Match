from __future__ import annotations

import gc
import os
import subprocess
import sys
import threading
import time
import types
from pathlib import Path

import pytest

import Label_Match as module


CHILD_GUARD = "LABEL_MATCH_TK_TEARDOWN_CHILD"
ACTIVE_WORKER_CHILD_GUARD = "LABEL_MATCH_ACTIVE_WORKER_TEARDOWN_CHILD"


def _isolated_env(data_root):
    env = os.environ.copy()
    env.update(
        {
            CHILD_GUARD: "1",
            "LABEL_MATCH_SAVE_DIR": os.fspath(data_root),
            "LABEL_MATCH_AUTOMATED_TEST": "1",
            "LABEL_MATCH_AUDIO_ENABLED": "off",
            "LABEL_MATCH_DIRECT_SYNC_BOOTSTRAP": "off",
            "LABEL_MATCH_SESSION_SYNC_TRIGGER": "off",
            "LABEL_MATCH_UPDATE_PROVIDER": "off",
            "KMTECH_TEST_SILENT_AUDIO": "1",
            "SDL_AUDIODRIVER": "dummy",
            "PYGAME_HIDE_SUPPORT_PROMPT": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
        }
    )
    for key in tuple(env):
        if key.startswith("LABEL_MATCH_LOGISTICS_") or key.startswith(
            "WORKER_ANALYSIS_LOGISTICS_"
        ):
            env.pop(key, None)
    return env


@pytest.mark.skipif(os.name != "nt", reason="Windows Tcl/Tk teardown regression")
def test_direct_destroy_joins_writer_and_tk_workers_in_fresh_process(tmp_path):
    if os.environ.get(CHILD_GUARD) != "1":
        node_id = (
            "tests/test_label_match_tk_teardown.py::"
            "test_direct_destroy_joins_writer_and_tk_workers_in_fresh_process"
        )
        result = subprocess.run(
            [sys.executable, "-B", "-m", "pytest", "-q", node_id],
            cwd=os.fspath(Path(__file__).resolve().parents[1]),
            env=_isolated_env(tmp_path / "child-data"),
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        return

    import ctypes
    import tkinter as tk

    ctypes.windll.kernel32.SetErrorMode(0x0001 | 0x0002)

    # Constructor-time close used to leave DataManager's daemon writer alive.
    early_app = module.Label_Match(run_tests=True)
    early_app.withdraw()
    early_manager = early_app.data_manager
    early_app.destroy()
    assert early_app._tk_destroy_complete is True
    assert not early_manager.log_thread.is_alive()
    assert not [
        thread.name
        for thread in early_app._tracked_tk_shutdown_threads()
        if thread.is_alive()
    ]
    del early_app
    gc.collect()

    for _index in range(2):
        app = module.Label_Match(run_tests=True)
        app.withdraw()
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline and not app.initialized_successfully:
            app.update()
            time.sleep(0.01)
        assert app.initialized_successfully
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and app.history_load_pending:
            app.update()
            time.sleep(0.01)
        manager = app.data_manager
        app.destroy()
        app.destroy()  # idempotency is part of the shutdown contract.
        assert app._tk_destroy_complete is True
        assert not manager.log_thread.is_alive()
        assert not [
            thread.name
            for thread in app._tracked_tk_shutdown_threads()
            if thread.is_alive()
        ]
        del app
        gc.collect()

        # Prove the prior root did not leave Tcl's command table poisoned.
        probe = tk.Tk()
        probe.withdraw()
        probe.update_idletasks()
        probe.destroy()
        gc.collect()

    assert not [
        thread.name
        for thread in threading.enumerate()
        if thread.name.startswith("label-match-")
    ]


@pytest.mark.skipif(os.name != "nt", reason="Windows Tcl/Tk teardown regression")
def test_active_audio_siren_and_simulation_workers_never_outlive_tcl(
    tmp_path, monkeypatch
):
    if os.environ.get(ACTIVE_WORKER_CHILD_GUARD) != "1":
        node_id = (
            "tests/test_label_match_tk_teardown.py::"
            "test_active_audio_siren_and_simulation_workers_never_outlive_tcl"
        )
        env = _isolated_env(tmp_path / "active-child-data")
        env[ACTIVE_WORKER_CHILD_GUARD] = "1"
        result = subprocess.run(
            [sys.executable, "-B", "-m", "pytest", "-q", node_id],
            cwd=os.fspath(Path(__file__).resolve().parents[1]),
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        return

    import ctypes
    import tkinter as tk

    ctypes.windll.kernel32.SetErrorMode(0x0001 | 0x0002)
    app = module.Label_Match(run_tests=True)
    app.withdraw()
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and not app.initialized_successfully:
        app.update()
        time.sleep(0.01)
    assert app.initialized_successfully

    main_thread_id = threading.get_ident()
    off_thread_after_calls = []
    real_after = app.after

    def guarded_after(*args, **kwargs):
        if threading.get_ident() != main_thread_id:
            off_thread_after_calls.append(threading.current_thread().name)
            raise AssertionError("Tk.after called from a worker")
        return real_after(*args, **kwargs)

    app.after = guarded_after
    audio_started = threading.Event()
    audio_release = threading.Event()

    class FakeMixer:
        def init(self):
            audio_started.set()
            audio_release.wait(30)

        def quit(self):
            return None

    fake_pygame = types.SimpleNamespace(mixer=FakeMixer())
    monkeypatch.setitem(sys.modules, "pygame", fake_pygame)
    monkeypatch.setattr(module, "_label_match_automated_test_mode", lambda: False)
    monkeypatch.setattr(module, "_label_match_audio_enabled", lambda: True)

    app.run_test_log_simulation(next(iter(app.items_data)), 100000)
    assert app._simulation_thread.is_alive()

    siren_started = threading.Event()

    class FakeSound:
        def play(self, *, loops):
            assert loops == -1
            siren_started.set()

        def stop(self):
            return None

    app.sound_objects["fail"] = FakeSound()
    app.run_tests = False
    app.audio_init_started = False
    app._start_audio_initialization()
    assert audio_started.wait(5)
    siren = app._start_error_siren_thread()
    app.run_tests = True
    assert siren is not None
    assert siren_started.wait(5)

    # The deliberately blocked audio initializer exceeds the join budget.
    # destroy() must keep Tcl alive and schedule a main-thread retry.
    app.destroy()
    assert app._tk_destroy_complete is False
    assert app.winfo_exists()

    audio_release.set()
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline and not app._tk_destroy_complete:
        try:
            app.update()
        except tk.TclError:
            break
        time.sleep(0.01)

    assert app._tk_destroy_complete is True
    assert off_thread_after_calls == []
    assert not [
        thread.name
        for thread in app._tracked_tk_shutdown_threads()
        if thread.is_alive()
    ]


def test_update_worker_never_constructs_tk_or_prompts_off_main_thread(monkeypatch):
    candidate = {
        "url": "https://updates.example.invalid/Label_Match-9.9.9.zip",
        "version": "9.9.9",
        "sha256": "a" * 64,
        "archive": {"top_level": "Label_Match-9.9.9", "required_files": ["Label_Match.exe"]},
        "install": module._default_update_install_policy(),
    }
    prompt_threads = []
    apply_calls = []
    scheduled = []
    app = object.__new__(module.Label_Match)
    app._tk_shutdown_requested = False
    app.after = lambda _delay, callback: scheduled.append(callback) or "after-update"

    monkeypatch.setattr(module, "_check_update_candidate", lambda: candidate)
    monkeypatch.setattr(module, "_can_apply_updates", lambda: True)
    monkeypatch.setattr(
        module.tk,
        "Tk",
        lambda: (_ for _ in ()).throw(AssertionError("worker must not construct a Tk root")),
    )
    monkeypatch.setattr(
        module.messagebox,
        "askyesno",
        lambda *args, **kwargs: prompt_threads.append(threading.get_ident()) or True,
    )
    monkeypatch.setattr(
        module,
        "download_and_apply_update",
        lambda *args, **kwargs: apply_calls.append((args, kwargs)),
    )

    worker = module.Label_Match._start_update_check(app)
    worker.join(timeout=5)
    assert not worker.is_alive()
    assert prompt_threads == []
    assert len(scheduled) == 1

    scheduled.pop()()

    assert prompt_threads == [threading.main_thread().ident]
    assert apply_calls == [
        (
            (candidate["url"],),
            {
                    "expected_sha256": candidate["sha256"],
                    "archive_policy": candidate["archive"],
                    "install_policy": candidate["install"],
                },
        )
    ]


def test_legacy_update_worker_refuses_background_tk_root(monkeypatch):
    candidate = {
        "url": "https://updates.example.invalid/Label_Match-9.9.9.zip",
        "version": "9.9.9",
        "sha256": "b" * 64,
    }
    prompts = []
    monkeypatch.setattr(module, "_check_update_candidate", lambda: candidate)
    monkeypatch.setattr(module, "_can_apply_updates", lambda: True)
    monkeypatch.setattr(
        module.tk,
        "Tk",
        lambda: (_ for _ in ()).throw(AssertionError("background Tk root forbidden")),
    )
    monkeypatch.setattr(
        module.messagebox,
        "askyesno",
        lambda *args, **kwargs: prompts.append((args, kwargs)) or True,
    )

    worker = threading.Thread(target=module.threaded_update_check)
    worker.start()
    worker.join(timeout=5)

    assert not worker.is_alive()
    assert prompts == []

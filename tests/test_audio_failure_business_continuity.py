"""Audio is optional feedback; device failures must preserve business work."""
import queue
import sys
import threading
from types import MethodType, SimpleNamespace

import Label_Match as lm
from tests.test_legacy_label_transition import MASTER
from tests.test_w9scanonly_packaging import completions, packaging, products, scan  # noqa: F401


class FailedAudioDevice:
    SND_FILENAME, SND_ASYNC, SND_NODEFAULT, SND_LOOP = 1, 2, 4, 8

    def __init__(self):
        self.calls = []

    def PlaySound(self, path, flags):
        self.calls.append((path, flags))
        raise RuntimeError("fake audio device failure")


def failed_sound(device):
    sound = object.__new__(lm._WindowsWaveSound)
    sound.path = "fake-wave.wav"
    sound.duration_ms = 40
    sound._winsound = device
    return sound


def test_failed_sound_stop_cannot_interrupt_modal_error_recovery():
    app = object.__new__(lm.Label_Match)
    device = FailedAudioDevice()
    logged, resets = [], []
    app.run_tests, app.is_blinking = True, False
    app.sound_objects = {"fail": failed_sound(device)}
    app.data_manager = SimpleNamespace(log_event=lambda *args: logged.append(args))
    app.after = lambda *args: None
    app._blink_background_loop = lambda: None
    app._reset_current_set = lambda **kwargs: resets.append(kwargs)

    def failed_window_update():
        # Fail before creating any Tk interpreter or window.
        raise RuntimeError("fake window update failure")

    app.update_idletasks = failed_window_update
    app._trigger_modal_error("Input error", "Restart this tray", "FAIL", "rejected input")
    assert len(device.calls) == 1
    assert len(logged) == 1 and logged[0][0] == app.Events.UI_ERROR
    assert resets == [{"full_reset": True}]
    assert not app.is_blinking


def test_failed_playback_preserves_completion_log_sync_and_next_scan(packaging, monkeypatch):
    app, dialogs = packaging
    device = FailedAudioDevice()
    app.sound_objects = {key: failed_sound(device) for key in ("scan_master", "pass", "fail")}
    app._play_sound = MethodType(lm.Label_Match._play_sound, app)
    monkeypatch.setattr(lm, "_label_match_automated_test_mode", lambda: False)
    syncs = []
    monkeypatch.setattr(
        lm, "_label_match_start_session_direct_sync",
        lambda context, *, reason: syncs.append(reason) or SimpleNamespace(is_alive=lambda: False),
    )
    values = [MASTER, *products()]
    for value in values:
        scan(app, value)
    rows = completions(app)
    assert len(rows) == 1
    assert rows[0][1]["final_result"] == app.Results.PASS
    assert rows[0][1]["scan_count"] == 5
    assert rows[0][1]["scanned_product_barcodes"] == values
    assert len(app.durable_commits) == 1
    assert app.current_set_info["raw"] == []
    assert syncs, "sound failure must not prevent scheduled server synchronization"
    scan(app, MASTER)
    assert app.current_set_info["raw"] == [MASTER]
    assert len(device.calls) == 6
    assert dialogs == []


def test_audio_device_initialization_failure_is_consumed_without_ui(monkeypatch):
    device = FailedAudioDevice()
    monkeypatch.setitem(sys.modules, "winsound", device)
    monkeypatch.setattr(lm, "_label_match_automated_test_mode", lambda: False)
    monkeypatch.setattr(lm, "_label_match_audio_enabled", lambda: True)
    app = object.__new__(lm.Label_Match)
    app.run_tests, app.audio_init_started = False, False
    app.initialized_successfully, app.audio_ready = True, False
    app._audio_stop_event = threading.Event()
    app._audio_init_result_queue = queue.Queue(maxsize=1)
    app.after = lambda *args: None
    app._start_audio_initialization()
    app._audio_init_thread.join(timeout=2)
    assert not app._audio_init_thread.is_alive()
    assert len(device.calls) == 1
    app._poll_audio_initialization()
    assert app.audio_init_finished and not app.audio_ready
    assert app.audio_error == "fake audio device failure"


def test_siren_playback_failure_is_queued_without_blocking_recovery():
    device = FailedAudioDevice()
    result = queue.Queue()
    lm.Label_Match._play_error_siren_loop(failed_sound(device), threading.Event(), result)
    assert len(device.calls) == 1
    assert result.get_nowait() == ("error", "fake audio device failure")

"""Linked shared-intake and real-render regressions for REVIEW-G86."""
from unittest.mock import Mock

import pytest

from tests import test_g86_scan_input_ownership as g

RESTART = "입력 칸 오류 — 프로그램을 다시 켜세요"


def feedback(app):
    notices, sounds = [], []
    app.update_big_display = lambda text, *_args: notices.append(text)
    app._play_sound = lambda sound: sounds.append(sound)
    return notices, sounds


def test_initial_read_failure_is_cleaned_and_next_scan_is_accepted():
    app = g.lm_app()
    notices, sounds = feedback(app)
    app.entry.insert("end", g.FIRST)
    get = app.entry.get
    calls = []
    def one_bad_read():
        calls.append(True)
        if len(calls) == 1:
            raise g.TclError("synthetic read failure")
        return get()
    app.entry.get = one_bad_read
    app._handle_scan_enter()
    assert app.entry.get() == "" and not getattr(app, "_scan_entry_broken", False)
    assert app.accepted == [] and notices and "다시 스캔" in notices[-1] and sounds
    app.entry.insert("end", g.SECOND)
    app._handle_scan_enter()
    assert app.accepted == [g.SECOND]


def test_permanent_read_failure_latches_only_after_cleanup_retry():
    app = g.lm_app()
    notices, sounds = feedback(app)
    calls = []
    def unreadable():
        calls.append(True)
        raise g.TclError("synthetic read failure")
    app.entry.get = unreadable
    app._handle_scan_enter()
    assert len(calls) >= 3
    assert app._scan_entry_broken and app.accepted == []
    assert notices[-1] == RESTART and sounds


@pytest.mark.parametrize("transport", ["bridge", "intake", "process"])
def test_broken_input_blocks_real_intake_and_process_input(transport):
    app = g.lm_app()
    notices, sounds = feedback(app)
    app.entry.insert("end", g.FIRST)
    app.entry.fail_delete = 20
    app._handle_scan_enter()
    assert app._scan_entry_broken
    app.entry.fail_delete = 0
    if transport == "bridge":
        bridge = type("ReviewBridge", (), g.LM_BRIDGE)()
        bridge.app, bridge._serial = app, 0
        bridge.scan(g.SECOND, {})
    elif transport == "intake":
        app.intake_scan(g.SECOND)
    else:
        app.process_input(raw=g.SECOND)
    assert app.accepted == []
    assert notices[-1] == RESTART and len(sounds) >= 2


@pytest.mark.parametrize("mode", ["normal", "completion-queue", "readonly"])
def test_broken_input_survives_actual_operator_workbench_rerender(mode):
    from tests.test_label_operator_action_gates import _render_app
    app = _render_app()
    app.update_big_display = Mock()
    app._play_sound = Mock()
    app.entry.insert("end", g.FIRST)
    original_delete = app.entry.delete
    # Use the actual Tk error type for the product method's exception boundary.
    import tkinter
    def failed_delete(*_args):
        raise tkinter.TclError("synthetic delete failure")
    app.entry.delete = failed_delete
    app._handle_scan_enter()
    assert app._scan_entry_broken
    app.entry.delete = original_delete
    if mode == "completion-queue":
        app._completion_scan_queue_open = lambda: True
    elif mode == "readonly":
        app.history_view_updates_active_state = False
    view = app._render_operator_workbench()
    assert not view.scan_input_enabled
    assert app.entry.cget("state") == "disabled"
    assert view.notice is not None
    assert RESTART in (view.notice.title, view.notice.message)
    assert app.big_display_label.options.get("text") == RESTART

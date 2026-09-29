"""Scans made during a completion save wait for it in scan order (w9lmscanmsg B).

The completion save (UI lane task f3-package-completion) keeps the entry
open; each Enter takes its value out of the entry into an ordered queue. The
queue runs as the next set's scans only after the completion is durable and
the set has returned to idle. Every other end refuses all held scans with the
warning sound and a "N건 ... 다시 스캔" notice, never silently.
"""
from __future__ import annotations

import threading

import pytest

import Label_Match as label_module
from package_logistics import PackageLogisticsError
from tests.test_label_match_core import load_label_match_module
from tests.test_label_operator_action_gates import FakeWidget
from tests.test_label_ui_lane_integration import (
    _CENTRAL_PHS2, _FIVE_SCANS, _ScannerEntry, _close_lane, _scanner, _workbench_with_lane,
)
from tests.test_legacy_label_transition import MASTER, _close, _events, _legacy_set, _packaging_app
from tests.test_tk_serial_ui_lane import FakeTkRoot
from tk_serial_ui_lane import LaneState, TkSerialUiLane

NEXT_SET = _legacy_set(61)


def _completion_app(tmp_path, monkeypatch):
    """A real legacy 5-scan completion on a real lane; its durable save waits
    until the test releases it."""

    module = load_label_match_module()
    app, _syncs = _packaging_app(module, tmp_path, monkeypatch, registered=True, transition=True)
    root = FakeTkRoot()
    app._ui_lane_generation = 0
    app._ui_lane_busy_label = ""
    app._ui_lane_busy_task = ""
    app._app_close_in_progress = False
    app.operator_workbench_ready = False
    app.ui_lane = TkSerialUiLane(
        root, poll_ms=1, generation_provider=lambda: app._ui_lane_generation,
        on_runner_fault=app._handle_ui_lane_fault,
    )
    app.after = root.after
    app.after_cancel = root.after_cancel
    app.entry = _ScannerEntry()
    app.big_display = []
    app.update_big_display = lambda text, *_args: app.big_display.append(text)
    entered, release = threading.Event(), threading.Event()
    app.commit_error = None
    commit = app._commit_finalized_set_durable

    def held_commit(**kwargs):
        entered.set()
        assert release.wait(timeout=10)
        if app.commit_error is not None:
            raise app.commit_error
        return commit(**kwargs)

    app._commit_finalized_set_durable = held_commit
    return module, app, root, entered, release


def _save_first_set(app, root, entered):
    for value in _legacy_set(60)[:4]:
        _scanner(app, value)
    assert not app.ui_lane.is_busy()
    _scanner(app, _legacy_set(60)[4])  # the fifth scan starts the completion save
    root.run_until(entered.is_set, timeout=10)
    assert app.ui_lane.is_busy() and app._ui_lane_busy_task == "f3-package-completion"


def _attempts(app):
    return [details.get("raw_input") for event, details in _logged(app) if event == "SCAN_ATTEMPT"]


def _logged(app):
    app.data_manager.flush(timeout=5)
    return list(app.logged)


def _record_events(app):
    app.logged = []
    log_event = app.data_manager.log_event

    def record(event, details=None, *args, **kwargs):
        app.logged.append((event, dict(details or {})))
        return log_event(event, details, *args, **kwargs)

    app.data_manager.log_event = record


def _status(app):
    label = app.status_label
    options = getattr(label, "options", None) or getattr(label, "kwargs", None) or {}
    return str(options.get("text") or "")


def _refused_aloud(app, count):
    status = _status(app)
    return app.sounds[-1:] == ["fail"] and f"{count}건" in status and "다시 스캔" in status


def _settle(app, root):
    # The lane keeps polling, so wait for idle and an empty queue, not for no jobs.
    root.run_until(
        lambda: not app.ui_lane.is_busy() and not app.__dict__.get("_scans_behind_completion"),
        timeout=10,
    )


def _finish(app, root, release):
    release.set()
    _settle(app, root)


def _teardown(app, root, release):
    # Close the lane even when broken: its worker thread is not a daemon and
    # only close_idle() stops it (as Label_Match.destroy() does), so a lane
    # left open kept pytest from exiting.
    release.set()
    try:
        if app.ui_lane.state is LaneState.BROKEN:
            app.ui_lane.close_idle()
            assert str(app.ui_lane.state) == "CLOSED"
        else:
            _close_lane(app, root)
    finally:
        _close(app)


def test_scans_during_a_completion_save_start_the_next_set_in_scan_order(tmp_path, monkeypatch):
    module, app, root, entered, release = _completion_app(tmp_path, monkeypatch)
    _record_events(app)
    try:
        _save_first_set(app, root, entered)
        sounds_before = list(app.sounds)
        _scanner(app, NEXT_SET[0])
        _scanner(app, NEXT_SET[1])

        # Held, not joined: the entry is empty for the next scan, nothing ran yet.
        assert app.entry.get() == ""
        assert _attempts(app) == _legacy_set(60)
        assert app.sounds == sounds_before
        assert "이어서 처리할 스캔 2건" in _status(app)

        _finish(app, root, release)

        assert app.current_set_info["raw"] == NEXT_SET[:2]
        assert _attempts(app) == _legacy_set(60) + NEXT_SET[:2]
        events = [event for event, _details in _logged(app)]
        assert events.index("TRAY_COMPLETE") < len(events) - 1 - events[::-1].index("SCAN_ATTEMPT")
        assert "fail" not in app.sounds
    finally:
        _teardown(app, root, release)


def test_a_scan_after_the_save_but_before_the_held_scans_run_keeps_its_turn(tmp_path, monkeypatch):
    module, app, root, entered, release = _completion_app(tmp_path, monkeypatch)
    _record_events(app)
    try:
        _save_first_set(app, root, entered)
        _scanner(app, NEXT_SET[0])
        release.set()
        root.run_until(lambda: not app.ui_lane.is_busy(), timeout=10)  # saved; held scan not run yet
        assert app.current_set_info["raw"] == []
        _scanner(app, NEXT_SET[1])
        _settle(app, root)

        assert app.current_set_info["raw"] == NEXT_SET[:2]
        assert _attempts(app) == _legacy_set(60) + NEXT_SET[:2]
        assert app.entry.get() == ""
    finally:
        _teardown(app, root, release)


def test_the_same_value_held_twice_runs_twice_as_two_scans_would(tmp_path, monkeypatch):
    """A double read while waiting is not merged away: both run in order and the
    second meets the same rule as two scans made without a save in between."""

    module, app, root, entered, release = _completion_app(tmp_path, monkeypatch)
    _record_events(app)
    (tmp_path / "reference").mkdir()
    _module, reference, ref_root, _entered, ref_release = _completion_app(tmp_path / "reference", monkeypatch)
    ref_release.set()
    try:
        _save_first_set(app, root, entered)
        _scanner(app, NEXT_SET[1])
        _scanner(app, NEXT_SET[1])
        sounds_before, errors_before = len(app.sounds), len(app.errors)
        _finish(app, root, release)

        for value in _legacy_set(60):
            _scanner(reference, value)
            _settle(reference, ref_root)
        reference.sounds.clear()
        reference.errors.clear()
        for value in (NEXT_SET[1], NEXT_SET[1]):
            _scanner(reference, value)

        assert _attempts(app)[-2:] == [NEXT_SET[1], NEXT_SET[1]]
        assert app.current_set_info["raw"] == reference.current_set_info["raw"]
        assert bool(app.current_set_info.get("has_error_or_reset")) == bool(
            reference.current_set_info.get("has_error_or_reset")
        )
        assert app.errors[errors_before:] == reference.errors
        assert app.sounds[sounds_before:] == ["pass", *reference.sounds]
    finally:
        try:
            _teardown(app, root, release)
        finally:
            _teardown(reference, ref_root, ref_release)


@pytest.mark.parametrize("error", [
    RuntimeError("completion save failed (test)"),
    PackageLogisticsError("completion held (test)"),
], ids=["failed", "held"])
def test_a_completion_that_does_not_finish_refuses_the_held_scans_aloud(tmp_path, monkeypatch, error):
    module, app, root, entered, release = _completion_app(tmp_path, monkeypatch)
    _record_events(app)
    try:
        _save_first_set(app, root, entered)
        _scanner(app, NEXT_SET[0])
        _scanner(app, NEXT_SET[1])
        app.commit_error = error
        _finish(app, root, release)

        assert app.blocks  # the completion's own block notice stays
        assert app.current_set_info["raw"] == _legacy_set(60)
        assert _attempts(app) == _legacy_set(60)
        assert _refused_aloud(app, 2), _status(app)
        assert app.__dict__.get("_scans_behind_completion") in (None, [])
    finally:
        _teardown(app, root, release)


def test_closing_during_the_save_refuses_the_held_scan_before_it_closes(tmp_path, monkeypatch):
    """Container_Audit review counterexample 1 (close before the save ends), in LM."""

    module, app, root, entered, release = _completion_app(tmp_path, monkeypatch)
    _record_events(app)
    try:
        _save_first_set(app, root, entered)
        _scanner(app, NEXT_SET[0])
        app.after = lambda *_args: None  # the close watchdog; the fake loop ignores delays
        module.Label_Match.on_closing(app, _confirmed=True)

        assert _refused_aloud(app, 1), _status(app)
        assert app.__dict__.get("_scans_behind_completion") in (None, [])

        release.set()
        root.run_until(lambda: not app.ui_lane.is_busy(), timeout=10)
        assert _attempts(app) == _legacy_set(60)  # the held scan never ran
        assert "TRAY_COMPLETE" in [event for event, _details in _logged(app)]  # the save itself finished
    finally:
        _teardown(app, root, release)


@pytest.mark.parametrize("change", ["f1-reset", "generation"])
def test_a_work_change_before_the_held_scans_run_refuses_them_aloud(tmp_path, monkeypatch, change):
    """Container_Audit review counterexample 2 (reset between the save's end and
    the queue drain), and a generation change without a reset, in LM."""

    module, app, root, entered, release = _completion_app(tmp_path, monkeypatch)
    _record_events(app)
    try:
        _save_first_set(app, root, entered)
        _scanner(app, NEXT_SET[0])
        release.set()
        root.run_until(lambda: not app.ui_lane.is_busy(), timeout=10)  # saved; drain not run yet
        assert "TRAY_COMPLETE" in [event for event, _details in _logged(app)]
        if change == "f1-reset":
            app._reset_current_set(full_reset=True)
            assert _refused_aloud(app, 1), _status(app)  # at the reset, not later
        else:
            app._advance_ui_lane_generation()
        _settle(app, root)

        assert _refused_aloud(app, 1), _status(app)
        assert app.current_set_info["raw"] == []
        assert _attempts(app) == _legacy_set(60)
    finally:
        _teardown(app, root, release)


def test_a_lane_fault_during_the_save_refuses_the_held_scans_aloud(tmp_path, monkeypatch):
    module, app, root, entered, release = _completion_app(tmp_path, monkeypatch)
    _record_events(app)
    try:
        _save_first_set(app, root, entered)
        _scanner(app, NEXT_SET[0])

        def broken_apply(*_args, **_kwargs):
            raise RuntimeError("completion apply fault (test)")

        app._finalize_set = broken_apply
        release.set()
        root.run_until(lambda: app.ui_lane.state is LaneState.BROKEN, timeout=10)

        assert "1건" in _status(app) and app.sounds[-1:] == ["fail"], _status(app)
        assert app.__dict__.get("_scans_behind_completion") in (None, [])
        assert _attempts(app) == _legacy_set(60)
    finally:
        _teardown(app, root, release)


def _render_completion_save(app, gate):
    admission = app._submit_ui_lane_task(
        name="f3-package-completion",
        busy_text="포장 완료 · 권한 확인 및 로컬 완료 저장 중",
        work=lambda: gate.wait(timeout=2.0),
        finish=lambda _value: None,
        fail=pytest.fail,
    )
    assert admission.accepted


@pytest.mark.parametrize("state", ["legacy", "central"])
def test_the_completion_save_keeps_the_scan_entry_open(state):
    """The rendered workbench leaves the entry open only for the completion save;
    a central 1/1 set waiting for F3 opens it too once F3 saves it."""

    app, root = _workbench_with_lane()
    if state == "central":
        app.current_set_info.update(raw=[_CENTRAL_PHS2], parsed=["ITEM-001"], central_inherit_all=True)
    else:
        app.current_set_info.update(raw=list(_FIVE_SCANS), parsed=["ITEM-001"] * 5)
    gate = threading.Event()
    try:
        _render_completion_save(app, gate)
        assert app.entry.cget("state") == "normal"

        _scanner(app, "ITEM-002")

        assert app.entry.get() == ""
        assert app.__dict__.get("_scans_behind_completion") == ["ITEM-002"]
        assert app.sounds == []
        assert "이어서 처리할 스캔 1건" in app.status_label.options["text"]
    finally:
        gate.set()
        _close_lane(app, root)


def test_other_lane_work_and_screen_gates_still_lock_the_entry():
    """Only the completion save holds scans: a lookup keeps the refusal, and a
    blocking notice keeps the entry locked even during the save."""

    app, root = _workbench_with_lane()
    gate = threading.Event()
    try:
        admission = app._submit_ui_lane_task(
            name="f4-central-source-lookup", busy_text="제품 교체 · 중앙 확인 중",
            work=lambda: gate.wait(timeout=2.0), finish=lambda _value: None, fail=pytest.fail,
        )
        assert admission.accepted and app.entry.cget("state") == "disabled"
        gate.set()
        root.run_until(lambda: not app.ui_lane.is_busy())

        gate.clear()
        app._workflow_blocking_notice = label_module.WorkflowNotice(
            title="확인 필요", message="현재 상태 확인", kind="blocked", tone="warning"
        )
        _render_completion_save(app, gate)
        assert app.entry.cget("state") == "disabled"
        _scanner(app, "ITEM-002")
        assert app.__dict__.get("_scans_behind_completion") in (None, [])
        assert app.sounds == ["fail"]
    finally:
        gate.set()
        _close_lane(app, root)

"""DESIGN §1.2: close a failed set durably without eating the next scan."""
from types import MethodType

import pytest

import Label_Match as lm
from tests.test_w9scanonly_packaging import packaging, completions, products  # noqa: F401
from tests.test_legacy_label_transition import MASTER, _app_for_recovery, _close, _events
from tests.test_label_match_core import _FakeEntry


class ScannerEntry(_FakeEntry):
    def focus_set(self):
        self.focused = True


def scan(app, value):
    app.entry = ScannerEntry(value)
    return app._handle_scan_enter()


def live_presenter(app):
    from tests.test_label_operator_action_gates import _render_app
    donor = _render_app(raw=tuple(app.current_set_info["raw"]), parsed=tuple(app.current_set_info["parsed"]))
    for name, value in donor.__dict__.items():
        app.__dict__.setdefault(name, value)
    app._render_operator_workbench = MethodType(lm.Label_Match._render_operator_workbench, app)
    app._return_to_idle_after_finalized_set = MethodType(lm.Label_Match._return_to_idle_after_finalized_set, app)


@pytest.mark.parametrize("bad", ["OTHER-ITEM-WRONG-PRODUCT", "BAD"])
def test_error_records_fail_immediately_and_the_next_entry_label_starts_a_set(packaging, bad):
    app, dialogs = packaging
    live_presenter(app)
    shown_after_failure = []
    error_scan = False
    display = app.update_big_display

    def show(message, *args, **kwargs):
        if error_scan:
            shown_after_failure.append(bool(completions(app)))
        return display(message, *args, **kwargs)

    app.update_big_display = show
    scan(app, MASTER)
    error_scan = True
    scan(app, bad)
    error_scan = False
    rows = completions(app)
    assert len(rows) == 1
    assert rows[0][1]["final_result"] in {app.Results.FAIL_MISMATCH, app.Results.FAIL_INPUT_ERROR}
    assert rows[0][1]["scanned_product_barcodes"] == [MASTER]
    event = app.Events.ERROR_INPUT if bad == "BAD" else app.Events.ERROR_MISMATCH
    audit = _events(app.save_directory, event)
    assert len(audit) == 1 and audit[0][1]["raw"] == bad
    assert app.current_set_info["raw"] == []
    assert app._render_operator_workbench().scan_input_enabled
    assert not app.__dict__.get("_pending_workflow_error")
    assert shown_after_failure and all(shown_after_failure)
    scan(app, MASTER)
    assert app.current_set_info["raw"] == [MASTER]
    assert app.entry.get() == ""
    assert len(completions(app)) == 1
    assert dialogs == []


@pytest.mark.parametrize("cut", ["before_fail_event", "after_fail_event"])
@pytest.mark.parametrize("empty", [False, True])
def test_restart_replays_one_failure_and_does_not_restore_the_failed_set(packaging, tmp_path, monkeypatch, cut, empty):
    app, _dialogs = packaging
    live_presenter(app)
    for value in (() if empty else (MASTER, products()[0])):
        scan(app, value)
    original_log = app.data_manager.log_event

    class PowerCut(BaseException):
        pass

    if cut == "before_fail_event":
        def stop_before(event, details):
            if event == app.Events.TRAY_COMPLETE:
                raise PowerCut()
            return original_log(event, details)
        monkeypatch.setattr(app.data_manager, "log_event", stop_before)
    else:
        def stop_after():
            raise PowerCut()
        app._return_to_idle_after_finalized_set = stop_after
    try:
        with pytest.raises(PowerCut):
            scan(app, "BAD" if empty else "OTHER-ITEM-WRONG-PRODUCT")
    finally:
        monkeypatch.setattr(app.data_manager, "log_event", original_log)
        _close(app)
    restored = _app_for_recovery(lm, tmp_path, monkeypatch)
    live_presenter(restored)
    try:
        restored._load_current_set_state()
        rows = completions(restored)
        result = restored.Results.FAIL_INPUT_ERROR if empty else restored.Results.FAIL_MISMATCH
        assert len(rows) == 1 and rows[0][1]["final_result"] == result
        event = restored.Events.ERROR_INPUT if empty else restored.Events.ERROR_MISMATCH
        audit = _events(restored.save_directory, event)
        assert len(audit) == 1 and audit[0][1]["raw"] == ("BAD" if empty else "OTHER-ITEM-WRONG-PRODUCT")
        assert restored.current_set_info["raw"] == []
        scan(restored, MASTER)
        assert restored.current_set_info["raw"] == [MASTER]
        assert len(completions(restored)) == 1
    finally:
        _close(restored)


def test_failed_completion_retry_keeps_its_failure_verdict(packaging, monkeypatch):
    app, _dialogs = packaging
    live_presenter(app)
    scan(app, MASTER)
    commit = app._commit_finalized_set_durable
    monkeypatch.setattr(app, "_commit_finalized_set_durable", lambda **kw: (_ for _ in ()).throw(OSError("synthetic failure save")))
    scan(app, "OTHER-ITEM-WRONG-PRODUCT")
    assert completions(app) == [] and app.current_set_info["raw"] == [MASTER]
    assert app.current_set_info["scanner_failure_result"] == app.Results.FAIL_MISMATCH
    monkeypatch.setattr(app, "_commit_finalized_set_durable", commit)
    app._retry_blocked_submission()
    rows = completions(app)
    assert len(rows) == 1 and rows[0][1]["final_result"] == app.Results.FAIL_MISMATCH
    assert app.current_set_info["raw"] == []

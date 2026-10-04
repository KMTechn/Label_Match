"""Shutdown diagnostics remain bounded, fenced and advisory at startup."""

from contextlib import contextmanager
import json
import os
from pathlib import Path
import sys

import pytest

import label_match_single_instance as instance
import writer_session_fence as fence
from writer_sink_inventory import derive_writer_sink_inventory


def test_live_native_process_notice_and_normal_cleanup(tmp_path):
    instance._set_shutdown_stall_notice(tmp_path, stalled=True)
    assert "PC를 재시작" in instance._shutdown_stall_message(tmp_path)
    instance._set_shutdown_stall_notice(tmp_path, stalled=False)
    assert instance._shutdown_stall_message(tmp_path) == ""


@pytest.mark.parametrize("condition", [
    "missing", "corrupt", "list", "dead", "other_image", "boolean_pid",
    "zero_pid", "negative_pid", "huge_pid", "oversize",
])
def test_unrelated_or_invalid_notice_does_not_claim_shutdown_stall(tmp_path, monkeypatch, condition):
    marker = tmp_path / "label_match_shutdown_stall.json"
    value = {"pid": os.getpid(), "executable": sys.executable}
    if condition == "missing":
        pass
    elif condition == "corrupt":
        marker.write_text("{", encoding="utf-8")
    elif condition == "list":
        marker.write_text("[]", encoding="utf-8")
    elif condition == "oversize":
        marker.write_text(" " * 4097, encoding="utf-8")
    else:
        if condition == "dead":
            value["pid"] = 2**32 - 1
        elif condition == "other_image":
            value["executable"] = "C:/unrelated/app.exe"
        elif condition == "boolean_pid":
            value["pid"] = True
        elif condition == "zero_pid":
            value["pid"] = 0
        elif condition == "negative_pid":
            value["pid"] = -1
        elif condition == "huge_pid":
            value["pid"] = 2**100
        marker.write_text(json.dumps(value), encoding="utf-8")
    assert instance._shutdown_stall_message(tmp_path) == ""


def test_general_sink_fences_both_write_and_delete(tmp_path, monkeypatch):
    marker = tmp_path / "label_match_shutdown_stall.json"
    rows = derive_writer_sink_inventory(Path(__file__).resolve().parents[1])
    assert [r.qualified_name for r in rows if r.source == "gui_shutdown_stall_notice"] == ["_set_shutdown_stall_notice"]
    attempts = []

    @contextmanager
    def denied(source):
        attempts.append(source)
        raise RuntimeError("writer admission denied")
        yield

    monkeypatch.setattr(fence, "writer_admission", denied)
    instance._try_update_shutdown_stall_notice(tmp_path, stalled=True)
    assert not marker.exists()
    marker.write_text("preserved", encoding="utf-8")
    instance._try_update_shutdown_stall_notice(tmp_path, stalled=False)
    assert marker.read_text(encoding="utf-8") == "preserved"
    assert attempts == ["gui_shutdown_stall_notice", "gui_shutdown_stall_notice"]


def test_notice_delete_failure_does_not_block_startup_or_guard_release(tmp_path, monkeypatch):
    marker = tmp_path / "label_match_shutdown_stall.json"
    marker.write_text("old notice", encoding="utf-8")
    unlink = Path.unlink

    def refuse_notice_delete(path, *args, **kwargs):
        if path == marker:
            raise PermissionError("injected notice deletion failure")
        return unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", refuse_notice_delete)
    started, closed = [], []
    lease = instance.MutexLease(True, "test-only", 1, closed.append)
    assert instance.run_guarded_entrypoint(
        lambda: started.append(True), data_scope=tmp_path, acquire=lambda _: lease,
    ) == 0
    assert started == [True]
    assert closed == [1]
    assert marker.read_text(encoding="utf-8") == "old notice"


def test_duplicate_without_window_reports_valid_stall_and_keeps_marker(tmp_path, capsys):
    instance._set_shutdown_stall_notice(tmp_path, stalled=True)
    lease = instance.MutexLease(False, "test-only")
    assert instance.run_guarded_entrypoint(
        lambda: pytest.fail("duplicate entered startup"), data_scope=tmp_path,
        acquire=lambda _: lease, activate=lambda: instance.ActivationResult(False),
    ) == 0
    assert "PC를 재시작" in capsys.readouterr().out
    assert (tmp_path / "label_match_shutdown_stall.json").exists()


def test_ui_notice_failure_still_records_stall_without_abandoning_wait(tmp_path):
    import Label_Match as lm

    app = lm.Label_Match.__new__(lm.Label_Match)
    app.save_directory = str(tmp_path)
    def unavailable_ui():
        raise RuntimeError("injected unavailable UI")
    app._show_app_close_cleanup_delay = unavailable_ui
    app._mark_app_close_stall()
    assert app._app_close_stall_noted is True
    assert "PC를 재시작" in instance._shutdown_stall_message(tmp_path)

"""Bounded, headless coverage of official stop during standalone notices."""

import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import Label_Match as lm
import auto_update
import item_catalog_sync
import startup_observation
import user_relay


@pytest.fixture
def notice_window(monkeypatch, tmp_path):
    monkeypatch.setenv("LABEL_MATCH_DIRECT_SYNC_ROOT", str(tmp_path / "relay"))
    monkeypatch.setattr(user_relay, "_app_stop_process_snapshot", lambda **kwargs: [])
    monkeypatch.setattr(user_relay, "request_user_relay_stop",
                        lambda *args, **kwargs: {"status": "ABSENT"})
    parents = []
    traces = []

    class Parent:
        def __init__(self):
            self.jobs = []
            self.destroyed = False
            self.destroyed_in_loop = False
            self.elapsed_ms = 0
            self.labels = []
            self.buttons = []
            parents.append(self)

        def __getattr__(self, name):
            return Mock()

        def after(self, delay, callback):
            self.jobs.append((delay, callback))
            return str(len(self.jobs))

        def destroy(self):
            self.destroyed = True

        def mainloop(self):
            # Use the public publisher, with only OS process/relay observation
            # replaced. The live fake window must not be reported as stopped.
            monkeypatch.setattr(user_relay, "_owned_app_stop_processes",
                                lambda *args, **kwargs: [object()] if not self.destroyed else [])
            result = user_relay.request_app_stop(
                tmp_path / "relay", app_root=tmp_path / "app", timeout_seconds=0,
            )
            assert result["status"] == "UNKNOWN"
            assert result["remaining_process_count"] == 1
            for _ in range(8):
                if self.destroyed or not self.jobs:
                    break
                delay, callback = self.jobs.pop(0)
                self.elapsed_ms += delay
                callback()
            self.destroyed_in_loop = self.destroyed

    monkeypatch.setattr(lm.tk, "Tk", Parent)

    class Widget:
        def __init__(self, parent, **kwargs):
            self.values = kwargs
            self.pack = Mock()
            self.focus_set = Mock()
            (parent.buttons if "command" in kwargs else parent.labels).append(self)

    def trace(stage, **details):
        if stage == "APP_STOP_REQUESTED_CLOSE":
            assert parents[-1].destroyed, "trace must follow destruction"
        traces.append((stage, details))

    monkeypatch.setattr(lm.tk, "Label", Widget)
    monkeypatch.setattr(lm.tk, "Button", Widget)
    monkeypatch.setattr(lm, "_label_match_startup_trace", trace)
    return SimpleNamespace(parents=parents, traces=traces)


def show_notice(notice, tmp_path):
    if notice == "registration-recovery":
        return lm._show_first_run_registration_recovery("POSSESSION_PROOF_REATTACH_REQUIRED")
    if notice == "startup-refusal":
        startup_observation.show_startup_blocked_notice()
    elif notice == "onboarding-error":
        return lm._show_first_run_onboarding_error(lm.CurrentUserOnboardingError(
            "unused", report_path=tmp_path / "report.json", status="BLOCKED", cause_code="TEST_NOTICE",
        ))
    elif notice == "integrity-warning":
        return lm._show_bootstrap_integrity_absent_warning()
    elif notice == "catalog-error":
        return lm._show_item_catalog_startup_error(catalog_error())
    elif notice == "mutex-recovery":
        return auto_update._show_update_mutex_recovery_notice()
    elif notice == "catalog-retry":
        return lm._offer_item_catalog_startup_retry(catalog_error())


def catalog_error():
    return lm.ItemCatalogSyncError("offline", cause_code=item_catalog_sync.REQUEST_FAILED_NO_CACHE)


def expected_message(notice, tmp_path):
    if notice == "registration-recovery":
        return lm.FIRST_RUN_REGISTRATION_RECOVERY_MESSAGE
    if notice == "startup-refusal":
        return startup_observation.STARTUP_BLOCKED_MESSAGE
    if notice == "onboarding-error":
        return lm.FIRST_RUN_ONBOARDING_ERROR_MESSAGE + "\n보고서: " + str(tmp_path / "report.json")
    if notice == "integrity-warning":
        return lm.BOOTSTRAP_INTEGRITY_ABSENT_WARNING_MESSAGE
    if notice == "mutex-recovery":
        return auto_update.UPDATE_MUTEX_RECOVERY_MESSAGE
    error = catalog_error()
    context = lm.get_catalog_attempt_context_from_error(error)
    details = lm._item_catalog_profile_details_for_display(context)
    if notice == "catalog-retry":
        return ("회사 서버 품목 목록을 사용할 수 없습니다\n연결을 확인하고 「다시 시도」를 누르세요. "
                "또 안 되면 현장 관리자에게 알리세요\n자세히: " + details + "\n코드: " + error.cause_code)
    return ("품목 목록을 확인하지 못했습니다\n연결을 확인하고 다시 시도하세요\n코드: "
            + lm.ITEM_CATALOG_STARTUP_ERROR_MESSAGE + " · " + details + " · " + error.cause_code)


@pytest.mark.parametrize("notice", [
    "registration-recovery", "startup-refusal", "onboarding-error", "integrity-warning",
    "catalog-error", "catalog-retry", "mutex-recovery",
])
def test_startup_notice_observes_official_request(monkeypatch, tmp_path, notice_window, notice):
    native_messages = []

    def retry(title, message, *, parent):
        native_messages.append(message)
        parent.mainloop()
        return True  # A stop must override the native affirmative result.

    monkeypatch.setattr(lm.messagebox, "askretrycancel", retry)
    result = show_notice(notice, tmp_path)
    parents = notice_window.parents

    assert len(parents) == 1
    assert parents[0].destroyed_in_loop, "official stop must close the running notice loop"
    assert parents[0].elapsed_ms <= 500
    texts = native_messages + [label.values["text"] for label in parents[0].labels]
    assert expected_message(notice, tmp_path).encode("utf-8") in [text.encode("utf-8") for text in texts]
    assert [(stage, details) for stage, details in notice_window.traces
            if stage == "APP_STOP_REQUESTED_CLOSE"] == [
                ("APP_STOP_REQUESTED_CLOSE", {"message": "멈춤 요청으로 닫힘"})]
    if notice == "catalog-retry":
        assert result is False


@pytest.mark.parametrize("notice", ["onboarding-error", "integrity-warning", "catalog-error"])
def test_tk_creation_failure_uses_native_with_identical_message(monkeypatch, tmp_path, notice):
    monkeypatch.setattr(lm.tk, "Tk", Mock(side_effect=lm.tk.TclError("unavailable")))
    native = Mock()
    monkeypatch.setattr(startup_observation, "show_bounded_native_notice", native, raising=False)
    show_notice(notice, tmp_path)
    native.assert_called_once()
    assert native.call_args.args[1].encode("utf-8") == expected_message(notice, tmp_path).encode("utf-8")


def test_trace_write_failure_does_not_prevent_normal_notice_return(monkeypatch, tmp_path, notice_window):
    monkeypatch.setattr(lm, "_label_match_startup_trace", Mock(side_effect=OSError("unavailable")))
    lm._show_first_run_registration_recovery("POSSESSION_PROOF_REATTACH_REQUIRED")
    assert notice_window.parents[0].destroyed_in_loop


def test_official_stop_waits_for_isolated_notice_process_exit(monkeypatch, tmp_path):
    child_root = tmp_path / "child"
    child_root.mkdir()
    child = subprocess.Popen(
        [sys.executable, "-B", str(Path(__file__).with_name("_startup_notice_stop_child.py")), str(child_root)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, creationflags=subprocess.CREATE_NO_WINDOW,
    )
    try:
        assert child.stdout.readline().strip() == b"NOTICE_READY"
        monkeypatch.setattr(user_relay, "_app_stop_process_snapshot", lambda **kwargs: [])
        monkeypatch.setattr(user_relay, "_owned_app_stop_processes",
                            lambda *args, **kwargs: [object()] if child.poll() is None else [])
        monkeypatch.setattr(user_relay, "request_user_relay_stop",
                            lambda *args, **kwargs: {"status": "ABSENT"})
        report = user_relay.request_app_stop(
            child_root / "relay", app_root=child_root / "app", timeout_seconds=5,
        )
        assert child.wait(timeout=10) == 0
        assert child.poll() == 0
        assert report["status"] == "STOPPED" and report["remaining_process_count"] == 0
        records = [json.loads(line) for path in (child_root / "KMTech-startup-trace").glob("*.log")
                   for line in path.read_text(encoding="utf-8").splitlines()]
        assert any(record.get("stage") == "APP_STOP_REQUESTED_CLOSE"
                   and record.get("message") == "멈춤 요청으로 닫힘" for record in records)
    finally:
        # This is the exact child created above. Ask its bounded fake loop to
        # finish normally if an assertion failed before the official request.
        (child_root / "finish-test").touch()
        child.communicate(timeout=10)

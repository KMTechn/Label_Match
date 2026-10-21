"""Bounded, headless coverage of official stop during standalone notices."""

import ctypes
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
from tests.test_portable_gui_onboarding import portable_gui


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


@pytest.mark.parametrize("warning", [False, True])
def test_bounded_native_preserves_text_and_uses_fixed_timeout(monkeypatch, warning):
    native = Mock(return_value=32000)  # IDTIMEOUT, never an affirmative choice.
    monkeypatch.setattr(ctypes, "WinDLL", lambda *args, **kwargs: SimpleNamespace(MessageBoxTimeoutW=native))
    title, message = "안내", "원래 문구\n둘째 줄"
    assert startup_observation.show_bounded_native_notice(
        title, message, warning=warning,
    ) is False
    native.assert_called_once()
    args = native.call_args.args
    assert args[1].encode("utf-8") == message.encode("utf-8")
    assert args[2].encode("utf-8") == title.encode("utf-8")
    assert args[3] & 0xF0 == (0x30 if warning else 0x10)
    assert args[-1] == 30000 and args[-1] < user_relay.APP_STOP_TIMEOUT_SECONDS * 1000


@pytest.mark.parametrize("notice", ["onboarding-error", "integrity-warning", "catalog-error"])
@pytest.mark.parametrize("native_failure", ["unavailable", "zero-result"])
def test_tk_and_native_failure_keeps_original_unavailable_trace(monkeypatch, tmp_path, notice, native_failure):
    monkeypatch.setattr(lm.tk, "Tk", Mock(side_effect=lm.tk.TclError("unavailable")))
    native = Mock(return_value=0)
    api = Mock(return_value=SimpleNamespace(MessageBoxTimeoutW=native))
    if native_failure == "unavailable":
        api.side_effect = OSError("unavailable")
    monkeypatch.setattr(ctypes, "WinDLL", api)
    unbounded = Mock(side_effect=AssertionError("no unbounded fallback"))
    monkeypatch.setattr(lm.messagebox, "showerror", unbounded)
    monkeypatch.setattr(lm.messagebox, "showwarning", unbounded)
    trace = Mock()
    monkeypatch.setattr(lm, "_label_match_startup_trace", trace)
    assert show_notice(notice, tmp_path) is None
    api.assert_called_once()
    unbounded.assert_not_called()
    trace.assert_called_once_with({
        "onboarding-error": "current_user_onboarding_dialog_unavailable",
        "integrity-warning": "bootstrap_integrity_warning_dialog_unavailable",
        "catalog-error": "item_catalog_startup_dialog_unavailable",
    }[notice])


@pytest.mark.parametrize("notice", ["onboarding-error", "integrity-warning", "catalog-error"])
def test_pending_official_stop_is_recorded_after_bounded_native_returns(monkeypatch, tmp_path, notice):
    root = tmp_path / "relay"
    monkeypatch.setenv("LABEL_MATCH_DIRECT_SYNC_ROOT", str(root))
    monkeypatch.setattr(lm.tk, "Tk", Mock(side_effect=lm.tk.TclError("unavailable")))
    monkeypatch.setattr(user_relay, "_app_stop_process_snapshot", lambda **kwargs: [])
    monkeypatch.setattr(user_relay, "request_user_relay_stop", lambda *args, **kwargs: {"status": "ABSENT"})
    returned = []
    def native(*args):
        user_relay.request_app_stop(root, app_root=tmp_path / "app", timeout_seconds=0)
        returned.append(True)
        return 32000
    original_library = ctypes.WinDLL
    def library(name, *args, **kwargs):
        if name == "user32":
            return SimpleNamespace(MessageBoxTimeoutW=Mock(side_effect=native))
        return original_library(name, *args, **kwargs)
    monkeypatch.setattr(ctypes, "WinDLL", library)
    records = []
    def trace(stage, **details):
        if stage == "APP_STOP_REQUESTED_CLOSE":
            assert returned, "native stop trace must follow the bounded call"
        records.append((stage, details))
    monkeypatch.setattr(lm, "_label_match_startup_trace", trace)
    result = show_notice(notice, tmp_path)
    assert returned == [True]
    assert records == [("APP_STOP_REQUESTED_CLOSE", {"message": "멈춤 요청으로 닫힘"})]
    if notice == "integrity-warning":
        assert result is False


def test_trace_write_failure_does_not_prevent_normal_notice_return(monkeypatch, tmp_path, notice_window):
    monkeypatch.setattr(lm, "_label_match_startup_trace", Mock(side_effect=OSError("unavailable")))
    lm._show_first_run_registration_recovery("POSSESSION_PROOF_REATTACH_REQUIRED")
    assert notice_window.parents[0].destroyed_in_loop


def test_early_mutex_notice_uses_owner_request_and_existing_trace_file(monkeypatch, tmp_path, notice_window):
    # The executable's first update gate precedes the app's reader and trace
    # definitions. It must still stop without importing or starting the app.
    application = SimpleNamespace(_APP_PROCESS_STARTED_AT=lm._APP_PROCESS_STARTED_AT)
    monkeypatch.setattr(startup_observation, "_notice_application_module", lambda: application)
    monkeypatch.setenv("TEMP", str(tmp_path))
    auto_update._show_update_mutex_recovery_notice()
    assert notice_window.parents[0].destroyed_in_loop
    records = [json.loads(line) for path in (tmp_path / "KMTech-startup-trace").glob("*.log")
               for line in path.read_text(encoding="utf-8").splitlines()]
    assert records == [{"stage": "APP_STOP_REQUESTED_CLOSE", "message": "멈춤 요청으로 닫힘"}]


def test_manual_notice_close_keeps_message_and_does_not_claim_official_stop(monkeypatch, tmp_path, notice_window):
    def manual_close(parent):
        parent.buttons[0].values["command"]()
    # Keep the same fake root, but close through the default button instead of
    # publishing a stop. No request file or stop audit should be produced.
    create = lm.tk.Tk
    def parent_factory():
        parent = create()
        parent.mainloop = lambda: manual_close(parent)
        return parent
    monkeypatch.setattr(lm.tk, "Tk", parent_factory)
    show_notice("onboarding-error", tmp_path)
    parent = notice_window.parents[0]
    assert parent.destroyed
    assert parent.labels[0].values["text"].encode("utf-8") == expected_message("onboarding-error", tmp_path).encode("utf-8")
    assert not user_relay.app_stop_request_path(tmp_path / "relay").exists()
    assert not [stage for stage, _ in notice_window.traces if stage == "APP_STOP_REQUESTED_CLOSE"]


@pytest.mark.parametrize("cause", ["POSSESSION_PROOF_REATTACH_REQUIRED", "ADMIN_RECOVERY_ACTION", "TEST_NOTICE"])
def test_stop_during_failed_onboarding_preserves_exit_cause_and_report(
    monkeypatch, tmp_path, portable_gui, notice_window, cause,
):
    report = tmp_path / "preserved-report.json"
    report.write_bytes(b"preserved-report")
    failure = lm.CurrentUserOnboardingError(
        "unchanged", report_path=report, status="RECOVERY_REQUIRED", cause_code=cause,
    )
    attempts = []
    def onboard(*args, **kwargs):
        attempts.append(True)
        raise failure
    forbidden = Mock(side_effect=AssertionError("no business startup after refusal"))
    monkeypatch.setattr(lm, "validate_pending_worker_install", lambda _root: None)
    monkeypatch.setattr(lm, "check_user_data_open", lambda _root: None)
    monkeypatch.setattr(lm, "verify_factory_contract_startup", lambda: None)
    monkeypatch.setattr(lm, "onboard_current_user", onboard)
    monkeypatch.setattr(lm, "resolve_data_scope", forbidden)
    monkeypatch.setattr(lm, "_run_label_match_application", forbidden)
    monkeypatch.setattr(startup_observation, "write_startup_result", lambda _result: False)
    assert lm.main(["--old-screen"]) == lm.ONBOARDING_EXIT_CODE
    assert attempts == [True]
    assert failure.status == "RECOVERY_REQUIRED" and failure.cause_code == cause
    assert report.read_bytes() == b"preserved-report"
    assert notice_window.parents[0].destroyed_in_loop
    forbidden.assert_not_called()


@pytest.mark.parametrize("native_result", [32000, 1])
def test_native_warning_timeout_cancels_startup_instead_of_confirming(monkeypatch, portable_gui, native_result):
    monkeypatch.setattr(lm, "validate_pending_worker_install", lambda _root: None)
    monkeypatch.setattr(lm, "check_user_data_open", lambda _root: None)
    monkeypatch.setattr(lm, "verify_factory_contract_startup", lambda: None)
    monkeypatch.setattr(auto_update, "startup_gate", lambda *args, **kwargs: None)
    monkeypatch.setattr(lm, "onboard_current_user",
                        lambda *args, **kwargs: {"bootstrap_integrity": {"status": "ABSENT"}})
    monkeypatch.setattr(lm.tk, "Tk", Mock(side_effect=lm.tk.TclError("unavailable")))
    native = Mock(return_value=native_result)
    monkeypatch.setattr(ctypes, "WinDLL", lambda *args, **kwargs: SimpleNamespace(MessageBoxTimeoutW=native))
    data_scope = Mock(return_value=object())
    business = Mock(return_value=37)
    monkeypatch.setattr(lm, "resolve_data_scope", data_scope)
    monkeypatch.setattr(lm, "run_guarded_entrypoint", business)
    assert lm.main(["--old-screen"]) == (0 if native_result == 32000 else 37)
    native.assert_called_once()
    if native_result == 32000:
        data_scope.assert_not_called()
        business.assert_not_called()
    else:
        data_scope.assert_called_once()
        business.assert_called_once()


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

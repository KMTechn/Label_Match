"""Existing startup decisions remain authoritative; diagnostic faults add no denial."""
import ast
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import uuid

import pytest

import Label_Match as app
import auto_update as update
import current_user_onboarding as onboarding
import item_catalog_sync as catalog
import label_data_generation as generation
import label_match_single_instance as instance
import startup_check as check
import startup_observation as observation
import writer_session_fence as fence
from package_errors import PackageRecoveryStorageError

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def diagnostic(tmp_path, monkeypatch):
    directory = tmp_path / "start-check"
    directory.mkdir()
    monkeypatch.setattr(observation, "START_CHECK_DIRECTORY", directory)
    monkeypatch.delenv("KMTECH_START_CHECK_TEST_CATEGORY", raising=False)
    identity = {"version": generation.CURRENT_VERSION,
                "build_id": hashlib.sha1(uuid.uuid4().bytes).hexdigest()}
    observation.begin_startup(identity)
    notices = []
    monkeypatch.setattr(observation, "show_startup_blocked_notice", lambda: notices.append(True))
    monkeypatch.setattr(check, "check_startup", lambda *a, **k: pytest.fail("GUI repeated the CLI check"))
    return SimpleNamespace(directory=directory, identity=identity, notices=notices)


def result_file(diagnostic):
    files = list(diagnostic.directory.iterdir())
    assert len(files) == 1 and files[0].name.endswith("-LABEL.json")
    assert len(files[0].stem.split("-")[0]) == 8
    text = files[0].read_text(encoding="utf-8")
    assert len(text.splitlines()) == 1 and text.endswith("\n")
    return json.loads(text)


def test_diagnostic_has_no_admission_network_database_child_or_directory_creation(diagnostic, monkeypatch):
    import socket
    import sqlite3
    def forbidden(*a, **k):
        pytest.fail("diagnostic acquired an authority or added another side effect")
    with monkeypatch.context() as patch:
        patch.setattr(fence, "writer_admission", forbidden)
        patch.setattr(fence, "_acquire_named_mutex", forbidden)
        patch.setattr(socket, "socket", forbidden)
        patch.setattr(sqlite3, "connect", forbidden)
        patch.setattr(subprocess, "Popen", forbidden)
        patch.setattr(Path, "mkdir", forbidden)
        observation.observe_startup(["ACTIVE_WRITER_FENCE"], refused=True)
    result = result_file(diagnostic)
    assert result["category"] == "DEPLOY_FENCE" and diagnostic.notices == [True]
    assert result["build_sha"] == diagnostic.identity["build_id"]
    assert set(result) == set(check.startup_result())
    assert all(word not in json.dumps(result) for word in ("report_path", "operator_sid", "raw_error"))


@pytest.mark.parametrize("category", list(dict.fromkeys(check.REASON_CATEGORIES.values())))
@pytest.mark.parametrize("refused", [False, True])
def test_test_only_category_changes_the_record_never_the_existing_decision(diagnostic, monkeypatch, category, refused):
    monkeypatch.setenv("KMTECH_START_CHECK_TEST_CATEGORY", category)
    observation.observe_startup(refused=refused)
    result = result_file(diagnostic)
    assert result["categories"] == [category]
    assert result["category"] == ("READY" if category == "ALREADY_RUNNING" else category)
    assert result["startable"] is (category == "ALREADY_RUNNING")
    assert diagnostic.notices == ([True] if refused else [])


@pytest.mark.parametrize("failure", ["missing", "open", "replace"])
def test_result_write_failure_is_quiet_and_preserves_existing_result(diagnostic, monkeypatch, failure):
    old = observation.observe_startup()
    target = next(diagnostic.directory.iterdir())
    original = target.read_bytes()
    if failure == "missing":
        target.unlink()
        diagnostic.directory.rmdir()
    elif failure == "open":
        monkeypatch.setattr(Path, "open", lambda *a, **k: (_ for _ in ()).throw(PermissionError()))
    else:
        monkeypatch.setattr(observation.os, "replace", lambda *a, **k: (_ for _ in ()).throw(PermissionError()))
    assert observation.observe_startup() is not None
    assert diagnostic.notices == []
    monkeypatch.undo()
    if failure != "missing":
        assert target.read_bytes() == original
        assert list(diagnostic.directory.iterdir()) == [target]


def test_result_projection_drops_private_fields_and_unknown_reason_text(diagnostic):
    observation.write_startup_result({"reasons": [str(uuid.uuid4())],
                                     "version": str(diagnostic.directory),
                                     "build_sha": str(uuid.uuid4()), "raw_error": str(diagnostic.directory)})
    result = result_file(diagnostic)
    assert result["reasons"] == ["UNKNOWN_CHECK"]
    assert result["version"] == result["build_sha"] == ""


def test_formatter_exception_produces_unknown_without_a_notice(diagnostic, monkeypatch):
    original = observation.startup_result
    attempts = []
    def broken_once(*a, **k):
        if not attempts:
            attempts.append(True)
            raise RuntimeError(str(diagnostic.directory))
        return original(*a, **k)
    monkeypatch.setattr(observation, "startup_result", broken_once)
    observation.observe_startup()
    assert result_file(diagnostic)["category"] == "UNKNOWN"
    assert diagnostic.notices == []


def test_slow_observation_produces_unknown_without_a_notice(diagnostic, monkeypatch):
    clock = iter([0, 11])
    monkeypatch.setattr(observation, "time", SimpleNamespace(monotonic=lambda: next(clock)))
    observation.observe_startup()
    assert result_file(diagnostic)["reasons"] == ["UNKNOWN_SLOW_CHECK"]
    assert diagnostic.notices == []


@pytest.mark.parametrize("notice", [None, PackageRecoveryStorageError])
def test_simple_notice_contains_only_the_fixed_sentence_and_close_button(monkeypatch, notice):
    import tkinter as tk
    labels, buttons, events = [], [], []
    class Parent:
        def __getattr__(self, name):
            return lambda *a, **k: events.append(name)
    parent = Parent()
    class Widget:
        def __init__(self, owner, **kwargs):
            assert owner is parent
            (buttons if "command" in kwargs else labels).append(kwargs)
        def pack(self, **kwargs):
            pass
    monkeypatch.setattr(tk, "Tk", lambda: parent)
    monkeypatch.setattr(tk, "Label", Widget)
    monkeypatch.setattr(tk, "Button", Widget)
    if notice is None:
        observation.show_startup_blocked_notice()
        assert len(labels) == len(buttons) == 1
        assert labels[0]["text"] == observation.STARTUP_BLOCKED_MESSAGE
        assert "\n" not in labels[0]["text"] and buttons[0]["text"] == "닫기"
        assert "mainloop" in events and "destroy" in events
    else:
        observation.show_startup_blocked_notice(notice)
        assert [label["text"] for label in labels] == [
            "작업 기록을 읽지 못했습니다\n잠시 후 프로그램을 다시 켜세요",
            "코드: PACKAGE_RECOVERY_STORAGE_UNAVAILABLE",
            "다시 켜도 같으면 현장 관리자에게 알리세요",
        ]
        assert labels[1]["font"] == "TkSmallCaptionFont"
        assert len(buttons) == 1 and buttons[0]["text"] == "닫기"
        assert "mainloop" in events and "destroy" in events


@pytest.fixture
def baseline_main():
    source = subprocess.check_output(
        ["git", "-c", "core.autocrlf=false", "show", "fdc01a0:Label_Match.py"],
        cwd=ROOT, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0)
    ).decode("utf-8-sig")
    node = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == "main")
    return node


@pytest.mark.parametrize("case,expected", [
    ("ready", 0), ("warning", 0), ("repair", 0), ("account", onboarding.ONBOARDING_EXIT_CODE),
    ("onboard_fence", onboarding.ONBOARDING_EXIT_CODE), ("data", onboarding.ONBOARDING_EXIT_CODE),
    ("data_unknown", onboarding.ONBOARDING_EXIT_CODE), ("catalog", app.ITEM_CATALOG_STARTUP_EXIT_CODE),
    ("application_fence", onboarding.ONBOARDING_EXIT_CODE), ("bootstrap", 0), ("update_account", 20),
])
def test_g0_existing_startup_outcomes_and_predicate_counts_are_identical(
    diagnostic, baseline_main, monkeypatch, tmp_path, case, expected,
):
    runs = []
    for before in (True, False):
        calls, notices = [], []
        namespace = dict(vars(app))
        namespace["__name__"] = "baseline_main" if before else app.__name__
        namespace["release_identity"] = lambda root: calls.append("identity") or diagnostic.identity
        namespace["_label_match_runtime_app_root"] = lambda: tmp_path
        namespace["_label_match_startup_trace"] = lambda *a, **k: None
        namespace["dispatch_product_mode"] = lambda args: None
        namespace["verify_factory_contract_startup"] = lambda: calls.append("factory")
        namespace["_first_run_onboarding_enabled"] = lambda: True
        namespace["requires_bootstrap_integrity"] = lambda root: False
        namespace["_default_label_match_settings_path"] = lambda: tmp_path / "settings.json"
        namespace["resolve_data_scope"] = lambda **k: str(tmp_path)
        namespace["_show_bootstrap_integrity_absent_warning"] = lambda: calls.append("warning")
        namespace["_show_first_run_onboarding_error"] = lambda error: notices.append("account")
        namespace["_show_installation_recovery_required"] = lambda error: notices.append("fence")
        namespace["_show_item_catalog_startup_error"] = lambda error: notices.append("catalog")
        namespace["write_item_catalog_failure_diagnostic"] = lambda *a: None
        def gate(*a, **k):
            calls.append("update")
            return 0 if case == "bootstrap" else 20 if case == "update_account" else None
        monkeypatch.setattr(update, "startup_gate", gate)
        def pending(root):
            calls.append("pending")
            if case == "account":
                raise onboarding.CurrentUserOnboardingError("blocked", report_path=tmp_path,
                                                             cause_code="PENDING_WORKER_SID_MISMATCH")
        namespace["validate_pending_worker_install"] = pending
        def data(root):
            calls.append("data")
            if case.startswith("data"):
                raise generation.DataGenerationError(
                    generation.UNKNOWN_MESSAGE if case == "data_unknown" else "blocked")
        namespace["check_user_data_open"] = data
        def onboard(*a, **k):
            calls.append("onboard")
            if case == "onboard_fence":
                raise fence.WriterFencedError("ACTIVE_WRITER_FENCE", "blocked")
            return {"bootstrap_integrity": {"status": "ABSENT" if case == "warning" else "PASS"}}
        namespace["onboard_current_user"] = onboard
        def application():
            calls.append("application")
            if case == "application_fence":
                raise fence.WriterFencedError("ACTIVE_WRITER_FENCE", "blocked")
            if case == "catalog":
                raise catalog.ItemCatalogSyncError("blocked", cause_code=catalog.REQUEST_FAILED_NO_CACHE)
            return 0
        namespace["_run_label_match_application"] = application
        namespace["run_guarded_entrypoint"] = lambda start, **k: calls.append("instance") or start()
        node = baseline_main if before else next(
            n for n in ast.parse((ROOT / "Label_Match.py").read_text(encoding="utf-8-sig")).body
            if isinstance(n, ast.FunctionDef) and n.name == "main")
        exec(compile(ast.Module(body=[node], type_ignores=[]), "<startup-decision>", "exec"), namespace)
        assert namespace["main"]([]) == expected
        runs.append((calls, notices))
    assert runs[0][0] == runs[1][0], "G0: existing admission predicates or exits changed"


def test_already_running_activates_once_without_starting_or_showing_a_notice(diagnostic):
    activated = []
    lease = SimpleNamespace(owner=False)
    assert instance.run_guarded_entrypoint(
        lambda: pytest.fail("duplicate entered workflow"), data_scope=diagnostic.directory,
        acquire=lambda root: lease,
        activate=lambda: activated.append(True) or instance.ActivationResult(found=True)
    ) == 0
    assert activated == [True] and diagnostic.notices == []
    assert result_file(diagnostic)["categories"] == ["ALREADY_RUNNING"]


@pytest.mark.parametrize("reasons", [["UPDATE_PREPARED"], ["UPDATE_RECOVERY"], ["UPDATE_PHASE"]])
def test_update_handoff_keeps_its_original_result_without_an_extra_check(diagnostic, reasons):
    observation.observe_startup(reasons, identity=diagnostic.identity)
    assert result_file(diagnostic)["category"] == "UPDATE_PENDING"
    assert diagnostic.notices == []


@pytest.mark.parametrize("fault", ["none", "unknown", "exception", "write", "folder", "transition"])
def test_real_pre_gui_flow_continues_after_observation_faults_and_transition(
    diagnostic, monkeypatch, fault,
):
    calls = []
    class Application:
        def __init__(self):
            calls.append("gui")
        def title(self):
            return "Label Match"
        def state(self):
            return "normal"
        def mainloop(self):
            calls.append("mainloop")
        def _finish_after_mainloop(self):
            calls.append("finish")
    def prepare():
        calls.append("catalog")
        if fault == "transition":
            raise catalog.ItemCatalogSyncError("blocked", cause_code=catalog.REQUEST_FAILED_NO_CACHE)
        return str(diagnostic.directory / "Item.csv")
    monkeypatch.setattr(app, "prepare_startup_item_catalog", prepare)
    monkeypatch.setattr(app, "legacy_label_transition_enabled", lambda: fault == "transition")
    monkeypatch.setattr(app, "_start_without_confirmed_item_catalog", lambda error: calls.append("transition"))
    monkeypatch.setattr(app, "get_sanitized_catalog_attempt_context", lambda: {})
    monkeypatch.setattr(app, "write_item_catalog_startup_diagnostic", lambda *a: None)
    monkeypatch.setattr(app, "_show_logistics_profile_warning", lambda context: None)
    monkeypatch.setattr(app, "_label_match_startup_trace", lambda *a, **k: None)
    monkeypatch.setattr(app, "Label_Match", Application)
    monkeypatch.setattr(update, "trial_context", lambda *a: None)
    if fault == "unknown":
        monkeypatch.setenv("KMTECH_START_CHECK_TEST_CATEGORY", "UNKNOWN")
    elif fault == "exception":
        original = observation.startup_result
        attempts = []
        def fail_once(*a, **k):
            if not attempts:
                attempts.append(True)
                raise RuntimeError("synthetic formatter failure")
            return original(*a, **k)
        monkeypatch.setattr(observation, "startup_result", fail_once)
    elif fault == "folder":
        diagnostic.directory.rmdir()
    elif fault == "write":
        monkeypatch.setattr(observation.os, "replace", lambda *a, **k: (_ for _ in ()).throw(PermissionError()))
    assert app._run_label_match_application() == 0
    assert calls == (["catalog", "transition", "gui", "mainloop", "finish"] if fault == "transition"
                     else ["catalog", "gui", "mainloop", "finish"])
    assert diagnostic.notices == []
    if fault not in {"write", "folder"}:
        assert result_file(diagnostic)["category"] == ("UNKNOWN" if fault in {"unknown", "exception"} else "READY")


@pytest.mark.parametrize("cause,category", [
    ("PENDING_WORKER_SID_MISMATCH", "ACCOUNT_MISMATCH"),
    ("PENDING_WORKER_INSTALL_INVALID", "ACCOUNT_MISMATCH"),
    ("BOOTSTRAP_INTEGRITY_INVALID", "INSTALL_MISMATCH"),
])
def test_existing_account_refusal_hook_records_and_uses_the_simple_notice(diagnostic, cause, category):
    error = onboarding.CurrentUserOnboardingError("private detail", report_path=diagnostic.directory,
                                                 cause_code=cause)
    error._startup_check_refusal = True
    app._show_first_run_onboarding_error(error)
    assert result_file(diagnostic)["category"] == category
    assert diagnostic.notices == [True]


def test_catalog_retry_success_preserves_startup_without_a_refusal_notice(diagnostic, monkeypatch):
    calls = []

    def prepare():
        calls.append("catalog")
        if calls.count("catalog") == 1:
            raise catalog.ItemCatalogSyncError("unavailable", cause_code=catalog.REQUEST_FAILED_NO_CACHE)
        return None

    monkeypatch.setattr(app, "prepare_startup_item_catalog", prepare)
    monkeypatch.setattr(app, "legacy_label_transition_enabled", lambda: False)
    monkeypatch.setattr(app, "_offer_item_catalog_startup_retry", lambda error: calls.append("retry") or True)
    monkeypatch.setattr(app, "_label_match_startup_trace", lambda *a, **k: None)
    monkeypatch.setattr(update, "trial_context", lambda *a: None)
    application = SimpleNamespace(title=lambda: "", state=lambda: "normal", mainloop=lambda: None,
                                  _finish_after_mainloop=lambda: None)
    monkeypatch.setattr(app, "Label_Match", lambda: calls.append("app") or application)
    assert app._run_label_match_application() == 0
    assert calls == ["catalog", "retry", "catalog", "app"]
    assert diagnostic.notices == []
    assert result_file(diagnostic)["category"] == "READY"


def test_result_write_failure_still_shows_only_the_existing_refusal(diagnostic, monkeypatch):
    monkeypatch.setattr(observation, "write_startup_result", lambda result: (_ for _ in ()).throw(PermissionError()))
    observation.observe_startup(["DATA_GENERATION_REFUSED"], refused=True)
    assert diagnostic.notices == [True]
    assert list(diagnostic.directory.iterdir()) == []


@pytest.mark.parametrize("case,category,expected", [
    ("prepared", "UPDATE_PENDING", 0), ("recovery", "UPDATE_PENDING", 0),
    ("phase", "UPDATE_PENDING", 0), ("build", "INSTALL_MISMATCH", 0),
    ("account", "ACCOUNT_MISMATCH", 20), ("ready", "READY", None),
])
def test_actual_update_gate_reports_after_ownership_and_keeps_bootstrap(
    diagnostic, monkeypatch, tmp_path, case, category, expected,
):
    from contextlib import contextmanager
    held, predicates, launches = [], [], []
    control = tmp_path / "update-control"
    state = {"phase": "FAILED" if case == "phase" else "CONFIRMED",
             "recovery_pending": case == "recovery"}
    monkeypatch.delenv("KMTECH_LM_UPDATE_DEFERRED_AT", raising=False)
    monkeypatch.setattr(update, "control_root", lambda root: control)
    monkeypatch.setattr(update, "load_policy", lambda root: {"operator_sid": ""})
    monkeypatch.setattr(update, "ordinary_user_context", lambda *a: case != "account")
    @contextmanager
    def lock(*a):
        held.append(True)
        try:
            yield
        finally:
            held.pop()
    monkeypatch.setattr(update, "update_lock", lock)
    monkeypatch.setattr(update, "optional_record", lambda path: state)
    monkeypatch.setattr(update, "read_json", lambda path: {"sha": "b" * 40 if case == "build"
                                                        else diagnostic.identity["build_id"]})
    monkeypatch.setattr(update, "version_manifest", lambda root: {
        "source_commit": diagnostic.identity["build_id"], "version": generation.CURRENT_VERSION})
    monkeypatch.setattr(update, "prepared_pending", lambda *a: case == "prepared")
    original = update.startup_bootstrap_reason
    def predicate(*a):
        predicates.append(True)
        return original(*a)
    monkeypatch.setattr(update, "startup_bootstrap_reason", predicate)
    def launch(*a, **k):
        assert not held
        launches.append(True)
    monkeypatch.setattr(update, "request_bootstrap", launch)
    real_observe = observation.observe_startup
    def observe(*a, **k):
        assert not held, "diagnostic wrote while update ownership was held"
        return real_observe(*a, **k)
    monkeypatch.setattr(observation, "observe_startup", observe)
    assert update.startup_gate(tmp_path / "app", []) == expected
    assert len(predicates) == (0 if case == "account" else 1)
    assert launches == ([] if case in {"account", "ready"} else [True])
    assert diagnostic.notices == ([True] if case == "account" else [])
    if case != "ready":
        assert result_file(diagnostic)["category"] == category

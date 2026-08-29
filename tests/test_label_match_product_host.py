import json
from pathlib import Path
import sys

import pytest

import current_user_onboarding
import label_match_product_host as product_host
from tools import direct_sync_relay_runner
import user_relay


def test_main_executable_dispatches_product_modes_before_gui_import():
    source = (Path(__file__).resolve().parents[1] / "Label_Match.py").read_text(
        encoding="utf-8"
    )

    assert source.index(
        "_early_product_mode_result = dispatch_product_mode"
    ) < source.index("import tkinter as tk")


def test_non_product_arguments_continue_to_gui_startup():
    assert product_host.dispatch_product_mode([]) is None
    assert product_host.dispatch_product_mode(["--ordinary-app-argument"]) is None


def test_relay_mode_reuses_main_onedir_process_and_forwards_arguments(monkeypatch):
    observed = []
    monkeypatch.setattr(
        direct_sync_relay_runner,
        "main",
        lambda arguments: observed.append(list(arguments)) or 17,
    )

    result = product_host.dispatch_product_mode(
        [product_host.DIRECT_SYNC_RELAY_MODE, "--db-path", "queue.sqlite3"]
    )

    assert result == 17
    assert observed == [["--db-path", "queue.sqlite3"]]


def test_frozen_hosted_relay_fails_closed_before_runtime_on_integrity_error(
    monkeypatch,
):
    observed = []
    monkeypatch.setattr(
        product_host,
        "_verify_frozen_host_integrity",
        lambda: (_ for _ in ()).throw(ValueError("tampered")),
    )
    monkeypatch.setattr(
        direct_sync_relay_runner,
        "main",
        lambda arguments: observed.append(list(arguments)) or 0,
    )

    result = product_host.dispatch_product_mode([product_host.DIRECT_SYNC_RELAY_MODE])

    assert result == product_host.HOSTED_RELAY_FAILURE_EXIT_CODE
    assert observed == []


def test_frozen_hosted_relay_warns_and_continues_when_record_is_absent(
    monkeypatch, capsys, tmp_path
):
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "LocalAppData"))
    monkeypatch.setattr(
        product_host,
        "_verify_frozen_host_integrity",
        lambda: {"status": "ABSENT"},
    )
    monkeypatch.setattr(direct_sync_relay_runner, "main", lambda _arguments: 0)

    assert (
        product_host.dispatch_product_mode([product_host.DIRECT_SYNC_RELAY_MODE]) == 0
    )
    assert "integrity record is absent" in capsys.readouterr().err
    warning_path = (
        tmp_path
        / "LocalAppData"
        / "KMTech"
        / "DirectSync"
        / "label_match"
        / "status"
        / product_host.BOOTSTRAP_INTEGRITY_WARNING_FILENAME
    )
    warning = json.loads(warning_path.read_text(encoding="utf-8"))
    assert warning["warning_code"] == "bootstrap_integrity_absent"


def test_windowed_host_persists_absent_integrity_warning_without_stderr(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "LocalAppData"))
    monkeypatch.setattr(
        product_host,
        "_verify_frozen_host_integrity",
        lambda: {"status": "ABSENT"},
    )
    monkeypatch.setattr(direct_sync_relay_runner, "main", lambda _arguments: 0)
    monkeypatch.setattr(sys, "stderr", None)

    assert (
        product_host.dispatch_product_mode([product_host.DIRECT_SYNC_RELAY_MODE]) == 0
    )

    warning_path = (
        tmp_path
        / "LocalAppData"
        / "KMTech"
        / "DirectSync"
        / "label_match"
        / "status"
        / product_host.BOOTSTRAP_INTEGRITY_WARNING_FILENAME
    )
    assert json.loads(warning_path.read_text(encoding="utf-8"))["status"] == "warning"
    assert sys.stderr is None


def test_windowed_host_supplies_and_restores_output_streams(monkeypatch):
    observed = []

    def fake_main(arguments):
        observed.append((sys.stdout is not None, sys.stderr is not None, arguments))
        return 0

    monkeypatch.setattr(direct_sync_relay_runner, "main", fake_main)
    monkeypatch.setattr(sys, "stdout", None)
    monkeypatch.setattr(sys, "stderr", None)

    assert (
        product_host.dispatch_product_mode([product_host.DIRECT_SYNC_RELAY_MODE]) == 0
    )
    assert observed == [(True, True, [])]
    assert sys.stdout is None
    assert sys.stderr is None


def test_relay_exception_becomes_bounded_redacted_durable_diagnostic(
    monkeypatch, tmp_path
):
    runtime_status_path = tmp_path / "status" / "runtime.json"
    log_path = tmp_path / "logs" / "runtime.jsonl"
    secret_text = "secret=must-not-be-persisted"

    def fail(_arguments):
        raise RuntimeError(secret_text)

    monkeypatch.setattr(direct_sync_relay_runner, "main", fail)
    result = product_host.dispatch_product_mode(
        [
            product_host.DIRECT_SYNC_RELAY_MODE,
            "--runtime-status-path",
            str(runtime_status_path),
            "--log-path",
            str(log_path),
            "--worker-id",
            "relay-worker",
        ]
    )

    assert result == product_host.HOSTED_RELAY_FAILURE_EXIT_CODE
    status = json.loads(runtime_status_path.read_text(encoding="utf-8"))
    event = json.loads(log_path.read_text(encoding="utf-8"))
    assert status["status"] == "runtime_error"
    assert status["error_code"] == "hosted_relay_unhandled_exception"
    assert event["error_type"] == "RuntimeError"
    assert secret_text not in runtime_status_path.read_text(encoding="utf-8")
    assert secret_text not in log_path.read_text(encoding="utf-8")
    assert runtime_status_path.stat().st_size < 16 * 1024
    assert log_path.stat().st_size < 16 * 1024


def test_relay_mode_preserves_system_exit_semantics(monkeypatch):
    monkeypatch.setattr(
        direct_sync_relay_runner,
        "main",
        lambda _arguments: (_ for _ in ()).throw(SystemExit(7)),
    )
    with pytest.raises(SystemExit) as caught:
        product_host.dispatch_product_mode([product_host.DIRECT_SYNC_RELAY_MODE])
    assert caught.value.code == 7


def test_user_relay_onboarding_and_removal_modes_dispatch_in_process(monkeypatch):
    observed = []
    monkeypatch.setattr(
        user_relay,
        "main",
        lambda arguments: observed.append(("relay", list(arguments))) or 0,
    )
    monkeypatch.setattr(
        user_relay,
        "scheduled_main",
        lambda arguments: observed.append(("scheduled", list(arguments))) or 0,
    )
    monkeypatch.setattr(
        current_user_onboarding,
        "onboarding_main",
        lambda arguments: observed.append(("onboard", list(arguments))) or 0,
    )
    monkeypatch.setattr(
        current_user_onboarding,
        "removal_main",
        lambda arguments: observed.append(("remove", list(arguments))) or 0,
    )

    assert (
        product_host.dispatch_product_mode([product_host.USER_RELAY_MODE, "--once"])
        == 0
    )
    assert (
        product_host.dispatch_product_mode(
            [product_host.SCHEDULED_RELAY_MODE, "--app-root", "app"]
        )
        == 0
    )
    assert (
        product_host.dispatch_product_mode(
            [product_host.ONBOARD_CURRENT_USER_MODE, "--app-root", "app"]
        )
        == 0
    )
    assert (
        product_host.dispatch_product_mode(
            [product_host.REMOVE_CURRENT_USER_MODE, "--app-root", "app"]
        )
        == 0
    )
    assert observed == [
        ("relay", ["--once"]),
        ("scheduled", ["--app-root", "app"]),
        ("onboard", ["--app-root", "app"]),
        ("remove", ["--app-root", "app"]),
    ]

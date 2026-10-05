import json
from tests.test_current_user_onboarding import isolated_data_generation_release  # noqa: F401
from pathlib import Path
import sys

import pytest

import current_user_onboarding
import label_match_product_host as product_host
from tools import direct_sync_relay_runner
import user_relay


@pytest.fixture
def raw_arguments(tmp_path):
    root = tmp_path / "relay-inputs"
    return [item for option, relative in (
        ("--db-path", "queue/relay.sqlite3"), ("--spool-dir", "spool"),
        ("--producer-manifest-path", "producer_manifest.json"),
        ("--credential-path", "credential.json"), ("--upload-status-dir", "upload_status"),
        ("--runtime-status-path", "status/raw.json"), ("--log-path", "logs/raw.jsonl"),
    ) for item in (option, str(root / relative))]


def test_portable_default_product_root_is_the_release_parent(monkeypatch, tmp_path):
    release = tmp_path / "portable"
    app = release / "app"
    (release / "runtime").mkdir(parents=True)
    app.mkdir()
    (release / "runtime" / "pythonw.exe").write_bytes(b"runtime")
    (release / "portable-manifest.json").write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(product_host, "__file__", str(app / "host.py"))

    assert product_host._default_product_root() == release.resolve()


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


def test_relay_mode_reuses_main_onedir_process_and_forwards_arguments(monkeypatch, raw_arguments):
    observed = []
    monkeypatch.setattr(
        direct_sync_relay_runner,
        "main",
        lambda arguments, **_prepared_inputs: observed.append(list(arguments)) or 17,
    )

    result = product_host.dispatch_product_mode(
        [product_host.DIRECT_SYNC_RELAY_MODE, *raw_arguments, "--db-path", "queue.sqlite3"]
    )

    assert result == 17
    assert observed == [[*raw_arguments, "--db-path", "queue.sqlite3"]]


def test_frozen_hosted_relay_fails_closed_before_runtime_on_integrity_error(
    monkeypatch, raw_arguments,
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

    result = product_host.dispatch_product_mode([product_host.DIRECT_SYNC_RELAY_MODE, *raw_arguments])

    assert result == product_host.HOSTED_RELAY_FAILURE_EXIT_CODE
    assert observed == []


def test_frozen_hosted_relay_warns_and_continues_when_record_is_absent(
    monkeypatch, capsys, tmp_path, raw_arguments
):
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "LocalAppData"))
    monkeypatch.delenv("LABEL_MATCH_DIRECT_SYNC_ROOT", raising=False)
    monkeypatch.delenv("LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT", raising=False)
    monkeypatch.setattr(
        product_host,
        "_verify_frozen_host_integrity",
        lambda: {"status": "ABSENT"},
    )
    monkeypatch.setattr(direct_sync_relay_runner, "main", lambda _arguments, **_prepared_inputs: 0)

    assert (
        product_host.dispatch_product_mode([product_host.DIRECT_SYNC_RELAY_MODE, *raw_arguments]) == 0
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
    monkeypatch, tmp_path, raw_arguments
):
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "LocalAppData"))
    monkeypatch.delenv("LABEL_MATCH_DIRECT_SYNC_ROOT", raising=False)
    monkeypatch.delenv("LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT", raising=False)
    monkeypatch.setattr(
        product_host,
        "_verify_frozen_host_integrity",
        lambda: {"status": "ABSENT"},
    )
    monkeypatch.setattr(direct_sync_relay_runner, "main", lambda _arguments, **_prepared_inputs: 0)
    monkeypatch.setattr(sys, "stderr", None)

    assert (
        product_host.dispatch_product_mode([product_host.DIRECT_SYNC_RELAY_MODE, *raw_arguments]) == 0
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


def test_windowed_host_supplies_and_restores_output_streams(monkeypatch, raw_arguments):
    observed = []

    def fake_main(arguments, **_prepared_inputs):
        observed.append((sys.stdout is not None, sys.stderr is not None, arguments))
        return 0

    monkeypatch.setattr(direct_sync_relay_runner, "main", fake_main)
    monkeypatch.setattr(sys, "stdout", None)
    monkeypatch.setattr(sys, "stderr", None)

    assert (
        product_host.dispatch_product_mode([product_host.DIRECT_SYNC_RELAY_MODE, *raw_arguments]) == 0
    )
    assert observed == [(True, True, raw_arguments)]
    assert sys.stdout is None
    assert sys.stderr is None


def test_relay_exception_becomes_bounded_redacted_durable_diagnostic(
    monkeypatch, tmp_path, raw_arguments
):
    runtime_status_path = tmp_path / "status" / "runtime.json"
    log_path = tmp_path / "logs" / "runtime.jsonl"
    secret_text = "secret=must-not-be-persisted"

    def fail(_arguments, **_prepared_inputs):
        raise RuntimeError(secret_text)

    monkeypatch.setattr(direct_sync_relay_runner, "main", fail)
    result = product_host.dispatch_product_mode(
        [
            product_host.DIRECT_SYNC_RELAY_MODE,
            *raw_arguments,
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


def test_relay_mode_preserves_system_exit_semantics(monkeypatch, raw_arguments):
    monkeypatch.setattr(
        direct_sync_relay_runner,
        "main",
        lambda _arguments, **_prepared_inputs: (_ for _ in ()).throw(SystemExit(7)),
    )
    with pytest.raises(SystemExit) as caught:
        product_host.dispatch_product_mode([product_host.DIRECT_SYNC_RELAY_MODE, *raw_arguments])
    assert caught.value.code == 7


def test_user_relay_onboarding_and_removal_modes_dispatch_in_process(monkeypatch):
    observed = []
    monkeypatch.setattr(
        user_relay,
        "main",
        lambda arguments, **_prepared_inputs: observed.append(("relay", list(arguments))) or 0,
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
        ("onboard", ["--app-root", "app"]),
        ("remove", ["--app-root", "app"]),
    ]


def test_retired_scheduled_argv_never_dispatches_a_relay_or_falls_through_to_gui(monkeypatch):
    def unexpected(*_args, **_kwargs):
        raise AssertionError("Retired task started an active product path")
    monkeypatch.setattr(user_relay, "main", unexpected)
    monkeypatch.setattr(product_host, "_verify_frozen_host_integrity", unexpected)
    assert product_host.dispatch_product_mode([
        product_host.RETIRED_SCHEDULED_RELAY_MODE, "--app-root", "old-task-root",
    ]) == 2
    assert product_host.RETIRED_SCHEDULED_RELAY_MODE not in product_host.PRODUCT_MODES

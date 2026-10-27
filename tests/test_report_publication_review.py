import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest
import report_publication as publication


def refused(value):
    raise ValueError("REPORT_UNAVAILABLE")


@pytest.mark.parametrize("error_type", [RuntimeError, ValueError])
def test_counter_failure_never_changes_report_result(tmp_path, error_type):
    target = tmp_path / "diagnostic.json"
    def fail_counter(path, value):
        raise error_type("synthetic-counter-failure")
    prepared = publication.prepare_report({"detail": "synthetic-diagnostic"},
        render=refused, fallback_render=json.dumps, unavailable_codes=("REPORT_UNAVAILABLE",))
    assert publication.failure_count(target) == 0
    target.write_text(prepared.text, encoding="utf-8")
    publication.record_publication(prepared, path=target, writer=fail_counter)
    assert prepared.unavailable
    assert publication.failure_count(target) == 1


def test_counter_writer_runs_outside_count_lock(tmp_path):
    ownership = []
    target = tmp_path / "diagnostic.json"
    prepared = publication.prepare_report({}, render=refused, fallback_render=json.dumps,
        unavailable_codes=("REPORT_UNAVAILABLE",))
    assert ownership == [] and publication.failure_count(target) == 0
    target.write_text(prepared.text, encoding="utf-8")
    publication.record_publication(prepared, path=target,
        writer=lambda path, value: ownership.append(publication._LOCK._is_owned()))
    assert ownership == [False]


def test_directory_counter_does_not_accumulate_process_files(tmp_path, monkeypatch):
    def write_counter(path, value):
        path.write_text(json.dumps(value), encoding="utf-8")
    target = tmp_path / "diagnostic.json"
    for process in (31001, 31002):
        with monkeypatch.context() as change:
            change.setattr(os, "getpid", lambda: process)
            prepared = publication.prepare_report({}, render=refused, fallback_render=json.dumps,
                unavailable_codes=("REPORT_UNAVAILABLE",))
            target.write_text(prepared.text, encoding="utf-8")
            publication.record_publication(prepared, path=target, writer=write_counter)
    files = list(tmp_path.glob(".report-unavailable-count*.json"))
    assert len(files) == 1
    assert files[0].name == ".report-unavailable-count.json"
    assert json.loads(files[0].read_text(encoding="utf-8"))["count"] == 2


def test_empty_and_nonstring_diagnostics_keep_their_types():
    value = {"failure": None, "message": 0, "stdout": False, "stderr": [],
             "detail": {}, "stdout_tail": "", "stderr_tail": "synthetic-diagnostic"}
    prepared = publication.prepare_report(value, render=refused,
        fallback_render=json.dumps, diagnostic_fields=publication.DIAGNOSTIC_FIELDS,
        unavailable_codes=("REPORT_UNAVAILABLE",))
    result = json.loads(prepared.text)
    assert result == {**value, "stderr_tail": publication.UNAVAILABLE}


def test_portable_isolated_auto_update_entry_starts(tmp_path):
    app = tmp_path / "portable" / "app"
    from tools import build_portable_release_candidate as builder
    repo = Path(__file__).resolve().parents[1]
    builder._copy_application(repo, app)
    result = subprocess.run([sys.executable, "-I", "-B", str(app / "auto_update.py"), "--help"],
        cwd=tmp_path, capture_output=True, timeout=60, check=False)
    assert result.returncode == 0, (result.returncode, b"ModuleNotFoundError" in result.stderr)


@pytest.mark.parametrize("field", ["hostname", "endpoint_url", "server_base_url"])
def test_registration_receipt_machine_collision_keeps_registered_state(tmp_path, monkeypatch, field):
    from tools import register_label_match_worker_pc as registration
    synthetic = "synthetic-registration-machine-collision"
    monkeypatch.setenv("KMTECH_TEST_API_TOKEN", synthetic)
    path = tmp_path / "registration.json"
    payload = {"status": "REGISTERED", "server_registration_verified": True,
               "base_url": "https://example.invalid", field: synthetic}
    registration._write_registration_report(path, payload)
    result = json.loads(path.read_text(encoding="utf-8"))
    assert result == payload


def test_fallback_keeps_public_projection_and_anonymous_diagnostics(tmp_path, monkeypatch):
    import current_user_onboarding as onboarding
    monkeypatch.setenv("KMTECH_TEST_API_TOKEN", "4826")
    payload = {"status": "FAILED", "cause_code": "SUPPORT_FAILURE",
               "server_registration_verified": True, "base_url": "https://example.invalid",
               "autostart": {"message": 4826},
               "blocked_reason": "synthetic 4826 diagnostic",
               "failure_traceback": "trace [REDACTED:KMTECH_TEST_API_TOKEN]"}
    payload.update(error="synthetic 4826 diagnostic", reason="synthetic 4826 diagnostic",
                   message_ko="synthetic 4826 diagnostic", server_error_code="synthetic 4826 diagnostic",
                   exception_chain="synthetic 4826 diagnostic")
    target = tmp_path / "current_user_onboarding.json"
    with onboarding.writer_admission(onboarding.ONBOARDING_WRITER_SOURCE):
        onboarding._write_json_atomic(target, payload)
    raw = target.read_text(encoding="utf-8")
    result = json.loads(raw)
    assert result["status"] == "FAILED"
    assert result["cause_code"] == "SUPPORT_FAILURE"
    assert result["server_registration_verified"] is True
    assert result["base_url"] == payload["base_url"]
    assert "4826" not in raw and "KMTECH_TEST_API_TOKEN" not in raw
    assert publication.failure_count(target) == 1


def test_removal_diagnostic_refusal_counts_after_path_publication(tmp_path, monkeypatch):
    import failure_redaction
    import current_user_onboarding as onboarding
    target = tmp_path / "current_user_removal.json"
    with publication.report_attempt():
        with monkeypatch.context() as change:
            change.setattr(failure_redaction, "redact_for_sink", lambda *args, **kwargs: refused(None))
            result = failure_redaction.protected_failure_details(RuntimeError("synthetic-original"), _report_path=target)
        assert result == {"failure_traceback": publication.UNAVAILABLE}
        assert publication.failure_count(target) == 0
        with onboarding.writer_admission(onboarding.ONBOARDING_WRITER_SOURCE):
            onboarding._write_json_atomic(target, {"status": "FAILED", **result})
    assert publication.failure_count(target) == 1
    assert json.loads((tmp_path / ".report-unavailable-count.json").read_text(encoding="utf-8"))["count"] == 1


def test_actual_label_metadata_refusal_is_handled_at_publication(tmp_path, monkeypatch):
    import current_user_onboarding as onboarding
    import failure_redaction
    synthetic = "synthetic-label-metadata-collision"
    monkeypatch.setenv("KMTECH_TEST_API_TOKEN", synthetic)
    original = failure_redaction.redact_json_for_sink
    def metadata_first(value, *args, **kwargs):
        failure_redaction.assert_machine_metadata_text(json.dumps(value), environ=os.environ)
        return original(value, *args, **kwargs)
    monkeypatch.setattr(failure_redaction, "redact_json_for_sink", metadata_first)
    target = tmp_path / "current_user_onboarding.json"
    with onboarding.writer_admission(onboarding.ONBOARDING_WRITER_SOURCE):
        onboarding._write_json_atomic(target, {"status": "READY", "detail": synthetic})
    result = json.loads(target.read_text(encoding="utf-8"))
    assert result == {"status": "READY", "detail": publication.UNAVAILABLE}
    assert publication.failure_count(target) == 1


def test_fallback_keeps_redacted_mandatory_machine_string(tmp_path, monkeypatch):
    import current_user_onboarding as onboarding
    monkeypatch.setenv("KMTECH_TEST_API_TOKEN", "4826")
    target = tmp_path / "current_user_onboarding.json"
    target.write_bytes(b'{"status":"READY","invocation":"prior"}\n')
    with onboarding.writer_admission(onboarding.ONBOARDING_WRITER_SOURCE):
        onboarding._write_json_atomic(target, {"status": "READY", "message": 4826, "hostname": "4826"})
    raw = target.read_text(encoding="utf-8")
    assert json.loads(raw) == {"status": "READY", "message": publication.UNAVAILABLE, "hostname": publication.UNAVAILABLE}
    assert "4826" not in raw and "KMTECH_TEST_API_TOKEN" not in raw
    assert publication.failure_count(target) == 1

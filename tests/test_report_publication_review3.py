import json

import pytest
import report_publication as publication
from tests.test_current_user_onboarding import isolated_data_generation_release


def publish(target, payload):
    import current_user_onboarding as onboarding
    with onboarding.writer_admission(onboarding.ONBOARDING_WRITER_SOURCE):
        return onboarding._write_json_atomic(target, payload)


def test_ready_fallback_preserves_redacted_machine_strings(tmp_path, monkeypatch):
    monkeypatch.setenv("KMTECH_TEST_API_TOKEN", "4826")
    target = tmp_path / "current_user_onboarding.json"
    payload = {"status": "READY", "message": 4826, "hostname": "station-4826",
               "profile_path": "C:/profiles/4826/data", "updated_at": "2026-10-27T12:13:14.4826Z"}
    publish(target, payload)
    raw = target.read_text(encoding="utf-8")
    result = json.loads(raw)
    assert result["status"] == "READY" and result["message"] == publication.UNAVAILABLE
    assert all(key in result for key in payload)
    assert "4826" not in raw and "KMTECH_TEST_API_TOKEN" not in raw
    assert result["hostname"].startswith("station-") and result["profile_path"].startswith("C:/profiles/")


def test_fallback_preserves_nested_diagnostic_siblings(tmp_path, monkeypatch):
    monkeypatch.setenv("KMTECH_TEST_API_TOKEN", "4826")
    target = tmp_path / "current_user_onboarding.json"
    nested = {"stdout": {"message": "benign diagnostic", "items": [1, 2]}, "sibling": {"quantity": 3}}
    publish(target, {"status": "READY", "message": 4826, "nested": nested})
    result = json.loads(target.read_text(encoding="utf-8"))
    assert result["nested"] == nested


def test_failed_publication_is_not_counted(tmp_path, monkeypatch):
    import current_user_onboarding as onboarding
    monkeypatch.setenv("KMTECH_TEST_API_TOKEN", "4826")
    target = tmp_path / "current_user_onboarding.json"
    def refused(*args):
        raise OSError("synthetic-publish-failure")
    monkeypatch.setattr(onboarding, "_write_json_bytes_atomic", refused)
    with pytest.raises(OSError):
        publish(target, {"status": "READY", "message": 4826})
    assert publication.failure_count(target) == 0
    assert not (tmp_path / ".report-unavailable-count.json").exists()


def test_publication_adds_to_persisted_counter(tmp_path, monkeypatch):
    monkeypatch.setenv("KMTECH_TEST_API_TOKEN", "4826")
    counter = tmp_path / ".report-unavailable-count.json"
    counter.write_text('{"count":7}', encoding="utf-8")
    for expected in (8, 9):
        publish(tmp_path / "current_user_onboarding.json", {"status": "READY", "message": 4826})
        assert json.loads(counter.read_text(encoding="utf-8"))["count"] == expected


def test_counter_accumulates_across_fresh_processes(tmp_path):
    from pathlib import Path
    import subprocess
    import sys
    counter = tmp_path / ".report-unavailable-count.json"
    counter.write_text('{"count":7}', encoding="utf-8")
    child = tmp_path / "publish_child.py"
    child.write_text("""import json, os, sys, uuid
from pathlib import Path
repo, directory = map(Path, sys.argv[1:])
sys.path[:0] = ['E:/KMTech/shared-tools/prodblock', str(repo)]
import sitecustomize
import report_publication as publication
def refused(value):
    raise ValueError('REPORT_UNAVAILABLE')
def writer(path, value):
    temporary = path.with_name(uuid.uuid4().hex + '.tmp')
    temporary.write_text(json.dumps(value), encoding='utf-8')
    os.replace(temporary, path)
prepared = publication.prepare_report({}, render=refused, fallback_render=json.dumps,
    unavailable_codes=('REPORT_UNAVAILABLE',))
target = directory / 'diagnostic.json'
target.write_text(prepared.text, encoding='utf-8')
publication.record_publication(prepared, path=target, writer=writer)
""", encoding="utf-8")
    for expected in (8, 9):
        result = subprocess.run([sys.executable, "-I", "-B", str(child),
            str(Path(__file__).resolve().parents[1]), str(tmp_path)], capture_output=True,
            timeout=60, creationflags=subprocess.CREATE_NO_WINDOW)
        assert result.returncode == 0
        assert json.loads(counter.read_text(encoding="utf-8"))["count"] == expected


def test_concurrent_publications_add_to_existing_counter(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    import os
    import uuid
    counter = tmp_path / ".report-unavailable-count.json"
    counter.write_text('{"count":7}', encoding="utf-8")
    def writer(path, value):
        temporary = path.with_name(uuid.uuid4().hex + ".tmp")
        temporary.write_text(json.dumps(value), encoding="utf-8")
        os.replace(temporary, path)
    def publish_one(index):
        target = tmp_path / (str(index) + ".json")
        prepared = publication.PreparedReport('{"message":"보고 못 씀"}', True)
        target.write_text(prepared.text, encoding="utf-8")
        publication.record_publication(prepared, path=target, writer=writer)
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(publish_one, (1, 2)))
    assert json.loads(counter.read_text(encoding="utf-8"))["count"] == 9


def test_actual_removal_stage_refusal_has_one_file_count(tmp_path, monkeypatch):
    import current_user_onboarding as onboarding
    import failure_redaction
    from tests import test_current_user_onboarding as support
    original = failure_redaction.protected_failure_details
    paths_seen = []
    def unavailable_details(error, environ=None, *, _report_path=None):
        paths_seen.append(_report_path)
        def refused(*args, **kwargs):
            raise ValueError("REPORT_UNAVAILABLE")
        with monkeypatch.context() as change:
            change.setattr(failure_redaction, "redact_for_sink", refused)
            return original(error, environ, _report_path=_report_path)
    monkeypatch.setattr(failure_redaction, "protected_failure_details", unavailable_details)
    support.test_public_remove_does_not_downgrade_unknown_relay_result(tmp_path)
    paths = onboarding.resolve_current_user_onboarding_paths(tmp_path / "app", environ=support._environment(tmp_path))
    target = paths.removal_report_path
    assert len(paths_seen) >= 2 and all(path == target for path in paths_seen)
    assert json.loads(target.read_text(encoding="utf-8"))["status"] == "UNKNOWN"
    assert publication.failure_count(target) == 1
    assert json.loads((target.parent / ".report-unavailable-count.json").read_text(encoding="utf-8"))["count"] == 1

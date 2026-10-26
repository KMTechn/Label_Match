"""Source validation retries transient HTTP errors through its durable consumer."""

from datetime import timedelta
from email.message import Message
from io import BytesIO
from unittest.mock import Mock
from urllib.error import HTTPError

import pytest

import package_logistics as logistics
from tests.test_deferred_intent_capture import _row
from tests.test_deferred_lease_clock_retry import _first_scan, clock_case  # noqa: F401


@pytest.mark.parametrize("status_code,body", [
    (408, b'{}'), (429, b'{}'), (500, b'{}'), (501, b'{}'),
    (520, b'{}'), (520, b'<html>temporarily unavailable</html>'), (599, b'{}'),
], ids=["408", "429", "500", "501", "520-json", "520-html", "599"])
def test_source_http_failure_recovers_without_new_capture(clock_case, monkeypatch, status_code, body):
    case = clock_case
    client = case.app.package_logistics_client
    resolve = client.resolve_package_source_evidence

    def unavailable(*args, **kwargs):
        raise HTTPError("https://fixture.invalid/source", status_code, "fixture unavailable",
                        Message(), BytesIO(body))

    monkeypatch.setattr(logistics, "urlopen", unavailable)
    client.resolve_package_source_evidence = lambda draft: logistics._default_transport(
        "GET", "https://fixture.invalid/source", {}, None, 1.0,
    )
    intent_id = _first_scan(case)
    before = _row(case.database, intent_id)
    assert before["state"] == "RETRY_WAIT_VALIDATION"
    assert case.calls == []
    assert case.accepted == []

    client.resolve_package_source_evidence = resolve
    case.instant[0] += timedelta(seconds=10)
    claim = case.app._prepare_deferred_intent_validation(intent_id)
    result = case.app._execute_deferred_label_validation(claim)
    assert result.state == "VALIDATED"
    assert case.app._materialize_validated_deferred_label(result) is True
    after = _row(case.database, intent_id)
    assert after["intent_id"] == before["intent_id"]
    assert case.calls == []  # Read-only admission never issues a completion lease.


@pytest.mark.parametrize("status_code,code", [
    (409, "VERSION_CONFLICT"), (412, "VERSION_CONFLICT"),
    (520, "RESIDUAL_TAG_NOT_ALLOWED"),
])
def test_source_definite_refusals_still_block(clock_case, status_code, code):
    case = clock_case
    case.app.package_logistics_client.resolve_package_source_evidence = Mock(
        side_effect=logistics.PackageApiError(status_code, code, "fixture refusal"),
    )
    intent_id = _first_scan(case)
    assert _row(case.database, intent_id)["state"] == "BLOCKED_INVALID"
    assert case.calls == case.accepted == []


@pytest.mark.parametrize("committed,outcome", [(None, "UNKNOWN_COMMIT"), (True, "CONFLICT")])
def test_mutating_validation_commit_precedence_survives_520(clock_case, committed, outcome):
    result = clock_case.app._classify_deferred_validation_error(
        logistics.PackageApiError(520, "SERVER_BUSY", "fixture unavailable", committed=committed),
        step_id="label-operation-lease",
        dispatch_record={"idempotency_key": "fixture-command", "request_hash": "a" * 64,
                         "recorded_at": "2026-09-05T03:28:14Z"},
    )
    assert result["outcome"] == outcome

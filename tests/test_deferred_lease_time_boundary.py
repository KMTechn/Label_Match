"""Datetime bounds must use the ordinary deferred-artifact refusal path."""
import copy

import pytest

import terminal_operation_lease as lease_module
from terminal_operation_lease import OperationLeaseError, normalize_issue_artifact
from tests.test_deferred_operation_lease_artifact import deferred


def test_deferred_completion_datetime_bounds_refuse_before_signature_and_compare_by_difference(
    deferred, monkeypatch
):
    artifact, _signer, claims, verifier, _now = deferred
    signature_calls = []
    original_verify = verifier.verify

    def observed_verify(*args, **kwargs):
        signature_calls.append(True)
        return original_verify(*args, **kwargs)

    monkeypatch.setattr(verifier, "verify", observed_verify)
    for completed in (
        "0001-01-01T00:00:00+00:01",
        "9999-12-31T23:59:59-00:01",
    ):
        changed = copy.deepcopy(artifact)
        changed["local_completed_at"] = completed
        changed["operation_snapshot"]["local_completed_at"] = completed
        with pytest.raises(OperationLeaseError) as refused:
            normalized = normalize_issue_artifact(changed)
            verifier.verify(
                normalized["token"],
                operation_snapshot=normalized["operation_snapshot"],
            )
        assert refused.value.code == 'OPERATION_LEASE_TIME_INVALID'
        assert signature_calls == []

    snapshot = copy.deepcopy(artifact["operation_snapshot"])
    snapshot["server_received_at"] = "9999-12-31T23:59:59Z"
    snapshot["local_completed_at"] = "9999-12-31T23:59:59+00:00"
    latest_claims = {**claims, "issued_at": snapshot["server_received_at"]}
    lease_module._verify_deferred_completion_time(snapshot, latest_claims)

    snapshot["server_received_at"] = "9999-12-31T23:59:20Z"
    snapshot["local_completed_at"] = "9999-12-31T23:59:51+00:00"
    latest_claims["issued_at"] = snapshot["server_received_at"]
    with pytest.raises(OperationLeaseError) as future:
        lease_module._verify_deferred_completion_time(snapshot, latest_claims)
    assert future.value.code == 'OPERATION_LEASE_TIME_INVALID'

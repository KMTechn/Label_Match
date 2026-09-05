"""Ordinary first-PHS capture must survive a signed future lease response."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import sqlite3
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import Label_Match as label_module
import deferred_intent_capture as capture_module
import terminal_operation_lease as lease_module
from tests.test_deferred_intent_capture import (
    _binding, _protect, _unprotect, _protect_v2, _unprotect_v2, _row,
)
from tests.test_package_logistics import (
    SCOPE, _OperationLeaseTestSigner, _work_group_response,
)
from package_logistics import PackageApiError, PackageClientConfig, PackageOutbox, PackageTransportError


@pytest.fixture
def clock_case(tmp_path, monkeypatch):
    instant = [datetime(2026, 9, 5, 3, 28, 14, tzinfo=timezone.utc)]

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return instant[0].astimezone(tz)

    monkeypatch.setattr(label_module, "datetime", Clock)
    monkeypatch.setattr(lease_module, "datetime", Clock)
    monkeypatch.setattr(capture_module, "utc_now", lambda: instant[0].isoformat())
    database = tmp_path / "capture.sqlite3"
    PackageOutbox(database)
    binding = replace(_binding(), authority_scope_id=SCOPE)

    def open_capture():
        return capture_module.DeferredIntentCaptureStore(
            database, binding, protect_bytes=_protect, unprotect_bytes=_unprotect,
            protect_payload_bytes=_protect_v2, unprotect_payload_bytes=_unprotect_v2,
            initialize_schema=False,
        )

    response = _work_group_response(split=False)
    group = response["phs_work_group"]
    # The established single-transfer deferred fixture avoids the separate
    # work-group destination-version-zero evidence gate (tracked in the report).
    snapshot = {
        "authority_scope_id": SCOPE, "authority_epoch": 5,
        "ledger_plane": "AUTHORITATIVE", "plane_epoch": 3,
        "bundle_id": "TRANSFER-CLOCK-1", "entity_version": 7,
        "member_count": group["member_count"], "membership_hash": group["membership_hash"],
    }
    config = PackageClientConfig(
        base_url="https://logistics.example.test", token="test-only",
        authority_scope_id=SCOPE, authority_epoch=5, ledger_plane="AUTHORITATIVE",
        plane_epoch=3, source_host_id="HOST-PACK-01", device_id="PACK-01",
    )
    lease_binding = label_module._label_match_operation_lease_binding(
        group["scan_payload"], snapshot, config,
    )
    signer = _OperationLeaseTestSigner()
    claims = {
        "contract_version": lease_module.LEASE_CONTRACT_VERSION,
        "lease_id": "LEASE-CLOCK-001", "site_id": "site-main", **lease_binding,
        "issued_at": "2026-09-05T03:28:54Z", "expires_at": "2026-09-05T03:58:54Z",
        "fence": 11, "snapshot_hash": lease_module.canonical_sha256(response),
    }
    artifact = signer.artifact(binding=lease_binding, operation_snapshot=response)
    artifact.update({key: claims[key] for key in ("lease_id", "expires_at", "fence", "snapshot_hash")})
    artifact["token"] = signer.sign(claims)
    calls = []
    accepted = []

    def issue(**kwargs):
        calls.append(kwargs)
        return artifact

    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app.current_set_info = {"id": None, "raw": [], "parsed": [], "start_time": None,
                            "error_count": 0, "has_error_or_reset": False}
    app.deferred_intent_capture = open_capture()
    app._deferred_intent_capture_error = ""
    app.run_tests = True
    app.status_label = Mock()
    app.package_logistics_client = SimpleNamespace(
        config=config, issue_operation_lease=issue,
        resolve_package_source_evidence=lambda _draft: response,
    )
    app.package_operation_lease_store = lease_module.OperationLeaseStore(database)
    app.package_operation_lease_keyring = lease_module.PinnedOperationLeaseKeyring(tmp_path / "keys.json")
    # Reuse the existing package-source fixture; cryptographic verification,
    # request recording, capture, retry claims and materialization stay real.
    evidence = SimpleNamespace(item_id=group["item_id"], active_label_id=group["label_id"],
                               membership_hash=group["membership_hash"], member_count=group["member_count"])
    app._central_phs2_response_parts = lambda _qr, _response: (evidence, snapshot, None)

    def accept(*args, **kwargs):
        accepted.append((args, kwargs))
        app.current_set_info["raw"] = [group["scan_payload"]]
        return True

    app._accept_resolved_central_phs2_scan = accept
    return SimpleNamespace(app=app, instant=instant, database=database, group=group,
                           calls=calls, accepted=accepted, claims=claims, artifact=artifact,
                           signer=signer, open_capture=open_capture)


def _first_scan(case):
    assert case.app._begin_central_phs2_scan_overlay(case.group["scan_payload"], case.group["item_id"])
    return case.app.current_set_info["deferred_intent_id"]


def test_signed_future_first_scan_retries_original_request_after_restart(clock_case):
    case = clock_case
    app = case.app
    intent_id = _first_scan(case)
    row = _row(case.database, intent_id)
    assert row["last_reason_code"] == "OPERATION_LEASE_NOT_YET_VALID"
    assert row["state"] == "RETRY_WAIT_VALIDATION"
    assert case.accepted == []
    assert app.current_set_info["raw"] == []
    assert app.package_operation_lease_store.get(lease_id=case.claims["lease_id"]) is None
    notice = app._deferred_capture_pending_notice()
    assert "시간" in notice.message and "자동" in notice.message and "IT" in notice.message
    assert "intent" not in notice.message and "OPERATION_LEASE" not in notice.message

    # Restart the durable capture reader, then let the ordinary scheduler claim
    # twice: still before signed issuance, then exactly at its lower boundary.
    app.deferred_intent_capture = case.open_capture()
    app._show_deferred_validation_result(app.deferred_intent_capture.validation_status(intent_id))
    assert "IT" in app._deferred_capture_pending_notice().message
    for elapsed, expected in ((10, "RETRY_WAIT_VALIDATION"), (40, "VALIDATED")):
        case.instant[0] = datetime(2026, 9, 5, 3, 28, 14, tzinfo=timezone.utc) + timedelta(seconds=elapsed)
        store = app.deferred_intent_capture
        assert store.next_validation_candidate() == intent_id
        claim = app._prepare_deferred_intent_validation(intent_id)
        assert isinstance(claim, capture_module.DeferredValidationClaim)
        result = app._execute_deferred_label_validation(claim)
        assert result.state == expected
        assert app._materialize_validated_deferred_label(result) is (expected == "VALIDATED")
        if expected != "VALIDATED":
            assert case.accepted == []
    assert len(case.calls) == 3
    assert case.calls[0] == case.calls[1] == case.calls[2]
    assert len(case.accepted) == 1
    assert _row(case.database, intent_id)["payload_hash"] == row["payload_hash"]
    lease = app.package_operation_lease_store.get(lease_id=case.claims["lease_id"])
    assert lease["fence"] == case.claims["fence"]
    with sqlite3.connect(case.database) as conn:
        assert conn.execute("SELECT COUNT(*) FROM package_command_outbox").fetchone()[0] == 0


@pytest.mark.parametrize("invalid", ["signature", "expired", "binding", "snapshot", "artifact_fence"])
def test_invalid_first_scan_remains_quarantined(clock_case, invalid):
    case = clock_case
    case.instant[0] += timedelta(seconds=40)
    if invalid == "signature":
        case.artifact["token"] = _OperationLeaseTestSigner().sign(case.claims)
    elif invalid == "artifact_fence":
        case.artifact["fence"] += 1
    else:
        if invalid == "expired":
            case.instant[0] += timedelta(minutes=30)
        elif invalid == "binding":
            case.claims["device_id"] = "ANOTHER-PC"
        else:
            case.claims["snapshot_hash"] = "f" * 64
        case.artifact["token"] = case.signer.sign(case.claims)
    intent_id = _first_scan(case)
    row = _row(case.database, intent_id)
    assert row["state"] == "BLOCKED_INVALID"
    assert case.app.deferred_intent_capture.next_validation_candidate() is None
    assert case.app.deferred_intent_capture.claim_validation(intent_id, worker_id="normal-retry") is None
    assert case.accepted == []
    assert case.app.package_operation_lease_store.get(lease_id=case.claims["lease_id"]) is None


def test_clock_retry_survives_intervening_read_transport_failure(clock_case):
    case = clock_case
    intent_id = _first_scan(case)
    client = case.app.package_logistics_client
    resolve = client.resolve_package_source_evidence
    client.resolve_package_source_evidence = Mock(side_effect=PackageTransportError("offline read"))
    case.instant[0] += timedelta(seconds=10)
    claim = case.app._prepare_deferred_intent_validation(intent_id)
    assert case.app._execute_deferred_label_validation(claim).state == "RETRY_WAIT_VALIDATION"
    assert len(case.calls) == 1
    client.resolve_package_source_evidence = resolve
    case.instant[0] += timedelta(seconds=30)
    claim = case.app._prepare_deferred_intent_validation(intent_id)
    assert case.app._execute_deferred_label_validation(claim).state == "VALIDATED"
    assert case.calls[0] == case.calls[1]


def test_clock_retry_with_lost_issue_response_uses_unknown_result_lane(clock_case):
    case = clock_case
    intent_id = _first_scan(case)
    issue = case.app.package_logistics_client.issue_operation_lease

    def lose_response(**kwargs):
        issue(**kwargs)
        raise PackageTransportError("reply lost after issue")

    case.app.package_logistics_client.issue_operation_lease = lose_response
    case.instant[0] += timedelta(seconds=40)
    claim = case.app._prepare_deferred_intent_validation(intent_id)
    result = case.app._execute_deferred_label_validation(claim)
    assert case.calls[0] == case.calls[1]
    assert result.state == "RECONCILE_PENDING_VALIDATION"
    assert result.reason_code == "OPERATION_LEASE_COMMIT_UNKNOWN"
    assert case.app.deferred_intent_capture.next_validation_candidate() is None
    assert case.accepted == []


def test_expired_original_clock_lease_is_blocked_without_new_request(clock_case):
    case = clock_case
    intent_id = _first_scan(case)
    case.instant[0] += timedelta(seconds=1840)
    claim = case.app._prepare_deferred_intent_validation(intent_id)
    result = case.app._execute_deferred_label_validation(claim)
    assert case.calls[0] == case.calls[1]
    assert result.state == "BLOCKED_INVALID"
    assert result.reason_code == "OPERATION_LEASE_EXPIRED"
    assert case.accepted == []


def test_api_error_text_is_not_a_verified_clock_response(clock_case):
    case = clock_case
    error = PackageApiError(400, "OPERATION_LEASE_NOT_YET_VALID", "unverified response", committed=False)
    case.app.package_logistics_client.issue_operation_lease = Mock(side_effect=error)
    intent_id = _first_scan(case)
    assert _row(case.database, intent_id)["state"] == "BLOCKED_INVALID"
    assert "재스캔하지 말고" not in case.app._deferred_capture_pending_notice().message

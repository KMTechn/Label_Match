"""Read-only PHS admission, F3 lease clocks, and retained validation recovery."""

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import Label_Match as label_module
import deferred_intent_capture as capture_module
import terminal_operation_lease as lease_module
from tests.test_deferred_intent_capture import (
    _binding, _protect, _unprotect, _protect_v2, _unprotect_v2, _row,
    _claim_and_plan,
)
from tests.test_package_logistics import (
    SCOPE, _OperationLeaseTestSigner, _work_group_response, _work_group_draft,
)
from tests.test_phs_label_workflow import _source_input_tag
from package_logistics import PackageApiError, PackageClientConfig, PackageLogisticsClient, PackageOutbox, PackageTransportError


@pytest.fixture
def clock_case(tmp_path, monkeypatch, request):
    instant = [datetime(2026, 9, 5, 3, 28, 14, tzinfo=timezone.utc)]

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return instant[0].astimezone(tz)

    monkeypatch.setattr(label_module, "datetime", Clock)
    monkeypatch.setattr(lease_module, "datetime", Clock)
    # Retain utc_now's production UTC serialization at exact retry boundaries.
    monkeypatch.setattr(capture_module, "datetime", Clock)
    database = tmp_path / "capture.sqlite3"
    PackageOutbox(database)
    binding = replace(_binding(), authority_scope_id=SCOPE)

    def open_capture():
        return capture_module.DeferredIntentCaptureStore(
            database, binding, protect_bytes=_protect, unprotect_bytes=_unprotect,
            protect_payload_bytes=_protect_v2, unprotect_payload_bytes=_unprotect_v2,
            initialize_schema=False,
        )

    work_group = getattr(request, "param", "merged")
    response = _work_group_response(split=work_group == "split")
    group = response["phs_work_group"]
    response["source_input_tags"] = [
        _source_input_tag(
            source_id,
            group["scan_payload"] if source_id == "ITG-WORK-ONE" else (
                f"PHS=2|SRC=KMTECH_INPUT_TAG|ITG={source_id}|"
                f"CLC={group['item_id']}|LBL=LBL-WORK-TWO|HSH=bbbbbbbbbbbbbbbb"
            ),
            item_id=group["item_id"],
        )
        for source_id in response["work_group_source"]["source_session_ids"]
    ]
    response["input_tag"] = response["source_input_tags"][0]
    PackageLogisticsClient._validate_work_group_source(
        response, _work_group_draft(response), expected_scope=SCOPE,
    )
    snapshot = label_module._label_match_package_source_snapshot(response)
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
    app.package_outbox = PackageOutbox(database)
    app._logistics_authoritative_required = True
    # Replace only UI acceptance; admission, source validation, F3 enqueue,
    # cryptography and durable stores use the production consumers.
    def accept(evidence, resolved_snapshot, sealed, operation_lease, **kwargs):
        accepted.append(kwargs)
        app.current_set_info.update({
            "raw": [group["scan_payload"]], "parsed": [group["item_id"]],
            "central_inherit_all": True,
            "physical_scanned_qr_payload": group["scan_payload"],
            "canonical_input_tag_qr": group["scan_payload"],
            "active_label_id": evidence.active_label_id,
            "package_source_snapshot": resolved_snapshot,
            "sealed_transfer": sealed,
        })
        return True

    app._accept_resolved_central_phs2_scan = accept
    return SimpleNamespace(app=app, instant=instant, database=database, group=group,
                           calls=calls, accepted=accepted, claims=claims, artifact=artifact,
                           signer=signer, open_capture=open_capture,
                           response=response, snapshot=snapshot)


def _first_scan(case):
    assert case.app._begin_central_phs2_scan_overlay(case.group["scan_payload"], case.group["item_id"])
    return case.app.current_set_info["deferred_intent_id"]


def _queue_f3(case):
    # The first-scan fixture runs synchronously; F3 must take the real durable
    # path rather than the app's simulation early return.
    case.app.run_tests = False
    return case.app._queue_authoritative_package(
        item_code=case.group["item_id"], is_manual_complete=False,
    )


def _assert_no_f3_effect(case):
    assert case.app.package_operation_lease_store.get(lease_id=case.claims["lease_id"]) is None
    with sqlite3.connect(case.database) as conn:
        assert conn.execute("SELECT COUNT(*) FROM package_command_outbox").fetchone()[0] == 0


def test_signed_future_f3_retries_original_request_after_restart(clock_case):
    case = clock_case
    app = case.app
    intent_id = _first_scan(case)
    original = _row(case.database, intent_id)
    current = deepcopy(app.current_set_info)
    assert original["state"] == "VALIDATED"
    assert original["last_reason_code"] == "ORDERED_LABEL_VALIDATION_VALID"
    assert len(case.accepted) == 1
    assert case.calls == []
    with sqlite3.connect(case.database) as conn:
        assert conn.execute("SELECT COUNT(*) FROM package_operation_lease_issue_attempts").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM deferred_intent_validation_steps WHERE step_effect='IDEMPOTENT_MUTATION'").fetchone()[0] == 0

    with pytest.raises(lease_module.OperationLeaseError) as blocked:
        _queue_f3(case)
    assert blocked.value.code == "OPERATION_LEASE_NOT_YET_VALID"
    _assert_no_f3_effect(case)
    assert app.current_set_info == current

    # Reopen both durable readers and the pinned keyring, with no shared
    # in-memory attempt, before the next ordinary F3 request.
    app.deferred_intent_capture = case.open_capture()
    app.package_operation_lease_store = lease_module.OperationLeaseStore(case.database)
    app.package_operation_lease_keyring = lease_module.PinnedOperationLeaseKeyring(
        app.package_operation_lease_keyring.path,
    )
    case.instant[0] += timedelta(seconds=39)
    with pytest.raises(lease_module.OperationLeaseError) as blocked:
        _queue_f3(case)
    assert blocked.value.code == "OPERATION_LEASE_NOT_YET_VALID"
    _assert_no_f3_effect(case)
    case.instant[0] += timedelta(seconds=1)
    prepared = _queue_f3(case)
    assert case.calls[0] == case.calls[1] == case.calls[2]
    assert len(case.accepted) == 1
    assert _row(case.database, intent_id)["payload_hash"] == original["payload_hash"]
    lease = app.package_operation_lease_store.get(lease_id=case.claims["lease_id"])
    assert lease["fence"] == case.claims["fence"]
    assert lease["status"] == "PREFETCHED"
    assert lease["set_id"] == current["id"]
    row = app.package_outbox.get_by_set_id(current["id"])
    assert row["idempotency_key"] == prepared["idempotency_key"]
    assert row["local_completion_committed"] == 0
    assert _queue_f3(case)["idempotency_key"] == prepared["idempotency_key"]
    assert len(case.calls) == 3
    with sqlite3.connect(case.database) as conn:
        assert conn.execute("SELECT COUNT(*) FROM package_command_outbox").fetchone()[0] == 1


@pytest.mark.parametrize("invalid,code", [
    ("signature", "OPERATION_LEASE_SIGNATURE_INVALID"),
    ("expired", "OPERATION_LEASE_EXPIRED"),
    ("binding", "OPERATION_LEASE_BINDING_MISMATCH"),
    ("snapshot", "OPERATION_LEASE_SNAPSHOT_MISMATCH"),
    ("artifact_fence", "OPERATION_LEASE_ARTIFACT_MISMATCH"),
])
def test_invalid_f3_lease_blocks_completion(clock_case, invalid, code):
    case = clock_case
    intent_id = _first_scan(case)
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
    current = deepcopy(case.app.current_set_info)
    with pytest.raises(lease_module.OperationLeaseError) as blocked:
        _queue_f3(case)
    assert blocked.value.code == code
    _assert_no_f3_effect(case)
    assert case.app.current_set_info == current
    assert _row(case.database, intent_id)["state"] == "VALIDATED"
    assert case.app.deferred_intent_capture.next_validation_candidate() is None


def test_read_transport_failure_retries_source_before_any_f3_lease(clock_case):
    case = clock_case
    client = case.app.package_logistics_client
    resolve = client.resolve_package_source_evidence
    client.resolve_package_source_evidence = Mock(side_effect=PackageTransportError("offline read"))
    intent_id = _first_scan(case)
    assert _row(case.database, intent_id)["state"] == "RETRY_WAIT_VALIDATION"
    assert case.calls == []
    assert case.accepted == []
    client.resolve_package_source_evidence = resolve
    case.instant[0] += timedelta(seconds=10)
    claim = case.app._prepare_deferred_intent_validation(intent_id)
    result = case.app._execute_deferred_label_validation(claim)
    assert result.state == "VALIDATED"
    assert case.app._materialize_validated_deferred_label(result) is True
    assert case.calls == []
    _assert_no_f3_effect(case)


def test_f3_clock_retry_with_lost_issue_response_preserves_unknown_cause(clock_case):
    case = clock_case
    _first_scan(case)
    with pytest.raises(lease_module.OperationLeaseError):
        _queue_f3(case)
    issue = case.app.package_logistics_client.issue_operation_lease

    def lose_response(**kwargs):
        issue(**kwargs)
        raise PackageTransportError("reply lost after issue")

    case.app.package_logistics_client.issue_operation_lease = lose_response
    case.instant[0] += timedelta(seconds=40)
    with pytest.raises(lease_module.OperationLeaseError) as blocked:
        _queue_f3(case)
    assert blocked.value.code == "OPERATION_LEASE_ISSUE_FAILED"
    assert isinstance(blocked.value.__cause__, PackageTransportError)
    assert case.calls[0] == case.calls[1]
    _assert_no_f3_effect(case)
    _, fingerprint = case.app._operation_lease_request_context(case.group["scan_payload"])
    attempt = case.app.package_operation_lease_store.get_issue_attempt(request_fingerprint=fingerprint)
    assert attempt["status"] == "ACTIVE"
    assert attempt["issue_idempotency_key"] == case.calls[0]["idempotency_key"]


@pytest.mark.parametrize("outages", [1, 2])
def test_f3_clock_retry_keeps_original_key_after_definite_service_failure(clock_case, outages):
    case = clock_case
    _first_scan(case)
    with pytest.raises(lease_module.OperationLeaseError):
        _queue_f3(case)
    issue = case.app.package_logistics_client.issue_operation_lease

    def unavailable_then_replay(**kwargs):
        artifact = issue(**kwargs)
        if 2 <= len(case.calls) <= 1 + outages:
            raise PackageApiError(503, "OPERATION_LEASE_UNAVAILABLE", "service unavailable", committed=False)
        return artifact

    case.app.package_logistics_client.issue_operation_lease = unavailable_then_replay
    case.instant[0] += timedelta(seconds=40)
    for _ in range(outages):
        with pytest.raises(lease_module.OperationLeaseError) as blocked:
            _queue_f3(case)
        assert isinstance(blocked.value.__cause__, PackageApiError)
        assert blocked.value.__cause__.committed is False
        _assert_no_f3_effect(case)
        case.app.package_operation_lease_store = lease_module.OperationLeaseStore(case.database)
    assert _queue_f3(case)["status"] == "PENDING"
    assert len(case.calls) == outages + 2
    assert all(call == case.calls[0] for call in case.calls)


def test_expired_original_f3_clock_lease_is_blocked_without_new_request(clock_case):
    case = clock_case
    _first_scan(case)
    with pytest.raises(lease_module.OperationLeaseError):
        _queue_f3(case)
    case.instant[0] += timedelta(seconds=1840)
    with pytest.raises(lease_module.OperationLeaseError) as blocked:
        _queue_f3(case)
    assert blocked.value.code == "OPERATION_LEASE_EXPIRED"
    assert case.calls[0] == case.calls[1]
    _assert_no_f3_effect(case)


def test_api_error_text_is_not_a_verified_f3_clock_response(clock_case):
    case = clock_case
    _first_scan(case)
    error = PackageApiError(400, "OPERATION_LEASE_NOT_YET_VALID", "unverified response", committed=False)
    case.app.package_logistics_client.issue_operation_lease = Mock(side_effect=error)
    with pytest.raises(lease_module.OperationLeaseError) as blocked:
        _queue_f3(case)
    assert blocked.value.code == "OPERATION_LEASE_ISSUE_FAILED"
    assert blocked.value.__cause__ is error
    _assert_no_f3_effect(case)


@pytest.mark.parametrize("clock_case", ["merged", "split"], indirect=True)
def test_work_group_read_only_admission_preserves_absent_destinations(clock_case):
    case = clock_case
    intent_id = _first_scan(case)
    assert _row(case.database, intent_id)["state"] == "VALIDATED"
    assert len(case.accepted) == 1
    assert case.calls == []
    _assert_no_f3_effect(case)
    expected = case.response["work_group_source"]["entity_versions"]
    with sqlite3.connect(case.database) as conn:
        raw = conn.execute(
            """SELECT evidence_json FROM deferred_intent_validation_steps
                 WHERE intent_id=? AND step_id='label-package-source'
                 ORDER BY validation_generation DESC LIMIT 1""", (intent_id,),
        ).fetchone()[0]
    assert json.loads(raw)["entity_versions"] == expected


@pytest.mark.parametrize("failure", ["read", "service", "repeated_service", "unknown", "api_clock"])
def test_retained_legacy_clock_plan_preserves_request_and_failure_lane(clock_case, failure):
    case = clock_case
    store = case.app.deferred_intent_capture
    captured = store.capture_label_package_source(
        local_work_identity="SET-LEGACY-CLOCK",
        physical_qr_payload=case.group["scan_payload"], item_code=case.group["item_id"],
    )
    # Use the existing historical-plan fixture only for saved two-step rows.
    # The current GUI/default plan above remains unpatched and read-only.
    errors = [None, PackageTransportError("intervening read unavailable")]
    if failure in {"service", "repeated_service"}:
        errors += [PackageApiError(503, "OPERATION_LEASE_UNAVAILABLE", "unavailable", committed=False)] * (2 if failure == "repeated_service" else 1)
    elif failure == "unknown":
        errors.append(PackageTransportError("issue reply lost"))
    elif failure == "api_clock":
        errors.append(PackageApiError(400, "OPERATION_LEASE_NOT_YET_VALID", "unverified response", committed=False))
    original_dispatch = None
    for index, injected in enumerate(errors):
        now = capture_module.utc_now()
        claim = _claim_and_plan(store, captured.intent_id, now=now)
        if index == 1:
            classified = case.app._classify_deferred_validation_error(
                injected, step_id="label-package-source",
            )
            step_id = "label-package-source"
        else:
            step_id = "label-operation-lease"
            dispatch = store.record_validation_mutation_attempt(claim, step_id=step_id, now=now)
            if original_dispatch is None:
                original_dispatch = dispatch
            assert dispatch["idempotency_key"] == original_dispatch["idempotency_key"]
            assert dispatch["request_hash"] == original_dispatch["request_hash"]
            if injected is not None:
                case.app.package_logistics_client.issue_operation_lease = Mock(side_effect=injected)
            with pytest.raises(lease_module.OperationLeaseError) as blocked:
                case.app._acquire_operation_lease(
                    case.group["scan_payload"], expected_snapshot=case.snapshot,
                    issue_idempotency_key=dispatch["idempotency_key"],
                    expected_issue_request_hash=dispatch["request_hash"],
                    reuse_allowed=False, persist_artifact=False,
                )
            classified = case.app._classify_deferred_validation_error(
                blocked.value, step_id=step_id, dispatch_record=dispatch,
            )
        result = store.finish_validation(claim, step_id=step_id, now=now, **classified)
        expected = (
            "RECONCILE_PENDING_VALIDATION" if index == 2 and failure == "unknown"
            else "BLOCKED_INVALID" if index == 2 and failure == "api_clock"
            else "RETRY_WAIT_VALIDATION"
        )
        assert result.state == expected
        if index == 0:
            assert result.reason_code == "OPERATION_LEASE_NOT_YET_VALID"
            case.app._show_deferred_validation_result(result)
            notice = case.app._deferred_capture_pending_notice()
            assert "시간" in notice.message and "자동" in notice.message and "IT" in notice.message
            assert "intent" not in notice.message and "OPERATION_LEASE" not in notice.message
        if expected == "RECONCILE_PENDING_VALIDATION":
            assert result.reason_code == "OPERATION_LEASE_COMMIT_UNKNOWN"
        store = case.open_capture()
        next_attempt = _row(case.database, captured.intent_id)["next_attempt_at"]
        if next_attempt:
            due = datetime.fromisoformat(next_attempt.replace("Z", "+00:00"))
            assert store.next_validation_candidate(
                now=(due - timedelta(microseconds=1)).isoformat().replace("+00:00", "Z"),
            ) is None
            # Follow the persisted backoff, including repeated service errors.
            case.instant[0] = due
            assert store.next_validation_candidate() == captured.intent_id
    if failure in {"unknown", "api_clock"}:
        assert store.next_validation_candidate() is None
        assert store.claim_validation(captured.intent_id, worker_id="automatic-retry") is None
    else:
        claim = _claim_and_plan(store, captured.intent_id, now=capture_module.utc_now())
        replay = store.record_validation_mutation_attempt(
            claim, step_id="label-operation-lease", now=capture_module.utc_now(),
        )
        assert replay["idempotency_key"] == original_dispatch["idempotency_key"]
        assert replay["request_hash"] == original_dispatch["request_hash"]
    assert case.accepted == []
    _assert_no_f3_effect(case)


@pytest.mark.parametrize("clock_case", ["split"], indirect=True)
@pytest.mark.parametrize("invalid", [
    "existing_source_zero", "existing_group_zero", "unrelated_zero", "destination_exists",
    "negative", "boolean", "float", "missing_destination", "remainder_exists",
    "missing_remainders", "unknown_remainder", "duplicate_remainder", "source_as_remainder",
])
def test_work_group_zero_versions_are_limited_to_exact_absent_destinations(clock_case, invalid):
    case = clock_case
    app = case.app
    normalized, snapshot, _sealed = app._central_phs2_response_parts(case.group["scan_payload"], case.response)
    evidence = app._deferred_package_source_evidence(case.group["scan_payload"], "SET-VERSION-GUARD", normalized, snapshot)
    request = {
        "authority_scope_id": SCOPE, "item_code": case.group["item_id"],
        "local_work_identity": "SET-VERSION-GUARD",
        "physical_qr_sha256": evidence["physical_qr_sha256"],
    }
    versions = evidence["entity_versions"]
    destination = "bundle:" + evidence["package_bundle_id"]
    source = "bundle:" + case.response["work_group_source"]["source_transfer_bundle_ids"][0]
    if invalid == "existing_source_zero":
        versions[source] = 0
    elif invalid == "existing_group_zero":
        versions["phs_work_group:" + case.group["group_id"]] = 0
    elif invalid == "unrelated_zero":
        versions["bundle:UNRELATED"] = 0
    elif invalid == "missing_destination":
        del versions[destination]
    elif invalid == "remainder_exists":
        versions["bundle:" + evidence["remainder_transfer_bundle_ids"][0]] = 1
    elif invalid == "missing_remainders":
        del evidence["remainder_transfer_bundle_ids"]
    elif invalid == "unknown_remainder":
        evidence["remainder_transfer_bundle_ids"].append("UNRELATED")
    elif invalid == "duplicate_remainder":
        evidence["remainder_transfer_bundle_ids"] *= 2
    elif invalid == "source_as_remainder":
        evidence["remainder_transfer_bundle_ids"].append(source.removeprefix("bundle:"))
    else:
        versions[destination] = {"destination_exists": 1, "negative": -1, "boolean": False, "float": 0.0}[invalid]
    with pytest.raises(capture_module.DeferredIntentCaptureError) as error:
        app.deferred_intent_capture._validate_verified_step_evidence(
            step_id="label-package-source",
            request_json=json.dumps(request), evidence_json=json.dumps(evidence),
        )
    assert error.value.code == "VALIDATION_EVIDENCE_INCOMPLETE"

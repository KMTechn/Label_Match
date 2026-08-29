from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path
import secrets
import sqlite3
from urllib.parse import urlparse
import uuid

import pytest

from auth_recovery_canary import (
    CanaryCheck,
    CanaryContractError,
    aggregate_status,
    assert_forbidden_values_absent,
    build_canary_report,
)
from direct_sync_push import (
    CONTRACT_VERSION,
    ProducerCredentials,
    canonical_request_string,
    init_relay_queue_schema,
    sign_canonical_request,
)
from kmtech_zero_pe import generate_public_jwk
from producer_runtime_client import canonical_json as runtime_canonical_json
from tools.label_auth_recovery_canary import (
    RECOVERY_BARRIER_PHASE,
    REQUIRED_CHECKS,
    _configure_import_roots,
    _runtime_scope,
    probe_credential_lease_state,
    probe_recovery,
    run_canary,
)


class _NoBodyResponse:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        self.closed = False

    @property
    def content(self) -> bytes:
        raise AssertionError("the canary must not read an artifact body")

    def iter_content(self, *args, **kwargs):
        raise AssertionError("the canary must not stream an artifact body")

    def close(self) -> None:
        self.closed = True


class _VerifyingRestoreSession:
    """Small verifier for Label's existing HMAC restore wire contract."""

    def __init__(self, *, credentials: ProducerCredentials, target_present: bool) -> None:
        self.credentials = credentials
        self.target_present = target_present
        self.calls: list[dict[str, object]] = []

    def get(self, url: str, **kwargs) -> _NoBodyResponse:
        headers = dict(kwargs["headers"])
        parsed = urlparse(url)
        metadata = json.loads(headers["X-Producer-Restore-Metadata"])
        timestamp = str(headers["X-Producer-Timestamp"])
        try:
            signed_at = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            fresh = abs(
                (datetime.now(timezone.utc) - signed_at.astimezone(timezone.utc))
                .total_seconds()
            ) <= 300
        except (TypeError, ValueError):
            fresh = False
        canonical = canonical_request_string(
            method="GET",
            path=parsed.path,
            query_string=parsed.query,
            timestamp=timestamp,
            nonce=str(headers["X-Producer-Nonce"]),
            producer_id=str(headers["X-Producer-Id"]),
            key_id=str(headers["X-Producer-Key-Id"]),
            metadata=metadata,
            content_sha256=str(metadata["content_sha256"]),
            byte_length=int(metadata["byte_length"]),
            content_type="",
        )
        expected = sign_canonical_request(self.credentials.secret, canonical)
        authenticated = (
            fresh
            and headers["X-Producer-Id"] == self.credentials.producer_id
            and headers["X-Producer-Key-Id"] == self.credentials.key_id
            and hmac.compare_digest(headers["X-Producer-Signature"], expected)
        )
        response = _NoBodyResponse(
            401 if not authenticated else (204 if self.target_present else 404)
        )
        self.calls.append(
            {
                "method": "GET",
                "stream": kwargs.get("stream"),
                "allow_redirects": kwargs.get("allow_redirects"),
                "response": response,
            }
        )
        return response


def test_portable_bootstrap_adds_vendored_site_packages(tmp_path: Path) -> None:
    tool_path = tmp_path / "app" / "tools" / "label_auth_recovery_canary.py"
    isolated_search_path: list[str] = []

    app_root, site_packages = _configure_import_roots(
        tool_path, isolated_search_path
    )

    assert app_root == (tmp_path / "app").resolve()
    assert site_packages == (tmp_path / "app" / "site-packages").resolve()
    assert isolated_search_path == [str(app_root), str(site_packages)]


def _write_runtime_inputs(root: Path) -> dict[str, object]:
    root.mkdir(parents=True)
    producer_id = "producer-" + uuid.uuid4().hex
    key_id = "key-" + uuid.uuid4().hex
    secret = secrets.token_urlsafe(48)
    producer_install_id = "install-" + uuid.uuid4().hex
    endpoint = "https://canary.example.invalid/api/producer-ingest/v1/source-file"
    credentials = ProducerCredentials(
        producer_id=producer_id,
        key_id=key_id,
        secret=secret,
        endpoint_url=endpoint,
    )
    credential_path = root / "credential.json"
    credential_path.write_text(
        json.dumps(
            {
                "producer_id": producer_id,
                "key_id": key_id,
                "secret": secret,
                "endpoint_url": endpoint,
            }
        ),
        encoding="utf-8",
    )
    metadata = {
        "contract_version": CONTRACT_VERSION,
        "producer_install_id": producer_install_id,
        "source_host_id": "label-canary-host",
        "producer_role": "label_match",
        "manifest_hash": hashlib.sha256(b"canary-manifest").hexdigest(),
        "stream_name": "label_match_events",
        "source_system": "label_match",
        "source_transport": "legacy_packaging_csv",
        "content_sha256": hashlib.sha256(b"canary-target").hexdigest(),
        "byte_length": len(b"canary-target"),
        "relative_path": "canary/target.jsonl",
    }
    target_path = root / "target.json"
    target_path.write_text(json.dumps({"metadata": metadata}), encoding="utf-8")
    relay_db = root / "relay.sqlite3"
    init_relay_queue_schema(relay_db)
    now = datetime.now(timezone.utc)
    scope = _runtime_scope(credentials, producer_install_id)
    public_jwk = generate_public_jwk()
    runtime_instance_id = "runtime-" + uuid.uuid4().hex
    request_token = secrets.token_urlsafe(32)
    with sqlite3.connect(relay_db) as conn:
        conn.execute(
            """INSERT INTO direct_sync_runtime_authority(
                   authority_scope, endpoint_url, producer_id, key_id,
                   producer_install_id, runtime_instance_id,
                   runtime_public_jwk_json, lease_id, fence,
                   next_request_token, next_request_sequence, expires_at,
                   assigned_relay_id, pending_request_json,
                   pending_issue_idempotency_key, status, last_error_code,
                   created_at, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL,
                         NULL, 'ACTIVE', NULL, ?, ?)""",
            (
                scope,
                endpoint,
                producer_id,
                key_id,
                producer_install_id,
                runtime_instance_id,
                runtime_canonical_json(public_jwk),
                "lease-" + uuid.uuid4().hex,
                1,
                request_token,
                1,
                (now + timedelta(minutes=15)).isoformat().replace("+00:00", "Z"),
                now.isoformat().replace("+00:00", "Z"),
                now.isoformat().replace("+00:00", "Z"),
            ),
        )
    return {
        "credentials": credentials,
        "credential_path": credential_path,
        "target_path": target_path,
        "relay_db": relay_db,
        "producer_install_id": producer_install_id,
        "runtime_instance_id": runtime_instance_id,
        "runtime_public_jwk": public_jwk,
        "runtime_request_token": request_token,
        "forbidden_values": (producer_id, key_id, secret),
    }


def _clear_production_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("APP_ENV", "ENV", "LABEL_MATCH_PRODUCTION", "DIRECT_SYNC_PRODUCTION"):
        monkeypatch.delenv(name, raising=False)


def test_common_contract_never_folds_unknown_into_pass() -> None:
    passing = CanaryCheck("authentication", "PASS", "OK", {})
    unknown = CanaryCheck("credential_lease_state", "UNKNOWN", "ABSENT", {})
    failing = CanaryCheck("recovery", "FAIL", "MISMATCH", {})

    assert aggregate_status([passing, passing]) == "PASS"
    assert aggregate_status([passing, unknown]) == "UNKNOWN"
    assert aggregate_status([passing, unknown, failing]) == "FAIL"


def test_common_contract_rejects_shape_drift_and_secret_evidence() -> None:
    with pytest.raises(CanaryContractError, match="exact contract"):
        build_canary_report(
            app_id="label_match",
            checks=[CanaryCheck("authentication", "PASS", "OK", {})],
            started_at_utc="2026-08-29T00:00:00Z",
            completed_at_utc="2026-08-29T00:00:01Z",
            duration_ms=1000,
            required_check_names=REQUIRED_CHECKS,
        )
    with pytest.raises(CanaryContractError, match="forbidden evidence key"):
        CanaryCheck(
            "authentication", "PASS", "OK", {"request_signature_present": True}
        ).as_dict()
    with pytest.raises(CanaryContractError, match="runtime credential material"):
        assert_forbidden_values_absent({"status": "PASS", "note": "needle"}, ["needle"])
    escaped_secret = 'quote"and\\slash'
    with pytest.raises(CanaryContractError, match="runtime credential material"):
        assert_forbidden_values_absent(
            {"status": "PASS", "note": f"prefix-{escaped_secret}-suffix"},
            [escaped_secret],
        )
    key_secret = "credential-as-object-key"
    with pytest.raises(CanaryContractError, match="runtime credential material"):
        assert_forbidden_values_absent(
            {"status": "PASS", key_secret: "present"}, [key_secret]
        )


def test_credential_lease_binding_and_expiry_fail_closed(tmp_path: Path) -> None:
    inputs = _write_runtime_inputs(tmp_path / "inputs")
    credentials = inputs["credentials"]
    assert isinstance(credentials, ProducerCredentials)
    relay_db = inputs["relay_db"]
    assert isinstance(relay_db, Path)

    passing = probe_credential_lease_state(
        db_path=relay_db,
        credentials=credentials,
        producer_install_id=str(inputs["producer_install_id"]),
    )
    mismatched = probe_credential_lease_state(
        db_path=relay_db,
        credentials=ProducerCredentials(
            producer_id=credentials.producer_id,
            key_id="different-key",
            secret=credentials.secret,
            endpoint_url=credentials.endpoint_url,
        ),
        producer_install_id=str(inputs["producer_install_id"]),
    )
    expired = probe_credential_lease_state(
        db_path=relay_db,
        credentials=credentials,
        producer_install_id=str(inputs["producer_install_id"]),
        now=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    with sqlite3.connect(relay_db) as conn:
        conn.execute(
            "UPDATE direct_sync_runtime_authority SET expires_at=?",
            ((datetime.now(timezone.utc) + timedelta(hours=1)).replace(tzinfo=None).isoformat(),),
        )
    naive_expiry = probe_credential_lease_state(
        db_path=relay_db,
        credentials=credentials,
        producer_install_id=str(inputs["producer_install_id"]),
    )
    with sqlite3.connect(relay_db) as conn:
        conn.execute(
            """UPDATE direct_sync_runtime_authority
               SET expires_at=?, runtime_instance_id='x'""",
            (
                (datetime.now(timezone.utc) + timedelta(hours=1))
                .isoformat()
                .replace("+00:00", "Z"),
            ),
        )
    malformed_runtime_id = probe_credential_lease_state(
        db_path=relay_db,
        credentials=credentials,
        producer_install_id=str(inputs["producer_install_id"]),
    )

    assert (passing.status, passing.reason_code) == ("PASS", "LEASE_STATE_ACTIVE")
    assert (mismatched.status, mismatched.reason_code) == (
        "FAIL",
        "LEASE_CREDENTIAL_BINDING_MISMATCH",
    )
    assert (expired.status, expired.reason_code) == (
        "FAIL",
        "LEASE_EXPIRED_OR_INVALID",
    )
    assert (naive_expiry.status, naive_expiry.reason_code) == (
        "FAIL",
        "LEASE_EXPIRED_OR_INVALID",
    )
    assert (malformed_runtime_id.status, malformed_runtime_id.reason_code) == (
        "FAIL",
        "LEASE_STATE_NOT_ACTIVE",
    )


def test_assigned_lease_requires_exact_relay_authority_binding(tmp_path: Path) -> None:
    inputs = _write_runtime_inputs(tmp_path / "inputs")
    credentials = inputs["credentials"]
    relay_db = inputs["relay_db"]
    assert isinstance(credentials, ProducerCredentials)
    assert isinstance(relay_db, Path)
    relay_id = "relay-" + uuid.uuid4().hex
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    relay_metadata = {
        "producer_install_id": inputs["producer_install_id"],
        "runtime_instance_id": inputs["runtime_instance_id"],
        "runtime_public_jwk": inputs["runtime_public_jwk"],
        "runtime_fence": 1,
        "runtime_request_token": inputs["runtime_request_token"],
        "runtime_request_sequence": 1,
    }
    with sqlite3.connect(relay_db) as conn:
        conn.execute(
            """INSERT INTO direct_sync_relay_batches(
                   relay_id, status, source_file_path, spooled_file_path,
                   producer_manifest_path, relative_path, content_sha256,
                   byte_length, attempt_count, next_attempt_at, metadata_json,
                   producer_id, key_id, endpoint_url, runtime_fencing_policy,
                   created_at, updated_at
               ) VALUES (?, 'leased', 'source', 'spool', 'manifest', 'relative',
                         ?, 1, 1, ?, ?, ?, ?, ?, 'runtime_required', ?, ?)""",
            (
                relay_id,
                hashlib.sha256(b"assigned-relay").hexdigest(),
                now,
                runtime_canonical_json(relay_metadata),
                credentials.producer_id,
                credentials.key_id,
                credentials.endpoint_url,
                now,
                now,
            ),
        )
        conn.execute(
            """UPDATE direct_sync_runtime_authority
               SET next_request_token=NULL, next_request_sequence=NULL,
                   assigned_relay_id=?""",
            (relay_id,),
        )

    passing = probe_credential_lease_state(
        db_path=relay_db,
        credentials=credentials,
        producer_install_id=str(inputs["producer_install_id"]),
    )
    with sqlite3.connect(relay_db) as conn:
        conn.execute(
            "UPDATE direct_sync_relay_batches SET key_id='wrong-key' WHERE relay_id=?",
            (relay_id,),
        )
    mismatched = probe_credential_lease_state(
        db_path=relay_db,
        credentials=credentials,
        producer_install_id=str(inputs["producer_install_id"]),
    )

    assert (passing.status, passing.reason_code) == ("PASS", "LEASE_STATE_ACTIVE")
    assert (mismatched.status, mismatched.reason_code) == (
        "FAIL",
        "LEASE_ASSIGNED_RELAY_BINDING_MISMATCH",
    )
    assert mismatched.evidence["assigned_relay_exact"] is False


def test_recovery_probe_hits_exact_boundary_and_reopens_twice(tmp_path: Path) -> None:
    check = probe_recovery(
        work_dir=tmp_path / "recovery",
        protected_roots=(Path(__file__).resolve().parents[1],),
    )

    assert (check.status, check.reason_code) == (
        "PASS",
        "RECOVERY_ROLLBACK_IDEMPOTENT",
    )
    assert check.evidence["phase"] == RECOVERY_BARRIER_PHASE
    assert check.evidence["same_connection_transaction_open"] is True
    assert check.evidence["intent_insert_seen"] is True
    assert check.evidence["audit_insert_executed"] is False
    assert check.evidence["external_preimage_visible"] is True
    assert check.evidence["restart_one_exact"] is True
    assert check.evidence["restart_two_exact"] is True
    assert check.evidence["post_recovery_row_count"] == 0
    assert check.evidence["seal_key_unchanged"] is True
    assert check.evidence["live_label_data_touched"] is False
    assert check.evidence["workdir_disjoint_from_protected_roots"] is True


def test_recovery_probe_rejects_a_live_input_ancestor(tmp_path: Path) -> None:
    live_root = tmp_path / "live"
    live_root.mkdir()
    work_dir = live_root / "canary-work"

    check = probe_recovery(work_dir=work_dir, protected_roots=(live_root,))

    assert (check.status, check.reason_code) == (
        "FAIL",
        "RECOVERY_WORKDIR_OVERLAPS_LIVE_INPUT",
    )
    assert check.evidence["workdir_disjoint_from_protected_roots"] is False
    assert not work_dir.exists()


def test_malformed_target_is_fail_for_authentication_and_lease(tmp_path: Path) -> None:
    inputs = _write_runtime_inputs(tmp_path / "inputs")
    malformed = tmp_path / "inputs" / "malformed-target.json"
    malformed.write_text("{", encoding="utf-8")

    report = run_canary(
        credential_path=str(inputs["credential_path"]),
        auth_target_path=str(malformed),
        relay_db_path=str(inputs["relay_db"]),
        work_dir=str(tmp_path / "separate-work"),
        report_path=str(tmp_path / "malformed-report.json"),
        live_roots=(tmp_path / "inputs",),
    )

    assert report["status"] == "FAIL"
    assert [check["status"] for check in report["checks"][:2]] == ["FAIL", "FAIL"]
    assert [check["reason_code"] for check in report["checks"][:2]] == [
        "AUTH_TARGET_INPUT_INVALID",
        "AUTH_TARGET_INPUT_INVALID",
    ]


def test_report_path_cannot_overwrite_a_live_input(tmp_path: Path) -> None:
    inputs = _write_runtime_inputs(tmp_path / "inputs")
    credential_path = Path(str(inputs["credential_path"]))
    credential_sha256 = hashlib.sha256(credential_path.read_bytes()).hexdigest()

    with pytest.raises(CanaryContractError, match="protected live root"):
        run_canary(
            credential_path=str(credential_path),
            auth_target_path=str(inputs["target_path"]),
            relay_db_path=str(inputs["relay_db"]),
            work_dir=str(tmp_path / "separate-work"),
            report_path=str(credential_path),
            live_roots=(tmp_path / "inputs",),
        )

    assert hashlib.sha256(credential_path.read_bytes()).hexdigest() == credential_sha256
    assert not (tmp_path / "separate-work").exists()


def test_machine_readable_pass_fail_unknown_matrix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _clear_production_profile(monkeypatch)
    export = os.getenv("LABEL_CANARY_TEST_EXPORT_DIR", "").strip()
    root = Path(export).resolve() if export else tmp_path / "matrix"
    root.mkdir(parents=True)
    inputs = _write_runtime_inputs(tmp_path / "runtime-inputs")
    credentials = inputs["credentials"]
    assert isinstance(credentials, ProducerCredentials)
    scenarios = (
        ("normal", "none", True, "PASS", "AUTHENTICATED_TARGET_READABLE"),
        ("invalid-secret", "invalid-secret", True, "FAIL", "INJECTED_CREDENTIAL_REJECTED"),
        ("expired-timestamp", "expired-timestamp", True, "FAIL", "INJECTED_CREDENTIAL_REJECTED"),
        ("target-absent", "none", False, "UNKNOWN", "AUTH_TARGET_ABSENT"),
    )

    for name, injection, target_present, expected, auth_reason in scenarios:
        session = _VerifyingRestoreSession(
            credentials=credentials, target_present=target_present
        )
        scenario_root = root / name
        report_path = scenario_root / "canary.json"
        report = run_canary(
            credential_path=str(inputs["credential_path"]),
            auth_target_path=str(inputs["target_path"]),
            relay_db_path=str(inputs["relay_db"]),
            work_dir=str(scenario_root / "work"),
            report_path=str(report_path),
            auth_injection=injection,
            session=session,
            live_roots=(Path(str(inputs["credential_path"])).parent,),
        )

        assert report["status"] == expected
        assert report["checks"][0]["reason_code"] == auth_reason
        assert report["checks"][1]["status"] == "PASS"
        assert report["checks"][2]["status"] == "PASS"
        assert report["secret_material_recorded"] is False
        assert session.calls[0]["stream"] is True
        assert session.calls[0]["allow_redirects"] is False
        response = session.calls[0]["response"]
        assert isinstance(response, _NoBodyResponse)
        assert response.closed is True
        serialized = report_path.read_text(encoding="utf-8")
        for forbidden in inputs["forbidden_values"]:
            assert str(forbidden) not in serialized

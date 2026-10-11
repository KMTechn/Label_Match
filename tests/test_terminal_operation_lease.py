import base64
import copy
import json
import hashlib
import logging
import sqlite3
from datetime import datetime, timedelta, timezone

import pytest
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature

from package_logistics import (
    PackageApiError,
    PackageClientConfig,
    PackageCommandDraft,
    PackageLogisticsClient,
    PackageLogisticsError,
    PackageOutbox,
    PackageOutboxProcessor,
    PackageTransportError,
)
from terminal_operation_lease import (
    ARTIFACT_CONTRACT_VERSION,
    KEYRING_CONTRACT_VERSION,
    LEASE_CONTRACT_VERSION,
    OperationLeaseError,
    OperationLeaseStore,
    PinnedOperationLeaseKeyring,
    canonical_json_bytes,
    canonical_sha256,
    issue_request_fingerprint,
    jwk_thumbprint,
    normalize_issue_artifact,
)


P256_ORDER = int(
    "FFFFFFFF00000000FFFFFFFFFFFFFFFFBCE6FAADA7179E84F3B9CAC2FC632551",
    16,
)


def b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


class Signer:
    def __init__(self, kid="lease-key-1"):
        self.kid = kid
        self.key = ec.generate_private_key(ec.SECP256R1())

    @property
    def jwk(self):
        numbers = self.key.public_key().public_numbers()
        return {
            "kty": "EC",
            "crv": "P-256",
            "x": b64(numbers.x.to_bytes(32, "big")),
            "y": b64(numbers.y.to_bytes(32, "big")),
        }

    def sign(self, payload):
        header = {
            "alg": "ES256",
            "kid": self.kid,
            "typ": "terminal-operation-lease+jws",
        }
        header_segment = b64(canonical_json_bytes(header))
        payload_segment = b64(canonical_json_bytes(payload))
        signing_input = f"{header_segment}.{payload_segment}".encode("ascii")
        r, s = decode_dss_signature(
            self.key.sign(signing_input, ec.ECDSA(hashes.SHA256()))
        )
        if s > P256_ORDER // 2:
            s = P256_ORDER - s
        signature = r.to_bytes(32, "big") + s.to_bytes(32, "big")
        return f"{header_segment}.{payload_segment}.{b64(signature)}"


def keyring(signer, *, site_id="site-main"):
    return {
        "contract_version": KEYRING_CONTRACT_VERSION,
        "site_id": site_id,
        "current_kid": signer.kid,
        "keys": [
            {
                "kid": signer.kid,
                "status": "current",
                "public_jwk": signer.jwk,
                "thumbprint": jwk_thumbprint(signer.jwk),
            }
        ],
    }


def binding(*, device_id="PACK-01", membership_hash="a" * 64):
    return {
        "program": "Label_Match",
        "device_id": device_id,
        "source_host_id": "HOST-PACK-01",
        "authority_scope_id": "scope-main",
        "ledger_plane": "AUTHORITATIVE",
        "plane_epoch": 7,
        "operation": "CREATE_PACKAGE",
        "resource_id": "phs-work-group:PHS-GROUP-001",
        "physical_label_id": "LBL-001",
        "physical_qr_sha256": "b" * 64,
        "item_id": "ITEM-001",
        "quantity": 3,
        "member_count": 3,
        "membership_hash": membership_hash,
        "expected_versions": {
            "bundle:TRANSFER-1": 4,
            "phs_work_group:PHS-GROUP-001": 2,
        },
    }


def payload(operation_snapshot, *, now=None, **overrides):
    instant = now or datetime(2026, 8, 1, 1, 0, tzinfo=timezone.utc)
    value = {
        "contract_version": LEASE_CONTRACT_VERSION,
        "lease_id": "LEASE-001",
        "site_id": "site-main",
        **binding(),
        "issued_at": instant.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "expires_at": (instant + timedelta(hours=2)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        "fence": 11,
        "snapshot_hash": canonical_sha256(operation_snapshot),
    }
    value.update(overrides)
    return value


def verified_fixture(tmp_path):
    signer = Signer()
    operation_snapshot = {
        "bundle": {
            "bundle_id": "TRANSFER-1",
            "member_ids": ["UNIT-1", "UNIT-2", "UNIT-3"],
        },
        "candidate_count": 1,
    }
    claims = payload(operation_snapshot)
    token = signer.sign(claims)
    verifier = PinnedOperationLeaseKeyring(tmp_path / "keys.json")
    verifier.bootstrap_authenticated(
        keyring(signer), authenticated_online=True
    )
    return signer, verifier, operation_snapshot, claims, token


def issue_fingerprint():
    return issue_request_fingerprint(
        program="Label_Match",
        device_id="PACK-01",
        source_host_id="HOST-PACK-01",
        authority_scope_id="scope-main",
        operation="CREATE_PACKAGE",
        physical_qr_hash="b" * 64,
    )


def test_signed_lease_verifies_exact_terminal_and_source_binding(tmp_path):
    _signer, verifier, snapshot, claims, token = verified_fixture(tmp_path)

    verified = verifier.verify(
        token,
        expected=binding(),
        operation_snapshot=snapshot,
        now=datetime(2026, 8, 1, 1, 30, tzinfo=timezone.utc),
    )

    assert verified == claims


def test_web_package_creation_snapshot_accepts_zero_expected_versions(tmp_path):
    """Match the authoritative package snapshot emitted by the web service."""

    signer = Signer()
    operation_snapshot = {
        "package_bundle_id": "PACKAGE-WORK-001",
        "member_ids": ["UNIT-1", "UNIT-2", "UNIT-3"],
    }
    expected = binding()
    expected["expected_versions"] = {
        "bundle:TRANSFER-1": 4,
        "bundle:PACKAGE-WORK-001": 0,
        "bundle:REMAINDER-1": 0,
        "phs_work_group:PHS-GROUP-001": 2,
    }
    claims = payload(
        operation_snapshot,
        expected_versions=expected["expected_versions"],
    )
    token = signer.sign(claims)
    verifier = PinnedOperationLeaseKeyring(tmp_path / "keys.json")
    verifier.bootstrap_authenticated(
        keyring(signer), authenticated_online=True
    )

    verified = verifier.verify(
        token,
        expected=expected,
        operation_snapshot=operation_snapshot,
        now=datetime(2026, 8, 1, 1, 30, tzinfo=timezone.utc),
    )

    assert verified["expected_versions"] == expected["expected_versions"]


@pytest.mark.parametrize(
    ("mutation", "code"),
    (
        (lambda expected: expected.update(device_id="PACK-02"), "OPERATION_LEASE_BINDING_MISMATCH"),
        (lambda expected: expected.update(source_host_id="HOST-PACK-02"), "OPERATION_LEASE_BINDING_MISMATCH"),
        (lambda expected: expected.update(authority_scope_id="scope-other"), "OPERATION_LEASE_BINDING_MISMATCH"),
        (lambda expected: expected.update(physical_qr_sha256="f" * 64), "OPERATION_LEASE_BINDING_MISMATCH"),
        (lambda expected: expected.update(resource_id="phs-work-group:PHS-GROUP-OTHER"), "OPERATION_LEASE_BINDING_MISMATCH"),
        (lambda expected: expected.update(membership_hash="c" * 64), "OPERATION_LEASE_BINDING_MISMATCH"),
        (lambda expected: expected["expected_versions"].update({"bundle:TRANSFER-1": 5}), "OPERATION_LEASE_BINDING_MISMATCH"),
    ),
)
def test_lease_rejects_terminal_physical_membership_and_version_changes(
    tmp_path, mutation, code
):
    _signer, verifier, snapshot, _claims, token = verified_fixture(tmp_path)
    expected = binding()
    mutation(expected)

    with pytest.raises(OperationLeaseError) as raised:
        verifier.verify(
            token,
            expected=expected,
            operation_snapshot=snapshot,
            now=datetime(2026, 8, 1, 1, 30, tzinfo=timezone.utc),
        )

    assert raised.value.code == code


def test_lease_rejects_tamper_expiry_and_snapshot_substitution(tmp_path):
    _signer, verifier, snapshot, _claims, token = verified_fixture(tmp_path)
    header, body, signature = token.split(".")
    tampered = f"{header}.{body}.{'A' if signature[0] != 'A' else 'B'}{signature[1:]}"

    with pytest.raises(OperationLeaseError) as signature_error:
        verifier.verify(
            tampered,
            expected=binding(),
            operation_snapshot=snapshot,
            now=datetime(2026, 8, 1, 1, 30, tzinfo=timezone.utc),
        )
    assert signature_error.value.code == "OPERATION_LEASE_SIGNATURE_INVALID"

    with pytest.raises(OperationLeaseError) as expired:
        verifier.verify(
            token,
            expected=binding(),
            operation_snapshot=snapshot,
            now=datetime(2026, 8, 1, 3, 0, tzinfo=timezone.utc),
        )
    assert expired.value.code == "OPERATION_LEASE_EXPIRED"

    with pytest.raises(OperationLeaseError) as snapshot_error:
        verifier.verify(
            token,
            expected=binding(),
            operation_snapshot={**snapshot, "candidate_count": 2},
            now=datetime(2026, 8, 1, 1, 30, tzinfo=timezone.utc),
        )
    assert snapshot_error.value.code == "OPERATION_LEASE_SNAPSHOT_MISMATCH"


def test_same_phs2_offline_token_is_exclusive_to_issuing_pc(tmp_path):
    _signer, verifier, snapshot, _claims, token = verified_fixture(tmp_path)

    assert verifier.verify(
        token,
        expected=binding(device_id="PACK-01"),
        operation_snapshot=snapshot,
        now=datetime(2026, 8, 1, 1, 30, tzinfo=timezone.utc),
    )["device_id"] == "PACK-01"

    with pytest.raises(OperationLeaseError) as other_pc:
        verifier.verify(
            token,
            expected=binding(device_id="PACK-02"),
            operation_snapshot=snapshot,
            now=datetime(2026, 8, 1, 1, 30, tzinfo=timezone.utc),
        )
    assert other_pc.value.code == "OPERATION_LEASE_BINDING_MISMATCH"


def test_issue_artifact_kid_must_match_the_signed_token(tmp_path):
    signer, _verifier, snapshot, claims, token = verified_fixture(tmp_path)
    artifact = {
        "contract_version": ARTIFACT_CONTRACT_VERSION,
        "lease_id": claims["lease_id"],
        "status": "ACTIVE",
        "replayed": False,
        "token": token,
        "kid": signer.kid,
        "expires_at": claims["expires_at"],
        "fence": claims["fence"],
        "snapshot_hash": claims["snapshot_hash"],
        "operation_snapshot": snapshot,
        "keyring": keyring(signer),
    }

    assert normalize_issue_artifact(artifact)["kid"] == signer.kid
    artifact["kid"] = "lease-key-other"
    with pytest.raises(OperationLeaseError) as mismatch:
        normalize_issue_artifact(artifact)
    assert mismatch.value.code == "OPERATION_LEASE_ARTIFACT_INVALID"


def test_keyring_requires_authenticated_bootstrap_and_never_tofus_token(
    tmp_path,
):
    signer = Signer()
    snapshot = {"source": "TRANSFER-1"}
    token = signer.sign(payload(snapshot))
    verifier = PinnedOperationLeaseKeyring(tmp_path / "keys.json")

    with pytest.raises(OperationLeaseError) as unauthenticated:
        verifier.bootstrap_authenticated(
            keyring(signer), authenticated_online=False
        )
    assert unauthenticated.value.code == "OPERATION_LEASE_KEYRING_UNAUTHENTICATED"
    with pytest.raises(OperationLeaseError) as not_pinned:
        verifier.verify(
            token,
            expected=binding(),
            operation_snapshot=snapshot,
            now=datetime(2026, 8, 1, 1, 30, tzinfo=timezone.utc),
        )
    assert not_pinned.value.code == "OPERATION_LEASE_KEYRING_NOT_PINNED"


def test_authenticated_rotation_rejects_kid_reuse_and_pin_removal(tmp_path):
    first = Signer(kid="lease-key-1")
    verifier = PinnedOperationLeaseKeyring(tmp_path / "keys.json")
    verifier.bootstrap_authenticated(
        keyring(first), authenticated_online=True
    )

    reused_kid = Signer(kid="lease-key-1")
    with pytest.raises(OperationLeaseError) as reuse:
        verifier.bootstrap_authenticated(
            keyring(reused_kid), authenticated_online=True
        )
    assert reuse.value.code == "OPERATION_LEASE_KID_REUSE_REJECTED"

    replacement = Signer(kid="lease-key-2")
    with pytest.raises(OperationLeaseError) as removal:
        verifier.bootstrap_authenticated(
            keyring(replacement), authenticated_online=True
        )
    assert removal.value.code == "OPERATION_LEASE_KEY_REMOVAL_REJECTED"


def test_issue_api_uses_machine_headers_exact_body_and_idempotency():
    calls = []

    def transport(method, url, headers, body, timeout):
        calls.append((method, url, headers, json.loads(body), timeout))
        return {"ok": True, "data": {"lease_id": "LEASE-1"}}

    client = PackageLogisticsClient(
        PackageClientConfig(
            base_url="https://logistics.example.test",
            token="machine-secret",
            authority_scope_id="scope-main",
            source_host_id="HOST-PACK-01",
            device_id="PACK-01",
        ),
        transport=transport,
    )

    assert client.issue_operation_lease(
        authority_scope_id="scope-main",
        operation="CREATE_PACKAGE",
        scan_payload="PHS=2|CLC=ITEM-001|LBL=LBL-001",
        idempotency_key="lease-issue-key",
    ) == {"lease_id": "LEASE-1"}

    method, url, headers, body, _timeout = calls[0]
    assert method == "POST"
    assert url.endswith("/logistics/api/v1/operation-leases/issue")
    assert headers["X-Logistics-API-Token"] == "machine-secret"
    assert headers["X-Logistics-Source-Host-Id"] == "HOST-PACK-01"
    assert headers["X-Logistics-Device-Id"] == "PACK-01"
    assert headers["X-Logistics-Program"] == "Label_Match"
    assert headers["Idempotency-Key"] == "lease-issue-key"
    assert body == {
        "authority_scope_id": "scope-main",
        "operation": "CREATE_PACKAGE",
        "scan_payload": "PHS=2|CLC=ITEM-001|LBL=LBL-001",
    }


def lease_draft(**overrides):
    values = {
        "set_id": "SET-1",
        "item_code": "ITEM-001",
        "source_bundle_id": "TRANSFER-1",
        "source_external_label": "",
        "source_input_tag_id": "",
        "source_bundle_hint": "",
        "source_authority_scope_id": "scope-main",
        "expected_member_count": 3,
        "expected_membership_hash": "a" * 64,
        "expected_authority_epoch": 9,
        "expected_ledger_plane": "AUTHORITATIVE",
        "expected_plane_epoch": 7,
        "package_bundle_id": "PACKAGE-1",
        "external_label": "PKG-LABEL-1",
        "membership_mode": "INHERIT_ALL",
        "sample_barcodes": (),
        "operation_lease_id": "LEASE-001",
        "operation_lease_token": "a.b.c",
        "operation_lease_fence": 11,
        "operation_lease_snapshot_hash": "d" * 64,
        "operation_lease_completed_at": "2026-08-01T01:20:00Z",
    }
    values.update(overrides)
    return PackageCommandDraft.build(**values)


def test_package_command_carries_exact_lease_object_and_same_idempotency(monkeypatch):
    client = PackageLogisticsClient(
        PackageClientConfig(
            base_url="https://logistics.example.test",
            token="machine-secret",
            authority_scope_id="scope-main",
            source_host_id="HOST-PACK-01",
            device_id="PACK-01",
        ),
        transport=lambda *_args: {},
    )
    projection = {
        "bundle_id": "TRANSFER-1",
        "transfer_bundle_id": "TRANSFER-1",
        "authority_scope_id": "scope-main",
        "authority_epoch": 9,
        "ledger_plane": "AUTHORITATIVE",
        "plane_epoch": 7,
        "entity_version": 4,
    }
    evidence = {
        "member_ids": ("UNIT-1", "UNIT-2", "UNIT-3"),
        "membership_hash": "unused",
        "barcode_membership_hash": "e" * 64,
        "barcode_to_unit": {},
        "barcodes": (),
    }
    evidence["membership_hash"] = __import__("package_logistics").membership_hash(
        evidence["member_ids"]
    )
    monkeypatch.setattr(client, "get_bundle", lambda *_args, **_kwargs: projection)
    monkeypatch.setattr(client, "_validate_projection", lambda *_args, **_kwargs: evidence)

    source_id, command = client.build_create_package_command(
        lease_draft(expected_membership_hash=evidence["membership_hash"]),
        idempotency_key="label-package-command-1",
    )

    assert source_id == "TRANSFER-1"
    assert command["idempotency_key"] == "label-package-command-1"
    assert command["payload"]["operation_lease"] == {
        "token": "a.b.c",
        "lease_id": "LEASE-001",
        "fence": 11,
        "snapshot_hash": "d" * 64,
        "operation_completed_at": "2026-08-01T01:20:00Z",
    }


def test_local_completion_updates_outbox_and_business_lease_atomically(tmp_path):
    database = tmp_path / "outbox.sqlite3"
    outbox = PackageOutbox(database)
    lease_store = OperationLeaseStore(database)
    draft = lease_draft()
    row = outbox.enqueue(draft)
    artifact = {
        "token": draft.operation_lease_token,
        "operation_snapshot": {"source": "TRANSFER-1"},
        "claims": {
            "lease_id": draft.operation_lease_id,
            "resource_id": "phs-work-group:PHS-GROUP-001",
            "snapshot_hash": draft.operation_lease_snapshot_hash,
            "fence": draft.operation_lease_fence,
        },
    }
    lease_store.save_prefetched(
        artifact=artifact,
        binding=binding(),
        issue_idempotency_key="lease-issue-key",
    )
    lease_store.attach_set(draft.operation_lease_id, draft.set_id)

    outbox.mark_local_completion_committed(
        row["idempotency_key"],
        operation_lease_id=draft.operation_lease_id,
        operation_completed_at=draft.operation_lease_completed_at,
    )

    queued = outbox.get_by_set_id(draft.set_id)
    lease = lease_store.get(lease_id=draft.operation_lease_id)
    assert queued["local_completion_committed"] == 1
    assert lease["status"] == "LOCAL_COMPLETED"
    assert lease["operation_result_id"] == row["idempotency_key"]
    assert lease["consume_idempotency_key"] == row["idempotency_key"]
    assert "SENDING_LEASE" not in lease.values()


def test_issue_attempt_survives_restart_then_retires_for_a_new_lifecycle(
    tmp_path,
):
    database = tmp_path / "outbox.sqlite3"
    outbox = PackageOutbox(database)
    store = OperationLeaseStore(database)
    first_attempt = store.reserve_issue_attempt(issue_fingerprint())

    restarted = OperationLeaseStore(database)
    replay_attempt = restarted.reserve_issue_attempt(issue_fingerprint())

    assert replay_attempt["issue_idempotency_key"] == first_attempt[
        "issue_idempotency_key"
    ]
    draft = lease_draft()
    artifact = {
        "token": draft.operation_lease_token,
        "operation_snapshot": {"source": "TRANSFER-1"},
        "claims": {
            "lease_id": draft.operation_lease_id,
            "resource_id": "phs-work-group:PHS-GROUP-001",
            "snapshot_hash": draft.operation_lease_snapshot_hash,
            "fence": draft.operation_lease_fence,
        },
    }
    restarted.save_prefetched(
        artifact=artifact,
        binding=binding(),
        issue_idempotency_key=first_attempt["issue_idempotency_key"],
    )
    restarted.attach_set(draft.operation_lease_id, draft.set_id)
    row = outbox.enqueue(draft)
    outbox.mark_local_completion_committed(
        row["idempotency_key"],
        operation_lease_id=draft.operation_lease_id,
        operation_completed_at=draft.operation_lease_completed_at,
    )
    assert outbox.claim_next()["status"] == "SENDING"
    outbox.mark_acked(
        row["idempotency_key"],
        {"receipt_id": "RECEIPT-1"},
        operation_lease_id=draft.operation_lease_id,
        operation_lease_consumption={"status": "CONSUMED"},
    )

    retired = restarted.get_issue_attempt(
        issue_idempotency_key=first_attempt["issue_idempotency_key"]
    )
    next_attempt = restarted.reserve_issue_attempt(issue_fingerprint())
    assert retired["status"] == "RETIRED"
    assert retired["retire_reason"] == "ACKED"
    assert next_attempt["issue_idempotency_key"] != first_attempt[
        "issue_idempotency_key"
    ]
    restarted.save_prefetched(
        artifact={
            **artifact,
            "claims": {
                **artifact["claims"],
                "lease_id": "LEASE-002",
                "fence": 12,
            },
        },
        binding=binding(),
        issue_idempotency_key=next_attempt["issue_idempotency_key"],
    )
    assert restarted.get(lease_id="LEASE-002")["status"] == "PREFETCHED"


def test_legacy_resource_unique_store_migrates_without_losing_active_lease(
    tmp_path,
):
    database = tmp_path / "legacy.sqlite3"
    encoded_binding = canonical_json_bytes(binding()).decode("utf-8")
    encoded_snapshot = canonical_json_bytes({"source": "TRANSFER-1"}).decode(
        "utf-8"
    )
    encoded_artifact = canonical_json_bytes(
        {
            "token": "a.b.c",
            "operation_snapshot": {"source": "TRANSFER-1"},
            "claims": {
                "lease_id": "LEASE-LEGACY",
                "resource_id": "phs-work-group:PHS-GROUP-001",
                "snapshot_hash": "d" * 64,
                "fence": 11,
            },
        }
    ).decode("utf-8")
    with sqlite3.connect(database) as connection:
        connection.execute(
            """CREATE TABLE package_operation_leases (
                   lease_id TEXT PRIMARY KEY,
                   resource_id TEXT NOT NULL UNIQUE,
                   set_id TEXT UNIQUE,
                   issue_idempotency_key TEXT NOT NULL UNIQUE,
                   token TEXT NOT NULL,
                   artifact_json TEXT NOT NULL,
                   binding_json TEXT NOT NULL,
                   operation_snapshot_json TEXT NOT NULL,
                   snapshot_hash TEXT NOT NULL,
                   fence INTEGER NOT NULL,
                   status TEXT NOT NULL CHECK(status IN (
                       'PREFETCHED','LOCAL_COMPLETED','ACKED','OPERATOR_REVIEW'
                   )),
                   operation_result_id TEXT,
                   operation_completed_at TEXT,
                   consume_idempotency_key TEXT,
                   consume_claimed_at TEXT,
                   consume_receipt_json TEXT,
                   last_error_code TEXT,
                   last_error_message TEXT,
                   created_at TEXT NOT NULL,
                   updated_at TEXT NOT NULL
               )"""
        )
        connection.execute(
            """INSERT INTO package_operation_leases(
                   lease_id,resource_id,issue_idempotency_key,token,
                   artifact_json,binding_json,operation_snapshot_json,
                   snapshot_hash,fence,status,created_at,updated_at
               ) VALUES (?,?,?,?,?,?,?,?,?,'PREFETCHED',?,?)""",
            (
                "LEASE-LEGACY",
                "phs-work-group:PHS-GROUP-001",
                "lease-issue-legacy",
                "a.b.c",
                encoded_artifact,
                encoded_binding,
                encoded_snapshot,
                "d" * 64,
                11,
                "2026-08-01T01:00:00Z",
                "2026-08-01T01:00:00Z",
            ),
        )

    migrated = OperationLeaseStore(database)

    assert migrated.get(lease_id="LEASE-LEGACY")["status"] == "PREFETCHED"
    attempt = migrated.get_issue_attempt(
        issue_idempotency_key="lease-issue-legacy"
    )
    assert attempt["lease_id"] == "LEASE-LEGACY"
    assert attempt["status"] == "ACTIVE"
    assert migrated.reserve_issue_attempt(issue_fingerprint())[
        "issue_idempotency_key"
    ] == "lease-issue-legacy"
    with sqlite3.connect(database) as connection:
        schema = connection.execute(
            """SELECT sql FROM sqlite_master
                 WHERE type='table' AND name='package_operation_leases'"""
        ).fetchone()[0]
    assert "resource_id TEXT NOT NULL UNIQUE" not in schema


def test_post_completion_conflict_atomically_preserves_local_work_for_review(tmp_path):
    database = tmp_path / "outbox.sqlite3"
    outbox = PackageOutbox(database)
    lease_store = OperationLeaseStore(database)
    draft = lease_draft()
    row = outbox.enqueue(draft)
    lease_store.save_prefetched(
        artifact={
            "token": draft.operation_lease_token,
            "operation_snapshot": {"source": "TRANSFER-1"},
            "claims": {
                "lease_id": draft.operation_lease_id,
                "resource_id": "phs-work-group:PHS-GROUP-001",
                "snapshot_hash": draft.operation_lease_snapshot_hash,
                "fence": draft.operation_lease_fence,
            },
        },
        binding=binding(),
        issue_idempotency_key="lease-issue-key",
    )
    lease_store.attach_set(draft.operation_lease_id, draft.set_id)
    outbox.mark_local_completion_committed(
        row["idempotency_key"],
        operation_lease_id=draft.operation_lease_id,
        operation_completed_at=draft.operation_lease_completed_at,
    )
    assert outbox.claim_next()["status"] == "SENDING"

    outbox.mark_conflict(
        row["idempotency_key"],
        PackageApiError(
            409,
            "OPERATION_LEASE_FENCE_CONFLICT",
            "stale lease fence",
            retryable=False,
        ),
        operation_lease_id=draft.operation_lease_id,
    )

    queued = outbox.get_by_set_id(draft.set_id)
    lease = lease_store.get(lease_id=draft.operation_lease_id)
    assert queued["status"] == "CONFLICT"
    assert queued["review_status"] == "OPERATOR_REVIEW"
    assert queued["local_completion_committed"] == 1
    assert lease["status"] == "OPERATOR_REVIEW"
    assert lease["operation_result_id"] == row["idempotency_key"]
    attempt = lease_store.get_issue_attempt(
        issue_idempotency_key="lease-issue-key"
    )
    assert attempt["status"] == "RETIRED"
    assert attempt["retire_reason"] == "OPERATOR_REVIEW"


def _refused_lease_fixture(tmp_path, monkeypatch, *, status="OPERATOR_REVIEW"):
    import package_logistics as module

    clock = [datetime(2026, 8, 1, 1, 30, tzinfo=timezone.utc)]
    monkeypatch.setattr(module, "utc_now", lambda: clock[0].strftime("%Y-%m-%dT%H:%M:%SZ"))
    monkeypatch.setattr(module, "_utc_after", lambda seconds:
        (clock[0] + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ"))
    database = tmp_path / "refused.sqlite3"
    outbox = PackageOutbox(database)
    store = OperationLeaseStore(database)
    snapshot = {"source": "TRANSFER-1"}
    claims = payload(snapshot)
    signed = Signer().sign(claims)
    draft = lease_draft(operation_lease_token=signed,
                        operation_lease_snapshot_hash=claims["snapshot_hash"])
    queued = outbox.enqueue(draft)
    store.save_prefetched(
        artifact={"token": signed, "operation_snapshot": snapshot,
                  "claims": claims, "expires_at": claims["expires_at"]},
        binding=binding(), issue_idempotency_key="lease-issue-refused",
    )
    store.attach_set(draft.operation_lease_id, draft.set_id)
    if status != "PREFETCHED":
        outbox.mark_local_completion_committed(queued["idempotency_key"],
            operation_lease_id=draft.operation_lease_id,
            operation_completed_at=draft.operation_lease_completed_at)
        claim = outbox.claim_next()
        if status == "OPERATOR_REVIEW":
            outbox.mark_conflict(queued["idempotency_key"],
                PackageApiError(409, "OPERATION_LEASE_FENCE_CONFLICT", "stale fence"),
                operation_lease_id=draft.operation_lease_id, expected_claim=claim)
        elif status == "ACKED":
            outbox.mark_acked(queued["idempotency_key"], {},
                operation_lease_id=draft.operation_lease_id, expected_claim=claim)
    return outbox, store, draft, clock


def _refusal_response(draft, *, replayed=False):
    # Exact eight-field data body from Web 374ddb77 service.py:955.
    return {"ok": True, "data": {
        "contract_version": "terminal-operation-lease-release-v1",
        "lease_id": draft.operation_lease_id, "status": "RECONCILIATION_REQUIRED",
        "fence": draft.operation_lease_fence, "resolution": "DEVICE_REPORTED_COMMAND_REFUSED",
        "reason": "COMMAND_REFUSED", "reported_at": "2026-08-01T01:30:00Z",
        "replayed": replayed,
    }}


def _default_clock_refused_lease(tmp_path, *, expired=False):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    completed = now - timedelta(minutes=2)
    snapshot = {"source": "TRANSFER-1"}
    claims = payload(snapshot,
        issued_at=(now - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        expires_at=(now + timedelta(minutes=-1 if expired else 30)).strftime("%Y-%m-%dT%H:%M:%SZ"))
    signed = Signer().sign(claims)
    draft = lease_draft(operation_lease_token=signed,
        operation_lease_snapshot_hash=claims["snapshot_hash"],
        operation_lease_completed_at=completed.strftime("%Y-%m-%dT%H:%M:%SZ"))
    outbox = PackageOutbox(tmp_path / "default-clock.sqlite3")
    store = OperationLeaseStore(outbox.db_path)
    row = outbox.enqueue(draft)
    store.save_prefetched(artifact={"token": signed, "operation_snapshot": snapshot,
        "claims": claims, "expires_at": claims["expires_at"]},
        binding=binding(), issue_idempotency_key="default-clock-issue")
    store.attach_set(draft.operation_lease_id, draft.set_id)
    outbox.mark_local_completion_committed(row["idempotency_key"],
        operation_lease_id=draft.operation_lease_id,
        operation_completed_at=draft.operation_lease_completed_at)
    claim = outbox.claim_next()
    outbox.mark_conflict(row["idempotency_key"],
        PackageApiError(409, "OPERATION_LEASE_FENCE_CONFLICT", "refused"),
        operation_lease_id=draft.operation_lease_id, expected_claim=claim)
    return outbox, draft


@pytest.mark.parametrize("expired", [False, True])
def test_refusal_default_clock_reports_or_expires_without_business_mutation(tmp_path, caplog, expired):
    caplog.set_level(logging.INFO, logger="package_outbox")
    outbox, draft = _default_clock_refused_lease(tmp_path, expired=expired)
    before = _business_bytes(outbox.db_path)
    calls = []
    def transport(*_args):
        calls.append(1)
        return _refusal_response(draft)
    processor = PackageOutboxProcessor(outbox, _refusal_client(transport))
    processor.drain(limit=1)
    processor.drain(limit=1)
    assert len(calls) == (0 if expired else 1)
    code = "EXPIRED_NOT_SENT" if expired else "REPORTED"
    assert [record.getMessage() for record in caplog.records if record.name == "package_outbox"] == [
        "package_operation_lease_refusal result=" + code]
    assert _business_bytes(outbox.db_path) == before


@pytest.mark.parametrize("invalid", ["clock", "lease"])
def test_refusal_invalid_local_clock_is_code_only_and_preserves_source(tmp_path, monkeypatch, caplog, invalid):
    import package_logistics as module
    caplog.set_level(logging.INFO, logger="package_outbox")
    outbox, draft = _default_clock_refused_lease(tmp_path)
    if invalid == "clock":
        monkeypatch.setattr(module, "utc_now", lambda: "invalid local clock")
    else:
        with sqlite3.connect(outbox.db_path) as conn:
            artifact = json.loads(conn.execute("SELECT artifact_json FROM package_operation_leases").fetchone()[0])
            artifact["expires_at"] = "2099-01-01T00:00:00.001Z"
            conn.execute("UPDATE package_operation_leases SET artifact_json=?", (json.dumps(artifact),))
    before = _business_bytes(outbox.db_path)
    processor = PackageOutboxProcessor(outbox, _refusal_client(
        lambda *_args: pytest.fail("invalid clock reached HTTP")))
    processor.drain(limit=1)
    assert caplog.records[-1].getMessage() == "package_operation_lease_refusal result=CLOCK_OR_LEASE_INVALID"
    assert draft.operation_lease_id not in processor._settled_refused_leases
    assert _business_bytes(outbox.db_path) == before


@pytest.mark.parametrize("limit,create_pending,timeout,receipt_pending", [(0, True, False, False), (1, True, False, False),
    (2, True, False, False), (2, True, True, False), (1, False, True, False),
    (1, False, False, True), (2, False, False, True)])
def test_refusal_drain_budget_prioritizes_create_and_bounds_followup_latency(monkeypatch, limit, create_pending, timeout, receipt_pending):
    from types import SimpleNamespace
    now = datetime.now(timezone.utc)
    draft = lease_draft()
    queue = [{"idempotency_key": "delivery-first", "draft_json": json.dumps(draft.to_dict())}] if create_pending else []
    events, result_codes = [], []
    elapsed = [0.0]
    default_timeout = _refusal_client(lambda *_args: None).config.timeout_seconds
    assert default_timeout == 8.0
    rows = [{"lease_id": "review-" + str(index), "fence": 1, "token": str(index),
        "artifact_json": json.dumps({"expires_at": (now + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")}),
        "binding_json": json.dumps({"authority_scope_id": "scope-main"})} for index in range(3)]
    outbox = SimpleNamespace(
        _utc_now=lambda: now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        pending_receipt_rechecks=lambda: [{"idempotency_key": "receipt-first", "draft_json": json.dumps(draft.to_dict()),
            "command_json": json.dumps({"authority_scope_id": "scope-main"}), "resolved_source_bundle_id": "TRANSFER-1"}] if receipt_pending else [],
        pending_refused_operation_leases=lambda: rows,
        claim_next=lambda **_kwargs: queue.pop(0) if queue else None,
        save_command=lambda *_args, **_kwargs: True,
        mark_acked=lambda *_args, **_kwargs: True,
        record_refused_operation_lease_result=lambda _row, code: result_codes.append(code) or True)
    class Client:
        def get_receipt_if_exists(self, *_args, **_kwargs):
            events.append("RECEIPT")
            return {}
        def build_create_package_command(self, _draft, *, idempotency_key):
            return "TRANSFER-1", {"authority_scope_id": "scope-main", "idempotency_key": idempotency_key}
        def create_package(self, _command):
            events.append("CREATE")
            return {}
        def report_refused_operation_lease(self, **_kwargs):
            events.append("REPORT")
            if timeout:
                elapsed[0] += default_timeout
                raise PackageTransportError("timeout")
            return {}
    processor = PackageOutboxProcessor(outbox, Client())
    monkeypatch.setattr(processor, "_validate_receipt", lambda *_args, **_kwargs: None)
    counts = processor.drain(limit=limit)
    created = int(create_pending and limit > 0)
    reports = int(limit > created + int(receipt_pending))
    assert counts == {"acked": created + int(receipt_pending), "retry": 0, "conflict": 0}
    assert events == (["RECEIPT"] if receipt_pending else []) + (["CREATE"] if created else []) + (["REPORT"] if reports else [])
    events.extend(["CANCEL", "REPLACE"])
    assert elapsed[0] == (default_timeout if reports and timeout else 0.0)
    assert elapsed[0] <= 8.0
    assert len(result_codes) == (reports if not timeout else 0)


def _refusal_client(transport):
    return PackageLogisticsClient(PackageClientConfig(
        "https://logistics.example.test", "test-machine-credential", "scope-main",
        "HOST-PACK-01", "PACK-01"), transport=transport)


def _business_bytes(database):
    # Return hashes only, so a failing assertion cannot dump signed evidence.
    with sqlite3.connect(database) as conn:
        schema = conn.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name").fetchall()
        tables = [row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        rows = {table: conn.execute('SELECT * FROM "' + table + '" ORDER BY rowid').fetchall()
                for table in tables}
    return (hashlib.sha256(canonical_json_bytes(schema)).hexdigest(),
            hashlib.sha256(canonical_json_bytes(rows)).hexdigest())


def test_refused_lease_report_next_cycle_restart_replay_and_business_bytes(
    tmp_path, monkeypatch, caplog, lock_remote,
):
    caplog.set_level(logging.INFO, logger="package_outbox")
    outbox, store, draft, _clock = _refused_lease_fixture(tmp_path, monkeypatch)
    before = _business_bytes(tmp_path / "refused.sqlite3")
    calls = []

    def transport(method, url, headers, body, _timeout):
        import writer_session_fence as fence
        assert getattr(fence._WRITER_LOCAL, "depth", 0) == 0
        lock_remote.transport("refused lease report")
        request = json.loads(body)
        assert (method, url.split(".test")[-1]) == (
            "POST", "/logistics/api/v1/operation-leases/release")
        assert set(request) == {"authority_scope_id", "operation", "token", "reason"}
        assert request["authority_scope_id"] == "scope-main"
        assert request["operation"] == "CREATE_PACKAGE"
        assert request["reason"] == "COMMAND_REFUSED"
        assert hashlib.sha256(request["token"].encode()).digest() == hashlib.sha256(
            store.get(lease_id=draft.operation_lease_id)["token"].encode()).digest()
        expected_key = "label-operation-lease-refused:" + hashlib.sha256(
            draft.operation_lease_id.encode()).hexdigest()
        assert headers["Idempotency-Key"] == expected_key
        calls.append((body, headers["Idempotency-Key"]))
        return _refusal_response(draft, replayed=len(calls) > 1)

    client = _refusal_client(transport)
    processor = PackageOutboxProcessor(outbox, client)
    assert processor.drain(limit=1) == {"acked": 0, "retry": 0, "conflict": 0}
    processor.drain(limit=1)
    assert len(calls) == 1
    restarted = PackageOutboxProcessor(PackageOutbox(outbox.db_path), client)
    restarted.drain(limit=1)
    restarted.drain(limit=1)
    assert len(calls) == 2
    assert calls[0] == calls[1]
    assert _business_bytes(tmp_path / "refused.sqlite3") == before
    assert [record.getMessage() for record in caplog.records
            if record.name == "package_outbox"] == [
        "package_operation_lease_refusal result=REPORTED"] * 2


@pytest.mark.parametrize("status", ["PREFETCHED", "LOCAL_COMPLETED", "ACKED"])
def test_refused_lease_reporting_ignores_non_review_status(tmp_path, monkeypatch, status):
    outbox, _store, _draft, _clock = _refused_lease_fixture(tmp_path, monkeypatch, status=status)
    monkeypatch.setattr(outbox, "claim_next", lambda **_kwargs: None)
    client = _refusal_client(lambda *_args: pytest.fail("unexpected refusal HTTP"))
    PackageOutboxProcessor(outbox, client).drain(limit=1)


def test_refused_lease_created_by_drain_reports_only_on_next_cycle(tmp_path, monkeypatch):
    outbox, store, draft, _clock = _refused_lease_fixture(tmp_path, monkeypatch, status="LOCAL_COMPLETED")
    key = outbox.get_by_set_id(draft.set_id)["idempotency_key"]
    outbox.mark_retry(key, PackageTransportError("ready for background delivery"))
    calls = []

    def transport(_method, url, _headers, _body, _timeout):
        calls.append(url.rsplit("/", 1)[-1])
        if url.endswith("/packages"):
            raise PackageApiError(409, "OPERATION_LEASE_FENCE_CONFLICT", "stale fence")
        return _refusal_response(draft)

    client = _refusal_client(transport)
    monkeypatch.setattr(client, "build_create_package_command", lambda _draft, *, idempotency_key:
        ("TRANSFER-1", {"authority_scope_id": "scope-main", "idempotency_key": idempotency_key}))
    processor = PackageOutboxProcessor(outbox, client)
    assert processor.drain(limit=1)["conflict"] == 1
    assert store.get(lease_id=draft.operation_lease_id)["status"] == "OPERATOR_REVIEW"
    assert calls == ["packages"]
    before = _business_bytes(tmp_path / "refused.sqlite3")
    processor.drain(limit=1)
    processor.drain(limit=1)
    assert calls == ["packages", "release"]
    assert _business_bytes(tmp_path / "refused.sqlite3") == before


def test_conflict_without_lease_and_conflict_rollback_do_not_report(tmp_path, monkeypatch):
    outbox, store, draft, _clock = _refused_lease_fixture(tmp_path, monkeypatch, status="LOCAL_COMPLETED")
    monkeypatch.setattr(outbox, "claim_next", lambda **_kwargs: None)
    before = _business_bytes(tmp_path / "refused.sqlite3")
    with sqlite3.connect(outbox.db_path) as conn:
        conn.execute("CREATE TRIGGER refuse_retirement BEFORE UPDATE ON package_operation_lease_issue_attempts "
                     "BEGIN SELECT RAISE(ABORT,'retirement failed'); END")
    with pytest.raises(sqlite3.IntegrityError, match="retirement failed"):
        outbox.mark_conflict(outbox.get_by_set_id(draft.set_id)["idempotency_key"],
            PackageApiError(409, "CONFLICT", "test conflict"), operation_lease_id=draft.operation_lease_id)
    with sqlite3.connect(outbox.db_path) as conn:
        conn.execute("DROP TRIGGER refuse_retirement")
    # SQL/rows are restored; SQLite page bookkeeping can differ after trigger DDL.
    assert _business_bytes(tmp_path / "refused.sqlite3")[:2] == before[:2]
    assert store.get(lease_id=draft.operation_lease_id)["status"] == "LOCAL_COMPLETED"
    client = _refusal_client(lambda *_args: pytest.fail("unexpected refusal HTTP"))
    processor = PackageOutboxProcessor(outbox, client)
    processor.drain(limit=1)
    outbox.mark_conflict(outbox.get_by_set_id(draft.set_id)["idempotency_key"],
                        PackageApiError(409, "CONFLICT", "no lease ID"))
    processor.drain(limit=1)


@pytest.mark.parametrize("status,code", [
    (409, "OPERATION_LEASE_ALREADY_CONSUMED"), (409, "OPERATION_LEASE_ALREADY_RELEASED"),
    (409, "OPERATION_LEASE_EVIDENCE_MISMATCH"), (409, "OPERATION_LEASE_DEVICE_MISMATCH"),
    (400, "OPERATION_LEASE_FIELD_INVALID"), (404, "HTTP_404"),
])
def test_refused_lease_terminal_rejection_is_code_only_and_not_retried(
    tmp_path, monkeypatch, caplog, status, code,
):
    caplog.set_level(logging.INFO, logger="package_outbox")
    outbox, _store, _draft, _clock = _refused_lease_fixture(tmp_path, monkeypatch)
    before = _business_bytes(tmp_path / "refused.sqlite3")
    calls = []
    def transport(*_args):
        calls.append(1)
        raise PackageApiError(status, code, "sensitive evidence must never be logged", retryable=True)
    client = _refusal_client(transport)
    processor = PackageOutboxProcessor(outbox, client)
    assert processor.drain(limit=1) == {'acked': 0, 'retry': 0, 'conflict': 0}
    assert processor.drain(limit=1) == {'acked': 0, 'retry': 0, 'conflict': 0}
    assert len(calls) == 1
    assert _business_bytes(tmp_path / "refused.sqlite3") == before
    assert caplog.records[-1].getMessage() == "package_operation_lease_refusal result=" + code
    assert "sensitive evidence" not in caplog.text

    # A terminal refusal must release the drain and allow the next ordinary
    # package to finish. Receipt contents have their own contract regressions.
    ordinary = lease_draft(
        set_id='following-package', operation_lease_id='',
        operation_lease_token='', operation_lease_fence=0,
        operation_lease_snapshot_hash='', operation_lease_completed_at='',
    )
    queued = outbox.enqueue(ordinary)
    outbox.mark_local_completion_committed(queued['idempotency_key'])
    created = []
    monkeypatch.setattr(client, 'build_create_package_command',
        lambda _draft, *, idempotency_key: (
            'following-source', {'authority_scope_id': 'scope-a',
                                 'idempotency_key': idempotency_key}))
    monkeypatch.setattr(client, 'create_package',
        lambda command: created.append(command['idempotency_key']) or {})
    monkeypatch.setattr(processor, '_validate_receipt', lambda *_args, **_kwargs: None)
    assert processor.drain(limit=1) == {'acked': 1, 'retry': 0, 'conflict': 0}
    assert created == [queued['idempotency_key']]
    assert outbox.get_by_set_id(ordinary.set_id)['status'] == 'ACKED'
    assert len(calls) == 1


@pytest.mark.parametrize("failure", ["connection", "timeout", 503, 429])
def test_refused_lease_transient_failure_retries_with_existing_backoff(
    tmp_path, monkeypatch, failure,
):
    outbox, _store, draft, clock = _refused_lease_fixture(tmp_path, monkeypatch)
    before = _business_bytes(tmp_path / "refused.sqlite3")
    calls = []
    def transport(*_args):
        calls.append(1)
        if len(calls) == 1:
            if isinstance(failure, str):
                raise PackageTransportError(failure)
            raise PackageApiError(failure, "RETRY", "wait", retry_after_seconds=10)
        return _refusal_response(draft)
    processor = PackageOutboxProcessor(outbox, _refusal_client(transport))
    processor.drain(limit=1)
    assert _business_bytes(tmp_path / "refused.sqlite3") == before
    if isinstance(failure, int):
        processor.drain(limit=1)
        assert len(calls) == 1
        clock[0] += timedelta(seconds=10)
    processor.drain(limit=1)
    processor.drain(limit=1)
    assert len(calls) == 2
    assert _business_bytes(tmp_path / "refused.sqlite3") == before


def test_expired_refused_lease_is_recorded_without_http(tmp_path, monkeypatch, caplog):
    caplog.set_level(logging.INFO, logger="package_outbox")
    outbox, _store, _draft, clock = _refused_lease_fixture(tmp_path, monkeypatch)
    clock[0] = datetime(2026, 8, 1, 3, 0, tzinfo=timezone.utc)
    before = _business_bytes(tmp_path / "refused.sqlite3")
    processor = PackageOutboxProcessor(outbox, _refusal_client(
        lambda *_args: pytest.fail("expired lease was sent")))
    processor.drain(limit=1)
    processor.drain(limit=1)
    assert caplog.records[-1].getMessage() == "package_operation_lease_refusal result=EXPIRED_NOT_SENT"
    assert _business_bytes(tmp_path / "refused.sqlite3") == before


@pytest.mark.parametrize("change", ["status", "evidence"])
def test_refused_lease_result_rechecks_exact_row_after_http(
    tmp_path, monkeypatch, caplog, change,
):
    caplog.set_level(logging.INFO, logger="package_outbox")
    outbox, _store, draft, _clock = _refused_lease_fixture(tmp_path, monkeypatch)
    def transport(*_args):
        with sqlite3.connect(outbox.db_path) as conn:
            if change == "status":
                conn.execute("UPDATE package_operation_leases SET status='ACKED'")
            else:
                conn.execute("UPDATE package_operation_leases SET updated_at='2026-08-01T01:31:00Z'")
        return _refusal_response(draft)
    processor = PackageOutboxProcessor(outbox, _refusal_client(transport))
    processor.drain(limit=1)
    assert draft.operation_lease_id not in processor._settled_refused_leases
    assert not [record for record in caplog.records if record.name == "package_outbox"]


def test_receipt_requires_exact_atomic_lease_consumption():
    draft = lease_draft()
    receipt = {
        "receipt_id": "RECEIPT-1",
        "operation_lease_consumption": {
            "contract_version": "terminal-operation-lease-consume-v1",
            "lease_id": draft.operation_lease_id,
            "status": "CONSUMED",
            "fence": draft.operation_lease_fence,
            "operation_result_id": "RECEIPT-1",
            "consumed_at": "2026-08-01T01:21:00Z",
        },
    }

    PackageOutboxProcessor._validate_operation_lease_receipt(draft, receipt)

    for field, replacement in (
        ("lease_id", "LEASE-OTHER"),
        ("fence", 12),
        ("operation_result_id", "RECEIPT-OTHER"),
    ):
        changed = copy.deepcopy(receipt)
        changed["operation_lease_consumption"][field] = replacement
        with pytest.raises(PackageLogisticsError):
            PackageOutboxProcessor._validate_operation_lease_receipt(
                draft, changed
            )


def test_second_pc_issue_conflict_does_not_create_a_local_business_lease():
    active_device = "PACK-01"

    def server_issue(device_id):
        if device_id != active_device:
            raise PackageApiError(
                409,
                "OPERATION_RESOURCE_ALREADY_LEASED",
                "resource already leased",
                retryable=False,
            )
        return {"contract_version": ARTIFACT_CONTRACT_VERSION}

    assert server_issue("PACK-01")["contract_version"] == ARTIFACT_CONTRACT_VERSION
    with pytest.raises(PackageApiError) as second:
        server_issue("PACK-02")
    assert second.value.code == "OPERATION_RESOURCE_ALREADY_LEASED"
    assert second.value.retryable is False

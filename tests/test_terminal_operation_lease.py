import base64
import copy
import json
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
        "resource_id": "PHS-GROUP-001",
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


def test_signed_lease_verifies_exact_terminal_and_source_binding(tmp_path):
    _signer, verifier, snapshot, claims, token = verified_fixture(tmp_path)

    verified = verifier.verify(
        token,
        expected=binding(),
        operation_snapshot=snapshot,
        now=datetime(2026, 8, 1, 1, 30, tzinfo=timezone.utc),
    )

    assert verified == claims


@pytest.mark.parametrize(
    ("mutation", "code"),
    (
        (lambda expected: expected.update(device_id="PACK-02"), "OPERATION_LEASE_BINDING_MISMATCH"),
        (lambda expected: expected.update(membership_hash="c" * 64), "OPERATION_LEASE_BINDING_MISMATCH"),
        (lambda expected: expected["expected_versions"].update({"bundle:TRANSFER-1": 5}), "OPERATION_LEASE_BINDING_MISMATCH"),
    ),
)
def test_lease_rejects_cross_device_membership_and_version_changes(
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
            "resource_id": "PHS-GROUP-001",
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
                "resource_id": "PHS-GROUP-001",
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

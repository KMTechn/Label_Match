"""Same-key enrollment continuation, entirely behind fake transport and keys."""

import copy
import datetime as dt
import json
import secrets
import subprocess
import uuid
from functools import lru_cache
from pathlib import Path
from types import SimpleNamespace

import pytest

from kmtech_zero_pe.possession_key import (
    REATTACH_AUDIENCE,
    REATTACH_PROOF_CONTRACT_VERSION,
    REATTACH_PROOF_FIELDS,
)
from tests.test_register_label_match_worker_pc import (
    fake_possession_descriptor,
    fake_v2_enrollment_response,
    load_registration_module,
)


@lru_cache(maxsize=1)
def _source_build_id():
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[1],
        text=True, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    ).strip()


@pytest.fixture
def reattach_case(tmp_path, monkeypatch):
    module = load_registration_module()
    import label_data_generation
    # A synthetic install lets focused RED run before the source is committed.
    monkeypatch.setattr(label_data_generation, "_installed_identity", lambda _root: {
        "version": label_data_generation.CURRENT_VERSION,
        "build_id": _source_build_id(),
        "data_compatible_min_version": label_data_generation.CURRENT_VERSION,
        "compatibility_verified": False,
    })
    data = tmp_path / "registration"
    data.mkdir()
    identity = {
        "schema_version": module.PRODUCER_IDENTITY_SCHEMA_VERSION,
        **{name: uuid.uuid4().hex for name in (
            "producer_id", "producer_install_id", "source_host_id", "pc_id"
        )},
    }
    identity_path = data / module.PRODUCER_IDENTITY_FILENAME
    identity_path.write_text(json.dumps(identity) + "\n", encoding="utf-8")
    original_identity = identity_path.read_bytes()
    case = SimpleNamespace(
        module=module, data=data, identity=identity, identity_path=identity_path,
        original_identity=original_identity, calls=[], signed=[], opened=[],
        challenge_change=lambda value: None, response_change=lambda value: None,
        failure="", secret=secrets.token_hex(32), signature=secrets.token_bytes(64),
        reused_initial_key=False, initial_key_calls=[],
    )

    class Key:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def descriptor(self):
            value = fake_possession_descriptor(created=False)
            return SimpleNamespace(as_dict=lambda: value)

        # Exercise the real proof validator and canonical signing encoder.
        sign_reattach_proof = module.PersistentPossessionKey.sign_reattach_proof

        def sign_es256(self, payload):
            case.signed.append(json.loads(payload))
            return case.signature

    def open_existing(**kwargs):
        case.opened.append(kwargs)
        if case.failure == "key_unavailable" and len(case.opened) == 2:
            raise module.AdminRecoveryRequired(
                "unavailable", "open", "recovery", "unavailable"
            )
        return Key()

    def no_new_key(**_kwargs):
        if case.reused_initial_key:
            case.initial_key_calls.append("reused")
            return Key()
        pytest.fail("existing identity must never provision a key")

    monkeypatch.setattr(module.PersistentPossessionKey, "open_existing", open_existing)
    monkeypatch.setattr(module.PersistentPossessionKey, "provision_initial", no_new_key)
    monkeypatch.setattr(module, "_write_dpapi_secret", lambda *_args, **_kw: data / "protected")
    monkeypatch.setattr(module, "_verify_dpapi_secret", lambda *_args: True)

    def post(url, **kwargs):
        case.calls.append((url, copy.deepcopy(kwargs)))
        payload = kwargs["json"]
        if url.endswith("/v2/enroll"):
            case.enrollment = payload
            code = ("producer_identity_conflict" if case.failure == "different_key"
                    else "reattach_proof_required")
            return SimpleNamespace(status_code=409, json=lambda: {"error": {"code": code}})
        stage = "challenge" if url.endswith("/challenge") else "reattach"
        if case.failure == stage + "_timeout":
            raise module.requests.exceptions.Timeout()
        if case.failure == stage + "_http":
            return SimpleNamespace(status_code=409, json=lambda: {
                "error": {"code": "reattach_proof_invalid"}
            })
        if case.failure == stage + "_json":
            def invalid_json():
                raise ValueError()
            return SimpleNamespace(status_code=200, json=invalid_json)
        if stage == "challenge":
            proof = {
                "contract_version": REATTACH_PROOF_CONTRACT_VERSION,
                "audience": REATTACH_AUDIENCE,
                "challenge_id": "reattach-" + uuid.uuid4().hex,
                "nonce": secrets.token_urlsafe(32),
                "expires_at": (dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5))
                .strftime("%Y-%m-%dT%H:%M:%SZ"),
                **{name: payload[name] for name in (
                    "producer_id", "producer_install_id", "source_host_id", "manifest_hash"
                )},
            }
            case.proof = copy.deepcopy(proof)
            result = {
                "contract_version": "producer-reattach-challenge-v1",
                "proof_payload": proof,
                "possession_key_fingerprint": fake_possession_descriptor()["fingerprint"],
            }
            case.challenge_change(result)
        else:
            if payload["proof"] != case.proof:
                return SimpleNamespace(status_code=409, json=lambda: {
                    "error": {"code": "reattach_proof_invalid"}
                })
            assert payload["signature"] == module.b64url_encode(case.signature)
            result = fake_v2_enrollment_response(module, case.enrollment, case.secret)
            for value in (result, result["client_receipt"]):
                value.update(contract_version="producer-reattach-complete-v1",
                             status="reattached", identity_action="REATTACHED",
                             credential_epoch=3)
                value.pop("server_binding", None)
            result["key_id"] = uuid.uuid4().hex
            result["client_receipt"]["key_id"] = result["key_id"]
            case.response_change(result)
            case.response = copy.deepcopy(result)
        return SimpleNamespace(status_code=200, json=lambda: result)

    monkeypatch.setattr(module.requests, "post", post)
    case.args = [
        "--apply", "--server-base-url", "https://registration.invalid",
        "--credential-scope", "current_user", "--enrollment-token-env", "",
        "--data-dir", str(data), "--sync-dir", str(tmp_path / "sync"),
    ]
    case.run = lambda: module.main(case.args)
    case.report = lambda: json.loads(
        (data / "status" / module.DEFAULT_REPORT_FILENAME).read_text(encoding="utf-8")
    )
    return case


@pytest.mark.parametrize("authorization_state", ["OPERATION_PENDING", "OPERATION_READY"])
def test_same_key_409_reattaches_and_persists_rotated_credentials(reattach_case, authorization_state, capsys):
    case = reattach_case
    def ready(response):
        response["authorization_state"] = authorization_state
        response["client_receipt"]["authorization_state"] = authorization_state
    case.response_change = ready
    assert case.run() == 0
    assert len(case.calls) == 3
    assert [url.rsplit("/", 1)[-1] for url, _ in case.calls] == ["enroll", "challenge", "reattach"]
    challenge = case.calls[1][1]
    assert challenge["json"] == {
        "contract_version": "producer-reattach-challenge-request-v1",
        **{name: case.proof[name] for name in (
            "producer_id", "producer_install_id", "source_host_id", "manifest_hash"
        )},
    }
    assert set(case.proof) == REATTACH_PROOF_FIELDS
    assert case.signed == [case.proof]
    assert len(case.opened) == 2
    report = case.report()
    assert report["status"] == "SELF_ENROLLMENT_REGISTERED"
    assert report["server_registration_verified"] is True
    assert report["secret_bootstrap_verified"] is True
    assert report["identity_action"] == "REATTACHED"
    assert report["credential_epoch"] == 3
    assert report["authorization_state"] == authorization_state
    assert report["registration_contract_version"] == "producer-reattach-complete-v1"
    assert report["registration_action"] == "possession_reattach"
    assert all(kwargs["allow_redirects"] is False for _, kwargs in case.calls[1:])
    saved_identity = json.loads(case.identity_path.read_text(encoding="utf-8"))
    assert saved_identity == case.identity
    credential = json.loads((case.data / case.module.DEFAULT_CREDENTIAL_FILENAME).read_text(encoding="utf-8"))
    assert credential["producer_id"] == case.identity["producer_id"]
    assert credential["key_id"] == case.response["key_id"]
    receipt = json.loads((case.data / "evidence" / case.module.DEFAULT_RECEIPT_FILENAME).read_text(encoding="utf-8"))
    assert receipt["credential_epoch"] == 3
    assert receipt["possession_key_fingerprint"] == fake_possession_descriptor()["fingerprint"]
    assert all(case.secret not in p.read_text(encoding="utf-8") for p in case.data.rglob("*.json"))
    console = capsys.readouterr()
    assert case.secret not in console.out + console.err
    assert case.proof["nonce"] not in console.out + console.err
    assert case.module.b64url_encode(case.signature) not in console.out + console.err


def test_reinstalled_identity_reuses_retained_key_for_reattach(reattach_case, monkeypatch):
    case = reattach_case
    case.identity_path.unlink()
    case.reused_initial_key = True
    monkeypatch.setattr(case.module, "_current_machine_guid", lambda: uuid.uuid4().hex)
    monkeypatch.setattr(case.module, "_current_user_sid", lambda: "S-1-5-21-101-102-103-1001")
    assert case.run() == 0
    assert case.initial_key_calls == ["reused"]
    assert len(case.opened) == 1
    assert case.report()["possession_key_created"] is False
    assert case.report()["producer_identity_source"] == "generated"
    saved = json.loads(case.identity_path.read_text(encoding="utf-8"))
    assert all(saved[field] == case.proof[field] for field in (
        "producer_id", "producer_install_id", "source_host_id"
    ))


def test_reattach_preserves_explicit_authentication_and_tls(reattach_case, capsys):
    case = reattach_case
    authorization = secrets.token_hex(32)
    ca = case.data / "test-ca.pem"
    ca.write_bytes(b"")
    case.args += ["--enrollment-token", authorization, "--tls-ca-bundle-path", str(ca)]
    assert case.run() == 0
    assert len(case.calls) == 3
    assert all(request["headers"]["X-Producer-Enrollment-Token"] == authorization
               and request["verify"] == str(ca) for _, request in case.calls)
    console = capsys.readouterr()
    assert authorization not in console.out + console.err
    assert all(authorization not in p.read_text(encoding="utf-8")
               for p in case.data.rglob("*.json"))


def test_different_key_conflict_keeps_audited_admin_recovery(reattach_case):
    case = reattach_case
    case.failure = "different_key"
    assert case.run() == 2
    assert len(case.calls) == 1
    assert case.report()["status"] == case.module.ADMIN_RECOVERY_ACTION
    assert case.identity_path.read_bytes() == case.original_identity
    assert case.signed == []


@pytest.mark.parametrize("failure,requests", [
    ("challenge_timeout", 2), ("challenge_http", 2), ("challenge_json", 2),
    ("reattach_timeout", 3), ("reattach_http", 3), ("reattach_json", 3),
    ("key_unavailable", 1),
])
def test_reattach_failure_preserves_required_state_without_retry(reattach_case, failure, requests):
    case = reattach_case
    case.failure = failure
    assert case.run() == 2
    assert len(case.calls) == requests
    if failure == "key_unavailable":
        assert len(case.opened) == 2
    assert case.report()["status"] == "POSSESSION_PROOF_REATTACH_REQUIRED"
    assert case.identity_path.read_bytes() == case.original_identity
    assert not (case.data / case.module.DEFAULT_CREDENTIAL_FILENAME).exists()


@pytest.mark.parametrize("field", [
    "contract_version", "audience", "challenge_id", "nonce", "expires_at",
    "producer_id", "producer_install_id", "source_host_id", "manifest_hash",
    "extra", "missing", "fingerprint", "expired", "far_future", "nonce_mismatch",
])
def test_invalid_challenge_is_never_signed(reattach_case, field):
    case = reattach_case
    def change(result):
        proof = result["proof_payload"]
        if field == "extra":
            proof["unexpected"] = "invalid"
        elif field == "missing":
            proof.pop("nonce")
        elif field == "fingerprint":
            result["possession_key_fingerprint"] = "invalid"
        elif field == "nonce_mismatch":
            proof["nonce"] = secrets.token_urlsafe(32)
        elif field in {"expired", "far_future"}:
            seconds = -1 if field == "expired" else 1200
            proof["expires_at"] = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(seconds=seconds))
            proof["expires_at"] = proof["expires_at"].strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            proof[field] = "invalid"
    case.challenge_change = change
    assert case.run() == 2
    assert len(case.calls) == (3 if field == "nonce_mismatch" else 2)
    assert len(case.signed) == (1 if field == "nonce_mismatch" else 0)
    assert case.report()["status"] == "POSSESSION_PROOF_REATTACH_REQUIRED"
    assert case.identity_path.read_bytes() == case.original_identity


@pytest.mark.parametrize("field", [
    "producer_id", "producer_install_id", "source_host_id", "endpoint_url",
    "active_manifest_hashes", "credential_epoch", "authorization_state", "secret_fingerprint_sha256",
    "possession_key", "client_receipt",
])
def test_invalid_rotated_response_is_not_persisted(reattach_case, field):
    case = reattach_case
    case.response_change = lambda value: value.__setitem__(field, None)
    assert case.run() == 2
    assert len(case.calls) == 3
    assert case.report()["status"] == "POSSESSION_PROOF_REATTACH_REQUIRED"
    assert case.identity_path.read_bytes() == case.original_identity
    assert not (case.data / case.module.DEFAULT_CREDENTIAL_FILENAME).exists()

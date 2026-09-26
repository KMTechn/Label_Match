"""Use the real registration validators with a fake HTTPS peer and fake KSP."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
import requests

from current_user_onboarding import resolve_current_user_onboarding_paths
from enrollment_mutex import EnrollmentMutex
from tests._possession_fixture import fake_possession_descriptor, TEST_POSSESSION_FINGERPRINT
from tests.test_register_label_match_worker_pc import fake_v2_enrollment_response
from tools import register_label_match_worker_pc as registration


class Reply:
    def __init__(self, status, value, headers=None):
        self.status_code, self.value = status, value
        self.headers = headers or {}
        self.content = b"bounded-synthetic-response"

    def json(self):
        return self.value


class Peer:
    def __init__(self, replies):
        self.replies, self.calls = list(replies), []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def get(self, url, **kwargs):
        return self.post(url, **kwargs)

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        assert kwargs["allow_redirects"] is False
        result = self.replies.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result


@pytest.fixture
def candidate(tmp_path, monkeypatch):
    paths = resolve_current_user_onboarding_paths(tmp_path / "packet", environ={"LOCALAPPDATA": str(tmp_path / "local")})
    target = {"origin": "https://fresh.example.invalid", "scope": "TEST1-FRESH", "pc": "LABEL-PC01",
              "acceptance": {"source_commit": "c" * 40}}
    monkeypatch.setattr(registration, "_current_machine_guid", lambda: "00112233-4455-6677-8899-aabbccddeeff")
    monkeypatch.setattr(registration, "_current_user_sid", lambda: "S-1-5-21-100-200-300-1001")
    monkeypatch.delenv("PRODUCER_SELF_ENROLL_TOKEN", raising=False)
    value = registration.prepare_fresh_candidate(paths, target, "a" * 32)
    descriptor = fake_possession_descriptor(created=False)
    signed = []

    class Key:
        @staticmethod
        def open_existing(**kwargs):
            assert kwargs == {"scope": "current_user"}
            return Key()

        @staticmethod
        def provision_initial(**kwargs):
            pytest.fail("fresh-server transition created or replaced the shared KSP key")

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def descriptor(self):
            return SimpleNamespace(fingerprint=descriptor["fingerprint"], as_dict=lambda: dict(descriptor))

        def sign_reattach_proof(self, proof):
            signed.append(proof)
            return "synthetic-signature"

    monkeypatch.setattr(registration, "PersistentPossessionKey", Key)
    state = {"phase": "REGISTERING", "candidate": value, "target": target, "possession_key": descriptor,
             "server_preflight": {"initial_challenge_status": 404, "initial_challenge_code": "producer_identity_not_found"}}
    return paths, state, signed


def response(state, *, reattached=False):
    candidate, target = state["candidate"], state["target"]
    request = {"manifest": candidate["manifest"], "producer_id": candidate["producer_id"],
               "key_id": candidate["credential"]["key_id"], "endpoint_url": candidate["credential"]["endpoint_url"]}
    result = fake_v2_enrollment_response(registration, request, "synthetic-new-producer-secret")
    if reattached:
        for payload in (result, result["client_receipt"]):
            payload.update(contract_version="producer-reattach-complete-v1", status="reattached",
                           identity_action="REATTACHED", credential_epoch=3)
    result["machine_credential_bundle"] = {
        "contract_version": "producer-self-enrollment-machine-credentials-v1",
        "bindings": {"app": "LabelMatch", "program": "Label_Match", "source_host_id": candidate["source_host_id"],
                     "device_id": target["pc"], "authority_scope_id": target["scope"]},
        "credentials": {
            "producer_ingest": {"audience": "producer-ingest-hmac-v1", "auth_scheme": "hmac-sha256",
                                "key_id": result["key_id"], "secret": result["secret"]},
            "logistics": {"audience": "worker-analysis-logistics-v1", "auth_scheme": "bearer",
                          "token_header": "X-Logistics-API-Token", "token": "synthetic-new-logistics-token"}},
        "profiles": {"logistics": {"contract_version": "km-logistics-runtime-profile-v1",
                    "base_url": target["origin"], "authority_scope": target["scope"], "authority_epoch": 1,
                    "authority_plane": "AUTHORITATIVE", "ledger_plane": "AUTHORITATIVE", "plane_epoch": 1,
                    "device_id": target["pc"], "source_host_id": candidate["source_host_id"], "timeout_seconds": 10}}}
    return result


def challenge(state):
    return {"contract_version": "producer-reattach-challenge-v1",
            "possession_key_fingerprint": TEST_POSSESSION_FINGERPRINT,
            "proof_payload": {"contract_version": "producer-reattach-proof-v1",
                              "audience": "worker-analysis-producer-reattach-v1",
                              "challenge_id": "reattach-" + "b" * 32, "nonce": "A" * 43,
                              "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=3)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                              **registration._fresh_bindings(state["candidate"])}}


@pytest.mark.parametrize("status,code", [(200, ""), (404, "wrong_source"), (403, "producer_credential_disabled"),
                                         (409, "producer_identity_conflict"), (302, "")])
def test_only_exact_absence_can_authorize_detachment(candidate, monkeypatch, status, code):
    paths, state, _signed = candidate
    peer = Peer([Reply(200, {}, {"X-KMTech-Source-Commit": "c" * 40}), Reply(status, {"error": {"code": code}})])
    monkeypatch.setattr(registration, "_fresh_session", lambda *a: peer)
    with EnrollmentMutex(), pytest.raises(registration.DirectSyncPushError, match="absence"):
        registration.preflight_fresh_server(paths, state["target"], state["candidate"])


def test_preflight_checks_source_even_when_url_is_unchanged(candidate, monkeypatch):
    paths, state, _signed = candidate
    peer = Peer([Reply(200, {}, {"X-KMTech-Source-Commit": "d" * 40})])
    monkeypatch.setattr(registration, "_fresh_session", lambda *a: peer)
    with EnrollmentMutex(), pytest.raises(registration.DirectSyncPushError, match="source commit"):
        registration.preflight_fresh_server(paths, state["target"], state["candidate"])
    assert len(peer.calls) == 1


def test_initial_registration_uses_only_existing_ksp_and_new_credentials(candidate, monkeypatch):
    paths, state, signed = candidate
    peer = Peer([Reply(200, response(state))])
    monkeypatch.setattr(registration, "_fresh_session", lambda *a: peer)
    with EnrollmentMutex():
        result = registration.enroll_fresh_candidate(paths, state)
    assert result["credential_epoch"] == 1
    assert peer.calls[0][1]["headers"] == {}
    assert "old" not in peer.calls[0][1]["json"]
    assert signed == []


def test_commit_then_lost_response_reattaches_same_candidate(candidate, monkeypatch):
    paths, state, signed = candidate
    peers = [Peer([requests.Timeout("synthetic lost response")]),
             Peer([Reply(409, {"error": {"code": "reattach_proof_required"}}),
                   Reply(200, challenge(state)), Reply(200, response(state, reattached=True))])]
    monkeypatch.setattr(registration, "_fresh_session", lambda *a: peers.pop(0))
    original = deepcopy(state["candidate"])
    with EnrollmentMutex():
        with pytest.raises(requests.Timeout):
            registration.enroll_fresh_candidate(paths, state)
        result = registration.enroll_fresh_candidate(paths, state)
    assert state["candidate"] == original
    assert result["credential_epoch"] == 3 and result["identity_action"] == "REATTACHED"
    assert len(signed) == 1 and signed[0]["manifest_hash"] == original["manifest_hash"]


@pytest.mark.parametrize("field,value", [("producer_id", "producer-other-valid"),
                                         ("producer_install_id", "install-other-valid"),
                                         ("manifest_hash", "f" * 64), ("nonce", "short")])
def test_wrong_challenge_is_never_signed(candidate, monkeypatch, field, value):
    paths, state, signed = candidate
    proof = challenge(state)
    proof["proof_payload"][field] = value
    peer = Peer([Reply(409, {"error": {"code": "reattach_proof_required"}}), Reply(200, proof)])
    monkeypatch.setattr(registration, "_fresh_session", lambda *a: peer)
    with EnrollmentMutex(), pytest.raises((ValueError, registration.DirectSyncPushError)):
        registration.enroll_fresh_candidate(paths, state)
    assert signed == []


@pytest.mark.parametrize("mutation", ["scope", "identity", "key", "epoch_bool", "origin", "receipt"])
def test_well_formed_wrong_binding_is_rejected_before_publication(candidate, mutation):
    _paths, state, _signed = candidate
    value = response(state)
    if mutation == "scope":
        value["machine_credential_bundle"]["bindings"]["authority_scope_id"] = "TEST1-OTHER"
        value["machine_credential_bundle"]["profiles"]["logistics"]["authority_scope"] = "TEST1-OTHER"
    elif mutation == "identity":
        value["producer_id"] = value["client_receipt"]["producer_id"] = "producer-valid-other"
    elif mutation == "key":
        value["possession_key"]["fingerprint"] = "other-valid-fingerprint"
        value["client_receipt"]["possession_key_fingerprint"] = "other-valid-fingerprint"
    elif mutation == "epoch_bool":
        value["credential_epoch"] = value["client_receipt"]["credential_epoch"] = True
    elif mutation == "origin":
        value["machine_credential_bundle"]["profiles"]["logistics"]["base_url"] = "https://other.example.invalid"
    else:
        value["client_receipt"]["active_manifest_hashes"] = ["f" * 64]
    with pytest.raises(registration.DirectSyncPushError):
        registration._validate_fresh_response(value, state["candidate"], state["target"], TEST_POSSESSION_FINGERPRINT)


def test_no_initial_absence_receipt_means_no_automatic_reattach(candidate, monkeypatch):
    paths, state, _signed = candidate
    state.pop("server_preflight")
    monkeypatch.setattr(registration, "_fresh_session", lambda *a: pytest.fail("unauthorized network request"))
    with EnrollmentMutex(), pytest.raises(registration.DirectSyncPushError, match="absence receipt"):
        registration.enroll_fresh_candidate(paths, state)


def test_fresh_transport_disables_environment_proxies(monkeypatch):
    session = SimpleNamespace(trust_env=True)
    monkeypatch.setattr(registration.requests, "Session", lambda: session)
    assert registration._fresh_session().trust_env is False

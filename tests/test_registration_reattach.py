import copy
import datetime
import hashlib
import json
from types import SimpleNamespace

import pytest

from enrollment_mutex import EnrollmentMutex
from tests._possession_fixture import fake_possession_descriptor
from tests.test_register_label_match_worker_pc import load_registration_module, fake_v2_enrollment_response


def _case(tmp_path):
    module = load_registration_module()
    data = tmp_path / "producer"
    profile = tmp_path / "profile" / "runtime-profile.json"
    ca = tmp_path / "source-ca.pem"
    ca.write_bytes(b"")
    args = SimpleNamespace(data_dir=str(data), sync_dir=str(tmp_path / "business"),
                           logistics_profile_path=str(profile), tls_ca_bundle_path=str(ca),
                           enrollment_timeout_seconds=30, report_path="", identity_path="",
                           manifest_path="", credential_path="", receipt_path="", credential_scope="current_user")
    manifest = module._build_manifest(
        pc_id="test-pc", source_host_id="test-source", producer_install_id="test-install",
        sync_dir=args.sync_dir, data_dir=args.data_dir,
        endpoint_url="https://registration.invalid/api/producer-ingest/v1/event-batches",
        secret_ref="dpapi:test-reference", identity_registry_status="self_enrolled",
    )
    credential = {"producer_id": "test-producer", "key_id": "test-key-reference",
                  "endpoint_url": manifest["sync"]["server_ingest_target"],
                  "secret_data_dir": str(data), "secret_ref": "dpapi:test-reference"}
    descriptor = fake_possession_descriptor(created=False)
    proof = {"contract_version": module.REATTACH_PROOF_VERSION,
             "audience": module.REATTACH_AUDIENCE, "challenge_id": "test-challenge",
             "nonce": "test-nonce", "expires_at": (datetime.datetime.now(datetime.timezone.utc)
                 + datetime.timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ"),
             "producer_id": credential["producer_id"], "source_host_id": "test-source",
             "producer_install_id": "test-install", "manifest_hash": module.manifest_hash(manifest)}
    challenge = {"contract_version": module.REATTACH_CHALLENGE_VERSION,
                 "proof_payload": proof, "possession_key_fingerprint": descriptor["fingerprint"]}
    payload = {"manifest": manifest, **credential}
    response = fake_v2_enrollment_response(module, payload, hashlib.sha256(b"").hexdigest())
    changes = {"contract_version": module.REATTACH_COMPLETE_VERSION, "status": "reattached",
               "identity_action": "REATTACHED", "credential_epoch": 2}
    response.update(changes)
    response["client_receipt"].update(changes)
    return module, args, manifest, credential, descriptor, challenge, response


def _install_transport(monkeypatch, module, descriptor, challenge, response):
    calls, signed = [], []
    session = SimpleNamespace(close=lambda: calls.append("closed"))
    monkeypatch.setattr(module, "_open_admin_recovery_session", lambda _ca: session)

    def post(payload, **kwargs):
        calls.append((copy.deepcopy(payload), kwargs))
        return copy.deepcopy(challenge if len(calls) == 1 else response)

    class Key:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def descriptor(self):
            return SimpleNamespace(as_dict=lambda: dict(descriptor))

        def assert_non_exportable(self):
            signed.append("nonexportable")

        def sign_es256(self, payload):
            signed.append(payload)
            return bytes(64)

    monkeypatch.setattr(module, "_enroll", post)
    monkeypatch.setattr(module.PersistentPossessionKey, "open_existing", staticmethod(lambda **_kwargs: Key()))
    monkeypatch.setattr(module.PersistentPossessionKey, "provision_initial",
                        staticmethod(lambda **_kwargs: pytest.fail("reattach must keep the existing key")))
    return calls, signed


def test_reattach_signs_exact_bound_challenge_and_verifies_new_generation(tmp_path, monkeypatch):
    module, args, manifest, credential, descriptor, challenge, response = _case(tmp_path)
    calls, signed = _install_transport(monkeypatch, module, descriptor, challenge, response)
    progress = module._AdminRecoveryProgress()
    with EnrollmentMutex():
        result = module._reattach(args, manifest, credential, descriptor, "", progress)
    assert result == response
    assert signed == ["nonexportable", module.canonical_json_bytes(challenge["proof_payload"])]
    assert calls[0][1]["enrollment_url"].endswith(module.REATTACH_PATH + "/challenge")
    assert calls[1][1]["enrollment_url"].endswith(module.REATTACH_PATH)
    assert calls[1][0]["proof"] == challenge["proof_payload"]
    assert calls[1][0]["manifest"] == manifest
    assert calls[0][1]["session"] is calls[1][1]["session"]
    assert calls[-1] == "closed"
    assert progress.server_credential_rotated and progress.fresh_local_state


@pytest.mark.parametrize("field", ["producer_id", "source_host_id", "producer_install_id", "manifest_hash",
                                  "audience", "contract_version", "expires_at", "extra"])
def test_reattach_rejects_changed_or_expired_challenge_before_signing(tmp_path, monkeypatch, field):
    module, args, manifest, credential, descriptor, challenge, response = _case(tmp_path)
    challenge["proof_payload"][field] = "changed"
    calls, signed = _install_transport(monkeypatch, module, descriptor, challenge, response)
    with EnrollmentMutex(), pytest.raises(module.DirectSyncPushError):
        module._reattach(args, manifest, credential, descriptor, "", module._AdminRecoveryProgress())
    assert signed == []
    assert len(calls) == 2 and calls[-1] == "closed"


def test_reattach_foreign_key_requires_administrator_without_signing(tmp_path, monkeypatch):
    module, args, manifest, credential, descriptor, challenge, response = _case(tmp_path)
    challenge["possession_key_fingerprint"] = hashlib.sha256(b"different").hexdigest()
    calls, signed = _install_transport(monkeypatch, module, descriptor, challenge, response)
    with EnrollmentMutex(), pytest.raises(module.PossessionKeyRecoveryRequired) as caught:
        module._reattach(args, manifest, credential, descriptor, "", module._AdminRecoveryProgress())
    assert caught.value.key_state["reason"] == "registered_possession_key_differs"
    assert signed == [] and calls[-1] == "closed"


@pytest.mark.parametrize("target", ["producer_identity.json", "credential.json", "queue", "spool",
                                  "relay_queue", "relay_state.sqlite3"])
def test_reattach_preserves_existing_local_state_before_http(tmp_path, monkeypatch, target):
    module, args, manifest, credential, descriptor, challenge, response = _case(tmp_path)
    path = tmp_path / "producer" / target
    path.parent.mkdir(parents=True)
    path.write_bytes(b"")
    calls, signed = _install_transport(monkeypatch, module, descriptor, challenge, response)
    with EnrollmentMutex(), pytest.raises(module.DirectSyncPushError):
        module._reattach(args, manifest, credential, descriptor, "", module._AdminRecoveryProgress())
    assert calls == [] and signed == [] and path.exists()


@pytest.mark.parametrize("target,field", [("response", "credential_epoch"), ("response", "producer_id"),
                                         ("receipt", "manifest_hash"), ("receipt", "key_id")])
def test_reattach_rejects_unbound_completion_and_records_uncertain_request(tmp_path, monkeypatch, target, field):
    module, args, manifest, credential, descriptor, challenge, response = _case(tmp_path)
    selected = response if target == "response" else response["client_receipt"]
    selected["active_manifest_hashes" if field == "manifest_hash" else field] = None
    calls, signed = _install_transport(monkeypatch, module, descriptor, challenge, response)
    progress = module._AdminRecoveryProgress()
    with EnrollmentMutex(), pytest.raises(module.DirectSyncPushError):
        module._reattach(args, manifest, credential, descriptor, "", progress)
    assert progress.possession_reattach_completion_requested
    assert not progress.server_credential_rotated and calls[-1] == "closed"


@pytest.mark.parametrize("resume", [False, True])
def test_apply_registration_recovers_409_and_uses_verified_reattach_receipt(tmp_path, monkeypatch, resume):
    module, args, manifest, credential, descriptor, challenge, response = _case(tmp_path)
    args.enrollment_token = args.enrollment_token_file = args.enrollment_token_env = ""
    args.credential_scope = "current_user"
    args.require_machine_credential_bundle = False
    report = {"producer_identity_source": "generated", "enrollment_url": "https://registration.invalid/enroll",
              "producer_install_id": "test-install", "source_host_id": "test-source",
              "manifest_hash": module.manifest_hash(manifest)}
    monkeypatch.setattr(module, "_prepare_possession_key", lambda _report: descriptor)
    if resume:
        module._write_json(tmp_path / "producer" / "status" / module.DEFAULT_REPORT_FILENAME, {
            "report_version": "label-match-worker-pc-registration-v1",
            "status": "POSSESSION_PROOF_REATTACH_REQUIRED", "recovery_action": "POSSESSION_PROOF_REATTACH_REQUIRED",
            "secret_material_persisted": False, "endpoint_url": credential["endpoint_url"],
            "producer_id": credential["producer_id"], "producer_install_id": "test-install",
            "source_host_id": "test-source",
        })

    def enroll(*_args, **_kwargs):
        if resume:
            pytest.fail("a prior 409 must resume without enrolling again")
        raise module.ProducerEnrollmentHTTPError(409, "reattach_proof_required", "")

    def reattach(*arguments):
        arguments[-1].possession_reattach_attempted = True
        arguments[-1].server_credential_rotated = True
        return response

    monkeypatch.setattr(module, "_enroll", enroll)
    monkeypatch.setattr(module, "_reattach", reattach)
    monkeypatch.setattr(module, "_validate_v2_enrollment_response", lambda *_args, **_kwargs: pytest.fail("initial receipt cannot validate reattach"))
    monkeypatch.setattr(module, "ensure_runtime_profile_from_enrollment_bundle", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(module, "_write_dpapi_secret", lambda *_args, **_kwargs: tmp_path / "protected.dpapi")
    monkeypatch.setattr(module, "_verify_dpapi_secret", lambda *_args, **_kwargs: True)
    result = module._apply_registration_locked(args, manifest, credential, report)
    assert result["registration_action"] == "possession_reattach"
    assert result["registration_contract_version"] == module.REATTACH_COMPLETE_VERSION
    assert result["credential_epoch"] == 2
    assert result["v2_client_receipt_verified"] and result["server_registration_verified"]
    assert result["client_receipt"]["status"] == "reattached"


@pytest.mark.parametrize("field", [None, "endpoint_url", "producer_id", "producer_install_id", "source_host_id"])
def test_prior_409_report_resumes_only_the_same_user_identity_and_origin(tmp_path, field):
    module, args, manifest, credential, descriptor, challenge, response = _case(tmp_path)
    previous = {"report_version": "label-match-worker-pc-registration-v1",
                "status": "POSSESSION_PROOF_REATTACH_REQUIRED", "recovery_action": "POSSESSION_PROOF_REATTACH_REQUIRED",
                "secret_material_persisted": False, "endpoint_url": credential["endpoint_url"],
                "producer_id": credential["producer_id"], "producer_install_id": "test-install",
                "source_host_id": "test-source"}
    if field is not None:
        previous[field] = "changed"
    path = tmp_path / "producer" / "status" / module.DEFAULT_REPORT_FILENAME
    module._write_json(path, previous)
    if field is None:
        assert module._prior_reattach_requested(args, manifest, credential)
    else:
        with pytest.raises(module.DirectSyncPushError):
            module._prior_reattach_requested(args, manifest, credential)


def test_reattach_expired_challenge_does_not_sign_or_complete(tmp_path, monkeypatch):
    module, args, manifest, credential, descriptor, challenge, response = _case(tmp_path)
    challenge["proof_payload"]["expires_at"] = (datetime.datetime.now(datetime.timezone.utc)
        - datetime.timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    calls, signed = _install_transport(monkeypatch, module, descriptor, challenge, response)
    with EnrollmentMutex(), pytest.raises(module.DirectSyncPushError):
        module._reattach(args, manifest, credential, descriptor, "", module._AdminRecoveryProgress())
    assert signed == [] and len(calls) == 2


def test_reattach_rejects_unowned_enrollment_before_transport(tmp_path, monkeypatch):
    from enrollment_mutex import EnrollmentMutexError
    module, args, manifest, credential, descriptor, challenge, response = _case(tmp_path)
    calls, signed = _install_transport(monkeypatch, module, descriptor, challenge, response)
    with pytest.raises(EnrollmentMutexError):
        module._reattach(args, manifest, credential, descriptor, "", module._AdminRecoveryProgress())
    assert calls == [] and signed == []


@pytest.mark.parametrize("status", [200, 302])
def test_reattach_transport_pins_session_and_refuses_redirects(tmp_path, monkeypatch, status):
    module, args, manifest, credential, descriptor, challenge, response = _case(tmp_path)
    calls = []
    session = SimpleNamespace(post=lambda url, **kwargs: calls.append((url, kwargs)) or
                              SimpleNamespace(status_code=status, json=lambda: challenge))
    with EnrollmentMutex():
        arguments = dict(enrollment_url="https://registration.invalid/reattach/challenge",
                         enrollment_token="", timeout_seconds=30, session=session)
        if status == 200:
            assert module._enroll({}, **arguments) == challenge
        else:
            with pytest.raises(module.DirectSyncPushError):
                module._enroll({}, **arguments)
    assert calls[0][1]["allow_redirects"] is False
    assert "X-Producer-Enrollment-Token" not in calls[0][1]["headers"]


def test_reattach_refuses_machine_credentials_before_transport(tmp_path, monkeypatch):
    module, args, manifest, credential, descriptor, challenge, response = _case(tmp_path)
    args.credential_scope = "machine"
    calls, signed = _install_transport(monkeypatch, module, descriptor, challenge, response)
    with EnrollmentMutex(), pytest.raises(module.DirectSyncPushError):
        module._reattach(args, manifest, credential, descriptor, "", module._AdminRecoveryProgress())
    assert calls == [] and signed == []


def test_reattach_preserves_broken_queue_link_before_transport(tmp_path, monkeypatch):
    module, args, manifest, credential, descriptor, challenge, response = _case(tmp_path)
    original = module.os.path.lexists
    target = tmp_path / "producer" / "relay_queue"
    monkeypatch.setattr(module.os.path, "lexists", lambda path: True if str(path) == str(target) else original(path))
    calls, signed = _install_transport(monkeypatch, module, descriptor, challenge, response)
    with EnrollmentMutex(), pytest.raises(module.DirectSyncPushError):
        module._reattach(args, manifest, credential, descriptor, "", module._AdminRecoveryProgress())
    assert calls == [] and signed == []


def test_registration_keeps_409_recovery_action_when_reattach_preflight_fails(tmp_path, monkeypatch):
    module, args, manifest, credential, descriptor, challenge, response = _case(tmp_path)
    report = {"report_version": "label-match-worker-pc-registration-v1", "status": "APPLY_REQUESTED",
              "producer_identity_source": "generated", "enrollment_url": "https://registration.invalid/enroll",
              "producer_id": credential["producer_id"], "producer_install_id": "test-install",
              "source_host_id": "test-source", "endpoint_url": credential["endpoint_url"],
              "manifest_hash": module.manifest_hash(manifest)}
    monkeypatch.setattr(module, "_registration_storage_preflight", lambda _args: None)
    monkeypatch.setattr(module, "check_user_data_open", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(module, "build_payloads", lambda _args: (manifest, credential, report))
    monkeypatch.setattr(module, "_prepare_possession_key", lambda _report: descriptor)

    def enroll(*_args, **_kwargs):
        raise module.ProducerEnrollmentHTTPError(409, "reattach_proof_required", "")

    def refused(*_args, **_kwargs):
        raise module.DirectSyncPushError("local preflight refused")

    monkeypatch.setattr(module, "_enroll", enroll)
    monkeypatch.setattr(module, "_reattach", refused)
    path = tmp_path / "producer" / "status" / module.DEFAULT_REPORT_FILENAME
    result = module.main(["--apply", "--data-dir", args.data_dir, "--sync-dir", args.sync_dir,
                          "--credential-scope", "current_user", "--enrollment-token-env", "",
                          "--report-path", str(path)])
    assert result == 2
    saved = json.loads(path.read_text(encoding="utf-8-sig"))
    assert saved["recovery_action"] == "POSSESSION_PROOF_REATTACH_REQUIRED"
    assert saved["status"] == "POSSESSION_PROOF_REATTACH_REQUIRED"
    assert saved["secret_material_persisted"] is False
    assert not (tmp_path / "producer" / "producer_identity.json").exists()

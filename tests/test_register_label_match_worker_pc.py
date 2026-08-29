import importlib.util
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


TEST_MACHINE_GUID = "00112233-4455-6677-8899-aabbccddeeff"
TEST_USER_SID = "S-1-5-21-100-200-300-1001"


def load_registration_module():
    module_path = Path(__file__).resolve().parents[1] / "tools" / "register_label_match_worker_pc.py"
    spec = importlib.util.spec_from_file_location("register_label_match_worker_pc_for_tests", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def generated_install_id(module, *, user_sid=TEST_USER_SID, app_id=None):
    return module.derive_path_independent_install_id(
        machine_guid=TEST_MACHINE_GUID,
        user_sid=user_sid,
        app_id=app_id or module.INSTALL_IDENTITY_APP_ID,
    )


TEST_POSSESSION_JWK = {
    "kty": "EC",
    "crv": "P-256",
    "x": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "y": "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
}
TEST_POSSESSION_FINGERPRINT = "test-possession-fingerprint"


def fake_possession_descriptor(*, created=True):
    return {
        "contract_version": "producer-machine-possession-key-v1",
        "provider_name": "Microsoft Software Key Storage Provider",
        "key_name": "KMTech.DirectSync.Possession.v1",
        "scope": "current_user",
        "unique_name": "test-unique-name",
        "created": created,
        "public_jwk": dict(TEST_POSSESSION_JWK),
        "fingerprint": TEST_POSSESSION_FINGERPRINT,
        "machine_key": False,
        "export_policy": 0,
        "key_usage": 2,
    }


def fake_v2_enrollment_response(
    module, payload, secret, *, authorization_state="OPERATION_PENDING"
):
    identity = payload["manifest"]["pc_identity"]
    active_manifest_hashes = [module.manifest_hash(payload["manifest"])]
    response = {
        "contract_version": module.ENROLLMENT_CONTRACT_VERSION,
        "status": "enrolled",
        "identity_action": "CREATED",
        "authorization_state": authorization_state,
        "credential_epoch": 1,
        "producer_id": payload["producer_id"],
        "key_id": payload["key_id"],
        "secret": secret,
        "secret_fingerprint_sha256": module._fingerprint(secret),
        "endpoint_url": payload["endpoint_url"],
        "source_host_id": identity["source_host_id"],
        "producer_install_id": identity["producer_install_id"],
        "active_manifest_hashes": active_manifest_hashes,
        "possession_key": {
            "contract_version": "producer-machine-possession-key-v1",
            "fingerprint": TEST_POSSESSION_FINGERPRINT,
        },
        "server_binding": {
            "producer_manifest_path": "/srv/producers/label/producer_manifest.json",
            "registry_path": "/srv/producers/label/source_registry.json",
        },
    }
    response["client_receipt"] = {
        "receipt_schema_version": "producer-self-enrollment-client-receipt-v1",
        "contract_version": module.ENROLLMENT_CONTRACT_VERSION,
        "status": "enrolled",
        "identity_action": "CREATED",
        "authorization_state": response["authorization_state"],
        "credential_epoch": 1,
        "producer_id": payload["producer_id"],
        "key_id": payload["key_id"],
        "secret_fingerprint_sha256": module._fingerprint(secret),
        "endpoint_url": payload["endpoint_url"],
        "source_host_id": identity["source_host_id"],
        "producer_install_id": identity["producer_install_id"],
        "active_manifest_hashes": active_manifest_hashes,
        "possession_key_fingerprint": TEST_POSSESSION_FINGERPRINT,
        "server_binding": dict(response["server_binding"]),
    }
    return response


def test_label_match_registration_dry_run_derives_per_pc_identity_without_secret(
    tmp_path, monkeypatch
):
    module = load_registration_module()
    monkeypatch.setattr(module, "_current_user_sid", lambda: TEST_USER_SID)
    report_path = tmp_path / "registration-dry-run.json"
    result = module.main(
        [
            "--dry-run",
            "--server-base-url",
            "https://worker.example.invalid",
            "--pc-id",
            "PACKING-PC-01",
            "--machine-guid",
            TEST_MACHINE_GUID,
            "--sync-dir",
            str(tmp_path / "Label_Match" / "data"),
            "--data-dir",
            str(tmp_path / "DirectSync" / "label_match"),
            "--report-path",
            str(report_path),
        ]
    )

    assert result == 0
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))
    report_text = report_path.read_text(encoding="utf-8-sig")
    assert report["status"] == "DRY_RUN"
    assert report["manual_pc_approval_required"] is False
    assert report["source_host_id"].startswith("label-match-packing-pc-01-")
    assert report["producer_id"] == f"producer-{report['source_host_id']}"
    assert report["key_id"] == f"key-{report['source_host_id']}"
    assert report["endpoint_url"] == "https://worker.example.invalid/api/producer-ingest/v1/source-file"
    assert report["server_registration_verified"] is False
    assert report["secret_bootstrap_verified"] is False
    assert TEST_MACHINE_GUID not in report_text
    assert "secret" not in report.get("secret_ref", "")


def test_fresh_guests_derive_distinct_per_pc_identity_without_source_host_override(
    tmp_path, monkeypatch
):
    module = load_registration_module()
    monkeypatch.setattr(module, "_current_user_sid", lambda: TEST_USER_SID)
    reports = []
    machine_guids = (
        TEST_MACHINE_GUID,
        "11112233-4455-6677-8899-aabbccddeeff",
    )
    for index, machine_guid in enumerate(machine_guids):
        report_path = tmp_path / f"fresh-{index}.json"
        assert module.main(
            [
                "--dry-run",
                "--server-base-url",
                "https://worker.example.invalid",
                "--pc-id",
                "PACKING-GUEST",
                "--machine-guid",
                machine_guid,
                "--sync-dir",
                str(tmp_path / f"label-{index}"),
                "--data-dir",
                str(tmp_path / f"relay-{index}"),
                "--report-path",
                str(report_path),
            ]
        ) == 0
        reports.append(json.loads(report_path.read_text(encoding="utf-8-sig")))

    assert reports[0]["source_host_id"] != reports[1]["source_host_id"]
    assert all(item["manual_pc_approval_required"] is False for item in reports)
    assert all(item["source_host_id"].startswith("label-match-packing-guest-") for item in reports)


def test_path_independent_install_identity_fixed_vector_and_collision_boundaries():
    module = load_registration_module()
    install_id = generated_install_id(module)

    assert install_id == "label-match-install-0fb8a3d24086e8b02e19d4861e95df92"
    assert (
        module.derive_path_independent_install_id(
            machine_guid="{00112233-4455-6677-8899-AABBCCDDEEFF}",
            user_sid=TEST_USER_SID.lower(),
            app_id=module.INSTALL_IDENTITY_APP_ID.upper(),
        )
        == install_id
    )
    assert (
        generated_install_id(
            module,
            user_sid="S-1-5-21-100-200-300-1002",
        )
        != install_id
    )
    assert generated_install_id(module, app_id="defect_inspection") != install_id


def test_generated_install_id_ignores_application_and_state_paths(
    tmp_path, monkeypatch
):
    module = load_registration_module()
    monkeypatch.setattr(module, "_current_user_sid", lambda: TEST_USER_SID)
    ids = []
    for name in ("a", "b"):
        report_path = tmp_path / "reports" / f"path-{name}.json"
        assert module.main(
            [
                "--dry-run",
                "--server-base-url",
                "https://worker.example.invalid",
                "--pc-id",
                "PATH-PROBE",
                "--machine-guid",
                TEST_MACHINE_GUID,
                "--sync-dir",
                str(tmp_path / f"release-{name}" / "data"),
                "--data-dir",
                str(tmp_path / f"state-{name}"),
                "--report-path",
                str(report_path),
            ]
        ) == 0
        report = json.loads(report_path.read_text(encoding="utf-8-sig"))
        assert (
            report["producer_install_id_derivation"]
            == module.INSTALL_IDENTITY_DERIVATION_VERSION
        )
        ids.append(report["producer_install_id"])

    assert ids == [generated_install_id(module), generated_install_id(module)]


def test_existing_identity_file_precedes_machine_and_user_lookups(
    tmp_path, monkeypatch
):
    module = load_registration_module()
    identity_path = tmp_path / "state" / module.PRODUCER_IDENTITY_FILENAME
    identity_path.parent.mkdir(parents=True)
    pinned = {
        "schema_version": module.PRODUCER_IDENTITY_SCHEMA_VERSION,
        "producer_id": "legacy-label-producer",
        "source_host_id": "legacy-label-host",
        "producer_install_id": "legacy-label-install-id",
        "pc_id": "LEGACY-LABEL-PC",
    }
    identity_path.write_text(json.dumps(pinned) + "\n", encoding="utf-8")
    report_path = tmp_path / "reports" / "identity-reuse.json"
    monkeypatch.setattr(
        module,
        "_current_machine_guid",
        lambda: (_ for _ in ()).throw(
            AssertionError("persisted identity must bypass machine lookup")
        ),
    )
    monkeypatch.setattr(
        module,
        "_current_user_sid",
        lambda: (_ for _ in ()).throw(
            AssertionError("persisted identity must bypass user lookup")
        ),
    )

    assert module.main(
        [
            "--dry-run",
            "--server-base-url",
            "https://worker.example.invalid",
            "--data-dir",
            str(identity_path.parent),
            "--report-path",
            str(report_path),
        ]
    ) == 0
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))

    assert report["producer_identity_source"] == "identity_file"
    assert report["producer_install_id_derivation"] == "identity_file"
    assert report["producer_install_id"] == pinned["producer_install_id"]
    assert report["source_host_id"] == pinned["source_host_id"]
    assert report["producer_id"] == pinned["producer_id"]


def test_cli_install_id_precedes_existing_identity_file(tmp_path, monkeypatch):
    module = load_registration_module()
    identity_path = tmp_path / "state" / module.PRODUCER_IDENTITY_FILENAME
    identity_path.parent.mkdir(parents=True)
    pinned = {
        "schema_version": module.PRODUCER_IDENTITY_SCHEMA_VERSION,
        "producer_id": "legacy-label-producer",
        "source_host_id": "legacy-label-host",
        "producer_install_id": "legacy-label-install-id",
    }
    identity_path.write_text(json.dumps(pinned) + "\n", encoding="utf-8")
    report_path = tmp_path / "reports" / "cli-identity.json"
    monkeypatch.setattr(
        module,
        "_current_machine_guid",
        lambda: (_ for _ in ()).throw(AssertionError("machine lookup not expected")),
    )
    monkeypatch.setattr(
        module,
        "_current_user_sid",
        lambda: (_ for _ in ()).throw(AssertionError("user lookup not expected")),
    )

    assert module.main(
        [
            "--dry-run",
            "--server-base-url",
            "https://worker.example.invalid",
            "--data-dir",
            str(identity_path.parent),
            "--producer-install-id",
            "cli-label-install-id",
            "--report-path",
            str(report_path),
        ]
    ) == 0
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))

    assert report["producer_identity_source"] == "cli"
    assert report["producer_install_id_derivation"] == "cli"
    assert report["producer_install_id"] == "cli-label-install-id"


def test_identity_file_rejects_duplicate_keys(tmp_path):
    module = load_registration_module()
    identity_path = tmp_path / module.PRODUCER_IDENTITY_FILENAME
    identity_path.write_text(
        '{"schema_version":"label-match-producer-identity-v1",'
        '"producer_id":"first","producer_id":"second",'
        '"source_host_id":"host","producer_install_id":"install"}\n',
        encoding="utf-8",
    )

    with pytest.raises(module.DirectSyncPushError, match="duplicate key"):
        module._load_producer_identity_file(identity_path)


def test_label_match_registration_manifest_includes_runtime_event_contract(tmp_path):
    module = load_registration_module()
    args = type(
        "Args",
        (),
        {
            "data_dir": str(tmp_path / "DirectSync" / "label_match"),
            "endpoint_url": "",
            "key_id": "",
            "machine_guid": TEST_MACHINE_GUID,
            "pc_id": "PACKING-PC-CONTRACT",
            "producer_id": "",
            "producer_install_id": "",
            "secret_ref_target": "",
            "server_base_url": "https://worker.example.invalid",
            "source_host_id": "",
            "sync_dir": str(tmp_path / "Label_Match" / "data"),
            "dry_run": True,
            "enrollment_url": "",
        },
    )()

    manifest, _credential, _report = module.build_payloads(args)
    raw_event_names = manifest["streams"][0]["raw_event_names"]

    assert raw_event_names == [
        "APP_CLOSE",
        "APP_START",
        "BASE64_DECODED",
        "ERROR_INPUT",
        "ERROR_MISMATCH",
        "LABEL_MATCHED",
        "PACKAGING_WAITING_OBSERVED",
        "PHS_REPLACEMENT_WAITING_MARKED",
        "POST_REVIEW_REQUIRED",
        "SCAN_ATTEMPT",
        "SCAN_OK",
        "SET_CANCELLED",
        "SET_DELETED",
        "SET_RESTORED",
        "SHIPPING_WAITING_OBSERVED",
        "TRAY_COMPLETE",
        "TRAY_COMPLETION_CANCELLED",
        "UI_ERROR",
    ]
    assert raw_event_names == module.RAW_EVENT_NAMES
    catalog_path = (
        Path(__file__).resolve().parents[1]
        / "kmtech_factory_contracts"
        / "bundle"
        / "v1"
        / "catalogs"
        / "canonical-stream-catalog.json"
    )
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    label_stream = next(
        row
        for row in catalog["streams"]
        if row["app_id"] == "label_match"
        and row["stream_id"] == "label_match_events"
    )
    assert raw_event_names == label_stream["raw_event_names"]
    data_root = (tmp_path / "DirectSync" / "label_match").resolve()
    sync_root = (tmp_path / "Label_Match" / "data").resolve()
    assert manifest["paths"] == {
        "data_dir": data_root.as_posix(),
        "evidence_dir": (data_root / "evidence").as_posix(),
        "rollback_dir": (data_root / "rollback").as_posix(),
    }
    assert manifest["sync"]["sync_dir"] == sync_root.as_posix()
    assert manifest["sync"]["queue"]["queue_dir"] == (
        data_root / "relay_queue"
    ).as_posix()
    assert manifest["sync"]["queue"]["client_state_db"] == (
        data_root / "relay_state.sqlite3"
    ).as_posix()


def test_label_match_registration_apply_writes_manifest_credential_and_receipt_without_raw_secret(
    tmp_path,
    monkeypatch,
):
    module = load_registration_module()
    data_dir = tmp_path / "DirectSync" / "label_match"
    sync_dir = tmp_path / "Label_Match" / "data"
    report_path = data_dir / "status" / "registration.json"
    secret = "server-issued-secret"

    def fake_enroll(
        payload,
        *,
        enrollment_url,
        enrollment_token,
        timeout_seconds,
        tls_ca_bundle_path="",
    ):
        assert enrollment_token == "install-token"
        assert payload["contract_version"] == module.ENROLLMENT_CONTRACT_VERSION
        assert enrollment_url.endswith("/api/producer-ingest/v2/enroll")
        assert payload["possession_public_jwk"] == TEST_POSSESSION_JWK
        assert payload["manifest"]["streams"][0]["stream_name"] == "label_match_events"
        return fake_v2_enrollment_response(module, payload, secret)

    monkeypatch.setattr(module, "_enroll", fake_enroll)
    monkeypatch.setattr(
        module,
        "_prepare_possession_key",
        lambda _report: fake_possession_descriptor(),
    )
    monkeypatch.setattr(module, "_write_dpapi_secret", lambda data_dir, target, secret_text: Path(data_dir) / "secrets" / f"{target}.dpapi")
    monkeypatch.setattr(module, "_verify_dpapi_secret", lambda data_dir, target, secret_text: secret_text == secret)

    result = module.main(
        [
            "--apply",
            "--server-base-url",
            "https://worker.example.invalid",
            "--enrollment-token",
            "install-token",
            "--pc-id",
            "PACKING-PC-02",
            "--machine-guid",
            "22222222-4455-6677-8899-aabbccddeeff",
            "--sync-dir",
            str(sync_dir),
            "--data-dir",
            str(data_dir),
            "--report-path",
            str(report_path),
        ]
    )

    assert result == 0
    manifest_path = data_dir / "producer_manifest.json"
    credential_path = data_dir / "credential.json"
    receipt_path = data_dir / "evidence" / "producer_self_enrollment_receipt.json"
    assert manifest_path.is_file()
    assert credential_path.is_file()
    assert receipt_path.is_file()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    credential = json.loads(credential_path.read_text(encoding="utf-8-sig"))
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))
    combined_text = "\n".join(
        [
            manifest_path.read_text(encoding="utf-8-sig"),
            credential_path.read_text(encoding="utf-8-sig"),
            receipt_path.read_text(encoding="utf-8-sig"),
            report_path.read_text(encoding="utf-8-sig"),
        ]
    )

    assert manifest["identity_registry"]["status"] == "self_enrolled"
    assert manifest["sync"]["sync_transport"] == "http_push"
    assert manifest["sync"]["sync_dir"] == sync_dir.resolve().as_posix()
    assert credential["secret_ref"].startswith("dpapi:")
    assert credential["secret_data_dir"] == str(data_dir.resolve())
    assert "secret" not in credential
    assert report["status"] == "SELF_ENROLLMENT_REGISTERED"
    assert report["server_registration_verified"] is True
    assert report["secret_bootstrap_verified"] is True
    assert report["possession_binding_verified"] is True
    assert report["possession_key_scope"] == "current_user"
    assert report["v2_client_receipt_verified"] is True
    assert report["manual_pc_approval_required"] is False
    assert "server-issued-secret" not in combined_text
    assert "install-token" not in combined_text


def test_label_match_registration_apply_can_use_ip_allowlisted_server_without_token(tmp_path, monkeypatch):
    module = load_registration_module()
    data_dir = tmp_path / "DirectSync" / "label_match"
    sync_dir = tmp_path / "Label_Match" / "data"
    report_path = data_dir / "status" / "registration.json"
    secret = "server-issued-secret"

    def fake_enroll(
        payload,
        *,
        enrollment_url,
        enrollment_token,
        timeout_seconds,
        tls_ca_bundle_path="",
    ):
        assert enrollment_token == ""
        return fake_v2_enrollment_response(module, payload, secret)

    monkeypatch.setattr(module, "_enroll", fake_enroll)
    monkeypatch.setattr(
        module,
        "_prepare_possession_key",
        lambda _report: fake_possession_descriptor(),
    )
    monkeypatch.setattr(module, "_write_dpapi_secret", lambda data_dir, target, secret_text: Path(data_dir) / "secrets" / f"{target}.dpapi")
    monkeypatch.setattr(module, "_verify_dpapi_secret", lambda data_dir, target, secret_text: secret_text == secret)

    result = module.main(
        [
            "--apply",
            "--server-base-url",
            "https://worker.example.invalid",
            "--enrollment-token-env",
            "",
            "--sync-dir",
            str(sync_dir),
            "--data-dir",
            str(data_dir),
            "--report-path",
            str(report_path),
        ]
    )

    assert result == 0
    report = json.loads(report_path.read_text(encoding="utf-8-sig"))
    assert report["status"] == "SELF_ENROLLMENT_REGISTERED"
    assert report["token_source"] == "ip_allowlist"
    assert report["manual_pc_approval_required"] is False
    assert report["raw_secret_written"] is False


def test_current_user_registration_selects_current_user_dpapi_and_profile_scope(
    tmp_path, monkeypatch
):
    module = load_registration_module()
    data_dir = tmp_path / "DirectSync" / "label_match"
    tls_ca_bundle_path = tmp_path / "private-ca.cert.pem"
    tls_ca_bundle_path.write_bytes(b"private-ca-fixture")
    args = type(
        "Args",
        (),
        {
            "credential_scope": "current_user",
            "data_dir": str(data_dir),
            "endpoint_url": "",
            "enrollment_timeout_seconds": 30,
            "enrollment_token": "",
            "enrollment_token_env": "",
            "enrollment_token_file": "",
            "enrollment_url": "",
            "key_id": "",
            "logistics_profile_path": str(tmp_path / "profile.json"),
            "machine_guid": TEST_MACHINE_GUID,
            "pc_id": "PACKING-USER",
            "producer_id": "",
            "producer_install_id": "",
            "require_machine_credential_bundle": True,
            "secret_ref_target": "",
            "server_base_url": "https://worker.example.invalid",
            "source_host_id": "",
            "sync_dir": str(tmp_path / "Label_Match" / "data"),
            "tls_ca_bundle_path": str(tls_ca_bundle_path),
            "dry_run": False,
        },
    )()
    manifest, credential, report = module.build_payloads(args)
    secret = "server-issued-secret"
    observed = {}

    def fake_enroll(payload, **_kwargs):
        response = fake_v2_enrollment_response(module, payload, secret)
        response["machine_credential_bundle"] = {"present": True}
        return response

    monkeypatch.setattr(
        module,
        "_enroll",
        fake_enroll,
    )
    monkeypatch.setattr(
        module,
        "_prepare_possession_key",
        lambda _report: fake_possession_descriptor(),
    )
    monkeypatch.setattr(
        module,
        "ensure_runtime_profile_from_enrollment_bundle",
        lambda _payload, **kwargs: observed.update(profile=kwargs)
        or {"status": "installed", "created_paths": []},
    )
    monkeypatch.setattr(
        module,
        "_write_dpapi_secret",
        lambda data_dir, target, secret_text, *, credential_scope: observed.update(
            secret_scope=credential_scope
        )
        or Path(data_dir)
        / "secrets"
        / f"{target}.dpapi",
    )
    monkeypatch.setattr(module, "_verify_dpapi_secret", lambda *_args: True)

    applied = module.apply_registration(args, manifest, credential, report)

    assert credential["dpapi_scope"] == "current_user"
    assert observed["secret_scope"] == "current_user"
    assert observed["profile"]["credential_scope"] == "current_user"
    assert observed["profile"]["tls_ca_bundle_path"] == str(tls_ca_bundle_path)
    assert applied["credential_scope"] == "current_user"
    assert applied["server_registration_verified"] is True


def test_enrollment_uses_explicit_private_ca_bundle(monkeypatch, tmp_path):
    module = load_registration_module()
    ca_bundle = tmp_path / "private-ca.pem"
    ca_bundle.write_bytes(b"private-ca-fixture")
    observed = {}

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return {"status": "enrolled"}

    monkeypatch.setattr(
        module.requests,
        "post",
        lambda url, **kwargs: observed.update(url=url, kwargs=kwargs) or Response(),
    )

    with module.EnrollmentMutex():
        result = module._enroll(
            {"contract_version": module.ENROLLMENT_CONTRACT_VERSION},
            enrollment_url="https://worker.example.invalid/api/producer-ingest/v2/enroll",
            enrollment_token="",
            timeout_seconds=30,
            tls_ca_bundle_path=str(ca_bundle),
        )

    assert result["status"] == "enrolled"
    assert observed["kwargs"]["verify"] == str(ca_bundle)


def test_vendored_zero_pe_sources_match_pinned_hash_manifest():
    root = Path(__file__).resolve().parents[1]
    manifest = json.loads(
        (root / "kmtech_zero_pe.vendor.json").read_text(encoding="utf-8")
    )

    assert manifest["source_commit"] == "67db9569bcf7f1eacebeed664f00b4c51e48ff54"
    assert set(manifest["files"]) == {
        "kmtech_zero_pe/__init__.py",
        "kmtech_zero_pe/cng_p256.py",
        "kmtech_zero_pe/gdi_print.py",
        "kmtech_zero_pe/possession_key.py",
        "kmtech_zero_pe/raster.py",
        "kmtech_zero_pe/release_signature.py",
    }
    for relative_path, expected_sha256 in manifest["files"].items():
        assert hashlib.sha256((root / relative_path).read_bytes()).hexdigest() == expected_sha256


def test_new_identity_provisions_only_current_user_possession_key(monkeypatch):
    module = load_registration_module()
    calls = []

    class Descriptor:
        def as_dict(self):
            return fake_possession_descriptor()

    class Key:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def descriptor(self):
            return Descriptor()

    monkeypatch.setattr(
        module.PersistentPossessionKey,
        "provision_initial",
        staticmethod(lambda **kwargs: calls.append(("provision", kwargs)) or Key()),
    )
    monkeypatch.setattr(
        module.PersistentPossessionKey,
        "open_existing",
        staticmethod(
            lambda **_kwargs: (_ for _ in ()).throw(
                AssertionError("new identity must use provision_initial")
            )
        ),
    )

    descriptor = module._prepare_possession_key(
        {"producer_identity_source": "generated"}
    )

    assert descriptor["scope"] == "current_user"
    assert calls == [("provision", {"scope": module.SCOPE_CURRENT_USER})]


def test_existing_identity_opens_key_without_provisioning(monkeypatch):
    module = load_registration_module()
    calls = []

    class Descriptor:
        def as_dict(self):
            return fake_possession_descriptor(created=False)

    class Key:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def descriptor(self):
            return Descriptor()

    monkeypatch.setattr(
        module.PersistentPossessionKey,
        "provision_initial",
        staticmethod(
            lambda **_kwargs: (_ for _ in ()).throw(
                AssertionError("existing identity must never provision a key")
            )
        ),
    )
    monkeypatch.setattr(
        module.PersistentPossessionKey,
        "open_existing",
        staticmethod(lambda **kwargs: calls.append(("open", kwargs)) or Key()),
    )

    descriptor = module._prepare_possession_key(
        {"producer_identity_source": "identity_file"}
    )

    assert descriptor["created"] is False
    assert calls == [("open", {"scope": module.SCOPE_CURRENT_USER})]


def test_existing_legacy_identity_missing_key_reports_admin_recovery_without_mutation(
    tmp_path, monkeypatch
):
    module = load_registration_module()
    data_dir = tmp_path / "state"
    data_dir.mkdir()
    identity_path = data_dir / module.PRODUCER_IDENTITY_FILENAME
    identity_payload = {
        "schema_version": module.PRODUCER_IDENTITY_SCHEMA_VERSION,
        "producer_id": "legacy-label-producer",
        "source_host_id": "legacy-label-host",
        "producer_install_id": "legacy-label-install-id",
        "pc_id": "LEGACY-LABEL-PC",
    }
    identity_path.write_text(json.dumps(identity_payload) + "\n", encoding="utf-8")
    identity_before = identity_path.read_bytes()
    report_path = data_dir / "status" / "registration.json"

    monkeypatch.setattr(
        module.PersistentPossessionKey,
        "provision_initial",
        staticmethod(
            lambda **_kwargs: (_ for _ in ()).throw(
                AssertionError("legacy identity must never provision a replacement key")
            )
        ),
    )
    monkeypatch.setattr(
        module.PersistentPossessionKey,
        "open_existing",
        staticmethod(
            lambda **_kwargs: (_ for _ in ()).throw(
                module.AdminRecoveryRequired(
                    "KMTech.DirectSync.Possession.v1",
                    "Microsoft Software Key Storage Provider",
                    "current_user",
                    "possession key is missing or cannot be opened",
                    status=0x80090016,
                )
            )
        ),
    )

    result = module.main(
        [
            "--apply",
            "--server-base-url",
            "https://worker.example.invalid",
            "--data-dir",
            str(data_dir),
            "--sync-dir",
            str(tmp_path / "label-data"),
            "--report-path",
            str(report_path),
        ]
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert result == 2
    assert identity_path.read_bytes() == identity_before
    assert report["status"] == module.ADMIN_RECOVERY_ACTION
    assert report["recovery_origin"] == "local_possession_key"
    assert report["automatic_key_replacement_performed"] is False
    assert report["existing_identity_preserved"] is True
    assert report["possession_key_state"]["scope"] == "current_user"
    assert not (data_dir / module.DEFAULT_MANIFEST_FILENAME).exists()
    assert not (data_dir / module.DEFAULT_CREDENTIAL_FILENAME).exists()


def test_server_legacy_rejection_is_reported_without_local_identity_commit(
    tmp_path, monkeypatch
):
    module = load_registration_module()
    data_dir = tmp_path / "state"
    report_path = data_dir / "status" / "registration.json"
    monkeypatch.setattr(module, "_current_user_sid", lambda: TEST_USER_SID)
    monkeypatch.setattr(
        module,
        "_prepare_possession_key",
        lambda _report: fake_possession_descriptor(),
    )

    class Response:
        status_code = 409

        @staticmethod
        def json():
            return {
                "error": {
                    "code": "admin_recovery_required",
                    "message": "Existing producer identity has no possession-key binding",
                }
            }

    monkeypatch.setattr(module.requests, "post", lambda *_args, **_kwargs: Response())

    result = module.main(
        [
            "--apply",
            "--server-base-url",
            "https://worker.example.invalid",
            "--enrollment-token-env",
            "",
            "--machine-guid",
            TEST_MACHINE_GUID,
            "--data-dir",
            str(data_dir),
            "--sync-dir",
            str(tmp_path / "label-data"),
            "--report-path",
            str(report_path),
        ]
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert result == 2
    assert report["status"] == module.ADMIN_RECOVERY_ACTION
    assert report["server_http_status"] == 409
    assert report["server_error_code"] == "admin_recovery_required"
    assert report["recovery_origin"] == "server_legacy_identity"
    assert report["automatic_legacy_upgrade_performed"] is False
    assert report["existing_identity_preserved"] is True
    assert not (data_dir / module.PRODUCER_IDENTITY_FILENAME).exists()
    assert not (data_dir / module.DEFAULT_MANIFEST_FILENAME).exists()
    assert not (data_dir / module.DEFAULT_CREDENTIAL_FILENAME).exists()


def test_label_match_registration_does_not_auto_load_adjacent_token_file(tmp_path, monkeypatch):
    module = load_registration_module()
    fake_tool_dir = tmp_path / "tools"
    fake_tool_dir.mkdir()
    fake_module_path = fake_tool_dir / "register_label_match_worker_pc.py"
    fake_module_path.write_text("# test fixture\n", encoding="utf-8")
    (fake_tool_dir / "enrollment_token.txt").write_text("should-not-be-used", encoding="utf-8")
    monkeypatch.setattr(module, "__file__", str(fake_module_path))

    args = type(
        "Args",
        (),
        {
            "enrollment_token": "",
            "enrollment_token_file": "",
            "enrollment_token_env": "",
        },
    )()

    assert module._token_from_sources(args) == ("ip_allowlist", "")


def _admin_recovery_contract(module, authorization_state):
    manifest = {
        "pc_identity": {
            "pc_id": "LABEL-PC-01",
            "producer_install_id": "install-label-01",
            "source_host_id": "label-host-01",
        },
        "streams": [],
    }
    credential = {
        "producer_id": "producer-label-01",
        "endpoint_url": (
            "https://worker.example.invalid/api/producer-ingest/v1/source-file"
        ),
    }
    possession_key = fake_possession_descriptor()
    digest = module.manifest_hash(manifest)
    response = {
        "contract_version": module.ADMIN_RECOVERY_COMPLETE_CONTRACT_VERSION,
        "status": "recovered",
        "identity_action": "REATTACHED",
        "recovery_action": "ADMIN_RECOVERY",
        "authorization_state": authorization_state,
        "credential_epoch": 2,
        "producer_id": credential["producer_id"],
        "producer_install_id": manifest["pc_identity"]["producer_install_id"],
        "source_host_id": manifest["pc_identity"]["source_host_id"],
        "endpoint_url": credential["endpoint_url"],
        "active_manifest_hashes": [digest],
        "key_id": "key-rotated",
        "secret_fingerprint_sha256": "f" * 64,
        "server_binding": {"producer_manifest_path": "/srv/producer.json"},
        "possession_key": {
            "contract_version": module.POSSESSION_KEY_CONTRACT_VERSION,
            "fingerprint": possession_key["fingerprint"],
        },
    }
    response["client_receipt"] = {
        "contract_version": response["contract_version"],
        "status": response["status"],
        "identity_action": response["identity_action"],
        "recovery_action": response["recovery_action"],
        "authorization_state": authorization_state,
        "credential_epoch": response["credential_epoch"],
        "producer_id": response["producer_id"],
        "producer_install_id": response["producer_install_id"],
        "source_host_id": response["source_host_id"],
        "endpoint_url": response["endpoint_url"],
        "active_manifest_hashes": [digest],
        "possession_key_fingerprint": possession_key["fingerprint"],
        "key_id": response["key_id"],
        "secret_fingerprint_sha256": response["secret_fingerprint_sha256"],
    }
    return manifest, credential, possession_key, response


def _initial_enrollment_contract(module, authorization_state):
    payload = {
        "manifest": {
            "pc_identity": {
                "producer_install_id": "install-label-01",
                "source_host_id": "label-host-01",
            }
        },
        "producer_id": "producer-label-01",
        "key_id": "key-label-01",
        "endpoint_url": (
            "https://worker.example.invalid/api/producer-ingest/v1/source-file"
        ),
    }
    response = fake_v2_enrollment_response(
        module,
        payload,
        "initial-producer-secret",
        authorization_state=authorization_state,
    )
    return payload, response


@pytest.mark.parametrize("authorization_state", ["LOGISTICS_READY", "OPERATION_PENDING"])
def test_initial_response_accepts_both_deployed_authorization_states(
    authorization_state,
):
    module = load_registration_module()
    payload, response = _initial_enrollment_contract(module, authorization_state)

    module._validate_v2_enrollment_response(
        response,
        expected_fingerprint=TEST_POSSESSION_FINGERPRINT,
        expected_producer_id=payload["producer_id"],
        expected_install_id=payload["manifest"]["pc_identity"][
            "producer_install_id"
        ],
        expected_source_host_id=payload["manifest"]["pc_identity"][
            "source_host_id"
        ],
        expected_endpoint_url=payload["endpoint_url"],
        expected_manifest_hash=module.manifest_hash(payload["manifest"]),
    )


def test_initial_response_rejects_unknown_authorization_state():
    module = load_registration_module()
    payload, response = _initial_enrollment_contract(module, "UNRECOGNIZED")

    with pytest.raises(module.DirectSyncPushError, match="authorization state mismatch"):
        module._validate_v2_enrollment_response(
            response,
            expected_fingerprint=TEST_POSSESSION_FINGERPRINT,
            expected_producer_id=payload["producer_id"],
            expected_install_id=payload["manifest"]["pc_identity"][
                "producer_install_id"
            ],
            expected_source_host_id=payload["manifest"]["pc_identity"][
                "source_host_id"
            ],
            expected_endpoint_url=payload["endpoint_url"],
            expected_manifest_hash=module.manifest_hash(payload["manifest"]),
        )


@pytest.mark.parametrize("authorization_state", ["LOGISTICS_READY", "OPERATION_PENDING"])
def test_admin_recovery_response_accepts_both_deployed_authorization_states(
    authorization_state,
):
    module = load_registration_module()
    manifest, credential, possession_key, response = _admin_recovery_contract(
        module, authorization_state
    )

    module._validate_admin_recovery_response(
        response,
        manifest=manifest,
        credential=credential,
        possession_key=possession_key,
    )


def test_admin_recovery_response_rejects_unknown_authorization_state():
    module = load_registration_module()
    manifest, credential, possession_key, response = _admin_recovery_contract(
        module, "UNRECOGNIZED"
    )

    with pytest.raises(
        module.DirectSyncPushError, match="authorization binding differs"
    ):
        module._validate_admin_recovery_response(
            response,
            manifest=manifest,
            credential=credential,
            possession_key=possession_key,
        )


def test_admin_recovery_manifest_mismatch_stops_before_key_or_http(monkeypatch):
    module = load_registration_module()
    manifest, credential, _possession_key, _response = _admin_recovery_contract(
        module, "LOGISTICS_READY"
    )
    calls = []
    monkeypatch.setattr(
        module.PersistentPossessionKey,
        "provision_initial",
        staticmethod(lambda **_kwargs: calls.append("key")),
    )
    monkeypatch.setattr(
        module,
        "_open_admin_recovery_session",
        lambda _path: calls.append("http"),
    )
    args = SimpleNamespace(
        credential_scope="current_user",
        pc_id="LABEL-PC-01",
        producer_id=credential["producer_id"],
        source_host_id=manifest["pc_identity"]["source_host_id"],
        producer_install_id=manifest["pc_identity"]["producer_install_id"],
        expected_active_manifest_hash="0" * 64,
        tls_ca_bundle_path="unused.pem",
    )

    with module.EnrollmentMutex():
        with pytest.raises(module.DirectSyncPushError, match="legacy_manifest_hash_mismatch"):
            module._admin_recover(args, manifest, credential)

    assert calls == []


@pytest.mark.parametrize("authorization_state", ["LOGISTICS_READY", "OPERATION_PENDING"])
def test_admin_recovery_executor_signs_exact_manifest_without_network(
    tmp_path, monkeypatch, authorization_state
):
    module = load_registration_module()
    manifest, credential, possession_key, response_payload = _admin_recovery_contract(
        module, authorization_state
    )
    response_payload["secret"] = "rotated-producer-secret"
    response_payload["machine_credential_bundle"] = {"fixture": True}
    ca_path = tmp_path / "private-ca.pem"
    ca_path.write_text("fixture", encoding="ascii")
    authorization_path = tmp_path / "authorization.json"
    authorization = {
        "contract_version": module.ADMIN_RECOVERY_AUTHORIZATION_CONTRACT_VERSION,
        "authorization_id": "authz-label-01",
        "producer_id": credential["producer_id"],
        "recovery_token": "one-time-recovery-secret",
        "nonce": "nonce-label-01",
        "expires_at": "2099-01-01T00:00:00Z",
        "audience": module.ADMIN_RECOVERY_AUDIENCE,
        "audit_event_id": "audit-label-01",
    }
    authorization_path.write_text(json.dumps(authorization), encoding="utf-8")
    calls = {}

    class Descriptor:
        def as_dict(self):
            return dict(possession_key)

    class NonExportability:
        private_export_status_hex = "0x80090010"

    class Key:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def descriptor(self):
            return Descriptor()

        def assert_non_exportable(self):
            return NonExportability()

        def sign_es256(self, value):
            calls["signed"] = value
            return b"s" * 64

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return response_payload

    class Session:
        trust_env = False

        def post(self, url, **kwargs):
            calls["post"] = {"url": url, **kwargs}
            return Response()

        def close(self):
            calls["closed"] = True

    monkeypatch.setattr(
        module.PersistentPossessionKey,
        "provision_initial",
        staticmethod(lambda **kwargs: calls.update(key_kwargs=kwargs) or Key()),
    )
    monkeypatch.setattr(
        module,
        "_open_admin_recovery_session",
        lambda path: calls.update(ca_path=path) or Session(),
    )
    monkeypatch.setattr(
        module,
        "_preflight_admin_recovery_local_state",
        lambda *_args: calls.update(local_preflight=True),
    )
    args = SimpleNamespace(
        credential_scope="current_user",
        pc_id="LABEL-PC-01",
        producer_id=credential["producer_id"],
        source_host_id=manifest["pc_identity"]["source_host_id"],
        producer_install_id=manifest["pc_identity"]["producer_install_id"],
        expected_active_manifest_hash=module.manifest_hash(manifest),
        tls_ca_bundle_path=str(ca_path),
        admin_recovery_secret_file=str(authorization_path),
        admin_recovery_url="",
        enrollment_token="",
        enrollment_token_file="",
        enrollment_token_env="",
        enrollment_timeout_seconds=30,
    )
    progress = module._AdminRecoveryProgress()

    with module.EnrollmentMutex():
        response, descriptor, token_source, returned_path, returned_authorization = (
            module._admin_recover(args, manifest, credential, progress)
        )

    assert response is response_payload
    assert descriptor["fingerprint"] == possession_key["fingerprint"]
    assert token_source == "ip_allowlist"
    assert returned_path == authorization_path.resolve()
    assert returned_authorization == authorization
    assert calls["key_kwargs"] == {"scope": module.SCOPE_CURRENT_USER}
    assert calls["local_preflight"] is True
    assert calls["ca_path"] == str(ca_path)
    assert calls["post"]["url"].endswith(module.ADMIN_RECOVERY_PATH)
    assert calls["post"]["allow_redirects"] is False
    assert calls["post"]["json"]["proof"]["manifest_hash"] == module.manifest_hash(
        manifest
    )
    assert calls["post"]["json"]["new_possession_public_jwk"] == TEST_POSSESSION_JWK
    assert calls["closed"] is True
    assert progress.server_credential_rotated is True


def test_admin_recovery_transport_disables_environment_and_pins_explicit_ca(
    tmp_path, monkeypatch
):
    module = load_registration_module()
    ca_path = tmp_path / "private-ca.pem"
    ca_path.write_text("fixture", encoding="ascii")

    class Session:
        trust_env = True
        verify = True

    session = Session()
    monkeypatch.setattr(module.requests, "Session", lambda: session)

    opened = module._open_admin_recovery_session(str(ca_path))

    assert opened is session
    assert session.trust_env is False
    assert session.verify == str(ca_path.resolve())


def test_producer_dpapi_replace_interruption_preserves_original_and_temp_zero(
    tmp_path, monkeypatch
):
    module = load_registration_module()
    target = module._secret_path(tmp_path, "producer-label")
    target.parent.mkdir(parents=True)
    target.write_bytes(b"original-protected-secret")
    before = hashlib.sha256(target.read_bytes()).hexdigest()
    monkeypatch.setattr(
        module, "_dpapi_protect_current_user", lambda _secret: b"rotated-protected-secret"
    )
    real_replace = module.os.replace

    def interrupted_replace(source, destination):
        if Path(destination) == target:
            raise OSError("simulated producer credential replace interruption")
        return real_replace(source, destination)

    monkeypatch.setattr(module.os, "replace", interrupted_replace)

    with pytest.raises(OSError, match="simulated producer credential"):
        module._write_dpapi_secret(
            tmp_path,
            "producer-label",
            "rotated-secret",
            credential_scope="current_user",
        )

    assert hashlib.sha256(target.read_bytes()).hexdigest() == before
    assert list(target.parent.iterdir()) == [target]


def test_admin_recovery_rejects_url_parameters():
    module = load_registration_module()
    endpoint = "https://worker.example.invalid/api/producer-ingest/v1/source-file"

    with pytest.raises(module.DirectSyncPushError, match="same-origin"):
        module._validate_admin_recovery_url(
            "https://worker.example.invalid/api/producer-ingest/v2/recover;ignored",
            endpoint,
        )


def test_admin_recovery_local_preflight_pins_manifest_and_rejects_path_alias(
    tmp_path,
):
    module = load_registration_module()
    data_dir = tmp_path / "state"
    identity_path = data_dir / "producer_identity.json"
    manifest_path = data_dir / "producer_manifest.json"
    credential_path = data_dir / "credential.json"
    receipt_path = data_dir / "evidence" / "receipt.json"
    report_path = data_dir / "status" / "registration.json"
    profile_path = tmp_path / "logistics" / "runtime-profile.json"
    logistics_secret_path = profile_path.parent / "secrets" / "bearer-token.dpapi"
    tls_path = profile_path.parent / module.TLS_CA_BUNDLE_RELATIVE_PATH
    producer_secret_path = data_dir / "secrets" / "producer-label-01.dpapi"
    authorization_path = tmp_path / "authorization.json"
    manifest = {
        "pc_identity": {
            "pc_id": "LABEL-PC-01",
            "producer_install_id": "install-label-01",
            "source_host_id": "label-host-01",
        },
        "streams": [],
    }
    credential = {
        "producer_id": "producer-label-01",
        "endpoint_url": (
            "https://worker.example.invalid/api/producer-ingest/v1/source-file"
        ),
        "secret_data_dir": str(data_dir),
        "secret_ref": "dpapi:producer-label-01",
    }
    module._write_json(identity_path, manifest["pc_identity"])
    module._write_json(manifest_path, manifest)
    module._write_json(credential_path, credential)
    for path, content in (
        (profile_path, b"{}"),
        (logistics_secret_path, b"protected-logistics"),
        (producer_secret_path, b"protected-producer"),
        (tls_path, b"private-ca"),
        (authorization_path, b"authorization"),
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    args = SimpleNamespace(
        data_dir=str(data_dir),
        identity_path=str(identity_path),
        manifest_path=str(manifest_path),
        credential_path=str(credential_path),
        receipt_path=str(receipt_path),
        report_path=str(report_path),
        logistics_profile_path=str(profile_path),
        tls_ca_bundle_path=str(tls_path),
    )

    module._preflight_admin_recovery_local_state(
        args,
        manifest,
        credential,
        authorization_path,
    )

    with pytest.raises(module.DirectSyncPushError, match="overlaps"):
        module._preflight_admin_recovery_local_state(
            args,
            manifest,
            credential,
            profile_path,
        )
    module._write_json(manifest_path, {**manifest, "unexpected": True})
    with pytest.raises(module.DirectSyncPushError, match="differs"):
        module._preflight_admin_recovery_local_state(
            args,
            manifest,
            credential,
            authorization_path,
        )


def test_admin_recovery_response_requires_machine_credential_bundle(monkeypatch):
    module = load_registration_module()
    manifest, credential, possession_key, response = _admin_recovery_contract(
        module, "LOGISTICS_READY"
    )
    response["secret"] = "rotated-producer-secret"
    response["secret_fingerprint_sha256"] = module._fingerprint(response["secret"])
    progress = module._AdminRecoveryProgress()
    args = SimpleNamespace(
        admin_recovery_secret_file="authorization.json",
        admin_recovery_url="",
        credential_scope="current_user",
        logistics_profile_path="profile.json",
        require_machine_credential_bundle=False,
        tls_ca_bundle_path="ca.pem",
    )
    report = {
        "admin_recovery_requested": True,
        "producer_install_id": manifest["pc_identity"]["producer_install_id"],
        "source_host_id": manifest["pc_identity"]["source_host_id"],
        "manifest_hash": module.manifest_hash(manifest),
        "producer_identity_source": "identity_file",
    }

    def fake_recover(_args, _manifest, _credential, observed_progress):
        observed_progress.server_credential_rotated = True
        return response, possession_key, "ip_allowlist", Path("authorization.json"), {
            "authorization_id": "authz-label-01",
            "audit_event_id": "audit-label-01",
        }

    monkeypatch.setattr(module, "_admin_recover", fake_recover)

    with pytest.raises(
        module.DirectSyncPushError,
        match="missing machine credential bundle",
    ):
        module.apply_registration(
            args,
            manifest,
            credential,
            report,
            progress,
        )

    assert progress.server_credential_rotated is True
    assert progress.logistics_credential_finalized is False


def test_post_recovery_failure_is_fenced_and_reported_without_exception_text(
    tmp_path, monkeypatch
):
    module = load_registration_module()
    report_path = tmp_path / "status" / "registration.json"
    authorization_path = tmp_path / "authorization.json"
    authorization_path.write_text("fixture", encoding="utf-8")
    manifest = {
        "pc_identity": {
            "pc_id": "LABEL-PC-01",
            "producer_install_id": "install-label-01",
            "source_host_id": "label-host-01",
        },
        "paths": {
            "evidence_dir": (tmp_path / "evidence").as_posix(),
            "rollback_dir": (tmp_path / "rollback").as_posix(),
        },
        "sync": {"sync_dir": (tmp_path / "sync").as_posix()},
    }
    credential = {
        "producer_id": "producer-label-01",
        "key_id": "key-label-01",
        "secret_data_dir": str(tmp_path),
        "secret_ref": "dpapi:producer-label-01",
    }
    initial_report = {
        "status": "APPLY_REQUESTED",
        "admin_recovery_requested": True,
        "manifest_hash": module.manifest_hash(manifest),
        "producer_id": credential["producer_id"],
    }
    monkeypatch.setattr(
        module,
        "build_payloads",
        lambda _args: (manifest, credential, dict(initial_report)),
    )

    def fail_after_commit(_args, _manifest, _credential, _report, progress):
        progress.server_credential_rotated = True
        raise RuntimeError("must-not-be-persisted-verbatim")

    monkeypatch.setattr(module, "apply_registration", fail_after_commit)

    result = module.main(
        [
            "--apply",
            "--admin-recovery-secret-file",
            str(authorization_path),
            "--report-path",
            str(report_path),
        ]
    )

    persisted = json.loads(report_path.read_text(encoding="utf-8-sig"))
    assert result == 3
    assert persisted["status"] == "BLOCKED_POST_RECOVERY_LOCAL_PERSISTENCE"
    assert persisted["server_credential_rotated"] is True
    assert persisted["recovery_action"] == "NEW_AUDITED_RECOVERY_REQUIRED"
    assert persisted["blocked_reason"] == "RuntimeError"
    assert "must-not-be-persisted-verbatim" not in report_path.read_text(
        encoding="utf-8-sig"
    )
    assert authorization_path.is_file()


def test_successful_local_recovery_finalization_deletes_authorization_last(
    tmp_path, monkeypatch
):
    module = load_registration_module()
    data_dir = tmp_path / "state"
    report_path = data_dir / "status" / "registration.json"
    authorization_path = tmp_path / "authorization.json"
    authorization_path.write_text("fixture", encoding="utf-8")
    manifest = {
        "pc_identity": {
            "pc_id": "LABEL-PC-01",
            "producer_install_id": "install-label-01",
            "source_host_id": "label-host-01",
        },
        "paths": {
            "evidence_dir": (data_dir / "evidence").as_posix(),
            "rollback_dir": (data_dir / "rollback").as_posix(),
        },
        "sync": {"sync_dir": (tmp_path / "sync").as_posix()},
    }
    credential = {
        "producer_id": "producer-label-01",
        "key_id": "key-label-01",
        "secret_data_dir": str(data_dir),
        "secret_ref": "dpapi:producer-label-01",
    }
    initial_report = {
        "status": "APPLY_REQUESTED",
        "admin_recovery_requested": True,
        "manifest_hash": module.manifest_hash(manifest),
        "producer_id": credential["producer_id"],
    }
    monkeypatch.setattr(
        module,
        "build_payloads",
        lambda _args: (manifest, credential, dict(initial_report)),
    )

    def finalize(_args, _manifest, _credential, report, progress):
        progress.server_credential_rotated = True
        progress.logistics_credential_finalized = True
        progress.producer_credential_finalized = True
        report.update(
            {
                "status": "ADMIN_RECOVERY_REGISTERED",
                "admin_recovery_secret_cleanup_required": True,
                "admin_recovery_secret_file": str(authorization_path),
                "client_receipt": {
                    "contract_version": module.ADMIN_RECOVERY_COMPLETE_CONTRACT_VERSION,
                    "status": "recovered",
                },
            }
        )
        return report

    monkeypatch.setattr(module, "apply_registration", finalize)

    result = module.main(
        [
            "--apply",
            "--admin-recovery-secret-file",
            str(authorization_path),
            "--data-dir",
            str(data_dir),
            "--report-path",
            str(report_path),
        ]
    )

    persisted = json.loads(report_path.read_text(encoding="utf-8-sig"))
    assert result == 0
    assert authorization_path.exists() is False
    assert persisted["admin_recovery_secret_file_deleted"] is True
    assert persisted["admin_recovery_secret_cleanup_required"] is False
    assert persisted["admin_recovery_progress"] == {
        "authorization_file_deleted": True,
        "local_documents_finalized": True,
        "logistics_credential_finalized": True,
        "producer_credential_finalized": True,
        "server_credential_rotated": True,
    }

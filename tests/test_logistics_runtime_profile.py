from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import stat
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

import Label_Match as label_module
import logistics_runtime_profile as runtime_module
import tools.install_logistics_runtime_profile as profile_installer_module
from logistics_runtime_profile import (
    LogisticsRuntimeConfigurationError,
    TEST1_ISOLATED_LEGACY_OVERRIDE_ENV,
    default_logistics_profile_path,
    load_logistics_runtime_profile,
    protect_machine_secret,
    unprotect_machine_secret,
)
from package_logistics import PackageLogisticsError, package_client_from_env
from tools.install_logistics_runtime_profile import (
    install_runtime_profile,
    install_tls_ca_bundle_for_existing_profile,
    main as install_main,
)
from tools.check_logistics_runtime_profile import main as readiness_main


def test_gui_startup_builds_client_without_network_readiness_probe(monkeypatch):
    calls = []
    sentinel = object()

    def fake_factory(*, probe_required=True):
        calls.append(probe_required)
        return sentinel

    monkeypatch.setattr(label_module, "package_client_from_env", fake_factory)

    assert label_module.label_match_startup_package_client() is sentinel
    assert calls == [False]


def test_production_test_commands_require_explicit_automation_switch(monkeypatch):
    monkeypatch.delenv(label_module.LABEL_MATCH_AUTOMATED_TEST_ENV, raising=False)
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.setattr(label_module.sys, "argv", ["Label_Match.py"])

    assert label_module.label_match_test_tools_enabled(run_tests=False) is False

    monkeypatch.setenv(label_module.LABEL_MATCH_AUTOMATED_TEST_ENV, "1")
    assert label_module.label_match_test_tools_enabled(run_tests=False) is True
    assert label_module.label_match_test_tools_enabled(run_tests=True) is True


def _profile(tmp_path, *, profile_path=None, **changes):
    profile_path = Path(profile_path or (tmp_path / "machine" / "profile.json"))
    secret_path = profile_path.parent / "secrets" / "bearer-token.dpapi"
    secret_path.parent.mkdir(parents=True)
    secret_path.write_bytes(b"encrypted-token")
    value = {
        "contract_version": "km-logistics-runtime-profile-v1",
        "base_url": "https://logistics.example.invalid",
        "authority_scope": "scope-machine",
        "authority_epoch": 7,
        "authority_plane": "AUTHORITATIVE",
        "plane_epoch": 3,
        "device_id": "label-pc-01",
        "source_host_id": "label-host-01",
        "bearer_token_ref": "dpapi:secrets/bearer-token.dpapi",
        "timeout_seconds": 4,
    }
    value.update(changes)
    profile_path.write_text(json.dumps(value), encoding="utf-8")
    return profile_path


def _env(monkeypatch, profile_path):
    monkeypatch.setenv("KM_LOGISTICS_REQUIRED", "1")
    monkeypatch.setenv("KM_LOGISTICS_PROFILE_PATH", str(profile_path))


def _private_ca_pem() -> bytes:
    private_key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, "Label Match Test Private CA")]
    )
    now = datetime.now(timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .sign(private_key, hashes.SHA256())
    )
    return certificate.public_bytes(serialization.Encoding.PEM)


def _machine_enrollment_response(
    timeout_seconds,
    *,
    logistics_token="logistics-token-01",
):
    return {
        "producer_id": "producer-label-host-01",
        "producer_install_id": "install-label-host-01",
        "source_host_id": "label-host-01",
        "endpoint_url": "https://worker.example.invalid/api/producer-ingest/v1/source-file",
        "active_manifest_hashes": ["a" * 64],
        "key_id": "producer-key-01",
        "secret": "producer-secret-01",
        "machine_credential_bundle": {
            "contract_version": "producer-self-enrollment-machine-credentials-v1",
            "bindings": {
                "app": "label_match",
                "program": "Label_Match",
                "source_host_id": "label-host-01",
                "device_id": "label-pc-01",
                "authority_scope_id": "scope-machine",
            },
            "credentials": {
                "producer_ingest": {
                    "audience": "producer-ingest-hmac-v1",
                    "auth_scheme": "hmac-sha256",
                    "key_id": "producer-key-01",
                    "secret": "producer-secret-01",
                },
                "logistics": {
                    "audience": "worker-analysis-logistics-v1",
                    "auth_scheme": "bearer",
                    "token_header": "X-Logistics-API-Token",
                    "token": logistics_token,
                },
            },
            "profiles": {
                "logistics": {
                    "contract_version": "km-logistics-runtime-profile-v1",
                    "base_url": "https://logistics.example.invalid",
                    "authority_scope": "scope-machine",
                    "authority_epoch": 7,
                    "authority_plane": "AUTHORITATIVE",
                    "ledger_plane": "AUTHORITATIVE",
                    "plane_epoch": 3,
                    "device_id": "label-pc-01",
                    "source_host_id": "label-host-01",
                    "timeout_seconds": timeout_seconds,
                }
            },
        },
    }


def test_profile_rotation_semantic_comparison_normalizes_only_json_numbers():
    equal = profile_installer_module._semantic_json_equal

    assert equal({"timeout_seconds": 10.0}, {"timeout_seconds": 10}) is True
    assert equal({"timeout_seconds": 10}, {"timeout_seconds": 10.5}) is False
    assert equal({"timeout_seconds": 10}, {"timeout_seconds": "10"}) is False
    assert equal({"timeout_seconds": 1}, {"timeout_seconds": True}) is False
    assert equal({"timeout_seconds": 0}, {"timeout_seconds": None}) is False


def test_enrollment_profile_reuses_float_timeout_for_equivalent_integer(
    tmp_path, monkeypatch
):
    target = tmp_path / "current-user" / "runtime-profile.json"
    monkeypatch.setattr(
        profile_installer_module,
        "protect_current_user_secret",
        lambda value: b"user-dpapi:" + value.encode("utf-8"),
    )
    monkeypatch.setattr(
        profile_installer_module,
        "unprotect_current_user_secret",
        lambda value: value.removeprefix(b"user-dpapi:").decode("utf-8"),
    )
    arguments = {
        "expected_app": "label_match",
        "expected_program": "Label_Match",
        "expected_source_host_id": "label-host-01",
        "expected_device_id": "label-pc-01",
        "profile_path": target,
        "credential_scope": "current_user",
    }

    installed = profile_installer_module.ensure_runtime_profile_from_enrollment_bundle(
        _machine_enrollment_response(10.0), **arguments
    )
    secret_path = target.parent / "secrets" / "bearer-token.dpapi"
    secret_before = secret_path.read_bytes()
    reused = profile_installer_module.ensure_runtime_profile_from_enrollment_bundle(
        _machine_enrollment_response(10), **arguments
    )

    assert installed["status"] == "installed"
    assert reused["status"] == "reused"
    assert json.loads(target.read_text(encoding="utf-8"))["timeout_seconds"] == 10.0
    assert secret_path.read_bytes() == secret_before

    for conflicting in (10.5, "10", True):
        with pytest.raises(FileExistsError, match="conflicts with enrollment"):
            profile_installer_module.ensure_runtime_profile_from_enrollment_bundle(
                _machine_enrollment_response(conflicting), **arguments
            )


def _recovery_profile_arguments(target):
    return {
        "expected_app": "label_match",
        "expected_program": "Label_Match",
        "expected_source_host_id": "label-host-01",
        "expected_device_id": "label-pc-01",
        "profile_path": target,
        "credential_scope": "current_user",
        "allow_existing_token_rotation": True,
        "expected_producer_id": "producer-label-host-01",
        "expected_producer_install_id": "install-label-host-01",
        "expected_manifest_hash": "a" * 64,
        "expected_endpoint_url": (
            "https://worker.example.invalid/api/producer-ingest/v1/source-file"
        ),
    }


def _fake_current_user_dpapi(monkeypatch):
    monkeypatch.setattr(
        profile_installer_module,
        "protect_current_user_secret",
        lambda value: b"user-dpapi:" + value.encode("utf-8"),
    )
    monkeypatch.setattr(
        profile_installer_module,
        "unprotect_current_user_secret",
        lambda value: value.removeprefix(b"user-dpapi:").decode("utf-8"),
    )


def test_recovery_rotates_only_token_when_every_non_secret_binding_matches(
    tmp_path, monkeypatch
):
    target = tmp_path / "current-user" / "runtime-profile.json"
    _fake_current_user_dpapi(monkeypatch)
    initial_arguments = {
        key: value
        for key, value in _recovery_profile_arguments(target).items()
        if key
        not in {
            "allow_existing_token_rotation",
            "expected_producer_id",
            "expected_producer_install_id",
            "expected_manifest_hash",
            "expected_endpoint_url",
        }
    }
    profile_installer_module.ensure_runtime_profile_from_enrollment_bundle(
        _machine_enrollment_response(10.0), **initial_arguments
    )
    profile_before = target.read_bytes()
    secret_path = target.parent / "secrets" / "bearer-token.dpapi"
    secret_before = secret_path.read_bytes()

    rotated = profile_installer_module.ensure_runtime_profile_from_enrollment_bundle(
        _machine_enrollment_response(10, logistics_token="logistics-token-rotated"),
        **_recovery_profile_arguments(target),
    )

    assert rotated["status"] == "rotated"
    assert rotated["non_secret_profile_preserved"] is True
    assert target.read_bytes() == profile_before
    assert secret_path.read_bytes() != secret_before
    assert (
        profile_installer_module.unprotect_current_user_secret(secret_path.read_bytes())
        == "logistics-token-rotated"
    )


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("producer_id",), "producer-other"),
        (("producer_install_id",), "install-other"),
        (("source_host_id",), "host-other"),
        (("active_manifest_hashes",), ["b" * 64]),
        (("endpoint_url",), "https://other.example.invalid/api/producer-ingest/v1/source-file"),
        (("machine_credential_bundle", "bindings", "authority_scope_id"), "scope-other"),
    ],
)
def test_recovery_rotation_rejects_every_non_secret_binding_mismatch(
    tmp_path, monkeypatch, path, value
):
    target = tmp_path / "current-user" / "runtime-profile.json"
    _fake_current_user_dpapi(monkeypatch)
    initial_arguments = {
        key: item
        for key, item in _recovery_profile_arguments(target).items()
        if key
        not in {
            "allow_existing_token_rotation",
            "expected_producer_id",
            "expected_producer_install_id",
            "expected_manifest_hash",
            "expected_endpoint_url",
        }
    }
    profile_installer_module.ensure_runtime_profile_from_enrollment_bundle(
        _machine_enrollment_response(10.0), **initial_arguments
    )
    secret_path = target.parent / "secrets" / "bearer-token.dpapi"
    profile_before = target.read_bytes()
    secret_before = secret_path.read_bytes()
    response = _machine_enrollment_response(
        10, logistics_token="logistics-token-rotated"
    )
    selected = response
    for field in path[:-1]:
        selected = selected[field]
    selected[path[-1]] = value
    if path == ("machine_credential_bundle", "bindings", "authority_scope_id"):
        response["machine_credential_bundle"]["profiles"]["logistics"][
            "authority_scope"
        ] = value

    with pytest.raises((FileExistsError, ValueError)):
        profile_installer_module.ensure_runtime_profile_from_enrollment_bundle(
            response, **_recovery_profile_arguments(target)
        )

    assert target.read_bytes() == profile_before
    assert secret_path.read_bytes() == secret_before


def test_initial_enrollment_still_rejects_existing_rotated_token(
    tmp_path, monkeypatch
):
    target = tmp_path / "current-user" / "runtime-profile.json"
    _fake_current_user_dpapi(monkeypatch)
    arguments = {
        key: value
        for key, value in _recovery_profile_arguments(target).items()
        if key
        not in {
            "allow_existing_token_rotation",
            "expected_producer_id",
            "expected_producer_install_id",
            "expected_manifest_hash",
            "expected_endpoint_url",
        }
    }
    profile_installer_module.ensure_runtime_profile_from_enrollment_bundle(
        _machine_enrollment_response(10.0), **arguments
    )
    secret_path = target.parent / "secrets" / "bearer-token.dpapi"
    profile_sha = hashlib.sha256(target.read_bytes()).hexdigest()
    secret_sha = hashlib.sha256(secret_path.read_bytes()).hexdigest()

    with pytest.raises(FileExistsError, match="conflicts with enrollment"):
        profile_installer_module.ensure_runtime_profile_from_enrollment_bundle(
            _machine_enrollment_response(
                10, logistics_token="logistics-token-rotated"
            ),
            **arguments,
        )

    assert hashlib.sha256(target.read_bytes()).hexdigest() == profile_sha
    assert hashlib.sha256(secret_path.read_bytes()).hexdigest() == secret_sha


def test_recovery_token_replace_interruption_preserves_original_and_cleans_temp(
    tmp_path, monkeypatch
):
    target = tmp_path / "current-user" / "runtime-profile.json"
    _fake_current_user_dpapi(monkeypatch)
    initial_arguments = {
        key: value
        for key, value in _recovery_profile_arguments(target).items()
        if key
        not in {
            "allow_existing_token_rotation",
            "expected_producer_id",
            "expected_producer_install_id",
            "expected_manifest_hash",
            "expected_endpoint_url",
        }
    }
    profile_installer_module.ensure_runtime_profile_from_enrollment_bundle(
        _machine_enrollment_response(10.0), **initial_arguments
    )
    secret_path = target.parent / "secrets" / "bearer-token.dpapi"
    profile_sha = hashlib.sha256(target.read_bytes()).hexdigest()
    secret_sha = hashlib.sha256(secret_path.read_bytes()).hexdigest()
    real_replace = profile_installer_module.os.replace

    def interrupted_replace(source, destination):
        if Path(destination) == secret_path:
            raise OSError("simulated os.replace interruption")
        return real_replace(source, destination)

    monkeypatch.setattr(profile_installer_module.os, "replace", interrupted_replace)
    with pytest.raises(OSError, match="simulated os.replace interruption"):
        profile_installer_module.ensure_runtime_profile_from_enrollment_bundle(
            _machine_enrollment_response(
                10, logistics_token="logistics-token-rotated"
            ),
            **_recovery_profile_arguments(target),
        )

    assert hashlib.sha256(target.read_bytes()).hexdigest() == profile_sha
    assert hashlib.sha256(secret_path.read_bytes()).hexdigest() == secret_sha
    assert list(secret_path.parent.glob("*.profile.tmp")) == []


def test_recovery_post_replace_readback_failure_rolls_back_original(
    tmp_path, monkeypatch
):
    target = tmp_path / "current-user" / "runtime-profile.json"
    _fake_current_user_dpapi(monkeypatch)
    initial_arguments = {
        key: value
        for key, value in _recovery_profile_arguments(target).items()
        if key
        not in {
            "allow_existing_token_rotation",
            "expected_producer_id",
            "expected_producer_install_id",
            "expected_manifest_hash",
            "expected_endpoint_url",
        }
    }
    profile_installer_module.ensure_runtime_profile_from_enrollment_bundle(
        _machine_enrollment_response(10.0), **initial_arguments
    )
    secret_path = target.parent / "secrets" / "bearer-token.dpapi"
    profile_sha = hashlib.sha256(target.read_bytes()).hexdigest()
    secret_sha = hashlib.sha256(secret_path.read_bytes()).hexdigest()
    real_loader = profile_installer_module.load_logistics_runtime_profile
    calls = 0

    def interrupted_readback(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("simulated post-replace readback interruption")
        return real_loader(*args, **kwargs)

    monkeypatch.setattr(
        profile_installer_module,
        "load_logistics_runtime_profile",
        interrupted_readback,
    )
    with pytest.raises(RuntimeError, match="post-replace readback"):
        profile_installer_module.ensure_runtime_profile_from_enrollment_bundle(
            _machine_enrollment_response(
                10, logistics_token="logistics-token-rotated"
            ),
            **_recovery_profile_arguments(target),
        )

    assert hashlib.sha256(target.read_bytes()).hexdigest() == profile_sha
    assert hashlib.sha256(secret_path.read_bytes()).hexdigest() == secret_sha
    assert list(secret_path.parent.glob("*.profile.tmp")) == []


def test_default_profile_path_is_program_scoped(tmp_path):
    assert default_logistics_profile_path({"PROGRAMDATA": str(tmp_path)}) == (
        tmp_path
        / "KMTech"
        / "Logistics"
        / "profiles"
        / "Label_Match"
        / "runtime-profile.json"
    )


def test_program_scoped_default_precedes_legacy_machine_profile(
    monkeypatch, tmp_path
):
    app_profile = default_logistics_profile_path({"PROGRAMDATA": str(tmp_path)})
    legacy_profile = tmp_path / "KMTech" / "Logistics" / "runtime-profile.json"
    monkeypatch.setenv("PROGRAMDATA", str(tmp_path))
    monkeypatch.delenv("KM_LOGISTICS_PROFILE_PATH", raising=False)
    monkeypatch.delenv("KM_LOGISTICS_REQUIRED", raising=False)

    _profile(tmp_path, profile_path=legacy_profile)
    loaded = load_logistics_runtime_profile(
        required=True,
        decryptor=lambda _value: "legacy-machine-secret",
    )
    assert loaded is not None
    assert Path(loaded.profile_path) == legacy_profile.resolve()

    _profile(tmp_path, profile_path=app_profile)
    loaded = load_logistics_runtime_profile(
        required=True,
        environ={
            "PROGRAMDATA": str(tmp_path),
            "KM_LOGISTICS_PROFILE_PATH": str(legacy_profile),
            "KM_LOGISTICS_REQUIRED": "1",
        },
        decryptor=lambda _value: "app-machine-secret",
    )
    assert loaded is not None
    assert Path(loaded.profile_path) == app_profile.resolve()

    explicit = load_logistics_runtime_profile(
        required=True,
        profile_path=legacy_profile,
        environ={
            "PROGRAMDATA": str(tmp_path),
            "KM_LOGISTICS_PROFILE_PATH": str(legacy_profile),
            "KM_LOGISTICS_REQUIRED": "1",
        },
        decryptor=lambda _value: "legacy-machine-secret",
    )
    assert explicit is not None
    assert Path(explicit.profile_path) == legacy_profile.resolve()

    custom_profile = _profile(
        tmp_path,
        profile_path=tmp_path / "custom" / "runtime-profile.json",
    )
    custom = load_logistics_runtime_profile(
        required=True,
        environ={
            "PROGRAMDATA": str(tmp_path),
            "KM_LOGISTICS_PROFILE_PATH": str(custom_profile),
            "KM_LOGISTICS_REQUIRED": "1",
        },
        decryptor=lambda _value: "custom-machine-secret",
    )
    assert custom is not None
    assert Path(custom.profile_path) == custom_profile.resolve()


def _capabilities():
    return {
        "capability_ids": ["sealed_transfer_member_replacement_v1"],
        "capabilities": {
            "sealed_transfer_member_replacement_v1": {
                "enabled": True,
                "command_type": "REPLACE_SEALED_TRANSFER_MEMBERS",
                "endpoint_template": "/logistics/api/v1/transfers/{target_bundle_id}/members/replace-and-reseal",
                "receipt_contract_version": "sealed-transfer-member-replacement-v1",
                "replacement_source_bundle_cardinality": "EXACTLY_ONE_ACTIVE_MEMBER",
                "multi_member_source_policy": "REJECT_STALE_PHYSICAL_LABEL",
                "multi_member_source_error_code": "REPLACEMENT_SOURCE_NOT_SINGLETON",
                "seal_qr_contract_version": "transfer-seal-qr-v1",
                "max_pairs": 2,
                "atomic": True,
                "fail_closed_when_unavailable": True,
                "disabled_server_behavior": "REJECT_COMMAND_DO_NOT_MUTATE_LOCAL_STATE",
                "client_rollout_gate": "REQUIRE_ENABLED_CAPABILITY_AND_EXACT_RECEIPT",
            }
        },
    }


def _transport(_method, _url, headers, _body, _timeout):
    assert headers["Authorization"] == "Bearer machine-secret"
    return {"ok": True, "data": _capabilities()}


def test_machine_profile_and_required_probe_are_secure(tmp_path, monkeypatch):
    path = _profile(tmp_path)
    _env(monkeypatch, path)

    profile = load_logistics_runtime_profile(
        decryptor=lambda _value: "machine-secret"
    )
    client = package_client_from_env(
        transport=_transport,
        profile_decryptor=lambda _value: "machine-secret",
    )

    assert profile is not None and client is not None
    assert profile.authority_plane == "AUTHORITATIVE"
    assert profile.ledger_plane == "AUTHORITATIVE"
    assert client.config.authoritative_required is True
    assert "machine-secret" not in repr(profile)
    assert "machine-secret" not in repr(client.config)


def test_machine_profile_defaults_to_no_explicit_tls_ca_bundle(tmp_path, monkeypatch):
    path = _profile(tmp_path)
    _env(monkeypatch, path)

    resolved = load_logistics_runtime_profile(
        decryptor=lambda _value: "machine-secret"
    )

    assert resolved is not None
    assert resolved.tls_ca_bundle_path == ""
    assert resolved.redacted_summary()["tls_private_ca_configured"] is False


def test_machine_profile_resolves_durable_tls_ca_bundle(tmp_path, monkeypatch):
    profile_root = tmp_path / "machine"
    ca_bundle = profile_root / "tls" / "ca-bundle.pem"
    ca_bundle.parent.mkdir(parents=True)
    ca_bundle.write_bytes(b"private-ca-fixture")
    path = _profile(tmp_path, tls_ca_bundle_path=str(ca_bundle.resolve()))
    _env(monkeypatch, path)

    resolved = load_logistics_runtime_profile(
        decryptor=lambda _value: "machine-secret"
    )

    assert resolved is not None
    assert resolved.tls_ca_bundle_path == str(ca_bundle.resolve())
    assert resolved.redacted_summary()["tls_private_ca_configured"] is True


@pytest.mark.parametrize("location", ["outside", "missing"])
def test_machine_profile_tls_ca_bundle_fails_closed_outside_owned_profile(
    tmp_path, monkeypatch, location
):
    if location == "outside":
        ca_bundle = tmp_path / "outside-ca.pem"
        ca_bundle.write_bytes(b"outside-private-ca-fixture")
    else:
        ca_bundle = tmp_path / "machine" / "tls" / "missing-ca.pem"
    path = _profile(tmp_path, tls_ca_bundle_path=str(ca_bundle.resolve()))
    _env(monkeypatch, path)

    with pytest.raises(
        LogisticsRuntimeConfigurationError,
        match=("inside the profile directory" if location == "outside" else "unavailable"),
    ):
        load_logistics_runtime_profile(decryptor=lambda _value: "machine-secret")


def test_required_profile_separates_authority_mode_from_selected_ledger_plane(
    tmp_path, monkeypatch
):
    path = _profile(tmp_path, ledger_plane="SHADOW_CANDIDATE")
    _env(monkeypatch, path)

    client = package_client_from_env(
        transport=_transport,
        profile_decryptor=lambda _value: "machine-secret",
    )

    assert client is not None
    assert client.config.authority_plane == "AUTHORITATIVE"
    assert client.config.ledger_plane == "SHADOW_CANDIDATE"
    client._assert_authority(
        "scope-machine",
        authority_epoch=7,
        ledger_plane="SHADOW_CANDIDATE",
        plane_epoch=3,
    )
    with pytest.raises(PackageLogisticsError, match="ledger plane"):
        client._assert_authority(
            "scope-machine",
            authority_epoch=7,
            ledger_plane="AUTHORITATIVE",
            plane_epoch=3,
        )


@pytest.mark.skipif(os.name != "nt", reason="Windows DPAPI round-trip")
def test_machine_scope_dpapi_round_trip_never_contains_plaintext():
    token = "DPAPI-ROUNDTRIP-SECRET"

    protected = protect_machine_secret(token)

    assert protected
    assert token.encode("utf-8") not in protected
    assert unprotect_machine_secret(protected) == token


@pytest.mark.parametrize("replacement", [None, "UNKNOWN_ERROR_CODE"])
def test_required_startup_rejects_missing_or_unknown_singleton_contract(
    tmp_path, monkeypatch, replacement
):
    path = _profile(tmp_path)
    _env(monkeypatch, path)
    capabilities = _capabilities()
    capability = capabilities["capabilities"][
        "sealed_transfer_member_replacement_v1"
    ]
    if replacement is None:
        capability.pop("multi_member_source_error_code")
    else:
        capability["multi_member_source_error_code"] = replacement

    def transport(_method, _url, _headers, _body, _timeout):
        return {"ok": True, "data": capabilities}

    with pytest.raises(
        LogisticsRuntimeConfigurationError,
        match="capability readiness is incomplete",
    ):
        package_client_from_env(
            transport=transport,
            profile_decryptor=lambda _value: "machine-secret",
        )


@pytest.mark.parametrize(
    "mode,attributes",
    [(stat.S_IFLNK, 0), (stat.S_IFREG, 0x400)],
)
def test_dpapi_secret_path_rejects_reparse_before_resolving(
    tmp_path, monkeypatch, mode, attributes
):
    path = _profile(tmp_path)
    secret_path = path.parent / "secrets" / "bearer-token.dpapi"
    original_lstat = runtime_module.os.lstat

    def fake_lstat(candidate):
        if runtime_module.Path(candidate) == secret_path:
            return SimpleNamespace(
                st_mode=mode,
                st_file_attributes=attributes,
            )
        return original_lstat(candidate)

    monkeypatch.setattr(runtime_module.os, "lstat", fake_lstat)

    with pytest.raises(LogisticsRuntimeConfigurationError, match="symlink|junction"):
        runtime_module._resolve_secret_path(
            path,
            "dpapi:secrets/bearer-token.dpapi",
        )


def test_required_mode_missing_profile_never_uses_legacy_env(tmp_path, monkeypatch):
    _env(monkeypatch, tmp_path / "missing.json")
    monkeypatch.setenv("LABEL_MATCH_LOGISTICS_API_BASE_URL", "https://legacy.invalid")
    monkeypatch.setenv("LABEL_MATCH_LOGISTICS_API_TOKEN", "legacy-secret")
    monkeypatch.setenv("LABEL_MATCH_LOGISTICS_SOURCE_HOST_ID", "legacy-host")

    with pytest.raises(LogisticsRuntimeConfigurationError, match="profile is missing"):
        package_client_from_env(
            transport=_transport,
            profile_decryptor=lambda _value: "machine-secret",
        )


def _enable_valid_test1_legacy_override(monkeypatch):
    run_root = runtime_module.Path(
        r"C:\KMTech\Test1\Runs\run-label-20260804"
    )
    ca_bundle = run_root / "tls" / "test1-ca.pem"
    monkeypatch.setattr(runtime_module.sys, "platform", "win32")
    monkeypatch.setenv(TEST1_ISOLATED_LEGACY_OVERRIDE_ENV, "1")
    monkeypatch.setenv("COMPUTERNAME", "test1")
    monkeypatch.setenv(
        "LABEL_MATCH_SAVE_DIR",
        str(run_root / "LabelMatch"),
    )
    monkeypatch.setenv(
        "LABEL_MATCH_LOGISTICS_API_BASE_URL",
        "https://127.0.0.1:19443",
    )
    monkeypatch.setenv(
        "LABEL_MATCH_LOGISTICS_API_TOKEN",
        "test1-label-token",
    )
    monkeypatch.setenv(
        "LABEL_MATCH_LOGISTICS_AUTHORITY_SCOPE_ID",
        "TEST1-LABEL-RUN",
    )
    monkeypatch.setenv(
        "LABEL_MATCH_LOGISTICS_SOURCE_HOST_ID",
        "test1-label-host",
    )
    monkeypatch.setenv(
        "LABEL_MATCH_LOGISTICS_DEVICE_ID",
        "test1-label-device",
    )
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", str(ca_bundle))
    monkeypatch.delenv("KM_LOGISTICS_PROFILE_PATH", raising=False)
    monkeypatch.delenv("KM_LOGISTICS_REQUIRED", raising=False)

    original_is_file = runtime_module.Path.is_file
    original_stat = runtime_module.Path.stat
    monkeypatch.setattr(
        runtime_module.Path,
        "is_file",
        lambda path: path == ca_bundle or original_is_file(path),
    )
    monkeypatch.setattr(
        runtime_module.Path,
        "stat",
        lambda path: (
            SimpleNamespace(st_size=20)
            if path == ca_bundle
            else original_stat(path)
        ),
    )
    return run_root, ca_bundle


def test_test1_isolated_legacy_override_uses_only_process_environment(
    monkeypatch,
):
    _enable_valid_test1_legacy_override(monkeypatch)
    machine_reads = []
    monkeypatch.setattr(
        runtime_module,
        "_machine_environment_value",
        lambda name: machine_reads.append(name)
        or {
            "KM_LOGISTICS_PROFILE_PATH": (
                r"C:\ProgramData\KMTech\Logistics\runtime-profile.json"
            ),
            "KM_LOGISTICS_REQUIRED": "1",
        }.get(name, ""),
    )

    assert runtime_module._runtime_environment(None) is os.environ
    assert load_logistics_runtime_profile(required=True) is None
    client = package_client_from_env(
        transport=_transport,
        probe_required=False,
    )

    assert client is not None
    assert client.config.base_url == "https://127.0.0.1:19443"
    assert client.config.authority_scope_id == "TEST1-LABEL-RUN"
    assert client.config.source_host_id == "test1-label-host"
    assert client.config.device_id == "test1-label-device"
    assert client.config.authoritative_required is False
    assert machine_reads == []


@pytest.mark.parametrize(
    ("environment_name", "value", "message"),
    [
        (
            TEST1_ISOLATED_LEGACY_OVERRIDE_ENV,
            "true",
            "must be exactly 1",
        ),
        ("COMPUTERNAME", "TEST10", "COMPUTERNAME=TEST1"),
        (
            "KM_LOGISTICS_PROFILE_PATH",
            "",
            "anchors to be absent",
        ),
        (
            "KM_LOGISTICS_REQUIRED",
            "0",
            "anchors to be absent",
        ),
        (
            "LABEL_MATCH_SAVE_DIR",
            "",
            "nonempty LABEL_MATCH_SAVE_DIR",
        ),
        (
            "LABEL_MATCH_SAVE_DIR",
            r"C:\KMTech\Test1\Runs",
            "nonempty run directory",
        ),
        (
            "LABEL_MATCH_SAVE_DIR",
            r"C:\ProgramData\KMTech\Test1\Runs\run-label-20260804",
            "LABEL_MATCH_SAVE_DIR under",
        ),
        (
            "LABEL_MATCH_LOGISTICS_API_BASE_URL",
            "http://127.0.0.1:19443",
            "exact loopback origin",
        ),
        (
            "LABEL_MATCH_LOGISTICS_API_BASE_URL",
            "https://localhost:19443",
            "exact loopback origin",
        ),
        (
            "LABEL_MATCH_LOGISTICS_API_BASE_URL",
            "https://127.0.0.1:19443/",
            "exact loopback origin",
        ),
        (
            "LABEL_MATCH_LOGISTICS_API_BASE_URL",
            "https://127.0.0.1:65536",
            "exact HTTPS loopback origin",
        ),
        (
            "LABEL_MATCH_LOGISTICS_AUTHORITY_SCOPE_ID",
            "PLANT-01",
            "TEST1- authority scope",
        ),
        (
            "LABEL_MATCH_LOGISTICS_AUTHORITY_SCOPE_ID",
            "TEST1-LABEL RUN",
            "TEST1- authority scope",
        ),
        (
            "LABEL_MATCH_LOGISTICS_SOURCE_HOST_ID",
            "label-host",
            "test1- source host",
        ),
        (
            "LABEL_MATCH_LOGISTICS_SOURCE_HOST_ID",
            "test1-label host",
            "test1- source host",
        ),
        (
            "LABEL_MATCH_LOGISTICS_DEVICE_ID",
            "label-device",
            "test1- device",
        ),
        (
            "LABEL_MATCH_LOGISTICS_API_TOKEN",
            "test1 token",
            "valid token",
        ),
        (
            "REQUESTS_CA_BUNDLE",
            (
                r"C:\KMTech\Test1\Runs\other-run"
                r"\tls\test1-ca.pem"
            ),
            "same run directory",
        ),
        (
            "REQUESTS_CA_BUNDLE",
            r"C:\ProgramData\test1-ca.pem",
            "REQUESTS_CA_BUNDLE under",
        ),
        (
            "REQUESTS_CA_BUNDLE",
            (
                r"C:\KMTech\Test1\Runs\run-label-20260804"
                r"\tls\missing.pem"
            ),
            "non-reparse file",
        ),
    ],
)
def test_test1_isolated_legacy_override_rejects_invalid_envelope(
    monkeypatch,
    environment_name,
    value,
    message,
):
    _enable_valid_test1_legacy_override(monkeypatch)
    monkeypatch.setenv(environment_name, value)
    machine_reads = []
    monkeypatch.setattr(
        runtime_module,
        "_machine_environment_value",
        lambda name: machine_reads.append(name) or "",
    )

    with pytest.raises(LogisticsRuntimeConfigurationError, match=message):
        load_logistics_runtime_profile(required=True)
    assert machine_reads == []


def test_test1_isolated_legacy_override_rejects_non_windows(monkeypatch):
    _enable_valid_test1_legacy_override(monkeypatch)
    monkeypatch.setattr(runtime_module.sys, "platform", "linux")

    with pytest.raises(LogisticsRuntimeConfigurationError, match="requires Windows"):
        load_logistics_runtime_profile(required=True)


def test_test1_isolated_legacy_override_rejects_reparse_ca_bundle(
    monkeypatch,
):
    _run_root, ca_bundle = _enable_valid_test1_legacy_override(monkeypatch)
    original_lstat = runtime_module.os.lstat

    def fake_lstat(path):
        if runtime_module.Path(path) == ca_bundle:
            return SimpleNamespace(
                st_mode=stat.S_IFREG,
                st_file_attributes=0x400,
            )
        return original_lstat(path)

    monkeypatch.setattr(runtime_module.os, "lstat", fake_lstat)
    with pytest.raises(
        LogisticsRuntimeConfigurationError,
        match="symlink|junction",
    ):
        load_logistics_runtime_profile(required=True)


@pytest.mark.skipif(os.name != "nt", reason="Windows Machine environment trust boundary")
def test_hklm_machine_profile_ignores_process_path_override(tmp_path, monkeypatch):
    machine = _profile(tmp_path / "machine-profile")
    process = _profile(tmp_path / "process-profile", base_url="https://attacker.invalid")
    monkeypatch.setenv("KM_LOGISTICS_PROFILE_PATH", str(process))
    monkeypatch.setenv("KM_LOGISTICS_REQUIRED", "0")
    values = {
        "KM_LOGISTICS_PROFILE_PATH": str(machine),
        "KM_LOGISTICS_REQUIRED": "1",
    }
    monkeypatch.setattr(
        runtime_module,
        "_machine_environment_value",
        lambda name: values.get(name, ""),
    )

    resolved = load_logistics_runtime_profile(decryptor=lambda _value: "machine-secret")

    assert resolved is not None
    assert resolved.base_url == "https://logistics.example.invalid"
    assert resolved.required is True


@pytest.mark.parametrize(
    "changes,message",
    [
        ({"base_url": "http://logistics.example.invalid"}, "HTTPS"),
        ({"base_url": "https://logistics.example.invalid/prefix"}, "HTTPS"),
        ({"base_url": "https://logistics.example.invalid:99999"}, "valid URL"),
        ({"base_url": "https://localhost:8443"}, "loopback"),
        ({"authority_plane": "SHADOW_CANDIDATE"}, "AUTHORITATIVE"),
        ({"ledger_plane": "UNKNOWN"}, "ledger_plane"),
        ({"bearer_token_ref": "dpapi:../token.dpapi"}, "profile directory"),
        ({"bearer_token": "plaintext"}, "plaintext"),
    ],
)
def test_invalid_machine_profile_fails_closed(tmp_path, monkeypatch, changes, message):
    path = _profile(tmp_path, **changes)
    _env(monkeypatch, path)
    with pytest.raises(LogisticsRuntimeConfigurationError, match=message):
        load_logistics_runtime_profile(decryptor=lambda _value: "secret")


def test_duplicate_profile_fields_and_whitespace_token_fail_closed(tmp_path, monkeypatch):
    path = _profile(tmp_path)
    raw = path.read_text(encoding="utf-8")
    path.write_text(
        raw.replace(
            '"base_url":',
            '"base_url":"https://attacker.invalid","base_url":',
            1,
        ),
        encoding="utf-8",
    )
    _env(monkeypatch, path)

    with pytest.raises(LogisticsRuntimeConfigurationError, match="duplicate field"):
        load_logistics_runtime_profile(decryptor=lambda _value: "secret")

    path = _profile(tmp_path / "token")
    _env(monkeypatch, path)
    with pytest.raises(LogisticsRuntimeConfigurationError, match="token"):
        load_logistics_runtime_profile(decryptor=lambda _value: "secret with spaces")


def test_required_packaging_never_returns_legacy_direct_sync_only(monkeypatch):
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app.run_tests = False
    app.is_running_simulation = False
    app._logistics_authoritative_required = True
    app.package_logistics_client = None
    app.current_set_info = {
        "raw": [
            "TRF=1|BND=T1|AUTH_SCOPE=S1|CLC=ITEM|QT=1|HSH="
            + ("a" * 64)
            + "|EPOCH=1|PLANE=AUTHORITATIVE|PE=1|SID=SID1|SREV=1|STK=TOKEN1",
            "ITEM-A",
            "ITEM-B",
            "ITEM-C",
            "FINAL",
        ]
    }
    monkeypatch.setattr(label_module, "logistics_runtime_required", lambda: True)

    with pytest.raises(PackageLogisticsError, match="AUTHORITATIVE_LOGISTICS_REQUIRED"):
        app._queue_authoritative_package(item_code="ITEM", is_manual_complete=False)


def test_required_mode_blocks_manual_completion(monkeypatch):
    app = label_module.Label_Match.__new__(label_module.Label_Match)
    app.run_tests = False
    app.is_running_simulation = False
    app._logistics_authoritative_required = True
    monkeypatch.setattr(label_module, "logistics_runtime_required", lambda: True)

    with pytest.raises(PackageLogisticsError, match="manual packaging completion"):
        app._queue_authoritative_package(item_code="ITEM", is_manual_complete=True)


def test_installer_dry_run_is_write_free_and_redacted(tmp_path, monkeypatch, capsys):
    token = "INSTALL-SECRET-MUST-NOT-PRINT"
    target = tmp_path / "not-created" / "profile.json"
    monkeypatch.setenv("INSTALL_TOKEN_TEST", token)
    result = install_main(
        [
            "--profile-path", str(target),
            "--base-url", "https://logistics.example.invalid",
            "--authority-scope", "scope-machine",
            "--authority-epoch", "7",
            "--ledger-plane", "SHADOW_CANDIDATE",
            "--plane-epoch", "3",
            "--device-id", "label-pc-01",
            "--source-host-id", "label-host-01",
            "--token-env", "INSTALL_TOKEN_TEST",
            "--dry-run",
        ]
    )
    captured = capsys.readouterr()
    assert result == 0
    assert token not in captured.out + captured.err
    report = json.loads(captured.out)
    assert report["authority_plane"] == "AUTHORITATIVE"
    assert report["ledger_plane"] == "SHADOW_CANDIDATE"
    assert not target.parent.exists()


def test_installer_validates_before_write_and_readiness_missing_is_blocked(tmp_path):
    target = tmp_path / "not-created" / "profile.json"
    with pytest.raises(LogisticsRuntimeConfigurationError, match="HTTPS"):
        install_runtime_profile(
            profile_path=target,
            base_url="http://invalid.example",
            authority_scope="scope-machine",
            authority_epoch=7,
            authority_plane="AUTHORITATIVE",
            plane_epoch=3,
            device_id="label-pc-01",
            source_host_id="label-host-01",
            bearer_token="secret",
        )
    assert not target.parent.exists()
    assert readiness_main(["--profile-path", str(target)]) == 2


def test_installer_requires_reader_principal_before_any_write(tmp_path):
    target = tmp_path / "not-created" / "profile.json"

    with pytest.raises(ValueError, match="reader_principal"):
        install_runtime_profile(
            profile_path=target,
            base_url="https://logistics.example.invalid",
            authority_scope="scope-machine",
            authority_epoch=7,
            authority_plane="AUTHORITATIVE",
            plane_epoch=3,
            device_id="label-pc-01",
            source_host_id="label-host-01",
            bearer_token="secret",
        )

    assert not target.parent.exists()


def test_installer_rejects_invalid_tls_ca_before_any_profile_write(
    tmp_path, monkeypatch
):
    target = tmp_path / "not-created" / "profile.json"
    invalid_ca = tmp_path / "invalid-ca.pem"
    invalid_ca.write_bytes(b"not-a-pem-certificate")
    monkeypatch.setattr(
        profile_installer_module,
        "protect_current_user_secret",
        lambda value: b"protected:" + value.encode("utf-8"),
    )

    with pytest.raises(
        LogisticsRuntimeConfigurationError,
        match="only PEM certificates",
    ):
        install_runtime_profile(
            profile_path=target,
            base_url="https://logistics.example.invalid",
            authority_scope="scope-current-user",
            authority_epoch=7,
            authority_plane="AUTHORITATIVE",
            plane_epoch=3,
            device_id="label-pc-user",
            source_host_id="label-host-user",
            bearer_token="secret",
            tls_ca_bundle_path=invalid_ca,
            credential_scope="current_user",
        )

    assert not target.parent.exists()


def test_current_user_profile_install_uses_current_user_dpapi_without_machine_acl(
    tmp_path, monkeypatch
):
    target = tmp_path / "current-user" / "runtime-profile.json"
    token = "current-user-secret"
    machine_acl_calls = []
    monkeypatch.setattr(
        profile_installer_module,
        "_secure_profile_directory",
        lambda *_args: machine_acl_calls.append(True),
    )
    monkeypatch.setattr(
        profile_installer_module,
        "protect_current_user_secret",
        lambda value: b"user-dpapi:" + value.encode("utf-8"),
    )
    monkeypatch.setattr(
        profile_installer_module,
        "unprotect_current_user_secret",
        lambda value: value.removeprefix(b"user-dpapi:").decode("utf-8"),
    )

    report = install_runtime_profile(
        profile_path=target,
        base_url="https://logistics.example.invalid:18456",
        authority_scope="scope-user",
        authority_epoch=7,
        authority_plane="AUTHORITATIVE",
        plane_epoch=3,
        device_id="label-pc-user",
        source_host_id="label-host-user",
        bearer_token=token,
        credential_scope="current_user",
    )

    payload = json.loads(target.read_text(encoding="utf-8"))
    secret_path = target.parent / "secrets" / "bearer-token.dpapi"
    assert report["status"] == "installed"
    assert report["credential_scope"] == "current_user"
    assert payload["credential_scope"] == "current_user"
    assert payload["authority_plane"] == "AUTHORITATIVE"
    assert secret_path.read_bytes() == b"user-dpapi:" + token.encode("utf-8")
    assert machine_acl_calls == []


def test_existing_current_user_profile_can_add_ca_without_rotating_secret(
    tmp_path, monkeypatch
):
    target = tmp_path / "current-user" / "runtime-profile.json"
    token = "current-user-secret"
    monkeypatch.setattr(
        profile_installer_module,
        "protect_current_user_secret",
        lambda value: b"user-dpapi:" + value.encode("utf-8"),
    )
    monkeypatch.setattr(
        profile_installer_module,
        "unprotect_current_user_secret",
        lambda value: value.removeprefix(b"user-dpapi:").decode("utf-8"),
    )
    install_runtime_profile(
        profile_path=target,
        base_url="https://logistics.example.invalid:18456",
        authority_scope="scope-user",
        authority_epoch=7,
        authority_plane="AUTHORITATIVE",
        plane_epoch=3,
        device_id="label-pc-user",
        source_host_id="label-host-user",
        bearer_token=token,
        credential_scope="current_user",
    )
    secret_path = target.parent / "secrets" / "bearer-token.dpapi"
    secret_before = secret_path.read_bytes()
    ca_source = tmp_path / "private-ca.cert.pem"
    ca_payload = _private_ca_pem()
    ca_source.write_bytes(ca_payload)

    report = install_tls_ca_bundle_for_existing_profile(
        profile_path=target,
        tls_ca_bundle_path=ca_source,
        credential_scope="current_user",
    )

    ca_target = target.parent / "tls" / "ca-bundle.pem"
    profile = json.loads(target.read_text(encoding="utf-8"))
    assert report["status"] == "upgraded"
    assert profile["tls_ca_bundle_path"] == str(ca_target.resolve())
    assert ca_target.read_bytes() == ca_payload
    assert secret_path.read_bytes() == secret_before

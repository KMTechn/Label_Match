"""Install one non-secret logistics profile and machine-scope DPAPI token."""

from __future__ import annotations

import argparse
from dataclasses import replace
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any, Mapping

from cryptography import x509


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from logistics_runtime_profile import (  # noqa: E402
    DEFAULT_TOKEN_REF,
    MAX_TLS_CA_BUNDLE_BYTES,
    PROFILE_CONTRACT_VERSION,
    assert_path_has_no_reparse_components,
    default_profile_path,
    load_logistics_runtime_profile,
    profile_from_values,
    protect_bearer_token,
)


DEFAULT_INSTALL_TOKEN_ENV = "KM_LOGISTICS_INSTALL_BEARER_TOKEN"
MACHINE_CREDENTIAL_BUNDLE_CONTRACT_VERSION = (
    "producer-self-enrollment-machine-credentials-v1"
)
MACHINE_CREDENTIAL_BUNDLE_FIELDS = frozenset(
    {"contract_version", "bindings", "credentials", "profiles"}
)
MACHINE_CREDENTIAL_BINDING_FIELDS = frozenset(
    {"app", "program", "source_host_id", "device_id", "authority_scope_id"}
)
MACHINE_CREDENTIAL_FIELDS = frozenset({"producer_ingest", "logistics"})
MACHINE_PROFILE_FIELDS = frozenset({"logistics"})
PRODUCER_INGEST_CREDENTIAL_FIELDS = frozenset(
    {"audience", "auth_scheme", "key_id", "secret"}
)
LOGISTICS_PROFILE_FIELDS = frozenset(
    {
        "contract_version", "base_url", "authority_scope", "authority_epoch",
        "authority_plane", "ledger_plane", "plane_epoch", "device_id",
        "source_host_id", "timeout_seconds",
    }
)
LOGISTICS_CREDENTIAL_FIELDS = frozenset(
    {"audience", "auth_scheme", "token_header", "token"}
)


def _secure_profile_directory(path: Path, reader_principal: str) -> None:
    reader = str(reader_principal or "").strip()
    if not reader or not re.fullmatch(r"\*?[A-Za-z0-9가-힣 _.-]+(?:\\[A-Za-z0-9가-힣 _.$-]+)?", reader):
        raise ValueError("reader_principal is required and must be a safe account name")
    if os.name != "nt":
        raise RuntimeError("machine profile ACL installation requires Windows")
    path.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "icacls", str(path), "/inheritance:r", "/grant:r",
            "*S-1-5-18:(OI)(CI)F", "*S-1-5-32-544:(OI)(CI)F",
            f"{reader}:(OI)(CI)R",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )


def _atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(data)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _resolve_tls_ca_bundle(
    source_path: str | os.PathLike[str] | None,
    profile_path: str | os.PathLike[str],
) -> tuple[Path, bytes] | None:
    value = str(source_path or "").strip()
    if not value:
        return None
    source = assert_path_has_no_reparse_components(
        value, label="TLS CA bundle source"
    ).resolve()
    if not source.is_file():
        raise ValueError("TLS CA bundle source is unavailable")
    try:
        size = source.stat().st_size
    except OSError as exc:
        raise ValueError("TLS CA bundle source could not be inspected") from exc
    if size <= 0 or size > MAX_TLS_CA_BUNDLE_BYTES:
        raise ValueError("TLS CA bundle source size is invalid")
    try:
        data = source.read_bytes()
    except OSError as exc:
        raise ValueError("TLS CA bundle source could not be read") from exc
    if len(data) != size or not data or len(data) > MAX_TLS_CA_BUNDLE_BYTES:
        raise ValueError("TLS CA bundle source size changed during inspection")
    try:
        x509.load_pem_x509_certificate(data)
    except ValueError:
        try:
            x509.load_der_x509_certificate(data)
        except ValueError as exc:
            raise ValueError(
                "TLS CA bundle source is not a valid certificate"
            ) from exc
    profile_target = assert_path_has_no_reparse_components(
        profile_path, label="runtime profile"
    ).resolve()
    target = assert_path_has_no_reparse_components(
        profile_target.parent / "tls" / "ca-bundle.pem",
        label="TLS CA bundle target",
    ).resolve()
    try:
        target.relative_to(profile_target.parent)
    except ValueError as exc:
        raise ValueError("TLS CA bundle target escapes the profile directory") from exc
    return target, data


def _profile_candidate_from_install_values(
    values: Mapping[str, Any],
    *,
    profile_path: Path,
    bearer_token: str,
    tls_ca_bundle: tuple[Path, bytes] | None,
):
    validation_values = dict(values)
    validation_values.pop("tls_ca_bundle_path", None)
    candidate = profile_from_values(
        validation_values,
        profile_path=profile_path,
        bearer_token=bearer_token,
        required=True,
    )
    if tls_ca_bundle is None:
        return candidate
    return replace(candidate, tls_ca_bundle_path=str(tls_ca_bundle[0]))


def install_runtime_profile(
    *,
    profile_path: str | os.PathLike[str],
    base_url: str,
    authority_scope: str,
    authority_epoch: int,
    authority_plane: str,
    ledger_plane: str | None = None,
    plane_epoch: int,
    device_id: str,
    source_host_id: str,
    bearer_token: str,
    timeout_seconds: float = 10.0,
    dry_run: bool = False,
    replace: bool = False,
    reader_principal: str = "",
    tls_ca_bundle_path: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    target = assert_path_has_no_reparse_components(
        profile_path, label="runtime profile"
    )
    tls_ca_bundle = _resolve_tls_ca_bundle(tls_ca_bundle_path, target)
    selected_ledger_plane = authority_plane if ledger_plane is None else ledger_plane
    values = {
        "contract_version": PROFILE_CONTRACT_VERSION,
        "base_url": base_url,
        "authority_scope": authority_scope,
        "authority_epoch": authority_epoch,
        "authority_plane": authority_plane,
        "ledger_plane": selected_ledger_plane,
        "plane_epoch": plane_epoch,
        "device_id": device_id,
        "source_host_id": source_host_id,
        "bearer_token_ref": DEFAULT_TOKEN_REF,
        "timeout_seconds": timeout_seconds,
    }
    if tls_ca_bundle is not None:
        values["tls_ca_bundle_path"] = str(tls_ca_bundle[0])
    validated = _profile_candidate_from_install_values(
        values,
        profile_path=target,
        bearer_token=bearer_token,
        tls_ca_bundle=tls_ca_bundle,
    )
    summary = validated.redacted_summary()
    summary["status"] = "dry-run" if dry_run else "installed"
    if dry_run:
        return summary
    if target.exists() and not replace:
        raise FileExistsError(
            "runtime profile already exists; use --replace for an intentional rotation"
        )
    if tls_ca_bundle is not None and tls_ca_bundle[0].exists() and not replace:
        raise FileExistsError("orphan machine logistics TLS CA bundle already exists")
    _secure_profile_directory(target.parent, reader_principal)
    protected = protect_bearer_token(bearer_token)
    secret_relative = DEFAULT_TOKEN_REF.split(":", 1)[1].replace("/", os.sep)
    secret_path = (target.parent / secret_relative).resolve()
    secret_path.relative_to(target.parent.resolve())
    created: list[Path] = []
    try:
        if tls_ca_bundle is not None:
            _atomic_write(tls_ca_bundle[0], tls_ca_bundle[1])
            created.append(tls_ca_bundle[0])
        _atomic_write(secret_path, protected)
        created.append(secret_path)
        _atomic_write(
            target,
            (json.dumps(values, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode(
                "utf-8"
            ),
        )
        created.append(target)
        readback = load_logistics_runtime_profile(required=True, profile_path=target)
        if readback is None or readback != validated:
            raise RuntimeError("runtime profile exact readback failed")
        if tls_ca_bundle is not None and tls_ca_bundle[0].read_bytes() != tls_ca_bundle[1]:
            raise RuntimeError("machine logistics TLS CA bundle exact readback failed")
    except Exception:
        for created_path in reversed(created):
            created_path.unlink(missing_ok=True)
        raise
    summary["profile_path"] = str(target)
    summary["created_paths"] = [str(target), str(secret_path)]
    if tls_ca_bundle is not None:
        summary["created_paths"].append(str(tls_ca_bundle[0]))
    return summary


def ensure_runtime_profile_from_enrollment_bundle(
    response_payload: Mapping[str, Any],
    *,
    expected_app: str,
    expected_program: str,
    expected_source_host_id: str,
    expected_device_id: str,
    reader_principal: str = "*S-1-5-32-545",
    profile_path: str | os.PathLike[str] | None = None,
    tls_ca_bundle_path: str | os.PathLike[str] | None = None,
) -> dict[str, Any] | None:
    bundle = response_payload.get("machine_credential_bundle")
    if bundle is None:
        return None
    if not isinstance(bundle, Mapping) or set(bundle) != MACHINE_CREDENTIAL_BUNDLE_FIELDS:
        raise ValueError("machine credential bundle fields are invalid")
    if bundle.get("contract_version") != MACHINE_CREDENTIAL_BUNDLE_CONTRACT_VERSION:
        raise ValueError("machine credential bundle contract is invalid")
    bindings = bundle.get("bindings")
    credentials = bundle.get("credentials")
    profiles = bundle.get("profiles")
    if not all(isinstance(item, Mapping) for item in (bindings, credentials, profiles)):
        raise ValueError("machine credential bundle sections are invalid")
    if set(bindings) != MACHINE_CREDENTIAL_BINDING_FIELDS:
        raise ValueError("machine credential bundle binding fields are invalid")
    if set(credentials) != MACHINE_CREDENTIAL_FIELDS:
        raise ValueError("machine credential bundle credential sections are invalid")
    if set(profiles) != MACHINE_PROFILE_FIELDS:
        raise ValueError("machine credential bundle profile sections are invalid")
    for field, expected in {
        "app": expected_app,
        "program": expected_program,
        "source_host_id": expected_source_host_id,
        "device_id": expected_device_id,
    }.items():
        if bindings.get(field) != expected:
            raise ValueError(f"machine credential bundle {field} binding mismatch")
    profile = profiles.get("logistics")
    producer_credential = credentials.get("producer_ingest")
    logistics_credential = credentials.get("logistics")
    if not isinstance(profile, Mapping) or set(profile) != LOGISTICS_PROFILE_FIELDS:
        raise ValueError("machine logistics profile fields are invalid")
    if (
        not isinstance(producer_credential, Mapping)
        or set(producer_credential) != PRODUCER_INGEST_CREDENTIAL_FIELDS
    ):
        raise ValueError("machine producer ingest credential fields are invalid")
    if (
        not isinstance(logistics_credential, Mapping)
        or set(logistics_credential) != LOGISTICS_CREDENTIAL_FIELDS
    ):
        raise ValueError("machine logistics credential fields are invalid")
    enrollment_key_id = response_payload.get("key_id")
    producer_secret = response_payload.get("secret")
    if (
        producer_credential.get("audience") != "producer-ingest-hmac-v1"
        or producer_credential.get("auth_scheme") != "hmac-sha256"
        or not isinstance(enrollment_key_id, str)
        or not enrollment_key_id.strip()
        or producer_credential.get("key_id") != enrollment_key_id
        or not isinstance(producer_secret, str)
        or not producer_secret.strip()
        or producer_credential.get("secret") != producer_secret
    ):
        raise ValueError("machine producer ingest credential contract is invalid")
    token = logistics_credential.get("token")
    if (
        logistics_credential.get("audience") != "worker-analysis-logistics-v1"
        or logistics_credential.get("auth_scheme") != "bearer"
        or logistics_credential.get("token_header") != "X-Logistics-API-Token"
        or not isinstance(token, str)
        or not token.strip()
    ):
        raise ValueError("machine logistics credential contract is invalid")
    if token == producer_secret:
        raise ValueError("machine credential audiences must use distinct secrets")
    if (
        profile.get("source_host_id") != expected_source_host_id
        or profile.get("device_id") != expected_device_id
        or not isinstance(bindings.get("authority_scope_id"), str)
        or not bindings["authority_scope_id"].strip()
        or profile.get("authority_scope") != bindings["authority_scope_id"]
    ):
        raise ValueError("machine logistics profile identity mismatch")
    target = Path(profile_path) if profile_path is not None else default_profile_path()
    tls_ca_bundle = _resolve_tls_ca_bundle(tls_ca_bundle_path, target)
    values = dict(profile)
    values["bearer_token_ref"] = DEFAULT_TOKEN_REF
    if tls_ca_bundle is not None:
        values["tls_ca_bundle_path"] = str(tls_ca_bundle[0])
    candidate = _profile_candidate_from_install_values(
        values,
        profile_path=target,
        bearer_token=token,
        tls_ca_bundle=tls_ca_bundle,
    )
    if target.exists():
        existing = load_logistics_runtime_profile(required=True, profile_path=target)
        if existing != candidate:
            raise FileExistsError("existing machine logistics profile conflicts with enrollment")
        if tls_ca_bundle is not None:
            try:
                existing_ca_bytes = tls_ca_bundle[0].read_bytes()
            except OSError as exc:
                raise FileExistsError(
                    "existing machine logistics TLS CA bundle is unavailable"
                ) from exc
            if existing_ca_bytes != tls_ca_bundle[1]:
                raise FileExistsError(
                    "existing machine logistics TLS CA bundle conflicts with enrollment"
                )
        summary = existing.redacted_summary()
        summary.update({"status": "reused", "profile_path": str(target), "created_paths": []})
        return summary
    secret_path = target.parent / DEFAULT_TOKEN_REF.split(":", 1)[1].replace("/", os.sep)
    if secret_path.exists():
        raise FileExistsError("orphan machine logistics credential already exists")
    if tls_ca_bundle is not None and tls_ca_bundle[0].exists():
        raise FileExistsError("orphan machine logistics TLS CA bundle already exists")
    return install_runtime_profile(
        profile_path=target,
        base_url=str(profile["base_url"]),
        authority_scope=str(profile["authority_scope"]),
        authority_epoch=int(profile["authority_epoch"]),
        authority_plane=str(profile["authority_plane"]),
        ledger_plane=str(profile["ledger_plane"]),
        plane_epoch=int(profile["plane_epoch"]),
        device_id=str(profile["device_id"]),
        source_host_id=str(profile["source_host_id"]),
        bearer_token=token,
        timeout_seconds=float(profile["timeout_seconds"]),
        reader_principal=reader_principal,
        tls_ca_bundle_path=tls_ca_bundle_path,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Install the shared KMTech logistics PC profile.")
    parser.add_argument("--profile-path", default=str(default_profile_path()))
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--authority-scope", required=True)
    parser.add_argument("--authority-epoch", type=int, required=True)
    parser.add_argument("--authority-plane", default="AUTHORITATIVE")
    parser.add_argument("--ledger-plane")
    parser.add_argument("--plane-epoch", type=int, required=True)
    parser.add_argument("--device-id", required=True)
    parser.add_argument("--source-host-id", required=True)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--token-env", default=DEFAULT_INSTALL_TOKEN_ENV)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--replace", action="store_true")
    parser.add_argument("--reader-principal", help="Windows account allowed to read the DPAPI blob")
    parser.add_argument("--tls-ca-bundle-path", default="")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    token = str(os.environ.get(args.token_env) or "").strip()
    if not token:
        print("BLOCKED: bearer token environment value is missing", file=sys.stderr)
        return 2
    try:
        report = install_runtime_profile(
            profile_path=args.profile_path,
            base_url=args.base_url,
            authority_scope=args.authority_scope,
            authority_epoch=args.authority_epoch,
            authority_plane=args.authority_plane,
            ledger_plane=args.ledger_plane,
            plane_epoch=args.plane_epoch,
            device_id=args.device_id,
            source_host_id=args.source_host_id,
            bearer_token=token,
            timeout_seconds=args.timeout_seconds,
            dry_run=args.dry_run,
            replace=args.replace,
            reader_principal=args.reader_principal or "",
            tls_ca_bundle_path=args.tls_ca_bundle_path,
        )
    except Exception as exc:
        print(f"BLOCKED: {exc.__class__.__name__}: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

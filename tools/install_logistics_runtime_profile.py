"""Install one non-secret logistics profile and its DPAPI token."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, replace as dataclass_replace
from decimal import Decimal
import json
import math
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
    LogisticsRuntimeConfigurationError,
    MAX_TLS_CA_BUNDLE_BYTES,
    PROFILE_CONTRACT_VERSION,
    assert_path_has_no_reparse_components,
    default_profile_path,
    load_logistics_runtime_profile,
    profile_from_values,
    protect_bearer_token,
    protect_current_user_secret,
    unprotect_current_user_secret,
)


DEFAULT_INSTALL_TOKEN_ENV = "KM_LOGISTICS_INSTALL_BEARER_TOKEN"
TLS_CA_BUNDLE_RELATIVE_PATH = Path("tls") / "ca-bundle.pem"
_PEM_CERTIFICATE_BLOCK_RE = re.compile(
    rb"-----BEGIN CERTIFICATE-----\r?\n"
    rb"(?:[A-Za-z0-9+/]+={0,2}\r?\n)+"
    rb"-----END CERTIFICATE-----",
)
_PEM_LINE_BREAKS_RE = re.compile(rb"(?:\r?\n)*")
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
        "contract_version",
        "base_url",
        "authority_scope",
        "authority_epoch",
        "authority_plane",
        "ledger_plane",
        "plane_epoch",
        "device_id",
        "source_host_id",
        "timeout_seconds",
    }
)
LOGISTICS_CREDENTIAL_FIELDS = frozenset(
    {"audience", "auth_scheme", "token_header", "token"}
)


@dataclass(frozen=True)
class _ResolvedTlsCaBundle:
    source_path: Path
    target_path: Path
    content: bytes


def _semantic_json_value(value: Any) -> tuple[Any, ...]:
    """Normalize number spelling while preserving every JSON value type."""

    if value is None:
        return ("null",)
    if isinstance(value, bool):
        return ("boolean", value)
    if isinstance(value, int):
        return ("number", Decimal(value))
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("profile comparison requires finite JSON numbers")
        return ("number", Decimal(str(value)))
    if isinstance(value, str):
        return ("string", value)
    if isinstance(value, Mapping):
        return (
            "object",
            tuple(
                (str(key), _semantic_json_value(item))
                for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            ),
        )
    if isinstance(value, (list, tuple)):
        return ("array", tuple(_semantic_json_value(item) for item in value))
    raise TypeError(
        "profile comparison value is not JSON-compatible: "
        f"{type(value).__name__}"
    )


def _semantic_json_equal(left: Any, right: Any) -> bool:
    return _semantic_json_value(left) == _semantic_json_value(right)


def _read_existing_profile_values_for_comparison(path: Path) -> dict[str, Any]:
    """Read the already-validated profile again without erasing JSON types."""

    try:
        value = json.loads(path.read_bytes().decode("utf-8-sig"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LogisticsRuntimeConfigurationError(
            "existing machine logistics profile cannot be compared"
        ) from exc
    if not isinstance(value, dict):
        raise LogisticsRuntimeConfigurationError(
            "existing machine logistics profile comparison requires an object"
        )
    return value


def _validate_tls_ca_bundle_pem(content: bytes) -> None:
    """Require the complete payload to contain PEM certificates and whitespace only."""

    cursor = 0
    certificate_count = 0
    for match in _PEM_CERTIFICATE_BLOCK_RE.finditer(content):
        separator = content[cursor : match.start()]
        if (
            (certificate_count == 0 and separator)
            or (certificate_count > 0 and not separator)
            or _PEM_LINE_BREAKS_RE.fullmatch(separator) is None
        ):
            raise LogisticsRuntimeConfigurationError(
                "TLS CA bundle source must contain only PEM certificates"
            )
        try:
            x509.load_pem_x509_certificate(match.group(0))
        except ValueError as exc:
            raise LogisticsRuntimeConfigurationError(
                "TLS CA bundle source contains an invalid PEM certificate"
            ) from exc
        certificate_count += 1
        cursor = match.end()
    if (
        certificate_count == 0
        or _PEM_LINE_BREAKS_RE.fullmatch(content[cursor:]) is None
    ):
        raise LogisticsRuntimeConfigurationError(
            "TLS CA bundle source must contain only PEM certificates"
        )


def _resolve_tls_ca_bundle(
    source_path: str | os.PathLike[str] | None,
    profile_path: str | os.PathLike[str],
) -> _ResolvedTlsCaBundle | None:
    source_text = str(source_path or "").strip()
    if not source_text:
        return None
    source = assert_path_has_no_reparse_components(
        source_text, label="TLS CA bundle source"
    )
    if not source.is_file():
        raise LogisticsRuntimeConfigurationError("TLS CA bundle source is unavailable")
    try:
        metadata_before = source.stat()
        with source.open("rb") as handle:
            content = handle.read(MAX_TLS_CA_BUNDLE_BYTES + 1)
            metadata_open = os.fstat(handle.fileno())
        source = assert_path_has_no_reparse_components(
            source, label="TLS CA bundle source"
        )
        metadata_after = source.stat()
    except OSError as exc:
        raise LogisticsRuntimeConfigurationError(
            "TLS CA bundle source could not be read"
        ) from exc
    if not 0 < metadata_before.st_size <= MAX_TLS_CA_BUNDLE_BYTES:
        raise LogisticsRuntimeConfigurationError("TLS CA bundle source size is invalid")
    if (
        len(content) != metadata_before.st_size
        or not os.path.samestat(metadata_before, metadata_open)
        or not os.path.samestat(metadata_before, metadata_after)
        or metadata_before.st_mtime_ns != metadata_open.st_mtime_ns
        or metadata_before.st_mtime_ns != metadata_after.st_mtime_ns
    ):
        raise LogisticsRuntimeConfigurationError(
            "TLS CA bundle source changed while it was being read"
        )
    _validate_tls_ca_bundle_pem(content)
    profile_target = assert_path_has_no_reparse_components(
        profile_path, label="runtime profile"
    )
    ca_target = assert_path_has_no_reparse_components(
        profile_target.parent / TLS_CA_BUNDLE_RELATIVE_PATH,
        label="TLS CA bundle target",
    )
    return _ResolvedTlsCaBundle(
        source_path=source,
        target_path=ca_target,
        content=content,
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
    credential_scope: str = "machine",
) -> dict[str, Any]:
    selected_scope = str(credential_scope or "").strip().lower()
    if selected_scope not in {"machine", "current_user"}:
        raise ValueError("credential_scope must be machine or current_user")
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
    if selected_scope == "current_user":
        values["credential_scope"] = "current_user"
    if tls_ca_bundle is not None:
        values["tls_ca_bundle_path"] = str(tls_ca_bundle.target_path)
    validation_values = dict(values)
    validation_values.pop("tls_ca_bundle_path", None)
    validated = profile_from_values(
        validation_values,
        profile_path=target,
        bearer_token=bearer_token,
        required=True,
    )
    if tls_ca_bundle is not None:
        validated = dataclass_replace(
            validated,
            tls_ca_bundle_path=str(tls_ca_bundle.target_path),
        )
    summary = validated.redacted_summary()
    summary["status"] = "dry-run" if dry_run else "installed"
    summary["credential_scope"] = selected_scope
    if dry_run:
        return summary
    if target.exists() and not replace:
        raise FileExistsError(
            "runtime profile already exists; use --replace for an intentional rotation"
        )
    if (
        tls_ca_bundle is not None
        and tls_ca_bundle.target_path.exists()
        and not replace
    ):
        raise FileExistsError("orphan machine logistics TLS CA bundle already exists")
    if selected_scope == "machine":
        _secure_profile_directory(target.parent, reader_principal)
        protected = protect_bearer_token(bearer_token)
        decryptor = None
    else:
        target.parent.mkdir(parents=True, exist_ok=True)
        protected = protect_current_user_secret(bearer_token)
        decryptor = unprotect_current_user_secret
    secret_relative = DEFAULT_TOKEN_REF.split(":", 1)[1].replace("/", os.sep)
    secret_path = (target.parent / secret_relative).resolve()
    secret_path.relative_to(target.parent.resolve())
    created: list[Path] = []
    try:
        if tls_ca_bundle is not None:
            _atomic_write(tls_ca_bundle.target_path, tls_ca_bundle.content)
            created.append(tls_ca_bundle.target_path)
        _atomic_write(secret_path, protected)
        created.append(secret_path)
        _atomic_write(
            target,
            (json.dumps(values, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode(
                "utf-8"
            ),
        )
        created.append(target)
        readback = load_logistics_runtime_profile(
            required=True,
            profile_path=target,
            decryptor=decryptor,
        )
        if readback is None or readback != validated:
            raise RuntimeError("runtime profile exact readback failed")
        if (
            tls_ca_bundle is not None
            and tls_ca_bundle.target_path.read_bytes() != tls_ca_bundle.content
        ):
            raise RuntimeError("runtime profile TLS CA bundle exact readback failed")
    except Exception:
        for created_path in reversed(created):
            created_path.unlink(missing_ok=True)
        raise
    summary["profile_path"] = str(target)
    summary["created_paths"] = [str(target), str(secret_path)]
    if tls_ca_bundle is not None:
        summary["created_paths"].append(str(tls_ca_bundle.target_path))
    return summary


def install_tls_ca_bundle_for_existing_profile(
    *,
    profile_path: str | os.PathLike[str],
    tls_ca_bundle_path: str | os.PathLike[str],
    credential_scope: str = "current_user",
) -> dict[str, Any]:
    """Attach a verified CA bundle without rotating or weakening profile credentials."""

    selected_scope = str(credential_scope or "").strip().lower()
    if selected_scope not in {"machine", "current_user"}:
        raise ValueError("credential_scope must be machine or current_user")
    target = assert_path_has_no_reparse_components(
        profile_path, label="runtime profile"
    )
    tls_ca_bundle = _resolve_tls_ca_bundle(tls_ca_bundle_path, target)
    if tls_ca_bundle is None:
        raise ValueError("tls_ca_bundle_path is required")
    decryptor = (
        unprotect_current_user_secret if selected_scope == "current_user" else None
    )
    existing = load_logistics_runtime_profile(
        required=True,
        profile_path=target,
        decryptor=decryptor,
    )
    if existing is None:
        raise FileNotFoundError("runtime profile is unavailable")
    expected_ca_path = str(tls_ca_bundle.target_path.resolve())
    if existing.tls_ca_bundle_path:
        if str(Path(existing.tls_ca_bundle_path).resolve()) != expected_ca_path:
            raise FileExistsError("existing runtime profile TLS CA path conflicts")
        try:
            existing_content = tls_ca_bundle.target_path.read_bytes()
        except OSError as exc:
            raise FileExistsError(
                "existing runtime profile TLS CA bundle is unavailable"
            ) from exc
        if existing_content != tls_ca_bundle.content:
            raise FileExistsError("existing runtime profile TLS CA bundle conflicts")
        summary = existing.redacted_summary()
        summary.update({"status": "reused", "updated_paths": []})
        return summary

    original_profile = target.read_bytes()
    try:
        profile_values = json.loads(original_profile.decode("utf-8"))
    except (UnicodeError, ValueError, TypeError, json.JSONDecodeError) as exc:
        raise LogisticsRuntimeConfigurationError(
            "runtime profile could not be upgraded"
        ) from exc
    if not isinstance(profile_values, dict):
        raise LogisticsRuntimeConfigurationError("runtime profile must be an object")
    if str(profile_values.get("tls_ca_bundle_path") or "").strip():
        raise FileExistsError("existing runtime profile TLS CA path conflicts")
    if (
        selected_scope == "current_user"
        and profile_values.get("credential_scope") != "current_user"
    ):
        raise LogisticsRuntimeConfigurationError(
            "runtime profile credential scope conflicts with current-user upgrade"
        )
    profile_values["tls_ca_bundle_path"] = expected_ca_path
    upgraded_profile = (
        json.dumps(profile_values, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    ).encode("utf-8")
    ca_existed = tls_ca_bundle.target_path.exists()
    if ca_existed and tls_ca_bundle.target_path.read_bytes() != tls_ca_bundle.content:
        raise FileExistsError("existing runtime profile TLS CA bundle conflicts")
    expected = dataclass_replace(existing, tls_ca_bundle_path=expected_ca_path)
    try:
        if not ca_existed:
            _atomic_write(tls_ca_bundle.target_path, tls_ca_bundle.content)
        _atomic_write(target, upgraded_profile)
        readback = load_logistics_runtime_profile(
            required=True,
            profile_path=target,
            decryptor=decryptor,
        )
        if readback != expected:
            raise RuntimeError("upgraded runtime profile exact readback failed")
        if tls_ca_bundle.target_path.read_bytes() != tls_ca_bundle.content:
            raise RuntimeError("upgraded runtime profile TLS CA exact readback failed")
    except Exception:
        _atomic_write(target, original_profile)
        if not ca_existed:
            tls_ca_bundle.target_path.unlink(missing_ok=True)
        raise
    summary = expected.redacted_summary()
    summary.update(
        {
            "status": "upgraded",
            "updated_paths": [str(target), str(tls_ca_bundle.target_path)],
        }
    )
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
    credential_scope: str = "machine",
) -> dict[str, Any] | None:
    """Install the server-issued logistics profile without rotating local state."""

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
    expected_bindings = {
        "app": expected_app,
        "program": expected_program,
        "source_host_id": expected_source_host_id,
        "device_id": expected_device_id,
    }
    for field, expected in expected_bindings.items():
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
    target = assert_path_has_no_reparse_components(
        Path(profile_path) if profile_path is not None else default_profile_path(),
        label="runtime profile",
    )
    tls_ca_bundle = _resolve_tls_ca_bundle(tls_ca_bundle_path, target)
    values = dict(profile)
    values["bearer_token_ref"] = DEFAULT_TOKEN_REF
    selected_scope = str(credential_scope or "").strip().lower()
    if selected_scope not in {"machine", "current_user"}:
        raise ValueError("credential_scope must be machine or current_user")
    if selected_scope == "current_user":
        values["credential_scope"] = "current_user"
    if tls_ca_bundle is not None:
        values["tls_ca_bundle_path"] = str(tls_ca_bundle.target_path)
    candidate_values = dict(values)
    candidate_values.pop("tls_ca_bundle_path", None)
    candidate = profile_from_values(
        candidate_values,
        profile_path=target,
        bearer_token=token,
        required=True,
    )
    if tls_ca_bundle is not None:
        candidate = dataclass_replace(
            candidate,
            tls_ca_bundle_path=str(tls_ca_bundle.target_path),
        )
    if target.exists():
        existing = load_logistics_runtime_profile(
            required=True,
            profile_path=target,
            decryptor=(
                unprotect_current_user_secret
                if selected_scope == "current_user"
                else None
            ),
        )
        existing_values = _read_existing_profile_values_for_comparison(target)
        if not _semantic_json_equal(existing_values, values):
            raise FileExistsError(
                "existing machine logistics profile conflicts with enrollment"
            )
        if existing != candidate:
            raise FileExistsError("existing machine logistics profile conflicts with enrollment")
        if tls_ca_bundle is not None:
            try:
                existing_ca_content = tls_ca_bundle.target_path.read_bytes()
            except OSError as exc:
                raise FileExistsError(
                    "existing machine logistics TLS CA bundle is unavailable"
                ) from exc
            if existing_ca_content != tls_ca_bundle.content:
                raise FileExistsError(
                    "existing machine logistics TLS CA bundle conflicts with enrollment"
                )
        summary = existing.redacted_summary()
        summary.update(
            {
                "status": "reused",
                "profile_path": str(target),
                "created_paths": [],
                "credential_scope": selected_scope,
            }
        )
        return summary
    secret_path = target.parent / DEFAULT_TOKEN_REF.split(":", 1)[1].replace("/", os.sep)
    if secret_path.exists():
        raise FileExistsError("orphan machine logistics credential already exists")
    if tls_ca_bundle is not None and tls_ca_bundle.target_path.exists():
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
        credential_scope=selected_scope,
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
    parser.add_argument("--tls-ca-bundle-path", default="")
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--token-env", default=DEFAULT_INSTALL_TOKEN_ENV)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--replace", action="store_true")
    parser.add_argument("--reader-principal", help="Windows account allowed to read the DPAPI blob")
    parser.add_argument(
        "--credential-scope",
        choices=("machine", "current_user"),
        default="machine",
    )
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
            credential_scope=args.credential_scope,
        )
    except Exception as exc:
        print(f"BLOCKED: {exc.__class__.__name__}: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

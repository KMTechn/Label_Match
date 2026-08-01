"""Offline verification and canonical identity for the protected administrator.

The protected credential and its verifier are intentionally absent from source.
An elevated installer creates a machine-local verifier profile with a fresh salt;
desktop callers only read that profile and immediately replace an authenticated
credential entry with the non-secret canonical operator id.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import secrets
import stat
from typing import Any

from logistics_runtime_profile import assert_path_has_no_reparse_components


PROTECTED_ADMIN_OPERATOR_ID = "protected-admin-local"
PROTECTED_ADMIN_DISPLAY_NAME = "보호된 관리자"
PROTECTED_ADMIN_ROLE = "ADMIN"
PROTECTED_ADMIN_PROFILE_SCHEMA = 1
PROTECTED_ADMIN_KDF = "pbkdf2-hmac-sha256"
PROTECTED_ADMIN_MIN_ITERATIONS = 600_000
PROTECTED_ADMIN_DEFAULT_ITERATIONS = PROTECTED_ADMIN_MIN_ITERATIONS
PROTECTED_ADMIN_ITERATIONS = PROTECTED_ADMIN_DEFAULT_ITERATIONS
PROTECTED_ADMIN_MAX_ITERATIONS = 5_000_000
MAX_PROTECTED_ADMIN_ITERATIONS = PROTECTED_ADMIN_MAX_ITERATIONS
PROTECTED_ADMIN_SALT_BYTES = 16
PROTECTED_ADMIN_DIGEST_BYTES = hashlib.sha256().digest_size
PROTECTED_ADMIN_PROFILE_ENV = "LABEL_MATCH_PROTECTED_ADMIN_PROFILE"
PROGRAM_DATA_ENV_NAMES = ("PROGRAMDATA", "ProgramData")
MAX_PROTECTED_ADMIN_PROFILE_BYTES = 16 * 1024

_PROFILE_FIELDS = frozenset(
    {"schema_version", "operator_id", "display_name", "role", "verifier"}
)
_VERIFIER_FIELDS = frozenset(
    {"algorithm", "iterations", "salt_hex", "digest_hex"}
)
_LOWERCASE_HEX_RE = re.compile(r"^[0-9a-f]+$")

__all__ = [
    "MAX_PROTECTED_ADMIN_ITERATIONS",
    "MAX_PROTECTED_ADMIN_PROFILE_BYTES",
    "PROTECTED_ADMIN_DEFAULT_ITERATIONS",
    "PROTECTED_ADMIN_DIGEST_BYTES",
    "PROTECTED_ADMIN_DISPLAY_NAME",
    "PROTECTED_ADMIN_ITERATIONS",
    "PROTECTED_ADMIN_KDF",
    "PROTECTED_ADMIN_MAX_ITERATIONS",
    "PROTECTED_ADMIN_MIN_ITERATIONS",
    "PROTECTED_ADMIN_OPERATOR_ID",
    "PROTECTED_ADMIN_PROFILE_ENV",
    "PROTECTED_ADMIN_PROFILE_SCHEMA",
    "PROTECTED_ADMIN_ROLE",
    "PROTECTED_ADMIN_SALT_BYTES",
    "ProtectedAdminProfileError",
    "build_protected_admin_profile",
    "canonical_operator_id",
    "default_protected_admin_profile_path",
    "display_operator_name",
    "is_protected_admin_candidate",
    "is_protected_admin_code",
    "load_protected_admin_profile",
    "operator_role",
    "persistent_operator_name",
    "redact_authenticated_credential_entry",
    "redact_protected_admin_code",
    "redact_protected_admin_identity",
    "sanitize_persistent_value",
    "validate_protected_admin_profile",
]


class ProtectedAdminProfileError(RuntimeError):
    """Raised when the protected-administrator profile is unusable."""


def default_protected_admin_profile_path() -> str:
    override = os.environ.get(PROTECTED_ADMIN_PROFILE_ENV, "").strip()
    if override:
        return override
    program_data = next(
        (
            os.environ.get(name, "").strip()
            for name in PROGRAM_DATA_ENV_NAMES
            if os.environ.get(name, "").strip()
        ),
        r"C:\ProgramData",
    )
    return str(
        Path(program_data)
        / "KMTech"
        / "Label_Match"
        / "protected"
        / "protected_admin.json"
    )


def _checked_profile_path(
    value: str | os.PathLike[str], *, label: str
) -> Path:
    try:
        return assert_path_has_no_reparse_components(value, label=label)
    except Exception as exc:
        raise ProtectedAdminProfileError(f"{label} path is unsafe") from exc


def _same_profile_snapshot(left: os.stat_result, right: os.stat_result) -> bool:
    return bool(
        os.path.samestat(left, right)
        and left.st_size == right.st_size
        and left.st_mtime_ns == right.st_mtime_ns
    )


def _validate_candidate(candidate: object) -> str:
    try:
        value = str(candidate or "").strip()
    except Exception as exc:
        raise ValueError("protected administrator credential format is invalid") from exc
    if not (len(value) == 6 and value.isascii() and value.isdecimal()):
        raise ValueError("protected administrator credential must be six ASCII digits")
    return value


def _validate_iterations(iterations: object) -> int:
    if type(iterations) is not int or not (
        PROTECTED_ADMIN_MIN_ITERATIONS
        <= iterations
        <= PROTECTED_ADMIN_MAX_ITERATIONS
    ):
        raise ProtectedAdminProfileError(
            "protected administrator profile iteration count is invalid"
        )
    return iterations


def _validate_lowercase_hex(value: object, *, byte_length: int, label: str) -> str:
    if type(value) is not str:
        raise ProtectedAdminProfileError(
            f"protected administrator profile {label} is invalid"
        )
    if len(value) != byte_length * 2 or not _LOWERCASE_HEX_RE.fullmatch(value):
        raise ProtectedAdminProfileError(
            f"protected administrator profile {label} is invalid"
        )
    return value


def validate_protected_admin_profile(payload: object) -> dict[str, object]:
    """Validate the exact dynamic profile schema and return it unchanged."""
    if type(payload) is not dict or set(payload) != _PROFILE_FIELDS:
        raise ProtectedAdminProfileError(
            "protected administrator profile fields are invalid"
        )
    if type(payload["schema_version"]) is not int or (
        payload["schema_version"] != PROTECTED_ADMIN_PROFILE_SCHEMA
    ):
        raise ProtectedAdminProfileError(
            "protected administrator profile schema version is invalid"
        )
    expected_metadata = {
        "operator_id": PROTECTED_ADMIN_OPERATOR_ID,
        "display_name": PROTECTED_ADMIN_DISPLAY_NAME,
        "role": PROTECTED_ADMIN_ROLE,
    }
    if any(payload[key] != expected for key, expected in expected_metadata.items()):
        raise ProtectedAdminProfileError(
            "protected administrator profile identity metadata is invalid"
        )

    verifier = payload["verifier"]
    if type(verifier) is not dict or set(verifier) != _VERIFIER_FIELDS:
        raise ProtectedAdminProfileError(
            "protected administrator verifier fields are invalid"
        )
    if verifier["algorithm"] != PROTECTED_ADMIN_KDF:
        raise ProtectedAdminProfileError(
            "protected administrator profile algorithm is invalid"
        )
    _validate_iterations(verifier["iterations"])
    _validate_lowercase_hex(
        verifier["salt_hex"],
        byte_length=PROTECTED_ADMIN_SALT_BYTES,
        label="salt",
    )
    _validate_lowercase_hex(
        verifier["digest_hex"],
        byte_length=PROTECTED_ADMIN_DIGEST_BYTES,
        label="digest",
    )
    return payload


def build_protected_admin_profile(
    candidate: object,
    *,
    iterations: int = PROTECTED_ADMIN_DEFAULT_ITERATIONS,
) -> dict[str, object]:
    """Build a verifier for an explicit credential using a fresh random salt."""
    value = _validate_candidate(candidate)
    checked_iterations = _validate_iterations(iterations)
    salt = secrets.token_bytes(PROTECTED_ADMIN_SALT_BYTES)
    if len(salt) != PROTECTED_ADMIN_SALT_BYTES:
        raise RuntimeError("secure random salt generation failed")
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        value.encode("utf-8"),
        salt,
        checked_iterations,
        dklen=PROTECTED_ADMIN_DIGEST_BYTES,
    )
    profile: dict[str, object] = {
        "schema_version": PROTECTED_ADMIN_PROFILE_SCHEMA,
        "operator_id": PROTECTED_ADMIN_OPERATOR_ID,
        "display_name": PROTECTED_ADMIN_DISPLAY_NAME,
        "role": PROTECTED_ADMIN_ROLE,
        "verifier": {
            "algorithm": PROTECTED_ADMIN_KDF,
            "iterations": checked_iterations,
            "salt_hex": salt.hex(),
            "digest_hex": digest.hex(),
        },
    }
    return validate_protected_admin_profile(profile)


def load_protected_admin_profile(
    profile_path: str | os.PathLike[str] | None = None,
) -> dict[str, object]:
    """Load the exact provisioned verifier contract or fail closed."""
    target = _checked_profile_path(
        profile_path or default_protected_admin_profile_path(),
        label="protected administrator profile",
    )

    def reject_duplicate_fields(
        pairs: list[tuple[str, Any]],
    ) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ProtectedAdminProfileError(
                    "protected administrator profile contains duplicate fields"
                )
            result[key] = value
        return result

    try:
        metadata = os.lstat(target)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_size < 1
            or metadata.st_size > MAX_PROTECTED_ADMIN_PROFILE_BYTES
        ):
            raise ProtectedAdminProfileError(
                "protected administrator profile size is invalid"
            )
        with target.open("rb") as handle:
            opened_before = os.fstat(handle.fileno())
            if not _same_profile_snapshot(metadata, opened_before):
                raise ProtectedAdminProfileError(
                    "protected administrator profile changed before being read"
                )
            raw = handle.read(MAX_PROTECTED_ADMIN_PROFILE_BYTES + 1)
            opened_after = os.fstat(handle.fileno())
        _checked_profile_path(target, label="protected administrator profile")
        metadata_after = os.lstat(target)
        if (
            not _same_profile_snapshot(opened_before, opened_after)
            or not _same_profile_snapshot(opened_after, metadata_after)
            or len(raw) != metadata.st_size
            or len(raw) > MAX_PROTECTED_ADMIN_PROFILE_BYTES
        ):
            raise ProtectedAdminProfileError(
                "protected administrator profile changed while being read"
            )
        payload = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=reject_duplicate_fields,
        )
        return validate_protected_admin_profile(payload)
    except ProtectedAdminProfileError:
        raise
    except Exception as exc:
        raise ProtectedAdminProfileError(
            "protected administrator profile is unavailable"
        ) from exc


def is_protected_admin_candidate(candidate: object) -> bool:
    """Return whether input has the protected credential-entry shape."""
    try:
        value = str(candidate or "").strip()
    except Exception:
        return False
    return len(value) == 6 and value.isascii() and value.isdecimal()


def display_operator_name(
    operator_id: object,
    *,
    authenticated_credential_entry: bool = False,
) -> str:
    """Return worker-facing copy without exposing the canonical principal id."""
    value = str(operator_id or "").strip()
    if value in {PROTECTED_ADMIN_OPERATOR_ID, PROTECTED_ADMIN_DISPLAY_NAME}:
        return PROTECTED_ADMIN_DISPLAY_NAME
    if authenticated_credential_entry and is_protected_admin_candidate(value):
        return PROTECTED_ADMIN_DISPLAY_NAME
    return value


def is_protected_admin_code(
    candidate: object,
    *,
    profile_path: str | os.PathLike[str] | None = None,
) -> bool:
    """Verify one credential entry against the provisioned profile."""
    try:
        value = _validate_candidate(candidate)
        profile = load_protected_admin_profile(profile_path)
        verifier = profile["verifier"]
        if not isinstance(verifier, dict):
            return False
        iterations = _validate_iterations(verifier["iterations"])
        salt = bytes.fromhex(
            _validate_lowercase_hex(
                verifier["salt_hex"],
                byte_length=PROTECTED_ADMIN_SALT_BYTES,
                label="salt",
            )
        )
        digest = bytes.fromhex(
            _validate_lowercase_hex(
                verifier["digest_hex"],
                byte_length=PROTECTED_ADMIN_DIGEST_BYTES,
                label="digest",
            )
        )
        derived = hashlib.pbkdf2_hmac(
            "sha256",
            value.encode("utf-8"),
            salt,
            iterations,
            dklen=PROTECTED_ADMIN_DIGEST_BYTES,
        )
        return hmac.compare_digest(derived, digest)
    except (ValueError, TypeError, ProtectedAdminProfileError, OSError):
        return False


def canonical_operator_id(candidate: object) -> str:
    """Resolve a verified credential and leave ordinary names unchanged."""
    value = str(candidate or "").strip()
    if value in {PROTECTED_ADMIN_OPERATOR_ID, PROTECTED_ADMIN_DISPLAY_NAME}:
        return ""
    if is_protected_admin_code(value):
        return PROTECTED_ADMIN_OPERATOR_ID
    return value


def operator_role(candidate: object, default: str = "WORKER") -> str:
    """Resolve the role without authorizing canonical/display identity text."""
    value = str(candidate or "").strip()
    if is_protected_admin_code(value):
        return PROTECTED_ADMIN_ROLE
    return str(default or "WORKER").strip().upper() or "WORKER"


def persistent_operator_name(
    candidate: object,
    *,
    authenticated_credential_entry: bool = False,
) -> str:
    """Return a non-authorizing label safe for files, logs, and recovery state."""
    value = str(candidate or "").strip()
    if value in {PROTECTED_ADMIN_OPERATOR_ID, PROTECTED_ADMIN_DISPLAY_NAME}:
        return PROTECTED_ADMIN_DISPLAY_NAME
    if authenticated_credential_entry and is_protected_admin_candidate(value):
        return PROTECTED_ADMIN_DISPLAY_NAME
    return value


def sanitize_persistent_value(value: Any, *, _field_name: str = "") -> Any:
    """Sanitize canonical identities without treating business numbers as secrets."""
    if isinstance(value, dict):
        return {
            str(key): sanitize_persistent_value(
                item,
                _field_name=str(key).strip().casefold(),
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [
            sanitize_persistent_value(item, _field_name=_field_name)
            for item in value
        ]
    if isinstance(value, tuple):
        return [
            sanitize_persistent_value(item, _field_name=_field_name)
            for item in value
        ]
    if isinstance(value, str):
        return value.replace(
            PROTECTED_ADMIN_OPERATOR_ID,
            PROTECTED_ADMIN_DISPLAY_NAME,
        )
    return value


def redact_protected_admin_identity(
    value: object,
    replacement: str = "[protected-admin]",
) -> str:
    """Redact only the canonical internal identity in untrusted text."""
    text = str(value or "")
    return text.replace(PROTECTED_ADMIN_OPERATOR_ID, replacement)


def redact_authenticated_credential_entry(
    value: object,
    *,
    authenticated: bool,
    replacement: str = "[protected-admin]",
) -> str:
    """Redact a credential entry only after its caller authenticated it."""
    text = str(value or "")
    if authenticated:
        return replacement
    return redact_protected_admin_identity(text, replacement)


def redact_protected_admin_code(
    value: object,
    replacement: str = "[protected-admin]",
    *,
    authenticated_credential_entry: bool = False,
) -> str:
    """Compatibility wrapper with opt-in credential-entry redaction only."""
    return redact_authenticated_credential_entry(
        value,
        authenticated=authenticated_credential_entry,
        replacement=replacement,
    )

"""Transitional detached-signature contract for release-controlled payloads.

New signatures use ES256 through the vendored Windows CNG P-256 wrapper.
Legacy Ed25519 verification is intentionally source-only so the finite bridge
window does not pull ``cryptography``/``cffi`` into frozen applications.
"""

from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any, Mapping

from .cng_p256 import verify_es256


LEGACY_ED25519_SIGNATURE_VERSION = "ed25519-v1"
ES256_SIGNATURE_VERSION = "es256-v1"
SUPPORTED_SIGNATURE_VERSIONS = frozenset(
    {LEGACY_ED25519_SIGNATURE_VERSION, ES256_SIGNATURE_VERSION}
)
SIGNATURE_BYTES = 64
SIGNATURE_METADATA_SUFFIX = ".json"
_MAX_KEY_CONFIG_CHARS = 8_192
_MAX_SIGNATURE_METADATA_BYTES = 1_024
_ED25519_PUBLIC_KEY_HEX = re.compile(r"[0-9A-Fa-f]{64}\Z")


class ReleaseSignatureError(ValueError):
    """Raised when a release signature contract or verification fails."""


def resolve_signature_version(value: Any, *, allow_legacy_missing: bool = False) -> str:
    """Validate an explicit version, with a bounded legacy-missing bridge."""

    if value is None and allow_legacy_missing:
        return LEGACY_ED25519_SIGNATURE_VERSION
    if not isinstance(value, str) or value not in SUPPORTED_SIGNATURE_VERSIONS:
        raise ReleaseSignatureError("unsupported or missing signature_version")
    return value


def manifest_signature_version(manifest: Mapping[str, Any]) -> str:
    if not isinstance(manifest, Mapping):
        raise ReleaseSignatureError("signed manifest must be an object")
    return resolve_signature_version(
        manifest.get("signature_version"),
        allow_legacy_missing="signature_version" not in manifest,
    )


def signature_metadata_path(signature_path: Path) -> Path:
    return signature_path.with_name(signature_path.name + SIGNATURE_METADATA_SUFFIX)


def detached_signature_version(signature_path: Path) -> str:
    """Read ``<payload>.sig.json``; absence means only the legacy bridge."""

    metadata_path = signature_metadata_path(signature_path)
    if not metadata_path.exists():
        return LEGACY_ED25519_SIGNATURE_VERSION
    try:
        if not metadata_path.is_file() or metadata_path.stat().st_size > _MAX_SIGNATURE_METADATA_BYTES:
            raise ReleaseSignatureError("signature metadata is missing, non-regular, or oversized")
        raw = metadata_path.read_bytes()
    except OSError as exc:
        raise ReleaseSignatureError("signature metadata is unreadable") from exc
    return signature_metadata_version_bytes(raw)


def signature_metadata_version_bytes(raw: bytes) -> str:
    """Parse bounded, exact signature-version metadata received from any transport."""

    if not isinstance(raw, bytes) or len(raw) > _MAX_SIGNATURE_METADATA_BYTES:
        raise ReleaseSignatureError("signature metadata must be bounded bytes")
    try:
        document = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReleaseSignatureError("signature metadata is unreadable") from exc
    if not isinstance(document, dict) or set(document) != {"signature_version"}:
        raise ReleaseSignatureError("signature metadata must contain only signature_version")
    return resolve_signature_version(document["signature_version"])


def signature_metadata_bytes(signature_version: str) -> bytes:
    version = resolve_signature_version(signature_version)
    return (
        json.dumps(
            {"signature_version": version},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        + "\n"
    ).encode("ascii")


def _key_document(public_key_config: str | Mapping[str, Any]) -> Mapping[str, Any] | str:
    if isinstance(public_key_config, Mapping):
        return public_key_config
    if not isinstance(public_key_config, str):
        raise ReleaseSignatureError("release public key configuration must be text or an object")
    text = public_key_config.strip()
    if not text or len(text) > _MAX_KEY_CONFIG_CHARS:
        raise ReleaseSignatureError("release public key configuration is empty or oversized")
    if _ED25519_PUBLIC_KEY_HEX.fullmatch(text):
        return text
    try:
        document = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ReleaseSignatureError("release public key configuration is malformed") from exc
    if not isinstance(document, dict):
        raise ReleaseSignatureError("release public key configuration must be an object")
    return document


def _key_for_version(
    public_key_config: str | Mapping[str, Any],
    signature_version: str,
) -> str | Mapping[str, Any]:
    document = _key_document(public_key_config)
    if isinstance(document, str):
        if signature_version != LEGACY_ED25519_SIGNATURE_VERSION:
            raise ReleaseSignatureError("ES256 public JWK is not configured")
        return document

    if set(document) == {"kty", "crv", "x", "y"}:
        if signature_version != ES256_SIGNATURE_VERSION:
            raise ReleaseSignatureError("legacy Ed25519 public key is not configured")
        return document

    if not set(document).issubset(SUPPORTED_SIGNATURE_VERSIONS):
        raise ReleaseSignatureError("release public key bundle contains unsupported fields")
    if signature_version not in document:
        raise ReleaseSignatureError(f"public key for {signature_version} is not configured")
    selected = document[signature_version]
    if signature_version == LEGACY_ED25519_SIGNATURE_VERSION:
        if not isinstance(selected, str) or _ED25519_PUBLIC_KEY_HEX.fullmatch(selected) is None:
            raise ReleaseSignatureError("legacy Ed25519 public key must be 32-byte hex")
        return selected
    if not isinstance(selected, Mapping):
        raise ReleaseSignatureError("ES256 public key must be a public JWK object")
    return selected


def validate_public_key_config(public_key_config: str | Mapping[str, Any]) -> None:
    """Validate a legacy key, ES256 JWK, or bounded transition key bundle."""

    document = _key_document(public_key_config)
    if isinstance(document, str):
        bytes.fromhex(document)
        return
    if set(document) == {"kty", "crv", "x", "y"}:
        from .cng_p256 import normalize_public_jwk

        normalize_public_jwk(document, error_code="RELEASE_ES256_KEY_INVALID")
        return
    if not document or not set(document).issubset(SUPPORTED_SIGNATURE_VERSIONS):
        raise ReleaseSignatureError("release public key bundle is empty or unsupported")
    for version in document:
        selected = _key_for_version(document, version)
        if version == ES256_SIGNATURE_VERSION:
            from .cng_p256 import normalize_public_jwk

            assert isinstance(selected, Mapping)
            normalize_public_jwk(selected, error_code="RELEASE_ES256_KEY_INVALID")


# Strict RFC 8032-compatible Ed25519 verification for the finite bridge only.
# Inputs are public, so Python big-integer timing does not expose signing keys.
_Q = 2**255 - 19
_L = 2**252 + 27742317777372353535851937790883648493
_D = (-121665 * pow(121666, _Q - 2, _Q)) % _Q
_SQRT_M1 = pow(2, (_Q - 1) // 4, _Q)
_IDENTITY = (0, 1, 1, 0)


def _recover_x(y: int, sign: int) -> int:
    xx = ((y * y - 1) * pow((_D * y * y + 1) % _Q, _Q - 2, _Q)) % _Q
    x = pow(xx, (_Q + 3) // 8, _Q)
    if (x * x - xx) % _Q:
        x = (x * _SQRT_M1) % _Q
    if (x * x - xx) % _Q:
        raise ReleaseSignatureError("legacy Ed25519 point is invalid")
    if (x & 1) != sign:
        x = _Q - x
    if x == 0 and sign:
        raise ReleaseSignatureError("legacy Ed25519 point encoding is non-canonical")
    return x


def _point_add(p: tuple[int, int, int, int], q: tuple[int, int, int, int]) -> tuple[int, int, int, int]:
    x1, y1, z1, t1 = p
    x2, y2, z2, t2 = q
    a = ((y1 - x1) * (y2 - x2)) % _Q
    b = ((y1 + x1) * (y2 + x2)) % _Q
    c = (2 * _D * t1 * t2) % _Q
    d = (2 * z1 * z2) % _Q
    e = (b - a) % _Q
    f = (d - c) % _Q
    g = (d + c) % _Q
    h = (b + a) % _Q
    return (e * f % _Q, g * h % _Q, f * g % _Q, e * h % _Q)


def _point_double(p: tuple[int, int, int, int]) -> tuple[int, int, int, int]:
    x, y, z, _t = p
    a = x * x % _Q
    b = y * y % _Q
    c = 2 * z * z % _Q
    d = (-a) % _Q
    e = ((x + y) * (x + y) - a - b) % _Q
    g = (d + b) % _Q
    f = (g - c) % _Q
    h = (d - b) % _Q
    return (e * f % _Q, g * h % _Q, f * g % _Q, e * h % _Q)


def _scalar_mult(scalar: int, point: tuple[int, int, int, int]) -> tuple[int, int, int, int]:
    result = _IDENTITY
    addend = point
    value = scalar
    while value:
        if value & 1:
            result = _point_add(result, addend)
        addend = _point_double(addend)
        value >>= 1
    return result


def _point_equal(p: tuple[int, int, int, int], q: tuple[int, int, int, int]) -> bool:
    return (p[0] * q[2] - q[0] * p[2]) % _Q == 0 and (p[1] * q[2] - q[1] * p[2]) % _Q == 0


def _decode_point(encoded: bytes) -> tuple[int, int, int, int]:
    if len(encoded) != 32:
        raise ReleaseSignatureError("legacy Ed25519 point must contain 32 bytes")
    value = int.from_bytes(encoded, "little")
    sign = value >> 255
    y = value & ((1 << 255) - 1)
    if y >= _Q:
        raise ReleaseSignatureError("legacy Ed25519 point encoding is non-canonical")
    x = _recover_x(y, sign)
    point = (x, y, 1, x * y % _Q)
    if _point_equal(point, _IDENTITY) or not _point_equal(_scalar_mult(_L, point), _IDENTITY):
        raise ReleaseSignatureError("legacy Ed25519 point is not in the prime-order subgroup")
    return point


_BASE_Y = 4 * pow(5, _Q - 2, _Q) % _Q
_BASE_X = _recover_x(_BASE_Y, 0)
_BASE_POINT = (_BASE_X, _BASE_Y, 1, _BASE_X * _BASE_Y % _Q)


def _verify_legacy_ed25519(payload: bytes, signature: bytes, public_key: bytes) -> None:
    import hashlib

    if len(signature) != SIGNATURE_BYTES or len(public_key) != 32:
        raise ReleaseSignatureError("legacy Ed25519 key/signature length is invalid")
    scalar = int.from_bytes(signature[32:], "little")
    if scalar >= _L:
        raise ReleaseSignatureError("legacy Ed25519 signature scalar is non-canonical")
    public_point = _decode_point(public_key)
    r_point = _decode_point(signature[:32])
    challenge = int.from_bytes(
        hashlib.sha512(signature[:32] + public_key + payload).digest(), "little"
    ) % _L
    if not _point_equal(
        _scalar_mult(scalar, _BASE_POINT),
        _point_add(r_point, _scalar_mult(challenge, public_point)),
    ):
        raise ReleaseSignatureError("legacy Ed25519 signature is invalid")


def verify_release_signature(
    payload: bytes,
    signature: bytes,
    public_key_config: str | Mapping[str, Any],
    signature_version: str,
) -> None:
    """Verify a detached release signature, selecting exactly one algorithm."""

    version = resolve_signature_version(signature_version)
    if not isinstance(payload, bytes) or not isinstance(signature, bytes):
        raise ReleaseSignatureError("release payload and signature must be bytes")
    if len(signature) != SIGNATURE_BYTES:
        raise ReleaseSignatureError("release signature must contain exactly 64 bytes")
    selected = _key_for_version(public_key_config, version)
    if version == LEGACY_ED25519_SIGNATURE_VERSION:
        assert isinstance(selected, str)
        _verify_legacy_ed25519(payload, signature, bytes.fromhex(selected))
        return
    assert isinstance(selected, Mapping)
    verify_es256(
        payload,
        signature,
        selected,
        key_error_code="RELEASE_ES256_KEY_INVALID",
        signature_error_code="RELEASE_ES256_SIGNATURE_INVALID",
        require_low_s=True,
    )


__all__ = [
    "ES256_SIGNATURE_VERSION",
    "LEGACY_ED25519_SIGNATURE_VERSION",
    "ReleaseSignatureError",
    "SIGNATURE_BYTES",
    "SUPPORTED_SIGNATURE_VERSIONS",
    "detached_signature_version",
    "manifest_signature_version",
    "resolve_signature_version",
    "signature_metadata_bytes",
    "signature_metadata_path",
    "signature_metadata_version_bytes",
    "validate_public_key_config",
    "verify_release_signature",
]

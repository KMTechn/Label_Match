from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import (
    decode_dss_signature,
    encode_dss_signature,
)

from kmtech_zero_pe import P256KeyPair, normalize_public_jwk, verify_es256
from producer_runtime_client import new_runtime_identity


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = json.loads(
    (ROOT / "tests" / "fixtures" / "es256-production-fixture.json").read_text(
        encoding="utf-8"
    )
)
P256_ORDER = int(
    "FFFFFFFF00000000FFFFFFFFFFFFFFFFBCE6FAADA7179E84F3B9CAC2FC632551",
    16,
)


def _decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _token_parts(token: str) -> tuple[bytes, bytes]:
    header, payload, signature = token.split(".")
    return f"{header}.{payload}".encode("ascii"), _decode(signature)


def _cryptography_verdict(signing_input: bytes, signature: bytes, jwk: dict) -> bool:
    if len(signature) != 64:
        return False
    r = int.from_bytes(signature[:32], "big")
    s = int.from_bytes(signature[32:], "big")
    if not 1 <= r < P256_ORDER or not 1 <= s <= P256_ORDER // 2:
        return False
    try:
        public_key = ec.EllipticCurvePublicNumbers(
            int.from_bytes(_decode(jwk["x"]), "big"),
            int.from_bytes(_decode(jwk["y"]), "big"),
            ec.SECP256R1(),
        ).public_key()
        public_key.verify(
            encode_dss_signature(r, s),
            signing_input,
            ec.ECDSA(hashes.SHA256()),
        )
    except (InvalidSignature, KeyError, TypeError, ValueError):
        return False
    return True


def _cng_verdict(signing_input: bytes, signature: bytes, jwk: dict) -> bool:
    try:
        verify_es256(signing_input, signature, jwk, require_low_s=True)
    except ValueError:
        return False
    return True


@pytest.mark.parametrize(
    ("token_name", "key_name"),
    (
        ("valid", "valid"),
        ("tampered_payload", "valid"),
        ("tampered_signature", "valid"),
        ("high_s_signature", "valid"),
        ("valid", "invalid_point"),
        ("valid", "short_coordinate"),
    ),
)
def test_seq259_production_fixture_matches_legacy_cryptography_verdict(
    token_name: str, key_name: str
) -> None:
    signing_input, signature = _token_parts(FIXTURE["tokens"][token_name])
    jwk = FIXTURE["keys"][key_name]

    assert _cng_verdict(signing_input, signature, jwk) == _cryptography_verdict(
        signing_input,
        signature,
        jwk,
    )


def test_cng_and_cryptography_cross_verify_fresh_low_s_signatures() -> None:
    message = b"Label_Match CNG migration parity vector v1"
    with P256KeyPair.generate() as cng_key:
        cng_signature = cng_key.sign_es256(message)
        cng_jwk = cng_key.public_jwk

    r = int.from_bytes(cng_signature[:32], "big")
    s = int.from_bytes(cng_signature[32:], "big")
    crypto_public = ec.EllipticCurvePublicNumbers(
        int.from_bytes(_decode(cng_jwk["x"]), "big"),
        int.from_bytes(_decode(cng_jwk["y"]), "big"),
        ec.SECP256R1(),
    ).public_key()
    crypto_public.verify(
        encode_dss_signature(r, s),
        message,
        ec.ECDSA(hashes.SHA256()),
    )

    crypto_private = ec.derive_private_key(7, ec.SECP256R1())
    der = crypto_private.sign(message, ec.ECDSA(hashes.SHA256()))
    crypto_r, crypto_s = decode_dss_signature(der)
    crypto_s = min(crypto_s, P256_ORDER - crypto_s)
    crypto_signature = crypto_r.to_bytes(32, "big") + crypto_s.to_bytes(32, "big")
    numbers = crypto_private.public_key().public_numbers()
    crypto_jwk = {
        "kty": "EC",
        "crv": "P-256",
        "x": base64.urlsafe_b64encode(numbers.x.to_bytes(32, "big"))
        .rstrip(b"=")
        .decode("ascii"),
        "y": base64.urlsafe_b64encode(numbers.y.to_bytes(32, "big"))
        .rstrip(b"=")
        .decode("ascii"),
    }
    verify_es256(message, crypto_signature, crypto_jwk, require_low_s=True)


def test_runtime_identity_is_a_cryptography_compatible_p256_point() -> None:
    runtime_id, jwk = new_runtime_identity()
    normalized = normalize_public_jwk(jwk)

    assert runtime_id.startswith("runtime-")
    assert normalized == jwk
    ec.EllipticCurvePublicNumbers(
        int.from_bytes(_decode(jwk["x"]), "big"),
        int.from_bytes(_decode(jwk["y"]), "big"),
        ec.SECP256R1(),
    ).public_key()

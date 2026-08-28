from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from kmtech_zero_pe.release_signature import (
    LEGACY_ED25519_SIGNATURE_VERSION,
    ReleaseSignatureError,
    verify_release_signature,
)


ROOT = Path(__file__).resolve().parents[1]
RFC_FIXTURE = json.loads(
    (ROOT / "tests" / "fixtures" / "rfc8032-ed25519.json").read_text(
        encoding="utf-8"
    )
)
RFC_VECTORS = RFC_FIXTURE["vectors"]
ED25519_ORDER = 2**252 + 27742317777372353535851937790883648493


def _parts(vector: dict[str, str]) -> tuple[bytes, bytes, bytes]:
    return (
        bytes.fromhex(vector["public_key"]),
        bytes.fromhex(vector["message"]),
        bytes.fromhex(vector["signature"]),
    )


def _pure_accepts(public_key: bytes, message: bytes, signature: bytes) -> bool:
    try:
        verify_release_signature(
            message,
            signature,
            public_key.hex(),
            LEGACY_ED25519_SIGNATURE_VERSION,
        )
    except ValueError:
        return False
    return True


def _cryptography_accepts(
    public_key: bytes, message: bytes, signature: bytes
) -> bool:
    try:
        Ed25519PublicKey.from_public_bytes(public_key).verify(signature, message)
    except (InvalidSignature, ValueError):
        return False
    return True


@pytest.mark.parametrize("vector", RFC_VECTORS, ids=lambda row: row["name"])
def test_rfc8032_section_7_1_vectors_and_cryptography_differential(
    vector: dict[str, str],
) -> None:
    public_key, message, signature = _parts(vector)

    assert _pure_accepts(public_key, message, signature) is True
    assert _cryptography_accepts(public_key, message, signature) is True


def test_legacy_verifier_and_cryptography_reject_basic_tampering() -> None:
    public_key, message, signature = _parts(RFC_VECTORS[0])
    wrong_public_key = _parts(RFC_VECTORS[1])[0]
    tampered_signature = bytes([signature[0] ^ 1]) + signature[1:]
    cases = (
        (wrong_public_key, message, signature),
        (public_key, message, tampered_signature),
        (public_key, message + b"tampered", signature),
        (public_key, message, bytes(64)),
    )

    for candidate_key, candidate_message, candidate_signature in cases:
        assert _pure_accepts(
            candidate_key, candidate_message, candidate_signature
        ) is False
        assert _cryptography_accepts(
            candidate_key, candidate_message, candidate_signature
        ) is False


def test_legacy_verifier_rejects_noncanonical_scalar_and_small_order_points() -> None:
    public_key, message, signature = _parts(RFC_VECTORS[0])
    noncanonical_s = signature[:32] + ED25519_ORDER.to_bytes(32, "little")
    identity = b"\x01" + bytes(31)
    order_four = bytes(32)

    assert _pure_accepts(public_key, message, noncanonical_s) is False
    assert _pure_accepts(identity, message, signature) is False
    assert _pure_accepts(order_four, message, signature) is False
    assert _pure_accepts(public_key, message, identity + signature[32:]) is False


@pytest.mark.parametrize(
    ("public_key", "signature"),
    (
        (bytes(31), bytes(64)),
        (bytes(33), bytes(64)),
        (bytes(32), bytes(63)),
        (bytes(32), bytes(65)),
        ((2**255 - 19).to_bytes(32, "little"), bytes(64)),
    ),
)
def test_legacy_verifier_fails_closed_on_malformed_inputs(
    public_key: bytes, signature: bytes
) -> None:
    with pytest.raises(ReleaseSignatureError):
        verify_release_signature(
            b"message",
            signature,
            public_key.hex(),
            LEGACY_ED25519_SIGNATURE_VERSION,
        )


def test_legacy_bridge_is_verification_only_and_stdlib_only() -> None:
    source_path = ROOT / "kmtech_zero_pe" / "release_signature.py"
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    imported_roots: set[str] = set()
    functions: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            imported_roots.add(node.module.split(".", 1)[0])
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            functions.add(node.name)

    assert imported_roots <= {"__future__", "hashlib", "json", "pathlib", "re", "typing"}
    assert not any("sign" in name and "signature" not in name for name in functions)

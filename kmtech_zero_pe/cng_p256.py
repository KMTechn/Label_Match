"""Windows CNG P-256/ES256 primitives with caller-owned error taxonomy.

The applications keep their existing JWS claim validation and error classes.
Callers may pass their current ``_error(code, message)`` factory so invalid
keys/signatures retain the exact public exception type and code.
"""

from __future__ import annotations

import base64
import ctypes
from ctypes import wintypes
from dataclasses import dataclass
import hashlib
import json
import math
import os
import struct
from typing import Any, Callable, Mapping


P256_ORDER = int("FFFFFFFF00000000FFFFFFFFFFFFFFFFBCE6FAADA7179E84F3B9CAC2FC632551", 16)
P256_BYTES = 32
BCRYPT_ECDSA_PUBLIC_P256_MAGIC = 0x31534345
BCRYPT_ECDSA_PRIVATE_P256_MAGIC = 0x32534345
BCRYPT_ECDSA_P256_ALGORITHM = "ECDSA_P256"
BCRYPT_ECCPUBLIC_BLOB = "ECCPUBLICBLOB"
BCRYPT_ECCPRIVATE_BLOB = "ECCPRIVATEBLOB"
STATUS_SUCCESS = 0
STATUS_INVALID_SIGNATURE = 0xC000A000


class CngError(RuntimeError):
    def __init__(self, operation: str, status: int) -> None:
        self.operation = str(operation)
        self.status = int(status) & 0xFFFFFFFF
        super().__init__(f"{operation} failed with NTSTATUS 0x{self.status:08X}")


class P256ContractError(ValueError):
    def __init__(self, code: str, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.code = str(code)
        self.message = str(message)
        self.status = None if status is None else int(status) & 0xFFFFFFFF


class P256KeyError(P256ContractError):
    pass


class P256SignatureError(P256ContractError):
    pass


class P256JwsError(P256ContractError):
    pass


ErrorFactory = Callable[[str, str], BaseException]


@dataclass(frozen=True)
class JwsErrorCodes:
    jws: str = "OPERATION_LEASE_JWS_INVALID"
    key: str = "OPERATION_LEASE_KEY_INVALID"
    signature: str = "OPERATION_LEASE_SIGNATURE_INVALID"


if os.name == "nt":
    _bcrypt = ctypes.WinDLL("bcrypt.dll", use_last_error=True)
    _bcrypt.BCryptOpenAlgorithmProvider.argtypes = (
        ctypes.POINTER(wintypes.HANDLE),
        wintypes.LPCWSTR,
        wintypes.LPCWSTR,
        wintypes.ULONG,
    )
    _bcrypt.BCryptOpenAlgorithmProvider.restype = wintypes.LONG
    _bcrypt.BCryptCloseAlgorithmProvider.argtypes = (wintypes.HANDLE, wintypes.ULONG)
    _bcrypt.BCryptCloseAlgorithmProvider.restype = wintypes.LONG
    _bcrypt.BCryptImportKeyPair.argtypes = (
        wintypes.HANDLE,
        wintypes.HANDLE,
        wintypes.LPCWSTR,
        ctypes.POINTER(wintypes.HANDLE),
        ctypes.c_void_p,
        wintypes.ULONG,
        wintypes.ULONG,
    )
    _bcrypt.BCryptImportKeyPair.restype = wintypes.LONG
    _bcrypt.BCryptGenerateKeyPair.argtypes = (
        wintypes.HANDLE,
        ctypes.POINTER(wintypes.HANDLE),
        wintypes.ULONG,
        wintypes.ULONG,
    )
    _bcrypt.BCryptGenerateKeyPair.restype = wintypes.LONG
    _bcrypt.BCryptFinalizeKeyPair.argtypes = (wintypes.HANDLE, wintypes.ULONG)
    _bcrypt.BCryptFinalizeKeyPair.restype = wintypes.LONG
    _bcrypt.BCryptExportKey.argtypes = (
        wintypes.HANDLE,
        wintypes.HANDLE,
        wintypes.LPCWSTR,
        ctypes.c_void_p,
        wintypes.ULONG,
        ctypes.POINTER(wintypes.ULONG),
        wintypes.ULONG,
    )
    _bcrypt.BCryptExportKey.restype = wintypes.LONG
    _bcrypt.BCryptDestroyKey.argtypes = (wintypes.HANDLE,)
    _bcrypt.BCryptDestroyKey.restype = wintypes.LONG
    _bcrypt.BCryptSignHash.argtypes = (
        wintypes.HANDLE,
        ctypes.c_void_p,
        ctypes.c_void_p,
        wintypes.ULONG,
        ctypes.c_void_p,
        wintypes.ULONG,
        ctypes.POINTER(wintypes.ULONG),
        wintypes.ULONG,
    )
    _bcrypt.BCryptSignHash.restype = wintypes.LONG
    _bcrypt.BCryptVerifySignature.argtypes = (
        wintypes.HANDLE,
        ctypes.c_void_p,
        ctypes.c_void_p,
        wintypes.ULONG,
        ctypes.c_void_p,
        wintypes.ULONG,
        wintypes.ULONG,
    )
    _bcrypt.BCryptVerifySignature.restype = wintypes.LONG


def ntstatus(value: int) -> int:
    return int(value) & 0xFFFFFFFF


def _require_status(value: int, operation: str) -> None:
    if ntstatus(value) != STATUS_SUCCESS:
        raise CngError(operation, value)


def b64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def b64url_decode(value: Any, *, field: str, maximum: int = 131_072) -> bytes:
    if not isinstance(value, str) or not value or "=" in value or len(value) > maximum:
        raise P256JwsError("P256_BASE64URL_INVALID", f"{field} is not canonical base64url")
    try:
        raw = base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
    except (ValueError, base64.binascii.Error) as exc:
        raise P256JwsError("P256_BASE64URL_INVALID", f"{field} is not base64url") from exc
    if b64url_encode(raw) != value:
        raise P256JwsError("P256_BASE64URL_INVALID", f"{field} is not canonical base64url")
    return raw


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _validate_json_complexity(value: Any, code: str, *, maximum_depth: int = 64, maximum_nodes: int = 10_000) -> None:
    stack = [(value, 0)]
    nodes = 0
    while stack:
        current, depth = stack.pop()
        nodes += 1
        if nodes > maximum_nodes or depth > maximum_depth:
            raise P256JwsError(code, "lease protected content is too complex")
        if isinstance(current, dict):
            stack.extend((child, depth + 1) for child in current.values())
        elif isinstance(current, list):
            stack.extend((child, depth + 1) for child in current)
        elif current is None or isinstance(current, (str, bool)):
            continue
        elif isinstance(current, int):
            if current.bit_length() > 4096:
                raise P256JwsError(code, "lease protected integer is too large")
        elif isinstance(current, float):
            if not math.isfinite(current):
                raise P256JwsError(code, "lease protected number is not finite")
        else:
            raise P256JwsError(code, "lease protected content contains an unsupported JSON value")


def _open_algorithm():
    if os.name != "nt":
        raise OSError("CNG P-256 requires Windows")
    handle = wintypes.HANDLE()
    _require_status(
        _bcrypt.BCryptOpenAlgorithmProvider(ctypes.byref(handle), BCRYPT_ECDSA_P256_ALGORITHM, None, 0),
        "BCryptOpenAlgorithmProvider(ECDSA_P256)",
    )
    return handle


def _close_algorithm(handle) -> None:
    if handle:
        _require_status(_bcrypt.BCryptCloseAlgorithmProvider(handle, 0), "BCryptCloseAlgorithmProvider")


def _raise_mapped(factory: ErrorFactory | None, fallback: P256ContractError) -> None:
    if factory is None:
        raise fallback
    mapped = factory(fallback.code, fallback.message)
    if not isinstance(mapped, BaseException):
        raise TypeError("error_factory must return an exception")
    raise mapped from fallback


def _coordinate(value: Any, field: str, code: str) -> bytes:
    try:
        raw = b64url_decode(value, field=field, maximum=64)
    except P256JwsError as exc:
        raise P256KeyError(code, exc.message) from exc
    if len(raw) != P256_BYTES:
        raise P256KeyError(code, f"{field} must contain exactly 32 bytes")
    return raw


def _public_blob(value: Mapping[str, Any], code: str) -> tuple[dict[str, str], bytes]:
    if not isinstance(value, Mapping) or set(value) != {"kty", "crv", "x", "y"}:
        raise P256KeyError(code, "public JWK must contain exactly kty, crv, x, and y")
    if value.get("kty") != "EC" or value.get("crv") != "P-256":
        raise P256KeyError(code, "public JWK must be an EC P-256 key")
    x = _coordinate(value.get("x"), "public_jwk.x", code)
    y = _coordinate(value.get("y"), "public_jwk.y", code)
    normalized = {"kty": "EC", "crv": "P-256", "x": b64url_encode(x), "y": b64url_encode(y)}
    return normalized, struct.pack("<II", BCRYPT_ECDSA_PUBLIC_P256_MAGIC, P256_BYTES) + x + y


def _import_public(algorithm, value: Mapping[str, Any], code: str):
    normalized, blob = _public_blob(value, code)
    buffer = ctypes.create_string_buffer(blob)
    key = wintypes.HANDLE()
    status = _bcrypt.BCryptImportKeyPair(
        algorithm,
        None,
        BCRYPT_ECCPUBLIC_BLOB,
        ctypes.byref(key),
        buffer,
        len(blob),
        0,
    )
    if ntstatus(status) != STATUS_SUCCESS:
        raise P256KeyError(
            code,
            f"public JWK is not a point on P-256 (NTSTATUS 0x{ntstatus(status):08X})",
            status=status,
        )
    return normalized, key


def normalize_public_jwk(
    value: Mapping[str, Any],
    *,
    error_factory: ErrorFactory | None = None,
    error_code: str = "OPERATION_LEASE_KEY_INVALID",
) -> dict[str, str]:
    algorithm = None
    key = None
    try:
        algorithm = _open_algorithm()
        normalized, key = _import_public(algorithm, value, error_code)
        return normalized
    except P256KeyError as exc:
        _raise_mapped(error_factory, exc)
        raise AssertionError("unreachable")
    finally:
        if key:
            _bcrypt.BCryptDestroyKey(key)
        if algorithm:
            _close_algorithm(algorithm)


def jwk_thumbprint(
    value: Mapping[str, Any],
    *,
    error_factory: ErrorFactory | None = None,
    error_code: str = "OPERATION_LEASE_KEY_INVALID",
) -> str:
    normalized = normalize_public_jwk(value, error_factory=error_factory, error_code=error_code)
    return b64url_encode(hashlib.sha256(canonical_json_bytes(normalized)).digest())


def _validate_raw_signature(signature: bytes, code: str, *, require_low_s: bool) -> bytes:
    if not isinstance(signature, bytes) or len(signature) != 2 * P256_BYTES:
        raise P256SignatureError(code, "ES256 signature must contain exactly 64 raw bytes")
    r = int.from_bytes(signature[:P256_BYTES], "big")
    s = int.from_bytes(signature[P256_BYTES:], "big")
    if not 1 <= r < P256_ORDER or not 1 <= s < P256_ORDER:
        raise P256SignatureError(code, "ES256 signature components are out of range")
    if require_low_s and s > P256_ORDER // 2:
        raise P256SignatureError(code, "ES256 signature must use low-S form")
    return signature


def verify_es256(
    signing_input: bytes,
    signature: bytes,
    public_jwk: Mapping[str, Any],
    *,
    error_factory: ErrorFactory | None = None,
    key_error_code: str = "OPERATION_LEASE_KEY_INVALID",
    signature_error_code: str = "OPERATION_LEASE_SIGNATURE_INVALID",
    require_low_s: bool = True,
) -> None:
    algorithm = None
    key = None
    try:
        raw_signature = _validate_raw_signature(signature, signature_error_code, require_low_s=require_low_s)
        algorithm = _open_algorithm()
        _, key = _import_public(algorithm, public_jwk, key_error_code)
        digest = hashlib.sha256(bytes(signing_input)).digest()
        digest_buffer = ctypes.create_string_buffer(digest)
        signature_buffer = ctypes.create_string_buffer(raw_signature)
        status = _bcrypt.BCryptVerifySignature(
            key,
            None,
            digest_buffer,
            len(digest),
            signature_buffer,
            len(raw_signature),
            0,
        )
        if ntstatus(status) != STATUS_SUCCESS:
            raise P256SignatureError(
                signature_error_code,
                f"ES256 signature is invalid (NTSTATUS 0x{ntstatus(status):08X})",
                status=status,
            )
    except P256KeyError as exc:
        _raise_mapped(error_factory, exc)
    except P256SignatureError as exc:
        _raise_mapped(error_factory, exc)
    finally:
        if key:
            _bcrypt.BCryptDestroyKey(key)
        if algorithm:
            _close_algorithm(algorithm)


class P256KeyPair:
    """A closeable CNG private-key handle used for ES256 signing."""

    def __init__(self, algorithm, key) -> None:
        self._algorithm = algorithm
        self._key = key
        self._closed = False

    @classmethod
    def generate(cls) -> "P256KeyPair":
        algorithm = _open_algorithm()
        key = wintypes.HANDLE()
        try:
            _require_status(
                _bcrypt.BCryptGenerateKeyPair(algorithm, ctypes.byref(key), 256, 0),
                "BCryptGenerateKeyPair",
            )
            _require_status(_bcrypt.BCryptFinalizeKeyPair(key, 0), "BCryptFinalizeKeyPair")
            return cls(algorithm, key)
        except Exception:
            if key:
                _bcrypt.BCryptDestroyKey(key)
            _close_algorithm(algorithm)
            raise

    @classmethod
    def from_private_blob(cls, blob: bytes) -> "P256KeyPair":
        if not isinstance(blob, bytes) or len(blob) != 8 + (3 * P256_BYTES):
            raise P256KeyError("P256_PRIVATE_KEY_INVALID", "CNG private blob must contain 104 bytes")
        magic, key_bytes = struct.unpack("<II", blob[:8])
        if magic != BCRYPT_ECDSA_PRIVATE_P256_MAGIC or key_bytes != P256_BYTES:
            raise P256KeyError("P256_PRIVATE_KEY_INVALID", "CNG private blob header is invalid")
        algorithm = _open_algorithm()
        key = wintypes.HANDLE()
        buffer = ctypes.create_string_buffer(blob)
        try:
            status = _bcrypt.BCryptImportKeyPair(
                algorithm,
                None,
                BCRYPT_ECCPRIVATE_BLOB,
                ctypes.byref(key),
                buffer,
                len(blob),
                0,
            )
            if ntstatus(status) != STATUS_SUCCESS:
                raise P256KeyError(
                    "P256_PRIVATE_KEY_INVALID",
                    f"CNG private blob is invalid (NTSTATUS 0x{ntstatus(status):08X})",
                    status=status,
                )
            return cls(algorithm, key)
        except Exception:
            if key:
                _bcrypt.BCryptDestroyKey(key)
            _close_algorithm(algorithm)
            raise

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("P-256 key pair is closed")

    def __enter__(self) -> "P256KeyPair":
        self._ensure_open()
        return self

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        _bcrypt.BCryptDestroyKey(self._key)
        _close_algorithm(self._algorithm)
        self._closed = True

    def _export(self, blob_type: str) -> bytes:
        self._ensure_open()
        size = wintypes.ULONG(0)
        _require_status(
            _bcrypt.BCryptExportKey(self._key, None, blob_type, None, 0, ctypes.byref(size), 0),
            "BCryptExportKey(size)",
        )
        buffer = ctypes.create_string_buffer(size.value)
        _require_status(
            _bcrypt.BCryptExportKey(
                self._key,
                None,
                blob_type,
                buffer,
                size.value,
                ctypes.byref(size),
                0,
            ),
            "BCryptExportKey(data)",
        )
        return buffer.raw[: size.value]

    @property
    def public_jwk(self) -> dict[str, str]:
        blob = self._export(BCRYPT_ECCPUBLIC_BLOB)
        magic, key_bytes = struct.unpack("<II", blob[:8])
        if magic != BCRYPT_ECDSA_PUBLIC_P256_MAGIC or key_bytes != P256_BYTES or len(blob) != 72:
            raise CngError("BCryptExportKey(public header)", 0xC000000D)
        return {
            "kty": "EC",
            "crv": "P-256",
            "x": b64url_encode(blob[8:40]),
            "y": b64url_encode(blob[40:72]),
        }

    def export_private_blob(self) -> bytes:
        """Return a raw CNG ECCPRIVATEBLOB; callers must treat it as a secret."""

        blob = self._export(BCRYPT_ECCPRIVATE_BLOB)
        if len(blob) != 104 or struct.unpack("<II", blob[:8]) != (BCRYPT_ECDSA_PRIVATE_P256_MAGIC, P256_BYTES):
            raise CngError("BCryptExportKey(private header)", 0xC000000D)
        return blob

    def sign_digest(self, digest: bytes) -> bytes:
        self._ensure_open()
        if not isinstance(digest, bytes) or len(digest) != 32:
            raise ValueError("ES256 requires a 32-byte SHA-256 digest")
        digest_buffer = ctypes.create_string_buffer(digest)
        size = wintypes.ULONG(0)
        _require_status(
            _bcrypt.BCryptSignHash(self._key, None, digest_buffer, len(digest), None, 0, ctypes.byref(size), 0),
            "BCryptSignHash(size)",
        )
        buffer = ctypes.create_string_buffer(size.value)
        _require_status(
            _bcrypt.BCryptSignHash(
                self._key,
                None,
                digest_buffer,
                len(digest),
                buffer,
                size.value,
                ctypes.byref(size),
                0,
            ),
            "BCryptSignHash(data)",
        )
        raw = buffer.raw[: size.value]
        _validate_raw_signature(raw, "P256_SIGNATURE_INVALID", require_low_s=False)
        r = raw[:P256_BYTES]
        s = int.from_bytes(raw[P256_BYTES:], "big")
        if s > P256_ORDER // 2:
            s = P256_ORDER - s
        return r + s.to_bytes(P256_BYTES, "big")

    def sign_es256(self, signing_input: bytes) -> bytes:
        return self.sign_digest(hashlib.sha256(bytes(signing_input)).digest())


def generate_public_jwk() -> dict[str, str]:
    with P256KeyPair.generate() as key:
        return key.public_jwk


def verify_compact_jws(
    token: str,
    *,
    public_jwk: Mapping[str, Any],
    expected_kid: str,
    expected_type: str,
    error_factory: ErrorFactory | None = None,
    codes: JwsErrorCodes = JwsErrorCodes(),
    maximum_token_bytes: int = 131_072,
    maximum_payload_bytes: int = 65_536,
    validate_payload: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    try:
        try:
            token_size = len(token.encode("utf-8")) if isinstance(token, str) else 0
        except UnicodeEncodeError as exc:
            raise P256JwsError(codes.jws, "lease token is not valid Unicode") from exc
        if (
            not isinstance(token, str)
            or not token
            or token_size > maximum_token_bytes
            or token.count(".") != 2
        ):
            raise P256JwsError(codes.jws, "lease token is invalid")
        header_part, payload_part, signature_part = token.split(".")
        try:
            header_raw = b64url_decode(header_part, field="protected header", maximum=2_048)
            payload_raw = b64url_decode(payload_part, field="lease payload", maximum=maximum_payload_bytes * 2)
        except P256JwsError as exc:
            raise P256JwsError(codes.jws, exc.message) from exc
        if len(payload_raw) > maximum_payload_bytes:
            raise P256JwsError(codes.jws, "lease payload is too large")
        try:
            header = json.loads(header_raw.decode("utf-8"))
            payload = json.loads(payload_raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as exc:
            raise P256JwsError(codes.jws, "lease JWS JSON is invalid") from exc
        _validate_json_complexity(header, codes.jws)
        _validate_json_complexity(payload, codes.jws)
        try:
            canonical_header = canonical_json_bytes(header)
            canonical_payload = canonical_json_bytes(payload)
        except (TypeError, ValueError, UnicodeEncodeError, RecursionError) as exc:
            raise P256JwsError(codes.jws, "lease protected content is not canonical JSON") from exc
        expected_header = {"alg": "ES256", "kid": str(expected_kid), "typ": str(expected_type)}
        if (
            header != expected_header
            or canonical_header != header_raw
            or not isinstance(payload, dict)
            or canonical_payload != payload_raw
        ):
            raise P256JwsError(codes.jws, "lease protected content is not canonical JSON")
        try:
            signature = b64url_decode(signature_part, field="signature", maximum=128)
        except P256JwsError as exc:
            raise P256SignatureError(codes.signature, exc.message) from exc
        verify_es256(
            f"{header_part}.{payload_part}".encode("ascii"),
            signature,
            public_jwk,
            error_factory=error_factory,
            key_error_code=codes.key,
            signature_error_code=codes.signature,
            require_low_s=True,
        )
        return validate_payload(payload) if validate_payload is not None else payload
    except P256JwsError as exc:
        _raise_mapped(error_factory, exc)
    except P256SignatureError as exc:
        _raise_mapped(error_factory, exc)
    raise AssertionError("unreachable")


def sign_compact_jws(
    payload: Mapping[str, Any],
    *,
    kid: str,
    typ: str,
    key: P256KeyPair,
) -> str:
    header_part = b64url_encode(canonical_json_bytes({"alg": "ES256", "kid": str(kid), "typ": str(typ)}))
    payload_part = b64url_encode(canonical_json_bytes(dict(payload)))
    signing_input = f"{header_part}.{payload_part}".encode("ascii")
    signature_part = b64url_encode(key.sign_es256(signing_input))
    return f"{header_part}.{payload_part}.{signature_part}"


__all__ = [
    "CngError",
    "JwsErrorCodes",
    "P256ContractError",
    "P256JwsError",
    "P256KeyError",
    "P256KeyPair",
    "P256SignatureError",
    "b64url_decode",
    "b64url_encode",
    "canonical_json_bytes",
    "generate_public_jwk",
    "jwk_thumbprint",
    "normalize_public_jwk",
    "sign_compact_jws",
    "verify_compact_jws",
    "verify_es256",
]

"""Pinned verification and durable state for server-signed operation leases.

The server is the only signer.  Label_Match stores public keys and signed
artifacts only, verifies every terminal/source snapshot binding fail closed,
and keeps transport claiming separate from the business lease state.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import secrets
import sqlite3
import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature


PROGRAM = "Label_Match"
OPERATION = "CREATE_PACKAGE"
JWS_ALGORITHM = "ES256"
JWS_TYPE = "terminal-operation-lease+jws"
LEASE_CONTRACT_VERSION = "terminal-operation-lease-v1"
ARTIFACT_CONTRACT_VERSION = "terminal-operation-lease-artifact-v1"
KEYRING_CONTRACT_VERSION = "terminal-operation-lease-keyring-v1"
STORE_CONTRACT_VERSION = "terminal-operation-lease-store-v1"

MAX_COMPACT_JWS_BYTES = 32_768
MAX_JWS_PAYLOAD_BYTES = 24_576
MAX_KEYRING_BYTES = 32_768
MAX_OPERATION_SNAPSHOT_BYTES = 512_000
MAX_KEYS = 8
MAX_EXPECTED_VERSIONS = 128
MAX_LEASE_SECONDS = 24 * 60 * 60

_P256_BYTES = 32
_P256_ORDER = int(
    "FFFFFFFF00000000FFFFFFFFFFFFFFFFBCE6FAADA7179E84F3B9CAC2FC632551",
    16,
)
_B64_RE = re.compile(r"[A-Za-z0-9_-]+")
_HASH_RE = re.compile(r"[0-9a-f]{64}")
_UTC_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")

LEASE_PAYLOAD_KEYS = frozenset(
    {
        "contract_version",
        "lease_id",
        "site_id",
        "program",
        "device_id",
        "source_host_id",
        "authority_scope_id",
        "ledger_plane",
        "plane_epoch",
        "operation",
        "resource_id",
        "physical_label_id",
        "physical_qr_sha256",
        "item_id",
        "quantity",
        "member_count",
        "membership_hash",
        "expected_versions",
        "issued_at",
        "expires_at",
        "fence",
        "snapshot_hash",
    }
)
LEASE_BINDING_KEYS = frozenset(
    {
        "program",
        "device_id",
        "source_host_id",
        "authority_scope_id",
        "ledger_plane",
        "plane_epoch",
        "operation",
        "resource_id",
        "physical_label_id",
        "physical_qr_sha256",
        "item_id",
        "quantity",
        "member_count",
        "membership_hash",
        "expected_versions",
    }
)
KEYRING_KEYS = frozenset(
    {"contract_version", "site_id", "current_kid", "keys"}
)
KEYRING_ENTRY_KEYS = frozenset(
    {"kid", "status", "public_jwk", "thumbprint"}
)
PUBLIC_JWK_KEYS = frozenset({"kty", "crv", "x", "y"})


class OperationLeaseError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = str(code)
        self.message = str(message)


def _error(code: str, message: str) -> OperationLeaseError:
    return OperationLeaseError(code, message)


def canonical_json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise _error(
            "OPERATION_LEASE_CANONICAL_JSON_INVALID",
            "value cannot be represented as canonical JSON",
        ) from exc


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def physical_qr_sha256(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise _error(
            "OPERATION_LEASE_BINDING_INVALID", "physical QR is required"
        )
    if len(value.encode("utf-8")) > 8_192:
        raise _error(
            "OPERATION_LEASE_BINDING_INVALID", "physical QR exceeds its bound"
        )
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def issue_request_fingerprint(
    *,
    program: str,
    device_id: str,
    source_host_id: str,
    authority_scope_id: str,
    operation: str,
    physical_qr_hash: str,
) -> str:
    """Hash the exact server issue identity without retaining the physical QR."""

    identity = {
        "program": _bounded_text(program, field="program", maximum=64),
        "device_id": _bounded_text(device_id, field="device_id"),
        "source_host_id": _bounded_text(
            source_host_id, field="source_host_id"
        ),
        "authority_scope_id": _bounded_text(
            authority_scope_id, field="authority_scope_id"
        ),
        "operation": _bounded_text(operation, field="operation", maximum=64),
        "physical_qr_sha256": _hash64(
            physical_qr_hash, field="physical_qr_sha256"
        ),
    }
    if identity["program"] != PROGRAM or identity["operation"] != OPERATION:
        raise _error(
            "OPERATION_LEASE_BINDING_MISMATCH",
            "issue identity is not the Label_Match packaging operation",
        )
    return canonical_sha256(identity)


def _bounded_text(value: Any, *, field: str, maximum: int = 256) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value.encode("utf-8")) > maximum
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise _error(
            "OPERATION_LEASE_PAYLOAD_INVALID",
            f"{field} is empty, non-canonical, or exceeds its bound",
        )
    return value


def _hash64(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or _HASH_RE.fullmatch(value) is None:
        raise _error(
            "OPERATION_LEASE_PAYLOAD_INVALID",
            f"{field} must be a lowercase SHA-256 hexadecimal value",
        )
    return value


def _positive_int(value: Any, *, field: str, allow_zero: bool = False) -> int:
    minimum = 0 if allow_zero else 1
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= 2_147_483_647:
        raise _error(
            "OPERATION_LEASE_PAYLOAD_INVALID", f"{field} is invalid"
        )
    return value


def _utc_text(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or _UTC_RE.fullmatch(value) is None:
        raise _error("OPERATION_LEASE_TIME_INVALID", f"{field} is invalid")
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise _error("OPERATION_LEASE_TIME_INVALID", f"{field} is invalid") from exc
    return value


def parse_utc(value: Any, *, field: str) -> datetime:
    return datetime.strptime(_utc_text(value, field=field), "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=timezone.utc
    )


def _expected_versions(value: Any) -> dict[str, int]:
    if not isinstance(value, dict) or not value or len(value) > MAX_EXPECTED_VERSIONS:
        raise _error(
            "OPERATION_LEASE_PAYLOAD_INVALID",
            "expected_versions must be a non-empty bounded object",
        )
    result: dict[str, int] = {}
    for raw_key, raw_version in value.items():
        key = _bounded_text(raw_key, field="expected_versions key")
        if key in result:
            raise _error(
                "OPERATION_LEASE_PAYLOAD_INVALID",
                "expected_versions contains a duplicate key",
            )
        result[key] = _positive_int(
            raw_version,
            field=f"expected_versions.{key}",
            allow_zero=True,
        )
    return result


def validate_payload(value: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != LEASE_PAYLOAD_KEYS:
        raise _error(
            "OPERATION_LEASE_PAYLOAD_INVALID",
            "lease payload does not contain the exact v1 fields",
        )
    result = {
        "contract_version": value.get("contract_version"),
        "lease_id": _bounded_text(value.get("lease_id"), field="lease_id", maximum=128),
        "site_id": _bounded_text(value.get("site_id"), field="site_id", maximum=128),
        "program": _bounded_text(value.get("program"), field="program", maximum=64),
        "device_id": _bounded_text(value.get("device_id"), field="device_id"),
        "source_host_id": _bounded_text(value.get("source_host_id"), field="source_host_id"),
        "authority_scope_id": _bounded_text(value.get("authority_scope_id"), field="authority_scope_id"),
        "ledger_plane": _bounded_text(value.get("ledger_plane"), field="ledger_plane", maximum=64).upper(),
        "plane_epoch": _positive_int(value.get("plane_epoch"), field="plane_epoch"),
        "operation": _bounded_text(value.get("operation"), field="operation", maximum=64),
        "resource_id": _bounded_text(value.get("resource_id"), field="resource_id"),
        "physical_label_id": _bounded_text(value.get("physical_label_id"), field="physical_label_id"),
        "physical_qr_sha256": _hash64(value.get("physical_qr_sha256"), field="physical_qr_sha256"),
        "item_id": _bounded_text(value.get("item_id"), field="item_id", maximum=128),
        "quantity": _positive_int(value.get("quantity"), field="quantity"),
        "member_count": _positive_int(value.get("member_count"), field="member_count"),
        "membership_hash": _hash64(value.get("membership_hash"), field="membership_hash"),
        "expected_versions": _expected_versions(value.get("expected_versions")),
        "issued_at": _utc_text(value.get("issued_at"), field="issued_at"),
        "expires_at": _utc_text(value.get("expires_at"), field="expires_at"),
        "fence": _positive_int(value.get("fence"), field="fence"),
        "snapshot_hash": _hash64(value.get("snapshot_hash"), field="snapshot_hash"),
    }
    if result["contract_version"] != LEASE_CONTRACT_VERSION:
        raise _error(
            "OPERATION_LEASE_PAYLOAD_INVALID", "lease contract is invalid"
        )
    if result["program"] != PROGRAM or result["operation"] != OPERATION:
        raise _error(
            "OPERATION_LEASE_BINDING_MISMATCH",
            "lease program or operation is not packaging",
        )
    if result["quantity"] != result["member_count"]:
        raise _error(
            "OPERATION_LEASE_PAYLOAD_INVALID",
            "quantity and member_count must match for packaging",
        )
    issued = parse_utc(result["issued_at"], field="issued_at")
    expires = parse_utc(result["expires_at"], field="expires_at")
    duration = int((expires - issued).total_seconds())
    if duration < 1 or duration > MAX_LEASE_SECONDS:
        raise _error(
            "OPERATION_LEASE_TIME_INVALID", "lease duration is invalid"
        )
    return result


def _b64decode(value: str, *, field: str, maximum: int) -> bytes:
    text = str(value or "")
    if not text or len(text) > maximum or _B64_RE.fullmatch(text) is None:
        raise _error("OPERATION_LEASE_JWS_INVALID", f"{field} is invalid")
    try:
        decoded = base64.b64decode(
            text + "=" * ((4 - len(text) % 4) % 4), altchars=b"-_", validate=True
        )
    except (ValueError, base64.binascii.Error) as exc:
        raise _error("OPERATION_LEASE_JWS_INVALID", f"{field} is invalid") from exc
    if base64.urlsafe_b64encode(decoded).rstrip(b"=").decode("ascii") != text:
        raise _error("OPERATION_LEASE_JWS_INVALID", f"{field} is not canonical")
    return decoded


def _jwk(value: Any) -> dict[str, str]:
    if not isinstance(value, dict) or set(value) != PUBLIC_JWK_KEYS:
        raise _error("OPERATION_LEASE_KEYRING_INVALID", "public JWK is invalid")
    result = {key: value.get(key) for key in ("kty", "crv", "x", "y")}
    if result["kty"] != "EC" or result["crv"] != "P-256":
        raise _error("OPERATION_LEASE_KEYRING_INVALID", "public JWK is not P-256")
    if any(not isinstance(item, str) for item in result.values()):
        raise _error("OPERATION_LEASE_KEYRING_INVALID", "public JWK is invalid")
    x = _b64decode(result["x"], field="jwk.x", maximum=64)
    y = _b64decode(result["y"], field="jwk.y", maximum=64)
    if len(x) != 32 or len(y) != 32:
        raise _error("OPERATION_LEASE_KEYRING_INVALID", "public JWK is invalid")
    try:
        ec.EllipticCurvePublicNumbers(
            int.from_bytes(x, "big"), int.from_bytes(y, "big"), ec.SECP256R1()
        ).public_key()
    except ValueError as exc:
        raise _error("OPERATION_LEASE_KEYRING_INVALID", "public JWK is invalid") from exc
    return {key: str(result[key]) for key in ("kty", "crv", "x", "y")}


def jwk_thumbprint(value: Any) -> str:
    digest = hashlib.sha256(canonical_json_bytes(_jwk(value))).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def normalize_keyring(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != KEYRING_KEYS:
        raise _error("OPERATION_LEASE_KEYRING_INVALID", "keyring fields are invalid")
    if value.get("contract_version") != KEYRING_CONTRACT_VERSION:
        raise _error("OPERATION_LEASE_KEYRING_INVALID", "keyring contract is invalid")
    site_id = _bounded_text(value.get("site_id"), field="site_id", maximum=128)
    current_kid = _bounded_text(value.get("current_kid"), field="current_kid", maximum=128)
    entries = value.get("keys")
    if not isinstance(entries, list) or not 1 <= len(entries) <= MAX_KEYS:
        raise _error("OPERATION_LEASE_KEYRING_INVALID", "keyring size is invalid")
    normalized = []
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != KEYRING_ENTRY_KEYS:
            raise _error("OPERATION_LEASE_KEYRING_INVALID", "keyring entry is invalid")
        kid = _bounded_text(entry.get("kid"), field="kid", maximum=128)
        status = entry.get("status")
        public_jwk = _jwk(entry.get("public_jwk"))
        thumbprint = _bounded_text(entry.get("thumbprint"), field="thumbprint", maximum=128)
        if status not in {"current", "retained"} or thumbprint != jwk_thumbprint(public_jwk):
            raise _error("OPERATION_LEASE_KEYRING_INVALID", "keyring entry is invalid")
        normalized.append(
            {"kid": kid, "status": status, "public_jwk": public_jwk, "thumbprint": thumbprint}
        )
    kids = [entry["kid"] for entry in normalized]
    materials = [entry["thumbprint"] for entry in normalized]
    currents = [entry["kid"] for entry in normalized if entry["status"] == "current"]
    if len(set(kids)) != len(kids) or len(set(materials)) != len(materials) or currents != [current_kid]:
        raise _error("OPERATION_LEASE_KEYRING_INVALID", "keyring identities are invalid")
    return {
        "contract_version": KEYRING_CONTRACT_VERSION,
        "site_id": site_id,
        "current_kid": current_kid,
        "keys": normalized,
    }


def _header(token: str) -> dict[str, str]:
    if not isinstance(token, str) or not token or len(token.encode("ascii", errors="ignore")) > MAX_COMPACT_JWS_BYTES:
        raise _error("OPERATION_LEASE_JWS_INVALID", "lease token is invalid")
    parts = token.split(".")
    if len(parts) != 3 or any(not part for part in parts):
        raise _error("OPERATION_LEASE_JWS_INVALID", "lease token is invalid")
    raw = _b64decode(parts[0], field="protected header", maximum=2048)
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise _error("OPERATION_LEASE_JWS_INVALID", "lease header is invalid") from exc
    if (
        not isinstance(value, dict)
        or set(value) != {"alg", "kid", "typ"}
        or value.get("alg") != JWS_ALGORITHM
        or value.get("typ") != JWS_TYPE
        or canonical_json_bytes(value) != raw
    ):
        raise _error("OPERATION_LEASE_JWS_INVALID", "lease header is invalid")
    return {"alg": JWS_ALGORITHM, "kid": _bounded_text(value.get("kid"), field="kid", maximum=128), "typ": JWS_TYPE}


class PinnedOperationLeaseKeyring:
    def __init__(self, path: str | os.PathLike[str]) -> None:
        raw = Path(path).expanduser()
        if not raw.name or raw.is_symlink():
            raise _error("OPERATION_LEASE_KEYRING_PATH_INVALID", "keyring path is invalid")
        self.path = Path(os.path.abspath(os.fspath(raw)))

    def _load(self, *, required: bool) -> dict[str, Any] | None:
        if self.path.is_symlink():
            raise _error("OPERATION_LEASE_KEYRING_PATH_INVALID", "keyring path is invalid")
        try:
            stat = self.path.stat()
        except FileNotFoundError:
            if required:
                raise _error("OPERATION_LEASE_KEYRING_NOT_PINNED", "no keyring is pinned")
            return None
        if not self.path.is_file() or not 1 <= stat.st_size <= MAX_KEYRING_BYTES:
            raise _error("OPERATION_LEASE_KEYRING_INVALID", "pinned keyring is invalid")
        try:
            raw = self.path.read_bytes()
            value = json.loads(raw.decode("utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise _error("OPERATION_LEASE_KEYRING_INVALID", "pinned keyring is invalid") from exc
        if canonical_json_bytes(value) != raw:
            raise _error("OPERATION_LEASE_KEYRING_INVALID", "pinned keyring is not canonical")
        return normalize_keyring(value)

    def bootstrap_authenticated(self, value: Mapping[str, Any], *, authenticated_online: bool) -> dict[str, Any]:
        if authenticated_online is not True:
            raise _error("OPERATION_LEASE_KEYRING_UNAUTHENTICATED", "keyring bootstrap requires authenticated online data")
        incoming = normalize_keyring(dict(value))
        existing = self._load(required=False)
        if existing is not None:
            if existing["site_id"] != incoming["site_id"]:
                raise _error("OPERATION_LEASE_KEYRING_BINDING_MISMATCH", "keyring site changed")
            old_by_kid = {entry["kid"]: entry["thumbprint"] for entry in existing["keys"]}
            old_by_material = {entry["thumbprint"]: entry["kid"] for entry in existing["keys"]}
            for entry in incoming["keys"]:
                if entry["kid"] in old_by_kid and old_by_kid[entry["kid"]] != entry["thumbprint"]:
                    raise _error("OPERATION_LEASE_KID_REUSE_REJECTED", "pinned kid changed key material")
                if entry["thumbprint"] in old_by_material and old_by_material[entry["thumbprint"]] != entry["kid"]:
                    raise _error("OPERATION_LEASE_KEY_ALIAS_REJECTED", "pinned key material changed kid")
            incoming_kids = {entry["kid"] for entry in incoming["keys"]}
            if not set(old_by_kid).issubset(incoming_kids):
                raise _error("OPERATION_LEASE_KEY_REMOVAL_REJECTED", "authenticated keyring removed a retained pin")
        raw = canonical_json_bytes(incoming)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(prefix=f".{self.path.name}.", suffix=".tmp", dir=str(self.path.parent))
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(raw)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
            temporary = ""
        finally:
            if temporary:
                try:
                    os.unlink(temporary)
                except OSError:
                    pass
        return incoming

    def verify(self, token: str, *, expected: Mapping[str, Any], operation_snapshot: Mapping[str, Any], now: datetime | None = None) -> dict[str, Any]:
        if not isinstance(expected, Mapping) or set(expected) != LEASE_BINDING_KEYS:
            raise _error("OPERATION_LEASE_BINDING_INVALID", "expected binding fields are invalid")
        if not isinstance(operation_snapshot, Mapping) or len(canonical_json_bytes(operation_snapshot)) > MAX_OPERATION_SNAPSHOT_BYTES:
            raise _error("OPERATION_LEASE_SNAPSHOT_INVALID", "operation snapshot is invalid")
        header = _header(token)
        keyring = self._load(required=True)
        assert keyring is not None
        entry = next((item for item in keyring["keys"] if item["kid"] == header["kid"]), None)
        if entry is None:
            raise _error("OPERATION_LEASE_KEY_NOT_PINNED", "lease kid is not pinned")
        parts = token.split(".")
        payload_raw = _b64decode(parts[1], field="lease payload", maximum=MAX_JWS_PAYLOAD_BYTES * 2)
        if len(payload_raw) > MAX_JWS_PAYLOAD_BYTES:
            raise _error("OPERATION_LEASE_JWS_INVALID", "lease payload is oversized")
        try:
            payload_value = json.loads(payload_raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise _error("OPERATION_LEASE_JWS_INVALID", "lease payload is invalid") from exc
        if not isinstance(payload_value, dict) or canonical_json_bytes(payload_value) != payload_raw:
            raise _error("OPERATION_LEASE_JWS_INVALID", "lease payload is not canonical")
        payload = validate_payload(payload_value)
        signature = _b64decode(parts[2], field="lease signature", maximum=128)
        if len(signature) != 64:
            raise _error("OPERATION_LEASE_SIGNATURE_INVALID", "lease signature is invalid")
        r = int.from_bytes(signature[:32], "big")
        s = int.from_bytes(signature[32:], "big")
        if not 1 <= r < _P256_ORDER or not 1 <= s <= _P256_ORDER // 2:
            raise _error("OPERATION_LEASE_SIGNATURE_INVALID", "lease signature is not low-S")
        jwk = entry["public_jwk"]
        x = _b64decode(jwk["x"], field="jwk.x", maximum=64)
        y = _b64decode(jwk["y"], field="jwk.y", maximum=64)
        public_key = ec.EllipticCurvePublicNumbers(int.from_bytes(x, "big"), int.from_bytes(y, "big"), ec.SECP256R1()).public_key()
        try:
            public_key.verify(encode_dss_signature(r, s), f"{parts[0]}.{parts[1]}".encode("ascii"), ec.ECDSA(hashes.SHA256()))
        except InvalidSignature as exc:
            raise _error("OPERATION_LEASE_SIGNATURE_INVALID", "lease signature is invalid") from exc
        instant = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        if instant < parse_utc(payload["issued_at"], field="issued_at"):
            raise _error("OPERATION_LEASE_NOT_YET_VALID", "lease is not yet valid")
        if instant >= parse_utc(payload["expires_at"], field="expires_at"):
            raise _error("OPERATION_LEASE_EXPIRED", "lease has expired")
        if canonical_sha256(dict(operation_snapshot)) != payload["snapshot_hash"]:
            raise _error("OPERATION_LEASE_SNAPSHOT_MISMATCH", "operation snapshot hash differs")
        actual = {key: payload[key] for key in LEASE_BINDING_KEYS}
        if canonical_json_bytes(actual) != canonical_json_bytes(dict(expected)):
            mismatches = [key for key in sorted(LEASE_BINDING_KEYS) if canonical_json_bytes(actual.get(key)) != canonical_json_bytes(expected.get(key))]
            raise OperationLeaseError("OPERATION_LEASE_BINDING_MISMATCH", "lease binding differs: " + ", ".join(mismatches))
        if payload["site_id"] != keyring["site_id"]:
            raise _error("OPERATION_LEASE_BINDING_MISMATCH", "lease site differs from pinned keyring")
        return payload


class OperationLeaseStore:
    """Durable lease state; consume claiming never changes business status."""

    def __init__(self, database_path: str | os.PathLike[str]) -> None:
        self.path = os.path.abspath(os.fspath(database_path))
        self._lock = threading.RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    def _initialize(self) -> None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            schema = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
                ("package_operation_leases",),
            ).fetchone()
            legacy_resource_unique = bool(
                schema
                and re.search(
                    r"resource_id\s+TEXT\s+NOT\s+NULL\s+UNIQUE",
                    str(schema["sql"] or ""),
                    flags=re.IGNORECASE,
                )
            )
            if legacy_resource_unique:
                conn.execute(
                    "ALTER TABLE package_operation_leases "
                    "RENAME TO package_operation_leases_v1"
                )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS package_operation_leases (
                       lease_id TEXT PRIMARY KEY,
                       resource_id TEXT NOT NULL,
                       set_id TEXT UNIQUE,
                       issue_idempotency_key TEXT NOT NULL UNIQUE,
                       token TEXT NOT NULL,
                       artifact_json TEXT NOT NULL,
                       binding_json TEXT NOT NULL,
                       operation_snapshot_json TEXT NOT NULL,
                       snapshot_hash TEXT NOT NULL,
                       fence INTEGER NOT NULL,
                       status TEXT NOT NULL CHECK(status IN (
                           'PREFETCHED','LOCAL_COMPLETED','ACKED','OPERATOR_REVIEW'
                       )),
                       operation_result_id TEXT,
                       operation_completed_at TEXT,
                       consume_idempotency_key TEXT,
                       consume_claimed_at TEXT,
                       consume_receipt_json TEXT,
                       last_error_code TEXT,
                       last_error_message TEXT,
                       created_at TEXT NOT NULL,
                       updated_at TEXT NOT NULL
                   )"""
            )
            if legacy_resource_unique:
                conn.execute(
                    """INSERT INTO package_operation_leases(
                           lease_id,resource_id,set_id,issue_idempotency_key,
                           token,artifact_json,binding_json,
                           operation_snapshot_json,snapshot_hash,fence,status,
                           operation_result_id,operation_completed_at,
                           consume_idempotency_key,consume_claimed_at,
                           consume_receipt_json,last_error_code,
                           last_error_message,created_at,updated_at
                       ) SELECT
                           lease_id,resource_id,set_id,issue_idempotency_key,
                           token,artifact_json,binding_json,
                           operation_snapshot_json,snapshot_hash,fence,status,
                           operation_result_id,operation_completed_at,
                           consume_idempotency_key,consume_claimed_at,
                           consume_receipt_json,last_error_code,
                           last_error_message,created_at,updated_at
                         FROM package_operation_leases_v1"""
                )
                conn.execute("DROP TABLE package_operation_leases_v1")
            conn.execute(
                """CREATE INDEX IF NOT EXISTS ix_package_operation_lease_status
                       ON package_operation_leases(status,updated_at,lease_id)"""
            )
            conn.execute(
                """CREATE UNIQUE INDEX IF NOT EXISTS
                       ux_package_operation_lease_unresolved_resource
                       ON package_operation_leases(resource_id)
                       WHERE status IN ('PREFETCHED','LOCAL_COMPLETED')"""
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS
                       package_operation_lease_issue_attempts (
                       attempt_id TEXT PRIMARY KEY,
                       request_fingerprint TEXT NOT NULL,
                       issue_idempotency_key TEXT NOT NULL UNIQUE,
                       lease_id TEXT UNIQUE,
                       status TEXT NOT NULL CHECK(status IN ('ACTIVE','RETIRED')),
                       retire_reason TEXT,
                       retired_at TEXT,
                       created_at TEXT NOT NULL,
                       updated_at TEXT NOT NULL
                   )"""
            )
            conn.execute(
                """CREATE UNIQUE INDEX IF NOT EXISTS
                       ux_package_operation_lease_active_issue_request
                       ON package_operation_lease_issue_attempts(request_fingerprint)
                       WHERE status='ACTIVE'"""
            )
            conn.execute(
                """CREATE INDEX IF NOT EXISTS
                       ix_package_operation_lease_issue_attempt_status
                       ON package_operation_lease_issue_attempts(
                           status,updated_at,attempt_id
                       )"""
            )
            self._backfill_issue_attempts(conn)
            conn.commit()

    @staticmethod
    def _fingerprint_from_binding(binding: Mapping[str, Any]) -> str:
        return issue_request_fingerprint(
            program=str(binding.get("program") or ""),
            device_id=str(binding.get("device_id") or ""),
            source_host_id=str(binding.get("source_host_id") or ""),
            authority_scope_id=str(
                binding.get("authority_scope_id") or ""
            ),
            operation=str(binding.get("operation") or ""),
            physical_qr_hash=str(
                binding.get("physical_qr_sha256") or ""
            ),
        )

    def _backfill_issue_attempts(self, conn: sqlite3.Connection) -> None:
        rows = conn.execute(
            """SELECT lease_id,issue_idempotency_key,binding_json,status,
                      created_at,updated_at
                   FROM package_operation_leases
                  WHERE issue_idempotency_key NOT IN (
                      SELECT issue_idempotency_key
                        FROM package_operation_lease_issue_attempts
                  )
                  ORDER BY created_at,lease_id"""
        ).fetchall()
        for row in rows:
            try:
                binding = json.loads(str(row["binding_json"]))
                fingerprint = self._fingerprint_from_binding(binding)
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                raise _error(
                    "OPERATION_LEASE_STORE_INVALID",
                    "stored lease binding cannot be migrated",
                ) from exc
            key = str(row["issue_idempotency_key"])
            status = (
                "ACTIVE"
                if str(row["status"]) in {"PREFETCHED", "LOCAL_COMPLETED"}
                else "RETIRED"
            )
            retired_at = None if status == "ACTIVE" else str(row["updated_at"])
            reason = None if status == "ACTIVE" else str(row["status"])
            conn.execute(
                """INSERT INTO package_operation_lease_issue_attempts(
                       attempt_id,request_fingerprint,issue_idempotency_key,
                       lease_id,status,retire_reason,retired_at,
                       created_at,updated_at
                   ) VALUES (?,?,?,?,?,?,?,?,?)""",
                (
                    "lease-attempt-"
                    + hashlib.sha256(key.encode("utf-8")).hexdigest(),
                    fingerprint,
                    key,
                    str(row["lease_id"]),
                    status,
                    reason,
                    retired_at,
                    str(row["created_at"]),
                    str(row["updated_at"]),
                ),
            )

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def reserve_issue_attempt(self, request_fingerprint: str) -> dict[str, Any]:
        fingerprint = _hash64(
            request_fingerprint, field="request_fingerprint"
        )
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                """SELECT * FROM package_operation_lease_issue_attempts
                      WHERE request_fingerprint=? AND status='ACTIVE'""",
                (fingerprint,),
            ).fetchone()
            if existing is not None:
                conn.commit()
                return dict(existing)
            now = self._now()
            for _attempt in range(5):
                attempt_id = "lease-attempt-" + secrets.token_hex(16)
                key = "lease-issue-" + secrets.token_hex(32)
                try:
                    conn.execute(
                        """INSERT INTO package_operation_lease_issue_attempts(
                               attempt_id,request_fingerprint,
                               issue_idempotency_key,status,created_at,updated_at
                           ) VALUES (?,?,?,'ACTIVE',?,?)""",
                        (attempt_id, fingerprint, key, now, now),
                    )
                except sqlite3.IntegrityError:
                    continue
                row = conn.execute(
                    """SELECT * FROM package_operation_lease_issue_attempts
                          WHERE attempt_id=?""",
                    (attempt_id,),
                ).fetchone()
                conn.commit()
                return dict(row)
            conn.rollback()
            raise _error(
                "OPERATION_LEASE_STORE_CONFLICT",
                "could not reserve a unique lease issue attempt",
            )

    def get_issue_attempt(
        self,
        *,
        request_fingerprint: str = "",
        issue_idempotency_key: str = "",
    ) -> dict[str, Any] | None:
        if not request_fingerprint and not issue_idempotency_key:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """SELECT * FROM package_operation_lease_issue_attempts
                      WHERE request_fingerprint=? OR issue_idempotency_key=?
                      ORDER BY CASE status WHEN 'ACTIVE' THEN 0 ELSE 1 END,
                               created_at DESC
                      LIMIT 1""",
                (request_fingerprint, issue_idempotency_key),
            ).fetchone()
            return dict(row) if row else None

    def get_reusable_prefetched(
        self, request_fingerprint: str
    ) -> dict[str, Any] | None:
        fingerprint = _hash64(
            request_fingerprint, field="request_fingerprint"
        )
        with self._connect() as conn:
            row = conn.execute(
                """SELECT lease.*,
                          attempt.request_fingerprint AS issue_request_fingerprint,
                          attempt.status AS issue_attempt_status
                     FROM package_operation_lease_issue_attempts AS attempt
                     JOIN package_operation_leases AS lease
                       ON lease.lease_id=attempt.lease_id
                    WHERE attempt.request_fingerprint=?
                      AND attempt.status='ACTIVE'
                      AND lease.status='PREFETCHED'
                    LIMIT 1""",
                (fingerprint,),
            ).fetchone()
            return dict(row) if row else None

    def retire_issue_attempt(
        self,
        *,
        lease_id: str,
        reason: str,
    ) -> bool:
        identity = _bounded_text(lease_id, field="lease_id", maximum=128)
        selected_reason = _bounded_text(
            reason, field="retire_reason", maximum=128
        )
        now = self._now()
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute(
                """UPDATE package_operation_lease_issue_attempts
                      SET status='RETIRED',retire_reason=?,retired_at=?,updated_at=?
                    WHERE lease_id=? AND status='ACTIVE'""",
                (selected_reason, now, now, identity),
            )
            conn.commit()
            return cursor.rowcount == 1

    def save_prefetched(self, *, artifact: Mapping[str, Any], binding: Mapping[str, Any], issue_idempotency_key: str) -> dict[str, Any]:
        payload = dict(artifact.get("claims") or {})
        lease_id = str(payload.get("lease_id") or "")
        resource_id = str(payload.get("resource_id") or "")
        issue_key = str(issue_idempotency_key or "").strip()
        if (
            not lease_id
            or not resource_id
            or not issue_key
            or set(binding) != LEASE_BINDING_KEYS
        ):
            raise _error("OPERATION_LEASE_STORE_INVALID", "verified lease identity is missing")
        fingerprint = self._fingerprint_from_binding(binding)
        encoded_artifact = canonical_json_bytes(dict(artifact)).decode("utf-8")
        encoded_binding = canonical_json_bytes(dict(binding)).decode("utf-8")
        encoded_snapshot = canonical_json_bytes(dict(artifact["operation_snapshot"])).decode("utf-8")
        now = self._now()
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            attempt = conn.execute(
                """SELECT * FROM package_operation_lease_issue_attempts
                      WHERE issue_idempotency_key=?""",
                (issue_key,),
            ).fetchone()
            if attempt is None:
                active = conn.execute(
                    """SELECT issue_idempotency_key
                         FROM package_operation_lease_issue_attempts
                        WHERE request_fingerprint=? AND status='ACTIVE'""",
                    (fingerprint,),
                ).fetchone()
                if active is not None:
                    conn.rollback()
                    raise _error(
                        "OPERATION_LEASE_STORE_CONFLICT",
                        "lease issue request already has another active attempt",
                    )
                conn.execute(
                    """INSERT INTO package_operation_lease_issue_attempts(
                           attempt_id,request_fingerprint,issue_idempotency_key,
                           status,created_at,updated_at
                       ) VALUES (?,?,?,'ACTIVE',?,?)""",
                    (
                        "lease-attempt-" + secrets.token_hex(16),
                        fingerprint,
                        issue_key,
                        now,
                        now,
                    ),
                )
                attempt = conn.execute(
                    """SELECT * FROM package_operation_lease_issue_attempts
                          WHERE issue_idempotency_key=?""",
                    (issue_key,),
                ).fetchone()
            if (
                str(attempt["request_fingerprint"]) != fingerprint
                or str(attempt["status"]) != "ACTIVE"
                or (
                    attempt["lease_id"] is not None
                    and str(attempt["lease_id"]) != lease_id
                )
            ):
                conn.rollback()
                raise _error(
                    "OPERATION_LEASE_STORE_CONFLICT",
                    "lease artifact differs from its durable issue attempt",
                )
            existing = conn.execute(
                "SELECT * FROM package_operation_leases WHERE lease_id=?",
                (lease_id,),
            ).fetchone()
            if existing:
                if existing["artifact_json"] != encoded_artifact or existing["binding_json"] != encoded_binding or existing["issue_idempotency_key"] != issue_key:
                    conn.rollback()
                    raise _error("OPERATION_LEASE_STORE_CONFLICT", "lease already has different durable evidence")
                conn.execute(
                    """UPDATE package_operation_lease_issue_attempts
                          SET lease_id=COALESCE(lease_id,?),updated_at=?
                        WHERE issue_idempotency_key=? AND status='ACTIVE'
                          AND (lease_id IS NULL OR lease_id=?)""",
                    (lease_id, now, issue_key, lease_id),
                )
                conn.commit()
                return dict(existing)
            unresolved = conn.execute(
                """SELECT lease_id FROM package_operation_leases
                      WHERE resource_id=?
                        AND status IN ('PREFETCHED','LOCAL_COMPLETED')""",
                (resource_id,),
            ).fetchone()
            if unresolved is not None:
                conn.rollback()
                raise _error(
                    "OPERATION_LEASE_STORE_CONFLICT",
                    "resource already has another unresolved durable lease",
                )
            conn.execute(
                """INSERT INTO package_operation_leases(
                       lease_id,resource_id,issue_idempotency_key,token,artifact_json,
                       binding_json,operation_snapshot_json,snapshot_hash,fence,status,
                       created_at,updated_at
                   ) VALUES (?,?,?,?,?,?,?,?,?,'PREFETCHED',?,?)""",
                (lease_id, resource_id, issue_key, artifact["token"], encoded_artifact, encoded_binding, encoded_snapshot, payload["snapshot_hash"], payload["fence"], now, now),
            )
            cursor = conn.execute(
                """UPDATE package_operation_lease_issue_attempts
                      SET lease_id=?,updated_at=?
                    WHERE issue_idempotency_key=? AND status='ACTIVE'
                      AND lease_id IS NULL""",
                (lease_id, now, issue_key),
            )
            if cursor.rowcount != 1:
                conn.rollback()
                raise _error(
                    "OPERATION_LEASE_STORE_CONFLICT",
                    "lease issue attempt lost its durable binding",
                )
            row = conn.execute("SELECT * FROM package_operation_leases WHERE lease_id=?", (lease_id,)).fetchone()
            conn.commit()
            return dict(row)

    def attach_set(self, lease_id: str, set_id: str) -> dict[str, Any]:
        with self._lock, self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cursor = conn.execute("UPDATE package_operation_leases SET set_id=COALESCE(set_id,?),updated_at=? WHERE lease_id=? AND (set_id IS NULL OR set_id=?)", (set_id, self._now(), lease_id, set_id))
            if cursor.rowcount != 1:
                conn.rollback()
                raise _error("OPERATION_LEASE_STORE_CONFLICT", "lease set binding changed")
            row = conn.execute("SELECT * FROM package_operation_leases WHERE lease_id=?", (lease_id,)).fetchone()
            conn.commit()
            return dict(row)

    def get(self, *, lease_id: str = "", set_id: str = "") -> dict[str, Any] | None:
        if not lease_id and not set_id:
            return None
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM package_operation_leases WHERE lease_id=? OR set_id=?", (lease_id, set_id)).fetchone()
            return dict(row) if row else None

def normalize_issue_artifact(value: Mapping[str, Any]) -> dict[str, Any]:
    required = {"contract_version", "lease_id", "status", "replayed", "token", "kid", "expires_at", "fence", "snapshot_hash", "operation_snapshot", "keyring"}
    if not isinstance(value, Mapping) or set(value) != required:
        raise _error("OPERATION_LEASE_ARTIFACT_INVALID", "issue artifact fields are invalid")
    if value.get("contract_version") != ARTIFACT_CONTRACT_VERSION or value.get("status") != "ACTIVE":
        raise _error("OPERATION_LEASE_ARTIFACT_INVALID", "issue artifact is not active")
    if not isinstance(value.get("replayed"), bool) or not isinstance(value.get("operation_snapshot"), Mapping):
        raise _error("OPERATION_LEASE_ARTIFACT_INVALID", "issue artifact is invalid")
    result = dict(value)
    result["lease_id"] = _bounded_text(
        value.get("lease_id"), field="lease_id", maximum=128
    )
    result["kid"] = _bounded_text(
        value.get("kid"), field="kid", maximum=128
    )
    result["expires_at"] = _utc_text(
        value.get("expires_at"), field="expires_at"
    )
    result["fence"] = _positive_int(value.get("fence"), field="fence")
    result["snapshot_hash"] = _hash64(
        value.get("snapshot_hash"), field="snapshot_hash"
    )
    if (
        len(canonical_json_bytes(dict(value["operation_snapshot"])))
        > MAX_OPERATION_SNAPSHOT_BYTES
        or _header(value.get("token"))["kid"] != result["kid"]
    ):
        raise _error(
            "OPERATION_LEASE_ARTIFACT_INVALID",
            "issue artifact metadata is invalid",
        )
    result["keyring"] = normalize_keyring(value.get("keyring"))
    return result


__all__ = [
    "LEASE_BINDING_KEYS",
    "ARTIFACT_CONTRACT_VERSION",
    "LEASE_CONTRACT_VERSION",
    "OPERATION",
    "OperationLeaseError",
    "OperationLeaseStore",
    "PinnedOperationLeaseKeyring",
    "PROGRAM",
    "canonical_json_bytes",
    "canonical_sha256",
    "issue_request_fingerprint",
    "normalize_issue_artifact",
    "physical_qr_sha256",
    "validate_payload",
]

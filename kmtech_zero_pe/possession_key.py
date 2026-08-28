"""Persistent non-exportable possession keys backed by Windows NCrypt.

The wire-format primitives come from :mod:`kmtech_zero_pe.cng_p256`, the
byte-identical seq259 BCrypt implementation.  This module adds only the KSP
lifecycle that BCrypt cannot provide: named KSP keys, policy inspection,
public-key export, and signing through ``ncrypt.dll``.

Routine application startup must call :meth:`PersistentPossessionKey.open_existing`.
Only a first-enrollment path that has independently established that no prior
server binding exists may call :meth:`PersistentPossessionKey.provision_initial`.
That split prevents a missing or damaged key from being silently replaced.
"""

from __future__ import annotations

import ctypes
from ctypes import wintypes
from dataclasses import dataclass
from datetime import datetime
import hashlib
import os
import re
import struct
from typing import Any, Mapping

from .cng_p256 import (
    BCRYPT_ECDSA_P256_ALGORITHM,
    BCRYPT_ECDSA_PUBLIC_P256_MAGIC,
    P256_BYTES,
    P256_ORDER,
    b64url_encode,
    canonical_json_bytes,
    jwk_thumbprint,
)


MS_KEY_STORAGE_PROVIDER = "Microsoft Software Key Storage Provider"
MS_PLATFORM_CRYPTO_PROVIDER = "Microsoft Platform Crypto Provider"
SUPPORTED_PROVIDER_NAMES = frozenset(
    {MS_KEY_STORAGE_PROVIDER, MS_PLATFORM_CRYPTO_PROVIDER}
)

DEFAULT_KEY_NAME = "KMTech.DirectSync.Possession.v1"
SCOPE_CURRENT_USER = "current_user"
SCOPE_LOCAL_MACHINE = "local_machine"
DEFAULT_KEY_SCOPE = SCOPE_CURRENT_USER
SUPPORTED_KEY_SCOPES = frozenset({SCOPE_CURRENT_USER, SCOPE_LOCAL_MACHINE})
POSSESSION_KEY_CONTRACT_VERSION = "producer-machine-possession-key-v1"
REATTACH_PROOF_CONTRACT_VERSION = "producer-reattach-proof-v1"
REATTACH_AUDIENCE = "worker-analysis-producer-reattach-v1"

NCRYPT_MACHINE_KEY_FLAG = 0x00000020
NCRYPT_SILENT_FLAG = 0x00000040
NCRYPT_PERSIST_FLAG = 0x80000000
NCRYPT_ALLOW_SIGNING_FLAG = 0x00000002

NCRYPT_ALGORITHM_PROPERTY = "Algorithm Name"
NCRYPT_EXPORT_POLICY_PROPERTY = "Export Policy"
NCRYPT_KEY_TYPE_PROPERTY = "Key Type"
NCRYPT_KEY_USAGE_PROPERTY = "Key Usage"
NCRYPT_UNIQUE_NAME_PROPERTY = "Unique Name"
BCRYPT_ECCPUBLIC_BLOB = "ECCPUBLICBLOB"
BCRYPT_ECCPRIVATE_BLOB = "ECCPRIVATEBLOB"

ERROR_SUCCESS = 0x00000000
NTE_BAD_KEY = 0x80090003
NTE_BAD_KEY_STATE = 0x8009000B
NTE_EXISTS = 0x8009000F
NTE_PERM = 0x80090010
NTE_BAD_KEYSET = 0x80090016
NTE_NOT_SUPPORTED = 0x80090029

ADMIN_RECOVERY_ACTION = "ADMIN_RECOVERY_REQUIRED"

REATTACH_PROOF_FIELDS = frozenset(
    {
        "contract_version",
        "challenge_id",
        "nonce",
        "expires_at",
        "audience",
        "producer_id",
        "producer_install_id",
        "source_host_id",
        "manifest_hash",
    }
)
REATTACH_CANONICAL_KEY_ORDER = tuple(sorted(REATTACH_PROOF_FIELDS))

_KEY_NAME = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9._-]{0,126}[A-Za-z0-9])?")
_LOWER_HEX_64 = re.compile(r"[0-9a-f]{64}")
_STRICT_UTC = "%Y-%m-%dT%H:%M:%SZ"
_MAX_PROPERTY_BYTES = 65_536


class NCryptError(RuntimeError):
    """A Windows NCrypt call failed."""

    def __init__(self, operation: str, status: int) -> None:
        self.operation = str(operation)
        self.status = int(status) & 0xFFFFFFFF
        super().__init__(f"{self.operation} failed with SECURITY_STATUS 0x{self.status:08X}")


class KeyPolicyError(RuntimeError):
    """An opened key does not satisfy the scope/signing/no-export contract."""


class ExportPolicyViolation(KeyPolicyError):
    """Private-key export unexpectedly succeeded."""


class ReattachProofError(ValueError):
    """A server challenge does not match the exact re-attach proof contract."""


class AdminRecoveryRequired(RuntimeError):
    """The existing possession key is missing, unreadable, or policy-invalid."""

    recovery_action = ADMIN_RECOVERY_ACTION

    def __init__(
        self,
        key_name: str,
        provider_name: str,
        scope: str,
        reason: str,
        *,
        status: int | None = None,
    ) -> None:
        self.key_name = str(key_name)
        self.provider_name = str(provider_name)
        self.scope = str(scope)
        self.reason = str(reason)
        self.status = None if status is None else int(status) & 0xFFFFFFFF
        suffix = "" if self.status is None else f" (SECURITY_STATUS 0x{self.status:08X})"
        super().__init__(
            f"possession key requires audited administrator recovery: {self.reason}{suffix}"
        )

    def public_state(self) -> dict[str, Any]:
        return {
            "action": self.recovery_action,
            "scope": self.scope,
            "reason": self.reason,
            "status": None if self.status is None else f"0x{self.status:08X}",
        }


@dataclass(frozen=True)
class KeyDescriptor:
    contract_version: str
    provider_name: str
    key_name: str
    scope: str
    unique_name: str
    created: bool
    public_jwk: dict[str, str]
    fingerprint: str
    machine_key: bool
    export_policy: int
    key_usage: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "contract_version": self.contract_version,
            "provider_name": self.provider_name,
            "key_name": self.key_name,
            "scope": self.scope,
            "unique_name": self.unique_name,
            "created": self.created,
            "public_jwk": dict(self.public_jwk),
            "fingerprint": self.fingerprint,
            "machine_key": self.machine_key,
            "export_policy": self.export_policy,
            "key_usage": self.key_usage,
        }


@dataclass(frozen=True)
class NonExportabilityProof:
    export_policy: int
    private_export_status: int

    @property
    def private_export_status_hex(self) -> str:
        return f"0x{self.private_export_status:08X}"

    def as_dict(self) -> dict[str, Any]:
        return {
            "export_policy": self.export_policy,
            "export_policy_hex": f"0x{self.export_policy:08X}",
            "private_export_status": self.private_export_status_hex,
        }


if os.name == "nt":
    _NCRYPT_HANDLE = ctypes.c_void_p
    _ncrypt = ctypes.WinDLL("ncrypt.dll", use_last_error=True)

    _ncrypt.NCryptOpenStorageProvider.argtypes = (
        ctypes.POINTER(_NCRYPT_HANDLE),
        wintypes.LPCWSTR,
        wintypes.DWORD,
    )
    _ncrypt.NCryptOpenStorageProvider.restype = wintypes.LONG
    _ncrypt.NCryptOpenKey.argtypes = (
        _NCRYPT_HANDLE,
        ctypes.POINTER(_NCRYPT_HANDLE),
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
    )
    _ncrypt.NCryptOpenKey.restype = wintypes.LONG
    _ncrypt.NCryptCreatePersistedKey.argtypes = (
        _NCRYPT_HANDLE,
        ctypes.POINTER(_NCRYPT_HANDLE),
        wintypes.LPCWSTR,
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
    )
    _ncrypt.NCryptCreatePersistedKey.restype = wintypes.LONG
    _ncrypt.NCryptSetProperty.argtypes = (
        _NCRYPT_HANDLE,
        wintypes.LPCWSTR,
        ctypes.c_void_p,
        wintypes.DWORD,
        wintypes.DWORD,
    )
    _ncrypt.NCryptSetProperty.restype = wintypes.LONG
    _ncrypt.NCryptGetProperty.argtypes = (
        _NCRYPT_HANDLE,
        wintypes.LPCWSTR,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
        wintypes.DWORD,
    )
    _ncrypt.NCryptGetProperty.restype = wintypes.LONG
    _ncrypt.NCryptFinalizeKey.argtypes = (_NCRYPT_HANDLE, wintypes.DWORD)
    _ncrypt.NCryptFinalizeKey.restype = wintypes.LONG
    _ncrypt.NCryptExportKey.argtypes = (
        _NCRYPT_HANDLE,
        _NCRYPT_HANDLE,
        wintypes.LPCWSTR,
        ctypes.c_void_p,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
        wintypes.DWORD,
    )
    _ncrypt.NCryptExportKey.restype = wintypes.LONG
    _ncrypt.NCryptSignHash.argtypes = (
        _NCRYPT_HANDLE,
        ctypes.c_void_p,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
        wintypes.DWORD,
    )
    _ncrypt.NCryptSignHash.restype = wintypes.LONG
    _ncrypt.NCryptDeleteKey.argtypes = (_NCRYPT_HANDLE, wintypes.DWORD)
    _ncrypt.NCryptDeleteKey.restype = wintypes.LONG
    _ncrypt.NCryptFreeObject.argtypes = (_NCRYPT_HANDLE,)
    _ncrypt.NCryptFreeObject.restype = wintypes.LONG


def security_status(value: int) -> int:
    return int(value) & 0xFFFFFFFF


def _require_windows() -> None:
    if os.name != "nt":
        raise OSError("persistent NCrypt possession keys require Windows")


def _require_status(value: int, operation: str) -> None:
    if security_status(value) != ERROR_SUCCESS:
        raise NCryptError(operation, value)


def _validated_provider_name(value: str) -> str:
    provider_name = str(value or "")
    if provider_name not in SUPPORTED_PROVIDER_NAMES:
        raise ValueError("provider_name must be a supported Microsoft CNG KSP")
    return provider_name


def _validated_key_name(value: str) -> str:
    key_name = str(value or "")
    if (
        not 1 <= len(key_name) <= 128
        or not _KEY_NAME.fullmatch(key_name)
        or any(ord(char) > 127 for char in key_name)
    ):
        raise ValueError("key_name must be 1-128 safe ASCII characters")
    return key_name


def _validated_scope(value: str) -> str:
    scope = str(value or "")
    if scope not in SUPPORTED_KEY_SCOPES:
        raise ValueError("scope must be current_user or local_machine")
    return scope


def _scope_flag(scope: str) -> int:
    return NCRYPT_MACHINE_KEY_FLAG if scope == SCOPE_LOCAL_MACHINE else 0


def _open_provider(provider_name: str):
    _require_windows()
    provider = _NCRYPT_HANDLE()
    _require_status(
        _ncrypt.NCryptOpenStorageProvider(
            ctypes.byref(provider), provider_name, 0
        ),
        "NCryptOpenStorageProvider",
    )
    return provider


def _free_object(handle, operation: str) -> None:
    if handle:
        _require_status(_ncrypt.NCryptFreeObject(handle), operation)


def _set_dword_property(handle, name: str, value: int) -> None:
    data = wintypes.DWORD(int(value) & 0xFFFFFFFF)
    _require_status(
        _ncrypt.NCryptSetProperty(
            handle,
            name,
            ctypes.byref(data),
            ctypes.sizeof(data),
            NCRYPT_PERSIST_FLAG,
        ),
        f"NCryptSetProperty({name})",
    )


def _get_property_bytes(handle, name: str) -> bytes:
    size = wintypes.DWORD(0)
    _require_status(
        _ncrypt.NCryptGetProperty(
            handle,
            name,
            None,
            0,
            ctypes.byref(size),
            NCRYPT_SILENT_FLAG,
        ),
        f"NCryptGetProperty({name}, size)",
    )
    if not 0 < size.value <= _MAX_PROPERTY_BYTES:
        raise KeyPolicyError(f"{name} has an invalid size")
    buffer = ctypes.create_string_buffer(size.value)
    returned = wintypes.DWORD(0)
    _require_status(
        _ncrypt.NCryptGetProperty(
            handle,
            name,
            buffer,
            size.value,
            ctypes.byref(returned),
            NCRYPT_SILENT_FLAG,
        ),
        f"NCryptGetProperty({name}, data)",
    )
    if returned.value > size.value:
        raise KeyPolicyError(f"{name} returned an invalid size")
    return buffer.raw[: returned.value]


def _get_dword_property(handle, name: str) -> int:
    raw = _get_property_bytes(handle, name)
    if len(raw) != ctypes.sizeof(wintypes.DWORD):
        raise KeyPolicyError(f"{name} must be a DWORD")
    return struct.unpack("<I", raw)[0]


def _get_utf16_property(handle, name: str) -> str:
    raw = _get_property_bytes(handle, name)
    if len(raw) < 2 or len(raw) % 2:
        raise KeyPolicyError(f"{name} must be UTF-16")
    try:
        text = raw.decode("utf-16-le")
    except UnicodeDecodeError as exc:
        raise KeyPolicyError(f"{name} must be UTF-16") from exc
    if not text.endswith("\x00"):
        raise KeyPolicyError(f"{name} must be null-terminated")
    value = text.rstrip("\x00")
    if not value:
        raise KeyPolicyError(f"{name} must not be empty")
    return value


def _export_blob(handle, blob_type: str) -> bytes:
    size = wintypes.DWORD(0)
    _require_status(
        _ncrypt.NCryptExportKey(
            handle,
            None,
            blob_type,
            None,
            None,
            0,
            ctypes.byref(size),
            NCRYPT_SILENT_FLAG,
        ),
        f"NCryptExportKey({blob_type}, size)",
    )
    if not 0 < size.value <= _MAX_PROPERTY_BYTES:
        raise KeyPolicyError(f"{blob_type} has an invalid size")
    buffer = ctypes.create_string_buffer(size.value)
    returned = wintypes.DWORD(0)
    _require_status(
        _ncrypt.NCryptExportKey(
            handle,
            None,
            blob_type,
            None,
            buffer,
            size.value,
            ctypes.byref(returned),
            NCRYPT_SILENT_FLAG,
        ),
        f"NCryptExportKey({blob_type}, data)",
    )
    if returned.value > size.value:
        raise KeyPolicyError(f"{blob_type} returned an invalid size")
    return buffer.raw[: returned.value]


class PersistentPossessionKey:
    """A closeable handle to one named, scoped NCrypt P-256 key."""

    def __init__(
        self,
        provider,
        key,
        *,
        provider_name: str,
        key_name: str,
        scope: str,
        created: bool,
    ) -> None:
        self._provider = provider
        self._key = key
        self.provider_name = provider_name
        self.key_name = key_name
        self.scope = scope
        self.created = bool(created)
        self._closed = False

    @classmethod
    def open_existing(
        cls,
        key_name: str = DEFAULT_KEY_NAME,
        *,
        provider_name: str = MS_KEY_STORAGE_PROVIDER,
        scope: str = DEFAULT_KEY_SCOPE,
    ) -> "PersistentPossessionKey":
        """Open a scoped key without ever creating or replacing one."""

        provider_name = _validated_provider_name(provider_name)
        key_name = _validated_key_name(key_name)
        scope = _validated_scope(scope)
        provider = _open_provider(provider_name)
        key = _NCRYPT_HANDLE()
        status = security_status(
            _ncrypt.NCryptOpenKey(
                provider,
                ctypes.byref(key),
                key_name,
                0,
                _scope_flag(scope) | NCRYPT_SILENT_FLAG,
            )
        )
        if status != ERROR_SUCCESS:
            _free_object(provider, "NCryptFreeObject(provider)")
            raise AdminRecoveryRequired(
                key_name,
                provider_name,
                scope,
                "possession key is missing or cannot be opened",
                status=status,
            )
        instance = cls(
            provider,
            key,
            provider_name=provider_name,
            key_name=key_name,
            scope=scope,
            created=False,
        )
        return instance._validated_or_recovery()

    @classmethod
    def provision_initial(
        cls,
        key_name: str = DEFAULT_KEY_NAME,
        *,
        provider_name: str = MS_KEY_STORAGE_PROVIDER,
        scope: str = DEFAULT_KEY_SCOPE,
    ) -> "PersistentPossessionKey":
        """Idempotently create/open the key for confirmed first enrollment.

        This method never overwrites a named key and never repairs an unreadable
        container.  Applications must not call it as fallback from
        :meth:`open_existing`; key loss is an audited server recovery event.
        """

        provider_name = _validated_provider_name(provider_name)
        key_name = _validated_key_name(key_name)
        scope = _validated_scope(scope)
        provider = _open_provider(provider_name)
        key = _NCRYPT_HANDLE()
        open_status = security_status(
            _ncrypt.NCryptOpenKey(
                provider,
                ctypes.byref(key),
                key_name,
                0,
                _scope_flag(scope) | NCRYPT_SILENT_FLAG,
            )
        )
        if open_status == ERROR_SUCCESS:
            return cls(
                provider,
                key,
                provider_name=provider_name,
                key_name=key_name,
                scope=scope,
                created=False,
            )._validated_or_recovery()
        if open_status != NTE_BAD_KEYSET:
            _free_object(provider, "NCryptFreeObject(provider)")
            raise AdminRecoveryRequired(
                key_name,
                provider_name,
                scope,
                "existing possession key cannot be opened",
                status=open_status,
            )

        create_status = security_status(
            _ncrypt.NCryptCreatePersistedKey(
                provider,
                ctypes.byref(key),
                BCRYPT_ECDSA_P256_ALGORITHM,
                key_name,
                0,
                _scope_flag(scope),
            )
        )
        if create_status == NTE_EXISTS:
            # A concurrent first-enrollment process may have won the race.  A
            # corrupt container can produce the same open/create status pair;
            # only a successful, policy-valid reopen is accepted.
            key = _NCRYPT_HANDLE()
            reopen_status = security_status(
                _ncrypt.NCryptOpenKey(
                    provider,
                    ctypes.byref(key),
                    key_name,
                    0,
                    _scope_flag(scope) | NCRYPT_SILENT_FLAG,
                )
            )
            if reopen_status == ERROR_SUCCESS:
                return cls(
                    provider,
                    key,
                    provider_name=provider_name,
                    key_name=key_name,
                    scope=scope,
                    created=False,
                )._validated_or_recovery()
            _free_object(provider, "NCryptFreeObject(provider)")
            raise AdminRecoveryRequired(
                key_name,
                provider_name,
                scope,
                "named key container exists but is not usable",
                status=reopen_status,
            )
        if create_status != ERROR_SUCCESS:
            _free_object(provider, "NCryptFreeObject(provider)")
            raise NCryptError("NCryptCreatePersistedKey", create_status)

        try:
            # Zero is an explicit no-export/no-archiving policy.  No
            # NCRYPT_ALLOW_* export flag is ever set.
            _set_dword_property(key, NCRYPT_EXPORT_POLICY_PROPERTY, 0)
            _set_dword_property(
                key, NCRYPT_KEY_USAGE_PROPERTY, NCRYPT_ALLOW_SIGNING_FLAG
            )
            _require_status(
                _ncrypt.NCryptFinalizeKey(key, NCRYPT_SILENT_FLAG),
                "NCryptFinalizeKey",
            )
        except Exception:
            delete_status = security_status(
                _ncrypt.NCryptDeleteKey(key, NCRYPT_SILENT_FLAG)
            )
            if delete_status != ERROR_SUCCESS:
                _free_object(key, "NCryptFreeObject(key)")
            _free_object(provider, "NCryptFreeObject(provider)")
            raise

        instance = cls(
            provider,
            key,
            provider_name=provider_name,
            key_name=key_name,
            scope=scope,
            created=True,
        )
        try:
            instance._enforce_policy()
        except Exception:
            # This key was created by this call and has never been exposed to
            # the caller; removing it is the atomic provisioning rollback.
            instance._delete_new_key()
            raise
        return instance

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("possession key is closed")

    def _validated_or_recovery(self) -> "PersistentPossessionKey":
        try:
            self._enforce_policy()
            return self
        except (NCryptError, KeyPolicyError) as exc:
            status = exc.status if isinstance(exc, NCryptError) else None
            reason = (
                exc.operation if isinstance(exc, NCryptError) else str(exc)
            )
            self.close()
            raise AdminRecoveryRequired(
                self.key_name,
                self.provider_name,
                self.scope,
                reason,
                status=status,
            ) from exc

    def _enforce_policy(self) -> None:
        self._ensure_open()
        algorithm = _get_utf16_property(self._key, NCRYPT_ALGORITHM_PROPERTY)
        key_type = _get_dword_property(self._key, NCRYPT_KEY_TYPE_PROPERTY)
        export_policy = _get_dword_property(
            self._key, NCRYPT_EXPORT_POLICY_PROPERTY
        )
        key_usage = _get_dword_property(self._key, NCRYPT_KEY_USAGE_PROPERTY)
        if algorithm != BCRYPT_ECDSA_P256_ALGORITHM:
            raise KeyPolicyError("possession key algorithm is not ECDSA_P256")
        actual_machine_scope = bool(key_type & NCRYPT_MACHINE_KEY_FLAG)
        expected_machine_scope = self.scope == SCOPE_LOCAL_MACHINE
        if actual_machine_scope != expected_machine_scope:
            raise KeyPolicyError("key storage scope does not match the requested scope")
        if export_policy != 0:
            raise KeyPolicyError("private-key export or archiving is allowed")
        if key_usage != NCRYPT_ALLOW_SIGNING_FLAG:
            raise KeyPolicyError("key usage is not signing-only")

    def __enter__(self) -> "PersistentPossessionKey":
        self._ensure_open()
        return self

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        key_error: Exception | None = None
        try:
            _free_object(self._key, "NCryptFreeObject(key)")
        except Exception as exc:  # pragma: no cover - provider failure path
            key_error = exc
        finally:
            self._key = _NCRYPT_HANDLE()
        try:
            _free_object(self._provider, "NCryptFreeObject(provider)")
        finally:
            self._provider = _NCRYPT_HANDLE()
            self._closed = True
        if key_error is not None:
            raise key_error

    def _delete_new_key(self) -> None:
        self._ensure_open()
        _require_status(
            _ncrypt.NCryptDeleteKey(self._key, NCRYPT_SILENT_FLAG),
            "NCryptDeleteKey(provisioning rollback)",
        )
        self._key = _NCRYPT_HANDLE()
        _free_object(self._provider, "NCryptFreeObject(provider)")
        self._provider = _NCRYPT_HANDLE()
        self._closed = True

    @property
    def unique_name(self) -> str:
        self._ensure_open()
        return _get_utf16_property(self._key, NCRYPT_UNIQUE_NAME_PROPERTY)

    @property
    def public_jwk(self) -> dict[str, str]:
        self._ensure_open()
        blob = _export_blob(self._key, BCRYPT_ECCPUBLIC_BLOB)
        if len(blob) != 8 + (2 * P256_BYTES):
            raise KeyPolicyError("public P-256 blob length is invalid")
        magic, key_bytes = struct.unpack("<II", blob[:8])
        if (
            magic != BCRYPT_ECDSA_PUBLIC_P256_MAGIC
            or key_bytes != P256_BYTES
        ):
            raise KeyPolicyError("public P-256 blob header is invalid")
        return {
            "kty": "EC",
            "crv": "P-256",
            "x": b64url_encode(blob[8:40]),
            "y": b64url_encode(blob[40:72]),
        }

    @property
    def fingerprint(self) -> str:
        return jwk_thumbprint(self.public_jwk)

    def descriptor(self) -> KeyDescriptor:
        self._ensure_open()
        public_jwk = self.public_jwk
        export_policy = _get_dword_property(
            self._key, NCRYPT_EXPORT_POLICY_PROPERTY
        )
        key_type = _get_dword_property(self._key, NCRYPT_KEY_TYPE_PROPERTY)
        key_usage = _get_dword_property(self._key, NCRYPT_KEY_USAGE_PROPERTY)
        return KeyDescriptor(
            contract_version=POSSESSION_KEY_CONTRACT_VERSION,
            provider_name=self.provider_name,
            key_name=self.key_name,
            scope=self.scope,
            unique_name=self.unique_name,
            created=self.created,
            public_jwk=public_jwk,
            fingerprint=jwk_thumbprint(public_jwk),
            machine_key=bool(key_type & NCRYPT_MACHINE_KEY_FLAG),
            export_policy=export_policy,
            key_usage=key_usage,
        )

    def assert_non_exportable(self) -> NonExportabilityProof:
        """Prove policy zero and a provider denial for private-blob export."""

        self._ensure_open()
        export_policy = _get_dword_property(
            self._key, NCRYPT_EXPORT_POLICY_PROPERTY
        )
        if export_policy != 0:
            raise ExportPolicyViolation("private-key export policy is nonzero")
        size = wintypes.DWORD(0)
        status = security_status(
            _ncrypt.NCryptExportKey(
                self._key,
                None,
                BCRYPT_ECCPRIVATE_BLOB,
                None,
                None,
                0,
                ctypes.byref(size),
                NCRYPT_SILENT_FLAG,
            )
        )
        if status == ERROR_SUCCESS:
            # No output buffer was supplied, so no private bytes were returned.
            raise ExportPolicyViolation(
                "private-key export size query unexpectedly succeeded"
            )
        if status not in {NTE_PERM, NTE_NOT_SUPPORTED}:
            raise NCryptError("NCryptExportKey(ECCPRIVATEBLOB)", status)
        return NonExportabilityProof(
            export_policy=export_policy,
            private_export_status=status,
        )

    def sign_digest(self, digest: bytes) -> bytes:
        self._ensure_open()
        if not isinstance(digest, bytes) or len(digest) != 32:
            raise ValueError("ES256 requires a 32-byte SHA-256 digest")
        digest_buffer = ctypes.create_string_buffer(digest)
        size = wintypes.DWORD(0)
        _require_status(
            _ncrypt.NCryptSignHash(
                self._key,
                None,
                digest_buffer,
                len(digest),
                None,
                0,
                ctypes.byref(size),
                NCRYPT_SILENT_FLAG,
            ),
            "NCryptSignHash(size)",
        )
        if size.value != 2 * P256_BYTES:
            raise KeyPolicyError("NCrypt ECDSA signature length is not 64 bytes")
        buffer = ctypes.create_string_buffer(size.value)
        returned = wintypes.DWORD(0)
        _require_status(
            _ncrypt.NCryptSignHash(
                self._key,
                None,
                digest_buffer,
                len(digest),
                buffer,
                size.value,
                ctypes.byref(returned),
                NCRYPT_SILENT_FLAG,
            ),
            "NCryptSignHash(data)",
        )
        raw = buffer.raw[: returned.value]
        if len(raw) != 2 * P256_BYTES:
            raise KeyPolicyError("NCrypt ECDSA signature length is not 64 bytes")
        r = int.from_bytes(raw[:P256_BYTES], "big")
        s = int.from_bytes(raw[P256_BYTES:], "big")
        if not 1 <= r < P256_ORDER or not 1 <= s < P256_ORDER:
            raise KeyPolicyError("NCrypt ECDSA signature component is out of range")
        if s > P256_ORDER // 2:
            s = P256_ORDER - s
        return r.to_bytes(P256_BYTES, "big") + s.to_bytes(P256_BYTES, "big")

    def sign_es256(self, value: bytes) -> bytes:
        return self.sign_digest(hashlib.sha256(bytes(value)).digest())

    def sign_reattach_proof(self, proof: Mapping[str, Any]) -> str:
        validated = validated_reattach_proof(proof)
        return b64url_encode(self.sign_es256(canonical_json_bytes(validated)))


def _required_proof_text(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ReattachProofError(f"{field} must be a string")
    if (
        not value
        or value != value.strip()
        or len(value.encode("utf-8")) > 1024
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ReattachProofError(f"{field} is invalid")
    return value


def validated_reattach_proof(value: Mapping[str, Any]) -> dict[str, str]:
    """Validate the exact ec3eda4 proof object before signing it."""

    if not isinstance(value, Mapping) or set(value) != REATTACH_PROOF_FIELDS:
        raise ReattachProofError("re-attach proof fields are not exact")
    proof = {
        field: _required_proof_text(value.get(field), field)
        for field in REATTACH_PROOF_FIELDS
    }
    if proof["contract_version"] != REATTACH_PROOF_CONTRACT_VERSION:
        raise ReattachProofError("re-attach proof contract version is invalid")
    if proof["audience"] != REATTACH_AUDIENCE:
        raise ReattachProofError("re-attach proof audience is invalid")
    if not _LOWER_HEX_64.fullmatch(proof["manifest_hash"]):
        raise ReattachProofError("manifest_hash must be lowercase SHA-256 hex")
    try:
        datetime.strptime(proof["expires_at"], _STRICT_UTC)
    except ValueError as exc:
        raise ReattachProofError("expires_at must be strict UTC") from exc
    return proof


__all__ = [
    "ADMIN_RECOVERY_ACTION",
    "AdminRecoveryRequired",
    "DEFAULT_KEY_NAME",
    "DEFAULT_KEY_SCOPE",
    "ExportPolicyViolation",
    "KeyDescriptor",
    "KeyPolicyError",
    "MS_KEY_STORAGE_PROVIDER",
    "MS_PLATFORM_CRYPTO_PROVIDER",
    "NCryptError",
    "NonExportabilityProof",
    "POSSESSION_KEY_CONTRACT_VERSION",
    "PersistentPossessionKey",
    "REATTACH_AUDIENCE",
    "REATTACH_CANONICAL_KEY_ORDER",
    "REATTACH_PROOF_CONTRACT_VERSION",
    "REATTACH_PROOF_FIELDS",
    "ReattachProofError",
    "SCOPE_CURRENT_USER",
    "SCOPE_LOCAL_MACHINE",
    "security_status",
    "validated_reattach_proof",
]

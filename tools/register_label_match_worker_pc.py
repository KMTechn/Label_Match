#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Register this PC as a self-enrolled Label_Match HTTPS producer."""

from __future__ import annotations

import argparse
import ctypes
from ctypes import wintypes
import datetime as _dt
import hashlib
import json
import os
import re
import socket
import sys
import uuid
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import requests  # noqa: E402

from enrollment_mutex import (  # noqa: E402
    DEFAULT_ENROLLMENT_MUTEX_TIMEOUT_SECONDS,
    EnrollmentMutex,
    EnrollmentMutexError,
    require_enrollment_mutex_owned,
)
from kmtech_zero_pe import (  # noqa: E402
    ADMIN_RECOVERY_ACTION,
    AdminRecoveryRequired,
    POSSESSION_KEY_CONTRACT_VERSION,
    PersistentPossessionKey,
    SCOPE_CURRENT_USER,
    b64url_encode,
    canonical_json_bytes,
)
from kmtech_factory_contracts.bundle import (  # noqa: E402
    load_contract_document,
    verify_bundled_contracts,
)

from direct_sync_push import (  # noqa: E402
    DEFAULT_ENDPOINT_PATH,
    DEFAULT_PRODUCER_ROLE,
    DEFAULT_SOURCE_SYSTEM,
    DEFAULT_SOURCE_TRANSPORT,
    DEFAULT_STREAM_NAME,
    DirectSyncPushError,
    manifest_hash,
    validate_endpoint_url,
)
from logistics_runtime_profile import (  # noqa: E402
    DEFAULT_TOKEN_REF as LOGISTICS_TOKEN_REF,
    assert_path_has_no_reparse_components,
)
from tools.install_logistics_runtime_profile import (  # noqa: E402
    TLS_CA_BUNDLE_RELATIVE_PATH,
    ensure_runtime_profile_from_enrollment_bundle,
)


DEFAULT_SERVER_BASE_URL = "https://worker.kmtecherp.com"
DEFAULT_LABEL_MATCH_DATA_ROOT = r"C:\ProgramData\KMTech\Label_Match\data"
DEFAULT_DIRECT_SYNC_ROOT = r"C:\ProgramData\KMTech\DirectSync\label_match"
DEFAULT_ENROLLMENT_TOKEN_ENV = "PRODUCER_SELF_ENROLL_TOKEN"
DEFAULT_CREDENTIAL_FILENAME = "credential.json"
DEFAULT_MANIFEST_FILENAME = "producer_manifest.json"
DEFAULT_RECEIPT_FILENAME = "producer_self_enrollment_receipt.json"
DEFAULT_REPORT_FILENAME = "label_match_worker_pc_registration.json"
ENROLLMENT_CONTRACT_VERSION = "producer-self-enrollment-v2"
ENROLLMENT_PATH = "/api/producer-ingest/v2/enroll"
ADMIN_RECOVERY_AUTHORIZATION_CONTRACT_VERSION = (
    "producer-admin-recovery-authorization-v1"
)
ADMIN_RECOVERY_PROOF_CONTRACT_VERSION = "producer-admin-recovery-proof-v1"
ADMIN_RECOVERY_COMPLETE_CONTRACT_VERSION = "producer-admin-recovery-complete-v1"
ADMIN_RECOVERY_AUDIENCE = "worker-analysis-producer-admin-recovery-v1"
ADMIN_RECOVERY_PATH = "/api/producer-ingest/v2/recover"
ADMIN_RECOVERY_AUTHORIZATION_STATES = frozenset(
    {"LOGISTICS_READY", "OPERATION_PENDING"}
)
POSSESSION_KEY_SCOPE = SCOPE_CURRENT_USER
CRYPTPROTECT_LOCAL_MACHINE = 0x4
CRYPTPROTECT_UI_FORBIDDEN = 0x1
LABEL_MATCH_APP = "LabelMatch"
SAFE_TOKEN_RE = re.compile(r"[^A-Za-z0-9._-]+")
PRODUCER_IDENTITY_SCHEMA_VERSION = "label-match-producer-identity-v1"
PRODUCER_IDENTITY_FILENAME = "producer_identity.json"
PRODUCER_IDENTITY_REQUIRED_FIELDS = (
    "producer_id",
    "source_host_id",
    "producer_install_id",
)
PRODUCER_IDENTITY_MAX_BYTES = 64 * 1024
INSTALL_IDENTITY_DERIVATION_VERSION = "label-match-install-identity-v1"
INSTALL_IDENTITY_APP_ID = "label_match"
INSTALL_IDENTITY_HASH_HEX_LENGTH = 32


class ProducerEnrollmentHTTPError(DirectSyncPushError):
    """A structured server rejection from the enrollment endpoint."""

    def __init__(self, status_code: int, error_code: str, message: str) -> None:
        self.status_code = int(status_code)
        self.error_code = str(error_code or status_code)
        self.server_message = str(message or "").strip()
        detail = f" ({self.server_message})" if self.server_message else ""
        super().__init__(f"self-enroll failed: {self.error_code}{detail}")


class PossessionKeyRecoveryRequired(DirectSyncPushError):
    """An existing producer identity cannot silently receive a replacement key."""

    def __init__(self, identity_source: str, key_state: Mapping[str, Any]) -> None:
        self.identity_source = str(identity_source or "unknown")
        self.key_state = dict(key_state)
        super().__init__(
            "existing producer identity requires audited administrator recovery; "
            "automatic possession-key replacement is forbidden"
        )


class _AdminRecoveryProgress:
    """Record the irreversible recovery boundary without retaining secrets."""

    def __init__(self) -> None:
        self.server_credential_rotated = False
        self.logistics_credential_finalized = False
        self.producer_credential_finalized = False
        self.local_documents_finalized = False
        self.authorization_file_deleted = False

    def redacted_summary(self) -> dict[str, bool]:
        return {
            "server_credential_rotated": self.server_credential_rotated,
            "logistics_credential_finalized": self.logistics_credential_finalized,
            "producer_credential_finalized": self.producer_credential_finalized,
            "local_documents_finalized": self.local_documents_finalized,
            "authorization_file_deleted": self.authorization_file_deleted,
        }


def _canonical_raw_event_names() -> list[str]:
    verify_bundled_contracts()
    catalog = load_contract_document("catalogs/canonical-stream-catalog.json")
    matches = [
        row
        for row in catalog.get("streams", [])
        if isinstance(row, dict)
        and row.get("app_id") == "label_match"
        and row.get("stream_id") == DEFAULT_STREAM_NAME
        and row.get("source_system") == DEFAULT_SOURCE_SYSTEM
        and row.get("source_transport") == DEFAULT_SOURCE_TRANSPORT
    ]
    if len(matches) != 1:
        raise RuntimeError("canonical Label_Match stream catalog row is unavailable")
    values = matches[0].get("raw_event_names")
    if (
        not isinstance(values, list)
        or not values
        or not all(isinstance(value, str) and value for value in values)
        or len(set(values)) != len(values)
    ):
        raise RuntimeError("canonical Label_Match raw-event catalog is invalid")
    return list(values)


RAW_EVENT_NAMES = _canonical_raw_event_names()


def _utc_now_text() -> str:
    return _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_json(path: str | os.PathLike[str], payload: Mapping[str, Any]) -> None:
    target = assert_path_has_no_reparse_components(path, label="JSON output")
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target.with_name(f"{target.name}.tmp.{os.getpid()}.{uuid.uuid4().hex}")
    try:
        with temp_path.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(dict(payload), handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, target)
    except Exception:
        try:
            temp_path.unlink()
        except OSError:
            pass
        raise


def _safe_token(value: str, fallback: str) -> str:
    text = str(value or "").strip() or fallback
    text = SAFE_TOKEN_RE.sub("-", text).strip(".-_")
    return (text or fallback)[:96].strip(".-_") or fallback


def _normalize_machine_guid(value: str) -> str:
    try:
        return str(uuid.UUID(str(value or "").strip().strip("{}"))).lower()
    except (AttributeError, ValueError) as exc:
        raise DirectSyncPushError("Windows MachineGuid is unavailable or invalid") from exc


def _normalize_user_sid(value: str) -> str:
    normalized = str(value or "").strip().upper()
    parts = normalized.split("-")
    if (
        len(parts) < 4
        or parts[0] != "S"
        or parts[1] != "1"
        or any(not part.isdigit() for part in parts[2:])
    ):
        raise DirectSyncPushError("current Windows user SID is unavailable or invalid")
    return normalized


def _normalize_install_identity_app_id(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if not re.fullmatch(r"[a-z][a-z0-9_-]{1,63}", normalized):
        raise DirectSyncPushError("install identity app_id is invalid")
    return normalized


def _current_machine_guid() -> str:
    if os.name != "nt":
        raise DirectSyncPushError("Windows MachineGuid lookup is only available on Windows")
    try:
        import winreg

        access = winreg.KEY_READ | getattr(winreg, "KEY_WOW64_64KEY", 0)
        with winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE,
            r"SOFTWARE\Microsoft\Cryptography",
            access=access,
        ) as key:
            value, value_type = winreg.QueryValueEx(key, "MachineGuid")
    except (OSError, ImportError) as exc:
        raise DirectSyncPushError("Windows MachineGuid lookup failed") from exc
    if value_type not in {winreg.REG_SZ, winreg.REG_EXPAND_SZ}:
        raise DirectSyncPushError("Windows MachineGuid registry type is invalid")
    return _normalize_machine_guid(value)


def _current_user_sid() -> str:
    if os.name != "nt":
        raise DirectSyncPushError("Windows user SID lookup is only available on Windows")

    class _SidAndAttributes(ctypes.Structure):
        _fields_ = [("sid", ctypes.c_void_p), ("attributes", wintypes.DWORD)]

    class _TokenUser(ctypes.Structure):
        _fields_ = [("user", _SidAndAttributes)]

    advapi32 = ctypes.WinDLL("advapi32", use_last_error=True)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    advapi32.OpenProcessToken.argtypes = (
        wintypes.HANDLE,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.HANDLE),
    )
    advapi32.OpenProcessToken.restype = wintypes.BOOL
    advapi32.GetTokenInformation.argtypes = (
        wintypes.HANDLE,
        ctypes.c_uint,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.POINTER(wintypes.DWORD),
    )
    advapi32.GetTokenInformation.restype = wintypes.BOOL
    advapi32.ConvertSidToStringSidW.argtypes = (
        ctypes.c_void_p,
        ctypes.POINTER(wintypes.LPWSTR),
    )
    advapi32.ConvertSidToStringSidW.restype = wintypes.BOOL
    kernel32.GetCurrentProcess.argtypes = ()
    kernel32.GetCurrentProcess.restype = wintypes.HANDLE
    kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
    kernel32.CloseHandle.restype = wintypes.BOOL
    kernel32.LocalFree.argtypes = (ctypes.c_void_p,)
    kernel32.LocalFree.restype = ctypes.c_void_p

    token = wintypes.HANDLE()
    if not advapi32.OpenProcessToken(
        kernel32.GetCurrentProcess(), 0x0008, ctypes.byref(token)
    ):
        raise DirectSyncPushError("current Windows user token lookup failed")
    try:
        required = wintypes.DWORD()
        advapi32.GetTokenInformation(token, 1, None, 0, ctypes.byref(required))
        if required.value <= 0:
            raise DirectSyncPushError("current Windows user SID size lookup failed")
        token_buffer = ctypes.create_string_buffer(required.value)
        if not advapi32.GetTokenInformation(
            token,
            1,
            token_buffer,
            required.value,
            ctypes.byref(required),
        ):
            raise DirectSyncPushError("current Windows user SID lookup failed")
        token_user = ctypes.cast(token_buffer, ctypes.POINTER(_TokenUser)).contents
        sid_text = wintypes.LPWSTR()
        if not advapi32.ConvertSidToStringSidW(
            token_user.user.sid, ctypes.byref(sid_text)
        ):
            raise DirectSyncPushError("current Windows user SID conversion failed")
        try:
            return _normalize_user_sid(sid_text.value)
        finally:
            kernel32.LocalFree(ctypes.cast(sid_text, ctypes.c_void_p))
    finally:
        kernel32.CloseHandle(token)


def derive_path_independent_install_id(
    *,
    machine_guid: str,
    user_sid: str,
    app_id: str = INSTALL_IDENTITY_APP_ID,
) -> str:
    """Derive a lookup identity, not possession proof, without path inputs."""

    canonical = {
        "app_id": _normalize_install_identity_app_id(app_id),
        "machine_guid": _normalize_machine_guid(machine_guid),
        "user_sid": _normalize_user_sid(user_sid),
        "version": INSTALL_IDENTITY_DERIVATION_VERSION,
    }
    seed = json.dumps(
        canonical,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    digest = hashlib.sha256(seed).hexdigest()[:INSTALL_IDENTITY_HASH_HEX_LENGTH]
    return f"label-match-install-{digest}"


def _machine_identity(args: argparse.Namespace) -> str:
    override = str(getattr(args, "machine_guid", "") or "").strip()
    return _normalize_machine_guid(override) if override else _current_machine_guid()


def _join_url(base_url: str, path: str) -> str:
    base = str(base_url or "").strip().rstrip("/")
    suffix = path if path.startswith("/") else f"/{path}"
    return f"{base}{suffix}"


def _endpoint_from_args(args: argparse.Namespace) -> str:
    endpoint = str(args.endpoint_url or "").strip()
    if endpoint:
        validate_endpoint_url(endpoint)
        return endpoint
    base_url = str(args.server_base_url or DEFAULT_SERVER_BASE_URL).strip()
    endpoint = _join_url(base_url, DEFAULT_ENDPOINT_PATH)
    validate_endpoint_url(endpoint)
    return endpoint


def _health_url_from_endpoint(endpoint_url: str) -> str:
    parsed = urlparse(endpoint_url)
    return f"{parsed.scheme}://{parsed.netloc}/health/ingest"


def _enrollment_url_from_endpoint(endpoint_url: str) -> str:
    parsed = urlparse(endpoint_url)
    return f"{parsed.scheme}://{parsed.netloc}{ENROLLMENT_PATH}"


def _admin_recovery_url_from_endpoint(endpoint_url: str) -> str:
    parsed = urlparse(endpoint_url)
    return f"{parsed.scheme}://{parsed.netloc}{ADMIN_RECOVERY_PATH}"


def _validate_admin_recovery_url(recovery_url: str, endpoint_url: str) -> str:
    validate_endpoint_url(endpoint_url)
    parsed_endpoint = urlparse(endpoint_url)
    parsed_recovery = urlparse(str(recovery_url or "").strip())
    if (
        parsed_recovery.scheme != "https"
        or parsed_recovery.netloc != parsed_endpoint.netloc
        or parsed_recovery.username
        or parsed_recovery.password
        or parsed_recovery.params
        or parsed_recovery.query
        or parsed_recovery.fragment
        or parsed_recovery.path != ADMIN_RECOVERY_PATH
    ):
        raise DirectSyncPushError(
            "admin_recovery_url must be HTTPS, same-origin, and use "
            f"{ADMIN_RECOVERY_PATH}"
        )
    return parsed_recovery.geturl()


def _build_stream(source_host_id: str) -> dict[str, Any]:
    return {
        "barcode_policy": "product_barcode_primary",
        "conflict_file_exclusion_policy": {
            "excluded_dirs": [".stfolder"],
            "excluded_name_contains": ["sync-conflict"],
        },
        "dispatch_key_fields": ["source_system", "source_transport_or_dataset", "raw_event_name"],
        "hash_chain_required": False,
        "hmac_required": False,
        "producer_role": DEFAULT_PRODUCER_ROLE,
        "quantity_basis": "PACKAGING_SET",
        "raw_event_names": RAW_EVENT_NAMES,
        "replay_policy": {
            "conflict_without_correction": "quarantine",
            "idempotency_key": ["source_system", "event_identity"],
            "same_legacy_row_locator_different_row_hash": "append_only_correction_required",
            "same_payload_hash": "replay",
        },
        "source_file_id_policy": {
            "example": f"{source_host_id}/{DEFAULT_PRODUCER_ROLE}/{DEFAULT_STREAM_NAME}/sample.csv",
            "format": "<source_host_id>/<producer_role>/<stream_name>/<relative_path_under_stream_root>",
            "legacy_sync_wrapper_format": "<source_host_id>:<parent_hash>:<filename>",
            "legacy_sync_wrapper_status": "not_canonical_for_batch1_onboarding",
        },
        "source_lineage_fields": [
            "source_host_id",
            "source_file_id",
            "source_file_hash",
            "source_row_number",
            "source_byte_offset",
            "legacy_row_locator",
            "row_hash",
        ],
        "source_system": DEFAULT_SOURCE_SYSTEM,
        "source_transport": DEFAULT_SOURCE_TRANSPORT,
        "source_transport_or_dataset": DEFAULT_SOURCE_TRANSPORT,
        "stability_window_policy": {
            "minimum_stable_seconds": 30,
            "requires_size_and_mtime_unchanged": True,
        },
        "stream_name": DEFAULT_STREAM_NAME,
        "temp_file_exclusion_policy": {
            "excluded_prefixes": ["~", "."],
            "excluded_suffixes": [".tmp", ".partial", ".crdownload"],
        },
    }


def _build_manifest(
    *,
    pc_id: str,
    source_host_id: str,
    producer_install_id: str,
    sync_dir: str,
    data_dir: str,
    endpoint_url: str,
    secret_ref: str,
    identity_registry_status: str,
) -> dict[str, Any]:
    data_root = Path(data_dir).expanduser().resolve()
    sync_root = Path(sync_dir).expanduser().resolve()
    data_root_text = data_root.as_posix()
    sync_root_text = sync_root.as_posix()
    return {
        "apps": [LABEL_MATCH_APP],
        "hmac_gate": {
            "decision": "not_required",
            "fixture_verifier_status": "not_required",
            "hash_chain_status": "not_required",
            "key_fingerprint": None,
            "registry_status": "not_required",
            "required": False,
            "row_verifier_code_hash": None,
            "row_verifier_evidence_hash": None,
            "row_verifier_id": None,
            "row_verifier_receipt_hash": None,
            "row_verifier_status": "not_required",
        },
        "identity_registry": {
            "required_for_pass": True,
            "source_host_id_unique": identity_registry_status in {"checked", "self_enrolled"},
            "status": identity_registry_status,
        },
        "paths": {
            "data_dir": data_root_text,
            "evidence_dir": (data_root / "evidence").as_posix(),
            "rollback_dir": (data_root / "rollback").as_posix(),
        },
        "pc_identity": {
            "pc_id": pc_id,
            "producer_install_id": producer_install_id,
            "source_host_id": source_host_id,
        },
        "plan_b_invariants": {
            "append_only_correction_required": True,
            "no_erp_write": True,
            "product_barcode_priority": True,
            "quarantine_projection_business_separated": True,
            "shipping_waiting_is_no_shipping_evidence": True,
            "source_csv_immutable": True,
        },
        "rollback": {"sync_dir_preserve": True},
        "schema_version": "producer-onboarding-manifest-v1",
        "server": {
            "contacted": False,
            "health_target": _health_url_from_endpoint(endpoint_url),
        },
        "streams": [_build_stream(source_host_id)],
        "sync": {
            "auth": {
                "method": "producer_hmac_v1",
                "secret_material_persisted": False,
                "secret_ref": secret_ref,
            },
            "fallback": {
                "sync_dir_preserved": True,
                "syncthing_folder_id_required": False,
            },
            "queue": {
                "allowed_streams": [DEFAULT_STREAM_NAME],
                "client_state_db": (data_root / "relay_state.sqlite3").as_posix(),
                "queue_dir": (data_root / "relay_queue").as_posix(),
                "status": "operator_supplied_uncontacted",
            },
            "server_ingest_target": endpoint_url,
            "status": "operator_supplied_uncontacted",
            "sync_dir": sync_root_text,
            "sync_transport": "http_push",
        },
    }


def _producer_identity_path(args: argparse.Namespace) -> Path:
    explicit = str(getattr(args, "identity_path", "") or "").strip()
    if explicit:
        return Path(explicit).expanduser()
    data_dir = Path(
        getattr(args, "data_dir", "") or DEFAULT_DIRECT_SYNC_ROOT
    ).expanduser()
    return data_dir / PRODUCER_IDENTITY_FILENAME


def _load_producer_identity_file(path: Path) -> dict[str, str]:
    try:
        size = path.stat().st_size
        if size <= 0 or size > PRODUCER_IDENTITY_MAX_BYTES:
            raise DirectSyncPushError("producer identity file size is invalid")

        def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
            value: dict[str, Any] = {}
            for key, item in pairs:
                if key in value:
                    raise DirectSyncPushError(
                        f"producer identity file contains duplicate key: {key}"
                    )
                value[key] = item
            return value

        payload = json.loads(
            path.read_bytes().decode("utf-8-sig"),
            object_pairs_hook=reject_duplicate_keys,
        )
    except DirectSyncPushError:
        raise
    except Exception as exc:
        raise DirectSyncPushError("producer identity file could not be read") from exc
    if not isinstance(payload, dict):
        raise DirectSyncPushError("producer identity file must be a JSON object")
    if str(payload.get("schema_version") or "").strip() != PRODUCER_IDENTITY_SCHEMA_VERSION:
        raise DirectSyncPushError("producer identity file schema_version is invalid")
    identity: dict[str, str] = {}
    for field in PRODUCER_IDENTITY_REQUIRED_FIELDS:
        value = str(payload.get(field) or "").strip()
        if not value:
            raise DirectSyncPushError(f"producer identity file missing {field}")
        identity[field] = value
    pc_id = str(payload.get("pc_id") or "").strip()
    if pc_id:
        identity["pc_id"] = pc_id
    return identity


def _derive_identity(args: argparse.Namespace, endpoint_url: str) -> dict[str, str]:
    identity_path = _producer_identity_path(args)
    loaded: dict[str, str] | None = None
    loaded_from = ""
    if identity_path.exists():
        if not identity_path.is_file():
            raise DirectSyncPushError("producer identity path is not a regular file")
        loaded = _load_producer_identity_file(identity_path)
        loaded_from = str(identity_path.resolve())

    pc_id = _safe_token(
        getattr(args, "pc_id", "")
        or (loaded or {}).get("pc_id")
        or socket.gethostname(),
        "worker-pc",
    )
    machine_guid: str | None = None
    cli_source_host_id = str(getattr(args, "source_host_id", "") or "").strip()
    loaded_source_host_id = str((loaded or {}).get("source_host_id") or "").strip()
    if cli_source_host_id:
        source_host_id = _safe_token(
            cli_source_host_id,
            "label-match-worker",
        ).lower()
    elif loaded_source_host_id:
        source_host_id = loaded_source_host_id
    else:
        machine_guid = _machine_identity(args)
        source_host_id = _safe_token(
            f"label-match-{pc_id}-"
            f"{hashlib.sha256(machine_guid.encode('utf-8')).hexdigest()[:12]}",
            "label-match-worker",
        ).lower()

    cli_producer_install_id = str(
        getattr(args, "producer_install_id", "") or ""
    ).strip()
    loaded_producer_install_id = str(
        (loaded or {}).get("producer_install_id") or ""
    ).strip()
    if cli_producer_install_id:
        producer_install_id = _safe_token(
            cli_producer_install_id,
            "install-label-match-worker",
        )
        producer_install_id_derivation = "cli"
    elif loaded_producer_install_id:
        producer_install_id = loaded_producer_install_id
        producer_install_id_derivation = "identity_file"
    else:
        if machine_guid is None:
            machine_guid = _machine_identity(args)
        producer_install_id = derive_path_independent_install_id(
            machine_guid=machine_guid,
            user_sid=_current_user_sid(),
        )
        producer_install_id_derivation = INSTALL_IDENTITY_DERIVATION_VERSION

    cli_producer_id = str(getattr(args, "producer_id", "") or "").strip()
    loaded_producer_id = str((loaded or {}).get("producer_id") or "").strip()
    if cli_producer_id:
        producer_id = _safe_token(
            cli_producer_id,
            "producer-label-match-worker",
        )
    elif loaded_producer_id:
        producer_id = loaded_producer_id
    else:
        producer_id = _safe_token(
            f"producer-{source_host_id}",
            "producer-label-match-worker",
        )
    if cli_source_host_id or cli_producer_install_id or cli_producer_id:
        identity_source = "cli"
    elif loaded is not None:
        identity_source = "identity_file"
    else:
        identity_source = "generated"
    return {
        "pc_id": pc_id,
        "source_host_id": source_host_id,
        "producer_install_id": producer_install_id,
        "producer_id": producer_id,
        "key_id": _safe_token(
            getattr(args, "key_id", "") or f"key-{source_host_id}",
            "key-label-match-worker",
        ),
        "secret_ref": _safe_token(
            getattr(args, "secret_ref_target", "")
            or f"producer-{source_host_id}-http-push-key",
            "producer-label-match-worker-http-push-key",
        ),
        "identity_source": identity_source,
        "identity_loaded_from": loaded_from,
        "identity_persist_path": str(identity_path),
        "producer_install_id_derivation": producer_install_id_derivation,
    }


def _token_from_sources(args: argparse.Namespace) -> tuple[str, str]:
    candidates: list[tuple[str, str]] = []
    if args.enrollment_token:
        candidates.append(("argument", args.enrollment_token.strip()))
    token_file = str(args.enrollment_token_file or "").strip()
    if token_file:
        candidates.append(("file", Path(token_file).read_text(encoding="utf-8-sig").strip()))
    if args.enrollment_token_env:
        env_value = os.getenv(args.enrollment_token_env, "")
        if env_value:
            candidates.append(("env", env_value.strip()))
    candidates = [(source, token) for source, token in candidates if token]
    if len(candidates) > 1:
        raise DirectSyncPushError("self-enroll requires exactly one enrollment token source")
    if not candidates:
        return "ip_allowlist", ""
    return candidates[0]


def _prepare_possession_key(report: Mapping[str, Any]) -> dict[str, Any]:
    """Open the pinned current-user key, creating it only for a new identity."""

    identity_source = str(report.get("producer_identity_source") or "").strip()
    try:
        if identity_source == "generated":
            key = PersistentPossessionKey.provision_initial(
                scope=POSSESSION_KEY_SCOPE
            )
        else:
            key = PersistentPossessionKey.open_existing(
                scope=POSSESSION_KEY_SCOPE
            )
    except AdminRecoveryRequired as exc:
        raise PossessionKeyRecoveryRequired(
            identity_source,
            exc.public_state(),
        ) from exc
    with key:
        descriptor = key.descriptor().as_dict()
    if descriptor.get("scope") != POSSESSION_KEY_SCOPE:
        raise DirectSyncPushError("possession key scope is not current_user")
    if descriptor.get("machine_key") is not False:
        raise DirectSyncPushError("possession key unexpectedly uses local_machine scope")
    if descriptor.get("contract_version") != POSSESSION_KEY_CONTRACT_VERSION:
        raise DirectSyncPushError("possession key contract version is invalid")
    public_jwk = descriptor.get("public_jwk")
    if not isinstance(public_jwk, dict):
        raise DirectSyncPushError("possession key public JWK is unavailable")
    return descriptor


def _enroll(
    payload: Mapping[str, Any],
    *,
    enrollment_url: str,
    enrollment_token: str,
    timeout_seconds: int,
    tls_ca_bundle_path: str = "",
) -> dict[str, Any]:
    require_enrollment_mutex_owned()
    headers = {"X-Producer-Enrollment-Token": enrollment_token} if enrollment_token else {}
    request_kwargs: dict[str, Any] = {
        "json": dict(payload),
        "headers": headers,
        "timeout": max(1, int(timeout_seconds)),
    }
    selected_ca = str(tls_ca_bundle_path or "").strip()
    if selected_ca:
        request_kwargs["verify"] = selected_ca
    response = requests.post(enrollment_url, **request_kwargs)
    try:
        response_payload = response.json()
    except ValueError as exc:
        raise DirectSyncPushError(f"self-enroll response is not JSON: HTTP {response.status_code}") from exc
    if response.status_code >= 400:
        error = response_payload.get("error") if isinstance(response_payload, dict) else {}
        code = str(error.get("code") or response.status_code) if isinstance(error, dict) else str(response.status_code)
        message = str(error.get("message") or "").strip() if isinstance(error, dict) else ""
        raise ProducerEnrollmentHTTPError(response.status_code, code, message)
    if not isinstance(response_payload, dict):
        raise DirectSyncPushError("self-enroll response must be a JSON object")
    return response_payload


def _validate_v2_enrollment_response(
    response_payload: Mapping[str, Any],
    *,
    expected_fingerprint: str,
    expected_producer_id: str,
    expected_install_id: str,
    expected_source_host_id: str,
    expected_endpoint_url: str,
    expected_manifest_hash: str,
) -> dict[str, Any]:
    """Reconcile the v2 response and durable client receipt before persistence."""

    if response_payload.get("contract_version") != ENROLLMENT_CONTRACT_VERSION:
        raise DirectSyncPushError("self-enroll response contract version mismatch")
    if response_payload.get("status") != "enrolled":
        raise DirectSyncPushError("v2 initial enrollment did not create a new identity")
    if response_payload.get("identity_action") != "CREATED":
        raise DirectSyncPushError("v2 initial enrollment identity action mismatch")
    if response_payload.get("credential_epoch") != 1:
        raise DirectSyncPushError("v2 initial enrollment credential epoch mismatch")
    authorization_state = response_payload.get("authorization_state")
    if authorization_state not in ADMIN_RECOVERY_AUTHORIZATION_STATES:
        raise DirectSyncPushError("v2 response authorization state mismatch")
    possession_key = response_payload.get("possession_key")
    if not isinstance(possession_key, dict):
        raise DirectSyncPushError("v2 response missing possession key binding")
    if possession_key.get("contract_version") != POSSESSION_KEY_CONTRACT_VERSION:
        raise DirectSyncPushError("v2 response possession key contract mismatch")
    if possession_key.get("fingerprint") != expected_fingerprint:
        raise DirectSyncPushError("v2 response possession key fingerprint mismatch")

    expected_values = {
        "producer_id": expected_producer_id,
        "producer_install_id": expected_install_id,
        "source_host_id": expected_source_host_id,
        "endpoint_url": expected_endpoint_url,
    }
    for field, expected in expected_values.items():
        if response_payload.get(field) != expected:
            raise DirectSyncPushError(f"v2 response {field} mismatch")
    if response_payload.get("active_manifest_hashes") != [expected_manifest_hash]:
        raise DirectSyncPushError("v2 response active manifest hash mismatch")

    receipt = response_payload.get("client_receipt")
    if not isinstance(receipt, dict):
        raise DirectSyncPushError("v2 response missing client receipt")
    receipt_values = {
        "contract_version": ENROLLMENT_CONTRACT_VERSION,
        "status": "enrolled",
        "identity_action": "CREATED",
        "credential_epoch": 1,
        "possession_key_fingerprint": expected_fingerprint,
        **expected_values,
    }
    for field, expected in receipt_values.items():
        if receipt.get(field) != expected:
            raise DirectSyncPushError(f"v2 client receipt {field} mismatch")
    if receipt.get("active_manifest_hashes") != [expected_manifest_hash]:
        raise DirectSyncPushError("v2 client receipt active manifest hash mismatch")
    for field in (
        "authorization_state",
        "key_id",
        "secret_fingerprint_sha256",
        "server_binding",
    ):
        if receipt.get(field) != response_payload.get(field):
            raise DirectSyncPushError(f"v2 client receipt {field} mismatch")
    return _safe_client_receipt(receipt)


class _DataBlob(ctypes.Structure):
    _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.c_void_p)]


def _dpapi_protect_machine(secret: str) -> bytes:
    if sys.platform != "win32":
        raise DirectSyncPushError("dpapi secret bootstrap requires Windows")
    from ctypes import byref

    secret_bytes = secret.encode("utf-8")
    input_buffer = ctypes.create_string_buffer(secret_bytes, len(secret_bytes))
    input_blob = _DataBlob(len(secret_bytes), ctypes.cast(input_buffer, ctypes.c_void_p))
    output_blob = _DataBlob()
    if not ctypes.windll.crypt32.CryptProtectData(
        byref(input_blob),
        None,
        None,
        None,
        None,
        CRYPTPROTECT_LOCAL_MACHINE | CRYPTPROTECT_UI_FORBIDDEN,
        byref(output_blob),
    ):
        raise DirectSyncPushError("dpapi secret bootstrap failed")
    try:
        return ctypes.string_at(output_blob.pbData, output_blob.cbData)
    finally:
        ctypes.windll.kernel32.LocalFree(ctypes.c_void_p(output_blob.pbData))


def _dpapi_protect_current_user(secret: str) -> bytes:
    if sys.platform != "win32":
        raise DirectSyncPushError("dpapi secret bootstrap requires Windows")
    from ctypes import byref

    secret_bytes = secret.encode("utf-8")
    input_buffer = ctypes.create_string_buffer(secret_bytes, len(secret_bytes))
    input_blob = _DataBlob(len(secret_bytes), ctypes.cast(input_buffer, ctypes.c_void_p))
    output_blob = _DataBlob()
    if not ctypes.windll.crypt32.CryptProtectData(
        byref(input_blob),
        None,
        None,
        None,
        None,
        CRYPTPROTECT_UI_FORBIDDEN,
        byref(output_blob),
    ):
        raise DirectSyncPushError("dpapi secret bootstrap failed")
    try:
        return ctypes.string_at(output_blob.pbData, output_blob.cbData)
    finally:
        ctypes.windll.kernel32.LocalFree(ctypes.c_void_p(output_blob.pbData))


def _dpapi_unprotect_current_user(protected: bytes) -> str:
    if sys.platform != "win32":
        raise DirectSyncPushError("dpapi secret verify requires Windows")
    from ctypes import byref

    input_buffer = ctypes.create_string_buffer(protected, len(protected))
    input_blob = _DataBlob(len(protected), ctypes.cast(input_buffer, ctypes.c_void_p))
    output_blob = _DataBlob()
    if not ctypes.windll.crypt32.CryptUnprotectData(byref(input_blob), None, None, None, None, 0, byref(output_blob)):
        raise DirectSyncPushError("dpapi secret verify failed")
    try:
        return ctypes.string_at(output_blob.pbData, output_blob.cbData).decode("utf-8")
    finally:
        ctypes.windll.kernel32.LocalFree(ctypes.c_void_p(output_blob.pbData))


def _secret_path(data_dir: str | os.PathLike[str], secret_ref_target: str) -> Path:
    return assert_path_has_no_reparse_components(
        Path(data_dir).expanduser() / "secrets" / f"{secret_ref_target}.dpapi",
        label="producer protected credential",
    )


def _write_dpapi_secret(
    data_dir: str | os.PathLike[str],
    secret_ref_target: str,
    secret: str,
    *,
    credential_scope: str = "machine",
) -> Path:
    target = _secret_path(data_dir, secret_ref_target)
    target.parent.mkdir(parents=True, exist_ok=True)
    if os.path.lexists(target) and (target.is_symlink() or not target.is_file()):
        raise DirectSyncPushError("producer credential path is not a regular file")
    selected_scope = str(credential_scope or "").strip().lower()
    if selected_scope == "current_user":
        protected = _dpapi_protect_current_user(secret)
    elif selected_scope == "machine":
        protected = _dpapi_protect_machine(secret)
    else:
        raise DirectSyncPushError("credential_scope must be machine or current_user")
    temporary = target.with_name(
        f".{os.getpid()}.{uuid.uuid4().hex}.credential.tmp"
    )
    try:
        with temporary.open("xb") as handle:
            handle.write(protected)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
    return target


def _verify_dpapi_secret(data_dir: str | os.PathLike[str], secret_ref_target: str, expected_secret: str) -> bool:
    return _dpapi_unprotect_current_user(_secret_path(data_dir, secret_ref_target).read_bytes()) == expected_secret


def _secret_from_response(response_payload: Mapping[str, Any]) -> str:
    secret = str(response_payload.get("secret") or "")
    if secret:
        return secret
    secret_hex = str(response_payload.get("secret_hex") or "").strip()
    try:
        secret = bytes.fromhex(secret_hex).decode("utf-8")
    except (ValueError, UnicodeDecodeError) as exc:
        raise DirectSyncPushError("self-enroll response missing valid secret") from exc
    if not secret:
        raise DirectSyncPushError("self-enroll response missing valid secret")
    return secret


def _fingerprint(secret: str) -> str:
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()


def _safe_server_binding(value: Any) -> dict[str, str]:
    if not isinstance(value, Mapping):
        return {}
    return {
        field: str(value[field])
        for field in ("producer_manifest_path", "registry_path")
        if isinstance(value.get(field), str) and value[field]
    }


def _safe_client_receipt(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    allowed = {
        "contract_version",
        "status",
        "identity_action",
        "recovery_action",
        "authorization_state",
        "producer_id",
        "key_id",
        "credential_epoch",
        "secret_fingerprint_sha256",
        "endpoint_url",
        "source_host_id",
        "producer_install_id",
        "active_manifest_hashes",
        "possession_key_fingerprint",
    }
    sanitized = {field: value[field] for field in allowed if field in value}
    server_binding = _safe_server_binding(value.get("server_binding"))
    if server_binding:
        sanitized["server_binding"] = server_binding
    return sanitized


def _load_json_no_duplicate_keys(raw: bytes) -> Any:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise DirectSyncPushError("JSON object contains duplicate keys")
            result[key] = value
        return result

    try:
        return json.loads(raw.decode("utf-8-sig"), object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DirectSyncPushError("JSON document is invalid") from exc


def _verify_json_file(path: Path, expected: Mapping[str, Any], *, label: str) -> None:
    try:
        size = path.stat().st_size
        if not 0 < size <= 1_048_576:
            raise DirectSyncPushError(f"{label} size is invalid")
        observed = _load_json_no_duplicate_keys(path.read_bytes())
    except OSError as exc:
        raise DirectSyncPushError(f"{label} readback failed") from exc
    if observed != dict(expected):
        raise DirectSyncPushError(f"{label} exact readback failed")


def _load_admin_recovery_authorization(
    path_value: str,
    *,
    expected_producer_id: str,
) -> tuple[Path, dict[str, Any]]:
    path = assert_path_has_no_reparse_components(
        str(path_value or ""),
        label="admin recovery authorization",
    )
    if not path.is_file():
        raise DirectSyncPushError("admin recovery authorization file is absent")
    try:
        size = path.stat().st_size
    except OSError as exc:
        raise DirectSyncPushError(
            "admin recovery authorization file is unavailable"
        ) from exc
    if size <= 0 or size > 65_536:
        raise DirectSyncPushError("admin recovery authorization file size is invalid")
    payload = _load_json_no_duplicate_keys(path.read_bytes())
    expected_fields = {
        "contract_version",
        "authorization_id",
        "producer_id",
        "recovery_token",
        "nonce",
        "expires_at",
        "audience",
        "audit_event_id",
    }
    if not isinstance(payload, dict) or set(payload) != expected_fields:
        raise DirectSyncPushError("admin recovery authorization fields are invalid")
    if (
        payload.get("contract_version")
        != ADMIN_RECOVERY_AUTHORIZATION_CONTRACT_VERSION
        or payload.get("audience") != ADMIN_RECOVERY_AUDIENCE
        or payload.get("producer_id") != expected_producer_id
    ):
        raise DirectSyncPushError(
            "admin recovery authorization identity or contract is invalid"
        )
    for field in (
        "authorization_id",
        "recovery_token",
        "nonce",
        "expires_at",
        "audit_event_id",
    ):
        if not isinstance(payload.get(field), str) or not payload[field].strip():
            raise DirectSyncPushError(
                f"admin recovery authorization {field} is invalid"
            )
    try:
        expires_at = _dt.datetime.strptime(
            payload["expires_at"], "%Y-%m-%dT%H:%M:%SZ"
        ).replace(tzinfo=_dt.timezone.utc)
    except ValueError as exc:
        raise DirectSyncPushError(
            "admin recovery authorization expiry is invalid"
        ) from exc
    if expires_at <= _dt.datetime.now(_dt.timezone.utc):
        raise DirectSyncPushError("admin recovery authorization is expired")
    return path, payload


def _validated_sha256_hex(value: str, *, label: str) -> str:
    normalized = str(value or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{64}", normalized):
        raise DirectSyncPushError(f"{label} must be lowercase SHA-256 hex")
    return normalized


def _open_admin_recovery_session(tls_ca_bundle_path: str) -> requests.Session:
    ca_path = assert_path_has_no_reparse_components(
        str(tls_ca_bundle_path or ""),
        label="admin recovery TLS CA bundle",
    )
    if not ca_path.is_file():
        raise DirectSyncPushError("admin recovery TLS CA bundle is unavailable")
    session = requests.Session()
    session.trust_env = False
    session.verify = str(ca_path)
    return session


def _validate_admin_recovery_response(
    response_payload: Mapping[str, Any],
    *,
    manifest: Mapping[str, Any],
    credential: Mapping[str, Any],
    possession_key: Mapping[str, Any],
) -> None:
    identity = manifest.get("pc_identity")
    if not isinstance(identity, Mapping):
        raise DirectSyncPushError("admin recovery candidate identity is invalid")
    candidate_manifest_hash = manifest_hash(manifest)
    authorization_state = response_payload.get("authorization_state")
    epoch = response_payload.get("credential_epoch")
    expected_values = {
        "contract_version": ADMIN_RECOVERY_COMPLETE_CONTRACT_VERSION,
        "status": "recovered",
        "identity_action": "REATTACHED",
        "recovery_action": "ADMIN_RECOVERY",
        "producer_id": str(credential["producer_id"]),
        "producer_install_id": str(identity["producer_install_id"]),
        "source_host_id": str(identity["source_host_id"]),
        "endpoint_url": str(credential["endpoint_url"]),
    }
    if any(response_payload.get(field) != value for field, value in expected_values.items()):
        raise DirectSyncPushError("admin recovery response identity contract differs")
    if (
        authorization_state not in ADMIN_RECOVERY_AUTHORIZATION_STATES
        or type(epoch) is not int
        or epoch < 2
        or response_payload.get("active_manifest_hashes")
        != [candidate_manifest_hash]
    ):
        raise DirectSyncPushError("admin recovery response authorization binding differs")
    response_possession = response_payload.get("possession_key")
    if (
        not isinstance(response_possession, Mapping)
        or response_possession.get("contract_version")
        != POSSESSION_KEY_CONTRACT_VERSION
        or response_possession.get("fingerprint")
        != possession_key.get("fingerprint")
    ):
        raise DirectSyncPushError("admin recovery response possession binding differs")
    receipt = response_payload.get("client_receipt")
    if not isinstance(receipt, Mapping):
        raise DirectSyncPushError("admin recovery response missing client receipt")
    receipt_values = {
        **expected_values,
        "authorization_state": authorization_state,
        "credential_epoch": epoch,
        "possession_key_fingerprint": possession_key.get("fingerprint"),
    }
    if any(receipt.get(field) != value for field, value in receipt_values.items()):
        raise DirectSyncPushError("admin recovery client receipt binding differs")
    if receipt.get("active_manifest_hashes") != [candidate_manifest_hash]:
        raise DirectSyncPushError("admin recovery client receipt manifest binding differs")
    for field in ("key_id", "secret_fingerprint_sha256"):
        if receipt.get(field) != response_payload.get(field):
            raise DirectSyncPushError(
                f"admin recovery client receipt {field} mismatch"
            )


def _resolved_output_path(value: str, fallback: Path) -> Path:
    selected = str(value or "").strip()
    return assert_path_has_no_reparse_components(
        Path(selected).expanduser() if selected else fallback,
        label="admin recovery output",
    )


def _preflight_admin_recovery_local_state(
    args: argparse.Namespace,
    manifest: Mapping[str, Any],
    credential: Mapping[str, Any],
    recovery_path: Path,
) -> None:
    """Prove recovery will replace an existing, non-overlapping local identity."""

    data_dir = assert_path_has_no_reparse_components(
        str(credential["secret_data_dir"]),
        label="admin recovery data directory",
    )
    profile_text = str(getattr(args, "logistics_profile_path", "") or "").strip()
    if not profile_text:
        raise DirectSyncPushError(
            "admin recovery requires an explicit existing logistics profile path"
        )
    profile_path = assert_path_has_no_reparse_components(
        profile_text,
        label="admin recovery logistics profile",
    )
    token_relative = LOGISTICS_TOKEN_REF.split(":", 1)[1].replace("/", os.sep)
    logistics_secret_path = assert_path_has_no_reparse_components(
        profile_path.parent / token_relative,
        label="admin recovery logistics protected credential",
    )
    producer_secret_path = _secret_path(
        credential["secret_data_dir"],
        str(credential["secret_ref"]).split(":", 1)[1],
    )
    manifest_path = _resolved_output_path(
        str(getattr(args, "manifest_path", "") or ""),
        data_dir / DEFAULT_MANIFEST_FILENAME,
    )
    credential_path = _resolved_output_path(
        str(getattr(args, "credential_path", "") or ""),
        data_dir / DEFAULT_CREDENTIAL_FILENAME,
    )
    receipt_path = _resolved_output_path(
        str(getattr(args, "receipt_path", "") or ""),
        data_dir / "evidence" / DEFAULT_RECEIPT_FILENAME,
    )
    report_path = _resolved_output_path(
        str(getattr(args, "report_path", "") or ""),
        data_dir / "status" / DEFAULT_REPORT_FILENAME,
    )
    identity_path = assert_path_has_no_reparse_components(
        _producer_identity_path(args),
        label="admin recovery producer identity",
    )
    tls_target = assert_path_has_no_reparse_components(
        profile_path.parent / TLS_CA_BUNDLE_RELATIVE_PATH,
        label="admin recovery logistics TLS CA target",
    )
    local_targets = [
        identity_path,
        manifest_path,
        credential_path,
        receipt_path,
        report_path,
        producer_secret_path,
        profile_path,
        logistics_secret_path,
        tls_target,
    ]
    target_keys = [os.path.normcase(str(path)) for path in local_targets]
    if len(target_keys) != len(set(target_keys)):
        raise DirectSyncPushError("admin recovery local targets must be distinct")
    recovery_key = os.path.normcase(str(recovery_path))
    ca_source = assert_path_has_no_reparse_components(
        str(getattr(args, "tls_ca_bundle_path", "") or ""),
        label="admin recovery TLS CA source",
    )
    if recovery_key in set(target_keys) or recovery_key == os.path.normcase(
        str(ca_source)
    ):
        raise DirectSyncPushError(
            "admin recovery authorization path overlaps a local target"
        )
    for path in local_targets:
        if os.path.lexists(path) and (path.is_symlink() or not path.is_file()):
            raise DirectSyncPushError("admin recovery local target is not a regular file")
    for path, label in (
        (identity_path, "producer identity"),
        (manifest_path, "producer manifest"),
        (credential_path, "producer credential reference"),
        (producer_secret_path, "producer protected credential"),
        (profile_path, "logistics profile"),
        (logistics_secret_path, "logistics protected credential"),
        (tls_target, "logistics TLS CA bundle"),
    ):
        if not path.is_file():
            raise DirectSyncPushError(
                f"admin recovery requires existing {label}"
            )
    manifest_size = manifest_path.stat().st_size
    if not 0 < manifest_size <= 65_536:
        raise DirectSyncPushError("existing producer manifest size is invalid")
    stored_manifest = _load_json_no_duplicate_keys(manifest_path.read_bytes())
    if stored_manifest != dict(manifest):
        raise DirectSyncPushError(
            "existing producer manifest differs from the recovery candidate"
        )


def _admin_recover(
    args: argparse.Namespace,
    manifest: dict[str, Any],
    credential: dict[str, Any],
    progress: _AdminRecoveryProgress | None = None,
) -> tuple[dict[str, Any], dict[str, Any], str, Path, dict[str, Any]]:
    require_enrollment_mutex_owned()
    if (
        str(getattr(args, "credential_scope", "") or "").strip().lower()
        != SCOPE_CURRENT_USER
    ):
        raise DirectSyncPushError(
            "admin recovery requires --credential-scope current_user"
        )
    for field, option in (
        ("pc_id", "--pc-id"),
        ("producer_id", "--producer-id"),
        ("source_host_id", "--source-host-id"),
        ("producer_install_id", "--producer-install-id"),
    ):
        if not str(getattr(args, field, "") or "").strip():
            raise DirectSyncPushError(f"admin recovery requires explicit {option}")
    candidate_manifest_hash = manifest_hash(manifest)
    expected_active_manifest_hash = _validated_sha256_hex(
        str(getattr(args, "expected_active_manifest_hash", "") or ""),
        label="expected_active_manifest_hash",
    )
    if candidate_manifest_hash != expected_active_manifest_hash:
        raise DirectSyncPushError(
            "legacy_manifest_hash_mismatch: recovery HTTP was not sent"
        )
    configured_ca_bundle_path = str(
        getattr(args, "tls_ca_bundle_path", "") or ""
    ).strip()
    if not configured_ca_bundle_path:
        raise DirectSyncPushError("admin recovery requires an explicit TLS CA bundle")
    recovery_path, authorization = _load_admin_recovery_authorization(
        str(getattr(args, "admin_recovery_secret_file", "") or ""),
        expected_producer_id=str(credential["producer_id"]),
    )
    _preflight_admin_recovery_local_state(
        args,
        manifest,
        credential,
        recovery_path,
    )
    recovery_url = _validate_admin_recovery_url(
        str(getattr(args, "admin_recovery_url", "") or "")
        or _admin_recovery_url_from_endpoint(str(credential["endpoint_url"])),
        str(credential["endpoint_url"]),
    )
    token_source, token = _token_from_sources(args)
    try:
        possession_context = PersistentPossessionKey.provision_initial(
            scope=SCOPE_CURRENT_USER
        )
    except AdminRecoveryRequired as exc:
        raise PossessionKeyRecoveryRequired(
            "admin_recovery",
            exc.public_state(),
        ) from exc
    with possession_context as possession_key_handle:
        descriptor = possession_key_handle.descriptor().as_dict()
        non_exportability = possession_key_handle.assert_non_exportable()
        descriptor["private_export_status"] = (
            non_exportability.private_export_status_hex
        )
        identity = manifest["pc_identity"]
        proof = {
            "contract_version": ADMIN_RECOVERY_PROOF_CONTRACT_VERSION,
            "authorization_id": authorization["authorization_id"],
            "nonce": authorization["nonce"],
            "expires_at": authorization["expires_at"],
            "audience": authorization["audience"],
            "producer_id": credential["producer_id"],
            "producer_install_id": identity["producer_install_id"],
            "source_host_id": identity["source_host_id"],
            "manifest_hash": candidate_manifest_hash,
            "new_possession_key_fingerprint": descriptor["fingerprint"],
        }
        signature = b64url_encode(
            possession_key_handle.sign_es256(canonical_json_bytes(proof))
        )
        headers = {"X-Producer-Enrollment-Token": token} if token else {}
        session = _open_admin_recovery_session(configured_ca_bundle_path)
        try:
            response = session.post(
                recovery_url,
                json={
                    "contract_version": ADMIN_RECOVERY_COMPLETE_CONTRACT_VERSION,
                    "proof": proof,
                    "signature": signature,
                    "recovery_token": authorization["recovery_token"],
                    "new_possession_public_jwk": dict(descriptor["public_jwk"]),
                    "manifest": manifest,
                    "endpoint_url": credential["endpoint_url"],
                },
                headers=headers,
                timeout=max(1, int(args.enrollment_timeout_seconds)),
                allow_redirects=False,
            )
            if response.status_code == 200 and progress is not None:
                progress.server_credential_rotated = True
        finally:
            session.close()
    try:
        response_payload = response.json()
    except ValueError as exc:
        raise DirectSyncPushError(
            f"admin recovery response is not JSON: HTTP {response.status_code}"
        ) from exc
    if response.status_code != 200:
        error = response_payload.get("error") if isinstance(response_payload, dict) else {}
        code = str(error.get("code") or response.status_code) if isinstance(error, dict) else str(response.status_code)
        message = str(error.get("message") or "").strip() if isinstance(error, dict) else ""
        raise ProducerEnrollmentHTTPError(response.status_code, code, message)
    if not isinstance(response_payload, dict):
        raise DirectSyncPushError("admin recovery response must be a JSON object")
    _validate_admin_recovery_response(
        response_payload,
        manifest=manifest,
        credential=credential,
        possession_key=descriptor,
    )
    return response_payload, descriptor, token_source, recovery_path, authorization


def build_payloads(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    endpoint_url = _endpoint_from_args(args)
    data_dir = str(Path(args.data_dir or DEFAULT_DIRECT_SYNC_ROOT).expanduser().resolve())
    sync_dir = str(Path(args.sync_dir or DEFAULT_LABEL_MATCH_DATA_ROOT).expanduser().resolve())
    identity = _derive_identity(args, endpoint_url)
    secret_ref = f"dpapi:{identity['secret_ref']}"
    manifest = _build_manifest(
        pc_id=identity["pc_id"],
        source_host_id=identity["source_host_id"],
        producer_install_id=identity["producer_install_id"],
        sync_dir=sync_dir,
        data_dir=data_dir,
        endpoint_url=endpoint_url,
        secret_ref=secret_ref,
        identity_registry_status="self_enrolled",
    )
    credential = {
        "credential_schema_version": "producer-ingest-credential-reference-v1",
        "created_at": _utc_now_text(),
        "endpoint_url": endpoint_url,
        "key_id": identity["key_id"],
        "producer_id": identity["producer_id"],
        "secret_data_dir": data_dir,
        "secret_ref": secret_ref,
        "dpapi_scope": str(
            getattr(args, "credential_scope", "machine") or "machine"
        ),
    }
    report = {
        "report_version": "label-match-worker-pc-registration-v1",
        "status": "DRY_RUN" if args.dry_run else "APPLY_REQUESTED",
        "admin_recovery_requested": bool(
            str(getattr(args, "admin_recovery_secret_file", "") or "").strip()
        ),
        "app": LABEL_MATCH_APP,
        "endpoint_url": endpoint_url,
        "enrollment_url": args.enrollment_url or _enrollment_url_from_endpoint(endpoint_url),
        "hostname": identity["pc_id"],
        "key_id": identity["key_id"],
        "manual_pc_approval_required": False,
        "producer_id": identity["producer_id"],
        "producer_install_id": identity["producer_install_id"],
        "producer_identity_loaded_from": identity["identity_loaded_from"],
        "producer_identity_path": identity["identity_persist_path"],
        "producer_identity_source": identity["identity_source"],
        "producer_install_id_derivation": identity[
            "producer_install_id_derivation"
        ],
        "raw_secret_written": False,
        "secret_material_persisted": False,
        "secret_ref": "[redacted]",
        "source_host_id": identity["source_host_id"],
        "sync_dir": sync_dir,
        "data_dir": data_dir,
        "manifest_hash": manifest_hash(manifest),
        "expected_active_manifest_hash": str(
            getattr(args, "expected_active_manifest_hash", "") or ""
        )
        .strip()
        .lower(),
    }
    return manifest, credential, report


def _apply_registration_locked(
    args: argparse.Namespace,
    manifest: dict[str, Any],
    credential: dict[str, Any],
    report: dict[str, Any],
    progress: _AdminRecoveryProgress | None = None,
) -> dict[str, Any]:
    admin_recovery_requested = bool(
        str(getattr(args, "admin_recovery_secret_file", "") or "").strip()
    )
    recovery_path: Path | None = None
    recovery_authorization: dict[str, Any] | None = None
    if admin_recovery_requested:
        (
            response_payload,
            possession_key,
            token_source,
            recovery_path,
            recovery_authorization,
        ) = _admin_recover(args, manifest, credential, progress)
        registration_contract_version = ADMIN_RECOVERY_COMPLETE_CONTRACT_VERSION
        registration_url = _validate_admin_recovery_url(
            str(getattr(args, "admin_recovery_url", "") or "")
            or _admin_recovery_url_from_endpoint(str(credential["endpoint_url"])),
            str(credential["endpoint_url"]),
        )
    else:
        token_source, token = _token_from_sources(args)
        enrollment_url = str(report["enrollment_url"])
        possession_key = _prepare_possession_key(report)
        enrollment_payload = {
            "contract_version": ENROLLMENT_CONTRACT_VERSION,
            "endpoint_url": credential["endpoint_url"],
            "key_id": credential["key_id"],
            "manifest": manifest,
            "manifest_hash": manifest_hash(manifest),
            "possession_public_jwk": dict(possession_key["public_jwk"]),
            "producer_id": credential["producer_id"],
        }
        response_payload = _enroll(
            enrollment_payload,
            enrollment_url=enrollment_url,
            enrollment_token=token,
            timeout_seconds=args.enrollment_timeout_seconds,
            tls_ca_bundle_path=str(
                getattr(args, "tls_ca_bundle_path", "") or ""
            ).strip(),
        )
        _validate_v2_enrollment_response(
            response_payload,
            expected_fingerprint=str(possession_key["fingerprint"]),
            expected_producer_id=str(credential["producer_id"]),
            expected_install_id=str(report["producer_install_id"]),
            expected_source_host_id=str(report["source_host_id"]),
            expected_endpoint_url=str(credential["endpoint_url"]),
            expected_manifest_hash=str(report["manifest_hash"]),
        )
        registration_contract_version = ENROLLMENT_CONTRACT_VERSION
        registration_url = enrollment_url
    possession_fingerprint = str(possession_key["fingerprint"])
    report.update(
        {
            "enrollment_contract_version": ENROLLMENT_CONTRACT_VERSION,
            "possession_key_contract_version": str(
                possession_key["contract_version"]
            ),
            "possession_key_created": bool(possession_key["created"]),
            "possession_key_fingerprint": possession_fingerprint,
            "possession_key_scope": str(possession_key["scope"]),
            "possession_key_provisioning_allowed": (
                admin_recovery_requested
                or report.get("producer_identity_source") == "generated"
            ),
        }
    )
    secret = _secret_from_response(response_payload)
    expected_fingerprint = str(response_payload.get("secret_fingerprint_sha256") or "")
    if expected_fingerprint and _fingerprint(secret) != expected_fingerprint:
        raise DirectSyncPushError("self-enroll secret fingerprint mismatch")
    credential["producer_id"] = str(response_payload.get("producer_id") or credential["producer_id"])
    credential["key_id"] = str(response_payload.get("key_id") or credential["key_id"])
    if admin_recovery_requested and not isinstance(
        response_payload.get("machine_credential_bundle"), Mapping
    ):
        raise DirectSyncPushError(
            "admin recovery response missing machine credential bundle"
        )
    machine_profile = ensure_runtime_profile_from_enrollment_bundle(
        response_payload,
        expected_app=LABEL_MATCH_APP,
        expected_program="Label_Match",
        expected_source_host_id=str(report["source_host_id"]),
        expected_device_id=str(manifest["pc_identity"]["pc_id"]),
        profile_path=str(getattr(args, "logistics_profile_path", "") or "").strip()
        or None,
        tls_ca_bundle_path=str(
            getattr(args, "tls_ca_bundle_path", "") or ""
        ).strip()
        or None,
        credential_scope=str(
            getattr(args, "credential_scope", "machine") or "machine"
        ),
        allow_existing_token_rotation=admin_recovery_requested,
        expected_producer_id=str(credential["producer_id"]),
        expected_producer_install_id=str(
            manifest["pc_identity"]["producer_install_id"]
        ),
        expected_manifest_hash=manifest_hash(manifest),
        expected_endpoint_url=str(credential["endpoint_url"]),
    )
    if machine_profile is None and bool(getattr(args, "require_machine_credential_bundle", False)):
        raise DirectSyncPushError("self-enroll response missing machine credential bundle")
    if admin_recovery_requested:
        if not isinstance(machine_profile, Mapping) or machine_profile.get(
            "status"
        ) != "rotated":
            raise DirectSyncPushError(
                "admin recovery did not rotate the existing logistics credential"
            )
        if progress is not None:
            progress.logistics_credential_finalized = True
    secret_target = str(credential["secret_ref"]).split(":", 1)[1]
    try:
        selected_scope = str(
            getattr(args, "credential_scope", "machine") or "machine"
        )
        expected_secret_path = _secret_path(
            credential["secret_data_dir"], secret_target
        )
        if expected_secret_path.exists() and not admin_recovery_requested:
            raise FileExistsError(
                f"producer credential path already exists: {expected_secret_path}"
            )
        if selected_scope == "current_user":
            secret_path = _write_dpapi_secret(
                credential["secret_data_dir"],
                secret_target,
                secret,
                credential_scope=selected_scope,
            )
        else:
            secret_path = _write_dpapi_secret(
                credential["secret_data_dir"], secret_target, secret
            )
        if not _verify_dpapi_secret(credential["secret_data_dir"], secret_target, secret):
            raise DirectSyncPushError("dpapi secret verify failed")
    except Exception:
        for created_path in (machine_profile or {}).get("created_paths", []):
            Path(created_path).unlink(missing_ok=True)
        raise
    if admin_recovery_requested and progress is not None:
        progress.producer_credential_finalized = True
    report.update(
        {
            "status": (
                "ADMIN_RECOVERY_REGISTERED"
                if admin_recovery_requested
                else "SELF_ENROLLMENT_REGISTERED"
            ),
            "enrollment_status": response_payload.get("status"),
            "key_id": credential["key_id"],
            "producer_id": credential["producer_id"],
            "secret_bootstrap_verified": True,
            "secret_fingerprint_sha256": expected_fingerprint or _fingerprint(secret),
            "secret_material_persisted": False,
            "server_binding": _safe_server_binding(
                response_payload.get("server_binding")
            ),
            "server_registration_verified": True,
            "token_source": token_source,
            "protected_secret_path": str(secret_path),
            "credential_scope": selected_scope,
            "identity_action": response_payload.get("identity_action"),
            "authorization_state": response_payload.get("authorization_state"),
            "credential_epoch": response_payload.get("credential_epoch"),
            "possession_binding_verified": True,
            "v2_client_receipt_verified": True,
            "machine_profiles": {"logistics": machine_profile} if machine_profile else {},
            "registration_action": (
                "admin_recovery"
                if admin_recovery_requested
                else "initial_enrollment"
            ),
            "registration_contract_version": registration_contract_version,
            "registration_url": registration_url,
            "enrollment_transport_trust_env": False,
        }
    )
    if admin_recovery_requested:
        assert recovery_path is not None and recovery_authorization is not None
        report.update(
            {
                "admin_recovery_requested": True,
                "admin_recovery_verified": True,
                "admin_recovery_action": "ADMIN_RECOVERY",
                "admin_recovery_authorization_id": recovery_authorization[
                    "authorization_id"
                ],
                "admin_recovery_authorization_audit_event_id": (
                    recovery_authorization["audit_event_id"]
                ),
                "admin_recovery_secret_cleanup_required": True,
                "admin_recovery_secret_file": str(recovery_path),
            }
        )
    client_receipt = _safe_client_receipt(response_payload.get("client_receipt"))
    if client_receipt:
        report["client_receipt"] = client_receipt
        report["client_receipt_status"] = client_receipt.get("status")
    return report


def apply_registration(
    args: argparse.Namespace,
    manifest: dict[str, Any],
    credential: dict[str, Any],
    report: dict[str, Any],
    progress: _AdminRecoveryProgress | None = None,
) -> dict[str, Any]:
    """Apply through the shared mutex, including direct imported callers."""

    guard = EnrollmentMutex(
        getattr(
            args,
            "enrollment_mutex_timeout_seconds",
            DEFAULT_ENROLLMENT_MUTEX_TIMEOUT_SECONDS,
        )
    )
    with guard as receipt:
        report["enrollment_mutex"] = receipt
        return _apply_registration_locked(
            args,
            manifest,
            credential,
            report,
            progress,
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Register this Label_Match PC as an HTTPS producer")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    parser.add_argument("--server-base-url", default=DEFAULT_SERVER_BASE_URL)
    parser.add_argument("--endpoint-url", default="")
    parser.add_argument("--enrollment-url", default="")
    parser.add_argument("--admin-recovery-url", default="")
    parser.add_argument(
        "--admin-recovery-secret-file",
        default="",
        help="one-time authorization file issued by the audited server admin tool",
    )
    parser.add_argument(
        "--expected-active-manifest-hash",
        default="",
        help="required exact server-active legacy manifest hash for recovery preflight",
    )
    parser.add_argument("--enrollment-token", default="")
    parser.add_argument("--enrollment-token-file", default="")
    parser.add_argument("--enrollment-token-env", default=DEFAULT_ENROLLMENT_TOKEN_ENV)
    parser.add_argument("--enrollment-timeout-seconds", type=int, default=30)
    parser.add_argument(
        "--enrollment-mutex-timeout-seconds",
        type=float,
        default=DEFAULT_ENROLLMENT_MUTEX_TIMEOUT_SECONDS,
        help="finite wait for the shared one-session enrollment mutex",
    )
    parser.add_argument("--require-machine-credential-bundle", action="store_true")
    parser.add_argument(
        "--credential-scope",
        choices=("machine", "current_user"),
        default="machine",
    )
    parser.add_argument("--logistics-profile-path", default="")
    parser.add_argument("--tls-ca-bundle-path", default="")
    parser.add_argument("--pc-id", default="")
    parser.add_argument("--source-host-id", default="")
    parser.add_argument("--producer-install-id", default="")
    parser.add_argument("--producer-id", default="")
    parser.add_argument("--key-id", default="")
    parser.add_argument("--secret-ref-target", default="")
    parser.add_argument("--machine-guid", default="")
    parser.add_argument("--sync-dir", default=DEFAULT_LABEL_MATCH_DATA_ROOT)
    parser.add_argument("--data-dir", default=DEFAULT_DIRECT_SYNC_ROOT)
    parser.add_argument("--identity-path", default="")
    parser.add_argument("--manifest-path", default="")
    parser.add_argument("--credential-path", default="")
    parser.add_argument("--receipt-path", default="")
    parser.add_argument("--report-path", default="")
    args = parser.parse_args(argv)

    data_dir = Path(args.data_dir or DEFAULT_DIRECT_SYNC_ROOT).expanduser().resolve()
    report_path = Path(args.report_path).expanduser() if args.report_path else data_dir / "status" / DEFAULT_REPORT_FILENAME
    recovery_progress = _AdminRecoveryProgress()
    report_context: dict[str, Any] = {}
    report: dict[str, Any] = {}
    enrollment_guard: EnrollmentMutex | None = None
    try:
        if args.apply:
            enrollment_guard = EnrollmentMutex(args.enrollment_mutex_timeout_seconds)
            report_context["enrollment_mutex"] = enrollment_guard.acquire()
        manifest, credential, report = build_payloads(args)
        if "enrollment_mutex" in report_context:
            report["enrollment_mutex"] = dict(report_context["enrollment_mutex"])
        report_context = {
            **report_context,
            "endpoint_url": report.get("endpoint_url"),
            "key_id": report.get("key_id"),
            "manual_pc_approval_required": report.get("manual_pc_approval_required"),
            "producer_id": report.get("producer_id"),
            "producer_identity_loaded_from": report.get(
                "producer_identity_loaded_from"
            ),
            "producer_identity_source": report.get("producer_identity_source"),
            "producer_install_id": report.get("producer_install_id"),
            "source_host_id": report.get("source_host_id"),
            "admin_recovery_requested": report.get("admin_recovery_requested"),
            "expected_active_manifest_hash": report.get(
                "expected_active_manifest_hash"
            ),
        }
        if args.apply:
            report = apply_registration(
                args,
                manifest,
                credential,
                report,
                recovery_progress,
            )
            for directory in [
                Path(credential["secret_data_dir"]),
                Path(manifest["paths"]["evidence_dir"]),
                Path(manifest["paths"]["rollback_dir"]),
                Path(manifest["sync"]["sync_dir"]),
            ]:
                directory.mkdir(parents=True, exist_ok=True)
            manifest_path = Path(args.manifest_path).expanduser() if args.manifest_path else data_dir / DEFAULT_MANIFEST_FILENAME
            credential_path = Path(args.credential_path).expanduser() if args.credential_path else data_dir / DEFAULT_CREDENTIAL_FILENAME
            receipt_path = Path(args.receipt_path).expanduser() if args.receipt_path else data_dir / "evidence" / DEFAULT_RECEIPT_FILENAME
            identity_path = _producer_identity_path(args)
            pc_identity = manifest["pc_identity"]
            identity = {
                "schema_version": PRODUCER_IDENTITY_SCHEMA_VERSION,
                "producer_id": str(credential["producer_id"]),
                "source_host_id": str(pc_identity["source_host_id"]),
                "producer_install_id": str(pc_identity["producer_install_id"]),
                "pc_id": str(pc_identity["pc_id"]),
            }
            _write_json(identity_path, identity)
            _write_json(manifest_path, manifest)
            _write_json(credential_path, credential)
            client_receipt = report.get("client_receipt")
            if isinstance(client_receipt, dict):
                _write_json(receipt_path, client_receipt)
            _verify_json_file(identity_path, identity, label="producer identity")
            _verify_json_file(manifest_path, manifest, label="producer manifest")
            _verify_json_file(
                credential_path,
                credential,
                label="producer credential reference",
            )
            if isinstance(client_receipt, dict):
                _verify_json_file(receipt_path, client_receipt, label="client receipt")
            report.update(
                {
                    "credential_path": str(credential_path.resolve()),
                    "identity_path": str(identity_path.resolve()),
                    "producer_identity_path": str(identity_path.resolve()),
                    "producer_identity_persisted": True,
                    "manifest_path": str(manifest_path.resolve()),
                    "receipt_path": str(receipt_path.resolve()),
                    "manifest_hash_verified": (
                        manifest_hash(manifest) == str(report["manifest_hash"])
                    ),
                    "persisted_manifest_hash_verified": (
                        manifest_hash(
                            json.loads(manifest_path.read_text(encoding="utf-8-sig"))
                        )
                        == str(report["manifest_hash"])
                    ),
                }
            )
            if (
                report["manifest_hash_verified"] is not True
                or report["persisted_manifest_hash_verified"] is not True
            ):
                raise DirectSyncPushError("persisted manifest hash readback failed")
            if report.get("admin_recovery_requested") is True:
                recovery_progress.local_documents_finalized = True
                report["admin_recovery_progress"] = (
                    recovery_progress.redacted_summary()
                )
                report["report_path"] = str(report_path.resolve())
                _write_json(report_path, report)
                _verify_json_file(
                    report_path,
                    report,
                    label="admin recovery preliminary report",
                )
            if report.get("admin_recovery_secret_cleanup_required") is True:
                recovery_secret_path = Path(
                    str(report.get("admin_recovery_secret_file") or "")
                )
                recovery_secret_path.unlink(missing_ok=True)
                if recovery_secret_path.exists():
                    raise OSError(
                        "consumed admin recovery authorization file still exists"
                    )
                report["admin_recovery_secret_file_deleted"] = True
                report["admin_recovery_secret_cleanup_required"] = False
                recovery_progress.authorization_file_deleted = True
                report["admin_recovery_progress"] = (
                    recovery_progress.redacted_summary()
                )
        else:
            report.update(
                {
                    "credential_path": str((data_dir / DEFAULT_CREDENTIAL_FILENAME).resolve()),
                    "identity_path": str((data_dir / "producer_identity.json").resolve()),
                    "manifest_path": str((data_dir / DEFAULT_MANIFEST_FILENAME).resolve()),
                    "receipt_path": str((data_dir / "evidence" / DEFAULT_RECEIPT_FILENAME).resolve()),
                    "server_registration_verified": False,
                    "secret_bootstrap_verified": False,
                }
            )
        report["report_path"] = str(report_path.resolve())
        _write_json(report_path, report)
        _verify_json_file(report_path, report, label="registration report")
        print(f"registration_report={report_path.resolve()}")
        return (
            0
            if args.dry_run
            or report["status"]
            in {"SELF_ENROLLMENT_REGISTERED", "ADMIN_RECOVERY_REGISTERED"}
            else 1
        )
    except Exception as exc:
        if recovery_progress.server_credential_rotated:
            blocked = {
                "report_version": "label-match-worker-pc-registration-v1",
                "status": "BLOCKED_POST_RECOVERY_LOCAL_PERSISTENCE",
                "blocked_reason": type(exc).__name__,
                "server_credential_rotated": True,
                "local_rollback_performed": False,
                "secret_material_persisted": None,
                "recovery_action": "NEW_AUDITED_RECOVERY_REQUIRED",
                "admin_recovery_progress": recovery_progress.redacted_summary(),
                "authorization_file_deleted": (
                    not Path(args.admin_recovery_secret_file).expanduser().exists()
                    if str(args.admin_recovery_secret_file or "").strip()
                    else False
                ),
                "report_path": str(report_path.resolve()),
            }
            blocked.update(
                {key: value for key, value in report_context.items() if value is not None}
            )
            try:
                _write_json(report_path, blocked)
                _verify_json_file(
                    report_path,
                    blocked,
                    label="post-recovery failure report",
                )
            except Exception:
                pass
            print(f"registration_report={report_path.resolve()}")
            return 3
        blocked = {
            "report_version": "label-match-worker-pc-registration-v1",
            "status": "BLOCKED",
            "blocked_reason": (
                f"self-enroll failed: {exc.error_code}"
                if isinstance(exc, ProducerEnrollmentHTTPError)
                else str(exc)
            ),
            "raw_secret_written": False,
            "secret_material_persisted": False,
        }
        blocked.update({key: value for key, value in report_context.items() if value is not None})
        if isinstance(exc, EnrollmentMutexError):
            blocked.update(
                {
                    "status": exc.report_status,
                    "blocked_reason": exc.reason_code,
                    "recovery_action": exc.recovery_action,
                    "enrollment_mutex": dict(exc.mutex_report),
                }
            )
        for key in (
            "enrollment_contract_version",
            "possession_key_contract_version",
            "possession_key_created",
            "possession_key_fingerprint",
            "possession_key_provisioning_allowed",
            "possession_key_scope",
        ):
            if key in report:
                blocked[key] = report[key]
        if isinstance(exc, PossessionKeyRecoveryRequired):
            blocked.update(
                {
                    "status": ADMIN_RECOVERY_ACTION,
                    "recovery_action": ADMIN_RECOVERY_ACTION,
                    "recovery_origin": "local_possession_key",
                    "possession_key_state": dict(exc.key_state),
                    "automatic_key_replacement_performed": False,
                    "existing_identity_preserved": True,
                }
            )
        elif isinstance(exc, ProducerEnrollmentHTTPError):
            blocked.update(
                {
                    "server_error_code": exc.error_code,
                    "server_http_status": exc.status_code,
                }
            )
            if exc.error_code == "admin_recovery_required":
                blocked.update(
                    {
                        "status": ADMIN_RECOVERY_ACTION,
                        "recovery_action": ADMIN_RECOVERY_ACTION,
                        "recovery_origin": "server_legacy_identity",
                        "automatic_legacy_upgrade_performed": False,
                        "existing_identity_preserved": True,
                    }
                )
            elif exc.error_code == "reattach_proof_required":
                blocked.update(
                    {
                        "status": "POSSESSION_PROOF_REATTACH_REQUIRED",
                        "recovery_action": "POSSESSION_PROOF_REATTACH_REQUIRED",
                        "automatic_credential_replay_performed": False,
                    }
                )
        blocked["report_path"] = str(report_path.resolve())
        _write_json(report_path, blocked)
        print(f"registration_report={report_path.resolve()}")
        return 2
    finally:
        if enrollment_guard is not None:
            enrollment_guard.release()


if __name__ == "__main__":
    raise SystemExit(main())

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


DEFAULT_SERVER_BASE_URL = "https://worker.kmtecherp.com"
DEFAULT_LABEL_MATCH_DATA_ROOT = r"C:\ProgramData\KMTech\Label_Match\data"
DEFAULT_DIRECT_SYNC_ROOT = r"C:\ProgramData\KMTech\DirectSync\label_match"
DEFAULT_ENROLLMENT_TOKEN_ENV = "PRODUCER_SELF_ENROLL_TOKEN"
DEFAULT_CREDENTIAL_FILENAME = "credential.json"
DEFAULT_MANIFEST_FILENAME = "producer_manifest.json"
DEFAULT_RECEIPT_FILENAME = "producer_self_enrollment_receipt.json"
DEFAULT_REPORT_FILENAME = "label_match_worker_pc_registration.json"
ENROLLMENT_CONTRACT_VERSION = "producer-self-enrollment-v1"
CRYPTPROTECT_LOCAL_MACHINE = 0x4
LABEL_MATCH_APP = "LabelMatch"
SAFE_TOKEN_RE = re.compile(r"[^A-Za-z0-9._-]+")
RAW_EVENT_NAMES = [
    "APP_START",
    "APP_CLOSE",
    "LABEL_MATCHED",
    "PACKAGING_WAITING_OBSERVED",
    "SHIPPING_WAITING_OBSERVED",
    "SCAN_ATTEMPT",
    "SCAN_OK",
    "TRAY_COMPLETE",
    "SET_DELETED",
    "SET_RESTORED",
    "SET_CANCELLED",
    "TRAY_COMPLETION_CANCELLED",
    "UI_ERROR",
    "ERROR_INPUT",
    "ERROR_MISMATCH",
    "BASE64_DECODED",
]


def _utc_now_text() -> str:
    return _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_json(path: str | os.PathLike[str], payload: Mapping[str, Any]) -> None:
    target = Path(path)
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


def _machine_identity(args: argparse.Namespace) -> str:
    if args.machine_guid:
        return args.machine_guid
    if os.name == "nt":
        try:
            import winreg

            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\Cryptography") as key:
                value, _value_type = winreg.QueryValueEx(key, "MachineGuid")
                if value:
                    return str(value)
        except OSError:
            pass
    return f"{socket.gethostname()}|{uuid.getnode():012x}"


def _identity_suffix(args: argparse.Namespace) -> str:
    return hashlib.sha256(_machine_identity(args).encode("utf-8")).hexdigest()[:12]


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
    return f"{parsed.scheme}://{parsed.netloc}/api/producer-ingest/v1/enroll"


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
            "data_dir": str(data_root),
            "evidence_dir": str(data_root / "evidence"),
            "rollback_dir": str(data_root / "rollback"),
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
                "client_state_db": str(data_root / "relay_state.sqlite3"),
                "queue_dir": str(data_root / "relay_queue"),
                "status": "operator_supplied_uncontacted",
            },
            "server_ingest_target": endpoint_url,
            "status": "operator_supplied_uncontacted",
            "sync_dir": str(sync_root),
            "sync_transport": "http_push",
        },
    }


def _derive_identity(args: argparse.Namespace, endpoint_url: str) -> dict[str, str]:
    pc_id = _safe_token(args.pc_id or socket.gethostname(), "worker-pc")
    source_host_id = _safe_token(
        args.source_host_id or f"label-match-{pc_id}-{_identity_suffix(args)}",
        "label-match-worker",
    ).lower()
    data_dir = str(Path(args.data_dir or DEFAULT_DIRECT_SYNC_ROOT).expanduser().resolve())
    seed = "|".join([LABEL_MATCH_APP, pc_id, source_host_id, data_dir, endpoint_url])
    producer_install_id = _safe_token(
        args.producer_install_id or f"install-{hashlib.sha256(seed.encode('utf-8')).hexdigest()[:32]}",
        "install-label-match-worker",
    )
    return {
        "pc_id": pc_id,
        "source_host_id": source_host_id,
        "producer_install_id": producer_install_id,
        "producer_id": _safe_token(args.producer_id or f"producer-{source_host_id}", "producer-label-match-worker"),
        "key_id": _safe_token(args.key_id or f"key-{source_host_id}", "key-label-match-worker"),
        "secret_ref": _safe_token(args.secret_ref_target or f"producer-{source_host_id}-http-push-key", "producer-label-match-worker-http-push-key"),
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


def _enroll(payload: Mapping[str, Any], *, enrollment_url: str, enrollment_token: str, timeout_seconds: int) -> dict[str, Any]:
    headers = {"X-Producer-Enrollment-Token": enrollment_token} if enrollment_token else {}
    response = requests.post(
        enrollment_url,
        json=dict(payload),
        headers=headers,
        timeout=max(1, int(timeout_seconds)),
    )
    try:
        response_payload = response.json()
    except ValueError as exc:
        raise DirectSyncPushError(f"self-enroll response is not JSON: HTTP {response.status_code}") from exc
    if response.status_code >= 400:
        error = response_payload.get("error") if isinstance(response_payload, dict) else {}
        code = str(error.get("code") or response.status_code) if isinstance(error, dict) else str(response.status_code)
        raise DirectSyncPushError(f"self-enroll failed: {code}")
    if not isinstance(response_payload, dict):
        raise DirectSyncPushError("self-enroll response must be a JSON object")
    return response_payload


class _DataBlob(ctypes.Structure):
    _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.c_void_p)]


def _dpapi_protect_machine(secret: str) -> bytes:
    if sys.platform != "win32":
        raise DirectSyncPushError("dpapi secret bootstrap requires Windows")
    from ctypes import byref, wintypes

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
        CRYPTPROTECT_LOCAL_MACHINE,
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
    return Path(data_dir).expanduser().resolve() / "secrets" / f"{secret_ref_target}.dpapi"


def _write_dpapi_secret(data_dir: str | os.PathLike[str], secret_ref_target: str, secret: str) -> Path:
    target = _secret_path(data_dir, secret_ref_target)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(_dpapi_protect_machine(secret))
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
    }
    report = {
        "report_version": "label-match-worker-pc-registration-v1",
        "status": "DRY_RUN" if args.dry_run else "APPLY_REQUESTED",
        "app": LABEL_MATCH_APP,
        "endpoint_url": endpoint_url,
        "enrollment_url": args.enrollment_url or _enrollment_url_from_endpoint(endpoint_url),
        "hostname": identity["pc_id"],
        "key_id": identity["key_id"],
        "manual_pc_approval_required": False,
        "producer_id": identity["producer_id"],
        "producer_install_id": identity["producer_install_id"],
        "raw_secret_written": False,
        "secret_material_persisted": False,
        "secret_ref": "[redacted]",
        "source_host_id": identity["source_host_id"],
        "sync_dir": sync_dir,
        "data_dir": data_dir,
        "manifest_hash": manifest_hash(manifest),
    }
    return manifest, credential, report


def apply_registration(args: argparse.Namespace, manifest: dict[str, Any], credential: dict[str, Any], report: dict[str, Any]) -> dict[str, Any]:
    token_source, token = _token_from_sources(args)
    enrollment_url = str(report["enrollment_url"])
    enrollment_payload = {
        "contract_version": ENROLLMENT_CONTRACT_VERSION,
        "endpoint_url": credential["endpoint_url"],
        "key_id": credential["key_id"],
        "manifest": manifest,
        "manifest_hash": manifest_hash(manifest),
        "producer_id": credential["producer_id"],
    }
    response_payload = _enroll(
        enrollment_payload,
        enrollment_url=enrollment_url,
        enrollment_token=token,
        timeout_seconds=args.enrollment_timeout_seconds,
    )
    secret = _secret_from_response(response_payload)
    expected_fingerprint = str(response_payload.get("secret_fingerprint_sha256") or "")
    if expected_fingerprint and _fingerprint(secret) != expected_fingerprint:
        raise DirectSyncPushError("self-enroll secret fingerprint mismatch")
    credential["producer_id"] = str(response_payload.get("producer_id") or credential["producer_id"])
    credential["key_id"] = str(response_payload.get("key_id") or credential["key_id"])
    secret_target = str(credential["secret_ref"]).split(":", 1)[1]
    secret_path = _write_dpapi_secret(credential["secret_data_dir"], secret_target, secret)
    if not _verify_dpapi_secret(credential["secret_data_dir"], secret_target, secret):
        raise DirectSyncPushError("dpapi secret verify failed")
    report.update(
        {
            "status": "SELF_ENROLLMENT_REGISTERED",
            "enrollment_status": response_payload.get("status"),
            "key_id": credential["key_id"],
            "producer_id": credential["producer_id"],
            "secret_bootstrap_verified": True,
            "secret_fingerprint_sha256": expected_fingerprint or _fingerprint(secret),
            "secret_material_persisted": False,
            "server_binding": response_payload.get("server_binding") or {},
            "server_registration_verified": True,
            "token_source": token_source,
            "protected_secret_path": str(secret_path),
        }
    )
    client_receipt = response_payload.get("client_receipt")
    if isinstance(client_receipt, dict):
        report["client_receipt"] = client_receipt
        report["client_receipt_status"] = client_receipt.get("status")
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Register this Label_Match PC as an HTTPS producer")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    parser.add_argument("--server-base-url", default=DEFAULT_SERVER_BASE_URL)
    parser.add_argument("--endpoint-url", default="")
    parser.add_argument("--enrollment-url", default="")
    parser.add_argument("--enrollment-token", default="")
    parser.add_argument("--enrollment-token-file", default="")
    parser.add_argument("--enrollment-token-env", default=DEFAULT_ENROLLMENT_TOKEN_ENV)
    parser.add_argument("--enrollment-timeout-seconds", type=int, default=30)
    parser.add_argument("--pc-id", default="")
    parser.add_argument("--source-host-id", default="")
    parser.add_argument("--producer-install-id", default="")
    parser.add_argument("--producer-id", default="")
    parser.add_argument("--key-id", default="")
    parser.add_argument("--secret-ref-target", default="")
    parser.add_argument("--machine-guid", default="")
    parser.add_argument("--sync-dir", default=DEFAULT_LABEL_MATCH_DATA_ROOT)
    parser.add_argument("--data-dir", default=DEFAULT_DIRECT_SYNC_ROOT)
    parser.add_argument("--manifest-path", default="")
    parser.add_argument("--credential-path", default="")
    parser.add_argument("--receipt-path", default="")
    parser.add_argument("--report-path", default="")
    args = parser.parse_args(argv)

    data_dir = Path(args.data_dir or DEFAULT_DIRECT_SYNC_ROOT).expanduser().resolve()
    report_path = Path(args.report_path).expanduser() if args.report_path else data_dir / "status" / DEFAULT_REPORT_FILENAME
    report_context: dict[str, Any] = {}
    try:
        manifest, credential, report = build_payloads(args)
        report_context = {
            "endpoint_url": report.get("endpoint_url"),
            "key_id": report.get("key_id"),
            "manual_pc_approval_required": report.get("manual_pc_approval_required"),
            "producer_id": report.get("producer_id"),
            "producer_install_id": report.get("producer_install_id"),
            "source_host_id": report.get("source_host_id"),
        }
        if args.apply:
            report = apply_registration(args, manifest, credential, report)
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
            _write_json(manifest_path, manifest)
            _write_json(credential_path, credential)
            client_receipt = report.get("client_receipt")
            if isinstance(client_receipt, dict):
                _write_json(receipt_path, client_receipt)
            report.update(
                {
                    "credential_path": str(credential_path.resolve()),
                    "manifest_path": str(manifest_path.resolve()),
                    "receipt_path": str(receipt_path.resolve()),
                }
            )
        else:
            report.update(
                {
                    "credential_path": str((data_dir / DEFAULT_CREDENTIAL_FILENAME).resolve()),
                    "manifest_path": str((data_dir / DEFAULT_MANIFEST_FILENAME).resolve()),
                    "receipt_path": str((data_dir / "evidence" / DEFAULT_RECEIPT_FILENAME).resolve()),
                    "server_registration_verified": False,
                    "secret_bootstrap_verified": False,
                }
            )
        report["report_path"] = str(report_path.resolve())
        _write_json(report_path, report)
        print(f"registration_report={report_path.resolve()}")
        return 0 if args.dry_run or report["status"] == "SELF_ENROLLMENT_REGISTERED" else 1
    except Exception as exc:
        blocked = {
            "report_version": "label-match-worker-pc-registration-v1",
            "status": "BLOCKED",
            "blocked_reason": str(exc),
            "raw_secret_written": False,
            "secret_material_persisted": False,
        }
        blocked.update({key: value for key, value in report_context.items() if value is not None})
        blocked["report_path"] = str(report_path.resolve())
        _write_json(report_path, blocked)
        print(f"registration_report={report_path.resolve()}")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

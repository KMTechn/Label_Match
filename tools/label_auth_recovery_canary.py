#!/usr/bin/env python
"""Short, secret-safe post-deploy auth/recovery canary for Label_Match.

The normal path performs one authenticated read-only raw-artifact restore
probe, reads the current Label runtime-authority row without mutation, and
exercises a real Label deferred-intent transaction in a fresh isolated
database.  Credential material is runtime input and is never copied into the
report.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import sqlite3
import subprocess
import sys
import time
from typing import Any, Mapping
import uuid


def _configure_import_roots(
    tool_path: Path, search_path: list[str] | None = None
) -> tuple[Path, Path]:
    """Expose both app source and vendored dependencies in portable layout."""

    selected_search_path = sys.path if search_path is None else search_path
    app_root = tool_path.resolve().parents[1]
    packaged_site_packages = app_root / "site-packages"
    for import_root in (packaged_site_packages, app_root):
        if str(import_root) not in selected_search_path:
            selected_search_path.insert(0, str(import_root))
    return app_root, packaged_site_packages


REPO_ROOT, PACKAGED_SITE_PACKAGES = _configure_import_roots(Path(__file__))

from auth_recovery_canary import (  # noqa: E402
    CanaryCheck,
    CanaryContractError,
    assert_forbidden_values_absent,
    build_canary_report,
    read_bounded_json_object,
    utc_now_text,
    write_json_atomic,
)
from deferred_intent_capture import (  # noqa: E402
    DeferredIntentBinding,
    DeferredIntentCaptureStore,
)
from direct_sync_push import (  # noqa: E402
    DirectSyncPushError,
    ProducerCredentials,
    build_raw_artifact_restore_url,
    restore_metadata_from_upload_metadata,
    restore_signed_headers,
)
from direct_sync_runtime import load_credentials_from_json  # noqa: E402
from kmtech_zero_pe import normalize_public_jwk  # noqa: E402
from producer_runtime_client import canonical_json as runtime_canonical_json  # noqa: E402


APP_ID = "label_match"
REQUIRED_CHECKS = ("authentication", "credential_lease_state", "recovery")
AUTH_INJECTIONS = ("none", "invalid-secret", "expired-timestamp")
RUNTIME_AUTHORITY_TABLE = "direct_sync_runtime_authority"
RECOVERY_BARRIER_PHASE = (
    "AFTER_DEFERRED_INTENT_INSERT_BEFORE_TRANSITION_AUDIT_INSERT"
)
RECOVERY_BARRIER_SCHEMA = "label-canary-recovery-barrier.v1"
RECOVERY_TIMEOUT_SECONDS = 12
RECOVERY_CHILD_MAXIMUM_SECONDS = 30
_RUNTIME_TOKEN_RE = re.compile(r"[A-Za-z0-9_-]{32,256}")
_RUNTIME_INSTANCE_RE = re.compile(r"runtime-[0-9a-f]{32}")


@dataclass(frozen=True)
class CredentialContext:
    credentials: ProducerCredentials
    forbidden_values: tuple[str | bytes, ...]
    protected_roots: tuple[Path, ...]


def _parse_utc(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_credential_context(path: str | os.PathLike[str]) -> CredentialContext:
    selected = Path(path).expanduser()
    if selected.is_symlink():
        raise CanaryContractError("credential input path must not be a symlink")
    if not selected.is_file():
        raise FileNotFoundError(str(selected))
    payload = read_bounded_json_object(selected)
    credentials = load_credentials_from_json(selected)
    forbidden: list[str | bytes] = [
        credentials.producer_id,
        credentials.key_id,
        credentials.secret,
    ]
    for name in ("producer_id", "key_id", "secret", "secret_ref"):
        value = payload.get(name)
        if isinstance(value, (str, bytes)) and value:
            forbidden.append(value)
    protected_roots = {selected.resolve().parent}
    secret_ref = str(payload.get("secret_ref") or "").strip()
    if secret_ref.lower().startswith("dpapi:"):
        configured_data_dir = str(payload.get("secret_data_dir") or "").strip()
        if configured_data_dir:
            secret_data_root = Path(configured_data_dir).expanduser()
        else:
            local_app_data = str(os.getenv("LOCALAPPDATA") or "").strip()
            secret_data_root = (
                Path(local_app_data) / "CompanyProducerConnector"
                if local_app_data
                else selected.resolve().parent / "CompanyProducerConnector"
            )
        protected_roots.add((secret_data_root / "secrets").resolve())
    return CredentialContext(
        credentials=credentials,
        forbidden_values=tuple(forbidden),
        protected_roots=tuple(sorted(protected_roots, key=lambda item: str(item).casefold())),
    )


def _load_auth_target(path: str | os.PathLike[str]) -> dict[str, Any]:
    selected = Path(path).expanduser()
    if selected.is_symlink():
        raise CanaryContractError("auth target input path must not be a symlink")
    if not selected.is_file():
        raise FileNotFoundError(str(selected))
    payload = read_bounded_json_object(selected)
    candidate: Any = payload.get("metadata", payload)
    if not isinstance(candidate, Mapping):
        raise CanaryContractError("auth target metadata is not an object")
    return restore_metadata_from_upload_metadata(candidate)


def _close_response(response: Any) -> bool:
    close = getattr(response, "close", None)
    if not callable(close):
        return True
    try:
        close()
    except Exception:
        return False
    return True


def probe_authentication(
    *,
    credentials: ProducerCredentials,
    metadata: Mapping[str, Any],
    injection: str = "none",
    session: Any = None,
    timeout_seconds: int = 10,
    tls_ca_bundle_path: str = "",
) -> CanaryCheck:
    """Probe Label's existing HMAC restore boundary without reading its body."""

    if injection not in AUTH_INJECTIONS:
        raise CanaryContractError("auth injection mode is invalid")
    selected_credentials = credentials
    timestamp = ""
    if injection == "invalid-secret":
        selected_credentials = replace(credentials, secret=secrets.token_urlsafe(48))
    elif injection == "expired-timestamp":
        timestamp = (
            datetime.now(timezone.utc) - timedelta(hours=24)
        ).isoformat().replace("+00:00", "Z")

    response: Any = None
    response_closed = True
    try:
        restore_metadata = restore_metadata_from_upload_metadata(metadata)
        restore_url = build_raw_artifact_restore_url(
            selected_credentials.endpoint_url,
            content_sha256=str(restore_metadata["content_sha256"]),
            byte_length=int(restore_metadata["byte_length"]),
        )
        headers = restore_signed_headers(
            selected_credentials,
            restore_metadata,
            restore_url,
            timestamp=timestamp,
        )
        if session is None:
            import requests

            session = requests.Session()
        request_kwargs: dict[str, Any] = {
            "headers": headers,
            "timeout": max(1, int(timeout_seconds)),
            "allow_redirects": False,
            "stream": True,
        }
        selected_ca = str(tls_ca_bundle_path or "").strip()
        if selected_ca:
            request_kwargs["verify"] = selected_ca
        response = session.get(restore_url, **request_kwargs)
        status_code = int(getattr(response, "status_code", 0) or 0)
    except DirectSyncPushError:
        return CanaryCheck(
            "authentication",
            "FAIL",
            "AUTH_PROBE_CONFIGURATION_INVALID",
            {"request_sent": False, "injection": injection},
        )
    except Exception:
        return CanaryCheck(
            "authentication",
            "UNKNOWN",
            "AUTH_PROBE_UNAVAILABLE",
            {"request_sent": True, "injection": injection},
        )
    finally:
        if response is not None:
            response_closed = _close_response(response)

    evidence = {
        "request_sent": True,
        "method": "GET",
        "transport": "https",
        "redirects_followed": False,
        "body_read": False,
        "response_closed": response_closed,
        "status_code": status_code,
        "injection": injection,
    }
    if not response_closed:
        return CanaryCheck(
            "authentication", "UNKNOWN", "AUTH_PROBE_UNAVAILABLE", evidence
        )
    if injection != "none":
        if status_code in {401, 403}:
            return CanaryCheck(
                "authentication", "FAIL", "INJECTED_CREDENTIAL_REJECTED", evidence
            )
        if 200 <= status_code < 300 or status_code in {404, 410}:
            return CanaryCheck(
                "authentication",
                "FAIL",
                "INJECTED_CREDENTIAL_NOT_REJECTED",
                evidence,
            )
        if status_code in {408, 429} or status_code >= 500 or status_code <= 0:
            return CanaryCheck(
                "authentication", "UNKNOWN", "AUTH_PROBE_UNAVAILABLE", evidence
            )
        return CanaryCheck(
            "authentication",
            "FAIL",
            "INJECTED_CREDENTIAL_REJECTION_UNPROVEN",
            evidence,
        )
    if 200 <= status_code < 300:
        return CanaryCheck(
            "authentication", "PASS", "AUTHENTICATED_TARGET_READABLE", evidence
        )
    if status_code in {401, 403}:
        return CanaryCheck(
            "authentication", "FAIL", "AUTHENTICATION_REJECTED", evidence
        )
    if status_code in {404, 410}:
        return CanaryCheck(
            "authentication", "UNKNOWN", "AUTH_TARGET_ABSENT", evidence
        )
    if status_code in {408, 429} or status_code >= 500 or status_code <= 0:
        return CanaryCheck(
            "authentication", "UNKNOWN", "AUTH_PROBE_UNAVAILABLE", evidence
        )
    return CanaryCheck(
        "authentication", "FAIL", "AUTH_PROBE_PROTOCOL_REJECTED", evidence
    )


def _runtime_scope(credentials: ProducerCredentials, producer_install_id: str) -> str:
    values = {
        "endpoint_url": str(credentials.endpoint_url or "").strip(),
        "producer_id": str(credentials.producer_id or "").strip(),
        "key_id": str(credentials.key_id or "").strip(),
        "producer_install_id": str(producer_install_id or "").strip(),
    }
    if not all(values.values()):
        raise ValueError("runtime authority scope is incomplete")
    return hashlib.sha256(runtime_canonical_json(values).encode("utf-8")).hexdigest()


def probe_credential_lease_state(
    *,
    db_path: str | os.PathLike[str],
    credentials: ProducerCredentials,
    producer_install_id: str,
    now: datetime | None = None,
) -> CanaryCheck:
    """Read Label's exact runtime-authority binding without mutation."""

    selected = Path(db_path).expanduser()
    if selected.is_symlink():
        return CanaryCheck(
            "credential_lease_state",
            "FAIL",
            "LEASE_STATE_PATH_INVALID",
            {"database_present": False, "read_only": True},
        )
    if not selected.is_file():
        return CanaryCheck(
            "credential_lease_state",
            "UNKNOWN",
            "LEASE_STATE_TARGET_ABSENT",
            {"database_present": False, "read_only": True},
        )
    required_columns = {
        "authority_scope",
        "endpoint_url",
        "producer_id",
        "key_id",
        "producer_install_id",
        "runtime_instance_id",
        "runtime_public_jwk_json",
        "lease_id",
        "fence",
        "next_request_token",
        "next_request_sequence",
        "expires_at",
        "assigned_relay_id",
        "pending_request_json",
        "status",
    }
    try:
        authority_scope = _runtime_scope(credentials, producer_install_id)
        uri = selected.resolve().as_uri() + "?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=2)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA query_only=ON")
            conn.execute("BEGIN")
            table = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (RUNTIME_AUTHORITY_TABLE,),
            ).fetchone()
            if table is None:
                return CanaryCheck(
                    "credential_lease_state",
                    "UNKNOWN",
                    "LEASE_STATE_TARGET_ABSENT",
                    {
                        "database_present": True,
                        "lease_table_present": False,
                        "read_only": True,
                    },
                )
            columns = {
                str(row["name"])
                for row in conn.execute(
                    f"PRAGMA table_info({RUNTIME_AUTHORITY_TABLE})"
                ).fetchall()
            }
            if not required_columns.issubset(columns):
                return CanaryCheck(
                    "credential_lease_state",
                    "FAIL",
                    "LEASE_STATE_SCHEMA_MISMATCH",
                    {
                        "database_present": True,
                        "lease_table_present": True,
                        "read_only": True,
                    },
                )
            row = conn.execute(
                f"SELECT * FROM {RUNTIME_AUTHORITY_TABLE} WHERE authority_scope=?",
                (authority_scope,),
            ).fetchone()
            conflicting_scope_count = 0
            if row is None:
                conflicting_scope_count = int(
                    conn.execute(
                        f"SELECT COUNT(*) FROM {RUNTIME_AUTHORITY_TABLE} "
                        "WHERE producer_install_id=?",
                        (str(producer_install_id or ""),),
                    ).fetchone()[0]
                )
            assigned_relay_present = False
            assigned_relay_exact = False
            assigned_metadata_exact = False
            if row is not None and str(row["assigned_relay_id"] or ""):
                assigned_relay_present = True
                relay = conn.execute(
                    "SELECT status, metadata_json, producer_id, key_id, endpoint_url, "
                    "runtime_fencing_policy FROM direct_sync_relay_batches WHERE relay_id=?",
                    (str(row["assigned_relay_id"]),),
                ).fetchone()
                assigned_relay_exact = (
                    relay is not None
                    and str(relay["status"] or "") == "leased"
                    and str(relay["producer_id"] or "") == credentials.producer_id
                    and str(relay["key_id"] or "") == credentials.key_id
                    and str(relay["endpoint_url"] or "") == credentials.endpoint_url
                    and str(relay["runtime_fencing_policy"] or "")
                    == "runtime_required"
                )
                if relay is not None:
                    try:
                        metadata = json.loads(str(relay["metadata_json"] or ""))
                        authority_public_jwk = json.loads(
                            str(row["runtime_public_jwk_json"] or "")
                        )
                        assigned_metadata_exact = (
                            isinstance(metadata, dict)
                            and str(metadata.get("producer_install_id") or "")
                            == str(row["producer_install_id"] or "")
                            and str(metadata.get("runtime_instance_id") or "")
                            == str(row["runtime_instance_id"] or "")
                            and runtime_canonical_json(
                                normalize_public_jwk(metadata.get("runtime_public_jwk"))
                            )
                            == runtime_canonical_json(
                                normalize_public_jwk(authority_public_jwk)
                            )
                            and metadata.get("runtime_fence") == row["fence"]
                            and isinstance(metadata.get("runtime_request_token"), str)
                            and _RUNTIME_TOKEN_RE.fullmatch(
                                str(metadata.get("runtime_request_token") or "")
                            )
                            is not None
                            and type(metadata.get("runtime_request_sequence")) is int
                            and int(metadata.get("runtime_request_sequence") or 0) >= 1
                        )
                    except (TypeError, ValueError, json.JSONDecodeError):
                        assigned_metadata_exact = False
        finally:
            conn.close()
    except (OSError, sqlite3.Error, ValueError):
        return CanaryCheck(
            "credential_lease_state",
            "UNKNOWN",
            "LEASE_STATE_UNAVAILABLE",
            {"database_present": True, "read_only": True},
        )

    if row is None and conflicting_scope_count:
        return CanaryCheck(
            "credential_lease_state",
            "FAIL",
            "LEASE_CREDENTIAL_BINDING_MISMATCH",
            {
                "database_present": True,
                "lease_row_present": False,
                "conflicting_scope_present": True,
                "read_only": True,
            },
        )
    if row is None:
        return CanaryCheck(
            "credential_lease_state",
            "UNKNOWN",
            "LEASE_STATE_TARGET_ABSENT",
            {
                "database_present": True,
                "lease_row_present": False,
                "read_only": True,
            },
        )
    binding_exact = (
        str(row["authority_scope"] or "") == authority_scope
        and str(row["producer_id"] or "") == credentials.producer_id
        and str(row["key_id"] or "") == credentials.key_id
        and str(row["endpoint_url"] or "") == credentials.endpoint_url
        and str(row["producer_install_id"] or "") == producer_install_id
    )
    expires_at = _parse_utc(row["expires_at"])
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    active = str(row["status"] or "") == "ACTIVE"
    try:
        public_jwk = json.loads(str(row["runtime_public_jwk_json"] or ""))
        normalize_public_jwk(public_jwk)
        runtime_identity_valid = (
            _RUNTIME_INSTANCE_RE.fullmatch(str(row["runtime_instance_id"] or ""))
            is not None
        )
    except Exception:
        runtime_identity_valid = False
    base_authority_present = (
        runtime_identity_valid
        and bool(str(row["lease_id"] or "").strip())
        and type(row["fence"]) is int
        and int(row["fence"]) >= 1
    )
    idle_authority = (
        not assigned_relay_present
        and not str(row["pending_request_json"] or "")
        and isinstance(row["next_request_token"], str)
        and _RUNTIME_TOKEN_RE.fullmatch(str(row["next_request_token"] or "")) is not None
        and type(row["next_request_sequence"]) is int
        and int(row["next_request_sequence"]) >= 1
    )
    assigned_authority = (
        assigned_relay_present
        and assigned_relay_exact
        and assigned_metadata_exact
        and not str(row["pending_request_json"] or "")
        and row["next_request_token"] is None
        and row["next_request_sequence"] is None
    )
    authority_present = base_authority_present and (idle_authority or assigned_authority)
    unexpired = expires_at is not None and expires_at > current
    evidence = {
        "database_present": True,
        "lease_row_present": True,
        "read_only": True,
        "binding_exact": binding_exact,
        "active": active,
        "runtime_identity_valid": runtime_identity_valid,
        "authority_fields_present": authority_present,
        "unexpired": unexpired,
        "assigned_relay_present": assigned_relay_present,
        "assigned_relay_exact": assigned_relay_exact if assigned_relay_present else True,
        "assigned_metadata_exact": (
            assigned_metadata_exact if assigned_relay_present else True
        ),
    }
    if not binding_exact:
        return CanaryCheck(
            "credential_lease_state",
            "FAIL",
            "LEASE_CREDENTIAL_BINDING_MISMATCH",
            evidence,
        )
    if not unexpired:
        return CanaryCheck(
            "credential_lease_state",
            "FAIL",
            "LEASE_EXPIRED_OR_INVALID",
            evidence,
        )
    if assigned_relay_present and not (
        assigned_relay_exact and assigned_metadata_exact
    ):
        return CanaryCheck(
            "credential_lease_state",
            "FAIL",
            "LEASE_ASSIGNED_RELAY_BINDING_MISMATCH",
            evidence,
        )
    if not active or not authority_present:
        return CanaryCheck(
            "credential_lease_state", "FAIL", "LEASE_STATE_NOT_ACTIVE", evidence
        )
    return CanaryCheck(
        "credential_lease_state", "PASS", "LEASE_STATE_ACTIVE", evidence
    )


def _database_snapshot(db_path: Path) -> dict[str, Any]:
    uri = db_path.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=2)
    try:
        objects = conn.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master "
            "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
        ).fetchall()
        tables = [str(row[1]) for row in objects if str(row[0]) == "table"]
        counts = {table: int(conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]) for table in tables}
        schema_text = "\n".join(
            "|".join(str(value or "") for value in row) for row in objects
        )
        quick_check = str(conn.execute("PRAGMA quick_check").fetchone()[0])
        foreign_key_violations = len(conn.execute("PRAGMA foreign_key_check").fetchall())
    finally:
        conn.close()
    return {
        "table_counts": counts,
        "schema_sha256": hashlib.sha256(schema_text.encode("utf-8")).hexdigest(),
        "quick_check": quick_check,
        "foreign_key_violations": foreign_key_violations,
    }


class _BarrierDeferredIntentCaptureStore(DeferredIntentCaptureStore):
    def __init__(self, *args: Any, barrier_path: Path, **kwargs: Any) -> None:
        self._canary_barrier_path = barrier_path
        self._canary_barrier_enabled = False
        self._canary_barrier_deadline = (
            time.monotonic() + RECOVERY_CHILD_MAXIMUM_SECONDS
        )
        super().__init__(*args, **kwargs)
        self._canary_barrier_enabled = True

    def _connect(self) -> sqlite3.Connection:
        conn = super()._connect()
        observed = {"intent_insert": False}

        def trace(statement: str) -> None:
            if not self._canary_barrier_enabled:
                return
            normalized = " ".join(str(statement or "").upper().split())
            if normalized.startswith("INSERT INTO DEFERRED_INTENTS"):
                observed["intent_insert"] = True
            if normalized.startswith("INSERT INTO DEFERRED_INTENT_TRANSITION_AUDIT"):
                write_json_atomic(
                    self._canary_barrier_path,
                    {
                        "schema": RECOVERY_BARRIER_SCHEMA,
                        "phase": RECOVERY_BARRIER_PHASE,
                        "owned_child_pid": os.getpid(),
                        "same_connection_transaction_open": bool(conn.in_transaction),
                        "intent_insert_seen": bool(observed["intent_insert"]),
                        "audit_insert_executed": False,
                    },
                )
                while time.monotonic() < self._canary_barrier_deadline:
                    time.sleep(0.2)
                raise RuntimeError("recovery canary parent did not terminate the child")

        conn.set_trace_callback(trace)
        return conn


def _recovery_binding() -> DeferredIntentBinding:
    return DeferredIntentBinding(
        producer_id="label-canary-producer",
        producer_install_id="label-canary-install",
        source_host_id="label-canary-host",
        manifest_hash=hashlib.sha256(b"label-canary-manifest").hexdigest(),
        authority_scope_id="label-canary-scope",
    )


def _run_recovery_child(args: argparse.Namespace) -> int:
    try:
        store = _BarrierDeferredIntentCaptureStore(
            Path(args._recovery_db),
            _recovery_binding(),
            seal_key_path=Path(args._seal_key_path),
            barrier_path=Path(args._barrier_path),
        )
        store.capture_label_package_source(
            local_work_identity="CANARY-" + uuid.uuid4().hex,
            physical_qr_payload="CANARY-QR-" + uuid.uuid4().hex,
            item_code="CANARY-ITEM",
        )
    except Exception:
        return 4
    return 5


def _stop_owned_child(child: subprocess.Popen[bytes]) -> bool:
    """Best-effort bounded cleanup for the exact Popen handle we created."""

    if child.poll() is not None:
        return True
    for stop in (child.kill, child.terminate):
        try:
            stop()
        except Exception:
            continue
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            continue
        if child.poll() is not None:
            return True
    try:
        child.wait(timeout=RECOVERY_CHILD_MAXIMUM_SECONDS + 2)
    except subprocess.TimeoutExpired:
        return False
    return child.poll() is not None


def probe_recovery(
    *,
    work_dir: str | os.PathLike[str],
    timeout_seconds: int = RECOVERY_TIMEOUT_SECONDS,
    protected_roots: tuple[str | os.PathLike[str], ...],
) -> CanaryCheck:
    """Kill only an owned child inside a real isolated Label transaction."""

    requested_root = Path(work_dir).expanduser()
    if requested_root.is_symlink():
        return CanaryCheck(
            "recovery",
            "FAIL",
            "RECOVERY_WORKDIR_NOT_FRESH",
            {"owned_child_started": False, "live_label_data_touched": False},
        )
    root = requested_root.resolve()
    for protected in protected_roots:
        protected_root = Path(protected).expanduser().resolve()
        if (
            root == protected_root
            or root.is_relative_to(protected_root)
            or protected_root.is_relative_to(root)
        ):
            return CanaryCheck(
                "recovery",
                "FAIL",
                "RECOVERY_WORKDIR_OVERLAPS_LIVE_INPUT",
                {
                    "owned_child_started": False,
                    "workdir_disjoint_from_protected_roots": False,
                    "protected_root_count": len(protected_roots),
                    "live_label_data_touched": False,
                },
            )
    if root.exists():
        return CanaryCheck(
            "recovery",
            "FAIL",
            "RECOVERY_WORKDIR_NOT_FRESH",
            {"owned_child_started": False, "live_label_data_touched": False},
        )
    try:
        root.mkdir(parents=True)
    except OSError:
        return CanaryCheck(
            "recovery",
            "FAIL",
            "RECOVERY_WORKDIR_CREATE_FAILED",
            {"owned_child_started": False, "live_label_data_touched": False},
        )

    db_path = root / "label-canary.sqlite3"
    seal_key_path = root / "label-canary-seal-key.dpapi"
    barrier_path = root / "barrier.json"
    stdout_path = root / "child.stdout.log"
    stderr_path = root / "child.stderr.log"
    try:
        store = DeferredIntentCaptureStore(
            db_path,
            _recovery_binding(),
            seal_key_path=seal_key_path,
        )
        store._load_or_create_seal_key()
        preimage = _database_snapshot(db_path)
        seal_key_sha256 = _sha256_file(seal_key_path)
    except Exception:
        return CanaryCheck(
            "recovery",
            "FAIL",
            "RECOVERY_SEED_FAILED",
            {"owned_child_started": False, "live_label_data_touched": False},
        )

    child_python = Path(getattr(sys, "_base_executable", sys.executable)).resolve()
    if not child_python.is_file():
        return CanaryCheck(
            "recovery",
            "FAIL",
            "RECOVERY_CHILD_INTERPRETER_ABSENT",
            {"owned_child_started": False, "live_label_data_touched": False},
        )
    command = [
        str(child_python),
        "-I",
        "-B",
        str(Path(__file__).resolve()),
        "--_recovery-child",
        "--_recovery-db",
        str(db_path),
        "--_seal-key-path",
        str(seal_key_path),
        "--_barrier-path",
        str(barrier_path),
    ]
    child: subprocess.Popen[bytes] | None = None
    barrier: dict[str, Any] = {}
    external_at_barrier: dict[str, Any] = {}
    child_exit = 0
    failure_stage = "open-child-logs"
    try:
        with stdout_path.open("xb") as stdout_handle, stderr_path.open("xb") as stderr_handle:
            failure_stage = "start-owned-child"
            child = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=stdout_handle,
                stderr=stderr_handle,
                cwd=str(REPO_ROOT),
                env=dict(os.environ),
            )
            deadline = time.monotonic() + max(2, int(timeout_seconds))
            failure_stage = "wait-for-boundary"
            while time.monotonic() < deadline:
                if barrier_path.is_file():
                    barrier = read_bounded_json_object(barrier_path, maximum_bytes=65536)
                    break
                if child.poll() is not None:
                    break
                time.sleep(0.05)
            failure_stage = "validate-boundary"
            if not barrier:
                raise RuntimeError("recovery barrier was not reached")
            failure_stage = "validate-owned-pid"
            if int(barrier.get("owned_child_pid") or 0) != child.pid:
                raise RuntimeError("recovery barrier PID differs from the owned child")
            failure_stage = "read-external-preimage"
            external_at_barrier = _database_snapshot(db_path)
            failure_stage = "terminate-owned-child"
            child.kill()
            failure_stage = "wait-for-owned-child"
            child_exit = int(child.wait(timeout=5))
    except Exception:
        owned_child_terminated = child is None or _stop_owned_child(child)
        return CanaryCheck(
            "recovery",
            "FAIL",
            "RECOVERY_BARRIER_OR_KILL_FAILED",
            {
                "owned_child_started": child is not None,
                "owned_child_terminated": owned_child_terminated,
                "failure_stage": failure_stage,
                "live_label_data_touched": False,
            },
        )

    try:
        DeferredIntentCaptureStore(
            db_path, _recovery_binding(), seal_key_path=seal_key_path
        )
        restart_one = _database_snapshot(db_path)
        DeferredIntentCaptureStore(
            db_path, _recovery_binding(), seal_key_path=seal_key_path
        )
        restart_two = _database_snapshot(db_path)
        seal_key_unchanged = _sha256_file(seal_key_path) == seal_key_sha256
    except Exception:
        return CanaryCheck(
            "recovery",
            "FAIL",
            "RECOVERY_REOPEN_FAILED",
            {
                "owned_child_started": True,
                "owned_child_terminated": True,
                "live_label_data_touched": False,
            },
        )

    barrier_exact = (
        barrier.get("schema") == RECOVERY_BARRIER_SCHEMA
        and barrier.get("phase") == RECOVERY_BARRIER_PHASE
        and barrier.get("same_connection_transaction_open") is True
        and barrier.get("intent_insert_seen") is True
        and barrier.get("audit_insert_executed") is False
    )
    external_preimage_exact = external_at_barrier == preimage
    restart_one_exact = restart_one == preimage
    restart_two_exact = restart_two == preimage
    row_count = sum(int(value) for value in restart_two["table_counts"].values())
    integrity_exact = (
        restart_one.get("quick_check") == "ok"
        and restart_two.get("quick_check") == "ok"
        and restart_one.get("foreign_key_violations") == 0
        and restart_two.get("foreign_key_violations") == 0
        and row_count == 0
    )
    passed = (
        barrier_exact
        and external_preimage_exact
        and child_exit != 0
        and restart_one_exact
        and restart_two_exact
        and integrity_exact
        and seal_key_unchanged
    )
    evidence = {
        "phase": str(barrier.get("phase") or ""),
        "owned_child_started": True,
        "owned_child_terminated": child_exit != 0,
        "same_connection_transaction_open": bool(
            barrier.get("same_connection_transaction_open")
        ),
        "intent_insert_seen": bool(barrier.get("intent_insert_seen")),
        "audit_insert_executed": bool(barrier.get("audit_insert_executed")),
        "external_preimage_visible": external_preimage_exact,
        "restart_one_exact": restart_one_exact,
        "restart_two_exact": restart_two_exact,
        "quick_check_ok": restart_two.get("quick_check") == "ok",
        "foreign_key_violations": int(
            restart_two.get("foreign_key_violations") or 0
        ),
        "post_recovery_row_count": row_count,
        "schema_sha256": str(restart_two.get("schema_sha256") or ""),
        "seal_key_unchanged": seal_key_unchanged,
        "barrier_sha256": _sha256_file(barrier_path),
        "child_stdout_bytes": stdout_path.stat().st_size,
        "child_stderr_bytes": stderr_path.stat().st_size,
        "live_label_data_touched": False,
        "workdir_disjoint_from_protected_roots": True,
        "protected_root_count": len(protected_roots),
    }
    return CanaryCheck(
        "recovery",
        "PASS" if passed else "FAIL",
        "RECOVERY_ROLLBACK_IDEMPOTENT" if passed else "RECOVERY_ROLLBACK_MISMATCH",
        evidence,
    )


def run_canary(
    *,
    credential_path: str,
    auth_target_path: str,
    relay_db_path: str,
    work_dir: str,
    report_path: str,
    auth_injection: str = "none",
    timeout_seconds: int = 10,
    tls_ca_bundle_path: str = "",
    session: Any = None,
    live_roots: tuple[str | os.PathLike[str], ...] = (),
) -> dict[str, Any]:
    started_text = utc_now_text()
    started = time.monotonic()
    credential_context: CredentialContext | None = None
    credential_input_invalid = False
    target_metadata: dict[str, Any] | None = None
    target_input_invalid = False

    if credential_path:
        try:
            credential_context = _load_credential_context(credential_path)
        except FileNotFoundError:
            credential_context = None
        except Exception:
            credential_input_invalid = True
    if auth_target_path:
        try:
            target_metadata = _load_auth_target(auth_target_path)
        except FileNotFoundError:
            target_metadata = None
        except Exception:
            target_input_invalid = True

    if not live_roots:
        raise CanaryContractError("at least one explicit live root is required")
    protected_root_set = {REPO_ROOT.resolve()}
    protected_root_set.update(Path(value).expanduser().resolve() for value in live_roots)
    protected_root_set.update(
        Path(value).expanduser().resolve().parent
        for value in (
            credential_path,
            auth_target_path,
            relay_db_path,
            tls_ca_bundle_path,
        )
        if str(value or "").strip()
    )
    if credential_context is not None:
        protected_root_set.update(credential_context.protected_roots)
    protected_roots = tuple(
        str(path) for path in sorted(protected_root_set, key=lambda item: str(item).casefold())
    )
    selected_report_path = Path(report_path).expanduser().resolve()
    if any(
        selected_report_path == Path(root)
        or selected_report_path.is_relative_to(Path(root))
        for root in protected_roots
    ):
        raise CanaryContractError("report path overlaps a protected live root")

    if credential_input_invalid:
        authentication = CanaryCheck(
            "authentication",
            "FAIL",
            "CREDENTIAL_INPUT_INVALID",
            {"credential_input_present": True, "request_sent": False},
        )
    elif target_input_invalid:
        authentication = CanaryCheck(
            "authentication",
            "FAIL",
            "AUTH_TARGET_INPUT_INVALID",
            {"target_input_present": True, "request_sent": False},
        )
    elif credential_context is None:
        authentication = CanaryCheck(
            "authentication",
            "UNKNOWN",
            "CREDENTIAL_INPUT_ABSENT",
            {"credential_input_present": False, "request_sent": False},
        )
    elif target_metadata is None:
        authentication = CanaryCheck(
            "authentication",
            "UNKNOWN",
            "AUTH_TARGET_INPUT_ABSENT",
            {"target_input_present": False, "request_sent": False},
        )
    else:
        authentication = probe_authentication(
            credentials=credential_context.credentials,
            metadata=target_metadata,
            injection=auth_injection,
            session=session,
            timeout_seconds=timeout_seconds,
            tls_ca_bundle_path=tls_ca_bundle_path,
        )

    if credential_input_invalid:
        lease = CanaryCheck(
            "credential_lease_state",
            "FAIL",
            "CREDENTIAL_INPUT_INVALID",
            {"credential_input_present": True, "read_only": True},
        )
    elif target_input_invalid:
        lease = CanaryCheck(
            "credential_lease_state",
            "FAIL",
            "AUTH_TARGET_INPUT_INVALID",
            {"target_input_present": True, "read_only": True},
        )
    elif credential_context is None:
        lease = CanaryCheck(
            "credential_lease_state",
            "UNKNOWN",
            "CREDENTIAL_INPUT_ABSENT",
            {"credential_input_present": False, "read_only": True},
        )
    elif target_metadata is None:
        lease = CanaryCheck(
            "credential_lease_state",
            "UNKNOWN",
            "LEASE_BINDING_TARGET_ABSENT",
            {"target_input_present": False, "read_only": True},
        )
    else:
        lease = probe_credential_lease_state(
            db_path=relay_db_path,
            credentials=credential_context.credentials,
            producer_install_id=str(target_metadata.get("producer_install_id") or ""),
        )

    recovery = probe_recovery(
        work_dir=str(Path(work_dir).expanduser().resolve() / "recovery"),
        timeout_seconds=RECOVERY_TIMEOUT_SECONDS,
        protected_roots=protected_roots,
    )
    checks = [authentication, lease, recovery]
    report = build_canary_report(
        app_id=APP_ID,
        checks=checks,
        started_at_utc=started_text,
        completed_at_utc=utc_now_text(),
        duration_ms=round((time.monotonic() - started) * 1000),
        required_check_names=REQUIRED_CHECKS,
    )
    forbidden_values = (
        credential_context.forbidden_values if credential_context is not None else ()
    )
    assert_forbidden_values_absent(report, forbidden_values)
    write_json_atomic(report_path, report)
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the Label_Match auth/recovery post-deploy canary"
    )
    parser.add_argument("--credential-path", default="")
    parser.add_argument("--auth-target-path", default="")
    parser.add_argument("--relay-db-path", default="")
    parser.add_argument("--work-dir", default="")
    parser.add_argument("--report-path", default="")
    parser.add_argument("--auth-injection", choices=AUTH_INJECTIONS, default="none")
    parser.add_argument("--timeout-seconds", type=int, default=10)
    parser.add_argument("--tls-ca-bundle-path", default="")
    parser.add_argument(
        "--live-root",
        action="append",
        default=[],
        help="Required live Label app/data root; repeat for every distinct root",
    )
    parser.add_argument("--_recovery-child", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--_recovery-db", default="", help=argparse.SUPPRESS)
    parser.add_argument("--_seal-key-path", default="", help=argparse.SUPPRESS)
    parser.add_argument("--_barrier-path", default="", help=argparse.SUPPRESS)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args._recovery_child:
        return _run_recovery_child(args)
    if not args.work_dir or not args.report_path:
        print("canary_status=FAIL")
        print("canary_error=work-dir-and-report-path-required")
        return 1
    try:
        report = run_canary(
            credential_path=args.credential_path,
            auth_target_path=args.auth_target_path,
            relay_db_path=args.relay_db_path,
            work_dir=args.work_dir,
            report_path=args.report_path,
            auth_injection=args.auth_injection,
            timeout_seconds=args.timeout_seconds,
            tls_ca_bundle_path=args.tls_ca_bundle_path,
            live_roots=tuple(args.live_root),
        )
    except Exception:
        print("canary_status=FAIL")
        print("canary_error=report-boundary-failed")
        return 1
    print(f"canary_status={report['status']}")
    print(f"canary_report={Path(args.report_path).expanduser().resolve()}")
    if report["status"] == "PASS":
        return 0
    if report["status"] == "UNKNOWN":
        return 2
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

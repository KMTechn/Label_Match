# -*- coding: utf-8 -*-
"""Durable producer-runtime lease state for the direct-sync relay.

The rotating request token is reserved to one relay row before network I/O.
That row then owns the exact metadata until a committed receipt returns the
next token.  The authority scope is deliberately bound to the complete
credential/endpoint/install tuple so a credential change cannot inherit it.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import sqlite3
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Mapping
from urllib.parse import urlparse, urlunparse

from kmtech_shared import runtime as _runtime_core

from kmtech_zero_pe import (
    generate_public_jwk,
    jwk_thumbprint as _cng_jwk_thumbprint,
    normalize_public_jwk,
)


CONTRACT_VERSION = "producer-runtime-lease.v1"
ENDPOINT_PATH = "/api/producer-ingest/v1/runtime-lease"
CONTENT_TYPE = "application/json"
SIGNATURE_VERSION = "PRODUCER-HMAC-SHA256-V1"
METADATA_FIELDS = (
    "runtime_instance_id",
    "runtime_public_jwk",
    "runtime_fence",
    "runtime_request_token",
    "runtime_request_sequence",
)
OPERATOR_REVIEW_CODES = frozenset(
    {
        "EXACT_CLONE_RUNTIME_CONFLICT",
        "STALE_RUNTIME_FENCE",
        "STALE_RUNTIME_REQUEST_TOKEN",
    }
)
CLIENT_RUNTIME_LEASE_MODES = frozenset({"observe", "enforce"})
LEGACY_DISABLED_STATUS = "LEGACY_DISABLED"
RUNTIME_FENCING_POLICY_RUNTIME_REQUIRED = "runtime_required"
RUNTIME_FENCING_POLICY_LEGACY_EXACT_REPLAY = "legacy_exact_replay"
RUNTIME_FENCING_POLICIES = frozenset(
    {
        RUNTIME_FENCING_POLICY_RUNTIME_REQUIRED,
        RUNTIME_FENCING_POLICY_LEGACY_EXACT_REPLAY,
    }
)
_TOKEN_RE = re.compile(r"[A-Za-z0-9_-]{32,256}")
_COORDINATE_RE = re.compile(r"[A-Za-z0-9_-]{43}")
_DEFAULT_TTL_SECONDS = 15 * 60
_MAX_RETRY_AFTER_SECONDS = 24 * 60 * 60
_BUSY_TIMEOUT_MS = 30000


@dataclass(frozen=True)
class RuntimePreparation:
    metadata: Dict[str, Any] | None = field(default=None, repr=False)
    status_code: int = 0
    retryable: bool = False
    operator_review: bool = False
    retry_after_seconds: int | None = None
    error_code: str = ""
    error_message: str = ""
    receipt: Dict[str, Any] = field(default_factory=dict)


def client_runtime_lease_mode(credentials: Any) -> str:
    mode = str(getattr(credentials, "runtime_lease_mode", "enforce") or "enforce").strip().lower()
    if mode not in CLIENT_RUNTIME_LEASE_MODES:
        raise ValueError("runtime_lease_mode must be observe or enforce")
    return mode


def _normalize_for_json(value: Any) -> Any:
    return _runtime_core._normalize_for_json(value)


def canonical_json(value: Mapping[str, Any]) -> str:
    return _runtime_core.canonical_json(value)


def _canonical_request(
    *,
    timestamp: str,
    nonce: str,
    producer_id: str,
    key_id: str,
    body: Mapping[str, Any],
) -> str:
    return _runtime_core._canonical_request(
        timestamp=timestamp,
        nonce=nonce,
        producer_id=producer_id,
        key_id=key_id,
        body=body,
    )


def _utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_time(value: Any) -> datetime | None:
    return _runtime_core._parse_time(value)


def _jwk_thumbprint(public_jwk: Mapping[str, Any]) -> str:
    return _runtime_core._jwk_thumbprint(public_jwk, _cng_jwk_thumbprint=_cng_jwk_thumbprint)


def new_runtime_identity() -> tuple[str, Dict[str, str]]:
    return f"runtime-{uuid.uuid4().hex}", generate_public_jwk()


def _scope_values(credentials: Any, producer_install_id: str) -> Dict[str, str]:
    return _runtime_core._scope_values(credentials, producer_install_id)


def _scope_key(values: Mapping[str, str]) -> str:
    return _runtime_core._scope_key(values)


def _runtime_endpoint(endpoint_url: str) -> str:
    parsed = urlparse(str(endpoint_url or ""))
    if parsed.scheme.lower() != "https" or not parsed.netloc or parsed.username or parsed.password:
        raise ValueError("runtime lease endpoint must use credential-free HTTPS")
    return urlunparse((parsed.scheme, parsed.netloc, ENDPOINT_PATH, "", "", ""))


def init_runtime_schema(conn: sqlite3.Connection) -> None:
    return _runtime_core.init_runtime_schema(conn)


def _connect(db_path: str | os.PathLike[str]) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=_BUSY_TIMEOUT_MS / 1000)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    init_runtime_schema(conn)
    return conn


def _metadata_shape_error(metadata: Mapping[str, Any]) -> str:
    return _runtime_core._metadata_shape_error(metadata, normalize_public_jwk=normalize_public_jwk)


def redact_runtime_secrets(value: Any) -> Any:
    """Remove rotating authority from logs/status payloads while preserving shape."""
    return _runtime_core.redact_runtime_secrets(value)


def _redact_known_values(value: Any, sensitive_values: tuple[Any, ...]) -> Any:
    return _runtime_core._redact_known_values(value, sensitive_values)


def scrub_terminal_runtime_metadata(metadata: Mapping[str, Any]) -> Dict[str, Any]:
    """Remove the consumed one-shot token while retaining an audit digest."""
    return _runtime_core.scrub_terminal_runtime_metadata(metadata)


def _retry_after(value: Any) -> int | None:
    return _runtime_core._retry_after(value)


def _response_payload(response: Any) -> Dict[str, Any]:
    try:
        value = response.json()
    except Exception:
        return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _post_lease_request(
    *,
    request_value: Mapping[str, Any],
    credentials: Any,
    session: Any,
    timeout: int,
    tls_ca_bundle_path: str = "",
) -> tuple[Dict[str, Any] | None, RuntimePreparation | None]:
    if session is None:
        import requests

        session = requests.Session()
    body = canonical_json(request_value).encode("utf-8")
    timestamp = _utc_now_text()
    nonce = uuid.uuid4().hex
    canonical = _canonical_request(
        timestamp=timestamp,
        nonce=nonce,
        producer_id=str(credentials.producer_id),
        key_id=str(credentials.key_id),
        body=request_value,
    )
    secret = credentials.secret
    secret_bytes = secret.encode("utf-8") if isinstance(secret, str) else secret
    signature = hmac.new(secret_bytes, canonical.encode("utf-8"), hashlib.sha256).hexdigest()
    headers = {
        "Content-Type": CONTENT_TYPE,
        "X-Producer-Id": str(credentials.producer_id),
        "X-Producer-Key-Id": str(credentials.key_id),
        "X-Producer-Timestamp": timestamp,
        "X-Producer-Nonce": nonce,
        "X-Producer-Signature": signature,
    }
    try:
        request_kwargs: Dict[str, Any] = {
            "data": body,
            "headers": headers,
            "timeout": timeout,
            "allow_redirects": False,
        }
        selected_ca = str(tls_ca_bundle_path or "").strip()
        if selected_ca:
            request_kwargs["verify"] = selected_ca
        response = session.post(
            _runtime_endpoint(str(credentials.endpoint_url)),
            **request_kwargs,
        )
    except Exception as exc:
        return None, RuntimePreparation(
            retryable=True,
            error_code="runtime_lease_transport_error",
            error_message=f"runtime lease transport error: {exc.__class__.__name__}",
        )
    status_code = int(getattr(response, "status_code", 0) or 0)
    payload = _response_payload(response)
    sensitive_values = (
        request_value.get("runtime_request_token"),
        credentials.secret,
        signature,
        "X-Producer-Signature",
        SIGNATURE_VERSION,
    )
    safe_payload = _redact_known_values(redact_runtime_secrets(payload), sensitive_values)
    response_headers = getattr(response, "headers", {}) or {}
    retry_after = _retry_after(response_headers.get("Retry-After") if hasattr(response_headers, "get") else "")
    if 200 <= status_code < 300:
        if not payload:
            return None, RuntimePreparation(
                status_code=status_code,
                operator_review=True,
                error_code="runtime_lease_response_invalid",
                error_message="runtime lease response is not valid JSON",
            )
        return payload, None
    error = payload.get("error") if isinstance(payload.get("error"), Mapping) else {}
    code = str(error.get("code") or "runtime_lease_request_failed")
    message = str(error.get("message") or f"runtime lease request failed with HTTP {status_code}")
    message = str(_redact_known_values(message, sensitive_values))[:500]
    retryable = payload.get("retryable") is True or status_code in {408, 429, 500, 502, 503, 504}
    operator_review = code in OPERATOR_REVIEW_CODES or bool(payload.get("operator_review"))
    return None, RuntimePreparation(
        status_code=status_code,
        retryable=retryable and not operator_review,
        operator_review=operator_review,
        retry_after_seconds=retry_after if retryable and not operator_review else None,
        error_code=code,
        error_message=message,
        receipt=safe_payload,
    )


def _grant_error(
    grant: Mapping[str, Any],
    *,
    state: sqlite3.Row,
    scope: Mapping[str, str],
    request_value: Mapping[str, Any],
) -> str:
    return _runtime_core._grant_error(
        grant,
        state=state,
        scope=scope,
        request_value=request_value,
        _cng_jwk_thumbprint=_cng_jwk_thumbprint,
    )


def _state_row(conn: sqlite3.Connection, authority_scope: str) -> sqlite3.Row | None:
    return _runtime_core._state_row(conn, authority_scope)


def _state_scope_matches(state: sqlite3.Row, scope: Mapping[str, str]) -> bool:
    return _runtime_core._state_scope_matches(state, scope)


def _create_state(conn: sqlite3.Connection, scope: Mapping[str, str], now_text: str) -> sqlite3.Row:
    return _runtime_core._create_state(
        conn,
        scope,
        now_text,
        new_runtime_identity=new_runtime_identity,
    )


def _replace_expired_identity(
    conn: sqlite3.Connection, state: sqlite3.Row, now_text: str
) -> sqlite3.Row:
    return _runtime_core._replace_expired_identity(
        conn,
        state,
        now_text,
        new_runtime_identity=new_runtime_identity,
    )


def _lease_request_value(state: sqlite3.Row, ttl_seconds: int) -> Dict[str, Any]:
    return _runtime_core._lease_request_value(state, ttl_seconds)


def _load_live_relay_metadata(conn: sqlite3.Connection, relay_id: str) -> Dict[str, Any]:
    row = conn.execute(
        "SELECT metadata_json FROM direct_sync_relay_batches WHERE relay_id=?",
        (relay_id,),
    ).fetchone()
    if row is None:
        raise ValueError("relay row disappeared while reserving runtime authority")
    value = json.loads(str(row["metadata_json"] or ""))
    if not isinstance(value, dict):
        raise ValueError("relay metadata is invalid")
    return value


def _runtime_liveness_receipt(
    state: sqlite3.Row,
    *,
    producer_install_id: str,
    request_sent: bool,
) -> Dict[str, Any]:
    """Return non-secret evidence that a server grant is still current."""
    return _runtime_core._runtime_liveness_receipt(
        state,
        producer_install_id=producer_install_id,
        request_sent=request_sent,
    )


def ensure_runtime_authority(
    *,
    db_path: str | os.PathLike[str],
    credentials: Any,
    producer_install_id: str,
    session: Any = None,
    timeout: int = 30,
    now: str = "",
    ttl_seconds: int = _DEFAULT_TTL_SECONDS,
    renewal_margin_seconds: int = 120,
    tls_ca_bundle_path: str = "",
) -> RuntimePreparation:
    """Issue or renew install-scoped liveness without consuming row authority."""

    try:
        runtime_mode = client_runtime_lease_mode(credentials)
        scope = _scope_values(credentials, str(producer_install_id or "").strip())
        _runtime_endpoint(scope["endpoint_url"])
    except (TypeError, ValueError) as exc:
        return RuntimePreparation(
            operator_review=True,
            error_code="runtime_authority_scope_invalid",
            error_message=str(exc),
        )
    if isinstance(ttl_seconds, bool) or not 1 <= int(ttl_seconds) <= 24 * 60 * 60:
        return RuntimePreparation(
            operator_review=True,
            error_code="runtime_lease_ttl_invalid",
            error_message="runtime lease TTL must be between 1 and 86400 seconds",
        )
    ttl_seconds = int(ttl_seconds)
    renewal_margin_seconds = max(0, min(int(renewal_margin_seconds), ttl_seconds - 1))
    authority_scope = _scope_key(scope)
    now_text = now or _utc_now_text()
    now_time = _parse_time(now_text) or datetime.now(timezone.utc)
    request_json = ""
    request_value: Dict[str, Any] = {}
    for _ in range(4):
        conn = _connect(db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            state = _state_row(conn, authority_scope)
            if state is None:
                state = _create_state(conn, scope, now_text)
            if not _state_scope_matches(state, scope):
                conn.rollback()
                return RuntimePreparation(
                    operator_review=True,
                    error_code="runtime_authority_scope_mismatch",
                    error_message="runtime authority credential scope does not match its database key",
                )
            if str(state["status"] or "") == LEGACY_DISABLED_STATUS:
                if runtime_mode == "observe":
                    conn.commit()
                    return RuntimePreparation(
                        receipt={
                            "contract_version": CONTRACT_VERSION,
                            "status": LEGACY_DISABLED_STATUS,
                            "server_grant_accepted": False,
                            "producer_install_id": scope["producer_install_id"],
                            "request_sent": False,
                        }
                    )
                state = _replace_expired_identity(conn, state, now_text)
            if str(state["status"] or "") == "OPERATOR_REVIEW":
                code = str(state["last_error_code"] or "runtime_authority_operator_review")
                conn.rollback()
                return RuntimePreparation(
                    operator_review=True,
                    error_code=code,
                    error_message="runtime authority requires operator review",
                )
            expires_at = _parse_time(state["expires_at"])
            if (
                expires_at is not None
                and expires_at <= now_time
                and not state["assigned_relay_id"]
                and not state["pending_request_json"]
            ):
                state = _replace_expired_identity(conn, state, now_text)
                expires_at = None
            renew_before = now_time + timedelta(seconds=renewal_margin_seconds)
            active = bool(
                str(state["status"] or "") == "ACTIVE"
                and state["lease_id"]
                and state["fence"]
                and state["next_request_token"]
                and state["next_request_sequence"]
                and expires_at is not None
                and expires_at > renew_before
                and not state["pending_request_json"]
            )
            if active:
                receipt = _runtime_liveness_receipt(
                    state,
                    producer_install_id=scope["producer_install_id"],
                    request_sent=False,
                )
                conn.commit()
                return RuntimePreparation(status_code=200, receipt=receipt)
            if state["assigned_relay_id"]:
                receipt = _runtime_liveness_receipt(
                    state,
                    producer_install_id=scope["producer_install_id"],
                    request_sent=False,
                )
                receipt["server_grant_accepted"] = bool(
                    str(state["status"] or "") == "ACTIVE"
                    and state["lease_id"]
                    and state["fence"]
                    and state["expires_at"]
                )
                receipt["request_in_flight"] = True
                conn.commit()
                return RuntimePreparation(
                    status_code=200,
                    receipt=receipt,
                )
            pending_json = str(state["pending_request_json"] or "")
            if pending_json:
                request_json = pending_json
                request_value = json.loads(pending_json)
            else:
                request_value = _lease_request_value(state, ttl_seconds)
                request_json = canonical_json(request_value)
                cursor = conn.execute(
                    """
                    UPDATE direct_sync_runtime_authority
                    SET pending_request_json=?, pending_issue_idempotency_key=?,
                        status='PENDING', updated_at=?
                    WHERE authority_scope=? AND pending_request_json IS NULL
                      AND assigned_relay_id IS NULL
                    """,
                    (
                        request_json,
                        request_value["issue_idempotency_key"],
                        now_text,
                        authority_scope,
                    ),
                )
                if cursor.rowcount != 1:
                    conn.rollback()
                    time.sleep(0.01)
                    continue
            conn.commit()
        except Exception as exc:
            if conn.in_transaction:
                conn.rollback()
            return RuntimePreparation(
                operator_review=True,
                error_code="runtime_state_invalid",
                error_message=f"runtime authority state is invalid: {exc.__class__.__name__}",
            )
        finally:
            conn.close()

        grant, request_error = _post_lease_request(
            request_value=request_value,
            credentials=credentials,
            session=session,
            timeout=timeout,
            tls_ca_bundle_path=tls_ca_bundle_path,
        )
        if request_error is not None:
            if request_error.operator_review:
                conn = _connect(db_path)
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    conn.execute(
                        """
                        UPDATE direct_sync_runtime_authority
                        SET status='OPERATOR_REVIEW', last_error_code=?, updated_at=?
                        WHERE authority_scope=? AND pending_request_json=?
                        """,
                        (request_error.error_code, now_text, authority_scope, request_json),
                    )
                    conn.commit()
                finally:
                    conn.close()
            return request_error
        assert grant is not None
        conn = _connect(db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            state = _state_row(conn, authority_scope)
            if state is None:
                conn.rollback()
                return RuntimePreparation(
                    operator_review=True,
                    error_code="runtime_state_missing",
                    error_message="runtime authority state disappeared during lease acquisition",
                )
            grant_error = _grant_error(grant, state=state, scope=scope, request_value=request_value)
            if grant_error:
                conn.execute(
                    """
                    UPDATE direct_sync_runtime_authority
                    SET status='OPERATOR_REVIEW', last_error_code=?, updated_at=?
                    WHERE authority_scope=?
                    """,
                    ("runtime_lease_response_invalid", now_text, authority_scope),
                )
                conn.commit()
                return RuntimePreparation(
                    status_code=200,
                    operator_review=True,
                    error_code="runtime_lease_response_invalid",
                    error_message=grant_error,
                    receipt=redact_runtime_secrets(grant),
                )
            cursor = conn.execute(
                """
                UPDATE direct_sync_runtime_authority
                SET lease_id=?, fence=?, next_request_token=?,
                    next_request_sequence=?, expires_at=?, assigned_relay_id=NULL,
                    pending_request_json=NULL, pending_issue_idempotency_key=NULL,
                    status='ACTIVE', last_error_code=NULL, updated_at=?
                WHERE authority_scope=? AND pending_request_json=?
                  AND assigned_relay_id IS NULL
                """,
                (
                    str(grant["lease_id"]),
                    int(grant["fence"]),
                    str(grant["next_request_token"]),
                    int(grant["next_request_sequence"]),
                    str(grant["expires_at"]),
                    now_text,
                    authority_scope,
                    request_json,
                ),
            )
            if cursor.rowcount == 1:
                state = _state_row(conn, authority_scope)
                assert state is not None
                receipt = _runtime_liveness_receipt(
                    state,
                    producer_install_id=scope["producer_install_id"],
                    request_sent=True,
                )
                conn.commit()
                return RuntimePreparation(status_code=200, receipt=receipt)
            conn.rollback()
            time.sleep(0.01)
        finally:
            conn.close()
    return RuntimePreparation(
        retryable=True,
        retry_after_seconds=1,
        error_code="runtime_authority_busy",
        error_message="runtime authority changed concurrently; retry the liveness tick",
    )


def prepare_runtime_metadata(
    *,
    db_path: str | os.PathLike[str],
    relay_id: str,
    metadata: Mapping[str, Any],
    credentials: Any,
    expected_lease_owner: str,
    expected_attempt_count: int,
    runtime_fencing_policy: str = RUNTIME_FENCING_POLICY_RUNTIME_REQUIRED,
    session: Any = None,
    timeout: int = 30,
    now: str = "",
    ttl_seconds: int = _DEFAULT_TTL_SECONDS,
    tls_ca_bundle_path: str = "",
) -> RuntimePreparation:
    """Return an immutable runtime metadata snapshot for one claimed row."""

    try:
        runtime_mode = client_runtime_lease_mode(credentials)
    except ValueError as exc:
        return RuntimePreparation(
            operator_review=True,
            error_code="runtime_lease_mode_invalid",
            error_message=str(exc),
        )
    policy = str(runtime_fencing_policy or "").strip().lower()
    if policy not in RUNTIME_FENCING_POLICIES:
        return RuntimePreparation(
            operator_review=True,
            error_code="runtime_fencing_policy_invalid",
            error_message="runtime fencing policy must be runtime_required or legacy_exact_replay",
        )
    shape_error = _metadata_shape_error(metadata)
    present = [field_name for field_name in METADATA_FIELDS if field_name in metadata]
    if present:
        if shape_error:
            return RuntimePreparation(
                operator_review=True,
                error_code="runtime_lease_metadata_invalid",
                error_message=shape_error,
            )
        return RuntimePreparation(metadata=dict(metadata))
    if policy == RUNTIME_FENCING_POLICY_LEGACY_EXACT_REPLAY:
        return RuntimePreparation(metadata=dict(metadata))
    try:
        scope = _scope_values(credentials, str(metadata.get("producer_install_id") or ""))
        _runtime_endpoint(scope["endpoint_url"])
    except (TypeError, ValueError) as exc:
        return RuntimePreparation(
            operator_review=True,
            error_code="runtime_authority_scope_invalid",
            error_message=str(exc),
        )
    authority_scope = _scope_key(scope)
    now_text = now or _utc_now_text()
    now_time = _parse_time(now_text) or datetime.now(timezone.utc)
    request_json = ""
    request_value: Dict[str, Any] = {}
    for _ in range(4):
        conn = _connect(db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            live_metadata = _load_live_relay_metadata(conn, relay_id)
            live_row = conn.execute(
                "SELECT runtime_fencing_policy FROM direct_sync_relay_batches WHERE relay_id=?",
                (relay_id,),
            ).fetchone()
            if live_row is None or str(live_row["runtime_fencing_policy"] or "") != policy:
                conn.rollback()
                return RuntimePreparation(
                    retryable=True,
                    error_code="relay_lease_lost",
                    error_message="relay runtime fencing policy changed before authority reservation",
                )
            live_present = [field_name for field_name in METADATA_FIELDS if field_name in live_metadata]
            live_shape_error = _metadata_shape_error(live_metadata)
            if live_present:
                if live_shape_error:
                    conn.rollback()
                    return RuntimePreparation(
                        operator_review=True,
                        error_code="runtime_lease_metadata_invalid",
                        error_message=live_shape_error,
                    )
                conn.commit()
                return RuntimePreparation(metadata=live_metadata)
            state = _state_row(conn, authority_scope)
            if state is None:
                state = _create_state(conn, scope, now_text)
            if not _state_scope_matches(state, scope):
                conn.rollback()
                return RuntimePreparation(
                    operator_review=True,
                    error_code="runtime_authority_scope_mismatch",
                    error_message="runtime authority credential scope does not match its database key",
                )
            if str(state["status"] or "") == LEGACY_DISABLED_STATUS:
                if runtime_mode == "observe":
                    conn.commit()
                    return RuntimePreparation(metadata=live_metadata)
                state = _replace_expired_identity(conn, state, now_text)
            if str(state["status"] or "") == "OPERATOR_REVIEW":
                code = str(state["last_error_code"] or "runtime_authority_operator_review")
                conn.rollback()
                return RuntimePreparation(
                    operator_review=True,
                    error_code=code,
                    error_message="runtime authority requires operator review",
                )
            expires_at = _parse_time(state["expires_at"])
            if (
                expires_at is not None
                and expires_at <= now_time
                and not state["assigned_relay_id"]
                and not state["pending_request_json"]
            ):
                state = _replace_expired_identity(conn, state, now_text)
                expires_at = None
            assigned_relay_id = str(state["assigned_relay_id"] or "")
            if assigned_relay_id and assigned_relay_id != relay_id:
                conn.commit()
                return RuntimePreparation(
                    retryable=True,
                    retry_after_seconds=1,
                    error_code="runtime_request_in_flight",
                    error_message="another relay row owns the current runtime request authority",
                )
            pending_json = str(state["pending_request_json"] or "")
            renew_before = now_time + timedelta(seconds=max(60, int(timeout) + 30))
            token_available = bool(
                state["next_request_token"]
                and state["next_request_sequence"]
                and state["fence"]
                and expires_at is not None
                and expires_at > renew_before
                and not pending_json
            )
            if token_available:
                public_jwk = json.loads(str(state["runtime_public_jwk_json"]))
                attached = dict(live_metadata)
                attached.update(
                    {
                        "runtime_instance_id": str(state["runtime_instance_id"]),
                        "runtime_public_jwk": public_jwk,
                        "runtime_fence": int(state["fence"]),
                        "runtime_request_token": str(state["next_request_token"]),
                        "runtime_request_sequence": int(state["next_request_sequence"]),
                    }
                )
                cursor = conn.execute(
                    """
                    UPDATE direct_sync_relay_batches
                    SET metadata_json=?, updated_at=?
                    WHERE relay_id=? AND status='leased' AND lease_owner=?
                      AND attempt_count=? AND runtime_fencing_policy=?
                    """,
                    (
                        canonical_json(attached),
                        now_text,
                        relay_id,
                        expected_lease_owner,
                        expected_attempt_count,
                        RUNTIME_FENCING_POLICY_RUNTIME_REQUIRED,
                    ),
                )
                if cursor.rowcount != 1:
                    conn.rollback()
                    return RuntimePreparation(
                        retryable=True,
                        error_code="relay_lease_lost",
                        error_message="relay lease changed before runtime authority was reserved",
                    )
                cursor = conn.execute(
                    """
                    UPDATE direct_sync_runtime_authority
                    SET next_request_token=NULL, next_request_sequence=NULL,
                        assigned_relay_id=?, status='ACTIVE', updated_at=?
                    WHERE authority_scope=? AND assigned_relay_id IS NULL
                      AND next_request_token=? AND next_request_sequence=?
                    """,
                    (
                        relay_id,
                        now_text,
                        authority_scope,
                        str(state["next_request_token"]),
                        int(state["next_request_sequence"]),
                    ),
                )
                if cursor.rowcount != 1:
                    conn.rollback()
                    time.sleep(0.01)
                    continue
                conn.commit()
                return RuntimePreparation(metadata=attached)
            if pending_json:
                request_json = pending_json
                request_value = json.loads(pending_json)
            else:
                request_value = _lease_request_value(state, ttl_seconds)
                request_json = canonical_json(request_value)
                conn.execute(
                    """
                    UPDATE direct_sync_runtime_authority
                    SET pending_request_json=?, pending_issue_idempotency_key=?,
                        status='PENDING', updated_at=?
                    WHERE authority_scope=? AND pending_request_json IS NULL
                    """,
                    (
                        request_json,
                        request_value["issue_idempotency_key"],
                        now_text,
                        authority_scope,
                    ),
                )
            conn.commit()
        except Exception as exc:
            if conn.in_transaction:
                conn.rollback()
            return RuntimePreparation(
                operator_review=True,
                error_code="runtime_state_invalid",
                error_message=f"runtime authority state is invalid: {exc.__class__.__name__}",
            )
        finally:
            conn.close()

        grant, request_error = _post_lease_request(
            request_value=request_value,
            credentials=credentials,
            session=session,
            timeout=timeout,
            tls_ca_bundle_path=tls_ca_bundle_path,
        )
        if request_error is not None:
            if request_error.operator_review:
                conn = _connect(db_path)
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    conn.execute(
                        """
                        UPDATE direct_sync_runtime_authority
                        SET status='OPERATOR_REVIEW', last_error_code=?, updated_at=?
                        WHERE authority_scope=? AND pending_request_json=?
                        """,
                        (request_error.error_code, now_text, authority_scope, request_json),
                    )
                    conn.commit()
                finally:
                    conn.close()
            return request_error
        assert grant is not None
        conn = _connect(db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            state = _state_row(conn, authority_scope)
            if state is None:
                conn.rollback()
                return RuntimePreparation(
                    operator_review=True,
                    error_code="runtime_state_missing",
                    error_message="runtime authority state disappeared during lease acquisition",
                )
            grant_error = _grant_error(grant, state=state, scope=scope, request_value=request_value)
            if grant_error:
                conn.execute(
                    """
                    UPDATE direct_sync_runtime_authority
                    SET status='OPERATOR_REVIEW', last_error_code=?, updated_at=?
                    WHERE authority_scope=?
                    """,
                    ("runtime_lease_response_invalid", now_text, authority_scope),
                )
                conn.commit()
                return RuntimePreparation(
                    status_code=200,
                    operator_review=True,
                    error_code="runtime_lease_response_invalid",
                    error_message=grant_error,
                    receipt=redact_runtime_secrets(grant),
                )
            cursor = conn.execute(
                """
                UPDATE direct_sync_runtime_authority
                SET lease_id=?, fence=?, next_request_token=?,
                    next_request_sequence=?, expires_at=?, assigned_relay_id=NULL,
                    pending_request_json=NULL, pending_issue_idempotency_key=NULL,
                    status='ACTIVE', last_error_code=NULL, updated_at=?
                WHERE authority_scope=? AND pending_request_json=?
                  AND assigned_relay_id IS NULL
                """,
                (
                    str(grant["lease_id"]),
                    int(grant["fence"]),
                    str(grant["next_request_token"]),
                    int(grant["next_request_sequence"]),
                    str(grant["expires_at"]),
                    now_text,
                    authority_scope,
                    request_json,
                ),
            )
            conn.commit()
            if cursor.rowcount != 1:
                time.sleep(0.01)
        finally:
            conn.close()
    return RuntimePreparation(
        retryable=True,
        retry_after_seconds=1,
        error_code="runtime_authority_busy",
        error_message="runtime authority changed concurrently; retry the relay row",
    )


def runtime_receipt_result(
    metadata: Mapping[str, Any], receipt: Mapping[str, Any]
) -> tuple[Dict[str, Any] | None, str, str]:
    """Validate and extract the secret next authority from an accepted receipt."""
    return _runtime_core.runtime_receipt_result(
        metadata,
        receipt,
        normalize_public_jwk=normalize_public_jwk,
    )


def apply_runtime_receipt_in_transaction(
    conn: sqlite3.Connection,
    *,
    relay_id: str,
    metadata: Mapping[str, Any],
    credentials: Any,
    runtime_lease: Mapping[str, Any],
    now: str,
) -> None:
    """Persist the next token inside the caller's relay ACK transaction."""
    return _runtime_core.apply_runtime_receipt_in_transaction(
        conn,
        relay_id=relay_id,
        metadata=metadata,
        credentials=credentials,
        runtime_lease=runtime_lease,
        now=now,
        normalize_public_jwk=normalize_public_jwk,
    )


def mark_runtime_operator_review_in_transaction(
    conn: sqlite3.Connection,
    *,
    relay_id: str,
    metadata: Mapping[str, Any],
    credentials: Any,
    error_code: str,
    now: str,
) -> None:
    """Quarantine local rotating authority with its terminal relay row."""
    return _runtime_core.mark_runtime_operator_review_in_transaction(
        conn,
        relay_id=relay_id,
        metadata=metadata,
        credentials=credentials,
        error_code=error_code,
        now=now,
        new_runtime_identity=new_runtime_identity,
    )


def reopen_reviewed_runtime_in_transaction(
    conn: sqlite3.Connection,
    *,
    relay_row: sqlite3.Row,
    metadata: Mapping[str, Any],
    receipt: Mapping[str, Any],
    expected_runtime_instance_id: str,
    expected_runtime_fence: int,
    expected_runtime_lease_id: str,
    expected_runtime_expires_at: str,
    now: str,
) -> Dict[str, Any]:
    """Reopen only a disposed, expired stale scope for normal authenticated issue.

    The caller validates the exact committed receipt and spool, ACKs its row,
    and durably audits the returned preimage before committing this transaction.
    This does not issue authority or change any source request metadata.
    """
    if not conn.in_transaction:
        raise RuntimeError("reviewed runtime recovery requires an open transaction")
    rows = conn.execute("SELECT * FROM direct_sync_runtime_authority").fetchall()
    if len(rows) != 1:
        raise ValueError("runtime_recovery_requires_one_authority")
    state = rows[0]
    scope = {key: str(relay_row[key] or "") for key in ("endpoint_url", "producer_id", "key_id")}
    scope["producer_install_id"] = str(metadata.get("producer_install_id") or "")
    if (not all(scope.values()) or not _state_scope_matches(state, scope)
            or state["authority_scope"] != _scope_key(scope)
            or receipt.get("producer_install_id") != scope["producer_install_id"]):
        raise ValueError("runtime_recovery_scope_mismatch")
    if (not expected_runtime_instance_id or not expected_runtime_lease_id
            or not expected_runtime_expires_at or type(expected_runtime_fence) is not int
            or expected_runtime_fence < 1
            or state["runtime_instance_id"] != expected_runtime_instance_id
            or state["fence"] != expected_runtime_fence
            or state["lease_id"] != expected_runtime_lease_id
            or state["expires_at"] != expected_runtime_expires_at
            or metadata.get("runtime_instance_id") != expected_runtime_instance_id
            or type(metadata.get("runtime_fence")) is not int
            or metadata.get("runtime_fence") != expected_runtime_fence):
        raise ValueError("runtime_recovery_preimage_mismatch")
    if (state["status"] != "OPERATOR_REVIEW"
            or state["last_error_code"] != "STALE_RUNTIME_FENCE"
            or relay_row["last_error_code"] != "STALE_RUNTIME_FENCE"):
        raise ValueError("runtime_recovery_review_cause_mismatch")
    runtime_receipt = receipt.get("runtime_lease")
    if (not isinstance(runtime_receipt, Mapping)
            or runtime_receipt.get("contract_version") != CONTRACT_VERSION
            or runtime_receipt.get("validation_status") != "observed_rejected"
            or runtime_receipt.get("reason_code") != "STALE_RUNTIME_FENCE"):
        raise ValueError("runtime_recovery_receipt_mismatch")
    expiry, current = _parse_time(state["expires_at"]), _parse_time(now)
    if expiry is None or current is None or expiry > current:
        raise ValueError("runtime_recovery_grant_not_expired")
    if any(state[key] is not None for key in (
        "assigned_relay_id", "pending_request_json", "pending_issue_idempotency_key",
        "next_request_token", "next_request_sequence",
    )):
        raise ValueError("runtime_recovery_authority_in_flight")
    try:
        public_jwk = json.loads(state["runtime_public_jwk_json"])
        metadata_jwk = metadata["runtime_public_jwk"]
        jwk_matches = canonical_json(public_jwk) == canonical_json(metadata_jwk)
    except (KeyError, TypeError, ValueError):
        jwk_matches = False
    token_digest = metadata.get("runtime_request_token_sha256")
    if (not jwk_matches or "runtime_request_token" in metadata
            or not isinstance(token_digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", token_digest) is None):
        raise ValueError("runtime_recovery_terminal_metadata_mismatch")
    for other in conn.execute(
        "SELECT metadata_json FROM direct_sync_relay_batches WHERE relay_id<>? AND status<>'acked'",
        (relay_row["relay_id"],),
    ):
        try:
            other_metadata = json.loads(other["metadata_json"] or "{}")
        except (TypeError, ValueError):
            raise ValueError("runtime_recovery_other_metadata_invalid") from None
        if not isinstance(other_metadata, dict):
            raise ValueError("runtime_recovery_other_metadata_invalid")
        if any(key in other_metadata for key in (*METADATA_FIELDS, "runtime_request_token_sha256")):
            raise ValueError("runtime_recovery_other_bound_row")
    before = {key: state[key] for key in (
        "authority_scope", "endpoint_url", "producer_id", "key_id", "producer_install_id",
        "runtime_instance_id", "lease_id", "fence", "expires_at", "status",
        "last_error_code", "created_at", "updated_at", "assigned_relay_id",
        "pending_request_json", "pending_issue_idempotency_key", "next_request_token",
        "next_request_sequence",
    )}
    before["public_jwk_sha256"] = hashlib.sha256(canonical_json(public_jwk).encode()).hexdigest()
    conn.execute(
        "UPDATE direct_sync_runtime_authority SET status='EXPIRED',updated_at=? WHERE authority_scope=?",
        (now, state["authority_scope"]),
    )
    return {"authority_before": before, "new_status": "EXPIRED", "normal_acquisition_required": True}


def release_runtime_request_in_transaction(
    conn: sqlite3.Connection,
    *,
    relay_id: str,
    metadata: Mapping[str, Any],
    credentials: Any,
    now: str,
) -> None:
    """Return a token after an explicit, non-committed server rejection."""
    return _runtime_core.release_runtime_request_in_transaction(
        conn,
        relay_id=relay_id,
        metadata=metadata,
        credentials=credentials,
        now=now,
        normalize_public_jwk=normalize_public_jwk,
        new_runtime_identity=new_runtime_identity,
    )


def disable_runtime_authority_in_transaction(
    conn: sqlite3.Connection,
    *,
    relay_id: str,
    metadata: Mapping[str, Any],
    credentials: Any,
    now: str,
) -> None:
    """Retire ambiguous authority after an observe-mode legacy receipt."""
    return _runtime_core.disable_runtime_authority_in_transaction(
        conn,
        relay_id=relay_id,
        metadata=metadata,
        credentials=credentials,
        now=now,
        new_runtime_identity=new_runtime_identity,
    )

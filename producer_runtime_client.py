# -*- coding: utf-8 -*-
"""Durable producer-runtime lease state for the direct-sync relay.

The rotating request token is reserved to one relay row before network I/O.
That row then owns the exact metadata until a committed receipt returns the
next token.  The authority scope is deliberately bound to the complete
credential/endpoint/install tuple so a credential change cannot inherit it.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import math
import os
import re
import sqlite3
import time
import unicodedata
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Mapping
from urllib.parse import urlparse, urlunparse

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
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value)
    if isinstance(value, list):
        return [_normalize_for_json(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _normalize_for_json(value[key]) for key in sorted(value)}
    return value


def canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        _normalize_for_json(dict(value)),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _canonical_request(
    *,
    timestamp: str,
    nonce: str,
    producer_id: str,
    key_id: str,
    body: Mapping[str, Any],
) -> str:
    body_bytes = canonical_json(body).encode("utf-8")
    metadata_hash = hashlib.sha256(body_bytes).hexdigest()
    return "\n".join(
        [
            SIGNATURE_VERSION,
            "POST",
            ENDPOINT_PATH,
            "",
            timestamp,
            nonce,
            producer_id,
            key_id,
            metadata_hash,
            metadata_hash,
            str(len(body_bytes)),
            CONTENT_TYPE,
        ]
    )


def _utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _jwk_thumbprint(public_jwk: Mapping[str, Any]) -> str:
    return _cng_jwk_thumbprint(public_jwk)


def new_runtime_identity() -> tuple[str, Dict[str, str]]:
    return f"runtime-{uuid.uuid4().hex}", generate_public_jwk()


def _scope_values(credentials: Any, producer_install_id: str) -> Dict[str, str]:
    values = {
        "endpoint_url": str(credentials.endpoint_url or "").strip(),
        "producer_id": str(credentials.producer_id or "").strip(),
        "key_id": str(credentials.key_id or "").strip(),
        "producer_install_id": str(producer_install_id or "").strip(),
    }
    if not all(values.values()):
        raise ValueError("runtime authority scope is incomplete")
    return values


def _scope_key(values: Mapping[str, str]) -> str:
    return hashlib.sha256(canonical_json(values).encode("utf-8")).hexdigest()


def _runtime_endpoint(endpoint_url: str) -> str:
    parsed = urlparse(str(endpoint_url or ""))
    if parsed.scheme.lower() != "https" or not parsed.netloc or parsed.username or parsed.password:
        raise ValueError("runtime lease endpoint must use credential-free HTTPS")
    return urlunparse((parsed.scheme, parsed.netloc, ENDPOINT_PATH, "", "", ""))


def init_runtime_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS direct_sync_runtime_authority (
            authority_scope TEXT PRIMARY KEY,
            endpoint_url TEXT NOT NULL,
            producer_id TEXT NOT NULL,
            key_id TEXT NOT NULL,
            producer_install_id TEXT NOT NULL,
            runtime_instance_id TEXT NOT NULL,
            runtime_public_jwk_json TEXT NOT NULL,
            lease_id TEXT,
            fence INTEGER,
            next_request_token TEXT,
            next_request_sequence INTEGER,
            expires_at TEXT,
            assigned_relay_id TEXT,
            pending_request_json TEXT,
            pending_issue_idempotency_key TEXT,
            status TEXT NOT NULL,
            last_error_code TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            CHECK(fence IS NULL OR fence >= 1),
            CHECK(next_request_sequence IS NULL OR next_request_sequence >= 1)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_direct_sync_runtime_assignment
        ON direct_sync_runtime_authority(assigned_relay_id)
        """
    )


def _connect(db_path: str | os.PathLike[str]) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=_BUSY_TIMEOUT_MS / 1000)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    init_runtime_schema(conn)
    return conn


def _metadata_shape_error(metadata: Mapping[str, Any]) -> str:
    present = [field_name for field_name in METADATA_FIELDS if field_name in metadata]
    if present and len(present) != len(METADATA_FIELDS):
        return "runtime lease metadata fields must be supplied together"
    if not present:
        return ""
    runtime_id = metadata.get("runtime_instance_id")
    if not isinstance(runtime_id, str) or not runtime_id.strip() or len(runtime_id.encode("utf-8")) > 256:
        return "runtime_instance_id is invalid"
    jwk = metadata.get("runtime_public_jwk")
    if not isinstance(jwk, Mapping) or set(jwk) != {"kty", "crv", "x", "y"}:
        return "runtime_public_jwk is invalid"
    if jwk.get("kty") != "EC" or jwk.get("crv") != "P-256":
        return "runtime_public_jwk is invalid"
    for coordinate in (jwk.get("x"), jwk.get("y")):
        if not isinstance(coordinate, str) or _COORDINATE_RE.fullmatch(coordinate) is None:
            return "runtime_public_jwk is invalid"
        try:
            raw = base64.urlsafe_b64decode(coordinate + "=" * (-len(coordinate) % 4))
        except Exception:
            return "runtime_public_jwk is invalid"
        if len(raw) != 32:
            return "runtime_public_jwk is invalid"
    try:
        normalize_public_jwk(jwk)
    except (TypeError, ValueError):
        return "runtime_public_jwk is invalid"
    fence = metadata.get("runtime_fence")
    if isinstance(fence, bool) or not isinstance(fence, int) or fence < 1:
        return "runtime_fence is invalid"
    token = metadata.get("runtime_request_token")
    if not isinstance(token, str) or _TOKEN_RE.fullmatch(token.strip()) is None:
        return "runtime_request_token is invalid"
    sequence = metadata.get("runtime_request_sequence")
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1:
        return "runtime_request_sequence is invalid"
    return ""


def redact_runtime_secrets(value: Any) -> Any:
    """Remove rotating authority from logs/status payloads while preserving shape."""

    secret_values: set[str] = set()

    def collect(item: Any) -> None:
        if isinstance(item, list):
            for nested in item:
                collect(nested)
        elif isinstance(item, Mapping):
            for key, nested in item.items():
                if str(key).lower() in {"runtime_request_token", "next_request_token"}:
                    if isinstance(nested, str) and nested:
                        secret_values.add(nested)
                else:
                    collect(nested)

    def redact(item: Any) -> Any:
        if isinstance(item, str):
            text = item
            for secret_value in secret_values:
                text = text.replace(secret_value, "[redacted]")
            return text
        if isinstance(item, list):
            return [redact(nested) for nested in item]
        if not isinstance(item, Mapping):
            return item
        redacted: Dict[str, Any] = {}
        for key, nested in item.items():
            key_text = str(key)
            if key_text.lower() in {"runtime_request_token", "next_request_token"}:
                redacted[key_text] = "[redacted]"
            else:
                redacted[key_text] = redact(nested)
        return redacted

    collect(value)
    return redact(value)


def _redact_known_values(value: Any, sensitive_values: tuple[Any, ...]) -> Any:
    variants: set[str] = set()
    for sensitive in sensitive_values:
        if not sensitive:
            continue
        variants.add(str(sensitive))
        if isinstance(sensitive, bytes):
            variants.add(sensitive.decode("utf-8", errors="ignore"))
            variants.add(sensitive.hex())

    def redact(item: Any) -> Any:
        if isinstance(item, str):
            text = item
            for variant in variants:
                if variant:
                    text = text.replace(variant, "[redacted]")
            return re.sub(
                r"(?i)authorization\s*:\s*[^\r\n\t ]+(?:[ \t]+[^\r\n\t ]+)?",
                "[redacted]",
                text,
            )
        if isinstance(item, list):
            return [redact(nested) for nested in item]
        if isinstance(item, Mapping):
            return {redact(str(key)): redact(nested) for key, nested in item.items()}
        return item

    return redact(value)


def scrub_terminal_runtime_metadata(metadata: Mapping[str, Any]) -> Dict[str, Any]:
    """Remove the consumed one-shot token while retaining an audit digest."""

    scrubbed = dict(metadata)
    token = scrubbed.pop("runtime_request_token", None)
    if isinstance(token, str) and token:
        scrubbed["runtime_request_token_sha256"] = hashlib.sha256(token.encode("utf-8")).hexdigest()
    return scrubbed


def _retry_after(value: Any) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        seconds = int(text)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(text)
        except (TypeError, ValueError, IndexError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        seconds = max(0, math.ceil((retry_at - datetime.now(timezone.utc)).total_seconds()))
    return min(max(0, seconds), _MAX_RETRY_AFTER_SECONDS)


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
    public_jwk = json.loads(str(state["runtime_public_jwk_json"]))
    expected_sequence = int(request_value.get("runtime_request_sequence") or 0) + 1
    expected_operation = "renewed" if "runtime_request_token" in request_value else "issued"
    checks = (
        grant.get("ok") is True,
        grant.get("status") == "ACTIVE",
        grant.get("contract_version") == CONTRACT_VERSION,
        grant.get("producer_install_id") == scope["producer_install_id"],
        grant.get("runtime_instance_id") == state["runtime_instance_id"],
        grant.get("public_jwk_thumbprint") == _jwk_thumbprint(public_jwk),
        grant.get("issue_idempotency_key") == request_value.get("issue_idempotency_key"),
        grant.get("operation") == expected_operation,
        isinstance(grant.get("lease_id"), str) and bool(str(grant.get("lease_id") or "").strip()),
        type(grant.get("fence")) is int and int(grant.get("fence") or 0) >= 1,
        isinstance(grant.get("next_request_token"), str)
        and _TOKEN_RE.fullmatch(str(grant.get("next_request_token") or "")) is not None,
        type(grant.get("next_request_sequence")) is int
        and int(grant.get("next_request_sequence") or 0) == expected_sequence,
        "runtime_fence" not in request_value
        or grant.get("fence") == request_value.get("runtime_fence"),
        _parse_time(grant.get("expires_at")) is not None,
    )
    return "" if all(checks) else "runtime lease response fields do not match the requested authority"


def _state_row(conn: sqlite3.Connection, authority_scope: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM direct_sync_runtime_authority WHERE authority_scope=?",
        (authority_scope,),
    ).fetchone()


def _state_scope_matches(state: sqlite3.Row, scope: Mapping[str, str]) -> bool:
    return all(str(state[field_name]) == str(scope[field_name]) for field_name in scope)


def _create_state(conn: sqlite3.Connection, scope: Mapping[str, str], now_text: str) -> sqlite3.Row:
    runtime_id, public_jwk = new_runtime_identity()
    authority_scope = _scope_key(scope)
    conn.execute(
        """
        INSERT INTO direct_sync_runtime_authority(
            authority_scope, endpoint_url, producer_id, key_id,
            producer_install_id, runtime_instance_id,
            runtime_public_jwk_json, status, created_at, updated_at
        ) VALUES(?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?)
        """,
        (
            authority_scope,
            scope["endpoint_url"],
            scope["producer_id"],
            scope["key_id"],
            scope["producer_install_id"],
            runtime_id,
            canonical_json(public_jwk),
            now_text,
            now_text,
        ),
    )
    row = _state_row(conn, authority_scope)
    assert row is not None
    return row


def _replace_expired_identity(
    conn: sqlite3.Connection, state: sqlite3.Row, now_text: str
) -> sqlite3.Row:
    runtime_id, public_jwk = new_runtime_identity()
    conn.execute(
        """
        UPDATE direct_sync_runtime_authority
        SET runtime_instance_id=?, runtime_public_jwk_json=?, lease_id=NULL,
            fence=NULL, next_request_token=NULL, next_request_sequence=NULL,
            expires_at=NULL, assigned_relay_id=NULL, pending_request_json=NULL,
            pending_issue_idempotency_key=NULL, status='PENDING',
            last_error_code=NULL, updated_at=?
        WHERE authority_scope=?
        """,
        (runtime_id, canonical_json(public_jwk), now_text, state["authority_scope"]),
    )
    row = _state_row(conn, str(state["authority_scope"]))
    assert row is not None
    return row


def _lease_request_value(state: sqlite3.Row, ttl_seconds: int) -> Dict[str, Any]:
    issue_key = f"runtime-lease-{uuid.uuid4().hex}"
    value: Dict[str, Any] = {
        "contract_version": CONTRACT_VERSION,
        "runtime_instance_id": str(state["runtime_instance_id"]),
        "public_jwk": json.loads(str(state["runtime_public_jwk_json"])),
        "issue_idempotency_key": issue_key,
        "ttl_seconds": int(ttl_seconds),
    }
    if state["next_request_token"] and state["next_request_sequence"] and state["fence"]:
        value.update(
            {
                "runtime_fence": int(state["fence"]),
                "runtime_request_token": str(state["next_request_token"]),
                "runtime_request_sequence": int(state["next_request_sequence"]),
            }
        )
    return value


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

    return {
        "contract_version": CONTRACT_VERSION,
        "status": "ACTIVE",
        "server_grant_accepted": bool(
            state["lease_id"]
            and state["fence"]
            and state["next_request_token"]
            and state["next_request_sequence"]
            and state["expires_at"]
        ),
        "producer_install_id": producer_install_id,
        "runtime_instance_id": str(state["runtime_instance_id"] or ""),
        "lease_id": str(state["lease_id"] or ""),
        "fence": int(state["fence"] or 0),
        "expires_at": str(state["expires_at"] or ""),
        "request_sent": bool(request_sent),
    }


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

    runtime_value = receipt.get("runtime_lease")
    if (
        isinstance(runtime_value, Mapping)
        and str(runtime_value.get("validation_status") or "") == "observed"
        and str(runtime_value.get("reason_code") or "") == "RUNTIME_LEASE_MISSING_OBSERVED"
    ):
        return (
            None,
            "runtime_lease_receipt_observed_legacy",
            "server accepted legacy metadata in runtime lease observe mode",
        )
    if not any(field_name in metadata for field_name in METADATA_FIELDS):
        return None, "", ""
    shape_error = _metadata_shape_error(metadata)
    if shape_error:
        return None, "runtime_lease_metadata_invalid", shape_error
    if not isinstance(runtime_value, Mapping):
        return None, "runtime_lease_receipt_missing", "accepted receipt is missing runtime lease rotation"
    if str(receipt.get("producer_install_id") or "") != str(
        metadata.get("producer_install_id") or ""
    ):
        return None, "runtime_lease_receipt_invalid", "accepted receipt producer install does not match runtime authority"
    validation_status = str(runtime_value.get("validation_status") or "")
    if validation_status == "observed_rejected":
        reason = str(runtime_value.get("reason_code") or "STALE_RUNTIME_REQUEST_TOKEN")
        return None, reason, "server observed a stale runtime request; operator review is required"
    expected_sequence = int(metadata["runtime_request_sequence"]) + 1
    valid = (
        runtime_value.get("contract_version") == CONTRACT_VERSION
        and validation_status == "consumed"
        and type(runtime_value.get("fence")) is int
        and int(runtime_value.get("fence")) == int(metadata["runtime_fence"])
        and isinstance(runtime_value.get("lease_id"), str)
        and bool(str(runtime_value.get("lease_id") or "").strip())
        and isinstance(runtime_value.get("next_request_token"), str)
        and _TOKEN_RE.fullmatch(str(runtime_value.get("next_request_token") or "")) is not None
        and type(runtime_value.get("next_request_sequence")) is int
        and int(runtime_value.get("next_request_sequence")) == expected_sequence
        and _parse_time(runtime_value.get("expires_at")) is not None
    )
    if not valid:
        return None, "runtime_lease_receipt_invalid", "accepted receipt runtime lease rotation is invalid"
    return dict(runtime_value), "", ""


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

    if not conn.in_transaction:
        raise RuntimeError("runtime receipt update requires an open transaction")
    error = _metadata_shape_error(metadata)
    if error:
        raise ValueError(error)
    validated_rotation, receipt_error_code, receipt_error_message = runtime_receipt_result(
        metadata,
        {
            "producer_install_id": metadata.get("producer_install_id"),
            "runtime_lease": dict(runtime_lease),
        },
    )
    if receipt_error_code or validated_rotation is None:
        raise ValueError(receipt_error_message or receipt_error_code)
    runtime_lease = validated_rotation
    scope = _scope_values(credentials, str(metadata.get("producer_install_id") or ""))
    authority_scope = _scope_key(scope)
    runtime_id = str(metadata["runtime_instance_id"])
    public_jwk_json = canonical_json(metadata["runtime_public_jwk"])
    state = _state_row(conn, authority_scope)
    if state is None:
        conn.execute(
            """
            INSERT INTO direct_sync_runtime_authority(
                authority_scope, endpoint_url, producer_id, key_id,
                producer_install_id, runtime_instance_id,
                runtime_public_jwk_json, lease_id, fence,
                next_request_token, next_request_sequence, expires_at,
                assigned_relay_id, status, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'ACTIVE', ?, ?)
            """,
            (
                authority_scope,
                scope["endpoint_url"],
                scope["producer_id"],
                scope["key_id"],
                scope["producer_install_id"],
                runtime_id,
                public_jwk_json,
                str(runtime_lease["lease_id"]),
                int(runtime_lease["fence"]),
                str(runtime_lease["next_request_token"]),
                int(runtime_lease["next_request_sequence"]),
                str(runtime_lease["expires_at"]),
                now,
                now,
            ),
        )
        return
    if not _state_scope_matches(state, scope):
        raise ValueError("runtime authority credential scope does not match its database key")
    if (
        str(state["runtime_instance_id"]) != runtime_id
        or canonical_json(json.loads(str(state["runtime_public_jwk_json"]))) != public_jwk_json
        or (state["assigned_relay_id"] and str(state["assigned_relay_id"]) != relay_id)
    ):
        raise ValueError("runtime receipt does not match the locally reserved authority")
    conn.execute(
        """
        UPDATE direct_sync_runtime_authority
        SET lease_id=?, fence=?, next_request_token=?, next_request_sequence=?,
            expires_at=?, assigned_relay_id=NULL, pending_request_json=NULL,
            pending_issue_idempotency_key=NULL, status='ACTIVE',
            last_error_code=NULL, updated_at=?
        WHERE authority_scope=?
        """,
        (
            str(runtime_lease["lease_id"]),
            int(runtime_lease["fence"]),
            str(runtime_lease["next_request_token"]),
            int(runtime_lease["next_request_sequence"]),
            str(runtime_lease["expires_at"]),
            now,
            authority_scope,
        ),
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

    if not conn.in_transaction:
        raise RuntimeError("runtime operator review update requires an open transaction")
    state = conn.execute(
        "SELECT * FROM direct_sync_runtime_authority WHERE assigned_relay_id=?",
        (relay_id,),
    ).fetchone()
    if state is None:
        try:
            scope = _scope_values(credentials, str(metadata.get("producer_install_id") or ""))
        except (AttributeError, TypeError, ValueError):
            return
        state = _state_row(conn, _scope_key(scope))
        if state is None:
            state = _create_state(conn, scope, now)
    if state is None:
        return
    conn.execute(
        """
        UPDATE direct_sync_runtime_authority
        SET next_request_token=NULL, next_request_sequence=NULL,
            assigned_relay_id=NULL, pending_request_json=NULL,
            pending_issue_idempotency_key=NULL, status='OPERATOR_REVIEW',
            last_error_code=?, updated_at=?
        WHERE authority_scope=?
        """,
        (str(error_code or "runtime_operator_review"), now, state["authority_scope"]),
    )


def release_runtime_request_in_transaction(
    conn: sqlite3.Connection,
    *,
    relay_id: str,
    metadata: Mapping[str, Any],
    credentials: Any,
    now: str,
) -> None:
    """Return a token after an explicit, non-committed server rejection."""

    if not conn.in_transaction:
        raise RuntimeError("runtime request release requires an open transaction")
    if _metadata_shape_error(metadata) or not all(field in metadata for field in METADATA_FIELDS):
        mark_runtime_operator_review_in_transaction(
            conn,
            relay_id=relay_id,
            metadata=metadata,
            credentials=credentials,
            error_code="runtime_authority_release_metadata_invalid",
            now=now,
        )
        return
    state = conn.execute(
        "SELECT * FROM direct_sync_runtime_authority WHERE assigned_relay_id=?",
        (relay_id,),
    ).fetchone()
    if state is None:
        return
    matches = (
        str(state["runtime_instance_id"]) == str(metadata["runtime_instance_id"])
        and canonical_json(json.loads(str(state["runtime_public_jwk_json"])))
        == canonical_json(metadata["runtime_public_jwk"])
        and int(state["fence"] or 0) == int(metadata["runtime_fence"])
    )
    if not matches:
        mark_runtime_operator_review_in_transaction(
            conn,
            relay_id=relay_id,
            metadata=metadata,
            credentials=credentials,
            error_code="runtime_authority_release_mismatch",
            now=now,
        )
        return
    conn.execute(
        """
        UPDATE direct_sync_runtime_authority
        SET next_request_token=?, next_request_sequence=?, assigned_relay_id=NULL,
            pending_request_json=NULL, pending_issue_idempotency_key=NULL,
            status='ACTIVE', last_error_code=NULL, updated_at=?
        WHERE authority_scope=? AND assigned_relay_id=?
        """,
        (
            str(metadata["runtime_request_token"]),
            int(metadata["runtime_request_sequence"]),
            now,
            state["authority_scope"],
            relay_id,
        ),
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

    if not conn.in_transaction:
        raise RuntimeError("runtime authority disable requires an open transaction")
    state = conn.execute(
        "SELECT * FROM direct_sync_runtime_authority WHERE assigned_relay_id=?",
        (relay_id,),
    ).fetchone()
    if state is None:
        try:
            scope = _scope_values(credentials, str(metadata.get("producer_install_id") or ""))
        except (AttributeError, TypeError, ValueError):
            return
        state = _state_row(conn, _scope_key(scope))
        if state is None:
            state = _create_state(conn, scope, now)
    if state is None:
        return
    conn.execute(
        """
        UPDATE direct_sync_runtime_authority
        SET lease_id=NULL, fence=NULL, next_request_token=NULL,
            next_request_sequence=NULL, expires_at=NULL, assigned_relay_id=NULL,
            pending_request_json=NULL, pending_issue_idempotency_key=NULL,
            status=?, last_error_code='legacy_accepted', updated_at=?
        WHERE authority_scope=?
        """,
        (LEGACY_DISABLED_STATUS, now, state["authority_scope"]),
    )

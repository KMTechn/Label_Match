"""Producer runtime receipt/lease computation and caller-owned SQL operations.

Transport, connection ownership, writer admission and recovery decisions remain
in each application. Required callbacks use the application's existing CNG JWK
validator/thumbprint and identity factory; this module neither replaces them
nor commits a transaction. Function names retain the app facade's existing seams.
"""
from __future__ import annotations

import base64
import hashlib
import json
import math
import re
import sqlite3
import unicodedata
import uuid
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Dict, Mapping


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


LEGACY_DISABLED_STATUS = "LEGACY_DISABLED"


_TOKEN_RE = re.compile(r"[A-Za-z0-9_-]{32,256}")


_COORDINATE_RE = re.compile(r"[A-Za-z0-9_-]{43}")


_MAX_RETRY_AFTER_SECONDS = 24 * 60 * 60


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


def _parse_time(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _jwk_thumbprint(public_jwk: Mapping[str, Any],
    *,
    _cng_jwk_thumbprint: Callable[[Mapping[str, Any]], str],
) -> str:
    return _cng_jwk_thumbprint(public_jwk)


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


def _metadata_shape_error(metadata: Mapping[str, Any],
    *,
    normalize_public_jwk: Callable[[Mapping[str, Any]], Mapping[str, Any]],
) -> str:
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


def _grant_error(
    grant: Mapping[str, Any],
    *,
    state: sqlite3.Row,
    scope: Mapping[str, str],
    request_value: Mapping[str, Any],
    _cng_jwk_thumbprint: Callable[[Mapping[str, Any]], str],
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
        grant.get("public_jwk_thumbprint") == _jwk_thumbprint(public_jwk, _cng_jwk_thumbprint=_cng_jwk_thumbprint),
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


def _create_state(conn: sqlite3.Connection, scope: Mapping[str, str], now_text: str,
    *,
    new_runtime_identity: Callable[[], tuple[str, Dict[str, str]]],
) -> sqlite3.Row:
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
    conn: sqlite3.Connection, state: sqlite3.Row, now_text: str,
    *,
    new_runtime_identity: Callable[[], tuple[str, Dict[str, str]]],
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


def runtime_receipt_result(
    metadata: Mapping[str, Any], receipt: Mapping[str, Any],
    *,
    normalize_public_jwk: Callable[[Mapping[str, Any]], Mapping[str, Any]],
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
    shape_error = _metadata_shape_error(metadata, normalize_public_jwk=normalize_public_jwk)
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
    normalize_public_jwk: Callable[[Mapping[str, Any]], Mapping[str, Any]],
) -> None:
    """Persist the next token inside the caller's relay ACK transaction."""

    if not conn.in_transaction:
        raise RuntimeError("runtime receipt update requires an open transaction")
    error = _metadata_shape_error(metadata, normalize_public_jwk=normalize_public_jwk)
    if error:
        raise ValueError(error)
    validated_rotation, receipt_error_code, receipt_error_message = runtime_receipt_result(
        metadata,
        {
            "producer_install_id": metadata.get("producer_install_id"),
            "runtime_lease": dict(runtime_lease),
        },
        normalize_public_jwk=normalize_public_jwk,
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
    new_runtime_identity: Callable[[], tuple[str, Dict[str, str]]],
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
            state = _create_state(conn, scope, now, new_runtime_identity=new_runtime_identity)
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
    normalize_public_jwk: Callable[[Mapping[str, Any]], Mapping[str, Any]],
    new_runtime_identity: Callable[[], tuple[str, Dict[str, str]]],
) -> None:
    """Return a token after an explicit, non-committed server rejection."""

    if not conn.in_transaction:
        raise RuntimeError("runtime request release requires an open transaction")
    if _metadata_shape_error(metadata, normalize_public_jwk=normalize_public_jwk) or not all(field in metadata for field in METADATA_FIELDS):
        mark_runtime_operator_review_in_transaction(
            conn,
            relay_id=relay_id,
            metadata=metadata,
            credentials=credentials,
            error_code="runtime_authority_release_metadata_invalid",
            now=now,
            new_runtime_identity=new_runtime_identity,
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
            new_runtime_identity=new_runtime_identity,
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
    new_runtime_identity: Callable[[], tuple[str, Dict[str, str]]],
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
            state = _create_state(conn, scope, now, new_runtime_identity=new_runtime_identity)
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

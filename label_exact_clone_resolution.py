"""Fail-closed evidence for resolving a Label runtime exact-clone conflict.

The runtime lease protocol deliberately quarantines a second runtime identity
that presents the same ``producer_install_id`` while another identity still
owns the install lease.  This module never performs that reconciliation.  It
captures the two-sided preimage and, after an operator has reconciled the
authorities, emits/validates the secret-free receipt consumed by current-user
onboarding.
"""

from __future__ import annotations

import base64
import copy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import uuid
from typing import Any, Mapping, Sequence

from user_relay_stop_marker import (
    StopMarkerLineageError,
    validate_marker_successor_lineage,
)


CONFLICT_CODE = "EXACT_CLONE_RUNTIME_CONFLICT"
PREIMAGE_SCHEMA = "label-match-exact-clone-conflict-preimage-v1"
RECEIPT_SCHEMA = "label-match-exact-clone-resolution-v1"
PORTABLE_REBIND_SCHEMA = "label-match-portable-successor-rebind-v1"
PORTABLE_MANIFEST_SCHEMA = "label-match-portable-tree-v1"
MAX_JSON_BYTES = 1024 * 1024
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_GIT_OBJECT_RE = re.compile(r"[0-9a-f]{40}")
_FORBIDDEN_RECEIPT_KEYS = frozenset(
    {
        "next_request_token",
        "runtime_request_token",
        "runtime_public_jwk_json",
        "public_jwk",
        "secret",
        "secret_value",
        "credential_payload",
        "private_key",
        "hmac_key",
    }
)
PORTABLE_REBIND_ALLOWED_PATHS = frozenset(
    {
        "current_user_onboarding.py",
        "label_exact_clone_resolution.py",
        "tests/test_current_user_onboarding.py",
        "tests/test_label_exact_clone_resolution.py",
        "tests/test_label_exact_clone_resolution_receipt_cli.py",
        "tools/label_exact_clone_resolution_receipt.py",
    }
)


class ExactCloneResolutionError(ValueError):
    """The conflict topology or proposed receipt is not exact."""


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def file_sha256(path: str | os.PathLike[str]) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_bounded_json(
    path: str | os.PathLike[str],
    *,
    label: str,
    maximum_bytes: int = MAX_JSON_BYTES,
) -> dict[str, Any]:
    selected = Path(path).resolve(strict=False)
    if not selected.is_file():
        raise ExactCloneResolutionError(f"{label} is absent: {selected}")
    size = selected.stat().st_size
    if size <= 0 or size > maximum_bytes:
        raise ExactCloneResolutionError(f"{label} size is invalid: {size}")
    try:
        value = json.loads(selected.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ExactCloneResolutionError(f"{label} is not valid UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise ExactCloneResolutionError(f"{label} must be a JSON object")
    return value


def read_pinned_json(
    path: str | os.PathLike[str],
    expected_sha256: str,
    *,
    label: str,
    maximum_bytes: int = MAX_JSON_BYTES,
) -> dict[str, Any]:
    selected = Path(path).resolve(strict=False)
    expected = _required_sha256(expected_sha256, f"{label} expected SHA-256")
    try:
        raw = selected.read_bytes()
    except OSError as exc:
        raise ExactCloneResolutionError(f"{label} is absent: {selected}") from exc
    if not raw or len(raw) > maximum_bytes:
        raise ExactCloneResolutionError(f"{label} size is invalid: {len(raw)}")
    if hashlib.sha256(raw).hexdigest() != expected:
        raise ExactCloneResolutionError(f"{label} SHA-256 differs")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ExactCloneResolutionError(f"{label} is not valid UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise ExactCloneResolutionError(f"{label} must be a JSON object")
    return value


def _json_file_bytes(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(dict(value), ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def json_document_sha256(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_json_file_bytes(value)).hexdigest()


def write_new_json(path: str | os.PathLike[str], value: Mapping[str, Any]) -> Path:
    selected = Path(path).resolve(strict=False)
    selected.parent.mkdir(parents=True, exist_ok=True)
    temporary = selected.parent / f".{selected.name}.{uuid.uuid4().hex}.tmp"
    raw = _json_file_bytes(value)
    descriptor: int | None = None
    try:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_BINARY"):
            flags |= os.O_BINARY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(temporary, flags, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = None
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, selected)
    except FileExistsError as exc:
        raise ExactCloneResolutionError(
            f"refusing to overwrite evidence: {selected}"
        ) from exc
    except OSError as exc:
        raise ExactCloneResolutionError(
            f"exclusive evidence publication failed: {selected}"
        ) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
    return selected


def _require_exact_keys(value: Mapping[str, Any], expected: set[str], label: str) -> None:
    observed = set(value)
    if observed != expected:
        missing = sorted(expected - observed)
        extra = sorted(observed - expected)
        raise ExactCloneResolutionError(
            f"{label} fields differ; missing={missing!r}; extra={extra!r}"
        )


def _required_text(value: Any, label: str) -> str:
    text = str(value or "").strip()
    if not text or len(text) > 1024 or "\x00" in text:
        raise ExactCloneResolutionError(f"{label} is empty or invalid")
    return text


def _required_sha256(value: Any, label: str) -> str:
    text = str(value or "").strip().lower()
    if _SHA256_RE.fullmatch(text) is None:
        raise ExactCloneResolutionError(f"{label} is not a lowercase SHA-256")
    return text


def _required_git_object(value: Any, label: str) -> str:
    text = str(value or "").strip().lower()
    if _GIT_OBJECT_RE.fullmatch(text) is None:
        raise ExactCloneResolutionError(f"{label} is not a lowercase Git object id")
    return text


def _positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ExactCloneResolutionError(f"{label} must be a positive integer")
    return value


def _resolved_text(path: str | os.PathLike[str]) -> str:
    return str(Path(path).resolve(strict=False))


def _same_path(left: Any, right: str | os.PathLike[str]) -> bool:
    try:
        return os.path.normcase(_resolved_text(str(left))) == os.path.normcase(
            _resolved_text(right)
        )
    except (OSError, TypeError, ValueError):
        return False


def _connect_read_only(path: str | os.PathLike[str]) -> sqlite3.Connection:
    selected = Path(path).resolve(strict=False)
    if not selected.is_file():
        raise ExactCloneResolutionError(f"SQLite database is absent: {selected}")
    connection = sqlite3.connect(
        f"file:{selected.as_posix()}?mode=ro",
        uri=True,
        timeout=30,
    )
    connection.row_factory = sqlite3.Row
    return connection


def _jwk_thumbprint(raw_json: Any) -> str:
    try:
        value = json.loads(str(raw_json or ""))
    except json.JSONDecodeError as exc:
        raise ExactCloneResolutionError("runtime public JWK is invalid") from exc
    if not isinstance(value, dict):
        raise ExactCloneResolutionError("runtime public JWK must be an object")
    required = {"crv", "kty", "x", "y"}
    if not required.issubset(value):
        raise ExactCloneResolutionError("runtime public JWK is incomplete")
    public = {name: _required_text(value[name], f"runtime JWK {name}") for name in required}
    digest = hashlib.sha256(_canonical_json(public).encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


_CLIENT_AUTHORITY_FIELDS = (
    "authority_scope",
    "endpoint_url",
    "producer_id",
    "key_id",
    "producer_install_id",
    "runtime_instance_id",
    "lease_id",
    "fence",
    "next_request_sequence",
    "expires_at",
    "assigned_relay_id",
    "pending_issue_idempotency_key",
    "status",
    "last_error_code",
    "created_at",
    "updated_at",
)


def client_authorities(path: str | os.PathLike[str]) -> list[dict[str, Any]]:
    connection = _connect_read_only(path)
    try:
        rows = connection.execute(
            "SELECT * FROM direct_sync_runtime_authority ORDER BY authority_scope"
        ).fetchall()
    except sqlite3.Error as exc:
        raise ExactCloneResolutionError("client runtime authority schema is unavailable") from exc
    finally:
        connection.close()
    result: list[dict[str, Any]] = []
    for row in rows:
        item = {name: row[name] for name in _CLIENT_AUTHORITY_FIELDS}
        item["runtime_public_jwk_thumbprint"] = _jwk_thumbprint(
            row["runtime_public_jwk_json"]
        )
        item["runtime_public_jwk_sha256"] = hashlib.sha256(
            str(row["runtime_public_jwk_json"] or "").encode("utf-8")
        ).hexdigest()
        item["next_request_token_present"] = bool(row["next_request_token"])
        item["next_request_token_sha256"] = (
            hashlib.sha256(str(row["next_request_token"]).encode("utf-8")).hexdigest()
            if row["next_request_token"]
            else ""
        )
        item["pending_request_present"] = bool(row["pending_request_json"])
        item["pending_request_sha256"] = (
            hashlib.sha256(str(row["pending_request_json"]).encode("utf-8")).hexdigest()
            if row["pending_request_json"]
            else ""
        )
        result.append(item)
    return result


def relay_batches_digest(path: str | os.PathLike[str]) -> dict[str, Any]:
    connection = _connect_read_only(path)
    digest = hashlib.sha256(b"label-match-relay-batches-v1\n")
    count = 0
    try:
        columns = [
            str(row[1])
            for row in connection.execute(
                "PRAGMA table_info(direct_sync_relay_batches)"
            ).fetchall()
        ]
        if not columns or "relay_id" not in columns:
            raise ExactCloneResolutionError("relay batch schema is unavailable")
        quoted = ",".join(f'"{name.replace(chr(34), chr(34) * 2)}"' for name in columns)
        rows = connection.execute(
            f"SELECT {quoted} FROM direct_sync_relay_batches ORDER BY relay_id"
        )
        for row in rows:
            normalized: list[Any] = []
            for value in row:
                if isinstance(value, bytes):
                    normalized.append(
                        {
                            "bytes_length": len(value),
                            "bytes_sha256": hashlib.sha256(value).hexdigest(),
                        }
                    )
                else:
                    normalized.append(value)
            digest.update(_canonical_json(normalized).encode("utf-8"))
            digest.update(b"\n")
            count += 1
    except sqlite3.Error as exc:
        raise ExactCloneResolutionError("relay batch digest failed") from exc
    finally:
        connection.close()
    return {
        "algorithm": "sha256-canonical-all-columns-relay-id-order-v1",
        "row_count": count,
        "sha256": digest.hexdigest(),
    }


def sqlite_logical_digest_on_connection(
    connection: sqlite3.Connection,
) -> dict[str, Any]:
    """Hash a caller-owned complete logical SQLite image without secret output."""

    digest = hashlib.sha256(b"label-match-sqlite-logical-itertdump-v1\n")
    statement_count = 0
    for statement in connection.iterdump():
        digest.update(statement.encode("utf-8"))
        digest.update(b"\n")
        statement_count += 1
    return {
        "algorithm": "sha256-sqlite-itertdump-v1",
        "statement_count": statement_count,
        "sha256": digest.hexdigest(),
    }


def sqlite_logical_digest(path: str | os.PathLike[str]) -> dict[str, Any]:
    """Hash the complete logical SQLite image without exposing stored secrets."""

    connection = _connect_read_only(path)
    try:
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        if integrity != "ok":
            raise ExactCloneResolutionError("SQLite logical digest integrity failed")
        return sqlite_logical_digest_on_connection(connection)
    except sqlite3.Error as exc:
        raise ExactCloneResolutionError("SQLite logical digest failed") from exc
    finally:
        connection.close()


def _server_rows(
    path: str | os.PathLike[str], producer_install_id: str
) -> dict[str, list[dict[str, Any]]]:
    connection = _connect_read_only(path)
    try:
        leases = [
            dict(row)
            for row in connection.execute(
                """
                SELECT lease_id, producer_install_id, runtime_instance_id,
                       public_jwk_thumbprint, issue_idempotency_key,
                       request_fingerprint, fence, current_request_sequence,
                       status, issued_at, expires_at, last_rotated_at,
                       response_hash
                  FROM producer_runtime_leases
                 WHERE producer_install_id=?
                 ORDER BY fence
                """,
                (producer_install_id,),
            )
        ]
        quarantines = [
            dict(row)
            for row in connection.execute(
                """
                SELECT audit_id, producer_install_id, runtime_instance_id,
                       public_jwk_thumbprint, issue_idempotency_key,
                       request_fingerprint, active_lease_id,
                       active_runtime_instance_id,
                       active_public_jwk_thumbprint, active_fence,
                       reason_code, occurred_at
                  FROM producer_runtime_quarantine_audit
                 WHERE producer_install_id=?
                 ORDER BY occurred_at
                """,
                (producer_install_id,),
            )
        ]
        anchors = [
            dict(row)
            for row in connection.execute(
                """
                SELECT producer_install_id, issue_idempotency_key,
                       request_fingerprint, outcome, response_hash,
                       lease_id, committed_at
                  FROM producer_runtime_issue_anchors
                 WHERE producer_install_id=?
                 ORDER BY committed_at
                """,
                (producer_install_id,),
            )
        ]
    except sqlite3.Error as exc:
        raise ExactCloneResolutionError("server runtime lease schema is unavailable") from exc
    finally:
        connection.close()
    return {"leases": leases, "quarantines": quarantines, "anchors": anchors}


def _portable_binding(portable_root: str | os.PathLike[str]) -> dict[str, Any]:
    root = Path(portable_root).resolve(strict=False)
    manifest_path = root / "portable-manifest.json"
    installer_path = root / "INSTALL_CANONICAL_PORTABLE.ps1"
    manifest = read_bounded_json(
        manifest_path, label="Label portable manifest", maximum_bytes=64 * 1024
    )
    if manifest.get("schema") != PORTABLE_MANIFEST_SCHEMA:
        raise ExactCloneResolutionError("Label portable manifest schema differs")
    source_commit = _required_git_object(
        manifest.get("source_commit"), "portable source_commit"
    )
    source_tree = _required_git_object(manifest.get("source_tree"), "portable source_tree")
    installer_hash = file_sha256(installer_path)
    if installer_hash != _required_sha256(
        manifest.get("canonical_installer_sha256"),
        "portable canonical_installer_sha256",
    ):
        raise ExactCloneResolutionError("Label portable installer hash differs")
    return {
        "root": str(root),
        "source_commit": source_commit,
        "source_tree": source_tree,
        "portable_manifest_sha256": file_sha256(manifest_path),
        "canonical_installer_sha256": installer_hash,
    }


def _identity_binding(
    identity_path: str | os.PathLike[str], credential_path: str | os.PathLike[str]
) -> dict[str, str]:
    identity = read_bounded_json(identity_path, label="Label producer identity")
    credential = read_bounded_json(credential_path, label="Label credential reference")
    return {
        "producer_install_id": _required_text(
            identity.get("producer_install_id"), "producer_install_id"
        ),
        "producer_id": _required_text(credential.get("producer_id"), "producer_id"),
        "key_id": _required_text(credential.get("key_id"), "key_id"),
        "endpoint_url": _required_text(
            credential.get("endpoint_url"), "endpoint_url"
        ),
    }


def _selected_client_authority(
    rows: Sequence[Mapping[str, Any]], identity: Mapping[str, str]
) -> dict[str, Any]:
    matches = [
        dict(row)
        for row in rows
        if str(row.get("producer_install_id") or "")
        == identity["producer_install_id"]
        and str(row.get("producer_id") or "") == identity["producer_id"]
        and str(row.get("key_id") or "") == identity["key_id"]
        and str(row.get("endpoint_url") or "") == identity["endpoint_url"]
    ]
    if len(matches) != 1:
        raise ExactCloneResolutionError(
            "current credential does not select exactly one client runtime authority"
        )
    return matches[0]


def capture_conflict_preimage(
    *,
    client_db_path: str | os.PathLike[str],
    server_db_path: str | os.PathLike[str],
    identity_path: str | os.PathLike[str],
    credential_path: str | os.PathLike[str],
    stop_marker_path: str | os.PathLike[str],
    portable_root: str | os.PathLike[str],
) -> dict[str, Any]:
    marker_path = Path(stop_marker_path).resolve(strict=False)
    marker = read_bounded_json(marker_path, label="Label relay stop marker")
    marker_request_id = _required_text(marker.get("request_id"), "stop marker request_id")
    identity = _identity_binding(identity_path, credential_path)
    client_rows = client_authorities(client_db_path)
    candidate = _selected_client_authority(client_rows, identity)
    if (
        candidate.get("status") != "OPERATOR_REVIEW"
        or candidate.get("last_error_code") != CONFLICT_CODE
        or not candidate.get("pending_request_present")
        or not candidate.get("pending_issue_idempotency_key")
    ):
        raise ExactCloneResolutionError(
            "current credential authority is not the exact quarantined conflict candidate"
        )
    server = _server_rows(server_db_path, identity["producer_install_id"])
    matching_quarantine = [
        row
        for row in server["quarantines"]
        if row["reason_code"] == CONFLICT_CODE
        and row["runtime_instance_id"] == candidate["runtime_instance_id"]
        and row["public_jwk_thumbprint"]
        == candidate["runtime_public_jwk_thumbprint"]
        and row["issue_idempotency_key"]
        == candidate["pending_issue_idempotency_key"]
    ]
    if len(matching_quarantine) != 1:
        raise ExactCloneResolutionError(
            "server quarantine does not exactly bind the local conflict candidate"
        )
    quarantine = matching_quarantine[0]
    active = [
        row
        for row in server["leases"]
        if row["lease_id"] == quarantine["active_lease_id"]
        and row["runtime_instance_id"] == quarantine["active_runtime_instance_id"]
        and row["fence"] == quarantine["active_fence"]
        and row["status"] == "ACTIVE"
    ]
    if len(active) != 1:
        raise ExactCloneResolutionError(
            "server quarantine active authority is absent or already differs"
        )
    rejected_matches = [
        row
        for row in client_rows
        if row["runtime_instance_id"] == quarantine["active_runtime_instance_id"]
        and row["lease_id"] == quarantine["active_lease_id"]
        and row["fence"] == quarantine["active_fence"]
    ]
    if len(rejected_matches) != 1:
        raise ExactCloneResolutionError(
            "the prior server authority does not map to one local authority row"
        )
    return {
        "schema_version": PREIMAGE_SCHEMA,
        "status": "CONFLICT_CONFIRMED",
        "conflict_code": CONFLICT_CODE,
        "captured_at": _utc_now(),
        "producer_install_id": identity["producer_install_id"],
        "current_credential": {
            "producer_id": identity["producer_id"],
            "key_id": identity["key_id"],
            "endpoint_url": identity["endpoint_url"],
        },
        "identity_input": {
            "path": _resolved_text(identity_path),
            "sha256": file_sha256(identity_path),
        },
        "credential_input": {
            "path": _resolved_text(credential_path),
            "sha256": file_sha256(credential_path),
        },
        "client": {
            "database_path": _resolved_text(client_db_path),
            "database_sha256": file_sha256(client_db_path),
            "database_logical_digest": sqlite_logical_digest(client_db_path),
            "relay_batches": relay_batches_digest(client_db_path),
            "candidate_authority": candidate,
            "prior_authority": rejected_matches[0],
            "authority_count": len(client_rows),
        },
        "server": {
            "database_path": _resolved_text(server_db_path),
            "database_sha256": file_sha256(server_db_path),
            "quarantine": quarantine,
            "prior_active_lease": active[0],
        },
        "stop_marker": {
            "path": str(marker_path),
            "sha256": file_sha256(marker_path),
            "request_id": marker_request_id,
        },
        "portable": _portable_binding(portable_root),
        "secret_material_included": False,
    }


def _safe_client_authority(value: Mapping[str, Any], label: str) -> dict[str, Any]:
    required = set(_CLIENT_AUTHORITY_FIELDS) | {
        "runtime_public_jwk_thumbprint",
        "runtime_public_jwk_sha256",
        "next_request_token_present",
        "next_request_token_sha256",
        "pending_request_present",
        "pending_request_sha256",
    }
    _require_exact_keys(value, required, label)
    return dict(value)


def create_resolution_receipt(
    *,
    preimage: Mapping[str, Any],
    client_db_path: str | os.PathLike[str],
    server_db_path: str | os.PathLike[str],
    identity_path: str | os.PathLike[str],
    credential_path: str | os.PathLike[str],
    stop_marker_path: str | os.PathLike[str],
    portable_root: str | os.PathLike[str],
) -> dict[str, Any]:
    if (
        preimage.get("schema_version") != PREIMAGE_SCHEMA
        or preimage.get("status") != "CONFLICT_CONFIRMED"
        or preimage.get("conflict_code") != CONFLICT_CODE
        or preimage.get("secret_material_included") is not False
    ):
        raise ExactCloneResolutionError("conflict preimage contract differs")
    identity = _identity_binding(identity_path, credential_path)
    if identity["producer_install_id"] != preimage.get("producer_install_id"):
        raise ExactCloneResolutionError("producer install identity changed after preimage")
    if dict(preimage.get("current_credential") or {}) != {
        "producer_id": identity["producer_id"],
        "key_id": identity["key_id"],
        "endpoint_url": identity["endpoint_url"],
    }:
        raise ExactCloneResolutionError("current credential binding changed after preimage")

    marker_path = Path(stop_marker_path).resolve(strict=False)
    marker = read_bounded_json(marker_path, label="Label relay stop marker")
    marker_preimage = dict(preimage.get("stop_marker") or {})
    if (
        not _same_path(marker_preimage.get("path"), marker_path)
        or file_sha256(marker_path) != marker_preimage.get("sha256")
        or marker.get("request_id") != marker_preimage.get("request_id")
    ):
        raise ExactCloneResolutionError("stop marker was not preserved during resolution")

    portable = _portable_binding(portable_root)
    if portable != dict(preimage.get("portable") or {}):
        raise ExactCloneResolutionError("Label portable binding changed after preimage")

    before_client = dict(preimage.get("client") or {})
    candidate_before = _safe_client_authority(
        dict(before_client.get("candidate_authority") or {}),
        "preimage candidate authority",
    )
    prior_before = _safe_client_authority(
        dict(before_client.get("prior_authority") or {}),
        "preimage prior authority",
    )
    client_rows = client_authorities(client_db_path)
    selected = _selected_client_authority(client_rows, identity)
    if (
        selected["authority_scope"] != candidate_before["authority_scope"]
        or selected["runtime_instance_id"] != candidate_before["runtime_instance_id"]
        or selected["runtime_public_jwk_thumbprint"]
        != candidate_before["runtime_public_jwk_thumbprint"]
        or selected["status"] != "ACTIVE"
        or not selected["lease_id"]
        or not selected["fence"]
        or not selected["next_request_sequence"]
        or selected["last_error_code"] not in (None, "")
        or selected["assigned_relay_id"] not in (None, "")
        or selected["pending_issue_idempotency_key"] not in (None, "")
        or selected["pending_request_present"]
        or not selected["next_request_token_present"]
    ):
        raise ExactCloneResolutionError(
            "selected local authority is not the resolved current-credential authority"
        )
    rejected = [
        row for row in client_rows if row["authority_scope"] != selected["authority_scope"]
    ]
    if not rejected or len(rejected) != len(client_rows) - 1:
        raise ExactCloneResolutionError("rejected local authorities are incomplete")
    for row in rejected:
        if (
            row["status"] != "LEGACY_DISABLED"
            or row["assigned_relay_id"] not in (None, "")
            or row["pending_issue_idempotency_key"] not in (None, "")
            or row["pending_request_present"]
            or row["next_request_token_present"]
        ):
            raise ExactCloneResolutionError(
                "a rejected local authority is still runnable or retains request authority"
            )
    if not any(
        row["authority_scope"] == prior_before["authority_scope"]
        and row["runtime_instance_id"] == prior_before["runtime_instance_id"]
        for row in rejected
    ):
        raise ExactCloneResolutionError("the prior active authority was not explicitly retired")

    batches_after = relay_batches_digest(client_db_path)
    if batches_after != before_client.get("relay_batches"):
        raise ExactCloneResolutionError("relay batch rows changed during authority resolution")

    server_before = dict(preimage.get("server") or {})
    quarantine_before = dict(server_before.get("quarantine") or {})
    prior_server = dict(server_before.get("prior_active_lease") or {})
    server_after = _server_rows(server_db_path, identity["producer_install_id"])
    active_after = [row for row in server_after["leases"] if row["status"] == "ACTIVE"]
    if len(active_after) != 1:
        raise ExactCloneResolutionError("server does not have exactly one active install lease")
    active = active_after[0]
    if (
        active["lease_id"] != selected["lease_id"]
        or active["runtime_instance_id"] != selected["runtime_instance_id"]
        or active["public_jwk_thumbprint"]
        != selected["runtime_public_jwk_thumbprint"]
        or active["fence"] != selected["fence"]
        or int(active["fence"]) <= int(prior_server.get("fence") or 0)
    ):
        raise ExactCloneResolutionError("server active lease does not bind the selected authority")
    prior_after = [
        row for row in server_after["leases"] if row["lease_id"] == prior_server.get("lease_id")
    ]
    if len(prior_after) != 1 or prior_after[0]["status"] != "EXPIRED":
        raise ExactCloneResolutionError("prior server lease was not expired")
    matching_quarantines = [
        row
        for row in server_after["quarantines"]
        if row["audit_id"] == quarantine_before.get("audit_id")
        and row["reason_code"] == CONFLICT_CODE
        and row["runtime_instance_id"] == selected["runtime_instance_id"]
        and row["active_lease_id"] == prior_server.get("lease_id")
    ]
    if len(matching_quarantines) != 1 or matching_quarantines[0] != quarantine_before:
        raise ExactCloneResolutionError("immutable server quarantine evidence changed")
    active_anchors = [
        row
        for row in server_after["anchors"]
        if row["outcome"] == "ACTIVE" and row["lease_id"] == selected["lease_id"]
    ]
    if len(active_anchors) != 1:
        raise ExactCloneResolutionError("fresh selected lease issue anchor is absent")

    return {
        "schema_version": RECEIPT_SCHEMA,
        "status": "RESOLVED",
        "conflict_code": CONFLICT_CODE,
        "captured_at": _utc_now(),
        "producer_install_id": identity["producer_install_id"],
        "current_credential": {
            "producer_id": identity["producer_id"],
            "key_id": identity["key_id"],
            "endpoint_url": identity["endpoint_url"],
        },
        "client": {
            "database_path": _resolved_text(client_db_path),
            "relay_batches": batches_after,
            "selected_authority": selected,
            "rejected_authorities": rejected,
        },
        "server": {
            "active_lease_count_after": 1,
            "selected_active_lease": active,
            "prior_lease_after": prior_after[0],
            "quarantine": matching_quarantines[0],
            "fresh_issue_anchor": active_anchors[0],
        },
        "stop_marker": {
            "path": str(marker_path),
            "sha256": file_sha256(marker_path),
            "request_id": str(marker.get("request_id")),
            "preserved_during_resolution": True,
        },
        "portable": portable,
        "invariants": {
            "current_credential_selects_one_authority": True,
            "selected_local_authority_active": True,
            "rejected_local_authorities_disabled": True,
            "server_has_one_active_lease": True,
            "server_fence_advanced": True,
            "server_quarantine_preserved": True,
            "relay_batches_unchanged": True,
            "stop_marker_preserved": True,
            "secret_material_included": False,
        },
    }


def _git_command(repo_root: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            ["git", *arguments],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise ExactCloneResolutionError("portable rebind git proof failed") from exc


def _git_output(repo_root: Path, *arguments: str) -> str:
    completed = _git_command(repo_root, *arguments)
    if completed.returncode != 0:
        raise ExactCloneResolutionError(
            "portable rebind git proof failed: "
            + str(completed.stderr or completed.stdout).strip()[:512]
        )
    return completed.stdout.strip()


def portable_rebind_changed_paths_sha256(paths: Sequence[str]) -> str:
    normalized = sorted(
        {
            _required_text(path, "portable rebind changed path").replace("\\", "/")
            for path in paths
        }
    )
    digest = hashlib.sha256(b"label-match-portable-rebind-changed-paths-v1\n")
    for path in normalized:
        digest.update(path.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def create_portable_successor_receipt(
    *,
    preimage_path: str | os.PathLike[str],
    preimage_sha256: str,
    predecessor_receipt_path: str | os.PathLike[str],
    predecessor_receipt_sha256: str,
    repo_root: str | os.PathLike[str],
    expected_successor_commit: str,
    expected_successor_tree: str,
    expected_successor_manifest_sha256: str,
    expected_successor_installer_sha256: str,
    expected_changed_paths_sha256: str,
    client_db_path: str | os.PathLike[str],
    server_db_path: str | os.PathLike[str],
    identity_path: str | os.PathLike[str],
    credential_path: str | os.PathLike[str],
    stop_marker_path: str | os.PathLike[str],
    portable_root: str | os.PathLike[str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Rebind a valid receipt to one narrowly reviewed portable successor.

    This is evidence-only.  It revalidates the predecessor receipt and the
    resolved live authority state, requires an exact clean Git descendant with
    only the fixed receipt/onboarding paths changed, and never changes either
    SQLite database or the stop marker.
    """

    selected_preimage_path = Path(preimage_path).resolve(strict=False)
    pinned_preimage_sha256 = _required_sha256(
        preimage_sha256,
        "portable rebind conflict preimage SHA-256",
    )
    preimage = read_pinned_json(
        selected_preimage_path,
        pinned_preimage_sha256,
        label="portable rebind conflict preimage",
    )
    selected_predecessor_path = Path(predecessor_receipt_path).resolve(strict=False)
    pinned_predecessor_sha256 = _required_sha256(
        predecessor_receipt_sha256,
        "predecessor conflict-resolution receipt SHA-256",
    )
    predecessor = read_pinned_json(
        selected_predecessor_path,
        pinned_predecessor_sha256,
        label="predecessor conflict-resolution receipt",
    )
    predecessor_portable = dict(predecessor.get("portable") or {})
    predecessor_portable_root = Path(
        _required_text(
            predecessor_portable.get("root"),
            "predecessor receipt portable root",
        )
    ).resolve(strict=False)
    validate_resolution_receipt(
        predecessor,
        client_db_path=client_db_path,
        identity_path=identity_path,
        credential_path=credential_path,
        stop_marker_path=stop_marker_path,
        portable_root=predecessor_portable_root,
    )
    reconstructed = create_resolution_receipt(
        preimage=preimage,
        client_db_path=client_db_path,
        server_db_path=server_db_path,
        identity_path=identity_path,
        credential_path=credential_path,
        stop_marker_path=stop_marker_path,
        portable_root=predecessor_portable_root,
    )
    predecessor_semantics = copy.deepcopy(predecessor)
    reconstructed_semantics = copy.deepcopy(reconstructed)
    predecessor_semantics.pop("captured_at", None)
    reconstructed_semantics.pop("captured_at", None)
    if predecessor_semantics != reconstructed_semantics:
        raise ExactCloneResolutionError(
            "predecessor receipt no longer exactly describes the resolved live state"
        )

    selected_repo_root = Path(repo_root).resolve(strict=False)
    if not (selected_repo_root / ".git").exists():
        # Linked worktrees use a .git file rather than a directory.
        if not (selected_repo_root / ".git").is_file():
            raise ExactCloneResolutionError("portable rebind repo root is not a Git worktree")
    verifier_source = Path(__file__).resolve(strict=False)
    try:
        verifier_relative = verifier_source.relative_to(selected_repo_root).as_posix()
    except ValueError as exc:
        raise ExactCloneResolutionError(
            "executing portable rebind verifier is outside the proved Git worktree"
        ) from exc
    if verifier_relative != "label_exact_clone_resolution.py":
        raise ExactCloneResolutionError(
            "executing portable rebind verifier path differs"
        )
    verifier_blob = _git_output(
        selected_repo_root,
        "hash-object",
        "--",
        str(verifier_source),
    ).lower()
    verifier_head_blob = _git_output(
        selected_repo_root,
        "rev-parse",
        "--verify",
        f"HEAD:{verifier_relative}",
    ).lower()
    if verifier_blob != verifier_head_blob:
        raise ExactCloneResolutionError(
            "executing portable rebind verifier bytes differ from proved Git HEAD"
        )
    if _git_output(
        selected_repo_root,
        "status",
        "--porcelain=v1",
        "--untracked-files=normal",
    ):
        raise ExactCloneResolutionError("portable rebind Git worktree is not clean")
    successor_portable = _portable_binding(portable_root)
    predecessor_commit = _required_git_object(
        predecessor_portable.get("source_commit"),
        "predecessor portable source_commit",
    )
    successor_commit = _required_git_object(
        successor_portable.get("source_commit"),
        "successor portable source_commit",
    )
    successor_tree = _required_git_object(
        successor_portable.get("source_tree"),
        "successor portable source_tree",
    )
    pinned_successor_commit = _required_git_object(
        expected_successor_commit,
        "expected successor source_commit",
    )
    pinned_successor_tree = _required_git_object(
        expected_successor_tree,
        "expected successor source_tree",
    )
    pinned_successor_manifest = _required_sha256(
        expected_successor_manifest_sha256,
        "expected successor portable manifest SHA-256",
    )
    pinned_successor_installer = _required_sha256(
        expected_successor_installer_sha256,
        "expected successor installer SHA-256",
    )
    pinned_changed_paths = _required_sha256(
        expected_changed_paths_sha256,
        "expected successor changed-paths SHA-256",
    )
    if (
        successor_commit != pinned_successor_commit
        or successor_tree != pinned_successor_tree
        or successor_portable["portable_manifest_sha256"]
        != pinned_successor_manifest
        or successor_portable["canonical_installer_sha256"]
        != pinned_successor_installer
    ):
        raise ExactCloneResolutionError("successor portable differs from explicit pins")
    if (
        _git_output(selected_repo_root, "rev-parse", "--verify", "HEAD").lower()
        != successor_commit
        or _git_output(selected_repo_root, "rev-parse", "--verify", "HEAD^{tree}").lower()
        != successor_tree
    ):
        raise ExactCloneResolutionError("successor portable does not bind the clean Git HEAD")
    ancestry = _git_command(
        selected_repo_root,
        "merge-base",
        "--is-ancestor",
        predecessor_commit,
        successor_commit,
    )
    if ancestry.returncode != 0:
        raise ExactCloneResolutionError(
            "successor portable source commit is not a descendant of the predecessor"
        )
    if (
        _git_output(
            selected_repo_root,
            "rev-parse",
            "--verify",
            f"{predecessor_commit}^{{tree}}",
        ).lower()
        != _required_git_object(
            predecessor_portable.get("source_tree"),
            "predecessor portable source_tree",
        )
    ):
        raise ExactCloneResolutionError(
            "predecessor portable source tree does not bind its source commit"
        )
    changed_paths = sorted(
        path.replace("\\", "/")
        for path in _git_output(
            selected_repo_root,
            "diff",
            "--name-only",
            "--no-renames",
            predecessor_commit,
            successor_commit,
            "--",
        ).splitlines()
        if path.strip()
    )
    if set(changed_paths) != set(PORTABLE_REBIND_ALLOWED_PATHS):
        raise ExactCloneResolutionError(
            "portable successor changed paths outside the exact reviewed receipt fix"
        )
    changed_paths_sha256 = portable_rebind_changed_paths_sha256(changed_paths)
    if changed_paths_sha256 != pinned_changed_paths:
        raise ExactCloneResolutionError(
            "portable successor changed-path digest differs from explicit pin"
        )

    successor_receipt = copy.deepcopy(reconstructed)
    successor_receipt["captured_at"] = _utc_now()
    successor_receipt["portable"] = successor_portable
    evidence = {
        "schema_version": PORTABLE_REBIND_SCHEMA,
        "status": "PASS",
        "captured_at": _utc_now(),
        "preimage": {
            "path": str(selected_preimage_path),
            "sha256": pinned_preimage_sha256,
        },
        "predecessor_receipt": {
            "path": str(selected_predecessor_path),
            "sha256": pinned_predecessor_sha256,
            "source_commit": predecessor_commit,
            "source_tree": _required_git_object(
                predecessor_portable.get("source_tree"),
                "predecessor portable source_tree",
            ),
        },
        "successor_portable": successor_portable,
        "git_proof": {
            "repo_root": str(selected_repo_root),
            "worktree_clean": True,
            "predecessor_is_ancestor": True,
            "successor_commit": successor_commit,
            "successor_tree": successor_tree,
            "changed_paths": changed_paths,
            "changed_paths_sha256": changed_paths_sha256,
            "allowed_changed_paths": sorted(PORTABLE_REBIND_ALLOWED_PATHS),
            "explicit_pins": {
                "successor_commit": pinned_successor_commit,
                "successor_tree": pinned_successor_tree,
                "portable_manifest_sha256": pinned_successor_manifest,
                "canonical_installer_sha256": pinned_successor_installer,
                "changed_paths_sha256": pinned_changed_paths,
            },
            "verifier_source": str(verifier_source),
            "verifier_blob": verifier_blob,
        },
        "invariants": {
            "authority_state_revalidated": True,
            "predecessor_receipt_revalidated": True,
            "receipt_semantics_changed_only_by_capture_time_and_portable": True,
            "client_state_mutated": False,
            "server_state_mutated": False,
            "stop_marker_removed": False,
            "secret_material_included": False,
        },
    }
    return successor_receipt, evidence


def _walk_keys(value: Any) -> list[str]:
    names: list[str] = []
    if isinstance(value, Mapping):
        for key, child in value.items():
            names.append(str(key))
            names.extend(_walk_keys(child))
    elif isinstance(value, list):
        for child in value:
            names.extend(_walk_keys(child))
    return names


def validate_resolution_receipt(
    receipt: Mapping[str, Any],
    *,
    client_db_path: str | os.PathLike[str],
    identity_path: str | os.PathLike[str],
    credential_path: str | os.PathLike[str],
    stop_marker_path: str | os.PathLike[str],
    portable_root: str | os.PathLike[str],
    allow_portable_relocation: bool = False,
) -> dict[str, Any]:
    expected_top = {
        "schema_version",
        "status",
        "conflict_code",
        "captured_at",
        "producer_install_id",
        "current_credential",
        "client",
        "server",
        "stop_marker",
        "portable",
        "invariants",
    }
    _require_exact_keys(receipt, expected_top, "resolution receipt")
    if (
        receipt.get("schema_version") != RECEIPT_SCHEMA
        or receipt.get("status") != "RESOLVED"
        or receipt.get("conflict_code") != CONFLICT_CODE
    ):
        raise ExactCloneResolutionError("resolution receipt status contract differs")
    for key in _walk_keys(receipt):
        if key.casefold() in _FORBIDDEN_RECEIPT_KEYS:
            raise ExactCloneResolutionError(
                f"resolution receipt contains forbidden secret-bearing field: {key}"
            )

    identity = _identity_binding(identity_path, credential_path)
    if receipt.get("producer_install_id") != identity["producer_install_id"]:
        raise ExactCloneResolutionError("receipt producer_install_id differs")
    if dict(receipt.get("current_credential") or {}) != {
        "producer_id": identity["producer_id"],
        "key_id": identity["key_id"],
        "endpoint_url": identity["endpoint_url"],
    }:
        raise ExactCloneResolutionError("receipt current credential differs")

    marker_path = Path(stop_marker_path).resolve(strict=False)
    stop = dict(receipt.get("stop_marker") or {})
    _require_exact_keys(
        stop,
        {"path", "sha256", "request_id", "preserved_during_resolution"},
        "resolution receipt stop_marker",
    )
    if (
        not _same_path(stop.get("path"), marker_path)
        or stop.get("preserved_during_resolution") is not True
    ):
        raise ExactCloneResolutionError("receipt stop marker binding differs")
    try:
        stop_marker_lineage = validate_marker_successor_lineage(
            marker_path,
            anchor_request_id=str(stop.get("request_id") or ""),
            anchor_sha256=_required_sha256(
                stop.get("sha256"), "receipt stop marker hash"
            ),
        )
    except StopMarkerLineageError as exc:
        raise ExactCloneResolutionError(
            f"receipt stop marker lineage differs: {exc}"
        ) from exc

    receipt_portable = dict(receipt.get("portable") or {})
    portable_fields = {
        "root",
        "source_commit",
        "source_tree",
        "portable_manifest_sha256",
        "canonical_installer_sha256",
    }
    _require_exact_keys(
        receipt_portable,
        portable_fields,
        "resolution receipt portable packet",
    )
    receipt_portable_root = Path(
        _required_text(receipt_portable.get("root"), "receipt portable root")
    )
    if not receipt_portable_root.is_absolute():
        raise ExactCloneResolutionError("receipt portable root is not absolute")
    try:
        receipt_source_portable = _portable_binding(receipt_portable_root)
    except ExactCloneResolutionError as exc:
        raise ExactCloneResolutionError(
            f"receipt source portable packet binding differs: {exc}"
        ) from exc
    if receipt_portable != receipt_source_portable:
        raise ExactCloneResolutionError("receipt source portable packet binding differs")
    validated_portable = _portable_binding(portable_root)
    portable_identity_fields = portable_fields - {"root"}
    if not allow_portable_relocation and receipt_portable != validated_portable:
        raise ExactCloneResolutionError("receipt portable packet binding differs")
    if allow_portable_relocation and any(
        receipt_portable[field] != validated_portable[field]
        for field in portable_identity_fields
    ):
        raise ExactCloneResolutionError("receipt portable packet binding differs")

    client = dict(receipt.get("client") or {})
    _require_exact_keys(
        client,
        {"database_path", "relay_batches", "selected_authority", "rejected_authorities"},
        "resolution receipt client",
    )
    if not _same_path(client.get("database_path"), client_db_path):
        raise ExactCloneResolutionError("receipt client database path differs")
    current_rows = client_authorities(client_db_path)
    selected_now = _selected_client_authority(current_rows, identity)
    selected_receipt = _safe_client_authority(
        dict(client.get("selected_authority") or {}), "receipt selected authority"
    )
    if selected_now != selected_receipt or selected_now["status"] != "ACTIVE":
        raise ExactCloneResolutionError("receipt selected authority no longer matches client state")
    rejected_receipt = [
        _safe_client_authority(dict(row), "receipt rejected authority")
        for row in list(client.get("rejected_authorities") or [])
    ]
    rejected_now = [
        row
        for row in current_rows
        if row["authority_scope"] != selected_now["authority_scope"]
    ]
    if rejected_now != rejected_receipt or any(
        row["status"] != "LEGACY_DISABLED"
        or row["next_request_token_present"]
        or row["pending_request_present"]
        or row["assigned_relay_id"] not in (None, "")
        for row in rejected_now
    ):
        raise ExactCloneResolutionError("receipt rejected authorities are not disabled")
    if dict(client.get("relay_batches") or {}) != relay_batches_digest(client_db_path):
        raise ExactCloneResolutionError("receipt relay batch digest differs")

    server = dict(receipt.get("server") or {})
    _require_exact_keys(
        server,
        {
            "active_lease_count_after",
            "selected_active_lease",
            "prior_lease_after",
            "quarantine",
            "fresh_issue_anchor",
        },
        "resolution receipt server",
    )
    selected_server = dict(server.get("selected_active_lease") or {})
    if (
        server.get("active_lease_count_after") != 1
        or selected_server.get("status") != "ACTIVE"
        or selected_server.get("lease_id") != selected_now["lease_id"]
        or selected_server.get("runtime_instance_id")
        != selected_now["runtime_instance_id"]
        or selected_server.get("public_jwk_thumbprint")
        != selected_now["runtime_public_jwk_thumbprint"]
        or selected_server.get("fence") != selected_now["fence"]
        or dict(server.get("prior_lease_after") or {}).get("status") != "EXPIRED"
        or dict(server.get("quarantine") or {}).get("reason_code") != CONFLICT_CODE
        or dict(server.get("fresh_issue_anchor") or {}).get("outcome") != "ACTIVE"
        or dict(server.get("fresh_issue_anchor") or {}).get("lease_id")
        != selected_now["lease_id"]
    ):
        raise ExactCloneResolutionError("receipt server resolution proof differs")

    invariants = dict(receipt.get("invariants") or {})
    expected_invariants = {
        "current_credential_selects_one_authority": True,
        "selected_local_authority_active": True,
        "rejected_local_authorities_disabled": True,
        "server_has_one_active_lease": True,
        "server_fence_advanced": True,
        "server_quarantine_preserved": True,
        "relay_batches_unchanged": True,
        "stop_marker_preserved": True,
        "secret_material_included": False,
    }
    if invariants != expected_invariants:
        raise ExactCloneResolutionError("resolution receipt invariants differ")
    return {
        "schema_version": RECEIPT_SCHEMA,
        "status": "RESOLVED",
        "producer_install_id": identity["producer_install_id"],
        "selected_authority_scope": selected_now["authority_scope"],
        "selected_runtime_instance_id": selected_now["runtime_instance_id"],
        "selected_lease_id": selected_now["lease_id"],
        "selected_fence": _positive_int(selected_now["fence"], "selected fence"),
        "rejected_authority_count": len(rejected_now),
        "stop_marker_lineage": stop_marker_lineage,
        "portable_source_commit": receipt_portable["source_commit"],
        "portable_source_tree": receipt_portable["source_tree"],
        "portable_receipt_root": str(receipt_portable_root.resolve(strict=False)),
        "portable_validated_root": validated_portable["root"],
        "portable_relocated": not _same_path(
            receipt_portable_root,
            validated_portable["root"],
        ),
    }


__all__ = [
    "CONFLICT_CODE",
    "PORTABLE_REBIND_ALLOWED_PATHS",
    "PORTABLE_REBIND_SCHEMA",
    "PREIMAGE_SCHEMA",
    "RECEIPT_SCHEMA",
    "ExactCloneResolutionError",
    "capture_conflict_preimage",
    "client_authorities",
    "create_portable_successor_receipt",
    "create_resolution_receipt",
    "file_sha256",
    "json_document_sha256",
    "portable_rebind_changed_paths_sha256",
    "read_bounded_json",
    "read_pinned_json",
    "relay_batches_digest",
    "sqlite_logical_digest",
    "sqlite_logical_digest_on_connection",
    "validate_resolution_receipt",
    "write_new_json",
]

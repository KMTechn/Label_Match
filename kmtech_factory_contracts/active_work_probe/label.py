"""Pure-read Label_Match active-work adapter.

The adapter intentionally duplicates the small, stable on-disk contracts it
needs.  Importing Label_Match modules would initialize Tk, create directories,
or migrate SQLite stores, which is forbidden during qualification.
"""

from __future__ import annotations

import base64
import hashlib
import json
import ntpath
import os
import re
import sqlite3
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import quote, urlsplit

from .adapters.common import TrustedRoots, production_roots, validate_profile
from .core import (
    DatabaseObservation,
    Observation,
    ObservationSession,
    ProbeError,
    QuerySpec,
    SqlitePlan,
    StatusDomain,
    snapshot_path,
    strict_json_bytes,
)


APP = "Label_Match"
APP_ID = "label_match"
DATABASE_IDENTITY_PATH = (
    "C:/ProgramData/KMTech/DirectSync/label_match/queue/"
    "direct_sync_relay.sqlite3"
)
PROFILE_IDENTITY_PATH = (
    "C:/ProgramData/KMTech/Logistics/profiles/Label_Match/"
    "runtime-profile.json"
)
OUTBOX_SCHEMA_VERSION = "label-match-package-outbox-v10"
SEALED_EXCHANGE_SCHEMA_VERSION = "label-match-sealed-transfer-exchange-v1"
PHS_JOURNAL_SCHEMA_VERSION = "label-match-phs-label-exchange-v1"
PHS_TERMINAL_STATUSES = frozenset({"COMMITTED", "CANCELLED"})

_RELAY_STATUSES = frozenset(
    {"pending", "leased", "retry_wait", "acked", "failed_permanent", "operator_review"}
)
_RUNTIME_STATUSES = frozenset({"PENDING", "ACTIVE", "OPERATOR_REVIEW", "LEGACY_DISABLED"})
_PACKAGE_STATUSES = frozenset({"PENDING", "SENDING", "ACKED", "CONFLICT"})
_CANCELLATION_STATUSES = frozenset(
    {"DEFERRED", "PENDING", "SENDING", "ACKED", "CONFLICT"}
)
_LEASE_STATUSES = frozenset(
    {"PREFETCHED", "LOCAL_COMPLETED", "ACKED", "OPERATOR_REVIEW"}
)
_ISSUE_ATTEMPT_STATUSES = frozenset({"ACTIVE", "RETIRED"})
_EXCHANGE_STATUSES = frozenset(
    {"PREPARED", "COMMAND_READY", "RETRY_WAIT", "ACKED", "OPERATOR_REVIEW"}
)
_SEAL_STATUSES = frozenset({"PENDING", "VERIFIED", "OPERATOR_REVIEW"})
_LOCAL_APPLY_STATUSES = frozenset({"PENDING", "APPLIED", "OPERATOR_REVIEW"})
_SOURCE_PREFIX = "포장실작업이벤트로그_"
_HEX_64_RE = re.compile(r"^[0-9a-f]{64}$")
_B64URL_COORDINATE_RE = re.compile(r"^[A-Za-z0-9_-]{43}$")
_RUNTIME_ID_RE = re.compile(r"^runtime-[0-9a-f]{32}$")
_RUNTIME_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{32,256}$")
_DELTA_NAME_RE = re.compile(
    r"^bytes-(?P<start>[0-9]+)-(?P<end>[0-9]+)-sha256-(?P<prefix>[0-9a-f]{16})\.csv$"
)


@dataclass(frozen=True)
class _ProducerIdentity:
    pc_id: str
    source_host_id: str
    producer_install_id: str
    producer_id: str
    key_id: str
    endpoint_url: str
    manifest_hash: str
    manifest_path: Path

# The broker has a hard 64-kind vocabulary limit.  Keep the two desktop
# adapters on a deliberately compact, deterministic vocabulary.
BLOCKER_KIND_CATALOG = (
    "label_exchange_pending",
    "label_operation_pending",
    "label_package_pending",
    "label_projection_pending",
    "relay_unacked_batch",
    "runtime_authority_unresolved",
    "session_recovery_active",
    "source_sync_pending",
)


def _windows_key(path: str | os.PathLike[str]) -> str:
    return ntpath.normcase(ntpath.normpath(os.fspath(path).replace("/", "\\")))


def _same_windows_path(left: str | os.PathLike[str], right: str | os.PathLike[str]) -> bool:
    return _windows_key(left) == _windows_key(right)


def _absolute(path: Path) -> Path:
    value = Path(os.path.abspath(os.fspath(path)))
    if not value.is_absolute():
        raise ProbeError("ROOT_UNTRUSTED", "probe root is not absolute")
    return value


def _safe_regular_children(root: Path) -> tuple[Path, ...]:
    """Enumerate direct regular children without following links/reparse points."""

    root_snapshot = snapshot_path(root)
    if not root_snapshot.exists:
        return ()
    if root_snapshot.kind != "directory":
        raise ProbeError("RESOURCE_TYPE_INVALID", "state root is not a directory")
    try:
        children = tuple(sorted(os.scandir(root), key=lambda item: item.name.casefold()))
    except OSError as exc:
        raise ProbeError("RESOURCE_SNAPSHOT_ERROR", repr(exc)) from exc
    regular: list[Path] = []
    for child in children:
        try:
            if child.is_symlink():
                raise ProbeError("RESOURCE_REPARSE_POINT", "reparse child is forbidden")
            if child.is_file(follow_symlinks=False):
                regular.append(Path(child.path))
        except OSError as exc:
            raise ProbeError("RESOURCE_SNAPSHOT_ERROR", repr(exc)) from exc
    return tuple(regular)


def _validate_app_profile(
    session: ObservationSession,
    canonical_path: Path,
    legacy_paths: Sequence[Path],
    *,
    app: str,
) -> Mapping[str, Any]:
    value = validate_profile(session, canonical_path, legacy_paths)
    required = {
        "contract_version",
        "base_url",
        "authority_scope",
        "authority_epoch",
        "authority_plane",
        "ledger_plane",
        "plane_epoch",
        "device_id",
        "source_host_id",
    }
    allowed = required | {"bearer_token_ref", "timeout_seconds"}
    if set(value) - allowed or not required.issubset(value):
        raise ProbeError("PROFILE_INVALID", "runtime profile fields are not canonical")
    if canonical_path.name.casefold() != "runtime-profile.json" or canonical_path.parent.name != app:
        raise ProbeError("PROFILE_INVALID", "runtime profile app identity is not canonical")
    reference = value.get("bearer_token_ref")
    if reference is not None and reference != "dpapi:secrets/bearer-token.dpapi":
        raise ProbeError("PROFILE_INVALID", "runtime profile secret reference is invalid")
    if str(value["authority_plane"]).upper() != "AUTHORITATIVE":
        raise ProbeError("PROFILE_INVALID", "runtime authority plane is not authoritative")
    timeout = value.get("timeout_seconds", 10)
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0.1 <= timeout <= 60:
        raise ProbeError("PROFILE_INVALID", "runtime profile timeout is invalid")
    return value


def _normalize_manifest_value(value: Any) -> Any:
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value)
    if isinstance(value, list):
        return [_normalize_manifest_value(item) for item in value]
    if isinstance(value, dict):
        return {
            str(key): _normalize_manifest_value(value[key])
            for key in sorted(value)
        }
    return value


def _canonical_manifest_hash(manifest: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        _normalize_manifest_value(dict(manifest)),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _validate_producer_registration(
    session: ObservationSession,
    runtime_profile: Mapping[str, Any],
    manifest_path: Path,
    registration_report_path: Path,
    *,
    manifest_app: str,
    report_version: str,
    report_manifest_path_field: str,
    report_app: str | None,
    producer_role: str,
    stream_name: str,
    source_system: str,
    source_transport: str,
    required_report_flags: Sequence[str],
) -> _ProducerIdentity:
    manifest = session.read_json(manifest_path, required=True)
    if not isinstance(manifest, Mapping):
        raise ProbeError("PRODUCER_MANIFEST_INVALID", "producer manifest is not an object")
    identity = manifest.get("pc_identity")
    streams = manifest.get("streams")
    if (
        manifest.get("schema_version") != "producer-onboarding-manifest-v1"
        or manifest.get("apps") != [manifest_app]
        or not isinstance(identity, Mapping)
        or not isinstance(streams, list)
        or len(streams) != 1
        or not isinstance(streams[0], Mapping)
    ):
        raise ProbeError("PRODUCER_MANIFEST_INVALID", "producer manifest identity is not canonical")
    identity_values: dict[str, str] = {}
    for key in ("pc_id", "source_host_id", "producer_install_id"):
        value = identity.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ProbeError("PRODUCER_MANIFEST_INVALID", "producer manifest identity is incomplete")
        identity_values[key] = value
    if (
        identity_values["source_host_id"] != runtime_profile["source_host_id"]
        or identity_values["pc_id"] != runtime_profile["device_id"]
    ):
        raise ProbeError(
            "RELAY_IDENTITY_MISMATCH",
            "producer manifest and runtime profile identities differ",
        )
    stream = streams[0]
    if (
        stream.get("producer_role") != producer_role
        or stream.get("stream_name") != stream_name
        or stream.get("source_system") != source_system
        or stream.get("source_transport") != source_transport
    ):
        raise ProbeError("PRODUCER_MANIFEST_INVALID", "producer manifest stream identity differs")
    expected_endpoint = (
        str(runtime_profile["base_url"]).rstrip("/")
        + "/api/producer-ingest/v1/source-file"
    )
    sync = manifest.get("sync")
    if not isinstance(sync, Mapping):
        raise ProbeError("PRODUCER_MANIFEST_INVALID", "producer manifest sync contract is absent")
    auth = sync.get("auth")
    queue = sync.get("queue")
    if (
        sync.get("sync_transport") != "http_push"
        or sync.get("server_ingest_target") != expected_endpoint
        or not isinstance(auth, Mapping)
        or auth.get("method") != "producer_hmac_v1"
        or not isinstance(queue, Mapping)
        or queue.get("allowed_streams") != [stream_name]
    ):
        raise ProbeError("PRODUCER_MANIFEST_INVALID", "producer manifest sync identity differs")
    manifest_digest = _canonical_manifest_hash(manifest)

    report = session.read_json(registration_report_path, required=True)
    if not isinstance(report, Mapping):
        raise ProbeError("REGISTRATION_REPORT_INVALID", "registration report is not an object")
    if (
        report.get("report_version") != report_version
        or report.get("status") != "SELF_ENROLLMENT_REGISTERED"
        or report.get("server_registration_verified") is not True
        or report.get("secret_bootstrap_verified") is not True
        or any(report.get(key) is not True for key in required_report_flags)
    ):
        raise ProbeError("REGISTRATION_REPORT_INVALID", "registration report is not verified")
    if report_app is not None and report.get("app") != report_app:
        raise ProbeError("REGISTRATION_REPORT_INVALID", "registration report app identity differs")
    if (
        report.get("hostname") != identity_values["pc_id"]
        or report.get("source_host_id") != identity_values["source_host_id"]
        or report.get("producer_install_id") != identity_values["producer_install_id"]
        or report.get("endpoint_url") != expected_endpoint
        or report.get("manifest_hash") != manifest_digest
        or not _same_windows_path(
            str(report.get(report_manifest_path_field) or ""),
            manifest_path,
        )
        or not _same_windows_path(
            str(report.get("report_path") or ""),
            registration_report_path,
        )
    ):
        raise ProbeError("RELAY_IDENTITY_MISMATCH", "registration identity differs")
    producer_id = report.get("producer_id")
    key_id = report.get("key_id")
    if (
        not isinstance(producer_id, str)
        or not producer_id.strip()
        or not isinstance(key_id, str)
        or not key_id.strip()
        or not _HEX_64_RE.fullmatch(manifest_digest)
    ):
        raise ProbeError("REGISTRATION_REPORT_INVALID", "registration producer identity is incomplete")
    return _ProducerIdentity(
        pc_id=identity_values["pc_id"],
        source_host_id=identity_values["source_host_id"],
        producer_install_id=identity_values["producer_install_id"],
        producer_id=producer_id,
        key_id=key_id,
        endpoint_url=expected_endpoint,
        manifest_hash=manifest_digest,
        manifest_path=manifest_path,
    )


def _validate_public_jwk(value: object) -> None:
    if not isinstance(value, Mapping) or set(value) != {"kty", "crv", "x", "y"}:
        raise ProbeError("RUNTIME_AUTHORITY_INVALID", "runtime public JWK shape is invalid")
    if value.get("kty") != "EC" or value.get("crv") != "P-256":
        raise ProbeError("RUNTIME_AUTHORITY_INVALID", "runtime public JWK curve is invalid")
    coordinates: list[int] = []
    for key in ("x", "y"):
        coordinate = value.get(key)
        if not isinstance(coordinate, str) or not _B64URL_COORDINATE_RE.fullmatch(coordinate):
            raise ProbeError("RUNTIME_AUTHORITY_INVALID", "runtime public JWK coordinate is invalid")
        try:
            raw = base64.urlsafe_b64decode(coordinate + "=")
        except (ValueError, TypeError) as exc:
            raise ProbeError("RUNTIME_AUTHORITY_INVALID", repr(exc)) from exc
        if len(raw) != 32:
            raise ProbeError("RUNTIME_AUTHORITY_INVALID", "runtime public JWK coordinate length is invalid")
        coordinates.append(int.from_bytes(raw, "big"))
    prime = 0xFFFFFFFF00000001000000000000000000000000FFFFFFFFFFFFFFFFFFFFFFFF
    curve_b = 0x5AC635D8AA3A93E7B3EBBD55769886BC651D06B0CC53B0F63BCE3C3E27D2604B
    x_value, y_value = coordinates
    if (
        not 0 < x_value < prime
        or not 0 < y_value < prime
        or pow(y_value, 2, prime)
        != (pow(x_value, 3, prime) - 3 * x_value + curve_b) % prime
    ):
        raise ProbeError("RUNTIME_AUTHORITY_INVALID", "runtime public JWK point is invalid")


def _immutable_connection(path: Path) -> sqlite3.Connection:
    uri_path = quote(Path(os.path.abspath(os.fspath(path))).as_posix(), safe="/:")
    try:
        connection = sqlite3.connect(
            f"file:{uri_path}?mode=ro&immutable=1",
            uri=True,
            timeout=0.0,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        return connection
    except sqlite3.Error as exc:
        raise ProbeError("SQLITE_OBSERVATION_FAILED", repr(exc)) from exc


def _table_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = connection.execute(f'PRAGMA table_info("{table}")').fetchall()
    except sqlite3.Error as exc:
        raise ProbeError("SQLITE_OBSERVATION_FAILED", repr(exc)) from exc
    if not rows:
        raise ProbeError("SQLITE_REQUIRED_TABLE_MISSING", table)
    return {str(row[1]) for row in rows}


def _require_columns(
    connection: sqlite3.Connection,
    table: str,
    required: set[str],
) -> None:
    missing = required - _table_columns(connection, table)
    if missing:
        raise ProbeError(
            "SQLITE_REQUIRED_COLUMN_MISSING",
            f"{table}:{sorted(missing)!r}",
        )


def _validate_outbox_schema(path: Path) -> None:
    connection = _immutable_connection(path)
    try:
        _require_columns(connection, "package_outbox_schema_info", {"key", "value"})
        rows = connection.execute(
            "SELECT value FROM package_outbox_schema_info WHERE key='schema_version'"
        ).fetchall()
        if len(rows) != 1 or str(rows[0][0] or "") != OUTBOX_SCHEMA_VERSION:
            raise ProbeError("SCHEMA_VERSION_UNKNOWN", "Label_Match outbox schema is unknown")
    except ProbeError:
        raise
    except sqlite3.Error as exc:
        raise ProbeError("SQLITE_OBSERVATION_FAILED", repr(exc)) from exc
    finally:
        connection.close()


def _validate_runtime_authority(
    path: Path,
    expected_identity: _ProducerIdentity,
    *,
    require_row: bool,
) -> None:
    connection = _immutable_connection(path)
    try:
        columns = {
            "authority_scope",
            "endpoint_url",
            "producer_id",
            "key_id",
            "producer_install_id",
            "status",
            "runtime_instance_id",
            "runtime_public_jwk_json",
            "lease_id",
            "fence",
            "next_request_token",
            "next_request_sequence",
            "expires_at",
            "assigned_relay_id",
            "pending_request_json",
            "pending_issue_idempotency_key",
            "last_error_code",
            "created_at",
            "updated_at",
        }
        _require_columns(connection, "direct_sync_runtime_authority", columns)
        relay_statuses = {
            str(row[0]): str(row[1])
            for row in connection.execute(
                "SELECT relay_id,status FROM direct_sync_relay_batches"
            )
        }
        rows = connection.execute(
            "SELECT authority_scope,endpoint_url,producer_id,key_id,producer_install_id,"
            "status,runtime_instance_id,runtime_public_jwk_json,"
            "lease_id,fence,next_request_token,next_request_sequence,expires_at,"
            "assigned_relay_id,pending_request_json,pending_issue_idempotency_key,"
            "last_error_code,created_at,updated_at "
            "FROM direct_sync_runtime_authority"
        ).fetchall()
        if len(rows) > 1 or (require_row and len(rows) != 1):
            raise ProbeError(
                "RUNTIME_AUTHORITY_INVALID",
                "runtime authority identity is absent or ambiguous",
            )
        expected_scope_values = {
            "endpoint_url": expected_identity.endpoint_url,
            "producer_id": expected_identity.producer_id,
            "key_id": expected_identity.key_id,
            "producer_install_id": expected_identity.producer_install_id,
        }
        for row in rows:
            status = str(row["status"] or "")
            try:
                public_jwk = strict_json_bytes(
                    str(row["runtime_public_jwk_json"] or "").encode("utf-8")
                )
            except ProbeError as exc:
                raise ProbeError("RUNTIME_AUTHORITY_INVALID", exc.detail) from exc
            _validate_public_jwk(public_jwk)
            endpoint = str(row["endpoint_url"] or "").strip()
            try:
                endpoint_parts = urlsplit(endpoint)
            except ValueError as exc:
                raise ProbeError("RUNTIME_AUTHORITY_INVALID", repr(exc)) from exc
            if (
                endpoint_parts.scheme.lower() != "https"
                or not endpoint_parts.netloc
                or endpoint_parts.username is not None
                or endpoint_parts.password is not None
                or endpoint_parts.path.rstrip("/") != "/api/producer-ingest/v1/source-file"
                or endpoint_parts.query
                or endpoint_parts.fragment
            ):
                raise ProbeError("RUNTIME_AUTHORITY_INVALID", "runtime endpoint is invalid")
            scope_values = {
                "endpoint_url": endpoint,
                "producer_id": str(row["producer_id"] or "").strip(),
                "key_id": str(row["key_id"] or "").strip(),
                "producer_install_id": str(row["producer_install_id"] or "").strip(),
            }
            if scope_values != expected_scope_values:
                raise ProbeError(
                    "RUNTIME_AUTHORITY_INVALID",
                    "runtime authority and registration identities differ",
                )
            expected_scope = hashlib.sha256(
                json.dumps(
                    expected_scope_values,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("utf-8")
            ).hexdigest()
            runtime_id = str(row["runtime_instance_id"] or "")
            if (
                not all(scope_values.values())
                or str(row["authority_scope"] or "") != expected_scope
                or not _RUNTIME_ID_RE.fullmatch(runtime_id)
            ):
                raise ProbeError("RUNTIME_AUTHORITY_INVALID", "runtime identity is incomplete")
            pending_json = str(row["pending_request_json"] or "")
            pending_key = str(row["pending_issue_idempotency_key"] or "")
            if bool(pending_json) != bool(pending_key):
                raise ProbeError("RUNTIME_AUTHORITY_INVALID", "pending request pair is incomplete")
            if pending_json:
                try:
                    pending_value = strict_json_bytes(pending_json.encode("utf-8"))
                except ProbeError as exc:
                    raise ProbeError("RUNTIME_AUTHORITY_INVALID", exc.detail) from exc
                if not isinstance(pending_value, dict):
                    raise ProbeError("RUNTIME_AUTHORITY_INVALID", "pending request is not an object")
                if (
                    pending_value.get("contract_version") != "producer-runtime-lease.v1"
                    or pending_value.get("runtime_instance_id") != runtime_id
                    or pending_value.get("public_jwk") != public_jwk
                    or pending_value.get("issue_idempotency_key") != pending_key
                ):
                    raise ProbeError("RUNTIME_AUTHORITY_INVALID", "pending request identity is invalid")
                optional_coordinates = (
                    "runtime_fence",
                    "runtime_request_token",
                    "runtime_request_sequence",
                )
                if set(pending_value) - {
                    "contract_version",
                    "runtime_instance_id",
                    "public_jwk",
                    "issue_idempotency_key",
                    "ttl_seconds",
                    *optional_coordinates,
                }:
                    raise ProbeError("RUNTIME_AUTHORITY_INVALID", "pending request fields are invalid")
                ttl = pending_value.get("ttl_seconds")
                if isinstance(ttl, bool) or not isinstance(ttl, int) or not 1 <= ttl <= 86400:
                    raise ProbeError("RUNTIME_AUTHORITY_INVALID", "pending request TTL is invalid")
                present_coordinates = [key in pending_value for key in optional_coordinates]
                if any(present_coordinates) and not all(present_coordinates):
                    raise ProbeError("RUNTIME_AUTHORITY_INVALID", "pending renewal coordinates are incomplete")
            assigned = str(row["assigned_relay_id"] or "")
            if assigned and relay_statuses.get(assigned) != "leased":
                raise ProbeError("RUNTIME_AUTHORITY_INVALID", "assigned relay is not leased")
            for timestamp_key in ("created_at", "updated_at"):
                timestamp = str(row[timestamp_key] or "")
                try:
                    parsed_timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                except ValueError as exc:
                    raise ProbeError("RUNTIME_AUTHORITY_INVALID", repr(exc)) from exc
                if parsed_timestamp.tzinfo is None:
                    raise ProbeError("RUNTIME_AUTHORITY_INVALID", "runtime timestamp lacks timezone")
            coordinates = (
                row["lease_id"],
                row["fence"],
                row["next_request_token"],
                row["next_request_sequence"],
                row["expires_at"],
            )
            if status == "ACTIVE" and not all(value not in (None, "") for value in coordinates):
                raise ProbeError("RUNTIME_AUTHORITY_INVALID", "ACTIVE coordinates are incomplete")
            if status == "ACTIVE":
                if (
                    isinstance(row["fence"], bool)
                    or not isinstance(row["fence"], int)
                    or int(row["fence"]) < 1
                    or isinstance(row["next_request_sequence"], bool)
                    or not isinstance(row["next_request_sequence"], int)
                    or int(row["next_request_sequence"]) < 1
                    or not _RUNTIME_TOKEN_RE.fullmatch(str(row["next_request_token"] or ""))
                ):
                    raise ProbeError("RUNTIME_AUTHORITY_INVALID", "ACTIVE coordinates are invalid")
                try:
                    expires_at = datetime.fromisoformat(
                        str(row["expires_at"] or "").replace("Z", "+00:00")
                    )
                except ValueError as exc:
                    raise ProbeError("RUNTIME_AUTHORITY_INVALID", repr(exc)) from exc
                if expires_at.tzinfo is None:
                    raise ProbeError("RUNTIME_AUTHORITY_INVALID", "runtime expiry lacks timezone")
            if status in {"PENDING", "LEGACY_DISABLED"} and any(
                value not in (None, "") for value in coordinates
            ):
                raise ProbeError("RUNTIME_AUTHORITY_INVALID", "inactive coordinates remain populated")
            if status == "LEGACY_DISABLED" and (assigned or pending_json or pending_key):
                raise ProbeError("RUNTIME_AUTHORITY_INVALID", "disabled authority retains work")
    except ProbeError:
        raise
    except sqlite3.Error as exc:
        raise ProbeError("SQLITE_OBSERVATION_FAILED", repr(exc)) from exc
    finally:
        connection.close()


def _is_within(path: Path, root: Path) -> bool:
    try:
        Path(os.path.abspath(os.fspath(path))).relative_to(
            Path(os.path.abspath(os.fspath(root)))
        )
        return True
    except ValueError:
        return False


def _safe_regular_tree(root: Path) -> tuple[Path, ...]:
    root_snapshot = snapshot_path(root)
    if not root_snapshot.exists:
        return ()
    if root_snapshot.kind != "directory":
        raise ProbeError("RESOURCE_TYPE_INVALID", "artifact root is not a directory")
    files: list[Path] = []
    try:
        for current, directories, names in os.walk(root, topdown=True, followlinks=False):
            directories.sort(key=str.casefold)
            names.sort(key=str.casefold)
            for name in names:
                candidate = Path(current) / name
                candidate_snapshot = snapshot_path(candidate)
                if candidate_snapshot.kind != "regular":
                    raise ProbeError("RESOURCE_TYPE_INVALID", "artifact is not a regular file")
                files.append(candidate)
    except OSError as exc:
        raise ProbeError("RESOURCE_SNAPSHOT_ERROR", repr(exc)) from exc
    return tuple(files)


def _read_stable_json(path: Path) -> Any:
    before = snapshot_path(path)
    if not before.exists or before.kind != "regular" or before.size <= 0:
        raise ProbeError("ARTIFACT_INVALID", "required artifact is absent")
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ProbeError("FILE_READ_ERROR", repr(exc)) from exc
    if hashlib.sha256(raw).hexdigest() != before.sha256:
        raise ProbeError("RESOURCE_CONTINUITY_CHANGED", "artifact changed during read")
    return strict_json_bytes(raw)


def _source_delta_key(path: Path) -> str:
    return hashlib.sha256(str(path.resolve()).encode("utf-8")).hexdigest()[:16]


def _parse_delta_identity(
    relative_path: str,
    source: Path,
    *,
    container: bool,
) -> tuple[int, int, str] | None:
    key = _source_delta_key(source)
    if container:
        prefixes = (f"d/{key}/", f"legacy_csv_deltas/{key}/", f"legacy_csv_deltas/{source.name}/")
    else:
        prefixes = (
            f"legacy_csv_deltas/source-{key}/",
            f"legacy_csv_deltas/{source.name}/",
        )
    prefix = next((value for value in prefixes if relative_path.startswith(value)), None)
    if prefix is None:
        return None
    match = _DELTA_NAME_RE.fullmatch(relative_path[len(prefix) :])
    if match is None:
        raise ProbeError("SOURCE_DELTA_INVALID", "delta relative path is invalid")
    start = int(match.group("start"))
    end = int(match.group("end"))
    if end <= start:
        raise ProbeError("SOURCE_DELTA_INVALID", "delta range is invalid")
    return start, end, match.group("prefix")


def _validate_relay_artifacts(
    path: Path,
    direct_sync_root: Path,
    sources: Sequence[Path],
    expected_identity: _ProducerIdentity,
    *,
    producer_role: str,
    stream_name: str,
    source_system: str,
    source_transport: str,
    container: bool,
) -> tuple[dict[str, list[tuple[int, int]]], int]:
    connection = _immutable_connection(path)
    spool_root = direct_sync_root / "spool"
    status_root = direct_sync_root / "status"
    known_spool: set[str] = set()
    known_status: set[str] = set()
    canonical_source_keys = {_windows_key(source) for source in sources}
    ranges: dict[str, list[tuple[int, int]]] = {
        _windows_key(source): [] for source in sources
    }
    try:
        required = {
            "relay_id",
            "status",
            "source_file_path",
            "spooled_file_path",
            "producer_manifest_path",
            "relative_path",
            "content_sha256",
            "byte_length",
            "lease_owner",
            "lease_expires_at",
            "receipt_json",
            "upload_status_path",
            "metadata_json",
            "producer_id",
            "key_id",
            "endpoint_url",
            "runtime_fencing_policy",
        }
        _require_columns(connection, "direct_sync_relay_batches", required)
        rows = connection.execute(
            "SELECT relay_id,status,source_file_path,spooled_file_path,"
            "producer_manifest_path,relative_path,content_sha256,byte_length,"
            "lease_owner,lease_expires_at,receipt_json,upload_status_path,"
            "metadata_json,producer_id,key_id,endpoint_url,runtime_fencing_policy "
            "FROM direct_sync_relay_batches ORDER BY relay_id"
        ).fetchall()
        for row in rows:
            relay_id = str(row["relay_id"] or "").strip()
            status = str(row["status"] or "")
            relative_path = str(row["relative_path"] or "").replace("\\", "/")
            content_hash = str(row["content_sha256"] or "")
            byte_length = row["byte_length"]
            if (
                not relay_id
                or not relative_path
                or not _HEX_64_RE.fullmatch(content_hash)
                or isinstance(byte_length, bool)
                or not isinstance(byte_length, int)
                or byte_length < 0
            ):
                raise ProbeError("RELAY_ROW_INVALID", "relay identity is invalid")
            try:
                metadata = strict_json_bytes(str(row["metadata_json"] or "").encode("utf-8"))
            except ProbeError as exc:
                raise ProbeError("RELAY_ROW_INVALID", exc.detail) from exc
            if not isinstance(metadata, Mapping):
                raise ProbeError("RELAY_ROW_INVALID", "relay metadata is not an object")
            required_metadata = {
                "contract_version",
                "producer_install_id",
                "source_host_id",
                "producer_role",
                "manifest_hash",
                "stream_name",
                "source_system",
                "source_transport",
                "content_sha256",
                "byte_length",
                "client_batch_id",
                "idempotency_key",
                "relative_path",
                "row_count",
            }
            if not required_metadata.issubset(metadata):
                raise ProbeError("RELAY_ROW_INVALID", "relay metadata fields are incomplete")
            if (
                metadata.get("contract_version") != "producer-ingest-source-file-v1"
                or metadata.get("producer_install_id")
                != expected_identity.producer_install_id
                or metadata.get("source_host_id") != expected_identity.source_host_id
                or metadata.get("producer_role") != producer_role
                or metadata.get("manifest_hash") != expected_identity.manifest_hash
                or metadata.get("stream_name") != stream_name
                or metadata.get("source_system") != source_system
                or metadata.get("source_transport") != source_transport
                or metadata.get("content_sha256") != content_hash
                or metadata.get("byte_length") != byte_length
                or metadata.get("relative_path") != relative_path
                or metadata.get("client_batch_id") != relay_id
                or not isinstance(metadata.get("row_count"), int)
                or isinstance(metadata.get("row_count"), bool)
                or int(metadata["row_count"]) < 0
            ):
                raise ProbeError("RELAY_ROW_INVALID", "relay metadata identity differs")
            for identity_field in (
                "producer_install_id",
                "source_host_id",
                "manifest_hash",
                "source_system",
                "source_transport",
                "idempotency_key",
            ):
                if not isinstance(metadata.get(identity_field), str) or not str(
                    metadata[identity_field]
                ).strip():
                    raise ProbeError("RELAY_ROW_INVALID", "relay metadata identity is incomplete")
            if (
                str(row["producer_id"] or "") != expected_identity.producer_id
                or str(row["key_id"] or "") != expected_identity.key_id
                or str(row["endpoint_url"] or "") != expected_identity.endpoint_url
            ):
                raise ProbeError("RELAY_IDENTITY_MISMATCH", "relay producer identity differs")
            spooled = Path(str(row["spooled_file_path"] or ""))
            source_path = Path(str(row["source_file_path"] or ""))
            manifest = Path(str(row["producer_manifest_path"] or ""))
            if not spooled.is_absolute() or not _is_within(spooled, spool_root):
                raise ProbeError("RELAY_ARTIFACT_INVALID", "spooled path is outside the canonical root")
            known_spool.add(_windows_key(spooled))
            if source_path.is_absolute() and _is_within(source_path, spool_root):
                known_spool.add(_windows_key(source_path))
            if manifest.is_absolute() and _is_within(manifest, spool_root):
                known_spool.add(_windows_key(manifest))
            spooled_snapshot = snapshot_path(spooled)
            if (
                spooled_snapshot.kind != "regular"
                or spooled_snapshot.size != byte_length
                or spooled_snapshot.sha256 != content_hash
            ):
                raise ProbeError("RELAY_ARTIFACT_INVALID", "spooled bytes differ from relay identity")
            if (
                not manifest.is_absolute()
                or not _same_windows_path(manifest, expected_identity.manifest_path)
                or snapshot_path(manifest).kind != "regular"
            ):
                raise ProbeError("RELAY_IDENTITY_MISMATCH", "relay producer manifest differs")
            matched_source: Path | None = None
            matched_delta: tuple[int, int, str] | None = None
            for source in sources:
                parsed = _parse_delta_identity(relative_path, source, container=container)
                if parsed is not None:
                    if matched_source is not None:
                        raise ProbeError("SOURCE_DELTA_INVALID", "delta source identity is ambiguous")
                    matched_source = source
                    matched_delta = parsed
            if relative_path.startswith(("d/", "legacy_csv_deltas/")):
                if matched_source is None or matched_delta is None:
                    raise ProbeError("SOURCE_DELTA_ORPHAN", "delta has no canonical source")
                expected_parent = _source_delta_key(matched_source)
                start_byte, end_byte, name_prefix = matched_delta
                source_snapshot = snapshot_path(matched_source)
                if source_snapshot.kind != "regular" or source_snapshot.size < end_byte:
                    raise ProbeError("SOURCE_DELTA_INVALID", "delta range exceeds source")
                try:
                    with matched_source.open("rb") as source_handle:
                        header = source_handle.readline()
                        data_start = source_handle.tell()
                        if start_byte and start_byte < data_start:
                            raise ProbeError("SOURCE_DELTA_INVALID", "delta starts inside CSV header")
                        source_handle.seek(start_byte)
                        body = source_handle.read(end_byte - start_byte)
                except OSError as exc:
                    raise ProbeError("FILE_READ_ERROR", repr(exc)) from exc
                expected_delta = body if start_byte == 0 else header + body
                expected_delta_hash = hashlib.sha256(expected_delta).hexdigest()
                if (
                    not source_path.is_absolute()
                    or not _is_within(source_path, spool_root / "_scan_delta_inputs")
                    or source_path.parent.name != expected_parent
                    or source_path.name != Path(relative_path).name
                    or name_prefix != content_hash[:16]
                    or len(body) != end_byte - start_byte
                    or expected_delta_hash != content_hash
                    or snapshot_path(source_path).sha256 != content_hash
                ):
                    raise ProbeError("SOURCE_DELTA_INVALID", "delta artifact identity differs")
                ranges[_windows_key(matched_source)].append(
                    (start_byte, end_byte)
                )
            elif (
                not source_path.is_absolute()
                or (
                    not _is_within(source_path, spool_root)
                    and _windows_key(source_path) not in canonical_source_keys
                )
            ):
                raise ProbeError("RELAY_ARTIFACT_INVALID", "source path is outside canonical roots")
            if status == "leased":
                if not str(row["lease_owner"] or "").strip() or not str(
                    row["lease_expires_at"] or ""
                ).strip():
                    raise ProbeError("RELAY_ROW_INVALID", "leased relay lacks lease coordinates")
            elif row["lease_owner"] not in (None, "") or row["lease_expires_at"] not in (None, ""):
                raise ProbeError("RELAY_ROW_INVALID", "non-leased relay retains lease coordinates")
            if status == "acked":
                try:
                    receipt = strict_json_bytes(str(row["receipt_json"] or "").encode("utf-8"))
                except ProbeError as exc:
                    raise ProbeError("RELAY_RECEIPT_INVALID", exc.detail) from exc
                if not isinstance(receipt, Mapping):
                    raise ProbeError("RELAY_RECEIPT_INVALID", "receipt is not an object")
                expected_server_id = (
                    f"{metadata['source_host_id']}/{producer_role}/{stream_name}/{relative_path}"
                )
                totals = receipt.get("totals")
                if (
                    receipt.get("committed") is not True
                    or receipt.get("status") != "accepted"
                    or receipt.get("retryable") is not False
                    or receipt.get("next_retry_after") is not None
                    or receipt.get("error") is not None
                    or receipt.get("client_batch_id") != relay_id
                    or receipt.get("server_source_file_id") != expected_server_id
                    or not isinstance(totals, Mapping)
                    or any(
                        isinstance(totals.get(key), bool)
                        or not isinstance(totals.get(key), int)
                        or int(totals[key]) < 0
                        for key in ("inserted", "replayed", "quarantined", "errors")
                    )
                    or sum(int(totals[key]) for key in ("inserted", "replayed", "quarantined", "errors"))
                    != int(metadata["row_count"])
                    or int(totals["errors"]) != 0
                    or int(totals["quarantined"]) != 0
                ):
                    raise ProbeError("RELAY_RECEIPT_INVALID", "ACKED receipt is not accepted")
                status_path = Path(str(row["upload_status_path"] or ""))
                if not status_path.is_absolute() or not _is_within(status_path, status_root):
                    raise ProbeError("RELAY_STATUS_INVALID", "upload status path is outside canonical root")
                known_status.add(_windows_key(status_path))
                status_value = _read_stable_json(status_path)
                if (
                    not isinstance(status_value, Mapping)
                    or status_value.get("success") is not True
                    or status_value.get("committed") is not True
                    or status_value.get("retryable") is not False
                    or status_value.get("receipt") != receipt
                    or str(status_value.get("source_file_path") or "") != str(source_path)
                    or not isinstance(status_value.get("metadata"), Mapping)
                ):
                    raise ProbeError("RELAY_STATUS_INVALID", "ACKED upload status does not correlate")
                status_metadata = status_value["metadata"]
                for key in required_metadata - {"row_count"}:
                    if key in {"runtime_request_token"}:
                        continue
                    if status_metadata.get(key) != metadata.get(key):
                        raise ProbeError("RELAY_STATUS_INVALID", "upload status metadata differs")
        for artifact in _safe_regular_tree(spool_root):
            if _windows_key(artifact) not in known_spool:
                raise ProbeError("RELAY_ARTIFACT_ORPHAN", "unreferenced spool artifact exists")
        for artifact in _safe_regular_tree(status_root):
            lowered = artifact.name.casefold()
            if (
                lowered.startswith("direct_sync_upload_status_")
                or lowered.endswith((".tmp", ".lock", ".csv"))
                or "tmp-" in lowered
            ) and _windows_key(artifact) not in known_status:
                raise ProbeError("RELAY_ARTIFACT_ORPHAN", "unreferenced status artifact exists")
        for artifact in _safe_regular_children(direct_sync_root):
            lowered = artifact.name.casefold()
            if (
                lowered.endswith((".csv", ".lock", ".tmp", ".temp", ".spool", ".delta"))
                or lowered.startswith("direct_sync_upload_status_")
                or any(marker in lowered for marker in (".tmp-", ".bad-", ".migrate-"))
            ):
                raise ProbeError("RELAY_ARTIFACT_ORPHAN", "unmodelled direct-sync artifact exists")
        for source_key, source_ranges in ranges.items():
            ordered = sorted(set(source_ranges))
            if len(ordered) != len(source_ranges):
                raise ProbeError("SOURCE_DELTA_DISCONTINUITY", source_key)
            for previous, current in zip(ordered, ordered[1:]):
                if previous[1] != current[0]:
                    raise ProbeError("SOURCE_DELTA_DISCONTINUITY", source_key)
            ranges[source_key] = ordered
        return ranges, len(rows)
    except ProbeError:
        raise
    except sqlite3.Error as exc:
        raise ProbeError("SQLITE_OBSERVATION_FAILED", repr(exc)) from exc
    finally:
        connection.close()


def _canonical_plan() -> SqlitePlan:
    relay_required = (
        "status",
        "source_file_path",
        "spooled_file_path",
        "producer_manifest_path",
        "relative_path",
        "content_sha256",
        "byte_length",
        "lease_owner",
        "lease_expires_at",
        "receipt_json",
        "upload_status_path",
        "metadata_json",
        "producer_id",
        "key_id",
        "endpoint_url",
        "runtime_fencing_policy",
    )
    authority_required = (
        "status",
        "assigned_relay_id",
        "pending_request_json",
        "pending_issue_idempotency_key",
    )
    return SqlitePlan(
        queries=(
            QuerySpec(
                "relay_unacked_batch",
                "active_work_count",
                "direct_sync_relay_batches",
                ("relay_id",),
                "status<>'acked'",
                relay_required,
            ),
            QuerySpec(
                "relay_unacked_batch",
                "active_lease_count",
                "direct_sync_relay_batches",
                ("relay_id",),
                "status='leased'",
                relay_required,
            ),
            QuerySpec(
                "relay_unacked_batch",
                "pending_commit_count",
                "direct_sync_relay_batches",
                ("relay_id",),
                "status IN ('pending','leased','retry_wait')",
                relay_required,
            ),
            QuerySpec(
                "runtime_authority_unresolved",
                "active_work_count",
                "direct_sync_runtime_authority",
                ("authority_scope",),
                "status='OPERATOR_REVIEW' OR assigned_relay_id IS NOT NULL "
                "OR pending_request_json IS NOT NULL "
                "OR pending_issue_idempotency_key IS NOT NULL",
                authority_required,
            ),
            QuerySpec(
                "runtime_authority_unresolved",
                "pending_commit_count",
                "direct_sync_runtime_authority",
                ("authority_scope",),
                "assigned_relay_id IS NOT NULL OR pending_request_json IS NOT NULL "
                "OR pending_issue_idempotency_key IS NOT NULL",
                authority_required,
            ),
        ),
        status_domains=(
            StatusDomain("direct_sync_relay_batches", "status", _RELAY_STATUSES),
            StatusDomain(
                "direct_sync_relay_batches",
                "runtime_fencing_policy",
                frozenset({"runtime_required", "legacy_exact_replay"}),
            ),
            StatusDomain("direct_sync_runtime_authority", "status", _RUNTIME_STATUSES),
        ),
        allowed_schema_versions=frozenset({0}),
    )


_COMMAND_TERMINAL = (
    "(status='ACKED' AND local_completion_committed=1) OR "
    "(status='CONFLICT' AND local_completion_committed=0 "
    " AND local_recovery_dismissed=1 "
    " AND UPPER(TRIM(COALESCE(last_error_code,'')))="
    "'PHS_WORK_GROUP_COMMAND_CONFLICT' "
    " AND TRIM(COALESCE(receipt_json,''))='') OR "
    "(status='CONFLICT' AND local_completion_committed=1 AND EXISTS ("
    " SELECT 1 FROM package_post_review_outbox p"
    " WHERE p.package_idempotency_key=package_command_outbox.idempotency_key"
    " AND p.local_csv_committed=1))"
)


def _business_plan() -> SqlitePlan:
    return SqlitePlan(
        queries=(
            QuerySpec(
                "label_package_pending",
                "active_work_count",
                "package_command_outbox",
                ("idempotency_key",),
                f"NOT ({_COMMAND_TERMINAL})",
                (
                    "status",
                    "local_completion_committed",
                    "local_recovery_dismissed",
                    "last_error_code",
                    "receipt_json",
                ),
            ),
            QuerySpec(
                "label_package_pending",
                "active_lease_count",
                "package_command_outbox",
                ("idempotency_key",),
                "status='SENDING'",
                ("status",),
            ),
            QuerySpec(
                "label_package_pending",
                "pending_commit_count",
                "package_command_outbox",
                ("idempotency_key",),
                f"NOT ({_COMMAND_TERMINAL})",
                ("status", "local_completion_committed"),
            ),
            QuerySpec(
                "label_package_pending",
                "active_work_count",
                "package_cancellation_outbox",
                ("idempotency_key",),
                "NOT (status='ACKED' AND local_event_committed=1)",
                ("status", "local_event_committed"),
            ),
            QuerySpec(
                "label_package_pending",
                "active_lease_count",
                "package_cancellation_outbox",
                ("idempotency_key",),
                "status='SENDING'",
                ("status",),
            ),
            QuerySpec(
                "label_package_pending",
                "pending_commit_count",
                "package_cancellation_outbox",
                ("idempotency_key",),
                "status IN ('DEFERRED','PENDING','SENDING','CONFLICT') "
                "OR local_event_committed=0",
                ("status", "local_event_committed"),
            ),
            QuerySpec(
                "label_projection_pending",
                "pending_commit_count",
                "package_replacement_waiting_outbox",
                ("dedupe_key",),
                "local_csv_committed=0",
                ("local_csv_committed",),
            ),
            QuerySpec(
                "label_projection_pending",
                "pending_commit_count",
                "package_post_review_outbox",
                ("review_event_id",),
                "local_csv_committed=0",
                ("package_idempotency_key", "local_csv_committed"),
            ),
            QuerySpec(
                "label_operation_pending",
                "active_lease_count",
                "package_operation_leases",
                ("lease_id",),
                "status IN ('PREFETCHED','LOCAL_COMPLETED','OPERATOR_REVIEW')",
                ("status",),
            ),
            QuerySpec(
                "label_operation_pending",
                "active_work_count",
                "package_operation_leases",
                ("lease_id",),
                "status IN ('PREFETCHED','LOCAL_COMPLETED','OPERATOR_REVIEW')",
                ("status",),
            ),
            QuerySpec(
                "label_operation_pending",
                "active_lease_count",
                "package_operation_lease_issue_attempts",
                ("attempt_id",),
                "status='ACTIVE'",
                ("status", "lease_id"),
            ),
            QuerySpec(
                "label_exchange_pending",
                "active_work_count",
                "sealed_transfer_exchange_intents",
                ("intent_id",),
                "status IN ('PREPARED','COMMAND_READY','RETRY_WAIT','OPERATOR_REVIEW') "
                "OR (status='ACKED' AND (seal_verification_status<>'VERIFIED' "
                "OR local_apply_status<>'APPLIED'))",
                (
                    "schema_version",
                    "status",
                    "seal_verification_status",
                    "local_apply_status",
                ),
            ),
        ),
        status_domains=(
            StatusDomain("package_command_outbox", "status", _PACKAGE_STATUSES),
            StatusDomain("package_cancellation_outbox", "status", _CANCELLATION_STATUSES),
            StatusDomain("package_operation_leases", "status", _LEASE_STATUSES),
            StatusDomain(
                "package_operation_lease_issue_attempts",
                "status",
                _ISSUE_ATTEMPT_STATUSES,
            ),
            StatusDomain("sealed_transfer_exchange_intents", "status", _EXCHANGE_STATUSES),
            StatusDomain(
                "sealed_transfer_exchange_intents",
                "seal_verification_status",
                _SEAL_STATUSES,
            ),
            StatusDomain(
                "sealed_transfer_exchange_intents",
                "local_apply_status",
                _LOCAL_APPLY_STATUSES,
            ),
            StatusDomain(
                "sealed_transfer_exchange_intents",
                "schema_version",
                frozenset({SEALED_EXCHANGE_SCHEMA_VERSION}),
            ),
        ),
        allowed_schema_versions=frozenset({0}),
    )


@dataclass(frozen=True)
class LabelMatchTestContext:
    """Explicit filesystem injection used only by adapter unit tests."""

    data_root: Path
    direct_sync_root: Path
    profile_path: Path
    alternate_roots: tuple[Path, ...] = ()
    install_root: Path | None = None
    legacy_profile_paths: tuple[Path, ...] = ()


@dataclass(frozen=True)
class LabelMatchAdapter:
    data_root: Path
    direct_sync_root: Path
    profile_path: Path
    install_root: Path
    alternate_roots: tuple[Path, ...] = ()
    legacy_profile_paths: tuple[Path, ...] = ()
    require_settings: bool = False

    app = APP
    app_id = APP_ID
    database_identity_path = DATABASE_IDENTITY_PATH
    profile_identity_path = PROFILE_IDENTITY_PATH

    @classmethod
    def from_trusted_target(
        cls,
        target_pc: str = "TEST1",
        *,
        roots: TrustedRoots | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> "LabelMatchAdapter":
        if target_pc != "TEST1":
            raise ProbeError("TARGET_PC_UNSUPPORTED", "Label_Match target is not mapped")
        if roots is None and environ is not None:
            raise ProbeError("ROOT_UNTRUSTED", "production environment cannot be injected")
        trusted = roots or production_roots(target_pc)
        if trusted.target_pc != target_pc:
            raise ProbeError("ROOT_AMBIGUOUS", "injected roots target differs")
        env = dict(os.environ if roots is None else (environ or {}))
        canonical_program_data = trusted.program_data
        observed_program_data = str(
            env.get("PROGRAMDATA", env.get("ProgramData", "")) or ""
        ).strip()
        if observed_program_data and not _same_windows_path(observed_program_data, canonical_program_data):
            raise ProbeError("ROOT_AMBIGUOUS", "ProgramData differs from contract root")
        data_root = canonical_program_data / "KMTech" / "Label_Match" / "data"
        override = str(env.get("LABEL_MATCH_SAVE_DIR", "") or "").strip()
        if override and not _same_windows_path(override, data_root):
            raise ProbeError("ROOT_AMBIGUOUS", "LABEL_MATCH_SAVE_DIR is alternate state")
        direct_sync_root = canonical_program_data / "KMTech" / "DirectSync" / "label_match"
        direct_override = str(
            env.get("LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT", "") or ""
        ).strip()
        if direct_override and not _same_windows_path(direct_override, direct_sync_root):
            raise ProbeError("ROOT_AMBIGUOUS", "direct-sync root override is active")
        profile = (
            canonical_program_data
            / "KMTech"
            / "Logistics"
            / "profiles"
            / "Label_Match"
            / "runtime-profile.json"
        )
        install_root = trusted.apps_root / "Label_Match" / "current"
        legacy_profile = canonical_program_data / "KMTech" / "Logistics" / "runtime-profile.json"
        return cls(
            data_root,
            direct_sync_root,
            profile,
            install_root,
            legacy_profile_paths=(legacy_profile,),
            require_settings=True,
        )

    @classmethod
    def from_test_context(cls, context: LabelMatchTestContext) -> "LabelMatchAdapter":
        data_root = _absolute(context.data_root)
        direct_root = _absolute(context.direct_sync_root)
        profile = _absolute(context.profile_path)
        install_root = _absolute(
            context.install_root
            if context.install_root is not None
            else context.profile_path.parent.parent.parent / "apps" / "Label_Match" / "current"
        )
        alternates = tuple(_absolute(path) for path in context.alternate_roots)
        legacy_profiles = tuple(_absolute(path) for path in context.legacy_profile_paths)
        protected = {
            _windows_key(data_root),
            _windows_key(direct_root),
            _windows_key(profile),
            _windows_key(install_root),
        }
        if any(_windows_key(path) in protected for path in alternates):
            raise ProbeError("ROOT_AMBIGUOUS", "alternate root aliases a trusted root")
        return cls(
            data_root,
            direct_root,
            profile,
            install_root,
            alternates,
            legacy_profiles,
            False,
        )

    @property
    def database_path(self) -> Path:
        return self.direct_sync_root / "queue" / "direct_sync_relay.sqlite3"

    @property
    def producer_manifest_path(self) -> Path:
        return self.direct_sync_root / "producer_manifest.json"

    @property
    def registration_report_path(self) -> Path:
        return self.status_root / "label_match_worker_pc_registration.json"

    @property
    def current_state_path(self) -> Path:
        return self.data_root / "_current_set_state_packaging.json"

    @property
    def business_database_path(self) -> Path:
        return self.data_root / "package_logistics_outbox.sqlite3"

    @property
    def phs_journal_path(self) -> Path:
        return (
            self.data_root
            / "phs_label_exchange"
            / "phs_label_exchange_recovery.json"
        )

    @property
    def settings_paths(self) -> tuple[Path, Path]:
        return (
            self.install_root / "_internal" / "config" / "app_settings.json",
            self.install_root / "config" / "app_settings.json",
        )

    @property
    def spool_root(self) -> Path:
        return self.direct_sync_root / "spool"

    @property
    def status_root(self) -> Path:
        return self.direct_sync_root / "status"

    def _source_files(self) -> tuple[Path, ...]:
        return tuple(
            path
            for path in _safe_regular_children(self.data_root)
            if path.name.startswith(_SOURCE_PREFIX) and path.suffix.casefold() == ".csv"
        )

    def resource_paths(self) -> Sequence[Path]:
        sources = self._source_files()
        resources = [
            self.data_root,
            self.direct_sync_root,
            self.spool_root,
            self.status_root,
            self.producer_manifest_path,
            self.registration_report_path,
            self.current_state_path,
            self.business_database_path,
            self.phs_journal_path,
            self.profile_path,
            *self.legacy_profile_paths,
            *(path.parent for path in self.settings_paths),
            *self.settings_paths,
            self.database_path,
            *self.alternate_roots,
            *sources,
        ]
        resources.extend(Path(os.fspath(path) + ".lock") for path in sources)
        return tuple(resources)

    def sqlite_paths(self) -> Sequence[Path]:
        return (self.database_path, self.business_database_path)

    def _reject_ambiguous_or_temporary_state(self, session: ObservationSession) -> None:
        for root in self.alternate_roots:
            if session.snapshot(root).exists:
                raise ProbeError("ROOT_AMBIGUOUS", "alternate Label_Match root contains state")
        sources = {_windows_key(path): path for path in self._source_files()}
        current = self.current_state_path.name.casefold()
        for path in _safe_regular_children(self.data_root):
            name = path.name.casefold()
            if name.startswith(current) and name != current:
                raise ProbeError("TEMPORARY_STATE_PRESENT", "unrecognized current-set artifact exists")
            if path.suffix.casefold() == ".csv" and _windows_key(path) not in sources:
                raise ProbeError("SOURCE_ARTIFACT_UNKNOWN", "unrecognized source CSV exists")
            if name.endswith(".lock"):
                source_key = _windows_key(Path(os.fspath(path)[: -len(".lock")]))
                if source_key not in sources:
                    raise ProbeError("SOURCE_ARTIFACT_UNKNOWN", "orphan source lock exists")
            if name.endswith((".tmp", ".temp", ".spool", ".delta")) or any(
                marker in name for marker in (".tmp-", ".bad-", ".migrate-")
            ):
                raise ProbeError("TEMPORARY_STATE_PRESENT", "temporary source state exists")
        journal_parent = self.phs_journal_path.parent
        prefix = self.phs_journal_path.name.casefold()
        for path in _safe_regular_children(journal_parent):
            if path.name.casefold() != prefix and path.name.casefold().startswith(prefix):
                raise ProbeError("TEMPORARY_STATE_PRESENT", "PHS journal temp state exists")

    def _validate_settings_root(self, session: ObservationSession) -> None:
        observed = 0
        roots: set[str] = set()
        for path in self.settings_paths:
            if not session.snapshot(path).exists:
                continue
            observed += 1
            value = session.read_json(path, required=True)
            if not isinstance(value, Mapping):
                raise ProbeError("ROOT_AMBIGUOUS", "Label_Match settings are invalid")
            configured = value.get("custom_save_path", "")
            if not isinstance(configured, str):
                raise ProbeError("ROOT_AMBIGUOUS", "custom_save_path has invalid type")
            selected = configured.strip() or os.fspath(self.data_root)
            if not _same_windows_path(selected, self.data_root):
                raise ProbeError("ROOT_AMBIGUOUS", "custom_save_path selects alternate state")
            roots.add(_windows_key(selected))
        if self.require_settings and observed == 0:
            raise ProbeError("ROOT_UNTRUSTED", "persisted Label_Match settings are absent")
        if len(roots) > 1:
            raise ProbeError("ROOT_AMBIGUOUS", "persisted Label_Match roots differ")
        for settings_path in self.settings_paths:
            canonical_name = settings_path.name.casefold()
            for child in _safe_regular_children(settings_path.parent):
                name = child.name.casefold()
                if name.startswith(canonical_name) and name != canonical_name:
                    raise ProbeError("ROOT_AMBIGUOUS", "unrecognized settings artifact exists")

    def _observe_current_state(
        self,
        session: ObservationSession,
        observation: Observation,
    ) -> None:
        value = session.read_json(self.current_state_path)
        if value is None:
            return
        if not isinstance(value, dict) or not isinstance(value.get("current_set_info"), dict):
            raise ProbeError("CURRENT_STATE_INVALID", "current set wrapper is invalid")
        current = value["current_set_info"]
        raw = current.get("raw")
        if not isinstance(raw, list) or not raw:
            raise ProbeError("CURRENT_STATE_INVALID", "current set has no raw scans")
        identity = str(current.get("id") or self.current_state_path.name)
        observation.add("session_recovery_active", "active_session_count", identity)
        observation.add("session_recovery_active", "active_work_count", identity)

    def _observe_phs_journal(
        self,
        session: ObservationSession,
        observation: Observation,
    ) -> None:
        value = session.read_json(self.phs_journal_path)
        if value is None:
            return
        if (
            not isinstance(value, dict)
            or value.get("schema_version") != PHS_JOURNAL_SCHEMA_VERSION
            or not isinstance(value.get("state"), dict)
        ):
            raise ProbeError("PHS_JOURNAL_INVALID", "PHS journal wrapper is invalid")
        state = value["state"]
        status = str(state.get("status") or "").strip().upper()
        if not status:
            raise ProbeError("PHS_JOURNAL_INVALID", "PHS journal status is absent")
        if status not in PHS_TERMINAL_STATUSES:
            identity = str(state.get("set_id") or self.phs_journal_path.name)
            observation.add("label_exchange_pending", "active_work_count", identity)
            observation.add("label_exchange_pending", "pending_commit_count", identity)

    def _observe_source_coverage(
        self,
        session: ObservationSession,
        observation: Observation,
        delta_ranges: Mapping[str, Sequence[tuple[int, int]]],
    ) -> None:
        sources = self._source_files()
        connection = _immutable_connection(self.database_path)
        try:
            _require_columns(
                connection,
                "direct_sync_source_scan_state",
                {"source_file_path", "sent_byte_count", "sent_prefix_sha256"},
            )
            rows = connection.execute(
                "SELECT source_file_path,sent_byte_count,sent_prefix_sha256 "
                "FROM direct_sync_source_scan_state"
            ).fetchall()
            states: dict[str, sqlite3.Row] = {}
            for row in rows:
                key = _windows_key(str(row["source_file_path"] or ""))
                if not key or key in states:
                    raise ProbeError("SOURCE_SCAN_STATE_INVALID", "duplicate source state")
                states[key] = row
            source_keys = {_windows_key(source) for source in sources}
            if set(states) - source_keys:
                raise ProbeError("SOURCE_SCAN_STATE_ORPHAN", "scan state has no canonical source")
            for source in sources:
                snapshot = session.snapshot(source)
                lock_path = Path(os.fspath(source) + ".lock")
                if session.snapshot(lock_path).exists:
                    observation.add("source_sync_pending", "active_work_count", source.name)
                    observation.add("source_sync_pending", "pending_commit_count", source.name)
                row = states.get(_windows_key(source))
                if row is None:
                    observation.add("source_sync_pending", "active_work_count", source.name)
                    observation.add("source_sync_pending", "pending_commit_count", source.name)
                    continue
                try:
                    sent = int(row["sent_byte_count"])
                except (TypeError, ValueError) as exc:
                    raise ProbeError("SOURCE_SCAN_STATE_INVALID", repr(exc)) from exc
                prefix_hash = str(row["sent_prefix_sha256"] or "")
                if sent < 0 or sent > snapshot.size or not _HEX_64_RE.fullmatch(prefix_hash):
                    raise ProbeError("SOURCE_SCAN_STATE_INVALID", "source scan bounds are invalid")
                digest = hashlib.sha256()
                remaining = sent
                try:
                    with source.open("rb") as handle:
                        while remaining:
                            chunk = handle.read(min(1024 * 1024, remaining))
                            if not chunk:
                                break
                            digest.update(chunk)
                            remaining -= len(chunk)
                except OSError as exc:
                    raise ProbeError("FILE_READ_ERROR", repr(exc)) from exc
                if remaining or digest.hexdigest() != prefix_hash:
                    raise ProbeError("SOURCE_SCAN_STATE_INVALID", "source prefix hash changed")
                ranges = delta_ranges.get(_windows_key(source), ())
                if ranges and ranges[-1][1] > sent:
                    raise ProbeError("SOURCE_DELTA_DISCONTINUITY", "delta exceeds durable source state")
                if sent < snapshot.size:
                    observation.add("source_sync_pending", "active_work_count", source.name)
                    observation.add("source_sync_pending", "pending_commit_count", source.name)
        except ProbeError:
            raise
        except sqlite3.Error as exc:
            raise ProbeError("SQLITE_OBSERVATION_FAILED", repr(exc)) from exc
        finally:
            connection.close()

    def observe(
        self,
        session: ObservationSession,
        observation: Observation,
    ) -> DatabaseObservation:
        self._reject_ambiguous_or_temporary_state(session)
        self._validate_settings_root(session)
        runtime_profile = _validate_app_profile(
            session,
            self.profile_path,
            self.legacy_profile_paths,
            app=APP,
        )
        producer_identity = _validate_producer_registration(
            session,
            runtime_profile,
            self.producer_manifest_path,
            self.registration_report_path,
            manifest_app="LabelMatch",
            report_version="label-match-worker-pc-registration-v1",
            report_manifest_path_field="manifest_path",
            report_app="LabelMatch",
            producer_role="label_match",
            stream_name="label_match_events",
            source_system="label_match",
            source_transport="legacy_packaging_csv",
            required_report_flags=(),
        )
        database = session.observe_sqlite(
            self.database_path,
            _canonical_plan(),
            observation,
            required=True,
        )
        assert database is not None
        sources = self._source_files()
        delta_ranges, relay_row_count = _validate_relay_artifacts(
            self.database_path,
            self.direct_sync_root,
            sources,
            producer_identity,
            producer_role="label_match",
            stream_name="label_match_events",
            source_system="label_match",
            source_transport="legacy_packaging_csv",
            container=False,
        )
        _validate_runtime_authority(
            self.database_path,
            producer_identity,
            require_row=relay_row_count > 0,
        )
        self._observe_current_state(session, observation)
        self._observe_phs_journal(session, observation)
        self._observe_source_coverage(session, observation, delta_ranges)
        if session.snapshot(self.business_database_path).exists:
            _validate_outbox_schema(self.business_database_path)
            session.observe_sqlite(
                self.business_database_path,
                _business_plan(),
                observation,
                required=True,
            )
        return database


def create_adapter(
    app: str,
    target_pc: str,
    *,
    roots: TrustedRoots | None = None,
) -> LabelMatchAdapter:
    if app != APP:
        raise ProbeError("APP_UNSUPPORTED", "Label_Match adapter app is invalid")
    return LabelMatchAdapter.from_trusted_target(target_pc, roots=roots)


__all__ = [
    "BLOCKER_KIND_CATALOG",
    "LabelMatchAdapter",
    "LabelMatchTestContext",
    "create_adapter",
]

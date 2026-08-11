"""Shared declarative pieces for active-work probe adapters."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat as stat_module
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import quote, urlsplit

from ..core import (
    DatabaseObservation,
    Observation,
    ObservationSession,
    ProbeError,
    QuerySpec,
    SqlitePlan,
    StatusDomain,
    strict_json_bytes,
)


RUNTIME_PROFILE_CONTRACT = "km-logistics-runtime-profile-v1"
RELAY_STATUSES = frozenset(
    {"pending", "leased", "retry_wait", "acked", "failed_permanent", "operator_review"}
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_RELAY_CLIENT_BATCH_ID_RE = re.compile(r"^relay-[0-9a-f]{32}$")
_TEMP_MARKERS = (".tmp", ".temp", ".part", ".partial", ".incomplete")
_DEFECT_REDACTED_KEYS = frozenset(
    {
        "authorization",
        "canonical_request",
        "hmac_key",
        "hmac_key_hex",
        "key_secret",
        "producer_signature",
        "producer_secret",
        "raw_payload",
        "raw_secret",
        "receipt_json",
        "runtime_request_token",
        "secret",
        "secret_hex",
        "source_file_bytes",
        "source_file_text",
        "next_request_token",
        "x-producer-signature",
    }
)
RELAY_DYNAMIC_BLOCKER_KINDS = (
    "relay_orphan_delta",
    "relay_orphan_spool",
    "relay_orphan_status",
    "relay_source_unsynced",
    "relay_source_unsynced_commit",
    "relay_source_writer",
    "relay_source_writer_commit",
    "relay_temporary_artifact",
)


@dataclass(frozen=True)
class RelayResources:
    """Canonical paths needed to prove one relay has no retained work."""

    database_path: Path
    producer_manifest_path: Path
    source_root: Path
    spool_root: Path
    upload_status_root: Path
    runtime_status_path: Path
    source_suffix: str
    source_marker: str
    expected_pc_id: str
    expected_producer_role: str
    expected_stream_name: str
    expected_source_system: str
    expected_source_transport: str
    report_root: Path | None = None

    def _regular_descendants(self, root: Path) -> tuple[Path, ...]:
        if not root.is_dir():
            return ()
        files: list[Path] = []
        pending: list[tuple[Path, int]] = [(root, 0)]
        while pending:
            current, depth = pending.pop()
            if depth > 8 or len(files) > 4096:
                raise ProbeError("RESOURCE_DIRECTORY_UNBOUNDED", "relay directory exceeded")
            try:
                entries = sorted(os.scandir(current), key=lambda item: item.name.casefold())
            except OSError as exc:
                raise ProbeError("RESOURCE_SNAPSHOT_ERROR", repr(exc)) from exc
            for entry in entries:
                path = current / entry.name
                try:
                    entry_stat = os.lstat(path)
                except OSError as exc:
                    raise ProbeError("RESOURCE_SNAPSHOT_ERROR", repr(exc)) from exc
                attributes = int(getattr(entry_stat, "st_file_attributes", 0) or 0)
                reparse_flag = int(
                    getattr(stat_module, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
                )
                if entry.is_symlink() or attributes & reparse_flag:
                    raise ProbeError("RESOURCE_REPARSE_POINT", "relay reparse entry is forbidden")
                if stat_module.S_ISDIR(entry_stat.st_mode):
                    pending.append((path, depth + 1))
                elif stat_module.S_ISREG(entry_stat.st_mode):
                    files.append(path)
                else:
                    raise ProbeError("RESOURCE_TYPE_INVALID", "relay resource is not regular")
        return tuple(sorted(files, key=lambda path: os.fspath(path).casefold()))

    def source_files(self) -> tuple[Path, ...]:
        return tuple(
            path
            for path in self._regular_descendants(self.source_root)
            if path.name.casefold().endswith(self.source_suffix.casefold())
            and self.source_marker.casefold() in path.stem.casefold()
        )

    def resource_paths(self) -> tuple[Path, ...]:
        roots = [
            self.database_path,
            self.producer_manifest_path,
            self.source_root,
            self.spool_root,
            self.upload_status_root,
            self.runtime_status_path,
        ]
        if self.report_root is not None:
            roots.append(self.report_root)
        descendants: list[Path] = []
        for root in (
            self.source_root,
            self.spool_root,
            self.upload_status_root,
            self.report_root,
        ):
            if root is not None:
                descendants.extend(self._regular_descendants(root))
        runtime_parent = self.runtime_status_path.parent
        if runtime_parent.is_dir():
            descendants.extend(self._regular_descendants(runtime_parent))
        return tuple(dict.fromkeys((*roots, *descendants)))


@dataclass(frozen=True)
class TrustedRoots:
    target_pc: str
    program_data: Path
    local_app_data: Path
    apps_root: Path


def production_roots(target_pc: str) -> TrustedRoots:
    """Return contract-owned native paths without trusting process environment."""

    if os.name != "nt":
        raise ProbeError("UNSUPPORTED_PLATFORM", "factory probe requires native Windows")
    if target_pc == "TEST1":
        local_app_data = Path(r"C:\Users\Worker_1\AppData\Local")
    elif target_pc == "INSPECTIONADMIN":
        local_app_data = Path("C:\\Users\\관리자\\AppData\\Local")
    else:
        raise ProbeError("TARGET_PC_UNSUPPORTED", "target has no trusted LocalAppData mapping")
    return TrustedRoots(
        target_pc=target_pc,
        program_data=Path(r"C:\ProgramData"),
        local_app_data=local_app_data,
        apps_root=Path(r"C:\KMTech\Apps"),
    )


def ensure_no_path_override(variable: str, expected: Path) -> None:
    value = str(os.environ.get(variable) or "").strip()
    if not value:
        return
    try:
        actual = os.path.normcase(os.path.abspath(value))
        canonical = os.path.normcase(os.path.abspath(os.fspath(expected)))
    except (OSError, ValueError) as exc:
        raise ProbeError("CONFIG_AMBIGUITY", repr(exc)) from exc
    if actual != canonical:
        raise ProbeError("CONFIG_AMBIGUITY", f"{variable} selects an alternate path")


def validate_profile(
    session: ObservationSession,
    canonical_path: Path,
    legacy_paths: Sequence[Path],
    *,
    allow_identical_legacy: bool = False,
) -> Mapping[str, Any]:
    ensure_no_path_override("KM_LOGISTICS_PROFILE_PATH", canonical_path)
    canonical_snapshot = session.require_regular_file(
        canonical_path,
        code="PROFILE_ABSENT",
    )
    for legacy_path in legacy_paths:
        legacy_snapshot = session.snapshot(legacy_path)
        if not legacy_snapshot.exists:
            continue
        if (
            not allow_identical_legacy
            or legacy_snapshot.kind != "regular"
            or legacy_snapshot.sha256 != canonical_snapshot.sha256
        ):
            raise ProbeError("CONFIG_AMBIGUITY", "legacy profile identity differs")
    value = session.read_json(canonical_path, required=True)
    if not isinstance(value, Mapping):
        raise ProbeError("PROFILE_INVALID", "runtime profile is not an object")
    if value.get("contract_version") != RUNTIME_PROFILE_CONTRACT:
        raise ProbeError("PROFILE_INVALID", "runtime profile contract is unsupported")
    if "bearer_token" in value or "token" in value:
        raise ProbeError("PROFILE_INVALID", "plaintext credential field is forbidden")
    required_strings = (
        "base_url",
        "authority_scope",
        "authority_plane",
        "ledger_plane",
        "device_id",
        "source_host_id",
    )
    if any(not isinstance(value.get(key), str) or not str(value[key]).strip() for key in required_strings):
        raise ProbeError("PROFILE_INVALID", "runtime profile identity fields are invalid")
    try:
        parsed = urlsplit(str(value["base_url"]))
    except ValueError as exc:
        raise ProbeError("PROFILE_INVALID", repr(exc)) from exc
    if (
        parsed.scheme.lower() != "https"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ProbeError("PROFILE_INVALID", "runtime profile HTTPS origin is invalid")
    if str(value["authority_plane"]).upper() not in {"AUTHORITATIVE", "SHADOW_CANDIDATE"}:
        raise ProbeError("PROFILE_INVALID", "authority plane is invalid")
    if str(value["ledger_plane"]).upper() not in {"AUTHORITATIVE", "SHADOW_CANDIDATE"}:
        raise ProbeError("PROFILE_INVALID", "ledger plane is invalid")
    for key in ("authority_epoch", "plane_epoch"):
        field = value.get(key)
        if isinstance(field, bool) or not isinstance(field, int) or field < 1:
            raise ProbeError("PROFILE_INVALID", f"{key} is invalid")
    return value


def relay_plan(*, defect: bool) -> SqlitePlan:
    if defect:
        runtime_status_column = "status"
        runtime_pending_column = "pending_request_json"
        runtime_statuses = frozenset({"request_pending", "active", "operator_review"})
        relay_artifact_columns = (
            "status",
            "source_file_path",
            "spooled_file_path",
            "producer_manifest_path",
            "relative_path",
            "content_sha256",
            "byte_length",
            "receipt_json",
            "upload_status_path",
            "metadata_json",
            "producer_id",
            "key_id",
            "endpoint_url",
        )
    else:
        runtime_status_column = "state_status"
        runtime_pending_column = "pending_issue_json"
        runtime_statuses = frozenset({"pending", "active", "operator_review"})
        relay_artifact_columns = (
            "status",
            "source_file_path",
            "spooled_file_path",
            "producer_manifest_path",
            "stream_name",
            "relative_path",
            "content_sha256",
            "byte_length",
            "receipt_json",
            "upload_status_path",
            "verification_report_path",
            "upload_metadata_json",
            "outbox_ids_json",
            "producer_id",
            "key_id",
            "endpoint_url",
        )
    return SqlitePlan(
        queries=(
            QuerySpec(
                "relay_unacked_batch",
                "pending_commit_count",
                "direct_sync_relay_batches",
                ("relay_id",),
                "status<>'acked'",
                relay_artifact_columns,
            ),
            QuerySpec(
                "runtime_authority_unresolved",
                "active_lease_count",
                "direct_sync_runtime_lease_state",
                ("producer_install_id",),
                f"{runtime_status_column}<>'active' OR "
                f"({runtime_pending_column} IS NOT NULL AND "
                f"TRIM({runtime_pending_column}) NOT IN ('','{{}}','null'))",
                (
                    runtime_status_column,
                    runtime_pending_column,
                    "producer_id",
                    "key_id",
                    "endpoint_url",
                ),
            ),
            QuerySpec(
                "runtime_authority_inflight",
                "active_lease_count",
                "direct_sync_runtime_lease_state",
                ("producer_install_id", "inflight_relay_id"),
                "inflight_relay_id IS NOT NULL AND TRIM(inflight_relay_id)<>''",
                ("inflight_relay_id",),
            ),
        ),
        status_domains=(
            StatusDomain("direct_sync_relay_batches", "status", RELAY_STATUSES),
            StatusDomain(
                "direct_sync_runtime_lease_state",
                runtime_status_column,
                runtime_statuses,
            ),
        ),
        require_schema_info=False,
        allowed_schema_versions=frozenset({0}),
    )


def _path_key(path: str | os.PathLike[str]) -> str:
    return os.path.normcase(os.path.normpath(os.path.abspath(os.fspath(path))))


def _require_under(path_text: str, root: Path, *, code: str) -> Path:
    if not path_text or not Path(path_text).is_absolute():
        raise ProbeError(code, "relay artifact path is not absolute")
    path = Path(os.path.abspath(path_text))
    try:
        path.relative_to(Path(os.path.abspath(os.fspath(root))))
    except ValueError as exc:
        raise ProbeError(code, "relay artifact escaped its canonical root") from exc
    return path


def open_immutable_sqlite(path: Path) -> sqlite3.Connection:
    uri_path = quote(Path(os.path.abspath(os.fspath(path))).as_posix(), safe="/:")
    connection = sqlite3.connect(f"file:{uri_path}?mode=ro&immutable=1", uri=True, timeout=0.0)
    connection.row_factory = sqlite3.Row
    try:
        connection.execute("PRAGMA query_only=ON")
        row = connection.execute("PRAGMA query_only").fetchone()
        if row is None or int(row[0]) != 1:
            raise ProbeError("SQLITE_QUERY_ONLY_FAILED", "query_only did not remain enabled")
        if [str(item[0]) for item in connection.execute("PRAGMA quick_check")] != ["ok"]:
            raise ProbeError("SQLITE_QUICK_CHECK_FAILED", "relay quick_check failed")
    except BaseException:
        connection.close()
        raise
    return connection


def _prefix_sha256(path: Path, length: int) -> str:
    remaining = length
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            while remaining:
                chunk = handle.read(min(1024 * 1024, remaining))
                if not chunk:
                    raise ProbeError("RELAY_SOURCE_CORRELATION_INVALID", "source prefix is short")
                digest.update(chunk)
                remaining -= len(chunk)
    except ProbeError:
        raise
    except OSError as exc:
        raise ProbeError("FILE_READ_ERROR", repr(exc)) from exc
    return digest.hexdigest()


def _has_complete_tail(path: Path, size: int) -> bool:
    if size <= 0:
        return False
    try:
        with path.open("rb") as handle:
            handle.seek(-1, os.SEEK_END)
            return handle.read(1) == b"\n"
    except OSError as exc:
        raise ProbeError("FILE_READ_ERROR", repr(exc)) from exc


def _read_strict_json_file(path: Path) -> Mapping[str, Any]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ProbeError("FILE_READ_ERROR", repr(exc)) from exc
    value = strict_json_bytes(raw)
    if not isinstance(value, Mapping):
        raise ProbeError("RELAY_ARTIFACT_INVALID", "relay JSON artifact is not an object")
    return value


def _terminal_safe_metadata(value: Any, *, defect: bool) -> Any:
    """Reproduce terminal artifact redaction without reading any credential store."""

    if isinstance(value, Mapping):
        source = {str(key): child for key, child in value.items()}
        result: dict[str, Any] = {}
        sensitive = False
        for key, child in source.items():
            normalized = key.strip().casefold()
            if normalized in {"runtime_request_token_sha256", "next_request_token_sha256"}:
                continue
            if normalized in {"runtime_request_token", "next_request_token"}:
                sensitive = True
                expected_marker = (
                    "[redacted-after-terminal-transition]" if defect else "[redacted]"
                )
                if child != expected_marker:
                    raise ProbeError(
                        "RELAY_SENSITIVE_METADATA_PRESENT",
                        "terminal relay metadata contains live authority",
                    )
                hash_name = (
                    "runtime_request_token_sha256"
                    if normalized == "runtime_request_token"
                    else "next_request_token_sha256"
                )
                token_hash = source.get(hash_name)
                if not isinstance(token_hash, str) or not _SHA256_RE.fullmatch(token_hash):
                    raise ProbeError(
                        "RELAY_SENSITIVE_METADATA_INVALID",
                        "terminal authority hash is invalid",
                    )
                result[key] = expected_marker
                result[hash_name] = token_hash
                continue
            if defect and normalized in _DEFECT_REDACTED_KEYS:
                sensitive = True
                continue
            result[key] = _terminal_safe_metadata(child, defect=defect)
        if defect and sensitive:
            result["redacted_sensitive_fields"] = True
        return result
    if isinstance(value, list):
        return [_terminal_safe_metadata(item, defect=defect) for item in value]
    return value


def _is_temporary_name(name: str) -> bool:
    folded = name.casefold()
    return any(marker in folded for marker in _TEMP_MARKERS)


def _add_source_writer(
    observation: Observation,
    identity: str,
) -> None:
    observation.add("relay_source_writer", "active_work_count", identity)
    observation.add("relay_source_writer_commit", "pending_commit_count", identity)


def _add_source_unsynced(
    observation: Observation,
    identity: str,
) -> None:
    observation.add("relay_source_unsynced", "active_work_count", identity)
    observation.add("relay_source_unsynced_commit", "pending_commit_count", identity)


def _relay_rows(
    connection: sqlite3.Connection,
    *,
    defect: bool,
) -> list[sqlite3.Row]:
    variant_columns = (
        ",metadata_json"
        if defect
        else ",stream_name,verification_report_path,upload_metadata_json,outbox_ids_json"
    )
    return list(
        connection.execute(
            "SELECT relay_id,status,source_file_path,spooled_file_path,"
            "producer_manifest_path,relative_path,content_sha256,byte_length,receipt_json,"
            "upload_status_path,producer_id,key_id,endpoint_url"
            f"{variant_columns} FROM direct_sync_relay_batches ORDER BY relay_id"
        )
    )


def _validate_relay_identity(
    connection: sqlite3.Connection,
    resources: RelayResources,
    runtime_profile: Mapping[str, Any],
    rows: Sequence[sqlite3.Row],
    manifest: Mapping[str, Any],
    *,
    defect: bool,
) -> None:
    identity = manifest.get("pc_identity")
    streams = manifest.get("streams")
    if (
        manifest.get("schema_version") != "producer-onboarding-manifest-v1"
        or not isinstance(identity, Mapping)
        or not isinstance(streams, list)
    ):
        raise ProbeError("PRODUCER_MANIFEST_INVALID", "manifest identity or streams are invalid")
    source_host_id = identity.get("source_host_id")
    producer_install_id = identity.get("producer_install_id")
    pc_id = identity.get("pc_id")
    if any(
        not isinstance(value, str) or not value.strip()
        for value in (source_host_id, producer_install_id, pc_id)
    ):
        raise ProbeError("PRODUCER_MANIFEST_INVALID", "manifest identity is incomplete")
    if str(pc_id) != resources.expected_pc_id:
        raise ProbeError("RELAY_IDENTITY_MISMATCH", "manifest and target PC differ")
    if str(source_host_id) != str(runtime_profile["source_host_id"]):
        raise ProbeError("RELAY_IDENTITY_MISMATCH", "manifest and profile source host differ")
    matching_streams = [
        stream
        for stream in streams
        if isinstance(stream, Mapping)
        and stream.get("stream_name") == resources.expected_stream_name
    ]
    if len(matching_streams) != 1:
        raise ProbeError("PRODUCER_MANIFEST_INVALID", "expected stream is not unique")
    stream = matching_streams[0]
    if (
        stream.get("producer_role") != resources.expected_producer_role
        or stream.get("source_system") != resources.expected_source_system
        or stream.get("source_transport") != resources.expected_source_transport
    ):
        raise ProbeError("PRODUCER_MANIFEST_INVALID", "stream identity is invalid")
    canonical_manifest_hash = hashlib.sha256(
        json.dumps(
            manifest,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()

    status_column = "status" if defect else "state_status"
    runtime_rows = list(
        connection.execute(
            "SELECT producer_install_id,producer_id,key_id,endpoint_url,"
            f"{status_column} FROM direct_sync_runtime_lease_state "
            "ORDER BY producer_install_id"
        )
    )
    if len(runtime_rows) != 1:
        raise ProbeError("RELAY_IDENTITY_AMBIGUOUS", "runtime identity row is not unique")
    runtime = runtime_rows[0]
    expected_endpoint = (
        str(runtime_profile["base_url"]).rstrip("/")
        + "/api/producer-ingest/v1/source-file"
    )
    producer_id = str(runtime["producer_id"] or "")
    key_id = str(runtime["key_id"] or "")
    endpoint_url = str(runtime["endpoint_url"] or "")
    if (
        str(runtime["producer_install_id"] or "") != str(producer_install_id)
        or not producer_id
        or not key_id
        or endpoint_url != expected_endpoint
    ):
        raise ProbeError("RELAY_IDENTITY_MISMATCH", "runtime identity is not canonical")

    metadata_column = "metadata_json" if defect else "upload_metadata_json"
    for row in rows:
        if _path_key(str(row["producer_manifest_path"] or "")) != _path_key(
            resources.producer_manifest_path
        ):
            raise ProbeError("RELAY_IDENTITY_MISMATCH", "relay manifest path differs")
        if (
            str(row["producer_id"] or "") != producer_id
            or str(row["key_id"] or "") != key_id
            or str(row["endpoint_url"] or "") != endpoint_url
        ):
            raise ProbeError("RELAY_IDENTITY_MISMATCH", "relay producer identity differs")
        if not defect and str(row["stream_name"] or "") != resources.expected_stream_name:
            raise ProbeError("RELAY_IDENTITY_MISMATCH", "relay stream differs")
        metadata_text = str(row[metadata_column] or "")
        metadata = strict_json_bytes(metadata_text.encode("utf-8")) if metadata_text else None
        relative_path = str(row["relative_path"] or "")
        relative_parts = Path(relative_path.replace("\\", "/")).parts
        row_count = metadata.get("row_count") if isinstance(metadata, Mapping) else None
        first_row_number = (
            metadata.get("first_row_number") if isinstance(metadata, Mapping) else None
        )
        last_row_number = (
            metadata.get("last_row_number") if isinstance(metadata, Mapping) else None
        )
        if not isinstance(metadata, Mapping) or (
            metadata.get("contract_version") != "producer-ingest-source-file-v1"
            or metadata.get("producer_install_id") != producer_install_id
            or metadata.get("client_batch_id") != row["relay_id"]
            or not isinstance(metadata.get("idempotency_key"), str)
            or not metadata.get("idempotency_key")
            or metadata.get("source_host_id") != source_host_id
            or metadata.get("producer_role") != resources.expected_producer_role
            or metadata.get("stream_name") != resources.expected_stream_name
            or metadata.get("source_system") != resources.expected_source_system
            or metadata.get("source_transport") != resources.expected_source_transport
            or metadata.get("manifest_hash") != canonical_manifest_hash
            or metadata.get("relative_path") != relative_path
            or metadata.get("content_sha256") != row["content_sha256"]
            or metadata.get("byte_length") != row["byte_length"]
            or metadata.get("batch_kind") != "whole_file"
            or isinstance(row_count, bool)
            or not isinstance(row_count, int)
            or row_count < 0
            or isinstance(first_row_number, bool)
            or not isinstance(first_row_number, int)
            or isinstance(last_row_number, bool)
            or not isinstance(last_row_number, int)
            or first_row_number != (2 if defect and row_count else (0 if defect else 1))
            or last_row_number != (row_count + 1 if defect and row_count else max(1, row_count))
            or not relative_path
            or relative_path != relative_path.replace("\\", "/").strip("/")
            or Path(relative_path).is_absolute()
            or any(part in {"", ".", ".."} for part in relative_parts)
            or any(part.startswith((".", "~")) or ":" in part for part in relative_parts)
            or Path(relative_path.replace("\\", "/")).name
            != Path(str(row["source_file_path"] or "")).name
        ):
            raise ProbeError("RELAY_IDENTITY_MISMATCH", "relay metadata identity differs")
        if defect:
            chain = metadata.get("hmac_chain")
            if (
                not isinstance(chain, Mapping)
                or chain.get("local_verification_status") != "HMAC_CHAIN_VERIFIED"
                or chain.get("row_count") != row_count
                or not isinstance(chain.get("first_prev_hmac"), str)
                or not _SHA256_RE.fullmatch(str(chain.get("first_prev_hmac")))
                or not isinstance(chain.get("last_hmac"), str)
                or not _SHA256_RE.fullmatch(str(chain.get("last_hmac")))
            ):
                raise ProbeError("RELAY_IDENTITY_MISMATCH", "relay HMAC metadata differs")
        # Runtime reports normalize the physical relay to http_push while
        # retaining the manifest's source transport.  Recompute that exact
        # identity from the persisted upload plan rather than trusting an
        # independently stored transport label.
        normalized_transport = {
            "source_transport": "http_push",
            "manifest_source_transport": metadata.get("source_transport"),
        }
        if normalized_transport != {
            "source_transport": "http_push",
            "manifest_source_transport": resources.expected_source_transport,
        }:
            raise ProbeError("RELAY_IDENTITY_MISMATCH", "relay transport identity differs")


def _validate_acked_row(
    row: sqlite3.Row,
    resources: RelayResources,
    *,
    defect: bool,
) -> None:
    relay_id = str(row["relay_id"] or "")
    content_sha256 = str(row["content_sha256"] or "")
    try:
        byte_length = int(row["byte_length"])
    except (TypeError, ValueError) as exc:
        raise ProbeError("RELAY_ACK_CORRELATION_INVALID", repr(exc)) from exc
    if not relay_id or not _SHA256_RE.fullmatch(content_sha256) or byte_length < 0:
        raise ProbeError("RELAY_ACK_CORRELATION_INVALID", "ACKED relay identity is invalid")

    spool = _require_under(
        str(row["spooled_file_path"] or ""),
        resources.spool_root,
        code="RELAY_ARTIFACT_PATH_INVALID",
    )
    try:
        spool_stat = os.lstat(spool)
    except OSError as exc:
        raise ProbeError("RELAY_ACK_CORRELATION_INVALID", repr(exc)) from exc
    if (
        not spool.is_file()
        or spool.is_symlink()
        or int(spool_stat.st_size) != byte_length
        or _prefix_sha256(spool, byte_length) != content_sha256
    ):
        raise ProbeError("RELAY_ACK_CORRELATION_INVALID", "ACKED spool does not match")

    receipt_text = str(row["receipt_json"] or "")
    if not receipt_text:
        raise ProbeError("RELAY_ACK_CORRELATION_INVALID", "ACKED receipt is absent")
    receipt = strict_json_bytes(receipt_text.encode("utf-8"))
    if not isinstance(receipt, Mapping) or receipt.get("committed") is not True:
        raise ProbeError("RELAY_ACK_CORRELATION_INVALID", "ACKED receipt is invalid")

    metadata_column = "metadata_json" if defect else "upload_metadata_json"
    metadata_text = str(row[metadata_column] or "")
    metadata = strict_json_bytes(metadata_text.encode("utf-8")) if metadata_text else None
    if not isinstance(metadata, Mapping):
        raise ProbeError("RELAY_ACK_CORRELATION_INVALID", "ACKED metadata is invalid")
    totals = receipt.get("totals")
    total_fields = ("inserted", "replayed", "quarantined", "errors")
    expected_server_id = (
        f"{metadata.get('source_host_id')}/{metadata.get('producer_role')}/"
        f"{metadata.get('stream_name')}/{metadata.get('relative_path')}"
    )
    trace_is_valid = (
        isinstance(receipt.get("request_id"), str)
        and bool(receipt.get("request_id"))
        and receipt.get("upload_id") == receipt.get("request_id")
    )
    client_batch_is_valid = receipt.get("client_batch_id") == metadata.get("client_batch_id")
    if defect and not client_batch_is_valid:
        client_batch_is_valid = trace_is_valid and bool(
            _RELAY_CLIENT_BATCH_ID_RE.fullmatch(str(receipt.get("client_batch_id") or ""))
        )
    if (
        receipt.get("status") != "accepted"
        or receipt.get("retryable") is not False
        or receipt.get("next_retry_after") is not None
        or receipt.get("error") is not None
        or not client_batch_is_valid
        or receipt.get("server_source_file_id") != expected_server_id
        or (not defect and not trace_is_valid)
        or not isinstance(totals, Mapping)
        or any(
            isinstance(totals.get(field), bool)
            or not isinstance(totals.get(field), int)
            or int(totals[field]) < 0
            for field in total_fields
        )
        or sum(int(totals[field]) for field in total_fields)
        != metadata.get("row_count")
        or int(totals["quarantined"]) != 0
        or int(totals["errors"]) != 0
    ):
        raise ProbeError("RELAY_ACK_CORRELATION_INVALID", "ACKED receipt is not accepted")
    safe_metadata = _terminal_safe_metadata(metadata, defect=defect)
    safe_receipt = _terminal_safe_metadata(receipt, defect=defect)

    status_path = _require_under(
        str(row["upload_status_path"] or ""),
        resources.upload_status_root,
        code="RELAY_ARTIFACT_PATH_INVALID",
    )
    status = _read_strict_json_file(status_path)
    if (
        status.get("success") is not True
        or status.get("committed") is not True
        or status.get("retryable") is not False
        or status.get("receipt") != safe_receipt
        or status.get("metadata") != safe_metadata
        or _path_key(str(status.get("source_file_path") or "")) != _path_key(spool)
    ):
        raise ProbeError("RELAY_ACK_CORRELATION_INVALID", "ACKED status does not correlate")

    if not defect:
        if resources.report_root is None:
            raise ProbeError("INTERNAL_RESOURCE_UNDECLARED", "relay report root is absent")
        report_path = _require_under(
            str(row["verification_report_path"] or ""),
            resources.report_root,
            code="RELAY_ARTIFACT_PATH_INVALID",
        )
        report = _read_strict_json_file(report_path)
        outbox_ids_text = str(row["outbox_ids_json"] or "")
        outbox_ids = (
            strict_json_bytes(outbox_ids_text.encode("utf-8"))
            if outbox_ids_text
            else None
        )
        if not isinstance(outbox_ids, list) or not all(
            isinstance(item, str) and item for item in outbox_ids
        ) or len(outbox_ids) != metadata.get("row_count"):
            raise ProbeError("RELAY_ACK_CORRELATION_INVALID", "ACKED outbox IDs are invalid")
        if (
            report.get("status") != "SYNCED"
            or report.get("conflict_free") is not True
            or report.get("server_receipt") != safe_receipt
            or report.get("metadata") != safe_metadata
            or _path_key(str(report.get("source_file_path") or "")) != _path_key(spool)
            or report.get("outbox_ids") != outbox_ids
            or report.get("verified_outbox_ids") != outbox_ids
            or report.get("conflict_outbox_ids") != []
        ):
            raise ProbeError("RELAY_ACK_CORRELATION_INVALID", "ACKED report does not correlate")
    else:
        chain = metadata.get("hmac_chain")
        hmac_check = status.get("hmac_check")
        if (
            not isinstance(chain, Mapping)
            or not isinstance(hmac_check, Mapping)
            or hmac_check.get("ok") is not True
            or hmac_check.get("row_count") != metadata.get("row_count")
            or str(hmac_check.get("first_prev_hmac") or "").casefold()
            != str(chain.get("first_prev_hmac") or "").casefold()
            or str(hmac_check.get("last_hmac") or "").casefold()
            != str(chain.get("last_hmac") or "").casefold()
        ):
            raise ProbeError("RELAY_ACK_CORRELATION_INVALID", "ACKED HMAC status differs")


def _observe_orphan_artifacts(
    resources: RelayResources,
    observation: Observation,
    rows: Sequence[sqlite3.Row],
    *,
    defect: bool,
) -> None:
    referenced_spool: set[str] = set()
    referenced_status: set[str] = set()
    referenced_reports: set[str] = set()
    for row in rows:
        for field, root, target in (
            ("spooled_file_path", resources.spool_root, referenced_spool),
            ("upload_status_path", resources.upload_status_root, referenced_status),
        ):
            value = str(row[field] or "")
            if value:
                target.add(_path_key(_require_under(value, root, code="RELAY_ARTIFACT_PATH_INVALID")))
        if not defect:
            value = str(row["verification_report_path"] or "")
            if value:
                assert resources.report_root is not None
                referenced_reports.add(
                    _path_key(
                        _require_under(
                            value,
                            resources.report_root,
                            code="RELAY_ARTIFACT_PATH_INVALID",
                        )
                    )
                )

    for root, referenced, blocker_kind in (
        (resources.spool_root, referenced_spool, "relay_orphan_spool"),
        (resources.upload_status_root, referenced_status, "relay_orphan_status"),
        (resources.report_root, referenced_reports, "relay_orphan_status"),
    ):
        if root is None:
            continue
        for path in resources._regular_descendants(root):
            if _is_temporary_name(path.name):
                observation.add("relay_temporary_artifact", "pending_commit_count", path.name)
            elif _path_key(path) not in referenced:
                observation.add(blocker_kind, "pending_commit_count", path.name)

    runtime_parent = resources.runtime_status_path.parent
    if runtime_parent.is_dir():
        expected = _path_key(resources.runtime_status_path)
        for path in resources._regular_descendants(runtime_parent):
            if _path_key(path) != expected and _is_temporary_name(path.name):
                observation.add("relay_temporary_artifact", "pending_commit_count", path.name)


def observe_relay(
    session: ObservationSession,
    observation: Observation,
    resources: RelayResources,
    runtime_profile: Mapping[str, Any],
    *,
    defect: bool,
    required: bool,
) -> DatabaseObservation | None:
    database = session.observe_sqlite(
        resources.database_path,
        relay_plan(defect=defect),
        observation,
        required=required,
    )
    sources = resources.source_files()
    if database is None:
        if sources:
            for source in sources:
                _add_source_unsynced(observation, source.name)
        return None

    manifest = session.read_json(resources.producer_manifest_path, required=True)
    if not isinstance(manifest, Mapping):
        raise ProbeError("PRODUCER_MANIFEST_INVALID", "producer manifest is not an object")

    connection: sqlite3.Connection | None = None
    try:
        connection = open_immutable_sqlite(resources.database_path)
        rows = _relay_rows(connection, defect=defect)
    except ProbeError:
        raise
    except sqlite3.Error as exc:
        raise ProbeError("SQLITE_OBSERVATION_FAILED", repr(exc)) from exc
    finally:
        if connection is not None:
            connection.close()

    identity_connection: sqlite3.Connection | None = None
    try:
        identity_connection = open_immutable_sqlite(resources.database_path)
        _validate_relay_identity(
            identity_connection,
            resources,
            runtime_profile,
            rows,
            manifest,
            defect=defect,
        )
    except ProbeError:
        raise
    except sqlite3.Error as exc:
        raise ProbeError("SQLITE_OBSERVATION_FAILED", repr(exc)) from exc
    finally:
        if identity_connection is not None:
            identity_connection.close()

    source_by_key = {_path_key(source): source for source in sources}
    if len(source_by_key) != len(sources):
        raise ProbeError("RELAY_SOURCE_LAYOUT_INVALID", "duplicate canonical source path")
    rows_by_source: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        source_path = _require_under(
            str(row["source_file_path"] or ""),
            resources.source_root,
            code="RELAY_SOURCE_PATH_INVALID",
        )
        rows_by_source.setdefault(_path_key(source_path), []).append(row)
        if str(row["status"]) == "acked":
            _validate_acked_row(row, resources, defect=defect)

    orphan_keys = set(rows_by_source) - set(source_by_key)
    for key in sorted(orphan_keys):
        observation.add("relay_orphan_delta", "pending_commit_count", Path(key).name)

    source_auxiliary = resources._regular_descendants(resources.source_root)
    for path in source_auxiliary:
        name = path.name.casefold()
        relevant_family = (
            resources.source_marker.casefold() in name
            or resources.source_suffix.casefold() in name
        )
        if relevant_family and (
            name.endswith(".lock") or _is_temporary_name(name)
        ):
            _add_source_writer(observation, path.name)

    for key, source in sorted(source_by_key.items()):
        snapshot = session.snapshot(source)
        if snapshot.kind != "regular":
            raise ProbeError("RELAY_SOURCE_LAYOUT_INVALID", "source is not a regular file")
        if not _has_complete_tail(source, snapshot.size):
            _add_source_writer(observation, source.name)
        matching_current_acked = any(
            str(row["status"]) == "acked"
            and int(row["byte_length"]) == snapshot.size
            and str(row["content_sha256"] or "") == snapshot.sha256
            for row in rows_by_source.get(key, ())
        )
        if not matching_current_acked:
            _add_source_unsynced(observation, source.name)

    _observe_orphan_artifacts(resources, observation, rows, defect=defect)
    return database


def observe_optional_relay(
    session: ObservationSession,
    observation: Observation,
    resources: RelayResources,
    runtime_profile: Mapping[str, Any],
    *,
    defect: bool,
) -> None:
    observe_relay(
        session,
        observation,
        resources,
        runtime_profile,
        defect=defect,
        required=False,
    )


__all__ = [
    "RELAY_DYNAMIC_BLOCKER_KINDS",
    "RelayResources",
    "RUNTIME_PROFILE_CONTRACT",
    "TrustedRoots",
    "ensure_no_path_override",
    "open_immutable_sqlite",
    "observe_relay",
    "observe_optional_relay",
    "production_roots",
    "relay_plan",
    "validate_profile",
]

"""Pure-read primitives for broker-owned active-work probes.

This module deliberately has no imports from any desktop application.  It
observes files and SQLite databases without creating, migrating, repairing, or
otherwise mutating production state.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat as stat_module
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Protocol, Sequence
from urllib.parse import quote


EVIDENCE_SCHEMA_VERSION = "kmtech-active-work-evidence-v1.0.3-corrective-1"
DIAGNOSTIC_SCHEMA_VERSION = "kmtech-active-work-diagnostic-v1.0.3.4"
BUILD_IDENTITY_SCHEMA_VERSION = "kmtech-active-work-probe-build-v1.0.3.4"
EXIT_CLEAR = 0
EXIT_ACTIVE = 10
EXIT_ERROR = 20

COUNT_FIELDS = (
    "active_process_count",
    "active_session_count",
    "active_lease_count",
    "active_work_count",
    "pending_commit_count",
)
ID_FIELDS = (
    "process_ids",
    "session_ids",
    "lease_ids",
    "work_ids",
    "pending_commit_ids",
)
_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}$")
_ERROR_CODE_RE = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")
_SQL_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_HEX_40_RE = re.compile(r"^[0-9a-f]{40}$")
_HEX_64_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_JSON_BYTES = 4 * 1024 * 1024
_MAX_DIRECTORY_ENTRIES = 4096
_MAX_DIRECTORY_DEPTH = 8
_MAX_HASHED_IDS_PER_BLOCKER = 256
_SQLITE_SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")


class ProbeError(RuntimeError):
    """A fail-closed observation error safe to represent by code and hash."""

    def __init__(self, code: str, detail: str = "") -> None:
        normalized = str(code or "").strip().upper()
        if not _ERROR_CODE_RE.fullmatch(normalized):
            normalized = "INTERNAL_PROBE_ERROR"
        self.code = normalized
        self.detail = str(detail or normalized)
        super().__init__(normalized)


def _probe_error(code: str, detail: str = "") -> ProbeError:
    return ProbeError(code, detail)


def ordered_json_bytes(value: Any) -> bytes:
    """Render insertion-ordered compact JSON like PowerShell ConvertTo-Json.

    Property ordering is supplied by callers.  Sorting here would change the
    broker digest material and is intentionally forbidden.
    """

    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise _probe_error("JSON_RENDER_ERROR", repr(exc)) from exc


def ordered_digest(value: Any) -> str:
    return hashlib.sha256(ordered_json_bytes(value)).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise _probe_error("FILE_READ_ERROR", repr(exc)) from exc
    return digest.hexdigest()


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON key")
        value[key] = item
    return value


def _reject_nonfinite(value: str) -> None:
    raise ValueError(f"non-finite number: {value}")


def strict_json_bytes(raw: bytes) -> Any:
    if not raw or len(raw) > _MAX_JSON_BYTES or raw.startswith(b"\xef\xbb\xbf"):
        raise _probe_error("JSON_INVALID", "JSON size or encoding marker is invalid")
    try:
        text = raw.decode("utf-8")
        return json.loads(
            text,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_nonfinite,
        )
    except (UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise _probe_error("JSON_INVALID", repr(exc)) from exc


def _is_reparse(stat_result: os.stat_result) -> bool:
    attributes = int(getattr(stat_result, "st_file_attributes", 0) or 0)
    reparse_flag = int(getattr(stat_module, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    return bool(attributes & reparse_flag)


def require_trusted_path_ancestry(path: Path) -> Path:
    """Reject symlink/reparse ancestors without resolving through them."""

    normalized = Path(os.path.abspath(os.fspath(path)))
    ancestors = normalized.parents
    if len(ancestors) > 64:
        raise _probe_error("RESOURCE_ANCESTRY_UNBOUNDED", "path ancestry exceeded")
    for ancestor in ancestors:
        try:
            ancestor_stat = os.lstat(ancestor)
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise _probe_error("RESOURCE_ANCESTRY_INVALID", repr(exc)) from exc
        if os.path.islink(ancestor) or _is_reparse(ancestor_stat):
            raise _probe_error("RESOURCE_REPARSE_POINT", "reparse ancestor is forbidden")
        if not stat_module.S_ISDIR(ancestor_stat.st_mode):
            raise _probe_error("RESOURCE_ANCESTRY_INVALID", "ancestor is not a directory")
    return normalized


@dataclass(frozen=True)
class FileSnapshot:
    path: Path
    exists: bool
    kind: str = "absent"
    dev: int = 0
    ino: int = 0
    nlink: int = 0
    size: int = 0
    mtime_ns: int = 0
    sha256: str = ""

    def file_object_identity(self, trusted_path: str) -> str:
        if not self.exists or self.kind != "regular":
            raise _probe_error("DATABASE_ABSENT", "canonical database is absent")
        material = OrderedDict(
            (
                ("path", trusted_path),
                ("dev", self.dev),
                ("ino", self.ino),
                ("type", self.kind),
                ("nlink", self.nlink),
            )
        )
        return ordered_digest(material)


def _regular_snapshot(path: Path, stat_result: os.stat_result) -> FileSnapshot:
    content_sha256 = file_sha256(path)
    try:
        after_hash = os.lstat(path)
    except OSError as exc:
        raise _probe_error("RESOURCE_CONTINUITY_CHANGED", repr(exc)) from exc
    before_identity = (
        int(stat_result.st_dev),
        int(stat_result.st_ino),
        int(stat_result.st_nlink),
        int(stat_result.st_size),
        int(stat_result.st_mtime_ns),
        stat_module.S_IFMT(stat_result.st_mode),
    )
    after_identity = (
        int(after_hash.st_dev),
        int(after_hash.st_ino),
        int(after_hash.st_nlink),
        int(after_hash.st_size),
        int(after_hash.st_mtime_ns),
        stat_module.S_IFMT(after_hash.st_mode),
    )
    if before_identity != after_identity or _is_reparse(after_hash):
        raise _probe_error("RESOURCE_CONTINUITY_CHANGED", "file changed while hashing")
    return FileSnapshot(
        path=path,
        exists=True,
        kind="regular",
        dev=int(stat_result.st_dev),
        ino=int(stat_result.st_ino),
        nlink=int(stat_result.st_nlink),
        size=int(stat_result.st_size),
        mtime_ns=int(stat_result.st_mtime_ns),
        sha256=content_sha256,
    )


def _snapshot_directory(path: Path, stat_result: os.stat_result) -> FileSnapshot:
    entries: list[OrderedDict[str, Any]] = []

    def walk(root: Path, relative: Path, depth: int) -> None:
        if depth > _MAX_DIRECTORY_DEPTH:
            raise _probe_error("RESOURCE_DIRECTORY_UNBOUNDED", "directory depth exceeded")
        try:
            children = sorted(os.scandir(root), key=lambda item: item.name.casefold())
        except OSError as exc:
            raise _probe_error("RESOURCE_SNAPSHOT_ERROR", repr(exc)) from exc
        for child in children:
            if len(entries) >= _MAX_DIRECTORY_ENTRIES:
                raise _probe_error("RESOURCE_DIRECTORY_UNBOUNDED", "entry count exceeded")
            child_path = root / child.name
            try:
                child_stat = os.lstat(child_path)
            except OSError as exc:
                raise _probe_error("RESOURCE_SNAPSHOT_ERROR", repr(exc)) from exc
            if child.is_symlink() or _is_reparse(child_stat):
                raise _probe_error("RESOURCE_REPARSE_POINT", "reparse resource is forbidden")
            child_relative = relative / child.name
            if stat_module.S_ISREG(child_stat.st_mode):
                child_kind = "regular"
                content_hash = file_sha256(child_path)
                try:
                    child_after = os.lstat(child_path)
                except OSError as exc:
                    raise _probe_error("RESOURCE_CONTINUITY_CHANGED", repr(exc)) from exc
                before_identity = (
                    int(child_stat.st_dev),
                    int(child_stat.st_ino),
                    int(child_stat.st_nlink),
                    int(child_stat.st_size),
                    int(child_stat.st_mtime_ns),
                    stat_module.S_IFMT(child_stat.st_mode),
                )
                after_identity = (
                    int(child_after.st_dev),
                    int(child_after.st_ino),
                    int(child_after.st_nlink),
                    int(child_after.st_size),
                    int(child_after.st_mtime_ns),
                    stat_module.S_IFMT(child_after.st_mode),
                )
                if before_identity != after_identity or _is_reparse(child_after):
                    raise _probe_error(
                        "RESOURCE_CONTINUITY_CHANGED",
                        "directory file changed while hashing",
                    )
            elif stat_module.S_ISDIR(child_stat.st_mode):
                child_kind = "directory"
                content_hash = ""
            else:
                raise _probe_error("RESOURCE_TYPE_INVALID", "non-file resource is forbidden")
            entries.append(
                OrderedDict(
                    (
                        ("relative_path", child_relative.as_posix()),
                        ("type", child_kind),
                        ("dev", int(child_stat.st_dev)),
                        ("ino", int(child_stat.st_ino)),
                        ("nlink", int(child_stat.st_nlink)),
                        ("size", int(child_stat.st_size)),
                        ("mtime_ns", int(child_stat.st_mtime_ns)),
                        ("sha256", content_hash),
                    )
                )
            )
            if child_kind == "directory":
                walk(child_path, child_relative, depth + 1)

    walk(path, Path(), 0)
    try:
        directory_after = os.lstat(path)
    except OSError as exc:
        raise _probe_error("RESOURCE_CONTINUITY_CHANGED", repr(exc)) from exc
    if _is_reparse(directory_after) or (
        int(stat_result.st_dev),
        int(stat_result.st_ino),
        int(stat_result.st_nlink),
        int(stat_result.st_size),
        int(stat_result.st_mtime_ns),
        stat_module.S_IFMT(stat_result.st_mode),
    ) != (
        int(directory_after.st_dev),
        int(directory_after.st_ino),
        int(directory_after.st_nlink),
        int(directory_after.st_size),
        int(directory_after.st_mtime_ns),
        stat_module.S_IFMT(directory_after.st_mode),
    ):
        raise _probe_error("RESOURCE_CONTINUITY_CHANGED", "directory changed while hashing")
    return FileSnapshot(
        path=path,
        exists=True,
        kind="directory",
        dev=int(stat_result.st_dev),
        ino=int(stat_result.st_ino),
        nlink=int(stat_result.st_nlink),
        size=int(stat_result.st_size),
        mtime_ns=int(stat_result.st_mtime_ns),
        sha256=ordered_digest(entries),
    )


def snapshot_path(path: Path) -> FileSnapshot:
    normalized = require_trusted_path_ancestry(path)
    try:
        stat_result = os.lstat(normalized)
    except FileNotFoundError:
        return FileSnapshot(path=normalized, exists=False)
    except OSError as exc:
        raise _probe_error("RESOURCE_SNAPSHOT_ERROR", repr(exc)) from exc
    if os.path.islink(normalized) or _is_reparse(stat_result):
        raise _probe_error("RESOURCE_REPARSE_POINT", "reparse resource is forbidden")
    if stat_module.S_ISREG(stat_result.st_mode):
        return _regular_snapshot(normalized, stat_result)
    if stat_module.S_ISDIR(stat_result.st_mode):
        return _snapshot_directory(normalized, stat_result)
    raise _probe_error("RESOURCE_TYPE_INVALID", "non-file resource is forbidden")


def sqlite_sidecar_paths(path: Path) -> tuple[Path, ...]:
    raw = os.fspath(path)
    return tuple(Path(raw + suffix) for suffix in _SQLITE_SIDECAR_SUFFIXES)


def _quoted_identifier(value: str) -> str:
    if not _SQL_IDENTIFIER_RE.fullmatch(value):
        raise _probe_error("INTERNAL_QUERY_INVALID", "unsafe SQL identifier")
    return '"' + value + '"'


@dataclass(frozen=True)
class QuerySpec:
    kind: str
    category: str
    table: str
    id_columns: tuple[str, ...]
    where: str
    required_columns: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.category not in COUNT_FIELDS:
            raise ValueError(f"invalid count category: {self.category}")
        if not _SQL_IDENTIFIER_RE.fullmatch(self.table):
            raise ValueError(f"invalid table: {self.table}")
        for column in (*self.id_columns, *self.required_columns):
            if not _SQL_IDENTIFIER_RE.fullmatch(column):
                raise ValueError(f"invalid column: {column}")
        if not self.id_columns or not self.where or ";" in self.where:
            raise ValueError("query must have identifiers and one static predicate")


@dataclass(frozen=True)
class StatusDomain:
    table: str
    column: str
    allowed: frozenset[str]
    allow_null: bool = False


@dataclass(frozen=True)
class SqlitePlan:
    queries: tuple[QuerySpec, ...]
    status_domains: tuple[StatusDomain, ...]
    require_schema_info: bool = False
    allowed_schema_versions: frozenset[int] | None = None


@dataclass
class _BlockerAccumulator:
    kind: str
    count: int = 0
    id_sha256: set[str] = field(default_factory=set)


@dataclass
class Observation:
    counts: dict[str, int] = field(
        default_factory=lambda: {field_name: 0 for field_name in COUNT_FIELDS}
    )
    _blockers: dict[str, _BlockerAccumulator] = field(default_factory=dict)

    def add(self, kind: str, category: str, raw_identity: Any) -> None:
        if category not in self.counts:
            raise _probe_error("INTERNAL_QUERY_INVALID", "unknown count category")
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,95}", kind):
            raise _probe_error("INTERNAL_QUERY_INVALID", "unsafe blocker kind")
        digest = ordered_digest(
            OrderedDict((("kind", kind), ("identity", raw_identity)))
        )
        self.counts[category] += 1
        blocker = self._blockers.setdefault(kind, _BlockerAccumulator(kind=kind))
        blocker.count += 1
        if len(blocker.id_sha256) < _MAX_HASHED_IDS_PER_BLOCKER:
            blocker.id_sha256.add(digest)

    @property
    def active(self) -> bool:
        return any(int(value) > 0 for value in self.counts.values())

    def sanitized_blockers(self) -> list[OrderedDict[str, Any]]:
        if len(self._blockers) > 64:
            raise _probe_error("BLOCKER_KIND_LIMIT_EXCEEDED", "more than 64 blocker kinds")
        rows: list[OrderedDict[str, Any]] = []
        for kind in sorted(self._blockers):
            blocker = self._blockers[kind]
            rows.append(
                OrderedDict(
                    (
                        ("kind", blocker.kind),
                        ("count", blocker.count),
                        ("id_sha256", sorted(blocker.id_sha256)),
                    )
                )
            )
        return rows


@dataclass(frozen=True)
class DatabaseObservation:
    snapshot: FileSnapshot
    schema_version: int


class ProbeAdapter(Protocol):
    app: str
    app_id: str
    database_path: Path
    database_identity_path: str
    profile_path: Path
    profile_identity_path: str

    def resource_paths(self) -> Sequence[Path]: ...

    def sqlite_paths(self) -> Sequence[Path]: ...

    def observe(
        self,
        session: "ObservationSession",
        observation: Observation,
    ) -> DatabaseObservation: ...


class ObservationSession:
    """One globally continuous snapshot around all adapter reads."""

    def __init__(self, resources: Sequence[Path], sqlite_paths: Sequence[Path]) -> None:
        if any(not Path(path).is_absolute() for path in (*resources, *sqlite_paths)):
            raise _probe_error("RESOURCE_PATH_INVALID", "adapter resource path must be absolute")
        sqlite_normalized = {Path(os.path.abspath(os.fspath(path))) for path in sqlite_paths}
        all_paths = {Path(os.path.abspath(os.fspath(path))) for path in resources}
        for sqlite_path in sqlite_normalized:
            all_paths.add(sqlite_path)
            all_paths.update(sqlite_sidecar_paths(sqlite_path))
        self.sqlite_paths = sqlite_normalized
        self.paths = tuple(sorted(all_paths, key=lambda item: os.fspath(item).casefold()))
        self.before = {path: snapshot_path(path) for path in self.paths}
        for sqlite_path in self.sqlite_paths:
            for sidecar in sqlite_sidecar_paths(sqlite_path):
                if self.before[sidecar].exists:
                    raise _probe_error("SQLITE_SIDECAR_PRESENT", "SQLite sidecar is present")

    def snapshot(self, path: Path) -> FileSnapshot:
        normalized = Path(os.path.abspath(os.fspath(path)))
        try:
            return self.before[normalized]
        except KeyError as exc:
            raise _probe_error("INTERNAL_RESOURCE_UNDECLARED", "adapter read undeclared path") from exc

    def require_regular_file(self, path: Path, *, code: str) -> FileSnapshot:
        snapshot = self.snapshot(path)
        if not snapshot.exists or snapshot.kind != "regular":
            raise _probe_error(code, "required regular file is absent")
        return snapshot

    def read_json(self, path: Path, *, required: bool = False) -> Any | None:
        snapshot = self.snapshot(path)
        if not snapshot.exists:
            if required:
                raise _probe_error("JSON_REQUIRED", "required JSON resource is absent")
            return None
        if snapshot.kind != "regular" or snapshot.size <= 0 or snapshot.size > _MAX_JSON_BYTES:
            raise _probe_error("JSON_INVALID", "JSON resource shape is invalid")
        try:
            raw = path.read_bytes()
        except OSError as exc:
            raise _probe_error("FILE_READ_ERROR", repr(exc)) from exc
        if hashlib.sha256(raw).hexdigest() != snapshot.sha256:
            raise _probe_error("RESOURCE_CONTINUITY_CHANGED", "JSON changed during read")
        return strict_json_bytes(raw)

    def observe_sqlite(
        self,
        path: Path,
        plan: SqlitePlan,
        observation: Observation,
        *,
        required: bool,
    ) -> DatabaseObservation | None:
        snapshot = self.snapshot(path)
        if not snapshot.exists:
            if required:
                raise _probe_error("DATABASE_ABSENT", "required database is absent")
            return None
        if snapshot.kind != "regular" or snapshot.size <= 0:
            raise _probe_error("DATABASE_INVALID", "database is not a nonempty regular file")
        uri_path = quote(Path(os.path.abspath(os.fspath(path))).as_posix(), safe="/:")
        uri = f"file:{uri_path}?mode=ro&immutable=1"
        connection: sqlite3.Connection | None = None
        try:
            connection = sqlite3.connect(uri, uri=True, timeout=0.0)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA query_only = ON")
            query_only = connection.execute("PRAGMA query_only").fetchone()
            if query_only is None or int(query_only[0]) != 1:
                raise _probe_error("SQLITE_QUERY_ONLY_FAILED", "query_only did not remain enabled")
            quick_check = [str(row[0]) for row in connection.execute("PRAGMA quick_check")]
            if quick_check != ["ok"]:
                raise _probe_error("SQLITE_QUICK_CHECK_FAILED", repr(quick_check[:3]))
            schema_version = self._schema_version(connection, plan)
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_schema WHERE type='table'"
                )
            }
            required_schema: dict[str, set[str]] = {}
            for query_spec in plan.queries:
                required_schema.setdefault(query_spec.table, set()).update(
                    query_spec.id_columns
                )
                required_schema[query_spec.table].update(query_spec.required_columns)
            for domain in plan.status_domains:
                required_schema.setdefault(domain.table, set()).add(domain.column)
            for table, columns in required_schema.items():
                if table not in tables:
                    raise _probe_error("SQLITE_REQUIRED_TABLE_MISSING", table)
                actual_columns = {
                    str(row[1])
                    for row in connection.execute(
                        f"PRAGMA table_info({_quoted_identifier(table)})"
                    )
                }
                if not columns.issubset(actual_columns):
                    raise _probe_error(
                        "SQLITE_REQUIRED_COLUMN_MISSING",
                        f"{table}:{sorted(columns - actual_columns)!r}",
                    )
            self._validate_status_domains(connection, plan.status_domains)
            for query_spec in plan.queries:
                selected = ",".join(_quoted_identifier(column) for column in query_spec.id_columns)
                sql = (
                    f"SELECT {selected} FROM {_quoted_identifier(query_spec.table)} "
                    f"WHERE {query_spec.where} ORDER BY {selected}"
                )
                for row in connection.execute(sql):
                    identity = OrderedDict(
                        (column, row[index])
                        for index, column in enumerate(query_spec.id_columns)
                    )
                    observation.add(query_spec.kind, query_spec.category, identity)
        except ProbeError:
            raise
        except sqlite3.Error as exc:
            raise _probe_error("SQLITE_OBSERVATION_FAILED", repr(exc)) from exc
        finally:
            if connection is not None:
                connection.close()
        return DatabaseObservation(snapshot=snapshot, schema_version=schema_version)

    @staticmethod
    def _schema_version(connection: sqlite3.Connection, plan: SqlitePlan) -> int:
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_schema WHERE type='table'"
            )
        }
        if "schema_info" in tables:
            columns = {
                str(row[1]) for row in connection.execute("PRAGMA table_info(schema_info)")
            }
            if not {"key", "value"}.issubset(columns):
                raise _probe_error("SCHEMA_INFO_INVALID", "schema_info columns are invalid")
            rows = list(
                connection.execute(
                    "SELECT value FROM schema_info WHERE key='schema_version'"
                )
            )
            if len(rows) != 1 or not re.fullmatch(r"[0-9]+", str(rows[0][0] or "")):
                raise _probe_error("SCHEMA_INFO_INVALID", "schema_version row is invalid")
            version = int(rows[0][0])
        else:
            if plan.require_schema_info:
                raise _probe_error("SCHEMA_INFO_MISSING", "schema_info is required")
            row = connection.execute("PRAGMA user_version").fetchone()
            if row is None or isinstance(row[0], bool) or int(row[0]) < 0:
                raise _probe_error("SCHEMA_VERSION_INVALID", "PRAGMA user_version is invalid")
            version = int(row[0])
        if plan.allowed_schema_versions is not None and version not in plan.allowed_schema_versions:
            raise _probe_error("SCHEMA_VERSION_UNKNOWN", str(version))
        return version

    @staticmethod
    def _validate_status_domains(
        connection: sqlite3.Connection,
        domains: Iterable[StatusDomain],
    ) -> None:
        for domain in domains:
            table = _quoted_identifier(domain.table)
            column = _quoted_identifier(domain.column)
            rows = connection.execute(
                f"SELECT DISTINCT {column} FROM {table} ORDER BY {column}"
            )
            for row in rows:
                value = row[0]
                if value is None and domain.allow_null:
                    continue
                if not isinstance(value, str) or value not in domain.allowed:
                    raise _probe_error(
                        "SQLITE_STATUS_DOMAIN_UNKNOWN",
                        f"{domain.table}.{domain.column}:{value!r}",
                    )

    def verify_continuity(self) -> None:
        for path in self.paths:
            after = snapshot_path(path)
            if after != self.before[path]:
                raise _probe_error(
                    "RESOURCE_CONTINUITY_CHANGED",
                    "resource object, bytes, or mtime changed",
                )


@dataclass(frozen=True)
class AdapterObservation:
    adapter: ProbeAdapter
    observation: Observation
    database: DatabaseObservation
    profile_snapshot: FileSnapshot


def observe_adapter(adapter: ProbeAdapter) -> AdapterObservation:
    session = ObservationSession(adapter.resource_paths(), adapter.sqlite_paths())
    database_snapshot = session.require_regular_file(
        adapter.database_path,
        code="DATABASE_ABSENT",
    )
    profile_snapshot = session.require_regular_file(
        adapter.profile_path,
        code="PROFILE_ABSENT",
    )
    observation = Observation()
    database: DatabaseObservation | None = None
    primary_error: BaseException | None = None
    try:
        database = adapter.observe(session, observation)
        if database.snapshot != database_snapshot:
            raise _probe_error("DATABASE_IDENTITY_MISMATCH", "canonical snapshot differs")
    except Exception as exc:  # continuity must still be checked on failure
        primary_error = exc
    try:
        session.verify_continuity()
    except Exception as continuity_error:
        raise continuity_error
    if primary_error is not None:
        raise primary_error
    assert database is not None
    return AdapterObservation(
        adapter=adapter,
        observation=observation,
        database=database,
        profile_snapshot=profile_snapshot,
    )


@dataclass(frozen=True)
class ProbeBinding:
    build_identity_sha256: str
    artifact_sha256: str
    source_commit: str
    artifact_path: str
    workflow_mode: str
    supported_apps: tuple[str, ...]


@dataclass(frozen=True)
class ProbeRequest:
    release_run_id: str
    run_id_name: str
    run_id: str
    target_pc: str
    app_id: str
    app: str

    def validate(self) -> None:
        if self.run_id_name not in {"canary_run_id", "qualification_run_id"}:
            raise _probe_error("REQUEST_IDENTITY_INVALID", "run-id selector is invalid")
        for value in (self.release_run_id, self.run_id, self.target_pc):
            if not _ID_RE.fullmatch(value):
                raise _probe_error("REQUEST_IDENTITY_INVALID", "request identifier is invalid")


def utc_timestamp(now: datetime | None = None) -> str:
    value = now or datetime.now(timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _receipt_prefix(
    schema_version: str,
    request: ProbeRequest,
    binding: ProbeBinding,
    captured_at_utc: str,
    *,
    diagnostic_kind: str | None = None,
) -> OrderedDict[str, Any]:
    value: OrderedDict[str, Any] = OrderedDict()
    value["schema_version"] = schema_version
    if diagnostic_kind is not None:
        value["diagnostic_kind"] = diagnostic_kind
    value["release_run_id"] = request.release_run_id
    value[request.run_id_name] = request.run_id
    value["target_pc"] = request.target_pc
    value["app_id"] = request.app_id
    value["app"] = request.app
    value["captured_at_utc"] = captured_at_utc
    value["probe_build_identity_sha256"] = binding.build_identity_sha256
    value["probe_artifact_sha256"] = binding.artifact_sha256
    value["probe_source_commit"] = binding.source_commit
    return value


def build_clear_evidence(
    request: ProbeRequest,
    binding: ProbeBinding,
    observed: AdapterObservation,
    *,
    now: datetime | None = None,
) -> OrderedDict[str, Any]:
    request.validate()
    if observed.observation.active:
        raise _probe_error("INTERNAL_DISPOSITION_MISMATCH", "active observation is not clear")
    receipt = _receipt_prefix(
        EVIDENCE_SCHEMA_VERSION,
        request,
        binding,
        utc_timestamp(now),
    )
    receipt["evidence_source"] = binding.artifact_path
    receipt["evidence_source_sha256"] = binding.artifact_sha256
    receipt["database_identity"] = OrderedDict(
        (
            ("path", observed.adapter.database_identity_path),
            (
                "instance_id",
                observed.database.snapshot.file_object_identity(
                    observed.adapter.database_identity_path
                ),
            ),
            ("schema_version", observed.database.schema_version),
            ("sha256", observed.database.snapshot.sha256),
        )
    )
    receipt["profile_identity"] = OrderedDict(
        (
            ("profile_id", observed.adapter.profile_identity_path),
            ("sha256", observed.profile_snapshot.sha256),
        )
    )
    for field_name in ID_FIELDS:
        receipt[field_name] = []
    receipt["counts"] = OrderedDict((field_name, 0) for field_name in COUNT_FIELDS)
    receipt["production_mutation_count"] = 0
    receipt["observation_digest"] = ordered_digest(receipt)
    return receipt


def build_active_diagnostic(
    request: ProbeRequest,
    binding: ProbeBinding,
    observation: Observation,
    *,
    now: datetime | None = None,
) -> OrderedDict[str, Any]:
    request.validate()
    if not observation.active:
        raise _probe_error("INTERNAL_DISPOSITION_MISMATCH", "active diagnostic has zero counts")
    receipt = _receipt_prefix(
        DIAGNOSTIC_SCHEMA_VERSION,
        request,
        binding,
        utc_timestamp(now),
        diagnostic_kind="ACTIVE_WORK_PRESENT",
    )
    receipt["counts"] = OrderedDict(
        (field_name, int(observation.counts[field_name])) for field_name in COUNT_FIELDS
    )
    receipt["sanitized_blockers"] = observation.sanitized_blockers()
    receipt["production_mutation_count"] = 0
    receipt["raw_ids_recorded"] = False
    receipt["diagnostic_digest"] = ordered_digest(receipt)
    return receipt


def build_error_diagnostic(
    request: ProbeRequest,
    binding: ProbeBinding,
    error: ProbeError,
    *,
    now: datetime | None = None,
) -> OrderedDict[str, Any]:
    receipt = _receipt_prefix(
        DIAGNOSTIC_SCHEMA_VERSION,
        request,
        binding,
        utc_timestamp(now),
        diagnostic_kind="PROBE_ERROR",
    )
    receipt["error_code"] = error.code
    receipt["error_detail_sha256"] = hashlib.sha256(error.detail.encode("utf-8")).hexdigest()
    receipt["production_mutation_count"] = 0
    receipt["raw_ids_recorded"] = False
    receipt["diagnostic_digest"] = ordered_digest(receipt)
    return receipt


def require_lower_hex(value: str, length: int, *, code: str) -> str:
    pattern = _HEX_40_RE if length == 40 else _HEX_64_RE
    if not pattern.fullmatch(str(value or "")):
        raise _probe_error(code, "lowercase hexadecimal identity is invalid")
    return value


__all__ = [
    "AdapterObservation",
    "BUILD_IDENTITY_SCHEMA_VERSION",
    "COUNT_FIELDS",
    "DIAGNOSTIC_SCHEMA_VERSION",
    "DatabaseObservation",
    "EVIDENCE_SCHEMA_VERSION",
    "EXIT_ACTIVE",
    "EXIT_CLEAR",
    "EXIT_ERROR",
    "FileSnapshot",
    "Observation",
    "ObservationSession",
    "ProbeAdapter",
    "ProbeBinding",
    "ProbeError",
    "ProbeRequest",
    "QuerySpec",
    "SqlitePlan",
    "StatusDomain",
    "build_active_diagnostic",
    "build_clear_evidence",
    "build_error_diagnostic",
    "file_sha256",
    "observe_adapter",
    "ordered_digest",
    "ordered_json_bytes",
    "require_trusted_path_ancestry",
    "require_lower_hex",
    "snapshot_path",
    "sqlite_sidecar_paths",
    "strict_json_bytes",
]

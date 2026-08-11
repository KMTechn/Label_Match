"""Pure-read Defect adapter, including all local auxiliary work stores."""

from __future__ import annotations

import base64
import csv
import hashlib
import json
import os
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit

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
from .common import (
    RelayResources,
    TrustedRoots,
    ensure_no_path_override,
    open_immutable_sqlite,
    observe_relay,
    production_roots,
    validate_profile,
)


RETURN_BUNDLE_STATUSES = frozenset(
    {
        "OPEN",
        "VERIFIED",
        "PARTIAL",
        "DOC_GENERATED",
        "PRINT_REQUESTED",
        "PRINT_CONFIRMED",
        "ATTACHED_CONFIRMED",
        "CENTRAL_DISPATCH_PENDING",
        "DISPATCHED",
        "CANCELLED",
        "REJECTED",
        "FROZEN_FOR_ROLLBACK",
    }
)
RETURN_TERMINAL_STATUSES = ("CANCELLED", "REJECTED", "DISPATCHED", "FROZEN_FOR_ROLLBACK")
PENDING_FILE_RE = re.compile(r"^.+\.csv\.pending\.part[0-9]+\.csv$")
ZERO_HASH = "0" * 64
JOURNAL_ESCAPE_PREFIX = "__DEFECT_CSV_ESC_B64__:"
RETURN_JOURNAL_COLUMNS = (
    "timestamp",
    "event_type",
    "worker",
    "return_bundle_id",
    "product_barcode",
    "item_code",
    "scan_status",
    "detail",
    "prev_hash",
    "row_hash",
)
WAREHOUSE_JOURNAL_COLUMNS = (
    "timestamp",
    "event_type",
    "worker",
    "item_code",
    "barcode",
    "direction",
    "lot_id",
    "quantity_delta",
    "detail",
    "prev_hash",
    "row_hash",
)
_CHAIN_EXCLUDED_DETAIL_FIELDS = frozenset(
    {"source_integrity_hash", "computed_integrity_hash", "row_c14n_hash", "prev_hash"}
)


def _restore_journal_cell(value: str) -> str:
    if not value.startswith(JOURNAL_ESCAPE_PREFIX):
        return value
    token = value[len(JOURNAL_ESCAPE_PREFIX) :]
    digest, separator, encoded = token.partition(":")
    if (
        separator != ":"
        or len(digest) != 16
        or any(char not in "0123456789abcdef" for char in digest)
    ):
        return value
    try:
        decoded = base64.urlsafe_b64decode(encoded.encode("ascii")).decode("utf-8")
    except (ValueError, UnicodeError):
        return value
    if hashlib.sha256(decoded.encode("utf-8")).hexdigest()[:16] != digest:
        return value
    return decoded


def _canonical_json(value: Mapping[str, Any]) -> str:
    try:
        return json.dumps(
            dict(value),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise ProbeError("DEFECT_AUXILIARY_JOURNAL_INVALID", repr(exc)) from exc


def _without_chain_fields(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _without_chain_fields(child)
            for key, child in value.items()
            if str(key) not in _CHAIN_EXCLUDED_DETAIL_FIELDS
        }
    if isinstance(value, list):
        return [_without_chain_fields(child) for child in value]
    return value


def _journal_detail(value: str) -> Mapping[str, Any]:
    try:
        parsed = strict_json_bytes((value or "{}").encode("utf-8"))
    except ProbeError as exc:
        raise ProbeError("DEFECT_AUXILIARY_JOURNAL_INVALID", exc.detail) from exc
    if not isinstance(parsed, Mapping):
        raise ProbeError("DEFECT_AUXILIARY_JOURNAL_INVALID", "journal detail is not an object")
    return parsed


def _journal_hash(
    row: Mapping[str, Any],
    prev_hash: str,
    *,
    warehouse: bool,
) -> str:
    if warehouse:
        payload = {
            "timestamp": row["timestamp"],
            "event_type": row["event_type"],
            "worker": row.get("worker", ""),
            "item_code": row.get("item_code", ""),
            "barcode": row.get("barcode", ""),
            "direction": row.get("direction", ""),
            "lot_id": row.get("lot_id", ""),
            "quantity_delta": int(row.get("quantity_delta", 0)),
            "detail": row.get("detail", {}),
        }
    else:
        payload = {
            "timestamp": row["timestamp"],
            "event_type": row["event_type"],
            "worker": row.get("worker", ""),
            "return_bundle_id": row.get("return_bundle_id", ""),
            "product_barcode": row.get("product_barcode", ""),
            "item_code": row.get("item_code", ""),
            "scan_status": row.get("scan_status", ""),
            "detail": _without_chain_fields(row.get("detail", {})),
        }
    material = (prev_hash + "|" + _canonical_json(payload)).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def _read_journal_rows(
    session: ObservationSession,
    path: Path,
    columns: tuple[str, ...],
    *,
    warehouse: bool,
) -> tuple[bool, list[dict[str, Any]]]:
    snapshot = session.snapshot(path)
    if not snapshot.exists:
        return False, []
    if snapshot.kind != "regular":
        raise ProbeError("DEFECT_AUXILIARY_JOURNAL_INVALID", "journal is not regular")
    if snapshot.size == 0:
        return False, []
    try:
        with path.open("rb") as binary:
            binary.seek(-1, os.SEEK_END)
            if binary.read(1) != b"\n":
                raise ProbeError(
                    "DEFECT_AUXILIARY_JOURNAL_INCOMPLETE",
                    "journal has incomplete trailing bytes",
                )
        with path.open("r", encoding="utf-8", newline="") as text:
            reader = csv.DictReader(text)
            if tuple(reader.fieldnames or ()) != columns:
                raise ProbeError(
                    "DEFECT_AUXILIARY_JOURNAL_INVALID",
                    "journal header does not match",
                )
            rows: list[dict[str, Any]] = []
            previous = ZERO_HASH
            for line_number, raw in enumerate(reader, start=2):
                if line_number > 1_000_001:
                    raise ProbeError(
                        "DEFECT_AUXILIARY_JOURNAL_UNBOUNDED",
                        "journal row count exceeded",
                    )
                if None in raw or any(raw.get(column) is None for column in columns):
                    raise ProbeError(
                        "DEFECT_AUXILIARY_JOURNAL_INVALID",
                        "journal row shape differs",
                    )
                restored = {
                    column: _restore_journal_cell(str(raw[column])) for column in columns
                }
                detail = _journal_detail(restored["detail"])
                row: dict[str, Any] = {**restored, "detail": detail}
                if warehouse:
                    try:
                        row["quantity_delta"] = int(restored["quantity_delta"])
                    except (TypeError, ValueError) as exc:
                        raise ProbeError(
                            "DEFECT_AUXILIARY_JOURNAL_INVALID",
                            "journal quantity is invalid",
                        ) from exc
                if restored["prev_hash"] != previous:
                    raise ProbeError(
                        "DEFECT_AUXILIARY_JOURNAL_INVALID",
                        "journal previous hash differs",
                    )
                expected_hash = _journal_hash(row, previous, warehouse=warehouse)
                if restored["row_hash"] != expected_hash:
                    raise ProbeError(
                        "DEFECT_AUXILIARY_JOURNAL_INVALID",
                        "journal row hash differs",
                    )
                previous = restored["row_hash"]
                rows.append(row)
    except ProbeError:
        raise
    except (OSError, UnicodeError, csv.Error) as exc:
        raise ProbeError("DEFECT_AUXILIARY_JOURNAL_INVALID", repr(exc)) from exc
    return True, rows


RETURN_PLAN = SqlitePlan(
    queries=(
        QuerySpec(
            "defect_return_bundle_nonterminal",
            "active_work_count",
            "return_bundles",
            ("return_bundle_id",),
            "status NOT IN ('CANCELLED','REJECTED','DISPATCHED','FROZEN_FOR_ROLLBACK')",
            ("status",),
        ),
        QuerySpec(
            "defect_return_lock_active",
            "active_lease_count",
            "return_bundles",
            ("return_bundle_id",),
            "active_lock_status='LOCK_ACTIVE'",
            ("active_lock_status",),
        ),
        QuerySpec(
            "defect_central_outbox_unresolved",
            "pending_commit_count",
            "return_central_logistics_outbox",
            ("outbox_id",),
            "status<>'ACKED' AND TRIM(superseded_by_outbox_id)=''",
            ("status", "superseded_by_outbox_id"),
        ),
        QuerySpec(
            "defect_final_claim_unresolved",
            "active_lease_count",
            "return_final_defect_claims",
            ("claim_id",),
            "state NOT IN ('RELEASED','CONSUMED')",
            ("state",),
        ),
        QuerySpec(
            "defect_claim_acquire_active",
            "active_lease_count",
            "return_final_defect_claim_acquire_attempts",
            ("acquire_idempotency_key",),
            "state IN ('ACTIVE','BOUND')",
            ("state",),
        ),
        QuerySpec(
            "defect_claim_release_unresolved",
            "pending_commit_count",
            "return_final_defect_claim_release_outbox",
            ("release_outbox_id",),
            "status<>'ACKED'",
            ("status",),
        ),
        QuerySpec(
            "defect_return_bundle_nonterminal",
            "active_work_count",
            "return_events",
            ("event_id",),
            "1=0",
            (
                "timestamp",
                "event_type",
                "worker",
                "return_bundle_id",
                "product_barcode",
                "item_code",
                "scan_status",
                "detail",
                "prev_hash",
                "row_hash",
            ),
        ),
    ),
    status_domains=(
        StatusDomain("return_bundles", "status", RETURN_BUNDLE_STATUSES),
        StatusDomain(
            "return_bundles",
            "active_lock_status",
            frozenset({"", "LOCK_ACTIVE", "LOCK_RELEASED"}),
        ),
        StatusDomain(
            "return_central_logistics_outbox",
            "status",
            frozenset({"PENDING", "ACKED", "CONFLICT"}),
        ),
        StatusDomain(
            "return_final_defect_claims",
            "state",
            frozenset(
                {"UNBOUND", "BOUND", "RELEASE_PENDING", "RELEASED", "CONFLICT", "CONSUMED"}
            ),
        ),
        StatusDomain(
            "return_final_defect_claim_acquire_attempts",
            "state",
            frozenset({"ACTIVE", "BOUND", "RELEASED", "CONSUMED", "ABANDONED"}),
        ),
        StatusDomain(
            "return_final_defect_claim_release_outbox",
            "status",
            frozenset({"PENDING", "ACKED", "CONFLICT"}),
        ),
    ),
    allowed_schema_versions=frozenset({0}),
)


WAREHOUSE_PLAN = SqlitePlan(
    queries=(
        QuerySpec(
            "defect_warehouse_lot_open",
            "active_work_count",
            "lots",
            ("lot_id",),
            "status='open'",
            ("status",),
        ),
        QuerySpec(
            "defect_warehouse_lot_open",
            "active_work_count",
            "movements",
            ("movement_id",),
            "1=0",
            (
                "timestamp",
                "event_type",
                "worker",
                "item_code",
                "barcode",
                "direction",
                "lot_id",
                "quantity_delta",
                "detail",
                "prev_hash",
                "row_hash",
            ),
        ),
    ),
    status_domains=(
        StatusDomain("lots", "status", frozenset({"open", "closed", "merged"})),
    ),
    allowed_schema_versions=frozenset({0}),
)


def _validate_journal_projection(
    session: ObservationSession,
    database_path: Path,
    journal_path: Path,
    *,
    warehouse: bool,
) -> None:
    columns = WAREHOUSE_JOURNAL_COLUMNS if warehouse else RETURN_JOURNAL_COLUMNS
    journal_present, journal_rows = _read_journal_rows(
        session,
        journal_path,
        columns,
        warehouse=warehouse,
    )
    connection: sqlite3.Connection | None = None
    try:
        connection = open_immutable_sqlite(database_path)
        event_table = "movements" if warehouse else "return_events"
        id_column = "movement_id" if warehouse else "event_id"
        event_rows = list(
            connection.execute(f"SELECT * FROM {event_table} ORDER BY {id_column}")
        )
        if not journal_present:
            projection_tables = (
                ("lots", "movements")
                if warehouse
                else (
                    "return_bundles",
                    "return_central_logistics_outbox",
                    "return_final_defect_claims",
                    "return_final_defect_claim_acquire_attempts",
                    "return_final_defect_claim_release_outbox",
                    "return_events",
                )
            )
            if any(
                connection.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone()
                is not None
                for table in projection_tables
            ):
                raise ProbeError(
                    "DEFECT_AUXILIARY_PROJECTION_MISMATCH",
                    "projection rows exist without a source journal",
                )
            return
        if len(event_rows) != len(journal_rows):
            raise ProbeError(
                "DEFECT_AUXILIARY_PROJECTION_MISMATCH",
                "journal and event projection row counts differ",
            )
        for index, (actual, expected) in enumerate(zip(event_rows, journal_rows), start=1):
            try:
                actual_detail = _journal_detail(str(actual["detail"] or "{}"))
                actual_id = int(actual[id_column])
                actual_quantity = int(actual["quantity_delta"]) if warehouse else None
            except (KeyError, TypeError, ValueError, IndexError) as exc:
                raise ProbeError(
                    "DEFECT_AUXILIARY_PROJECTION_MISMATCH",
                    "event projection row is invalid",
                ) from exc
            expected_values = {
                column: expected[column]
                for column in columns
                if column not in {"detail", "quantity_delta"}
            }
            if (
                actual_id != index
                or actual_detail != expected["detail"]
                or (warehouse and actual_quantity != expected["quantity_delta"])
                or any(str(actual[column]) != str(value) for column, value in expected_values.items())
            ):
                raise ProbeError(
                    "DEFECT_AUXILIARY_PROJECTION_MISMATCH",
                    "journal and event projection rows differ",
                )
    except ProbeError:
        raise
    except sqlite3.Error as exc:
        raise ProbeError("SQLITE_OBSERVATION_FAILED", repr(exc)) from exc
    finally:
        if connection is not None:
            connection.close()


@dataclass
class DefectAdapter:
    roots: TrustedRoots
    app: str = "Defect_Inspection"
    app_id: str = "defect_inspection"

    def __post_init__(self) -> None:
        self.database_path = (
            self.roots.program_data
            / "KMTech"
            / "DirectSync"
            / "defect_inspection"
            / "queue"
            / "direct_sync_hmac_relay.sqlite3"
        )
        self.database_identity_path = str(self.database_path)
        relay_root = self.database_path.parents[1]
        self.source_root = self.roots.program_data / "Defect_Inspection" / "audit"
        self.relay_resources = RelayResources(
            database_path=self.database_path,
            producer_manifest_path=relay_root / "producer_manifest.json",
            source_root=self.source_root,
            spool_root=relay_root / "spool",
            upload_status_root=relay_root / "upload_status",
            report_root=None,
            runtime_status_path=(
                relay_root / "runtime_status" / "direct_sync_hmac_relay_status.json"
            ),
            source_suffix="_defect_return.csv",
            source_marker="_defect_return",
            expected_pc_id=self.roots.target_pc,
            expected_producer_role="defect_inspection",
            expected_stream_name="defect_return_events",
            expected_source_system="defect_return_bundle_ledger",
            expected_source_transport="hmac_csv",
        )
        self.profile_path = (
            self.roots.program_data
            / "KMTech"
            / "Logistics"
            / "profiles"
            / self.app
            / "runtime-profile.json"
        )
        self.profile_identity_path = str(self.profile_path)
        self.legacy_profile_path = (
            self.roots.program_data / "KMTech" / "Logistics" / "runtime-profile.json"
        )
        self.central_profile_path = (
            self.roots.program_data
            / "KMTech"
            / "Defect_Inspection"
            / "central-api-profile.json"
        )
        defect_root = self.roots.local_app_data / "Defect_Inspection"
        self.return_db_path = defect_root / "return_bundle_ledger.db"
        self.return_journal_path = defect_root / "return_bundle_journal.csv"
        self.warehouse_db_path = defect_root / "warehouse_ledger.db"
        self.warehouse_journal_path = defect_root / "warehouse_ledger_journal.csv"
        self.pending_dir = defect_root / "pending"

    def resource_paths(self) -> Sequence[Path]:
        return (
            *self.relay_resources.resource_paths(),
            self.profile_path,
            self.legacy_profile_path,
            self.central_profile_path,
            self.return_db_path,
            self.return_journal_path,
            self.warehouse_db_path,
            self.warehouse_journal_path,
            self.pending_dir,
        )

    def sqlite_paths(self) -> Sequence[Path]:
        return (self.database_path, self.return_db_path, self.warehouse_db_path)

    def observe(
        self,
        session: ObservationSession,
        observation: Observation,
    ) -> DatabaseObservation:
        self._check_overrides()
        runtime_profile = validate_profile(
            session,
            self.profile_path,
            (self.legacy_profile_path,),
            allow_identical_legacy=True,
        )
        self._validate_central_profile(session, runtime_profile)
        database = observe_relay(
            session,
            observation,
            self.relay_resources,
            runtime_profile,
            defect=True,
            required=True,
        )
        assert database is not None
        return_db = session.observe_sqlite(
            self.return_db_path,
            RETURN_PLAN,
            observation,
            required=False,
        )
        warehouse_db = session.observe_sqlite(
            self.warehouse_db_path,
            WAREHOUSE_PLAN,
            observation,
            required=False,
        )
        if session.snapshot(self.return_journal_path).exists and return_db is None:
            raise ProbeError("DEFECT_AUXILIARY_DB_MISSING", "return journal has no projection DB")
        if session.snapshot(self.warehouse_journal_path).exists and warehouse_db is None:
            raise ProbeError("DEFECT_AUXILIARY_DB_MISSING", "warehouse journal has no projection DB")
        if return_db is not None:
            _validate_journal_projection(
                session,
                self.return_db_path,
                self.return_journal_path,
                warehouse=False,
            )
        if warehouse_db is not None:
            _validate_journal_projection(
                session,
                self.warehouse_db_path,
                self.warehouse_journal_path,
                warehouse=True,
            )
        self._observe_pending_files(session, observation)
        return database

    def _check_overrides(self) -> None:
        for variable, expected in (
            ("DEFECT_RETURN_DB", self.return_db_path),
            ("DEFECT_RETURN_JOURNAL", self.return_journal_path),
            ("DEFECT_WAREHOUSE_DB", self.warehouse_db_path),
            ("DEFECT_WAREHOUSE_JOURNAL", self.warehouse_journal_path),
        ):
            ensure_no_path_override(variable, expected)
        ensure_no_path_override("DEFECT_INSPECTION_SYNC_DIR", self.source_root)
        production_program_data = os.path.normcase(
            os.path.abspath(r"C:\ProgramData")
        )
        if os.path.normcase(os.path.abspath(os.fspath(self.roots.program_data))) == production_program_data:
            configured = str(os.environ.get("DEFECT_INSPECTION_SYNC_DIR") or "").strip()
            if not configured:
                raise ProbeError(
                    "CONFIG_AMBIGUITY",
                    "production Defect source root binding is absent",
                )
            if any(
                str(os.environ.get(name) or "").strip().casefold()
                in {"1", "true", "yes", "on", "legacy", "syncthing"}
                for name in (
                    "DEFECT_INSPECTION_USE_LEGACY_SYNCTHING",
                    "DEFECT_INSPECTION_LEGACY_SYNCTHING",
                )
            ):
                raise ProbeError("CONFIG_AMBIGUITY", "legacy Defect source mode is enabled")

    def _validate_central_profile(
        self,
        session: ObservationSession,
        runtime_profile: Mapping[str, object],
    ) -> None:
        value = session.read_json(self.central_profile_path, required=False)
        if value is None:
            return
        if (
            not isinstance(value, Mapping)
            or set(value)
            != {"contract_version", "server_base_url", "source_host_id", "timeout_seconds"}
            or value.get("contract_version") != "km-defect-central-api-profile-v1"
            or not isinstance(value.get("server_base_url"), str)
            or not isinstance(value.get("source_host_id"), str)
            or not str(value.get("source_host_id") or "").strip()
            or isinstance(value.get("timeout_seconds"), bool)
            or not isinstance(value.get("timeout_seconds"), (int, float))
            or not 0 < float(value["timeout_seconds"]) <= 60
        ):
            raise ProbeError("DEFECT_CENTRAL_PROFILE_INVALID", "central profile is invalid")
        try:
            parsed = urlsplit(str(value["server_base_url"]))
        except ValueError as exc:
            raise ProbeError("DEFECT_CENTRAL_PROFILE_INVALID", repr(exc)) from exc
        if (
            parsed.scheme.lower() != "https"
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
            or str(value["server_base_url"]) != str(runtime_profile["base_url"])
            or str(value["source_host_id"]) != str(runtime_profile["source_host_id"])
        ):
            raise ProbeError(
                "DEFECT_CENTRAL_PROFILE_BINDING_MISMATCH",
                "central and logistics profile identities differ",
            )

    def _observe_pending_files(
        self,
        session: ObservationSession,
        observation: Observation,
    ) -> None:
        snapshot = session.snapshot(self.pending_dir)
        if not snapshot.exists:
            return
        if snapshot.kind != "directory":
            raise ProbeError("DEFECT_PENDING_STORE_INVALID", "pending path is not a directory")
        try:
            entries = sorted(os.scandir(self.pending_dir), key=lambda item: item.name.casefold())
        except OSError as exc:
            raise ProbeError("DEFECT_PENDING_STORE_INVALID", repr(exc)) from exc
        for entry in entries:
            if not entry.is_file(follow_symlinks=False) or entry.is_symlink():
                raise ProbeError("DEFECT_PENDING_STORE_INVALID", "pending entry is not a regular file")
            if not PENDING_FILE_RE.fullmatch(entry.name):
                raise ProbeError("DEFECT_PENDING_STORE_UNKNOWN", "unknown pending filename")
            observation.add("defect_local_buffer_pending", "pending_commit_count", entry.name)


def create_adapter(
    app: str,
    target_pc: str,
    *,
    roots: TrustedRoots | None = None,
) -> DefectAdapter:
    if app != "Defect_Inspection":
        raise ProbeError("APP_UNSUPPORTED", "Defect adapter app is invalid")
    return DefectAdapter(roots=roots or production_roots(target_pc))


__all__ = ["DefectAdapter", "RETURN_PLAN", "WAREHOUSE_PLAN", "create_adapter"]

"""Pure-read Container_Audit active-work adapter."""

from __future__ import annotations

import hashlib
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Mapping, Sequence

from .adapters.common import TrustedRoots, production_roots
from .core import (
    DatabaseObservation,
    Observation,
    ObservationSession,
    ProbeError,
    QuerySpec,
    SqlitePlan,
    StatusDomain,
)
from .label import (
    _absolute,
    _canonical_plan,
    _immutable_connection,
    _require_columns,
    _safe_regular_children,
    _same_windows_path,
    _validate_app_profile,
    _validate_producer_registration,
    _validate_relay_artifacts,
    _validate_runtime_authority,
    _windows_key,
)


APP = "Container_Audit"
APP_ID = "container_audit"
DATABASE_IDENTITY_PATH = (
    "C:/ProgramData/KMTech/DirectSync/container_audit/queue/"
    "direct_sync_relay.sqlite3"
)
PROFILE_IDENTITY_PATH = (
    "C:/ProgramData/KMTech/Logistics/profiles/Container_Audit/"
    "runtime-profile.json"
)
TRANSFER_SCHEMA_VERSION = "container-audit-transfer-seal-v1"
MEMBER_EXCHANGE_SCHEMA_VERSION = "container-audit-member-exchange-v1"
LEASE_STORE_SCHEMA_VERSION = "container-terminal-operation-lease-store-v1"
PHS_JOURNAL_SCHEMA_VERSION = "container-audit-phs-label-exchange-v1"
PHS_TERMINAL_STATUSES = frozenset({"COMMITTED", "CANCELLED"})

_TRANSFER_STATUSES = frozenset(
    {"PREPARED", "COMMAND_READY", "RETRY_WAIT", "ACKED", "OPERATOR_REVIEW"}
)
_LOCAL_APPLY_STATUSES = frozenset({"PENDING", "APPLIED", "OPERATOR_REVIEW"})
_LEASE_ISSUE_STATUSES = frozenset(
    {
        "PENDING",
        "PREFETCHED",
        "LOCAL_COMPLETED",
        "OPERATOR_REVIEW",
        "EXPIRED_UNRECONCILED",
        "RECONCILIATION_REQUIRED",
        "CONSUMED_UNACKED",
        "ACKED",
        "RELEASED",
    }
)
_UNRESOLVED_LEASE_STATUSES = (
    "'PENDING','PREFETCHED','LOCAL_COMPLETED','OPERATOR_REVIEW',"
    "'EXPIRED_UNRECONCILED','RECONCILIATION_REQUIRED','CONSUMED_UNACKED'"
)
_SOURCE_PREFIX = "이적작업이벤트로그_"

BLOCKER_KIND_CATALOG = (
    "container_exchange_pending",
    "container_operation_pending",
    "container_projection_pending",
    "container_transfer_pending",
    "relay_unacked_batch",
    "runtime_authority_unresolved",
    "session_recovery_active",
    "source_sync_pending",
)


def _validate_transfer_schema(path: Path) -> None:
    connection = _immutable_connection(path)
    try:
        _require_columns(
            connection,
            "terminal_operation_lease_meta",
            {"singleton", "schema_version"},
        )
        rows = connection.execute(
            "SELECT schema_version FROM terminal_operation_lease_meta WHERE singleton=1"
        ).fetchall()
        if len(rows) != 1 or str(rows[0][0] or "") != LEASE_STORE_SCHEMA_VERSION:
            raise ProbeError("SCHEMA_VERSION_UNKNOWN", "operation lease schema is unknown")
    except ProbeError:
        raise
    except sqlite3.Error as exc:
        raise ProbeError("SQLITE_OBSERVATION_FAILED", repr(exc)) from exc
    finally:
        connection.close()


def _validate_member_exchange_dismissals(path: Path) -> None:
    connection = _immutable_connection(path)
    try:
        _require_columns(
            connection,
            "transfer_member_exchange_dismissals",
            {"intent_id", "reason", "dismissed_at"},
        )
        _require_columns(
            connection,
            "transfer_member_exchange_intents",
            {"intent_id", "status", "command_json", "command_id", "receipt_json"},
        )
        rows = connection.execute(
            "SELECT d.intent_id,d.reason,d.dismissed_at,i.status,i.command_json,"
            "i.command_id,i.receipt_json FROM transfer_member_exchange_dismissals d "
            "LEFT JOIN transfer_member_exchange_intents i ON i.intent_id=d.intent_id "
            "ORDER BY d.intent_id"
        ).fetchall()
        for row in rows:
            reason = str(row["reason"] or "").strip()
            dismissed_at = str(row["dismissed_at"] or "").strip()
            try:
                parsed = datetime.fromisoformat(dismissed_at.replace("Z", "+00:00"))
            except ValueError as exc:
                raise ProbeError("DISMISSAL_INVALID", repr(exc)) from exc
            if (
                not str(row["intent_id"] or "").strip()
                or not reason
                or parsed.tzinfo is None
                or row["status"] not in {"PREPARED", "RETRY_WAIT", "OPERATOR_REVIEW"}
                or row["command_json"] is not None
                or row["command_id"] is not None
                or row["receipt_json"] is not None
            ):
                raise ProbeError("DISMISSAL_INVALID", "member-exchange dismissal is not pre-command")
    except ProbeError:
        raise
    except sqlite3.Error as exc:
        raise ProbeError("SQLITE_OBSERVATION_FAILED", repr(exc)) from exc
    finally:
        connection.close()


def _business_plan() -> SqlitePlan:
    member_blocking = (
        "(status IN ('PREPARED','COMMAND_READY','RETRY_WAIT','OPERATOR_REVIEW') "
        "OR (status='ACKED' AND local_apply_status<>'APPLIED')) AND NOT EXISTS ("
        "SELECT 1 FROM transfer_member_exchange_dismissals d "
        "WHERE d.intent_id=transfer_member_exchange_intents.intent_id "
        "AND transfer_member_exchange_intents.status IN "
        "('PREPARED','RETRY_WAIT','OPERATOR_REVIEW') "
        "AND transfer_member_exchange_intents.command_json IS NULL "
        "AND transfer_member_exchange_intents.command_id IS NULL "
        "AND transfer_member_exchange_intents.receipt_json IS NULL)"
    )
    return SqlitePlan(
        queries=(
            QuerySpec(
                "container_transfer_pending",
                "active_work_count",
                "transfer_seal_intents",
                ("intent_id",),
                "status IN ('PREPARED','COMMAND_READY','RETRY_WAIT')",
                ("schema_version", "status", "operation_lease_id"),
            ),
            QuerySpec(
                "container_transfer_pending",
                "pending_commit_count",
                "transfer_seal_intents",
                ("intent_id",),
                "status IN ('PREPARED','COMMAND_READY','RETRY_WAIT')",
                ("status",),
            ),
            QuerySpec(
                "container_transfer_pending",
                "active_lease_count",
                "transfer_seal_intents",
                ("intent_id", "operation_lease_id"),
                "status IN ('PREPARED','COMMAND_READY','RETRY_WAIT') "
                "AND TRIM(operation_lease_id)<>''",
                ("status", "operation_lease_id"),
            ),
            QuerySpec(
                "container_projection_pending",
                "pending_commit_count",
                "transfer_post_review_outbox",
                ("review_case_id",),
                "projection_log_file_path<>'' AND NOT EXISTS ("
                "SELECT 1 FROM transfer_post_review_projection_receipts receipt "
                "WHERE receipt.review_case_id="
                "transfer_post_review_outbox.review_case_id)",
                ("projection_log_file_path",),
            ),
            QuerySpec(
                "container_projection_pending",
                "pending_commit_count",
                "phs_replacement_waiting_outbox",
                ("intent_id",),
                "projection_log_file_path IS NOT NULL "
                "AND projection_log_file_path<>'' AND NOT EXISTS ("
                "SELECT 1 FROM phs_replacement_waiting_projection_receipts receipt "
                "WHERE receipt.intent_id=phs_replacement_waiting_outbox.intent_id)",
                ("projection_log_file_path",),
            ),
            QuerySpec(
                "container_exchange_pending",
                "active_work_count",
                "transfer_member_exchange_intents",
                ("intent_id",),
                member_blocking,
                (
                    "schema_version",
                    "status",
                    "local_apply_status",
                    "command_id",
                    "command_json",
                    "receipt_json",
                ),
            ),
            QuerySpec(
                "container_exchange_pending",
                "pending_commit_count",
                "transfer_member_exchange_intents",
                ("intent_id",),
                member_blocking,
                ("status", "local_apply_status"),
            ),
            QuerySpec(
                "container_operation_pending",
                "active_lease_count",
                "terminal_operation_lease_issue_attempts",
                ("attempt_id", "lease_id"),
                f"status IN ({_UNRESOLVED_LEASE_STATUSES})",
                ("status", "lease_id"),
            ),
            QuerySpec(
                "container_operation_pending",
                "active_work_count",
                "terminal_operation_lease_issue_attempts",
                ("attempt_id",),
                f"status IN ({_UNRESOLVED_LEASE_STATUSES})",
                ("status",),
            ),
            QuerySpec(
                "container_operation_pending",
                "pending_commit_count",
                "terminal_operation_lease_outbox",
                ("outbox_id", "lease_id"),
                "NOT EXISTS (SELECT 1 FROM terminal_operation_lease_receipts receipt "
                "WHERE receipt.lease_id=terminal_operation_lease_outbox.lease_id)",
                ("lease_id",),
            ),
        ),
        status_domains=(
            StatusDomain("transfer_seal_intents", "status", _TRANSFER_STATUSES),
            StatusDomain(
                "transfer_seal_intents",
                "schema_version",
                frozenset({TRANSFER_SCHEMA_VERSION}),
            ),
            StatusDomain(
                "transfer_member_exchange_intents",
                "status",
                _TRANSFER_STATUSES,
            ),
            StatusDomain(
                "transfer_member_exchange_intents",
                "local_apply_status",
                _LOCAL_APPLY_STATUSES,
            ),
            StatusDomain(
                "transfer_member_exchange_intents",
                "schema_version",
                frozenset({MEMBER_EXCHANGE_SCHEMA_VERSION}),
            ),
            StatusDomain(
                "terminal_operation_lease_issue_attempts",
                "status",
                _LEASE_ISSUE_STATUSES,
            ),
        ),
        allowed_schema_versions=frozenset({0}),
    )


@dataclass(frozen=True)
class ContainerAuditTestContext:
    """Explicit filesystem injection used only by adapter unit tests."""

    data_root: Path
    direct_sync_root: Path
    profile_path: Path
    install_root: Path
    alternate_roots: tuple[Path, ...] = ()
    legacy_profile_paths: tuple[Path, ...] = ()


@dataclass(frozen=True)
class ContainerAuditAdapter:
    data_root: Path
    direct_sync_root: Path
    profile_path: Path
    install_root: Path
    alternate_roots: tuple[Path, ...] = ()
    legacy_profile_paths: tuple[Path, ...] = ()

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
    ) -> "ContainerAuditAdapter":
        if target_pc != "TEST1":
            raise ProbeError("TARGET_PC_UNSUPPORTED", "Container_Audit target is not mapped")
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
        local_app_data_text = str(env.get("LOCALAPPDATA", "") or "").strip()
        user_profile_text = str(env.get("USERPROFILE", "") or "").strip()
        expected_local_app_data = trusted.local_app_data
        expected_user_profile = expected_local_app_data.parent.parent
        if local_app_data_text and not _same_windows_path(local_app_data_text, expected_local_app_data):
            raise ProbeError("ROOT_AMBIGUOUS", "LOCALAPPDATA differs from the target user")
        if user_profile_text and not _same_windows_path(user_profile_text, expected_user_profile):
            raise ProbeError("ROOT_AMBIGUOUS", "USERPROFILE differs from the target user")
        data_root = expected_local_app_data / "KMTech" / "ContainerAudit"
        override = str(env.get("CONTAINER_AUDIT_DATA_ROOT", "") or "").strip()
        if override and not _same_windows_path(override, data_root):
            raise ProbeError("ROOT_AMBIGUOUS", "CONTAINER_AUDIT_DATA_ROOT is alternate state")
        direct_root = canonical_program_data / "KMTech" / "DirectSync" / "container_audit"
        profile = (
            canonical_program_data
            / "KMTech"
            / "Logistics"
            / "profiles"
            / "Container_Audit"
            / "runtime-profile.json"
        )
        install_root = trusted.apps_root / "Container_Audit" / "current"
        legacy_profile = canonical_program_data / "KMTech" / "Logistics" / "runtime-profile.json"
        return cls(
            data_root,
            direct_root,
            profile,
            install_root,
            legacy_profile_paths=(legacy_profile,),
        )

    @classmethod
    def from_test_context(cls, context: ContainerAuditTestContext) -> "ContainerAuditAdapter":
        data_root = _absolute(context.data_root)
        direct_root = _absolute(context.direct_sync_root)
        profile = _absolute(context.profile_path)
        install_root = _absolute(context.install_root)
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
        return cls(data_root, direct_root, profile, install_root, alternates, legacy_profiles)

    @property
    def database_path(self) -> Path:
        return self.direct_sync_root / "queue" / "direct_sync_relay.sqlite3"

    @property
    def producer_manifest_path(self) -> Path:
        return self.direct_sync_root / "producer_manifest.json"

    @property
    def registration_report_path(self) -> Path:
        return self.status_root / "worker_pc_registration.json"

    @property
    def events_root(self) -> Path:
        return self.data_root / "events"

    @property
    def business_database_path(self) -> Path:
        return self.data_root / "transfer_seal" / "transfer_seal.db"

    @property
    def phs_journal_path(self) -> Path:
        return (
            self.data_root
            / "phs_label_exchange"
            / "phs_label_exchange_recovery.json"
        )

    @property
    def parked_root(self) -> Path:
        return self.install_root / "config" / "parked_trays"

    @property
    def spool_root(self) -> Path:
        return self.direct_sync_root / "spool"

    @property
    def status_root(self) -> Path:
        return self.direct_sync_root / "status"

    def _current_files(self) -> tuple[Path, ...]:
        return self._glob_files(self.events_root, "_current_tray_state_*.json")

    def _parked_files(self) -> tuple[Path, ...]:
        return self._glob_files(self.parked_root, "parked_*.json")

    def _source_files(self) -> tuple[Path, ...]:
        return self._glob_files(self.events_root, f"{_SOURCE_PREFIX}*.csv")

    @staticmethod
    def _glob_files(root: Path, pattern: str) -> tuple[Path, ...]:
        prefix, suffix = pattern.split("*", 1)
        return tuple(
            path
            for path in _safe_regular_children(root)
            if path.name.startswith(prefix) and path.name.endswith(suffix)
        )

    def resource_paths(self) -> Sequence[Path]:
        current = self._current_files()
        parked = self._parked_files()
        sources = self._source_files()
        resources = [
            self.events_root,
            self.parked_root,
            self.direct_sync_root,
            self.spool_root,
            self.status_root,
            self.producer_manifest_path,
            self.registration_report_path,
            self.business_database_path,
            self.phs_journal_path,
            self.profile_path,
            *self.legacy_profile_paths,
            self.database_path,
            *self.alternate_roots,
            *current,
            *parked,
            *sources,
        ]
        resources.extend(Path(os.fspath(path) + ".lock") for path in sources)
        return tuple(resources)

    def sqlite_paths(self) -> Sequence[Path]:
        return (self.database_path, self.business_database_path)

    def _reject_ambiguous_or_temporary_state(self, session: ObservationSession) -> None:
        for root in self.alternate_roots:
            if session.snapshot(root).exists:
                raise ProbeError("ROOT_AMBIGUOUS", "alternate Container_Audit root contains state")
        current = {_windows_key(path) for path in self._current_files()}
        parked = {_windows_key(path) for path in self._parked_files()}
        sources = {_windows_key(path) for path in self._source_files()}
        for path in _safe_regular_children(self.events_root):
            name = path.name.casefold()
            key = _windows_key(path)
            if name.startswith("_current_tray_state_") and key not in current:
                raise ProbeError("TEMPORARY_STATE_PRESENT", "unrecognized current tray artifact exists")
            if path.suffix.casefold() == ".csv" and key not in sources:
                raise ProbeError("SOURCE_ARTIFACT_UNKNOWN", "unrecognized source CSV exists")
            if name.endswith(".lock"):
                source_key = _windows_key(Path(os.fspath(path)[: -len(".lock")]))
                if source_key not in sources:
                    raise ProbeError("SOURCE_ARTIFACT_UNKNOWN", "orphan source lock exists")
            if name.endswith((".tmp", ".temp", ".spool", ".delta")) or any(
                marker in name for marker in (".tmp-", ".bad-", ".migrate-")
            ):
                raise ProbeError("TEMPORARY_STATE_PRESENT", "temporary event state exists")
        for path in _safe_regular_children(self.parked_root):
            if path.name.casefold().startswith("parked_") and _windows_key(path) not in parked:
                raise ProbeError("TEMPORARY_STATE_PRESENT", "unrecognized parked tray artifact exists")
        prefix = self.phs_journal_path.name.casefold()
        for path in _safe_regular_children(self.phs_journal_path.parent):
            if path.name.casefold() != prefix and path.name.casefold().startswith(prefix):
                raise ProbeError("TEMPORARY_STATE_PRESENT", "PHS journal temp state exists")

    @staticmethod
    def _validate_tray_state(value: object) -> Mapping[str, object]:
        if not isinstance(value, dict):
            raise ProbeError("TRAY_STATE_INVALID", "tray state is not an object")
        required_strings = (
            "worker_name",
            "master_label_code",
            "item_code",
            "item_name",
            "item_spec",
        )
        if any(not isinstance(value.get(key), str) for key in required_strings):
            raise ProbeError("TRAY_STATE_INVALID", "tray text fields are invalid")
        if not str(value.get("worker_name") or "").strip() or not str(
            value.get("master_label_code") or ""
        ).strip():
            raise ProbeError("TRAY_STATE_INVALID", "tray identity is incomplete")
        scanned = value.get("scanned_barcodes")
        if not isinstance(scanned, list) or not all(isinstance(item, str) for item in scanned):
            raise ProbeError("TRAY_STATE_INVALID", "tray scans are invalid")
        lease_id = value.get("operation_lease_id", "")
        if not isinstance(lease_id, str):
            raise ProbeError("TRAY_STATE_INVALID", "operation lease ID is invalid")
        review = value.get("pending_operator_review")
        if review is not None:
            if (
                not isinstance(review, dict)
                or review.get("schema_version") != 1
                or review.get("outcome")
                not in {"OPERATOR_REVIEW", "RETRY_WAIT", "LOCAL_EVENT_RETRY"}
            ):
                raise ProbeError("TRAY_STATE_INVALID", "operator review payload is invalid")
        completion = value.get("pending_completion_event")
        if completion is not None:
            if (
                not isinstance(completion, dict)
                or completion.get("schema_version") != 1
                or completion.get("event_type") != "TRAY_COMPLETE"
                or not str(completion.get("transfer_intent_id") or "").strip()
            ):
                raise ProbeError("TRAY_STATE_INVALID", "completion payload is invalid")
            expected = f"tray-complete:{str(completion['transfer_intent_id']).strip()}"
            if completion.get("idempotency_key") != expected:
                raise ProbeError("TRAY_STATE_INVALID", "completion identity is invalid")
        return value

    def _observe_tray_files(
        self,
        session: ObservationSession,
        observation: Observation,
    ) -> None:
        for kind, paths in (
            ("session_recovery_active", self._current_files()),
            ("session_recovery_active", self._parked_files()),
        ):
            for path in paths:
                value = self._validate_tray_state(session.read_json(path, required=True))
                identity = str(value.get("master_label_code") or path.name)
                observation.add(kind, "active_session_count", identity)
                observation.add(kind, "active_work_count", identity)
                lease_id = str(value.get("operation_lease_id") or "").strip()
                if lease_id:
                    observation.add("container_operation_pending", "active_lease_count", lease_id)
                if value.get("pending_operator_review") is not None:
                    observation.add("container_operation_pending", "active_work_count", identity)
                completion = value.get("pending_completion_event")
                if isinstance(completion, dict):
                    observation.add(
                        "container_operation_pending",
                        "pending_commit_count",
                        completion.get("idempotency_key"),
                    )

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
            identity = str(state.get("canonical_input_tag_qr") or self.phs_journal_path.name)
            observation.add("container_transfer_pending", "active_work_count", identity)
            observation.add("container_transfer_pending", "pending_commit_count", identity)

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
                    observation.add(
                        "source_sync_pending",
                        "pending_commit_count",
                        source.name,
                    )
                row = states.get(_windows_key(source))
                if row is None:
                    observation.add("source_sync_pending", "active_work_count", source.name)
                    observation.add(
                        "source_sync_pending",
                        "pending_commit_count",
                        source.name,
                    )
                    continue
                try:
                    sent = int(row["sent_byte_count"])
                except (TypeError, ValueError) as exc:
                    raise ProbeError("SOURCE_SCAN_STATE_INVALID", repr(exc)) from exc
                prefix_hash = str(row["sent_prefix_sha256"] or "")
                if sent < 0 or sent > snapshot.size or not re.fullmatch(r"[0-9a-f]{64}", prefix_hash):
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
                    observation.add(
                        "source_sync_pending",
                        "pending_commit_count",
                        source.name,
                    )
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
            manifest_app="ContainerAudit",
            report_version="container-audit-worker-pc-registration-v1",
            report_manifest_path_field="producer_manifest_path",
            report_app=None,
            producer_role="container_audit",
            stream_name="container_audit_events",
            source_system="container_audit",
            source_transport="legacy_transfer_csv",
            required_report_flags=(
                "manifest_hash_verified",
                "persisted_manifest_hash_verified",
            ),
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
            producer_role="container_audit",
            stream_name="container_audit_events",
            source_system="container_audit",
            source_transport="legacy_transfer_csv",
            container=True,
        )
        _validate_runtime_authority(
            self.database_path,
            producer_identity,
            require_row=relay_row_count > 0,
        )
        self._observe_tray_files(session, observation)
        self._observe_phs_journal(session, observation)
        self._observe_source_coverage(session, observation, delta_ranges)
        if session.snapshot(self.business_database_path).exists:
            _validate_transfer_schema(self.business_database_path)
            _validate_member_exchange_dismissals(self.business_database_path)
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
) -> ContainerAuditAdapter:
    if app != APP:
        raise ProbeError("APP_UNSUPPORTED", "Container_Audit adapter app is invalid")
    return ContainerAuditAdapter.from_trusted_target(target_pc, roots=roots)


__all__ = [
    "BLOCKER_KIND_CATALOG",
    "ContainerAuditAdapter",
    "ContainerAuditTestContext",
    "create_adapter",
]

"""Pure-read Inspection/Rework adapter over the shared product ledger."""

from __future__ import annotations

import os
import stat as stat_module
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..core import (
    DatabaseObservation,
    Observation,
    ObservationSession,
    ProbeError,
    QuerySpec,
    SqlitePlan,
    StatusDomain,
)
from .common import (
    RelayResources,
    TrustedRoots,
    ensure_no_path_override,
    observe_optional_relay,
    production_roots,
    validate_profile,
)


LEDGER_PLAN = SqlitePlan(
    queries=(
        QuerySpec(
            "inspection_sessions_staged",
            "active_session_count",
            "inspection_sessions",
            ("session_id",),
            "status='STAGED'",
            ("status",),
        ),
        QuerySpec(
            "inspection_members_staged",
            "active_work_count",
            "inspection_session_members",
            ("session_id", "product_barcode"),
            "member_state='STAGED'",
            ("member_state",),
        ),
        QuerySpec(
            "residual_sessions_staged",
            "active_session_count",
            "residual_preinspection_sessions",
            ("session_id",),
            "status='STAGED'",
            ("status",),
        ),
        QuerySpec(
            "residual_members_staged",
            "active_work_count",
            "residual_preinspection_members",
            ("session_id", "product_barcode"),
            "member_state='STAGED'",
            ("member_state",),
        ),
        QuerySpec(
            "linked_stage_pending",
            "pending_commit_count",
            "linked_stage_intents",
            ("intent_id",),
            "status='PENDING'",
            ("status",),
        ),
        QuerySpec(
            "linked_stage_operator_review",
            "pending_commit_count",
            "linked_stage_intents",
            ("intent_id",),
            "status IN ('CONFLICT','OPERATOR_REVIEW')",
            ("status",),
        ),
        QuerySpec(
            "linked_completion_pending",
            "pending_commit_count",
            "linked_completion_intents",
            ("intent_id",),
            "status='PENDING'",
            ("status",),
        ),
        QuerySpec(
            "linked_completion_operator_review",
            "pending_commit_count",
            "linked_completion_intents",
            ("intent_id",),
            "status IN ('CONFLICT','OPERATOR_REVIEW')",
            ("status",),
        ),
        QuerySpec(
            "linked_completion_continuation_pending",
            "pending_commit_count",
            "linked_completion_intents",
            ("intent_id",),
            "pooled_continuation_status='PENDING'",
            ("pooled_continuation_status",),
        ),
        QuerySpec(
            "residual_stage_pending",
            "pending_commit_count",
            "residual_stage_intents",
            ("intent_id",),
            "status='PENDING'",
            ("status",),
        ),
        QuerySpec(
            "residual_stage_operator_review",
            "pending_commit_count",
            "residual_stage_intents",
            ("intent_id",),
            "status='OPERATOR_REVIEW'",
            ("status",),
        ),
        QuerySpec(
            "residual_imports_reserved",
            "active_lease_count",
            "residual_import_reservations",
            ("session_id", "bundle_id"),
            "status='RESERVED'",
            ("status",),
        ),
        QuerySpec(
            "rework_imports_reserved",
            "active_lease_count",
            "rework_good_import_reservations",
            ("session_id", "source_bundle_id"),
            "status='RESERVED'",
            ("status",),
        ),
        QuerySpec(
            "residual_bundles_reserved",
            "active_lease_count",
            "residual_bundles",
            ("bundle_id",),
            "bundle_state='RESERVED'",
            ("bundle_state",),
        ),
        QuerySpec(
            "residual_bundles_incomplete",
            "pending_commit_count",
            "residual_bundles",
            ("bundle_id",),
            "label_state IN ('LABEL_PENDING','PRINT_FAILED','ATTACH_PENDING') "
            "OR bundle_state='FINALIZATION_PENDING_REMAINDER_LABEL'",
            ("label_state", "bundle_state"),
        ),
        QuerySpec(
            "products_operator_review",
            "active_work_count",
            "products",
            ("product_barcode",),
            "judgement_status IN ('PENDING','HOLD')",
            ("judgement_status",),
        ),
        QuerySpec(
            "outbox_pending",
            "pending_commit_count",
            "outbox_events",
            ("outbox_id",),
            "status IN ('PENDING','EXPORTING','EXPORTED','FAILED','SYNC_DEGRADED','SYNC_CONFLICT')",
            ("status",),
        ),
        QuerySpec(
            "warehouse_transfer_response_unresolved",
            "pending_commit_count",
            "warehouse_transfer_response_applications",
            ("source_system", "event_id"),
            "status IN ('DEFERRED','PROCESSING')",
            ("status",),
        ),
        QuerySpec(
            "work_order_locks_active",
            "active_lease_count",
            "work_order_locks",
            ("lock_id", "work_order_id"),
            "status='ACTIVE'",
            ("status",),
        ),
    ),
    status_domains=(
        StatusDomain(
            "inspection_sessions", "status", frozenset({"STAGED", "COMPLETED", "CANCELLED"})
        ),
        StatusDomain(
            "inspection_session_members",
            "member_state",
            frozenset({"STAGED", "FINALIZED", "CANCELLED"}),
        ),
        StatusDomain(
            "residual_preinspection_sessions",
            "status",
            frozenset({"STAGED", "COMPLETED", "CANCELLED"}),
        ),
        StatusDomain(
            "residual_preinspection_members",
            "member_state",
            frozenset({"STAGED", "FINALIZED", "CANCELLED"}),
        ),
        StatusDomain(
            "linked_stage_intents",
            "status",
            frozenset({"PENDING", "ACKED", "CONFLICT", "OPERATOR_REVIEW"}),
        ),
        StatusDomain(
            "linked_completion_intents",
            "status",
            frozenset({"PENDING", "ACKED", "CONFLICT", "OPERATOR_REVIEW"}),
        ),
        StatusDomain(
            "linked_completion_intents",
            "pooled_continuation_status",
            frozenset({"NOT_REQUIRED", "PENDING", "ACKED"}),
        ),
        StatusDomain(
            "residual_stage_intents",
            "status",
            frozenset({"PENDING", "ACKED", "REJECTED", "OPERATOR_REVIEW"}),
        ),
        StatusDomain(
            "residual_import_reservations",
            "status",
            frozenset({"RESERVED", "RELEASED", "CONSUMED"}),
        ),
        StatusDomain(
            "rework_good_import_reservations",
            "status",
            frozenset({"RESERVED", "CONSUMED"}),
        ),
        StatusDomain(
            "residual_bundles",
            "label_state",
            frozenset({"LABEL_PENDING", "PRINT_FAILED", "ATTACH_PENDING", "ATTACHED"}),
        ),
        StatusDomain(
            "residual_bundles",
            "bundle_state",
            frozenset(
                {
                    "FINALIZATION_PENDING_REMAINDER_LABEL",
                    "AVAILABLE",
                    "RESERVED",
                    "CONSUMED",
                }
            ),
        ),
        StatusDomain(
            "products",
            "judgement_status",
            frozenset({"NONE", "PENDING", "RETURN", "HOLD"}),
        ),
        StatusDomain(
            "outbox_events",
            "status",
            frozenset(
                {
                    "PENDING",
                    "EXPORTING",
                    "EXPORTED",
                    "SYNCED",
                    "FAILED",
                    "SYNC_DEGRADED",
                    "SYNC_CONFLICT",
                }
            ),
        ),
        StatusDomain("work_order_locks", "status", frozenset({"ACTIVE", "RELEASED"})),
        StatusDomain(
            "warehouse_transfer_response_applications",
            "status",
            frozenset({"DEFERRED", "PROCESSING", "APPLIED", "REJECTED"}),
        ),
    ),
    require_schema_info=True,
    allowed_schema_versions=frozenset({2}),
)


def _regular_descendants(root: Path) -> tuple[Path, ...]:
    """Enumerate state-store files without traversing links or junctions."""

    if not root.is_dir():
        return ()
    files: list[Path] = []
    pending: list[tuple[Path, int]] = [(root, 0)]
    entries_seen = 0
    reparse_flag = int(getattr(stat_module, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    while pending:
        current, depth = pending.pop()
        if depth > 8:
            raise ProbeError("RESOURCE_DIRECTORY_UNBOUNDED", "state directory depth exceeded")
        try:
            entries = sorted(os.scandir(current), key=lambda item: item.name.casefold())
        except OSError as exc:
            raise ProbeError("RESOURCE_SNAPSHOT_ERROR", repr(exc)) from exc
        for entry in entries:
            entries_seen += 1
            if entries_seen > 4096:
                raise ProbeError("RESOURCE_DIRECTORY_UNBOUNDED", "state directory entry count exceeded")
            path = current / entry.name
            try:
                entry_stat = os.lstat(path)
            except OSError as exc:
                raise ProbeError("RESOURCE_SNAPSHOT_ERROR", repr(exc)) from exc
            attributes = int(getattr(entry_stat, "st_file_attributes", 0) or 0)
            if entry.is_symlink() or attributes & reparse_flag:
                raise ProbeError("RESOURCE_REPARSE_POINT", "state reparse entry is forbidden")
            if stat_module.S_ISDIR(entry_stat.st_mode):
                pending.append((path, depth + 1))
            elif stat_module.S_ISREG(entry_stat.st_mode):
                files.append(path)
            else:
                raise ProbeError("RESOURCE_TYPE_INVALID", "state resource is not regular")
    return tuple(sorted(files, key=lambda path: os.fspath(path).casefold()))


def _nonnegative_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _validate_session_state(value: Any) -> Mapping[str, Any]:
    if (
        isinstance(value, Mapping)
        and value.get("version") == "2.0"
        and isinstance(value.get("session"), Mapping)
    ):
        value = value["session"]
    if not isinstance(value, Mapping) or not isinstance(value.get("session_active"), bool):
        raise ProbeError("SESSION_STATE_INVALID", "session recovery shape is invalid")
    for key in (
        "worker_name",
        "worker_role",
        "work_order_id",
        "source_session_id",
        "inspection_session_id",
        "input_tag_id",
        "item_code",
        "item_name",
        "item_spec",
        "phs_label_guidance",
        "recovery_file",
    ):
        if key in value and not isinstance(value.get(key), str):
            raise ProbeError("SESSION_STATE_INVALID", f"{key} has invalid type")
    for key in ("target_quantity", "good_count", "defect_count"):
        if key in value and not _nonnegative_int(value.get(key)):
            raise ProbeError("SESSION_STATE_INVALID", f"{key} has invalid type")
    elapsed = value.get("elapsed_time")
    if elapsed is not None and (
        isinstance(elapsed, bool)
        or not isinstance(elapsed, (int, float))
        or elapsed < 0
    ):
        raise ProbeError("SESSION_STATE_INVALID", "elapsed_time is invalid")
    for key in ("residual_import_state", "residual_split_state"):
        if key in value and not isinstance(value.get(key), Mapping):
            raise ProbeError("SESSION_STATE_INVALID", f"{key} has invalid type")
    if value["session_active"]:
        identifiers = OrderedSessionIdentity.from_state(value)
        if not any(identifiers.values()) or not str(value.get("item_code") or "").strip():
            raise ProbeError("SESSION_STATE_INVALID", "active session identity is incomplete")
    return value


class OrderedSessionIdentity(dict[str, str]):
    @classmethod
    def from_state(cls, value: Mapping[str, Any]) -> "OrderedSessionIdentity":
        return cls(
            (key, str(value.get(key) or "").strip())
            for key in (
                "inspection_session_id",
                "source_session_id",
                "work_order_id",
                "input_tag_id",
            )
        )


@dataclass
class InspectionReworkAdapter:
    app: str
    roots: TrustedRoots

    def __post_init__(self) -> None:
        if self.app not in {"Inspection_worker", "Rework_worker"}:
            raise ProbeError("APP_UNSUPPORTED", "shared-ledger adapter app is invalid")
        self.app_id = "inspection_worker" if self.app == "Inspection_worker" else "rework_worker"
        self.database_path = (
            self.roots.program_data
            / "KMTech"
            / "InspectionWorker"
            / "ledger"
            / "inspection_product_ledger.sqlite3"
        )
        self.database_identity_path = str(self.database_path)
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
        self.session_path = (
            self.roots.program_data
            / "KMTech"
            / "InspectionWorker"
            / "sessions"
            / "current_session.json"
        )
        self.session_root = self.session_path.parent
        self.legacy_session_path = (
            self.roots.apps_root / self.app / "current" / "config" / "session_recovery.json"
        )
        self.legacy_session_root = self.legacy_session_path.parent
        relay_component = "inspection_worker" if self.app == "Inspection_worker" else "rework"
        self.relay_path = (
            self.roots.program_data / "KMTech" / "DirectSync" / relay_component
            / "queue" / "direct_sync_relay.sqlite3"
        )
        relay_root = self.relay_path.parents[1]
        self.relay_resources = RelayResources(
            database_path=self.relay_path,
            producer_manifest_path=relay_root / "producer_manifest.json",
            source_root=relay_root / "ledger_outbox",
            spool_root=relay_root / "spool",
            upload_status_root=relay_root / "upload_status",
            report_root=relay_root / "reports",
            runtime_status_path=relay_root / "status" / "direct_sync_relay_status.json",
            source_suffix=".jsonl",
            source_marker="_outbox",
            expected_pc_id=self.roots.target_pc,
            expected_producer_role="inspection_worker",
            expected_stream_name=(
                "inspection_product_events"
                if self.app == "Inspection_worker"
                else "rework_product_events"
            ),
            expected_source_system="inspection_worker_product_ledger",
            expected_source_transport="outbox",
        )
        self.rework_state_path = (
            self.roots.program_data
            / "KMTech"
            / "ReworkWorker"
            / "logistics"
            / "rework_bundle_state.json"
        )
        self.rework_state_root = self.rework_state_path.parent
        self.rework_candidate_path = (
            self.roots.program_data
            / "KMTech"
            / "ReworkWorker"
            / "candidates"
            / "temporary_ng_cache.json"
        )
        self.rework_candidate_root = self.rework_candidate_path.parent

    def resource_paths(self) -> Sequence[Path]:
        paths = [
            self.database_path,
            self.profile_path,
            self.legacy_profile_path,
            self.session_root,
            self.session_path,
            self.legacy_session_root,
            self.legacy_session_path,
            *self.relay_resources.resource_paths(),
        ]
        if self.app == "Rework_worker":
            paths.extend(
                (
                    self.rework_state_root,
                    self.rework_state_path,
                    self.rework_candidate_root,
                    self.rework_candidate_path,
                )
            )
        return tuple(paths)

    def sqlite_paths(self) -> Sequence[Path]:
        return (self.database_path, self.relay_path)

    def observe(
        self,
        session: ObservationSession,
        observation: Observation,
    ) -> DatabaseObservation:
        runtime_profile = validate_profile(
            session,
            self.profile_path,
            (self.legacy_profile_path,),
        )
        database = session.observe_sqlite(
            self.database_path,
            LEDGER_PLAN,
            observation,
            required=True,
        )
        assert database is not None
        self._observe_session_residue(session, observation)
        self._observe_session(session, observation)
        observe_optional_relay(
            session,
            observation,
            self.relay_resources,
            runtime_profile,
            defect=False,
        )
        if self.app == "Rework_worker":
            self._observe_rework_write_residue(session, observation)
            self._observe_rework_state(session, observation)
            self._observe_candidate_cache(session, observation)
        return database

    def _observe_session_residue(
        self,
        session: ObservationSession,
        observation: Observation,
    ) -> None:
        canonical_root = session.snapshot(self.session_root)
        if canonical_root.exists and canonical_root.kind != "directory":
            raise ProbeError("SESSION_RECOVERY_STORE_INVALID", "session root is not a directory")
        for path in _regular_descendants(self.session_root):
            relative = path.relative_to(self.session_root)
            if relative == Path(self.session_path.name):
                continue
            parts = relative.parts
            if parts and parts[0].casefold() == "quarantine":
                observation.add(
                    "session_recovery_active",
                    "active_session_count",
                    {"scope": "quarantine", "path": relative.as_posix()},
                )
                continue
            name = path.name.casefold()
            prefix = self.session_path.name.casefold() + "."
            if name.startswith(prefix) and name.endswith(".tmp"):
                observation.add(
                    "session_recovery_active",
                    "active_session_count",
                    {"scope": "atomic_write", "path": relative.as_posix()},
                )
                continue
            raise ProbeError(
                "SESSION_RECOVERY_RESIDUE_UNKNOWN",
                "undeclared canonical session-recovery residue",
            )

        legacy_root = session.snapshot(self.legacy_session_root)
        if legacy_root.exists and legacy_root.kind != "directory":
            raise ProbeError("SESSION_RECOVERY_STORE_INVALID", "legacy session root is invalid")
        legacy_name = self.legacy_session_path.name.casefold()
        for path in _regular_descendants(self.legacy_session_root):
            name = path.name.casefold()
            is_temp_sibling = (
                name.startswith(legacy_name + ".")
                or name.startswith("." + legacy_name + ".")
            ) and name.endswith(".tmp")
            if is_temp_sibling:
                observation.add(
                    "session_recovery_active",
                    "active_session_count",
                    {"scope": "legacy_atomic_write", "path": path.name},
                )

    def _observe_rework_write_residue(
        self,
        session: ObservationSession,
        observation: Observation,
    ) -> None:
        for root in (self.rework_state_root, self.rework_candidate_root):
            snapshot = session.snapshot(root)
            if snapshot.exists and snapshot.kind != "directory":
                raise ProbeError("REWORK_STATE_STORE_INVALID", "Rework state root is invalid")

        state_prefix = "." + self.rework_state_path.name.casefold() + "."
        for path in _regular_descendants(self.rework_state_root):
            name = path.name.casefold()
            if name.startswith(state_prefix) and name.endswith(".tmp"):
                observation.add(
                    "rework_result_intent_pending",
                    "pending_commit_count",
                    {"scope": "state_atomic_write", "path": path.name},
                )

        candidate_temp = (self.rework_candidate_path.name + ".tmp").casefold()
        for path in _regular_descendants(self.rework_candidate_root):
            if path.name.casefold() == candidate_temp:
                observation.add(
                    "rework_candidate_result_pending",
                    "pending_commit_count",
                    {"scope": "candidate_atomic_write", "path": path.name},
                )

    def _observe_session(
        self,
        session: ObservationSession,
        observation: Observation,
    ) -> None:
        canonical_exists = session.snapshot(self.session_path).exists
        legacy_exists = session.snapshot(self.legacy_session_path).exists
        if canonical_exists and legacy_exists:
            raise ProbeError("CONFIG_AMBIGUITY", "canonical and legacy session state coexist")
        selected = self.session_path if canonical_exists else self.legacy_session_path
        value = session.read_json(selected, required=False)
        if value is None:
            return
        state = _validate_session_state(value)
        if state["session_active"]:
            observation.add(
                "session_recovery_active",
                "active_session_count",
                OrderedSessionIdentity.from_state(state),
            )

    def _observe_rework_state(
        self,
        session: ObservationSession,
        observation: Observation,
    ) -> None:
        ensure_no_path_override("REWORK_LOGISTICS_STATE_PATH", self.rework_state_path)
        value = session.read_json(self.rework_state_path, required=False)
        if value is None:
            return
        if (
            not isinstance(value, Mapping)
            or value.get("schema_version") != "rework-linked-bundle-state-v1"
            or not isinstance(value.get("bundles"), Mapping)
        ):
            raise ProbeError("REWORK_STATE_INVALID", "bundle state schema is invalid")
        for bundle_id, entry in sorted(value["bundles"].items(), key=lambda item: str(item[0])):
            if not isinstance(bundle_id, str) or not bundle_id or not isinstance(entry, Mapping):
                raise ProbeError("REWORK_STATE_INVALID", "bundle entry is invalid")
            state = entry.get("state")
            if state not in {"CLAIMED", "COMPLETED"}:
                raise ProbeError("REWORK_STATE_UNKNOWN", f"bundle state {state!r}")
            results = entry.get("results", {})
            if not isinstance(results, Mapping):
                raise ProbeError("REWORK_STATE_INVALID", "bundle results are invalid")
            if state == "CLAIMED":
                observation.add("rework_bundle_claimed", "active_work_count", bundle_id)
                if results:
                    observation.add(
                        "rework_completion_pending",
                        "pending_commit_count",
                        bundle_id,
                    )
            intent = entry.get("pending_result_intent")
            if intent is not None:
                if (
                    not isinstance(intent, Mapping)
                    or intent.get("state") != "PREPARED"
                    or not str(intent.get("intent_id") or "").strip()
                ):
                    raise ProbeError("REWORK_STATE_INVALID", "result intent is invalid")
                observation.add(
                    "rework_result_intent_pending",
                    "pending_commit_count",
                    str(intent["intent_id"]),
                )

    def _observe_candidate_cache(
        self,
        session: ObservationSession,
        observation: Observation,
    ) -> None:
        ensure_no_path_override("REWORK_TEMP_NG_CACHE_PATH", self.rework_candidate_path)
        value = session.read_json(self.rework_candidate_path, required=False)
        if value is None:
            return
        if not isinstance(value, Mapping) or value.get("schema_version") != 2:
            raise ProbeError("REWORK_CANDIDATE_CACHE_INVALID", "cache schema is invalid")
        available = value.get("available")
        claims = value.get("claims")
        attempts = value.get("claim_attempt_ids")
        if not isinstance(available, list) or not isinstance(claims, list) or not isinstance(attempts, Mapping):
            raise ProbeError("REWORK_CANDIDATE_CACHE_INVALID", "cache collections are invalid")
        seen_available: set[str] = set()
        for row in available:
            if not isinstance(row, Mapping) or not str(row.get("product_barcode") or "").strip():
                raise ProbeError("REWORK_CANDIDATE_CACHE_INVALID", "available row is invalid")
            barcode = str(row["product_barcode"]).strip()
            if barcode in seen_available:
                raise ProbeError("REWORK_CANDIDATE_CACHE_INVALID", "duplicate available row")
            seen_available.add(barcode)
        seen_claims: set[str] = set()
        for row in claims:
            if not isinstance(row, Mapping) or not str(row.get("product_barcode") or "").strip():
                raise ProbeError("REWORK_CANDIDATE_CACHE_INVALID", "claim row is invalid")
            barcode = str(row["product_barcode"]).strip()
            if barcode in seen_claims:
                raise ProbeError("REWORK_CANDIDATE_CACHE_INVALID", "duplicate claim row")
            seen_claims.add(barcode)
            observation.add("rework_candidate_claim", "active_lease_count", barcode)
            pending = row.get("_local_result_pending", False)
            if not isinstance(pending, bool):
                raise ProbeError("REWORK_CANDIDATE_CACHE_INVALID", "pending marker is invalid")
            if pending:
                observation.add(
                    "rework_candidate_result_pending",
                    "pending_commit_count",
                    barcode,
                )
        for barcode, attempt_id in sorted(attempts.items(), key=lambda item: str(item[0])):
            normalized_barcode = str(barcode or "").strip()
            normalized_attempt = str(attempt_id or "").strip()
            if not normalized_barcode or not normalized_attempt:
                raise ProbeError("REWORK_CANDIDATE_CACHE_INVALID", "claim attempt is invalid")
            if normalized_barcode not in seen_claims:
                observation.add(
                    "rework_candidate_claim_attempt",
                    "active_lease_count",
                    normalized_attempt,
                )


def create_adapter(
    app: str,
    target_pc: str,
    *,
    roots: TrustedRoots | None = None,
) -> InspectionReworkAdapter:
    return InspectionReworkAdapter(app=app, roots=roots or production_roots(target_pc))


__all__ = ["InspectionReworkAdapter", "LEDGER_PLAN", "create_adapter"]

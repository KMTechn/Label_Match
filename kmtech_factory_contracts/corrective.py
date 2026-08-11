"""Strict corrective-revision document validation for Factory Contract 1.0.3.

The validators are deliberately pure: callers supply JSON values and the code
only compares them with the immutable bundled compatibility declarations.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping, Sequence

from .bundle import (
    CONTRACT_BUNDLE_CORRECTIVE_REVISION,
    CONTRACT_BUNDLE_SHA256,
    CONTRACT_BUNDLE_VERSION,
    MINIMUM_INSTALLER_VERSION,
    MINIMUM_VERIFIER_VERSION,
    bundle_root,
)
from .canonical import canonical_sha256, load_json_strict
from .errors import FactoryContractError


INTEGRATED_TARGET_PC = "TEST1"
INTEGRATED_WORKFLOW_ID = "test1-five-app-integrated"
INTEGRATED_APP_ORDER = (
    "Inspection_worker",
    "Rework_worker",
    "Defect_Inspection",
    "Container_Audit",
    "Label_Match",
)
APP_NAME_TO_ID = {
    "Container_Audit": "container_audit",
    "Defect_Inspection": "defect_inspection",
    "Inspection_worker": "inspection_worker",
    "Label_Match": "label_match",
    "Rework_worker": "rework_worker",
}
APP_ID_TO_NAME = {app_id: app for app, app_id in APP_NAME_TO_ID.items()}
APP_IDS = frozenset(APP_ID_TO_NAME)
BROAD_ROOT_FIELDS = ("install_root", "data_root", "log_root")
LIFECYCLE_ACCESS = frozenset({"initialize", "migrate", "restore"})

DOCUMENT_SCHEMAS = {
    "active-work-evidence": "active-work-evidence.schema.json",
    "coinstall-matrix": "coinstall-matrix.schema.json",
    "db-transition": "db-transition.schema.json",
    "installer-field-layout": "installer-field-layout.schema.json",
    "installer-plan": "installer-plan.schema.json",
    "preflight-report": "preflight-report.schema.json",
    "resource-namespaces": "resource-namespaces.schema.json",
    "rollback-receipt": "rollback-receipt.schema.json",
    "rollback-request": "rollback-request.schema.json",
    "snapshot": "snapshot.schema.json",
    "transaction-report": "transaction-report.schema.json",
}


def _fail(code: str, message: str, **details: Any) -> None:
    raise FactoryContractError(code, message, details=details)


def _normalized_kind(kind: str) -> str:
    normalized = str(kind or "").strip().lower().replace("_", "-")
    if normalized not in DOCUMENT_SCHEMAS:
        _fail(
            "CONTRACT_DOCUMENT_KIND_INVALID",
            f"unsupported corrective document kind: {kind!r}",
            supported_kinds=sorted(DOCUMENT_SCHEMAS),
        )
    return normalized


def _load_schema(kind: str, *, root: Path | None = None) -> Mapping[str, Any]:
    schema_path = (root or bundle_root()) / "schemas" / DOCUMENT_SCHEMAS[kind]
    schema = load_json_strict(schema_path)
    if not isinstance(schema, Mapping):
        _fail("CONTRACT_SCHEMA_INVALID", f"schema must be an object: {schema_path.name}")
    return schema


def _schema_validate(kind: str, document: Any, *, root: Path | None = None) -> None:
    try:
        from jsonschema import Draft202012Validator, FormatChecker
    except ImportError as exc:
        raise FactoryContractError(
            "CONTRACT_VALIDATOR_DEPENDENCY_MISSING",
            "jsonschema is required for corrective contract validation",
        ) from exc
    schema = _load_schema(kind, root=root)
    Draft202012Validator.check_schema(schema)
    errors = sorted(
        Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(document),
        key=lambda error: tuple(str(part) for part in error.absolute_path),
    )
    if errors:
        first = errors[0]
        path = ".".join(str(part) for part in first.absolute_path) or "<root>"
        _fail(
            "CONTRACT_DOCUMENT_INVALID",
            f"{kind} failed strict schema validation at {path}: {first.message}",
            document_kind=kind,
            field=path,
            error_count=len(errors),
        )


def _precheck_resource_failures(document: Any) -> None:
    if not isinstance(document, Mapping):
        return
    shared = document.get("shared_resources")
    if not isinstance(shared, Sequence) or isinstance(shared, (str, bytes)):
        return
    for row in shared:
        if not isinstance(row, Mapping):
            continue
        resource_id = str(row.get("resource_id") or "<unknown>")
        owner = row.get("owner_app_id")
        if not isinstance(owner, str) or not owner.strip():
            _fail(
                "RESOURCE_OWNER_MISSING",
                f"shared resource has no explicit owner: {resource_id}",
                resource_id=resource_id,
            )
        migrators = [
            str(participant.get("app_id") or "")
            for participant in row.get("participants", [])
            if isinstance(participant, Mapping)
            and isinstance(participant.get("access"), list)
            and "migrate" in participant["access"]
        ]
        declared_migrator = row.get("migrator_app_id")
        if len(migrators) > 1 or (migrators and migrators != [declared_migrator]):
            _fail(
                "DUAL_DB_MIGRATORS",
                f"shared database must have exactly one declared migrator: {resource_id}",
                resource_id=resource_id,
                migrators=migrators,
            )


def _precheck_empty_rollback(kind: str, document: Any) -> None:
    if not isinstance(document, Mapping):
        return
    if kind in {"preflight-report", "transaction-report"}:
        rollback = document.get("rollback_receipt")
        if not isinstance(rollback, Mapping) or not rollback:
            _fail(
                "ROLLBACK_RECEIPT_EMPTY",
                f"{kind} must carry an explicit rollback disposition",
            )
    if kind == "rollback-receipt" and not document:
        _fail("ROLLBACK_RECEIPT_EMPTY", "rollback receipt cannot be empty")


def _validate_matrix_semantics(matrix: Mapping[str, Any]) -> None:
    pairs = matrix["pairs"]
    expected_pairs = {
        tuple(sorted((left, right)))
        for index, left in enumerate(sorted(APP_IDS))
        for right in sorted(APP_IDS)[index + 1 :]
    }
    actual_pairs = [tuple(row["apps"]) for row in pairs]
    if set(actual_pairs) != expected_pairs or len(actual_pairs) != len(set(actual_pairs)):
        _fail(
            "COMPATIBILITY_MATRIX_INVALID",
            "default matrix must contain each unordered app pair exactly once",
        )
    if any(
        row["status"] != "blocked"
        or row["reason_code"] != "ROLE_PC_SEPARATION_REQUIRED"
        or row["shared_resources"]
        for row in pairs
    ):
        _fail(
            "ROLE_PC_SEPARATION_INVALID",
            "default role-PC policy must remain blocked without shared-resource waivers",
        )
    workflow = matrix["integrated_workflows"][0]
    if (
        workflow["target_pc"] != INTEGRATED_TARGET_PC
        or workflow["workflow_id"] != INTEGRATED_WORKFLOW_ID
        or tuple(workflow["ordered_apps"]) != INTEGRATED_APP_ORDER
        or workflow["rollback_mode"] != "group-deferred"
    ):
        _fail(
            "INTEGRATED_WORKFLOW_INVALID",
            "the only integrated exception must be the exact TEST1 five-app workflow",
        )


def _duplicate_values(rows: Sequence[Mapping[str, Any]], field: str) -> dict[str, list[str]]:
    owners: dict[str, list[str]] = {}
    for row in rows:
        value = str(row.get(field) or "").replace("\\", "/").rstrip("/").casefold()
        owners.setdefault(value, []).append(str(row.get("app_id") or ""))
    return {value: app_ids for value, app_ids in owners.items() if value and len(app_ids) > 1}


def _validate_resource_semantics(resources: Mapping[str, Any]) -> None:
    rows = resources["resources"]
    row_ids = [row["app_id"] for row in rows]
    if row_ids != sorted(APP_IDS):
        _fail(
            "RESOURCE_DECLARATION_INVALID",
            "resource rows must cover the five apps once in canonical ID order",
        )
    for field in BROAD_ROOT_FIELDS:
        duplicated = _duplicate_values(rows, field)
        if duplicated:
            _fail(
                "BROAD_RESOURCE_SHARING_FORBIDDEN",
                f"broad resource roots may not be shared: {field}",
                field=field,
                collisions=duplicated,
            )
    shared_rows = resources["shared_resources"]
    shared_ids = [row["resource_id"] for row in shared_rows]
    if len(shared_ids) != len(set(shared_ids)):
        _fail("RESOURCE_DECLARATION_INVALID", "shared resource IDs must be unique")
    rows_by_app = {row["app_id"]: row for row in rows}
    declared_shared_paths: set[str] = set()
    for shared in shared_rows:
        resource_id = shared["resource_id"]
        owner = shared["owner_app_id"]
        participants = shared["participants"]
        participant_ids = [row["app_id"] for row in participants]
        if (
            set(participant_ids) != {"inspection_worker", "rework_worker"}
            or len(participant_ids) != len(set(participant_ids))
            or owner not in participant_ids
        ):
            _fail(
                "RESOURCE_OWNER_MISSING",
                f"owner and the exact Inspection/Rework participant pair are required: {resource_id}",
                resource_id=resource_id,
            )
        lifecycle_owners = {
            shared["initializer_app_id"],
            shared["migrator_app_id"],
            shared["restorer_app_id"],
        }
        if lifecycle_owners != {owner}:
            _fail(
                "RESOURCE_LIFECYCLE_OWNER_INVALID",
                f"initialize, migrate, and restore authority must remain with the owner: {resource_id}",
                resource_id=resource_id,
            )
        migrators = [row["app_id"] for row in participants if "migrate" in row["access"]]
        if migrators != [owner]:
            _fail(
                "DUAL_DB_MIGRATORS",
                f"shared database must expose one owner migrator: {resource_id}",
                resource_id=resource_id,
                migrators=migrators,
            )
        for participant in participants:
            app_id = participant["app_id"]
            access = set(participant["access"])
            if app_id == owner:
                if not LIFECYCLE_ACCESS <= access:
                    _fail(
                        "RESOURCE_LIFECYCLE_OWNER_INVALID",
                        f"owner lacks lifecycle authority: {resource_id}",
                    )
            elif access & LIFECYCLE_ACCESS:
                _fail(
                    "RESOURCE_LIFECYCLE_OWNER_INVALID",
                    f"non-owner has lifecycle authority: {resource_id}",
                    app_id=app_id,
                )
            if app_id == "rework_worker" and (
                access != {"read", "write"}
                or participant["rollback_mode"] != "group-deferred"
            ):
                _fail(
                    "REWORK_RESOURCE_AUTHORITY_INVALID",
                    "Rework must remain a limited ledger writer with group-deferred rollback",
                )
            if rows_by_app[app_id]["state_db"].casefold() != shared["path"].casefold():
                _fail(
                    "RESOURCE_PATH_MISMATCH",
                    f"participant state DB differs from the shared-resource path: {app_id}",
                )
        if owner != "inspection_worker":
            _fail(
                "RESOURCE_OWNER_INVALID",
                "Inspection_worker must own the Inspection/Rework ledger",
            )
        declared_shared_paths.add(shared["path"].replace("\\", "/").casefold())
    duplicated_state = _duplicate_values(rows, "state_db")
    undeclared = {
        path: app_ids for path, app_ids in duplicated_state.items() if path not in declared_shared_paths
    }
    if undeclared:
        _fail(
            "RESOURCE_SHARING_UNDECLARED",
            "cross-app state DB sharing must name an exact shared resource",
            collisions=undeclared,
        )


def _validate_layout_semantics(
    layout: Mapping[str, Any],
    resources: Mapping[str, Any] | None = None,
) -> None:
    rows = layout["apps"]
    if [row["app_id"] for row in rows] != sorted(APP_IDS):
        _fail(
            "INSTALLER_FIELD_LAYOUT_INVALID",
            "installer layout must cover the five apps once in canonical ID order",
        )
    if (
        layout["minimum_installer_version"] != MINIMUM_INSTALLER_VERSION
        or layout["minimum_verifier_version"] != MINIMUM_VERIFIER_VERSION
    ):
        _fail(
            "INSTALLER_FIELD_LAYOUT_INVALID",
            "corrective installer/verifier minimum differs from the runtime constants",
        )
    for row in rows:
        if APP_NAME_TO_ID[row["repository_name"]] != row["app_id"]:
            _fail(
                "INSTALLER_FIELD_LAYOUT_INVALID",
                f"repository/app identity mismatch: {row['app_id']}",
            )
        if row["install_root"] != f"C:/KMTech/Apps/{row['repository_name']}/current":
            _fail(
                "INSTALLER_FIELD_LAYOUT_INVALID",
                f"non-canonical install root: {row['app_id']}",
            )
    if resources is not None:
        resources_by_app = {row["app_id"]: row for row in resources["resources"]}
        compared = ("app_version", "install_root", "state_db", "task_name", "task_action")
        for row in rows:
            resource = resources_by_app[row["app_id"]]
            mismatched = [field for field in compared if resource[field] != row[field]]
            if mismatched:
                _fail(
                    "INSTALLER_FIELD_LAYOUT_INVALID",
                    f"layout differs from resource declaration: {row['app_id']}",
                    mismatched_fields=mismatched,
                )


def validate_compatibility_contracts(
    matrix: Mapping[str, Any] | None = None,
    resources: Mapping[str, Any] | None = None,
    layout: Mapping[str, Any] | None = None,
    *,
    root: Path | None = None,
) -> dict[str, Any]:
    contract_root = root or bundle_root()
    matrix = matrix or load_json_strict(contract_root / "compatibility" / "coinstall-matrix.json")
    resources = resources or load_json_strict(
        contract_root / "compatibility" / "resource-namespaces.json"
    )
    layout = layout or load_json_strict(
        contract_root / "compatibility" / "installer-field-layout.json"
    )
    _precheck_resource_failures(resources)
    _schema_validate("coinstall-matrix", matrix, root=contract_root)
    _schema_validate("resource-namespaces", resources, root=contract_root)
    _schema_validate("installer-field-layout", layout, root=contract_root)
    _validate_matrix_semantics(matrix)
    _validate_resource_semantics(resources)
    _validate_layout_semantics(layout, resources)
    workflow = matrix["integrated_workflows"][0]
    declared_ids = {row["resource_id"] for row in resources["shared_resources"]}
    if set(workflow["shared_resource_ids"]) != declared_ids:
        _fail(
            "COMPATIBILITY_MATRIX_INVALID",
            "integrated workflow shared-resource IDs differ from resource declarations",
        )
    return {
        "status": "PASS",
        "corrective_revision": CONTRACT_BUNDLE_CORRECTIVE_REVISION,
        "integrated_target_pc": INTEGRATED_TARGET_PC,
        "integrated_app_order": list(INTEGRATED_APP_ORDER),
        "shared_resource_count": len(declared_ids),
    }


def _validate_app_identity_rows(rows: Sequence[Mapping[str, Any]]) -> None:
    seen_ids: set[str] = set()
    for row in rows:
        app_id = row["app_id"]
        if APP_NAME_TO_ID[row["app"]] != app_id or app_id in seen_ids:
            _fail(
                "APP_IDENTITY_INVALID",
                "app names, IDs, and row uniqueness must agree",
                app_id=app_id,
            )
        seen_ids.add(app_id)


def _validate_integrated_scope(document: Mapping[str, Any], app_names: Sequence[str]) -> None:
    if document["workflow_mode"] == "integrated":
        if document["target_pc"] != INTEGRATED_TARGET_PC or tuple(app_names) != INTEGRATED_APP_ORDER:
            _fail(
                "INTEGRATED_WORKFLOW_SCOPE_INVALID",
                "integrated mode is allowed only for the exact TEST1 five-app order",
            )


def _validate_plan_semantics(
    plan: Mapping[str, Any],
    matrix: Mapping[str, Any],
    resources: Mapping[str, Any],
    layout: Mapping[str, Any],
) -> None:
    artifacts = plan["artifacts"]
    _validate_app_identity_rows(artifacts)
    artifact_names = [row["app"] for row in artifacts]
    if artifact_names != plan["ordered_apps"]:
        _fail("INSTALLER_PLAN_ORDER_INVALID", "artifact rows must follow ordered_apps exactly")
    _validate_integrated_scope(plan, artifact_names)
    if plan["workflow_mode"] == "integrated":
        if (
            plan["workflow_id"] != INTEGRATED_WORKFLOW_ID
            or plan["rollback_policy"]["mode"] != "group-deferred"
            or not plan["rollback_policy"]["group_id"]
            or plan["rollback_policy"]["requires_complete_group_receipt"] is not True
        ):
            _fail(
                "INTEGRATED_WORKFLOW_INVALID",
                "TEST1 integrated plans require one group-deferred rollback receipt",
            )
    elif plan["workflow_id"] is not None:
        _fail("INSTALLER_PLAN_INVALID", "independent plans may not name an integrated workflow")
    expected_compatibility = {
        "coinstall_matrix_sha256": canonical_sha256(matrix),
        "resource_namespaces_sha256": canonical_sha256(resources),
        "installer_field_layout_sha256": canonical_sha256(layout),
    }
    if plan["compatibility_identity"] != expected_compatibility:
        _fail(
            "COMPATIBILITY_IDENTITY_MISMATCH",
            "installer plan is not bound to the exact matrix/resource/layout documents",
            expected=expected_compatibility,
            actual=plan["compatibility_identity"],
        )
    if plan["contract_bundle_sha256"] != CONTRACT_BUNDLE_SHA256:
        _fail(
            "CONTRACT_HASH_MISMATCH",
            "installer plan bundle identity differs from the loaded canonical bundle",
        )
    binding = resources["shared_resources"][0]["dependency_binding"]
    rework = next((row for row in artifacts if row["app_id"] == "rework_worker"), None)
    if rework is not None and rework["dependency"] != {
        "kind": binding["kind"],
        "commit": binding["commit"],
        "sha256": binding["sha256"],
    }:
        _fail(
            "DEPENDENCY_SHA_MISMATCH",
            "Rework dependency identity differs from the canonical Inspection provider binding",
            expected_sha256=binding["sha256"],
            actual_sha256=rework["dependency"].get("sha256"),
        )


def _validate_phase_and_mutation_semantics(document: Mapping[str, Any]) -> None:
    phases = document["phase_results"]
    if [row["phase_index"] for row in phases] != list(range(1, len(phases) + 1)):
        _fail("PHASE_ORDER_INVALID", "phase_results must be contiguous and ordered from 1")
    inventory = document["mutation_inventory"]
    if inventory["exact_count"] != len(inventory["items"]):
        _fail(
            "MUTATION_INVENTORY_INVALID",
            "exact mutation count differs from the ordered mutation inventory",
        )
    apps = document["apps"]
    _validate_app_identity_rows(apps)
    _validate_integrated_scope(document, [row["app"] for row in apps])
    if document["bundle_identity"]["contract_bundle_sha256"] != CONTRACT_BUNDLE_SHA256:
        _fail("CONTRACT_HASH_MISMATCH", "report bundle identity differs from the canonical bundle")
    rollback = document["rollback_receipt"]
    if document["workflow_mode"] == "integrated" and (
        rollback["status"] not in {"DEFERRED", "PASS", "FAILED"}
        or not rollback["group_id"]
    ):
        _fail(
            "ROLLBACK_RECEIPT_INVALID",
            "integrated workflow requires an explicit group rollback disposition",
        )
    if document.get("action") == "Rollback":
        if (
            rollback["status"] not in {"PASS", "FAILED"}
            or rollback["parity"] is None
            or not rollback["receipt_path"]
            or not rollback["receipt_sha256"]
        ):
            _fail(
                "ROLLBACK_RECEIPT_EMPTY",
                "rollback transactions require a complete receipt and parity result",
            )


def _validate_db_transition_semantics(document: Mapping[str, Any]) -> None:
    if document["to_schema_version"] < document["from_schema_version"]:
        _fail("DB_TRANSITION_INVALID", "database schema version may not regress")
    if document["status"] == "PROVEN" and document["error_code"] is not None:
        _fail("DB_TRANSITION_INVALID", "a proven DB transition may not carry an error code")
    if document["status"] == "FAILED" and not document["error_code"]:
        _fail("DB_TRANSITION_INVALID", "a failed DB transition requires an error code")


def _validate_active_work_semantics(
    document: Mapping[str, Any],
    *,
    root: Path,
) -> None:
    _validate_app_identity_rows([document])
    resources = load_json_strict(root / "compatibility" / "resource-namespaces.json")
    resource = next(
        row for row in resources["resources"] if row["app_id"] == document["app_id"]
    )
    if document["database_identity"]["path"].casefold() != resource["state_db"].casefold():
        _fail(
            "ACTIVE_WORK_DATABASE_IDENTITY_MISMATCH",
            "active-work evidence DB identity differs from the canonical app resource",
        )


def _validate_rollback_request_semantics(document: Mapping[str, Any]) -> None:
    _validate_integrated_scope(document, document["ordered_apps"])
    if document["workflow_mode"] == "integrated" and (
        document["rollback_mode"] != "group-deferred" or not document["group_id"]
    ):
        _fail(
            "ROLLBACK_REQUEST_INVALID",
            "integrated rollback requests must bind the whole deferred group",
        )


def _validate_rollback_receipt_semantics(document: Mapping[str, Any]) -> None:
    _validate_integrated_scope(document, document["ordered_apps"])
    if document["workflow_mode"] == "integrated" and (
        document["rollback_mode"] != "group-deferred" or not document["group_id"]
    ):
        _fail(
            "ROLLBACK_RECEIPT_INVALID",
            "integrated rollback receipts must bind the whole deferred group",
        )
    parity_rows = [row["parity"] for row in document["restored_inventory"]]
    if document["status"] == "PASS":
        if (
            not document["parity"]["match"]
            or not all(parity_rows)
            or document["error_code"] is not None
        ):
            _fail(
                "ROLLBACK_PARITY_INVALID",
                "PASS requires exact top-level and per-resource rollback parity",
            )
    elif not document["error_code"]:
        _fail("ROLLBACK_RECEIPT_INVALID", "FAILED rollback requires an error code")


def validate_corrective_document(
    kind: str,
    document: Any,
    *,
    root: Path | None = None,
) -> dict[str, Any]:
    """Validate one strict corrective document and its executable invariants."""

    normalized = _normalized_kind(kind)
    _precheck_empty_rollback(normalized, document)
    if normalized == "resource-namespaces":
        _precheck_resource_failures(document)
    _schema_validate(normalized, document, root=root)
    if not isinstance(document, Mapping):  # defensive; schema already enforces this
        _fail("CONTRACT_DOCUMENT_INVALID", f"{normalized} must be an object")

    contract_root = root or bundle_root()
    if normalized in {"coinstall-matrix", "resource-namespaces", "installer-field-layout"}:
        matrix = (
            document
            if normalized == "coinstall-matrix"
            else load_json_strict(contract_root / "compatibility" / "coinstall-matrix.json")
        )
        resources = (
            document
            if normalized == "resource-namespaces"
            else load_json_strict(contract_root / "compatibility" / "resource-namespaces.json")
        )
        layout = (
            document
            if normalized == "installer-field-layout"
            else load_json_strict(contract_root / "compatibility" / "installer-field-layout.json")
        )
        validate_compatibility_contracts(matrix, resources, layout, root=contract_root)
    elif normalized == "installer-plan":
        matrix = load_json_strict(contract_root / "compatibility" / "coinstall-matrix.json")
        resources = load_json_strict(
            contract_root / "compatibility" / "resource-namespaces.json"
        )
        layout = load_json_strict(
            contract_root / "compatibility" / "installer-field-layout.json"
        )
        validate_compatibility_contracts(matrix, resources, layout, root=contract_root)
        _validate_plan_semantics(document, matrix, resources, layout)
    elif normalized in {"preflight-report", "transaction-report"}:
        _validate_phase_and_mutation_semantics(document)
    elif normalized == "active-work-evidence":
        _validate_active_work_semantics(document, root=contract_root)
    elif normalized == "db-transition":
        _validate_db_transition_semantics(document)
    elif normalized == "rollback-request":
        _validate_rollback_request_semantics(document)
    elif normalized == "rollback-receipt":
        _validate_rollback_receipt_semantics(document)

    return {
        "status": "PASS",
        "document_kind": normalized,
        "document_sha256": canonical_sha256(document),
        "contract_bundle_version": CONTRACT_BUNDLE_VERSION,
        "corrective_revision": CONTRACT_BUNDLE_CORRECTIVE_REVISION,
    }


def validate_corrective_file(kind: str, path: Path, *, root: Path | None = None) -> dict[str, Any]:
    return validate_corrective_document(kind, load_json_strict(path), root=root)


__all__ = [
    "APP_ID_TO_NAME",
    "APP_NAME_TO_ID",
    "DOCUMENT_SCHEMAS",
    "INTEGRATED_APP_ORDER",
    "INTEGRATED_TARGET_PC",
    "INTEGRATED_WORKFLOW_ID",
    "validate_compatibility_contracts",
    "validate_corrective_document",
    "validate_corrective_file",
]

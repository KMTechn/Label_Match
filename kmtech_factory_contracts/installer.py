"""Pure offline installer preflight and post-apply readiness evaluation.

The functions in this module inspect caller-supplied evidence only.  They do
not access the registry, credential stores, scheduled tasks, databases, or the
network, and they never mutate installer state.
"""

from __future__ import annotations

import ntpath
import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from .bundle import MINIMUM_INSTALLER_VERSION
from .errors import FactoryContractError


INSTALLER_CONTRACT_VERSION = "factory-installer-v1"
READINESS_CONTRACT_VERSION = "factory-install-readiness-v1"


BUILD_COMPATIBILITY_FIELDS = {
    "matrix_schema_version",
    "app_id",
    "app_version",
    "source_commit",
    "source_tree",
    "build_identity_sha256",
    "contract_bundle_sha256",
    "dependency",
    "db_schema_supported",
    "server_api_contract_versions",
    "event_contract_version",
    "manifest_contract_version",
    "minimum_installer_version",
    "resources",
    "coinstall_with",
}

RESOURCE_FIELDS = {
    "profile_id",
    "credential_target",
    "task_name",
    "task_action",
    "task_principal",
    "install_root",
    "data_root",
    "state_db",
    "log_root",
    "machine_identity_scope",
}

RESOURCE_COLLISION_CODES = {
    "profile_id": "PROFILE_ID_COLLISION",
    "credential_target": "CREDENTIAL_TARGET_COLLISION",
    "task_name": "TASK_NAME_COLLISION",
    "install_root": "INSTALL_ROOT_COLLISION",
    "data_root": "DATA_ROOT_COLLISION",
    "state_db": "STATE_ROOT_COLLISION",
    "log_root": "LOG_ROOT_COLLISION",
    "machine_identity_scope": "MACHINE_IDENTITY_SCOPE_COLLISION",
}

COINSTALL_RULE_FIELDS = {
    "app_id",
    "app_version",
    "status",
    "reason_code",
    "shared_resources",
}

SAME_APP_EXACT_FIELDS = (
    "source_commit",
    "source_tree",
    "build_identity_sha256",
    "contract_bundle_sha256",
    "dependency",
    "db_schema_supported",
    "server_api_contract_versions",
    "event_contract_version",
    "manifest_contract_version",
    "minimum_installer_version",
    "coinstall_with",
)

PATH_RESOURCE_FIELDS = {
    "profile_id",
    "install_root",
    "data_root",
    "state_db",
    "log_root",
}

INSPECTION_REWORK_APPS = {"inspection_worker", "rework_worker"}
INSPECTION_REWORK_SHARED_RESOURCES = {"data_root", "log_root", "state_db"}
REASON_CODE_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]+$")


@dataclass(frozen=True)
class PreflightIssue:
    code: str
    field: str
    conflicting_app_id: str
    message: str

    def as_dict(self) -> dict[str, str]:
        return {
            "code": self.code,
            "field": self.field,
            "conflicting_app_id": self.conflicting_app_id,
            "message": self.message,
        }


def _issue(
    code: str,
    field: str,
    app_id: str,
    message: str,
) -> PreflightIssue:
    return PreflightIssue(code, field, app_id or "<unknown>", message)


def _is_non_empty_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _is_hex(value: Any, length: int) -> bool:
    return bool(
        isinstance(value, str)
        and len(value) == length
        and value == value.lower()
        and all(character in "0123456789abcdef" for character in value)
    )


def _normalize_resource(field: str, value: Any) -> str:
    text = str(value or "").strip()
    if field in PATH_RESOURCE_FIELDS:
        return ntpath.normcase(ntpath.normpath(text.replace("/", "\\")))
    if field == "task_action":
        return text.replace("\\", "/").casefold()
    return text.casefold()


def _validate_dependency(value: Any) -> bool:
    if not isinstance(value, Mapping) or set(value) != {"kind", "commit", "sha256"}:
        return False
    kind = value.get("kind")
    commit = value.get("commit")
    sha256 = value.get("sha256")
    if not _is_non_empty_text(kind):
        return False
    if commit is not None and not _is_hex(commit, 40):
        return False
    if sha256 is not None and not _is_hex(sha256, 64):
        return False
    if kind != "none" and (commit is None or sha256 is None):
        return False
    return True


def _validate_coinstall_rows(value: Any) -> bool:
    if not isinstance(value, list):
        return False
    keys: list[tuple[str, str]] = []
    for row in value:
        if not isinstance(row, Mapping) or set(row) != COINSTALL_RULE_FIELDS:
            return False
        app_id = row.get("app_id")
        app_version = row.get("app_version")
        reason_code = row.get("reason_code")
        shared = row.get("shared_resources")
        if not _is_non_empty_text(app_id) or not _is_non_empty_text(app_version):
            return False
        if row.get("status") not in {"allowed", "conditional"}:
            return False
        if not isinstance(reason_code, str) or not REASON_CODE_PATTERN.fullmatch(reason_code):
            return False
        if (
            not isinstance(shared, list)
            or any(field not in RESOURCE_FIELDS for field in shared)
            or len(shared) != len(set(shared))
            or shared != sorted(shared)
        ):
            return False
        keys.append((app_id, app_version))
    return len(keys) == len(set(keys)) and keys == sorted(keys)


def _declaration_issues(
    declaration: Any,
    *,
    owner: str,
) -> list[PreflightIssue]:
    if not isinstance(declaration, Mapping):
        return [
            _issue(
                "COMPATIBILITY_DECLARATION_INVALID",
                "declaration",
                owner,
                "build compatibility declaration must be an object",
            )
        ]
    app_id = str(declaration.get("app_id") or owner)
    issues: list[PreflightIssue] = []
    if set(declaration) != BUILD_COMPATIBILITY_FIELDS:
        issues.append(
            _issue(
                "COMPATIBILITY_DECLARATION_INVALID",
                "declaration",
                app_id,
                "build compatibility fields do not match the installer contract",
            )
        )
    if declaration.get("matrix_schema_version") != 1:
        issues.append(
            _issue(
                "COMPATIBILITY_DECLARATION_INVALID",
                "matrix_schema_version",
                app_id,
                "unsupported build compatibility schema",
            )
        )
    for field in (
        "app_id",
        "app_version",
        "event_contract_version",
        "manifest_contract_version",
        "minimum_installer_version",
    ):
        if not _is_non_empty_text(declaration.get(field)):
            issues.append(
                _issue(
                    "COMPATIBILITY_DECLARATION_INVALID",
                    field,
                    app_id,
                    f"{field} must be a non-empty string",
                )
            )
    if declaration.get("minimum_installer_version") != MINIMUM_INSTALLER_VERSION:
        issues.append(
            _issue(
                "COMPATIBILITY_DECLARATION_INVALID",
                "minimum_installer_version",
                app_id,
                "minimum installer version does not match this contract bundle",
            )
        )
    for field, length in (
        ("source_commit", 40),
        ("source_tree", 40),
        ("build_identity_sha256", 64),
        ("contract_bundle_sha256", 64),
    ):
        if not _is_hex(declaration.get(field), length):
            issues.append(
                _issue(
                    "COMPATIBILITY_DECLARATION_INVALID",
                    field,
                    app_id,
                    f"{field} has no exact lowercase hexadecimal identity",
                )
            )
    if not _validate_dependency(declaration.get("dependency")):
        issues.append(
            _issue(
                "COMPATIBILITY_DECLARATION_INVALID",
                "dependency",
                app_id,
                "dependency identity is incomplete or malformed",
            )
        )
    db_schema = declaration.get("db_schema_supported")
    if not (
        isinstance(db_schema, Mapping)
        and set(db_schema) == {"minimum", "maximum"}
        and type(db_schema.get("minimum")) is int
        and type(db_schema.get("maximum")) is int
        and db_schema["minimum"] >= 0
        and db_schema["minimum"] <= db_schema["maximum"]
    ):
        issues.append(
            _issue(
                "COMPATIBILITY_DECLARATION_INVALID",
                "db_schema_supported",
                app_id,
                "supported database schema range is incomplete or invalid",
            )
        )
    server_versions = declaration.get("server_api_contract_versions")
    if not (
        isinstance(server_versions, list)
        and server_versions
        and all(_is_non_empty_text(version) for version in server_versions)
        and len(server_versions) == len(set(server_versions))
    ):
        issues.append(
            _issue(
                "COMPATIBILITY_DECLARATION_INVALID",
                "server_api_contract_versions",
                app_id,
                "server API contract versions are incomplete or duplicated",
            )
        )
    resources = declaration.get("resources")
    if not isinstance(resources, Mapping) or set(resources) != RESOURCE_FIELDS:
        issues.append(
            _issue(
                "COMPATIBILITY_DECLARATION_INVALID",
                "resources",
                app_id,
                "resource declaration fields do not match the installer contract",
            )
        )
    else:
        for field in sorted(RESOURCE_FIELDS):
            if not _is_non_empty_text(resources.get(field)):
                issues.append(
                    _issue(
                        "COMPATIBILITY_DECLARATION_INVALID",
                        f"resources.{field}",
                        app_id,
                        f"{field} must be a non-empty string",
                    )
                )
    if not _validate_coinstall_rows(declaration.get("coinstall_with")):
        issues.append(
            _issue(
                "COMPATIBILITY_DECLARATION_INVALID",
                "coinstall_with",
                app_id,
                "co-install declarations are incomplete, duplicated, or unsorted",
            )
        )
    return issues


def _coinstall_rule(
    candidate: Mapping[str, Any],
    installed: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    installed_key = (str(installed.get("app_id") or ""), str(installed.get("app_version") or ""))
    for row in candidate.get("coinstall_with", []):
        if not isinstance(row, Mapping):
            continue
        if (str(row.get("app_id") or ""), str(row.get("app_version") or "")) == installed_key:
            return row
    return None


def _coinstall_rules_match(
    candidate_rule: Mapping[str, Any] | None,
    installed_rule: Mapping[str, Any] | None,
) -> bool:
    if candidate_rule is None or installed_rule is None:
        return False
    return bool(
        candidate_rule.get("status") == installed_rule.get("status")
        and candidate_rule.get("reason_code") == installed_rule.get("reason_code")
        and candidate_rule.get("shared_resources") == installed_rule.get("shared_resources")
    )


def _inspection_dependency_matches(
    candidate: Mapping[str, Any],
    installed: Mapping[str, Any],
    candidate_rule: Mapping[str, Any] | None,
    installed_rule: Mapping[str, Any] | None,
) -> bool:
    pair = {str(candidate.get("app_id") or ""), str(installed.get("app_id") or "")}
    if pair != INSPECTION_REWORK_APPS:
        return True
    if not _coinstall_rules_match(candidate_rule, installed_rule):
        return False
    expected_rule = bool(
        candidate_rule
        and installed_rule
        and candidate_rule.get("status") == "conditional"
        and candidate_rule.get("reason_code") == "INSPECTION_DEPENDENCY_IDENTITY_REQUIRED"
        and set(candidate_rule.get("shared_resources") or ()) == INSPECTION_REWORK_SHARED_RESOURCES
        and set(installed_rule.get("shared_resources") or ()) == INSPECTION_REWORK_SHARED_RESOURCES
    )
    rework = candidate if candidate.get("app_id") == "rework_worker" else installed
    inspection = candidate if candidate.get("app_id") == "inspection_worker" else installed
    dependency = rework.get("dependency")
    return bool(
        expected_rule
        and isinstance(dependency, Mapping)
        and dependency.get("kind") == "vendored_inspection_worker"
        and dependency.get("commit") == inspection.get("source_commit")
        and _is_hex(dependency.get("sha256"), 64)
    )


def _same_app_issues(
    candidate: Mapping[str, Any],
    installed: Mapping[str, Any],
) -> list[PreflightIssue]:
    app_id = str(candidate.get("app_id") or "")
    if candidate.get("app_version") != installed.get("app_version"):
        return [
            _issue(
                "SAME_APP_VERSION_CONFLICT",
                "app_version",
                app_id,
                "a different runtime version of the same app is installed",
            )
        ]
    mismatched = [
        field for field in SAME_APP_EXACT_FIELDS if candidate.get(field) != installed.get(field)
    ]
    candidate_resources = candidate.get("resources")
    installed_resources = installed.get("resources")
    if isinstance(candidate_resources, Mapping) and isinstance(installed_resources, Mapping):
        mismatched.extend(
            f"resources.{field}"
            for field in sorted(RESOURCE_FIELDS)
            if _normalize_resource(field, candidate_resources.get(field))
            != _normalize_resource(field, installed_resources.get(field))
        )
    return [
        _issue(
            "SAME_APP_VERSION_MISMATCH",
            field,
            app_id,
            "the installed declaration differs from the candidate for the same app/version",
        )
        for field in sorted(set(mismatched))
    ]


def _report(
    *,
    candidate_app: str,
    candidate_version: str,
    issues: Iterable[PreflightIssue],
) -> dict[str, Any]:
    unique: dict[tuple[str, str, str, str], PreflightIssue] = {}
    for issue in issues:
        key = (issue.code, issue.field, issue.conflicting_app_id, issue.message)
        unique[key] = issue
    ordered = sorted(
        unique.values(),
        key=lambda issue: (issue.code, issue.field, issue.conflicting_app_id, issue.message),
    )
    return {
        "contract_version": INSTALLER_CONTRACT_VERSION,
        "status": "PASS" if not ordered else "FAILED",
        "mutation_allowed": not ordered,
        "candidate": {"app_id": candidate_app, "app_version": candidate_version},
        "issues": [issue.as_dict() for issue in ordered],
    }


def offline_preflight(
    candidate: Mapping[str, Any],
    installed_builds: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Validate a proposed install using only immutable input declarations."""

    candidate_app = str(candidate.get("app_id") or "") if isinstance(candidate, Mapping) else ""
    candidate_version = str(candidate.get("app_version") or "") if isinstance(candidate, Mapping) else ""
    issues = _declaration_issues(candidate, owner="<candidate>")
    if issues:
        return _report(
            candidate_app=candidate_app,
            candidate_version=candidate_version,
            issues=issues,
        )
    if isinstance(installed_builds, (str, bytes, Mapping)):
        return _report(
            candidate_app=candidate_app,
            candidate_version=candidate_version,
            issues=[
                _issue(
                    "COMPATIBILITY_DECLARATION_INVALID",
                    "installed_builds",
                    "<installed>",
                    "installed build declarations must be an array",
                )
            ],
        )
    try:
        installed_rows = list(installed_builds)
    except TypeError:
        installed_rows = []
        issues.append(
            _issue(
                "COMPATIBILITY_DECLARATION_INVALID",
                "installed_builds",
                "<installed>",
                "installed build declarations must be iterable",
            )
        )
    seen_installed: set[tuple[str, str]] = set()
    for index, installed in enumerate(installed_rows):
        installed_issues = _declaration_issues(installed, owner=f"<installed:{index}>")
        issues.extend(installed_issues)
        if installed_issues or not isinstance(installed, Mapping):
            continue
        installed_app = str(installed.get("app_id") or "")
        installed_version = str(installed.get("app_version") or "")
        installed_key = (installed_app, installed_version)
        if installed_key in seen_installed:
            issues.append(
                _issue(
                    "COMPATIBILITY_DECLARATION_INVALID",
                    "installed_builds",
                    installed_app,
                    "installed build declaration is duplicated",
                )
            )
            continue
        seen_installed.add(installed_key)
        if installed_app == candidate_app:
            issues.extend(_same_app_issues(candidate, installed))
            continue

        candidate_rule = _coinstall_rule(candidate, installed)
        installed_rule = _coinstall_rule(installed, candidate)
        rules_match = _coinstall_rules_match(candidate_rule, installed_rule)
        contract_bundle_matches = (
            candidate.get("contract_bundle_sha256") == installed.get("contract_bundle_sha256")
        )
        dependency_matches = _inspection_dependency_matches(
            candidate,
            installed,
            candidate_rule,
            installed_rule,
        )
        if not rules_match:
            issues.append(
                _issue(
                    "COINSTALL_INCOMPATIBLE",
                    "coinstall_with",
                    installed_app,
                    "candidate and installed app do not declare the same exact co-install rule",
                )
            )
        if not contract_bundle_matches:
            issues.append(
                _issue(
                    "COINSTALL_INCOMPATIBLE",
                    "contract_bundle_sha256",
                    installed_app,
                    "candidate and installed app use different immutable contract bundles",
                )
            )
        if rules_match and not dependency_matches:
            issues.append(
                _issue(
                    "COINSTALL_INCOMPATIBLE",
                    "dependency",
                    installed_app,
                    "Inspection/Rework dependency identity does not match the installed provider",
                )
            )

        candidate_resources = candidate["resources"]
        installed_resources = installed["resources"]
        shared_resources = (
            set(candidate_rule.get("shared_resources") or ())
            if rules_match and contract_bundle_matches and dependency_matches and candidate_rule
            else set()
        )
        for field, code in RESOURCE_COLLISION_CODES.items():
            if (
                _normalize_resource(field, candidate_resources.get(field))
                == _normalize_resource(field, installed_resources.get(field))
                and field not in shared_resources
            ):
                issues.append(
                    _issue(
                        code,
                        field,
                        installed_app,
                        f"{field} is already owned by another app",
                    )
                )
        candidate_task = _normalize_resource("task_name", candidate_resources.get("task_name"))
        installed_task = _normalize_resource("task_name", installed_resources.get("task_name"))
        if candidate_task == installed_task:
            if _normalize_resource("task_action", candidate_resources.get("task_action")) != _normalize_resource(
                "task_action", installed_resources.get("task_action")
            ):
                issues.append(
                    _issue(
                        "TASK_ACTION_MISMATCH",
                        "task_action",
                        installed_app,
                        "scheduled task action differs for the same task name",
                    )
                )
            if _normalize_resource(
                "task_principal", candidate_resources.get("task_principal")
            ) != _normalize_resource("task_principal", installed_resources.get("task_principal")):
                issues.append(
                    _issue(
                        "TASK_PRINCIPAL_MISMATCH",
                        "task_principal",
                        installed_app,
                        "scheduled task principal differs for the same task name",
                    )
                )
    return _report(
        candidate_app=candidate_app,
        candidate_version=candidate_version,
        issues=issues,
    )


def evaluate_readiness(
    *,
    app_id: str,
    app_version: str,
    build_identity_sha256: str,
    preflight_passed: bool,
    applied: bool,
    server_reachable: bool,
    server_grant_accepted: bool,
    manifest_match: bool,
    profile_match: bool,
    credential_reference_valid: bool,
    task_valid: bool,
    lease_state: str,
    fence_identity_match: bool,
    connectivity_status: str,
    receipt_required: bool,
    receipt: Mapping[str, Any] | None,
    correlation_id: str,
    rollback_receipt: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Classify post-apply evidence without performing any observations itself."""

    preflight_ok = preflight_passed is True
    applied_ok = applied is True
    server_ok = server_reachable is True
    grant_ok = server_grant_accepted is True
    manifest_ok = manifest_match is True
    profile_ok = profile_match is True
    credential_ok = credential_reference_valid is True
    scheduled_task_ok = task_valid is True
    fence_ok = fence_identity_match is True
    reasons: list[str] = []
    if not preflight_ok:
        reasons.append("INSTALL_PREFLIGHT_FAILED")
    if not applied_ok:
        reasons.append("INSTALL_NOT_APPLIED")
    if not server_ok:
        reasons.append("SERVER_UNREACHABLE")
    if server_ok and not grant_ok:
        reasons.append("SERVER_GRANT_REJECTED")
    if not manifest_ok:
        reasons.append("MANIFEST_HASH_MISMATCH")
    if not profile_ok or not credential_ok:
        reasons.append("PROFILE_OR_CREDENTIAL_MISMATCH")
    if not scheduled_task_ok:
        reasons.append("TASK_ACTION_MISMATCH")
    if lease_state != "ACTIVE" or not fence_ok:
        reasons.append("RUNTIME_LEASE_INACTIVE")
    if connectivity_status not in {"connected", "idle_current"}:
        reasons.append("SOURCE_NOT_CONNECTED")
    receipt_mapping = receipt if isinstance(receipt, Mapping) else None
    totals_value = receipt_mapping.get("totals") if receipt_mapping else None
    receipt_totals = dict(totals_value) if isinstance(totals_value, Mapping) else {}
    if type(receipt_required) is not bool:
        reasons.append("INSTALL_READINESS_INPUT_INVALID")
    if receipt_required is not False:
        errors = receipt_totals.get("errors")
        quarantined = receipt_totals.get("quarantined")
        receipt_ok = bool(
            receipt_mapping
            and (
                receipt_mapping.get("committed") is True
                or receipt_mapping.get("status") == "accepted"
            )
            and type(errors) is int
            and errors == 0
            and type(quarantined) is int
            and quarantined == 0
        )
        if not receipt_ok:
            reasons.append("FIRST_RECEIPT_NOT_CLEAN")
    ready = not reasons
    status = "READY" if ready else ("APPLIED_NOT_READY" if applied_ok and preflight_ok else "FAILED")
    return {
        "contract_version": READINESS_CONTRACT_VERSION,
        "status": status,
        "app": {
            "app_id": app_id,
            "app_version": app_version,
            "build_identity_sha256": build_identity_sha256,
        },
        "preflight_passed": preflight_ok,
        "server_grant_accepted": grant_ok,
        "manifest_match": manifest_ok,
        "profile_match": profile_ok,
        "credential_reference_valid": credential_ok,
        "task_valid": scheduled_task_ok,
        "lease_state": lease_state,
        "fence_identity_match": fence_ok,
        "connectivity_status": connectivity_status,
        "receipt_totals": receipt_totals,
        "rollback_receipt": dict(rollback_receipt) if isinstance(rollback_receipt, Mapping) else {},
        "correlation_id": correlation_id,
        "reason_codes": sorted(set(reasons)),
    }


READINESS_REQUEST_REQUIRED_FIELDS = {
    "app_id",
    "app_version",
    "build_identity_sha256",
    "preflight_passed",
    "applied",
    "server_reachable",
    "server_grant_accepted",
    "manifest_match",
    "profile_match",
    "credential_reference_valid",
    "task_valid",
    "lease_state",
    "fence_identity_match",
    "connectivity_status",
    "receipt_required",
    "receipt",
    "correlation_id",
}

READINESS_BOOLEAN_FIELDS = {
    "preflight_passed",
    "applied",
    "server_reachable",
    "server_grant_accepted",
    "manifest_match",
    "profile_match",
    "credential_reference_valid",
    "task_valid",
    "fence_identity_match",
    "receipt_required",
}


def evaluate_readiness_request(evidence: Mapping[str, Any]) -> dict[str, Any]:
    """Validate a JSON-shaped readiness request before classifying it."""

    if not isinstance(evidence, Mapping):
        raise FactoryContractError(
            "INSTALL_READINESS_INPUT_INVALID",
            "readiness evidence must be an object",
        )
    allowed = READINESS_REQUEST_REQUIRED_FIELDS | {"rollback_receipt"}
    if not READINESS_REQUEST_REQUIRED_FIELDS <= set(evidence) or not set(evidence) <= allowed:
        raise FactoryContractError(
            "INSTALL_READINESS_INPUT_INVALID",
            "readiness evidence fields do not match the installer contract",
        )
    invalid_booleans = sorted(
        field for field in READINESS_BOOLEAN_FIELDS if type(evidence.get(field)) is not bool
    )
    if invalid_booleans:
        raise FactoryContractError(
            "INSTALL_READINESS_INPUT_INVALID",
            "readiness boolean evidence must use JSON booleans",
            details={"invalid_fields": invalid_booleans},
        )
    invalid_strings = sorted(
        field
        for field in ("app_id", "app_version", "lease_state", "connectivity_status", "correlation_id")
        if not _is_non_empty_text(evidence.get(field))
    )
    if invalid_strings or not _is_hex(evidence.get("build_identity_sha256"), 64):
        raise FactoryContractError(
            "INSTALL_READINESS_INPUT_INVALID",
            "readiness identity evidence is incomplete or malformed",
            details={"invalid_fields": invalid_strings},
        )
    if evidence.get("receipt") is not None and not isinstance(evidence.get("receipt"), Mapping):
        raise FactoryContractError(
            "INSTALL_READINESS_INPUT_INVALID",
            "receipt evidence must be an object or null",
        )
    if evidence.get("rollback_receipt") is not None and not isinstance(
        evidence.get("rollback_receipt"), Mapping
    ):
        raise FactoryContractError(
            "INSTALL_READINESS_INPUT_INVALID",
            "rollback receipt evidence must be an object or null",
        )
    return evaluate_readiness(**dict(evidence))

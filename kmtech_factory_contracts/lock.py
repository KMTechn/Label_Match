"""Consumer lock loading and exact bundle verification."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .bundle import (
    CONTRACT_BUNDLE_CORRECTIVE_REVISION,
    CONTRACT_BUNDLE_SHA256,
    CONTRACT_BUNDLE_VERSION,
    MINIMUM_INSTALLER_VERSION,
    MINIMUM_VERIFIER_VERSION,
    verify_bundled_contracts,
)
from .canonical import load_json_strict
from .errors import FactoryContractError


LOCK_SCHEMA_VERSION = 1
DESKTOP_APP_IDS = frozenset(
    {
        "container_audit",
        "defect_inspection",
        "inspection_worker",
        "label_match",
        "rework_worker",
    }
)
REQUIRED_FIELDS = {
    "lock_schema_version",
    "app_id",
    "contract_bundle_version",
    "contract_bundle_corrective_revision",
    "contract_bundle_sha256",
    "required_capabilities",
    "server_api_contract_version",
    "event_contract_version",
    "manifest_contract_version",
    "dependency",
    "db_schema_supported",
    "minimum_verifier_version",
    "minimum_installer_version",
}


def _is_lower_hex(value: Any, length: int) -> bool:
    return (
        isinstance(value, str)
        and len(value) == length
        and value == value.lower()
        and all(character in "0123456789abcdef" for character in value)
    )


def dependency_identity_is_valid(app_id: Any, dependency: Any) -> bool:
    """Return whether a desktop dependency follows its app-specific lock policy."""
    if app_id not in DESKTOP_APP_IDS or not isinstance(dependency, dict):
        return False
    if set(dependency) != {"kind", "commit", "sha256"}:
        return False
    if app_id == "rework_worker":
        return (
            dependency.get("kind") == "vendored_inspection_worker"
            and _is_lower_hex(dependency.get("commit"), 40)
            and _is_lower_hex(dependency.get("sha256"), 64)
        )
    return dependency == {"kind": "none", "commit": None, "sha256": None}


def load_and_verify_contract_lock(
    path: Path,
    *,
    expected_app_id: str,
) -> dict[str, Any]:
    """Load a desktop consumer lock and prove its exact bundled contract identity."""
    raw = load_json_strict(path)
    if not isinstance(raw, dict) or set(raw) != REQUIRED_FIELDS:
        raise FactoryContractError(
            "CONTRACT_LOCK_INVALID",
            "consumer contract lock fields are incomplete or unexpected",
        )
    if raw.get("lock_schema_version") != LOCK_SCHEMA_VERSION:
        raise FactoryContractError("CONTRACT_LOCK_INVALID", "unsupported consumer lock schema")
    if raw.get("app_id") != expected_app_id:
        raise FactoryContractError("CONTRACT_LOCK_INVALID", "consumer lock app identity differs")
    if raw.get("contract_bundle_version") != CONTRACT_BUNDLE_VERSION:
        raise FactoryContractError("CONTRACT_VERSION_MISMATCH", "consumer contract version differs")
    if raw.get("contract_bundle_corrective_revision") != CONTRACT_BUNDLE_CORRECTIVE_REVISION:
        raise FactoryContractError(
            "CONTRACT_VERSION_MISMATCH",
            "consumer contract corrective revision differs",
        )
    if raw.get("contract_bundle_sha256") != CONTRACT_BUNDLE_SHA256:
        raise FactoryContractError("CONTRACT_HASH_MISMATCH", "consumer contract hash differs")
    if raw.get("minimum_verifier_version") != MINIMUM_VERIFIER_VERSION:
        raise FactoryContractError("CONTRACT_LOCK_INVALID", "minimum verifier version differs")
    if raw.get("minimum_installer_version") != MINIMUM_INSTALLER_VERSION:
        raise FactoryContractError("CONTRACT_LOCK_INVALID", "minimum installer version differs")
    capabilities = raw.get("required_capabilities")
    if not isinstance(capabilities, dict) or not capabilities or any(
        not isinstance(key, str) or not key or not isinstance(value, str) or not value
        for key, value in capabilities.items()
    ):
        raise FactoryContractError("CONTRACT_LOCK_INVALID", "required capabilities are invalid")
    dependency = raw.get("dependency")
    if not dependency_identity_is_valid(raw.get("app_id"), dependency):
        raise FactoryContractError("CONTRACT_LOCK_INVALID", "dependency identity is invalid")
    db_range = raw.get("db_schema_supported")
    if (
        not isinstance(db_range, dict)
        or set(db_range) != {"minimum", "maximum"}
        or not isinstance(db_range.get("minimum"), int)
        or not isinstance(db_range.get("maximum"), int)
        or db_range["minimum"] > db_range["maximum"]
    ):
        raise FactoryContractError("CONTRACT_LOCK_INVALID", "DB schema support range is invalid")
    verify_bundled_contracts(expected_sha256=str(raw["contract_bundle_sha256"]))
    return raw

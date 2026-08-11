"""Offline build-identity and staged-package verification."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Mapping

from .bundle import MINIMUM_INSTALLER_VERSION
from .canonical import canonical_sha256, file_sha256, load_json_strict, require_posix_relative_path
from .errors import FactoryContractError
from .lock import load_and_verify_contract_lock


BUILD_IDENTITY_SCHEMA_VERSION = 1
BUILD_MANIFEST_SCHEMA_VERSION = 1
BUILD_COMPATIBILITY_SCHEMA_VERSION = 1
SHA256_LENGTH = 64


BUILD_IDENTITY_REQUIRED_FIELDS = (
    "build_identity_schema_version",
    "app_id",
    "app_version",
    "source_commit",
    "source_tree",
    "dirty",
    "contract_bundle_version",
    "contract_bundle_sha256",
    "db_schema",
    "server_api_contract_version",
    "event_contract_version",
    "manifest_contract_version",
    "dependency",
    "builder",
    "python_version",
    "pyinstaller_version",
    "dependency_lock_sha256",
    "build_compatibility_sha256",
)


def _sha256(value: Any, field: str) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != SHA256_LENGTH or any(char not in "0123456789abcdef" for char in normalized):
        raise FactoryContractError(
            "PACKAGE_PROVENANCE_MISMATCH",
            f"{field} must be an exact SHA-256",
        )
    return normalized


def validate_build_identity(
    identity: Mapping[str, Any],
    *,
    release_mode: bool = True,
) -> dict[str, Any]:
    missing = [field for field in BUILD_IDENTITY_REQUIRED_FIELDS if field not in identity]
    if missing:
        raise FactoryContractError(
            "PACKAGE_PROVENANCE_MISMATCH",
            "build identity is missing required fields",
            details={"missing_fields": missing},
        )
    normalized = dict(identity)
    if normalized.get("build_identity_schema_version") != BUILD_IDENTITY_SCHEMA_VERSION:
        raise FactoryContractError("PACKAGE_PROVENANCE_MISMATCH", "unsupported build identity schema")
    for field in (
        "contract_bundle_sha256",
        "dependency_lock_sha256",
        "build_compatibility_sha256",
    ):
        normalized[field] = _sha256(normalized.get(field), field)
    source_commit = str(normalized.get("source_commit") or "").strip().lower()
    source_tree = str(normalized.get("source_tree") or "").strip().lower()
    if len(source_commit) != 40 or len(source_tree) != 40:
        raise FactoryContractError(
            "PACKAGE_PROVENANCE_MISMATCH",
            "source commit and tree must be exact 40-character Git object IDs",
        )
    dependency = normalized.get("dependency")
    if not isinstance(dependency, dict) or not dependency.get("kind"):
        raise FactoryContractError("DEPENDENCY_PROVENANCE_MISMATCH", "dependency identity is required")
    if dependency.get("sha256"):
        _sha256(dependency.get("sha256"), "dependency.sha256")
    if dependency.get("commit") and len(str(dependency["commit"])) != 40:
        raise FactoryContractError("DEPENDENCY_PROVENANCE_MISMATCH", "dependency commit is invalid")
    db_schema = normalized.get("db_schema")
    if not isinstance(db_schema, dict) or not {"current", "minimum", "maximum"} <= set(db_schema):
        raise FactoryContractError("PACKAGE_PROVENANCE_MISMATCH", "DB schema range is incomplete")
    if release_mode and normalized.get("dirty") is not False:
        raise FactoryContractError(
            "PACKAGE_PROVENANCE_MISMATCH",
            "release package identity must declare dirty=false",
        )
    return normalized


def build_identity_binding_sha256(identity: Mapping[str, Any]) -> str:
    """Hash identity fields except the compatibility backlink to avoid a hash cycle."""
    core = dict(identity)
    core.pop("build_compatibility_sha256", None)
    return canonical_sha256(core)


def create_build_compatibility(
    identity: Mapping[str, Any],
    *,
    resources: Mapping[str, Any],
    coinstall_with: Iterable[Mapping[str, str]],
) -> dict[str, Any]:
    normalized = validate_build_identity(identity, release_mode=False)
    return {
        "matrix_schema_version": BUILD_COMPATIBILITY_SCHEMA_VERSION,
        "app_id": normalized["app_id"],
        "app_version": normalized["app_version"],
        "source_commit": normalized["source_commit"],
        "source_tree": normalized["source_tree"],
        "build_identity_sha256": build_identity_binding_sha256(normalized),
        "contract_bundle_sha256": normalized["contract_bundle_sha256"],
        "dependency": normalized["dependency"],
        "db_schema_supported": {
            "minimum": normalized["db_schema"]["minimum"],
            "maximum": normalized["db_schema"]["maximum"],
        },
        "server_api_contract_versions": [normalized["server_api_contract_version"]],
        "event_contract_version": normalized["event_contract_version"],
        "manifest_contract_version": normalized["manifest_contract_version"],
        "minimum_installer_version": MINIMUM_INSTALLER_VERSION,
        "resources": dict(resources),
        "coinstall_with": sorted(
            (dict(row) for row in coinstall_with),
            key=lambda row: (row.get("app_id", ""), row.get("app_version", "")),
        ),
    }


def validate_build_compatibility(
    compatibility: Mapping[str, Any],
    *,
    identity: Mapping[str, Any],
) -> dict[str, Any]:
    normalized_identity = validate_build_identity(identity, release_mode=False)
    normalized = dict(compatibility)
    if normalized.get("matrix_schema_version") != BUILD_COMPATIBILITY_SCHEMA_VERSION:
        raise FactoryContractError("PACKAGE_PROVENANCE_MISMATCH", "unsupported compatibility schema")
    exact_fields = {
        "app_id": normalized_identity["app_id"],
        "app_version": normalized_identity["app_version"],
        "source_commit": normalized_identity["source_commit"],
        "source_tree": normalized_identity["source_tree"],
        "build_identity_sha256": build_identity_binding_sha256(normalized_identity),
        "contract_bundle_sha256": normalized_identity["contract_bundle_sha256"],
        "dependency": normalized_identity["dependency"],
        "event_contract_version": normalized_identity["event_contract_version"],
        "manifest_contract_version": normalized_identity["manifest_contract_version"],
        "minimum_installer_version": MINIMUM_INSTALLER_VERSION,
    }
    mismatched = sorted(
        field for field, expected in exact_fields.items() if normalized.get(field) != expected
    )
    expected_db_range = {
        "minimum": normalized_identity["db_schema"]["minimum"],
        "maximum": normalized_identity["db_schema"]["maximum"],
    }
    if normalized.get("db_schema_supported") != expected_db_range:
        mismatched.append("db_schema_supported")
    if normalized.get("server_api_contract_versions") != [normalized_identity["server_api_contract_version"]]:
        mismatched.append("server_api_contract_versions")
    resources = normalized.get("resources")
    required_resources = {
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
    if not isinstance(resources, dict) or set(resources) != required_resources:
        mismatched.append("resources")
    if mismatched:
        raise FactoryContractError(
            "PACKAGE_PROVENANCE_MISMATCH",
            "build compatibility differs from embedded identity",
            details={"mismatched_fields": sorted(set(mismatched))},
        )
    return normalized


def _payload_files(stage_root: Path, *, excluded: set[str]) -> Iterable[Path]:
    for path in sorted(stage_root.rglob("*"), key=lambda value: value.as_posix()):
        if not path.is_file():
            continue
        relative = path.relative_to(stage_root).as_posix()
        if relative not in excluded:
            yield path


def payload_inventory(stage_root: Path, *, excluded: Iterable[str] = ()) -> list[dict[str, Any]]:
    excluded_set = {require_posix_relative_path(value) for value in excluded}
    rows = []
    for path in _payload_files(stage_root, excluded=excluded_set):
        relative = require_posix_relative_path(path.relative_to(stage_root).as_posix())
        rows.append(
            {
                "path": relative,
                "size": path.stat().st_size,
                "sha256": file_sha256(path),
            }
        )
    return rows


def create_build_manifest(
    stage_root: Path,
    *,
    identity_path: str = "build-identity.json",
    compatibility_path: str = "build-compatibility.json",
    manifest_path: str = "build-manifest.json",
    expected_files: Iterable[str] = (),
    built_at_utc: str | None = None,
) -> dict[str, Any]:
    normalized_identity_path = require_posix_relative_path(identity_path)
    identity = load_json_strict(stage_root / Path(*PurePosixPath(normalized_identity_path).parts))
    if not isinstance(identity, dict):
        raise FactoryContractError("PACKAGE_PROVENANCE_MISMATCH", "embedded identity must be an object")
    validate_build_identity(identity, release_mode=False)
    normalized_compatibility_path = require_posix_relative_path(compatibility_path)
    compatibility = load_json_strict(
        stage_root / Path(*PurePosixPath(normalized_compatibility_path).parts)
    )
    if not isinstance(compatibility, dict):
        raise FactoryContractError("PACKAGE_PROVENANCE_MISMATCH", "build compatibility must be an object")
    validate_build_compatibility(compatibility, identity=identity)
    compatibility_sha256 = canonical_sha256(compatibility)
    if compatibility_sha256 != identity["build_compatibility_sha256"]:
        raise FactoryContractError(
            "PACKAGE_PROVENANCE_MISMATCH",
            "embedded identity compatibility hash differs",
        )
    inventory = payload_inventory(stage_root, excluded={manifest_path})
    inventory_sha256 = canonical_sha256(inventory)
    return {
        "build_manifest_schema_version": BUILD_MANIFEST_SCHEMA_VERSION,
        "app_id": identity["app_id"],
        "app_version": identity["app_version"],
        "identity_path": normalized_identity_path,
        "identity_sha256": canonical_sha256(identity),
        "build_compatibility_path": normalized_compatibility_path,
        "contract_bundle_sha256": identity["contract_bundle_sha256"],
        "dependency": identity["dependency"],
        "build_compatibility_sha256": compatibility_sha256,
        "payload_inventory": inventory,
        "payload_inventory_sha256": inventory_sha256,
        "expected_files": sorted(require_posix_relative_path(item) for item in expected_files),
        "built_at_utc": built_at_utc
        or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }


def verify_staged_package(
    stage_root: Path,
    *,
    manifest_path: str = "build-manifest.json",
    release_mode: bool = True,
    expected_contract_sha256: str | None = None,
    expected_source_commit: str | None = None,
    expected_source_tree: str | None = None,
    expected_dependency_sha256: str | None = None,
) -> dict[str, Any]:
    manifest_relative = require_posix_relative_path(manifest_path)
    manifest = load_json_strict(stage_root / Path(*PurePosixPath(manifest_relative).parts))
    if not isinstance(manifest, dict) or manifest.get("build_manifest_schema_version") != 1:
        raise FactoryContractError("PACKAGE_PROVENANCE_MISMATCH", "outer build manifest is invalid")
    identity_relative = require_posix_relative_path(str(manifest.get("identity_path") or ""))
    identity = load_json_strict(stage_root / Path(*PurePosixPath(identity_relative).parts))
    if not isinstance(identity, dict):
        raise FactoryContractError("EMBEDDED_IDENTITY_MISMATCH", "embedded identity is invalid")
    identity = validate_build_identity(identity, release_mode=release_mode)
    staged_lock_path = stage_root / "contract.lock.json"
    staged_lock = load_and_verify_contract_lock(
        staged_lock_path,
        expected_app_id=str(identity.get("app_id") or ""),
    )
    if canonical_sha256(staged_lock) != identity.get("dependency_lock_sha256"):
        raise FactoryContractError(
            "EMBEDDED_IDENTITY_MISMATCH",
            "staged contract lock does not match the embedded dependency-lock identity",
        )
    lock_bound_fields = {
        "contract_bundle_version": staged_lock.get("contract_bundle_version"),
        "contract_bundle_sha256": staged_lock.get("contract_bundle_sha256"),
        "server_api_contract_version": staged_lock.get("server_api_contract_version"),
        "event_contract_version": staged_lock.get("event_contract_version"),
        "manifest_contract_version": staged_lock.get("manifest_contract_version"),
        "dependency": staged_lock.get("dependency"),
    }
    mismatched_lock_fields = sorted(
        field
        for field, expected in lock_bound_fields.items()
        if identity.get(field) != expected
    )
    expected_db_range = staged_lock.get("db_schema_supported")
    if not isinstance(expected_db_range, dict) or {
        "minimum": identity["db_schema"]["minimum"],
        "maximum": identity["db_schema"]["maximum"],
    } != expected_db_range:
        mismatched_lock_fields.append("db_schema")
    if mismatched_lock_fields:
        raise FactoryContractError(
            "EMBEDDED_IDENTITY_MISMATCH",
            "staged contract lock and embedded build identity differ",
            details={"mismatched_fields": sorted(set(mismatched_lock_fields))},
        )
    if canonical_sha256(identity) != manifest.get("identity_sha256"):
        raise FactoryContractError("EMBEDDED_IDENTITY_MISMATCH", "embedded and outer identity differ")
    if manifest.get("app_id") != identity.get("app_id") or manifest.get("app_version") != identity.get("app_version"):
        raise FactoryContractError("EMBEDDED_IDENTITY_MISMATCH", "outer app identity differs")
    if manifest.get("contract_bundle_sha256") != identity.get("contract_bundle_sha256"):
        raise FactoryContractError("CONTRACT_HASH_MISMATCH", "outer and embedded contract hashes differ")
    compatibility_relative = require_posix_relative_path(
        str(manifest.get("build_compatibility_path") or "")
    )
    compatibility = load_json_strict(
        stage_root / Path(*PurePosixPath(compatibility_relative).parts)
    )
    if not isinstance(compatibility, dict):
        raise FactoryContractError("PACKAGE_PROVENANCE_MISMATCH", "build compatibility is invalid")
    validate_build_compatibility(compatibility, identity=identity)
    compatibility_sha256 = canonical_sha256(compatibility)
    if (
        compatibility_sha256 != identity.get("build_compatibility_sha256")
        or compatibility_sha256 != manifest.get("build_compatibility_sha256")
    ):
        raise FactoryContractError(
            "PACKAGE_PROVENANCE_MISMATCH",
            "outer, embedded, and compatibility identities differ",
        )
    inventory = payload_inventory(stage_root, excluded={manifest_relative})
    if inventory != manifest.get("payload_inventory"):
        raise FactoryContractError("PAYLOAD_HASH_MISMATCH", "payload inventory differs from the manifest")
    if canonical_sha256(inventory) != manifest.get("payload_inventory_sha256"):
        raise FactoryContractError("PAYLOAD_HASH_MISMATCH", "payload inventory digest differs")
    inventory_paths = {row["path"] for row in inventory}
    required_paths = set(manifest.get("expected_files") or ())
    if not required_paths <= inventory_paths:
        raise FactoryContractError(
            "PAYLOAD_HASH_MISMATCH",
            "required package files are missing",
            details={"missing_files": sorted(required_paths - inventory_paths)},
        )
    if expected_contract_sha256 and identity.get("contract_bundle_sha256") != expected_contract_sha256:
        raise FactoryContractError("CONTRACT_HASH_MISMATCH", "package contract hash differs from lock")
    if expected_source_commit and identity.get("source_commit") != expected_source_commit:
        raise FactoryContractError("PACKAGE_PROVENANCE_MISMATCH", "source commit differs from release input")
    if expected_source_tree and identity.get("source_tree") != expected_source_tree:
        raise FactoryContractError("PACKAGE_PROVENANCE_MISMATCH", "source tree differs from release input")
    if expected_dependency_sha256 and identity.get("dependency", {}).get("sha256") != expected_dependency_sha256:
        raise FactoryContractError("DEPENDENCY_PROVENANCE_MISMATCH", "dependency payload differs")
    return {
        "status": "PASS",
        "app_id": identity["app_id"],
        "app_version": identity["app_version"],
        "source_commit": identity["source_commit"],
        "source_tree": identity["source_tree"],
        "contract_bundle_sha256": identity["contract_bundle_sha256"],
        "payload_inventory_sha256": manifest["payload_inventory_sha256"],
        "file_count": len(inventory),
    }


def write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
        newline="\n",
    )

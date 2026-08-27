#!/usr/bin/env python
"""Build the deterministic, unsigned Label_Match pre-push release archive."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import stat
from typing import Sequence
import zipfile


RELEASE_IDENTITY_SCHEMA = "label-match-release-identity-v3"
STAGED_INSTALLER_SCHEMA = "label-match-staged-installer-verification-v3"
CONTRACT_BUNDLE_SHA256 = "adaa08684ebb291837327f63f967a4f22650dff72c4c1dc56ce1a9bee6b5404a"
PYTHON_VERSION = "3.12.10"
PYINSTALLER_VERSION = "6.20.0"
PROBE_SCHEMA = "kmtech-active-work-probe-build-v1.0.3.4"
PROBE_NAME = "KMTechActiveWorkProbe"
PROBE_VERSION = "v1.0.3.4"
SEMVER_TAG_RE = re.compile(r"^v(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)$")
GIT_OID_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

ALL_PROBE_APPS = [
    "Inspection_worker",
    "Rework_worker",
    "Defect_Inspection",
    "Container_Audit",
    "Label_Match",
]
FACTORY_EXPECTED_FILES = {
    "Label_Match.exe",
    "KMTechActiveWorkProbe.exe",
    "KMTechActiveWorkProbe.independent.build-identity.json",
    "KMTechActiveWorkProbe.integrated.build-identity.json",
    "contract.lock.json",
    "build-identity.json",
    "build-compatibility.json",
}
EXPECTED_CONTRACT_LOCK = {
    "app_id": "label_match",
    "contract_bundle_corrective_revision": 1,
    "contract_bundle_sha256": CONTRACT_BUNDLE_SHA256,
    "contract_bundle_version": "1.0.3",
    "db_schema_supported": {"maximum": 0, "minimum": 0},
    "dependency": {"commit": None, "kind": "none", "sha256": None},
    "event_contract_version": "common-event-envelope-v1",
    "lock_schema_version": 1,
    "manifest_contract_version": "producer-onboarding-manifest-v1",
    "minimum_installer_version": "1.0.3.4",
    "minimum_verifier_version": "1.0.3.4",
    "required_capabilities": {
        "producer_ingest": "producer-ingest-v1",
        "runtime_lease": "producer-runtime-lease-v1",
    },
    "server_api_contract_version": "logistics-v1",
}
EXPECTED_FACTORY_RESOURCES = {
    "profile_id": "C:/ProgramData/KMTech/Logistics/profiles/Label_Match/runtime-profile.json",
    "credential_target": "dpapi:producer-{source_host_id}-http-push-key",
    "task_name": "direct-sync-relay-label-match",
    "task_action": (
        "wscript.exe //B //NoLogo "
        "C:/ProgramData/KMTech/DirectSync/label_match/bin/"
        "run_direct-sync-relay-label-match.vbs"
    ),
    "task_principal": "SYSTEM",
    "install_root": "C:/KMTech/Apps/Label_Match/current",
    "data_root": "C:/ProgramData/KMTech/Label_Match/data",
    "state_db": (
        "C:/ProgramData/KMTech/DirectSync/label_match/queue/"
        "direct_sync_relay.sqlite3"
    ),
    "log_root": "C:/ProgramData/KMTech/Label_Match/data",
    "machine_identity_scope": "label_match:{pc_id}",
}
REQUIRED_PACKAGE_MEMBERS = {
    "CENTRAL_LOGISTICS_PC_ROLLOUT.md",
    "INSTALL_THIS_PC.ps1",
    "Label_Match.exe",
    "Label_Match_Protected_Admin_Install.exe",
    "PROTECTED_ADMIN_PROVISIONING.md",
    "PROVISION_PROTECTED_ADMIN_ACL.ps1",
    "build-identity.json",
    "build-compatibility.json",
    "build-manifest.json",
    "contract.lock.json",
    "release-identity.json",
    "staged-installer-verification.json",
    "KMTechActiveWorkProbe.exe",
    "KMTechActiveWorkProbe.independent.build-identity.json",
    "KMTechActiveWorkProbe.integrated.build-identity.json",
    "KMTech_Logistics_Profile_Check.exe",
    "KMTech_Logistics_Profile_Install.exe",
    "config/app_settings.json",
    "_internal/config/app_settings.json",
    "_internal/python312.dll",
    "_internal/base_library.zip",
    "direct_sync_operator.py",
    "direct_sync_push.py",
    "direct_sync_runtime.py",
    "logistics_runtime_profile.py",
    "producer_runtime_client.py",
    "tools/check_logistics_runtime_profile.py",
    "tools/direct_sync_relay_runner.py",
    "tools/install_logistics_runtime_profile.py",
    "tools/register_label_match_worker_pc.py",
}
RETIRED_HELPER_EXECUTABLES = {
    "install_label_match_direct_sync.ps1",
    "tools/direct_sync_relay_install_pack.py",
    "tools/direct_sync_relay_install_pack/direct_sync_relay_install_pack.exe",
    "tools/direct_sync_relay_install_pack.exe",
    "tools/direct_sync_relay_runner/direct_sync_relay_runner.exe",
    "tools/register_label_match_worker_pc.exe",
    "tools/invoke_embedded_python.ps1",
}


class ReleaseArchiveError(RuntimeError):
    """Raised when the staged candidate cannot prove the release contract."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _files(root: Path, *, casefold: bool = True) -> list[Path]:
    def key(path: Path) -> tuple[str, str] | str:
        relative = path.relative_to(root).as_posix()
        return (relative.casefold(), relative) if casefold else relative

    return sorted((path for path in root.rglob("*") if path.is_file()), key=key)


def _inventory(
    root: Path,
    *,
    excluded: set[str] | None = None,
    casefold: bool = True,
) -> list[dict[str, object]]:
    excluded = excluded or set()
    return [
        {
            "path": path.relative_to(root).as_posix(),
            "size": path.stat().st_size,
            "sha256": _sha256(path),
        }
        for path in _files(root, casefold=casefold)
        if path.relative_to(root).as_posix() not in excluded
    ]


def _inventory_digest(inventory: list[dict[str, object]]) -> str:
    """Match verify_staged_release_installer.py's inventory digest."""

    canonical = json.dumps(
        inventory,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _canonical_sha256(value: object) -> str:
    canonical = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number is forbidden: {value}")


def _json(path: Path, label: str) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        if raw.startswith(b"\xef\xbb\xbf"):
            raise ValueError("UTF-8 BOM is forbidden")
        payload = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_constant,
        )
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        raise ReleaseArchiveError(f"{label} is invalid: {exc}") from exc
    if not isinstance(payload, dict):
        raise ReleaseArchiveError(f"{label} must be a JSON object")
    return payload


def _required_int(
    payload: dict[str, object],
    key: str,
    *,
    label: str,
    minimum: int | None = None,
    exact: int | None = None,
) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ReleaseArchiveError(f"{label} {key} must be an integer")
    if minimum is not None and value < minimum:
        raise ReleaseArchiveError(f"{label} {key} must be at least {minimum}")
    if exact is not None and value != exact:
        raise ReleaseArchiveError(f"{label} {key} must equal {exact}")
    return value


def _safe_relative(value: object, *, label: str) -> str:
    path = str(value or "")
    pure = PurePosixPath(path)
    if (
        not path
        or "\\" in path
        or ":" in path
        or path.endswith("/")
        or pure.is_absolute()
        or any(part in {"", ".", ".."} for part in pure.parts)
        or pure.as_posix() != path
    ):
        raise ReleaseArchiveError(f"{label} path is not a canonical POSIX relative path: {path!r}")
    return path


def _assert_no_reparse_points(root: Path) -> None:
    for path in root.rglob("*"):
        try:
            attributes = getattr(path.lstat(), "st_file_attributes", 0)
        except OSError as exc:
            raise ReleaseArchiveError(f"package path could not be inspected: {path}") from exc
        if path.is_symlink() or bool(attributes & 0x400):
            raise ReleaseArchiveError(f"package contains a reparse point: {path}")


def _normalize_inventory(
    value: object,
    *,
    label: str,
) -> list[dict[str, object]]:
    if not isinstance(value, list) or not value:
        raise ReleaseArchiveError(f"{label} inventory is missing")
    normalized: list[dict[str, object]] = []
    seen: set[str] = set()
    for raw in value:
        if not isinstance(raw, dict) or set(raw) != {"path", "size", "sha256"}:
            raise ReleaseArchiveError(f"{label} inventory entry is invalid")
        relative = _safe_relative(raw.get("path"), label=label)
        folded = relative.casefold()
        if folded in seen:
            raise ReleaseArchiveError(f"{label} inventory path is duplicated: {relative}")
        seen.add(folded)
        size = _required_int(raw, "size", label=f"{label} {relative}", minimum=0)
        sha256 = str(raw.get("sha256") or "")
        if SHA256_RE.fullmatch(sha256) is None:
            raise ReleaseArchiveError(f"{label} inventory SHA-256 is invalid: {relative}")
        normalized.append({"path": relative, "size": size, "sha256": sha256})
    if normalized != value:
        raise ReleaseArchiveError(f"{label} inventory is not canonical")
    return normalized


def _validate_release_identity(
    package_root: Path,
    *,
    expected_tag: str | None,
) -> dict[str, str]:
    identity = _json(package_root / "release-identity.json", "release identity")
    required = {
        "schema_version",
        "status",
        "tag",
        "app_version",
        "commit",
        "tree",
        "clean_checkout",
        "release_trust",
        "tag_object_type",
        "annotated_tag",
        "tag_signature_verified",
        "reviewed_ref",
        "reviewed_ref_commit",
        "reviewed_main_ancestor",
        "reviewed_ref_exact",
    }
    if set(identity) != required:
        raise ReleaseArchiveError("release identity v3 fields differ from the exact contract")
    tag = str(identity.get("tag") or "")
    commit = str(identity.get("commit") or "")
    tree = str(identity.get("tree") or "")
    if (
        identity.get("schema_version") != RELEASE_IDENTITY_SCHEMA
        or identity.get("status") != "PASS"
        or identity.get("app_version") != tag
        or SEMVER_TAG_RE.fullmatch(tag) is None
        or (expected_tag is not None and tag != expected_tag)
        or GIT_OID_RE.fullmatch(commit) is None
        or GIT_OID_RE.fullmatch(tree) is None
        or identity.get("clean_checkout") is not True
        or identity.get("release_trust") != "internal_unsigned"
        or identity.get("tag_object_type") != "tag"
        or identity.get("annotated_tag") is not True
        or identity.get("tag_signature_verified") is not False
        or identity.get("reviewed_ref") != "refs/remotes/origin/main"
        or identity.get("reviewed_ref_commit") != commit
        or identity.get("reviewed_main_ancestor") is not True
        or identity.get("reviewed_ref_exact") is not True
    ):
        raise ReleaseArchiveError("release identity is not an exact internal_unsigned v3 PASS")
    return {"tag": tag, "commit": commit, "tree": tree}


def _validate_cli_tools_manifest(
    package_root: Path,
    *,
    identity: dict[str, str],
) -> dict[str, int]:
    manifest = _json(
        package_root / "tools" / "release_cli_tools_manifest.json",
        "release CLI tools manifest",
    )
    if set(manifest) != {
        "schema_version",
        "status",
        "artifact_phase",
        "commit",
        "tree",
        "app_version",
        "python_version",
        "pyinstaller_version",
        "probe_policy",
        "tools",
    }:
        raise ReleaseArchiveError("release CLI tools manifest fields differ from the exact contract")
    if (
        manifest.get("schema_version") != CLI_TOOLS_SCHEMA
        or manifest.get("status") != "PASS"
        or manifest.get("artifact_phase") != "unsigned_pre_sign"
        or manifest.get("commit") != identity["commit"]
        or manifest.get("tree") != identity["tree"]
        or manifest.get("app_version") != identity["tag"]
        or manifest.get("python_version") != PYTHON_VERSION
        or manifest.get("pyinstaller_version") != PYINSTALLER_VERSION
        or manifest.get("probe_policy") != CLI_PROBE_POLICY
    ):
        raise ReleaseArchiveError(
            "release CLI tools manifest source/version/toolchain identity mismatch"
        )

    tools = manifest.get("tools")
    if not isinstance(tools, list) or len(tools) != len(CLI_TOOL_SPECS):
        raise ReleaseArchiveError("release CLI tools inventory is not exactly three tools")
    onedir_runtime_count = 0
    for tool, (expected_name, expected_source, expected_mode) in zip(
        tools, CLI_TOOL_SPECS, strict=True
    ):
        if not isinstance(tool, dict) or set(tool) != {
            "name",
            "source",
            "mode",
            "executable_sha256",
            "executable_size",
            "payload_inventory",
            "help_runs",
            "archive_verification",
        }:
            raise ReleaseArchiveError(f"release CLI tool evidence is not exact: {expected_name}")
        if (
            tool.get("name") != expected_name
            or tool.get("source") != expected_source
            or tool.get("mode") != expected_mode
            or not (package_root / PurePosixPath(expected_source)).is_file()
        ):
            raise ReleaseArchiveError(
                f"release CLI tool source/order/mode mismatch: {expected_name}"
            )

        executable_sha = str(tool.get("executable_sha256") or "")
        executable_size = _required_int(
            tool,
            "executable_size",
            label=f"release CLI executable {expected_name}",
            minimum=1,
        )
        if SHA256_RE.fullmatch(executable_sha) is None:
            raise ReleaseArchiveError(f"release CLI executable hash is invalid: {expected_name}")
        inventory = _normalize_inventory(
            tool.get("payload_inventory"), label=f"release CLI payload {expected_name}"
        )
        expected_payload_root = (
            package_root / "tools" / expected_name
            if expected_mode == "onedir"
            else package_root / "tools"
        )
        if expected_mode == "onefile":
            expected_inventory_paths = {f"{expected_name}.exe"}
        else:
            expected_inventory_paths = {
                path.relative_to(expected_payload_root).as_posix()
                for path in _files(expected_payload_root)
            }
            onedir_runtime_count = sum(
                1 for path in expected_inventory_paths if path.startswith("_internal/")
            )
        if {str(item["path"]) for item in inventory} != expected_inventory_paths:
            raise ReleaseArchiveError(
                f"release CLI payload membership mismatch: {expected_name}"
            )
        for item in inventory:
            target = expected_payload_root / PurePosixPath(str(item["path"]))
            if (
                not target.is_file()
                or target.stat().st_size != item["size"]
                or _sha256(target) != item["sha256"]
            ):
                raise ReleaseArchiveError(
                    f"release CLI payload byte mismatch: {expected_name}/{item['path']}"
                )
        executable = (
            expected_payload_root / f"{expected_name}.exe"
            if expected_mode == "onedir"
            else package_root / "tools" / f"{expected_name}.exe"
        )
        if (
            not executable.is_file()
            or executable.stat().st_size != executable_size
            or _sha256(executable) != executable_sha
        ):
            raise ReleaseArchiveError(
                f"release CLI executable evidence mismatch: {expected_name}"
            )

        help_runs = tool.get("help_runs")
        if not isinstance(help_runs, list) or len(help_runs) != 3:
            raise ReleaseArchiveError(f"release CLI help evidence is incomplete: {expected_name}")
        for run_no, run in enumerate(help_runs, start=1):
            if not isinstance(run, dict) or set(run) != {
                "run",
                "status",
                "elapsed_ms",
                "returncode",
                "stdout_bytes",
                "stderr_bytes",
                "probe_executable_sha256",
                "residual_process_count",
            }:
                raise ReleaseArchiveError(f"release CLI help evidence is not exact: {expected_name}")
            if (
                run.get("run") != run_no
                or run.get("status") != "PASS"
                or run.get("probe_executable_sha256") != executable_sha
            ):
                raise ReleaseArchiveError(f"release CLI help evidence is invalid: {expected_name}")
            _required_int(run, "elapsed_ms", label=f"release CLI help run {expected_name}", minimum=0)
            _required_int(run, "returncode", label=f"release CLI help run {expected_name}", exact=0)
            _required_int(run, "stdout_bytes", label=f"release CLI help run {expected_name}", minimum=1)
            _required_int(run, "stderr_bytes", label=f"release CLI help run {expected_name}", exact=0)
            _required_int(
                run,
                "residual_process_count",
                label=f"release CLI help run {expected_name}",
                exact=0,
            )

        archive_evidence = tool.get("archive_verification")
        if not isinstance(archive_evidence, dict) or set(archive_evidence) != {
            "status",
            "viewer_stdout_sha256",
            "viewer_stderr_bytes",
        }:
            raise ReleaseArchiveError(
                f"release CLI CArchive evidence is not exact: {expected_name}"
            )
        if (
            archive_evidence.get("status") != "PASS"
            or SHA256_RE.fullmatch(
                str(archive_evidence.get("viewer_stdout_sha256") or "")
            )
            is None
        ):
            raise ReleaseArchiveError(
                f"release CLI CArchive evidence is invalid: {expected_name}"
            )
        _required_int(
            archive_evidence,
            "viewer_stderr_bytes",
            label=f"release CLI CArchive {expected_name}",
            exact=0,
        )
    if onedir_runtime_count < 1:
        raise ReleaseArchiveError("release CLI install-pack onedir runtime is missing")
    return {
        "cli_tool_count": len(tools),
        "install_onedir_runtime_file_count": onedir_runtime_count,
    }


def _validate_staged_installer(package_root: Path) -> dict[str, object]:
    report_path = package_root / "staged-installer-verification.json"
    report = _json(report_path, "staged installer verification")
    if set(report) != {
        "schema_version",
        "status",
        "proof_classification",
        "dynamic_qualification",
        "public_entrypoint",
        "runtime_host",
        "bootstrap_contract",
        "state_contract",
        "legacy_authority_contract",
        "manifest_contract",
        "system_python_required",
        "original_package_file_count",
        "original_package_inventory",
        "original_package_inventory_sha256",
        "original_package_unchanged",
        "output_bound_bytes",
        "timeout_seconds",
        "stdout_bytes",
        "stderr_bytes",
    }:
        raise ReleaseArchiveError("staged installer evidence fields differ from the exact contract")
    if (
        report.get("schema_version") != STAGED_INSTALLER_SCHEMA
        or report.get("status") != "PASS"
        or report.get("proof_classification") != "STATIC_ISOLATED_DRY_RUN"
        or report.get("dynamic_qualification") != "NOT_TESTED"
        or report.get("system_python_required") is not False
        or report.get("original_package_unchanged") is not True
    ):
        raise ReleaseArchiveError("staged installer did not prove the current package")
    output_bound = _required_int(
        report, "output_bound_bytes", label="staged installer", exact=64 * 1024
    )
    _required_int(report, "timeout_seconds", label="staged installer", exact=120)
    stdout_bytes = _required_int(report, "stdout_bytes", label="staged installer", minimum=1)
    if stdout_bytes > output_bound:
        raise ReleaseArchiveError("staged installer stdout exceeded its declared bound")
    _required_int(report, "stderr_bytes", label="staged installer", exact=0)

    manifest_contract = report.get("manifest_contract")
    if (
        not isinstance(manifest_contract, dict)
        or set(manifest_contract)
        != {
            "path",
            "sha256",
            "payload_file_count",
            "payload_inventory_sha256",
            "preseal_isolated_manifest",
        }
        or manifest_contract.get("path") != "build-manifest.json"
        or manifest_contract.get("preseal_isolated_manifest") is not True
        or not isinstance(manifest_contract.get("sha256"), str)
        or SHA256_RE.fullmatch(str(manifest_contract.get("sha256"))) is None
        or not isinstance(manifest_contract.get("payload_inventory_sha256"), str)
        or SHA256_RE.fullmatch(str(manifest_contract.get("payload_inventory_sha256"))) is None
    ):
        raise ReleaseArchiveError("staged installer manifest contract is invalid")

    expected_bootstrap = {
        "canonical_code_root": "C:\\KMTech\\Apps\\Label_Match\\current",
        "elevation_points": ["code_placement"],
        "identity_profile_created": False,
        "state_scope": "current_user_first_run",
        "exact_inventory_readback": True,
        "onedir_required": True,
    }
    if report.get("bootstrap_contract") != expected_bootstrap:
        raise ReleaseArchiveError("staged code-only bootstrap contract is invalid")
    expected_state = {
        "identity_scope": "current_user_per_pc",
        "profile_scope": "current_user",
        "credential_scope": "current_user_dpapi",
        "ledger_scope": "current_user",
        "operation_lease_store": "AUTHORITATIVE_SNAPSHOT_PRESERVED",
        "relay_persistence": "HKCU_RUN",
        "relay_port_contract": 18456,
        "source_host_override_required": False,
    }
    if report.get("state_contract") != expected_state:
        raise ReleaseArchiveError("staged current-user state contract is invalid")
    legacy_authority = report.get("legacy_authority_contract")
    if (
        not isinstance(legacy_authority, dict)
        or legacy_authority.get("system_scheduled_task_supported") is not False
        or legacy_authority.get("task_creation_tokens_absent") is not True
        or legacy_authority.get("legacy_owned_task_cleanup_only") is not True
        or legacy_authority.get("forbidden_package_members_absent") is not True
        or sorted(legacy_authority.get("forbidden_members") or [])
        != sorted(RETIRED_HELPER_EXECUTABLES)
    ):
        raise ReleaseArchiveError("staged retired task-authority contract is invalid")
    inventory = _normalize_inventory(
        report.get("original_package_inventory"), label="staged installer original package"
    )
    if inventory != sorted(inventory, key=lambda item: (str(item["path"]).casefold(), str(item["path"]))):
        raise ReleaseArchiveError("staged installer package inventory order is not canonical")
    if _inventory_digest(inventory) != report.get("original_package_inventory_sha256"):
        raise ReleaseArchiveError("staged installer package inventory digest mismatch")
    if _required_int(
        report, "original_package_file_count", label="staged installer", minimum=1
    ) != len(inventory):
        raise ReleaseArchiveError("staged installer package file count mismatch")

    public_entrypoint = report.get("public_entrypoint")
    if (
        not isinstance(public_entrypoint, dict)
        or set(public_entrypoint) != {"path", "sha256"}
        or public_entrypoint.get("path") != "INSTALL_THIS_PC.ps1"
        or public_entrypoint.get("sha256")
        != _sha256(package_root / "INSTALL_THIS_PC.ps1")
    ):
        raise ReleaseArchiveError("staged public bootstrap binding is invalid")
    runtime_host = report.get("runtime_host")
    if (
        not isinstance(runtime_host, dict)
        or runtime_host.get("path") != "Label_Match.exe"
        or runtime_host.get("sha256") != _sha256(package_root / "Label_Match.exe")
        or runtime_host.get("package_layout") != "onedir"
        or runtime_host.get("relay_execution_boundary") != "product_host"
        or runtime_host.get("current_user_relay_mode")
        != "--label-match-user-relay"
        or runtime_host.get("direct_sync_relay_mode")
        != "--label-match-direct-sync-relay"
    ):
        raise ReleaseArchiveError("staged onedir product-host binding is invalid")
    payload_inventory = [
        item for item in inventory if item["path"] != "build-manifest.json"
    ]
    payload_inventory = sorted(payload_inventory, key=lambda item: str(item["path"]))
    preseal_manifest_bytes = (
        json.dumps(
            {
                "build_manifest_schema_version": 1,
                "payload_inventory": payload_inventory,
                "payload_inventory_sha256": _canonical_sha256(payload_inventory),
            },
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if (
        len(payload_inventory) != manifest_contract["payload_file_count"]
        or _canonical_sha256(payload_inventory)
        != manifest_contract["payload_inventory_sha256"]
        or hashlib.sha256(preseal_manifest_bytes).hexdigest()
        != manifest_contract["sha256"]
    ):
        raise ReleaseArchiveError("staged installer preseal manifest binding is invalid")
    return {"report": report, "inventory": inventory}


def _validate_probe_identities(
    package_root: Path,
    *,
    expected_commit: str,
) -> str:
    executable = package_root / "KMTechActiveWorkProbe.exe"
    if not executable.is_file() or executable.stat().st_size <= 0:
        raise ReleaseArchiveError("active-work probe executable is missing")
    probe_sha = _sha256(executable)
    specs = (
        (
            "KMTechActiveWorkProbe.independent.build-identity.json",
            "independent",
            ["Label_Match"],
        ),
        (
            "KMTechActiveWorkProbe.integrated.build-identity.json",
            "integrated",
            ALL_PROBE_APPS,
        ),
    )
    for filename, mode, apps in specs:
        identity = _json(package_root / filename, filename)
        if set(identity) != {
            "schema_version",
            "probe_name",
            "probe_version",
            "probe_artifact_sha256",
            "probe_source_commit",
            "workflow_mode",
            "supported_apps",
        }:
            raise ReleaseArchiveError(f"active-work probe identity fields differ: {filename}")
        if (
            identity.get("schema_version") != PROBE_SCHEMA
            or identity.get("probe_name") != PROBE_NAME
            or identity.get("probe_version") != PROBE_VERSION
            or identity.get("probe_artifact_sha256") != probe_sha
            or identity.get("probe_source_commit") != expected_commit
            or identity.get("workflow_mode") != mode
            or identity.get("supported_apps") != apps
        ):
            raise ReleaseArchiveError(f"active-work probe identity mismatch: {filename}")
    return probe_sha


def _validate_factory_manifest(
    package_root: Path,
    *,
    identity: dict[str, str],
    staged: dict[str, object],
    expected_built_at_utc: str,
) -> dict[str, object]:
    build_identity = _json(package_root / "build-identity.json", "factory build identity")
    compatibility = _json(
        package_root / "build-compatibility.json", "factory build compatibility"
    )
    contract_lock = _json(package_root / "contract.lock.json", "factory contract lock")
    manifest = _json(package_root / "build-manifest.json", "factory build manifest")
    if (
        build_identity.get("build_identity_schema_version") != 1
        or build_identity.get("app_id") != "label_match"
        or build_identity.get("app_version") != identity["tag"]
        or build_identity.get("source_commit") != identity["commit"]
        or build_identity.get("source_tree") != identity["tree"]
        or build_identity.get("dirty") is not False
        or build_identity.get("contract_bundle_version") != "1.0.3"
        or build_identity.get("contract_bundle_corrective_revision") != 1
        or build_identity.get("python_version") != PYTHON_VERSION
        or build_identity.get("pyinstaller_version") != PYINSTALLER_VERSION
        or build_identity.get("contract_bundle_sha256") != CONTRACT_BUNDLE_SHA256
        or build_identity.get("db_schema")
        != {"current": 0, "minimum": 0, "maximum": 0}
        or build_identity.get("server_api_contract_version") != "logistics-v1"
        or build_identity.get("event_contract_version")
        != "common-event-envelope-v1"
        or build_identity.get("manifest_contract_version")
        != "producer-onboarding-manifest-v1"
        or build_identity.get("dependency") != EXPECTED_CONTRACT_LOCK["dependency"]
        or build_identity.get("builder")
        != {"name": "kmtech_factory_contracts.build_cli", "version": "1.0.3"}
    ):
        raise ReleaseArchiveError("factory build identity mismatch")
    if (
        compatibility.get("matrix_schema_version") != 1
        or compatibility.get("app_id") != "label_match"
        or compatibility.get("app_version") != identity["tag"]
        or compatibility.get("source_commit") != identity["commit"]
        or compatibility.get("source_tree") != identity["tree"]
        or compatibility.get("contract_bundle_sha256") != CONTRACT_BUNDLE_SHA256
        or compatibility.get("contract_bundle_corrective_revision") != 1
        or compatibility.get("dependency") != EXPECTED_CONTRACT_LOCK["dependency"]
        or compatibility.get("minimum_installer_version") != "1.0.3.4"
        or compatibility.get("resources") != EXPECTED_FACTORY_RESOURCES
    ):
        raise ReleaseArchiveError("factory build compatibility mismatch")
    compatibility_sha = _canonical_sha256(compatibility)
    build_identity_sha = _canonical_sha256(build_identity)
    build_identity_core = dict(build_identity)
    build_identity_core.pop("build_compatibility_sha256", None)
    build_identity_binding_sha = _canonical_sha256(build_identity_core)
    if (
        build_identity.get("build_compatibility_sha256") != compatibility_sha
        or compatibility.get("build_identity_sha256") != build_identity_binding_sha
    ):
        raise ReleaseArchiveError("factory identity and compatibility hashes differ")
    if (
        contract_lock != EXPECTED_CONTRACT_LOCK
        or build_identity.get("dependency_lock_sha256") != _canonical_sha256(contract_lock)
    ):
        raise ReleaseArchiveError("factory contract lock identity mismatch")

    exact_manifest_keys = {
        "build_manifest_schema_version",
        "app_id",
        "app_version",
        "identity_path",
        "identity_sha256",
        "build_compatibility_path",
        "contract_bundle_corrective_revision",
        "contract_bundle_sha256",
        "dependency",
        "build_compatibility_sha256",
        "payload_inventory",
        "payload_inventory_sha256",
        "expected_files",
        "built_at_utc",
    }
    if set(manifest) != exact_manifest_keys:
        raise ReleaseArchiveError("factory build manifest fields differ from the exact contract")
    if (
        manifest.get("build_manifest_schema_version") != 1
        or manifest.get("app_id") != "label_match"
        or manifest.get("app_version") != identity["tag"]
        or manifest.get("identity_path") != "build-identity.json"
        or manifest.get("identity_sha256") != build_identity_sha
        or manifest.get("build_compatibility_path") != "build-compatibility.json"
        or manifest.get("build_compatibility_sha256") != compatibility_sha
        or manifest.get("contract_bundle_sha256") != CONTRACT_BUNDLE_SHA256
        or manifest.get("contract_bundle_corrective_revision") != 1
        or manifest.get("dependency") != build_identity.get("dependency")
        or manifest.get("expected_files") != sorted(FACTORY_EXPECTED_FILES)
        or manifest.get("built_at_utc") != expected_built_at_utc
    ):
        raise ReleaseArchiveError("factory build manifest identity mismatch")
    inventory = _normalize_inventory(
        manifest.get("payload_inventory"), label="factory build manifest"
    )
    if inventory != sorted(inventory, key=lambda item: str(item["path"])):
        raise ReleaseArchiveError("factory build manifest inventory order is not canonical")
    if _canonical_sha256(inventory) != manifest.get("payload_inventory_sha256"):
        raise ReleaseArchiveError("factory build manifest inventory digest mismatch")
    current_inventory = _inventory(
        package_root, excluded={"build-manifest.json"}, casefold=False
    )
    if current_inventory != inventory:
        raise ReleaseArchiveError("current package differs from the factory build manifest")

    staged_inventory = [
        item for item in staged["inventory"] if item["path"] != "build-manifest.json"
    ]
    manifest_before_staged_report = [
        item for item in inventory if item["path"] != "staged-installer-verification.json"
    ]
    if staged_inventory != sorted(
        manifest_before_staged_report,
        key=lambda item: (str(item["path"]).casefold(), str(item["path"])),
    ):
        raise ReleaseArchiveError(
            "current package predecessor differs from staged installer evidence"
        )
    return {
        "manifest": manifest,
        "inventory": inventory,
        "payload_inventory_sha256": manifest["payload_inventory_sha256"],
    }


def validate_release_evidence(
    package_root: Path,
    *,
    expected_tag: str | None,
    source_epoch: int,
) -> dict[str, object]:
    _assert_no_reparse_points(package_root)
    identity = _validate_release_identity(package_root, expected_tag=expected_tag)
    staged = _validate_staged_installer(package_root)
    probe_sha = _validate_probe_identities(
        package_root, expected_commit=identity["commit"]
    )
    factory = _validate_factory_manifest(
        package_root,
        identity=identity,
        staged=staged,
        expected_built_at_utc=datetime.fromtimestamp(
            int(source_epoch), tz=timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )
    package_paths = {path.relative_to(package_root).as_posix() for path in _files(package_root)}
    missing = sorted(REQUIRED_PACKAGE_MEMBERS - package_paths)
    if missing:
        raise ReleaseArchiveError(f"required release package members are missing: {missing}")
    retired_present = sorted(RETIRED_HELPER_EXECUTABLES & package_paths)
    if retired_present:
        raise ReleaseArchiveError(f"retired helper executables remain packaged: {retired_present}")
    return {
        "tag": identity["tag"],
        "commit": identity["commit"],
        "tree": identity["tree"],
        "release_trust": "internal_unsigned",
        "release_identity_schema": RELEASE_IDENTITY_SCHEMA,
        "annotated_tag": True,
        "tag_signature_verified": False,
        "authenticode_required": False,
        "python_version": PYTHON_VERSION,
        "pyinstaller_version": PYINSTALLER_VERSION,
        "contract_bundle_sha256": CONTRACT_BUNDLE_SHA256,
        "payload_inventory_sha256": factory["payload_inventory_sha256"],
        "active_work_probe_sha256": probe_sha,
        "staged_installer_verified": True,
        "code_only_bootstrap_verified": True,
        "onedir_product_host_verified": True,
        "current_user_state_contract_verified": True,
        "hkcu_relay_contract_verified": True,
        "system_task_authority_absent": True,
        "authoritative_snapshot_lease_preserved": True,
        "dynamic_install_qualification": "NOT_TESTED",
        "factory_manifest_verified": True,
        "retired_helper_executables_absent": True,
    }


def _zip_datetime(source_epoch: int) -> tuple[int, int, int, int, int, int]:
    instant = datetime.fromtimestamp(max(int(source_epoch), 315532800), tz=timezone.utc)
    year = min(2107, max(1980, instant.year))
    return (
        year,
        instant.month,
        instant.day,
        instant.hour,
        instant.minute,
        instant.second // 2 * 2,
    )


def build_release_archive(
    package_root: Path,
    archive_path: Path,
    *,
    source_epoch: int,
    top_level: str = "Label_Match",
    expected_tag: str | None = None,
) -> dict[str, object]:
    package_root = package_root.resolve()
    archive_path = archive_path.resolve()
    if not package_root.is_dir():
        raise ReleaseArchiveError(f"package root is missing: {package_root}")
    if archive_path.exists():
        raise ReleaseArchiveError(f"archive already exists: {archive_path}")
    if not top_level or "/" in top_level or "\\" in top_level or top_level in {".", ".."}:
        raise ReleaseArchiveError("top-level archive directory is invalid")
    if expected_tag is not None and SEMVER_TAG_RE.fullmatch(expected_tag) is None:
        raise ReleaseArchiveError("expected tag is not strict semver")

    evidence = validate_release_evidence(
        package_root, expected_tag=expected_tag, source_epoch=source_epoch
    )
    package_inventory = _inventory(package_root)
    folded: set[str] = set()
    for item in package_inventory:
        relative = _safe_relative(item["path"], label="package")
        folded_path = relative.casefold()
        if folded_path in folded:
            raise ReleaseArchiveError(f"case-insensitive package path collision: {relative}")
        folded.add(folded_path)

    archive_path.parent.mkdir(parents=True, exist_ok=True)
    timestamp = _zip_datetime(source_epoch)
    created = False
    try:
        with zipfile.ZipFile(
            archive_path,
            "x",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=9,
            allowZip64=True,
        ) as archive:
            created = True
            for source in _files(package_root):
                relative = source.relative_to(package_root).as_posix()
                info = zipfile.ZipInfo(f"{top_level}/{relative}", date_time=timestamp)
                info.compress_type = zipfile.ZIP_DEFLATED
                info.create_system = 3
                info.external_attr = (stat.S_IFREG | 0o644) << 16
                with source.open("rb") as input_handle, archive.open(info, "w") as output_handle:
                    for chunk in iter(lambda: input_handle.read(1024 * 1024), b""):
                        output_handle.write(chunk)

        with zipfile.ZipFile(archive_path, "r") as archive:
            bad_member = archive.testzip()
            if bad_member is not None:
                raise ReleaseArchiveError(f"archive CRC failed: {bad_member}")
            infos = archive.infolist()
            if any(info.is_dir() for info in infos):
                raise ReleaseArchiveError("archive contains an unexpected directory entry")
            names = [info.filename for info in infos]
            if len(names) != len(set(names)) or len({name.casefold() for name in names}) != len(names):
                raise ReleaseArchiveError("archive contains duplicate or case-colliding paths")
            expected_names = {f"{top_level}/{item['path']}" for item in package_inventory}
            if set(names) != expected_names:
                raise ReleaseArchiveError("archive file membership differs from the staged package")
            by_path = {str(item["path"]): item for item in package_inventory}
            for info in infos:
                pure = PurePosixPath(info.filename)
                if pure.is_absolute() or ".." in pure.parts or pure.parts[0] != top_level:
                    raise ReleaseArchiveError(f"unsafe archive path: {info.filename}")
                relative = PurePosixPath(*pure.parts[1:]).as_posix()
                digest = hashlib.sha256()
                with archive.open(info, "r") as handle:
                    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                        digest.update(chunk)
                expected = by_path[relative]
                if info.file_size != expected["size"] or digest.hexdigest() != expected["sha256"]:
                    raise ReleaseArchiveError(f"archive byte parity failed: {info.filename}")
    except Exception:
        if created:
            archive_path.unlink(missing_ok=True)
        raise

    main_exe_sha256 = next(
        str(item["sha256"])
        for item in package_inventory
        if item["path"] == "Label_Match.exe"
    )
    return {
        "schema_version": "label-match-release-archive-verification-v1",
        "status": "PASS",
        "archive": archive_path.name,
        "archive_sha256": _sha256(archive_path),
        "archive_size": archive_path.stat().st_size,
        "main_exe_sha256": main_exe_sha256,
        "source_epoch": int(source_epoch),
        "normalized_zip_timestamp_utc": "%04d-%02d-%02dT%02d:%02d:%02dZ" % timestamp,
        "top_level": top_level,
        "package_file_count": len(package_inventory),
        "package_total_bytes": sum(int(item["size"]) for item in package_inventory),
        "exact_membership": True,
        "byte_parity": True,
        "crc_verified": True,
        "deterministic_metadata": True,
        **evidence,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build and verify the unsigned Label_Match pre-push release ZIP"
    )
    parser.add_argument("--package-root", required=True)
    parser.add_argument("--archive", required=True)
    parser.add_argument("--source-epoch", required=True, type=int)
    parser.add_argument("--top-level", default="Label_Match")
    parser.add_argument("--expected-tag", default="")
    parser.add_argument("--report", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = build_release_archive(
            Path(args.package_root),
            Path(args.archive),
            source_epoch=args.source_epoch,
            top_level=args.top_level,
            expected_tag=args.expected_tag or None,
        )
        report = Path(args.report)
        if report.exists():
            raise ReleaseArchiveError(f"archive report already exists: {report}")
        report.parent.mkdir(parents=True, exist_ok=True)
        with report.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    except (OSError, ValueError, zipfile.BadZipFile, ReleaseArchiveError) as exc:
        print(f"release_archive=DENY reason={exc}")
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

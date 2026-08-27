#!/usr/bin/env python
"""Verify the staged Label_Match code-only bootstrap package."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
from typing import Any, Mapping, Sequence


REPORT_SCHEMA = "label-match-staged-installer-verification-v3"
OUTPUT_BOUND_BYTES = 64 * 1024
TIMEOUT_SECONDS = 120
REQUIRED_MEMBERS = {
    "INSTALL_THIS_PC.ps1",
    "Label_Match.exe",
    "contract.lock.json",
    "_internal/config/app_settings.json",
}
FORBIDDEN_ACTIVE_AUTHORITY_MEMBERS = {
    "install_label_match_direct_sync.ps1",
    "tools/direct_sync_relay_install_pack.py",
    "tools/direct_sync_relay_install_pack.exe",
    "tools/direct_sync_relay_install_pack/direct_sync_relay_install_pack.exe",
    "tools/direct_sync_relay_runner/direct_sync_relay_runner.exe",
    "tools/register_label_match_worker_pc.exe",
    "tools/invoke_embedded_python.ps1",
}


class StagedInstallerVerificationError(RuntimeError):
    """Raised when the staged package violates the supported topology."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(value: object) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _files(root: Path) -> list[Path]:
    return sorted(
        (path for path in root.rglob("*") if path.is_file()),
        key=lambda path: (
            path.relative_to(root).as_posix().casefold(),
            path.relative_to(root).as_posix(),
        ),
    )


def _inventory(root: Path) -> list[dict[str, object]]:
    return [
        {
            "path": path.relative_to(root).as_posix(),
            "size": path.stat().st_size,
            "sha256": _sha256(path),
        }
        for path in _files(root)
        if path.name not in {"staged-installer-verification.json", "build-manifest.json"}
    ]


def _inventory_digest(inventory: list[dict[str, object]]) -> str:
    payload = json.dumps(
        inventory,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _assert_safe_package(package_root: Path) -> set[str]:
    if not package_root.is_dir():
        raise StagedInstallerVerificationError("package root is absent")
    paths: set[str] = set()
    folded: set[str] = set()
    root = package_root.resolve()
    for candidate in package_root.rglob("*"):
        if candidate.is_symlink():
            raise StagedInstallerVerificationError(
                f"package reparse/symlink entry is forbidden: {candidate}"
            )
        if not candidate.is_file():
            continue
        relative = candidate.resolve().relative_to(root).as_posix()
        pure = PurePosixPath(relative)
        if pure.is_absolute() or any(part in {"", ".", ".."} for part in pure.parts):
            raise StagedInstallerVerificationError("package member path is unsafe")
        key = relative.casefold()
        if key in folded:
            raise StagedInstallerVerificationError("case-colliding package members exist")
        folded.add(key)
        paths.add(relative)
    missing = sorted(REQUIRED_MEMBERS - paths)
    if missing:
        raise StagedInstallerVerificationError(
            f"required staged members are absent: {missing}"
        )
    forbidden = sorted(FORBIDDEN_ACTIVE_AUTHORITY_MEMBERS & paths)
    if forbidden:
        raise StagedInstallerVerificationError(
            f"retired active task-authority members remain packaged: {forbidden}"
        )
    if not (package_root / "_internal").is_dir():
        raise StagedInstallerVerificationError("onedir _internal runtime is absent")
    return paths


def _assert_bootstrap_source(installer: Path) -> None:
    source = installer.read_text(encoding="utf-8-sig")
    lowered = source.casefold()
    required = (
        "invoke-selfelevated",
        "bootstrap-integrity.json",
        "state_scope = 'current_user_first_run'",
        "package_layout = 'onedir'",
        "kmtech.labelmatch.relay",
        "remove-ownedlegacytask",
    )
    missing = [token for token in required if token not in lowered]
    if missing:
        raise StagedInstallerVerificationError(
            f"code-only bootstrap contract is incomplete: {missing}"
        )
    forbidden = (
        "new-scheduledtasktrigger",
        "new-scheduledtaskprincipal",
        "schtasks.exe",
        "install_label_match_direct_sync.ps1",
        "--source-host-id",
        "sourcehostid",
    )
    present = [token for token in forbidden if token in lowered]
    if re.search(r"(?im)^\s*Register-ScheduledTask\b", source):
        present.append("register-scheduledtask")
    if present:
        raise StagedInstallerVerificationError(
            f"bootstrap exposes retired enrollment/task authority: {present}"
        )


def _run_bootstrap_dry_run(package_root: Path) -> tuple[bytes, bytes]:
    powershell = (
        Path(os.environ.get("SystemRoot", r"C:\Windows"))
        / "System32"
        / "WindowsPowerShell"
        / "v1.0"
        / "powershell.exe"
    )
    if not powershell.is_file():
        raise StagedInstallerVerificationError("Windows PowerShell is unavailable")
    install_root = package_root.parent / ".label-match-staged-bootstrap-dry-run" / "current"
    env = dict(os.environ)
    env["KMTECH_FACTORY_INSTALL_TEST_MODE"] = "1"
    completed = subprocess.run(
        [
            str(powershell),
            "-NoLogo",
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(package_root / "INSTALL_THIS_PC.ps1"),
            "-DryRun",
            "-SourceRoot",
            str(package_root),
            "-InstallRoot",
            str(install_root),
            "-AllowNoncanonicalLayoutForTest",
        ],
        check=False,
        capture_output=True,
        timeout=TIMEOUT_SECONDS,
        env=env,
    )
    stdout = completed.stdout[: OUTPUT_BOUND_BYTES + 1]
    stderr = completed.stderr[: OUTPUT_BOUND_BYTES + 1]
    if len(stdout) > OUTPUT_BOUND_BYTES or len(stderr) > OUTPUT_BOUND_BYTES:
        raise StagedInstallerVerificationError("bootstrap output exceeded its bound")
    if completed.returncode != 0:
        raise StagedInstallerVerificationError(
            f"bootstrap dry run failed with exit code {completed.returncode}"
        )
    decoded = stdout.decode("utf-8", errors="replace")
    for token in (
        "bootstrap_status=DRY_RUN",
        "identity_profile_created=false",
        "elevation_points=1:code_placement",
    ):
        if token not in decoded:
            raise StagedInstallerVerificationError(
                f"bootstrap dry-run output omitted {token}"
            )
    if stderr:
        raise StagedInstallerVerificationError("bootstrap dry run wrote stderr")
    if install_root.exists():
        raise StagedInstallerVerificationError("bootstrap dry run changed install state")
    return stdout, stderr


def verify_staged_package(package_root: Path) -> dict[str, Any]:
    package_root = package_root.resolve()
    paths_before = _assert_safe_package(package_root)
    installer = package_root / "INSTALL_THIS_PC.ps1"
    _assert_bootstrap_source(installer)
    inventory = _inventory(package_root)
    stdout, stderr = _run_bootstrap_dry_run(package_root)
    paths_after = _assert_safe_package(package_root)
    if paths_after != paths_before or _inventory(package_root) != inventory:
        raise StagedInstallerVerificationError("bootstrap dry run changed package bytes")
    ordered_inventory = sorted(inventory, key=lambda item: str(item["path"]))
    preseal_manifest = {
        "build_manifest_schema_version": 1,
        "payload_inventory": ordered_inventory,
        "payload_inventory_sha256": _canonical_sha256(ordered_inventory),
    }
    preseal_bytes = (
        json.dumps(preseal_manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    return {
        "schema_version": REPORT_SCHEMA,
        "status": "PASS",
        "proof_classification": "STATIC_ISOLATED_DRY_RUN",
        "dynamic_qualification": "NOT_TESTED",
        "public_entrypoint": {
            "path": "INSTALL_THIS_PC.ps1",
            "sha256": _sha256(installer),
        },
        "runtime_host": {
            "path": "Label_Match.exe",
            "sha256": _sha256(package_root / "Label_Match.exe"),
            "package_layout": "onedir",
            "relay_execution_boundary": "product_host",
            "current_user_relay_mode": "--label-match-user-relay",
            "direct_sync_relay_mode": "--label-match-direct-sync-relay",
        },
        "bootstrap_contract": {
            "canonical_code_root": r"C:\KMTech\Apps\Label_Match\current",
            "elevation_points": ["code_placement"],
            "identity_profile_created": False,
            "state_scope": "current_user_first_run",
            "exact_inventory_readback": True,
            "onedir_required": True,
        },
        "state_contract": {
            "identity_scope": "current_user_per_pc",
            "profile_scope": "current_user",
            "credential_scope": "current_user_dpapi",
            "ledger_scope": "current_user",
            "operation_lease_store": "AUTHORITATIVE_SNAPSHOT_PRESERVED",
            "relay_persistence": "HKCU_RUN",
            "relay_port_contract": 18456,
            "source_host_override_required": False,
        },
        "legacy_authority_contract": {
            "system_scheduled_task_supported": False,
            "task_creation_tokens_absent": True,
            "legacy_owned_task_cleanup_only": True,
            "forbidden_package_members_absent": True,
            "forbidden_members": sorted(FORBIDDEN_ACTIVE_AUTHORITY_MEMBERS),
        },
        "manifest_contract": {
            "path": "build-manifest.json",
            "sha256": hashlib.sha256(preseal_bytes).hexdigest(),
            "payload_file_count": len(inventory),
            "payload_inventory_sha256": preseal_manifest[
                "payload_inventory_sha256"
            ],
            "preseal_isolated_manifest": True,
        },
        "original_package_file_count": len(inventory),
        "original_package_inventory": inventory,
        "original_package_inventory_sha256": _inventory_digest(inventory),
        "original_package_unchanged": True,
        "system_python_required": False,
        "output_bound_bytes": OUTPUT_BOUND_BYTES,
        "timeout_seconds": TIMEOUT_SECONDS,
        "stdout_bytes": len(stdout),
        "stderr_bytes": len(stderr),
    }


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(dict(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package-root", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        report = verify_staged_package(args.package_root)
        _write_json(args.report, report)
    except Exception as exc:
        print(f"staged_installer_status=FAIL error_type={exc.__class__.__name__}")
        return 1
    print("staged_installer_status=PASS")
    print(f"staged_installer_report={args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

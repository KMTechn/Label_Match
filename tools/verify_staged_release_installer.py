#!/usr/bin/env python
"""Prove the packaged public install contract in an isolated Windows dry run."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import tempfile
from typing import Sequence


class StagedInstallerVerificationError(RuntimeError):
    """Raised when the staged package cannot prove its installer wiring."""


SCHEMA_VERSION = "label-match-staged-installer-verification-v2"
CANONICAL_INSTALL_ROOT = r"C:\KMTech\Apps\Label_Match\current"
CANONICAL_DIRECT_SYNC_ROOT = r"C:\ProgramData\KMTech\DirectSync\label_match"
CANONICAL_TASK_NAME = "direct-sync-relay-label-match"
CANONICAL_TASK_LAUNCHER_PATH = (
    r"C:\ProgramData\KMTech\DirectSync\label_match\bin\run_direct-sync-relay-label-match.vbs"
)
CANONICAL_STATE_DB_PATH = (
    r"C:\ProgramData\KMTech\DirectSync\label_match\queue\direct_sync_relay.sqlite3"
)
CANONICAL_START_MENU_LAUNCHER = (
    r"C:\ProgramData\Microsoft\Windows\Start Menu\Programs\KMTech\Label Match.lnk"
)
APP_INVENTORY_CONTRACT = "label-match-app-immutable-inventory-v1"
MUTABLE_APP_RELATIVE_PATHS = ["_internal/config/app_settings.json"]
MAXIMUM_OUTPUT_BYTES = 64 * 1024
MAXIMUM_RUNTIME_SECONDS = 120
REQUIRED_PUBLIC_MEMBERS = {
    "INSTALL_THIS_PC.ps1",
    "install_label_match_direct_sync.ps1",
    "Label_Match.exe",
    "build-manifest.json",
    "_internal/python312.dll",
    "_internal/base_library.zip",
    "tools/invoke_embedded_python.ps1",
    "tools/direct_sync_relay_install_pack.py",
    "tools/direct_sync_relay_runner.exe",
    "tools/direct_sync_relay_runner.py",
    "tools/register_label_match_worker_pc.py",
}
RETIRED_HELPER_EXECUTABLES = {
    "tools/direct_sync_relay_install_pack/direct_sync_relay_install_pack.exe",
    "tools/direct_sync_relay_install_pack.exe",
    "tools/register_label_match_worker_pc.exe",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_reparse_point(path: Path) -> bool:
    try:
        attributes = getattr(path.lstat(), "st_file_attributes", 0)
    except OSError:
        return True
    return path.is_symlink() or bool(attributes & getattr(os, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))


def _inventory(root: Path) -> list[dict[str, object]]:
    return [
        {
            "path": path.relative_to(root).as_posix(),
            "size": path.stat().st_size,
            "sha256": _sha256(path),
        }
        for path in sorted(
            (candidate for candidate in root.rglob("*") if candidate.is_file()),
            key=lambda candidate: (
                candidate.relative_to(root).as_posix().casefold(),
                candidate.relative_to(root).as_posix(),
            ),
        )
    ]


def _inventory_digest(inventory: list[dict[str, object]]) -> str:
    canonical = json.dumps(
        inventory,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _canonical_path(path: Path | str) -> str:
    """Resolve Windows aliases (including 8.3 names) before comparing paths."""

    absolute = os.path.abspath(os.fspath(path))
    return os.path.normcase(os.path.normpath(os.path.realpath(absolute)))


def _same_path(left: Path | str, right: Path | str) -> bool:
    return _canonical_path(left) == _canonical_path(right)


def _safe_manifest_relative(value: object) -> str:
    text = str(value or "")
    pure = PurePosixPath(text)
    if (
        not text
        or "\\" in text
        or ":" in text
        or text.startswith("/")
        or text.endswith("/")
        or pure.is_absolute()
        or any(part in {"", ".", ".."} for part in pure.parts)
        or pure.as_posix() != text
    ):
        raise StagedInstallerVerificationError(f"unsafe build-manifest payload path: {text!r}")
    return text


def _validate_manifest_payload(package_root: Path) -> dict[str, object]:
    for candidate in package_root.rglob("*"):
        if _is_reparse_point(candidate):
            raise StagedInstallerVerificationError(
                f"package contains a reparse point: {candidate}"
            )
    manifest_path = package_root / "build-manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError) as exc:
        raise StagedInstallerVerificationError("build manifest is missing or invalid") from exc
    if not isinstance(manifest, dict) or manifest.get("build_manifest_schema_version") != 1:
        raise StagedInstallerVerificationError("build manifest schema is unsupported")
    inventory = manifest.get("payload_inventory")
    if not isinstance(inventory, list):
        raise StagedInstallerVerificationError("build manifest payload inventory is missing")
    normalized: list[dict[str, object]] = []
    folded: set[str] = set()
    for entry in inventory:
        if not isinstance(entry, dict) or set(entry) != {"path", "size", "sha256"}:
            raise StagedInstallerVerificationError("build manifest payload entry fields differ")
        relative = _safe_manifest_relative(entry.get("path"))
        casefolded = relative.casefold()
        if casefolded in folded:
            raise StagedInstallerVerificationError("build manifest has a case-colliding payload path")
        folded.add(casefolded)
        size = entry.get("size")
        sha256 = entry.get("sha256")
        if (
            not isinstance(size, int)
            or isinstance(size, bool)
            or size < 0
            or not isinstance(sha256, str)
            or len(sha256) != 64
            or any(character not in "0123456789abcdef" for character in sha256)
        ):
            raise StagedInstallerVerificationError("build manifest payload metadata is invalid")
        target = package_root / PurePosixPath(relative)
        if not target.is_file() or _is_reparse_point(target):
            raise StagedInstallerVerificationError(f"manifest payload file is missing or unsafe: {relative}")
        if target.stat().st_size != size or _sha256(target) != sha256:
            raise StagedInstallerVerificationError(f"manifest payload byte mismatch: {relative}")
        normalized.append({"path": relative, "size": size, "sha256": sha256})
    if normalized != sorted(normalized, key=lambda item: str(item["path"])):
        raise StagedInstallerVerificationError("build manifest payload inventory is not canonical")
    actual = [
        item
        for item in _inventory(package_root)
        if item["path"] != "build-manifest.json"
    ]
    if actual != sorted(normalized, key=lambda item: (str(item["path"]).casefold(), str(item["path"]))):
        raise StagedInstallerVerificationError("package contains missing or unexpected payload files")
    missing = sorted(REQUIRED_PUBLIC_MEMBERS - {item["path"] for item in _inventory(package_root)})
    if missing:
        raise StagedInstallerVerificationError(f"public installer members are missing: {missing}")
    if manifest.get("payload_inventory_sha256") != _inventory_digest(normalized):
        raise StagedInstallerVerificationError("build manifest payload inventory digest mismatch")
    return {
        "path": "build-manifest.json",
        "sha256": _sha256(manifest_path),
        "payload_file_count": len(normalized),
        "payload_inventory_sha256": manifest["payload_inventory_sha256"],
        "hashes_and_sizes_verified": True,
        "safe_paths_verified": True,
        "case_collisions_absent": True,
        "unexpected_files_absent": True,
        "reparse_points_absent": True,
        "preseal_isolated_manifest": True,
    }


def _write_preseal_manifest(package_root: Path) -> None:
    manifest_path = package_root / "build-manifest.json"
    manifest_path.unlink(missing_ok=True)
    inventory = sorted(_inventory(package_root), key=lambda item: str(item["path"]))
    manifest = {
        "build_manifest_schema_version": 1,
        "payload_inventory": inventory,
        "payload_inventory_sha256": _inventory_digest(inventory),
    }
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _load_json(path: Path, label: str) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError) as exc:
        raise StagedInstallerVerificationError(f"{label} is missing or invalid JSON") from exc
    if not isinstance(payload, dict):
        raise StagedInstallerVerificationError(f"{label} is not a JSON object")
    return payload


def _bounded_text(path: Path) -> tuple[str, int]:
    size = path.stat().st_size if path.exists() else 0
    if size > MAXIMUM_OUTPUT_BYTES:
        raise StagedInstallerVerificationError(
            f"installer output exceeded {MAXIMUM_OUTPUT_BYTES} bytes: {path.name}={size}"
        )
    return (path.read_text(encoding="utf-8", errors="replace") if path.exists() else "", size)


def verify_staged_installer(package_root: Path) -> dict[str, object]:
    if os.name != "nt":
        raise StagedInstallerVerificationError("staged installer verification requires Windows")
    package_root = package_root.resolve()
    public_entrypoint = package_root / "INSTALL_THIS_PC.ps1"
    installer = package_root / "install_label_match_direct_sync.ps1"
    powershell = shutil.which("powershell.exe") or shutil.which("pwsh.exe") or shutil.which("pwsh")
    if not powershell:
        raise StagedInstallerVerificationError("PowerShell is required")

    original_inventory = _inventory(package_root)
    original_paths = {str(item["path"]) for item in original_inventory}
    retired_present = sorted(RETIRED_HELPER_EXECUTABLES & original_paths)
    if retired_present:
        raise StagedInstallerVerificationError(
            f"retired helper executables remain packaged: {retired_present}"
        )
    with tempfile.TemporaryDirectory(prefix="label-match-staged-installer-") as temp_dir:
        root = Path(temp_dir)
        extracted_root = root / "ordinary-extraction" / "Label_Match"
        shutil.copytree(package_root, extracted_root)
        _write_preseal_manifest(extracted_root)
        manifest_contract = _validate_manifest_payload(extracted_root)
        install_root = root / "installed" / "current"
        program_data = root / "runtime" / "direct-sync"
        scan_source = root / "runtime" / "label-data"
        common_programs = root / "common-programs"
        receipt_root = root / "installer-receipts"
        stdout_path = root / "stdout.txt"
        stderr_path = root / "stderr.txt"
        env = dict(os.environ)
        env["KMTECH_FACTORY_INSTALL_TEST_MODE"] = "1"
        env["PATH"] = str(Path(powershell).resolve().parent)
        command = [
            powershell,
            "-NoProfile",
            "-NonInteractive",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(extracted_root / public_entrypoint.name),
            "-DryRun",
            "-AllowNoncanonicalLayoutForTest",
            "-InstallRootForTest",
            str(install_root),
            "-ProgramDataRoot",
            str(program_data),
            "-ScanSourceDir",
            str(scan_source),
            "-CommonProgramsRootForTest",
            str(common_programs),
            "-RollbackReceiptRootForTest",
            str(receipt_root),
        ]
        with stdout_path.open("wb") as stdout_handle, stderr_path.open("wb") as stderr_handle:
            try:
                completed = subprocess.run(
                    command,
                    check=False,
                    stdout=stdout_handle,
                    stderr=stderr_handle,
                    env=env,
                    timeout=MAXIMUM_RUNTIME_SECONDS,
                )
            except subprocess.TimeoutExpired as exc:
                raise StagedInstallerVerificationError("public installer dry run exceeded its time bound") from exc
        stdout, stdout_bytes = _bounded_text(stdout_path)
        stderr, stderr_bytes = _bounded_text(stderr_path)
        if completed.returncode != 0:
            detail = (stderr.strip() or stdout.strip())[:2048]
            raise StagedInstallerVerificationError(
                f"public installer dry run failed with exit {completed.returncode}: {detail}"
            )
        if stderr.strip():
            raise StagedInstallerVerificationError("public installer dry run wrote stderr")

        public_report = _load_json(
            receipt_root / "label_match_public_install_report.json", "public install report"
        )
        install_report = _load_json(
            program_data / "status" / "label_match_direct_sync_install.json",
            "direct-sync install report",
        )
        summary = _load_json(
            program_data / "status" / "label_match_one_step_install_summary.json",
            "one-step install summary",
        )
        if public_report.get("report_version") != "label-match-public-install-v2":
            raise StagedInstallerVerificationError("public install report schema is not v2")
        if public_report.get("status") != "DRY_RUN_STAGED":
            raise StagedInstallerVerificationError("public install did not report isolated staging")
        if public_report.get("source_manifest_sha256") != manifest_contract["sha256"]:
            raise StagedInstallerVerificationError("public install did not bind the source manifest")
        staging = public_report.get("staging")
        if not isinstance(staging, dict) or any(
            staging.get(field) is not True
            for field in (
                "ordinary_extracted_root_supported",
                "manifest_validated",
                "manifest_hashes_and_sizes_verified",
                "safe_relative_paths_verified",
                "unexpected_payload_files_absent",
                "same_volume_candidate_verified",
                "candidate_byte_parity_verified",
                "atomic_rename_used",
                "nested_label_match_directory_absent",
            )
        ):
            raise StagedInstallerVerificationError("ordinary-root staging evidence is incomplete")
        if not install_root.is_dir() or (install_root / "Label_Match").exists():
            raise StagedInstallerVerificationError("public entrypoint produced a nested install layout")
        if not _same_path(public_report.get("install_root", ""), install_root):
            raise StagedInstallerVerificationError("public staging report names the wrong install root")
        if not isinstance(staging.get("created_install_parent_paths"), list) or not isinstance(
            staging.get("created_receipt_directory_paths"), list
        ):
            raise StagedInstallerVerificationError("public staging did not record directory ancestry")

        launcher = public_report.get("launcher_contract")
        expected_launcher = common_programs / "KMTech" / "Label Match.lnk"
        expected_exe = install_root / "Label_Match.exe"
        if (
            not isinstance(launcher, dict)
            or launcher.get("count") != 1
            or launcher.get("scope") != "all_users"
            or not _same_path(launcher.get("path", ""), expected_launcher)
            or not _same_path(launcher.get("target", ""), expected_exe)
            or not _same_path(launcher.get("working_directory", ""), install_root)
            or not _same_path(launcher.get("icon", ""), expected_exe)
        ):
            raise StagedInstallerVerificationError("all-users launcher contract is incomplete")
        removal = public_report.get("removal_contract")
        if (
            not isinstance(removal, dict)
            or removal.get("uninstall") != "DATA_PRESERVING_UNINSTALL"
            or removal.get("rollback") != "EXACT_FRESH_TARGET_ROLLBACK"
            or removal.get("task_operations") != ["stop", "delete", "absence"]
            or removal.get("task_results_are_typed") is not True
            or removal.get("rollback_evidence_external") is not True
            or removal.get("evidence_maximum_files") != 10000
            or removal.get("evidence_maximum_bytes") != 2147483648
            or removal.get("no_plaintext_secrets_in_reports") is not True
            or removal.get("fresh_evidence_root_required") is not True
            or removal.get("reparse_points_rejected") is not True
            or removal.get("directory_ancestry_tracked") is not True
            or removal.get("typed_task_reports_bound_to_phase_and_identity") is not True
            or removal.get("public_wrapper_finalizes_rollback_report") is not True
            or removal.get("final_evidence_bytes_reverified") is not True
            or removal.get("final_receipt_binds_evidence_hashes") is not True
            or removal.get("app_inventory_contract") != APP_INVENTORY_CONTRACT
            or removal.get("mutable_app_relative_paths") != MUTABLE_APP_RELATIVE_PATHS
            or removal.get("immutable_app_drift_rejected") is not True
        ):
            raise StagedInstallerVerificationError("removal lifecycle contract is incomplete")

        if install_report.get("status") != "DRY_RUN":
            raise StagedInstallerVerificationError("direct-sync installer report status is not DRY_RUN")
        field_layout = install_report.get("field_layout_contract")
        if not isinstance(field_layout, dict):
            raise StagedInstallerVerificationError("installer field-layout evidence is missing")
        expected_layout = {
            "expected_install_root": CANONICAL_INSTALL_ROOT,
            "expected_direct_sync_root": CANONICAL_DIRECT_SYNC_ROOT,
            "expected_task_launcher_path": CANONICAL_TASK_LAUNCHER_PATH,
            "expected_state_db_path": CANONICAL_STATE_DB_PATH,
        }
        for field_name, expected_path in expected_layout.items():
            if not _same_path(str(field_layout.get(field_name) or ""), expected_path):
                raise StagedInstallerVerificationError(
                    f"installer field-layout {field_name} is not canonical"
                )
        if field_layout.get("expected_task_name") != CANONICAL_TASK_NAME:
            raise StagedInstallerVerificationError("installer field-layout task name is not canonical")
        if field_layout.get("local_test_override_enabled") is not True:
            raise StagedInstallerVerificationError("isolated staged test override was not explicit")

        expected_settings = (
            install_root / "_internal/config/app_settings.json"
            if (install_root / "_internal").is_dir()
            else install_root / "config/app_settings.json"
        )
        if not _same_path(str(install_report.get("app_settings_path") or ""), expected_settings):
            raise StagedInstallerVerificationError("installer did not bind staged app settings")
        settings = _load_json(expected_settings, "staged app settings")
        if not _same_path(str(settings.get("custom_save_path") or ""), scan_source):
            raise StagedInstallerVerificationError("staged app save path differs from relay scan source")

        runner = install_root / "tools" / "direct_sync_relay_runner.exe"
        runner_source = install_root / "tools" / "direct_sync_relay_runner.py"
        registration = install_root / "tools" / "register_label_match_worker_pc.py"
        install_helper = install_root / "tools" / "direct_sync_relay_install_pack.py"
        embedded_python_host = install_root / "tools" / "invoke_embedded_python.ps1"
        runner_command = install_report.get("runner_command")
        baseline_command = install_report.get("source_scan_baseline_command")
        self_enrollment = install_report.get("self_enrollment")
        if (
            not isinstance(runner_command, list)
            or not runner_command
            or not _same_path(runner_command[0], runner)
            or not _same_path(str(install_report.get("runner_exe") or ""), runner)
            or install_report.get("runner_command_mode") != "bundled_executable"
            or not isinstance(baseline_command, list)
            or not baseline_command
            or not _same_path(baseline_command[0], runner_source)
            or not isinstance(self_enrollment, dict)
            or self_enrollment.get("registration_command_mode") != "in_process_source"
            or self_enrollment.get("registration_executable") != ""
            or summary.get("installer_execution_mode") != "in_process_embedded_python"
        ):
            raise StagedInstallerVerificationError("in-process runtime selection is not proven")
        if summary.get("installer_report_version") != "label-match-direct-sync-one-step-install-v2":
            raise StagedInstallerVerificationError("one-step install summary schema is not v2")
        lifecycle = summary.get("lifecycle_contract")
        if not isinstance(lifecycle, dict) or lifecycle.get("task_removal_order") != [
            "stop",
            "delete",
            "absence",
        ]:
            raise StagedInstallerVerificationError("one-step lifecycle summary is incomplete")
        if any(
            lifecycle.get(field) is not True
            for field in (
                "fresh_evidence_root_required",
                "reparse_points_rejected",
                "directory_ancestry_tracked",
                "typed_task_reports_bound_to_phase_and_identity",
                "public_wrapper_finalizes_rollback_report",
                "final_evidence_bytes_reverified",
                "final_receipt_binds_evidence_hashes",
            )
        ):
            raise StagedInstallerVerificationError("one-step lifecycle safety claims are incomplete")
        if (
            lifecycle.get("app_inventory_contract") != APP_INVENTORY_CONTRACT
            or lifecycle.get("mutable_app_relative_paths") != MUTABLE_APP_RELATIVE_PATHS
            or lifecycle.get("immutable_app_drift_rejected") is not True
        ):
            raise StagedInstallerVerificationError("one-step app inventory lifecycle claim is incomplete")
        created_directories = (summary.get("resources") or {}).get("created_directory_paths")
        if (
            not isinstance(created_directories, dict)
            or set(created_directories)
            != {"data_root", "direct_sync_root", "machine_profile_root", "launcher_parent"}
            or any(not isinstance(value, list) for value in created_directories.values())
        ):
            raise StagedInstallerVerificationError("one-step created-directory ancestry is incomplete")
        app_root_resource = (summary.get("resources") or {}).get("app_root")
        if (
            not isinstance(app_root_resource, dict)
            or app_root_resource.get("inventory_contract") != APP_INVENTORY_CONTRACT
            or app_root_resource.get("mutable_relative_paths") != MUTABLE_APP_RELATIVE_PATHS
            or not isinstance(app_root_resource.get("immutable_file_count"), int)
            or app_root_resource.get("immutable_file_count", 0) < 1
            or not isinstance(app_root_resource.get("immutable_inventory_sha256"), str)
            or len(app_root_resource.get("immutable_inventory_sha256", "")) != 64
            or any(
                character not in "0123456789abcdef"
                for character in app_root_resource.get("immutable_inventory_sha256", "")
            )
        ):
            raise StagedInstallerVerificationError("one-step app inventory contract is incomplete")
        if _inventory(package_root) != original_inventory:
            raise StagedInstallerVerificationError("verification mutated the original staged package")

        return {
            "schema_version": SCHEMA_VERSION,
            "status": "PASS",
            "proof_classification": "STATIC_ISOLATED_DRY_RUN",
            "dynamic_qualification": "NOT_TESTED",
            "public_entrypoint": {"path": "INSTALL_THIS_PC.ps1", "sha256": _sha256(public_entrypoint)},
            "installer": {"path": "install_label_match_direct_sync.ps1", "sha256": _sha256(installer)},
            "install_helper": {
                "path": "tools/direct_sync_relay_install_pack.py",
                "sha256": _sha256(install_helper),
                "execution_boundary": "in_process",
            },
            "runner": {"path": "tools/direct_sync_relay_runner.exe", "sha256": _sha256(runner), "selected": True, "execution_boundary": "scheduled_task"},
            "registration": {"path": "tools/register_label_match_worker_pc.py", "sha256": _sha256(registration), "selected": True, "execution_boundary": "in_process"},
            "embedded_python_host": {"path": "tools/invoke_embedded_python.ps1", "sha256": _sha256(embedded_python_host)},
            "retired_helper_executables_absent": True,
            "manifest_contract": manifest_contract,
            "staging_contract": {
                "ordinary_extracted_root": True,
                "canonical_production_root_declared": CANONICAL_INSTALL_ROOT,
                "direct_children_staged": True,
                "nested_label_match_directory_absent": True,
                "candidate_byte_parity_verified": True,
                "unknown_target_fail_closed_declared": True,
                "directory_ancestry_tracked": True,
            },
            "launcher_contract": {
                "count": 1,
                "scope": "all_users",
                "canonical_path": CANONICAL_START_MENU_LAUNCHER,
                "target_relative": "Label_Match.exe",
                "working_directory_is_install_root": True,
                "icon_is_target": True,
                "install_verify_remove_lifecycle_declared": True,
            },
            "removal_contract": {
                "uninstall_mode": "DATA_PRESERVING_UNINSTALL",
                "uninstall_preserves_business_data": True,
                "rollback_mode": "EXACT_FRESH_TARGET_ROLLBACK",
                "rollback_requires_external_evidence": True,
                "rollback_requires_absent_prestate": True,
                "task_operations": ["stop", "delete", "absence"],
                "task_results_are_typed": True,
                "bounded_external_evidence": True,
                "maximum_evidence_files": 10000,
                "maximum_evidence_bytes": 2147483648,
                "fresh_evidence_root_required": True,
                "reparse_points_rejected": True,
                "directory_ancestry_tracked": True,
                "typed_task_reports_bound_to_phase_and_identity": True,
                "public_wrapper_finalizes_rollback_report": True,
                "final_evidence_bytes_reverified": True,
                "final_receipt_binds_evidence_hashes": True,
                "app_inventory_contract": APP_INVENTORY_CONTRACT,
                "mutable_app_relative_paths": MUTABLE_APP_RELATIVE_PATHS,
                "immutable_app_drift_rejected": True,
            },
            "field_layout_contract_verified": True,
            "system_python_required": False,
            "original_package_file_count": len(original_inventory),
            "original_package_inventory": original_inventory,
            "original_package_inventory_sha256": _inventory_digest(original_inventory),
            "original_package_unchanged": True,
            "app_settings_path": expected_settings.relative_to(install_root).as_posix(),
            "app_save_path_matches_relay_scan_source": True,
            "output_bound_bytes": MAXIMUM_OUTPUT_BYTES,
            "timeout_seconds": MAXIMUM_RUNTIME_SECONDS,
            "stdout_bytes": stdout_bytes,
            "stderr_bytes": stderr_bytes,
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify the staged Label_Match public installer")
    parser.add_argument("--package-root", required=True)
    parser.add_argument("--report", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = verify_staged_installer(Path(args.package_root))
        report = Path(args.report)
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
            newline="\n",
        )
    except (OSError, subprocess.SubprocessError, StagedInstallerVerificationError) as exc:
        print(f"staged_installer=DENY reason={str(exc)[:2048]}")
        return 2
    print(
        json.dumps(
            {
                "schema_version": result["schema_version"],
                "status": result["status"],
                "report": str(Path(args.report).resolve()),
                "package_file_count": result["original_package_file_count"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

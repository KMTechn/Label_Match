#!/usr/bin/env python
"""Exercise the packaged direct-sync installer in an isolated dry-run copy."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from typing import Sequence


class StagedInstallerVerificationError(RuntimeError):
    """Raised when the staged package cannot prove its installer wiring."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _inventory(root: Path) -> list[dict[str, object]]:
    return [
        {
            "path": path.relative_to(root).as_posix(),
            "size": path.stat().st_size,
            "sha256": _sha256(path),
        }
        for path in sorted(
            (candidate for candidate in root.rglob("*") if candidate.is_file()),
            key=lambda candidate: candidate.relative_to(root).as_posix().casefold(),
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


def _same_path(left: Path | str, right: Path | str) -> bool:
    return os.path.normcase(os.path.abspath(os.fspath(left))) == os.path.normcase(
        os.path.abspath(os.fspath(right))
    )


def verify_staged_installer(package_root: Path) -> dict[str, object]:
    if os.name != "nt":
        raise StagedInstallerVerificationError("staged installer verification requires Windows")
    package_root = package_root.resolve()
    installer = package_root / "install_label_match_direct_sync.ps1"
    if not installer.is_file():
        raise StagedInstallerVerificationError(f"installer is missing: {installer}")
    powershell = shutil.which("pwsh") or shutil.which("powershell")
    if not powershell:
        raise StagedInstallerVerificationError("PowerShell is required")

    original_inventory = _inventory(package_root)
    with tempfile.TemporaryDirectory(prefix="label-match-staged-installer-") as temp_dir:
        root = Path(temp_dir)
        staged_copy = root / "Label_Match"
        shutil.copytree(package_root, staged_copy)
        program_data = root / "ProgramData"
        scan_source = root / "scan-source"
        copied_installer = staged_copy / installer.name
        env = dict(os.environ)
        # The bundled EXEs must make the official package independent of a system Python install.
        env["PATH"] = str(Path(powershell).resolve().parent)
        completed = subprocess.run(
            [
                powershell,
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(copied_installer),
                "-DryRun",
                "-ProgramDataRoot",
                str(program_data),
                "-ScanSourceDir",
                str(scan_source),
            ],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=env,
            timeout=120,
        )
        if completed.returncode != 0:
            detail = completed.stderr.strip() or completed.stdout.strip()
            raise StagedInstallerVerificationError(
                f"packaged installer dry-run failed with exit {completed.returncode}: {detail}"
            )
        if completed.stderr.strip():
            raise StagedInstallerVerificationError("packaged installer dry-run wrote stderr")

        install_report_path = program_data / "status" / "label_match_direct_sync_install.json"
        if not install_report_path.is_file():
            raise StagedInstallerVerificationError("packaged installer did not write its dry-run report")
        try:
            install_report = json.loads(install_report_path.read_text(encoding="utf-8-sig"))
        except ValueError as exc:
            raise StagedInstallerVerificationError("packaged installer report is invalid JSON") from exc
        if install_report.get("status") != "DRY_RUN":
            raise StagedInstallerVerificationError("packaged installer report status is not DRY_RUN")

        expected_settings = (
            staged_copy / "_internal/config/app_settings.json"
            if (staged_copy / "_internal").is_dir()
            else staged_copy / "config/app_settings.json"
        )
        if not _same_path(str(install_report.get("app_settings_path") or ""), expected_settings):
            raise StagedInstallerVerificationError("installer did not bind the packaged app settings path")
        try:
            settings = json.loads(expected_settings.read_text(encoding="utf-8-sig"))
        except (OSError, ValueError) as exc:
            raise StagedInstallerVerificationError("installer did not write valid packaged app settings") from exc
        if not _same_path(str(settings.get("custom_save_path") or ""), scan_source):
            raise StagedInstallerVerificationError("packaged app save path differs from relay scan source")

        runner = staged_copy / "tools" / "direct_sync_relay_runner.exe"
        registration = staged_copy / "tools" / "register_label_match_worker_pc.exe"
        install_helper = (
            staged_copy
            / "tools"
            / "direct_sync_relay_install_pack"
            / "direct_sync_relay_install_pack.exe"
        )
        for label, path in (
            ("runner", runner),
            ("registration", registration),
            ("install helper", install_helper),
        ):
            if not path.is_file():
                raise StagedInstallerVerificationError(f"bundled {label} executable is missing")

        runner_command = install_report.get("runner_command")
        if not isinstance(runner_command, list) or not runner_command:
            raise StagedInstallerVerificationError("installer report runner command is missing")
        if not _same_path(runner_command[0], runner):
            raise StagedInstallerVerificationError("installer did not select the bundled runner executable")
        if not _same_path(str(install_report.get("runner_exe") or ""), runner):
            raise StagedInstallerVerificationError("installer runner_exe evidence does not match the package")
        self_enrollment = install_report.get("self_enrollment")
        if not isinstance(self_enrollment, dict):
            raise StagedInstallerVerificationError("installer self-enrollment evidence is missing")
        if self_enrollment.get("registration_command_mode") != "bundled_executable":
            raise StagedInstallerVerificationError("installer did not select bundled registration")
        if not _same_path(str(self_enrollment.get("registration_executable") or ""), registration):
            raise StagedInstallerVerificationError("installer registration evidence does not match the package")
        if _inventory(package_root) != original_inventory:
            raise StagedInstallerVerificationError("verification mutated the original staged package")

        return {
            "schema_version": "label-match-staged-installer-verification-v1",
            "status": "PASS",
            "installer_status": "DRY_RUN",
            "system_python_required": False,
            "installer": {
                "path": "install_label_match_direct_sync.ps1",
                "sha256": _sha256(installer),
            },
            "install_helper": {
                "path": "tools/direct_sync_relay_install_pack/direct_sync_relay_install_pack.exe",
                "sha256": _sha256(install_helper),
            },
            "runner": {
                "path": "tools/direct_sync_relay_runner.exe",
                "sha256": _sha256(runner),
                "selected": True,
            },
            "registration": {
                "path": "tools/register_label_match_worker_pc.exe",
                "sha256": _sha256(registration),
                "selected": True,
            },
            "original_package_file_count": len(original_inventory),
            "original_package_inventory": original_inventory,
            "original_package_inventory_sha256": _inventory_digest(original_inventory),
            "original_package_unchanged": True,
            "app_settings_path": expected_settings.relative_to(staged_copy).as_posix(),
            "app_save_path_matches_relay_scan_source": True,
            "stdout_bytes": len(completed.stdout.encode("utf-8", errors="replace")),
            "stderr_bytes": len(completed.stderr.encode("utf-8", errors="replace")),
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify the staged Label_Match direct-sync installer")
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
        print(f"staged_installer=DENY reason={exc}")
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

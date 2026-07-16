#!/usr/bin/env python
"""Create and byte-for-byte verify the deterministic Label_Match release ZIP."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
from typing import Sequence
import zipfile


class ReleaseArchiveError(RuntimeError):
    """Raised when the final package or ZIP evidence is inconsistent."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _files(root: Path) -> list[Path]:
    return sorted(
        (path for path in root.rglob("*") if path.is_file()),
        key=lambda path: path.relative_to(root).as_posix().casefold(),
    )


def _inventory(root: Path) -> list[dict[str, object]]:
    return [
        {
            "path": path.relative_to(root).as_posix(),
            "size": path.stat().st_size,
            "sha256": _sha256(path),
        }
        for path in _files(root)
    ]


def _inventory_digest(inventory: list[dict[str, object]]) -> str:
    canonical = json.dumps(
        inventory,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _json(path: Path, label: str) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError) as exc:
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


def _read_authenticode_signature(executable: Path) -> dict[str, str]:
    if os.name != "nt":
        raise ReleaseArchiveError("final Authenticode verification requires Windows")
    powershell = shutil.which("pwsh") or shutil.which("powershell")
    if not powershell:
        raise ReleaseArchiveError("PowerShell is required for final Authenticode verification")
    env = dict(os.environ)
    env["LABEL_MATCH_SIGNATURE_TARGET"] = str(executable.resolve())
    script = """
$ErrorActionPreference = 'Stop'
$signature = Get-AuthenticodeSignature -LiteralPath $env:LABEL_MATCH_SIGNATURE_TARGET
[ordered]@{
  status = $signature.Status.ToString()
  signer_thumbprint = if ($signature.SignerCertificate) { $signature.SignerCertificate.Thumbprint } else { '' }
  timestamp_thumbprint = if ($signature.TimeStamperCertificate) { $signature.TimeStamperCertificate.Thumbprint } else { '' }
} | ConvertTo-Json -Compress
"""
    completed = subprocess.run(
        [powershell, "-NoProfile", "-NonInteractive", "-Command", script],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
        timeout=30,
    )
    if completed.returncode != 0 or completed.stderr.strip():
        detail = completed.stderr.strip() or completed.stdout.strip() or f"exit {completed.returncode}"
        raise ReleaseArchiveError(f"final Authenticode verification failed for {executable}: {detail}")
    try:
        payload = json.loads(completed.stdout)
    except ValueError as exc:
        raise ReleaseArchiveError(f"final Authenticode verification returned invalid JSON for {executable}") from exc
    return {
        "status": str(payload.get("status") or ""),
        "signer_thumbprint": str(payload.get("signer_thumbprint") or "").replace(" ", "").upper(),
        "timestamp_thumbprint": str(payload.get("timestamp_thumbprint") or "").replace(" ", "").upper(),
    }


def _validate_release_evidence(
    package_root: Path,
    *,
    expected_signer_thumbprint: str,
) -> dict[str, object]:
    tools_root = package_root / "tools"
    post_path = tools_root / "release_cli_tools_post_sign_manifest.json"
    post = _json(post_path, "post-sign helper manifest")
    if (
        post.get("schema_version") != "label-match-release-cli-tools-v1"
        or post.get("status") != "PASS"
        or post.get("artifact_phase") != "signed_post_sign"
    ):
        raise ReleaseArchiveError("post-sign helper manifest is not a signed PASS artifact")

    if post.get("pre_sign_manifest") != "release_cli_tools_manifest.json":
        raise ReleaseArchiveError("post-sign predecessor manifest name is invalid")
    if post.get("authenticode_manifest") != "authenticode-manifest.json":
        raise ReleaseArchiveError("post-sign Authenticode manifest name is invalid")
    predecessor_path = tools_root / "release_cli_tools_manifest.json"
    if not predecessor_path.is_file() or _sha256(predecessor_path) != post.get("pre_sign_manifest_sha256"):
        raise ReleaseArchiveError("post-sign manifest does not bind the packaged pre-sign manifest")
    authenticode_path = package_root / "authenticode-manifest.json"
    if not authenticode_path.is_file() or _sha256(authenticode_path) != post.get("authenticode_manifest_sha256"):
        raise ReleaseArchiveError("post-sign manifest does not bind the packaged Authenticode manifest")

    expected_signed_paths = {
        "Label_Match.exe",
        "tools/direct_sync_relay_runner.exe",
        "tools/direct_sync_relay_install_pack/direct_sync_relay_install_pack.exe",
        "tools/register_label_match_worker_pc.exe",
    }
    approved_signer = str(expected_signer_thumbprint or "").replace(" ", "").upper()
    if not re.fullmatch(r"[0-9A-F]{40}", approved_signer):
        raise ReleaseArchiveError("approved Authenticode signer thumbprint is invalid")
    signed_entries = post.get("authenticode_verification")
    if not isinstance(signed_entries, list):
        raise ReleaseArchiveError("post-sign Authenticode verification list is missing")
    signed_paths: set[str] = set()
    for entry in signed_entries:
        if not isinstance(entry, dict):
            raise ReleaseArchiveError("post-sign Authenticode entry is invalid")
        relative = str(entry.get("path") or "").replace("\\", "/")
        if not relative or relative in signed_paths:
            raise ReleaseArchiveError(f"duplicate or missing signed path: {relative!r}")
        signed_paths.add(relative)
        target = package_root / PurePosixPath(relative)
        signed_size = _required_int(entry, "size", label=f"signed executable {relative}", minimum=0)
        if (
            not target.is_file()
            or target.stat().st_size != signed_size
            or _sha256(target) != str(entry.get("sha256") or "").lower()
            or entry.get("status") != "Valid"
        ):
            raise ReleaseArchiveError(f"signed executable evidence mismatch: {relative}")
        entry_signer = str(entry.get("signer_thumbprint") or "").replace(" ", "").upper()
        entry_timestamp = str(entry.get("timestamp_thumbprint") or "").replace(" ", "").upper()
        if entry_signer != approved_signer or not re.fullmatch(r"[0-9A-F]{40}", entry_timestamp):
            raise ReleaseArchiveError(f"signed executable trust evidence mismatch: {relative}")
        live_signature = _read_authenticode_signature(target)
        if (
            live_signature.get("status") != "Valid"
            or live_signature.get("signer_thumbprint") != approved_signer
            or live_signature.get("timestamp_thumbprint") != entry_timestamp
        ):
            raise ReleaseArchiveError(f"final live Authenticode verification mismatch: {relative}")
    if signed_paths != expected_signed_paths:
        raise ReleaseArchiveError("post-sign manifest does not bind exactly four release executables")

    tools = post.get("tools")
    if not isinstance(tools, list) or len(tools) != 3:
        raise ReleaseArchiveError("post-sign helper inventory must contain exactly three tools")
    expected_modes = {
        "direct_sync_relay_runner": "onefile",
        "direct_sync_relay_install_pack": "onedir",
        "register_label_match_worker_pc": "onefile",
    }
    expected_probe_policy = {
        "probe_count": 3,
        "help_timeout_seconds": 15.0,
        "fresh_copy_per_probe": True,
        "isolated_environment_per_probe": True,
        "residual_process_policy": "fail_closed_new_exact_executable_path_with_baseline",
    }
    if post.get("probe_policy") != expected_probe_policy:
        raise ReleaseArchiveError("post-sign helper probe policy differs from the release contract")
    observed_tools: set[str] = set()
    install_runtime_count = 0
    for tool in tools:
        if not isinstance(tool, dict):
            raise ReleaseArchiveError("post-sign helper entry is invalid")
        name = str(tool.get("name") or "")
        mode = str(tool.get("mode") or "")
        if name in observed_tools:
            raise ReleaseArchiveError(f"duplicate post-sign helper: {name}")
        observed_tools.add(name)
        if expected_modes.get(name) != mode:
            raise ReleaseArchiveError(f"post-sign helper mode mismatch for {name}")
        archive_verification = tool.get("archive_verification")
        if (
            not isinstance(archive_verification, dict)
            or archive_verification.get("status") != "PASS"
            or not re.fullmatch(
                r"[0-9a-f]{64}",
                str(archive_verification.get("viewer_stdout_sha256") or "").lower(),
            )
        ):
            raise ReleaseArchiveError(f"post-sign CArchive evidence is incomplete for {name}")
        _required_int(
            archive_verification,
            "viewer_stderr_bytes",
            label=f"post-sign CArchive {name}",
            exact=0,
        )
        help_runs = tool.get("help_runs")
        if not isinstance(help_runs, list) or len(help_runs) != 3:
            raise ReleaseArchiveError(f"post-sign help probe count is incomplete for {name}")
        expected_executable_sha256 = str(tool.get("executable_sha256") or "").lower()
        if not re.fullmatch(r"[0-9a-f]{64}", expected_executable_sha256):
            raise ReleaseArchiveError(f"post-sign executable hash is invalid for {name}")
        for expected_run_no, run in enumerate(help_runs, start=1):
            if (
                not isinstance(run, dict)
                or run.get("run") != expected_run_no
                or run.get("status") != "PASS"
                or str(run.get("probe_executable_sha256") or "").lower()
                != expected_executable_sha256
            ):
                raise ReleaseArchiveError(f"post-sign help probe evidence is invalid for {name}")
            _required_int(run, "returncode", label=f"post-sign help probe {name}", exact=0)
            _required_int(run, "stderr_bytes", label=f"post-sign help probe {name}", exact=0)
            _required_int(
                run,
                "residual_process_count",
                label=f"post-sign help probe {name}",
                exact=0,
            )
            _required_int(run, "stdout_bytes", label=f"post-sign help probe {name}", minimum=1)
            _required_int(run, "elapsed_ms", label=f"post-sign help probe {name}", minimum=0)
        inventory = tool.get("payload_inventory")
        if not isinstance(inventory, list) or not inventory:
            raise ReleaseArchiveError(f"post-sign payload inventory is missing for {name}")
        prefix = f"tools/{name}/" if mode == "onedir" else "tools/"
        expected_payload_paths: set[str] = set()
        for entry in inventory:
            if not isinstance(entry, dict):
                raise ReleaseArchiveError(f"invalid payload inventory entry for {name}")
            child = str(entry.get("path") or "").replace("\\", "/")
            relative = f"{prefix}{child}"
            if relative in expected_payload_paths:
                raise ReleaseArchiveError(f"duplicate payload inventory path for {name}: {relative}")
            expected_payload_paths.add(relative)
            target = package_root / PurePosixPath(relative)
            payload_size = _required_int(
                entry,
                "size",
                label=f"payload inventory {relative}",
                minimum=0,
            )
            if (
                not target.is_file()
                or target.stat().st_size != payload_size
                or _sha256(target) != str(entry.get("sha256") or "").lower()
            ):
                raise ReleaseArchiveError(f"packaged helper payload mismatch: {relative}")
            if name == "direct_sync_relay_install_pack" and child.startswith("_internal/"):
                install_runtime_count += 1
        executable_relative = (
            f"tools/{name}/{name}.exe" if mode == "onedir" else f"tools/{name}.exe"
        )
        executable_target = package_root / PurePosixPath(executable_relative)
        executable_size = _required_int(
            tool,
            "executable_size",
            label=f"post-sign executable {name}",
            minimum=1,
        )
        if (
            not executable_target.is_file()
            or executable_target.stat().st_size != executable_size
            or _sha256(executable_target) != expected_executable_sha256
        ):
            raise ReleaseArchiveError(f"post-sign executable evidence mismatch for {name}")
        if mode == "onedir":
            payload_root = package_root / "tools" / name
            actual_payload_paths = {
                path.relative_to(package_root).as_posix()
                for path in _files(payload_root)
            }
            if actual_payload_paths != expected_payload_paths:
                raise ReleaseArchiveError(f"packaged onedir membership mismatch for {name}")
    if observed_tools != set(expected_modes):
        raise ReleaseArchiveError("post-sign helper names differ from the release contract")
    if install_runtime_count < 1:
        raise ReleaseArchiveError("install helper onedir runtime inventory is missing")

    staged_installer = _json(
        package_root / "staged-installer-verification.json",
        "staged installer verification",
    )
    if (
        staged_installer.get("schema_version") != "label-match-staged-installer-verification-v1"
        or staged_installer.get("status") != "PASS"
        or staged_installer.get("installer_status") != "DRY_RUN"
        or staged_installer.get("system_python_required") is not False
        or staged_installer.get("app_settings_path") != "_internal/config/app_settings.json"
        or staged_installer.get("app_save_path_matches_relay_scan_source") is not True
        or staged_installer.get("original_package_unchanged") is not True
    ):
        raise ReleaseArchiveError("staged installer verification did not prove bundled execution")
    _required_int(staged_installer, "stderr_bytes", label="staged installer", exact=0)
    _required_int(staged_installer, "stdout_bytes", label="staged installer", minimum=1)
    staged_inventory = staged_installer.get("original_package_inventory")
    if not isinstance(staged_inventory, list) or not staged_inventory:
        raise ReleaseArchiveError("staged installer package inventory is missing")
    normalized_staged_inventory: list[dict[str, object]] = []
    seen_inventory_paths: set[str] = set()
    for entry in staged_inventory:
        if not isinstance(entry, dict):
            raise ReleaseArchiveError("staged installer package inventory entry is invalid")
        relative = str(entry.get("path") or "").replace("\\", "/")
        pure = PurePosixPath(relative)
        if (
            not relative
            or pure.is_absolute()
            or ".." in pure.parts
            or relative in seen_inventory_paths
            or relative == "staged-installer-verification.json"
        ):
            raise ReleaseArchiveError(f"staged installer package inventory path is invalid: {relative!r}")
        seen_inventory_paths.add(relative)
        inventory_size = _required_int(
            entry,
            "size",
            label=f"staged package inventory {relative}",
            minimum=0,
        )
        normalized_staged_inventory.append(
            {
                "path": relative,
                "size": inventory_size,
                "sha256": str(entry.get("sha256") or "").lower(),
            }
        )
    if normalized_staged_inventory != staged_inventory:
        raise ReleaseArchiveError("staged installer package inventory is not canonical")
    if _inventory_digest(normalized_staged_inventory) != staged_installer.get(
        "original_package_inventory_sha256"
    ):
        raise ReleaseArchiveError("staged installer package inventory digest mismatch")
    if _required_int(
        staged_installer,
        "original_package_file_count",
        label="staged installer",
        minimum=1,
    ) != len(normalized_staged_inventory):
        raise ReleaseArchiveError("staged installer package file count mismatch")
    current_without_staged_report = [
        entry
        for entry in _inventory(package_root)
        if entry["path"] != "staged-installer-verification.json"
    ]
    if current_without_staged_report != normalized_staged_inventory:
        raise ReleaseArchiveError("package membership changed after staged installer verification")

    allowed_tool_entries = {
        "direct_sync_relay_runner.py",
        "direct_sync_relay_operator.py",
        "direct_sync_relay_install_pack.py",
        "direct_sync_phase_g_label_match_runtime_report.py",
        "register_label_match_worker_pc.py",
        "direct_sync_relay_runner.exe",
        "register_label_match_worker_pc.exe",
        "direct_sync_relay_install_pack",
        "release_cli_tools_manifest.json",
        "release_cli_tools_post_sign_manifest.json",
        "enrollment_token.txt.template",
    }
    actual_tool_entries = {path.name for path in tools_root.iterdir()}
    if actual_tool_entries != allowed_tool_entries:
        raise ReleaseArchiveError("top-level tools membership differs from the release contract")
    staged_bindings = {
        "installer": "install_label_match_direct_sync.ps1",
        "install_helper": "tools/direct_sync_relay_install_pack/direct_sync_relay_install_pack.exe",
        "runner": "tools/direct_sync_relay_runner.exe",
        "registration": "tools/register_label_match_worker_pc.exe",
    }
    for name, expected_path in staged_bindings.items():
        entry = staged_installer.get(name)
        if not isinstance(entry, dict) or entry.get("path") != expected_path:
            raise ReleaseArchiveError(f"staged installer {name} binding is missing")
        target = package_root / PurePosixPath(expected_path)
        if not target.is_file() or _sha256(target) != str(entry.get("sha256") or "").lower():
            raise ReleaseArchiveError(f"staged installer {name} hash mismatch")
        if name in {"runner", "registration"} and entry.get("selected") is not True:
            raise ReleaseArchiveError(f"staged installer {name} was not selected")
    identity = _json(package_root / "release-identity.json", "release identity")
    if (
        identity.get("schema_version") != "label-match-release-identity-v2"
        or identity.get("status") != "PASS"
        or identity.get("annotated_tag") is not True
        or identity.get("tag_signature_verified") is not True
        or identity.get("reviewed_main_ancestor") is not True
        or identity.get("reviewed_ref_exact") is not True
        or identity.get("clean_checkout") is not True
    ):
        raise ReleaseArchiveError("release identity is not PASS")
    tag = str(identity.get("tag") or "")
    app_version = str(identity.get("app_version") or "")
    commit = str(identity.get("commit") or "").lower()
    tree = str(identity.get("tree") or "").lower()
    if not re.fullmatch(r"v(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)", tag):
        raise ReleaseArchiveError("release identity tag is invalid")
    if tag != app_version or post.get("app_version") != app_version:
        raise ReleaseArchiveError("release identity and post-sign app version differ")
    if not re.fullmatch(r"[0-9a-f]{40}", commit) or post.get("commit") != commit:
        raise ReleaseArchiveError("release identity and post-sign commit differ")
    if not re.fullmatch(r"[0-9a-f]{40}", tree) or post.get("tree") != tree:
        raise ReleaseArchiveError("release identity and post-sign tree differ")
    return {
        "commit": commit,
        "tree": tree,
        "tag": tag,
        "install_onedir_runtime_file_count": install_runtime_count,
        "signed_executable_count": len(signed_paths),
        "approved_signer_thumbprint": approved_signer,
    }


def _zip_datetime(source_epoch: int) -> tuple[int, int, int, int, int, int]:
    instant = datetime.fromtimestamp(max(int(source_epoch), 315532800), tz=timezone.utc)
    year = min(2107, max(1980, instant.year))
    return (year, instant.month, instant.day, instant.hour, instant.minute, instant.second // 2 * 2)


def build_release_archive(
    package_root: Path,
    archive_path: Path,
    *,
    source_epoch: int,
    top_level: str = "Label_Match",
    expected_signer_thumbprint: str,
) -> dict[str, object]:
    package_root = package_root.resolve()
    archive_path = archive_path.resolve()
    if not package_root.is_dir():
        raise ReleaseArchiveError(f"package root is missing: {package_root}")
    if archive_path.exists():
        raise ReleaseArchiveError(f"archive already exists: {archive_path}")
    if not top_level or "/" in top_level or "\\" in top_level or top_level in {".", ".."}:
        raise ReleaseArchiveError("top-level archive directory is invalid")

    evidence = _validate_release_evidence(
        package_root,
        expected_signer_thumbprint=expected_signer_thumbprint,
    )
    package_inventory = _inventory(package_root)
    folded: set[str] = set()
    for item in package_inventory:
        folded_path = str(item["path"]).casefold()
        if folded_path in folded:
            raise ReleaseArchiveError(f"case-insensitive package path collision: {item['path']}")
        folded.add(folded_path)

    archive_path.parent.mkdir(parents=True, exist_ok=True)
    timestamp = _zip_datetime(source_epoch)
    with zipfile.ZipFile(
        archive_path,
        "x",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=9,
        allowZip64=True,
    ) as archive:
        for source in _files(package_root):
            relative = source.relative_to(package_root).as_posix()
            info = zipfile.ZipInfo(f"{top_level}/{relative}", date_time=timestamp)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.create_system = 3
            info.external_attr = (stat.S_IFREG | 0o644) << 16
            with source.open("rb") as handle:
                archive.writestr(info, handle.read(), compress_type=zipfile.ZIP_DEFLATED, compresslevel=9)

    with zipfile.ZipFile(archive_path, "r") as archive:
        infos = [info for info in archive.infolist() if not info.is_dir()]
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

    return {
        "schema_version": "label-match-release-archive-verification-v1",
        "status": "PASS",
        "archive": archive_path.name,
        "archive_sha256": _sha256(archive_path),
        "archive_size": archive_path.stat().st_size,
        "source_epoch": int(source_epoch),
        "normalized_zip_timestamp_utc": "%04d-%02d-%02dT%02d:%02d:%02dZ" % timestamp,
        "top_level": top_level,
        "package_file_count": len(package_inventory),
        "package_total_bytes": sum(int(item["size"]) for item in package_inventory),
        "exact_membership": True,
        "byte_parity": True,
        **evidence,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build and verify the Label_Match release ZIP")
    parser.add_argument("--package-root", required=True)
    parser.add_argument("--archive", required=True)
    parser.add_argument("--source-epoch", required=True, type=int)
    parser.add_argument("--top-level", default="Label_Match")
    parser.add_argument(
        "--expected-signer-thumbprint",
        default=os.getenv("WINDOWS_CODE_SIGNING_CERT_THUMBPRINT", ""),
    )
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
            expected_signer_thumbprint=args.expected_signer_thumbprint,
        )
        report = Path(args.report)
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
            newline="\n",
        )
    except (OSError, ValueError, zipfile.BadZipFile, ReleaseArchiveError) as exc:
        print(f"release_archive=DENY reason={exc}")
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python
"""Verify externally published, already-qualified Label_Match release assets."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path, PurePosixPath
import re
import stat
import tempfile
from typing import Sequence
import unicodedata
import zipfile


SEMVER_TAG_RE = re.compile(r"^v(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
GIT_OID_RE = re.compile(r"^[0-9a-f]{40}$")
CHECKSUM_RE = re.compile(r"^(?P<sha>[0-9a-f]{64})  (?P<name>[^\r\n]+)\n$")
TOP_LEVEL = "Label_Match"
CONTRACT_BUNDLE_SHA256 = "adaa08684ebb291837327f63f967a4f22650dff72c4c1dc56ce1a9bee6b5404a"
PROBE_SCHEMA = "kmtech-active-work-probe-build-v1.0.3.4"
PROBE_NAME = "KMTechActiveWorkProbe"
PROBE_VERSION = "v1.0.3.4"
MAX_ARCHIVE_BYTES = 4 * 1024 * 1024 * 1024
MAX_ARCHIVE_ENTRIES = 10_000
MAX_MEMBER_BYTES = 512 * 1024 * 1024
MAX_TOTAL_UNCOMPRESSED_BYTES = 2 * 1024 * 1024 * 1024
MAX_JSON_MEMBER_BYTES = 8 * 1024 * 1024
WINDOWS_RESERVED_NAMES = {
    "aux",
    "con",
    "nul",
    "prn",
    *(f"com{index}" for index in range(1, 10)),
    *(f"lpt{index}" for index in range(1, 10)),
}
WINDOWS_FORBIDDEN_PATH_CHARACTERS = set('<>"|?*')
MAX_WINDOWS_COMPONENT_UTF16_UNITS = 255
MAX_WINDOWS_ARCHIVE_PATH_UTF16_UNITS = 240
ALL_PROBE_APPS = [
    "Inspection_worker",
    "Rework_worker",
    "Defect_Inspection",
    "Container_Audit",
    "Label_Match",
]
REQUIRED_MEMBERS = {
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
    "install_label_match_direct_sync.ps1",
    "logistics_runtime_profile.py",
    "producer_runtime_client.py",
    "tools/check_logistics_runtime_profile.py",
    "tools/direct_sync_relay_install_pack.py",
    "tools/direct_sync_relay_runner.exe",
    "tools/direct_sync_relay_runner.py",
    "tools/invoke_embedded_python.ps1",
    "tools/install_logistics_runtime_profile.py",
    "tools/register_label_match_worker_pc.py",
}
RETIRED_HELPER_EXECUTABLES = {
    "tools/direct_sync_relay_install_pack/direct_sync_relay_install_pack.exe",
    "tools/direct_sync_relay_install_pack.exe",
    "tools/register_label_match_worker_pc.exe",
}


class FrozenReleaseError(RuntimeError):
    """Raised when published frozen bytes do not match the release contract."""


def _load_release_archive_validator():
    """Load the build-time validator only from this script's resolved directory."""
    verifier_path = Path(__file__).resolve()
    validator_path = verifier_path.with_name("build_release_archive.py")
    if (
        not validator_path.is_file()
        or validator_path.parent != verifier_path.parent
        or validator_path.name != "build_release_archive.py"
    ):
        raise FrozenReleaseError("exact sibling build-time validator is missing")
    spec = importlib.util.spec_from_file_location(
        "_label_match_exact_release_archive_validator", validator_path
    )
    if spec is None or spec.loader is None or spec.origin is None:
        raise FrozenReleaseError("exact sibling build-time validator cannot be loaded")
    if Path(spec.origin).resolve() != validator_path:
        raise FrozenReleaseError("build-time validator resolved outside the trusted sibling path")
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        raise FrozenReleaseError(f"build-time validator load failed: {exc}") from exc
    if not callable(getattr(module, "validate_release_evidence", None)):
        raise FrozenReleaseError("build-time release evidence validator entry point is missing")
    return module


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number: {value}")


def _json_bytes(payload: bytes, label: str) -> dict[str, object]:
    try:
        value = json.loads(
            payload.decode("utf-8-sig"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, ValueError) as exc:
        raise FrozenReleaseError(f"{label} is invalid JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise FrozenReleaseError(f"{label} must be a JSON object")
    return value


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise FrozenReleaseError(message)


def _validate_zip_member(info: zipfile.ZipInfo) -> tuple[str, bool]:
    name = info.filename
    _require(bool(name) and len(name) <= 4096, "archive member name is empty or too long")
    _require(not (info.flag_bits & 0x1), f"encrypted archive member is forbidden: {name}")
    _require("\\" not in name and "\x00" not in name, f"unsafe archive path: {name}")
    _require(
        all(ord(character) >= 32 and ord(character) != 127 for character in name),
        f"archive path contains control characters: {name!r}",
    )
    is_directory = info.is_dir()
    canonical = name[:-1] if is_directory else name
    _require(bool(canonical) and not canonical.startswith("/"), f"unsafe archive path: {name}")
    _require(unicodedata.normalize("NFC", canonical) == canonical, f"archive path is not NFC: {name}")
    parts = canonical.split("/")
    _require(
        all(part not in {"", ".", ".."} for part in parts)
        and len(parts) >= 1
        and parts[0] == TOP_LEVEL,
        f"unsafe archive path: {name}",
    )
    for part in parts:
        _require(part == part.strip() and not part.endswith((".", " ")), f"Windows-unsafe archive path: {name}")
        _require(":" not in part, f"Windows alternate stream path is forbidden: {name}")
        _require(
            not (set(part) & WINDOWS_FORBIDDEN_PATH_CHARACTERS),
            f"Windows-forbidden archive path: {name}",
        )
        _require(
            len(part.encode("utf-16-le")) // 2 <= MAX_WINDOWS_COMPONENT_UTF16_UNITS,
            f"Windows archive path component is too long: {name}",
        )
        stem = part.split(".", 1)[0].casefold()
        _require(stem not in WINDOWS_RESERVED_NAMES, f"Windows reserved archive path: {name}")
    _require(
        len(canonical.encode("utf-16-le")) // 2 <= MAX_WINDOWS_ARCHIVE_PATH_UTF16_UNITS,
        f"Windows archive path is too long: {name}",
    )
    pure = PurePosixPath(canonical)
    _require(not pure.is_absolute() and pure.as_posix() == canonical, f"unsafe archive path: {name}")

    unix_mode = (info.external_attr >> 16) & 0xFFFF
    file_type = stat.S_IFMT(unix_mode)
    _require(file_type != stat.S_IFLNK, f"symbolic-link archive member is forbidden: {name}")
    if is_directory:
        _require(file_type in {0, stat.S_IFDIR}, f"unsupported directory archive member type: {name}")
    else:
        _require(file_type in {0, stat.S_IFREG}, f"unsupported archive member type: {name}")
        _require(info.file_size <= MAX_MEMBER_BYTES, f"archive member is too large: {name}")
    _require(info.file_size >= 0 and info.compress_size >= 0, f"invalid archive member size: {name}")
    return canonical, is_directory


def _sha256_zip_member(archive: zipfile.ZipFile, info: zipfile.ZipInfo) -> str:
    digest = hashlib.sha256()
    observed = 0
    try:
        with archive.open(info, "r") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                observed += len(chunk)
                _require(observed <= info.file_size, f"archive member expanded beyond declared size: {info.filename}")
                digest.update(chunk)
    except (NotImplementedError, RuntimeError, zipfile.BadZipFile) as exc:
        raise FrozenReleaseError(f"archive member failed integrity verification: {info.filename}: {exc}") from exc
    _require(observed == info.file_size, f"archive member size changed while reading: {info.filename}")
    return digest.hexdigest()


def _manifest_inventory(payload: dict[str, object]) -> list[dict[str, object]]:
    inventory = payload.get("payload_inventory")
    _require(isinstance(inventory, list) and bool(inventory), "build manifest payload inventory is empty")
    normalized: list[dict[str, object]] = []
    seen: set[str] = set()
    for item in inventory:
        _require(isinstance(item, dict), "build manifest inventory entry must be an object")
        path = str(item.get("path") or "")
        size = item.get("size")
        sha256 = str(item.get("sha256") or "").lower()
        pure = PurePosixPath(path)
        _require(
            bool(path)
            and not pure.is_absolute()
            and ".." not in pure.parts
            and "\\" not in path
            and path == pure.as_posix(),
            f"unsafe build manifest path: {path!r}",
        )
        folded = path.casefold()
        _require(folded not in seen, f"duplicate build manifest path: {path}")
        seen.add(folded)
        _require(isinstance(size, int) and not isinstance(size, bool) and size >= 0, f"invalid size for {path}")
        _require(bool(SHA256_RE.fullmatch(sha256)), f"invalid SHA-256 for {path}")
        normalized.append({"path": path, "size": size, "sha256": sha256})
    canonical = json.dumps(
        normalized,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    digest = _sha256_bytes(canonical)
    _require(
        digest == str(payload.get("payload_inventory_sha256") or "").lower(),
        "build manifest payload inventory digest mismatch",
    )
    return normalized


def _validate_cli_tools_manifest(
    payload: dict[str, object],
    *,
    expected_commit: str,
    expected_tree: str,
    expected_version: str,
    archive: zipfile.ZipFile,
    by_name: dict[str, zipfile.ZipInfo],
) -> None:
    _require(
        payload.get("schema_version") == "label-match-release-cli-tools-v1"
        and payload.get("status") == "PASS"
        and payload.get("artifact_phase") == "unsigned_pre_sign",
        "release CLI tools manifest is not an unsigned PASS artifact",
    )
    _require(
        payload.get("commit") == expected_commit
        and payload.get("tree") == expected_tree
        and payload.get("app_version") == expected_version,
        "release CLI tools source identity mismatch",
    )
    _require(payload.get("python_version") == "3.12.10", "release CLI tools Python version mismatch")
    _require(
        payload.get("pyinstaller_version") == "6.20.0",
        "release CLI tools PyInstaller version mismatch",
    )
    expected_policy = {
        "probe_count": 3,
        "help_timeout_seconds": 15.0,
        "fresh_copy_per_probe": True,
        "isolated_environment_per_probe": True,
        "residual_process_policy": "fail_closed_new_exact_executable_path_with_baseline",
    }
    _require(payload.get("probe_policy") == expected_policy, "release CLI probe policy mismatch")
    expected_specs = [
        ("direct_sync_relay_runner", "tools/direct_sync_relay_runner.py", "onefile"),
        ("direct_sync_relay_install_pack", "tools/direct_sync_relay_install_pack.py", "onedir"),
        ("register_label_match_worker_pc", "tools/register_label_match_worker_pc.py", "onefile"),
    ]
    tools = payload.get("tools")
    _require(isinstance(tools, list) and len(tools) == len(expected_specs), "release CLI tool inventory is invalid")
    for tool, (expected_name, expected_source, expected_mode) in zip(tools, expected_specs, strict=True):
        _require(isinstance(tool, dict), "release CLI tool entry is invalid")
        _require(
            tool.get("name") == expected_name
            and tool.get("source") == expected_source
            and tool.get("mode") == expected_mode,
            "release CLI tool order, source, or mode mismatch",
        )
        executable_sha = str(tool.get("executable_sha256") or "")
        executable_size = tool.get("executable_size")
        _require(SHA256_RE.fullmatch(executable_sha) is not None, f"release CLI executable hash is invalid: {expected_name}")
        _require(isinstance(executable_size, int) and not isinstance(executable_size, bool) and executable_size > 0, f"release CLI executable size is invalid: {expected_name}")
        payload_inventory = tool.get("payload_inventory")
        _require(isinstance(payload_inventory, list) and bool(payload_inventory), f"release CLI payload inventory is missing: {expected_name}")
        expected_payload_names: set[str] = set()
        for entry in payload_inventory:
            _require(isinstance(entry, dict), f"release CLI payload entry is invalid: {expected_name}")
            relative = str(entry.get("path") or "")
            pure = PurePosixPath(relative)
            _require(
                bool(relative)
                and not pure.is_absolute()
                and ".." not in pure.parts
                and "\\" not in relative
                and pure.as_posix() == relative,
                f"release CLI payload path is invalid: {expected_name}",
            )
            member = (
                f"{TOP_LEVEL}/tools/{relative}"
                if expected_mode == "onefile"
                else f"{TOP_LEVEL}/tools/{expected_name}/{relative}"
            )
            _require(member not in expected_payload_names, f"release CLI payload path is duplicated: {expected_name}")
            expected_payload_names.add(member)
            _require(member in by_name, f"release CLI payload is missing: {member}")
            entry_size = entry.get("size")
            entry_sha = str(entry.get("sha256") or "")
            _require(
                isinstance(entry_size, int)
                and not isinstance(entry_size, bool)
                and entry_size >= 0
                and SHA256_RE.fullmatch(entry_sha) is not None,
                f"release CLI payload identity is invalid: {member}",
            )
            _require(
                by_name[member].file_size == entry_size
                and _sha256_zip_member(archive, by_name[member]) == entry_sha,
                f"release CLI payload differs from its manifest: {member}",
            )
        executable_member = (
            f"{TOP_LEVEL}/tools/{expected_name}.exe"
            if expected_mode == "onefile"
            else f"{TOP_LEVEL}/tools/{expected_name}/{expected_name}.exe"
        )
        _require(executable_member in by_name, f"release CLI executable is missing: {expected_name}")
        _require(
            by_name[executable_member].file_size == executable_size
            and _sha256_zip_member(archive, by_name[executable_member]) == executable_sha,
            f"release CLI executable differs from its manifest: {expected_name}",
        )
        if expected_mode == "onedir":
            prefix = f"{TOP_LEVEL}/tools/{expected_name}/"
            actual_payload_names = {name for name in by_name if name.startswith(prefix)}
            _require(
                actual_payload_names == expected_payload_names,
                f"release CLI onedir membership differs from its manifest: {expected_name}",
            )
        help_runs = tool.get("help_runs")
        _require(isinstance(help_runs, list) and len(help_runs) == 3, f"release CLI help evidence is incomplete: {expected_name}")
        for index, run in enumerate(help_runs, start=1):
            _require(
                isinstance(run, dict)
                and run.get("run") == index
                and run.get("status") == "PASS"
                and run.get("returncode") == 0
                and run.get("stderr_bytes") == 0
                and run.get("residual_process_count") == 0
                and run.get("probe_executable_sha256") == executable_sha,
                f"release CLI help evidence is invalid: {expected_name}",
            )
        archive_evidence = tool.get("archive_verification")
        _require(
            isinstance(archive_evidence, dict)
            and archive_evidence.get("status") == "PASS"
            and archive_evidence.get("viewer_stderr_bytes") == 0
            and SHA256_RE.fullmatch(str(archive_evidence.get("viewer_stdout_sha256") or "")) is not None,
            f"release CLI archive evidence is invalid: {expected_name}",
        )


QUALIFICATION_RECEIPT_KEYS = {
    "schema_version",
    "status",
    "phase",
    "release_title",
    "release_trust",
    "tag",
    "tag_object",
    "tag_object_type",
    "tag_peeled_commit",
    "canonical_tag_message",
    "tag_identity_report",
    "tag_identity_report_sha256",
    "tag_recorded_before_release_identity_and_build",
    "commit",
    "tree",
    "tag_signature_verified",
    "python_version",
    "pyinstaller_version",
    "source_epoch",
    "path_identity",
    "archive",
    "archive_sha256",
    "archive_size",
    "checksum",
    "checksum_sha256",
    "main_exe_sha256",
    "factory_contract_sha256",
    "update_provider",
    "update_channel",
    "frozen_bytes",
    "network_used",
    "publication_mutated",
    "tag_mutated",
    "external_post_download_parity_required",
}


def _positive_int(value: object, label: str) -> int:
    _require(
        isinstance(value, int) and not isinstance(value, bool) and value > 0,
        f"{label} must be a positive integer",
    )
    return int(value)


def _validate_qualification_receipt(
    path: Path,
    *,
    expected_tag: str,
    expected_commit: str,
    expected_tree: str,
    expected_tag_object: str,
    expected_source_epoch: int,
) -> dict[str, object]:
    _require(path.is_file(), f"preserved qualification receipt is missing: {path}")
    _require(
        path.stat().st_size <= 1024 * 1024,
        "preserved qualification receipt is unexpectedly large",
    )
    receipt = _json_bytes(path.read_bytes(), "preserved qualification receipt")
    _require(
        set(receipt) == QUALIFICATION_RECEIPT_KEYS,
        "preserved qualification receipt fields differ from the exact contract",
    )
    archive_name = f"Label_Match-{expected_tag}.zip"
    _require(
        receipt.get("schema_version") == "label-match-pre-push-qualification-v2"
        and receipt.get("status") == "PASS"
        and receipt.get("phase") == "phase_b_pre_push_frozen_candidate"
        and receipt.get("release_title") == f"Release {expected_tag}"
        and receipt.get("release_trust") == "internal_unsigned"
        and receipt.get("tag") == expected_tag
        and receipt.get("tag_object") == expected_tag_object
        and receipt.get("tag_object_type") == "tag"
        and receipt.get("tag_peeled_commit") == expected_commit
        and receipt.get("canonical_tag_message") == f"Release {expected_tag}"
        and receipt.get("tag_identity_report")
        == f"Label_Match-{expected_tag}.final-tag-identity.json"
        and receipt.get("tag_recorded_before_release_identity_and_build") is True
        and receipt.get("commit") == expected_commit
        and receipt.get("tree") == expected_tree
        and receipt.get("tag_signature_verified") is False
        and receipt.get("python_version") == "3.12.10"
        and receipt.get("pyinstaller_version") == "6.20.0"
        and receipt.get("source_epoch") == expected_source_epoch
        and receipt.get("archive") == archive_name
        and receipt.get("checksum") == f"{archive_name}.sha256"
        and receipt.get("factory_contract_sha256") == CONTRACT_BUNDLE_SHA256
        and receipt.get("update_provider") == "github"
        and receipt.get("update_channel") == "stable"
        and receipt.get("frozen_bytes") is True
        and receipt.get("network_used") is False
        and receipt.get("publication_mutated") is False
        and receipt.get("tag_mutated") is False
        and receipt.get("external_post_download_parity_required") is True,
        "preserved qualification receipt identity or gate claims differ",
    )
    _require(
        SHA256_RE.fullmatch(str(receipt.get("tag_identity_report_sha256") or ""))
        is not None,
        "preserved tag identity report hash is invalid",
    )
    for field in (
        "archive_sha256",
        "checksum_sha256",
        "main_exe_sha256",
    ):
        _require(
            SHA256_RE.fullmatch(str(receipt.get(field) or "")) is not None,
            f"preserved qualification receipt {field} is invalid",
        )
    _positive_int(receipt.get("archive_size"), "preserved archive size")
    return receipt


def _extract_validated_archive(
    archive: zipfile.ZipFile,
    infos: list[zipfile.ZipInfo],
    package_root: Path,
) -> None:
    package_root.mkdir(parents=True, exist_ok=False)
    resolved_root = package_root.resolve()
    for info in infos:
        relative_parts = PurePosixPath(info.filename).parts[1:]
        destination = package_root.joinpath(*relative_parts)
        resolved_destination = destination.resolve()
        _require(
            resolved_destination.is_relative_to(resolved_root),
            f"archive extraction escaped the isolated verifier root: {info.filename}",
        )
        destination.parent.mkdir(parents=True, exist_ok=True)
        observed = 0
        try:
            with archive.open(info, "r") as source, destination.open("xb") as output:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    observed += len(chunk)
                    _require(
                        observed <= info.file_size,
                        f"archive member expanded beyond declared size: {info.filename}",
                    )
                    output.write(chunk)
        except (OSError, RuntimeError, zipfile.BadZipFile) as exc:
            raise FrozenReleaseError(
                f"archive extraction failed for {info.filename}: {exc}"
            ) from exc
        _require(
            observed == info.file_size,
            f"archive member size changed during extraction: {info.filename}",
        )


def verify_frozen_release_assets(
    archive_path: Path,
    checksum_path: Path,
    *,
    expected_tag: str,
    expected_commit: str,
    expected_tree: str,
    expected_tag_object: str,
    expected_source_epoch: int,
    expected_archive_sha256: str = "",
    expected_archive_size: int = 0,
    expected_main_exe_sha256: str = "",
    qualification_receipt_path: Path | None = None,
) -> dict[str, object]:
    archive_path = archive_path.resolve()
    checksum_path = checksum_path.resolve()
    expected_tag = str(expected_tag or "").strip()
    expected_commit = str(expected_commit or "").strip().lower()
    expected_tree = str(expected_tree or "").strip().lower()
    expected_tag_object = str(expected_tag_object or "").strip().lower()
    expected_archive_sha256 = str(expected_archive_sha256 or "").strip().lower()
    expected_main_exe_sha256 = str(expected_main_exe_sha256 or "").strip().lower()
    _require(SEMVER_TAG_RE.fullmatch(expected_tag) is not None, "expected tag is not strict semver")
    _require(GIT_OID_RE.fullmatch(expected_commit) is not None, "expected commit must be a full lowercase Git OID")
    _require(GIT_OID_RE.fullmatch(expected_tree) is not None, "expected tree must be a full lowercase Git OID")
    _require(GIT_OID_RE.fullmatch(expected_tag_object) is not None, "expected tag object must be a full lowercase Git OID")
    _positive_int(expected_source_epoch, "expected source epoch")
    _require(archive_path.is_file(), f"release archive is missing: {archive_path}")
    _require(checksum_path.is_file(), f"release checksum is missing: {checksum_path}")
    _require(archive_path.stat().st_size <= MAX_ARCHIVE_BYTES, "release archive exceeds the size limit")
    _require(checksum_path.stat().st_size <= 1024, "release checksum is unexpectedly large")
    receipt: dict[str, object] | None = None
    if qualification_receipt_path is not None:
        receipt = _validate_qualification_receipt(
            qualification_receipt_path.resolve(),
            expected_tag=expected_tag,
            expected_commit=expected_commit,
            expected_tree=expected_tree,
            expected_tag_object=expected_tag_object,
            expected_source_epoch=expected_source_epoch,
        )
        receipt_archive_sha = str(receipt["archive_sha256"])
        receipt_archive_size = int(receipt["archive_size"])
        receipt_main_exe_sha = str(receipt["main_exe_sha256"])
        if expected_archive_sha256:
            _require(
                expected_archive_sha256 == receipt_archive_sha,
                "publication archive hash differs from preserved qualification receipt",
            )
        if expected_archive_size:
            _require(
                expected_archive_size == receipt_archive_size,
                "publication archive size differs from preserved qualification receipt",
            )
        if expected_main_exe_sha256:
            _require(
                expected_main_exe_sha256 == receipt_main_exe_sha,
                "publication main executable hash differs from preserved qualification receipt",
            )
        expected_archive_sha256 = receipt_archive_sha
        expected_archive_size = receipt_archive_size
        expected_main_exe_sha256 = receipt_main_exe_sha
    _require(
        SHA256_RE.fullmatch(expected_archive_sha256) is not None,
        "expected archive SHA-256 is invalid or missing",
    )
    _positive_int(expected_archive_size, "expected archive size")
    _require(
        SHA256_RE.fullmatch(expected_main_exe_sha256) is not None,
        "expected main executable SHA-256 is invalid or missing",
    )
    expected_name = f"Label_Match-{expected_tag}.zip"
    _require(archive_path.name == expected_name, f"archive name must be {expected_name}")
    _require(checksum_path.name == f"{expected_name}.sha256", "checksum filename does not match archive")
    try:
        checksum_text = checksum_path.read_bytes().decode("utf-8")
    except UnicodeDecodeError as exc:
        raise FrozenReleaseError("checksum is not exact UTF-8 text") from exc
    match = CHECKSUM_RE.fullmatch(checksum_text)
    _require(match is not None, "checksum must use exact '<sha256>  <archive>' format")
    checksum_sha = match.group("sha").lower()
    _require(match.group("name") == expected_name, "checksum references the wrong archive")
    archive_sha = _sha256_file(archive_path)
    _require(checksum_sha == archive_sha, "published checksum does not match archive bytes")
    _require(archive_sha == expected_archive_sha256, "archive differs from expected frozen ZIP SHA-256")
    _require(archive_path.stat().st_size == expected_archive_size, "archive differs from expected frozen ZIP size")
    if receipt is not None:
        _require(
            _sha256_file(checksum_path) == receipt.get("checksum_sha256"),
            "downloaded checksum differs from preserved qualification receipt",
        )

    try:
        archive = zipfile.ZipFile(archive_path, "r")
    except (OSError, zipfile.BadZipFile) as exc:
        raise FrozenReleaseError(f"release archive is invalid: {exc}") from exc
    with archive:
        all_infos = archive.infolist()
        _require(bool(all_infos), "release archive is empty")
        _require(len(all_infos) <= MAX_ARCHIVE_ENTRIES, "release archive contains too many entries")
        entry_kinds: dict[str, bool] = {}
        total_uncompressed = 0
        for info in all_infos:
            canonical, is_directory = _validate_zip_member(info)
            folded = canonical.casefold()
            _require(folded not in entry_kinds, f"archive contains a file/directory or case collision: {info.filename}")
            entry_kinds[folded] = is_directory
            if not is_directory:
                total_uncompressed += info.file_size
                _require(
                    total_uncompressed <= MAX_TOTAL_UNCOMPRESSED_BYTES,
                    "release archive expands beyond the total size limit",
                )
        for folded, is_directory in entry_kinds.items():
            parts = folded.split("/")
            for length in range(1, len(parts)):
                prefix = "/".join(parts[:length])
                _require(
                    prefix not in entry_kinds or entry_kinds[prefix] is True,
                    f"archive file is also used as a directory: {folded}",
                )
        explicit_directories = [info.filename for info in all_infos if info.is_dir()]
        if explicit_directories:
            raise FrozenReleaseError(
                f"archive contains unsealed directory entries: {explicit_directories[0]}"
            )
        infos = all_infos
        names = [info.filename for info in infos]
        _require(len(names) == len(set(names)), "archive contains duplicate paths")
        _require(len(names) == len({name.casefold() for name in names}), "archive contains case-colliding paths")
        for name in names:
            pure = PurePosixPath(name)
            _require(
                not pure.is_absolute()
                and ".." not in pure.parts
                and "\\" not in name
                and len(pure.parts) >= 2
                and pure.parts[0] == TOP_LEVEL,
                f"unsafe archive path: {name}",
            )
        retired_members = {
            f"{TOP_LEVEL}/{relative}" for relative in RETIRED_HELPER_EXECUTABLES
        }
        retired_present = sorted(retired_members & set(names))
        _require(
            not retired_present,
            f"retired helper executables remain packaged: {retired_present}",
        )

        by_name = {info.filename: info for info in infos}
        validator = _load_release_archive_validator()
        expected_zip_timestamp = validator._zip_datetime(expected_source_epoch)
        _require(archive.comment == b"", "release archive comment is not deterministic")
        for info in infos:
            unix_mode = (info.external_attr >> 16) & 0xFFFF
            _require(
                info.date_time == expected_zip_timestamp
                and info.compress_type == zipfile.ZIP_DEFLATED
                and info.create_system == 3
                and unix_mode == (stat.S_IFREG | 0o644)
                and info.extra == b""
                and info.comment == b"",
                f"archive member metadata differs from the qualified builder: {info.filename}",
            )

        with tempfile.TemporaryDirectory(prefix="label-match-frozen-verify-") as temporary:
            package_root = Path(temporary) / TOP_LEVEL
            _extract_validated_archive(archive, infos, package_root)
            try:
                evidence = validator.validate_release_evidence(
                    package_root,
                    expected_tag=expected_tag,
                    source_epoch=expected_source_epoch,
                )
            except (OSError, ValueError, validator.ReleaseArchiveError) as exc:
                raise FrozenReleaseError(
                    f"exact build-time release evidence validation failed: {exc}"
                ) from exc
            _require(
                evidence.get("commit") == expected_commit
                and evidence.get("tree") == expected_tree,
                "exact build-time validator source identity differs",
            )

        manifest_name = f"{TOP_LEVEL}/build-manifest.json"
        _require(manifest_name in by_name, "archive is missing the sealed build manifest")
        _require(
            by_name[manifest_name].file_size <= MAX_JSON_MEMBER_BYTES,
            "archive build manifest is unexpectedly large",
        )
        manifest = _json_bytes(archive.read(manifest_name), "build manifest")
        inventory = manifest.get("payload_inventory")
        _require(isinstance(inventory, list), "validated build manifest inventory is invalid")
        main_exe_name = f"{TOP_LEVEL}/Label_Match.exe"
        _require(main_exe_name in by_name, "archive is missing the main executable")
        main_exe_sha = _sha256_zip_member(archive, by_name[main_exe_name])
        _require(
            main_exe_sha == expected_main_exe_sha256,
            "main executable differs from expected frozen SHA-256",
        )

    _require(_sha256_file(archive_path) == archive_sha, "archive bytes changed during verification")
    _require(archive_path.stat().st_size == expected_archive_size, "archive size changed during verification")
    return {
        "schema_version": "label-match-frozen-release-assets-v1",
        "status": "PASS",
        "tag": expected_tag,
        "tag_object": expected_tag_object,
        "commit": expected_commit,
        "tree": expected_tree,
        "source_epoch": expected_source_epoch,
        "archive": archive_path.name,
        "archive_sha256": archive_sha,
        "archive_size": archive_path.stat().st_size,
        "archive_file_count": len(names),
        "payload_inventory_sha256": manifest["payload_inventory_sha256"],
        "payload_file_count": len(inventory),
        "contract_bundle_sha256": CONTRACT_BUNDLE_SHA256,
        "safe_paths": True,
        "crc_verified": True,
        "exact_membership": True,
        "manifest_byte_parity": True,
        "embedded_identities_verified": True,
        "staged_installer_verified": True,
        "factory_manifest_verified": True,
        "retired_helper_executables_absent": evidence[
            "retired_helper_executables_absent"
        ],
        "qualification_receipt_status": (
            "PASS" if receipt is not None else "NOT_TESTED_EXTERNAL_REQUIRED"
        ),
        "qualification_receipt_sha256": (
            _sha256_file(qualification_receipt_path.resolve())
            if qualification_receipt_path is not None
            else None
        ),
        "main_exe_sha256": main_exe_sha,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify frozen Label_Match release assets")
    parser.add_argument("--archive", required=True)
    parser.add_argument("--checksum", required=True)
    parser.add_argument("--qualification-receipt", default="")
    parser.add_argument("--expected-tag", required=True)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--expected-tree", required=True)
    parser.add_argument("--expected-tag-object", required=True)
    parser.add_argument("--expected-source-epoch", required=True, type=int)
    parser.add_argument("--expected-archive-sha256", default="")
    parser.add_argument("--expected-archive-size", default=0, type=int)
    parser.add_argument("--expected-main-exe-sha256", default="")
    parser.add_argument("--report", default="")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = verify_frozen_release_assets(
            Path(args.archive),
            Path(args.checksum),
            expected_tag=args.expected_tag,
            expected_commit=args.expected_commit,
            expected_tree=args.expected_tree,
            expected_tag_object=args.expected_tag_object,
            expected_source_epoch=args.expected_source_epoch,
            expected_archive_sha256=args.expected_archive_sha256,
            expected_archive_size=args.expected_archive_size,
            expected_main_exe_sha256=args.expected_main_exe_sha256,
            qualification_receipt_path=(
                Path(args.qualification_receipt)
                if args.qualification_receipt
                else None
            ),
        )
    except FrozenReleaseError as exc:
        print(f"frozen_release_assets=DENY reason={exc}")
        return 2
    payload = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.report:
        report_path = Path(args.report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(payload, encoding="utf-8", newline="\n")
    print(payload, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python
"""Build the Label_Match signed-CPython portable tree without freezing."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import shutil
import subprocess
import sys
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from kmtech_zero_pe.release_signature import validate_public_key_config  # noqa: E402

EXPECTED_PYTHON = (3, 12, 10)
PORTABLE_SCHEMA = "label-match-portable-tree-v1"
RUNTIME_ROOT_FILES = (
    "LICENSE.txt",
    "python.exe",
    "pythonw.exe",
    "python3.dll",
    "python312.dll",
    "vcruntime140.dll",
    "vcruntime140_1.dll",
)
APP_PACKAGE_DIRS = (
    "kmtech_factory_contracts",
    "kmtech_zero_pe",
    "ui",
)
APP_DATA_DIRS = ("assets", "config")
APP_DATA_FILES = ("contract.lock.json", "kmtech_zero_pe.vendor.json")
APP_TOOL_FILES = (
    "label_auth_recovery_canary.py",
    "label_legacy_task_quiescence.py",
)
UPDATE_KEY_CONFIG_FILENAME = "update-manifest-key-config.json"
UPDATE_KEY_CONFIG_SCHEMA = "label-match-update-key-config-v1"
CANONICAL_INSTALLER_FILENAME = "INSTALL_CANONICAL_PORTABLE.ps1"
LEGACY_INSTALLER_FILENAME = "INSTALL_THIS_PC.ps1"
BOOTSTRAP_INTEGRITY_HELPER = Path("tools/bootstrap_integrity.ps1")
THIRD_PARTY = {
    "babel": ("2.18.0", ("babel",)),
    "certifi": ("2026.6.17", ("certifi",)),
    "chardet": ("5.2.0", ("chardet",)),
    "idna": ("3.18", ("idna",)),
    "qrcode": ("8.2", ("qrcode",)),
    "requests": ("2.34.2", ("requests",)),
    "tkcalendar": ("1.6.1", ("tkcalendar",)),
    "typing-extensions": ("4.15.0", ("typing_extensions.py",)),
    "urllib3": ("2.7.0", ("urllib3",)),
}
ALLOWED_APP_NATIVE_NAMES: set[str] = set()


class PortableBuildError(RuntimeError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _ignore(_directory: str, names: list[str]) -> set[str]:
    ignored = {
        name
        for name in names
        if name == "__pycache__" or name.endswith((".pyc", ".pyo"))
    }
    return ignored


def _copy_tree(source: Path, target: Path) -> None:
    if not source.is_dir():
        raise PortableBuildError(f"required source directory is missing: {source}")
    shutil.copytree(source, target, ignore=_ignore)


def _runtime_source(python_home: Path, runtime: Path) -> None:
    if tuple(sys.version_info[:3]) != EXPECTED_PYTHON:
        raise PortableBuildError(
            f"builder Python must be exact {EXPECTED_PYTHON}, got {tuple(sys.version_info[:3])}"
        )
    if platform.architecture()[0] != "64bit":
        raise PortableBuildError("portable runtime requires 64-bit CPython")
    runtime.mkdir(parents=True)
    for name in RUNTIME_ROOT_FILES:
        source = python_home / name
        if not source.is_file():
            raise PortableBuildError(
                f"curated CPython runtime file is missing: {source}"
            )
        shutil.copy2(source, runtime / name)

    dll_source = python_home / "DLLs"
    dll_target = runtime / "DLLs"
    dll_target.mkdir()
    for source in sorted(dll_source.iterdir(), key=lambda item: item.name.casefold()):
        if source.is_file() and source.suffix.casefold() in {".dll", ".pyd"}:
            shutil.copy2(source, dll_target / source.name)

    lib_source = python_home / "Lib"
    lib_target = runtime / "Lib"

    def lib_ignore(directory: str, names: list[str]) -> set[str]:
        ignored = _ignore(directory, names)
        if Path(directory).resolve() == lib_source.resolve():
            ignored.update(
                {"site-packages", "ensurepip", "idlelib", "test", "turtledemo"}
            )
        return ignored

    shutil.copytree(lib_source, lib_target, ignore=lib_ignore)

    def tcl_ignore(directory: str, names: list[str]) -> set[str]:
        ignored = _ignore(directory, names)
        ignored.update(
            name
            for name in names
            if Path(name).suffix.casefold() in {".dll", ".exe", ".pyd"}
        )
        return ignored

    shutil.copytree(python_home / "tcl", runtime / "tcl", ignore=tcl_ignore)


def _distribution_root(name: str, expected_version: str) -> tuple[Path, Path]:
    distribution = importlib.metadata.distribution(name)
    actual = distribution.version
    if actual != expected_version:
        raise PortableBuildError(
            f"distribution {name} must be exact {expected_version}, got {actual}"
        )
    metadata_path = Path(distribution._path).resolve()  # type: ignore[attr-defined]
    return metadata_path.parent, metadata_path


def _copy_third_party(site_packages: Path) -> dict[str, str]:
    versions: dict[str, str] = {}
    locations: set[Path] = set()
    for distribution_name, (version, package_names) in THIRD_PARTY.items():
        source_root, metadata_path = _distribution_root(distribution_name, version)
        locations.add(source_root)
        versions[distribution_name] = version
        for package_name in package_names:
            source = source_root / package_name
            target = site_packages / package_name
            if source.is_dir():
                _copy_tree(source, target)
            elif source.is_file():
                shutil.copy2(source, target)
            else:
                raise PortableBuildError(
                    f"distribution {distribution_name} payload is missing: {source}"
                )
        shutil.copytree(
            metadata_path, site_packages / metadata_path.name, ignore=_ignore
        )
    if len(locations) != 1:
        raise PortableBuildError(
            "third-party packages must come from one exact environment"
        )
    return versions


def _copy_application(repo_root: Path, app_root: Path) -> None:
    app_root.mkdir(parents=True)
    for source in sorted(repo_root.glob("*.py"), key=lambda item: item.name.casefold()):
        shutil.copy2(source, app_root / source.name)
    for name in APP_PACKAGE_DIRS:
        _copy_tree(repo_root / name, app_root / name)
    for name in APP_DATA_DIRS:
        _copy_tree(repo_root / name, app_root / name)
    for name in APP_DATA_FILES:
        source = repo_root / name
        if not source.is_file():
            raise PortableBuildError(f"application data file is missing: {source}")
        shutil.copy2(source, app_root / name)
    tools_root = app_root / "tools"
    tools_root.mkdir()
    for name in APP_TOOL_FILES:
        source = repo_root / "tools" / name
        if not source.is_file():
            raise PortableBuildError(f"application tool file is missing: {source}")
        shutil.copy2(source, tools_root / name)
    shutil.copy2(repo_root / "portable" / "main.py", app_root / "main.py")


def _is_pe(path: Path) -> bool:
    if not path.is_file() or path.stat().st_size < 2:
        return False
    with path.open("rb") as handle:
        return handle.read(2) == b"MZ"


def _app_native_inventory(app_root: Path) -> list[str]:
    native = sorted(
        path.relative_to(app_root).as_posix()
        for path in app_root.rglob("*")
        if _is_pe(path)
    )
    names = {Path(path).name for path in native}
    if names != ALLOWED_APP_NATIVE_NAMES or native:
        raise PortableBuildError(
            "portable app native closure must be empty: " + ", ".join(native)
        )
    return native


def _update_public_key_config(value: str) -> str | dict[str, object]:
    text = str(value or "").strip()
    if not text:
        raise PortableBuildError("portable build requires an update public key config")
    try:
        validate_public_key_config(text)
    except ValueError as exc:
        raise PortableBuildError(
            "portable update public key config is invalid"
        ) from exc
    if len(text) == 64 and all(
        character in "0123456789abcdefABCDEF" for character in text
    ):
        return text.lower()
    try:
        document = json.loads(text)
    except json.JSONDecodeError as exc:
        raise PortableBuildError(
            "portable update public key config is malformed"
        ) from exc
    if not isinstance(document, dict):
        raise PortableBuildError("portable update public key config must be an object")
    return document


def _write_update_key_config(app_root: Path, value: str) -> str:
    configured = _update_public_key_config(value)
    payload = {
        "schema": UPDATE_KEY_CONFIG_SCHEMA,
        "manifest_public_key": configured,
    }
    raw = (
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    path = app_root / UPDATE_KEY_CONFIG_FILENAME
    path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


def _tree_metrics(root: Path) -> tuple[int, int]:
    files = [path for path in root.rglob("*") if path.is_file()]
    return len(files), sum(path.stat().st_size for path in files)


def _git_value(repo_root: Path, expression: str) -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "--verify", expression],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if completed.returncode != 0:
        raise PortableBuildError(f"git identity is unavailable: {expression}")
    return completed.stdout.strip().lower()


def _assert_clean_source(repo_root: Path) -> None:
    completed = subprocess.run(
        ["git", "status", "--porcelain=v1", "--untracked-files=normal"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if completed.returncode != 0:
        raise PortableBuildError("git source cleanliness could not be verified")
    if completed.stdout.strip():
        raise PortableBuildError("portable source tree must be clean and committed")


def build(
    repo_root: Path,
    python_home: Path,
    output: Path,
    *,
    update_manifest_public_key_config: str,
) -> dict[str, object]:
    repo_root = repo_root.resolve()
    python_home = python_home.resolve()
    output = output.resolve()
    _assert_clean_source(repo_root)
    if output.exists():
        raise PortableBuildError(f"portable output already exists: {output}")
    output.mkdir(parents=True)
    _runtime_source(python_home, output / "runtime")
    app_root = output / "app"
    _copy_application(repo_root, app_root)
    update_key_config_sha256 = _write_update_key_config(
        app_root,
        update_manifest_public_key_config,
    )
    site_packages = app_root / "site-packages"
    site_packages.mkdir()
    versions = _copy_third_party(site_packages)
    shutil.copy2(
        repo_root / "portable" / "launch-label-match.cmd",
        output / "launch-label-match.cmd",
    )
    installer_source = repo_root / CANONICAL_INSTALLER_FILENAME
    if not installer_source.is_file():
        raise PortableBuildError(
            f"canonical portable installer is missing: {installer_source}"
        )
    shutil.copy2(installer_source, output / CANONICAL_INSTALLER_FILENAME)
    legacy_installer_source = repo_root / LEGACY_INSTALLER_FILENAME
    if not legacy_installer_source.is_file():
        raise PortableBuildError(
            f"legacy compatibility installer is missing: {legacy_installer_source}"
        )
    shutil.copy2(legacy_installer_source, output / LEGACY_INSTALLER_FILENAME)
    bootstrap_helper_source = repo_root / BOOTSTRAP_INTEGRITY_HELPER
    if not bootstrap_helper_source.is_file():
        raise PortableBuildError(
            f"bootstrap integrity helper is missing: {bootstrap_helper_source}"
        )
    bootstrap_helper_target = output / BOOTSTRAP_INTEGRITY_HELPER
    bootstrap_helper_target.parent.mkdir()
    shutil.copy2(bootstrap_helper_source, bootstrap_helper_target)
    native = _app_native_inventory(app_root)
    forbidden_roots = [
        name
        for name in ("PIL", "pygame", "charset_normalizer", "cryptography", "cffi")
        if (site_packages / name).exists()
    ]
    if forbidden_roots:
        raise PortableBuildError(
            "forbidden package roots entered the portable tree: "
            + ", ".join(forbidden_roots)
        )
    file_count, byte_count = _tree_metrics(output)
    manifest = {
        "schema": PORTABLE_SCHEMA,
        "source_commit": _git_value(repo_root, "HEAD^{commit}"),
        "source_tree": _git_value(repo_root, "HEAD^{tree}"),
        "python_version": ".".join(str(value) for value in EXPECTED_PYTHON),
        "python_home": str(python_home),
        "runtime_python_sha256": _sha256(output / "runtime" / "python.exe"),
        "runtime_pythonw_sha256": _sha256(output / "runtime" / "pythonw.exe"),
        "entrypoint": "runtime/pythonw.exe app/main.py",
        "launcher": "launch-label-match.cmd",
        "launcher_sha256": _sha256(output / "launch-label-match.cmd"),
        "canonical_installer": CANONICAL_INSTALLER_FILENAME,
        "canonical_installer_sha256": _sha256(output / CANONICAL_INSTALLER_FILENAME),
        "third_party_versions": versions,
        "allowed_unsigned_app_pe": native,
        "update_key_config_sha256": update_key_config_sha256,
        "forbidden_package_roots": forbidden_roots,
        "file_count_before_manifest": file_count,
        "byte_count_before_manifest": byte_count,
    }
    (output / "portable-manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    return manifest


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--python-home", type=Path, default=Path(sys.base_prefix))
    parser.add_argument(
        "--repo-root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument("--update-manifest-public-key-config", required=True)
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    result = build(
        args.repo_root,
        args.python_home,
        args.output,
        update_manifest_public_key_config=args.update_manifest_public_key_config,
    )
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

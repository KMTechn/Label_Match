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
THIRD_PARTY = {
    "babel": ("2.18.0", ("babel",)),
    "certifi": ("2026.6.17", ("certifi",)),
    "cffi": ("2.1.0", ("cffi",)),
    "chardet": ("5.2.0", ("chardet",)),
    "cryptography": ("49.0.0", ("cryptography",)),
    "idna": ("3.18", ("idna",)),
    "pycparser": ("3.0", ("pycparser",)),
    "qrcode": ("8.2", ("qrcode",)),
    "requests": ("2.34.2", ("requests",)),
    "tkcalendar": ("1.6.1", ("tkcalendar",)),
    "typing-extensions": ("4.15.0", ("typing_extensions.py",)),
    "urllib3": ("2.7.0", ("urllib3",)),
}
ALLOWED_APP_NATIVE_NAMES = {
    "_cffi_backend.cp312-win_amd64.pyd",
    "_rust.pyd",
}


class PortableBuildError(RuntimeError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _ignore(_directory: str, names: list[str]) -> set[str]:
    ignored = {name for name in names if name == "__pycache__" or name.endswith((".pyc", ".pyo"))}
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
            raise PortableBuildError(f"curated CPython runtime file is missing: {source}")
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
            ignored.update({"site-packages", "ensurepip", "idlelib", "test", "turtledemo"})
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
        shutil.copytree(metadata_path, site_packages / metadata_path.name, ignore=_ignore)
    if len(locations) != 1:
        raise PortableBuildError("third-party packages must come from one exact environment")
    source_root = next(iter(locations))
    for backend in sorted(source_root.glob("_cffi_backend*.pyd")):
        shutil.copy2(backend, site_packages / backend.name)
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
    if names != ALLOWED_APP_NATIVE_NAMES or len(native) != 2:
        raise PortableBuildError(
            "portable app native closure must be exactly cryptography+cffi: "
            + ", ".join(native)
        )
    return native


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


def build(repo_root: Path, python_home: Path, output: Path) -> dict[str, object]:
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
    site_packages = app_root / "site-packages"
    site_packages.mkdir()
    versions = _copy_third_party(site_packages)
    shutil.copy2(
        repo_root / "portable" / "launch-label-match.cmd",
        output / "launch-label-match.cmd",
    )
    native = _app_native_inventory(app_root)
    forbidden_roots = [
        name
        for name in ("PIL", "pygame", "charset_normalizer")
        if (site_packages / name).exists()
    ]
    if forbidden_roots:
        raise PortableBuildError(
            "forbidden package roots entered the portable tree: " + ", ".join(forbidden_roots)
        )
    file_count, byte_count = _tree_metrics(output)
    manifest = {
        "schema": PORTABLE_SCHEMA,
        "source_commit": _git_value(repo_root, "HEAD^{commit}"),
        "source_tree": _git_value(repo_root, "HEAD^{tree}"),
        "python_version": ".".join(str(value) for value in EXPECTED_PYTHON),
        "python_home": str(python_home),
        "runtime_pythonw_sha256": _sha256(output / "runtime" / "pythonw.exe"),
        "entrypoint": "runtime/pythonw.exe app/main.py",
        "launcher": "launch-label-match.cmd",
        "launcher_sha256": _sha256(output / "launch-label-match.cmd"),
        "third_party_versions": versions,
        "allowed_unsigned_app_pe": native,
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
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    result = build(args.repo_root, args.python_home, args.output)
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

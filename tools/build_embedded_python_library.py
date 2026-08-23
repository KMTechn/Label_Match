#!/usr/bin/env python
"""Materialize pure modules beside the frozen runtime for in-process hosting."""

from __future__ import annotations

import argparse
import ast
from pathlib import Path


class EmbeddedLibraryBuildError(RuntimeError):
    pass


def _module_relative_path(module_name: str, source_path: Path | None) -> Path:
    module_parts = module_name.split(".")
    if not module_parts or any(not part for part in module_parts):
        raise EmbeddedLibraryBuildError(f"invalid module name: {module_name!r}")
    if source_path is None or source_path.name == "__init__.py":
        return Path(*module_parts, "__init__.py")
    return Path(*module_parts).with_suffix(".py")


def _load_pure_modules(toc_path: Path) -> list[tuple[str, Path | None]]:
    try:
        payload = ast.literal_eval(toc_path.read_text(encoding="utf-8"))
    except (OSError, SyntaxError, ValueError) as exc:
        raise EmbeddedLibraryBuildError(f"invalid PyInstaller analysis TOC: {toc_path}") from exc
    if not isinstance(payload, tuple) or len(payload) < 15 or not isinstance(payload[14], list):
        raise EmbeddedLibraryBuildError("PyInstaller analysis TOC shape is invalid")
    pure_rows = payload[14]
    modules: list[tuple[str, Path | None]] = []
    for row in pure_rows:
        if not isinstance(row, tuple) or len(row) != 3 or row[2] != "PYMODULE":
            raise EmbeddedLibraryBuildError("PyInstaller analysis TOC module row is invalid")
        module_name = str(row[0])
        source_text = str(row[1])
        source_path = None if source_text == "-" else Path(source_text).resolve()
        if source_path is not None and (
            source_path.suffix.lower() != ".py" or not source_path.is_file()
        ):
            raise EmbeddedLibraryBuildError(f"pure module source is missing: {module_name}")
        modules.append((module_name, source_path))
    modules.sort(key=lambda item: item[0])
    if not modules:
        raise EmbeddedLibraryBuildError("PyInstaller analysis TOC has no pure modules")
    return modules


def build_embedded_python_library(toc_path: Path, runtime_root: Path) -> int:
    toc_path = toc_path.resolve()
    runtime_root = runtime_root.resolve()
    if not runtime_root.is_dir():
        raise EmbeddedLibraryBuildError(f"runtime root is missing: {runtime_root}")
    modules = _load_pure_modules(toc_path)
    targets: list[tuple[Path, Path | None]] = []
    seen_targets: set[Path] = set()
    for module_name, source_path in modules:
        relative_path = _module_relative_path(module_name, source_path)
        target_path = runtime_root / relative_path
        if target_path in seen_targets:
            raise EmbeddedLibraryBuildError(f"duplicate module path: {relative_path.as_posix()}")
        if target_path.exists():
            raise EmbeddedLibraryBuildError(f"refusing to overwrite runtime member: {target_path}")
        seen_targets.add(target_path)
        targets.append((target_path, source_path))
    for target_path, source_path in targets:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(b"" if source_path is None else source_path.read_bytes())
    return len(modules)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--analysis-toc", required=True)
    parser.add_argument("--runtime-root", required=True)
    args = parser.parse_args(argv)
    try:
        module_count = build_embedded_python_library(
            Path(args.analysis_toc),
            Path(args.runtime_root),
        )
    except EmbeddedLibraryBuildError as exc:
        print(f"BLOCKED: {exc}")
        return 2
    print(f"embedded_python_module_count={module_count}")
    print(f"embedded_python_runtime_root={Path(args.runtime_root).resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

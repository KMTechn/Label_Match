"""Read-only shared package/manifest check using this app's committed lock.

The four validation functions below are copied unchanged from
kmtech_shared c067d38 manifest/sync_shared.py; the explicit integration test
checks their source against the canonical checkout. No sibling is needed here.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[1]
FILES = ("kmtech_shared/__init__.py", "kmtech_shared/catalog.py", "kmtech_shared/raster.py",
         "kmtech_shared/runtime.py")
SCHEMA = "kmtech.shared.source.v1"


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def inventory(root: Path) -> dict[str, str]:
    package = root / "kmtech_shared"
    if package.is_symlink() or (hasattr(package, "is_junction") and package.is_junction()):
        raise ValueError("package must be a literal directory")
    found = set()
    for path in package.rglob("*"):
        if path.is_symlink() or (hasattr(path, "is_junction") and path.is_junction()):
            raise ValueError("package entries must not be links")
        if "__pycache__" in path.relative_to(package).parts:
            continue
        if path.is_file():
            found.add(path.relative_to(root).as_posix())
    if found != set(FILES):
        raise ValueError(f"package inventory mismatch: missing={sorted(set(FILES) - found)}, extra={sorted(found - set(FILES))}")
    return {name: digest((root / name).read_bytes()) for name in FILES}


def package_version(root: Path) -> str:
    tree = ast.parse((root / FILES[0]).read_text(encoding="utf-8"))
    values = [ast.literal_eval(n.value) for n in tree.body if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "__version__" for t in n.targets)]
    if len(values) != 1 or not isinstance(values[0], str):
        raise ValueError("package must declare one literal version")
    return values[0]


def check_manifest(root: Path, manifest: Path, expected_sha256: str | None = None) -> dict:
    raw = manifest.read_bytes()
    if expected_sha256 is not None:
        if not re.fullmatch(r"[0-9a-f]{64}", expected_sha256) or digest(raw) != expected_sha256:
            raise ValueError("manifest pin mismatch")
    record = json.loads(raw)
    if not isinstance(record, dict) or set(record) != {"schema", "version", "source_commit", "files"}:
        raise ValueError("manifest fields mismatch")
    if record["schema"] != SCHEMA or not isinstance(record["source_commit"], str) or not re.fullmatch(r"[0-9a-f]{40}", record["source_commit"]):
        raise ValueError("manifest schema or source commit is invalid")
    if record["files"] != inventory(root):
        raise ValueError("manifest file hashes mismatch")
    if record["version"] != package_version(root):
        raise ValueError("manifest version mismatch")
    return record


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", required=True, help="read-only pin check")
    parser.add_argument("--root", type=Path, default=ROOT, help="app root; defaults to this checkout")
    args = parser.parse_args(argv)
    try:
        root = args.root.resolve()
        lock = json.loads((root / "kmtech_shared.lock.json").read_bytes())
        if (
            not isinstance(lock, dict)
            or set(lock) != {"version", "manifest_sha256"}
            or not isinstance(lock["version"], str)
            or not isinstance(lock["manifest_sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", lock["manifest_sha256"])
        ):
            raise ValueError("shared lock fields or values are invalid")
        result = check_manifest(root, root / "kmtech_shared.manifest.json", lock["manifest_sha256"])
        if lock["version"] != result["version"]:
            raise ValueError("shared lock version mismatch")
        print(f"PASS shared {result['version']}: {len(result['files'])} files at {result['source_commit']}")
        return 0
    except (OSError, ValueError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

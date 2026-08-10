"""Locate and read the immutable bundled contract set."""

from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import Any

from .canonical import load_json_strict, verify_contract_set


def bundle_root() -> Path:
    return Path(__file__).resolve().parent / "bundle" / "v1"


CONTRACT_BUNDLE_VERSION = "1.0.0"
CONTRACT_BUNDLE_SHA256 = (bundle_root() / "bundle.sha256").read_text(
    encoding="ascii"
).strip()


def load_contract_document(relative_path: str) -> Any:
    relative = PurePosixPath(relative_path)
    if relative.is_absolute() or ".." in relative.parts or "\\" in relative_path:
        raise ValueError("contract document path must be a normalized relative POSIX path")
    return load_json_strict(bundle_root() / Path(*relative.parts))


def verify_bundled_contracts(*, expected_sha256: str | None = None) -> dict[str, Any]:
    expected = expected_sha256 or CONTRACT_BUNDLE_SHA256
    return verify_contract_set(
        bundle_root(),
        expected_version=CONTRACT_BUNDLE_VERSION,
        expected_sha256=expected,
    )

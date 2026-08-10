"""Canonical JSON and deterministic contract-set hashing."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Mapping

from .errors import FactoryContractError


CONTRACT_SET_FILENAME = "contract-set.json"
BUNDLE_HASH_FILENAME = "bundle.sha256"


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON number is forbidden: {value}")


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def loads_json_strict(raw: bytes | str, *, source: str = "<memory>") -> Any:
    if isinstance(raw, bytes):
        if raw.startswith(b"\xef\xbb\xbf"):
            raise FactoryContractError(
                "CONTRACT_JSON_INVALID",
                f"UTF-8 BOM is forbidden: {source}",
            )
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise FactoryContractError(
                "CONTRACT_JSON_INVALID",
                f"contract JSON is not UTF-8: {source}",
            ) from exc
    else:
        text = raw
        if text.startswith("\ufeff"):
            raise FactoryContractError(
                "CONTRACT_JSON_INVALID",
                f"UTF-8 BOM is forbidden: {source}",
            )
    try:
        return json.loads(
            text,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_constant,
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise FactoryContractError(
            "CONTRACT_JSON_INVALID",
            f"invalid contract JSON: {source}",
        ) from exc


def load_json_strict(path: Path) -> Any:
    return loads_json_strict(path.read_bytes(), source=path.as_posix())


def canonical_json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise FactoryContractError(
            "CONTRACT_JSON_INVALID",
            "value cannot be represented as canonical JSON",
        ) from exc


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def require_posix_relative_path(value: str) -> str:
    if not value or "\\" in value:
        raise FactoryContractError(
            "CONTRACT_PATH_INVALID",
            f"contract path must use POSIX separators: {value!r}",
        )
    parsed = PurePosixPath(value)
    if parsed.is_absolute() or any(part in {"", ".", ".."} for part in parsed.parts):
        raise FactoryContractError(
            "CONTRACT_PATH_INVALID",
            f"contract path must be a normalized relative path: {value!r}",
        )
    return parsed.as_posix()


def _bundle_files(bundle_dir: Path) -> Iterable[Path]:
    excluded = {CONTRACT_SET_FILENAME, BUNDLE_HASH_FILENAME}
    for path in sorted(bundle_dir.rglob("*"), key=lambda item: item.as_posix()):
        if path.is_file() and path.name not in excluded:
            yield path


def build_contract_set(
    bundle_dir: Path,
    *,
    bundle_version: str,
) -> dict[str, Any]:
    files = []
    for path in _bundle_files(bundle_dir):
        relative = require_posix_relative_path(path.relative_to(bundle_dir).as_posix())
        files.append(
            {
                "path": relative,
                "size": path.stat().st_size,
                "sha256": file_sha256(path),
            }
        )
    return {
        "contract_set_schema_version": 1,
        "contract_bundle_version": bundle_version,
        "hash_algorithm": "sha256",
        "files": files,
    }


def contract_set_sha256(contract_set: Mapping[str, Any]) -> str:
    return canonical_sha256(dict(contract_set))


def write_contract_set(bundle_dir: Path, *, bundle_version: str) -> str:
    contract_set = build_contract_set(bundle_dir, bundle_version=bundle_version)
    rendered = json.dumps(
        contract_set,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        indent=2,
    ) + "\n"
    (bundle_dir / CONTRACT_SET_FILENAME).write_text(rendered, encoding="utf-8", newline="\n")
    digest = contract_set_sha256(contract_set)
    (bundle_dir / BUNDLE_HASH_FILENAME).write_text(digest + "\n", encoding="ascii", newline="\n")
    return digest


def verify_contract_set(
    bundle_dir: Path,
    *,
    expected_version: str | None = None,
    expected_sha256: str | None = None,
) -> dict[str, Any]:
    manifest_path = bundle_dir / CONTRACT_SET_FILENAME
    if not manifest_path.is_file():
        raise FactoryContractError("CONTRACT_SET_MISSING", "contract-set.json is missing")
    manifest = load_json_strict(manifest_path)
    if not isinstance(manifest, dict):
        raise FactoryContractError("CONTRACT_SET_INVALID", "contract-set.json must be an object")
    if manifest.get("contract_set_schema_version") != 1:
        raise FactoryContractError("CONTRACT_SET_INVALID", "unsupported contract-set schema")
    if manifest.get("hash_algorithm") != "sha256":
        raise FactoryContractError("CONTRACT_SET_INVALID", "unsupported hash algorithm")
    if expected_version and manifest.get("contract_bundle_version") != expected_version:
        raise FactoryContractError(
            "CONTRACT_VERSION_MISMATCH",
            "contract bundle version differs from the exact lock",
        )
    rows = manifest.get("files")
    if not isinstance(rows, list):
        raise FactoryContractError("CONTRACT_SET_INVALID", "contract file inventory must be an array")
    paths: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            raise FactoryContractError("CONTRACT_SET_INVALID", "contract file row must be an object")
        relative = require_posix_relative_path(str(row.get("path") or ""))
        paths.append(relative)
        path = bundle_dir / Path(*PurePosixPath(relative).parts)
        if not path.is_file():
            raise FactoryContractError("CONTRACT_FILE_MISSING", f"contract file is missing: {relative}")
        if row.get("size") != path.stat().st_size or row.get("sha256") != file_sha256(path):
            raise FactoryContractError(
                "CONTRACT_HASH_MISMATCH",
                f"contract file identity mismatch: {relative}",
            )
    if len(paths) != len(set(paths)) or paths != sorted(paths):
        raise FactoryContractError(
            "CONTRACT_SET_INVALID",
            "contract paths must be unique and lexicographically sorted",
        )
    actual_paths = [
        path.relative_to(bundle_dir).as_posix() for path in _bundle_files(bundle_dir)
    ]
    if paths != actual_paths:
        raise FactoryContractError(
            "CONTRACT_SET_INVALID",
            "contract inventory does not exactly match the bundle",
        )
    digest = contract_set_sha256(manifest)
    detached_path = bundle_dir / BUNDLE_HASH_FILENAME
    if detached_path.is_file() and detached_path.read_text(encoding="ascii").strip() != digest:
        raise FactoryContractError(
            "CONTRACT_HASH_MISMATCH",
            "detached contract bundle hash does not match contract-set.json",
        )
    if expected_sha256 and digest != expected_sha256:
        raise FactoryContractError(
            "CONTRACT_HASH_MISMATCH",
            "contract bundle hash differs from the exact lock",
        )
    return {"manifest": manifest, "sha256": digest}

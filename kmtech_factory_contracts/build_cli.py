"""Offline build identity, package manifest, and verifier CLI."""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from .bundle import CONTRACT_BUNDLE_SHA256, CONTRACT_BUNDLE_VERSION, load_contract_document
from .canonical import canonical_sha256
from .errors import FactoryContractError
from .lock import load_and_verify_contract_lock
from .package import (
    create_build_compatibility,
    create_build_manifest,
    verify_staged_package,
    write_json,
)


def _git(repository: Path, *args: str) -> str:
    process = subprocess.run(
        ["git", "-C", str(repository), *args],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if process.returncode != 0:
        raise FactoryContractError(
            "PACKAGE_PROVENANCE_MISMATCH",
            f"Git identity command failed: {' '.join(args)}",
        )
    return process.stdout.strip()


def _release_git_gate(repository: Path, app_version: str, source_commit: str) -> None:
    exact_tag = _git(repository, "describe", "--tags", "--exact-match", "HEAD")
    if exact_tag != app_version:
        raise FactoryContractError(
            "PACKAGE_PROVENANCE_MISMATCH",
            "release HEAD tag does not equal the application version",
        )
    origin_main = _git(repository, "rev-parse", "origin/main^{commit}")
    if origin_main != source_commit:
        raise FactoryContractError(
            "PACKAGE_PROVENANCE_MISMATCH",
            "release source is not the exact origin/main commit",
        )


def _resource_declaration(app_id: str) -> dict[str, Any]:
    document = load_contract_document("compatibility/resource-namespaces.json")
    matches = [row for row in document["resources"] if row.get("app_id") == app_id]
    if len(matches) != 1:
        raise FactoryContractError(
            "COMPATIBILITY_MATRIX_INVALID",
            "resource declaration is missing or ambiguous",
        )
    row = dict(matches[0])
    row.pop("app_id")
    row.pop("app_version")
    return row


def _coinstall_rows(app_id: str) -> list[dict[str, str]]:
    matrix = load_contract_document("compatibility/coinstall-matrix.json")
    resources = load_contract_document("compatibility/resource-namespaces.json")
    versions = {row["app_id"]: row["app_version"] for row in resources["resources"]}
    rows = []
    for pair in matrix["pairs"]:
        apps = pair["apps"]
        if app_id not in apps or pair["status"] not in {"allowed", "conditional"}:
            continue
        other = apps[0] if apps[1] == app_id else apps[1]
        rows.append(
            {
                "app_id": other,
                "app_version": versions[other],
                "status": pair["status"],
                "reason_code": pair["reason_code"],
                "shared_resources": sorted(pair["shared_resources"]),
            }
        )
    return rows


def prepare_identity(
    *,
    repository: Path,
    stage_root: Path,
    app_id: str,
    app_version: str,
    db_schema_current: int,
    development: bool,
) -> dict[str, Any]:
    repository = repository.resolve()
    stage_root = stage_root.resolve()
    lock_path = repository / "contract.lock.json"
    lock = load_and_verify_contract_lock(lock_path, expected_app_id=app_id)
    source_commit = _git(repository, "rev-parse", "HEAD^{commit}")
    source_tree = _git(repository, "rev-parse", "HEAD^{tree}")
    dirty = bool(_git(repository, "status", "--porcelain=v1", "--untracked-files=all"))
    if not development:
        if dirty:
            raise FactoryContractError(
                "PACKAGE_PROVENANCE_MISMATCH",
                "release build source tree is dirty",
            )
        _release_git_gate(repository, app_version, source_commit)
    supported = lock["db_schema_supported"]
    if not supported["minimum"] <= db_schema_current <= supported["maximum"]:
        raise FactoryContractError(
            "DB_SCHEMA_UNSUPPORTED",
            "build DB schema is outside the consumer lock range",
        )
    try:
        pyinstaller_version = importlib.metadata.version("pyinstaller")
    except importlib.metadata.PackageNotFoundError:
        pyinstaller_version = "not-installed"
    identity = {
        "build_identity_schema_version": 1,
        "app_id": app_id,
        "app_version": app_version,
        "source_commit": source_commit,
        "source_tree": source_tree,
        "dirty": dirty,
        "contract_bundle_version": CONTRACT_BUNDLE_VERSION,
        "contract_bundle_sha256": CONTRACT_BUNDLE_SHA256,
        "db_schema": {
            "current": db_schema_current,
            "minimum": supported["minimum"],
            "maximum": supported["maximum"],
        },
        "server_api_contract_version": lock["server_api_contract_version"],
        "event_contract_version": lock["event_contract_version"],
        "manifest_contract_version": lock["manifest_contract_version"],
        "dependency": lock["dependency"],
        "builder": {"name": "kmtech_factory_contracts.build_cli", "version": "1.0.0"},
        "python_version": ".".join(str(part) for part in sys.version_info[:3]),
        "pyinstaller_version": pyinstaller_version,
        "dependency_lock_sha256": canonical_sha256(lock),
        "build_compatibility_sha256": "0" * 64,
    }
    compatibility = create_build_compatibility(
        identity,
        resources=_resource_declaration(app_id),
        coinstall_with=_coinstall_rows(app_id),
    )
    identity["build_compatibility_sha256"] = canonical_sha256(compatibility)
    stage_root.mkdir(parents=True, exist_ok=True)
    write_json(stage_root / "build-identity.json", identity)
    write_json(stage_root / "build-compatibility.json", compatibility)
    shutil.copyfile(lock_path, stage_root / "contract.lock.json")
    return {
        "status": "PASS",
        "mode": "development" if development else "release",
        "dirty": dirty,
        "source_commit": source_commit,
        "source_tree": source_tree,
        "build_identity_sha256": canonical_sha256(identity),
        "build_compatibility_sha256": identity["build_compatibility_sha256"],
        "contract_bundle_sha256": CONTRACT_BUNDLE_SHA256,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    prepare = subparsers.add_parser("prepare")
    prepare.add_argument("--repository", type=Path, required=True)
    prepare.add_argument("--stage-root", type=Path, required=True)
    prepare.add_argument("--app-id", required=True)
    prepare.add_argument("--app-version", required=True)
    prepare.add_argument("--db-schema-current", required=True, type=int)
    prepare.add_argument("--development", action="store_true")
    manifest = subparsers.add_parser("manifest")
    manifest.add_argument("--stage-root", type=Path, required=True)
    manifest.add_argument("--expected-file", action="append", default=[])
    manifest.add_argument("--built-at-utc")
    verify = subparsers.add_parser("verify")
    verify.add_argument("--stage-root", type=Path, required=True)
    verify.add_argument("--development", action="store_true")
    verify.add_argument("--expected-contract-sha256")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "prepare":
            report = prepare_identity(
                repository=args.repository,
                stage_root=args.stage_root,
                app_id=args.app_id,
                app_version=args.app_version,
                db_schema_current=args.db_schema_current,
                development=args.development,
            )
        elif args.command == "manifest":
            manifest = create_build_manifest(
                args.stage_root,
                expected_files=args.expected_file,
                built_at_utc=args.built_at_utc,
            )
            write_json(args.stage_root / "build-manifest.json", manifest)
            report = {
                "status": "PASS",
                "payload_inventory_sha256": manifest["payload_inventory_sha256"],
                "file_count": len(manifest["payload_inventory"]),
            }
        else:
            report = verify_staged_package(
                args.stage_root,
                release_mode=not args.development,
                expected_contract_sha256=args.expected_contract_sha256,
            )
    except (FactoryContractError, OSError, ValueError) as exc:
        error = exc.as_dict() if isinstance(exc, FactoryContractError) else {
            "code": "PACKAGE_PROVENANCE_MISMATCH",
            "message": str(exc),
            "retryable": False,
            "details": {},
        }
        print(json.dumps({"status": "FAILED", "error": error}, ensure_ascii=False, sort_keys=True))
        return 1
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

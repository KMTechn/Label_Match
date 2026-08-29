#!/usr/bin/env python3
"""Capture or verify the evidence for Label exact-clone reconciliation.

This command is deliberately read-only with respect to both SQLite databases.
It writes only the requested new JSON evidence file and refuses to overwrite
an existing artifact.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from label_exact_clone_resolution import (  # noqa: E402
    ExactCloneResolutionError,
    capture_conflict_preimage,
    create_portable_successor_receipt,
    create_resolution_receipt,
    json_document_sha256,
    read_bounded_json,
    write_new_json,
)


def _default_root() -> Path:
    value = str(os.environ.get("LOCALAPPDATA") or "").strip()
    if not value:
        raise ExactCloneResolutionError("LOCALAPPDATA is unavailable")
    return Path(value) / "KMTech" / "DirectSync" / "label_match"


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--client-db", type=Path)
    parser.add_argument("--server-db", type=Path, required=True)
    parser.add_argument("--identity", type=Path)
    parser.add_argument("--credential", type=Path)
    parser.add_argument("--stop-marker", type=Path)
    parser.add_argument("--portable-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Capture the current Label exact-clone conflict or emit a receipt "
            "after an independently authorized reconciliation. The command "
            "does not change client/server state or remove the stop marker."
        )
    )
    subparsers = parser.add_subparsers(dest="operation", required=True)
    capture = subparsers.add_parser("capture", help="capture unresolved conflict preimage")
    _add_common(capture)
    receipt = subparsers.add_parser(
        "receipt", help="emit a receipt only when resolved state exactly matches preimage"
    )
    _add_common(receipt)
    receipt.add_argument("--preimage", type=Path, required=True)
    rebind = subparsers.add_parser(
        "rebind",
        help="rebind a valid receipt to the exact reviewed portable successor",
    )
    _add_common(rebind)
    rebind.add_argument("--preimage", type=Path, required=True)
    rebind.add_argument("--preimage-sha256", required=True)
    rebind.add_argument("--predecessor-receipt", type=Path, required=True)
    rebind.add_argument("--predecessor-receipt-sha256", required=True)
    rebind.add_argument("--repo-root", type=Path, required=True)
    rebind.add_argument("--expected-successor-commit", required=True)
    rebind.add_argument("--expected-successor-tree", required=True)
    rebind.add_argument("--expected-successor-manifest-sha256", required=True)
    rebind.add_argument("--expected-successor-installer-sha256", required=True)
    rebind.add_argument("--expected-changed-paths-sha256", required=True)
    rebind.add_argument("--rebind-evidence-output", type=Path, required=True)
    return parser.parse_args(argv)


def _paths(args: argparse.Namespace) -> dict[str, Path]:
    root = _default_root()
    return {
        "client_db_path": args.client_db
        or root / "queue" / "direct_sync_relay.sqlite3",
        "server_db_path": args.server_db,
        "identity_path": args.identity or root / "producer_identity.json",
        "credential_path": args.credential or root / "credential.json",
        "stop_marker_path": args.stop_marker
        or root / "control" / "label_match_user_relay.stop.json",
        "portable_root": args.portable_root,
    }


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        inputs = _paths(args)
        extra_summary = {}
        if args.operation == "capture":
            payload = capture_conflict_preimage(**inputs)
        elif args.operation == "receipt":
            preimage = read_bounded_json(
                args.preimage,
                label="Label exact-clone conflict preimage",
            )
            payload = create_resolution_receipt(preimage=preimage, **inputs)
        else:
            if args.output.resolve(strict=False) == args.rebind_evidence_output.resolve(
                strict=False
            ):
                raise ExactCloneResolutionError(
                    "successor receipt and rebind evidence outputs must differ"
                )
            if args.output.exists() or args.rebind_evidence_output.exists():
                raise ExactCloneResolutionError(
                    "refusing to overwrite successor receipt or rebind evidence"
                )
            payload, rebind_evidence = create_portable_successor_receipt(
                preimage_path=args.preimage,
                preimage_sha256=args.preimage_sha256,
                predecessor_receipt_path=args.predecessor_receipt,
                predecessor_receipt_sha256=args.predecessor_receipt_sha256,
                repo_root=args.repo_root,
                expected_successor_commit=args.expected_successor_commit,
                expected_successor_tree=args.expected_successor_tree,
                expected_successor_manifest_sha256=(
                    args.expected_successor_manifest_sha256
                ),
                expected_successor_installer_sha256=(
                    args.expected_successor_installer_sha256
                ),
                expected_changed_paths_sha256=args.expected_changed_paths_sha256,
                **inputs,
            )
        if args.operation == "rebind":
            output_sha256 = json_document_sha256(payload)
            rebind_evidence["successor_receipt"] = {
                "path": str(args.output.resolve(strict=False)),
                "sha256": output_sha256,
            }
            evidence_output = write_new_json(
                args.rebind_evidence_output,
                rebind_evidence,
            )
            evidence_sha256 = json_document_sha256(rebind_evidence)
            output = write_new_json(args.output, payload)
            extra_summary = {
                "rebind_evidence": str(evidence_output),
                "rebind_evidence_sha256": evidence_sha256,
            }
        else:
            output = write_new_json(args.output, payload)
            output_sha256 = json_document_sha256(payload)
        summary = {
            "status": payload["status"],
            "schema_version": payload["schema_version"],
            "output": str(output),
            "output_sha256": output_sha256,
            "client_state_mutated": False,
            "server_state_mutated": False,
            "stop_marker_removed": False,
            **extra_summary,
        }
        print(json.dumps(summary, ensure_ascii=True, sort_keys=True))
        return 0
    except ExactCloneResolutionError as exc:
        print(
            json.dumps(
                {
                    "status": "BLOCKED",
                    "error": str(exc),
                    "client_state_mutated": False,
                    "server_state_mutated": False,
                    "stop_marker_removed": False,
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 4


if __name__ == "__main__":
    raise SystemExit(main())

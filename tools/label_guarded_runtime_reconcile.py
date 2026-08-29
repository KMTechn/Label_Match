#!/usr/bin/env python3
"""Execute the approved, fail-closed Label exact-clone authority cutover."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import uuid


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from label_guarded_runtime_reconcile import (  # noqa: E402
    GuardedRuntimeReconcileError,
    RECONCILE_EXIT_CODE,
    run_guarded_runtime_reconcile,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Retire one exact stale Label authority, acquire a fresh authenticated "
            "server fence, and write the secret-free resolution receipt."
        )
    )
    parser.add_argument("--preimage", required=True)
    parser.add_argument("--client-backup", required=True)
    parser.add_argument("--client-backup-sha256", required=True)
    parser.add_argument("--live-client-proof", required=True)
    parser.add_argument("--live-client-proof-sha256", required=True)
    parser.add_argument("--server-initializer-proof", required=True)
    parser.add_argument("--server-initializer-proof-sha256", required=True)
    parser.add_argument("--server-source-root", required=True)
    parser.add_argument("--server-source-commit", required=True)
    parser.add_argument("--server-launcher", required=True)
    parser.add_argument("--server-launcher-sha256", required=True)
    parser.add_argument("--server-executable", required=True)
    parser.add_argument("--server-executable-sha256", required=True)
    parser.add_argument("--client-db", required=True)
    parser.add_argument("--server-db", required=True)
    parser.add_argument("--identity", required=True)
    parser.add_argument("--credential", required=True)
    parser.add_argument("--stop-marker", required=True)
    parser.add_argument("--portable-root", required=True)
    parser.add_argument("--receipt-output", required=True)
    parser.add_argument("--evidence-output", required=True)
    parser.add_argument("--tls-ca-bundle", default="")
    parser.add_argument("--tls-ca-bundle-sha256", required=True)
    return parser


def _write_new(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.link(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _preflight_output(path: Path) -> None:
    if path.exists():
        raise FileExistsError(f"output already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    token = uuid.uuid4().hex
    probe = path.with_name(f".{path.name}.{token}.probe")
    linked = path.with_name(f".{path.name}.{token}.linked")
    try:
        with probe.open("xb") as handle:
            handle.write(b"output-preflight\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.link(probe, linked)
    finally:
        linked.unlink(missing_ok=True)
        probe.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    evidence_path = Path(args.evidence_output).expanduser().resolve(strict=False)
    receipt_path = Path(args.receipt_output).expanduser().resolve(strict=False)
    started = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    try:
        if os.path.normcase(str(evidence_path)) == os.path.normcase(str(receipt_path)):
            raise ValueError("receipt and evidence outputs must differ")
        _preflight_output(evidence_path)
        _preflight_output(receipt_path)
        result = run_guarded_runtime_reconcile(
            preimage_path=args.preimage,
            client_backup_path=args.client_backup,
            client_backup_sha256=args.client_backup_sha256,
            live_client_proof_path=args.live_client_proof,
            live_client_proof_sha256=args.live_client_proof_sha256,
            server_initializer_proof_path=args.server_initializer_proof,
            server_initializer_proof_sha256=args.server_initializer_proof_sha256,
            server_source_root=args.server_source_root,
            server_source_commit=args.server_source_commit,
            server_launcher_path=args.server_launcher,
            server_launcher_sha256=args.server_launcher_sha256,
            server_executable_path=args.server_executable,
            server_executable_sha256=args.server_executable_sha256,
            client_db_path=args.client_db,
            server_db_path=args.server_db,
            identity_path=args.identity,
            credential_path=args.credential,
            stop_marker_path=args.stop_marker,
            portable_root=args.portable_root,
            receipt_output_path=args.receipt_output,
            tls_ca_bundle_path=args.tls_ca_bundle,
            tls_ca_bundle_sha256=args.tls_ca_bundle_sha256,
        )
    except Exception as exc:
        server_forward = bool(
            isinstance(exc, GuardedRuntimeReconcileError) and exc.server_forward
        )
        failure = {
            "schema_version": "label-match-guarded-runtime-reconcile-evidence-v1",
            "status": "FAILED_FORWARD_ONLY" if server_forward else "FAILED_NO_FORWARD",
            "started_at": started,
            "completed_at": datetime.now(timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "failure": str(exc)[:500],
            "error_type": exc.__class__.__name__,
            "server_forward": server_forward,
            "stop_marker_removed": False,
            "secret_material_included": False,
        }
        try:
            _write_new(evidence_path, failure)
        except Exception as write_error:
            failure["evidence_write_failure"] = write_error.__class__.__name__
        print(json.dumps(failure, ensure_ascii=True, sort_keys=True))
        return RECONCILE_EXIT_CODE
    evidence = {
        "schema_version": "label-match-guarded-runtime-reconcile-evidence-v1",
        "status": result["status"],
        "server_forward": True,
        "stop_marker_removed": False,
        "secret_material_included": False,
        "reconcile_result": result,
    }
    try:
        _write_new(evidence_path, evidence)
    except Exception as exc:
        failure = {
            "schema_version": "label-match-guarded-runtime-reconcile-evidence-v1",
            "status": "FAILED_FORWARD_ONLY",
            "started_at": started,
            "completed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "failure": f"forward transition succeeded but evidence publish failed: {exc}",
            "error_type": exc.__class__.__name__,
            "server_forward": True,
            "stop_marker_removed": False,
            "secret_material_included": False,
        }
        print(json.dumps(failure, ensure_ascii=True, sort_keys=True))
        return RECONCILE_EXIT_CODE
    print(
        json.dumps(
            {
                "status": evidence["status"],
                "receipt_path": result["receipt_path"],
                "receipt_sha256": result["receipt_sha256"],
                "selected_fence": result["selected_fence"],
                "evidence_output": str(evidence_path),
                "server_forward": True,
                "stop_marker_removed": False,
            },
            ensure_ascii=True,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

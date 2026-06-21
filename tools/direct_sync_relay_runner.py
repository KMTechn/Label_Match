#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Run one Label_Match direct-sync relay cycle."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from direct_sync_runtime import DirectSyncRuntimeConfig, enqueue_completed_source_file, run_relay_once  # noqa: E402


def _build_config(args: argparse.Namespace) -> DirectSyncRuntimeConfig:
    return DirectSyncRuntimeConfig(
        db_path=args.db_path,
        spool_dir=args.spool_dir,
        producer_manifest_path=args.producer_manifest_path,
        credential_path=args.credential_path,
        upload_status_dir=args.upload_status_dir,
        runtime_status_path=args.runtime_status_path,
        log_path=args.log_path,
        worker_id=args.worker_id,
        min_free_bytes=args.min_free_bytes,
        retry_base_seconds=args.retry_base_seconds,
        timeout_seconds=args.timeout_seconds,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Label_Match direct-sync relay runner")
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--spool-dir", required=True)
    parser.add_argument("--producer-manifest-path", required=True)
    parser.add_argument("--credential-path", required=True)
    parser.add_argument("--upload-status-dir", required=True)
    parser.add_argument("--runtime-status-path", required=True)
    parser.add_argument("--log-path", required=True)
    parser.add_argument("--worker-id", default="direct-sync-relay-label-match")
    parser.add_argument("--min-free-bytes", type=int, default=0)
    parser.add_argument("--retry-base-seconds", type=int, default=60)
    parser.add_argument("--timeout-seconds", type=int, default=30)
    parser.add_argument("--enqueue-source-file", default="")
    parser.add_argument("--relative-path", default="")
    args = parser.parse_args(argv)

    config = _build_config(args)
    if args.enqueue_source_file:
        status = enqueue_completed_source_file(
            config,
            source_file_path=args.enqueue_source_file,
            relative_path=args.relative_path,
        )
    else:
        status = run_relay_once(config)
    print(f"direct_sync_relay_status={status['status']}")
    return 2 if status["status"] == "blocked_disk_pressure" else 0


if __name__ == "__main__":
    raise SystemExit(main())

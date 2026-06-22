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


ALLOWED_SOURCE_PREFIX = "포장실작업이벤트로그_"
ALLOWED_SOURCE_SUFFIX = ".csv"


def _validate_source_glob(pattern: str) -> str:
    text = str(pattern or "").strip()
    if not text:
        raise SystemExit("source glob must not be empty")
    if "**" in text or "/" in text or "\\" in text:
        raise SystemExit("source glob must be a direct-child file pattern")
    return text


def _is_allowed_source_file(path: Path) -> bool:
    return path.name.startswith(ALLOWED_SOURCE_PREFIX) and path.suffix.lower() == ALLOWED_SOURCE_SUFFIX


def _scan_source_files(scan_source_dir: str, patterns: list[str], max_files: int) -> list[Path]:
    root = Path(scan_source_dir)
    if not root.is_dir():
        raise SystemExit(f"scan source dir does not exist: {root}")
    scan_patterns = [_validate_source_glob(pattern) for pattern in (patterns or ["*.csv"])]
    seen: set[str] = set()
    files: list[Path] = []
    for pattern in scan_patterns:
        for path in root.glob(pattern):
            if not path.is_file() or not _is_allowed_source_file(path):
                continue
            resolved = str(path.resolve())
            if resolved in seen:
                continue
            seen.add(resolved)
            files.append(path)
    files.sort(key=lambda item: (item.stat().st_mtime_ns, str(item)))
    return files[: max(0, max_files)]


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
        operator_pause_path=args.operator_pause_path,
        max_active_queue_count=args.max_active_queue_count,
        max_active_queue_age_seconds=args.max_active_queue_age_seconds,
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
    parser.add_argument("--operator-pause-path", default="")
    parser.add_argument("--max-active-queue-count", type=int, default=1000)
    parser.add_argument("--max-active-queue-age-seconds", type=int, default=24 * 60 * 60)
    parser.add_argument("--enqueue-source-file", default="")
    parser.add_argument("--relative-path", default="")
    parser.add_argument("--scan-source-dir", default="")
    parser.add_argument("--source-glob", action="append", default=[])
    parser.add_argument("--max-enqueue-files", type=int, default=100)
    args = parser.parse_args(argv)
    if args.enqueue_source_file and args.scan_source_dir:
        parser.error("--enqueue-source-file and --scan-source-dir are mutually exclusive")

    config = _build_config(args)
    if args.enqueue_source_file:
        status = enqueue_completed_source_file(
            config,
            source_file_path=args.enqueue_source_file,
            relative_path=args.relative_path,
        )
    elif args.scan_source_dir:
        statuses = []
        enqueued_count = 0
        attempted_count = 0
        preflight_status = None
        for source_file in _scan_source_files(
            args.scan_source_dir,
            args.source_glob,
            args.max_enqueue_files,
        ):
            current = enqueue_completed_source_file(config, source_file_path=source_file)
            if current["status"] in {"paused_by_operator", "blocked_queue_backpressure", "blocked_disk_pressure"}:
                preflight_status = current
                break
            statuses.append(current)
            attempted_count += 1
            if current["status"] == "enqueued":
                enqueued_count += 1
            else:
                current["scan_failed_source_file"] = str(source_file)
                break
        status = preflight_status or (statuses[-1] if statuses else {"status": "scan_no_files"})
        status["scan_enqueued_count"] = enqueued_count
        status["scan_attempted_count"] = attempted_count
    else:
        status = run_relay_once(config)
    print(f"direct_sync_relay_status={status['status']}")
    if "scan_enqueued_count" in status:
        print(f"direct_sync_scan_enqueued_count={status['scan_enqueued_count']}")
    if "scan_attempted_count" in status:
        print(f"direct_sync_scan_attempted_count={status['scan_attempted_count']}")
    if status.get("scan_failed_source_file"):
        print(f"direct_sync_scan_failed_source_file={status['scan_failed_source_file']}")
    if status["status"] in {"blocked_disk_pressure", "blocked_queue_backpressure"}:
        return 2
    if status["status"] in {"enqueue_error", "runtime_error"}:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

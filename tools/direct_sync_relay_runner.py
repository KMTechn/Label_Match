#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Run one Label_Match direct-sync relay cycle."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from direct_sync_runtime import DirectSyncRuntimeConfig, enqueue_completed_source_file, run_relay_once, utc_now_text  # noqa: E402


ALLOWED_SOURCE_PREFIX = "포장실작업이벤트로그_"
ALLOWED_SOURCE_SUFFIX = ".csv"
DELTA_PROGRESS_STATUSES = {"pending", "leased", "retry_wait", "acked"}
SQLITE_BUSY_TIMEOUT_MS = 30000


def _validate_source_glob(pattern: str) -> str:
    text = str(pattern or "").strip()
    if not text:
        raise SystemExit("source glob must not be empty")
    if "**" in text or "/" in text or "\\" in text:
        raise SystemExit("source glob must be a direct-child file pattern")
    return text


def _is_allowed_source_file(path: Path) -> bool:
    return path.name.startswith(ALLOWED_SOURCE_PREFIX) and path.suffix.lower() == ALLOWED_SOURCE_SUFFIX


def _content_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _source_state_key(path: Path) -> str:
    return str(path.resolve())


def _source_delta_key(path: Path) -> str:
    return hashlib.sha256(_source_state_key(path).encode("utf-8")).hexdigest()[:16]


def _delta_relative_prefix(path: Path) -> str:
    return f"legacy_csv_deltas/source-{_source_delta_key(path)}/"


def _delta_relative_path(path: Path, start_byte: int, end_byte: int, content_sha256: str) -> str:
    return f"{_delta_relative_prefix(path)}bytes-{start_byte}-{end_byte}-sha256-{content_sha256[:16]}.csv"


def _file_prefix_sha256(path: Path, byte_count: int) -> str:
    digest = hashlib.sha256()
    remaining = max(0, int(byte_count))
    with path.open("rb") as handle:
        while remaining:
            chunk = handle.read(min(1024 * 1024, remaining))
            if not chunk:
                break
            digest.update(chunk)
            remaining -= len(chunk)
    return digest.hexdigest()


def _scan_state_connect(db_path: str | Path) -> sqlite3.Connection:
    target = Path(db_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(target), timeout=SQLITE_BUSY_TIMEOUT_MS / 1000)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS direct_sync_source_scan_state (
            source_file_path TEXT PRIMARY KEY,
            sent_byte_count INTEGER NOT NULL,
            sent_prefix_sha256 TEXT NOT NULL DEFAULT '',
            updated_at_unix REAL NOT NULL
        )
        """
    )
    columns = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(direct_sync_source_scan_state)").fetchall()
    }
    if "sent_prefix_sha256" not in columns:
        conn.execute("ALTER TABLE direct_sync_source_scan_state ADD COLUMN sent_prefix_sha256 TEXT NOT NULL DEFAULT ''")
    conn.commit()
    return conn


def _read_source_scan_state(db_path: str | Path, source_file: Path) -> tuple[int, str]:
    conn = _scan_state_connect(db_path)
    try:
        row = conn.execute(
            "SELECT sent_byte_count, sent_prefix_sha256 FROM direct_sync_source_scan_state WHERE source_file_path = ?",
            (_source_state_key(source_file),),
        ).fetchone()
        explicit_state = (int(row["sent_byte_count"]), str(row["sent_prefix_sha256"] or "")) if row else (0, "")
        queued_state = _read_queued_delta_progress(conn, source_file)
        return queued_state if queued_state[0] > explicit_state[0] else explicit_state
    finally:
        conn.close()


def _parse_delta_range(relative_path: str, source_file: Path) -> tuple[int, int] | None:
    text = str(relative_path or "").replace("\\", "/")
    prefixes = (
        f"{_delta_relative_prefix(source_file)}bytes-",
        f"legacy_csv_deltas/{source_file.name}/bytes-",
    )
    prefix = next((candidate for candidate in prefixes if text.startswith(candidate)), "")
    if not prefix:
        return None
    range_text = text[len(prefix):].split("-sha256-", 1)[0]
    try:
        start_text, end_text = range_text.split("-", 1)
        start_byte = int(start_text)
        end_byte = int(end_text)
    except (TypeError, ValueError):
        return None
    if start_byte < 0 or end_byte <= start_byte:
        return None
    return start_byte, end_byte


def _delta_content_sha256_for_range(source_file: Path, start_byte: int, end_byte: int) -> str | None:
    if source_file.stat().st_size < end_byte:
        return None
    with source_file.open("rb") as handle:
        header = handle.readline()
        data_start = handle.tell()
        if start_byte and start_byte < data_start:
            return None
        handle.seek(start_byte)
        body = handle.read(end_byte - start_byte)
    if len(body) != end_byte - start_byte:
        return None
    delta_content = body if start_byte == 0 else header + body
    return hashlib.sha256(delta_content).hexdigest()


def _read_queued_delta_progress(conn: sqlite3.Connection, source_file: Path) -> tuple[int, str]:
    has_relay_table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'direct_sync_relay_batches'"
    ).fetchone()
    if not has_relay_table:
        return 0, ""
    source_delta_key = _source_delta_key(source_file)
    rows = conn.execute(
        """
        SELECT source_file_path, relative_path, content_sha256, status, receipt_json
        FROM direct_sync_relay_batches
        WHERE relative_path LIKE 'legacy_csv_deltas/%'
        """
    ).fetchall()
    matching_ranges: dict[int, int] = {}
    for row in rows:
        status = str(row["status"] or "")
        if status not in DELTA_PROGRESS_STATUSES and not _operator_review_committed(row):
            continue
        source_path = Path(str(row["source_file_path"] or ""))
        if source_path.parent.name != source_delta_key:
            continue
        parsed_range = _parse_delta_range(str(row["relative_path"] or ""), source_file)
        if parsed_range is None:
            continue
        start_byte, end_byte = parsed_range
        delta_hash = _delta_content_sha256_for_range(source_file, start_byte, end_byte)
        if delta_hash and delta_hash == str(row["content_sha256"] or ""):
            matching_ranges[start_byte] = max(matching_ranges.get(start_byte, 0), end_byte)
    best_end_byte = 0
    while best_end_byte in matching_ranges:
        next_end_byte = matching_ranges[best_end_byte]
        if next_end_byte <= best_end_byte:
            break
        best_end_byte = next_end_byte
    if best_end_byte <= 0:
        return 0, ""
    return best_end_byte, _file_prefix_sha256(source_file, best_end_byte)


def _operator_review_committed(row: sqlite3.Row) -> bool:
    if str(row["status"] or "") != "operator_review":
        return False
    try:
        receipt = json.loads(str(row["receipt_json"] or "{}"))
    except (TypeError, ValueError):
        return False
    return receipt.get("committed") is True and receipt.get("retryable") is False


def _last_result_advances_source_progress(last_result: dict) -> bool:
    status = str(last_result.get("status") or "")
    if status == "acked":
        return True
    return (
        status == "operator_review"
        and last_result.get("committed") is True
        and last_result.get("retryable") is False
    )


def _record_source_sent_byte_count(db_path: str | Path, source_file: Path, sent_byte_count: int) -> None:
    sent_prefix_sha256 = _file_prefix_sha256(source_file, sent_byte_count)
    conn = _scan_state_connect(db_path)
    try:
        conn.execute(
            """
            INSERT INTO direct_sync_source_scan_state (source_file_path, sent_byte_count, sent_prefix_sha256, updated_at_unix)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(source_file_path) DO UPDATE SET
                sent_byte_count = excluded.sent_byte_count,
                sent_prefix_sha256 = excluded.sent_prefix_sha256,
                updated_at_unix = excluded.updated_at_unix
            """,
            (_source_state_key(source_file), int(sent_byte_count), sent_prefix_sha256, time.time()),
        )
        conn.commit()
    finally:
        conn.close()


def _complete_file_byte_count(path: Path) -> int:
    data = path.read_bytes()
    return len(_complete_line_prefix(data))


def _baseline_existing_source_files(
    db_path: str | Path,
    scan_source_dir: str,
    patterns: list[str],
    max_files: int,
    min_file_age_seconds: int = 0,
) -> dict:
    source_files, deferred_count = _scan_source_files(
        scan_source_dir,
        patterns,
        max_files,
        min_file_age_seconds,
    )
    baselined = []
    skipped = []
    limit = max(0, int(max_files or 0))
    for source_file in source_files[:limit]:
        try:
            sent_byte_count = _complete_file_byte_count(source_file)
            if sent_byte_count <= 0:
                skipped.append({"path": str(source_file), "reason": "no_complete_rows"})
                continue
            _record_source_sent_byte_count(db_path, source_file, sent_byte_count)
            baselined.append({"path": str(source_file), "sent_byte_count": sent_byte_count})
        except OSError as exc:
            skipped.append({"path": str(source_file), "reason": exc.__class__.__name__})
    return {
        "status": "baseline_complete",
        "baseline_count": len(baselined),
        "deferred_count": deferred_count,
        "skipped_count": len(skipped),
        "baselined_sources": baselined,
        "skipped_sources": skipped,
    }


def _complete_line_prefix(data: bytes) -> bytes:
    if not data:
        return b""
    if data.endswith((b"\n", b"\r")):
        return data
    last_newline = max(data.rfind(b"\n"), data.rfind(b"\r"))
    if last_newline < 0:
        return b""
    return data[: last_newline + 1]


def _build_delta_source_file(config: DirectSyncRuntimeConfig, source_file: Path) -> tuple[Path, str, int] | None:
    source_size = source_file.stat().st_size
    sent_byte_count, sent_prefix_sha256 = _read_source_scan_state(config.db_path, source_file)
    if sent_byte_count > 0:
        replaced_or_truncated = not sent_prefix_sha256 or source_size < sent_byte_count
        if not replaced_or_truncated:
            replaced_or_truncated = _file_prefix_sha256(source_file, sent_byte_count) != sent_prefix_sha256
        if replaced_or_truncated:
            sent_byte_count = 0
    if source_size <= sent_byte_count:
        return None

    with source_file.open("rb") as handle:
        header = handle.readline()
        if not header:
            return None
        data_start = handle.tell()
        start_byte = sent_byte_count if sent_byte_count >= data_start else 0
        handle.seek(start_byte)
        delta_body = handle.read()

    complete_delta_body = _complete_line_prefix(delta_body)
    if not complete_delta_body.strip():
        return None
    end_byte = start_byte + len(complete_delta_body)
    if start_byte == 0 and end_byte <= data_start:
        return None

    delta_content = complete_delta_body if start_byte == 0 else header + complete_delta_body
    delta_hash = hashlib.sha256(delta_content).hexdigest()
    delta_source = (
        Path(config.spool_dir)
        / "_scan_delta_inputs"
        / _source_delta_key(source_file)
        / f"bytes-{start_byte}-{end_byte}-sha256-{delta_hash[:16]}.csv"
    )
    delta_source.parent.mkdir(parents=True, exist_ok=True)
    delta_source.write_bytes(delta_content)
    return delta_source, _delta_relative_path(source_file, start_byte, end_byte, delta_hash), end_byte


def _scan_source_files(
    scan_source_dir: str,
    patterns: list[str],
    max_files: int,
    min_file_age_seconds: int = 0,
    now: float | None = None,
) -> tuple[list[Path], int]:
    root = Path(scan_source_dir)
    if not root.is_dir():
        raise SystemExit(f"scan source dir does not exist: {root}")
    root_resolved = root.resolve()
    scan_patterns = [_validate_source_glob(pattern) for pattern in (patterns or ["*.csv"])]
    min_age = max(0, int(min_file_age_seconds or 0))
    current_time = time.time() if now is None else float(now)
    seen: set[str] = set()
    files: list[tuple[int, str, Path]] = []
    deferred_count = 0
    for pattern in scan_patterns:
        for path in root.glob(pattern):
            try:
                if path.is_symlink() or not path.is_file():
                    continue
                stat_result = path.stat()
                resolved_path = path.resolve()
            except OSError:
                continue
            if not resolved_path.is_relative_to(root_resolved):
                continue
            if not _is_allowed_source_file(path):
                continue
            resolved = str(resolved_path)
            if resolved in seen:
                continue
            seen.add(resolved)
            if min_age and current_time - stat_result.st_mtime < min_age:
                deferred_count += 1
                continue
            files.append((stat_result.st_mtime_ns, str(path), path))
    files.sort(key=lambda item: (item[0], item[1]))
    return [path for _, _, path in files], deferred_count


def _source_file_still_eligible_for_enqueue(
    path: Path,
    scan_source_dir: str,
    min_file_age_seconds: int,
) -> bool:
    try:
        root_resolved = Path(scan_source_dir).resolve()
        if path.is_symlink() or not path.is_file():
            return False
        stat_result = path.stat()
        resolved_path = path.resolve()
    except OSError:
        return False
    if not resolved_path.is_relative_to(root_resolved):
        return False
    if not _is_allowed_source_file(path):
        return False
    min_age = max(0, int(min_file_age_seconds or 0))
    return min_age == 0 or time.time() - stat_result.st_mtime >= min_age


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


def _write_scan_runtime_error(config: DirectSyncRuntimeConfig, exc: Exception) -> dict:
    status = {
        "status": "runtime_error",
        "error_code": type(exc).__name__,
        "error_message": str(exc),
        "scan_error": True,
    }
    for path_value, payload in [
        (config.runtime_status_path, status),
        (
            config.log_path,
            {
                "event": "scan_runtime_error",
                "status": status["status"],
                "error_code": status["error_code"],
                "error_message": status["error_message"],
            },
        ),
    ]:
        path = Path(path_value)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path_value == config.log_path:
            with path.open("a", encoding="utf-8", newline="\n") as handle:
                handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True))
                handle.write("\n")
        else:
            path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
    return status


def _persist_scan_runtime_status(config: DirectSyncRuntimeConfig, status: dict) -> None:
    status_path = Path(config.runtime_status_path)
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(
        json.dumps(status, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    log_path = Path(config.log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "event": "relay_runner_scan_once",
        "app": "Label_Match",
        "worker_id": config.worker_id,
        "credential_ref": str(config.credential_path),
        "generated_at": utc_now_text(),
    }
    entry.update(status)
    with log_path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False, sort_keys=True))
        handle.write("\n")


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
    parser.add_argument("--min-source-file-age-seconds", type=int, default=0)
    parser.add_argument("--baseline-existing-source-files", action="store_true")
    args = parser.parse_args(argv)
    if args.enqueue_source_file and args.scan_source_dir:
        parser.error("--enqueue-source-file and --scan-source-dir are mutually exclusive")
    if args.baseline_existing_source_files and not args.scan_source_dir:
        parser.error("--baseline-existing-source-files requires --scan-source-dir")

    config = _build_config(args)
    if args.baseline_existing_source_files:
        try:
            status = _baseline_existing_source_files(
                config.db_path,
                args.scan_source_dir,
                args.source_glob,
                args.max_enqueue_files,
                args.min_source_file_age_seconds,
            )
            _persist_scan_runtime_status(config, status)
        except (OSError, sqlite3.DatabaseError, ValueError) as exc:
            status = _write_scan_runtime_error(config, exc)
    elif args.enqueue_source_file:
        status = enqueue_completed_source_file(
            config,
            source_file_path=args.enqueue_source_file,
            relative_path=args.relative_path,
        )
    elif args.scan_source_dir:
        try:
            max_enqueue_files = max(0, int(args.max_enqueue_files or 0))
            statuses = []
            enqueued_count = 0
            attempted_count = 0
            no_new_count = 0
            preflight_status = None
            pending_delta_progress: dict[str, tuple[Path, int]] = {}
            source_files, deferred_count = _scan_source_files(
                args.scan_source_dir,
                args.source_glob,
                args.max_enqueue_files,
                args.min_source_file_age_seconds,
            )
            for source_file in source_files:
                if enqueued_count >= max_enqueue_files:
                    break
                if not _source_file_still_eligible_for_enqueue(
                    source_file,
                    args.scan_source_dir,
                    args.min_source_file_age_seconds,
                ):
                    deferred_count += 1
                    continue
                delta = _build_delta_source_file(config, source_file)
                if delta is None:
                    no_new_count += 1
                    continue
                delta_source_file, relative_path, sent_byte_count = delta
                current = enqueue_completed_source_file(
                    config,
                    source_file_path=delta_source_file,
                    relative_path=relative_path,
                )
                if current["status"] in {"paused_by_operator", "blocked_queue_backpressure", "blocked_disk_pressure"}:
                    preflight_status = current
                    break
                statuses.append(current)
                attempted_count += 1
                if current["status"] == "enqueued":
                    enqueued_count += 1
                    last_result = current.get("last_result") if isinstance(current.get("last_result"), dict) else {}
                    relay_id = str(last_result.get("relay_id") or "")
                    if relay_id:
                        pending_delta_progress[relay_id] = (source_file, sent_byte_count)
                else:
                    current["scan_failed_source_file"] = str(source_file)
                    break
            status = preflight_status or (
                statuses[-1]
                if statuses
                else {
                    "status": (
                        "scan_deferred_sources"
                        if deferred_count
                        else "scan_no_new_rows"
                        if no_new_count
                        else "scan_no_files"
                    )
                }
            )
            status["scan_enqueued_count"] = enqueued_count
            status["scan_attempted_count"] = attempted_count
            status["scan_deferred_count"] = deferred_count
            status["scan_no_new_count"] = no_new_count
            status["scan_status"] = status["status"]
            recovery = (
                status.get("queue_backpressure", {}).get("recovery", {})
                if isinstance(status.get("queue_backpressure"), dict)
                else {}
            )
            age_recovery_already_attempted = (
                status["status"] == "blocked_queue_backpressure"
                and bool(recovery.get("attempted"))
            )
            if not age_recovery_already_attempted and status["status"] not in {
                "paused_by_operator",
                "blocked_disk_pressure",
                "enqueue_error",
                "runtime_error",
            }:
                targeted_drain_results = []
                relay_status = None
                for relay_id in list(pending_delta_progress):
                    current_status = run_relay_once(config, target_relay_id=relay_id)
                    last_result = (
                        current_status.get("last_result")
                        if isinstance(current_status.get("last_result"), dict)
                        else {}
                    )
                    acked_relay_id = str(last_result.get("relay_id") or "")
                    targeted_result = {
                        "target_relay_id": relay_id,
                        "status": current_status.get("status", ""),
                        "acked_relay_id": acked_relay_id,
                        "error_code": last_result.get("error_code", ""),
                    }
                    if _last_result_advances_source_progress(last_result) and acked_relay_id in pending_delta_progress:
                        source_file, sent_byte_count = pending_delta_progress[acked_relay_id]
                        _record_source_sent_byte_count(config.db_path, source_file, sent_byte_count)
                    targeted_drain_results.append(targeted_result)
                    relay_status = current_status
                if relay_status is None:
                    relay_status = run_relay_once(config)
                relay_status["scan_status"] = status["scan_status"]
                relay_status["scan_enqueued_count"] = enqueued_count
                relay_status["scan_attempted_count"] = attempted_count
                relay_status["scan_deferred_count"] = deferred_count
                relay_status["scan_no_new_count"] = no_new_count
                if targeted_drain_results:
                    relay_status["targeted_drain_results"] = targeted_drain_results
                status = relay_status
        except (OSError, sqlite3.DatabaseError, ValueError) as exc:
            status = _write_scan_runtime_error(config, exc)
        if "scan_status" in status:
            _persist_scan_runtime_status(config, status)
    else:
        status = run_relay_once(config)
    print(f"direct_sync_relay_status={status['status']}")
    if "scan_status" in status:
        print(f"direct_sync_scan_status={status['scan_status']}")
    if "scan_enqueued_count" in status:
        print(f"direct_sync_scan_enqueued_count={status['scan_enqueued_count']}")
    if "scan_attempted_count" in status:
        print(f"direct_sync_scan_attempted_count={status['scan_attempted_count']}")
    if "scan_deferred_count" in status:
        print(f"direct_sync_scan_deferred_count={status['scan_deferred_count']}")
    if "scan_no_new_count" in status:
        print(f"direct_sync_scan_no_new_count={status['scan_no_new_count']}")
    if status.get("scan_failed_source_file"):
        print(f"direct_sync_scan_failed_source_file={status['scan_failed_source_file']}")
    targeted_drain_results = status.get("targeted_drain_results") or []
    targeted_ack_incomplete = False
    if targeted_drain_results:
        targeted_ack_count = sum(
            1
            for item in targeted_drain_results
            if item.get("status") == "acked" and item.get("acked_relay_id") == item.get("target_relay_id")
        )
        print(f"direct_sync_targeted_ack_count={targeted_ack_count}")
        print(f"direct_sync_targeted_attempt_count={len(targeted_drain_results)}")
        targeted_ack_incomplete = targeted_ack_count != len(targeted_drain_results)
    if status["status"] in {"blocked_disk_pressure", "blocked_queue_backpressure"} or status.get("scan_status") in {
        "blocked_disk_pressure",
        "blocked_queue_backpressure",
    }:
        return 2
    if status["status"] in {"enqueue_error", "runtime_error"}:
        return 1
    if targeted_ack_incomplete:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

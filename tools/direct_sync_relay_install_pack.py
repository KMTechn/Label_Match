#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Build or apply the Label_Match direct-sync scheduled-task install pack."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Sequence


DEFAULT_TASK_NAME = "direct-sync-relay-label-match"
DEFAULT_PROGRAM_DATA_ROOT = r"C:\ProgramData\KMTech\DirectSync\label_match"


def _quote_cmd(parts: Sequence[str]) -> str:
    return " ".join(shlex.quote(str(part)) for part in parts)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _runtime_paths(program_data_root: str | os.PathLike[str]) -> dict[str, str]:
    root = Path(program_data_root)
    return {
        "db_path": str(root / "queue" / "direct_sync_relay.sqlite3"),
        "spool_dir": str(root / "spool"),
        "upload_status_dir": str(root / "upload_status"),
        "runtime_status_path": str(root / "status" / "direct_sync_relay_status.json"),
        "log_path": str(root / "logs" / "direct_sync_relay.jsonl"),
    }


def _source_scan_config(args: argparse.Namespace) -> dict:
    scan_source_dir = str(getattr(args, "scan_source_dir", "") or "").strip()
    source_globs = [str(item) for item in (getattr(args, "source_glob", []) or [])]
    max_enqueue_files = max(0, int(getattr(args, "max_enqueue_files", 100) or 0))
    return {
        "enabled": bool(scan_source_dir),
        "scan_source_dir": str(Path(scan_source_dir).resolve()) if scan_source_dir else "",
        "source_globs": source_globs,
        "max_enqueue_files": max_enqueue_files,
    }


def _append_source_scan_args(runner_parts: list[str], source_scan: dict) -> None:
    if not source_scan["enabled"]:
        return
    runner_parts.extend(["--scan-source-dir", source_scan["scan_source_dir"]])
    for pattern in source_scan["source_globs"]:
        runner_parts.extend(["--source-glob", pattern])
    runner_parts.extend(["--max-enqueue-files", str(source_scan["max_enqueue_files"])])


def build_install_plan(args: argparse.Namespace) -> dict:
    app_root = Path(args.app_root).resolve()
    python_exe = str(Path(args.python_exe).resolve())
    runner_script = app_root / "tools" / "direct_sync_relay_runner.py"
    paths = _runtime_paths(args.program_data_root)
    source_scan = _source_scan_config(args)
    runner_parts = [
        python_exe,
        str(runner_script),
        "--db-path",
        paths["db_path"],
        "--spool-dir",
        paths["spool_dir"],
        "--producer-manifest-path",
        str(Path(args.producer_manifest_path).resolve()),
        "--credential-path",
        str(Path(args.credential_path).resolve()),
        "--upload-status-dir",
        paths["upload_status_dir"],
        "--runtime-status-path",
        paths["runtime_status_path"],
        "--log-path",
        paths["log_path"],
        "--worker-id",
        args.task_name,
        "--min-free-bytes",
        str(max(0, int(args.min_free_bytes))),
    ]
    _append_source_scan_args(runner_parts, source_scan)
    task_action = _quote_cmd(runner_parts)
    create_command = [
        "schtasks.exe",
        "/Create",
        "/TN",
        args.task_name,
        "/SC",
        "MINUTE",
        "/MO",
        str(max(1, int(args.minute_interval))),
        "/TR",
        task_action,
        "/F",
    ]
    delete_command = ["schtasks.exe", "/Delete", "/TN", args.task_name, "/F"]
    return {
        "report_version": "label-match-direct-sync-install-pack-v1",
        "status": "DRY_RUN" if not args.apply else "APPLY_REQUESTED",
        "apply": bool(args.apply),
        "uninstall": bool(args.uninstall),
        "task_name": args.task_name,
        "program_data_root": str(Path(args.program_data_root)),
        "runtime_paths": paths,
        "source_scan": source_scan,
        "runner_script": str(runner_script),
        "runner_command": runner_parts,
        "scheduled_task_create_command": create_command,
        "scheduled_task_delete_command": delete_command,
        "secret_redaction": {
            "credential_path_only": True,
            "raw_secret_in_report": False,
        },
        "production_apply_guard": {
            "requires_apply": True,
            "requires_confirm_production_install": True,
            "confirm_production_install": bool(args.confirm_production_install),
        },
    }


def _run_command(command: Sequence[str]) -> dict:
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    return {
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Label_Match direct-sync relay scheduled-task install pack")
    parser.add_argument("--app-root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--python-exe", default=sys.executable)
    parser.add_argument("--program-data-root", default=DEFAULT_PROGRAM_DATA_ROOT)
    parser.add_argument("--producer-manifest-path", required=True)
    parser.add_argument("--credential-path", required=True)
    parser.add_argument("--task-name", default=DEFAULT_TASK_NAME)
    parser.add_argument("--minute-interval", type=int, default=1)
    parser.add_argument("--min-free-bytes", type=int, default=512 * 1024 * 1024)
    parser.add_argument("--scan-source-dir", default="")
    parser.add_argument("--source-glob", action="append", default=[])
    parser.add_argument("--max-enqueue-files", type=int, default=100)
    parser.add_argument("--report-path", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--uninstall", action="store_true")
    parser.add_argument("--confirm-production-install", action="store_true")
    args = parser.parse_args(argv)

    plan = build_install_plan(args)
    if args.apply and not args.confirm_production_install:
        plan["status"] = "BLOCKED"
        plan["blocked_reason"] = "apply requires --confirm-production-install"
        _write_json(Path(args.report_path), plan)
        print(f"install_pack_report={Path(args.report_path).resolve()}")
        return 2

    if args.apply:
        command = plan["scheduled_task_delete_command"] if args.uninstall else plan["scheduled_task_create_command"]
        plan["command_result"] = _run_command(command)
        plan["status"] = "PASS" if plan["command_result"]["returncode"] == 0 else "FAIL"

    _write_json(Path(args.report_path), plan)
    print(f"install_pack_report={Path(args.report_path).resolve()}")
    return 0 if plan["status"] in {"DRY_RUN", "PASS"} else 1


if __name__ == "__main__":
    raise SystemExit(main())

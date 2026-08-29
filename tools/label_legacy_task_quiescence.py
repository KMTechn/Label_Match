#!/usr/bin/env python
"""Read-only canonical-only gate for the legacy Label scheduled task."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Any, Callable, Mapping, Sequence


def _configure_import_root(tool_path: Path, search_path: list[str] | None = None) -> Path:
    selected = sys.path if search_path is None else search_path
    app_root = tool_path.resolve().parents[1]
    if str(app_root) not in selected:
        selected.insert(0, str(app_root))
    return app_root


APP_ROOT = _configure_import_root(Path(__file__))

from current_user_scheduled_task import (  # noqa: E402
    LEGACY_TASK_QUIESCENCE_VERSION,
    LEGACY_TASK_REMEDIATION,
    read_legacy_system_task_quiescence,
)
from auth_recovery_canary import write_json_atomic  # noqa: E402


def _write_json_atomic(path: Path, value: Mapping[str, Any]) -> None:
    selected = path.expanduser()
    if not selected.is_absolute():
        raise ValueError("report path must be absolute")
    write_json_atomic(selected, value)


def _validate_report(value: Mapping[str, Any]) -> dict[str, Any]:
    report = dict(value)
    status = report.get("status")
    reason = report.get("reason_code")
    if (
        report.get("schema") != LEGACY_TASK_QUIESCENCE_VERSION
        or status not in {"PASS", "FAIL"}
        or not isinstance(reason, str)
        or not reason
        or report.get("required_state") != "ABSENT_OR_DISABLED"
        or report.get("read_only") is not True
        or report.get("task_or_process_mutated") is not False
    ):
        raise ValueError("legacy scheduled-task report schema is invalid")
    if status == "PASS" and reason not in {
        "LEGACY_TASK_ABSENT",
        "LEGACY_TASK_DISABLED",
    }:
        raise ValueError("legacy scheduled-task PASS reason is invalid")
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Require the root legacy Label task to be absent or Disabled without "
            "changing tasks or processes."
        )
    )
    parser.add_argument("--report-path", required=True)
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    reader: Callable[[], Mapping[str, Any]] = read_legacy_system_task_quiescence,
) -> int:
    args = _parser().parse_args(argv)
    try:
        report = _validate_report(reader())
    except Exception as exc:
        report = {
            "schema": LEGACY_TASK_QUIESCENCE_VERSION,
            "status": "FAIL",
            "reason_code": "LEGACY_TASK_READBACK_FAILED",
            "required_state": "ABSENT_OR_DISABLED",
            "read_only": True,
            "task_or_process_mutated": False,
            "remediation": LEGACY_TASK_REMEDIATION,
            "failure": f"{exc.__class__.__name__}: {exc}"[:500],
        }
    _write_json_atomic(Path(args.report_path), report)
    status = str(report.get("status") or "FAIL")
    reason = str(report.get("reason_code") or "LEGACY_TASK_READBACK_FAILED")
    print(f"{status} {reason}")
    return 0 if status == "PASS" else 4


if __name__ == "__main__":
    raise SystemExit(main())

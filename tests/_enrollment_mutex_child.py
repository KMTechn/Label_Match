"""Separate-process probe for the real Label enrollment entrypoint gate."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import time
import uuid


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from enrollment_mutex import ENROLLMENT_MUTEX_NAME  # noqa: E402
from tools import register_label_match_worker_pc as registration  # noqa: E402


def _runtime_metadata() -> dict:
    return {
        "mutex_name": ENROLLMENT_MUTEX_NAME,
        "runtime_executable": str(Path(sys.executable).resolve()),
        "runtime_frozen": bool(getattr(sys, "frozen", False)),
    }


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _wait_for_start(path: Path, timeout_seconds: float = 20.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while not path.is_file():
        if time.monotonic() >= deadline:
            raise TimeoutError("start boundary was not released")
        time.sleep(0.01)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--start-path", required=True)
    parser.add_argument("--attempt-path", required=True)
    parser.add_argument("--entered-path", required=True)
    parser.add_argument("--result-path", required=True)
    parser.add_argument("--hold-seconds", type=float, default=0.0)
    parser.add_argument("--mutex-timeout-seconds", type=float, default=0.0)
    args = parser.parse_args(argv)

    start_path = Path(args.start_path)
    attempt_path = Path(args.attempt_path)
    entered_path = Path(args.entered_path)
    result_path = Path(args.result_path)
    started = time.monotonic()
    try:
        _wait_for_start(start_path)
        _write_json(
            attempt_path,
            {"label": args.label, "pid": os.getpid(), "status": "ENTERING"},
        )

        child_data_root = result_path.parent / f"{args.label}-registration-data"
        registration_report_path = result_path.with_name(
            f"{args.label}-registration-report.json"
        )

        def local_build_payloads(_args: argparse.Namespace) -> tuple[dict, dict, dict]:
            manifest = {
                "pc_identity": {
                    "pc_id": f"LOCAL-{args.label}",
                    "producer_install_id": f"install-{args.label}",
                    "source_host_id": f"source-{args.label}",
                },
                "paths": {
                    "evidence_dir": str(child_data_root / "evidence"),
                    "rollback_dir": str(child_data_root / "rollback"),
                },
                "sync": {"sync_dir": str(child_data_root / "sync")},
            }
            credential = {
                "producer_id": f"producer-{args.label}",
                "secret_data_dir": str(child_data_root / "secrets"),
            }
            return manifest, credential, {
                "status": "LOCAL_PROBE_PREPARED",
                "manifest_hash": registration.manifest_hash(manifest),
            }

        def local_enrollment_body(
            _args: argparse.Namespace,
            manifest: dict,
            _credential: dict,
            report: dict,
            _progress: object = None,
        ) -> dict:
            _write_json(
                entered_path,
                {"label": args.label, "pid": os.getpid(), "status": "BODY_ENTERED"},
            )
            time.sleep(max(0.0, args.hold_seconds))
            report.update(
                {
                    "status": "SELF_ENROLLMENT_REGISTERED",
                    "body_pid": os.getpid(),
                    "manifest_hash": registration.manifest_hash(manifest),
                }
            )
            return report

        registration.build_payloads = local_build_payloads
        registration._apply_registration_locked = local_enrollment_body
        return_code = registration.main(
            [
                "--apply",
                "--data-dir",
                str(child_data_root),
                "--report-path",
                str(registration_report_path),
                "--enrollment-mutex-timeout-seconds",
                str(args.mutex_timeout_seconds),
            ]
        )
        report = json.loads(registration_report_path.read_text(encoding="utf-8-sig"))
        _write_json(
            result_path,
            {
                **_runtime_metadata(),
                "body_entered": entered_path.is_file(),
                "elapsed_milliseconds": int(
                    round((time.monotonic() - started) * 1000.0)
                ),
                "label": args.label,
                "mutex": report.get("enrollment_mutex"),
                "pid": os.getpid(),
                "registration_return_code": return_code,
                "status": (
                    "PASSED" if return_code == 0 else str(report.get("status") or "FAILED")
                ),
            },
        )
        return int(return_code)
    except Exception as exc:
        _write_json(
            result_path,
            {
                **_runtime_metadata(),
                "body_entered": entered_path.is_file(),
                "elapsed_milliseconds": int(
                    round((time.monotonic() - started) * 1000.0)
                ),
                "error_type": exc.__class__.__name__,
                "label": args.label,
                "mutex": getattr(exc, "mutex_report", None),
                "pid": os.getpid(),
                "reason_code": str(getattr(exc, "reason_code", "")),
                "status": str(getattr(exc, "report_status", "FAILED")),
            },
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

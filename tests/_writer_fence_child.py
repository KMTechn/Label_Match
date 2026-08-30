"""Separate-process probes for the real Label writer fence boundaries."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from user_relay import run_persistent_relay_loop  # noqa: E402
from writer_session_fence import WriterFencedError, writer_admission  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("persistent", "placement"), required=True)
    parser.add_argument("--target", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.mode == "persistent":
            result = run_persistent_relay_loop(
                lambda: args.target.write_text("MUTATED", encoding="utf-8")
                or {"status": "PASS"},
                status_path=args.target.with_suffix(".status.json"),
                interval_seconds=0,
                max_cycles=1,
            )
            denied = result.get("status") == "FENCED"
        else:
            with writer_admission("canonical_placement"):
                args.target.write_text("MUTATED", encoding="utf-8")
            denied = False
    except WriterFencedError as exc:
        print(json.dumps({"status": "DENIED", "reason_code": exc.code}))
        return 4
    print(json.dumps({"status": "DENIED" if denied else "MUTATED"}))
    return 4 if denied else 0


if __name__ == "__main__":
    raise SystemExit(main())

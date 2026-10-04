"""Read-only readiness check for the shared central-logistics PC profile."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from logistics_runtime_profile import default_profile_path, load_logistics_runtime_profile  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate the installed logistics profile.")
    parser.add_argument("--profile-path")
    parser.add_argument("--optional", action="store_true")
    args = parser.parse_args(argv)
    if args.profile_path is None:
        args.profile_path = str(default_profile_path())
    from label_data_generation import DataGenerationError, check_user_data_open, record_user_data_open
    try:
        check_user_data_open(REPO_ROOT, (Path(args.profile_path).absolute().parent,))
        record_user_data_open(REPO_ROOT, ())
    except DataGenerationError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    try:
        profile = load_logistics_runtime_profile(
            required=not args.optional,
            profile_path=args.profile_path,
        )
    except Exception as exc:
        print(f"BLOCKED: {exc.__class__.__name__}: {exc}", file=sys.stderr)
        return 2
    if profile is None:
        print(json.dumps({"status": "optional-not-configured"}, sort_keys=True))
        return 0
    report = profile.redacted_summary()
    report["status"] = "ready"
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Headless product modes; never import the GUI application entry point."""
from __future__ import annotations

from pathlib import Path
import sys

APP_ROOT = Path(__file__).resolve().parent
for path in (APP_ROOT, APP_ROOT / "site-packages"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


def main(argv=None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    from label_match_product_host import PRODUCT_MODES, dispatch_product_mode
    if not arguments or arguments[0] not in PRODUCT_MODES:
        raise ValueError("headless entry requires an explicit product mode")
    if arguments[0] in {"--label-match-user-relay", "--label-match-direct-sync-relay"}:
        from current_user_onboarding import _machine_worker_context
        _machine_worker_context(APP_ROOT)
    result = dispatch_product_mode(arguments)
    if result is None:
        raise ValueError("headless product mode was not dispatched")
    return int(result)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        from failure_redaction import emit_support_failure
        emit_support_failure(error)
        raise SystemExit(1)

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
    from startup_timing import StartupStopRequested
    running = [False]
    running_token = None
    if arguments[0] == "--label-match-user-relay":
        from user_relay import _relay_entry_running
        running_token = _relay_entry_running.set(running)
    try:
        if arguments[0] in {"--label-match-user-relay", "--label-match-direct-sync-relay"}:
            from current_user_onboarding import _machine_worker_context
            _machine_worker_context(APP_ROOT)
        result = dispatch_product_mode(arguments)
        if result is None:
            raise ValueError("headless product mode was not dispatched")
        return int(result)
    except StartupStopRequested:
        return 0
    except Exception as error:
        try:
            if arguments[0] == "--label-match-user-relay" and not getattr(error, "_relay_exit_recorded", False):
                from user_relay import _record_relay_reason
                reason = "RELAY_UNHANDLED_EXCEPTION" if running[0] else "RELAY_EXITED_BEFORE_RUNNING"
                _record_relay_reason(reason, app_root=APP_ROOT, error=error)
        except Exception:
            pass  # Keep native worker refusal and the original abnormal exit.
        raise
    finally:
        if running_token is not None:
            _relay_entry_running.reset(running_token)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        from failure_redaction import emit_support_failure
        emit_support_failure(error)
        raise SystemExit(1)

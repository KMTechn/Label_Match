"""Portable Label_Match entry point for the signed CPython runtime."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys


APP_ROOT = Path(__file__).resolve().parent
if str(APP_ROOT) not in sys.path:
    sys.path.insert(0, str(APP_ROOT))
SITE_PACKAGES = APP_ROOT / "site-packages"
if str(SITE_PACKAGES) not in sys.path:
    sys.path.insert(1, str(SITE_PACKAGES))


def _portable_smoke() -> int:
    if str(os.environ.get("LABEL_MATCH_AUTOMATED_TEST") or "").strip().lower() not in {
        "1",
        "true",
        "yes",
        "on",
        "enabled",
    }:
        raise RuntimeError("portable smoke requires LABEL_MATCH_AUTOMATED_TEST")
    marker_text = str(os.environ.get("LABEL_MATCH_PORTABLE_SMOKE_MARKER") or "").strip()
    if not marker_text:
        raise RuntimeError("portable smoke marker path is required")

    import Label_Match  # noqa: F401

    roots = ("PIL", "pygame", "charset_normalizer", "cryptography", "cffi")
    loaded = sorted(
        name
        for name in sys.modules
        if any(name == root or name.startswith(root + ".") for root in roots)
    )
    payload = {
        "schema": "label-match-portable-smoke-v1",
        "python_executable": sys.executable,
        "python_version": list(sys.version_info[:3]),
        "app_root": str(APP_ROOT),
        "forbidden_modules_loaded": loaded,
    }
    marker = Path(marker_text).expanduser().resolve()
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    return 0


def main() -> int:
    if sys.argv[1:2] == ["--label-match-lifecycle-context"]:
        from auto_update import lifecycle_context_main
        return lifecycle_context_main(sys.argv[2:])
    if "--zero-pe-portable-smoke" in sys.argv[1:]:
        return _portable_smoke()
    if not sys.argv[1:]:
        # Show the worker-account guidance before the existing startup SID gate.
        from current_user_onboarding import CurrentUserOnboardingError, validate_pending_worker_install
        try:
            validate_pending_worker_install(APP_ROOT)
        except CurrentUserOnboardingError as exc:
            from Label_Match import ONBOARDING_EXIT_CODE, _show_first_run_onboarding_error
            exc._startup_check_refusal = True
            _show_first_run_onboarding_error(exc)
            return ONBOARDING_EXIT_CODE
    from auto_update import startup_gate
    from writer_session_fence import WriterFenceError
    try:
        result = startup_gate(APP_ROOT, list(sys.argv[1:]))
    except WriterFenceError as error:
        if error.code != "WRITER_GATE_ABANDONED":
            raise
        from Label_Match import _show_installation_recovery_required
        _show_installation_recovery_required(error)
        return 1
    if result is not None:
        return result
    if sys.argv[1:2] == ["--label-match-direct-sync-relay"]:
        from label_match_product_host import dispatch_product_mode

        return int(dispatch_product_mode(sys.argv[1:]))
    from Label_Match import main as application_main

    return int(application_main())


if __name__ == "__main__":
    raise SystemExit(main())

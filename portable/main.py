"""Portable Label_Match entry point for the signed CPython runtime."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys


APP_ROOT = Path(__file__).resolve().parent
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
    if "--zero-pe-portable-smoke" in sys.argv[1:]:
        return _portable_smoke()
    from Label_Match import main as application_main

    return int(application_main())


if __name__ == "__main__":
    raise SystemExit(main())

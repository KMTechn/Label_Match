"""Actual pinned files required by the isolated PowerShell bootstrap fixtures."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def shared_bootstrap_files(prefix=""):
    return {prefix + name: (ROOT / name).read_bytes() for name in (
        "kmtech_shared.lock.json", "kmtech_shared.manifest.json", "kmtech_shared/powershell/portable.ps1",
    )}

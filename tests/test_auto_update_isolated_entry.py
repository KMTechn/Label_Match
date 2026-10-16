"""Exercise the complete portable app through its isolated script entry."""
import json
from pathlib import Path
import re
import subprocess
import sys

import pytest

from tools.build_portable_release_candidate import _copy_application


@pytest.mark.parametrize("mode", ["task", "relay", "apply"])
def test_isolated_entry_imports(tmp_path, mode):
    packet = tmp_path / "packet"
    app = packet / "app"
    _copy_application(Path(__file__).resolve().parents[1], app)
    if mode == "task":
        args = ["--print-logon-task", "--install-root", str(packet)]
    elif mode == "relay":
        # Parse refusal precedes worker identity, registration and relay launch.
        args = ["--bootstrap", "--role", "relay", "--import-smoke-only"]
    else:
        # A wrong root is rejected before reading or applying prepared state.
        args = ["--apply-prepared", "--control-root", str(packet / "wrong-control")]
    child = subprocess.run(
        [sys.executable, "-I", "-B", str(app / "auto_update.py"), *args],
        capture_output=True, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        timeout=30,
    )
    assert b"ModuleNotFoundError" not in child.stderr
    assert b"ImportError" not in child.stderr
    if mode == "task":
        assert child.returncode == 0
        spec = json.loads(child.stdout)
        assert re.fullmatch(r"KMTech-Label-Match-Startup-Update-[0-9a-f]{16}", spec["name"])
    elif mode == "relay":
        assert child.returncode == 2
        assert b"--import-smoke-only" in child.stderr
    else:
        assert child.returncode == 1
    assert not (packet / "wrong-control").exists()

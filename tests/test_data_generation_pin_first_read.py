"""Actual late credential admission in a separate, host-isolated process."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.mark.parametrize("scenario", ["same", "tls-same", "startup-high", "late-high", "action-late-high", "tls-late-high"])
def test_pin_first_protected_read_rechecks_owner(tmp_path, scenario):
    repo = Path(os.environ.get("W9_PIN_TEST_REPO", Path(__file__).resolve().parents[1]))
    child = Path(__file__).with_name("_data_generation_pin_child.py")
    output = tmp_path / "observation.json"
    process = subprocess.run(
        [sys.executable, "-B", str(child), str(repo), str(tmp_path / "runtime"), scenario, str(output)],
        capture_output=True, text=True, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    assert process.returncode == 0, "child failed; isolated observation retained"
    result = json.loads(output.read_text(encoding="utf-8"))
    assert result["native_calls"] == result["network_calls"] == 0
    assert result["state_unchanged"]
    if scenario in {"same", "tls-same"}:
        assert not result["startup_refused"]
        assert result["pin_client_returned"]
        assert result["events"] == {"locator_reads": 1, "secret_reads": 1, "decrypt_calls": 1}
    else:
        assert result["startup_refused"] == (scenario == "startup-high")
        assert not result["pin_client_returned"]
        assert result["events"]["secret_reads"] == result["events"]["decrypt_calls"] == 0
        assert result["events"]["locator_reads"] == (1 if scenario in {"late-high", "action-late-high"} else 0)
        if scenario in {"late-high", "action-late-high", "tls-late-high"}:
            assert result["error_type"] == "DataGenerationError"

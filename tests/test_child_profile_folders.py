import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.mark.parametrize("program_data_name", [None, "PROGRAMDATA", "ProgramData", "empty", "rebuilt-parent"])
def test_rebuilt_child_environment_keeps_profile_folders(tmp_path, monkeypatch, program_data_name):
    names = ("TEMP", "TMP", "LOCALAPPDATA", "APPDATA", "PROGRAMDATA", "ProgramData")
    expected = {name: os.environ.get(name) for name in names}
    environment = {name: os.environ[name] for name in ("SYSTEMROOT", "PYTHONPATH")
                   if name in os.environ}
    child_environment = environment
    if program_data_name == "rebuilt-parent":
        monkeypatch.setattr(os, "environ", environment)
        child_environment = None
        expected["ProgramData"] = expected["PROGRAMDATA"]
    elif program_data_name == "empty":
        environment.update(TEMP="", TMP="", PROGRAMDATA="", ProgramData="")
        expected["ProgramData"] = expected["PROGRAMDATA"]
    elif program_data_name:
        program_data = str(tmp_path / "child-program")
        environment[program_data_name] = program_data
        expected.update(PROGRAMDATA=program_data, ProgramData=program_data)
    else:
        expected["ProgramData"] = expected["PROGRAMDATA"]
    original = dict(environment)
    script = """
import json, os, sys
expected = json.load(sys.stdin)
checks = {name: bool(value) and os.environ.get(name) == value
          for name, value in expected.items()}
checks["unrelated_configuration_absent"] = (
    "LABEL_MATCH_SAVE_DIR" not in os.environ
    and "KMTECH_LABEL_WRITER_TEST_MODE" not in os.environ)
print(json.dumps(checks))
"""
    child = subprocess.run(
        [sys.executable, "-B", "-c", script], env=child_environment,
        cwd=Path(__file__).resolve().parents[1], input=json.dumps(expected),
        capture_output=True, text=True, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    assert child.returncode == 0, "isolated child failed"
    assert all(json.loads(child.stdout).values()), "child lost an isolated profile folder"
    caller_unchanged = environment == original
    assert caller_unchanged

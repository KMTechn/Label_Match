import ast
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest


def _trace_function(environment, arguments, created):
    source = Path(__file__).resolve().parents[1] / "Label_Match.py"
    tree = ast.parse(source.read_text(encoding="utf-8-sig"))
    function = next(node for node in tree.body
                    if isinstance(node, ast.FunctionDef)
                    and node.name == "_label_match_startup_trace")

    def make_directory(path, *, exist_ok):
        created.append(Path(path))
        os.makedirs(path, exist_ok=exist_ok)

    namespace = {
        "datetime": datetime, "timezone": timezone, "json": json,
        "os": SimpleNamespace(environ=environment, path=os.path,
                              getpid=lambda: 172, getcwd=lambda: "isolated",
                              makedirs=make_directory),
        "sys": SimpleNamespace(argv=arguments, executable="isolated"),
        "_label_match_safe_trace_value": lambda value, **kwargs: value,
    }
    exec(compile(ast.Module(body=[function], type_ignores=[]),
                 str(source), "exec"), namespace)
    return namespace[function.name]


@pytest.mark.parametrize("local_available", [True, False])
def test_non_test_startup_trace_uses_only_user_roots(tmp_path, local_available):
    machine_root = tmp_path / "machine"
    local_root = tmp_path / "local"
    temporary_root = tmp_path / "temporary"
    environment = {
        "ProgramData": str(machine_root),
        "LOCALAPPDATA": str(local_root) if local_available else "",
        "TEMP": str(temporary_root),
    }
    created = []
    trace = _trace_function(environment, ["Label_Match.py"], created)

    trace("regression")

    expected = ([local_root / "KMTech" / "startup-trace"]
                if local_available else [])
    expected.append(temporary_root / "KMTech-startup-trace")
    assert created == expected
    assert not machine_root.exists()
    assert not (machine_root / "KMTech").exists()
    assert all(len(list(root.glob("Label_Match-startup-*.log"))) == 1
               for root in expected)


def test_automated_startup_trace_keeps_temporary_only_branch(tmp_path):
    machine_root = tmp_path / "machine"
    local_root = tmp_path / "local"
    temporary_root = tmp_path / "temporary"
    environment = {
        "ProgramData": str(machine_root),
        "LOCALAPPDATA": str(local_root),
        "TEMP": str(temporary_root),
        "PYTEST_CURRENT_TEST": "isolated",
    }
    created = []
    trace = _trace_function(environment, ["Label_Match.py"], created)

    trace("regression")

    assert created == [temporary_root / "KMTech-startup-trace"]
    assert not machine_root.exists()
    assert not local_root.exists()

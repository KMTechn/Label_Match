from __future__ import annotations

import importlib.util
from pathlib import Path
import sys

import pytest


MODULE_PATH = Path(__file__).resolve().parents[1] / "tools" / "build_embedded_python_library.py"
SPEC = importlib.util.spec_from_file_location("build_embedded_python_library_for_tests", MODULE_PATH)
assert SPEC and SPEC.loader
builder = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = builder
SPEC.loader.exec_module(builder)


def _analysis_toc(path: Path, module_rows: list[tuple[str, str, str]]) -> None:
    payload = tuple([None] * 14 + [module_rows])
    path.write_text(repr(payload), encoding="utf-8")


def test_materializes_pyinstaller_pure_modules_inside_existing_runtime(tmp_path):
    source = tmp_path / "source" / "package" / "feature.py"
    source.parent.mkdir(parents=True)
    source.write_text("VALUE = 7\n", encoding="utf-8")
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    toc = tmp_path / "Analysis-00.toc"
    _analysis_toc(
        toc,
        [
            ("namespace_package", "-", "PYMODULE"),
            ("package.feature", str(source), "PYMODULE"),
        ],
    )

    count = builder.build_embedded_python_library(toc, runtime)

    assert count == 2
    assert (runtime / "namespace_package/__init__.py").read_bytes() == b""
    assert (runtime / "package/feature.py").read_text(encoding="utf-8") == "VALUE = 7\n"


def test_refuses_to_overwrite_an_existing_runtime_member(tmp_path):
    source = tmp_path / "module.py"
    source.write_text("VALUE = 1\n", encoding="utf-8")
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    (runtime / "module.py").write_text("existing\n", encoding="utf-8")
    toc = tmp_path / "Analysis-00.toc"
    _analysis_toc(toc, [("module", str(source), "PYMODULE")])

    with pytest.raises(builder.EmbeddedLibraryBuildError, match="refusing to overwrite"):
        builder.build_embedded_python_library(toc, runtime)


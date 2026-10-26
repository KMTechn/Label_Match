"""Compare the same isolated contracts against an explicit committed source."""
from pathlib import Path
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[2]


def pytest_addoption(parser):
    parser.addoption('--source-ref', default=None)


@pytest.fixture(autouse=True)
def committed_source(request, monkeypatch):
    ref = request.config.getoption('--source-ref')
    if not ref:
        return
    original = Path.read_text
    cache = {}

    def read_text(path, *args, **kwargs):
        try:
            relative = path.resolve().relative_to(ROOT).as_posix()
        except ValueError:
            return original(path, *args, **kwargs)
        if relative.startswith('tests/') or not relative.endswith(('.py', '.ps1')):
            return original(path, *args, **kwargs)
        if relative not in cache:
            result = subprocess.run(['git', 'show', ref + ':' + relative], cwd=ROOT,
                                    capture_output=True, check=True,
                                    creationflags=subprocess.CREATE_NO_WINDOW)
            cache[relative] = result.stdout.decode('utf-8-sig')
        return cache[relative]

    monkeypatch.setattr(Path, 'read_text', read_text)

"""Real query child: compiler preparation and the native query keep separate budgets."""
import json
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('engine', ['powershell.exe', 'pwsh.exe'])
@pytest.mark.parametrize('scenario,preparation,query,expected', [
    ('slow', 11, 0, 'RETURNED'),
    ('preparation-timeout', 21, 0, 'STARTUP_QUERY_TIMEOUT_UNKNOWN'),
    ('query-timeout', 0, 11, 'STARTUP_QUERY_TIMEOUT_UNKNOWN'),
    ('missing-ready', 0, 0, 'STARTUP_QUERY_INCOMPLETE_UNKNOWN'),
])
def test_actual_query_child_preparation_and_query_budgets(tmp_path, engine, scenario, preparation, query, expected):
    command = [shutil.which(engine), '-NoLogo', '-NoProfile', '-NonInteractive',
        '-WindowStyle', 'Hidden', '-ExecutionPolicy', 'Bypass', '-File',
        str(ROOT / 'tests/lm_startup_query_preparation.ps1'), '-HostRoot', str(ROOT),
        '-FixtureRoot', str(tmp_path), '-PreparationSeconds', str(preparation),
        '-QuerySeconds', str(query)]
    if scenario == 'missing-ready':
        command.append('-ExitBeforeReady')
    child = subprocess.run(command, capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    (tmp_path / 'query.stdout').write_bytes(child.stdout)
    (tmp_path / 'query.stderr').write_bytes(child.stderr)
    assert child.returncode == 0
    assert not child.stderr
    row = json.loads(child.stdout)
    assert row['status'] == expected
    assert row['declarations'] == (0 if scenario == 'missing-ready' else 1)
    if scenario == 'slow':
        assert row['empty'] and row['seconds'] >= 11
    elif scenario == 'preparation-timeout':
        assert row['seconds'] >= 20

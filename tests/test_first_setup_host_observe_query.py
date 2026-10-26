"""Exercise the host harness's generated Observe without a packet or installation."""
import ast
import json
from pathlib import Path
import shutil
import subprocess

import pytest

import auto_update
from tests import _first_setup_host as host
from tests._bounded_powershell import run as run_bounded

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('engine', ['powershell.exe', 'pwsh.exe'])
@pytest.mark.parametrize('generator', ['pre-fix', 'current'])
def test_host_observe_generated_query_supplies_its_dependencies(tmp_path, engine, generator):
    make_script = host.observe_script
    if generator == 'pre-fix':
        raw = subprocess.check_output(['git', 'show', '7cc2e594:tests/_first_setup_host.py'],
            cwd=ROOT, creationflags=subprocess.CREATE_NO_WINDOW, timeout=auto_update.APPLY_PROGRESS_TIMEOUT)
        tree = ast.parse(raw.decode('utf-8-sig'))
        function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'observe_script')
        scope = vars(host).copy()
        exec(compile(ast.Module(body=[function], type_ignores=[]), 'historical-host-observe', 'exec'), scope)
        make_script = scope['observe_script']
    processes = tmp_path / 'processes.json'
    processes.write_text('[]', encoding='utf-8')
    driver = tmp_path / 'observe.ps1'
    driver.write_text(make_script(ROOT / 'tools/apply_prepared_update.ps1', tmp_path / 'current', processes),
                      encoding='utf-8-sig')
    result = run_bounded([shutil.which(engine), '-NoLogo', '-NoProfile', '-NonInteractive',
        '-WindowStyle', 'Hidden', '-File', str(driver)], capture_output=True,
        creationflags=subprocess.CREATE_NO_WINDOW, timeout=auto_update.APPLY_PROGRESS_TIMEOUT)
    assert result.owned_descendants_after == 0
    if generator == 'pre-fix':
        assert result.returncode != 0
        assert b'Initialize-LabelStartupUpdateProcessToken' in result.stderr
    else:
        assert result.returncode == 0
        assert json.loads(result.stdout) == []

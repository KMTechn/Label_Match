import json
from pathlib import Path
import shutil
import subprocess

import pytest


@pytest.mark.parametrize('engine', ['powershell.exe', 'pwsh.exe'])
def test_operating_standalone_uninstall_defines_real_emergency_removal_and_publishes_removed(tmp_path, engine):
    executable = shutil.which(engine)
    if not executable:
        pytest.fail('The required PowerShell interpreter is unavailable')
    helper = Path(__file__).with_name('lm_standalone_emergency_closure.ps1')
    result = subprocess.run([executable, '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass',
                             '-File', str(helper), '-Root', str(tmp_path)],
                            capture_output=True, text=True,
                            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    assert result.returncode == 0
    marker = json.loads((tmp_path / 'application/removed.json').read_text(encoding='utf-8-sig'))
    assert marker['lifecycle_state'] == 'REMOVED'
    assert marker['business_data_and_registration_preserved'] is True
    assert not (tmp_path / 'application/관리용/포장 옛 화면 (비상).lnk').exists()
    assert 'uninstall_status=PASS_CODE_REMOVED_STATE_PRESERVED' in result.stdout

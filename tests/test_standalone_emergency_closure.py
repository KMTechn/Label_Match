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


@pytest.mark.parametrize('engine', ['powershell.exe', 'pwsh.exe'])
@pytest.mark.parametrize('fault', ['link', 'directory'])
def test_emergency_refusal_restores_real_link_and_returns_without_removed_marker(tmp_path, engine, fault):
    executable=shutil.which(engine)
    if not executable:
        pytest.fail('The required PowerShell interpreter is unavailable')
    result=subprocess.run([executable,'-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass',
        '-File',str(Path(__file__).with_name('lm_standalone_emergency_closure.ps1')),
        '-Root',str(tmp_path),'-EmergencyFault',fault],capture_output=True,text=True,
        creationflags=subprocess.CREATE_NO_WINDOW)
    assert result.returncode != 0
    assert not (tmp_path/'application/removed.json').exists()
    link=tmp_path/'application/관리용/포장 옛 화면 (비상).lnk'
    assert link.exists() and link.parent.exists()
    assert link.read_bytes() == (tmp_path/'emergency-preimage.bin').read_bytes()
    assert 'uninstall_status=PASS_CODE_REMOVED_STATE_PRESERVED' not in result.stdout

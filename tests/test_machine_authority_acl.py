import json
from pathlib import Path
import shutil
import subprocess

import pytest
import writer_session_fence as fence
from tests._machine_acl_native import bind_native_acl_probe

ROOT=Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('engine',['powershell.exe','pwsh.exe'])
@pytest.mark.parametrize('case',['default','programdata-owner-class','shared-owner-class','root-owner-class',
    'shared-delete','shared-delete-child','shared-write-dac','shared-write-owner',
    'root-write','leaf-write','root-inheritance'])
def test_native_machine_fence_acl_tiers_preserve_code_owner_rule(tmp_path,monkeypatch,engine,case):
    result=subprocess.run([shutil.which(engine),'-NoLogo','-NoProfile','-NonInteractive','-WindowStyle','Hidden',
        '-File',str(ROOT/'tests/lm_machine_acl_probe.ps1'),'-Repo',str(ROOT),'-Root',str(tmp_path),'-Case',case],
        capture_output=True,creationflags=subprocess.CREATE_NO_WINDOW)
    assert result.returncode==0
    state=json.loads((tmp_path/'acl-fixture.json').read_bytes())
    expected=case=='default'
    assert state['fixture_rule_match'] and state['accepted'] is expected and state['default_denied']
    program_data=Path(state['program_data'])
    seen=bind_native_acl_probe(program_data,Path(state['anchor']),monkeypatch,state['untrusted_owner_path'])
    monkeypatch.setattr(fence,'_installer_program_data_root',lambda:program_data)
    if expected:
        fence._assert_protected_owner_path(Path(state['leaf']),machine_fence_root=Path(state['root']))
    else:
        with pytest.raises(OSError):
            fence._assert_protected_owner_path(Path(state['leaf']),machine_fence_root=Path(state['root']))
    assert str(program_data).lower() in seen
    with pytest.raises(OSError):
        fence._assert_protected_owner_path(Path(state['leaf']))

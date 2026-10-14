from pathlib import Path
import shutil
import subprocess

import pytest
from tests import test_placement_helper_scope as existing_scope


@pytest.mark.parametrize('engine',['powershell.exe','pwsh.exe'])
@pytest.mark.parametrize('outer',[False,True])
@pytest.mark.parametrize('state,fault',[('REMOVED','none'),('INSTALLED','none'),
    ('INSTALLED','query_throw'),('INSTALLED','query_exit'),('INSTALLED','query_json'),
    ('INSTALLED','activation_throw')])
def test_memory_placement_query_and_activation_release_the_reacquired_local_lease(tmp_path,engine,outer,state,fault):
    executable=shutil.which(engine)
    if not executable:
        pytest.fail('Required PowerShell interpreter unavailable')
    command=[executable,'-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',
        str(Path(__file__).with_name('lm_placement_finally_scope.ps1')),'-Root',str(tmp_path),
        '-State',state,'-Fault',fault]
    if outer:
        command.append('-OuterLease')
    result=subprocess.run(command,capture_output=True,creationflags=subprocess.CREATE_NO_WINDOW)
    assert result.returncode==0
    assert result.stdout.strip()==b'PASS'


@pytest.mark.parametrize('name',['test_public_memory_helper_observes_removed_state_in_its_own_scope',
                               'test_memory_helper_rejects_missing_admission_before_child'])
def test_required_scope_checks_fail_when_the_interpreter_is_missing(tmp_path,monkeypatch,name):
    monkeypatch.setattr(shutil,'which',lambda _:None)
    function=getattr(existing_scope,name)
    arguments={key:(tmp_path if key=='tmp_path' else False) for key in
               function.__code__.co_varnames[:function.__code__.co_argcount]}
    with pytest.raises(pytest.fail.Exception):
        function(**arguments)


@pytest.mark.parametrize('engine',['powershell.exe','pwsh.exe'])
def test_same_memory_launcher_exposes_parent_query_leak_and_releases_fixed_query(tmp_path,engine):
    executable=shutil.which(engine)
    if not executable:
        pytest.fail('Required PowerShell interpreter unavailable')
    root=Path(__file__).resolve().parents[1]
    parent=tmp_path/'parent-installer.ps1'
    parent.write_bytes(subprocess.check_output(['git','-C',str(root),'show',
        '1c242b555a6283f317e30f96d7ea1c22e9047d4f:INSTALL_THIS_PC.ps1']))
    command=[executable,'-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',
        str(Path(__file__).with_name('lm_placement_finally_scope.ps1')),'-Fault','query_throw']
    before=subprocess.run([*command,'-Root',str(tmp_path/'before'),'-SourcePath',str(parent)],
                          capture_output=True,creationflags=subprocess.CREATE_NO_WINDOW)
    after=subprocess.run([*command,'-Root',str(tmp_path/'after')],
                         capture_output=True,creationflags=subprocess.CREATE_NO_WINDOW)
    assert before.returncode != 0
    assert after.returncode == 0
    assert after.stdout.strip()==b'PASS'

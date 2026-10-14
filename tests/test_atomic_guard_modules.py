from pathlib import Path
import shutil
import subprocess

import pytest

ROOT=Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('relative', ['product.psm1', 'kmtech_shared/product.psm1', 'kmtech_shared/product.ps1'])
def test_whole_guard_rejects_new_module_or_shared_folder_publication(tmp_path, relative):
    files=subprocess.check_output(['git', '-C', str(ROOT), 'ls-files', '--', '*.ps1', '*.psm1'], text=True).splitlines()
    for name in files:
        destination=tmp_path/name
        destination.parent.mkdir(parents=True,exist_ok=True)
        destination.write_bytes((ROOT/name).read_bytes())
    fixtures=tmp_path/'tests/fixtures'
    fixtures.mkdir(parents=True,exist_ok=True)
    name='lm_atomic_directory_sites.json'
    (fixtures/name).write_bytes((ROOT/'tests/fixtures'/name).read_bytes())
    subprocess.run(['git','init','-q',str(tmp_path)],check=True,capture_output=True)
    subprocess.run(['git','-C',str(tmp_path),'add','--','.'],check=True,capture_output=True)
    engine=shutil.which('powershell.exe')
    if not engine:
        pytest.fail('Required PowerShell interpreter unavailable')
    command=[engine,'-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',
             str(ROOT/'tests/lm_atomic_guard.ps1'),'-HostRoot',str(tmp_path)]
    baseline=subprocess.run(command,capture_output=True,text=True,creationflags=subprocess.CREATE_NO_WINDOW)
    assert baseline.returncode == 0
    candidate=tmp_path/relative
    candidate.parent.mkdir(parents=True,exist_ok=True)
    candidate.write_text("Microsoft.PowerShell.Management\\Rename-Item -LiteralPath 'temporary' -NewName 'live'",encoding='utf-8')
    subprocess.run(['git','-C',str(tmp_path),'add','--',relative],check=True,capture_output=True)
    result=subprocess.run(command,capture_output=True,text=True,
        creationflags=subprocess.CREATE_NO_WINDOW)
    assert result.returncode != 0

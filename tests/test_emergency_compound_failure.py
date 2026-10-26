from pathlib import Path
import shutil
import subprocess

import pytest

ROOT=Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('engine',['powershell.exe','pwsh.exe'])
@pytest.mark.parametrize('later',['worker','core'])
def test_real_emergency_unlink_error_prevents_later_stage_failure(tmp_path,engine,later):
    executable=shutil.which(engine)
    if not executable:
        pytest.fail('Required PowerShell interpreter unavailable')
    result=subprocess.run([executable,'-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass',
        '-File',str(ROOT/'tests/lm_emergency_removal_failure.ps1'),'-HostRoot',str(ROOT),
        '-FixtureRoot',str(tmp_path),'-NextFailure',later],capture_output=True,
        creationflags=subprocess.CREATE_NO_WINDOW)
    assert result.returncode==0
    assert not result.stderr

from pathlib import Path
import shutil
import subprocess
import pytest

ROOT=Path(__file__).resolve().parents[1]

@pytest.mark.parametrize('engine',['powershell.exe','pwsh.exe'])
@pytest.mark.parametrize('scenario',['ready','original','no_signal','stale','wrong_pid','wrong_sha'])
def test_rollback_requires_fresh_cycle_from_exact_old_relay(tmp_path,engine,scenario):
    executable=shutil.which(engine)
    assert executable is not None
    child=subprocess.run([executable,'-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',
        str(ROOT/'tests/lm_rollback_ready.ps1'),'-Repo',str(ROOT),'-Root',str(tmp_path),'-Scenario',scenario],
        capture_output=True,text=True,creationflags=subprocess.CREATE_NO_WINDOW)
    assert child.returncode == 0 and child.stdout.strip() == 'PASS'

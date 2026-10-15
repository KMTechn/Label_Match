from pathlib import Path
import shutil
import subprocess
import pytest

ROOT=Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("engine",["powershell.exe","pwsh.exe"])
@pytest.mark.parametrize("placement",["REUSED_VERIFIED","INSTALL_REQUIRED"])
@pytest.mark.parametrize("failure",["success","undeclared","absent"])
def test_ordinary_installer_stops_preimage_after_admission_and_requires_ready(tmp_path,engine,placement,failure):
    executable=shutil.which(engine)
    assert executable is not None
    child=subprocess.run([executable,"-NoLogo","-NoProfile","-NonInteractive","-File",
        str(ROOT/"tests/lm_installer_relay_stop.ps1"),"-Repo",str(ROOT),"-Root",str(tmp_path),
        "-Placement",placement,"-Failure",failure],capture_output=True,text=True,
        creationflags=subprocess.CREATE_NO_WINDOW)
    assert child.returncode==0 and child.stdout.splitlines()[-1]=="PASS"

from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('engine', ['powershell.exe', 'pwsh.exe'])
@pytest.mark.parametrize('source', ['INSTALL_THIS_PC.ps1', 'INSTALL_CANONICAL_PORTABLE.ps1', 'tools/label_worker_shortcuts.ps1'])
@pytest.mark.parametrize('scenario', ['partial', 'locked', 'refused5', 'success', 'absent'])
def test_atomic_copy_preserves_live_and_preimage(tmp_path, engine, source, scenario):
    executable = shutil.which(engine)
    assert executable is not None
    child = subprocess.run([executable, '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File',
                            str(ROOT/'tests/lm_atomic_copy_publication.ps1'), '-SourceFile', str(ROOT/source),
                            '-Root', str(tmp_path), '-Scenario', scenario], capture_output=True, text=True,
                           creationflags=subprocess.CREATE_NO_WINDOW)
    assert child.returncode == 0
    assert child.stdout.strip() == 'PASS'


@pytest.mark.parametrize('engine', ['powershell.exe', 'pwsh.exe'])
@pytest.mark.parametrize('scenario', ['readback', 'foreign', 'state_failure'])
def test_shortcut_audit_and_state_failure(tmp_path, engine, scenario):
    executable = shutil.which(engine)
    assert executable is not None
    child = subprocess.run([executable, '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File',
                            str(ROOT/'tests/lm_shortcut_publication.ps1'), '-Repo', str(ROOT),
                            '-Root', str(tmp_path), '-Scenario', scenario], capture_output=True, text=True,
                           creationflags=subprocess.CREATE_NO_WINDOW)
    assert child.returncode == 0
    assert child.stdout.strip() == 'PASS'

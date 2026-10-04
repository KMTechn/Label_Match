"""Definition-only PS admission and real resolver-child path identity."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

SHELLS = [name for name in ('powershell.exe', 'pwsh.exe') if shutil.which(name)]


def _run(tmp_path, shell, field, state, spelling):
    repo = Path(__file__).resolve().parents[1]
    root = tmp_path / 'isolated-user'
    app = root / 'packet/app'
    app.mkdir(parents=True)
    for source in repo.glob('*.py'):
        shutil.copy2(source, app / source.name)
    for source in repo.iterdir():
        if source.is_dir() and (source / '__init__.py').is_file():
            shutil.copytree(source, app / source.name, ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
    output = tmp_path / 'result.json'
    completed = subprocess.run([shell, '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass',
        '-File', str(repo / 'tests/_data_generation_installer_child.ps1'), '-Repo', str(repo),
        '-Root', str(root), '-Python', sys.executable, '-Field', field, '-State', state,
        '-Spelling', spelling, '-Output', str(output)], capture_output=True,
        env=os.environ.copy(), creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    if completed.returncode:
        (tmp_path / 'process-stderr.log').write_bytes(completed.stderr)
    assert completed.returncode == 0, 'definition-only installer child failed; inspect preserved logs'
    return json.loads(output.read_text(encoding='utf-8-sig'))


@pytest.mark.parametrize('shell', SHELLS)
@pytest.mark.parametrize('field', ['settings', 'profile', 'direct', 'legacy-direct', 'data', 'custom', 'default', 'default-profile'])
@pytest.mark.parametrize('state', ['high', 'corrupt'])
def test_installer_admits_known_inputs_before_settings_or_lifecycle(tmp_path, shell, field, state):
    result = _run(tmp_path, shell, field, state, 'absolute')
    assert result['refused'] and result['guidance'], result
    assert result['settings_reads'] == (1 if field == 'custom' else 0), result
    assert result['changed'] == result['added'] == result['lifecycle_calls'] == 0, result


@pytest.mark.parametrize('shell', SHELLS)
@pytest.mark.parametrize('field', ['settings', 'profile', 'direct', 'legacy-direct', 'data', 'custom', 'default'])
@pytest.mark.parametrize('state', ['same', 'absent'])
@pytest.mark.parametrize('spelling', ['absolute', 'tilde', 'relative'])
def test_installer_and_real_child_use_same_selected_paths(tmp_path, shell, field, state, spelling):
    result = _run(tmp_path, shell, field, state, spelling)
    assert not result['refused'], result
    assert result['settings_reads'] == 1, result
    assert result['parent_child_same_paths'] and result['child_cwd_matches'], result
    assert result['changed'] == result['added'] == result['lifecycle_calls'] == 0, result

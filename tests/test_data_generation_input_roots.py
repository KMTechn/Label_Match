"""Each selected external input is independently admitted before its first read."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


def _probe(tmp_path, kind, state, spelling='absolute'):
    repo = Path(__file__).resolve().parents[1]
    output = tmp_path / 'result.json'
    completed = subprocess.run([sys.executable, '-B', str(repo / 'tests/_data_generation_input_child.py'),
        str(repo), str(tmp_path / 'isolated-user'), kind, state, spelling, str(output)],
        env=os.environ.copy(), capture_output=True, creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    assert completed.returncode == 0, 'isolated probe process failed; inspect preserved output'
    return json.loads(output.read_text(encoding='utf-8'))


@pytest.mark.parametrize('kind', ['settings', 'spool', 'stop', 'reconcile-stop', 'recovery-db', 'recovery-key'])
@pytest.mark.parametrize('state', ['high', 'corrupt'])
@pytest.mark.parametrize('spelling', ['absolute', 'tilde', 'relative'])
def test_one_external_input_refuses_before_read_or_state_change(tmp_path, kind, state, spelling):
    result = _probe(tmp_path, kind, state, spelling)
    assert result['control_marker_absent_before']
    assert result['refused_before_read'], result


@pytest.mark.parametrize('kind', ['settings', 'spool', 'stop', 'reconcile-stop', 'recovery-db', 'recovery-key'])
@pytest.mark.parametrize('state', ['same', 'absent'])
def test_allowed_external_input_reaches_existing_reader(tmp_path, kind, state):
    result = _probe(tmp_path, kind, state)
    assert not result['guidance'], result
    assert result['input_reads'] > 0 or result['sqlite_opens'] > 0, result
    if kind == 'spool':
        assert result['exit'] == 0, result


@pytest.mark.parametrize('entry,state', [
    ('registrar', 'external-tls'), ('registrar', 'external-enrollment'),
    ('install-pack', 'external-tls'), ('install-pack', 'external-enrollment'),
    ('install-pack', 'external-task-password'), ('raw', 'external-tls'),
    ('canary', 'external-tls'), ('install-profile', 'external-tls'),
])
def test_additional_configuration_input_is_checked_without_reading_or_writing(tmp_path, entry, state):
    from test_data_generation_direct_entries import test_direct_process_refuses_without_identity_or_lifecycle_changes
    test_direct_process_refuses_without_identity_or_lifecycle_changes(entry, state, tmp_path)


@pytest.mark.parametrize('kind', ['recovery-empty-db', 'recovery-empty-key', 'recovery-empty-barrier'])
def test_hidden_child_unresolvable_input_refuses_before_initializing_database(tmp_path, kind):
    result = _probe(tmp_path, kind, 'absent')
    assert result['refused_before_read'], result

"""External security inputs must be admitted before any persistent state change."""
import json
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.mark.parametrize('kind', (
    'onboarding-valid-ca', 'onboarding-default-ca',
    'admin-profile', 'admin-profile-default', 'logistics-ca',
))
@pytest.mark.parametrize('state', ('high', 'unknown', 'same', 'absent'))
def test_security_input_parent_is_checked_before_the_existing_reader(tmp_path, kind, state):
    repo = Path(__file__).resolve().parents[1]
    output = tmp_path / 'observation.json'
    completed = subprocess.run([
        sys.executable, '-B', str(repo / 'tests/_data_generation_security_child.py'),
        str(repo), str(tmp_path / 'input'), kind, state, str(output),
    ], capture_output=True, text=True, check=False,
       creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    assert completed.returncode == 0
    result = json.loads(output.read_text(encoding='utf-8'))
    assert result['native_calls'] == result['network_calls'] == 0
    assert result['changed_original_files'] == []
    if state in ('high', 'unknown'):
        assert result['exit'] != 0
        assert result['ca_reads'] == 0
        assert result['added_file_count'] == result['generation_records_added'] == 0
        assert result.get('profile_accepted', False) is False
    elif kind.startswith('admin-profile'):
        assert result['exit'] == 0
        assert result['profile_accepted'] is True
        assert result['ca_reads'] == 1
        assert result['added_file_count'] == 0
    elif kind == 'logistics-ca':
        assert result['exit'] == 0
        assert result['selected_path_matches'] is True
        assert result['added_file_count'] == 0
    else:
        assert result['ca_reads'] == 1
        assert result['generation_records_added'] > 0

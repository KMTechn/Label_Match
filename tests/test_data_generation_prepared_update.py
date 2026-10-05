"""Admission at authenticated update profile and TLS reads after startup."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.mark.parametrize('state', ['same', 'late-profile', 'late-high', 'late-tls'])
def test_prepared_update_admits_current_profile_and_tls_before_read(tmp_path, state):
    repo = Path(os.environ.get('W9_PIN_TEST_REPO', Path(__file__).resolve().parents[1]))
    output = tmp_path / 'observation.json'
    child = Path(__file__).with_name('_data_generation_prepared_update_child.py')
    process = subprocess.run([sys.executable, '-B', str(child), str(repo),
                              str(tmp_path/'runtime'), 'prepared-update', state, str(output)],
                             capture_output=True, text=True,
                             creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    assert process.returncode == 0, 'isolated child failed; observation retained'
    result = json.loads(output.read_text(encoding='utf-8'))
    events = result['events']
    assert result['native_calls'] == 0
    if state == 'same':
        assert result['returned']
        assert events['profile_reads'] == events['secret_reads'] == events['decrypt_calls'] == 1
        assert events['tls_reads'] == events['connections'] == 2
    else:
        assert not result['returned']
        assert result['error_type'] == 'DataGenerationError'
        assert events['tls_reads'] == events['connections'] == events['mutations'] == 0
        if state == 'late-profile':
            assert events['profile_reads'] == events['secret_reads'] == events['decrypt_calls'] == 0
        elif state == 'late-high':
            assert events['profile_reads'] == 1
            assert events['secret_reads'] == events['decrypt_calls'] == 0
        else:
            assert events['profile_reads'] == events['secret_reads'] == events['decrypt_calls'] == 1
    assert result['state_unchanged']

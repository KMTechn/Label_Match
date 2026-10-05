"""Same-generation and late-high controls for each delayed reader."""
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.mark.parametrize('consumer', ['package', 'lease', 'restore', 'upload', 'catalog',
                                     'update', 'profile', 'relay', 'enqueue'])
@pytest.mark.parametrize('state', ['same', 'late-high'])
def test_delayed_reader_checks_current_owner_before_payload(tmp_path, consumer, state):
    repo = Path(os.environ.get('W9_PIN_TEST_REPO', Path(__file__).resolve().parents[1]))
    child = Path(__file__).with_name('_data_generation_late_reader_child.py')
    output = tmp_path / 'observation.json'
    process = subprocess.run([sys.executable, '-B', str(child), str(repo), str(tmp_path/'runtime'),
                              consumer, state, str(output)], capture_output=True, text=True,
                             creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    assert process.returncode == 0, 'child failed; isolated observation retained'
    result = json.loads(output.read_text(encoding='utf-8'))
    assert result['native_calls'] == 0
    if state == 'late-high':
        assert result['events']['secret_reads'] == result['events']['decrypt_calls'] == 0
        assert result['events']['tls_reads'] == result['events']['connections'] == 0
        assert result['events']['mutations'] == 0
        assert result['state_unchanged']
    elif consumer in {'relay', 'enqueue'}:
        assert result['events']['locator_reads'] == result['events']['secret_reads'] == 1
        assert result['events']['decrypt_calls'] == 1
        assert result['events']['mutations'] > 0
    elif consumer == 'profile':
        assert result['events']['secret_reads'] == result['events']['decrypt_calls'] == 1
    else:
        assert result['events']['tls_reads'] > 0
        assert result['events']['connections'] > 0

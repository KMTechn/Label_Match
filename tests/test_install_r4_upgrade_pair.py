"""Exercise the designated 464830d-to-candidate exact probe and closed mutations."""
from pathlib import Path
import json
import subprocess

import pytest

from tests.test_data_generation_upgrade_pair_r4 import ROOT, _pair_layouts, probe

BASE = '464830d78a305acb7aa01a7a7afc9e72a924a5c9'


@pytest.fixture(scope='module')
def r4_base(tmp_path_factory):
    path = tmp_path_factory.mktemp('r4-base') / 'source'
    subprocess.run(['git', 'clone', '--quiet', '--shared', '--no-checkout',
                    '-c', 'core.autocrlf=false', str(ROOT), str(path)], check=True, capture_output=True)
    subprocess.run(['git', '-C', str(path), 'checkout', '--quiet', '--detach', BASE], check=True, capture_output=True)
    return path


@pytest.mark.parametrize('mutation', ['exact', 'helper', 'redaction_rules', 'runtime', 'unlisted'])
def test_r4_declared_pair_and_closed_mutations(tmp_path, r4_base, mutation):
    old, new = _pair_layouts(tmp_path, r4_base, ROOT, third_party=True)
    if mutation != 'exact':
        relative = {'helper': 'INSTALL_THIS_PC.ps1',
                    'redaction_rules': 'app/tools/failure_redaction_rules.json',
                    'runtime': 'runtime/fixture', 'unlisted': 'app/unreviewed.py'}[mutation]
        with (new / relative).open('ab') as stream:
            stream.write(b'\n')
    forward = probe(new, old, integrated=True)
    if mutation != 'exact':
        assert forward.returncode != 0
        assert 'WRITER_TRANSITION_' in forward.stderr
        return
    assert forward.returncode == 0, forward.stderr[-1800:]
    assert json.loads(forward.stdout)['compatibility'] == 'PINNED_STARTUP_UPDATE_BOOTSTRAP'
    for signed in (False, True):
        reverse = probe(old, new, signed=signed, integrated=True)
        assert reverse.returncode != 0
        assert 'WRITER_TRANSITION_' in reverse.stderr

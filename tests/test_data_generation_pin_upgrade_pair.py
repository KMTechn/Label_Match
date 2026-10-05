"""Exercise the actual installer probe for the finite delayed-reader correction."""
import json
from pathlib import Path
import subprocess

import pytest

from tests.test_data_generation_upgrade_pair import ROOT, _package_pair, probe


@pytest.fixture(scope='module')
def pin_preimage(tmp_path_factory):
    root = tmp_path_factory.mktemp('pin-preimage') / 'repo'
    subprocess.run(['git', 'clone', '--quiet', '--shared', '--no-checkout', '-c', 'core.autocrlf=false', str(ROOT), str(root)],
                   check=True, capture_output=True)
    subprocess.run(['git', '-c', 'core.autocrlf=false', '-C', str(root), 'checkout',
                    '--quiet', '--detach', '2c46de16f2303117e88d4794bf3793f0d7de8c76'],
                   check=True, capture_output=True)
    return root


def test_delayed_reader_pair_is_exact_forward_only(tmp_path, pin_preimage):
    installed, source = _package_pair(tmp_path, pin_preimage)
    result = probe(source, installed)
    assert result.returncode == 0
    assert json.loads(result.stdout)['compatibility'] == 'PINNED_DATA_GENERATION_G_CORRECTION'
    assert probe(installed, source).returncode != 0


@pytest.mark.parametrize('relative', ['app/Label_Match.py', 'app/label_admin_pin.py',
                                    'app/logistics_runtime_profile.py',
                                    'app/writer_session_fence.py', 'tools/label_writer_fence.ps1'])
def test_delayed_reader_pair_rejects_unreviewed_bytes(tmp_path, pin_preimage, relative):
    installed, source = _package_pair(tmp_path, pin_preimage)
    target = Path(source) / relative
    target.write_bytes(target.read_bytes() + b'\n# unreviewed fixture mutation\n')
    assert probe(source, installed).returncode != 0

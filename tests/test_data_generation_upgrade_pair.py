"""Run the installer's actual read-only upgrade probe on isolated source copies."""
import json
from pathlib import Path
import shutil
import subprocess
import sys

import pytest
from tools import build_portable_release_candidate as builder

ROOT = Path(__file__).resolve().parents[1]
BASE_COMMIT = 'e89c18879f5bf59ddcd550152bf26ffedc4d687a'
# The original signed reverse contract belongs to the pure-G source, before
# ac745's forward-only audit-index companion changes.
ORIGINAL_G_COMMIT = '362f30f7c6c7a4d952bb4d1ed985adc6f0a8cea8'


@pytest.fixture(scope='module')
def baseline_source(tmp_path_factory):
    baseline = tmp_path_factory.mktemp('g-source') / 'baseline'
    subprocess.run(['git', 'clone', '--quiet', '--shared', '--no-checkout',
                    '-c', 'core.autocrlf=false', str(ROOT), str(baseline)],
                   check=True, capture_output=True)
    subprocess.run(['git', '-C', str(baseline), 'checkout', '--quiet', '--detach',
                    BASE_COMMIT], check=True, capture_output=True)
    return baseline


@pytest.fixture
def pair(tmp_path, baseline_source):
    return _package_pair(tmp_path, baseline_source)


def _package_pair(tmp_path, baseline_source):
    from tests.test_data_generation_upgrade_pair_r4 import _pair_layouts
    return _pair_layouts(tmp_path, baseline_source, ROOT, third_party=True)


@pytest.fixture(scope='module')
def correction_source(tmp_path_factory):
    baseline = tmp_path_factory.mktemp('g-correction-source') / 'baseline'
    subprocess.run(['git', 'clone', '--quiet', '--shared', '--no-checkout',
                    '-c', 'core.autocrlf=false', str(ROOT), str(baseline)],
                   check=True, capture_output=True)
    subprocess.run(['git', '-C', str(baseline), 'checkout', '--quiet', '--detach',
                    'a56a58769c43d1b19abdd9881425f95a49dbd7f7'], check=True, capture_output=True)
    return baseline


@pytest.fixture
def correction_pair(tmp_path, correction_source):
    return _package_pair(tmp_path, correction_source)


def probe(source, installed, *, signed=False, installer=None):
    installer = installer or ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1'
    text = installer.read_text(encoding='utf-8-sig')
    start = text.index("$probe = @'") + len("$probe = @'")
    code = text[start:text.index("\n'@", start)].lstrip()
    return subprocess.run([sys.executable, '-B', '-', str(source), str(installed),
                           'signed-compatible-downgrade' if signed else 'no-data-compatibility-override'],
                          input=code, capture_output=True, text=True, timeout=30,
                          creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))


def _tree_bytes(*roots):
    return {(index, path.relative_to(root)): path.read_bytes()
            for index, root in enumerate(roots)
            for path in root.rglob("*") if path.is_file()}


def test_exact_g_pair_adds_only_one_writer_and_signed_reverse_is_explicit(pair, baseline_source):
    old, new = pair
    forward = probe(new, old)
    assert forward.returncode == 0, forward.stderr[-2000:]
    assert json.loads(forward.stdout)['compatibility'] == 'PINNED_DATA_GENERATION_G'
    assert probe(old, new).returncode != 0
    reverse = probe(old, new, signed=True)
    # The merged audit-index companion pair is forward only, including when
    # an explicit signed marker permits the original ten-file G reversal.
    assert reverse.returncode != 0
    assert 'WRITER_TRANSITION_SOURCE_SET_DIFFERS' in reverse.stderr
    historical = new.parent / 'original-g-source'
    subprocess.run(['git', 'clone', '--quiet', '--shared', '--no-checkout',
                    '-c', 'core.autocrlf=false', str(ROOT), str(historical)],
                   check=True, capture_output=True)
    subprocess.run(['git', '-C', str(historical), 'checkout', '--quiet', '--detach',
                    ORIGINAL_G_COMMIT], check=True, capture_output=True)
    from tests.test_data_generation_upgrade_pair_r4 import _pair_layouts
    old, legacy = _pair_layouts(new.parent / 'original-g', baseline_source, historical)
    installer = legacy / 'INSTALL_CANONICAL_PORTABLE.ps1'
    original_reverse = probe(old, legacy, signed=True, installer=installer)
    assert original_reverse.returncode == 0, original_reverse.stderr[-2000:]
    assert json.loads(original_reverse.stdout)['compatibility'] == 'SIGNED_COMPATIBLE_DATA_GENERATION_REVERSE'


def test_canonical_installer_preserves_one_utf8_bom_for_windows_powershell():
    raw = (ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1').read_bytes()
    assert raw.startswith(b'\xef\xbb\xbf')
    assert not raw[3:].startswith(b'\xef\xbb\xbf')


@pytest.mark.parametrize('mutation', ['source', 'preimage', 'signed_reverse'])
def test_source_admission_inventory_reader_pair_is_exact_and_forward_only(pair, mutation):
    old, new = pair
    assert probe(new, old).returncode == 0
    if mutation == 'signed_reverse':
        refused = probe(old, new, signed=True)
    else:
        changed = new if mutation == 'source' else old
        path = changed / 'app/label_exact_clone_resolution.py'
        with path.open('ab') as stream:
            stream.write(b'\nUNREVIEWED_SOURCE_ADMISSION = True\n')
        refused = probe(new, old)
    assert refused.returncode != 0
    assert 'WRITER_TRANSITION_' in refused.stderr


def test_quiesced_transport_companion_refuses_signed_g_reverse(pair):
    """operations.md:1002 requires the first rejection for each invalid mixed layout."""
    from tests.test_writer_transition import _pin

    old, new = pair
    installed = new.parent / 'quiesced-g'
    shutil.copytree(new, installed)
    for relative in ('app/label_data_manager.py', 'app/label_recovery_schema.py'):
        (installed / relative).write_bytes((old / relative).read_bytes())
    before = _tree_bytes(old, installed)
    unpinned = probe(old, installed, signed=True)
    assert unpinned.returncode != 0
    assert "WRITER_TRANSITION_PIN_INVALID" in unpinned.stderr
    assert _tree_bytes(old, installed) == before

    _pin(installed)
    before = _tree_bytes(old, installed)
    reverse = probe(old, installed, signed=True)
    assert reverse.returncode != 0
    assert "WRITER_TRANSITION_SOURCE_SET_DIFFERS" in reverse.stderr
    assert _tree_bytes(old, installed) == before


def test_unpinned_transport_companion_mix_is_rejected_before_writes(pair):
    old, new = pair
    installed = new.parent / 'quiesced-g'
    shutil.copytree(new, installed)
    # Remove the older audit-index companion so this rejection identifies
    # the independent, forward-only maintenance admission repair.
    for relative in ('app/label_data_manager.py', 'app/label_recovery_schema.py'):
        (installed / relative).write_bytes((old / relative).read_bytes())
    before = {path.relative_to(installed): path.read_bytes()
              for path in installed.rglob("*") if path.is_file()}
    reverse = probe(old, installed, signed=True)
    assert reverse.returncode != 0
    assert "WRITER_TRANSITION_PIN_INVALID" in reverse.stderr
    after = {path.relative_to(installed): path.read_bytes()
             for path in installed.rglob("*") if path.is_file()}
    assert after == before


@pytest.mark.parametrize('mutation', ['runtime', 'extra_app', 'helper', 'old_source', 'shutdown_notice', 'old_notice', 'corrected_host'])
def test_g_pair_rejects_changes_outside_its_exact_declaration(pair, mutation):
    old, new = pair
    if mutation == 'runtime':
        (new / 'runtime' / 'fixture').write_bytes(b'changed runtime')
    elif mutation == 'extra_app':
        (new / 'app' / 'extra.py').write_text('UNREVIEWED = True\n', encoding='utf-8')
    elif mutation == 'helper':
        with (new / 'app' / 'kmtech_shared' / 'data_generation.py').open('ab') as handle:
            handle.write(b'\nUNREVIEWED = True\n')
    elif mutation == 'old_source':
        with (old / 'app' / 'Label_Match.py').open('ab') as handle:
            handle.write(b'\nUNREVIEWED = True\n')
    elif mutation == 'corrected_host':
        with (new / 'app' / 'label_match_product_host.py').open('ab') as handle:
            handle.write(b'\nUNREVIEWED = True\n')
    else:
        package = old if mutation == 'old_notice' else new
        with (package / 'app' / 'label_match_single_instance.py').open('ab') as handle:
            handle.write(b'\nUNREVIEWED = True\n')
    assert probe(new, old).returncode != 0
    assert probe(old, new, signed=True).returncode != 0


@pytest.mark.parametrize('revision,compatibility', [
    ('4b7630f', 'PINNED_MAINLOOP_EXIT_CLEANUP'),
    ('640d770', 'PINNED_MAINLOOP_EXIT_NOTICE'),
])
def test_g_composes_only_with_the_exact_shutdown_repair(pair, revision, compatibility):
    old, new = pair
    snapshot = new.parent / 'shutdown-source'
    subprocess.run(['git', 'clone', '--quiet', '--shared', '--no-checkout',
                    '-c', 'core.autocrlf=false', str(ROOT), str(snapshot)], check=True, capture_output=True)
    subprocess.run(['git', '-C', str(snapshot), 'checkout', '--quiet', '--detach', revision],
                   check=True, capture_output=True)
    from tests.test_data_generation_upgrade_pair_r4 import _pair_layouts
    old, _ = _package_pair(new.parent / 'shutdown-pair', snapshot)
    forward = probe(new, old)
    assert forward.returncode == 0, forward.stderr[-2000:]
    assert json.loads(forward.stdout)['compatibility'] == compatibility
    assert probe(old, new, signed=True).returncode != 0
    with (old / 'app/label_match_single_instance.py').open('ab') as handle:
        handle.write(b'\nUNREVIEWED = True\n')
    assert probe(new, old).returncode != 0


@pytest.mark.parametrize('relative,mutation,reason', [
    ('app/label_data_manager.py', 'old', 'app/label_recovery_schema.py'),
    ('app/label_recovery_schema.py', 'old', 'app/label_data_manager.py'),
    ('app/label_data_manager.py', 'one-byte', 'app/label_data_manager.py'),
    ('app/label_recovery_schema.py', 'one-byte', 'app/label_data_manager.py'),
    ('app/carrier_identity_port.py', 'one-byte', 'app/carrier_identity_port.py'),
])
def test_g_forward_companions_refuse_stale_partial_and_unreviewed_bytes(pair, relative, mutation, reason):
    old, new = pair
    before = {path.relative_to(old): path.read_bytes() for path in old.rglob('*') if path.is_file()}
    target = new / relative
    if mutation == 'old':
        target.write_bytes((old / relative).read_bytes())
    else:
        target.write_bytes(target.read_bytes() + b'X')
    result = probe(new, old)
    assert result.returncode != 0
    assert 'WRITER_TRANSITION_SOURCE_SET_DIFFERS' in result.stderr
    assert {path.relative_to(old): path.read_bytes() for path in old.rglob('*') if path.is_file()} == before


@pytest.mark.parametrize('revision', ['362f30f', 'a56a587'])
def test_already_g_committed_preimage_uses_only_forward_companions(pair, revision):
    old, new = pair
    installed = new.parent / 'already-g'
    shutil.copytree(new, installed)
    for relative in ('app/label_data_manager.py', 'app/label_recovery_schema.py'):
        before = subprocess.check_output(['git', '-C', str(ROOT), 'show', revision + ':' + relative.removeprefix('app/')])
        assert before.replace(b'\r\n', b'\n') == (old / relative).read_bytes().replace(b'\r\n', b'\n')
        (installed / relative).write_bytes(before)
    preserved = {path.relative_to(installed): path.read_bytes() for path in installed.rglob('*') if path.is_file()}
    forward = probe(new, installed)
    assert forward.returncode == 0, forward.stderr[-2000:]
    assert json.loads(forward.stdout)['compatibility'] == 'PINNED_DATA_GENERATION_G'
    for signed in (False, True):
        reverse = probe(installed, new, signed=signed)
        assert reverse.returncode != 0
        assert 'WRITER_TRANSITION_SEMANTICS_DIFFER: app/label_data_manager.py' in reverse.stderr
    assert {path.relative_to(installed): path.read_bytes() for path in installed.rglob('*') if path.is_file()} == preserved


def test_already_g_correction_is_exact_forward_and_refuses_reverse(correction_pair):
    old, new = correction_pair
    forward = probe(new, old)
    assert forward.returncode == 0, forward.stderr[-2000:]
    assert json.loads(forward.stdout)['compatibility'] == 'PINNED_DATA_GENERATION_G_CORRECTION'
    assert probe(old, new).returncode != 0
    assert probe(old, new, signed=True).returncode != 0


@pytest.mark.parametrize('mutation', ['declared_one_byte', 'unlisted_file'])
def test_already_g_correction_rejects_declared_or_unlisted_mutation(correction_pair, mutation):
    old, new = correction_pair
    relative, extra = ('app/label_match_product_host.py', b'\n') if mutation == 'declared_one_byte' else (
        'app/label_worker_roster.py', b'\nUNREVIEWED = True\n')
    with (new / relative).open('ab') as handle:
        handle.write(extra)
    assert probe(new, old).returncode != 0

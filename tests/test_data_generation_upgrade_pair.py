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


@pytest.fixture(scope='module')
def baseline_source(tmp_path_factory):
    baseline = tmp_path_factory.mktemp('g-source') / 'baseline'
    subprocess.run(['git', 'clone', '--quiet', '--no-hardlinks', '--no-checkout',
                    '-c', 'core.autocrlf=false', str(ROOT), str(baseline)],
                   check=True, capture_output=True)
    subprocess.run(['git', '-C', str(baseline), 'checkout', '--quiet', '--detach',
                    BASE_COMMIT], check=True, capture_output=True)
    return baseline


@pytest.fixture
def pair(tmp_path, baseline_source):
    result = []
    for name, source in (('old', baseline_source), ('new', ROOT)):
        package = tmp_path / name
        builder._copy_application(source, package / 'app')
        (package / 'runtime').mkdir()
        (package / 'runtime' / 'fixture').write_bytes(b'identical runtime preimage')
        (package / 'launch-label-match.cmd').write_bytes(b'identical launcher fixture\r\n')
        for relative in ('INSTALL_THIS_PC.ps1',
                         'tools/bootstrap_integrity.ps1', 'tools/label_writer_fence.ps1',
                         'tools/label_writer_fence_contract.json'):
            target = package / relative
            target.parent.mkdir(exist_ok=True)
            shutil.copyfile(source / relative, target)
        # Stock uses committed blobs, not CRLF checkout data. Keep the same
        # serialization for tracked LF inputs while allowing candidate edits.
        eol = subprocess.check_output(['git', '-C', str(source), 'ls-files',
                                       '--eol', '-z']).decode('utf-8')
        for row in filter(None, eol.split('\0')):
            metadata, relative = row.split('\t', 1)
            if not metadata.startswith('i/lf'):
                continue
            targets = [package / 'app' / relative, package / relative]
            if relative == 'portable/main.py':
                targets.append(package / 'app/main.py')
            for target in targets:
                if target.is_file():
                    target.write_bytes(target.read_bytes().replace(b'\r\n', b'\n'))
        result.append(package)
    return result


def probe(source, installed, *, signed=False):
    text = (ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1').read_text(encoding='utf-8-sig')
    start = text.index("$probe = @'") + len("$probe = @'")
    code = text[start:text.index("\n'@", start)].lstrip()
    return subprocess.run([sys.executable, '-B', '-', str(source), str(installed),
                           'signed-compatible-downgrade' if signed else 'no-data-compatibility-override'],
                          input=code, capture_output=True, text=True, timeout=30,
                          creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))


def test_exact_g_pair_adds_only_one_writer_and_signed_reverse_is_explicit(pair):
    old, new = pair
    forward = probe(new, old)
    assert forward.returncode == 0, forward.stderr[-2000:]
    assert json.loads(forward.stdout)['compatibility'] == 'PINNED_DATA_GENERATION_G'
    assert probe(old, new).returncode != 0
    reverse = probe(old, new, signed=True)
    assert reverse.returncode == 0, reverse.stderr[-2000:]
    assert json.loads(reverse.stdout)['compatibility'] == 'SIGNED_COMPATIBLE_DATA_GENERATION_REVERSE'


@pytest.mark.parametrize('mutation', ['runtime', 'extra_app', 'helper', 'old_source', 'shutdown_notice', 'old_notice'])
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
    for relative in ('app/Label_Match.py', 'app/label_match_single_instance.py',
                     'app/writer_session_fence.py', 'tools/label_writer_fence.ps1'):
        (old / relative).write_bytes(subprocess.check_output([
            'git', '-C', str(ROOT), 'show', revision + ':' + relative.removeprefix('app/')]))
    forward = probe(new, old)
    assert forward.returncode == 0, forward.stderr[-2000:]
    assert json.loads(forward.stdout)['compatibility'] == compatibility
    assert probe(old, new, signed=True).returncode != 0
    with (old / 'app/label_match_single_instance.py').open('ab') as handle:
        handle.write(b'\nUNREVIEWED = True\n')
    assert probe(new, old).returncode != 0

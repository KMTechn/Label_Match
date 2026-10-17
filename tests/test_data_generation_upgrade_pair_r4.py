"""Run the installer's actual read-only upgrade probe on isolated source copies."""
import json
import importlib.util
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
BASE_COMMIT = 'e89c18879f5bf59ddcd550152bf26ffedc4d687a'
HISTORICAL_G_COMMIT = '362f30f7c6c7a4d952bb4d1ed985adc6f0a8cea8'
COMPANION_G_COMMIT = 'ac745f0'


@pytest.fixture(scope='module')
def baseline_source(tmp_path_factory):
    baseline = tmp_path_factory.mktemp('g-source') / 'baseline'
    subprocess.run(['git', 'clone', '--quiet', '--shared', '--no-checkout',
                    '-c', 'core.autocrlf=false', str(ROOT), str(baseline)],
                   check=True, capture_output=True)
    subprocess.run(['git', '-C', str(baseline), 'checkout', '--quiet', '--detach',
                    BASE_COMMIT], check=True, capture_output=True)
    return baseline


@pytest.fixture(scope='module')
def historical_g_source(tmp_path_factory):
    source = tmp_path_factory.mktemp('historical-g-source') / 'source'
    subprocess.run(['git', 'clone', '--quiet', '--shared', '--no-checkout',
                    '-c', 'core.autocrlf=false', str(ROOT), str(source)],
                   check=True, capture_output=True)
    subprocess.run(['git', '-C', str(source), 'checkout', '--quiet', '--detach',
                    HISTORICAL_G_COMMIT], check=True, capture_output=True)
    return source


@pytest.fixture(scope='module')
def companion_g_source(tmp_path_factory):
    source = tmp_path_factory.mktemp('companion-g-source') / 'source'
    subprocess.run(['git', 'clone', '--quiet', '--shared', '--no-checkout',
                    '-c', 'core.autocrlf=false', str(ROOT), str(source)],
                   check=True, capture_output=True)
    subprocess.run(['git', '-C', str(source), 'checkout', '--quiet', '--detach',
                    COMPANION_G_COMMIT], check=True, capture_output=True)
    return source


def _pair_layouts(tmp_path, baseline_source, candidate_source, *, third_party=False):
    from tools import build_portable_release_candidate as candidate_builder
    from tools import refresh_first_update_declarations as generator

    result = []
    for name, source in (('old', baseline_source), ('new', candidate_source)):
        package = tmp_path / name
        spec = importlib.util.spec_from_file_location(name + '_g_builder',
            source / 'tools/build_portable_release_candidate.py')
        own_builder = importlib.util.module_from_spec(spec)
        import_path = sys.path[:]
        try:
            spec.loader.exec_module(own_builder)
            # Match the layout era to the installer used by the probe.
            if third_party:
                generator.application_layout(source, package)
                if name == "old":
                    site_packages = (package / "app/site-packages").resolve()
                    for relative in candidate_builder.THIRD_PARTY_EXCLUDED_PATHS:
                        excluded = (site_packages / relative).resolve()
                        if not excluded.is_relative_to(site_packages):
                            raise ValueError("third-party exclusion escapes site-packages")
                        if excluded.is_dir():
                            shutil.rmtree(excluded)
                        elif excluded.is_file():
                            excluded.unlink()
            else:
                own_builder._copy_application(source, package / 'app')
        finally:
            sys.path[:] = import_path
        (package / 'runtime').mkdir()
        (package / 'runtime' / 'fixture').write_bytes(b'identical runtime preimage')
        (package / 'launch-label-match.cmd').write_bytes(b'identical launcher fixture\r\n')
        for relative in ('INSTALL_THIS_PC.ps1', 'INSTALL_CANONICAL_PORTABLE.ps1',
                         'tools/bootstrap_integrity.ps1', 'tools/label_writer_fence.ps1',
                         'tools/label_writer_fence_contract.json'):
            target = package / relative
            target.parent.mkdir(exist_ok=True)
            shutil.copyfile(source / relative, target)
        for relative in getattr(own_builder, 'FAILURE_SINK_DATA', ()):
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
        commit = subprocess.check_output(['git', '-C', str(source), 'rev-parse',
                                          'HEAD'], text=True).strip()
        (package / 'portable-manifest.json').write_text(json.dumps({
            'source_commit': commit}), encoding='utf-8')
        if (source / 'tools/apply_prepared_update.ps1').is_file():
            if not third_party:
                from auto_update import canonical_startup_settings
                (package / 'app/auto-update-settings.json').write_bytes(
                    canonical_startup_settings({}))
            public = json.loads((source / 'config/update-manifest-key-config.json').read_text(
                encoding='utf-8-sig'))
            (package / 'app/update-manifest-key-config.json').write_bytes(
                (json.dumps(public, ensure_ascii=True, sort_keys=True, indent=2) + '\n').encode())
            shutil.copyfile(source / 'tools/apply_prepared_update.ps1',
                            package / 'tools/apply_prepared_update.ps1')
        else:
            shutil.copyfile(ROOT / 'tests/fixtures/LM-e88011c-public-key-config.json',
                            package / 'app/update-manifest-key-config.json')
        result.append(package)
    return result


@pytest.fixture
def historical_pair(tmp_path, baseline_source, historical_g_source):
    """Preserve the accepted pure-G pair and its signed reverse contract."""
    return _pair_layouts(tmp_path, baseline_source, historical_g_source)


@pytest.fixture
def pair(tmp_path, baseline_source, companion_g_source):
    """Use the reviewed legacyguard package and its own companion declarations."""
    return _pair_layouts(tmp_path, baseline_source, companion_g_source)


@pytest.fixture
def integration_pair(tmp_path, baseline_source):
    return _pair_layouts(tmp_path, baseline_source, ROOT, third_party=True)


def probe(source, installed, *, signed=False, integrated=False, historical=False):
    if integrated:
        text = (ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1').read_text(encoding='utf-8-sig')
    else:
        text = subprocess.check_output(['git', '-C', str(ROOT), 'show',
            (HISTORICAL_G_COMMIT if historical else COMPANION_G_COMMIT)
            + ':INSTALL_CANONICAL_PORTABLE.ps1']).decode('utf-8-sig')
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
    # The merged audit-index companion pair is forward only, including when
    # an explicit signed marker permits the original ten-file G reversal.
    assert reverse.returncode != 0
    assert 'WRITER_TRANSITION_SEMANTICS_DIFFER: app/label_data_manager.py' in reverse.stderr
    legacy = new.parent / 'original-g'
    shutil.copytree(new, legacy)
    for relative in ('app/label_data_manager.py', 'app/label_recovery_schema.py'):
        (legacy / relative).write_bytes((old / relative).read_bytes())
    original_reverse = probe(old, legacy, signed=True)
    assert original_reverse.returncode == 0, original_reverse.stderr[-2000:]
    assert json.loads(original_reverse.stdout)['compatibility'] == 'SIGNED_COMPATIBLE_DATA_GENERATION_REVERSE'


def test_integrated_g_upgrade_is_exactly_forward_only(integration_pair):
    old, new = integration_pair
    forward = probe(new, old, integrated=True)
    assert forward.returncode == 0, forward.stderr[-2000:]
    assert json.loads(forward.stdout)['compatibility'] == 'PINNED_DATA_GENERATION_G'
    for signed in (False, True):
        reverse = probe(old, new, signed=signed, integrated=True)
        assert reverse.returncode != 0
        assert 'WRITER_TRANSITION_' in reverse.stderr


@pytest.mark.parametrize('change', ['one_byte', 'unlisted', 'helper'])
def test_integrated_g_upgrade_refuses_undeclared_changes(integration_pair, change):
    old, new = integration_pair
    relative = {'one_byte': 'app/Label_Match.py', 'unlisted': 'app/undeclared.py',
                'helper': 'app/kmtech_shared/data_generation.py'}[change]
    with (new / relative).open('ab') as handle:
        handle.write(b'\n')
    result = probe(new, old, integrated=True)
    assert result.returncode != 0
    assert 'WRITER_TRANSITION_' in result.stderr


@pytest.mark.parametrize('mutation', ['runtime', 'extra_app', 'helper', 'old_source', 'shutdown_notice', 'old_notice'])
def test_g_pair_rejects_changes_outside_its_exact_declaration(historical_pair, mutation):
    old, new = historical_pair
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
    assert probe(new, old, historical=True).returncode != 0
    assert probe(old, new, signed=True, historical=True).returncode != 0


@pytest.mark.parametrize('revision,compatibility', [
    ('4b7630f', 'PINNED_MAINLOOP_EXIT_CLEANUP'),
    ('640d770', 'PINNED_MAINLOOP_EXIT_NOTICE'),
])
def test_g_composes_only_with_the_exact_shutdown_repair(historical_pair, revision, compatibility):
    old, new = historical_pair
    for relative in ('app/Label_Match.py', 'app/label_match_single_instance.py',
                     'app/writer_session_fence.py', 'tools/label_writer_fence.ps1'):
        (old / relative).write_bytes(subprocess.check_output([
            'git', '-C', str(ROOT), 'show', revision + ':' + relative.removeprefix('app/')]))
    forward = probe(new, old, historical=True)
    assert forward.returncode == 0, forward.stderr[-2000:]
    assert json.loads(forward.stdout)['compatibility'] == compatibility
    assert probe(old, new, signed=True, historical=True).returncode != 0
    with (old / 'app/label_match_single_instance.py').open('ab') as handle:
        handle.write(b'\nUNREVIEWED = True\n')
    assert probe(new, old, historical=True).returncode != 0


def test_original_pure_g_forward_and_signed_reverse_remain_proven(historical_pair):
    old, new = historical_pair
    forward = probe(new, old, historical=True)
    assert forward.returncode == 0, forward.stderr[-2000:]
    assert json.loads(forward.stdout)['compatibility'] == 'PINNED_DATA_GENERATION_G'
    assert probe(old, new, historical=True).returncode != 0
    reverse = probe(old, new, signed=True, historical=True)
    assert reverse.returncode == 0, reverse.stderr[-2000:]
    assert json.loads(reverse.stdout)['compatibility'] == 'SIGNED_COMPATIBLE_DATA_GENERATION_REVERSE'


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
    assert 'WRITER_TRANSITION_SEMANTICS_DIFFER: ' + reason in result.stderr
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

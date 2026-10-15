"""Generated payload inventory and the actual pinned PowerShell transition gate.

Only the runtime executable location is substituted with the contained host
Python. Assert-WriterTransition and its exact pinned stdin program are intact.
"""
import ast
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest

from tests.test_writer_transition import _definitions, _ps, _quote
from tools import build_portable_release_candidate as stock
from tools import refresh_first_update_declarations as generator

ROOT = Path(__file__).resolve().parents[1]
BEFORE_HELPER = '3f349dbf3c4ac0950326268161dfade4107c7bc3'
BEFORE_FIX = '16efb1b4d7a445e29c37f43249b8149aa180256f'
APPROVED_SETTINGS = Path('D:/KMTech/program-improvement-20260912/work/program/w9mgrweb/field-update-feed-1015/label_match.auto-update-settings.json')
SETTINGS = 'app/auto-update-settings.json'
INSTALLED_REVISION = 'e7b9075b2383889e77f524be7a1f0f80d7630033'
UNTRIMMED_REVISIONS = (
    'd9160b6ab5dce3e7a0010679f4ae8f28077eea76',
    '89155f17233fb1c8b5bae2a9e470832772a4d785',
)


def declarations(revision=None):
    raw = ((ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1').read_bytes() if revision is None else
           subprocess.check_output(['git', 'show', revision + ':INSTALL_CANONICAL_PORTABLE.ps1'], cwd=ROOT))
    assignments = generator.writer_probe(raw.decode('utf-8-sig'))[3]
    return {name: ast.literal_eval(assignments[name].value)
            for name in ('startup_release_replacements', 'startup_release_additions')}


def test_all_sixteen_generated_settings_additions_and_historical_before_pins():
    old = declarations(BEFORE_HELPER)
    before_fix = declarations(BEFORE_FIX)
    current = declarations()
    expected = {revision for revision, paths in old['startup_release_additions'].items() if SETTINGS in paths}
    actual = {revision for revision, paths in current['startup_release_additions'].items() if SETTINGS in paths}
    assert len(expected) == 16 and actual == expected
    assert len(current['startup_release_replacements']) == 23
    for revision, paths in before_fix['startup_release_replacements'].items():
        assert set(paths) == set(current['startup_release_replacements'][revision])
        for relative, pair in paths.items():
            assert current['startup_release_replacements'][revision][relative][0] == pair[0]
    for revision in expected:
        assert current['startup_release_additions'][revision][SETTINGS] == old['startup_release_additions'][revision][SETTINGS]


def test_layout_inventories_actual_helper_outputs_instead_of_caller_literals(tmp_path):
    source = tmp_path / 'source'
    (source / 'tools').mkdir(parents=True)
    (source / 'tools/build_portable_release_candidate.py').write_text('''
def _copy_application(source, target):
    target.mkdir(parents=True)
def _copy_third_party(target):
    pass
def _write_auto_update_settings(app_root, raw):
    (app_root / 'helper-output.json').write_bytes(raw)
def build():
    _write_auto_update_settings(app_root, raw)
''', encoding='utf-8')
    (source / 'auto_update.py').write_text('def policy_from_settings(settings):\n    return {}\n', encoding='utf-8')
    (source / 'INSTALL_THIS_PC.ps1').write_text('# fixture\n', encoding='utf-8')
    target = tmp_path / 'package'
    layout = generator.application_layout(source, target)
    assert set(layout) == {'INSTALL_THIS_PC.ps1', 'app/helper-output.json'}
    assert layout['app/helper-output.json'] == generator.digest((target / 'app/helper-output.json').read_bytes())


def materialize(source, target, revision):
    layout = generator.application_layout(source, target)
    for relative in layout:
        if relative.startswith('app/'):
            continue
        destination = target / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source / relative, destination)
    shutil.copyfile(source / 'portable/launch-label-match.cmd', target / 'launch-label-match.cmd')
    # This build-input trust configuration is separate from the source layout.
    # Keep the same approved public configuration on both sides (no rotation).
    shutil.copyfile(ROOT / 'config/update-manifest-key-config.json',
                    target / 'app/update-manifest-key-config.json')
    (target / 'runtime').mkdir()
    (target / 'portable-manifest.json').write_text(json.dumps({'source_commit': revision}), encoding='utf-8')
    return layout


@pytest.fixture(scope='module')
def transition_layouts():
    root = Path(os.environ['KMTECH_TEST_CA_TASK_ROOT']) / 'settings-transition'
    root.mkdir()
    old_source = root / 'historical-source'
    generator.snapshot(INSTALLED_REVISION, old_source)
    installed = root / 'installed'
    old_layout = materialize(old_source, installed, INSTALLED_REVISION)
    candidate = root / 'candidate'
    candidate_layout = materialize(ROOT, candidate, 'candidate')
    assert subprocess.run(['git', 'merge-base', '--is-ancestor', '8929d8f', INSTALLED_REVISION],
                          cwd=ROOT, capture_output=True).returncode == 0
    assert SETTINGS in old_layout
    assert not list((installed / 'app/site-packages/qrcode/tests').glob('*.py'))
    assert SETTINGS in candidate_layout
    actual_app = {path.relative_to(candidate).as_posix(): generator.digest(path.read_bytes())
                  for path in (candidate / 'app').rglob('*')
                  if path.is_file() and path.relative_to(candidate).as_posix() != 'app/update-manifest-key-config.json'}
    assert actual_app == {name: pin for name, pin in candidate_layout.items() if name.startswith('app/')}
    raw, _ = stock._release_update_settings(APPROVED_SETTINGS)
    stock._write_auto_update_settings(candidate / 'app', raw)
    # Settings are a release build input. This post-trim direction already
    # supplies that file; use the same approved policy on each side.
    stock._write_auto_update_settings(installed / 'app', raw)
    assert (candidate / SETTINGS).read_bytes() == APPROVED_SETTINGS.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == stock.RELEASE_AUTO_UPDATE_SETTINGS_SHA256
    ps_root = root / 'ps'
    ps_root.mkdir()
    return installed, candidate, ps_root


def actual_transition(installed, candidate, ps_root):
    code = _definitions()
    launcher = "-Python (Join-Path $Source 'runtime\\python.exe')"
    assert code.count(launcher) == 1
    code = code.replace(launcher, '-Python ' + _quote(sys.executable))
    code += f'''
try {{
    $result=Assert-WriterTransition {_quote(candidate)} {_quote(installed)}
    @{{status='PASS';compatibility=$result.compatibility}} | ConvertTo-Json -Compress
}} catch {{
    $failure=$_.Exception.Data['child_output']
    $fixed=@()
    if($null -ne $failure -and $failure.Contains('report_path')) {{
        $path=[IO.Path]::GetFullPath([string]$failure.report_path)
        $temp=[IO.Path]::GetFullPath([IO.Path]::GetTempPath())
        if($path.StartsWith($temp,[StringComparison]::OrdinalIgnoreCase) -and (Get-Item -LiteralPath $path).Length -le 65536) {{
            $fixed=@([regex]::Matches([IO.File]::ReadAllText($path),'\\bWRITER_TRANSITION_[A-Z_]+\\b') | ForEach-Object {{$_.Value}} | Select-Object -Unique)
        }}
    }}
    @{{status='REJECTED';codes=$fixed}} | ConvertTo-Json -Compress
    exit 7
}}
'''
    result = _ps(ps_root, code, os.environ.copy(), timeout=90)
    return result.returncode, json.loads(result.stdout)


@pytest.mark.parametrize('mutation', ['none', 'missing', 'tampered', 'unexpected-existing'])
def test_actual_assert_writer_transition_with_approved_generated_settings(transition_layouts, mutation):
    installed, candidate, ps_root = transition_layouts
    settings = candidate / SETTINGS
    unexpected = installed / 'app/unexpected-existing.txt'
    original = settings.read_bytes()
    try:
        if mutation == 'missing':
            settings.unlink()
        elif mutation == 'tampered':
            settings.write_bytes(original + b'x')
        elif mutation == 'unexpected-existing':
            unexpected.write_bytes(b'unexpected')
        returncode, payload = actual_transition(installed, candidate, ps_root)
        if mutation == 'none':
            assert returncode == 0
            assert payload['status'] == 'PASS'
            assert payload['compatibility'] == 'PINNED_STARTUP_UPDATE_BOOTSTRAP'
        else:
            assert returncode == 7 and payload['status'] == 'REJECTED'
            if mutation == 'tampered':
                assert payload['codes'] == ['WRITER_TRANSITION_ADDED_ASSET_DIFFERS']
            elif mutation == 'unexpected-existing':
                assert payload['codes'] == ['WRITER_TRANSITION_SOURCE_SET_DIFFERS']
    finally:
        settings.write_bytes(original)
        unexpected.unlink(missing_ok=True)


@pytest.mark.parametrize('revision', UNTRIMMED_REVISIONS)
def test_untrimmed_99_100_inputs_are_intentionally_rejected(transition_layouts, revision):
    _, candidate, ps_root = transition_layouts
    root = ps_root.parent
    source = root / ('old-source-' + revision)
    generator.snapshot(revision, source)
    installed = root / ('old-installed-' + revision)
    materialize(source, installed, revision)
    raw, _ = stock._release_update_settings(APPROVED_SETTINGS)
    stock._write_auto_update_settings(installed / 'app', raw)
    old_files = {p.relative_to(installed).as_posix() for p in (installed / 'app').rglob('*') if p.is_file()}
    new_files = {p.relative_to(candidate).as_posix() for p in (candidate / 'app').rglob('*') if p.is_file()}
    extra = old_files - new_files
    assert len(extra) == 10
    assert all(name.startswith('app/site-packages/qrcode/tests/') and name.endswith('.py') for name in extra)
    returncode, payload = actual_transition(installed, candidate, ps_root)
    assert returncode == 7 and payload == {'status': 'REJECTED', 'codes': ['WRITER_TRANSITION_SOURCE_SET_DIFFERS']}

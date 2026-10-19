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
    scan_guard_replacements = {
        'app/shell/SHELL-READY.json',  # Shell v3.16.3.1 READY provenance (19389ecb).
        'app/shell/bridge.py',  # Shell READY files[]: shell-v3.16.3.1/bridge.py.
        'app/shell/host.py',  # Shell READY files[]: shell-v3.16.3.1/host.py.
        'app/ui/parts/app/scan.js',  # UI MANIFEST d6436ff7 files[]: app/scan.js.
        'app/ui/parts/app/sheet.js',  # UI MANIFEST d6436ff7 files[]: app/sheet.js.
    }
    scan_guard_stock_revisions = {
        'e7b9075b2383889e77f524be7a1f0f80d7630033',
        '4272c5601e035f6a111e493a4acfa5d8d182978f',
        '06a474b46095fa044a0c46a045b3445cece1ee57',
    }
    for revision, paths in before_fix['startup_release_replacements'].items():
        assert (
            (reviewed_parents := (
                declarations("ebeb1ef368355a8dceebf7d33f9d1270d8cbda3d")["startup_release_replacements"][revision],
                declarations("732b0a536f30d582cd95602b243bb34a57d8b473")["startup_release_replacements"][revision],
            ))
            and set(current["startup_release_replacements"][revision])
            == (set().union(*reviewed_parents)
                | (scan_guard_replacements if revision in scan_guard_stock_revisions else set()))
            and all(
                current["startup_release_replacements"][revision][relative][0] == pair[0]
                for parent in reviewed_parents
                for relative, pair in parent.items()
            )
        )
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


def assert_builder_model_layout(builder_output, model_output, model_layout, settings_raw, public_config):
    """Compare independent app/bootstrap outputs with identical build inputs.

    Runtime, launcher, canonical installer and package manifest are outside the
    generator contract. Settings and trust config use explicit identical inputs
    in the separate model tree; bootstrap leaves are compared by content too.
    """
    stock._write_auto_update_settings(model_output / "app", settings_raw)
    stock._write_update_key_config(model_output / "app", public_config)
    expected = {p.relative_to(model_output).as_posix(): generator.digest(p.read_bytes())
                for p in (model_output / "app").rglob("*") if p.is_file()}
    expected.update({name: pin for name, pin in model_layout.items() if not name.startswith("app/")})
    outside_contract = {stock.CANONICAL_INSTALLER_FILENAME, "launch-label-match.cmd", "portable-manifest.json"}
    actual = {p.relative_to(builder_output).as_posix(): generator.digest(p.read_bytes())
              for p in builder_output.rglob("*")
              if p.is_file() and not p.relative_to(builder_output).as_posix().startswith("runtime/")
              and p.relative_to(builder_output).as_posix() not in outside_contract}
    assert actual == expected, "BUILDER_MODEL_LAYOUT_DIFFERS"


@pytest.mark.parametrize("build_only_helper", [False, True])
def test_actual_builder_fixture_matches_independent_generator_model(tmp_path, monkeypatch, build_only_helper):
    """Real build publishers; runtime/release qualification are fixture seams."""
    builder_output = tmp_path / "builder-output"
    model_output = tmp_path / "model-output"
    settings_raw, _ = stock._release_update_settings(APPROVED_SETTINGS)
    public_value = json.loads((ROOT / "tests/fixtures/lm_stock_public_update_key.json").read_bytes())["manifest_public_key"]
    public_config = public_value if isinstance(public_value, str) else json.dumps(public_value)
    model_layout = generator.application_layout(ROOT, model_output)
    for name in model_layout:
        if not name.startswith("app/"):
            destination = model_output / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, destination)

    def fixture_runtime(_source, destination):
        destination.mkdir()
        for name in ("python.exe", "pythonw.exe"):
            (destination / name).write_bytes(b"fixture-runtime")

    original_run = subprocess.run
    def fixture_run(argv, *args, **kwargs):
        if len(argv) > 3 and Path(str(argv[3])).name == "check_kmtech_shared.py":
            return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")
        return original_run(argv, *args, **kwargs)

    monkeypatch.setattr(stock, "_assert_clean_source", lambda *_: None)
    monkeypatch.setattr(stock, "_runtime_source", fixture_runtime)
    monkeypatch.setattr(subprocess, "run", fixture_run)
    for name in ("_assert_portable_import_closure", "_assert_packet_file_pins",
                 "_assert_installer_unsigned_pins", "_assert_atomic_replace_helper_pins"):
        monkeypatch.setattr(stock, name, lambda *_: None)
    original_publisher = stock._write_auto_update_settings
    def build_publisher(app, raw):
        original_publisher(app, raw)
        if build_only_helper:
            (app / "build-only-helper.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(stock, "_write_auto_update_settings", build_publisher)
    stock.build(ROOT, tmp_path / "runtime-source", builder_output,
                update_manifest_public_key_config=public_config,
                auto_update_settings=APPROVED_SETTINGS)
    monkeypatch.setattr(stock, "_write_auto_update_settings", original_publisher)
    if build_only_helper:
        with pytest.raises(AssertionError, match="BUILDER_MODEL_LAYOUT_DIFFERS"):
            assert_builder_model_layout(builder_output, model_output, model_layout, settings_raw, public_config)
    else:
        assert_builder_model_layout(builder_output, model_output, model_layout, settings_raw, public_config)


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


TRANSITION_MUTATIONS = ['none', 'missing', 'tampered', 'unexpected-existing']


@pytest.mark.parametrize("mutation", TRANSITION_MUTATIONS)
def test_actual_assert_writer_transition_with_approved_generated_settings(transition_layouts, mutation, evidence_cases=None):
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
        if evidence_cases is not None:
            kind = ("approved", "missing_settings", "tampered_settings", "unexpected_existing_settings")[TRANSITION_MUTATIONS.index(mutation)]
            evidence_cases.append(transition_case(kind, returncode, payload))
    finally:
        settings.write_bytes(original)
        unexpected.unlink(missing_ok=True)


@pytest.mark.parametrize('revision', UNTRIMMED_REVISIONS)
def test_untrimmed_99_100_inputs_are_intentionally_rejected(transition_layouts, revision, evidence_cases=None):
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

    if evidence_cases is not None:
        kind = ("untrimmed_first", "untrimmed_second")[UNTRIMMED_REVISIONS.index(revision)]
        evidence_cases.append(transition_case(kind, returncode, payload))


EXPECTED_TRANSITION_CODES = {
    "approved": "PINNED_STARTUP_UPDATE_BOOTSTRAP",
    # Missing file fails before the probe emits a WRITER_TRANSITION_* code.
    # Preserve that fact with a fixed evidence code, rather than inventing one.
    "missing_settings": "NO_FIXED_PROBE_CODE",
    "tampered_settings": "WRITER_TRANSITION_ADDED_ASSET_DIFFERS",
    "unexpected_existing_settings": "WRITER_TRANSITION_SOURCE_SET_DIFFERS",
    "untrimmed_first": "WRITER_TRANSITION_SOURCE_SET_DIFFERS",
    "untrimmed_second": "WRITER_TRANSITION_SOURCE_SET_DIFFERS",
}


def transition_case(kind, returncode, payload):
    status = "PASS" if returncode == 0 else "REJECTED"
    assert payload["status"] == status
    if status == "PASS":
        code = payload["compatibility"]
    else:
        codes = payload["codes"]
        assert isinstance(codes, list) and len(codes) <= 1
        code = codes[0] if codes else "NO_FIXED_PROBE_CODE"
    assert code == EXPECTED_TRANSITION_CODES[kind]
    return {"kind": kind, "status": status, "code": code}


def validate_transition_evidence(evidence, source_sha256):
    assert set(evidence) == {"schema", "test_source_sha256", "cases", "passed_count", "rejected_count"}
    assert evidence["schema"] == 1 and evidence["test_source_sha256"] == source_sha256
    cases = evidence["cases"]
    assert len(cases) == 6
    assert {case["kind"] for case in cases} == set(EXPECTED_TRANSITION_CODES)
    for case in cases:
        assert set(case) == {"kind", "status", "code"}
        assert case["code"] == EXPECTED_TRANSITION_CODES[case["kind"]]
        assert case["status"] == ("PASS" if case["kind"] == "approved" else "REJECTED")
    assert evidence["passed_count"] == sum(case["status"] == "PASS" for case in cases) == 1
    assert evidence["rejected_count"] == sum(case["status"] == "REJECTED" for case in cases) == 5


def write_transition_evidence(path, cases):
    source_sha256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    evidence = {
        "schema": 1, "test_source_sha256": source_sha256,
        "cases": sorted(cases, key=lambda case: case["kind"]),
        "passed_count": sum(case["status"] == "PASS" for case in cases),
        "rejected_count": sum(case["status"] == "REJECTED" for case in cases),
    }
    validate_transition_evidence(evidence, source_sha256)
    path.write_text(json.dumps(evidence, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    validate_transition_evidence(json.loads(path.read_bytes()), source_sha256)


def test_retained_actual_transition_evidence(transition_layouts):
    cases = []
    installed, candidate, ps_root = transition_layouts
    replay_ps = ps_root.parent / "evidence-replay" / "ps"
    replay_ps.mkdir(parents=True)
    replay_layouts = installed, candidate, replay_ps
    for mutation in TRANSITION_MUTATIONS:
        test_actual_assert_writer_transition_with_approved_generated_settings(replay_layouts, mutation, cases)
    for revision in UNTRIMMED_REVISIONS:
        test_untrimmed_99_100_inputs_are_intentionally_rejected(replay_layouts, revision, cases)
    target = Path(os.environ["KMTECH_TEST_CA_EVIDENCE_ROOT"]) / "actual-transition-results.json"
    write_transition_evidence(target, cases)


@pytest.mark.parametrize("damage", ["empty", "code", "status", "counter", "source"])
def test_transition_evidence_rejects_incomplete_or_unbound_collection(tmp_path, damage):
    cases = [{"kind": kind, "status": "PASS" if kind == "approved" else "REJECTED", "code": code}
             for kind, code in EXPECTED_TRANSITION_CODES.items()]
    target = tmp_path / "evidence.json"
    if damage == "empty":
        with pytest.raises(AssertionError):
            write_transition_evidence(target, [])
        assert not target.exists()
        return
    write_transition_evidence(target, cases)
    evidence = json.loads(target.read_bytes())
    if damage == "code":
        evidence["cases"][1]["code"] = "WRONG_FIXED_CODE"
    elif damage == "status":
        evidence["cases"][1]["status"] = "PASS"
    elif damage == "counter":
        evidence["passed_count"] = 0
    elif damage == "source":
        evidence["test_source_sha256"] = "0" * 64
    with pytest.raises(AssertionError):
        validate_transition_evidence(evidence, hashlib.sha256(Path(__file__).read_bytes()).hexdigest())

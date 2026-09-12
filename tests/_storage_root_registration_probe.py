import review_run
from review_run import ROOT, REPO
import ast, hashlib, importlib.util, json, os, subprocess, sys, types
from pathlib import Path
from unittest.mock import patch
import current_user_onboarding as current
import label_match_single_instance as current_guard
from user_relay import _resolve_scan_source_dir

def load_file(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module

fixtures = load_file('review_registration_fixtures', REPO / 'tests/test_current_user_onboarding.py')

def baseline(name, filename):
    module = types.ModuleType(name)
    module.__file__ = str(REPO / filename)
    sys.modules[name] = module
    source = subprocess.check_output(['git', 'show', 'e45ec3e:' + filename], cwd=REPO).decode('utf-8-sig')
    exec(compile(source, module.__file__ + '@e45ec3e', 'exec'), module.__dict__)
    return module

old = baseline('registration_baseline_onboard', 'current_user_onboarding.py')
old_guard = baseline('registration_baseline_guard', 'label_match_single_instance.py')

def compare(label, onboard, guard):
    root = ROOT / 'registration-fixtures' / label
    local, program = root / 'local', root / 'program'
    local_data = local / 'KMTech/Label_Match/data'
    legacy_data = program / 'KMTech/Label_Match/data'
    legacy_data.mkdir(parents=True)
    existing = legacy_data / 'old.csv'
    existing.write_bytes(b'existing legacy completion CSV')
    settings = local / 'KMTech/Label_Match/config/app_settings.json'
    fixtures._write_json(settings, {'operator_name': 'synthetic-review'})
    before = hashlib.sha256(settings.read_bytes()).hexdigest()
    original_csv = hashlib.sha256(existing.read_bytes()).hexdigest()
    environment = {'LOCALAPPDATA': str(local), 'ProgramData': str(program)}
    observed = {}

    def category(path):
        p = Path(path)
        if p == local_data or p.parent == local_data:
            return 'A'
        if p == legacy_data or p.parent == legacy_data:
            return 'P'
        return str(p)

    def registration(paths):
        observed['registration_argument'] = category(paths.data_root)
        fixtures._ready_state(paths)
        observed['right_after_registration'] = category(onboard.resolve_current_user_onboarding_paths(root / 'app', environ=environment).data_root)
        return 0

    def relay(_root):
        selected = onboard.resolve_current_user_onboarding_paths(root / 'app', environ=environment)
        observed['relay_before_environment'] = category(_resolve_scan_source_dir('', data_root=selected.data_root, settings_path=selected.settings_path))
        return fixtures._relay_start(_root)

    with patch.dict(os.environ, environment, clear=True):
        report = onboard.onboard_current_user(
            root / 'app', environ=environment, require_bootstrap_integrity=False,
            registration_runner=registration, profile_loader=fixtures._profile_loader,
            credential_loader=fixtures._credential_loader, ledger_factory=fixtures._ledger_factory,
            autostart_installer=fixtures._autostart,
            scheduled_task_remover=fixtures._scheduled_task_absent,
            legacy_task_quiescence_reader=fixtures._legacy_task_quiescent,
            relay_launcher=relay,
        )
    observed.update(
        report_status=report['status'], report_data_root=category(report['data_root']),
        ledger=category(report['ledger_path']),
        applied_environment=category(environment['LABEL_MATCH_SAVE_DIR']),
        guard_after_onboarding=category(guard.resolve_data_scope(environment=environment, settings_path=settings)),
    )
    fresh = {'LOCALAPPDATA': str(local), 'ProgramData': str(program)}
    restart = onboard.resolve_current_user_onboarding_paths(root / 'app', environ=fresh)
    observed.update(
        fresh_restart=category(restart.data_root), fresh_restart_ledger=category(restart.ledger_path),
        settings_hash_unchanged=hashlib.sha256(settings.read_bytes()).hexdigest() == before,
        legacy_csv_hash_unchanged=hashlib.sha256(existing.read_bytes()).hexdigest() == original_csv,
    )
    return observed

results = {'base': compare('base', old, old_guard), 'head': compare('head', current, current_guard)}
(ROOT / 'registration-comparison.json').write_text(json.dumps(results, indent=2), encoding='utf-8')
print(json.dumps(results, indent=2), flush=True)
for key in ('right_after_registration', 'relay_before_environment', 'applied_environment', 'guard_after_onboarding', 'fresh_restart'):
    assert results['head'][key] == results['base'][key] == 'A', f'Legacy registration transition differs at {key}'
assert all(row['settings_hash_unchanged'] and row['legacy_csv_hash_unchanged'] for row in results.values())

"""Administrator-only orphan exits use real audit Save and a real fence gate."""
import copy
import hashlib
import json
import re
from pathlib import Path
import shutil
import subprocess

import pytest

import writer_session_fence as fence
from tests.test_terminal_audit_orphan_fence import (
    ROOT, _powershell_batch, _startup_fence_gate, carried, failed_install,
    fence_only_removal,
)


@pytest.mark.parametrize('engine', ('powershell', 'pwsh'), ids=('PS51', 'PS7'))
def test_saved_terminal_audits_admin_release_finishes_without_replaying(carried, tmp_path, monkeypatch, engine):
    control, payload, audit, original = carried
    rows = _powershell_batch(tmp_path, engine, audit, control, original)
    seeds = []
    for index, row in enumerate(rows):
        saved = Path(row.pop('copy')).read_bytes()
        record = json.loads(saved.decode('utf-8-sig'))
        case_root = tmp_path / ('admin-case-' + str(index))
        install = case_root / 'managed/current'
        install.mkdir(parents=True)
        for name in ('INSTALL_THIS_PC.ps1', 'tools/bootstrap_integrity.ps1', 'tools/label_writer_fence.ps1', 'app/payload.txt'):
            path = install / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b'terminal-candidate-fixture\n')
        active = copy.deepcopy(payload)
        active['status'] = row['fence_status']
        # Native/registry/ACL observation is isolated; the installer Save,
        # inventory, owner identity, publication and fence gates remain real.
        record.update(install_root=str(install), code_state='UNKNOWN', code_preimage='ABSENT',
                      rollback={'applied': False}, source_commit='a' * 40)
        seeds.append(dict(case_root=str(case_root), audit=record, active=active, row=row,
                          owner_created=original['writer_fence']['owner_process_started_at_filetime'], layout="machine" if row["lane"] == "machine" else "user"))
    seed = tmp_path / 'admin-seeds.json'
    seed.write_text(json.dumps(seeds), encoding='utf-8')
    output = tmp_path / 'admin-results.json'
    driver = ROOT / 'tests/terminal_orphan_admin_driver.ps1'
    result = subprocess.run([
        shutil.which(engine), '-NoLogo', '-NoProfile', '-NonInteractive',
        '-ExecutionPolicy', 'Bypass', '-File', str(driver), '-Repo', str(ROOT),
        '-Seed', str(seed), '-Output', str(output),
    ], capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    diagnostics = {'rc': result.returncode, 'stdout_bytes': len(result.stdout), 'stderr_bytes': len(result.stderr),
                   'error_ids': re.findall(rb'FullyQualifiedErrorId\s*:\s*([A-Za-z0-9_.]+)', result.stderr),
                   'source_lines': re.findall(rb'(?:Line\s*\||\.ps1:)([0-9]+)', result.stderr)}
    diagnostics = {key: [x.decode('ascii') for x in value] if isinstance(value, list) else value
                   for key, value in diagnostics.items()}
    (tmp_path / 'admin-driver-diagnostics.json').write_text(json.dumps(diagnostics), encoding='utf-8')
    assert result.returncode == 0, 'isolated administrative driver failed'
    results = json.loads(output.read_text(encoding='utf-8-sig'))
    for item in results:
        item['startup_open'] = False
        if item['released']:
            read_active = fence.active_fence
            with monkeypatch.context() as patch:
                patch.setattr(fence, 'active_fence', lambda **kwargs: read_active(Path(item['control'])))
                item['startup_open'] = _startup_fence_gate() != 'ACTIVE_WRITER_FENCE'
    (tmp_path / 'admin-final-results.json').write_text(json.dumps(results, indent=2), encoding='utf-8')
    assert len(results) == 49
    machine_rows = [item for item, seed in zip(results, seeds) if seed['layout'] == 'machine']
    assert machine_rows
    assert all(item['audit_in_machine_root'] and not item['audit_in_operator_root'] for item in machine_rows)
    assert all(item['audit_unchanged'] and item['replay_count'] == 0 for item in results)
    assert all(item['released'] and item['reinstall_open'] and item['startup_open'] and
               item['idempotent'] and item['subsequent_fence_preserved'] for item in results), 'administrator exit did not finish for every Save site'


def _contract_case(tmp_path, index, original, payload, mode, code_class='CANDIDATE'):
    # Keep native PS5.1 evidence names below its ordinary-path limit.
    tag = hashlib.sha256(str(tmp_path).encode('utf-8')).hexdigest()[:8]
    root = tmp_path.parent / ('o-' + tag + '-' + str(index))
    assert not root.exists()
    install = root / 'managed/current'
    install.parent.mkdir(parents=True)
    if code_class != 'ABSENT':
        for name in ('INSTALL_THIS_PC.ps1', 'tools/bootstrap_integrity.ps1',
                     'tools/label_writer_fence.ps1', 'app/payload.txt'):
            path = install / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b'terminal-result-fixture\n')
    audit = copy.deepcopy(original)
    audit.update(schema='label-match-canonical-portable-install-v1', status='RECOVERY_REQUIRED',
                 install_root=str(install), source_commit='a' * 40, code_state='UNKNOWN',
                 code_preimage='ABSENT', rollback={'applied': False})
    return dict(case_root=str(root), audit=audit, active=copy.deepcopy(payload),
                mode=mode, code_class=code_class,
                row={'site': 0, 'status': 'RECOVERY_REQUIRED', 'fence_status': payload['status']})


def _contract_driver(tmp_path, engine, seeds):
    seed = tmp_path / 'contract-seeds.json'
    seed.write_text(json.dumps(seeds), encoding='utf-8')
    output = tmp_path / 'contract-results.json'
    result = subprocess.run([
        shutil.which(engine), '-NoLogo', '-NoProfile', '-NonInteractive',
        '-ExecutionPolicy', 'Bypass', '-File', str(ROOT / 'tests/terminal_orphan_admin_driver.ps1'),
        '-Repo', str(ROOT), '-Seed', str(seed), '-Output', str(output),
    ], capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    diagnostics = {'rc': result.returncode, 'stdout_bytes': len(result.stdout), 'stderr_bytes': len(result.stderr),
                   'error_ids': [x.decode('ascii') for x in re.findall(rb'FullyQualifiedErrorId\s*:\s*([A-Za-z0-9_.]+)', result.stderr)],
                   'source_lines': [x.decode('ascii') for x in re.findall(rb'(?:Line\s*\||\.ps1:)([0-9]+)', result.stderr)]}
    (tmp_path / 'contract-driver-diagnostics.json').write_text(json.dumps(diagnostics), encoding='utf-8')
    assert result.returncode == 0, 'contract driver preparation failed'
    return json.loads(output.read_text(encoding='utf-8-sig'))


@pytest.mark.parametrize('engine', ('powershell', 'pwsh'), ids=('PS51', 'PS7'))
def test_admin_release_refuses_unproven_or_contradictory_results(carried, tmp_path, engine):
    _, payload, _, original = carried
    modes = ['tuple_' + field for field in (
        'session_id', 'attempt_id', 'replacement_transaction_id', 'writer_inventory_sha256',
        'orchestrator_sha256', 'writer_contract_sha256', 'session_started_at_utc',
    )]
    modes += [
        'census_saved_descendant', 'census_owner_unreadable_birth',
        'free_authority_handle',
        'owner_live', 'owner_birth', 'protected_unreadable', 'census_helper', 'census_bootstrap',
        'census_encoded', 'census_nested', 'census_unreadable', 'census_failure', 'census_descendant',
        'known_absent', 'candidate_restored', 'installed_retained', 'absent_present',
        'preimage_changed', 'inventory_changed', 'run_changed', 'stop_changed', 'task_changed',
        'pending_changed', 'shortcut_changed', 'audit_corrupt', 'owner_corrupt', 'release_corrupt',
        'save_before_failure', 'fence_swap', 'equal_unknown', 'child_result_mismatch',
        'child_result_corrupt', 'child_result_reused', 'child_result_live', 'duplicate_owner',
        'known_run_unreadable', 'known_task_unreadable',
    ]
    seeds = [_contract_case(tmp_path, index, original, payload, mode,
                            'INSTALLED' if mode in ('installed_retained', 'task_changed', 'known_task_unreadable')
                            else 'ABSENT' if mode == 'absent_present' else 'CANDIDATE')
             for index, mode in enumerate(modes)]
    results = _contract_driver(tmp_path, engine, seeds)
    assert len(results) == len(modes)
    assert all(not item['released'] and item['audit_unchanged'] and item['replay_count'] == 0 for item in results)
    assert all(item['foreign_fence_preserved'] if item['mode'] == 'fence_swap'
               else item['fence_unchanged'] for item in results), 'rejection must retain the observed fence'


@pytest.mark.parametrize('engine', ('powershell', 'pwsh'), ids=('PS51', 'PS7'))
def test_admin_release_result_classes_and_interrupted_publication(carried, tmp_path, engine):
    _, payload, _, original = carried
    scenes = [
        ('normal', 'INSTALLED'), ('normal', 'ABSENT'), ('equal_candidate', 'CANDIDATE'),
        ('equal_installed', 'INSTALLED'), ('census_idle', 'CANDIDATE'), ('census_self', 'CANDIDATE'),
        ('census_unrelated', 'CANDIDATE'), ('child_result_absent', 'CANDIDATE'),
        ('record_stop_once', 'CANDIDATE'), ('clear_once', 'CANDIDATE'), ('final_save_once', 'CANDIDATE'),
        ('minimal_released', 'CANDIDATE'), ('minimal_record_stop_once', 'CANDIDATE'),
        ('minimal_final_save_once', 'CANDIDATE'), ('unknown_artifacts', 'CANDIDATE'), ('operator_edit', 'CANDIDATE'),
    ]
    seeds = [_contract_case(tmp_path, index, original, payload, mode, code_class)
             for index, (mode, code_class) in enumerate(scenes)]
    results = _contract_driver(tmp_path, engine, seeds)
    assert len(results) == len(scenes)
    assert all(item['released'] and item['reinstall_open'] and item['idempotent'] and
               item['subsequent_fence_preserved'] and
               item['audit_unchanged'] and item['writer_race_refused'] and item['replay_count'] == 0
               for item in results), 'valid result or interrupted publication failed to settle'
    assert {item['observed_code_class'] for item in results} == {'CANDIDATE', 'INSTALLED', 'ABSENT'}
    for item in results:
        if item['mode'] in ('record_stop_once', 'clear_once', 'final_save_once',
                            'minimal_record_stop_once', 'minimal_final_save_once'):
            assert item['first_failure_proven'], 'the interrupted stage did not retain its required evidence'
        if item['mode'] == 'operator_edit':
            assert item['operator_edit_preserved']
        if item['mode'] == 'unknown_artifacts':
            assert item['unknown_artifacts_preserved']
        assert item['events'].index('SAVE_RELEASING') < item['events'].index('DELETE_FENCE')


@pytest.mark.parametrize('engine', ('powershell', 'pwsh'), ids=('PS51', 'PS7'))
def test_failed_audit_without_residual_fence_retains_original_resume_path(carried, tmp_path, engine):
    _, payload, _, original = carried
    seeds = [_contract_case(tmp_path, index, original, payload, 'legacy_no_fence') for index in range(2)]
    for seed, status in zip(seeds, ('ROLLBACK_FAILED', 'FAILED_ROLLBACK_INCOMPLETE')):
        seed['audit']['status'] = status
    results = _contract_driver(tmp_path, engine, seeds)
    assert all(item['legacy_path_reached'] and item['audit_unchanged'] for item in results)

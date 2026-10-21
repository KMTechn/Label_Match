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
                          owner_created=original['writer_fence']['owner_process_started_at_filetime']))
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
    assert all(item['audit_unchanged'] and item['replay_count'] == 0 for item in results)
    assert all(item['released'] and item['reinstall_open'] and item['startup_open'] and
               item['idempotent'] for item in results), 'administrator exit did not finish for every Save site'

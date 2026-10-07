"""Run the actual production removal entry in fresh engines with native adapters."""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

import pytest

from tests.test_r4_official_relay import FAMILIES, vector_input
from tests.test_r4_support import engine

ROOT = Path(__file__).resolve().parents[1]


def run_entry(tmp_path, engine, scenario, repo=ROOT):
    family = next(name for name, row in FAMILIES.items()
                  if row['identity']['principal.user'] == 'SYSTEM')
    payload = vector_input({'family': family, 'mode': 'remove'})
    if scenario == 'elevation-log':
        payload['tasks'] = []
    source = tmp_path / 'input.json'
    source.write_text(json.dumps(payload), encoding='utf-8')
    environment = os.environ.copy()
    environment.update(TEMP=str(tmp_path), TMP=str(tmp_path),
                       LOCALAPPDATA=str(tmp_path), APPDATA=str(tmp_path), PROGRAMDATA=str(tmp_path))
    command = [engine, '-NoProfile', '-NonInteractive', '-WindowStyle', 'Hidden', '-File',
               str(ROOT / 'tests/fixtures/uninstall-entry-driver.ps1'),
               '-Repo', str(repo), '-Root', str(tmp_path), '-InputPath', str(source), '-Scenario', scenario]
    result = subprocess.run(command, cwd=tmp_path, env=environment, capture_output=True,
                            creationflags=0x08000000)
    (tmp_path / 'entry.stdout').write_bytes(result.stdout)
    (tmp_path / 'entry.stderr').write_bytes(result.stderr)
    return result


@pytest.mark.parametrize('scenario', ['legacy', 'elevation-log', 'query', 'export', 'query-sidecar-failure'])
def test_production_uninstall_loads_reachable_definitions(tmp_path, engine, scenario):
    result = run_entry(tmp_path, engine, scenario)
    public = result.stdout.decode('utf-8', 'replace') + result.stderr.decode('utf-8', 'replace')
    adapter = json.loads((tmp_path / 'adapter.json').read_text())
    assert not any(row['exception_type'] == 'System.Management.Automation.CommandNotFoundException'
                   and row['command'] != 'Get-Command'
                   for row in adapter['errors']), str(tmp_path)
    assert 'CommandNotFoundException' not in public, str(tmp_path)
    if scenario in ('legacy', 'elevation-log'):
        assert result.returncode == 0, str(tmp_path)
        assert 'uninstall_status=PASS_CODE_REMOVED_STATE_PRESERVED' in public
        marker = json.loads((tmp_path / 'removed.json').read_text(encoding='utf-8-sig'))
        assert marker['lifecycle_state'] == 'REMOVED'
        if scenario == 'legacy':
            preimages = list((tmp_path / 'removal-evidence').glob('*/official-legacy-relay/preimage.xml'))
            assert len(preimages) == 1
            assert preimages[0].with_suffix('.json').is_file()
            assert adapter['enabled'] == [False]
            assert adapter['events'].count('disable') == 1
        else:
            records = [json.loads(line) for line in (tmp_path / 'elevation.jsonl').read_text().splitlines()]
            assert records[0]['status'] == 'STARTED'
            assert 'report_sha256' in records[0]
    else:
        assert result.returncode == 1
        assert 'Legacy scheduler diagnostic:' in public
        assert 'SYNTHETIC_NATIVE_CAUSE_409' not in public
        assert not (tmp_path / 'removed.json').exists()
        if scenario == 'query-sidecar-failure':
            assert 'sidecar_sha256=unavailable' in public
        else:
            evidence = list(tmp_path.glob('label-legacy-native-diagnostic-*/native-exception.txt'))
            assert any('SYNTHETIC_NATIVE_CAUSE_409' in path.read_text() for path in evidence)


def test_uninstall_redaction_pins_match_packet_bytes():
    source = (ROOT / 'INSTALL_THIS_PC.ps1').read_text(encoding='utf-8-sig')
    block = source.split('$UninstallRedactionPins = [ordered]@{', 1)[1].split('}', 1)[0]
    pins = dict(re.findall(r"'([^']+)'\s*=\s*'([0-9a-f]{64})'", block))
    assert set(pins) == {'tools/failure_redaction.ps1', 'tools/failure_redaction_rules.json'}
    for path, expected in pins.items():
        assert hashlib.sha256((ROOT / path).read_bytes()).hexdigest() == expected


@pytest.mark.parametrize('relative', ['tools/failure_redaction.ps1', 'tools/failure_redaction_rules.json'])
def test_uninstall_rejects_changed_diagnostics_before_loading(tmp_path, engine, relative):
    packet = tmp_path / 'packet'
    for name in ('INSTALL_THIS_PC.ps1', 'tools/failure_redaction.ps1', 'tools/failure_redaction_rules.json'):
        target = packet / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, target)
    with (packet / relative).open('ab') as stream:
        stream.write(b'\nSYNTHETIC_UNTRUSTED_DIAGNOSTIC_409')
    result = run_entry(tmp_path, engine, 'legacy', repo=packet)
    assert result.returncode == 1
    public = result.stderr.decode('utf-8', 'replace')
    record = json.loads(public.split('LABEL_MATCH_INSTALLER_FAILURE=', 1)[1])
    assert record['reason_code'] == 'INSTALLER_FAILED'
    assert record['report_status'] == 'REPORT_UNAVAILABLE'
    assert record['location_module'] == 'INSTALL_THIS_PC.ps1'
    assert record['location_function'] == 'ScriptBlock'
    assert record['location_line'] > 0
    assert 'SYNTHETIC_UNTRUSTED_DIAGNOSTIC_409' not in public
    adapter = json.loads((tmp_path / 'adapter.json').read_text())
    assert adapter['events'] == []
    assert not (tmp_path / 'removed.json').exists()

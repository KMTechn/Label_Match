"""Original machine-audit binding and native encoded-command spellings (LM-5)."""
import base64
import json
import shutil
import subprocess

import pytest

from tests.test_terminal_audit_orphan_fence import carried, failed_install, fence_only_removal
from tests.test_terminal_orphan_admin_release import _contract_case, _contract_driver


SPELLINGS = ['-' + 'encodedcommand'[:size] for size in range(1, 15)] + ['-ec']
SPELLINGS += [dash + name for dash in ('/', '\u2013', '\u2014', '\u2015')
              for name in ('e', 'ec', 'encodedcommand')]
SPELLINGS += ['-EC', '-Ec', '-EncodedCommand']
ENGINES = pytest.mark.parametrize('engine', ('powershell', 'pwsh'), ids=('ps51', 'ps7'))


@ENGINES
def test_machine_original_audit_rejects_other_bindings(carried, tmp_path, engine):
    _, payload, _, original = carried
    modes = ['owner_other_control', 'owner_other_tuple', 'fence_other_control',
             'audit_other_root', 'install_other', 'machine_audit_operator_fence',
             'operator_audit_machine_fence']
    seeds = [_contract_case(tmp_path, i, original, payload, mode) for i, mode in enumerate(modes)]
    for seed in seeds:
        seed['layout'] = 'machine'
    seeds[-1]['layout'] = 'user'
    results = _contract_driver(tmp_path, engine, seeds)
    assert len(results) == len(modes)
    assert all(not row['released'] and row['audit_unchanged'] and row['fence_unchanged']
               and row['replay_count'] == 0 for row in results)
    assert all(row['reason'] == 'ORPHAN_AUDIT_ROOT_DIFFERS' for row in results[-2:])


@ENGINES
def test_encoded_command_spellings_related_and_native(carried, tmp_path, engine):
    _, payload, _, original = carried
    encoded = base64.b64encode('exit 7'.encode('utf-16-le')).decode('ascii')
    seeds = []
    for index, spelling in enumerate(SPELLINGS):
        completed = subprocess.run([shutil.which(engine), '-NoProfile', '-NonInteractive',
                                    spelling, encoded], capture_output=True,
                                   creationflags=subprocess.CREATE_NO_WINDOW)
        assert completed.returncode == 7, 'native encoded-command spelling was rejected'
        seed = _contract_case(tmp_path, index, original, payload, 'census_spelling')
        seed.update(layout='user', spelling=spelling)
        seeds.append(seed)
    rows = _contract_driver(tmp_path, engine, seeds)
    assert len(rows) == len(SPELLINGS)
    assert all(not row['released'] and row['audit_unchanged'] and row['fence_unchanged']
               and row['replay_count'] == 0 and row['reason'] == 'ORPHAN_CENSUS_RELATED_ALIVE'
               for row in rows)


@ENGINES
def test_encoded_command_spellings_unrelated(carried, tmp_path, engine):
    _, payload, _, original = carried
    seeds = []
    for index, spelling in enumerate(SPELLINGS):
        seed = _contract_case(tmp_path, index, original, payload, 'census_spelling_unrelated')
        seed.update(layout='user', spelling=spelling)
        seeds.append(seed)
    rows = _contract_driver(tmp_path, engine, seeds)
    assert len(rows) == len(SPELLINGS)
    assert all(row['released'] and row['reinstall_open'] and row['idempotent']
               and row['audit_unchanged'] and row['replay_count'] == 0 for row in rows)


@ENGINES
def test_encoded_command_nested_bare_quoted_and_unreadable(carried, tmp_path, engine):
    _, payload, _, original = carried
    scenes = [('census_nested_unquoted', '-ec'), ('census_spelling', '-ec'),
              ('census_undecodable', '-ec')]
    seeds = []
    for index, (mode, spelling) in enumerate(scenes):
        seed = _contract_case(tmp_path, index, original, payload, mode)
        seed.update(layout='user', spelling=spelling)
        seed['quote_payload'] = index == 1
        seeds.append(seed)
    rows = _contract_driver(tmp_path, engine, seeds)
    assert len(rows) == len(scenes)
    assert all(not row['released'] and row['audit_unchanged'] and row['fence_unchanged']
               and row['replay_count'] == 0 for row in rows)
    assert rows[-1]['reason'] == 'ORPHAN_CENSUS_UNREADABLE'


@ENGINES
def test_encoded_command_invalid_charset_base64_and_utf16_at_each_depth(carried, tmp_path, engine):
    _, payload, _, original = carried
    invalid = ['@@@@', 'AA?A', 'ABC', base64.b64encode(b'\x00').decode('ascii'),
               base64.b64encode(b'\x00\xd8').decode('ascii')]
    scenes = [(argument, depth) for argument in invalid for depth in range(3)]
    scenes += [(quote + '@@@@' + quote, 1) for quote in ('"', "'")]
    seeds = []
    for index, (argument, depth) in enumerate(scenes):
        command = 'powershell.exe -NoProfile -ec ' + argument
        for _ in range(depth):
            command = 'powershell.exe -NoProfile -ec ' + base64.b64encode(
                command.encode('utf-16-le')).decode('ascii')
        seed = _contract_case(tmp_path, index, original, payload, 'census_command')
        seed.update(command_line=command)
        seeds.append(seed)
    rows = _contract_driver(tmp_path, engine, seeds)
    assert len(rows) == len(scenes)
    assert all(not row['released'] and row['fence_unchanged'] and row['audit_unchanged']
               and row['replay_count'] == 0 and row['reason'] == 'ORPHAN_CENSUS_UNREADABLE'
               for row in rows)


@ENGINES
def test_encoded_command_without_argument_keeps_existing_census_behavior(carried, tmp_path, engine):
    _, payload, _, original = carried
    codes = []
    for spelling in SPELLINGS:
        completed = subprocess.run([shutil.which(engine), '-NoProfile', '-NonInteractive', spelling],
                                   capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
        codes.append(completed.returncode)
    (tmp_path / 'native-missing-argument.json').write_text(json.dumps({
        'engine': engine, 'cases': len(codes), 'all_rejected': all(codes),
        'exit_codes': sorted(set(codes)),
    }), encoding='utf-8')
    assert all(code != 0 for code in codes), 'native engine accepted a missing encoded-command argument'
    seeds = []
    for index, spelling in enumerate(('-ec', '-encodedcommand')):
        seed = _contract_case(tmp_path, index, original, payload, 'census_command')
        seed.update(command_line='powershell.exe -NoProfile ' + spelling)
        seeds.append(seed)
    rows = _contract_driver(tmp_path, engine, seeds)
    assert len(rows) == len(seeds)
    assert all(row['released'] and row['idempotent'] and row['reinstall_open']
               and row['audit_unchanged'] and row['replay_count'] == 0 for row in rows)

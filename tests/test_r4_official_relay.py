"""Public-tag identity vectors and scheduler transitions; no live native targets."""
import copy
import hashlib
import json
import ntpath
import xml.etree.ElementTree as ET

import pytest

from tests.test_r4_support import FIXTURES, engine, fingerprint, run_driver

NS = {'t': 'http://schemas.microsoft.com/windows/2004/02/mit/task'}
PREFLIGHT_REFUSAL = 'Healthy lifecycle task belongs to another '
REMOVAL_REFUSAL = 'Refusing to remove a historical task with different '
MANIFEST = json.loads((FIXTURES / 'official-identities.json').read_text(encoding='utf-8'))
TABLE = json.loads((FIXTURES / 'expected-outcomes.json').read_text(encoding='utf-8'))
FAMILIES = {row['id']: row for row in MANIFEST['families']}
FIELD_PATHS = {
    'action.command': 't:Actions/t:Exec/t:Command',
    'action.arguments': 't:Actions/t:Exec/t:Arguments',
    'action.workdir': 't:Actions/t:Exec/t:WorkingDirectory',
    'principal.user': 't:Principals/t:Principal/t:UserId',
    'principal.logon_type': 't:Principals/t:Principal/t:LogonType',
    'principal.run_level': 't:Principals/t:Principal/t:RunLevel',
    'trigger.repetition_interval': 't:Triggers/t:TimeTrigger/t:Repetition/t:Interval',
}


def identity_from_xml(task):
    """Independent oracle for these explicit synthetic definitions, not a product parser."""
    tree = ET.fromstring(task['xml'])
    actions = list(tree.find('t:Actions', NS))
    triggers = list(tree.find('t:Triggers', NS))
    action = actions[0]
    trigger = triggers[0]
    principal = tree.find('t:Principals/t:Principal', NS)
    user = principal.findtext('t:UserId', '', NS)
    label = ('SYSTEM' if user == 'S-1-5-18' else 'CURRENT_USER'
             if user == MANIFEST['same_user_sid'] else 'OTHER_USER')
    command = action.findtext('t:Command', '', NS)
    arguments = action.findtext('t:Arguments', '', NS).strip()
    arguments = ' '.join(arguments.split())
    # These fixtures have precisely two non-path wscript switches and one path.
    pieces = arguments.split(' ', 2)
    if len(pieces) == 3:
        arguments = ' '.join(pieces[:2]) + ' ' + ntpath.normpath(pieces[2].strip('"')).lower()
    return {'task.path': task['path'].replace('/', '\\').rstrip('\\').lower() + '\\',
            'task.name': task['name'].lower(), 'action.count': len(actions),
            'action.type': action.tag.rsplit('}', 1)[-1],
            'action.command': (ntpath.join(MANIFEST['system_directory'], command.strip('"')).lower()
                               if command.lower() == 'wscript.exe' else ntpath.normpath(command.strip('"')).lower() if command else ''),
            'action.arguments': arguments,
            'action.workdir': action.findtext('t:WorkingDirectory', '', NS).lower(),
            'principal.user': label, 'principal.logon_type': principal.findtext('t:LogonType', '', NS),
            'principal.run_level': principal.findtext('t:RunLevel', '', NS),
            'trigger.count': len(triggers), 'trigger.type': trigger.tag.rsplit('}', 1)[-1],
            'trigger.repetition_interval': trigger.findtext('t:Repetition/t:Interval', '', NS)}


def vector_input(vector):
    family = FAMILIES[vector['family']]
    tree = ET.fromstring((FIXTURES / family['xml']).read_text(encoding='utf-8'))
    task = {'name': family['identity']['task.name'], 'path': '\\', 'enabled': True, 'state': 'Ready'}
    mutation = vector.get('mutation') or {}
    field = mutation.get('field')
    if field in FIELD_PATHS:
        node = tree.find(FIELD_PATHS[field], NS)
        replacements = {'action.command': 'cscript.exe', 'action.arguments': '//B //NoLogo C:\\R4\\foreign.vbs',
                        'action.workdir': 'C:\\R4', 'principal.user': 'S-1-5-21-111-222-333-1002',
                        'principal.logon_type': 'S4U', 'principal.run_level': 'HighestAvailable',
                        'trigger.repetition_interval': 'PT2M'}
        node.text = replacements[field]
    elif field == 'task.path':
        task['path'] = '\\Foreign\\'
    elif field == 'task.name':
        task['name'] = task['name'][:-1] + ('0' if task['name'][-1] != '0' else '1')
    elif field in ('action.count', 'trigger.count'):
        parent = tree.find('t:Actions' if field == 'action.count' else 't:Triggers', NS)
        parent.append(copy.deepcopy(parent[0]))
    elif field == 'action.type':
        parent = tree.find('t:Actions', NS); parent.remove(parent[0])
        handler = ET.SubElement(parent, '{' + NS['t'] + '}ComHandler')
        ET.SubElement(handler, '{' + NS['t'] + '}ClassId').text = '{00000000-0000-0000-0000-000000000001}'
    elif field == 'trigger.type':
        parent = tree.find('t:Triggers', NS); parent.remove(parent[0])
        trigger = ET.SubElement(parent, '{' + NS['t'] + '}CalendarTrigger')
        ET.SubElement(trigger, '{' + NS['t'] + '}StartBoundary').text = '2026-10-05T00:00:00'
        daily = ET.SubElement(trigger, '{' + NS['t'] + '}ScheduleByDay')
        ET.SubElement(daily, '{' + NS['t'] + '}DaysInterval').text = '1'
    record_only = mutation.get('record_only')
    paths = {'Author': 't:RegistrationInfo/t:Author', 'Date': 't:RegistrationInfo/t:Date',
             'StartBoundary': 't:Triggers/t:TimeTrigger/t:StartBoundary', 'URI': 't:RegistrationInfo/t:URI'}
    if record_only in paths:
        tree.find(paths[record_only], NS).text = ('2027-01-01T00:00:00' if record_only in ('Date', 'StartBoundary')
                                                else 'SYNTHETIC_CHANGED_VALUE')
    elif record_only == 'Settings':
        ET.SubElement(tree.find('t:Settings', NS), '{' + NS['t'] + '}Hidden').text = 'true'
    elif record_only == 'schema_version':
        tree.set('version', '1.3')
    scenario = mutation.get('scenario')
    if scenario == 'disabled':
        task['enabled'] = False
        tree.find('t:Settings/t:Enabled', NS).text = 'false'
    if scenario == 'running':
        task['state'] = 'Running'
    if scenario == 'other-user':
        tree.find(FIELD_PATHS['principal.user'], NS).text = 'S-1-5-21-111-222-333-1002'
    if scenario == 'not-in-any-family':
        tree.find(FIELD_PATHS['principal.logon_type'], NS).text = 'S4U'
    task['xml'] = ET.tostring(tree, encoding='unicode')
    tasks = [task, copy.deepcopy(task)] if scenario == 'multiple' else [task]
    return {'tasks': tasks, 'expected_identities': [row['identity'] for row in FAMILIES.values()],
            'same_user_sid': MANIFEST['same_user_sid'], 'computer_name': MANIFEST['computer_name'],
            'source_host_id': MANIFEST['source_host_id'], 'program_data_root': MANIFEST['program_data_root'],
            'mode': vector['mode'], 'generation_allowed': scenario != 'rollback-refused',
            'query_failure': scenario == 'query-failure'}


@pytest.mark.parametrize('vector', TABLE['vectors'], ids=lambda row: row['id'])
def test_official_identity_and_retained_task_transition(tmp_path, engine, vector):
    payload = vector_input(vector)
    input_path = tmp_path / 'input.json'
    input_path.write_text(json.dumps(payload), encoding='utf-8')
    record = run_driver(tmp_path, engine, 'relay-driver.ps1', InputPath=input_path)
    assert record['seam_available'], 'R4 official relay seams missing from product; see SEAMS-NEEDED.md'
    assert record['register_calls'] == record['delete_calls'] == 0, 'retain definition; change Enabled without password'
    if not vector['proceed']:
        if vector['reason_code']:
            prefix = REMOVAL_REFUSAL if vector['mode'] == 'remove' else PREFLIGHT_REFUSAL
            assert vector['reason_code'] == prefix
            assert record['reason_code'].startswith(prefix)
        assert record['task_count'] == len(payload['tasks'])
        assert record['old_enabled'] == [row['enabled'] for row in payload['tasks']]
        assert not record['new_relay_enabled']
        assert not any(event in record['events'] for event in ('disable', 'enable', 'new_relay_start'))
        if vector['reason_code']:
            assert not record['transition']['preimage_saved'], 'identity rejection precedes preimage storage'
        return
    assert not record['reason_code']
    assert record['task_count'] == 1
    observed_identity = identity_from_xml(payload['tasks'][0])
    assert fingerprint(observed_identity) == fingerprint(FAMILIES[vector['family']]['identity'])
    full, fields = fingerprint(observed_identity)
    assert record['identity_audit']['full_fingerprint'] == full
    assert record['identity_audit']['field_fingerprints'] == fields
    transition = record['transition']
    assert transition['preimage_saved']
    assert transition['metadata_fingerprint'] == full
    assert transition['preimage_xml_sha256'] == hashlib.sha256(payload['tasks'][0]['xml'].encode('utf-8')).hexdigest()
    assert transition['preimage_enabled'] is payload['tasks'][0]['enabled']
    scenario = (vector.get('mutation') or {}).get('scenario')
    if vector['mode'] in ('install', 'remove'):
        assert record['old_enabled'] == [False]
        assert record['new_relay_enabled'] is (vector['mode'] == 'install')
        assert transition['stop_confirmed']
        if payload['tasks'][0]['enabled']:
            assert record['events'].index('xml_read') < record['events'].index('disable')
    elif scenario in ('rollback-allowed', 'disabled'):
        assert record['old_enabled'] == [payload['tasks'][0]['enabled']]
        assert transition['full_fingerprint'] == full
    else:
        assert record['old_enabled'] == [False]
        assert not record['new_relay_enabled']
        # Korean G guidance is asserted against actual G/catch in test_r4_rollback.
    assert not (any(record['old_enabled']) and record['new_relay_enabled'])


def test_every_available_public_tag_has_an_explicit_expected_set():
    by_tag = {row['tag']: row for row in MANIFEST['tags']}
    assert len(by_tag) == MANIFEST['public_release_count'] == 53
    assert {row['tag'] for row in TABLE['tags']} == set(by_tag)
    assert len([row for row in by_tag.values() if row['relay_creator']]) == 42
    for row in TABLE['tags']:
        assert row['expected_families'] == by_tag[row['tag']]['expected_families']
        for family in row['expected_families']:
            assert row['tag'] in FAMILIES[family]['tags']
    assert len(FAMILIES) == 6
    assert not MANIFEST['hkcu_run_source_hits'], 'revisit HKCU Run vectors if a public writer is discovered'


def test_fingerprint_contract_uses_sorted_lf_utf8_bytes():
    import hashlib
    full, fields = fingerprint({'z': 'PT1M', 'a': 'Exec'})
    assert full == hashlib.sha256(b'a=Exec\nz=PT1M\n').hexdigest()
    assert fields['a'] == hashlib.sha256(b'a=Exec\n').hexdigest()
    assert full != hashlib.sha256(b'a=Exec\r\nz=PT1M\r\n').hexdigest()


@pytest.mark.parametrize('vector', TABLE['vectors'], ids=lambda row: row['id'])
def test_vectors_are_parseable_and_each_identity_field_is_covered(vector):
    payload = vector_input(vector)
    for task in payload['tasks']:
        ET.fromstring(task['xml'])
    covered = {row['mutation']['field'] for row in TABLE['vectors'] if (row.get('mutation') or {}).get('field')}
    assert covered == set(next(iter(FAMILIES.values()))['identity'])
    identity = identity_from_xml(payload['tasks'][0])
    field = (vector.get('mutation') or {}).get('field')
    if field:
        assert identity[field] != FAMILIES[vector['family']]['identity'][field]
        assert all(identity != row['identity'] for row in FAMILIES.values()), 'mutation matched another approved family'
    elif (vector.get('mutation') or {}).get('record_only'):
        assert fingerprint(identity) == fingerprint(FAMILIES[vector['family']]['identity'])

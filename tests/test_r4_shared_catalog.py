"""Shared per-release template entries versus the actual product expectation getter."""
import json
import ntpath
import os
import re
from pathlib import Path

from tests.test_r4_official_relay import FAMILIES, MANIFEST
from tests.test_r4_support import FIXTURES, engine, fingerprint, run_driver

DEFAULT_CATALOG = FIXTURES / 'relay-identity' / 'Label_Match'


def bind_entry(entry, context):
    fields = {name: str(value).format_map(context) for name, value in entry['field_templates'].items()}
    for name in ('task.name', 'task.path'):
        fields[name] = fields[name].lower()
    fields['task.path'] = fields['task.path'].replace('/', '\\').rstrip('\\') + '\\'
    for name in ('action.command', 'action.workdir'):
        value = fields[name].strip().strip('"').replace('/', '\\')
        if name == 'action.command' and value.lower() in ('wscript.exe', 'cscript.exe', 'powershell.exe'):
            value = ntpath.join(context['system_directory'], value)
        fields[name] = ntpath.normpath(value).lower() if value else ''
    pieces = re.findall(r'"[^"\r\n]*"|\S+', fields['action.arguments'].strip())
    normalized = []
    for token in pieces:
        quoted = token.startswith('"') and token.endswith('"')
        value = token[1:-1] if quoted else token
        if re.match(r'^(?:[A-Za-z]:[\\/]|\\\\[^\\]+\\[^\\]+)', value):
            value = ntpath.normpath(value.replace('/', '\\')).lower()
        normalized.append('"' + value + '"' if quoted else value)
    fields['action.arguments'] = ' '.join(normalized)
    fields['principal.run_level'] = {'Limited': 'LeastPrivilege', 'Highest': 'HighestAvailable'}.get(
        fields['principal.run_level'], fields['principal.run_level'])
    return fields


def test_product_constants_match_source_families(tmp_path, engine):
    record = run_driver(tmp_path, engine, 'catalog-driver.ps1')
    assert record['seam_available'], 'product expectation getter absent at 464830d'
    expected = {fingerprint(row['identity'])[0] for row in FAMILIES.values()}
    actual = {row['full_fingerprint'] for row in record['entries']}
    assert actual == expected, 'product constants differ from published source identities/schema labels'


def shared_expected_fingerprints(root):
    paths = sorted(root.glob('*.json'))
    assert paths, 'shared relay identity entries have not arrived; source/guest owners must supply them'
    releases = {row['tagName'] for row in json.loads((FIXTURES / 'release-index.json').read_text(encoding='utf-8-sig'))}
    creator_tags = {row['tag'] for row in MANIFEST['tags'] if row['relay_creator']}
    context = {'system_directory': MANIFEST['system_directory'], 'program_data': r'C:\ProgramData',
               'user_local_app_data': r'C:\R4\Local', 'safe_computer_name': 'R4-FIXTURE-PC',
               'machine_suffix': MANIFEST['source_host_id'].rsplit('-', 1)[1],
               'label_match_source_host_id': MANIFEST['source_host_id'], 'label_match_code_root': r'C:\R4\Code',
               'container_audit_code_root': r'C:\R4\Container', 'current_user': 'CURRENT_USER'}
    expected = set(); seen = set()
    for path in paths:
        entry = json.loads(path.read_text(encoding='utf-8-sig'))
        assert entry['schema'] == 'kmtech-old-relay-identity-entry-v1'
        assert entry['app'] == 'Label_Match' and entry['tag'] in releases
        assert entry['mechanism'] == 'scheduled_task', 'unexpected public HKCU Run writer: extend the vectors'
        assert set(entry['field_templates']) == set(next(iter(FAMILIES.values()))['identity'])
        assert set(entry['source_refs']) == set(entry['field_templates'])
        fields = bind_entry(entry, context)
        for family in FAMILIES.values():
            if fields == {name: str(value) for name, value in family['identity'].items()}:
                fields = family['identity']
                break
        expected.add(fingerprint(fields)[0]); seen.add(entry['tag'])
    assert creator_tags <= seen, 'published creator tags are missing from the shared template set'
    return expected


def test_shared_entries_match_source_fixture_families():
    root = Path(os.environ.get('R4_OFFICIAL_IDENTITY_CATALOG', str(DEFAULT_CATALOG)))
    expected = shared_expected_fingerprints(root)
    assert expected == {fingerprint(row['identity'])[0] for row in FAMILIES.values()}


def test_shared_entries_match_product_constants(tmp_path, engine):
    root = Path(os.environ.get('R4_OFFICIAL_IDENTITY_CATALOG', str(DEFAULT_CATALOG)))
    expected = shared_expected_fingerprints(root)
    record = run_driver(tmp_path, engine, 'catalog-driver.ps1')
    assert record['seam_available'], 'product expectation getter absent at 464830d'
    assert {row['full_fingerprint'] for row in record['entries']} == expected

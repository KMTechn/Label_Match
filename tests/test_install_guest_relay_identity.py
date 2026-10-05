"""Actual scheduler-export XML, separate from synthetic source fixtures."""
import hashlib
import json
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from tests.test_shared_ancestor_protection import run_functions
from tests.test_r4_support import engine, fingerprint

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / 'tests/fixtures/guest_relay'
ENTRY = json.loads((FIXTURE / 'v2.0.38-expected.json').read_text(encoding='utf-8-sig'))
NS = {'t': 'http://schemas.microsoft.com/windows/2004/02/mit/task'}


def test_guest_item_is_exact_and_matches_published_product_template():
    raw = (FIXTURE / 'v2.0.38-expected.json').read_bytes()
    assert hashlib.sha256(raw).hexdigest() == '52b1d63613e260ddc5667ba644b163984b4bc731b556cebc1927a879cf7c9cab'
    entries = json.loads((ROOT / 'tests/fixtures/official_legacy_relay_sources.json').read_text())
    exact = [e for e in entries if e['tag'] == ENTRY['tag'] and e['branch'] == ENTRY['branch']]
    assert len(exact) == 1
    assert exact[0]['field_templates'] == ENTRY['field_templates']
    full, fields = fingerprint(ENTRY['normalized_fields'])
    assert full == ENTRY['sha256']
    assert fields == ENTRY['field_sha256']


@pytest.mark.parametrize('explicit_defaults', [False, True])
def test_guest_xml_and_explicit_default_variant_have_same_product_fingerprint(tmp_path, engine, explicit_defaults):
    source = FIXTURE / 'Label_Match-v2.0.38.xml'
    assert source.read_bytes().startswith(b'\xff\xfe')
    text = source.read_text(encoding='utf-16')
    tree = ET.fromstring(text)
    principal = tree.find('t:Principals/t:Principal', NS)
    assert principal.find('t:RunLevel', NS) is None
    assert principal.findtext('t:UserId', '', NS) == 'FIXTURE_CURRENT_USER'
    if explicit_defaults:
        ET.SubElement(principal, '{' + NS['t'] + '}RunLevel').text = 'LeastPrivilege'
        text = ET.tostring(tree, encoding='unicode')
    input_path = tmp_path / 'input.xml'
    input_path.write_text(text, encoding='utf-8')
    expected = tmp_path / 'expected.json'
    expected.write_text(json.dumps(ENTRY['normalized_fields']), encoding='utf-8')
    output = tmp_path / 'result.json'
    result = run_functions(tmp_path, ROOT / 'INSTALL_THIS_PC.ps1', [
        'ConvertTo-LabelLegacyTaskPath', 'Get-LabelOfficialLegacyTaskFingerprint'], r'''
$expected=Get-Content -LiteralPath $env:LM_GUEST_EXPECTED -Raw -Encoding UTF8 | ConvertFrom-Json
$map=[ordered]@{};foreach($property in $expected.PSObject.Properties){$map[$property.Name]=[string]$property.Value}
function Get-LabelOfficialLegacyTaskExpectations { return $map }
function ConvertTo-LabelTaskSid {param($Identity)
 if($Identity -ceq 'FIXTURE_CURRENT_USER'){return [Security.Principal.WindowsIdentity]::GetCurrent().User.Value}
 throw 'Unsanitized or different principal in guest fixture'
}
$xml=Get-Content -LiteralPath $env:LM_GUEST_XML -Raw -Encoding UTF8
$result=Get-LabelOfficialLegacyTaskFingerprint $map['task.name'] $map['task.path'] $xml
[IO.File]::WriteAllText($env:LM_GUEST_OUTPUT,($result | ConvertTo-Json -Depth 5),[Text.UTF8Encoding]::new($false))
''', engine=engine, values={'LM_GUEST_EXPECTED': str(expected), 'LM_GUEST_XML': str(input_path), 'LM_GUEST_OUTPUT': str(output)})
    assert result.returncode == 0, 'Guest fingerprint driver failed; inspect isolated fixture root'
    actual = json.loads(output.read_text())
    assert actual['matched'] is True
    assert actual['sha256'] == ENTRY['sha256']
    assert actual['fields'] == ENTRY['field_sha256']

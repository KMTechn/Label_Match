"""Exception prefixes cannot hide subsequent fields in real diagnostic sinks."""
import json
from pathlib import Path
import uuid

import pytest

import failure_redaction
import label_match_product_host as product_host
from tools import direct_sync_relay_runner
from tests.test_label_match_product_host import (  # noqa: F401
    isolated_data_generation_release, raw_arguments,
)
from tests.test_shared_ancestor_protection import run_functions
from tests.test_failure_redaction import VECTORS, check, generated

ROOT = Path(__file__).resolve().parents[1]
PREFIXES = ('RuntimeError', 'ValueError', 'OSError', 'ApplicationFault', 'traceback')
FORMS = ('assignment', 'mapping', 'quoted-assignment', 'json')


def diagnostic(prefix, form, sentinel):
    value = {
        'assignment': 'password=' + sentinel,
        'mapping': "{'password': '" + sentinel + "'}",
        'quoted-assignment': "secret='" + sentinel + "'",
        'json': json.dumps({'password': sentinel}),
    }[form]
    context = ('Traceback (most recent call last):\n  File "probe.py", line 1\nRuntimeError'
               if prefix == 'traceback' else prefix)
    return context + ': ' + value


@pytest.mark.parametrize('prefix', PREFIXES)
@pytest.mark.parametrize('form', FORMS)
def test_exception_prefix_has_zero_secret_bytes_in_real_sinks(
    monkeypatch, tmp_path, raw_arguments, prefix, form,
):
    sentinel = uuid.uuid4().hex + uuid.uuid4().hex
    text = diagnostic(prefix, form, sentinel)
    protected = failure_redaction.redact_for_sink(text, environ={})
    assert (sentinel in protected) is False
    assert '[REDACTED:field]' in protected

    runtime = tmp_path / 'status' / 'runtime.json'
    log = tmp_path / 'logs' / 'runtime.jsonl'

    def fail(_arguments, **_prepared_inputs):
        raise RuntimeError(text)

    monkeypatch.setattr(direct_sync_relay_runner, 'main', fail)
    result = product_host.dispatch_product_mode([
        product_host.DIRECT_SYNC_RELAY_MODE, *raw_arguments,
        '--runtime-status-path', str(runtime), '--log-path', str(log),
    ])
    assert result == product_host.HOSTED_RELAY_FAILURE_EXIT_CODE
    for path in (runtime, log):
        assert (sentinel.encode() in path.read_bytes()) is False
        assert 'RuntimeError' in path.read_text(encoding='utf-8')

    # The standalone evidence writer sees only the explicit synthetic environment.
    with monkeypatch.context() as synthetic:
        synthetic.setattr(failure_redaction.os, 'environ', {})
        reference = failure_redaction.write_failure_evidence(text)
    evidence = Path(reference['path']).read_bytes()
    assert (sentinel.encode() in evidence) is False
    assert '[REDACTED:field]'.encode() in evidence
    assert len(evidence) <= failure_redaction.RULES['limit']


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
def test_exception_prefix_masks_powershell_and_capture_conversion(tmp_path, engine):
    sentinel = uuid.uuid4().hex + uuid.uuid4().hex
    cases = [dict(name=prefix + '-' + form, text=diagnostic(prefix, form, sentinel))
             for prefix in PREFIXES for form in FORMS]
    inputs = tmp_path / 'prefix-input.json'
    inputs.write_text(json.dumps(dict(synthetic=sentinel, cases=cases)), encoding='utf-8')
    result = run_functions(tmp_path, ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1',
        ['Initialize-LabelChildOutputReader', 'Protect-LabelFailureText', 'ConvertTo-LabelFailureOutput'], r'''
function Get-KmtechSinkEnvironment { return @{} }
Initialize-LabelChildOutputReader
$inputRecord=Get-Content -LiteralPath $env:LM_PREFIX_INPUT -Raw -Encoding UTF8 | ConvertFrom-Json
foreach($case in $inputRecord.cases) {
    $protected=ConvertTo-KmtechRedactedSinkText -Text ([string]$case.text) -Environment @{}
    if($protected.Contains([string]$inputRecord.synthetic)) { throw ('Text exposure: '+$case.name) }
    $memory=[IO.MemoryStream]::new([Text.Encoding]::UTF8.GetBytes([string]$case.text),$false)
    $reader=[IO.StreamReader]::new($memory,[Text.Encoding]::UTF8)
    try {
        $capture=[LabelChildOutputReader]::Capture($reader)
        $capture.Completion.Wait()
        $converted=ConvertTo-LabelFailureOutput ([LabelChildOutputReader]::Snapshot($capture))
        if($converted.text.Contains([string]$inputRecord.synthetic)) { throw ('Converted exposure: '+$case.name) }
    } finally { $reader.Dispose();$memory.Dispose() }
}
'PASS'
''', values={'LM_PREFIX_INPUT': str(inputs)}, engine=engine)
    assert result.returncode == 0, 'prefix matrix failed; inspect task-local log'
    assert result.stdout.strip() == 'PASS'


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
@pytest.mark.parametrize('prefix', ('', 'RuntimeError: '), ids=('plain', 'exception'))
def test_capture_masks_exception_assignment_before_truncation(tmp_path, engine, prefix):
    sentinel = uuid.uuid4().hex + uuid.uuid4().hex
    inputs = tmp_path / 'overflow-input.json'
    inputs.write_text(json.dumps(dict(synthetic=sentinel,
        text=prefix + 'password=' + sentinel * 3000)), encoding='utf-8')
    result = run_functions(tmp_path, ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1',
        ['Initialize-LabelChildOutputReader', 'Protect-LabelFailureText', 'ConvertTo-LabelFailureOutput'], r'''
function Get-KmtechSinkEnvironment { return @{} }
Initialize-LabelChildOutputReader
$inputRecord=Get-Content -LiteralPath $env:LM_PREFIX_INPUT -Raw -Encoding UTF8 | ConvertFrom-Json
$memory=[IO.MemoryStream]::new([Text.Encoding]::UTF8.GetBytes([string]$inputRecord.text),$false)
$reader=[IO.StreamReader]::new($memory,[Text.Encoding]::UTF8)
try {
    $capture=[LabelChildOutputReader]::Capture($reader)
    $capture.Completion.Wait()
    $snapshot=[LabelChildOutputReader]::Snapshot($capture)
    $converted=ConvertTo-LabelFailureOutput $snapshot
    if($snapshot.Text.Contains([string]$inputRecord.synthetic)) { throw 'Capture exposure before truncation' }
    if($converted.text.Contains([string]$inputRecord.synthetic)) { throw 'Converted exposure after truncation' }
    if([Text.Encoding]::UTF8.GetByteCount($converted.text) -gt 65536) { throw 'Capture byte limit exceeded' }
} finally { $reader.Dispose();$memory.Dispose() }
'PASS'
''', values={'LM_PREFIX_INPUT': str(inputs)}, engine=engine)
    assert result.returncode == 0, 'bounded capture regression failed; inspect task-local log'
    assert result.stdout.strip() == 'PASS'


@pytest.mark.parametrize('engine', ('powershell.exe', 'pwsh.exe'))
def test_capture_obeys_the_same_contract_vectors(tmp_path, engine):
    sentinel = uuid.uuid4().hex + uuid.uuid4().hex
    vectors = [*VECTORS,
        dict(id='exception-prefix-password-assignment', environ={},
             text='RuntimeError: password=' + sentinel,
             expected='RuntimeError: password=[REDACTED:field]'),
        dict(id='exception-prefix-single-quoted-mapping', environ={},
             text="RuntimeError: {'password': '" + sentinel + "'}",
             expected="RuntimeError: {'password': '[REDACTED:field]'}"),
    ]
    fill = chr(65 + int(uuid.uuid4().hex[0], 16))
    for prefix, label in (('', 'plain'), ('RuntimeError: ', 'exception-prefix')):
        vectors.append(dict(id='long-password-assignment-' + label, environ={},
            text_generator=dict(head=prefix + 'password=', fill_char=fill, fill_bytes=83200, tail=''),
            expected=prefix + 'password=[REDACTED:field]'))
    cases = [dict(name=vector['id'], environment=vector.get('environ', {}),
                  json_value=vector.get('json'), is_json='json' in vector,
                  text=json.dumps(vector['json'], ensure_ascii=False) if 'json' in vector
                  else vector['text'] if 'text' in vector else generated(vector))
             for vector in vectors]
    inputs, outputs = tmp_path / 'vectors-input.json', tmp_path / 'vectors-output.json'
    inputs.write_text(json.dumps(cases), encoding='utf-8')
    result = run_functions(tmp_path, ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1',
        ['Initialize-LabelChildOutputReader', 'Protect-LabelFailureText', 'ConvertTo-LabelFailureOutput'], r'''
function Get-KmtechSinkEnvironment {
    $result=@{}
    foreach($entry in $script:SyntheticEnvironment.PSObject.Properties) {
        if(Test-KmtechSecretEnvironmentValue -Name $entry.Name -Value ([string]$entry.Value)) {
            $result[$entry.Name]=[string]$entry.Value
        }
    }
    return $result
}
$rows=@()
foreach($case in (Get-Content -LiteralPath $env:LM_PREFIX_INPUT -Raw -Encoding UTF8 | ConvertFrom-Json)) {
    $script:SyntheticEnvironment=$case.environment
    $environment=@{}
    foreach($entry in $case.environment.PSObject.Properties) {
        $environment[$entry.Name]=[string]$entry.Value
    }
    $direct=if($case.is_json) {
        ConvertTo-KmtechRedactedSinkObject -Value $case.json_value -Environment $environment |
            ConvertTo-Json -Depth 20 -Compress
    } else {
        ConvertTo-KmtechRedactedSinkText -Text ([string]$case.text) -Environment $environment
    }
    Initialize-LabelChildOutputReader
    $memory=[IO.MemoryStream]::new([Text.Encoding]::UTF8.GetBytes([string]$case.text),$false)
    $reader=[IO.StreamReader]::new($memory,[Text.Encoding]::UTF8)
    try {
        $capture=[LabelChildOutputReader]::Capture($reader)
        $capture.Completion.Wait()
        $converted=ConvertTo-LabelFailureOutput ([LabelChildOutputReader]::Snapshot($capture))
        $rows+=@{name=$case.name;text=$converted.text;direct_text=$direct}
    } finally { $reader.Dispose();$memory.Dispose() }
}
[IO.File]::WriteAllText($env:LM_PREFIX_OUTPUT,($rows|ConvertTo-Json -Depth 20),[Text.UTF8Encoding]::new($false))
'PASS'
''', values={'LM_PREFIX_INPUT': str(inputs), 'LM_PREFIX_OUTPUT': str(outputs)}, engine=engine)
    assert result.returncode == 0, 'capture vector driver failed; inspect task-local log'
    rows = json.loads(outputs.read_text(encoding='utf-8'))
    assert len(rows) == len(vectors)
    for vector, row in zip(vectors, rows):
        python_actual = (failure_redaction.redact_json_for_sink(vector['json'], vector['environ'])
                         if 'json' in vector else failure_redaction.redact_for_sink(
                             vector['text'] if 'text' in vector else generated(vector), vector['environ']))
        check(vector, python_actual)
        direct_actual = json.loads(row['direct_text']) if 'expected_json' in vector else row['direct_text']
        check(vector, direct_actual)
        actual = json.loads(row['text']) if 'expected_json' in vector else row['text']
        # The product's final JSON sink formats valid JSON, while retaining
        # exactly the same values; free text and all boundary properties stay exact.
        if 'expected' in vector:
            try:
                expected_json = json.loads(vector['expected'])
            except json.JSONDecodeError:
                pass
            else:
                assert json.loads(row['text']) == expected_json
                assert len(row['text'].encode('utf-8')) <= failure_redaction.RULES['limit']
                continue
        check(vector, actual)


def test_metadata_keys_stay_literal_and_many_prefixes_do_not_recurse():
    sentinel = uuid.uuid4().hex
    text = 'Context: ' * 2000 + 'secret=' + sentinel
    assert (sentinel in failure_redaction.redact_for_sink(text, environ={})) is False
    for key in ('credential_path', 'delegation_token_sha256'):
        value = "RuntimeError: {'password': '" + sentinel + "'}"
        text = key + '=' + json.dumps(value)
        assert failure_redaction.redact_for_sink(text, environ={}) == text

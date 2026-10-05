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


def test_metadata_keys_stay_literal_and_many_prefixes_do_not_recurse():
    sentinel = uuid.uuid4().hex
    text = 'Context: ' * 2000 + 'secret=' + sentinel
    assert (sentinel in failure_redaction.redact_for_sink(text, environ={})) is False
    for key in ('credential_path', 'delegation_token_sha256'):
        value = "RuntimeError: {'password': '" + sentinel + "'}"
        text = key + '=' + json.dumps(value)
        assert failure_redaction.redact_for_sink(text, environ={}) == text

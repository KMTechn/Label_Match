import contextlib
import hashlib
import json
import re
from pathlib import Path
import secrets
import shutil
import subprocess

import pytest

import auto_update
import current_user_onboarding
import failure_redaction

ROOT = Path(__file__).resolve().parents[1]


SHAPES = ('header', 'assignment', 'json', 'proxy_header', 'proxy_assignment', 'nested_json',
          'header_newline', 'scheme_newline', 'value_newline', 'mixed_header')

SCHEMES = ('Bearer', 'Basic', 'bEaReR', 'bAsIc')
SCHEMES += ('Token', 'tOkEn', 'Negotiate', 'nEgOtIaTe')

def sample(shape, scheme):
    material = secrets.token_urlsafe(32) + '=='
    name = 'Proxy-Authorization' if shape.startswith('proxy_') else 'Authorization'
    if shape == 'mixed_header':
        name = 'AUTHORIZATION'
    value = scheme + ' ' + material
    if shape in ('json', 'nested_json'):
        text = json.dumps({name: value})
        if shape == 'nested_json':
            text = 'native failure: ' + text
    else:
        separator = '=' if shape.endswith('assignment') else ': '
        text = name + separator + value
        if shape == 'header_newline':
            text = name + ':\r\n' + value
        elif shape == 'scheme_newline':
            text = name + ': ' + scheme + '\r\n' + material
        elif shape == 'value_newline':
            text += '\r\nnative failure: access denied'
    return text, material

def require_absent(material, value):
    raw = value if isinstance(value, bytes) else str(value).encode('utf-8')
    if material.encode('utf-8') in raw:
        pytest.fail('authorization credential remains', pytrace=False)

@pytest.mark.parametrize('shape', SHAPES)
@pytest.mark.parametrize('scheme', SCHEMES)
def test_authorization_common_sink(shape, scheme):
    text, material = sample(shape, scheme)
    require_absent(material, failure_redaction.redact_for_sink(text, environ={}))
    padded = secrets.token_hex(16) + '=='
    require_absent(padded, failure_redaction.redact_for_sink('Authorization: ' + scheme + '\r\n' + padded, {}))
    word = RuntimeError.__name__
    assert word not in failure_redaction.redact_for_sink('Authorization: ' + scheme + '\r\n' + word, {})


@pytest.mark.parametrize('shape', SHAPES)
@pytest.mark.parametrize('scheme', SCHEMES)
@pytest.mark.parametrize('link', ('__cause__', '__context__'))
def test_deep_failure_details_and_streams(tmp_path, shape, scheme, link):
    text, material = sample(shape, scheme)
    native = OSError(5, text)
    native.winerror = 32
    native.exit_code = 7
    native.returncode = 7
    native.stdout = text
    native.stderr = text
    middle, outer = RuntimeError('middle failure'), RuntimeError('outer failure')
    setattr(outer, link, middle)
    setattr(middle, link, native)
    require_absent(material, json.dumps(failure_redaction.protected_failure_details(outer, {})))
    output = auto_update.child_failure_output(native)
    require_absent(material, json.dumps(output))
    assert output['returncode'] == 7
    details = auto_update.launch_failure_details(outer, include_original=True)
    assert len(details) == 3
    plain = auto_update.launch_failure_details(outer)
    assert plain[2]['type'] == type(native).__module__ + '.' + type(native).__name__
    assert plain[2]['win32_error'] == 32
    assert native.errno == 5 and native.exit_code == 7
    assert details[2]['exception_type'] == plain[2]['type']
    for detail in details:
        raw = Path(detail['report_path']).read_bytes()
        require_absent(material, raw)
        assert hashlib.sha256(raw).hexdigest() == detail['report_sha256']
        private = json.loads(raw)
        reference = private['full_exception_trace']
        require_absent(material, Path(reference['path']).read_bytes())
    private = json.loads(Path(details[2]['report_path']).read_bytes())
    assert private['child_output']['returncode'] == 7
    for key in ('stdout_evidence', 'stderr_evidence'):
        reference = private['child_output'][key]
        require_absent(material, Path(reference['path']).read_bytes())


@pytest.mark.parametrize('shape', SHAPES)
@pytest.mark.parametrize('scheme', SCHEMES)
def test_real_support_report_readback(tmp_path, monkeypatch, shape, scheme):
    text, material = sample(shape, scheme)
    error = RuntimeError(text)
    path = tmp_path / 'support.json'
    monkeypatch.setattr(current_user_onboarding, 'writer_admission', lambda *_a, **_k: contextlib.nullcontext())
    current_user_onboarding._publish_failure_report(path, {'status': 'FAILED'}, error)
    if not path.is_file():
        pytest.fail('support report was not published', pytrace=False)
    raw = path.read_bytes()
    require_absent(material, raw)
    assert json.loads(raw)['status'] == 'FAILED'
    assert hashlib.sha256(raw).hexdigest() == error._failure_report_ref['sha256']


@pytest.mark.parametrize('scheme', SCHEMES)
def test_adjacent_native_diagnostics_and_missing_credential(scheme):
    text, material = sample(SHAPES[0], scheme)
    prefix = 'OSError errno=5 winerror=32 exit_code=7; '
    suffix = '\nwinerror=87 errno=13 exit_code=9'
    safe = failure_redaction.redact_for_sink(prefix + text + suffix, {})
    require_absent(material, safe)
    assert safe.startswith(prefix) and safe.endswith(suffix)
    incomplete = 'Authorization: ' + scheme + suffix
    expected = (re.sub(r'^(\s*)\S+', lambda match: match[1] + failure_redaction.RULES['field'], suffix)
                if scheme.lower() == 'bearer' else suffix)
    assert failure_redaction.redact_for_sink(incomplete, {}).endswith(expected)


def test_ordinary_scheme_words_remain():
    text = 'Basic is available; authentication uses the word Bearer'
    assert failure_redaction.redact_for_sink(text, {}) == text


@pytest.mark.parametrize('scheme', SCHEMES)
@pytest.mark.parametrize('line_end', ('\n', '\r\n'))
def test_unindented_next_line_is_not_an_authorization_value(scheme, line_end):
    for diagnostic in ('ordinary diagnostic words; errno=5 winerror=32 exit_code=7',
                       'errno=5', 'winerror=32', 'exit_code=7'):
        suffix = line_end + diagnostic
        safe = failure_redaction.redact_for_sink('Authorization: ' + scheme + suffix, {})
        # ANSWER-4 restores the legacy Bearer rule across every whitespace gap.
        expected = (re.sub(r'^(\s*)\S+', lambda match: match[1] + failure_redaction.RULES['field'], suffix)
                    if scheme.lower() == 'bearer' else suffix)
        assert safe.endswith(expected)


@pytest.mark.parametrize('engine', ('powershell', 'pwsh'))
def test_powershell_schemes_failure_details_and_support_readback(tmp_path, engine):
    executable = shutil.which(engine)
    if executable is None:
        pytest.fail('required PowerShell engine unavailable', pytrace=False)
    cases = []
    for shape in SHAPES:
        for scheme in SCHEMES:
            text, material = sample(shape, scheme)
            cases.append(dict(shape=shape, scheme=scheme, text=text, material=material, fragments=[material]))
    for scheme in SCHEMES:
        padded = secrets.token_hex(16) + '=='
        cases.append(dict(shape='crlf-padding', scheme=scheme, text='Authorization: ' + scheme + '\r\n' + padded,
                          material=padded, fragments=[padded]))
        for folded in (False, True):
            fragments = [secrets.token_urlsafe(24), secrets.token_urlsafe(24)]
            material = ':'.join(fragments)
            gap = '\r\n\t' if folded else ' '
            cases.append(dict(shape='legacy-folded' if folded else 'legacy-inline', scheme=scheme,
                              text='Authorization: ' + scheme + gap + material,
                              material=material, fragments=fragments))
    cases_path = tmp_path / 'cases.json'
    cases_path.write_text(json.dumps(cases), encoding='utf-8')
    script = tmp_path / 'probe.ps1'
    script.write_text(r'''
param([string]$Repo,[string]$Cases)
$ErrorActionPreference='Stop'
trap {
    $diagnostic=@{exception_type=$_.Exception.GetType().FullName;category=$_.CategoryInfo.Category.ToString();line=$_.InvocationInfo.ScriptLineNumber;null_message=($_.Exception.Message -like '*null*');directory_message=($_.Exception.Message -like '*directory*');path_message=($_.Exception.Message -like '*path*');unsupported_message=($_.Exception.Message -like '*support*')}
    [IO.File]::WriteAllText((Join-Path $PSScriptRoot 'driver-error.json'),($diagnostic|ConvertTo-Json -Compress))
    exit 9
}
. (Join-Path $Repo 'tools/invoke_embedded_python.ps1')
$rows=@()
$inputs=ConvertFrom-Json -InputObject ([IO.File]::ReadAllText($Cases))
foreach($case in $inputs){
    $safe=ConvertTo-KmtechRedactedSinkText -Text $case.text -Environment @{}
    $native=[ComponentModel.Win32Exception]::new(32,[string]$case.text)
    $middle=[InvalidOperationException]::new('middle failure',$native)
    $outer=[Exception]::new('outer failure',$middle)
    $details=New-InstallerFailureDiagnostic -CommandIdentity 'probe' -ChildExitCode 7 -FailureCode 'CHILD_EXCEPTION' -Exception $outer
    $trace=[IO.File]::ReadAllText($details.full_exception_trace.path)
    $sink=Write-InstallerFailureEvidence -Text $case.text
    $readback=[IO.File]::ReadAllText($sink.path)
    $actualBytes=[IO.File]::ReadAllBytes($sink.path)
    $expectedBytes=[Text.Encoding]::UTF8.GetBytes((ConvertTo-KmtechRedactedSinkText -Text $case.text))
    $bytesMatch=$actualBytes.Length -eq $expectedBytes.Length
    if($bytesMatch){for($index=0;$index -lt $actualBytes.Length;$index++){if($actualBytes[$index] -ne $expectedBytes[$index]){$bytesMatch=$false;break}}}
    $bounds=$bytesMatch -and $actualBytes.Length -le 65536 -and $sink.limit_bytes -eq 65536 -and -not $sink.truncated
    $prefix='OSError errno=5 winerror=32 exit_code=7; '
    $suffix="`nwinerror=87 errno=13 exit_code=9"
    $adjacent=ConvertTo-KmtechRedactedSinkText -Text ($prefix+$case.text+$suffix) -Environment @{}
    $incomplete=ConvertTo-KmtechRedactedSinkText -Text ('Authorization: '+$case.scheme+$suffix) -Environment @{}
    $normalLine="`nordinary diagnostic words; errno=5 winerror=32 exit_code=7"
    $normalCrlf="`r"+$normalLine
    $nextLines=(ConvertTo-KmtechRedactedSinkText -Text ('Authorization: '+$case.scheme+$normalLine) -Environment @{}).EndsWith($normalLine) -and (ConvertTo-KmtechRedactedSinkText -Text ('Authorization: '+$case.scheme+$normalCrlf) -Environment @{}).EndsWith($normalCrlf)
    $assignmentLine="`r`nwinerror=87"
    $nextLines=$nextLines -and (ConvertTo-KmtechRedactedSinkText -Text ('Authorization: '+$case.scheme+$assignmentLine) -Environment @{}).EndsWith($assignmentLine)
    if($case.scheme -ieq 'Bearer'){
        $nextLines=$true
        foreach($line in @($normalLine,$normalCrlf,$assignmentLine)){
            $expected=[regex]::Replace($line,'^(\s*)\S+',('$1'+$Script:KmtechRedactionRules.field))
            $nextLines=$nextLines -and (ConvertTo-KmtechRedactedSinkText -Text ('Authorization: '+$case.scheme+$line) -Environment @{}).EndsWith($expected)
        }
    }
    $commonLeak=@($case.fragments|Where-Object {$safe.Contains([string]$_)}).Count -gt 0
    $detailsLeak=@($case.fragments|Where-Object {$trace.Contains([string]$_)}).Count -gt 0
    $supportLeak=@($case.fragments|Where-Object {$readback.Contains([string]$_)}).Count -gt 0
    $incompleteSuffix=if($case.scheme -ieq 'Bearer'){[regex]::Replace($suffix,'^(\s*)\S+',('$1'+$Script:KmtechRedactionRules.field))}else{$suffix}
    $rows+=@{shape=$case.shape;scheme=$case.scheme;common_leak=$commonLeak;details_leak=$detailsLeak;support_leak=$supportLeak;diagnostics=($adjacent.StartsWith($prefix) -and $adjacent.EndsWith($suffix) -and $incomplete.EndsWith($incompleteSuffix));native=($native.NativeErrorCode -eq 32 -and $details.child_exit_code -eq 7 -and $details.inner_exception_type -eq 'Win32Exception');sha=((Get-FileHash -LiteralPath $sink.path -Algorithm SHA256).Hash.ToLowerInvariant() -ceq $sink.sha256);bounds=$bounds;next_lines=$nextLines}
}
$ordinary='Basic is available; authentication uses the word Bearer'
@{rows=$rows;ordinary=((ConvertTo-KmtechRedactedSinkText -Text $ordinary -Environment @{}) -ceq $ordinary)}|ConvertTo-Json -Depth 8 -Compress
''', encoding='utf-8-sig')
    completed = subprocess.run([executable, '-NoProfile', '-NonInteractive', '-File', str(script),
                                '-Repo', str(ROOT), '-Cases', str(cases_path)],
                               capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=290)
    if completed.returncode:
        pytest.fail('PowerShell probe failed', pytrace=False)
    result = json.loads(completed.stdout)
    # Probe evidence contains shapes and booleans only, never source material.
    (tmp_path / 'probe-result.json').write_text(json.dumps(result), encoding='utf-8')
    if any(row['common_leak'] or row['details_leak'] or row['support_leak'] for row in result['rows']):
        pytest.fail('authorization credential remained in a PowerShell sink', pytrace=False)
    for row in result['rows']:
        assert row['diagnostics'] and row['native'] and row['sha'] and row['bounds'] and row['next_lines']
    assert result['ordinary']

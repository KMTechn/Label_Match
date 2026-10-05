"""Common sink vectors plus real child failure artifacts, with synthetic values."""
import json
from pathlib import Path

import pytest

from failure_redaction import redact_for_sink, redact_json_for_sink
from tests.test_shared_ancestor_protection import run_functions

ROOT = Path(__file__).resolve().parents[1]
VECTORS = json.loads((ROOT / "tests/fixtures/failure_redaction_vectors.json").read_text(encoding="utf-8"))["vectors"]


def generated(vector):
    spec = vector["text_generator"]
    return spec["head"] + spec["fill_char"] * spec["fill_bytes"] + spec["tail"]


def check(vector, actual):
    if "expected" in vector:
        assert actual == vector["expected"]
    elif "expected_json" in vector:
        assert actual == vector["expected_json"]
    else:
        import re
        props = vector["expected_properties"]
        assert len(actual.encode("utf-8")) <= props.get("max_utf8_bytes", 65536)
        if "starts_with" in props:
            assert actual.startswith(props["starts_with"])
        if "ends_with" in props:
            assert actual.endswith(props["ends_with"])
        if "contains_regex" in props:
            assert re.search(props["contains_regex"], actual)
        if "not_contains" in props:
            assert props["not_contains"] not in actual


@pytest.mark.parametrize("vector", VECTORS, ids=lambda row: row["id"])
def test_python_sink_contract(vector):
    if "json" in vector:
        actual = redact_json_for_sink(vector["json"], vector["environ"])
    else:
        actual = redact_for_sink(vector.get("text") or generated(vector), vector["environ"])
    check(vector, actual)


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
def test_powershell_sink_contract(tmp_path, engine):
    output = tmp_path / "results.json"
    result = run_functions(tmp_path, ROOT / "INSTALL_CANONICAL_PORTABLE.ps1", ['Protect-LabelFailureText'], r'''
$vectors=Get-Content -LiteralPath $env:LM_VECTOR_FILE -Raw -Encoding UTF8 | ConvertFrom-Json
$results=@()
foreach($v in $vectors.vectors) {
 $values=@{};foreach($p in $v.environ.PSObject.Properties){$values[$p.Name]=$p.Value}
 if($null -ne $v.PSObject.Properties['json']){$actual=ConvertTo-KmtechRedactedSinkObject $v.json $values}
 else{
  $text=[string]$v.text
  if($null -ne $v.PSObject.Properties['text_generator']){$g=$v.text_generator;$text=$g.head+($g.fill_char * $g.fill_bytes)+$g.tail}
  $actual=ConvertTo-KmtechRedactedSinkText $text $values
 }
 $results+=,[ordered]@{id=$v.id;actual=$actual}
}
[IO.File]::WriteAllText($env:LM_VECTOR_OUTPUT,($results|ConvertTo-Json -Depth 20),[Text.UTF8Encoding]::new($false))
''', engine=engine, values={"LM_VECTOR_FILE": str(ROOT / "tests/fixtures/failure_redaction_vectors.json"),
                            "LM_VECTOR_OUTPUT": str(output)})
    assert result.returncode == 0, result.stdout + result.stderr
    for vector, row in zip(VECTORS, json.loads(output.read_text(encoding="utf-8"))):
        check(vector, row["actual"])


def test_multibyte_head_tail_limits():
    value = "처음" + "가" * 50000 + "ValueError: 끝"
    result = redact_for_sink(value, {})
    assert len(result.encode("utf-8")) <= 65536
    assert result.startswith("처음") and result.endswith("ValueError: 끝")


def test_actual_product_failure_sinks_keep_cause_and_zero_secret_bytes(tmp_path):
    result = run_functions(tmp_path, ROOT / "INSTALL_CANONICAL_PORTABLE.ps1",
                           ['Arg', 'Product', 'Initialize-LabelChildOutputReader',
                            'ConvertTo-LabelFailureOutput', 'Protect-LabelFailureText',
                            'New-LabelChildFailure', 'Get-LabelLaunchFailure', 'Save'], r'''
$root=$env:LM_FAILURE_ROOT;$source=Join-Path $root 'packet'
New-Item -ItemType Directory -Path (Join-Path $source 'runtime') | Out-Null
$secret='syntheticOnlyDelegation0987654321'
$env:KMTECH_LABEL_WRITER_DELEGATION_TOKEN=$secret
Add-Type -OutputAssembly (Join-Path $source 'runtime/pythonw.exe') -OutputType ConsoleApplication -TypeDefinition @'
using System;
public static class FailureProbe {
 public static void Main() {
  string value=Environment.GetEnvironmentVariable("KMTECH_LABEL_WRITER_DELEGATION_TOKEN");
  Console.Error.WriteLine("Traceback (most recent call last):");
  for(int n=0;n<40;n++) Console.Error.Write(new String('x',4091)+value+"\n");
  Console.Error.WriteLine("ValueError: final synthetic cause "+value);
  Console.Out.WriteLine("{\"status\":\"FAILED\",\"credential\":{\"secret\":\"syntheticFileValue0987\"}}");
  Environment.Exit(1);
 }
}
'@
$auditPath=Join-Path $root 'audit.json';$summaryPath=Join-Path $root 'failure-summary.json';$guide=Join-Path $root 'guidance.txt'
try{Product $source '--onboard-current-user';throw 'Actual child did not fail'}
catch {
 $failure=Get-LabelLaunchFailure $_ 'forward'
 Save $auditPath ([ordered]@{failure_details=$failure})
 Save $summaryPath ([ordered]@{type=$failure.child_output.lowest_exception_type;message=$failure.child_output.last_line})
 [IO.File]::WriteAllText($guide,$_.Exception.Message,[Text.UTF8Encoding]::new($false))
 $record=$failure.child_output
 if($record.lowest_exception_type -cne 'ValueError' -or $record.last_line -notlike '*final synthetic cause*'){throw 'Final child cause was lost'}
 $files=@($auditPath,$summaryPath,$guide)
 foreach($name in @('stdout','stderr')){
  if(-not $record[$name].full_output.path){throw 'Full child evidence reference absent'}
  $files+=[string]$record[$name].full_output.path
 }
 foreach($path in $files){
  $bytes=[IO.File]::ReadAllBytes($path)
  $text=[Text.Encoding]::UTF8.GetString($bytes)
  if($text.Contains($secret) -or $text.Contains('syntheticFileValue0987')){throw 'Synthetic secret bytes reached a failure sink'}
  if($bytes.Length -gt 65536){throw 'Failure sink exceeded the full-output cap'}
 }
 $stderr=[IO.File]::ReadAllText($record.stderr.full_output.path)
 if(-not $stderr.StartsWith('Traceback') -or -not $stderr.Contains('[TRUNCATED:') -or -not $stderr.Contains('final synthetic cause')){throw 'Bounded child evidence lost head or tail'}
}
'PASS'
''', values={"LM_FAILURE_ROOT": str(tmp_path)})
    assert result.returncode == 0, result.stdout + result.stderr
    assert "syntheticOnlyDelegation0987654321" not in result.stdout + result.stderr

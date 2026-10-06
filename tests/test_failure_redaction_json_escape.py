"""Synthetic JSON spellings must stay secret through parsing and publication."""
import json
from pathlib import Path

import pytest

import failure_redaction as redaction
from tests.test_shared_ancestor_protection import run_functions

ROOT = Path(__file__).resolve().parents[1]


def _escaped(value, style):
    units = value.encode("utf-16-be")
    escapes = ["\\u%04x" % int.from_bytes(units[i:i + 2], "big")
               for i in range(0, len(units), 2)]
    if style == "upper":
        return "".join(item[:2] + item[2:].upper() for item in escapes)
    if style == "mixed":
        return "".join(item[:2] + (item[2:].upper() if i % 2 else item[2:])
                       for i, item in enumerate(escapes))
    if style == "partial":
        return "".join(escapes[i] if i % 2 else char for i, char in enumerate(value))
    return "".join(escapes)


def _case(style):
    secret = "synthetic/Alphaabcdef357"
    if style == "surrogate":
        secret = "synthetic-한글-🚚-357"
    if style == "quoted":
        secret = 'synthetic-"quoted"-\\-357'
    if style == "structured":
        secret = '{"opaque":"synthetic-abcdef357"}'
        spelling = '{"opaque":"' + _escaped("synthetic-abcdef357", "lower") + '"}'
        encoded = json.dumps(spelling, ensure_ascii=True)
    else:
        encoded = '"' + _escaped(secret, style) + '"'
    if style == "slash":
        encoded = json.dumps(secret).replace("/", "\\/")
    text = ('{"cause":"first cause","metadata":{"count":2,"enabled":false},'
            '"nested":[{"message":' + encoded + ',' + encoded + ':"kept"}]}')
    return secret, text


STYLES = ["lower", "upper", "mixed", "partial", "surrogate", "quoted", "structured", "slash"]


def _require_safe(text, secret):
    assert secret not in text, "known synthetic value remains in final text"
    parsed = json.loads(text)
    assert parsed["cause"] == "first cause"
    assert parsed["metadata"] == {"count": 2, "enabled": False}
    assert len(parsed["nested"]) == 1 and "kept" in parsed["nested"][0].values()
    def check(value):
        if isinstance(value, dict):
            for key, item in value.items():
                check(key)
                check(item)
        elif isinstance(value, list):
            for item in value:
                check(item)
        elif isinstance(value, str):
            assert secret not in value, "JSON decoding restored a known synthetic value"
            try:
                nested = json.loads(value)
            except ValueError:
                return
            if nested != value:
                check(nested)
    check(parsed)


@pytest.mark.parametrize("style", STYLES)
@pytest.mark.parametrize("sink", ["text", "object", "evidence"])
def test_python_json_spellings(tmp_path, monkeypatch, style, sink):
    secret, text = _case(style)
    environment = {"SYNTHETIC_API_TOKEN": secret}
    if sink == "object":
        actual = json.dumps(redaction.redact_json_for_sink(json.loads(text), environment), ensure_ascii=False)
    elif sink == "evidence":
        monkeypatch.setattr(redaction.os, "environ", environment)
        monkeypatch.setattr(redaction.tempfile, "tempdir", str(tmp_path))
        reference = redaction.write_failure_evidence(text)
        raw = Path(reference["path"]).read_bytes()
        import hashlib
        assert hashlib.sha256(raw).hexdigest() == reference["sha256"]
        actual = raw.decode("utf-8")
    else:
        actual = redaction.redact_for_sink(text, environment)
    _require_safe(actual, secret)


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
def test_powershell_json_spellings_and_private_sidecar(tmp_path, engine):
    inputs = tmp_path / "inputs.json"
    inputs.write_text(json.dumps([{"secret": _case(style)[0], "text": _case(style)[1]}
                                 for style in STYLES]), encoding="utf-8")
    output = tmp_path / "outputs.json"
    result = run_functions(tmp_path, ROOT / "INSTALL_CANONICAL_PORTABLE.ps1",
                           ["Protect-LabelFailureText"], r'''
$rows=@()
foreach($case in (Get-Content -LiteralPath $env:LM_ESCAPE_INPUT -Raw -Encoding UTF8 | ConvertFrom-Json)) {
 $script:syntheticValues=@{SYNTHETIC_API_TOKEN=[string]$case.secret}
 function Get-KmtechSinkEnvironment {return $script:syntheticValues}
 $text=ConvertTo-KmtechRedactedSinkText -Text $case.text -Environment $script:syntheticValues
 $value=ConvertTo-KmtechRedactedSinkObject -Value ($case.text|ConvertFrom-Json) -Environment $script:syntheticValues
 $canonical=Protect-LabelFailureText $case.text
 $reference=Write-KmtechProtectedFailureSidecar -Text $case.text
 $raw=[IO.File]::ReadAllText($reference.path,[Text.Encoding]::UTF8)
 if((Get-FileHash -LiteralPath $reference.path -Algorithm SHA256).Hash.ToLowerInvariant() -cne $reference.sha256){throw 'Sidecar SHA differs'}
 $rows+=,[ordered]@{text=$text;object=(ConvertTo-Json -InputObject $value -Depth 30 -Compress);canonical=$canonical;sidecar=$raw}
}
[IO.File]::WriteAllText($env:LM_ESCAPE_OUTPUT,(ConvertTo-Json -InputObject $rows -Depth 35),[Text.UTF8Encoding]::new($false))
''', engine=engine, values={"LM_ESCAPE_INPUT": str(inputs), "LM_ESCAPE_OUTPUT": str(output)})
    assert result.returncode == 0, "isolated JSON escape probe failed; evidence retained"
    rows = json.loads(output.read_text(encoding="utf-8-sig"))
    assert len(rows) == len(STYLES)
    for style, row in zip(STYLES, rows):
        for text in row.values():
            _require_safe(text, _case(style)[0])


def test_python_refuses_redacted_key_collision():
    environment = {"SYNTHETIC_API_TOKEN": "synthetic-357"}
    with pytest.raises(ValueError, match="REPORT_UNAVAILABLE"):
        redaction.redact_json_for_sink({"synthetic-357": "one", "[REDACTED:SYNTHETIC_API_TOKEN]": "two"}, environment)


def test_python_unavailable_if_normalization_recreates_an_unmaskable_secret(tmp_path, monkeypatch):
    # Masking this secret would alter the JSON container rather than a leaf.
    secret = '{"opaque":"synthetic357"}'
    text = '{"opaque":"' + _escaped("synthetic357", "mixed") + '"}'
    environment = {"SYNTHETIC_API_TOKEN": secret}
    monkeypatch.setattr(redaction.os, "environ", environment)
    monkeypatch.setattr(redaction.tempfile, "tempdir", str(tmp_path))
    result = redaction.try_write_failure_evidence(text)
    assert result["report_status"] == "REPORT_UNAVAILABLE"
    assert "path" not in result and "sha256" not in result
    assert not list(tmp_path.glob("*/trace.txt"))


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
def test_powershell_refuses_redacted_key_collision(tmp_path, engine):
    result = run_functions(tmp_path, ROOT / "INSTALL_CANONICAL_PORTABLE.ps1", ["Protect-LabelFailureText"], r'''
try {
 ConvertTo-KmtechRedactedSinkObject -Value ([ordered]@{'synthetic-357'='one';'[REDACTED:SYNTHETIC_API_TOKEN]'='two'}) -Environment @{SYNTHETIC_API_TOKEN='synthetic-357'} | Out-Null
 throw 'Collision was accepted'
} catch {if($_.Exception.Message -cne 'REPORT_UNAVAILABLE'){throw};'PASS'}
''', engine=engine)
    assert result.returncode == 0 and result.stdout.strip() == "PASS"


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
def test_powershell_unavailable_if_normalization_changes_json_container(tmp_path, engine):
    result = run_functions(tmp_path, ROOT / "INSTALL_CANONICAL_PORTABLE.ps1", ["Protect-LabelFailureText"], r'''
$script:syntheticValues=@{SYNTHETIC_API_TOKEN='{"opaque":"synthetic357"}'}
function Get-KmtechSinkEnvironment {return $script:syntheticValues}
try {
 Write-KmtechProtectedFailureSidecar -Text '{"opaque":"\u0073\u0079\u006e\u0074\u0068\u0065\u0074\u0069\u0063\u0033\u0035\u0037"}' | Out-Null
 throw 'Unsafe report was accepted'
} catch {if($_.Exception.Message -cne 'REPORT_UNAVAILABLE'){throw};'PASS'}
''', engine=engine)
    assert result.returncode == 0 and result.stdout.strip() == "PASS"

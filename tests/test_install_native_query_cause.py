"""The old refusal is public; native cause text stays in protected evidence."""
from pathlib import Path
import hashlib
import json

import pytest

from tests.test_r4_official_relay import FAMILIES, vector_input
from tests.test_r4_support import FIXTURES, engine, run_driver


@pytest.mark.parametrize("stage", ["query", "export"])
@pytest.mark.parametrize("evidence_failure", [False, True])
def test_native_scheduler_failure_preserves_private_cause(tmp_path, engine, stage, evidence_failure):
    payload = vector_input({"family": next(iter(FAMILIES)), "mode": "install"})
    path = tmp_path / "input.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    secret = "R4_SYNTHETIC_NATIVE_SECRET_TOKEN_274"
    author, sid = "FIXTURE_NATIVE_AUTHOR_274", "FIXTURE_NATIVE_SID_274"
    prefix = (FIXTURES / "relay-driver.ps1").read_text(encoding="utf-8-sig").split("$reason='';")[0]
    prefix = prefix.replace("$ErrorActionPreference='Stop'", "$ErrorActionPreference='Stop'\n. (Join-Path $Repo 'tools/failure_redaction.ps1')", 1)
    body = """
$env:KMTECH_LABEL_WRITER_DELEGATION_TOKEN='__SECRET__'
function __NATIVE__ {
    param($TaskName,$TaskPath,$ErrorAction)
    $script:events.Add('__STAGE__')
    throw [IO.IOException]::new('synthetic native __STAGE__ lower cause: __SECRET__; author=__AUTHOR__; sid=__SID__')
}
__EVIDENCE_FAILURE__
$reason='';$native=$null;$failure=$null
try{Get-LabelOfficialLegacyTaskSnapshot | Out-Null}catch{
    $failure=$_;$reason=$_.Exception.Message;$cursor=$_.Exception
    while($null -ne $cursor){
        if($cursor.Data.Contains('label_legacy_native_failure')){$native=$cursor.Data['label_legacy_native_failure'];break}
        $cursor=$cursor.InnerException
    }
}
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile((Join-Path $Repo 'INSTALL_CANONICAL_PORTABLE.ps1'),[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'Product parse failed'}
foreach($definition in $ast.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -in @('Get-LabelLaunchFailure','Protect-LabelFailureText')},$true)){
    . ([scriptblock]::Create($definition.Extent.Text))
}
$audit=Get-LabelLaunchFailure $failure 'legacy_preflight'
$protected=$false
if($native.Contains('sidecar')){$protected=(Get-Acl -LiteralPath (Split-Path -Parent $native.sidecar.path)).AreAccessRulesProtected}
[ordered]@{reason_code=$reason;native=$native;audit=$audit;protected=$protected;events=$script:events.ToArray();
    register_calls=$script:registerCalls;delete_calls=$script:deleteCalls} | ConvertTo-Json -Depth 10 | Set-Content -LiteralPath $Output -Encoding UTF8
"""
    replacements = {"__SECRET__": secret, "__AUTHOR__": author, "__SID__": sid,
                    "__NATIVE__": "Get-ScheduledTask" if stage == "query" else "Export-ScheduledTask",
                    "__STAGE__": stage,
                    "__EVIDENCE_FAILURE__": "function Write-KmtechProtectedFailureSidecar {throw [IO.IOException]::new('synthetic secondary evidence failure')}" if evidence_failure else ""}
    for key, value in replacements.items():
        body = body.replace(key, value)
    driver = tmp_path / "native-failure.ps1"
    driver.write_text(prefix + body, encoding="utf-8-sig")
    result = run_driver(tmp_path, engine, driver, InputPath=path)
    assert result["reason_code"] == "Healthy lifecycle task belongs to another principal/command."
    assert result["native"]["exception_type"] == "System.IO.IOException"
    assert result["native"]["hresult"] == -2146232800
    assert result["audit"]["legacy_native_failure"] == result["native"]
    assert result["audit"]["exception_type"] == "System.IO.IOException"
    assert result["register_calls"] == result["delete_calls"] == 0
    assert not any(event in result["events"] for event in ("disable", "enable", "new_relay_start"))
    public = (tmp_path / "result.json").read_bytes() + (tmp_path / "driver.stdout").read_bytes() + (tmp_path / "driver.stderr").read_bytes()
    assert all(value.encode() not in public and value.encode("utf-16le") not in public for value in (secret, author, sid))
    if evidence_failure:
        assert "sidecar" not in result["native"]
        assert result["native"]["evidence_failure_type"].endswith("Exception")
        return
    assert result["protected"] is True
    reference = result["native"]["sidecar"]
    raw = Path(reference["path"]).read_bytes()
    assert len(raw) == reference["bytes"] <= 65536
    assert hashlib.sha256(raw).hexdigest() == reference["sha256"]
    assert b"System.IO.IOException" in raw and ("synthetic native " + stage + " lower cause").encode() in raw
    assert secret.encode() not in raw
    assert author.encode() in raw and sid.encode() in raw
    metadata = Path(reference["path"]).with_name("native-exception.json").read_bytes()
    assert json.loads(metadata) == result["native"]
    assert all(value.encode() not in metadata for value in (secret, author, sid))

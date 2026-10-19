"""Product metadata preserves bytes and rejects synthetic secrets before I/O."""
import json
from pathlib import Path

import pytest

from tests.test_shared_ancestor_protection import run_functions

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('engine', ['powershell.exe', 'pwsh.exe'])
@pytest.mark.parametrize('writer', ['Save-LabelMachineMetadata', 'Write-BootstrapUtf8Json'])
def test_machine_metadata_is_unbounded_and_secret_free(tmp_path, engine, writer):
    output = tmp_path / 'result.json'
    result = run_functions(tmp_path, ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1',
                           ['Save-LabelMachineMetadata'], r'''
function Get-KmtechSinkEnvironment {
 return @{'KMTECH_LABEL_WRITER_DELEGATION_TOKEN'='SyntheticMetadataSecret_0987654321';
          'KMTECH_LABEL_WRITER_DELEGATION_TRANSACTION_ID'='12345678-1234-1234-1234-123456789abc'}
}
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile($env:LM_METADATA_HELPER,[ref]$tokens,[ref]$errors)
$definition=$ast.Find({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Write-BootstrapUtf8Json'},$true)
. ([scriptblock]::Create($definition.Extent.Text))
$writer=$env:LM_METADATA_WRITER
$path=Join-Path $env:LM_METADATA_ROOT 'preserved.json'
$payload=[ordered]@{inventory=('x' * 70000);transaction_id='12345678-1234-1234-1234-123456789abc'}
$depth=if($writer -ceq 'Write-BootstrapUtf8Json'){8}else{30}
$expected=[Text.Encoding]::UTF8.GetBytes(($payload|ConvertTo-Json -Depth $depth)+[Environment]::NewLine)
& $writer $path $payload
$actual=[IO.File]::ReadAllBytes($path)
if($actual.Length -le 65536 -or [Convert]::ToBase64String($actual) -cne [Convert]::ToBase64String($expected)){throw 'METADATA_BYTES_DIFFER'}
$rejected=0
foreach($value in @('SyntheticMetadataSecret_0987654321','\u0053\u0079\u006e\u0074\u0068\u0065\u0074\u0069\u0063\u004d\u0065\u0074\u0061\u0064\u0061\u0074\u0061\u0053\u0065\u0063\u0072\u0065\u0074\u005f\u0030\u0039\u0038\u0037\u0036\u0035\u0034\u0033\u0032\u0031')) {
 try {Assert-KmtechMachineMetadataText $value -Environment (Get-KmtechSinkEnvironment);throw 'METADATA_SECRET_ACCEPTED'}
 catch {if($_.Exception.Message -cne 'PRODUCT_METADATA_SECRET_VALUE_REFUSED'){throw};$rejected++}
}
$refused=Join-Path $env:LM_METADATA_ROOT 'missing-parent/refused.json'
try {& $writer $refused @{value='SyntheticMetadataSecret_0987654321'};throw 'METADATA_WRITE_ACCEPTED'}
catch {if($_.Exception.Message -cne 'PRODUCT_METADATA_SECRET_VALUE_REFUSED'){throw};$rejected++}
if(Test-Path (Split-Path -Parent $refused)){throw 'REFUSAL_CHANGED_STATE'}
$data=@{bytes=$actual.Length;exact_bytes=$true;secret_refusals=$rejected;refusal_state_changed=$false}
[IO.File]::WriteAllText($env:LM_METADATA_OUTPUT,($data|ConvertTo-Json),[Text.UTF8Encoding]::new($false))
''', engine=engine, values={'LM_METADATA_HELPER': str(ROOT / 'tools/bootstrap_integrity.ps1'),
                            'LM_METADATA_WRITER': writer, 'LM_METADATA_ROOT': str(tmp_path),
                            'LM_METADATA_OUTPUT': str(output)})
    assert result.returncode == 0, result.stdout + result.stderr
    actual = json.loads(output.read_text(encoding='utf-8'))
    assert actual['bytes'] > 65536
    assert actual['exact_bytes']
    assert actual['secret_refusals'] == 3
    assert actual['refusal_state_changed'] is False

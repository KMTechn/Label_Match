"""Check every shipped embedded probe and the actual preparation catch."""
from pathlib import Path
import os

import pytest

from tests.test_shared_ancestor_protection import run_functions


ROOT = Path(__file__).resolve().parents[1]
CANONICAL = ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1'


@pytest.mark.parametrize('engine', ['powershell.exe', 'pwsh'])
@pytest.mark.parametrize('mutation', ['none', 'crlf', 'payload', 'pin'])
def test_all_embedded_probe_pins_match_runtime_normalization(tmp_path, engine, mutation):
    raw = Path(os.environ.get('LM_TEST_PROBE_CANONICAL', CANONICAL)).read_bytes()
    if mutation == 'crlf':
        raw = raw.replace(b'\r\n', b'\n').replace(b'\n', b'\r\n')
    elif mutation == 'payload':
        raw = raw.replace(b"$probe = @'\n", b"$probe = @'\n# changed probe\n", 1)
    elif mutation == 'pin':
        import re
        raw = re.sub(rb"(-ExpectedSha256 ')[a-f0-9]{64}", lambda m: m[1] + b'0' * 64, raw, count=1)
    source = tmp_path / 'canonical.ps1'
    source.write_bytes(raw)
    result = run_functions(tmp_path, source, ['Invoke-LabelPinnedProbe'], r'''
$calls=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.CommandAst] -and
 $n.GetCommandName() -ceq 'Invoke-LabelPinnedProbe'},$true))
if($calls.Count -ne 3){throw 'Embedded probe call inventory differs.'}
foreach($call in $calls){
 $owner=$call.Parent
 while($owner -and $owner -isnot [Management.Automation.Language.FunctionDefinitionAst]){$owner=$owner.Parent}
 if(-not $owner){throw 'Probe owner is missing.'}
 $literals=@($owner.FindAll({param($n) $n -is [Management.Automation.Language.StringConstantExpressionAst] -and
   $n.StringConstantType.ToString() -ceq 'SingleQuotedHereString'},$true))
 if($literals.Count -ne 1){throw 'Probe literal is ambiguous.'}
 $elements=$call.CommandElements
 $expected=''
 for($i=0;$i -lt $elements.Count-1;$i++){
   if($elements[$i] -is [Management.Automation.Language.CommandParameterAst] -and
      $elements[$i].ParameterName -ceq 'ExpectedSha256'){$expected=$elements[$i+1].Value}
 }
 if($expected -cnotmatch '^[a-f0-9]{64}$'){throw 'Probe pin must be a fixed SHA256 literal.'}
 $bytes=[Text.Encoding]::UTF8.GetBytes($literals[0].Value.Replace("`r`n","`n"))
 $h=[Security.Cryptography.SHA256]::Create()
 try{$actual=([BitConverter]::ToString($h.ComputeHash($bytes))).Replace('-','').ToLowerInvariant()}finally{$h.Dispose()}
 if($actual -cne $expected){throw ('PIN_MISMATCH: '+$owner.Name)}
 'PIN_MATCH'
}
''', engine=engine)
    if mutation in ('payload', 'pin'):
        assert result.returncode != 0
        assert 'PIN_MISMATCH' in result.stderr
    else:
        assert result.returncode == 0, result.stderr
        assert result.stdout.count('PIN_MATCH') == 3


@pytest.mark.parametrize('engine', ['powershell.exe', 'pwsh'])
@pytest.mark.parametrize('stage', ['Relays', 'Assert-WriterTransition', 'HealthyLifecycle',
                                  'ScheduledTaskSnapshot', 'Save', 'native-pin', 'archive', 'archive-failed'])
def test_preparation_failures_publish_existing_status_and_preserve_cause(tmp_path, engine, stage):
    source = CANONICAL
    if stage == 'native-pin':
        import re
        source = tmp_path / 'canonical.ps1'
        source.write_bytes(re.sub(rb"(-ExpectedSha256 ')[a-f0-9]{64}",
            lambda m: m[1] + b'0' * 64, CANONICAL.read_bytes(), count=1))
    result = run_functions(tmp_path, source,
        ['Write-LabelPreparationFailure', 'Save', 'Full', 'Assert-WriterTransition', 'Invoke-LabelPinnedProbe'], r'''
. $env:LM_SHARED
. $env:LM_REDACTION
function Get-LabelLaunchFailure {param($Failure,$Phase)
 if($Failure.Exception.Message -cne $expectedCause){throw 'First cause changed.'}
 return [ordered]@{status='FAILED';stage='SUPPORT';reason_code='INSTALLER_FAILED';report_status='REPORT_UNAVAILABLE'}
}
$source=$env:LM_ROOT;$lad=$env:LM_ROOT;$receiptSource=@{sha256=('a'*64)}
$EvidencePath=Join-Path $lad 'external.json'
$existingVerified=$false;$startupLifecycle=$null;$pristineInstall=$false;$legacyFirstRegistration=$false
$audit=$null;$archiveRoot=$null;$retainedAuditPath=$null
$boundary=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.TryStatementAst] -and
 $n.Body.Extent.Text.Contains('$old = @(Relays)') -and
 $n.Body.Extent.Text.Contains('$replacementBackupsBefore = @()')},$true))
if($boundary.Count -ne 1 -or $boundary[0].Body.Extent.Text.Contains('Enter-LabelWriterSessionAuthority')){
 throw 'Preparation boundary changed.'
}
$catch=$boundary[0].CatchClauses[0].Extent.Text
$stage=$env:LM_STAGE
$expectedCause=if($stage -ceq 'native-pin'){'PINNED_PROBE_SHA256_MISMATCH'}else{'ISOLATED_PREPARATION_REFUSED'}
if($stage -ceq 'native-pin'){
 $statement='$writerTransition = Assert-WriterTransition $source $source'
}elseif($stage -like 'archive*'){
 $audit=@{status='FAILED_PRETRANSACTION';recovery=@{artifacts_archived=($stage -ceq 'archive')}}
 $auditPath=Join-Path $lad 'pretransaction.json'
 if($stage -ceq 'archive'){
  $archiveRoot=Join-Path $lad 'archive';New-Item -ItemType Directory -Path $archiveRoot|Out-Null
  Save (Join-Path $archiveRoot 'pretransaction.json') $audit
 }else{$retainedAuditPath=$auditPath;Save $auditPath $audit}
 $originalBytes=[IO.File]::ReadAllBytes($(if($archiveRoot){Join-Path $archiveRoot 'pretransaction.json'}else{$auditPath}))
 $statement="throw 'ISOLATED_PREPARATION_REFUSED'"
}else{
 $selected=@($boundary[0].Body.Statements | Where-Object {
   $commands=@($_.FindAll({param($n) $n -is [Management.Automation.Language.CommandAst] -and $n.GetCommandName() -ceq $stage},$true))
   $commands.Count -eq 1 -and ($stage -cne 'Save' -or $_.Extent.Text.StartsWith('Save $auditPath $audit')) -and
     ($stage -cne 'Assert-WriterTransition' -or $_.Extent.Text.StartsWith('$writerTransition ='))
 })
 if($selected.Count -ne 1){throw 'Actual preparation statement is ambiguous.'}
 $statement=$selected[0].Extent.Text
 if($stage -ceq 'Save'){
  $originalSave=(Get-Item Function:\Save).ScriptBlock
  $script:saveAttempt=0
  Set-Item Function:\Save -Value {param($Path,$Value)
   $script:saveAttempt++
   if($script:saveAttempt -eq 1){throw 'ISOLATED_PREPARATION_REFUSED'}
   & $originalSave $Path $Value
  }
 }else{Set-Item -Path ('Function:\'+$stage) -Value {throw 'ISOLATED_PREPARATION_REFUSED'}}
}
try{. ([scriptblock]::Create("try {`n"+$statement+"`n}`n"+$catch));throw 'Failure swallowed.'}
catch{if($_.Exception.Message -cne $expectedCause){throw}}
if($stage -like 'archive*'){
 $path=$(if($archiveRoot){Join-Path $archiveRoot 'pretransaction.json'}else{$auditPath})
 if([Convert]::ToBase64String($originalBytes) -cne [Convert]::ToBase64String([IO.File]::ReadAllBytes($path))){throw 'Existing recovery audit changed.'}
}else{
 $files=@(Get-ChildItem -LiteralPath (Join-Path $lad 'KMTech\Label_Match\install-audit') -File)
 if($files.Count -ne 1){throw 'Failure audit absent or duplicated.'}
 $record=Get-Content -LiteralPath $files[0].FullName -Raw | ConvertFrom-Json
 if($record.status -cne 'FAILED' -or $record.preparation_status -cne 'FAILED_PREPARATION_ADMISSION' -or
    $record.code_placement -cne 'NOT_STARTED' -or $record.rollback.status -cne 'NOT_REQUIRED' -or
    $record.rollback.applied){throw 'Preparation failure state differs.'}
 if((Get-FileHash -LiteralPath $files[0].FullName).Hash -cne (Get-FileHash -LiteralPath $EvidencePath).Hash){throw 'External failure evidence differs.'}
}
'CAUSE_PRESERVED'
    ''', engine=engine, values={'LM_ROOT': str(tmp_path), 'LM_STAGE': stage,
                           'LM_SHARED': str(ROOT / 'kmtech_shared/powershell/portable.ps1'),
                           'LM_REDACTION': str(ROOT / 'tools/failure_redaction.ps1')})
    assert result.returncode == 0, result.stderr
    expected = 'FAILED_PRETRANSACTION' if stage.startswith('archive') else 'FAILED'
    assert result.stdout.count('install_status=') == 1
    assert 'install_status=' + expected in result.stdout
    assert result.stdout.count('audit_path=') == 1
    assert 'CAUSE_PRESERVED' in result.stdout

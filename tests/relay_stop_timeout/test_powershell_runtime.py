"""Execute selected real PS functions with fake clocks and native observations."""
import json
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[2]
IS_CA = (ROOT / 'container_audit_product_host.py').is_file()


def run_ps(tmp_path, engine, source, names, script):
    binary = shutil.which(engine)
    if not binary:
        pytest.skip(engine + ' unavailable')
    product = tmp_path / 'source.ps1'
    product.write_text((ROOT / source).read_text(encoding='utf-8-sig'), encoding='utf-8-sig')
    driver = tmp_path / 'driver.ps1'
    driver.write_text(r'''
param([string]$Source)
$ErrorActionPreference='Stop'
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile($Source,[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'source parse failed'}
$nodes=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst]},$true))
foreach($name in @(''' + ','.join("'" + name + "'" for name in names) + r''')){
    $node=@($nodes|Where-Object {$_.Name -ceq $name})
    if($node.Count -eq 0 -and $name -like 'Write-*WaitElapsed'){
        Set-Item -Path ('Function:'+ $name) -Value {}
        continue
    }
    if($node.Count -ne 1){throw ('missing selected function '+$name)}
    # Replace only the native clock dependency, leaving the actual wait loop.
    Invoke-Expression $node[0].Extent.Text.Replace('[DateTime]::UtcNow','(Get-Date)')
}
$assignments=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.AssignmentStatementAst]},$true))
foreach($assignment in $assignments){
    if($assignment.Left.Extent.Text -match '^\$Script:(Canonical|Label)RelayNormalStopTimeoutSeconds$'){
        Invoke-Expression $assignment.Extent.Text
    }
}
$script:seconds=0.0;$script:events=[Collections.Generic.List[object]]::new()
function Get-Date { [datetime]::Parse('2026-10-26T00:00:00Z').ToUniversalTime().AddSeconds($script:seconds) }
function Start-Sleep {param([int]$Milliseconds=0,[int]$Seconds=0) $script:seconds+=$Seconds+$Milliseconds/1000.0}
function Write-InstallationEvidenceEvent($Payload){[void]$script:events.Add($Payload)}
''' + script, encoding='utf-8-sig')
    result = subprocess.run([binary, '-NoLogo', '-NoProfile', '-NonInteractive', '-File',
                             str(driver), '-Source', str(product)], capture_output=True,
                            text=True, creationflags=subprocess.CREATE_NO_WINDOW)
    assert result.returncode == 0, result.stderr[-1500:]
    return json.loads(result.stdout), result.stderr


@pytest.mark.parametrize('engine', ['powershell.exe', 'pwsh.exe'])
@pytest.mark.parametrize('exit_after', [20, 149, 151])
def test_relay_process_exit_wait_preserves_success_and_timeout(tmp_path, engine, exit_after):
    if IS_CA:
        names = ['Write-CanonicalWaitElapsed', 'Disable-CanonicalOfficialLegacyRelay']
        script = r'''
$script:disabled=$false
function Get-CanonicalOfficialLegacyRelayPreimage { @{identity=@{sha256='fixture'};enabled=(-not $script:disabled)} }
function DisableAndStop-ContainerScheduledTaskUnderWriterFence {$script:disabled=$true}
function Get-CanonicalOfficialLegacyRelayRunningCount {if($script:seconds -ge EXIT_AFTER){return 0};return 1}
$before=@{identity=@{sha256='fixture'};task_name='fixture';task_path='fixture'}
$ok=$false;$failure=''
try{$result=Disable-CanonicalOfficialLegacyRelay $before;$ok=$true}catch{$failure=$_.Exception.Message}
[Console]::Out.WriteLine((@{ok=$ok;seconds=$script:seconds;failure=$failure;events=@($script:events.ToArray())}|ConvertTo-Json -Depth 6 -Compress))
'''
    else:
        names = ['Write-LabelWaitElapsed', 'Wait-LabelRelayPreimageExit']
        script = r'''
function UnquiescedProductWriters {if($script:seconds -lt EXIT_AFTER){[pscustomobject]@{ExecutablePath='fixture';CommandLine='fixture'}}}
function Same($Left,$Right){return $Left -ceq $Right}
function Normalize-RelayExecutableQuoting($Command,$Executable){return $Command}
$before=[pscustomobject]@{ExecutablePath='fixture';CommandLine='fixture'}
$ok=$false;$failure=''
try{Wait-LabelRelayPreimageExit @($before);$ok=$true}catch{$failure=$_.Exception.Message}
[Console]::Out.WriteLine((@{ok=$ok;seconds=$script:seconds;failure=$failure}|ConvertTo-Json -Compress))
'''
    result, stderr = run_ps(tmp_path, engine, 'INSTALL_CANONICAL_PORTABLE.ps1', names,
                            script.replace('EXIT_AFTER', str(exit_after)))
    assert result['ok'] == (exit_after <= 150)
    assert result['seconds'] == pytest.approx(min(exit_after, 150), abs=0.1)
    if exit_after > 150:
        expected = 'OFFICIAL_LEGACY_RELAY_QUIESCENCE_UNPROVEN' if IS_CA else 'Normal relay process exit remains unproven.'
        assert result['failure'] == expected
    events = result['events'] if IS_CA else [json.loads(line) for line in stderr.splitlines()]
    if IS_CA:
        assert events == []  # ANSWER-6: keep the mandatory journal unchanged.
        return
    assert len(events) == 1
    assert events[0]['elapsed_ms'] >= 0
    assert events[0]['status'] == ('COMPLETE' if exit_after <= 150 else 'ERROR')


@pytest.mark.skipif(not IS_CA, reason='CA scheduled writer contract')
@pytest.mark.parametrize('ready_after', [100, 121])
def test_scheduled_writer_can_reach_a_safe_boundary_after_90_seconds(tmp_path, ready_after):
    script = r'''
function Get-CanonicalWriterSnapshot($Root){
    @{restore_required=$true;last_task_result=0;state=$(if($script:seconds -lt READY_AFTER){'Running'}else{'Ready'});next_run_time_utc=(Get-Date).AddSeconds(30).ToString('o')}
}
$ok=$false;$failure=''
try{$result=Get-CanonicalWriterPreimageForQuiesce 'fixture';$ok=$true}catch{$failure=$_.Exception.Message}
[Console]::Out.WriteLine((@{ok=$ok;seconds=$script:seconds;failure=$failure;events=@($script:events.ToArray())}|ConvertTo-Json -Depth 6 -Compress))
'''
    result, _ = run_ps(tmp_path, 'powershell.exe', 'INSTALL_CANONICAL_PORTABLE.ps1',
                       ['Write-CanonicalWaitElapsed', 'Get-CanonicalWriterPreimageForQuiesce'],
                       script.replace('READY_AFTER', str(ready_after)))
    assert result['ok'] == (ready_after < 120)
    assert result['seconds'] == min(ready_after, 120)
    assert result['events'] == []
    if ready_after > 120:
        assert result['failure'] == 'CANONICAL_WRITER_PRESTATE_SAFE_BOUNDARY_NOT_FOUND'


@pytest.mark.skipif(not IS_CA, reason='CA task/process windows are separate')
@pytest.mark.parametrize('task_delay,process_delay', [(100, 100), (121, 0), (0, 121)])
def test_task_and_process_each_keep_their_own_120_second_window(tmp_path, task_delay, process_delay):
    script = r'''
function DisableAndStop-ContainerScheduledTaskUnderWriterFence { @{State='Running';Settings=@{Enabled=$false}} }
function Get-ScheduledTask { @{State=$(if($script:seconds -lt TASK_DELAY){'Running'}else{'Ready'});Settings=@{Enabled=$false}} }
function Get-CanonicalWriterProcesses($Root){if($script:seconds -lt [Math]::Min(TASK_DELAY,120)+PROCESS_DELAY){[pscustomobject]@{Id=100}}}
function Get-CanonicalWriterSnapshot($Root){@{binding_sha256='fixture';enabled=$false}}
$before=@{binding_sha256='fixture';task_name='fixture';task_path='fixture'}
$ok=$false;$failure=''
try{$result=Disable-CanonicalWriter 'fixture' $before;$ok=$true}catch{$failure=$_.Exception.Message}
[Console]::Out.WriteLine((@{ok=$ok;seconds=$script:seconds;failure=$failure;events=@($script:events.ToArray())}|ConvertTo-Json -Depth 6 -Compress))
'''
    result, _ = run_ps(tmp_path, 'powershell.exe', 'INSTALL_CANONICAL_PORTABLE.ps1',
                       ['Write-CanonicalWaitElapsed', 'Disable-CanonicalWriter'],
                       script.replace('TASK_DELAY', str(task_delay)).replace('PROCESS_DELAY', str(process_delay)))
    assert result['ok'] == (task_delay <= 120 and process_delay <= 120)
    assert result['events'] == []
    if not result['ok']:
        assert result['failure'] == 'CANONICAL_WRITER_QUIESCE_READBACK_FAILED'


@pytest.mark.parametrize('engine', ['powershell.exe', 'pwsh.exe'])
@pytest.mark.parametrize('action_fails', [False, True])
def test_timed_admission_preserves_the_result_or_original_failure(tmp_path, engine, action_fails):
    prefix = 'Container' if IS_CA else 'Label'
    script = r'''
$ok=$false;$failure='';$same=$false
$lease=[pscustomobject]@{proof='fixture'}
try{
    $result=INVOKE 'admission_fixture' {if(ACTION_FAILS){throw 'original-admission-failure'};return $lease}
    $same=[object]::ReferenceEquals($lease,$result);$ok=$true
}catch{$failure=$_.Exception.Message}
[Console]::Out.WriteLine((@{ok=$ok;same=$same;failure=$failure}|ConvertTo-Json -Compress))
'''
    result, stderr = run_ps(tmp_path, engine, 'INSTALL_THIS_PC.ps1',
                            ['Write-' + prefix + 'WaitElapsed', 'Invoke-' + prefix + 'TimedWait'],
                            script.replace('INVOKE', 'Invoke-' + prefix + 'TimedWait')
                            .replace('ACTION_FAILS', '$true' if action_fails else '$false'))
    assert result['ok'] == (not action_fails)
    assert result['failure'] == ('original-admission-failure' if action_fails else '')
    assert result['same'] == (not action_fails)
    record = json.loads(stderr)
    assert record['event'] == 'admission_fixture'
    assert record['status'] == ('ERROR' if action_fails else 'COMPLETE')
    assert record['elapsed_ms'] >= 0

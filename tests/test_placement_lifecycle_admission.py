"""Real PS placement admission and lifecycle Python child regression."""
from pathlib import Path
import json
import os
import secrets
import subprocess
import sys

import pytest
import writer_session_fence as fence
from tests.test_supported_lifecycle import healthy, NEW, CANDIDATE
from tests.test_writer_session_fence import _payload, _write_active

ROOT = Path(__file__).resolve().parents[1]
CHILD = ROOT / 'tests/_placement_lifecycle_child.py'
SOURCES = ['canonical_placement', 'auto_update_task_mutation', 'auto_update_restore_preimage',
           'auto_update_fixed_helper', 'auto_update_supported_lifecycle_transition',
           'auto_update_persist_switch', 'auto_update_write_json_durable']

def quote(value):
    return "'" + str(value).replace("'", "''") + "'"

@pytest.mark.parametrize('mode,failure', [('inspect', False), ('activate', False),
                                        ('inspect', True), ('activate', True), ('removed', False)])
def test_placement_lifecycle_child_preserves_fence_and_parent_grant(tmp_path, monkeypatch, mode, failure):
    machine = tmp_path / 'machine'
    machine.mkdir()
    if mode == 'removed':
        fixed = machine / 'current'
        control = machine / 'update-control'
        control.mkdir()
        (machine / 'removed.json').write_text(json.dumps({
            'schema': 'label-match-supported-removal-v1', 'lifecycle_state': 'REMOVED', 'retained_paths': []}))
    else:
        fixed, selected, control, _ = healthy(machine, monkeypatch)
        candidate = machine / 'versions' / CANDIDATE
        (candidate / 'app').mkdir(parents=True)
        (candidate / 'portable-manifest.json').write_text(json.dumps({'source_commit': CANDIDATE}))
        for name in ('update-manifest-key-config.json', 'auto-update-settings.json'):
            (candidate / 'app' / name).write_bytes((fixed / 'app' / name).read_bytes())
    preserved = tmp_path / 'outbox'
    preserved.write_bytes(b'durable UNKNOWN pending work')
    before = {p: p.read_bytes() for p in (fixed / 'portable-manifest.json', preserved, machine / 'removed.json') if p.exists()}
    before_control = {p: p.read_bytes() for p in (control / 'active.json', control / 'transition.json') if p.exists()}
    packet = tmp_path / 'packet'
    (packet / 'app').mkdir(parents=True)
    (packet / 'app/main.py').write_text(
        'import runpy\nrunpy.run_path(' + repr(str(CHILD)) + ', run_name="__main__")\n')
    writer_control = tmp_path / 'writer-control'
    token = secrets.token_hex(32)
    payload = _payload(delegated_sources=SOURCES, token=token)
    _write_active(writer_control, payload)
    env = os.environ.copy()
    env.update({fence.TEST_MODE_ENV: '1', fence.CONTROL_ROOT_OVERRIDE_ENV: str(writer_control),
                fence.DELEGATION_TOKEN_ENV: token, fence.DELEGATION_SESSION_ENV: str(payload['session_id']),
                fence.DELEGATION_ATTEMPT_ENV: str(payload['attempt_id']),
                fence.DELEGATION_TRANSACTION_ENV: str(payload['replacement_transaction_id']),
                'LM_PLACEMENT_MACHINE': str(machine),
                'LM_PLACEMENT_PARENT_GRANT_SHA': str(payload['delegation_sha256']),
                'LM_PLACEMENT_VERIFY_FAIL': '1' if failure else '0'})
    statement = '$activation' if mode == 'activate' else '$healthOutput'
    installer = ROOT / 'INSTALL_THIS_PC.ps1'
    harness = tmp_path / 'placement.ps1'
    harness.write_text(f"""
$ErrorActionPreference='Stop'
. {quote(ROOT / 'tools/label_writer_fence.ps1')}
$tokens=$null;$errors=$null
$tree=[Management.Automation.Language.Parser]::ParseFile({quote(installer)},[ref]$tokens,[ref]$errors)
if($errors.Count){{throw 'installer parse failed'}}
$wanted=@('Enter-LabelCanonicalPlacement','Invoke-LabelPlacementLifecycleChild')
foreach($node in $tree.FindAll({{param($a) $a -is [Management.Automation.Language.FunctionDefinitionAst]}},$true)){{
 if($node.Name -in $wanted){{Invoke-Expression $node.Extent.Text}}
}}
$node=@($tree.FindAll({{param($a) $a -is [Management.Automation.Language.AssignmentStatementAst] -and $a.Left.Extent.Text -ceq {quote(statement)}}},$true))
if($node.Count -ne 1){{throw 'child assignment differs'}}
$childAssignment=$node[0].Extent.Text
function Join-Path([string]$Path,[string]$ChildPath){{
 if($ChildPath -ceq 'runtime\\python.exe'){{return {quote(sys.executable)}}}
 Microsoft.PowerShell.Management\\Join-Path -Path $Path -ChildPath $ChildPath
}}
$sourceRootFull={quote(packet)};$applicationParent={quote(machine)}
$ExpectedActiveCommit={quote(NEW)};$nextCommit={quote(CANDIDATE)}
$WriterFenceControlRoot={quote(writer_control)}
$active=Read-LabelWriterFence $WriterFenceControlRoot
$WriterFenceSessionId=[string]$active.session_id;$WriterFenceAttemptId=[string]$active.attempt_id
$WriterFenceReplacementTransactionId=[string]$active.replacement_transaction_id
$WriterFenceDelegationToken=[Environment]::GetEnvironmentVariable('KMTECH_LABEL_WRITER_DELEGATION_TOKEN','Process')
$before=$active|ConvertTo-Json -Depth 20 -Compress
$script:writerFenceLease=Enter-LabelCanonicalPlacement -TimeoutMilliseconds 15000
$childCode=-1;$denied=$false;$restored=$false;$held=$false;$timedOut=$false
try{{
 # This foreign process has no delegation. It must never mutate the fixture.
 $foreign=@(& {quote(sys.executable)} -I -B {quote(CHILD)} --foreign)
 $denied=($LASTEXITCODE -eq 4)
 Invoke-Expression $childAssignment
 $childCode=$LASTEXITCODE
 $output=if({quote(mode)} -ceq 'activate'){{$activation}}else{{$healthOutput}}
 $lifecycleState=if($childCode -eq 0){{(($output -join "`n")|ConvertFrom-Json).lifecycle_state}}else{{''}}
 $timedOut=($output -join "`n") -match 'WRITER_GATE_TIMEOUT'
 $restored=(($before) -ceq ((Read-LabelWriterFence $WriterFenceControlRoot)|ConvertTo-Json -Depth 20 -Compress))
 $environmentRestored=([Environment]::GetEnvironmentVariable('KMTECH_LABEL_WRITER_DELEGATION_TOKEN','Process') -ceq $WriterFenceDelegationToken -and
  [Environment]::GetEnvironmentVariable('KMTECH_LABEL_WRITER_DELEGATION_SESSION_ID','Process') -ceq $WriterFenceSessionId -and
  [Environment]::GetEnvironmentVariable('KMTECH_LABEL_WRITER_DELEGATION_ATTEMPT_ID','Process') -ceq $WriterFenceAttemptId -and
  [Environment]::GetEnvironmentVariable('KMTECH_LABEL_WRITER_DELEGATION_TRANSACTION_ID','Process') -ceq $WriterFenceReplacementTransactionId)
 $contender=@(& {quote(sys.executable)} -I -B {quote(CHILD)} --foreign)
 $held=($LASTEXITCODE -eq 4 -and ($contender -join "`n") -match 'WRITER_GATE_TIMEOUT')
}}finally{{if($null -ne $script:writerFenceLease){{Exit-LabelWriterAdmission $script:writerFenceLease}}}}
[pscustomobject]@{{child_code=$childCode;lifecycle_state=$lifecycleState;foreign_denied=$denied;grant_restored=$restored;environment_restored=$environmentRestored;parent_held=$held;writer_timeout=$timedOut}}|ConvertTo-Json -Compress
""", encoding='utf-8-sig')
    authority = fence._acquire_named_mutex(str(payload['session_authority_mutex_name']), 0)
    assert authority is not None
    try:
        result = subprocess.run(['powershell.exe', '-NoLogo', '-NoProfile', '-NonInteractive',
                                 '-ExecutionPolicy', 'Bypass', '-File', str(harness)],
                                env=env, capture_output=True, text=True,
                                creationflags=subprocess.CREATE_NO_WINDOW)
    finally:
        authority.release()
    assert result.returncode == 0, 'isolated PS harness failed'
    summary = json.loads(result.stdout.strip())
    if (machine / 'child-failure.json').exists():
        observed = json.loads((machine / 'child-failure.json').read_text())
        assert observed['code'] != 'WRITER_GATE_TIMEOUT', observed
    assert summary['foreign_denied'] and summary['parent_held']
    assert summary['grant_restored']
    assert summary['environment_restored']
    assert not summary['writer_timeout'], 'lifecycle child waited for parent admission'
    assert summary['child_code'] == (1 if failure else 0)
    if not failure:
        assert summary['lifecycle_state'] == ('REMOVED' if mode == 'removed' else 'INSTALLED')
    child_grant = json.loads((machine / 'child-grant.json').read_text())
    assert child_grant['fresh']
    assert child_grant['foreign_exit'] == 4
    assert child_grant['foreign_code'] == 'ACTIVE_WRITER_FENCE'
    expected_sources = ['auto_update_fixed_helper']
    if mode == 'activate':
        expected_sources += ['canonical_placement', 'auto_update_supported_lifecycle_transition',
                             'auto_update_persist_switch', 'auto_update_write_json_durable']
    assert child_grant['sources'] == sorted(expected_sources)
    assert all(p.read_bytes() == raw for p, raw in before.items())
    assert not (machine / 'foreign-mutation').exists()
    if mode == 'activate' and not failure:
        assert json.loads((control / 'active.json').read_text())['sha'] == CANDIDATE
        assert json.loads((control / 'transition.json').read_text())['failed_shas'] == ['9' * 40]
    else:
        assert all(p.read_bytes() == raw for p, raw in before_control.items())


@pytest.mark.parametrize('damage', ['throw', 'grant_changed', 'missing_environment'])
def test_placement_child_scope_rechecks_fence_and_unwinds_exceptions(tmp_path, monkeypatch, damage):
    from tests.test_startup_update_installer import functions
    control = tmp_path / 'writer-control'
    token = secrets.token_hex(32)
    payload = _payload(delegated_sources=SOURCES, token=token)
    _write_active(control, payload)
    env = os.environ.copy()
    env.update({fence.DELEGATION_TOKEN_ENV: token, fence.DELEGATION_SESSION_ENV: str(payload['session_id']),
                fence.DELEGATION_ATTEMPT_ENV: str(payload['attempt_id']),
                fence.DELEGATION_TRANSACTION_ENV: str(payload['replacement_transaction_id'])})
    if damage == 'missing_environment':
        env.pop(fence.DELEGATION_SESSION_ENV, None)
    harness = tmp_path / 'scope.ps1'
    harness.write_text("$ErrorActionPreference='Stop'\n" +
        f". {quote(ROOT / 'tools/label_writer_fence.ps1')}\n" +
        functions('INSTALL_THIS_PC.ps1', ['Enter-LabelCanonicalPlacement', 'Invoke-LabelPlacementLifecycleChild']) +
        f"""
$WriterFenceControlRoot={quote(control)}
$active=Read-LabelWriterFence $WriterFenceControlRoot
$WriterFenceSessionId=[string]$active.session_id;$WriterFenceAttemptId=[string]$active.attempt_id
$WriterFenceReplacementTransactionId=[string]$active.replacement_transaction_id
$WriterFenceDelegationToken=[Environment]::GetEnvironmentVariable('KMTECH_LABEL_WRITER_DELEGATION_TOKEN','Process')
$before=$active|ConvertTo-Json -Depth 20 -Compress
$script:writerFenceLease=Enter-LabelCanonicalPlacement
$global:LASTEXITCODE=0
$caught='';$restored=$false;$environmentRestored=$false
try{{
 try{{
  $output=@(Invoke-LabelPlacementLifecycleChild -Lease ([ref]$script:writerFenceLease) -Action {{
   if({quote(damage)} -ceq 'throw'){{throw 'INJECTED_CHILD_FAILURE'}}
   if({quote(damage)} -ceq 'grant_changed'){{
    $changed=Read-LabelWriterFence $WriterFenceControlRoot
    $changed.activated_at_utc=[DateTime]::UtcNow.AddSeconds(1).ToString('o')
    [void](Write-LabelWriterFenceAtomic $WriterFenceControlRoot $changed)
   }}
  }})
 }}catch{{$caught=$_.Exception.Message}}
 $restored=($before -ceq ((Read-LabelWriterFence $WriterFenceControlRoot)|ConvertTo-Json -Depth 20 -Compress))
 $expectedSession=if({quote(damage)} -ceq 'missing_environment'){{$null}}else{{$WriterFenceSessionId}}
 $environmentRestored=([Environment]::GetEnvironmentVariable('KMTECH_LABEL_WRITER_DELEGATION_TOKEN','Process') -ceq $WriterFenceDelegationToken -and
  [Environment]::GetEnvironmentVariable('KMTECH_LABEL_WRITER_DELEGATION_SESSION_ID','Process') -ceq $expectedSession -and
  [Environment]::GetEnvironmentVariable('KMTECH_LABEL_WRITER_DELEGATION_ATTEMPT_ID','Process') -ceq $WriterFenceAttemptId -and
  [Environment]::GetEnvironmentVariable('KMTECH_LABEL_WRITER_DELEGATION_TRANSACTION_ID','Process') -ceq $WriterFenceReplacementTransactionId)
 $held=$null -ne $script:writerFenceLease -and $script:writerFenceLease.acquired
}}finally{{if($null -ne $script:writerFenceLease){{Exit-LabelWriterAdmission $script:writerFenceLease}}}}
[pscustomobject]@{{caught=$caught;restored=$restored;environment_restored=$environmentRestored;held=$held}}|ConvertTo-Json -Compress
""", encoding='utf-8-sig')
    authority = fence._acquire_named_mutex(str(payload['session_authority_mutex_name']), 0)
    assert authority is not None
    try:
        result = subprocess.run(['powershell.exe', '-NoLogo', '-NoProfile', '-NonInteractive',
                                 '-ExecutionPolicy', 'Bypass', '-File', str(harness)],
                                env=env, capture_output=True, text=True,
                                creationflags=subprocess.CREATE_NO_WINDOW)
    finally:
        authority.release()
    assert result.returncode == 0, 'isolated scope harness failed'
    summary = json.loads(result.stdout)
    expected_error = {'throw': 'INJECTED_CHILD_FAILURE', 'grant_changed': 'LABEL_PLACEMENT_CHILD_FENCE_CHANGED',
                      'missing_environment': ''}[damage]
    assert summary['caught'] == expected_error
    assert summary['restored'] == (damage != 'grant_changed')
    assert summary['environment_restored'] and summary['held']

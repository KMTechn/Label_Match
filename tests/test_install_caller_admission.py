"""Admitted fresh public entry; all OS/token/placement boundaries are synthetic."""
import json
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]


def _run_entry(tmp_path, security, *, placement=False, query_failure=False, invalid_input=False):
    source = (ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1').read_text(encoding='utf-8-sig')
    start = source.index('$wanted = Command $install')
    stop = source.index('$auditPath = Join-Path $localAuditRoot', start)
    entry = source[start:stop].replace(
        '[Environment]::GetFolderPath([Environment+SpecialFolder]::LocalApplicationData)', '$lad').replace(
        '[Security.Principal.WindowsIdentity]::GetCurrent().User.Value', '$caller.sid')
    # The current installer wraps preparation and placement in one try block.
    # The extracted prefix stops at its first audit writer, before that catch.
    entry = entry.replace("$retainedAuditPath = ''\ntry {", "$retainedAuditPath = ''")
    library = str(ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1').replace("'", "''")
    failure_trap = source[source.index('trap {'):source.index('# TEMPLATE-CONFIG-GUARD-BEGIN')]
    if invalid_input:
        entry = "throw 'synthetic invalid input'"
    script = r'''
$ErrorActionPreference='Stop'
$script:callerReads=0; $script:privateReports=0
function Get-CanonicalRelaySecurity([int]$ProcessId) {
    if($ProcessId -ne $PID){throw 'Original caller PID differs'}
    $script:callerReads++
    if($script:queryFailure){throw 'PRIVATE_NATIVE_FAILURE_E_SYNTHETIC_PATH'}
    return $caller
}
function Write-KmtechProtectedFailureSidecar { $script:privateReports++; $script:writes++; throw 'synthetic private writer' }
function ConvertTo-KmtechPublicRecord($Fields) { return $Fields }
function Get-LabelLaunchFailure { $script:privateReports++; $script:writes++; throw 'Unadmitted private reporter invoked' }

$script:writes=0; $script:consents=0; $script:refused=$false
function Full($Value,$Purpose) { return $Value }
function Same($Left,$Right) { return $Left -ceq $Right }
function Command($Root) { return 'synthetic command' }
function Manifest { return $sourceManifest }
function Snapshot { return @{exists=$false} }
function Relays { return @() }
function Get-ScheduledTask { param($ErrorAction); return @() }
function Get-LabelCurrentUserScheduledTasks { return @() }
function Resolve-LabelGenerationInputPath($Value,$Root) { return $Value }
function Get-LabelPortableDataVersion { return 'v2.0.99' }
function Assert-LabelDataDowngrade { return @{} }
function Get-LabelOfficialLegacyTaskSnapshot { return $null }
function Assert-LabelOfficialLegacyTaskLauncher { }
function Invoke-LabelSourceDataAdmission { }
function Read-FirstRegistrationAttempt { return $null }
function Sha { return ('a'*64) }
function Test-Path { param($LiteralPath,$Path,$PathType); return $false }
function Test-PristineInstallState { return $true }
function PortableInventory { return @{critical_file_sha256=@{bootstrap_integrity_helper=('a'*64)}} }
function Assert-WriterTransition { return @{} }
function New-Item { param($ItemType,$Path,[switch]$Force); $script:writes++ }
function Start-Process {
    param($FilePath,$Verb,$ArgumentList,$WindowStyle,[switch]$Wait,[switch]$PassThru)
    if ($Verb -cne 'RunAs' -or $WindowStyle -cne 'Hidden' -or -not $Wait) { throw 'UAC boundary differs' }
    $script:consents++; return @{ExitCode=0}
}
$testMode=$false; $PlanOnly=$false; $conflictReceiptSupplied=$false
$source='E:\synthetic-packet'; $install='E:\synthetic-current'
$sourceManifest=@{source_commit=('a'*40)}; $receiptSource=$null
''' + ('$script:queryFailure=' + ('$true' if query_failure else '$false') + '\n') + '$env:LOCALAPPDATA=\'' + str(tmp_path).replace("'", "''") + "'\n" + \
        '$caller=' + '@{elevated=' + ('$true' if security[0] else '$false') + \
        ';integrity_rid=' + str(security[1]) + ';session_id=' + str(security[2]) + \
        ";sid='synthetic-worker'}\ntry {\n" + entry + '\n'
    script = '$script:CanonicalCallerAdmitted=$false\n' + failure_trap + script
    if placement:
        script += r'''
$tokens=$null; $errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile('@@library@@',[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'Product parser failed'}
$definition=$ast.FindAll({param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -ceq 'InvokeFrozenPlacementHelper'},$true)
# Execute the existing launch/consent branch; transport bytes, native caller
# membership and Start-Process are supplied by the synthetic OS boundary.
$body=$definition[0].Extent.Text
$launchStart=$body.IndexOf('    $launchParameters = @{')
$launchStop=$body.IndexOf('    if (Test-Path -LiteralPath $failurePath', $launchStart)
if($launchStart -lt 0 -or $launchStop -lt 0){throw 'Existing UAC branch not found'}
$launch=$body.Substring($launchStart,$launchStop-$launchStart).Replace('$principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)', '[bool]$caller.elevated')
$winps='synthetic powershell';$encodedLauncher='synthetic transport';$TransportForTest=$null
Invoke-Expression $launch
if($childExit -ne 0){throw 'Synthetic elevated child failed'}
'''
    script += "\n} finally { @{writes=$script:writes;consents=$script:consents;caller_reads=$script:callerReads;private_reports=$script:privateReports}|ConvertTo-Json -Compress }\n"
    path = tmp_path / 'caller.ps1'
    path.write_text(script.replace('@@library@@', library), encoding='utf-8-sig')
    executable = shutil.which('powershell.exe')
    assert executable, 'Windows PowerShell required'
    result = subprocess.run([executable,'-NoLogo','-NoProfile','-NonInteractive','-File',str(path)],
                            capture_output=True,text=True,check=False,creationflags=subprocess.CREATE_NO_WINDOW)
    assert result.returncode in (0, 1), 'Synthetic public entry harness failed'
    lines = result.stdout.strip().splitlines()
    observed = json.loads(lines[-1])
    observed['refused'] = result.returncode == 1
    observed['exit_code'] = result.returncode
    public = [line for line in (*lines[:-1], *result.stderr.splitlines()) if line.strip()]
    if result.returncode:
        assert len(public) == 1, 'Refusal must emit exactly one public failure'
        prefix = 'CONTAINER_AUDIT_INSTALL_CANONICAL=' if (ROOT / 'Container_Audit.py').is_file() else 'LABEL_MATCH_INSTALLER_FAILURE='
        assert public[0].startswith(prefix)
        failure = json.loads(public[0][len(prefix):])
        assert failure['status'] == 'FAILED' and failure['report_status'] == 'REPORT_UNAVAILABLE'
        assert failure['reason_code'] == ('INSTALLER_CANONICAL_FAILURE' if (ROOT / 'Container_Audit.py').is_file() else 'INSTALLER_FAILED')
        assert 'report_path' not in failure
        assert 'PRIVATE_NATIVE_FAILURE' not in public[0] and 'synthetic-worker' not in public[0]
    else:
        assert public == []
    return observed


@pytest.mark.parametrize('security', [(True,12288,7),(False,12288,7),(True,16384,7),(False,8192,0)],
                         ids=['elevated','high','system','session-zero'])
def test_public_caller_refuses_before_user_state(tmp_path, security):
    observed = _run_entry(tmp_path, security)
    assert observed['refused'] and observed['writes'] == 0, 'Unsafe public caller reached the audit directory writer'
    assert observed['exit_code'] == 1 and observed['consents'] == 0 and observed['caller_reads'] == 1 and observed['private_reports'] == 0


def test_normal_caller_keeps_existing_uac_child(tmp_path):
    observed = _run_entry(tmp_path, (False,8192,7), placement=True)
    assert not observed['refused'] and observed['writes'] == 1 and observed['consents'] == 1
    assert observed['exit_code'] == 0 and observed['caller_reads'] == 1 and observed['private_reports'] == 0

def test_caller_security_query_failure_has_no_private_report(tmp_path):
    observed = _run_entry(tmp_path, (False,8192,7), query_failure=True)
    assert observed['refused'] and observed['writes'] == 0 and observed['consents'] == 0
    assert observed['caller_reads'] == 1 and observed['private_reports'] == 0


@pytest.mark.parametrize('security', [(False,4096,7),(False,8192,-1)], ids=['low','negative-session'])
def test_other_non_normal_callers_refuse_without_writes(tmp_path, security):
    observed = _run_entry(tmp_path, security)
    assert observed['refused'] and observed['writes'] == 0 and observed['private_reports'] == 0

def test_invalid_input_before_caller_admission_has_no_private_report(tmp_path):
    observed = _run_entry(tmp_path, (False,8192,7), invalid_input=True)
    assert observed['refused'] and observed['writes'] == 0 and observed['private_reports'] == 0
    assert observed['caller_reads'] == 0

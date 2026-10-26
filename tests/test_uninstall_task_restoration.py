"""Real removal/transition functions with a scheduler that cannot reach the host."""
import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest

from tests.test_uninstall_restore_transaction import DRIVER, REPO, make_tree
from auto_update import logon_task_xml


SCHEDULER = r'''
$script:events=[Collections.Generic.List[string]]::new()
$script:exists=$true;$script:enabled=($Cut -cnotin @('legacy-initially-disabled','official-initially-disabled-running'));$script:sddl='saved-sddl'
$script:originalXml=[IO.File]::ReadAllText((Join-Path $Root 'task.xml'))
$script:taskXml=$script:originalXml
$script:legacy=$Cut.StartsWith('legacy-')
$script:official=$Cut.StartsWith('official-')
$script:taskName=if($script:legacy){'direct-sync-relay-label-match-current-pc'}elseif($script:official){'official-fixture'}else{'KMTech.LabelMatch.StartupUpdate.'+('a'*24)}
$script:native=[pscustomobject]@{}
$script:native | Add-Member ScriptProperty Xml { $script:taskXml }
$script:native | Add-Member ScriptProperty Enabled { $script:enabled } { param($value) throw 'Direct Enabled writes are not the existing cmdlet idiom' }
$script:native | Add-Member ScriptMethod GetSecurityDescriptor { param($flags) $script:sddl }
$script:native | Add-Member ScriptMethod SetSecurityDescriptor { param($value,$flags) $script:events.Add('sddl');$script:sddl=$value }
$script:native | Add-Member ScriptMethod Stop { param($flags) $script:events.Add('stop') }
$script:native | Add-Member ScriptMethod GetInstances {
    param($flags)
    if($Cut -ceq 'legacy-disabled-failure'){throw 'INJECTED_AFTER_DISABLE'}
    return [pscustomobject]@{Count=0}
}
$folder=[pscustomobject]@{}
$folder | Add-Member ScriptMethod GetTask {
    param($name)
    if(-not $script:exists){throw [IO.FileNotFoundException]::new('absent')}
    return $script:native
}
$folder | Add-Member ScriptMethod DeleteTask { param($name,$flags) $script:events.Add('delete');$script:exists=$false }
$service=[pscustomobject]@{}
$service | Add-Member ScriptMethod Connect { }
$service | Add-Member ScriptMethod GetFolder { param($path) return $folder }
function New-Object {
    [CmdletBinding(DefaultParameterSetName='Type')]
    param([Parameter(Position=0,ParameterSetName='Type')][string]$TypeName,
        [Parameter(Position=1,ParameterSetName='Type')][object[]]$ArgumentList,
        [Parameter(ParameterSetName='Com')][string]$ComObject)
    if($PSCmdlet.ParameterSetName -ceq 'Type'){return Microsoft.PowerShell.Utility\New-Object @PSBoundParameters}
    if($ComObject -cne 'Schedule.Service'){throw 'Unmocked COM refused'}
    return $service
}
function Get-LabelSupportedStartupTasks {
    param($Current)
    if(-not $script:legacy -and -not $script:official -and $script:exists){[pscustomobject]@{TaskName=$script:taskName}}
}
function Get-LegacyTaskByNameFailClosed {
    param($Name)
    if($script:legacy -and $script:exists){
        [pscustomobject]@{TaskName=$Name;TaskPath='\';Settings=[pscustomobject]@{Enabled=$script:enabled};
            Principal=[pscustomobject]@{UserId='S-1-5-18';LogonType='ServiceAccount';RunLevel='Highest'};
            Actions=@([pscustomobject]@{Execute='wscript.exe';WorkingDirectory='';Arguments='//B //NoLogo C:\ProgramData\KMTech\DirectSync\label-match-margin-r2\bin\run_direct-sync-relay-label-match-current-pc.vbs'})}
    }
}
function Export-ScheduledTask { param($TaskName,$TaskPath,$ErrorAction) return $script:taskXml }
function Get-ScheduledTask { throw 'Unexpected native scheduler query refused' }
function Stop-ScheduledTask { throw 'Unexpected native scheduler stop refused' }
function Start-ScheduledTask { throw 'Unexpected native scheduler start refused' }
function schtasks.exe { throw 'Native scheduler executable refused' }
function Set-FixtureEnabled([bool]$Value){
    $script:enabled=$Value
    $xml=[xml]$script:taskXml
    $xml.Task.Settings.Enabled=([string]$Value).ToLowerInvariant()
    $script:taskXml=$xml.OuterXml
}
function Disable-ScheduledTask {
    param($TaskName,$TaskPath,$ErrorAction)
    $script:events.Add('disable')
    if($Cut -ceq 'legacy-unchanged-failure'){throw 'INJECTED_BEFORE_DISABLE'}
    Set-FixtureEnabled $false
    if($Cut -ceq 'legacy-foreign-change'){$script:taskXml=$script:taskXml.Replace('wscript.exe','foreign.exe')}
}
function Enable-ScheduledTask {
    param($TaskName,$TaskPath,$ErrorAction)
    $script:events.Add('enable')
    if($Cut -ceq 'official-restore-failure'){throw 'INJECTED_RESTORE_FAILURE'}
    Set-FixtureEnabled $true
}
function Unregister-ScheduledTask { param($TaskName,$TaskPath,$Confirm,$ErrorAction) $script:events.Add('delete');$script:exists=$false }
function Register-ScheduledTask {
    param($TaskName,$Xml,[switch]$Force)
    $script:events.Add('register');$script:exists=$true;$script:enabled= -not ($Xml -cmatch '<Settings><Enabled>false</Enabled>');$script:sddl='scheduler-default'
    # Scheduler exports omit an exact default that the established comparer allows.
    $script:taskXml=$Xml.Replace('<AllowStartOnDemand>true</AllowStartOnDemand>','')
    if($Cut -ceq 'startup-readback-corrupt'){$script:taskXml=$script:taskXml.Replace('powershell.exe','foreign.exe')}
    if($Cut -ceq 'startup-readback-disabled'){$script:enabled=$false}
}
function Get-LabelOfficialLegacyTaskSnapshot {
    if($script:official){
        $digest=[Security.Cryptography.SHA256]::Create()
        try{$hash=([BitConverter]::ToString($digest.ComputeHash([Text.Encoding]::UTF8.GetBytes($script:taskXml)))).Replace('-','').ToLowerInvariant()}finally{$digest.Dispose()}
        [pscustomobject]@{name=$script:taskName;path='\';enabled=$script:enabled;xml=$script:taskXml;xml_sha256=$hash;fingerprint=[pscustomobject]@{sha256=('b'*64)}}
    }
}
function Get-LabelOfficialLegacyTaskExpectation { return @{} }
function Get-LabelOfficialLegacyTaskFingerprint { param($TaskName,$TaskPath,$Xml) return [pscustomobject]@{sha256=('b'*64)} }
function Assert-LabelOfficialLegacyTaskStopped {
    param($Name)
    $script:events.Add('stopped')
    if($Cut -cin @('official-running','official-initially-disabled-running')){throw 'LEGACY_RELAY_STOP_UNPROVEN'}
}
function Set-HardenedCodeAcl { param($Path,[switch]$Recursive) }
function Assert-HardenedCodeAcl { param($Path,[switch]$Recursive) }
function Assert-NoReparsePoint { param($Path,$Purpose) }
$finish={throw 'INJECTED_LATE_FAILURE'}
'''


def run_driver(tmp_path, engine, cut, script, *, task_xml=None):
    root = tmp_path / 'application'
    before = make_tree(root)
    if task_xml is not None:
        (root / 'task.xml').write_text(task_xml, encoding='utf-8')
    driver = tmp_path / 'driver.ps1'
    driver.write_text(script, encoding='utf-8-sig')
    result = subprocess.run([
        shutil.which(engine), '-NoLogo', '-NoProfile', '-NonInteractive', '-File', str(driver),
        '-Repo', str(REPO), '-Root', str(root), '-Product', 'Label', '-Cut', cut,
        '-Source', os.environ.get('REMOVAL_RESTORE_SOURCE', str(REPO / 'INSTALL_THIS_PC.ps1'))],
        env=dict(os.environ, PROGRAMDATA=str(tmp_path / 'programdata')),
        capture_output=True, creationflags=0x08000000)
    (tmp_path / 'driver.stderr').write_bytes(result.stderr)
    assert result.returncode == 0, 'isolated scheduler driver failed; see driver.stderr'
    return root, before, json.loads((root / 'outcome.json').read_text(encoding='utf-8-sig'))


@pytest.mark.parametrize('engine', ['powershell.exe', 'pwsh.exe'])
@pytest.mark.parametrize('cut', [
    'legacy-disabled-failure', 'legacy-unchanged-failure', 'legacy-deleted',
    'legacy-foreign-change', 'startup-deleted', 'startup-readback-corrupt',
    'official-running', 'official-disabled', 'official-restore-failure',
    'legacy-initially-disabled', 'official-initially-disabled-running', 'startup-readback-disabled'])
def test_task_compensation_changes_only_removed_or_disabled_state(tmp_path, engine, cut):
    assert shutil.which(engine), 'Required PowerShell interpreter unavailable'
    xml = logon_task_xml(app_root=tmp_path / 'application/current', operator_sid='S-1-5-18')
    if cut.startswith(('legacy-', 'official-')):
        xml = '<Task><Settings><Enabled>true</Enabled></Settings><Actions><Exec><Command>wscript.exe</Command></Exec></Actions></Task>'
    original_enabled = 'initially-disabled' not in cut
    if not original_enabled:
        xml = xml.replace('<Enabled>true</Enabled>', '<Enabled>false</Enabled>')
    script = DRIVER.replace('$failure=$false;$guidance=$false', SCHEDULER + '\n$failure=$false;$guidance=$false')
    script = script.replace('-TaskFolder $null', '-TaskFolder $folder')
    script = script.replace('@{failed=$failure;guidance=$guidance}',
        '@{failed=$failure;guidance=$guidance;enabled=$script:enabled;exists=$script:exists;xml=$script:taskXml;sddl=$script:sddl;events=@($script:events)}')
    root, before, outcome = run_driver(tmp_path, engine, cut, script, task_xml=xml)
    assert outcome['failed']
    damaged = cut in ('legacy-foreign-change', 'startup-readback-corrupt', 'startup-readback-disabled', 'official-restore-failure')
    assert outcome['guidance'] is damaged
    assert bool(list((root / 'removal-evidence').glob('*/rollback.json'))) is damaged
    assert not (root / 'removed.json').exists()
    assert all((root / name).read_bytes() == raw for name, raw in before.items())
    events = outcome['events']
    if cut in ('legacy-deleted', 'legacy-initially-disabled', 'startup-deleted', 'startup-readback-corrupt', 'startup-readback-disabled'):
        assert events.count('register') == 1 and events.count('sddl') == 1
    else:
        assert 'register' not in events and 'sddl' not in events
    if cut in ('legacy-disabled-failure', 'official-disabled'):
        assert outcome['enabled'] and events.count('enable') == 1
    if cut in ('legacy-unchanged-failure', 'official-running'):
        assert outcome['enabled'] and 'enable' not in events
    if cut in ('official-running', 'official-initially-disabled-running'):
        assert events.count('stopped') == 1
    if cut == 'legacy-foreign-change':
        assert 'foreign.exe' in outcome['xml'] and 'enable' not in events
    if not damaged:
        assert outcome['exists'] and outcome['enabled'] is original_enabled and outcome['sddl'] == 'saved-sddl'


PRESERVATION = r'''
if($Cut -ceq 'foreign-parent'){
    $extra=Join-Path (Split-Path -Parent $Root) 'PublicDesktop/owned.lnk'
    [void][IO.Directory]::CreateDirectory((Split-Path -Parent $extra))
    [IO.File]::WriteAllText($extra,'owned link bytes')
    $finish={
        [IO.Directory]::SetLastWriteTimeUtc((Split-Path -Parent $extra),[datetime]'2002-02-02Z')
        throw 'INJECTED_LATE_FAILURE'
    }
}
if($Cut -cin @('foreign-machine-link','changed-machine-link')){
    $script:ownLink=($Cut -ceq 'changed-machine-link')
    $script:foreignLink=Join-Path $env:PROGRAMDATA 'StartMenu/KMTech 포장.lnk'
    [void][IO.Directory]::CreateDirectory((Split-Path -Parent $script:foreignLink))
    [IO.File]::WriteAllText($script:foreignLink,'foreign link bytes')
    [IO.File]::WriteAllText((Join-Path $Root 'update-control/worker-pending.json'),'{}')
    function New-Object {
        param($ComObject)
        if($ComObject -cne 'WScript.Shell'){throw 'Unmocked COM refused'}
        $shell=[pscustomobject]@{}
        $shell | Add-Member ScriptMethod CreateShortcut {
            param($path)
            if($script:ownLink){[pscustomobject]@{TargetPath=(Join-Path $Root 'current/runtime/pythonw.exe');Arguments=('-I -B "'+(Join-Path $Root 'current/app/main.py')+'"');WorkingDirectory=(Join-Path $Root 'current')}}
            else{[pscustomobject]@{TargetPath='foreign';Arguments='foreign';WorkingDirectory='foreign'}}
        }
        return $shell
    }
    if($Cut -ceq 'changed-machine-link'){
        $begin={$script:ownLink=$false;[IO.File]::WriteAllText($script:foreignLink,'changed foreign link')}
    }
    $finish={[IO.File]::Delete($script:foreignLink);throw 'INJECTED_LATE_FAILURE'}
}
if($Cut -ceq 'empty-management'){
    [IO.File]::Delete($extra)
    $begin={Remove-LabelEmergencyShortcut (Join-Path $Root 'current');throw 'INJECTED_LATE_FAILURE'}
}
if($Cut -ceq 'save-failure'){
    $originalTestFile=(Get-Command Test-LabelRemovalFile -CommandType Function).ScriptBlock
    function Test-LabelRemovalFile {
        param($Path,$Sha256)
        $backups=@(Get-ChildItem -LiteralPath (Join-Path $Root 'removal-evidence') -Filter '.restore-preimage' -Directory -Recurse)
        if($backups.Count -and @([IO.Directory]::EnumerateFiles($backups[0].FullName)).Count -ge 1 -and
            -not $Path.Contains('.restore-preimage')){throw 'INJECTED_SAVE_FAILURE'}
        & $originalTestFile $Path $Sha256
    }
}
if($Cut -ceq 'cleanup-failure'){
    $WarningPreference='Stop'
    $finish={
        $backups=@(Get-ChildItem -LiteralPath (Join-Path $Root 'removal-evidence') -Filter '.restore-preimage' -Directory -Recurse)
        $script:restoreLock=[IO.File]::Open((Join-Path $backups[0].FullName '0'),'Open','Read','None')
    }
}
'''


@pytest.mark.parametrize('engine', ['powershell.exe', 'pwsh.exe'])
@pytest.mark.parametrize('cut', ['foreign-parent', 'foreign-machine-link', 'changed-machine-link', 'empty-management',
                                'locked-unchanged', 'save-failure', 'cleanup-failure', 'success'])
def test_compensation_preserves_foreign_and_unchanged_material(tmp_path, engine, cut):
    assert shutil.which(engine), 'Required PowerShell interpreter unavailable'
    script = DRIVER.replace('$failure=$false;$guidance=$false', PRESERVATION + '\n$failure=$false;$guidance=$false')
    script = script.replace('Move-Item -LiteralPath $Source -Destination $Destination', r'''
        Move-Item -LiteralPath $Source -Destination $Destination
        if($Cut -ceq 'locked-unchanged' -and (Split-Path -Leaf $Source) -ceq 'first.bin'){
            $script:restoreLock=[IO.File]::Open((Join-Path $Root 'update-control/policy.json'),'Open','Read','None')
        }
''')
    root, before, outcome = run_driver(tmp_path, engine, cut, script)
    assert not outcome['guidance']
    assert not list((root / 'removal-evidence').glob('*/rollback.json'))
    if cut in ('success', 'cleanup-failure'):
        assert not outcome['failed'] and (root / 'removed.json').exists()
    else:
        assert outcome['failed'] and not (root / 'removed.json').exists()
    if cut == 'foreign-parent':
        from datetime import datetime, timezone
        parent = tmp_path / 'PublicDesktop'
        assert parent.stat().st_mtime == datetime(2002, 2, 2, tzinfo=timezone.utc).timestamp()
        assert (parent / 'owned.lnk').read_text() == 'owned link bytes'
    elif cut in ('foreign-machine-link', 'changed-machine-link'):
        assert not (tmp_path / 'programdata/StartMenu/KMTech 포장.lnk').exists()
    elif cut == 'empty-management':
        assert (root / '관리용').is_dir() and not list((root / '관리용').iterdir())
    elif cut == 'locked-unchanged':
        assert all((root / name).read_bytes() == raw for name, raw in before.items())
    preimages = list((root / 'removal-evidence').glob('*/.restore-preimage'))
    if cut == 'cleanup-failure':
        assert preimages, 'a locked scratch copy stays without turning removal into damage'
    else:
        assert not preimages, 'successful compensation/removal and partial Save release their scratch copies'

"""Actual Label removal and placement fragments; scheduler/process/ACL seams only."""
import hashlib
import json
from pathlib import Path
import random
import shutil
import subprocess
import uuid
import pytest
import auto_update as update
from tests.test_auto_update import OLD, NEW
REPO = Path(__file__).resolve().parents[1]


@pytest.fixture
def tmp_path(tmp_path_factory):
    root = tmp_path_factory.getbasetemp().parent / "p" / uuid.uuid4().hex[:6]
    root.mkdir(parents=True)
    return root

REMOVAL_SCRIPT = r"""
param([string]$Repo,[string]$Root,[string]$Cut='', [string]$Install='True')
$ErrorActionPreference='Stop'
[Console]::OutputEncoding=New-Object Text.UTF8Encoding($false)
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile((Join-Path $Repo 'INSTALL_THIS_PC.ps1'),[ref]$tokens,[ref]$errors)
if ($errors.Count) { throw 'source parse failed' }
$names=@('Get-LabelVerifiedRemovalProcessChain','Test-LabelRemovalProcessReferencesRoot','Test-LabelRemovalCanonicalOwner','Get-LabelRemovalNativeProcessStartTime','Get-StrictFullPath','Test-SamePath','Get-LabelRemovalIoPath','Assert-LabelRemovalBoundary',
    'Assert-LabelRemovalQuiescent','Remove-LabelScheduledTasks', 'Get-LabelSupportedStartupTasks',
    'Test-LabelRemovalFile','Get-LabelArtifactFilePlan',
    'Get-LabelRemovalArtifacts','Remove-LabelEmptiedParents','Invoke-LabelConvergentRemoval')
$functions=$ast.FindAll({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -in $names},$true)
if ($functions.Count -ne $names.Count) { throw 'missing removal functions' }
foreach ($function in $functions) { . ([scriptblock]::Create($function.Extent.Text)) }
Add-Type -TypeDefinition @'
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
public class RemovalTask {
    public string Xml="<Task><Fixture/></Task>";
    public string GetSecurityDescriptor(int flags) { return "fixture-sddl"; }
    public void Stop(int flags) { }
}
public class RemovalFolder {
    public HashSet<string> Tasks=new HashSet<string>();
    public string Cut="";
    public RemovalFolder() {
      Tasks.Add("fixture-startup-task"); Tasks.Add("fixture-legacy-task");
    }
    public RemovalTask GetTask(string name) {
      if (!Tasks.Contains(name)) throw new COMException("absent",unchecked((int)0x80070002));
      return new RemovalTask();
    }
    public void DeleteTask(string name, int flags) {
      Tasks.Remove(name);
      if (Cut==name) throw new COMException("injected interruption",unchecked((int)0x80004005));
    }
    public RemovalTask RegisterTask(string name,string xml,int flags,string user,object password,int logon,string sddl) {
      Tasks.Add(name);return new RemovalTask();
    }
}
'@
$folder=New-Object RemovalFolder
$folder.Cut=$Cut
$script:cut=$Cut
$script:processScenario=$Cut
$installRootFull=Join-Path $Root 'current';$taskFolder=$folder
$calls=$ast.FindAll({param($n) $n -is [Management.Automation.Language.CommandAst] -and
    $n.GetCommandName() -ceq 'Invoke-LabelConvergentRemoval'},$true)
if ($calls.Count -ne 1) { throw 'missing supported removal caller' }
$invokeRemoval=[scriptblock]::Create($calls[0].Extent.Text)

$testOverride=$true
function Get-LabelSupportedStartupTasks { param($Root)
    foreach ($name in @($folder.Tasks)) { [pscustomobject]@{TaskName=$name} }
}
function Get-OwnedLegacyTaskRetirementSnapshot { return $null }
function Get-CimInstance {
    if ($script:processScenario -eq 'process-unknown') { return @() }
    if ($script:processScenario -eq 'process-live') { return [pscustomobject]@{ProcessId=124;ParentProcessId=0;CreationDate=[datetime]::UtcNow;ExecutablePath=(Join-Path $Root 'versions\fixture\runtime\python.exe');CommandLine='fixture'} }
    if ($script:processScenario -eq 'release-owner') { return [pscustomobject]@{ProcessId=123;ParentProcessId=0;CreationDate=[datetime]::UtcNow;ExecutablePath='fixture';CommandLine=(Join-Path $Root 'current')} }
    return [pscustomobject]@{ProcessId=$PID;ParentProcessId=0;CreationDate=[datetime]::UtcNow;ExecutablePath='fixture';CommandLine='fixture'}
}
function Write-LabelStartupUpdateRecord {
    param([string]$Path,$Payload,[switch]$ProtectedOwner)
    if ($script:cut -ceq 'marker-before' -and (Split-Path -Leaf $Path) -ceq 'removed.json') { throw 'injected before marker' }
    [IO.File]::WriteAllText($Path,($Payload | ConvertTo-Json -Depth 12), (New-Object Text.UTF8Encoding($false)))
    if ($script:cut -ceq 'marker-after' -and (Split-Path -Leaf $Path) -ceq 'removed.json') { throw 'injected after marker' }
}
function Move-Item {
    [CmdletBinding()]param([string]$LiteralPath,[string]$Destination)
    if ($script:cut -ceq 'metadata-before' -and (Split-Path -Leaf $LiteralPath) -ceq 'bootstrap-integrity.json') { throw 'injected before metadata' }
    Microsoft.PowerShell.Management\Move-Item @PSBoundParameters
    if ((Split-Path -Leaf $LiteralPath) -ceq $script:cut -or
        (Split-Path -Leaf (Split-Path -Parent $LiteralPath)) -ceq $script:cut) { throw 'injected after rename' }
}
$current=Join-Path $Root 'current'
$interrupted=$false
try { [void](& $invokeRemoval) }
catch { if (-not $Cut) { throw }; $interrupted=$true }
$script:cut='';$folder.Cut=''
[void](& $invokeRemoval)
[void](& $invokeRemoval)
if ($folder.Tasks.Count -ne 0) { throw 'task removal did not converge' }
if ((Get-LabelRemovalArtifacts $Root).artifacts | Where-Object { (Split-Path -Leaf $_) -cne 'removed.json' }) {
    throw 'known machine artifact remains'
}

if ($Install -eq 'True') {
    $stagingRoot=Join-Path $Root 'new-stock';$placementRoot=$current
    $placement=$ast.FindAll({param($n) $n -is [Management.Automation.Language.CommandAst] -and
        $n.Extent.Text -ceq 'Move-Item -LiteralPath $stagingRoot -Destination $placementRoot'},$true)
    if (-not $placement.Count) { throw 'missing actual first placement' }
    & ([scriptblock]::Create($placement[0].Extent.Text))
}
@{status='PASS';interrupted=$interrupted;installed=($Install -eq 'True');task_exists=($folder.Tasks.Count -ne 0)} | ConvertTo-Json -Compress
"""

def _run_removal(root, tmp_path, *, cut='', install=True, refusal=''):
    executable = shutil.which('powershell.exe')
    if not executable:
        pytest.skip('Windows PowerShell required')
    script = tmp_path / 'remove-functions.ps1'
    script.write_text(REMOVAL_SCRIPT, encoding='ascii')
    result = subprocess.run([executable, '-NoProfile', '-NonInteractive', '-File', str(script),
        str(REPO), str(root), cut, str(install)], capture_output=True,
        creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    if refusal:
        assert result.returncode != 0
        assert refusal in result.stderr.decode('utf-8', errors='replace')
        return
    assert result.returncode == 0, result.stderr.decode('utf-8', errors='replace')
    return json.loads(result.stdout.decode('utf-8-sig'))

def _new_stock(root):
    (root / 'new-stock').mkdir(parents=True, exist_ok=True)
    update.write_json_durable(root / 'new-stock/portable-manifest.json', {'source_commit': NEW})
    update.write_json_durable(root / 'new-stock/bootstrap-integrity.json', {'aggregate_sha256': 'a' * 64})

def _write_inventory(artifact, owned):
    rows = [{'path': p.relative_to(artifact).as_posix(), 'size': len(raw),
             'sha256': hashlib.sha256(raw).hexdigest()} for p, raw in owned.items()]
    aggregate = '\n'.join(f"{r['path']}|{r['size']}|{r['sha256']}" for r in rows)
    update.write_json_durable(artifact / 'bootstrap-integrity.json', {
        'schema_version': 'label-match-bootstrap-integrity-v1', 'status': 'PASS',
        'files': rows, 'file_count': len(rows),
        'aggregate_sha256': hashlib.sha256(aggregate.encode()).hexdigest()})

def _owned_artifact(root, slot):
    artifact = (root / 'current' if slot == 'current' else root / 'versions' / NEW
                if slot == 'version' else root / 'update-control' / ('packet-' + 'a' * 32))
    if slot == 'incoming':
        contents = {'manifest.json': json.dumps({'app_id': 'Label_Match',
                    'artifact': {'sha256': hashlib.sha256(b'owned-packet').hexdigest(),
                                 'size_bytes': len(b'owned-packet')}}).encode(),
                    'manifest.sig': b'owned-signature', 'packet.zip': b'owned-packet'}
    else:
        contents = {'portable-manifest.json': json.dumps({'schema': 'label-match-portable-tree-v1',
                                                         'source_commit': NEW}).encode(),
                    'app/main.py': b'owned-code', 'runtime/pythonw.exe': b'owned-runtime'}
    for relative, raw in contents.items():
        path = artifact / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
    owned = {artifact / relative: raw for relative, raw in contents.items()}
    if slot != 'incoming':
        _write_inventory(artifact, owned)
    else:
        owned = {artifact / 'packet.zip': contents['packet.zip']}
    return artifact, owned

def _r5_junction(link, target, boundary):
    script = boundary / "junction.ps1"
    script.write_text(r'''param([string]$Link,[string]$Target,[string]$Boundary)
$ErrorActionPreference='Stop'
$root=[IO.Path]::GetFullPath($Boundary).TrimEnd('\')+'\'
foreach($path in @($Link,$Target)) {
 if(-not ([IO.Path]::GetFullPath($path)).StartsWith($root,[StringComparison]::OrdinalIgnoreCase)) { throw 'outside isolated task root' }
}
New-Item -ItemType Junction -Path $Link -Value $Target | Out-Null
''', encoding="ascii")
    result = subprocess.run([shutil.which("powershell.exe"), "-NoProfile", "-NonInteractive", "-File",
        str(script), str(link), str(target), str(boundary)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert link.resolve() == target.resolve()

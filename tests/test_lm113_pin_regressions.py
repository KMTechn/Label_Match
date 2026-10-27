"""113 review regressions: packet bytes, shared lock, and test harness closure."""
import hashlib
import ast
import json
from pathlib import Path
import re
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('relative', ['INSTALL_CANONICAL_PORTABLE.ps1', 'tools/bootstrap_integrity.ps1'])
def test_shared_lock_version_comes_from_verified_manifest(relative):
    source = (ROOT / relative).read_text(encoding='utf-8-sig')
    start = source.index('function Get-LabelSharedPortableLeafPath')
    end = source.index('    $sharedLeaf =', start)
    body = source[start:end]
    lock = json.loads((ROOT / 'kmtech_shared.lock.json').read_bytes())
    literals = re.findall(r"\$lock.version -cne '([^']+)'", body)
    assert not literals, 'shared version must come from authenticated manifest bytes'
    assert '[string]$lock.version -cne [string]$pinnedSharedManifest.version' in body
    assert body.index('if ($actualManifestSha -cne') < body.index('$pinnedSharedManifest.version')
    manifest = (ROOT / 'kmtech_shared.manifest.json').read_bytes()
    assert hashlib.sha256(manifest).hexdigest() == lock['manifest_sha256']
    assert json.loads(manifest)['version'] == lock['version']


@pytest.mark.parametrize('relative', ['INSTALL_CANONICAL_PORTABLE.ps1', 'tools/bootstrap_integrity.ps1'])
@pytest.mark.parametrize('mutation', ['unchanged', 'lock-version', 'manifest-bytes'])
def test_shared_leaf_self_check_in_fake_root(tmp_path, relative, mutation):
    """Only AST-extract the read-only helper; never execute the installer entry."""
    packet = tmp_path / 'packet'
    (packet / 'kmtech_shared/powershell').mkdir(parents=True)
    for name in ['kmtech_shared.lock.json', 'kmtech_shared.manifest.json',
                 'kmtech_shared/powershell/portable.ps1',
                 'kmtech_shared/powershell/data_generation.ps1',
                 'kmtech_shared/powershell/ps51_module_prologue.ps1']:
        shutil.copyfile(ROOT / name, packet / name)
    if mutation == 'lock-version':
        path = packet / 'kmtech_shared.lock.json'
        lock = json.loads(path.read_bytes())
        lock['version'] = 'mismatched-version'
        path.write_text(json.dumps(lock))
    elif mutation == 'manifest-bytes':
        with (packet / 'kmtech_shared.manifest.json').open('ab') as stream:
            stream.write(b' ')
    script = tmp_path / 'self-check.ps1'
    script.write_text('''param($Source,$Root)
$ErrorActionPreference='Stop'
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile($Source,[ref]$tokens,[ref]$errors)
$functions=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Get-LabelSharedPortableLeafPath'},$true))
if($functions.Count -ne 1){exit 3}
. ([scriptblock]::Create($functions[0].Extent.Text))
try { $leaf=Get-LabelSharedPortableLeafPath $Root; if(-not (Test-Path -LiteralPath $leaf)){exit 4}; exit 0 } catch { exit 2 }
''', encoding='utf-8-sig')
    engine = Path('C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe')
    if not engine.is_file():
        pytest.skip('Windows PowerShell 5.1 required')
    result = subprocess.run([str(engine), '-NoLogo', '-NoProfile', '-NonInteractive',
                             '-ExecutionPolicy', 'Bypass', '-File', str(script),
                             str(ROOT / relative), str(packet)], capture_output=True,
                            creationflags=subprocess.CREATE_NO_WINDOW)
    assert result.returncode == (0 if mutation == 'unchanged' else 2)


def test_atomic_fixture_pins_match_shared_bytes():
    fixture = json.loads((ROOT / 'tests/fixtures/lm_atomic_exclusions.json').read_bytes())
    source = (ROOT / 'tests/fixtures/lm_atomic_exclusions.json').read_text()
    # Inspect each shared filename and its adjacent sha without copying expected hashes.
    for relative in ['kmtech_shared/__init__.py', 'kmtech_shared/powershell/portable.ps1']:
        expected = hashlib.sha256((ROOT / relative).read_bytes()).hexdigest()
        assert expected in source
    portable = hashlib.sha256((ROOT / 'kmtech_shared/powershell/portable.ps1').read_bytes()).hexdigest()
    assert portable in (ROOT / 'tests/lm_atomic_guard.ps1').read_text(encoding='utf-8-sig')


def test_removal_harness_includes_census_dependencies():
    source = (ROOT / 'tests/test_supported_removal_convergence.py').read_text()
    for name in ['Get-LabelVerifiedRemovalProcessChain', 'Test-LabelRemovalProcessReferencesRoot',
                 'Test-LabelRemovalCanonicalOwner', 'Get-LabelRemovalNativeProcessStartTime']:
        assert name in source
    assert 'CreationDate=' in source
    assert 'ParentProcessId=' in source


def test_removal_harness_census_runs_without_installer_entry(tmp_path):
    tree = ast.parse((ROOT / 'tests/test_supported_removal_convergence.py').read_text())
    harness = next(ast.literal_eval(node.value) for node in tree.body
                   if isinstance(node, ast.Assign) and any(
                       isinstance(target, ast.Name) and target.id == 'REMOVAL_SCRIPT'
                       for target in node.targets))
    fixture = tmp_path / 'harness.ps1'
    fixture.write_text(harness, encoding='utf-8-sig')
    script = tmp_path / 'census.ps1'
    script.write_text('''param($Fixture,$Source,$Root)
$ErrorActionPreference='Stop'
$tokens=$null;$errors=$null
$h=[Management.Automation.Language.Parser]::ParseFile($Fixture,[ref]$tokens,[ref]$errors)
$assignment=$h.Find({param($n) $n -is [Management.Automation.Language.AssignmentStatementAst] -and $n.Left.Extent.Text -ceq '$names'},$true)
. ([scriptblock]::Create($assignment.Extent.Text))
$ast=[Management.Automation.Language.Parser]::ParseFile($Source,[ref]$tokens,[ref]$errors)
$functions=@($ast.FindAll({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -in $names},$true))
if($functions.Count -ne $names.Count){exit 3}
foreach($f in $functions){ . ([scriptblock]::Create($f.Extent.Text)) }
$mock=$h.Find({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Get-CimInstance'},$true)
. ([scriptblock]::Create($mock.Extent.Text))
$script:processScenario='none'
Assert-LabelRemovalQuiescent $Root
''', encoding='utf-8-sig')
    engine = Path('C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe')
    if not engine.is_file():
        pytest.skip('Windows PowerShell 5.1 required')
    result = subprocess.run([str(engine), '-NoLogo', '-NoProfile', '-NonInteractive',
                             '-ExecutionPolicy', 'Bypass', '-File', str(script), str(fixture),
                             str(ROOT / 'INSTALL_THIS_PC.ps1'), str(tmp_path / 'application')],
                            capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    assert result.returncode == 0


def test_startup_wait_expectations_reference_contract():
    operations = (ROOT / 'docs/spec/operations.md').read_text(encoding='utf-8-sig')
    assert '「잠시 뒤 다시」' in operations
    assert '「이 건만 보류 — 틀린 횟수로 세지 않음」' in operations
    source = (ROOT / 'tests/test_window_first_startup.py').read_text()
    assert '잠시 뒤 다시' in source
    assert '이 건만 보류 — 틀린 횟수로 세지 않음' in source


@pytest.mark.parametrize('proof', ['NEVER_STARTED', 'PROFILE_ABSENT', 'USER_RECEIPT', 'ACCOUNT_UNAVAILABLE', None])
@pytest.mark.parametrize('run_present', [True, False, None])
def test_removal_receipt_and_decisions_share_worker_proof(tmp_path, proof, run_present):
    """Execute the actual proof/receipt AST nodes with an isolated record sink."""
    source = (ROOT / 'INSTALL_THIS_PC.ps1').read_text(encoding='utf-8-sig')
    assert 'worker_removal_proof=$workerRemovalProof' in source
    script = tmp_path / 'receipt-proof.ps1'
    script.write_text('''param($Source,$InputPath,$OutputPath)
$ErrorActionPreference='Stop'
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile($Source,[ref]$tokens,[ref]$errors)
$fn=$ast.Find({param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq 'Invoke-LabelConvergentRemoval'},$true)
$assignment=$fn.Find({param($n) $n -is [Management.Automation.Language.AssignmentStatementAst] -and $n.Left.Extent.Text -ceq '$workerRemovalProof'},$true)
$decision=$fn.Find({param($n) $n -is [Management.Automation.Language.IfStatementAst] -and $n.Extent.Text.Contains("disposition=('WORKER_PART_'+$"+'workerRemovalProof.proof)')},$true)
$record=$fn.Find({param($n) $n -is [Management.Automation.Language.CommandAst] -and $n.GetCommandName() -ceq 'Write-LabelStartupUpdateRecord' -and $n.Extent.Text.Contains('worker_removal_proof=$workerRemovalProof')},$true)
if($null -eq $assignment -or $null -eq $decision -or $null -eq $record){exit 3}
$inputObject=Get-Content -LiteralPath $InputPath -Raw -Encoding UTF8 | ConvertFrom-Json
$WorkerPartProof=$inputObject.worker
$plan=@{decisions=@()};$remaining=@{retained_paths=@()};$markerPath=$OutputPath;$evidence='isolated-evidence'
function Write-LabelStartupUpdateRecord($Path,$Payload){
 [IO.File]::WriteAllText($Path,($Payload|ConvertTo-Json -Depth 8),[Text.UTF8Encoding]::new($false))
}
. ([scriptblock]::Create($assignment.Extent.Text))
. ([scriptblock]::Create($decision.Extent.Text))
. ([scriptblock]::Create($record.Extent.Text))
''', encoding='utf-8-sig')
    worker = None if proof is None else {'proof': proof, 'run_present': run_present}
    incoming = tmp_path / 'input.json'
    outgoing = tmp_path / 'receipt.json'
    incoming.write_text(json.dumps({'worker': worker}))
    engine = Path('C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe')
    if not engine.is_file():
        pytest.skip('Windows PowerShell 5.1 required')
    result = subprocess.run([str(engine), '-NoLogo', '-NoProfile', '-NonInteractive',
                             '-ExecutionPolicy', 'Bypass', '-File', str(script),
                             str(ROOT / 'INSTALL_THIS_PC.ps1'), str(incoming), str(outgoing)],
                            capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    assert result.returncode == 0
    receipt = json.loads(outgoing.read_text())
    shared = receipt['worker_removal_proof']
    decisions = receipt['artifact_dispositions']
    if proof is None:
        assert shared is None
        assert decisions == []
    else:
        assert shared == {'proof': proof, 'worker_run_present': run_present}
        assert decisions == [{'path': 'application', 'disposition': 'WORKER_PART_' + shared['proof'],
                              'worker_run_present': shared['worker_run_present']}]

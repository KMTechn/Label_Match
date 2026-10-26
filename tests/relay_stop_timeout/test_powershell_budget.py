"""Budget contracts for the installer waits selected by ANSWER-1/2."""
import ast
from pathlib import Path
import re
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[2]
IS_CA = (ROOT / 'container_audit_product_host.py').is_file()
PREFIX = 'Canonical' if IS_CA else 'Label'


def has_expression(tmp_path, path, name, expression):
    source = tmp_path / 'source.ps1'
    source.write_text((ROOT / path).read_text(encoding='utf-8-sig'), encoding='utf-8-sig')
    driver = tmp_path / 'inspect.ps1'
    driver.write_text(r'''
param([string]$Source,[string]$Name,[string]$Expression)
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile($Source,[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'source parse failed'}
$node=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $Name},$true))
if($node.Count -ne 1){throw 'selected function missing'}
[Console]::Out.WriteLine($node[0].Extent.Text.Contains($Expression))
''', encoding='utf-8-sig')
    result = subprocess.run([shutil.which('powershell.exe'), '-NoProfile', '-NonInteractive',
                             '-File', str(driver), '-Source', str(source), '-Name', name,
                             '-Expression', expression], capture_output=True, text=True,
                            creationflags=subprocess.CREATE_NO_WINDOW)
    assert result.returncode == 0
    return result.stdout.strip() == 'True'


def test_powershell_relay_budget_is_bound_to_the_python_child_limit():
    source = (ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1').read_text(encoding='utf-8-sig')
    matches = re.findall(r'(?m)^\$Script:' + PREFIX + r'RelayNormalStopTimeoutSeconds\s*=\s*(\d+)\s*$', source)
    assert len(matches) == 1
    tree = ast.parse((ROOT / 'user_relay.py').read_text(encoding='utf-8-sig'))
    child = next(ast.literal_eval(node.value) for node in tree.body if isinstance(node, ast.Assign)
                 and any(isinstance(target, ast.Name) and target.id == 'RELAY_DIRECT_SYNC_PROCESS_TIMEOUT_SECONDS'
                         for target in node.targets))
    assert int(matches[0]) == child + 30


CA = [
    ('INSTALL_CANONICAL_PORTABLE.ps1', 'Stop-CanonicalInstallerRelayPreimage', 'AddSeconds($Script:CanonicalRelayNormalStopTimeoutSeconds)'),
    ('INSTALL_CANONICAL_PORTABLE.ps1', 'Confirm-CanonicalRuntimeQuiescence', 'AddSeconds($Script:CanonicalRelayNormalStopTimeoutSeconds)'),
    ('INSTALL_CANONICAL_PORTABLE.ps1', 'Disable-CanonicalOfficialLegacyRelay', 'AddSeconds($Script:CanonicalRelayNormalStopTimeoutSeconds)'),
    ('INSTALL_CANONICAL_PORTABLE.ps1', 'Get-CanonicalWriterPreimageForQuiesce', '$deadline = (Get-Date).ToUniversalTime().AddSeconds(120)'),
    ('INSTALL_CANONICAL_PORTABLE.ps1', 'Disable-CanonicalWriter', 'AddSeconds(120)'),
    ('INSTALL_CANONICAL_PORTABLE.ps1', 'Enable-CanonicalWriter', '$deadline = (Get-Date).ToUniversalTime().AddSeconds(120)'),
    ('INSTALL_CANONICAL_PORTABLE.ps1', 'Confirm-CanonicalWriterRunning', '$boundary.AddSeconds(120)'),
    ('INSTALL_THIS_PC.ps1', 'Enter-ContainerPlacementWriterFence', '-TimeoutMilliseconds 120000'),
]
LM = [
    ('INSTALL_CANONICAL_PORTABLE.ps1', 'Wait-LabelRelayPreimageExit', 'AddSeconds($Script:LabelRelayNormalStopTimeoutSeconds)'),
    ('INSTALL_CANONICAL_PORTABLE.ps1', 'Start-LabelPlacementSession', 'WaitOne(120000)'),
    ('INSTALL_CANONICAL_PORTABLE.ps1', 'Start-LabelPlacementSession', 'Read($pipe,120000,$null)'),
    ('INSTALL_CANONICAL_PORTABLE.ps1', 'Close-LabelPlacementSession', 'Read($Session.pipe,120000,$null)'),
    ('INSTALL_CANONICAL_PORTABLE.ps1', 'Close-LabelPlacementSession', 'WaitForExit(120000)'),
    ('INSTALL_CANONICAL_PORTABLE.ps1', 'Invoke-LabelDelegatedPlacementHelper', '-TimeoutMilliseconds 120000'),
    ('INSTALL_CANONICAL_PORTABLE.ps1', 'Confirm-LabelCanonicalPlacement', '-TimeoutMilliseconds 120000'),
    ('tools/label_writer_fence.ps1', 'Stop-LabelWriterFence', '$TimeoutMilliseconds = 120000'),
]


@pytest.mark.parametrize('path,name,expression', CA if IS_CA else LM)
def test_selected_wait_uses_the_lane_budget(tmp_path, path, name, expression):
    assert has_expression(tmp_path, path, name, expression)

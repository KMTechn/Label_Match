"""Cold type preparation is outside the existing atomic publication deadline."""
import pytest

from tests.relay_stop_timeout.test_powershell_runtime import run_ps


@pytest.mark.parametrize('engine', ['powershell.exe', 'pwsh.exe'])
@pytest.mark.parametrize('preparation_fails', [False, True])
def test_slow_type_preparation_preserves_the_atomic_operation(tmp_path, engine, preparation_fails):
    script = r'''
$script:prepared=0
function Add-Type([string]$TypeDefinition) {
    $script:prepared++
    [Threading.Thread]::Sleep(1100)
    if(PREPARATION_FAILS){throw 'type-preparation-original-failure'}
    Microsoft.PowerShell.Utility\Add-Type -TypeDefinition @'
public static class KMTechStartupUpdateAtomicFile {
 public static bool MoveFileExW(string source, string target, uint flags) { return true; }
}
'@
}
$ok=$false;$failure=''
try{Invoke-LabelAtomicReplace -Source 'unused-source' -Destination 'unused-target' -Mode WriteThrough;$ok=$true}
catch{$failure=$_.Exception.Message}
[Console]::Out.WriteLine((@{ok=$ok;failure=$failure;prepared=$script:prepared}|ConvertTo-Json -Compress))
'''
    result, _ = run_ps(
        tmp_path, engine, 'INSTALL_THIS_PC.ps1', ['Invoke-LabelAtomicReplace'],
        script.replace('PREPARATION_FAILS', '$true' if preparation_fails else '$false'),
    )
    assert result['prepared'] == 1
    assert result['ok'] == (not preparation_fails)
    assert result['failure'] == ('type-preparation-original-failure' if preparation_fails else '')

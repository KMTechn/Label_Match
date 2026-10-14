"""Native links in isolated folders; no product, GUI, or real known folder."""
import os
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('engine', ['powershell.exe', 'pwsh.exe'])
@pytest.mark.parametrize('scenario', ['owned', 'foreign', 'other_file', 'owned_and_foreign', 'corrupt'])
def test_emergency_link_readback_update_removal_and_foreign_preservation(tmp_path, engine, scenario):
    executable = shutil.which(engine)
    assert executable is not None
    script = tmp_path / 'shortcut.ps1'
    source = str(ROOT / 'tools/bootstrap_integrity.ps1').replace("'", "''")
    shared = str(ROOT / 'kmtech_shared/powershell/portable.ps1').replace("'", "''")
    script.write_text(". '" + shared + "'\n. '" + source + "'\n" + r'''
$ErrorActionPreference='Stop'
$root=Join-Path $env:W178_SHORTCUT_ROOT 'app\current'
[void][IO.Directory]::CreateDirectory($root)
[IO.File]::WriteAllText((Join-Path $root 'launch-label-match.cmd'),'@exit /b')
$path=Get-LabelEmergencyShortcutPath $root
$directory=Split-Path -Parent $path
if($path.StartsWith($root+'\',[StringComparison]::OrdinalIgnoreCase)){throw 'Link entered code inventory.'}
$before=@(Get-BootstrapCodeInventory -Root $root | ConvertTo-Json -Depth 8)
$shell=New-Object -ComObject WScript.Shell
try{
    $status=Install-LabelEmergencyShortcut $root
    if($status.status -cne 'CREATED'){throw 'Link not created.'}
    $link=$shell.CreateShortcut($path)
    if($link.Arguments -cne '--ui tk' -or $link.TargetPath -ine (Join-Path $root 'launch-label-match.cmd')){throw 'Link readback failed.'}
    if($env:W178_SHORTCUT_SCENARIO -eq 'owned'){
        $link.WorkingDirectory=Split-Path -Parent $root;$link.Save()
        if((Install-LabelEmergencyShortcut $root).status -cne 'UPDATED'){throw 'Owned link not updated.'}
        if((Install-LabelEmergencyShortcut $root).status -cne 'RETAINED'){throw 'Owned link not retained.'}
        Remove-LabelEmergencyShortcut $root
        if((Test-Path -LiteralPath $path) -or (Test-Path -LiteralPath $directory)){throw 'Owned link or empty folder retained.'}
    }else{
        if($env:W178_SHORTCUT_SCENARIO -eq 'foreign'){$link.Description='foreign';$link.Save()}
        if($env:W178_SHORTCUT_SCENARIO -eq 'corrupt'){[IO.File]::WriteAllText($path,'foreign bytes')}
        if($env:W178_SHORTCUT_SCENARIO -in @('other_file','owned_and_foreign')){
            [IO.File]::WriteAllText((Join-Path $directory 'foreign.txt'),'preserve')
        }
        $saved=[IO.File]::ReadAllBytes($path)
        if((Install-LabelEmergencyShortcut $root).status -cne 'PRESERVED_FOREIGN'){throw 'Foreign content not preserved.'}
        Remove-LabelEmergencyShortcut $root
        if([Convert]::ToBase64String($saved) -cne [Convert]::ToBase64String([IO.File]::ReadAllBytes($path))){throw 'Foreign link changed.'}
    }
    $after=@(Get-BootstrapCodeInventory -Root $root | ConvertTo-Json -Depth 8)
    if(($before -join '') -cne ($after -join '')){throw 'Code inventory changed.'}
}finally{[void][Runtime.InteropServices.Marshal]::FinalReleaseComObject($shell)}
'PASS'
''', encoding='utf-8-sig')
    environment = dict(os.environ)
    environment['W178_SHORTCUT_ROOT'] = str(tmp_path)
    environment['W178_SHORTCUT_SCENARIO'] = scenario
    result = subprocess.run([executable, '-NoLogo', '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass',
                             '-File', str(script)], env=environment, capture_output=True, text=True,
                            creationflags=subprocess.CREATE_NO_WINDOW, check=False)
    assert result.returncode == 0
    assert 'PASS' in result.stdout


def test_installation_and_removal_use_the_emergency_link_helpers():
    source = (ROOT / 'INSTALL_THIS_PC.ps1').read_text(encoding='utf-8-sig')
    assert 'Install-LabelEmergencyShortcut $installRootFull' in source
    assert 'Remove-LabelEmergencyShortcut $installRootFull' in source

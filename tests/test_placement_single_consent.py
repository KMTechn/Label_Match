"""Single-consent transport guards; no elevation or product-function mocks."""
from pathlib import Path

import pytest

from tests.test_shared_ancestor_protection import run_functions


INSTALLER = Path(__file__).resolve().parents[1] / "INSTALL_CANONICAL_PORTABLE.ps1"


@pytest.mark.parametrize("engine", ("powershell.exe", "pwsh.exe"))
@pytest.mark.parametrize("factory_mode,allow_test_layout", (("0", True), ("1", False)))
def test_product_path_rejects_transport_injection_before_launch(
    tmp_path, engine, factory_mode, allow_test_layout
):
    result = run_functions(
        tmp_path,
        INSTALLER,
        ["InvokeFrozenPlacementHelper"],
        r"""
$marker=Join-Path $env:LM_CONSENT_TEST_ROOT 'injection-executed'
$parameters=@{AllowNoncanonicalLayoutForTest=($env:LM_CONSENT_TEST_LAYOUT -ceq '1')}
$transport={param($launch) [IO.File]::WriteAllText($marker,'unsafe');throw 'Injection executed'}
$rejected=$false
try {[void](InvokeFrozenPlacementHelper $null $parameters $transport)}
catch {
    if($_.Exception.Message -cne 'Placement transport injection requires explicit factory test mode and layout.'){throw}
    $rejected=$true
}
if(-not $rejected -or [IO.File]::Exists($marker)){throw 'Production transport injection was not refused.'}
'PRODUCTION_INJECTION_REFUSED'
""",
        values={
            "KMTECH_FACTORY_INSTALL_TEST_MODE": factory_mode,
            "LM_CONSENT_TEST_LAYOUT": "1" if allow_test_layout else "0",
            "LM_CONSENT_TEST_ROOT": str(tmp_path),
        },
        engine=engine,
    )
    assert result.returncode == 0, result.stderr
    assert "PRODUCTION_INJECTION_REFUSED" in result.stdout
    assert not (tmp_path / "injection-executed").exists()


@pytest.mark.parametrize("engine", ("powershell.exe", "pwsh.exe"))
def test_launch_options_follow_current_token_and_retire_transport(tmp_path, engine):
    result = run_functions(
        tmp_path,
        INSTALLER,
        ["ByteSha", "InvokeFrozenPlacementHelper"],
        r"""
$root=$env:LM_CONSENT_TEST_ROOT
$frozen=[pscustomobject]@{
 root=$root;helper_path=(Join-Path $root 'helper.ps1');helper_sha256=('a'*64)
 integrity_sha256=('b'*64);writer_fence_path=(Join-Path $root 'writer.ps1');writer_fence_sha256=('c'*64)
 data_generation_path=(Join-Path $root 'generation.ps1');data_generation_sha256=('d'*64)
}
$principal=[Security.Principal.WindowsPrincipal]::new([Security.Principal.WindowsIdentity]::GetCurrent())
$elevated=$principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
$transport={param($launch)
 if($launch.ContainsKey('Verb') -eq $elevated){throw 'Current access token dispatch differs.'}
 if(-not $elevated -and $launch.Verb -cne 'RunAs'){throw 'Ordinary token consent is absent.'}
 if($launch.WindowStyle -cne 'Hidden' -or -not $launch.Wait -or -not $launch.PassThru){throw 'Native dispatch contract differs.'}
 [IO.File]::WriteAllText((Join-Path $root 'dispatch-observed'),'observed')
 throw [InvalidOperationException]::new('DISPATCH_OBSERVED')
}
$observed=$false
try {
 [void](InvokeFrozenPlacementHelper $frozen @{
  ElevationLogPath=(Join-Path $root 'placement');AllowNoncanonicalLayoutForTest=$true
 } $transport)
} catch {
 if($_.Exception.Message -cne 'DISPATCH_OBSERVED'){throw}
 $observed=$true
}
if(-not $observed -or -not [IO.File]::Exists((Join-Path $root 'dispatch-observed'))){throw 'Transport seam was not reached.'}
if(@(Get-ChildItem -LiteralPath $root -Filter 'placement-child-*').Count){throw 'Private transport was retained.'}
if($elevated){'CURRENT_TOKEN_ELEVATED_DIRECT'}else{'CURRENT_TOKEN_ORDINARY_RUNAS'}
""",
        values={
            "KMTECH_FACTORY_INSTALL_TEST_MODE": "1",
            "LM_CONSENT_TEST_ROOT": str(tmp_path),
        },
        engine=engine,
    )
    assert result.returncode == 0, result.stderr
    assert any(
        label in result.stdout
        for label in ("CURRENT_TOKEN_ELEVATED_DIRECT", "CURRENT_TOKEN_ORDINARY_RUNAS")
    )
    assert (tmp_path / "dispatch-observed").read_text() == "observed"

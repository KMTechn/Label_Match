"""Review regressions using real functions; no installer, relay or GUI starts."""
from pathlib import Path
import hashlib
import json
import subprocess

import pytest
import auto_update as updates


ROOT = Path(__file__).resolve().parents[1]


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def powershell(tmp_path, code):
    script = tmp_path / "actual-functions.ps1"
    script.write_text("$ErrorActionPreference='Stop'\n" + code, encoding="utf-8-sig")
    result = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-File", str(script)],
                            cwd=tmp_path, capture_output=True, text=True,
                            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    assert result.returncode == 0, result.stdout + result.stderr
    return result.stdout.strip()


@pytest.mark.parametrize("trial", [False, True])
def test_ordinary_user_reopens_native_update_mutex(tmp_path, trial):
    if not updates.ordinary_user_context():
        pytest.skip("native reopening requires an ordinary interactive caller")
    # The unique tmp path hashes to a fresh object; no product mutex is touched.
    sid = updates.current_process_sid()
    with updates.update_lock(tmp_path, sid, trial=trial):
        with updates.update_lock(tmp_path, sid, trial=trial):
            pass


@pytest.mark.parametrize("existing_version", [False, True])
def test_initialize_uses_complete_placement_function_bundle(tmp_path, existing_version):
    current = tmp_path / "current"
    (current / "app").mkdir(parents=True)
    raw = updates.canonical_startup_settings({})
    (current / "app/auto-update-settings.json").write_bytes(raw)
    (current / "portable-manifest.json").write_text(json.dumps({
        "source_commit": "1" * 40, "startup_update_protocol": 1,
    }), encoding="utf-8")
    if existing_version:
        (tmp_path / "versions" / ("1" * 40)).mkdir(parents=True)
    # Load every real top-level function, with the same two preloaded scripts
    # as InvokeFrozenPlacementHelper. No canonical wrapper/Sha function exists.
    output = powershell(tmp_path, f"""
. {quote(ROOT/'tools/bootstrap_integrity.ps1')} -SharedCodeRoot {quote(ROOT)}
. {quote(ROOT/'tools/label_writer_fence.ps1')}
$tokens=$null; $errors=$null
$tree=[Management.Automation.Language.Parser]::ParseFile({quote(ROOT/'INSTALL_THIS_PC.ps1')},[ref]$tokens,[ref]$errors)
if($errors.Count){{throw 'Actual placement parse failed'}}
$bundle=@($tree.EndBlock.Statements | Where-Object {{ $_ -is [Management.Automation.Language.FunctionDefinitionAst] }})
if($bundle.Count -lt 20){{throw 'Incomplete placement function bundle'}}
. ([ScriptBlock]::Create(($bundle.Extent.Text -join [Environment]::NewLine)))
if(Get-Command Sha -ErrorAction SilentlyContinue){{throw 'Canonical wrapper leaked into placement scope'}}
$testOverride=$false; $OperatorSid='S-1-5-21-1-2-3-1001'; $OperatorLocalAppDataRoot={quote(tmp_path/'local')}
$WriterFenceAttemptId='regression'
function Assert-LabelStartupUpdateTrust($Root){{}}
function Save-LabelStartupUpdatePreimage($Root,$Purpose){{return [pscustomobject]@{{path={quote(tmp_path/'preimage')}}}}}
function Set-HardenedCodeAcl($Path,[switch]$Recursive){{}}
function Assert-BootstrapIntegrityRecord($Root){{}}
function Write-BootstrapIntegrityRecord($Root,$CodeRoot){{}}
function Get-CodeInventory($Root){{return @()}}
function Get-InventoryAggregate($Items){{return 'same'}}
function Get-ItemProperty{{return [pscustomobject]@{{HiberbootEnabled=1}}}}
function Write-LabelStartupUpdateRecord($Path,$Payload){{
 if([IO.Path]::GetFileName($Path) -eq 'policy.json'){{$script:settingsDigest=$Payload.settings_sha256}}
}}
function Get-LabelStartupUpdateTaskSpec($Root,$Sid){{return [pscustomobject]@{{name='isolated-regression'}}}}
function Invoke-LabelStartupUpdateTaskMutation($Action){{}}
Initialize-LabelStartupUpdate {quote(current)}
if($script:settingsDigest -cne '{hashlib.sha256(raw).hexdigest()}'){{throw 'Policy did not hash actual packet settings'}}
'INITIALIZED'
""")
    assert output.splitlines() == [
        "startup_update_status=REGISTERED_LOGON_SYSTEM_CODE_ONLY",
        "startup_update_task=isolated-regression", "hiberboot_enabled=1",
        "hiberboot_read_status=READ", "INITIALIZED",
    ]

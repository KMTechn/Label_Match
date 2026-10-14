"""Isolated admin admission, machine placement and first worker startup."""

import os
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

import pytest
from tests.test_shared_ancestor_protection import run_functions
from tests.test_writer_transition import portable_pair  # noqa: F401


INSTALLER = Path(__file__).resolve().parents[1] / "INSTALL_CANONICAL_PORTABLE.ps1"


@pytest.mark.parametrize("replacement", ["Run", "Exec", "Save", "wrong-progid"])
def test_staged_verifier_keeps_machine_shortcut_observation_bounded(tmp_path, replacement):
    from tools.verify_staged_release_installer import (
        StagedInstallerVerificationError, _assert_bootstrap_source,
    )
    original = INSTALLER.with_name("INSTALL_THIS_PC.ps1").read_text(encoding="utf-8-sig")
    if replacement == "wrong-progid":
        changed = original.replace("-ComObject WScript.Shell", "-ComObject Shell.Application", 1)
    else:
        changed = original.replace(".CreateShortcut($shortcutPath)", f".{replacement}($shortcutPath)", 1)
    assert changed != original
    installer = tmp_path / "INSTALL_THIS_PC.ps1"
    installer.write_text(changed, encoding="utf-8-sig")
    helper = INSTALLER.parent / "tools/bootstrap_integrity.ps1"
    # Only parse these commands; no COM operation or installation executes.
    with pytest.raises(StagedInstallerVerificationError):
        _assert_bootstrap_source(installer, helper)


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("case", ["valid", "absent-folders", "other-worker", "missing-local", "relative-local", "wrong-local", "file-local", "link-local", "link-programs", "link-desktop"])
def test_machine_worker_reads_profile_scope_before_plan_without_writing(tmp_path, engine, case):
    result = run_functions(tmp_path, INSTALLER, ["Assert-LabelMachineWorkerReadScope"], r'''
$testMode=$true
$root=[IO.Path]::GetFullPath($env:LM_SCOPE_ROOT)
$profile=Join-Path $root 'profile'
$local=Join-Path $profile 'AppData\Local'
$programs=Join-Path $profile 'AppData\Roaming\Microsoft\Windows\Start Menu\Programs'
$desktop=Join-Path $profile 'Desktop'
$target=Join-Path $root 'preserved-target'
$case=$env:LM_SCOPE_CASE
$directories=if($case -ceq 'absent-folders'){@($profile,$target)}else{@($local,$programs,$desktop,$target)}
foreach($path in $directories){[void][IO.Directory]::CreateDirectory($path)}
$witness=Join-Path $target 'witness.txt';[IO.File]::WriteAllText($witness,'preserved fixture')
$worker=[pscustomobject]@{caller_is_worker=$true;profile_root=$profile;local_app_data=$local}
$env:LOCALAPPDATA=$local
$link=$null
if($case -ceq 'other-worker'){$worker.caller_is_worker=$false;$env:LOCALAPPDATA=''}
if($case -ceq 'missing-local'){$env:LOCALAPPDATA=''}
if($case -ceq 'relative-local'){$env:LOCALAPPDATA='relative'}
if($case -ceq 'wrong-local'){$env:LOCALAPPDATA=Join-Path $profile 'other-local'}
if($case -ceq 'file-local'){[IO.Directory]::Delete($local);[IO.File]::WriteAllText($local,'fixture file')}
if($case -like 'link-*'){
 $link=switch($case){'link-local'{$local};'link-programs'{$programs};'link-desktop'{$desktop}}
 foreach($checked in @($link,$target)){
  if(-not [IO.Path]::GetFullPath($checked).StartsWith($root.TrimEnd('\')+'\',[StringComparison]::OrdinalIgnoreCase)){throw 'Fixture escaped its root'}
 }
 [IO.Directory]::Delete($link)
 [void](New-Item -ItemType Junction -Path $link -Target $target -ErrorAction Stop)
}
$before=[IO.File]::ReadAllBytes($witness)
$failure=$null
try{Assert-LabelMachineWorkerReadScope $worker}catch{$failure=$_}
finally{if($link){[IO.Directory]::Delete($link)}}
if($case -in @('valid','absent-folders','other-worker')){
 if($null -ne $failure){throw 'Valid or deferred worker scope was rejected'}
}elseif($null -eq $failure){throw 'Invalid worker scope reached PlanOnly'}
if(-not (Test-Path -LiteralPath $target -PathType Container) -or
 [Convert]::ToBase64String([IO.File]::ReadAllBytes($witness)) -cne [Convert]::ToBase64String($before)){throw 'Scope read changed the fixture'}
'PASS'
''', engine=engine, values={"LM_SCOPE_ROOT":str(tmp_path), "LM_SCOPE_CASE":case})
    assert result.returncode == 0, "worker scope read failed; isolated native evidence retained"
    assert result.stdout.strip() == "PASS"


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
def test_machine_worker_scope_call_precedes_plan_and_diagnostics_in_real_installer(tmp_path, engine):
    result = run_functions(tmp_path, INSTALLER, ["Assert-LabelMachineWorkerReadScope"], r'''
$calls=@($ast.EndBlock.Statements | Where-Object {
 $_ -is [Management.Automation.Language.PipelineAst] -and
 $_.PipelineElements[0] -is [Management.Automation.Language.CommandAst] -and
 $_.PipelineElements[0].GetCommandName() -ceq 'Assert-LabelMachineWorkerReadScope'
})
$plans=@($ast.EndBlock.Statements | Where-Object {
 $_ -is [Management.Automation.Language.IfStatementAst] -and $_.Clauses[0].Item1.Extent.Text -ceq '$PlanOnly'
})
$diagnostics=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.CommandAst] -and
 $n.GetCommandName() -ceq 'Initialize-LabelMachineDiagnostics'},$true))
if($calls.Count -ne 1 -or $plans.Count -ne 1 -or $diagnostics.Count -ne 1 -or
 $calls[0].Extent.StartOffset -ge $plans[0].Extent.StartOffset -or
 $calls[0].Extent.StartOffset -ge $diagnostics[0].Extent.StartOffset){throw 'Worker scope check occurs after planning or writes'}
'PASS'
''', engine=engine)
    assert result.returncode == 0, "real installer scope order is invalid; native evidence retained"
    assert result.stdout.strip() == "PASS"


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
def test_real_placement_helper_worker_binding_accepts_same_and_other_sid(tmp_path, engine):
    executable = shutil.which(engine)
    if executable is None:
        pytest.skip(f"{engine} is required")
    helper = INSTALLER.with_name("INSTALL_THIS_PC.ps1")
    driver = tmp_path / "worker-binding.ps1"
    driver.write_text(r'''
$ErrorActionPreference='Stop'
$tokens=$null; $errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile(
    $env:LM_WORKER_BINDING_SOURCE,[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'Helper parse failed'}
# Extract the real top-level binding block. No placement/helper body executes.
$blocks=@($ast.EndBlock.Statements | Where-Object {
    $_ -is [Management.Automation.Language.IfStatementAst] -and
    $_.Extent.Text -match '^if\s*\(\$WorkerSid\)\s*\{'
})
if($blocks.Count -ne 1){throw 'Worker binding block is ambiguous'}
$binding=[scriptblock]::Create($blocks[0].Extent.Text)
function Assert-AlreadyElevated {
    $script:elevationChecks++
    if(-not $script:elevated){throw 'Synthetic caller is not elevated'}
}
foreach($case in @('same','other','invalid-sid','wrong-subject','missing-profile','tls','limited')){
    $OperatorSid='S-1-5-21-101-202-303-404'
    $originalOperator=$OperatorSid
    $WorkerSid=if($case -ceq 'other'){'S-1-5-21-101-202-303-505'}else{$OperatorSid}
    if($case -ceq 'invalid-sid'){$WorkerSid='invalid'}
    $DataGenerationSubjectSid=$WorkerSid
    if($case -ceq 'wrong-subject'){$DataGenerationSubjectSid='S-1-5-21-101-202-303-606'}
    $OperatorLocalAppDataRoot=$env:LM_WORKER_BINDING_PROFILE
    if($case -ceq 'missing-profile'){$OperatorLocalAppDataRoot=''}
    $TlsCaBundlePath=if($case -ceq 'tls'){'unapproved-ca'}else{''}
    $script:elevationChecks=0
    $script:elevated=($case -cne 'limited')
    $failure=$null
    try{. $binding}catch{$failure=$_}
    if($script:elevationChecks -ne 1){throw 'Elevation guard was bypassed'}
    if($case -cin @('same','other')){
        if($null -ne $failure){throw 'Valid worker binding was rejected'}
        if($OperatorSid -cne $WorkerSid){throw 'Worker subject binding was lost'}
    }else{
        if($null -eq $failure -or $OperatorSid -cne $originalOperator){throw 'Invalid worker binding was accepted'}
    }
}
'PASS'
''', encoding="utf-8-sig")
    environment = os.environ.copy()
    environment["LM_WORKER_BINDING_SOURCE"] = str(helper)
    environment["LM_WORKER_BINDING_PROFILE"] = str(tmp_path / "profile")
    result = subprocess.run(
        [executable, "-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(driver)],
        cwd=tmp_path, env=environment, capture_output=True, encoding="utf-8",
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0), check=False,
    )
    (tmp_path / "binding-stdout.txt").write_text(result.stdout, encoding="utf-8")
    (tmp_path / "binding-stderr.txt").write_text(result.stderr, encoding="utf-8")
    assert result.returncode == 0, "real helper worker binding failed; native evidence retained"
    assert result.stdout.strip() == "PASS"
    assert not (tmp_path / "profile").exists()


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
def test_kit_admin_entry_accepts_explicit_worker_account(tmp_path, engine):
    executable = shutil.which(engine)
    if executable is None:
        pytest.skip(f"{engine} is required")
    driver = tmp_path / "bind-admin-entry.ps1"
    driver.write_text(
        r"""
$ErrorActionPreference='Stop'
$tokens=$null; $errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile(
    $env:LM_ADMIN_ENTRY_SOURCE,[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'Installer parse failed'}
# Only its real parameter block is loaded; the installer body never executes.
$attributes=(@($ast.ParamBlock.Attributes | ForEach-Object {$_.Extent.Text}) -join "`n")
$binding=[scriptblock]::Create($attributes + "`n" + $ast.ParamBlock.Extent.Text + "`n'BOUND'")
$result=& $binding -WorkerAccount 'S-1-5-21-111-222-333-1001'
if($result -cne 'BOUND'){throw 'Worker target was not accepted'}
'PASS'
""",
        encoding="utf-8-sig",
    )
    environment = os.environ.copy()
    environment["LM_ADMIN_ENTRY_SOURCE"] = str(INSTALLER)
    result = subprocess.run(
        [executable, "-NoLogo", "-NoProfile", "-NonInteractive", "-File", str(driver)],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        encoding="utf-8",
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        check=False,
    )
    (tmp_path / "binding-stdout.txt").write_text(result.stdout, encoding="utf-8")
    (tmp_path / "binding-stderr.txt").write_text(result.stderr, encoding="utf-8")
    assert result.returncode == 0, "canonical entry does not accept the kit worker target"
    assert result.stdout.strip() == "PASS"


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("case", [
    "sid", "account", "session-zero", "missing-account", "missing-inventory",
    "absent-file", "invalid-json", "not-ready", "wrong-sid", "wrong-profile",
    "same-admin", "ordinary-arguments", "ordinary-default", "ordinary-empty-arguments", "unresolved-account", "absent-profile", "session-zero-not-elevated",
])
def test_worker_inventory_admission_precedes_every_write(tmp_path, engine, case):
    # Elevated callers sharing the worker SID defer profile setup to the ordinary session.
    profile = tmp_path / "worker"
    inventory = tmp_path / "inventory.json"
    payload = {"status": "READY", "target_sid": "S-1-5-21-111-222-333-1001",
               "target_profile_root": str(profile)}
    if case == "not-ready":
        payload["status"] = "BLOCKED"
    if case == "wrong-sid":
        payload["target_sid"] = "S-1-5-21-111-222-333-1002"
    if case == "wrong-profile":
        payload["target_profile_root"] = str(tmp_path / "other")
    if case == "same-admin":
        payload["target_sid"] = "S-1-5-21-111-222-333-9999"
    if case != "absent-file":
        inventory.write_text("not JSON" if case == "invalid-json" else json.dumps(payload), encoding="utf-8")
    before = {p.relative_to(tmp_path).as_posix(): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    result = run_functions(tmp_path, INSTALLER, ["ByteSha", "Assert-LabelMachineEvidencePath", "Resolve-LabelMachineWorker"], r'''
$case=$env:LM_ADMIN_CASE
$sid='S-1-5-21-111-222-333-1001'
$caller=[pscustomobject]@{sid='S-1-5-21-111-222-333-9999';elevated=$true;session_id=2}
function Resolve-LabelWorkerSid {param($Account)
 if($env:LM_ADMIN_CASE -ceq 'unresolved-account'){throw 'Synthetic account cannot be resolved'}
 if($Account -ceq 'TEST\worker'){return 'S-1-5-21-111-222-333-1001'}
 return $Account
}
function Get-LabelWorkerProfileRoot {param($Sid) if($env:LM_ADMIN_CASE -ceq 'absent-profile'){return ''}; return $env:LM_ADMIN_PROFILE}
$account=$sid;$path=$env:LM_ADMIN_INVENTORY
if($case -ceq 'account'){$account='TEST\worker'}
if($case -ceq 'session-zero'){$caller.session_id=0}
if($case -ceq 'session-zero-not-elevated'){$caller.session_id=0;$caller.elevated=$false}
if($case -ceq 'same-admin'){$account=$caller.sid;$sid=$caller.sid}
if($case -ceq 'missing-account'){$account=''}
if($case -ceq 'missing-inventory'){$path=''}
if($case -like 'ordinary-*'){$caller.elevated=$false}
if($case -in @('ordinary-default','ordinary-empty-arguments')){$account='';$path=''}
$worker=$null;$failure=$null
try{$worker=Resolve-LabelMachineWorker $caller $account $path ($case -ceq 'ordinary-empty-arguments')}catch{$failure=$_}
if($case -cin @('sid','account','session-zero','same-admin')){
 if($null -ne $failure){throw $failure}
 if($worker.sid -cne $sid -or $worker.inventory_sha256 -cne $env:LM_ADMIN_INVENTORY_SHA){throw 'Worker inventory binding lost'}
 if($worker.local_app_data -cne (Join-Path $env:LM_ADMIN_PROFILE 'AppData\Local')){throw 'Profile differs'}
 if($worker.caller_is_worker -ne $false){throw 'Caller worker identity differs'}
}elseif($case -ceq 'ordinary-default'){
 if($null -ne $failure -or $null -ne $worker){throw 'Ordinary entry changed'}
}else{
 if($null -eq $failure -or $null -ne $worker -or $failure.Exception.Data['worker_pending_failure'] -ne $true){throw 'Invalid worker input accepted'}
}
'PASS'
''', engine=engine, values={"LM_ADMIN_CASE": case, "LM_ADMIN_PROFILE": str(profile),
                            "LM_ADMIN_INVENTORY": str(inventory),
                            "LM_ADMIN_INVENTORY_SHA": hashlib.sha256(inventory.read_bytes()).hexdigest() if inventory.exists() else ""})
    assert result.returncode == 0, "isolated worker admission failed; native evidence retained"
    assert result.stdout.strip() == "PASS"
    after = {p.relative_to(tmp_path).as_posix(): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    assert after.keys() == before.keys() | {"contract.ps1"}
    assert all(after[p] == content for p, content in before.items())


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("case", ["same-console", "ssh-same", "ssh-other", "no-console", "ambiguous", "changed", "invalid-profile", "test-layout", "plan-only", "explicit-empty"])
def test_implicit_admin_entry_binds_one_stable_console_worker(tmp_path, engine, case):
    # Elevated callers sharing the worker SID defer profile setup to the ordinary session.
    result = run_functions(tmp_path, INSTALLER,
        ["ByteSha", "Get-LabelConsoleWorker", "Resolve-LabelMachineWorker", "Assert-LabelMachineEvidencePath"], r'''
$case=$env:LM_CONSOLE_CASE
$workerSid='S-1-5-21-'+(@(101,202,303,404)-join '-')
$otherSid='S-1-5-21-'+(@(101,202,303,505)-join '-')
$caller=[pscustomobject]@{sid=$workerSid;elevated=$true;session_id=7}
if($case -in @('ssh-same','ssh-other')){$caller.session_id=0}
if($case -ceq 'ssh-other'){$caller.sid=$otherSid}
$testMode=($case -ceq 'test-layout');$PlanOnly=($case -ceq 'plan-only');$EvidencePath=''
$script:observations=0
function Resolve-LabelWorkerSid($account){if($account -cne 'console-worker'){throw 'Unexpected account'};return $workerSid}
function Get-LabelWorkerProfileRoot($sid){
 if($sid -cne $workerSid){throw 'Unexpected profile subject'}
 if($case -ceq 'invalid-profile'){return [IO.Path]::GetPathRoot($env:LM_CONSOLE_PROFILE)}
 return $env:LM_CONSOLE_PROFILE
}
function Get-LabelConsoleWorkerObservation {
 $script:observations++
 if($case -in @('test-layout','plan-only','explicit-empty')){throw 'Unexpected native console observation'}
 if($case -ceq 'no-console'){throw 'Console is absent'}
 $account=if($case -ceq 'changed' -and $script:observations -eq 2){'changed-worker'}else{'console-worker'}
 $row=[pscustomobject]@{session_id=7;account=$account}
 if($case -ceq 'ambiguous'){return @($row,$row)}
 return $row
}
$worker=$null;$failure=$null
try{$worker=Resolve-LabelMachineWorker $caller '' '' ($case -ceq 'explicit-empty')}catch{$failure=$_}
if($case -in @('same-console','test-layout','plan-only')){
 if($null -ne $failure -or $null -ne $worker){throw 'Legacy current worker entry changed'}
 if($script:observations -ne $(if($case -ceq 'same-console'){2}else{0})){throw 'Console observation count differs'}
}elseif($case -in @('ssh-same','ssh-other')){
 if($null -ne $failure -or $worker.sid -cne $workerSid -or $worker.target_source -cne 'ACTIVE_CONSOLE' -or
    $worker.profile_root -cne $env:LM_CONSOLE_PROFILE -or $worker.inventory_sha256 -cnotmatch '^[0-9a-f]{64}$' -or
    $worker.caller_is_worker -ne $false -or $script:observations -ne 2){throw 'Implicit worker binding differs'}
}else{
 if($null -eq $failure -or $failure.Exception.Data['worker_pending_failure'] -ne $true -or $null -ne $worker){throw 'Ambiguous worker was accepted'}
}
'PASS'
''', engine=engine, values={"LM_CONSOLE_CASE":case, "LM_CONSOLE_PROFILE":str(tmp_path/"console-profile")})
    assert result.returncode == 0, "implicit console admission failed; native evidence retained"
    assert result.stdout.strip() == "PASS"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["contract.ps1"]


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
def test_console_native_observer_compiles_without_observing_the_host(tmp_path, engine):
    result = run_functions(tmp_path, INSTALLER, ["Get-LabelConsoleWorkerObservation"], r'''
$definitions=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.FunctionDefinitionAst] -and
 $n.Name -ceq 'Get-LabelConsoleWorkerObservation'},$true))
if($definitions.Count -ne 1){throw 'Console observer definition differs'}
$commands=@($definitions[0].FindAll({param($n)$n -is [Management.Automation.Language.CommandAst] -and
 $n.GetCommandName() -ceq 'Add-Type'},$true))
if($commands.Count -ne 1){throw 'Console native type definition differs'}
. ([scriptblock]::Create($commands[0].Extent.Text))
if(-not ('KMTechLabelConsoleWorker' -as [type])){throw 'Console native type is absent'}
'PASS'
''', engine=engine)
    assert result.returncode == 0, "console observer type did not compile; host observation was not called"
    assert result.stdout.strip() == "PASS"


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
def test_product_powershell_process_stops_force_one_explicit_pid(tmp_path, engine):
    root = INSTALLER.parent
    paths = [root / name for name in subprocess.check_output(
        ["git", "-c", "core.autocrlf=false", "ls-files", "*.ps1"], cwd=root,
        text=True).splitlines() if not name.startswith("tests/")]
    inputs = tmp_path / "powershell-members.json"
    inputs.write_text(json.dumps([str(path) for path in paths]), encoding="utf-8")
    result = run_functions(tmp_path, INSTALLER, ["Get-LabelConsoleWorkerObservation"], r'''
$paths=Get-Content -LiteralPath $env:LM_STOP_AST_MEMBERS -Raw -Encoding UTF8 | ConvertFrom-Json
$count=0
foreach($path in $paths){
 $tokens=$null;$errors=$null
 $sourceAst=[Management.Automation.Language.Parser]::ParseFile($path,[ref]$tokens,[ref]$errors)
 if($errors.Count){throw 'Product PowerShell parse failed'}
 foreach($command in @($sourceAst.FindAll({param($n)$n -is [Management.Automation.Language.CommandAst] -and
  $n.GetCommandName() -match '(^|\\)(Stop-Process|spps|kill)$'},$true))){
  $count++
  $parameters=@($command.CommandElements | Where-Object {$_ -is [Management.Automation.Language.CommandParameterAst]})
  $force=@($parameters | Where-Object {$_.ParameterName -ieq 'Force'})
  $ids=@($parameters | Where-Object {$_.ParameterName -ieq 'Id'})
  if($force.Count -ne 1 -or $null -ne $force[0].Argument -or $ids.Count -ne 1 -or
     @($parameters | Where-Object {$_.ParameterName -imatch '^(Name|InputObject)$'}).Count){throw 'Process stop is not an unconditional forced PID stop'}
  $index=[Array]::IndexOf([object[]]@($command.CommandElements),$ids[0])
  $argument=if($null -ne $ids[0].Argument){$ids[0].Argument}else{$command.CommandElements[$index+1]}
  if($argument.Extent.Text -notmatch '^\(?\s*\[int\]\s*\$[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*\s*\)?$'){
   throw 'Process stop does not name one scalar integer PID'
  }
 }
}
@{scripts=@($paths).Count;stop_process_calls=$count;unsafe_calls=0}|ConvertTo-Json -Compress|
 Set-Content -LiteralPath $env:LM_STOP_AST_RESULT -Encoding UTF8
'PASS'
''', engine=engine, values={"LM_STOP_AST_MEMBERS":str(inputs),
                             "LM_STOP_AST_RESULT":str(tmp_path / "stop-process-audit.json")})
    assert result.returncode == 0, "product process stop AST contract failed; native evidence retained"
    assert result.stdout.strip() == "PASS"
    audit = json.loads((tmp_path / "stop-process-audit.json").read_text(encoding="utf-8-sig"))
    assert audit["scripts"] == len(paths) and audit["unsafe_calls"] == 0


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("arguments", ["missing", "invalid-json", "ordinary-empty"])
@pytest.mark.parametrize("preloaded", [False, True])
def test_actual_entry_refuses_before_any_import_or_write(tmp_path, engine, arguments, preloaded):
    product = INSTALLER.read_text(encoding="utf-8-sig")
    caller = "(Get-LabelInstallerCaller)"
    assert product.count(caller) == 1
    identity = "([pscustomobject]@{sid='S-1-5-21-111-222-333-9999';elevated=$false;session_id=2})" if arguments == "ordinary-empty" else "([pscustomobject]@{sid='S-1-5-21-111-222-333-9999';elevated=$true;session_id=0})"
    product = product.replace(caller, identity)
    # Missing explicit input also requires an absent console candidate. Keep the
    # real entry and its rejection path while avoiding observation of the host.
    observer = "function Get-LabelConsoleWorkerObservation {"
    assert product.count(observer) == 1
    product = product.replace(observer, observer + "\n    throw 'Synthetic console worker absent.'")
    driver = tmp_path / "actual-entry.ps1"
    driver.write_text(product, encoding="utf-8-sig")
    bad_inventory = tmp_path / "inventory.json"
    bad_inventory.write_text("not JSON", encoding="utf-8")
    command = [shutil.which(engine), "-NoLogo", "-NoProfile", "-NonInteractive", "-File", str(driver)]
    if preloaded:
        parent = tmp_path / "preloaded-entry.ps1"
        quote = lambda path: "'" + str(path).replace("'", "''") + "'"
        parent.write_text(
            "param([string]$WorkerAccount,[string]$WorkerSurveyInventory)\n"
            + ". " + quote(INSTALLER.parent / "tools/failure_redaction.ps1") + "\n"
            + "function Get-KmtechSinkEnvironment{return @{}}\n"
            + "function Write-KmtechProtectedFailureSidecar {param($Text) "
              "[IO.File]::WriteAllText((Join-Path $PSScriptRoot 'unexpected-diagnostic.txt'),'synthetic');throw 'Unexpected input diagnostic'}\n"
            + "$forward=@{};if($PSBoundParameters.ContainsKey('WorkerAccount')){$forward.WorkerAccount=$WorkerAccount};"
              "if($PSBoundParameters.ContainsKey('WorkerSurveyInventory')){$forward.WorkerSurveyInventory=$WorkerSurveyInventory}\n"
            + "& " + quote(driver) + " @forward\nexit $LASTEXITCODE\n", encoding="utf-8-sig")
        command[-1] = str(parent)
    before = {p: p.read_bytes() for p in tmp_path.iterdir()}
    if arguments == "invalid-json":
        command += ["-WorkerAccount", "S-1-5-21-111-222-333-1001", "-WorkerSurveyInventory", str(bad_inventory)]
    elif arguments == "ordinary-empty":
        command += ["-WorkerAccount", ""]
    result = subprocess.run(command, cwd=tmp_path, capture_output=True, text=True,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    assert result.returncode == 1
    assert result.stdout == ""
    lines = result.stderr.strip().splitlines()
    assert len(lines) == 1 and lines[0].startswith("LABEL_MATCH_INSTALLER_FAILURE=")
    assert json.loads(lines[0].split("=", 1)[1])["reason_code"] == "PENDING_TARGET_INVALID"
    assert {p: p.read_bytes() for p in tmp_path.iterdir()} == before


@pytest.fixture
def pending_install(tmp_path, monkeypatch):
    import auto_update
    import current_user_onboarding as onboarding
    import sys
    from types import SimpleNamespace
    root = tmp_path / "machine" / "current"
    root.mkdir(parents=True)
    marker = root.parent / "update-control" / "worker-pending.json"
    marker.parent.mkdir()
    payload = {"schema": "label-match-machine-worker-pending-v1",
               "install_id": "20261011T000000000Z-" + "1" * 32,
               "target_sid": "S-1-5-21-111-222-333-1001", "source_commit": "a" * 40,
               "version": "v2.0.99", "installed_at": "2026-10-11T00:00:00Z", "inventory_sha256": "b" * 64}
    marker.write_text(json.dumps(payload), encoding="utf-8")
    (root / "portable-manifest.json").write_text(json.dumps({"source_commit": "a" * 40, "version": "v2.0.99"}), encoding="utf-8")
    monkeypatch.setattr(auto_update, "current_process_sid", lambda: payload["target_sid"])
    store = {}
    writes = []
    class Key:
        def __enter__(self): return self
        def __exit__(self, *args): return None
    def read(_key, name):
        if name not in store:
            raise FileNotFoundError
        return store[name], 1
    def write(_key, name, _reserved, kind, value):
        assert kind == 1
        writes.append(value)
        store[name] = value
    def open_key(hive, subkey, _reserved, _access):
        assert hive == "isolated-HKCU" and subkey == r"Software\KMTech\Label_Match"
        return Key()
    monkeypatch.setitem(sys.modules, "winreg", SimpleNamespace(HKEY_CURRENT_USER="isolated-HKCU", KEY_QUERY_VALUE=1,
        KEY_SET_VALUE=2, REG_SZ=1, OpenKey=open_key, CreateKeyEx=open_key, QueryValueEx=read, SetValueEx=write))
    return root, marker, payload, store, writes


def test_worker_completion_keeps_machine_marker_and_is_per_install_id(pending_install):
    import current_user_onboarding as onboarding
    root, marker, payload, store, writes = pending_install
    original = marker.read_bytes()
    pending = onboarding.validate_pending_worker_install(root)
    onboarding._complete_worker_install(pending)
    assert onboarding.validate_pending_worker_install(root) is None
    assert len(writes) == 1
    assert marker.read_bytes() == original
    payload["install_id"] = "20261011T000000001Z-" + "2" * 32
    marker.write_text(json.dumps(payload), encoding="utf-8")
    assert onboarding.validate_pending_worker_install(root)["install_id"] == payload["install_id"]


@pytest.mark.parametrize("case", ["wrong-sid", "wrong-release", "malformed", "changed-during-onboarding"])
def test_pending_failure_cannot_complete_or_begin_onboarding(pending_install, monkeypatch, case):
    import current_user_onboarding as onboarding
    import auto_update
    root, marker, payload, store, writes = pending_install
    if case == "wrong-sid":
        monkeypatch.setattr(auto_update, "current_process_sid", lambda: "S-1-5-21-111-222-333-1002")
    elif case == "wrong-release":
        payload["source_commit"] = "c" * 40
        marker.write_text(json.dumps(payload), encoding="utf-8")
    elif case == "malformed":
        marker.write_text("not JSON", encoding="utf-8")
    if case == "changed-during-onboarding":
        pending = onboarding.validate_pending_worker_install(root)
        marker.write_text(marker.read_text() + " ", encoding="utf-8")
        with pytest.raises(ValueError, match="changed during"):
            onboarding._complete_worker_install(pending)
    else:
        # Native lifecycle verification is covered separately; this incomplete
        # machine fixture isolates the refusal before onboarding's first write.
        from contextlib import nullcontext
        import writer_session_fence
        monkeypatch.setattr(writer_session_fence, "_onboarding_lifecycle_scope",
                            lambda _root, *, recheck=False: nullcontext())
        monkeypatch.setattr(onboarding, "prepare_current_user_data_open", lambda *_a, **_k: pytest.fail("pending refusal opened user state"))
        with pytest.raises(onboarding.CurrentUserOnboardingError):
            onboarding.onboard_current_user(root)
    assert writes == [] and store == {}


def test_pending_absence_preserves_normal_startup(tmp_path):
    import current_user_onboarding as onboarding
    assert onboarding.validate_pending_worker_install(tmp_path / "source") is None
    assert onboarding.validate_pending_worker_install(tmp_path / "current") is None


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("collision", [False, True])
def test_common_shortcut_is_portable_and_preserves_legacy_collision(tmp_path, engine, collision):
    menu = tmp_path / "menu"
    menu.mkdir()
    result = run_functions(tmp_path, INSTALLER, ["Install-LabelMachineWorkerShortcut", "Arg", "ByteSha"], r'''
function Same($a,$b){return [StringComparer]::OrdinalIgnoreCase.Equals([IO.Path]::GetFullPath($a),[IO.Path]::GetFullPath($b))}
function Sha($p){return ByteSha ([IO.File]::ReadAllBytes($p))}
$install=Join-Path $env:LM_SHORTCUT_ROOT 'current'
$menu=Join-Path $env:LM_SHORTCUT_ROOT 'menu'
$path=Join-Path $menu 'KMTech 포장.lnk'
$shell=New-Object -ComObject WScript.Shell
if($env:LM_SHORTCUT_COLLISION -ceq 'True'){
 $old=$shell.CreateShortcut($path);$old.TargetPath=Join-Path $env:LM_SHORTCUT_ROOT 'legacy\Label_Match.exe';$old.Save()
 $before=Sha $path;$refused=$false
 try{[void](Install-LabelMachineWorkerShortcut $install $menu)}catch{$refused=$true}
 if(-not $refused -or (Sha $path) -cne $before){throw 'Legacy shortcut was changed'}
}else{
 $record=Install-LabelMachineWorkerShortcut $install $menu
 $link=$shell.CreateShortcut($record.path)
 if([IO.Path]::GetFileName($link.TargetPath) -cne 'pythonw.exe' -or $link.Arguments -cne ('-I -B "'+(Join-Path $install 'app\main.py')+'"')){throw 'Shortcut differs'}
 # Survey's legacy executable probe compares Label_Match.exe exactly.
 if([IO.Path]::GetFileName($link.TargetPath) -ceq 'Label_Match.exe'){throw 'New common shortcut is a legacy candidate'}
 $before=Sha $path;[void](Install-LabelMachineWorkerShortcut $install $menu)
 if((Sha $path) -cne $before){throw 'Reusing shortcut changed its bytes'}
}
'PASS'
''', engine=engine, values={"LM_SHORTCUT_ROOT": str(tmp_path), "LM_SHORTCUT_COLLISION": str(collision)})
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "PASS"


@pytest.mark.parametrize("ready", [True, False])
def test_existing_onboarding_completes_pending_only_after_ready(pending_install, monkeypatch, ready):
    import current_user_onboarding as onboarding
    from tests.test_current_user_onboarding import isolated_data_generation_release
    from tests.test_supported_lifecycle import _canonical_user_setup, _resume_user
    root, marker, payload, store, writes = pending_install
    # Reuse the established managed lifecycle fixture; only native machine
    # verification, HKCU and registration are isolated.
    isolated_data_generation_release.__wrapped__(monkeypatch)
    fixed, selected, environment, paths = _canonical_user_setup(root.parent.parent / "managed", monkeypatch, active=payload["source_commit"])
    marker = fixed.parent / "update-control" / "worker-pending.json"
    selected_manifest = json.loads((selected / "portable-manifest.json").read_text())
    payload["version"] = selected_manifest["version"]
    marker.write_text(json.dumps(payload), encoding="utf-8")
    original = marker.read_bytes()
    calls = []
    complete = onboarding._complete_worker_install
    def observe(pending):
        report = json.loads(paths.onboarding_report_path.read_text(encoding="utf-8"))
        assert report["status"] == "READY" and report["server_registration_verified"] is True
        assert report["relay_start"]["status"] == "ALIVE"
        calls.append("complete")
        complete(pending)
    monkeypatch.setattr(onboarding, "_complete_worker_install", observe)
    if ready:
        assert _resume_user(selected, environment)["status"] == "READY"
        assert _resume_user(selected, environment)["status"] == "READY"
        assert calls == ["complete"] and len(writes) == 1
    else:
        with pytest.raises(onboarding.CurrentUserOnboardingError):
            _resume_user(selected, environment, launcher=lambda _root: {"status": "UNKNOWN"})
        assert calls == [] and writes == []
    assert marker.read_bytes() == original


def test_canonical_reinstall_does_not_complete_pending_before_relay_proof(pending_install, monkeypatch):
    import current_user_onboarding as onboarding
    import writer_session_fence as fence
    from tests.test_current_user_onboarding import isolated_data_generation_release
    from tests.test_supported_lifecycle import _canonical_user_setup, _resume_user
    root, _marker, payload, _store, writes = pending_install
    isolated_data_generation_release.__wrapped__(monkeypatch)
    fixed, selected, environment, _paths = _canonical_user_setup(
        root.parent.parent / "canonical-reinstall", monkeypatch, active=payload["source_commit"])
    marker = fixed.parent / "update-control/worker-pending.json"
    payload["version"] = json.loads((selected / "portable-manifest.json").read_text())["version"]
    marker.write_text(json.dumps(payload), encoding="utf-8")
    original_marker = marker.read_bytes()
    original = onboarding.onboard_current_user
    # The existing isolated lifecycle fixture owns storage/registration. Supply
    # its canonical authority observation while running real onboarding logic.
    original_control_root = fence.canonical_control_root
    monkeypatch.setattr(fence, "canonical_control_root", lambda values=None:
                        marker.parent / "writer-session" if values is environment else original_control_root(values))
    monkeypatch.setattr(fence, "active_fence", lambda *_args, **kwargs:
                        {"fixture": "canonical-owner"} if kwargs.get("environ") is environment else None)
    monkeypatch.setattr(fence, "_delegation_matches", lambda _fence, *, source, environ:
                        source == "current_user_onboarding" and environ is environment)
    monkeypatch.setattr(onboarding, "onboard_current_user", lambda *args, **kwargs:
                        original(*args, **kwargs, defer_canonical_relay_start=True))
    relay_calls = []
    report = _resume_user(selected, environment, launcher=lambda _root: relay_calls.append("relay"))
    assert report["relay_start"]["status"] == "DEFERRED_CANONICAL_INSTALL"
    assert not relay_calls and not writes
    assert marker.read_bytes() == original_marker
    monkeypatch.setattr(onboarding, "onboard_current_user", original)
    assert _resume_user(selected, environment)["relay_start"]["status"] == "ALIVE"
    assert len(writes) == 1 and marker.read_bytes() == original_marker


@pytest.mark.parametrize("caller", ["same-elevated", "limited", "other-worker"])
def test_same_worker_profile_preparation_leaves_interactive_completion_pending(pending_install, monkeypatch, caller):
    import auto_update
    import current_user_onboarding as onboarding
    from tests.test_current_user_onboarding import isolated_data_generation_release
    from tests.test_supported_lifecycle import _canonical_user_setup, _resume_user
    root, _marker, payload, _store, writes = pending_install
    isolated_data_generation_release.__wrapped__(monkeypatch)
    fixed, selected, environment, paths = _canonical_user_setup(
        root.parent.parent / "managed", monkeypatch, active=payload["source_commit"])
    marker = fixed.parent / "update-control" / "worker-pending.json"
    payload["version"] = json.loads((selected / "portable-manifest.json").read_text())["version"]
    marker.write_text(json.dumps(payload), encoding="utf-8")
    original_marker = marker.read_bytes()
    monkeypatch.setattr(auto_update, "process_security", lambda _pid: {
        "sid": payload["target_sid"] if caller != "other-worker" else "different-worker",
        "elevated": caller != "limited", "session_id": 0})
    original = onboarding.onboard_current_user
    monkeypatch.setattr(onboarding, "onboard_current_user", lambda *args, **kwargs:
        original(*args, **kwargs, defer_machine_worker_relay_start=True))
    relay_calls = []
    autostart_calls = []
    if caller == "same-elevated":
        def prepare_autostart(app_root):
            autostart_calls.append(app_root)
            return {"status": "PASS"}
        report = _resume_user(selected, environment, launcher=lambda _root: relay_calls.append("relay"),
                              autostart=prepare_autostart)
        assert report["status"] == "WORKER_SESSION_PENDING"
        assert report["relay_autostart"]["status"] == "PASS"
        assert report["ledger_status"] == "READY" and paths.ledger_path.is_file()
        assert report["relay_start"]["status"] == "DEFERRED_WORKER_SESSION"
        assert not writes and not relay_calls
        assert autostart_calls == [fixed]
        monkeypatch.setattr(onboarding, "onboard_current_user", original)
        assert _resume_user(selected, environment)["status"] == "READY"
        assert len(writes) == 1
    else:
        with pytest.raises(ValueError, match="elevated pending worker"):
            _resume_user(selected, environment)
        assert not writes and not paths.onboarding_report_path.exists()
    assert marker.read_bytes() == original_marker


def test_wrong_worker_gui_stops_before_data_generation_or_onboarding(pending_install, monkeypatch):
    import Label_Match as app
    import auto_update
    root, marker, payload, store, writes = pending_install
    monkeypatch.setattr(auto_update, "current_process_sid", lambda: "S-1-5-21-111-222-333-1002")
    monkeypatch.setattr(auto_update, "startup_gate", lambda *_a: None)
    monkeypatch.setattr(app, "_label_match_runtime_app_root", lambda: str(root))
    monkeypatch.setattr(app, "dispatch_product_mode", lambda *_a: None)
    monkeypatch.setattr(app, "verify_factory_contract_startup", lambda: None)
    monkeypatch.setattr(app, "check_user_data_open", lambda *_a: pytest.fail("wrong worker opened G"))
    monkeypatch.setattr(app, "onboard_current_user", lambda *_a, **_k: pytest.fail("wrong worker onboards"))
    errors = []
    monkeypatch.setattr(app, "_show_first_run_onboarding_error", errors.append)
    assert app.main([]) == app.ONBOARDING_EXIT_CODE
    assert errors[0].cause_code == "PENDING_WORKER_SID_MISMATCH"
    assert writes == [] and store == {}


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("worker_started", [False, True])
def test_pending_machine_removal_before_and_after_worker_start(tmp_path, engine, worker_started):
    from tests.test_supported_removal_convergence import _owned_artifact, REMOVAL_SCRIPT, REPO
    root = tmp_path / "machine"
    _owned_artifact(root, "current")
    control = root / "update-control"
    control.mkdir()
    (control / "worker-pending.json").write_text(json.dumps({"schema": "label-match-machine-worker-pending-v1"}))
    menu = tmp_path / "program" / "StartMenu"
    menu.mkdir(parents=True)
    setup = run_functions(tmp_path, INSTALLER, ["Install-LabelMachineWorkerShortcut", "Arg", "ByteSha"], r'''
function Same($a,$b){return [StringComparer]::OrdinalIgnoreCase.Equals([IO.Path]::GetFullPath($a),[IO.Path]::GetFullPath($b))}
function Sha($p){return ByteSha ([IO.File]::ReadAllBytes($p))}
[void](Install-LabelMachineWorkerShortcut (Join-Path $env:LM_REMOVE_ROOT 'current') (Join-Path $env:PROGRAMDATA 'StartMenu'))
''', engine=engine, values={"LM_REMOVE_ROOT": str(root), "PROGRAMDATA": str(menu.parent)})
    assert setup.returncode == 0, setup.stderr
    worker = tmp_path / "worker" / "HKCU-completion.json"
    if worker_started:
        worker.parent.mkdir()
        worker.write_text('{"WorkerInstallCompleted":"synthetic-id:commit"}')
    before = worker.read_bytes() if worker_started else None
    script = tmp_path / "remove-machine.ps1"
    script.write_text(REMOVAL_SCRIPT.replace("$ErrorActionPreference='Stop'",
        "$ErrorActionPreference='Stop'\nfunction Get-LabelOfficialLegacyTaskSnapshot {return $null}"), encoding="utf-8-sig")
    env = os.environ.copy()
    env["PROGRAMDATA"] = str(menu.parent)
    result = subprocess.run([shutil.which(engine), "-NoLogo", "-NoProfile", "-NonInteractive", "-File", str(script),
        str(REPO), str(root), "", "False"], env=env, capture_output=True, text=True,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["status"] == "PASS"
    assert not (control / "worker-pending.json").exists()
    assert not (menu / "KMTech 포장.lnk").exists()
    assert worker.read_bytes() == before if worker_started else not worker.exists()


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("outcome", ["pending", "reuse", "helper-unproven", "same-worker", "shortcut-failure", "report-failure", "restore-existing", "switched-version-failure", "post-switch-census-failure"])
def test_machine_install_uses_pinned_helper_and_never_writes_profiles(tmp_path, engine, outcome, monkeypatch):
    for name in ("worker", "admin", "other", "legacy"):
        folder = tmp_path / name
        folder.mkdir()
        (folder / "preserve.dat").write_bytes(("untouched-" + name).encode())
    (tmp_path / "worker/AppData").mkdir()
    profile_members = {name: {p.relative_to(tmp_path / name) for p in (tmp_path / name).rglob("*")}
                       for name in ("worker", "admin", "other", "legacy")}
    originals = {p: p.read_bytes() for p in tmp_path.rglob("*.dat")}
    result = run_functions(tmp_path, INSTALLER, ["Invoke-LabelMachineWorkerInstall", "New-LabelMachineWorkerPendingMarker", "Assert-LabelMachineEvidencePath", "Save", "Save-LabelMachineMetadata", "ByteSha", "Same"], r'''
$testMode=$true
$program=Join-Path $env:LM_MACHINE_ROOT 'program';$env:PROGRAMDATA=$program
[void][IO.Directory]::CreateDirectory($program)
[IO.File]::SetAttributes($program,([IO.File]::GetAttributes($program) -bor [IO.FileAttributes]::Hidden))
$fixtureAppData=Join-Path $env:LM_MACHINE_ROOT 'worker\AppData'
[void][IO.Directory]::CreateDirectory($fixtureAppData)
[IO.File]::SetAttributes($fixtureAppData,([IO.File]::GetAttributes($fixtureAppData) -bor [IO.FileAttributes]::Hidden))
$EvidencePath=Join-Path $program 'KMTech\Label_Match\install-audit\machine-evidence.json'
$install=Join-Path $env:LM_MACHINE_ROOT 'machine\current'
$source=Join-Path $env:LM_MACHINE_ROOT 'source'
$script:placed=0;$script:released=0;$script:authorityReleased=0
$script:userSetup=0;$serverBaseUrl=''
if($env:LM_MACHINE_OUTCOME -ceq 'reuse'){[void](New-Item -ItemType Directory -Path $install)}
function Full($p,$purpose){return [IO.Path]::GetFullPath($p)}
function Sha($p){return ('c'*64)}
function UnquiescedProductWriters{return @()}
function PortableInventory($root){
 $sha=if($env:LM_MACHINE_OUTCOME -in @('switched-version-failure','post-switch-census-failure') -and $root -ceq $install -and $script:placed -eq 0){'0'*64}else{'a'*64}
 return [pscustomobject]@{sha256=$sha;bootstrap_aggregate_sha256=('b'*64);file_count=1;byte_count=4;critical_file_sha256=@{}}
}
function Read-CanonicalStartupLifecycle($source,$root){
 if($env:LM_MACHINE_OUTCOME -ceq 'post-switch-census-failure'){throw 'Synthetic post-switch census failure'}
 return [pscustomobject]@{active_code_root=$root}
}
function Assert-WriterTransition($candidate,$installed){return [pscustomobject]@{compatibility='fixture';candidate_inventory_sha256=('d'*64);installed_inventory_sha256=('d'*64)}}
function FreezePlacementHelper($root,$auditRoot,$run,$critical){
 $frozen=Join-Path $auditRoot 'frozen';[void](New-Item -ItemType Directory -Path (Join-Path $frozen 'tools'))
 [IO.File]::WriteAllText((Join-Path $frozen 'tools\bootstrap_integrity.ps1'),'param($SharedCodeRoot)')
 [IO.File]::WriteAllText((Join-Path $frozen 'writer.ps1'),"`$Script:LabelWriterFenceInventorySha256='"+('d'*64)+"'")
 return [pscustomobject]@{root=$frozen;helper_path=(Join-Path $frozen 'helper.ps1');helper_sha256=('e'*64);integrity_sha256=('f'*64);writer_fence_path=(Join-Path $frozen 'writer.ps1');writer_fence_sha256=('1'*64)}
}
function PinnedFileBytes($path,$digest){return ,[IO.File]::ReadAllBytes($path)}
function Enter-LabelWriterSessionAuthority{param($SessionId,$AttemptId,$OrchestratorSha256,$ReplacementTransactionId,$WriterContractSha256) return [pscustomobject]@{fixture=$true}}
function Exit-LabelWriterSessionAuthority($authority){if(-not $authority.fixture){throw 'Authority lost'};$script:authorityReleased++}
function Get-LabelLaunchFailure($failure,$phase){return [ordered]@{status='FAILED';reason_code='HELPER_CHILD_UNPROVEN'}}
function Start-LabelWriterFence{param($ControlRoot,$Status,$SessionId,$AttemptId,$ReplacementTransactionId,$SessionStartedAtUtc,$OrchestratorSha256,$WriterContractSha256,[switch]$AuthorityOwnedByCaller)
 if($Status -cne 'INSTALLING' -or -not $AuthorityOwnedByCaller){throw 'Machine authority differs'}
}
function Set-LabelWriterFenceDelegation{param($ControlRoot,$Status,$SessionId,$AttemptId,$ReplacementTransactionId,$DelegationToken,$LifetimeSeconds,$DelegatedSources)
 if('worker_enrollment_network' -in $DelegatedSources -or 'current_user_onboarding' -in $DelegatedSources){throw 'Machine entry delegated user writes'}
}
function Stop-LabelWriterFence{param($ControlRoot,$SessionId,$AttemptId,$ReplacementTransactionId) $script:released++}
function Install-LabelMachineWorkerShortcut($root,$menu){
 [void](New-Item -ItemType Directory -Path $menu -ErrorAction SilentlyContinue)
 $path=Join-Path $menu 'KMTech 포장.lnk'
 [IO.File]::WriteAllText($path,'new shortcut')
  if($env:LM_MACHINE_OUTCOME -in @('shortcut-failure','report-failure','restore-existing','switched-version-failure')){
  if($env:LM_MACHINE_OUTCOME -ceq 'report-failure'){function script:Save($Path,$Value){throw 'Synthetic report failure'}}
  throw 'Synthetic shortcut readback failure'
 }
 return [ordered]@{path=$path;sha256=('2'*64)}
}
function Product($Root,$Mode,$Arguments){
 if($env:LM_MACHINE_OUTCOME -cne 'same-worker' -or $Mode -cne '--onboard-current-user' -or
    '--defer-machine-worker-relay-start' -notin $Arguments -or $script:released -ne 1){throw 'Unexpected user setup'}
 $script:userSetup++
}
function InvokeFrozenPlacementHelper($frozen,$parameters){
 $script:placed++
 if($parameters.WorkerSid -cne 'S-1-5-21-111-222-333-1001' -or
    $parameters.DataGenerationSubjectSid -cne $parameters.WorkerSid -or
    $parameters.OperatorLocalAppDataRoot -cne (Join-Path $env:LM_MACHINE_ROOT 'worker\AppData\Local') -or
    $parameters.ExpectedBootstrapScriptSha256 -cne $frozen.helper_sha256 -or
    $parameters.VerifiedBootstrapScriptPath -cne $frozen.helper_path -or
    -not $parameters.BootstrapIntegrityPreloaded -or -not $parameters.WriterFenceFunctionsPreloaded -or
    -not $Script:LabelPlacementSessionRequested){throw 'Pinned machine helper contract differs'}
 foreach($path in $parameters.DataGenerationMarkerPaths){
  if(-not $path.StartsWith($parameters.OperatorLocalAppDataRoot+'\',[StringComparison]::OrdinalIgnoreCase)){throw 'Other profile marker selected'}
 }
 # Placement is the separately tested native helper boundary; this fixture
 # checks orchestration without launching host installation or user modes.
 if($env:LM_MACHINE_OUTCOME -ceq 'helper-unproven'){
  $Script:LabelPlacementSession=[pscustomobject]@{closed=$true;child_exit_unproven=$true}
  throw 'Synthetic unproven placement child'
 }
  if(-not (Test-Path -LiteralPath $parameters.InstallRoot)){[void](New-Item -ItemType Directory -Path $parameters.InstallRoot)}
  if($env:LM_MACHINE_OUTCOME -in @('switched-version-failure','post-switch-census-failure')){
   if($parameters.ExpectedActiveCommit -cne ('5'*40)){throw 'Previous active identity was lost'}
   Save-LabelMachineMetadata (Join-Path $parameters.InstallRoot 'portable-manifest.json') $manifest
   Save-LabelMachineMetadata (Join-Path (Split-Path -Parent $parameters.InstallRoot) 'update-control\active.json') @{sha=$manifest.source_commit}
  }
  return 0
}
function Snapshot{throw 'Administrator read HKCU Run'}
function Restore{throw 'Administrator wrote HKCU Run'}
function InvokeRaw{throw 'Administrator ran user onboarding'}
function StartRaw{throw 'Administrator started worker GUI/relay'}
$worker=[pscustomobject]@{sid='S-1-5-21-111-222-333-1001';local_app_data=(Join-Path $env:LM_MACHINE_ROOT 'worker\AppData\Local');inventory_sha256=('3'*64)}
$manifest=[pscustomobject]@{startup_update_protocol=1;source_commit=('4'*40);version='v2.0.99'}
$failed=$false
$pendingPath=Join-Path (Split-Path -Parent $install) 'update-control\worker-pending.json'
$shortcutPath=Join-Path $program 'StartMenu\KMTech 포장.lnk'
$lifecycle=$null
if($env:LM_MACHINE_OUTCOME -in @('switched-version-failure','post-switch-census-failure')){
 [void](New-Item -ItemType Directory -Path $install)
 $lifecycle=[pscustomobject]@{lifecycle_state='INSTALLED';active_code_root=$install;from_commit=('5'*40)}
 Save-LabelMachineMetadata (Join-Path (Split-Path -Parent $install) 'update-control\policy.json') @{operator_sid=$worker.sid;operator_local_app_data=$worker.local_app_data}
 Save-LabelMachineMetadata (Join-Path (Split-Path -Parent $install) 'update-control\active.json') @{sha=$lifecycle.from_commit}
 Save-LabelMachineMetadata (Join-Path $install 'portable-manifest.json') @{source_commit=$lifecycle.from_commit;version='v2.0.99'}
 Save-LabelMachineMetadata $pendingPath @{schema='label-match-machine-worker-pending-v1';install_id=('20261011T000000000Z-'+('a'*32));source_commit=$lifecycle.from_commit;version='v2.0.99';target_sid=$worker.sid;inventory_sha256=('3'*64)}
}
if($env:LM_MACHINE_OUTCOME -ceq 'restore-existing'){
 [void](New-Item -ItemType Directory -Path (Split-Path -Parent $pendingPath))
 [void](New-Item -ItemType Directory -Path (Split-Path -Parent $shortcutPath))
 [IO.File]::WriteAllText($pendingPath,'old marker');[IO.File]::WriteAllText($shortcutPath,'old shortcut')
}
try{Invoke-LabelMachineWorkerInstall $worker $source $install $manifest $lifecycle ($env:LM_MACHINE_OUTCOME -ceq 'same-worker')}catch{
  if($env:LM_MACHINE_OUTCOME -notin @('helper-unproven','shortcut-failure','report-failure','restore-existing','switched-version-failure','post-switch-census-failure')){throw}
 if($env:LM_MACHINE_OUTCOME -ceq 'report-failure' -and $_.Exception.Message -cne 'Synthetic shortcut readback failure'){throw 'Reporting hid original failure'}
 $failed=$true
}
if($env:LM_MACHINE_OUTCOME -in @('shortcut-failure','report-failure','restore-existing','switched-version-failure','post-switch-census-failure')){
  if(-not $failed -or $script:authorityReleased -ne 1){throw 'Injected failure was lost'}
  $pending=Get-Content -LiteralPath $pendingPath -Raw -Encoding UTF8 | ConvertFrom-Json
  if($pending.source_commit -cne $manifest.source_commit -or $pending.target_sid -cne $worker.sid){throw 'Pending record does not describe retained code'}
  if($env:LM_MACHINE_OUTCOME -ceq 'restore-existing'){
   if([IO.File]::ReadAllText($shortcutPath) -cne 'old shortcut'){throw 'Worker shortcut bytes not restored'}
  }elseif(Test-Path -LiteralPath $shortcutPath){throw 'New worker shortcut survived failure'}
 'PASS';exit 0
}
$audit=Get-Content -LiteralPath $EvidencePath -Raw -Encoding UTF8 | ConvertFrom-Json
if($env:LM_MACHINE_OUTCOME -ceq 'helper-unproven'){
 if(-not $failed -or $audit.status -cne 'RECOVERY_REQUIRED' -or $audit.code_state -cne 'UNKNOWN' -or
    $script:released -ne 0 -or $script:authorityReleased -ne 1 -or $Script:LabelPlacementSessionRequested -or
    (Test-Path -LiteralPath (Join-Path (Split-Path -Parent $install) 'update-control\worker-pending.json'))){throw 'Unproven child custody lost'}
 'PASS';exit 0
}
$pending=Get-Content -LiteralPath (Join-Path (Split-Path -Parent $install) 'update-control\worker-pending.json') -Raw -Encoding UTF8 | ConvertFrom-Json
if($script:placed -ne 1 -or $script:released -ne 1 -or
 $audit.schema -cne 'label-match-canonical-portable-install-v1' -or $audit.status -cne 'PASS_MACHINE_WORKER_PENDING' -or
  $audit.worker_setup -cne 'PENDING' -or $script:userSetup -ne 0 -or $pending.target_sid -cne $worker.sid -or
 $pending.inventory_sha256 -cne $worker.inventory_sha256 -or $pending.source_commit -cne $manifest.source_commit -or
 $pending.install_id -cne $audit.run_id -or $audit.worker_pending.path -cne (Join-Path (Split-Path -Parent $install) 'update-control\worker-pending.json')){throw 'Machine pending evidence differs'}
'PASS'
''', engine=engine, values={"LM_MACHINE_ROOT": str(tmp_path), "LM_MACHINE_OUTCOME": outcome})
    assert result.returncode == 0, result.stderr
    assert ("install_status=PASS_MACHINE_WORKER_PENDING" in result.stdout) == (outcome in {"pending", "reuse", "same-worker"})
    assert "install_status=PASS\n" not in result.stdout
    assert all(p.read_bytes() == before for p, before in originals.items())
    assert {name: {p.relative_to(tmp_path / name) for p in (tmp_path / name).rglob("*")}
            for name in profile_members} == profile_members
    if outcome in {"switched-version-failure", "post-switch-census-failure"}:
        import auto_update
        import current_user_onboarding as onboarding
        managed = tmp_path / "machine"
        pending = json.loads((managed / "update-control/worker-pending.json").read_text(encoding="utf-8-sig"))
        active = json.loads((managed / "update-control/active.json").read_text(encoding="utf-8-sig"))
        manifest = json.loads((managed / "current/portable-manifest.json").read_text(encoding="utf-8-sig"))
        assert pending["source_commit"] == active["sha"] == manifest["source_commit"]
        monkeypatch.setattr(auto_update, "installation_root", lambda _root: managed)
        monkeypatch.setattr(auto_update, "current_process_sid", lambda: pending["target_sid"])
        monkeypatch.setattr(onboarding, "_worker_install_completion", lambda: "")
        assert onboarding.validate_pending_worker_install(managed / "current")["source_commit"] == active["sha"]


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("worker_started", [False, True])
def test_existing_helper_replacement_restores_exact_code_preimage(tmp_path, engine, worker_started):
    source = tmp_path / "source"
    current = tmp_path / "machine" / "current"
    for root, commit, content in ((source, "a" * 40, b"new-code"), (current, "b" * 40, b"prior-code")):
        (root / "app").mkdir(parents=True)
        (root / "app/main.py").write_bytes(content)
        (root / "runtime").mkdir()
        (root / "runtime/pythonw.exe").write_bytes(b"synthetic-runtime")
        (root / "portable-manifest.json").write_text(json.dumps({"schema": "label-match-portable-tree-v1", "source_commit": commit}))
    business = tmp_path / "worker" / "business.dat"
    business.parent.mkdir()
    business.write_bytes(b"new-user-ready" if worker_started else b"legacy-user-state")
    result = run_functions(tmp_path, INSTALLER.parent / "INSTALL_THIS_PC.ps1", ["Get-StrictFullPath", "Test-SamePath", "Get-CodeInventory", "Get-InventoryAggregate"], r'''
. (Join-Path (Split-Path -Parent $env:LM_CONTRACT_SOURCE) 'tools\bootstrap_integrity.ps1') -SharedCodeRoot (Split-Path -Parent $env:LM_CONTRACT_SOURCE)
function Assert-AlreadyElevated{}
$WorkerSid='S-1-5-21-111-222-333-1001';$DataGenerationSubjectSid=$WorkerSid
$OperatorLocalAppDataRoot=Join-Path $env:LM_REPLACEMENT_ROOT 'worker\AppData\Local';$TlsCaBundlePath=''
$identity=@($ast.EndBlock.Statements | Where-Object {
 ($_ -is [Management.Automation.Language.AssignmentStatementAst] -and $_.Left.Extent.Text -ceq '$OperatorSid') -or
 ($_ -is [Management.Automation.Language.IfStatementAst] -and $_.Extent.Text.StartsWith('if ($WorkerSid) {'))
})
if($identity.Count -ne 2){throw 'Worker helper identity block differs'}
foreach($statement in $identity){. ([scriptblock]::Create($statement.Extent.Text))}
if($OperatorSid -cne $WorkerSid){throw 'Helper used administrator SID'}
$IntegrityFileName='bootstrap-integrity.json';$IntegritySchema='label-match-bootstrap-integrity-v1'
$sourceRootFull=Join-Path $env:LM_REPLACEMENT_ROOT 'source'
$applicationParent=Join-Path $env:LM_REPLACEMENT_ROOT 'machine'
$installRootFull=Join-Path $applicationParent 'current';$placementRoot=$installRootFull
$stagingRoot=Join-Path $applicationParent '.current.bootstrap.fixture'
$sourceInventory=@(Get-CodeInventory $sourceRootFull);$sourceAggregate=Get-InventoryAggregate $sourceInventory
[void](Write-BootstrapIntegrityRecord -Root $installRootFull -CodeRoot $installRootFull)
$before=(Get-InventoryAggregate @(Get-CodeInventory $installRootFull))
$body=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.TryStatementAst] -and
 $n.Body.Statements.Count -gt 0 -and $n.Body.Statements[0].Extent.Text -ceq 'Assert-PlacementDataDowngrade $sourceRootFull'},$true))
if($body.Count -ne 1){throw 'Actual placement body differs'}
$testOverride=$true;$applyHardenedAcl=$false;$aclReadbackStatus='NOT_TESTED'
$versionedPlacement=$false;$ReplaceExistingVerifiedPortable=[switch]$true;$replacementApplied=$false
$replacementRollbackRoot='';$writerFenceLease=$null;$releaseLayout='portable'
$Script:StartupUpdatePreimage=$null
function Assert-PlacementDataDowngrade($root){}
function Install-CurrentUserTlsCaBootstrap{throw 'Injected machine readback failure'}
function Initialize-LabelStartupUpdate($root){
 if($OperatorSid -cne 'S-1-5-21-111-222-333-1001'){throw 'Machine task targets administrator'}
}
[void](New-Item -ItemType Directory -Path $stagingRoot)
$refused=$false
try{. ([scriptblock]::Create($body[0].Extent.Text))}catch{
 if($_.Exception.Message -cne 'Verified replacement failed and the prior canonical tree was restored.'){throw}
 $refused=$true
}
if(-not $refused -or (Get-InventoryAggregate @(Get-CodeInventory $installRootFull)) -cne $before){throw 'Existing helper failed exact code restore'}
[void](Assert-BootstrapIntegrityRecord -Root $installRootFull)
'RESTORE_PASS'
''', engine=engine, values={"LM_REPLACEMENT_ROOT": str(tmp_path)})
    assert result.returncode == 0, result.stderr
    assert "RESTORE_PASS" in result.stdout
    assert (current / "app/main.py").read_bytes() == b"prior-code"
    assert business.read_bytes() == (b"new-user-ready" if worker_started else b"legacy-user-state")


@pytest.mark.parametrize("case", ["forward", "reverse", "one-byte", "other-before", "extra-writer"])
def test_admin_writer_delta_accepts_only_exact_reviewed_direction(tmp_path, portable_pair, case):
    from tests.test_writer_transition import _pin, _manifest
    import sys
    candidate = tmp_path / "reviewed-source"
    shutil.copytree(portable_pair[0], candidate)
    (candidate / "app/main.py").write_bytes((INSTALLER.parent / "portable/main.py").read_bytes())
    for relative in ("INSTALL_THIS_PC.ps1", "tools/apply_prepared_update.ps1", "tools/failure_redaction.ps1",
                     "tools/failure_redaction_rules.json"):
        shutil.copyfile(INSTALLER.parent / relative, candidate / relative)
    _pin(candidate)
    _manifest(candidate, "7" * 40)
    installed = tmp_path / "installed"
    shutil.copytree(candidate, installed)
    for relative in ("app/main.py", "app/Label_Match.py", "app/label_admin_pin.py", "app/current_user_onboarding.py", "app/writer_session_fence.py",
                     "INSTALL_THIS_PC.ps1", "tools/label_writer_fence.ps1",
                     "tools/bootstrap_integrity.ps1", "tools/apply_prepared_update.ps1", "tools/failure_redaction.ps1"):
        local = "portable/main.py" if relative == "app/main.py" else relative.removeprefix("app/")
        (installed / relative).write_bytes(subprocess.check_output([
            "git", "-c", "core.autocrlf=false", "-C", str(INSTALLER.parent), "show", "4550330:" + local]))
    _pin(installed)
    _manifest(installed, "5" * 40)
    source = candidate
    if case == "reverse":
        source, installed = installed, candidate
    elif case in {"one-byte", "other-before", "extra-writer"}:
        if case == "one-byte":
            source = tmp_path / "candidate"
            shutil.copytree(candidate, source)
        modified = source if case == "one-byte" else installed
        if case == "extra-writer":
            (modified / "app/extra_writer.py").write_text("from writer_session_fence import writer_sink\n@writer_sink('unexpected_admin_writer')\ndef write(): pass\n")
        else:
            path = modified / "app/current_user_onboarding.py"
            path.write_bytes(path.read_bytes() + b"\n")
        _pin(modified)
        _manifest(modified, "6" * 40)
    # Execute the exact shipped read-only probe. The existing native launcher
    # suite separately tests its attestation and child transport.
    text = INSTALLER.read_text(encoding="utf-8-sig")
    start = text.index("$probe = @'") + len("$probe = @'")
    probe = text[start:text.index("\n'@", start)].lstrip()
    result = subprocess.run([sys.executable, "-I", "-B", "-", str(source), str(installed)],
        input=probe, capture_output=True, text=True,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    (tmp_path / "writer-probe.stdout.log").write_text(result.stdout)
    (tmp_path / "writer-probe.stderr.log").write_text(result.stderr)
    if case == "forward":
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout)["compatibility"] == "PINNED_ADMIN_WORKER_PENDING"
    else:
        assert result.returncode != 0
        assert "WRITER_TRANSITION_" in result.stderr


def test_pending_fixed_bootstrap_checks_the_selected_version(pending_install):
    import current_user_onboarding as onboarding
    root, marker, payload, store, writes = pending_install
    selected = root.parent / "versions" / payload["source_commit"]
    selected.mkdir(parents=True)
    (selected / "portable-manifest.json").write_bytes((root / "portable-manifest.json").read_bytes())
    (root / "portable-manifest.json").write_text(json.dumps({"source_commit": "c" * 40, "version": payload["version"]}))
    active = marker.parent / "active.json"
    active.write_text(json.dumps({"sha": payload["source_commit"]}))
    assert onboarding.validate_pending_worker_install(root)["source_commit"] == payload["source_commit"]
    active.write_text(json.dumps({"sha": "d" * 40}))
    with pytest.raises(onboarding.CurrentUserOnboardingError):
        onboarding.validate_pending_worker_install(root)
    assert writes == []


def test_portable_bootstrap_shows_wrong_worker_before_startup_gate(pending_install, monkeypatch):
    import importlib.util
    import sys
    import auto_update
    import Label_Match as app
    root, marker, payload, store, writes = pending_install
    monkeypatch.setattr(sys, "path", list(sys.path))
    spec = importlib.util.spec_from_file_location("isolated_pending_bootstrap", INSTALLER.parent / "portable/main.py")
    bootstrap = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bootstrap)
    monkeypatch.setattr(bootstrap, "APP_ROOT", root / "app")
    monkeypatch.setattr(sys, "argv", ["main.py"])
    monkeypatch.setattr(auto_update, "current_process_sid", lambda: "S-1-5-21-111-222-333-1002")
    monkeypatch.setattr(auto_update, "startup_gate", lambda *_a, **_k: pytest.fail("wrong worker reached startup gate"))
    errors = []
    monkeypatch.setattr(app, "_show_first_run_onboarding_error", errors.append)
    assert bootstrap.main() == app.ONBOARDING_EXIT_CODE
    assert errors[0].cause_code == "PENDING_WORKER_SID_MISMATCH"
    assert writes == [] and store == {}


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
def test_admin_failure_diagnostics_and_child_temp_stay_in_machine_scope(tmp_path, engine):
    # Keep this fixture under MAX_PATH for the real .NET Framework file stream.
    program = tmp_path.parent / ("diag-" + engine.removesuffix(".exe"))
    result = run_functions(tmp_path, INSTALLER, ["Initialize-LabelMachineDiagnostics"], r'''
$testMode=$true
$env:PROGRAMDATA=$env:LM_DIAGNOSTIC_ROOT
[void][IO.Directory]::CreateDirectory($env:PROGRAMDATA)
[IO.File]::SetAttributes($env:PROGRAMDATA,([IO.File]::GetAttributes($env:PROGRAMDATA) -bor [IO.FileAttributes]::Hidden))
function Full($path,$purpose){return [IO.Path]::GetFullPath($path)}
function Get-KmtechSinkEnvironment{return @{}}
# ACL application requires an elevated token in production. Its exact principal
# set is checked here; file creation and inherited child TEMP remain native.
function Set-Acl {
 [CmdletBinding()]param($LiteralPath,$AclObject)
 $rules=@($AclObject.GetAccessRules($true,$true,[Security.Principal.SecurityIdentifier]))
 if((Split-Path -Leaf $LiteralPath) -like 'tmp-*' -and $rules.Count -eq 2){
  foreach($rule in $rules){if($rule.IdentityReference.Value -notin @('S-1-5-18','S-1-5-32-544')){throw 'Unexpected diagnostic ACL'}}
  if(-not $AclObject.AreAccessRulesProtected){throw 'Diagnostic ACL inherits'}
 }
}
Initialize-LabelMachineDiagnostics
$root=[IO.Path]::GetFullPath([IO.Path]::GetTempPath()).TrimEnd('\')
if(-not $root.StartsWith($env:PROGRAMDATA+'\',[StringComparison]::OrdinalIgnoreCase)){throw 'Diagnostic root escaped machine scope'}
$reference=Write-KmtechProtectedFailureSidecar -Text 'Synthetic machine failure'
if(-not ([string]$reference.path).StartsWith($root+'\',[StringComparison]::OrdinalIgnoreCase)){throw 'Failure trace escaped machine scope'}
$child=& $env:LM_DIAGNOSTIC_ENGINE -NoLogo -NoProfile -NonInteractive -Command "[IO.File]::WriteAllText((Join-Path ([IO.Path]::GetTempPath()) 'inherited.txt'),'machine'); 'CHILD_PASS'"
if($LASTEXITCODE -ne 0 -or $child -cne 'CHILD_PASS' -or -not (Test-Path -LiteralPath (Join-Path $root 'inherited.txt'))){throw 'Child diagnostic temp differs'}
'PASS'
''', engine=engine, values={"LM_DIAGNOSTIC_ROOT": str(program), "LM_DIAGNOSTIC_ENGINE": shutil.which(engine) or engine})
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "PASS"
    assert set(p.name for p in tmp_path.iterdir()) == {"contract.ps1"}


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
def test_native_writer_probe_pin_matches_shipped_powershell_bytes(tmp_path, engine):
    result = run_functions(tmp_path, INSTALLER, ["Assert-WriterTransition"], r'''
$function=@($ast.FindAll({param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -ceq 'Assert-WriterTransition'},$true))
if($function.Count -ne 1){throw 'Writer probe function differs'}
$assignment=@($function[0].FindAll({param($node) $node -is [Management.Automation.Language.AssignmentStatementAst] -and $node.Left.Extent.Text -ceq '$probe'},$true))
if($assignment.Count -ne 1){throw 'Writer probe assignment differs'}
$literal=@($assignment[0].Right.FindAll({param($node) $node -is [Management.Automation.Language.StringConstantExpressionAst] -and $node.StringConstantType -eq 'SingleQuotedHereString'},$true))
if($literal.Count -ne 1){throw 'Writer probe literal differs'}
$declaration=[regex]::Match($function[0].Extent.Text,"-ProbeBytes \`$probeBytes -ExpectedSha256 '([0-9a-f]{64})'")
if(-not $declaration.Success){throw 'Writer probe pin is absent'}
$sha=[Security.Cryptography.SHA256]::Create()
try{$actual=([BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes($literal[0].Value.Replace("`r`n","`n"))))).Replace('-','').ToLowerInvariant()}finally{$sha.Dispose()}
if($actual -cne $declaration.Groups[1].Value){throw 'Shipped writer probe is rejected by its fixed pin'}
'PASS'
''', engine=engine)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "PASS"


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("linked", [False, True])
def test_machine_evidence_refuses_profile_and_reparse_paths_before_writing(tmp_path, engine, linked):
    result = run_functions(tmp_path, INSTALLER, ["Assert-LabelMachineEvidencePath"], r'''
$testMode=$true;$env:PROGRAMDATA=Join-Path $env:LM_EVIDENCE_ROOT 'program'
function Full($path,$purpose){return [IO.Path]::GetFullPath($path)}
$profile=Join-Path $env:LM_EVIDENCE_ROOT 'worker'
[void](New-Item -ItemType Directory -Path $profile)
$preserved=Join-Path $profile 'preserve.dat';[IO.File]::WriteAllText($preserved,'preserved-worker-data')
$EvidencePath=Join-Path $profile 'evidence.json'
if($env:LM_EVIDENCE_LINKED -ceq 'True'){
 $parent=Join-Path $env:PROGRAMDATA 'KMTech\Label_Match'
 [void](New-Item -ItemType Directory -Path $parent)
 [void](New-Item -ItemType Junction -Path (Join-Path $parent 'install-audit') -Target $profile)
 $EvidencePath=Join-Path $parent 'install-audit\evidence.json'
}
$refused=$false
try{Assert-LabelMachineEvidencePath}catch{$refused=$true}
if(-not $refused -or (Test-Path -LiteralPath (Join-Path $profile 'evidence.json')) -or
   [IO.File]::ReadAllText($preserved) -cne 'preserved-worker-data'){throw 'Machine evidence wrote a worker profile'}
'PASS'
''', engine=engine, values={"LM_EVIDENCE_ROOT": str(tmp_path), "LM_EVIDENCE_LINKED": str(linked)})
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "PASS"


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
def test_next_kit_survey_does_not_collect_the_new_common_shortcut(tmp_path, engine):
    survey = Path("D:/KMTech/program-improvement-20260912/work/program/w9setupprep/sub-478/lab-wrapper/Legacy-Survey.ps1")
    if not survey.is_file():
        pytest.skip("read-only kit survey evidence is unavailable")
    result = run_functions(tmp_path, INSTALLER, ["Install-LabelMachineWorkerShortcut", "ByteSha"], r'''
function Same($a,$b){return [StringComparer]::OrdinalIgnoreCase.Equals([IO.Path]::GetFullPath($a),[IO.Path]::GetFullPath($b))}
function Sha($path){return ByteSha ([IO.File]::ReadAllBytes($path))}
function Resolve-KmtechLegacyPath($path){return [IO.Path]::GetFullPath($path)}
function Assert-KmtechOrdinaryPath($path){if((Get-Item -LiteralPath $path).Attributes -band [IO.FileAttributes]::ReparsePoint){throw 'Fixture path is linked'};return $path}
$survey=[Management.Automation.Language.Parser]::ParseFile($env:LM_KIT_SURVEY,[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'Read-only kit survey parse failed'}
foreach($name in @('Get-KmtechLegacySurveyCandidates','Read-KmtechLegacySurveyCandidate')){
 $definitions=@($survey.FindAll({param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -ceq $name},$true))
 if($definitions.Count -ne 1){throw 'Kit survey boundary differs'}
 . ([scriptblock]::Create($definitions[0].Extent.Text))
}
$install=Join-Path $env:LM_SURVEY_ROOT 'current';$menu=Join-Path $env:LM_SURVEY_ROOT 'menu'
[void](New-Item -ItemType Directory -Path (Join-Path $install 'runtime'))
[void](New-Item -ItemType Directory -Path $menu)
$target=Join-Path $install 'runtime\pythonw.exe';[IO.File]::WriteAllText($target,'synthetic-portable-runtime')
$record=Install-LabelMachineWorkerShortcut $install $menu
$shortcut=(New-Object -ComObject WScript.Shell).CreateShortcut($record.path)
if(@(Get-KmtechLegacySurveyCandidates @($menu,$install)).Count){throw 'Next survey collected a new portable candidate'}
$refused=$false;try{Read-KmtechLegacySurveyCandidate $shortcut.TargetPath 'shortcut'}catch{$refused=$true}
if(-not $refused){throw 'Next survey accepted portable shortcut as legacy'}
'PASS'
''', engine=engine, values={"LM_KIT_SURVEY": str(survey), "LM_SURVEY_ROOT": str(tmp_path)})
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "PASS"


PENDING_ORIGIN = "https://pending-origin.test:8443"
PENDING_FIELDS = ["schema", "install_id", "target_sid", "source_commit", "version", "installed_at", "inventory_sha256"]


@pytest.mark.parametrize("engine", ["powershell.exe", "pwsh.exe"])
def test_machine_pending_marker_carries_only_an_explicit_server_origin(tmp_path, engine):
    result = run_functions(tmp_path, INSTALLER, ["New-LabelMachineWorkerPendingMarker"], r"""
$worker=[pscustomobject]@{sid='worker-sid-fixture';inventory_sha256=('3'*64)}
$manifest=[pscustomobject]@{source_commit=('a'*40);version='v2.0.99'}
foreach($origin in @('', $env:LM_PENDING_ORIGIN)){
 $marker=New-LabelMachineWorkerPendingMarker 'install-id-fixture' $worker $manifest $origin
 [Console]::Out.WriteLine(($marker | ConvertTo-Json -Compress))
}
""", engine=engine, values={"LM_PENDING_ORIGIN": PENDING_ORIGIN})
    assert result.returncode == 0, result.stderr
    without, carried = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
    assert list(without) == PENDING_FIELDS
    assert list(carried) == PENDING_FIELDS + ["server_base_url"]
    assert carried["server_base_url"] == PENDING_ORIGIN
    assert without["schema"] == carried["schema"] == "label-match-machine-worker-pending-v1"
    # The machine install passes the ServerBaseUrlOrigin result, '' when -ServerBaseUrl is absent.
    text = INSTALLER.read_text(encoding="utf-8-sig")
    assert text.count("New-LabelMachineWorkerPendingMarker $run $Worker $SourceManifest $serverBaseUrl") == 1
    assert text.count("$serverBaseUrl = ServerBaseUrlOrigin $ServerBaseUrl") == 1


def _write_pending_origin(marker, payload, origin):
    payload["server_base_url"] = origin
    marker.write_text(json.dumps(payload), encoding="utf-8")


@pytest.mark.parametrize("origin", [None, "https://pending-origin.test", PENDING_ORIGIN, "HTTPS://Pending-Origin.test:65535"])
def test_pending_marker_server_origin_reaches_first_run(pending_install, origin):
    import current_user_onboarding as onboarding
    root, marker, payload, store, writes = pending_install
    if origin is not None:
        _write_pending_origin(marker, payload, origin)
    pending = onboarding.validate_pending_worker_install(root)
    assert pending.get("server_base_url") == origin
    assert writes == [] and store == {}


@pytest.mark.parametrize("origin", [
    "http://pending-origin.test", "https://pending-origin.test/path", "https://user@pending-origin.test",
    "https://pending-origin.test?x=1", "https://pending-origin.test#x", "https://pending-origin.test/",
    "https://pending-origin.test:0", "https://pending-origin.test:65536", " https://pending-origin.test",
    "https://pending-origin.test\n", "https://pending_origin.test", "", 443, None,
])
def test_invalid_pending_server_origin_refuses_without_production_fallback(pending_install, monkeypatch, origin):
    import current_user_onboarding as onboarding
    from contextlib import nullcontext
    import writer_session_fence
    root, marker, payload, store, writes = pending_install
    _write_pending_origin(marker, payload, origin)
    with pytest.raises(onboarding.CurrentUserOnboardingError) as caught:
        onboarding.validate_pending_worker_install(root)
    assert caught.value.cause_code == "PENDING_WORKER_INSTALL_INVALID"
    monkeypatch.setattr(writer_session_fence, "_onboarding_lifecycle_scope",
                        lambda _root, *, recheck=False: nullcontext())
    monkeypatch.setattr(onboarding, "prepare_current_user_data_open", lambda *_a, **_k: pytest.fail("invalid origin opened user state"))
    with pytest.raises(onboarding.CurrentUserOnboardingError):
        onboarding.onboard_current_user(root, registration_runner=lambda _paths: pytest.fail("invalid origin registered"))
    assert writes == [] and store == {}


def _label_match_main(tmp_path, monkeypatch, pending):
    import ast
    import os
    import sys
    import auto_update
    import startup_observation
    from current_user_onboarding import CurrentUserOnboardingError, ONBOARDING_EXIT_CODE
    from label_data_generation import DataGenerationError
    from writer_session_fence import WriterFenceError
    monkeypatch.setattr(auto_update, "startup_gate", lambda *_a, **_k: None)
    monkeypatch.setattr(startup_observation, "begin_startup", lambda *_a, **_k: None)
    source = Path(__file__).parents[1] / "Label_Match.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    selected = {"main", "_first_run_server_base_url", "_onboarding_refusal_codes"}
    functions = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in selected]
    assert len(functions) == len(selected)
    onboarded, reports = [], []

    def validate(_root):
        if isinstance(pending, Exception):
            raise pending
        return pending

    namespace = {
        "Path": Path, "__file__": str(source), "os": os, "sys": sys,
        "re": __import__("re"), "_startup_app_stop_requested": lambda: False,
        "WriterFenceError": WriterFenceError, "CurrentUserOnboardingError": CurrentUserOnboardingError,
        "DataGenerationError": DataGenerationError, "ONBOARDING_EXIT_CODE": ONBOARDING_EXIT_CODE,
        "release_identity": lambda _root: {},
        "dispatch_product_mode": lambda args: None,
        "verify_factory_contract_startup": lambda: None,
        "validate_pending_worker_install": validate,
        "check_user_data_open": lambda root: None,
        "_label_match_startup_trace": lambda event, **kw: None,
        "_first_run_onboarding_enabled": lambda: True,
        "_label_match_runtime_app_root": lambda: tmp_path,
        "onboard_current_user": lambda root, **kw: onboarded.append(kw["server_base_url"]) or {},
        "requires_bootstrap_integrity": lambda root: True,
        "LABEL_MATCH_DIRECT_SYNC_SERVER_BASE_URL_ENV": "LABEL_MATCH_DIRECT_SYNC_SERVER_BASE_URL",
        "LABEL_MATCH_DIRECT_SYNC_DEFAULT_SERVER_BASE_URL": "https://product-default.test",
        "_show_first_run_onboarding_error": reports.append,
        "_default_label_match_settings_path": lambda: tmp_path / "settings.json",
        "resolve_data_scope": lambda **kw: str(tmp_path),
        "resource_path": lambda path: path,
        "run_guarded_entrypoint": lambda *_a, **_k: 0,
        "_run_label_match_application": lambda: None,
    }
    constants = [node for node in tree.body if isinstance(node, ast.Assign)
                 and any(isinstance(target, ast.Name) and target.id in {
                     "LABEL_MATCH_NEW_SCREEN_ARGUMENT", "LABEL_MATCH_SCREEN_HOST"}
                     for target in node.targets)]
    exec(compile(ast.Module(body=constants, type_ignores=[]), str(source), "exec"), namespace)
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(source), "exec"), namespace)
    return namespace["main"]([]), onboarded, reports


@pytest.mark.parametrize("case", ["marker", "environment", "absent", "completed", "invalid"])
def test_gui_first_run_uses_environment_then_marker_then_default(tmp_path, monkeypatch, case):
    from current_user_onboarding import CurrentUserOnboardingError, ONBOARDING_EXIT_CODE
    monkeypatch.delenv("LABEL_MATCH_DIRECT_SYNC_SERVER_BASE_URL", raising=False)
    pending = {"schema": "label-match-machine-worker-pending-v1", "server_base_url": PENDING_ORIGIN}
    if case == "environment":
        monkeypatch.setenv("LABEL_MATCH_DIRECT_SYNC_SERVER_BASE_URL", " https://environment-origin.test ")
    elif case == "absent":
        pending.pop("server_base_url")
    elif case == "completed":
        pending = None
    elif case == "invalid":
        pending = CurrentUserOnboardingError("invalid", report_path=tmp_path / "worker-pending.json",
                                             cause_code="PENDING_WORKER_INSTALL_INVALID")
    code, onboarded, reports = _label_match_main(tmp_path, monkeypatch, pending)
    expected = {"marker": [PENDING_ORIGIN], "environment": ["https://environment-origin.test"],
                "absent": ["https://product-default.test"], "completed": ["https://product-default.test"],
                "invalid": []}[case]
    assert onboarded == expected
    assert code == (ONBOARDING_EXIT_CODE if case == "invalid" else 0)
    assert [r.cause_code for r in reports] == (["PENDING_WORKER_INSTALL_INVALID"] if case == "invalid" else [])

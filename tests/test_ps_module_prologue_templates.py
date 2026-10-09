"""Independent Windows PS5.1 scripts must survive a PS7 parent's module path."""
import ast
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _constant(path, name):
    tree = ast.parse((ROOT / path).read_text(encoding="utf-8-sig"))
    return next(ast.literal_eval(node.value) for node in tree.body
                if isinstance(node, ast.Assign)
                and any(isinstance(target, ast.Name) and target.id == name for target in node.targets))


def _relay_script(name):
    tree = ast.parse((ROOT / "tools/direct_sync_relay_install_pack.py").read_text(encoding="utf-8"))
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name)
    namespace = {
        "Sequence": list, "argparse": type("Argparse", (), {"Namespace": object}),
        "Path": Path, "_ps_single_quote": lambda value: "'" + str(value).replace("'", "''") + "'",
        "_quote_cmd": lambda parts: " ".join(parts), "_encoded_powershell_command": lambda script: script,
    }
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(ROOT / "tools/direct_sync_relay_install_pack.py"), "exec"), namespace)
    if name == "_scheduled_task_probe_command":
        return namespace[name]("synthetic-absent-task")
    if name == "_stored_password_task_register_command":
        args = type("Args", (), {"task_run_user": "fixture", "task_run_password_env": "", "task_run_password_file": "fixture"})()
        return namespace[name](task_name="synthetic-task", minute_interval=1, task_action_parts=["fixture.exe"], args=args)
    return namespace[name]("synthetic-absent-task", ["fixture.exe"])


@pytest.mark.parametrize("kind", ["legacy", "task", "probe", "stop", "delete", "register", "provision"])
def test_literal_prologue_precedes_first_os_module_command(kind):
    prologue = (ROOT / "kmtech_shared/powershell/ps51_module_prologue.ps1").read_bytes()
    assert hashlib.sha256(prologue).hexdigest() == "9c5e083b8e332573f3368be3e9592a2e2409aa89891e15942026361be17d0b71"
    if kind in {"legacy", "task"}:
        script = _constant("current_user_scheduled_task.py", "_LEGACY_TASK_READBACK_POWERSHELL" if kind == "legacy" else "_TASK_POWERSHELL")
    elif kind == "provision":
        script = (ROOT / "tools/provision_protected_admin_acl.ps1").read_text(encoding="utf-8-sig")
    else:
        script = _relay_script("_stored_password_task_register_command" if kind == "register" else f"_scheduled_task_{kind}_command")
    assert script.encode("utf-8").count(prologue) == 1
    before, after = script.split(prologue.decode("utf-8"))
    assert not re.search(r"\b(?:Convert(?:To|From)-Json|Get-ScheduledTask|Get-Date|New-TimeSpan|New-Object|Out-Null|Write-Output|Join-Path|Get-FileHash|Get-Acl|Set-Acl|Get-AuthenticodeSignature)\b", before)
    assert re.search(r"\b(?:Convert(?:To|From)-Json|Get-ScheduledTask|Get-Date|Join-Path)\b", after)


def _run_ps7_parent(tmp_path, script):
    child = tmp_path / "native-child.ps1"
    child.write_text(script, encoding="utf-8-sig")
    parent = tmp_path / "ps7-parent.ps1"
    parent.write_text(r"""param([string]$Child,[string]$Probe)
$env:PSModulePath=[IO.Path]::Combine($PSHOME,'Modules')
$start=[Diagnostics.ProcessStartInfo]::new()
$start.FileName=$Child;$start.UseShellExecute=$false;$start.CreateNoWindow=$true
$start.RedirectStandardOutput=$true;$start.RedirectStandardError=$true
foreach($arg in @('-NoProfile','-NonInteractive','-WindowStyle','Hidden','-File',$Probe)){$start.ArgumentList.Add($arg)}
$p=[Diagnostics.Process]::Start($start)
$output=$p.StandardOutput.ReadToEnd();$failure=$p.StandardError.ReadToEnd()
$p.WaitForExit();[Console]::Out.Write($output);[Console]::Error.Write($failure);exit $p.ExitCode
""", encoding="utf-8")
    pwsh = shutil.which("pwsh.exe")
    assert pwsh, "Required PS7 parent is unavailable"
    native = Path(os.environ["SystemRoot"]) / "System32/WindowsPowerShell/v1.0/powershell.exe"
    environment = os.environ.copy()
    environment["TEMP"] = environment["TMP"] = str(tmp_path)
    return subprocess.run([pwsh, "-NoProfile", "-NonInteractive", "-WindowStyle", "Hidden", "-File", str(parent),
                           "-Child", str(native), "-Probe", str(child)], env=environment,
                          capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW)


@pytest.mark.parametrize("kind", ["legacy", "probe", "stop", "delete"])
def test_actual_absent_task_script_under_ps7_parent(tmp_path, kind):
    # Only scheduler access is replaced. Actual PS5.1 JSON commands and the whole
    # product script execute; no host task is observed, registered or removed.
    script = (_constant("current_user_scheduled_task.py", "_LEGACY_TASK_READBACK_POWERSHELL")
              if kind == "legacy" else _relay_script(f"_scheduled_task_{kind}_command"))
    result = _run_ps7_parent(tmp_path, "function Get-ScheduledTask {}\n" + script)
    assert result.returncode == 0, result.stderr
    record = json.loads(result.stdout)
    assert record == ({"exists": False, "name": "direct-sync-relay-label-match-current-pc"} if kind == "legacy"
                      else {"operation": "probe", "state": "ABSENT", "task_name": "synthetic-absent-task"} if kind == "probe"
                      else {"operation": kind, "status": "ABSENT", **({"task_name": "synthetic-absent-task"} if kind == "stop" else {})})


@pytest.mark.parametrize("kind", ["bootstrap", "read"])
def test_actual_acl_template_under_ps7_parent(tmp_path, kind):
    target = tmp_path / "synthetic-profile.json"
    target.write_text("{}", encoding="utf-8")
    prefix = "$env:KMTECH_PROTECTED_ACL_TARGET='" + str(target).replace("'", "''") + "'\n"
    if kind == "bootstrap":
        prefix += "$env:KMTECH_PROTECTED_POWERSHELL_MODULE_ROOT=[IO.Path]::Combine($PSHOME,'Modules')\n"
        prefix += "$env:KMTECH_PROTECTED_POWERSHELL_SECURITY_MANIFEST=[IO.Path]::Combine($PSHOME,'Modules','Microsoft.PowerShell.Security','Microsoft.PowerShell.Security.psd1')\n"
        script = _constant("tools/install_protected_admin.py", "_WINDOWS_POWERSHELL_SECURITY_BOOTSTRAP")
        script += "\n$acl=Get-Acl -LiteralPath $env:KMTECH_PROTECTED_ACL_TARGET\n$hash=Get-FileHash -LiteralPath $env:KMTECH_PROTECTED_ACL_TARGET\n[Console]::WriteLine('PASS_ACTUAL_ACL')\n"
    else:
        script = _constant("tools/install_protected_admin.py", "_READ_EXACT_ACL_SCRIPT")
    result = _run_ps7_parent(tmp_path, prefix + script)
    assert result.returncode == 0, result.stderr
    if kind == "bootstrap":
        assert result.stdout.strip() == "PASS_ACTUAL_ACL"
    else:
        record = json.loads(result.stdout)
        assert set(record) == {"protected", "entries"}
        assert isinstance(record["protected"], bool)
        assert record["entries"] and all(set(entry) == {"sid", "access_type", "rights", "inheritance", "propagation", "inherited"} for entry in record["entries"])

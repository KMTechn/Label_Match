"""First-setup/relay use headless entry points and the worker's native context."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest

import auto_update
import current_user_onboarding as onboarding
import label_match_relay_entry as entry
import user_relay

ROOT = Path(__file__).resolve().parents[1]


def _worker(monkeypatch, tmp_path, identity):
    from contextlib import nullcontext
    import writer_session_fence
    monkeypatch.setattr(writer_session_fence, "_onboarding_lifecycle_scope", lambda *_args, **_kw: nullcontext())
    managed = tmp_path / "installed"
    policy = managed / "update-control/policy.json"
    policy.parent.mkdir(parents=True)
    policy.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(auto_update, "installation_root", lambda *_: managed)
    monkeypatch.setattr(auto_update, "load_policy", lambda *_: {"operator_sid": "worker"})
    monkeypatch.setattr(auto_update, "process_security", lambda *_: identity)


@pytest.mark.parametrize("change", [{"session_id": 0}, {"session_id": -1},
    {"session_id": True}, {"elevated": True}, {"integrity_rid": 12288},
    {"integrity_rid": True}, {"sid": "other"}])
def test_relay_refuses_native_worker_context_before_launch_or_reuse(tmp_path, monkeypatch, change):
    identity = dict(sid="worker", elevated=False, integrity_rid=8192, session_id=2)
    identity.update(change)
    _worker(monkeypatch, tmp_path, identity)
    calls = []
    monkeypatch.setattr(user_relay, "build_user_relay_command", lambda *_: calls.append("command"))
    with pytest.raises(onboarding.CurrentUserOnboardingError):
        user_relay.start_user_relay_process(tmp_path, launcher=lambda *_: calls.append("launch"))
    with pytest.raises(onboarding.CurrentUserOnboardingError):
        user_relay.reuse_or_start_user_relay_process(tmp_path)
    assert calls == []


def test_noninteractive_first_setup_and_relay_leave_shell_calls_and_marker_zero(tmp_path, monkeypatch):
    _worker(monkeypatch, tmp_path, dict(sid="worker", elevated=False, integrity_rid=8192, session_id=0))
    import shell.host as shell
    calls = []
    monkeypatch.setattr(shell, "run_app", lambda *_args, **_kw: calls.append("shell"))
    marker = tmp_path / "shell-fallback.json"
    monkeypatch.setattr(shell, "_save_fallback", lambda *_args, **_kw: marker.write_text("failure"))
    with pytest.raises(onboarding.CurrentUserOnboardingError):
        onboarding.onboard_current_user(tmp_path)
    with pytest.raises(onboarding.CurrentUserOnboardingError):
        entry.main(["--label-match-user-relay"])
    assert calls == [] and not marker.exists()
    # Next interactive worker run passes its native gate, keeps the same
    # copied shell's new-screen eligibility, and dispatches only the relay.
    monkeypatch.setattr(auto_update, "process_security", lambda *_:
        dict(sid="worker", elevated=False, integrity_rid=8192, session_id=2))
    import label_match_product_host as host
    monkeypatch.setattr(host, "dispatch_product_mode", lambda args: calls.append(tuple(args)) or 0)
    assert entry.main(["--label-match-user-relay"]) == 0
    assert shell.web_allowed(tmp_path, "v2.0.106") is True
    assert calls == [("--label-match-user-relay",)] and not marker.exists()


def test_dedicated_entry_rejects_unknown_mode_without_importing_gui(tmp_path):
    script = "import sys;sys.path.insert(0," + repr(str(ROOT)) + ");import label_match_relay_entry as e;"
    script += "\ntry:e.main(['unknown'])\nexcept ValueError:pass\nelse:raise AssertionError('mode accepted')"
    script += "\nassert 'Label_Match' not in sys.modules and 'label_match_screen' not in sys.modules and 'shell.host' not in sys.modules"
    child = subprocess.run([sys.executable, "-B", "-c", script], cwd=tmp_path,
                           capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    assert child.returncode == 0


def test_source_relay_command_names_the_headless_entry():
    command = user_relay.build_user_relay_command(ROOT)
    assert str(ROOT / "label_match_relay_entry.py") in command
    assert not any(Path(value).name in {"main.py", "Label_Match.py"} for value in command)
    assert command[-1] == "--label-match-user-relay"


def test_update_version_launch_preserves_relay_and_gui_entry_boundaries(tmp_path, monkeypatch):
    (tmp_path / "app").mkdir()
    (tmp_path / "app/label_match_relay_entry.py").write_text("", encoding="utf-8")
    commands = []
    monkeypatch.setattr(auto_update, "version_bundle", lambda *_: tmp_path)
    monkeypatch.setattr(auto_update, "version_manifest", lambda *_: {"source_commit": "a" * 40})
    monkeypatch.setattr(auto_update, "start_ordinary_process", lambda command, **_: commands.append(command) or
                        type("Child", (), {"pid": 10})())
    for arguments in [["--label-match-user-relay"], ["--label-match-direct-sync-relay"], []]:
        assert auto_update.launch_version(tmp_path, "a" * 40, arguments) == 10
    assert [Path(command[3]).name for command in commands] == [
        "label_match_relay_entry.py", "label_match_relay_entry.py", "main.py"]


@pytest.mark.parametrize("engine", ["powershell", "pwsh"])
def test_worker_first_run_requires_same_sid_and_positive_session_and_headless_entry(tmp_path, engine):
    from tests.test_shared_ancestor_protection import run_functions
    result = run_functions(tmp_path, ROOT / "INSTALL_CANONICAL_PORTABLE.ps1",
        ["Test-LabelWorkerFirstRunAllowed", "Invoke-LabelWorkerFirstRun"], r"""
Set-StrictMode -Version Latest
$worker=@{sid='worker'}
foreach($caller in @(@{sid='worker';session_id=0},@{sid='worker';session_id=-1},
 @{sid='other';session_id=2},@{sid='worker';session_id=$true})){
 if(Test-LabelWorkerFirstRunAllowed $worker $caller){throw 'first-run gate accepted'}
}
function Get-LabelInstallerCaller{return @{sid='worker';session_id=2}}
function RelayCodeRoot($Root){return $Root}
function Arg([string]$Value){return '"'+$Value+'"'}
$serverBaseUrl=''
function StartRaw([string]$Line){
 if($Line -notmatch 'label_match_relay_entry.py' -or $Line -notmatch '--onboard-current-user' -or $Line -match 'app\\main.py'){throw 'GUI entry started'}
 return 42
}
if((Invoke-LabelWorkerFirstRun $worker 'E:\isolated') -ne 42){throw 'first run not requested'}
'PASS'
""", engine=engine)
    assert result.returncode == 0 and result.stdout.strip() == "PASS"


def test_machine_completion_never_onboards_or_starts_relay_inside_transaction():
    source = (ROOT / "INSTALL_CANONICAL_PORTABLE.ps1").read_text(encoding="utf-8-sig")
    start = source.index("function Invoke-LabelMachineWorkerInstall(")
    end = source.index("if (-not $SourceRoot)", start)
    completion = source[start:end]
    assert "--onboard-current-user" not in completion and "StartRaw" not in completion
    assert "Stop-LabelWriterFence" in completion and "Install-LabelMachineWorkerShortcut" in completion
    assert source.index("Invoke-LabelWorkerFirstRun $machineWorker $install", end) > end


@pytest.mark.parametrize("engine", ["powershell", "pwsh"])
def test_operational_install_without_worker_account_refuses_before_console_or_writes(tmp_path, engine):
    from tests.test_shared_ancestor_protection import run_functions
    result = run_functions(tmp_path, ROOT / "INSTALL_CANONICAL_PORTABLE.ps1", ["Resolve-LabelMachineWorker"], r"""
$testMode=$false;$Uninstall=$false;$PlanOnly=$false
function Get-LabelConsoleWorker{throw 'console observed'}
$refused=$false
try{Resolve-LabelMachineWorker @{sid='caller';session_id=0;elevated=$true} '' '' $false}
catch{if($_.Exception.Message -notmatch 'WorkerAccount'){throw};$refused=$true}
if(-not $refused){throw 'operational install accepted'}
'PASS'
""", engine=engine)
    assert result.returncode == 0 and result.stdout.strip() == "PASS"


@pytest.mark.parametrize("engine", ["powershell", "pwsh"])
@pytest.mark.parametrize("case", ["caller-session-zero", "child-session-zero", "child-elevated", "child-other-sid", "normal"])
def test_canonical_relay_start_checks_native_session_sid_and_elevation(tmp_path, engine, case):
    from tests.test_shared_ancestor_protection import run_functions
    result = run_functions(tmp_path, ROOT / "INSTALL_CANONICAL_PORTABLE.ps1", ["StartRaw"], r"""
$ErrorActionPreference='Stop';$script:started=0
function Initialize-CanonicalUserProcessApi{}
function Get-CanonicalRelaySecurity([int]$ProcessId){
 if($ProcessId -eq $PID){return @{sid='worker';session_id=$(if($env:LM_NATIVE_CASE -ceq 'caller-session-zero'){0}else{2});elevated=$false;integrity_rid=8192}}
 return @{sid=$(if($env:LM_NATIVE_CASE -ceq 'child-other-sid'){'other'}else{'worker'});
  session_id=$(if($env:LM_NATIVE_CASE -ceq 'child-session-zero'){0}else{2});
  elevated=($env:LM_NATIVE_CASE -ceq 'child-elevated');integrity_rid=8192}
}
function Invoke-CimMethod{$script:started++;return @{ReturnValue=0;ProcessId=42}}
$refused=$false
try{$result=StartRaw 'headless relay'}catch{$refused=$true}
if($refused -ne ($env:LM_NATIVE_CASE -cne 'normal')){throw 'native relay gate differs'}
if($env:LM_NATIVE_CASE -ceq 'caller-session-zero' -and $script:started -ne 0){throw 'noninteractive process started'}
if($env:LM_NATIVE_CASE -ceq 'normal' -and $result -ne 42){throw 'ordinary relay not started'}
'PASS'
""", engine=engine, values={"LM_NATIVE_CASE": case})
    assert result.returncode == 0 and result.stdout.strip() == "PASS"

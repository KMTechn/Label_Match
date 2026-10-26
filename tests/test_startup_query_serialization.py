"""Exercise the real Observe query child, doubling only OS process identity."""
import json
import re
import shutil
import subprocess
import time
from functools import partial
from types import SimpleNamespace

import pytest

import auto_update
from tests.test_live_startup_query import _source, _supervisor
from tests.test_recovery_relay_bootstrap import recovery  # noqa: F401
from tests.test_bootstrap_relay_readback import relay_bootstrap  # noqa: F401


REAL_PROCESS_SNAPSHOT = auto_update.process_snapshot


def _shell(name):
    if name == "PS51":
        return str(auto_update.system_executable("WindowsPowerShell/v1.0/powershell.exe"))
    executable = shutil.which("pwsh")
    assert executable, "PowerShell 7 is required for the serialization contract"
    return executable


def _literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def _observe(tmp_path, shell, count, *, parent=None, role="gui"):
    root = tmp_path / "managed/current"
    root.mkdir(parents=True)
    identity = parent or {"pid": 4100, "sha": "a" * 40, "sid": "operator",
                          "created_at": "2026-10-16T10:00:01Z"}
    (root / "portable-manifest.json").write_text(
        json.dumps({"source_commit": identity["sha"]}), encoding="utf-8")
    processes = [dict(ProcessId=identity["pid"] + index, Name="pythonw.exe",
                      ExecutablePath=str(root / "runtime/pythonw.exe"),
                       CommandLine='"' + str(root / "app/main.py") + '"'
                                   + (' --label-match-user-relay' if role == "relay" else ''),
                      CreationDate=identity["created_at"])
                 for index in range(count)]
    process_file = tmp_path / "cim-rows.json"
    process_file.write_text(json.dumps(processes), encoding="utf-8")
    source_file = tmp_path / "query-source.ps1"
    source_file.write_text(_source(), encoding="utf-8-sig")
    # The supervisor copies only its production allowlisted query functions to
    # the child. Put OS doubles inside that copied function's scope, leaving
    # its actual row construction and the supervisor serialization untouched.
    boundary = r'''
trap {
    @{type=$_.Exception.GetType().Name;error_id=$_.FullyQualifiedErrorId;line=$_.InvocationInfo.ScriptLineNumber} |
        ConvertTo-Json -Compress | Set-Content -LiteralPath ERROR_FILE -Encoding UTF8
    throw
}
if (-not ('KMTechStartupUpdateToken' -as [type])) {
    Add-Type 'public static class KMTechStartupUpdateToken { public static bool Ordinary(uint pid) { return true; } }'
}
function Get-CimInstance {
    $items=Get-Content -LiteralPath PROCESS_FILE -Raw | ConvertFrom-Json
    foreach($item in $items){$item}
}
function Invoke-CimMethod { @{ReturnValue=0;Sid=OPERATOR_SID} }
'''.replace("PROCESS_FILE", _literal(process_file)).replace("OPERATOR_SID", _literal(identity["sid"])).replace(
        "ERROR_FILE", _literal(tmp_path / "query-error.json"))
    supervisor = _supervisor(_source())
    # Production uses PS5.1. Also run the identical query/supervisor on PS7,
    # changing only the child executable so both engines cover the contract.
    supervisor = supervisor.replace(
        "Join-Path ([Environment]::SystemDirectory) 'WindowsPowerShell\\v1.0\\powershell.exe'",
        _literal(_shell(shell)))
    script = r'''
$ErrorActionPreference='Stop'
$tokens=$null;$errors=$null
$ast=[Management.Automation.Language.Parser]::ParseFile(SOURCE_FILE,[ref]$tokens,[ref]$errors)
if($errors.Count){throw 'source parse failed'}
foreach($name in @('Assert-LabelStartupNoReparsePoint','Get-StartupUpdateProcesses')){
    $definition=$ast.FindAll({param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -ceq $name},$true)
    if($definition.Count -ne 1){throw 'query function missing or ambiguous'}
    Invoke-Expression $definition[0].Extent.Text
}
$definition=$ast.FindAll({param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -ceq 'Get-StartupUpdateProcesses'},$true)[0]
$text=$definition.Extent.Text
$offset=$definition.Body.Extent.StartOffset-$definition.Extent.StartOffset+1
Invoke-Expression ($text.Substring(0,$offset)+BOUNDARY+$text.Substring($offset))
SUPERVISOR
# The query boundary above supplies the existing synthetic token implementation.
function Initialize-LabelStartupUpdateProcessToken {}
Invoke-LabelStartupReadOnlyQuery -QueryMode Observe -Root ROOT -OperatorSid OPERATOR_SID
'''.replace("SOURCE_FILE", _literal(source_file)).replace("BOUNDARY", _literal(boundary)).replace(
        "SUPERVISOR", supervisor).replace("ROOT", _literal(root)).replace("OPERATOR_SID", _literal(identity["sid"]))
    driver = tmp_path / "real-query.ps1"
    driver.write_text(script, encoding="utf-8-sig")
    result = subprocess.run([_shell(shell), "-NoLogo", "-NoProfile", "-NonInteractive",
                             "-ExecutionPolicy", "Bypass", "-File", str(driver)],
                            capture_output=True, text=True,
                            creationflags=subprocess.CREATE_NO_WINDOW)
    status = {"rc": result.returncode,
              "query_codes": sorted(set(re.findall(r"STARTUP_[A-Z_]+", result.stderr)))}
    (tmp_path / "query-status.json").write_text(json.dumps(status), encoding="utf-8")
    assert result.returncode == 0, status
    return result.stdout.strip()


@pytest.mark.parametrize("shell", ["PS51", "PS7"])
@pytest.mark.parametrize("count", [0, 1, 2])
def test_observe_real_child_returns_one_array(tmp_path, shell, count):
    rows = json.loads(_observe(tmp_path, shell, count))
    assert isinstance(rows, list)
    assert len(rows) == count
    assert all(isinstance(row, dict) for row in rows)
    assert [row["pid"] for row in rows] == list(range(4100, 4100 + count))
    assert all(row["observable"] is True and row["role"] == "gui" for row in rows)


@pytest.mark.parametrize("raw", ['[[]]', '[[{"pid":1,"observable":true}]]', '["a"]', '[null]'])
def test_snapshot_rejects_non_mapping_rows(monkeypatch, tmp_path, raw):
    monkeypatch.setattr(auto_update, "_run_fixed_helper", lambda *_a, **_k: raw)
    with pytest.raises(ValueError, match="observation is incomplete"):
        auto_update.process_snapshot(tmp_path)


@pytest.mark.parametrize("raw", ['[]', '[{"pid":5,"observable":true,"bootstrap":false}]'])
def test_snapshot_preserves_valid_rows(monkeypatch, tmp_path, raw):
    monkeypatch.setattr(auto_update, "_run_fixed_helper", lambda *_a, **_k: raw)
    assert auto_update.process_snapshot(tmp_path) == json.loads(raw)


def test_absent_claim_relay_bootstrap_uses_real_observe_rows(recovery, monkeypatch, tmp_path):
    recovery.configure("absent")
    recovery.policy["operator_local_app_data"] = str(tmp_path / "operator-local")
    raw = _observe(tmp_path, "PS51", 1, parent=recovery.parent)
    monkeypatch.setattr(auto_update, "process_snapshot", REAL_PROCESS_SNAPSHOT)
    monkeypatch.setattr(auto_update, "_run_fixed_helper", lambda *_a, **_k: raw)
    launch_boundary = auto_update.launch_version

    def launch(*args, relay_deadline, **kwargs):
        assert relay_deadline > auto_update.time.monotonic()
        return launch_boundary(*args, relay_deadline=relay_deadline, **kwargs)

    # The older shared fixture predates propagation of the relay deadline to
    # the external launch boundary; retain that production argument here.
    monkeypatch.setattr(auto_update, "launch_version", launch)
    assert auto_update._bootstrap(recovery.control, ["--label-match-user-relay"], role="relay") == 0
    assert recovery.launches == [{"deferred_at": "2026-10-16T10:00:00Z"}]
    assert recovery.ready_calls[-1]["pid"] == recovery.parent["pid"] + 1
    recovery.unchanged()


@pytest.mark.parametrize("sample", range(1, 6))
def test_completed_bootstrap_owner_uses_real_query_child(relay_bootstrap, monkeypatch, tmp_path, sample):
    import current_user_onboarding
    import user_relay

    fixture = relay_bootstrap
    query_root = tmp_path / "query/managed/current"
    stamp = user_relay._now()
    identity = {"pid": 77, "sha": fixture.version.name, "sid": "operator", "created_at": stamp}
    direct = current_user_onboarding.resolve_current_user_onboarding_paths(fixture.current).direct_sync_root
    fixture.status.update(status="RUNNING", worker_id=user_relay.LABEL_MATCH_WORKER_ID,
                          process_id=77, app_root=str(fixture.current), code_root=str(query_root),
                          direct_sync_root=str(direct), updated_at=stamp)
    monkeypatch.setattr(auto_update, "version_bundle", lambda *a: query_root)
    monkeypatch.setattr(auto_update, "process_snapshot", REAL_PROCESS_SNAPSHOT)

    def query(*args, **kwargs):
        return _observe(tmp_path / "query", "PS51", 1, parent=identity, role="relay")

    monkeypatch.setattr(auto_update, "_run_fixed_helper", query)
    owner = partial(user_relay._existing_user_relay_owner, fixture.current, direct,
                    is_bootstrap=True, allow_fast_path=False)
    started = time.perf_counter()
    result = user_relay._read_started_relay(
        SimpleNamespace(pid=42, poll=lambda: 0), owner, 0.0,
        time.monotonic() + 600.0, True, lambda _: None,
    )
    elapsed = time.perf_counter() - started
    (tmp_path / "owner-query-timing.json").write_text(json.dumps({
        "sample": sample, "seconds": elapsed, "actual_owner": True,
        "actual_process_snapshot": True, "actual_query_child": True,
    }), encoding="utf-8")
    assert result["status"] == "ALIVE" and result["process_id"] == 77
    assert result["bootstrap_process_id"] == 42

"""G46: real Observe classification with synthetic CIM processes only."""
import ast
import copy
import json
from pathlib import Path
import subprocess

import pytest

from recovery_memory_world import World
from test_recovery_relay_bootstrap import (
    recovery,
    updates,
    test_concurrent_relay_requests_launch_once as check_concurrent_requests,
)


ROOT = Path(__file__).resolve().parents[1]


class Observe:
    def __init__(self, directory):
        self.directory = directory
        self.current = directory / "managed/current"
        self.sequence = 0
        self.driver = directory / "observe.ps1"
        self.driver.write_text(r"""
param([string]$Helper, [string]$Fixture)
$ErrorActionPreference = 'Stop'
$inputRows = Get-Content -LiteralPath $Fixture -Raw -Encoding UTF8 | ConvertFrom-Json
Add-Type -TypeDefinition @'
public static class KMTechStartupUpdateToken {
 public static System.Collections.Generic.HashSet<uint> Elevated = new System.Collections.Generic.HashSet<uint>();
 public static bool Ordinary(uint pid) { return !Elevated.Contains(pid); }
}
'@
foreach ($row in $inputRows.processes) {
 if ($row.elevated) { [void][KMTechStartupUpdateToken]::Elevated.Add([uint32]$row.ProcessId) }
}
function Get-CimInstance {
 [CmdletBinding()] param($ClassName, $Filter)
 if ($Filter) { return @($inputRows.processes | Where-Object { ('ProcessId=' + $_.ProcessId) -eq $Filter }) }
 return @($inputRows.processes)
}
function Invoke-CimMethod {
 [CmdletBinding()] param($InputObject, $MethodName)
 if ($InputObject.unobservable) { return @{ReturnValue=1;Sid=$null} }
 return @{ReturnValue=0;Sid=$InputObject.owner}
}
function Assert-LabelStartupNoReparsePoint { param($Path, $Label) }
$tokens=$null; $errors=$null
$tree=[Management.Automation.Language.Parser]::ParseFile($Helper,[ref]$tokens,[ref]$errors)
if ($errors.Count) { throw 'helper parse failed' }
$function=$tree.Find({param($node) $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Get-StartupUpdateProcesses'},$true)
if ($null -eq $function) { throw 'Observe function missing' }
Invoke-Expression $function.Extent.Text
$rows=Get-StartupUpdateProcesses $inputRows.root $inputRows.operator
@{rows=$rows} | ConvertTo-Json -Depth 8 -Compress
""", encoding="utf-8-sig")

    def snapshot(self, rows, *, bootstraps=(), variants=()):
        processes = []
        for index, row in enumerate(rows):
            variant = variants[index] if index < len(variants) else "real"
            bundle = self.current
            if variant in {"outside", "outside_bootstrap"}:
                bundle = self.directory / "outside/current"
            elif variant == "candidate":
                bundle = self.current.parent / "versions" / row["sha"]
            bundle.mkdir(parents=True, exist_ok=True)
            (bundle / "portable-manifest.json").write_text(json.dumps({
                "schema": "label-match-portable-tree-v1", "source_commit": row["sha"],
            }), encoding="utf-8")
            image = bundle / "runtime/pythonw.exe"
            entry = bundle / "app/main.py"
            arguments = " --label-match-user-relay" if row["role"] == "relay" else ""
            if variant in {"bootstrap", "other_runtime", "other_entry", "outside_bootstrap", "console_runtime"}:
                entry = bundle / "app/auto_update.py"
                arguments = " --bootstrap --role " + row["role"] + " --" + arguments
                if variant == "other_runtime":
                    image = bundle / "other-runtime/pythonw.exe"
                elif variant == "other_entry":
                    entry = bundle / "app/other_update.py"
                elif variant == "console_runtime":
                    image = bundle / "runtime/python.exe"
            processes.append({
                "ProcessId": row["pid"], "Name": image.name,
                "ExecutablePath": str(image),
                "CommandLine": f'"{image}" -I -B "{entry}"{arguments}',
                "CreationDate": row["created_at"], "owner": row["sid"],
                "elevated": row.get("ordinary_user") is not True,
                "unobservable": row.get("observable") is not True,
            })
        for index, bootstrap in enumerate(bootstraps):
            base = dict(bootstrap)
            base["pid"] = 9000 + index
            processes.append({
                "ProcessId": base["pid"], "Name": "pythonw.exe",
                "ExecutablePath": str(self.current / "runtime/pythonw.exe"),
                "CommandLine": f'"{self.current / "runtime/pythonw.exe"}" -I -B "{self.current / "app/auto_update.py"}" --bootstrap --role {base["role"]} --'
                               + (" --label-match-user-relay" if base["role"] == "relay" else ""),
                "CreationDate": base["created_at"], "owner": base["sid"],
                "elevated": False, "unobservable": False,
            })
        self.sequence += 1
        fixture = self.directory / f"processes-{self.sequence}.json"
        fixture.write_text(json.dumps({"root": str(self.current), "operator": "operator", "processes": processes}), encoding="utf-8")
        result = subprocess.run([
            "powershell.exe", "-NoProfile", "-NonInteractive", "-WindowStyle", "Hidden",
            "-ExecutionPolicy", "Bypass", "-File", str(self.driver),
            "-Helper", str(ROOT / "tools/apply_prepared_update.ps1"), "-Fixture", str(fixture),
        ], capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW)
        assert result.returncode == 0, "isolated Observe failed; process details withheld"
        return json.loads(result.stdout)["rows"]


def trial_world(phase):
    source = (ROOT / "auto_update.py").read_text(encoding="utf-8-sig")
    world = World("lm-g46", source, phase=phase, writer=None)
    if phase == "APPLIED_PENDING_READY":
        world.files[str(world.control / "active.json")]["sha"] = world.candidate
    world.policy["operator_sid"] = "operator"
    world.state["rollback_at"] = world.at(-10)
    world.ready_trial()
    tree = ast.parse(source)
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "wait_for_rollback_ready")
    exec(compile(ast.Module(body=[node], type_ignores=[]), "auto_update.py", "exec"), world.ns)
    return world


def observed_trial(world, snapshot):
    return world.ns["observe_trial_ready"](world.control, world.policy, world.state, snapshot,
                      world.optional(world.stage / "trial.json"), world.optional(world.stage / "ready.json"),
                      deadline=world.clock + 120)


@pytest.mark.parametrize("restart", [False, True])
def test_rollback_with_live_relay_bootstrap_reaches_rolled_back(tmp_path, restart):
    world = trial_world("ROLLBACK_PENDING_READY")
    observe = Observe(tmp_path)
    rows = observe.snapshot(world.rows, bootstraps=[world.rows[1]])
    if restart:
        rows = observe.snapshot(world.rows, bootstraps=[world.rows[1]])
    assert len(world.ns["writer_processes"](rows, own_pid=world.own_pid)) == 2
    assert sum(row.get("role") == "gui" for row in rows) == 1
    world.ns["process_snapshot"] = lambda *_: copy.deepcopy(rows)
    assert world.ns["wait_for_rollback_ready"](world.control, world.policy) == 0
    assert world.state["phase"] == "ROLLED_BACK"
    assert len(rows) == 2


def test_forward_trial_with_previous_version_bootstrap_still_confirms(tmp_path):
    world = trial_world("APPLIED_PENDING_READY")
    observe = Observe(tmp_path)
    bootstrap = {**world.rows[1], "sha": world.previous}
    # The installed current remains previous while actual trial rows run candidate.
    observe.current.mkdir(parents=True)
    (observe.current / "portable-manifest.json").write_text(json.dumps({"source_commit": world.previous}), encoding="utf-8")
    rows = observe.snapshot(world.rows, variants=["candidate", "candidate"], bootstraps=[bootstrap])
    state, confirmed, _ = observed_trial(world, rows)
    assert confirmed and state["phase"] == "CONFIRMED"
    assert len(world.ns["writer_processes"](rows, own_pid=world.own_pid)) == 2


def test_two_actual_relays_still_fail_rollback_readiness(tmp_path):
    world = trial_world("ROLLBACK_PENDING_READY")
    observe = Observe(tmp_path)
    world.rows.append({**world.rows[1], "pid": world.rows[1]["pid"] + 1})
    rows = observe.snapshot(world.rows, bootstraps=[world.rows[1]])
    world.ns["process_snapshot"] = lambda *_: copy.deepcopy(rows)
    assert world.ns["wait_for_rollback_ready"](world.control, world.policy) == 20
    assert world.state["phase"] == "FAILED"
    assert world.state["reason"] == "previous version readiness failed"


@pytest.mark.parametrize("phase", ["APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY"])
def test_bootstrap_without_actual_relay_cannot_confirm(tmp_path, phase):
    world = trial_world(phase)
    observe = Observe(tmp_path)
    rows = observe.snapshot(world.rows[:1], bootstraps=[world.rows[1]])
    state, confirmed, _ = observed_trial(world, rows)
    assert not confirmed
    assert state["phase"] == phase
    assert not any(row.get("role") == "relay" for row in rows)


@pytest.mark.parametrize("variant", ["outside", "outside_bootstrap", "other_runtime", "other_entry", "console_runtime"])
def test_similar_nonbootstrap_commands_keep_existing_classification(tmp_path, variant):
    world = trial_world("ROLLBACK_PENDING_READY")
    rows = Observe(tmp_path).snapshot([world.rows[1]], variants=[variant])
    assert len(rows) == 1
    assert rows[0]["role"] == "relay"
    assert rows[0]["bootstrap"] is False


@pytest.mark.parametrize("identity", ["foreign", "elevated", "unobservable"])
def test_nonbootstrap_identity_and_permission_observations_are_preserved(tmp_path, identity):
    world = trial_world("ROLLBACK_PENDING_READY")
    row = {**world.rows[1]}
    if identity == "foreign":
        row["sid"] = "other-operator"
    elif identity == "elevated":
        row["ordinary_user"] = False
    else:
        row["observable"] = False
    rows = Observe(tmp_path).snapshot([row])
    assert len(rows) == 1
    if identity == "unobservable":
        assert rows[0]["observable"] is False
    else:
        assert rows[0]["sid"] == row["sid"]
        assert rows[0]["ordinary_user"] == row["ordinary_user"]


def test_gui_bootstrap_keeps_census_and_trial_gui_count(tmp_path):
    world = trial_world("ROLLBACK_PENDING_READY")
    rows = Observe(tmp_path).snapshot(world.rows, bootstraps=[world.rows[0]])
    assert len(world.ns["writer_processes"](rows, own_pid=world.own_pid)) == 2
    assert sum(row.get("role") == "gui" for row in rows) == 1
    state, confirmed, _ = observed_trial(world, rows)
    assert confirmed and state["phase"] == "ROLLED_BACK"


def test_concurrent_g41_requests_reuse_real_relay_with_waiting_bootstraps(recovery, tmp_path, monkeypatch):
    observe = Observe(tmp_path)
    def snapshot(*_):
        bootstrap = {**recovery.parent, "role": "relay"}
        return observe.snapshot(recovery.rows, bootstraps=[bootstrap, bootstrap])
    monkeypatch.setattr(updates, "process_snapshot", snapshot)
    check_concurrent_requests(recovery)

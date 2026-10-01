"""Review regressions using real functions; no installer, relay or GUI starts."""
from pathlib import Path
import hashlib
import json
import subprocess
import time
from contextlib import nullcontext

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


def bootstrap_fixture(monkeypatch, tmp_path):
    old, new = "1" * 40, "2" * 40
    stamp = "2026-10-02T00:00:00+00:00"
    state = updates.apply_transition(updates.prepare_transition(
        previous_sha=old, candidate_sha=new, packet_sha256="3" * 64,
        forward_pin="4" * 64, rollback_pin="5" * 64,
        transition_id="6" * 32, nonce="7" * 32, now=stamp,
    ), writers_absent=True, packet_verified=True, durable_prepared=True, now=stamp)
    policy = {"operator_sid": "operator", "staging_root": str(tmp_path)}
    records = {"transition.json": state, "active.json": {"sha": new}}
    monkeypatch.setattr(updates, "control_root", lambda _: tmp_path)
    monkeypatch.setattr(updates, "load_policy", lambda _: policy)
    monkeypatch.setattr(updates, "ordinary_user_context", lambda *_: True)
    monkeypatch.setattr(updates, "update_lock", lambda *a, **k: nullcontext())
    monkeypatch.setattr(updates, "prepared_pending", lambda *_: False)
    monkeypatch.setattr(updates, "optional_record", lambda p: records.get(p.name))
    monkeypatch.setattr(updates, "read_json", lambda p: records[p.name])
    monkeypatch.setattr(updates, "write_json_durable", lambda p, s: records.__setitem__(p.name, dict(s)))
    monkeypatch.setattr(updates, "request_apply_task", lambda *_: None)
    tick = iter(range(1000))
    monkeypatch.setattr(time, "monotonic", lambda: next(tick))
    return old, new, state, records


@pytest.mark.parametrize("failed_role", ["gui", "relay"])
def test_trial_launch_failure_waits_and_starts_previous_code(monkeypatch, tmp_path, failed_role):
    old, new, original, records = bootstrap_fixture(monkeypatch, tmp_path)
    calls = []
    def launch(control, sha, args, **kwargs):
        role = "relay" if args == ["--label-match-user-relay"] else "gui"
        calls.append((sha, role))
        if sha == new and role == failed_role:
            raise OSError(5, "injected trial launch failure")
        return 42 if role == "gui" else 43
    monkeypatch.setattr(updates, "launch_version", launch)
    def system_recovery(_):
        if records["transition.json"]["phase"] == "APPLIED_PENDING_READY":
            failure = records["trial-failure.json"]
            assert failure["nonce"] == original["nonce"]
            assert failure["transition_id"] == original["transition_id"]
            assert failure["failed_role"] == failed_role
            rollback = updates.rollback_transition(
                updates.request_rollback(original, reason="injected child start failed"),
                writers_absent=True, previous_verified=True, durable_rollback=True,
                nonce="8" * 32, now="2026-10-02T00:00:01+00:00",
            )
            records["transition.json"] = rollback
            records["active.json"] = {"sha": old}
        else:
            records["transition.json"] = {**records["transition.json"], "phase": "ROLLED_BACK"}
    monkeypatch.setattr(time, "sleep", system_recovery)
    assert updates.bootstrap(tmp_path / "app", [], role="gui") == 0
    assert calls[-2:] == [(old, "gui"), (old, "relay")]
    assert calls.count((new, failed_role)) == 1


def test_failed_trial_keeps_unknown_and_never_launches_previous_code(monkeypatch, tmp_path):
    old, new, _, records = bootstrap_fixture(monkeypatch, tmp_path)
    calls = []
    def fail(control, sha, args, **kwargs):
        calls.append(sha)
        raise OSError(5, "injected child start failure")
    monkeypatch.setattr(updates, "launch_version", fail)
    monkeypatch.setattr(time, "sleep", lambda _: records["transition.json"].update(phase="UNKNOWN"))
    assert updates.bootstrap(tmp_path / "app", [], role="gui") == 20
    assert calls == [new]
    assert records["active.json"]["sha"] == new
    assert records["trial-failure.json"]["launch_failure_details"][0]["win32_error"] == 5


@pytest.mark.parametrize("change", ["missing", "reused_pid", "wrong_sid", "wrong_sha", "wrong_transition", "missing_relay"])
def test_existing_trial_claim_requires_live_bound_processes(monkeypatch, tmp_path, change):
    _, new, state, records = bootstrap_fixture(monkeypatch, tmp_path)
    records["trial.json"] = {"pid": 42, "nonce": state["nonce"],
                             "transition_id": state["transition_id"], "claimed_at": state["applied_at"]}
    gui = dict(pid=42, observable=True, sha=new, sid="operator", role="gui",
               ordinary_user=True, created_at=state["applied_at"])
    relay = {**gui, "pid": 43, "role": "relay"}
    if change == "reused_pid": gui["created_at"] = "2026-10-02T00:00:01+00:00"
    if change == "wrong_sid": gui["sid"] = "another-user"
    if change == "wrong_sha": gui["sha"] = "9" * 40
    if change == "wrong_transition": records["trial.json"]["transition_id"] = "9" * 32
    rows = [] if change == "missing" else [gui] if change == "missing_relay" else [gui, relay]
    monkeypatch.setattr(updates, "process_snapshot", lambda _: rows)
    monkeypatch.setattr(updates, "launch_version", lambda *a, **k: pytest.fail("stale claim launched code"))
    monkeypatch.setattr(time, "sleep", lambda _: records["transition.json"].update(phase="UNKNOWN"))
    assert updates.bootstrap(tmp_path / "app", [], role="gui") == 20
    assert records["trial-failure.json"]["sha"] == new

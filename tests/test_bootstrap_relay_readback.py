"""Actual bootstrap/relay-start contracts with only external boundaries injected."""
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
import json
import os

import pytest
import auto_update as updates
import user_relay
import current_user_onboarding
import subprocess


@pytest.fixture
def relay_bootstrap(monkeypatch, tmp_path):
    current = tmp_path / "current"
    (current / "app").mkdir(parents=True)
    control = tmp_path / "update-control"
    control.mkdir()
    (control / "policy.json").write_text("{}")
    state_root = tmp_path / "user-state"
    state_root.mkdir()
    sha = "1" * 40
    version = tmp_path / "versions" / sha
    status = {}
    records = {"active.json": {"sha": sha}, "transition.json": {
        "schema": updates.SCHEMA, "phase": "DEFERRED", "deferred_at": user_relay._now(),
    }}
    policy = {"operator_sid": "operator", "staging_root": str(tmp_path / "stage")}
    monkeypatch.setattr(updates, "load_policy", lambda _: policy)
    monkeypatch.setattr(updates, "process_security", lambda _: {
        "sid": policy["operator_sid"], "elevated": False,
        "integrity_rid": 8192, "session_id": 2,
    })
    monkeypatch.setattr(updates, "read_json", lambda path: records[path.name])
    monkeypatch.setattr(updates, "optional_record", lambda path: records.get(path.name))
    monkeypatch.setattr(updates, "ordinary_user_context", lambda *_: True)
    monkeypatch.setattr(updates, "update_lock", lambda *a, **k: nullcontext())
    monkeypatch.setattr(updates, "prepared_pending", lambda *a: False)
    monkeypatch.setattr(current_user_onboarding, "resolve_current_user_onboarding_paths",
                        lambda _: SimpleNamespace(direct_sync_root=state_root))
    monkeypatch.setattr(user_relay, "_read_json", lambda _: dict(status))
    monkeypatch.setattr(user_relay, "_acquire_relay_lease",
                        lambda _: None if status else SimpleNamespace(close=lambda: None))
    fresh = user_relay._fresh_persistent_owner
    monkeypatch.setattr(user_relay, "_fresh_persistent_owner",
                        lambda payload, **kwargs: fresh(payload, pid_exists=lambda _: True, **kwargs))
    launched = []
    rows = []
    def launch(control, selected, args, **kwargs):
        launched.append((selected, args))
        status.update(status="RUNNING", worker_id=user_relay.LABEL_MATCH_WORKER_ID,
                      process_id=77, app_root=str(current), code_root=str(version),
                      direct_sync_root=str(state_root), updated_at=user_relay._now())
        rows.append(dict(pid=77, observable=True, sha=sha, sid="operator", role="relay",
                         ordinary_user=True, created_at=user_relay._now()))
        status["updated_at"] = user_relay._now()
        return 77
    monkeypatch.setattr(updates, "launch_version", launch)
    monkeypatch.setattr(updates, "process_snapshot", lambda _: rows)
    def start(command, **kwargs):
        assert command[3] == str(current / "app/auto_update.py")
        # This is the real bootstrap; it starts a version relay and returns.
        result = updates.bootstrap(current / "app", ["--label-match-user-relay"], role="relay")
        return SimpleNamespace(pid=42, poll=lambda: result)
    monkeypatch.setattr(updates, "start_ordinary_process", start)
    return SimpleNamespace(current=current, version=version, status=status, rows=rows, launched=launched, records=records)


def test_bootstrap_exit_zero_is_bound_to_actual_active_version_relay(relay_bootstrap):
    fixture = relay_bootstrap
    result = user_relay.start_user_relay_process(fixture.current, wait=lambda _: None)
    assert result["status"] == "ALIVE"
    assert result["process_id"] == 77
    assert result["bootstrap_process_id"] == 42
    assert fixture.launched == [("1" * 40, ["--label-match-user-relay"])]


@pytest.mark.parametrize("invalid", ["wrong_sha", "wrong_sid", "wrong_role", "elevated", "wrong_code_root", "future_creation", "ambiguous"])
def test_temporary_bootstrap_never_proves_foreign_relay(monkeypatch, relay_bootstrap, invalid):
    fixture = relay_bootstrap
    def mutate(_):
        row = fixture.rows[0]
        if invalid == "wrong_sha": row["sha"] = "2" * 40
        if invalid == "wrong_sid": row["sid"] = "other-user"
        if invalid == "wrong_role": row["role"] = "gui"
        if invalid == "elevated": row["ordinary_user"] = False
        if invalid == "wrong_code_root": fixture.status["code_root"] = str(fixture.version.parent / ("2" * 40))
        if invalid == "future_creation": row["created_at"] = "2099-01-01T00:00:00+00:00"
        if invalid == "ambiguous": fixture.rows.append(dict(row))
    with pytest.raises(user_relay.UserRelayError):
        user_relay.start_user_relay_process(fixture.current, wait=mutate, bootstrap_readiness_seconds=0)


def test_installation_context_does_not_relax_ordinary_bootstrap_guard(monkeypatch, relay_bootstrap):
    monkeypatch.setattr(updates, "ordinary_user_context", lambda *_: False)
    with pytest.raises(ValueError, match="ordinary operator"):
        updates.bootstrap(relay_bootstrap.current / "app", ["--label-match-user-relay"], role="relay")
    assert relay_bootstrap.launched == []


def test_completed_bootstrap_waits_for_delayed_actual_relay_status(monkeypatch, relay_bootstrap):
    fixture = relay_bootstrap
    statuses = user_relay._read_json
    calls = []
    monkeypatch.setattr(user_relay, "_read_json", lambda path: {} if len(calls) < 3 else statuses(path))
    ticks = iter(range(100))
    monkeypatch.setattr(user_relay.time, "monotonic", lambda: next(ticks))
    result = user_relay.start_user_relay_process(fixture.current, wait=lambda seconds: calls.append(seconds))
    assert result["process_id"] == 77
    assert result["bootstrap_process_id"] == 42
    assert calls == [2.0, 0.1, 0.1]


def test_canonical_commands_keep_fixed_run_and_real_version_relay_separate(tmp_path):
    repo = Path(__file__).resolve().parents[1]
    current = tmp_path / "current"
    control = tmp_path / "update-control"
    control.mkdir()
    (control / "policy.json").write_text("{}")
    version = tmp_path / "versions" / ("1" * 40)
    (version / "app").mkdir(parents=True)
    (version / "app/main.py").write_text("# never executed")
    (control / "active.json").write_text(json.dumps({"schema": updates.SCHEMA, "sha": "1" * 40}))
    quote = lambda p: "'" + str(p).replace("'", "''") + "'"
    code = f"""
$ErrorActionPreference='Stop'
$tokens=$null; $errors=$null
$tree=[Management.Automation.Language.Parser]::ParseFile({quote(repo/'INSTALL_CANONICAL_PORTABLE.ps1')},[ref]$tokens,[ref]$errors)
if($errors.Count){{throw 'Actual canonical parse failed'}}
$bundle=@($tree.EndBlock.Statements | Where-Object {{ $_ -is [Management.Automation.Language.FunctionDefinitionAst] }})
. ([ScriptBlock]::Create(($bundle.Extent.Text -join [Environment]::NewLine)))
$run=Command {quote(current)}
$relayRoot=RelayCodeRoot {quote(current)}
$relay=Command $relayRoot
if(-not $run.Contains('--bootstrap --role relay') -or -not $run.Contains({quote(current/'app/auto_update.py')})){{throw 'Run is not fixed bootstrap'}}
if($relayRoot -cne {quote(version)} -or $relay.Contains('--bootstrap') -or -not $relay.Contains({quote(version/'app/main.py')})){{throw 'Active runtime was mistaken for Run bootstrap'}}
'FIXED_RUN_AND_VERSION_RELAY'
"""
    script = tmp_path / "commands.ps1"
    script.write_text(code, encoding="utf-8-sig")
    result = subprocess.run(["powershell.exe", "-NoProfile", "-NonInteractive", "-File", str(script)],
                            cwd=tmp_path, capture_output=True, text=True,
                            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "FIXED_RUN_AND_VERSION_RELAY"


def test_relay_recovery_never_transforms_relay_mode_into_gui(relay_bootstrap, monkeypatch):
    fixture = relay_bootstrap
    sha = fixture.version.name
    fixture.records["transition.json"] = {"phase": "DEFERRED", "retained_candidate": True}
    monkeypatch.setattr(updates, "prepared_pending", lambda *_: False)
    monkeypatch.setattr(updates, "startup_action", lambda *_args, **_kw: "WAIT_FOR_RECOVERY")
    monkeypatch.setattr(updates, "recovery_sha", lambda *_args, **_kw: sha)
    received = []
    monkeypatch.setattr(updates, "start_current", lambda _control, _policy, _state, _active, args, **_kw:
                        received.append(args) or 0)
    assert updates.bootstrap(fixture.current / "app", [user_relay.USER_RELAY_MODE], role="relay") == 0
    assert received == [[user_relay.USER_RELAY_MODE]]


def test_relay_bootstrap_without_relay_mode_refuses_before_gui(relay_bootstrap):
    with pytest.raises(ValueError, match="explicit relay mode"):
        updates.bootstrap(relay_bootstrap.current / "app", [], role="relay")
    assert relay_bootstrap.launched == []

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
    version = updates.version_bundle(control, sha)
    status = {}
    records = {"active.json": {"sha": sha}, "transition.json": {
        "schema": updates.SCHEMA, "phase": "DEFERRED", "deferred_at": user_relay._now(),
    }}
    policy = {"operator_sid": "operator", "staging_root": str(tmp_path / "stage"),
              "operator_local_app_data": str(tmp_path / "operator-local")}
    monkeypatch.setattr(updates, "load_policy", lambda _: policy)
    monkeypatch.setattr(updates, "process_security", lambda _: {
        "sid": policy["operator_sid"], "elevated": False,
        "integrity_rid": 8192, "session_id": 2,
    })
    monkeypatch.setattr(updates, "read_json", lambda path: records[path.name])
    monkeypatch.setattr(updates, "optional_record", lambda path: dict(status) if path.name == user_relay.user_relay_status_path(state_root).name else records.get(path.name))
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
        rows.append(dict(pid=77, observable=True, sha=sha, sid=policy["operator_sid"], role="relay",
                         ordinary_user=True, created_at=updates.now_utc()))
        status.update(source_commit=selected, ordinary_user=True, sid=policy["operator_sid"])
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


def test_completed_bootstrap_without_running_status_fails_after_grace(monkeypatch, relay_bootstrap):
    fixture = relay_bootstrap
    calls = []
    monkeypatch.setattr(user_relay, "_read_json", lambda path: {})
    clock = [0.0]
    monkeypatch.setattr(user_relay.time, "monotonic", lambda: clock[0])
    def wait(seconds):
        calls.append(seconds)
        clock[0] += seconds
    with pytest.raises(user_relay.UserRelayError) as caught:
        user_relay.start_user_relay_process(fixture.current, wait=wait)
    assert caught.value.cause_code == "RELAY_EXITED_BEFORE_RUNNING"
    assert caught.value.exit_code == 0
    assert sum(calls) == pytest.approx(4.0)


def test_completed_bootstrap_waits_for_delayed_actual_relay_status(monkeypatch, relay_bootstrap):
    import startup_timing
    statuses = user_relay._read_json
    calls = []
    clock = [0.0]
    monkeypatch.setattr(user_relay, "_read_json", lambda path: {} if len(calls) < 3 else statuses(path))
    monkeypatch.setattr(user_relay.time, "monotonic", lambda: clock[0])
    original_wait = startup_timing.startup_wait
    def traced_wait(seconds, callback):
        calls.append(seconds)
        original_wait(seconds, callback)
    monkeypatch.setattr(startup_timing, "startup_wait", traced_wait)
    def wait(seconds):
        clock[0] += seconds
    result = user_relay.start_user_relay_process(relay_bootstrap.current, wait=wait)
    assert result["process_id"] == 77 and result["bootstrap_process_id"] == 42
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


def _clocked_bootstrap(monkeypatch, fixture, *, proof_at, exit_at=None, exit_code=1):
    """Fake clock + fake wait: relay proof appears at proof_at; child exits at exit_at."""
    clock = [0.0]
    monkeypatch.setattr(user_relay.time, "monotonic", lambda: clock[0])
    def wait(seconds):
        clock[0] += seconds
    def poll():
        if exit_at is not None and clock[0] >= exit_at:
            return exit_code
        return None
    monkeypatch.setattr(updates, "start_ordinary_process",
                        lambda command, **kw: SimpleNamespace(pid=42, poll=poll))
    owner = {"process_id": 77, "updated_at": "t"}
    monkeypatch.setattr(user_relay, "_existing_user_relay_owner",
                        lambda *a, **k: owner if (proof_at is not None and clock[0] >= proof_at) else None)
    return clock, wait


def test_bootstrap_relay_proof_at_90_seconds_is_accepted(monkeypatch, relay_bootstrap):
    clock, wait = _clocked_bootstrap(monkeypatch, relay_bootstrap, proof_at=90)
    result = user_relay.start_user_relay_process(relay_bootstrap.current, wait=wait)
    assert result["status"] == "ALIVE" and result["process_id"] == 77


def test_bootstrap_child_nonzero_exit_fails_immediately(monkeypatch, relay_bootstrap):
    clock, wait = _clocked_bootstrap(monkeypatch, relay_bootstrap, proof_at=None, exit_at=30)
    with pytest.raises(user_relay.UserRelayError):
        user_relay.start_user_relay_process(relay_bootstrap.current, wait=wait)
    assert clock[0] < 40


@pytest.mark.parametrize("exit_code", [0, 7])
def test_official_relay_start_detects_child_exit_before_running(monkeypatch, relay_bootstrap, exit_code):
    clock, wait = _clocked_bootstrap(
        monkeypatch, relay_bootstrap, proof_at=None, exit_at=0, exit_code=exit_code,
    )
    with pytest.raises(user_relay.UserRelayError) as caught:
        user_relay.start_user_relay_process(relay_bootstrap.current, wait=wait)
    assert caught.value.cause_code == "RELAY_EXITED_BEFORE_RUNNING"
    assert caught.value.exit_code == exit_code
    assert clock[0] == pytest.approx(4.0 if exit_code == 0 else 2.0)


def test_bootstrap_without_proof_fails_after_600_seconds(monkeypatch, relay_bootstrap):
    clock, wait = _clocked_bootstrap(monkeypatch, relay_bootstrap, proof_at=None)
    with pytest.raises(user_relay.UserRelayError, match="did not prove the protected active-version relay"):
        user_relay.start_user_relay_process(relay_bootstrap.current, wait=wait)
    assert 600 <= clock[0] < 605


def test_bootstrap_proof_after_600_seconds_plus_survival_is_rejected(monkeypatch, relay_bootstrap):
    clock, wait = _clocked_bootstrap(monkeypatch, relay_bootstrap, proof_at=605)
    with pytest.raises(user_relay.UserRelayError):
        user_relay.start_user_relay_process(relay_bootstrap.current, wait=wait)


def test_installer_relay_readiness_ceiling_is_600_seconds():
    text = (Path(__file__).resolve().parents[1] / "INSTALL_CANONICAL_PORTABLE.ps1").read_text(encoding="utf-8-sig")
    assert "$relayStartedAt.AddSeconds(600)" in text
    assert "$remainingReadySeconds=600-(" in text
    assert "[double]$TimeoutSeconds = 600) {" in text
    assert "$TimeoutSeconds -gt 600) { throw 'Rollback readiness deadline differs.' }" in text
    assert "AddSeconds(75)" not in text and "-gt 75)" not in text


def test_installer_relay_proof_loop_compares_utc_with_its_utc_deadline():
    # $relayStartedAt is UtcNow, so the loop must read UtcNow too: PowerShell
    # compares DateTime ticks without Kind, and local time east of UTC would
    # already be past a UTC deadline before the first proof read.
    text = (Path(__file__).resolve().parents[1] / "INSTALL_CANONICAL_PORTABLE.ps1").read_text(encoding="utf-8-sig")
    assert "$relayStartedAt=[DateTime]::UtcNow" in text
    assert "$deadline = $relayStartedAt.AddSeconds(600)" in text
    assert "while ([DateTime]::UtcNow -lt $deadline) {" in text
    assert "while ((Get-Date) -lt $deadline) {" not in text

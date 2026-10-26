"""Keep a verified first-setup parent out of its relay child's writer cohort."""
import contextlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

import auto_update as updates
import writer_session_fence as fence


PARENT_ENV = "KMTECH_LM_RELAY_PARENT"
SHA = "a" * 40
SID = "S-1-5-21-543"
BOUNDARY = "2026-10-23T00:01:00Z"
BIRTH = "2026-10-23T00:00:00Z"


def _observe_script(root, processes):
    """Real Observe supervisor/query; replace only CIM and process-token I/O."""
    source = (Path(updates.__file__).parent / "tools/apply_prepared_update.ps1").read_text(encoding="utf-8-sig")
    supervisor = source[source.index("function Invoke-LabelStartupReadOnlyQuery {"):source.index("if ($SupportedLifecycle -and")]
    query = source[source.index("function Get-StartupUpdateProcesses("):source.index("function Assert-StartupUpdateVersion(")]
    literal = str(processes).replace("'", "''")
    adapters = r'''
    Add-Type 'public static class KMTechStartupUpdateToken { public static bool Ordinary(uint pid) { return true; } }'
    $script:testProcesses = Get-Content -LiteralPath 'PROCESSES' -Raw | ConvertFrom-Json
    function Get-CimInstance {
        foreach ($item in $script:testProcesses) {
            $item.CreationDate = [datetime]$item.CreationDate
            $item
        }
    }
    function Invoke-CimMethod { return @{ReturnValue=0;Sid='S-1-5-21-543'} }
'''.replace("PROCESSES", literal)
    assert query.count("    # Query only app-related rows;") == 1
    query = query.replace("    # Query only app-related rows;", adapters + "\n    # Query only app-related rows;", 1)
    # Token I/O is already replaced by the adapter in the copied query body.
    return (supervisor + "\nfunction Initialize-LabelStartupUpdateProcessToken {}\nfunction Assert-LabelStartupNoReparsePoint {}\n" + query
            + "\nInvoke-LabelStartupReadOnlyQuery -QueryMode Observe -Root '"
            + str(root).replace("'", "''") + "' -OperatorSid 'S-1-5-21-543'\n")


def _query(script):
    result = subprocess.run([updates.system_executable("WindowsPowerShell/v1.0/powershell.exe"),
        "-NoLogo", "-NoProfile", "-NonInteractive", "-WindowStyle", "Hidden", "-File", str(script)],
        capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW)
    if result.returncode:
        script.with_suffix(".private.log").write_text(result.stderr, encoding="utf-8")
    assert result.returncode == 0, f"isolated Observe query failed: rc {result.returncode}"
    return result.stdout.strip()


@pytest.fixture
def cohort(tmp_path, monkeypatch):
    control = tmp_path / "managed/update-control"
    bundle = control.parent / "current"
    app = bundle / "app"
    app.mkdir(parents=True)
    control.mkdir()
    (app / "label_match_relay_entry.py").touch()
    (bundle / "runtime").mkdir()
    repo = Path(updates.__file__).parent
    for name in (*updates.CONTRACT_FILES, *("app/" + name for name in updates.DATA_API_FILES), "INSTALL_THIS_PC.ps1"):
        target = bundle / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((repo / name.removeprefix("app/")).read_bytes())
    (app / "Label_Match.py").write_text('APP_VERSION = "v2.0.123"\n')
    manifest = {"schema": "label-match-portable-tree-v1", "startup_update_protocol": 1,
        "source_commit": SHA, "version": "v2.0.123",
        "writer_sink_inventory_contract_sha256": updates.version_writer_inventory_sha256(bundle)}
    (bundle / "portable-manifest.json").write_text(json.dumps(manifest))
    (control / "active.json").write_text(json.dumps({"sha": SHA}))
    (control / "transition.json").write_text(json.dumps({"schema": updates.SCHEMA, "phase": "DEFERRED"}))
    stage = tmp_path / "stage"
    stage.mkdir()
    policy = {"operator_sid": SID, "staging_root": str(stage)}
    parent_pid = os.getpid() + 10000
    rows = [{"ProcessId": parent_pid, "Name": "pythonw.exe",
        "ExecutablePath": str(bundle / "runtime/pythonw.exe"),
        "CommandLine": str(app / "label_match_relay_entry.py") + " --onboard-current-user",
        "CreationDate": BIRTH}]
    processes = tmp_path / "processes.json"
    processes.write_text(json.dumps(rows))
    script = tmp_path / "observe.ps1"
    script.write_text(_observe_script(bundle, processes), encoding="utf-8-sig")
    monkeypatch.setattr(updates, "control_root", lambda *_: control)
    monkeypatch.setattr(updates, "load_policy", lambda *_: policy)
    monkeypatch.setattr(updates, "version_bundle", lambda *_: bundle)
    monkeypatch.setattr(updates, "ordinary_user_context", lambda *_: True)
    monkeypatch.setattr(updates, "_run_fixed_helper", lambda *_a, **_k: _query(script))
    monkeypatch.setattr(updates, "current_generation_status", lambda *_: "ALLOW")
    monkeypatch.setattr(updates, "assert_version_writer", lambda *_a, **_k: None)
    monkeypatch.setattr(updates, "native_process_identity", lambda *_: {"status": "LIVE", "creation_identity": "12345"})
    monkeypatch.setattr(fence, "writer_admission", lambda *_a, **_k: contextlib.nullcontext())
    @contextlib.contextmanager
    def busy(*_a, **_k):
        raise RuntimeError("update mutex ownership is busy")
        yield
    monkeypatch.setattr(updates, "update_lock", busy)
    monkeypatch.setenv("KMTECH_LM_UPDATE_DEFERRED_AT", BOUNDARY)
    monkeypatch.delenv("KMTECH_LM_UPDATE_TRIAL_NONCE", raising=False)
    monkeypatch.setenv(PARENT_ENV, f"{parent_pid}:12345")
    return SimpleNamespace(control=control, app=app, policy=policy, rows=rows,
        processes=processes, script=script, pid=parent_pid, bundle=bundle)


def test_observe_classifies_headless_onboarding_as_writer_row(cohort):
    rows = updates.process_snapshot(cohort.control)
    assert len(rows) == 1
    assert rows[0]["role"] == "writer" and rows[0]["observable"] is True
    assert rows[0]["ordinary_user"] is True and rows[0]["pid"] == cohort.pid
    assert updates._utc(rows[0]["created_at"]) < updates._utc(BOUNDARY)


def test_busy_gate_relay_child_admits_verified_onboarding_parent(cohort):
    assert updates.startup_gate(cohort.app, ["--label-match-user-relay"]) is None


@pytest.mark.parametrize("binding", [None, "", "garbage", "0:12345", "-1:12345", "1:bad", "1:2:3", " 1:2", "different-pid", "reused-pid"])
def test_busy_gate_still_refuses_unbound_earlier_writer(cohort, monkeypatch, binding):
    if binding is None:
        monkeypatch.delenv(PARENT_ENV)
    else:
        binding = {"different-pid": f"{cohort.pid + 1}:12345", "reused-pid": f"{cohort.pid}:54321"}.get(binding, binding)
        monkeypatch.setenv(PARENT_ENV, binding)
    with pytest.raises(ValueError, match="earlier or unrelated writer"):
        updates.startup_gate(cohort.app, ["--label-match-user-relay"])


@pytest.mark.parametrize("difference", ["other-writer", "duplicate", "sha", "sid", "ordinary", "identity-unknown", "identity-error"])
def test_parent_binding_preserves_other_writer_refusals(cohort, monkeypatch, difference):
    if difference in {"other-writer", "duplicate"}:
        other = dict(cohort.rows[0])
        if difference == "other-writer":
            other["ProcessId"] += 1
        cohort.processes.write_text(json.dumps([*cohort.rows, other]))
    elif difference == "sha":
        (cohort.bundle / "portable-manifest.json").write_text(json.dumps({"source_commit": "b" * 40}))
        monkeypatch.setattr(updates, "version_manifest", lambda *_: {"source_commit": SHA})
    elif difference == "sid":
        cohort.policy["operator_sid"] = "another-operator"
    elif difference == "ordinary":
        cohort.script.write_text(cohort.script.read_text(encoding="utf-8-sig").replace("return true;", "return false;"), encoding="utf-8-sig")
    elif difference == "identity-unknown":
        monkeypatch.setattr(updates, "native_process_identity", lambda *_: {"status": "UNKNOWN"})
    else:
        def unavailable(*_):
            raise OSError("isolated identity unavailable")
        monkeypatch.setattr(updates, "native_process_identity", unavailable)
    with pytest.raises(ValueError, match="earlier or unrelated writer"):
        updates.startup_gate(cohort.app, ["--label-match-user-relay"])


@pytest.mark.parametrize("kind", ["verified", "independent", "plain", "trial"])
def test_launch_version_forwards_only_verified_relay_parent(cohort, monkeypatch, kind):
    captured = []
    monkeypatch.setattr(updates, "start_ordinary_process", lambda _cmd, **kw: captured.append(kw) or SimpleNamespace(pid=99))
    deadline = time.monotonic() + 60
    if kind in {"verified", "trial"}:
        deadline = updates._RelayPreparationDeadline(deadline, parent_pid=cohort.pid, parent_identity="12345")
        if kind == "trial":
            deadline = deadline.bounded_by(time.monotonic() + 30)
    elif kind == "independent":
        deadline = updates._RelayPreparationDeadline(deadline)
    updates.launch_version(cohort.control, SHA, ["--label-match-user-relay"],
        relay_deadline=deadline, nonce="b" * 32 if kind == "trial" else "")
    environment = captured[0]["environment"]
    if kind == "verified":
        assert environment[PARENT_ENV] == f"{cohort.pid}:12345"
    else:
        assert PARENT_ENV not in environment


@pytest.mark.parametrize("descendant", [False, True])
def test_relay_consumes_parent_binding_after_first_gate(cohort, monkeypatch, descendant):
    import label_data_generation
    import user_relay

    class ReachedDataBoundary(Exception):
        pass

    def after_gate(*_a, **_kw):
        if descendant:
            # The finite direct-sync relay uses this real subprocess boundary.
            result = user_relay._run_command([sys.executable, "-B", "-c",
                "import os; raise SystemExit('KMTECH_LM_RELAY_PARENT' in os.environ)"], 30)
            assert result["returncode"] == 0
        else:
            assert PARENT_ENV not in os.environ
        raise ReachedDataBoundary

    monkeypatch.setattr(label_data_generation, "normalize_data_arguments", lambda *_: None)
    monkeypatch.setattr(user_relay, "check_user_data_open", after_gate)
    args = SimpleNamespace(request_app_stop=False, direct_sync_root="", scan_source_dir="")
    with pytest.raises(ReachedDataBoundary):
        user_relay._run_user_relay(args, cohort.bundle, [])


def _use_gui_parent(cohort):
    (cohort.app / "main.py").touch()
    cohort.rows[0]["CommandLine"] = str(cohort.app / "main.py")
    cohort.processes.write_text(json.dumps(cohort.rows))
    assert updates.process_snapshot(cohort.control)[0]["role"] == "gui"


def test_later_recovery_proof_refuses_bound_gui_parent(cohort):
    _use_gui_parent(cohort)
    with pytest.raises(ValueError, match="earlier or unrelated writer"):
        updates.assert_ordinary_recovery(cohort.control, cohort.policy, SHA, BOUNDARY)


@pytest.mark.parametrize("phase", ["PREPARED", "FAILED", "DEFERRED"])
def test_later_startup_gate_refuses_bound_gui_parent(cohort, monkeypatch, phase):
    _use_gui_parent(cohort)
    (cohort.control / "transition.json").write_text(json.dumps(
        {"phase": phase, "recovery_pending": True}))
    monkeypatch.setattr(updates, "update_lock", lambda *_a, **_kw: contextlib.nullcontext())
    with pytest.raises(ValueError, match="earlier or unrelated writer"):
        updates.startup_gate(cohort.app, ["--label-match-user-relay"])


def _integration_process(config_path, mode, parent_pid=None, parent_deadline=None):
    """Separate bootstrap/relay processes with the real contended update mutex."""
    import current_user_onboarding as onboarding
    import user_relay
    config_path = Path(config_path)
    config = json.loads(config_path.read_text())
    control, app = Path(config["control"]), Path(config["app"])
    policy = config["policy"]
    native_lock = updates.update_lock
    native_sid = updates.current_process_sid()
    updates.control_root = lambda *_: control
    updates.load_policy = lambda *_: policy
    updates.version_bundle = lambda *_: app.parent
    updates.ordinary_user_context = lambda *_: True
    updates.runtime_matches = lambda *_: True
    updates.read_trial_generation = lambda *_: {"status": "READ", "marker_path": str(config_path.parent / "absent-generation.json")}
    updates.update_lock = lambda root, _sid, **kw: native_lock(root, native_sid, **kw)
    updates._run_fixed_helper = lambda *_a, **_k: _query(Path(config["query"]))
    fence.writer_admission = lambda *_a, **_k: contextlib.nullcontext()
    onboarding._machine_worker_context = lambda *_: None
    onboarding.resolve_current_user_onboarding_paths = lambda *_: SimpleNamespace(direct_sync_root=Path(config["direct"]))
    if mode == "relay":
        rows = json.loads(Path(config["processes"]).read_text())
        rows.append({**rows[0], "ProcessId": os.getpid(),
            "CommandLine": str(app / "label_match_relay_entry.py") + " --label-match-user-relay",
            "CreationDate": updates.now_utc()})
        Path(config["processes"]).write_text(json.dumps(rows))
        assert updates.startup_gate(app, ["--label-match-user-relay"]) is None
        status = {"status": "RUNNING", "source_commit": SHA, "ordinary_user": True,
            "process_id": os.getpid(), "updated_at": updates.now_utc(), "code_root": str(app)}
        target = user_relay.user_relay_status_path(Path(config["direct"]))
        target.parent.mkdir(parents=True, exist_ok=True)
        updates.write_json_durable(target, status)
        # Keep the real relay PID alive until bootstrap readiness is read back.
        while not (config_path.parent / "release-relay").exists():
            time.sleep(0.1)
        return 0
    children = []
    def launch(command, *, cwd, environment):
        assert command[-1] == "--label-match-user-relay"
        process = _integration_launch(config_path, "relay", environment=environment)
        children.append(process)
        return process
    updates.start_ordinary_process = launch
    try:
        return updates._bootstrap(app, ["--label-match-user-relay"], role="relay",
            parent_pid=int(parent_pid), parent_deadline=float(parent_deadline))
    finally:
        (config_path.parent / "release-relay").touch()
        for child in children:
            child.wait()


def _integration_launch(config_path, mode, *, environment=None, parent_pid=None, parent_deadline=None):
    native = getattr(subprocess.Popen, "native", subprocess.Popen)
    code = ("import sys;sys.path.insert(0,sys.argv[1]);"
        "from tests.test_first_setup_relay_parent_cohort import _integration_process;"
        "raise SystemExit(_integration_process(*sys.argv[2:]))")
    command = [sys.executable, "-B", "-c", code, str(Path(updates.__file__).parent), str(config_path), mode]
    if parent_pid is not None:
        command += [str(parent_pid), str(parent_deadline)]
    log = Path(config_path).parent / (mode + ".private.log")
    with log.open("wb") as stream:
        return native(command, env=environment, stdout=stream, stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NO_WINDOW)


def test_onboarding_real_bootstrap_child_hits_busy_gate_with_parent_row(cohort, monkeypatch, tmp_path):
    import current_user_onboarding as onboarding
    import user_relay
    # The test process is the live onboarding parent. Its actual native birth
    # identity is captured by _relay_preparation_deadline in another process.
    cohort.rows[0]["ProcessId"] = os.getpid()
    cohort.processes.write_text(json.dumps(cohort.rows))
    direct = tmp_path / "direct"
    direct.mkdir()
    local = tmp_path / "local"
    local.mkdir()
    cohort.policy["operator_local_app_data"] = str(local)
    config = tmp_path / "integration.json"
    config.write_text(json.dumps({"control": str(cohort.control), "app": str(cohort.app),
        "policy": cohort.policy, "query": str(cohort.script),
        "processes": str(cohort.processes), "direct": str(direct)}))
    monkeypatch.setattr(onboarding, "_machine_worker_context", lambda *_: None)
    monkeypatch.setattr(onboarding, "resolve_current_user_onboarding_paths", lambda *_: SimpleNamespace(direct_sync_root=direct))
    children = []
    def start(command, **kwargs):
        assert "--bootstrap" in command
        process = _integration_launch(config, "bootstrap",
            parent_pid=command[command.index("--relay-parent-pid") + 1],
            parent_deadline=command[command.index("--relay-parent-deadline") + 1])
        children.append(process)
        return process
    monkeypatch.setattr(updates, "start_ordinary_process", start)
    def existing(*_a, **_kw):
        path = user_relay.user_relay_status_path(direct)
        return json.loads(path.read_text()) if path.exists() else None
    monkeypatch.setattr(user_relay, "_existing_user_relay_owner", existing)
    monkeypatch.delenv(PARENT_ENV, raising=False)
    try:
        result = user_relay.start_user_relay_process(cohort.bundle, survival_seconds=0,
            bootstrap_readiness_seconds=60)
        assert result["status"] == "ALIVE"
        assert existing()["status"] == "RUNNING"
        assert len(children) == 1 and children[0].wait() == 0
        assert updates.read_json(cohort.control / "transition.json")["phase"] == "DEFERRED"
    finally:
        (tmp_path / "release-relay").touch()
        for child in children:
            child.wait()

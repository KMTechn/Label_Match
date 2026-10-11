"""Exercise installed inputs without GUI, credentials stores or startup writers."""
import builtins
import hashlib
import io
import json
from pathlib import Path
import socket
import sqlite3
import subprocess
from types import SimpleNamespace
import uuid

import pytest

import auto_update as update
import current_user_onboarding as onboarding
import item_catalog_sync as catalog
import label_data_generation as generation
import logistics_runtime_profile as logistics
import protected_admin
import startup_check as check
import user_relay
import writer_session_fence as fence
from tests.dpapi_double import DPAPIDouble
from tests.test_current_user_onboarding import _ready_state
from tests.test_item_catalog_sync import CATALOG, _profile
from tests.test_writer_session_fence import _payload
from direct_sync_push import DEFAULT_ENDPOINT_PATH


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def snapshot(root):
    return {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob("*") if p.is_file()}


@pytest.fixture
def installed(tmp_path, monkeypatch, isolated_dpapi):
    root = tmp_path / "managed/current"
    (root / "app/assets").mkdir(parents=True)
    (root / "runtime").mkdir()
    (root / "runtime/pythonw.exe").write_bytes(b"runtime")
    source = Path(__file__).resolve().parents[1]
    for name in ("main.py", "startup_check.py", "label_data_generation.py", "auto_update.py",
                 "current_user_onboarding.py", "logistics_runtime_profile.py", "item_catalog_sync.py", "writer_session_fence.py"):
        (root / "app" / name).write_bytes((source / ("portable/main.py" if name == "main.py" else name)).read_bytes())
    (root / "app/assets/Item.csv").write_bytes(CATALOG)
    build = hashlib.sha1(uuid.uuid4().bytes).hexdigest()
    write(root / "portable-manifest.json", {"schema": "label-match-portable-tree-v1",
                                           "version": generation.CURRENT_VERSION, "source_commit": build})
    rows = [{"path": p.relative_to(root).as_posix(), "size": p.stat().st_size,
             "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
            for p in sorted(root.rglob("*"), key=lambda p: p.relative_to(root).as_posix().encode("utf-16-be")) if p.is_file()]
    aggregate = hashlib.sha256("".join(f"{r['sha256']} {r['size']} {r['path']}\n" for r in rows).encode()).hexdigest()
    write(root / "bootstrap-integrity.json", {"schema_version": onboarding.PORTABLE_BOOTSTRAP_INTEGRITY_VERSION,
          "status": "PASS", "code_root": ".", "file_count": len(rows), "files": rows,
          "aggregate_sha256": aggregate, "identity_profile_created": False, "state_scope": "current_user_first_run"})
    sid = "S-1-5-21-" + str(uuid.uuid4().int % 100000)
    monkeypatch.setattr(protected_admin, "current_process_sid", lambda: sid)
    monkeypatch.setattr(update, "current_process_sid", lambda: sid)
    monkeypatch.setattr(update, "ordinary_user_context", lambda expected=None: expected is None or expected == sid)
    monkeypatch.setattr(check, "_already_running", lambda _root: False)
    monkeypatch.setattr(logistics, "legacy_label_transition_enabled", lambda: False)
    control = root.parent / "update-control"
    policy = {"operator_sid": sid, "staging_root": str(tmp_path / "stage")}
    from tests.test_auto_update import bind_test_packet_settings
    bind_test_packet_settings(control, policy)
    write(control / "policy.json", policy)
    write(control / "active.json", {"schema": update.SCHEMA, "sha": build})
    write(control / "transition.json", {"schema": update.SCHEMA, "phase": "CONFIRMED"})
    paths = onboarding.resolve_current_user_onboarding_paths(root)
    identity = _ready_state(paths)
    paths.settings_path.parent.mkdir(parents=True, exist_ok=True)
    paths.settings_path.write_text("{}", encoding="utf-8")
    cipher = DPAPIDouble((tmp_path / "dpapi-double.bin").read_bytes())
    secret = uuid.uuid4().hex
    producer_secret = paths.direct_sync_root / "secrets" / "fixture.dpapi"
    producer_secret.parent.mkdir(exist_ok=True)
    producer_secret.write_bytes(cipher.protect(secret.encode()))
    write(paths.credential_path, {"producer_id": identity["producer_id"], "key_id": uuid.uuid4().hex,
          "dpapi_scope": "current_user", "secret_ref": "dpapi:fixture",
          "secret_data_dir": str(paths.direct_sync_root), "endpoint_url": onboarding.DEFAULT_SERVER_BASE_URL + DEFAULT_ENDPOINT_PATH})
    profile = _profile(source_host_id=identity["source_host_id"], device_id=uuid.uuid4().hex, bearer_token=uuid.uuid4().hex)
    profile_values = {name: getattr(profile, name) for name in ("base_url", "authority_scope", "authority_epoch", "authority_plane", "ledger_plane", "plane_epoch", "source_host_id", "device_id")}
    profile_values.update(contract_version=logistics.PROFILE_CONTRACT_VERSION,
                          credential_scope="current_user", bearer_token_ref=logistics.DEFAULT_TOKEN_REF)
    write(paths.logistics_profile_path, profile_values)
    paths.logistics_secret_path.write_bytes(cipher.protect(profile.bearer_token.encode(), logistics.DPAPI_ENTROPY))
    cache = catalog.default_cache_path()
    catalog._write_authenticated_cache(cache, CATALOG, url=profile.base_url + catalog.CATALOG_PATH,
                                      source_host_id=profile.source_host_id, device_id=profile.device_id, bearer_token=profile.bearer_token)
    monkeypatch.setattr(user_relay, "_registry_get", lambda: user_relay.user_relay_command_line(root))
    return SimpleNamespace(root=root, control=control, paths=paths, sid=sid, build=build, cache=cache, tmp=tmp_path)


@pytest.mark.parametrize("blocked", [False, True])
def test_read_only_with_real_installed_inputs(installed, monkeypatch, blocked):
    if blocked:
        write(fence.canonical_control_root() / "active.json", _payload(delegated_sources=[]))
    before = snapshot(installed.tmp)
    attempted = []
    original_open = builtins.open
    def guarded_open(file, mode="r", *args, **kwargs):
        if any(flag in mode for flag in "wax+"):
            attempted.append("write")
            raise AssertionError("unexpected startup write")
        return original_open(file, mode, *args, **kwargs)
    def forbidden(*args, **kwargs):
        attempted.append("side effect")
        raise AssertionError("startup side effect")
    with monkeypatch.context() as m:
        m.setattr(builtins, "open", guarded_open)
        m.setattr(io, "open", guarded_open)
        for module, name in ((subprocess, "Popen"), (socket, "socket"), (sqlite3, "connect"), (fence, "_acquire_named_mutex")):
            m.setattr(module, name, forbidden)
        result = check.check_startup(installed.root)
    assert attempted == []
    assert snapshot(installed.tmp) == before
    assert result["startable"] is (not blocked)
    assert result["category"] == ("DEPLOY_FENCE" if blocked else "READY")
    assert result["build_sha"] == installed.build
    assert not any(p.suffix in (".pyc", ".db") or p.name.endswith(("-wal", "-shm")) for p in installed.tmp.rglob("*"))


@pytest.mark.parametrize("phase", ["PREPARED", "APPLIED_PENDING_READY", "ROLLBACK_PENDING_READY", "FAILED", "UNKNOWN"])
def test_cli_uses_the_actual_bootstrap_predicate(installed, phase):
    state = {"schema": update.SCHEMA, "phase": phase}
    write(installed.control / "transition.json", state)
    reason = update.startup_bootstrap_reason(installed.build, {"sha": installed.build}, state, False)
    assert reason in check.check_startup(installed.root)["reasons"]


@pytest.mark.parametrize("case,category", [("account", "ACCOUNT_MISMATCH"), ("relay", "INSTALL_MISMATCH"),
                                         ("manifest", "INSTALL_MISMATCH"), ("catalog", "CATALOG_OFFLINE"),
                                         ("data", "DATA_GUARD"), ("running", "READY")])
def test_each_startup_cause(installed, monkeypatch, case, category):
    if case == "account":
        monkeypatch.setattr(update, "ordinary_user_context", lambda *_args: False)
    elif case == "relay":
        monkeypatch.setattr(user_relay, "_registry_get", lambda: "")
    elif case == "manifest":
        (installed.root / "app/main.py").write_bytes(b"changed")
    elif case == "catalog":
        installed.cache.unlink()
        catalog._cache_recovery_path(installed.cache).unlink()
    elif case == "data":
        from kmtech_shared.data_generation import record_generation
        installed.paths.data_root.mkdir(parents=True, exist_ok=True)
        record_generation(installed.paths.data_root / "data-generation.json", "v999.0.0", app_id=generation.APP_ID,
                          subject_sid=installed.sid, build_id=installed.build)
    elif case == "running":
        monkeypatch.setattr(check, "_already_running", lambda _root: True)
    result = check.check_startup(installed.root)
    assert result["category"] == category
    assert result["startable"] is (case == "running")
    if case == "running":
        assert result["categories"] == ["ALREADY_RUNNING"]


def test_combined_categories_and_unknown_fail_closed(installed, monkeypatch):
    write(fence.canonical_control_root() / "active.json", _payload(delegated_sources=[]))
    monkeypatch.setattr(user_relay, "_registry_get", lambda: "")
    monkeypatch.setattr(check, "_already_running", lambda _root: (_ for _ in ()).throw(OSError()))
    result = check.check_startup(installed.root)
    assert result["categories"] == ["DEPLOY_FENCE", "INSTALL_MISMATCH", "UNKNOWN"]
    assert result["category"] == "DEPLOY_FENCE"


def test_json_is_one_line_and_exit_codes_do_not_expose_private_values(installed, monkeypatch, capsys):
    monkeypatch.setattr(check, "check_startup", lambda: check.startup_result(["ALREADY_RUNNING"]))
    assert check.main(["--json"]) == 0
    assert json.loads(capsys.readouterr().out)["startable"]
    monkeypatch.setattr(check, "check_startup", lambda: check.startup_result(["ACTIVE_WRITER_FENCE"]))
    assert check.main(["--json"]) == 10
    capsys.readouterr()
    def broken():
        print(uuid.uuid4().hex)
        raise RuntimeError(str(installed.root))
    monkeypatch.setattr(check, "check_startup", broken)
    assert check.main(["--json"]) == 20
    output = capsys.readouterr()
    assert len(output.out.splitlines()) == 1 and output.err == ""
    assert json.loads(output.out)["reasons"] == ["UNKNOWN_CHECK"]


def test_slow_check_is_unknown_without_running_later_probes(installed, monkeypatch):
    clock = iter([0, 20] + [20] * 50)
    monkeypatch.setattr(check.time, "monotonic", lambda: next(clock))
    result = check.check_startup(installed.root)
    assert result["category"] == "UNKNOWN" and not result["startable"]
    assert "UNKNOWN_SLOW_CHECK" in result["reasons"]


@pytest.mark.parametrize("case", ["unreadable", "oversized", "changed_build"])
def test_unknown_inputs_never_claim_ready(installed, case):
    if case == "unreadable":
        (installed.root / "bootstrap-integrity.json").unlink()
    elif case == "oversized":
        installed.cache.write_bytes(b" " * (1024 * 1024 + 1))
    else:
        from kmtech_shared.data_generation import record_generation
        installed.paths.data_root.mkdir(parents=True, exist_ok=True)
        record_generation(installed.paths.data_root / "data-generation.json", generation.CURRENT_VERSION,
                          app_id=generation.APP_ID, subject_sid=installed.sid,
                          build_id=hashlib.sha1(uuid.uuid4().bytes).hexdigest())
    result = check.check_startup(installed.root)
    assert not result["startable"] and "UNKNOWN" in result["categories"]


def test_isolated_interpreter_does_not_import_gui_or_create_bytecode(installed):
    import sys
    source = Path(__file__).resolve().parents[1]
    driver = installed.tmp / "isolated-command.py"
    driver.write_text(f'''import sys
from pathlib import Path
sys.path.insert(0, {str(source)!r})
def audit(event, args):
    if event == 'import' and (args[0] == 'Label_Match' or args[0].startswith('tkinter')):
        raise AssertionError('GUI import in startup CLI')
sys.addaudithook(audit)
import pytest
import startup_check as check
import auto_update, protected_admin, logistics_runtime_profile, user_relay
from tests.dpapi_double import DPAPIDouble
patch = pytest.MonkeyPatch()
DPAPIDouble(Path({str(installed.tmp / "dpapi-double.bin")!r}).read_bytes()).install(patch)
protected_admin.current_process_sid = lambda: {installed.sid!r}
auto_update.current_process_sid = protected_admin.current_process_sid
auto_update.ordinary_user_context = lambda expected=None: expected is None or expected == protected_admin.current_process_sid()
logistics_runtime_profile.legacy_label_transition_enabled = lambda: False
check._already_running = lambda root: False
user_relay._registry_get = lambda: user_relay.user_relay_command_line(Path({str(installed.root)!r}))
run = check.check_startup
check.check_startup = lambda: run(Path({str(installed.root)!r}))
raise SystemExit(check.main(['--json']))
''', encoding="utf-8")
    before = snapshot(installed.tmp)
    completed = subprocess.run([sys.executable, "-I", "-B", str(driver)], capture_output=True,
                               text=True, creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    assert completed.returncode == 0
    assert len(completed.stdout.splitlines()) == 1 and not completed.stderr
    assert json.loads(completed.stdout)["startable"]
    assert snapshot(installed.tmp) == before


@pytest.mark.parametrize("authenticated,enrolled,legacy,expected", [(True, True, False, True), (False, True, False, False),
                                                                  (False, True, True, True), (False, False, False, True)])
def test_offline_transition_decision(authenticated, enrolled, legacy, expected):
    assert catalog.offline_catalog_startable(authenticated=authenticated, enrolled=enrolled, legacy=legacy) is expected

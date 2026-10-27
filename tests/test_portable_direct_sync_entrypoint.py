import ast
import builtins
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys
from types import SimpleNamespace
import uuid

import pytest

import auto_update
import direct_sync_push
import direct_sync_runtime
import label_data_generation
import label_match_product_host as product_host
from tools import direct_sync_relay_runner as runner
from tools import build_portable_release_candidate as portable_builder
from tests.test_direct_sync_runtime import (
    EchoAcceptedSession,
    _isolate_legacy_runtime_tests_from_runtime_lease,  # noqa: F401
    make_config,
    write_csv,
)


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def entrypoint(monkeypatch):
    monkeypatch.setattr(sys, "path", list(sys.path))
    spec = importlib.util.spec_from_file_location(
        "portable_direct_sync_entrypoint", ROOT / "portable/main.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def relay_arguments(tmp_path):
    return [product_host.DIRECT_SYNC_RELAY_MODE, *[
        item for option, name in (
            ("--db-path", "relay.sqlite3"), ("--spool-dir", "spool"),
            ("--producer-manifest-path", "manifest.json"),
            ("--credential-path", "credentials.json"),
            ("--upload-status-dir", "upload_status"),
            ("--runtime-status-path", "status.json"),
            ("--log-path", "relay.jsonl"),
        ) for item in (option, str(tmp_path / name))
    ]]


def test_fresh_relay_child_does_not_load_application(tmp_path):
    code = r'''
import builtins, ctypes, importlib.util, json, pathlib, socket, sys, time
root, work = map(pathlib.Path, sys.argv[1:])
sys.path.insert(0, str(root))
def blocked(*args, **kwargs):
    raise AssertionError('native host access blocked')
socket.socket.connect = socket.socket.connect_ex = socket.create_connection = blocked
if hasattr(ctypes, 'windll'):
    for name in ('SHGetKnownFolderPath', 'SHGetFolderPathW'):
        setattr(ctypes.windll.shell32, name, blocked)
    for name in ('CredReadW', 'CredEnumerateW', 'CredWriteW', 'CredDeleteW'):
        setattr(ctypes.windll.advapi32, name, blocked)
original_import = builtins.__import__
def guarded_import(name, *args, **kwargs):
    if name in ('Label_Match', 'tkinter') or name.startswith('tkinter.'):
        raise AssertionError('relay child loaded application')
    return original_import(name, *args, **kwargs)
builtins.__import__ = guarded_import
before = set(sys.modules)
started = time.perf_counter()
import auto_update, label_data_generation, direct_sync_runtime
import label_match_product_host as host
from tools import direct_sync_relay_runner as runner
gates = []
auto_update.startup_gate = lambda root, args, **kw: gates.append(kw.get('role'))
label_data_generation.check_user_data_open = lambda *args: None
label_data_generation.normalize_data_arguments = lambda *args: None
direct_sync_runtime.prepare_credentials_from_json = lambda *args, **kw: object()
runner.main = lambda args, **kw: 7
arguments = [host.DIRECT_SYNC_RELAY_MODE]
for option, name in (
    ('--db-path', 'relay.sqlite3'), ('--spool-dir', 'spool'),
    ('--producer-manifest-path', 'manifest.json'),
    ('--credential-path', 'credentials.json'),
    ('--upload-status-dir', 'upload_status'),
    ('--runtime-status-path', 'status.json'), ('--log-path', 'relay.jsonl'),
):
    arguments.extend((option, str(work / name)))
spec = importlib.util.spec_from_file_location('portable_entry', root / 'portable/main.py')
entry = importlib.util.module_from_spec(spec)
spec.loader.exec_module(entry)
sys.argv = [str(root / 'portable/main.py'), *arguments]
assert entry.main() == 7
assert 'Label_Match' not in sys.modules
assert 'relay' in gates
print(json.dumps({'modules': len(sys.modules), 'added_modules': len(set(sys.modules) - before),
                  'seconds': time.perf_counter() - started, 'application_modules': 0}))
'''
    result = subprocess.run(
        [sys.executable, "-I", "-B", "-c", code, str(ROOT), str(tmp_path)],
        capture_output=True, text=True,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    assert result.returncode == 0
    evidence = json.loads(result.stdout)
    assert evidence["application_modules"] == 0
    assert evidence["added_modules"] > 0
    print(json.dumps(evidence, sort_keys=True))


@pytest.mark.parametrize("blocked_gate", ["portable", "relay"])
def test_startup_refusal_stops_before_runner(
    monkeypatch, entrypoint, relay_arguments, blocked_gate
):
    calls = []

    def gate(root, arguments, *, role="gui"):
        calls.append((Path(root), list(arguments), role))
        return 20 if (role == "relay") == (blocked_gate == "relay") else None

    monkeypatch.setattr(auto_update, "startup_gate", gate)
    monkeypatch.setattr(runner, "main", lambda *args, **kw: pytest.fail("runner admitted"))
    monkeypatch.setattr(sys, "argv", ["main.py", *relay_arguments])
    original_import = builtins.__import__

    def no_application(name, *args, **kwargs):
        if name == "Label_Match":
            pytest.fail("application loaded before relay refusal")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_application)
    assert entrypoint.main() == 20
    assert [call[2] for call in calls] == (
        ["gui"] if blocked_gate == "portable" else ["gui", "relay"]
    )
    assert all(call[1] == relay_arguments for call in calls)
    assert calls[0][0] == entrypoint.APP_ROOT
    if blocked_gate == "relay":
        assert calls[1][0] == Path(product_host.__file__).resolve().parent


@pytest.mark.parametrize("failure", [None, ValueError, SystemExit])
def test_existing_host_preserves_arguments_and_exit_semantics(
    monkeypatch, entrypoint, relay_arguments, failure
):
    calls = []
    credential_input = object()
    monkeypatch.setattr(auto_update, "startup_gate", lambda *args, **kw: None)
    monkeypatch.setattr(label_data_generation, "check_user_data_open", lambda *args: None)
    monkeypatch.setattr(label_data_generation, "normalize_data_arguments", lambda *args: None)
    monkeypatch.setattr(
        direct_sync_runtime, "prepare_credentials_from_json",
        lambda *args, **kw: credential_input,
    )
    monkeypatch.setattr(product_host, "_record_hosted_relay_failure",
                        lambda args, error: calls.append((list(args), type(error))))

    def run(arguments, **prepared):
        assert list(arguments) == relay_arguments[1:]
        assert prepared["credential_input"] is credential_input
        if failure:
            raise failure(17)
        return 17

    monkeypatch.setattr(runner, "main", run)
    monkeypatch.setattr(sys, "argv", ["main.py", *relay_arguments])
    if failure is SystemExit:
        with pytest.raises(SystemExit) as error:
            entrypoint.main()
        assert error.value.code == 17
    else:
        assert entrypoint.main() == (1 if failure else 17)
    assert calls == ([(relay_arguments[1:], ValueError)] if failure is ValueError else [])


def test_other_arguments_keep_application_entry(monkeypatch, entrypoint):
    calls = []
    monkeypatch.setattr(auto_update, "startup_gate", lambda *args, **kw: None)
    monkeypatch.setattr(sys, "argv", ["main.py", product_host.USER_RELAY_MODE])
    monkeypatch.setitem(sys.modules, "Label_Match", SimpleNamespace(main=lambda: calls.append(True) or 11))
    monkeypatch.setattr(product_host, "dispatch_product_mode",
                        lambda *args: pytest.fail("other entry changed"))
    assert entrypoint.main() == 11
    assert calls == [True]


def test_fake_server_result_matches_application_dispatch(
    monkeypatch, tmp_path, capsys, entrypoint
):
    config = make_config(tmp_path)
    fixed_now = "2026-10-13T00:00:00Z"
    monkeypatch.setattr(direct_sync_push.uuid, "uuid4", lambda: uuid.UUID(int=1))
    monkeypatch.setattr(direct_sync_runtime, "utc_now_text", lambda: fixed_now)
    monkeypatch.setattr(direct_sync_push, "utc_now_text", lambda: fixed_now)
    monkeypatch.setattr(auto_update, "startup_gate", lambda *args, **kw: None)
    monkeypatch.setattr(label_data_generation, "check_user_data_open", lambda *args: None)
    monkeypatch.setattr(label_data_generation, "record_user_data_open", lambda *args: None)
    monkeypatch.setattr(label_data_generation, "normalize_data_arguments", lambda *args: None)
    monkeypatch.setattr(direct_sync_runtime, "prepare_credentials_from_json",
                        lambda *args, **kw: object())
    credentials = direct_sync_runtime.load_credentials_from_json(config.credential_path)
    direct_sync_runtime.enqueue_completed_source_file(
        config, source_file_path=write_csv(tmp_path), credentials=credentials,
    )
    disk = direct_sync_runtime._disk_pressure_report(config)
    monkeypatch.setattr(direct_sync_runtime, "_disk_pressure_report", lambda config: disk)
    arguments = [product_host.DIRECT_SYNC_RELAY_MODE]
    for option, value in (
        ("--db-path", config.db_path), ("--spool-dir", config.spool_dir),
        ("--producer-manifest-path", config.producer_manifest_path),
        ("--credential-path", config.credential_path),
        ("--upload-status-dir", config.upload_status_dir),
        ("--runtime-status-path", config.runtime_status_path),
        ("--log-path", config.log_path),
    ):
        arguments.extend((option, str(value)))
    session = EchoAcceptedSession()
    monkeypatch.setattr(runner, "run_relay_once", lambda config, **kw:
                        direct_sync_runtime.run_relay_once(
                            config, session=session, credentials=credentials, now=fixed_now,
                        ))
    tree = ast.parse((ROOT / "Label_Match.py").read_text(encoding="utf-8"))
    main = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "main")
    namespace = {"sys": sys, "Path": Path, "dispatch_product_mode": product_host.dispatch_product_mode,
                 "release_identity": lambda root: {}, "_label_match_runtime_app_root": lambda: ROOT,
                 "__file__": str(ROOT / "Label_Match.py"),
                 "_label_match_startup_trace": lambda *a, **kw: None}
    namespace["_first_run_onboarding_enabled"] = lambda: False
    constants = [node for node in tree.body if isinstance(node, ast.Assign)
                 and any(isinstance(target, ast.Name) and target.id in {
                     "LABEL_MATCH_NEW_SCREEN_ARGUMENT", "LABEL_MATCH_SCREEN_HOST"}
                     for target in node.targets)]
    exec(compile(ast.Module(body=constants, type_ignores=[]), "application-constants", "exec"), namespace)
    from tests._startup_timing_fixture import bind_startup_timing_namespace
    bind_startup_timing_namespace(namespace)
    exec(compile(ast.Module(body=[main], type_ignores=[]), "application-main", "exec"), namespace)
    before = {path: path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}
    monkeypatch.setattr(sys, "argv", ["main.py", *arguments])
    old_result = namespace["main"]()
    old_output = capsys.readouterr()
    old_calls = list(session.calls)
    old_artifacts = {path: path.read_bytes() for path in (
        Path(config.runtime_status_path), Path(config.log_path),
        *Path(config.upload_status_dir).glob("*.json"),
    )}
    for path in tmp_path.rglob("*"):
        if path.is_file() and path not in before:
            path.unlink()
    for path, content in before.items():
        path.write_bytes(content)
    session.calls.clear()
    new_result = entrypoint.main()
    new_output = capsys.readouterr()
    assert new_result == old_result == 0
    assert new_output == old_output
    assert len(session.calls) == len(old_calls) == 1
    assert session.calls == old_calls
    assert {path: path.read_bytes() for path in old_artifacts} == old_artifacts


def test_stock_builder_supplies_entrypoint_and_relay_imports(tmp_path):
    sources = portable_builder._copy_application(ROOT, tmp_path / "app")
    assert (tmp_path / "app/main.py").read_bytes() == (ROOT / "portable/main.py").read_bytes()
    assert (tmp_path / "app/label_match_product_host.py").is_file()
    assert (tmp_path / "app/tools/direct_sync_relay_runner.py").is_file()
    portable_builder._assert_portable_import_closure(
        tmp_path, ROOT, sources, python_executable=Path(sys.executable),
    )


@pytest.fixture(scope="module")
def isolated_guard_git(tmp_path_factory):
    from tests import test_legacy_label_upgrade_guard as guard

    isolated = tmp_path_factory.mktemp("legacy-guard-git") / "source"
    subprocess.run(
        ["git", "-c", "core.autocrlf=false", "clone", "--shared", "--no-checkout",
         "--no-hardlinks", str(ROOT), str(isolated)],
        check=True, capture_output=True,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    paths = {"Label_Match.py", "writer_session_fence.py"}
    paths.update(name.removeprefix("app/") for name in guard._declared_current_user_profile_pairs())
    for name in paths:
        target = isolated / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, target)
    return isolated


@pytest.mark.parametrize("test_name", [
    "test_declared_current_user_profile_pair_matches_reviewed_preimage_and_tree",
    "test_writer_preimages_cover_only_the_accepted_profile_releases",
    "test_declared_nextfix_pair_is_exactly_e88011c_and_this_tree",
])
def test_existing_upgrade_guard_with_independent_git(monkeypatch, isolated_guard_git, test_name):
    from tests import test_legacy_label_upgrade_guard as guard

    monkeypatch.setattr(guard, "ROOT", isolated_guard_git)
    getattr(guard, test_name)()

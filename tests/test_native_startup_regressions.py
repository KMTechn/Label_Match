"""lm10 native defects reproduced through the installer/consumer/relay paths."""

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

import current_user_onboarding as onboarding
import current_user_scheduled_task as scheduled
import label_match_product_host as host
import user_relay as relay

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/bootstrap_inventory_order.json"
pytestmark = pytest.mark.skipif(os.name != "nt", reason="Windows native regression")


@pytest.fixture
def inventory_tree(tmp_path, monkeypatch):
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    root = tmp_path / "portable"
    content = fixture["content"].encode("utf-8")
    rows = []
    for name in reversed(fixture["paths_in_expected_order"]):
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    for name in fixture["paths_in_expected_order"]:
        rows.append(dict(path=name, size=len(content), sha256=hashlib.sha256(content).hexdigest()))
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "local"))
    for key in ("LABEL_MATCH_SAVE_DIR", "LABEL_MATCH_SETTINGS_PATH", "LABEL_MATCH_DIRECT_SYNC_ROOT",
                "LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT", "KM_LOGISTICS_PROFILE_PATH"):
        monkeypatch.delenv(key, raising=False)
    return root, rows


def _aggregate(rows):
    return hashlib.sha256("".join(
        f"{r['sha256']} {r['size']} {r['path']}\n" for r in rows
    ).encode("utf-8")).hexdigest()


def _record(root, rows):
    path = root / "bootstrap-integrity.json"
    path.write_text(json.dumps(dict(
        schema_version="label-match-bootstrap-integrity-v1", status="PASS",
        code_root=str(root), file_count=len(rows), files=rows,
        aggregate_sha256=_aggregate(rows), identity_profile_created=False,
        state_scope="current_user_first_run",
    )), encoding="utf-8")
    return path


def _ps(tmp_path, body):
    script = tmp_path / "probe.ps1"
    script.write_text("$ErrorActionPreference = 'Stop'\n" + body, encoding="utf-8-sig")
    env = os.environ.copy()
    # A PS7 parent injects its incompatible Security module into PS5.1 otherwise.
    env["PSModulePath"] = os.pathsep.join([
        str(Path(os.environ["ProgramFiles"]) / "WindowsPowerShell/Modules"),
        str(Path(os.environ["SystemRoot"]) / "System32/WindowsPowerShell/v1.0/Modules"),
    ])
    powershell = Path(os.environ["SystemRoot"]) / "System32/WindowsPowerShell/v1.0/powershell.exe"
    result = subprocess.run([str(powershell), "-NoProfile", "-NonInteractive", "-ExecutionPolicy",
                             "Bypass", "-File", str(script)], env=env, capture_output=True, timeout=45)
    assert len(result.stdout) < 16384 and len(result.stderr) < 16384
    assert result.returncode == 0, result.stderr.decode(errors="replace")
    assert result.stderr == b""
    return result.stdout.decode("utf-8-sig").strip()


def _quote(path):
    return "'" + str(path).replace("'", "''") + "'"


@pytest.mark.parametrize("culture", ["en-US", "tr-TR", "ko-KR"])
def test_real_ps51_inventory_writer_is_accepted_by_python(inventory_tree, tmp_path, culture):
    root, rows = inventory_tree
    output = _ps(tmp_path, f"""
[Threading.Thread]::CurrentThread.CurrentCulture = [Globalization.CultureInfo]::GetCultureInfo('{culture}')
. {_quote(ROOT / 'tools/bootstrap_integrity.ps1')}
if ($PSVersionTable.PSVersion.Major -ne 5) {{ throw 'requires stock PS5.1' }}
$record = Write-BootstrapIntegrityRecord -Root {_quote(root)} -CodeRoot {_quote(root)}
[void](Assert-BootstrapIntegrityRecord -Root {_quote(root)})
Write-Output $record.aggregate_sha256
""")
    result = onboarding.verify_bootstrap_integrity(
        onboarding.resolve_current_user_onboarding_paths(root), required=True)
    assert result["status"] == "PASS"
    assert output == result["aggregate_sha256"] == _aggregate(rows)
    assert json.loads((root / "bootstrap-integrity.json").read_text(encoding="utf-8"))["files"] == rows


def test_python_inventory_enforces_ordinal_order_and_aggregate(inventory_tree):
    root, rows = inventory_tree
    paths = onboarding.resolve_current_user_onboarding_paths(root)
    path = _record(root, rows)
    assert onboarding.verify_bootstrap_integrity(paths, required=True)["aggregate_sha256"] == _aggregate(rows)
    _record(root, list(reversed(rows)))
    with pytest.raises(ValueError, match="inventory integrity"):
        onboarding.verify_bootstrap_integrity(paths, required=True)
    path = _record(root, rows)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["aggregate_sha256"] = "0" * 64
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="inventory integrity"):
        onboarding.verify_bootstrap_integrity(paths, required=True)


def test_ps51_verifier_rejects_self_consistent_noncanonical_order(inventory_tree, tmp_path):
    root, rows = inventory_tree
    _record(root, list(reversed(rows)))
    output = _ps(tmp_path, f"""
. {_quote(ROOT / 'tools/bootstrap_integrity.ps1')}
try {{ [void](Assert-BootstrapIntegrityRecord -Root {_quote(root)}) }}
catch {{ Write-Output 'rejected'; exit 0 }}
Write-Output 'accepted'
""")
    assert output == "rejected"


def test_canonical_installer_expected_bootstrap_aggregate_matches_contract(inventory_tree, tmp_path):
    root, _ = inventory_tree
    for name in ("INSTALL_THIS_PC.ps1", "tools/bootstrap_integrity.ps1", "tools/label_writer_fence.ps1"):
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("fixture\n", encoding="utf-8")
    # Extract actual function definitions without invoking installer mutations.
    output = _ps(tmp_path, f"""
$ast = [Management.Automation.Language.Parser]::ParseFile({_quote(ROOT / 'INSTALL_CANONICAL_PORTABLE.ps1')}, [ref]$null, [ref]$null)
foreach ($name in @('Full', 'Sha', 'UInt64BE', 'HexBytes', 'PortableInventory')) {{
    $node = $ast.Find({{param($n) $n -is [Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $name}}, $false)
    Invoke-Expression $node.Extent.Text
}}
. {_quote(ROOT / 'tools/bootstrap_integrity.ps1')}
$expected = (PortableInventory {_quote(root)}).bootstrap_aggregate_sha256
$record = Write-BootstrapIntegrityRecord -Root {_quote(root)} -CodeRoot {_quote(root)}
if ($expected -cne $record.aggregate_sha256) {{ throw 'installer aggregate mismatch' }}
Write-Output $expected
""")
    paths = onboarding.resolve_current_user_onboarding_paths(root)
    assert onboarding.verify_bootstrap_integrity(paths, required=True)["aggregate_sha256"] == output


def _legacy_clear():
    return dict(schema=onboarding.LEGACY_TASK_QUIESCENCE_VERSION,
                required_state=onboarding.LEGACY_TASK_REQUIRED_STATE, status="PASS",
                read_only=True, task_or_process_mutated=False)


def test_portable_cli_rejects_out_of_order_record_before_state_inspection(inventory_tree, monkeypatch):
    root, rows = inventory_tree
    _record(root, list(reversed(rows)))
    real = onboarding.onboard_current_user
    inspected = []
    monkeypatch.setattr(sys, "frozen", False, raising=False)
    monkeypatch.setattr(onboarding, "inspect_current_user_state", lambda *a, **k: inspected.append(True) or {})
    monkeypatch.setattr(onboarding, "onboard_current_user", lambda *a, **k: real(
        *a, **k, legacy_task_quiescence_reader=_legacy_clear))
    assert host.dispatch_product_mode(["--onboard-current-user", "--app-root", str(root)]) == onboarding.ONBOARDING_EXIT_CODE
    report = json.loads(onboarding.resolve_current_user_onboarding_paths(root).onboarding_report_path.read_text(encoding="utf-8"))
    assert "bootstrap inventory integrity" in report["failure"]
    assert inspected == []


def test_portable_cli_default_root_matches_gui(inventory_tree, monkeypatch):
    import Label_Match as app
    root, _ = inventory_tree
    monkeypatch.setattr(sys, "frozen", False, raising=False)
    monkeypatch.setattr(host, "__file__", str(root / "app/label_match_product_host.py"))
    monkeypatch.setattr(onboarding, "__file__", str(root / "app/current_user_onboarding.py"))
    monkeypatch.setattr(app, "__file__", str(root / "app/Label_Match.py"))
    assert onboarding._default_app_root() == Path(app._label_match_runtime_app_root()) == root
    assert host.requires_bootstrap_integrity(onboarding._default_app_root())


@pytest.mark.parametrize("cause, required_words, forbidden_words", [
    (ValueError("bootstrap inventory integrity record is invalid"), ("무결성", "다시 설치", "관리자"), ("네트워크 연결",)),
    (ConnectionError("server unreachable"), ("네트워크", "서버"), ("다시 설치",)),
])
def test_real_first_run_dialog_uses_cause_and_foreground_parent(inventory_tree, monkeypatch, cause, required_words, forbidden_words):
    import Label_Match as app
    root, _ = inventory_tree
    monkeypatch.setattr(onboarding, "verify_bootstrap_integrity", lambda *a, **k: (_ for _ in ()).throw(cause))
    with pytest.raises(onboarding.CurrentUserOnboardingError) as caught:
        onboarding.onboard_current_user(root, require_bootstrap_integrity=True,
                                       legacy_task_quiescence_reader=_legacy_clear)
    calls = []
    class Parent:
        def __getattr__(self, name):
            return lambda *args, **kwargs: calls.append((name, args))
    parent = Parent()
    monkeypatch.setattr(app.tk, "Tk", lambda: parent)
    def show(title, text, **kwargs):
        assert kwargs.get("parent") is parent
        assert ("attributes", ("-topmost", True)) in calls
        assert any(name == "update" for name, _ in calls)
        assert any(name == "deiconify" for name, _ in calls)
        assert any(name == "lift" for name, _ in calls)
        assert all(word in text for word in required_words)
        assert all(word not in text for word in forbidden_words)
        assert str(caught.value.report_path) in text
        calls.append(("show", ()))
    monkeypatch.setattr(app.messagebox, "showerror", show)
    app._show_first_run_onboarding_error(caught.value)
    assert ("show", ()) in calls
    assert calls[-1] == ("destroy", ())


def test_pid_probe_keeps_live_child_alive_and_rejects_dead_pid():
    child = subprocess.Popen([sys.executable, "-B", "-c", "import sys; print('ready', flush=True); sys.stdin.read()"],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             creationflags=subprocess.CREATE_NO_WINDOW)
    try:
        assert child.stdout.readline() == b"ready\r\n"
        assert relay._pid_exists(child.pid)
        time.sleep(0.05)
        assert child.poll() is None, "liveness probe terminated its owned child"
    finally:
        child.communicate(timeout=5)
    assert relay._pid_exists(child.pid) is False
    assert relay._pid_exists(0) is False
    assert relay._pid_exists(-1) is False


@pytest.mark.parametrize("observer_console", [False, True])
def test_scheduled_handler_recognizes_real_persistent_writer(inventory_tree, tmp_path, monkeypatch, observer_console):
    root, _ = inventory_tree
    (root / "runtime/python.exe").write_bytes(b"fixture")
    monkeypatch.setattr(scheduled, "CANONICAL_ROOT", root)
    monkeypatch.setattr("logistics_runtime_profile.load_logistics_runtime_profile",
                        lambda **k: SimpleNamespace(tls_ca_bundle_path=""))
    paths = onboarding.resolve_current_user_onboarding_paths(root)
    script = tmp_path / "owned-relay.py"
    script.write_text(
        "import sys\nfrom types import SimpleNamespace\nsys.path.insert(0, " + repr(str(ROOT)) + ")\n"
        "import user_relay, logistics_runtime_profile\n"
        "logistics_runtime_profile.load_logistics_runtime_profile = lambda **k: SimpleNamespace(tls_ca_bundle_path='')\n"
        "user_relay._runtime_cycle = lambda **k: dict(status='idle', process_status='PASS', process_returncode=0, relay_status='idle')\n"
        "raise SystemExit(user_relay.main(sys.argv[1:]))\n", encoding="utf-8")
    with (tmp_path / "child-out.log").open("wb") as out, (tmp_path / "child-err.log").open("wb") as err:
        child = subprocess.Popen([str(Path(sys.executable).with_name('pythonw.exe')), "-B", str(script), "--app-root", str(root), "--interval-seconds", "1"],
                                 stdout=out, stderr=err, creationflags=subprocess.CREATE_NO_WINDOW)
        try:
            deadline = time.monotonic() + 10
            while not relay.user_relay_status_path(paths.direct_sync_root).is_file() and time.monotonic() < deadline:
                assert child.poll() is None
                time.sleep(0.05)
            status = json.loads(relay.user_relay_status_path(paths.direct_sync_root).read_text(encoding="utf-8"))
            assert status["status"] == "RUNNING" and status["process_id"] == child.pid
            observer = tmp_path / "scheduled-observer.py"
            observer.write_text(
                "import sys\nfrom pathlib import Path\nfrom types import SimpleNamespace\nsys.path.insert(0, " + repr(str(ROOT)) + ")\n"
                "import user_relay, logistics_runtime_profile, current_user_scheduled_task\n"
                "logistics_runtime_profile.load_logistics_runtime_profile = lambda **k: SimpleNamespace(tls_ca_bundle_path='')\n"
                "current_user_scheduled_task.CANONICAL_ROOT = Path(sys.argv[1])\n"
                "raise SystemExit(user_relay.scheduled_main(['--app-root', sys.argv[1]]))\n", encoding="utf-8")
            startup = subprocess.STARTUPINFO()
            startup.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startup.wShowWindow = subprocess.SW_HIDE
            result = subprocess.run([sys.executable, "-B", str(observer), str(root)],
                                    capture_output=True, timeout=10, startupinfo=startup,
                                    creationflags=subprocess.CREATE_NEW_CONSOLE if observer_console else subprocess.CREATE_NO_WINDOW)
            scheduled_status = json.loads((paths.status_dir / "scheduled_direct_sync_relay_status.json").read_text(encoding="utf-8"))
            assert result.returncode == 0, scheduled_status
            assert scheduled_status["outcome"] == "existing_healthy_relay"
            assert scheduled_status["persistent_process_id"] == child.pid
            time.sleep(0.05)
            assert child.poll() is None, "scheduled owner probe terminated persistent writer"
        finally:
            relay.user_relay_stop_path(paths.direct_sync_root).parent.mkdir(parents=True, exist_ok=True)
            relay.user_relay_stop_path(paths.direct_sync_root).touch()
            child.wait(timeout=10)


def test_scheduled_argv_preserves_bounded_exception_detail_and_log(inventory_tree, monkeypatch):
    root, _ = inventory_tree
    (root / "runtime/python.exe").write_bytes(b"fixture")
    monkeypatch.setattr(scheduled, "CANONICAL_ROOT", root)
    spec = scheduled.build_current_user_task_spec(root)
    import ctypes
    from ctypes import wintypes
    shell32 = ctypes.WinDLL("shell32", use_last_error=True)
    shell32.CommandLineToArgvW.argtypes = (wintypes.LPCWSTR, ctypes.POINTER(ctypes.c_int))
    shell32.CommandLineToArgvW.restype = ctypes.POINTER(wintypes.LPWSTR)
    count = ctypes.c_int()
    parsed = shell32.CommandLineToArgvW('python ' + spec["arguments"], ctypes.byref(count))
    try:
        arguments = [parsed[i] for i in range(1, count.value)]
    finally:
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.LocalFree.argtypes = (ctypes.c_void_p,)
        kernel32.LocalFree.restype = ctypes.c_void_p
        kernel32.LocalFree(parsed)
    mode = arguments.index(host.SCHEDULED_RELAY_MODE)
    log_path = onboarding.resolve_current_user_onboarding_paths(root).logs_dir / "scheduled_direct_sync_relay.jsonl"
    assert host._option_value(arguments, "--log-path") == str(log_path)
    def fail(argv):
        relay.build_scheduled_parser().parse_args(argv)
        raise RuntimeError("owner status unreadable; secret=do-not-store " + "x" * 2000)
    monkeypatch.setattr(relay, "scheduled_main", fail)
    assert host.dispatch_product_mode(arguments[mode:]) == 1
    status_path = onboarding.resolve_current_user_onboarding_paths(root).status_dir / "scheduled_direct_sync_relay_status.json"
    status = json.loads(status_path.read_text(encoding="utf-8"))
    event = json.loads(log_path.read_text(encoding="utf-8"))
    assert status["error_type"] == event["error_type"] == "RuntimeError"
    for message in (status["error_message"], event["error_message"]):
        assert "owner status unreadable" in message
        assert "do-not-store" not in message
        assert len(message) <= 512


def test_hosted_failure_retains_exception_message_without_secret(tmp_path):
    status_path = tmp_path / "failure.json"
    log_path = tmp_path / "failure.jsonl"
    host._record_hosted_relay_failure(
        ["--runtime-status-path", str(status_path), "--log-path", str(log_path)],
        RuntimeError("owner status unreadable; secret=never-persist " + "x" * 2000),
    )
    for path in (status_path, log_path):
        diagnostic = json.loads(path.read_text(encoding="utf-8"))
        assert "owner status unreadable" in diagnostic["error_message"]
        assert diagnostic["error_type"] == "RuntimeError"
        assert "never-persist" not in diagnostic["error_message"]
        assert len(diagnostic["error_message"]) <= 512


def test_first_run_dialog_prepares_real_tk_parent(monkeypatch, tmp_path):
    import Label_Match as app
    parents = []
    def show(title, message, **kwargs):
        parent = kwargs["parent"]
        assert parent.winfo_viewable()
        assert parent.attributes("-topmost")
        assert parent.title() == app.FIRST_RUN_ONBOARDING_ERROR_TITLE
        assert "무결성" in message and "다시 설치" in message
        parents.append(parent)
    monkeypatch.setattr(app.messagebox, "showerror", show)
    failure = onboarding.CurrentUserOnboardingError(
        "bootstrap inventory integrity record is invalid", report_path=tmp_path / "report.json")
    failure.cause_code = "BOOTSTRAP_INTEGRITY_INVALID"
    app._show_first_run_onboarding_error(failure)
    assert len(parents) == 1
    with pytest.raises(app.tk.TclError):
        parents[0].winfo_exists()


@pytest.mark.parametrize("transport_result, expected_cause, expected_status", [
    ("connection", "NETWORK_OR_SERVER_UNAVAILABLE", "FAILED"),
    ("connect-timeout", "NETWORK_OR_SERVER_UNAVAILABLE", "FAILED"),
    ("read-timeout", "NETWORK_OR_SERVER_UNAVAILABLE", "FAILED"),
    ("500-json", "NETWORK_OR_SERVER_UNAVAILABLE", "FAILED"),
    ("502-json", "NETWORK_OR_SERVER_UNAVAILABLE", "FAILED"),
    ("503-json", "NETWORK_OR_SERVER_UNAVAILABLE", "FAILED"),
    ("504-json", "NETWORK_OR_SERVER_UNAVAILABLE", "FAILED"),
    ("502-html", "NETWORK_OR_SERVER_UNAVAILABLE", "FAILED"),
    ("503-html", "NETWORK_OR_SERVER_UNAVAILABLE", "FAILED"),
    ("504-html", "NETWORK_OR_SERVER_UNAVAILABLE", "FAILED"),
    ("408-json", "NETWORK_OR_SERVER_UNAVAILABLE", "FAILED"),
    ("429-json", "NETWORK_OR_SERVER_UNAVAILABLE", "FAILED"),
    ("403-json", "SETUP_FAILED", "FAILED"),
    ("409-conflict", "SETUP_FAILED", "RECOVERY_REQUIRED"),
    ("503-recovery", "SETUP_FAILED", "RECOVERY_REQUIRED"),
])
def test_real_enrollment_transport_failure_reaches_dialog(
    inventory_tree, monkeypatch, transport_result, expected_cause, expected_status,
):
    import Label_Match as app
    from tools import register_label_match_worker_pc as registration

    root, rows = inventory_tree
    _record(root, rows)
    monkeypatch.setattr(sys, "frozen", False, raising=False)
    class OwnedMutex:
        def __init__(self, *args, **kwargs):
            pass
        def acquire(self):
            return {"status": "ACQUIRED", "scope": "fixture"}
        def release(self):
            pass
        def __enter__(self):
            return self.acquire()
        def __exit__(self, *args):
            self.release()

    # Only transport and OS/token/key boundaries are substituted. The real CLI,
    # persisted report, integer return, state inspection and GUI selection run.
    attempts = []
    def transport(url, **kwargs):
        assert url == "https://transport.example.invalid/api/producer-ingest/v2/enroll"
        attempts.append(url)
        exceptions = {
            "connection": registration.requests.exceptions.ConnectionError,
            "connect-timeout": registration.requests.exceptions.ConnectTimeout,
            "read-timeout": registration.requests.exceptions.ReadTimeout,
        }
        if transport_result in exceptions:
            raise exceptions[transport_result]("fixture transport unavailable")
        status, response_kind = transport_result.split("-", 1)
        def response_json():
            if response_kind == "html":
                raise ValueError("gateway returned HTML")
            code = {
                "conflict": "producer_identity_conflict",
                "recovery": "admin_recovery_required",
            }.get(response_kind, "fixture_http_failure")
            return {"error": {"code": code, "message": "fixture server response"}}
        return SimpleNamespace(status_code=int(status), json=response_json)

    monkeypatch.setattr(registration.requests, "post", transport)
    monkeypatch.setattr(registration, "_prepare_possession_key", lambda *a, **k: {"public_jwk": {}})
    monkeypatch.setattr(registration, "_token_from_sources", lambda *a, **k: ("ip_allowlist", ""))
    monkeypatch.setattr(registration, "_machine_identity", lambda args: "00000000-0000-4000-8000-000000000001")
    monkeypatch.setattr(registration, "_current_user_sid", lambda: "S-1-5-21-1-2-3-1001")
    monkeypatch.setattr(registration, "require_enrollment_mutex_owned", lambda: None)
    monkeypatch.setattr(registration, "EnrollmentMutex", OwnedMutex)
    monkeypatch.setattr(onboarding, "EnrollmentMutex", OwnedMutex)
    dialogs = []
    class Parent:
        def __getattr__(self, name):
            return lambda *a, **k: None
    monkeypatch.setattr(app.tk, "Tk", Parent)
    monkeypatch.setattr(app.messagebox, "showerror", lambda title, text, **k: dialogs.append(text))

    with pytest.raises(onboarding.CurrentUserOnboardingError) as caught:
        onboarding.onboard_current_user(root, server_base_url="https://transport.example.invalid",
                                       legacy_task_quiescence_reader=_legacy_clear)
    app._show_first_run_onboarding_error(caught.value)
    paths = onboarding.resolve_current_user_onboarding_paths(root)
    registration_report = json.loads(paths.registration_report_path.read_text(encoding="utf-8"))
    report = json.loads(paths.onboarding_report_path.read_text(encoding="utf-8"))
    assert len(attempts) == len(dialogs) == 1
    assert report["bootstrap_integrity"]["status"] == "PASS"
    assert not paths.identity_path.exists() and not paths.ledger_path.exists()
    assert caught.value.status == expected_status
    assert caught.value.cause_code == expected_cause
    assert report["cause_code"] == expected_cause
    if expected_status == "RECOVERY_REQUIRED":
        assert registration_report["status"] == "ADMIN_RECOVERY_REQUIRED"
        assert "기존 등록을 복구" in dialogs[0]
        assert "네트워크" not in dialogs[0]
        assert "failure_category" not in registration_report
    elif expected_cause == "NETWORK_OR_SERVER_UNAVAILABLE":
        assert registration_report["status"] == "BLOCKED"
        assert registration_report["failure_category"] == expected_cause
        assert "네트워크" in dialogs[0] and "서버" in dialogs[0]
        assert "다시 설치" not in dialogs[0]
    else:
        assert "네트워크" not in dialogs[0]
        assert "failure_category" not in registration_report

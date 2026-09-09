"""Normal installer transactions with isolated native/transport boundaries.

The complete canonical installer, placement, writer fence, removal, stop-marker
protocol and relay loop execute. Registry/tasks and enrollment transport use
file-backed fixtures; these tests do not claim native VM qualification.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import time

import pytest

from current_user_onboarding import resolve_current_user_onboarding_paths
from direct_sync_push import manifest_hash
from producer_runtime_client import init_runtime_schema, _create_state, _scope_values
from tests.test_current_user_onboarding import _ready_state
from tests.test_writer_transition import (
    INSTALLER, _definitions, _environment, _manifest, _pin, _ps, _quote, portable_pair,
)
from tools.register_label_match_worker_pc import (
    _current_machine_guid, _current_user_sid, derive_path_independent_install_id,
)


@pytest.fixture
def tmp_path(tmp_path_factory):
    # Windows PowerShell 5.1 frozen-helper I/O retains MAX_PATH constraints.
    return tmp_path_factory.mktemp("h")


@pytest.fixture(scope="module")
def healthy_pair(tmp_path_factory, portable_pair):
    root = tmp_path_factory.mktemp("healthy-pair")
    result = []
    for label, source in zip(("old", "candidate"), portable_pair[:2]):
        target = root / label
        shutil.copytree(source, target)
        module = target / "app/current_user_onboarding.py"
        # The only simulated Python boundary is credential/profile decryption.
        # The production inspector still checks all persisted bindings.
        with module.open("a", encoding="utf-8", newline="\n") as stream:
            stream.write('''
if os.environ.get("LM_HEALTHY_LIFECYCLE_FIXTURE") == "1":
    from types import SimpleNamespace
    def _fixture_credential(path):
        value = _read_json(path, "fixture credential")
        if value.get("secret_ref") != "dpapi:fixture-current-user":
            raise ValueError("fixture current-user DPAPI owner mismatch")
        return SimpleNamespace(**value)
    def _fixture_profile(path):
        return SimpleNamespace(**_read_json(path, "fixture profile"))
    inspect_current_user_state.__kwdefaults__.update(
        credential_loader=_fixture_credential, profile_loader=_fixture_profile)
    load_credentials_from_json = _fixture_credential
    CANONICAL_PORTABLE_ROOT = Path(os.environ["LM_HEALTHY_INSTALL_ROOT"])
''')
        _pin(target)
        _manifest(target, ("1" if label == "old" else "2") * 40)
        result.append(target)
    return tuple(result)


def _json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _fixture(tmp_path, healthy_pair):
    old, candidate = healthy_pair
    install = tmp_path / "canonical/current"
    shutil.copytree(old, install)
    env = _environment(tmp_path)
    for name in (
        "LABEL_MATCH_DIRECT_SYNC_ROOT",
        "LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT",
        "LABEL_MATCH_SAVE_DIR",
        "LABEL_MATCH_SETTINGS_PATH",
        "KM_LOGISTICS_PROFILE_PATH",
    ):
        env.pop(name, None)
    for name in ("KMTECH_LABEL_CONFLICT_RESOLUTION_RECEIPT_PATH", "KMTECH_LABEL_CONFLICT_RESOLUTION_RECEIPT_SHA256"):
        env.pop(name, None)
    env.update(LM_HEALTHY_LIFECYCLE_FIXTURE="1", LM_HEALTHY_INSTALL_ROOT=str(install), LM_TRANSITION_ACTIVATION_FIXTURE="1")
    paths = resolve_current_user_onboarding_paths(install, environ=env)
    # The no-network relay adapter uses the selected root explicitly.
    env["LABEL_MATCH_DIRECT_SYNC_ROOT"] = str(paths.direct_sync_root)
    identity = _ready_state(paths)
    identity["producer_install_id"] = derive_path_independent_install_id(machine_guid=_current_machine_guid(), user_sid=_current_user_sid())
    _json(paths.identity_path, identity)
    manifest = json.loads(paths.producer_manifest_path.read_text(encoding="utf-8"))
    manifest["pc_identity"]["producer_install_id"] = identity["producer_install_id"]
    _json(paths.producer_manifest_path, manifest)
    registration = json.loads(paths.registration_report_path.read_text(encoding="utf-8"))
    registration["manifest_hash"] = manifest_hash(manifest)
    _json(paths.registration_report_path, registration)
    credential = json.loads(paths.credential_path.read_text(encoding="utf-8"))
    credential.update(key_id="healthy-key", endpoint_url="https://fixture.invalid/api/producer-ingest/v2/events", secret_ref="dpapi:fixture-current-user", secret_data_dir=str(paths.direct_sync_root))
    _json(paths.credential_path, credential)
    secret = paths.direct_sync_root / "secrets/fixture-current-user.dpapi"
    secret.parent.mkdir(parents=True, exist_ok=True)
    secret.write_bytes(b"current-user-credential-dpapi-fixture")
    paths.settings_path.parent.mkdir(parents=True, exist_ok=True)
    paths.settings_path.write_text('{"worker_name":"TEST operator"}', encoding="utf-8")
    paths.data_root.mkdir(parents=True, exist_ok=True)
    (paths.data_root / "business.csv").write_text("PHS2,completed,preserved\n", encoding="utf-8")
    from types import SimpleNamespace
    database = paths.direct_sync_root / "queue/direct_sync_relay.sqlite3"
    database.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(database) as connection:
        connection.row_factory = sqlite3.Row
        init_runtime_schema(connection)
        _create_state(connection, _scope_values(SimpleNamespace(**credential), identity['producer_install_id']), "2026-09-05T00:00:00Z")
    return install, candidate, env, paths


def _adapters(install, candidate, env, tmp_path, *, failure=""):
    state = Path(env["LM_TRANSITION_NATIVE_STATE"])
    launcher = tmp_path / "restore-relay.py"
    launcher.write_text(
        "import json, pathlib, subprocess, sys\n"
        "command = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding='utf-8-sig'))\n"
        "process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, close_fds=True)\n"
        "print(process.pid)\n", encoding="utf-8")
    return f'''
$state = {_quote(state)}
function Snapshot {{
    $p = Join-Path $state 'registry.txt'
    if (Test-Path $p) {{ return [ordered]@{{exists=$true; kind='String'; data=[IO.File]::ReadAllText($p)}} }}
    return [ordered]@{{exists=$false; kind=''; data=''}}
}}
function Restore($Before) {{
    $p = Join-Path $state 'registry.txt'
    if ($Before.exists) {{ [IO.File]::WriteAllText($p, [string]$Before.data) }}
    elseif (Test-Path $p) {{ Remove-Item -LiteralPath $p }}
}}
function Get-ScheduledTask {{ param($TaskName, $TaskPath, $ErrorAction) return @() }}
function ScheduledTaskSnapshot($AuditRoot, $RunId) {{ return [pscustomobject]@{{exists=$false; enabled=$false; xml_sha256=''; backup_path=''}} }}
function RestoreScheduledTask($Before) {{}}
function Get-AuthenticodeSignature {{ param($FilePath) return @{{Status='Valid'}} }}
function Set-Acl {{ param($LiteralPath, $AclObject, $Path) }}
function Relays {{
    return @(Get-CimInstance Win32_Process -Filter "Name='pythonw.exe'" | Where-Object {{ $_.ExecutablePath -ceq {_quote(install / 'runtime/pythonw.exe')} -and $_.CommandLine -like '*--label-match-user-relay*' }})
}}
function UnquiescedProductWriters {{ return @(Relays) }}
function StartRaw([string]$Line) {{
    if (Test-Path (Get-LabelWriterFenceActivePath $writerFenceControlRoot)) {{ throw 'fixture relay restarted under active fence' }}
    foreach ($name in $WriterDelegationEnvironmentNames) {{ [Environment]::SetEnvironmentVariable($name, '', 'Process') }}
    [IO.File]::WriteAllText({_quote(tmp_path / 'restore-command.json')}, ($Line | ConvertTo-Json -Compress))
    $pidText = & (Join-Path $source 'runtime/python.exe') -I -B {_quote(launcher)} {_quote(tmp_path / 'restore-command.json')}
    if ($LASTEXITCODE -ne 0) {{ throw 'fixture relay restart failed' }}
    return [int]$pidText
}}
function Start-Process {{
    [CmdletBinding()] param([string]$FilePath, [string[]]$ArgumentList, [string]$Verb, [string]$WindowStyle,
        [switch]$Wait, [switch]$PassThru, [string]$RedirectStandardOutput, [string]$RedirectStandardError)
    # Only UAC presentation is replaced; the production frozen/encoded launcher
    # and privileged helper still execute, including typed pins and rollback.
    if ($PSBoundParameters.ContainsKey('Verb')) {{
        if ($Verb -cne 'RunAs' -or $env:KMTECH_FACTORY_INSTALL_TEST_MODE -cne '1') {{ throw 'unexpected fixture elevation' }}
        if ({_quote(failure)} -ceq 'placement') {{ throw 'injected placement elevation failure' }}
        [void]$PSBoundParameters.Remove('Verb')
        $PSBoundParameters.RedirectStandardOutput = {_quote(tmp_path / 'placement.stdout.log')}
        $PSBoundParameters.RedirectStandardError = {_quote(tmp_path / 'placement.stderr.log')}
    }}
    Microsoft.PowerShell.Management\\Start-Process @PSBoundParameters
}}
$script:realProduct = ${{function:Product}}
function Product([string]$Root, [string]$Mode, [string[]]$ExtraArguments = @()) {{
    if ({_quote(failure)} -ceq 'onboarding' -and $Mode -ceq '--onboard-current-user') {{ throw 'injected onboarding transport failure' }}
    & $script:realProduct $Root $Mode $ExtraArguments
}}
'''


def _install(tmp_path, install, candidate, env, *, failure=""):
    source = INSTALLER.read_text(encoding="utf-8")
    anchor = "if (-not $SourceRoot) { $SourceRoot = $PSScriptRoot }"
    code = source.replace(anchor, _adapters(install, candidate, env, tmp_path, failure=failure) + "\n" + anchor, 1)
    code = code.replace("$writerFenceControlRoot = Join-Path $defaultDirectSyncRoot 'control\\writer-session'", "$writerFenceControlRoot = $env:KMTECH_LABEL_WRITER_CONTROL_ROOT")
    script = tmp_path / f"canonical-{time.time_ns()}.ps1"
    script.write_text(code, encoding="utf-8-sig")
    return _ps(tmp_path, f"$ErrorActionPreference='Stop'\n& {_quote(script)} -SourceRoot {_quote(candidate)} -InstallRoot {_quote(install)} -EvidencePath {_quote(tmp_path / 'audit.json')} -AllowNoncanonicalLayoutForTest -SkipSignatureValidationForTest\nexit $LASTEXITCODE", env, timeout=180)


def _hashes(paths):
    files = [paths.identity_path, paths.producer_manifest_path, paths.credential_path, paths.registration_report_path, paths.logistics_profile_path, paths.logistics_secret_path, paths.settings_path, paths.data_root / "business.csv", paths.direct_sync_root / "queue/direct_sync_relay.sqlite3", paths.direct_sync_root / "secrets/fixture-current-user.dpapi"]
    return {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in files}


def _prepare_installed(tmp_path, install, candidate, env):
    return _ps(tmp_path, f". {_quote(candidate / 'tools/bootstrap_integrity.ps1')}\n[void](Write-BootstrapIntegrityRecord -Root {_quote(install)} -CodeRoot {_quote(install)})", env)


def _stop_fixture(tmp_path, install, env):
    _ps(tmp_path, f"Get-CimInstance Win32_Process -Filter \"Name='pythonw.exe'\" | Where-Object {{ $_.ExecutablePath -ceq {_quote(install / 'runtime/pythonw.exe')} }} | ForEach-Object {{ Stop-Process -Id $_.ProcessId -ErrorAction SilentlyContinue }}", env)


def _uninstall(tmp_path, install, candidate, env):
    return _ps(tmp_path, f"$ErrorActionPreference='Stop'\n& {_quote(install / 'runtime/python.exe')} -I -B {_quote(install / 'app/main.py')} --remove-current-user-setup --app-root {_quote(install)}\nif ($LASTEXITCODE -ne 0) {{ exit $LASTEXITCODE }}\nfunction Get-ScheduledTask {{ param($TaskName, $TaskPath, $ErrorAction) return @() }}\n& {_quote(candidate / 'INSTALL_THIS_PC.ps1')} -Uninstall -InstallRoot {_quote(install)} -OperatorLocalAppDataRoot {_quote(env['LOCALAPPDATA'])} -AllowNoncanonicalLayoutForTest", env)


@pytest.mark.parametrize("operation", ["upgrade", "repeat", "uninstall-reinstall"])
def test_healthy_same_user_upgrade_preserves_identity_and_business_data(tmp_path, healthy_pair, operation):
    install, candidate, env, paths = _fixture(tmp_path, healthy_pair)
    assert _prepare_installed(tmp_path, install, candidate, env).returncode == 0
    before = _hashes(paths)
    try:
        result = _install(tmp_path, install, candidate, env)
        assert result.returncode == 0, result.stderr[-2200:] + result.stdout[-1000:]
        assert _hashes(paths) == before
        assert json.loads((install / "portable-manifest.json").read_text())["source_commit"] == "2" * 40
        assert json.loads((tmp_path / "audit.json").read_text(encoding="utf-8-sig"))["status"] == "PASS"
        if operation == "uninstall-reinstall":
            removed = _uninstall(tmp_path, install, candidate, env)
            assert removed.returncode == 0, removed.stderr[-2000:]
            assert not install.exists()
            assert _hashes(paths) == before
        if operation != "upgrade":
            result = _install(tmp_path, install, candidate, env)
            assert result.returncode == 0, result.stderr[-2400:] + result.stdout[-1000:]
            assert _hashes(paths) == before
            assert json.loads((tmp_path / "audit.json").read_text(encoding="utf-8-sig"))["status"] == "PASS"
    finally:
        _stop_fixture(tmp_path, install, env)


def _start_quoted_relay(tmp_path, install, env, *, difference=""):
    executable = str(install / "runtime/pythonw.exe")
    script = str(install / "app/main.py")
    expected = subprocess.list2cmdline(
        [executable, "-I", "-B", script, "--label-match-user-relay"]
    )
    arguments = ["-I", "-B", script, "--label-match-user-relay"]
    observed_executable = executable
    if difference == "executable-case":
        observed_executable = executable.upper()
    elif difference == "script-case":
        arguments[2] = script.upper()
    elif difference == "extra-argument":
        arguments.extend(["--app-root", str(install)])
    command = '"' + observed_executable + '" ' + subprocess.list2cmdline(arguments)
    state = Path(env["LM_TRANSITION_NATIVE_STATE"])
    (state / "registry.txt").write_text(expected, encoding="utf-8")
    process = subprocess.Popen(
        command, executable=executable, env=env, stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        creationflags=subprocess.CREATE_NO_WINDOW,
    )
    try:
        deadline = time.monotonic() + 15
        while not (state / "relay-tick.json").exists() and time.monotonic() < deadline:
            assert process.poll() is None
            time.sleep(.05)
        assert (state / "relay-tick.json").exists()
        observed = _ps(tmp_path, f"(Get-CimInstance Win32_Process -Filter 'ProcessId={process.pid}').CommandLine | ConvertTo-Json -Compress", env)
        assert observed.returncode == 0, observed.stderr
        assert json.loads(observed.stdout) == command
        return process
    except BaseException:
        process.terminate()
        process.wait(timeout=10)
        raise


def test_quoted_relay_allows_normal_removal_uninstall_and_reinstall(tmp_path, healthy_pair):
    install, candidate, env, paths = _fixture(tmp_path, healthy_pair)
    assert _prepare_installed(tmp_path, install, candidate, env).returncode == 0
    before = _hashes(paths)
    process = _start_quoted_relay(tmp_path, install, env)
    try:
        # The real canonical transaction validates ownership, removes the quoted
        # running relay, places code, and activates the replacement relay.
        result = _install(tmp_path, install, candidate, env)
        assert result.returncode == 0, result.stderr[-2200:] + result.stdout[-1000:]
        assert process.wait(timeout=10) == 0
        assert json.loads((tmp_path / "audit.json").read_text(encoding="utf-8-sig"))["status"] == "PASS"
        assert _hashes(paths) == before
        removed = _uninstall(tmp_path, install, candidate, env)
        assert removed.returncode == 0, removed.stderr[-2000:]
        assert not install.exists()
        assert _hashes(paths) == before
        result = _install(tmp_path, install, candidate, env)
        assert result.returncode == 0, result.stderr[-2200:] + result.stdout[-1000:]
        assert json.loads((tmp_path / "audit.json").read_text(encoding="utf-8-sig"))["status"] == "PASS"
        assert _hashes(paths) == before
    finally:
        _stop_fixture(tmp_path, install, env)
        process.wait(timeout=10)


@pytest.mark.parametrize("difference", ["executable-case", "script-case", "extra-argument"])
def test_quoted_relay_different_command_rejects_before_removal(tmp_path, healthy_pair, difference):
    install, candidate, env, paths = _fixture(tmp_path, healthy_pair)
    assert _prepare_installed(tmp_path, install, candidate, env).returncode == 0
    before = _hashes(paths)
    code_before = {str(path.relative_to(install)): hashlib.sha256(path.read_bytes()).hexdigest()
                   for path in install.rglob("*") if path.is_file()}
    process = _start_quoted_relay(tmp_path, install, env, difference=difference)
    try:
        result = _install(tmp_path, install, candidate, env)
        assert result.returncode != 0
        assert "Healthy lifecycle relay belongs to another owner or command." in result.stderr
        assert process.poll() is None
        assert _hashes(paths) == before
        assert not paths.removal_report_path.exists()
        assert not (Path(env["KMTECH_LABEL_WRITER_CONTROL_ROOT"]) / "active.json").exists()
        assert code_before == {str(path.relative_to(install)): hashlib.sha256(path.read_bytes()).hexdigest()
                               for path in install.rglob("*") if path.is_file()}
    finally:
        _stop_fixture(tmp_path, install, env)
        process.wait(timeout=10)


@pytest.mark.parametrize("space,expected_quoted,actual_quoted,different_arguments,accepted", [
    (False, False, True, False, True),
    (False, True, False, False, True),
    (True, True, True, False, True),
    (True, True, False, False, False),
    (False, True, True, True, False),
])
def test_rollback_relay_readback_preserves_binding_with_executable_quotes(
    tmp_path, space, expected_quoted, actual_quoted, different_arguments, accepted,
):
    env = _environment(tmp_path)
    executable = str(tmp_path / ("space root" if space else "root") / "pythonw.exe")
    arguments = " -I -B app.py --label-match-user-relay"
    expected = ('"' + executable + '"' if expected_quoted else executable) + arguments
    actual = ('"' + executable + '"' if actual_quoted else executable) + arguments
    if different_arguments:
        actual += " --different"
    code = _definitions() + f'''
function Relays {{
    return @([pscustomobject]@{{ExecutablePath={_quote(executable)}; CommandLine={_quote(actual)}}})
}}
$expected = @([pscustomobject]@{{ExecutablePath={_quote(executable)}; CommandLine={_quote(expected)}}})
Assert-RollbackRelayPreimage -ExpectedRelays $expected | Out-Null
'''
    result = _ps(tmp_path, code, env)
    if accepted:
        assert result.returncode == 0, result.stderr
    else:
        assert result.returncode != 0
        assert "rollback relay executable/command readback failed" in result.stderr


@pytest.mark.parametrize("conflict", ["foreign-user", "credential-owner", "partial", "quarantined", "marker", "foreign-autostart", "tampered-code"])
def test_unhealthy_lifecycle_rejects_before_mutation(tmp_path, healthy_pair, conflict):
    install, candidate, env, paths = _fixture(tmp_path, healthy_pair)
    assert _prepare_installed(tmp_path, install, candidate, env).returncode == 0
    if conflict == "foreign-user":
        identity = json.loads(paths.identity_path.read_text())
        identity["producer_install_id"] = "another-user-install"
        _json(paths.identity_path, identity)
        manifest = json.loads(paths.producer_manifest_path.read_text())
        manifest["pc_identity"]["producer_install_id"] = identity["producer_install_id"]
        _json(paths.producer_manifest_path, manifest)
        registration = json.loads(paths.registration_report_path.read_text())
        registration["manifest_hash"] = manifest_hash(manifest)
        _json(paths.registration_report_path, registration)
    elif conflict == "credential-owner":
        value = json.loads(paths.credential_path.read_text())
        value["secret_ref"] = "another-owner"
        _json(paths.credential_path, value)
    elif conflict == "partial":
        paths.registration_report_path.unlink()
    elif conflict == "quarantined":
        database = paths.direct_sync_root / "queue/direct_sync_relay.sqlite3"
        with sqlite3.connect(database) as connection:
            connection.execute("UPDATE direct_sync_runtime_authority SET status='OPERATOR_REVIEW', last_error_code='EXACT_CLONE_RUNTIME_CONFLICT'")
    elif conflict == "marker":
        marker = paths.direct_sync_root / "control/label_match_user_relay.stop.json"
        _json(marker, {"schema_version": "label-match-user-relay-stop-v1", "request_id": "a" * 32, "requested_at": "2026-09-05T00:00:00Z"})
    elif conflict == "foreign-autostart":
        (Path(env["LM_TRANSITION_NATIVE_STATE"]) / "registry.txt").write_text("foreign-command")
    elif conflict == "tampered-code":
        (install / "app/foreign.py").write_text("raise SystemExit(1)")
    original = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in install.rglob("*") if path.is_file()}
    result = _install(tmp_path, install, candidate, env)
    assert result.returncode != 0, result.stdout[-1000:]
    assert not (Path(env["KMTECH_LABEL_WRITER_CONTROL_ROOT"]) / "active.json").exists()
    assert not (paths.direct_sync_root / "status/current_user_removal.json").exists()
    assert original == {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in install.rglob("*") if path.is_file()}


@pytest.mark.parametrize("quoted", [False, True])
def test_healthy_upgrade_failure_restores_exact_code_identity_and_running_relay(tmp_path, healthy_pair, quoted):
    install, candidate, env, paths = _fixture(tmp_path, healthy_pair)
    assert _prepare_installed(tmp_path, install, candidate, env).returncode == 0
    before = _hashes(paths)
    old_files = {str(path.relative_to(install)): hashlib.sha256(path.read_bytes()).hexdigest() for path in install.rglob("*") if path.is_file() and path.name != "bootstrap-integrity.json"}
    command = [str(install / "runtime/pythonw.exe"), "-I", "-B", str(install / "app/main.py"), "--label-match-user-relay"]
    (Path(env["LM_TRANSITION_NATIVE_STATE"]) / "registry.txt").write_text(subprocess.list2cmdline(command))
    process = (_start_quoted_relay(tmp_path, install, env) if quoted else
               subprocess.Popen(command, env=env, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
    try:
        state = Path(env["LM_TRANSITION_NATIVE_STATE"])
        deadline = time.monotonic() + 15
        while not (state / "relay-tick.json").exists() and time.monotonic() < deadline:
            assert process.poll() is None
            time.sleep(.05)
        assert (state / "relay-tick.json").exists()
        result = _install(tmp_path, install, candidate, env, failure="onboarding")
        assert result.returncode != 0
        audit = json.loads((tmp_path / "audit.json").read_text(encoding="utf-8-sig"))
        assert audit["status"] == "FAILED_ROLLED_BACK", result.stderr[-2000:]
        assert audit["rollback"]["code_placement"] == "RESTORED_PREIMAGE"
        assert audit["rollback"]["runtime_restored"] is True
        assert _hashes(paths) == before
        assert old_files == {str(path.relative_to(install)): hashlib.sha256(path.read_bytes()).hexdigest() for path in install.rglob("*") if path.is_file() and path.name != "bootstrap-integrity.json"}
        assert (state / "registry.txt").read_text() == subprocess.list2cmdline(command)
        assert not (paths.direct_sync_root / "control/label_match_user_relay.stop.json").exists()
        assert not (Path(env["KMTECH_LABEL_WRITER_CONTROL_ROOT"]) / "active.json").exists()
        assert int((state / "relay.pid").read_text()) != process.pid
        restored_pid = int((state / "relay.pid").read_text())
        observed = _ps(tmp_path, f"(Get-CimInstance Win32_Process -Filter 'ProcessId={restored_pid}').CommandLine | ConvertTo-Json -Compress", env)
        assert observed.returncode == 0
        assert json.loads(observed.stdout) == (process.args if quoted else subprocess.list2cmdline(command))
    finally:
        _stop_fixture(tmp_path, install, env)
        process.wait(timeout=10)


@pytest.mark.parametrize("foreign", ["principal", "action"])
def test_healthy_admission_rejects_foreign_task_ownership(tmp_path, healthy_pair, foreign):
    install, candidate, env, paths = _fixture(tmp_path, healthy_pair)
    code = _definitions() + f'''
$defaultDirectSyncRoot = {_quote(paths.direct_sync_root)}
$sid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$task = [pscustomobject]@{{Principal=[pscustomobject]@{{UserId=$(if ({_quote(foreign)} -ceq 'principal') {{ 'S-1-5-18' }} else {{ $sid }}); RunLevel='Limited'; LogonType='Interactive'}}; Actions=@([pscustomobject]@{{Execute='C:\\foreign\\python.exe'; Arguments='foreign'; WorkingDirectory='C:\\foreign'}})}}
Assert-HealthyLifecycleOwnership ([pscustomobject]@{{exists=$false}}) @() @($task) {_quote(install)}
'''
    result = _ps(tmp_path, code, env)
    assert result.returncode != 0
    assert "Healthy lifecycle task belongs to another" in result.stderr


@pytest.mark.parametrize("foreign_log", [False, True])
def test_healthy_task_admission_matches_normal_creator_log_path(
    tmp_path, monkeypatch, foreign_log,
):
    import current_user_onboarding
    import current_user_scheduled_task

    # Exercise the real creator and real read-only PowerShell admission function;
    # no scheduled task, runtime, profile credential or installed tree is changed.
    install = tmp_path / "canonical path" / "current"
    (install / "runtime").mkdir(parents=True)
    (install / "app").mkdir()
    (install / "runtime" / "python.exe").write_bytes(b"never executed")
    (install / "app" / "main.py").write_text("raise AssertionError('never run')\n")
    env = _environment(tmp_path)
    paths = resolve_current_user_onboarding_paths(install, environ=env)
    monkeypatch.setattr(current_user_scheduled_task, "CANONICAL_ROOT", install)
    monkeypatch.setattr(current_user_onboarding, "resolve_current_user_onboarding_paths",
                        lambda _root: paths)
    spec = current_user_scheduled_task.build_current_user_task_spec(install)
    if foreign_log:
        spec["arguments"] = spec["arguments"].replace(
            subprocess.list2cmdline([str(paths.logs_dir / "scheduled_direct_sync_relay.jsonl")]),
            subprocess.list2cmdline([str(tmp_path / "foreign log.jsonl")]),
        )
    spec_path = tmp_path / "task-spec.json"
    _json(spec_path, spec)
    code = _definitions() + f'''
$defaultDirectSyncRoot = {_quote(paths.direct_sync_root)}
$spec = Get-Content -LiteralPath {_quote(spec_path)} -Raw | ConvertFrom-Json
$sid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$task = [pscustomobject]@{{Principal=[pscustomobject]@{{UserId=$sid; RunLevel='Limited'; LogonType='Interactive'}}; Actions=@([pscustomobject]@{{Execute=$spec.execute; Arguments=$spec.arguments; WorkingDirectory=$spec.working_directory}})}}
Assert-HealthyLifecycleOwnership ([pscustomobject]@{{exists=$false}}) @() @($task) {_quote(install)}
Write-Output 'OWNERSHIP_PASS'
'''
    result = _ps(tmp_path, code, env)
    if foreign_log:
        assert result.returncode != 0
        assert "Healthy lifecycle task belongs to another command" in result.stderr
    else:
        assert result.returncode == 0, result.stderr
        assert "OWNERSHIP_PASS" in result.stdout


def test_normal_marker_release_requires_active_writer_fence(tmp_path, healthy_pair):
    install, candidate, env, paths = _fixture(tmp_path, healthy_pair)
    code = _definitions() + f'''
$healthy = HealthyLifecycle {_quote(candidate)} {_quote(install)}
HealthyLifecycle {_quote(candidate)} {_quote(install)} 'release' ([string]$healthy.state_sha256)
'''
    result = _ps(tmp_path, code, env)
    assert result.returncode != 0
    assert "requires an active replacement fence" in result.stderr


@pytest.mark.parametrize("failure", ["onboarding", "placement"])
def test_healthy_reinstall_failure_restores_code_absence_and_original_stop_marker(tmp_path, healthy_pair, failure):
    install, candidate, env, paths = _fixture(tmp_path, healthy_pair)
    assert _prepare_installed(tmp_path, install, candidate, env).returncode == 0
    removed = _uninstall(tmp_path, install, candidate, env)
    assert removed.returncode == 0, removed.stderr[-1000:]
    assert not install.exists()
    before = _hashes(paths)
    marker = paths.direct_sync_root / "control/label_match_user_relay.stop.json"
    marker_before = marker.read_bytes()
    removal_before = paths.removal_report_path.read_bytes()
    result = _install(tmp_path, install, candidate, env, failure=failure)
    assert result.returncode != 0
    audit = json.loads((tmp_path / "audit.json").read_text(encoding="utf-8-sig"))
    assert audit["status"] == "FAILED_ROLLED_BACK", result.stderr[-2000:]
    assert audit["rollback"]["code_placement"] == "RESTORED_ABSENCE"
    assert audit["rollback"]["code_restored"] is True
    assert audit["rollback"]["runtime_restored"] is True
    assert not install.exists()
    assert _hashes(paths) == before
    assert marker.read_bytes() == marker_before
    assert paths.removal_report_path.read_bytes() == removal_before
    assert not (Path(env["KMTECH_LABEL_WRITER_CONTROL_ROOT"]) / "active.json").exists()


def test_recovered_identity_without_code_or_removal_marker_installs(tmp_path, healthy_pair):
    install, candidate, env, paths = _fixture(tmp_path, healthy_pair)
    assert install.resolve().is_relative_to(tmp_path.resolve())
    shutil.rmtree(install)
    before = _hashes(paths)
    assert not paths.removal_report_path.exists()
    try:
        result = _install(tmp_path, install, candidate, env)
        assert result.returncode == 0, result.stderr[-2200:]
        audit = json.loads((tmp_path / "audit.json").read_text(encoding="utf-8-sig"))
        assert audit["status"] == "PASS"
        assert audit["onboarding"]["status"] == "READY"
        assert _hashes(paths) == before
        assert not paths.removal_report_path.exists()
        assert not (paths.control_dir / "label_match_user_relay.stop.json").exists()
    finally:
        _stop_fixture(tmp_path, install, env)


def test_recovered_fresh_failure_restores_absent_code_and_removal_report(tmp_path, healthy_pair):
    install, candidate, env, paths = _fixture(tmp_path, healthy_pair)
    assert install.resolve().is_relative_to(tmp_path.resolve())
    shutil.rmtree(install)
    before = _hashes(paths)
    result = _install(tmp_path, install, candidate, env, failure="onboarding")
    assert result.returncode != 0
    audit = json.loads((tmp_path / "audit.json").read_text(encoding="utf-8-sig"))
    assert audit["status"] == "FAILED_ROLLED_BACK", result.stderr[-2200:]
    assert not install.exists()
    assert not paths.removal_report_path.exists()
    assert not (paths.control_dir / "label_match_user_relay.stop.json").exists()
    assert _hashes(paths) == before
    try:
        retry = _install(tmp_path, install, candidate, env)
        assert retry.returncode == 0, retry.stderr[-2200:]
        assert _hashes(paths) == before
    finally:
        _stop_fixture(tmp_path, install, env)


@pytest.mark.parametrize("residue", ["missing-marker", "autostart"])
def test_recovered_no_code_rejects_removal_or_persistence_residue(tmp_path, healthy_pair, residue):
    install, candidate, env, paths = _fixture(tmp_path, healthy_pair)
    assert install.resolve().is_relative_to(tmp_path.resolve())
    shutil.rmtree(install)
    if residue == "missing-marker":
        _json(paths.removal_report_path, {"status": "PASS_DATA_PRESERVED"})
    else:
        command = subprocess.list2cmdline([str(install / "runtime/pythonw.exe"), "-I", "-B", str(install / "app/main.py"), "--label-match-user-relay"])
        (Path(env["LM_TRANSITION_NATIVE_STATE"]) / "registry.txt").write_text(command)
    before = _hashes(paths)
    result = _install(tmp_path, install, candidate, env)
    assert result.returncode != 0
    assert "Healthy reinstall requires completed same-user removal and absent persistence" in result.stderr
    assert not install.exists()
    assert not (Path(env["KMTECH_LABEL_WRITER_CONTROL_ROOT"]) / "active.json").exists()
    assert _hashes(paths) == before


def _fresh_fixture(tmp_path, healthy_pair, error):
    _old, candidate = healthy_pair
    install = tmp_path / "canonical/current"
    env = _environment(tmp_path)
    for name in (
        "LABEL_MATCH_DIRECT_SYNC_ROOT",
        "LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT",
        "LABEL_MATCH_SAVE_DIR",
        "LABEL_MATCH_SETTINGS_PATH",
        "KM_LOGISTICS_PROFILE_PATH",
    ):
        env.pop(name, None)
    env.update(
        LM_HEALTHY_LIFECYCLE_FIXTURE="1", LM_HEALTHY_INSTALL_ROOT=str(install),
        LM_TRANSITION_NATIVE_ADAPTER=str(Path(__file__).with_name("_fresh_install_native.py")),
        LM_FRESH_ENROLLMENT_ERROR=error,
    )
    paths = resolve_current_user_onboarding_paths(install, environ=env)
    env["LABEL_MATCH_DIRECT_SYNC_ROOT"] = str(paths.direct_sync_root)
    return install, candidate, env, paths


def test_fresh_identity_conflict_retains_code_and_reports_recovery_required(tmp_path, healthy_pair):
    install, candidate, env, paths = _fresh_fixture(tmp_path, healthy_pair, "producer_identity_conflict")
    result = _install(tmp_path, install, candidate, env)
    assert result.returncode == 0, result.stderr[-2200:]
    registration = json.loads(paths.registration_report_path.read_text(encoding="utf-8"))
    assert registration["server_error_code"] == "producer_identity_conflict"
    assert registration["server_http_status"] == 409
    audit = json.loads((tmp_path / "audit.json").read_text(encoding="utf-8-sig"))
    assert audit["status"] == "RECOVERY_REQUIRED"
    assert audit["code_state"] == "PRESENT"
    assert audit["rollback"]["applied"] is False
    assert audit["onboarding"]["status"] == "RECOVERY_REQUIRED"
    assert "normal enrollment token and a separate administrator authorization" in result.stdout
    assert (install / "app/tools/register_label_match_worker_pc.py").is_file()
    assert not paths.identity_path.exists()
    assert not paths.credential_path.exists()
    assert not paths.logistics_profile_path.exists()
    assert not (Path(env["LM_TRANSITION_NATIVE_STATE"]) / "registry.txt").exists()
    assert not (Path(env["KMTECH_LABEL_WRITER_CONTROL_ROOT"]) / "active.json").exists()
    assert not paths.removal_report_path.exists()


def test_fresh_genuine_enrollment_failure_restores_actual_code_absence(tmp_path, healthy_pair):
    install, candidate, env, paths = _fresh_fixture(tmp_path, healthy_pair, "enrollment_token_invalid")
    result = _install(tmp_path, install, candidate, env)
    assert result.returncode != 0
    registration = json.loads(paths.registration_report_path.read_text(encoding="utf-8"))
    assert registration["server_error_code"] == "enrollment_token_invalid"
    assert registration["server_http_status"] == 401
    audit = json.loads((tmp_path / "audit.json").read_text(encoding="utf-8-sig"))
    assert audit["status"] == "FAILED_ROLLED_BACK", result.stderr[-2200:]
    assert not install.exists()
    assert audit["code_state"] == "ABSENT"
    assert audit["rollback"]["code_restored"] is True
    assert audit["rollback"]["code_placement"] == "RESTORED_ABSENCE"
    assert not (Path(env["KMTECH_LABEL_WRITER_CONTROL_ROOT"]) / "active.json").exists()
    assert not paths.removal_report_path.exists()

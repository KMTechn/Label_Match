"""Canonical version-slot onboarding uses the stable task identity."""
import json
import os
from pathlib import Path
import threading
import sqlite3
from types import SimpleNamespace

import pytest

from tests.test_current_user_onboarding import (
    isolated_data_generation_release,
    _ready_state, _profile_loader, _credential_loader, _autostart,
    _relay_start, _legacy_task_quiescent,
)
from current_user_onboarding import onboard_current_user, resolve_current_user_onboarding_paths
import current_user_scheduled_task as tasks


@pytest.fixture(autouse=True)
def isolated_onboarding_environment(monkeypatch):
    import current_user_onboarding as onboarding
    for name in (onboarding.LABEL_MATCH_SETTINGS_PATH_ENV,
                 onboarding.LABEL_MATCH_DIRECT_SYNC_ROOT_ENV,
                 onboarding.LEGACY_DIRECT_SYNC_ROOT_ENV,
                 onboarding.LABEL_MATCH_LOGISTICS_PROFILE_PATH_ENV,
                 onboarding.REQUIRED_ENV):
        monkeypatch.setenv(name, "")


@pytest.fixture
def canonical_delegation(tmp_path, monkeypatch):
    import writer_session_fence as fence
    from tests.test_writer_session_fence import _payload, _write_active
    token = "isolated-sub412-delegation-" + "6" * 64
    payload = _payload(delegated_sources=["current_user_onboarding", "data_generation_record",
        "package_outbox", "operation_lease", "scheduled_task_remove", "worker_enrollment_network"], token=token)
    control = tmp_path / "canonical-control"
    _write_active(control, payload)
    for name, value in ((fence.TEST_MODE_ENV, "1"), (fence.CONTROL_ROOT_OVERRIDE_ENV, str(control)),
        (fence.DELEGATION_TOKEN_ENV, token), (fence.DELEGATION_SESSION_ENV, payload["session_id"]),
        (fence.DELEGATION_ATTEMPT_ENV, payload["attempt_id"]),
        (fence.DELEGATION_TRANSACTION_ENV, payload["replacement_transaction_id"])):
        monkeypatch.setenv(name, value)
    ready, release = threading.Event(), threading.Event()
    ownership = []

    def hold_authority():
        authority = fence._acquire_named_mutex(payload["session_authority_mutex_name"], 0)
        ownership.append(authority is not None)
        ready.set()
        if authority is not None:
            try:
                release.wait()
            finally:
                authority.release()

    owner = threading.Thread(target=hold_authority)
    owner.start()
    assert ready.wait(10) and ownership == [True]
    try:
        yield control
    finally:
        release.set()
        owner.join(10)
        assert not owner.is_alive()


def test_version_slot_onboarding_migrates_fixed_current_task(tmp_path, monkeypatch):
    import auto_update
    fixed = tmp_path / "install/current"
    active = tmp_path / "install/versions" / ("a" * 40)
    fixed.mkdir(parents=True)
    active.mkdir(parents=True)
    (fixed.parent / "update-control").mkdir()
    environment = {"LOCALAPPDATA": str(tmp_path / "local"),
                   "LABEL_MATCH_SAVE_DIR": str(tmp_path / "data")}
    monkeypatch.setattr(auto_update, "resolve_lifecycle_context", lambda root, **kw: {
        "active_code_root": active, "fixed_root": str(fixed)})
    monkeypatch.setattr(tasks, "CANONICAL_ROOT", fixed)
    calls = []

    def register(paths):
        _ready_state(paths)
        return 0

    def runner(*args, **kwargs):
        calls.append(True)
        return SimpleNamespace(returncode=0, stderr="", stdout=json.dumps({
            "schema": tasks.TASK_CONTRACT_VERSION, "status": "ABSENT",
            "manual_start": False, "process_or_task_stopped": False}))

    def migrate(root):
        spec = tasks.build_current_user_task_spec(root)
        assert str(active) not in json.dumps(spec)
        return tasks.remove_current_user_scheduled_task(root, runner=runner)

    result = onboard_current_user(fixed, environ=environment,
        require_bootstrap_integrity=False, registration_runner=register,
        profile_loader=_profile_loader, credential_loader=_credential_loader,
        autostart_installer=_autostart, scheduled_task_remover=migrate,
        legacy_task_quiescence_reader=_legacy_task_quiescent,
        relay_launcher=_relay_start)
    assert result["status"] == "READY"
    assert calls == [True]
    assert resolve_current_user_onboarding_paths(active, environ=environment).ledger_path.is_file()


@pytest.mark.parametrize("failure", (None, "migration"))
@pytest.mark.parametrize("engine", ("powershell.exe", "pwsh.exe"))
def test_canonical_preparation_never_opens_business_data(tmp_path, monkeypatch,
                                                       canonical_delegation, failure, engine):
    import auto_update
    from current_user_onboarding import CurrentUserOnboardingError
    fixed = tmp_path / "current"
    active = tmp_path / "versions" / ("a" * 40)
    fixed.mkdir(parents=True)
    active.mkdir(parents=True)
    (fixed.parent / "update-control").mkdir()
    monkeypatch.setattr(auto_update, "resolve_lifecycle_context", lambda root, **kw: {
        "active_code_root": active, "fixed_root": str(fixed)})
    monkeypatch.setattr(tasks, "CANONICAL_ROOT", fixed)
    paths = resolve_current_user_onboarding_paths(active)

    def register(selected):
        _ready_state(selected)
        return 0

    def migrate(root):
        # The existing task identity guard remains mandatory.
        tasks.build_current_user_task_spec(fixed)
        if failure == "migration":
            raise tasks.CurrentUserScheduledTaskError("isolated migration failure")
        return {"status": "ABSENT"}

    kwargs = dict(require_bootstrap_integrity=False, registration_runner=register,
        profile_loader=_profile_loader, credential_loader=_credential_loader,
        autostart_installer=_autostart, scheduled_task_remover=migrate,
        legacy_task_quiescence_reader=_legacy_task_quiescent, relay_launcher=_relay_start,
        defer_canonical_relay_start=True)
    if failure:
        with pytest.raises(CurrentUserOnboardingError):
            onboard_current_user(fixed, **kwargs)
    else:
        result = onboard_current_user(fixed, **kwargs)
        assert result["status"] == "READY"
        assert result["ledger_status"] == "DEFERRED_CANONICAL_INSTALL"
        assert result["settings"]["status"] == "DEFERRED_CANONICAL_INSTALL"
    assert not paths.ledger_path.exists()
    assert not paths.settings_path.exists()
    assert not list(tmp_path.rglob("data-generation.json"))
    assert not (canonical_delegation / "data-generation").exists()
    if failure:
        from label_data_generation import marker_paths
        from protected_admin import current_process_sid
        from tests.test_install_recovery import test_exact_preimages_and_safe_resume_with_real_generation
        from tests.test_round5_startup_rollback import test_actual_startup_restore_refreshes_only_live_bound_owner
        generation = marker_paths((paths.data_root, paths.ledger_path.parent, paths.direct_sync_root,
            paths.queue_dir, paths.logistics_profile_path.parent), current_process_sid())
        test_exact_preimages_and_safe_resume_with_real_generation(tmp_path, engine, "fresh_strict",
            generation_marker_paths=[str(p) for p in generation], version_root=active.parent)
        assert not fixed.exists() and not active.parent.exists()
        restored = tmp_path / "wrapper-recovery"
        restored.mkdir()
        wrapper = os.environ.get("LM_B1_WRAPPER")
        assert wrapper, "pinned legacy wrapper is required for N4 folder restoration"
        monkeypatch.setenv("LM_ROUND5_WRAPPER_SOURCE", wrapper)
        test_actual_startup_restore_refreshes_only_live_bound_owner(restored, engine, "valid")
        assert (restored / "old-folder/old-file.dat").is_file()
        assert not (restored / "old-folder-preserved").exists()
        assert all(not p.exists() for p in generation)


@pytest.mark.parametrize("engine", ("powershell.exe", "pwsh.exe"))
@pytest.mark.parametrize("first_open_fails", (False, True))
def test_confirmed_first_open_is_idempotent_or_preserves_candidate(tmp_path, monkeypatch,
        canonical_delegation, engine, first_open_fails):
    import auto_update
    import current_user_onboarding as onboarding
    from tests.test_shared_ancestor_protection import run_functions
    fixed = tmp_path / "current"
    active = tmp_path / "versions" / ("a" * 40)
    fixed.mkdir()
    active.mkdir(parents=True)
    (tmp_path / "update-control").mkdir()
    (active / "candidate.dat").write_bytes(b"candidate-original")
    legacy = tmp_path / "legacy-retained.dat"
    legacy.write_bytes(b"old-business-original")
    monkeypatch.setattr(auto_update, "resolve_lifecycle_context", lambda root, **kw: {
        "active_code_root": active, "fixed_root": str(fixed)})
    monkeypatch.setattr(tasks, "CANONICAL_ROOT", fixed)
    paths = resolve_current_user_onboarding_paths(active)

    def register(selected):
        _ready_state(selected)
        import direct_sync_runtime as runtime
        from direct_sync_push import manifest_hash
        manifest = json.loads(selected.producer_manifest_path.read_text(encoding="utf-8"))
        manifest["streams"] = [{"producer_role": runtime.DEFAULT_PRODUCER_ROLE,
            "stream_name": runtime.DEFAULT_STREAM_NAME, "source_system": runtime.DEFAULT_SOURCE_SYSTEM,
            "source_transport": runtime.DEFAULT_SOURCE_TRANSPORT}]
        selected.producer_manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        report = json.loads(selected.registration_report_path.read_text(encoding="utf-8"))
        report["manifest_hash"] = manifest_hash(manifest)
        selected.registration_report_path.write_text(json.dumps(report), encoding="utf-8")
        return 0

    kwargs = dict(require_bootstrap_integrity=False, registration_runner=register,
        profile_loader=_profile_loader, credential_loader=_credential_loader,
        autostart_installer=lambda root: {"status": "PASS", "command": "fixed-bootstrap"},
        scheduled_task_remover=lambda root: {"status": "ABSENT"},
        legacy_task_quiescence_reader=_legacy_task_quiescent, relay_launcher=_relay_start)
    onboard_current_user(fixed, defer_canonical_relay_start=True, **kwargs)
    result = run_functions(tmp_path, Path(onboarding.__file__).parent / "INSTALL_CANONICAL_PORTABLE.ps1",
        ["Confirm-LabelCanonicalPlacement", "Get-LabelLaunchFailure"], r'''
Set-StrictMode -Version Latest
$Script:LabelPlacementSession=$null
$path=$env:LM_412_ONBOARDING
$prepared=Get-Content -LiteralPath $path -Raw|ConvertFrom-Json
function Snapshot { [pscustomobject]@{exists=$true;data='fixed-bootstrap'} }
function Stop-LabelWriterFence {
 param($ControlRoot,$SessionId,$AttemptId,$ReplacementTransactionId,$TimeoutMilliseconds)
 Remove-Item -LiteralPath (Join-Path $ControlRoot 'active.json')
}
Confirm-LabelCanonicalPlacement $prepared 'fixed-bootstrap' $path ([DateTime]::UtcNow.AddMinutes(-1)) '' $env:LM_412_CONTROL 's' 'a' 't'
if($Script:LabelCanonicalConfirmation -cne 'CONFIRMED'){throw 'T1 did not commit'}
if($env:LM_412_OPEN_FAIL -ceq '1'){
 $install=Join-Path (Get-Location).Path 'current'
 $audit=[ordered]@{rollback=[ordered]@{}};$auditPath=Join-Path (Get-Location).Path 'post-confirmation.json';$EvidencePath=''
 function Save {param($Path,$Value) [IO.File]::WriteAllText($Path,($Value|ConvertTo-Json -Depth 20))}
 function Invoke-LabelCanonicalRollback {throw 'POST_CONFIRMATION_ROLLBACK_FORBIDDEN'}
 $clauses=@($ast.FindAll({param($n)$n -is [Management.Automation.Language.CatchClauseAst] -and $n.Body.Extent.Text.Contains('$forwardFailure = $_')},$true))
 if($clauses.Count -ne 1){throw 'Forward failure handler is ambiguous'}
 $body=$clauses[0].Body.Extent.Text
 try{throw [IO.IOException]::new('isolated first-open failure')}catch{try{. ([scriptblock]::Create($body.Substring(1,$body.Length-2)))}catch{}}
 if($audit.status -cne 'RECOVERY_REQUIRED' -or $audit.reason_code -cne 'POST_CONFIRMATION_FAILED' -or
    $audit.code_state -cne 'PRESENT' -or $audit.rollback.applied -ne $false -or -not $audit.recovery_guidance){throw 'First-open recovery state differs'}
 if(-not $audit.recovery_command.Contains('--onboard-current-user --app-root') -or
    -not $audit.recovery_command.Contains((Join-Path $install 'runtime\python.exe')) -or
    -not $audit.recovery_command.Contains((Join-Path $install 'app\main.py'))){throw 'Same-version support command is absent'}
}
'CONFIRMED'
''', engine=engine, values={"LM_412_ONBOARDING": str(paths.onboarding_report_path),
                           "LM_412_CONTROL": str(canonical_delegation),
                           "LM_412_OPEN_FAIL": "1" if first_open_fails else "0"})
    assert result.returncode == 0, result.stderr
    if first_open_fails:
        assert "install_status=RECOVERY_REQUIRED" in result.stdout
        assert "audit_path=" in result.stdout and "recovery_command=" in result.stdout
        def fail_open(path):
            raise OSError("isolated first open failure")
        with pytest.raises(onboarding.CurrentUserOnboardingError) as caught:
            onboard_current_user(fixed, ledger_factory=fail_open, **kwargs)
        failure = json.loads(paths.onboarding_report_path.read_text(encoding="utf-8"))
        assert failure["status"] == "FAILED"
        assert failure["cause_code"] == "BUSINESS_DATA_OPEN_FAILED"
        assert failure["recovery_command"] == caught.value.recovery_command
        assert "--onboard-current-user --app-root" in caught.value.recovery_command
        assert isinstance(caught.value.__cause__, OSError)
        assert failure["failure_traceback"]
        (tmp_path / "first-open-failure.json").write_text(json.dumps(failure, ensure_ascii=False), encoding="utf-8")
        # This is the actual later GUI failure path, after the installer has
        # already returned PASS. Replace only Tk/window APIs; create no window.
        import Label_Match as app
        dialogs = []
        parent = SimpleNamespace(**{name: (lambda *a, **kw: None) for name in
            ("withdraw", "title", "geometry", "attributes", "deiconify", "lift",
             "update", "focus_force", "destroy")})
        monkeypatch.setattr(app.tk, "Tk", lambda: parent)
        monkeypatch.setattr(app.messagebox, "showerror", lambda title, message, **kw: dialogs.append(message))
        monkeypatch.setattr(app, "dispatch_product_mode", lambda argv: None)
        monkeypatch.setattr(app, "verify_factory_contract_startup", lambda: None)
        monkeypatch.setattr(app, "_label_match_startup_trace", lambda *a, **kw: None)
        monkeypatch.setattr(app, "_label_match_runtime_app_root", lambda: fixed)
        monkeypatch.setattr(app, "_first_run_onboarding_enabled", lambda: True)
        def failed_onboarding(*a, **kw):
            raise caught.value
        monkeypatch.setattr(app, "onboard_current_user", failed_onboarding)
        assert app.main([]) == app.ONBOARDING_EXIT_CODE
        assert len(dialogs) == 1 and "업무 자료를 열지 못해" in dialogs[0]
        assert str(paths.onboarding_report_path) in dialogs[0]
        assert caught.value.recovery_command in dialogs[0]
        # The published command uses this existing CLI mode. Exercise its real
        # parser and normal onboarding after the isolated disk failure is fixed.
        original_onboard = onboarding.onboard_current_user
        monkeypatch.setattr(onboarding, "onboard_current_user", lambda app_root, **ignored:
            original_onboard(app_root, **kwargs))
        assert onboarding.onboarding_main(["--app-root", str(fixed)]) == 0
        assert json.loads(paths.onboarding_report_path.read_text(encoding="utf-8"))["ledger_status"] == "READY"
    else:
        # T1 launches relay before the GUI. Keep its actual entry, cycle, queue
        # and readiness writer; adapt process launch and network authority only.
        import sys
        import user_relay
        import direct_sync_runtime as runtime
        import tools.direct_sync_relay_runner as runner
        import logistics_runtime_profile
        from direct_sync_push import ProducerCredentials
        opened = []
        real_relay_generation = user_relay.record_user_data_open
        monkeypatch.setattr(user_relay, "record_user_data_open", lambda *args, **kw:
            (opened.append("relay_generation"), real_relay_generation(*args, **kw))[1])
        monkeypatch.setattr(auto_update, "startup_gate", lambda *a, **kw: None)
        monkeypatch.setattr(auto_update, "trial_stop_requested", lambda *a: False)
        monkeypatch.setattr(auto_update, "relay_ready_metadata", lambda *a: {})
        monkeypatch.setattr(auto_update, "prepare_feed_packet", lambda *a: {"status": "OFF"})
        monkeypatch.setattr(logistics_runtime_profile, "load_logistics_runtime_profile",
            lambda **kw: _profile_loader(paths.logistics_profile_path))
        credential_input = runtime.CredentialInput(paths.credential_path)
        monkeypatch.setattr(runtime, "prepare_credentials_from_json", lambda *a, **kw: credential_input)
        monkeypatch.setattr(runtime, "load_credentials_from_json", lambda *a, **kw:
            ProducerCredentials("label-match-user-1", "isolated-key", "isolated-value", "https://isolated.invalid"))
        monkeypatch.setattr(runtime, "ensure_runtime_authority", lambda **kw:
            SimpleNamespace(receipt={}, operator_review=False, error_code=""))
        monkeypatch.setattr(user_relay, "_application_command", lambda *a, **kw:
            [sys.executable, "-B", "isolated-host.py"])
        readiness = []
        real_status_write = user_relay._write_json_atomic
        def relay_status_write(path, value):
            readiness.append(dict(value))
            return real_status_write(path, value)
        monkeypatch.setattr(user_relay, "_write_json_atomic", relay_status_write)
        def relay_child(command, timeout_seconds):
            assert not paths.ledger_path.exists() and not paths.settings_path.exists()
            result = runner.main(command[command.index(user_relay.DIRECT_SYNC_RELAY_MODE) + 1:],
                credential_input=credential_input)
            return {"status": "PASS" if result == 0 else "FAIL", "returncode": result}
        monkeypatch.setattr(user_relay, "_run_command", relay_child)
        assert user_relay.main(["--app-root", str(active), "--once"], credential_input=credential_input) == 0
        relay_report = json.loads(user_relay.user_relay_status_path(paths.direct_sync_root).read_text(encoding="utf-8"))
        assert readiness[0]["status"] == "RUNNING"
        assert readiness[0]["last_cycle"]["process_status"] == "PASS"
        assert relay_report["status"] == "COMPLETED" and relay_report["persistent_retry"] is True
        assert not paths.ledger_path.exists() and not paths.settings_path.exists()
        assert opened == ["relay_generation"] and list(tmp_path.rglob("data-generation.json"))
        real_app_generation = onboarding.record_user_data_open
        real_ledger, real_settings = onboarding._create_ledger, onboarding._ensure_user_settings
        monkeypatch.setattr(onboarding, "record_user_data_open", lambda *a, **kw:
            (opened.append("app_generation"), real_app_generation(*a, **kw))[1])
        def ledger(path):
            assert list(tmp_path.rglob("data-generation.json")) and not paths.settings_path.exists()
            opened.append("ledger")
            return real_ledger(path)
        def settings(selected):
            assert paths.ledger_path.is_file()
            opened.append("settings")
            return real_settings(selected)
        first_kwargs = {**kwargs, "ledger_factory": ledger, "settings_factory": settings}
        first = onboard_current_user(fixed, **first_kwargs)
        assert opened == ["relay_generation", "app_generation", "ledger", "settings"]
        def logical_ledger():
            with sqlite3.connect(paths.ledger_path.as_uri() + "?mode=ro", uri=True) as connection:
                # The existing schema initializer recreates the same trigger;
                # sqlite_master row order is not part of ledger identity.
                return tuple(sorted(connection.iterdump()))
        before = (paths.settings_path.read_bytes(), logical_ledger())
        second = onboard_current_user(fixed, **kwargs)
        assert first["ledger_status"] == second["ledger_status"] == "READY"
        after = (paths.settings_path.read_bytes(), logical_ledger())
        (tmp_path / "ledger-before.sql").write_text("\n".join(before[1]), encoding="utf-8")
        (tmp_path / "ledger-after.sql").write_text("\n".join(after[1]), encoding="utf-8")
        assert before == after, "repeated first-open changed the logical ledger; compare preserved SQL snapshots"
    assert list(tmp_path.rglob("data-generation.json"))
    assert legacy.read_bytes() == b"old-business-original"
    assert (active / "candidate.dat").read_bytes() == b"candidate-original"


def test_deferred_preparation_requires_canonical_ownership_before_mutation(tmp_path):
    from tools import register_label_match_worker_pc as registration
    with pytest.raises(ValueError, match="exact canonical installer ownership"):
        onboard_current_user(tmp_path / "current", defer_canonical_relay_start=True)
    assert not list(tmp_path.rglob("data-generation.json"))
    assert registration.main(["--apply", "--data-dir", str(tmp_path / "direct"),
        "--sync-dir", str(tmp_path / "data")], defer_canonical_data_open=True) != 0
    assert not (tmp_path / "direct").exists()


@pytest.mark.parametrize("engine", ("powershell.exe", "pwsh.exe"))
def test_failed_candidate_preserves_generation_and_old_start(tmp_path, monkeypatch,
        canonical_delegation, engine):
    import auto_update
    import label_data_generation as generation
    from kmtech_shared.data_generation import record_generation
    from protected_admin import current_process_sid
    from tests.test_install_recovery import test_exact_preimages_and_safe_resume_with_real_generation
    fixed, active = tmp_path / "current", tmp_path / "versions" / ("a" * 40)
    fixed.mkdir(); active.mkdir(parents=True); (tmp_path / "update-control").mkdir()
    monkeypatch.setattr(auto_update, "resolve_lifecycle_context", lambda *a, **kw:
        {"fixed_root": str(fixed), "active_code_root": active})
    paths = resolve_current_user_onboarding_paths(active)
    roots = (paths.data_root, paths.ledger_path.parent, paths.direct_sync_root,
             paths.queue_dir, paths.logistics_profile_path.parent)
    markers = generation.marker_paths(roots, current_process_sid())
    for marker in markers:
        marker.parent.mkdir(parents=True, exist_ok=True)
        record_generation(marker, "v2.0.95", app_id="Label_Match",
            subject_sid=current_process_sid(), build_id="b" * 40)
    before = {p: p.read_bytes() for p in markers}
    monkeypatch.setattr(generation, "_installed_identity", lambda root: {
        "version": "v2.0.96", "build_id": "a" * 40,
        "data_compatible_min_version": "v2.0.96", "compatibility_verified": False})
    kwargs = dict(require_bootstrap_integrity=False,
        registration_runner=lambda selected: (_ready_state(selected), 0)[1],
        profile_loader=_profile_loader, credential_loader=_credential_loader,
        autostart_installer=_autostart, legacy_task_quiescence_reader=_legacy_task_quiescent,
        relay_launcher=_relay_start)
    with pytest.raises(__import__('current_user_onboarding').CurrentUserOnboardingError):
        onboard_current_user(fixed, defer_canonical_relay_start=True,
            scheduled_task_remover=lambda root: (_ for _ in ()).throw(
                tasks.CurrentUserScheduledTaskError("isolated migration failure")), **kwargs)
    assert before == {p: p.read_bytes() for p in markers}
    # Actual existingVerified rollback and old-version generation predicate.
    test_exact_preimages_and_safe_resume_with_real_generation(tmp_path, engine, "placement_strict",
        generation_marker_paths=[str(p) for p in markers], rollback_target_version="v2.0.95")
    (canonical_delegation / "active.json").unlink()
    monkeypatch.setattr(auto_update, "resolve_lifecycle_context", lambda *a, **kw:
        {"fixed_root": str(fixed), "active_code_root": fixed})
    monkeypatch.setattr(generation, "_installed_identity", lambda root: {
        "version": "v2.0.95", "build_id": "b" * 40,
        "data_compatible_min_version": "v2.0.95", "compatibility_verified": False})
    assert generation.check_user_data_open(fixed, roots)["version"] == "v2.0.95"
    assert onboard_current_user(fixed, scheduled_task_remover=lambda root: {"status":"ABSENT"},
        **kwargs)["ledger_status"] == "READY"
    assert before == {p: p.read_bytes() for p in markers}


@pytest.mark.parametrize("deferred", (False, True))
def test_successful_registration_defers_generation_only_for_canonical(tmp_path, monkeypatch,
        canonical_delegation, deferred):
    from tests.test_register_label_match_worker_pc import (
        load_registration_module, fake_v2_enrollment_response)
    from tests._possession_fixture import fake_possession_descriptor
    module = load_registration_module()
    monkeypatch.setattr(module, "_prepare_possession_key", lambda report: fake_possession_descriptor())
    monkeypatch.setattr(module, "_enroll", lambda payload, **kwargs:
        fake_v2_enrollment_response(module, payload, "isolated-registration-value"))
    monkeypatch.setattr(module, "_write_dpapi_secret", lambda data_dir, target, value:
        Path(data_dir) / "secrets" / (target + ".dpapi"))
    monkeypatch.setattr(module, "_verify_dpapi_secret", lambda *args: True)
    report_path = tmp_path / "direct/status/registration.json"
    result = module.main(["--apply", "--server-base-url", "https://isolated.example.invalid",
        "--pc-id", "ISOLATED-412", "--machine-guid", "00112233-4455-6677-8899-aabbccddeeff",
        "--sync-dir", str(tmp_path / "data"), "--data-dir", str(tmp_path / "direct"),
        "--report-path", str(report_path)], defer_canonical_data_open=deferred)
    assert result == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "SELF_ENROLLMENT_REGISTERED"
    assert report["server_registration_verified"] is True
    assert bool(list(tmp_path.rglob("data-generation.json"))) is not deferred


@pytest.mark.parametrize("engine", ("powershell.exe", "pwsh.exe"))
def test_same_pc_retry_reports_reattach_and_restores_old_app(tmp_path, monkeypatch,
        canonical_delegation, engine):
    import auto_update
    from current_user_onboarding import CurrentUserOnboardingError
    from tests.test_register_label_match_worker_pc import load_registration_module, fake_v2_enrollment_response
    from tests._possession_fixture import fake_possession_descriptor
    from tests.test_shared_ancestor_protection import run_functions
    from tests.test_install_recovery import test_exact_preimages_and_safe_resume_with_real_generation
    from tests.test_round5_startup_rollback import test_actual_startup_restore_refreshes_only_live_bound_owner
    module = load_registration_module()
    fixed, active = tmp_path / "current", tmp_path / "versions" / ("a" * 40)
    fixed.mkdir(); active.mkdir(parents=True); (tmp_path / "update-control").mkdir()
    monkeypatch.setattr(auto_update, "resolve_lifecycle_context", lambda *a, **kw:
        {"fixed_root": str(fixed), "active_code_root": active})
    paths = resolve_current_user_onboarding_paths(active)
    monkeypatch.setattr(module, "_prepare_possession_key", lambda report: fake_possession_descriptor())
    monkeypatch.setattr(module, "_write_dpapi_secret", lambda data_dir, target, value:
        Path(data_dir) / "secrets" / (target + ".dpapi"))
    monkeypatch.setattr(module, "_verify_dpapi_secret", lambda *a: True)
    calls = []
    def server_contract(url, **kw):
        payload = kw["json"]
        calls.append(payload["manifest"]["pc_identity"]["producer_install_id"])
        if len(calls) == 1:
            return SimpleNamespace(status_code=200, json=lambda:
                fake_v2_enrollment_response(module, payload, "isolated-registration-value"))
        assert calls[0] == calls[1]
        # Exact branch: Web producer_self_enrollment.py existing possession
        # binding rejects initial enrollment and demands signed re-attach.
        return SimpleNamespace(status_code=409, json=lambda: {"error": {
            "code": "reattach_proof_required", "message": "possession-proof re-attach required"}})
    monkeypatch.setattr(module.requests, "post", server_contract)
    argv = ["--apply", "--server-base-url", "https://isolated.invalid", "--pc-id", "ISOLATED-412",
        "--machine-guid", "00112233-4455-6677-8899-aabbccddeeff", "--sync-dir", str(paths.data_root),
        "--data-dir", str(paths.direct_sync_root), "--report-path", str(paths.registration_report_path)]
    assert module.main(argv, defer_canonical_data_open=True) == 0
    quarantine = tmp_path / "first-registration-quarantine"
    selected = [str(paths.credential_path), str(paths.logistics_profile_path.parent), str(paths.direct_sync_root)]
    result = run_functions(tmp_path, Path(module.__file__).parents[1] / "INSTALL_CANONICAL_PORTABLE.ps1",
        ["Full", "Same", "Save-LabelMachineMetadata", "FirstRegistrationAbsence", "Move-LegacyFirstRegistrationState"], r'''
. (Join-Path $env:LM_412_REPO 'kmtech_shared/powershell/portable.ps1')
$paths=@((ConvertFrom-Json -InputObject $env:LM_412_STATE_PATHS).paths)
$restored=Move-LegacyFirstRegistrationState $paths $env:LM_412_QUARANTINE $true
if(-not $restored.restored_absent -or $restored.server_rollback_performed){throw 'Local registration preimage differs'}
'LOCAL_ABSENT_SERVER_RETAINED'
''', engine=engine, values={"LM_412_REPO": str(Path(module.__file__).parents[1]),
        "LM_412_STATE_PATHS": json.dumps({"paths": selected}), "LM_412_QUARANTINE": str(quarantine)})
    assert result.returncode == 0, result.stderr
    with pytest.raises(CurrentUserOnboardingError):
        onboard_current_user(fixed, require_bootstrap_integrity=False, defer_canonical_relay_start=True,
            registration_runner=lambda selected: module.main(argv, defer_canonical_data_open=True),
            profile_loader=_profile_loader, credential_loader=_credential_loader,
            legacy_task_quiescence_reader=_legacy_task_quiescent,
            autostart_installer=lambda root: pytest.fail("refused enrollment activated Run"))
    assert calls[0] == calls[1] and len(calls) == 2
    report = json.loads(paths.registration_report_path.read_text(encoding="utf-8"))
    assert report["status"] == "POSSESSION_PROOF_REATTACH_REQUIRED"
    assert report["automatic_credential_replay_performed"] is False
    assert not list(tmp_path.rglob("data-generation.json"))
    test_exact_preimages_and_safe_resume_with_real_generation(tmp_path, engine, "fresh_strict",
        version_root=active.parent)
    restored = tmp_path / "wrapper-recovery"; restored.mkdir()
    monkeypatch.setenv("LM_ROUND5_WRAPPER_SOURCE", os.environ["LM_B1_WRAPPER"])
    test_actual_startup_restore_refreshes_only_live_bound_owner(restored, engine, "valid")
    assert not fixed.exists() and not active.parent.exists()
    assert (restored / "old-folder/old-file.dat").is_file()


@pytest.mark.parametrize("engine", ("powershell.exe", "pwsh.exe"))
@pytest.mark.parametrize("valid_pin", (False, True))
def test_startup_update_absent_preimage_removes_only_recorded_task(tmp_path, engine, valid_pin):
    import auto_update
    from tests.test_shared_ancestor_protection import run_functions
    root = Path(__file__).resolve().parents[1]
    expected_xml = tmp_path / "owned-task.xml"
    expected_xml.write_text(auto_update.logon_task_xml(app_root=tmp_path / "current",
        operator_sid="S-1-5-21-100-200-300-1001"), encoding="utf-8")
    result = run_functions(tmp_path, root / "INSTALL_THIS_PC.ps1",
        ["Restore-LabelStartupUpdatePreimage", "Assert-LabelStartupUpdateTaskDefinition", "ConvertTo-LabelTaskSid"], r'''
Set-StrictMode -Version Latest
$fixtureRoot=$env:LM_412_ROOT;$fixed=Join-Path $fixtureRoot 'current';$control=Join-Path $fixtureRoot 'update-control'
$OperatorSid='S-1-5-21-100-200-300-1001';$preimage=Join-Path $fixtureRoot ('.startup-update-install.'+('a'*32))
[void][IO.Directory]::CreateDirectory($control);[void][IO.Directory]::CreateDirectory($preimage)
$xml=[IO.File]::ReadAllText((Join-Path $fixtureRoot 'owned-task.xml'))
$ownedTaskName='KMTech.LabelMatch.StartupUpdate.'+('b'*24)
$files=@();foreach($name in @('policy.json','active.json','transition.json')){
 [IO.File]::WriteAllText((Join-Path $control $name),'new-control')
 $files+=@{name=$name;exists=$false;sha256=''}
}
$record=@{schema='label-match-startup-update-install-preimage-v1';root=$fixed;operator_sid=$OperatorSid;
 files=$files;task=@{name=$ownedTaskName;exists=$false;expected_xml=$xml;xml='';sddl=''}}
$recordPath=Join-Path $preimage 'preimage.json';$record|ConvertTo-Json -Depth 8|Set-Content -LiteralPath $recordPath -Encoding UTF8
$pin=(Get-FileHash -LiteralPath $recordPath).Hash.ToLowerInvariant()
if($env:LM_412_VALID_PIN -cne '1'){$pin='0'*64}
$script:task=[pscustomobject]@{State='Ready'};$script:removed=0
$registered=[pscustomobject]@{Xml=$xml};$registered|Add-Member ScriptMethod GetInstances {param($Flags) [pscustomobject]@{Count=0}}
$folder=[pscustomobject]@{};$folder|Add-Member ScriptMethod GetTask {param($Name) return $registered}
$script:isolatedService=[pscustomobject]@{};$isolatedService|Add-Member ScriptMethod Connect {}; $isolatedService|Add-Member ScriptMethod GetFolder {param($Path) return $folder}
# Adapt only the two native COM construction sites. A global New-Object mock
# also intercepts Windows PowerShell's own Get-FileHash implementation.
$restore=(Get-Command Restore-LabelStartupUpdatePreimage).ScriptBlock.ToString()
$comSite="New-Object -ComObject 'Schedule.Service'"
if(([regex]::Matches($restore,[regex]::Escape($comSite))).Count -ne 2){throw 'Native COM boundary changed'}
function Get-IsolatedScheduleService {return $script:isolatedService}
Set-Item -LiteralPath function:Restore-LabelStartupUpdatePreimage -Value ([scriptblock]::Create($restore.Replace($comSite,'Get-IsolatedScheduleService')))
function Assert-LabelStartupUpdateTrust {param($Root)}
function Assert-ProtectedCodeAncestors {param($Path)
 if(-not (Test-SamePath $Path $fixtureRoot)){throw 'Unexpected protected parent'}
}
function Get-StrictFullPath {param($Path,$Purpose) [IO.Path]::GetFullPath($Path)}
function Test-SamePath {param($Left,$Right) [StringComparer]::OrdinalIgnoreCase.Equals([IO.Path]::GetFullPath($Left),[IO.Path]::GetFullPath($Right))}
function Assert-NoReparsePoint {param($Path,$Purpose)
 $full=[IO.Path]::GetFullPath($Path)
 if(-not $full.StartsWith($fixtureRoot+'\',[StringComparison]::OrdinalIgnoreCase)){throw 'Fixture boundary escaped'}
 if((Test-Path -LiteralPath $full) -and ((Get-Item -LiteralPath $full).Attributes -band [IO.FileAttributes]::ReparsePoint)){throw 'Fixture reparse point'}
}
function Assert-HardenedCodeAcl {param($Path,[switch]$Recursive) Assert-NoReparsePoint $Path 'ACL adapter'}
function Get-FileSha256 {param($Path) (Get-FileHash -LiteralPath $Path).Hash.ToLowerInvariant()}
function Get-LabelStartupUpdateTask {param($Name) if($Name -cne $ownedTaskName){throw 'Foreign task query'};return $script:task}
function Export-ScheduledTask {param($TaskName) if($TaskName -cne $ownedTaskName){throw 'Foreign task export'};return $xml}
function Invoke-LabelStartupUpdateTaskMutation {param([scriptblock]$Operation) & $Operation}
function Unregister-ScheduledTask {param($TaskName,[switch]$Confirm)
 if($TaskName -cne $ownedTaskName){throw 'Foreign task mutation'};$script:task=$null;$script:removed++
}
$rejected=$false
try{Restore-LabelStartupUpdatePreimage $fixed $preimage $pin}catch{
 if($env:LM_412_VALID_PIN -ceq '1' -or $_.Exception.Message -cne 'STARTUP_UPDATE_RESTORE_PREIMAGE_PIN_DIFFERS'){throw};$rejected=$true
}
if($env:LM_412_VALID_PIN -ceq '1'){
 if($null -ne $script:task -or $script:removed -ne 1){throw 'New StartupUpdate task survived rollback'}
 foreach($name in @('policy.json','active.json','transition.json')){if(Test-Path -LiteralPath (Join-Path $control $name)){throw 'New control file survived rollback'}}
}elseif(-not $rejected -or $null -eq $script:task -or $script:removed -ne 0){throw 'Unproven preimage mutated task'}
'RECORDED_TASK_ONLY'
''', engine=engine, values={"LM_412_ROOT": str(tmp_path), "LM_412_VALID_PIN": "1" if valid_pin else "0"})
    assert result.returncode == 0, result.stderr
    assert "RECORDED_TASK_ONLY" in result.stdout

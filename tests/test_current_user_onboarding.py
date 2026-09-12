import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from current_user_onboarding import (
    CurrentUserOnboardingError,
    DEFAULT_SERVER_BASE_URL,
    ENROLLMENT_TLS_CA_BUNDLE_PATH_ENV,
    _registration_runner,
    inspect_current_user_state,
    onboard_current_user,
    remove_current_user_setup,
    resolve_current_user_onboarding_paths,
    verify_bootstrap_integrity,
)
from direct_sync_push import manifest_hash


def test_recovery_required_onboarding_is_visible_and_does_not_repeat_enrollment(tmp_path):
    env = {"LOCALAPPDATA": str(tmp_path / "local"), "ProgramData": str(tmp_path / "program-data")}
    paths = resolve_current_user_onboarding_paths(tmp_path / "app", environ=env)
    attempts = []

    def registration(selected):
        attempts.append(True)
        _write_json(selected.registration_report_path, {
            "status": "ADMIN_RECOVERY_REQUIRED", "recovery_action": "ADMIN_RECOVERY_REQUIRED",
            "secret_material_persisted": False,
        })
        return 2

    for _ in range(2):
        with pytest.raises(CurrentUserOnboardingError) as caught:
            onboard_current_user(
                paths.app_root, environ=env, require_bootstrap_integrity=False,
                registration_runner=registration,
                legacy_task_quiescence_reader=lambda: {
                    "schema": "label-match-legacy-task-quiescence-v1", "status": "PASS",
                    "required_state": "ABSENT_OR_DISABLED", "read_only": True,
                    "task_or_process_mutated": False,
                },
                autostart_installer=lambda _root: pytest.fail("recovery activated persistence"),
            )
        assert caught.value.status == "RECOVERY_REQUIRED"
        assert "일반 등록 토큰과 별도의 관리자 복구 승인" in str(caught.value)
        report = json.loads(paths.onboarding_report_path.read_text(encoding="utf-8"))
        assert report["status"] == "RECOVERY_REQUIRED"
        assert report["recovery_action"] == "ADMIN_RECOVERY_REQUIRED"
        assert report["server_registration_verified"] is False
    assert attempts == [True]
    assert not paths.identity_path.exists()
    assert not paths.ledger_path.exists()


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _ready_state(paths, *, source_host_id="label-match-user-1"):
    identity = {
        "schema_version": "label-match-producer-identity-v1",
        "producer_id": source_host_id,
        "source_host_id": source_host_id,
        "producer_install_id": "label-match-install-1",
        "pc_id": "LABEL-PC01",
    }
    manifest = {
        "schema_version": "producer-onboarding-manifest-v1",
        "pc_identity": {
            "pc_id": identity["pc_id"],
            "source_host_id": source_host_id,
            "producer_install_id": identity["producer_install_id"],
        },
        "apps": ["LabelMatch"],
        "streams": [],
    }
    _write_json(paths.identity_path, identity)
    _write_json(paths.producer_manifest_path, manifest)
    _write_json(
        paths.credential_path,
        {
            "credential_schema_version": "producer-ingest-credential-reference-v1",
            "producer_id": source_host_id,
            "dpapi_scope": "current_user",
        },
    )
    _write_json(
        paths.registration_report_path,
        {
            "status": "SELF_ENROLLMENT_REGISTERED",
            "server_registration_verified": True,
            "manifest_hash_verified": True,
            "persisted_manifest_hash_verified": True,
            "manifest_hash": manifest_hash(manifest),
        },
    )
    _write_json(
        paths.logistics_profile_path,
        {
            "source_host_id": source_host_id,
            "credential_scope": "current_user",
            "authority_plane": "AUTHORITATIVE",
        },
    )
    paths.logistics_secret_path.parent.mkdir(parents=True, exist_ok=True)
    paths.logistics_secret_path.write_bytes(b"current-user-dpapi-fixture")
    return identity


def _profile_loader(path: Path):
    payload = json.loads(path.read_text(encoding="utf-8"))
    return SimpleNamespace(
        source_host_id=payload["source_host_id"],
        authority_plane=payload["authority_plane"],
        tls_ca_bundle_path=payload.get("tls_ca_bundle_path", ""),
    )


def _credential_loader(_path: Path):
    return SimpleNamespace(producer_id="label-match-user-1")


def _ledger_factory(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"SQLite format 3\x00")


def _autostart(_app_root):
    return {"status": "PASS", "principal": "current_user"}


def _relay_start(_app_root):
    return {"status": "ALIVE", "process_id": 123, "survival_seconds": 2.0}




def _scheduled_task_absent(_app_root):
    return {"status": "ABSENT"}


def _legacy_task_quiescent():
    return {
        "schema": "label-match-legacy-task-quiescence-v1",
        "status": "PASS",
        "reason_code": "LEGACY_TASK_ABSENT",
        "required_state": "ABSENT_OR_DISABLED",
        "read_only": True,
        "task_or_process_mutated": False,
    }


def _environment(tmp_path):
    return {"LABEL_MATCH_SAVE_DIR": str(tmp_path / "state" / "data")}


def test_paths_separate_code_and_current_user_state(tmp_path):
    app_root = tmp_path / "hardened-code"
    paths = resolve_current_user_onboarding_paths(
        app_root,
        environ={"LOCALAPPDATA": str(tmp_path / "local-app-data"), "ProgramData": str(tmp_path / "program-data")},
    )

    assert paths.app_root == app_root.resolve()
    assert (
        paths.data_root
        == (tmp_path / "local-app-data" / "KMTech" / "Label_Match" / "data").resolve()
    )
    assert (
        paths.direct_sync_root
        == (
            tmp_path / "local-app-data" / "KMTech" / "DirectSync" / "label_match"
        ).resolve()
    )
    assert (
        paths.logistics_profile_path
        == (
            tmp_path
            / "local-app-data"
            / "KMTech"
            / "Logistics"
            / "profiles"
            / "Label_Match"
            / "runtime-profile.json"
        ).resolve()
    )
    assert paths.ledger_path.name == "package_logistics_outbox.sqlite3"
    assert paths.app_root not in paths.data_root.parents


@pytest.mark.parametrize("launch", ["env-a", "env-b"])
def test_custom_root_is_shared_by_onboarding_gui_guard_relay_and_registration(
    monkeypatch, tmp_path, launch
):
    import Label_Match as app_module
    from label_match_single_instance import resolve_data_scope
    from tools import register_label_match_worker_pc
    from user_relay import _resolve_scan_source_dir

    custom = tmp_path / "custom-c"
    settings = tmp_path / "settings.json"
    payload = {"custom_save_path": str(custom)}
    _write_json(settings, payload)
    environment = {
        "LOCALAPPDATA": str(tmp_path / "local"),
        "LABEL_MATCH_SAVE_DIR": str(tmp_path / launch),
        "LABEL_MATCH_SETTINGS_PATH": str(settings),
    }
    for key, value in environment.items():
        monkeypatch.setenv(key, value)
    paths = resolve_current_user_onboarding_paths(tmp_path / "app", environ=environment)
    app = object.__new__(app_module.Label_Match)
    app.app_settings = payload
    args = []
    monkeypatch.setattr(register_label_match_worker_pc, "main", lambda argv: args.extend(argv) or 0)
    assert _registration_runner(paths, server_base_url="https://example.invalid", environ=environment) == 0

    assert paths.data_root == custom
    assert paths.ledger_path == custom / "package_logistics_outbox.sqlite3"
    assert app._resolve_configured_save_path() == str(custom)
    assert resolve_data_scope(environment=environment, settings_path=settings) == str(custom)
    assert _resolve_scan_source_dir("", data_root=paths.data_root, settings_path=settings) == custom
    assert args[args.index("--sync-dir") + 1] == str(custom)
    assert not custom.exists()  # Resolution and argument construction are read-only.


def test_first_onboarding_uses_settings_template_before_registration(tmp_path):
    app_root = tmp_path / "app"
    custom = tmp_path / "custom-c"
    _write_json(app_root / "config" / "app_settings.json", {"custom_save_path": str(custom)})
    environment = {"LOCALAPPDATA": str(tmp_path / "local"), "ProgramData": str(tmp_path / "program-data")}
    registrations = []

    def register(paths):
        assert not paths.settings_path.exists()
        registrations.append(paths.data_root)
        _ready_state(paths)
        return 0

    report = onboard_current_user(
        app_root, environ=environment, require_bootstrap_integrity=False,
        registration_runner=register, profile_loader=_profile_loader,
        credential_loader=_credential_loader, ledger_factory=_ledger_factory,
        autostart_installer=_autostart, scheduled_task_remover=_scheduled_task_absent,
        legacy_task_quiescence_reader=_legacy_task_quiescent, relay_launcher=_relay_start,
    )
    assert registrations == [custom]
    assert report["data_root"] == str(custom)
    assert report["ledger_path"] == str(custom / "package_logistics_outbox.sqlite3")
    assert report["storage_root_compatibility"] == "UNIFIED"
    assert resolve_current_user_onboarding_paths(app_root, environ=environment).data_root == custom


def test_explicit_settings_custom_root_does_not_require_default_environment(tmp_path):
    settings = tmp_path / "settings.json"
    custom = tmp_path / "custom"
    _write_json(settings, {"custom_save_path": str(custom)})
    paths = resolve_current_user_onboarding_paths(
        tmp_path / "app", environ={"LABEL_MATCH_SETTINGS_PATH": str(settings)},
    )
    assert paths.data_root == custom
    assert paths.settings_path == settings
    assert not custom.exists()


def test_standalone_default_root_selection_preserves_existing_data(monkeypatch, tmp_path):
    import Label_Match as app_module
    from label_match_single_instance import resolve_data_scope
    from user_relay import _resolve_scan_source_dir

    environment = {"LOCALAPPDATA": str(tmp_path / "local"), "ProgramData": str(tmp_path / "program")}
    legacy = tmp_path / "program/KMTech/Label_Match/data"
    for key in ("LABEL_MATCH_SAVE_DIR", "LABEL_MATCH_DIRECT_SYNC_ROOT", "LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT"):
        monkeypatch.delenv(key, raising=False)
    for key, value in environment.items():
        monkeypatch.setenv(key, value)
    app = object.__new__(app_module.Label_Match)
    app.app_settings = {}
    assert app._resolve_configured_save_path() == str(legacy)
    assert resolve_data_scope(environment=environment, settings={}) == str(legacy)
    assert not legacy.exists()

    legacy.mkdir(parents=True)
    csv = legacy / "existing.csv"
    csv.write_bytes(b"existing standalone completion")
    for _ in range(2):
        paths = resolve_current_user_onboarding_paths(tmp_path / "app", environ=environment)
        assert paths.data_root == paths.ledger_path.parent == legacy
        assert not paths.identity_path.exists()
        assert app._resolve_configured_save_path() == str(legacy)
        assert resolve_data_scope(environment=environment, settings_path=paths.settings_path) == str(legacy)
        assert _resolve_scan_source_dir("", data_root=paths.data_root, settings_path=paths.settings_path) == legacy
    assert csv.read_bytes() == b"existing standalone completion"
    assert sorted(path for path in tmp_path.rglob("*") if path.is_file()) == [csv]


@pytest.mark.parametrize("installation", ["new", "onboarded", "split-defaults"])
def test_default_root_stays_stable_through_registration_and_relay_start(
    monkeypatch, tmp_path, caplog, installation
):
    import Label_Match as app_module
    from label_match_single_instance import resolve_data_scope
    from user_relay import _resolve_scan_source_dir

    local = tmp_path / "local"
    program = tmp_path / "program-data"
    environment = {"LOCALAPPDATA": str(local), "ProgramData": str(program)}
    app_root = tmp_path / "app"
    local_data = local / "KMTech" / "Label_Match" / "data"
    legacy_data = program / "KMTech" / "Label_Match" / "data"
    expected = local_data
    if installation == "split-defaults":
        legacy_data.mkdir(parents=True)
        (legacy_data / "existing.csv").write_bytes(b"existing standalone completion")
        local_data.mkdir(parents=True)
        (local_data / "package_logistics_outbox.sqlite3").write_bytes(b"existing onboarding ledger")
    if installation in {"onboarded", "split-defaults"}:
        _ready_state(resolve_current_user_onboarding_paths(app_root, environ=environment))
    paths = resolve_current_user_onboarding_paths(app_root, environ=environment)
    assert paths.data_root == expected
    assert paths.ledger_path.parent == expected
    registration_roots = []

    def register(selected):
        assert installation == "new"
        registration_roots.append(selected.data_root)
        _ready_state(selected)
        # Identity has appeared but registration is not yet finished: no switch.
        assert resolve_data_scope(environment=environment, settings_path=selected.settings_path) == str(expected)
        return 0

    def relay(_root):
        # The real relay starts before onboarding applies its process environment.
        selected = resolve_current_user_onboarding_paths(app_root, environ=environment)
        assert _resolve_scan_source_dir("", data_root=selected.data_root, settings_path=selected.settings_path) == expected
        return _relay_start(_root)

    ledger_before = paths.ledger_path.read_bytes() if paths.ledger_path.exists() else None
    report = onboard_current_user(
        app_root, environ=environment, require_bootstrap_integrity=False,
        registration_runner=register, profile_loader=_profile_loader,
        credential_loader=_credential_loader,
        ledger_factory=lambda path: _ledger_factory(path) if not path.exists() else None,
        autostart_installer=_autostart, scheduled_task_remover=_scheduled_task_absent,
        legacy_task_quiescence_reader=_legacy_task_quiescent, relay_launcher=relay,
    )
    assert report["data_root"] == str(expected)
    assert registration_roots == ([expected] if installation == "new" else [])
    for key, value in environment.items():
        monkeypatch.setenv(key, value)
    app = object.__new__(app_module.Label_Match)
    app.app_settings = {}
    assert app._resolve_configured_save_path() == str(expected)
    assert resolve_data_scope(environment=environment, settings_path=paths.settings_path) == str(expected)
    # A fresh process has no onboarding-applied SAVE_DIR override.
    fresh = {"LOCALAPPDATA": str(local), "ProgramData": str(program)}
    assert resolve_current_user_onboarding_paths(app_root, environ=fresh).data_root == expected
    assert resolve_data_scope(environment=fresh, settings_path=paths.settings_path) == str(expected)
    if ledger_before is not None:
        assert paths.ledger_path.read_bytes() == ledger_before
    if installation == "split-defaults":
        assert (legacy_data / "existing.csv").read_bytes() == b"existing standalone completion"
    assert report["storage_root_compatibility"] == "UNIFIED"
    assert "rule=onboarding_state" in caplog.text
    assert "storage_root_selected:" in caplog.text


@pytest.mark.parametrize("legacy_override", [False, True], ids=["local-default", "env-a"])
def test_split_installation_preserves_both_stores_and_identity_after_restart(
    tmp_path, caplog, legacy_override
):
    from current_user_onboarding import apply_current_user_runtime_environment
    from user_relay import _resolve_scan_source_dir

    environment = {"LOCALAPPDATA": str(tmp_path / "local"), "ProgramData": str(tmp_path / "program-data")}
    if legacy_override:
        environment["LABEL_MATCH_SAVE_DIR"] = str(tmp_path / "env-a")
    app_root = tmp_path / "app"
    old = resolve_current_user_onboarding_paths(app_root, environ=environment)
    old.ledger_path.parent.mkdir(parents=True)
    old.ledger_path.write_bytes(b"existing onboarding ledger")
    custom = tmp_path / "custom-c"
    custom.mkdir()
    business_ledger = custom / "package_logistics_outbox.sqlite3"
    business_ledger.write_bytes(b"existing pending business identity")
    csv = custom / "existing.csv"
    csv.write_bytes(b"existing completion CSV")
    _write_json(old.settings_path, {"custom_save_path": str(custom)})
    _ready_state(old)
    queued = old.queue_dir / "pending.json"
    _write_json(queued, {"key": "same-key", "endpoint": "https://example.invalid", "pending": True})
    spool = old.spool_dir / "pending.csv"
    spool.parent.mkdir(parents=True)
    spool.write_bytes(b"pending producer payload")
    preserved = {
        path: path.read_bytes() for path in (
            old.ledger_path, business_ledger, csv, old.settings_path, old.identity_path,
            old.producer_manifest_path, old.credential_path, old.registration_report_path,
            old.logistics_profile_path, old.logistics_secret_path, queued, spool,
        )
    }
    ledgers = []
    for launch in (None, "env-b"):
        if launch:
            environment["LABEL_MATCH_SAVE_DIR"] = str(tmp_path / launch)
        paths = resolve_current_user_onboarding_paths(app_root, environ=environment)
        assert paths.data_root == custom
        assert paths.ledger_path == old.ledger_path
        assert paths.direct_sync_root == old.direct_sync_root
        report = onboard_current_user(
            app_root, environ=environment, require_bootstrap_integrity=False,
            registration_runner=lambda _paths: pytest.fail("existing identity re-enrolled"),
            profile_loader=_profile_loader, credential_loader=_credential_loader,
            ledger_factory=lambda path: ledgers.append(path), autostart_installer=_autostart,
            scheduled_task_remover=_scheduled_task_absent,
            legacy_task_quiescence_reader=_legacy_task_quiescent, relay_launcher=_relay_start,
        )
        assert report["action"] == "REUSED"
        assert report["storage_root_compatibility"] == "SPLIT_PRESERVED"
        apply_current_user_runtime_environment(paths, environ=environment)
        assert resolve_current_user_onboarding_paths(app_root, environ=environment).ledger_path == old.ledger_path
        assert _resolve_scan_source_dir("", data_root=paths.data_root, settings_path=paths.settings_path) == custom
    assert ledgers == [old.ledger_path, old.ledger_path]
    assert {path: path.read_bytes() for path in preserved} == preserved
    assert "storage_root_split_preserved" in caplog.text


def test_preserved_ledger_cannot_bypass_existing_legacy_sync_path_rejection(monkeypatch, tmp_path):
    import current_user_onboarding as onboarding

    environment = _environment(tmp_path)
    paths = resolve_current_user_onboarding_paths(tmp_path / "app", environ=environment)
    redirected = tmp_path / "forbidden" / "package_logistics_outbox.sqlite3"
    redirected.parent.mkdir()
    redirected.write_bytes(b"preserved legacy ledger")
    _write_json(paths.onboarding_report_path, {"ledger_path": str(redirected)})
    original = onboarding._is_legacy_sync_path
    monkeypatch.setattr(onboarding, "_is_legacy_sync_path", lambda path: path == redirected or original(path))
    with pytest.raises(CurrentUserOnboardingError, match="must not use the legacy Sync root"):
        resolve_current_user_onboarding_paths(tmp_path / "app", environ=environment)
    assert redirected.read_bytes() == b"preserved legacy ledger"
    assert not paths.ledger_path.exists()


def test_state_absent_partial_and_ready_are_distinguished(tmp_path):
    paths = resolve_current_user_onboarding_paths(
        tmp_path / "app", environ=_environment(tmp_path)
    )
    assert (
        inspect_current_user_state(
            paths,
            profile_loader=_profile_loader,
            credential_loader=_credential_loader,
        )["status"]
        == "ABSENT"
    )
    _write_json(paths.identity_path, {"source_host_id": "partial"})
    assert (
        inspect_current_user_state(
            paths,
            profile_loader=_profile_loader,
            credential_loader=_credential_loader,
        )["status"]
        == "RECOVERY_REQUIRED"
    )
    paths.identity_path.unlink()
    _ready_state(paths)
    ready = inspect_current_user_state(
        paths,
        profile_loader=_profile_loader,
        credential_loader=_credential_loader,
    )
    assert ready["status"] == "READY"
    assert ready["authority_plane"] == "AUTHORITATIVE"


def test_first_run_and_rerun_succeed_without_mutating_readonly_code_root(tmp_path):
    app_root = tmp_path / "hardened-app"
    internal = app_root / "_internal"
    internal.mkdir(parents=True)
    (app_root / "Label_Match.exe").write_bytes(b"main")
    (internal / "python312.dll").write_bytes(b"runtime")
    _write_bootstrap_root_record(app_root)
    environment = _environment(tmp_path)
    paths = resolve_current_user_onboarding_paths(app_root, environ=environment)
    registration_calls = []

    def register(selected_paths):
        registration_calls.append(selected_paths)
        _ready_state(selected_paths)
        return 0

    kwargs = {
        "environ": environment,
        "require_bootstrap_integrity": True,
        "registration_runner": register,
        "profile_loader": _profile_loader,
        "credential_loader": _credential_loader,
        "ledger_factory": _ledger_factory,
        "autostart_installer": _autostart,
        "scheduled_task_remover": _scheduled_task_absent,
        "legacy_task_quiescence_reader": _legacy_task_quiescent,
        "relay_launcher": _relay_start,
    }
    code_files = [path for path in app_root.rglob("*") if path.is_file()]
    code_directories = [internal, app_root]
    code_before = {
        path.relative_to(app_root)
        .as_posix(): hashlib.sha256(path.read_bytes())
        .hexdigest()
        for path in code_files
    }
    for path in code_files:
        path.chmod(0o444)
    for path in code_directories:
        path.chmod(0o555)
    try:
        first = onboard_current_user(app_root, **kwargs)
        identity_before = paths.identity_path.read_bytes()
        second = onboard_current_user(app_root, **kwargs)
        code_after = {
            path.relative_to(app_root)
            .as_posix(): hashlib.sha256(path.read_bytes())
            .hexdigest()
            for path in app_root.rglob("*")
            if path.is_file()
        }
    finally:
        for path in reversed(code_directories):
            path.chmod(0o777)
        for path in code_files:
            path.chmod(0o666)

    assert first["action"] == "CREATED"
    assert second["action"] == "REUSED"
    assert first["bootstrap_integrity"]["status"] == "PASS"
    assert second["bootstrap_integrity"]["status"] == "PASS"
    assert len(registration_calls) == 1
    assert paths.identity_path.read_bytes() == identity_before
    assert paths.ledger_path.is_file()
    assert first["operation_lease_store"] == "AUTHORITATIVE_SNAPSHOT_PRESERVED"
    assert first["system_scheduled_task_required"] is False
    assert first["current_user_scheduled_task_required"] is False
    assert environment["LABEL_MATCH_DIRECT_SYNC_ROOT"] == str(paths.direct_sync_root)
    assert code_after == code_before


def test_enabled_legacy_task_blocks_before_enrollment_or_persistence(tmp_path):
    app_root = tmp_path / "app"
    app_root.mkdir()
    environment = _environment(tmp_path)
    paths = resolve_current_user_onboarding_paths(app_root, environ=environment)
    calls: list[str] = []

    def called(name, result):
        def invoke(*_args, **_kwargs):
            calls.append(name)
            return result

        return invoke

    legacy_failure = {
        "schema": "label-match-legacy-task-quiescence-v1",
        "status": "FAIL",
        "reason_code": "LEGACY_TASK_PRESENT_ENABLED",
        "required_state": "ABSENT_OR_DISABLED",
        "read_only": True,
        "task_or_process_mutated": False,
        "remediation": (
            "Disable or remove root scheduled task "
            r"\direct-sync-relay-label-match-current-pc before Label enrollment."
        ),
    }

    with pytest.raises(
        CurrentUserOnboardingError,
        match="LEGACY_TASK_PRESENT_ENABLED",
    ):
        onboard_current_user(
            app_root,
            environ=environment,
            require_bootstrap_integrity=False,
            legacy_task_quiescence_reader=lambda: legacy_failure,
            registration_runner=called("registration", 0),
            ledger_factory=called("ledger", None),
            settings_factory=called("settings", {"status": "CREATED"}),
            autostart_installer=called("autostart", {"status": "PASS"}),
            scheduled_task_remover=called("scheduled-task", {"status": "ABSENT"}),
            relay_launcher=called("relay", {"status": "ALIVE"}),
        )

    assert calls == []
    assert not paths.identity_path.exists()
    assert not paths.ledger_path.exists()
    report = json.loads(paths.onboarding_report_path.read_text(encoding="utf-8"))
    assert report["status"] == "FAILED"
    assert report["legacy_task_quiescence"]["reason_code"] == (
        "LEGACY_TASK_PRESENT_ENABLED"
    )
    assert r"\direct-sync-relay-label-match-current-pc" in report["failure"]


def test_registration_runner_derives_identity_without_source_host_override(
    monkeypatch,
    tmp_path,
):
    app_root = tmp_path / "app"
    app_root.mkdir()
    paths = resolve_current_user_onboarding_paths(
        app_root, environ=_environment(tmp_path)
    )
    calls = []

    def run_registration(arguments):
        calls.append(list(arguments))
        return 0

    import tools

    monkeypatch.setattr(
        tools,
        "register_label_match_worker_pc",
        SimpleNamespace(main=run_registration),
        raising=False,
    )

    assert (
        _registration_runner(paths, server_base_url="https://worker.example.invalid")
        == 0
    )
    assert len(calls) == 1
    arguments = calls[0]
    assert "--source-host-id" not in arguments
    assert arguments[arguments.index("--credential-scope") + 1] == "current_user"
    assert arguments[arguments.index("--identity-path") + 1] == str(paths.identity_path)


def test_registration_runner_forwards_bootstrap_tls_ca_bundle(tmp_path, monkeypatch):
    app_root = tmp_path / "app"
    app_root.mkdir()
    local_app_data = tmp_path / "LocalAppData"
    environment = {"LOCALAPPDATA": str(local_app_data), "ProgramData": str(tmp_path / "program-data")}
    paths = resolve_current_user_onboarding_paths(app_root, environ=environment)
    paths.bootstrap_tls_ca_bundle_path.parent.mkdir(parents=True)
    paths.bootstrap_tls_ca_bundle_path.write_bytes(b"private-ca-fixture")
    calls = []

    import tools

    monkeypatch.setattr(
        tools,
        "register_label_match_worker_pc",
        SimpleNamespace(main=lambda arguments: calls.append(list(arguments)) or 0),
        raising=False,
    )

    assert (
        _registration_runner(
            paths,
            server_base_url="https://worker.example.invalid",
            environ=environment,
        )
        == 0
    )
    arguments = calls[0]
    assert arguments[arguments.index("--tls-ca-bundle-path") + 1] == str(
        paths.bootstrap_tls_ca_bundle_path
    )


def test_ready_profile_adds_configured_ca_without_registration(tmp_path, monkeypatch):
    app_root = tmp_path / "app"
    app_root.mkdir()
    ca_source = tmp_path / "private-ca.cert.pem"
    ca_source.write_bytes(b"private-ca-fixture")
    environment = {
        "LABEL_MATCH_SAVE_DIR": str(tmp_path / "state" / "data"),
        ENROLLMENT_TLS_CA_BUNDLE_PATH_ENV: str(ca_source),
    }
    paths = resolve_current_user_onboarding_paths(app_root, environ=environment)
    _ready_state(paths)
    upgrades = []

    def fake_upgrade(**kwargs):
        upgrades.append(kwargs)
        payload = json.loads(paths.logistics_profile_path.read_text(encoding="utf-8"))
        payload["tls_ca_bundle_path"] = str(
            paths.logistics_profile_path.parent / "tls" / "ca-bundle.pem"
        )
        _write_json(paths.logistics_profile_path, payload)
        return {"status": "upgraded"}

    monkeypatch.setattr(
        "tools.install_logistics_runtime_profile.install_tls_ca_bundle_for_existing_profile",
        fake_upgrade,
    )

    report = onboard_current_user(
        app_root,
        environ=environment,
        require_bootstrap_integrity=False,
        registration_runner=lambda _paths: (_ for _ in ()).throw(
            AssertionError("ready profile must not be registered again")
        ),
        profile_loader=_profile_loader,
        credential_loader=_credential_loader,
        ledger_factory=_ledger_factory,
        autostart_installer=_autostart,
        scheduled_task_remover=_scheduled_task_absent,
        legacy_task_quiescence_reader=_legacy_task_quiescent,
        relay_launcher=_relay_start,
    )

    assert report["status"] == "READY"
    assert report["action"] == "REUSED"
    assert report["state_readback"]["tls_private_ca_configured"] is True
    assert len(upgrades) == 1
    assert upgrades[0]["tls_ca_bundle_path"] == str(ca_source)


def test_missing_registration_result_is_unknown_not_success(tmp_path):
    app_root = tmp_path / "app"
    app_root.mkdir()
    with pytest.raises(CurrentUserOnboardingError) as caught:
        onboard_current_user(
            app_root,
            environ=_environment(tmp_path),
            require_bootstrap_integrity=False,
            registration_runner=lambda _paths: None,
            profile_loader=_profile_loader,
            credential_loader=_credential_loader,
            ledger_factory=_ledger_factory,
            autostart_installer=_autostart,
            scheduled_task_remover=_scheduled_task_absent,
            legacy_task_quiescence_reader=_legacy_task_quiescent,
            relay_launcher=_relay_start,
        )

    assert caught.value.status == "UNKNOWN"
    report = json.loads(caught.value.report_path.read_text(encoding="utf-8"))
    assert report["status"] == "UNKNOWN"


def test_stop_marker_is_preserved_until_canonical_portable_install(tmp_path):
    app_root = tmp_path / "not-canonical"
    app_root.mkdir()
    environment = _environment(tmp_path)
    paths = resolve_current_user_onboarding_paths(app_root, environ=environment)
    _ready_state(paths)
    marker = paths.control_dir / "label_match_user_relay.stop.json"
    _write_json(marker, {"schema_version": "label-match-user-relay-stop-v1"})

    with pytest.raises(CurrentUserOnboardingError, match="safety fence"):
        onboard_current_user(
            app_root,
            environ=environment,
            require_bootstrap_integrity=False,
            profile_loader=_profile_loader,
            credential_loader=_credential_loader,
            ledger_factory=_ledger_factory,
            autostart_installer=_autostart,
            scheduled_task_remover=_scheduled_task_absent,
            legacy_task_quiescence_reader=_legacy_task_quiescent,
            relay_launcher=_relay_start,
        )

    assert marker.is_file()


def test_stop_marker_remains_when_canonical_task_binding_fails(monkeypatch, tmp_path):
    app_root = (tmp_path / "canonical").resolve()
    (app_root / "runtime").mkdir(parents=True)
    runtime = app_root / "runtime" / "pythonw.exe"
    installer = app_root / "INSTALL_CANONICAL_PORTABLE.ps1"
    runtime.write_bytes(b"signed-runtime-fixture")
    installer.write_text("# canonical installer fixture\n", encoding="utf-8")
    commit = "a" * 40
    tree = "b" * 40
    _write_json(
        app_root / "portable-manifest.json",
        {
            "schema": "label-match-portable-tree-v1",
            "entrypoint": "runtime/pythonw.exe app/main.py",
            "source_commit": commit,
            "source_tree": tree,
            "allowed_unsigned_app_pe": [],
            "forbidden_package_roots": [],
            "runtime_pythonw_sha256": hashlib.sha256(runtime.read_bytes()).hexdigest(),
            "canonical_installer": "INSTALL_CANONICAL_PORTABLE.ps1",
            "canonical_installer_sha256": hashlib.sha256(
                installer.read_bytes()
            ).hexdigest(),
        },
    )
    inventory = []
    for path in sorted(
        (candidate for candidate in app_root.rglob("*") if candidate.is_file()),
        key=lambda candidate: candidate.relative_to(app_root).as_posix().casefold(),
    ):
        relative = path.relative_to(app_root).as_posix()
        payload = path.read_bytes()
        inventory.append(
            {
                "path": relative,
                "size": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        )
    aggregate = hashlib.sha256(
        "".join(
            f"{row['sha256']} {row['size']} {row['path']}\n" for row in inventory
        ).encode("utf-8")
    ).hexdigest()
    _write_json(
        app_root / "bootstrap-integrity.json",
        {
            "schema_version": "label-match-bootstrap-integrity-v1",
            "status": "PASS",
            "code_root": str(app_root),
            "file_count": len(inventory),
            "aggregate_sha256": aggregate,
            "files": inventory,
            "identity_profile_created": False,
            "state_scope": "current_user_first_run",
        },
    )
    monkeypatch.setattr("current_user_onboarding.CANONICAL_PORTABLE_ROOT", app_root)
    receipt_validation_call = {}

    def validate_receipt_fixture(*_args, **kwargs):
        receipt_validation_call.update(kwargs)
        return {
            "status": "RESOLVED",
            "selected_authority_scope": "fixture",
            "stop_marker_lineage": {
                "current_request_id": "fixture-stop-request",
                "current_sha256": "0" * 64,
            },
        }

    monkeypatch.setattr(
        "current_user_onboarding.validate_resolution_receipt",
        validate_receipt_fixture,
    )
    environment = _environment(tmp_path)
    conflict_receipt = tmp_path / "label-conflict-resolution.json"
    _write_json(conflict_receipt, {"status": "RESOLVED", "authority": "fixture"})
    environment["KMTECH_LABEL_CONFLICT_RESOLUTION_RECEIPT_PATH"] = str(
        conflict_receipt
    )
    environment["KMTECH_LABEL_CONFLICT_RESOLUTION_RECEIPT_SHA256"] = (
        hashlib.sha256(conflict_receipt.read_bytes()).hexdigest()
    )
    paths = resolve_current_user_onboarding_paths(app_root, environ=environment)
    _ready_state(paths)
    marker = paths.control_dir / "label_match_user_relay.stop.json"
    _write_json(marker, {"schema_version": "label-match-user-relay-stop-v1"})

    with pytest.raises(CurrentUserOnboardingError, match="scheduled task"):
        onboard_current_user(
            app_root,
            environ=environment,
            require_bootstrap_integrity=False,
            profile_loader=_profile_loader,
            credential_loader=_credential_loader,
            ledger_factory=_ledger_factory,
            autostart_installer=_autostart,
            scheduled_task_remover=lambda _root: {"status": "FAIL"},
            legacy_task_quiescence_reader=_legacy_task_quiescent,
            relay_launcher=_relay_start,
        )

    assert marker.is_file()
    assert receipt_validation_call["portable_root"] == app_root
    assert receipt_validation_call["allow_portable_relocation"] is True


def _write_bootstrap_root_record(app_root: Path) -> Path:
    canonical_entries = []
    file_count = 0
    for path in app_root.rglob("*"):
        if not path.is_file() or path.name == "bootstrap-integrity.json":
            continue
        payload = path.read_bytes()
        relative_path = path.relative_to(app_root).as_posix()
        canonical_entries.append(
            (
                f"{hashlib.sha256(payload).hexdigest()} {len(payload)} "
                f"{relative_path.encode('utf-8').hex()}\n"
            ).encode("ascii")
        )
        file_count += 1
    digest = hashlib.sha256(b"label-match-code-root-v1\n")
    for entry in sorted(canonical_entries):
        digest.update(entry)
    record_path = app_root / "bootstrap-integrity.json"
    _write_json(
        record_path,
        {
            "schema_version": "label-match-bootstrap-integrity-v2",
            "status": "PASS",
            "code_root": str(app_root.resolve()),
            "file_count": file_count,
            "inventory_algorithm": "sha256-file-hash-size-utf8-path-v1",
            "root_sha256": digest.hexdigest(),
            "package_layout": "onedir",
        },
    )
    return record_path


def _write_bootstrap_inventory_record(app_root: Path) -> Path:
    rows = []
    for path in sorted(
        (candidate for candidate in app_root.rglob("*") if candidate.is_file()),
        key=lambda candidate: candidate.relative_to(app_root).as_posix().casefold(),
    ):
        if path.name.casefold() == "bootstrap-integrity.json":
            continue
        payload = path.read_bytes()
        rows.append(
            {
                "path": path.relative_to(app_root).as_posix(),
                "size": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        )
    aggregate = hashlib.sha256(
        "".join(
            f"{row['sha256']} {row['size']} {row['path']}\n" for row in rows
        ).encode("utf-8")
    ).hexdigest()
    record_path = app_root / "bootstrap-integrity.json"
    _write_json(
        record_path,
        {
            "schema_version": "label-match-bootstrap-integrity-v1",
            "status": "PASS",
            "code_root": str(app_root.resolve()),
            "file_count": len(rows),
            "aggregate_sha256": aggregate,
            "files": rows,
            "identity_profile_created": False,
            "state_scope": "current_user_first_run",
        },
    )
    return record_path


def test_bootstrap_integrity_accepts_installer_inventory_record(tmp_path):
    app_root = (tmp_path / "portable").resolve()
    (app_root / "runtime").mkdir(parents=True)
    (app_root / "app").mkdir()
    (app_root / "runtime" / "pythonw.exe").write_bytes(b"runtime")
    (app_root / "app" / "main.py").write_text("pass\n", encoding="utf-8")
    record_path = _write_bootstrap_inventory_record(app_root)
    paths = resolve_current_user_onboarding_paths(
        app_root, environ=_environment(tmp_path)
    )

    result = verify_bootstrap_integrity(paths, required=True)

    assert result["status"] == "PASS"
    assert result["schema_version"] == "label-match-bootstrap-integrity-v1"
    assert result["package_layout"] == "portable_cpython"
    (app_root / "app" / "main.py").write_text("tampered\n", encoding="utf-8")
    with pytest.raises(ValueError, match="inventory integrity"):
        verify_bootstrap_integrity(paths, required=True)
    assert record_path.is_file()


def test_bootstrap_integrity_requires_exact_onedir_root_hash(tmp_path):
    app_root = tmp_path / "hardened-app"
    internal = app_root / "_internal"
    internal.mkdir(parents=True)
    executable = app_root / "Label_Match.exe"
    runtime = internal / "python312.dll"
    executable.write_bytes(b"main")
    runtime.write_bytes(b"runtime")
    record_path = _write_bootstrap_root_record(app_root)
    paths = resolve_current_user_onboarding_paths(
        app_root, environ=_environment(tmp_path)
    )

    result = verify_bootstrap_integrity(paths, required=True)

    assert result["status"] == "PASS"
    assert result["file_count"] == 2
    assert (
        result["root_sha256"]
        == json.loads(record_path.read_text(encoding="utf-8"))["root_sha256"]
    )
    runtime.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="code root integrity failed"):
        verify_bootstrap_integrity(paths, required=True)


def test_bootstrap_integrity_absent_warns_and_continues(tmp_path):
    app_root = tmp_path / "hardened-app"
    (app_root / "_internal").mkdir(parents=True)
    (app_root / "Label_Match.exe").write_bytes(b"main")
    paths = resolve_current_user_onboarding_paths(
        app_root, environ=_environment(tmp_path)
    )

    result = verify_bootstrap_integrity(paths, required=True)

    assert result["status"] == "ABSENT"
    assert result["warning"] is True
    assert result["record_path"] == str(app_root / "bootstrap-integrity.json")


def test_bootstrap_integrity_non_file_record_fails_closed(tmp_path):
    app_root = tmp_path / "hardened-app"
    (app_root / "_internal").mkdir(parents=True)
    (app_root / "Label_Match.exe").write_bytes(b"main")
    (app_root / "bootstrap-integrity.json").mkdir()
    paths = resolve_current_user_onboarding_paths(
        app_root, environ=_environment(tmp_path)
    )

    with pytest.raises(ValueError, match="not a regular file"):
        verify_bootstrap_integrity(paths, required=True)


def test_bootstrap_integrity_accepts_packaged_relative_code_root(tmp_path):
    app_root = tmp_path / "Downloads" / "Label_Match"
    internal = app_root / "_internal"
    internal.mkdir(parents=True)
    (app_root / "Label_Match.exe").write_bytes(b"main")
    (internal / "python312.dll").write_bytes(b"runtime")
    record_path = _write_bootstrap_root_record(app_root)
    record = json.loads(record_path.read_text(encoding="utf-8"))
    record["code_root"] = "."
    _write_json(record_path, record)
    paths = resolve_current_user_onboarding_paths(
        app_root, environ=_environment(tmp_path)
    )

    result = verify_bootstrap_integrity(paths, required=True)

    assert result["status"] == "PASS"
    assert result["code_root"] == str(app_root.resolve())


@pytest.mark.parametrize("mutation", ["add", "remove", "rename"])
def test_bootstrap_integrity_root_detects_file_set_changes(tmp_path, mutation):
    app_root = tmp_path / "hardened-app"
    internal = app_root / "_internal"
    internal.mkdir(parents=True)
    (app_root / "Label_Match.exe").write_bytes(b"main")
    runtime = internal / "python312.dll"
    runtime.write_bytes(b"runtime")
    _write_bootstrap_root_record(app_root)
    paths = resolve_current_user_onboarding_paths(
        app_root, environ=_environment(tmp_path)
    )

    if mutation == "add":
        (internal / "added.dll").write_bytes(b"added")
    elif mutation == "remove":
        runtime.unlink()
    else:
        runtime.rename(internal / "renamed.dll")

    with pytest.raises(ValueError, match="code root integrity failed"):
        verify_bootstrap_integrity(paths, required=True)


def test_public_remove_clears_relay_but_preserves_identity_profile_and_ledger(
    tmp_path,
):
    app_root = tmp_path / "app"
    app_root.mkdir()
    environment = _environment(tmp_path)
    paths = resolve_current_user_onboarding_paths(app_root, environ=environment)
    _ready_state(paths)
    paths.ledger_path.parent.mkdir(parents=True, exist_ok=True)
    paths.ledger_path.write_bytes(b"preserve")
    events = []

    def completed_step(name):
        events.append(name)
        return {"status": "ABSENT"}

    report = remove_current_user_setup(
        app_root,
        environ=environment,
        autostart_remover=lambda: completed_step("autostart"),
        scheduled_task_remover=lambda _root: completed_step("task-retirement"),
        relay_stopper=lambda _root: completed_step("relay-stopped"),
    )

    assert report["status"] == "PASS_DATA_PRESERVED"
    assert events == ["autostart", "relay-stopped", "task-retirement"]
    assert paths.identity_path.is_file()
    assert paths.logistics_profile_path.is_file()
    assert paths.ledger_path.read_bytes() == b"preserve"


def test_public_remove_does_not_downgrade_unknown_relay_result(tmp_path):
    app_root = tmp_path / "app"
    app_root.mkdir()

    def must_not_retire(_root):
        raise AssertionError("Task retirement preceded proven relay quiescence")
    with pytest.raises(CurrentUserOnboardingError) as caught:
        remove_current_user_setup(
            app_root,
            environ=_environment(tmp_path),
            autostart_remover=lambda: {"status": "ABSENT"},
            scheduled_task_remover=must_not_retire,
            relay_stopper=lambda _root: {"status": "UNKNOWN"},
        )

    assert caught.value.status == "UNKNOWN"


def test_onboarding_product_mode_forwards_server_base_url_argument(monkeypatch, tmp_path):
    import current_user_onboarding
    import label_match_product_host as product_host

    calls = []

    def fake_onboard(app_root, **kwargs):
        calls.append((str(app_root), dict(kwargs)))
        return {"status": "READY", "action": "CREATED"}

    monkeypatch.setattr(current_user_onboarding, "onboard_current_user", fake_onboard)
    app_root = tmp_path / "app"
    app_root.mkdir()
    explicit = "https://isolated.example.invalid:8443"

    assert (
        product_host.dispatch_product_mode(
            [
                "--onboard-current-user",
                "--app-root",
                str(app_root),
                "--server-base-url",
                explicit,
            ]
        )
        == 0
    )
    assert (
        product_host.dispatch_product_mode(
            ["--onboard-current-user", "--app-root", str(app_root)]
        )
        == 0
    )

    assert [call[0] for call in calls] == [str(app_root), str(app_root)]
    assert [call[1]["server_base_url"] for call in calls] == [
        explicit,
        DEFAULT_SERVER_BASE_URL,
    ]
    assert DEFAULT_SERVER_BASE_URL == "https://worker.kmtecherp.com"
    assert all(call[1]["require_bootstrap_integrity"] is False for call in calls)


def test_onboarding_forwards_server_base_url_to_registration_and_keeps_ready_profiles(
    monkeypatch, tmp_path
):
    import tools

    explicit = "https://isolated.example.invalid:8443"
    registrations = []

    def onboard(name, **extra):
        app_root = tmp_path / name / "app"
        app_root.mkdir(parents=True)
        environment = {
            "LABEL_MATCH_SAVE_DIR": str(tmp_path / name / "state" / "data"),
            "LOCALAPPDATA": str(tmp_path / name / "local-app-data"),
        }
        paths = resolve_current_user_onboarding_paths(app_root, environ=environment)

        def register(arguments):
            registrations.append(list(arguments))
            _ready_state(paths)
            return 0

        monkeypatch.setattr(
            tools,
            "register_label_match_worker_pc",
            SimpleNamespace(main=register),
            raising=False,
        )
        kwargs = {
            "environ": environment,
            "require_bootstrap_integrity": False,
            "profile_loader": _profile_loader,
            "credential_loader": _credential_loader,
            "ledger_factory": _ledger_factory,
            "autostart_installer": _autostart,
            "scheduled_task_remover": _scheduled_task_absent,
            "legacy_task_quiescence_reader": _legacy_task_quiescent,
            "relay_launcher": _relay_start,
        }
        report = onboard_current_user(app_root, **kwargs, **extra)
        return report, paths, app_root, kwargs

    first, paths, app_root, kwargs = onboard("explicit", server_base_url=explicit)
    default, _default_paths, _default_root, _default_kwargs = onboard("default")

    assert first["action"] == "CREATED"
    assert default["action"] == "CREATED"
    assert len(registrations) == 2
    explicit_arguments, default_arguments = registrations
    assert explicit_arguments[0] == "--apply"
    assert explicit_arguments.count("--server-base-url") == 1
    assert explicit_arguments[explicit_arguments.index("--server-base-url") + 1] == explicit
    assert explicit_arguments[
        explicit_arguments.index("--logistics-profile-path") + 1
    ] == str(paths.logistics_profile_path)
    assert explicit_arguments[explicit_arguments.index("--credential-path") + 1] == str(
        paths.credential_path
    )
    assert default_arguments[default_arguments.index("--server-base-url") + 1] == (
        DEFAULT_SERVER_BASE_URL
    )

    # An existing READY profile keeps its enrolled endpoint: a later explicit URL
    # must not trigger a second registration.
    rerun = onboard_current_user(
        app_root,
        **kwargs,
        server_base_url="https://other.example.invalid",
        registration_runner=lambda _paths: (_ for _ in ()).throw(
            AssertionError("re-registration attempted")
        ),
    )

    assert rerun["action"] == "REUSED"
    assert len(registrations) == 2

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from current_user_onboarding import (
    CurrentUserOnboardingError,
    ENROLLMENT_TLS_CA_BUNDLE_PATH_ENV,
    _registration_runner,
    inspect_current_user_state,
    onboard_current_user,
    remove_current_user_setup,
    resolve_current_user_onboarding_paths,
    verify_bootstrap_integrity,
)
from direct_sync_push import manifest_hash


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
    return {"status": "START_REQUESTED", "process_id": 123}


def _environment(tmp_path):
    return {"LABEL_MATCH_SAVE_DIR": str(tmp_path / "state" / "data")}


def test_paths_separate_code_and_current_user_state(tmp_path):
    app_root = tmp_path / "hardened-code"
    paths = resolve_current_user_onboarding_paths(
        app_root,
        environ={"LOCALAPPDATA": str(tmp_path / "local-app-data")},
    )

    assert paths.app_root == app_root.resolve()
    assert paths.data_root == (
        tmp_path / "local-app-data" / "KMTech" / "Label_Match" / "data"
    ).resolve()
    assert paths.direct_sync_root == (
        tmp_path / "local-app-data" / "KMTech" / "DirectSync" / "label_match"
    ).resolve()
    assert paths.ledger_path.name == "package_logistics_outbox.sqlite3"
    assert paths.app_root not in paths.data_root.parents


def test_state_absent_partial_and_ready_are_distinguished(tmp_path):
    paths = resolve_current_user_onboarding_paths(
        tmp_path / "app", environ=_environment(tmp_path)
    )
    assert inspect_current_user_state(
        paths,
        profile_loader=_profile_loader,
        credential_loader=_credential_loader,
    )["status"] == "ABSENT"
    _write_json(paths.identity_path, {"source_host_id": "partial"})
    assert inspect_current_user_state(
        paths,
        profile_loader=_profile_loader,
        credential_loader=_credential_loader,
    )["status"] == "RECOVERY_REQUIRED"
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
        "relay_launcher": _relay_start,
    }
    code_files = [path for path in app_root.rglob("*") if path.is_file()]
    code_directories = [internal, app_root]
    code_before = {
        path.relative_to(app_root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
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
            path.relative_to(app_root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
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
    assert environment["LABEL_MATCH_DIRECT_SYNC_ROOT"] == str(paths.direct_sync_root)
    assert code_after == code_before


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

    assert _registration_runner(
        paths, server_base_url="https://worker.example.invalid"
    ) == 0
    assert len(calls) == 1
    arguments = calls[0]
    assert "--source-host-id" not in arguments
    assert arguments[arguments.index("--credential-scope") + 1] == "current_user"
    assert arguments[arguments.index("--identity-path") + 1] == str(
        paths.identity_path
    )


def test_registration_runner_forwards_bootstrap_tls_ca_bundle(tmp_path, monkeypatch):
    app_root = tmp_path / "app"
    app_root.mkdir()
    local_app_data = tmp_path / "LocalAppData"
    environment = {"LOCALAPPDATA": str(local_app_data)}
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

    assert _registration_runner(
        paths,
        server_base_url="https://worker.example.invalid",
        environ=environment,
    ) == 0
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
            relay_launcher=_relay_start,
        )

    assert caught.value.status == "UNKNOWN"
    report = json.loads(caught.value.report_path.read_text(encoding="utf-8"))
    assert report["status"] == "UNKNOWN"


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
    assert result["root_sha256"] == json.loads(
        record_path.read_text(encoding="utf-8")
    )["root_sha256"]
    runtime.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="code root integrity failed"):
        verify_bootstrap_integrity(paths, required=True)


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

    report = remove_current_user_setup(
        app_root,
        environ=environment,
        autostart_remover=lambda: {"status": "ABSENT"},
        relay_stopper=lambda _root: {"status": "ABSENT"},
    )

    assert report["status"] == "PASS_DATA_PRESERVED"
    assert paths.identity_path.is_file()
    assert paths.logistics_profile_path.is_file()
    assert paths.ledger_path.read_bytes() == b"preserve"


def test_public_remove_does_not_downgrade_unknown_relay_result(tmp_path):
    app_root = tmp_path / "app"
    app_root.mkdir()
    with pytest.raises(CurrentUserOnboardingError) as caught:
        remove_current_user_setup(
            app_root,
            environ=_environment(tmp_path),
            autostart_remover=lambda: {"status": "ABSENT"},
            relay_stopper=lambda _root: {"status": "UNKNOWN"},
        )

    assert caught.value.status == "UNKNOWN"

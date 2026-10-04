"""Direct product entry and installer downgrade boundaries, with isolated data."""
from pathlib import Path
import json
import subprocess

import pytest

import Label_Match as app
import current_user_onboarding as onboarding
import protected_admin
import label_data_generation as generation
from kmtech_shared.data_generation import probe_downgrade, record_generation

ROOT = Path(__file__).resolve().parents[1]
SID = "S-1-5-21-900-901-902-1001"
BUILD = "a" * 40
OLD_BUILD = "b" * 40
REAL_INSTALLED_IDENTITY = generation._installed_identity


@pytest.fixture(autouse=True)
def isolated_release_identity(monkeypatch, tmp_path):
    monkeypatch.setenv("KMTECH_LABEL_WRITER_TEST_MODE", "1")
    monkeypatch.setenv("KMTECH_LABEL_WRITER_CONTROL_ROOT", str(tmp_path / "control"))
    monkeypatch.setattr(generation, "_installed_identity", lambda _root: {
        "version": generation.CURRENT_VERSION, "build_id": BUILD,
        "data_compatible_min_version": generation.CURRENT_VERSION,
        "compatibility_verified": False,
    })


def test_direct_product_start_records_before_workflow_and_blocks_old_code(monkeypatch, tmp_path):
    data = tmp_path / "custom-data"
    source = tmp_path / "source"
    source.mkdir()
    monkeypatch.setattr(app, "dispatch_product_mode", lambda _argv: None)
    monkeypatch.setattr(app, "verify_factory_contract_startup", lambda: None)
    monkeypatch.setattr(app, "_label_match_startup_trace", lambda *_a, **_k: None)
    monkeypatch.setattr(app, "_first_run_onboarding_enabled", lambda: False)
    monkeypatch.setattr(app, "_label_match_runtime_app_root", lambda: source)
    monkeypatch.setattr(app, "_default_label_match_settings_path", lambda: tmp_path / "settings.json")
    monkeypatch.setattr(app, "resolve_data_scope", lambda **_kwargs: str(data))
    monkeypatch.setattr(protected_admin, "current_process_sid", lambda: SID)
    monkeypatch.setattr(app, "run_guarded_entrypoint", lambda start, **_kwargs: start())

    def data_manager(_directory, *_args, **_kwargs):
        marker = data / "data-generation.json"
        assert marker.is_file(), "direct executable entry reached business workflow without a generation marker"
        assert json.loads(marker.read_bytes())["version"] == app.APP_VERSION
        from package_logistics import PackageOutbox
        PackageOutbox(data / "package_logistics_outbox.sqlite3")
        assert (data / "package_logistics_outbox.sqlite3").is_file()
        decision = probe_downgrade([marker], "v2.0.38", app_id="Label_Match", subject_sid=SID,
                                  target_build_id=OLD_BUILD)
        assert decision["status"] == "REFUSE"
        assert "옛 판으로 되돌릴 수 없습니다" in decision["message"]
        return 0

    monkeypatch.setattr(app, "DataManager", data_manager)
    monkeypatch.setattr(app, "_run_label_match_application", lambda: app._open_label_data_manager(data))
    assert app.main([]) == 0


@pytest.mark.parametrize("shell", ["powershell.exe", "pwsh.exe"])
@pytest.mark.parametrize("state", ["before_open", "direct_open", "installer_open", "compatible", "corrupt",
                                  "same_build", "same_version_other_build"])
def test_actual_powershell_installer_decision(shell, state, tmp_path):
    control, data = tmp_path / "control.json", tmp_path / "data.json"
    if state != "before_open":
        selected = control if state == "installer_open" else data
        record_generation(selected, "v2.0.95", app_id="Label_Match", subject_sid=SID,
                          data_compatible_min_version="v2.0.38",
                          compatibility_verified=state == "compatible", build_id=BUILD)
    if state == "corrupt":
        data.write_text('{"broken":true}', encoding="utf-8")
    script = tmp_path / "probe.ps1"
    # Definitions only; product start, registry and native installation remain excluded.
    text = (ROOT / "INSTALL_CANONICAL_PORTABLE.ps1").read_text(encoding="utf-8-sig")
    prefix = text[:text.index("if (-not $SourceRoot) { $SourceRoot = $PSScriptRoot }")]
    script.write_text(prefix + "\n" +
        f". '{ROOT.as_posix()}/kmtech_shared/powershell/data_generation.ps1'\n" +
        f"$result = Assert-LabelDataDowngrade -MarkerPaths @('{control.as_posix()}','{data.as_posix()}') " +
        f"-TargetVersion '{'v2.0.95' if state.startswith('same_') else 'v2.0.38'}' -SubjectSid '{SID}' " +
        f"-TargetBuildId '{BUILD if state == 'same_build' else OLD_BUILD}'\n" +
        "$result | ConvertTo-Json -Compress\n", encoding="utf-8-sig")
    completed = subprocess.run([shell, "-NoProfile", "-NonInteractive", "-File", str(script)],
                               capture_output=True, text=True, encoding="utf-8", errors="replace",
                               creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    expected = {"before_open": "ALLOW", "compatible": "ALLOW", "same_build": "ALLOW",
                "corrupt": "UNKNOWN", "same_version_other_build": "UNKNOWN"}.get(state, "REFUSE")
    if expected == "ALLOW":
        assert completed.returncode == 0, completed.stderr[-1800:]
        result = json.loads(completed.stdout.strip().splitlines()[-1])
        assert result["status"] == "ALLOW" and result["allowed"] is True
    else:
        assert completed.returncode != 0
        assert "자료" in completed.stderr
        assert "복구" in completed.stderr


def test_data_generation_is_recorded_before_onboarding_ledger(monkeypatch, tmp_path):
    from tests.test_current_user_onboarding import (
        _autostart, _credential_loader, _environment, _legacy_task_quiescent,
        _profile_loader, _ready_state, _relay_start, _scheduled_task_absent,
    )
    source = tmp_path / "source"
    source.mkdir()
    environment = _environment(tmp_path)
    paths = onboarding.resolve_current_user_onboarding_paths(source, environ=environment)
    _ready_state(paths)
    monkeypatch.setattr(protected_admin, "current_process_sid", lambda: SID)

    def ledger(path):
        marker = paths.data_root / "data-generation.json"
        assert marker.is_file(), "onboarding opened its ledger before recording generation"
        assert json.loads(marker.read_bytes())["version"] == app.APP_VERSION
        onboarding._create_ledger(path)

    report = onboarding.onboard_current_user(source, environ=environment, require_bootstrap_integrity=False,
        profile_loader=_profile_loader, credential_loader=_credential_loader,
        ledger_factory=ledger, autostart_installer=_autostart,
        scheduled_task_remover=_scheduled_task_absent,
        legacy_task_quiescence_reader=_legacy_task_quiescent, relay_launcher=_relay_start)
    assert report["status"] == "READY"


def test_onboarding_marker_write_failure_never_opens_ledger(monkeypatch, tmp_path):
    from tests.test_current_user_onboarding import (
        _autostart, _credential_loader, _environment, _legacy_task_quiescent,
        _profile_loader, _ready_state, _relay_start, _scheduled_task_absent,
    )
    source = tmp_path / "source"
    source.mkdir()
    environment = _environment(tmp_path)
    paths = onboarding.resolve_current_user_onboarding_paths(source, environ=environment)
    _ready_state(paths)
    monkeypatch.setattr(protected_admin, "current_process_sid", lambda: SID)
    opened = []

    def fail_record(*_args, **_kwargs):
        raise OSError("isolated marker persistence failure")

    monkeypatch.setattr(generation, "record_generation", fail_record)
    with pytest.raises(onboarding.CurrentUserOnboardingError, match="자료 판"):
        onboarding.onboard_current_user(source, environ=environment, require_bootstrap_integrity=False,
            profile_loader=_profile_loader, credential_loader=_credential_loader,
            ledger_factory=lambda path: opened.append(path), autostart_installer=_autostart,
            scheduled_task_remover=_scheduled_task_absent,
            legacy_task_quiescence_reader=_legacy_task_quiescent, relay_launcher=_relay_start)
    assert opened == []
    assert not paths.ledger_path.exists()


def test_private_es256_compatibility_is_bound_to_version_build_and_explicit_floor(monkeypatch, tmp_path):
    from tests.test_update_manifest import signed_es256_manifest_payload, valid_manifest
    manifest = valid_manifest()
    manifest.update(version=generation.CURRENT_VERSION, source_commit=BUILD,
                    data_compatible_min_version="v2.0.38")
    public, signature = signed_es256_manifest_payload(manifest)
    proof = generation.verified_private_compatibility(
        manifest, signature, public, version=generation.CURRENT_VERSION, build_id=BUILD)
    monkeypatch.setattr(generation, "_installed_identity", lambda _root: proof)
    monkeypatch.setattr(protected_admin, "current_process_sid", lambda: SID)
    data = tmp_path / "data"
    generation.record_user_data_open(ROOT, (data,), opener=lambda: data.joinpath("business").write_text("opened"))
    markers = generation.marker_paths((data,), SID)
    assert probe_downgrade(markers, "v2.0.38", app_id="Label_Match", subject_sid=SID,
                           target_build_id=OLD_BUILD)["allowed"] is True
    with pytest.raises(ValueError, match="signature"):
        generation.verified_private_compatibility(
            dict(manifest, data_compatible_min_version="v1.0.0"), signature, public,
            version=generation.CURRENT_VERSION, build_id=BUILD)
    with pytest.raises(generation.DataGenerationError):
        generation.verified_private_compatibility(
            manifest, signature, public, version=generation.CURRENT_VERSION, build_id=OLD_BUILD)
    manifest.pop("data_compatible_min_version")
    public, signature = signed_es256_manifest_payload(manifest)
    proof = generation.verified_private_compatibility(
        manifest, signature, public, version=generation.CURRENT_VERSION, build_id=BUILD)
    assert proof["compatibility_verified"] is False
    assert proof["data_compatible_min_version"] == generation.CURRENT_VERSION


@pytest.mark.parametrize("state", ["signed", "tampered", "missing_signature"])
def test_existing_portable_metadata_carries_signed_proof_without_app_additions(monkeypatch, tmp_path, state):
    from tests.test_update_manifest import signed_es256_manifest_payload, valid_manifest
    default = {"version": generation.CURRENT_VERSION, "build_id": BUILD,
               "data_compatible_min_version": generation.CURRENT_VERSION,
               "compatibility_verified": False}
    monkeypatch.setattr(generation, "release_identity", lambda _root: default)
    manifest = valid_manifest()
    manifest.update(version=generation.CURRENT_VERSION, source_commit=BUILD,
                    data_compatible_min_version="v2.0.94")
    public, signature = signed_es256_manifest_payload(manifest)
    monkeypatch.setattr(app, "_get_update_manifest_public_key", lambda: public)
    metadata = {"data_compatible_min_version": "v0.0.0",
                "data_generation_signed_manifest": manifest,
                "data_generation_signature_hex": signature.hex()}
    if state == "tampered":
        manifest["data_compatible_min_version"] = "v2.0.38"
    elif state == "missing_signature":
        metadata.pop("data_generation_signature_hex")
    (tmp_path / "portable-manifest.json").write_text(json.dumps(metadata), encoding="utf-8")
    if state == "signed":
        identity = REAL_INSTALLED_IDENTITY(tmp_path)
        assert identity["compatibility_verified"] is True
        assert identity["data_compatible_min_version"] == "v2.0.94"
        assert sorted(path.name for path in tmp_path.iterdir()) == ["portable-manifest.json"]
    else:
        with pytest.raises((ValueError, generation.DataGenerationError)):
            REAL_INSTALLED_IDENTITY(tmp_path)


@pytest.mark.parametrize("shell", ["powershell.exe", "pwsh.exe"])
def test_failed_launch_does_not_record_generation(shell, tmp_path):
    text = (ROOT / "INSTALL_CANONICAL_PORTABLE.ps1").read_text(encoding="utf-8-sig")
    prefix = text[:text.index("if (-not $SourceRoot) { $SourceRoot = $PSScriptRoot }")]
    script = tmp_path / "launch-failure.ps1"
    script.write_text(prefix + "\nfunction Start-Process { throw 'fixture launch failure' }\n" +
                     f"try {{ Product '{tmp_path.as_posix()}' '--onboard-current-user' }} catch {{ $_.Exception.Message }}\n",
                     encoding="utf-8-sig")
    completed = subprocess.run([shell, "-NoProfile", "-NonInteractive", "-File", str(script)],
                               capture_output=True, text=True, encoding="utf-8", errors="replace",
                               creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    assert completed.returncode == 0
    assert "fixture launch failure" in completed.stdout
    assert not list(tmp_path.rglob("data-generation.json"))


def test_direct_python_executable_uses_product_data_boundary(tmp_path):
    import sys
    script = tmp_path / "direct-product.py"
    data = tmp_path / "data"
    script.write_text(f'''from pathlib import Path
import Label_Match as app
import label_data_generation as generation
import protected_admin
app.dispatch_product_mode = lambda _: None
app.verify_factory_contract_startup = lambda: None
app._label_match_startup_trace = lambda *a, **k: None
app._first_run_onboarding_enabled = lambda: False
app.resolve_data_scope = lambda **k: {str(data)!r}
app.run_guarded_entrypoint = lambda start, **k: start()
generation._installed_identity = lambda _: {{"version": generation.CURRENT_VERSION, "build_id": {BUILD!r}, "data_compatible_min_version": generation.CURRENT_VERSION, "compatibility_verified": False}}
protected_admin.current_process_sid = lambda: {SID!r}
def open_business(root):
    assert Path(root, "data-generation.json").is_file()
    from package_logistics import PackageOutbox
    PackageOutbox(Path(root, "package_logistics_outbox.sqlite3"))
    return 0
app.DataManager = open_business
app._run_label_match_application = lambda: app._open_label_data_manager({str(data)!r})
raise SystemExit(app.main([]))
''', encoding="utf-8")
    completed = subprocess.run([sys.executable, "-B", str(script)], capture_output=True,
                               text=True, timeout=30,
                               creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    assert completed.returncode == 0, completed.stderr[-1800:]
    assert (data / "package_logistics_outbox.sqlite3").is_file()
    assert probe_downgrade([data / "data-generation.json"], "v2.0.38", app_id="Label_Match",
                           subject_sid=SID, target_build_id=OLD_BUILD)["status"] == "REFUSE"

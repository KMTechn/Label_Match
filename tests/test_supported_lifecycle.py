"""Actual lifecycle resolver/publisher and Label default storage boundaries."""
import json
import hashlib
from pathlib import Path
import pytest
from contextlib import nullcontext
import auto_update as update
import current_user_onboarding as onboarding
from tests.test_auto_update import OLD, NEW, NOW, bind_test_packet_settings
from tests.test_supported_removal_convergence import _owned_artifact, _run_removal, _new_stock, tmp_path

CANDIDATE = "3" * 40


def _canonical_user_setup(root, monkeypatch, active=NEW):
    from tests.test_current_user_onboarding import (_environment, _ready_state,
        _write_bootstrap_inventory_record)
    fixed, selected, control, verified = healthy(root, monkeypatch, active=active)
    from label_data_generation import CURRENT_VERSION
    for packet, commit in ((fixed, OLD), (selected, active)):
        (packet / "runtime").mkdir()
        runtime = packet / "runtime/pythonw.exe"
        runtime.write_bytes(b"synthetic-stock-runtime")
        (packet / "app/main.py").write_bytes(
            (Path(__file__).resolve().parents[1] / "portable/main.py").read_bytes())
        installer = packet / "INSTALL_CANONICAL_PORTABLE.ps1"
        installer.write_bytes(b"synthetic-stock-installer")
        (packet / "portable-manifest.json").write_text(json.dumps(dict(
            schema="label-match-portable-tree-v1", entrypoint="runtime/pythonw.exe app/main.py",
            version=CURRENT_VERSION, source_commit=commit, source_tree="b" * 40,
            allowed_unsigned_app_pe=[], forbidden_package_roots=[],
            canonical_installer=installer.name,
            canonical_installer_sha256=hashlib.sha256(installer.read_bytes()).hexdigest(),
            runtime_pythonw_sha256=hashlib.sha256(runtime.read_bytes()).hexdigest())))
        _write_bootstrap_inventory_record(packet)
    monkeypatch.setattr(onboarding, "CANONICAL_PORTABLE_ROOT", fixed)
    environment = _environment(root.parent / "user")
    paths = onboarding.resolve_current_user_onboarding_paths(selected, environ=environment)
    _ready_state(paths)
    paths.ledger_path.parent.mkdir(parents=True, exist_ok=True)
    paths.ledger_path.write_bytes(b"durable quantity=7; UNKNOWN=pending")
    return fixed, selected, environment, paths


def _resume_user(root, environment, *, launcher=None, autostart=None):
    from tests.test_current_user_onboarding import (_profile_loader, _credential_loader,
        _ledger_factory, _autostart, _relay_start, _scheduled_task_absent, _legacy_task_quiescent)
    return onboarding.onboard_current_user(root, environ=environment,
        require_bootstrap_integrity=False, profile_loader=_profile_loader,
        credential_loader=_credential_loader,
        ledger_factory=lambda p: _ledger_factory(p) if not p.exists() else None,
        autostart_installer=autostart or _autostart, relay_launcher=launcher or _relay_start,
        scheduled_task_remover=_scheduled_task_absent,
        legacy_task_quiescence_reader=_legacy_task_quiescent)


@pytest.mark.parametrize("active", [OLD, NEW, CANDIDATE])
@pytest.mark.parametrize("caller", ["owner", "active"])
def test_supported_user_removal_then_registration_resumes_healthy_active(tmp_path, monkeypatch, active, caller):
    from user_relay import user_relay_stop_path
    fixed, selected, environment, paths = _canonical_user_setup(tmp_path / "machine", monkeypatch, active)
    root = fixed if caller == "owner" else selected
    original = {p: p.read_bytes() for p in (paths.identity_path, paths.producer_manifest_path,
        paths.credential_path, paths.logistics_profile_path, paths.ledger_path)}
    removal = onboarding.remove_current_user_setup(root, environ=environment,
        autostart_remover=lambda: {"status": "ABSENT"},
        scheduled_task_remover=lambda _p: {"status": "ABSENT"})
    assert removal["status"] == "PASS_DATA_PRESERVED"
    marker = user_relay_stop_path(paths.direct_sync_root)
    assert marker.is_file()
    report = _resume_user(root, environment)
    assert report["status"] == "READY"
    assert report["stop_marker_release"]["status"] == "SUPPORTED_USER_REMOVAL_PROVEN"
    assert not marker.exists()
    assert all(p.read_bytes() == raw for p, raw in original.items())


@pytest.mark.parametrize("damage", ["missing", "root", "request", "hash", "unknown"])
def test_managed_resume_preserves_marker_when_removal_proof_differs(tmp_path, monkeypatch, damage):
    from user_relay import user_relay_stop_path
    fixed, selected, environment, paths = _canonical_user_setup(tmp_path / "machine", monkeypatch)
    onboarding.remove_current_user_setup(selected, environ=environment,
        autostart_remover=lambda: {"status": "ABSENT"}, scheduled_task_remover=lambda _p: {"status": "ABSENT"})
    marker = user_relay_stop_path(paths.direct_sync_root)
    raw = marker.read_bytes()
    proof = json.loads(paths.removal_report_path.read_text())
    if damage == "missing":
        paths.removal_report_path.unlink()
    else:
        if damage == "root": proof["machine_code_root"] = str(tmp_path / "foreign")
        if damage == "request": proof["relay_process"]["request_id"] = "f" * 32
        if damage == "hash": proof["relay_process"]["stop_request_sha256"] = "f" * 64
        if damage == "unknown": proof["status"] = "UNKNOWN"
        paths.removal_report_path.write_text(json.dumps(proof))
    with pytest.raises(onboarding.CurrentUserOnboardingError):
        _resume_user(fixed, environment)
    assert marker.read_bytes() == raw


def test_managed_resume_rechecks_exact_marker_before_release(tmp_path, monkeypatch):
    from user_relay import request_user_relay_stop, user_relay_stop_path
    fixed, selected, environment, paths = _canonical_user_setup(tmp_path / "machine", monkeypatch)
    onboarding.remove_current_user_setup(fixed, environ=environment,
        autostart_remover=lambda: {"status": "ABSENT"}, scheduled_task_remover=lambda _p: {"status": "ABSENT"})
    def changed(_root):
        request_user_relay_stop(paths.direct_sync_root, timeout_seconds=0)
        return {"status": "PASS"}
    with pytest.raises(onboarding.CurrentUserOnboardingError, match="marker changed"):
        _resume_user(selected, environment, autostart=changed)
    assert user_relay_stop_path(paths.direct_sync_root).is_file()


@pytest.mark.parametrize("active", [OLD, NEW])
def test_managed_conflict_receipt_remains_bound_to_fixed_owner(tmp_path, monkeypatch, active):
    from user_relay import request_user_relay_stop, user_relay_stop_path
    fixed, selected, environment, paths = _canonical_user_setup(tmp_path / "machine", monkeypatch, active)
    stopped = request_user_relay_stop(paths.direct_sync_root, timeout_seconds=0)
    receipt = tmp_path / "receipt.json"
    receipt.write_text('{"status":"RESOLVED"}')
    environment[onboarding.CONFLICT_RECEIPT_PATH_ENV] = str(receipt)
    environment[onboarding.CONFLICT_RECEIPT_SHA256_ENV] = hashlib.sha256(receipt.read_bytes()).hexdigest()
    calls = []
    def validated(_receipt, **kwargs):
        calls.append(kwargs)
        return {"stop_marker_lineage": {"current_request_id": stopped["request_id"],
            "current_sha256": stopped["stop_request_sha256"]}}
    monkeypatch.setattr(onboarding, "validate_resolution_receipt", validated)
    assert _resume_user(selected, environment)["status"] == "READY"
    assert calls[0]["portable_root"] == fixed
    assert calls[0]["allow_portable_relocation"] is True
    assert not user_relay_stop_path(paths.direct_sync_root).exists()


def test_managed_resume_does_not_bypass_bad_conflict_receipt_with_normal_removal(tmp_path, monkeypatch):
    from user_relay import user_relay_stop_path
    fixed, selected, environment, paths = _canonical_user_setup(tmp_path / "machine", monkeypatch)
    onboarding.remove_current_user_setup(fixed, environ=environment,
        autostart_remover=lambda: {"status": "ABSENT"}, scheduled_task_remover=lambda _p: {"status": "ABSENT"})
    receipt = tmp_path / "receipt.json"
    receipt.write_text('{"status":"RESOLVED"}')
    environment[onboarding.CONFLICT_RECEIPT_PATH_ENV] = str(receipt)
    environment[onboarding.CONFLICT_RECEIPT_SHA256_ENV] = "f" * 64
    with pytest.raises(onboarding.CurrentUserOnboardingError, match="receipt pin differs"):
        _resume_user(selected, environment)
    assert user_relay_stop_path(paths.direct_sync_root).exists()


def healthy(root, monkeypatch, active=NEW):
    fixed = root / "current"
    current = root / "versions" / active
    for path, sha in ((fixed, OLD), (current, active)):
        (path / "app").mkdir(parents=True, exist_ok=True)
        (path / "portable-manifest.json").write_text(json.dumps(dict(source_commit=sha)))
        (path / "bootstrap-integrity.json").write_text(json.dumps(dict(aggregate_sha256="a" * 64)))
        (path / "app/update-manifest-key-config.json").write_bytes(b"synthetic-key-configuration")
    control = root / "update-control"; control.mkdir(exist_ok=True)
    policy = {"operator_sid": "S-1-5-21-11-22-33-1001"}; bind_test_packet_settings(control, policy)
    (current / "app/auto-update-settings.json").write_bytes((fixed / "app/auto-update-settings.json").read_bytes())
    for name, value in (("policy.json", policy), ("active.json", dict(schema=update.SCHEMA, sha=active)),
        ("transition.json", dict(schema=update.SCHEMA, phase="DEFERRED", failed_shas=["9" * 40]))):
        (control / name).write_text(json.dumps(value))
    verified = []
    monkeypatch.setattr(update, "fixed_helper", lambda _control, mode, **kw: verified.append((mode, kw.get("code_root"))) or "")
    monkeypatch.setattr(update, "version_manifest", lambda path: update.read_json(path / "portable-manifest.json"))
    monkeypatch.setattr(update, "lifecycle_task_observation", lambda _root: dict(task_exists=True))
    monkeypatch.setattr(update, "update_lock", lambda *_a, **_k: nullcontext())
    return fixed, current, control, verified


@pytest.mark.parametrize("active", [OLD, NEW])
def test_owner_a_and_active_a_or_b_are_healthy_and_support_c(tmp_path, monkeypatch, active):
    import writer_session_fence as fence
    fixed, current, control, verified = healthy(tmp_path, monkeypatch, active)
    before = {p: p.read_bytes() for p in fixed.rglob("*") if p.is_file()}
    context = update.resolve_lifecycle_context(tmp_path, operation="install")
    assert context["from_commit"] == active
    assert context["owner_app_root"] == str(fixed / "app")
    assert context["active_code_root"] == str(current)
    assert [("Verify", fixed), ("Verify", current)] == verified
    candidate = tmp_path / "versions" / CANDIDATE
    (candidate / "app").mkdir(parents=True)
    (candidate / "portable-manifest.json").write_text(json.dumps(dict(source_commit=CANDIDATE)))
    for name in ("update-manifest-key-config.json", "auto-update-settings.json"):
        (candidate / "app" / name).write_bytes((fixed / "app" / name).read_bytes())
    monkeypatch.setattr(update, "process_security", lambda _pid: dict(elevated=True))
    monkeypatch.setattr(fence, "active_fence", lambda *_a, **_k: {"delegated_sources": ["canonical_placement", "auto_update_supported_lifecycle_transition", "auto_update_persist_switch", "auto_update_write_json_durable", "auto_update_fixed_helper"]})
    monkeypatch.setattr(fence, "_delegation_matches", lambda active, **kw: kw["source"] in active["delegated_sources"])
    result = update.supported_lifecycle_transition(tmp_path, expected_from_commit=active, candidate_commit=CANDIDATE)
    assert result["from_commit"] == CANDIDATE and result["bootstrap_commit"] == OLD
    assert update.read_json(control / "active.json")["sha"] == CANDIDATE
    assert update.read_json(control / "transition.json")["failed_shas"] == ["9" * 40]
    assert all(p.read_bytes() == raw for p, raw in before.items())


@pytest.mark.parametrize("phase", ["PREPARED", "APPLIED_PENDING_READY", "ROLLBACK_PENDING", "ROLLBACK_PENDING_READY", "UNKNOWN", "FAILED"])
def test_unsettled_install_refuses_without_mutation_but_removal_has_exit(tmp_path, monkeypatch, phase):
    _, _, control, _ = healthy(tmp_path, monkeypatch)
    (control / "transition.json").write_text(json.dumps(dict(schema=update.SCHEMA, phase=phase)))
    before = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    with pytest.raises(ValueError):
        update.resolve_lifecycle_context(tmp_path, operation="install")
    assert update.resolve_lifecycle_context(tmp_path, operation="uninstall")["lifecycle_state"] == "REMOVAL_READY"
    assert before == {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}


@pytest.mark.parametrize("phase", ["CONFIRMED", "DEFERRED", "PREPARED", "APPLIED_PENDING_READY",
    "ROLLBACK_PENDING", "ROLLBACK_PENDING_READY", "ROLLED_BACK", "UNKNOWN", "FAILED", "absent"])
@pytest.mark.parametrize("damage", ["normal", "missing", "truncated", "mismatch", "directory"])
def test_every_label_phase_and_corrupt_control_has_remove_install_exit(tmp_path, monkeypatch, phase, damage):
    root = tmp_path / "r"
    _owned_artifact(root, "current")
    control = root / "update-control"; control.mkdir()
    for name in ("policy.json", "active.json", "transition.json"):
        path = control / name
        if damage == "missing" or phase == "absent":
            continue
        if damage == "directory":
            path.mkdir(); (path / "foreign-registration").write_bytes(b"unique-registration")
        else:
            path.write_bytes(b'{"cut":' if damage == "truncated" else
                json.dumps(dict(schema=update.SCHEMA, phase=phase, sha=OLD if damage == "normal" else NEW)).encode())
    data = root / "user-data/pending"; data.parent.mkdir(); data.write_bytes(b"quantity=7;pending=UNKNOWN")
    unknown = control / "unknown-credential-bytes"; unknown.write_bytes(b"unique-synthetic-identity")
    retained = {p: p.read_bytes() for p in (data, unknown)}
    for path in control.glob("*/foreign-registration"):
        retained[path] = path.read_bytes()
    assert _run_removal(root, tmp_path, install=False)["status"] == "PASS"
    assert update.read_json(root / "removed.json")["lifecycle_state"] == "REMOVED"
    monkeypatch.setattr(update, "lifecycle_task_observation", lambda _root: dict(task_exists=False))
    if damage == "directory" and phase != "absent":
        with pytest.raises(ValueError):
            update.resolve_lifecycle_context(root, operation="install")
    else:
        assert update.resolve_lifecycle_context(root, operation="install")["lifecycle_state"] == "REMOVED"
        _new_stock(root)
        assert _run_removal(root, tmp_path)["installed"] is True
        assert update.read_json(root / "current/portable-manifest.json")["source_commit"] == NEW
    assert all(path.read_bytes() == raw for path, raw in retained.items())


@pytest.mark.parametrize("operation", ["uninstall", "user_remove"])
def test_removal_never_reads_control(tmp_path, monkeypatch, operation):
    monkeypatch.setattr(update, "read_json", lambda *_a, **_k: pytest.fail("removal read control"))
    assert update.resolve_lifecycle_context(tmp_path, operation=operation)["lifecycle_state"] == "REMOVAL_READY"


@pytest.mark.parametrize("slot", ["current", "versions", "update-control", "removal-evidence", "removed.json"])
def test_new_registration_rejects_machine_storage_before_writing(tmp_path, monkeypatch, slot):
    app = tmp_path / "machine/current"
    (app / "config").mkdir(parents=True)
    local = tmp_path / "user"; local.mkdir()
    environment = {"LOCALAPPDATA": str(local), "LABEL_MATCH_SAVE_DIR": str(tmp_path / "machine" / slot / "data")}
    with pytest.raises(ValueError, match="겹칩니다"):
        onboarding.onboard_current_user(app, environ=environment)
    assert not (local / "KMTech").exists()


def test_normal_label_default_data_is_outside_machine_code(tmp_path):
    app = tmp_path / "machine/current"
    local = tmp_path / "user"
    paths = onboarding.resolve_current_user_onboarding_paths(app, environ={"LOCALAPPDATA": str(local)})
    assert paths.data_root == local / "KMTech/Label_Match/data"
    assert not paths.data_root.is_relative_to(app.parent)


@pytest.mark.parametrize("slot", ["current", "versions", "update-control", "removal-evidence", "removed.json"])
def test_direct_registration_refuses_machine_storage_before_identity_or_report(tmp_path, monkeypatch, slot):
    from tools import register_label_match_worker_pc as registration
    from argparse import Namespace
    app = tmp_path / "machine/current/app"; app.mkdir(parents=True)
    monkeypatch.setattr(registration, "ROOT", app)
    monkeypatch.setattr(registration, "_derive_identity", lambda *_a: pytest.fail("identity touched"))
    args = Namespace(data_dir=str(app.parent.parent / slot / "data"), sync_dir=str(tmp_path / "user"))
    with pytest.raises(ValueError, match="겹칩니다"):
        registration.build_payloads(args)
    assert registration.main(["--dry-run", "--data-dir", args.data_dir, "--sync-dir", args.sync_dir]) == 2
    assert not (app.parent.parent / slot / "data").exists()


def test_direct_registration_defaults_are_outside_canonical_code(monkeypatch):
    from tools import register_label_match_worker_pc as registration
    from argparse import Namespace
    monkeypatch.setattr(update, "_assert_no_reparse_components", lambda *_a: None)
    registration._registration_storage_preflight(Namespace())


def test_actual_user_removal_ignores_corrupt_machine_control_and_preserves_data(tmp_path, monkeypatch):
    from tests.test_current_user_onboarding import _environment, _ready_state
    app = tmp_path / "machine/current"; app.mkdir(parents=True)
    control = app.parent / "update-control"; control.mkdir()
    (control / "policy.json").write_bytes(b'{"cut":')
    environment = _environment(tmp_path)
    paths = onboarding.resolve_current_user_onboarding_paths(app, environ=environment)
    _ready_state(paths)
    paths.ledger_path.parent.mkdir(parents=True, exist_ok=True)
    paths.ledger_path.write_bytes(b"quantity=7;pending=UNKNOWN")
    before = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    monkeypatch.setattr(update, "load_policy", lambda *_a: pytest.fail("removal read policy"))
    result = onboarding.remove_current_user_setup(app, environ=environment,
        autostart_remover=lambda: {"status": "ABSENT"},
        relay_stopper=lambda *_a: {"status": "ABSENT"},
        scheduled_task_remover=lambda *_a: {"status": "ABSENT"})
    assert result["status"] == "PASS_DATA_PRESERVED"
    assert all(path.read_bytes() == raw for path, raw in before.items())

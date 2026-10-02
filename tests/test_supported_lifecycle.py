"""Actual lifecycle resolver/publisher and Label default storage boundaries."""
import json
from pathlib import Path
import pytest
from contextlib import nullcontext
import auto_update as update
import current_user_onboarding as onboarding
from tests.test_auto_update import OLD, NEW, NOW, bind_test_packet_settings

CANDIDATE = "3" * 40


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

"""The first same-key rotation must recover through the next official start."""
import json
from pathlib import Path
import subprocess
import sys
import uuid

import pytest
import current_user_onboarding as onboarding
from tests._review467_first_rotation_child import prepare_case
from tests.test_current_user_onboarding import (
    _environment, _profile_loader, _credential_loader, _ledger_factory,
    _autostart, _relay_start, _scheduled_task_absent, _legacy_task_quiescent,
)


@pytest.mark.parametrize('mode', ['producer_io', 'response_exit', 'diagnostic_after', 'marker_before'])
def test_first_rotation_failure_then_next_official_start_is_ready(tmp_path, monkeypatch, mode):
    root = tmp_path / 'fresh'
    root.mkdir()
    (root / 'fixture-context.json').write_text(json.dumps({
        'machine_guid': uuid.uuid4().hex, 'pc_name': 'review-' + uuid.uuid4().hex[:12],
    }))
    child = subprocess.run([
        sys.executable, '-B', str(Path(__file__).with_name('_review467_first_rotation_child.py')),
        str(root), mode,
    ], capture_output=True, cwd=Path(__file__).resolve().parents[1],
       creationflags=subprocess.CREATE_NO_WINDOW)
    assert child.returncode == 0
    first = json.loads((root / 'first-phase.json').read_text())
    assert first['had_normal_report_before'] is False
    assert first['had_receipt_before'] is False
    assert first['had_identity_before'] is False
    assert first['retained_key_reused'] is True
    paths_before = onboarding.resolve_current_user_onboarding_paths(root / 'app', environ=_environment(root))
    protected_paths = [paths_before.identity_path, paths_before.producer_manifest_path,
        paths_before.credential_path, paths_before.registration_report_path,
        paths_before.registration_receipt_path, paths_before.logistics_profile_path,
        paths_before.logistics_secret_path,
        paths_before.direct_sync_root / 'status' / 'label_match_reattach_persistence.json']
    preimages = {p: p.read_bytes() if p.exists() else None for p in protected_paths}
    case, paths, server_path, base_url = prepare_case(root, monkeypatch)
    preparation_preserved_product_files = all(
        (p.read_bytes() if p.exists() else None) == original for p, original in preimages.items()
    )
    assert preparation_preserved_product_files
    import tools.register_label_match_worker_pc as official_registration
    runs = []
    def registered(argv, **kwargs):
        runs.append(True)
        return case.module.main(argv, **kwargs)
    monkeypatch.setattr(official_registration, 'main', registered)
    state = onboarding.inspect_current_user_state(paths)
    completed = False
    failure = None
    try:
        result = onboarding.onboard_current_user(
            root / 'app', environ=_environment(root), server_base_url=base_url,
            require_bootstrap_integrity=False, profile_loader=_profile_loader,
            credential_loader=_credential_loader, ledger_factory=_ledger_factory,
            autostart_installer=_autostart, scheduled_task_remover=_scheduled_task_absent,
            legacy_task_quiescence_reader=_legacy_task_quiescent, relay_launcher=_relay_start,
        )
        completed = result['status'] == 'READY'
    except onboarding.CurrentUserOnboardingError as exc:
        failure = {'status': exc.status, 'cause_code': exc.cause_code}
    observation = {
        'first': first, 'state_before_official_start': state['status'],
        'next_official_start_ready': completed, 'failure': failure,
        'official_registration_runner_calls': len(runs),
        'next_start_http_calls': len(case.calls),
        'server_epoch_after': json.loads(server_path.read_text())['epoch'],
        'parent_preparation_preserved_product_files': preparation_preserved_product_files,
    }
    (tmp_path / 'first-rotation-observation.json').write_text(json.dumps(observation))
    assert completed, 'first rotation did not recover through the next official start'


@pytest.mark.parametrize("mode", ["producer_io", "marker_before"])
@pytest.mark.parametrize("mismatch", ["key", "identity", "unavailable"])
def test_first_rotation_unbound_retry_is_recovery_without_http(tmp_path, monkeypatch, mode, mismatch):
    root = tmp_path / 'fresh'
    root.mkdir()
    (root / 'fixture-context.json').write_text(json.dumps({
        'machine_guid': uuid.uuid4().hex, 'pc_name': 'review-' + uuid.uuid4().hex[:12],
    }))
    child = subprocess.run([
        sys.executable, '-B', str(Path(__file__).with_name('_review467_first_rotation_child.py')),
        str(root), mode,
    ], capture_output=True, cwd=Path(__file__).resolve().parents[1],
       creationflags=subprocess.CREATE_NO_WINDOW)
    assert child.returncode == 0
    first = json.loads((root / 'first-phase.json').read_text())
    assert first['had_normal_report_before'] is False
    assert first['had_receipt_before'] is False
    assert first['had_identity_before'] is False
    assert first['retained_key_reused'] is True
    paths_before = onboarding.resolve_current_user_onboarding_paths(root / 'app', environ=_environment(root))
    protected_paths = [paths_before.identity_path, paths_before.producer_manifest_path,
        paths_before.credential_path, paths_before.registration_report_path,
        paths_before.registration_receipt_path, paths_before.logistics_profile_path,
        paths_before.logistics_secret_path,
        paths_before.direct_sync_root / 'status' / 'label_match_reattach_persistence.json']
    preimages = {p: p.read_bytes() if p.exists() else None for p in protected_paths}
    case, paths, server_path, base_url = prepare_case(root, monkeypatch)
    preparation_preserved_product_files = all(
        (p.read_bytes() if p.exists() else None) == original for p, original in preimages.items()
    )
    assert preparation_preserved_product_files
    import tools.register_label_match_worker_pc as official_registration
    runs = []
    def registered(argv, **kwargs):
        runs.append(True)
        return case.module.main(argv, **kwargs)
    monkeypatch.setattr(official_registration, 'main', registered)

    marker = paths.direct_sync_root / "status" / case.module.REATTACH_PERSISTENCE_FILENAME
    target = marker if marker.exists() else paths.registration_report_path
    if mismatch == "unavailable":
        def unavailable(**_kwargs):
            raise RuntimeError("existing key unavailable")
        monkeypatch.setattr(case.module.PersistentPossessionKey, "open_existing", unavailable)
    else:
        payload = json.loads(target.read_text())
        field = "possession_key_fingerprint" if mismatch == "key" else "producer_install_id"
        payload[field] = uuid.uuid4().hex
        target.write_text(json.dumps(payload))
    original = {p: p.read_bytes() if p.exists() else None for p in protected_paths}
    state = onboarding.inspect_current_user_state(paths)
    assert state["status"] == "RECOVERY_REQUIRED"
    with pytest.raises(onboarding.CurrentUserOnboardingError) as failure:
        onboarding.onboard_current_user(
            root / 'app', environ=_environment(root), server_base_url=base_url,
            require_bootstrap_integrity=False, profile_loader=_profile_loader,
            credential_loader=_credential_loader, ledger_factory=_ledger_factory,
            autostart_installer=_autostart, scheduled_task_remover=_scheduled_task_absent,
            legacy_task_quiescence_reader=_legacy_task_quiescent, relay_launcher=_relay_start,
        )
    assert runs == []
    assert case.calls == []
    assert all((p.read_bytes() if p.exists() else None) == before for p, before in original.items())


def test_first_report_retry_opens_existing_key_without_provisioning(tmp_path, monkeypatch):
    mode = "marker_before"
    root = tmp_path / 'fresh'
    root.mkdir()
    (root / 'fixture-context.json').write_text(json.dumps({
        'machine_guid': uuid.uuid4().hex, 'pc_name': 'review-' + uuid.uuid4().hex[:12],
    }))
    child = subprocess.run([
        sys.executable, '-B', str(Path(__file__).with_name('_review467_first_rotation_child.py')),
        str(root), mode,
    ], capture_output=True, cwd=Path(__file__).resolve().parents[1],
       creationflags=subprocess.CREATE_NO_WINDOW)
    assert child.returncode == 0
    first = json.loads((root / 'first-phase.json').read_text())
    assert first['had_normal_report_before'] is False
    assert first['had_receipt_before'] is False
    assert first['had_identity_before'] is False
    assert first['retained_key_reused'] is True
    paths_before = onboarding.resolve_current_user_onboarding_paths(root / 'app', environ=_environment(root))
    protected_paths = [paths_before.identity_path, paths_before.producer_manifest_path,
        paths_before.credential_path, paths_before.registration_report_path,
        paths_before.registration_receipt_path, paths_before.logistics_profile_path,
        paths_before.logistics_secret_path,
        paths_before.direct_sync_root / 'status' / 'label_match_reattach_persistence.json']
    preimages = {p: p.read_bytes() if p.exists() else None for p in protected_paths}
    case, paths, server_path, base_url = prepare_case(root, monkeypatch)
    preparation_preserved_product_files = all(
        (p.read_bytes() if p.exists() else None) == original for p, original in preimages.items()
    )
    assert preparation_preserved_product_files
    import tools.register_label_match_worker_pc as official_registration
    runs = []
    def registered(argv, **kwargs):
        runs.append(True)
        return case.module.main(argv, **kwargs)
    monkeypatch.setattr(official_registration, 'main', registered)
    def no_provisioning(**_kwargs):
        pytest.fail("retry must open the existing key")
    monkeypatch.setattr(case.module.PersistentPossessionKey, "provision_initial", no_provisioning)
    state = onboarding.inspect_current_user_state(paths)
    completed = False
    failure = None
    try:
        result = onboarding.onboard_current_user(
            root / 'app', environ=_environment(root), server_base_url=base_url,
            require_bootstrap_integrity=False, profile_loader=_profile_loader,
            credential_loader=_credential_loader, ledger_factory=_ledger_factory,
            autostart_installer=_autostart, scheduled_task_remover=_scheduled_task_absent,
            legacy_task_quiescence_reader=_legacy_task_quiescent, relay_launcher=_relay_start,
        )
        completed = result['status'] == 'READY'
    except onboarding.CurrentUserOnboardingError as exc:
        failure = {'status': exc.status, 'cause_code': exc.cause_code}
    observation = {
        'first': first, 'state_before_official_start': state['status'],
        'next_official_start_ready': completed, 'failure': failure,
        'official_registration_runner_calls': len(runs),
        'next_start_http_calls': len(case.calls),
        'server_epoch_after': json.loads(server_path.read_text())['epoch'],
        'parent_preparation_preserved_product_files': preparation_preserved_product_files,
    }
    (tmp_path / 'first-rotation-observation.json').write_text(json.dumps(observation))
    assert completed, 'first rotation did not recover through the next official start'

    assert len(runs) == 1
    assert len(case.calls) == 3


@pytest.mark.parametrize("mode", ["producer_io", "response_exit", "diagnostic_after"])
def test_first_rotation_marker_preflight_accepts_the_same_existing_key(tmp_path, monkeypatch, mode):
    root = tmp_path / 'fresh'
    root.mkdir()
    (root / 'fixture-context.json').write_text(json.dumps({
        'machine_guid': uuid.uuid4().hex, 'pc_name': 'review-' + uuid.uuid4().hex[:12],
    }))
    child = subprocess.run([
        sys.executable, '-B', str(Path(__file__).with_name('_review467_first_rotation_child.py')),
        str(root), mode,
    ], capture_output=True, cwd=Path(__file__).resolve().parents[1],
       creationflags=subprocess.CREATE_NO_WINDOW)
    assert child.returncode == 0
    first = json.loads((root / 'first-phase.json').read_text())
    assert first['had_normal_report_before'] is False
    assert first['had_receipt_before'] is False
    assert first['had_identity_before'] is False
    assert first['retained_key_reused'] is True
    paths_before = onboarding.resolve_current_user_onboarding_paths(root / 'app', environ=_environment(root))
    protected_paths = [paths_before.identity_path, paths_before.producer_manifest_path,
        paths_before.credential_path, paths_before.registration_report_path,
        paths_before.registration_receipt_path, paths_before.logistics_profile_path,
        paths_before.logistics_secret_path,
        paths_before.direct_sync_root / 'status' / 'label_match_reattach_persistence.json']
    preimages = {p: p.read_bytes() if p.exists() else None for p in protected_paths}
    case, paths, server_path, base_url = prepare_case(root, monkeypatch)
    preparation_preserved_product_files = all(
        (p.read_bytes() if p.exists() else None) == original for p, original in preimages.items()
    )
    assert preparation_preserved_product_files
    import tools.register_label_match_worker_pc as official_registration
    runs = []
    def registered(argv, **kwargs):
        runs.append(True)
        return case.module.main(argv, **kwargs)
    monkeypatch.setattr(official_registration, 'main', registered)

    def no_provisioning(**_kwargs):
        pytest.fail("a bound rotation retry must not provision a key")
    monkeypatch.setattr(case.module.PersistentPossessionKey, "provision_initial", no_provisioning)
    assert case.run() == 0
    assert len(case.calls) == 3
    assert case.report()["status"] == "SELF_ENROLLMENT_REGISTERED"
    assert not (paths.direct_sync_root / "status" / case.module.REATTACH_PERSISTENCE_FILENAME).exists()

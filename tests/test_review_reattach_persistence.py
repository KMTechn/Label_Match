"""An existing local target must not cause failure after remote rotation."""
import json
from types import SimpleNamespace
import current_user_onboarding as onboarding
from tests.test_register_label_match_reattach import reattach_case
import pytest
import tests.test_register_label_match_reattach as reattach_tests
from pathlib import Path


@pytest.fixture(autouse=True)
def no_git_child(monkeypatch):
    monkeypatch.setattr(reattach_tests, '_source_build_id',
                        lambda: 'b468c5d8ce62e5dac77fd5c1454157dbabb11cab')


def _preserved_binding(case):
    assert case.run() == 0
    saved = json.loads((case.data / case.module.DEFAULT_CREDENTIAL_FILENAME).read_text())
    target = case.module._secret_path(case.data, saved['secret_ref'].split(':', 1)[1])
    target.parent.mkdir(exist_ok=True)
    _install_test_producer_crypto(case)
    target.write_bytes(case.module._dpapi_protect_current_user(case.secret))
    case.original_protected = target.read_bytes()
    case.calls.clear()
    previous_epoch = case.report()['credential_epoch']
    response_change = case.response_change
    def next_generation(response):
        response_change(response)
        response['credential_epoch'] = previous_epoch + 1
        response['client_receipt']['credential_epoch'] = previous_epoch + 1
    case.response_change = next_generation
    case.secret = __import__('secrets').token_hex(24)
    report = case.data / 'status' / case.module.DEFAULT_REPORT_FILENAME
    return target, report, report.read_bytes()


def _install_test_producer_crypto(case):
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    material_path = case.data / 'test-credential-material.bin'
    if not material_path.exists():
        material_path.write_bytes(AESGCM.generate_key(bit_length=256))
    cipher = AESGCM(material_path.read_bytes())
    def protect(value):
        nonce = __import__('secrets').token_bytes(12)
        return nonce + cipher.encrypt(nonce, value.encode(), None)
    case.module._dpapi_protect_current_user = protect
    case.module._dpapi_unprotect_current_user = lambda raw: cipher.decrypt(raw[:12], raw[12:], None).decode()
    from tests.test_register_label_match_worker_pc import load_registration_module
    from types import FunctionType
    real = load_registration_module()
    for name in ('_write_dpapi_secret', '_verify_dpapi_secret'):
        function = getattr(real, name)
        setattr(case.module, name, FunctionType(function.__code__, case.module.__dict__,
                                               function.__name__, function.__defaults__, function.__closure__))
        getattr(case.module, name).__kwdefaults__ = function.__kwdefaults__


def test_retained_binding_finishes_rotation_in_existing_slot(reattach_case):
    case = reattach_case
    _preserved_binding(case)
    assert case.run() == 0
    assert len(case.calls) == 3
    assert case.report()['registration_action'] == 'possession_reattach'
    assert case.module._secret_path(case.data, json.loads((case.data / case.module.DEFAULT_CREDENTIAL_FILENAME).read_text())['secret_ref'].split(':', 1)[1]).read_bytes() != case.original_protected
    assert json.loads((case.data / case.module.DEFAULT_CREDENTIAL_FILENAME).read_text())['key_id'] == case.response['key_id']


def _use_real_profile(case, monkeypatch, profile=None):
    from tests.test_logistics_runtime_profile import _machine_enrollment_response, _fake_current_user_dpapi
    from tools import install_logistics_runtime_profile as installer
    _fake_current_user_dpapi(monkeypatch)
    monkeypatch.setattr(installer, '_secure_profile_directory', lambda *_args: None)
    profile = profile or case.data / 'logistics' / 'runtime-profile.json'
    case.args += ['--logistics-profile-path', str(profile), '--require-machine-credential-bundle']
    def bundle(response):
        value = _machine_enrollment_response(10, logistics_token=__import__('secrets').token_hex(24))['machine_credential_bundle']
        bindings = value['bindings']
        bindings.update(app='LabelMatch', program='Label_Match',
                        source_host_id=case.identity['source_host_id'], device_id=case.identity['pc_id'])
        value['profiles']['logistics'].update(
            source_host_id=case.identity['source_host_id'], device_id=case.identity['pc_id'],
            base_url=response['endpoint_url'].split('/api/', 1)[0])
        value['credentials']['producer_ingest'].update(key_id=response['key_id'], secret=response['secret'])
        response['machine_credential_bundle'] = value
    case.response_change = bundle
    return profile, profile.parent / installer.DEFAULT_TOKEN_REF.split(':', 1)[1]


def test_profile_and_producer_rotate_together_in_retained_slots(reattach_case, monkeypatch):
    case = reattach_case
    profile, token_path = _use_real_profile(case, monkeypatch)
    target, _, _ = _preserved_binding(case)
    before_profile, before_token = profile.read_bytes(), token_path.read_bytes()
    assert case.run() == 0
    assert profile.read_bytes() == before_profile
    assert token_path.read_bytes() != before_token
    assert target.read_bytes() != case.original_protected
    assert not list(case.data.rglob('*.tmp'))


def test_producer_failure_restores_rotated_profile_token(reattach_case, monkeypatch):
    case = reattach_case
    profile, token_path = _use_real_profile(case, monkeypatch)
    target, report, original = _preserved_binding(case)
    before_profile, before_token = profile.read_bytes(), token_path.read_bytes()
    def denied(*_args, **_kwargs):
        raise OSError()
    monkeypatch.setattr(case.module, '_write_dpapi_secret', denied)
    assert case.run() == 3
    assert report.read_bytes() == original
    assert profile.read_bytes() == before_profile
    assert token_path.read_bytes() == before_token
    assert target.read_bytes() == case.original_protected


def test_other_producer_is_rejected_before_any_server_request(reattach_case):
    case = reattach_case
    target, report, original = _preserved_binding(case)
    credential = case.data / case.module.DEFAULT_CREDENTIAL_FILENAME
    payload = json.loads(credential.read_text())
    payload['producer_id'] = __import__('uuid').uuid4().hex
    credential.write_text(json.dumps(payload))
    assert case.run() == 2
    assert case.calls == []
    assert report.read_bytes() == original
    assert target.read_bytes() == case.original_protected


def test_unwritable_preflight_preserves_ready_report_without_server_calls(reattach_case, monkeypatch):
    case = reattach_case
    target, report, original = _preserved_binding(case)
    def denied(*_args, **_kwargs):
        raise PermissionError()
    monkeypatch.setattr(case.module, '_probe_reattach_target', denied, raising=False)
    assert case.run() == 2
    assert case.calls == []
    assert report.read_bytes() == original
    assert target.read_bytes() == case.original_protected


def test_post_rotation_write_failure_preserves_report_and_allows_retry(reattach_case, monkeypatch):
    case = reattach_case
    target, report, original = _preserved_binding(case)
    writer = case.module._write_dpapi_secret
    def denied(*_args, **_kwargs):
        raise OSError()
    monkeypatch.setattr(case.module, '_write_dpapi_secret', denied)
    assert case.run() == 3
    assert len(case.calls) == 3
    assert report.read_bytes() == original
    assert target.read_bytes() == case.original_protected
    monkeypatch.setattr(case.module, '_write_dpapi_secret', writer)
    case.calls.clear()
    assert case.run() == 0
    assert len(case.calls) == 3


def _retarget_to_official_paths(case, paths):
    case.data = paths.direct_sync_root
    case.data.mkdir(parents=True, exist_ok=True)
    if not paths.identity_path.exists():
        paths.identity_path.write_bytes(case.original_identity)
    case.identity_path = paths.identity_path
    case.identity = json.loads(paths.identity_path.read_text())
    case.original_identity = paths.identity_path.read_bytes()
    for option, value in (('--data-dir', paths.direct_sync_root), ('--sync-dir', paths.data_root)):
        case.args[case.args.index(option) + 1] = str(value)
    case.report = lambda: json.loads(paths.registration_report_path.read_text())


def test_process_exit_then_official_onboarding_reattaches_same_key_to_ready(
    reattach_case, tmp_path, monkeypatch,
):
    import subprocess
    import sys
    from tests.test_current_user_onboarding import (
        _environment, _profile_loader, _credential_loader, _ledger_factory,
        _autostart, _relay_start, _scheduled_task_absent, _legacy_task_quiescent,
    )
    root = tmp_path / 'restart-scenario'
    child = subprocess.run(
        [sys.executable, '-B', str(Path(__file__).with_name('_reattach_restart_child.py')), str(root)],
        cwd=Path(__file__).resolve().parents[1], capture_output=True,
        creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0),
    )
    assert child.returncode == 0
    result = json.loads((root / 'child-result.json').read_text())
    assert result['failed_registration_exit'] != 0
    assert result['server_rotated'] is True
    server_path = root / 'server-state.json'
    assert json.loads(server_path.read_text())['retired_generations'] == 1
    case = reattach_case
    environ = _environment(root)
    paths = onboarding.resolve_current_user_onboarding_paths(root / 'app', environ=environ)
    _retarget_to_official_paths(case, paths)
    _install_test_producer_crypto(case)
    _use_real_profile(case, monkeypatch, paths.logistics_profile_path)
    include_bundle = case.response_change
    def next_generation(response):
        include_bundle(response)
        state = json.loads(server_path.read_text())
        state['epoch'] += 1
        state['retired_generations'] += 1
        server_path.write_text(json.dumps(state))
        response['credential_epoch'] = state['epoch']
        response['client_receipt']['credential_epoch'] = state['epoch']
    case.response_change = next_generation
    import tools.register_label_match_worker_pc as official_registration
    runs = []
    def registered(argv, **kwargs):
        runs.append(True)
        return case.module.main(argv, **kwargs)
    monkeypatch.setattr(official_registration, 'main', registered)
    report = onboarding.onboard_current_user(
        root / 'app', environ=environ,
        server_base_url=case.report()['endpoint_url'].split('/api/', 1)[0],
        require_bootstrap_integrity=False,
        profile_loader=_profile_loader, credential_loader=_credential_loader,
        ledger_factory=_ledger_factory, autostart_installer=_autostart,
        scheduled_task_remover=_scheduled_task_absent,
        legacy_task_quiescence_reader=_legacy_task_quiescent, relay_launcher=_relay_start,
    )
    assert report['status'] == 'READY'
    assert runs == [True]
    assert len(case.calls) == 3
    assert case.initial_key_calls == []
    assert case.signed[0]['producer_id'] == case.identity['producer_id']
    assert json.loads(server_path.read_text())['retired_generations'] == 2


def test_existing_binding_is_not_invalidated_before_local_storage_rejection(reattach_case, tmp_path, monkeypatch):
    case = reattach_case
    module = case.module
    assert case.run() == 0
    credential_path = case.data / module.DEFAULT_CREDENTIAL_FILENAME
    before_credential = credential_path.read_bytes()
    old_key_id = json.loads(before_credential)['key_id']
    target = module._secret_path(
        case.data, 'producer-' + case.identity['source_host_id'] + '-http-push-key'
    )
    target.parent.mkdir()
    target.write_bytes(b'opaque-protected-preimage')
    profile = tmp_path / 'runtime-profile.json'
    profile.write_text(json.dumps({'credential_scope': 'current_user'}), encoding='utf-8')
    profile_secret = tmp_path / 'profile-credential.dpapi'
    profile_secret.write_bytes(b'opaque-profile-preimage')
    paths = SimpleNamespace(
        identity_path=case.identity_path,
        producer_manifest_path=case.data / module.DEFAULT_MANIFEST_FILENAME,
        credential_path=credential_path,
        registration_report_path=case.data / 'status' / module.DEFAULT_REPORT_FILENAME,
        logistics_profile_path=profile,
        logistics_secret_path=profile_secret,
    )
    monkeypatch.setattr(onboarding, 'require_install_server_origin_matches', lambda *_args, **_kwargs: None)
    def startup_state():
        return onboarding.inspect_current_user_state(
            paths,
            credential_loader=lambda _path: object(),
            profile_loader=lambda _path: SimpleNamespace(
                source_host_id=case.identity['source_host_id'], authority_plane='AUTHORITATIVE'
            ),
        )['status']
    before_state = startup_state()
    assert before_state == 'READY'
    case.calls.clear()
    result = case.run()
    rotation_completed = any(url.endswith('/api/producer-ingest/v2/reattach') for url, _ in case.calls)
    summary = {
        'initial_registration_succeeded': True,
        'second_registration_exit': result,
        'second_server_rotation_completed': rotation_completed,
        'server_issued_different_key_id': rotation_completed and old_key_id != case.response['key_id'],
        'local_credential_reference_unchanged': credential_path.read_bytes() == before_credential,
        'protected_preimage_unchanged': target.read_bytes() == b'opaque-protected-preimage',
        'report_status': case.report()['status'],
        'report_marks_remote_rotation': case.report().get('server_credential_rotated') is True,
        'startup_state_before': before_state,
        'startup_state_after': startup_state(),
    }
    (tmp_path / 'persistence-observation.json').write_text(json.dumps(summary), encoding='utf-8')
    post_rotation_persistence_failure = (
        result != 0 and summary['server_issued_different_key_id']
        and summary['local_credential_reference_unchanged']
    )
    assert not post_rotation_persistence_failure
    assert startup_state() == 'READY'

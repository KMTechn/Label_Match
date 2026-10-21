"""An existing local target must not cause failure after remote rotation."""
import json
from types import SimpleNamespace
import current_user_onboarding as onboarding
from tests.test_register_label_match_reattach import reattach_case
import pytest
import tests.test_register_label_match_reattach as reattach_tests


@pytest.fixture(autouse=True)
def no_git_child(monkeypatch):
    monkeypatch.setattr(reattach_tests, '_source_build_id',
                        lambda: 'b468c5d8ce62e5dac77fd5c1454157dbabb11cab')


def _preserved_binding(case):
    assert case.run() == 0
    saved = json.loads((case.data / case.module.DEFAULT_CREDENTIAL_FILENAME).read_text())
    target = case.module._secret_path(case.data, saved['secret_ref'].split(':', 1)[1])
    target.parent.mkdir(exist_ok=True)
    target.write_bytes(b'protected-existing')
    case.calls.clear()
    previous_epoch = case.report()['credential_epoch']
    def next_generation(response):
        response['credential_epoch'] = previous_epoch + 1
        response['client_receipt']['credential_epoch'] = previous_epoch + 1
    case.response_change = next_generation
    report = case.data / 'status' / case.module.DEFAULT_REPORT_FILENAME
    return target, report, report.read_bytes()


def test_retained_binding_finishes_rotation_in_existing_slot(reattach_case):
    case = reattach_case
    _preserved_binding(case)
    assert case.run() == 0
    assert len(case.calls) == 3
    assert case.report()['registration_action'] == 'possession_reattach'
    assert json.loads((case.data / case.module.DEFAULT_CREDENTIAL_FILENAME).read_text())['key_id'] == case.response['key_id']


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
    assert target.read_bytes() == b'protected-existing'


def test_unwritable_preflight_preserves_ready_report_without_server_calls(reattach_case, monkeypatch):
    case = reattach_case
    target, report, original = _preserved_binding(case)
    def denied(*_args, **_kwargs):
        raise PermissionError()
    monkeypatch.setattr(case.module, '_probe_reattach_target', denied, raising=False)
    assert case.run() == 2
    assert case.calls == []
    assert report.read_bytes() == original
    assert target.read_bytes() == b'protected-existing'


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
    assert target.read_bytes() == b'protected-existing'
    monkeypatch.setattr(case.module, '_write_dpapi_secret', writer)
    case.calls.clear()
    assert case.run() == 0
    assert len(case.calls) == 3


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

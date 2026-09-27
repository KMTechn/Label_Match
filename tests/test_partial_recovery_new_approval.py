"""A fresh root approval can supersede a terminal partial-registration attempt."""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.test_partial_registration_recovery import recovery, put_residue, recovery_argv


@pytest.fixture
def attempts(recovery, monkeypatch):
    r = recovery
    r.transactions = {}
    r.requests = []
    r.fault = ''
    r.epoch = 1
    r.status_override = None
    r.approval = json.loads(Path(r.args.admin_recovery_secret_file).read_text())
    from tests.test_logistics_runtime_profile import _private_ca_pem, _machine_enrollment_response
    from tools.install_logistics_runtime_profile import ensure_runtime_profile_from_enrollment_bundle
    Path(r.args.tls_ca_bundle_path).write_bytes(_private_ca_pem())
    monkeypatch.setattr(r.m, 'ensure_runtime_profile_from_enrollment_bundle',
                        ensure_runtime_profile_from_enrollment_bundle)

    class Session:
        def post(self, url, **kwargs):
            action = url.rsplit('/', 1)[-1]
            proof = kwargs['json']['proof']
            r.requests.append((action, dict(proof)))
            if r.fault == 'before_' + action:
                r.fault = ''
                raise r.m.requests.Timeout('synthetic interruption before server')
            auth_id = proof['authorization_id']
            if action == 'prepare':
                if auth_id != r.approval['authorization_id'] or any(
                    item['recovery_state'] == 'PREPARED' and aid != auth_id
                    for aid, item in r.transactions.items()
                ):
                    return SimpleNamespace(status_code=409, json=lambda: {
                        'error': {'code': 'recovery_prepare_in_progress'}})
                if auth_id not in r.transactions:
                    bundle = _machine_enrollment_response(10)['machine_credential_bundle']
                    bundle['bindings'].update(app='LabelMatch', source_host_id=r.args.source_host_id,
                                              device_id=r.args.pc_id)
                    bundle['credentials']['producer_ingest'].update(key_id='test-' + auth_id,
                                                                   secret='test-producer-secret')
                    bundle['profiles']['logistics'].update(source_host_id=r.args.source_host_id,
                                                          device_id=r.args.pc_id)
                    r.transactions[auth_id] = dict(
                        prepare_id='prepare-' + auth_id, client_request_id=proof['client_request_id'],
                        producer_id=r.args.producer_id, source_host_id=r.args.source_host_id,
                        producer_install_id=r.args.producer_install_id,
                        recovery_state='PREPARED', authorization_state='RESERVED',
                        identity_action='REATTACHED', recovery_action='ADMIN_RECOVERY',
                        prepare_expires_at='2099-01-01T00:00:00Z',
                        proposed_credential_epoch=max([r.epoch] + [
                            t['proposed_credential_epoch'] for t in r.transactions.values()]) + 1,
                        endpoint_url=r.credential['endpoint_url'], key_id='test-' + auth_id,
                        secret='test-producer-secret', secret_fingerprint_sha256=r.m._fingerprint('test-producer-secret'),
                        possession_key=dict(contract_version=r.m.POSSESSION_KEY_CONTRACT_VERSION,
                                            fingerprint=r.descriptor['fingerprint']),
                        active_manifest_hashes=[r.m.manifest_hash(r.manifest)], machine_credential_bundle=bundle)
                assert r.transactions[auth_id]['client_request_id'] == proof['client_request_id']
            item = r.transactions[auth_id]
            if action == 'commit':
                if item['recovery_state'] != 'COMMITTED':
                    r.epoch = item['proposed_credential_epoch']
                item.update(recovery_state='COMMITTED', authorization_state='OPERATION_PENDING',
                            credential_epoch=r.epoch, commit_id=proof['commit_id'],
                            committed_at='2026-09-27T00:00:00Z')
            payload = dict(item, contract_version=f'producer-admin-recovery-{action}-response-v1',
                           status={'prepare': 'prepared', 'status': 'observed', 'commit': 'recovered'}[action])
            if action == 'status' and r.status_override:
                if isinstance(r.status_override, Exception):
                    raise r.status_override
                payload.update(r.status_override)
            if r.fault == action:
                r.fault = ''
                raise r.m.requests.Timeout('synthetic lost ACK')
            return SimpleNamespace(status_code=200, json=lambda: payload)

        def close(self):
            pass

    monkeypatch.setattr(r.m, '_open_admin_recovery_session', lambda _ca: Session())
    r.archive = Path(r.args.data_dir) / 'recovery/partial-registration'
    return r


def issue_new_approval(r, *, terminal='EXPIRED', new_path=False):
    if r.approval['authorization_id'] in r.transactions:
        r.transactions[r.approval['authorization_id']]['recovery_state'] = terminal
    r.approval = dict(r.approval, authorization_id='new-auth', recovery_token='new-private-approval',
                      nonce='new-nonce', audit_event_id='new-audit')
    if new_path:
        r.args.admin_recovery_secret_file = str(r.root / 'new-approval.json')
    r.m._write_json(r.args.admin_recovery_secret_file, r.approval)


def assert_ready_and_first_request(r):
    import current_user_onboarding as onboarding
    from tests.test_current_user_onboarding import _legacy_task_quiescent
    from package_logistics import PackageClientConfig, PackageLogisticsClient
    result = onboarding.onboard_current_user(
        r.root / 'app', environ=dict(LOCALAPPDATA=str(r.root / 'local'), LABEL_MATCH_SAVE_DIR=r.args.sync_dir,
                                    LABEL_MATCH_DIRECT_SYNC_ROOT=r.args.data_dir,
                                    KM_LOGISTICS_PROFILE_PATH=r.args.logistics_profile_path),
        require_bootstrap_integrity=False,
        registration_runner=lambda _p: pytest.fail('recovery must not enroll again'),
        autostart_installer=lambda _p: {'status': 'PASS'},
        scheduled_task_remover=lambda _p: {'status': 'ABSENT'},
        legacy_task_quiescence_reader=_legacy_task_quiescent,
        relay_launcher=lambda _p: {'status': 'ALIVE', 'process_id': 123})
    assert (result['status'], result['action']) == ('READY', 'REUSED')
    profile = onboarding._default_profile_loader(Path(r.args.logistics_profile_path))
    calls = []
    def transport(method, url, headers, body, timeout):
        calls.append((method, url, headers))
        return {'ok': True, 'data': {'bundle_id': 'first-bundle'}}
    client = PackageLogisticsClient(PackageClientConfig(
        base_url=profile.base_url, token=profile.bearer_token, authority_scope_id=profile.authority_scope,
        source_host_id=profile.source_host_id, device_id=profile.device_id, authority_epoch=profile.authority_epoch,
        authority_plane=profile.authority_plane, ledger_plane=profile.ledger_plane, plane_epoch=profile.plane_epoch,
        authoritative_required=True, tls_ca_bundle_path=profile.tls_ca_bundle_path), transport=transport)
    assert client.get_bundle('first-bundle', authority_scope_id=profile.authority_scope) == {'bundle_id': 'first-bundle'}
    assert calls[0][2]['Authorization'] == 'Bearer ' + profile.bearer_token
    assert profile.source_host_id == r.args.source_host_id
    assert profile.device_id == r.args.pc_id


@pytest.mark.parametrize('cut', ['before_prepare', 'before_commit', 'after_success'])
@pytest.mark.parametrize('new_path', [False, True])
def test_new_root_approval_recovers_terminal_attempt_and_is_ready(attempts, capsys, cut, new_path):
    r = attempts
    original, content = put_residue(r, 'identity')
    r.fault = cut
    first = r.m.main(recovery_argv(r))
    if cut == 'after_success':
        assert first == 0
        Path(r.args.logistics_profile_path).unlink()
    else:
        assert first == 2
    previous = (r.archive / 'journal.dpapi').read_bytes()
    originals = {p.name: p.read_bytes() for p in r.archive.glob('*.original')}
    issue_new_approval(r, terminal='COMMITTED' if cut == 'after_success' else 'EXPIRED', new_path=new_path)
    assert r.m.main(recovery_argv(r)) == 0
    assert r.epoch == (2 if cut == 'before_prepare' else 3)
    assert sum(t['recovery_state'] == 'COMMITTED' for t in r.transactions.values()) == (2 if cut == 'after_success' else 1)
    assert any(p.read_bytes() == previous for p in r.archive.glob('journal-*.dpapi'))
    assert all((r.archive / name).read_bytes() == raw for name, raw in originals.items())
    assert any(p.read_bytes() == content for p in r.archive.glob('*.original'))
    assert_ready_and_first_request(r)
    output = capsys.readouterr().out
    assert 'new-private-approval' not in output and 'test-producer-secret' not in output


@pytest.mark.parametrize('observation', ['PREPARED', 'UNKNOWN', 'timeout', 'wrong_identity', 'http_error'])
def test_new_approval_requires_bound_terminal_status(attempts, observation):
    r = attempts
    put_residue(r, 'identity')
    r.fault = 'before_commit'
    assert r.m.main(recovery_argv(r)) == 2
    previous = (r.archive / 'journal.dpapi').read_bytes()
    originals = {p.name: p.read_bytes() for p in r.archive.glob('*.original')}
    issue_new_approval(r)
    if observation == 'timeout':
        r.status_override = r.m.requests.Timeout('unavailable')
    elif observation == 'http_error':
        r.status_override = r.m.ProducerEnrollmentHTTPError(409, 'recovery_authorization_invalid', '')
    elif observation == 'wrong_identity':
        r.status_override = {'client_request_id': 'another-attempt'}
    else:
        r.status_override = {'recovery_state': observation}
    before = len(r.requests)
    assert r.m.main(recovery_argv(r)) == 2
    assert [action for action, _proof in r.requests[before:]] == ['status']
    assert (r.archive / 'journal.dpapi').read_bytes() == previous
    assert all((r.archive / name).read_bytes() == raw for name, raw in originals.items())
    assert Path(r.args.admin_recovery_secret_file).exists()
    assert r.epoch == 1


@pytest.mark.parametrize('mutation', ['user', 'pc', 'source_host', 'producer', 'key', 'ca'])
def test_new_approval_cannot_change_owner_or_installation(attempts, monkeypatch, mutation):
    r = attempts
    put_residue(r, 'identity')
    r.fault = 'before_commit'
    assert r.m.main(recovery_argv(r)) == 2
    previous = (r.archive / 'journal.dpapi').read_bytes()
    issue_new_approval(r)
    if mutation == 'user':
        monkeypatch.setattr(r.m, '_current_user_sid', lambda: 'S-1-5-21-100-200-300-9999')
    elif mutation == 'pc':
        monkeypatch.setattr(r.m, '_current_machine_guid', lambda: 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee')
    elif mutation == 'source_host':
        r.args.source_host_id = 'another-host'
    elif mutation == 'producer':
        r.args.producer_id = 'another-producer'
    elif mutation == 'key':
        r.descriptor['fingerprint'] = 'another-key'
    else:
        Path(r.args.tls_ca_bundle_path).write_bytes(b'another-ca')
    before = len(r.requests)
    assert r.m.main(recovery_argv(r)) == 2
    assert len(r.requests) == before
    assert (r.archive / 'journal.dpapi').read_bytes() == previous
    assert r.epoch == 1


def test_unknown_prepare_id_does_not_replace_server_prepared_attempt(attempts, capsys):
    r = attempts
    original, content = put_residue(r, 'identity')
    r.fault = 'prepare'
    assert r.m.main(recovery_argv(r)) == 2
    previous = (r.archive / 'journal.dpapi').read_bytes()
    issue_new_approval(r, terminal='PREPARED')
    assert r.m.main(recovery_argv(r)) == 2
    assert (r.archive / 'journal.dpapi').read_bytes() == previous
    assert original.read_bytes() == content
    output = capsys.readouterr().out
    assert '서버 root' in output and '만료 시각' in output and '새 승인' in output
    assert 'new-private-approval' not in output
    candidate_request = r.requests[-1][1]['client_request_id']
    r.transactions['test-auth']['recovery_state'] = 'EXPIRED'
    assert r.m.main(recovery_argv(r)) == 0
    assert r.transactions['new-auth']['client_request_id'] == candidate_request
    assert r.epoch == 3
    assert sum(t['recovery_state'] == 'COMMITTED' for t in r.transactions.values()) == 1
    assert_ready_and_first_request(r)


@pytest.mark.parametrize('cut', ['before_prepare', 'prepare', 'candidate_saved', 'retired_saved',
                                'promoted', 'before_commit', 'commit'])
def test_new_transaction_interruption_and_ack_loss_resume_same_request(attempts, monkeypatch, cut):
    r = attempts
    put_residue(r, 'identity')
    r.fault = 'before_commit'
    assert r.m.main(recovery_argv(r)) == 2
    previous = (r.archive / 'journal.dpapi').read_bytes()
    issue_new_approval(r, terminal='ABORTED', new_path=True)
    fired = False
    if cut in {'candidate_saved', 'promoted'}:
        save = r.m._save_partial_recovery
        def interrupted(path, state):
            nonlocal fired
            save(path, state)
            if not fired and state.get('predecessor') and (
                (cut == 'candidate_saved' and state['prepared'] and path.name.startswith('attempt-'))
                or (cut == 'promoted' and path.name == 'journal.dpapi')
            ):
                fired = True
                raise OSError('synthetic interruption after protected save')
        monkeypatch.setattr(r.m, '_save_partial_recovery', interrupted)
    elif cut == 'retired_saved':
        write = r.m._atomic_write
        def interrupted(path, data):
            nonlocal fired
            write(path, data)
            if not fired and path.name.startswith('journal-'):
                fired = True
                raise OSError('synthetic interruption after old journal preservation')
        monkeypatch.setattr(r.m, '_atomic_write', interrupted)
    else:
        r.fault = cut
    assert r.m.main(recovery_argv(r)) == 2
    if cut in {'candidate_saved', 'retired_saved', 'promoted'}:
        assert fired
    requests = [proof['client_request_id'] for action, proof in r.requests
                if action == 'prepare' and proof['authorization_id'] == 'new-auth']
    assert len(set(requests)) == 1
    if cut in {'before_prepare', 'prepare', 'candidate_saved', 'retired_saved'}:
        assert (r.archive / 'journal.dpapi').read_bytes() == previous
    assert r.m.main(recovery_argv(r)) == 0
    assert r.transactions['new-auth']['client_request_id'] == requests[0]
    assert r.epoch == 3
    assert sum(t['recovery_state'] == 'COMMITTED' for t in r.transactions.values()) == 1
    assert any(p.read_bytes() == previous for p in r.archive.glob('journal-*.dpapi'))
    assert_ready_and_first_request(r)


def test_new_transaction_inherits_unfinished_original_move(attempts, monkeypatch):
    r = attempts
    original, content = put_residue(r, 'identity')
    save = r.m._save_partial_recovery
    fired = False
    def interrupted(path, state):
        nonlocal fired
        if not fired and any(entry['moved'] for entry in state['archives']):
            fired = True
            raise OSError('synthetic interruption after rename before moved readback')
        save(path, state)
    monkeypatch.setattr(r.m, '_save_partial_recovery', interrupted)
    assert r.m.main(recovery_argv(r)) == 2
    assert fired and not original.exists()
    archived = {p.name: p.read_bytes() for p in r.archive.glob('*.original')}
    assert list(archived.values()) == [content]
    issue_new_approval(r)
    assert r.m.main(recovery_argv(r)) == 0
    assert {p.name: p.read_bytes() for p in r.archive.glob('*.original')} == archived
    assert_ready_and_first_request(r)

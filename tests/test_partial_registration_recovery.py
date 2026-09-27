"""Audited partial recovery: real files and onboarding, synthetic transport/key."""
from contextlib import contextmanager
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.test_register_label_match_worker_pc import (
    TEST_MACHINE_GUID, TEST_USER_SID, generated_install_id, load_registration_module,
)
from tests._possession_fixture import fake_possession_descriptor


@pytest.fixture
def recovery(tmp_path, monkeypatch):
    m = load_registration_module()
    monkeypatch.setattr(m, '_current_user_sid', lambda: TEST_USER_SID)
    monkeypatch.setattr(m, '_current_machine_guid', lambda: TEST_MACHINE_GUID)
    monkeypatch.setattr(m, '_secure_partial_directory', lambda path, sid: path.mkdir(parents=True, exist_ok=True), raising=False)
    ca = tmp_path / 'ca.pem'
    ca.write_bytes(b'test-ca')
    args = SimpleNamespace(
        dry_run=False, credential_scope='current_user', pc_id='LABEL-PC', producer_id='producer-label',
        source_host_id='label-host', producer_install_id=generated_install_id(m),
        machine_guid=TEST_MACHINE_GUID, server_base_url='https://worker.example.invalid',
        endpoint_url='', enrollment_url='', admin_recovery_url='', key_id='', secret_ref_target='',
        data_dir=str(tmp_path / 'ds'), sync_dir=str(tmp_path / 'work'), identity_path='',
        manifest_path='', credential_path='', receipt_path='', report_path='',
        logistics_profile_path=str(tmp_path / 'lp' / 'profile.json'),
        tls_ca_bundle_path=str(ca), admin_recovery_secret_file=str(tmp_path / 'auth.json'),
        expected_active_manifest_hash='', enrollment_token='', enrollment_token_file='',
        enrollment_token_env='', enrollment_timeout_seconds=3,
        require_machine_credential_bundle=True, recover_partial_local_state=True,
    )
    manifest, credential, report = m.build_payloads(args)
    args.expected_active_manifest_hash = m.manifest_hash(manifest)
    auth = dict(contract_version=m.ADMIN_RECOVERY_AUTHORIZATION_CONTRACT_VERSION,
                authorization_id='test-auth', producer_id=args.producer_id,
                recovery_token='test-private-approval', nonce='test-nonce',
                expires_at='2099-01-01T00:00:00Z', audience=m.ADMIN_RECOVERY_AUDIENCE,
                audit_event_id='test-audit')
    m._write_json(args.admin_recovery_secret_file, auth)
    state = dict(calls=[], committed=False, commits=0, fault='', rejection='')
    descriptor = fake_possession_descriptor()

    class Key:
        def descriptor(self):
            return SimpleNamespace(as_dict=lambda: dict(descriptor))

        def assert_non_exportable(self):
            return SimpleNamespace(private_export_status_hex='0x80090010')

        def sign_es256(self, value):
            return b's' * 64

    @contextmanager
    def key(**kwargs):
        assert kwargs == {'scope': 'current_user'}
        yield Key()

    monkeypatch.setattr(m.PersistentPossessionKey, 'open_existing', staticmethod(key))
    monkeypatch.setattr(m.PersistentPossessionKey, 'provision_initial',
                        lambda **_kw: pytest.fail('shared key must never be provisioned'))

    class Session:
        def post(self, url, **kwargs):
            action = url.rsplit('/', 1)[-1]
            state['calls'].append(action)
            proof = kwargs['json']['proof']
            assert kwargs['allow_redirects'] is False
            assert proof['manifest_hash'] == m.manifest_hash(manifest)
            if state['rejection']:
                return SimpleNamespace(status_code=403, json=lambda: {'error': {'code': state['rejection']}})
            common = dict(producer_id=args.producer_id, source_host_id=args.source_host_id,
                          producer_install_id=args.producer_install_id, prepare_id='test-prepare',
                          client_request_id=proof['client_request_id'], key_id='test-rotated',
                          secret_fingerprint_sha256=m._fingerprint('test-producer-secret'),
                          possession_key=dict(contract_version=m.POSSESSION_KEY_CONTRACT_VERSION,
                                              fingerprint=descriptor['fingerprint']),
                          active_manifest_hashes=[m.manifest_hash(manifest)],
                          identity_action='REATTACHED', recovery_action='ADMIN_RECOVERY')
            if action == 'prepare':
                response = dict(common, contract_version='producer-admin-recovery-prepare-response-v1',
                                status='prepared', recovery_state='PREPARED', authorization_state='RESERVED',
                                prepare_expires_at='2099-01-01T00:00:00Z', proposed_credential_epoch=2,
                                endpoint_url=credential['endpoint_url'], secret='test-producer-secret',
                                machine_credential_bundle={'test': True})
            else:
                if action == 'commit':
                    if not state['committed']:
                        state['commits'] += 1
                    state.update(committed=True, commit_id=proof['commit_id'])
                response = dict(common, contract_version=f'producer-admin-recovery-{action}-response-v1',
                                status='recovered' if action == 'commit' else 'observed',
                                recovery_state='COMMITTED' if state['committed'] else 'PREPARED',
                                authorization_state='OPERATION_PENDING' if state['committed'] else 'RESERVED',
                                commit_id=state.get('commit_id', ''), credential_epoch=2,
                                committed_at='2026-09-27T00:00:00Z')
            if state['fault'] == action:
                state['fault'] = ''
                raise m.requests.Timeout('synthetic lost ACK')
            return SimpleNamespace(status_code=200, json=lambda: response)

        def close(self):
            pass

    monkeypatch.setattr(m, '_open_admin_recovery_session', lambda _ca: Session())
    # Exercise native current-user DPAPI for the journal and producer key. No
    # WinCred or persistent CNG key is created; profile installation is a boundary fake.
    def install(_response, **kwargs):
        assert kwargs['allow_existing_token_rotation'] is False
        p = Path(args.logistics_profile_path)
        m._write_json(p, dict(credential_scope='current_user', source_host_id=args.source_host_id,
                             authority_plane='AUTHORITATIVE', tls_ca_bundle_path=str(ca)))
        secret = p.parent / 'secrets' / 'bearer-token.dpapi'
        secret.parent.mkdir(exist_ok=True)
        secret.write_bytes(b'test-protected-logistics')
        target = p.parent / m.TLS_CA_BUNDLE_RELATIVE_PATH
        target.parent.mkdir(exist_ok=True)
        target.write_bytes(ca.read_bytes())
        return dict(status='installed', created_paths=[str(p), str(secret), str(target)])
    monkeypatch.setattr(m, 'ensure_runtime_profile_from_enrollment_bundle', install)
    return SimpleNamespace(m=m, args=args, manifest=manifest, credential=credential, report=report,
                           state=state, descriptor=descriptor, root=tmp_path)


def put_residue(r, kind):
    m, a = r.m, r.args
    paths = {
        'identity': Path(a.data_dir) / m.PRODUCER_IDENTITY_FILENAME,
        'manifest': Path(a.data_dir) / m.DEFAULT_MANIFEST_FILENAME,
        'credential': Path(a.data_dir) / m.DEFAULT_CREDENTIAL_FILENAME,
        'producer_secret': m._secret_path(a.data_dir, r.credential['secret_ref'].split(':')[1]),
        'profile': Path(a.logistics_profile_path),
        'logistics_secret': Path(a.logistics_profile_path).parent / 'secrets' / 'bearer-token.dpapi',
        'tls': Path(a.logistics_profile_path).parent / m.TLS_CA_BUNDLE_RELATIVE_PATH,
        'receipt': Path(a.data_dir) / 'evidence' / m.DEFAULT_RECEIPT_FILENAME,
        'registration': Path(a.data_dir) / 'status' / m.DEFAULT_REPORT_FILENAME,
    }
    path = paths[kind]
    path.parent.mkdir(parents=True, exist_ok=True)
    if kind == 'manifest':
        content = json.dumps(r.manifest).encode()
    elif kind == 'identity':
        content = json.dumps(dict(schema_version=m.PRODUCER_IDENTITY_SCHEMA_VERSION,
                                  producer_id=a.producer_id, **r.manifest['pc_identity'])).encode()
    else:
        content = b'interrupted-partial-file'
    path.write_bytes(content)
    return path, content


@pytest.mark.parametrize('kind', ['identity', 'manifest', 'credential', 'producer_secret',
                                'profile', 'logistics_secret', 'tls', 'receipt', 'registration'])
def test_partial_recovery_preserves_each_residue_and_commits_once(recovery, kind):
    r = recovery
    path, content = put_residue(r, kind)
    result = r.m.apply_registration(r.args, r.manifest, r.credential, r.report)
    assert result['status'] == 'ADMIN_RECOVERY_REGISTERED'
    assert r.state['commits'] == 1
    archive = Path(result['partial_recovery_archive'])
    assert any(p.read_bytes() == content for p in archive.rglob('*.original'))


@pytest.mark.parametrize('fault', ['prepare', 'commit'])
def test_partial_recovery_lost_ack_reuses_transaction(recovery, fault):
    r = recovery
    put_residue(r, 'identity')
    r.state['fault'] = fault
    with pytest.raises(r.m.requests.Timeout):
        r.m.apply_registration(r.args, r.manifest, r.credential, r.report)
    result = r.m.apply_registration(r.args, r.manifest, r.credential, r.report)
    assert result['status'] == 'ADMIN_RECOVERY_REGISTERED'
    assert r.state['commits'] == 1


@pytest.mark.parametrize('bad', ['user', 'key', 'expired', 'manifest', 'grant'])
def test_partial_recovery_rejects_mismatched_authority_without_moving(recovery, monkeypatch, bad):
    r = recovery
    path, content = put_residue(r, 'identity')
    if bad == 'user':
        monkeypatch.setattr(r.m, '_current_user_sid', lambda: 'S-1-5-21-100-200-300-9999')
    elif bad == 'key':
        r.m._write_json(Path(r.args.data_dir) / 'evidence' / r.m.DEFAULT_RECEIPT_FILENAME,
                        {'possession_key_fingerprint': 'different-key'})
    elif bad == 'expired':
        auth = json.loads(Path(r.args.admin_recovery_secret_file).read_text())
        auth['expires_at'] = '2000-01-01T00:00:00Z'
        r.m._write_json(r.args.admin_recovery_secret_file, auth)
    elif bad == 'manifest':
        r.args.expected_active_manifest_hash = '0' * 64
    else:
        r.state['rejection'] = 'producer_grant_revoked'
    with pytest.raises(r.m.DirectSyncPushError):
        r.m.apply_registration(r.args, r.manifest, r.credential, r.report)
    assert path.read_bytes() == content
    assert r.state['commits'] == 0


def recovery_argv(r):
    result = ['--apply', '--recover-partial-local-state']
    for key in ('credential_scope', 'pc_id', 'producer_id', 'source_host_id', 'producer_install_id',
                'server_base_url', 'data_dir', 'sync_dir', 'logistics_profile_path',
                'tls_ca_bundle_path', 'admin_recovery_secret_file', 'expected_active_manifest_hash',
                'enrollment_token_env'):
        result += ['--' + key.replace('_', '-'), str(getattr(r.args, key))]
    return result


@pytest.mark.parametrize('cut', ['archive_move', 'profile', 'producer', 'documents', 'authorization_cleanup'])
def test_partial_recovery_interruption_resumes_without_second_rotation(recovery, monkeypatch, cut):
    r = recovery
    path, original = put_residue(r, 'identity')
    fired = False
    if cut == 'archive_move':
        save = r.m._save_partial_recovery
        def interrupted(journal, state):
            nonlocal fired
            if not fired and any(e['moved'] for e in state['archives']):
                fired = True
                raise OSError('synthetic interruption after rename')
            return save(journal, state)
        monkeypatch.setattr(r.m, '_save_partial_recovery', interrupted)
    elif cut == 'profile':
        install = r.m.ensure_runtime_profile_from_enrollment_bundle
        def interrupted(*args, **kwargs):
            nonlocal fired
            result = install(*args, **kwargs)
            if not fired:
                fired = True
                raise OSError('synthetic profile interruption')
            return result
        monkeypatch.setattr(r.m, 'ensure_runtime_profile_from_enrollment_bundle', interrupted)
    elif cut == 'producer':
        write = r.m._write_dpapi_secret
        def interrupted(*args, **kwargs):
            nonlocal fired
            result = write(*args, **kwargs)
            if not fired:
                fired = True
                raise OSError('synthetic producer interruption')
            return result
        monkeypatch.setattr(r.m, '_write_dpapi_secret', interrupted)
    elif cut == 'documents':
        write = r.m._write_json
        def interrupted(target, value):
            nonlocal fired
            if not fired and Path(target).name == r.m.DEFAULT_MANIFEST_FILENAME:
                fired = True
                raise OSError('synthetic document interruption')
            return write(target, value)
        monkeypatch.setattr(r.m, '_write_json', interrupted)
    else:
        unlink = Path.unlink
        def interrupted(target, *args, **kwargs):
            nonlocal fired
            result = unlink(target, *args, **kwargs)
            if not fired and target == Path(r.args.admin_recovery_secret_file):
                fired = True
                raise OSError('synthetic approval cleanup interruption')
            return result
        monkeypatch.setattr(Path, 'unlink', interrupted)
    assert r.m.main(recovery_argv(r)) in {2, 3}
    assert fired
    assert r.m.main(recovery_argv(r)) == 0
    assert r.state['commits'] == 1
    assert any(p.read_bytes() == original for p in (Path(r.args.data_dir) / 'recovery').rglob('*.original'))


@pytest.mark.parametrize('mutation', ['user', 'key', 'ca', 'journal', 'source', 'archive', 'authorization'])
def test_partial_recovery_resume_rejects_changed_binding(recovery, monkeypatch, mutation):
    r = recovery
    path, original = put_residue(r, 'identity')
    r.state['fault'] = 'commit'
    with pytest.raises(r.m.requests.Timeout):
        r.m.apply_registration(r.args, r.manifest, r.credential, r.report)
    calls_before = len(r.state['calls'])
    archive = Path(r.args.data_dir) / 'recovery' / 'partial-registration'
    if mutation == 'user':
        monkeypatch.setattr(r.m, '_current_user_sid', lambda: 'S-1-5-21-100-200-300-9999')
    elif mutation == 'key':
        r.descriptor['fingerprint'] = 'other-key'
    elif mutation == 'ca':
        Path(r.args.tls_ca_bundle_path).write_bytes(b'other-ca')
    elif mutation == 'journal':
        (archive / 'journal.dpapi').write_bytes(b'corrupt')
    elif mutation == 'source':
        r.m._write_json(path, {'producer_id': 'another-producer'})
    elif mutation == 'authorization':
        auth = json.loads(Path(r.args.admin_recovery_secret_file).read_text())
        auth['authorization_id'] = 'new-approval'
        r.m._write_json(r.args.admin_recovery_secret_file, auth)
    else:
        next(archive.glob('*.original')).write_bytes(b'changed-archive')
    with pytest.raises(Exception):
        r.m.apply_registration(r.args, r.manifest, r.credential, r.report)
    assert r.state['commits'] == 1
    if mutation != 'archive':
        assert len(r.state['calls']) == calls_before


def test_partial_recovery_real_profile_onboarding_and_first_request(recovery, monkeypatch, capsys):
    import current_user_onboarding as onboarding
    import tools.install_logistics_runtime_profile as installer
    from tests.test_logistics_runtime_profile import _private_ca_pem, _machine_enrollment_response
    from tests.test_current_user_onboarding import _legacy_task_quiescent
    from package_logistics import PackageClientConfig, PackageLogisticsClient

    r = recovery
    Path(r.args.tls_ca_bundle_path).write_bytes(_private_ca_pem())
    for kind in ('identity', 'manifest', 'credential', 'producer_secret', 'profile',
                 'logistics_secret', 'tls', 'receipt', 'registration'):
        put_residue(r, kind)
    # Include damaged identity in an otherwise fully present registration.
    (Path(r.args.data_dir) / r.m.PRODUCER_IDENTITY_FILENAME).write_bytes(b'{broken')
    session_factory = r.m._open_admin_recovery_session
    def session(ca):
        value = session_factory(ca)
        post = value.post
        def with_bundle(url, **kwargs):
            response = post(url, **kwargs)
            payload = response.json()
            if url.endswith('/prepare'):
                bundle = _machine_enrollment_response(10)['machine_credential_bundle']
                bundle['bindings'].update(app='LabelMatch', source_host_id=r.args.source_host_id, device_id=r.args.pc_id)
                bundle['credentials']['producer_ingest'].update(key_id=payload['key_id'], secret=payload['secret'])
                bundle['profiles']['logistics'].update(source_host_id=r.args.source_host_id, device_id=r.args.pc_id)
                payload['machine_credential_bundle'] = bundle
            return SimpleNamespace(status_code=response.status_code, json=lambda: payload)
        value.post = with_bundle
        return value
    monkeypatch.setattr(r.m, '_open_admin_recovery_session', session)
    monkeypatch.setattr(r.m, 'ensure_runtime_profile_from_enrollment_bundle', installer.ensure_runtime_profile_from_enrollment_bundle)
    assert r.m.main(recovery_argv(r)) == 0
    environment = dict(LOCALAPPDATA=str(r.root / 'local'), LABEL_MATCH_SAVE_DIR=r.args.sync_dir,
                       LABEL_MATCH_DIRECT_SYNC_ROOT=r.args.data_dir, KM_LOGISTICS_PROFILE_PATH=r.args.logistics_profile_path)
    result = onboarding.onboard_current_user(
        r.root / 'app', environ=environment, require_bootstrap_integrity=False,
        registration_runner=lambda _p: pytest.fail('recovery must not enroll again'),
        autostart_installer=lambda _p: {'status': 'PASS'},
        scheduled_task_remover=lambda _p: {'status': 'ABSENT'},
        legacy_task_quiescence_reader=_legacy_task_quiescent,
        relay_launcher=lambda _p: {'status': 'ALIVE', 'process_id': 123},
    )
    assert result['status'] == 'READY'
    assert result['action'] == 'REUSED'
    profile = onboarding._default_profile_loader(Path(r.args.logistics_profile_path))
    calls = []
    def transport(method, url, headers, body, timeout):
        calls.append((method, url, headers))
        return {'ok': True, 'data': {'bundle_id': 'first-bundle'}}
    client = PackageLogisticsClient(PackageClientConfig(
        base_url=profile.base_url, token=profile.bearer_token,
        authority_scope_id=profile.authority_scope, source_host_id=profile.source_host_id,
        device_id=profile.device_id, authority_epoch=profile.authority_epoch,
        authority_plane=profile.authority_plane, ledger_plane=profile.ledger_plane,
        plane_epoch=profile.plane_epoch, authoritative_required=True,
        tls_ca_bundle_path=profile.tls_ca_bundle_path), transport=transport)
    assert client.get_bundle('first-bundle', authority_scope_id=profile.authority_scope) == {'bundle_id': 'first-bundle'}
    assert calls[0][2]['Authorization'] == 'Bearer logistics-token-01'
    output = capsys.readouterr().out
    assert '보관' in output
    assert 'test-private-approval' not in output and 'test-producer-secret' not in output
    assert r.m.main(recovery_argv(r)) == 0
    assert r.state['commits'] == 1


def test_partial_archive_native_acl_is_private(tmp_path):
    m = load_registration_module()
    path = tmp_path / 'protected'
    sid = m._current_user_sid()
    m._secure_partial_directory(path, sid)
    result = m.subprocess.run(['icacls', str(path)], capture_output=True, check=True)
    assert b'(I)' not in result.stdout
    assert path.is_dir()


def test_partial_recovery_rejects_hardlink_before_http(recovery):
    r = recovery
    source = r.root / 'outside.json'
    source.write_bytes(b'original')
    identity = Path(r.args.data_dir) / r.m.PRODUCER_IDENTITY_FILENAME
    identity.parent.mkdir(parents=True)
    identity.hardlink_to(source)
    with pytest.raises(r.m.DirectSyncPushError):
        r.m.apply_registration(r.args, r.manifest, r.credential, r.report)
    assert source.read_bytes() == b'original'
    assert not r.state['calls']


@pytest.mark.parametrize('action,field,value', [
    ('prepare', 'producer_install_id', 'another-install'),
    ('prepare', 'secret_fingerprint_sha256', '0' * 64),
    ('prepare', 'proposed_credential_epoch', True),
    ('prepare', 'prepare_expires_at', 'invalid'),
    ('commit', 'commit_id', 'another-commit'),
    ('commit', 'status', 'failed'),
    ('commit', 'authorization_state', 'REVOKED'),
])
def test_partial_recovery_never_installs_unbound_response(recovery, monkeypatch, action, field, value):
    r = recovery
    put_residue(r, 'identity')
    factory = r.m._open_admin_recovery_session
    def session(ca):
        selected = factory(ca)
        post = selected.post
        def altered(url, **kwargs):
            response = post(url, **kwargs)
            payload = response.json()
            if url.endswith('/' + action):
                payload[field] = value
            return SimpleNamespace(status_code=response.status_code, json=lambda: payload)
        selected.post = altered
        return selected
    monkeypatch.setattr(r.m, '_open_admin_recovery_session', session)
    with pytest.raises(r.m.DirectSyncPushError):
        r.m.apply_registration(r.args, r.manifest, r.credential, r.report)
    assert not Path(r.args.logistics_profile_path).exists()
    assert r.state['commits'] == (action == 'commit')

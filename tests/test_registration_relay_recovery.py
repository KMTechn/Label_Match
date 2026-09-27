"""Recovery on a used PC must preserve pending wire requests and resume the relay."""
import json
import csv
import hashlib
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace

import pytest

import current_user_onboarding as onboarding
from direct_sync_push import init_relay_queue_schema, enqueue_source_file_for_relay, ProducerCredentials
from producer_runtime_client import _create_state, _scope_key, _scope_values
from tests.test_partial_registration_recovery import recovery, put_residue, recovery_argv
from user_relay import request_user_relay_stop, user_relay_stop_path


def used_pc(r, monkeypatch):
    root = Path(r.args.data_dir)
    database = root / 'queue/direct_sync_relay.sqlite3'
    init_relay_queue_schema(database)
    credentials = SimpleNamespace(**r.credential)
    manifest_path = root / 'candidate.json'
    r.m._write_json(manifest_path, r.manifest)
    source = Path(r.args.sync_dir) / '포장실작업이벤트로그_LABEL-PC_20260927.csv'
    source.parent.mkdir(parents=True, exist_ok=True)
    with source.open('w', encoding='utf-8-sig', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(['timestamp', 'worker_name', 'event', 'details'])
        writer.writerow(['2026-09-27T01:00:00', 'worker', 'APP_CLOSE',
                         json.dumps({'message': 'Application closed.', 'close_attempt_id': 'close-1'})])
    r.pending = enqueue_source_file_for_relay(
        db_path=database, spool_dir=root / 'spool', source_file_path=source,
        producer_manifest_path=manifest_path,
        credentials=ProducerCredentials(producer_id=credentials.producer_id, key_id=credentials.key_id,
                                        endpoint_url=credentials.endpoint_url, secret='old-secret'),
    )
    scope = _scope_values(credentials, r.args.producer_install_id)
    with sqlite3.connect(database) as conn:
        conn.row_factory = sqlite3.Row
        authority = dict(_create_state(conn, scope, '2026-09-27T00:00:00Z'))
        conn.execute("UPDATE direct_sync_runtime_authority SET status='ACTIVE', lease_id='old-lease', fence=1, next_request_token=?, next_request_sequence=1, expires_at='2099-01-01T00:00:00Z'", ('A' * 43,))
    # Use the supported stop operation; all native persistence boundaries are fakes.
    app = r.root / 'canonical'
    app.mkdir()
    monkeypatch.setattr(onboarding, 'CANONICAL_PORTABLE_ROOT', app)
    r.database, r.old_scope, r.app = database, scope, app
    normal_remove(r)
    return authority


def environment(r):
    return dict(LOCALAPPDATA=str(r.root / 'local'), LABEL_MATCH_SAVE_DIR=r.args.sync_dir,
                LABEL_MATCH_DIRECT_SYNC_ROOT=r.args.data_dir, KM_LOGISTICS_PROFILE_PATH=r.args.logistics_profile_path)


def normal_remove(r):
    return onboarding.remove_current_user_setup(
        r.app, environ=environment(r), autostart_remover=lambda: {'status': 'ABSENT'},
        scheduled_task_remover=lambda _p: {'status': 'ABSENT'},
    )


def portable_install(r):
    (r.app / 'runtime').mkdir()
    runtime = r.app / 'runtime/pythonw.exe'
    runtime.write_bytes(b'test-runtime')
    installer = r.app / 'INSTALL_CANONICAL_PORTABLE.ps1'
    installer.write_bytes(b'test-installer')
    r.m._write_json(r.app / 'portable-manifest.json', dict(
        schema='label-match-portable-tree-v1', entrypoint='runtime/pythonw.exe app/main.py',
        source_commit='a' * 40, source_tree='b' * 40, allowed_unsigned_app_pe=[], forbidden_package_roots=[],
        canonical_installer=installer.name, canonical_installer_sha256=hashlib.sha256(installer.read_bytes()).hexdigest(),
        runtime_pythonw_sha256=hashlib.sha256(runtime.read_bytes()).hexdigest(),
    ))
    rows = [dict(path=p.relative_to(r.app).as_posix(), size=p.stat().st_size,
                 sha256=hashlib.sha256(p.read_bytes()).hexdigest())
            for p in sorted(r.app.rglob('*'), key=lambda p: p.relative_to(r.app).as_posix().encode('utf-16-be')) if p.is_file()]
    aggregate = hashlib.sha256(''.join(f"{row['sha256']} {row['size']} {row['path']}\n" for row in rows).encode()).hexdigest()
    r.m._write_json(r.app / 'bootstrap-integrity.json', dict(
        schema_version='label-match-bootstrap-integrity-v1', status='PASS', code_root='.',
        file_count=len(rows), files=rows, aggregate_sha256=aggregate,
        identity_profile_created=False, state_scope='current_user_first_run',
    ))


def native_profile(r, monkeypatch):
    import tools.install_logistics_runtime_profile as installer
    from tests.test_logistics_runtime_profile import _private_ca_pem, _machine_enrollment_response
    Path(r.args.tls_ca_bundle_path).write_bytes(_private_ca_pem())
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


def resume(r, monkeypatch, *, launcher=None):
    import registration_relay_recovery as handoff
    from tests.test_current_user_onboarding import _legacy_task_quiescent
    monkeypatch.setattr(handoff, 'current_owner', lambda: dict(
        user_sid=r.m._current_user_sid(), producer_install_id=r.args.producer_install_id))
    return onboarding.onboard_current_user(
        r.app, environ=environment(r), require_bootstrap_integrity=False,
        registration_runner=lambda _p: pytest.fail('recovery must not enroll again'),
        autostart_installer=lambda _p: {'status': 'PASS'},
        scheduled_task_remover=lambda _p: {'status': 'ABSENT'},
        legacy_task_quiescence_reader=_legacy_task_quiescent,
        relay_launcher=launcher or (lambda _p: {'status': 'ALIVE', 'process_id': 123}),
    )


def test_committed_partial_recovery_transfers_used_pc_authority(recovery, monkeypatch):
    r = recovery
    used_pc(r, monkeypatch)
    put_residue(r, 'identity')
    result = r.m.apply_registration(r.args, r.manifest, r.credential, r.report)
    assert result['status'] == 'ADMIN_RECOVERY_REGISTERED'
    with sqlite3.connect(r.database) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute('SELECT * FROM direct_sync_runtime_authority').fetchall()
        pending = conn.execute('SELECT * FROM direct_sync_relay_batches').fetchone()
    assert len(rows) == 1
    assert rows[0]['key_id'] == 'test-rotated'
    assert rows[0]['authority_scope'] == _scope_key({**r.old_scope, 'key_id': 'test-rotated'})
    assert rows[0]['fence'] == 1
    assert rows[0]['next_request_token'] == 'A' * 43
    assert pending['key_id'] == 'test-rotated'
    assert pending['status'] == 'pending'
    assert json.loads(pending['metadata_json']) == r.pending.metadata
    assert Path(pending['spooled_file_path']).read_bytes() == Path(pending['source_file_path']).read_bytes()
    assert user_relay_stop_path(r.args.data_dir).exists()


def test_recovery_then_onboarding_releases_only_the_proven_stop(recovery, monkeypatch):
    r = recovery
    used_pc(r, monkeypatch)
    portable_install(r)
    native_profile(r, monkeypatch)
    put_residue(r, 'identity')
    assert r.m.main(recovery_argv(r)) == 0
    result = resume(r, monkeypatch)
    assert result['status'] == 'READY'
    assert result['action'] == 'REUSED'
    assert result['stop_marker_release']['status'] == 'ADMIN_RECOVERY_COMMITTED'
    assert not user_relay_stop_path(r.args.data_dir).exists()


@pytest.mark.parametrize('fault', ['prepare', 'commit'])
def test_used_pc_unknown_server_result_preserves_old_authority_then_resumes(recovery, monkeypatch, fault):
    r = recovery
    used_pc(r, monkeypatch)
    put_residue(r, 'identity')
    r.state['fault'] = fault
    with pytest.raises(r.m.requests.Timeout):
        r.m.apply_registration(r.args, r.manifest, r.credential, r.report)
    with sqlite3.connect(r.database) as conn:
        assert conn.execute('SELECT key_id FROM direct_sync_runtime_authority').fetchone()[0] == r.old_scope['key_id']
        assert conn.execute('SELECT key_id FROM direct_sync_relay_batches').fetchone()[0] == r.old_scope['key_id']
    assert r.m.apply_registration(r.args, r.manifest, r.credential, r.report)['relay_recovery']['status'] == 'TRANSFERRED'
    assert r.state['commits'] == 1


@pytest.mark.parametrize('mutation', ['user', 'machine', 'producer', 'endpoint', 'scope', 'quarantined',
                                    'row_identity', 'row_manifest', 'marker', 'removal'])
def test_used_pc_foreign_or_unproven_state_is_rejected_before_server(recovery, monkeypatch, mutation):
    r = recovery
    used_pc(r, monkeypatch)
    put_residue(r, 'identity')
    if mutation == 'user':
        monkeypatch.setattr(r.m, '_current_user_sid', lambda: 'S-1-5-21-1-2-3-9999')
    elif mutation == 'machine':
        monkeypatch.setattr(r.m, '_current_machine_guid', lambda: 'd' * 32)
    elif mutation == 'marker':
        request_user_relay_stop(r.args.data_dir)
    elif mutation == 'removal':
        (Path(r.args.data_dir) / 'status/current_user_removal.json').unlink()
    else:
        with sqlite3.connect(r.database) as conn:
            if mutation.startswith('row_'):
                metadata = dict(r.pending.metadata)
                metadata['producer_install_id' if mutation == 'row_identity' else 'manifest_hash'] = 'foreign'
                conn.execute('UPDATE direct_sync_relay_batches SET metadata_json=?', (json.dumps(metadata),))
            else:
                field = {'producer': 'producer_id', 'endpoint': 'endpoint_url', 'scope': 'authority_scope',
                         'quarantined': 'status'}[mutation]
                conn.execute(f'UPDATE direct_sync_runtime_authority SET {field}=?', ('foreign',))
    with pytest.raises((ValueError, OSError)):
        r.m.apply_registration(r.args, r.manifest, r.credential, r.report)
    assert r.state['calls'] == []


def test_audit_failure_rolls_back_queue_and_authority_together(recovery, monkeypatch):
    import registration_relay_recovery as handoff
    r = recovery
    used_pc(r, monkeypatch)
    put_residue(r, 'identity')
    protect = handoff.protect_handoff
    def fail(_value):
        raise OSError('audit write failed')
    monkeypatch.setattr(handoff, 'protect_handoff', fail)
    with pytest.raises(OSError, match='audit write failed'):
        r.m.apply_registration(r.args, r.manifest, r.credential, r.report)
    with sqlite3.connect(r.database) as conn:
        assert conn.execute('SELECT key_id FROM direct_sync_runtime_authority').fetchone()[0] == r.old_scope['key_id']
        assert conn.execute('SELECT key_id FROM direct_sync_relay_batches').fetchone()[0] == r.old_scope['key_id']
    monkeypatch.setattr(handoff, 'protect_handoff', protect)
    assert r.m.apply_registration(r.args, r.manifest, r.credential, r.report)['relay_recovery']['status'] == 'TRANSFERRED'
    assert r.state['commits'] == 1


def test_after_handoff_interruption_reuses_protected_audit(recovery, monkeypatch):
    r = recovery
    used_pc(r, monkeypatch)
    put_residue(r, 'identity')
    first = r.m.apply_registration(r.args, r.manifest, r.credential, r.report)
    assert first['relay_recovery']['status'] == 'TRANSFERRED'
    second = r.m.apply_registration(r.args, r.manifest, r.credential, r.report)
    assert second['relay_recovery']['status'] == 'REUSED'
    assert r.state['commits'] == 1
    with sqlite3.connect(r.database) as conn:
        assert conn.execute('SELECT COUNT(*) FROM direct_sync_credential_recoveries').fetchone()[0] == 1


def test_failed_resume_stays_visible_and_supported_removal_allows_retry(recovery, monkeypatch):
    r = recovery
    used_pc(r, monkeypatch)
    portable_install(r)
    native_profile(r, monkeypatch)
    put_residue(r, 'identity')
    assert r.m.main(recovery_argv(r)) == 0
    with pytest.raises(onboarding.CurrentUserOnboardingError) as error:
        resume(r, monkeypatch, launcher=lambda _p: {'status': 'UNKNOWN'})
    assert error.value.cause_code == 'RELAY_RESUME_REQUIRED'
    assert user_relay_stop_path(r.args.data_dir).exists()
    # An unconfirmed launch fence cannot be cleared by simply retrying.
    with pytest.raises(onboarding.CurrentUserOnboardingError):
        resume(r, monkeypatch)
    assert normal_remove(r)['status'] == 'PASS_DATA_PRESERVED'
    assert resume(r, monkeypatch)['status'] == 'READY'
    assert r.state['commits'] == 1


def test_installer_healthy_lifecycle_accepts_recovered_authority(recovery, monkeypatch, capsys):
    import tools.register_label_match_worker_pc as register
    r = recovery
    used_pc(r, monkeypatch)
    portable_install(r)
    native_profile(r, monkeypatch)
    put_residue(r, 'identity')
    assert r.m.main(recovery_argv(r)) == 0
    monkeypatch.setattr(register, '_current_machine_guid', r.m._current_machine_guid)
    monkeypatch.setattr(register, '_current_user_sid', r.m._current_user_sid)
    for name, value in environment(r).items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(sys, 'argv', ['probe', str(r.app), str(r.app), 'inspect', ''])
    installer = (Path(__file__).parents[1] / 'INSTALL_CANONICAL_PORTABLE.ps1').read_text(encoding='utf-8-sig')
    probe = installer.split('function HealthyLifecycle(', 1)[1].split("$probe = @'\n", 1)[1].split("\n'@", 1)[0]
    # Execute the exact stock installer's read-only Python consumer. No
    # PowerShell placement, registry, process or UAC operation runs here.
    namespace = {}
    exec(compile(probe, 'stock-healthy-lifecycle', 'exec'), namespace)
    assert namespace['result']['status'] == 'HEALTHY_CURRENT_USER'
    assert namespace['result']['marker_present'] is True


@pytest.mark.parametrize('status', ['acked', 'operator_review', 'failed_permanent'])
def test_handoff_preserves_terminal_outcomes_and_audits_original_authority(recovery, monkeypatch, status):
    import registration_relay_recovery as handoff
    r = recovery
    used_pc(r, monkeypatch)
    put_residue(r, 'identity')
    with sqlite3.connect(r.database) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute('UPDATE direct_sync_relay_batches SET status=?, receipt_json=?', (status, '{"preserved":true}'))
        before = dict(conn.execute('SELECT * FROM direct_sync_relay_batches').fetchone())
        authority = dict(conn.execute('SELECT * FROM direct_sync_runtime_authority').fetchone())
    r.m.apply_registration(r.args, r.manifest, r.credential, r.report)
    with sqlite3.connect(r.database) as conn:
        conn.row_factory = sqlite3.Row
        after = dict(conn.execute('SELECT * FROM direct_sync_relay_batches').fetchone())
        proof = json.loads(handoff.unprotect_handoff(bytes(conn.execute(
            'SELECT protected_handoff FROM direct_sync_credential_recoveries').fetchone()[0])))
    assert after == {**before, 'key_id': 'test-rotated'}
    assert proof['authority_before'] == authority

"""App callbacks and the real relay transaction around the pinned runtime core."""
import json
import sqlite3

import pytest

import direct_sync_push as push
import producer_runtime_client as client
from kmtech_shared import runtime as core
from tests.test_producer_runtime_client import (
    _LeaseSession,
    _credentials,
    _insert_claimed_row,
    _prepare,
)


def test_identity_and_thumbprint_callbacks_stay_live_across_issue_retry_and_expiry(tmp_path, monkeypatch):
    identities = [client.new_runtime_identity(), client.new_runtime_identity()]
    generated, thumbprints = [], []
    native_thumbprint = client._cng_jwk_thumbprint

    def identity():
        generated.append(identities[len(generated)])
        return generated[-1]

    def thumbprint(jwk):
        thumbprints.append(jwk)
        return native_thumbprint(jwk)

    monkeypatch.setattr(client, 'new_runtime_identity', identity)
    monkeypatch.setattr(client, '_cng_jwk_thumbprint', thumbprint)
    db_path = tmp_path / 'relay.sqlite3'
    session = _LeaseSession(failures=1)
    args = dict(db_path=db_path, credentials=_credentials(), producer_install_id='install-test', session=session)
    assert client.ensure_runtime_authority(**args, now='2026-08-06T00:00:00Z').retryable
    assert len(generated) == 1
    assert client.ensure_runtime_authority(**args, now='2026-08-06T00:00:01Z').error_code == ''
    assert session.calls[0][1] == session.calls[1][1]
    assert len(generated) == 1
    assert client.ensure_runtime_authority(**args, now='2026-08-06T00:00:02Z').error_code == ''
    assert len(session.calls) == 2
    # A later monkeypatch must reach the core too; it must not capture callbacks at import.
    monkeypatch.setattr(client, 'new_runtime_identity', lambda: identities[1])
    assert client.ensure_runtime_authority(**args, now='2099-08-07T00:00:00Z').error_code == ''
    assert session.calls[2][1]['runtime_instance_id'] == identities[1][0]
    assert session.calls[2][1]['public_jwk'] == identities[1][1]
    assert all(value[1] in thumbprints for value in identities)
    with sqlite3.connect(db_path) as conn:
        assert conn.execute('SELECT runtime_instance_id FROM direct_sync_runtime_authority').fetchone() == (identities[1][0],)


@pytest.mark.parametrize('action', ['review', 'disable', 'invalid_release'])
def test_terminal_fallback_uses_current_app_identity_factory(tmp_path, monkeypatch, action):
    db_path = tmp_path / 'relay.sqlite3'
    _insert_claimed_row(db_path, 'relay-a')
    identity = client.new_runtime_identity()
    calls = []

    def generate():
        calls.append(True)
        return identity

    monkeypatch.setattr(client, 'new_runtime_identity', generate)
    with client._connect(db_path) as conn:
        conn.execute('BEGIN IMMEDIATE')
        args = dict(relay_id='relay-a', metadata={'producer_install_id': 'install-test'},
                    credentials=_credentials(), now='2026-08-06T00:00:00Z')
        if action == 'review':
            client.mark_runtime_operator_review_in_transaction(conn, error_code='STALE_RUNTIME_FENCE', **args)
        elif action == 'disable':
            client.disable_runtime_authority_in_transaction(conn, **args)
        else:
            client.release_runtime_request_in_transaction(conn, **args)
        row = conn.execute('SELECT * FROM direct_sync_runtime_authority').fetchone()
        assert row['runtime_instance_id'] == identity[0]
        assert json.loads(row['runtime_public_jwk_json']) == identity[1]
        assert row['status'] == ('LEGACY_DISABLED' if action == 'disable' else 'OPERATOR_REVIEW')
        assert row['next_request_token'] is row['assigned_relay_id'] is None
        assert conn.in_transaction
    assert calls == [True]


def test_live_jwk_validator_rejection_reaches_receipt_and_transaction_facades(tmp_path, monkeypatch):
    db_path = tmp_path / 'relay.sqlite3'
    _insert_claimed_row(db_path, 'relay-a')
    prepared = _prepare(db_path, 'relay-a', _LeaseSession())
    assert prepared.error_code == ''
    calls = []

    def reject(jwk):
        calls.append(jwk)
        raise ValueError('rejected by app validator')

    monkeypatch.setattr(client, 'normalize_public_jwk', reject)
    assert client._metadata_shape_error(prepared.metadata) == 'runtime_public_jwk is invalid'
    rotation, code, message = client.runtime_receipt_result(prepared.metadata, {})
    assert rotation is None and code == 'runtime_lease_metadata_invalid'
    assert message == 'runtime_public_jwk is invalid'
    with client._connect(db_path) as conn:
        conn.execute('BEGIN IMMEDIATE')
        args = dict(relay_id='relay-a', metadata=prepared.metadata, credentials=_credentials(), now='2026-08-06T00:00:01Z')
        before = tuple(conn.execute('SELECT * FROM direct_sync_runtime_authority').fetchone())
        with pytest.raises(ValueError, match='runtime_public_jwk is invalid'):
            client.apply_runtime_receipt_in_transaction(conn, runtime_lease={}, **args)
        assert tuple(conn.execute('SELECT * FROM direct_sync_runtime_authority').fetchone()) == before
        client.release_runtime_request_in_transaction(conn, **args)
        state = conn.execute('SELECT status,next_request_token,assigned_relay_id FROM direct_sync_runtime_authority').fetchone()
        assert tuple(state) == ('OPERATOR_REVIEW', None, None)
    assert len(calls) == 4
    assert all(jwk == prepared.metadata['runtime_public_jwk'] for jwk in calls)


@pytest.mark.parametrize('interrupt', [False, True])
def test_app_ack_and_shared_rotation_have_one_commit_and_rollback(tmp_path, monkeypatch, interrupt):
    db_path = tmp_path / 'relay.sqlite3'
    _insert_claimed_row(db_path, 'relay-a')
    prepared = _prepare(db_path, 'relay-a', _LeaseSession())
    assert prepared.error_code == ''
    lease = dict(contract_version=client.CONTRACT_VERSION, validation_status='consumed',
                 lease_id='lease-test', fence=prepared.metadata['runtime_fence'],
                 next_request_token='R' * 43, next_request_sequence=2, expires_at='2099-08-06T00:00:00Z')

    def snapshot(conn):
        batch = tuple(conn.execute('SELECT status,metadata_json,receipt_json FROM direct_sync_relay_batches').fetchone())
        authority = tuple(conn.execute('SELECT next_request_token,next_request_sequence,assigned_relay_id FROM direct_sync_runtime_authority').fetchone())
        return batch, authority

    with sqlite3.connect(db_path) as reader:
        before = snapshot(reader)
    assert before[0][0] == 'leased' and before[1] == (None, None, 'relay-a')
    apply = core.apply_runtime_receipt_in_transaction
    observed = []

    def apply_before_commit(conn, **kwargs):
        assert conn.in_transaction
        assert snapshot(conn)[0][0] == 'acked'
        assert kwargs['normalize_public_jwk'] is client.normalize_public_jwk
        apply(conn, **kwargs)
        assert conn.in_transaction
        assert snapshot(conn)[1] == ('R' * 43, 2, None)
        with sqlite3.connect(db_path) as reader:
            assert snapshot(reader) == before
        observed.append(True)
        if interrupt:
            raise OSError('simulated interruption after rotation, before ACK commit')

    monkeypatch.setattr(core, 'apply_runtime_receipt_in_transaction', apply_before_commit)
    args = dict(db_path=db_path, relay_id='relay-a', lease_owner='worker', expected_attempt_count=1,
                status=push.RELAY_STATUS_ACKED, runtime_credentials=_credentials(), runtime_lease=lease)
    if interrupt:
        with pytest.raises(OSError, match='simulated interruption'):
            push._set_relay_status(**args)
        with sqlite3.connect(db_path) as reader:
            assert snapshot(reader) == before
        # The exact reserved request is still usable after rollback.
        monkeypatch.setattr(core, 'apply_runtime_receipt_in_transaction', apply)
        push._set_relay_status(**args)
    else:
        push._set_relay_status(**args)
    assert observed == [True]
    with sqlite3.connect(db_path) as reader:
        batch, authority = snapshot(reader)
    assert batch[0] == 'acked' and authority == ('R' * 43, 2, None)
    assert 'runtime_request_token' not in json.loads(batch[1])
    assert 'runtime_request_token_sha256' in json.loads(batch[1])

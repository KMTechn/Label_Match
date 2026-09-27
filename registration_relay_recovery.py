"""Same-PC credential handoff after a verified administrator recovery.

Runtime proof is independent of the HMAC key. Preserve it byte-for-byte so an
already committed source can replay its exact receipt after a lost ACK.
"""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from direct_sync_push import manifest_hash
from logistics_runtime_profile import assert_path_has_no_reparse_components
from tools.register_label_match_worker_pc import (
    _dpapi_protect_current_user as protect_handoff,
    _dpapi_unprotect_current_user as unprotect_handoff,
)
from producer_runtime_client import _scope_key, _scope_values
from user_relay import _acquire_relay_lease, user_relay_stop_path
from user_relay_stop_marker import (
    read_stop_marker, _validated_node, canonical_marker_bytes,
)
from writer_session_fence import writer_sink


HANDOFF_TABLE = 'direct_sync_credential_recoveries'
RESUME_MESSAGE = ('신원 복구는 끝났지만 중앙 전송 릴레이는 정지 상태입니다. '
                  '같은 Windows 사용자로 초기 설정(--onboard-current-user)을 다시 실행하고 '
                  'READY 및 릴레이 ALIVE를 확인하세요. 미전송 기록은 보존됩니다.')


def current_owner():
    from tools.register_label_match_worker_pc import (
        _current_machine_guid, _current_user_sid, derive_path_independent_install_id,
    )
    sid = _current_user_sid()
    return {'user_sid': sid, 'producer_install_id': derive_path_independent_install_id(
        machine_guid=_current_machine_guid(), user_sid=sid)}


def _safe_path(path):
    path = assert_path_has_no_reparse_components(path, label='relay recovery')
    if path.exists() and path.is_file() and path.stat().st_nlink != 1:
        raise ValueError('relay recovery does not accept hard links')
    return path


def _json_file(path):
    path = _safe_path(path)
    if not path.is_file() or path.stat().st_size > 1024 * 1024:
        raise ValueError('relay recovery state is missing or too large')
    value = json.loads(path.read_text(encoding='utf-8-sig'))
    if not isinstance(value, dict):
        raise ValueError('relay recovery state is not an object')
    return value


@contextmanager
def _stopped(root):
    # Hold both leases through readback/commit. A removal report alone is not
    # evidence that a process is still absent or that its marker is unchanged.
    leases = []
    try:
        for name in ('user-relay-stop-marker', 'user-relay-instance'):
            lease = _acquire_relay_lease(root / name)
            if lease is None:
                raise ValueError('relay recovery requires the stopped relay and stable stop marker')
            leases.append(lease)
        yield
    finally:
        for lease in reversed(leases):
            close = getattr(lease, 'close', None) or lease.release
            close()


def _normal_stop(root, app_root):
    marker_path = _safe_path(user_relay_stop_path(root))
    marker, raw, sha = read_stop_marker(marker_path)
    _validated_node(marker)
    removal = _json_file(root / 'status/current_user_removal.json')
    relay = removal.get('relay_process', {})
    if (raw != canonical_marker_bytes(marker)
            or removal.get('status') != 'PASS_DATA_PRESERVED'
            or removal.get('data_preserved') is not True
            or Path(removal.get('machine_code_root', '')).resolve() != app_root.resolve()
            or removal.get('relay_autostart', {}).get('status') != 'ABSENT'
            or removal.get('scheduled_task', {}).get('status') != 'ABSENT'
            or relay.get('status') != 'ABSENT'
            or relay.get('request_id') != marker['request_id']
            or relay.get('stop_request_sha256') != sha):
        raise ValueError('relay recovery requires the exact supported current-user removal')
    return {'request_id': marker['request_id'], 'sha256': sha}


@contextmanager
def _connect(database, *, readonly=False):
    database = _safe_path(database)
    for suffix in ('-wal', '-shm', '-journal'):
        _safe_path(Path(str(database) + suffix))
    conn = sqlite3.connect(database.as_uri() + ('?mode=ro' if readonly else '?mode=rw'), uri=True)
    try:
        conn.row_factory = sqlite3.Row
        if not readonly:
            conn.execute('PRAGMA synchronous=FULL')
        with conn:
            yield conn
    finally:
        conn.close()


def _queue_digest(conn, *, scope=None, manifest=None):
    digest = hashlib.sha256()
    count = 0
    for row in conn.execute('SELECT * FROM direct_sync_relay_batches ORDER BY relay_id'):
        value = dict(row)
        if scope is not None:
            metadata = json.loads(value['metadata_json'])
            if (any(value.get(k) != scope[k] for k in ('producer_id', 'key_id', 'endpoint_url'))
                    or metadata.get('producer_install_id') != scope['producer_install_id']
                    or metadata.get('source_host_id') != manifest['pc_identity']['source_host_id']
                    or metadata.get('manifest_hash') != manifest_hash(manifest)):
                raise ValueError('relay recovery queue belongs to another identity or manifest')
        digest.update(json.dumps(value, sort_keys=True, separators=(',', ':')).encode('utf-8'))
        digest.update(b'\n')
        count += 1
    return {'sha256': digest.hexdigest(), 'count': count}


def prepare_relay_handoff(root, manifest, credential, *, owner):
    """Read-only preflight before the server can rotate credentials."""
    from current_user_onboarding import CANONICAL_PORTABLE_ROOT
    root = _safe_path(Path(root).absolute())
    database = root / 'queue/direct_sync_relay.sqlite3'
    if not database.exists() and not user_relay_stop_path(root).exists():
        return None
    if owner['producer_install_id'] != manifest['pc_identity']['producer_install_id']:
        raise ValueError('relay recovery installation belongs to another Windows user or PC')
    with _stopped(root):
        marker = _normal_stop(root, CANONICAL_PORTABLE_ROOT)
        authority = None
        scope = _scope_values(SimpleNamespace(**credential), owner['producer_install_id'])
        queue = {'sha256': hashlib.sha256().hexdigest(), 'count': 0}
        if database.exists():
            with _connect(database, readonly=True) as conn:
                rows = conn.execute('SELECT * FROM direct_sync_runtime_authority').fetchall()
                if len(rows) > 1:
                    raise ValueError('relay recovery requires one unambiguous runtime authority')
                if rows:
                    authority = dict(rows[0])
                    scope = {k: authority[k] for k in ('producer_id', 'key_id', 'endpoint_url', 'producer_install_id')}
                    expected = _scope_values(SimpleNamespace(**credential), owner['producer_install_id'])
                    if (any(scope[k] != expected[k] for k in scope if k != 'key_id')
                            or authority['authority_scope'] != _scope_key(scope)
                            or authority['status'] not in ('ACTIVE', 'PENDING')
                            or authority['last_error_code']):
                        raise ValueError('relay recovery authority is foreign, quarantined, or requires review')
                else:
                    scope = _scope_values(SimpleNamespace(**credential), owner['producer_install_id'])
                queue = _queue_digest(conn, scope=scope, manifest=manifest)
        return {'root': str(root), 'app_root': str(CANONICAL_PORTABLE_ROOT.resolve()),
                'owner': owner, 'identity': manifest['pc_identity'],
                'manifest_hash': manifest_hash(manifest), 'marker': marker,
                'scope_before': scope, 'authority_before': authority, 'queue_before': queue}


@writer_sink('registration_relay_recovery')
def commit_relay_handoff(preflight, manifest, credential, receipt, authorization_id):
    """Called only after the registration consumer validates the server response."""
    if preflight is None:
        return {'status': 'NOT_REQUIRED'}
    root = Path(preflight['root'])
    database = root / 'queue/direct_sync_relay.sqlite3'
    scope = _scope_values(SimpleNamespace(**credential), preflight['owner']['producer_install_id'])
    if (receipt.get('status') != 'recovered' or receipt.get('recovery_action') != 'ADMIN_RECOVERY'
            or receipt.get('identity_action') != 'REATTACHED'
            or receipt.get('producer_id') != scope['producer_id']
            or receipt.get('producer_install_id') != scope['producer_install_id']
            or receipt.get('key_id') != scope['key_id']
            or receipt.get('active_manifest_hashes') != [preflight['manifest_hash']]
            or type(receipt.get('credential_epoch')) is not int or receipt['credential_epoch'] < 2
            or manifest_hash(manifest) != preflight['manifest_hash']):
        raise ValueError('relay recovery has no verified committed credential binding')
    with _stopped(root):
        if _normal_stop(root, Path(preflight['app_root'])) != preflight['marker']:
            raise ValueError('relay recovery stop marker changed during credential recovery')
        if not database.exists():
            from direct_sync_push import init_relay_queue_schema
            init_relay_queue_schema(database)
        with _connect(database) as conn:
            conn.execute('BEGIN IMMEDIATE')
            before = preflight['authority_before']
            rows = [dict(row) for row in conn.execute('SELECT * FROM direct_sync_runtime_authority')]
            if rows != ([] if before is None else [before]) or _queue_digest(conn) != preflight['queue_before']:
                raise ValueError('relay recovery queue changed during credential recovery')
            conn.execute(f'''CREATE TABLE IF NOT EXISTS {HANDOFF_TABLE} (
                authorization_id TEXT PRIMARY KEY, successor_scope TEXT NOT NULL,
                protected_handoff BLOB NOT NULL)''')
            existing = conn.execute(f'SELECT * FROM {HANDOFF_TABLE} WHERE authorization_id=?',
                                    (authorization_id,)).fetchone()
            if existing is not None:
                proof = json.loads(unprotect_handoff(bytes(existing['protected_handoff'])))
                if (proof['scope_after'] != scope or proof['owner'] != preflight['owner']
                        or proof['marker'] != preflight['marker']):
                    raise ValueError('relay recovery retry differs from the committed handoff')
                return {'status': 'REUSED', 'queue_rows': proof['queue_before']['count'],
                        'message_ko': RESUME_MESSAGE}
            if before is not None:
                conn.execute('UPDATE direct_sync_runtime_authority SET authority_scope=?, key_id=? WHERE authority_scope=?',
                             (_scope_key(scope), scope['key_id'], before['authority_scope']))
            # Terminal rows keep their outcome and receipt too: their binding is
            # part of deduplication, so leaving old keys would block later scans.
            conn.execute('UPDATE direct_sync_relay_batches SET key_id=?', (scope['key_id'],))
            proof = {**preflight, 'schema': 'label-match-relay-recovery-v1',
                     'scope_after': scope, 'authorization_id': authorization_id,
                     'receipt': receipt, 'queue_after': _queue_digest(conn)}
            sealed = protect_handoff(json.dumps(proof, sort_keys=True))
            if json.loads(unprotect_handoff(sealed)) != proof:
                raise ValueError('relay recovery audit protection readback failed')
            conn.execute(f'INSERT INTO {HANDOFF_TABLE} VALUES (?,?,?)',
                         (authorization_id, _scope_key(scope), sealed))
        return {'status': 'TRANSFERRED', 'queue_rows': preflight['queue_before']['count'],
                'message_ko': RESUME_MESSAGE}


def recovered_stop_release(paths):
    """Read the protected local commit; never treat a public report as authority."""
    from direct_sync_runtime import load_credentials_from_json
    database = _safe_path(paths.direct_sync_root / 'queue/direct_sync_relay.sqlite3')
    if not database.exists():
        return None
    with _connect(database, readonly=True) as conn:
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (HANDOFF_TABLE,)).fetchone() is None:
            return None
    credential = load_credentials_from_json(paths.credential_path)
    identity = _json_file(paths.identity_path)
    owner = current_owner()
    if owner['producer_install_id'] != identity['producer_install_id']:
        raise ValueError('relay resume belongs to another Windows user or PC')
    scope = _scope_values(credential, identity['producer_install_id'])
    with _connect(database, readonly=True) as conn:
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (HANDOFF_TABLE,)).fetchone() is None:
            return None
        rows = conn.execute(f'SELECT protected_handoff FROM {HANDOFF_TABLE} WHERE successor_scope=?',
                            (_scope_key(scope),)).fetchall()
        if len(rows) != 1:
            raise ValueError('relay recovery handoff is absent or ambiguous')
        proof = json.loads(unprotect_handoff(bytes(rows[0][0])))
        if (proof['schema'] != 'label-match-relay-recovery-v1' or proof['owner'] != owner
                or proof['scope_after'] != scope or proof['root'] != str(paths.direct_sync_root)
                or Path(proof['app_root']).resolve() != paths.app_root.resolve()
                or any(identity.get(k) != v for k, v in proof['identity'].items())
                or proof['manifest_hash'] != manifest_hash(_json_file(paths.producer_manifest_path))):
            raise ValueError('relay recovery handoff identity differs')
        secret = credential.secret
        if isinstance(secret, str):
            secret = secret.encode('utf-8')
        if hashlib.sha256(secret).hexdigest() != proof['receipt']['secret_fingerprint_sha256']:
            raise ValueError('relay recovery credential fingerprint differs')
        authorities = [dict(row) for row in conn.execute('SELECT * FROM direct_sync_runtime_authority')]
        if any(row['authority_scope'] != _scope_key(scope)
               or any(row[k] != v for k, v in scope.items())
               or row['status'] not in ('ACTIVE', 'PENDING') or row['last_error_code'] for row in authorities):
            raise ValueError('relay recovery current authority requires review')
    marker = _normal_stop(paths.direct_sync_root, paths.app_root)
    # A later supported removal is independently authoritative for its exact
    # current marker. This also permits retry after onboarding re-fenced a
    # failed launch; that unconfirmed marker alone never authorizes release.
    return {'status': 'ADMIN_RECOVERY_COMMITTED', 'marker_present': True,
            'current_stop_marker_request_id': marker['request_id'],
            'current_stop_marker_sha256': marker['sha256']}

"""Isolated real CLI/reader probes for one external input at a time."""
import contextlib
import hashlib
import io
import json
import os
import re
from pathlib import Path
import sqlite3
import sys
from types import SimpleNamespace

repo, root, kind, state, spelling, output = sys.argv[1:]
repo, root, output = map(Path, (repo, root, output))
root.mkdir(parents=True, exist_ok=False)
sys.path.insert(0, str(repo))
for name, relative in {'LOCALAPPDATA': 'local', 'APPDATA': 'roaming',
                       'PROGRAMDATA': 'program', 'LABEL_MATCH_SAVE_DIR': 'data',
                       'TEMP': 'temp', 'TMP': 'temp', 'USERPROFILE': 'home',
                       'KMTECH_LABEL_WRITER_CONTROL_ROOT': 'writer-control'}.items():
    os.environ[name] = str(root / relative)
os.environ['KMTECH_LABEL_WRITER_TEST_MODE'] = '1'
os.environ.pop('LABEL_MATCH_SETTINGS_PATH', None)
os.environ.pop('LABEL_MATCH_LOGISTICS_PROFILE_PATH', None)
os.environ.pop('LABEL_MATCH_DIRECT_SYNC_ROOT', None)
os.environ.pop('LABEL_MATCH_DIRECT_SYNC_PROGRAM_DATA_ROOT', None)
import protected_admin
import label_data_generation as generation
from kmtech_shared.data_generation import record_generation

sid = '-'.join(map(str, ('S', 1, 5, 21, 900, 901, 902, 1001)))
protected_admin.current_process_sid = lambda: sid
generation._installed_identity = lambda _root: {
    'version': generation.CURRENT_VERSION, 'build_id': 'a' * 40,
    'data_compatible_min_version': generation.CURRENT_VERSION,
    'compatibility_verified': False,
}
external = root / 'home' / 'external'
external.mkdir(parents=True)
os.chdir(root)
def requested(path):
    if spelling == 'tilde' and path.is_relative_to(root / 'home'):
        return '~/' + path.relative_to(root / 'home').as_posix()
    if spelling == 'relative':
        return os.path.relpath(path, root)
    return str(path)

def blocked(*_args, **_kwargs):
    raise AssertionError('native/network/lifecycle action blocked')
import requests
requests.sessions.Session.request = blocked
import winreg
winreg.SetValueEx = blocked
winreg.DeleteValue = blocked

events = {'input_reads': 0, 'sqlite_opens': 0, 'post_admission_calls': 0}
target = external / 'input.json'
if kind == 'settings':
    from tools import direct_sync_relay_install_pack as entry
    target.write_text(json.dumps({'custom_save_path': str(root / 'scan')}), encoding='utf-8')
    def safe_plan(args, run_preflight=False):
        events['post_admission_calls'] += 1
        entry._app_save_path_scan_dir_check(Path(args.app_root), args.scan_source_dir, args.app_settings_path)
        return {'task_principal': {'status': 'BLOCKED', 'blocked_reason': 'isolated lifecycle boundary'}}
    entry.build_install_plan = safe_plan
    argv = ['--app-root', str(repo), '--app-settings-path', requested(target),
            '--program-data-root', str(root / 'direct'), '--scan-source-dir', str(root / 'scan'),
            '--producer-manifest-path', str(root / 'identity/producer_manifest.json'),
            '--credential-path', str(root / 'identity/credential.json'), '--report-path', str(root / 'report.json')]
elif kind == 'spool':
    from tools import direct_sync_relay_operator as entry
    from direct_sync_push import RELAY_STATUS_ACKED
    target = external / 'batch.csv'
    target.write_bytes(b'synthetic,isolated\n')
    db = root / 'queue/relay.sqlite3'
    db.parent.mkdir()
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE direct_sync_relay_batches (relay_id TEXT,status TEXT,spooled_file_path TEXT,content_sha256 TEXT,byte_length INTEGER,metadata_json TEXT,producer_id TEXT,key_id TEXT,endpoint_url TEXT)')
        conn.execute('INSERT INTO direct_sync_relay_batches VALUES(?,?,?,?,?,?,?,?,?)',
                     ('isolated', RELAY_STATUS_ACKED, str(target), hashlib.sha256(target.read_bytes()).hexdigest(),
                      target.stat().st_size, '{}', 'producer', 'isolated',
                      'https://example.invalid/api/producer-ingest/v1/source-file'))
    entry.load_credentials_from_json = lambda *_a, **_k: SimpleNamespace(
        endpoint_url='https://example.invalid/api/producer-ingest/v1/source-file', producer_id='producer',
        key_id='isolated')
    argv = ['restore-spool', '--db-path', str(db), '--relay-id', 'isolated', '--spool-root', requested(external),
            '--credential-path', str(root / 'identity/credential.json'), '--operator-id', 'isolated', '--reason', 'isolated']
elif kind in ('stop', 'reconcile-stop'):
    from tools import label_exact_clone_resolution_receipt as receipt
    target.write_text(json.dumps({'request_id': 'isolated'}), encoding='utf-8')
    inputs = ['--client-db', str(root / 'client/queue.sqlite3'), '--server-db', str(root / 'server/server.sqlite3'),
              '--identity', str(root / 'identity/producer_identity.json'), '--credential', str(root / 'identity/credential.json'),
              '--stop-marker', requested(target), '--portable-root', str(repo)]
    if kind == 'stop':
        entry = receipt
        argv = ['capture', *inputs, '--output', str(root / 'capture.json')]
    else:
        from tools import label_guarded_runtime_reconcile as entry
        def safe_reconcile(**kwargs):
            events['post_admission_calls'] += 1
            from label_exact_clone_resolution import capture_conflict_preimage
            capture_conflict_preimage(**{name: kwargs[name] for name in (
                'client_db_path', 'server_db_path', 'identity_path', 'credential_path', 'stop_marker_path', 'portable_root')})
        entry.run_guarded_runtime_reconcile = safe_reconcile
        argv = [*inputs, '--preimage', str(root / 'proof/preimage.json'), '--client-backup', str(root / 'proof/client.sqlite3'),
                '--client-backup-sha256', 'b' * 64, '--live-client-proof', str(root / 'proof/live.json'),
                '--live-client-proof-sha256', 'b' * 64, '--server-initializer-proof', str(root / 'proof/server.json'),
                '--server-initializer-proof-sha256', 'b' * 64, '--server-source-root', str(repo),
                '--server-source-commit', 'b' * 40, '--server-launcher', str(root / 'proof/launcher.py'),
                '--server-launcher-sha256', 'b' * 64, '--server-executable', sys.executable,
                '--server-executable-sha256', 'b' * 64, '--tls-ca-bundle', str(root / 'proof/ca.pem'),
                '--tls-ca-bundle-sha256', 'b' * 64, '--evidence-output', str(root / 'evidence.json'),
                '--receipt-output', str(root / 'receipt.json')]
elif kind in ('recovery-db', 'recovery-key', 'recovery-empty-db', 'recovery-empty-key', 'recovery-empty-barrier'):
    from tools import label_auth_recovery_canary as entry
    db = (external if kind == 'recovery-db' else root / 'db') / 'existing.sqlite3'
    db.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db) as conn:
        conn.execute('CREATE TABLE preserved_input(value TEXT)')
    seal = (external if kind == 'recovery-key' else root / 'key') / 'seal.dpapi'
    seal.parent.mkdir(parents=True, exist_ok=True)
    seal.write_bytes(b'isolated invalid sealed input')
    target = db if kind == 'recovery-db' else seal
    argv = ['--_recovery-child', '--_recovery-db', requested(db), '--_seal-key-path', requested(seal),
            '--_barrier-path', str(root / 'barrier.json')]
    if kind.startswith('recovery-empty-'):
        field = {'db': '--_recovery-db', 'key': '--_seal-key-path', 'barrier': '--_barrier-path'}[kind.rsplit('-', 1)[1]]
        argv[argv.index(field) + 1] = ''
else:
    raise AssertionError('unknown isolated entry')

marker = external / 'data-generation.json'
if state != 'absent':
    record_generation(marker, 'v3.0.0' if state == 'high' else generation.CURRENT_VERSION,
                      app_id=generation.APP_ID, subject_sid=sid, build_id='a' * 40)
    if state == 'corrupt':
        marker.write_bytes(b'{broken')
original_open, original_connect = Path.open, sqlite3.connect
def snapshot():
    return {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob('*') if p.is_file()}
before = snapshot()
control_absent = not any((root / 'writer-control/data-generation').glob('*.json'))
def observed_open(path, mode='r', *args, **kwargs):
    if Path(path).resolve() == target.resolve() and 'r' in mode:
        events['input_reads'] += 1
    return original_open(path, mode, *args, **kwargs)
def observed_connect(database, *args, **kwargs):
    if kind.startswith('recovery') and str(Path(database).resolve()) == str(db.resolve()):
        events['sqlite_opens'] += 1
    return original_connect(database, *args, **kwargs)
Path.open, sqlite3.connect = observed_open, observed_connect
stdout, stderr = io.StringIO(), io.StringIO()
error_type = ''
with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
    try:
        code = entry.main(argv)
    except Exception as exc:
        code = -1
        error_type = type(exc).__name__
        stderr.write(type(exc).__name__)
Path.open, sqlite3.connect = original_open, original_connect
after = snapshot()
def safe_name(name):
    return re.sub(r'S-1-\d+(?:-\d+)+', '<subject>', name)
changed = sorted(safe_name(name) for name in before if after.get(name) != before[name])
added = sorted(safe_name(name) for name in after if name not in before and 'startup-trace' not in name)
message = stdout.getvalue() + stderr.getvalue()
result = {'kind': kind, 'state': state, 'spelling': spelling, 'exit': code, **events,
          'control_marker_absent_before': control_absent, 'changed': changed, 'added': added, 'error_type': error_type,
          'guidance': '자료' in message and '복구' in message,
          'refused_before_read': not any(events.values()) and not changed and not added
                                and code != 0 and '자료' in message and '복구' in message}
output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')

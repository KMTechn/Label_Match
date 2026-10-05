"""Isolated real G/file readers; only identity, decryption and HTTP are adapters."""
import ast
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import ssl
import sys
from types import SimpleNamespace

repo, work, consumer, state, output = sys.argv[1:]
repo, work, output = Path(repo), Path(work), Path(output)
fixture = Path(__file__).with_name('_data_generation_pin_fixture.py')
sys.argv = [str(fixture), str(repo), str(work), 'explicit', 'same', str(output)]
setup = {'__name__': 'isolated_late_reader_setup'}
with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
    exec(compile(fixture.read_text(encoding='utf-8'), str(fixture), 'exec'), setup)
generation = setup['generation']
runtime = setup['runtime']
events = {'locator_reads': 0, 'secret_reads': 0, 'decrypt_calls': 0,
          'tls_reads': 0, 'connections': 0, 'mutations': 0}
ca = setup['external'] / 'private-ca.pem'
import certifi
shutil.copyfile(certifi.where(), ca)
source = work / 'source.csv'
source.write_bytes(b'isolated-data\n')
import direct_sync_push as push
credentials = runtime.load_credentials_from_json(setup['credential'])
metadata = {'contract_version': push.CONTRACT_VERSION,
            'content_sha256': hashlib.sha256(source.read_bytes()).hexdigest(),
            'manifest_hash': hashlib.sha256(b'isolated-manifest').hexdigest(),
            'byte_length': source.stat().st_size, 'relative_path': 'legacy_csv/source.csv'}
metadata.update({field: metadata.get(field, 'isolated') for field in push.RESTORE_REQUIRED_METADATA_FIELDS})
plan = push.SourceFilePlan(str(source), metadata, metadata['content_sha256'], metadata['byte_length'])
prepared = runtime.prepare_credentials_from_json(setup['credential'], app_root=repo)

def mutate(*args, **kwargs):
    events['mutations'] += 1
    return {'paused': False, 'status': 'pass'}

original_context = ssl.create_default_context
def tls_context(*args, **kwargs):
    if kwargs.get('cafile'):
        events['tls_reads'] += 1
    return original_context(*args, **kwargs)
ssl.create_default_context = tls_context

class Response:
    status_code = 200
    reason = 'OK'
    headers = {}
    content = b'{}'
    def json(self): return {}
    def raise_for_status(self): return None
    def close(self): return None
    def read(self): return b'{}'
    def __enter__(self): return self
    def __exit__(self, *args): return None
    def iter_content(self, **kwargs): yield b''

def request(*args, **kwargs):
    if kwargs.get('verify'):
        ssl.create_default_context(cafile=kwargs['verify'])
    events['connections'] += 1
    return Response()
session = SimpleNamespace(get=request, post=request)

import package_logistics as package
package.urlopen = request
import producer_runtime_client as lease
import item_catalog_sync as catalog
import logistics_runtime_profile as profile_module

if consumer == 'catalog':
    profile = SimpleNamespace(base_url='https://reader.invalid', bearer_token=credentials.secret,
        source_host_id='isolated', device_id='isolated', authority_scope='isolated',
        tls_ca_bundle_path=str(ca), isolated_qualification_authority_id='')
    catalog._load_item_catalog_logistics_profile = lambda: profile
    catalog._is_trusted_authenticated_catalog_url = lambda *args: True
    catalog._profile_secret_values = lambda *args: ()
if consumer == 'profile':
    # Use the existing profile serializer fixture's schema, without invoking DPAPI.
    profile_root = setup['external'] / 'profile-owner'
    profile_root.mkdir()
    profile_path = profile_root / 'profile.json'
    secret_path = profile_root / 'secrets' / 'profile.dpapi'
    ca = profile_root / 'private-ca.pem'
    shutil.copyfile(certifi.where(), ca)
    fixture_tree = ast.parse((repo/'tests/test_logistics_runtime_profile.py').read_text(encoding='utf-8-sig'))
    fixture_node = next(n for n in fixture_tree.body if isinstance(n, ast.FunctionDef) and n.name == '_profile')
    fixture_ns = {'Path': Path, 'json': json}
    exec(compile(ast.Module(body=[fixture_node], type_ignores=[]), 'existing-valid-profile-fixture', 'exec'), fixture_ns)
    fixture_ns['_profile'](setup['external'], profile_path=profile_path,
        bearer_token_ref='dpapi:secrets/profile.dpapi', tls_ca_bundle_path=str(ca))
    secret_path.write_bytes(b'isolated-protected-profile')
    setup['target'] = secret_path
if consumer in {'relay', 'enqueue'}:
    runtime._paused_by_operator = mutate
    runtime._disk_pressure_report = mutate
    runtime._queue_backpressure_report = mutate
    runtime.reset_stale_relay_leases = lambda **kwargs: 0

generation.check_user_data_open(repo, (setup['external'],))
if state == 'late-high':
    from kmtech_shared.data_generation import record_generation
    version = list(map(int, str(setup['identity']['version']).lstrip('v').split('.')))
    version[-1] += 1
    version = 'v' + '.'.join(map(str, version))
    record_generation([secret_path.parent/'data-generation.json'] if consumer=='profile' else setup['marker'], app_id='Label_Match', subject_sid=setup['subject'],
        version=version, build_id=hashlib.sha256(b'isolated-higher-build').hexdigest()[:40], data_compatible_min_version=version)

before = setup['snapshot']()
original_open = Path.open
def observed_open(path, mode='r', *args, **kwargs):
    if 'r' in mode:
        if Path(path).resolve() == setup['credential'].resolve(): events['locator_reads'] += 1
        if Path(path).resolve() == setup['target'].resolve(): events['secret_reads'] += 1
    return original_open(path, mode, *args, **kwargs)
Path.open = observed_open
original_decrypt = runtime._dpapi_unprotect_current_user
def decrypt(value):
    events['decrypt_calls'] += 1
    return original_decrypt(value)
runtime._dpapi_unprotect_current_user = decrypt
error_type = ''
returned = False
try:
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        if consumer == 'package':
            package._default_transport('POST', 'https://reader.invalid', {}, b'{}', 1,
                                       tls_ca_bundle_path=str(ca))
        elif consumer == 'lease':
            lease._post_lease_request(request_value={}, credentials=credentials, session=session,
                                      timeout=1, tls_ca_bundle_path=str(ca))
        elif consumer == 'restore':
            push.restore_raw_artifact_to_file(credentials=credentials, metadata=metadata,
                destination_path=work / 'restored.csv', session=session, tls_ca_bundle_path=str(ca))
        elif consumer == 'upload':
            push.upload_source_file(plan, credentials, session=session, tls_ca_bundle_path=str(ca))
        elif consumer == 'catalog':
            catalog.refresh_item_catalog(bundled_path=repo / 'assets/Item.csv',
                cache_path=work / 'catalog/Item.csv', get=request)
        elif consumer == 'profile':
            profile_module.load_logistics_runtime_profile(True, profile_path=profile_path,
                decryptor=lambda value: (events.__setitem__('decrypt_calls', events['decrypt_calls']+1) or hashlib.sha256(value).hexdigest()))
        elif consumer in {'relay', 'enqueue'}:
            config = runtime.DirectSyncRuntimeConfig(work/'queue.sqlite', work/'spool',
                work/'manifest.json', setup['credential'], work/'uploads', work/'status.json', work/'events.jsonl')
            if consumer == 'relay': runtime.run_relay_once(config, credential_input=prepared, session=session)
            else: runtime.enqueue_completed_source_file(config, source_file_path=source, credential_input=prepared)
        elif consumer == 'update':
            tree = ast.parse((repo/'Label_Match.py').read_text(encoding='utf-8-sig'))
            node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_check_private_manifest_for_updates')
            ns = {'Path': Path, '_label_match_runtime_app_root': lambda: repo,
                  '_get_update_manifest_url': lambda: 'https://reader.invalid/manifest',
                  '_get_update_manifest_public_key': lambda: 'isolated-public-key',
                  '_assert_https_update_url': lambda *args: None, '_is_github_hosted_update_url': lambda *args: False,
                  'requests': SimpleNamespace(get=request), '_get_update_manifest_signature_url': lambda url: url,
                  '_verify_update_manifest_signature': lambda *args: None, '_get_update_channel': lambda: 'stable',
                  '_update_candidate_from_manifest': lambda *args, **kwargs: {}}
            exec(compile(ast.Module(body=[node], type_ignores=[]), 'actual-update-reader', 'exec'), ns)
            ns[node.name](tls_ca_bundle_path=str(ca))
        returned = True
except Exception as exc:
    error_type = type(exc).__name__
finally:
    Path.open = original_open
result = {'consumer': consumer, 'state': state, 'returned': returned, 'error_type': error_type,
          'events': events, 'state_unchanged': setup['snapshot']() == before,
          'native_calls': setup['events']['native_calls']}
output.write_text(json.dumps(result, indent=2), encoding='utf-8')
print(json.dumps(result))

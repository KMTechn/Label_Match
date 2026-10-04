"""Independent admission probes; all paths and identities are synthetic."""
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import sys

repo, root, mode, state, output = sys.argv[1:]
repo, root, output = Path(repo), Path(root), Path(output)
root.mkdir(parents=True, exist_ok=False)
sys.path.insert(0, str(repo))
for name, part in [('LOCALAPPDATA','local'),('APPDATA','roaming'),('PROGRAMDATA','program'),
                   ('LABEL_MATCH_SAVE_DIR','data'),('TEMP','temp'),('TMP','temp'),
                   ('KMTECH_LABEL_WRITER_CONTROL_ROOT','writer-control'),
                   ('LABEL_MATCH_DIRECT_SYNC_ROOT','direct'),('LABEL_MATCH_SETTINGS_PATH','settings.json'),
                   ('LABEL_MATCH_LOGISTICS_PROFILE_PATH','profile/profile.json')]:
    os.environ[name] = str(root / part)
os.environ['KMTECH_LABEL_WRITER_TEST_MODE'] = '1'
import protected_admin
import label_data_generation as generation
from kmtech_shared.data_generation import record_generation
subject = '-'.join(map(str, ['S',1,5,21,257,900,901,1001]))
protected_admin.current_process_sid = lambda: subject
generation._installed_identity = lambda _root: {'version': generation.CURRENT_VERSION,
    'build_id': 'a' * 40, 'data_compatible_min_version': generation.CURRENT_VERSION,
    'compatibility_verified': False}
external = root / 'external-ca'
if mode == 'onboarding-default-ca':
    external = root / 'local' / 'KMTech' / 'Bootstrap' / 'Label_Match'
elif mode == 'admin-profile-default':
    external = root / 'program' / 'KMTech' / 'Label_Match' / 'protected'
external.mkdir(parents=True)
ca = external / ('protected_admin.json' if mode.startswith('admin-profile') else 'ca-bundle.pem' if mode == 'onboarding-default-ca' else 'bundle.pem')
ca.write_bytes(b'isolated probe CA input')
if mode.startswith('admin-profile'):
    import secrets
    profile_secret=f'{secrets.randbelow(1000000):06d}'
    ca.write_text(json.dumps(protected_admin.build_protected_admin_profile(profile_secret)),encoding='utf-8')
    if mode == 'admin-profile':
        os.environ[protected_admin.PROTECTED_ADMIN_PROFILE_ENV]=str(ca)
    else:
        os.environ.pop(protected_admin.PROTECTED_ADMIN_PROFILE_ENV,None)
if mode in ('onboarding-valid-ca','onboarding-default-ca','logistics-ca'):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID
    from datetime import datetime, timedelta, timezone
    ephemeral_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'isolated admission probe')])
    now = datetime.now(timezone.utc)
    certificate = (x509.CertificateBuilder().subject_name(name).issuer_name(name)
        .public_key(ephemeral_key.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(now-timedelta(days=1)).not_valid_after(now+timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True,path_length=None),critical=True)
        .sign(ephemeral_key, hashes.SHA256()))
    ca.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    del ephemeral_key
marker = external / 'data-generation.json'
if state != 'absent':
    record_generation(marker, generation.CURRENT_VERSION if state == 'same' else 'v3.0.0',
                      app_id=generation.APP_ID, subject_sid=subject,
                      build_id=('a' if state == 'same' else 'b') * 40)
    if state == 'unknown':
        marker.write_bytes(b'{')
events = {'ca_reads':0,'native_calls':0,'network_calls':0}
original_open = Path.open
def observed(path, mode='r', *args, **kwargs):
    if Path(path).resolve() == ca.resolve() and 'r' in mode:
        events['ca_reads'] += 1
    return original_open(path, mode, *args, **kwargs)
Path.open = observed
def blocked_native(*args, **kwargs):
    events['native_calls'] += 1
    raise AssertionError('native action blocked')
import requests
def blocked_network(*args, **kwargs):
    events['network_calls'] += 1
    raise AssertionError('network action blocked')
requests.sessions.Session.request = blocked_network
import winreg
winreg.SetValueEx = winreg.DeleteValue = blocked_native
before = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
          for p in root.rglob('*') if p.is_file()}
events['ca_reads'] = 0
refused_directly = False
try:
    generation.check_user_data_open(repo, (external,))
except generation.DataGenerationError:
    refused_directly = True
stdout, stderr = io.StringIO(), io.StringIO()
error_kind = ''
try:
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        if mode == 'hosted-raw':
            import label_match_product_host as host
            argv = [host.DIRECT_SYNC_RELAY_MODE]
            for option, path in [('--db-path','queue/db.sqlite3'),('--spool-dir','spool'),
                                ('--producer-manifest-path','identity/manifest.json'),
                                ('--credential-path','identity/credential.json'),
                                ('--upload-status-dir','upload'),('--runtime-status-path','status.json'),
                                ('--log-path','relay.jsonl')]:
                argv.extend([option, str(root/path)])
            argv.extend(['--tls-ca-bundle-path', str(ca)])
            code = host.dispatch_product_mode(argv)
        elif mode in ('onboarding', 'onboarding-default-ca', 'onboarding-valid-ca'):
            import current_user_onboarding as onboarding
            if mode in ('onboarding','onboarding-valid-ca'):
                os.environ[onboarding.ENROLLMENT_TLS_CA_BUNDLE_PATH_ENV] = str(ca)
            else:
                os.environ.pop(onboarding.ENROLLMENT_TLS_CA_BUNDLE_PATH_ENV, None)
            onboarding.inspect_current_user_state = lambda *args, **kwargs: {'status':'READY'}
            legacy = lambda: {'schema':onboarding.LEGACY_TASK_QUIESCENCE_VERSION,
                             'required_state':onboarding.LEGACY_TASK_REQUIRED_STATE,
                             'read_only':True,'task_or_process_mutated':False,'status':'PASS'}
            try:
                onboarding.onboard_current_user(repo, require_bootstrap_integrity=False,
                    legacy_task_quiescence_reader=legacy, autostart_installer=blocked_native,
                    scheduled_task_remover=blocked_native, relay_launcher=blocked_native)
                code = 0
            except Exception as exc:
                code = 1
                error_kind = type(exc).__name__
        elif mode.startswith('admin-profile'):
            events['profile_accepted']=protected_admin.is_protected_admin_code(profile_secret)
            code=0 if events['profile_accepted'] else 1
        elif mode == 'logistics-ca':
            import secrets
            import package_logistics as logistics
            os.environ['LABEL_MATCH_LOGISTICS_TLS_CA_BUNDLE_PATH'] = str(ca)
            selected = dict(os.environ)
            selected['KM_LOGISTICS_RUNTIME_REQUIRED'] = '0'
            selected['LABEL_MATCH_LOGISTICS_API_BASE_URL'] = 'https://isolated.invalid'
            selected['LABEL_MATCH_LOGISTICS_API_TOKEN'] = secrets.token_hex(16)
            selected['LABEL_MATCH_LOGISTICS_SOURCE_HOST_ID'] = secrets.token_hex(8)
            logistics.load_logistics_runtime_profile = lambda *args, **kwargs: None
            client = logistics.package_client_from_env(environ=selected, probe_required=False)
            events['selected_path_matches'] = client.config.tls_ca_bundle_path == str(ca.resolve())
            code = 0
        else:
            raise ValueError('unknown probe')
except Exception as exc:
    code = 1
    error_kind = type(exc).__name__

Path.open = original_open
after = {str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
         for p in root.rglob('*') if p.is_file()}
result = {'mode':mode,'state':state,'exit':code,**events,'error_kind':error_kind,
          'same_external_root_G_refused':refused_directly,
          'added_files':sorted(set(after)-set(before)),
          'added_file_count':len(set(after)-set(before)),
          'changed_original_files':sorted(p for p in before if after.get(p)!=before[p]),
          'generation_records_added':sum(p.endswith('.json') and 'data-generation' in p for p in set(after)-set(before)),
          'scope':'real entry and admission; synthetic release/SID/known folders; onboarding state and task observation stand-ins'}
output.write_text(json.dumps(result,indent=2),encoding='utf-8')
print(json.dumps({k:v for k,v in result.items() if k not in ('added_files','changed_original_files','scope')}))

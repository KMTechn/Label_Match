import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import secrets
import sys

repo, root, mode, state, output = sys.argv[1:]
repo, root, output = Path(repo), Path(root), Path(output)
root.mkdir(parents=True, exist_ok=False)
for name, part in {
    'LOCALAPPDATA':'local', 'APPDATA':'roaming', 'PROGRAMDATA':'program',
    'LABEL_MATCH_SAVE_DIR':'data', 'LABEL_MATCH_SETTINGS_PATH':'settings/input.json',
    'LABEL_MATCH_LOGISTICS_PROFILE_PATH':'profile/input.json',
    'LABEL_MATCH_DIRECT_SYNC_ROOT':'sync', 'KMTECH_LABEL_WRITER_CONTROL_ROOT':'control',
    'TEMP':'temp', 'TMP':'temp',
}.items():
    os.environ[name]=str(root/part)
os.environ['KMTECH_LABEL_WRITER_TEST_MODE']='1'
for name in ('LABEL_MATCH_ENROLLMENT_TLS_CA_BUNDLE_PATH',
             'LABEL_MATCH_LOGISTICS_TLS_CA_BUNDLE_PATH',
             'KMTECH_LABEL_CONFLICT_RESOLUTION_RECEIPT_PATH',
             'LABEL_MATCH_PROTECTED_ADMIN_PROFILE'):
    os.environ.pop(name,None)
sys.path.insert(0,str(repo))
import protected_admin
import label_data_generation as generation
from kmtech_shared.data_generation import record_generation
from tools import direct_sync_relay_operator as entry
import direct_sync_runtime as runtime
from direct_sync_push import DEFAULT_ENDPOINT_PATH
import requests
subject='-'.join(map(str,('S',1,5,21,266,901,902,1001)))
protected_admin.current_process_sid=lambda:subject
external=root/'external' if mode=='explicit' else root/'local'/'CompanyProducerConnector'
external.mkdir(parents=True)
target=external/'secrets'/(secrets.token_hex(8)+'.dpapi')
target.parent.mkdir()
target.write_bytes(secrets.token_bytes(32))
credential=root/'credential'/'input.json'
credential.parent.mkdir()
payload={'producer_id':secrets.token_hex(8),'key_id':secrets.token_hex(8),
         'secret_ref':'dpapi:'+target.stem,
         'endpoint_url':'https://example.invalid'+DEFAULT_ENDPOINT_PATH}
if mode=='explicit': payload['secret_data_dir']=str(external)
credential.write_text(json.dumps(payload),encoding='utf-8')
identity=generation.release_identity(repo)
marker=external/'data-generation.json'
if state!='absent':
    record_generation(marker,identity['version'] if state=='same' else 'v999.0.0',
        app_id=generation.APP_ID,subject_sid=subject,build_id=str(identity['build_id']))
    if state=='unknown': marker.write_bytes(b'{}')
events={'secret_reads':0,'decrypt_calls':0,'downstream_calls':0,'network_calls':0,'native_calls':0}
def decrypt(_):
    events['decrypt_calls']+=1
    return secrets.token_hex(32).encode()
runtime._dpapi_unprotect_current_user=decrypt
def downstream(**kwargs):
    events['downstream_calls']+=1
    return {'status':'READY'}
entry.restore_relay_spool_from_server=downstream
def blocked_network(*a,**k):
    events['network_calls']+=1
    raise AssertionError('network blocked')
requests.sessions.Session.request=blocked_network
import winreg
def blocked_native(*a,**k):
    events['native_calls']+=1
    raise AssertionError('native mutation blocked')
winreg.SetValueEx=winreg.DeleteValue=blocked_native
def snapshot():
    return {str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob('*') if p.is_file()}
before=snapshot()

"""Exercise exact GUI function AST with isolated native/data stand-ins."""
import ast
import contextlib
import io
import json
from pathlib import Path
import runpy
import sys
from types import SimpleNamespace

evidence=Path(__file__).resolve().parent
repo=Path(sys.argv[1])
scenario=sys.argv[3]
output=Path(sys.argv[4])
work=Path(sys.argv[2])
sys.argv=[str(evidence/'_data_generation_pin_fixture.py'),str(repo),str(work),'explicit','same',str(output)]
probe_tree=ast.parse((evidence/'_data_generation_pin_fixture.py').read_text(encoding='utf-8-sig'))
setup={'__name__':'isolated_fixture_setup'}
with contextlib.redirect_stdout(io.StringIO()),contextlib.redirect_stderr(io.StringIO()):
    # Fixture setup only: omit the operator execution and every protected read.
    exec(compile(ast.Module(body=[n for n in probe_tree.body if n.lineno<=76],type_ignores=[]),
                 'private-fixture-setup','exec'),setup)
import label_data_generation as generation
from kmtech_shared.data_generation import record_generation
from label_admin_pin import AdminPinClient
from current_user_onboarding import resolve_current_user_onboarding_paths
actual_paths=resolve_current_user_onboarding_paths(repo)
actual_paths.credential_path.parent.mkdir(parents=True,exist_ok=True)
actual_paths.credential_path.write_bytes(setup['credential'].read_bytes())
setup['credential']=actual_paths.credential_path
import certifi, shutil
ca_path=setup['external']/'private-ca.pem'
shutil.copyfile(certifi.where(),ca_path)

tree=ast.parse((repo/'Label_Match.py').read_text(encoding='utf-8-sig'))
open_node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_open_label_data_manager')
run_node=next(n for c in tree.body if isinstance(c,ast.ClassDef) for n in c.body if isinstance(n,ast.FunctionDef) and n.name=='_run_package_pin_action')
pin_node=next(n for c in tree.body if isinstance(c,ast.ClassDef)
              for n in c.body if isinstance(n,ast.FunctionDef) and n.name=='_package_pin_client')
ns={'Path':Path,'record_user_data_open':generation.record_user_data_open,
    'check_user_data_open':generation.check_user_data_open,'DataGenerationError':generation.DataGenerationError,
    '_label_match_runtime_app_root':lambda:repo,
    'resolve_current_user_onboarding_paths':resolve_current_user_onboarding_paths,
    'DataManager':lambda *a,**k:object(), 'AdminPinClient':AdminPinClient,
    '_label_match_client_tls_ca_bundle_path':lambda client:''}
exec(compile(ast.Module(body=[open_node,pin_node,run_node],type_ignores=[]),'actual-GUI-functions','exec'),ns)
version=list(map(int,str(setup['identity']['version']).lstrip('v').split('.')))
version[2]+=1
def advance_owner():
    record_generation(setup['marker'],'v'+'.'.join(map(str,version)),app_id=generation.APP_ID,
                      subject_sid=setup['subject'],build_id=str(setup['identity']['build_id']))
if scenario=='startup-high':
    advance_owner()
startup_refused=False
try:
    ns['_open_label_data_manager'](work/'business')
except generation.DataGenerationError:
    startup_refused=True
if scenario in ('late-high','action-late-high','tls-late-high'):
    advance_owner()
direct_refused=False
try:
    generation.check_user_data_open(repo,(setup['external'],))
except generation.DataGenerationError:
    direct_refused=True
events={'locator_reads':0,'secret_reads':0,'decrypt_calls':0}
before_pin=setup['snapshot']()
original_open=Path.open
original_decrypt=setup['runtime']._dpapi_unprotect_current_user
def observed(path,mode='r',*args,**kwargs):
    resolved=Path(path).resolve()
    if 'r' in mode and resolved==setup['credential'].resolve():
        events['locator_reads']+=1
    if 'r' in mode and resolved==setup['target'].resolve():
        events['secret_reads']+=1
    return original_open(path,mode,*args,**kwargs)
def decrypt(value):
    events['decrypt_calls']+=1
    return original_decrypt(value)
Path.open=observed
setup['runtime']._dpapi_unprotect_current_user=decrypt
error_type=''
try:
    if startup_refused:
        returned=False
    else:
        actor=SimpleNamespace()
        actor._package_pin_client=lambda:ns['_package_pin_client'](actor)
        if scenario in ('tls-late-high','tls-same'):
            ns['_label_match_client_tls_ca_bundle_path']=lambda client:str(ca_path)
        if scenario=='action-late-high':
            actor._admin_pin_closing_intent=object()
            original_state=actor._admin_pin_closing_intent
            message=ns['_run_package_pin_action'](actor,'LABEL.SET_HOLD','isolated-set')
            returned=False
            assert actor._admin_pin_closing_intent is original_state
            assert message and actor._admin_pin_last_message==message
            error_type='DataGenerationError'
        else:
            client=actor._package_pin_client()
            returned=client is not None
except Exception as exc:
    returned=False
    error_type=type(exc).__name__
finally:
    Path.open=original_open
    setup['runtime']._dpapi_unprotect_current_user=original_decrypt
result={'state_unchanged':setup['snapshot']()==before_pin,'scenario':scenario,'direct_owner_G_refused':direct_refused,'startup_refused':startup_refused,'pin_client_returned':returned,
        'events':events,'error_type':error_type,'GUI_open_line':open_node.lineno,
        'GUI_pin_line':pin_node.lineno,'native_calls':setup['events']['native_calls'],
        'network_calls':setup['events']['network_calls'],
        'scope':'Exact two GUI AST functions, actual path resolver/G/credential reader; DataManager/SID/decrypt/TLS stand-ins'}
output.write_text(json.dumps(result,indent=2)+'\n',encoding='utf-8')
print(json.dumps({k:result[k] for k in ('scenario','startup_refused','pin_client_returned','events','error_type','native_calls','network_calls')}))

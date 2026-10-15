"""Guarded native child for parent lifecycle admission; no GUI or transport."""
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import auto_update as update
import writer_session_fence as fence
from tests.test_writer_session_fence import _payload


mode, request_path = sys.argv[1:3]
request = json.loads(Path(request_path).read_bytes())
directory = Path(request_path).parent
managed = Path(request['managed'])
if mode == 'publish':
    active = _payload(delegated_sources=request['sources'], token=request['delegation'])
    active.update(request['tuple'])
    active['status'] = request['status']
    active['session_authority_mutex_name'] = fence.session_authority_mutex_name(
        active['session_id'], active['attempt_id'], active['orchestrator_sha256'],
        active['replacement_transaction_id'], active['writer_contract_sha256'])
    control = Path(request['control'])
    control.mkdir(parents=True, exist_ok=True)
    owner = dict(schema=fence._INSTALLER_OWNER_SCHEMA, run_id=request['run'],
        install_root=str(managed/'current'), control_root=str(control),
        owner_process_id=request['owner_pid'], owner_process_started_at_filetime=request['owner_birth'],
        **{name: active[name] for name in fence._INSTALLER_TUPLE_FIELDS})
    owner_path = managed/('.installer-owner-'+active['session_id']+'-'+request['run'].rsplit('-',1)[-1]+'.json')
    (control/fence.ACTIVE_FILENAME).write_text(json.dumps(active),encoding='utf-8')
    owner_path.write_text(json.dumps(owner),encoding='utf-8')
    (directory/'authority-descriptor.json').write_text(json.dumps({'mutex':active['session_authority_mutex_name']}))
    raise SystemExit(0)
if mode == 'mutate':
    path = Path(request['control'])/fence.ACTIVE_FILENAME
    active = json.loads(path.read_bytes())
    fault = request['fault']
    if fault == 'token':
        active['delegation_sha256'] = '0'*64
    elif fault == 'source':
        active['delegated_sources'] = ['canonical_placement']
    elif fault == 'expiry':
        active['delegation_expires_at_utc'] = (datetime.now(timezone.utc)-timedelta(minutes=1)).isoformat()
    elif fault == 'tuple':
        owner_path = managed/('.installer-owner-'+active['session_id']+'-'+request['run'].rsplit('-',1)[-1]+'.json')
        owner = json.loads(owner_path.read_bytes())
        owner['attempt_id'] = '0'*32
        owner_path.write_text(json.dumps(owner))
    path.write_text(json.dumps(active))
    raise SystemExit(0)

# Physical placement, canonical location and ACL observation are fixture
# boundaries. Keep owner PID/birth, tuple/token/source/expiry and mutex native.
fence._installer_owner_root = lambda: managed.absolute()
from tests._machine_acl_native import bind_native_acl_probe
native_acl_paths = bind_native_acl_probe(Path(request['program_data']),Path(request['acl_anchor']))
native_owner_path = fence._assert_protected_owner_path
def scoped_native_path(path, **kwargs):
    if Path(path).parent == Path(request['control']):
        return native_owner_path(path, **kwargs)
fence._assert_protected_owner_path = scoped_native_path
if hasattr(fence, '_installer_program_data_root'):
    fence._installer_program_data_root = lambda: Path(request['program_data'])
isolated_default = fence.canonical_control_root
def scoped_control_root(environ=None):
    bound = getattr(fence._WRITER_LOCAL, 'installer_authority_root', None)
    return bound if bound is not None else isolated_default(environ)
fence.canonical_control_root = scoped_control_root
update._assert_canonical_lifecycle_root = lambda _root: None
update.process_security = lambda _pid: {'elevated':True}
update.version_manifest = lambda root: update.read_json(root/'portable-manifest.json')
update.lifecycle_task_observation = lambda _root: {'task_exists':True}
actual_root = Path(request['control']).absolute()
assert fence.canonical_control_root() != actual_root
observed = []
def verify(*_a, **_k):
    observed.append(fence.canonical_control_root() == actual_root)
    return 'PASS'
update._run_fixed_helper = verify
import failure_redaction
original_emit = failure_redaction.emit_support_failure
def failed(error, **kwargs):
    (directory/'authority-failure.json').write_text(json.dumps({'code':getattr(error,'code',type(error).__name__),'native_acl_mutable':'user mutable' in str(error),'program_data_seen':str(Path(request['program_data'])).lower() in native_acl_paths}))
    return original_emit(error, **kwargs)
failure_redaction.emit_support_failure = failed
arguments = sys.argv[3:]
assert arguments.pop(0) == '--label-match-lifecycle-context'
rc = update.lifecycle_context_main(arguments)
with (directory/'readback-results.jsonl').open('a') as output:
    output.write(json.dumps({'rc':rc,'verify_root_bound':bool(observed) and all(observed),
        'default_root_restored':fence.canonical_control_root()!=actual_root})+'\n')
raise SystemExit(rc)

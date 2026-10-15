"""Isolated native installer child; only stock/OS observation uses doubles."""
from pathlib import Path
import json
import os
import runpy
import socket
import subprocess
import sys
import time
import tkinter
import tkinter.messagebox

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
machine = Path(os.environ['LM_PLACEMENT_MACHINE'])
def blocked(*args, **kwargs):
    raise RuntimeError('isolated lifecycle child forbids UI and network')
tkinter.Tk.__init__ = blocked
tkinter.messagebox._show = blocked
socket.socket.connect = blocked
socket.create_connection = blocked
allowed_roots = [machine.parent]
for name in ('TEMP', 'LOCALAPPDATA', 'APPDATA', 'PROGRAMDATA', 'LABEL_MATCH_SAVE_DIR'):
    if os.environ.get(name):
        allowed_roots.append(Path(os.environ[name]).absolute())
def isolated_writes(event, args):
    if event in ('socket.connect', 'os.startfile'):
        blocked()
    paths = ()
    if event == 'open' and args[2] & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND):
        paths = args[:1]
    elif event in ('os.mkdir', 'os.remove', 'os.rmdir', 'os.chmod', 'os.utime'):
        paths = args[:1]
    elif event in ('os.rename', 'os.link', 'os.symlink'):
        paths = args[:2]
    for value in paths:
        if isinstance(value, (str, bytes, os.PathLike)):
            path = Path(os.fsdecode(value)).absolute()
            if not any(path.is_relative_to(root) for root in allowed_roots):
                raise PermissionError('lifecycle child write escapes its fixture')
sys.addaudithook(isolated_writes)
import auto_update as update  # noqa: E402
import writer_session_fence as fence  # noqa: E402
import failure_redaction  # noqa: E402

started = time.monotonic()
if sys.argv[1:2] == ["--foreign"]:
    if len(sys.argv) > 2:
        os.environ[fence.CONTROL_ROOT_OVERRIDE_ENV] = sys.argv[2]
    for name in (fence.DELEGATION_TOKEN_ENV, fence.DELEGATION_SESSION_ENV,
                 fence.DELEGATION_ATTEMPT_ENV, fence.DELEGATION_TRANSACTION_ENV):
        os.environ.pop(name, None)
    try:
        with fence.writer_admission('canonical_placement'):
            (machine / 'foreign-mutation').write_text('forbidden')
    except fence.WriterFencedError as error:
        print(json.dumps({'status': 'DENIED', 'code': error.code}))
        raise SystemExit(4)
    raise SystemExit(0)

original_emit = failure_redaction.emit_support_failure
def observed_failure(error, **kwargs):
    (machine / 'child-failure.json').write_text(json.dumps({
        'code': getattr(error, 'code', type(error).__name__),
        'seconds': round(time.monotonic() - started, 2),
    }))
    return original_emit(error, **kwargs)
failure_redaction.emit_support_failure = observed_failure

native_path = Path
def isolated_path(*args):
    if len(args) == 1 and str(args[0]).replace('\\', '/').casefold() == 'c:/kmtech/apps/label_match':
        return machine
    return native_path(*args)
update.Path = isolated_path
update.process_security = lambda _pid: {'elevated': True}
update.version_manifest = lambda root: update.read_json(root / 'portable-manifest.json')
update.lifecycle_task_observation = lambda root: {'task_exists': (root / 'current').exists()}

original_run = subprocess.run
def stock_verify(command, **kwargs):
    if '-Mode' not in command or command[command.index('-Mode') + 1] != 'Verify':
        raise AssertionError('unexpected native stock boundary')
    if os.environ.get('LM_PLACEMENT_VERIFY_FAIL') == '1':
        return subprocess.CompletedProcess(command, 1, '', 'isolated stock verification failed')
    return subprocess.CompletedProcess(command, 0, 'PASS', '')
subprocess.run = stock_verify
machine_expected = os.environ.get("KMTECH_TEST_PLACEMENT_MACHINE") == "1"
machine_arguments = "--machine-installer-authority" in sys.argv
if machine_expected and not machine_arguments:
    raise AssertionError("machine authority arguments were not forwarded")
diagnostic_control = fence.canonical_control_root()
if machine_arguments:
    diagnostic_control = Path(sys.argv[sys.argv.index("--installer-authority-root") + 1])
    assert diagnostic_control != fence.canonical_control_root()
    # Native privileged ACL observation and canonical location are the host
    # facets; native owner PID/birth, mutex, tuple and the actual CLI stay real.
    fence._installer_owner_root = lambda: machine.absolute()
    fence._assert_protected_owner_path = lambda _path, **kwargs: None
active = fence.active_fence(diagnostic_control)
foreign_environment = dict(os.environ)
foreign_environment[fence.CONTROL_ROOT_OVERRIDE_ENV] = str(diagnostic_control)
foreign = original_run([sys.executable, '-I', '-B', str(Path(__file__).resolve()), '--foreign'],
                       capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW, env=foreign_environment)
(machine / 'child-grant.json').write_text(json.dumps({
    'sources': active['delegated_sources'],
    'fresh': active['delegation_sha256'] != os.environ['LM_PLACEMENT_PARENT_GRANT_SHA'],
    'foreign_exit': foreign.returncode,
    'foreign_code': json.loads(foreign.stdout)['code'],
}))
# Decorated fixed_helper, admission, session authority, update mutex and durable
# transition/pointer writes all retain their real implementations.
runpy.run_path(str(ROOT / 'portable/main.py'), run_name='__main__')

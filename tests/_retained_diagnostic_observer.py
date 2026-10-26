"""Read-only ANSWER-45 evidence; no admission, wait, or process selection changes."""
import ast
import ctypes
from ctypes import wintypes
import functools
import hashlib
import inspect
import json
import os
from pathlib import Path
import sys
import threading
import time

from tests._first_setup_lock_observer import Timeline as _Timeline


CONTROL = 'retained-diagnostic-control.json'
_SERIAL = threading.Lock()


class Timeline(_Timeline):
    """Keep diagnostic records separate from the existing concurrent lock log."""
    def __init__(self, root, role):
        super().__init__(root, role)
        self.path = Path(root) / 'retained-diagnostics' / (str(os.getpid()) + '.jsonl')
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.serial = _SERIAL


def read(path):
    path = Path(path)
    if path.stat().st_size > 32768:
        raise ValueError('oversized diagnostic input')
    return json.loads(path.read_bytes())


def emit_error(root, section, stage, error):
    """Record both failures without copying exception messages or local values."""
    chain, seen = [], set()
    current = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        frames = []
        tb = current.__traceback__
        while tb is not None:
            frames.append(dict(file=Path(tb.tb_frame.f_code.co_filename).name,
                               function=tb.tb_frame.f_code.co_name, line=tb.tb_lineno))
            tb = tb.tb_next
        code = getattr(current, 'code', None)
        chain.append(dict(type=type(current).__name__,
                          code=code if isinstance(code, str) and len(code) < 100 else None,
                          frames=frames))
        current = current.__cause__ or current.__context__
    Timeline(root, 'retained_test').event(section + '_exception', stage=stage, chain=chain)


def native_process(pid):
    """Native birth and parent only; never read a command line or environment."""
    from auto_update import native_process_identity
    result = dict(pid=pid, **native_process_identity(pid))
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    kernel.OpenProcess.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    handle = kernel.OpenProcess(0x1000, False, pid)
    if not handle:
        return result
    try:
        # PROCESS_BASIC_INFORMATION: ExitStatus, Peb, Affinity, Priority, PID, PPID.
        fields = (ctypes.c_void_p * 6)()
        query = ctypes.WinDLL('ntdll').NtQueryInformationProcess
        query.argtypes = [wintypes.HANDLE, wintypes.ULONG, ctypes.c_void_p,
                          wintypes.ULONG, ctypes.POINTER(wintypes.ULONG)]
        query.restype = ctypes.c_long
        size = wintypes.ULONG()
        status = query(handle, 0, fields, ctypes.sizeof(fields), ctypes.byref(size))
        if status >= 0:
            result['parent_pid'] = int(fields[5] or 0)
        else:
            result['parent_query_status'] = status
    finally:
        kernel.CloseHandle(handle)
    return result


def own_job_pids():
    """NULL selects the caller's immediate Job, including on a nested Job host."""
    query = ctypes.WinDLL('kernel32', use_last_error=True).QueryInformationJobObject
    query.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p,
                      wintypes.DWORD, ctypes.POINTER(wintypes.DWORD)]
    query.restype = wintypes.BOOL
    capacity = 256
    for _ in range(3):
        class ProcessList(ctypes.Structure):
            _fields_ = [('assigned', wintypes.DWORD), ('count', wintypes.DWORD),
                        ('pids', ctypes.c_size_t * capacity)]
        value, returned = ProcessList(), wintypes.DWORD()
        if query(None, 3, ctypes.byref(value), ctypes.sizeof(value), ctypes.byref(returned)):
            return {int(pid) for pid in value.pids[:value.count]}
        error = ctypes.get_last_error()
        if error != 234:
            raise ctypes.WinError(error)
        capacity = max(capacity * 2, int(value.assigned) + 16)
    raise RuntimeError('own Job process-list snapshot changed repeatedly')


def mutex_owner(name, root):
    """Open for MUTANT_QUERY_STATE only; never acquire/release the observed mutex."""
    facts = dict(mutex_scope_sha256=hashlib.sha256(name.encode()).hexdigest(),
                 query_only=True, owner_pid=None, owner_native_birth=None,
                 owner_in_own_job=None, owner_is_SYSTEM_applying_Y=None)
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.OpenMutexW.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.LPCWSTR]
    kernel.OpenMutexW.restype = wintypes.HANDLE
    kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    handle = kernel.OpenMutexW(1, False, name)
    if not handle:
        facts['open_error'] = ctypes.get_last_error()
        return facts
    try:
        owner = (ctypes.c_void_p * 2)()
        query = ctypes.WinDLL('ntdll').NtQueryMutant
        query.argtypes = [wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p,
                          wintypes.ULONG, ctypes.POINTER(wintypes.ULONG)]
        query.restype = ctypes.c_long
        size = wintypes.ULONG()
        status = query(handle, 1, owner, ctypes.sizeof(owner), ctypes.byref(size))
        facts['query_status'] = status
        if status < 0:
            return facts
        pid = int(owner[0] or 0)
        facts.update(owner_pid=pid, owner_thread_id=int(owner[1] or 0))
        if not pid:
            facts.update(owner_in_own_job=False, owner_is_SYSTEM_applying_Y=False)
            return facts
        identity = native_process(pid)
        facts.update(owner_native_birth=identity.get('creation_identity'),
                     owner_native_status=identity.get('status'))
        members = own_job_pids()
        facts['observer_in_own_job'] = os.getpid() in members
        facts['owner_in_own_job'] = pid in members if os.getpid() in members else None
        # A native helper can own admission on behalf of the recorded apply PID.
        # Keep the ancestry proof distinct from direct Python-owner attribution.
        chain = []
        while identity['pid'] in members and len(chain) < 8:
            chain.append(identity)
            path = Path(root) / 'retained-apply-observations' / (str(identity['pid']) + '.json')
            if path.exists():
                context = read(path)
                matched = (identity.get('status') == 'LIVE' and
                           str(identity.get('creation_identity')) == str(context.get('native_birth')))
                facts.update(apply_context_birth_matches=matched,
                             apply_context_pid=identity['pid'],
                             apply_context_is_direct_owner=identity['pid'] == pid,
                             apply_context_candidate_attribution=context.get('candidate_attribution'),
                             apply_context_prepared_is_Y=context.get('prepared_is_Y'),
                             SYSTEM_attribution_source='existing_virtual_SYSTEM_token_adapter',
                             owner_is_SYSTEM_applying_Y=bool(matched and context.get('active')
                                 and context.get('SYSTEM_apply') and context.get('prepared_is_Y')))
                break
            parent = identity.get('parent_pid')
            if not parent or any(row['pid'] == parent for row in chain):
                break
            previous = identity
            identity = native_process(parent)
            if (identity.get('status') != 'LIVE' or previous.get('status') != 'LIVE'
                    or int(identity['creation_identity']) > int(previous['creation_identity'])):
                break
        facts['owner_ancestry'] = chain
        facts['apply_attribution_status'] = ('ATTRIBUTED' if 'apply_context_pid' in facts else 'UNRESOLVED')
        if facts['owner_is_SYSTEM_applying_Y'] is None:
            facts['owner_is_SYSTEM_applying_Y'] = False
        return facts
    finally:
        kernel.CloseHandle(handle)


def install_mutex_observer(root, fence):
    if not (Path(root) / CONTROL).exists():
        return lambda: None
    original = fence._acquire_named_mutex
    timeline = Timeline(root, 'retained_diagnostic')

    @functools.wraps(original)
    def observed(name, timeout_seconds):
        result = original(name, timeout_seconds)
        if result is None:
            try:
                timeline.event('writer_timeout_owner', **mutex_owner(name, root))
            except Exception as error:
                timeline.event('writer_timeout_owner_error', error_type=type(error).__name__)
        return result
    fence._acquire_named_mutex = observed

    def restore():
        fence._acquire_named_mutex = original
    return restore


def install(root, updates, fence, bootstrap_code):
    if not (Path(root) / CONTROL).exists():
        return
    install_mutex_observer(root, fence)
    timeline = Timeline(root, 'retained_diagnostic')
    timeline.event('retained_diagnostic_enter',
        native_birth=updates.native_process_identity(os.getpid()).get('creation_identity'),
        virtual_SYSTEM_context=updates.current_process_sid() == 'S-1-5-18',
        apply_argument_present='--apply-prepared' in sys.argv)
    original_apply = updates.apply_prepared

    @functools.wraps(original_apply)
    def apply(*args, **kwargs):
        from tests._first_setup_host import _write_observation
        context = dict(pid=os.getpid(), native_birth=updates.native_process_identity(os.getpid()).get('creation_identity'),
                       SYSTEM_apply=('--apply-prepared' in sys.argv and
                                     updates.current_process_sid() == 'S-1-5-18'), active=True,
                       candidate_attribution='prepared_marker_at_apply_entry_not_verification')
        try:
            control = read(Path(root) / CONTROL)
            prepared = read(Path(root) / 'stage/prepared.json')
            context['prepared_is_Y'] = prepared.get('source_commit') == control['following']
        except Exception as error:
            context['observation_error_type'] = type(error).__name__
        target = Path(root) / 'retained-apply-observations' / (str(os.getpid()) + '.json')
        def record(value):
            try:
                _write_observation(target, value)
            except Exception as error:
                timeline.event('apply_context_observation_error', error_type=type(error).__name__)
        record(context)
        try:
            return original_apply(*args, **kwargs)
        finally:
            record(dict(context, active=False))
    updates.apply_prepared = apply

    for name in ('request_apply_task', 'observe_trial_ready', 'wait_current_relay'):
        original = getattr(updates, name)

        @functools.wraps(original)
        def call(*args, _name=name, _original=original, **kwargs):
            deadline = kwargs.get('deadline')
            timeline.event('confirmation_product_call', function=_name,
                           deadline=float(deadline) if isinstance(deadline, (int, float)) else None,
                           configured_READY_seconds=updates.READY_WINDOW_SECONDS)
            return _original(*args, **kwargs)
        setattr(updates, name, call)

    # Trace just the existing bootstrap frame, composing existing receipt/guard traces.
    source, first = inspect.getsourcelines(bootstrap_code)
    tree = ast.parse(''.join(source))
    skip_lines = {first + statement.lineno - 1 for node in ast.walk(tree)
                  if isinstance(node, ast.If) and isinstance(node.test, ast.UnaryOp)
                  and isinstance(node.test.op, ast.Not) and isinstance(node.test.operand, ast.Name)
                  and node.test.operand.id == 'prepared' for statement in node.body}
    original_bootstrap = updates._bootstrap

    @functools.wraps(original_bootstrap)
    def bootstrap(*args, **kwargs):
        previous = sys.gettrace()
        last = None

        def trace(frame, event, argument):
            delegated = previous(frame, event, argument) if previous else None
            if frame.f_code is not bootstrap_code:
                return delegated

            def local(frame, event, argument):
                nonlocal delegated, last
                if delegated:
                    delegated = delegated(frame, event, argument)
                if event in {'line', 'return'}:
                    values = frame.f_locals
                    selected = {key: values.get(key) for key in
                        ('requested', 'requested_at', 'progress_deadline', 'saw_trial',
                         'apply_started', 'waiting_for_decision', 'started', 'deadline', 'stage_deadline')}
                    relay_deadline = values.get('relay_deadline')
                    selected['relay_deadline'] = float(relay_deadline) if relay_deadline is not None else None
                    state = values.get('state') or {}
                    selected.update(phase=state.get('phase'), retained=state.get('retained_candidate') is True,
                                    prepared=values.get('prepared'), claim_status=values.get('claim_status'))
                    if selected != last or frame.f_lineno in skip_lines or event == 'return':
                        timeline.event('confirmation_bootstrap_observation', file='auto_update.py',
                            line=frame.f_lineno, trace_event=event,
                            retained_skip_no_prepared=frame.f_lineno in skip_lines,
                            configured_STARTUP_seconds=updates.STARTUP_WAIT_SECONDS, **selected)
                        last = selected
                return local
            return local
        try:
            sys.settrace(trace)
            return original_bootstrap(*args, **kwargs)
        finally:
            sys.settrace(previous)
    updates._bootstrap = bootstrap

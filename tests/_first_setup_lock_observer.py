"""Read-only diagnostics for the lane-authorized eleventh process chain."""
from contextlib import contextmanager
import functools
import hashlib
import inspect
import json
import logging
import os
from pathlib import Path
import sys
import threading
import time


def process_role(arguments, entry=''):
    if '--apply-prepared' in arguments:
        return 'SYSTEM'
    if '--bootstrap' in arguments:
        return 'bootstrap'
    if '--label-match-user-relay' in arguments:
        return 'relay'
    if '--label-match-direct-sync-relay' in arguments:
        return 'direct_sync'
    if Path(entry).name in {'main.py', 'Label_Match.py'}:
        return 'GUI'
    return 'ordinary_setup'


def frames(frame=None):
    frame = frame or sys._getframe(1)
    result = []
    while frame is not None and len(result) < 12:
        name = Path(frame.f_code.co_filename).name
        if name in {'auto_update.py', 'writer_session_fence.py', 'user_relay.py',
                    'label_match_relay_entry.py', 'current_user_onboarding.py'}:
            result.append({'file': name, 'function': frame.f_code.co_name,
                           'line': frame.f_lineno})
        frame = frame.f_back
    return result


class Timeline:
    def __init__(self, root, role):
        self.root = Path(root)
        self.role = role
        self.path = self.root / 'lock-timeline' / (str(os.getpid()) + '.jsonl')
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.serial = threading.Lock()
        self.bound_logs = set()

    def event(self, kind, **fields):
        try:
            value = dict(event=kind, monotonic=time.monotonic(), time_ns=time.time_ns(), pid=os.getpid(),
                         thread=threading.get_ident(), role=self.role, **fields)
            with self.serial, self.path.open('a', encoding='utf-8') as stream:
                stream.write(json.dumps(value, sort_keys=True) + '\n')
        except Exception:
            pass

    def bind_logs(self):
        try:
            loggers = [logging.getLogger(), *[v for v in logging.Logger.manager.loggerDict.values()
                                            if isinstance(v, logging.Logger)]]
            for logger in loggers:
                for handler in logger.handlers:
                    if isinstance(handler, logging.FileHandler):
                        path = Path(handler.baseFilename).resolve()
                        if path.is_relative_to(self.root.resolve()) and str(path) not in self.bound_logs:
                            self.bound_logs.add(str(path))
                            self.event('log_bound_in_process', path=str(path))
        except Exception:
            pass


def observed_update_lock(original, native_sid, timeline):
    @contextmanager
    def lock(control, _sid, **options):
        kind = 'trial' if options.get('trial', False) else 'root'
        began = time.monotonic()
        call = frames()
        timeline.event('update_request', mutex=kind, frames=call,
                       sid_source='native_current_process_sid',
                       operator_sid_adapter=True)
        acquired = False
        try:
            with original(control, native_sid, **options):
                acquired = True
                timeline.event('update_acquired', mutex=kind, frames=call,
                               wait_seconds=time.monotonic()-began)
                yield
        except BaseException as error:
            timeline.event('update_error', mutex=kind, acquired=acquired,
                           error_type=type(error).__name__,
                           busy=isinstance(error, RuntimeError)
                                and str(error) == 'update mutex ownership is busy',
                           elapsed_seconds=time.monotonic()-began, frames=frames())
            raise
        finally:
            if acquired:
                # The original context has completed its native ReleaseMutex.
                timeline.event('update_released', mutex=kind, frames=call,
                               elapsed_seconds=time.monotonic()-began)
    return lock


def install(root, updates, fence, native_lock, native_sid, app):
    role = process_role(sys.argv, sys.argv[0])
    timeline = Timeline(root, role)
    # Native PID/birth is emitted by the parent holding the original Popen handle.
    timeline.event('child_observer_enter', app=str(Path(app).resolve()),
                   identity_sources=dict(pid_birth='native_Popen_handle',
                     role='host_launch_arguments', security='existing_virtual_token_adapter',
                     process_rows='existing_CIM_metadata_adapter'))
    updates.update_lock = observed_update_lock(native_lock, native_sid, timeline)
    original_acquire = fence._acquire_named_mutex
    def acquire(name, timeout_seconds):
        tag = hashlib.sha256(name.encode()).hexdigest()
        began = time.monotonic()
        timeline.event('writer_request', mutex_sha256=tag, timeout_seconds=timeout_seconds,
                       frames=frames())
        try:
            lease = original_acquire(name, timeout_seconds)
        except BaseException as error:
            timeline.event('writer_error', mutex_sha256=tag, error_type=type(error).__name__,
                           elapsed_seconds=time.monotonic()-began)
            raise
        timeline.event('writer_acquired' if lease is not None else 'writer_timeout',
                       mutex_sha256=tag, elapsed_seconds=time.monotonic()-began,
                       abandoned=bool(lease and lease.abandoned), frames=frames())
        if lease is not None:
            lease._host_timeline_mutex = tag
        return lease
    fence._acquire_named_mutex = acquire
    for method in ('release', 'suspend', 'resume'):
        original = getattr(fence._NamedMutexLease, method)
        def observed(lease, *args, _method=method, _original=original, **kwargs):
            tag = getattr(lease, '_host_timeline_mutex', None)
            tracked = tag is not None and bool(lease.handle)
            if tracked:
                timeline.event('writer_'+_method+'_request', mutex_sha256=tag,
                               owned=bool(lease.owned), frames=frames())
            try:
                result = _original(lease, *args, **kwargs)
            except BaseException as error:
                if tracked:
                    timeline.event('writer_'+_method+'_error', mutex_sha256=tag,
                                   error_type=type(error).__name__)
                raise
            if tracked:
                timeline.event('writer_'+_method+'_return', mutex_sha256=tag,
                               owned=bool(lease.owned), frames=frames())
            return result
        setattr(fence._NamedMutexLease, method, observed)
    # The four labels identify the lane's retained initial/terminal exceptions.
    # Observe only these calls; a global Python profile slows every inventory walk.
    def product_observer(original, label):
        @functools.wraps(original)
        def observed(*args, **kwargs):
            call_frames = [label, *frames()][:12]
            began = time.monotonic()
            timeline.bind_logs()
            timeline.event('product_call', frames=call_frames)
            result = None
            error_type = None
            try:
                result = original(*args, **kwargs)
                return result
            except BaseException as error:
                error_type = type(error).__name__
                raise
            finally:
                timeline.bind_logs()
                timeline.event('product_return', frames=call_frames,
                    elapsed_seconds=time.monotonic()-began,
                    return_type=type(result).__name__, error_type=error_type)
        return observed
    for name in ('_launch_bootstrap_version', 'start_current',
                 'recovery_writer_result', 'wait_current_relay'):
        original = getattr(updates, name, None)
        if original is None:
            continue
        source = inspect.unwrap(original).__code__
        label = dict(file=Path(source.co_filename).name, function=name,
                     line=source.co_firstlineno)
        setattr(updates, name, product_observer(original, label))
    return timeline


def observe_launch(config, entry, arguments, process, native_times, fixed_log):
    """Retain this exact Popen handle through exit; never signal or reap it."""
    root = Path(config).parent
    role = process_role(arguments, entry)
    timeline = Timeline(root, role)
    identity = native_times(process)
    timeline.event('native_launch', child_pid=identity['pid'],
                   child_birth=identity['creation_identity'], entry=Path(entry).name,
                   output=str(process.host_test_output), native_alive=identity['alive'],
                   child_cpu_seconds=identity['cpu_seconds'],
                   child_wall_seconds=identity['wall_seconds'])
    def observe_exit():
        next_alive = time.monotonic() + 5
        while True:
            try:
                state = native_times(process)
                if not state['alive']:
                    timeline.event('native_exit', child_pid=identity['pid'],
                        child_birth=identity['creation_identity'],
                        identity_matches=state['pid']==identity['pid'] and
                            state['creation_identity']==identity['creation_identity'],
                        exit_code=state['exit_code'], fixed_child_log=fixed_log(process),
                        child_cpu_seconds=state['cpu_seconds'],
                        child_wall_seconds=state['wall_seconds'])
                    return
                if time.monotonic() >= next_alive:
                    timeline.event('native_alive', child_pid=identity['pid'],
                        child_birth=identity['creation_identity'],
                        identity_matches=state['pid']==identity['pid'] and
                            state['creation_identity']==identity['creation_identity'],
                        child_cpu_seconds=state['cpu_seconds'],
                        child_wall_seconds=state['wall_seconds'])
                    next_alive = time.monotonic() + 5
            except Exception as error:
                timeline.event('native_observer_error', error_type=type(error).__name__)
                return
            time.sleep(0.5)
    threading.Thread(target=observe_exit, daemon=True).start()

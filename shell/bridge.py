"""Pure Python: API intake -> one owner -> independent UI push pump.
Grading uses V152 immutable event-order evidence; intake hook state is diagnostic.
"""
import copy
import queue
import threading
import time
from concurrent.futures import Future, wait as wait_futures
from collections import OrderedDict

from journal import Events, Journal
from pedal import Pedal, read_physical_f12
from sound import Sound


class Bridge:
    """processor (app processing layer, owner thread only) may define state(),
    scan(text, info), command(name, args), admin(name, args). None = synthetic test journal.
    owner='thread' runs our own owner thread; owner='external' means the app calls
    attach_owner(processor) and then pump() from its own thread (e.g. a Tk thread)."""
    def __init__(self, data, profile, pedal=None, src='py', sound=None, physical_reader=None, test_control=False,
                 processor_factory=None, owner='thread'):
        if owner not in ('thread', 'external'): raise ValueError('owner must be thread or external')
        self.profile, self.src, self.data = profile, src, data
        self.events = Events(data)
        self.pedal = pedal or Pedal()
        self._sound = sound
        self._physical_reader = physical_reader or read_physical_f12
        self._test_control = test_control
        self._processor_factory, self._processor, self._external = processor_factory, None, owner == 'external'
        self.owner_thread_id = None
        # Unbounded in-memory queues: put_nowait never fails and equals put (no file/network write).
        self._tasks, self._pushes = queue.Queue(), queue.Queue()
        self._intake_lock = threading.Lock()
        self._notify = None
        self._closed = False
        self._owner_done = False
        self._exit_handler, self._exit_requested = None, False
        self._file_picker = None
        self._file_pick_lock = threading.Lock()
        self._idle = None
        self._publisher_stop = threading.Event()
        self._initialized = Future()
        self._owner = None if self._external else threading.Thread(target=self._run, name='shell-owner', daemon=True)
        self._publisher = threading.Thread(target=self._publish, name='shell-push', daemon=True)
        if self._owner:
            self._owner.start()
            self._initialized.result()
        self._publisher.start()
        self.pedal.set_callback(self._pedal_changed)

    def attach_owner(self, processor=None, idle=None):
        """External owner only: call once on the owner thread before pump().
        idle() runs on the owner thread while it waits inside pick_file (Tk: root.update) so the
        app's own timers keep running; the bridge keeps pumping its queue itself."""
        if not self._external or self._initialized.done(): raise RuntimeError('attach_owner: external owner only, once')
        if idle is not None and not callable(idle): raise TypeError('idle must be callable or None')
        self._idle = idle
        try: self._init_owner(processor)
        except Exception as error:
            self._initialized.set_exception(error)
            raise
        self._initialized.set_result(True)

    def fail_owner(self, error):
        """External owner only: the app could not build its processor (e.g. a startup gate inside
        app construction failed). Ends run_app's start wait with this error; no window is created."""
        if not self._external or self._initialized.done(): raise RuntimeError('fail_owner: external owner only, before attach')
        self._initialized.set_exception(error if isinstance(error, BaseException) else RuntimeError(str(error)))

    def wait_owner(self, timeout=None):
        return self._initialized.result(timeout)

    def pump(self, limit=100):
        """External owner only: run queued work on the owner thread; never blocks.
        Returns False once the bridge is closed (the owner should then stop its loop)."""
        if threading.get_ident() != self.owner_thread_id: raise RuntimeError('pump: not the owner thread')
        for _ in range(limit):
            if self._owner_done: return False
            try: op, value, future = self._tasks.get_nowait()
            except queue.Empty: break
            if not self._dispatch(op, value, future): return False
        self._expire_pending()
        return not self._owner_done

    def set_notifier(self, notify):
        self._notify = notify

    def set_exit_handler(self, handler):
        self._exit_handler = handler

    def set_file_picker(self, picker):
        """Host binds after shown; None detaches before closing. No UI dependency."""
        if picker is not None and not callable(picker): raise TypeError('picker must be callable or None')
        self._file_picker = picker

    def pick_file(self, title, kinds=None, timeout=None) -> str | None:
        """Owner operation; returns when the dialog settles. While the host's dialog is open
        the owner keeps pumping (queued scans/state/push_state, gap timer, idle hook).
        Cancel, window closing or timeout (seconds, default none) -> None. Reserve before
        queueing so a second caller returns busy immediately instead of opening another dialog."""
        if not self._file_pick_lock.acquire(blocking=False):
            self.events.record(self.src, 'pick_file', result='busy')
            return None
        try:
            if self._file_picker is None or self._closed or self._exit_requested:
                self.events.record(self.src, 'pick_file', result='unavailable')
                return None
            try: return self._call('pick_file', (title, copy.deepcopy(kinds), timeout))
            except Exception:
                self.events.record(self.src, 'pick_file', result='unavailable')
                return None
        finally: self._file_pick_lock.release()

    def request_exit(self):
        """Any thread (usually the owner after the app drained its work): close the
        window so run_app returns. Never blocks; the window X question is skipped."""
        self._exit_requested = True
        self.events.record(self.src, 'exit_requested')
        if self._exit_handler:
            threading.Thread(target=self._exit_handler, name='shell-exit', daemon=True).start()

    def ask_closing(self, timeout=3):
        """UI thread, window X: True = let it close. processor.closing() decides on the
        owner thread; no answer within timeout -> close (a stuck owner must not trap the user)."""
        if self._exit_requested or self._closed or self._processor is None or not hasattr(self._processor, 'closing'):
            return True
        future = Future()
        self._tasks.put_nowait(('closing', None, future))
        try: return future.result(timeout) is not False
        except Exception as error:
            self.events.record(self.src, 'closing_timeout', error=type(error).__name__)
            return True

    def push(self, event):
        self._pushes.put_nowait(copy.deepcopy(event))

    def _publish(self):
        while not self._publisher_stop.is_set():
            event = self._pushes.get()
            if event is None:
                return
            if self._notify:
                try:
                    self._notify(event)
                except Exception as error:
                    self.events.record(self.src, 'push_failed', kind=event.get('kind'), reason=type(error).__name__)

    def _pedal_changed(self, pressed, generation):
        self.events.record(self.src, 'pedal', pressed=pressed, generation=generation)
        self.push({'kind': 'pedal', 'pressed': pressed, 'generation': generation})

    def _call(self, op, value=None):
        if threading.get_ident() == self.owner_thread_id:
            return self._handle(op, value)
        future = Future()
        with self._intake_lock:
            if self._closed:
                raise RuntimeError('bridge closed')
            self._tasks.put_nowait((op, value, future))
        return future.result()

    def scan(self, text, meta):
        hook, arrived = self.pedal.snapshot(), time.time()
        captured = copy.deepcopy(meta)
        with self._intake_lock:
            if self._closed:
                raise RuntimeError('bridge closed')
            seq = captured.get('seq')
            if seq is None:
                seq = self._intake_seq + 1
                captured['seq'] = seq
            if type(seq) is int:
                self._intake_seq = max(self._intake_seq, seq)
            future = Future()
            self._tasks.put_nowait(('scan', (text, captured, arrived, hook), future))
        return future.result()

    def submit_scan(self, text, meta):
        return self.scan(text, meta)  # Existing kit adapter compatibility.

    def state(self): return self._call('state')
    def get_state(self): return self.state()
    def command(self, name, args=None): return self._call('command', (name, copy.deepcopy(args)))
    def admin(self, name, args=None): return self._call('admin', (name, copy.deepcopy(args)))
    def push_state(self, **extra):
        # Any thread: the snapshot itself is taken on the owner thread.
        self._tasks.put_nowait(('push_state', extra, Future()))
    def ready(self, meta): return self._call('ready', copy.deepcopy(meta))
    def log_batch(self, batch): return self._call('log_batch', copy.deepcopy(batch))
    def f12_physical(self): return self._call('f12_physical')
    def delay_next_scan(self, ms): return self._call('delay', ms)
    def mark_recovery(self):
        # Failure/reload callbacks on the UI thread must never wait for owner I/O.
        self._tasks.put_nowait(('recovery', None, Future()))

    def _snapshot(self):
        pressed, generation = self.pedal.snapshot()
        # With an app processor, counts/lists come only from the processor (APP-SCREENS invariant 6).
        base = self.journal.state() if self._processor is None else {**self._processor_state(), 'last_seq': self._last_seq}
        return {**base, 'last_request_seq': self._last_seq, 'profile': self.profile,
                'pedal': {'pressed': pressed, 'generation': generation}, 'journal_uncertain': self._failed,
                'events_dropped': self.events.dropped,
                'recovery': self._view_count > 0 or self._recovered_startup or self._recovery_pending,
                'owner_thread_id': self.owner_thread_id,
                'cache_sizes': {'journal_recent': len(self.journal.rows), 'scan_results': len(self._completed)}}

    def _processor_state(self):
        if not hasattr(self._processor, 'state'): return {}
        try: return dict(self._processor.state())
        except Exception as error:
            self.events.record(self.src, 'processor_error', op='state', error=type(error).__name__)
            return {'processor_error': type(error).__name__}

    @staticmethod
    def _journal_reply(row):
        return {'ok': True, 'accepted': True, 'seq': row['seq'], 'kind': 'scan_result',
                'verdict': row.get('app_verdict', row['verdict']),
                'sound': row.get('sound', 'ok' if row['verdict'] == 'ok' else 'error'),
                'message': '', 'text': row['text']}

    def _remember_result(self, seq, reply):
        self._completed[seq] = reply
        self._completed.move_to_end(seq)
        while len(self._completed) > 128: self._completed.popitem(last=False)

    def _old_result(self, seq):
        old = self._completed.get(seq)
        if old is None:
            row = self.journal.find(seq)
            old = self._journal_reply(row) if row else self.events.find_receipt(seq, self._receipt_offset)
            if old is not None: self._remember_result(seq, old)
        return old

    def _handle(self, op, value):
        if op == 'pick_file':
            picker, path, result = self._file_picker, None, 'unavailable'
            if picker is not None and not self._closed and not self._exit_requested:
                title, kinds, timeout = value
                try:
                    path = picker(title, kinds)
                    # The host returns a Future (dialog on the UI thread); a plain value = synchronous fake.
                    if isinstance(path, Future): path = self._wait_picked(path, picker, timeout)
                except TimeoutError: path, result = None, 'timeout'
                except Exception: pass  # Never log exception text: it may contain a path/title.
                else:
                    if self._closed or self._exit_requested or self._file_picker is not picker:
                        path = None
                    elif isinstance(path, str) and path: result = 'chosen'
                    elif path is None or path == '': result = 'cancelled'
            self.events.record(self.src, 'pick_file', result=result)
            return path if result == 'chosen' else None
        if op == 'state': return self._snapshot()
        if op == 'ready':
            self.events.record(self.src, 'ready', js=value)
            self._view_count += 1
            self._recovery_pending = False
            return self._snapshot()
        if op == 'recovery': self._recovery_pending = True; return True
        if op == 'delay':
            if not self._test_control: raise ValueError('test control not enabled')
            if type(value) is not int or not 0 <= value <= 10000: raise ValueError('ms must be 0..10000')
            self._delay_ms = value
            return {'ms': value}
        if op == 'f12_physical':
            started = time.perf_counter()
            try: result = self._physical_reader()
            except Exception: result = None
            if result is not True and result is not False: result = None
            reply = {'result': result, 'ms': (time.perf_counter() - started) * 1000}
            self.events.record(self.src, 'f12_physical', **reply)
            return reply
        if op == 'log_batch':
            for row in value:
                fields = dict(row); ev = fields.pop('ev'); fields.pop('src', None)
                self.events.record('js', ev, **fields)
            return {'logged': len(value)}
        if op in ('command', 'admin'):
            name, args = value
            if self._processor is None or not hasattr(self._processor, op): raise ValueError(f'{op} not available')
            if not isinstance(name, str) or not name: raise ValueError(f'{op} name required')
            self.events.record(self.src, op, name=name)  # never the args: admin carries a PIN
            try: result = getattr(self._processor, op)(name, args)
            except Exception as error:
                self.events.record(self.src, 'processor_error', op=op, name=name, error=type(error).__name__)
                return {'ok': False, 'error': type(error).__name__, 'state': self._snapshot()}
            # The frame (frame.js bridge, 57f87c1a) unwraps a result dict that carries 'ok'.
            return {'ok': True, 'result': result, 'state': self._snapshot()}
        if op == 'closing':
            try: allowed = self._processor.closing() is not False
            except Exception as error:
                self.events.record(self.src, 'processor_error', op='closing', error=type(error).__name__)
                allowed = True
            self.events.record(self.src, 'closing', allowed=allowed)
            return allowed
        if op == 'push_state':
            self.push({**(value or {}), 'kind': 'state', 'state': self._snapshot()})
            return True
        raise ValueError('unknown owner operation')

    def _wait_picked(self, pending, picker, timeout):
        """Owner thread: keep the owner pump going until the UI-thread dialog settles.
        Closing/exit/detached picker -> None; past the deadline -> TimeoutError (a late
        selection is dropped)."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while not pending.done():
            if self._owner_done or self._closed or self._exit_requested or self._file_picker is not picker: return None
            if deadline is not None and time.monotonic() >= deadline: raise TimeoutError
            self.pump()
            if self._idle is not None and not self._owner_done:
                try: self._idle()
                except Exception as error:
                    self._idle = None  # A destroyed Tk root must not loop errors; the bridge still pumps.
                    self.events.record(self.src, 'idle_error', error=type(error).__name__)
            wait_futures((pending,), timeout=0.01)
        return pending.result()

    def _play(self, seq, sound):
        try:
            self._sound.play(sound)
            self.events.record(self.src, 'sound', seq=seq, kind=sound, ok=True)
        except Exception as error:
            self.events.record(self.src, 'sound', seq=seq, kind=sound, ok=False, error=type(error).__name__)

    def _processor_scan(self, seq, text, meta, evidence):
        info = {'seq': seq, 'pedal_pressed': evidence['pedal_pressed'], 'evidence': copy.deepcopy(evidence), 'profile': self.profile}
        try: out = dict(self._processor.scan(text, info) or {})
        except Exception as error:
            self.events.record(self.src, 'processor_error', op='scan', seq=seq, error=type(error).__name__)
            self._play(seq, 'error')
            return {'ok': False, 'accepted': False, 'seq': seq, 'kind': 'processing_error', 'verdict': None,
                    'sound': 'error', 'message': '처리 확인 필요 — 다시 확인하세요'}
        accepted = out.get('accepted') is True
        sound = out.get('sound', 'ok' if accepted else 'error')
        reply = {'ok': accepted, 'accepted': accepted, 'seq': seq, 'kind': out.get('kind', 'scan_result'),
                 'verdict': out.get('verdict'), 'sound': sound, 'message': out.get('message', ''), 'result': out.get('result')}
        if accepted:
            # Shell receipt journal = seq idempotency after screen reload; the app's own store stays the record.
            row = {'seq': seq, 'text': text, 'hex': text.encode('utf-8').hex(), 'len': len(text),
                   'verdict': 'ng' if out.get('verdict') == 'ng' else 'ok', 'app_verdict': out.get('verdict'),
                   'sound': sound, 'pedal_pressed': evidence['pedal_pressed'], 'evidence': evidence,
                   't_enter_js': meta.get('t_enter', meta.get('at')), 't_first_js': meta.get('t_first'),
                   'paths': meta.get('paths', {}), 'n_keys': meta.get('n_keys', 0), 'owner_thread_id': self.owner_thread_id}
            try: self.journal.append(row)
            except Exception:
                # Receipt cache only (app store = record): keep scanning; idempotency stays in _completed.
                self.events.record(self.src, 'journal_write_failed', seq=seq)
            else: self.events.record(self.src, 'scan', **row)
        if sound: self._play(seq, sound)
        return reply

    def _result(self, seq, kind, verdict=None, accepted=False, message=''):
        sound = 'ok' if accepted and verdict == 'ok' else 'error'
        try:
            self._sound.play(sound)
            self.events.record(self.src, 'sound', seq=seq, kind=sound, ok=True)
        except Exception as error:
            self.events.record(self.src, 'sound', seq=seq, kind=sound, ok=False, error=type(error).__name__)
        return {'ok': accepted, 'accepted': accepted, 'seq': seq, 'kind': kind,
                'verdict': verdict, 'sound': sound, 'message': message}

    def _process_scan(self, value):
        text, meta, arrived, hook = value
        seq = meta['seq']
        if self._delay_ms:
            delay, self._delay_ms = self._delay_ms, 0
            self.events.record(self.src, 'scan_delay', seq=seq, ms=delay)
            time.sleep(delay / 1000)
        evidence = meta.get('evidence')
        if not isinstance(text, str) or not text or not isinstance(evidence, dict) or type(evidence.get('pedal_pressed')) is not bool:
            return self._result(seq, 'invalid_evidence', message='다시 스캔하세요')
        if meta.get('recovery_first_scan') and meta.get('recovered'):
            first, attach = meta.get('t_first'), meta.get('t_attach')
            if not isinstance(first, (int, float)) or not isinstance(attach, (int, float)):
                return self._result(seq, 'invalid_evidence', message='다시 스캔하세요')
            if 0 <= first - attach < 300:
                self.events.record(self.src, 'recovery_guard', seq=seq, t_attach=attach, t_first=first, len=len(text))
                return self._result(seq, 'recovery_guard', message='화면이 다시 열렸습니다 — 다시 스캔하세요')
        if self._failed:
            return self._result(seq, 'journal_uncertain', message='기록 확인 필요 — 다시 확인하세요')
        if self._processor is not None:
            return self._processor_scan(seq, text, meta, evidence)
        verdict = 'ng' if evidence['pedal_pressed'] else 'ok'
        row = {'seq': seq, 'text': text, 'hex': text.encode('utf-8').hex(), 'len': len(text),
               'verdict': verdict, 'pedal_pressed': evidence['pedal_pressed'],
               'pedal_generation': evidence.get('press_generation'), 'evidence': evidence,
               'hook_pressed_at_arrival': bool(hook[0]), 'hook_generation_at_arrival': hook[1],
               't_arrive': arrived, 't_enter_js': meta.get('t_enter', meta.get('at')),
               't_first_js': meta.get('t_first'), 'paths': meta.get('paths', {}), 'n_keys': meta.get('n_keys', 0),
               'owner_thread_id': self.owner_thread_id}
        try: self.journal.append(row)
        except Exception:
            self._failed = True
            self.events.record(self.src, 'journal_uncertain', seq=seq)
            return self._result(seq, 'journal_uncertain', message='기록 확인 필요 — 다시 확인하세요')
        self.events.record(self.src, 'scan', **row)
        return self._result(seq, 'scan_result', verdict, True)

    def _init_owner(self, processor=None):
        self.owner_thread_id = threading.get_ident()
        self._recovered_startup = self.events.path.exists() and self.events.path.stat().st_size > 0
        self._receipt_offset = self.events.path.stat().st_size if self.events.path.exists() else 0
        self.journal = Journal(self.data)
        self._sound = self._sound or Sound(self.data)
        self._last_seq = self.journal.state()['last_seq']
        self._intake_seq = self._last_seq
        self._next_seq = self._last_seq + 1
        self._failed, self._view_count, self._recovery_pending, self._delay_ms = False, 0, False, 0
        self._completed = OrderedDict((r['seq'], self._journal_reply(r)) for r in self.journal.rows)
        if self.journal.recovered_tail:
            self.events.record(self.src, 'journal_recovery', preserved=self.journal.recovered_tail)
        self._pending = {}
        # The processor is built on the owner thread (thread-bound handles such as sqlite).
        self._processor = processor if processor is not None else (self._processor_factory() if self._processor_factory else None)

    def _run(self):
        try: self._init_owner()
        except Exception as error:
            self._initialized.set_exception(error)
            return
        self._initialized.set_result(True)
        while not self._owner_done:  # a pick_file wait may have consumed 'close'
            try: op, value, future = self._tasks.get(timeout=0.05)
            except queue.Empty:
                self._expire_pending()
                continue
            if not self._dispatch(op, value, future): return

    def _expire_pending(self):
        pending = self._pending
        if pending and time.monotonic() - min(row[2] for row in pending.values()) > 2:
            for seq, (_, waiter, _) in pending.items():
                self.events.record(self.src, 'seq_out_of_order', seq=seq, expected=self._next_seq)
                waiter.set_result(self._result(seq, 'sequence_gap', message='순서 확인 필요 — 다시 스캔하세요'))
            pending.clear()

    def _dispatch(self, op, value, future):
        """One queued item on the owner thread. Returns False after 'close'."""
        pending = self._pending
        if op == 'close':
            for _, waiter, _ in pending.values(): waiter.set_exception(RuntimeError('bridge closed'))
            self._owner_done = True
            future.set_result(True)
            return False
        try:
            if op != 'scan': future.set_result(self._handle(op, value)); return True
            seq = value[1].get('seq')
            if type(seq) is not int or seq < 1:
                future.set_result(self._result(seq, 'invalid_scan')); return True
            if seq < self._next_seq:
                old = self._old_result(seq)
                reply = dict(old or {'accepted': False, 'ok': False, 'seq': seq, 'kind': 'sequence_conflict'})
                if old and old.get('text') != value[0]: reply.update(ok=False, accepted=False, kind='sequence_conflict')
                reply['duplicate'] = True; future.set_result(reply); return True
            if seq in pending:
                future.set_result({'ok': False, 'accepted': False, 'seq': seq, 'kind': 'sequence_conflict'}); return True
            pending[seq] = (value, future, time.monotonic())
            while self._next_seq in pending:
                scan, waiter, _ = pending.pop(self._next_seq)
                try:
                    reply = self._process_scan(scan)
                    self._last_seq = self._next_seq
                    if not reply['accepted']:
                        self.events.record(self.src, 'scan_receipt', seq=self._next_seq, text=scan[0], result=reply)
                    self._remember_result(self._next_seq, {**reply, 'text': scan[0]})
                    self._next_seq += 1
                    self.push({**reply, 'kind': 'scan_result', 'result_kind': reply['kind'],
                               'text': scan[0], 'state': self._snapshot()})
                    waiter.set_result(reply)
                except Exception as error:
                    self._failed = True
                    if not waiter.done(): waiter.set_exception(error)
        except Exception as error:
            if not future.done(): future.set_exception(error)
        return True

    def close(self):
        with self._intake_lock:
            if self._closed: return
            self._closed = True
            future = Future(); self._tasks.put_nowait(('close', None, future))
        if self._external:
            if threading.get_ident() == self.owner_thread_id: self.pump()
            elif self._initialized.done() and self._initialized.exception() is None:
                # An external owner that already stopped pumping must not hang shutdown.
                try: future.result(5)
                except Exception as error: self.events.record(self.src, 'owner_close_timeout', error=type(error).__name__)
        else:
            future.result()
            self._owner.join(2)
        self.pedal.close()
        self._publisher_stop.set(); self._pushes.put_nowait(None)
        self._publisher.join(2)

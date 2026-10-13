"""Run only on the designated test PCs. This entrypoint creates a real window."""
import argparse
import json
import os
import re
import socket
import sys
import threading
import time
from concurrent.futures import Future
from pathlib import Path

SHELL = Path(__file__).resolve().parent
PACKAGED_SITE = SHELL.parent / 'site-packages'
if PACKAGED_SITE.is_dir():
    sys.path.insert(0, str(PACKAGED_SITE))
sys.path.insert(1 if PACKAGED_SITE.is_dir() else 0, str(SHELL))

from bridge import Bridge

SETTINGS = ('AreDevToolsEnabled', 'AreBrowserAcceleratorKeysEnabled',
            'AreDefaultContextMenusEnabled', 'IsZoomControlEnabled', 'IsPinchZoomEnabled',
            'IsSwipeNavigationEnabled', 'IsStatusBarEnabled', 'IsGeneralAutofillEnabled',
            'IsPasswordAutosaveEnabled')

LEAN_ARGUMENTS = ('--disable-features=SpareRendererForSitePerProcess,msSmartScreenProtection,Translate,'
                  'OptimizationHints,MediaRouter,BackForwardCache,CalculateNativeWinOcclusion '
                  '--disable-background-networking --disable-component-update --disable-sync '
                  '--no-pings --disable-extensions --renderer-process-limit=1')
BROWSER_PRESETS = {'none': '', 'lean': LEAN_ARGUMENTS,
                   'lean-nogpu': LEAN_ARGUMENTS + ' --disable-gpu --disable-gpu-compositing',
                   'lean-inproc': LEAN_ARGUMENTS + ' --in-process-gpu'}
TRANSPORT_WAIT_SECONDS = 8
TRANSPORT_STOP_MESSAGE = '화면 준비가 늦습니다 — 잠시 뒤 다시 엽니다'


def browser_preset(name, events):
    arguments = BROWSER_PRESETS[name]
    # None preserves the previous host behavior, including inherited settings.
    # Never read/merge/log an inherited environment value.
    if arguments: os.environ['WEBVIEW2_ADDITIONAL_BROWSER_ARGUMENTS'] = arguments
    events.record('py', 'browser_preset', name=name, args=arguments)


def set_memory_target(core, level):
    from System import Enum
    prop = core.GetType().GetProperty('MemoryUsageTargetLevel')
    if prop is None: return {'supported': False}
    setattr(core, 'MemoryUsageTargetLevel', Enum.Parse(prop.PropertyType, level))
    readback = str(core.MemoryUsageTargetLevel)
    return {'supported': True, 'ok': readback == level, 'readback': readback}


def foreground_hwnd():
    import ctypes
    function = ctypes.WinDLL('user32', use_last_error=True).GetForegroundWindow
    function.argtypes, function.restype = [], ctypes.c_void_p
    return int(function() or 0)


def move_webview_focus(control):
    # This pinned WinForms assembly does not expose its controller publicly.
    # The field name and MoveFocus API were verified without creating a control.
    import clr
    from System.Reflection import BindingFlags
    from Microsoft.Web.WebView2.WinForms import WebView2
    from Microsoft.Web.WebView2.Core import CoreWebView2MoveFocusReason
    fields = {'control_focus': None, 'move_focus_called': False, 'move_focus_ok': False}
    try: fields['control_focus'] = bool(control.Focus())
    except Exception as error: fields['control_focus_error'] = type(error).__name__
    try:
        field = clr.GetClrType(WebView2).GetField('_coreWebView2Controller', BindingFlags.Instance | BindingFlags.NonPublic)
        controller = field.GetValue(control) if field is not None else None
        if controller is None: raise RuntimeError('WebView2 controller unavailable')
        fields['move_focus_called'] = True
        controller.MoveFocus(CoreWebView2MoveFocusReason.Programmatic)
        fields['move_focus_ok'] = True  # MoveFocus is void: returned without exception.
    except Exception as error: fields['move_focus_error'] = type(error).__name__
    return fields


class CallbackRegistry(dict):
    # pywebview 6.2.1 stores one UUID->None for every synchronous evaluate_js
    # but only real async callbacks have a consumer/deletion path (util.py:311).
    def __setitem__(self, ident, callback):
        if callback is not None: super().__setitem__(ident, callback)


class API:
    """Expose only the agreed intake methods and compatibility aliases."""
    def __init__(self, bridge): self._bridge, self._on_ready = bridge, None
    def scan(self, text, meta): return self._bridge.scan(text, meta)
    def state(self): return self._bridge.state()
    def f12_physical(self): return self._bridge.f12_physical()
    def command(self, name, args=None): return self._bridge.command(name, args)
    def admin(self, name, args=None): return self._bridge.admin(name, args)
    def submit_scan(self, text, meta): return self._bridge.submit_scan(text, meta)
    def get_state(self): return self._bridge.get_state()
    def ready(self, meta):
        result = self._bridge.ready(meta)
        if self._on_ready: self._on_ready(meta)
        return result
    def log_batch(self, batch): return self._bridge.log_batch(batch)


class NativeHost:
    def __init__(self, window, bridge, udf, foreground_reader=None, focus_mover=None, memory_setter=None, test_control=False, focus_probe=None):
        self.window, self.bridge, self.udf = window, bridge, udf
        self.form = None
        self.core = None
        self.refs = []
        self.focus_attached = False
        self._page_lock = threading.RLock()
        self._page_ready = False
        self._signals = []
        self._generation = 0
        self._ready_timer = None
        self._awaiting_since = 0
        self._loaded_since = 0
        self._transport_retries = 0
        self._transport_stopped = False
        self._closed = False
        self._focus_generation = -1
        self._foreground_reader = foreground_reader or foreground_hwnd
        self._focus_mover = focus_mover or move_webview_focus
        self._focus_probe = focus_probe or self._probe_focus
        self._memory_setter = memory_setter or set_memory_target
        self._memory_level = 'Normal'
        self._memory_applied = None
        self._test_control_enabled = bool(test_control)

    def before_load(self):
        # pywebview calls before_load synchronously from NavigationCompleted.
        # Do not invoke the UI or wait for the processing owner in this callback.
        self.pause_page()

    def pause_page(self):
        with self._page_lock:
            self._page_ready = False
            self._awaiting_since = time.time() * 1000
            self._generation += 1
            if self._ready_timer: self._ready_timer.cancel()

    def _arm_transport_watchdog(self):
        # Start after loaded/core setup, excluding cold environment creation.
        with self._page_lock:
            if self._page_ready or self._transport_stopped or self._closed: return
            if self._ready_timer: self._ready_timer.cancel()
            generation = self._generation
            self._loaded_since = time.monotonic()
            def expired():
                try: self.ui(lambda: self._transport_expired(generation))
                except Exception as error:
                    self.bridge.events.record('py', 'transport_watchdog_failed', generation=generation, error=type(error).__name__)
            self._ready_timer = threading.Timer(TRANSPORT_WAIT_SECONDS, expired)
            self._ready_timer.daemon = True
            self._ready_timer.start()

    def _transport_expired(self, generation):
        # UI thread; a ready/new document invalidates any queued timer work.
        with self._page_lock:
            if generation != self._generation or self._page_ready or self._transport_stopped or self._closed: return
            wait_ms = round((time.monotonic() - self._loaded_since) * 1000)
            if self._transport_retries == 0:
                self._transport_retries = 1
                self.bridge.events.record('py', 'transport_retry', generation=generation, wait_ms=wait_ms)
                self.pause_page()
                try: self.core.Reload()
                except Exception as error:
                    self._stop_transport(wait_ms, error=type(error).__name__)
            else:
                self._stop_transport(wait_ms)

    def _stop_transport(self, wait_ms, **fields):
        self._transport_stopped = True
        if self._ready_timer: self._ready_timer.cancel()
        self.bridge.events.record('py', 'transport_timeout', mode='pywebview', generation=self._generation, wait_ms=wait_ms)
        try:
            message = json.dumps(TRANSPORT_STOP_MESSAGE, ensure_ascii=False)
            self.core.ExecuteScriptAsync(f'window.KMHost && KMHost.transportStopped({message})')
        except Exception as error: fields['display_error'] = type(error).__name__
        self.bridge.events.record('py', 'transport_stopped', generation=self._generation, wait_ms=wait_ms, retries=self._transport_retries, **fields)

    def page_ready(self, meta=None):
        with self._page_lock:
            if self._closed: return
            if self._transport_stopped:
                self.bridge.events.record('py', 'ready_ignored', reason='transport_stopped', generation=self._generation)
                return
            if meta is not None and meta.get('readyAt', 0) < self._awaiting_since:
                self.bridge.events.record('py', 'ready_ignored', reason='previous_document', generation=self._generation)
                return
            self._page_ready = True
            self._transport_retries = 0
            if self._ready_timer: self._ready_timer.cancel()
        self._schedule_signals()

    def close(self):
        with self._page_lock:
            self._closed = True
            self._page_ready = False
            self._generation += 1
            if self._ready_timer: self._ready_timer.cancel()

    def _schedule_signals(self):
        from System import Action
        form = self.window.native
        if form is not None and not form.IsDisposed:
            try: form.BeginInvoke(Action(self._flush_signals))
            except Exception as error:
                self.bridge.events.record('py', 'push_failed', kind='schedule', reason=type(error).__name__)

    def _flush_signals(self):
        # Only the UI thread removes and sends items. Before load/recovery or
        # before the Python loaded handler configures the core, retain the FIFO.
        with self._page_lock:
            self._restore_focus()
            while self._page_ready and self.core and self._signals:
                name, value = self._signals[0]
                try:
                    self.core.ExecuteScriptAsync(f'window.KMHost && KMHost.{name}({json.dumps(value, ensure_ascii=False)})')
                except Exception as error:
                    self.bridge.events.record('py', 'push_failed', kind=name, reason=type(error).__name__)
                    return
                self._signals.pop(0)

    def _restore_focus(self):
        # Called only from UI work after both app.ready and core configuration.
        if not self._page_ready or not self.core or self.form is None or self._focus_generation == self._generation:
            return
        self._focus_generation = self._generation
        generation = self._generation
        fields = {'generation': generation, 'control_focus': None, 'move_focus_called': False, 'move_focus_ok': False}
        try:
            hwnd = int(self.form.Handle.ToInt64())
            if self._foreground_reader() != hwnd:
                fields['skipped'] = 'not_foreground'
            else:
                fields.update(self._focus_mover(self.form.browser.webview))
        except Exception as error:
            fields['error'] = type(error).__name__
        def observed(active, error=None):
            with self._page_lock:
                if generation != self._generation or not self._page_ready: return
                fields.update(page_active=active, ok=active is True)
                if error: fields['probe_error'] = error
                self.bridge.events.record('py', 'focus_restored', **fields)
        try: self._focus_probe(observed)
        except Exception as error: observed(None, type(error).__name__)

    def _probe_focus(self, callback):
        # No evaluate_js semaphore on the UI thread. Observe the page after the
        # focus calls, refreshing its hasFocus/visibility/host-active predicate.
        from System import Action, String
        from System.Threading.Tasks import Task
        def completed(task):
            try: active = json.loads(str(task.Result)) is True
            except Exception as error: callback(None, type(error).__name__)
            else: callback(active)
        script = 'window.KMHost ? (KMHost.focus("page_focus"), KMHost.control("get_state").active) : false'
        self.core.ExecuteScriptAsync(script).ContinueWith(Action[Task[String]](completed), self.form.browser.syncContextTaskScheduler)

    def ui(self, callback):
        from System import Action
        form = self.window.native
        if form.InvokeRequired:
            form.Invoke(Action(callback))
        else:
            callback()

    def pick_file(self, title, kinds=None):
        # pywebview 6.2.1 has no title argument for create_file_dialog.
        # Only return the path; never open/stat/read the selected file.
        # Posted to the UI thread; the owner keeps pumping until the Future settles (bridge._wait_picked).
        import webview
        from System import Action
        picked = Future()
        def choose():
            path = None
            try:
                form = self.window.native
                if self._closed or self.bridge._closed or self.bridge._exit_requested or form is None or form.IsDisposed: return
                file_types = tuple(f'{description} ({patterns})' for description, patterns in (kinds or ()))
                selected = self.window.create_file_dialog(webview.OPEN_DIALOG, allow_multiple=False, file_types=file_types)
                if selected and not self._closed and not self.bridge._closed and not self.bridge._exit_requested:
                    path = selected[0]
            except Exception as error: picked.set_exception(error)
            finally:
                if not picked.done(): picked.set_result(path)
        form = self.window.native
        if form is None or form.IsDisposed: picked.set_result(None)
        else: form.BeginInvoke(Action(choose))
        return picked

    def page_signal(self, name, value):
        with self._page_lock: self._signals.append((name, value))
        self._flush_signals()

    def notify_push(self, value):
        # A crashed renderer must not park the publisher on pywebview's
        # evaluate_js semaphore. Queue UI work; the hook never waits for it.
        with self._page_lock: self._signals.append(('push', value))
        self._schedule_signals()

    def activated(self, sender=None, args=None):
        self.memory_target('Normal')
        control = self.form.browser.webview
        control.Focus()
        self.bridge.events.record('py', 'focus', state='win_activated')
        self.page_signal('focus', 'win_activated')

    def deactivated(self, sender=None, args=None):
        self.memory_target('Low')
        self.bridge.events.record('py', 'focus', state='win_deactivated')
        self.page_signal('focus', 'win_deactivated')

    def memory_target(self, level):
        # Called by UI event/configuration handlers, never the processing owner.
        self._memory_level = level
        if self.core is None or self._memory_applied == (self.core, level): return
        self._memory_applied = (self.core, level)
        try: fields = self._memory_setter(self.core, level)
        except Exception as error: fields = {'supported': False, 'error': type(error).__name__}
        self.bridge.events.record('py', 'memory_target', level=level, **fields)

    def resized(self, sender=None, args=None):
        from System.Windows.Forms import FormWindowState
        if self.form.WindowState == FormWindowState.Minimized:
            self.memory_target('Low')
        else:
            foreground = self._foreground_reader() == int(self.form.Handle.ToInt64())
            self.memory_target('Normal' if foreground else 'Low')

    def loaded(self):
        def configure():
            self.form = self.window.native
            control = self.form.browser.webview
            core = control.CoreWebView2
            readback, errors = {}, {}
            for name in SETTINGS:
                try:
                    setattr(core.Settings, name, False)
                    readback[name] = bool(getattr(core.Settings, name))
                except Exception as error:
                    errors[name] = type(error).__name__
            self.bridge.events.record('py', 'settings', **readback, errors=errors,
                                      all_disabled=len(readback) == len(SETTINGS) and not any(readback.values()))
            if self.core != core:
                core.ProcessFailed += self.process_failed
                self.refs.append(self.process_failed)
                self.core = core
            if not self.focus_attached:
                self.form.Activated += self.activated
                self.form.Deactivate += self.deactivated
                self.form.Resize += self.resized
                self.refs.extend((self.activated, self.deactivated, self.resized))
                self.focus_attached = True
                self.bridge.events.record('py', 'window_created', hwnd=int(self.form.Handle.ToInt64()), title=self.window.title)
            self.bridge.events.record('py', 'loaded')
            try:
                foreground = self._foreground_reader() == int(self.form.Handle.ToInt64())
                level = 'Normal' if foreground else 'Low'
            except Exception:
                level = self._memory_level
            self.memory_target(level)
            self._flush_signals()
            self._arm_transport_watchdog()
        try:
            self.ui(configure)
        except Exception as error:
            self.bridge.events.record('py', 'native_setup_failed', reason=type(error).__name__)

    def process_failed(self, sender, args):
        kind = str(args.ProcessFailedKind)
        reason = str(getattr(args, 'Reason', 'unknown'))
        self.bridge.events.record('py', 'proc_failed', kind=kind, reason=reason)
        self.pause_page()
        self.bridge.mark_recovery()
        try:
            if 'BrowserProcessExited' in kind:
                # pywebview BrowserForm accesses its browser dynamically. Reattach
                # only the control; the Python bridge/journal and Form stay alive.
                from webview.platforms.edgechromium import EdgeChrome
                old = self.form.browser.webview
                self.form.Controls.Remove(old)
                old.Dispose()
                self.core = None
                self.form.browser = EdgeChrome(self.form, self.window, str(self.udf))
                def initialized(control, result):
                    if result.IsSuccess:
                        self.bridge.events.record('py', 'reload_complete', method='recreate_control')
                    else:
                        self.bridge.events.record('py', 'reload_failed', kind=kind, reason=str(result.InitializationException))
                self.form.browser.webview.CoreWebView2InitializationCompleted += initialized
                self.refs.append(initialized)
                self.bridge.events.record('py', 'reload', method='recreate_control')
            else:
                sender.Reload()
                self.bridge.events.record('py', 'reload', method='CoreWebView2.Reload')
        except Exception as error:
            self.bridge.events.record('py', 'reload_failed', kind=kind, reason=type(error).__name__)

    def reload(self):
        self.bridge.mark_recovery()
        def action():
            self.pause_page()
            self.core.Reload()
            self.bridge.events.record('py', 'reload', method='test_control')
        self.ui(action)
        return {'reloading': True}

    def churn_list(self, rows, times):
        if not self._test_control_enabled: raise ValueError('test control not enabled')
        if type(rows) is not int or type(times) is not int or not 1 <= rows <= 1000 or not 1 <= times <= 100:
            raise ValueError('rows must be 1..1000; times must be 1..100')
        options = json.dumps({'rows':rows, 'times':times})
        result = self.window.evaluate_js(f'KMHost.control("churn_list",{options})')
        self.bridge.events.record('js', 'churn_done', **result)
        return result

    def rect(self):
        from System.Drawing import Point
        page = self.window.evaluate_js('KMHost.control("rect")')
        result = {}
        def locate():
            form, control = self.window.native, self.window.native.browser.webview
            origin = control.PointToScreen(Point(0, 0))
            # CSS -> WinForms client coordinates -> screen physical coordinates.
            scale_x = control.ClientSize.Width / page['viewport']['width']
            scale_y = control.ClientSize.Height / page['viewport']['height']
            for name in ('input', 'button'):
                box = page[name]
                result[name] = {'x': origin.X + box['x'] * scale_x, 'y': origin.Y + box['y'] * scale_y,
                                'width': box['width'] * scale_x, 'height': box['height'] * scale_y}
            result['window'] = {'x': form.Bounds.X, 'y': form.Bounds.Y,
                                'width': form.Bounds.Width, 'height': form.Bounds.Height}
            result['hwnd'] = int(form.Handle.ToInt64())
            result['page'] = {'x': origin.X, 'y': origin.Y, 'width': control.ClientSize.Width,
                              'height': control.ClientSize.Height}
            result['css'] = page
        self.ui(locate)
        return result


def test_controls(path, host, stop):
    path.mkdir(parents=True, exist_ok=True)
    seen = set()
    while not stop.wait(0.05):
        for file in sorted(path.glob('cmd-*.json')):
            if file.name in seen:
                continue
            try:
                command = json.loads(file.read_text(encoding='utf-8-sig'))
            except (ValueError, OSError):
                continue  # A producer may still be writing; retry only a complete JSON.
            ident = str(command.get('id', ''))
            if not re.fullmatch(r'[A-Za-z0-9_-]{1,80}', ident):
                seen.add(file.name)
                continue
            op = command.get('op')
            try:
                if op == 'reload': result = host.reload()
                elif op == 'rect': result = host.rect()
                elif op == 'get_state':
                    result = {**host.bridge.get_state(), 'page': host.window.evaluate_js('KMHost.control("get_state")')}
                elif op == 'delay_next_scan': result = host.bridge.delay_next_scan(command.get('ms'))
                elif op == 'churn_list': result = host.churn_list(command.get('rows'), command.get('times'))
                elif op in ('focus_input', 'focus_sink', 'read_input', 'click_button'):
                    result = host.window.evaluate_js(f'KMHost.control({json.dumps(op)})')
                else: raise ValueError('unsupported operation')
                ack = {'id': command['id'], 'op': op, 'ok': True, 'result': result}
            except Exception as error:
                ack = {'id': command['id'], 'op': op, 'ok': False, 'error': type(error).__name__}
            target = path / f'ack-{ident}.json'
            temporary = path / f'ack-{ident}.json.tmp'
            temporary.write_text(json.dumps(ack, ensure_ascii=False), encoding='utf-8')
            os.replace(temporary, target)
            seen.add(file.name)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--profile', choices=('rw', 'iw'), required=True)
    parser.add_argument('--data', type=Path, required=True)
    parser.add_argument('--udf', type=Path)
    parser.add_argument('--pedal', action='store_true')
    parser.add_argument('--test-control', type=Path)
    parser.add_argument('--title-suffix', default='')
    parser.add_argument('--browser-preset', choices=tuple(BROWSER_PRESETS), default='none')
    args = parser.parse_args(argv)
    title = 'KMTech 셸 시험 — ' + ('리워크' if args.profile == 'rw' else '검사') + args.title_suffix
    return run_app(args.profile, args.data, udf=args.udf, pedal=args.pedal, title=title,
                   browser_preset_name=args.browser_preset, test_control=args.test_control)


class OwnerExited(RuntimeError):
    """owner_main returned/failed before attach_owner or fail_owner."""


def wait_owner_start(bridge, owner_thread, timeout=None, poll=0.5):
    """Wait (no window) until the app's owner attaches or reports failure. Default: no time
    limit — an HDD field PC may build its app slowly, and giving up would cut the daemon owner
    thread mid-write when the process exits. Ends early only if the owner thread is gone."""
    deadline = None if timeout is None else time.monotonic() + timeout
    while True:
        wait = poll if deadline is None else max(0, min(poll, deadline - time.monotonic()))
        try: return bridge.wait_owner(wait)
        except TimeoutError:
            if not owner_thread.is_alive():
                # attach/fail may have landed right before the thread ended
                if bridge._initialized.done(): return bridge.wait_owner(0)
                raise OwnerExited('owner thread ended before attach_owner/fail_owner')
            if deadline is not None and time.monotonic() >= deadline: raise


def run_app(profile, data, *, udf=None, pedal=False, title='KMTech', page=None, processor_factory=None,
            owner_main=None, browser_preset_name='none', test_control=None, owner_timeout=None, root=None):
    """App entry (HOST-USE.md). Call on the main thread; returns when the window closes.
    profile 'rw'|'iw' = scan framing; data = this app's shell folder (events/journal/sounds);
    page = the app's index.html (default = shell test page); root = folder served as the local
    http root (default = page's folder) so the page may load ../ shared files below root.
    processor_factory() runs on our owner thread, or owner_main(bridge) runs on a new thread
    that calls bridge.attach_owner(processor) and then bridge.pump() until it returns False."""
    page_path = Path(page).resolve() if page else SHELL / 'web/index.html'
    root_path = Path(root).resolve() if root else None
    if root_path is not None and root_path not in page_path.parents:
        raise ValueError('page must be inside root')
    data = Path(data).resolve()
    udf = Path(udf or data / 'webview2').resolve()
    udf.mkdir(parents=True, exist_ok=True)
    bridge = Bridge(data, profile, test_control=bool(test_control), processor_factory=processor_factory,
                    owner='external' if owner_main else 'thread')
    bridge.events.record('py', 'py_start', pid=os.getpid(), argv=sys.argv, profile=profile)
    owner_thread = None
    if owner_main:
        owner_thread = threading.Thread(target=owner_main, args=(bridge,), name='app-owner', daemon=True)
        owner_thread.start()
        try: wait_owner_start(bridge, owner_thread, owner_timeout)
        except Exception as error:
            bridge.events.record('py', 'owner_start_failed', error=type(error).__name__)
            bridge.close()
            raise
    browser_preset(browser_preset_name, bridge.events)
    import webview
    webview.settings['OPEN_EXTERNAL_LINKS_IN_BROWSER'] = False
    webview.settings['ALLOW_DOWNLOADS'] = False
    webview.settings['OPEN_DEVTOOLS_IN_DEBUG'] = False
    url = str(page_path) + ('#test=1' if test_control else '')
    api = API(bridge)
    window = webview.create_window(title, url=url, js_api=api, width=1366, height=768,
                                   text_select=False, zoomable=False, background_color='#F7F7F4')
    window._callbacks = CallbackRegistry({ident:callback for ident,callback in window._callbacks.items() if callback is not None})
    native = NativeHost(window, bridge, udf, test_control=bool(test_control))
    api._on_ready = native.page_ready
    window.events.before_load += native.before_load
    window.events.loaded += native.loaded
    bridge.set_notifier(native.notify_push)
    bridge.set_exit_handler(window.destroy)
    picker_closed = threading.Event()
    def picker(title, kinds=None):
        # A Future; the bridge drops it once the picker is detached (closing/closed).
        return None if picker_closed.is_set() else native.pick_file(title, kinds)
    def shown():
        if not picker_closed.is_set(): bridge.set_file_picker(picker)
    def detach_picker():
        picker_closed.set()
        bridge.set_file_picker(None)
    def closing():
        allowed = bridge.ask_closing()
        if allowed: detach_picker()
        return allowed
    window.events.shown += shown
    window.events.closing += closing  # False cancels the X (pywebview 6.2.1 winforms on_closing).
    window.events.closed += detach_picker
    native.pause_page()
    stop = threading.Event()
    window.events.closed += stop.set
    def started():
        if pedal:
            bridge.pedal.start()
        if test_control:
            threading.Thread(target=test_controls, args=(Path(test_control).resolve(), native, stop),
                             name='shell-test-control', daemon=True).start()
    code = 0
    try:
        # pywebview otherwise uses its fixed DEFAULT_HTTP_PORT in persistent
        # mode. Reserve an ephemeral candidate so RW/IW can run side by side.
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            http_port = reservation.getsockname()[1]
        if root_path is not None:
            # pywebview 6.2.1 serves the common folder of the window URLs (one window = the page's
            # folder). Start its global server on root first; window._resolve_url then maps the page
            # relative to root (webview/http.py start_global_server, window.py _resolve_url).
            from webview import http as webview_http
            webview_http.start_global_server(http_port=http_port, urls=[str(root_path)])
        bridge.events.record('py', 'http_server', port=http_port, host='127.0.0.1', root=str(root_path or page_path.parent))
        # private_mode=False binds storage to exactly --udf, avoiding the backend's
        # temporary profile and automatic recursive cleanup. This contains no auth.
        webview.start(started, gui='edgechromium', debug=False, private_mode=False,
                      storage_path=str(udf), http_server=True, http_port=http_port)
    except Exception:
        code = 1
        raise
    finally:
        stop.set()
        detach_picker()
        native.close()
        bridge.close()
        if owner_thread: owner_thread.join(5)
        bridge.events.record('py', 'exit', code=code)
    return code


if __name__ == '__main__':
    raise SystemExit(main())

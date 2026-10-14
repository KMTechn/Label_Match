"""Run only on the designated test PCs. This entrypoint creates a real window."""
import argparse
import json
import math
import os
import re
import socket
import sys
import threading
import time
from concurrent.futures import Future
from pathlib import Path
from uuid import uuid4

SHELL = Path(__file__).resolve().parent
PACKAGED_SITE = SHELL.parent / 'site-packages'
if PACKAGED_SITE.is_dir():
    sys.path.insert(0, str(PACKAGED_SITE))
sys.path.insert(1 if PACKAGED_SITE.is_dir() else 0, str(SHELL))

from bridge import Bridge, OWNER_JOIN_S
import shell_fileio
from shell_fileio import _guard

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
CLOSING_WAIT_SECONDS = 120
READY_TIMEOUT_S = 30
DESTROY_WAIT_S = 5
STARTUP_DESTROY_WAIT_S = 2
EXIT_WRITE_WAIT_S = 120
EXIT_POLL_S = 5
STARTUP_NOTICE_TEXT = '새 화면을 열지 못해 옛 화면으로 바꿉니다. 바로 가기를 한 번 더 누르세요. 그래도 열리지 않으면 현장 관리자에게 알리세요.'


def web_allowed(data, version, *, write_guard=None):
    """A failed version stays on its fallback screen until an update."""
    fileio = shell_fileio.FileIO(write_guard)
    data = Path(data).resolve()
    app_root = Path(__file__).resolve().parents[1]
    if data == app_root or app_root in data.parents:
        import logging
        logging.error('SHELL_DATA_IN_APP_ROOT')
        return False
    marker = data / 'shell-fallback.json'
    try:
        if not marker.exists():
            return True
        if marker.is_symlink() or marker.stat().st_size > 4096:
            return False
        saved = json.loads(marker.read_text(encoding='utf-8'))
        if not isinstance(saved, dict) or set(saved) != {'code', 'at', 'version'}:
            return False
        if saved.get('version') == str(version):
            return False
        fileio.remove(marker, 'udf')
        return True
    except (OSError, ValueError, TypeError, AttributeError):
        import logging
        logging.error('SHELL_FALLBACK_MARKER_UNAVAILABLE')
        return False


def _save_fallback(data, code, version, fileio=None):
    fileio = fileio or shell_fileio.FileIO()
    from datetime import datetime, timezone
    marker = {'code': code, 'at': datetime.now(timezone.utc).isoformat(), 'version': str(version)}
    fileio.replace_atomic(Path(data) / 'shell-fallback.json',
                                (json.dumps(marker, ensure_ascii=False) + '\n').encode('utf-8'), 'udf', fsync=True)


def show_startup_notice(title):
    """Best-effort OS notice, bounded by MessageBoxTimeoutW's 30-second timeout."""
    try:
        import ctypes
        from ctypes import wintypes
        notice = ctypes.WinDLL('user32', use_last_error=True).MessageBoxTimeoutW
        notice.argtypes = [wintypes.HWND, wintypes.LPCWSTR, wintypes.LPCWSTR,
                           wintypes.UINT, wintypes.WORD, wintypes.DWORD]
        notice.restype = ctypes.c_int
        if not notice(None, STARTUP_NOTICE_TEXT, str(title), 0x00040040, 0, 30000):
            raise OSError('SHELL_START_NOTICE_UNAVAILABLE')
    except Exception:
        import logging
        logging.error('SHELL_START_NOTICE_UNAVAILABLE')


def _wait_before_exit(before_exit, code):
    if before_exit is None:
        return
    def drained(seconds):
        try:
            return bool(before_exit(code, seconds))
        except Exception:
            return False
    if drained(EXIT_WRITE_WAIT_S):
        return
    import logging
    logging.error('SHELL_EXIT_DEFERRED')
    while True:
        time.sleep(EXIT_POLL_S)
        if drained(0):
            return


class ClosingWait:
    """Cancel a slow X, then observe its one owner Future off the UI thread."""
    def __init__(self, bridge, show, timeout=CLOSING_WAIT_SECONDS):
        if not math.isfinite(timeout) or timeout <= 0: raise ValueError('closing_timeout must be positive and finite')
        self.bridge, self.show, self.timeout = bridge, show, timeout
        self._lock = threading.RLock()
        self._pending, self._timer = None, None
        self._generation, self._timed_out, self._closed = 0, False, False
        self.forced = False

    def closing(self):
        with self._lock:
            if self.bridge._exit_requested or self._closed: return True
            if self._pending is not None: return False
            allowed = self.bridge.ask_closing()
            if allowed is not None: return allowed
            self._pending = self.bridge._closing_future
            self.bridge.events.record('py', 'closing_pending')
            self.show('waiting')
            self._arm()
            # Future callbacks run on the owner. Move all host work off that thread.
            self._pending.add_done_callback(lambda future: threading.Thread(
                target=self._answered, args=(future,), name='shell-closing', daemon=True).start())
            return False

    def _arm(self):
        if self._timer: self._timer.cancel()
        self._generation += 1
        self._timer = threading.Timer(self.timeout, self._expired, (self._pending, self._generation))
        self._timer.daemon = True
        self._timer.start()

    def _answered(self, future):
        with self._lock:
            if self._closed or self._pending is not future or self.bridge._exit_requested: return
            try: allowed = future.result() is not False
            except Exception: allowed = True  # same owner-error behavior as a quick answer
            self._pending = None
            self._timed_out = False
            if self._timer: self._timer.cancel()
            if allowed: self.bridge.request_exit()
            else: self.show(None)

    def _expired(self, future, generation):
        with self._lock:
            if self._closed or self._pending is not future or generation != self._generation or self.bridge._exit_requested: return
            if future.done():
                self._answered(future)
                return
            self._timed_out = True
            self.bridge.events.record('py', 'closing_timeout', wait_seconds=self.timeout)
            self.show('choice')

    def choose(self, action):
        with self._lock:
            if self._closed or self._pending is None or not self._timed_out or self.bridge._exit_requested: return
            if self._pending.done():
                self._answered(self._pending)
                return
            if action == 'wait':
                self.bridge.events.record('py', 'closing_wait_more')
                self._timed_out = False
                self.show('waiting')
                self._arm()
            elif action == 'close':
                self.bridge.events.record('py', 'closing_force')
                self.forced = True
                if self._timer: self._timer.cancel()
                self.bridge.request_exit()

    def close(self):
        with self._lock:
            self._closed = True
            if self._timer: self._timer.cancel()


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
        self._closing_banner = None

    def show_closing(self, mode, choose):
        """Native strip inside the existing Form, independent of page/owner readiness.
        BeginInvoke never parks the owner or the closing waiter on the UI thread."""
        from System import Action
        form = self.window.native
        if form is None or form.IsDisposed: return
        def render():
            if self._closed or form.IsDisposed: return
            if mode is None:
                if self._closing_banner:
                    panel, _, _, _ = self._closing_banner
                    restore_focus = panel.ContainsFocus
                    form.Controls.Remove(panel)
                    panel.Dispose()
                    self._closing_banner = None
                    if restore_focus: form.browser.webview.Focus()
                return
            if self._closing_banner is None:
                from System.Windows.Forms import Button, DockStyle, FlowLayoutPanel, Label, Padding
                from System.Drawing import Font
                panel, label, close_button, wait_button = FlowLayoutPanel(), Label(), Button(), Button()
                panel.Dock, panel.Height, panel.WrapContents = DockStyle.Top, 84, False
                panel.Font = Font('Malgun Gothic', 12)
                label.AutoSize, label.Margin = True, Padding(16, 24, 24, 0)
                for index, (button, text, action) in enumerate(((close_button, '그래도 닫기', 'close'),
                                                             (wait_button, '더 기다리기', 'wait'))):
                    button.Text, button.AccessibleName = text, text
                    button.AutoSize, button.TabStop, button.TabIndex = True, True, index
                    button.Margin = Padding(8, 16, 8, 0)
                    button.Click += lambda sender, args, action=action: choose(action)
                for control in (label, close_button, wait_button): panel.Controls.Add(control)
                form.Controls.Add(panel)
                panel.BringToFront()
                self._closing_banner = panel, label, close_button, wait_button
            _, label, close_button, wait_button = self._closing_banner
            label.Text = ('응답이 없습니다 —\n그래도 닫으면 저장 중인 것은 잃을 수 있습니다'
                          if mode == 'choice' else '끝내는 중 — 남은 일을 마치고 닫습니다')
            close_button.Visible = wait_button.Visible = mode == 'choice'
            if mode == 'choice': wait_button.Focus()  # Enter waits; Shift+Tab then Enter/Space closes
        try: form.BeginInvoke(Action(render))
        except Exception as error:
            self.bridge.events.record('py', 'closing_display_failed', error=type(error).__name__)

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
    host.bridge.fileio.make_dirs(path, 'test_control')
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
            host.bridge.fileio.replace_atomic(path / f'ack-{ident}.json', json.dumps(ack, ensure_ascii=False).encode('utf-8'), 'test_control')
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
    run_app(args.profile, args.data, udf=args.udf, pedal=args.pedal, title=title,
            browser_preset_name=args.browser_preset, test_control=args.test_control)
    return 0


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


class ShellDataRootError(RuntimeError):
    """Safe startup error for the app bridge; never includes a raw path or native error."""
    def __init__(self, code):
        self.code = code
        self.message = {
            'SHELL_DATA_IN_APP_ROOT': '셸 자료 폴더·WebView2 자료 폴더는 작업자 %LOCALAPPDATA% 아래여야 합니다(받은 자리: 앱 설치 폴더 안)',
            'SHELL_DATA_NOT_WRITABLE': '셸 자료 폴더에 쓸 수 없습니다 — 작업자 %LOCALAPPDATA% 아래여야 합니다',
        }[code]
        super().__init__(f'{code}: {self.message}')


class ShellStartupError(RuntimeError):
    """A fixed startup code, without the native exception or paths."""
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def check_data_roots(data, udf, fileio=None):
    fileio = fileio or shell_fileio.FileIO()
    app_root = Path(__file__).resolve().parents[1]
    folders = tuple(dict.fromkeys((data, udf)))
    # Check both resolved locations before even creating the external data folder.
    if any(folder == app_root or app_root in folder.parents for folder in folders):
        raise ShellDataRootError('SHELL_DATA_IN_APP_ROOT')
    try:
        for folder in folders:
            fileio.make_dirs(folder, 'udf')
            fileio.probe_writable(folder / ('.shell-write-probe-' + uuid4().hex), 'udf',
                                        cleanup_limit=20 // len(folders))
    except OSError:
        raise ShellDataRootError('SHELL_DATA_NOT_WRITABLE') from None


def run_app(profile, data, *, udf=None, pedal=False, title='KMTech', page=None, processor_factory=None,
            owner_main=None, browser_preset_name='none', test_control=None, owner_timeout=None, root=None,
            write_guard=None, closing_timeout=CLOSING_WAIT_SECONDS, on_force_exit=None,
            version='unknown', before_exit=None):
    """Run one shell screen. Startup failure after app creation drains and exits.

    Recovery closes intake, records the version marker, attempts window destruction
    for at most two seconds, drains accepted writes, then exits.
    Only failures before application creation can return to the caller for Tk.
    """
    # The bootstrap/control-plane writer belongs to this invocation/bridge.
    recovery_fileio = shell_fileio.FileIO(write_guard)
    if on_force_exit is not None and not callable(on_force_exit):
        raise TypeError('on_force_exit must be callable or None')
    if not math.isfinite(closing_timeout) or closing_timeout <= 0:
        raise ValueError('closing_timeout must be positive and finite')
    page_path = Path(page).resolve() if page else SHELL / 'web/index.html'
    root_path = Path(root).resolve() if root else None
    if root_path is not None and root_path not in page_path.parents:
        raise ValueError('page must be inside root')
    data = Path(data).resolve()
    udf = Path(udf or data / 'webview2').resolve()
    check_data_roots(data, udf, recovery_fileio)
    bridge = Bridge(data, profile, test_control=bool(test_control), processor_factory=processor_factory,
                    owner='external' if owner_main else 'thread', write_guard=write_guard)
    bridge.events.record('py', 'py_start', pid=os.getpid(), argv=sys.argv, profile=profile)
    recovery_finished = threading.Event()
    recovery_started = threading.Event()
    bridge._startup_failure = None
    bridge._startup_ready = False
    bridge._app_created = False
    owner_thread = None
    window = native = closing_wait = None
    stop, picker_closed = threading.Event(), threading.Event()
    ready, watchdog_stop = threading.Event(), threading.Event()
    failure_lock, close_lock, recovery_lock = (threading.Lock() for _ in range(3))
    bridge_closed = False
    force_requested, force_finished, normal_close_done = (threading.Event() for _ in range(3))

    def force_exit():
        try:
            if on_force_exit is not None:
                errors = []
                def finish_app():
                    try: on_force_exit()
                    except BaseException as error: errors.append(type(error).__name__)
                bridge.events.record('py', 'force_exit_start')
                worker = threading.Thread(target=finish_app, name='app-force-exit', daemon=True)
                try:
                    worker.start()
                    worker.join(2)
                except Exception as error:
                    bridge.events.record('py', 'force_exit_error', error=type(error).__name__)
                else:
                    if worker.is_alive(): bridge.events.record('py', 'force_exit_timeout', wait_seconds=2)
                    elif errors: bridge.events.record('py', 'force_exit_error', error=errors[0])
                    else: bridge.events.record('py', 'force_exit_done')
            bridge.events.record('py', 'exit', code=code, forced=True)
            os._exit(code)
        finally:
            force_finished.set()

    def watch_force_exit():
        while True:
            if force_requested.is_set() or (closing_wait is not None and closing_wait.forced):
                force_requested.set()
                force_exit()
                return
            if normal_close_done.is_set():
                return
            force_requested.wait(0.01)

    def fail_start(code):
        with failure_lock:
            if ready.is_set() or bridge._startup_failure:
                return False
            bridge._startup_failure = code
            return True

    def close_bridge():
        nonlocal bridge_closed
        with close_lock:
            if bridge_closed:
                return
            bridge_closed = True
        bridge.close()

    def recover_start(code):
        with recovery_lock:
            if recovery_started.is_set():
                return
            recovery_started.set()
        try:
            try:
                bridge.close_intake()
            except Exception:
                bridge.events.record('py', 'shell_start_cleanup_failed')
            try:
                _save_fallback(data, code, version, recovery_fileio)
            except Exception:
                import logging
                logging.error('SHELL_FALLBACK_MARK_FAILED')
            destroyed = threading.Event()
            def destroy_window():
                try:
                    if window is not None:
                        bridge._exit_requested = True
                        window.destroy()
                except Exception:
                    bridge.events.record('py', 'shell_start_cleanup_failed')
                finally:
                    destroyed.set()
            threading.Thread(target=destroy_window, name='shell-start-destroy', daemon=True).start()
            destroyed.wait(STARTUP_DESTROY_WAIT_S)
            try:
                show_startup_notice(title)
            except Exception:
                import logging
                logging.error('SHELL_START_NOTICE_UNAVAILABLE')
            _wait_before_exit(before_exit, code)
            import logging
            logging.error('%s', code)
            os._exit(3)
        finally:
            recovery_finished.set()

    def watch_start():
        if watchdog_stop.wait(READY_TIMEOUT_S) or ready.is_set():
            return
        if fail_start('SHELL_START_TIMEOUT'):
            recover_start('SHELL_START_TIMEOUT')

    code = 0
    try:
        if owner_main:
            def own():
                try:
                    owner_main(bridge)
                except BaseException as error:
                    if not bridge._initialized.done():
                        bridge.fail_owner(error)
                    else:
                        fail_start('SHELL_START_FAILED')
            owner_thread = threading.Thread(target=own, name='app-owner', daemon=True)
            owner_thread.start()
            try:
                wait_owner_start(bridge, owner_thread, owner_timeout)
                bridge._app_created = True
            except Exception:
                if not bridge._app_created:
                    bridge.events.record('py', 'owner_start_failed')
                    close_bridge()
                    raise
                fail_start('SHELL_START_FAILED')
                recover_start('SHELL_START_FAILED')
                return 'exit'
        browser_preset(browser_preset_name, bridge.events)
        import webview
        webview.settings['OPEN_EXTERNAL_LINKS_IN_BROWSER'] = False
        webview.settings['ALLOW_DOWNLOADS'] = False
        webview.settings['OPEN_DEVTOOLS_IN_DEBUG'] = False
        url = str(page_path) + ('#test=1' if test_control else '')
        api = API(bridge)
        threading.Thread(target=watch_start, name='shell-start-watch', daemon=True).start()
        window = webview.create_window(title, url=url, js_api=api, width=1366, height=768,
                                       text_select=False, zoomable=False, background_color='#F7F7F4')
        if recovery_started.is_set():
            raise ShellStartupError(bridge._startup_failure)
        window._callbacks = CallbackRegistry({ident:callback for ident,callback in window._callbacks.items() if callback is not None})
        native = NativeHost(window, bridge, udf, test_control=bool(test_control))
        def page_ready(meta):
            with failure_lock:
                if bridge._startup_failure:
                    return
                native.page_ready(meta)
                ready.set()
                bridge._startup_ready = True
                watchdog_stop.set()
        api._on_ready = page_ready
        window.events.before_load += native.before_load
        window.events.loaded += native.loaded
        bridge.set_notifier(native.notify_push)
        bridge.set_exit_handler(window.destroy)
        closing_wait = ClosingWait(bridge, lambda mode: native.show_closing(mode, closing_wait.choose), closing_timeout)
        def picker(title, kinds=None):
            return None if picker_closed.is_set() else native.pick_file(title, kinds)
        def shown():
            if not picker_closed.is_set(): bridge.set_file_picker(picker)
        def detach_picker():
            picker_closed.set()
            bridge.set_file_picker(None)
        def closing():
            allowed = closing_wait.closing()
            if allowed: detach_picker()
            return allowed
        window.events.shown += shown
        window.events.closing += closing
        window.events.closed += detach_picker
        native.pause_page()
        window.events.closed += stop.set
        def started():
            if pedal:
                bridge.pedal.start()
            if test_control:
                threading.Thread(target=test_controls, args=(Path(test_control).resolve(), native, stop),
                                 name='shell-test-control', daemon=True).start()
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            http_port = reservation.getsockname()[1]
        if root_path is not None:
            from webview import http as webview_http
            webview_http.start_global_server(http_port=http_port, urls=[str(root_path)])
        bridge.events.record('py', 'http_server', port=http_port, host='127.0.0.1', root=str(root_path or page_path.parent))
        if bridge._startup_failure:
            raise ShellStartupError(bridge._startup_failure)
        webview.start(started, gui='edgechromium', debug=False, private_mode=False,
                      storage_path=str(udf), http_server=True, http_port=http_port)
        if not ready.is_set():
            fail_start('SHELL_START_FAILED')
    except Exception:
        code = 1
        if ready.is_set() or (owner_main and not bridge._app_created):
            raise
        fail_start('SHELL_START_FAILED')
    finally:
        watchdog_stop.set()
        stop.set()
        if bridge._startup_failure:
            recover_start(bridge._startup_failure)
            recovery_finished.wait()
            return 'exit'  # Only reached by a headless replacement of os._exit.
        else:
            normal_owner_deadline = time.monotonic() + OWNER_JOIN_S
            force_watch = threading.Thread(target=watch_force_exit, name='shell-force-exit', daemon=True)
            force_watch.start()  # Arm before any potentially blocked owner close.
            try:
                close_bridge()
            except TimeoutError:
                bridge.events.record('py', 'owner_close_timeout', wait_seconds=OWNER_JOIN_S)
            for cleanup in ((closing_wait.close if closing_wait is not None else None),
                            lambda: bridge.set_file_picker(None),
                            (native.close if native is not None else None)):
                if cleanup is not None:
                    try:
                        cleanup()
                    except Exception:
                        bridge.events.record('py', 'shell_start_cleanup_failed')
        picker_closed.set()
        if not bridge._startup_failure:
            normal_close_done.set()
            force_watch.join(0.1)
            if force_requested.is_set():
                force_finished.wait()
                return 'web'
        if owner_thread:
            owner_thread.join(max(0, normal_owner_deadline - time.monotonic()))
            if owner_thread.is_alive():
                import logging
                logging.error('SHELL_OWNER_JOIN_TIMEOUT')
        bridge.events.record('py', 'exit', code=code)
    return 'exit' if bridge._startup_failure else 'web'


if __name__ == '__main__':
    raise SystemExit(main())

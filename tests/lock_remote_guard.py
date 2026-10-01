"""Reusable lock/HTTP guard; no application names, paths, or credentials.

Register application locks and native mutex acquisition in conftest, then
register fake transports (or instrument the real transport modules). The real
lock and writer admission remain in place. Events contain labels and timing
only; request arguments, bodies, headers and response data are never recorded.
"""
import functools
import threading
import time


class RemoteWhileLocked(AssertionError):
    pass


class LockRemoteGuard:
    def __init__(self, *, strict=True, site_factory=lambda: ''):
        self.strict = strict
        self.site = site_factory
        self.local = threading.local()
        self.events = []
        self.node = ''

    def held(self):
        if not hasattr(self.local, 'held'):
            self.local.held = []
        return self.local.held

    def enter(self, identity, kind, site=''):
        self.held().append((identity, kind, site))
        self.events.append({'event': 'acquired', 'kind': kind, 'site': site, 'thread': threading.get_ident(), 'at': time.monotonic(), 'node': self.node})

    def leave(self, identity):
        held = self.held()
        for index in range(len(held) - 1, -1, -1):
            if held[index][0] == identity:
                _identity, kind, site = held.pop(index)
                self.events.append({'event': 'released', 'kind': kind, 'site': site, 'thread': threading.get_ident(), 'at': time.monotonic(), 'node': self.node})
                return

    def transport(self, label):
        locks = [{'kind': kind, 'site': site} for _identity, kind, site in self.held()]
        self.events.append({'event': 'remote', 'transport': label, 'locks': locks, 'thread': threading.get_ident(), 'at': time.monotonic(), 'node': self.node})
        if locks and self.strict:
            raise RemoteWhileLocked('remote request while holding ' + ', '.join(item['kind'] + '@' + item['site'] for item in locks))

    def wrap_transport(self, function, label='HTTP'):
        @functools.wraps(function)
        def wrapped(*args, **kwargs):
            self.transport(label)
            return function(*args, **kwargs)
        return wrapped

    def wrap_lock(self, lock, *, label='thread lock', site=''):
        return TrackedLock(lock, self, site, label)

    def wrap_mutex_acquire(self, acquire, *, label='writer mutex'):
        guard = self
        class Lease:
            def __init__(self, raw, site):
                self.raw = raw
                self.closed = False
                guard.enter(id(self), label, site)
            def __getattr__(self, name):
                return getattr(self.raw, name)
            def close(self):
                if not self.closed:
                    self.raw.close()
                    guard.leave(id(self))
                    self.closed = True
            def __enter__(self):
                return self
            def __exit__(self, *args):
                self.close()
        @functools.wraps(acquire)
        def wrapped(*args, **kwargs):
            site = guard.site()
            started = time.monotonic()
            guard.events.append({'event': 'mutex_wait_started', 'kind': label, 'site': site, 'thread': threading.get_ident(), 'at': started, 'node': guard.node})
            try:
                raw = acquire(*args, **kwargs)
            except Exception as error:
                guard.events.append({'event': 'mutex_wait_failed', 'kind': label, 'site': site, 'thread': threading.get_ident(), 'at': time.monotonic(), 'wait_seconds': time.monotonic() - started, 'error_type': type(error).__name__, 'node': guard.node})
                raise
            guard.events.append({'event': 'mutex_wait_finished', 'kind': label, 'site': site, 'thread': threading.get_ident(), 'at': time.monotonic(), 'wait_seconds': time.monotonic() - started, 'acquired': raw is not None, 'node': guard.node})
            return Lease(raw, site) if raw is not None else None
        return wrapped

    def instrument_transports(self, monkeypatch):
        import http.client
        import urllib.request
        import requests.sessions
        for owner, attribute, label in ((requests.sessions.Session, 'request', 'requests'), (urllib.request, 'urlopen', 'urllib'), (http.client.HTTPConnection, 'request', 'http.client')):
            monkeypatch.setattr(owner, attribute, self.wrap_transport(getattr(owner, attribute), label))

    def assert_clean(self):
        bad = [event for event in self.events if event['event'] == 'remote' and event['locks']]
        if bad:
            raise RemoteWhileLocked(f'{len(bad)} remote requests while holding registered locks')


class TrackedLock:
    def __init__(self, raw, guard, site='', label='thread lock'):
        self.raw, self.guard, self.site, self.label = raw, guard, site, label
    def acquire(self, *args, **kwargs):
        result = self.raw.acquire(*args, **kwargs)
        if result:
            self.guard.enter(id(self), self.label, self.site)
        return result
    def release(self):
        self.raw.release()
        self.guard.leave(id(self))
    def __enter__(self):
        self.acquire()
        return self
    def __exit__(self, *args):
        self.release()
    def __getattr__(self, name):
        return getattr(self.raw, name)
    def _is_owned(self):
        if hasattr(self.raw, '_is_owned'):
            return self.raw._is_owned()
        if self.raw.acquire(False):
            self.raw.release()
            return False
        return True
    def _release_save(self):
        depth = sum(item[0] == id(self) for item in self.guard.held())
        for _ in range(depth):
            self.guard.leave(id(self))
        if hasattr(self.raw, '_release_save'):
            return self.raw._release_save(), depth
        self.raw.release()
        return None, depth
    def _acquire_restore(self, state):
        raw_state, depth = state
        if hasattr(self.raw, '_acquire_restore'):
            self.raw._acquire_restore(raw_state)
        else:
            self.raw.acquire()
        for _ in range(depth):
            self.guard.enter(id(self), self.label, self.site)

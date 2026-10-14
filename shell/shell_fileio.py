"""The shell's only local file writes (v3.14). Every write runs inside the app's write guard.

An app with a writer fence passes FileIO(factory) (or run_app(write_guard=)) before the
shell writes anything: factory(op, path, kind) returns a context manager entered around that one
write. kind = 'events' | 'journal' | 'sound_cache' | 'udf' | 'test_control' (choose a policy without
matching path strings). Without a factory the writes are plain. Each bridge has its own FileIO, including its close fence.
Startup folder creation and exclusive create/remove probes use kind 'udf'.
Other shell modules must not write files (sub-1/test_v314.py checks this statically).
"""
import contextlib
import os
import re
import time
from pathlib import Path

SHELL_WRITE_SITES = ('append_bytes', 'create_new', 'truncate', 'replace_atomic', 'make_dirs', 'probe_writable', 'remove')


class ShellWriteRefused(RuntimeError):
    pass


class FileIO:
    """One bridge's writes; neither guards nor closure state are module globals."""
    def __init__(self, factory=None, closed=None):
        if factory is not None and not callable(factory):
            raise TypeError('write guard must be callable or None')
        self.factory = factory
        self.closed = closed or (lambda: False)

    def guard(self, op, path, kind):
        if self.closed():
            refused = getattr(self.factory, 'refused_type', ShellWriteRefused)
            raise refused('SHELL_WRITE_CLOSED')
        alive = getattr(self.factory, 'owner_alive', None)
        if alive is not None:
            try:
                held = bool(alive())
            except Exception:
                held = False
            if not held:
                refused = getattr(self.factory, 'refused_type', ShellWriteRefused)
                raise refused('SHELL_WRITE_ADMISSION_EXPIRED')
        return contextlib.nullcontext() if self.factory is None else self.factory(op, Path(path), kind)

    def make_dirs(self, path, kind):
        return make_dirs(path, kind, fileio=self)

    def probe_writable(self, path, kind, cleanup_limit=20):
        return probe_writable(path, kind, cleanup_limit, fileio=self)

    def append_bytes(self, path, data, kind, fsync=False):
        return append_bytes(path, data, kind, fsync, fileio=self)

    def create_new(self, path, data, kind):
        return create_new(path, data, kind, fileio=self)

    def truncate(self, path, size, kind):
        return truncate(path, size, kind, fileio=self)

    def remove(self, path, kind):
        return remove(path, kind, fileio=self)

    def replace_atomic(self, path, data, kind, fsync=False):
        return replace_atomic(path, data, kind, fsync, fileio=self)


def set_write_guard(factory):
    """Compatibility factory: returns a private writer; never installs global state."""
    return FileIO(factory)


def _guard(op, path, kind, *, owner=None):
    return contextlib.nullcontext() if owner is None else owner.guard(op, path, kind)


def make_dirs(path, kind, *, fileio=None):
    with _guard('make_dirs', path, kind, owner=fileio):
        Path(path).mkdir(parents=True, exist_ok=True)


def probe_writable(path, kind, cleanup_limit=None, *, fileio=None):
    """Sweep all empty UUID probes, then verify exclusive creation and removal.

    The retained cleanup_limit argument is ignored. A failed removal fails this
    startup probe; a future start retries it. Unrelated files and links stay put.
    """
    with _guard('probe_writable', path, kind, owner=fileio):
        for old in Path(path).parent.glob('.shell-write-probe-*'):
            if (not re.fullmatch(r'\.shell-write-probe-[0-9a-f]{32}', old.name)
                    or old.is_symlink() or not old.is_file() or old.stat().st_size):
                continue
            remove(old, kind, fileio=fileio)
        with open(path, 'xb'):
            pass
        remove(path, kind, fileio=fileio)  # A failed removal stays in the next startup sweep.


def append_bytes(path, data, kind, fsync=False, *, fileio=None):
    with _guard('append_bytes', path, kind, owner=fileio):
        with open(path, 'ab') as stream:
            stream.write(data)
            if fsync:
                stream.flush(); os.fsync(stream.fileno())


def create_new(path, data, kind, *, fileio=None):
    """Exclusive create + fsync (never replaces an existing file)."""
    with _guard('create_new', path, kind, owner=fileio):
        with open(path, 'xb') as stream:
            stream.write(data); stream.flush(); os.fsync(stream.fileno())


def truncate(path, size, kind, *, fileio=None):
    with _guard('truncate', path, kind, owner=fileio):
        with open(path, 'r+b') as stream:
            stream.truncate(size); stream.flush(); os.fsync(stream.fileno())


def remove(path, kind, *, fileio=None):
    with _guard('remove', path, kind, owner=fileio):
        Path(path).unlink()


def replace_atomic(path, data, kind, fsync=False, *, fileio=None):
    """Write <name>.tmp, then os.replace onto path (readers see the old or the new file)."""
    path = Path(path)
    with _guard('replace_atomic', path, kind, owner=fileio):
        temporary = path.with_name(path.name + '.tmp')
        with open(temporary, 'wb') as stream:
            stream.write(data)
            if fsync:
                stream.flush(); os.fsync(stream.fileno())
        first_error = None
        delays = iter((0.1, 0.2, 0.4, 0.8))
        while True:
            try:
                os.replace(temporary, path)
                break
            except OSError as error:
                if getattr(error, 'winerror', None) not in (5, 32):
                    raise
                if first_error is None:
                    first_error = error
                delay = next(delays, None)
                if delay is None:
                    raise first_error
                time.sleep(delay)

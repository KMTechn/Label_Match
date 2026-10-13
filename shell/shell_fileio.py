"""The shell's only local file writes (v3.14). Every write runs inside the app's write guard.

An app with a writer fence installs set_write_guard(factory) (or run_app(write_guard=)) before the
shell writes anything: factory(op, path, kind) returns a context manager entered around that one
write. kind = 'events' | 'journal' | 'sound_cache' | 'udf' | 'test_control' (choose a policy without
matching path strings). Without a factory the writes are plain, as before v3.14.
Other shell modules must not write files (sub-1/test_v314.py checks this statically).
"""
import contextlib
import os
from pathlib import Path

SHELL_WRITE_SITES = ('append_bytes', 'create_new', 'truncate', 'replace_atomic', 'make_dirs')
_guard_factory = None


def set_write_guard(factory):
    """factory(op, path, kind) -> context manager around one write; None = plain writes."""
    global _guard_factory
    if factory is not None and not callable(factory): raise TypeError('write guard must be callable or None')
    _guard_factory = factory


def _guard(op, path, kind):
    return contextlib.nullcontext() if _guard_factory is None else _guard_factory(op, Path(path), kind)


def make_dirs(path, kind):
    with _guard('make_dirs', path, kind):
        Path(path).mkdir(parents=True, exist_ok=True)


def append_bytes(path, data, kind, fsync=False):
    with _guard('append_bytes', path, kind):
        with open(path, 'ab') as stream:
            stream.write(data)
            if fsync:
                stream.flush(); os.fsync(stream.fileno())


def create_new(path, data, kind):
    """Exclusive create + fsync (never replaces an existing file)."""
    with _guard('create_new', path, kind):
        with open(path, 'xb') as stream:
            stream.write(data); stream.flush(); os.fsync(stream.fileno())


def truncate(path, size, kind):
    with _guard('truncate', path, kind):
        with open(path, 'r+b') as stream:
            stream.truncate(size); stream.flush(); os.fsync(stream.fileno())


def replace_atomic(path, data, kind):
    """Write <name>.tmp, then os.replace onto path (readers see the old or the new file)."""
    path = Path(path)
    with _guard('replace_atomic', path, kind):
        temporary = path.with_name(path.name + '.tmp')
        with open(temporary, 'wb') as stream:
            stream.write(data)
        os.replace(temporary, path)

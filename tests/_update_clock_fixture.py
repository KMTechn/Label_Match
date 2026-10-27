"""Keep updater clocks local while durable publication uses its real clock."""
import builtins
import time as _stdlib_time

import auto_update as _updates
import user_relay as _relay


class _UpdateTime:
    def __getattr__(self, name):
        return getattr(_stdlib_time, name)

    def __setattr__(self, name, value):
        if name in {"monotonic", "sleep"}:
            object.__setattr__(self, name, value)
        else:
            setattr(_stdlib_time, name, value)


def isolated_update_time(monkeypatch):
    clock = _updates.time
    if clock is _stdlib_time:
        clock = _UpdateTime()
    for module in (_updates, _relay):
        if module.time is _stdlib_time:
            monkeypatch.setattr(module, "time", clock)

    # _apply_prepared_once imports time locally. Scope that import to the same
    # updater clock without changing time imported by durable IO or the runner.
    previous_import = builtins.__import__
    original_import = getattr(previous_import, "_update_fixture_original", previous_import)

    def local_import(name, globals=None, locals=None, fromlist=(), level=0):
        if (name == "time" and level == 0 and globals is not None
                and globals.get("__name__") in {_updates.__name__, _relay.__name__}):
            return clock
        return original_import(name, globals, locals, fromlist, level)

    local_import._update_fixture_original = original_import
    monkeypatch.setattr(builtins, "__import__", local_import)
    return clock

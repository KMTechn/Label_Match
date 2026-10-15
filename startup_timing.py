"""Value-free, monotonic observations of the existing startup gates."""
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
import time


_observer = ContextVar("startup_timing_observer", default=None)
VALUE_FREE_STAGES = frozenset({
    "main_function_enter", "fast_window_first_miss", "full_lifecycle_verify",
    "full_inventory", "full_observe_lifecycle", "full_legacy_task_query",
    "full_bootstrap_bytes", "full_data_generation", "full_db_open",
    "full_task_retirement", "full_relay_reuse",
})


@contextmanager
def startup_timing_scope(observer):
    token = _observer.set(observer)
    try:
        yield
    finally:
        _observer.reset(token)


def startup_timing_event(stage, result_code, started):
    observer = _observer.get()
    if observer is not None:
        try:
            observer(stage, result_code=result_code,
                     elapsed_ms=round(max(0.0, time.monotonic() - started) * 1000, 3))
        except Exception:
            pass  # Diagnostics cannot change admission or hide its exception.


@contextmanager
def startup_timing_step(stage):
    started = time.monotonic()
    result = "RAISED"
    try:
        yield
        result = "RETURNED"
    finally:
        startup_timing_event(stage, result, started)


def startup_timed(stage):
    def decorate(function):
        @wraps(function)
        def observed(*args, **kwargs):
            with startup_timing_step(stage):
                return function(*args, **kwargs)
        return observed
    return decorate


def observe_startup_timing(observer):
    def decorate(function):
        @wraps(function)
        def observed(*args, **kwargs):
            with startup_timing_scope(observer):
                return function(*args, **kwargs)
        return observed
    return decorate

"""Value-free, monotonic observations of the existing startup gates."""
from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
import time


_observer = ContextVar("startup_timing_observer", default=None)
_stop_checkpoint = ContextVar("startup_stop_checkpoint", default=None)


class StartupStopRequested(Exception):
    """A cooperative stop at a completed startup durability boundary."""


@contextmanager
def startup_stop_scope(checkpoint):
    token = _stop_checkpoint.set(checkpoint)
    try:
        yield
    finally:
        _stop_checkpoint.reset(token)


def startup_stop_checkpoint():
    checkpoint = _stop_checkpoint.get()
    if checkpoint is not None:
        checkpoint()


def startup_wait(seconds, wait=time.sleep):
    # Keep the existing owner polling cadence, including during survival waits.
    remaining = max(0.0, seconds)
    while remaining > 0:
        startup_stop_checkpoint()
        step = min(0.1, remaining)
        wait(step)
        remaining = max(0.0, remaining - step)
    startup_stop_checkpoint()
VALUE_FREE_STAGES = frozenset({
    "main_function_enter", "fast_window_first_miss", "full_lifecycle_verify",
    "full_inventory", "full_observe_lifecycle", "full_legacy_task_query",
    "full_bootstrap_bytes", "full_data_generation", "full_db_open",
    "full_task_retirement", "full_relay_reuse",
    "full_relay_owner_query", "full_relay_survival_wait", "full_relay_bootstrap_wait",
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


def startup_progress(stage):
    """Record entering a value-free wait using the existing stage-only trace."""
    observer = _observer.get()
    if observer is not None:
        try:
            observer(stage)
        except Exception:
            pass


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

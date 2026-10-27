"""Use the real value-free timing scopes in extracted startup entrypoints."""
from startup_timing import (
    StartupStopRequested,
    observe_startup_timing,
    startup_stop_scope,
)


def bind_startup_timing_namespace(namespace):
    namespace.setdefault("observe_startup_timing", observe_startup_timing)
    namespace.setdefault("startup_stop_scope", startup_stop_scope)
    namespace.setdefault("StartupStopRequested", StartupStopRequested)


def startup_body_namespace(function):
    from inspect import unwrap
    return unwrap(function).__globals__

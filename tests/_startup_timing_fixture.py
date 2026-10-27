"""Use the real value-free timing scopes in extracted startup entrypoints."""
from startup_timing import (
    StartupStopRequested,
    observe_startup_timing,
    startup_stop_scope,
)
from package_errors import PackageRecoveryEvidenceError, PackageRecoveryStorageError
from package_logistics import PackageLogisticsError


def bind_startup_timing_namespace(namespace):
    namespace.setdefault("observe_startup_timing", observe_startup_timing)
    namespace.setdefault("startup_stop_scope", startup_stop_scope)
    namespace.setdefault("StartupStopRequested", StartupStopRequested)


def bind_startup_exception_namespace(namespace):
    for exception in (PackageLogisticsError, PackageRecoveryStorageError, PackageRecoveryEvidenceError):
        namespace.setdefault(exception.__name__, exception)


def startup_body_namespace(function):
    from inspect import unwrap
    return unwrap(function).__globals__

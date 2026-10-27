"""Keep strict report redaction failures outside application state transitions."""
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import os
from pathlib import Path
from threading import RLock

UNAVAILABLE = "보고 못 씀"
DIAGNOSTIC_FIELDS = frozenset(("failure", "message", "detail", "stdout", "stderr", "stdout_tail", "stderr_tail"))
_LOCK = RLock()
_COUNTS = {}
_ATTEMPT = ContextVar("diagnostic_report_attempt", default=None)


@contextmanager
def report_attempt():
    token = _ATTEMPT.set(set())
    try:
        yield
    finally:
        _ATTEMPT.reset(token)

@dataclass(frozen=True)
class PreparedReport:
    text: str
    unavailable: bool = False


def unavailable_diagnostics(value, fields):
    if isinstance(value, dict):
        return {key: UNAVAILABLE if key in fields else unavailable_diagnostics(item, fields)
                for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [unavailable_diagnostics(item, fields) for item in value]
    return value


def failure_count(path=None):
    """Process-local aggregate for reports in one directory; no payload is retained."""
    key = None if path is None else str(Path(path).absolute().parent)
    with _LOCK:
        return _COUNTS.get(key, 0)


def _note_unavailable(path, counter_writer):
    if path is None:
        key = None
    else:
        key = str(Path(path).absolute().parent)
    with _LOCK:
        attempt = _ATTEMPT.get()
        report_key = None if path is None else str(Path(path).absolute())
        if attempt is not None:
            if report_key in attempt:
                return
            attempt.add(report_key)
        count = _COUNTS.get(key, 0) + 1
        _COUNTS[key] = count
        _write_counter(path, counter_writer, count)


def _write_counter(path, counter_writer, count):
    if path is not None and counter_writer is not None:
        counter = Path(path).parent / (".report-unavailable-count-" + str(os.getpid()) + ".json")
        try:
            counter_writer(counter, {"count": count})
        except OSError:
            pass


def flush_report_counter(prepared, *, path, writer):
    if prepared.unavailable:
        with _LOCK:
            _write_counter(path, writer, _COUNTS.get(str(Path(path).absolute().parent), 0))


def prepare_report(payload, *, render, fallback_render, path=None,
                   diagnostic_fields=None, counter_writer=None,
                   unavailable_codes=("REPORT_UNAVAILABLE", "MACHINE_METADATA_SECRET_DETECTED")):
    """Validate a replacement before publishing or counting; never return rejected text."""
    def replacement():
        value = ({"report_status": "REPORT_UNAVAILABLE", "message": UNAVAILABLE}
                 if diagnostic_fields is None else unavailable_diagnostics(payload, diagnostic_fields))
        return PreparedReport(fallback_render(value), True)
    return protect_diagnostic(lambda: PreparedReport(render(payload)), fallback=replacement,
                              path=path, counter_writer=counter_writer,
                              unavailable_codes=unavailable_codes)


@contextmanager
def report_io(prepared, *, required):
    try:
        yield
    except OSError:
        if required or not prepared.unavailable:
            raise


def publish_report(prepared, writer, *, required):
    with report_io(prepared, required=required):
        return writer(prepared.text)


def protect_diagnostic(build, *, fallback, path=None, counter_writer=None,
                       unavailable_codes=("REPORT_UNAVAILABLE", "MACHINE_METADATA_SECRET_DETECTED")):
    """The same guard also covers diagnostic construction before JSON serialization."""
    try:
        return build()
    except ValueError as error:
        if str(error) not in unavailable_codes:
            raise
    result = fallback()
    _note_unavailable(path, counter_writer)
    return result

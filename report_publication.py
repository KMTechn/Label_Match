"""Keep strict report redaction failures outside application state transitions."""
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
import re
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


def unavailable_diagnostics(value, fields, refuses=None):
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            replacement = unavailable_diagnostics(item, fields, refuses)
            if key in fields:
                if isinstance(item, str) and item:
                    replacement = UNAVAILABLE
                elif item is not None and refuses is not None and refuses(key, replacement):
                    replacement = UNAVAILABLE
            result[key] = replacement
        return result
    if isinstance(value, (list, tuple)):
        return [unavailable_diagnostics(item, fields, refuses) for item in value]
    return value


def _anonymous_diagnostics(value, diagnostic=False):
    fields = DIAGNOSTIC_FIELDS | frozenset(("failure_traceback", "traceback", "original_failure_text", "failure_details"))
    if isinstance(value, dict):
        return {key: _anonymous_diagnostics(item, diagnostic or key in fields)
                for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_anonymous_diagnostics(item, diagnostic) for item in value]
    if diagnostic and isinstance(value, str):
        return re.sub(r"\[(?:REDACTED:[^\]]*|REDACTION_NOTE:short_value_skipped:[^\]]*)\]", UNAVAILABLE, value)
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
    _write_counter(path, counter_writer)


def _write_counter(path, counter_writer):
    if path is None or counter_writer is None:
        return
    counter = Path(path).parent / ".report-unavailable-count.json"
    key = str(Path(path).absolute().parent)
    while True:
        with _LOCK:
            count = _COUNTS.get(key, 0)
        try:
            counter_writer(counter, {"count": count})
        except Exception:
            # Auxiliary evidence cannot replace the operation's result or cause.
            return
        with _LOCK:
            if _COUNTS.get(key, 0) == count:
                return
        # A later count raced this write; publish the latest snapshot unlocked.


def flush_report_counter(prepared, *, path, writer):
    if prepared.unavailable:
        _write_counter(path, writer)


def prepare_report(payload, *, render, fallback_render, path=None,
                   diagnostic_fields=None, counter_writer=None, fallback_tree=None,
                   unavailable_codes=("REPORT_UNAVAILABLE", "MACHINE_METADATA_SECRET_DETECTED")):
    """Validate a replacement before publishing or counting; never return rejected text."""
    def refuses(key, value):
        return protect_diagnostic(
            lambda: (fallback_render({key: value}), False)[1], fallback=lambda: True,
            unavailable_codes=unavailable_codes, record_failure=False)
    def replacement():
        value = ({"report_status": "REPORT_UNAVAILABLE", "message": UNAVAILABLE}
                 if diagnostic_fields is None else unavailable_diagnostics(payload, diagnostic_fields, refuses))
        if fallback_tree is not None:
            value = fallback_tree(value)
        return PreparedReport(fallback_render(_anonymous_diagnostics(value)), True)
    return protect_diagnostic(lambda: PreparedReport(render(payload)), fallback=replacement,
                              path=path, counter_writer=counter_writer,
                              unavailable_codes=unavailable_codes)


@contextmanager
def report_io(prepared, *, required):
    outcome = {"written": False}
    try:
        yield outcome
        outcome["written"] = True
    except OSError:
        if required or not prepared.unavailable:
            raise


def protect_diagnostic(build, *, fallback, path=None, counter_writer=None,
                       unavailable_codes=("REPORT_UNAVAILABLE", "MACHINE_METADATA_SECRET_DETECTED"),
                       record_failure=True):
    """The same guard also covers diagnostic construction before JSON serialization."""
    try:
        return build()
    except ValueError as error:
        if str(error) not in unavailable_codes:
            raise
    result = fallback()
    if record_failure:
        _note_unavailable(path, counter_writer)
    return result

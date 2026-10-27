"""Keep strict report redaction failures outside application state transitions."""
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import json
from pathlib import Path
from threading import RLock

UNAVAILABLE = "보고 못 씀"
DIAGNOSTIC_FIELDS = frozenset(("failure", "message", "detail", "stdout", "stderr", "stdout_tail", "stderr_tail"))
_LOCK = RLock()
_COUNTS = {}
_DELTAS = {}
_FLUSHING = set()
_ATTEMPT = ContextVar("diagnostic_report_attempt", default=None)


@contextmanager
def report_attempt():
    if _ATTEMPT.get() is not None:
        yield
        return
    token = _ATTEMPT.set({"pending": set(), "counted": set()})
    try:
        yield
    finally:
        _ATTEMPT.reset(token)


@dataclass(frozen=True)
class PreparedReport:
    text: str
    unavailable: bool = False


def unavailable_diagnostics(value, fields, refuses, *, replace_strings=True):
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            replacement = unavailable_diagnostics(item, fields, refuses, replace_strings=replace_strings)
            if key in fields and item is not None:
                if (replace_strings and isinstance(item, str) and item) or refuses(key, replacement):
                    replacement = UNAVAILABLE
            result[key] = replacement
        return result
    if isinstance(value, (list, tuple)):
        return [unavailable_diagnostics(item, fields, refuses, replace_strings=replace_strings) for item in value]
    return value


def _anonymous_text(text):
    # Redaction can nest its own labels when a known value occurs in REDACTED.
    # Match the complete generated bracket group, including every nested label.
    stack = []
    closing = {}
    prefix = {}
    for index, char in enumerate(text):
        if char == "[":
            stack.append(index)
        elif char == "]" and stack:
            closing[stack.pop()] = index
    for index in range(len(text) - 1, -1, -1):
        if text[index] == "[":
            prefix[index] = prefix.get(index + 1, index + 1)
    result = []
    index = 0
    while index < len(text):
        end = closing.get(index)
        start = prefix.get(index, index)
        named = text.startswith(("REDACTED:", "REDACTED]", "REDACTION_NOTE:short_value_skipped:"), start)
        if end is not None and named:
            result.append(UNAVAILABLE)
            index = end + 1
        else:
            result.append(text[index])
            index += 1
    return "".join(result)


def _anonymous_diagnostics(value, diagnostic=False):
    fields = DIAGNOSTIC_FIELDS | frozenset(("failure_traceback", "traceback", "original_failure_text", "failure_details"))
    if isinstance(value, dict):
        return {key: _anonymous_diagnostics(item, diagnostic or key in fields)
                for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_anonymous_diagnostics(item, diagnostic) for item in value]
    if diagnostic and isinstance(value, str):
        return _anonymous_text(value)
    return value


def failure_count(path=None):
    """Process-local count of published unavailable reports in one directory."""
    key = None if path is None else str(Path(path).absolute().parent)
    with _LOCK:
        return _COUNTS.get(key, 0)


def mark_unavailable(path):
    attempt = _ATTEMPT.get()
    if attempt is not None and path is not None:
        attempt["pending"].add(str(Path(path).absolute()))


def record_publication(prepared, *, path, writer):
    """Call only after successful publication/readback, never during preparation."""
    report_key = str(Path(path).absolute())
    key = str(Path(path).absolute().parent)
    with _LOCK:
        attempt = _ATTEMPT.get()
        if not prepared.unavailable and not (attempt and report_key in attempt["pending"]):
            return
        if attempt is not None:
            if report_key in attempt["counted"]:
                return
            attempt["counted"].add(report_key)
        _COUNTS[key] = _COUNTS.get(key, 0) + 1
        _DELTAS[key] = _DELTAS.get(key, 0) + 1
        if key in _FLUSHING:
            return
        _FLUSHING.add(key)
    counter = Path(path).parent / ".report-unavailable-count.json"
    while True:
        with _LOCK:
            delta = _DELTAS.pop(key, 0)
            if not delta:
                _FLUSHING.remove(key)
                return
        try:
            try:
                previous = json.loads(counter.read_text(encoding="utf-8"))["count"]
            except FileNotFoundError:
                previous = 0
            if type(previous) is not int or previous < 0:
                raise ValueError("invalid diagnostic counter")
            # No count lock is held while acquiring a product writer admission.
            writer(counter, {"count": previous + delta})
        except Exception:
            with _LOCK:
                _DELTAS[key] = _DELTAS.get(key, 0) + delta
                _FLUSHING.remove(key)
            # Auxiliary evidence cannot replace the operation's result or cause.
            return


def prepare_report(payload, *, render, fallback_render, unavailable_codes,
                   diagnostic_fields=None, fallback_payload=None, replace_strings=True):
    """Validate a replacement before publication; never return rejected text."""
    def refuses(key, value):
        return protect_diagnostic(
            lambda: (fallback_render({key: value}), False)[1], fallback=lambda: True,
            unavailable_codes=unavailable_codes)

    def replacement():
        source = payload if fallback_payload is None else fallback_payload()
        value = ({"report_status": "REPORT_UNAVAILABLE", "message": UNAVAILABLE}
                 if diagnostic_fields is None else unavailable_diagnostics(
                     source, diagnostic_fields, refuses, replace_strings=replace_strings))
        return PreparedReport(fallback_render(_anonymous_diagnostics(value)), True)

    return protect_diagnostic(lambda: PreparedReport(render(payload)), fallback=replacement,
                              unavailable_codes=unavailable_codes)


def protect_diagnostic(build, *, fallback, unavailable_codes, path=None):
    """The same guard also covers diagnostic construction before serialization."""
    try:
        return build()
    except ValueError as error:
        if str(error) not in unavailable_codes:
            raise
    result = fallback()
    mark_unavailable(path)
    return result

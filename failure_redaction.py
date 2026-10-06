"""Installation failure sink contract; raw diagnostics stay in memory."""
import json
import os
from pathlib import Path
import re
import hashlib
import tempfile
import argparse

RULES = json.loads((Path(__file__).parent / "tools/failure_redaction_rules.json").read_text(encoding="utf-8"))


class InstallerArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        emit_support_failure(ValueError(message))
        raise SystemExit(2)


def redact_success_summary(value):
    """Preserve the success schema while removing URL credentials and secrets."""
    from urllib.parse import urlsplit, urlunsplit
    def clean(item):
        if isinstance(item, dict):
            result = {}
            for key, field in item.items():
                if isinstance(field, str) and (key == "url" or key.endswith("_url")):
                    try:
                        parts = urlsplit(field)
                        field = urlunsplit((parts.scheme, parts.netloc.rsplit("@", 1)[-1],
                                            parts.path, "", ""))
                    except ValueError:
                        field = "[REDACTED:url]"
                result[key] = clean(field)
            return result
        if isinstance(item, list):
            return [clean(field) for field in item]
        return item
    return redact_json_for_sink(clean(value))


def public_record(fields, allowed_codes=None, safe_messages=None):
    """Allow only typed, source-declared fields in a public diagnostic record."""
    codes = set(RULES["allowed_codes"] if allowed_codes is None else allowed_codes)
    messages = set(RULES["safe_messages"] if safe_messages is None else safe_messages)
    patterns = {
        "key": r"[a-z][a-z0-9_]{0,63}", "identifier": r"[A-Za-z_][A-Za-z0-9_.]{0,127}",
        "sha256": r"[0-9a-f]{64}", "commit": r"(?:[0-9a-f]{40}|[0-9a-f]{64})",
        "version": r"v?[0-9]{1,5}(?:\.[0-9]{1,5}){1,3}",
        "id": r"(?:[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}|[0-9A-Fa-f]{32}|[0-9A-Fa-f]{64})",
        "utc": r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,7})?Z",
        "path": r'(?:[A-Za-z]:\\[^\x00-\x1f"<>|?*]{0,256}|/[^\x00-\x1f]{0,255})',
    }
    result, invalid, nonconforming = {}, 0, []
    for key, value in fields.items():
        if not isinstance(key, str) or re.fullmatch(patterns["key"], key) is None:
            invalid += 1
            continue
        kind = None
        if key in ("status", "stage", "code"):
            kind = "code"
        elif key == "guidance":
            kind = "message"
        elif key in ("exception_type", "location_module", "location_function"):
            kind = "identifier"
        elif key in ("exit_code", "location_line"):
            kind = "integer"
        elif key in ("version", "utc"):
            kind = key
        else:
            for suffixes, candidate in (
                (("_status", "_stage", "_code"), "code"), (("_message",), "message"),
                (("_count", "_bytes", "_ms", "_line"), "integer"),
                (("_sha256",), "sha256"), (("_commit",), "commit"), (("_version",), "version"),
                (("_id",), "id"), (("_utc",), "utc"), (("_path", "_dir", "_root", "_file"), "path"),
            ):
                if key.endswith(suffixes):
                    kind = candidate
                    break
        allowed = value is None or type(value) is bool
        if not allowed:
            if kind == "integer":
                allowed = type(value) is int
            elif isinstance(value, str):
                if kind == "code":
                    allowed = value in codes
                elif kind == "message":
                    allowed = value in messages
                elif kind in patterns:
                    allowed = re.fullmatch(patterns[kind], value) is not None
        if allowed:
            result[key] = value
        else:
            nonconforming.append(key)
    if nonconforming:
        result["nonconforming_fields"] = nonconforming
    if invalid:
        result["nonconforming_invalid_key_count"] = invalid
    if nonconforming or invalid:
        result["public_record_code"] = "PUBLIC_FIELD_NONCONFORMING"
    return result


def public_failure_record(error, stage, *, report_reference=None):
    """No exception sentences, source lines, locals, or child output are public."""
    fields = {"status": getattr(error, "status", "FAILED"), "stage": stage,
              "reason_code": getattr(error, "cause_code", None) or getattr(error, "code", None) or "SETUP_FAILED",
              "exception_type": type(error).__module__ + "." + type(error).__qualname__,
              "guidance": RULES["safe_messages"][0]}
    current, seen = error, set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        frame = current.__traceback__
        while frame is not None:
            fields.update(location_module=str(frame.tb_frame.f_globals.get("__name__", "")),
                          location_function=frame.tb_frame.f_code.co_name, location_line=frame.tb_lineno)
            frame = frame.tb_next
        if current.__cause__ is None and (current.__suppress_context__ or current.__context__ is None):
            fields["exception_type"] = type(current).__module__ + "." + type(current).__qualname__
        current = current.__cause__ or (None if current.__suppress_context__ else current.__context__)
    reference = report_reference if report_reference is not None else getattr(error, "_failure_report_ref", None)
    if isinstance(reference, dict) and set(reference) == {"path", "sha256"}:
        accepted = public_record({"report_path": reference["path"], "report_sha256": reference["sha256"]})
        if "report_path" in accepted and "report_sha256" in accepted:
            fields.update(report_path=reference["path"], report_sha256=reference["sha256"])
        else:
            fields["report_status"] = "REPORT_UNAVAILABLE"
    else:
        fields["report_status"] = "REPORT_UNAVAILABLE"
    return public_record(fields)


def emit_public_failure(error, stage, *, prefix="LABEL_MATCH_INSTALLER_FAILURE", report_reference=None, stream=None):
    import sys
    print(prefix + "=" + json.dumps(public_failure_record(error, stage, report_reference=report_reference),
                                    ensure_ascii=False, separators=(",", ":")),
          file=sys.stderr if stream is None else stream, flush=True)


def protected_failure_details(error, environ=None):
    import traceback
    return {"failure_traceback": redact_for_sink(
        "".join(traceback.TracebackException.from_exception(error).format()), environ)}


def public_diagnostic_tree(value):
    """Preserve machine-consumed audit structure, allowing only diagnostic fields."""
    if isinstance(value, dict):
        text_keys = {"message", "last_line", "inner_exception_message", "stdout", "stderr", "exception_chain"}
        if "returncode" in value and ("stdout" in value or "stderr" in value):
            # Command-result metadata is consumed by installation/recovery.
            # Replace only the stream slots; retain returncode and other state.
            return {key: (public_diagnostic_tree({"message": item})
                          if key in ("stdout", "stderr") and item else public_diagnostic_tree(item))
                    for key, item in value.items()}
        free_text = any(value.get(key) and not (isinstance(value[key], str) and
                        (value[key] in RULES["safe_messages"] or value[key] in RULES["allowed_codes"]))
                        for key in text_keys)
        if free_text or (text_keys.intersection(value) and "full_exception_trace" in value):
            candidate = {"status": "FAILED", "reason_code": "INSTALLER_FAILED", "guidance": RULES["safe_messages"][0]}
            candidate.update((key, item) for key, item in value.items() if key not in text_keys)
            for source, target in (("type", "exception_type"), ("inner_exception_type", "exception_type"),
                                   ("returncode", "exit_code"), ("child_exit_code", "exit_code"), ("failure_code", "reason_code")):
                if source in value:
                    candidate[target] = value[source]
            reference = try_write_failure_evidence(json.dumps(redact_json_for_sink(value), ensure_ascii=False))
            if "path" in reference and "sha256" in reference:
                candidate.update(report_path=reference["path"], report_sha256=reference["sha256"])
            else:
                candidate["report_status"] = "REPORT_UNAVAILABLE"
            return public_record(candidate)
        result = {}
        for key, item in value.items():
            if key in ("failure", "error", "error_message", "reason", "blocked_reason", "message_ko", "registration_report_summary_error") and isinstance(item, str) and item and item not in RULES["allowed_codes"] and item not in RULES["safe_messages"]:
                result[key] = public_diagnostic_tree({"message": item})
            else:
                result[key] = public_diagnostic_tree(item)
        return result
    if isinstance(value, (tuple, list)):
        return [public_diagnostic_tree(item) for item in value]
    return value


def emit_support_failure(error, *, prefix="LABEL_MATCH_SUPPORT_FAILURE", stage="SUPPORT", fields=None):
    details = protected_failure_details(error)
    evidence = try_write_failure_evidence(details["failure_traceback"])
    reference = ({"path": evidence["path"], "sha256": evidence["sha256"]}
                 if "path" in evidence and "sha256" in evidence else None)
    if fields is None:
        emit_public_failure(error, stage, prefix=prefix, report_reference=reference)
    else:
        import sys
        record=public_failure_record(error, stage, report_reference=reference)
        record=public_record({**record, **fields})
        print(prefix + "=" + json.dumps(record, ensure_ascii=False, separators=(",", ":")),
              file=sys.stderr, flush=True)


def _secret_environment_entries(environ):
    for name, value in environ.items():
        explicit = name in RULES["explicit"]
        identifier = any(re.search(suffix, name, re.I) and re.fullmatch(pattern, str(value))
                         for suffix, pattern in RULES["identifier_patterns"].items())
        if explicit or (re.search(RULES["pattern"], name)
                        and not re.search(RULES["path_suffix"], name) and not identifier):
            yield name, value


def assert_machine_metadata_text(text, environ=None):
    """Refuse secrets before writing unchanged machine metadata bytes."""
    environ = os.environ if environ is None else environ
    for _name, value in _secret_environment_entries(environ):
        value = str(value or "")
        if not value:
            continue
        units = value.encode('utf-16-be', errors='surrogatepass')
        unicode_escaped = ''.join('\\u%04x' % int.from_bytes(units[index:index + 2], 'big')
                                  for index in range(0, len(units), 2))
        forms = (value, json.dumps(value, ensure_ascii=True)[1:-1],
                 json.dumps(value, ensure_ascii=False)[1:-1], unicode_escaped)
        if any(form in text for form in forms):
            raise ValueError("PRODUCT_METADATA_SECRET_VALUE_REFUSED")


def redact_for_sink(text, environ=None, *, redact_values=()):
    if environ is None:
        environ = os.environ
    text = text.decode('utf-8', errors='replace') if isinstance(text, bytes) else str(text)
    for value in sorted({str(item) for item in redact_values if len(str(item)) >= 4}, key=len, reverse=True):
        for form in dict.fromkeys((json.dumps(value)[1:-1], value.replace("'", "''"), value.replace('"', '""'), value)):
            text = text.replace(form, RULES['field'])
    notes = []
    entries = list(_secret_environment_entries(environ))
    for name, value in sorted(entries, key=lambda row: (-len(row[1]), row[0])):
        if not value:
            continue
        if len(value) < 4:
            notes.append(f"[REDACTION_NOTE:short_value_skipped:{name}]")
            continue
        escaped = json.dumps(value, ensure_ascii=True)[1:-1]
        forms = (escaped, json.dumps(value, ensure_ascii=False)[1:-1],
                 "".join(f"\\u{ord(char):04x}" for char in value),
                 value.replace("'", "''"), value.replace('"', '""'), value)
        for form in dict.fromkeys(forms):
            text = text.replace(form, f"[REDACTED:{name}]")
    # Scan keys independently: an ordinary exception prefix must not consume
    # the assignment or mapping that follows it as an opaque value.
    key_pattern = r'''(?<![A-Za-z0-9_.-])(?P<key>"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|[A-Za-z_][\w.-]*)(?P<separator>\s*[:=]\s*)'''
    value_pattern = r'''"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|[^\s,;}]+'''
    value_regex = re.compile(value_pattern)
    parts = []
    copied = protected_until = 0
    for match in re.finditer(key_pattern, text):
        if match.start() < protected_until:
            continue
        key = match['key'].strip("\"'")
        metadata_key = (re.search(RULES['path_suffix'], key)
                    or (re.search(RULES['digest_suffix'], key) and not re.search(RULES['short_secret_digest'], key)))
        sensitive = re.search(RULES['pattern'], key) or key.lower() in ('pin', 'otp', 'password')
        if not sensitive and not metadata_key:
            continue
        value_match = value_regex.match(text, match.end())
        if value_match is None:
            continue
        value = value_match[0]
        protected_until = value_match.end()
        structured = False
        delimiters = RULES['structured_delimiters']
        start = value_match.start()
        opening = delimiters.find(text[start])
        if sensitive and not metadata_key and opening >= 0 and opening % 2 == 0 and not text.startswith('[REDACTED:', start):
            structured = True
            line_end = text.find('\n', start)
            if line_end < 0:
                line_end = len(text)
            protected_until = line_end
            closing = []
            quote = ''
            escaped = False
            for position in range(start, len(text)):
                char = text[position]
                if quote:
                    if escaped:
                        escaped = False
                    elif char == '\\':
                        escaped = True
                    elif char == quote:
                        quote = ''
                elif char in ('"', "'"):
                    quote = char
                else:
                    paired = delimiters.find(char)
                    if paired >= 0 and paired % 2 == 0:
                        closing.append(delimiters[paired + 1])
                    elif paired >= 0:
                        if not closing or closing[-1] != char:
                            break
                        closing.pop()
                        if not closing:
                            protected_until = position + 1
                            break
            value = text[start:protected_until]
        if metadata_key or value in ('true', 'false', 'null') or value.strip("\"'").startswith('[REDACTED:'):
            continue
        quote = '"' if structured else (value[0] if value.startswith(('"', "'")) else '')
        parts.extend((text[copied:match.start()], match['key'] + match['separator'] + quote + RULES['field'] + quote))
        copied = protected_until
    text = ''.join(parts) + text[copied:]
    text = re.sub(r'(?i)\b(Bearer\s+)(?!\[REDACTED:)[^\s"\',;]+', lambda match: match[1] + RULES['field'], text)
    if notes:
        text += "\n" + "\n".join(notes)
    raw = text.encode("utf-8")
    if len(raw) <= RULES["limit"]:
        return text
    head = raw[:RULES["head"]].decode("utf-8", errors="ignore").encode("utf-8")
    tail_length = RULES["limit"] - len(head)
    while True:
        omitted = len(raw) - len(head) - tail_length
        marker = f"\n[TRUNCATED: {omitted} bytes omitted]\n".encode("utf-8")
        available = RULES["limit"] - len(head) - len(marker)
        if available >= tail_length:
            break
        tail_length = available
    tail = raw[-tail_length:].decode("utf-8", errors="ignore").encode("utf-8")
    marker = f"\n[TRUNCATED: {len(raw)-len(head)-len(tail)} bytes omitted]\n".encode("utf-8")
    return (head + marker + tail).decode("utf-8")


def redact_json_for_sink(value, environ=None):
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            metadata = (item is None or type(item) is bool or re.search(RULES["path_suffix"], key)
                        or (re.search(RULES["digest_suffix"], key)
                            and not re.search(RULES["short_secret_digest"], key)))
            sensitive = re.search(RULES["pattern"], key) or key.lower() in ("pin", "otp", "password")
            result[key] = RULES["field"] if sensitive and not metadata else redact_json_for_sink(item, environ)
        return result
    if isinstance(value, (list, tuple)):
        return [redact_json_for_sink(item, environ) for item in value]
    return redact_for_sink(value, environ) if isinstance(value, str) else value


def write_failure_evidence(text, *, redact_values=()):
    """Create private, sanitized evidence without expanding command/env transport."""
    directory = Path(tempfile.mkdtemp(prefix="label-failure-diagnostic-"))
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes
        api = ctypes.WinDLL("advapi32", use_last_error=True)
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        convert = api.ConvertStringSecurityDescriptorToSecurityDescriptorW
        convert.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, ctypes.POINTER(wintypes.LPVOID), ctypes.POINTER(wintypes.DWORD)]
        convert.restype = wintypes.BOOL
        protect = api.SetFileSecurityW
        protect.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, wintypes.LPVOID]
        protect.restype = wintypes.BOOL
        kernel.LocalFree.argtypes = [wintypes.LPVOID]
        descriptor = wintypes.LPVOID()
        if not convert("D:P(A;OICI;FA;;;OW)(A;OICI;FA;;;SY)(A;OICI;FA;;;BA)", 1, ctypes.byref(descriptor), None):
            raise OSError(ctypes.get_last_error(), "private diagnostic ACL failed")
        try:
            if not protect(str(directory), 0x80000004, descriptor):
                raise OSError(ctypes.get_last_error(), "private diagnostic ACL failed")
        finally:
            kernel.LocalFree(descriptor)
    text = str(text)
    for value in sorted({str(item) for item in redact_values if len(str(item)) >= 4}, key=len, reverse=True):
        for form in dict.fromkeys((json.dumps(value)[1:-1], value.replace("'", "''"), value.replace('"', '""'), value)):
            text = text.replace(form, RULES["field"])
    protected = redact_for_sink(text)
    raw = protected.encode("utf-8")
    path = directory / "trace.txt"
    with path.open("xb") as output:
        output.write(raw)
        output.flush()
        os.fsync(output.fileno())
    return dict(path=str(path), sha256=hashlib.sha256(raw).hexdigest(),
                limit_bytes=RULES["limit"], truncated="[TRUNCATED:" in protected)


def try_write_failure_evidence(text, *, redact_values=()):
    """A secondary evidence error must leave the original cause reportable."""
    try:
        return write_failure_evidence(text, redact_values=redact_values)
    except Exception as error:
        return public_record({"exception_type": type(error).__name__, "report_status": "REPORT_UNAVAILABLE"})


def validate_failure_evidence(reference):
    path = Path(reference["path"]).absolute()
    temporary = Path(tempfile.gettempdir()).absolute()
    if not path.is_relative_to(temporary) or not re.fullmatch(
        r"label-(?:failure|embedded|product)-diagnostic-[0-9a-z_]+", path.parent.name
    ) or path.name not in ("trace.txt", "stdout.txt", "stderr.txt"):
        raise ValueError("CHILD_FAILURE_EVIDENCE_PATH_INVALID")
    for part in (path, *path.parents):
        info = part.stat(follow_symlinks=False)
        if part.is_symlink() or getattr(info, "st_file_attributes", 0) & 0x400:
            raise ValueError("CHILD_FAILURE_EVIDENCE_LINK_INVALID")
    raw = path.read_bytes()
    if len(raw) > RULES["limit"] or hashlib.sha256(raw).hexdigest() != reference["sha256"]:
        raise ValueError("CHILD_FAILURE_EVIDENCE_BYTES_DIFFER")
    raw.decode("utf-8", errors="strict")
    return reference

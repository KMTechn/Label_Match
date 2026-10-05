"""Installation failure sink contract; raw diagnostics stay in memory."""
import json
import os
from pathlib import Path
import re
import hashlib
import tempfile

RULES = json.loads((Path(__file__).parent / "tools/failure_redaction_rules.json").read_text(encoding="utf-8"))


def redact_for_sink(text, environ=None, *, redact_values=()):
    if environ is None:
        environ = os.environ
    text = text.decode('utf-8', errors='replace') if isinstance(text, bytes) else str(text)
    for value in sorted({str(item) for item in redact_values if len(str(item)) >= 4}, key=len, reverse=True):
        for form in dict.fromkeys((json.dumps(value)[1:-1], value.replace("'", "''"), value.replace('"', '""'), value)):
            text = text.replace(form, RULES['field'])
    notes = []
    entries = [(name, value) for name, value in environ.items()
               if (re.search(RULES["pattern"], name) or name in RULES["explicit"])
               and not re.search(RULES["path_suffix"], name)]
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
    def assignment(match):
        key = match['key'].strip('"')
        value = match['value']
        metadata = (value in ('true', 'false', 'null') or re.search(RULES['path_suffix'], key)
                    or (re.search(RULES['digest_suffix'], key) and not re.search(RULES['short_secret_digest'], key)))
        sensitive = re.search(RULES['pattern'], key) or key.lower() in ('pin', 'otp', 'password')
        if not sensitive or metadata or value.strip('"').startswith('[REDACTED:'):
            return match[0]
        return match['key'] + match['separator'] + (json.dumps(RULES['field']) if value.startswith('"') else RULES['field'])
    text = re.sub(r'(?P<key>"(?:\\.|[^"\\])*"|[A-Za-z_][\w.-]*)(?P<separator>\s*[:=]\s*)(?P<value>"(?:\\.|[^"\\])*"|[^\s,;}]+)', assignment, text)
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
        lines = redact_for_sink(str(error)).splitlines()
        return {"evidence_failure": {"type": type(error).__name__,
                "last_line": next((line for line in reversed(lines) if line.strip()), "")[-300:]}}


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

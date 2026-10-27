"""Bind app-owned PowerShell callers to the single installer retry function."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess

ROOT = Path(__file__).resolve().parents[1]
CONSUMERS = ("tools/label_writer_fence.ps1", "tools/label_worker_shortcuts.ps1", "INSTALL_CANONICAL_PORTABLE.ps1")
PIN = re.compile(r"(?m)(\$expectedAtomicBodySha256 = ')[0-9a-f]{64}(')")
INLINE = re.compile(r"(?s)(# LABEL_ATOMIC_INLINE_START\n\$text = ')(.*?)('\n# LABEL_ATOMIC_INLINE_END)")
RESTORE_CATALOG = "tests/fixtures/lm_atomic_directory_sites.json"


def restore_move_site(root):
    engine = shutil.which("pwsh") or shutil.which("powershell")
    if engine is None:
        raise ValueError("PowerShell compiler is required for the atomic restore declaration")
    completed = subprocess.run(
        [engine, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File",
         str(Path(__file__).with_name("_atomic_restore_catalog.ps1")), "-Root", str(root)],
        capture_output=True, text=True, check=False,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    if completed.returncode:
        raise ValueError("atomic restore declaration could not be derived")
    site = json.loads(completed.stdout)
    if (site.get("file") != "INSTALL_THIS_PC.ps1"
            or site.get("function") != "Restore-RemovalFilePreimage"
            or site.get("count") != 1
            or not re.fullmatch(r"[0-9a-f]{64}", site.get("sha256", ""))):
        raise ValueError("atomic restore declaration is invalid")
    return {
        "function": site["function"],
        "count": site["count"],
        "sha256": site["sha256"],
        "reason": "Approved same-volume removal restoration; exact AST extent pinned.",
        "file": site["file"],
    }


def refresh_restore_catalog(root, *, check=False):
    expected = restore_move_site(root)
    path = root / RESTORE_CATALOG
    raw = path.read_bytes()
    entries = json.loads(raw.decode("utf-8-sig"))
    matching = [entry for entry in entries
                if (entry.get("file"), entry.get("function"))
                == (expected["file"], expected["function"])]
    if len(matching) > 1:
        raise ValueError("atomic restore declaration is ambiguous")
    if not check:
        entries = [entry for entry in entries if entry not in matching]
        entries.append(expected)
        newline = "\r\n" if b"\r\n" in raw else "\n"
        text = (json.dumps(entries, indent=2) + "\n").replace("\n", newline)
        prefix = b"\xef\xbb\xbf" if raw.startswith(b"\xef\xbb\xbf") else b""
        path.write_bytes(prefix + text.encode("utf-8"))
        matching = [expected]
    if matching != [expected]:
        raise ValueError("atomic restore declaration differs")


def helper_body(root):
    text = (root / "INSTALL_THIS_PC.ps1").read_text(encoding="utf-8-sig")
    start = text.index("function Invoke-LabelAtomicReplace {")
    end = text.index("\n# LABEL_ATOMIC_REPLACE_END", start)
    return text[start:end].replace("\r\n", "\n")


def validate(root):
    pin = hashlib.sha256(helper_body(root).encode("utf-8")).hexdigest()
    for name in CONSUMERS:
        matches = PIN.findall((root / name).read_text(encoding="utf-8-sig"))
        source = (root / name).read_text(encoding="utf-8-sig")
        found = re.findall(r"\$expectedAtomicBodySha256 = '([0-9a-f]{64})'", source)
        copies = INLINE.findall(source)
        if len(matches) != 1 or found != [pin] or len(copies) != 1 or copies[0][1].replace("''", "'") != helper_body(root):
            raise ValueError("atomic replacement function pin differs: " + name)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    pin = hashlib.sha256(helper_body(ROOT).encode("utf-8")).hexdigest()
    if not args.check:
        for name in CONSUMERS:
            path = ROOT / name
            raw = path.read_bytes()
            text, count = PIN.subn(lambda match: match[1] + pin + match[2], raw.decode("utf-8-sig"))
            if count != 1:
                raise ValueError("atomic replacement declaration is ambiguous")
            text, copies = INLINE.subn(lambda match: match[1] + helper_body(ROOT).replace("'", "''") + match[3], text)
            if copies != 1:
                raise ValueError("atomic replacement function copy is ambiguous")
            path.write_text(text, encoding="utf-8-sig" if raw.startswith(b"\xef\xbb\xbf") else "utf-8", newline="\n")
    refresh_restore_catalog(ROOT, check=args.check)
    validate(ROOT)
    print("Atomic replacement declarations verified.")


if __name__ == "__main__":
    main()

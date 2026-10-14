"""Bind app-owned PowerShell callers to the single installer retry function."""
import argparse
import hashlib
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
CONSUMERS = ("tools/label_writer_fence.ps1", "tools/label_worker_shortcuts.ps1")
PIN = re.compile(r"(?m)(\$expectedAtomicBodySha256 = ')[0-9a-f]{64}(')")
INLINE = re.compile(r"(?s)(# LABEL_ATOMIC_INLINE_START\n\$text = ')(.*?)('\n# LABEL_ATOMIC_INLINE_END)")


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
    validate(ROOT)
    print("Atomic replacement declarations verified.")


if __name__ == "__main__":
    main()

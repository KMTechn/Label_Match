"""Check tracked PowerShell UTF-8 BOM and historical newline conventions."""
from __future__ import annotations

import argparse
import io
import json
from pathlib import Path
import subprocess
import zipfile

ROOT = Path(__file__).resolve().parents[1]


def assert_distribution_encoding(root: Path) -> int:
    """Check actual payload bytes, including PowerShell leaves in nested ZIPs.

    Archive traversal is bounded and never extracts or executes a member.
    Paths and source text are deliberately absent from failure messages.
    """
    checked = 0
    remaining = 128 * 1024 * 1024

    def inspect(name: str, raw: bytes, depth: int) -> None:
        nonlocal checked, remaining
        remaining -= len(raw)
        if remaining < 0 or depth > 8:
            raise ValueError("POWERSHELL_ENCODING_SCAN_LIMIT")
        suffix = Path(name).suffix.casefold()
        if suffix in (".ps1", ".psm1", ".psd1"):
            try:
                source = raw.decode("utf-8-sig")
            except UnicodeError:
                raise ValueError("POWERSHELL_UTF8_REQUIRED") from None
            if any(ord(char) > 127 for char in source) and not raw.startswith(b"\xef\xbb\xbf"):
                raise ValueError("POWERSHELL_UTF8_BOM_REQUIRED")
            checked += 1
        elif suffix == ".zip":
            with zipfile.ZipFile(io.BytesIO(raw)) as archive:
                for member in archive.infolist():
                    if member.is_dir() or Path(member.filename).suffix.casefold() not in (
                        ".ps1", ".psm1", ".psd1", ".zip"
                    ):
                        continue
                    if member.file_size > remaining:
                        raise ValueError("POWERSHELL_ENCODING_SCAN_LIMIT")
                    inspect(member.filename, archive.read(member), depth + 1)

    paths = root.rglob("*") if root.is_dir() else [root]
    for path in paths:
        if path.is_file() and path.suffix.casefold() in (".ps1", ".psm1", ".psd1", ".zip"):
            if path.stat().st_size > remaining:
                raise ValueError("POWERSHELL_ENCODING_SCAN_LIMIT")
            inspect(path.name, path.read_bytes(), 0)
    return checked


def git(*args):
    return subprocess.run(["git", "-c", "core.autocrlf=false", "-C", str(ROOT), *args],
                          capture_output=True, check=False)


def newline_kind(raw):
    crlf = raw.count(b"\r\n")
    lf = raw.count(b"\n") - crlf
    cr = raw.count(b"\r") - crlf
    if cr or (crlf and lf):
        return "mixed"
    return "CRLF" if crlf else "LF"


def check(baseline):
    result = git("ls-files", "-z", "*.ps1")
    result.check_returncode()
    rows = []
    for name in filter(None, result.stdout.decode("utf-8").split("\0")):
        raw = (ROOT / name).read_bytes()
        source = raw.decode("utf-8-sig")
        non_ascii = any(ord(char) > 127 for char in source)
        old = git("show", baseline + ":" + name)
        expected = newline_kind(old.stdout) if old.returncode == 0 else "LF"
        bom = raw.startswith(b"\xef\xbb\xbf")
        rows.append({"file": name, "non_ascii": non_ascii, "bom": bom,
                     "newlines": newline_kind(raw), "baseline_newlines": expected,
                     "ok": (not non_ascii or bom) and newline_kind(raw) == expected})
    return {"baseline": baseline, "files": len(rows),
            "non_ascii": sum(row["non_ascii"] for row in rows),
            "missing_bom": sum(row["non_ascii"] and not row["bom"] for row in rows),
            "failures": sum(not row["ok"] for row in rows), "details": rows}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", default="d9160b6")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    result = check(args.baseline)
    args.report.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({key: value for key, value in result.items() if key != "details"}))
    raise SystemExit(1 if result["failures"] else 0)

"""Check tracked PowerShell UTF-8 BOM and historical newline conventions."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


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

"""Keep Windows PowerShell's encoding marker on tracked product scripts."""

import codecs
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]


def test_tracked_non_ascii_product_powershell_has_exactly_one_utf8_bom():
    tracked = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    ).stdout.decode("utf-8").split("\0")
    violations = []
    for name in tracked:
        path = Path(name)
        if not name or path.parts[0] == "tests":
            continue
        if path.suffix.lower() not in {".ps1", ".psm1"}:
            continue
        raw = (ROOT / path).read_bytes()
        non_ascii_count = sum(byte >= 128 for byte in raw)
        if non_ascii_count and (
            not raw.startswith(codecs.BOM_UTF8)
            or raw[len(codecs.BOM_UTF8):].startswith(codecs.BOM_UTF8)
        ):
            violations.append(f"{name}: non-ASCII bytes={non_ascii_count}")
    assert not violations, "Expected exactly one UTF-8 BOM:\n" + "\n".join(violations)

"""Refresh the reviewed admin-worker delta without changing historical preimages.

Run after runtime edits, then use --check. Existing candidate declarations are
updated only when they match the previous generated digest for the same file.
The exact historical before values and every unrelated declaration stay intact.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
from pathlib import Path
import pprint
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
BASE = "455033056f461492c3d4563d4663268f440b6090"
FILES = {
    "app/main.py": "portable/main.py",
    "app/Label_Match.py": "Label_Match.py",
    "app/current_user_onboarding.py": "current_user_onboarding.py",
    "INSTALL_THIS_PC.ps1": "INSTALL_THIS_PC.ps1",
    "app/writer_session_fence.py": "writer_session_fence.py",
    "tools/label_writer_fence.ps1": "tools/label_writer_fence.ps1",
}
BEGIN = "# ADMIN-WORKER-DECLARATIONS-BEGIN"
END = "# ADMIN-WORKER-DECLARATIONS-END"


def digest(raw):
    return hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest()


def refresh(*, check=False):
    sys.path.insert(0, str(ROOT))
    from writer_sink_inventory import derive_writer_sink_inventory, writer_sink_inventory_sha256

    pin = writer_sink_inventory_sha256(derive_writer_sink_inventory(ROOT))
    staged = {}
    for path, pattern, replacement in (
        ("writer_session_fence.py", r'(?m)^WRITER_INVENTORY_SHA256 = "[0-9a-f]{64}"', f'WRITER_INVENTORY_SHA256 = "{pin}"'),
        ("tools/label_writer_fence.ps1", r"(?m)^\$Script:LabelWriterFenceInventorySha256 = '[0-9a-f]{64}'", f"$Script:LabelWriterFenceInventorySha256 = '{pin}'"),
    ):
        raw = (ROOT / path).read_bytes()
        text = raw.decode("utf-8-sig")
        updated, count = re.subn(pattern, replacement, text)
        if count != 1:
            raise ValueError("writer inventory declaration is ambiguous")
        staged[path] = (b"\xef\xbb\xbf" if raw.startswith(b"\xef\xbb\xbf") else b"") + updated.encode("utf-8")

    installer = ROOT / "INSTALL_CANONICAL_PORTABLE.ps1"
    raw = installer.read_bytes()
    text = raw.decode("utf-8-sig")
    start = text.index("$probe = @'") + len("$probe = @'")
    finish = text.index("\n'@", start)
    probe = text[start:finish]
    declaration_start, declaration_end = probe.index(BEGIN), probe.index(END) + len(END)
    previous = ast.parse(probe[declaration_start:declaration_end])
    declared = next(ast.literal_eval(node.value) for node in previous.body if isinstance(node, ast.Assign))
    pairs = {}
    replacements = {}
    for relative, path in FILES.items():
        baseline = subprocess.check_output(["git", "-c", "core.autocrlf=false", "-C", str(ROOT), "show", BASE + ":" + path])
        before = digest(baseline)
        after = digest(staged.get(path, (ROOT / path).read_bytes()))
        if relative in declared and declared[relative][0] != before:
            raise ValueError("historical admin-worker preimage changed")
        pairs[relative] = before, after
        replacements[before] = after
        replacements[declared.get(relative, (before, before))[1]] = after
    def rewrite(value):
        tree = ast.parse(value)
        parents = {child: node for node in ast.walk(tree) for child in ast.iter_child_nodes(node)}
        lines = value.splitlines(keepends=True)
        offsets = [0]
        for line in lines: offsets.append(offsets[-1] + len(line))
        edits = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Constant) or node.value not in replacements:
                continue
            ancestor = node
            historical = False
            while ancestor in parents:
                parent = parents[ancestor]
                if isinstance(parent, ast.Tuple) and len(parent.elts) == 2 and ancestor is parent.elts[0]:
                    historical = True
                if isinstance(parent, ast.Compare) and "installed" in ast.unparse(parent.left):
                    historical = True
                ancestor = parent
            if historical: continue
            def position(line, column):
                return offsets[line-1] + len(lines[line-1].encode("utf-8")[:column].decode("utf-8"))
            edits.append((position(node.lineno, node.col_offset), position(node.end_lineno, node.end_col_offset),
                          repr(replacements[node.value])))
        for start, finish, replacement in sorted(edits, reverse=True):
            value = value[:start] + replacement + value[finish:]
        return value
    declaration = BEGIN + "\nadmin_worker_replacements = " + pprint.pformat(pairs, sort_dicts=True, width=110) + "\n" + END
    updated = rewrite(probe[:declaration_start]) + declaration + rewrite(probe[declaration_end:])
    probe_sha = hashlib.sha256(updated.removeprefix("\n").encode("utf-8")).hexdigest()
    tail, count = re.subn(r"(-ProbeBytes \$probeBytes -ExpectedSha256 ')[0-9a-f]{64}(')",
                         lambda match: match[1] + probe_sha + match[2], text[finish:], count=1)
    if count != 1:
        raise ValueError("writer probe hash declaration is ambiguous")
    staged[installer.name] = (b"\xef\xbb\xbf" if raw.startswith(b"\xef\xbb\xbf") else b"") + (text[:start] + updated + tail).encode("utf-8")
    changed = [path for path, data in staged.items() if (ROOT / path).read_bytes() != data]
    if check and changed:
        raise ValueError("generated writer declarations are stale: " + ", ".join(changed))
    if not check:
        for path in changed:
            (ROOT / path).write_bytes(staged[path])
    print("ADMIN_WORKER_DECLARATIONS_PASS" + (" --check" if check else ""))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    refresh(check=parser.parse_args().check)

"""Fail-closed path classifier for the hosted Label_Match UI evidence lane."""

from __future__ import annotations

import argparse
import re
import subprocess
from collections.abc import Iterable


COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
PROCESS_ONLY_EXACT = {
    ".gitattributes",
    ".gitignore",
    "CLAUDE.md",
    "CODEX.md",
    "README.txt",
    "pytest.ini",
}
UI_EVIDENCE_TEST = "tests/test_label_operator_workbench.py"


def normalize_path(value: str) -> str:
    path = value.strip().replace("\\", "/")
    while path.startswith("./"):
        path = path[2:]
    return path


def is_process_only_path(value: str) -> bool:
    """Return True only for paths explicitly known not to affect runtime UI."""

    path = normalize_path(value)
    if not path or path == UI_EVIDENCE_TEST:
        return False
    if path in PROCESS_ONLY_EXACT:
        return True
    if path.startswith((".github/", "docs/")):
        return True
    if path.startswith("tests/"):
        return True
    if "/" not in path and path.lower().endswith(".md"):
        return True
    return False


def requires_hosted_ui_evidence(paths: Iterable[str]) -> bool:
    changed = [normalize_path(path) for path in paths if normalize_path(path)]
    return not changed or any(not is_process_only_path(path) for path in changed)


def _git(*arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *arguments],
        check=check,
        capture_output=True,
        text=True,
    )


def classify_commits(before: str, head: str) -> bool:
    before = before.strip().lower()
    head = head.strip().lower()
    if not COMMIT_RE.fullmatch(head):
        raise ValueError("head must be an exact 40-character lowercase commit SHA")
    _git("cat-file", "-e", f"{head}^{{commit}}")
    if not COMMIT_RE.fullmatch(before) or set(before) == {"0"}:
        return True
    if _git("cat-file", "-e", f"{before}^{{commit}}", check=False).returncode != 0:
        return True
    changed = _git(
        "diff",
        "--name-only",
        "--diff-filter=ACMRD",
        before,
        head,
    ).stdout.splitlines()
    return requires_hosted_ui_evidence(changed)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--before", required=True)
    parser.add_argument("--head", required=True)
    arguments = parser.parse_args()
    print("true" if classify_commits(arguments.before, arguments.head) else "false")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python
"""Verify the one final canonical annotated Git tag used before candidate build."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import subprocess
from typing import Sequence


SEMVER_TAG_RE = re.compile(r"^v(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)$")
OID_RE = re.compile(r"^[0-9a-f]{40}$")


class TagAttestationError(RuntimeError):
    """Raised when the final annotated tag identity is not canonical."""


def _git(repo_root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "git command failed"
        raise TagAttestationError(detail)
    return completed.stdout


def _git_bytes(repo_root: Path, *args: str) -> bytes:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=False,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or b"git command failed").decode(
            "utf-8", errors="replace"
        ).strip()
        raise TagAttestationError(detail)
    return completed.stdout


def verify_release_tag_attestation(
    repo_root: Path,
    *,
    expected_tag: str,
    expected_commit: str,
) -> dict[str, object]:
    repo_root = repo_root.resolve()
    expected_tag = str(expected_tag or "").strip()
    expected_commit = str(expected_commit or "").strip().lower()
    if SEMVER_TAG_RE.fullmatch(expected_tag) is None:
        raise TagAttestationError("expected tag is not strict semver")
    if OID_RE.fullmatch(expected_commit) is None:
        raise TagAttestationError("expected commit must be a full lowercase Git OID")
    tag_ref = f"refs/tags/{expected_tag}"
    tag_object = _git(repo_root, "rev-parse", "--verify", tag_ref).strip().lower()
    if OID_RE.fullmatch(tag_object) is None:
        raise TagAttestationError("tag object did not resolve to a full Git OID")
    if _git(repo_root, "cat-file", "-t", tag_ref).strip() != "tag":
        raise TagAttestationError("release identity requires an annotated tag")
    peeled_commit = _git(
        repo_root, "rev-parse", "--verify", f"{tag_ref}^{{commit}}"
    ).strip().lower()
    if peeled_commit != expected_commit:
        raise TagAttestationError("annotated tag does not peel to the expected commit")
    raw_bytes = _git_bytes(repo_root, "cat-file", "tag", tag_ref)
    if b"\r" in raw_bytes or b"\x00" in raw_bytes:
        raise TagAttestationError("annotated tag object is not canonical LF UTF-8 text")
    try:
        raw = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise TagAttestationError("annotated tag object is not canonical LF UTF-8 text") from exc
    header, separator, message = raw.partition("\n\n")
    if not separator:
        raise TagAttestationError("annotated tag message is missing")
    headers: dict[str, str] = {}
    for line in header.splitlines():
        key, space, value = line.partition(" ")
        if not space or key in headers:
            raise TagAttestationError("annotated tag header is malformed")
        headers[key] = value
    if set(headers) != {"object", "type", "tag", "tagger"}:
        raise TagAttestationError("annotated tag headers are not canonical")
    if headers["object"].lower() != expected_commit or headers["type"] != "commit":
        raise TagAttestationError("annotated tag does not bind the expected commit")
    if headers["tag"] != expected_tag:
        raise TagAttestationError("annotated tag header does not bind the expected tag")
    canonical_message = f"Release {expected_tag}\n"
    if message != canonical_message:
        raise TagAttestationError(
            "annotated tag message must be the canonical single release-title line"
        )
    return {
        "schema_version": "label-match-canonical-annotated-tag-v1",
        "status": "PASS",
        "tag": expected_tag,
        "tag_object": tag_object,
        "tag_object_type": "tag",
        "annotated_tag": True,
        "commit": expected_commit,
        "peeled_commit": peeled_commit,
        "message": canonical_message.rstrip("\n"),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify Label_Match canonical annotated tag")
    parser.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--expected-tag", required=True)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--report", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = verify_release_tag_attestation(
            Path(args.repo_root),
            expected_tag=args.expected_tag,
            expected_commit=args.expected_commit,
        )
    except TagAttestationError as exc:
        print(f"release_tag_attestation=DENY reason={exc}")
        return 2
    payload = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    report = Path(args.report)
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(payload, encoding="utf-8", newline="\n")
    print(payload, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

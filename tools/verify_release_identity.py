#!/usr/bin/env python
"""Fail closed when a release tag does not match the committed application."""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import subprocess
from pathlib import Path
from typing import Sequence


SEMVER_TAG_RE = re.compile(r"^v(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)$")


class ReleaseIdentityError(RuntimeError):
    """Raised when the tag, source identity, or checkout is not release-safe."""


def read_literal_app_version(source_path: Path) -> str:
    tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
    values: list[str] = []
    for node in ast.walk(tree):
        value_node: ast.expr | None = None
        targets: list[ast.expr] = []
        if isinstance(node, ast.Assign):
            targets = list(node.targets)
            value_node = node.value
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
            value_node = node.value
        if not any(isinstance(target, ast.Name) and target.id == "APP_VERSION" for target in targets):
            continue
        if not isinstance(value_node, ast.Constant) or not isinstance(value_node.value, str):
            raise ReleaseIdentityError("APP_VERSION must be a literal string")
        values.append(value_node.value)
    if len(values) != 1:
        raise ReleaseIdentityError(f"expected exactly one literal APP_VERSION, found {len(values)}")
    if not SEMVER_TAG_RE.fullmatch(values[0]):
        raise ReleaseIdentityError(f"APP_VERSION is not a strict semver tag: {values[0]!r}")
    return values[0]


def _run_git(repo_root: Path, *args: str) -> subprocess.CompletedProcess[str]:
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
        raise ReleaseIdentityError(detail)
    return completed


def _git(repo_root: Path, *args: str) -> str:
    completed = _run_git(repo_root, *args)
    return completed.stdout.strip()


def _verified_tag_signer_fingerprints(repo_root: Path, tag_ref: str) -> dict[str, str]:
    completed = _run_git(repo_root, "verify-tag", "--raw", tag_ref)
    status = f"{completed.stdout}\n{completed.stderr}"
    matches = re.findall(
        r"(?m)^\[GNUPG:\]\s+VALIDSIG\s+([0-9A-Fa-f]{40,64})\b([^\r\n]*)$",
        status,
    )
    if len(matches) != 1:
        raise ReleaseIdentityError("release tag must expose exactly one valid OpenPGP signer fingerprint")
    signing = matches[0][0].upper()
    trailing_fingerprints = re.findall(r"\b[0-9A-Fa-f]{40,64}\b", matches[0][1])
    primary = trailing_fingerprints[-1].upper() if trailing_fingerprints else signing
    return {
        "signing_fingerprint": signing,
        "primary_fingerprint": primary,
    }


def verify_release_identity(
    repo_root: Path,
    *,
    expected_tag: str,
    expected_sha: str,
    expected_tag_signer: str,
    reviewed_ref: str = "refs/remotes/origin/main",
) -> dict[str, object]:
    repo_root = repo_root.resolve()
    expected_tag = str(expected_tag or "").strip()
    expected_sha = str(expected_sha or "").strip().lower()
    expected_tag_signer = str(expected_tag_signer or "").replace(" ", "").upper()
    reviewed_ref = str(reviewed_ref or "").strip()
    if not SEMVER_TAG_RE.fullmatch(expected_tag):
        raise ReleaseIdentityError(f"release tag is not strict semver: {expected_tag!r}")
    if not re.fullmatch(r"[0-9a-f]{40}", expected_sha):
        raise ReleaseIdentityError("expected SHA must be a full 40-character lowercase Git object id")
    if not re.fullmatch(r"[0-9A-F]{40,64}", expected_tag_signer):
        raise ReleaseIdentityError("expected tag signer must be a 40-64 character hexadecimal fingerprint")
    if not reviewed_ref.startswith("refs/remotes/") or any(char.isspace() for char in reviewed_ref):
        raise ReleaseIdentityError("reviewed ref must be an explicit refs/remotes/* name")

    app_version = read_literal_app_version(repo_root / "Label_Match.py")
    if app_version != expected_tag:
        raise ReleaseIdentityError(
            f"APP_VERSION {app_version!r} does not match release tag {expected_tag!r}"
        )

    head = _git(repo_root, "rev-parse", "HEAD").lower()
    if head != expected_sha:
        raise ReleaseIdentityError(f"checkout HEAD {head} does not match expected SHA {expected_sha}")

    tag_ref = f"refs/tags/{expected_tag}"
    tag_type = _git(repo_root, "cat-file", "-t", tag_ref)
    if tag_type != "tag":
        raise ReleaseIdentityError(f"release tag {expected_tag} must be an annotated tag object")
    tag_signer = _verified_tag_signer_fingerprints(repo_root, tag_ref)
    if expected_tag_signer not in set(tag_signer.values()):
        raise ReleaseIdentityError(
            "release tag signer fingerprint does not match the approved fingerprint"
        )
    tag_commit = _git(repo_root, "rev-parse", f"{tag_ref}^{{commit}}").lower()
    if tag_commit != head:
        raise ReleaseIdentityError(f"tag {expected_tag} resolves to {tag_commit}, not checkout HEAD {head}")

    reviewed_commit = _git(repo_root, "rev-parse", "--verify", reviewed_ref).lower()
    if not re.fullmatch(r"[0-9a-f]{40}", reviewed_commit):
        raise ReleaseIdentityError(f"reviewed ref {reviewed_ref} did not resolve to a commit")
    if reviewed_commit != head:
        raise ReleaseIdentityError(
            f"checkout HEAD {head} is not the exact reviewed ref commit {reviewed_commit}"
        )

    status_lines = [line for line in _git(repo_root, "status", "--porcelain=v1", "--untracked-files=all").splitlines() if line]
    if status_lines:
        raise ReleaseIdentityError("release checkout must be clean")

    tree = _git(repo_root, "rev-parse", "HEAD^{tree}").lower()
    return {
        "schema_version": "label-match-release-identity-v2",
        "status": "PASS",
        "tag": expected_tag,
        "app_version": app_version,
        "commit": head,
        "tree": tree,
        "clean_checkout": True,
        "annotated_tag": True,
        "tag_signature_verified": True,
        "expected_tag_signer_fingerprint": expected_tag_signer,
        "tag_signing_fingerprint": tag_signer["signing_fingerprint"],
        "tag_primary_fingerprint": tag_signer["primary_fingerprint"],
        "reviewed_ref": reviewed_ref,
        "reviewed_ref_commit": reviewed_commit,
        "reviewed_main_ancestor": True,
        "reviewed_ref_exact": True,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Verify the Label_Match release tag and committed source identity")
    parser.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--expected-tag", default=os.getenv("GITHUB_REF_NAME", ""))
    parser.add_argument("--expected-sha", default=os.getenv("GITHUB_SHA", ""))
    parser.add_argument(
        "--expected-tag-signer",
        default=os.getenv("LABEL_MATCH_RELEASE_TAG_SIGNER_FINGERPRINT", ""),
    )
    parser.add_argument("--reviewed-ref", default="refs/remotes/origin/main")
    parser.add_argument("--report", default="")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = verify_release_identity(
            Path(args.repo_root),
            expected_tag=args.expected_tag,
            expected_sha=args.expected_sha,
            expected_tag_signer=args.expected_tag_signer,
            reviewed_ref=args.reviewed_ref,
        )
    except ReleaseIdentityError as exc:
        print(f"release_identity=DENY reason={exc}")
        return 2
    payload = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.report:
        report_path = Path(args.report)
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(payload, encoding="utf-8", newline="\n")
    print(payload, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

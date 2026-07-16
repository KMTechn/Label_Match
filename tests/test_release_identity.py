from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess

import pytest


MODULE_PATH = Path(__file__).resolve().parents[1] / "tools" / "verify_release_identity.py"
SPEC = importlib.util.spec_from_file_location("verify_release_identity_for_tests", MODULE_PATH)
assert SPEC and SPEC.loader
identity = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(identity)
SIGNER = "C" * 40


def _approve_tag_signer(monkeypatch):
    monkeypatch.setattr(
        identity,
        "_verified_tag_signer_fingerprints",
        lambda _root, _tag: {
            "signing_fingerprint": SIGNER,
            "primary_fingerprint": SIGNER,
        },
    )


def test_verified_tag_signer_fingerprint_parses_signing_and_primary_keys(tmp_path, monkeypatch):
    signing = "D" * 40
    primary = "E" * 40
    raw = (
        f"[GNUPG:] VALIDSIG {signing} 2026-07-16 0 4 0 1 10 00 {primary}\n"
    )
    monkeypatch.setattr(
        identity,
        "_run_git",
        lambda *_args: subprocess.CompletedProcess([], 0, "", raw),
    )

    assert identity._verified_tag_signer_fingerprints(tmp_path, "refs/tags/v2.0.36") == {
        "signing_fingerprint": signing,
        "primary_fingerprint": primary,
    }

    monkeypatch.setattr(
        identity,
        "_run_git",
        lambda *_args: subprocess.CompletedProcess([], 0, "", "Good signature without raw fingerprint"),
    )
    with pytest.raises(identity.ReleaseIdentityError, match="exactly one valid OpenPGP"):
        identity._verified_tag_signer_fingerprints(tmp_path, "refs/tags/v2.0.36")


def test_read_literal_app_version_requires_one_strict_literal(tmp_path):
    source = tmp_path / "Label_Match.py"
    source.write_text('APP_VERSION = "v2.0.36"\n', encoding="utf-8")
    assert identity.read_literal_app_version(source) == "v2.0.36"

    source.write_text('APP_VERSION = make_version()\n', encoding="utf-8")
    with pytest.raises(identity.ReleaseIdentityError, match="literal string"):
        identity.read_literal_app_version(source)

    source.write_text('APP_VERSION = "2.0.36"\n', encoding="utf-8")
    with pytest.raises(identity.ReleaseIdentityError, match="strict semver"):
        identity.read_literal_app_version(source)

    source.write_text('APP_VERSION = " v2.0.36 "\n', encoding="utf-8")
    with pytest.raises(identity.ReleaseIdentityError, match="strict semver"):
        identity.read_literal_app_version(source)


def test_verify_release_identity_binds_tag_head_tree_version_and_clean_checkout(tmp_path, monkeypatch):
    (tmp_path / "Label_Match.py").write_text('APP_VERSION = "v2.0.36"\n', encoding="utf-8")
    commit = "a" * 40
    tree = "b" * 40
    replies = {
        ("rev-parse", "HEAD"): commit,
        ("cat-file", "-t", "refs/tags/v2.0.36"): "tag",
        ("rev-parse", "refs/tags/v2.0.36^{commit}"): commit,
        ("rev-parse", "--verify", "refs/remotes/origin/main"): commit,
        ("status", "--porcelain=v1", "--untracked-files=all"): "",
        ("rev-parse", "HEAD^{tree}"): tree,
    }
    monkeypatch.setattr(identity, "_git", lambda _root, *args: replies[args])
    _approve_tag_signer(monkeypatch)

    result = identity.verify_release_identity(
        tmp_path,
        expected_tag="v2.0.36",
        expected_sha=commit,
        expected_tag_signer=SIGNER,
    )

    assert result == {
        "schema_version": "label-match-release-identity-v2",
        "status": "PASS",
        "tag": "v2.0.36",
        "app_version": "v2.0.36",
        "commit": commit,
        "tree": tree,
        "clean_checkout": True,
        "annotated_tag": True,
        "tag_signature_verified": True,
        "expected_tag_signer_fingerprint": SIGNER,
        "tag_signing_fingerprint": SIGNER,
        "tag_primary_fingerprint": SIGNER,
        "reviewed_ref": "refs/remotes/origin/main",
        "reviewed_ref_commit": commit,
        "reviewed_main_ancestor": True,
        "reviewed_ref_exact": True,
    }


def test_verify_release_identity_rejects_app_version_mismatch(tmp_path, monkeypatch):
    (tmp_path / "Label_Match.py").write_text('APP_VERSION = "v2.0.35"\n', encoding="utf-8")
    monkeypatch.setattr(identity, "_git", lambda *_args: "a" * 40)
    with pytest.raises(identity.ReleaseIdentityError, match="does not match release tag"):
        identity.verify_release_identity(
            tmp_path,
            expected_tag="v2.0.36",
            expected_sha="a" * 40,
            expected_tag_signer=SIGNER,
        )


@pytest.mark.parametrize(
    ("tag", "sha", "changed_reply", "message"),
    [
        ("release-2.0.36", "a" * 40, None, "strict semver"),
        ("v2.0.36", "c" * 40, None, "checkout HEAD"),
        ("v2.0.36", "a" * 40, (("rev-parse", "refs/tags/v2.0.36^{commit}"), "c" * 40), "tag v2.0.36 resolves"),
        ("v2.0.36", "a" * 40, (("status", "--porcelain=v1", "--untracked-files=all"), " M file.py"), "must be clean"),
    ],
)
def test_verify_release_identity_rejects_mismatches(tmp_path, monkeypatch, tag, sha, changed_reply, message):
    (tmp_path / "Label_Match.py").write_text('APP_VERSION = "v2.0.36"\n', encoding="utf-8")
    replies = {
        ("rev-parse", "HEAD"): "a" * 40,
        ("cat-file", "-t", "refs/tags/v2.0.36"): "tag",
        ("rev-parse", "refs/tags/v2.0.36^{commit}"): "a" * 40,
        ("rev-parse", "--verify", "refs/remotes/origin/main"): "a" * 40,
        ("status", "--porcelain=v1", "--untracked-files=all"): "",
        ("rev-parse", "HEAD^{tree}"): "b" * 40,
    }
    if changed_reply:
        replies[changed_reply[0]] = changed_reply[1]
    monkeypatch.setattr(identity, "_git", lambda _root, *args: replies[args])
    _approve_tag_signer(monkeypatch)

    with pytest.raises(identity.ReleaseIdentityError, match=message):
        identity.verify_release_identity(
            tmp_path,
            expected_tag=tag,
            expected_sha=sha,
            expected_tag_signer=SIGNER,
        )


def test_verify_release_identity_rejects_lightweight_or_bad_signature_tag(tmp_path, monkeypatch):
    (tmp_path / "Label_Match.py").write_text('APP_VERSION = "v2.0.36"\n', encoding="utf-8")
    commit = "a" * 40

    def lightweight(_root, *args):
        replies = {
            ("rev-parse", "HEAD"): commit,
            ("cat-file", "-t", "refs/tags/v2.0.36"): "commit",
        }
        return replies[args]

    monkeypatch.setattr(identity, "_git", lightweight)
    with pytest.raises(identity.ReleaseIdentityError, match="annotated tag"):
        identity.verify_release_identity(
            tmp_path,
            expected_tag="v2.0.36",
            expected_sha=commit,
            expected_tag_signer=SIGNER,
        )

    monkeypatch.setattr(identity, "_git", lambda _root, *args: {
        ("rev-parse", "HEAD"): commit,
        ("cat-file", "-t", "refs/tags/v2.0.36"): "tag",
    }[args])
    monkeypatch.setattr(
        identity,
        "_verified_tag_signer_fingerprints",
        lambda *_args: (_ for _ in ()).throw(identity.ReleaseIdentityError("bad signature")),
    )
    with pytest.raises(identity.ReleaseIdentityError, match="bad signature"):
        identity.verify_release_identity(
            tmp_path,
            expected_tag="v2.0.36",
            expected_sha=commit,
            expected_tag_signer=SIGNER,
        )


def test_verify_release_identity_rejects_commit_that_is_not_exact_reviewed_main(tmp_path, monkeypatch):
    (tmp_path / "Label_Match.py").write_text('APP_VERSION = "v2.0.36"\n', encoding="utf-8")
    commit = "a" * 40

    def git_reply(_root, *args):
        replies = {
            ("rev-parse", "HEAD"): commit,
            ("cat-file", "-t", "refs/tags/v2.0.36"): "tag",
            ("rev-parse", "refs/tags/v2.0.36^{commit}"): commit,
            ("rev-parse", "--verify", "refs/remotes/origin/main"): "b" * 40,
        }
        return replies[args]

    monkeypatch.setattr(identity, "_git", git_reply)
    _approve_tag_signer(monkeypatch)
    with pytest.raises(identity.ReleaseIdentityError, match="not the exact reviewed ref"):
        identity.verify_release_identity(
            tmp_path,
            expected_tag="v2.0.36",
            expected_sha=commit,
            expected_tag_signer=SIGNER,
        )


def test_verify_release_identity_rejects_unapproved_tag_signer(tmp_path, monkeypatch):
    (tmp_path / "Label_Match.py").write_text('APP_VERSION = "v2.0.36"\n', encoding="utf-8")
    commit = "a" * 40
    monkeypatch.setattr(identity, "_git", lambda _root, *args: {
        ("rev-parse", "HEAD"): commit,
        ("cat-file", "-t", "refs/tags/v2.0.36"): "tag",
    }[args])
    monkeypatch.setattr(
        identity,
        "_verified_tag_signer_fingerprints",
        lambda *_args: {
            "signing_fingerprint": "D" * 40,
            "primary_fingerprint": "D" * 40,
        },
    )
    with pytest.raises(identity.ReleaseIdentityError, match="approved fingerprint"):
        identity.verify_release_identity(
            tmp_path,
            expected_tag="v2.0.36",
            expected_sha=commit,
            expected_tag_signer=SIGNER,
        )

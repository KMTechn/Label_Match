from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


MODULE_PATH = Path(__file__).resolve().parents[1] / "tools" / "verify_release_identity.py"
SPEC = importlib.util.spec_from_file_location("verify_release_identity_for_tests", MODULE_PATH)
assert SPEC and SPEC.loader
identity = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(identity)


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


@pytest.mark.parametrize(
    ("tag_type", "annotated"),
    [("commit", False), ("tag", True)],
)
def test_verify_release_identity_accepts_internal_lightweight_or_annotated_tag(
    tmp_path,
    monkeypatch,
    tag_type,
    annotated,
):
    (tmp_path / "Label_Match.py").write_text('APP_VERSION = "v2.0.36"\n', encoding="utf-8")
    commit = "a" * 40
    tree = "b" * 40
    replies = {
        ("rev-parse", "HEAD"): commit,
        ("cat-file", "-t", "refs/tags/v2.0.36"): tag_type,
        ("rev-parse", "refs/tags/v2.0.36^{commit}"): commit,
        ("rev-parse", "--verify", "refs/remotes/origin/main"): commit,
        ("status", "--porcelain=v1", "--untracked-files=all"): "",
        ("rev-parse", "HEAD^{tree}"): tree,
    }
    monkeypatch.setattr(identity, "_git", lambda _root, *args: replies[args])

    result = identity.verify_release_identity(
        tmp_path,
        expected_tag="v2.0.36",
        expected_sha=commit,
    )

    assert result == {
        "schema_version": "label-match-release-identity-v3",
        "status": "PASS",
        "tag": "v2.0.36",
        "app_version": "v2.0.36",
        "commit": commit,
        "tree": tree,
        "clean_checkout": True,
        "release_trust": "internal_unsigned",
        "tag_object_type": tag_type,
        "annotated_tag": annotated,
        "tag_signature_verified": False,
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
        )


@pytest.mark.parametrize(
    ("tag", "sha", "changed_reply", "message"),
    [
        ("release-2.0.36", "a" * 40, None, "strict semver"),
        ("v2.0.36", "c" * 40, None, "checkout HEAD"),
        (
            "v2.0.36",
            "a" * 40,
            (("rev-parse", "refs/tags/v2.0.36^{commit}"), "c" * 40),
            "tag v2.0.36 resolves",
        ),
        (
            "v2.0.36",
            "a" * 40,
            (("status", "--porcelain=v1", "--untracked-files=all"), " M file.py"),
            "must be clean",
        ),
    ],
)
def test_verify_release_identity_rejects_mismatches(
    tmp_path,
    monkeypatch,
    tag,
    sha,
    changed_reply,
    message,
):
    (tmp_path / "Label_Match.py").write_text('APP_VERSION = "v2.0.36"\n', encoding="utf-8")
    replies = {
        ("rev-parse", "HEAD"): "a" * 40,
        ("cat-file", "-t", "refs/tags/v2.0.36"): "commit",
        ("rev-parse", "refs/tags/v2.0.36^{commit}"): "a" * 40,
        ("rev-parse", "--verify", "refs/remotes/origin/main"): "a" * 40,
        ("status", "--porcelain=v1", "--untracked-files=all"): "",
        ("rev-parse", "HEAD^{tree}"): "b" * 40,
    }
    if changed_reply:
        replies[changed_reply[0]] = changed_reply[1]
    monkeypatch.setattr(identity, "_git", lambda _root, *args: replies[args])

    with pytest.raises(identity.ReleaseIdentityError, match=message):
        identity.verify_release_identity(
            tmp_path,
            expected_tag=tag,
            expected_sha=sha,
        )


def test_verify_release_identity_rejects_non_tag_object(tmp_path, monkeypatch):
    (tmp_path / "Label_Match.py").write_text('APP_VERSION = "v2.0.36"\n', encoding="utf-8")
    commit = "a" * 40
    monkeypatch.setattr(
        identity,
        "_git",
        lambda _root, *args: {
            ("rev-parse", "HEAD"): commit,
            ("cat-file", "-t", "refs/tags/v2.0.36"): "blob",
        }[args],
    )

    with pytest.raises(identity.ReleaseIdentityError, match="lightweight or annotated"):
        identity.verify_release_identity(
            tmp_path,
            expected_tag="v2.0.36",
            expected_sha=commit,
        )


def test_verify_release_identity_rejects_commit_that_is_not_exact_main(tmp_path, monkeypatch):
    (tmp_path / "Label_Match.py").write_text('APP_VERSION = "v2.0.36"\n', encoding="utf-8")
    commit = "a" * 40
    replies = {
        ("rev-parse", "HEAD"): commit,
        ("cat-file", "-t", "refs/tags/v2.0.36"): "commit",
        ("rev-parse", "refs/tags/v2.0.36^{commit}"): commit,
        ("rev-parse", "--verify", "refs/remotes/origin/main"): "b" * 40,
    }
    monkeypatch.setattr(identity, "_git", lambda _root, *args: replies[args])

    with pytest.raises(identity.ReleaseIdentityError, match="not the exact reviewed ref"):
        identity.verify_release_identity(
            tmp_path,
            expected_tag="v2.0.36",
            expected_sha=commit,
        )

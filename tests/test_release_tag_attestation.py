import importlib.util
from pathlib import Path

import pytest


MODULE_PATH = Path(__file__).resolve().parents[1] / "tools" / "verify_release_tag_attestation.py"
SPEC = importlib.util.spec_from_file_location("tag_attestation_for_tests", MODULE_PATH)
assert SPEC and SPEC.loader
attestation = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(attestation)


TAG = "v2.0.79"
COMMIT = "1" * 40
TAG_OBJECT = "2" * 40


def _raw(message=None):
    if message is None:
        message = f"Release {TAG}\n"
    return (
        f"object {COMMIT}\n"
        "type commit\n"
        f"tag {TAG}\n"
        "tagger KMTechn <release@example.invalid> 1700000000 +0900\n\n"
        f"{message}"
    )


def _mock_git(monkeypatch, raw=None, tag_type="tag"):
    replies = {
        ("rev-parse", "--verify", f"refs/tags/{TAG}"): TAG_OBJECT + "\n",
        ("cat-file", "-t", f"refs/tags/{TAG}"): tag_type + "\n",
        ("rev-parse", "--verify", f"refs/tags/{TAG}^{{commit}}"): COMMIT + "\n",
    }
    monkeypatch.setattr(attestation, "_git", lambda _root, *args: replies[args])
    raw_value = _raw() if raw is None else raw
    monkeypatch.setattr(
        attestation,
        "_git_bytes",
        lambda _root, *args: raw_value.encode("utf-8"),
    )


def test_attestation_accepts_exact_canonical_annotated_tag(tmp_path, monkeypatch):
    _mock_git(monkeypatch)

    result = attestation.verify_release_tag_attestation(
        tmp_path, expected_tag=TAG, expected_commit=COMMIT
    )

    assert result["status"] == "PASS"
    assert result["tag_object"] == TAG_OBJECT
    assert result["tag_object_type"] == "tag"
    assert result["peeled_commit"] == COMMIT
    assert result["message"] == f"Release {TAG}"
    assert set(result) == {
        "schema_version",
        "status",
        "tag",
        "tag_object",
        "tag_object_type",
        "annotated_tag",
        "commit",
        "peeled_commit",
        "message",
    }


@pytest.mark.parametrize(
    "message",
    [
        f"Release {TAG}",
        f"Release {TAG}\n\n",
        f"Release {TAG}\nQualified-ZIP-SHA256: {'3' * 64}\n",
        f"Release {TAG}\nextra\n",
    ],
)
def test_attestation_rejects_noncanonical_or_hash_bearing_message(
    tmp_path, monkeypatch, message
):
    _mock_git(monkeypatch, raw=_raw(message))

    with pytest.raises(attestation.TagAttestationError, match="canonical single"):
        attestation.verify_release_tag_attestation(
            tmp_path, expected_tag=TAG, expected_commit=COMMIT
        )


def test_attestation_rejects_lightweight_tag(tmp_path, monkeypatch):
    _mock_git(monkeypatch, tag_type="commit")

    with pytest.raises(attestation.TagAttestationError, match="annotated tag"):
        attestation.verify_release_tag_attestation(
            tmp_path, expected_tag=TAG, expected_commit=COMMIT
        )


def test_attestation_rejects_crlf_tag_object(tmp_path, monkeypatch):
    _mock_git(monkeypatch)
    monkeypatch.setattr(
        attestation,
        "_git_bytes",
        lambda _root, *args: _raw().replace("\n", "\r\n").encode(),
    )

    with pytest.raises(attestation.TagAttestationError, match="canonical LF"):
        attestation.verify_release_tag_attestation(
            tmp_path, expected_tag=TAG, expected_commit=COMMIT
        )

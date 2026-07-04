import hashlib
import importlib.util
import json
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey


def load_label_match_module():
    module_path = Path(__file__).resolve().parents[1] / "Label_Match.py"
    spec = importlib.util.spec_from_file_location("label_match_manifest_test", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeResponse:
    def __init__(self, payload=None, content=b""):
        self.payload = payload
        self.content = content

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeTk:
    def withdraw(self):
        return None

    def destroy(self):
        return None


def valid_manifest():
    return {
        "schema_version": "kmtech-private-update-manifest-v1",
        "manifest_version": 1,
        "app_id": "Label_Match",
        "package_id": "Label_Match",
        "channel": "stable",
        "version": "v2.0.17",
        "artifact": {
            "name": "Label_Match-v2.0.17.zip",
            "url": "https://updates.example/label_match/Label_Match-v2.0.17.zip",
            "size_bytes": 123,
            "sha256": "b" * 64,
        },
        "archive": {
            "format": "zip",
            "top_level": "Label_Match",
            "entrypoint": "Label_Match.exe",
            "required_files": ["Label_Match/Label_Match.exe"],
        },
        "install": {
            "strategy": "robocopy_backup_then_mirror",
            "preserve_paths": ["config/app_settings.json"],
        },
        "rollout": {
            "percentage": 100,
            "allow_pc_ids": [],
            "deny_pc_ids": [],
        },
    }


def signed_manifest_payload(manifest):
    private = Ed25519PrivateKey.generate()
    signature = private.sign(json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8"))
    public_hex = private.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    ).hex()
    return public_hex, signature


def test_update_provider_off_does_not_call_network(monkeypatch):
    module = load_label_match_module()
    monkeypatch.setenv("LABEL_MATCH_UPDATE_PROVIDER", "off")

    def fail_get(*args, **kwargs):
        raise AssertionError("requests.get should not be called when updater is off")

    monkeypatch.setattr(module.requests, "get", fail_get)

    assert module.check_for_updates() == (None, None)


def test_update_provider_defaults_to_off_without_network(monkeypatch):
    module = load_label_match_module()
    monkeypatch.delenv("LABEL_MATCH_UPDATE_PROVIDER", raising=False)

    def fail_get(*args, **kwargs):
        raise AssertionError("requests.get should not be called without explicit updater provider")

    monkeypatch.setattr(module.requests, "get", fail_get)

    assert module.check_for_updates() == (None, None)


def test_private_manifest_provider_returns_update_candidate(monkeypatch):
    module = load_label_match_module()
    manifest = valid_manifest()
    public_hex, signature = signed_manifest_payload(manifest)
    monkeypatch.setenv("LABEL_MATCH_UPDATE_PROVIDER", "private_manifest")
    monkeypatch.setenv("LABEL_MATCH_UPDATE_MANIFEST_URL", "https://updates.example/label_match/latest.json")
    monkeypatch.setenv("LABEL_MATCH_UPDATE_MANIFEST_PUBLIC_KEY", public_hex)
    monkeypatch.setenv("LABEL_MATCH_UPDATE_CHANNEL", "stable")

    def fake_get(url, timeout=0):
        assert timeout == 5
        if url == "https://updates.example/label_match/latest.json":
            return FakeResponse(manifest)
        if url == "https://updates.example/label_match/latest.json.sig":
            return FakeResponse(content=signature)
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(module.requests, "get", fake_get)

    assert module.check_for_updates() == (
        "https://updates.example/label_match/Label_Match-v2.0.17.zip",
        "v2.0.17",
    )
    candidate = module._check_update_candidate()
    assert candidate["sha256"] == "b" * 64
    assert candidate["archive"] == {
        "top_level": "Label_Match",
        "required_files": ["Label_Match/Label_Match.exe"],
    }


def test_private_manifest_rollout_blocks_and_allowlists_current_pc(monkeypatch):
    module = load_label_match_module()
    manifest = valid_manifest()
    manifest["rollout"]["percentage"] = 0
    monkeypatch.setenv(module.UPDATE_PC_ID_ENV, "line-a-pc-01")

    assert module._update_candidate_from_manifest(manifest, "stable") is None

    manifest["rollout"]["allow_pc_ids"] = [" LINE-A-PC-01 "]
    candidate = module._update_candidate_from_manifest(manifest, "stable")

    assert candidate["url"] == "https://updates.example/label_match/Label_Match-v2.0.17.zip"
    assert candidate["sha256"] == "b" * 64


@pytest.mark.parametrize(
    "mutate",
    [
        lambda manifest: manifest.update({"app_id": "Other_App"}),
        lambda manifest: manifest.update({"channel": "canary"}),
        lambda manifest: manifest.update({"version": "v2.0.7"}),
        lambda manifest: manifest["artifact"].update({"sha256": "not-a-sha"}),
        lambda manifest: manifest["artifact"].update({"name": "Label_Match.exe"}),
        lambda manifest: manifest["artifact"].update({"size_bytes": True}),
        lambda manifest: manifest["artifact"].update({"url": "https://updates.example/update.zip?access_token=abc"}),
        lambda manifest: manifest["artifact"].update({"url": "https://updates.example/update.zip?access-token=abc"}),
        lambda manifest: manifest["artifact"].update({"url": "https://updates.example/update.zip?access%5Ftoken=abc"}),
        lambda manifest: manifest["artifact"].update({"url": "https://updates.example/update.zip#token=abc"}),
        lambda manifest: manifest["artifact"].update({"url": "http://updates.example/update.zip"}),
        lambda manifest: manifest["artifact"].update({"url": "https://updates.example/update.exe"}),
        lambda manifest: manifest["artifact"].update({"url": "https://github.com/KMTechn/Label_Match/releases/download/v2.0.17/Label_Match-v2.0.17.zip"}),
        lambda manifest: manifest["artifact"].update({"url": "https://raw.githubusercontent.com/KMTechn/update-feed/main/Label_Match-v2.0.17.zip"}),
        lambda manifest: manifest.pop("archive"),
        lambda manifest: manifest.pop("install"),
        lambda manifest: manifest.pop("rollout"),
        lambda manifest: manifest["rollout"].pop("allow_pc_ids"),
        lambda manifest: manifest["rollout"].update({"percentage": True}),
    ],
)
def test_private_manifest_provider_rejects_invalid_manifest(monkeypatch, mutate):
    module = load_label_match_module()
    manifest = valid_manifest()
    mutate(manifest)
    public_hex, signature = signed_manifest_payload(manifest)
    monkeypatch.setenv("LABEL_MATCH_UPDATE_PROVIDER", "private_manifest")
    monkeypatch.setenv("LABEL_MATCH_UPDATE_MANIFEST_URL", "https://updates.example/latest.json")
    monkeypatch.setenv("LABEL_MATCH_UPDATE_MANIFEST_PUBLIC_KEY", public_hex)

    def fake_get(url, *args, **kwargs):
        if url == "https://updates.example/latest.json":
            return FakeResponse(manifest)
        if url == "https://updates.example/latest.json.sig":
            return FakeResponse(content=signature)
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(module.requests, "get", fake_get)

    assert module.check_for_updates() == (None, None)


def test_private_manifest_provider_rejects_missing_public_key(monkeypatch):
    module = load_label_match_module()
    monkeypatch.setenv("LABEL_MATCH_UPDATE_PROVIDER", "private_manifest")
    monkeypatch.setenv("LABEL_MATCH_UPDATE_MANIFEST_URL", "https://updates.example/latest.json")

    def fail_get(*args, **kwargs):
        raise AssertionError("requests.get should not be called without a manifest public key")

    monkeypatch.setattr(module.requests, "get", fail_get)

    assert module.check_for_updates() == (None, None)


def test_private_manifest_provider_rejects_bad_signature(monkeypatch):
    module = load_label_match_module()
    manifest = valid_manifest()
    public_hex, _signature = signed_manifest_payload(manifest)
    monkeypatch.setenv("LABEL_MATCH_UPDATE_PROVIDER", "private_manifest")
    monkeypatch.setenv("LABEL_MATCH_UPDATE_MANIFEST_URL", "https://updates.example/latest.json")
    monkeypatch.setenv("LABEL_MATCH_UPDATE_MANIFEST_PUBLIC_KEY", public_hex)

    def fake_get(url, *args, **kwargs):
        if url == "https://updates.example/latest.json":
            return FakeResponse(manifest)
        if url == "https://updates.example/latest.json.sig":
            return FakeResponse(content=b"0" * 64)
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(module.requests, "get", fake_get)

    assert module.check_for_updates() == (None, None)


def test_private_manifest_provider_rejects_insecure_manifest_url(monkeypatch):
    module = load_label_match_module()
    monkeypatch.setenv("LABEL_MATCH_UPDATE_PROVIDER", "private_manifest")
    monkeypatch.setenv("LABEL_MATCH_UPDATE_MANIFEST_URL", "http://updates.example/latest.json")

    def fail_get(*args, **kwargs):
        raise AssertionError("requests.get should not be called for insecure manifest URL")

    monkeypatch.setattr(module.requests, "get", fail_get)

    assert module.check_for_updates() == (None, None)


def test_private_manifest_provider_rejects_fragment_manifest_url(monkeypatch):
    module = load_label_match_module()
    monkeypatch.setenv("LABEL_MATCH_UPDATE_PROVIDER", "private_manifest")
    monkeypatch.setenv("LABEL_MATCH_UPDATE_MANIFEST_URL", "https://updates.example/latest.json#token=abc")

    def fail_get(*args, **kwargs):
        raise AssertionError("requests.get should not be called for fragment manifest URL")

    monkeypatch.setattr(module.requests, "get", fail_get)

    assert module.check_for_updates() == (None, None)


@pytest.mark.parametrize(
    "query",
    [
        "sig=raw",
        "signature=raw",
        "X-Amz-Signature=raw",
        "X-Goog-Signature=raw",
    ],
)
def test_update_url_rejects_signed_credential_query_keys(query):
    module = load_label_match_module()

    with pytest.raises(ValueError, match="raw token"):
        module._assert_https_update_url(f"https://updates.example/Label_Match-v2.0.17.zip?{query}", require_zip=True)


def test_private_manifest_provider_rejects_github_hosted_manifest_url(monkeypatch):
    module = load_label_match_module()
    monkeypatch.setenv("LABEL_MATCH_UPDATE_PROVIDER", "private_manifest")
    monkeypatch.setenv("LABEL_MATCH_UPDATE_MANIFEST_URL", "https://raw.githubusercontent.com/KMTechn/update-feed/main/latest.json")
    monkeypatch.setenv("LABEL_MATCH_UPDATE_MANIFEST_PUBLIC_KEY", "a" * 64)

    def fail_get(*args, **kwargs):
        raise AssertionError("requests.get should not be called for GitHub-hosted private manifest URL")

    monkeypatch.setattr(module.requests, "get", fail_get)

    assert module.check_for_updates() == (None, None)


def test_private_manifest_provider_rejects_github_hosted_signature_url(monkeypatch):
    module = load_label_match_module()
    manifest = valid_manifest()
    public_hex, _signature = signed_manifest_payload(manifest)
    monkeypatch.setenv("LABEL_MATCH_UPDATE_PROVIDER", "private_manifest")
    monkeypatch.setenv("LABEL_MATCH_UPDATE_MANIFEST_URL", "https://updates.example/latest.json")
    monkeypatch.setenv("LABEL_MATCH_UPDATE_MANIFEST_SIGNATURE_URL", "https://github-releases.githubusercontent.com/123/latest.json.sig")
    monkeypatch.setenv("LABEL_MATCH_UPDATE_MANIFEST_PUBLIC_KEY", public_hex)

    def fake_get(url, *args, **kwargs):
        if url == "https://updates.example/latest.json":
            return FakeResponse(manifest)
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(module.requests, "get", fake_get)

    assert module.check_for_updates() == (None, None)


def test_github_provider_skips_release_asset_without_checksum_when_explicit(monkeypatch):
    module = load_label_match_module()
    monkeypatch.setenv("LABEL_MATCH_UPDATE_PROVIDER", "github")
    release_payload = {
        "tag_name": "v2.0.17",
        "assets": [
            {"name": "notes.txt", "browser_download_url": "https://example.invalid/notes.txt"},
            {
                "name": "Label_Match-v2.0.17.zip",
                "browser_download_url": "https://github.com/KMTechn/Label_Match/releases/download/v2.0.17/Label_Match-v2.0.17.zip",
            },
        ],
    }
    monkeypatch.setattr(module.requests, "get", lambda *args, **kwargs: FakeResponse(release_payload))

    assert module.check_for_updates() == (None, None)


def test_github_provider_uses_release_asset_with_checksum_when_explicit(monkeypatch):
    module = load_label_match_module()
    monkeypatch.setenv("LABEL_MATCH_UPDATE_PROVIDER", "github")
    zip_url = "https://github.com/KMTechn/Label_Match/releases/download/v2.0.17/Label_Match-v2.0.17.zip"
    checksum_url = f"{zip_url}.sha256"
    release_payload = {
        "tag_name": "v2.0.17",
        "assets": [
            {"name": "notes.txt", "browser_download_url": "https://example.invalid/notes.txt"},
            {
                "name": "Label_Match-v2.0.17.zip",
                "browser_download_url": zip_url,
            },
            {
                "name": "Label_Match-v2.0.17.zip.sha256",
                "browser_download_url": checksum_url,
            },
        ],
    }

    def fake_get(url, *args, **kwargs):
        if url == "https://api.github.com/repos/KMTechn/Label_Match/releases/latest":
            return FakeResponse(release_payload)
        if url == checksum_url:
                return FakeResponse(content=f"{'e' * 64}  Label_Match-v2.0.17.zip\n".encode("utf-8"))
        raise AssertionError(f"unexpected URL: {url}")

    monkeypatch.setattr(module.requests, "get", fake_get)

    assert module.check_for_updates() == (
        zip_url,
        "v2.0.17",
    )
    candidate = module._check_update_candidate()
    assert candidate["sha256"] == "e" * 64
    assert candidate["provider"] == module.UPDATE_PROVIDER_GITHUB


def test_github_provider_uses_release_asset_digest_when_present(monkeypatch):
    module = load_label_match_module()
    monkeypatch.setenv("LABEL_MATCH_UPDATE_PROVIDER", "github")
    zip_url = "https://github.com/KMTechn/Label_Match/releases/download/v2.0.17/Label_Match-v2.0.17.zip"
    release_payload = {
        "tag_name": "v2.0.17",
        "assets": [
            {
                    "name": "Label_Match-v2.0.17.zip",
                "browser_download_url": zip_url,
                "digest": f"sha256:{'f' * 64}",
            },
        ],
    }

    def fake_get(url, *args, **kwargs):
        if url == "https://api.github.com/repos/KMTechn/Label_Match/releases/latest":
            return FakeResponse(release_payload)
        raise AssertionError(f"unexpected checksum fetch: {url}")

    monkeypatch.setattr(module.requests, "get", fake_get)

    candidate = module._check_update_candidate()

    assert candidate["url"] == zip_url
    assert candidate["sha256"] == "f" * 64


def test_source_mode_cannot_apply_updates():
    module = load_label_match_module()

    assert module._can_apply_updates() is False


def test_threaded_update_check_passes_private_manifest_archive_policy_to_apply(monkeypatch):
    module = load_label_match_module()
    candidate = module._update_candidate_from_manifest(valid_manifest(), "stable")
    captured = {}

    monkeypatch.setattr(module, "_check_update_candidate", lambda: candidate)
    monkeypatch.setattr(module, "_can_apply_updates", lambda: True)
    monkeypatch.setattr(module.tk, "Tk", lambda: FakeTk())
    monkeypatch.setattr(module.messagebox, "askyesno", lambda *args, **kwargs: True)

    def fake_apply(url, expected_sha256=None, archive_policy=None):
        captured["url"] = url
        captured["expected_sha256"] = expected_sha256
        captured["archive_policy"] = archive_policy

    monkeypatch.setattr(module, "download_and_apply_update", fake_apply)

    module.threaded_update_check()

    assert captured == {
        "url": candidate["url"],
        "expected_sha256": candidate["sha256"],
        "archive_policy": candidate["archive"],
    }


def test_threaded_update_check_source_mode_skips_prompt_and_apply(monkeypatch):
    module = load_label_match_module()
    candidate = module._update_candidate_from_manifest(valid_manifest(), "stable")
    prompts = []
    apply_calls = []

    monkeypatch.setattr(module, "_check_update_candidate", lambda: candidate)
    monkeypatch.setattr(module, "_can_apply_updates", lambda: False)
    monkeypatch.setattr(module.messagebox, "askyesno", lambda *args, **kwargs: prompts.append((args, kwargs)) or True)
    monkeypatch.setattr(module, "download_and_apply_update", lambda *args, **kwargs: apply_calls.append((args, kwargs)))

    module.threaded_update_check()

    assert prompts == []
    assert apply_calls == []


def test_download_and_apply_update_source_mode_aborts_before_network_or_batch(monkeypatch, tmp_path):
    module = load_label_match_module()
    network_calls = []
    popen_calls = []
    errors = []

    monkeypatch.setenv("TEMP", str(tmp_path))
    monkeypatch.setattr(module, "_can_apply_updates", lambda: False)
    monkeypatch.setattr(module.tk, "Tk", lambda: FakeTk())
    monkeypatch.setattr(module.messagebox, "showerror", lambda *args, **kwargs: errors.append((args, kwargs)))

    def fake_get(*args, **kwargs):
        network_calls.append((args, kwargs))
        raise AssertionError("network should not start in source mode")

    def fake_popen(*args, **kwargs):
        popen_calls.append((args, kwargs))
        raise AssertionError("updater batch should not start in source mode")

    monkeypatch.setattr(module.requests, "get", fake_get)
    monkeypatch.setattr(module.subprocess, "Popen", fake_popen)

    with pytest.raises(SystemExit) as exc_info:
        module.download_and_apply_update(
            "https://updates.example/label_match/Label_Match-v2.0.17.zip",
            expected_sha256="b" * 64,
            archive_policy={"top_level": "Label_Match", "required_files": ["Label_Match/Label_Match.exe"]},
        )

    assert exc_info.value.code == 1
    assert network_calls == []
    assert popen_calls == []
    assert errors
    assert not (tmp_path / "update.zip").exists()


def test_verify_update_file_hash_accepts_matching_sha256(tmp_path):
    module = load_label_match_module()
    path = tmp_path / "update.zip"
    path.write_bytes(b"zip bytes")
    expected = hashlib.sha256(b"zip bytes").hexdigest()

    module._verify_update_file_hash(str(path), expected)


def test_verify_update_file_hash_rejects_mismatch(tmp_path):
    module = load_label_match_module()
    path = tmp_path / "update.zip"
    path.write_bytes(b"zip bytes")

    with pytest.raises(ValueError, match="SHA256 mismatch"):
        module._verify_update_file_hash(str(path), "0" * 64)


def test_verify_update_file_hash_requires_expected_sha256(tmp_path):
    module = load_label_match_module()
    path = tmp_path / "update.zip"
    path.write_bytes(b"zip bytes")

    with pytest.raises(ValueError, match="requires"):
        module._verify_update_file_hash(str(path), None)

